import os
import time
from datetime import datetime
from pathlib import Path

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_ID = os.getenv("FRESHNESS_TEST_WAREHOUSE_ID")
BINARY_FILE_SCHEMA = (
    "schema => 'path string, modificationTime timestamp, length bigint, content binary'"
)
COLUMNS = [
    "checked_at",
    "window_start",
    "hourly_prices_latest_modified_at",
    "company_info_latest_modified_at",
    "hourly_prices_fresh_files",
    "company_info_fresh_files",
    "should_run_dbt",
]


def _case(
    name,
    prices,
    company,
    counts,
    decision,
    *,
    checked_at="2026-10-20 07:00:00",
    window_start="2026-10-19 07:00:00",
):
    return pytest.param(
        checked_at, prices, company, window_start, counts, decision, id=name
    )


def _inventory_parts(sql):
    prefix, found, rest = sql.partition("file_inventory as (")
    assert found, "Production file_inventory CTE is missing"
    inventory, found, suffix = rest.partition("),\n\nsource_status as (")
    assert found, "Production source_status CTE is missing"
    return prefix, inventory, suffix


def test_production_inventory_is_json_file_metadata_only():
    sql = (ROOT / "src" / "sql" / "check_raw_file_freshness.sql").read_text()
    _, inventory, _ = _inventory_parts(sql)
    assert inventory.count("read_files(") == 2
    assert inventory.count("format => 'binaryFile'") == 2
    assert inventory.count(BINARY_FILE_SCHEMA) == 2
    assert inventory.count("fileNamePattern => '*.json'") == 2
    assert inventory.count(
        "convert_timezone(current_timezone(), 'UTC', modificationTime)"
    ) == 2
    for source in ("hourly_prices", "company_info"):
        assert f"'/Volumes/api_data/raw/yfinance/{source}'" in inventory
    assert "content" not in inventory.replace(BINARY_FILE_SCHEMA, "").lower()
    assert sql.count("current_timestamp()") == 1


@pytest.fixture(scope="module")
def production_sql():
    return (ROOT / "src" / "sql" / "check_raw_file_freshness.sql").read_text()


@pytest.fixture(scope="module")
def warehouse_client(production_sql):
    return WorkspaceClient()


def _fixture_sql(sql, checked_at, prices, company):
    assert sql.count("current_timestamp()") == 1
    instant = datetime.fromisoformat(checked_at).isoformat() + "Z"
    sql = sql.replace("current_timestamp()", f"to_timestamp('{instant}')")
    prefix, inventory, suffix = _inventory_parts(sql)
    assert inventory.count("read_files(") == 2
    rows = []
    for source, timestamps in (("hourly_prices", prices), ("company_info", company)):
        for timestamp in timestamps:
            value = datetime.fromisoformat(timestamp).isoformat(sep=" ")
            rows.append(f"('{source}', timestamp_ntz'{value}')")
    if rows:
        fixture = (
            "  select * from values\n    "
            + ",\n    ".join(rows)
            + "\n  as fixtures(source_name, modified_at)\n"
        )
    else:
        fixture = (
            "  select cast(null as string) as source_name,\n"
            "    cast(null as timestamp_ntz) as modified_at\n"
            "  where false\n"
        )
    sql = prefix + "file_inventory as (\n" + fixture + "),\n\nsource_status as (" + suffix
    assert "read_files(" not in sql, "Tests must never access raw storage"
    return sql


def _execute(client, sql, expected_state=StatementState.SUCCEEDED):
    response = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
    )
    deadline = time.monotonic() + 180
    while response.status and response.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        if time.monotonic() >= deadline:
            client.statement_execution.cancel_execution(response.statement_id)
            pytest.fail(f"SQL test timed out: {response.statement_id}")
        time.sleep(2)
        response = client.statement_execution.get_statement(response.statement_id)
    assert response.status and response.status.state == expected_state, (
        response.status
    )
    return response


@pytest.mark.skipif(not WAREHOUSE_ID, reason="Requires an explicit test SQL warehouse")
@pytest.mark.parametrize(
    "checked_at,prices,company,window_start,counts,decision",
    [
        _case("inclusive-lower", ["2026-10-19 07:00:00"], ["2026-10-19 07:00:00"], (1, 1), 1),
        _case("before-lower", ["2026-10-19 06:59:59"], ["2026-10-19 06:59:59"], (0, 0), 0),
        _case("inclusive-upper", ["2026-10-20 07:00:00"], ["2026-10-20 07:00:00"], (1, 1), 1),
        _case("future-only", ["2026-10-20 07:00:01"], ["2026-10-20 07:00:01"], (0, 0), 0),
        _case("prices-only", ["2026-10-20 06:00:00"], [], (1, 0), 0),
        _case("company-only", [], ["2026-10-20 05:00:00"], (0, 1), 0),
        _case("empty", [], [], (0, 0), 0),
        _case("company-stale", ["2026-10-20 06:00:00"], ["2026-10-19 06:00:00"], (1, 0), 0),
        _case("prices-stale", ["2026-10-19 06:00:00"], ["2026-10-20 05:00:00"], (0, 1), 0),
        _case(
            "distinct-multiple-and-future",
            ["2026-10-18 07:00:00", "2026-10-19 07:00:00", "2026-10-20 06:00:00",
             "2026-10-20 07:00:01"],
            ["2026-10-20 05:00:00", "2026-10-21 07:00:00"],
            (2, 1), 1,
        ),
        _case(
            "future-prices-valid-company",
            ["2026-10-20 07:00:01"], ["2026-10-20 05:00:00"], (0, 1), 0,
        ),
        _case(
            "valid-prices-future-company",
            ["2026-10-20 06:00:00"], ["2026-10-20 07:00:01"], (1, 0), 0,
        ),
        _case(
            "monday-friday-inclusive",
            ["2026-10-16 07:00:00"], ["2026-10-16 07:00:00"], (1, 1), 1,
            checked_at="2026-10-19 07:00:00", window_start="2026-10-16 07:00:00",
        ),
        _case(
            "monday-before-friday",
            ["2026-10-16 06:59:59"], ["2026-10-16 06:59:59"], (0, 0), 0,
            checked_at="2026-10-19 07:00:00", window_start="2026-10-16 07:00:00",
        ),
        _case(
            "monday-weekend-uploads",
            ["2026-10-17 12:00:00"], ["2026-10-18 10:00:00"], (1, 1), 1,
            checked_at="2026-10-19 07:00:00", window_start="2026-10-16 07:00:00",
        ),
        _case(
            "spring-dst-71-hours",
            ["2026-03-27 08:00:00"], ["2026-03-27 08:00:00"], (1, 1), 1,
            checked_at="2026-03-30 07:00:00", window_start="2026-03-27 08:00:00",
        ),
        _case(
            "spring-dst-before-lower",
            ["2026-03-27 07:59:59"], ["2026-03-27 07:59:59"], (0, 0), 0,
            checked_at="2026-03-30 07:00:00", window_start="2026-03-27 08:00:00",
        ),
        _case(
            "autumn-dst-73-hours",
            ["2026-10-23 07:00:00"], ["2026-10-23 07:00:00"], (1, 1), 1,
            checked_at="2026-10-26 08:00:00", window_start="2026-10-23 07:00:00",
        ),
        _case(
            "autumn-dst-before-lower",
            ["2026-10-23 06:59:59"], ["2026-10-23 06:59:59"], (0, 0), 0,
            checked_at="2026-10-26 08:00:00", window_start="2026-10-23 07:00:00",
        ),
        _case(
            "saturday-24-hours",
            ["2026-10-16 07:00:00"], ["2026-10-16 08:00:00"], (1, 1), 1,
            checked_at="2026-10-17 07:00:00", window_start="2026-10-16 07:00:00",
        ),
        _case(
            "sunday-not-friday",
            ["2026-10-16 07:00:00"], ["2026-10-16 08:00:00"], (0, 0), 0,
            checked_at="2026-10-18 07:00:00", window_start="2026-10-17 07:00:00",
        ),
        _case(
            "spring-sunday-24-elapsed-hours",
            ["2026-03-28 07:00:00"], ["2026-03-28 07:30:00"], (1, 1), 1,
            checked_at="2026-03-29 07:00:00", window_start="2026-03-28 07:00:00",
        ),
        _case(
            "autumn-sunday-24-elapsed-hours",
            ["2026-10-24 06:59:59"], ["2026-10-24 07:00:00"], (0, 1), 0,
            checked_at="2026-10-25 07:00:00", window_start="2026-10-24 07:00:00",
        ),
        _case(
            "berlin-monday-utc-sunday",
            ["2026-10-15 22:30:00"], ["2026-10-16 00:00:00"], (1, 1), 1,
            checked_at="2026-10-18 22:30:00", window_start="2026-10-15 22:30:00",
        ),
        _case(
            "berlin-tuesday-utc-monday",
            ["2026-10-18 22:30:00"], ["2026-10-19 00:00:00"], (1, 1), 1,
            checked_at="2026-10-19 22:30:00", window_start="2026-10-18 22:30:00",
        ),
        _case(
            "subsecond-boundaries",
            ["2026-10-19 06:59:59.999", "2026-10-19 07:00:00"],
            ["2026-10-20 07:00:00", "2026-10-20 07:00:00.001"],
            (1, 1), 1,
        ),
    ],
)
def test_freshness_decision(
    production_sql,
    warehouse_client,
    checked_at,
    prices,
    company,
    window_start,
    counts,
    decision,
):
    response = _execute(
        warehouse_client, _fixture_sql(production_sql, checked_at, prices, company)
    )
    assert response.manifest.total_row_count == 1
    columns = response.manifest.schema.columns
    assert [column.name for column in columns] == COLUMNS
    assert [column.type_text.lower() for column in columns[:4]] == ["timestamp_ntz"] * 4
    assert [column.type_name.value for column in columns[4:6]] == ["LONG", "LONG"]
    assert columns[-1].type_name.value == "INT"
    assert len(response.result.data_array) == 1
    values = response.result.data_array[0]
    assert len(values) == len(COLUMNS)
    row = dict(zip(COLUMNS, values))
    assert datetime.fromisoformat(row["checked_at"]) == datetime.fromisoformat(checked_at)
    assert datetime.fromisoformat(row["window_start"]) == datetime.fromisoformat(
        window_start
    )
    for source, timestamps in (("hourly_prices", prices), ("company_info", company)):
        latest = row[f"{source}_latest_modified_at"]
        if timestamps:
            assert datetime.fromisoformat(latest) == max(
                map(datetime.fromisoformat, timestamps)
            )
        else:
            assert latest is None
    assert int(row["hourly_prices_fresh_files"]) == counts[0]
    assert int(row["company_info_fresh_files"]) == counts[1]
    assert int(row["should_run_dbt"]) == decision


@pytest.mark.skipif(not WAREHOUSE_ID, reason="Requires an explicit test SQL warehouse")
@pytest.mark.parametrize(
    "empty_sources",
    [
        ("hourly_prices", "company_info"),
        ("hourly_prices",),
        ("company_info",),
    ],
    ids=["both-empty", "prices-empty", "company-empty"],
)
def test_actual_reader_no_matching_files(
    production_sql, warehouse_client, empty_sources
):
    prefix, inventory, suffix = _inventory_parts(production_sql)
    scans = inventory.split("  union all\n")
    assert len(scans) == 2
    for index, source in enumerate(("hourly_prices", "company_info")):
        if source in empty_sources:
            assert scans[index].count("fileNamePattern => '*.json'") == 1
            scans[index] = scans[index].replace(
                "fileNamePattern => '*.json'",
                "fileNamePattern => '__freshness_no_match_269368c8__.json'",
            )
    sql = (
        prefix + "file_inventory as (" + "  union all\n".join(scans)
        + "),\n\nsource_status as (" + suffix
    )
    response = _execute(warehouse_client, sql)
    assert response.manifest.total_row_count == 1
    assert [column.name for column in response.manifest.schema.columns] == COLUMNS
    assert len(response.result.data_array) == 1
    row = dict(zip(COLUMNS, response.result.data_array[0]))
    for source in empty_sources:
        assert int(row[f"{source}_fresh_files"]) == 0
        assert row[f"{source}_latest_modified_at"] is None
    assert int(row["should_run_dbt"]) == 0


@pytest.mark.skipif(not WAREHOUSE_ID, reason="Requires an explicit test SQL warehouse")
@pytest.mark.parametrize("source", ["hourly_prices", "company_info"])
def test_actual_reader_missing_path_fails(
    production_sql, warehouse_client, source
):
    path = f"/Volumes/api_data/raw/yfinance/{source}"
    assert production_sql.count(path) == 1
    sql = production_sql.replace(
        path, "/Volumes/api_data/raw/yfinance/__freshness_missing_269368c8__"
    )
    response = _execute(warehouse_client, sql, expected_state=StatementState.FAILED)
    assert response.status.error
    assert any(
        error_class in response.status.error.message
        for error_class in (
            "[PATH_NOT_FOUND]",
            "[CF_PATH_DOES_NOT_EXIST_FOR_READ_FILES]",
        )
    ), response.status.error


@pytest.mark.skipif(not WAREHOUSE_ID, reason="Requires an explicit test SQL warehouse")
def test_actual_reader_projects_metadata_only(production_sql, warehouse_client):
    response = _execute(warehouse_client, "explain formatted " + production_sql)
    plan = "\n".join(row[0] for row in response.result.data_array)
    schemas = [
        line.strip()
        for line in plan.splitlines()
        if "ReadSchema" in line
    ]
    assert sum(
        line.strip().endswith("Scan binaryFile") for line in plan.splitlines()
    ) == 2, plan
    assert len(schemas) == 2, plan
    assert schemas == ["ReadSchema: struct<modificationTime:timestamp>"] * 2
