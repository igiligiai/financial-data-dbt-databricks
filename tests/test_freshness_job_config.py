import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_dbt_tasks_are_guarded_by_sql_result():
    config = yaml.safe_load(
        (ROOT / "resources" / "financial_data_dbt_databricks.job.yml").read_text()
    )
    job = config["resources"]["jobs"]["financial_data_dbt_databricks_job"]
    tasks = {task["task_key"]: task for task in job["tasks"]}
    check = tasks["check_raw_file_freshness"]
    assert check.get("depends_on", []) == []
    assert check["sql_task"] == {
        "warehouse_id": "${var.sql_warehouse_id}",
        "file": {
            "path": "../src/sql/check_raw_file_freshness.sql",
            "source": "WORKSPACE",
        },
    }
    condition = tasks["raw_files_are_fresh"]
    assert condition["depends_on"] == [{"task_key": "check_raw_file_freshness"}]
    assert condition["condition_task"] == {
        "op": "EQUAL_TO",
        "left": "{{tasks.check_raw_file_freshness.output.first_row.should_run_dbt}}",
        "right": "1",
    }
    assert tasks["dbt_run"]["depends_on"] == [
        {"task_key": "raw_files_are_fresh", "outcome": "true"}
    ]
    assert tasks["dbt_test"]["depends_on"] == [{"task_key": "dbt_run"}]
    for key in ("raw_files_are_fresh", "dbt_run", "dbt_test"):
        assert tasks[key].get("run_if", "ALL_SUCCESS") == "ALL_SUCCESS"
    variables = yaml.safe_load((ROOT / "variables.yml").read_text())["variables"]
    warehouse_id = variables["sql_warehouse_id"]["default"]
    profiles = yaml.safe_load((ROOT / "dbt_profiles" / "profiles.yml").read_text())
    outputs = profiles["financial_data_dbt_databricks"]["outputs"]
    for target in ("dev", "prod"):
        assert outputs[target]["http_path"].endswith(f"/{warehouse_id}")


def test_freshness_inventory_matches_dbt_source_paths():
    sources = yaml.safe_load(
        (ROOT / "src" / "models" / "sources" / "_sources.yml").read_text()
    )["sources"]
    source = next(item for item in sources if item["name"] == "yfinance_raw")
    expected = {
        table["config"]["meta"]["volume_path"]
        for table in source["tables"]
        if table["name"] in ("hourly_prices", "company_info")
    }
    sql = (ROOT / "src" / "sql" / "check_raw_file_freshness.sql").read_text()
    actual = set(re.findall(r"read_files\(\s*'([^']+)'", sql))
    assert len(expected) == 2
    assert actual == expected
