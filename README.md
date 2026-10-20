# financial_data_dbt_databricks

A financial-data pipeline that collects NASDAQ-100 stock snapshots and company
fundamentals from Yahoo Finance, stores raw JSON in Databricks Unity Catalog
Volumes, and builds analytics with dbt on Databricks. Extraction runs on a GitHub
Actions schedule or on demand through an authenticated FastAPI service prepared
for deployment to Render.

## Data pipeline

1. **Extract:** `datasets\main.py` uses yfinance to fetch price snapshots and
   company profiles. NASDAQ-100 symbols come from Nasdaq, with a seven-day local
   cache and stale-cache/static-list fallbacks.
2. **Store:** Timestamped price files and an overwritten `company_info.json` are
   uploaded under `/Volumes/api_data/raw/yfinance`. Missing files and upload
   failures cause the extraction command to fail.
3. **Transform:** Bronze views read raw JSON; silver models type and clean the
   data, with price snapshots incrementally merged by symbol and extraction
   time; gold views expose market summaries and stock analytics.
4. **Orchestrate:** Databricks jobs check raw-file freshness before running dbt
   and its data tests. A separate weekly job performs table maintenance.

Gold analytics include sector performance, last-trading-day gainers, 21-day
decliners and volatility, volume anomalies, moving-average trend signals, and
52-week range positioning.

## Project layout

| Path | Purpose |
| --- | --- |
| `datasets\` | Extraction CLI, FastAPI app, subprocess supervisor, and unit tests |
| `src\models\` | Bronze, silver, and gold dbt models and schema tests |
| `src\analyses\` | Analytical SQL, including sector valuation comparisons |
| `src\sql\` | Raw-file freshness gate |
| `src\macros\` | Source-volume helpers and weekly optimization |
| `src\tests\` | dbt SQL data tests |
| `resources\` | Main transformation and weekly maintenance jobs |
| `tests\` | Python configuration checks and opt-in warehouse tests |
| `.github\workflows\` | Scheduled extraction and build/deployment workflows |
| `databricks.yml`, `dbt_profiles\` | Bundle targets and deployed dbt profiles |
| `render.yaml` | Single-instance extraction API Blueprint |

## Local Python setup and extraction

Use Python 3.13. From the repository root in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
$env:DATABRICKS_HOST = "https://<workspace-host>"
$env:DATABRICKS_TOKEN = "<databricks-token>"
.\.venv\Scripts\python.exe -m datasets.main
```

The editable install includes dbt, the extraction/API dependencies, and HTTP test
clients. `requirements.txt` contains the smaller runtime dependency set used by
the extraction workflow and Render. Databricks credentials must allow writes to
the configured raw volume; running extraction contacts external services and
uploads data. Local JSON and the symbol cache are written to `datasets\output`.
Keep real credentials out of Git.

## dbt development and Databricks deployment

The project originated from the Databricks Asset Bundles dbt template. Models,
macros, analyses, and SQL data tests live under `src\`, as configured in
`dbt_project.yml`.

For local dbt commands, activate the environment created above and configure
the `financial_data_dbt_databricks` profile in your user-level
`~\.dbt\profiles.yml` with your workspace, SQL warehouse, catalog, schema, and
credentials. `dbt init` can guide profile setup.

```powershell
.\.venv\Scripts\Activate.ps1
dbt deps
dbt run
dbt test
dbt run --select silver_hourly_prices
```

`dbt test` runs model/schema checks and SQL tests from `src\tests`; the Python
tests under `tests\` are run with pytest instead. Local dbt commands require
Databricks access and do not run the job's raw-file freshness gate.

Install and authenticate the
[Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
before deploying the bundle:

```powershell
databricks configure
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle deploy --target prod
```

Review workspace and permission settings in `databricks.yml` before deployment.
The configured targets are `dev` (default, development mode with schedules
paused) and `prod` (production mode with schedules enabled). Deployed dbt jobs
use `dbt_profiles\profiles.yml`, with `workspace.default` for dev and
`workspace.finance` for prod; Databricks supplies `DBT_HOST` and
`DBT_ACCESS_TOKEN` at runtime. The main job is scheduled at 09:00 on weekdays,
and weekly maintenance at 06:00 on Mondays, both in `Europe/Berlin`.

See the [dbt project documentation](https://docs.getdbt.com/docs/build/projects)
and [Databricks Asset Bundles documentation](https://docs.databricks.com/en/dev-tools/bundles/index.html)
for general configuration guidance.

## Raw-file freshness gate

Before running dbt, the main Databricks job checks JSON file modification times
in both raw yfinance inputs (`hourly_prices` and `company_info`). Each must have
at least one file modified in the preceding **24 hours**. On Monday, the window
starts at the previous Friday's same local time in `Europe/Berlin`, including
weekend uploads and accounting for daylight-saving changes.

Stale or empty inputs **skip both dbt tasks without failing the job**.
Missing paths, permission errors, and SQL failures still fail the job.
This checks file arrival, including overwrites, not payload contents or
`extracted_at`. Ingestion, schedules, and weekly maintenance are unchanged;
delayed uploads do not trigger an automatic retry.

GitHub ingestion is scheduled at minute 17 of each hour from 09:17 through 22:17
on weekdays in `Europe/Berlin`, avoiding the busiest start-of-hour period.
Scheduling remains best-effort: runs can still be delayed or dropped.

The check uses the `sql_warehouse_id` bundle variable, defaulting to the existing
dbt warehouse. Inspect `check_raw_file_freshness` for UTC window boundaries,
latest modification times, eligible-file counts, and `should_run_dbt`.
Warehouse tests in `tests/test_raw_file_freshness.py` require authenticated access
and explicit opt-in via `FRESHNESS_TEST_WAREHOUSE_ID`; otherwise they are skipped.

## On-demand extraction API on Render

The API starts the same `python -m datasets.main` pipeline as GitHub Actions.
It fetches the default stock universe and uploads both JSON outputs to Databricks.
The existing hourly GitHub Actions workflow stays active and independent.

### Local startup

Use Python 3.13 and a virtual environment. From the repository root in PowerShell:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:EXTRACTION_API_TOKEN = "<separate-api-token>"
$env:DATABRICKS_HOST = "https://<workspace-host>"
$env:DATABRICKS_TOKEN = "<databricks-token>"
.\.venv\Scripts\python.exe -m uvicorn datasets.api:app --host 0.0.0.0 --port 8000 --workers 1
```

Values above are placeholders. Do not commit real tokens or secret-bearing files.
The server requires all three variables at startup. The API token is separate
from the Databricks token. Databricks authentication and volume permissions are
checked by extraction, not by the health endpoint.

### Deploy to Render

Create a Blueprint from this repository in Render and select the branch you want
to deploy (the feature branch before merge). Render reads the root `render.yaml`.
Supply `EXTRACTION_API_TOKEN`, `DATABRICKS_HOST`, and `DATABRICKS_TOKEN` when
prompted. Render also supports importing environment values from a local
secret-bearing file through its dashboard; do not add that file to Git.

The Blueprint selects native Python 3.13.16 and the Free plan, installs
`requirements.txt`, and uses this Linux start command:

```sh
python -m uvicorn datasets.api:app --host 0.0.0.0 --port $PORT --workers 1
```

The service must use one instance and one worker. Do not add worker processes or
autoscaling: admission and history are process-local. Configure `/health` as the
health check. Render injects environment variables directly; no custom parser is
needed. Existing dotenv loading does not override these injected values.

### Trigger and inspect a run

From PowerShell, with the same API token configured in your local environment:

```powershell
$base = "https://<service-name>.onrender.com"
$headers = @{ Authorization = "Bearer $env:EXTRACTION_API_TOKEN" }
$run = Invoke-RestMethod -Method Post -Uri "$base/run-extraction" -Headers $headers
$run
Invoke-RestMethod -Method Get -Uri "$base$($run.status_url)" -Headers $headers
```

The POST responds immediately with HTTP 202 and `run_id`, `status: queued`, and
`status_url`. This means accepted, not completed. Poll the status URL for
`queued`, `running`, `succeeded`, or `failed`, plus UTC `submitted_at`,
`started_at`, `finished_at`, and `error_code` fields. Unset timestamps and error
codes are JSON null. HTTP 401 means invalid/missing bearer authorization,
409 means another extraction is active, and 404 means an unknown, expired, or
pre-restart ID. HTTP 503 can indicate the runner is shutting down or cannot
safely accept another job after a cleanup failure.

Execution has a five-minute timeout. Failure codes are `launch_failed`,
`process_failed`, `timeout`, and `shutdown`. Details and child output go to
Render logs, not HTTP responses. Failed uploads cause the run to fail; uploads
are not transactional, so one output may already exist when another fails.
Success means both uploads completed, not that every ticker lookup succeeded:
the extractor retains its existing per-ticker error records and symbol fallbacks.

### Best-effort limits

History keeps the latest 100 finished runs and the active run in memory.
Deploys/restarts erase history and can interrupt work. There is no durable queue,
restart recovery, automatic retry, or exactly-once guarantee. Do not blindly retry
an ambiguous POST: it may already have started an extraction.

Free services sleep after 15 minutes without inbound traffic, have cold starts,
and may restart. Background work does not keep them awake. Local JSON files and
symbol caches are ephemeral; Databricks uploads are the durable output.
The API lock does not cover GitHub Actions, whose runs can overlap API runs and
overwrite the same output paths.

For local API tests, install the project test extra and run:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
.\.venv\Scripts\python.exe -m pytest datasets\tests tests\test_render_config.py -q
```
