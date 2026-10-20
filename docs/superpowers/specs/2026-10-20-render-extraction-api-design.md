# Render extraction API design

## Goal and scope

Expose the existing `python -m datasets.main` pipeline through an authenticated
FastAPI service on Render. The expected extraction duration is under one minute.
The trigger returns immediately; clients obtain completion information separately.

This design prepares application code, deployment configuration, documentation,
and tests. Deploying to a Render account is not part of implementation.
The existing hourly GitHub Actions workflow remains unchanged.

## Decisions and alternatives

Use a subprocess running the existing module rather than importing its entry
point into the API process. This preserves the command's extraction and upload
flow, isolates blocking work, and avoids constructing its Databricks client when
the API module is imported.

Alternatives considered:

- A shared extraction function called from a background thread provides richer
  structured results but changes more existing code and shares process state.
- A durable queue and separate worker support restart recovery and scaling but
  require infrastructure outside the accepted best-effort scope.

Use POST for the side-effecting trigger and GET for status. Do not add a GET
trigger, arbitrary command execution, ticker selection, or request options.

## Architecture and boundaries

The API owns authentication, a process-local run registry, and a single active-run
reservation. A background execution component owns subprocess creation,
monitoring, timeout enforcement, cleanup, and terminal state updates.
The extraction module continues to own fetching, local JSON output, and uploads.

Run exactly one Render instance and one Uvicorn worker. The run reservation must
be acquired atomically before returning acceptance, including while a run is
queued. It covers queued and running states and is released after subprocess
cleanup, including on launch failure or timeout. Do not implement it as an
unprotected check followed by a later background-task update.

The reservation does not coordinate with independent GitHub Actions executions.
Both entry points retain the existing file names and Databricks destinations;
concurrent GitHub/API runs can overwrite the same destination.

## HTTP contract

All response bodies are JSON. Run IDs are server-generated UUIDs. Timestamps are
UTC ISO 8601 strings. No endpoint exposes environment values, subprocess output,
exception text, or credentials.

### POST /run-extraction

Requires `Authorization: Bearer <EXTRACTION_API_TOKEN>`. There are no supported
request-body parameters.

After reserving the run slot and scheduling execution, return HTTP 202:

```json
{
  "run_id": "f06184b9-83c8-47f2-a780-16bfd41eabef",
  "status": "queued",
  "status_url": "/extractions/f06184b9-83c8-47f2-a780-16bfd41eabef"
}
```

This response confirms acceptance, not extraction or upload success.
If another run is queued or running, return HTTP 409 with a generic conflict
detail. Unauthorized requests must not create a run or reveal the active run.

### GET /extractions/{run_id}

Requires the same bearer token. Return HTTP 200 for a retained run:

```json
{
  "run_id": "f06184b9-83c8-47f2-a780-16bfd41eabef",
  "status": "running",
  "submitted_at": "2026-10-20T10:00:00+00:00",
  "started_at": "2026-10-20T10:00:00.100000+00:00",
  "finished_at": null,
  "error_code": null
}
```

Statuses are `queued`, `running`, `succeeded`, and `failed`. `started_at` is null
until execution is attempted. `finished_at` is null until a terminal state.
`error_code` is null except for failed runs, where it is one of
`launch_failed`, `process_failed`, `timeout`, or `shutdown`.
Unknown, malformed, expired, or pre-restart run IDs return HTTP 404.

Retain the most recent 100 finished runs, ordered by completion, plus the active
run. Evict older finished records; never evict an active record. The registry
has no persistence or cross-instance guarantees.

### GET /health

Public HTTP 200 liveness response, with no configuration or run details.
It does not contact Yahoo or Databricks, run extraction, or validate external
credentials. Extraction must not block health or status requests.

### Authentication and configuration failures

Missing, malformed, or invalid bearer credentials return HTTP 401 with
`WWW-Authenticate: Bearer`. Compare tokens using a constant-time comparison.
Perform authorization before accessing extraction state.

Validate nonempty `EXTRACTION_API_TOKEN`, `DATABRICKS_HOST`, and
`DATABRICKS_TOKEN` during application startup. Missing configuration fails startup
with variable names but no secret values, preventing acceptance of unusable runs.
External authentication and permissions are evaluated by the extraction process,
not by a startup network call.

## Execution lifecycle

1. Authenticate, reserve the slot, generate the ID, and register a queued run.
2. Schedule supervised background execution and return the acceptance response.
3. Mark the execution attempt running and set `started_at`.
4. Launch the current interpreter with `-m datasets.main` from the repository
   root using an argument list, not a shell command.
5. Inherit the service environment. Use unbuffered Python output so extraction
   logs reach Render promptly. Forward stdout and stderr to service logs rather
   than buffering them into run records or HTTP responses.
6. Wait asynchronously for completion, with a fixed five-minute execution limit.
   Exit zero means succeeded; nonzero means failed with `process_failed`.
   Failure to launch means failed with `launch_failed`.
7. On timeout, terminate the child, escalate to killing it if it does not exit
   promptly, and await it to avoid leaving an orphan or zombie. Record `timeout`.
8. On graceful service shutdown, stop accepting new work and cancel/supervise the
   active execution, terminate and reap its child, and mark it failed with
   `shutdown` while the process remains alive. Cleanup must have bounded waits.
9. Set the terminal timestamp, release the reservation, and enforce retention.

Associate lifecycle logs with the run ID. Record unexpected execution failures
in server logs and put the run into a failed state; never leave it indefinitely
running or convert an exception into a successful response.

The five-minute limit concerns execution, not waiting for a free slot: overlapping
requests are rejected rather than queued behind another run.

## Extraction success and error propagation

Currently, `upload_files_to_databricks` prints an error and suppresses exceptions.
Change this directly related behavior so failed uploads cause a nonzero CLI exit.
A missing local upload file must also raise an explicit error rather than skip.
Retain useful failure logging and propagate the error.

Both expected uploads must complete for the subprocess to exit successfully.
This improves failure reporting for both the API and the unchanged GitHub
workflow. Do not introduce transactional uploads, rollback, or automatic retries:
one file might already have been uploaded when the other fails.

Preserve existing per-ticker error records and symbol-fetch fallback behavior.
`succeeded` means the existing pipeline completed and both files uploaded, not
that all ticker records contain valid prices. Do not parse stdout to infer
business-level success.

## Render configuration

Add a root `render.yaml` Blueprint for a single native Python web service:

- Initially use Render's Free compute plan.
- Select Python 3.13 to match `pyproject.toml`; use a Render-supported explicit
  patch version in deployment configuration.
- Build with `pip install -r requirements.txt`.
- Start Uvicorn for the API module on `0.0.0.0:$PORT` with exactly one worker and
  without development reload.
- Configure `/health` as the health-check path.
- Declare `EXTRACTION_API_TOKEN`, `DATABRICKS_HOST`, and `DATABRICKS_TOKEN` as
  dashboard-supplied values with no committed credentials.

Add compatible FastAPI and Uvicorn dependencies to both `requirements.txt` and
`pyproject.toml`, following their existing dependency conventions. Keep unrelated
dbt dependency differences unchanged. Add API test dependencies only to the
existing development dependency mechanism where needed.

Render injects environment variables into the application and child process;
no custom environment parser or committed `.env` file is required. Existing
`load_dotenv()` behavior must not override injected values.

The Blueprint can be deployed from the feature branch through Render's
repository/branch selection. Do not hardcode a feature branch into permanent
configuration that should also work after merge.

## Reliability and storage limits

Accepted runs are best-effort. There is no durable queue, job database, automatic
retry, restart recovery, cross-instance locking, or exactly-once guarantee.
Deployments, crashes, and forced shutdowns can interrupt runs and erase history.
After restart, old run IDs return 404.

Render Free services sleep after 15 minutes without inbound traffic, have cold
starts, and may restart. Background work does not establish durability or keep
the service awake. Clients must account for cold starts and must not blindly
retry ambiguous trigger requests, which could create another run after the first
finishes.

Local JSON output and symbol caches remain temporary on Render's ephemeral
filesystem. Databricks uploads are the durable output. No persistent disk is
required or promised by this design.

## Documentation

Update the README with:

- Local installation and single-worker startup instructions.
- Render Blueprint setup, feature-branch selection, Python runtime, build/start
  commands, and health-check configuration.
- Dashboard environment-variable entry or import, with placeholder values only
  and a warning not to commit secret-bearing files.
- Authenticated POST and status-polling examples, plus the meaning of HTTP 202,
  401, 404, and 409.
- Lifecycle states, error codes, timeout, retention, partial-output behavior,
  and the distinction between pipeline success and per-ticker success.
- Free-plan cold starts, interrupted jobs, erased history, ephemeral local
  files, and the requirement for a single worker and instance.
- Confirmation that the existing GitHub schedule remains active and independent.

## Verification

Use existing pytest patterns and mocked subprocesses/network clients. Tests must
not contact Yahoo or Databricks or require real credentials.

Cover accepted requests returning before child completion; bearer-token
enforcement for trigger and status; missing startup configuration; atomic
overlap rejection including queued work; successful and failed status transitions;
launch failures; timeout and shutdown termination/reaping; run-slot release after
failures; and bounded retention with active-run preservation.

Add focused extraction tests proving successful uploads still work while missing
files and upload errors propagate to callers, allowing CLI failure. Keep the
existing extraction tests passing. Use a lightweight health/configuration check
to ensure the deployment entry point is importable and consistent with the
single-worker startup command.

## Reference documentation

- Render web services: https://render.com/docs/web-services
- Render Free service limitations: https://render.com/docs/free

These references establish public reachability, `0.0.0.0` and `PORT` binding,
dashboard environment configuration, idle spin-down, and ephemeral storage.
