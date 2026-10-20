# Render Extraction API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an authenticated FastAPI trigger for the existing extraction command and prepare a single-instance Render deployment.

**Architecture:** Run the unchanged CLI entry point in a supervised subprocess, except for correcting suppressed upload errors. FastAPI owns authentication and HTTP responses; an event-loop-confined runner owns subprocess lifecycle, single-run admission, and bounded in-memory history. Accepted runs are best-effort, not durable jobs.

**Tech Stack:** Python 3.13, FastAPI, Uvicorn, asyncio subprocesses, pytest, HTTPX, Render native Python Blueprint.

## Global Constraints

- Approved spec: `docs\superpowers\specs\2026-10-20-render-extraction-api-design.md`.
- Run exactly one Render instance and one Uvicorn worker.
- Use POST for the side-effecting trigger and GET for status.
- Require `Authorization: Bearer <EXTRACTION_API_TOKEN>`.
- Validate nonempty `EXTRACTION_API_TOKEN`, `DATABRICKS_HOST`, and `DATABRICKS_TOKEN` during application startup.
- The existing hourly GitHub Actions workflow remains unchanged.
- Retain the most recent 100 finished runs, ordered by completion, plus the active run.
- Wait asynchronously for completion, with a fixed five-minute execution limit.
- Unknown, malformed, expired, or pre-restart run IDs return HTTP 404.
- Initially use Render's Free compute plan.
- No persistent disk, durable queue, automatic retry, arbitrary commands, or custom ticker input.
- Tests must not contact Yahoo or Databricks or require real credentials.
- Use Windows paths for local commands; Render's start command executes on Linux.
- Stage explicit paths only. Include `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>` on every commit.

---

## File map and task order

| File | Responsibility |
| --- | --- |
| `datasets\main.py` | Existing extraction CLI; propagate missing-file/upload failures. |
| `datasets\tests\test_uploads.py` | Mocked upload success and failure regression tests. |
| `datasets\extraction_runner.py` | Run records, admission, subprocess lifecycle, retention, shutdown. |
| `datasets\tests\test_extraction_runner.py` | Async lifecycle tests using `asyncio.run` and fake processes. |
| `datasets\api.py` | App factory, lifespan validation, bearer auth, HTTP contract. |
| `datasets\tests\test_api.py` | Authenticated HTTP contract with mocked process creation. |
| `requirements.txt` | Runtime FastAPI/Uvicorn dependencies used by Render. |
| `pyproject.toml` | Matching runtime dependencies and HTTPX test extra. |
| `render.yaml` | Single-worker Render service and dashboard-supplied variables. |
| `tests\test_render_config.py` | Deployment entry point and Blueprint regression checks. |
| `README.md` | Local/API/Render operational instructions and limits. |

Tasks 1 and 2 are independent foundations. Task 3 consumes Task 2. Task 4 consumes
Task 3. Do not modify the ignored `uv.lock`, existing Python version file,
Databricks jobs, dbt resources, or GitHub workflow.

## Task 1: Make CLI upload failures observable

**Files:** Modify `datasets\main.py` (`upload_files_to_databricks`); create `datasets\tests\test_uploads.py`.

**Interfaces:**
- Consumes: existing module-global `WS` and `upload_files_to_databricks(local_path: Path, databricks_path: str)`.
- Produces: same public signature; missing files raise `FileNotFoundError`, SDK errors propagate, success returns `None`.

- [ ] **Step 1: Add regression tests.**

Create `datasets\tests\test_uploads.py`:

```python
import importlib
import io
from unittest.mock import Mock

import pytest
import databricks.sdk


@pytest.fixture
def uploader(monkeypatch):
    client = Mock()
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda: client)
    module = importlib.import_module("datasets.main")
    monkeypatch.setattr(module, "WS", client)
    return module, client


def test_upload_preserves_bytes_and_destination(tmp_path, uploader):
    module, client = uploader
    source = tmp_path / "prices.json"
    source.write_bytes(b'[{"symbol":"AAPL"}]')
    module.upload_files_to_databricks(source, "/Volumes/test/prices.json")
    args, kwargs = client.files.upload.call_args
    assert args[0] == "/Volumes/test/prices.json"
    assert isinstance(args[1], io.BytesIO)
    assert args[1].getvalue() == source.read_bytes()
    assert kwargs == {"overwrite": True}


def test_upload_error_propagates(tmp_path, uploader):
    module, client = uploader
    source = tmp_path / "prices.json"
    source.write_text("[]")
    client.files.upload.side_effect = RuntimeError("upload rejected")
    with pytest.raises(RuntimeError, match="upload rejected"):
        module.upload_files_to_databricks(source, "/Volumes/test/prices.json")


def test_missing_file_fails_without_calling_sdk(tmp_path, uploader):
    module, client = uploader
    with pytest.raises(FileNotFoundError):
        module.upload_files_to_databricks(
            tmp_path / "missing.json", "/Volumes/test/prices.json"
        )
    client.files.upload.assert_not_called()
```

- [ ] **Step 2: Run the failing cases.**

Run `python -m pytest datasets\tests\test_uploads.py -q`.
Expected: the missing-file and upload-error cases fail because errors are suppressed.
Use the existing virtual environment if the system interpreter is not the project
interpreter. Install missing tools only after a missing-dependency failure.

- [ ] **Step 3: Replace only the upload helper.**

```python
def upload_files_to_databricks(local_path: Path, databricks_path: str):
    """Upload a local file; propagate failures to the CLI exit status."""
    if not local_path.exists():
        raise FileNotFoundError(f"Upload source does not exist: {local_path}")
    logger.info("Uploading %s to %s", local_path, databricks_path)
    with local_path.open("rb") as source:
        binary_data = io.BytesIO(source.read())
    WS.files.upload(databricks_path, binary_data, overwrite=True)
    logger.info("Successfully uploaded %s to %s", local_path, databricks_path)
```

The existing uncaught exception at the module entry point produces a nonzero exit
and traceback. Do not alter extraction fields, symbol handling, or upload paths.

- [ ] **Step 4: Run targeted regressions.**

Run `python -m pytest datasets\tests\test_uploads.py datasets\tests\extractor_test.py -q`.
Expected: all selected tests pass without network access.

- [ ] **Step 5: Commit this independently useful correction.**

```powershell
git add -- datasets\main.py datasets\tests\test_uploads.py
git commit -m "fix: propagate extraction upload failures" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

## Task 2: Supervise one extraction subprocess

**Files:** Create `datasets\extraction_runner.py`, `datasets\tests\test_extraction_runner.py`.

**Interfaces:**
- Consumes: `asyncio.create_subprocess_exec`, the current interpreter, and repository-root working directory.
- Produces: `ExtractionRunner.submit() -> RunRecord`, `get(run_id: str) -> RunRecord | None`, `async shutdown() -> None`; `RunBusy` and `RunnerClosed` admission errors.
- All runner methods execute on the app event loop. `submit` contains no await,
  making reservation atomic without thread locks. HTTP handlers must remain async.

- [ ] **Step 1: Create the runner tests and reusable fake process.**

Create `datasets\tests\test_extraction_runner.py`:

```python
import asyncio
import sys

import pytest

import datasets.extraction_runner as runner_module
from datasets.extraction_runner import ExtractionRunner, RunBusy


class FakeProcess:
    def __init__(self, code=0, ignore_terminate=False):
        self.returncode = None
        self.code = code
        self.ignore_terminate = ignore_terminate
        self.done = asyncio.Event()
        self.terminated = False
        self.killed = False
        self.wait_count = 0

    async def wait(self):
        self.wait_count += 1
        await self.done.wait()
        return self.returncode

    def finish(self):
        self.returncode = self.code
        self.done.set()

    def terminate(self):
        self.terminated = True
        if not self.ignore_terminate:
            self.code = -15
            self.finish()

    def kill(self):
        self.killed = True
        self.code = -9
        self.finish()


async def settle(runner, run_id):
    for _ in range(100):
        record = runner.get(run_id)
        if record.status in ("succeeded", "failed"):
            return record
        await asyncio.sleep(0)
    raise AssertionError("run did not reach a terminal state")


def test_atomic_admission_success_and_fixed_command(monkeypatch):
    async def scenario():
        process = FakeProcess()
        calls = []

        async def spawn(*args, **kwargs):
            calls.append((args, kwargs))
            return process

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        runner = ExtractionRunner()
        first = runner.submit()
        assert first.status == "queued"
        with pytest.raises(RunBusy):
            runner.submit()
        for _ in range(10):
            await asyncio.sleep(0)
        assert runner.get(first.run_id).status == "running"
        assert calls[0][0] == (sys.executable, "-u", "-m", "datasets.main")
        assert calls[0][1] == {"cwd": runner_module.ROOT}
        process.finish()
        record = await settle(runner, first.run_id)
        assert record.status == "succeeded"
        assert record.started_at and record.finished_at
        assert record.error_code is None
        await runner.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "mode, expected",
    [("nonzero", "process_failed"), ("launch", "launch_failed"),
     ("unexpected", "process_failed"), ("timeout", "timeout")],
)
def test_failures_release_slot(monkeypatch, mode, expected):
    async def scenario():
        process = FakeProcess(code=1)
        if mode == "nonzero":
            process.finish()

        async def spawn(*args, **kwargs):
            if mode == "launch":
                raise OSError("cannot launch")
            if mode == "unexpected":
                raise RuntimeError("unexpected supervisor error")
            return process

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        monkeypatch.setattr(runner_module, "EXECUTION_TIMEOUT", 0.001)
        runner = ExtractionRunner()
        first = runner.submit()
        if mode == "timeout":
            await asyncio.sleep(0.02)
        record = await settle(runner, first.run_id)
        assert record.status == "failed"
        assert record.error_code == expected
        assert record.finished_at
        if mode == "timeout":
            assert process.terminated
            assert process.returncode is not None
        assert runner.submit().run_id != first.run_id
        await runner.shutdown()

    asyncio.run(scenario())


def test_shutdown_cleans_child_even_during_launch(monkeypatch):
    async def scenario():
        process = FakeProcess(ignore_terminate=True)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def spawn(*args, **kwargs):
            entered.set()
            await release.wait()
            return process

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        monkeypatch.setattr(runner_module, "STOP_GRACE", 0.001)
        runner = ExtractionRunner()
        record = runner.submit()
        await entered.wait()
        shutdown = asyncio.create_task(runner.shutdown())
        await asyncio.sleep(0)
        release.set()
        await shutdown
        assert process.terminated and process.killed
        assert process.returncode is not None
        assert process.wait_count >= 2
        assert runner.get(record.run_id).error_code == "shutdown"

    asyncio.run(scenario())


def test_retention_and_snapshot_isolation(monkeypatch):
    async def scenario():
        processes = []

        async def spawn(*args, **kwargs):
            process = FakeProcess()
            processes.append(process)
            if len(processes) <= 101:
                process.finish()
            return process

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        runner = ExtractionRunner()
        ids = []
        for _ in range(101):
            record = runner.submit()
            ids.append(record.run_id)
            await settle(runner, record.run_id)
        assert runner.get(ids[0]) is None
        assert all(runner.get(run_id) for run_id in ids[1:])
        active = runner.submit()
        assert runner.get(active.run_id).status == "queued"
        active.status = "failed"
        assert runner.get(active.run_id).status == "queued"
        assert runner.get("not-a-run") is None
        await runner.shutdown()

    asyncio.run(scenario())
```

- [ ] **Step 2: Confirm tests fail before implementation.**

Run `python -m pytest datasets\tests\test_extraction_runner.py -q`.
Expected: collection fails because `datasets.extraction_runner` does not exist.

- [ ] **Step 3: Implement the event-loop-confined runner.**

Create `datasets\extraction_runner.py`:

```python
import asyncio
import logging
import sys
from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from uuid import uuid4


logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[1]
EXECUTION_TIMEOUT = 300
STOP_GRACE = 5
HISTORY_LIMIT = 100
Status = Literal["queued", "running", "succeeded", "failed"]
ErrorCode = Literal["launch_failed", "process_failed", "timeout", "shutdown"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RunRecord:
    run_id: str
    status: Status
    submitted_at: str
    started_at: str | None = None
    finished_at: str | None = None
    error_code: ErrorCode | None = None


class RunBusy(Exception):
    pass


class RunnerClosed(Exception):
    pass


class ExtractionRunner:
    def __init__(self):
        self._records: OrderedDict[str, RunRecord] = OrderedDict()
        self._active: str | None = None
        self._task: asyncio.Task[None] | None = None
        self._closing = False

    def submit(self) -> RunRecord:
        if self._closing:
            raise RunnerClosed()
        if self._active is not None:
            raise RunBusy()
        record = RunRecord(str(uuid4()), "queued", utc_now())
        self._records[record.run_id] = record
        self._active = record.run_id
        self._task = asyncio.create_task(self._run(record))
        logger.info("Extraction %s accepted", record.run_id)
        return replace(record)

    def get(self, run_id: str) -> RunRecord | None:
        record = self._records.get(run_id)
        return replace(record) if record is not None else None

    async def shutdown(self) -> None:
        self._closing = True
        task = self._task
        if task is not None and not task.done():
            # Let a newly scheduled supervisor enter its cancellation handler.
            await asyncio.sleep(0)
            if not task.done():
                task.cancel()
            await task

    async def _stop(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.wait(), STOP_GRACE)
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await asyncio.wait_for(process.wait(), STOP_GRACE)

    async def _run(self, record: RunRecord) -> None:
        process = None
        launch = None
        record.status = "running"
        record.started_at = utc_now()
        logger.info("Extraction %s started", record.run_id)
        try:
            launch = asyncio.create_task(asyncio.create_subprocess_exec(
                sys.executable, "-u", "-m", "datasets.main", cwd=ROOT
            ))
            # Preserve the handle if shutdown races subprocess creation.
            process = await asyncio.shield(launch)
            code = await asyncio.wait_for(process.wait(), EXECUTION_TIMEOUT)
            if code != 0:
                record.error_code = "process_failed"
                logger.error("Extraction %s exited with %s", record.run_id, code)
        except TimeoutError:
            record.error_code = "timeout"
            logger.error("Extraction %s timed out", record.run_id)
        except asyncio.CancelledError:
            record.error_code = "shutdown"
            logger.warning("Extraction %s interrupted by shutdown", record.run_id)
            if process is None and launch is not None:
                try:
                    process = await asyncio.wait_for(
                        asyncio.shield(launch), STOP_GRACE
                    )
                except (OSError, TimeoutError):
                    logger.exception("Extraction %s launch cleanup failed", record.run_id)
                    self._closing = True
        except OSError:
            record.error_code = "launch_failed"
            logger.exception("Extraction %s could not launch", record.run_id)
        except Exception:
            record.error_code = "process_failed"
            logger.exception("Extraction %s supervisor failed", record.run_id)
        finally:
            try:
                if process is not None:
                    await self._stop(process)
            except (OSError, TimeoutError):
                self._closing = True
                record.error_code = record.error_code or "process_failed"
                logger.exception("Extraction %s child cleanup failed", record.run_id)
            record.status = "failed" if record.error_code else "succeeded"
            record.finished_at = utc_now()
            self._active = None
            self._records.move_to_end(record.run_id)
            while len(self._records) > HISTORY_LIMIT:
                self._records.popitem(last=False)
            logger.info("Extraction %s %s", record.run_id, record.status)
```

The explicit unexpected-error handler reports failure rather than swallowing it.
Inherited stdout/stderr stream directly to Render without pipes or unbounded
buffers. Do not add stdout parsing. Cleanup failure closes admission instead of
starting another potentially overlapping process.

- [ ] **Step 4: Exercise lifecycle cases.**

Run `python -m pytest datasets\tests\test_extraction_runner.py -q`.
Expected: all cases pass, including cancellation during launch, terminate-to-kill
escalation, and 101 completed runs plus an active record.

Before considering this task complete, resolve the launch-cleanup timeout case:
if `launch` outlives its cleanup deadline, attach a supervised cleanup coroutine
that obtains the resulting process and terminates/reaps it; keep admission closed
and log this exceptional condition. Do not abandon an in-flight launch task.
Add the following regression and helper before the final run:

```python
# Add to ExtractionRunner.
async def _finish_late_launch(self, launch, run_id: str) -> None:
    try:
        process = await launch
        await self._stop(process)
    except Exception:
        logger.exception("Extraction %s late launch cleanup failed", run_id)
```

Keep late cleanup tasks in a `set[asyncio.Task[None]]` on the runner so they remain
strongly referenced. The timeout branch creates the task and registers its removal
callback:

```python
# Initialize in __init__:
self._cleanup_tasks: set[asyncio.Task[None]] = set()

# Use separate except clauses around the launch cleanup await:
except TimeoutError:
    self._closing = True
    logger.exception("Extraction %s launch cleanup timed out", record.run_id)
    cleanup = asyncio.create_task(
        self._finish_late_launch(launch, record.run_id)
    )
    self._cleanup_tasks.add(cleanup)
    cleanup.add_done_callback(self._cleanup_tasks.discard)
except OSError:
    self._closing = True
    logger.exception("Extraction %s launch cleanup failed", record.run_id)
```

```python
def test_delayed_launch_is_eventually_reaped(monkeypatch):
    async def scenario():
        process = FakeProcess()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def spawn(*args, **kwargs):
            entered.set()
            await release.wait()
            return process

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        monkeypatch.setattr(runner_module, "STOP_GRACE", 0.001)
        runner = ExtractionRunner()
        record = runner.submit()
        await entered.wait()
        await runner.shutdown()
        assert runner.get(record.run_id).error_code == "shutdown"
        from datasets.extraction_runner import RunnerClosed
        with pytest.raises(RunnerClosed):
            runner.submit()
        release.set()
        for _ in range(100):
            if process.returncode is not None:
                break
            await asyncio.sleep(0)
        assert process.terminated
        assert process.returncode is not None

    asyncio.run(scenario())
```

This is bounded best-effort shutdown: forced process exit can still interrupt
late cleanup, consistent with the spec's crash/restart limitations.

- [ ] **Step 5: Commit the runner and its tests.**

```powershell
git add -- datasets\extraction_runner.py datasets\tests\test_extraction_runner.py
git commit -m "feat: supervise single extraction subprocess" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

## Task 3: Expose the authenticated FastAPI contract

**Files:** Create `datasets\api.py`, `datasets\tests\test_api.py`; modify `requirements.txt`, `pyproject.toml`.

**Interfaces:**
- Consumes: Task 2's `ExtractionRunner`, `RunRecord`, `RunBusy`, `RunnerClosed`.
- Produces: `datasets.api:app` and `create_app() -> FastAPI`.
- HTTP handlers are async and use the runner only on the app event loop.

- [ ] **Step 1: Add runtime dependencies and the test extra.**

Append these lines to `requirements.txt`, and the same requirement strings to
`project.dependencies` in `pyproject.toml`:

```text
fastapi>=0.115.0,<1.0.0
uvicorn>=0.30.0,<1.0.0
```

Append this section to `pyproject.toml`:

```toml
[project.optional-dependencies]
test = [
    "httpx>=0.27.0,<1.0.0",
]
```

Then run `python -m pip install -e ".[test]"`.
Expected: successful dependency resolution with existing dbt/Databricks bounds.
Do not commit generated packaging artifacts or the ignored lockfile.

- [ ] **Step 2: Add API tests before creating the API module.**

Create `datasets\tests\test_api.py`:

```python
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from datasets.api import create_app
import datasets.extraction_runner as runner_module
from datasets.tests.test_extraction_runner import FakeProcess


AUTH = {"Authorization": "Bearer test-extraction-token"}


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("EXTRACTION_API_TOKEN", "test-extraction-token")
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.invalid")
    monkeypatch.setenv("DATABRICKS_TOKEN", "test-databricks-token")
    monkeypatch.setattr(
        runner_module.asyncio, "create_subprocess_exec",
        AsyncMock(side_effect=lambda *args, **kwargs: FakeProcess()),
    )
    return create_app()


@pytest.mark.parametrize("header", [{}, {"Authorization": "Basic abc"},
    {"Authorization": "Bearer wrong"}, {"Authorization": "Bearer"}])
def test_protected_routes_reject_bad_credentials(app, header):
    with TestClient(app) as client:
        for method, path in [
            ("post", "/run-extraction"), ("get", "/extractions/unknown")
        ]:
            response = getattr(client, method)(path, headers=header)
            assert response.status_code == 401
            assert response.headers["www-authenticate"] == "Bearer"
        assert client.get("/health").json() == {"status": "ok"}
        assert client.post("/run-extraction", headers=AUTH).status_code == 202


def test_acceptance_conflict_and_status(app):
    with TestClient(app) as client:
        response = client.post("/run-extraction", headers=AUTH)
        assert response.status_code == 202
        accepted = response.json()
        assert set(accepted) == {"run_id", "status", "status_url"}
        assert accepted["status"] == "queued"
        assert accepted["status_url"] == f"/extractions/{accepted['run_id']}"
        assert client.post("/run-extraction", headers=AUTH).status_code == 409
        response = client.get(accepted["status_url"], headers=AUTH)
        assert response.status_code == 200
        record = response.json()
        assert set(record) == {"run_id", "status", "submitted_at",
                              "started_at", "finished_at", "error_code"}
        assert record["status"] in ("queued", "running")
        assert record["finished_at"] is None
        assert record["error_code"] is None
        assert client.get("/health").status_code == 200
        assert client.get("/extractions/not-a-uuid", headers=AUTH).status_code == 404
        assert "test-databricks-token" not in response.text


@pytest.mark.parametrize("variable",
    ["EXTRACTION_API_TOKEN", "DATABRICKS_HOST", "DATABRICKS_TOKEN"])
@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_config_fails_startup(app, monkeypatch, variable, value):
    if value is None:
        monkeypatch.delenv(variable)
    else:
        monkeypatch.setenv(variable, value)
    with pytest.raises(RuntimeError, match=variable):
        with TestClient(app):
            pass


def test_new_app_has_no_previous_history(app):
    with TestClient(app) as client:
        accepted = client.post("/run-extraction", headers=AUTH).json()
    with TestClient(create_app()) as restarted:
        assert restarted.get(accepted["status_url"], headers=AUTH).status_code == 404


@pytest.mark.parametrize("code, status, error_code",
    [(0, "succeeded", None), (1, "failed", "process_failed")])
def test_terminal_status_is_exposed(app, monkeypatch, code, status, error_code):
    async def spawn(*args, **kwargs):
        process = FakeProcess(code=code)
        process.finish()
        return process

    monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
    with TestClient(app) as client:
        accepted = client.post("/run-extraction", headers=AUTH).json()
        for _ in range(100):
            response = client.get(accepted["status_url"], headers=AUTH)
            assert response.status_code == 200
            record = response.json()
            if record["status"] in ("succeeded", "failed"):
                break
        else:
            raise AssertionError("terminal status was not exposed")
        assert record["status"] == status
        assert record["error_code"] == error_code
        assert record["started_at"] and record["finished_at"]
```

The fake process remains incomplete until context shutdown; acceptance therefore
cannot accidentally be tested only with an already completed child.

- [ ] **Step 3: Verify the expected collection failure.**

Run `python -m pytest datasets\tests\test_api.py -q`.
Expected: `ModuleNotFoundError` for `datasets.api`.

- [ ] **Step 4: Add the app factory, lifespan, and routes.**

Create `datasets\api.py`:

```python
import logging
import os
import secrets
from contextlib import asynccontextmanager
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from .extraction_runner import (
    ExtractionRunner, RunBusy, RunRecord, RunnerClosed,
)


security = HTTPBearer(auto_error=False)


class AcceptedRun(BaseModel):
    run_id: str
    status: Literal["queued"]
    status_url: str


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.basicConfig(level=logging.INFO)
        required = ("EXTRACTION_API_TOKEN", "DATABRICKS_HOST", "DATABRICKS_TOKEN")
        missing = [name for name in required if not os.environ.get(name, "").strip()]
        if missing:
            raise RuntimeError("Missing required configuration: " + ", ".join(missing))
        app.state.api_token = os.environ["EXTRACTION_API_TOKEN"].encode("utf-8")
        app.state.runner = ExtractionRunner()
        try:
            yield
        finally:
            await app.state.runner.shutdown()

    app = FastAPI(lifespan=lifespan)

    async def authorize(
        request: Request,
        credentials: Annotated[
            HTTPAuthorizationCredentials | None, Depends(security)
        ],
    ) -> None:
        if credentials is None or not secrets.compare_digest(
            credentials.credentials.encode("utf-8"), request.app.state.api_token
        ):
            raise HTTPException(
                status_code=401,
                detail="Invalid or missing bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/run-extraction", status_code=202, response_model=AcceptedRun,
        dependencies=[Depends(authorize)],
    )
    async def run_extraction(request: Request) -> AcceptedRun:
        runner: ExtractionRunner = request.app.state.runner
        try:
            record = runner.submit()
        except RunBusy:
            raise HTTPException(409, "An extraction is already active") from None
        except RunnerClosed:
            raise HTTPException(503, "Extraction service is stopping") from None
        return AcceptedRun(
            run_id=record.run_id,
            status="queued",
            status_url=f"/extractions/{record.run_id}",
        )

    @app.get(
        "/extractions/{run_id}", response_model=RunRecord,
        dependencies=[Depends(authorize)],
    )
    async def extraction_status(run_id: str, request: Request) -> RunRecord:
        runner: ExtractionRunner = request.app.state.runner
        record = runner.get(run_id)
        if record is None:
            raise HTTPException(404, "Extraction not found")
        return record

    return app


app = create_app()
```

Using UTF-8 bytes for constant-time comparison avoids rejecting non-ASCII token
strings through `compare_digest`'s ASCII-string restriction. Credentials are
validated in lifespan, not module import, and the API never imports `datasets.main`.

- [ ] **Step 5: Run the combined extraction/API suite.**

Run `python -m pytest datasets\tests -q`.
Expected: all extraction, upload, runner, and API cases pass.
If framework serialization differs, correct the implementation to preserve the
exact response keys and HTTP codes; do not loosen contract assertions.

- [ ] **Step 6: Commit the endpoint deliverable.**

```powershell
git add -- datasets\api.py datasets\tests\test_api.py requirements.txt pyproject.toml
git commit -m "feat: expose authenticated extraction API" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

## Task 4: Package and document the Render deployment

**Files:** Create `render.yaml`, `tests\test_render_config.py`; modify `README.md`.

**Interfaces:**
- Consumes: `datasets.api:app`, runtime requirements, public `/health`.
- Produces: a Render Blueprint and operational instructions, not a live deployment.

- [ ] **Step 1: Add deployment contract checks.**

Create `tests\test_render_config.py`:

```python
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_render_configuration_matches_api_entry_point():
    blueprint = yaml.safe_load((ROOT / "render.yaml").read_text())
    assert len(blueprint["services"]) == 1
    service = blueprint["services"][0]
    assert service["type"] == "web"
    assert service["runtime"] == "python"
    assert service["plan"] == "free"
    assert service.get("numInstances", 1) == 1
    assert "scaling" not in service
    assert service["healthCheckPath"] == "/health"
    assert service["buildCommand"] == "pip install -r requirements.txt"
    assert service["startCommand"] == (
        "python -m uvicorn datasets.api:app --host 0.0.0.0 --port $PORT --workers 1"
    )
    env = {entry["key"]: entry for entry in service["envVars"]}
    assert env["PYTHON_VERSION"]["value"].startswith("3.13.")
    for key in ("EXTRACTION_API_TOKEN", "DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        assert env[key] == {"key": key, "sync": False}
    assert "branch" not in service
    from datasets.api import app
    routes = {(route.path, method)
              for route in app.routes for method in getattr(route, "methods", [])}
    assert ("/health", "GET") in routes
    assert ("/run-extraction", "POST") in routes
    assert ("/extractions/{run_id}", "GET") in routes
```

This checks the deployment-to-application boundary, not generic YAML behavior.
Existing configuration tests already use PyYAML. If missing locally, install it
only after the targeted command reports the missing dependency.

- [ ] **Step 2: Run the failing deployment test.**

Run `python -m pytest tests\test_render_config.py -q`.
Expected: failure because `render.yaml` is absent.

- [ ] **Step 3: Create the root Blueprint.**

Create `render.yaml`:

```yaml
services:
  - type: web
    name: financial-stock-extractor
    runtime: python
    plan: free
    buildCommand: pip install -r requirements.txt
    startCommand: python -m uvicorn datasets.api:app --host 0.0.0.0 --port $PORT --workers 1
    healthCheckPath: /health
    envVars:
      - key: PYTHON_VERSION
        value: "3.13.16"
      - key: EXTRACTION_API_TOKEN
        sync: false
      - key: DATABRICKS_HOST
        sync: false
      - key: DATABRICKS_TOKEN
        sync: false
```

Python 3.13.16 is a released maintenance version dated September 30, 2026:
https://www.python.org/downloads/release/python-31316/.
The Free service is inherently single-instance; omit manual scaling and
autoscaling fields. Preserve that deployment invariant if changing compute plans
later. The single-worker startup flag remains explicit.

- [ ] **Step 4: Append the following README section.**

````markdown
## On-demand extraction API on Render

The API starts the same `python -m datasets.main` pipeline as GitHub Actions.
It fetches the default stock universe and uploads both JSON outputs to Databricks.
The existing hourly GitHub Actions workflow stays active and independent.

### Local startup

Use Python 3.13 and a virtual environment. From the repository root in PowerShell:

```powershell
python -m pip install -r requirements.txt
$env:EXTRACTION_API_TOKEN = "<separate-api-token>"
$env:DATABRICKS_HOST = "https://<workspace-host>"
$env:DATABRICKS_TOKEN = "<databricks-token>"
python -m uvicorn datasets.api:app --host 0.0.0.0 --port 8000 --workers 1
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
python -m pip install -e ".[test]"
python -m pytest datasets\tests tests\test_render_config.py -q
```
````

- [ ] **Step 5: Run the final focused suite and inspect the diff.**

```powershell
python -m pytest datasets\tests tests\test_render_config.py -q
git diff --check
git --no-pager diff --stat
```

Expected: selected tests pass; no whitespace errors; only API, directly related
upload behavior, dependency manifests, Render configuration, and docs changed.
Do not run real extraction, deploy resources, or contact Databricks as a smoke
test. Do not modify unrelated dbt tests or fix pre-existing documentation issues.

- [ ] **Step 6: Commit the deployment deliverable.**

```powershell
git add -- render.yaml tests\test_render_config.py README.md
git commit -m "feat: prepare Render extraction service deployment" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
git status --short
```

Expected: no task-owned uncommitted implementation files remain.

## Plan self-review

The four tasks cover the spec's CLI compatibility, accurate upload failure exit,
atomic admission, HTTP response shapes, auth and startup validation, subprocess
logging, timeout/shutdown cleanup, bounded history, Render environment variables,
single-worker deployment, unchanged scheduler, documentation, and offline tests.
Shared names and return types are defined in Task 2 and consumed consistently in
Task 3. Forced shutdown and expired run IDs remain intentionally best-effort.
No deployment to the user's account is authorized by this plan.
