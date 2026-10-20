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
    """One process-local reservation; all methods run on the application event loop."""

    def __init__(self):
        self._records: OrderedDict[str, RunRecord] = OrderedDict()
        self._active: str | None = None
        self._task: asyncio.Task[None] | None = None
        self._closing = False
        self._finalizing = False
        self._shutdown_task: asyncio.Task[None] | None = None
        self._cleanup_tasks: set[asyncio.Task[None]] = set()

    def submit(self) -> RunRecord:
        if self._closing:
            raise RunnerClosed()
        if self._active is not None:
            raise RunBusy()
        record = RunRecord(str(uuid4()), "queued", utc_now())
        self._records[record.run_id] = record
        self._active = record.run_id
        self._finalizing = False
        self._task = asyncio.create_task(self._run(record))
        logger.info("Extraction %s accepted", record.run_id)
        return replace(record)

    def get(self, run_id: str) -> RunRecord | None:
        record = self._records.get(run_id)
        return replace(record) if record is not None else None

    async def shutdown(self) -> None:
        self._closing = True
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(self._shutdown_active())
        # A cancelled caller or a second shutdown must not cancel child cleanup.
        await asyncio.shield(self._shutdown_task)

    async def _shutdown_active(self) -> None:
        task = self._task
        if task is not None and not task.done():
            # Let a newly scheduled supervisor enter its cancellation handler.
            await asyncio.sleep(0)
            if not task.done() and not self._finalizing:
                task.cancel()
            await task

    def _supervise_cleanup(self, coroutine) -> None:
        cleanup = asyncio.create_task(coroutine)
        self._cleanup_tasks.add(cleanup)
        cleanup.add_done_callback(self._cleanup_tasks.discard)

    async def _stop(self, process: asyncio.subprocess.Process, run_id: str) -> None:
        if process.returncode is not None:
            return
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        except Exception:
            self._closing = True
            logger.exception("Extraction %s terminate failed; will escalate", run_id)
        try:
            await asyncio.wait_for(process.wait(), STOP_GRACE)
            return
        except TimeoutError:
            logger.warning("Extraction %s did not terminate; killing", run_id)
        except Exception:
            self._closing = True
            logger.exception("Extraction %s cleanup wait failed; will escalate", run_id)
        try:
            process.kill()
        except ProcessLookupError:
            pass
        await asyncio.wait_for(process.wait(), STOP_GRACE)

    async def _reap(self, process: asyncio.subprocess.Process, run_id: str) -> None:
        try:
            await process.wait()
            logger.info("Extraction %s deferred child reaped", run_id)
        except asyncio.CancelledError:
            logger.warning("Extraction %s deferred reaping interrupted", run_id)
            raise
        except Exception:
            logger.exception("Extraction %s deferred reaping failed; admission closed", run_id)

    async def _cleanup_child(
        self, process: asyncio.subprocess.Process, run_id: str
    ) -> bool:
        try:
            await self._stop(process, run_id)
            return True
        except Exception:
            self._closing = True
            logger.exception("Extraction %s child cleanup failed; admission closed", run_id)
            self._supervise_cleanup(self._reap(process, run_id))
            return False

    async def _finish_late_launch(self, launch, run_id: str) -> None:
        try:
            process = await asyncio.shield(launch)
            await self._cleanup_child(process, run_id)
        except asyncio.CancelledError:
            logger.warning("Extraction %s late launch cleanup interrupted", run_id)
            raise
        except Exception:
            logger.exception("Extraction %s late launch cleanup failed", run_id)

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
            if process is None:
                record.error_code = "launch_failed"
                logger.exception("Extraction %s could not launch", record.run_id)
            else:
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
                except TimeoutError:
                    self._closing = True
                    logger.exception("Extraction %s launch cleanup timed out", record.run_id)
                    self._supervise_cleanup(
                        self._finish_late_launch(launch, record.run_id)
                    )
                except Exception:
                    logger.exception("Extraction %s launch cleanup failed", record.run_id)
                    self._closing = True
        except OSError:
            record.error_code = "launch_failed" if process is None else "process_failed"
            logger.exception("Extraction %s subprocess failed", record.run_id)
        except Exception:
            record.error_code = "process_failed"
            logger.exception("Extraction %s supervisor failed", record.run_id)
        finally:
            # Shutdown must await, not cancel, an already-finalizing supervisor.
            self._finalizing = True
            if process is not None:
                if not await self._cleanup_child(process, record.run_id):
                    record.error_code = record.error_code or "process_failed"
            record.status = "failed" if record.error_code else "succeeded"
            record.finished_at = utc_now()
            self._active = None
            self._records.move_to_end(record.run_id)
            while len(self._records) > HISTORY_LIMIT:
                self._records.popitem(last=False)
            logger.info("Extraction %s %s", record.run_id, record.status)
