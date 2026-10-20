import asyncio
import sys
from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest

import datasets.extraction_runner as runner_module
from datasets.extraction_runner import ExtractionRunner, RunBusy, RunnerClosed


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
        assert runner_module.ROOT == Path(__file__).resolve().parents[2]
        assert runner_module.EXECUTION_TIMEOUT == 300
        assert runner_module.STOP_GRACE == 5
        assert runner_module.HISTORY_LIMIT == 100
        process.finish()
        record = await settle(runner, first.run_id)
        assert record.status == "succeeded"
        assert record.started_at and record.finished_at
        assert record.error_code is None
        await runner.shutdown()

    asyncio.run(scenario())


def test_record_contract_and_shutdown_without_work(monkeypatch):
    async def scenario():
        async def spawn(*args, **kwargs):
            return FakeProcess()

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        runner = ExtractionRunner()
        record = runner.submit()
        assert str(UUID(record.run_id)) == record.run_id
        assert datetime.fromisoformat(record.submitted_at).utcoffset().total_seconds() == 0
        assert record.started_at is record.finished_at is record.error_code is None
        snapshot = runner.get(record.run_id)
        snapshot.status = "failed"
        assert runner.get(record.run_id).status == "queued"
        assert runner.get("") is None
        assert ExtractionRunner().get(record.run_id) is None
        await runner.shutdown()
        assert runner.get(record.run_id).error_code == "shutdown"
        await runner.shutdown()
        with pytest.raises(RunnerClosed):
            runner.submit()
        empty = ExtractionRunner()
        await empty.shutdown()
        await empty.shutdown()
        with pytest.raises(RunnerClosed):
            empty.submit()

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
        assert all(runner.get(run_id) for run_id in ids[1:])
        active.status = "failed"
        assert runner.get(active.run_id).status == "queued"
        assert runner.get("not-a-run") is None
        await runner.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize("ignore_terminate", [False, True])
def test_delayed_launch_is_eventually_reaped(monkeypatch, caplog, ignore_terminate):
    async def scenario():
        process = FakeProcess(ignore_terminate=ignore_terminate)
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
        with pytest.raises(RunnerClosed):
            runner.submit()
        assert runner._cleanup_tasks
        release.set()
        for _ in range(100):
            if process.returncode is not None:
                break
            await asyncio.sleep(0.001)
        assert process.terminated
        assert process.killed == ignore_terminate
        assert process.returncode is not None
        for _ in range(10):
            await asyncio.sleep(0)
        assert not runner._cleanup_tasks
        with pytest.raises(RunnerClosed):
            runner.submit()
        assert any(
            "launch cleanup timed out" in entry.message and entry.exc_info
            for entry in caplog.records
        )

    asyncio.run(scenario())


@pytest.mark.parametrize("error", [OSError("wait failed"), RuntimeError("wait failed")])
def test_wait_failure_is_not_a_launch_failure(monkeypatch, caplog, error):
    async def scenario():
        class BrokenWaitProcess(FakeProcess):
            async def wait(self):
                if not self.terminated:
                    raise error
                return await super().wait()

        process = BrokenWaitProcess()

        async def spawn(*args, **kwargs):
            return process

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        runner = ExtractionRunner()
        record = runner.submit()
        result = await settle(runner, record.run_id)
        assert result.error_code == "process_failed"
        assert process.terminated and process.returncode is not None
        assert any(entry.exc_info for entry in caplog.records)
        runner.submit()
        await runner.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize("cancel_caller", [False, True])
def test_repeated_shutdown_does_not_cancel_child_cleanup(monkeypatch, cancel_caller):
    async def scenario():
        cleanup_entered = asyncio.Event()
        release_cleanup = asyncio.Event()
        running = asyncio.Event()

        class SlowCleanupProcess(FakeProcess):
            async def wait(self):
                running.set()
                if self.terminated:
                    cleanup_entered.set()
                    await release_cleanup.wait()
                    self.finish()
                return await super().wait()

            def terminate(self):
                self.terminated = True

        process = SlowCleanupProcess()

        async def spawn(*args, **kwargs):
            return process

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        runner = ExtractionRunner()
        record = runner.submit()
        await running.wait()
        first = asyncio.create_task(runner.shutdown())
        await cleanup_entered.wait()
        second = asyncio.create_task(runner.shutdown())
        if cancel_caller:
            first.cancel()
        for _ in range(10):
            await asyncio.sleep(0)
        release_cleanup.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        assert results[1] is None
        if cancel_caller:
            assert isinstance(results[0], asyncio.CancelledError)
        else:
            assert results[0] is None
        assert process.returncode is not None
        result = runner.get(record.run_id)
        assert result.status == "failed"
        assert result.error_code == "shutdown" and result.finished_at
        with pytest.raises(RunnerClosed):
            runner.submit()

    asyncio.run(asyncio.wait_for(scenario(), 2))


def test_launch_timeout_error_is_not_execution_timeout(monkeypatch, caplog):
    async def scenario():
        async def spawn(*args, **kwargs):
            raise TimeoutError("launch syscall timed out")

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        runner = ExtractionRunner()
        record = runner.submit()
        result = await settle(runner, record.run_id)
        assert result.error_code == "launch_failed"
        assert any(entry.exc_info for entry in caplog.records)
        await runner.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["terminate", "kill", "wait", "reap_timeout"])
def test_cleanup_failure_closes_admission_and_keeps_reaping(
    monkeypatch, caplog, failure
):
    async def scenario():
        running = asyncio.Event()

        class BrokenCleanupProcess(FakeProcess):
            async def wait(self):
                running.set()
                if self.terminated and failure == "wait" and not self.killed:
                    raise RuntimeError("cleanup wait failed")
                return await super().wait()

            def terminate(self):
                self.terminated = True
                if failure == "terminate":
                    raise RuntimeError("terminate failed")

            def kill(self):
                self.killed = True
                if failure == "kill":
                    raise OSError("kill failed")
                if failure != "reap_timeout":
                    self.code = -9
                    self.finish()

        process = BrokenCleanupProcess()

        async def spawn(*args, **kwargs):
            return process

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        monkeypatch.setattr(runner_module, "STOP_GRACE", 0.001)
        monkeypatch.setattr(runner_module, "EXECUTION_TIMEOUT", 0.001)
        runner = ExtractionRunner()
        record = runner.submit()
        await running.wait()
        await asyncio.sleep(0.03)
        result = await settle(runner, record.run_id)
        assert result.status == "failed" and result.finished_at
        assert result.error_code == "timeout"
        with pytest.raises(RunnerClosed):
            runner.submit()
        assert process.killed
        assert any(entry.exc_info for entry in caplog.records)
        if process.returncode is None:
            assert runner._cleanup_tasks
            process.finish()
            for _ in range(100):
                if not runner._cleanup_tasks:
                    break
                await asyncio.sleep(0)
            assert not runner._cleanup_tasks
        await runner.shutdown()

    asyncio.run(asyncio.wait_for(scenario(), 2))


@pytest.mark.parametrize("late", [False, True])
@pytest.mark.parametrize("error", [OSError("launch failed"), RuntimeError("launch failed")])
def test_launch_failure_during_shutdown_is_logged(monkeypatch, caplog, late, error):
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()

        async def spawn(*args, **kwargs):
            entered.set()
            await release.wait()
            raise error

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        monkeypatch.setattr(runner_module, "STOP_GRACE", 0.001)
        runner = ExtractionRunner()
        record = runner.submit()
        await entered.wait()
        shutdown = asyncio.create_task(runner.shutdown())
        await asyncio.sleep(0)
        if not late:
            release.set()
        await shutdown
        release.set()
        for _ in range(100):
            if not runner._cleanup_tasks:
                break
            await asyncio.sleep(0)
        assert not runner._cleanup_tasks
        result = runner.get(record.run_id)
        assert result.error_code == "shutdown" and result.finished_at
        assert any(
            "launch cleanup failed" in entry.message and entry.exc_info
            for entry in caplog.records
        )

    asyncio.run(scenario())


def test_shutdown_during_timeout_cleanup_does_not_interrupt_reaping(monkeypatch):
    async def scenario():
        cleanup_entered = asyncio.Event()
        release = asyncio.Event()

        class SlowCleanupProcess(FakeProcess):
            async def wait(self):
                if self.terminated:
                    cleanup_entered.set()
                    await release.wait()
                    self.finish()
                return await super().wait()

            def terminate(self):
                self.terminated = True

        process = SlowCleanupProcess()

        async def spawn(*args, **kwargs):
            return process

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        monkeypatch.setattr(runner_module, "EXECUTION_TIMEOUT", 0.001)
        runner = ExtractionRunner()
        record = runner.submit()
        await cleanup_entered.wait()
        with pytest.raises(RunBusy):
            runner.submit()
        shutdown = asyncio.create_task(runner.shutdown())
        for _ in range(10):
            await asyncio.sleep(0)
        release.set()
        await shutdown
        assert process.returncode is not None
        result = runner.get(record.run_id)
        assert result.status == "failed" and result.finished_at
        assert result.error_code == "timeout"

    asyncio.run(asyncio.wait_for(scenario(), 2))


def test_unexpected_cleanup_failure_cannot_report_success(monkeypatch, caplog):
    async def scenario():
        process = FakeProcess()
        process.finish()

        async def spawn(*args, **kwargs):
            return process

        async def broken_stop(*args):
            raise RuntimeError("unexpected cleanup failure")

        monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
        runner = ExtractionRunner()
        monkeypatch.setattr(runner, "_stop", broken_stop)
        record = runner.submit()
        result = await settle(runner, record.run_id)
        assert result.status == "failed"
        assert result.error_code == "process_failed" and result.finished_at
        with pytest.raises(RunnerClosed):
            runner.submit()
        assert any(
            "child cleanup failed" in entry.message and entry.exc_info
            for entry in caplog.records
        )
        await runner.shutdown()

    asyncio.run(scenario())
