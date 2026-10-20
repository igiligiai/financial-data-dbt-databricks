import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx2
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


@pytest.mark.parametrize("header", [
    {},
    {"Authorization": "Basic abc"},
    {"Authorization": "Bearer wrong-token"},
    {"Authorization": "Bearer"},
    {"Authorization": "Bearer "},
])
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
        assert set(record) == {
            "run_id", "status", "submitted_at",
            "started_at", "finished_at", "error_code",
        }
        assert record["status"] in ("queued", "running")
        assert record["finished_at"] is None
        assert record["error_code"] is None
        assert client.get("/health").status_code == 200
        assert client.get("/extractions/not-a-uuid", headers=AUTH).status_code == 404
        assert client.get(f"/extractions/{uuid4()}", headers=AUTH).status_code == 404
        assert "test-databricks-token" not in response.text
        for method, path in [
            ("post", "/run-extraction"), ("get", accepted["status_url"])
        ]:
            unauthorized = getattr(client, method)(path)
            assert unauthorized.status_code == 401
            assert unauthorized.headers["www-authenticate"] == "Bearer"
            assert accepted["run_id"] not in unauthorized.text


@pytest.mark.parametrize("variable", [
    "EXTRACTION_API_TOKEN", "DATABRICKS_HOST", "DATABRICKS_TOKEN",
])
@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_config_fails_startup(app, monkeypatch, variable, value):
    if value is None:
        monkeypatch.delenv(variable)
    else:
        monkeypatch.setenv(variable, value)
    with pytest.raises(RuntimeError, match=variable) as error:
        with TestClient(app):
            pass
    assert "test-databricks-token" not in str(error.value)
    assert "test-extraction-token" not in str(error.value)


def test_new_app_has_no_previous_history(app):
    with TestClient(app) as client:
        accepted = client.post("/run-extraction", headers=AUTH).json()
    with TestClient(create_app()) as restarted:
        assert restarted.get(accepted["status_url"], headers=AUTH).status_code == 404


@pytest.mark.parametrize("code, status, error_code", [
    (0, "succeeded", None), (1, "failed", "process_failed"),
])
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


def test_lifespan_terminates_and_reaps_active_child(app, monkeypatch):
    processes = []

    async def spawn(*args, **kwargs):
        process = FakeProcess()
        processes.append(process)
        return process

    monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", spawn)
    with TestClient(app) as client:
        accepted = client.post("/run-extraction", headers=AUTH).json()
        for _ in range(100):
            if processes:
                break
            client.get("/health")
        else:
            raise AssertionError("fake subprocess was not launched")
        assert processes[0].returncode is None
    assert processes[0].terminated
    assert processes[0].returncode is not None
    assert processes[0].wait_count >= 2
    record = app.state.runner.get(accepted["run_id"])
    assert record.status == "failed"
    assert record.error_code == "shutdown"
    assert record.finished_at


def test_closed_runner_returns_service_unavailable(app):
    with TestClient(app) as client:
        client.portal.call(app.state.runner.shutdown)
        response = client.post("/run-extraction", headers=AUTH)
        assert response.status_code == 503
        assert response.json() == {"detail": "Extraction service is stopping"}


def test_non_ascii_tokens_use_utf8_comparison(app, monkeypatch):
    token = "test-extraction-\N{LATIN SMALL LETTER E WITH ACUTE}"
    monkeypatch.setenv("EXTRACTION_API_TOKEN", token)

    async def scenario():
        async with app.router.lifespan_context(app):
            # Preserve raw header bytes instead of TestClient's UTF-8 re-encoding.
            async with httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/run-extraction",
                    headers={b"Authorization": f"Bearer {token}".encode("latin-1")},
                )
                assert response.status_code == 202

    asyncio.run(scenario())
