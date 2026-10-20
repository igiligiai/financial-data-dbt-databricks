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

    routes = {
        (route.path, method)
        for route in app.routes
        for method in getattr(route, "methods", [])
    }
    assert ("/health", "GET") in routes
    assert ("/run-extraction", "POST") in routes
    assert ("/extractions/{run_id}", "GET") in routes
