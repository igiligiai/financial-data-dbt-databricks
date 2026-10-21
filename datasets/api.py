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
