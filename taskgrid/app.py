from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from pydantic import BaseModel, Field

from . import core
from .db import db_path, init_db
from .ui import router as ui_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="TaskGrid", version="0.10.1", lifespan=lifespan)
app.include_router(ui_router, prefix="/ui", tags=["web-ui"])


class JobSubmit(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    task_type: str = Field(..., min_length=1, max_length=120)
    tasks: list[dict[str, Any]] = Field(..., min_length=1)
    priority: int | None = None
    session_priority: int | None = None
    max_retries: int = Field(2, ge=0, le=20)
    metadata: dict[str, Any] = Field(default_factory=dict)
    session_id: str | None = None
    session_name: str | None = None
    session_metadata: dict[str, Any] = Field(default_factory=dict)
    require_capable_worker: bool = False


class HeartbeatIn(BaseModel):
    worker_id: str
    hostname: str | None = None
    status: str = "idle"
    current_task_id: str | None = None
    version: str = "dev"
    metadata: dict[str, Any] = Field(default_factory=dict)


class LeaseIn(BaseModel):
    worker_id: str
    instance_id: str | None = None
    limit: int = Field(1, ge=1, le=50)
    lease_seconds: int = Field(60, ge=5, le=3600)


class CompleteIn(BaseModel):
    worker_id: str
    instance_id: str | None = None
    result: Any


class FailIn(BaseModel):
    worker_id: str
    instance_id: str | None = None
    error: str




class SessionPriorityIn(BaseModel):
    priority: int

class WorkerConfigIn(BaseModel):
    # Kept as desired_concurrency for API compatibility; this now represents
    # desired single-task worker instances for the node.
    desired_concurrency: int = Field(1, ge=1, le=core.MAX_WORKER_CONCURRENCY)


@app.get("/health")
def health() -> dict[str, Any]:
    init_db()
    return {"ok": True, "db": str(db_path())}


@app.post("/jobs", status_code=201)
def submit_job(payload: JobSubmit) -> dict[str, Any]:
    try:
        return core.create_job(
            name=payload.name,
            task_type=payload.task_type,
            payloads=payload.tasks,
            priority=payload.priority,
            max_retries=payload.max_retries,
            metadata=payload.metadata,
            session_id=payload.session_id,
            session_name=payload.session_name,
            session_metadata=payload.session_metadata,
            session_priority=payload.session_priority,
            require_capable_worker=payload.require_capable_worker,
        )
    except core.CapabilityError as exc:
        raise HTTPException(status_code=409, detail={"message": str(exc), "capability": exc.details}) from exc


@app.get("/sessions")
def sessions(limit: int = Query(100, ge=1, le=500), status: str | None = None) -> list[dict[str, Any]]:
    return core.list_sessions(limit=limit, status=status)


@app.get("/sessions/{session_id}")
def session(session_id: str) -> dict[str, Any]:
    out = core.get_session(session_id)
    if not out:
        raise HTTPException(status_code=404, detail="session not found")
    return out


@app.get("/sessions/{session_id}/jobs")
def session_jobs(session_id: str) -> list[dict[str, Any]]:
    if not core.get_session(session_id):
        raise HTTPException(status_code=404, detail="session not found")
    return core.list_session_jobs(session_id)


@app.post("/sessions/{session_id}/priority")
def update_session_priority(session_id: str, payload: SessionPriorityIn) -> dict[str, Any]:
    out = core.set_session_priority(session_id, payload.priority, updated_by="api")
    if not out:
        raise HTTPException(status_code=404, detail="session not found")
    return out


@app.get("/sessions/{session_id}/results")
def session_results(session_id: str) -> dict[str, Any]:
    out = core.get_session_results(session_id)
    if not out:
        raise HTTPException(status_code=404, detail="session not found")
    return out


@app.get("/jobs")
def jobs(limit: int = Query(100, ge=1, le=500), status: str | None = None) -> list[dict[str, Any]]:
    return core.list_jobs(limit=limit, status=status)


@app.get("/jobs/{job_id}")
def job(job_id: str) -> dict[str, Any]:
    out = core.get_job(job_id)
    if not out:
        raise HTTPException(status_code=404, detail="job not found")
    return out


@app.post("/jobs/{job_id}/cancel")
def cancel(job_id: str) -> dict[str, Any]:
    out = core.cancel_job(job_id)
    if not out:
        raise HTTPException(status_code=404, detail="job not found")
    return out


@app.post("/jobs/{job_id}/retry-failed")
def retry_failed(job_id: str, reset_attempts: bool = True) -> dict[str, Any]:
    out = core.retry_failed_tasks(job_id, reset_attempts=reset_attempts)
    if not out:
        raise HTTPException(status_code=404, detail="job not found")
    return out


@app.get("/jobs/{job_id}/tasks")
def job_tasks(job_id: str) -> list[dict[str, Any]]:
    if not core.get_job(job_id):
        raise HTTPException(status_code=404, detail="job not found")
    return core.list_tasks(job_id=job_id)


@app.get("/jobs/{job_id}/results")
def job_results(job_id: str) -> dict[str, Any]:
    out = core.get_job_results(job_id)
    if not out:
        raise HTTPException(status_code=404, detail="job not found")
    return out


@app.get("/task-catalog")
def task_catalog(task_type: str | None = None, required_tags: str | None = None) -> dict[str, Any]:
    tags = [tag.strip() for tag in (required_tags or "").split(",") if tag.strip()]
    return core.get_task_catalog(task_type=task_type, required_tags=tags)


@app.get("/tasks")
def tasks(limit: int = Query(100, ge=1, le=500)) -> list[dict[str, Any]]:
    return core.list_tasks(limit=limit)


@app.get("/tasks/{task_id}")
def task(task_id: str) -> dict[str, Any]:
    out = core.get_task(task_id)
    if not out:
        raise HTTPException(status_code=404, detail="task not found")
    return out


@app.post("/tasks/{task_id}/retry")
def retry_task(task_id: str, reset_attempts: bool = True) -> dict[str, Any]:
    out = core.retry_task(task_id, reset_attempts=reset_attempts)
    if not out:
        raise HTTPException(status_code=404, detail="task not found")
    return out


@app.post("/workers/heartbeat")
def heartbeat(payload: HeartbeatIn) -> dict[str, Any]:
    return core.heartbeat_worker(
        worker_id=payload.worker_id,
        hostname=payload.hostname,
        status=payload.status,
        current_task_id=payload.current_task_id,
        version=payload.version,
        metadata=payload.metadata,
    )


@app.get("/workers")
def workers(limit: int = Query(100, ge=1, le=500)) -> list[dict[str, Any]]:
    return core.list_workers(limit=limit)


@app.get("/workers/{worker_id}/logs")
def worker_logs(worker_id: str) -> dict[str, Any]:
    try:
        return core.list_worker_logs(worker_id)
    except core.WorkerLogError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/workers/{worker_id}/logs/{filename:path}", response_class=PlainTextResponse)
def worker_log_file(worker_id: str, filename: str, tail: int = Query(core.MAX_WORKER_LOG_TAIL_BYTES, ge=1, le=core.MAX_WORKER_LOG_TAIL_BYTES)) -> str:
    try:
        return core.read_worker_log(worker_id, filename, tail_bytes=tail)
    except core.WorkerLogError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/workers/{worker_id}/config")
def worker_config(worker_id: str) -> dict[str, Any]:
    return core.get_worker_config(worker_id)


@app.post("/workers/{worker_id}/config")
def update_worker_config(worker_id: str, payload: WorkerConfigIn) -> dict[str, Any]:
    return core.set_worker_config(worker_id, payload.desired_concurrency, updated_by="api")


@app.post("/tasks/lease")
def lease(payload: LeaseIn) -> list[dict[str, Any]]:
    return core.lease_tasks(payload.worker_id, limit=payload.limit, lease_seconds=payload.lease_seconds, instance_id=payload.instance_id)


@app.post("/tasks/{task_id}/complete")
def complete(task_id: str, payload: CompleteIn) -> dict[str, Any]:
    out = core.complete_task(task_id, payload.worker_id, payload.result, instance_id=payload.instance_id)
    if not out:
        raise HTTPException(status_code=404, detail="task not found")
    return out


@app.post("/tasks/{task_id}/fail")
def fail(task_id: str, payload: FailIn) -> dict[str, Any]:
    out = core.fail_task(task_id, payload.worker_id, payload.error, instance_id=payload.instance_id)
    if not out:
        raise HTTPException(status_code=404, detail="task not found")
    return out


@app.get("/events")
def events(limit: int = Query(200, ge=1, le=1000), entity_type: str | None = None, entity_id: str | None = None, code: str | None = None) -> list[dict[str, Any]]:
    return core.list_events(entity_type=entity_type, entity_id=entity_id, code=code, limit=limit)


@app.get("/manager/log", response_class=PlainTextResponse)
def manager_log(tail: int = Query(core.MAX_MANAGER_LOG_TAIL_BYTES, ge=1, le=core.MAX_MANAGER_LOG_TAIL_BYTES)) -> str:
    return core.read_manager_log(tail_bytes=tail)


def _html_escape(value: Any) -> str:
    text = str(value if value is not None else "")
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


@app.get("/admin")
def admin() -> RedirectResponse:
    return RedirectResponse(url="/ui", status_code=307)


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/ui", status_code=307)
