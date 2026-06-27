from __future__ import annotations

from contextlib import asynccontextmanager
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from pydantic import BaseModel, Field

from . import core, security
from .db import db_path, init_db
from .ui import router as ui_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="TaskGrid", version="0.21.0", lifespan=lifespan)
app.include_router(ui_router, prefix="/ui", tags=["web-ui"])


def _auth_login_page(error: str | None = None) -> HTMLResponse:
    message = "<p style='color:#b42318;font-weight:700'>Invalid token.</p>" if error else ""
    return HTMLResponse(f"""
    <!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
    <title>TaskGrid Login</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fb;color:#101828;display:grid;place-items:center;min-height:100vh;margin:0}}.card{{background:white;border:1px solid #e4e7ec;border-radius:16px;padding:24px;box-shadow:0 1px 5px rgba(16,24,40,.08);width:min(420px,calc(100vw - 32px))}}input{{width:100%;padding:11px;border:1px solid #d0d5dd;border-radius:10px;margin:8px 0 14px}}button{{background:#111827;color:white;border:0;border-radius:10px;padding:10px 13px;font-weight:800}}.muted{{color:#667085}}</style></head>
    <body><form class='card' method='post' action='/ui/login'><h1>TaskGrid</h1><p class='muted'>Enter the UI token to access the manager web UI.</p>{message}<label>UI token<input type='password' name='ui_token' autofocus></label><button type='submit'>Sign in</button></form></body></html>
    """)


@app.middleware("http")
async def taskgrid_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path == "/ui/login":
        return await call_next(request)
    roles = security.role_for_path(path, request.method)
    if roles == ("ui",) and security.route_requires_auth(path, request.method) and not security.ui_is_authenticated(request):
        return RedirectResponse(url="/ui/login", status_code=303)
    if roles and roles != ("ui",) and security.route_requires_auth(path, request.method):
        if not security.token_has_role(request, roles):
            return Response(content='{"detail":"TaskGrid token required"}', status_code=401, media_type="application/json", headers={"WWW-Authenticate": "Bearer"})
    return await call_next(request)


@app.get("/ui/login", response_class=HTMLResponse, include_in_schema=False)
def ui_login_form() -> HTMLResponse:
    return _auth_login_page()


@app.post("/ui/login", include_in_schema=False)
async def ui_login(request: Request):
    body = (await request.body()).decode("utf-8", errors="replace")
    from urllib.parse import parse_qs
    ui_token = (parse_qs(body).get("ui_token") or [""])[0]
    fake_request = type("R", (), {"cookies": {}, "query_params": {"ui_token": ui_token}, "headers": {}})()
    if not security.config().ui_token or security.ui_is_authenticated(fake_request):
        response = RedirectResponse(url="/ui", status_code=303)
        response.set_cookie("taskgrid_ui_token", ui_token, httponly=True, samesite="lax")
        return response
    return _auth_login_page("invalid")


@app.post("/ui/logout", include_in_schema=False)
def ui_logout() -> RedirectResponse:
    response = RedirectResponse(url="/ui/login", status_code=303)
    response.delete_cookie("taskgrid_ui_token")
    return response


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
    client_id: str | None = None
    resume_token: str | None = None


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
    wait_seconds: float = Field(0, ge=0, le=30)


class LeaseBatchIn(BaseModel):
    worker_id: str
    instance_ids: list[str] = Field(..., min_length=1, max_length=core.MAX_WORKER_CONCURRENCY)
    lease_seconds: int = Field(60, ge=5, le=3600)
    wait_seconds: float = Field(0, ge=0, le=30)


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


class ResumeIn(BaseModel):
    resume_token: str


class WorkerConfigIn(BaseModel):
    # Kept as desired_concurrency for API compatibility; this now represents
    # desired single-task worker instances for the node.
    desired_concurrency: int = Field(1, ge=1, le=core.MAX_WORKER_CONCURRENCY)


class WorkerStateIn(BaseModel):
    reason: str | None = None


class WorkerBulkStateIn(BaseModel):
    worker_ids: list[str] = Field(..., min_length=1, max_length=500)
    disabled: bool
    reason: str | None = None


class PauseIn(BaseModel):
    reason: str | None = None


class SessionBulkPauseIn(BaseModel):
    session_ids: list[str] = Field(..., min_length=1, max_length=500)
    paused: bool
    reason: str | None = None


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
            client_id=payload.client_id,
            resume_token=payload.resume_token,
        )
    except core.CapabilityError as exc:
        raise HTTPException(status_code=409, detail={"message": str(exc), "capability": exc.details}) from exc


@app.get("/sessions")
def sessions(limit: int = Query(100, ge=1, le=500), status: str | None = None) -> list[dict[str, Any]]:
    return core.list_sessions(limit=limit, status=status)


@app.get("/clients/{client_id}/sessions")
def client_sessions(client_id: str, resume_token: str, limit: int = Query(100, ge=1, le=500), status: str | None = None) -> list[dict[str, Any]]:
    return core.list_client_sessions(client_id, resume_token, limit=limit, status=status)


@app.get("/clients/{client_id}/sessions/{session_id}")
def client_session(client_id: str, session_id: str, resume_token: str) -> dict[str, Any]:
    out = core.get_client_session(client_id, resume_token, session_id)
    if not out:
        raise HTTPException(status_code=404, detail="session not found for client/resume token")
    return out


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


@app.post("/sessions/{session_id}/pause")
def pause_session(session_id: str, payload: PauseIn | None = None) -> dict[str, Any]:
    out = core.set_session_paused(session_id, True, reason=(payload.reason if payload else None), updated_by="api")
    if not out:
        raise HTTPException(status_code=404, detail="session not found")
    return out


@app.post("/sessions/{session_id}/resume")
def resume_session(session_id: str) -> dict[str, Any]:
    out = core.set_session_paused(session_id, False, updated_by="api")
    if not out:
        raise HTTPException(status_code=404, detail="session not found")
    return out


@app.post("/sessions/bulk-pause")
def bulk_pause_sessions(payload: SessionBulkPauseIn) -> dict[str, Any]:
    return core.set_sessions_paused(payload.session_ids, payload.paused, reason=payload.reason, updated_by="api")


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
def cancel(job_id: str, mode: str = Query("graceful", pattern="^(graceful|force|immediate|hard)$")) -> dict[str, Any]:
    out = core.cancel_job(job_id, mode=mode)
    if not out:
        raise HTTPException(status_code=404, detail="job not found")
    return out


@app.post("/jobs/{job_id}/pause")
def pause_job(job_id: str, payload: PauseIn | None = None) -> dict[str, Any]:
    out = core.set_job_paused(job_id, True, reason=(payload.reason if payload else None), updated_by="api")
    if not out:
        raise HTTPException(status_code=404, detail="job not found")
    return out


@app.post("/jobs/{job_id}/resume")
def resume_job(job_id: str) -> dict[str, Any]:
    out = core.set_job_paused(job_id, False, updated_by="api")
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


@app.get("/jobs/{job_id}/results/export")
def job_results_export(job_id: str, format: str = Query("json", pattern="^(json|csv)$"), order: str = "input", failed_only: bool = False) -> Response:
    out = core.export_job_results(job_id, fmt=format, order=order, failed_only=failed_only)
    if not out:
        raise HTTPException(status_code=404, detail="job not found")
    body, media_type, filename = out
    return Response(content=body, media_type=media_type, headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@app.get("/sessions/{session_id}/results/export")
def session_results_export(session_id: str, format: str = Query("json", pattern="^(json|csv)$"), order: str = "input", failed_only: bool = False) -> Response:
    out = core.export_session_results(session_id, fmt=format, order=order, failed_only=failed_only)
    if not out:
        raise HTTPException(status_code=404, detail="session not found")
    body, media_type, filename = out
    return Response(content=body, media_type=media_type, headers={"Content-Disposition": f'attachment; filename="{filename}"'})


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


@app.post("/workers/purge-offline")
def purge_offline_workers(
    active_seconds: int | None = Query(None, ge=1, le=86400),
    include_running: bool = Query(False),
) -> dict[str, Any]:
    return core.purge_offline_workers(active_seconds=active_seconds, include_running=include_running, updated_by="api")


@app.post("/workers/{worker_id}/disable")
def disable_worker(worker_id: str, payload: WorkerStateIn | None = None) -> dict[str, Any]:
    return core.set_worker_enabled(worker_id, enabled=False, reason=(payload.reason if payload else None), updated_by="api")


@app.post("/workers/{worker_id}/enable")
def enable_worker(worker_id: str, payload: WorkerStateIn | None = None) -> dict[str, Any]:
    return core.set_worker_enabled(worker_id, enabled=True, reason=(payload.reason if payload else None), updated_by="api")


@app.post("/workers/bulk-state")
def bulk_worker_state(payload: WorkerBulkStateIn) -> dict[str, Any]:
    return core.set_workers_enabled(payload.worker_ids, enabled=not payload.disabled, reason=payload.reason, updated_by="api")


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


def _wait_for_leases(callable_lease, wait_seconds: float) -> list[dict[str, Any]]:
    deadline = time.monotonic() + max(0.0, float(wait_seconds or 0))
    while True:
        out = callable_lease()
        if out or time.monotonic() >= deadline:
            return out
        time.sleep(min(0.25, max(0.05, deadline - time.monotonic())))


@app.post("/tasks/lease")
def lease(payload: LeaseIn) -> list[dict[str, Any]]:
    return _wait_for_leases(
        lambda: core.lease_tasks(payload.worker_id, limit=payload.limit, lease_seconds=payload.lease_seconds, instance_id=payload.instance_id),
        payload.wait_seconds,
    )


@app.post("/tasks/lease-batch")
def lease_batch(payload: LeaseBatchIn) -> list[dict[str, Any]]:
    return _wait_for_leases(
        lambda: core.lease_tasks_for_instances(payload.worker_id, payload.instance_ids, lease_seconds=payload.lease_seconds),
        payload.wait_seconds,
    )


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


@app.get("/maintenance/status")
def maintenance_status() -> dict[str, Any]:
    return core.recovery_status()


@app.post("/maintenance/recover")
def maintenance_recover() -> dict[str, Any]:
    return core.recover_expired_leases(updated_by="api")


@app.get("/manager/log", response_class=PlainTextResponse)
def manager_log(tail: int = Query(core.MAX_MANAGER_LOG_TAIL_BYTES, ge=1, le=core.MAX_MANAGER_LOG_TAIL_BYTES)) -> str:
    return core.read_manager_log(tail_bytes=tail)


@app.get("/maintenance/retention/preview")
def maintenance_retention_preview(
    completed_days: int = Query(30, ge=0, le=3650),
    failed_days: int = Query(90, ge=0, le=3650),
    event_days: int = Query(30, ge=0, le=3650),
    purge_workers_active_seconds: int | None = Query(None, ge=1, le=86400),
) -> dict[str, Any]:
    return core.retention_preview(
        completed_days=completed_days,
        failed_days=failed_days,
        event_days=event_days,
        purge_workers_active_seconds=purge_workers_active_seconds,
    )


@app.post("/maintenance/retention/apply")
def maintenance_retention_apply(
    completed_days: int = Query(30, ge=0, le=3650),
    failed_days: int = Query(90, ge=0, le=3650),
    event_days: int = Query(30, ge=0, le=3650),
    purge_workers_active_seconds: int | None = Query(None, ge=1, le=86400),
    manager_log_keep_bytes: int | None = Query(None, ge=0, le=100_000_000),
) -> dict[str, Any]:
    return core.apply_retention_cleanup(
        completed_days=completed_days,
        failed_days=failed_days,
        event_days=event_days,
        purge_workers_active_seconds=purge_workers_active_seconds,
        manager_log_keep_bytes=manager_log_keep_bytes,
        updated_by="api",
    )


def _html_escape(value: Any) -> str:
    text = str(value if value is not None else "")
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


@app.get("/admin")
def admin() -> RedirectResponse:
    return RedirectResponse(url="/ui", status_code=307)


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/ui", status_code=307)
