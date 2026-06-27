from __future__ import annotations

import csv
import io
import json
import os
import socket
import uuid
from pathlib import Path
from urllib import parse, request
from urllib.error import HTTPError, URLError
from datetime import datetime, timedelta, timezone
from typing import Any

from .db import connection, db_path, init_db

TERMINAL_TASK_STATUSES = {"succeeded", "failed", "cancelled"}
TERMINAL_JOB_STATUSES = {"succeeded", "failed", "cancelled"}
CANCELLING_JOB_STATUS = "cancelling"
MAX_WORKER_CONCURRENCY = 128
MAX_WORKER_LOG_TAIL_BYTES = 1_000_000
MAX_MANAGER_LOG_TAIL_BYTES = 2_000_000
DEFAULT_WORKER_ACTIVE_SECONDS = int(os.environ.get("TASKGRID_WORKER_ACTIVE_SECONDS", "60"))
LEASE_CANDIDATE_MULTIPLIER = max(1, int(os.environ.get("TASKGRID_LEASE_CANDIDATE_MULTIPLIER", "50")))
VERBOSE_TASK_EVENT_CODES = {"TaskAccepted", "TaskCompleted"}
DEBUG_EVENT_MODES = {"debug", "verbose", "trace", "all"}



class CapabilityError(ValueError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def loads(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default



def _coerce_concurrency(value: Any, default: int = 1) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = default
    return max(1, min(MAX_WORKER_CONCURRENCY, out))


def _metadata_concurrency(metadata: dict[str, Any] | None, default: int = 1) -> int:
    metadata = metadata or {}
    return _coerce_concurrency(
        metadata.get("configured_instances")
        or metadata.get("instance_count")
        or metadata.get("configured_concurrency")
        or metadata.get("active_concurrency")
        or metadata.get("default_concurrency")
        or metadata.get("concurrency"),
        default=default,
    )

def _coerce_priority(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default



def _safe_log_part(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(value or ""))[:180]


def _safe_log_relative_path(value: str) -> str | None:
    text = parse.unquote(str(value or "")).strip().replace("\\", "/")
    if not text or text.startswith("/"):
        return None
    parts = [part for part in text.split("/") if part]
    if not parts:
        return None
    for part in parts:
        if part in {".", ".."} or part.startswith(".") or _safe_log_part(part) != part:
            return None
    return "/".join(parts)



def executor_id(worker_id: str, instance_id: str | None = None) -> str:
    """Stable manager-facing execution slot id.

    worker_id is the node id used for worker config/tags/log proxying.
    instance_id is the single-task logical engine slot inside that node.
    """
    clean_worker = str(worker_id or "").strip()
    clean_instance = str(instance_id or "").strip()
    if clean_instance:
        return f"{clean_worker}-{clean_instance}"
    return clean_worker


def execution_event_data(worker_id: str, instance_id: str | None = None, **extra: Any) -> dict[str, Any]:
    data = {"worker_id": worker_id, "executor_id": executor_id(worker_id, instance_id)}
    if instance_id:
        data["instance_id"] = instance_id
    data.update(extra)
    return data

def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:18]}"


def new_resume_token() -> str:
    # Longer than regular ids because this acts as a bearer-style reconnect secret.
    return f"rt_{uuid.uuid4().hex}{uuid.uuid4().hex[:12]}"


def _normalize_client_id(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return "".join(ch if ch.isalnum() or ch in "._:@-" else "_" for ch in text)[:160]


def _verify_resume_token(stored: str | None, supplied: str | None) -> bool:
    if not stored:
        return True
    if supplied is None:
        return True
    return str(stored) == str(supplied)


def row_to_dict(row: Any) -> dict[str, Any]:
    data = dict(row)
    for key in ("payload_json", "result_json", "metadata_json", "data_json"):
        if key in data:
            out_key = key[:-5] if key.endswith("_json") else key
            data[out_key] = loads(data.pop(key), {} if key != "result_json" else None)
    return data


def manager_log_path() -> Path:
    explicit = os.environ.get("TASKGRID_MANAGER_LOG")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return db_path().with_name("manager.log")


def _event_code_from_message(message: str) -> str:
    words = []
    current = []
    for ch in str(message or ""):
        if ch.isalnum():
            current.append(ch)
        elif current:
            words.append("".join(current))
            current = []
    if current:
        words.append("".join(current))
    return "".join(word[:1].upper() + word[1:] for word in words) or "Event"


def _append_manager_log(record: dict[str, Any]) -> None:
    try:
        path = manager_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(dumps(record) + "\n")
    except OSError:
        # Manager logging should never break the broker transaction path.
        pass


def read_manager_log(tail_bytes: int = MAX_MANAGER_LOG_TAIL_BYTES) -> str:
    path = manager_log_path()
    if not path.exists():
        return ""
    tail = max(1, min(MAX_MANAGER_LOG_TAIL_BYTES, int(tail_bytes)))
    size = path.stat().st_size
    with path.open("rb") as handle:
        if size > tail:
            handle.seek(-tail, os.SEEK_END)
        return handle.read().decode("utf-8", errors="replace")


def truncate_manager_log(keep_bytes: int = MAX_MANAGER_LOG_TAIL_BYTES) -> dict[str, Any]:
    """Keep only the tail of manager.log.

    This is a local manager file operation. It is intentionally separate from
    event row cleanup because task/job/session database state is authoritative.
    """
    path = manager_log_path()
    keep = max(0, int(keep_bytes or 0))
    if not path.exists():
        return {"path": str(path), "existed": False, "old_size_bytes": 0, "new_size_bytes": 0, "truncated": False}
    old_size = path.stat().st_size
    if keep <= 0:
        path.write_text("", encoding="utf-8")
        return {"path": str(path), "existed": True, "old_size_bytes": old_size, "new_size_bytes": 0, "truncated": old_size > 0}
    if old_size <= keep:
        return {"path": str(path), "existed": True, "old_size_bytes": old_size, "new_size_bytes": old_size, "truncated": False}
    with path.open("rb") as handle:
        handle.seek(-keep, os.SEEK_END)
        data = handle.read()
    # Drop a partial first line so the retained JSONL tail starts cleanly when possible.
    first_newline = data.find(b"\n")
    if first_newline >= 0 and first_newline + 1 < len(data):
        data = data[first_newline + 1 :]
    with path.open("wb") as handle:
        handle.write(data)
    return {"path": str(path), "existed": True, "old_size_bytes": old_size, "new_size_bytes": len(data), "truncated": True}


def event_log_mode() -> str:
    """Return the manager event/audit verbosity mode.

    Normal mode keeps durable task/job/session state authoritative but skips
    high-volume successful task lifecycle events. Debug/verbose modes record
    every TaskAccepted/TaskCompleted event for trace-level investigation.
    """
    return str(os.environ.get("TASKGRID_EVENT_MODE") or os.environ.get("TASKGRID_LOG_MODE") or "normal").strip().lower()


def verbose_task_events_enabled() -> bool:
    explicit = str(os.environ.get("TASKGRID_VERBOSE_TASK_EVENTS") or "").strip().lower()
    if explicit in {"1", "true", "yes", "on"}:
        return True
    if explicit in {"0", "false", "no", "off"}:
        return False
    return event_log_mode() in DEBUG_EVENT_MODES


def should_record_event(level: str, code: str) -> bool:
    # Successful per-task lifecycle events are very high volume and duplicate
    # state already stored on the task row. Keep them for debug mode only.
    if code in VERBOSE_TASK_EVENT_CODES and str(level).lower() == "info":
        return verbose_task_events_enabled()
    return True


def log_event(conn, level: str, entity_type: str, entity_id: str, message: str, data: dict[str, Any] | None = None, code: str | None = None) -> None:
    timestamp = now()
    event_code = code or _event_code_from_message(message)
    if not should_record_event(level, event_code):
        return
    payload = data or {}
    conn.execute(
        """
        INSERT INTO events(at, level, code, entity_type, entity_id, message, data_json)
        VALUES(?,?,?,?,?,?,?)
        """,
        (timestamp, level, event_code, entity_type, entity_id, message, dumps(payload)),
    )
    _append_manager_log(
        {
            "at": timestamp,
            "level": level,
            "code": event_code,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "message": message,
            "data": payload,
        }
    )



def _session_status_from_counts(total: int, completed: int, failed: int, cancelled: int, running: int, queued: int, cancelling_jobs: int = 0) -> str:
    if total == 0:
        return "queued"
    if cancelling_jobs > 0 and running > 0:
        return CANCELLING_JOB_STATUS
    if completed == total:
        return "succeeded"
    if failed > 0 and queued == 0 and running == 0 and completed + failed + cancelled == total:
        return "failed"
    if cancelled > 0 and queued == 0 and running == 0 and completed + failed + cancelled == total:
        return "cancelled"
    if running > 0 or completed > 0 or failed > 0:
        return "running"
    return "queued"


def recalc_session(conn, session_id: str | None) -> None:
    if not session_id:
        return
    session = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
    if not session:
        return
    counts = conn.execute(
        """
        SELECT
            COUNT(DISTINCT j.id) AS total_jobs,
            COUNT(t.id) AS total_tasks,
            SUM(CASE WHEN t.status = 'queued' THEN 1 ELSE 0 END) AS queued,
            SUM(CASE WHEN t.status = 'running' THEN 1 ELSE 0 END) AS running,
            SUM(CASE WHEN t.status = 'succeeded' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN t.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
            MIN(t.started_at) AS first_started_at,
            SUM(CASE WHEN j.status = 'cancelling' THEN 1 ELSE 0 END) AS cancelling_jobs
        FROM jobs j
        LEFT JOIN tasks t ON t.job_id = j.id
        WHERE j.session_id = ?
        """,
        (session_id,),
    ).fetchone()
    total_jobs = int(counts["total_jobs"] or 0)
    total = int(counts["total_tasks"] or 0)
    queued = int(counts["queued"] or 0)
    running = int(counts["running"] or 0)
    completed = int(counts["completed"] or 0)
    failed = int(counts["failed"] or 0)
    cancelled = int(counts["cancelled"] or 0)
    next_status = _session_status_from_counts(total, completed, failed, cancelled, running, queued, int(counts['cancelling_jobs'] or 0))
    timestamp = now()
    patch: list[Any] = [total_jobs, total, queued, running, completed, failed, cancelled]
    set_parts = [
        "total_jobs=?",
        "total_tasks=?",
        "queued_tasks=?",
        "running_tasks=?",
        "completed_tasks=?",
        "failed_tasks=?",
        "cancelled_tasks=?",
    ]
    if next_status != session["status"]:
        set_parts.append("status=?")
        patch.append(next_status)
        log_event(conn, "info", "service_session", session_id, f"service session status changed to {next_status}", {"status": next_status}, code="ServiceSessionStatusChanged")
    if session["started_at"] is None and (counts["first_started_at"] or next_status == "running"):
        set_parts.append("started_at=?")
        patch.append(counts["first_started_at"] or timestamp)
    if next_status in TERMINAL_JOB_STATUSES and session["finished_at"] is None:
        set_parts.append("finished_at=?")
        patch.append(timestamp)
    if next_status not in TERMINAL_JOB_STATUSES and session["finished_at"] is not None:
        set_parts.append("finished_at=NULL")
    patch.append(session_id)
    conn.execute(f"UPDATE service_sessions SET {', '.join(set_parts)} WHERE id=?", patch)

def recalc_job(conn, job_id: str) -> None:
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not job:
        return
    if job["status"] == "cancelled":
        counts = conn.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
            FROM tasks WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
        conn.execute(
            "UPDATE jobs SET completed_tasks=?, failed_tasks=?, cancelled_tasks=? WHERE id=?",
            (counts["completed"] or 0, counts["failed"] or 0, counts["cancelled"] or 0, job_id),
        )
        recalc_session(conn, job["session_id"] if "session_id" in job.keys() else None)
        return

    counts = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
            SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
            SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued
        FROM tasks WHERE job_id = ?
        """,
        (job_id,),
    ).fetchone()

    total = counts["total"] or 0
    completed = counts["completed"] or 0
    failed = counts["failed"] or 0
    cancelled = counts["cancelled"] or 0
    running = counts["running"] or 0
    queued = counts["queued"] or 0

    current_status = job["status"]
    next_status = current_status
    patch: list[Any] = [completed, failed, cancelled]
    set_parts = ["completed_tasks=?", "failed_tasks=?", "cancelled_tasks=?"]

    if current_status == CANCELLING_JOB_STATUS and running > 0:
        next_status = CANCELLING_JOB_STATUS
    elif total == 0:
        next_status = "succeeded"
    elif completed == total:
        next_status = "succeeded"
    elif failed > 0 and queued == 0 and running == 0 and completed + failed + cancelled == total:
        next_status = "failed"
    elif cancelled > 0 and queued == 0 and running == 0 and completed + failed + cancelled == total:
        next_status = "cancelled"
    elif running > 0 or completed > 0 or failed > 0:
        next_status = "running"
    else:
        next_status = "queued"

    if next_status != current_status:
        set_parts.append("status=?")
        patch.append(next_status)
        if next_status == "running" and job["started_at"] is None:
            set_parts.append("started_at=?")
            patch.append(now())
        if next_status in TERMINAL_JOB_STATUSES and job["finished_at"] is None:
            set_parts.append("finished_at=?")
            patch.append(now())
        log_event(conn, "info", "job", job_id, f"job status changed to {next_status}", {"status": next_status}, code="JobStatusChanged")

    patch.append(job_id)
    conn.execute(f"UPDATE jobs SET {', '.join(set_parts)} WHERE id=?", patch)
    recalc_session(conn, job["session_id"] if "session_id" in job.keys() else None)



def _mark_task_started_incremental(conn, job_id: str, session_id: str | None, timestamp: str) -> None:
    """Fast counter path for queued -> running transitions.

    This avoids a full job/session recount on every task lease. Transactions are
    already serialized with BEGIN IMMEDIATE, so denormalized counters remain safe
    for the normal lease/complete path. Recovery/cancel/retry paths still use the
    slower full recalculation helpers where correctness is more important than
    raw tiny-task throughput.
    """
    job = conn.execute("SELECT status, started_at FROM jobs WHERE id=?", (job_id,)).fetchone()
    if job and job["status"] == "queued":
        conn.execute(
            "UPDATE jobs SET status='running', started_at=COALESCE(started_at, ?) WHERE id=?",
            (timestamp, job_id),
        )
        log_event(conn, "info", "job", job_id, "job status changed to running", {"status": "running"}, code="JobStatusChanged")
    if not session_id:
        return
    session = conn.execute("SELECT status, started_at FROM service_sessions WHERE id=?", (session_id,)).fetchone()
    if not session:
        return
    was_queued = session["status"] == "queued"
    conn.execute(
        """
        UPDATE service_sessions
        SET queued_tasks=MAX(queued_tasks - 1, 0),
            running_tasks=running_tasks + 1,
            status=CASE WHEN status='queued' THEN 'running' ELSE status END,
            started_at=COALESCE(started_at, ?),
            finished_at=NULL
        WHERE id=?
        """,
        (timestamp, session_id),
    )
    if was_queued:
        log_event(conn, "info", "service_session", session_id, "service session status changed to running", {"status": "running"}, code="ServiceSessionStatusChanged")


def _mark_task_completed_incremental(conn, job_id: str, session_id: str | None, timestamp: str) -> None:
    """Fast counter path for running -> succeeded transitions."""
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if job:
        completed = int(job["completed_tasks"] or 0) + 1
        failed = int(job["failed_tasks"] or 0)
        cancelled = int(job["cancelled_tasks"] or 0)
        total = int(job["total_tasks"] or 0)
        next_status = job["status"]
        set_parts = ["completed_tasks=?"]
        patch: list[Any] = [completed]
        if completed == total:
            next_status = "succeeded"
        elif next_status == "queued":
            next_status = "running"
        if next_status != job["status"]:
            set_parts.append("status=?")
            patch.append(next_status)
            if next_status == "running" and job["started_at"] is None:
                set_parts.append("started_at=?")
                patch.append(timestamp)
            if next_status in TERMINAL_JOB_STATUSES and job["finished_at"] is None:
                set_parts.append("finished_at=?")
                patch.append(timestamp)
            log_event(conn, "info", "job", job_id, f"job status changed to {next_status}", {"status": next_status}, code="JobStatusChanged")
        patch.append(job_id)
        conn.execute(f"UPDATE jobs SET {', '.join(set_parts)} WHERE id=?", patch)

    if not session_id:
        return
    session = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
    if not session:
        return
    completed = int(session["completed_tasks"] or 0) + 1
    failed = int(session["failed_tasks"] or 0)
    cancelled = int(session["cancelled_tasks"] or 0)
    running = max(int(session["running_tasks"] or 0) - 1, 0)
    queued = int(session["queued_tasks"] or 0)
    total = int(session["total_tasks"] or 0)
    next_status = _session_status_from_counts(total, completed, failed, cancelled, running, queued)
    set_parts = ["completed_tasks=?", "running_tasks=?"]
    patch = [completed, running]
    if next_status != session["status"]:
        set_parts.append("status=?")
        patch.append(next_status)
        log_event(conn, "info", "service_session", session_id, f"service session status changed to {next_status}", {"status": next_status}, code="ServiceSessionStatusChanged")
    if next_status in TERMINAL_JOB_STATUSES and session["finished_at"] is None:
        set_parts.append("finished_at=?")
        patch.append(timestamp)
    if next_status not in TERMINAL_JOB_STATUSES and session["finished_at"] is not None:
        set_parts.append("finished_at=NULL")
    patch.append(session_id)
    conn.execute(f"UPDATE service_sessions SET {', '.join(set_parts)} WHERE id=?", patch)

def create_job(
    name: str,
    task_type: str,
    payloads: list[dict[str, Any]],
    priority: int | None = None,
    max_retries: int = 2,
    metadata: dict[str, Any] | None = None,
    session_id: str | None = None,
    session_name: str | None = None,
    session_metadata: dict[str, Any] | None = None,
    session_priority: int | None = None,
    require_capable_worker: bool = False,
    client_id: str | None = None,
    resume_token: str | None = None,
) -> dict[str, Any]:
    init_db()
    job_id = new_id("job")
    created = now()
    session_created = False
    normalized_client_id = _normalize_client_id(client_id) or new_id("client")
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        capability = _capability_for_submit(conn, task_type, metadata or {})
        if require_capable_worker and capability.get("warnings"):
            conn.execute("ROLLBACK")
            raise CapabilityError("no active capable worker for submitted task", capability)

        if session_id:
            existing_session = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        else:
            existing_session = None

        supplied_job_priority = priority is not None
        requested_job_priority = _coerce_priority(priority, default=0)
        requested_session_priority = _coerce_priority(
            session_priority,
            default=requested_job_priority if supplied_job_priority else 0,
        )

        if not existing_session:
            session_id = session_id or new_id("sess")
            effective_session_priority = requested_session_priority
            conn.execute(
                """
                INSERT INTO service_sessions(id, name, status, priority, client_id, resume_token, created_at, metadata_json)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    session_id,
                    session_name or name,
                    "queued",
                    effective_session_priority,
                    normalized_client_id,
                    resume_token or new_resume_token(),
                    created,
                    dumps(session_metadata or {}),
                ),
            )
            session_created = True
            log_event(
                conn,
                "info",
                "service_session",
                session_id,
                "service session created",
                {"name": session_name or name, "priority": effective_session_priority, "client_id": normalized_client_id},
                code="ServiceSessionCreated",
            )
        else:
            if not _verify_resume_token(existing_session["resume_token"], resume_token):
                conn.execute("ROLLBACK")
                raise CapabilityError("resume token does not match service session", {"session_id": session_id})
            effective_session_priority = _coerce_priority(existing_session["priority"], default=0)
            if session_priority is not None and requested_session_priority != effective_session_priority:
                effective_session_priority = requested_session_priority
                conn.execute("UPDATE service_sessions SET priority=? WHERE id=?", (effective_session_priority, session_id))
                # Existing queued work in the session moves with the session priority.
                conn.execute(
                    """
                    UPDATE jobs SET priority=?
                    WHERE session_id=? AND status IN ('queued','running')
                    """,
                    (effective_session_priority, session_id),
                )
                conn.execute(
                    """
                    UPDATE tasks SET priority=?, updated_at=?
                    WHERE job_id IN (SELECT id FROM jobs WHERE session_id=?)
                      AND status='queued'
                    """,
                    (effective_session_priority, created, session_id),
                )
                log_event(conn, "warning", "service_session", session_id, "service session priority updated", {"priority": effective_session_priority, "updated_by": "submit"}, code="ServiceSessionPriorityUpdated")

        effective_job_priority = requested_job_priority if supplied_job_priority else effective_session_priority
        conn.execute(
            """
            INSERT INTO jobs(id, session_id, name, task_type, status, priority, total_tasks, created_at, metadata_json)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (job_id, session_id, name, task_type, "queued", effective_job_priority, len(payloads), created, dumps(metadata or {})),
        )
        task_rows = [
            (new_id("task"), job_id, task_type, dumps(payload), "queued", effective_job_priority, max_retries, created, created)
            for payload in payloads
        ]
        conn.executemany(
            """
            INSERT INTO tasks(
                id, job_id, task_type, payload_json, status, priority,
                max_retries, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            task_rows,
        )
        capability_event = {
            "session_id": session_id,
            "tasks": len(payloads),
            "task_type": task_type,
            "priority": effective_job_priority,
            "session_priority": effective_session_priority,
            "required_tags": capability.get("required_tags", []),
            "capable_workers": [worker.get("id") for worker in capability.get("capable_workers", [])],
            "capability_warnings": capability.get("warnings", []),
            "client_id": normalized_client_id,
        }
        log_event(conn, "info", "job", job_id, "job submitted", capability_event, code="JobSubmitted")
        if capability.get("warnings"):
            log_event(conn, "warning", "job", job_id, "task capability warning", capability_event, code="TaskCapabilityWarning")
        log_event(conn, "info", "service_session", session_id, "job created in service session", {"job_id": job_id, "tasks": len(payloads), "task_type": task_type, "new_session": session_created, "job_priority": effective_job_priority, "session_priority": effective_session_priority}, code="ServiceSessionJobCreated")
        recalc_session(conn, session_id)
        conn.execute("COMMIT")
    out = get_job(job_id) or {"id": job_id, "session_id": session_id}
    session = get_session(session_id) if session_id else None
    if session:
        out["client_id"] = session.get("client_id")
        out["resume_token"] = session.get("resume_token")
    return out


def heartbeat_worker(worker_id: str, hostname: str | None = None, status: str = "idle", current_task_id: str | None = None, version: str = "dev", metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    init_db()
    hostname = hostname or socket.gethostname()
    timestamp = now()
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute("SELECT metadata_json FROM workers WHERE id=?", (worker_id,)).fetchone()
        # Internal status heartbeats should not erase capability metadata such as tags.
        metadata_json = existing["metadata_json"] if metadata is None and existing else dumps(metadata or {})
        conn.execute(
            """
            INSERT INTO workers(id, hostname, status, current_task_id, last_heartbeat_at, version, metadata_json)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                hostname=excluded.hostname,
                status=excluded.status,
                current_task_id=excluded.current_task_id,
                last_heartbeat_at=excluded.last_heartbeat_at,
                version=excluded.version,
                metadata_json=excluded.metadata_json
            """,
            (worker_id, hostname, status, current_task_id, timestamp, version, metadata_json),
        )
        config = conn.execute("SELECT * FROM worker_configs WHERE worker_id=?", (worker_id,)).fetchone()
        if not config:
            desired = _metadata_concurrency(metadata, default=1)
            conn.execute(
                """
                INSERT INTO worker_configs(worker_id, desired_concurrency, updated_at, updated_by)
                VALUES(?,?,?,?)
                """,
                (worker_id, desired, timestamp, "worker-default"),
            )
            log_event(conn, "info", "worker", worker_id, "worker config initialized", {"desired_instances": desired}, code="WorkerConfigInitialized")
        row = conn.execute(
            """
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
            FROM workers w
            LEFT JOIN worker_configs wc ON wc.worker_id = w.id
            WHERE w.id=?
            """,
            (worker_id,),
        ).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(row)


def get_worker_config(worker_id: str) -> dict[str, Any]:
    init_db()
    with connection() as conn:
        config = conn.execute("SELECT * FROM worker_configs WHERE worker_id=?", (worker_id,)).fetchone()
        if config:
            return row_to_dict(config)
        return set_worker_config(worker_id, desired_concurrency=1, updated_by="default")


def set_worker_config(worker_id: str, desired_concurrency: int, updated_by: str = "ui") -> dict[str, Any]:
    init_db()
    desired = _coerce_concurrency(desired_concurrency)
    timestamp = now()
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO worker_configs(worker_id, desired_concurrency, updated_at, updated_by)
            VALUES(?,?,?,?)
            ON CONFLICT(worker_id) DO UPDATE SET
                desired_concurrency=excluded.desired_concurrency,
                updated_at=excluded.updated_at,
                updated_by=excluded.updated_by
            """,
            (worker_id, desired, timestamp, updated_by),
        )
        log_event(conn, "warning", "worker", worker_id, "worker config updated", {"desired_instances": desired, "updated_by": updated_by}, code="WorkerConfigUpdated")
        out = conn.execute("SELECT * FROM worker_configs WHERE worker_id=?", (worker_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out)


def set_worker_enabled(worker_id: str, enabled: bool, reason: str | None = None, updated_by: str = "api") -> dict[str, Any]:
    """Enable or disable a worker node from receiving new task leases.

    Disabling is graceful: running tasks may finish, but the manager will not
    lease additional work to the node. Heartbeating workers also receive the
    disabled flag and should scale their local instance loops down to zero.
    """
    init_db()
    timestamp = now()
    disabled = 0 if enabled else 1
    clean_reason = str(reason or "").strip()[:500] or None
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute("SELECT * FROM worker_configs WHERE worker_id=?", (worker_id,)).fetchone()
        desired = int(existing["desired_concurrency"] or 1) if existing else 1
        conn.execute(
            """
            INSERT INTO worker_configs(worker_id, desired_concurrency, disabled, disabled_at, disabled_reason, updated_at, updated_by)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(worker_id) DO UPDATE SET
                disabled=excluded.disabled,
                disabled_at=excluded.disabled_at,
                disabled_reason=excluded.disabled_reason,
                updated_at=excluded.updated_at,
                updated_by=excluded.updated_by
            """,
            (worker_id, desired, disabled, None if enabled else timestamp, None if enabled else clean_reason, timestamp, updated_by),
        )
        code = "WorkerEnabled" if enabled else "WorkerDisabled"
        message = "worker enabled" if enabled else "worker disabled"
        log_event(conn, "warning", "worker", worker_id, message, {"enabled": enabled, "disabled": bool(disabled), "reason": clean_reason, "updated_by": updated_by}, code=code)
        out = conn.execute("SELECT * FROM worker_configs WHERE worker_id=?", (worker_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out)


def set_workers_enabled(worker_ids: list[str], enabled: bool, reason: str | None = None, updated_by: str = "api") -> dict[str, Any]:
    ids: list[str] = []
    seen: set[str] = set()
    for raw in worker_ids:
        worker_id = str(raw or "").strip()
        if worker_id and worker_id not in seen:
            seen.add(worker_id)
            ids.append(worker_id)
    changed = [set_worker_enabled(worker_id, enabled=enabled, reason=reason, updated_by=updated_by) for worker_id in ids]
    return {
        "enabled": enabled,
        "disabled": not enabled,
        "count": len(changed),
        "worker_ids": ids,
        "workers": changed,
    }


def _normalize_tags(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        raw = value.split(",")
    elif isinstance(value, list | tuple | set):
        raw = value
    else:
        return set()
    return {str(item).strip().lower() for item in raw if str(item).strip()}


def _required_tags(metadata: dict[str, Any] | None) -> set[str]:
    metadata = metadata or {}
    return _normalize_tags(metadata.get("required_tags") or metadata.get("required_worker_tags"))


def _worker_tags(metadata: dict[str, Any] | None) -> set[str]:
    metadata = metadata or {}
    return _normalize_tags(metadata.get("tags") or metadata.get("worker_tags"))


def _worker_task_types(metadata: dict[str, Any] | None) -> set[str]:
    metadata = metadata or {}
    raw = (
        metadata.get("task_types")
        or metadata.get("supported_task_types")
        or metadata.get("tasks")
        or []
    )
    if isinstance(raw, str):
        items = raw.split(",")
    elif isinstance(raw, list | tuple | set):
        items = raw
    else:
        items = []
    return {str(item).strip() for item in items if str(item).strip()}


def _parse_utc(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _worker_is_active(row: Any, active_seconds: int | None = None) -> bool:
    if str(row["status"] or "").lower() == "stopped":
        return False
    active_for = DEFAULT_WORKER_ACTIVE_SECONDS if active_seconds is None else int(active_seconds)
    if active_for <= 0:
        return True
    last = _parse_utc(row["last_heartbeat_at"])
    if not last:
        return False
    return datetime.now(timezone.utc) - last <= timedelta(seconds=active_for)


def _worker_capability_row(row: Any, *, active_seconds: int | None = None) -> dict[str, Any]:
    data = row_to_dict(row)
    metadata = data.get("metadata", {}) or {}
    tags = sorted(_worker_tags(metadata))
    task_types = sorted(_worker_task_types(metadata))
    data["tags"] = tags
    data["task_types"] = task_types
    data["service_name"] = metadata.get("service_name") or metadata.get("service") or ""
    data["service_version"] = metadata.get("service_version") or metadata.get("image_version") or ""
    data["disabled"] = bool(int(data.get("disabled") or 0))
    data["disabled_reason"] = data.get("disabled_reason") or ""
    data["active"] = (not data["disabled"]) and _worker_is_active(row, active_seconds=active_seconds)
    return data


def _capability_summary_from_rows(
    rows: list[Any],
    *,
    task_type: str | None = None,
    required_tags: set[str] | None = None,
    active_seconds: int | None = None,
) -> dict[str, Any]:
    required_tags = required_tags or set()
    worker_items = [_worker_capability_row(row, active_seconds=active_seconds) for row in rows]
    capable_workers: list[dict[str, Any]] = []
    supporting_workers: list[dict[str, Any]] = []
    active_workers: list[dict[str, Any]] = []
    task_catalog: dict[str, dict[str, Any]] = {}

    for worker in worker_items:
        if worker.get("active"):
            active_workers.append(worker)
        tags = set(worker.get("tags") or [])
        task_types = set(worker.get("task_types") or [])
        for registered_type in sorted(task_types):
            item = task_catalog.setdefault(
                registered_type,
                {"task_type": registered_type, "active_workers": 0, "total_workers": 0, "workers": [], "tags": set()},
            )
            item["total_workers"] += 1
            if worker.get("active"):
                item["active_workers"] += 1
            item["workers"].append(
                {
                    "id": worker["id"],
                    "status": "disabled" if worker.get("disabled") else worker["status"],
                    "active": worker["active"],
                    "disabled": bool(worker.get("disabled")),
                    "tags": worker.get("tags") or [],
                    "service_name": worker.get("service_name") or "",
                    "service_version": worker.get("service_version") or "",
                }
            )
            item["tags"].update(tags)

        if task_type and task_type in task_types:
            supporting_workers.append(worker)
            if worker.get("active") and required_tags.issubset(tags):
                capable_workers.append(worker)

    catalog_list = []
    for item in sorted(task_catalog.values(), key=lambda value: value["task_type"]):
        item = dict(item)
        item["tags"] = sorted(item["tags"])
        catalog_list.append(item)

    warnings: list[str] = []
    if task_type:
        if not supporting_workers:
            warnings.append(f"no worker has advertised task_type '{task_type}'")
        elif not capable_workers:
            warnings.append(f"no active worker supports task_type '{task_type}' with required tags {sorted(required_tags)}")

    return {
        "active_seconds": DEFAULT_WORKER_ACTIVE_SECONDS if active_seconds is None else int(active_seconds),
        "task_type": task_type,
        "required_tags": sorted(required_tags),
        "workers_total": len(worker_items),
        "workers_active": len(active_workers),
        "capable_workers": capable_workers,
        "supporting_workers": supporting_workers,
        "warnings": warnings,
        "task_types": catalog_list,
        "workers": worker_items,
    }


def get_task_catalog(task_type: str | None = None, required_tags: list[str] | set[str] | tuple[str, ...] | None = None, active_seconds: int | None = None) -> dict[str, Any]:
    init_db()
    with connection() as conn:
        rows = conn.execute("""
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
            FROM workers w
            LEFT JOIN worker_configs wc ON wc.worker_id = w.id
            ORDER BY w.last_heartbeat_at DESC
        """).fetchall()
    return _capability_summary_from_rows(
        rows,
        task_type=task_type,
        required_tags=_normalize_tags(required_tags),
        active_seconds=active_seconds,
    )


def _capability_for_submit(conn, task_type: str, metadata: dict[str, Any] | None) -> dict[str, Any]:
    required_tags = _required_tags(metadata or {})
    rows = conn.execute("""
        SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
        FROM workers w
        LEFT JOIN worker_configs wc ON wc.worker_id = w.id
        ORDER BY w.last_heartbeat_at DESC
    """).fetchall()
    return _capability_summary_from_rows(rows, task_type=task_type, required_tags=required_tags)


def expire_stale_tasks(conn) -> dict[str, Any]:
    """Requeue or fail running tasks whose leases have expired.

    This is intentionally idempotent and safe to call from worker lease requests,
    API maintenance actions, or tests. Running tasks are only touched when their
    lease is already past due. Late results from the old worker are ignored by
    complete_task/fail_task unless the task is still assigned to that executor.
    """
    timestamp = now()
    stale = conn.execute(
        """
        SELECT id, job_id, task_type, attempts, max_retries, assigned_worker_id, leased_at, lease_expires_at
        FROM tasks
        WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
        ORDER BY lease_expires_at ASC, updated_at ASC
        """,
        (timestamp,),
    ).fetchall()
    summary: dict[str, Any] = {
        "checked_at": timestamp,
        "expired": len(stale),
        "requeued": 0,
        "failed": 0,
        "tasks": [],
    }
    for task in stale:
        allowed_attempts = 1 + int(task["max_retries"])
        task_summary = {
            "task_id": task["id"],
            "job_id": task["job_id"],
            "task_type": task["task_type"],
            "assigned_worker_id": task["assigned_worker_id"],
            "attempts": int(task["attempts"] or 0),
            "max_attempts": allowed_attempts,
            "leased_at": task["leased_at"],
            "lease_expires_at": task["lease_expires_at"],
        }
        if int(task["attempts"]) < allowed_attempts:
            conn.execute(
                """
                UPDATE tasks
                SET status='queued', assigned_worker_id=NULL, leased_at=NULL, lease_expires_at=NULL,
                    error=?, updated_at=?
                WHERE id=? AND status='running'
                """,
                ("lease expired; task requeued", timestamp, task["id"]),
            )
            summary["requeued"] += 1
            task_summary["next_status"] = "queued"
            log_event(
                conn,
                "warning",
                "task",
                task["id"],
                f"lease expired from {task['assigned_worker_id']}; requeued",
                task_summary,
                code="TaskLeaseExpiredRequeued",
            )
        else:
            conn.execute(
                """
                UPDATE tasks
                SET status='failed', assigned_worker_id=NULL, lease_expires_at=NULL,
                    finished_at=?, error=?, updated_at=?
                WHERE id=? AND status='running'
                """,
                (timestamp, "lease expired; retries exhausted", timestamp, task["id"]),
            )
            summary["failed"] += 1
            task_summary["next_status"] = "failed"
            log_event(
                conn,
                "error",
                "task",
                task["id"],
                f"lease expired from {task['assigned_worker_id']}; retries exhausted",
                task_summary,
                code="TaskLeaseExpiredFailed",
            )
        summary["tasks"].append(task_summary)
        recalc_job(conn, task["job_id"])
    return summary


def _worker_running_assignment_count(conn: Any, worker_id: str) -> int:
    """Return running tasks currently assigned to this worker node or its instances."""
    prefix = f"{worker_id}-"
    rows = conn.execute(
        "SELECT assigned_worker_id FROM tasks WHERE status='running' AND assigned_worker_id IS NOT NULL"
    ).fetchall()
    count = 0
    for row in rows:
        assigned = str(row["assigned_worker_id"] or "")
        if assigned == worker_id or assigned.startswith(prefix):
            count += 1
    return count


def purge_offline_workers(active_seconds: int | None = None, include_running: bool = False, updated_by: str = "api") -> dict[str, Any]:
    """Delete stale worker rows/configs from the manager DB.

    Worker rows are historical manager records created by heartbeats. Purging them
    removes stale UI clutter only; it does not delete manager events, task rows,
    or remote worker log files. By default, workers with running task assignments
    are skipped so operators can recover expired leases first.
    """
    init_db()
    threshold = DEFAULT_WORKER_ACTIVE_SECONDS if active_seconds is None else int(active_seconds)
    timestamp = now()
    purged: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute("SELECT * FROM workers ORDER BY last_heartbeat_at DESC").fetchall()
        for row in rows:
            worker = _worker_capability_row(row, active_seconds=threshold)
            if worker.get("active"):
                continue
            worker_id = str(worker.get("id") or "")
            running_assignments = _worker_running_assignment_count(conn, worker_id)
            item = {
                "id": worker_id,
                "hostname": worker.get("hostname"),
                "status": worker.get("status"),
                "last_heartbeat_at": worker.get("last_heartbeat_at"),
                "running_assignments": running_assignments,
            }
            if running_assignments and not include_running:
                item["reason"] = "running_assignments"
                skipped.append(item)
                continue
            conn.execute("DELETE FROM worker_configs WHERE worker_id=?", (worker_id,))
            conn.execute("DELETE FROM workers WHERE id=?", (worker_id,))
            purged.append(item)
            log_event(
                conn,
                "warning",
                "worker",
                worker_id,
                "offline worker purged from manager registry",
                {**item, "active_seconds": threshold, "updated_by": updated_by},
                code="WorkerPurgedOffline",
            )
        summary = {
            "checked_at": timestamp,
            "active_seconds": threshold,
            "include_running": bool(include_running),
            "purged_count": len(purged),
            "skipped_count": len(skipped),
            "purged_workers": purged,
            "skipped_workers": skipped,
            "updated_by": updated_by,
        }
        log_event(
            conn,
            "info",
            "manager",
            "workers",
            f"offline worker purge run; purged {len(purged)} skipped {len(skipped)}",
            summary,
            code="WorkerPurgeOfflineRun",
        )
        conn.execute("COMMIT")
        return summary


def recovery_status(active_seconds: int | None = None) -> dict[str, Any]:
    """Return manager-side health/recovery state without mutating task state."""
    init_db()
    timestamp = now()
    with connection() as conn:
        worker_rows = conn.execute("SELECT * FROM workers ORDER BY last_heartbeat_at DESC").fetchall()
        workers = [_worker_capability_row(row, active_seconds=active_seconds) for row in worker_rows]
        stale_workers = [worker for worker in workers if not worker.get("active")]
        running = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM tasks
            WHERE status='running'
            """
        ).fetchone()["count"]
        expired_rows = conn.execute(
            """
            SELECT t.id, t.job_id, t.task_type, t.assigned_worker_id, t.attempts, t.max_retries,
                   t.leased_at, t.lease_expires_at, t.updated_at, j.session_id, j.name AS job_name
            FROM tasks t
            JOIN jobs j ON j.id=t.job_id
            WHERE t.status='running' AND t.lease_expires_at IS NOT NULL AND t.lease_expires_at < ?
            ORDER BY t.lease_expires_at ASC
            LIMIT 200
            """,
            (timestamp,),
        ).fetchall()
        expired_tasks = [row_to_dict(row) for row in expired_rows]
        failed_due_to_lease = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM events
            WHERE code='TaskLeaseExpiredFailed'
            """
        ).fetchone()["count"]
        requeued_due_to_lease = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM events
            WHERE code='TaskLeaseExpiredRequeued'
            """
        ).fetchone()["count"]
    return {
        "checked_at": timestamp,
        "active_seconds": DEFAULT_WORKER_ACTIVE_SECONDS if active_seconds is None else int(active_seconds),
        "workers_total": len(workers),
        "workers_active": len(workers) - len(stale_workers),
        "workers_stale": len(stale_workers),
        "stale_workers": stale_workers,
        "running_tasks": int(running or 0),
        "expired_running_tasks": len(expired_tasks),
        "expired_tasks": expired_tasks,
        "lease_requeue_events": int(requeued_due_to_lease or 0),
        "lease_failed_events": int(failed_due_to_lease or 0),
    }


def recover_expired_leases(updated_by: str = "api") -> dict[str, Any]:
    """Manager maintenance action to immediately process expired task leases."""
    init_db()
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        summary = expire_stale_tasks(conn)
        log_event(
            conn,
            "info",
            "manager",
            "maintenance",
            "expired lease recovery run",
            {**summary, "updated_by": updated_by},
            code="MaintenanceRecoveryRun",
        )
        conn.execute("COMMIT")
    summary["status"] = recovery_status()
    return summary


def lease_tasks(worker_id: str, limit: int = 1, lease_seconds: int = 60, instance_id: str | None = None) -> list[dict[str, Any]]:
    init_db()
    # The node supervisor owns worker heartbeats. Avoid heartbeat writes on every
    # lease request; under scale, per-instance polling would otherwise double the
    # write load on the manager DB.
    timestamp = now()
    assigned_id = executor_id(worker_id, instance_id)
    lease_until = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat(timespec="seconds")
    leased: list[dict[str, Any]] = []
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        expire_stale_tasks(conn)
        worker = conn.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
        if not worker:
            # Direct core/API tests may lease before a supervisor heartbeat. Runtime
            # worker nodes heartbeat first, so this path is only a compatibility
            # fallback and does not add per-lease writes for normal workers.
            conn.execute(
                """
                INSERT INTO workers(id, hostname, status, current_task_id, last_heartbeat_at, version, metadata_json)
                VALUES(?,?,?,?,?,?,?)
                """,
                (worker_id, socket.gethostname(), "leasing", None, timestamp, "dev", dumps({})),
            )
            conn.execute(
                """
                INSERT INTO worker_configs(worker_id, desired_concurrency, updated_at, updated_by)
                VALUES(?,?,?,?)
                ON CONFLICT(worker_id) DO NOTHING
                """,
                (worker_id, 1, timestamp, "lease-fallback"),
            )
            worker = conn.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
        config = conn.execute("SELECT * FROM worker_configs WHERE worker_id=?", (worker_id,)).fetchone()
        if config and int(config["disabled"] or 0):
            conn.execute("COMMIT")
            return []
        worker_metadata = row_to_dict(worker).get("metadata", {}) if worker else {}
        tags = _worker_tags(worker_metadata)
        supported_task_types = _worker_task_types(worker_metadata)
        # Fetch extra candidates because some queued tasks may require tags or task
        # types this worker does not have. If a worker has advertised task types,
        # do not lease unsupported work to it.
        rows = conn.execute(
            """
            SELECT t.*, j.metadata_json AS job_metadata_json, j.session_id AS job_session_id, j.status AS job_status
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            LEFT JOIN service_sessions s ON s.id = j.session_id
            WHERE t.status = 'queued'
              AND j.status NOT IN ('cancelled','cancelling')
              AND COALESCE(j.paused, 0) = 0
              AND (s.id IS NULL OR COALESCE(s.paused, 0) = 0)
            ORDER BY t.priority DESC, t.created_at ASC
            LIMIT ?
            """,
            (max(limit * LEASE_CANDIDATE_MULTIPLIER, limit),),
        ).fetchall()
        for row in rows:
            if supported_task_types and row["task_type"] not in supported_task_types:
                continue
            job_metadata = loads(row["job_metadata_json"], {})
            required = _required_tags(job_metadata)
            if required and not required.issubset(tags):
                continue
            conn.execute(
                """
                UPDATE tasks
                SET status='running', attempts=attempts+1, assigned_worker_id=?,
                    leased_at=?, lease_expires_at=?, started_at=COALESCE(started_at, ?), updated_at=?
                WHERE id=? AND status='queued'
                """,
                (assigned_id, timestamp, lease_until, timestamp, timestamp, row["id"]),
            )
            updated = conn.execute("SELECT * FROM tasks WHERE id=?", (row["id"],)).fetchone()
            if updated:
                leased.append(row_to_dict(updated))
                log_event(conn, "info", "task", row["id"], "task accepted by worker", execution_event_data(worker_id, instance_id, job_id=row["job_id"], task_type=row["task_type"], worker_tags=sorted(tags), required_tags=sorted(required)), code="TaskAccepted")
                _mark_task_started_incremental(conn, row["job_id"], row["job_session_id"], timestamp)
            if len(leased) >= limit:
                break
        conn.execute("COMMIT")
    return leased




def lease_tasks_for_instances(worker_id: str, instance_ids: list[str], lease_seconds: int = 60) -> list[dict[str, Any]]:
    """Lease at most one task per supplied logical instance in one transaction.

    This is the node-level batch leasing path. It reduces manager HTTP requests
    while keeping task assignment precise: every leased task is assigned to the
    concrete executor id ``worker-id-instance-id``. Existing per-instance leasing
    remains available through ``lease_tasks``.
    """
    init_db()
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in instance_ids[:MAX_WORKER_CONCURRENCY]:
        item = str(raw or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        cleaned.append(item)
    if not cleaned:
        return []
    timestamp = now()
    lease_until = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat(timespec="seconds")
    leased: list[dict[str, Any]] = []
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        expire_stale_tasks(conn)
        worker = conn.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
        if not worker:
            conn.execute(
                """
                INSERT INTO workers(id, hostname, status, current_task_id, last_heartbeat_at, version, metadata_json)
                VALUES(?,?,?,?,?,?,?)
                """,
                (worker_id, socket.gethostname(), "leasing", None, timestamp, "dev", dumps({})),
            )
            conn.execute(
                """
                INSERT INTO worker_configs(worker_id, desired_concurrency, updated_at, updated_by)
                VALUES(?,?,?,?)
                ON CONFLICT(worker_id) DO NOTHING
                """,
                (worker_id, max(1, len(cleaned)), timestamp, "lease-fallback"),
            )
            worker = conn.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
        config = conn.execute("SELECT * FROM worker_configs WHERE worker_id=?", (worker_id,)).fetchone()
        if config and int(config["disabled"] or 0):
            conn.execute("COMMIT")
            return []
        worker_metadata = row_to_dict(worker).get("metadata", {}) if worker else {}
        tags = _worker_tags(worker_metadata)
        supported_task_types = _worker_task_types(worker_metadata)
        rows = conn.execute(
            """
            SELECT t.*, j.metadata_json AS job_metadata_json, j.session_id AS job_session_id, j.status AS job_status
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            LEFT JOIN service_sessions s ON s.id = j.session_id
            WHERE t.status = 'queued'
              AND j.status NOT IN ('cancelled','cancelling')
              AND COALESCE(j.paused, 0) = 0
              AND (s.id IS NULL OR COALESCE(s.paused, 0) = 0)
            ORDER BY t.priority DESC, t.created_at ASC
            LIMIT ?
            """,
            (max(len(cleaned) * LEASE_CANDIDATE_MULTIPLIER, len(cleaned)),),
        ).fetchall()
        instance_iter = iter(cleaned)
        current_instance = next(instance_iter, None)
        for row in rows:
            if current_instance is None:
                break
            if supported_task_types and row["task_type"] not in supported_task_types:
                continue
            job_metadata = loads(row["job_metadata_json"], {})
            required = _required_tags(job_metadata)
            if required and not required.issubset(tags):
                continue
            assigned_id = executor_id(worker_id, current_instance)
            conn.execute(
                """
                UPDATE tasks
                SET status='running', attempts=attempts+1, assigned_worker_id=?,
                    leased_at=?, lease_expires_at=?, started_at=COALESCE(started_at, ?), updated_at=?
                WHERE id=? AND status='queued'
                """,
                (assigned_id, timestamp, lease_until, timestamp, timestamp, row["id"]),
            )
            updated = conn.execute("SELECT * FROM tasks WHERE id=?", (row["id"],)).fetchone()
            if updated:
                item = row_to_dict(updated)
                item["leased_instance_id"] = current_instance
                leased.append(item)
                log_event(
                    conn,
                    "info",
                    "task",
                    row["id"],
                    "task accepted by worker",
                    execution_event_data(worker_id, current_instance, job_id=row["job_id"], task_type=row["task_type"], worker_tags=sorted(tags), required_tags=sorted(required), batch=True),
                    code="TaskAccepted",
                )
                _mark_task_started_incremental(conn, row["job_id"], row["job_session_id"], timestamp)
                current_instance = next(instance_iter, None)
        conn.execute("COMMIT")
    return leased

def complete_task(task_id: str, worker_id: str, result: Any, instance_id: str | None = None) -> dict[str, Any] | None:
    init_db()
    timestamp = now()
    assigned_id = executor_id(worker_id, instance_id)
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        task = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not task:
            conn.execute("COMMIT")
            return None
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (task["job_id"],)).fetchone()
        if job and job["status"] == "cancelled":
            conn.execute(
                """
                UPDATE tasks SET status='cancelled', finished_at=?, updated_at=?, lease_expires_at=NULL
                WHERE id=?
                """,
                (timestamp, timestamp, task_id),
            )
            log_event(conn, "info", "task", task_id, "worker completed after job cancellation; kept cancelled", execution_event_data(worker_id, instance_id, job_id=task["job_id"]), code="TaskCompletedAfterCancellation")
        elif task["status"] == "running" and task["assigned_worker_id"] in {assigned_id, worker_id}:
            conn.execute(
                """
                UPDATE tasks
                SET status='succeeded', result_json=?, error=NULL, finished_at=?, updated_at=?, lease_expires_at=NULL
                WHERE id=?
                """,
                (dumps(result), timestamp, timestamp, task_id),
            )
            log_event(conn, "info", "task", task_id, "task completed", execution_event_data(worker_id, instance_id, job_id=task["job_id"], task_type=task["task_type"], attempts=int(task["attempts"] or 0)), code="TaskCompleted")
            if job and job["status"] == CANCELLING_JOB_STATUS:
                recalc_job(conn, task["job_id"])
            else:
                _mark_task_completed_incremental(conn, task["job_id"], job["session_id"] if job and "session_id" in job.keys() else None, timestamp)
        else:
            reason = "task_not_running" if task["status"] != "running" else "assigned_worker_mismatch"
            log_event(
                conn,
                "warning",
                "task",
                task_id,
                f"late or duplicate task result ignored from {assigned_id}",
                execution_event_data(
                    worker_id,
                    instance_id,
                    job_id=task["job_id"],
                    task_type=task["task_type"],
                    reason=reason,
                    current_status=task["status"],
                    assigned_worker_id=task["assigned_worker_id"],
                ),
                code="TaskResultIgnored",
            )
        if not (task["status"] == "running" and task["assigned_worker_id"] in {assigned_id, worker_id} and not (job and job["status"] == "cancelled")):
            recalc_job(conn, task["job_id"])
        out = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out) if out else None


def fail_task(task_id: str, worker_id: str, error: str, instance_id: str | None = None) -> dict[str, Any] | None:
    init_db()
    timestamp = now()
    assigned_id = executor_id(worker_id, instance_id)
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        task = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not task:
            conn.execute("COMMIT")
            return None
        if task["status"] != "running" or task["assigned_worker_id"] not in {assigned_id, worker_id}:
            reason = "task_not_running" if task["status"] != "running" else "assigned_worker_mismatch"
            log_event(
                conn,
                "warning",
                "task",
                task_id,
                f"late or duplicate task failure ignored from {assigned_id}",
                execution_event_data(
                    worker_id,
                    instance_id,
                    job_id=task["job_id"],
                    task_type=task["task_type"],
                    reason=reason,
                    current_status=task["status"],
                    assigned_worker_id=task["assigned_worker_id"],
                    error=error[:1000],
                ),
                code="TaskFailureIgnored",
            )
            out = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            conn.execute("COMMIT")
            return row_to_dict(out) if out else None

        allowed_attempts = 1 + int(task["max_retries"])
        if int(task["attempts"]) < allowed_attempts:
            next_status = "queued"
            finished_at = None
            assigned_worker = None
            leased_at = None
            lease_expires = None
            message = "task failed; requeued"
            level = "warning"
        else:
            next_status = "failed"
            finished_at = timestamp
            assigned_worker = None
            leased_at = None
            lease_expires = None
            message = "task failed; retries exhausted"
            level = "error"

        conn.execute(
            """
            UPDATE tasks
            SET status=?, assigned_worker_id=?, leased_at=?, lease_expires_at=?, finished_at=?, error=?, updated_at=?
            WHERE id=?
            """,
            (next_status, assigned_worker, leased_at, lease_expires, finished_at, error[:4000], timestamp, task_id),
        )
        log_event(conn, level, "task", task_id, message, execution_event_data(worker_id, instance_id, job_id=task["job_id"], task_type=task["task_type"], next_status=next_status, error=error[:1000]), code="TaskFailedRequeued" if next_status == "queued" else "TaskFailed")
        recalc_job(conn, task["job_id"])
        out = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out) if out else None


def retry_task(task_id: str, reset_attempts: bool = True) -> dict[str, Any] | None:
    init_db()
    timestamp = now()
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        task = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not task:
            conn.execute("COMMIT")
            return None
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (task["job_id"],)).fetchone()
        if job and job["status"] in {"cancelled", CANCELLING_JOB_STATUS}:
            out = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            conn.execute("COMMIT")
            return row_to_dict(out) if out else None
        if task["status"] in {"failed", "cancelled"}:
            attempts_sql = "attempts=0," if reset_attempts else ""
            conn.execute(
                f"""
                UPDATE tasks
                SET status='queued', {attempts_sql} assigned_worker_id=NULL, leased_at=NULL,
                    lease_expires_at=NULL, finished_at=NULL, error=NULL, updated_at=?
                WHERE id=?
                """,
                (timestamp, task_id),
            )
            if job and job["status"] in TERMINAL_JOB_STATUSES and job["status"] != "cancelled":
                conn.execute("UPDATE jobs SET status='queued', finished_at=NULL WHERE id=?", (task["job_id"],))
            log_event(conn, "warning", "task", task_id, "task manually requeued", {"job_id": task["job_id"], "reset_attempts": reset_attempts}, code="TaskRequeuedManual")
            recalc_job(conn, task["job_id"])
        out = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out) if out else None


def retry_failed_tasks(job_id: str, reset_attempts: bool = True) -> dict[str, Any] | None:
    init_db()
    timestamp = now()
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            conn.execute("COMMIT")
            return None
        if job["status"] in {"cancelled", CANCELLING_JOB_STATUS}:
            out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            conn.execute("COMMIT")
            return row_to_dict(out) if out else None
        failed_count = conn.execute("SELECT COUNT(*) AS count FROM tasks WHERE job_id=? AND status='failed'", (job_id,)).fetchone()["count"]
        attempts_sql = "attempts=0," if reset_attempts else ""
        conn.execute(
            f"""
            UPDATE tasks
            SET status='queued', {attempts_sql} assigned_worker_id=NULL, leased_at=NULL,
                lease_expires_at=NULL, finished_at=NULL, error=NULL, updated_at=?
            WHERE job_id=? AND status='failed'
            """,
            (timestamp, job_id),
        )
        if failed_count:
            conn.execute("UPDATE jobs SET status='queued', finished_at=NULL WHERE id=?", (job_id,))
            log_event(conn, "warning", "job", job_id, "failed tasks manually requeued", {"tasks": int(failed_count), "reset_attempts": reset_attempts}, code="FailedTasksRequeuedManual")
            recalc_job(conn, job_id)
        out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out) if out else None


def cancel_job(job_id: str, mode: str = "graceful") -> dict[str, Any] | None:
    """Cancel a job.

    Graceful cancellation is the default: queued tasks are cancelled immediately,
    running tasks are allowed to finish, and the job stays in ``cancelling``
    until those running tasks report back. Force cancellation marks both queued
    and running tasks cancelled immediately.
    """
    init_db()
    timestamp = now()
    normalized_mode = str(mode or "graceful").strip().lower()
    force = normalized_mode in {"force", "immediate", "hard"}
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            conn.execute("COMMIT")
            return None
        if job["status"] in TERMINAL_JOB_STATUSES:
            out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            conn.execute("COMMIT")
            return row_to_dict(out) if out else None

        counts = conn.execute(
            """
            SELECT
              SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued,
              SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running
            FROM tasks WHERE job_id=?
            """,
            (job_id,),
        ).fetchone()
        queued_count = int(counts["queued"] or 0)
        running_count = int(counts["running"] or 0)

        if force or running_count == 0:
            conn.execute(
                """
                UPDATE jobs SET status='cancelled', cancelled_at=?, finished_at=COALESCE(finished_at, ?) WHERE id=?
                """,
                (timestamp, timestamp, job_id),
            )
            conn.execute(
                """
                UPDATE tasks
                SET status='cancelled', finished_at=COALESCE(finished_at, ?), lease_expires_at=NULL, updated_at=?
                WHERE job_id=? AND status IN ('queued','running')
                """,
                (timestamp, timestamp, job_id),
            )
            log_event(conn, "warning", "job", job_id, "job force-cancelled" if force else "job cancelled", {"mode": "force" if force else "graceful", "queued_cancelled": queued_count, "running_cancelled": running_count}, code="JobCancelled")
        else:
            conn.execute(
                """
                UPDATE jobs SET status='cancelling', cancelled_at=?, finished_at=NULL WHERE id=?
                """,
                (timestamp, job_id),
            )
            conn.execute(
                """
                UPDATE tasks
                SET status='cancelled', finished_at=COALESCE(finished_at, ?), lease_expires_at=NULL, updated_at=?
                WHERE job_id=? AND status='queued'
                """,
                (timestamp, timestamp, job_id),
            )
            log_event(conn, "warning", "job", job_id, "job cancellation requested", {"mode": "graceful", "queued_cancelled": queued_count, "running_allowed_to_finish": running_count}, code="JobCancellationRequested")
        recalc_job(conn, job_id)
        out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out) if out else None



def set_job_paused(job_id: str, paused: bool, reason: str | None = None, updated_by: str = "api") -> dict[str, Any] | None:
    """Pause or resume a job without cancelling running work.

    Paused jobs keep their queued tasks queued, but the scheduler will not lease
    them. Running tasks are allowed to finish. Resuming makes remaining queued
    tasks eligible again, subject to session pause state, worker tags, and
    priority.
    """
    init_db()
    timestamp = now()
    clean_reason = str(reason or "").strip()[:500] or None
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            conn.execute("COMMIT")
            return None
        if job["status"] in TERMINAL_JOB_STATUSES:
            out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            conn.execute("COMMIT")
            return row_to_dict(out) if out else None
        conn.execute(
            """
            UPDATE jobs
            SET paused=?, paused_at=?, pause_reason=?
            WHERE id=?
            """,
            (1 if paused else 0, timestamp if paused else None, clean_reason if paused else None, job_id),
        )
        code = "JobPaused" if paused else "JobResumed"
        message = "job paused" if paused else "job resumed"
        log_event(conn, "warning", "job", job_id, message, {"paused": paused, "reason": clean_reason, "updated_by": updated_by}, code=code)
        out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out) if out else None


def set_session_paused(session_id: str, paused: bool, reason: str | None = None, updated_by: str = "api") -> dict[str, Any] | None:
    """Pause or resume a service session.

    Session pause is a top-level hold. It does not overwrite per-job pause
    state; resuming the session does not accidentally resume jobs that were
    individually paused.
    """
    init_db()
    timestamp = now()
    clean_reason = str(reason or "").strip()[:500] or None
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        session = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        if not session:
            conn.execute("COMMIT")
            return None
        if session["status"] in TERMINAL_JOB_STATUSES:
            out = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
            conn.execute("COMMIT")
            return row_to_dict(out) if out else None
        conn.execute(
            """
            UPDATE service_sessions
            SET paused=?, paused_at=?, pause_reason=?
            WHERE id=?
            """,
            (1 if paused else 0, timestamp if paused else None, clean_reason if paused else None, session_id),
        )
        code = "ServiceSessionPaused" if paused else "ServiceSessionResumed"
        message = "service session paused" if paused else "service session resumed"
        log_event(conn, "warning", "service_session", session_id, message, {"paused": paused, "reason": clean_reason, "updated_by": updated_by}, code=code)
        out = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out) if out else None


def set_sessions_paused(session_ids: list[str], paused: bool, reason: str | None = None, updated_by: str = "api") -> dict[str, Any]:
    ids: list[str] = []
    seen: set[str] = set()
    for raw in session_ids:
        session_id = str(raw or "").strip()
        if session_id and session_id not in seen:
            seen.add(session_id)
            ids.append(session_id)
    changed = [item for item in (set_session_paused(session_id, paused, reason=reason, updated_by=updated_by) for session_id in ids) if item]
    return {"paused": paused, "count": len(changed), "session_ids": ids, "sessions": changed}


def get_job(job_id: str) -> dict[str, Any] | None:
    init_db()
    with connection() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return row_to_dict(row) if row else None



def set_session_priority(session_id: str, priority: int, updated_by: str = "api") -> dict[str, Any] | None:
    init_db()
    new_priority = _coerce_priority(priority, default=0)
    timestamp = now()
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        session = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        if not session:
            conn.execute("COMMIT")
            return None
        old_priority = _coerce_priority(session["priority"], default=0)
        if old_priority != new_priority:
            conn.execute("UPDATE service_sessions SET priority=? WHERE id=?", (new_priority, session_id))
            conn.execute(
                """
                UPDATE jobs SET priority=?
                WHERE session_id=? AND status IN ('queued','running')
                """,
                (new_priority, session_id),
            )
            queued = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM tasks
                WHERE job_id IN (SELECT id FROM jobs WHERE session_id=?)
                  AND status='queued'
                """,
                (session_id,),
            ).fetchone()["count"]
            conn.execute(
                """
                UPDATE tasks SET priority=?, updated_at=?
                WHERE job_id IN (SELECT id FROM jobs WHERE session_id=?)
                  AND status='queued'
                """,
                (new_priority, timestamp, session_id),
            )
            log_event(
                conn,
                "warning",
                "service_session",
                session_id,
                "service session priority updated",
                {"old_priority": old_priority, "priority": new_priority, "queued_tasks_reprioritized": int(queued or 0), "updated_by": updated_by},
                code="ServiceSessionPriorityUpdated",
            )
        out = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out) if out else None


def get_session(session_id: str) -> dict[str, Any] | None:
    init_db()
    with connection() as conn:
        row = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        return row_to_dict(row) if row else None


def list_sessions(limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
    init_db()
    with connection() as conn:
        if status:
            rows = conn.execute(
                "SELECT * FROM service_sessions WHERE status=? ORDER BY priority DESC, created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM service_sessions ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [row_to_dict(row) for row in rows]


def list_client_sessions(client_id: str, resume_token: str, limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
    init_db()
    normalized_client_id = _normalize_client_id(client_id)
    if not normalized_client_id or not resume_token:
        return []
    with connection() as conn:
        if status:
            rows = conn.execute(
                """
                SELECT * FROM service_sessions
                WHERE client_id=? AND resume_token=? AND status=?
                ORDER BY priority DESC, created_at DESC LIMIT ?
                """,
                (normalized_client_id, resume_token, status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM service_sessions
                WHERE client_id=? AND resume_token=?
                ORDER BY created_at DESC LIMIT ?
                """,
                (normalized_client_id, resume_token, limit),
            ).fetchall()
        sessions = [row_to_dict(row) for row in rows]
        if sessions:
            log_event(
                conn,
                "info",
                "client",
                normalized_client_id,
                "client listed resumable sessions",
                {"sessions": len(sessions)},
                code="ClientSessionsListed",
            )
        return sessions


def get_client_session(client_id: str, resume_token: str, session_id: str) -> dict[str, Any] | None:
    init_db()
    normalized_client_id = _normalize_client_id(client_id)
    if not normalized_client_id or not resume_token:
        return None
    with connection() as conn:
        row = conn.execute(
            """
            SELECT * FROM service_sessions
            WHERE id=? AND client_id=? AND resume_token=?
            """,
            (session_id, normalized_client_id, resume_token),
        ).fetchone()
        if row:
            log_event(
                conn,
                "info",
                "service_session",
                session_id,
                "client resumed service session",
                {"client_id": normalized_client_id},
                code="ClientSessionResumed",
            )
        return row_to_dict(row) if row else None


def list_session_jobs(session_id: str, limit: int = 500) -> list[dict[str, Any]]:
    init_db()
    with connection() as conn:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE session_id=? ORDER BY created_at ASC LIMIT ?",
            (session_id, limit),
        ).fetchall()
        return [row_to_dict(row) for row in rows]

def list_jobs(limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
    init_db()
    with connection() as conn:
        if status:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [row_to_dict(row) for row in rows]


def get_task(task_id: str) -> dict[str, Any] | None:
    init_db()
    with connection() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return row_to_dict(row) if row else None




def _json_byte_size(value: Any) -> int:
    try:
        return len(dumps(value).encode("utf-8"))
    except Exception:
        return 0


def _task_result_row(task: dict[str, Any]) -> dict[str, Any]:
    payload = task.get("payload", {})
    result = task.get("result")
    return {
        "task_id": task["id"],
        "job_id": task["job_id"],
        "task_type": task["task_type"],
        "status": task["status"],
        "attempts": task["attempts"],
        "max_retries": task["max_retries"],
        "assigned_worker_id": task.get("assigned_worker_id"),
        "created_at": task.get("created_at"),
        "started_at": task.get("started_at"),
        "finished_at": task.get("finished_at"),
        "updated_at": task.get("updated_at"),
        "payload": payload,
        "payload_bytes": _json_byte_size(payload),
        "result": result,
        "result_bytes": _json_byte_size(result) if result is not None else 0,
        "error": task.get("error"),
    }


def get_job_results(job_id: str) -> dict[str, Any] | None:
    init_db()
    with connection() as conn:
        job_row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job_row:
            return None
        job = row_to_dict(job_row)
        task_rows = conn.execute("SELECT * FROM tasks WHERE job_id=? ORDER BY created_at ASC", (job_id,)).fetchall()
        tasks = [_task_result_row(row_to_dict(row)) for row in task_rows]
        return {
            "job_id": job["id"],
            "session_id": job.get("session_id"),
            "name": job["name"],
            "task_type": job["task_type"],
            "status": job["status"],
            "priority": job["priority"],
            "total_tasks": job["total_tasks"],
            "completed_tasks": job["completed_tasks"],
            "failed_tasks": job["failed_tasks"],
            "cancelled_tasks": job["cancelled_tasks"],
            "created_at": job.get("created_at"),
            "started_at": job.get("started_at"),
            "finished_at": job.get("finished_at"),
            "metadata": job.get("metadata", {}),
            "tasks": tasks,
            # Backwards-compatible compact shape used by early client code.
            "results": [
                {"task_id": item["task_id"], "status": item["status"], "result": item.get("result"), "error": item.get("error")}
                for item in tasks
            ],
        }


def get_session_results(session_id: str) -> dict[str, Any] | None:
    init_db()
    with connection() as conn:
        session_row = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        if not session_row:
            return None
        session = row_to_dict(session_row)
        job_rows = conn.execute("SELECT * FROM jobs WHERE session_id=? ORDER BY created_at ASC", (session_id,)).fetchall()
        jobs: list[dict[str, Any]] = []
        all_tasks: list[dict[str, Any]] = []
        for job_row in job_rows:
            job = row_to_dict(job_row)
            task_rows = conn.execute("SELECT * FROM tasks WHERE job_id=? ORDER BY created_at ASC", (job["id"],)).fetchall()
            tasks = [_task_result_row(row_to_dict(row)) for row in task_rows]
            all_tasks.extend(tasks)
            jobs.append(
                {
                    "job_id": job["id"],
                    "name": job["name"],
                    "task_type": job["task_type"],
                    "status": job["status"],
                    "priority": job.get("priority", 0),
                    "total_tasks": job["total_tasks"],
                    "completed_tasks": job["completed_tasks"],
                    "failed_tasks": job["failed_tasks"],
                    "cancelled_tasks": job["cancelled_tasks"],
                    "created_at": job.get("created_at"),
                    "started_at": job.get("started_at"),
                    "finished_at": job.get("finished_at"),
                    "metadata": job.get("metadata", {}),
                    "tasks": tasks,
                }
            )
        return {
            "session_id": session["id"],
            "name": session["name"],
            "status": session["status"],
            "priority": session.get("priority", 0),
            "total_jobs": session.get("total_jobs", len(jobs)),
            "total_tasks": session.get("total_tasks", len(all_tasks)),
            "queued_tasks": session.get("queued_tasks", 0),
            "running_tasks": session.get("running_tasks", 0),
            "completed_tasks": session.get("completed_tasks", 0),
            "failed_tasks": session.get("failed_tasks", 0),
            "cancelled_tasks": session.get("cancelled_tasks", 0),
            "created_at": session.get("created_at"),
            "started_at": session.get("started_at"),
            "finished_at": session.get("finished_at"),
            "metadata": session.get("metadata", {}),
            "jobs": jobs,
            "tasks": all_tasks,
            "results": [
                {"task_id": item["task_id"], "job_id": item["job_id"], "status": item["status"], "result": item.get("result"), "error": item.get("error")}
                for item in all_tasks
            ],
        }



def _ordered_result_tasks(items: list[dict[str, Any]], order: str = "input") -> list[dict[str, Any]]:
    key = str(order or "input").lower()
    if key in {"completed", "finished", "finished_at"}:
        return sorted(items, key=lambda item: (item.get("finished_at") or "9999", item.get("created_at") or "", item.get("task_id") or ""))
    if key in {"started", "started_at"}:
        return sorted(items, key=lambda item: (item.get("started_at") or "9999", item.get("created_at") or "", item.get("task_id") or ""))
    if key == "status":
        return sorted(items, key=lambda item: (item.get("status") or "", item.get("created_at") or "", item.get("task_id") or ""))
    return list(items)


def _results_csv_rows(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in tasks:
        rows.append(
            {
                "task_id": item.get("task_id"),
                "job_id": item.get("job_id"),
                "task_type": item.get("task_type"),
                "status": item.get("status"),
                "attempts": item.get("attempts"),
                "max_retries": item.get("max_retries"),
                "assigned_worker_id": item.get("assigned_worker_id"),
                "created_at": item.get("created_at"),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
                "payload_json": dumps(item.get("payload", {})),
                "result_json": dumps(item.get("result")) if item.get("result") is not None else "",
                "error": item.get("error") or "",
            }
        )
    return rows


def results_to_csv(tasks: list[dict[str, Any]]) -> str:
    fields = [
        "task_id",
        "job_id",
        "task_type",
        "status",
        "attempts",
        "max_retries",
        "assigned_worker_id",
        "created_at",
        "started_at",
        "finished_at",
        "payload_json",
        "result_json",
        "error",
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(_results_csv_rows(tasks))
    return output.getvalue()


def export_job_results(job_id: str, *, fmt: str = "json", order: str = "input", failed_only: bool = False) -> tuple[str, str, str] | None:
    data = get_job_results(job_id)
    if not data:
        return None
    tasks = _ordered_result_tasks(data.get("tasks", []), order=order)
    if failed_only:
        tasks = [item for item in tasks if item.get("status") == "failed" or item.get("error")]
    data = dict(data)
    data["tasks"] = tasks
    data["results"] = [
        {"task_id": item["task_id"], "status": item["status"], "result": item.get("result"), "error": item.get("error")}
        for item in tasks
    ]
    safe_name = _safe_log_part(data.get("name") or job_id)
    if str(fmt).lower() == "csv":
        suffix = "failed-tasks" if failed_only else "results"
        return results_to_csv(tasks), "text/csv; charset=utf-8", f"taskgrid-job-{safe_name}-{suffix}.csv"
    suffix = "failed-tasks" if failed_only else "results"
    return dumps(data), "application/json; charset=utf-8", f"taskgrid-job-{safe_name}-{suffix}.json"


def export_session_results(session_id: str, *, fmt: str = "json", order: str = "input", failed_only: bool = False) -> tuple[str, str, str] | None:
    data = get_session_results(session_id)
    if not data:
        return None
    tasks = _ordered_result_tasks(data.get("tasks", []), order=order)
    if failed_only:
        tasks = [item for item in tasks if item.get("status") == "failed" or item.get("error")]
    data = dict(data)
    data["tasks"] = tasks
    data["results"] = [
        {"task_id": item["task_id"], "job_id": item.get("job_id"), "status": item["status"], "result": item.get("result"), "error": item.get("error")}
        for item in tasks
    ]
    # Keep nested job summaries but do not duplicate every task inside each job for failed-only exports.
    if failed_only:
        data["jobs"] = [
            {k: v for k, v in job.items() if k != "tasks"}
            for job in data.get("jobs", [])
        ]
    safe_name = _safe_log_part(data.get("name") or session_id)
    if str(fmt).lower() == "csv":
        suffix = "failed-tasks" if failed_only else "results"
        return results_to_csv(tasks), "text/csv; charset=utf-8", f"taskgrid-session-{safe_name}-{suffix}.csv"
    suffix = "failed-tasks" if failed_only else "results"
    return dumps(data), "application/json; charset=utf-8", f"taskgrid-session-{safe_name}-{suffix}.json"


def _cutoff_iso(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=max(0, int(days)))).isoformat(timespec="milliseconds")


def retention_preview(
    *,
    completed_days: int = 30,
    failed_days: int = 90,
    event_days: int = 30,
    purge_workers_active_seconds: int | None = None,
) -> dict[str, Any]:
    init_db()
    completed_cutoff = _cutoff_iso(completed_days)
    failed_cutoff = _cutoff_iso(failed_days)
    event_cutoff = _cutoff_iso(event_days)
    with connection() as conn:
        completed_sessions = conn.execute(
            """
            SELECT COUNT(*) AS count FROM service_sessions
            WHERE status IN ('succeeded','cancelled') AND COALESCE(finished_at, created_at) < ?
            """,
            (completed_cutoff,),
        ).fetchone()["count"]
        failed_sessions = conn.execute(
            """
            SELECT COUNT(*) AS count FROM service_sessions
            WHERE status='failed' AND COALESCE(finished_at, created_at) < ?
            """,
            (failed_cutoff,),
        ).fetchone()["count"]
        orphan_jobs = conn.execute(
            """
            SELECT COUNT(*) AS count FROM jobs
            WHERE session_id IS NULL AND status IN ('succeeded','failed','cancelled')
              AND COALESCE(finished_at, created_at) < ?
            """,
            (failed_cutoff,),
        ).fetchone()["count"]
        old_events = conn.execute("SELECT COUNT(*) AS count FROM events WHERE at < ?", (event_cutoff,)).fetchone()["count"]
        stale_workers = []
        if purge_workers_active_seconds is not None:
            rows = conn.execute("SELECT * FROM workers ORDER BY last_heartbeat_at DESC").fetchall()
            stale_workers = [
                _worker_capability_row(row, active_seconds=purge_workers_active_seconds)
                for row in rows
                if not _worker_is_active(row, active_seconds=purge_workers_active_seconds)
            ]
    return {
        "checked_at": now(),
        "completed_days": int(completed_days),
        "failed_days": int(failed_days),
        "event_days": int(event_days),
        "completed_cutoff": completed_cutoff,
        "failed_cutoff": failed_cutoff,
        "event_cutoff": event_cutoff,
        "completed_or_cancelled_sessions": int(completed_sessions or 0),
        "failed_sessions": int(failed_sessions or 0),
        "orphan_terminal_jobs": int(orphan_jobs or 0),
        "old_events": int(old_events or 0),
        "stale_workers": len(stale_workers),
        "stale_worker_ids": [worker.get("id") for worker in stale_workers[:100]],
    }


def apply_retention_cleanup(
    *,
    completed_days: int = 30,
    failed_days: int = 90,
    event_days: int = 30,
    purge_workers_active_seconds: int | None = None,
    manager_log_keep_bytes: int | None = None,
    updated_by: str = "api",
) -> dict[str, Any]:
    init_db()
    preview = retention_preview(
        completed_days=completed_days,
        failed_days=failed_days,
        event_days=event_days,
        purge_workers_active_seconds=purge_workers_active_seconds,
    )
    completed_cutoff = preview["completed_cutoff"]
    failed_cutoff = preview["failed_cutoff"]
    event_cutoff = preview["event_cutoff"]
    deleted_session_ids: list[str] = []
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        session_rows = conn.execute(
            """
            SELECT id FROM service_sessions
            WHERE (status IN ('succeeded','cancelled') AND COALESCE(finished_at, created_at) < ?)
               OR (status='failed' AND COALESCE(finished_at, created_at) < ?)
            ORDER BY COALESCE(finished_at, created_at) ASC
            """,
            (completed_cutoff, failed_cutoff),
        ).fetchall()
        deleted_session_ids = [row["id"] for row in session_rows]
        deleted_jobs = 0
        deleted_tasks = 0
        for session_id in deleted_session_ids:
            task_count = conn.execute(
                "SELECT COUNT(*) AS count FROM tasks WHERE job_id IN (SELECT id FROM jobs WHERE session_id=?)",
                (session_id,),
            ).fetchone()["count"]
            job_count = conn.execute("SELECT COUNT(*) AS count FROM jobs WHERE session_id=?", (session_id,)).fetchone()["count"]
            deleted_tasks += int(task_count or 0)
            deleted_jobs += int(job_count or 0)
            conn.execute("DELETE FROM jobs WHERE session_id=?", (session_id,))
            conn.execute("DELETE FROM service_sessions WHERE id=?", (session_id,))
        orphan_task_count = conn.execute(
            """
            SELECT COUNT(*) AS count FROM tasks
            WHERE job_id IN (
                SELECT id FROM jobs WHERE session_id IS NULL AND status IN ('succeeded','failed','cancelled')
                  AND COALESCE(finished_at, created_at) < ?
            )
            """,
            (failed_cutoff,),
        ).fetchone()["count"]
        orphan_job_count = conn.execute(
            """
            SELECT COUNT(*) AS count FROM jobs
            WHERE session_id IS NULL AND status IN ('succeeded','failed','cancelled')
              AND COALESCE(finished_at, created_at) < ?
            """,
            (failed_cutoff,),
        ).fetchone()["count"]
        conn.execute(
            """
            DELETE FROM jobs
            WHERE session_id IS NULL AND status IN ('succeeded','failed','cancelled')
              AND COALESCE(finished_at, created_at) < ?
            """,
            (failed_cutoff,),
        )
        old_event_count = conn.execute("SELECT COUNT(*) AS count FROM events WHERE at < ?", (event_cutoff,)).fetchone()["count"]
        conn.execute("DELETE FROM events WHERE at < ?", (event_cutoff,))
        summary = {
            **preview,
            "applied_at": now(),
            "updated_by": updated_by,
            "deleted_sessions": len(deleted_session_ids),
            "deleted_session_ids": deleted_session_ids[:100],
            "deleted_jobs": deleted_jobs + int(orphan_job_count or 0),
            "deleted_tasks": deleted_tasks + int(orphan_task_count or 0),
            "deleted_events": int(old_event_count or 0),
        }
        log_event(conn, "warning", "manager", "retention", "retention cleanup applied", summary, code="RetentionCleanupApplied")
        conn.execute("COMMIT")
    if purge_workers_active_seconds is not None:
        summary["offline_worker_purge"] = purge_offline_workers(active_seconds=purge_workers_active_seconds, updated_by="retention", include_running=False)
    if manager_log_keep_bytes is not None:
        summary["manager_log_truncate"] = truncate_manager_log(manager_log_keep_bytes)
    return summary

def dashboard_stats() -> dict[str, int]:
    init_db()
    stats = {
        "jobs_total": 0,
        "jobs_queued": 0,
        "jobs_running": 0,
        "jobs_succeeded": 0,
        "jobs_failed": 0,
        "jobs_cancelled": 0,
        "tasks_total": 0,
        "tasks_queued": 0,
        "tasks_running": 0,
        "tasks_succeeded": 0,
        "tasks_failed": 0,
        "tasks_cancelled": 0,
        "workers_total": 0,
        "sessions_total": 0,
        "sessions_queued": 0,
        "sessions_running": 0,
        "sessions_succeeded": 0,
        "sessions_failed": 0,
        "sessions_cancelled": 0,
        "worker_slots_desired": 0,
        "worker_slots_active": 0,
        "workers_active": 0,
        "workers_stale": 0,
        "tasks_expired_leases": 0,
    }
    with connection() as conn:
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM service_sessions GROUP BY status").fetchall():
            stats["sessions_total"] += int(row["count"] or 0)
            key = f"sessions_{row['status']}"
            if key in stats:
                stats[key] = int(row["count"] or 0)
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status").fetchall():
            stats["jobs_total"] += int(row["count"] or 0)
            key = f"jobs_{row['status']}"
            if key in stats:
                stats[key] = int(row["count"] or 0)
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM tasks GROUP BY status").fetchall():
            stats["tasks_total"] += int(row["count"] or 0)
            key = f"tasks_{row['status']}"
            if key in stats:
                stats[key] = int(row["count"] or 0)
        worker_rows = conn.execute("SELECT * FROM workers").fetchall()
        stats["workers_total"] = len(worker_rows)
        config_row = conn.execute("SELECT SUM(desired_concurrency) AS slots FROM worker_configs").fetchone()
        stats["worker_slots_desired"] = int(config_row["slots"] or 0) if config_row else 0
        for worker in worker_rows:
            if _worker_is_active(worker):
                stats["workers_active"] += 1
            else:
                stats["workers_stale"] += 1
            metadata = loads(worker["metadata_json"], {})
            stats["worker_slots_active"] += _coerce_concurrency(metadata.get("active_concurrency") or metadata.get("configured_concurrency"), default=0)
        expired = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM tasks
            WHERE status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
            """,
            (now(),),
        ).fetchone()
        stats["tasks_expired_leases"] = int(expired["count"] or 0) if expired else 0
    return stats


def list_tasks(job_id: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
    init_db()
    with connection() as conn:
        if job_id:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE job_id=? ORDER BY created_at ASC LIMIT ?",
                (job_id, limit),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [row_to_dict(row) for row in rows]


def list_workers(limit: int = 100) -> list[dict[str, Any]]:
    init_db()
    with connection() as conn:
        rows = conn.execute(
            """
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
            FROM workers w
            LEFT JOIN worker_configs wc ON wc.worker_id = w.id
            ORDER BY w.last_heartbeat_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        out = []
        for row in rows:
            data = row_to_dict(row)
            metadata = data.get("metadata", {}) or {}
            if data.get("desired_concurrency") is None:
                data["desired_concurrency"] = _metadata_concurrency(metadata, default=1)
            data["active_concurrency"] = _metadata_concurrency(metadata, default=int(data.get("desired_concurrency") or 1))
            data["running_tasks"] = int(metadata.get("running_tasks") or 0)
            data["pending_concurrency"] = metadata.get("pending_concurrency")
            data["tags"] = sorted(_worker_tags(metadata))
            data["task_types"] = sorted(_worker_task_types(metadata))
            data["service_name"] = metadata.get("service_name") or metadata.get("service") or ""
            data["service_version"] = metadata.get("service_version") or metadata.get("image_version") or ""
            data["disabled"] = bool(int(data.get("disabled") or 0))
            data["disabled_reason"] = data.get("disabled_reason") or ""
            if "instance_count" in metadata:
                try:
                    data["active_concurrency"] = max(0, min(MAX_WORKER_CONCURRENCY, int(metadata.get("instance_count") or 0)))
                except (TypeError, ValueError):
                    pass
            data["active"] = _worker_is_active(row)
            out.append(data)
        return out


class WorkerLogError(RuntimeError):
    """Raised when a worker log server cannot be reached or returns invalid data."""


def get_worker(worker_id: str) -> dict[str, Any] | None:
    init_db()
    with connection() as conn:
        row = conn.execute(
            """
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
            FROM workers w
            LEFT JOIN worker_configs wc ON wc.worker_id = w.id
            WHERE w.id=?
            """,
            (worker_id,),
        ).fetchone()
        if not row:
            return None
        data = row_to_dict(row)
        metadata = data.get("metadata", {}) or {}
        if data.get("desired_concurrency") is None:
            data["desired_concurrency"] = _metadata_concurrency(metadata, default=1)
        data["active_concurrency"] = _metadata_concurrency(metadata, default=int(data.get("desired_concurrency") or 1))
        data["running_tasks"] = int(metadata.get("running_tasks") or 0)
        data["pending_concurrency"] = metadata.get("pending_concurrency")
        data["tags"] = sorted(_worker_tags(metadata))
        data["task_types"] = sorted(_worker_task_types(metadata))
        data["service_name"] = metadata.get("service_name") or metadata.get("service") or ""
        data["service_version"] = metadata.get("service_version") or metadata.get("image_version") or ""
        data["disabled"] = bool(int(data.get("disabled") or 0))
        data["disabled_reason"] = data.get("disabled_reason") or ""
        if "instance_count" in metadata:
            try:
                data["active_concurrency"] = max(0, min(MAX_WORKER_CONCURRENCY, int(metadata.get("instance_count") or 0)))
            except (TypeError, ValueError):
                pass
        data["active"] = _worker_is_active(row)
        return data


def _worker_log_base_url(worker_id: str) -> str:
    worker = get_worker(worker_id)
    if not worker:
        raise WorkerLogError("worker not found")
    metadata = worker.get("metadata", {}) or {}
    log_url = str(metadata.get("log_url") or "").strip().rstrip("/")
    if not log_url:
        raise WorkerLogError("worker has not advertised a log_url")
    return log_url


def _fetch_worker_log_json(worker_id: str, path: str) -> dict[str, Any]:
    url = f"{_worker_log_base_url(worker_id)}{path}"
    try:
        with request.urlopen(url, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise WorkerLogError(f"could not read worker logs: {exc}") from exc


def list_worker_logs(worker_id: str) -> dict[str, Any]:
    return _fetch_worker_log_json(worker_id, "/logs")


def read_worker_log(worker_id: str, filename: str, tail_bytes: int = MAX_WORKER_LOG_TAIL_BYTES) -> str:
    safe_filename = _safe_log_relative_path(filename)
    if not safe_filename:
        raise WorkerLogError("invalid log filename")
    tail = max(1, min(MAX_WORKER_LOG_TAIL_BYTES, int(tail_bytes)))
    url = f"{_worker_log_base_url(worker_id)}/logs/{parse.quote(safe_filename, safe='/')}?tail={tail}"
    try:
        with request.urlopen(url, timeout=5) as response:
            return response.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise WorkerLogError(f"could not read worker log: {exc}") from exc


def list_events(entity_type: str | None = None, entity_id: str | None = None, code: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    init_db()
    where: list[str] = []
    params: list[Any] = []
    if entity_type:
        where.append("entity_type=?")
        params.append(entity_type)
    if entity_id:
        where.append("entity_id=?")
        params.append(entity_id)
    if code:
        where.append("code=?")
        params.append(code)
    query = "SELECT * FROM events"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY at DESC, id DESC LIMIT ?"
    params.append(limit)
    with connection() as conn:
        rows = conn.execute(query, params).fetchall()
        return [row_to_dict(row) for row in rows]
