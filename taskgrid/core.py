from __future__ import annotations

import csv
import io
import json
import os
import socket
import time
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
OPERATOR_CACHE_TTL_MS = max(0, int(os.environ.get("TASKGRID_OPERATOR_CACHE_TTL_MS", "500")))
LEASE_RECOVERY_INTERVAL_MS = max(0, int(os.environ.get("TASKGRID_LEASE_RECOVERY_INTERVAL_MS", "500")))
_OPERATOR_CACHE: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_LAST_LEASE_RECOVERY_AT = 0.0



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


def _cache_key_part(value: Any) -> Any:
    if isinstance(value, (list, tuple, set)):
        return tuple(sorted(_cache_key_part(item) for item in value))
    if isinstance(value, dict):
        return tuple(sorted((str(key), _cache_key_part(val)) for key, val in value.items()))
    return value


def _operator_cache_get(key: tuple[Any, ...], *, refresh: bool = False) -> dict[str, Any] | None:
    if refresh or OPERATOR_CACHE_TTL_MS <= 0:
        return None
    cached = _OPERATOR_CACHE.get(key)
    if not cached:
        return None
    created, value = cached
    age_ms = (time.monotonic() - created) * 1000.0
    if age_ms > OPERATOR_CACHE_TTL_MS:
        _OPERATOR_CACHE.pop(key, None)
        return None
    out = dict(value)
    out["cached"] = True
    out["cache_age_ms"] = int(age_ms)
    out["cache_ttl_ms"] = OPERATOR_CACHE_TTL_MS
    return out


def _operator_cache_set(key: tuple[Any, ...], value: dict[str, Any]) -> dict[str, Any]:
    if OPERATOR_CACHE_TTL_MS > 0:
        stored = dict(value)
        stored["cached"] = False
        stored["cache_age_ms"] = 0
        stored["cache_ttl_ms"] = OPERATOR_CACHE_TTL_MS
        _OPERATOR_CACHE[key] = (time.monotonic(), stored)
        return dict(stored)
    out = dict(value)
    out["cached"] = False
    out["cache_age_ms"] = 0
    out["cache_ttl_ms"] = 0
    return out


def clear_operator_cache() -> None:
    _OPERATOR_CACHE.clear()


def _maybe_expire_stale_tasks(conn, *, force: bool = False) -> dict[str, Any]:
    global _LAST_LEASE_RECOVERY_AT
    if not force and LEASE_RECOVERY_INTERVAL_MS > 0:
        current = time.monotonic()
        if (current - _LAST_LEASE_RECOVERY_AT) * 1000.0 < LEASE_RECOVERY_INTERVAL_MS:
            return {"checked_at": now(), "expired": 0, "requeued": 0, "failed": 0, "tasks": [], "skipped": True, "reason": "lease-recovery-throttled"}
        _LAST_LEASE_RECOVERY_AT = current
    summary = expire_stale_tasks(conn)
    if force or LEASE_RECOVERY_INTERVAL_MS > 0:
        _LAST_LEASE_RECOVERY_AT = time.monotonic()
    return summary


def _leased_task_response(row: Any, *, assigned_id: str, timestamp: str, lease_until: str) -> dict[str, Any]:
    return {
        "id": row["id"],
        "job_id": row["job_id"],
        "task_type": row["task_type"],
        "payload": loads(row["payload_json"], {}),
        "input_index": row["input_index"] if "input_index" in row.keys() else None,
        "input_key": row["input_key"] if "input_key" in row.keys() else None,
        "status": "running",
        "priority": row["priority"],
        "attempts": int(row["attempts"] or 0) + 1,
        "max_retries": row["max_retries"],
        "assigned_worker_id": assigned_id,
        "leased_at": timestamp,
        "lease_expires_at": lease_until,
        "started_at": row["started_at"] or timestamp,
        "finished_at": row["finished_at"],
        "result": loads(row["result_json"], None),
        "error": row["error"],
        "created_at": row["created_at"],
        "updated_at": timestamp,
    }


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


def _coerce_input_key(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:500] if text else None


def _input_key_for_payload(payload: dict[str, Any], index: int, input_keys: list[Any] | None = None) -> str | None:
    if input_keys is not None and index < len(input_keys):
        explicit = _coerce_input_key(input_keys[index])
        if explicit is not None:
            return explicit
    if isinstance(payload, dict):
        for key in ("id", "input_key", "key"):
            if key in payload:
                derived = _coerce_input_key(payload.get(key))
                if derived is not None:
                    return derived
    return None



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


def _normalize_idempotency_key(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text[:200]


def _job_response_with_resume(conn, job_id: str) -> dict[str, Any] | None:
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not job:
        return None
    out = row_to_dict(job)
    session_id = out.get("session_id")
    if session_id:
        session = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        if session:
            session_data = row_to_dict(session)
            out["client_id"] = session_data.get("client_id")
            out["resume_token"] = session_data.get("resume_token")
    return out


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
        total = conn.execute("SELECT COUNT(*) AS count FROM tasks WHERE job_id=?", (job_id,)).fetchone()["count"]
        conn.execute(
            "UPDATE jobs SET total_tasks=?, completed_tasks=?, failed_tasks=?, cancelled_tasks=? WHERE id=?",
            (int(total or 0), counts["completed"] or 0, counts["failed"] or 0, counts["cancelled"] or 0, job_id),
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
    patch: list[Any] = [int(total or 0), completed, failed, cancelled]
    set_parts = ["total_tasks=?", "completed_tasks=?", "failed_tasks=?", "cancelled_tasks=?"]

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
    if next_status not in TERMINAL_JOB_STATUSES and job["finished_at"] is not None:
        set_parts.append("finished_at=NULL")

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
    input_keys: list[Any] | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    init_db()
    clear_operator_cache()
    if input_keys is not None and len(input_keys) != len(payloads):
        raise ValueError("input_keys length must match payloads length")
    job_id = new_id("job")
    created = now()
    session_created = False
    normalized_client_id = _normalize_client_id(client_id) or new_id("client")
    normalized_idempotency_key = _normalize_idempotency_key(idempotency_key)
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        if normalized_idempotency_key:
            existing_job = conn.execute(
                """
                SELECT id FROM jobs
                WHERE client_id=? AND idempotency_key=?
                LIMIT 1
                """,
                (normalized_client_id, normalized_idempotency_key),
            ).fetchone()
            if existing_job:
                out = _job_response_with_resume(conn, existing_job["id"])
                log_event(
                    conn,
                    "info",
                    "job",
                    existing_job["id"],
                    "idempotent job submission replayed",
                    {"client_id": normalized_client_id, "idempotency_key": normalized_idempotency_key},
                    code="JobSubmissionReplayed",
                )
                conn.execute("COMMIT")
                return out or {"id": existing_job["id"]}
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
            INSERT INTO jobs(id, session_id, name, task_type, status, priority, total_tasks, client_id, idempotency_key, created_at, metadata_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (job_id, session_id, name, task_type, "queued", effective_job_priority, len(payloads), normalized_client_id, normalized_idempotency_key, created, dumps(metadata or {})),
        )
        task_rows = [
            (
                new_id("task"),
                job_id,
                task_type,
                dumps(payload),
                index,
                _input_key_for_payload(payload, index, input_keys),
                "queued",
                effective_job_priority,
                max_retries,
                created,
                created,
            )
            for index, payload in enumerate(payloads)
        ]
        conn.executemany(
            """
            INSERT INTO tasks(
                id, job_id, task_type, payload_json, input_index, input_key, status, priority,
                max_retries, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
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
            "idempotency_key": normalized_idempotency_key,
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
    clear_operator_cache()
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
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.draining, wc.drain_at, wc.drain_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
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
    clear_operator_cache()
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
    clear_operator_cache()
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




def _clean_reason(reason: str | None) -> str | None:
    return str(reason or "").strip()[:500] or None


def _clean_instance_id(instance_id: str) -> str:
    item = str(instance_id or "").strip()
    if not item:
        raise ValueError("instance_id is required")
    return item[:200]


def _config_disabled(config: Any) -> bool:
    return bool(config and int(config["disabled"] or 0))


def _config_draining(config: Any) -> bool:
    return bool(config and int(config["draining"] or 0))


def _config_blocks_leases(config: Any) -> bool:
    return _config_disabled(config) or _config_draining(config)


def _drained_instance_ids(conn: Any, worker_id: str) -> set[str]:
    rows = conn.execute(
        "SELECT instance_id FROM worker_instance_configs WHERE worker_id=? AND draining=1",
        (worker_id,),
    ).fetchall()
    return {str(row["instance_id"]) for row in rows}


def _instance_drained(conn: Any, worker_id: str, instance_id: str | None) -> bool:
    if not instance_id:
        return False
    row = conn.execute(
        "SELECT draining FROM worker_instance_configs WHERE worker_id=? AND instance_id=?",
        (worker_id, instance_id),
    ).fetchone()
    return bool(row and int(row["draining"] or 0))


def list_worker_instance_configs(worker_id: str) -> list[dict[str, Any]]:
    init_db()
    with connection() as conn:
        rows = conn.execute(
            "SELECT * FROM worker_instance_configs WHERE worker_id=? ORDER BY instance_id ASC",
            (worker_id,),
        ).fetchall()
        return [row_to_dict(row) for row in rows]


def set_worker_draining(worker_id: str, draining: bool, reason: str | None = None, updated_by: str = "api") -> dict[str, Any]:
    """Set worker drain mode.

    Draining blocks new task leases to the node but does not ask the worker
    supervisor to scale down. Running tasks may finish and idle instances stay
    available to resume later. This is different from disable, which also tells
    heartbeating workers to scale their local instance loops down to zero.
    """
    init_db()
    clear_operator_cache()
    timestamp = now()
    clean_reason = _clean_reason(reason)
    drain_flag = 1 if draining else 0
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute("SELECT * FROM worker_configs WHERE worker_id=?", (worker_id,)).fetchone()
        desired = int(existing["desired_concurrency"] or 1) if existing else 1
        disabled = int(existing["disabled"] or 0) if existing else 0
        conn.execute(
            """
            INSERT INTO worker_configs(worker_id, desired_concurrency, disabled, draining, drain_at, drain_reason, updated_at, updated_by)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(worker_id) DO UPDATE SET
                draining=excluded.draining,
                drain_at=excluded.drain_at,
                drain_reason=excluded.drain_reason,
                updated_at=excluded.updated_at,
                updated_by=excluded.updated_by
            """,
            (worker_id, desired, disabled, drain_flag, timestamp if draining else None, clean_reason if draining else None, timestamp, updated_by),
        )
        code = "WorkerDrainStarted" if draining else "WorkerDrainCleared"
        message = "worker drain started" if draining else "worker drain cleared"
        log_event(conn, "warning", "worker", worker_id, message, {"draining": draining, "reason": clean_reason, "updated_by": updated_by}, code=code)
        out = conn.execute("SELECT * FROM worker_configs WHERE worker_id=?", (worker_id,)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out)


def set_worker_instance_draining(worker_id: str, instance_id: str, draining: bool, reason: str | None = None, updated_by: str = "api") -> dict[str, Any]:
    """Set drain mode for one logical worker instance."""
    init_db()
    clear_operator_cache()
    timestamp = now()
    instance_id = _clean_instance_id(instance_id)
    clean_reason = _clean_reason(reason)
    flag = 1 if draining else 0
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO worker_configs(worker_id, desired_concurrency, updated_at, updated_by)
            VALUES(?,?,?,?)
            ON CONFLICT(worker_id) DO NOTHING
            """,
            (worker_id, 1, timestamp, updated_by),
        )
        conn.execute(
            """
            INSERT INTO worker_instance_configs(worker_id, instance_id, draining, drain_at, drain_reason, updated_at, updated_by)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(worker_id, instance_id) DO UPDATE SET
                draining=excluded.draining,
                drain_at=excluded.drain_at,
                drain_reason=excluded.drain_reason,
                updated_at=excluded.updated_at,
                updated_by=excluded.updated_by
            """,
            (worker_id, instance_id, flag, timestamp if draining else None, clean_reason if draining else None, timestamp, updated_by),
        )
        code = "WorkerInstanceDrainStarted" if draining else "WorkerInstanceDrainCleared"
        message = "worker instance drain started" if draining else "worker instance drain cleared"
        log_event(conn, "warning", "worker-instance", f"{worker_id}:{instance_id}", message, {"worker_id": worker_id, "instance_id": instance_id, "draining": draining, "reason": clean_reason, "updated_by": updated_by}, code=code)
        out = conn.execute("SELECT * FROM worker_instance_configs WHERE worker_id=? AND instance_id=?", (worker_id, instance_id)).fetchone()
        conn.execute("COMMIT")
        return row_to_dict(out)

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
    data["draining"] = bool(int(data.get("draining") or 0))
    data["drain_reason"] = data.get("drain_reason") or ""
    if data.get("desired_concurrency") is None:
        data["desired_concurrency"] = _metadata_concurrency(metadata, default=1)
    else:
        data["desired_concurrency"] = _coerce_concurrency(data.get("desired_concurrency"), default=1)
    data["active_concurrency"] = _metadata_concurrency(metadata, default=int(data.get("desired_concurrency") or 1))
    if "instance_count" in metadata:
        try:
            data["active_concurrency"] = max(0, min(MAX_WORKER_CONCURRENCY, int(metadata.get("instance_count") or 0)))
        except (TypeError, ValueError):
            pass
    data["running_tasks"] = int(metadata.get("running_tasks") or 0)
    data["heartbeat_active"] = _worker_is_active(row, active_seconds=active_seconds)
    data["active"] = (not data["disabled"]) and (not data["draining"]) and bool(data["heartbeat_active"])
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
                    "status": "disabled" if worker.get("disabled") else ("draining" if worker.get("draining") else worker["status"]),
                    "active": worker["active"],
                    "disabled": bool(worker.get("disabled")),
                    "draining": bool(worker.get("draining")),
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


def _queued_task_diagnostic(
    raw: dict[str, Any],
    *,
    workers: list[dict[str, Any]],
    worker_running_counts: dict[str, int],
    worker_drained_counts: dict[str, int],
) -> dict[str, Any]:
    job_metadata = raw.get("job_metadata", {}) or loads(raw.get("job_metadata_json"), {}) or {}
    required_tags = sorted(_required_tags(job_metadata))
    task_type = str(raw.get("task_type") or "")
    blockers: list[str] = []
    reason = "leaseable_now"
    detail = "At least one active capable worker appears to have a free execution slot."

    if raw.get("job_status") in {"cancelled", "cancelling"}:
        reason = "job_not_leaseable"
        detail = f"Job status is {raw.get('job_status')}."
        blockers.append(reason)
    elif int(raw.get("job_paused") or 0):
        reason = "job_paused"
        detail = raw.get("job_pause_reason") or "Job is paused."
        blockers.append(reason)
    elif int(raw.get("session_paused") or 0):
        reason = "session_paused"
        detail = raw.get("session_pause_reason") or "Session is paused."
        blockers.append(reason)
    else:
        supporting: list[dict[str, Any]] = []
        tag_capable: list[dict[str, Any]] = []
        active_capable: list[dict[str, Any]] = []
        free_capable: list[dict[str, Any]] = []
        required = set(required_tags)
        for worker in workers:
            task_types = set(worker.get("task_types") or [])
            # Empty task catalog means legacy/open worker: same behavior as the
            # lease path, which only filters when a worker advertises task types.
            supports_type = (not task_types) or task_type in task_types
            if not supports_type:
                continue
            supporting.append(worker)
            tags = set(worker.get("tags") or [])
            if not required.issubset(tags):
                continue
            tag_capable.append(worker)
            if not worker.get("active"):
                continue
            active_capable.append(worker)
            wid = str(worker.get("id") or "")
            slots = int(worker.get("active_concurrency") or worker.get("desired_concurrency") or 1)
            busy = int(worker_running_counts.get(wid, 0))
            drained = int(worker_drained_counts.get(wid, 0))
            free_slots = max(0, slots - busy - drained)
            if free_slots > 0:
                item = dict(worker)
                item["estimated_free_slots"] = free_slots
                item["estimated_running_slots"] = busy
                item["drained_instances"] = drained
                free_capable.append(item)

        if not workers:
            reason = "no_workers_registered"
            detail = "No worker nodes have heartbeated yet."
        elif not supporting:
            reason = "no_worker_supports_task_type"
            detail = f"No worker advertises task type '{task_type}'."
        elif not tag_capable:
            reason = "missing_required_tags"
            detail = f"Workers support task type '{task_type}', but none match required tags {required_tags}."
        elif not active_capable:
            disabled = sum(1 for w in tag_capable if w.get("disabled"))
            draining = sum(1 for w in tag_capable if w.get("draining"))
            stale = sum(1 for w in tag_capable if not w.get("heartbeat_active"))
            if disabled and disabled == len(tag_capable):
                reason = "all_capable_workers_disabled"
                detail = "All capable workers are disabled."
            elif draining and draining == len(tag_capable):
                reason = "all_capable_workers_draining"
                detail = "All capable workers are draining."
            elif stale and stale == len(tag_capable):
                reason = "all_capable_workers_stale"
                detail = "All capable workers are stale/offline."
            else:
                reason = "no_active_capable_worker"
                detail = "Workers are registered, but no active capable worker can lease this task right now."
        elif not free_capable:
            reason = "all_capable_workers_busy"
            detail = "Active capable workers exist, but their advertised execution slots appear busy or drained."
        if reason != "leaseable_now":
            blockers.append(reason)

    return {
        "task_id": raw.get("id"),
        "job_id": raw.get("job_id"),
        "job_name": raw.get("job_name") or "",
        "job_status": raw.get("job_status") or "",
        "session_id": raw.get("session_id"),
        "session_name": raw.get("session_name") or "",
        "session_status": raw.get("session_status") or "",
        "task_type": task_type,
        "input_index": raw.get("input_index"),
        "input_key": raw.get("input_key"),
        "priority": raw.get("priority"),
        "attempts": int(raw.get("attempts") or 0),
        "max_retries": int(raw.get("max_retries") or 0),
        "created_at": raw.get("created_at"),
        "updated_at": raw.get("updated_at"),
        "required_tags": required_tags,
        "reason": reason,
        "blockers": blockers,
        "detail": detail,
    }


def get_queue_diagnostics(*, limit: int = 1000, active_seconds: int | None = None, refresh: bool = False) -> dict[str, Any]:
    """Explain queued backlog and why pending tasks are or are not leaseable.

    This is an operator diagnostic, not the scheduler itself. It mirrors the
    major lease gates: session/job pause state, task type catalog, required
    tags, worker enable/drain/stale state, and advertised slot pressure.
    """
    init_db()
    limit = max(1, min(10000, int(limit or 1000)))
    active_for = DEFAULT_WORKER_ACTIVE_SECONDS if active_seconds is None else int(active_seconds)
    cache_key = ("queue_diagnostics", str(db_path()), limit, active_for)
    cached = _operator_cache_get(cache_key, refresh=refresh)
    if cached is not None:
        return cached
    timestamp = now()
    with connection() as conn:
        worker_rows = conn.execute(
            """
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason,
                   wc.draining, wc.drain_at, wc.drain_reason, wc.updated_at AS config_updated_at,
                   wc.updated_by AS config_updated_by
            FROM workers w
            LEFT JOIN worker_configs wc ON wc.worker_id = w.id
            ORDER BY w.last_heartbeat_at DESC
            """
        ).fetchall()
        workers = [_worker_capability_row(row, active_seconds=active_for) for row in worker_rows]
        known_worker_ids = {str(worker.get("id")) for worker in workers if worker.get("id")}
        running_rows = conn.execute("SELECT assigned_worker_id FROM tasks WHERE status='running' AND assigned_worker_id IS NOT NULL").fetchall()
        worker_running_counts: dict[str, int] = {}
        for row in running_rows:
            worker_id, _instance_id = _split_executor_assignment(row["assigned_worker_id"], known_worker_ids)
            if worker_id:
                worker_running_counts[worker_id] = worker_running_counts.get(worker_id, 0) + 1
        drained_rows = conn.execute("SELECT worker_id, COUNT(*) AS count FROM worker_instance_configs WHERE draining=1 GROUP BY worker_id").fetchall()
        worker_drained_counts = {str(row["worker_id"]): int(row["count"] or 0) for row in drained_rows}
        queued_rows = conn.execute(
            """
            SELECT t.*, j.name AS job_name, j.status AS job_status, j.paused AS job_paused,
                   j.pause_reason AS job_pause_reason, j.metadata_json AS job_metadata_json,
                   j.session_id AS session_id, s.name AS session_name, s.status AS session_status,
                   s.paused AS session_paused, s.pause_reason AS session_pause_reason
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            LEFT JOIN service_sessions s ON s.id = j.session_id
            WHERE t.status='queued'
            ORDER BY t.priority DESC, t.created_at ASC, COALESCE(t.input_index, 9223372036854775807) ASC, t.id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    diagnostics: list[dict[str, Any]] = []
    reason_counts: dict[str, int] = {}
    task_type_groups: dict[str, dict[str, Any]] = {}
    session_groups: dict[str, dict[str, Any]] = {}
    job_groups: dict[str, dict[str, Any]] = {}

    for row in queued_rows:
        raw = row_to_dict(row)
        # row_to_dict turns job_metadata_json into job_metadata.
        item = _queued_task_diagnostic(
            raw,
            workers=workers,
            worker_running_counts=worker_running_counts,
            worker_drained_counts=worker_drained_counts,
        )
        diagnostics.append(item)
        reason = item["reason"]
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

        type_group = task_type_groups.setdefault(
            item["task_type"],
            {"task_type": item["task_type"], "queued_tasks": 0, "leaseable_now": 0, "blocked_tasks": 0, "reasons": {}},
        )
        type_group["queued_tasks"] += 1
        if reason == "leaseable_now":
            type_group["leaseable_now"] += 1
        else:
            type_group["blocked_tasks"] += 1
        type_group["reasons"][reason] = type_group["reasons"].get(reason, 0) + 1

        session_id = item.get("session_id") or "orphan"
        session_group = session_groups.setdefault(
            session_id,
            {"session_id": session_id, "name": item.get("session_name") or "", "status": item.get("session_status") or "", "queued_tasks": 0, "leaseable_now": 0, "blocked_tasks": 0, "reasons": {}},
        )
        session_group["queued_tasks"] += 1
        if reason == "leaseable_now":
            session_group["leaseable_now"] += 1
        else:
            session_group["blocked_tasks"] += 1
        session_group["reasons"][reason] = session_group["reasons"].get(reason, 0) + 1

        job_id = item.get("job_id") or "unknown"
        job_group = job_groups.setdefault(
            job_id,
            {"job_id": job_id, "name": item.get("job_name") or "", "status": item.get("job_status") or "", "session_id": session_id, "task_type": item.get("task_type"), "queued_tasks": 0, "leaseable_now": 0, "blocked_tasks": 0, "reasons": {}},
        )
        job_group["queued_tasks"] += 1
        if reason == "leaseable_now":
            job_group["leaseable_now"] += 1
        else:
            job_group["blocked_tasks"] += 1
        job_group["reasons"][reason] = job_group["reasons"].get(reason, 0) + 1

    worker_summaries: list[dict[str, Any]] = []
    for worker in workers:
        wid = str(worker.get("id") or "")
        slots = int(worker.get("active_concurrency") or worker.get("desired_concurrency") or 1)
        busy = int(worker_running_counts.get(wid, 0))
        drained = int(worker_drained_counts.get(wid, 0))
        worker_summaries.append({
            "worker_id": wid,
            "hostname": worker.get("hostname") or "",
            "active": bool(worker.get("active")),
            "heartbeat_active": bool(worker.get("heartbeat_active")),
            "disabled": bool(worker.get("disabled")),
            "draining": bool(worker.get("draining")),
            "service_name": worker.get("service_name") or "",
            "service_version": worker.get("service_version") or "",
            "task_types": sorted(worker.get("task_types") or []),
            "tags": sorted(worker.get("tags") or []),
            "active_slots": slots,
            "running_slots": busy,
            "drained_slots": drained,
            "estimated_free_slots": max(0, slots - busy - drained) if worker.get("active") else 0,
            "last_heartbeat_at": worker.get("last_heartbeat_at"),
        })

    return _operator_cache_set(cache_key, {
        "checked_at": timestamp,
        "active_seconds": active_for,
        "queued_tasks": len(diagnostics),
        "leaseable_now": reason_counts.get("leaseable_now", 0),
        "blocked_tasks": len(diagnostics) - reason_counts.get("leaseable_now", 0),
        "reason_counts": dict(sorted(reason_counts.items())),
        "workers_total": len(workers),
        "workers_active": sum(1 for worker in workers if worker.get("active")),
        "estimated_free_slots": sum(int(worker.get("estimated_free_slots") or 0) for worker in worker_summaries),
        "task_types": sorted(task_type_groups.values(), key=lambda item: str(item.get("task_type") or "")),
        "sessions": sorted(session_groups.values(), key=lambda item: (-int(item.get("queued_tasks") or 0), str(item.get("session_id") or ""))),
        "jobs": sorted(job_groups.values(), key=lambda item: (-int(item.get("queued_tasks") or 0), str(item.get("job_id") or ""))),
        "tasks": diagnostics,
        "workers": worker_summaries,
    })


def get_task_catalog(task_type: str | None = None, required_tags: list[str] | set[str] | tuple[str, ...] | None = None, active_seconds: int | None = None) -> dict[str, Any]:
    init_db()
    with connection() as conn:
        rows = conn.execute("""
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.draining, wc.drain_at, wc.drain_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
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




def get_services(service_name: str | None = None, service_version: str | None = None, active_seconds: int | None = None, *, refresh: bool = False) -> dict[str, Any]:
    """Return service/application versions currently advertised by worker nodes.

    This is a derived runtime registry. Workers advertise service metadata in
    their heartbeat, usually from Docker image/build metadata. The manager keeps
    the raw worker heartbeat as source of truth and groups rows here by
    service name + service version so operators can see mixed deployments,
    stale versions, and which task types each service can execute.
    """
    init_db()
    active_for = DEFAULT_WORKER_ACTIVE_SECONDS if active_seconds is None else int(active_seconds)
    name_filter = str(service_name or "").strip()
    version_filter = str(service_version or "").strip()
    cache_key = ("services", str(db_path()), name_filter, version_filter, active_for)
    cached = _operator_cache_get(cache_key, refresh=refresh)
    if cached is not None:
        return cached
    with connection() as conn:
        rows = conn.execute("""
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.draining, wc.drain_at, wc.drain_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
            FROM workers w
            LEFT JOIN worker_configs wc ON wc.worker_id = w.id
            ORDER BY w.last_heartbeat_at DESC
        """).fetchall()

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        worker = _worker_capability_row(row, active_seconds=active_for)
        metadata = worker.get("metadata", {}) or {}
        svc_name = str(worker.get("service_name") or metadata.get("image_name") or metadata.get("module") or "unknown-service").strip() or "unknown-service"
        svc_version = str(worker.get("service_version") or metadata.get("image_tag") or worker.get("version") or "unknown").strip() or "unknown"
        if name_filter and svc_name != name_filter:
            continue
        if version_filter and svc_version != version_filter:
            continue

        service = grouped.setdefault(
            (svc_name, svc_version),
            {
                "service_name": svc_name,
                "service_version": svc_version,
                "workers_total": 0,
                "workers_active": 0,
                "workers_stale": 0,
                "workers_disabled": 0,
                "workers_draining": 0,
                "desired_instances": 0,
                "active_instances": 0,
                "running_tasks": 0,
                "task_types": set(),
                "tags": set(),
                "taskgrid_versions": set(),
                "workers": [],
            },
        )
        service["workers_total"] += 1
        if worker.get("active"):
            service["workers_active"] += 1
        if not worker.get("heartbeat_active"):
            service["workers_stale"] += 1
        if worker.get("disabled"):
            service["workers_disabled"] += 1
        if worker.get("draining"):
            service["workers_draining"] += 1
        service["desired_instances"] += int(worker.get("desired_concurrency") or 0)
        service["active_instances"] += int(worker.get("active_concurrency") or 0) if worker.get("heartbeat_active") else 0
        service["running_tasks"] += int(worker.get("running_tasks") or 0)
        service["task_types"].update(worker.get("task_types") or [])
        service["tags"].update(worker.get("tags") or [])
        if worker.get("version"):
            service["taskgrid_versions"].add(str(worker.get("version")))
        service["workers"].append(
            {
                "id": worker.get("id"),
                "hostname": worker.get("hostname"),
                "status": "disabled" if worker.get("disabled") else ("draining" if worker.get("draining") else (worker.get("status") if worker.get("heartbeat_active") else "stale")),
                "active": bool(worker.get("active")),
                "heartbeat_active": bool(worker.get("heartbeat_active")),
                "disabled": bool(worker.get("disabled")),
                "draining": bool(worker.get("draining")),
                "desired_instances": int(worker.get("desired_concurrency") or 0),
                "active_instances": int(worker.get("active_concurrency") or 0),
                "running_tasks": int(worker.get("running_tasks") or 0),
                "task_types": worker.get("task_types") or [],
                "tags": worker.get("tags") or [],
                "taskgrid_version": worker.get("version"),
                "last_heartbeat_at": worker.get("last_heartbeat_at"),
            }
        )

    services: list[dict[str, Any]] = []
    for item in grouped.values():
        normalized = dict(item)
        normalized["task_types"] = sorted(item["task_types"])
        normalized["tags"] = sorted(item["tags"])
        normalized["taskgrid_versions"] = sorted(item["taskgrid_versions"])
        normalized["workers"] = sorted(item["workers"], key=lambda w: str(w.get("id") or ""))
        services.append(normalized)
    services.sort(key=lambda item: (str(item["service_name"]), str(item["service_version"])))
    return _operator_cache_set(cache_key, {
        "active_seconds": active_for,
        "service_name": name_filter or None,
        "service_version": version_filter or None,
        "services_total": len(services),
        "services": services,
    })


def _capability_for_submit(conn, task_type: str, metadata: dict[str, Any] | None) -> dict[str, Any]:
    required_tags = _required_tags(metadata or {})
    rows = conn.execute("""
        SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.draining, wc.drain_at, wc.drain_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
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
    active_for = DEFAULT_WORKER_ACTIVE_SECONDS if active_seconds is None else int(active_seconds)
    timestamp = now()
    with connection() as conn:
        worker_rows = conn.execute("SELECT * FROM workers ORDER BY last_heartbeat_at DESC").fetchall()
        workers = [_worker_capability_row(row, active_seconds=active_for) for row in worker_rows]
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



_RECONCILE_JOB_FIELDS = (
    "status",
    "total_tasks",
    "completed_tasks",
    "failed_tasks",
    "cancelled_tasks",
    "started_at",
    "finished_at",
)

_RECONCILE_SESSION_FIELDS = (
    "status",
    "total_jobs",
    "total_tasks",
    "queued_tasks",
    "running_tasks",
    "completed_tasks",
    "failed_tasks",
    "cancelled_tasks",
    "started_at",
    "finished_at",
)


def _snapshot_rows(conn: Any, table: str, fields: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    columns = ", ".join(("id", *fields))
    return {
        row["id"]: {field: row[field] for field in fields}
        for row in conn.execute(f"SELECT {columns} FROM {table}").fetchall()
    }


def _changed_rows(before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]], *, limit: int = 200) -> tuple[int, list[dict[str, Any]]]:
    changed: list[dict[str, Any]] = []
    count = 0
    for row_id in sorted(set(before) | set(after)):
        old = before.get(row_id)
        new = after.get(row_id)
        if old == new:
            continue
        count += 1
        if len(changed) >= limit:
            continue
        changed.append({"id": row_id, "before": old, "after": new})
    return count, changed


def reconcile_manager_state(recover_expired: bool = True, updated_by: str = "api") -> dict[str, Any]:
    """Repair manager-derived state after crashes, restarts, or manual DB edits.

    Task rows are the durable source of truth. This pass optionally recovers
    expired running leases, then recalculates job and service-session counters
    and terminal statuses from the task table. Valid running leases are left
    untouched, so the call is safe on manager startup.
    """
    init_db()
    timestamp = now()
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        jobs_before = _snapshot_rows(conn, "jobs", _RECONCILE_JOB_FIELDS)
        sessions_before = _snapshot_rows(conn, "service_sessions", _RECONCILE_SESSION_FIELDS)
        lease_recovery = expire_stale_tasks(conn) if recover_expired else {
            "checked_at": timestamp,
            "expired": 0,
            "requeued": 0,
            "failed": 0,
            "tasks": [],
            "skipped": True,
        }
        job_ids = [row["id"] for row in conn.execute("SELECT id FROM jobs ORDER BY created_at ASC, id ASC").fetchall()]
        for job_id in job_ids:
            recalc_job(conn, job_id)
        session_ids = [row["id"] for row in conn.execute("SELECT id FROM service_sessions ORDER BY created_at ASC, id ASC").fetchall()]
        for session_id in session_ids:
            recalc_session(conn, session_id)
        jobs_after = _snapshot_rows(conn, "jobs", _RECONCILE_JOB_FIELDS)
        sessions_after = _snapshot_rows(conn, "service_sessions", _RECONCILE_SESSION_FIELDS)
        job_change_count, changed_jobs = _changed_rows(jobs_before, jobs_after)
        session_change_count, changed_sessions = _changed_rows(sessions_before, sessions_after)
        summary = {
            "checked_at": timestamp,
            "recover_expired": bool(recover_expired),
            "updated_by": updated_by,
            "jobs_checked": len(job_ids),
            "sessions_checked": len(session_ids),
            "jobs_reconciled": job_change_count,
            "sessions_reconciled": session_change_count,
            "changed_jobs": changed_jobs,
            "changed_sessions": changed_sessions,
            "lease_recovery": lease_recovery,
        }
        log_event(
            conn,
            "info",
            "manager",
            "maintenance",
            f"manager reconcile run; jobs {job_change_count} sessions {session_change_count} expired {lease_recovery.get('expired', 0)}",
            {
                **summary,
                "changed_jobs": changed_jobs[:25],
                "changed_sessions": changed_sessions[:25],
            },
            code="MaintenanceReconcileRun",
        )
        conn.execute("COMMIT")
    summary["status"] = recovery_status()
    return summary


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
        _maybe_expire_stale_tasks(conn)
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
        if _config_blocks_leases(config) or _instance_drained(conn, worker_id, instance_id):
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
            ORDER BY t.priority DESC, t.created_at ASC, COALESCE(t.input_index, 9223372036854775807) ASC, t.id ASC
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
            cur = conn.execute(
                """
                UPDATE tasks
                SET status='running', attempts=attempts+1, assigned_worker_id=?,
                    leased_at=?, lease_expires_at=?, started_at=COALESCE(started_at, ?), updated_at=?
                WHERE id=? AND status='queued'
                """,
                (assigned_id, timestamp, lease_until, timestamp, timestamp, row["id"]),
            )
            if cur.rowcount:
                leased.append(_leased_task_response(row, assigned_id=assigned_id, timestamp=timestamp, lease_until=lease_until))
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
        _maybe_expire_stale_tasks(conn)
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
        if _config_blocks_leases(config):
            conn.execute("COMMIT")
            return []
        drained_instances = _drained_instance_ids(conn, worker_id)
        cleaned = [item for item in cleaned if item not in drained_instances]
        if not cleaned:
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
            ORDER BY t.priority DESC, t.created_at ASC, COALESCE(t.input_index, 9223372036854775807) ASC, t.id ASC
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
            cur = conn.execute(
                """
                UPDATE tasks
                SET status='running', attempts=attempts+1, assigned_worker_id=?,
                    leased_at=?, lease_expires_at=?, started_at=COALESCE(started_at, ?), updated_at=?
                WHERE id=? AND status='queued'
                """,
                (assigned_id, timestamp, lease_until, timestamp, timestamp, row["id"]),
            )
            if cur.rowcount:
                item = _leased_task_response(row, assigned_id=assigned_id, timestamp=timestamp, lease_until=lease_until)
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
            # Preserve the final executor slot for durable task history. Queued
            # retries clear assignment, but terminal failures should still show
            # which worker instance produced the final failure.
            assigned_worker = assigned_id
            leased_at = task["leased_at"]
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



def bulk_update_tasks(
    *,
    action: str,
    session_id: str | None = None,
    job_id: str | None = None,
    task_ids: list[str] | None = None,
    statuses: list[str] | None = None,
    reset_attempts: bool = True,
    include_running: bool = False,
    limit: int = 5000,
    updated_by: str = "api",
) -> dict[str, Any]:
    """Apply an operator task action to a bounded selected task set.

    This is intentionally selection-driven rather than a scheduler primitive:
    it lets UI/API clients make queue/history drilldowns actionable without
    creating hidden per-task logs or a second task-control model.
    """
    init_db()
    clear_operator_cache()
    normalized_action = str(action or "").strip().lower().replace("-", "_")
    if normalized_action in {"retry_failed", "retry_cancelled", "requeue"}:
        normalized_action = "retry"
    if normalized_action in {"cancel_queued", "cancel_filtered"}:
        normalized_action = "cancel"
    if normalized_action not in {"retry", "cancel"}:
        raise ValueError("action must be retry or cancel")

    allowed_statuses = {"queued", "running", "succeeded", "failed", "cancelled"}
    selected_statuses = {str(item).strip().lower() for item in (statuses or []) if str(item).strip()}
    if not selected_statuses:
        selected_statuses = {"failed", "cancelled"} if normalized_action == "retry" else {"queued"}
    selected_statuses = {status for status in selected_statuses if status in allowed_statuses}
    if normalized_action == "retry":
        selected_statuses &= {"failed", "cancelled"}
    elif not include_running:
        selected_statuses -= {"running"}
    if not selected_statuses:
        return {
            "action": normalized_action,
            "selected": 0,
            "changed": 0,
            "skipped": 0,
            "statuses": [],
            "tasks": [],
            "message": "no eligible statuses selected",
        }

    bounded_limit = max(1, min(int(limit or 5000), 10000))
    task_ids = [str(item) for item in (task_ids or []) if str(item)]
    timestamp = now()

    where = []
    params: list[Any] = []
    if session_id:
        where.append("j.session_id=?")
        params.append(session_id)
    if job_id:
        where.append("t.job_id=?")
        params.append(job_id)
    if task_ids:
        placeholders = ",".join("?" for _ in task_ids)
        where.append(f"t.id IN ({placeholders})")
        params.extend(task_ids)
    if not where:
        raise ValueError("session_id, job_id, or task_ids is required")
    placeholders = ",".join("?" for _ in selected_statuses)
    where.append(f"t.status IN ({placeholders})")
    params.extend(sorted(selected_statuses))

    order_sql = "COALESCE(t.input_index, 9223372036854775807) ASC, t.created_at ASC, t.id ASC"
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            f"""
            SELECT t.id, t.job_id, t.status, t.assigned_worker_id, t.input_index, t.input_key,
                   j.session_id, j.status AS job_status
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            WHERE {' AND '.join(where)}
            ORDER BY {order_sql}
            LIMIT ?
            """,
            (*params, bounded_limit),
        ).fetchall()

        selected = [row_to_dict(row) for row in rows]
        changed_ids: list[str] = []
        skipped: list[dict[str, Any]] = []
        changed_job_ids: set[str] = set()
        changed_session_ids: set[str] = set()

        if normalized_action == "retry":
            eligible = [task for task in selected if task.get("job_status") not in {"cancelled", CANCELLING_JOB_STATUS}]
            skipped = [
                {"task_id": task["id"], "reason": "job_cancelled", "status": task.get("status")}
                for task in selected
                if task.get("job_status") in {"cancelled", CANCELLING_JOB_STATUS}
            ]
            if eligible:
                ids = [task["id"] for task in eligible]
                id_sql = ",".join("?" for _ in ids)
                attempts_sql = "attempts=0," if reset_attempts else ""
                conn.execute(
                    f"""
                    UPDATE tasks
                    SET status='queued', {attempts_sql} assigned_worker_id=NULL, leased_at=NULL,
                        lease_expires_at=NULL, finished_at=NULL, error=NULL, updated_at=?
                    WHERE id IN ({id_sql})
                    """,
                    (timestamp, *ids),
                )
                changed_ids = ids
                for task in eligible:
                    changed_job_ids.add(task["job_id"])
                    if task.get("session_id"):
                        changed_session_ids.add(task["session_id"])
        else:
            eligible = selected
            if eligible:
                ids = [task["id"] for task in eligible]
                id_sql = ",".join("?" for _ in ids)
                # Queued tasks have no live assignment. Running tasks may be force-cancelled
                # with include_running=true; preserve their assignment so history shows the
                # slot that was interrupted/ignored later.
                conn.execute(
                    f"""
                    UPDATE tasks
                    SET status='cancelled',
                        assigned_worker_id=CASE WHEN status='running' THEN assigned_worker_id ELSE NULL END,
                        lease_expires_at=NULL, finished_at=?, error=NULL, updated_at=?
                    WHERE id IN ({id_sql})
                    """,
                    (timestamp, timestamp, *ids),
                )
                changed_ids = ids
                for task in eligible:
                    changed_job_ids.add(task["job_id"])
                    if task.get("session_id"):
                        changed_session_ids.add(task["session_id"])

        for jid in sorted(changed_job_ids):
            recalc_job(conn, jid)
        scope: dict[str, Any] = {"session_id": session_id, "job_id": job_id, "task_ids": task_ids[:100] if task_ids else []}
        event_entity_type = "service_session" if session_id else "job" if job_id else "task"
        event_entity_id = session_id or job_id or (changed_ids[0] if changed_ids else "bulk")
        log_event(
            conn,
            "warning",
            event_entity_type,
            str(event_entity_id),
            f"bulk task {normalized_action} applied",
            {
                "action": normalized_action,
                "selected": len(selected),
                "changed": len(changed_ids),
                "skipped": len(skipped),
                "statuses": sorted(selected_statuses),
                "reset_attempts": reset_attempts,
                "include_running": include_running,
                "updated_by": updated_by,
                **{k: v for k, v in scope.items() if v},
            },
            code="TaskBulkActionRun",
        )
        task_rows = []
        if changed_ids:
            id_sql = ",".join("?" for _ in changed_ids)
            task_rows = conn.execute(
                f"""
                SELECT id AS task_id, job_id, status, input_index, input_key, assigned_worker_id, updated_at
                FROM tasks WHERE id IN ({id_sql})
                ORDER BY COALESCE(input_index, 9223372036854775807) ASC, created_at ASC, id ASC
                """,
                changed_ids,
            ).fetchall()
        conn.execute("COMMIT")

    return {
        "action": normalized_action,
        "selected": len(selected),
        "changed": len(changed_ids),
        "skipped": len(skipped),
        "statuses": sorted(selected_statuses),
        "reset_attempts": reset_attempts,
        "include_running": include_running,
        "session_id": session_id,
        "job_id": job_id,
        "changed_jobs": sorted(changed_job_ids),
        "changed_sessions": sorted(changed_session_ids),
        "tasks": [row_to_dict(row) for row in task_rows],
        "skipped_tasks": skipped,
        "limit": bounded_limit,
    }

def retry_task(task_id: str, reset_attempts: bool = True) -> dict[str, Any] | None:
    init_db()
    clear_operator_cache()
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
    clear_operator_cache()
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



def _split_executor_assignment(assigned_worker_id: Any, known_worker_ids: set[str] | None = None) -> tuple[str | None, str | None]:
    """Split a stored executor id into worker node and logical instance ids.

    TaskGrid stores precise execution slots in ``assigned_worker_id`` using the
    ``worker-id-instance-id`` form. Worker ids may themselves contain hyphens,
    so prefer matching the longest known worker id and fall back to the common
    ``-instance-`` suffix pattern used by worker supervisors.
    """
    assigned = str(assigned_worker_id or "").strip()
    if not assigned:
        return None, None
    known_worker_ids = known_worker_ids or set()
    for worker_id in sorted((item for item in known_worker_ids if item), key=len, reverse=True):
        if assigned == worker_id:
            return worker_id, None
        prefix = f"{worker_id}-"
        if assigned.startswith(prefix):
            return worker_id, assigned[len(prefix):] or None
    marker = "-instance-"
    if marker in assigned:
        left, right = assigned.rsplit(marker, 1)
        if left and right:
            return left, f"instance-{right}"
    return assigned, None


def _seconds_between(start: Any, end: Any | None = None) -> float | None:
    started = _parse_utc(start)
    if not started:
        return None
    ended = _parse_utc(end) if end else datetime.now(timezone.utc)
    if not ended:
        return None
    return max(0.0, (ended - started).total_seconds())


def get_session_assignments(session_id: str, *, limit: int = 1000) -> dict[str, Any] | None:
    """Return a session-centric view of current task placement and slot stats.

    This powers the session drilldown UI: operators can see queued/running/final
    task counts, which worker/instance each running task is assigned to, and a
    compact per-worker/per-instance rollup for the selected service session.
    """
    init_db()
    limit = max(1, min(5000, int(limit or 1000)))
    timestamp = now()
    with connection() as conn:
        session_row = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        if not session_row:
            return None
        session = row_to_dict(session_row)
        job_rows = conn.execute("SELECT * FROM jobs WHERE session_id=? ORDER BY created_at ASC", (session_id,)).fetchall()
        jobs_by_id = {row["id"]: row_to_dict(row) for row in job_rows}
        worker_rows = conn.execute(
            """
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason,
                   wc.draining, wc.drain_at, wc.drain_reason,
                   wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
            FROM workers w
            LEFT JOIN worker_configs wc ON wc.worker_id = w.id
            ORDER BY w.last_heartbeat_at DESC
            """
        ).fetchall()
        workers = [_worker_capability_row(row) for row in worker_rows]
        worker_by_id = {str(worker.get("id")): worker for worker in workers if worker.get("id")}
        known_worker_ids = set(worker_by_id)
        drained_rows = conn.execute("SELECT * FROM worker_instance_configs ORDER BY worker_id ASC, instance_id ASC").fetchall()
        drained_instances = {
            (str(row["worker_id"]), str(row["instance_id"])): row_to_dict(row)
            for row in drained_rows
            if int(row["draining"] or 0)
        }
        task_rows = conn.execute(
            """
            SELECT t.*, j.name AS job_name, j.status AS job_status, j.priority AS job_priority,
                   j.created_at AS job_created_at
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            WHERE j.session_id=?
            ORDER BY
              CASE t.status WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'failed' THEN 2 WHEN 'cancelled' THEN 3 ELSE 4 END,
              COALESCE(t.input_index, 9223372036854775807) ASC,
              t.created_at ASC,
              t.id ASC
            LIMIT ?
            """,
            (session_id, limit),
        ).fetchall()

    status_counts: dict[str, int] = {"queued": 0, "running": 0, "succeeded": 0, "failed": 0, "cancelled": 0}
    worker_stats: dict[str, dict[str, Any]] = {}
    unassigned = 0
    tasks: list[dict[str, Any]] = []

    def worker_state(worker: dict[str, Any] | None) -> str:
        if not worker:
            return "unknown"
        if worker.get("disabled"):
            return "disabled"
        if worker.get("draining"):
            return "draining"
        if not worker.get("heartbeat_active"):
            return "stale"
        return "online"

    def ensure_worker(worker_id: str | None) -> dict[str, Any] | None:
        if not worker_id:
            return None
        worker = worker_by_id.get(worker_id)
        stats = worker_stats.get(worker_id)
        if stats:
            return stats
        metadata = (worker or {}).get("metadata", {}) or {}
        desired = int((worker or {}).get("desired_concurrency") or _metadata_concurrency(metadata, default=1))
        active_instances = int((worker or {}).get("active_concurrency") or desired or 1)
        stats = {
            "worker_id": worker_id,
            "hostname": (worker or {}).get("hostname") or "",
            "state": worker_state(worker),
            "service_name": (worker or {}).get("service_name") or metadata.get("service_name") or metadata.get("service") or "",
            "service_version": (worker or {}).get("service_version") or metadata.get("service_version") or metadata.get("image_version") or "",
            "taskgrid_version": (worker or {}).get("version") or "",
            "tags": sorted((worker or {}).get("tags") or []),
            "task_types": sorted((worker or {}).get("task_types") or []),
            "desired_instances": desired,
            "active_instances": active_instances,
            "disabled": bool((worker or {}).get("disabled")),
            "draining": bool((worker or {}).get("draining")),
            "last_heartbeat_at": (worker or {}).get("last_heartbeat_at"),
            "session_tasks": 0,
            "running_tasks": 0,
            "queued_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "cancelled_tasks": 0,
            "instances": {},
        }
        for i in range(1, max(1, active_instances) + 1):
            instance_id = f"instance-{i:03d}"
            drained = drained_instances.get((worker_id, instance_id))
            stats["instances"][instance_id] = {
                "worker_id": worker_id,
                "instance_id": instance_id,
                "state": "drained" if drained else ("disabled" if stats["disabled"] else ("draining" if stats["draining"] else ("stale" if stats["state"] == "stale" else "idle"))),
                "draining": bool(drained),
                "drain_reason": (drained or {}).get("drain_reason") or "",
                "session_tasks": 0,
                "running_tasks": 0,
                "completed_tasks": 0,
                "failed_tasks": 0,
                "cancelled_tasks": 0,
                "current_task": None,
            }
        worker_stats[worker_id] = stats
        return stats

    for row in task_rows:
        raw = row_to_dict(row)
        status = str(raw.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        worker_id, instance_id = _split_executor_assignment(raw.get("assigned_worker_id"), known_worker_ids)
        job = jobs_by_id.get(raw.get("job_id"), {})
        runtime_seconds = _seconds_between(raw.get("started_at"), raw.get("finished_at") if status != "running" else None)
        item = {
            "task_id": raw.get("id"),
            "job_id": raw.get("job_id"),
            "job_name": raw.get("job_name") or job.get("name") or "",
            "job_status": raw.get("job_status") or job.get("status") or "",
            "task_type": raw.get("task_type"),
            "input_index": raw.get("input_index"),
            "input_key": raw.get("input_key"),
            "status": status,
            "attempts": int(raw.get("attempts") or 0),
            "max_retries": int(raw.get("max_retries") or 0),
            "assigned_worker_id": raw.get("assigned_worker_id"),
            "worker_id": worker_id,
            "instance_id": instance_id,
            "executor_id": raw.get("assigned_worker_id"),
            "created_at": raw.get("created_at"),
            "started_at": raw.get("started_at"),
            "finished_at": raw.get("finished_at"),
            "updated_at": raw.get("updated_at"),
            "lease_expires_at": raw.get("lease_expires_at"),
            "runtime_seconds": runtime_seconds,
            "error": raw.get("error"),
        }
        tasks.append(item)
        if not worker_id:
            unassigned += 1
            continue
        stats = ensure_worker(worker_id)
        if not stats:
            continue
        stats["session_tasks"] += 1
        if status == "running":
            stats["running_tasks"] += 1
        elif status == "queued":
            stats["queued_tasks"] += 1
        elif status == "succeeded":
            stats["completed_tasks"] += 1
        elif status == "failed":
            stats["failed_tasks"] += 1
        elif status == "cancelled":
            stats["cancelled_tasks"] += 1
        if instance_id:
            instances = stats["instances"]
            inst = instances.setdefault(
                instance_id,
                {
                    "worker_id": worker_id,
                    "instance_id": instance_id,
                    "state": "idle",
                    "draining": bool(drained_instances.get((worker_id, instance_id))),
                    "drain_reason": (drained_instances.get((worker_id, instance_id)) or {}).get("drain_reason") or "",
                    "session_tasks": 0,
                    "running_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0,
                    "cancelled_tasks": 0,
                    "current_task": None,
                },
            )
            inst["session_tasks"] += 1
            if status == "running":
                inst["running_tasks"] += 1
                inst["state"] = "running"
                inst["current_task"] = item
            elif status == "succeeded":
                inst["completed_tasks"] += 1
            elif status == "failed":
                inst["failed_tasks"] += 1
            elif status == "cancelled":
                inst["cancelled_tasks"] += 1

    normalized_workers: list[dict[str, Any]] = []
    for stats in worker_stats.values():
        instances = list(stats.pop("instances").values())
        instances.sort(key=lambda item: str(item.get("instance_id") or ""))
        stats["instances"] = instances
        normalized_workers.append(stats)
    normalized_workers.sort(key=lambda item: str(item.get("worker_id") or ""))

    running_tasks = [item for item in tasks if item["status"] == "running"]
    return {
        "session": session,
        "session_id": session_id,
        "name": session.get("name"),
        "status": session.get("status"),
        "checked_at": timestamp,
        "task_limit": limit,
        "task_count_returned": len(tasks),
        "status_counts": status_counts,
        "unassigned_tasks": unassigned,
        "running_assignments": len(running_tasks),
        "assigned_workers": len(normalized_workers),
        "assigned_instances": sum(1 for worker in normalized_workers for inst in worker.get("instances", []) if inst.get("session_tasks")),
        "current_assignments": running_tasks,
        "workers": normalized_workers,
        "tasks": tasks,
    }


def _task_assignment_summary(raw: dict[str, Any], *, known_worker_ids: set[str] | None = None) -> dict[str, Any]:
    status = str(raw.get("status") or "unknown")
    worker_id, instance_id = _split_executor_assignment(raw.get("assigned_worker_id"), known_worker_ids)
    return {
        "task_id": raw.get("id"),
        "job_id": raw.get("job_id"),
        "job_name": raw.get("job_name") or "",
        "job_status": raw.get("job_status") or "",
        "session_id": raw.get("session_id"),
        "session_name": raw.get("session_name") or "",
        "session_status": raw.get("session_status") or "",
        "task_type": raw.get("task_type"),
        "input_index": raw.get("input_index"),
        "input_key": raw.get("input_key"),
        "status": status,
        "attempts": int(raw.get("attempts") or 0),
        "max_retries": int(raw.get("max_retries") or 0),
        "assigned_worker_id": raw.get("assigned_worker_id"),
        "worker_id": worker_id,
        "instance_id": instance_id,
        "executor_id": raw.get("assigned_worker_id"),
        "created_at": raw.get("created_at"),
        "started_at": raw.get("started_at"),
        "finished_at": raw.get("finished_at"),
        "updated_at": raw.get("updated_at"),
        "lease_expires_at": raw.get("lease_expires_at"),
        "runtime_seconds": _seconds_between(raw.get("started_at"), raw.get("finished_at") if status != "running" else None),
        "error": raw.get("error"),
    }


def _worker_state_from_worker(worker: dict[str, Any] | None) -> str:
    if not worker:
        return "unknown"
    if worker.get("disabled"):
        return "disabled"
    if worker.get("draining"):
        return "draining"
    if not worker.get("heartbeat_active"):
        return "stale"
    return "online"


def _worker_slots_from_metadata(worker: dict[str, Any], drained_instances: dict[tuple[str, str], dict[str, Any]]) -> dict[str, dict[str, Any]]:
    worker_id = str(worker.get("id") or "")
    metadata = worker.get("metadata", {}) or {}
    desired = int(worker.get("desired_concurrency") or _metadata_concurrency(metadata, default=1) or 1)
    active_instances = int(worker.get("active_concurrency") or desired or 1)
    snapshot_rows = metadata.get("instances") if isinstance(metadata.get("instances"), list) else []
    slots: dict[str, dict[str, Any]] = {}

    def base_slot(instance_id: str) -> dict[str, Any]:
        drained = drained_instances.get((worker_id, instance_id))
        return {
            "worker_id": worker_id,
            "instance_id": instance_id,
            "executor_id": executor_id(worker_id, instance_id),
            "state": "idle",
            "worker_state": _worker_state_from_worker(worker),
            "worker_disabled": bool(worker.get("disabled")),
            "worker_draining": bool(worker.get("draining")),
            "worker_stale": not bool(worker.get("heartbeat_active")),
            "instance_draining": bool(drained),
            "draining": bool(drained),
            "drain_reason": (drained or {}).get("drain_reason") or "",
            "status": "idle",
            "current_task": None,
            "current_task_id": None,
            "advertised_current_task_id": None,
            "tasks_completed": 0,
            "last_poll_at": None,
            "last_error": None,
            "log_path": f"instances/{instance_id}/worker.log",
        }

    for i in range(1, max(0, active_instances) + 1):
        iid = f"instance-{i:03d}"
        slots[iid] = base_slot(iid)

    for snap in snapshot_rows:
        if not isinstance(snap, dict):
            continue
        iid = str(snap.get("instance_id") or "").strip()
        if not iid:
            continue
        slot = slots.setdefault(iid, base_slot(iid))
        slot["status"] = snap.get("status") or slot["status"]
        slot["advertised_current_task_id"] = snap.get("current_task_id")
        slot["current_task_id"] = snap.get("current_task_id")
        slot["tasks_completed"] = int(snap.get("tasks_completed") or 0)
        slot["last_poll_at"] = snap.get("last_poll_at")
        slot["last_error"] = snap.get("last_error")

    for (drain_worker_id, iid), drained in drained_instances.items():
        if drain_worker_id != worker_id:
            continue
        slot = slots.setdefault(iid, base_slot(iid))
        slot["instance_draining"] = True
        slot["draining"] = True
        slot["drain_reason"] = drained.get("drain_reason") or ""

    for slot in slots.values():
        if slot.get("current_task"):
            slot["state"] = "running"
        elif slot.get("worker_disabled"):
            slot["state"] = "disabled"
        elif slot.get("worker_stale"):
            slot["state"] = "stale"
        elif slot.get("instance_draining"):
            slot["state"] = "drained"
        elif slot.get("worker_draining"):
            slot["state"] = "draining"
        else:
            slot["state"] = str(slot.get("status") or "idle")
            if slot["state"] in {"leasing", "polling"}:
                slot["state"] = "idle"
    return slots


def list_executors(*, limit: int = 1000, active_seconds: int | None = None, refresh: bool = False) -> dict[str, Any]:
    """Return a manager-wide execution-slot dashboard.

    Unlike session assignments, this shows every known worker instance/slot and
    its current task regardless of session. It is derived from worker heartbeat
    metadata, drain config, and durable running task rows.
    """
    init_db()
    bounded_limit = max(1, min(5000, int(limit or 1000)))
    active_for = DEFAULT_WORKER_ACTIVE_SECONDS if active_seconds is None else int(active_seconds)
    cache_key = ("executors", str(db_path()), bounded_limit, active_for)
    cached = _operator_cache_get(cache_key, refresh=refresh)
    if cached is not None:
        return cached
    timestamp = now()
    with connection() as conn:
        worker_rows = conn.execute(
            """
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason,
                   wc.draining, wc.drain_at, wc.drain_reason, wc.updated_at AS config_updated_at,
                   wc.updated_by AS config_updated_by
            FROM workers w
            LEFT JOIN worker_configs wc ON wc.worker_id = w.id
            ORDER BY w.last_heartbeat_at DESC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()
        workers = [_worker_capability_row(row, active_seconds=active_for) for row in worker_rows]
        known_worker_ids = {str(worker.get("id")) for worker in workers if worker.get("id")}
        drained_rows = conn.execute("SELECT * FROM worker_instance_configs ORDER BY worker_id ASC, instance_id ASC").fetchall()
        drained_instances = {
            (str(row["worker_id"]), str(row["instance_id"])): row_to_dict(row)
            for row in drained_rows
            if int(row["draining"] or 0)
        }
        running_rows = conn.execute(
            """
            SELECT t.*, j.name AS job_name, j.status AS job_status, j.session_id AS session_id,
                   s.name AS session_name, s.status AS session_status
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            LEFT JOIN service_sessions s ON s.id = j.session_id
            WHERE t.status='running'
            ORDER BY t.started_at ASC, t.updated_at ASC, t.id ASC
            """
        ).fetchall()

    running_tasks = [_task_assignment_summary(row_to_dict(row), known_worker_ids=known_worker_ids) for row in running_rows]
    running_by_executor = {str(item.get("executor_id") or ""): item for item in running_tasks if item.get("executor_id")}
    running_by_worker: dict[str, list[dict[str, Any]]] = {}
    for item in running_tasks:
        if item.get("worker_id"):
            running_by_worker.setdefault(str(item["worker_id"]), []).append(item)

    worker_items: list[dict[str, Any]] = []
    all_instances: list[dict[str, Any]] = []
    for worker in workers:
        worker_id = str(worker.get("id") or "")
        metadata = worker.get("metadata", {}) or {}
        slots = _worker_slots_from_metadata(worker, drained_instances)
        for item in running_by_worker.get(worker_id, []):
            instance_id = item.get("instance_id") or "node"
            slot = slots.setdefault(instance_id, {
                "worker_id": worker_id,
                "instance_id": instance_id,
                "executor_id": item.get("executor_id") or executor_id(worker_id, instance_id),
                "state": "running",
                "worker_state": _worker_state_from_worker(worker),
                "worker_disabled": bool(worker.get("disabled")),
                "worker_draining": bool(worker.get("draining")),
                "worker_stale": not bool(worker.get("heartbeat_active")),
                "instance_draining": bool(drained_instances.get((worker_id, instance_id))),
                "draining": bool(drained_instances.get((worker_id, instance_id))),
                "drain_reason": (drained_instances.get((worker_id, instance_id)) or {}).get("drain_reason") or "",
                "status": "running",
                "current_task": None,
                "current_task_id": None,
                "advertised_current_task_id": None,
                "tasks_completed": 0,
                "last_poll_at": None,
                "last_error": None,
                "log_path": f"instances/{instance_id}/worker.log" if instance_id != "node" else "supervisor.log",
            })
            slot["state"] = "running"
            slot["status"] = "running"
            slot["current_task"] = item
            slot["current_task_id"] = item.get("task_id")

        normalized_slots = list(slots.values())
        normalized_slots.sort(key=lambda item: str(item.get("instance_id") or ""))
        for slot in normalized_slots:
            task = slot.get("current_task")
            if task:
                slot["session_id"] = task.get("session_id")
                slot["job_id"] = task.get("job_id")
                slot["task_id"] = task.get("task_id")
            slot["service_name"] = worker.get("service_name") or metadata.get("service_name") or metadata.get("service") or ""
            slot["service_version"] = worker.get("service_version") or metadata.get("service_version") or metadata.get("image_version") or ""
            slot["taskgrid_version"] = worker.get("version") or ""
            slot["hostname"] = worker.get("hostname") or ""
            all_instances.append(slot)

        worker_items.append({
            "worker_id": worker_id,
            "hostname": worker.get("hostname") or "",
            "state": _worker_state_from_worker(worker),
            "status": worker.get("status") or "",
            "service_name": worker.get("service_name") or metadata.get("service_name") or metadata.get("service") or "",
            "service_version": worker.get("service_version") or metadata.get("service_version") or metadata.get("image_version") or "",
            "taskgrid_version": worker.get("version") or "",
            "tags": sorted(worker.get("tags") or []),
            "task_types": sorted(worker.get("task_types") or []),
            "desired_instances": int(worker.get("desired_concurrency") or _metadata_concurrency(metadata, default=1) or 1),
            "active_instances": int(worker.get("active_concurrency") or 0),
            "running_instances": sum(1 for slot in normalized_slots if slot.get("state") == "running"),
            "idle_instances": sum(1 for slot in normalized_slots if slot.get("state") == "idle"),
            "drained_instances": sum(1 for slot in normalized_slots if slot.get("instance_draining")),
            "disabled": bool(worker.get("disabled")),
            "draining": bool(worker.get("draining")),
            "stale": not bool(worker.get("heartbeat_active")),
            "last_heartbeat_at": worker.get("last_heartbeat_at"),
            "instances": normalized_slots,
        })

    state_counts: dict[str, int] = {}
    for slot in all_instances:
        state = str(slot.get("state") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1

    return _operator_cache_set(cache_key, {
        "checked_at": timestamp,
        "worker_count": len(worker_items),
        "instance_count": len(all_instances),
        "running_instances": state_counts.get("running", 0),
        "idle_instances": state_counts.get("idle", 0),
        "drained_instances": sum(1 for slot in all_instances if slot.get("instance_draining")),
        "disabled_instances": state_counts.get("disabled", 0),
        "stale_instances": state_counts.get("stale", 0),
        "state_counts": state_counts,
        "running_tasks": len(running_tasks),
        "workers": worker_items,
        "instances": all_instances,
    })


def get_executor(worker_id: str, instance_id: str, *, recent_limit: int = 100) -> dict[str, Any] | None:
    init_db()
    summary = list_executors(limit=1000)
    instance = None
    worker = None
    for worker_item in summary.get("workers", []):
        if worker_item.get("worker_id") == worker_id:
            worker = worker_item
            for slot in worker_item.get("instances", []):
                if slot.get("instance_id") == instance_id:
                    instance = slot
                    break
            break
    if worker is None:
        return None
    if instance is None:
        instance = {
            "worker_id": worker_id,
            "instance_id": instance_id,
            "executor_id": executor_id(worker_id, instance_id),
            "state": "unknown",
            "current_task": None,
            "log_path": f"instances/{instance_id}/worker.log",
        }
    exec_id = executor_id(worker_id, instance_id)
    with connection() as conn:
        rows = conn.execute(
            """
            SELECT t.*, j.name AS job_name, j.status AS job_status, j.session_id AS session_id,
                   s.name AS session_name, s.status AS session_status
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            LEFT JOIN service_sessions s ON s.id = j.session_id
            WHERE t.assigned_worker_id=?
            ORDER BY t.updated_at DESC, t.created_at DESC, t.id DESC
            LIMIT ?
            """,
            (exec_id, recent_limit),
        ).fetchall()
    recent_tasks = [_task_assignment_summary(row_to_dict(row), known_worker_ids={worker_id}) for row in rows]
    return {
        "checked_at": now(),
        "worker": {key: value for key, value in worker.items() if key != "instances"},
        "instance": instance,
        "current_task": instance.get("current_task"),
        "recent_tasks": recent_tasks,
        "recent_task_count": len(recent_tasks),
    }

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
        "input_index": task.get("input_index"),
        "input_key": task.get("input_key"),
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


def get_job_results(job_id: str, *, order: str = "input") -> dict[str, Any] | None:
    init_db()
    with connection() as conn:
        job_row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job_row:
            return None
        job = row_to_dict(job_row)
        task_rows = conn.execute(
            "SELECT * FROM tasks WHERE job_id=? ORDER BY COALESCE(input_index, 9223372036854775807) ASC, created_at ASC, id ASC",
            (job_id,),
        ).fetchall()
        tasks = _ordered_result_tasks([_task_result_row(row_to_dict(row)) for row in task_rows], order=order)
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
                {"task_id": item["task_id"], "input_index": item.get("input_index"), "input_key": item.get("input_key"), "status": item["status"], "result": item.get("result"), "error": item.get("error")}
                for item in tasks
            ],
        }


def get_session_results(session_id: str, *, order: str = "input") -> dict[str, Any] | None:
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
            task_rows = conn.execute(
                "SELECT * FROM tasks WHERE job_id=? ORDER BY COALESCE(input_index, 9223372036854775807) ASC, created_at ASC, id ASC",
                (job["id"],),
            ).fetchall()
            tasks = _ordered_result_tasks([_task_result_row(row_to_dict(row)) for row in task_rows], order=order)
            for item in tasks:
                item["job_created_at"] = job.get("created_at")
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
            "tasks": _ordered_result_tasks(all_tasks, order=order),
            "results": [
                {"task_id": item["task_id"], "job_id": item["job_id"], "input_index": item.get("input_index"), "input_key": item.get("input_key"), "status": item["status"], "result": item.get("result"), "error": item.get("error")}
                for item in _ordered_result_tasks(all_tasks, order=order)
            ],
        }



def get_session_task_history(
    session_id: str,
    *,
    limit: int = 5000,
    status: str | None = None,
    job_id: str | None = None,
    order: str = "input",
) -> dict[str, Any] | None:
    """Return durable task history for a service session.

    Unlike ``get_session_assignments(...)``, which is optimized around current
    placement, this endpoint is history-first: finished sessions remain fully
    drillable, with every task's final worker/instance assignment, attempt
    counts, timing, payload/result/error, and job mapping.
    """
    init_db()
    limit = max(1, min(10000, int(limit or 5000)))
    normalized_status = str(status or "").strip().lower() or None
    normalized_job_id = str(job_id or "").strip() or None
    with connection() as conn:
        session_row = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        if not session_row:
            return None
        session = row_to_dict(session_row)
        job_rows = conn.execute("SELECT * FROM jobs WHERE session_id=? ORDER BY created_at ASC, id ASC", (session_id,)).fetchall()
        jobs = [row_to_dict(row) for row in job_rows]
        worker_rows = conn.execute("SELECT id FROM workers").fetchall()
        known_worker_ids = {str(row["id"]) for row in worker_rows if row["id"]}

        clauses = ["j.session_id=?"]
        params: list[Any] = [session_id]
        if normalized_status:
            clauses.append("t.status=?")
            params.append(normalized_status)
        if normalized_job_id:
            clauses.append("j.id=?")
            params.append(normalized_job_id)
        params.append(limit)
        rows = conn.execute(
            f"""
            SELECT t.*, j.name AS job_name, j.status AS job_status, j.priority AS job_priority,
                   j.created_at AS job_created_at, j.session_id AS session_id,
                   s.name AS session_name, s.status AS session_status
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            LEFT JOIN service_sessions s ON s.id = j.session_id
            WHERE {' AND '.join(clauses)}
            ORDER BY
              j.created_at ASC,
              j.id ASC,
              COALESCE(t.input_index, 9223372036854775807) ASC,
              t.created_at ASC,
              t.id ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

    tasks: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {"queued": 0, "running": 0, "succeeded": 0, "failed": 0, "cancelled": 0}
    worker_ids: set[str] = set()
    instance_keys: set[tuple[str, str]] = set()
    total_runtime = 0.0
    runtime_count = 0

    for row in rows:
        raw = row_to_dict(row)
        assignment = _task_assignment_summary(raw, known_worker_ids=known_worker_ids)
        result_info = _task_result_row(raw)
        task = {
            **assignment,
            "job_priority": raw.get("job_priority"),
            "job_created_at": raw.get("job_created_at"),
            "payload": result_info.get("payload"),
            "payload_bytes": result_info.get("payload_bytes"),
            "result": result_info.get("result"),
            "result_bytes": result_info.get("result_bytes"),
        }
        status_counts[task["status"]] = status_counts.get(task["status"], 0) + 1
        if task.get("worker_id"):
            worker_ids.add(str(task["worker_id"]))
        if task.get("worker_id") and task.get("instance_id"):
            instance_keys.add((str(task["worker_id"]), str(task["instance_id"])))
        if task.get("runtime_seconds") is not None:
            total_runtime += float(task["runtime_seconds"] or 0)
            runtime_count += 1
        tasks.append(task)

    ordered_tasks = _ordered_result_tasks(tasks, order=order)
    return {
        "session_id": session["id"],
        "name": session["name"],
        "status": session["status"],
        "priority": session.get("priority", 0),
        "total_jobs": session.get("total_jobs", len(jobs)),
        "total_tasks": session.get("total_tasks", len(tasks)),
        "queued_tasks": session.get("queued_tasks", 0),
        "running_tasks": session.get("running_tasks", 0),
        "completed_tasks": session.get("completed_tasks", 0),
        "failed_tasks": session.get("failed_tasks", 0),
        "cancelled_tasks": session.get("cancelled_tasks", 0),
        "created_at": session.get("created_at"),
        "started_at": session.get("started_at"),
        "finished_at": session.get("finished_at"),
        "filters": {"status": normalized_status, "job_id": normalized_job_id, "order": order, "limit": limit},
        "status_counts": status_counts,
        "assigned_workers": len(worker_ids),
        "assigned_instances": len(instance_keys),
        "average_runtime_seconds": (total_runtime / runtime_count) if runtime_count else None,
        "jobs": [
            {
                "job_id": job["id"],
                "name": job["name"],
                "task_type": job["task_type"],
                "status": job["status"],
                "priority": job.get("priority", 0),
                "total_tasks": job.get("total_tasks", 0),
                "completed_tasks": job.get("completed_tasks", 0),
                "failed_tasks": job.get("failed_tasks", 0),
                "cancelled_tasks": job.get("cancelled_tasks", 0),
                "created_at": job.get("created_at"),
                "started_at": job.get("started_at"),
                "finished_at": job.get("finished_at"),
            }
            for job in jobs
        ],
        "tasks": ordered_tasks,
        "task_count": len(ordered_tasks),
        "has_more": len(tasks) >= limit,
    }




def _result_stream_summary_from_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_id": job.get("id") or job.get("job_id"),
        "session_id": job.get("session_id"),
        "name": job.get("name"),
        "task_type": job.get("task_type"),
        "status": job.get("status"),
        "priority": job.get("priority", 0),
        "total_tasks": int(job.get("total_tasks") or 0),
        "queued_tasks": int(job.get("total_tasks") or 0) - int(job.get("completed_tasks") or 0) - int(job.get("failed_tasks") or 0) - int(job.get("cancelled_tasks") or 0),
        "completed_tasks": int(job.get("completed_tasks") or 0),
        "failed_tasks": int(job.get("failed_tasks") or 0),
        "cancelled_tasks": int(job.get("cancelled_tasks") or 0),
        "created_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
    }


def _result_stream_summary_from_session(session: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": session.get("id") or session.get("session_id"),
        "name": session.get("name"),
        "status": session.get("status"),
        "priority": session.get("priority", 0),
        "total_jobs": int(session.get("total_jobs") or 0),
        "total_tasks": int(session.get("total_tasks") or 0),
        "queued_tasks": int(session.get("queued_tasks") or 0),
        "running_tasks": int(session.get("running_tasks") or 0),
        "completed_tasks": int(session.get("completed_tasks") or 0),
        "failed_tasks": int(session.get("failed_tasks") or 0),
        "cancelled_tasks": int(session.get("cancelled_tasks") or 0),
        "created_at": session.get("created_at"),
        "started_at": session.get("started_at"),
        "finished_at": session.get("finished_at"),
    }


def _cursor_clause(alias: str, after_updated_at: str | None, after_task_id: str | None) -> tuple[str, list[Any]]:
    if not after_updated_at:
        return "", []
    return f" AND ({alias}.updated_at > ? OR ({alias}.updated_at = ? AND {alias}.id > ?))", [after_updated_at, after_updated_at, after_task_id or ""]


def latest_job_result_cursor(job_id: str) -> dict[str, str | None] | None:
    """Return the latest terminal task cursor for a job result stream."""
    init_db()
    with connection() as conn:
        row = conn.execute(
            """
            SELECT updated_at, id AS task_id
            FROM tasks
            WHERE job_id=? AND status IN ('succeeded','failed','cancelled')
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (job_id,),
        ).fetchone()
        if not row:
            return {"updated_at": None, "task_id": None}
        return {"updated_at": row["updated_at"], "task_id": row["task_id"]}


def latest_session_result_cursor(session_id: str) -> dict[str, str | None] | None:
    """Return the latest terminal task cursor for a session result stream."""
    init_db()
    with connection() as conn:
        exists = conn.execute("SELECT 1 FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        if not exists:
            return None
        row = conn.execute(
            """
            SELECT t.updated_at, t.id AS task_id
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            WHERE j.session_id=? AND t.status IN ('succeeded','failed','cancelled')
            ORDER BY t.updated_at DESC, t.id DESC
            LIMIT 1
            """,
            (session_id,),
        ).fetchone()
        if not row:
            return {"updated_at": None, "task_id": None}
        return {"updated_at": row["updated_at"], "task_id": row["task_id"]}


def get_job_result_updates(
    job_id: str,
    *,
    after_updated_at: str | None = None,
    after_task_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any] | None:
    """Return newly terminal task results for an SSE/polling cursor.

    The cursor is a stable `(updated_at, task_id)` pair so multiple tasks that
    finish in the same millisecond are still streamed exactly once.
    """
    init_db()
    limit = max(1, min(1000, int(limit or 200)))
    with connection() as conn:
        job_row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job_row:
            return None
        job = row_to_dict(job_row)
        clause, params = _cursor_clause("t", after_updated_at, after_task_id)
        rows = conn.execute(
            f"""
            SELECT t.*
            FROM tasks t
            WHERE t.job_id=? AND t.status IN ('succeeded','failed','cancelled')
            {clause}
            ORDER BY t.updated_at ASC, t.id ASC
            LIMIT ?
            """,
            [job_id, *params, limit],
        ).fetchall()
        tasks = [_task_result_row(row_to_dict(row)) for row in rows]
        next_cursor = {"updated_at": after_updated_at, "task_id": after_task_id}
        if tasks:
            last = tasks[-1]
            next_cursor = {"updated_at": last.get("updated_at"), "task_id": last.get("task_id")}
        return {
            "scope": "job",
            "terminal": job.get("status") in TERMINAL_JOB_STATUSES,
            "summary": _result_stream_summary_from_job(job),
            "tasks": tasks,
            "cursor": next_cursor,
            "has_more": len(tasks) >= limit,
        }


def get_session_result_updates(
    session_id: str,
    *,
    after_updated_at: str | None = None,
    after_task_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any] | None:
    """Return newly terminal task results across every job in a service session."""
    init_db()
    limit = max(1, min(1000, int(limit or 200)))
    with connection() as conn:
        session_row = conn.execute("SELECT * FROM service_sessions WHERE id=?", (session_id,)).fetchone()
        if not session_row:
            return None
        session = row_to_dict(session_row)
        clause, params = _cursor_clause("t", after_updated_at, after_task_id)
        rows = conn.execute(
            f"""
            SELECT t.*, j.created_at AS job_created_at, j.name AS job_name
            FROM tasks t
            JOIN jobs j ON j.id = t.job_id
            WHERE j.session_id=? AND t.status IN ('succeeded','failed','cancelled')
            {clause}
            ORDER BY t.updated_at ASC, t.id ASC
            LIMIT ?
            """,
            [session_id, *params, limit],
        ).fetchall()
        tasks: list[dict[str, Any]] = []
        for row in rows:
            task = _task_result_row(row_to_dict(row))
            task["job_created_at"] = row["job_created_at"]
            task["job_name"] = row["job_name"]
            tasks.append(task)
        next_cursor = {"updated_at": after_updated_at, "task_id": after_task_id}
        if tasks:
            last = tasks[-1]
            next_cursor = {"updated_at": last.get("updated_at"), "task_id": last.get("task_id")}
        return {
            "scope": "session",
            "terminal": session.get("status") in TERMINAL_JOB_STATUSES,
            "summary": _result_stream_summary_from_session(session),
            "tasks": tasks,
            "cursor": next_cursor,
            "has_more": len(tasks) >= limit,
        }

def _ordered_result_tasks(items: list[dict[str, Any]], order: str = "input") -> list[dict[str, Any]]:
    key = str(order or "input").lower()
    if key in {"completed", "finished", "finished_at"}:
        return sorted(items, key=lambda item: (item.get("finished_at") or "9999", item.get("created_at") or "", item.get("input_index") if item.get("input_index") is not None else 9223372036854775807, item.get("task_id") or ""))
    if key in {"started", "started_at"}:
        return sorted(items, key=lambda item: (item.get("started_at") or "9999", item.get("created_at") or "", item.get("input_index") if item.get("input_index") is not None else 9223372036854775807, item.get("task_id") or ""))
    if key == "status":
        return sorted(items, key=lambda item: (item.get("status") or "", item.get("input_index") if item.get("input_index") is not None else 9223372036854775807, item.get("created_at") or "", item.get("task_id") or ""))
    return sorted(items, key=lambda item: (item.get("job_created_at") or "", item.get("job_id") or "", item.get("input_index") if item.get("input_index") is not None else 9223372036854775807, item.get("created_at") or "", item.get("task_id") or ""))


def _results_csv_rows(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in tasks:
        rows.append(
            {
                "input_index": item.get("input_index"),
                "input_key": item.get("input_key") or "",
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
        "input_index",
        "input_key",
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
    data = get_job_results(job_id, order=order)
    if not data:
        return None
    tasks = _ordered_result_tasks(data.get("tasks", []), order=order)
    if failed_only:
        tasks = [item for item in tasks if item.get("status") == "failed" or item.get("error")]
    data = dict(data)
    data["tasks"] = tasks
    data["results"] = [
        {"task_id": item["task_id"], "input_index": item.get("input_index"), "input_key": item.get("input_key"), "status": item["status"], "result": item.get("result"), "error": item.get("error")}
        for item in tasks
    ]
    safe_name = _safe_log_part(data.get("name") or job_id)
    if str(fmt).lower() == "csv":
        suffix = "failed-tasks" if failed_only else "results"
        return results_to_csv(tasks), "text/csv; charset=utf-8", f"taskgrid-job-{safe_name}-{suffix}.csv"
    suffix = "failed-tasks" if failed_only else "results"
    return dumps(data), "application/json; charset=utf-8", f"taskgrid-job-{safe_name}-{suffix}.json"


def export_session_results(session_id: str, *, fmt: str = "json", order: str = "input", failed_only: bool = False) -> tuple[str, str, str] | None:
    data = get_session_results(session_id, order=order)
    if not data:
        return None
    tasks = _ordered_result_tasks(data.get("tasks", []), order=order)
    if failed_only:
        tasks = [item for item in tasks if item.get("status") == "failed" or item.get("error")]
    data = dict(data)
    data["tasks"] = tasks
    data["results"] = [
        {"task_id": item["task_id"], "job_id": item.get("job_id"), "input_index": item.get("input_index"), "input_key": item.get("input_key"), "status": item["status"], "result": item.get("result"), "error": item.get("error")}
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
                "SELECT * FROM tasks WHERE job_id=? ORDER BY COALESCE(input_index, 9223372036854775807) ASC, created_at ASC, id ASC LIMIT ?",
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
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.draining, wc.drain_at, wc.drain_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
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
            data["draining"] = bool(int(data.get("draining") or 0))
            data["drain_reason"] = data.get("drain_reason") or ""
            data["drained_instances"] = [item.get("instance_id") for item in list_worker_instance_configs(data["id"]) if int(item.get("draining") or 0)]
            if "instance_count" in metadata:
                try:
                    data["active_concurrency"] = max(0, min(MAX_WORKER_CONCURRENCY, int(metadata.get("instance_count") or 0)))
                except (TypeError, ValueError):
                    pass
            data["active"] = (not data["disabled"]) and (not data["draining"]) and _worker_is_active(row)
            out.append(data)
        return out


class WorkerLogError(RuntimeError):
    """Raised when a worker log server cannot be reached or returns invalid data."""


def get_worker(worker_id: str) -> dict[str, Any] | None:
    init_db()
    with connection() as conn:
        row = conn.execute(
            """
            SELECT w.*, wc.desired_concurrency, wc.disabled, wc.disabled_at, wc.disabled_reason, wc.draining, wc.drain_at, wc.drain_reason, wc.updated_at AS config_updated_at, wc.updated_by AS config_updated_by
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
        data["draining"] = bool(int(data.get("draining") or 0))
        data["drain_reason"] = data.get("drain_reason") or ""
        data["instance_configs"] = list_worker_instance_configs(data["id"])
        data["drained_instances"] = [item.get("instance_id") for item in data["instance_configs"] if int(item.get("draining") or 0)]
        if "instance_count" in metadata:
            try:
                data["active_concurrency"] = max(0, min(MAX_WORKER_CONCURRENCY, int(metadata.get("instance_count") or 0)))
            except (TypeError, ValueError):
                pass
        data["active"] = (not data["disabled"]) and (not data["draining"]) and _worker_is_active(row)
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
