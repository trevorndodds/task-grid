from __future__ import annotations

import argparse
import contextlib
import io
import json
import logging
import os
import random
import queue
from collections import deque
import socket
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

import requests
from requests.adapters import HTTPAdapter

from .tasks import list_task_types, load_modules, run_task

VERSION = "0.29.0"
MAX_INSTANCES = 128
MAX_LOG_TAIL_BYTES = 1_000_000
MAX_EMBEDDED_TASK_OUTPUT_BYTES = 250_000


def _clamp_instance_count(value: int) -> int:
    return max(1, min(MAX_INSTANCES, int(value)))


def _clamp_instance_target(value: int) -> int:
    return max(0, min(MAX_INSTANCES, int(value)))


# Backwards-compatible name for older imports/tests/docs. The value now means
# logical single-task instances managed by this node, not multi-task concurrency
# inside one shared worker instance.
def _clamp_concurrency(value: int) -> int:
    return _clamp_instance_count(value)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_filename(value: str) -> str:
    text = str(value or "log")
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in text)[:180] or "log"


def _safe_log_relative_path(value: str) -> str | None:
    text = parse.unquote(str(value or "")).strip().replace("\\", "/")
    if not text or text.startswith("/"):
        return None
    parts = [part for part in text.split("/") if part]
    if not parts:
        return None
    for part in parts:
        if part in {".", ".."} or part.startswith(".") or _safe_filename(part) != part:
            return None
    return "/".join(parts)


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _text_response(handler: BaseHTTPRequestHandler, status: int, text: str) -> None:
    data = text.encode("utf-8", errors="replace")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/plain; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _file_modified_at(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(timespec="seconds")


def _list_log_files(log_dir: Path) -> list[dict[str, Any]]:
    if not log_dir.exists():
        return []
    files: list[dict[str, Any]] = []
    for path in log_dir.rglob("*"):
        if not path.is_file() or path.name.startswith("."):
            continue
        if path.name.endswith(".lock"):
            continue
        relative_name = path.relative_to(log_dir).as_posix()
        stat = path.stat()
        files.append(
            {
                "name": relative_name,
                "size_bytes": stat.st_size,
                "modified_at": _file_modified_at(path),
            }
        )
    files.sort(key=lambda item: str(item["modified_at"]), reverse=True)
    return files


def _read_tail(path: Path, tail_bytes: int) -> str:
    tail_bytes = max(1, min(MAX_LOG_TAIL_BYTES, int(tail_bytes)))
    size = path.stat().st_size
    with path.open("rb") as fh:
        if size > tail_bytes:
            fh.seek(size - tail_bytes)
            prefix = f"... showing last {tail_bytes} bytes of {size} bytes ...\n"
        else:
            prefix = ""
        return prefix + fh.read().decode("utf-8", errors="replace")


def _truncate_task_output(text: str) -> str:
    data = text.encode("utf-8", errors="replace")
    if len(data) <= MAX_EMBEDDED_TASK_OUTPUT_BYTES:
        return text
    tail = data[-MAX_EMBEDDED_TASK_OUTPUT_BYTES:].decode("utf-8", errors="replace")
    return f"... task output truncated; showing last {MAX_EMBEDDED_TASK_OUTPUT_BYTES} bytes ...\n{tail}"


def _append_log(log_file: Path, section: str) -> None:
    # No file lock: each logical instance is single-task only, so only one child
    # task writes to that instance's worker.log at a time.
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(section)
        if not section.endswith("\n"):
            fh.write("\n")


class _WorkerLogHandler(BaseHTTPRequestHandler):
    log_dir: Path
    worker_id: str

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - inherited name
        # Do not spam stderr for every proxied log read. The supervisor logger records startup/shutdown.
        return

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        parsed = parse.urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse.parse_qs(parsed.query)
        try:
            if path == "/health":
                _json_response(self, 200, {"ok": True, "worker_id": self.worker_id})
                return
            if path == "/logs":
                _json_response(
                    self,
                    200,
                    {
                        "worker_id": self.worker_id,
                        "log_dir": str(self.log_dir),
                        "files": _list_log_files(self.log_dir),
                    },
                )
                return
            if path.startswith("/logs/"):
                raw_name = path.removeprefix("/logs/")
                relative_name = _safe_log_relative_path(raw_name)
                if not relative_name:
                    _text_response(self, 400, "invalid log filename")
                    return
                log_file = self.log_dir / relative_name
                try:
                    resolved = log_file.resolve()
                    resolved.relative_to(self.log_dir.resolve())
                except ValueError:
                    _text_response(self, 400, "invalid log filename")
                    return
                if not resolved.exists() or not resolved.is_file():
                    _text_response(self, 404, "log file not found")
                    return
                tail_raw = (query.get("tail") or [str(MAX_LOG_TAIL_BYTES)])[0]
                try:
                    tail = int(tail_raw)
                except ValueError:
                    tail = MAX_LOG_TAIL_BYTES
                _text_response(self, 200, _read_tail(resolved, tail))
                return
            _json_response(self, 404, {"detail": "not found"})
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            _json_response(self, 500, {"detail": str(exc)})


def _start_log_server(log_dir: Path, worker_id: str, host: str, port: int) -> tuple[ThreadingHTTPServer, str]:
    handler = type(
        "WorkerLogHandler",
        (_WorkerLogHandler,),
        {"log_dir": log_dir, "worker_id": worker_id},
    )
    server = ThreadingHTTPServer((host, port), handler)
    actual_host, actual_port = server.server_address[:2]
    advertised_host = actual_host
    if advertised_host in {"0.0.0.0", "::", ""}:
        advertised_host = socket.gethostname()
    public_url = f"http://{advertised_host}:{actual_port}"
    thread = threading.Thread(target=server.serve_forever, name=f"{worker_id}-log-api", daemon=True)
    thread.start()
    return server, public_url


def _setup_logging(worker_id: str, log_root: str) -> tuple[logging.Logger, Path]:
    node_log_dir = Path(log_root).expanduser().resolve() / _safe_filename(worker_id)
    node_log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"taskgrid.worker.{worker_id}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    file_handler = logging.FileHandler(node_log_dir / "supervisor.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger, node_log_dir


def _instance_id(index: int) -> str:
    return f"instance-{index:03d}"


def _instance_log_dir(node_log_dir: Path, instance_id: str) -> Path:
    return node_log_dir / "instances" / _safe_filename(instance_id)


class BrokerClient:
    """Bounded shared broker client for a worker node.

    Logical instances share a small pool of HTTP sessions. This avoids creating
    one socket pool per instance, but it also avoids serializing every lease,
    completion, and heartbeat call behind a single global lock.
    """

    def __init__(self, base_url: str, *, timeout_seconds: float = 30.0, pool_size: int = 8, token: str | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.pool_size = max(1, pool_size)
        self.token = (token or "").strip() or None
        self._closed = False
        self._sessions: queue.LifoQueue[requests.Session] = queue.LifoQueue(maxsize=self.pool_size)
        for _ in range(self.pool_size):
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, pool_block=True)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._sessions.put(session)

    def post_json(self, path: str, payload: dict[str, Any]) -> Any:
        if self._closed:
            raise RuntimeError("broker client is closed")
        session = self._sessions.get(block=True)
        try:
            headers = {"X-TaskGrid-Token": self.token} if self.token else None
            response = session.post(f"{self.base_url}{path}", json=payload, headers=headers, timeout=self.timeout_seconds)
            response.raise_for_status()
            if not response.content:
                return None
            return response.json()
        finally:
            self._sessions.put(session)

    def close(self) -> None:
        self._closed = True
        while True:
            try:
                session = self._sessions.get_nowait()
            except queue.Empty:
                break
            session.close()


def post_json(base_url: str, path: str, payload: dict[str, Any]) -> Any:
    # Backwards-compatible helper for tests/older imports. The worker runtime uses BrokerClient.
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def heartbeat(base_url: str, worker_id: str, status: str, current_task_id: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "worker_id": worker_id,
        "hostname": socket.gethostname(),
        "status": status,
        "current_task_id": current_task_id,
        "version": VERSION,
    }
    if metadata is not None:
        payload["metadata"] = metadata
    return post_json(base_url, "/workers/heartbeat", payload)


def lease(base_url: str, worker_id: str, limit: int, lease_seconds: int) -> list[dict[str, Any]]:
    return post_json(base_url, "/tasks/lease", {"worker_id": worker_id, "limit": limit, "lease_seconds": lease_seconds})


def complete(base_url: str, task_id: str, worker_id: str, result: Any) -> None:
    post_json(base_url, f"/tasks/{task_id}/complete", {"worker_id": worker_id, "instance_id": state.instance_id, "result": result})


def fail(base_url: str, task_id: str, worker_id: str, error: str) -> None:
    post_json(base_url, f"/tasks/{task_id}/fail", {"worker_id": worker_id, "instance_id": state.instance_id, "error": error})


def _execute_task(
    task_id: str,
    task_type: str,
    payload: dict[str, Any],
    modules: list[str],
    instance_id: str,
    instance_log_dir: str,
) -> dict[str, Any]:
    # Child processes need task modules too, especially on spawn-based platforms.
    # Each logical instance runs one task at a time, so its worker.log is simple append-only.
    log_path = Path(instance_log_dir)
    log_file = log_path / "worker.log"
    started = time.time()
    captured = io.StringIO()
    try:
        with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
            load_modules(modules)
            result = run_task(task_type, payload)
        duration = round(time.time() - started, 4)
        output = _truncate_task_output(captured.getvalue())
        section = "\n".join(
            [
                "",
                f"===== TASK {task_id} START {_utc_now()} instance={instance_id} type={task_type} status=succeeded duration_seconds={duration} =====",
                json.dumps({"payload": payload}, ensure_ascii=False, default=str),
                "----- stdout/stderr -----",
                output.rstrip() or "(no output)",
                f"===== TASK {task_id} END {_utc_now()} instance={instance_id} status=succeeded =====",
                "",
            ]
        )
        _append_log(log_file, section)
        return {
            "value": result,
            "duration_seconds": duration,
            "log_file": f"instances/{instance_id}/worker.log",
            "log_instance": instance_id,
            "log_section": task_id,
        }
    except Exception:
        duration = round(time.time() - started, 4)
        output = _truncate_task_output(captured.getvalue())
        section = "\n".join(
            [
                "",
                f"===== TASK {task_id} START {_utc_now()} instance={instance_id} type={task_type} status=failed duration_seconds={duration} =====",
                json.dumps({"payload": payload}, ensure_ascii=False, default=str),
                "----- stdout/stderr -----",
                output.rstrip() or "(no output)",
                "----- traceback -----",
                traceback.format_exc().rstrip(),
                f"===== TASK {task_id} END {_utc_now()} instance={instance_id} status=failed =====",
                "",
            ]
        )
        _append_log(log_file, section)
        raise


@dataclass
class InstanceState:
    index: int
    instance_id: str
    log_dir: Path
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    current_task_id: str | None = None
    status: str = "idle"
    tasks_completed: int = 0
    last_poll_at: str | None = None
    last_error: str | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "instance_id": self.instance_id,
                "status": self.status,
                "current_task_id": self.current_task_id,
                "tasks_completed": self.tasks_completed,
                "last_poll_at": self.last_poll_at,
                "last_error": self.last_error,
            }

    def set_status(self, status: str, current_task_id: str | None = None, last_error: str | None = None) -> None:
        with self._lock:
            self.status = status
            self.current_task_id = current_task_id
            self.last_error = last_error

    def mark_poll(self) -> None:
        with self._lock:
            self.last_poll_at = _utc_now()

    def mark_completed(self) -> None:
        with self._lock:
            self.tasks_completed += 1


def _make_metadata(
    *,
    tags: list[str],
    modules: list[str],
    task_types: list[str],
    service_name: str | None,
    service_version: str | None,
    instances: dict[str, InstanceState],
    configured_instances: int,
    log_dir: Path,
    log_url: str | None,
    pending_instances: int | None = None,
    poll_seconds: float | None = None,
    broker_pool_size: int | None = None,
    execution_mode: str | None = None,
    batch_lease_size: int | None = None,
    long_poll_seconds: float | None = None,
) -> dict[str, Any]:
    snapshots = [state.snapshot() for state in sorted(instances.values(), key=lambda item: item.index)]
    active_instances = [item["instance_id"] for item in snapshots if item.get("current_task_id")]
    running_tasks = len(active_instances)
    metadata: dict[str, Any] = {
        "tags": tags,
        "modules": modules,
        "task_types": task_types,
        "supported_task_types": task_types,
        "service_name": service_name or (modules[0] if modules else "taskgrid-default"),
        "service_version": service_version or VERSION,
        "execution_model": "independent_single_task_instance_loops",
        "instance_count": configured_instances,
        "configured_instances": configured_instances,
        "active_instances": active_instances,
        "instances": snapshots,
        "running_tasks": running_tasks,
        "log_dir": str(log_dir),
        # Backwards-compatible UI/core fields. These now mean managed instances.
        "active_concurrency": configured_instances,
        "configured_concurrency": configured_instances,
    }
    if poll_seconds is not None:
        metadata["poll_seconds"] = poll_seconds
    if broker_pool_size is not None:
        metadata["broker_pool_size"] = broker_pool_size
    if execution_mode is not None:
        metadata["execution_mode"] = execution_mode
    if batch_lease_size is not None:
        metadata["batch_lease_size"] = batch_lease_size
    if long_poll_seconds is not None:
        metadata["long_poll_seconds"] = long_poll_seconds
    if log_url:
        metadata["log_url"] = log_url
    if pending_instances is not None and pending_instances != configured_instances:
        metadata["pending_instances"] = pending_instances
        metadata["pending_concurrency"] = pending_instances
    return metadata


def _response_disabled(response: dict[str, Any]) -> bool:
    try:
        return bool(int(response.get("disabled") or 0))
    except (TypeError, ValueError):
        return str(response.get("disabled") or "").lower() in {"true", "yes", "disabled"}


def _desired_from_heartbeat(response: dict[str, Any], fallback: int) -> int:
    if _response_disabled(response):
        return 0
    raw = response.get("desired_concurrency")
    if raw is None:
        raw = (response.get("metadata") or {}).get("desired_concurrency")
    try:
        return _clamp_instance_count(int(raw))
    except (TypeError, ValueError):
        return fallback


def _node_status(instances: dict[str, InstanceState]) -> str:
    snapshots = [state.snapshot() for state in instances.values()]
    if any(item.get("status") == "running" for item in snapshots):
        return "running"
    if any(item.get("status") == "starting" for item in snapshots):
        return "starting"
    if any(item.get("status") == "stopping" for item in snapshots):
        return "stopping"
    return "idle"


def _current_task_id(instances: dict[str, InstanceState]) -> str | None:
    for state in sorted(instances.values(), key=lambda item: item.index):
        current = state.snapshot().get("current_task_id")
        if current:
            return str(current)
    return None


def _active_instance_ids(instances: dict[str, InstanceState]) -> list[str]:
    return [
        item["instance_id"]
        for item in (state.snapshot() for state in sorted(instances.values(), key=lambda value: value.index))
        if item.get("current_task_id")
    ]


def _instance_from_assigned(worker_id: str, assigned: str | None) -> str | None:
    text = str(assigned or "")
    prefix = f"{worker_id}-"
    if text.startswith(prefix):
        return text[len(prefix) :]
    return None


class LeaseCoordinator:
    """Optional node-level batch lease coordinator.

    Instances remain single-task loops. When batch leasing is enabled, the first
    idle instance to ask for work leases tasks for several currently idle
    instances in one manager request, then each instance consumes only its own
    pre-assigned task. This reduces manager connection pressure while preserving
    precise worker-instance assignment in task rows.
    """

    def __init__(
        self,
        *,
        broker: BrokerClient,
        worker_id: str,
        lease_seconds: int,
        batch_lease_size: int,
        long_poll_seconds: float,
        states: dict[str, "InstanceState"],
        logger: logging.Logger,
    ) -> None:
        self.broker = broker
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.batch_lease_size = max(1, int(batch_lease_size))
        self.long_poll_seconds = max(0.0, float(long_poll_seconds or 0.0))
        self.states = states
        self.logger = logger
        self._lock = threading.Lock()
        self._queues: dict[str, deque[dict[str, Any]]] = {}

    def _pop_locked(self, instance_id: str) -> dict[str, Any] | None:
        q = self._queues.get(instance_id)
        if q:
            try:
                return q.popleft()
            except IndexError:
                return None
        return None

    def acquire(self, state: "InstanceState") -> list[dict[str, Any]]:
        if self.batch_lease_size <= 1:
            return self.broker.post_json(
                "/tasks/lease",
                {
                    "worker_id": self.worker_id,
                    "instance_id": state.instance_id,
                    "limit": 1,
                    "lease_seconds": self.lease_seconds,
                    "wait_seconds": self.long_poll_seconds,
                },
            ) or []

        with self._lock:
            existing = self._pop_locked(state.instance_id)
            if existing:
                return [existing]

            idle_ids: list[str] = []
            for item in sorted(self.states.values(), key=lambda value: value.index):
                snap = item.snapshot()
                if snap.get("current_task_id"):
                    continue
                if snap.get("status") in {"stopping", "stopped"}:
                    continue
                if self._queues.get(item.instance_id):
                    continue
                idle_ids.append(item.instance_id)
                if len(idle_ids) >= self.batch_lease_size:
                    break
            if state.instance_id not in idle_ids:
                idle_ids.insert(0, state.instance_id)
            idle_ids = idle_ids[: self.batch_lease_size]
            if not idle_ids:
                return []

            leased = self.broker.post_json(
                "/tasks/lease-batch",
                {
                    "worker_id": self.worker_id,
                    "instance_ids": idle_ids,
                    "lease_seconds": self.lease_seconds,
                    "wait_seconds": self.long_poll_seconds,
                },
            ) or []
            for task in leased:
                instance_id = task.get("leased_instance_id") or _instance_from_assigned(self.worker_id, task.get("assigned_worker_id"))
                if not instance_id:
                    instance_id = state.instance_id
                self._queues.setdefault(str(instance_id), deque()).append(task)
            found = self._pop_locked(state.instance_id)
            return [found] if found else []


def _sleep_with_stop(stop_event: threading.Event, seconds: float) -> bool:
    return stop_event.wait(max(0.01, seconds))


def _instance_loop(
    *,
    state: InstanceState,
    broker: BrokerClient,
    worker_id: str,
    lease_seconds: int,
    poll_seconds: float,
    lease_coordinator: LeaseCoordinator | None,
    modules: list[str],
    logger: logging.Logger,
    execution_mode: str,
) -> None:
    # One execution slot per logical instance. The default process mode isolates
    # CPU-heavy tasks. Thread/inline modes are useful for trusted tiny or I/O-heavy
    # tasks where process startup/scheduling overhead dominates throughput.
    if execution_mode == "process":
        executor: ProcessPoolExecutor | ThreadPoolExecutor | None = ProcessPoolExecutor(max_workers=1)
    elif execution_mode == "thread":
        executor = ThreadPoolExecutor(max_workers=1)
    else:
        executor = None
    backoff = poll_seconds
    idle_jitter = random.uniform(0.0, max(0.05, poll_seconds))
    if _sleep_with_stop(state.stop_event, idle_jitter):
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        return
    try:
        state.set_status("idle")
        while not state.stop_event.is_set():
            try:
                state.mark_poll()
                if lease_coordinator is not None:
                    leased = lease_coordinator.acquire(state)
                else:
                    leased = broker.post_json(
                        "/tasks/lease",
                        {"worker_id": worker_id, "instance_id": state.instance_id, "limit": 1, "lease_seconds": lease_seconds},
                    ) or []
                if not leased:
                    state.set_status("idle", None, None)
                    # Idle polling backs off a little and has jitter so a 50-instance
                    # node does not send synchronized lease requests every second.
                    sleep_for = min(max(backoff, poll_seconds), max(5.0, poll_seconds)) + random.uniform(0.0, min(0.5, poll_seconds))
                    backoff = min(sleep_for * 1.25, max(5.0, poll_seconds))
                    if _sleep_with_stop(state.stop_event, sleep_for):
                        break
                    continue

                backoff = poll_seconds
                task = leased[0]
                task_id = task["id"]
                task_type = task["task_type"]
                payload = task.get("payload", {})
                state.set_status("running", task_id, None)
                logger.info("%s %s leased executor=%s-%s", task_id, task_type, worker_id, state.instance_id)

                try:
                    if executor is None:
                        result = _execute_task(
                            task_id,
                            task_type,
                            payload,
                            modules,
                            state.instance_id,
                            str(state.log_dir),
                        )
                    else:
                        future = executor.submit(
                            _execute_task,
                            task_id,
                            task_type,
                            payload,
                            modules,
                            state.instance_id,
                            str(state.log_dir),
                        )
                        result = future.result()
                    broker.post_json(f"/tasks/{task_id}/complete", {"worker_id": worker_id, "instance_id": state.instance_id, "result": result})
                    state.mark_completed()
                    logger.info(
                        "%s %s succeeded in %ss executor=%s-%s log=%s",
                        task_id,
                        task_type,
                        result.get("duration_seconds"),
                        worker_id,
                        state.instance_id,
                        result.get("log_file"),
                    )
                except Exception:
                    error = "".join(traceback.format_exception(*sys.exc_info()))
                    try:
                        broker.post_json(f"/tasks/{task_id}/fail", {"worker_id": worker_id, "instance_id": state.instance_id, "error": error})
                    except Exception as report_exc:  # noqa: BLE001 - defensive reporting boundary
                        logger.warning("failed to report task failure for %s: %s", task_id, report_exc)
                    state.set_status("idle", None, error.splitlines()[-1] if error else "task failed")
                    logger.exception("%s %s failed executor=%s-%s", task_id, task_type, worker_id, state.instance_id)
                finally:
                    state.set_status("idle", None, None)

            except requests.RequestException as exc:
                state.set_status("idle", None, str(exc))
                logger.warning("broker unavailable for %s: %s", state.instance_id, exc)
                if _sleep_with_stop(state.stop_event, max(poll_seconds, 2.0) + random.uniform(0.0, 1.0)):
                    break
            except Exception as exc:  # pragma: no cover - defensive instance loop boundary
                state.set_status("idle", None, str(exc))
                logger.exception("instance loop error instance=%s", state.instance_id)
                if _sleep_with_stop(state.stop_event, max(poll_seconds, 2.0)):
                    break
    finally:
        state.set_status("stopping", None, None)
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)
        state.set_status("stopped", None, None)
        logger.info("instance stopped instance=%s", state.instance_id)

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TaskGrid worker node supervisor")
    parser.add_argument("--broker", default="http://127.0.0.1:8000", help="TaskGrid API URL")
    parser.add_argument("--worker-token", default=os.environ.get("TASKGRID_WORKER_TOKEN"), help="Optional worker token sent to the manager")
    parser.add_argument("--worker-id", default=f"worker_{socket.gethostname()}_{uuid.uuid4().hex[:6]}")
    parser.add_argument("--poll-seconds", type=float, default=1.0, help="Base idle polling delay per instance. Idle instances back off with jitter.")
    parser.add_argument("--heartbeat-seconds", type=float, default=2.0, help="How often the node supervisor heartbeats to the broker")
    parser.add_argument("--lease-seconds", type=int, default=120)
    parser.add_argument("--instances", type=int, default=None, help="Number of independent single-task worker instances managed by this node")
    parser.add_argument("--concurrency", type=int, default=None, help="Deprecated alias for --instances")
    parser.add_argument("--limit", type=int, default=None, help="Deprecated alias for --instances when --instances/--concurrency are omitted")
    parser.add_argument("--broker-pool-size", type=int, default=4, help="Max pooled HTTP connections from this node to the broker")
    parser.add_argument("--batch-lease-size", type=int, default=int(os.environ.get("TASKGRID_BATCH_LEASE_SIZE") or "1"), help="Optional node-level lease batch size. 1 keeps per-instance leasing; >1 leases for multiple idle instances per request.")
    parser.add_argument("--long-poll-seconds", type=float, default=float(os.environ.get("TASKGRID_LONG_POLL_SECONDS") or "0"), help="Optional manager long-poll wait for idle lease requests. 0 disables long polling.")
    parser.add_argument(
        "--execution-mode",
        choices=["process", "thread", "inline"],
        default=os.environ.get("TASKGRID_EXECUTION_MODE", "process"),
        help="How each single-task instance runs task code. process isolates CPU work; thread/inline reduce overhead for tiny trusted tasks.",
    )
    parser.add_argument("--module", action="append", default=[], help="Import a Python module that registers @task functions")
    parser.add_argument("--tag", action="append", default=[], help="Worker capability tag, e.g. gpu or risk-model-v2. Can be repeated.")
    parser.add_argument("--service-name", default=os.environ.get("TASKGRID_SERVICE_NAME"), help="Optional service/application name advertised to the manager")
    parser.add_argument("--service-version", default=os.environ.get("TASKGRID_SERVICE_VERSION"), help="Optional service/application version advertised to the manager")
    parser.add_argument("--log-root", default=os.environ.get("TASKGRID_LOG_ROOT") or "./taskgrid_logs", help="Root folder for per-node/per-instance logs")
    parser.add_argument("--log-api-host", default=os.environ.get("TASKGRID_LOG_HOST") or "127.0.0.1", help="Host/interface for the worker log API")
    parser.add_argument("--log-api-port", type=int, default=int(os.environ.get("TASKGRID_LOG_PORT") or "0"), help="Port for worker log API. Use 0 for a free port, -1 to disable.")
    parser.add_argument("--log-public-url", default=os.environ.get("TASKGRID_LOG_PUBLIC_URL"), help="URL the broker should use to reach this worker's log API")
    args = parser.parse_args(argv)

    logger, node_log_dir = _setup_logging(args.worker_id, args.log_root)
    log_server: ThreadingHTTPServer | None = None
    log_url: str | None = None
    if args.log_api_port >= 0:
        log_server, generated_url = _start_log_server(node_log_dir, args.worker_id, args.log_api_host, args.log_api_port)
        log_url = args.log_public_url or generated_url

    load_modules(args.module)
    task_types = list_task_types()
    tags = sorted({tag.strip().lower() for tag in args.tag if tag.strip()})
    initial_instances = args.instances if args.instances is not None else (args.concurrency if args.concurrency is not None else (args.limit if args.limit is not None else 1))
    desired_instances = _clamp_instance_count(initial_instances)
    pending_instances: int | None = None
    instances: dict[str, InstanceState] = {}
    broker = BrokerClient(args.broker, pool_size=max(1, min(64, int(args.broker_pool_size))), token=args.worker_token)
    lease_coordinator = LeaseCoordinator(
        broker=broker,
        worker_id=args.worker_id,
        lease_seconds=args.lease_seconds,
        batch_lease_size=max(1, int(args.batch_lease_size)),
        long_poll_seconds=max(0.0, float(args.long_poll_seconds)),
        states=instances,
        logger=logger,
    )

    logger.info(
        "TaskGrid worker node %s polling %s tags=%s task_types=%s instances=%s poll_seconds=%s broker_pool_size=%s batch_lease_size=%s long_poll_seconds=%s execution_mode=%s log_dir=%s log_url=%s",
        args.worker_id,
        args.broker,
        tags,
        task_types,
        desired_instances,
        args.poll_seconds,
        args.broker_pool_size,
        args.batch_lease_size,
        args.long_poll_seconds,
        args.execution_mode,
        node_log_dir,
        log_url or "disabled",
    )

    def start_instance(index: int) -> None:
        instance_id = _instance_id(index)
        if instance_id in instances:
            return
        state = InstanceState(index=index, instance_id=instance_id, log_dir=_instance_log_dir(node_log_dir, instance_id))
        state.log_dir.mkdir(parents=True, exist_ok=True)
        thread = threading.Thread(
            target=_instance_loop,
            kwargs={
                "state": state,
                "broker": broker,
                "worker_id": args.worker_id,
                "lease_seconds": args.lease_seconds,
                "poll_seconds": max(0.1, float(args.poll_seconds)),
                "lease_coordinator": lease_coordinator,
                "modules": args.module,
                "logger": logger,
                "execution_mode": args.execution_mode,
            },
            name=f"{args.worker_id}-{instance_id}",
            daemon=True,
        )
        state.thread = thread
        instances[instance_id] = state
        state.set_status("starting")
        thread.start()
        logger.info("instance started instance=%s", instance_id)

    def request_stop_instance(instance_id: str) -> None:
        state = instances.get(instance_id)
        if not state:
            return
        if not state.stop_event.is_set():
            state.stop_event.set()
            snapshot = state.snapshot()
            state.set_status("stopping", snapshot.get("current_task_id"))
            logger.info("instance stop requested instance=%s", instance_id)

    def reconcile_instances(target: int) -> None:
        target = _clamp_instance_target(target)
        # Add missing instances immediately.
        for index in range(1, target + 1):
            start_instance(index)
        # Stop extra instances. They finish any current task first and then exit.
        for state in list(instances.values()):
            if state.index > target:
                request_stop_instance(state.instance_id)
        # Remove fully stopped instance records so they no longer count as active capacity.
        for instance_id, state in list(instances.items()):
            if state.thread and not state.thread.is_alive() and state.snapshot().get("status") == "stopped":
                state.thread.join(timeout=0.1)
                instances.pop(instance_id, None)

    def send_heartbeat(status: str) -> dict[str, Any]:
        metadata = _make_metadata(
            tags=tags,
            modules=args.module,
            task_types=task_types,
            service_name=args.service_name,
            service_version=args.service_version,
            instances=instances,
            configured_instances=len(instances),
            pending_instances=pending_instances,
            log_dir=node_log_dir,
            log_url=log_url,
            poll_seconds=max(0.1, float(args.poll_seconds)),
            broker_pool_size=max(1, min(64, int(args.broker_pool_size))),
            execution_mode=args.execution_mode,
            batch_lease_size=max(1, int(args.batch_lease_size)),
            long_poll_seconds=max(0.0, float(args.long_poll_seconds)),
        )
        payload = {
            "worker_id": args.worker_id,
            "hostname": socket.gethostname(),
            "status": status,
            "current_task_id": _current_task_id(instances),
            "version": VERSION,
            "metadata": metadata,
        }
        return broker.post_json("/workers/heartbeat", payload)

    try:
        reconcile_instances(desired_instances)
        response = send_heartbeat(_node_status(instances))
        broker_desired = _desired_from_heartbeat(response, desired_instances)
        if broker_desired != desired_instances:
            desired_instances = broker_desired
            pending_instances = broker_desired

        while True:
            try:
                reconcile_instances(desired_instances)
                if len(instances) == desired_instances:
                    pending_instances = None

                response = send_heartbeat(_node_status(instances))
                broker_desired = _desired_from_heartbeat(response, desired_instances)
                if broker_desired != desired_instances:
                    old = desired_instances
                    desired_instances = broker_desired
                    pending_instances = broker_desired
                    logger.info("worker node %s desired instances changed %s -> %s", args.worker_id, old, desired_instances)

                time.sleep(max(0.5, float(args.heartbeat_seconds)))
            except requests.RequestException as exc:
                logger.warning("broker unavailable: %s", exc)
                time.sleep(max(float(args.heartbeat_seconds), 2.0) + random.uniform(0.0, 1.0))
    except KeyboardInterrupt:
        try:
            for state in list(instances.values()):
                state.stop_event.set()
            for state in list(instances.values()):
                if state.thread:
                    state.thread.join(timeout=5)
            try:
                payload = {
                    "worker_id": args.worker_id,
                    "hostname": socket.gethostname(),
                    "status": "stopped",
                    "current_task_id": None,
                    "version": VERSION,
                    "metadata": _make_metadata(
                        tags=tags,
                        modules=args.module,
                        task_types=task_types,
                        service_name=args.service_name,
                        service_version=args.service_version,
                        instances=instances,
                        configured_instances=0,
                        log_dir=node_log_dir,
                        log_url=log_url,
                        poll_seconds=max(0.1, float(args.poll_seconds)),
                        broker_pool_size=max(1, min(64, int(args.broker_pool_size))),
                        execution_mode=args.execution_mode,
                        batch_lease_size=max(1, int(args.batch_lease_size)),
                        long_poll_seconds=max(0.0, float(args.long_poll_seconds)),
                    ),
                }
                broker.post_json("/workers/heartbeat", payload)
            except Exception:
                logger.warning("could not send stopped heartbeat", exc_info=True)
        finally:
            broker.close()
            if log_server:
                log_server.shutdown()
                log_server.server_close()
            logger.info("worker node stopped")
        return 0

if __name__ == "__main__":
    raise SystemExit(main())
