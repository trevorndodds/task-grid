from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter


def _wait_for_health(base_url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            response = requests.get(f"{base_url}/health", timeout=2)
            if response.ok:
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(0.2)
    raise RuntimeError(f"manager did not become healthy: {last_error}")


def _terminate(processes: list[subprocess.Popen[str]]) -> None:
    for proc in reversed(processes):
        if proc.poll() is None:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception:  # noqa: BLE001
                pass
    deadline = time.time() + 8
    for proc in reversed(processes):
        while proc.poll() is None and time.time() < deadline:
            time.sleep(0.1)
        if proc.poll() is None:
            proc.terminate()
    for proc in reversed(processes):
        if proc.poll() is None:
            proc.kill()


def _event_count(db_file: Path, code: str) -> int:
    with sqlite3.connect(db_file) as conn:
        row = conn.execute("SELECT COUNT(*) FROM events WHERE code=?", (code,)).fetchone()
    return int(row[0] or 0)


def _submit_job(base_url: str, index: int, tasks_per_client: int, priority: int) -> dict[str, Any]:
    payload = {
        "name": f"connection-bench-client-{index}",
        "task_type": "square",
        "session_name": f"Connection Benchmark Client {index}",
        "session_priority": priority,
        "max_retries": 0,
        "tasks": [{"x": index * tasks_per_client + i} for i in range(tasks_per_client)],
    }
    response = requests.post(f"{base_url}/jobs", json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


class Counters:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.lease_requests = 0
        self.empty_leases = 0
        self.tasks_completed = 0
        self.errors = 0
        self.http_errors: dict[str, int] = {}

    def inc(self, name: str, count: int = 1) -> None:
        with self.lock:
            setattr(self, name, int(getattr(self, name)) + count)

    def http_error(self, key: str) -> None:
        with self.lock:
            self.errors += 1
            self.http_errors[key] = self.http_errors.get(key, 0) + 1

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "lease_requests": self.lease_requests,
                "empty_leases": self.empty_leases,
                "tasks_completed": self.tasks_completed,
                "errors": self.errors,
                "http_errors": dict(self.http_errors),
            }


def _make_session(pool_connections: int, pool_maxsize: int) -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize, pool_block=True, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _heartbeat_loop(
    *,
    base_url: str,
    worker_id: str,
    instances_per_worker: int,
    heartbeat_seconds: float,
    stop_event: threading.Event,
    timeout: float,
) -> None:
    session = _make_session(1, 1)
    metadata = {
        "tags": ["bench"],
        "modules": ["simulated.manager.connection.benchmark"],
        "task_types": ["square"],
        "supported_task_types": ["square"],
        "service_name": "connection-bench-worker",
        "service_version": "simulated",
        "execution_model": "simulated_single_task_instance_loops",
        "instance_count": instances_per_worker,
        "configured_instances": instances_per_worker,
        "active_concurrency": instances_per_worker,
        "configured_concurrency": instances_per_worker,
    }
    while not stop_event.is_set():
        try:
            response = session.post(
                f"{base_url}/workers/heartbeat",
                json={
                    "worker_id": worker_id,
                    "hostname": "connection-bench",
                    "status": "idle",
                    "version": "bench",
                    "metadata": metadata,
                },
                timeout=timeout,
            )
            response.raise_for_status()
        except Exception:
            # The benchmark counts instance request errors; heartbeat errors are
            # intentionally non-fatal so shutdown races do not hide task results.
            pass
        stop_event.wait(heartbeat_seconds)
    session.close()


def _instance_loop(
    *,
    base_url: str,
    worker_id: str,
    instance_id: str,
    counters: Counters,
    target_tasks: int,
    stop_event: threading.Event,
    poll_seconds: float,
    lease_seconds: int,
    timeout: float,
    shared_session: requests.Session | None,
) -> None:
    session = shared_session or _make_session(1, 1)
    try:
        while not stop_event.is_set():
            if counters.snapshot()["tasks_completed"] >= target_tasks:
                return
            try:
                counters.inc("lease_requests")
                lease_response = session.post(
                    f"{base_url}/tasks/lease",
                    json={"worker_id": worker_id, "instance_id": instance_id, "limit": 1, "lease_seconds": lease_seconds},
                    timeout=timeout,
                )
                lease_response.raise_for_status()
                tasks = lease_response.json()
                if not tasks:
                    counters.inc("empty_leases")
                    stop_event.wait(poll_seconds)
                    continue
                task = tasks[0]
                complete_response = session.post(
                    f"{base_url}/tasks/{task['id']}/complete",
                    json={
                        "worker_id": worker_id,
                        "instance_id": instance_id,
                        "result": {"ok": True, "simulated": True, "input": task.get("payload")},
                    },
                    timeout=timeout,
                )
                complete_response.raise_for_status()
                counters.inc("tasks_completed")
            except requests.HTTPError as exc:
                status = getattr(exc.response, "status_code", "http")
                counters.http_error(str(status))
                stop_event.wait(min(1.0, poll_seconds * 2))
            except Exception as exc:  # noqa: BLE001
                counters.http_error(type(exc).__name__)
                stop_event.wait(min(1.0, poll_seconds * 2))
    finally:
        if shared_session is None:
            session.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stress TaskGrid manager connections without worker executor overhead")
    parser.add_argument("--workers", type=int, default=4, help="simulated worker nodes")
    parser.add_argument("--instances-per-worker", type=int, default=100, help="simulated single-task instances per worker")
    parser.add_argument("--clients", type=int, default=8, help="concurrent client submissions")
    parser.add_argument("--tasks-per-client", type=int, default=100, help="tasks per submitted job")
    parser.add_argument("--port", type=int, default=8900)
    parser.add_argument("--poll-seconds", type=float, default=0.05)
    parser.add_argument("--heartbeat-seconds", type=float, default=2.0)
    parser.add_argument("--lease-seconds", type=int, default=60)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--request-timeout", type=float, default=15.0)
    parser.add_argument("--shared-session-per-worker", action="store_true", help="use one pooled Session per worker node instead of one Session per instance")
    parser.add_argument("--keep-workdir", action="store_true")
    args = parser.parse_args(argv)

    base_url = f"http://127.0.0.1:{args.port}"
    root_ctx = tempfile.TemporaryDirectory(prefix="taskgrid-manager-conn-bench-")
    root = Path(root_ctx.name)
    env = os.environ.copy()
    env.update(
        {
            "TASKGRID_DB": str(root / "taskgrid.db"),
            "TASKGRID_MANAGER_LOG": str(root / "manager.log"),
            "TASKGRID_SQLITE_SYNCHRONOUS": env.get("TASKGRID_SQLITE_SYNCHRONOUS", "NORMAL"),
        }
    )
    processes: list[subprocess.Popen[str]] = []
    stop_event = threading.Event()
    try:
        manager = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "taskgrid.app:app", "--host", "127.0.0.1", "--port", str(args.port), "--log-level", "warning"],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        processes.append(manager)
        _wait_for_health(base_url)

        heartbeat_threads: list[threading.Thread] = []
        for worker_index in range(args.workers):
            worker_id = f"conn-node-{worker_index + 1}"
            thread = threading.Thread(
                target=_heartbeat_loop,
                kwargs={
                    "base_url": base_url,
                    "worker_id": worker_id,
                    "instances_per_worker": args.instances_per_worker,
                    "heartbeat_seconds": args.heartbeat_seconds,
                    "stop_event": stop_event,
                    "timeout": args.request_timeout,
                },
                daemon=True,
                name=f"heartbeat-{worker_id}",
            )
            thread.start()
            heartbeat_threads.append(thread)
        time.sleep(max(1.0, args.heartbeat_seconds * 1.2))

        total_tasks = args.clients * args.tasks_per_client
        submit_started = time.time()
        job_ids: list[str] = []
        with ThreadPoolExecutor(max_workers=args.clients) as pool:
            futures = [_submit_job(base_url, i + 1, args.tasks_per_client, i % 5) for i in range(args.clients)] if False else []
            futures = [pool.submit(_submit_job, base_url, i + 1, args.tasks_per_client, i % 5) for i in range(args.clients)]
            for future in as_completed(futures):
                job = future.result()
                job_ids.append(job["id"])
        submit_seconds = time.time() - submit_started

        counters = Counters()
        shared_sessions: dict[str, requests.Session] = {}
        if args.shared_session_per_worker:
            for worker_index in range(args.workers):
                worker_id = f"conn-node-{worker_index + 1}"
                shared_sessions[worker_id] = _make_session(args.instances_per_worker, args.instances_per_worker)

        started = time.time()
        instance_threads: list[threading.Thread] = []
        for worker_index in range(args.workers):
            worker_id = f"conn-node-{worker_index + 1}"
            for instance_index in range(args.instances_per_worker):
                instance_id = f"instance-{instance_index + 1:03d}"
                thread = threading.Thread(
                    target=_instance_loop,
                    kwargs={
                        "base_url": base_url,
                        "worker_id": worker_id,
                        "instance_id": instance_id,
                        "counters": counters,
                        "target_tasks": total_tasks,
                        "stop_event": stop_event,
                        "poll_seconds": args.poll_seconds,
                        "lease_seconds": args.lease_seconds,
                        "timeout": args.request_timeout,
                        "shared_session": shared_sessions.get(worker_id),
                    },
                    daemon=True,
                    name=f"{worker_id}-{instance_id}",
                )
                thread.start()
                instance_threads.append(thread)

        deadline = started + args.timeout
        while time.time() < deadline:
            if counters.snapshot()["tasks_completed"] >= total_tasks:
                break
            time.sleep(0.2)
        stop_event.set()
        for thread in instance_threads:
            thread.join(timeout=3)
        for thread in heartbeat_threads:
            thread.join(timeout=2)
        for session in shared_sessions.values():
            session.close()

        elapsed = time.time() - started
        snapshot = counters.snapshot()
        workers = requests.get(f"{base_url}/workers", timeout=10).json()
        recovery = requests.get(f"{base_url}/maintenance/status", timeout=10).json()
        summary = {
            "workers": args.workers,
            "instances_per_worker": args.instances_per_worker,
            "simulated_instances": args.workers * args.instances_per_worker,
            "clients": args.clients,
            "jobs": len(job_ids),
            "tasks": total_tasks,
            "completed_by_simulator": snapshot["tasks_completed"],
            "submit_seconds": round(submit_seconds, 3),
            "run_seconds": round(elapsed, 3),
            "throughput_tasks_per_second": round(snapshot["tasks_completed"] / elapsed, 2) if elapsed else None,
            "lease_requests": snapshot["lease_requests"],
            "lease_requests_per_second": round(snapshot["lease_requests"] / elapsed, 2) if elapsed else None,
            "empty_leases": snapshot["empty_leases"],
            "errors": snapshot["errors"],
            "http_errors": snapshot["http_errors"],
            "task_accepted_events": _event_count(root / "taskgrid.db", "TaskAccepted"),
            "task_completed_events": _event_count(root / "taskgrid.db", "TaskCompleted"),
            "task_result_ignored_events": _event_count(root / "taskgrid.db", "TaskResultIgnored"),
            "workers_registered": len(workers),
            "stale_workers": recovery.get("workers_stale"),
            "expired_running_tasks": recovery.get("expired_running_tasks"),
            "shared_session_per_worker": args.shared_session_per_worker,
            "workdir": str(root),
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if snapshot["tasks_completed"] == total_tasks and snapshot["errors"] == 0 else 2
    finally:
        stop_event.set()
        _terminate(processes)
        if args.keep_workdir:
            root_ctx.cleanup = lambda: None  # type: ignore[method-assign]
            print(f"kept workdir: {root}", file=sys.stderr)
        else:
            root_ctx.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
