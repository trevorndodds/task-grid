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


def _submit_job(base_url: str, index: int, tasks_per_client: int, priority: int) -> dict[str, Any]:
    payload = {
        "name": f"bench-client-{index}",
        "task_type": "square",
        "session_name": f"Benchmark Client {index}",
        "session_priority": priority,
        "max_retries": 0,
        "tasks": [{"x": index * tasks_per_client + i} for i in range(tasks_per_client)],
    }
    response = requests.post(f"{base_url}/jobs", json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def _poll_jobs(base_url: str, job_ids: list[str], timeout: float) -> list[dict[str, Any]]:
    terminal = {"succeeded", "failed", "cancelled"}
    deadline = time.time() + timeout
    seen: dict[str, dict[str, Any]] = {}
    while time.time() < deadline:
        done = 0
        for job_id in job_ids:
            response = requests.get(f"{base_url}/jobs/{job_id}", timeout=10)
            response.raise_for_status()
            job = response.json()
            seen[job_id] = job
            if job.get("status") in terminal:
                done += 1
        if done == len(job_ids):
            return [seen[job_id] for job_id in job_ids]
        time.sleep(0.25)
    unfinished = [job_id for job_id in job_ids if seen.get(job_id, {}).get("status") not in terminal]
    raise TimeoutError(f"timed out waiting for jobs: {unfinished[:10]}")


def _event_count(db_file: Path, code: str) -> int:
    with sqlite3.connect(db_file) as conn:
        row = conn.execute("SELECT COUNT(*) FROM events WHERE code=?", (code,)).fetchone()
    return int(row[0] or 0)


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a local TaskGrid scale benchmark")
    parser.add_argument("--workers", type=int, default=4, help="worker node processes")
    parser.add_argument("--instances-per-worker", type=int, default=1, help="single-task instances per worker node")
    parser.add_argument("--clients", type=int, default=8, help="concurrent client submissions")
    parser.add_argument("--tasks-per-client", type=int, default=50, help="tasks per submitted job")
    parser.add_argument("--port", type=int, default=8800)
    parser.add_argument("--poll-seconds", type=float, default=0.2)
    parser.add_argument("--heartbeat-seconds", type=float, default=2.0)
    parser.add_argument("--execution-mode", choices=["process", "thread", "inline"], default="process", help="worker task execution mode")
    parser.add_argument("--batch-lease-size", type=int, default=1, help="node-level batch lease size passed to workers")
    parser.add_argument("--long-poll-seconds", type=float, default=0.0, help="long-poll wait seconds passed to workers")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--keep-workdir", action="store_true")
    args = parser.parse_args(argv)

    base_url = f"http://127.0.0.1:{args.port}"
    root_ctx = tempfile.TemporaryDirectory(prefix="taskgrid-bench-")
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

        for i in range(args.workers):
            worker_id = f"bench-node-{i + 1}"
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "taskgrid.worker",
                    "--broker",
                    base_url,
                    "--worker-id",
                    worker_id,
                    "--module",
                    "examples.custom_tasks",
                    "--instances",
                    str(args.instances_per_worker),
                    "--poll-seconds",
                    str(args.poll_seconds),
                    "--heartbeat-seconds",
                    str(args.heartbeat_seconds),
                    "--execution-mode",
                    args.execution_mode,
                    "--broker-pool-size",
                    str(min(64, max(2, args.instances_per_worker))),
                    "--batch-lease-size",
                    str(args.batch_lease_size),
                    "--long-poll-seconds",
                    str(args.long_poll_seconds),
                    "--log-root",
                    str(root / "worker-logs"),
                    "--log-api-port",
                    "-1",
                ],
                env=env,
                text=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            processes.append(proc)

        # Let workers heartbeat and advertise capabilities before strict-ish benchmark submissions.
        time.sleep(max(2.0, args.heartbeat_seconds * 1.5))

        submit_started = time.time()
        job_ids: list[str] = []
        with ThreadPoolExecutor(max_workers=args.clients) as pool:
            futures = [pool.submit(_submit_job, base_url, i + 1, args.tasks_per_client, i % 5) for i in range(args.clients)]
            for future in as_completed(futures):
                job = future.result()
                job_ids.append(job["id"])
        submit_elapsed = time.time() - submit_started

        run_started = time.time()
        jobs = _poll_jobs(base_url, job_ids, timeout=args.timeout)
        elapsed = time.time() - run_started
        total_tasks = args.clients * args.tasks_per_client
        succeeded = sum(int(job.get("completed_tasks") or 0) for job in jobs)
        failed = sum(int(job.get("failed_tasks") or 0) for job in jobs)
        cancelled = sum(int(job.get("cancelled_tasks") or 0) for job in jobs)
        worker_rows = requests.get(f"{base_url}/workers", timeout=10).json()
        recovery = requests.get(f"{base_url}/maintenance/status", timeout=10).json()
        summary = {
            "workers": args.workers,
            "instances_per_worker": args.instances_per_worker,
            "execution_mode": args.execution_mode,
            "batch_lease_size": args.batch_lease_size,
            "long_poll_seconds": args.long_poll_seconds,
            "execution_slots": args.workers * args.instances_per_worker,
            "clients": args.clients,
            "jobs": len(job_ids),
            "tasks": total_tasks,
            "succeeded": succeeded,
            "failed": failed,
            "cancelled": cancelled,
            "submit_seconds": round(submit_elapsed, 3),
            "run_seconds": round(elapsed, 3),
            "throughput_tasks_per_second": round(succeeded / elapsed, 2) if elapsed else None,
            "task_accepted_events": _event_count(root / "taskgrid.db", "TaskAccepted"),
            "task_completed_events": _event_count(root / "taskgrid.db", "TaskCompleted"),
            "workers_registered": len(worker_rows),
            "stale_workers": recovery.get("workers_stale"),
            "expired_running_tasks": recovery.get("expired_running_tasks"),
            "workdir": str(root),
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if succeeded == total_tasks and failed == 0 and cancelled == 0 else 2
    finally:
        _terminate(processes)
        if args.keep_workdir:
            root_ctx.cleanup = lambda: None  # type: ignore[method-assign]
            print(f"kept workdir: {root}", file=sys.stderr)
        else:
            root_ctx.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
