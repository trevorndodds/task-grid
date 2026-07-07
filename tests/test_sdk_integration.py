from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib import request

from taskgrid.sdk import TaskGridClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _json_request(method: str, base_url: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = request.Request(
        f"{base_url}{path}",
        data=data,
        headers={"Content-Type": "application/json"} if payload is not None else {},
        method=method,
    )
    with request.urlopen(req, timeout=10) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else None


@contextmanager
def _live_manager():
    with tempfile.TemporaryDirectory() as tmp:
        port = _free_port()
        env = os.environ.copy()
        env["TASKGRID_DB"] = str(Path(tmp) / "taskgrid-live.db")
        env["PYTHONPATH"] = str(PROJECT_ROOT)
        for name in ("TASKGRID_ADMIN_TOKEN", "TASKGRID_CLIENT_TOKEN", "TASKGRID_WORKER_TOKEN", "TASKGRID_UI_TOKEN"):
            env.pop(name, None)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "taskgrid.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        base_url = f"http://127.0.0.1:{port}"
        deadline = time.time() + 10
        last_error: Exception | None = None
        try:
            while time.time() < deadline:
                if proc.poll() is not None:
                    stdout, stderr = proc.communicate(timeout=1)
                    raise RuntimeError(f"manager exited early\nstdout={stdout}\nstderr={stderr}")
                try:
                    _json_request("GET", base_url, "/health")
                    break
                except Exception as exc:  # pragma: no cover - diagnostic path
                    last_error = exc
                    time.sleep(0.1)
            else:  # pragma: no cover - diagnostic path
                raise RuntimeError(f"manager did not start: {last_error!r}")
            yield base_url
        finally:
            proc.terminate()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:  # pragma: no cover - defensive cleanup
                proc.kill()
                proc.communicate(timeout=5)


def test_live_sdk_manager_flow_covers_recent_client_and_manager_features():
    with _live_manager() as base_url:
        client = TaskGridClient(base_url)
        key = client.new_idempotency_key("integration")
        job = client.submit(
            name="integration indexed job",
            task_type="echo",
            tasks=[{"value": 1}, {"value": 2}, {"value": 3}],
            session_name="Integration Session",
            input_keys=["row-a", "row-b", "row-c"],
            client_id="integration-client",
            idempotency_key=key,
        )
        assert job["client_id"] == "integration-client"
        assert job["resume_token"].startswith("rt_")
        replay = client.submit(
            name="integration indexed job retry after lost response",
            task_type="echo",
            tasks=[{"value": 999}],
            client_id="integration-client",
            idempotency_key=key,
        )
        assert replay["id"] == job["id"]
        assert client.tasks(job["id"])[0]["input_key"] == "row-a"

        # Reconnect-token APIs are distinct from scheduling pause/resume controls.
        reconnecting = TaskGridClient(base_url, client_id=job["client_id"], resume_token=job["resume_token"])
        owned = reconnecting.my_sessions()
        assert [item["id"] for item in owned] == [job["session_id"]]
        resumed_owned = reconnecting.reconnect_session(job["session_id"])
        assert resumed_owned["id"] == job["session_id"]
        assert reconnecting.resume_owned_session(job["session_id"])["id"] == job["session_id"]

        paused_session = reconnecting.pause_session(job["session_id"], reason="integration hold")
        assert paused_session["paused"] == 1
        assert _json_request("POST", base_url, "/tasks/lease", {"worker_id": "node-a", "limit": 3}) == []
        resumed_session = reconnecting.resume_session(job["session_id"])
        assert resumed_session["paused"] == 0
        assert reconnecting.set_session_priority(job["session_id"], 7)["priority"] == 7

        _json_request(
            "POST",
            base_url,
            "/workers/heartbeat",
            {
                "worker_id": "node-a",
                "hostname": "integration-host",
                "metadata": {"tags": ["cpu"], "task_types": ["echo"], "active_concurrency": 3, "service_name": "integration-service", "service_version": "2026.06"},
            },
        )
        services = client.services()
        integration_service = next(item for item in services["services"] if item["service_name"] == "integration-service")
        assert integration_service["service_version"] == "2026.06"
        assert integration_service["task_types"] == ["echo"]
        assert integration_service["workers_total"] == 1
        assert client.services(service_name="integration-service")["services_total"] == 1

        assert client.set_worker_instances("node-a", 3)["desired_concurrency"] == 3
        assert client.disable_worker("node-a", reason="integration disable")["disabled"] == 1
        assert _json_request("POST", base_url, "/tasks/lease", {"worker_id": "node-a", "limit": 1}) == []
        assert client.enable_worker("node-a", reason="integration enable")["disabled"] == 0
        assert client.drain_worker("node-a", reason="integration drain")["draining"] == 1
        assert _json_request("POST", base_url, "/tasks/lease", {"worker_id": "node-a", "limit": 1}) == []
        assert client.undrain_worker("node-a", reason="integration resume")["draining"] == 0
        assert client.drain_worker_instance("node-a", "instance-002", reason="slot maintenance")["draining"] == 1
        one_slot = _json_request(
            "POST",
            base_url,
            "/tasks/lease-batch",
            {"worker_id": "node-a", "instance_ids": ["instance-002"]},
        )
        assert one_slot == []
        assert client.undrain_worker_instance("node-a", "instance-002")["draining"] == 0

        leased = _json_request(
            "POST",
            base_url,
            "/tasks/lease-batch",
            {"worker_id": "node-a", "instance_ids": ["instance-001", "instance-002", "instance-003"]},
        )
        assert [task["input_key"] for task in leased] == ["row-a", "row-b", "row-c"]
        assert [task["input_index"] for task in leased] == [0, 1, 2]

        assignments = reconnecting.session_assignments(job["session_id"])
        assert assignments["running_assignments"] == 3
        assert assignments["assigned_workers"] == 1
        assert {item["instance_id"] for item in assignments["current_assignments"]} == {"instance-001", "instance-002", "instance-003"}
        assert assignments["workers"][0]["service_name"] == "integration-service"

        # Complete out of order; result APIs and exports should stay in input order.
        for task in reversed(leased):
            instance_id = task["assigned_worker_id"].removeprefix("node-a-")
            _json_request(
                "POST",
                base_url,
                f"/tasks/{task['id']}/complete",
                {"worker_id": "node-a", "instance_id": instance_id, "result": {"seen": task["input_key"]}},
            )

        final = client.wait(job["id"], poll_seconds=0.05, timeout_seconds=5)
        assert final["status"] == "succeeded"
        results = reconnecting.results(job["id"], order="input")
        assert [item["input_key"] for item in results["tasks"]] == ["row-a", "row-b", "row-c"]
        assert [item["result"]["seen"] for item in results["tasks"]] == ["row-a", "row-b", "row-c"]
        history = reconnecting.session_task_history(job["session_id"])
        assert [item["input_key"] for item in history["tasks"][:3]] == ["row-a", "row-b", "row-c"]
        assert {item["instance_id"] for item in history["tasks"][:3]} == {"instance-001", "instance-002", "instance-003"}
        assert reconnecting.session_task_history(job["session_id"], status="succeeded")["status_counts"]["succeeded"] == 3

        csv_text = reconnecting.export_results(job["id"], format="csv").decode("utf-8")
        assert "input_index,input_key" in csv_text
        assert "0,row-a" in csv_text
        assert "2,row-c" in csv_text

        stream_events = list(reconnecting.stream_results(job["id"], poll_seconds=0.1, timeout_seconds=2, replay=True))
        assert [event["_event"] for event in stream_events].count("result") == 3
        assert stream_events[-1]["_event"] == "done"

        session_events = list(reconnecting.stream_session_results(job["session_id"], poll_seconds=0.1, timeout_seconds=2, replay=True))
        assert any(event["_event"] == "result" for event in session_events)
        assert session_events[-1]["_event"] == "done"

        retry_job = reconnecting.submit(
            name="integration retry job",
            task_type="echo",
            tasks=[{"input_key": "retry-row"}],
            session_id=job["session_id"],
            max_retries=0,
        )
        assert reconnecting.pause_job(retry_job["id"], reason="integration job hold")["paused"] == 1
        assert _json_request("POST", base_url, "/tasks/lease", {"worker_id": "node-a", "limit": 1}) == []
        assert reconnecting.resume_job(retry_job["id"])["paused"] == 0
        retry_task = _json_request("POST", base_url, "/tasks/lease", {"worker_id": "node-a", "limit": 1})[0]
        _json_request(
            "POST",
            base_url,
            f"/tasks/{retry_task['id']}/fail",
            {"worker_id": "node-a", "error": "integration failure"},
        )
        assert reconnecting.get_job(retry_job["id"])["status"] == "failed"
        assert reconnecting.retry_failed(retry_job["id"])["status"] in {"queued", "running"}
        assert reconnecting.tasks(retry_job["id"])[0]["input_key"] == "retry-row"

        status = reconnecting.recovery_status()
        assert "running_tasks" in status
        reconcile = reconnecting.reconcile_manager(recover_expired=True)
        assert "jobs_reconciled" in reconcile
        retention = reconnecting.retention_preview(completed_days=0, failed_days=0, event_days=0)
        assert "completed_or_cancelled_sessions" in retention
        assert "old_events" in retention
