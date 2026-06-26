from __future__ import annotations

import os
import tempfile


def test_job_success_flow():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/test.db"
        from taskgrid import core

        job = core.create_job("test", "echo", [{"n": 1}, {"n": 2}], max_retries=1)
        tasks = core.lease_tasks("w1", limit=2, lease_seconds=30)
        assert len(tasks) == 2
        for task in tasks:
            core.complete_task(task["id"], "w1", {"ok": True})
        final = core.get_job(job["id"])
        assert final is not None
        assert final["status"] == "succeeded"
        assert final["completed_tasks"] == 2


def test_failure_retries_then_fails():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/test.db"
        from taskgrid import core

        job = core.create_job("test", "echo", [{"n": 1}], max_retries=1)
        first = core.lease_tasks("w1", limit=1)[0]
        failed = core.fail_task(first["id"], "w1", "boom")
        assert failed is not None
        assert failed["status"] == "queued"
        second = core.lease_tasks("w1", limit=1)[0]
        failed_again = core.fail_task(second["id"], "w1", "boom again")
        assert failed_again is not None
        assert failed_again["status"] == "failed"
        final = core.get_job(job["id"])
        assert final is not None
        assert final["status"] == "failed"


def test_cancel_job():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/test.db"
        from taskgrid import core

        job = core.create_job("test", "echo", [{"n": 1}, {"n": 2}])
        cancelled = core.cancel_job(job["id"])
        assert cancelled is not None
        assert cancelled["status"] == "cancelled"
        assert all(t["status"] == "cancelled" for t in core.list_tasks(job["id"]))


def test_worker_tags_gate_task_leasing():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/tags.db"
        from taskgrid import core

        job = core.create_job(
            "gpu test",
            "echo",
            [{"n": 1}],
            metadata={"required_tags": ["gpu"]},
        )
        core.heartbeat_worker("plain-worker", metadata={"tags": []})
        assert core.lease_tasks("plain-worker", limit=1) == []

        core.heartbeat_worker("gpu-worker", metadata={"tags": ["gpu", "risk"]})
        leased = core.lease_tasks("gpu-worker", limit=1)
        assert len(leased) == 1
        assert leased[0]["job_id"] == job["id"]


def test_retry_failed_tasks_requeues_job():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/retry.db"
        from taskgrid import core

        job = core.create_job("retry test", "echo", [{"n": 1}], max_retries=0)
        task = core.lease_tasks("w1", limit=1)[0]
        failed = core.fail_task(task["id"], "w1", "boom")
        assert failed is not None
        assert failed["status"] == "failed"
        assert core.get_job(job["id"])["status"] == "failed"

        retried = core.retry_failed_tasks(job["id"])
        assert retried is not None
        assert retried["status"] in {"queued", "running"}
        tasks = core.list_tasks(job["id"])
        assert tasks[0]["status"] == "queued"
        assert tasks[0]["attempts"] == 0


def test_worker_config_round_trip_and_list_workers():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/worker-config.db"
        from taskgrid import core

        core.heartbeat_worker("node-a", metadata={"tags": ["cpu"], "active_concurrency": 2})
        config = core.set_worker_config("node-a", 5, updated_by="test")
        assert config["desired_concurrency"] == 5
        workers = core.list_workers()
        node = next(w for w in workers if w["id"] == "node-a")
        assert node["desired_concurrency"] == 5
        assert node["active_concurrency"] == 2


def test_worker_log_server_lists_and_reads_files():
    with tempfile.TemporaryDirectory() as tmp:
        from urllib import request
        from taskgrid.worker import _setup_logging, _start_log_server

        logger, log_dir = _setup_logging("node-log", f"{tmp}/logs")
        logger.info("hello worker log")
        for handler in logger.handlers:
            handler.flush()
        server, url = _start_log_server(log_dir, "node-log", "127.0.0.1", 0)
        try:
            with request.urlopen(f"{url}/logs", timeout=5) as response:
                listing = response.read().decode("utf-8")
            assert "supervisor.log" in listing
            with request.urlopen(f"{url}/logs/supervisor.log", timeout=5) as response:
                text = response.read().decode("utf-8")
            assert "hello worker log" in text
        finally:
            server.shutdown()
            server.server_close()


def test_task_execution_embeds_task_section_in_worker_log_without_task_file():
    with tempfile.TemporaryDirectory() as tmp:
        from pathlib import Path
        from taskgrid.worker import _execute_task

        node_dir = Path(tmp) / "logs" / "node-embedded"
        log_dir = node_dir / "instances" / "instance-001"
        result = _execute_task("task-123", "echo", {"n": 1}, [], "instance-001", str(log_dir))
        assert result["log_file"] == "instances/instance-001/worker.log"
        assert result["log_section"] == "task-123"
        assert not list(node_dir.rglob("task_*.log"))
        assert not list(node_dir.rglob("*.lock"))
        text = (log_dir / "worker.log").read_text()
        assert "TASK task-123 START" in text
        assert "status=succeeded" in text
        assert '{"payload": {"n": 1}}' in text


def test_manager_events_have_codes_and_jsonl_log():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/manager-events.db"
        os.environ["TASKGRID_MANAGER_LOG"] = f"{tmp}/manager.log"
        from taskgrid import core

        job = core.create_job("coded events", "echo", [{"n": 1}])
        task = core.lease_tasks("node-a:instance-001", limit=1)[0]
        core.complete_task(task["id"], "node-a:instance-001", {"ok": True})

        events = core.list_events(limit=20)
        codes = {event["code"] for event in events}
        assert "JobSubmitted" in codes
        assert "TaskAccepted" in codes
        assert "TaskCompleted" in codes

        task_events = core.list_events(entity_type="task", entity_id=task["id"])
        assert any(event["code"] == "TaskAccepted" for event in task_events)
        assert any(event["code"] == "TaskCompleted" for event in task_events)

        text = core.read_manager_log()
        assert '"code":"TaskAccepted"' in text
        assert '"code":"TaskCompleted"' in text
        assert job["id"] in text

        os.environ.pop("TASKGRID_MANAGER_LOG", None)


def test_client_submit_creates_service_session_and_tracks_counts():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/sessions.db"
        from taskgrid import core

        job = core.create_job("session demo", "echo", [{"n": 1}, {"n": 2}], session_name="session A")
        assert job["session_id"].startswith("sess_")
        session = core.get_session(job["session_id"])
        assert session is not None
        assert session["name"] == "session A"
        assert session["status"] == "queued"
        assert session["total_jobs"] == 1
        assert session["total_tasks"] == 2
        assert session["queued_tasks"] == 2
        assert session["running_tasks"] == 0

        task = core.lease_tasks("node-a:instance-001", limit=1)[0]
        session = core.get_session(job["session_id"])
        assert session["status"] == "running"
        assert session["running_tasks"] == 1
        assert session["queued_tasks"] == 1
        assert session["started_at"] is not None

        core.complete_task(task["id"], "node-a:instance-001", {"ok": True})
        remaining = [t for t in core.list_tasks(job["id"]) if t["status"] == "queued"][0]
        leased = core.lease_tasks("node-a:instance-001", limit=1)[0]
        assert leased["id"] == remaining["id"]
        core.complete_task(leased["id"], "node-a:instance-001", {"ok": True})

        session = core.get_session(job["session_id"])
        assert session["status"] == "succeeded"
        assert session["completed_tasks"] == 2
        assert session["finished_at"] is not None
        assert core.list_session_jobs(job["session_id"])[0]["id"] == job["id"]


def test_job_and_session_results_include_payload_worker_and_result():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/results.db"
        from taskgrid import core

        job = core.create_job("result demo", "echo", [{"n": 1}, {"n": 2}], session_name="Result Session")
        task = core.lease_tasks("node-r:instance-001", limit=1)[0]
        core.complete_task(task["id"], "node-r:instance-001", {"value": 123})

        job_results = core.get_job_results(job["id"])
        assert job_results is not None
        assert job_results["job_id"] == job["id"]
        assert job_results["session_id"] == job["session_id"]
        assert len(job_results["tasks"]) == 2
        completed = [item for item in job_results["tasks"] if item["status"] == "succeeded"][0]
        assert completed["assigned_worker_id"] == "node-r:instance-001"
        assert completed["payload"] == {"n": 1}
        assert completed["result"] == {"value": 123}
        assert completed["payload_bytes"] > 0
        assert completed["result_bytes"] > 0

        session_results = core.get_session_results(job["session_id"])
        assert session_results is not None
        assert session_results["session_id"] == job["session_id"]
        assert session_results["total_tasks"] == 2
        assert len(session_results["jobs"]) == 1
        assert any(item["task_id"] == task["id"] for item in session_results["tasks"])


def test_session_priority_controls_queued_task_leasing_and_can_be_updated():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/session-priority.db"
        from taskgrid import core

        low = core.create_job(
            "low priority session",
            "echo",
            [{"name": "low-1"}, {"name": "low-2"}],
            session_name="Low",
            session_priority=1,
        )
        high = core.create_job(
            "high priority session",
            "echo",
            [{"name": "high-1"}, {"name": "high-2"}],
            session_name="High",
            session_priority=9,
        )

        first = core.lease_tasks("priority-worker", limit=1)[0]
        assert first["job_id"] == high["id"]
        core.complete_task(first["id"], "priority-worker", {"ok": True})

        updated = core.set_session_priority(low["session_id"], 20, updated_by="test")
        assert updated is not None
        assert updated["priority"] == 20

        second = core.lease_tasks("priority-worker", limit=1)[0]
        assert second["job_id"] == low["id"]
        assert second["priority"] == 20

        events = core.list_events(entity_type="service_session", entity_id=low["session_id"], code="ServiceSessionPriorityUpdated")
        assert events
        assert events[0]["data"]["queued_tasks_reprioritized"] >= 1


def test_task_catalog_reports_worker_capabilities_and_strict_submit_rejects_unknown_task():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/catalog.db"
        from taskgrid import core

        core.heartbeat_worker(
            "risk-node",
            metadata={
                "tags": ["risk", "cpu"],
                "task_types": ["price_risk", "square"],
                "service_name": "risk-worker",
                "service_version": "1.2.0",
                "active_concurrency": 2,
            },
        )
        catalog = core.get_task_catalog(task_type="price_risk", required_tags=["risk"])
        assert not catalog["warnings"]
        assert catalog["capable_workers"][0]["id"] == "risk-node"
        assert any(item["task_type"] == "price_risk" for item in catalog["task_types"])

        queued = core.create_job("warn only", "missing_task", [{"n": 1}])
        assert queued["status"] == "queued"
        warnings = core.list_events(entity_type="job", entity_id=queued["id"], code="TaskCapabilityWarning")
        assert warnings

        try:
            core.create_job("strict missing", "missing_task", [{"n": 1}], require_capable_worker=True)
        except core.CapabilityError as exc:
            assert "missing_task" in " ".join(exc.details["warnings"])
        else:  # pragma: no cover
            raise AssertionError("strict submit should reject unknown task capability")
