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


def test_task_input_index_and_key_make_results_deterministic():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/input-order.db"
        from taskgrid import core

        job = core.create_job(
            "indexed",
            "echo",
            [{"n": 1}, {"n": 2}, {"n": 3}],
            input_keys=["row-a", "row-b", "row-c"],
        )
        tasks = core.list_tasks(job["id"])
        assert [task["input_index"] for task in tasks] == [0, 1, 2]
        assert [task["input_key"] for task in tasks] == ["row-a", "row-b", "row-c"]

        leased = core.lease_tasks("node-order", limit=3)
        # Finish out of input order; normal result reads should still come back
        # in the client-submitted input order.
        for task in reversed(leased):
            core.complete_task(task["id"], "node-order", {"seen": task["input_key"]})

        results = core.get_job_results(job["id"])
        assert [item["input_index"] for item in results["tasks"]] == [0, 1, 2]
        assert [item["input_key"] for item in results["tasks"]] == ["row-a", "row-b", "row-c"]
        assert [item["result"]["seen"] for item in results["tasks"]] == ["row-a", "row-b", "row-c"]


def test_input_key_can_be_derived_from_payload_and_survives_retry():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/input-key-retry.db"
        from taskgrid import core

        job = core.create_job("derived keys", "echo", [{"id": "invoice-1"}], max_retries=0)
        task = core.lease_tasks("node-key", limit=1)[0]
        assert task["input_index"] == 0
        assert task["input_key"] == "invoice-1"

        failed = core.fail_task(task["id"], "node-key", "boom")
        assert failed["status"] == "failed"
        retried = core.retry_task(task["id"])
        assert retried["status"] == "queued"
        assert retried["input_index"] == 0
        assert retried["input_key"] == "invoice-1"


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


def test_manager_events_have_codes_and_jsonl_log_in_debug_mode():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/manager-events.db"
        os.environ["TASKGRID_MANAGER_LOG"] = f"{tmp}/manager.log"
        os.environ["TASKGRID_EVENT_MODE"] = "debug"
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

        os.environ.pop("TASKGRID_EVENT_MODE", None)
        os.environ.pop("TASKGRID_MANAGER_LOG", None)


def test_normal_mode_suppresses_noisy_success_task_events_but_tracks_state():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/normal-events.db"
        os.environ["TASKGRID_MANAGER_LOG"] = f"{tmp}/manager.log"
        os.environ.pop("TASKGRID_EVENT_MODE", None)
        os.environ.pop("TASKGRID_VERBOSE_TASK_EVENTS", None)
        from taskgrid import core

        job = core.create_job("normal events", "echo", [{"n": 1}, {"n": 2}])
        task = core.lease_tasks("node-a", limit=1, instance_id="instance-001")[0]
        core.complete_task(task["id"], "node-a", {"ok": True}, instance_id="instance-001")

        codes = {event["code"] for event in core.list_events(limit=50)}
        assert "JobSubmitted" in codes
        assert "TaskAccepted" not in codes
        assert "TaskCompleted" not in codes

        stored = core.get_task(task["id"])
        assert stored["status"] == "succeeded"
        assert stored["assigned_worker_id"] == "node-a-instance-001"
        assert stored["started_at"]
        assert stored["finished_at"]
        assert core.get_job(job["id"])["completed_tasks"] == 1

        text = core.read_manager_log()
        assert '"code":"JobSubmitted"' in text
        assert '"code":"TaskAccepted"' not in text
        assert '"code":"TaskCompleted"' not in text

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


def test_client_resume_token_lists_owned_sessions_and_blocks_bad_token():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/client-resume.db"
        from taskgrid import core

        job = core.create_job(
            "owned session",
            "echo",
            [{"n": 1}],
            session_name="Owned Session",
            client_id="client-alpha",
        )
        assert job["client_id"] == "client-alpha"
        assert job["resume_token"].startswith("rt_")

        sessions = core.list_client_sessions("client-alpha", job["resume_token"])
        assert len(sessions) == 1
        assert sessions[0]["id"] == job["session_id"]

        resumed = core.get_client_session("client-alpha", job["resume_token"], job["session_id"])
        assert resumed is not None
        assert resumed["name"] == "Owned Session"

        assert core.list_client_sessions("client-alpha", "wrong-token") == []
        assert core.get_client_session("client-alpha", "wrong-token", job["session_id"]) is None

        try:
            core.create_job(
                "bad attach",
                "echo",
                [{"n": 2}],
                session_id=job["session_id"],
                resume_token="wrong-token",
            )
        except core.CapabilityError as exc:
            assert "resume token" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("mismatched resume token should not attach")


def test_expired_lease_recovery_requeues_and_ignores_late_result():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/recovery.db"
        from taskgrid import core

        job = core.create_job("recover lease", "echo", [{"n": 1}], max_retries=1)
        task = core.lease_tasks("node-a", limit=1, lease_seconds=30, instance_id="instance-001")[0]
        assert task["assigned_worker_id"] == "node-a-instance-001"

        with core.connection() as conn:
            conn.execute(
                "UPDATE tasks SET lease_expires_at=? WHERE id=?",
                ("2000-01-01T00:00:00+00:00", task["id"]),
            )

        status = core.recovery_status()
        assert status["expired_running_tasks"] == 1
        assert status["running_tasks"] == 1

        recovered = core.recover_expired_leases(updated_by="test")
        assert recovered["expired"] == 1
        assert recovered["requeued"] == 1
        recovered_task = core.get_task(task["id"])
        assert recovered_task is not None
        assert recovered_task["status"] == "queued"
        assert recovered_task["assigned_worker_id"] is None

        # A late result from the original executor must not revive or complete the task.
        late = core.complete_task(task["id"], "node-a", {"too_late": True}, instance_id="instance-001")
        assert late is not None
        assert late["status"] == "queued"
        ignored = core.list_events(entity_type="task", entity_id=task["id"], code="TaskResultIgnored")
        assert ignored
        assert ignored[0]["data"]["executor_id"] == "node-a-instance-001"


def test_stale_worker_is_reported_in_recovery_status():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/stale-worker.db"
        from taskgrid import core

        core.heartbeat_worker("stale-node", metadata={"tags": ["cpu"], "task_types": ["echo"]})
        with core.connection() as conn:
            conn.execute(
                "UPDATE workers SET last_heartbeat_at=? WHERE id=?",
                ("2000-01-01T00:00:00+00:00", "stale-node"),
            )
        status = core.recovery_status(active_seconds=60)
        assert status["workers_stale"] == 1
        assert status["stale_workers"][0]["id"] == "stale-node"


def test_purge_offline_workers_removes_stale_registry_rows_and_keeps_active():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/purge-workers.db"
        from taskgrid import core

        core.heartbeat_worker("active-node", metadata={"tags": ["cpu"], "task_types": ["echo"]})
        core.heartbeat_worker("stale-node", metadata={"tags": ["old"], "task_types": ["echo"]})
        core.set_worker_config("stale-node", 3, updated_by="test")
        with core.connection() as conn:
            conn.execute(
                "UPDATE workers SET last_heartbeat_at=? WHERE id=?",
                ("2000-01-01T00:00:00+00:00", "stale-node"),
            )

        summary = core.purge_offline_workers(active_seconds=60, updated_by="test")
        assert summary["purged_count"] == 1
        assert summary["purged_workers"][0]["id"] == "stale-node"
        assert {worker["id"] for worker in core.list_workers()} == {"active-node"}
        with core.connection() as conn:
            assert conn.execute("SELECT COUNT(*) AS count FROM worker_configs WHERE worker_id=?", ("stale-node",)).fetchone()["count"] == 0
        events = core.list_events(code="WorkerPurgedOffline", limit=10)
        assert events and events[0]["entity_id"] == "stale-node"


def test_purge_offline_workers_skips_running_assignments_by_default():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/purge-running-workers.db"
        from taskgrid import core

        job = core.create_job("running on stale", "echo", [{"n": 1}])
        task = core.lease_tasks("stale-node", limit=1, instance_id="instance-001")[0]
        assert task["assigned_worker_id"] == "stale-node-instance-001"
        with core.connection() as conn:
            conn.execute(
                "UPDATE workers SET last_heartbeat_at=? WHERE id=?",
                ("2000-01-01T00:00:00+00:00", "stale-node"),
            )

        summary = core.purge_offline_workers(active_seconds=60, updated_by="test")
        assert summary["purged_count"] == 0
        assert summary["skipped_count"] == 1
        assert summary["skipped_workers"][0]["running_assignments"] == 1
        assert core.get_worker("stale-node") is not None
        assert core.get_task(task["id"])["status"] == "running"

        forced = core.purge_offline_workers(active_seconds=60, include_running=True, updated_by="test")
        assert forced["purged_count"] == 1
        assert core.get_worker("stale-node") is None
        assert core.get_task(task["id"])["status"] == "running"
        assert core.get_job(job["id"])["status"] == "running"


def test_batch_lease_assigns_distinct_instances_and_completes():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/batch.db"
        from taskgrid import core

        job = core.create_job("batch", "echo", [{"id": f"row-{i}", "n": i} for i in range(4)])
        core.heartbeat_worker("node-a", metadata={"task_types": ["echo"], "configured_instances": 4})
        leased = core.lease_tasks_for_instances("node-a", ["instance-001", "instance-002", "instance-003"], lease_seconds=30)
        assert len(leased) == 3
        assert {item["assigned_worker_id"] for item in leased} == {
            "node-a-instance-001",
            "node-a-instance-002",
            "node-a-instance-003",
        }
        assert [item["input_index"] for item in leased] == [0, 1, 2]
        assert [item["input_key"] for item in leased] == ["row-0", "row-1", "row-2"]
        for item in leased:
            core.complete_task(item["id"], "node-a", {"ok": item["leased_instance_id"]}, instance_id=item["leased_instance_id"])
        assert core.get_job(job["id"])["completed_tasks"] == 3


def test_result_exports_json_csv_and_failed_only():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/exports.db"
        from taskgrid import core

        job = core.create_job("exports", "echo", [{"n": 1}, {"n": 2}], max_retries=0)
        first, second = core.lease_tasks("node-a", limit=2)
        core.complete_task(first["id"], "node-a", {"ok": True})
        core.fail_task(second["id"], "node-a", "boom")

        body, media, name = core.export_job_results(job["id"], fmt="json")
        assert media.startswith("application/json")
        assert name.endswith("results.json")
        assert '"tasks"' in body

        csv_body, csv_media, csv_name = core.export_job_results(job["id"], fmt="csv")
        assert csv_media.startswith("text/csv")
        assert csv_body.startswith("input_index,input_key,task_id,job_id")
        assert csv_name.endswith("results.csv")

        failed_csv, _, failed_name = core.export_job_results(job["id"], fmt="csv", failed_only=True)
        assert "boom" in failed_csv
        assert first["id"] not in failed_csv
        assert failed_name.endswith("failed-tasks.csv")


def test_retention_preview_and_cleanup_terminal_sessions_only():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/retention.db"
        from taskgrid import core

        old = core.create_job("old", "echo", [{"n": 1}])
        task = core.lease_tasks("node-a", limit=1)[0]
        core.complete_task(task["id"], "node-a", {"ok": True})
        live = core.create_job("live", "echo", [{"n": 2}])

        with core.connection() as conn:
            conn.execute("UPDATE service_sessions SET finished_at='2000-01-01T00:00:00.000+00:00' WHERE id=?", (old["session_id"],))
            conn.execute("UPDATE jobs SET finished_at='2000-01-01T00:00:00.000+00:00' WHERE id=?", (old["id"],))

        preview = core.retention_preview(completed_days=1, failed_days=1, event_days=3650)
        assert preview["completed_or_cancelled_sessions"] >= 1
        summary = core.apply_retention_cleanup(completed_days=1, failed_days=1, event_days=3650)
        assert summary["deleted_sessions"] >= 1
        assert core.get_session(old["session_id"]) is None
        assert core.get_session(live["session_id"]) is not None


def test_graceful_cancel_leaves_running_task_to_finish():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/graceful-cancel.db"
        from taskgrid import core

        job = core.create_job("cancel demo", "echo", [{"n": 1}, {"n": 2}])
        task = core.lease_tasks("node-c", limit=1, instance_id="instance-001")[0]
        cancelled = core.cancel_job(job["id"], mode="graceful")
        assert cancelled is not None
        assert cancelled["status"] == "cancelling"
        statuses = {item["status"] for item in core.list_tasks(job["id"])}
        assert statuses == {"running", "cancelled"}

        completed = core.complete_task(task["id"], "node-c", {"ok": True}, instance_id="instance-001")
        assert completed is not None
        assert completed["status"] == "succeeded"
        final = core.get_job(job["id"])
        assert final is not None
        assert final["status"] == "cancelled"
        assert final["completed_tasks"] == 1
        assert final["cancelled_tasks"] == 1


def test_force_cancel_marks_running_task_cancelled():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/force-cancel.db"
        from taskgrid import core

        job = core.create_job("force cancel demo", "echo", [{"n": 1}, {"n": 2}])
        core.lease_tasks("node-c", limit=1, instance_id="instance-001")
        cancelled = core.cancel_job(job["id"], mode="force")
        assert cancelled is not None
        assert cancelled["status"] == "cancelled"
        assert all(item["status"] == "cancelled" for item in core.list_tasks(job["id"]))


def test_disabled_worker_does_not_receive_new_leases_and_can_be_reenabled():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/disabled-worker.db"
        from taskgrid import core

        core.heartbeat_worker("node-disable", metadata={"tags": ["cpu"], "task_types": ["echo"], "instance_count": 1})
        job = core.create_job("disable demo", "echo", [{"n": 1}, {"n": 2}])

        config = core.set_worker_enabled("node-disable", enabled=False, reason="maintenance", updated_by="test")
        assert bool(config["disabled"])
        assert core.lease_tasks("node-disable", limit=1, instance_id="instance-001") == []

        worker = core.list_workers()[0]
        assert worker["disabled"] is True
        assert worker["disabled_reason"] == "maintenance"

        config = core.set_worker_enabled("node-disable", enabled=True, updated_by="test")
        assert not bool(config["disabled"])
        leased = core.lease_tasks("node-disable", limit=1, instance_id="instance-001")
        assert len(leased) == 1
        assert leased[0]["job_id"] == job["id"]


def test_bulk_worker_disable_prevents_batch_leases_for_selected_nodes():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/bulk-disabled-worker.db"
        from taskgrid import core

        core.heartbeat_worker("node-a", metadata={"task_types": ["echo"], "instance_count": 2})
        core.heartbeat_worker("node-b", metadata={"task_types": ["echo"], "instance_count": 2})
        core.create_job("bulk disable", "echo", [{"n": i} for i in range(4)])

        summary = core.set_workers_enabled(["node-a", "node-b"], enabled=False, reason="pause", updated_by="test")
        assert summary["count"] == 2
        assert core.lease_tasks_for_instances("node-a", ["instance-001", "instance-002"]) == []
        assert core.lease_tasks_for_instances("node-b", ["instance-001", "instance-002"]) == []

        core.set_worker_enabled("node-b", enabled=True, updated_by="test")
        leased = core.lease_tasks_for_instances("node-b", ["instance-001", "instance-002"])
        assert len(leased) == 2
        assert all(task["assigned_worker_id"].startswith("node-b-instance-") for task in leased)


def test_disabled_worker_is_not_capable_for_strict_submit():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/disabled-capability.db"
        from taskgrid import core

        core.heartbeat_worker("node-cap", metadata={"task_types": ["echo"], "tags": ["cpu"], "instance_count": 1})
        assert core.get_task_catalog(task_type="echo")["capable_workers"]
        core.set_worker_enabled("node-cap", enabled=False, reason="pause", updated_by="test")
        catalog = core.get_task_catalog(task_type="echo")
        assert catalog["supporting_workers"]
        assert not catalog["capable_workers"]
        assert catalog["workers"][0]["disabled"] is True
        try:
            core.create_job("strict", "echo", [{"n": 1}], require_capable_worker=True)
        except core.CapabilityError as exc:
            assert "no active capable worker" in str(exc)
        else:
            raise AssertionError("strict submit should fail when only supporting worker is disabled")

def test_paused_session_blocks_new_leases_until_resumed():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/pause-session.db"
        from taskgrid import core

        job = core.create_job("pause session", "echo", [{"n": 1}, {"n": 2}])
        paused = core.set_session_paused(job["session_id"], True, reason="maintenance", updated_by="test")
        assert paused is not None
        assert int(paused["paused"]) == 1
        assert core.lease_tasks("node-a", limit=1) == []
        assert all(task["status"] == "queued" for task in core.list_tasks(job["id"]))

        resumed = core.set_session_paused(job["session_id"], False, updated_by="test")
        assert resumed is not None
        assert int(resumed["paused"]) == 0
        leased = core.lease_tasks("node-a", limit=1)
        assert len(leased) == 1
        assert leased[0]["job_id"] == job["id"]


def test_paused_job_blocks_leasing_without_pausing_whole_session():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/pause-job.db"
        from taskgrid import core

        first = core.create_job("paused job", "echo", [{"n": 1}], session_name="pause mix")
        second = core.create_job("active job", "echo", [{"n": 2}], session_id=first["session_id"], resume_token=first["resume_token"])
        paused = core.set_job_paused(first["id"], True, reason="hold", updated_by="test")
        assert paused is not None
        assert int(paused["paused"]) == 1

        leased = core.lease_tasks("node-a", limit=2)
        assert len(leased) == 1
        assert leased[0]["job_id"] == second["id"]

        core.set_job_paused(first["id"], False, updated_by="test")
        leased_again = core.lease_tasks("node-a", limit=1)
        assert len(leased_again) == 1
        assert leased_again[0]["job_id"] == first["id"]


def test_job_result_updates_stream_cursor_returns_each_terminal_task_once():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/stream-cursor.db"
        from taskgrid import core

        job = core.create_job("stream cursor", "echo", [{"id": "a"}, {"id": "b"}], max_retries=0)
        first, second = core.lease_tasks("node-stream", limit=2)
        core.complete_task(first["id"], "node-stream", {"ok": "a"})

        update = core.get_job_result_updates(job["id"])
        assert update is not None
        assert update["terminal"] is False
        assert [task["task_id"] for task in update["tasks"]] == [first["id"]]
        assert update["tasks"][0]["input_key"] == "a"
        cursor = update["cursor"]

        assert core.get_job_result_updates(job["id"], after_updated_at=cursor["updated_at"], after_task_id=cursor["task_id"])["tasks"] == []

        core.fail_task(second["id"], "node-stream", "boom")
        update2 = core.get_job_result_updates(job["id"], after_updated_at=cursor["updated_at"], after_task_id=cursor["task_id"])
        assert update2 is not None
        assert update2["terminal"] is True
        assert [task["task_id"] for task in update2["tasks"]] == [second["id"]]
        assert update2["tasks"][0]["status"] == "failed"
        assert update2["summary"]["failed_tasks"] == 1


def test_session_result_updates_stream_across_jobs():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/session-stream.db"
        from taskgrid import core

        first_job = core.create_job("first stream job", "echo", [{"id": "a"}], session_name="stream session")
        second_job = core.create_job("second stream job", "echo", [{"id": "b"}], session_id=first_job["session_id"])
        first_task, second_task = core.lease_tasks("session-node", limit=2)
        core.complete_task(second_task["id"], "session-node", {"seen": second_task["input_key"]})
        core.complete_task(first_task["id"], "session-node", {"seen": first_task["input_key"]})

        update = core.get_session_result_updates(first_job["session_id"])
        assert update is not None
        assert update["terminal"] is True
        assert {task["job_id"] for task in update["tasks"]} == {first_job["id"], second_job["id"]}
        assert {task["input_key"] for task in update["tasks"]} == {"a", "b"}
        assert update["summary"]["total_jobs"] == 2
        assert core.latest_session_result_cursor(first_job["session_id"])["task_id"] in {first_task["id"], second_task["id"]}


def test_reconcile_repairs_job_and_session_counters_from_task_rows():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/reconcile-counters.db"
        from taskgrid import core

        job = core.create_job("reconcile counters", "echo", [{"n": 1}, {"n": 2}, {"n": 3}], max_retries=0)
        first, second = core.lease_tasks("node-r", limit=2)
        core.complete_task(first["id"], "node-r", {"ok": 1})
        core.fail_task(second["id"], "node-r", "boom")

        with core.connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status='succeeded', total_tasks=999, completed_tasks=999,
                    failed_tasks=0, cancelled_tasks=0, finished_at='2000-01-01T00:00:00+00:00'
                WHERE id=?
                """,
                (job["id"],),
            )
            conn.execute(
                """
                UPDATE service_sessions
                SET status='succeeded', total_jobs=99, total_tasks=999,
                    queued_tasks=0, running_tasks=0, completed_tasks=999,
                    failed_tasks=0, cancelled_tasks=0, finished_at='2000-01-01T00:00:00+00:00'
                WHERE id=?
                """,
                (job["session_id"],),
            )

        summary = core.reconcile_manager_state(recover_expired=False, updated_by="test")
        assert summary["jobs_checked"] == 1
        assert summary["sessions_checked"] == 1
        assert summary["jobs_reconciled"] == 1
        assert summary["sessions_reconciled"] == 1
        assert summary["lease_recovery"]["skipped"] is True

        repaired_job = core.get_job(job["id"])
        assert repaired_job is not None
        assert repaired_job["status"] == "running"
        assert repaired_job["total_tasks"] == 3
        assert repaired_job["completed_tasks"] == 1
        assert repaired_job["failed_tasks"] == 1
        assert repaired_job["cancelled_tasks"] == 0
        assert repaired_job["finished_at"] is None

        repaired_session = core.get_session(job["session_id"])
        assert repaired_session is not None
        assert repaired_session["status"] == "running"
        assert repaired_session["total_jobs"] == 1
        assert repaired_session["total_tasks"] == 3
        assert repaired_session["queued_tasks"] == 1
        assert repaired_session["running_tasks"] == 0
        assert repaired_session["completed_tasks"] == 1
        assert repaired_session["failed_tasks"] == 1
        assert repaired_session["finished_at"] is None
        assert core.list_events(code="MaintenanceReconcileRun", limit=1)


def test_reconcile_recovers_expired_leases_but_keeps_valid_running_leases():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/reconcile-leases.db"
        from taskgrid import core

        expired_job = core.create_job("expired", "echo", [{"n": 1}], max_retries=1)
        valid_job = core.create_job("valid", "echo", [{"n": 2}], max_retries=1)
        expired = core.lease_tasks("node-expired", limit=1, lease_seconds=30, instance_id="instance-001")[0]
        valid = core.lease_tasks("node-valid", limit=1, lease_seconds=3600, instance_id="instance-001")[0]
        with core.connection() as conn:
            conn.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?", ("2000-01-01T00:00:00+00:00", expired["id"]))

        summary = core.reconcile_manager_state(recover_expired=True, updated_by="test")
        assert summary["lease_recovery"]["expired"] == 1
        assert summary["lease_recovery"]["requeued"] == 1

        expired_after = core.get_task(expired["id"])
        valid_after = core.get_task(valid["id"])
        assert expired_after is not None and expired_after["status"] == "queued"
        assert valid_after is not None and valid_after["status"] == "running"
        assert valid_after["assigned_worker_id"] == "node-valid-instance-001"
        assert core.get_job(expired_job["id"])["status"] == "queued"
        assert core.get_job(valid_job["id"])["status"] == "running"


def test_idempotent_job_submit_replays_existing_job_for_retry():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/idempotent-submit.db"
        from taskgrid import core

        first = core.create_job(
            "idempotent",
            "echo",
            [{"n": 1}, {"n": 2}],
            client_id="client-a",
            idempotency_key="submit-123",
        )
        second = core.create_job(
            "idempotent retry",
            "echo",
            [{"n": 999}],
            client_id="client-a",
            idempotency_key="submit-123",
        )

        assert second["id"] == first["id"]
        assert second["session_id"] == first["session_id"]
        assert second["resume_token"] == first["resume_token"]
        assert len(core.list_tasks(first["id"])) == 2

        other = core.create_job(
            "same key different client",
            "echo",
            [{"n": 3}],
            client_id="client-b",
            idempotency_key="submit-123",
        )
        assert other["id"] != first["id"]


def test_drained_worker_blocks_new_leases_without_disabling_node():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/drained-worker.db"
        from taskgrid import core

        core.heartbeat_worker("node-drain", metadata={"task_types": ["echo"], "instance_count": 2})
        job = core.create_job("drain demo", "echo", [{"n": 1}, {"n": 2}])

        config = core.set_worker_draining("node-drain", True, reason="maintenance", updated_by="test")
        assert int(config["draining"]) == 1
        assert int(config["disabled"] or 0) == 0
        assert core.lease_tasks("node-drain", limit=1, instance_id="instance-001") == []
        assert core.lease_tasks_for_instances("node-drain", ["instance-001", "instance-002"]) == []

        worker = core.get_worker("node-drain")
        assert worker is not None
        assert worker["draining"] is True
        assert worker["disabled"] is False
        assert worker["drain_reason"] == "maintenance"

        config = core.set_worker_draining("node-drain", False, updated_by="test")
        assert int(config["draining"] or 0) == 0
        leased = core.lease_tasks("node-drain", limit=1, instance_id="instance-001")
        assert len(leased) == 1
        assert leased[0]["job_id"] == job["id"]


def test_drained_instance_is_skipped_by_batch_leasing_and_can_resume():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/drained-instance.db"
        from taskgrid import core

        core.heartbeat_worker("node-inst", metadata={"task_types": ["echo"], "instance_count": 3})
        core.create_job("instance drain demo", "echo", [{"n": i} for i in range(3)])

        instance_config = core.set_worker_instance_draining("node-inst", "instance-002", True, reason="bad slot", updated_by="test")
        assert int(instance_config["draining"]) == 1
        assert core.lease_tasks("node-inst", limit=1, instance_id="instance-002") == []

        leased = core.lease_tasks_for_instances("node-inst", ["instance-001", "instance-002", "instance-003"])
        assert len(leased) == 2
        assert {task["leased_instance_id"] for task in leased} == {"instance-001", "instance-003"}
        assert all("instance-002" not in task["assigned_worker_id"] for task in leased)

        worker = core.get_worker("node-inst")
        assert worker is not None
        assert worker["drained_instances"] == ["instance-002"]

        core.set_worker_instance_draining("node-inst", "instance-002", False, updated_by="test")
        resumed = core.lease_tasks("node-inst", limit=1, instance_id="instance-002")
        assert len(resumed) == 1
        assert resumed[0]["assigned_worker_id"] == "node-inst-instance-002"


def test_drained_worker_is_not_capable_for_strict_submit():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/drain-capability.db"
        from taskgrid import core

        core.heartbeat_worker("node-cap-drain", metadata={"task_types": ["echo"], "tags": ["cpu"], "instance_count": 1})
        core.set_worker_draining("node-cap-drain", True, reason="maintenance", updated_by="test")
        catalog = core.get_task_catalog(task_type="echo")
        assert catalog["supporting_workers"]
        assert not catalog["capable_workers"]
        assert catalog["workers"][0]["draining"] is True
        try:
            core.create_job("strict drain", "echo", [{"n": 1}], require_capable_worker=True)
        except core.CapabilityError as exc:
            assert "no active capable worker" in str(exc)
        else:
            raise AssertionError("strict submit should fail when only supporting worker is drained")


def test_services_registry_groups_worker_service_versions_and_states():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/services.db"
        from taskgrid import core

        core.heartbeat_worker(
            "risk-a",
            metadata={
                "service_name": "risk-engine",
                "service_version": "1.0.0",
                "task_types": ["price", "risk"],
                "tags": ["cpu", "risk"],
                "instance_count": 2,
                "running_tasks": 1,
            },
        )
        core.heartbeat_worker(
            "risk-b",
            metadata={
                "service_name": "risk-engine",
                "service_version": "1.0.0",
                "task_types": ["risk"],
                "tags": ["gpu", "risk"],
                "instance_count": 3,
            },
        )
        core.heartbeat_worker(
            "risk-old",
            metadata={
                "service_name": "risk-engine",
                "service_version": "0.9.0",
                "task_types": ["risk"],
                "tags": ["risk"],
                "instance_count": 1,
            },
        )
        core.set_worker_draining("risk-b", True, reason="rollout", updated_by="test")
        core.set_worker_enabled("risk-old", enabled=False, reason="old image", updated_by="test")

        registry = core.get_services()
        services = {(item["service_name"], item["service_version"]): item for item in registry["services"]}
        current = services[("risk-engine", "1.0.0")]

        assert current["workers_total"] == 2
        assert current["workers_active"] == 1
        assert current["workers_draining"] == 1
        assert current["running_tasks"] == 1
        assert current["active_instances"] == 5
        assert current["task_types"] == ["price", "risk"]
        assert current["tags"] == ["cpu", "gpu", "risk"]
        assert {worker["id"] for worker in current["workers"]} == {"risk-a", "risk-b"}

        old = services[("risk-engine", "0.9.0")]
        assert old["workers_disabled"] == 1
        assert old["workers_active"] == 0

        filtered = core.get_services(service_name="risk-engine", service_version="1.0.0")
        assert filtered["services_total"] == 1
        assert filtered["services"][0]["service_version"] == "1.0.0"


def test_session_assignments_show_worker_instance_and_status_rollups():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/session-assignments.db"
        from taskgrid import core

        core.heartbeat_worker(
            "node-session",
            metadata={
                "task_types": ["echo"],
                "tags": ["cpu"],
                "instance_count": 3,
                "service_name": "session-worker",
                "service_version": "1.0.0",
            },
        )
        job = core.create_job(
            "assignment demo",
            "echo",
            [{"n": 1}, {"n": 2}, {"n": 3}],
            input_keys=["row-1", "row-2", "row-3"],
        )
        leased = core.lease_tasks_for_instances("node-session", ["instance-001", "instance-002"])
        assert len(leased) == 2
        core.complete_task(leased[0]["id"], "node-session", {"ok": leased[0]["input_key"]}, instance_id="instance-001")

        summary = core.get_session_assignments(job["session_id"])
        assert summary is not None
        assert summary["status_counts"]["succeeded"] == 1
        assert summary["status_counts"]["running"] == 1
        assert summary["status_counts"]["queued"] == 1
        assert summary["running_assignments"] == 1
        running = summary["current_assignments"][0]
        assert running["worker_id"] == "node-session"
        assert running["instance_id"] == "instance-002"
        assert running["input_key"] == "row-2"
        worker = summary["workers"][0]
        assert worker["worker_id"] == "node-session"
        assert worker["service_name"] == "session-worker"
        assert worker["running_tasks"] == 1
        assert worker["completed_tasks"] == 1
        instance = next(item for item in worker["instances"] if item["instance_id"] == "instance-002")
        assert instance["state"] == "running"
        assert instance["current_task"]["task_id"] == leased[1]["id"]


def test_executors_show_global_instance_slots_and_current_tasks():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/executors.db"
        from taskgrid import core

        core.heartbeat_worker(
            "node-exec",
            metadata={
                "task_types": ["echo"],
                "tags": ["cpu"],
                "instance_count": 3,
                "service_name": "executor-service",
                "service_version": "1.0.0",
                "instances": [
                    {"instance_id": "instance-001", "status": "idle", "tasks_completed": 2, "last_poll_at": "2026-01-01T00:00:00+00:00"},
                    {"instance_id": "instance-002", "status": "running", "current_task_id": "advertised-only"},
                    {"instance_id": "instance-003", "status": "idle"},
                ],
            },
        )
        core.set_worker_instance_draining("node-exec", "instance-003", True, reason="slot maintenance", updated_by="test")
        job = core.create_job("executor demo", "echo", [{"n": 1}, {"n": 2}], input_keys=["one", "two"])
        leased = core.lease_tasks_for_instances("node-exec", ["instance-001", "instance-002"])
        assert len(leased) == 2

        summary = core.list_executors()
        assert summary["worker_count"] == 1
        assert summary["instance_count"] == 3
        assert summary["running_instances"] == 2
        assert summary["drained_instances"] == 1
        slots = {(item["worker_id"], item["instance_id"]): item for item in summary["instances"]}
        slot_1 = slots[("node-exec", "instance-001")]
        assert slot_1["state"] == "running"
        assert slot_1["current_task"]["session_id"] == job["session_id"]
        assert slot_1["current_task"]["input_key"] == "one"
        assert slot_1["service_name"] == "executor-service"
        slot_3 = slots[("node-exec", "instance-003")]
        assert slot_3["state"] == "drained"
        assert slot_3["drain_reason"] == "slot maintenance"

        detail = core.get_executor("node-exec", "instance-001")
        assert detail is not None
        assert detail["instance"]["state"] == "running"
        assert detail["current_task"]["task_id"] == leased[0]["id"]
        assert detail["recent_tasks"][0]["task_id"] == leased[0]["id"]


def test_session_task_history_keeps_finished_session_drilldown():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/session-history.db"
        from taskgrid import core

        core.heartbeat_worker(
            "node-history",
            metadata={
                "task_types": ["echo"],
                "instance_count": 2,
                "service_name": "history-worker",
                "service_version": "1.0.0",
            },
        )
        job = core.create_job(
            "history demo",
            "echo",
            [{"n": 1}, {"n": 2}],
            input_keys=["row-1", "row-2"],
            max_retries=0,
        )
        leased = core.lease_tasks_for_instances("node-history", ["instance-001", "instance-002"])
        assert len(leased) == 2
        core.complete_task(leased[0]["id"], "node-history", {"ok": leased[0]["input_key"]}, instance_id="instance-001")
        core.fail_task(leased[1]["id"], "node-history", "boom", instance_id="instance-002")

        session = core.get_session(job["session_id"])
        assert session is not None
        assert session["status"] == "failed"

        history = core.get_session_task_history(job["session_id"])
        assert history is not None
        assert history["task_count"] == 2
        assert history["assigned_workers"] == 1
        assert history["assigned_instances"] == 2
        assert history["status_counts"]["succeeded"] == 1
        assert history["status_counts"]["failed"] == 1
        assert [task["input_key"] for task in history["tasks"]] == ["row-1", "row-2"]
        assert {task["instance_id"] for task in history["tasks"]} == {"instance-001", "instance-002"}
        assert history["tasks"][0]["result"] == {"ok": "row-1"}
        assert history["tasks"][1]["error"] == "boom"

        failed_only = core.get_session_task_history(job["session_id"], status="failed")
        assert failed_only is not None
        assert failed_only["task_count"] == 1
        assert failed_only["tasks"][0]["input_key"] == "row-2"


def test_session_task_history_api_and_ui_are_clickable_from_finished_session():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/session-history-ui.db"
        from fastapi.testclient import TestClient
        from taskgrid import app as taskgrid_app, core

        core.heartbeat_worker("node-history-ui", metadata={"task_types": ["echo"], "instance_count": 1})
        job = core.create_job("history ui", "echo", [{"n": 1}], input_keys=["row-ui"])
        leased = core.lease_tasks("node-history-ui", limit=1, instance_id="instance-001")
        core.complete_task(leased[0]["id"], "node-history-ui", {"ok": True}, instance_id="instance-001")

        client = TestClient(taskgrid_app.app)
        raw = client.get(f"/sessions/{job['session_id']}/tasks/history")
        assert raw.status_code == 200
        assert raw.json()["tasks"][0]["input_key"] == "row-ui"
        assert raw.json()["tasks"][0]["instance_id"] == "instance-001"

        session_page = client.get(f"/ui/sessions/{job['session_id']}")
        assert session_page.status_code == 200
        assert "Task History" in session_page.text
        history_page = client.get(f"/ui/sessions/{job['session_id']}/history")
        assert history_page.status_code == 200
        assert "Each Task History" in history_page.text
        assert "row-ui" in history_page.text
        assert "node-history-ui" in history_page.text


def test_queue_diagnostics_explains_paused_missing_capacity_and_leaseable_backlog():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/queue-diagnostics.db"
        from taskgrid import core

        core.heartbeat_worker("node-echo", metadata={"task_types": ["echo"], "tags": ["cpu"], "instance_count": 1})
        leaseable = core.create_job("leaseable", "echo", [{"n": 1}], metadata={"required_tags": ["cpu"]})
        paused = core.create_job("paused", "echo", [{"n": 2}], session_name="paused session")
        core.set_session_paused(paused["session_id"], True, reason="operator hold", updated_by="test")
        missing_type = core.create_job("missing type", "gpu_task", [{"n": 3}])
        missing_tags = core.create_job("missing tags", "echo", [{"n": 4}], metadata={"required_tags": ["gpu"]})

        diagnostics = core.get_queue_diagnostics()
        reasons = {item["task_id"]: item["reason"] for item in diagnostics["tasks"]}
        leaseable_task = core.list_tasks(leaseable["id"])[0]["id"]
        paused_task = core.list_tasks(paused["id"])[0]["id"]
        missing_type_task = core.list_tasks(missing_type["id"])[0]["id"]
        missing_tags_task = core.list_tasks(missing_tags["id"])[0]["id"]

        assert reasons[leaseable_task] == "leaseable_now"
        assert reasons[paused_task] == "session_paused"
        assert reasons[missing_type_task] == "no_worker_supports_task_type"
        assert reasons[missing_tags_task] == "missing_required_tags"
        assert diagnostics["reason_counts"]["leaseable_now"] == 1
        assert diagnostics["reason_counts"]["session_paused"] == 1
        assert diagnostics["reason_counts"]["missing_required_tags"] == 1
        assert any(group["task_type"] == "echo" for group in diagnostics["task_types"])
        assert diagnostics["workers"][0]["estimated_free_slots"] == 1


def test_queue_diagnostics_reports_all_capable_workers_busy():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/queue-busy.db"
        from taskgrid import core

        core.heartbeat_worker("node-busy", metadata={"task_types": ["echo"], "instance_count": 1})
        running_job = core.create_job("running", "echo", [{"n": 1}])
        queued_job = core.create_job("queued", "echo", [{"n": 2}])
        leased = core.lease_tasks("node-busy", limit=1, instance_id="instance-001")
        assert leased[0]["job_id"] == running_job["id"]

        diagnostics = core.get_queue_diagnostics()
        queued_task_id = core.list_tasks(queued_job["id"])[0]["id"]
        item = next(task for task in diagnostics["tasks"] if task["task_id"] == queued_task_id)
        assert item["reason"] == "all_capable_workers_busy"
        assert diagnostics["reason_counts"]["all_capable_workers_busy"] == 1
        assert diagnostics["workers"][0]["running_slots"] == 1
        assert diagnostics["workers"][0]["estimated_free_slots"] == 0


def test_bulk_task_action_retries_and_cancels_session_tasks():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/bulk-actions.db"
        from taskgrid import core

        job = core.create_job("bulk controls", "echo", [{"n": 1}, {"n": 2}, {"n": 3}], max_retries=0)
        leased = core.lease_tasks("node-bulk", limit=1, instance_id="instance-001")
        failed = core.fail_task(leased[0]["id"], "node-bulk", "boom", instance_id="instance-001")
        assert failed["status"] == "failed"

        retried = core.bulk_update_tasks(action="retry", session_id=job["session_id"], statuses=["failed"])
        assert retried["changed"] == 1
        assert retried["tasks"][0]["status"] == "queued"
        assert core.get_task(leased[0]["id"])["status"] == "queued"

        cancelled = core.bulk_update_tasks(action="cancel", session_id=job["session_id"], statuses=["queued"])
        assert cancelled["changed"] == 3
        assert all(task["status"] == "cancelled" for task in core.list_tasks(job["id"]))
        assert core.get_session(job["session_id"])["status"] == "cancelled"


def test_bulk_task_action_can_force_cancel_running_and_api_ui_expose_controls():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/bulk-actions-api.db"
        from fastapi.testclient import TestClient
        from taskgrid import app as taskgrid_app, core

        job = core.create_job("bulk running", "echo", [{"n": 1}, {"n": 2}], max_retries=0)
        leased = core.lease_tasks("node-force", limit=1, instance_id="instance-001")
        assert leased[0]["status"] == "running"

        client = TestClient(taskgrid_app.app)
        page = client.get(f"/ui/sessions/{job['session_id']}/history")
        assert page.status_code == 200
        assert "Task Controls" in page.text
        assert "Force Cancel Running" in page.text

        response = client.post(
            f"/sessions/{job['session_id']}/tasks/bulk-action",
            json={"action": "cancel", "statuses": ["running"], "include_running": True},
        )
        assert response.status_code == 200
        assert response.json()["changed"] == 1
        task = core.get_task(leased[0]["id"])
        assert task["status"] == "cancelled"
        assert task["assigned_worker_id"] == "node-force-instance-001"

        # Late worker completion after force-cancel should not overwrite the cancelled state.
        ignored = core.complete_task(leased[0]["id"], "node-force", {"late": True}, instance_id="instance-001")
        assert ignored["status"] == "cancelled"


def test_operator_snapshot_cache_reuses_and_refreshes_executor_rollup():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/operator-cache.db"
        from taskgrid import core

        core.clear_operator_cache()
        core.heartbeat_worker("cache-node", metadata={"task_types": ["echo"], "instance_count": 2})
        first = core.list_executors()
        second = core.list_executors()
        refreshed = core.list_executors(refresh=True)

        assert first["cached"] is False
        assert second["cached"] is True
        assert second["cache_ttl_ms"] >= 0
        assert refreshed["cached"] is False
        assert refreshed["worker_count"] == 1
        assert refreshed["instance_count"] == 2


def test_batch_lease_response_preserves_task_fields_without_extra_read():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/lease-response.db"
        from taskgrid import core

        core.heartbeat_worker("lease-node", metadata={"task_types": ["echo"], "instance_count": 2})
        job = core.create_job("lease response", "echo", [{"n": 1}, {"n": 2}], input_keys=["a", "b"])
        leased = core.lease_tasks_for_instances("lease-node", ["instance-001", "instance-002"])

        assert len(leased) == 2
        assert [task["status"] for task in leased] == ["running", "running"]
        assert [task["attempts"] for task in leased] == [1, 1]
        assert [task["input_key"] for task in leased] == ["a", "b"]
        assert [task["payload"]["n"] for task in leased] == [1, 2]
        assert all(task["assigned_worker_id"].startswith("lease-node-instance-") for task in leased)
        assert [task["job_id"] for task in leased] == [job["id"], job["id"]]
