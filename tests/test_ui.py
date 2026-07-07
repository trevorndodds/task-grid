from __future__ import annotations

import os
import tempfile

from fastapi.testclient import TestClient


def test_web_ui_submit_and_detail_pages():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui.db"
        from taskgrid.app import app

        client = TestClient(app)
        assert client.get("/ui").status_code == 200
        assert client.get("/ui/jobs").status_code == 200
        assert client.get("/ui/workers").status_code == 200

        response = client.post(
            "/ui/submit",
            data={
                "name": "ui demo",
                "task_type": "square",
                "priority": "1",
                "max_retries": "2",
                "tasks_json": '[{"x":2},{"x":3}]',
                "metadata_json": '{"source":"test"}',
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        detail = client.get(response.headers["location"])
        assert detail.status_code == 200
        assert "ui demo" in detail.text
        assert "Tasks" in detail.text


def test_web_ui_rejects_bad_json():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-bad.db"
        from taskgrid.app import app

        client = TestClient(app)
        response = client.post(
            "/ui/submit",
            data={
                "name": "bad",
                "task_type": "square",
                "priority": "0",
                "max_retries": "2",
                "tasks_json": "not-json",
                "metadata_json": "{}",
            },
        )
        assert response.status_code == 200
        assert "alert error" in response.text


def test_web_ui_worker_concurrency_config():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-worker-config.db"
        from taskgrid import core
        from taskgrid.app import app

        core.heartbeat_worker("node-a", metadata={"tags": ["cpu"], "active_concurrency": 1})
        client = TestClient(app)
        response = client.post(
            "/ui/workers/node-a/config",
            data={"desired_concurrency": "5"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert core.get_worker_config("node-a")["desired_concurrency"] == 5
        page = client.get("/ui/workers")
        assert "node-a" in page.text
        assert "value='5'" in page.text


def test_broker_proxies_worker_logs():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/logs-proxy.db"
        from taskgrid import core
        from taskgrid.app import app
        from taskgrid.worker import _setup_logging, _start_log_server

        logger, log_dir = _setup_logging("node-proxy", f"{tmp}/worker-logs")
        logger.info("proxied log line")
        for handler in logger.handlers:
            handler.flush()
        server, url = _start_log_server(log_dir, "node-proxy", "127.0.0.1", 0)
        try:
            core.heartbeat_worker("node-proxy", metadata={"active_concurrency": 1, "log_url": url, "log_dir": str(log_dir)})
            client = TestClient(app)
            listing = client.get("/workers/node-proxy/logs")
            assert listing.status_code == 200
            assert any(item["name"] == "supervisor.log" for item in listing.json()["files"])
            raw = client.get("/workers/node-proxy/logs/supervisor.log")
            assert raw.status_code == 200
            assert "proxied log line" in raw.text
            page = client.get("/ui/workers/node-proxy/logs")
            assert page.status_code == 200
            assert "supervisor.log" in page.text
            assert "Worker Logs" in page.text
        finally:
            server.shutdown()
            server.server_close()


def test_web_ui_manager_log_page():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/manager-log-ui.db"
        os.environ["TASKGRID_MANAGER_LOG"] = f"{tmp}/manager.log"
        from taskgrid import core
        from taskgrid.app import app

        core.create_job("manager log demo", "echo", [{"n": 1}])
        client = TestClient(app)
        page = client.get("/ui/manager/log")
        assert page.status_code == 200
        assert "Manager Log" in page.text
        assert "JobSubmitted" in page.text
        raw = client.get("/manager/log")
        assert raw.status_code == 200
        assert '"code":"JobSubmitted"' in raw.text
        os.environ.pop("TASKGRID_MANAGER_LOG", None)


def test_web_ui_service_sessions_pages():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-sessions.db"
        from taskgrid import core
        from taskgrid.app import app

        job = core.create_job("ui session job", "echo", [{"n": 1}, {"n": 2}], session_name="UI Session")
        client = TestClient(app)
        listing = client.get("/ui/sessions")
        assert listing.status_code == 200
        assert "Service Sessions" in listing.text
        assert "UI Session" in listing.text
        detail = client.get(f"/ui/sessions/{job['session_id']}")
        assert detail.status_code == 200
        assert "Created Jobs" in detail.text
        assert "Pending / Running" in detail.text
        assert "ui session job" in detail.text
        api = client.get(f"/sessions/{job['session_id']}")
        assert api.status_code == 200
        assert api.json()["total_tasks"] == 2


def test_web_ui_results_and_data_flow_pages():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-results.db"
        from taskgrid import core
        from taskgrid.app import app

        job = core.create_job("ui result job", "echo", [{"n": 1}], session_name="UI Results")
        task = core.lease_tasks("node-ui:instance-001", limit=1)[0]
        core.complete_task(task["id"], "node-ui:instance-001", {"ok": True})

        client = TestClient(app)
        job_page = client.get(f"/ui/jobs/{job['id']}/results")
        assert job_page.status_code == 200
        assert "Job Results" in job_page.text
        assert "node-ui:instance-001" in job_page.text
        assert "Task Results" in job_page.text

        session_page = client.get(f"/ui/sessions/{job['session_id']}/results")
        assert session_page.status_code == 200
        assert "Session Results" in session_page.text
        assert "ui result job" in session_page.text

        api = client.get(f"/sessions/{job['session_id']}/results")
        assert api.status_code == 200
        assert api.json()["results"][0]["result"] == {"ok": True}

        flow = client.get("/ui/manager/data-flow")
        assert flow.status_code == 200
        assert "Data Flow" in flow.text
        assert "TaskAccepted" in flow.text
        assert "GET /jobs" in flow.text


def test_web_ui_and_api_session_priority_update():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-session-priority.db"
        from taskgrid import core
        from taskgrid.app import app

        job = core.create_job("priority ui job", "echo", [{"n": 1}], session_name="Priority UI", session_priority=2)
        client = TestClient(app)
        detail = client.get(f"/ui/sessions/{job['session_id']}")
        assert detail.status_code == 200
        assert "Session Priority" in detail.text
        assert "value='2'" in detail.text

        response = client.post(
            f"/ui/sessions/{job['session_id']}/priority",
            data={"priority": "7"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert core.get_session(job["session_id"])["priority"] == 7

        api = client.post(f"/sessions/{job['session_id']}/priority", json={"priority": 11})
        assert api.status_code == 200
        assert api.json()["priority"] == 11


def test_web_ui_task_catalog_page_and_strict_submit():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-catalog.db"
        from taskgrid import core
        from taskgrid.app import app

        core.heartbeat_worker("cat-node", metadata={"tags": ["cpu"], "task_types": ["square"], "active_concurrency": 1})
        client = TestClient(app)
        page = client.get("/ui/task-catalog?task_type=square")
        assert page.status_code == 200
        assert "Task Catalog" in page.text
        assert "cat-node" in page.text
        assert "square" in page.text

        response = client.post(
            "/ui/submit",
            data={
                "name": "strict bad",
                "task_type": "unknown_ui_task",
                "priority": "0",
                "max_retries": "2",
                "tasks_json": '[{"x":2}]',
                "metadata_json": "{}",
                "require_capable_worker": "1",
            },
        )
        assert response.status_code == 200
        assert "alert error" in response.text
        assert "unknown_ui_task" in response.text


def test_client_resume_api_and_ui_page():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/client-resume-ui.db"
        from taskgrid import core
        from taskgrid.app import app

        job = core.create_job(
            "owned ui session",
            "echo",
            [{"n": 1}],
            session_name="Owned UI Session",
            client_id="client-ui",
        )
        client = TestClient(app)
        api = client.get(f"/clients/client-ui/sessions?resume_token={job['resume_token']}")
        assert api.status_code == 200
        assert api.json()[0]["id"] == job["session_id"]

        one = client.get(f"/clients/client-ui/sessions/{job['session_id']}?resume_token={job['resume_token']}")
        assert one.status_code == 200
        assert one.json()["name"] == "Owned UI Session"

        denied = client.get(f"/clients/client-ui/sessions/{job['session_id']}?resume_token=bad")
        assert denied.status_code == 404

        page = client.get(f"/ui/client-sessions?client_id=client-ui&resume_token={job['resume_token']}")
        assert page.status_code == 200
        assert "Client Resume" in page.text
        assert "Owned UI Session" in page.text

        detail = client.get(f"/ui/sessions/{job['session_id']}")
        assert "client_id=client-ui" in detail.text
        assert job["resume_token"] in detail.text


def test_recovery_ui_and_api_show_expired_lease_and_recover_it():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-recovery.db"
        from taskgrid import core
        from taskgrid.app import app

        job = core.create_job("ui recover", "echo", [{"n": 1}], max_retries=1)
        task = core.lease_tasks("ui-node", limit=1, lease_seconds=30, instance_id="instance-001")[0]
        with core.connection() as conn:
            conn.execute(
                "UPDATE tasks SET lease_expires_at=? WHERE id=?",
                ("2000-01-01T00:00:00+00:00", task["id"]),
            )

        client = TestClient(app)
        status = client.get("/maintenance/status")
        assert status.status_code == 200
        assert status.json()["expired_running_tasks"] == 1

        page = client.get("/ui/manager/recovery")
        assert page.status_code == 200
        assert "Recovery" in page.text
        assert "Expired Running Tasks" in page.text
        assert "ui-node-instance-001" in page.text

        response = client.post("/ui/manager/recovery/run", follow_redirects=False)
        assert response.status_code == 303
        assert core.get_task(task["id"])["status"] == "queued"
        assert client.get("/maintenance/status").json()["expired_running_tasks"] == 0


def test_api_tokens_protect_client_worker_and_admin_routes():
    import importlib
    import sys
    import tempfile
    import os
    from fastapi.testclient import TestClient

    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/auth.db"
        os.environ["TASKGRID_ADMIN_TOKEN"] = "admin-secret"
        os.environ["TASKGRID_CLIENT_TOKEN"] = "client-secret"
        os.environ["TASKGRID_WORKER_TOKEN"] = "worker-secret"
        sys.modules.pop("taskgrid.app", None)
        from taskgrid.app import app

        client = TestClient(app)
        assert client.post("/jobs", json={"name": "no", "task_type": "echo", "tasks": [{"n": 1}]}).status_code == 401
        ok = client.post(
            "/jobs",
            headers={"X-TaskGrid-Token": "client-secret"},
            json={"name": "yes", "task_type": "echo", "tasks": [{"n": 1}]},
        )
        assert ok.status_code == 201
        assert client.post("/workers/heartbeat", json={"worker_id": "w-auth"}).status_code == 401
        assert client.post("/workers/heartbeat", headers={"X-TaskGrid-Token": "worker-secret"}, json={"worker_id": "w-auth"}).status_code == 200
        assert client.get("/maintenance/status", headers={"X-TaskGrid-Token": "client-secret"}).status_code == 401
        assert client.get("/maintenance/status", headers={"X-TaskGrid-Token": "admin-secret"}).status_code == 200

        for key in ["TASKGRID_ADMIN_TOKEN", "TASKGRID_CLIENT_TOKEN", "TASKGRID_WORKER_TOKEN"]:
            os.environ.pop(key, None)
        sys.modules.pop("taskgrid.app", None)


def test_web_ui_worker_disable_enable_and_bulk_state():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-worker-state.db"
        from taskgrid import core
        from taskgrid.app import app

        core.heartbeat_worker("node-state-a", metadata={"task_types": ["echo"], "instance_count": 1})
        core.heartbeat_worker("node-state-b", metadata={"task_types": ["echo"], "instance_count": 1})
        client = TestClient(app)

        response = client.post(
            "/ui/workers/node-state-a/disable",
            data={"reason": "maintenance"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert core.get_worker("node-state-a")["disabled"] is True

        page = client.get("/ui/workers")
        assert page.status_code == 200
        assert "disabled" in page.text
        assert "maintenance" in page.text

        response = client.post(
            "/ui/workers/bulk-state",
            data={"worker_ids": ["node-state-a", "node-state-b"], "action": "enable"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert core.get_worker("node-state-a")["disabled"] is False
        assert core.get_worker("node-state-b")["disabled"] is False

        api = client.post("/workers/node-state-b/disable", json={"reason": "api"})
        assert api.status_code == 200
        assert core.get_worker("node-state-b")["disabled"] is True

def test_web_ui_and_api_pause_resume_controls():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-pause.db"
        from taskgrid import core
        from taskgrid.app import app

        job = core.create_job("pause ui job", "echo", [{"n": 1}], session_name="Pause UI")
        client = TestClient(app)

        response = client.post(f"/ui/sessions/{job['session_id']}/pause", data={"reason": "hold"}, follow_redirects=False)
        assert response.status_code == 303
        assert int(core.get_session(job["session_id"])["paused"]) == 1
        page = client.get(f"/ui/sessions/{job['session_id']}")
        assert page.status_code == 200
        assert "paused" in page.text.lower()
        assert core.lease_tasks("node-a", limit=1) == []

        response = client.post(f"/ui/sessions/{job['session_id']}/resume", follow_redirects=False)
        assert response.status_code == 303
        assert int(core.get_session(job["session_id"])["paused"]) == 0

        api_pause = client.post(f"/jobs/{job['id']}/pause", json={"reason": "job hold"})
        assert api_pause.status_code == 200
        assert int(api_pause.json()["paused"]) == 1
        assert core.lease_tasks("node-a", limit=1) == []

        api_resume = client.post(f"/jobs/{job['id']}/resume", json={})
        assert api_resume.status_code == 200
        assert int(api_resume.json()["paused"]) == 0
        assert len(core.lease_tasks("node-a", limit=1)) == 1


def test_job_and_session_result_stream_endpoints_emit_sse_events():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/sse-api.db"
        from taskgrid import core
        from taskgrid.app import app

        job = core.create_job("sse job", "echo", [{"id": "one"}, {"id": "two"}], max_retries=0, session_name="SSE Session")
        first, second = core.lease_tasks("sse-node", limit=2)
        core.complete_task(second["id"], "sse-node", {"ok": "two"})
        core.fail_task(first["id"], "sse-node", "bad one")

        client = TestClient(app)
        job_stream = client.get(f"/jobs/{job['id']}/results/stream?poll_seconds=0.1&timeout_seconds=5")
        assert job_stream.status_code == 200
        assert job_stream.headers["content-type"].startswith("text/event-stream")
        assert "event: progress" in job_stream.text
        assert "event: result" in job_stream.text
        assert "event: done" in job_stream.text
        assert '"input_key":"one"' in job_stream.text
        assert '"input_key":"two"' in job_stream.text

        session_stream = client.get(f"/sessions/{job['session_id']}/results/stream?poll_seconds=0.1&timeout_seconds=5")
        assert session_stream.status_code == 200
        assert "event: done" in session_stream.text
        assert '"scope":"session"' in session_stream.text


def test_maintenance_reconcile_api_and_ui():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/reconcile-api.db"
        from taskgrid import core
        from taskgrid.app import app

        job = core.create_job("reconcile api", "echo", [{"n": 1}, {"n": 2}])
        task = core.lease_tasks("node-api", limit=1)[0]
        core.complete_task(task["id"], "node-api", {"ok": True})
        with core.connection() as conn:
            conn.execute(
                "UPDATE jobs SET completed_tasks=0, status='queued' WHERE id=?",
                (job["id"],),
            )
            conn.execute(
                "UPDATE service_sessions SET completed_tasks=0, status='queued' WHERE id=?",
                (job["session_id"],),
            )

        client = TestClient(app)
        api = client.post("/maintenance/reconcile?recover_expired=false")
        assert api.status_code == 200
        payload = api.json()
        assert payload["jobs_reconciled"] >= 1
        assert payload["sessions_reconciled"] >= 1
        assert payload["lease_recovery"]["skipped"] is True
        assert core.get_job(job["id"])["completed_tasks"] == 1

        page = client.get("/ui/manager/recovery")
        assert page.status_code == 200
        assert "Reconcile Manager State" in page.text
        response = client.post("/ui/manager/recovery/reconcile", follow_redirects=False)
        assert response.status_code == 303


def test_manager_startup_reconcile_repairs_counter_drift():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/startup-reconcile.db"
        from taskgrid import core
        from taskgrid.app import app

        job = core.create_job("startup reconcile", "echo", [{"n": 1}])
        task = core.lease_tasks("node-start", limit=1)[0]
        core.complete_task(task["id"], "node-start", {"ok": True})
        with core.connection() as conn:
            conn.execute("UPDATE jobs SET status='queued', completed_tasks=0, finished_at=NULL WHERE id=?", (job["id"],))
            conn.execute("UPDATE service_sessions SET status='queued', completed_tasks=0, finished_at=NULL WHERE id=?", (job["session_id"],))

        with TestClient(app) as client:
            response = client.get(f"/jobs/{job['id']}")
            assert response.status_code == 200
            assert response.json()["status"] == "succeeded"
            assert response.json()["completed_tasks"] == 1
        assert core.list_events(code="MaintenanceReconcileRun", limit=1)


def test_web_ui_services_page_and_api():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/services-ui.db"
        from fastapi.testclient import TestClient
        from taskgrid import core
        from taskgrid.app import app

        core.heartbeat_worker(
            "svc-node",
            metadata={
                "service_name": "pricing-service",
                "service_version": "2.1.0",
                "task_types": ["price"],
                "tags": ["cpu"],
                "instance_count": 2,
            },
        )
        client = TestClient(app)
        raw = client.get("/services")
        assert raw.status_code == 200
        assert raw.json()["services"][0]["service_name"] == "pricing-service"

        page = client.get("/ui/services")
        assert page.status_code == 200
        assert "pricing-service" in page.text
        assert "2.1.0" in page.text
        assert "price" in page.text


def test_session_detail_shows_assignment_drilldown_and_api():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-session-assignments.db"
        from taskgrid import core
        from taskgrid.app import app

        core.heartbeat_worker(
            "ui-node-session",
            metadata={"task_types": ["echo"], "instance_count": 2, "service_name": "ui-service", "service_version": "1.0"},
        )
        job = core.create_job("ui assignment", "echo", [{"n": 1}, {"n": 2}], input_keys=["first", "second"])
        leased = core.lease_tasks_for_instances("ui-node-session", ["instance-001", "instance-002"])
        core.complete_task(leased[0]["id"], "ui-node-session", {"ok": True}, instance_id="instance-001")

        client = TestClient(app)
        api = client.get(f"/sessions/{job['session_id']}/assignments")
        assert api.status_code == 200
        payload = api.json()
        assert payload["running_assignments"] == 1
        assert payload["current_assignments"][0]["instance_id"] == "instance-002"

        page = client.get(f"/ui/sessions/{job['session_id']}")
        assert page.status_code == 200
        assert "Live Assignment Stats" in page.text
        assert "Current Running Assignments" in page.text
        assert "ui-node-session" in page.text
        assert "instance-002" in page.text
        assert "Assignments JSON" in page.text


def test_executor_api_and_ui_show_global_instance_assignments():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-executors.db"
        from taskgrid import core
        from taskgrid.app import app

        core.heartbeat_worker(
            "ui-node-exec",
            metadata={
                "task_types": ["echo"],
                "instance_count": 2,
                "service_name": "ui-executor-service",
                "service_version": "1.0",
                "instances": [
                    {"instance_id": "instance-001", "status": "idle"},
                    {"instance_id": "instance-002", "status": "idle"},
                ],
            },
        )
        job = core.create_job("ui executor", "echo", [{"n": 1}], input_keys=["first"])
        leased = core.lease_tasks_for_instances("ui-node-exec", ["instance-002"])
        assert leased

        client = TestClient(app)
        api = client.get("/executors")
        assert api.status_code == 200
        payload = api.json()
        assert payload["running_instances"] == 1
        assert payload["instances"][1]["instance_id"] == "instance-002"
        assert payload["instances"][1]["current_task"]["session_id"] == job["session_id"]

        detail_api = client.get("/executors/ui-node-exec/instances/instance-002")
        assert detail_api.status_code == 200
        assert detail_api.json()["current_task"]["input_key"] == "first"

        page = client.get("/ui/executors")
        assert page.status_code == 200
        assert "Live Instance Slots" in page.text
        assert "ui-node-exec" in page.text
        assert "instance-002" in page.text
        assert "ui-executor-service" in page.text

        detail_page = client.get("/ui/executors/ui-node-exec/instances/instance-002")
        assert detail_page.status_code == 200
        assert "Executor Instance" in detail_page.text
        assert "Current Assignment" in detail_page.text
        assert "Recent Tasks On This Instance" in detail_page.text


def test_queue_diagnostics_api_and_ui_explain_backlog():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["TASKGRID_DB"] = f"{tmp}/ui-queue.db"
        from taskgrid import core
        from taskgrid.app import app
        from fastapi.testclient import TestClient

        core.heartbeat_worker("ui-queue-node", metadata={"task_types": ["echo"], "tags": ["cpu"], "instance_count": 1})
        core.create_job("queue ok", "echo", [{"n": 1}], metadata={"required_tags": ["cpu"]})
        paused = core.create_job("queue paused", "echo", [{"n": 2}], session_name="Queue Pause")
        core.set_session_paused(paused["session_id"], True, reason="hold", updated_by="test")

        client = TestClient(app)
        api = client.get("/queue/diagnostics")
        assert api.status_code == 200
        assert api.json()["reason_counts"]["leaseable_now"] == 1
        assert api.json()["reason_counts"]["session_paused"] == 1

        page = client.get("/ui/queue")
        assert page.status_code == 200
        assert "Queue Diagnostics" in page.text
        assert "session_paused" in page.text
        assert "ui-queue-node" in page.text
