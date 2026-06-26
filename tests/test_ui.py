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
