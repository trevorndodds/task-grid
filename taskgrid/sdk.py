from __future__ import annotations

import json
import time
import uuid
from typing import Any, Iterator
from urllib import parse, request


class TaskGridClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8000", client_id: str | None = None, resume_token: str | None = None, api_token: str | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id
        self.resume_token = resume_token
        self.api_token = api_token

    def attach(self, client_id: str, resume_token: str) -> None:
        self.client_id = client_id
        self.resume_token = resume_token

    def new_idempotency_key(self, prefix: str = "job") -> str:
        """Return a caller-storable key for safe submit retries after network/manager failure."""
        return f"{prefix}-{uuid.uuid4().hex}"

    def _headers(self, has_payload: bool = False) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"} if has_payload else {}
        if self.api_token:
            headers["X-TaskGrid-Token"] = self.api_token
        return headers

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=self._headers(payload is not None),
            method=method,
        )
        with request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))


    def _raw_request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> bytes:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=self._headers(payload is not None),
            method=method,
        )
        with request.urlopen(req, timeout=30) as response:
            return response.read()



    def _stream_request(self, path: str, timeout_seconds: float | None = None) -> Iterator[dict[str, Any]]:
        headers = self._headers(False)
        headers["Accept"] = "text/event-stream"
        timeout = None if timeout_seconds is None or timeout_seconds <= 0 else float(timeout_seconds) + 10.0
        req = request.Request(f"{self.base_url}{path}", headers=headers, method="GET")
        with request.urlopen(req, timeout=timeout) as response:
            event = "message"
            data_lines: list[str] = []
            for raw_line in response:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                if not line:
                    if data_lines:
                        payload = "\n".join(data_lines)
                        try:
                            out = json.loads(payload)
                        except json.JSONDecodeError:
                            out = {"data": payload}
                        out["_event"] = event
                        yield out
                    event = "message"
                    data_lines = []
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    event = line[6:].strip() or "message"
                elif line.startswith("data:"):
                    value = line[5:]
                    if value.startswith(" "):
                        value = value[1:]
                    data_lines.append(value)

    def submit(
        self,
        name: str,
        task_type: str,
        tasks: list[dict[str, Any]],
        priority: int | None = None,
        session_priority: int | None = None,
        max_retries: int = 2,
        metadata: dict[str, Any] | None = None,
        session_id: str | None = None,
        session_name: str | None = None,
        session_metadata: dict[str, Any] | None = None,
        require_capable_worker: bool = False,
        client_id: str | None = None,
        resume_token: str | None = None,
        input_keys: list[str | int] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        out = self._request(
            "POST",
            "/jobs",
            {
                "name": name,
                "task_type": task_type,
                "tasks": tasks,
                "priority": priority,
                "session_priority": session_priority,
                "max_retries": max_retries,
                "metadata": metadata or {},
                "session_id": session_id,
                "session_name": session_name,
                "session_metadata": session_metadata or {},
                "require_capable_worker": require_capable_worker,
                "client_id": client_id or self.client_id,
                "resume_token": resume_token or self.resume_token,
                "input_keys": input_keys,
                "idempotency_key": idempotency_key,
            },
        )
        self.client_id = out.get("client_id") or self.client_id
        self.resume_token = out.get("resume_token") or self.resume_token
        return out

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", f"/jobs/{job_id}")

    def sessions(self) -> list[dict[str, Any]]:
        return self._request("GET", "/sessions")

    def my_sessions(self, limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
        if not self.client_id or not self.resume_token:
            raise ValueError("client_id and resume_token are required to list resumable sessions")
        params = [("resume_token", self.resume_token), ("limit", str(limit))]
        if status:
            params.append(("status", status))
        return self._request("GET", f"/clients/{parse.quote(self.client_id, safe='')}/sessions?{parse.urlencode(params)}")

    def reconnect_session(self, session_id: str) -> dict[str, Any]:
        """Return an owned session using the client's reconnect credentials.

        This is intentionally separate from ``resume_session(...)``, which
        unpauses a paused service session.
        """
        if not self.client_id or not self.resume_token:
            raise ValueError("client_id and resume_token are required to reconnect to a session")
        return self._request("GET", f"/clients/{parse.quote(self.client_id, safe='')}/sessions/{parse.quote(session_id, safe='')}?resume_token={parse.quote(self.resume_token, safe='')}")

    def resume_owned_session(self, session_id: str) -> dict[str, Any]:
        """Backwards-readable alias for reconnecting to an owned session."""
        return self.reconnect_session(session_id)

    def get_session(self, session_id: str) -> dict[str, Any]:
        return self._request("GET", f"/sessions/{session_id}")

    def set_session_priority(self, session_id: str, priority: int) -> dict[str, Any]:
        return self._request("POST", f"/sessions/{session_id}/priority", {"priority": priority})

    def pause_session(self, session_id: str, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", f"/sessions/{session_id}/pause", {"reason": reason})

    def resume_session(self, session_id: str) -> dict[str, Any]:
        return self._request("POST", f"/sessions/{session_id}/resume", {})

    def set_sessions_paused(self, session_ids: list[str], paused: bool, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", "/sessions/bulk-pause", {"session_ids": session_ids, "paused": paused, "reason": reason})

    def session_jobs(self, session_id: str) -> list[dict[str, Any]]:
        return self._request("GET", f"/sessions/{session_id}/jobs")

    def session_assignments(self, session_id: str, limit: int = 1000) -> dict[str, Any]:
        return self._request("GET", f"/sessions/{session_id}/assignments?{parse.urlencode({'limit': limit})}")

    def session_task_history(
        self,
        session_id: str,
        limit: int = 5000,
        status: str | None = None,
        job_id: str | None = None,
        order: str = "input",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit, "order": order}
        if status:
            params["status"] = status
        if job_id:
            params["job_id"] = job_id
        return self._request("GET", f"/sessions/{parse.quote(session_id, safe='')}/tasks/history?{parse.urlencode(params)}")

    def session_results(self, session_id: str, order: str = "input") -> dict[str, Any]:
        return self._request("GET", f"/sessions/{session_id}/results?{parse.urlencode({'order': order})}")


    def stream_session_results(
        self,
        session_id: str,
        poll_seconds: float = 0.5,
        timeout_seconds: float | None = None,
        replay: bool = True,
    ) -> Iterator[dict[str, Any]]:
        params = {
            "poll_seconds": str(float(poll_seconds)),
            "timeout_seconds": str(float(timeout_seconds or 0)),
            "replay": str(bool(replay)).lower(),
        }
        yield from self._stream_request(f"/sessions/{parse.quote(session_id, safe='')}/results/stream?{parse.urlencode(params)}", timeout_seconds=timeout_seconds)

    def export_session_results(self, session_id: str, format: str = "json", order: str = "input", failed_only: bool = False) -> bytes:
        params = parse.urlencode({"format": format, "order": order, "failed_only": str(bool(failed_only)).lower()})
        return self._raw_request("GET", f"/sessions/{session_id}/results/export?{params}")

    def tasks(self, job_id: str) -> list[dict[str, Any]]:
        return self._request("GET", f"/jobs/{job_id}/tasks")

    def results(self, job_id: str, order: str = "input") -> dict[str, Any]:
        return self._request("GET", f"/jobs/{job_id}/results?{parse.urlencode({'order': order})}")


    def stream_results(
        self,
        job_id: str,
        poll_seconds: float = 0.5,
        timeout_seconds: float | None = None,
        replay: bool = True,
    ) -> Iterator[dict[str, Any]]:
        params = {
            "poll_seconds": str(float(poll_seconds)),
            "timeout_seconds": str(float(timeout_seconds or 0)),
            "replay": str(bool(replay)).lower(),
        }
        yield from self._stream_request(f"/jobs/{parse.quote(job_id, safe='')}/results/stream?{parse.urlencode(params)}", timeout_seconds=timeout_seconds)

    def export_results(self, job_id: str, format: str = "json", order: str = "input", failed_only: bool = False) -> bytes:
        params = parse.urlencode({"format": format, "order": order, "failed_only": str(bool(failed_only)).lower()})
        return self._raw_request("GET", f"/jobs/{job_id}/results/export?{params}")

    def cancel(self, job_id: str, mode: str = "graceful") -> dict[str, Any]:
        return self._request("POST", f"/jobs/{job_id}/cancel?mode={parse.quote(mode, safe='')}", {})

    def pause_job(self, job_id: str, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", f"/jobs/{job_id}/pause", {"reason": reason})

    def resume_job(self, job_id: str) -> dict[str, Any]:
        return self._request("POST", f"/jobs/{job_id}/resume", {})

    def retry_failed(self, job_id: str, reset_attempts: bool = True) -> dict[str, Any]:
        suffix = "true" if reset_attempts else "false"
        return self._request("POST", f"/jobs/{job_id}/retry-failed?reset_attempts={suffix}", {})

    def retry_task(self, task_id: str, reset_attempts: bool = True) -> dict[str, Any]:
        suffix = "true" if reset_attempts else "false"
        return self._request("POST", f"/tasks/{task_id}/retry?reset_attempts={suffix}", {})


    def bulk_task_action(
        self,
        action: str,
        *,
        session_id: str | None = None,
        job_id: str | None = None,
        task_ids: list[str] | None = None,
        statuses: list[str] | None = None,
        reset_attempts: bool = True,
        include_running: bool = False,
        limit: int = 5000,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "action": action,
            "reset_attempts": bool(reset_attempts),
            "include_running": bool(include_running),
            "limit": int(limit),
        }
        if session_id:
            payload["session_id"] = session_id
        if job_id:
            payload["job_id"] = job_id
        if task_ids:
            payload["task_ids"] = task_ids
        if statuses:
            payload["statuses"] = statuses
        return self._request("POST", "/tasks/bulk-action", payload)

    def retry_session_tasks(
        self,
        session_id: str,
        statuses: list[str] | None = None,
        reset_attempts: bool = True,
        limit: int = 5000,
    ) -> dict[str, Any]:
        return self.bulk_task_action(
            "retry",
            session_id=session_id,
            statuses=statuses or ["failed", "cancelled"],
            reset_attempts=reset_attempts,
            limit=limit,
        )

    def cancel_session_tasks(
        self,
        session_id: str,
        statuses: list[str] | None = None,
        include_running: bool = False,
        limit: int = 5000,
    ) -> dict[str, Any]:
        return self.bulk_task_action(
            "cancel",
            session_id=session_id,
            statuses=statuses or ["queued"],
            include_running=include_running,
            limit=limit,
        )

    def retry_job_tasks(
        self,
        job_id: str,
        statuses: list[str] | None = None,
        reset_attempts: bool = True,
        limit: int = 5000,
    ) -> dict[str, Any]:
        return self.bulk_task_action(
            "retry",
            job_id=job_id,
            statuses=statuses or ["failed", "cancelled"],
            reset_attempts=reset_attempts,
            limit=limit,
        )

    def cancel_job_tasks(
        self,
        job_id: str,
        statuses: list[str] | None = None,
        include_running: bool = False,
        limit: int = 5000,
    ) -> dict[str, Any]:
        return self.bulk_task_action(
            "cancel",
            job_id=job_id,
            statuses=statuses or ["queued"],
            include_running=include_running,
            limit=limit,
        )

    def queue_diagnostics(self, limit: int = 1000, active_seconds: int | None = None, refresh: bool = False) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": int(limit)}
        if active_seconds is not None:
            params["active_seconds"] = int(active_seconds)
        if refresh:
            params["refresh"] = "true"
        return self._request("GET", f"/queue/diagnostics?{parse.urlencode(params)}")

    def task_catalog(self, task_type: str | None = None, required_tags: list[str] | None = None) -> dict[str, Any]:
        params = []
        if task_type:
            params.append(("task_type", task_type))
        if required_tags:
            params.append(("required_tags", ",".join(required_tags)))
        query = ""
        if params:
            query = "?" + parse.urlencode(params)
        return self._request("GET", f"/task-catalog{query}")

    def workers(self) -> list[dict[str, Any]]:
        return self._request("GET", "/workers")

    def services(self, service_name: str | None = None, service_version: str | None = None, refresh: bool = False) -> dict[str, Any]:
        params = []
        if service_name:
            params.append(("service_name", service_name))
        if service_version:
            params.append(("service_version", service_version))
        if refresh:
            params.append(("refresh", "true"))
        query = "?" + parse.urlencode(params) if params else ""
        return self._request("GET", f"/services{query}")

    def executors(self, limit: int = 1000, active_seconds: int | None = None, refresh: bool = False) -> dict[str, Any]:
        params = [("limit", str(int(limit)))]
        if active_seconds is not None:
            params.append(("active_seconds", str(int(active_seconds))))
        if refresh:
            params.append(("refresh", "true"))
        return self._request("GET", f"/executors?{parse.urlencode(params)}")

    def executor(self, worker_id: str, instance_id: str, recent_limit: int = 100) -> dict[str, Any]:
        params = parse.urlencode({"recent_limit": int(recent_limit)})
        return self._request("GET", f"/executors/{parse.quote(worker_id, safe='')}/instances/{parse.quote(instance_id, safe='')}?{params}")

    def worker_config(self, worker_id: str) -> dict[str, Any]:
        return self._request("GET", f"/workers/{worker_id}/config")

    def set_worker_concurrency(self, worker_id: str, desired_concurrency: int) -> dict[str, Any]:
        # Backwards-compatible method name. The value now means desired
        # single-task instances managed by the worker node.
        return self._request("POST", f"/workers/{worker_id}/config", {"desired_concurrency": desired_concurrency})

    def set_worker_instances(self, worker_id: str, desired_instances: int) -> dict[str, Any]:
        return self.set_worker_concurrency(worker_id, desired_instances)

    def disable_worker(self, worker_id: str, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", f"/workers/{worker_id}/disable", {"reason": reason})

    def enable_worker(self, worker_id: str, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", f"/workers/{worker_id}/enable", {"reason": reason})

    def drain_worker(self, worker_id: str, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", f"/workers/{worker_id}/drain", {"reason": reason})

    def undrain_worker(self, worker_id: str, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", f"/workers/{worker_id}/undrain", {"reason": reason})

    def drain_worker_instance(self, worker_id: str, instance_id: str, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", f"/workers/{worker_id}/instances/{parse.quote(instance_id, safe='')}/drain", {"reason": reason})

    def undrain_worker_instance(self, worker_id: str, instance_id: str, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", f"/workers/{worker_id}/instances/{parse.quote(instance_id, safe='')}/undrain", {"reason": reason})

    def set_workers_disabled(self, worker_ids: list[str], disabled: bool, reason: str | None = None) -> dict[str, Any]:
        return self._request("POST", "/workers/bulk-state", {"worker_ids": worker_ids, "disabled": disabled, "reason": reason})

    def disable_workers(self, worker_ids: list[str], reason: str | None = None) -> dict[str, Any]:
        return self.set_workers_disabled(worker_ids, True, reason=reason)

    def enable_workers(self, worker_ids: list[str], reason: str | None = None) -> dict[str, Any]:
        return self.set_workers_disabled(worker_ids, False, reason=reason)



    def recovery_status(self) -> dict[str, Any]:
        return self._request("GET", "/maintenance/status")

    def recover_expired_leases(self) -> dict[str, Any]:
        return self._request("POST", "/maintenance/recover", {})

    def reconcile_manager(self, recover_expired: bool = True) -> dict[str, Any]:
        suffix = "true" if recover_expired else "false"
        return self._request("POST", f"/maintenance/reconcile?recover_expired={suffix}", {})

    def worker_logs(self, worker_id: str) -> dict[str, Any]:
        return self._request("GET", f"/workers/{worker_id}/logs")

    def worker_log(self, worker_id: str, filename: str, tail_bytes: int = 1_000_000) -> str:
        req = request.Request(
            f"{self.base_url}/workers/{worker_id}/logs/{parse.quote(filename, safe='/')}?tail={int(tail_bytes)}",
            headers=self._headers(False),
            method="GET",
        )
        with request.urlopen(req, timeout=30) as response:
            return response.read().decode("utf-8", errors="replace")

    def purge_offline_workers(self, active_seconds: int | None = None, include_running: bool = False) -> dict[str, Any]:
        params = []
        if active_seconds is not None:
            params.append(("active_seconds", str(int(active_seconds))))
        if include_running:
            params.append(("include_running", "true"))
        query = "?" + parse.urlencode(params) if params else ""
        return self._request("POST", f"/workers/purge-offline{query}", {})


    def retention_preview(
        self,
        completed_days: int = 30,
        failed_days: int = 90,
        event_days: int = 30,
        purge_workers_active_seconds: int | None = None,
    ) -> dict[str, Any]:
        params = [
            ("completed_days", str(int(completed_days))),
            ("failed_days", str(int(failed_days))),
            ("event_days", str(int(event_days))),
        ]
        if purge_workers_active_seconds is not None:
            params.append(("purge_workers_active_seconds", str(int(purge_workers_active_seconds))))
        return self._request("GET", f"/maintenance/retention/preview?{parse.urlencode(params)}")

    def apply_retention_cleanup(
        self,
        completed_days: int = 30,
        failed_days: int = 90,
        event_days: int = 30,
        purge_workers_active_seconds: int | None = None,
        manager_log_keep_bytes: int | None = None,
    ) -> dict[str, Any]:
        params = [
            ("completed_days", str(int(completed_days))),
            ("failed_days", str(int(failed_days))),
            ("event_days", str(int(event_days))),
        ]
        if purge_workers_active_seconds is not None:
            params.append(("purge_workers_active_seconds", str(int(purge_workers_active_seconds))))
        if manager_log_keep_bytes is not None:
            params.append(("manager_log_keep_bytes", str(int(manager_log_keep_bytes))))
        return self._request("POST", f"/maintenance/retention/apply?{parse.urlencode(params)}", {})

    def wait(self, job_id: str, poll_seconds: float = 1.0, timeout_seconds: float | None = None) -> dict[str, Any]:
        started = time.time()
        while True:
            job = self.get_job(job_id)
            if job["status"] in {"succeeded", "failed", "cancelled"}:
                return job
            if timeout_seconds is not None and time.time() - started > timeout_seconds:
                raise TimeoutError(f"job {job_id} did not finish in {timeout_seconds}s")
            time.sleep(poll_seconds)

