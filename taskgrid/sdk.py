from __future__ import annotations

import json
import time
from typing import Any
from urllib import parse, request


class TaskGridClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8000") -> None:
        self.base_url = base_url.rstrip("/")

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers={"Content-Type": "application/json"} if payload is not None else {},
            method=method,
        )
        with request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

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
    ) -> dict[str, Any]:
        return self._request(
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
            },
        )

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", f"/jobs/{job_id}")

    def sessions(self) -> list[dict[str, Any]]:
        return self._request("GET", "/sessions")

    def get_session(self, session_id: str) -> dict[str, Any]:
        return self._request("GET", f"/sessions/{session_id}")

    def set_session_priority(self, session_id: str, priority: int) -> dict[str, Any]:
        return self._request("POST", f"/sessions/{session_id}/priority", {"priority": priority})

    def session_jobs(self, session_id: str) -> list[dict[str, Any]]:
        return self._request("GET", f"/sessions/{session_id}/jobs")

    def session_results(self, session_id: str) -> dict[str, Any]:
        return self._request("GET", f"/sessions/{session_id}/results")

    def tasks(self, job_id: str) -> list[dict[str, Any]]:
        return self._request("GET", f"/jobs/{job_id}/tasks")

    def results(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", f"/jobs/{job_id}/results")

    def cancel(self, job_id: str) -> dict[str, Any]:
        return self._request("POST", f"/jobs/{job_id}/cancel", {})

    def retry_failed(self, job_id: str, reset_attempts: bool = True) -> dict[str, Any]:
        suffix = "true" if reset_attempts else "false"
        return self._request("POST", f"/jobs/{job_id}/retry-failed?reset_attempts={suffix}", {})

    def retry_task(self, task_id: str, reset_attempts: bool = True) -> dict[str, Any]:
        suffix = "true" if reset_attempts else "false"
        return self._request("POST", f"/tasks/{task_id}/retry?reset_attempts={suffix}", {})


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

    def worker_config(self, worker_id: str) -> dict[str, Any]:
        return self._request("GET", f"/workers/{worker_id}/config")

    def set_worker_concurrency(self, worker_id: str, desired_concurrency: int) -> dict[str, Any]:
        # Backwards-compatible method name. The value now means desired
        # single-task instances managed by the worker node.
        return self._request("POST", f"/workers/{worker_id}/config", {"desired_concurrency": desired_concurrency})

    def set_worker_instances(self, worker_id: str, desired_instances: int) -> dict[str, Any]:
        return self.set_worker_concurrency(worker_id, desired_instances)


    def worker_logs(self, worker_id: str) -> dict[str, Any]:
        return self._request("GET", f"/workers/{worker_id}/logs")

    def worker_log(self, worker_id: str, filename: str, tail_bytes: int = 1_000_000) -> str:
        req = request.Request(
            f"{self.base_url}/workers/{worker_id}/logs/{parse.quote(filename, safe='/')}?tail={int(tail_bytes)}",
            method="GET",
        )
        with request.urlopen(req, timeout=30) as response:
            return response.read().decode("utf-8", errors="replace")

    def wait(self, job_id: str, poll_seconds: float = 1.0, timeout_seconds: float | None = None) -> dict[str, Any]:
        started = time.time()
        while True:
            job = self.get_job(job_id)
            if job["status"] in {"succeeded", "failed", "cancelled"}:
                return job
            if timeout_seconds is not None and time.time() - started > timeout_seconds:
                raise TimeoutError(f"job {job_id} did not finish in {timeout_seconds}s")
            time.sleep(poll_seconds)

# Backwards-compatible alias for code written before the rename.
GridLiteClient = TaskGridClient
