# TaskGrid API Reference

This document covers the REST API exposed by the TaskGrid manager/broker.

The manager is a FastAPI app. When it is running, interactive API docs are also available at:

```text
/docs
/redoc
/openapi.json
```

Default base URL for local development:

```text
http://127.0.0.1:8000
```


## Authentication

Authentication is optional and disabled unless tokens are configured on the manager.

```bash
TASKGRID_ADMIN_TOKEN=admin-secret
TASKGRID_CLIENT_TOKEN=client-secret
TASKGRID_WORKER_TOKEN=worker-secret
TASKGRID_UI_TOKEN=ui-secret
```

Send tokens using either header:

```http
X-TaskGrid-Token: client-secret
Authorization: Bearer client-secret
```

Role behavior:

| Token | Purpose |
|---|---|
| `TASKGRID_ADMIN_TOKEN` | Admin/maintenance APIs and accepted everywhere. |
| `TASKGRID_CLIENT_TOKEN` | Job, session, result, task catalog, and client resume APIs. |
| `TASKGRID_WORKER_TOKEN` | Worker heartbeat, leasing, complete, and fail APIs. |
| `TASKGRID_UI_TOKEN` | Browser access to `/ui`. |

If any token is configured, admin routes require the admin token. If a role token is not configured and auth is otherwise disabled, local development remains open.

## Concepts

```text
Service Session
  A client-facing run/grouping. Usually created automatically when a client submits a job.

Job
  A named batch of tasks with the same task_type.

Task
  One independent unit of work. Workers lease one task at a time per instance.

Worker Node
  A process/container that supervises one or more single-task instances.

Worker Instance
  A single execution slot inside a worker node, e.g. node-a-instance-003.
```

## Status values

Common session/job statuses:

```text
queued
running
cancelling
succeeded
failed
cancelled
```

Sessions and jobs also expose a separate `paused` flag. Pause is a scheduling hold: queued tasks remain queued, but the manager will not lease them until the session/job is resumed.

Common task statuses:

```text
queued
running
succeeded
failed
cancelled
```

## Public/client endpoints

These are the endpoints a normal submitting client or operator is expected to use.

### Health

```http
GET /health
```

Example response:

```json
{
  "ok": true,
  "db": "/path/to/taskgrid.db"
}
```

---

## Jobs

### Submit a job

```http
POST /jobs
Content-Type: application/json
```

Request body:

```json
{
  "name": "square numbers",
  "task_type": "square",
  "tasks": [
    {"x": 1},
    {"x": 2},
    {"x": 3}
  ],
  "input_keys": ["row-1", "row-2", "row-3"],
  "priority": null,
  "session_priority": 10,
  "max_retries": 2,
  "metadata": {},
  "session_id": null,
  "session_name": "Morning Square Run",
  "session_metadata": {},
  "client_id": "client-alpha",
  "resume_token": null,
  "idempotency_key": "submit-2026-06-27-001"
}
```

Fields:

| Field | Required | Notes |
|---|---:|---|
| `name` | yes | Human-readable job name. |
| `task_type` | yes | Must match a task registered on eligible workers. |
| `tasks` | yes | List of JSON payloads. One task row is created per payload. |
| `input_keys` | no | Optional stable keys matching `tasks` length. If omitted, TaskGrid derives keys from payload `id`, `input_key`, or `key` when present. |
| `idempotency_key` | no | Optional per-client submit key. Reusing the same `client_id` + `idempotency_key` returns the existing job, which makes retries safe after network loss or manager restart. |
| `priority` | no | Job/task priority override. If omitted, uses `session_priority` or existing session priority. |
| `session_priority` | no | Priority for the service session. Higher priority leases first. |
| `max_retries` | no | Automatic retry count for failed tasks. Default `2`. |
| `metadata` | no | Job metadata. Use `required_tags` here for routing. |
| `session_id` | no | Attach this job to an existing session. |
| `session_name` | no | Create a named session when `session_id` is omitted. |
| `client_id` | no | Stable client owner id. Use one with `idempotency_key` when submit retries must survive a lost response. |
| `resume_token` | no | Token used to attach to existing owned sessions. |
| `session_metadata` | no | Metadata for a newly created session. |
| `client_id` | no | Client owner for reconnect/list-my-sessions flows. Generated if omitted. |
| `resume_token` | no | Bearer-style reconnect token. Generated for new sessions if omitted; required for client-owned resume/list APIs. |
| `require_capable_worker` | no | If `true`, rejects the submission with HTTP 409 when no active worker advertises support for the task type and required tags. |

Routing example using worker tags:

```json
{
  "name": "GPU risk batch",
  "task_type": "risk_price",
  "tasks": [{"trade_id": "T1"}],
  "metadata": {
    "required_tags": ["gpu", "risk-v1.1.0"]
  }
}
```

Example response:

```json
{
  "id": "job_abc123",
  "session_id": "sess_def456",
  "name": "square numbers",
  "task_type": "square",
  "status": "queued",
  "priority": 10,
  "total_tasks": 3,
  "completed_tasks": 0,
  "failed_tasks": 0,
  "cancelled_tasks": 0,
  "client_id": "client-alpha",
  "resume_token": "rt_...",
  "created_at": "2026-06-26T18:00:00.000+00:00",
  "started_at": null,
  "finished_at": null,
  "metadata": {}
}
```

### List jobs

```http
GET /jobs?limit=100&status=running
```

Query parameters:

| Parameter | Notes |
|---|---|
| `limit` | `1` to `500`, default `100`. |
| `status` | Optional status filter. |

### Get job

```http
GET /jobs/{job_id}
```

### Cancel a job

```http
POST /jobs/{job_id}/cancel?mode=graceful
POST /jobs/{job_id}/cancel?mode=force
```

`graceful` is the default. It cancels queued tasks immediately and lets running tasks finish while the job reports `cancelling`. `force` marks both queued and running tasks cancelled immediately.

### Pause/resume a job

```http
POST /jobs/{job_id}/pause
Content-Type: application/json

{"reason":"maintenance"}

POST /jobs/{job_id}/resume
Content-Type: application/json

{}
```

Pausing a job does not cancel running tasks. It prevents queued tasks in that job from being leased. Resuming makes queued tasks eligible again unless the parent session is also paused.

### Get job tasks

```http
GET /jobs/{job_id}/tasks
```


### Bulk task actions

```http
POST /tasks/bulk-action
POST /sessions/{session_id}/tasks/bulk-action
POST /jobs/{job_id}/tasks/bulk-action
```

Request body:

```json
{
  "action": "retry",
  "session_id": "sess_abc123",
  "job_id": null,
  "task_ids": null,
  "statuses": ["failed", "cancelled"],
  "reset_attempts": true,
  "include_running": false,
  "limit": 5000
}
```

Supported actions are `retry` and `cancel`, with aliases such as `retry_failed` and `cancel_queued`. Retry only applies to failed/cancelled tasks. Cancel defaults to queued tasks; pass `include_running=true` with `statuses:["running"]` to force-cancel running tasks. Force-cancelled running tasks keep their final executor assignment for history, and late worker completions are ignored.

Response includes selected/changed/skipped counts, changed task ids, and affected jobs/sessions.

### Get job results

```http
GET /jobs/{job_id}/results?order=input
```

`order` defaults to `input` and can be `input`, `started`, `started_at`, `completed`, `finished`, `finished_at`, or `status`. Input order uses first-class `input_index`, so results remain stable even when tasks finish out of order.

Example response shape:

```json
{
  "job_id": "job_abc123",
  "session_id": "sess_def456",
  "name": "square numbers",
  "status": "succeeded",
  "tasks": [
    {
      "task_id": "task_001",
      "job_id": "job_abc123",
      "task_type": "square",
      "input_index": 0,
      "input_key": "row-1",
      "status": "succeeded",
      "payload": {"x": 2},
      "result": {"x": 2, "square": 4},
      "error": null,
      "attempts": 1,
      "assigned_worker_id": "node-a-instance-001",
      "started_at": "2026-06-26T18:00:01.000+00:00",
      "finished_at": "2026-06-26T18:00:01.050+00:00",
      "payload_bytes": 7,
      "result_bytes": 20
    }
  ],
  "results": [
    {
      "task_id": "task_001",
      "input_index": 0,
      "input_key": "row-1",
      "status": "succeeded",
      "result": {"x": 2, "square": 4},
      "error": null
    }
  ]
}
```


### Get session task history

```http
GET /sessions/{session_id}/tasks/history?status=failed&order=input&limit=5000
```

Returns durable task history for a service session. This is intended for finished-session drilldown and audit-style review. It is not limited to currently running assignments.

Query parameters:

| Parameter | Description |
| --- | --- |
| `status` | Optional `queued`, `running`, `succeeded`, `failed`, or `cancelled` filter. |
| `job_id` | Optional filter to one job inside the session. |
| `order` | `input` by default; also supports `completed`, `started`, and `status`. |
| `limit` | Maximum task rows, 1 to 10000. |

Response includes session counters, status counts, assigned worker/instance counts, jobs, and task rows with `task_id`, `job_id`, `input_index`, `input_key`, `status`, `worker_id`, `instance_id`, attempts, timing, payload/result sizes, result, and error.

### Stream job results

```http
GET /jobs/{job_id}/results/stream?poll_seconds=0.5&timeout_seconds=0&replay=true
Accept: text/event-stream
```

Streams Server-Sent Events for async clients. Event names are:

- `progress`: current job counters/status.
- `result`: one newly terminal task result, including `input_index`, `input_key`, payload, result/error, worker, attempts, and timing.
- `done`: job is terminal and all terminal task results up to the final cursor were sent.
- `timeout`: `timeout_seconds` was reached before the job finished.

`replay=true` sends already terminal task results before waiting for new ones. Use `replay=false` when reconnecting clients only want completions after the stream opens. The cursor uses `(updated_at, task_id)` and is independent of manager logs.

Example SSE frame:

```text
event: result
data: {"scope":"job","task":{"task_id":"task_001","input_index":0,"input_key":"row-1","status":"succeeded","result":{"ok":true}}}

```

### Cancel job

```http
POST /jobs/{job_id}/cancel
Content-Type: application/json

{}
```

Queued tasks are cancelled. Running tasks are allowed to finish, but completions after cancellation are logged separately.

### Retry all failed tasks in a job

```http
POST /jobs/{job_id}/retry-failed?reset_attempts=true
Content-Type: application/json

{}
```

---

## Service sessions

### List sessions

```http
GET /sessions?limit=100&status=running
```

A session row includes job/task totals and timing:

```json
{
  "id": "sess_def456",
  "name": "Morning Square Run",
  "status": "running",
  "priority": 10,
  "total_jobs": 1,
  "total_tasks": 100,
  "queued_tasks": 80,
  "running_tasks": 4,
  "completed_tasks": 16,
  "failed_tasks": 0,
  "cancelled_tasks": 0,
  "client_id": "client-alpha",
  "resume_token": "rt_...",
  "created_at": "2026-06-26T18:00:00.000+00:00",
  "started_at": "2026-06-26T18:00:01.000+00:00",
  "finished_at": null,
  "metadata": {}
}
```

### Get session

```http
GET /sessions/{session_id}
```


### Client-owned resume/list APIs

Clients can disconnect after submission and later reconnect if they kept the returned `client_id` and `resume_token`.

```http
GET /clients/{client_id}/sessions?resume_token=rt_...&limit=100
```

Returns only sessions owned by that client/token pair.

```http
GET /clients/{client_id}/sessions/{session_id}?resume_token=rt_...
```

Returns one owned session or `404` if the token/client/session do not match.

Notes:

```text
- `POST /jobs` returns `client_id` and `resume_token`.
- New sessions get a generated client id and resume token when omitted.
- Store the resume token like a secret; it allows listing/downloading that client's sessions.
- Existing admin endpoints such as GET /sessions are still unauthenticated in this MVP.
```

### Set session priority

```http
POST /sessions/{session_id}/priority
Content-Type: application/json
```

Request:

```json
{"priority": 20}
```

Behavior:

```text
- Updates service_sessions.priority.
- Updates non-terminal jobs in that session.
- Updates queued tasks in that session.
- Running tasks are not interrupted.
- Completed/failed/cancelled tasks are unchanged.
```

### Pause/resume a session

```http
POST /sessions/{session_id}/pause
Content-Type: application/json

{"reason":"investigating input data"}

POST /sessions/{session_id}/resume
Content-Type: application/json

{}
```

A paused session prevents all queued tasks in that session from being leased. Running tasks may finish. Resuming the session does not override jobs that were individually paused.

Bulk session hold:

```http
POST /sessions/bulk-pause
Content-Type: application/json

{"session_ids":["sess_a","sess_b"],"paused":true,"reason":"maintenance window"}
```

### List jobs in a session

```http
GET /sessions/{session_id}/jobs
```

### Get session results

```http
GET /sessions/{session_id}/results
```

Returns results across every job attached to the session.

### Session assignment drilldown

```http
GET /sessions/{session_id}/assignments?limit=1000
```

Returns a session-scoped execution view with `status_counts`, `running_assignments`, `current_assignments`, `workers`, and `tasks`. Each running assignment includes the job, task, input index/key, worker id, instance id, executor id, lease expiry, attempts, and runtime seconds. Worker rollups include session task counts by status plus per-instance state/current task data. This is the API behind the Session UI assignment drilldown.

---

## Tasks

### List tasks

```http
GET /tasks?limit=100
```

### Get task

```http
GET /tasks/{task_id}
```

### Retry one failed task

```http
POST /tasks/{task_id}/retry?reset_attempts=true
Content-Type: application/json

{}
```

---

---

## Task catalog / capability validation

Workers advertise their loaded task registry on heartbeat. The manager uses this catalog to show which task types are runnable and to warn or reject submissions that no active worker can currently run.

### Read task catalog

```http
GET /task-catalog
GET /task-catalog?task_type=square
GET /task-catalog?task_type=price_risk&required_tags=risk,gpu
```

Example response shape:

```json
{
  "task_type": "square",
  "required_tags": [],
  "workers_total": 2,
  "workers_active": 2,
  "warnings": [],
  "capable_workers": [
    {
      "id": "risk-node-1",
      "active": true,
      "tags": ["risk"],
      "task_types": ["square", "price_risk"],
      "service_name": "risk-worker",
      "service_version": "1.2.0"
    }
  ],
  "task_types": [
    {
      "task_type": "square",
      "active_workers": 2,
      "total_workers": 2,
      "tags": ["cpu", "risk"],
      "workers": []
    }
  ]
}
```

### Strict submit validation

By default, TaskGrid remains permissive: if no capable worker is online, the manager still queues the job but records a `TaskCapabilityWarning` event on the job.

To reject early instead, send:

```json
{
  "name": "strict run",
  "task_type": "price_risk",
  "tasks": [{"trade_id": "T1"}],
  "metadata": {"required_tags": ["risk"]},
  "require_capable_worker": true
}
```

If no active worker advertises that task type and tag set, the manager returns HTTP `409` with capability details.



### Stream session results

```http
GET /sessions/{session_id}/assignments
GET /sessions/{session_id}/results/stream?poll_seconds=0.5&timeout_seconds=0&replay=true
Accept: text/event-stream
```

Streams the same `progress`, `result`, `done`, and `timeout` event types across every job in the service session. Each result includes `job_id` and `job_name` so clients can route completions back to the originating job.


## Queue diagnostics

```http
GET /queue/diagnostics?limit=1000
```

Returns an operator view of queued backlog and schedulability. It does not mutate queue state. It explains each queued task using the same major gates as the lease path: job/session pause state, job cancellation state, advertised task types, required tags, worker disabled/drain/stale state, and estimated slot pressure.

Response shape:

```json
{
  "queued_tasks": 4,
  "leaseable_now": 1,
  "blocked_tasks": 3,
  "reason_counts": {
    "leaseable_now": 1,
    "session_paused": 1,
    "missing_required_tags": 1,
    "no_worker_supports_task_type": 1
  },
  "task_types": [
    {"task_type": "echo", "queued_tasks": 2, "leaseable_now": 1, "blocked_tasks": 1}
  ],
  "tasks": [
    {
      "task_id": "task_...",
      "session_id": "sess_...",
      "job_id": "job_...",
      "task_type": "echo",
      "input_index": 0,
      "input_key": "row-1",
      "reason": "leaseable_now",
      "required_tags": ["cpu"],
      "detail": "At least one active capable worker appears to have a free execution slot."
    }
  ],
  "workers": [
    {
      "worker_id": "node-a",
      "active": true,
      "task_types": ["echo"],
      "tags": ["cpu"],
      "running_slots": 1,
      "active_slots": 4,
      "estimated_free_slots": 3
    }
  ]
}
```

The web UI is available at `/ui/queue`. It is intended for “why is this session/job not moving?” triage and complements `/ui/executors`, which shows what is running now.

## Services registry

```http
GET /services
GET /services?service_name=risk-engine
GET /services?service_name=risk-engine&service_version=1.2.0
```

Returns service/application versions derived from worker heartbeats. Workers normally advertise these values with `TASKGRID_SERVICE_NAME` and `TASKGRID_SERVICE_VERSION` or worker metadata. The registry is grouped by `service_name + service_version` and includes worker counts, active/stale/disabled/draining counts, desired and active instances, running tasks, task types, tags, TaskGrid package versions, and member workers.

This is a runtime registry only; TaskGrid still does not distribute code dynamically. Docker images remain the code/package distribution mechanism.


## Executors

### List all execution slots

```http
GET /executors?limit=1000
```

Returns a manager-wide live view of worker instances. Unlike `GET /sessions/{session_id}/assignments`, this is not scoped to one session. It includes idle, running, drained, disabled, and stale slots.

Response shape:

```json
{
  "checked_at": "2026-06-28T12:00:00+00:00",
  "worker_count": 1,
  "instance_count": 2,
  "running_instances": 1,
  "idle_instances": 1,
  "drained_instances": 0,
  "state_counts": {"running": 1, "idle": 1},
  "instances": [
    {
      "worker_id": "node-a",
      "instance_id": "instance-001",
      "executor_id": "node-a-instance-001",
      "state": "running",
      "service_name": "risk-engine",
      "service_version": "1.2.0",
      "current_task": {
        "task_id": "task_...",
        "session_id": "sess_...",
        "job_id": "job_...",
        "task_type": "price"
      },
      "log_path": "instances/instance-001/worker.log"
    }
  ]
}
```

### Get one execution slot

```http
GET /executors/{worker_id}/instances/{instance_id}?recent_limit=100
```

Returns worker metadata, the selected instance, the current running task if one exists, and recent task history assigned to that instance.

## Workers

### List workers

```http
GET /workers?limit=100
```

Worker metadata includes node status, tags, configured instances, active instances, log URL, current task summaries, heartbeat time, disabled/draining state, and drained instance IDs when available.

### Get worker logs

```http
GET /workers/{worker_id}/logs
```

Example response:

```json
{
  "worker_id": "node-a",
  "files": [
    {"path": "supervisor.log", "size": 2048},
    {"path": "instances/instance-001/worker.log", "size": 8192}
  ]
}
```

### Read a proxied worker log

```http
GET /workers/{worker_id}/logs/{path}?tail=1000000
```

Examples:

```text
GET /workers/node-a/logs/supervisor.log
GET /workers/node-a/logs/instances/instance-001/worker.log
```

The manager proxies this request to the worker node's advertised log API.

### Get worker config

```http
GET /workers/{worker_id}/config
```

### Set worker instance count

```http
POST /workers/{worker_id}/config
Content-Type: application/json
```

Request:

```json
{"desired_concurrency": 5}
```

The field is named `desired_concurrency` for backwards compatibility, but it now means desired single-task instances for that node.

### Drain worker or instance

```http
POST /workers/{worker_id}/drain
POST /workers/{worker_id}/undrain
POST /workers/{worker_id}/instances/{instance_id}/drain
POST /workers/{worker_id}/instances/{instance_id}/undrain
Content-Type: application/json
```

Request body for drain calls:

```json
{"reason": "maintenance window"}
```

Drain mode is a scheduling hold. Running tasks are allowed to finish, but the manager does not issue new leases to the drained worker or drained instance. This differs from disable: disabled workers also receive a heartbeat config that tells the supervisor to scale local instance loops down to zero until re-enabled.

### Purge offline workers

```http
POST /workers/purge-offline?active_seconds=60&include_running=false
```

Deletes stale/offline worker rows and their config rows from the manager registry. This keeps the Workers UI clean after containers or nodes are retired. It does not delete jobs, tasks, results, events, or remote worker log files.

By default, stale workers that still have running task assignments are skipped so operators can recover expired leases first. Set `include_running=true` only when you intentionally want to remove the worker registry row while preserving the running task rows.

Example response:

```json
{
  "active_seconds": 60,
  "include_running": false,
  "purged_count": 1,
  "skipped_count": 0,
  "purged_workers": [
    {"id": "old-node", "running_assignments": 0}
  ],
  "skipped_workers": []
}
```

Events:

```text
WorkerPurgeOfflineRun
WorkerPurgedOffline
```

---

## Maintenance and recovery

### Recovery status

```http
GET /maintenance/status
```

Returns manager-side health and recovery state:

```json
{
  "workers_total": 2,
  "workers_active": 1,
  "workers_stale": 1,
  "running_tasks": 4,
  "expired_running_tasks": 1,
  "expired_tasks": [
    {
      "id": "task_001",
      "job_id": "job_abc",
      "assigned_worker_id": "node-a-instance-001",
      "lease_expires_at": "2026-06-26T18:00:00+00:00"
    }
  ]
}
```

### Reconcile manager state

```http
POST /maintenance/reconcile?recover_expired=true
```

Repairs manager-derived state from durable task rows. The reconcile pass recomputes job counters, session counters, and job/session statuses, then optionally recovers expired running leases. Valid running leases are not touched, so this endpoint is safe after manager restarts.

Example response fields:

```json
{
  "jobs_checked": 12,
  "sessions_checked": 4,
  "jobs_reconciled": 1,
  "sessions_reconciled": 1,
  "lease_recovery": {"expired": 1, "requeued": 1, "failed": 0}
}
```

### Recover expired leases

```http
POST /maintenance/recover
```

Requeues expired running tasks that still have retry attempts left. If retries are exhausted, the task is marked failed. Valid running leases are not touched. Late or duplicate worker results after recovery are ignored and logged as `TaskResultIgnored` or `TaskFailureIgnored`.

Important events:

```text
MaintenanceReconcileRun
MaintenanceRecoveryRun
TaskLeaseExpiredRequeued
TaskLeaseExpiredFailed
TaskResultIgnored
TaskFailureIgnored
```

---

## Events and manager logs

### List structured events

```http
GET /events?limit=200&entity_type=task&entity_id=task_001&code=TaskCompleted
```

All query parameters are optional.

Task tracking is authoritative on the `tasks`, `jobs`, and `service_sessions` rows. The event stream and `manager.log` are operational trace/audit aids, not the source of truth for task status.

Normal mode suppresses high-volume successful task lifecycle events (`TaskAccepted`, `TaskCompleted`) so tiny-task workloads do not spend most of their time writing logs. Enable trace-level task lifecycle events with:

```bash
TASKGRID_EVENT_MODE=debug
# or
TASKGRID_VERBOSE_TASK_EVENTS=1
```

Important event codes include:

```text
JobSubmitted
JobStatusChanged
JobCancelled
TaskFailed
TaskFailedRequeued
TaskLeaseExpiredRequeued
TaskLeaseExpiredFailed
TaskRequeuedManual
TaskCompletedAfterCancellation
TaskResultIgnored
TaskFailureIgnored
TaskCapabilityWarning
MaintenanceRecoveryRun
WorkerConfigInitialized
WorkerConfigUpdated
ServiceSessionCreated
ServiceSessionJobCreated
ServiceSessionStatusChanged
ServiceSessionPriorityChanged
```

Debug-only successful task lifecycle events:

```text
TaskAccepted
TaskCompleted
```

For task lifecycle events, the event data includes the node and instance:

```json
{
  "worker_id": "node-a",
  "instance_id": "instance-003",
  "executor_id": "node-a-instance-003"
}
```

### Read manager JSONL log

```http
GET /manager/log?tail=2000000
```

Returns raw text from `manager.log`. In normal mode this contains operational events; debug mode also includes every successful task accept/complete event.

---

## Worker/internal endpoints

These endpoints are used by TaskGrid workers. They are documented so custom workers can be written, but normal clients should use the public endpoints above.

### Worker heartbeat

```http
POST /workers/heartbeat
Content-Type: application/json
```

Request:

```json
{
  "worker_id": "node-a",
  "hostname": "host-1",
  "status": "idle",
  "current_task_id": null,
  "version": "dev",
  "metadata": {
    "tags": ["risk", "python3.12"],
    "configured_instances": 5,
    "active_instances": 5,
    "log_url": "http://node-a:9201",
    "log_dir": "/logs/node-a"
  }
}
```

### Lease task

```http
POST /tasks/lease
Content-Type: application/json
```

Request:

```json
{
  "worker_id": "node-a",
  "instance_id": "instance-001",
  "limit": 1,
  "lease_seconds": 60
}
```

Idempotent submit retry:

```bash
# First call may commit, then the client may lose the HTTP response if the
# manager is killed or the network drops. Retrying with the same client_id and
# idempotency_key returns the original job instead of creating a duplicate.
curl -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "safe submit",
    "task_type": "square",
    "tasks": [{"x":1}],
    "client_id": "client-alpha",
    "idempotency_key": "safe-submit-001"
  }'
```

Response:

```json
[
  {
    "id": "task_001",
    "job_id": "job_abc123",
    "session_id": "sess_def456",
    "task_type": "square",
    "payload": {"x": 2},
    "priority": 10,
    "attempts": 1,
    "max_retries": 2,
    "metadata": {}
  }
]
```

Workers should request `limit: 1` per single-task instance loop.

### Complete task

```http
POST /tasks/{task_id}/complete
Content-Type: application/json
```

Request:

```json
{
  "worker_id": "node-a",
  "instance_id": "instance-001",
  "result": {"x": 2, "square": 4}
}
```

### Fail task

```http
POST /tasks/{task_id}/fail
Content-Type: application/json
```

Request:

```json
{
  "worker_id": "node-a",
  "instance_id": "instance-001",
  "error": "traceback or error message"
}
```

If attempts remain, the manager requeues the task. Otherwise it marks the task failed.

## Scheduling rules

Current MVP scheduling order:

```text
highest task priority first
then oldest queued task first
```

Tag routing:

```text
job.metadata.required_tags must all be present on the worker node heartbeat metadata tags
```

Session priority affects queued tasks across the session. Job priority can override session priority for a specific job.

## Response/error notes

- Successful JSON endpoints return JSON objects or arrays.
- Log endpoints return plain text.
- Unknown entities return HTTP `404` with a FastAPI detail body.
- Worker log proxy failures return HTTP `502`.
- Authentication is optional and controlled by the `TASKGRID_*_TOKEN` environment variables.


## Scale benchmark utility

The API package includes `scripts/benchmark_scale.py`, which launches a temporary manager, workers, and concurrent clients against the HTTP API. It is intended for local validation of lease throughput, completion throughput, stale-worker state, and expired lease counts. The benchmark supports `--execution-mode process|thread|inline` to separate manager/broker overhead from worker-side process overhead for tiny tasks.

## Result export endpoints

Download full result sets or failed-task reports without scraping the UI.

```text
GET /jobs/{job_id}/results/export?format=json|csv&order=input|started|completed|status&failed_only=false
GET /sessions/{session_id}/results/export?format=json|csv&order=input|started|completed|status&failed_only=false
```

CSV exports include `input_index,input_key,task_id,job_id,...` so clients can map rows back to submitted inputs deterministically.

Examples:

```bash
curl -o job-results.csv "http://127.0.0.1:8000/jobs/job_x/results/export?format=csv"
curl -o failed.csv "http://127.0.0.1:8000/sessions/sess_x/results/export?format=csv&failed_only=true"
```

## Retention endpoints

Preview before deleting:

```text
GET /maintenance/retention/preview?completed_days=30&failed_days=90&event_days=30
```

Apply cleanup:

```text
POST /maintenance/retention/apply?completed_days=30&failed_days=90&event_days=30
```

Optional parameters:

```text
purge_workers_active_seconds=60
manager_log_keep_bytes=2000000
```

Retention deletes terminal sessions/jobs/tasks older than the selected windows and old manager events. It does not delete queued or running work.

## Batch lease endpoint

Workers normally use `POST /tasks/lease` per instance. Larger nodes can reduce manager polling by using:

```text
POST /tasks/lease-batch
```

Payload:

```json
{
  "worker_id": "node-a",
  "instance_ids": ["instance-001", "instance-002", "instance-003"],
  "lease_seconds": 120,
  "wait_seconds": 0
}
```

Each returned task is assigned to a concrete worker instance, for example `node-a-instance-002`, so task tracking remains precise.


## Operator endpoint caching

The heavy operator read endpoints support a short in-process snapshot cache:

```http
GET /queue/diagnostics?refresh=false
GET /executors?refresh=false
GET /services?refresh=false
```

Set `refresh=true` to bypass the cache. Responses include:

```json
{
  "cached": false,
  "cache_age_ms": 0,
  "cache_ttl_ms": 500
}
```

Tune the TTL with `TASKGRID_OPERATOR_CACHE_TTL_MS`. A value of `0` disables the cache. The cache is intentionally short-lived and is meant to reduce repeated UI/API read pressure while the manager is under active lease/complete load.
