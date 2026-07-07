# TaskGrid Python SDK Reference

The SDK is a small dependency-free wrapper around the manager REST API.

```python
from taskgrid.sdk import TaskGridClient

client = TaskGridClient("http://127.0.0.1:8000")
```

## Install/use locally

From the repository root:

```bash
pip install -r requirements.txt
```

Then run a manager and worker:

```bash
uvicorn taskgrid.app:app --reload
python -m taskgrid.worker --module examples.custom_tasks --worker-id node-a --instances 4
```

## Client constructor

```python
TaskGridClient(
    base_url="http://127.0.0.1:8000",
    client_id=None,
    resume_token=None,
    api_token=None,
)
```

`base_url` should point at the manager/broker. `client_id` and `resume_token` are optional reconnect credentials. `api_token` is sent as `X-TaskGrid-Token` when the manager has client/admin auth enabled. The SDK stores generated reconnect credentials after the first submit response.

## Submitting work

```python
job = client.submit(
    name="square numbers",
    task_type="square",
    tasks=[{"x": i} for i in range(10)],
    input_keys=[f"row-{i}" for i in range(10)],
    idempotency_key=client.new_idempotency_key("square"),
    session_name="Square Run",
    session_priority=10,
    max_retries=2,
)
```

Parameters:

| Parameter | Type | Notes |
|---|---|---|
| `name` | `str` | Human-readable job name. |
| `task_type` | `str` | Registered task type on the worker. |
| `tasks` | `list[dict]` | One payload dict per task. |
| `input_keys` | `list[str | int] | None` | Optional stable key per payload. If omitted, keys are derived from payload `id`, `input_key`, or `key` when present. |
| `idempotency_key` | `str | None` | Optional stable submit key. Reusing the same `client_id` + key returns the existing job after ambiguous failures. |
| `priority` | `int | None` | Optional job-level priority override. |
| `session_priority` | `int | None` | Priority for the service session. |
| `max_retries` | `int` | Automatic retries per task. |
| `metadata` | `dict | None` | Job metadata. Use `required_tags` for worker routing. |
| `session_id` | `str | None` | Attach to an existing session. |
| `session_name` | `str | None` | Name for a new session. |
| `session_metadata` | `dict | None` | Metadata for a new session. |
| `client_id` | `str | None` | Override the client's owner id for this submission. |
| `resume_token` | `str | None` | Resume token for securely attaching to an existing owned session. |
| `require_capable_worker` | `bool` | Reject early when no capable worker is online. |

Safe retry after a lost submit response:

```python
client = TaskGridClient("http://127.0.0.1:8000", client_id="client-alpha")
submit_key = client.new_idempotency_key("daily-square")

try:
    job = client.submit("daily square", "square", [{"x": 1}], idempotency_key=submit_key)
except OSError:
    # The manager may have committed before the connection died. Retry with the
    # same key to recover the existing job instead of creating a duplicate.
    job = client.submit("daily square", "square", [{"x": 1}], idempotency_key=submit_key)
```

Return value is the created job object, including `id`, `session_id`, `client_id`, and `resume_token`.

### Submit with worker tags

```python
job = client.submit(
    name="GPU risk run",
    task_type="risk_price",
    tasks=[{"trade_id": "T1"}],
    metadata={"required_tags": ["gpu", "risk-v1.1.0"]},
)
```

### Attach more work to an existing session

```python
first = client.submit(
    name="batch 1",
    task_type="square",
    tasks=[{"x": 1}],
    session_name="Large Client Run",
    session_priority=5,
)

second = client.submit(
    name="batch 2",
    task_type="square",
    tasks=[{"x": 2}],
    session_id=first["session_id"],
)
```

## Waiting and results

### Wait for a job

```python
job = client.wait(job["id"], poll_seconds=1.0, timeout_seconds=300)
```

Returns the final job object when the job reaches `succeeded`, `failed`, or `cancelled`.

Raises `TimeoutError` if `timeout_seconds` is supplied and exceeded.

### Get job results

```python
results = client.results(job["id"], order="input")
```

### Get session results

```python
session_results = client.session_results(job["session_id"], order="input")
assignments = client.session_assignments(job["session_id"])

# Finished-session/task audit drilldown.
history = client.session_task_history(job["session_id"], status="failed")
```

`session_assignments(...)` returns the same live assignment drilldown used by the Session UI: running tasks by worker instance, worker/instance rollups, task status counts, and task/job/session mapping.

Result entries include `input_index`, `input_key`, payload, result/error, status, assigned worker instance, attempts, and timing. `order="input"` is the default and remains deterministic even when tasks finish out of order. `order` can also be `started`, `completed`, or `status`.


### Stream results

```python
for event in client.stream_results(job["id"], timeout_seconds=60):
    if event["_event"] == "progress":
        print(event["summary"]["status"], event["summary"].get("completed_tasks"))
    elif event["_event"] == "result":
        task = event["task"]
        print(task["input_index"], task["input_key"], task["status"], task.get("result"))
    elif event["_event"] in {"done", "timeout"}:
        break

for event in client.stream_session_results(job["session_id"], replay=True):
    ...
```

Streams use Server-Sent Events underneath and yield decoded dictionaries. The `_event` key contains `progress`, `result`, `done`, or `timeout`. `replay=True` is the default, so terminal results that finished before the client connected are sent first.


## Queue diagnostics

```python
diag = client.queue_diagnostics(limit=1000)
print(diag["reason_counts"])
```

`queue_diagnostics(...)` returns the same backlog explanation used by `/ui/queue`. It is useful when a client has submitted work but tasks remain queued. Reasons include paused sessions/jobs, missing task type support, missing required tags, disabled/draining/stale workers, and all capable workers being busy.

## Jobs

```python
job = client.get_job(job_id)
tasks = client.tasks(job_id)
results = client.results(job_id)
```

Cancel a job:

```python
client.cancel(job_id)                 # graceful: queued tasks cancel, running tasks finish
client.cancel(job_id, mode="force")   # immediate/force cancel
```

Pause or resume a job without cancelling running tasks:

```python
client.pause_job(job_id, reason="hold for review")
client.resume_job(job_id)
```

Retry failed tasks in a job:

```python
client.retry_failed(job_id, reset_attempts=True)
```

Retry a single task:

```python
client.retry_task(task_id, reset_attempts=True)
```

## Client reconnect

After the first submission, keep the returned reconnect credentials:

```python
job = client.submit(
    name="Risk Run",
    task_type="square",
    tasks=[{"x": 1}],
    session_name="Morning Run",
)

client_id = job["client_id"]
resume_token = job["resume_token"]
```

Later, from a new process:

```python
client = TaskGridClient(
    "http://127.0.0.1:8000",
    client_id=client_id,
    resume_token=resume_token,
)

sessions = client.my_sessions()
resumed = client.reconnect_session(job["session_id"])
results = client.session_results(job["session_id"])
```

Or attach credentials after constructing the client:

```python
client.attach(client_id, resume_token)
```


`reconnect_session(...)` is the reconnect/resume-token path. `resume_session(...)` is reserved for unpausing a paused service session.

## Service sessions

List all sessions:

```python
sessions = client.sessions()
```

Get one session:

```python
session = client.get_session(session_id)
```

List jobs in a session:

```python
jobs = client.session_jobs(session_id)
```

Set session priority:

```python
client.set_session_priority(session_id, 20)
```

Pause/resume a session:

```python
client.pause_session(session_id, reason="maintenance")
client.resume_session(session_id)
client.set_sessions_paused([session_id], paused=True, reason="bulk hold")
```

Pause holds queued work without cancelling running tasks. Resuming a session does not override jobs that were individually paused.

Behavior:

```text
- queued work in the session is promoted/demoted
- running tasks are not interrupted
- completed tasks are unchanged
- new jobs attached to the session inherit the session priority unless overridden
```

## Executor instance dashboard

Use the executor helpers for the manager-wide instance view. This is separate from `session_assignments(...)`, which only shows tasks belonging to one session.

```python
summary = client.executors()
for slot in summary["instances"]:
    print(slot["worker_id"], slot["instance_id"], slot["state"], slot.get("current_task"))

detail = client.executor("node-a", "instance-001", recent_limit=100)
print(detail["current_task"])
print(detail["recent_tasks"])
```

## Workers

List workers:

```python
workers = client.workers()
```

Get worker config:

```python
config = client.worker_config("node-a")
```

Set desired worker instances:

```python
client.set_worker_instances("node-a", 5)
```

Disable or re-enable workers:

```python
client.disable_worker("node-a", reason="maintenance")
client.enable_worker("node-a")

client.disable_workers(["node-a", "node-b"], reason="maintenance window")
client.enable_workers(["node-a", "node-b"])
```

Drain a worker without scaling the supervisor down, or drain one logical instance slot:

```python
client.drain_worker("node-a", reason="maintenance")
client.undrain_worker("node-a")
client.drain_worker_instance("node-a", "instance-003", reason="slot investigation")
client.undrain_worker_instance("node-a", "instance-003")
```

Disabled and drained workers do not receive new leases. Running tasks are allowed to finish, and the manager keeps task history/results/log references. Use disable when the node should scale local instance loops down to zero; use drain when the node should stay up but stop receiving new work.

Backwards-compatible alias:

```python
client.set_worker_concurrency("node-a", 5)
```

The value now means desired single-task instances, not multi-task execution inside one instance.

## Maintenance and recovery

Check manager-side recovery state:

```python
status = client.recovery_status()
print(status["workers_stale"], status["expired_running_tasks"])
```

Reconcile manager state after a restart or suspected counter drift:

```python
summary = client.reconcile_manager()
print(summary["jobs_reconciled"], summary["sessions_reconciled"])
```

This recomputes job/session counters and statuses from task rows. By default it also recovers expired task leases. Valid running leases are left alone, so this is safe to run after manager restarts.

Recover expired task leases only:

```python
summary = client.recover_expired_leases()
print(summary["requeued"], summary["failed"])
```

This only touches running tasks whose leases have already expired. Late results from old workers are ignored and logged by the manager.

Purge stale/offline worker registry rows:

```python
summary = client.purge_offline_workers(active_seconds=60)
print(summary["purged_count"], summary["skipped_count"])
```

By default, workers with running task assignments are skipped so you can recover expired leases first. Pass `include_running=True` only when you intentionally want to remove the stale worker record while preserving task history.

## Worker logs

List proxied logs for a worker node:

```python
logs = client.worker_logs("node-a")
```

Read a log:

```python
text = client.worker_log("node-a", "supervisor.log")
text = client.worker_log("node-a", "instances/instance-001/worker.log")
```

The manager proxies the request to the worker's advertised log API.

## Full example

```python
from taskgrid.sdk import TaskGridClient

client = TaskGridClient("http://127.0.0.1:8000")

job = client.submit(
    name="square numbers",
    task_type="square",
    tasks=[{"x": i} for i in range(20)],
    session_name="SDK Demo",
    session_priority=10,
    max_retries=2,
)

print("job", job["id"])
print("session", job["session_id"])

final_job = client.wait(job["id"], poll_seconds=0.5, timeout_seconds=60)
print("final status", final_job["status"])

for row in client.results(job["id"])["results"]:
    print(row["id"], row["status"], row.get("result"), row.get("executor_id"))
```

## Current SDK methods

```python
client.submit(...)
client.get_job(job_id)
client.tasks(job_id)
client.results(job_id)
client.cancel(job_id)
client.retry_failed(job_id, reset_attempts=True)
client.retry_task(task_id, reset_attempts=True)

client.sessions()
client.get_session(session_id)
client.set_session_priority(session_id, priority)
client.session_jobs(session_id)
client.session_results(session_id)

client.task_catalog(task_type=None, required_tags=None)
client.workers()
client.worker_config(worker_id)
client.set_worker_instances(worker_id, desired_instances)
client.set_worker_concurrency(worker_id, desired_concurrency)  # alias
client.disable_worker(worker_id, reason=None)
client.enable_worker(worker_id, reason=None)
client.drain_worker(worker_id, reason=None)
client.undrain_worker(worker_id, reason=None)
client.drain_worker_instance(worker_id, instance_id, reason=None)
client.undrain_worker_instance(worker_id, instance_id)
client.disable_workers(worker_ids, reason=None)
client.enable_workers(worker_ids, reason=None)
client.set_workers_disabled(worker_ids, disabled, reason=None)
client.worker_logs(worker_id)
client.worker_log(worker_id, filename, tail_bytes=1_000_000)
client.purge_offline_workers(active_seconds=None, include_running=False)

client.recovery_status()
client.reconcile_manager(recover_expired=True)
client.recover_expired_leases()

client.wait(job_id, poll_seconds=1.0, timeout_seconds=None)
```

## Limitations

- The SDK currently uses Python standard-library `urllib` and raises underlying HTTP/URL exceptions.
- It does not yet expose typed response classes.
- It supports TaskGrid token authentication through `api_token` on `TaskGridClient`.

## Export helpers

```python
client.export_results(job_id, format="json")
client.export_results(job_id, format="csv")
client.export_results(job_id, format="csv", failed_only=True)

client.export_session_results(session_id, format="json")
client.export_session_results(session_id, format="csv")
```

These return `bytes`, so callers can write them to a file:

```python
Path("results.csv").write_bytes(client.export_session_results(session_id, format="csv"))
```

## Retention helpers

```python
preview = client.retention_preview(completed_days=30, failed_days=90, event_days=30)
summary = client.apply_retention_cleanup(completed_days=30, failed_days=90, event_days=30)
```

Cleanup only removes terminal sessions/jobs/tasks older than the selected windows. Running and queued work is preserved.


## Service registry

Workers advertise service/application metadata through heartbeat fields such as `service_name`, `service_version`, task types, and tags. The SDK can read the manager's derived registry:

```python
services = client.services()
current = client.services(service_name="risk-engine", service_version="1.2.0")
```

Each service summary includes active/total/stale/disabled/draining worker counts, desired and active instances, running task count, task types, tags, TaskGrid package versions, and the worker IDs currently advertising that service version.


### Session task history

Use `client.session_task_history(...)` when a finished session needs task-by-task drilldown. It is history-focused and separate from `client.session_assignments(...)`, which is optimized for current live placement.


## Bulk task actions

Use bulk task actions when a drilldown or diagnostic view identifies a set of tasks that should be retried or cancelled.

```python
client.retry_session_tasks(session_id, statuses=["failed", "cancelled"])
client.cancel_session_tasks(session_id, statuses=["queued"])
client.cancel_session_tasks(session_id, statuses=["running"], include_running=True)

client.retry_job_tasks(job_id)
client.cancel_job_tasks(job_id, statuses=["queued"])

client.bulk_task_action(
    "retry",
    session_id=session_id,
    statuses=["failed"],
    reset_attempts=True,
    limit=5000,
)
```

Force-cancelling running tasks marks them cancelled immediately and preserves the executor assignment for history. If the old worker later posts a completion, TaskGrid ignores that late result.



## Operator snapshot refresh

The heavier operator helpers accept `refresh=True` to bypass TaskGrid's short in-process dashboard cache:

```python
client.queue_diagnostics(refresh=True)
client.executors(refresh=True)
client.services(refresh=True)
```

Without `refresh=True`, these calls may return a snapshot from the last few hundred milliseconds. The response includes `cached`, `cache_age_ms`, and `cache_ttl_ms`.
