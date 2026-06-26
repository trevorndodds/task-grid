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
TaskGridClient(base_url="http://127.0.0.1:8000")
```

`base_url` should point at the manager/broker.

## Submitting work

```python
job = client.submit(
    name="square numbers",
    task_type="square",
    tasks=[{"x": i} for i in range(10)],
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
| `priority` | `int | None` | Optional job-level priority override. |
| `session_priority` | `int | None` | Priority for the service session. |
| `max_retries` | `int` | Automatic retries per task. |
| `metadata` | `dict | None` | Job metadata. Use `required_tags` for worker routing. |
| `session_id` | `str | None` | Attach to an existing session. |
| `session_name` | `str | None` | Name for a new session. |
| `session_metadata` | `dict | None` | Metadata for a new session. |

Return value is the created job object, including `id` and `session_id`.

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
results = client.results(job["id"])
```

### Get session results

```python
session_results = client.session_results(job["session_id"])
```

Result entries include payload, result/error, status, worker node, instance ID, executor ID, attempts, and timing.

## Jobs

```python
job = client.get_job(job_id)
tasks = client.tasks(job_id)
results = client.results(job_id)
```

Cancel a job:

```python
client.cancel(job_id)
```

Retry failed tasks in a job:

```python
client.retry_failed(job_id, reset_attempts=True)
```

Retry a single task:

```python
client.retry_task(task_id, reset_attempts=True)
```

## Service sessions

List sessions:

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

Behavior:

```text
- queued work in the session is promoted/demoted
- running tasks are not interrupted
- completed tasks are unchanged
- new jobs attached to the session inherit the session priority unless overridden
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

Backwards-compatible alias:

```python
client.set_worker_concurrency("node-a", 5)
```

The value now means desired single-task instances, not multi-task execution inside one instance.

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
client.worker_logs(worker_id)
client.worker_log(worker_id, filename, tail_bytes=1_000_000)

client.wait(job_id, poll_seconds=1.0, timeout_seconds=None)
```

## Limitations

- The SDK currently uses Python standard-library `urllib` and raises underlying HTTP/URL exceptions.
- It does not yet expose typed response classes.
- It does not yet include authentication headers because authentication has not been added to the manager.
