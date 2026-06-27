# TaskGrid

A tiny, boring, useful distributed task manager for grid-style workloads:

- submit a service session/job made of many independent tasks
- workers lease tasks from a broker
- tasks run in parallel across nodes by running multiple single-task worker instances
- jobs can require worker capability tags such as `gpu` or `risk-model-v2`
- workers advertise task types and the manager exposes a task catalog / capability checker
- failed tasks retry automatically, and operators can manually retry failed tasks
- expired leases can be recovered when a worker dies mid-task
- stale workers and stuck/expired task leases are visible from the Recovery UI
- workers can be disabled/enabled from the manager so nodes can be drained without deleting history
- sessions and individual jobs can be paused/resumed so queued work is held without cancelling running tasks
- each single-task instance writes to its own instance folder, exposed by the node log API
- the broker proxies worker logs so operators can view them from the manager UI
- service sessions group client submissions and show job/task counts, pending/running/completed totals, start time, and end time
- results and logs are queryable
- durable task/job/session rows are the source of truth for tracking; verbose successful task lifecycle events are debug-only
- the broker writes a manager-side JSONL audit log for operational events such as submissions, warnings, failures, recovery, and admin actions
- a lightweight server-rendered web UI shows jobs, tasks, workers, events, results, retry controls, recovery state, logs, and a submit form

This is **not** a drop-in clone of any enterprise grid product. It is a clean minimal distributed task runner.

## Documentation

Detailed API and SDK docs are included in the zip:

```text
docs/API.md
docs/SDK.md
```

When the manager is running, FastAPI also exposes interactive docs at:

```text
/docs
/redoc
/openapi.json
```

## Operational controls

TaskGrid separates scheduling holds from cancellation:

```text
Pause session/job   -> queued work stays queued, no new leases are issued
Resume session/job  -> queued work becomes eligible again
Cancel job          -> queued work is cancelled; running work may finish gracefully
Disable worker      -> node keeps history/logs but receives no new leases
```

Pause is useful when a client run should wait behind maintenance, investigation, or a priority decision. It does not delete results, cancel running work, or reset attempts.

## Architecture

```text
Client SDK / REST API
        ↓
Service Session + Job submission
        ↓
FastAPI broker
        ↓
SQLite session/job/task state store + optional event/manager.log trace
        ↓
HTTP polling worker instances
        ↓
Python task registry
```

SQLite is used on purpose for the MVP. It keeps the first version easy to run. The next natural upgrade is Postgres + Redis/RabbitMQ.

## Install

```bash
cd taskgrid
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run locally

Terminal 1:

```bash
uvicorn taskgrid.app:app --reload
```

Terminal 2:

```bash
python -m taskgrid.worker --module examples.custom_tasks --worker-id node-a --instances 2
```

By default the manager writes a broker-side JSONL operational log beside the SQLite database:

```text
manager.log
```

Normal mode records important operational events such as `JobSubmitted`, capability warnings, failures, recovery actions, ignored late results, and admin/config changes. Successful high-volume per-task events such as `TaskAccepted` and `TaskCompleted` are intentionally suppressed in normal mode because the task rows already store authoritative tracking fields: status, assigned worker instance, attempts, start time, finish time, result, and error.

Enable verbose task lifecycle tracing only when debugging:

```bash
export TASKGRID_EVENT_MODE=debug
# or
export TASKGRID_VERBOSE_TASK_EVENTS=1
```

In debug mode, manager log lines include stable event codes and execution slots such as:

```json
{"code":"TaskAccepted","entity_type":"task","entity_id":"task_...","data":{"worker_id":"node-a","instance_id":"instance-001","executor_id":"node-a-instance-001"}}
{"code":"TaskCompleted","entity_type":"task","entity_id":"task_...","data":{"worker_id":"node-a","instance_id":"instance-001","executor_id":"node-a-instance-001"}}
```

Override the manager log path with:

```bash
export TASKGRID_MANAGER_LOG=/var/log/taskgrid/manager.log
```

View it from the web UI or raw API:

```text
/ui/manager/log          # parsed table view with raw toggle
/ui/manager/log?view=raw # raw web UI view
/manager/log             # raw API/text endpoint
```

By default the worker writes logs under:

```text
taskgrid_logs/node-a/
```

and starts a tiny local log API on a free port. The worker node advertises that log URL to the broker in its heartbeat, so `/ui/workers` can show a **Logs** button for the node and its instance logs.

Tagged worker example:

```bash
python -m taskgrid.worker --module examples.custom_tasks --instances 4 --tag gpu --tag risk-model-v2
```

A worker node supervises logical single-task instances. `--instances 4` means the node can run four tasks at the same time, but each instance runs only one task and writes to its own `worker.log`. `--concurrency` is kept as a deprecated alias for `--instances`.

Worker execution modes:

```bash
--execution-mode process  # default; process isolation for CPU-heavy work
--execution-mode thread   # lower overhead for trusted tiny/I/O-heavy work
--execution-mode inline   # lowest overhead, no isolation; trusted code only
```

For realistic compute tasks, keep `process`. For overload tests with thousands of tiny tasks, `thread` avoids process-management overhead so the benchmark measures the manager/broker path more directly.

Terminal 3:

```bash
python examples/submit_demo.py
```

Open the web UI:

```text
http://127.0.0.1:8000/ui
```

`/admin` redirects to `/ui` for convenience.



## Optional security

TaskGrid is open by default for local development. To protect a manager, set one or more tokens before starting it:

```bash
export TASKGRID_ADMIN_TOKEN=admin-secret
export TASKGRID_CLIENT_TOKEN=client-secret
export TASKGRID_WORKER_TOKEN=worker-secret
export TASKGRID_UI_TOKEN=ui-secret
```

Tokens are supplied with `X-TaskGrid-Token` or `Authorization: Bearer ...`. The admin token is accepted for all protected API routes. Client tokens cover job/session/result APIs. Worker tokens cover heartbeat, lease, complete, and fail APIs. UI tokens protect `/ui`; if only `TASKGRID_ADMIN_TOKEN` is set, the admin token can also be used on the UI login screen.

Worker example:

```bash
python -m taskgrid.worker   --broker http://127.0.0.1:8000   --worker-token worker-secret   --module examples.custom_tasks   --instances 4
```

SDK example:

```python
client = TaskGridClient("http://127.0.0.1:8000", api_token="client-secret")
```

For production, set all three API tokens and a UI token, run the manager behind TLS, and avoid exposing worker log APIs directly to untrusted networks.

## Cancellation behavior

TaskGrid cancellation is graceful by default:

```text
queued tasks    -> cancelled immediately
running tasks   -> allowed to finish
job/session     -> cancelling until running tasks finish
late results    -> do not revive cancelled work
```

API/SDK examples:

```bash
POST /jobs/{job_id}/cancel?mode=graceful
POST /jobs/{job_id}/cancel?mode=force
```

```python
client.cancel(job_id)                 # graceful
client.cancel(job_id, mode="force")   # immediate/force cancel
```

The UI exposes both **Graceful Cancel** and **Force Cancel** on the job detail page.

## Web UI

The MVP now includes a simple web UI with no frontend build system:

```text
/ui                 dashboard
/ui/sessions              service session list with task totals and timing
/ui/sessions/{id}         session detail, created jobs, counts, start/end time, events
/ui/sessions/{id}/results session-level task results view
/ui/jobs                  job list and filters
/ui/jobs/{job_id}         job detail, tasks, events, cancel and retry-failed buttons
/ui/jobs/{job_id}/results job-level task results view
/ui/tasks/{task_id}       task payload/result/error detail and retry button
/ui/workers               worker heartbeat/status list plus per-node instance-count config
/ui/workers/{id}/logs     worker log file list proxied through the broker
/ui/manager/data-flow     manager-side client → broker → worker → result flow explanation
/ui/submit                submit a job from the browser
```

The dashboard and running job pages auto-refresh every 5 seconds. It is intentionally plain server-rendered HTML so the project stays dependency-light.


## Service sessions

Every client submission creates a service session by default. A session groups the created job and exposes the operational view you would expect from a small grid-style manager:

```text
session id / name / status
session priority
created job count
pending tasks
running tasks
completed tasks
failed tasks
total tasks
created time
started time
finished time
```

API endpoints:

```text
GET /sessions
GET /sessions/{session_id}
POST /sessions/{session_id}/priority
GET /sessions/{session_id}/jobs
GET /sessions/{session_id}/results
```

A job response includes `session_id`, `client_id`, and `resume_token`. To attach another job to an existing session, pass `session_id` when submitting. To name a new session explicitly, pass `session_name`:

```python
client.submit(
    name="risk batch",
    task_type="square",
    tasks=[{"x": 2}, {"x": 3}],
    session_name="Risk Run 2026-06-26",
    session_priority=10,
)

# Later, promote queued work in the whole session.
client.set_session_priority(job["session_id"], 20)
```

The web UI exposes this at:

```text
/ui/sessions
/ui/sessions/{session_id}
/ui/client-sessions
```

Clients can disconnect after submit and later reconnect if they kept their `client_id` and `resume_token`:

```python
job = client.submit(
    name="risk batch",
    task_type="square",
    tasks=[{"x": 2}],
    session_name="Risk Run",
)

client_id = job["client_id"]
resume_token = job["resume_token"]

# Later, possibly in a new process:
client = TaskGridClient("http://127.0.0.1:8000", client_id=client_id, resume_token=resume_token)
print(client.my_sessions())
print(client.session_results(job["session_id"]))
```

Session priority controls queued work across the session. Changing priority updates the session, non-terminal jobs in that session, and queued tasks. Running tasks are not interrupted; completed tasks are unchanged. Job-level `priority` remains available as an override, but if omitted, jobs inherit the session priority.


## Data and result flow

TaskGrid moves task **data**, not task code. Worker nodes already have code loaded by `--module`; the broker only hands out JSON payloads.

```text
Client POST /jobs
  -> broker creates service session, job, and queued task rows
  -> worker instance POST /tasks/lease
  -> broker returns one task with task_type + payload JSON
  -> engine runs run_task(task_type, payload) locally
  -> worker POST /tasks/{task_id}/complete with JSON result
  -> client reads GET /jobs/{job_id}/results or GET /sessions/{session_id}/results
```

The manager UI exposes this at:

```text
/ui/manager/data-flow
/ui/jobs/{job_id}/results
/ui/sessions/{session_id}/results
```

The result endpoints include task status, worker assignment, payload, payload size, result/error, result size, attempts, and timing.

## Worker instances and node config

Start a worker node with local single-task instances:

```bash
python -m taskgrid.worker --module examples.custom_tasks --worker-id node-a --instances 5
```

This means `node-a` can run up to five tasks at the same time, but not inside one shared worker instance. The node manages five logical instances: `instance-001` through `instance-005`. Each instance can have only one active task and writes to its own log folder.

You can change a node's desired instance count from the UI:

```text
/ui/workers
```

Change the desired instance count, click **Apply**, and the broker stores it in `worker_configs` using the existing `desired_concurrency` column for API compatibility. The worker node picks up the new value on heartbeat. Increasing the value starts more independent single-task instance loops immediately. Decreasing the value asks the extra instances to stop after their current task finishes, so in-flight work is not intentionally killed.

API equivalents:

```text
GET  /workers/{worker_id}/config
POST /workers/{worker_id}/config
```

```json
{"desired_concurrency":5}
```



## Polling and connection behavior

Each logical instance owns its own simple loop:

```text
lease one pending task → run it → return result/failure → lease next task
```

The node supervisor owns one shared broker HTTP client with a small bounded connection pool. Instance loops use that shared client rather than creating their own sessions. Idle instances use backoff and jitter, so many idle instances do not all poll the broker at the same instant. Useful knobs:

```bash
python -m taskgrid.worker \
  --module examples.custom_tasks \
  --worker-id node-a \
  --instances 5 \
  --poll-seconds 1.0 \
  --heartbeat-seconds 2.0 \
  --broker-pool-size 4
```

For this MVP, capacity should be scaled deliberately: `--instances 5` means five independent single-task loops on that node, not unbounded connection/thread creation.

## Per-node and per-instance logs

Each worker node owns one node folder with a supervisor log plus one folder per logical instance:

```text
taskgrid_logs/<worker-id>/
  supervisor.log
  instances/
    instance-001/
      worker.log
    instance-002/
      worker.log
```

`supervisor.log` records node lifecycle events, broker connectivity, instance startup/shutdown, task completion summaries, failures, and instance-count resize events. Each instance `worker.log` embeds bounded `TASK ... START/END` sections for the tasks that ran on that instance. Because each instance can run only one task at a time, no task-level file lock is needed for the instance log.

The main worker node process exposes the whole node log folder through a small local HTTP log API:

```text
GET /health
GET /logs
GET /logs/{path}?tail=1000000
```

The broker/manager proxies those logs through:

```text
GET /workers/{worker_id}/logs
GET /workers/{worker_id}/logs/{path}
```

The UI surfaces this from:

```text
/ui/workers
/ui/workers/{worker_id}/logs
```

For a worker running on another machine, bind the log API and advertise a reachable URL:

```bash
python -m taskgrid.worker   --broker http://manager-host:8000   --worker-id node-b   --module examples.custom_tasks   --instances 5   --log-api-host 0.0.0.0   --log-api-port 9201   --log-public-url http://node-b-host:9201
```

For local development, the defaults are enough.

## Run with Docker

TaskGrid now supports three Docker layouts:

```text
1. Fully containerized stack: manager + workers in Docker
2. Manager-only container: manager in Docker, workers elsewhere
3. Worker-only container: manager outside Docker, workers in Docker
```

### 1. Manager and workers all in Docker

This is the preferred local/dev setup. It starts:

```text
manager          FastAPI broker + web UI + SQLite DB + manager.log
risk-worker      example worker node with 5 single-task instances
pricing-worker   example worker node with 2 single-task instances
```

Start everything:

```bash
docker compose up --build
```

Open the UI:

```text
http://127.0.0.1:8000/ui
```

The manager is the only service exposed to the host. Workers stay on the internal Docker network. The manager reaches worker logs through Docker DNS:

```text
manager -> http://risk-worker:9201
manager -> http://pricing-worker:9201
```

Persistent local folders:

```text
taskgrid-data/
  taskgrid.db
  manager.log

worker-logs/
  risk-node-1/
  pricing-node-1/
```

The compose file uses `--instances`, not the deprecated `--concurrency` alias.

You can change instance counts when starting the stack:

```bash
RISK_INSTANCES=8 PRICING_INSTANCES=4 docker compose up --build
```

Or use the worker page in the UI to change a node's desired instance count while it is running.

Submit a demo job from your host:

```bash
python examples/submit_demo.py
```

Equivalent client endpoint:

```text
http://127.0.0.1:8000
```

### 2. Manager-only container

Use this when you want the manager/UI in Docker, but workers run elsewhere.

Start the manager:

```bash
docker compose -f docker-compose.manager.yml up --build
```

Open:

```text
http://127.0.0.1:8000/ui
```

A worker on another machine/container should connect to the manager using an address it can reach:

```bash
python -m taskgrid.worker \
  --broker http://manager-host:8000 \
  --worker-id remote-node-1 \
  --module examples.custom_tasks \
  --instances 5 \
  --log-api-host 0.0.0.0 \
  --log-api-port 9201 \
  --log-public-url http://remote-node-1:9201
```

Important: `--log-public-url` must be reachable **from the manager container** so the UI can proxy logs.

### 3. Docker worker with manager running on the host

Start the manager directly on your host first. It must bind to `0.0.0.0` so containers can reach it:

```bash
uvicorn taskgrid.app:app --host 0.0.0.0 --port 8000
```

Then start a worker-only Docker node:

```bash
docker compose -f docker-compose.worker.yml up --build
```

The worker-only compose file defaults to:

```text
MANAGER_URL=http://host.docker.internal:8000
WORKER_ID=docker-node-1
INSTANCES=5
LOG_API_PORT=9201
LOG_PUBLIC_URL=http://127.0.0.1:9201
```

That split is intentional:

```text
worker container -> manager API: http://host.docker.internal:8000
manager host -> worker logs: http://127.0.0.1:9201
```

For Linux Docker, `docker-compose.worker.yml` includes `host.docker.internal:host-gateway`. For a remote manager or remote Docker host, override the URLs with addresses each side can actually reach.

Examples:

```bash
# Same host, different log port / worker ID
WORKER_ID=docker-node-2 \
INSTANCES=8 \
LOG_API_PORT=9202 \
LOG_PUBLIC_URL=http://127.0.0.1:9202 \
docker compose -f docker-compose.worker.yml up --build
```

```bash
# Docker host is a different machine from the manager
MANAGER_URL=http://manager-host:8000 \
WORKER_ID=remote-docker-node-1 \
INSTANCES=8 \
LOG_API_PORT=9201 \
LOG_PUBLIC_URL=http://docker-host:9201 \
docker compose -f docker-compose.worker.yml up --build
```

Add worker tags through `WORKER_TAGS`:

```bash
WORKER_TAGS="--tag risk --tag risk-v1.0.0" \
docker compose -f docker-compose.worker.yml up --build
```

You can also run the equivalent `docker run` command manually:

```bash
docker build -t taskgrid .

docker run --rm \
  --name taskgrid-worker-1 \
  --add-host=host.docker.internal:host-gateway \
  -p 9201:9201 \
  -v "$(pwd)/worker-logs/docker-node-1:/logs" \
  taskgrid \
  python -m taskgrid.worker \
    --broker http://host.docker.internal:8000 \
    --worker-id docker-node-1 \
    --module examples.custom_tasks \
    --instances 5 \
    --log-root /logs \
    --log-api-host 0.0.0.0 \
    --log-api-port 9201 \
    --log-public-url http://127.0.0.1:9201
```

### Docker networking cheat sheet

```text
All in Docker:
  worker -> manager: http://manager:8000
  manager -> worker logs: http://risk-worker:9201

Manager on host, worker in Docker:
  worker -> manager: http://host.docker.internal:8000
  manager -> worker logs: http://127.0.0.1:9201

Manager in Docker, worker on remote host:
  worker -> manager: http://manager-host:8000
  manager -> worker logs: http://worker-host:9201
```



## Define tasks

Create a module and register functions with `@task`:

```python
from taskgrid.tasks import task

@task("square")
def square(payload):
    x = payload["x"]
    return {"x": x, "square": x * x}
```

Start workers with that module:

```bash
python -m taskgrid.worker --module my_tasks
```

Start a worker with capability tags:

```bash
python -m taskgrid.worker --module my_tasks --tag gpu --tag risk-model-v2
```

Then submit a job that requires those tags by adding metadata:

```python
client.submit(
    name="gpu-only job",
    task_type="square",
    tasks=[{"x": 2}],
    metadata={"required_tags": ["gpu"]},
)
```

A worker must have all required tags to lease that job's tasks. Untagged jobs can run anywhere.

Or use an environment variable:

```bash
TASKGRID_TASK_MODULES=my_tasks python -m taskgrid.worker
```

## Submit via SDK

```python
from taskgrid.sdk import TaskGridClient

client = TaskGridClient("http://127.0.0.1:8000")
job = client.submit(
    name="square numbers",
    task_type="square",
    tasks=[{"x": i} for i in range(100)],
    input_keys=[f"row-{i}" for i in range(100)],
    idempotency_key=client.new_idempotency_key("square"),
    session_name="Square Run",
    session_priority=10,
    max_retries=2,
)
client.wait(job["id"])
print(client.results(job["id"]))
```

Each submitted payload is stored with a durable `input_index` and optional `input_key`. Pass `input_keys` when the client already has stable row IDs; otherwise TaskGrid derives a key from payload `id`, `input_key`, or `key` when present. Result APIs and CSV/JSON exports default to `order=input`, so clients can map results back to their original input list even when workers complete tasks out of order.

For crash-safe submit retries, pass a stable `client_id` and `idempotency_key`. If the manager commits a job but the client loses the response, retrying with the same key returns the original durable job instead of creating a duplicate. The SDK helper `client.new_idempotency_key(...)` creates a suitable key.

Client reconnect uses `client.reconnect_session(session_id)` with the stored `client_id` and `resume_token`. Session scheduling control uses `client.pause_session(...)` and `client.resume_session(...)`, so reconnect and unpause are no longer overloaded in the SDK.

## Submit via curl

```bash
curl -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "add demo",
    "task_type": "add",
    "tasks": [{"a":1,"b":2},{"a":10,"b":20}],
    "client_id": "client-alpha",
    "idempotency_key": "add-demo-001",
    "session_priority": 1,
    "max_retries": 2
  }'
```

## API

Core endpoints:

```text
GET  /health
POST /jobs
GET  /sessions
GET  /sessions/{session_id}
GET  /clients/{client_id}/sessions
GET  /clients/{client_id}/sessions/{session_id}
POST /sessions/{session_id}/priority
GET  /sessions/{session_id}/jobs
GET  /sessions/{session_id}/results
GET  /sessions/{session_id}/results/stream
GET  /jobs
GET  /jobs/{job_id}
GET  /jobs/{job_id}/tasks
GET  /jobs/{job_id}/results
GET  /jobs/{job_id}/results/stream
POST /jobs/{job_id}/cancel
POST /jobs/{job_id}/retry-failed
GET  /workers
GET  /workers/{worker_id}/logs
GET  /workers/{worker_id}/logs/{filename}
GET  /workers/{worker_id}/config
POST /workers/{worker_id}/config
POST /workers/purge-offline
GET  /events
GET  /ui
GET  /ui/sessions
GET  /ui/sessions/{session_id}
GET  /ui/sessions/{session_id}/results
GET  /ui/client-sessions
GET  /ui/jobs
GET  /ui/jobs/{job_id}
GET  /ui/jobs/{job_id}/results
POST /ui/jobs/{job_id}/retry-failed
GET  /ui/tasks/{task_id}
POST /ui/tasks/{task_id}/retry
GET  /ui/workers
POST /ui/workers/purge-offline
GET  /ui/workers/{worker_id}/logs
GET  /ui/workers/{worker_id}/logs/{filename}
GET  /ui/manager/data-flow
GET  /ui/manager/log
GET  /ui/submit
POST /ui/submit
GET  /admin
```

Worker endpoints:

```text
POST /workers/heartbeat
POST /tasks/lease
POST /tasks/{task_id}/complete
POST /tasks/{task_id}/fail
POST /tasks/{task_id}/retry
```

## Operational capabilities

### Recovery and stale worker handling

TaskGrid treats task leases as recoverable. If a worker node or container dies mid-task, the manager can detect expired running leases and either requeue the task or fail it when retries are exhausted. Operators can view this from:

```text
/ui/manager/recovery
```

Programmatic endpoints are also available:

```text
GET  /maintenance/status
POST /maintenance/reconcile
POST /maintenance/recover
POST /workers/purge-offline
```

`POST /maintenance/reconcile` is the normal operator-safe repair action. It recomputes job and service-session counters/statuses from durable task rows and, by default, recovers expired leases. The manager also runs this safe reconcile pass on startup so a restart repairs stale derived state without touching valid running leases.

Late or duplicate results from an old worker are ignored and recorded with manager events such as `TaskResultIgnored` or `TaskFailureIgnored`.

Offline workers stay in the manager registry after their last heartbeat so operators can see what disappeared. They can be purged from `/ui/workers`, `/ui/manager/recovery`, or `POST /workers/purge-offline`. Purging removes stale worker/config rows only; jobs, tasks, results, events, and remote worker logs are preserved. Workers with running task assignments are skipped by default.


### Worker capacity and instance control

- A worker node manages N logical single-task instances with `--instances N`; `--concurrency` remains only as a deprecated alias.
- One worker node can run multiple tasks at once by supervising multiple independent instances.
- Each instance can run only one task at a time.
- Desired instance counts can be changed from `/ui/workers` or `POST /workers/{worker_id}/config`.
- Increasing the desired instance count starts more loops; shrinking waits for extra instances to finish current work before stopping.
- The dashboard and Workers page show busy instances, desired instances, active tasks, tags, and heartbeat age.

### Capability-aware scheduling

- Workers can advertise tags with `python -m taskgrid.worker --tag gpu --tag risk-model-v2`.
- Jobs can set `metadata.required_tags`, so only matching workers lease those tasks.
- Workers also advertise loaded task types, service name, service version, tags, modules, and instance state.
- `/ui/task-catalog` shows which active workers can run each task type.
- `GET /task-catalog?task_type=square` returns task capability data for API clients.
- Submissions can set `require_capable_worker=true` to reject work when no active capable worker is online.

### Retries and recovery controls

- Failed tasks retry automatically up to the task/job retry limit.
- Operators can manually retry one failed task.
- Operators can retry all failed tasks in a job.
- Retry controls are available from the task detail and job detail pages.

### Logging and traceability

- Instance logs live under `taskgrid_logs/<worker-id>/instances/<instance-id>/worker.log`.
- Node lifecycle and log-proxy messages live in `taskgrid_logs/<worker-id>/supervisor.log`.
- Task stdout/stderr and tracebacks are embedded into bounded `TASK ... START/END` sections in the relevant instance `worker.log`.
- In debug/verbose event mode, successful task events in `manager.log` include the execution slot in `node-id-instance-id` form. In normal mode, task rows/results/counters provide the authoritative tracking without writing accept/complete event lines for every tiny task.
- The main worker node process exposes supervisor and instance logs through the local HTTP log API.
- The broker/UI can proxy nested log paths such as `instances/instance-001/worker.log`.

## Current MVP limits

- No auth yet.
- No per-user permissions yet.
- SQLite is fine for local/small internal use, not for a large fleet.
- Workers run trusted Python code. There is no sandbox.
- Tasks should be independent/idempotent because retries can rerun work.
- No task dependencies/DAGs yet.
- Worker instance-count changes are cooperative; shrinking waits until current tasks finish instead of killing them.
- Worker tags are simple exact-match strings; there is no advanced resource accounting yet.
- No binary artifact store yet; return JSON only or store external paths in results.
- Worker log proxy assumes the broker can reach the advertised worker `log_url`; use `--log-public-url` for remote nodes.

## Sensible next upgrades

1. Add API keys and worker tokens.
2. Add Postgres for broker state.
3. Add Redis/RabbitMQ for higher-throughput queueing.
4. Add task artifact storage for files/results.
5. Add per-task timeout enforcement.
6. Add basic scheduler rules, e.g. per-job concurrency caps.
7. Add log retention/rotation policies.
8. Add role-aware UI permissions.
9. Add a React admin UI if the server-rendered UI becomes too limiting.



### Scale benchmarking

TaskGrid includes a local benchmark harness for manager/worker/client scale checks:

```bash
python scripts/benchmark_scale.py --workers 4 --instances-per-worker 1 --clients 8 --tasks-per-client 50
python scripts/benchmark_scale.py --workers 1 --instances-per-worker 8 --clients 8 --tasks-per-client 50
python scripts/benchmark_scale.py --workers 4 --instances-per-worker 4 --clients 16 --tasks-per-client 100
python scripts/benchmark_scale.py --workers 4 --instances-per-worker 100 --clients 8 --tasks-per-client 100 --execution-mode thread
```

The benchmark starts a temporary manager, launches worker processes, submits concurrent client jobs, waits for completion, and prints JSON with task throughput, worker counts, stale workers, expired leases, and task state totals from SQLite. If debug event mode is enabled, it also reports verbose task lifecycle event counts. It uses a temporary SQLite database and log folder by default.

Observed local smoke results from this package:

| Shape | Tasks | Result | Throughput | Notes |
|---|---:|---|---:|---|
| 4 worker nodes × 1 instance | 400 | 400 succeeded / 0 failed | ~115 tasks/sec | baseline multi-node test |
| 1 worker node × 8 instances | 400 | 400 succeeded / 0 failed | ~105 tasks/sec | validates bounded session pool inside one node |
| 8 worker nodes × 2 instances | 1,600 | 1,600 succeeded / 0 failed | ~117 tasks/sec | larger multi-node test |
| 4 worker nodes × 4 instances | 1,600 | 1,600 succeeded / 0 failed | ~106 tasks/sec | larger multi-instance test |

These are tiny `square` tasks, so they mostly measure manager/broker overhead rather than real compute throughput. CPU-heavy tasks should scale differently because the manager does less work per second relative to engine compute time.

Scale-oriented defaults:

- Worker supervisors own heartbeats; lease polling does not write heartbeat state per request.
- Worker nodes use a bounded HTTP session pool, so many instances do not create unbounded sockets but also do not serialize every broker call.
- Idle instances use jitter/backoff to avoid synchronized polling bursts.
- SQLite runs in WAL mode with `synchronous=NORMAL` by default for better single-manager throughput.
- Additional task/event indexes are created during startup/migration.
- Job creation bulk-inserts task rows with `executemany`.

Relevant tuning knobs:

```bash
TASKGRID_SQLITE_SYNCHRONOUS=NORMAL
TASKGRID_LEASE_CANDIDATE_MULTIPLIER=50
python -m taskgrid.worker --poll-seconds 0.5 --heartbeat-seconds 2 --broker-pool-size 8
```

See also:

```text
docs/SCALE.md
```


### Manager connection stress benchmark

To stress the manager/broker directly, use the connection-focused benchmark. It simulates many worker nodes and instance polling loops over HTTP without launching real worker executors:

```bash
python scripts/benchmark_manager_connections.py \
  --workers 4 \
  --instances-per-worker 100 \
  --clients 16 \
  --tasks-per-client 100 \
  --shared-session-per-worker
```

See `docs/SCALE.md` for current benchmark results and bottleneck notes.



## Streaming results

For async clients that want completed task results as soon as they are available, TaskGrid exposes Server-Sent Events streams:

```text
GET /jobs/{job_id}/results/stream
GET /sessions/{session_id}/results/stream
```

Streams emit JSON payloads with these event names:

```text
progress  current job/session counters and status
result    one newly terminal task result
done      job/session reached a terminal state and all terminal results were sent
timeout   optional timeout_seconds was reached before terminal completion
```

Useful query parameters:

```text
poll_seconds=0.5      manager polling cadence, 0.1 to 10 seconds
timeout_seconds=0     0 means no stream timeout
replay=true           send already-completed terminal results first
limit=200             max result events fetched per manager poll
```

The stream cursor uses `(updated_at, task_id)`, so tasks that finish in the same millisecond are streamed once without relying on manager logs. Normal result APIs and exports remain the durable source of truth for reconnect/replay.

Python SDK example:

```python
for event in client.stream_results(job["id"], timeout_seconds=60):
    if event["_event"] == "result":
        task = event["task"]
        print(task["input_index"], task["input_key"], task["status"], task.get("result"))
    elif event["_event"] == "done":
        break
```

## Result exports

TaskGrid keeps task rows as the source of truth and lets clients download completed output later, even after disconnecting.

Useful endpoints:

```text
GET /jobs/{job_id}/results/export?format=json&order=input
GET /jobs/{job_id}/results/export?format=csv&order=input
GET /jobs/{job_id}/results/export?format=csv&failed_only=true
GET /sessions/{session_id}/results/export?format=json&order=input
GET /sessions/{session_id}/results/export?format=csv&order=input
GET /sessions/{session_id}/results/export?format=csv&failed_only=true
```

The web UI exposes these from job/session result pages as **Download JSON**, **Download CSV**, and **Failed CSV**. CSV rows start with `input_index,input_key,...` for stable input-to-result mapping.

## Retention and cleanup

TaskGrid includes cautious retention cleanup for local manager state:

```text
GET  /maintenance/retention/preview
POST /maintenance/retention/apply
```

The UI page is:

```text
/ui/manager/retention
```

Cleanup only removes terminal sessions/jobs/tasks older than the selected windows. Queued and running work is never removed. Manager events can be trimmed separately, and stale worker rows can be purged as part of cleanup.

## Batch leasing and long polling

The default worker path remains one single-task instance leasing one task at a time. For larger worker nodes, enable node-level batch leasing:

```bash
python -m taskgrid.worker \
  --module examples.custom_tasks \
  --instances 16 \
  --batch-lease-size 8
```

Batch leasing reduces manager request pressure by letting a node lease work for multiple idle instances in one request. Each task is still assigned to a precise execution slot such as:

```text
node-a-instance-003
```

Optional long polling reduces empty idle polls:

```bash
python -m taskgrid.worker \
  --module examples.custom_tasks \
  --instances 16 \
  --batch-lease-size 8 \
  --long-poll-seconds 2
```

Keep `--batch-lease-size 1` to use the original per-instance lease path.
