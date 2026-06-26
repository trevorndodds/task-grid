# TaskGrid

A tiny, boring, useful distributed task manager for grid-style workloads:

- submit a service session/job made of many independent tasks
- workers lease tasks from a broker
- tasks run in parallel across nodes by running multiple single-task worker instances
- jobs can require worker capability tags such as `gpu` or `risk-model-v2`
- workers advertise task types and the manager exposes a task catalog / capability checker
- failed tasks retry automatically, and operators can manually retry failed tasks
- each single-task instance writes to its own instance folder, exposed by the node log API
- the broker proxies worker logs so operators can view them from the manager UI
- service sessions group client submissions and show job/task counts, pending/running/completed totals, start time, and end time
- results and logs are queryable
- the broker writes a manager-side JSONL audit log with event codes such as `TaskAccepted` and `TaskCompleted`
- a lightweight server-rendered web UI shows jobs, tasks, workers, events, results, retry controls, logs, and a submit form

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

## Architecture

```text
Client SDK / REST API
        ↓
Service Session + Job submission
        ↓
FastAPI broker
        ↓
SQLite session/job/task/event store + manager.log JSONL audit log
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

By default the manager writes a broker-side JSONL audit log beside the SQLite database:

```text
manager.log
```

Each line includes a stable event code, for example:

```json
{"code":"TaskAccepted","entity_type":"task","entity_id":"task_...","data":{"worker_id":"node-a:instance-001"}}
{"code":"TaskCompleted","entity_type":"task","entity_id":"task_...","data":{"worker_id":"node-a:instance-001"}}
```

Override the path with:

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

Terminal 3:

```bash
python examples/submit_demo.py
```

Open the web UI:

```text
http://127.0.0.1:8000/ui
```

`/admin` redirects to `/ui` for convenience.


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

A job response includes its `session_id`. To attach another job to an existing session, pass `session_id` when submitting. To name a new session explicitly, pass `session_name`:

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
    session_name="Square Run",
    session_priority=10,
    max_retries=2,
)
client.wait(job["id"])
print(client.results(job["id"]))
```

## Submit via curl

```bash
curl -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "add demo",
    "task_type": "add",
    "tasks": [{"a":1,"b":2},{"a":10,"b":20}],
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
POST /sessions/{session_id}/priority
GET  /sessions/{session_id}/jobs
GET  /sessions/{session_id}/results
GET  /jobs
GET  /jobs/{job_id}
GET  /jobs/{job_id}/tasks
GET  /jobs/{job_id}/results
POST /jobs/{job_id}/cancel
POST /jobs/{job_id}/retry-failed
GET  /workers
GET  /workers/{worker_id}/logs
GET  /workers/{worker_id}/logs/{filename}
GET  /workers/{worker_id}/config
POST /workers/{worker_id}/config
GET  /events
GET  /ui
GET  /ui/sessions
GET  /ui/sessions/{session_id}
GET  /ui/sessions/{session_id}/results
GET  /ui/jobs
GET  /ui/jobs/{job_id}
GET  /ui/jobs/{job_id}/results
POST /ui/jobs/{job_id}/retry-failed
GET  /ui/tasks/{task_id}
POST /ui/tasks/{task_id}/retry
GET  /ui/workers
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

## Round 5 additions

This round adds the single-task instance model and cleaner logs:

- A worker node manages N logical instances with `--instances N`; `--concurrency` remains a deprecated alias.
- Each logical instance can run only one task at a time.
- Instance logs live under `taskgrid_logs/<worker-id>/instances/<instance-id>/worker.log`.
- Node lifecycle/log-proxy messages live in `taskgrid_logs/<worker-id>/supervisor.log`.
- Task stdout/stderr and tracebacks are embedded into bounded `TASK ... START/END` sections in the relevant instance `worker.log`.
- Removed task-section file locking because one instance cannot have two active tasks writing to the same log.
- The main worker node process exposes supervisor and instance logs through the local HTTP log API.
- The broker/UI can proxy nested log paths such as `instances/instance-001/worker.log`.

## Round 4 additions

This round adds the first real node-management features:

- Earlier process-pool worker concurrency via `--concurrency N` has been superseded by `--instances N`.
- One worker node can still run multiple tasks at once by supervising multiple single-task instances.
- Broker-managed desired instance count per worker/node, stored in the existing config column for compatibility.
- Workers safely restart their instance pool to apply UI/API config changes.
- Workers UI can set desired instances per node.
- Dashboard shows busy and configured worker instances.
- Extra tests for worker config and UI updates.

## Round 3 additions

This round adds small but important operational features:

- Worker capability tags via `python -m taskgrid.worker --tag gpu`.
- Job metadata `required_tags`, so only matching workers lease those tasks.
- Manual retry for one failed task.
- Manual retry for all failed tasks in a job.
- Retry buttons in the web UI.
- Extra core tests for worker-tag scheduling and manual requeue.

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


## Task catalog / capabilities

Workers advertise loaded task types and tags in heartbeat metadata. Open `/ui/task-catalog` to see which workers can run each task type, or call `GET /task-catalog?task_type=square`. Submissions can set `require_capable_worker=true` to reject work when no active capable worker is online.
