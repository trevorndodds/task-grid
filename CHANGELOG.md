# Changelog


## 0.10.1 - Rename to TaskGrid

- Renamed the project, package, UI, Docker examples, README, API docs, and SDK docs from GridLite to TaskGrid.
- New commands use `taskgrid`, for example `uvicorn taskgrid.app:app` and `python -m taskgrid.worker`.
- Added backwards-compatible `gridlite` import/module shims so older scripts can keep working during migration.
- Added `TaskGridClient` as the primary SDK client class; `GridLiteClient` remains as a compatibility alias.
- Renamed default runtime paths and environment variables to `TASKGRID_*`, while still accepting legacy `GRIDLITE_*` variables where practical.


## 0.10.0 - Task catalog and capability validation

- Workers now advertise supported `task_types`, service name, service version, tags, modules, and instance state in heartbeat metadata.
- Added manager task catalog API: `GET /task-catalog`.
- Added Task Catalog UI at `/ui/task-catalog` with capability checks by task type and required tags.
- Job submission now logs `TaskCapabilityWarning` when no active capable worker is online for the submitted task.
- Added optional strict submission flag `require_capable_worker`; strict submissions return HTTP 409 / UI error instead of queueing work that no worker can run.
- Workers support optional `--service-name` and `--service-version` metadata flags.
- Workers page now shows advertised task types per node.
- SDK added `task_catalog(...)` and `require_capable_worker` support on `submit(...)`.
- Added tests for catalog reporting, strict submit rejection, and UI catalog pages.


## 0.9.3 - Service session priority

- Added first-class `service_sessions.priority` with SQLite migration support.
- Added `session_priority` to job submission; jobs inherit session priority unless job-level `priority` is explicitly supplied.
- Added `POST /sessions/{session_id}/priority` to promote/demote a whole session.
- Reprioritizes queued tasks in the session without interrupting running work.
- Added session priority controls to `/ui/sessions/{session_id}` and priority display on session/result pages.
- Added SDK helper `set_session_priority(session_id, priority)`.
- Improved event ordering by storing manager timestamps with milliseconds and ordering events by timestamp plus event id.
- Added priority scheduling tests and UI/API tests.


## 0.9.2 - Manager data-flow and result views

- Added structured job result summaries via `GET /jobs/{job_id}/results`, including payload, worker assignment, result/error, payload/result byte sizes, attempts, and timing.
- Added session-level result aggregation via `GET /sessions/{session_id}/results`.
- Added web UI result pages at `/ui/jobs/{job_id}/results` and `/ui/sessions/{session_id}/results`.
- Added `/ui/manager/data-flow`, documenting and surfacing the client → broker → worker instance → result path with recent task lifecycle events.
- Added SDK helper `session_results(session_id)`.
- Added tests for result summaries and the manager data-flow UI.


## 0.9.1 - Docker worker deployment cleanup

- Updated `docker-compose.yml` to use `--instances` instead of the deprecated `--concurrency` alias.
- Added `docker-compose.worker.yml` for the common setup where the manager runs on the host and Docker only runs worker nodes.
- Added worker-only compose defaults for `MANAGER_URL`, `WORKER_ID`, `INSTANCES`, `LOG_API_PORT`, and `LOG_PUBLIC_URL`.
- Documented the two-way networking requirement: workers call the manager API, and the manager calls the worker log API.
- Corrected same-host Docker log proxy examples to advertise `http://127.0.0.1:<port>` to a host-running manager.
- Added `EXPOSE 9201` to the Docker image as the default worker log API port.


## 0.9.0 - Service sessions

- Added first-class service sessions above jobs.
- Each client job submission creates a service session by default and the job response includes `session_id`.
- Added session task counters: pending/queued, running, completed, failed, cancelled, total, and created job count.
- Added session lifecycle timing: created, started, and finished timestamps.
- Added session status recalculation from underlying jobs/tasks.
- Added API endpoints: `GET /sessions`, `GET /sessions/{session_id}`, and `GET /sessions/{session_id}/jobs`.
- Added `/ui/sessions` and `/ui/sessions/{session_id}` web UI pages.
- Added service-session event codes such as `ServiceSessionCreated`, `ServiceSessionJobCreated`, and `ServiceSessionStatusChanged`.
- Updated SDK helpers for listing/reading sessions.
- Added tests for core session tracking and web UI session pages.


## 0.8.1 - Manager log web UI

- Improved Manager Log web UI at `/ui/manager/log`.
- Added parsed table view for JSONL manager events, with columns for time, level, code, entity, ID, message, and data.
- Added raw web UI toggle at `/ui/manager/log?view=raw` while preserving the raw API endpoint `/manager/log`.

## 0.8.0 - Manager audit log event codes

- Added broker-side manager JSONL log at `manager.log` by default, configurable with `TASKGRID_MANAGER_LOG`.
- Manager log now records structured lifecycle events with stable PascalCase codes such as `JobSubmitted`, `TaskAccepted`, `TaskCompleted`, `TaskFailed`, and `TaskLeaseExpiredRequeued`.
- Added `code` to persisted events and migration support for older SQLite databases.
- Added raw manager log endpoint: `GET /manager/log`.
- Added Manager Log page in the web UI at `/ui/manager/log`.
- Recent events tables now show the event code column.
- Added tests for event codes, JSONL manager log output, and the web UI manager log page.

## 0.7.0 - Independent instance loops and broker-friendly polling

- Changed worker execution from supervisor-central leasing to independent single-task instance loops.
- Each instance now leases exactly one task, runs it, reports the result/failure, then leases the next pending task.
- Kept the node as the unit shown/configured in the manager UI; `--instances N` controls how many logical single-task instances the node supervises.
- Added one shared broker HTTP client per node with a bounded connection pool.
- Added idle polling backoff and jitter so many idle instances do not hammer the broker in synchronized bursts.
- Added `--heartbeat-seconds` and `--broker-pool-size` worker options.
- Downscaling now asks extra instances to stop after their current task, while upscaling starts new instance loops immediately.

## 0.6.0

- Reframed worker scaling as node-managed single-task instances instead of shared multi-task worker concurrency.
- Added `--instances N`; kept `--concurrency` and `--limit` as deprecated aliases.
- Each logical instance can run only one task at a time. A node with 5 instances can run 5 tasks total, but each instance has its own isolated `worker.log`.
- Changed log layout to `taskgrid_logs/<worker-id>/supervisor.log` plus `taskgrid_logs/<worker-id>/instances/<instance-id>/worker.log`.
- Removed task-log file locking because a single instance cannot have multiple active task writers.
- Worker log API, broker proxy, and UI now support nested log paths such as `instances/instance-001/worker.log`.
- UI labels now describe busy/configured instances instead of shared slots.

## 0.5.1

- Changed task logging to embed bounded task sections inside the worker instance `worker.log`.
- Removed generation of one `task_<task-id>.log` file per task.
- Task sections now include payload, captured stdout/stderr, status, duration, and traceback on failure.
- Added a file lock around child-process task section appends so concurrent slots do not corrupt the instance log.
- Updated tests for embedded task logging.

## 0.5.0

- Added per-worker instance log folders under `taskgrid_logs/<worker-id>/` by default.
- Added `worker.log` for node lifecycle/lease/completion/resize messages.
- Added one `task_<task-id>.log` per task with stdout/stderr capture.
- Added a tiny worker-side log HTTP API served by the main worker process.
- Workers now advertise `metadata.log_url` and `metadata.log_dir` in heartbeats.
- Added broker proxy endpoints: `GET /workers/{worker_id}/logs` and `GET /workers/{worker_id}/logs/{filename}`.
- Added Workers UI log buttons plus log list/detail pages.
- Added SDK helpers to list/read worker logs.
- Added tests for worker log serving and broker/UI log proxying.


## 0.4.0

- Added real worker process-pool concurrency with `--concurrency N`.
- Kept `--limit` as a backwards-compatible alias when `--concurrency` is omitted.
- Added broker-managed per-worker desired concurrency in `worker_configs`.
- Added API endpoints to get/update worker config: `GET/POST /workers/{worker_id}/config`.
- Added Workers UI controls to change desired slots per node.
- Workers now safely restart their local executor pool to apply new slot counts without killing running tasks.
- Dashboard and Workers UI now show active slots, desired slots, running tasks, and pending config.
- Added SDK helpers for worker config.
- Added tests for worker config and UI updates.

## 0.3.0

- Added worker capability tags via `--tag`.
- Added job-level `metadata.required_tags` scheduling gate.
- Added manual retry for a single failed task.
- Added manual retry for all failed tasks in a job.
- Added retry buttons to the web UI.
- Preserved worker metadata during internal leasing/status heartbeats.
- Added tests for tag-gated leasing and manual retry.

## 0.2.0

- Added server-rendered web UI under `/ui`.
- Added job, task, worker, submit, and dashboard pages.
- Redirected `/admin` to `/ui`.

## 0.1.0

- Initial MVP broker, SQLite store, worker, SDK, examples, and tests.

## 0.9.3 - Dockerized manager deployment polish

- Reworked `docker-compose.yml` into a fully containerized manager + worker stack.
- Renamed the compose manager service from `broker` to `manager` for clearer TaskGrid terminology.
- Added bind-mounted persistence for `taskgrid-data/` and `worker-logs/`.
- Added explicit manager environment for `/data/taskgrid.db` and `/data/manager.log`.
- Added manager healthcheck before worker startup.
- Added `docker-compose.manager.yml` for manager-only container deployment.
- Updated worker-only compose docs for host-manager, manager-container, and remote-manager layouts.
- Added `.env.example` for Docker Compose overrides.
- Updated Dockerfile to create `/data` and `/logs`, set `TASKGRID_MANAGER_LOG`, and run unbuffered.
