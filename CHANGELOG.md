# Changelog

## 0.24.2 - Idempotent submission and failure recovery smoke tests

- Added optional `idempotency_key` on job submission so clients can safely retry after a manager crash or lost HTTP response without creating duplicate jobs.
- Added SDK helper `client.new_idempotency_key(...)` and `client.submit(..., idempotency_key=...)`.
- Jobs now store `client_id` and `idempotency_key` with a per-client uniqueness guard.
- Added regression tests for idempotent replay and live SDK replay behavior.
- Ran local process failure tests for manager kill during submission, restart after expired leases, late duplicate completion, and restart/reconcile after partial completion.

## 0.24.1 - SDK reconnect integration hardening

- Fixed the Python SDK session method collision by separating reconnect-token lookup from pause/resume controls.
- Added `client.reconnect_session(...)` for client-owned session recovery and kept `client.resume_session(...)` for unpausing a service session.
- Added `client.resume_owned_session(...)` as a readable reconnect alias.
- Added live manager + SDK integration coverage across submit, reconnect, pause/resume, leasing, completion, indexed results, exports, SSE replay, worker controls, retention preview, and reconcile.

## 0.24.0 - Manager reconcile and restart recovery

- Added `POST /maintenance/reconcile` to repair manager-derived job/session counters and statuses from durable task rows.
- Reconcile optionally recovers expired running leases while leaving valid running leases untouched.
- Manager startup now runs a safe reconcile pass so restart/crash recovery repairs expired leases and denormalized counters.
- Added SDK helper `client.reconcile_manager(...)`.
- Added a Reconcile Manager State action to the Recovery UI.
- Added `MaintenanceReconcileRun` manager events.
- Extended tests for counter drift repair, expired lease recovery during reconcile, valid running lease preservation, API, and UI flow.

## 0.23.0 - Streaming result updates

- Added Server-Sent Events result streams for jobs and service sessions.
- Added `GET /jobs/{job_id}/results/stream` and `GET /sessions/{session_id}/results/stream`.
- Streams emit `progress`, `result`, `done`, and `timeout` events with JSON payloads.
- Added cursor-based terminal-task streaming using `(updated_at, task_id)` so same-millisecond completions are not skipped or duplicated.
- Added SDK helpers `client.stream_results(...)` and `client.stream_session_results(...)`.
- Result streams support `replay=false` for clients that only want new completions after connecting.
- Added tests for cursor behavior and SSE endpoints.

## 0.22.0 - First-class task input indexes

- Added durable `input_index` and optional `input_key` fields to every task row.
- Job submission now assigns task indexes in the same order as the submitted payload list.
- Clients can pass `input_keys`, or TaskGrid can derive keys from payload `id`, `input_key`, or `key` fields.
- Result APIs and exports now default to deterministic input order even when tasks finish out of order.
- Result JSON and CSV exports include `input_index` and `input_key` for stable mapping back to client inputs.
- Job/task/result UI tables now show input index and key.
- Existing databases are migrated and backfilled with stable per-job input indexes.

## 0.21.0 - Session and job pause controls

- Added manager/API/SDK controls to pause and resume service sessions.
- Added manager/API/SDK controls to pause and resume individual jobs.
- Paused sessions and jobs keep queued tasks queued but prevent new task leases.
- Running tasks are allowed to finish; pause is a scheduling hold, not cancellation.
- Added bulk session pause/resume API support.
- Updated web UI to show paused pills and pause/resume actions on session and job detail pages.
- Lease and batch-lease paths now skip paused sessions and paused jobs.

## 0.20.0 - Worker disable controls

- Added manager-controlled worker enable/disable state.
- Disabled workers continue to heartbeat but do not receive new task leases.
- Heartbeating disabled workers scale their local instance loops down to zero and restore their configured instance count when re-enabled.
- Added API endpoints for disabling/enabling one worker and bulk state updates.
- Added Workers UI controls for single-worker and selected-worker disable/enable actions.
- Added SDK helpers for worker state controls.
- Lease and batch-lease paths now skip disabled worker nodes.

## 0.18.0 - Exports, retention, and batch leasing

- Added downloadable job/session result exports as JSON or CSV.
- Added failed-task report exports for jobs and service sessions.
- Added retention preview/apply APIs and `/ui/manager/retention`.
- Added cautious cleanup for terminal sessions/jobs/tasks, old manager events, optional stale worker purging, and optional manager log truncation.
- Added node-level batch leasing with `POST /tasks/lease-batch` and worker `--batch-lease-size`.
- Added optional lease long polling with `--long-poll-seconds` / `wait_seconds`.
- Preserved precise task assignment to `worker-id-instance-id` when using batch leasing.
- Added SDK helpers for exports and retention cleanup.
- Updated Docker Compose, README, API docs, SDK docs, and scale docs.

## 0.17.0 - Normal-mode logging reduction

- Made task rows, job counters, and session counters the explicit source of truth for task tracking.
- Added `TASKGRID_EVENT_MODE=debug` and `TASKGRID_VERBOSE_TASK_EVENTS=1` for verbose successful task lifecycle traces.
- Normal mode now suppresses high-volume successful `TaskAccepted` and `TaskCompleted` event/manager-log writes.
- Important operational events still record normally, including submissions, warnings, failures, retries, recovery actions, ignored late results, worker config changes, and offline worker purge actions.
- Updated Manager Log and Data Flow UI copy to clarify normal vs debug logging.
- Updated API/scale documentation and environment examples for the new logging mode.

## 0.16.0 - Manager connection stress and write-path optimization

- Added `scripts/benchmark_manager_connections.py` to stress manager lease/complete traffic without worker process-pool overhead.
- Added simulated high-instance manager stress results to `docs/SCALE.md`.
- Optimized normal task accepted/completed state updates with incremental job/session counter updates.
- Documented manager write-path bottlenecks and next scaling steps.

## 0.15.0 - High-instance overload testing and execution modes

- Added worker `--execution-mode process|thread|inline`.
- Kept `process` as the default for isolated CPU-heavy work.
- Added `thread` and `inline` modes for trusted tiny or I/O-heavy tasks where process scheduling overhead dominates.
- Added `TASKGRID_EXECUTION_MODE` / `EXECUTION_MODE` Docker configuration examples.
- Extended `scripts/benchmark_scale.py` with `--execution-mode`.
- Documented 100-instance-per-worker overload test results and tuning guidance in `docs/SCALE.md`.

## 0.14.0 - Scale benchmark and lease-path optimization

- Added `scripts/benchmark_scale.py` for local manager/worker/client scale tests.
- Added `docs/SCALE.md` with validated local smoke results and tuning guidance.
- Removed per-lease worker heartbeat writes; worker supervisors now remain the heartbeat source of truth.
- Added a bounded HTTP session pool per worker node so multiple instances do not serialize all broker calls or open unbounded sockets.
- Bulk-insert task rows during job creation.
- Lease logic now respects worker-advertised task types when workers publish a task catalog.
- Added SQLite scale indexes for assigned tasks, task finish ordering, task status/update scans, and event time scans.
- Set SQLite `synchronous=NORMAL` by default while keeping WAL mode. Override with `TASKGRID_SQLITE_SYNCHRONOUS`.
- Added `TASKGRID_LEASE_CANDIDATE_MULTIPLIER` for tuning candidate scans when tag-constrained work is queued.
- Documented benchmark commands and scale tuning knobs.



## 0.13.0 - Offline worker purge

- Added manager/API support for purging stale/offline worker registry rows.
- Added `/workers/purge-offline` and SDK `purge_offline_workers()`.
- Added purge buttons to the Workers and Recovery UI pages.
- Purging removes stale worker/config rows only; jobs, tasks, results, events, and remote worker logs are preserved.
- Workers with running task assignments are skipped by default so operators can recover expired leases first.
- Added `WorkerPurgeOfflineRun` and `WorkerPurgedOffline` manager events.


## 0.12.0 - Recovery and stale worker hardening

- Added manager-side recovery status for stale workers and expired running task leases.
- Added `GET /maintenance/status` and `POST /maintenance/recover`.
- Added a Recovery page in the web UI at `/ui/manager/recovery`.
- Expired task leases now log assigned worker/instance, attempt count, and next status before being requeued or failed.
- Late or duplicate task completions/failures are ignored safely and recorded as `TaskResultIgnored` / `TaskFailureIgnored`.
- Dashboard and Workers UI now surface stale workers and expired lease counts.
- SDK now includes `recovery_status()` and `recover_expired_leases()`.
- Added tests for expired lease recovery, stale workers, ignored late results, and Recovery UI/API paths.

## Documentation polish

- Cleaned up README wording so it reads like a project guide rather than an implementation log.
- Replaced old phase/iteration-style README headings with operational sections for worker capacity, capability-aware scheduling, retries, and logging.
- Removed the standalone task-catalog duplicate from the end of the README and folded it into the operational capabilities section.

## 0.11.0 - Client resume tokens and cleanup

- Removed the legacy package-name compatibility shim for a clean new TaskGrid repository.
- Removed legacy environment-variable fallbacks; runtime configuration now uses `TASKGRID_*`.
- Added `client_id` and `resume_token` to service sessions so clients can disconnect and later reattach.
- Job submission now returns the created/reused session client ID and resume token.
- Added client resume APIs: `GET /clients/{client_id}/sessions` and `GET /clients/{client_id}/sessions/{session_id}`.
- Added a Client Resume page in the web UI for listing resumable sessions.
- SDK can now store reconnect credentials, list `my_sessions()`, and `resume_session(...)`.
- Fixed duplicate task-complete handling in the FastAPI route.
- Added tests for client resume API/SDK/UI flows.


## 0.10.1 - Rename to TaskGrid

- Renamed the project, package, UI, Docker examples, README, API docs, and SDK docs to TaskGrid.
- New commands use `taskgrid`, for example `uvicorn taskgrid.app:app` and `python -m taskgrid.worker`.
- Added `TaskGridClient` as the primary SDK client class.
- Renamed default runtime paths and environment variables to `TASKGRID_*`.


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
- Manager log records structured lifecycle events with stable PascalCase codes. As of 0.18.0, high-volume successful `TaskAccepted` and `TaskCompleted` events are debug-only; operational events such as `JobSubmitted`, `TaskFailed`, and `TaskLeaseExpiredRequeued` still record normally.
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


## 0.19.0 - Security and cancellation polish

- Added optional token-based security for manager/admin, client, worker, and UI access.
- Added `TASKGRID_ADMIN_TOKEN`, `TASKGRID_CLIENT_TOKEN`, `TASKGRID_WORKER_TOKEN`, and `TASKGRID_UI_TOKEN`.
- Added browser UI login/logout support when `TASKGRID_UI_TOKEN` or `TASKGRID_ADMIN_TOKEN` is configured.
- Worker nodes can now send `--worker-token` / `TASKGRID_WORKER_TOKEN` to authenticated managers.
- SDK `TaskGridClient(..., api_token="...")` now sends `X-TaskGrid-Token` on API requests.
- Changed job cancellation to graceful by default: queued tasks cancel immediately while already-running tasks finish.
- Added `cancelling` job/session status for graceful cancellation in progress.
- Added force cancellation mode for operators that want queued and running tasks marked cancelled immediately.
- Updated UI cancel controls to expose Graceful Cancel and Force Cancel.
- Added tests for token-protected routes and graceful/force cancellation behavior.
