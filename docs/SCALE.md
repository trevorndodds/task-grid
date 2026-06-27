# TaskGrid Scale Notes

TaskGrid currently uses one FastAPI manager, SQLite/WAL, and HTTP polling workers. This is intentionally simple and works well for small to moderate local grids, but the manager is the bottleneck for very small tasks.

## Validated local smoke tests

These tests were run with `scripts/benchmark_scale.py` using `examples.custom_tasks:square`. Tiny tasks mostly measure broker overhead.

| Shape | Clients | Tasks | Result | Runtime | Throughput |
|---|---:|---:|---|---:|---:|
| 4 worker nodes × 1 instance | 8 | 400 | 400 succeeded / 0 failed | ~3.46s | ~115 tasks/sec |
| 1 worker node × 8 instances | 8 | 400 | 400 succeeded / 0 failed | ~3.82s | ~105 tasks/sec |
| 8 worker nodes × 2 instances | 16 | 1,600 | 1,600 succeeded / 0 failed | ~13.64s | ~117 tasks/sec |
| 4 worker nodes × 4 instances | 16 | 1,600 | 1,600 succeeded / 0 failed | ~15.05s | ~106 tasks/sec |
| 1 worker node × 100 instances, process mode | 8 | 800 | 800 succeeded / 0 failed | ~10.59s | ~75 tasks/sec |
| 2 worker nodes × 100 instances, process mode | 8 | 800 | 800 succeeded / 0 failed | ~8.64s | ~93 tasks/sec |
| 4 worker nodes × 100 instances, thread mode | 8 | 800 | 800 succeeded / 0 failed | ~4.38s | ~183 tasks/sec |
| 4 worker nodes × 100 instances, thread mode | 16 | 1,600 | 1,600 succeeded / 0 failed | ~13.63s | ~117 tasks/sec |

No stale workers or expired running tasks were reported in those runs. Current normal mode suppresses high-volume successful `TaskAccepted` and `TaskCompleted` event rows; task state/results/counters are the authoritative measurement. Enable `TASKGRID_EVENT_MODE=debug` only when exact accept/complete event traces are needed.

## What was optimized

- Removed heartbeat writes from the lease path. The worker supervisor remains the heartbeat source of truth.
- Added a bounded HTTP session pool per worker node. Multiple instances share a fixed-size pool instead of opening unbounded sockets or serializing all requests behind one lock.
- Added SQLite indexes for assigned tasks, finished task ordering, status/update scans, and event time scans.
- Set SQLite `synchronous=NORMAL` by default while keeping WAL mode.
- Bulk-insert tasks during job submission.
- Benchmarks report task state/result totals from the temporary SQLite DB. Debug event mode can also report exact accept/complete event counts when needed.

## Current practical guidance

For this SQLite MVP, a reasonable target is:

```text
10-25 worker nodes comfortably
25-50 worker nodes with tuned polling and realistic task durations
50+ worker nodes should be benchmarked on the deployment host
```

The number of instances matters more than the number of containers when tasks are tiny. For very small tasks, manager write rate dominates because every task produces lease and completion writes plus job/session counter updates. Normal mode avoids successful per-task event/log rows; debug mode intentionally adds them back for traceability.

## High-instance overload notes

`--instances 100` is intentionally aggressive for this MVP. It is useful for stressing broker polling, SQLite write contention, and worker process/thread overhead.

The default worker execution mode is:

```bash
--execution-mode process
```

That mode gives each task execution process isolation and is the safest default for CPU-heavy or untrusted task code, but tiny tasks can become dominated by process scheduling overhead. In this environment, 1-2 workers with 100 process-backed instances completed cleanly, but a 4-worker × 100-instance process-backed run exceeded the command timeout/resource envelope and is not counted as validated.

For trusted tiny or I/O-heavy tasks, use:

```bash
--execution-mode thread
```

or, for the lowest overhead when task code is trusted and does not need isolation:

```bash
--execution-mode inline
```

Thread mode completed the 4-worker × 100-instance overload shape cleanly in local testing. This does not mean 400 busy instances is a good production default; it means the broker/DB path can survive the shape when worker-side process overhead is removed. For CPU-heavy Python work, prefer fewer instances close to the physical core count and keep `process` mode.

## Tuning knobs

```bash
TASKGRID_SQLITE_SYNCHRONOUS=NORMAL
TASKGRID_LEASE_CANDIDATE_MULTIPLIER=50
python -m taskgrid.worker \
  --instances 4 \
  --poll-seconds 0.5 \
  --heartbeat-seconds 2 \
  --broker-pool-size 4 \
  --execution-mode process
```

Tiny trusted task benchmark example:

```bash
python scripts/benchmark_scale.py \
  --workers 4 \
  --instances-per-worker 100 \
  --clients 8 \
  --tasks-per-client 100 \
  --execution-mode thread
```

Use a larger poll interval for many idle workers. Use a broker pool size near the node's instance count, capped to a small value, to prevent socket storms.

## When to move beyond SQLite

Move to Postgres and/or a queue backend when you need:

- hundreds of worker nodes,
- thousands of active instances,
- high sustained throughput for tiny tasks,
- multiple manager replicas,
- stronger concurrent write behavior,
- production backup/restore tooling.

Likely next architecture step:

```text
FastAPI manager + Postgres state store + Redis/RabbitMQ/NATS queue/lease layer
```

Until then, TaskGrid is best suited to small and medium grids where each task does enough work that broker overhead is not the dominant cost.

## Manager connection stress benchmark

Use `scripts/benchmark_manager_connections.py` when you want to stress the manager/broker without paying worker process-pool overhead. It simulates many worker nodes and instance polling loops directly over HTTP:

```bash
python scripts/benchmark_manager_connections.py \
  --workers 4 \
  --instances-per-worker 100 \
  --clients 16 \
  --tasks-per-client 100 \
  --poll-seconds 0.05 \
  --shared-session-per-worker
```

This is intentionally harsher than normal operation. It creates hundreds of logical instance loops and drives lease/complete traffic into the manager as fast as SQLite and the HTTP stack allow.

Recent local results in this environment:

| Shape | Simulated instances | Tasks | Result | Throughput | Lease requests/sec |
|---|---:|---:|---|---:|---:|
| 4 workers × 100 instances | 400 | 800 | 800 succeeded / 0 failed | ~58 tasks/sec | ~121 req/sec |
| 4 workers × 100 instances | 400 | 1,600 | 1,600 succeeded / 0 failed | ~80 tasks/sec | ~131 req/sec |
| 8 workers × 100 instances | 800 | 800 | 800 succeeded / 0 failed | ~36 tasks/sec | ~121 req/sec |



Normal event mode was used for the latest 4 × 100 / 800-task run, so successful `TaskAccepted` and `TaskCompleted` event counts were intentionally `0`; task completion was measured from task rows/results. A smaller debug-mode trace run with 4 × 100 simulated instances and 400 tasks recorded 400 `TaskAccepted` and 400 `TaskCompleted` events, completed successfully, and dropped throughput to roughly 34 tasks/sec in this environment.

The bottleneck was not connection creation alone. The bottleneck is the manager write path: task lease transaction, task complete transaction, and job/session counter updates. In normal mode, successful accept/complete event inserts and JSONL audit writes are suppressed; debug mode intentionally adds that overhead back. TaskGrid also uses an incremental counter path for the normal accepted/completed flow to avoid full job/session recounts on every tiny task, but SQLite still serializes writes by design.

For real work, this is acceptable because task duration usually dominates broker overhead. For thousands of tiny tasks per second, the next architectural changes would be:

1. Lease task batches at the node level while preserving one active task per instance.
2. Add Postgres for higher write concurrency.
3. Add Redis/RabbitMQ/NATS for queue leasing.
4. Add async/buffered logging for debug lifecycle traces if verbose mode is needed at scale.
5. Add long-poll leasing to reduce idle empty polls.


## Batch leasing

For tiny tasks, manager pressure often comes from empty lease polling and lease/complete write churn rather than compute. TaskGrid supports optional node-level batch leasing:

```bash
python -m taskgrid.worker \
  --instances 32 \
  --batch-lease-size 16 \
  --long-poll-seconds 2 \
  --execution-mode thread \
  --module examples.custom_tasks
```

Batch leasing asks the manager for work for several idle instances in one request. It still assigns each task to a concrete execution slot (`worker-id-instance-id`), preserving task tracking and result attribution.

Suggested starting points:

```text
CPU-heavy trusted/untrusted tasks: --execution-mode process --instances roughly CPU cores --batch-lease-size 1..4
Tiny trusted tasks:              --execution-mode thread  --instances 16..100         --batch-lease-size 8..32
Idle-heavy clusters:             add --long-poll-seconds 1..5
```
