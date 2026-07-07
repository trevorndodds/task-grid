from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name) or default


DEFAULT_DB_PATH = Path(_env("TASKGRID_DB", "./taskgrid.db"))


def db_path() -> Path:
    return Path(_env("TASKGRID_DB", str(DEFAULT_DB_PATH))).expanduser().resolve()


def connect() -> sqlite3.Connection:
    path = db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA synchronous={os.environ.get('TASKGRID_SQLITE_SYNCHRONOUS', 'NORMAL')}")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-20000")
    return conn


@contextmanager
def connection() -> Iterator[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS service_sessions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                total_jobs INTEGER NOT NULL DEFAULT 0,
                total_tasks INTEGER NOT NULL DEFAULT 0,
                queued_tasks INTEGER NOT NULL DEFAULT 0,
                running_tasks INTEGER NOT NULL DEFAULT 0,
                completed_tasks INTEGER NOT NULL DEFAULT 0,
                failed_tasks INTEGER NOT NULL DEFAULT 0,
                cancelled_tasks INTEGER NOT NULL DEFAULT 0,
                client_id TEXT,
                resume_token TEXT,
                paused INTEGER NOT NULL DEFAULT 0,
                paused_at TEXT,
                pause_reason TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_service_sessions_status
                ON service_sessions(status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_service_sessions_priority
                ON service_sessions(status, priority DESC, created_at ASC);
            CREATE INDEX IF NOT EXISTS idx_service_sessions_client
                ON service_sessions(client_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                session_id TEXT REFERENCES service_sessions(id) ON DELETE SET NULL,
                name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                total_tasks INTEGER NOT NULL DEFAULT 0,
                completed_tasks INTEGER NOT NULL DEFAULT 0,
                failed_tasks INTEGER NOT NULL DEFAULT 0,
                cancelled_tasks INTEGER NOT NULL DEFAULT 0,
                client_id TEXT,
                idempotency_key TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                cancelled_at TEXT,
                paused INTEGER NOT NULL DEFAULT 0,
                paused_at TEXT,
                pause_reason TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                task_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                input_index INTEGER,
                input_key TEXT,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 2,
                assigned_worker_id TEXT,
                leased_at TEXT,
                lease_expires_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                result_json TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_session
                ON jobs(session_id, created_at DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_client_idempotency
                ON jobs(client_id, idempotency_key)
                WHERE client_id IS NOT NULL AND idempotency_key IS NOT NULL;

            CREATE INDEX IF NOT EXISTS idx_tasks_queue
                ON tasks(status, priority DESC, created_at ASC);
            CREATE INDEX IF NOT EXISTS idx_tasks_job
                ON tasks(job_id, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_input_order
                ON tasks(job_id, input_index, id);
            CREATE INDEX IF NOT EXISTS idx_tasks_lease
                ON tasks(status, lease_expires_at);
            CREATE INDEX IF NOT EXISTS idx_tasks_assigned
                ON tasks(status, assigned_worker_id);
            CREATE INDEX IF NOT EXISTS idx_tasks_assigned_history
                ON tasks(assigned_worker_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_tasks_queue_order
                ON tasks(status, priority DESC, created_at ASC, input_index ASC, id ASC);
            CREATE INDEX IF NOT EXISTS idx_tasks_finished
                ON tasks(job_id, finished_at);
            CREATE INDEX IF NOT EXISTS idx_tasks_status_updated
                ON tasks(status, updated_at);

            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                hostname TEXT NOT NULL,
                status TEXT NOT NULL,
                current_task_id TEXT,
                last_heartbeat_at TEXT NOT NULL,
                version TEXT NOT NULL DEFAULT 'dev',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_workers_heartbeat
                ON workers(last_heartbeat_at);

            CREATE TABLE IF NOT EXISTS worker_configs (
                worker_id TEXT PRIMARY KEY,
                desired_concurrency INTEGER NOT NULL DEFAULT 1,
                disabled INTEGER NOT NULL DEFAULT 0,
                disabled_at TEXT,
                disabled_reason TEXT,
                draining INTEGER NOT NULL DEFAULT 0,
                drain_at TEXT,
                drain_reason TEXT,
                updated_at TEXT NOT NULL,
                updated_by TEXT NOT NULL DEFAULT 'system'
            );

            CREATE TABLE IF NOT EXISTS worker_instance_configs (
                worker_id TEXT NOT NULL,
                instance_id TEXT NOT NULL,
                draining INTEGER NOT NULL DEFAULT 0,
                drain_at TEXT,
                drain_reason TEXT,
                updated_at TEXT NOT NULL,
                updated_by TEXT NOT NULL DEFAULT 'system',
                PRIMARY KEY(worker_id, instance_id)
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at TEXT NOT NULL,
                level TEXT NOT NULL,
                code TEXT NOT NULL DEFAULT '',
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                message TEXT NOT NULL,
                data_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_events_entity
                ON events(entity_type, entity_id, at DESC);
            CREATE INDEX IF NOT EXISTS idx_events_code
                ON events(code, at DESC);
            CREATE INDEX IF NOT EXISTS idx_events_at
                ON events(at DESC);
            """
        )
        job_columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        if "session_id" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN session_id TEXT")
        if "paused" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN paused INTEGER NOT NULL DEFAULT 0")
        if "paused_at" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN paused_at TEXT")
        if "pause_reason" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN pause_reason TEXT")
        if "client_id" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN client_id TEXT")
            conn.execute("UPDATE jobs SET client_id=(SELECT client_id FROM service_sessions WHERE service_sessions.id=jobs.session_id) WHERE client_id IS NULL")
        if "idempotency_key" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN idempotency_key TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_session ON jobs(session_id, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_paused ON jobs(paused, status)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_client_idempotency ON jobs(client_id, idempotency_key) WHERE client_id IS NOT NULL AND idempotency_key IS NOT NULL")

        session_columns = {row[1] for row in conn.execute("PRAGMA table_info(service_sessions)").fetchall()}
        if "priority" not in session_columns:
            conn.execute("ALTER TABLE service_sessions ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
        if "client_id" not in session_columns:
            conn.execute("ALTER TABLE service_sessions ADD COLUMN client_id TEXT")
        if "resume_token" not in session_columns:
            conn.execute("ALTER TABLE service_sessions ADD COLUMN resume_token TEXT")
        if "paused" not in session_columns:
            conn.execute("ALTER TABLE service_sessions ADD COLUMN paused INTEGER NOT NULL DEFAULT 0")
        if "paused_at" not in session_columns:
            conn.execute("ALTER TABLE service_sessions ADD COLUMN paused_at TEXT")
        if "pause_reason" not in session_columns:
            conn.execute("ALTER TABLE service_sessions ADD COLUMN pause_reason TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_service_sessions_priority ON service_sessions(status, priority DESC, created_at ASC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_service_sessions_client ON service_sessions(client_id, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_service_sessions_paused ON service_sessions(paused, status)")

        task_columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        added_input_index = False
        if "input_index" not in task_columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN input_index INTEGER")
            added_input_index = True
        if "input_key" not in task_columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN input_key TEXT")
        if added_input_index:
            # Existing databases predate first-class input indexes. Backfill a
            # stable per-job order from the old insertion order approximation so
            # historical exports become deterministic without rewriting task ids.
            job_ids = [row[0] for row in conn.execute("SELECT id FROM jobs ORDER BY created_at ASC, id ASC").fetchall()]
            for job_id in job_ids:
                task_ids = [
                    row[0]
                    for row in conn.execute(
                        "SELECT id FROM tasks WHERE job_id=? ORDER BY created_at ASC, id ASC",
                        (job_id,),
                    ).fetchall()
                ]
                for index, task_id in enumerate(task_ids):
                    conn.execute("UPDATE tasks SET input_index=? WHERE id=? AND input_index IS NULL", (index, task_id))

        columns = {row[1] for row in conn.execute("PRAGMA table_info(events)").fetchall()}
        if "code" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN code TEXT NOT NULL DEFAULT ''")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_code ON events(code, at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_at ON events(at DESC)")

        worker_config_columns = {row[1] for row in conn.execute("PRAGMA table_info(worker_configs)").fetchall()}
        if "disabled" not in worker_config_columns:
            conn.execute("ALTER TABLE worker_configs ADD COLUMN disabled INTEGER NOT NULL DEFAULT 0")
        if "disabled_at" not in worker_config_columns:
            conn.execute("ALTER TABLE worker_configs ADD COLUMN disabled_at TEXT")
        if "disabled_reason" not in worker_config_columns:
            conn.execute("ALTER TABLE worker_configs ADD COLUMN disabled_reason TEXT")
        if "draining" not in worker_config_columns:
            conn.execute("ALTER TABLE worker_configs ADD COLUMN draining INTEGER NOT NULL DEFAULT 0")
        if "drain_at" not in worker_config_columns:
            conn.execute("ALTER TABLE worker_configs ADD COLUMN drain_at TEXT")
        if "drain_reason" not in worker_config_columns:
            conn.execute("ALTER TABLE worker_configs ADD COLUMN drain_reason TEXT")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS worker_instance_configs (
                worker_id TEXT NOT NULL,
                instance_id TEXT NOT NULL,
                draining INTEGER NOT NULL DEFAULT 0,
                drain_at TEXT,
                drain_reason TEXT,
                updated_at TEXT NOT NULL,
                updated_by TEXT NOT NULL DEFAULT 'system',
                PRIMARY KEY(worker_id, instance_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_worker_instance_configs_state ON worker_instance_configs(worker_id, draining)")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(status, assigned_worker_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_finished ON tasks(job_id, finished_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status_updated ON tasks(status, updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_assigned_history ON tasks(assigned_worker_id, updated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_queue_order ON tasks(status, priority DESC, created_at ASC, input_index ASC, id ASC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_input_order ON tasks(job_id, input_index, id)")
