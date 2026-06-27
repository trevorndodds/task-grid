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

            CREATE INDEX IF NOT EXISTS idx_tasks_queue
                ON tasks(status, priority DESC, created_at ASC);
            CREATE INDEX IF NOT EXISTS idx_tasks_job
                ON tasks(job_id, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_lease
                ON tasks(status, lease_expires_at);
            CREATE INDEX IF NOT EXISTS idx_tasks_assigned
                ON tasks(status, assigned_worker_id);
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
                updated_at TEXT NOT NULL,
                updated_by TEXT NOT NULL DEFAULT 'system'
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_session ON jobs(session_id, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_paused ON jobs(paused, status)")

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

        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(status, assigned_worker_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_finished ON tasks(job_id, finished_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status_updated ON tasks(status, updated_at)")
