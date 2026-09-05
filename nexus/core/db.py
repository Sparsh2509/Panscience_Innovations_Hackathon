"""
Database layer for NEXUS.
Single source of truth using SQLite in WAL mode with busy timeout handling.
"""

from contextlib import contextmanager
import os
from pathlib import Path
import sqlite3
import time
from typing import Optional

# Base directory for the project workspace
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_DB_PATH = DATA_DIR / "nexus.db"


def get_default_db_path() -> Path:
    """Returns the default database path or overrides via NEXUS_DB_PATH env var."""
    override = os.getenv("NEXUS_DB_PATH")
    if override:
        return Path(override).resolve()
    return DEFAULT_DB_PATH


def get_db(db_path: Optional[str | Path] = None) -> sqlite3.Connection:
    """
    Returns an SQLite connection configured with:
    - WAL mode
    - busy_timeout = 5000ms
    - foreign_keys = ON
    - row_factory = sqlite3.Row
    """
    resolved_path = Path(db_path).resolve() if db_path else get_default_db_path()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)

    # isolation_level=None enables autocommit mode, allowing explicit BEGIN / COMMIT / ROLLBACK control
    conn = sqlite3.connect(
        str(resolved_path),
        timeout=5.0,
        isolation_level=None,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row

    # Enforce SQLite configuration pragmas
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")

    return conn


@contextmanager
def transaction(conn: sqlite3.Connection, mode: str = "IMMEDIATE"):
    """
    Context manager for atomic SQLite transactions.
    Default mode is IMMEDIATE to serialize writers and prevent deadlocks in WAL mode.
    """
    if not conn.in_transaction:
        conn.execute(f"BEGIN {mode};")
    try:
        yield conn
        if conn.in_transaction:
            conn.execute("COMMIT;")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK;")
        raise


SCHEMA_SQL = """
-- 1. JOBS: Core job state machine with leasing and idempotency
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    job_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'DEAD_LETTER')),
    priority INTEGER NOT NULL DEFAULT 0,
    release_version TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    run_at REAL NOT NULL,
    leased_by TEXT,
    lease_token TEXT,
    lease_expires_at REAL,
    result TEXT,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

-- 2. AUDIT_EVENTS: Append-only immutable log of state transitions & decisions
CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK(severity IN ('INFO', 'WARN', 'ERROR', 'CRITICAL')),
    actor TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at REAL NOT NULL
);

-- 3. WORKERS: Worker fleet registry and heartbeat tracking
CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('IDLE', 'BUSY', 'DEAD')),
    current_job_id TEXT,
    last_heartbeat_at REAL NOT NULL,
    started_at REAL NOT NULL
);

-- 4. RELEASES: Release versions and active deployment flag
CREATE TABLE IF NOT EXISTS releases (
    version TEXT PRIMARY KEY,
    is_active INTEGER NOT NULL DEFAULT 0 CHECK(is_active IN (0, 1)),
    description TEXT NOT NULL,
    config TEXT NOT NULL,
    deployed_at REAL NOT NULL,
    deployed_by TEXT NOT NULL
);

-- INDEXES
CREATE INDEX IF NOT EXISTS idx_jobs_queued ON jobs (status, run_at, priority DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry ON jobs (status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency ON jobs (idempotency_key);
CREATE INDEX IF NOT EXISTS idx_jobs_release_version ON jobs (release_version);
CREATE INDEX IF NOT EXISTS idx_audit_job_id ON audit_events (job_id);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_events (created_at DESC);
"""


def init_db(db_path: Optional[str | Path] = None) -> sqlite3.Connection:
    """
    Initializes the database schema and default initial release.
    Safe to run multiple times (idempotent).
    """
    conn = get_db(db_path)

    # executescript handles multi-statement DDL and auto-commits
    conn.executescript(SCHEMA_SQL)

    with transaction(conn):
        # Seed initial release v1.0.0 if releases table is empty or v1.0.0 not present
        cursor = conn.execute("SELECT COUNT(*) AS cnt FROM releases")
        count = cursor.fetchone()["cnt"]
        if count == 0:
            now = time.time()
            conn.execute(
                """
                INSERT INTO releases (version, is_active, description, config, deployed_at, deployed_by)
                VALUES (?, 1, ?, ?, ?, ?)
                """,
                ("v1.0.0", "Initial stable release", "{}", now, "system"),
            )
        else:
            # Ensure at least one active release exists
            active = conn.execute("SELECT COUNT(*) AS cnt FROM releases WHERE is_active = 1").fetchone()["cnt"]
            if active == 0:
                conn.execute("UPDATE releases SET is_active = 1 WHERE version = 'v1.0.0'")

    return conn


def get_journal_mode(conn: sqlite3.Connection) -> str:
    """Helper to query current journal mode."""
    cursor = conn.execute("PRAGMA journal_mode;")
    row = cursor.fetchone()
    return row[0].upper() if row else ""


def get_busy_timeout(conn: sqlite3.Connection) -> int:
    """Helper to query current busy_timeout in ms."""
    cursor = conn.execute("PRAGMA busy_timeout;")
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Helper to check if a table exists in sqlite_master."""
    cursor = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return cursor.fetchone() is not None
