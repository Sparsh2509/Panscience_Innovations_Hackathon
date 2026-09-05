"""
Tests for Phase 1: Database Foundation & Schema Verification.
"""

import gc
import sqlite3
import tempfile
from pathlib import Path
import pytest

from nexus.core.db import (
    init_db,
    get_db,
    table_exists,
    get_journal_mode,
    get_busy_timeout,
    transaction,
)


@pytest.fixture
def temp_db_path():
    """Provides an isolated temporary database file path for testing."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "test_nexus.db"
        yield db_path
        gc.collect()


def test_database_initialization(temp_db_path):
    """Verify that database initialization successfully creates the database and tables."""
    conn = init_db(temp_db_path)
    try:
        assert temp_db_path.exists()
        for table in ["jobs", "audit_events", "workers", "releases"]:
            assert table_exists(conn, table), f"Table {table} should exist"
    finally:
        conn.close()


def test_initialization_is_idempotent(temp_db_path):
    """Verify that init_db can safely run multiple times without error or data corruption."""
    conn1 = init_db(temp_db_path)
    conn1.close()

    # Second initialization on existing database
    conn2 = init_db(temp_db_path)
    try:
        for table in ["jobs", "audit_events", "workers", "releases"]:
            assert table_exists(conn2, table)

        # Ensure releases table still has exactly 1 release
        cursor = conn2.execute("SELECT COUNT(*) AS cnt FROM releases")
        assert cursor.fetchone()["cnt"] == 1
    finally:
        conn2.close()


def test_all_four_tables_and_columns_exist(temp_db_path):
    """Verify all required tables and their key columns are properly created."""
    conn = init_db(temp_db_path)
    try:
        # 1. JOBS columns
        job_cols = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        expected_job_cols = {
            "id",
            "idempotency_key",
            "job_type",
            "payload",
            "status",
            "priority",
            "release_version",
            "attempt_count",
            "max_retries",
            "run_at",
            "leased_by",
            "lease_token",
            "lease_expires_at",
            "result",
            "last_error",
            "created_at",
            "updated_at",
        }
        assert expected_job_cols.issubset(job_cols), f"Missing job columns: {expected_job_cols - job_cols}"

        # 2. AUDIT_EVENTS columns
        audit_cols = {row["name"] for row in conn.execute("PRAGMA table_info(audit_events)").fetchall()}
        expected_audit_cols = {"id", "job_id", "event_type", "severity", "actor", "details", "created_at"}
        assert expected_audit_cols.issubset(audit_cols), f"Missing audit columns: {expected_audit_cols - audit_cols}"

        # 3. WORKERS columns
        worker_cols = {row["name"] for row in conn.execute("PRAGMA table_info(workers)").fetchall()}
        expected_worker_cols = {"id", "pid", "status", "current_job_id", "last_heartbeat_at", "started_at"}
        assert expected_worker_cols.issubset(worker_cols), f"Missing worker columns: {expected_worker_cols - worker_cols}"

        # 4. RELEASES columns
        release_cols = {row["name"] for row in conn.execute("PRAGMA table_info(releases)").fetchall()}
        expected_release_cols = {"version", "is_active", "description", "config", "deployed_at", "deployed_by"}
        assert expected_release_cols.issubset(release_cols), f"Missing release columns: {expected_release_cols - release_cols}"

    finally:
        conn.close()


def test_wal_mode_enabled(temp_db_path):
    """Verify that SQLite Write-Ahead Logging (WAL) mode is active."""
    conn = init_db(temp_db_path)
    try:
        mode = get_journal_mode(conn)
        assert mode == "WAL", f"Expected journal_mode to be WAL, got '{mode}'"
    finally:
        conn.close()


def test_busy_timeout_is_5000(temp_db_path):
    """Verify that SQLite busy_timeout is set to 5000 ms."""
    conn = init_db(temp_db_path)
    try:
        timeout = get_busy_timeout(conn)
        assert timeout == 5000, f"Expected busy_timeout to be 5000 ms, got {timeout}"
    finally:
        conn.close()


def test_default_release_v1_exists_and_active(temp_db_path):
    """Verify that default initial release v1.0.0 exists and is marked active."""
    conn = init_db(temp_db_path)
    try:
        cursor = conn.execute("SELECT version, is_active FROM releases WHERE version = 'v1.0.0'")
        row = cursor.fetchone()
        assert row is not None, "Release v1.0.0 must exist"
        assert row["version"] == "v1.0.0"
        assert row["is_active"] == 1, "Release v1.0.0 must be active by default"
    finally:
        conn.close()


def test_indexes_exist(temp_db_path):
    """Verify all requested performance and safety indexes exist."""
    conn = init_db(temp_db_path)
    try:
        indexes = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
        expected_indexes = {
            "idx_jobs_queued",
            "idx_jobs_lease_expiry",
            "idx_jobs_idempotency",
            "idx_audit_job_id",
            "idx_audit_created_at",
        }
        assert expected_indexes.issubset(indexes), f"Missing indexes: {expected_indexes - indexes}"
    finally:
        conn.close()


def test_transaction_context_manager(temp_db_path):
    """Verify that the transaction context manager commits on success and rolls back on exception."""
    conn = init_db(temp_db_path)
    try:
        # Success transaction
        with transaction(conn):
            conn.execute(
                """
                INSERT INTO releases (version, is_active, description, config, deployed_at, deployed_by)
                VALUES ('v1.0.1-test', 0, 'Test release', '{}', 1000.0, 'test')
                """
            )

        row = conn.execute("SELECT version FROM releases WHERE version = 'v1.0.1-test'").fetchone()
        assert row is not None

        # Failing transaction -> rollback
        with pytest.raises(RuntimeError):
            with transaction(conn):
                conn.execute(
                    """
                    INSERT INTO releases (version, is_active, description, config, deployed_at, deployed_by)
                    VALUES ('v1.0.2-fail', 0, 'Fail release', '{}', 2000.0, 'test')
                    """
                )
                raise RuntimeError("Simulated transaction error")

        row_fail = conn.execute("SELECT version FROM releases WHERE version = 'v1.0.2-fail'").fetchone()
        assert row_fail is None, "Failed transaction should have rolled back"
    finally:
        conn.close()
