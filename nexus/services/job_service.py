"""
Job service for NEXUS.
Centralizes durable job creation, idempotent submission, atomic leasing, and lifecycle transitions.
"""

import json
import sqlite3
import time
import uuid
from typing import Any, Optional

from nexus.core.config import (
    DEFAULT_JOB_PRIORITY,
    DEFAULT_LEASE_DURATION_SECONDS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BACKOFF_FACTOR,
    DEFAULT_RETRY_BASE_DELAY_SECONDS,
    DEFAULT_RETRY_MAX_DELAY_SECONDS,
)
from nexus.core.db import transaction
from nexus.services.audit_service import record_audit_event


class JobError(Exception):
    """Base exception for job-related errors."""
    pass


class JobValidationError(JobError):
    """Raised when job input fields are invalid or missing."""
    pass


class JobNotFoundError(JobError):
    """Raised when a requested job ID is not found."""
    pass


class LeaseAuthorizationError(JobError):
    """Raised when a worker attempts an unauthorized operation due to mismatched lease token or worker ownership."""
    pass


def _row_to_job_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Helper to convert a sqlite3.Row for a job into a clean Python dictionary."""
    data = dict(row)
    if "payload" in data and isinstance(data["payload"], str):
        try:
            data["payload"] = json.loads(data["payload"])
        except (json.JSONDecodeError, TypeError):
            pass
    if "result" in data and isinstance(data["result"], str):
        try:
            data["result"] = json.loads(data["result"])
        except (json.JSONDecodeError, TypeError):
            pass
    return data


def create_job(
    conn: sqlite3.Connection,
    job_type: str,
    payload: dict[str, Any],
    idempotency_key: Optional[str] = None,
    priority: int = DEFAULT_JOB_PRIORITY,
    max_retries: int = DEFAULT_MAX_RETRIES,
    run_at: Optional[float] = None,
    job_id: Optional[str] = None,
) -> tuple[dict[str, Any], bool]:
    """
    Durable job creation with idempotency guarantees.

    Returns:
        tuple (job_dict, is_dedup)
        - If first request: returns (created_job, False) and logs JOB_ENQUEUED.
        - If duplicate idempotency key: returns (existing_job, True) and logs DEDUP_HIT.

    Ensures the transaction commits before returning success.
    """
    if not job_type or not isinstance(job_type, str) or not job_type.strip():
        raise JobValidationError("Field 'job_type' must be a non-empty string.")
    if payload is None or not isinstance(payload, dict):
        raise JobValidationError("Field 'payload' must be a dictionary.")

    job_id = job_id or str(uuid.uuid4())
    now = time.time()
    scheduled_run_at = run_at if run_at is not None else now
    payload_json = json.dumps(payload)

    with transaction(conn, mode="IMMEDIATE"):
        # 1. Fetch active release version
        cursor = conn.execute("SELECT version FROM releases WHERE is_active = 1 LIMIT 1")
        release_row = cursor.fetchone()
        release_version = release_row["version"] if release_row else "v1.0.0"

        # 2. Check for existing idempotency key before attempting insert
        if idempotency_key:
            cursor = conn.execute("SELECT * FROM jobs WHERE idempotency_key = ?", (idempotency_key,))
            existing_row = cursor.fetchone()
            if existing_row:
                existing_job = _row_to_job_dict(existing_row)
                record_audit_event(
                    conn,
                    event_type="DEDUP_HIT",
                    actor="api",
                    job_id=existing_job["id"],
                    severity="INFO",
                    details={
                        "idempotency_key": idempotency_key,
                        "reason": "Duplicate submission detected with identical idempotency key",
                        "status": existing_job["status"],
                    },
                )
                return existing_job, True

        # 3. Insert new job record
        try:
            conn.execute(
                """
                INSERT INTO jobs (
                    id, idempotency_key, job_type, payload, status,
                    priority, release_version, attempt_count, max_retries,
                    run_at, leased_by, lease_token, lease_expires_at,
                    result, last_error, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, 'QUEUED',
                    ?, ?, 0, ?,
                    ?, NULL, NULL, NULL,
                    NULL, NULL, ?, ?
                )
                """,
                (
                    job_id,
                    idempotency_key,
                    job_type.strip(),
                    payload_json,
                    priority,
                    release_version,
                    max_retries,
                    scheduled_run_at,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as e:
            # Handle concurrent race where another process inserted the same idempotency_key
            if idempotency_key and "idempotency_key" in str(e).lower():
                cursor = conn.execute("SELECT * FROM jobs WHERE idempotency_key = ?", (idempotency_key,))
                existing_row = cursor.fetchone()
                if existing_row:
                    existing_job = _row_to_job_dict(existing_row)
                    record_audit_event(
                        conn,
                        event_type="DEDUP_HIT",
                        actor="api",
                        job_id=existing_job["id"],
                        severity="INFO",
                        details={
                            "idempotency_key": idempotency_key,
                            "reason": "Concurrent duplicate submission detected",
                            "status": existing_job["status"],
                        },
                    )
                    return existing_job, True
            raise

        # 4. Record JOB_ENQUEUED audit event
        record_audit_event(
            conn,
            event_type="JOB_ENQUEUED",
            actor="api",
            job_id=job_id,
            severity="INFO",
            details={
                "job_type": job_type.strip(),
                "priority": priority,
                "release_version": release_version,
                "max_retries": max_retries,
                "run_at": scheduled_run_at,
                "idempotency_key": idempotency_key,
            },
        )

        cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        new_row = cursor.fetchone()
        return _row_to_job_dict(new_row), False


def get_job(conn: sqlite3.Connection, job_id: str) -> Optional[dict[str, Any]]:
    """
    Fetches a job by ID. Returns None if not found.
    """
    cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cursor.fetchone()
    return _row_to_job_dict(row) if row else None


def list_jobs(
    conn: sqlite3.Connection,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """
    Lists jobs optionally filtered by status, ordered by creation time descending.
    """
    if status:
        cursor = conn.execute(
            """
            SELECT * FROM jobs
            WHERE status = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            (status, limit, offset),
        )
    else:
        cursor = conn.execute(
            """
            SELECT * FROM jobs
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )
    return [_row_to_job_dict(row) for row in cursor.fetchall()]


def claim_next_job(
    conn: sqlite3.Connection,
    worker_id: str,
    lease_duration: float = DEFAULT_LEASE_DURATION_SECONDS,
) -> Optional[dict[str, Any]]:
    """
    Atomically claims the highest priority eligible job (status='QUEUED' and run_at <= current_time).

    Transitions job to RUNNING, increments attempt_count, assigns worker_id and unique lease_token,
    sets lease expiration, updates updated_at, and records a LEASE_ACQUIRED audit event.

    Returns the claimed job dict, or None if no eligible job is available.
    """
    if not worker_id:
        raise ValueError("Worker ID must be provided to claim a job.")

    with transaction(conn, mode="IMMEDIATE"):
        now = time.time()

        # Find candidate job
        cursor = conn.execute(
            """
            SELECT * FROM jobs
            WHERE status = 'QUEUED' AND run_at <= ?
            ORDER BY priority DESC, run_at ASC, created_at ASC
            LIMIT 1
            """,
            (now,),
        )
        job_row = cursor.fetchone()
        if not job_row:
            return None

        job_id = job_row["id"]
        new_attempt = job_row["attempt_count"] + 1
        lease_token = str(uuid.uuid4())
        lease_expires_at = now + lease_duration

        # Atomically transition job to RUNNING
        update_cursor = conn.execute(
            """
            UPDATE jobs
            SET status = 'RUNNING',
                attempt_count = ?,
                leased_by = ?,
                lease_token = ?,
                lease_expires_at = ?,
                updated_at = ?
            WHERE id = ? AND status = 'QUEUED'
            """,
            (new_attempt, worker_id, lease_token, lease_expires_at, now, job_id),
        )

        if update_cursor.rowcount != 1:
            # Another concurrent transaction claimed this job
            return None

        # Record LEASE_ACQUIRED audit event
        record_audit_event(
            conn,
            event_type="LEASE_ACQUIRED",
            actor=f"worker:{worker_id}",
            job_id=job_id,
            severity="INFO",
            details={
                "attempt": new_attempt,
                "lease_token": lease_token,
                "lease_duration": lease_duration,
                "lease_expires_at": lease_expires_at,
            },
        )

        updated_cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        return _row_to_job_dict(updated_cursor.fetchone())


def complete_job(
    conn: sqlite3.Connection,
    job_id: str,
    worker_id: str,
    lease_token: str,
    result: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Safely transitions a RUNNING job to COMPLETED.

    Verifies:
    - job status is RUNNING
    - worker_id matches leased_by
    - lease_token matches current lease_token

    Clears lease ownership fields, persists result, and logs JOB_COMPLETED audit event.
    Raises LeaseAuthorizationError if caller is not authorized.
    """
    now = time.time()
    result_json = json.dumps(result) if result is not None else None

    with transaction(conn, mode="IMMEDIATE"):
        update_cursor = conn.execute(
            """
            UPDATE jobs
            SET status = 'COMPLETED',
                result = ?,
                leased_by = NULL,
                lease_token = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE id = ? AND status = 'RUNNING' AND leased_by = ? AND lease_token = ?
            """,
            (result_json, now, job_id, worker_id, lease_token),
        )

        if update_cursor.rowcount != 1:
            # Query job state to provide detailed authorization error
            cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            if not row:
                raise JobNotFoundError(f"Job '{job_id}' not found.")
            raise LeaseAuthorizationError(
                f"Unauthorized completion attempt by worker '{worker_id}' with token '{lease_token}'. "
                f"Current state: status='{row['status']}', leased_by='{row['leased_by']}'."
            )

        # Record JOB_COMPLETED audit event
        record_audit_event(
            conn,
            event_type="JOB_COMPLETED",
            actor=f"worker:{worker_id}",
            job_id=job_id,
            severity="INFO",
            details={
                "result": result,
                "reason": "Job processing completed successfully",
            },
        )

        cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        return _row_to_job_dict(cursor.fetchone())


def requeue_job(
    conn: sqlite3.Connection,
    job_id: str,
    worker_id: str,
    lease_token: str,
    reason: Optional[str] = None,
) -> dict[str, Any]:
    """
    Safely moves a currently leased RUNNING job back to QUEUED.

    Verifies:
    - job status is RUNNING
    - worker_id matches leased_by
    - lease_token matches current lease_token

    Clears lease ownership fields, resets status to QUEUED, and logs JOB_REQUEUED audit event.
    Raises LeaseAuthorizationError if caller is not authorized.
    """
    now = time.time()

    with transaction(conn, mode="IMMEDIATE"):
        update_cursor = conn.execute(
            """
            UPDATE jobs
            SET status = 'QUEUED',
                leased_by = NULL,
                lease_token = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE id = ? AND status = 'RUNNING' AND leased_by = ? AND lease_token = ?
            """,
            (now, job_id, worker_id, lease_token),
        )

        if update_cursor.rowcount != 1:
            cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            if not row:
                raise JobNotFoundError(f"Job '{job_id}' not found.")
            raise LeaseAuthorizationError(
                f"Unauthorized requeue attempt by worker '{worker_id}' with token '{lease_token}'. "
                f"Current state: status='{row['status']}', leased_by='{row['leased_by']}'."
            )

        # Record JOB_REQUEUED audit event
        record_audit_event(
            conn,
            event_type="JOB_REQUEUED",
            actor=f"worker:{worker_id}",
            job_id=job_id,
            severity="WARN",
            details={
                "reason": reason or "Job requeued by worker",
            },
        )

        cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        return _row_to_job_dict(cursor.fetchone())


def extend_lease(
    conn: sqlite3.Connection,
    job_id: str,
    worker_id: str,
    lease_token: str,
    extension_duration: float = DEFAULT_LEASE_DURATION_SECONDS,
) -> bool:
    """
    Extends the lease expiration of an active RUNNING job.

    Verifies:
    - job status is RUNNING
    - worker_id matches leased_by
    - lease_token matches current lease_token

    Returns True if extended successfully, False if lease was stolen, expired, or worker mismatch.
    """
    now = time.time()
    new_expires_at = now + extension_duration

    with transaction(conn, mode="IMMEDIATE"):
        cursor = conn.execute(
            """
            UPDATE jobs
            SET lease_expires_at = ?,
                updated_at = ?
            WHERE id = ? AND status = 'RUNNING' AND leased_by = ? AND lease_token = ?
            """,
            (new_expires_at, now, job_id, worker_id, lease_token),
        )

        if cursor.rowcount == 1:
            record_audit_event(
                conn,
                event_type="LEASE_EXTENDED",
                actor=f"worker:{worker_id}",
                job_id=job_id,
                severity="INFO",
                details={
                    "lease_duration": extension_duration,
                    "new_lease_expires_at": new_expires_at,
                },
            )
            return True
        return False


def fail_job(
    conn: sqlite3.Connection,
    job_id: str,
    worker_id: str,
    lease_token: str,
    error_msg: str,
    base_delay: float = DEFAULT_RETRY_BASE_DELAY_SECONDS,
    backoff_factor: float = DEFAULT_RETRY_BACKOFF_FACTOR,
    max_delay: float = DEFAULT_RETRY_MAX_DELAY_SECONDS,
) -> dict[str, Any]:
    """
    Handles a job failure during worker execution.

    Applies deterministic exponential backoff retry:
    - If attempt_count < max_retries:
        Calculates delay = min(max_delay, base_delay * (backoff_factor ** (attempt - 1)))
        Resets status to QUEUED with run_at = now + delay
        Clears lease fields
        Emits JOB_FAILED and RETRY_SCHEDULED audit events
    - If attempt_count >= max_retries:
        Transitions status to DEAD_LETTER
        Clears lease fields
        Emits JOB_FAILED and DEAD_LETTERED audit events

    Raises LeaseAuthorizationError if caller is not the authorized lease holder.
    """
    now = time.time()

    with transaction(conn, mode="IMMEDIATE"):
        cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        job_row = cursor.fetchone()
        if not job_row:
            raise JobNotFoundError(f"Job '{job_id}' not found.")
        if job_row["status"] != "RUNNING" or job_row["leased_by"] != worker_id or job_row["lease_token"] != lease_token:
            raise LeaseAuthorizationError(
                f"Unauthorized fail attempt by worker '{worker_id}' with token '{lease_token}'."
            )

        attempt_count = job_row["attempt_count"]
        max_retries = job_row["max_retries"]

        # Record failure event
        record_audit_event(
            conn,
            event_type="JOB_FAILED",
            actor=f"worker:{worker_id}",
            job_id=job_id,
            severity="WARN",
            details={
                "attempt": attempt_count,
                "max_retries": max_retries,
                "error": error_msg,
            },
        )

        if attempt_count < max_retries:
            # Exponential backoff formula: base * (factor ** (attempt - 1))
            delay = min(max_delay, base_delay * (backoff_factor ** (attempt_count - 1)))
            next_run_at = now + delay

            conn.execute(
                """
                UPDATE jobs
                SET status = 'QUEUED',
                    run_at = ?,
                    last_error = ?,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (next_run_at, error_msg, now, job_id),
            )

            record_audit_event(
                conn,
                event_type="RETRY_SCHEDULED",
                actor=f"worker:{worker_id}",
                job_id=job_id,
                severity="INFO",
                details={
                    "attempt": attempt_count,
                    "max_retries": max_retries,
                    "delay_seconds": delay,
                    "next_run_at": next_run_at,
                    "error": error_msg,
                },
            )
        else:
            # Max retries reached -> move to DEAD_LETTER
            conn.execute(
                """
                UPDATE jobs
                SET status = 'DEAD_LETTER',
                    last_error = ?,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (error_msg, now, job_id),
            )

            record_audit_event(
                conn,
                event_type="DEAD_LETTERED",
                actor=f"worker:{worker_id}",
                job_id=job_id,
                severity="ERROR",
                details={
                    "attempt": attempt_count,
                    "max_retries": max_retries,
                    "error": error_msg,
                    "reason": "Max retries exhausted",
                },
            )

        updated_row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return _row_to_job_dict(updated_row)


def register_worker(conn: sqlite3.Connection, worker_id: str, pid: int) -> dict[str, Any]:
    """
    Registers a worker process in the workers table in IDLE status.
    Safe for restarted workers (upsert). Emits WORKER_STARTED audit event.
    """
    now = time.time()
    with transaction(conn, mode="IMMEDIATE"):
        conn.execute(
            """
            INSERT INTO workers (id, pid, status, current_job_id, last_heartbeat_at, started_at)
            VALUES (?, ?, 'IDLE', NULL, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                pid = excluded.pid,
                status = 'IDLE',
                current_job_id = NULL,
                last_heartbeat_at = excluded.last_heartbeat_at,
                started_at = excluded.started_at
            """,
            (worker_id, pid, now, now),
        )

        record_audit_event(
            conn,
            event_type="WORKER_STARTED",
            actor=f"worker:{worker_id}",
            severity="INFO",
            details={"pid": pid, "status": "IDLE"},
        )

        row = conn.execute("SELECT * FROM workers WHERE id = ?", (worker_id,)).fetchone()
        return dict(row)


def update_worker_heartbeat(
    conn: sqlite3.Connection,
    worker_id: str,
    status: Optional[str] = None,
    current_job_id: Optional[str] = None,
    clear_job: bool = False,
) -> bool:
    """
    Updates a worker's last_heartbeat_at timestamp and optionally its status or current_job_id.
    If clear_job is True, sets current_job_id to NULL.
    """
    now = time.time()
    with transaction(conn, mode="IMMEDIATE"):
        if clear_job:
            if status is not None:
                cursor = conn.execute(
                    "UPDATE workers SET last_heartbeat_at = ?, status = ?, current_job_id = NULL WHERE id = ?",
                    (now, status, worker_id),
                )
            else:
                cursor = conn.execute(
                    "UPDATE workers SET last_heartbeat_at = ?, current_job_id = NULL WHERE id = ?",
                    (now, worker_id),
                )
        elif current_job_id is not None:
            if status is not None:
                cursor = conn.execute(
                    "UPDATE workers SET last_heartbeat_at = ?, status = ?, current_job_id = ? WHERE id = ?",
                    (now, status, current_job_id, worker_id),
                )
            else:
                cursor = conn.execute(
                    "UPDATE workers SET last_heartbeat_at = ?, current_job_id = ? WHERE id = ?",
                    (now, current_job_id, worker_id),
                )
        elif status is not None:
            cursor = conn.execute(
                "UPDATE workers SET last_heartbeat_at = ?, status = ? WHERE id = ?",
                (now, status, worker_id),
            )
        else:
            cursor = conn.execute(
                "UPDATE workers SET last_heartbeat_at = ? WHERE id = ?",
                (now, worker_id),
            )
        return cursor.rowcount > 0


def get_worker(conn: sqlite3.Connection, worker_id: str) -> Optional[dict[str, Any]]:
    """
    Retrieves worker record by worker_id.
    """
    row = conn.execute("SELECT * FROM workers WHERE id = ?", (worker_id,)).fetchone()
    return dict(row) if row else None


def list_workers(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """
    Lists all workers ordered by start time.
    """
    rows = conn.execute("SELECT * FROM workers ORDER BY started_at ASC").fetchall()
    return [dict(r) for r in rows]


def retry_job(
    conn: sqlite3.Connection,
    job_id: str,
    actor: str = "operator",
) -> dict[str, Any]:
    """
    Manually re-queues a failed or dead-letter job for execution.
    Resets status to QUEUED, clears lease tokens, and extends max_retries
    if needed so the job can be claimed and executed by workers.
    """
    now = time.time()
    with transaction(conn, mode="IMMEDIATE"):
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            raise JobNotFoundError(f"Job '{job_id}' not found.")

        current_attempts = row["attempt_count"]
        current_max = row["max_retries"]
        new_max = max(current_max, current_attempts + 3)

        conn.execute(
            """
            UPDATE jobs
            SET status = 'QUEUED',
                run_at = ?,
                max_retries = ?,
                leased_by = NULL,
                lease_token = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (now, new_max, now, job_id),
        )

        record_audit_event(
            conn,
            event_type="JOB_REQUEUED",
            actor=actor,
            job_id=job_id,
            severity="INFO",
            details={
                "action": "Manual retry initiated by operator",
                "previous_status": row["status"],
                "previous_attempts": current_attempts,
                "new_max_retries": new_max,
            },
        )

        updated = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return _row_to_job_dict(updated)

