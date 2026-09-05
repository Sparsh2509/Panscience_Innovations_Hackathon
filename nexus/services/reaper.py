"""
Reaper service for NEXUS.
Detects expired leases on RUNNING jobs, marks dead workers, and transactionally recovers orphaned jobs.
"""

import sqlite3
import time
from typing import Any, Optional

from nexus.core.config import (
    DEFAULT_RETRY_BACKOFF_FACTOR,
    DEFAULT_RETRY_BASE_DELAY_SECONDS,
    DEFAULT_RETRY_MAX_DELAY_SECONDS,
)
from nexus.core.db import transaction
from nexus.services.audit_service import record_audit_event
from nexus.services.job_service import _row_to_job_dict


def reap_expired_jobs(
    conn: sqlite3.Connection,
    base_delay: float = DEFAULT_RETRY_BASE_DELAY_SECONDS,
    backoff_factor: float = DEFAULT_RETRY_BACKOFF_FACTOR,
    max_delay: float = DEFAULT_RETRY_MAX_DELAY_SECONDS,
    actor: str = "reaper",
) -> list[dict[str, Any]]:
    """
    Scans for RUNNING jobs where lease_expires_at <= current_time.

    For each expired job:
    1. Transactionally re-verifies that the lease is still expired under BEGIN IMMEDIATE.
    2. Marks the holding worker as DEAD if registered.
    3. If attempts < max_retries:
         Computes exponential backoff delay and moves job to QUEUED with run_at = now + delay.
         Emits LEASE_EXPIRED, WORKER_CRASHED, and JOB_RECOVERED audit events.
       Else:
         Moves job to DEAD_LETTER.
         Emits LEASE_EXPIRED and DEAD_LETTERED audit events.
    4. Clears worker lease ownership.

    Returns the list of recovered job dictionaries.
    """
    now = time.time()

    # Step 1: Query candidates outside transaction for speed
    candidates = conn.execute(
        """
        SELECT id, leased_by, lease_token, lease_expires_at
        FROM jobs
        WHERE status = 'RUNNING'
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at <= ?
        """,
        (now,),
    ).fetchall()

    recovered_jobs = []

    for cand in candidates:
        job_id = cand["id"]
        expected_token = cand["lease_token"]

        with transaction(conn, mode="IMMEDIATE"):
            # Step 2: Atomic re-verification: Ensure lease is STILL expired and hasn't been extended/completed
            cursor = conn.execute(
                """
                SELECT * FROM jobs
                WHERE id = ?
                  AND status = 'RUNNING'
                  AND lease_expires_at <= ?
                  AND lease_token = ?
                """,
                (job_id, now, expected_token),
            )
            job_row = cursor.fetchone()
            if not job_row:
                # Job was legitimately extended or completed concurrently
                continue

            stale_worker = job_row["leased_by"]
            attempt_count = job_row["attempt_count"]
            max_retries = job_row["max_retries"]
            error_msg = f"Orphaned job recovered by reaper: lease expired from worker '{stale_worker}'"

            # Step 3: Mark stale worker as DEAD
            if stale_worker:
                conn.execute(
                    "UPDATE workers SET status = 'DEAD' WHERE id = ? AND status != 'DEAD'",
                    (stale_worker,),
                )
                record_audit_event(
                    conn,
                    event_type="WORKER_CRASHED",
                    actor=actor,
                    job_id=job_id,
                    severity="WARN",
                    details={
                        "worker_id": stale_worker,
                        "reason": f"Worker lease expired without renewal (expired at {job_row['lease_expires_at']}, reaped at {now})",
                    },
                )

            # Step 4: Record LEASE_EXPIRED
            record_audit_event(
                conn,
                event_type="LEASE_EXPIRED",
                actor=actor,
                job_id=job_id,
                severity="WARN",
                details={
                    "stale_worker": stale_worker,
                    "lease_token": expected_token,
                    "lease_expires_at": job_row["lease_expires_at"],
                    "reaped_at": now,
                    "attempt": attempt_count,
                },
            )

            # Step 5: Bounded retry or dead-letter decision
            if attempt_count < max_retries:
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
                    event_type="JOB_RECOVERED",
                    actor=actor,
                    job_id=job_id,
                    severity="INFO",
                    details={
                        "previous_worker": stale_worker,
                        "attempt": attempt_count,
                        "max_retries": max_retries,
                        "next_run_at": next_run_at,
                        "delay_seconds": delay,
                        "action": "Requeued with exponential backoff",
                    },
                )
            else:
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
                    actor=actor,
                    job_id=job_id,
                    severity="ERROR",
                    details={
                        "previous_worker": stale_worker,
                        "attempt": attempt_count,
                        "max_retries": max_retries,
                        "reason": "Max retries exhausted after lease expiry",
                    },
                )

            updated = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            recovered_jobs.append(_row_to_job_dict(updated))

    return recovered_jobs
