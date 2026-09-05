"""
Tests for Phase 2: Durable Job Queue, Idempotency, and Atomic Leasing.
"""

import gc
import tempfile
import time
from pathlib import Path
import pytest

from nexus.core.db import get_db, init_db
from nexus.services.audit_service import get_audit_events_for_job
from nexus.services.job_service import (
    JobValidationError,
    LeaseAuthorizationError,
    claim_next_job,
    complete_job,
    create_job,
    get_job,
    list_jobs,
    requeue_job,
)


@pytest.fixture
def temp_db():
    """Provides an initialized isolated database connection for testing."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "test_phase2.db"
        init_db(db_path)
        conn = get_db(db_path)
        yield conn, db_path
        conn.close()
        gc.collect()


# --- A. Job Creation Tests ---
def test_job_creation_basic(temp_db):
    conn, _ = temp_db
    payload = {"task": "invoice_generate", "customer_id": 42}
    job, is_dedup = create_job(conn, job_type="billing", payload=payload)

    assert not is_dedup
    assert job["id"] is not None
    assert job["job_type"] == "billing"
    assert job["payload"] == payload
    assert job["status"] == "QUEUED"
    assert job["attempt_count"] == 0
    assert job["release_version"] == "v1.0.0"
    assert job["priority"] == 0
    assert job["leased_by"] is None
    assert job["lease_token"] is None
    assert job["lease_expires_at"] is None


def test_job_creation_validation(temp_db):
    conn, _ = temp_db
    with pytest.raises(JobValidationError):
        create_job(conn, job_type="", payload={"key": "val"})

    with pytest.raises(JobValidationError):
        create_job(conn, job_type="valid", payload=None)  # type: ignore


# --- B. Durability Tests ---
def test_job_durability_across_connections(temp_db):
    conn, db_path = temp_db
    payload = {"account": "acc_99"}
    job, _ = create_job(conn, job_type="sync", payload=payload)
    job_id = job["id"]

    # Close the current connection
    conn.close()

    # Reconnect to the database file
    new_conn = get_db(db_path)
    try:
        persisted_job = get_job(new_conn, job_id)
        assert persisted_job is not None
        assert persisted_job["id"] == job_id
        assert persisted_job["payload"] == payload
        assert persisted_job["status"] == "QUEUED"
    finally:
        new_conn.close()


# --- C. Idempotency Tests ---
def test_idempotency_duplicate_submission(temp_db):
    conn, _ = temp_db
    idempotency_key = "req_unique_12345"
    payload = {"order_id": 1001}

    # First submission
    job1, is_dedup1 = create_job(
        conn,
        job_type="order_process",
        payload=payload,
        idempotency_key=idempotency_key,
    )
    assert not is_dedup1
    assert job1["idempotency_key"] == idempotency_key

    # Second submission with identical key
    job2, is_dedup2 = create_job(
        conn,
        job_type="order_process",
        payload={"order_id": 9999},  # even with different payload, same key
        idempotency_key=idempotency_key,
    )
    assert is_dedup2
    assert job1["id"] == job2["id"]

    # Verify only ONE job exists in database
    jobs = list_jobs(conn)
    assert len(jobs) == 1

    # Verify DEDUP_HIT audit event is recorded
    audits = get_audit_events_for_job(conn, job1["id"])
    event_types = [a["event_type"] for a in audits]
    assert "JOB_ENQUEUED" in event_types
    assert "DEDUP_HIT" in event_types


# --- D. Concurrent / Idempotent Safety ---
def test_unique_constraint_enforced(temp_db):
    conn, _ = temp_db
    key = "idem_strict_key"

    create_job(conn, job_type="t", payload={}, idempotency_key=key)

    # Attempt direct insert with existing key outside service
    with pytest.raises(Exception):
        conn.execute(
            """
            INSERT INTO jobs (id, idempotency_key, job_type, payload, status, release_version, run_at, created_at, updated_at)
            VALUES ('different-id', ?, 't', '{}', 'QUEUED', 'v1.0.0', 1.0, 1.0, 1.0)
            """,
            (key,),
        )


# --- E. Atomic Claiming Tests ---
def test_atomic_claim_next_job(temp_db):
    conn, _ = temp_db
    job, _ = create_job(conn, job_type="email", payload={"to": "user@test.com"})

    # Worker 1 claims job
    claimed_1 = claim_next_job(conn, worker_id="worker-alpha", lease_duration=5.0)
    assert claimed_1 is not None
    assert claimed_1["id"] == job["id"]
    assert claimed_1["status"] == "RUNNING"
    assert claimed_1["leased_by"] == "worker-alpha"
    assert claimed_1["attempt_count"] == 1
    assert claimed_1["lease_token"] is not None
    assert claimed_1["lease_expires_at"] > time.time()

    # Worker 2 attempts to claim while job is RUNNING
    claimed_2 = claim_next_job(conn, worker_id="worker-beta", lease_duration=5.0)
    assert claimed_2 is None


def test_future_scheduled_jobs_not_claimed(temp_db):
    conn, _ = temp_db
    future_time = time.time() + 60.0  # scheduled 1 minute in the future
    create_job(conn, job_type="future_task", payload={}, run_at=future_time)

    # Should not be claimable yet
    claimed = claim_next_job(conn, worker_id="worker-1")
    assert claimed is None


# --- F. Lease Ownership Tests ---
def test_lease_ownership_fields(temp_db):
    conn, _ = temp_db
    create_job(conn, job_type="task", payload={"step": 1})

    claimed = claim_next_job(conn, worker_id="worker-77", lease_duration=12.0)
    assert claimed is not None
    assert claimed["leased_by"] == "worker-77"
    assert isinstance(claimed["lease_token"], str)
    assert len(claimed["lease_token"]) > 10
    assert claimed["lease_expires_at"] >= time.time() + 11.0


# --- G. Completion Authorization Tests ---
def test_completion_authorization(temp_db):
    conn, _ = temp_db
    create_job(conn, job_type="compute", payload={"data": 123})
    claimed = claim_next_job(conn, worker_id="worker-1")
    job_id = claimed["id"]
    valid_token = claimed["lease_token"]

    # Wrong worker cannot complete
    with pytest.raises(LeaseAuthorizationError):
        complete_job(conn, job_id=job_id, worker_id="worker-imposter", lease_token=valid_token)

    # Wrong lease token cannot complete
    with pytest.raises(LeaseAuthorizationError):
        complete_job(conn, job_id=job_id, worker_id="worker-1", lease_token="fake-token-999")

    # Correct worker + valid token succeeds
    completed = complete_job(
        conn,
        job_id=job_id,
        worker_id="worker-1",
        lease_token=valid_token,
        result={"status": "success", "processed_records": 10},
    )
    assert completed["status"] == "COMPLETED"
    assert completed["result"] == {"status": "success", "processed_records": 10}
    assert completed["leased_by"] is None
    assert completed["lease_token"] is None
    assert completed["lease_expires_at"] is None

    # Stale attempt on already completed job fails
    with pytest.raises(LeaseAuthorizationError):
        complete_job(conn, job_id=job_id, worker_id="worker-1", lease_token=valid_token)


# --- H. Requeue Authorization Tests ---
def test_requeue_authorization(temp_db):
    conn, _ = temp_db
    create_job(conn, job_type="transient", payload={"val": 1})
    claimed = claim_next_job(conn, worker_id="worker-1")
    job_id = claimed["id"]
    valid_token = claimed["lease_token"]

    # Wrong worker cannot requeue
    with pytest.raises(LeaseAuthorizationError):
        requeue_job(conn, job_id=job_id, worker_id="worker-wrong", lease_token=valid_token)

    # Wrong token cannot requeue
    with pytest.raises(LeaseAuthorizationError):
        requeue_job(conn, job_id=job_id, worker_id="worker-1", lease_token="invalid-token")

    # Correct worker + token successfully requeues
    requeued = requeue_job(
        conn,
        job_id=job_id,
        worker_id="worker-1",
        lease_token=valid_token,
        reason="Temporary upstream timeout",
    )
    assert requeued["status"] == "QUEUED"
    assert requeued["leased_by"] is None
    assert requeued["lease_token"] is None
    assert requeued["lease_expires_at"] is None

    # Now another worker can claim it
    claimed_again = claim_next_job(conn, worker_id="worker-2")
    assert claimed_again is not None
    assert claimed_again["id"] == job_id
    assert claimed_again["leased_by"] == "worker-2"
    assert claimed_again["attempt_count"] == 2


# --- I. Audit Trail Verification ---
def test_audit_events_immutable_and_comprehensive(temp_db):
    conn, _ = temp_db
    idempotency_key = "audit_idem_key"
    job, _ = create_job(conn, job_type="audited", payload={"x": 1}, idempotency_key=idempotency_key)
    job_id = job["id"]

    # Duplicate call
    create_job(conn, job_type="audited", payload={"x": 2}, idempotency_key=idempotency_key)

    # Claim
    claimed = claim_next_job(conn, worker_id="worker-auditor")
    token = claimed["lease_token"]

    # Requeue
    requeue_job(conn, job_id=job_id, worker_id="worker-auditor", lease_token=token, reason="Need more data")

    # Claim again
    claimed_2 = claim_next_job(conn, worker_id="worker-auditor-2")
    token_2 = claimed_2["lease_token"]

    # Complete
    complete_job(conn, job_id=job_id, worker_id="worker-auditor-2", lease_token=token_2, result={"done": True})

    # Retrieve all audit events for this job
    events = get_audit_events_for_job(conn, job_id)
    event_types = [e["event_type"] for e in events]

    assert event_types == [
        "JOB_ENQUEUED",
        "DEDUP_HIT",
        "LEASE_ACQUIRED",
        "JOB_REQUEUED",
        "LEASE_ACQUIRED",
        "JOB_COMPLETED",
    ]

    # Verify immutable details
    for e in events:
        assert e["created_at"] > 0
        assert e["actor"] in ["api", "worker:worker-auditor", "worker:worker-auditor-2"]
        assert isinstance(e["details"], dict)
