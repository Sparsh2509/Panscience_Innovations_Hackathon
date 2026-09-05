"""
Tests for Phase 3: Worker Runtime, Heartbeats, Reaper, Retries, and Crash Recovery.
"""

import gc
import os
import subprocess
import tempfile
import time
from pathlib import Path
import pytest

from nexus.core.db import get_db, init_db
from nexus.services.audit_service import get_audit_events_for_job, list_audit_events
from nexus.services.job_service import (
    claim_next_job,
    create_job,
    extend_lease,
    fail_job,
    get_job,
    get_worker,
    register_worker,
    update_worker_heartbeat,
)
from nexus.services.reaper import reap_expired_jobs
from nexus.workers.supervisor import WorkerSupervisor
from nexus.workers.worker import Worker


@pytest.fixture
def temp_db():
    """Provides an isolated initialized temporary SQLite database."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "test_phase3.db"
        init_db(db_path)
        conn = get_db(db_path)
        yield conn, db_path
        conn.close()
        gc.collect()


# --- A. Worker Registration Tests ---
def test_worker_registration(temp_db):
    conn, _ = temp_db
    worker_rec = register_worker(conn, worker_id="worker-test-1", pid=os.getpid())

    assert worker_rec["id"] == "worker-test-1"
    assert worker_rec["pid"] == os.getpid()
    assert worker_rec["status"] == "IDLE"
    assert worker_rec["last_heartbeat_at"] > 0
    assert worker_rec["started_at"] > 0

    # Verify query
    fetched = get_worker(conn, "worker-test-1")
    assert fetched is not None
    assert fetched["status"] == "IDLE"


# --- B & C. Worker Claims, Executes, and Returns to IDLE ---
def test_worker_executes_and_returns_to_idle(temp_db):
    conn, db_path = temp_db
    # Create an echo job
    create_job(conn, job_type="echo", payload={"message": "hello nexus"})

    worker = Worker(worker_id="worker-exec", db_path=db_path)
    worker.register()

    # Process single job
    result = worker.run_once()
    assert result is not None
    assert result["status"] == "COMPLETED"
    assert result["result"] == {"status": "ok", "echo": "hello nexus"}

    # Verify worker returned to IDLE
    worker_rec = get_worker(conn, "worker-exec")
    assert worker_rec["status"] == "IDLE"
    assert worker_rec["current_job_id"] is None


# --- D. Heartbeat & Lease Extension Tests ---
def test_heartbeat_and_lease_extension(temp_db):
    conn, _ = temp_db
    register_worker(conn, "worker-hb", os.getpid())
    time.sleep(0.05)

    # 1. Test heartbeat timestamp update
    before = get_worker(conn, "worker-hb")["last_heartbeat_at"]
    time.sleep(0.05)
    updated = update_worker_heartbeat(conn, "worker-hb", status="BUSY")
    assert updated is True
    after = get_worker(conn, "worker-hb")["last_heartbeat_at"]
    assert after > before

    # 2. Test lease extension
    job, _ = create_job(conn, job_type="sleep", payload={"seconds": 1})
    claimed = claim_next_job(conn, worker_id="worker-hb", lease_duration=5.0)
    original_expiry = claimed["lease_expires_at"]

    time.sleep(0.05)
    # Valid worker + token extends lease
    extended = extend_lease(
        conn,
        job_id=claimed["id"],
        worker_id="worker-hb",
        lease_token=claimed["lease_token"],
        extension_duration=10.0,
    )
    assert extended is True
    updated_job = get_job(conn, claimed["id"])
    assert updated_job["lease_expires_at"] > original_expiry

    # Wrong worker cannot extend lease
    stolen = extend_lease(
        conn,
        job_id=claimed["id"],
        worker_id="worker-imposter",
        lease_token=claimed["lease_token"],
        extension_duration=10.0,
    )
    assert stolen is False

    # Wrong lease token cannot extend lease
    invalid_token = extend_lease(
        conn,
        job_id=claimed["id"],
        worker_id="worker-hb",
        lease_token="invalid-token-123",
        extension_duration=10.0,
    )
    assert invalid_token is False


# --- E. Retry with Exponential Backoff ---
def test_retry_exponential_backoff(temp_db):
    conn, _ = temp_db
    job, _ = create_job(conn, job_type="fail", payload={"message": "fail 1"}, max_retries=3)
    job_id = job["id"]

    # Claim attempt 1
    c1 = claim_next_job(conn, "worker-retry")
    assert c1["attempt_count"] == 1

    # Fail attempt 1
    t0 = time.time()
    base_delay = 1.0
    backoff_factor = 2.0
    f1 = fail_job(
        conn,
        job_id=job_id,
        worker_id="worker-retry",
        lease_token=c1["lease_token"],
        error_msg="transient network drop",
        base_delay=base_delay,
        backoff_factor=backoff_factor,
    )
    # Attempt 1: delay = 1.0 * (2 ** 0) = 1.0s
    assert f1["status"] == "QUEUED"
    assert f1["attempt_count"] == 1
    assert f1["run_at"] >= t0 + 0.95
    assert f1["last_error"] == "transient network drop"

    # Verify audit event for retry
    audits = get_audit_events_for_job(conn, job_id)
    types = [a["event_type"] for a in audits]
    assert "JOB_FAILED" in types
    assert "RETRY_SCHEDULED" in types


# --- F. Retry Limit & Dead-Letter State ---
def test_retry_limit_dead_letter(temp_db):
    conn, _ = temp_db
    job, _ = create_job(conn, job_type="fail", payload={}, max_retries=2)
    job_id = job["id"]

    # Attempt 1
    c1 = claim_next_job(conn, "w1")
    fail_job(conn, job_id, "w1", c1["lease_token"], "error 1", base_delay=0.01)

    # Re-enable claimable run_at for test speed
    conn.execute("UPDATE jobs SET run_at = 0 WHERE id = ?", (job_id,))

    # Attempt 2 (reaches max_retries=2)
    c2 = claim_next_job(conn, "w1")
    assert c2["attempt_count"] == 2
    f2 = fail_job(conn, job_id, "w1", c2["lease_token"], "error 2")

    # Should now be DEAD_LETTER
    assert f2["status"] == "DEAD_LETTER"
    assert f2["last_error"] == "error 2"

    # Ensure no further claims are possible
    assert claim_next_job(conn, "w1") is None

    # Verify DEAD_LETTERED audit event
    audits = get_audit_events_for_job(conn, job_id)
    types = [a["event_type"] for a in audits]
    assert "DEAD_LETTERED" in types


# --- G. Reaper Service Recovery ---
def test_reaper_detects_and_recovers_expired_lease(temp_db):
    conn, _ = temp_db
    job, _ = create_job(conn, job_type="sleep", payload={"seconds": 5}, max_retries=3)
    job_id = job["id"]

    # Claim job with very short lease
    claimed = claim_next_job(conn, "worker-crash", lease_duration=0.1)

    # Wait for lease to expire
    time.sleep(0.2)

    # Run reaper
    recovered = reap_expired_jobs(conn, base_delay=0.5)
    assert len(recovered) == 1
    assert recovered[0]["id"] == job_id
    assert recovered[0]["status"] == "QUEUED"
    assert recovered[0]["leased_by"] is None
    assert recovered[0]["lease_token"] is None
    assert "reaper" in recovered[0]["last_error"].lower()

    # Verify audit events
    audits = get_audit_events_for_job(conn, job_id)
    types = [a["event_type"] for a in audits]
    assert "LEASE_EXPIRED" in types
    assert "JOB_RECOVERED" in types


# --- H. Crash Recovery End-to-End ---
def test_crash_recovery_with_second_worker(temp_db):
    conn, db_path = temp_db
    # Create job
    job, _ = create_job(conn, job_type="echo", payload={"message": "resilient"}, max_retries=3)
    job_id = job["id"]

    # Worker 1 claims job
    claimed = claim_next_job(conn, "worker-1", lease_duration=0.1)

    # Simulate Worker 1 abrupt death (process disappears without completing or heartbeat)
    time.sleep(0.2)

    # Reaper runs, detects expired lease, requeues job
    recovered = reap_expired_jobs(conn, base_delay=0.0)  # zero delay for immediate test claim
    assert len(recovered) == 1
    # Set run_at to now so worker 2 can claim immediately
    conn.execute("UPDATE jobs SET run_at = ? WHERE id = ?", (time.time() - 1, job_id))

    # Worker 2 claims the recovered job and completes it
    worker_2 = Worker(worker_id="worker-2", db_path=db_path)
    finished = worker_2.run_once()

    assert finished is not None
    assert finished["id"] == job_id
    assert finished["status"] == "COMPLETED"
    assert finished["result"] == {"status": "ok", "echo": "resilient"}


# --- I. Supervisor Subprocess Management ---
def test_supervisor_detects_and_restarts_crashed_worker(temp_db):
    _, db_path = temp_db
    supervisor = WorkerSupervisor(num_workers=1, db_path=db_path, base_worker_name="w-sup")
    supervisor.start()

    try:
        # Give worker a moment to start and register
        time.sleep(0.5)

        worker_id = "w-sup-1"
        proc = supervisor.procs.get(worker_id)
        assert proc is not None
        old_pid = proc.pid

        # Kill the worker subprocess abruptly
        proc.kill()
        proc.wait(timeout=2.0)

        # Supervisor checks and restarts
        restarted = supervisor.check_and_restart()
        assert worker_id in restarted

        # Verify a new process was launched with a new PID
        new_proc = supervisor.procs.get(worker_id)
        assert new_proc is not None
        assert new_proc.pid != old_pid
    finally:
        supervisor.stop()


# --- J. Audit Events Comprehensive Verification ---
def test_audit_events_created_for_phase3_transitions(temp_db):
    conn, _ = temp_db
    # Verify WORKER_STARTED event
    register_worker(conn, "worker-audit", os.getpid())

    # Create & claim job
    job, _ = create_job(conn, "fail", {"message": "audit check"}, max_retries=1)
    claimed = claim_next_job(conn, "worker-audit", lease_duration=0.05)

    # Expire lease & reap
    time.sleep(0.1)
    reap_expired_jobs(conn)

    # Check audit events recorded
    events = list_audit_events(conn)
    event_types = {e["event_type"] for e in events}

    expected = {
        "WORKER_STARTED",
        "JOB_ENQUEUED",
        "LEASE_ACQUIRED",
        "LEASE_EXPIRED",
        "DEAD_LETTERED",
    }
    assert expected.issubset(event_types)
