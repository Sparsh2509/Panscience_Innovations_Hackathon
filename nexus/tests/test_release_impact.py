"""
Tests for Phase 7: R-07 Release-to-Behaviour Correlation.
Validates that deployed releases correlate directly to jobs created/executed,
failures, dead-letter jobs, worker crashes, worker restarts, rollbacks,
milestones, unified event timelines, and operator health summaries.
"""

import gc
import json
from pathlib import Path
import tempfile
import time
from fastapi.testclient import TestClient
import pytest

from nexus.api.app import app
from nexus.api.dependencies import set_db_override
from nexus.core.db import get_db, init_db
from nexus.services.audit_service import record_audit_event
from nexus.services.job_service import (
    claim_next_job,
    complete_job,
    create_job,
    fail_job,
    register_worker,
)
from nexus.services.release_impact import get_release_impact
from nexus.services.release_service import (
    ReleaseNotFoundError,
    create_release,
    deploy_release,
    rollback_release,
)


@pytest.fixture
def temp_db():
    """Provides an isolated initialized temporary SQLite database."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "test_phase7.db"
        init_db(db_path)
        conn = get_db(db_path)
        yield conn, db_path
        conn.close()
        gc.collect()


@pytest.fixture
def client():
    """Provides a TestClient with an isolated temporary SQLite database."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "test_phase7_api.db"
        init_db(db_path)
        set_db_override(db_path)

        with TestClient(app) as test_client:
            yield test_client, db_path

        set_db_override(None)
        gc.collect()


# 1. Non-existent and Invalid Release Tests
def test_unknown_release_impact_not_found(temp_db):
    conn, _ = temp_db
    with pytest.raises(ReleaseNotFoundError):
        get_release_impact(conn, "v9.9.9")

    with pytest.raises(ReleaseNotFoundError):
        get_release_impact(conn, "   ")


# 2. Un-deployed Release Candidate Impact
def test_undeployed_release_candidate_impact(temp_db):
    conn, _ = temp_db
    create_release(conn, version="v1.2.0-candidate", description="Unreleased candidate")

    impact = get_release_impact(conn, "v1.2.0-candidate")
    assert impact["version"] == "v1.2.0-candidate"
    assert impact["has_impact"] is False
    assert impact["health"] == "NOT_DEPLOYED"
    assert impact["jobs"]["total"] == 0
    assert impact["jobs"]["completed"] == 0
    assert impact["failures"]["total_failures"] == 0
    assert impact["workers"]["crashes_detected"] == 0
    assert impact["rollback"]["was_rolled_back"] is False
    assert "not been deployed" in impact["summary"]


# 3. Initial v1.0.0 Release Impact
def test_initial_v1_release_impact(temp_db):
    conn, _ = temp_db
    impact = get_release_impact(conn, "v1.0.0")

    assert impact["version"] == "v1.0.0"
    assert impact["has_impact"] is True
    assert impact["is_active"] is True
    assert impact["health"] == "HEALTHY"
    assert impact["jobs"]["total"] == 0
    assert impact["jobs"]["success_rate_percent"] == 100.0
    assert impact["rollback"]["was_rolled_back"] is False
    assert impact["milestones"]["deployed_at"] > 0


# 4. Job Correlation and Isolation Across Releases
def test_job_correlation_and_cross_release_isolation(temp_db):
    conn, _ = temp_db

    # Ingest 2 jobs under v1.0.0
    j1, _ = create_job(conn, job_type="invoice", payload={"id": 1})
    j2, _ = create_job(conn, job_type="invoice", payload={"id": 2})

    # Register worker and complete j1, j2
    register_worker(conn, "worker-1", 1001)
    claimed1 = claim_next_job(conn, "worker-1", 30.0)
    assert claimed1["id"] == j1["id"]
    complete_job(conn, claimed1["id"], "worker-1", claimed1["lease_token"], result={"done": True})

    claimed2 = claim_next_job(conn, "worker-1", 30.0)
    assert claimed2["id"] == j2["id"]
    complete_job(conn, claimed2["id"], "worker-1", claimed2["lease_token"], result={"done": True})

    # Deploy v1.1.0
    create_release(conn, "v1.1.0", "Next release")
    deploy_release(conn, "v1.1.0", "alice")

    # Ingest 3 jobs under v1.1.0
    j3, _ = create_job(conn, job_type="email", payload={"id": 3})
    j4, _ = create_job(conn, job_type="email", payload={"id": 4})
    j5, _ = create_job(conn, job_type="email", payload={"id": 5})

    claimed3 = claim_next_job(conn, "worker-1", 30.0)
    assert claimed3["id"] == j3["id"]
    complete_job(conn, claimed3["id"], "worker-1", claimed3["lease_token"], result={"sent": True})

    # Verify v1.0.0 impact ONLY has the 2 jobs from v1.0.0
    impact_v1 = get_release_impact(conn, "v1.0.0")
    assert impact_v1["jobs"]["total"] == 2
    assert impact_v1["jobs"]["completed"] == 2
    assert impact_v1["jobs"]["success_rate_percent"] == 100.0
    assert impact_v1["is_active"] is False
    assert impact_v1["deployment"]["end_reason"] == "SUPERSEDED_BY_v1.1.0"

    # Verify v1.1.0 impact ONLY has the 3 jobs from v1.1.0
    impact_v11 = get_release_impact(conn, "v1.1.0")
    assert impact_v11["jobs"]["total"] == 3
    assert impact_v11["jobs"]["completed"] == 1
    assert impact_v11["jobs"]["queued"] == 2
    assert impact_v11["is_active"] is True
    assert impact_v11["health"] == "HEALTHY"


# 5. Failures, Retries, and Dead-Letters Correlation
def test_failures_retries_and_deadletter_correlation(temp_db):
    conn, _ = temp_db

    create_release(conn, "v1.1.0", "Buggy release")
    deploy_release(conn, "v1.1.0", "bob")

    # Create job with max_retries = 2
    j, _ = create_job(conn, job_type="process_data", payload={"bad": True}, max_retries=2)

    register_worker(conn, "worker-1", 1001)

    # Attempt 1: Fails, scheduled for retry
    c1 = claim_next_job(conn, "worker-1", 30.0)
    fail_job(conn, c1["id"], "worker-1", c1["lease_token"], error_msg="ValueError: invalid field", base_delay=0.01)

    # Force run_at to 0 so worker can claim retry immediately
    conn.execute("UPDATE jobs SET run_at = 0 WHERE id = ?", (j["id"],))

    # Attempt 2: Fails again, moves to DEAD_LETTER
    c2 = claim_next_job(conn, "worker-1", 30.0)
    assert c2 is not None
    fail_job(conn, c2["id"], "worker-1", c2["lease_token"], error_msg="ValueError: invalid field again")

    impact = get_release_impact(conn, "v1.1.0")
    assert impact["jobs"]["total"] == 1
    assert impact["jobs"]["dead_letter"] == 1
    assert impact["jobs"]["retried"] == 1
    assert impact["failures"]["total_failures"] == 1
    assert impact["failures"]["dead_letter_count"] == 1
    assert "ValueError" in impact["failures"]["failure_types"]
    assert len(impact["failures"]["sample_errors"]) == 1
    assert impact["failures"]["sample_errors"][0]["is_dead_letter"] is True
    assert impact["health"] == "CRITICAL"
    assert impact["milestones"]["first_failure_at"] is not None
    assert impact["milestones"]["first_dead_letter_at"] is not None


# 6. Worker Crash and Restart Tracking
def test_worker_crash_and_restart_tracking(temp_db):
    conn, _ = temp_db

    create_release(conn, "v1.1.0", "Worker intensive release")
    deploy_release(conn, "v1.1.0", "charlie")

    now = time.time()
    # Simulate worker crash audit event
    record_audit_event(
        conn,
        event_type="WORKER_CRASHED",
        actor="supervisor",
        severity="WARN",
        details={"worker_id": "worker-42", "reason": "SIGSEGV segmentation violation"},
    )
    # Simulate supervisor auto-restart of worker
    record_audit_event(
        conn,
        event_type="WORKER_STARTED",
        actor="worker:worker-42",
        severity="INFO",
        details={"pid": 9999, "status": "IDLE"},
    )

    impact = get_release_impact(conn, "v1.1.0")
    assert impact["workers"]["crashes_detected"] == 1
    assert impact["workers"]["restarts_observed"] == 1
    assert "worker-42" in impact["workers"]["affected_workers"]
    assert len(impact["workers"]["events"]) == 2
    assert impact["milestones"]["first_worker_restart_at"] is not None


# 7. One-Action Rollback Correlation
def test_rollback_correlation(temp_db):
    conn, _ = temp_db

    create_release(conn, "v1.1.0", "Release with regression")
    deploy_release(conn, "v1.1.0", "dev")

    # Ingest a failing job
    j, _ = create_job(conn, job_type="flaky", payload={}, max_retries=0)
    register_worker(conn, "worker-1", 1001)
    c = claim_next_job(conn, "worker-1", 30.0)
    fail_job(conn, c["id"], "worker-1", c["lease_token"], error_msg="Fatal regression error")

    # Execute one-action rollback
    rollback_release(conn, actor="operator-sre", reason="Critical error rate detected")

    impact_v11 = get_release_impact(conn, "v1.1.0")
    assert impact_v11["is_active"] is False
    assert impact_v11["rollback"]["was_rolled_back"] is True
    assert impact_v11["rollback"]["rolled_back_to"] == "v1.0.0"
    assert impact_v11["rollback"]["rolled_back_by"] == "operator-sre"
    assert "Critical error rate" in impact_v11["rollback"]["reason"]
    assert impact_v11["deployment"]["end_reason"] == "ROLLED_BACK_TO_v1.0.0"
    assert impact_v11["milestones"]["rolled_back_at"] is not None
    assert impact_v11["health"] == "CRITICAL"
    assert "rolled back to v1.0.0" in impact_v11["summary"]


# 8. Timeline Strict Chronological Ordering
def test_timeline_chronological_ordering(temp_db):
    conn, _ = temp_db

    create_release(conn, "v1.1.0", "Ordered timeline release")
    deploy_release(conn, "v1.1.0", "tester")

    create_job(conn, "t1", {})
    create_job(conn, "t2", {})

    impact = get_release_impact(conn, "v1.1.0")
    timeline = impact["timeline"]
    assert len(timeline) >= 3

    # Assert monotonic timestamps
    for i in range(len(timeline) - 1):
        assert timeline[i]["timestamp"] <= timeline[i + 1]["timestamp"]


# 9. API Endpoint Verification
def test_api_release_impact_endpoints(client):
    test_client, _ = client

    # 404 for unknown release
    res_404 = test_client.get("/api/releases/v9.9.9/impact")
    assert res_404.status_code == 404

    # 200 for initial v1.0.0
    res_v1 = test_client.get("/api/releases/v1.0.0/impact")
    assert res_v1.status_code == 200
    data_v1 = res_v1.json()
    assert data_v1["version"] == "v1.0.0"
    assert data_v1["health"] in ("HEALTHY", "DEGRADED", "CRITICAL")
    assert "summary" in data_v1
    assert "timeline" in data_v1
    assert "milestones" in data_v1

    # Deploy v1.1.0 via API and submit job
    test_client.post(
        "/api/releases",
        json={"version": "v1.1.0", "description": "API test release", "deployed_by": "api_test"},
    )
    test_client.post(
        "/api/releases/v1.1.0/deploy",
        json={"actor": "api_test", "reason": "Testing R-07 via API"},
    )
    test_client.post(
        "/api/jobs",
        json={"job_type": "api_job", "payload": {"test": True}},
    )

    res_v11 = test_client.get("/api/releases/v1.1.0/impact")
    assert res_v11.status_code == 200
    data_v11 = res_v11.json()
    assert data_v11["version"] == "v1.1.0"
    assert data_v11["jobs"]["total"] == 1
    assert data_v11["is_active"] is True
