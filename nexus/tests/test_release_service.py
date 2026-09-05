"""
Tests for Phase 4: Release Management and One-Action Rollback (R-06).
"""

import gc
import tempfile
from pathlib import Path
import pytest

from nexus.core.db import get_db, init_db, transaction
from nexus.services.job_service import create_job, get_job
from nexus.services.release_service import (
    NoRollbackTargetError,
    ReleaseAlreadyExistsError,
    ReleaseNotFoundError,
    ReleaseValidationError,
    create_release,
    deploy_release,
    get_active_release,
    get_release,
    get_release_audit_history,
    list_releases,
    rollback_release,
)


@pytest.fixture
def temp_db():
    """Provides an isolated initialized temporary SQLite database."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "test_phase4.db"
        init_db(db_path)
        conn = get_db(db_path)
        yield conn, db_path
        conn.close()
        gc.collect()


# --- A. Release Creation Tests ---
def test_create_release(temp_db):
    conn, _ = temp_db
    rel = create_release(
        conn,
        version="v1.1.0",
        description="Release with new billing engine",
        config={"feature_flag_billing_v2": True},
        deployed_by="alice",
    )

    assert rel["version"] == "v1.1.0"
    assert rel["is_active"] == 0
    assert rel["description"] == "Release with new billing engine"
    assert rel["config"] == {"feature_flag_billing_v2": True}

    # Attempt duplicate creation
    with pytest.raises(ReleaseAlreadyExistsError):
        create_release(conn, version="v1.1.0", description="Duplicate attempt")

    # Invalid empty version
    with pytest.raises(ReleaseValidationError):
        create_release(conn, version="", description="Empty version")


# --- B. Active Release Verification ---
def test_initial_active_release(temp_db):
    conn, _ = temp_db
    active = get_active_release(conn)
    assert active is not None
    assert active["version"] == "v1.0.0"
    assert active["is_active"] == 1


# --- C. Deployment Tests ---
def test_deploy_release(temp_db):
    conn, _ = temp_db
    create_release(conn, "v1.1.0", "New release")

    # Deploy v1.1.0
    deployed = deploy_release(conn, version="v1.1.0", deployed_by="deployer-1")
    assert deployed["version"] == "v1.1.0"
    assert deployed["is_active"] == 1

    # Verify v1.0.0 is now inactive
    old_rel = get_release(conn, "v1.0.0")
    assert old_rel["is_active"] == 0

    # Invariant: exactly one release active
    active_count = conn.execute("SELECT COUNT(*) AS cnt FROM releases WHERE is_active = 1").fetchone()["cnt"]
    assert active_count == 1


# --- D. Atomic Deployment Test ---
def test_atomic_deployment_failure_does_not_corrupt_state(temp_db):
    conn, _ = temp_db
    initial_active = get_active_release(conn)

    # Attempting to deploy non-existent release fails
    with pytest.raises(ReleaseNotFoundError):
        deploy_release(conn, version="v9.9.9", deployed_by="operator")

    # Active release should remain unchanged
    after_attempt = get_active_release(conn)
    assert after_attempt["version"] == initial_active["version"]
    assert after_attempt["is_active"] == 1


# --- E & F. One-Action Rollback (R-06) ---
def test_one_action_rollback(temp_db):
    conn, _ = temp_db
    create_release(conn, "v1.1.0", "Buggy release")
    create_release(conn, "v1.2.0", "Another release")

    # 1. Deploy v1.1.0
    deploy_release(conn, "v1.1.0", deployed_by="operator-1")
    assert get_active_release(conn)["version"] == "v1.1.0"

    # 2. Execute ONE-ACTION rollback
    result = rollback_release(conn, actor="operator-1", reason="High error rate detected")

    # Verify return contract
    assert result["rolled_back"] is True
    assert result["from_version"] == "v1.1.0"
    assert result["to_version"] == "v1.0.0"

    # Verify DB state: v1.0.0 is active again, v1.1.0 is inactive
    active = get_active_release(conn)
    assert active["version"] == "v1.0.0"
    assert active["is_active"] == 1

    rel_v1_1 = get_release(conn, "v1.1.0")
    assert rel_v1_1["is_active"] == 0

    # Invariant: exactly one active
    active_count = conn.execute("SELECT COUNT(*) AS cnt FROM releases WHERE is_active = 1").fetchone()["cnt"]
    assert active_count == 1


# --- G. Invalid Release Deployment ---
def test_deploy_unknown_version_fails(temp_db):
    conn, _ = temp_db
    with pytest.raises(ReleaseNotFoundError):
        deploy_release(conn, version="nonexistent-v2", deployed_by="tester")


# --- H. No Rollback Target Test ---
def test_rollback_with_no_history_fails(temp_db):
    conn, _ = temp_db
    # Fresh database has only initial v1.0.0 seeded with no prior deployments
    with pytest.raises(NoRollbackTargetError):
        rollback_release(conn, actor="operator")


# --- I. Audit Trail Verification ---
def test_audit_events_for_deploy_and_rollback(temp_db):
    conn, _ = temp_db
    create_release(conn, "v1.1.0", "Release A")
    deploy_release(conn, "v1.1.0", deployed_by="alice", reason="Scheduled deploy")
    rollback_release(conn, actor="bob", reason="Failed smoke tests")

    history = get_release_audit_history(conn)
    types = [h["event_type"] for h in history]

    assert "RELEASE_CREATED" in types
    assert "RELEASE_DEPLOYED" in types
    assert "RELEASE_ROLLED_BACK" in types

    # Inspect RELEASE_DEPLOYED payload
    deploy_event = next(h for h in history if h["event_type"] == "RELEASE_DEPLOYED")
    assert deploy_event["actor"] == "alice"
    assert deploy_event["details"]["from_version"] == "v1.0.0"
    assert deploy_event["details"]["to_version"] == "v1.1.0"

    # Inspect RELEASE_ROLLED_BACK payload
    rollback_event = next(h for h in history if h["event_type"] == "RELEASE_ROLLED_BACK")
    assert rollback_event["actor"] == "bob"
    assert rollback_event["details"]["from_version"] == "v1.1.0"
    assert rollback_event["details"]["to_version"] == "v1.0.0"
    assert rollback_event["details"]["reason"] == "Failed smoke tests"


# --- J. Job to Release Association Across Deploy & Rollback ---
def test_jobs_capture_active_release_across_transitions(temp_db):
    conn, _ = temp_db

    # 1. Job created under v1.0.0
    job1, _ = create_job(conn, job_type="echo", payload={"step": 1})
    assert job1["release_version"] == "v1.0.0"

    # 2. Deploy v1.1.0
    create_release(conn, "v1.1.0", "Release 1.1")
    deploy_release(conn, "v1.1.0", deployed_by="ci")

    # 3. Job created under v1.1.0
    job2, _ = create_job(conn, job_type="echo", payload={"step": 2})
    assert job2["release_version"] == "v1.1.0"

    # 4. Rollback to v1.0.0
    rollback_release(conn, actor="operator", reason="Rollback")

    # 5. Job created after rollback
    job3, _ = create_job(conn, job_type="echo", payload={"step": 3})
    assert job3["release_version"] == "v1.0.0"

    # 6. Verify historical jobs retain their original release_version
    assert get_job(conn, job1["id"])["release_version"] == "v1.0.0"
    assert get_job(conn, job2["id"])["release_version"] == "v1.1.0"
    assert get_job(conn, job3["id"])["release_version"] == "v1.0.0"
