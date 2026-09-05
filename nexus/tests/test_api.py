"""
Tests for Phase 5: FastAPI Control Plane & Operator/Chaos APIs.
"""

import gc
import os
import tempfile
from pathlib import Path
from fastapi.testclient import TestClient
import pytest

from nexus.api.app import app
from nexus.api.dependencies import set_db_override
from nexus.core.db import get_db, init_db
from nexus.services.job_service import register_worker


@pytest.fixture
def client():
    """Provides a TestClient with an isolated temporary SQLite database."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "test_api.db"
        init_db(db_path)
        set_db_override(db_path)

        with TestClient(app) as test_client:
            yield test_client, db_path

        set_db_override(None)
        gc.collect()


# 1. Health Endpoint Test
def test_health_endpoint(client):
    test_client, _ = client
    response = test_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == "nexus"
    assert data["database"] == "connected"


# 2 & 3. Job Submission & Idempotency
def test_job_submission_and_idempotency(client):
    test_client, _ = client
    payload = {
        "job_type": "payment_sync",
        "payload": {"amount": 100.0, "currency": "USD"},
        "idempotency_key": "PAY-API-101",
        "priority": 5,
    }

    # First submission
    res1 = test_client.post("/api/jobs", json=payload)
    assert res1.status_code == 201
    data1 = res1.json()
    assert data1["deduplicated"] is False
    assert data1["job"]["job_type"] == "payment_sync"
    assert data1["job"]["idempotency_key"] == "PAY-API-101"
    job_id = data1["job"]["id"]

    # Duplicate submission
    res2 = test_client.post("/api/jobs", json=payload)
    assert res2.status_code == 201  # response_model returns with deduplicated=True
    data2 = res2.json()
    assert data2["deduplicated"] is True
    assert data2["job"]["id"] == job_id


# 4 & 5. List and Inspect Jobs
def test_list_and_get_jobs(client):
    test_client, _ = client
    test_client.post("/api/jobs", json={"job_type": "job_a", "payload": {"a": 1}})
    res_b = test_client.post("/api/jobs", json={"job_type": "job_b", "payload": {"b": 2}})
    job_b_id = res_b.json()["job"]["id"]

    # List jobs
    list_res = test_client.get("/api/jobs")
    assert list_res.status_code == 200
    jobs = list_res.json()
    assert len(jobs) >= 2

    # Get single job
    get_res = test_client.get(f"/api/jobs/{job_b_id}")
    assert get_res.status_code == 200
    assert get_res.json()["id"] == job_b_id
    assert get_res.json()["job_type"] == "job_b"


# 6. Unknown Job Returns 404
def test_unknown_job_404(client):
    test_client, _ = client
    res = test_client.get("/api/jobs/nonexistent-uuid-999")
    assert res.status_code == 404
    assert "not found" in res.json()["detail"].lower()


# 7. Workers Endpoint
def test_workers_endpoints(client):
    test_client, db_path = client
    conn = get_db(db_path)
    try:
        register_worker(conn, "worker-api-1", pid=os.getpid())
    finally:
        conn.close()

    # List workers
    res = test_client.get("/api/workers")
    assert res.status_code == 200
    workers = res.json()
    assert len(workers) == 1
    assert workers[0]["id"] == "worker-api-1"
    assert "heartbeat_age_seconds" in workers[0]
    assert "healthy" in workers[0]

    # Get single worker
    single_res = test_client.get("/api/workers/worker-api-1")
    assert single_res.status_code == 200
    assert single_res.json()["id"] == "worker-api-1"

    # Unknown worker 404
    unknown_res = test_client.get("/api/workers/worker-ghost")
    assert unknown_res.status_code == 404


# 8 & 9. Releases List and Active
def test_releases_endpoints(client):
    test_client, _ = client
    # Default active release
    active_res = test_client.get("/api/releases/active")
    assert active_res.status_code == 200
    assert active_res.json()["version"] == "v1.0.0"

    # List releases
    list_res = test_client.get("/api/releases")
    assert list_res.status_code == 200
    assert len(list_res.json()) >= 1


# 10, 11, 12. Create Release, Deploy, and One-Action Rollback
def test_release_lifecycle_and_rollback(client):
    test_client, _ = client
    # Create v1.1.0
    create_res = test_client.post(
        "/api/releases",
        json={
            "version": "v1.1.0",
            "description": "API test release",
            "config": {"flag": True},
            "deployed_by": "tester",
        },
    )
    assert create_res.status_code == 201
    assert create_res.json()["version"] == "v1.1.0"
    assert create_res.json()["is_active"] == 0

    # Deploy v1.1.0
    deploy_res = test_client.post(
        "/api/releases/v1.1.0/deploy",
        json={"actor": "test-suite", "reason": "Deploying 1.1"},
    )
    assert deploy_res.status_code == 200
    assert deploy_res.json()["version"] == "v1.1.0"
    assert deploy_res.json()["is_active"] == 1

    # Verify active is now v1.1.0
    assert test_client.get("/api/releases/active").json()["version"] == "v1.1.0"

    # Rollback in ONE action
    rollback_res = test_client.post(
        "/api/releases/rollback",
        json={"actor": "sre", "reason": "Testing rollback via HTTP"},
    )
    assert rollback_res.status_code == 200
    rb_data = rollback_res.json()
    assert rb_data["rolled_back"] is True
    assert rb_data["from_version"] == "v1.1.0"
    assert rb_data["to_version"] == "v1.0.0"

    # Verify active is back to v1.0.0
    assert test_client.get("/api/releases/active").json()["version"] == "v1.0.0"


# 13 & 14. Audit History Endpoints
def test_audit_endpoints(client):
    test_client, _ = client
    # Submit job to generate audit events
    job_res = test_client.post("/api/jobs", json={"job_type": "audited", "payload": {}})
    job_id = job_res.json()["job"]["id"]

    # Global audit stream
    audit_res = test_client.get("/api/audit")
    assert audit_res.status_code == 200
    events = audit_res.json()
    assert len(events) >= 1

    # Job-specific audit stream
    job_audit_res = test_client.get(f"/api/audit/jobs/{job_id}")
    assert job_audit_res.status_code == 200
    job_events = job_audit_res.json()
    assert len(job_events) >= 1
    assert job_events[0]["event_type"] == "JOB_ENQUEUED"


# 15. Invalid Release Deployment Returns 404
def test_invalid_release_deploy_404(client):
    test_client, _ = client
    res = test_client.post("/api/releases/v9.9.9-ghost/deploy", json={"actor": "me"})
    assert res.status_code == 404
    assert "does not exist" in res.json()["detail"].lower()


# 16. Rollback with No History Returns 409
def test_rollback_no_history_409(client):
    test_client, _ = client
    # Baseline database has no deployment history
    res = test_client.post("/api/releases/rollback", json={"actor": "me"})
    assert res.status_code == 409
    assert "no previous release" in res.json()["detail"].lower()


# 17. Chaos Endpoints Safety & Validation
def test_chaos_endpoints(client):
    test_client, db_path = client
    # 1. Fail job via chaos API
    job_res = test_client.post("/api/jobs", json={"job_type": "echo", "payload": {}, "max_retries": 2})
    job_id = job_res.json()["job"]["id"]

    fail_res = test_client.post(f"/api/chaos/fail-job/{job_id}")
    assert fail_res.status_code == 200
    fail_data = fail_res.json()
    assert fail_data["result"] == "failure_injected"
    assert fail_data["new_status"] == "QUEUED"
    assert fail_data["attempt_count"] == 1

    # Unknown job to fail returns 404
    assert test_client.post("/api/chaos/fail-job/nonexistent-id").status_code == 404

    # 2. Crash worker via chaos API
    conn = get_db(db_path)
    try:
        register_worker(conn, "worker-chaos-1", pid=os.getpid())
    finally:
        conn.close()

    # Unknown worker crash returns 404
    assert test_client.post("/api/chaos/crash-worker/worker-ghost").status_code == 404

    # 3. Simulate release incident
    sim_res = test_client.post("/api/chaos/simulate-release-incident")
    assert sim_res.status_code == 200
    assert sim_res.json()["active_release"] == "v1.1.0-buggy"

    # Verify one-action rollback recovers cleanly
    rb_res = test_client.post("/api/releases/rollback", json={"actor": "chaos-test"})
    assert rb_res.status_code == 200
    assert rb_res.json()["to_version"] == "v1.0.0"
