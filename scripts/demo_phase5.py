"""
NEXUS Phase 5 — FastAPI Control Plane & Operator API Demonstration

Demonstrates:
STEP 1: GET /health (Platform and database connectivity)
STEP 2: POST /api/jobs (Durable job ingestion)
STEP 3: POST /api/jobs (Same idempotency key -> deduplicated=true)
STEP 4: GET /api/jobs/{job_id} (Inspection of job and active release version)
STEP 5: GET /api/workers (Fleet visibility, status, heartbeat age)
STEP 6: GET /api/releases/active (Active release inspection)
STEP 7: Deploy & POST /api/releases/rollback (API-level one-action rollback)
STEP 8: GET /api/audit (Immutable audit log stream)
"""

import json
import os
from pathlib import Path
import sys
import tempfile
from fastapi.testclient import TestClient

# Ensure project root on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from nexus.api.app import app
from nexus.api.dependencies import set_db_override
from nexus.core.db import get_db, init_db
from nexus.services.job_service import register_worker


def run_demo():
    print("=" * 70)
    print("NEXUS PHASE 5 — FASTAPI CONTROL PLANE & OPERATOR API DEMONSTRATION")
    print("=" * 70)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "demo_phase5.db"
        init_db(db_path)
        set_db_override(db_path)

        # Register a sample worker in SQLite for demonstration
        conn = get_db(db_path)
        register_worker(conn, "worker-node-1", pid=os.getpid())
        conn.close()

        with TestClient(app) as client:
            # STEP 1: GET /health
            print("\n[STEP 1] Testing Health Endpoint: GET /health")
            health_res = client.get("/health")
            print(f" -> HTTP Status: {health_res.status_code}")
            print(f" -> Response:    {json.dumps(health_res.json(), indent=2)}")
            assert health_res.status_code == 200
            assert health_res.json()["status"] == "ok"

            # STEP 2: POST /api/jobs (First submission)
            print("\n[STEP 2] Submitting Job: POST /api/jobs (idempotency_key='DEMO-REQ-4001')")
            job_payload = {
                "job_type": "demo_task",
                "payload": {"customer": "Acme Corp", "action": "generate_report"},
                "idempotency_key": "DEMO-REQ-4001",
                "priority": 10,
                "max_retries": 3,
            }
            post_res1 = client.post("/api/jobs", json=job_payload)
            print(f" -> HTTP Status:   {post_res1.status_code}")
            data1 = post_res1.json()
            job_id = data1["job"]["id"]
            print(f" -> Created Job ID: {job_id}")
            print(f" -> Status:         {data1['job']['status']}")
            print(f" -> Deduplicated:   {data1['deduplicated']}")
            assert data1["deduplicated"] is False

            # STEP 3: POST /api/jobs (Duplicate submission with same idempotency key)
            print("\n[STEP 3] Resubmitting IDENTICAL Job: POST /api/jobs (same key 'DEMO-REQ-4001')")
            post_res2 = client.post("/api/jobs", json=job_payload)
            print(f" -> HTTP Status:   {post_res2.status_code}")
            data2 = post_res2.json()
            print(f" -> Returned Job ID: {data2['job']['id']}")
            print(f" -> Same Job ID:     {data1['job']['id'] == data2['job']['id']}")
            print(f" -> Deduplicated:    {data2['deduplicated']} (Deduplicated successfully!)")
            assert data2["deduplicated"] is True

            # STEP 4: GET /api/jobs/{job_id}
            print(f"\n[STEP 4] Inspecting Job: GET /api/jobs/{job_id}")
            get_res = client.get(f"/api/jobs/{job_id}")
            print(f" -> HTTP Status:     {get_res.status_code}")
            job_data = get_res.json()
            print(f" -> Job Type:        {job_data['job_type']}")
            print(f" -> Status:          {job_data['status']}")
            print(f" -> Release Version: {job_data['release_version']} (Automatically linked)")
            print(f" -> Priority:        {job_data['priority']}")

            # STEP 5: GET /api/workers
            print("\n[STEP 5] Inspecting Worker Fleet: GET /api/workers")
            workers_res = client.get("/api/workers")
            print(f" -> HTTP Status: {workers_res.status_code}")
            workers_data = workers_res.json()
            for w in workers_data:
                print(f"  * Worker: {w['id']} | PID: {w['pid']} | Status: {w['status']}")
                print(f"    Heartbeat Age: {w['heartbeat_age_seconds']}s | Healthy: {w['healthy']}")

            # STEP 6: GET /api/releases/active
            print("\n[STEP 6] Inspecting Active Release: GET /api/releases/active")
            active_res = client.get("/api/releases/active")
            print(f" -> HTTP Status:    {active_res.status_code}")
            print(f" -> Active Version: {active_res.json()['version']}")
            print(f" -> Description:    '{active_res.json()['description']}'")

            # STEP 7: Release Deployment and One-Action Rollback via HTTP
            print("\n[STEP 7] Deploying v1.1.0 then executing ONE-ACTION Rollback via HTTP")
            # 7a. Create v1.1.0
            client.post(
                "/api/releases",
                json={
                    "version": "v1.1.0",
                    "description": "High-throughput billing worker release",
                    "config": {"workers": 4},
                    "deployed_by": "ci-cd",
                },
            )
            # 7b. Deploy v1.1.0
            deploy_res = client.post(
                "/api/releases/v1.1.0/deploy",
                json={"actor": "release-engineer", "reason": "Canary deployment"},
            )
            print(f" -> Deployed Release: {deploy_res.json()['version']} (Now Active)")

            # 7c. Rollback in ONE HTTP call: POST /api/releases/rollback
            print(" -> Executing ONE-ACTION Rollback: POST /api/releases/rollback...")
            rollback_res = client.post(
                "/api/releases/rollback",
                json={"actor": "oncall-sre", "reason": "Canary metrics exceeded error threshold"},
            )
            print(f" -> HTTP Status: {rollback_res.status_code}")
            rb = rollback_res.json()
            print(f" -> Rollback Success: {rb['rolled_back']}")
            print(f" -> From Version:     {rb['from_version']}")
            print(f" -> To Version:       {rb['to_version']}")
            print(f" -> Restored Active:  {rb['active_release']['version']}")

            # STEP 8: GET /api/audit
            print("\n[STEP 8] Querying Immutable Audit Log: GET /api/audit")
            audit_res = client.get("/api/audit?limit=10")
            print(f" -> HTTP Status: {audit_res.status_code}")
            events = audit_res.json()
            print(f" -> Retrieved {len(events)} recent audit events:")
            print("-" * 70)
            for i, ev in enumerate(events[:6], 1):
                print(f"  {i}. [{ev['event_type']}] Actor={ev['actor']} | Severity={ev['severity']}")
                print(f"     Details: {json.dumps(ev['details'])}")
            print("-" * 70)

        set_db_override(None)

    print("\n" + "=" * 70)
    print("DEMONSTRATION 5 PASSED: FastAPI Control Plane & Operator API Verified")
    print("=" * 70)


if __name__ == "__main__":
    run_demo()
