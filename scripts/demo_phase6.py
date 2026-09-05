"""
NEXUS Phase 6 — Operator Dashboard & End-to-End Reliability Demonstration

Demonstrates:
STEP 1: Static dashboard serving at / and /dashboard with assets at /static
STEP 2: Initial control room state (API connected, baseline v1.0.0 active, empty jobs)
STEP 3: Job submission (durable ingestion)
STEP 4: Duplicate submission with same idempotency key (deduplicated=True)
STEP 5: Job detail inspection and complete audit timeline
STEP 6: Worker fleet registration and health monitoring
STEP 7: Controlled job failure injection via chaos API (requeue + attempt increment)
STEP 8: Controlled worker failure simulation via chaos API
STEP 9: Faulty release deployment (v1.1.0-buggy)
STEP 10: Prominent ONE-ACTION ROLLBACK (v1.1.0-buggy -> v1.0.0)
STEP 11: Full audit trail validation showing append-only immutable history
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
    print("=" * 75)
    print("NEXUS PHASE 6 — OPERATOR DASHBOARD & FULL RELIABILITY DEMO")
    print("=" * 75)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "demo_phase6.db"
        init_db(db_path)
        set_db_override(db_path)

        with TestClient(app) as client:
            # STEP 1: Verify Dashboard static routes
            print("\n[STEP 1] Testing Operator Dashboard Serving: GET /")
            res_root = client.get("/")
            assert res_root.status_code == 200
            assert "NEXUS" in res_root.text
            assert "CONTROL ROOM" in res_root.text
            print(" -> HTTP 200 OK: Control room HTML served at /")

            res_css = client.get("/static/style.css")
            assert res_css.status_code == 200
            print(" -> HTTP 200 OK: Dark operations CSS served at /static/style.css")

            res_js = client.get("/static/app.js")
            assert res_js.status_code == 200
            print(" -> HTTP 200 OK: Vanilla JS controller served at /static/app.js")

            # STEP 2: Initial System State
            print("\n[STEP 2] Inspecting Initial System Overview")
            active_res = client.get("/api/releases/active")
            assert active_res.status_code == 200
            active_ver = active_res.json()["version"]
            print(f" -> Active Release: {active_ver}")

            jobs_res = client.get("/api/jobs")
            assert jobs_res.status_code == 200
            print(f" -> Current Jobs Count: {len(jobs_res.json())}")

            # STEP 3: Submit Job
            print("\n[STEP 3] Operator submits job via form: POST /api/jobs")
            job_payload = {
                "job_type": "invoice_processor",
                "payload": {"invoice_id": "INV-2026-901", "amount": 450.00},
                "idempotency_key": "IDEM-KEY-DEMO-601",
                "priority": 5,
                "max_retries": 3,
            }
            res_sub1 = client.post("/api/jobs", json=job_payload)
            assert res_sub1.status_code == 201
            data1 = res_sub1.json()
            job1 = data1["job"]
            print(f" -> Job Enqueued: ID={job1['id'][:8]}..., Status={job1['status']}, Deduplicated={data1['deduplicated']}")
            assert data1["deduplicated"] is False

            # STEP 4: Submit Same Job (Idempotency Protection)
            print("\n[STEP 4] Submitting same job again with same idempotency key")
            res_sub2 = client.post("/api/jobs", json=job_payload)
            assert res_sub2.status_code == 201
            data2 = res_sub2.json()
            print(f" -> Safe Deduplication: ID={data2['job']['id'][:8]}..., Deduplicated={data2['deduplicated']}")
            assert data2["deduplicated"] is True
            assert data2["job"]["id"] == job1["id"]

            # STEP 5: Inspect Job Details & Audit History
            print(f"\n[STEP 5] Inspecting Job Detail Modal: GET /api/jobs/{job1['id']}")
            res_detail = client.get(f"/api/jobs/{job1['id']}")
            assert res_detail.status_code == 200
            print(f" -> Job Data: Type={res_detail.json()['job_type']}, Release={res_detail.json()['release_version']}, Retries={res_detail.json()['max_retries']}")

            res_job_audit = client.get(f"/api/jobs/{job1['id']}/audit")
            assert res_job_audit.status_code == 200
            print(f" -> Job Audit Events ({len(res_job_audit.json())} events):")
            for ev in res_job_audit.json():
                print(f"    - [{ev['event_type']}] actor={ev['actor']} details={ev['details']}")

            # STEP 6: Worker Fleet Registration & Health
            print("\n[STEP 6] Registering worker and inspecting fleet: GET /api/workers")
            import subprocess
            import time
            worker_proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
            conn = get_db(db_path)
            register_worker(conn, "nexus-worker-demo-1", pid=worker_proc.pid)
            conn.close()

            res_workers = client.get("/api/workers")
            assert res_workers.status_code == 200
            fleet = res_workers.json()
            print(f" -> Total registered workers: {len(fleet)}")
            for w in fleet:
                print(f"    - Worker: {w['id']} | PID: {w['pid']} | Status: {w['status']} | Healthy: {w['healthy']} | Started: {w['started_at']}")

            # STEP 7: Chaos API: Force Job Failure
            print("\n[STEP 7] Chaos Simulation: Force Controlled Job Failure: POST /api/chaos/fail-job/{id}")
            res_fail = client.post(f"/api/chaos/fail-job/{job1['id']}")
            assert res_fail.status_code == 200
            fail_data = res_fail.json()
            print(f" -> Injected Failure: New Status={fail_data['new_status']}, Attempt={fail_data['attempt_count']}/{fail_data['max_retries']}")
            assert fail_data["new_status"] == "QUEUED"
            assert fail_data["attempt_count"] == 1

            # STEP 8: Chaos API: Crash Worker Process
            print("\n[STEP 8] Chaos Simulation: Crash Worker Process: POST /api/chaos/crash-worker/{worker_id}")
            res_crash = client.post("/api/chaos/crash-worker/nexus-worker-demo-1")
            assert res_crash.status_code == 200
            print(f" -> Worker Crash Dispatched: Result={res_crash.json()['result']}, Worker={res_crash.json()['worker_id']}")
            # Allow OS signal processing
            time.sleep(0.5)
            assert worker_proc.poll() is not None
            print(f" -> Process PID {worker_proc.pid} verified terminated (exit code={worker_proc.returncode})")

            # STEP 9: Simulate Release Incident
            print("\n[STEP 9] Chaos Simulation: Deploy Faulty Release: POST /api/chaos/simulate-release-incident")
            res_inc = client.post("/api/chaos/simulate-release-incident")
            assert res_inc.status_code == 200
            print(f" -> Incident Active: Current Active Release is now '{res_inc.json()['active_release']}'")

            # Verify active release changed
            res_active_now = client.get("/api/releases/active")
            assert res_active_now.json()["version"] == "v1.1.0-buggy"

            # STEP 10: ONE-ACTION ROLLBACK (R-06)
            print("\n[STEP 10] Operator clicks [ ROLLBACK TO PREVIOUS RELEASE ] (R-06)")
            res_rb = client.post("/api/releases/rollback", json={"actor": "operator-dashboard", "reason": "Incident recovery"})
            assert res_rb.status_code == 200
            rb_data = res_rb.json()
            print(f" -> ROLLBACK EXECUTED IN ONE ACTION:")
            print(f"    From: {rb_data['from_version']}")
            print(f"    To:   {rb_data['to_version']}")
            print(f"    Release successfully restored to: {rb_data['to_version']}")
            assert rb_data["to_version"] == "v1.0.0"

            # STEP 11: Audit Trail Verification
            print("\n[STEP 11] Operator views complete Audit Trail: GET /api/audit")
            res_audit = client.get("/api/audit?limit=10")
            assert res_audit.status_code == 200
            audit_events = res_audit.json()
            print(f" -> Audit events count: {len(audit_events)}")
            for ev in audit_events[:5]:
                print(f"    - [{ev['event_type']}] severity={ev['severity']} actor={ev['actor']}")

    print("\n" + "=" * 75)
    print("PHASE 6 END-TO-END DEMONSTRATION COMPLETE: ALL 11 STEPS PASSED!")
    print("=" * 75)


if __name__ == "__main__":
    run_demo()
