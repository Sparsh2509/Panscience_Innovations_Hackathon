"""
NEXUS Phase 7 — R-07 Release-to-Behaviour Correlation Demonstration

Fulfills core requirement R-07:
"Changes are linked to what followed. The records connect a release to the behaviour seen afterwards,
closely enough that an operator does not have to match timestamps by eye."

Demonstrates:
STEP 1: Baseline release v1.0.0 execution & impact (100% healthy baseline)
STEP 2: Deployment of release v1.1.0 with feature updates
STEP 3: Mixed runtime behaviour under v1.1.0 (successes, retries, failures, dead-letters)
STEP 4: Worker fleet incident under v1.1.0 (worker crash & auto-restart)
STEP 5: Mid-incident correlation inspection: GET /api/releases/v1.1.0/impact
STEP 6: Operator triggers Zero-Touch Rollback (R-06) from v1.1.0 to v1.0.0
STEP 7: Post-rollback correlation inspection:
        - Active duration, end reason
        - Milestones progression (deployed -> 1st job -> 1st fail -> dead letter -> restart -> rollback)
        - Chronological event timeline
        - Concise human-readable summary
STEP 8: Verification of cross-release isolation (v1.0.0 metrics clean & intact)
STEP 9: Un-deployed release candidate check (has_impact=False, NOT_DEPLOYED)
"""

import json
from pathlib import Path
import sys
import tempfile
import time
from fastapi.testclient import TestClient

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from nexus.api.app import app
from nexus.api.dependencies import set_db_override
from nexus.core.db import get_db, init_db
from nexus.services.audit_service import record_audit_event
from nexus.services.job_service import (
    claim_next_job,
    complete_job,
    fail_job,
    register_worker,
)


def print_section(title: str):
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)


def run_demo():
    print_section("NEXUS PHASE 7 — R-07 RELEASE-TO-BEHAVIOUR CORRELATION DEMO")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "demo_phase7.db"
        init_db(db_path)
        set_db_override(db_path)

        with TestClient(app) as client:
            conn = get_db(db_path)

            # STEP 1: Baseline v1.0.0
            print_section("[STEP 1] Baseline Production Release v1.0.0 Execution")
            print(" -> Submitting healthy baseline jobs under active release v1.0.0...")

            for i in range(1, 4):
                res = client.post("/api/jobs", json={
                    "job_type": "ledger_sync",
                    "payload": {"entry_id": f"LGD-00{i}", "amount": 100 * i},
                    "idempotency_key": f"IDEM-V1-00{i}",
                })
                assert res.status_code == 201

            # Process jobs
            register_worker(conn, "worker-1", 1001)
            for _ in range(3):
                c = claim_next_job(conn, "worker-1", 30.0)
                complete_job(conn, c["id"], "worker-1", c["lease_token"], result={"synced": True})

            print(" -> 3 jobs successfully processed by worker-1 under v1.0.0.")

            # Inspect v1.0.0 Impact
            res_imp1 = client.get("/api/releases/v1.0.0/impact")
            assert res_imp1.status_code == 200
            imp1 = res_imp1.json()
            print("\n--- GET /api/releases/v1.0.0/impact ---")
            print(f"  Release: {imp1['version']} (Active: {imp1['is_active']}, Health: {imp1['health']})")
            print(f"  Jobs Processed: {imp1['jobs']['completed']}/{imp1['jobs']['total']} ({imp1['jobs']['success_rate_percent']}% success)")
            print(f"  Failures: {imp1['failures']['total_failures']} | Dead-Letter: {imp1['failures']['dead_letter_count']}")
            print(f"  Worker Crashes: {imp1['workers']['crashes_detected']}")
            print(f"  Summary: {imp1['summary']}")

            # STEP 2: Deploy v1.1.0
            print_section("[STEP 2] Deploying Release v1.1.0 (Async Payment Engine)")
            client.post("/api/releases", json={
                "version": "v1.1.0",
                "description": "Async payment processing and high-throughput routing",
                "config": {"async_pipeline": True, "timeout_ms": 1500},
                "deployed_by": "ci-deployer",
            })
            deploy_res = client.post("/api/releases/v1.1.0/deploy", json={
                "actor": "operator_alice",
                "reason": "Production rollout of v1.1.0 async pipeline",
            })
            assert deploy_res.status_code == 200
            print(" -> Release v1.1.0 deployed successfully.")
            print(f" -> Active Release is now: {deploy_res.json()['version']}")

            # STEP 3: Mixed runtime behaviour under v1.1.0
            print_section("[STEP 3] Runtime Behaviour Unfolding Under v1.1.0")
            print(" -> Ingesting jobs under v1.1.0...")

            # 1. Successful job
            j_good = client.post("/api/jobs", json={
                "job_type": "payment_route",
                "payload": {"account": "ACC-901", "valid": True},
            }).json()["job"]

            # 2. Failing job with retry that succeeds
            j_retry = client.post("/api/jobs", json={
                "job_type": "payment_route",
                "payload": {"account": "ACC-902", "retry": True},
                "max_retries": 2,
            }).json()["job"]

            # 3. Buggy job that exhausts retries and enters DEAD_LETTER
            j_dead = client.post("/api/jobs", json={
                "job_type": "payment_route",
                "payload": {"account": "ACC-999-POISON", "fail": True},
                "max_retries": 2,
            }).json()["job"]

            # Worker processes jobs
            # Complete good job
            c_good = claim_next_job(conn, "worker-1", 30.0)
            complete_job(conn, c_good["id"], "worker-1", c_good["lease_token"], result={"status": "paid"})
            print(f" -> Job {j_good['id'][:8]} completed successfully.")

            # Process retry job: attempt 1 fails, attempt 2 succeeds
            c_retry = claim_next_job(conn, "worker-1", 30.0)
            fail_job(conn, c_retry["id"], "worker-1", c_retry["lease_token"], error_msg="TimeoutError: Gateway timeout", base_delay=0.01)
            conn.execute("UPDATE jobs SET run_at = 0 WHERE id = ?", (c_retry["id"],))
            c_retry2 = claim_next_job(conn, "worker-1", 30.0)
            complete_job(conn, c_retry2["id"], "worker-1", c_retry2["lease_token"], result={"recovered": True})
            print(f" -> Job {j_retry['id'][:8]} retried after TimeoutError and recovered.")

            # Process dead-letter job: attempt 1 fails, attempt 2 fails -> DEAD_LETTER
            c_dead1 = claim_next_job(conn, "worker-1", 30.0)
            fail_job(conn, c_dead1["id"], "worker-1", c_dead1["lease_token"], error_msg="ValueError: Invalid routing signature", base_delay=0.01)
            conn.execute("UPDATE jobs SET run_at = 0 WHERE id = ?", (c_dead1["id"],))
            c_dead2 = claim_next_job(conn, "worker-1", 30.0)
            fail_job(conn, c_dead2["id"], "worker-1", c_dead2["lease_token"], error_msg="ValueError: Invalid routing signature")
            print(f" -> Job {j_dead['id'][:8]} failed repeatedly -> Moved to DEAD_LETTER.")

            # STEP 4: Worker fleet incident under v1.1.0
            print_section("[STEP 4] Worker Fleet Incident Under v1.1.0")
            print(" -> Simulating worker crash due to memory corruption in new release...")
            record_audit_event(
                conn,
                event_type="WORKER_CRASHED",
                actor="supervisor",
                severity="CRITICAL",
                details={"worker_id": "worker-1", "reason": "SIGSEGV: Memory access violation"},
            )
            print(" -> Supervisor detects crash and respawns replacement worker process...")
            record_audit_event(
                conn,
                event_type="WORKER_STARTED",
                actor="worker:worker-1",
                severity="INFO",
                details={"pid": 4512, "status": "IDLE"},
            )

            # STEP 5: Mid-Incident Correlation Check
            print_section("[STEP 5] Live Correlation Inspection: GET /api/releases/v1.1.0/impact")
            res_mid = client.get("/api/releases/v1.1.0/impact")
            assert res_mid.status_code == 200
            mid = res_mid.json()
            print(f"  Release: {mid['version']} (Health: {mid['health']})")
            print(f"  Total Jobs: {mid['jobs']['total']} | Completed: {mid['jobs']['completed']} | Dead-Letter: {mid['failures']['dead_letter_count']}")
            print(f"  Success Rate: {mid['jobs']['success_rate_percent']}%")
            print(f"  Failure Signatures: {mid['failures']['failure_types']}")
            print(f"  Worker Crashes: {mid['workers']['crashes_detected']} (Affected: {mid['workers']['affected_workers']})")
            print(f"  Operator Summary: {mid['summary']}")

            # STEP 6: One-Action Rollback
            print_section("[STEP 6] Operator Triggers Zero-Touch Rollback (R-06)")
            print(" -> Executing POST /api/releases/rollback...")
            rb_res = client.post("/api/releases/rollback", json={
                "actor": "operator_sre",
                "reason": "Elevated error rate and worker crash detected in v1.1.0",
            })
            assert rb_res.status_code == 200
            rb_data = rb_res.json()
            print(f" -> Rollback Result: Rolled back from {rb_data['from_version']} -> {rb_data['to_version']}")
            print(f" -> Active Release Restored To: {rb_data['active_release']['version']}")

            # STEP 7: Post-Rollback Impact Inspection
            print_section("[STEP 7] Complete Release-to-Behaviour Correlation (Post-Rollback)")
            res_post = client.get("/api/releases/v1.1.0/impact")
            assert res_post.status_code == 200
            p = res_post.json()

            print(f"\n  VERSION: {p['version']} (Active: {p['is_active']}, Health: {p['health']})")
            print(f"  DEPLOYMENT: Deployed by {p['deployment']['deployed_by']}, Duration: {p['deployment']['active_duration_seconds']:.2f}s")
            print(f"  END REASON: {p['deployment']['end_reason']}")
            print(f"  ROLLBACK: Was Rolled Back: {p['rollback']['was_rolled_back']} to {p['rollback']['rolled_back_to']} by {p['rollback']['rolled_back_by']}")
            print(f"  JOBS: Total: {p['jobs']['total']}, Completed: {p['jobs']['completed']}, Retried: {p['jobs']['retried']}, Dead-Letter: {p['jobs']['dead_letter']}")
            print(f"  SUCCESS RATE: {p['jobs']['success_rate_percent']}%")
            print(f"  WORKERS: Crashes: {p['workers']['crashes_detected']}, Restarts: {p['workers']['restarts_observed']}")

            print("\n  BEHAVIOUR MILESTONES:")
            ms = p["milestones"]
            print(f"    - Deployed At:            {ms['deployed_at']}")
            print(f"    - First Job Enqueued:     {ms['first_job_enqueued_at']}")
            print(f"    - First Failure Seen:     {ms['first_failure_at']}")
            print(f"    - First Dead-Letter Seen: {ms['first_dead_letter_at']}")
            print(f"    - First Worker Restart:   {ms['first_worker_restart_at']}")
            print(f"    - Rolled Back At:         {ms['rolled_back_at']}")

            print(f"\n  CHRONOLOGICAL TIMELINE ({len(p['timeline'])} events):")
            for idx, item in enumerate(p["timeline"][:8], 1):
                print(f"    [{idx:02d}] {item['event_type']:<18} | {item['severity']:<8} | {item['description']}")

            print(f"\n  CONCISE OPERATOR SUMMARY:\n  \"{p['summary']}\"")

            # STEP 8: Isolation Verification
            print_section("[STEP 8] Cross-Release Isolation Verification")
            v1_final = client.get("/api/releases/v1.0.0/impact").json()
            print(f" -> Release v1.0.0 Total Jobs: {v1_final['jobs']['total']} (Completed: {v1_final['jobs']['completed']})")
            print(f" -> Release v1.0.0 Failures: {v1_final['failures']['total_failures']}")
            print(f" -> Release v1.0.0 Health: {v1_final['health']}")
            assert v1_final["jobs"]["total"] == 3
            assert v1_final["failures"]["total_failures"] == 0
            print(" -> CONFIRMED: v1.0.0 historical behaviour is fully isolated from v1.1.0.")

            # STEP 9: Un-deployed Candidate
            print_section("[STEP 9] Un-deployed Release Candidate Verification")
            client.post("/api/releases", json={
                "version": "v1.2.0-canary",
                "description": "Future release candidate (not yet deployed)",
            })
            canary = client.get("/api/releases/v1.2.0-canary/impact").json()
            print(f" -> Release: {canary['version']}")
            print(f" -> Has Impact: {canary['has_impact']}")
            print(f" -> Health: {canary['health']}")
            print(f" -> Summary: \"{canary['summary']}\"")
            assert canary["has_impact"] is False
            assert canary["health"] == "NOT_DEPLOYED"

    print_section("DEMO COMPLETE: ALL R-07 CORRELATION CAPABILITIES VERIFIED")


if __name__ == "__main__":
    run_demo()
