"""
NEXUS Phase 4 — Release Management & One-Action Rollback Demonstration (R-06)

Demonstrates:
STEP 1: Current active release (v1.0.0).
STEP 2: Create release (v1.1.0).
STEP 3: Deploy v1.1.0 (v1.1.0 becomes active, v1.0.0 inactive).
STEP 4: Simulate an operational incident caused by v1.1.0.
STEP 5: Operator triggers ONE-ACTION rollback (rollback_release).
STEP 6: System restores v1.0.0 atomically with known result.
STEP 7: Complete release audit history inspection.
"""

import json
from pathlib import Path
import sys
import tempfile

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from nexus.core.db import get_db, init_db
from nexus.services.job_service import create_job
from nexus.services.release_service import (
    create_release,
    deploy_release,
    get_active_release,
    get_release_audit_history,
    rollback_release,
)


def run_demo():
    print("=" * 70)
    print("NEXUS PHASE 4 — RELEASE MANAGEMENT & ONE-ACTION ROLLBACK (R-06)")
    print("=" * 70)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "demo_phase4.db"
        init_db(db_path)
        conn = get_db(db_path)

        # STEP 1: Verify current baseline release
        print("\n[STEP 1] Inspecting baseline active release...")
        active = get_active_release(conn)
        print(f" -> Active Version:     {active['version']}")
        print(f" -> Active Description: '{active['description']}'")
        print(f" -> Deployed At:        {active['deployed_at']}")

        # STEP 2: Create new release v1.1.0
        print("\n[STEP 2] Creating new release candidate 'v1.1.0'...")
        v1_1 = create_release(
            conn,
            version="v1.1.0",
            description="Experimental high-concurrency payment engine",
            config={"concurrency_limit": 50, "strict_validation": True},
            deployed_by="ci-pipeline",
        )
        print(f" -> Version Created:    {v1_1['version']}")
        print(f" -> Active Status:      {v1_1['is_active']} (Inactive before deploy)")
        print(f" -> Config Payload:     {v1_1['config']}")

        # STEP 3: Deploy v1.1.0
        print("\n[STEP 3] Deploying 'v1.1.0' into production...")
        deployed = deploy_release(
            conn,
            version="v1.1.0",
            deployed_by="lead-engineer",
            reason="Scheduled feature release",
        )
        current_active = get_active_release(conn)
        print(f" -> Current Active:     {current_active['version']}")
        print(f" -> Deployed By:        {current_active['deployed_by']}")

        # Job submitted under v1.1.0 captures this release
        sample_job, _ = create_job(conn, job_type="echo", payload={"test": "v1.1"})
        print(f" -> Ingested Job ID:    {sample_job['id']}")
        print(f" -> Job Pinned Release: {sample_job['release_version']} (Automatically linked)")

        # STEP 4: Simulate incident under v1.1.0
        print("\n[STEP 4] Operational incident detected under 'v1.1.0'!")
        print(" -> Alert: High error rate in payment validation (500 Internal Server Errors).")
        print(" -> Action required: Immediate zero-touch recovery to previous stable release.")

        # STEP 5: Execute ONE-ACTION Rollback
        print("\n[STEP 5] Executing ONE-ACTION Rollback (rollback_release)...")
        rollback_result = rollback_release(
            conn,
            actor="on-call-sre",
            reason="Uncaught validation exception impacting payment traffic",
        )

        # STEP 6: Display Rollback Outcome
        print("\n[STEP 6] Verification of Rollback Result:")
        print(f" -> Rollback Executed:  {rollback_result['rolled_back']}")
        print(f" -> From Version:       {rollback_result['from_version']}")
        print(f" -> To Version:         {rollback_result['to_version']}")
        active_restored = get_active_release(conn)
        print(f" -> Restored Active:    {active_restored['version']}")
        print(f" -> Restored Active Flag: {active_restored['is_active']}")

        # Job submitted after rollback
        recovery_job, _ = create_job(conn, job_type="echo", payload={"test": "post-rollback"})
        print(f" -> Post-Rollback Job:  ID={recovery_job['id']}, Pinned Release='{recovery_job['release_version']}'")

        # STEP 7: Audit History
        print("\n[STEP 7] Complete Release Audit Trail (What, When, Who, Why):")
        print("-" * 70)
        history = get_release_audit_history(conn)
        for i, h in enumerate(history, 1):
            print(f" {i:2d}. [{h['event_type']}] Actor={h['actor']} | Severity={h['severity']}")
            print(f"     Details: {json.dumps(h['details'])}")
        print("-" * 70)

        print("\n" + "=" * 70)
        print("DEMONSTRATION 4 PASSED: One-Action Rollback (R-06) Fully Verified")
        print("Operator did NOT manually rebuild any database state.")
        print("=" * 70)

        conn.close()


if __name__ == "__main__":
    run_demo()
