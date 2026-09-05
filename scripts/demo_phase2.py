"""
Interactive / Manual demonstration of Phase 2 core capabilities:
1. Create job
2. Duplicate submission (idempotency verification)
3. Atomic job claim (worker lease)
4. Safe job completion
5. Audit trail inspection
"""

import json
import sys
import tempfile
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nexus.core.db import get_db, init_db
from nexus.services.audit_service import get_audit_events_for_job
from nexus.services.job_service import (
    claim_next_job,
    complete_job,
    create_job,
)


def run_demo():
    print("=" * 60)
    print("NEXUS Phase 2 — Live Demonstration")
    print("=" * 60)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "demo.db"
        init_db(db_path)
        conn = get_db(db_path)

        # 1. Create Job
        print("\n[STEP 1] Creating new job with idempotency key 'PAY-INV-9021'...")
        payload = {"invoice_id": "INV-9021", "amount": 250.75, "currency": "USD"}
        job1, is_dedup1 = create_job(
            conn,
            job_type="process_payment",
            payload=payload,
            idempotency_key="PAY-INV-9021",
            priority=10,
        )
        print(f" -> Job ID:          {job1['id']}")
        print(f" -> Status:          {job1['status']}")
        print(f" -> Release Version: {job1['release_version']}")
        print(f" -> Priority:        {job1['priority']}")
        print(f" -> Is Duplicate:    {is_dedup1}")

        # 2. Duplicate Submission
        print("\n[STEP 2] Submitting duplicate request with same idempotency key 'PAY-INV-9021'...")
        job2, is_dedup2 = create_job(
            conn,
            job_type="process_payment",
            payload={"invoice_id": "DIFFERENT_PAYLOAD", "amount": 999.99},
            idempotency_key="PAY-INV-9021",
        )
        print(f" -> Returned Job ID: {job2['id']}")
        print(f" -> Same Job ID:     {job1['id'] == job2['id']}")
        print(f" -> Is Duplicate:    {is_dedup2}")

        # 3. Claim Job
        print("\n[STEP 3] Worker 'worker-node-1' claiming next eligible job...")
        claimed = claim_next_job(conn, worker_id="worker-node-1", lease_duration=15.0)
        print(f" -> Claimed Job ID:  {claimed['id']}")
        print(f" -> Status:          {claimed['status']}")
        print(f" -> Leased By:       {claimed['leased_by']}")
        print(f" -> Attempt Count:   {claimed['attempt_count']}")
        print(f" -> Lease Token:     {claimed['lease_token']}")
        print(f" -> Lease Expires:   {claimed['lease_expires_at']}")

        # Attempt claim by another worker (should return None)
        print("\n[STEP 3b] Worker 'worker-node-2' attempting to claim the same job...")
        claimed_none = claim_next_job(conn, worker_id="worker-node-2")
        print(f" -> Result for worker-2: {claimed_none} (Correct! No duplicate claims)")

        # 4. Complete Job
        print("\n[STEP 4] Completing job with worker 'worker-node-1' and valid lease token...")
        result = {"transaction_id": "tx_live_884920", "gateway_status": "settled"}
        completed = complete_job(
            conn,
            job_id=claimed["id"],
            worker_id="worker-node-1",
            lease_token=claimed["lease_token"],
            result=result,
        )
        print(f" -> Final Status:    {completed['status']}")
        print(f" -> Result Output:   {completed['result']}")
        print(f" -> Leased By:       {completed['leased_by']} (Lease cleared)")
        print(f" -> Lease Token:     {completed['lease_token']} (Token cleared)")

        # 5. Audit Trail Inspection
        print("\n[STEP 5] Inspecting immutable audit trail for this job:")
        events = get_audit_events_for_job(conn, job1["id"])
        for i, ev in enumerate(events, 1):
            print(f"  {i}. [{ev['event_type']}] Actor={ev['actor']}, Severity={ev['severity']}")
            print(f"     Details: {json.dumps(ev['details'])}")

        print("\n" + "=" * 60)
        print("DEMONSTRATION COMPLETED SUCCESSFULLY")
        print("=" * 60)

        conn.close()


if __name__ == "__main__":
    run_demo()
