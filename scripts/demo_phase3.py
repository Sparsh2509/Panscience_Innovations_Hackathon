"""
NEXUS Phase 3 — Crash Recovery & Orphan Reclamation Demonstration

Demonstrates:
1. Start worker-1 subprocess.
2. Submit a long-running job.
3. Allow worker-1 to claim the job and begin execution.
4. Kill worker-1 process abruptly (hard crash simulation).
5. Verify worker-1 is dead.
6. Wait for time-bounded lease to expire.
7. Run Reaper to detect orphaned job, mark worker DEAD, and reclaim job with backoff.
8. Start worker-2 to claim the recovered job and execute to completion.
9. Verify final COMPLETED status.
10. Inspect immutable audit log explaining the entire lifecycle.
"""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time

# Ensure project root is on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from nexus.core.db import get_db, init_db
from nexus.services.audit_service import get_audit_events_for_job
from nexus.services.job_service import create_job, get_job, get_worker
from nexus.services.reaper import reap_expired_jobs
from nexus.workers.worker import Worker


def run_demo():
    print("=" * 70)
    print("NEXUS Phase 3 — Worker Crash & Orphan Recovery Demonstration")
    print("=" * 70)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "demo_phase3.db"
        init_db(db_path)
        conn = get_db(db_path)

        # 1. Start Worker 1 as an independent subprocess with a short lease (3s)
        print("\n[STEP 1] Spawning Worker 1 subprocess ('worker-crash-target')...")
        cmd = [
            sys.executable,
            "-m",
            "nexus.workers.worker",
            "worker-crash-target",
            "--db-path",
            str(db_path),
            "--lease-duration",
            "3.0",
            "--heartbeat-interval",
            "0.5",
        ]
        worker1_proc = subprocess.Popen(cmd, cwd=str(BASE_DIR))
        print(f" -> Worker 1 process started (PID: {worker1_proc.pid})")

        # Wait for worker 1 registration
        for _ in range(20):
            time.sleep(0.1)
            w1 = get_worker(conn, "worker-crash-target")
            if w1:
                break
        print(f" -> Worker 1 verified in database: Status='{w1['status']}'")

        # 2. Submit a long-running job
        print("\n[STEP 2] Submitting a job requiring processing time...")
        job, _ = create_job(
            conn,
            job_type="sleep",
            payload={"seconds": 2.0},
            idempotency_key="CRASH-RECOVER-DEMO-001",
            priority=5,
            max_retries=3,
        )
        job_id = job["id"]
        print(f" -> Job submitted: ID={job_id}, Status='{job['status']}'")

        # 3. Allow Worker 1 to claim the job
        print("\n[STEP 3] Waiting for Worker 1 to claim job...")
        claimed_job = None
        for _ in range(30):
            time.sleep(0.1)
            j = get_job(conn, job_id)
            if j and j["status"] == "RUNNING":
                claimed_job = j
                break

        assert claimed_job is not None, "Worker 1 failed to claim job in time"
        print(f" -> Job claimed by:  {claimed_job['leased_by']}")
        print(f" -> Status:          {claimed_job['status']}")
        print(f" -> Attempt Count:   {claimed_job['attempt_count']}")
        print(f" -> Lease Token:     {claimed_job['lease_token']}")
        print(f" -> Lease Expires:   {claimed_job['lease_expires_at']}")

        # 4. Abruptly kill Worker 1 process (simulate hardware fault / SIGKILL)
        print("\n[STEP 4] Simulating hard crash on Worker 1 (sending SIGKILL)...")
        worker1_proc.kill()
        worker1_proc.wait(timeout=2.0)
        print(f" -> Worker 1 process terminated with exit code: {worker1_proc.returncode}")

        # 5. Verify worker process stopped
        print("\n[STEP 5] Verifying Worker 1 process is dead...")
        assert worker1_proc.poll() is not None
        print(" -> Confirmed: Worker 1 OS process no longer running.")

        # 6. Wait for the time-bounded lease to expire
        print("\n[STEP 6] Waiting for lease to expire (visibility timeout threshold)...")
        time_to_wait = max(0.1, claimed_job["lease_expires_at"] - time.time() + 0.5)
        print(f" -> Sleeping {time_to_wait:.2f}s for lease to lapse...")
        time.sleep(time_to_wait)
        print(" -> Lease expiration threshold reached.")

        # 7. Reaper scans and recovers the orphaned job
        print("\n[STEP 7] Running Reaper daemon to reclaim orphaned jobs...")
        recovered = reap_expired_jobs(conn, base_delay=0.0)  # zero delay for instant demo pickup
        assert len(recovered) == 1
        reaped_job = recovered[0]
        print(f" -> Reaper action:   Recovered Job ID: {reaped_job['id']}")
        print(f" -> New Status:      {reaped_job['status']}")
        print(f" -> Cleared Lease:   leased_by={reaped_job['leased_by']}, lease_token={reaped_job['lease_token']}")
        print(f" -> Last Error:      '{reaped_job['last_error']}'")

        # Verify worker marked DEAD
        w1_after = get_worker(conn, "worker-crash-target")
        print(f" -> Worker 1 status: '{w1_after['status']}' (Marked DEAD)")

        # 8. Start Worker 2 to claim the recovered job
        print("\n[STEP 8] Starting Worker 2 ('worker-recovery-hero') to process recovered job...")
        worker2 = Worker(worker_id="worker-recovery-hero", db_path=db_path)
        worker2.register()
        finished = worker2.run_once()

        # 9. Verify final completion
        print("\n[STEP 9] Verifying final job outcome...")
        assert finished is not None
        assert finished["id"] == job_id
        assert finished["status"] == "COMPLETED"
        print(f" -> Final Status:    {finished['status']}")
        print(f" -> Result Output:   {finished['result']}")
        print(f" -> Total Attempts:  {finished['attempt_count']}")

        # 10. Print comprehensive audit trail
        print("\n[STEP 10] Complete Immutable Audit Trail for this Job:")
        print("-" * 70)
        events = get_audit_events_for_job(conn, job_id)
        for i, ev in enumerate(events, 1):
            print(f" {i:2d}. [{ev['event_type']}] Actor={ev['actor']} | Severity={ev['severity']}")
            print(f"     Details: {json.dumps(ev['details'])}")
        print("-" * 70)

        print("\n" + "=" * 70)
        print("DEMONSTRATION 3 PASSED: Automatic Recovery from Worker Crash Verified")
        print("=" * 70)

        conn.close()


if __name__ == "__main__":
    run_demo()
