"""
Launcher for NEXUS Worker Supervisor.
Runs a pool of local worker subprocesses that claim, execute, and heartbeat jobs,
along with automatic crash detection and orphaned job lease reaping.

Usage:
    python scripts/run_workers.py
"""

from pathlib import Path
import sys
import time

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from nexus.core.db import get_db
from nexus.services.reaper import reap_expired_jobs
from nexus.workers.supervisor import WorkerSupervisor


def main():
    print("=" * 65)
    print("Starting NEXUS Worker Supervisor & Fleet (2 workers)...")
    print("=" * 65)

    supervisor = WorkerSupervisor(num_workers=2)
    supervisor.start()
    print("[FLEET] Workers spawned: worker-1, worker-2.")
    print("[FLEET] Auto-heartbeat and supervisor active. Press CTRL+C to stop.")

    conn = get_db()
    try:
        while True:
            # 1. Check for crashed worker processes and auto-restart
            crashed = supervisor.check_and_restart()
            if crashed:
                print(f"[SUPERVISOR] Detected crashed worker(s) and restarted: {', '.join(crashed)}")

            # 2. Automatically reap any orphaned jobs with expired leases
            try:
                reaped = reap_expired_jobs(conn)
                if reaped:
                    print(f"[REAPER] Recovered {len(reaped)} orphaned job(s) from expired leases.")
            except Exception:
                pass

            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopping worker fleet cleanly...")
        supervisor.stop()
        conn.close()
        print("All workers stopped.")


if __name__ == "__main__":
    main()
