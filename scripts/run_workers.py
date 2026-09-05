"""
Launcher for NEXUS Worker Supervisor.
Runs a pool of local worker subprocesses that claim, execute, and heartbeat jobs.

Usage:
    python scripts/run_workers.py
"""

from pathlib import Path
import sys
import time

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from nexus.workers.supervisor import WorkerSupervisor


def main():
    print("Starting NEXUS Worker Supervisor (2 workers)...")
    supervisor = WorkerSupervisor(num_workers=2)
    supervisor.start()
    print("Workers spawned: worker-1, worker-2. Press CTRL+C to stop.")

    try:
        while True:
            supervisor.reap_and_restart_crashed()
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopping workers cleanly...")
        supervisor.stop()
        print("All workers stopped.")


if __name__ == "__main__":
    main()
