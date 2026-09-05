"""
Production Entrypoint for NEXUS on Render.
Runs both:
1. Worker Supervisor (managing local worker subprocesses in the background)
2. FastAPI Control Plane (Uvicorn Web Server binding to $PORT and 0.0.0.0)
"""

import os
from pathlib import Path
import sys
import threading
import time

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from nexus.core.db import get_db, init_db
from nexus.services.reaper import reap_expired_jobs
from nexus.workers.supervisor import WorkerSupervisor


def run_workers():
    print("[RENDER] Starting Worker Supervisor & Fleet...")
    supervisor = WorkerSupervisor(num_workers=2)
    supervisor.start()
    conn = get_db()
    try:
        while True:
            supervisor.check_and_restart()
            try:
                reap_expired_jobs(conn)
            except Exception:
                pass
            time.sleep(1.5)
    except Exception as e:
        print(f"[RENDER] Worker supervisor loop stopped: {e}")
    finally:
        supervisor.stop()
        conn.close()


def main():
    port = int(os.environ.get("PORT", 8000))
    host = "0.0.0.0"

    print(f"[RENDER] Starting NEXUS Production Platform on {host}:{port}...")

    # Ensure schema and initial release are created before spawning workers
    init_db()

    # Start worker supervisor in background daemon thread
    worker_thread = threading.Thread(target=run_workers, daemon=True)
    worker_thread.start()

    # Run Uvicorn in the main thread
    import uvicorn
    uvicorn.run("nexus.api.app:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
