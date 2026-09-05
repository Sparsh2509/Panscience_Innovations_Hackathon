"""
Worker Supervisor for NEXUS.
Launches, monitors, and automatically restarts local worker subprocesses.
"""

import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Optional

from nexus.core.db import get_db, transaction
from nexus.services.audit_service import record_audit_event


class WorkerSupervisor:
    """
    Supervises a pool of Python worker subprocesses.
    Detects crashed processes, marks their status as DEAD in SQLite, and restarts them.
    """

    def __init__(
        self,
        num_workers: int = 2,
        db_path: Optional[str | Path] = None,
        base_worker_name: str = "worker",
    ):
        self.num_workers = num_workers
        self.db_path = str(Path(db_path).resolve()) if db_path else None
        self.base_worker_name = base_worker_name
        self.procs: dict[str, subprocess.Popen] = {}
        self.running = False

    def _spawn_worker(self, worker_id: str) -> subprocess.Popen:
        """Spawns a single worker subprocess."""
        cmd = [
            sys.executable,
            "-m",
            "nexus.workers.worker",
            worker_id,
        ]
        if self.db_path:
            cmd.extend(["--db-path", self.db_path])

        # Project root directory
        cwd = str(Path(__file__).resolve().parent.parent.parent)

        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.procs[worker_id] = proc
        return proc

    def start(self):
        """Starts all worker subprocesses."""
        self.running = True
        for i in range(1, self.num_workers + 1):
            worker_id = f"{self.base_worker_name}-{i}"
            self._spawn_worker(worker_id)

    def check_and_restart(self) -> list[str]:
        """
        Polls worker subprocesses. If any process died unexpectedly:
        - Marks it DEAD in the workers table.
        - Emits a WORKER_CRASHED audit event.
        - Restarts a replacement worker process.

        Returns a list of crashed worker IDs that were restarted.
        """
        crashed = []
        conn = get_db(self.db_path)
        try:
            for worker_id, proc in list(self.procs.items()):
                returncode = proc.poll()
                if returncode is not None:
                    # Worker process has exited
                    crashed.append(worker_id)
                    with transaction(conn, mode="IMMEDIATE"):
                        conn.execute(
                            "UPDATE workers SET status = 'DEAD' WHERE id = ?",
                            (worker_id,),
                        )
                        record_audit_event(
                            conn,
                            event_type="WORKER_CRASHED",
                            actor="supervisor",
                            severity="WARN",
                            details={
                                "worker_id": worker_id,
                                "pid": proc.pid,
                                "returncode": returncode,
                                "action": "Automatic supervisor restart",
                            },
                        )

                    # Only restart if supervisor is still in running state
                    if self.running:
                        new_proc = self._spawn_worker(worker_id)
                        self.procs[worker_id] = new_proc

        finally:
            conn.close()

        return crashed

    def reap_and_restart_crashed(self) -> list[str]:
        """Alias for check_and_restart."""
        return self.check_and_restart()

    def stop(self):
        """Terminates all supervised worker processes cleanly."""
        self.running = False
        conn = get_db(self.db_path)
        try:
            for worker_id, proc in self.procs.items():
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=2.0)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=1.0)

                with transaction(conn, mode="IMMEDIATE"):
                    conn.execute("UPDATE workers SET status = 'DEAD' WHERE id = ?", (worker_id,))
            self.procs.clear()
        finally:
            conn.close()
