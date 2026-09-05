"""
Worker process runtime for NEXUS.
Registers with the workers table, leases jobs atomically, executes deterministic handlers,
sends heartbeats and extends leases during execution, and handles clean shutdowns.
"""

import argparse
import os
import signal
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any, Optional

# Ensure nexus package is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from nexus.core.config import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_LEASE_DURATION_SECONDS,
    DEFAULT_WORKER_POLL_INTERVAL_SECONDS,
)
from nexus.core.db import get_db
from nexus.services.audit_service import record_audit_event
from nexus.services.job_service import (
    claim_next_job,
    complete_job,
    extend_lease,
    fail_job,
    get_job,
    register_worker,
    update_worker_heartbeat,
)
from nexus.workers.handlers import execute_job_handler


class HeartbeatRunner:
    """
    Background thread that periodically updates worker heartbeat and extends job lease while processing.
    """

    def __init__(
        self,
        db_path: Optional[str | Path],
        worker_id: str,
        job_id: str,
        lease_token: str,
        interval: float,
        lease_duration: float,
    ):
        self.db_path = db_path
        self.worker_id = worker_id
        self.job_id = job_id
        self.lease_token = lease_token
        self.interval = interval
        self.lease_duration = lease_duration
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=2.0)

    def _run(self):
        conn = get_db(self.db_path)
        try:
            while not self.stop_event.wait(timeout=self.interval):
                # Update worker heartbeat
                update_worker_heartbeat(conn, self.worker_id, status="BUSY")
                # Extend active lease
                extended = extend_lease(
                    conn,
                    self.job_id,
                    self.worker_id,
                    self.lease_token,
                    self.lease_duration,
                )
                if not extended:
                    # Lease was revoked or reaped
                    break
        finally:
            conn.close()


class Worker:
    """
    NEXUS Worker instance.
    Runs job claim loop, executes payload handlers, maintains heartbeats and leases.
    """

    def __init__(
        self,
        worker_id: str,
        db_path: Optional[str | Path] = None,
        lease_duration: float = DEFAULT_LEASE_DURATION_SECONDS,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        poll_interval: float = DEFAULT_WORKER_POLL_INTERVAL_SECONDS,
    ):
        self.worker_id = worker_id
        self.db_path = db_path
        self.lease_duration = lease_duration
        self.heartbeat_interval = heartbeat_interval
        self.poll_interval = poll_interval
        self.stop_event = threading.Event()
        self.pid = os.getpid()

    def _get_conn(self) -> sqlite3.Connection:
        return get_db(self.db_path)

    def register(self) -> dict[str, Any]:
        """Registers the worker in the database."""
        conn = self._get_conn()
        try:
            return register_worker(conn, self.worker_id, self.pid)
        finally:
            conn.close()

    def run_once(self) -> Optional[dict[str, Any]]:
        """
        Executes a single iteration of the worker claim loop.
        Returns the finished job dict (completed or failed), or None if no job was claimable.
        """
        conn = self._get_conn()
        try:
            # Update idle heartbeat
            update_worker_heartbeat(conn, self.worker_id, status="IDLE")

            # Claim next eligible job
            job = claim_next_job(conn, self.worker_id, self.lease_duration)
            if not job:
                return None

            job_id = job["id"]
            lease_token = job["lease_token"]

            # Update worker to BUSY
            update_worker_heartbeat(
                conn,
                self.worker_id,
                status="BUSY",
                current_job_id=job_id,
            )

            # Start background heartbeat & lease renewal
            heartbeat = HeartbeatRunner(
                db_path=self.db_path,
                worker_id=self.worker_id,
                job_id=job_id,
                lease_token=lease_token,
                interval=self.heartbeat_interval,
                lease_duration=self.lease_duration,
            )
            heartbeat.start()

            try:
                # Execute deterministic job handler
                result = execute_job_handler(job["job_type"], job.get("payload", {}))
                # Complete job
                completed = complete_job(conn, job_id, self.worker_id, lease_token, result=result)
                return completed
            except Exception as exc:
                # Fail job and apply bounded exponential backoff
                failed = fail_job(conn, job_id, self.worker_id, lease_token, error_msg=str(exc))
                return failed
            finally:
                heartbeat.stop()
                # Return worker to IDLE and clear current_job_id
                update_worker_heartbeat(
                    conn,
                    self.worker_id,
                    status="IDLE",
                    clear_job=True,
                )

        finally:
            conn.close()

    def stop(self):
        """Signals the worker to stop processing."""
        self.stop_event.set()

    def run(self):
        """Main continuous execution loop."""
        self.register()

        # Handle termination signals
        def _handle_signal(sig, frame):
            self.stop()

        try:
            signal.signal(signal.SIGINT, _handle_signal)
            signal.signal(signal.SIGTERM, _handle_signal)
        except (ValueError, AttributeError):
            # Not in main thread or unsupported platform signal
            pass

        try:
            while not self.stop_event.is_set():
                processed = self.run_once()
                if processed is None:
                    # Queue is empty, wait before polling again
                    self.stop_event.wait(timeout=self.poll_interval)
        finally:
            conn = self._get_conn()
            try:
                conn.execute(
                    "UPDATE workers SET status = 'DEAD' WHERE id = ?",
                    (self.worker_id,),
                )
                record_audit_event(
                    conn,
                    event_type="WORKER_STOPPED",
                    actor=f"worker:{self.worker_id}",
                    severity="INFO",
                    details={"pid": self.pid, "reason": "Graceful worker shutdown"},
                )
            finally:
                conn.close()


def main():
    parser = argparse.ArgumentParser(description="NEXUS Worker Process")
    parser.add_argument("worker_id", type=str, help="Unique identifier for this worker")
    parser.add_argument("--db-path", type=str, default=None, help="Path to SQLite database file")
    parser.add_argument("--lease-duration", type=float, default=DEFAULT_LEASE_DURATION_SECONDS)
    parser.add_argument("--heartbeat-interval", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
    args = parser.parse_args()

    worker = Worker(
        worker_id=args.worker_id,
        db_path=args.db_path,
        lease_duration=args.lease_duration,
        heartbeat_interval=args.heartbeat_interval,
    )
    worker.run()


if __name__ == "__main__":
    main()
