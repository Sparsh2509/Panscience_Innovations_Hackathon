"""
Worker fleet inspection routes for NEXUS.
Allows operators to observe active worker processes, health status, and heartbeat timestamps.
"""

import sqlite3
import time
from typing import Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status

from nexus.api.dependencies import get_db_conn
from nexus.services.job_service import get_worker, list_workers

router = APIRouter()


def _enrich_worker(worker: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> dict[str, Any]:
    """Adds calculated fields: heartbeat_age_seconds, healthy status, and recent activity."""
    now = time.time()
    last_hb = worker.get("last_heartbeat_at", now)
    age = max(0.0, round(now - last_hb, 2))
    is_healthy = (worker.get("status") != "DEAD") and (age < 15.0)

    enriched = dict(worker)
    enriched["heartbeat_age_seconds"] = age
    enriched["healthy"] = is_healthy
    enriched["jobs_completed_count"] = 0
    enriched["last_event"] = None

    if conn:
        w_id = worker["id"]
        try:
            total_completed = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM audit_events
                WHERE (actor = ? OR actor = ?) AND event_type = 'JOB_COMPLETED'
                """,
                (w_id, f"worker:{w_id}"),
            ).fetchone()["cnt"]
            enriched["jobs_completed_count"] = total_completed

            last_ev = conn.execute(
                """
                SELECT job_id, event_type, created_at
                FROM audit_events
                WHERE (actor = ? OR actor = ?) AND job_id IS NOT NULL
                ORDER BY id DESC LIMIT 1
                """,
                (w_id, f"worker:{w_id}"),
            ).fetchone()

            if last_ev:
                enriched["last_event"] = {
                    "job_id": last_ev["job_id"],
                    "event_type": last_ev["event_type"],
                    "seconds_ago": max(0, int(round(now - last_ev["created_at"]))),
                }
        except Exception:
            pass

    return enriched


@router.get("", summary="List worker processes")
def get_workers(conn: sqlite3.Connection = Depends(get_db_conn)):
    """
    Returns all registered workers in the fleet with heartbeat age and health status.
    """
    workers = list_workers(conn)
    return [_enrich_worker(w, conn=conn) for w in workers]


@router.get("/{worker_id}", summary="Inspect a specific worker")
def get_single_worker(worker_id: str, conn: sqlite3.Connection = Depends(get_db_conn)):
    """
    Retrieves status, process ID, and heartbeat health for a specific worker.
    """
    worker = get_worker(conn, worker_id)
    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worker '{worker_id}' not found.",
        )
    return _enrich_worker(worker, conn=conn)


@router.post("/{worker_id}/start", summary="Start or restart a worker process")
def start_worker(worker_id: str):
    """
    Spawns a new worker subprocess for the given worker_id.
    Safe to call on dead or stale workers — the worker uses an upsert on registration,
    so it will overwrite the dead record with its new PID.
    """
    import subprocess
    import sys
    import re
    from pathlib import Path

    # Validate worker_id to prevent injection (alphanumeric + hyphens only)
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', worker_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid worker_id. Use alphanumeric characters and hyphens only.",
        )

    project_root = str(Path(__file__).resolve().parent.parent.parent.parent)

    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "nexus.workers.worker", worker_id],
            cwd=project_root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return {
            "result": "worker_started",
            "worker_id": worker_id,
            "pid": proc.pid,
            "message": f"Worker '{worker_id}' process spawned with PID {proc.pid}.",
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to spawn worker process: {exc}",
        )
