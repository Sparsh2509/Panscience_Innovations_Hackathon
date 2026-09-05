"""
Worker fleet inspection routes for NEXUS.
Allows operators to observe active worker processes, health status, and heartbeat timestamps.
"""

import sqlite3
import time
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, status

from nexus.api.dependencies import get_db_conn
from nexus.services.job_service import get_worker, list_workers

router = APIRouter()


def _enrich_worker(worker: dict[str, Any]) -> dict[str, Any]:
    """Adds calculated fields: heartbeat_age_seconds and healthy status."""
    now = time.time()
    last_hb = worker.get("last_heartbeat_at", now)
    age = max(0.0, round(now - last_hb, 2))
    is_healthy = (worker.get("status") != "DEAD") and (age < 15.0)

    enriched = dict(worker)
    enriched["heartbeat_age_seconds"] = age
    enriched["healthy"] = is_healthy
    return enriched


@router.get("", summary="List worker processes")
def get_workers(conn: sqlite3.Connection = Depends(get_db_conn)):
    """
    Returns all registered workers in the fleet with heartbeat age and health status.
    """
    workers = list_workers(conn)
    return [_enrich_worker(w) for w in workers]


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
    return _enrich_worker(worker)
