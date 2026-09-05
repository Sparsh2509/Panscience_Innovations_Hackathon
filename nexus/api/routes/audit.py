"""
Audit history routes for NEXUS.
Allows operators to query immutable audit events explaining all system decisions, recoveries, and transitions.
"""

import sqlite3
from typing import Optional
from fastapi import APIRouter, Depends, Query

from nexus.api.dependencies import get_db_conn
from nexus.services.audit_service import get_audit_events_for_job, list_audit_events

router = APIRouter()


@router.get("", summary="Query audit event stream")
def get_audit_events(
    event_type: Optional[str] = Query(None, description="Filter by event type (e.g. JOB_ENQUEUED, LEASE_ACQUIRED, WORKER_CRASHED)"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """
    Returns immutable audit event stream ordered chronologically descending.
    """
    return list_audit_events(conn, event_type=event_type, limit=limit, offset=offset)


@router.get("/jobs/{job_id}", summary="Get audit events for a specific job")
def get_job_specific_audit(
    job_id: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """
    Returns complete chronological audit history for a single job.
    """
    return get_audit_events_for_job(conn, job_id)
