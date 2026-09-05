"""
Job management routes for NEXUS.
Exposes durable job creation, idempotent ingestion, inspection, and job audit histories.
"""

import sqlite3
from typing import Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from nexus.api.dependencies import get_db_conn
from nexus.core.config import DEFAULT_JOB_PRIORITY, DEFAULT_MAX_RETRIES
from nexus.services.audit_service import get_audit_events_for_job
from nexus.services.job_service import (
    JobNotFoundError,
    JobValidationError,
    create_job,
    get_job,
    list_jobs,
    retry_job,
)

router = APIRouter()


class JobCreateRequest(BaseModel):
    job_type: str = Field(..., min_length=1, description="Type or name of work handler to execute")
    payload: dict[str, Any] = Field(default_factory=dict, description="Arbitrary task payload")
    idempotency_key: Optional[str] = Field(None, description="Unique client key to prevent duplicate processing")
    priority: int = Field(default=DEFAULT_JOB_PRIORITY, description="Numeric priority (higher executes first)")
    max_retries: int = Field(default=DEFAULT_MAX_RETRIES, ge=0, description="Bounded retry attempts")
    run_at: Optional[float] = Field(None, description="Scheduled execution timestamp (epoch seconds)")


class JobResponse(BaseModel):
    job: dict[str, Any]
    deduplicated: bool


@router.post("", response_model=JobResponse, status_code=status.HTTP_201_CREATED, summary="Submit background job")
def submit_job(
    request: JobCreateRequest,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """
    Durably submits a background job into NEXUS.
    If the same idempotency key is submitted multiple times, returns the original job record
    with deduplicated=True without enqueuing duplicates.
    """
    try:
        job, is_dedup = create_job(
            conn=conn,
            job_type=request.job_type,
            payload=request.payload,
            idempotency_key=request.idempotency_key,
            priority=request.priority,
            max_retries=request.max_retries,
            run_at=request.run_at,
        )
    except JobValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    return {"job": job, "deduplicated": is_dedup}


@router.get("", summary="List background jobs")
def get_jobs(
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status (QUEUED, RUNNING, COMPLETED, FAILED, DEAD_LETTER)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """Lists jobs with optional status filter and pagination."""
    return list_jobs(conn, status=status_filter, limit=limit, offset=offset)


@router.get("/{job_id}", summary="Inspect a specific job")
def get_single_job(
    job_id: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """Retrieves detailed job status, attempt count, lease info, and result."""
    job = get_job(conn, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found.")
    return job


@router.get("/{job_id}/audit", summary="Get audit timeline for a job")
def get_job_audit(
    job_id: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """Retrieves immutable audit history explaining all decisions and state transitions for a job."""
    job = get_job(conn, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found.")
    return get_audit_events_for_job(conn, job_id)


@router.post("/{job_id}/retry", summary="Manually retry a failed or dead-letter job")
def retry_failed_job(
    job_id: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """
    Manually re-queues a failed or dead-letter job for execution.
    Resets status to QUEUED and extends max retries so workers claim it.
    """
    try:
        return retry_job(conn, job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

