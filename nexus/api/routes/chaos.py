"""
Chaos engineering and controlled failure injection routes for NEXUS.
Allows operators and judges to safely trigger deterministic failures (worker crashes, job exceptions, release bugs)
without executing arbitrary shell commands or bypassing reliability accounting.
"""

import os
import signal
import sqlite3
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, status

from nexus.api.dependencies import get_db_conn
from nexus.services.job_service import (
    claim_next_job,
    fail_job,
    get_job,
    get_worker,
)
from nexus.services.release_service import (
    ReleaseAlreadyExistsError,
    create_release,
    deploy_release,
    get_active_release,
)

router = APIRouter()


@router.post("/fail-job/{job_id}", summary="Force controlled job failure and retry")
def inject_job_failure(
    job_id: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """
    Forces a controlled failure on an active or queued job.
    Uses existing bounded retry and exponential backoff accounting without bypassing reliability guarantees.
    """
    job = get_job(conn, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )

    if job["status"] in ("COMPLETED", "DEAD_LETTER"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot fail job '{job_id}' with terminal status '{job['status']}'.",
        )

    # Case 1: Job is actively RUNNING under a worker lease
    if job["status"] == "RUNNING":
        updated = fail_job(
            conn=conn,
            job_id=job_id,
            worker_id=job["leased_by"],
            lease_token=job["lease_token"],
            error_msg="Simulated failure via Chaos API (in-flight crash)",
        )
        return {
            "job_id": job_id,
            "result": "failure_injected",
            "previous_status": "RUNNING",
            "new_status": updated["status"],
            "attempt_count": updated["attempt_count"],
            "max_retries": updated["max_retries"],
            "run_at": updated["run_at"],
        }

    # Case 2: Job is QUEUED -> Claim temporarily and trigger failure
    elif job["status"] == "QUEUED":
        claimed = claim_next_job(conn, worker_id="chaos-agent", lease_duration=5.0)
        if not claimed or claimed["id"] != job_id:
            # Re-read in case of race
            job = get_job(conn, job_id)
            if not job or job["status"] != "RUNNING":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Could not lease job for failure injection.",
                )
            claimed = job

        updated = fail_job(
            conn=conn,
            job_id=job_id,
            worker_id=claimed["leased_by"],
            lease_token=claimed["lease_token"],
            error_msg="Simulated failure via Chaos API (queued job faulted)",
        )
        return {
            "job_id": job_id,
            "result": "failure_injected",
            "previous_status": "QUEUED",
            "new_status": updated["status"],
            "attempt_count": updated["attempt_count"],
            "max_retries": updated["max_retries"],
            "run_at": updated["run_at"],
        }


@router.post("/crash-worker/{worker_id}", summary="Terminate a registered worker process")
def crash_worker(
    worker_id: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """
    Simulates a hard worker crash by terminating the worker's OS process.
    Safety constraint: Only targets PIDs registered in the database workers table.
    Arbitrary PID or command input is strictly rejected.
    """
    worker = get_worker(conn, worker_id)
    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worker '{worker_id}' not found in registry.",
        )

    pid = worker["pid"]
    if not pid or pid <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid worker PID '{pid}'.",
        )

    if worker["status"] == "DEAD":
        return {
            "worker_id": worker_id,
            "pid": pid,
            "result": "worker_already_dead",
            "message": f"Worker '{worker_id}' is already in DEAD state.",
        }

    try:
        os.kill(pid, signal.SIGTERM)
        return {
            "worker_id": worker_id,
            "pid": pid,
            "result": "termination_requested",
            "message": f"Signal sent to worker process {pid}. Supervisor/Reaper will execute recovery.",
        }
    except ProcessLookupError:
        return {
            "worker_id": worker_id,
            "pid": pid,
            "result": "process_already_exited",
            "message": f"Worker process {pid} was not found active in the OS.",
        }
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Permission denied terminating process: {exc}",
        )


@router.post("/simulate-release-incident", summary="Deploy a faulty release to test rollback")
def simulate_release_incident(conn: sqlite3.Connection = Depends(get_db_conn)):
    """
    Creates and deploys 'v1.1.0-buggy' to demonstrate zero-touch one-action rollback.
    """
    version = "v1.1.0-buggy"
    try:
        create_release(
            conn=conn,
            version=version,
            description="Simulated faulty release causing payload validation exceptions",
            config={"fault_mode": True, "error_rate": 1.0},
            deployed_by="chaos-agent",
        )
    except ReleaseAlreadyExistsError:
        pass  # Already registered

    deployed = deploy_release(
        conn=conn,
        version=version,
        deployed_by="chaos-agent",
        reason="Demonstration of faulty release triggering R-06 rollback",
    )

    return {
        "result": "incident_simulated",
        "active_release": deployed["version"],
        "recommended_action": "Execute POST /api/releases/rollback to recover in one action.",
    }
