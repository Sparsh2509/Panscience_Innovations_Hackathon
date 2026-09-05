"""
Release management routes for NEXUS.
Provides endpoints for version inspection, deployment, and zero-touch one-action rollback (R-06).
"""

import sqlite3
from typing import Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from nexus.api.dependencies import get_db_conn
from nexus.services.release_impact import get_release_impact
from nexus.services.release_service import (
    NoRollbackTargetError,
    ReleaseAlreadyExistsError,
    ReleaseNotFoundError,
    ReleaseValidationError,
    create_release,
    deploy_release,
    get_active_release,
    get_release,
    list_releases,
    rollback_release,
)

router = APIRouter()


class ReleaseCreateRequest(BaseModel):
    version: str = Field(..., min_length=1, description="Semantic version string, e.g. 'v1.1.0'")
    description: str = Field(..., min_length=1, description="Release summary and changes")
    config: Optional[dict[str, Any]] = Field(default_factory=dict, description="Release-specific configuration")
    deployed_by: str = Field("operator", description="Author or CI identity")


class DeployRequest(BaseModel):
    actor: str = Field("operator", description="Operator or automated system initiating deployment")
    reason: Optional[str] = Field(None, description="Optional deployment rationale")


class RollbackRequest(BaseModel):
    actor: str = Field("operator", description="Operator initiating one-action rollback")
    reason: Optional[str] = Field(None, description="Operational justification for rollback")


@router.get("", summary="List all releases")
def get_all_releases(conn: sqlite3.Connection = Depends(get_db_conn)):
    """Lists all registered releases in descending deployment order."""
    return list_releases(conn)


@router.get("/active", summary="Get current active release")
def get_active(conn: sqlite3.Connection = Depends(get_db_conn)):
    """Retrieves the currently active production release."""
    active = get_active_release(conn)
    if not active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active release found in the system.",
        )
    return active


@router.post("", status_code=status.HTTP_201_CREATED, summary="Create a new release")
def create_new_release(
    req: ReleaseCreateRequest,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """Creates a new inactive release candidate."""
    try:
        return create_release(
            conn=conn,
            version=req.version,
            description=req.description,
            config=req.config,
            deployed_by=req.deployed_by,
        )
    except ReleaseAlreadyExistsError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    except ReleaseValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.post("/{version}/deploy", summary="Deploy a release")
def deploy(
    version: str,
    req: DeployRequest,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """
    Atomically deploys a target release, deactivating the previous release and recording audit events.
    """
    try:
        return deploy_release(
            conn=conn,
            version=version,
            deployed_by=req.actor,
            reason=req.reason,
        )
    except ReleaseNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except ReleaseValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.post("/rollback", summary="Execute one-action rollback (R-06)")
def rollback(
    req: RollbackRequest,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """
    Takes back the current release in ONE action with a known result (R-06).
    Restores the previous release automatically from audit history without manual state reconstruction.
    """
    try:
        return rollback_release(
            conn=conn,
            actor=req.actor,
            reason=req.reason,
        )
    except NoRollbackTargetError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))


@router.get("/{version}", summary="Get release by version")
def get_release_by_version(
    version: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """Retrieves metadata for a specific release version."""
    rel = get_release(conn, version)
    if not rel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Release '{version}' not found.",
        )
    return rel


@router.get("/{version}/impact", summary="Get release-to-behaviour correlation (R-07)")
def get_release_impact_metrics(
    version: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
):
    """
    Connects a release to the behaviour seen afterwards (R-07).
    Returns correlated jobs (completed, failed, dead-lettered, retried), failure diagnostics,
    worker fleet health (crashes, restarts), rollback events, milestone timestamps,
    and a unified chronological event timeline.
    """
    try:
        return get_release_impact(conn, version)
    except ReleaseNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

