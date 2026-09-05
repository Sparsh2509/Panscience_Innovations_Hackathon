"""
Release management and one-action rollback service for NEXUS.
Fulfills core requirement R-06: Any release pushed out can be taken back in one action.
Uses the existing SQLite releases table and immutable audit events as the single source of truth.
"""

import json
import sqlite3
import time
from typing import Any, Optional

from nexus.core.db import transaction
from nexus.services.audit_service import record_audit_event


class ReleaseError(Exception):
    """Base exception for release management errors."""
    pass


class ReleaseValidationError(ReleaseError):
    """Raised when release input parameters are missing or invalid."""
    pass


class ReleaseNotFoundError(ReleaseError):
    """Raised when a requested release version does not exist."""
    pass


class ReleaseAlreadyExistsError(ReleaseError):
    """Raised when attempting to create a duplicate release version."""
    pass


class NoRollbackTargetError(ReleaseError):
    """Raised when rollback is impossible because there is no previous release in history."""
    pass


def _row_to_release_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Helper to convert a releases table row into a structured dictionary."""
    data = dict(row)
    if "config" in data and isinstance(data["config"], str):
        try:
            data["config"] = json.loads(data["config"])
        except (json.JSONDecodeError, TypeError):
            pass
    return data


def create_release(
    conn: sqlite3.Connection,
    version: str,
    description: str,
    config: Optional[dict[str, Any] | str] = None,
    deployed_by: str = "system",
) -> dict[str, Any]:
    """
    Creates a new release entry in the releases table.
    New releases are created in inactive status (is_active = 0).
    """
    if not version or not isinstance(version, str) or not version.strip():
        raise ReleaseValidationError("Release version must be a non-empty string.")
    version = version.strip()

    if not description or not isinstance(description, str) or not description.strip():
        raise ReleaseValidationError("Release description must be a non-empty string.")
    description = description.strip()

    config_str = json.dumps(config or {}) if isinstance(config, dict) else (config or "{}")
    now = time.time()

    with transaction(conn, mode="IMMEDIATE"):
        cursor = conn.execute("SELECT version FROM releases WHERE version = ?", (version,))
        if cursor.fetchone():
            raise ReleaseAlreadyExistsError(f"Release version '{version}' already exists.")

        conn.execute(
            """
            INSERT INTO releases (version, is_active, description, config, deployed_at, deployed_by)
            VALUES (?, 0, ?, ?, ?, ?)
            """,
            (version, description, config_str, now, deployed_by),
        )

        record_audit_event(
            conn,
            event_type="RELEASE_CREATED",
            actor=deployed_by,
            severity="INFO",
            details={
                "version": version,
                "description": description,
            },
        )

        row = conn.execute("SELECT * FROM releases WHERE version = ?", (version,)).fetchone()
        return _row_to_release_dict(row)


def get_active_release(conn: sqlite3.Connection) -> Optional[dict[str, Any]]:
    """
    Retrieves the currently active release. Returns None if no release is active.
    """
    row = conn.execute("SELECT * FROM releases WHERE is_active = 1 LIMIT 1").fetchone()
    return _row_to_release_dict(row) if row else None


def get_release(conn: sqlite3.Connection, version: str) -> Optional[dict[str, Any]]:
    """
    Retrieves a release by version string. Returns None if not found.
    """
    row = conn.execute("SELECT * FROM releases WHERE version = ?", (version,)).fetchone()
    return _row_to_release_dict(row) if row else None


def list_releases(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """
    Lists all releases ordered by deployment time descending.
    """
    rows = conn.execute("SELECT * FROM releases ORDER BY deployed_at DESC, version DESC").fetchall()
    return [_row_to_release_dict(r) for r in rows]


def deploy_release(
    conn: sqlite3.Connection,
    version: str,
    deployed_by: str,
    reason: Optional[str] = None,
) -> dict[str, Any]:
    """
    Atomically deploys a target release version:
    1. Verifies the target release exists.
    2. Identifies the currently active release.
    3. Atomically sets all releases to inactive, and activates the target release.
    4. Records an immutable RELEASE_DEPLOYED audit event with previous & new versions.
    """
    if not version or not isinstance(version, str) or not version.strip():
        raise ReleaseValidationError("Release version must be a non-empty string.")
    version = version.strip()

    now = time.time()
    with transaction(conn, mode="IMMEDIATE"):
        # 1. Verify target release exists
        target = conn.execute("SELECT * FROM releases WHERE version = ?", (version,)).fetchone()
        if not target:
            raise ReleaseNotFoundError(f"Cannot deploy: Release version '{version}' does not exist.")

        # 2. Identify currently active release
        current = conn.execute("SELECT * FROM releases WHERE is_active = 1 LIMIT 1").fetchone()
        from_version = current["version"] if current else None

        # Deterministic no-op if target is already active
        if from_version == version:
            return _row_to_release_dict(target)

        # 3. Atomically update active release flag
        conn.execute("UPDATE releases SET is_active = 0")
        conn.execute(
            """
            UPDATE releases
            SET is_active = 1, deployed_at = ?, deployed_by = ?
            WHERE version = ?
            """,
            (now, deployed_by, version),
        )

        # 4. Record audit event
        record_audit_event(
            conn,
            event_type="RELEASE_DEPLOYED",
            actor=deployed_by,
            severity="INFO",
            details={
                "from_version": from_version,
                "to_version": version,
                "reason": reason or f"Deployment of release {version}",
                "actor": deployed_by,
            },
        )

        new_active = conn.execute("SELECT * FROM releases WHERE version = ?", (version,)).fetchone()
        return _row_to_release_dict(new_active)


def rollback_release(
    conn: sqlite3.Connection,
    actor: str,
    reason: Optional[str] = None,
) -> dict[str, Any]:
    """
    Executes a one-action rollback (R-06):
    1. Determines the currently active release.
    2. Identifies the previous known release from the deployment history in audit events.
    3. Atomically switches active release back to the previous release.
    4. Records an immutable RELEASE_ROLLED_BACK audit event.
    5. Returns a structured summary:
       {"rolled_back": True, "from_version": ..., "to_version": ..., "active_release": ...}

    Raises NoRollbackTargetError if there is no previous release in history.
    """
    now = time.time()
    with transaction(conn, mode="IMMEDIATE"):
        # 1. Find currently active release
        current = conn.execute("SELECT * FROM releases WHERE is_active = 1 LIMIT 1").fetchone()
        if not current:
            raise NoRollbackTargetError("Cannot rollback: No active release found in system.")
        from_version = current["version"]

        # 2. Scan deployment history in audit_events for the event that activated from_version
        cursor = conn.execute(
            """
            SELECT details
            FROM audit_events
            WHERE event_type = 'RELEASE_DEPLOYED'
            ORDER BY id DESC
            """
        )
        rows = cursor.fetchall()

        previous_version = None
        for row in rows:
            try:
                details = json.loads(row["details"])
                if details.get("to_version") == from_version and details.get("from_version"):
                    candidate = details["from_version"]
                    # Ensure candidate exists in releases table and is different
                    cand_row = conn.execute("SELECT version FROM releases WHERE version = ?", (candidate,)).fetchone()
                    if cand_row and candidate != from_version:
                        previous_version = candidate
                        break
            except (json.JSONDecodeError, TypeError):
                continue

        if not previous_version:
            raise NoRollbackTargetError(
                f"Cannot rollback: No previous release found in deployment history prior to '{from_version}'."
            )

        # 3. Atomically switch active release
        conn.execute("UPDATE releases SET is_active = 0")
        conn.execute(
            """
            UPDATE releases
            SET is_active = 1, deployed_at = ?, deployed_by = ?
            WHERE version = ?
            """,
            (now, actor, previous_version),
        )

        # 4. Record audit event
        record_audit_event(
            conn,
            event_type="RELEASE_ROLLED_BACK",
            actor=actor,
            severity="WARN",
            details={
                "from_version": from_version,
                "to_version": previous_version,
                "reason": reason or f"One-action rollback from {from_version} to {previous_version}",
                "actor": actor,
            },
        )

        new_active = conn.execute("SELECT * FROM releases WHERE version = ?", (previous_version,)).fetchone()
        return {
            "rolled_back": True,
            "from_version": from_version,
            "to_version": previous_version,
            "active_release": _row_to_release_dict(new_active),
        }


def get_release_audit_history(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """
    Retrieves chronological release lifecycle events (creation, deployment, rollback).
    """
    rows = conn.execute(
        """
        SELECT id, event_type, severity, actor, details, created_at
        FROM audit_events
        WHERE event_type IN ('RELEASE_CREATED', 'RELEASE_DEPLOYED', 'RELEASE_ROLLED_BACK')
        ORDER BY id ASC
        """
    ).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        if "details" in d and isinstance(d["details"], str):
            try:
                d["details"] = json.loads(d["details"])
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(d)
    return result
