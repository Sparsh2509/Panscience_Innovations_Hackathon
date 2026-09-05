"""
Audit service for recording append-only, immutable events in NEXUS.
"""

import json
import sqlite3
import time
from typing import Any, Optional


def record_audit_event(
    conn: sqlite3.Connection,
    event_type: str,
    actor: str,
    job_id: Optional[str] = None,
    severity: str = "INFO",
    details: Optional[dict[str, Any]] = None,
) -> int:
    """
    Appends an immutable audit event explaining what happened, why, and which actor performed it.
    Returns the auto-generated event ID.
    """
    now = time.time()
    details_str = json.dumps(details or {})

    cursor = conn.execute(
        """
        INSERT INTO audit_events (job_id, event_type, severity, actor, details, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (job_id, event_type, severity, actor, details_str, now),
    )
    return cursor.lastrowid  # type: ignore


def get_audit_events_for_job(
    conn: sqlite3.Connection,
    job_id: str,
) -> list[dict[str, Any]]:
    """
    Retrieves all audit events associated with a specific job in chronological order.
    """
    cursor = conn.execute(
        """
        SELECT id, job_id, event_type, severity, actor, details, created_at
        FROM audit_events
        WHERE job_id = ?
        ORDER BY created_at ASC, id ASC
        """,
        (job_id,),
    )
    rows = cursor.fetchall()
    events = []
    for row in rows:
        events.append({
            "id": row["id"],
            "job_id": row["job_id"],
            "event_type": row["event_type"],
            "severity": row["severity"],
            "actor": row["actor"],
            "details": json.loads(row["details"]) if row["details"] else {},
            "created_at": row["created_at"],
        })
    return events


def list_audit_events(
    conn: sqlite3.Connection,
    event_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """
    Lists audit events with optional event_type filtering.
    """
    if event_type:
        cursor = conn.execute(
            """
            SELECT id, job_id, event_type, severity, actor, details, created_at
            FROM audit_events
            WHERE event_type = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (event_type, limit, offset),
        )
    else:
        cursor = conn.execute(
            """
            SELECT id, job_id, event_type, severity, actor, details, created_at
            FROM audit_events
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )
    rows = cursor.fetchall()
    events = []
    for row in rows:
        events.append({
            "id": row["id"],
            "job_id": row["job_id"],
            "event_type": row["event_type"],
            "severity": row["severity"],
            "actor": row["actor"],
            "details": json.loads(row["details"]) if row["details"] else {},
            "created_at": row["created_at"],
        })
    return events
