"""
Release-to-Behaviour Correlation Service for NEXUS.
Fulfills core requirement R-07: Connects a release to the behaviour seen afterwards,
closely enough that an operator does not have to match timestamps by eye.
"""

from datetime import timedelta
import json
import sqlite3
import time
from typing import Any, Optional

from nexus.services.release_service import ReleaseNotFoundError, get_release


def _format_duration(seconds: float) -> str:
    """Formats seconds into human-readable duration like '4m 12s' or '45s'."""
    if seconds < 0:
        seconds = 0
    td = int(round(seconds))
    mins, secs = divmod(td, 60)
    hours, mins = divmod(mins, 60)
    if hours > 0:
        return f"{hours}h {mins}m {secs}s"
    if mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def _extract_error_type(error_msg: Optional[str]) -> str:
    """Extracts a short error type/category from an error string."""
    if not error_msg:
        return "UnknownError"
    error_msg = error_msg.strip()
    if ":" in error_msg:
        prefix = error_msg.split(":", 1)[0].strip()
        if " " not in prefix and len(prefix) < 40:
            return prefix
    for common in ("ValueError", "KeyError", "TimeoutError", "RuntimeError", "ZeroDivisionError", "ConnectionError", "Exception"):
        if common in error_msg:
            return common
    return error_msg[:30] + "..." if len(error_msg) > 30 else error_msg


def get_release_impact(conn: sqlite3.Connection, version: str) -> dict[str, Any]:
    """
    Computes comprehensive release-to-behaviour correlation for a given release version.
    Returns:
    - version & metadata
    - deployment window (deployed_at, ended_at, active_duration_seconds, end_reason)
    - job metrics (total, completed, failed, dead_letter, running, queued, retried, success_rate)
    - failure diagnostics (total, dead letters, breakdown by error type, sample errors)
    - worker fleet behaviour (crashes, restarts, affected workers, events)
    - rollback details (was_rolled_back, rolled_back_at, rolled_back_to, reason)
    - behaviour milestones (first failure, first dead letter, first worker restart, rollback)
    - unified chronological timeline
    - health classification (HEALTHY, DEGRADED, CRITICAL, NOT_DEPLOYED)
    - concise operator summary
    """
    if not version or not isinstance(version, str) or not version.strip():
        raise ReleaseNotFoundError("Release version must be a non-empty string.")
    version = version.strip()

    # 1. Fetch release record
    release = get_release(conn, version)
    if not release:
        raise ReleaseNotFoundError(f"Release version '{version}' does not exist.")

    is_active = bool(release["is_active"])
    now = time.time()

    # 2. Determine deployment history & active window
    # Search audit events for deployment / rollback activations of this version
    deploy_events = conn.execute(
        """
        SELECT * FROM audit_events
        WHERE event_type IN ('RELEASE_DEPLOYED', 'RELEASE_ROLLED_BACK')
        ORDER BY created_at ASC, id ASC
        """
    ).fetchall()

    deployed_at = release["deployed_at"]
    deployed_by = release["deployed_by"]
    has_activation_event = False
    activation_timestamps = []

    for row in deploy_events:
        try:
            details = json.loads(row["details"])
            to_ver = details.get("to_version")
            if to_ver == version:
                has_activation_event = True
                activation_timestamps.append(row["created_at"])
                deployed_at = row["created_at"]
                deployed_by = row["actor"]
        except (json.JSONDecodeError, TypeError):
            continue

    # If never explicitly deployed via audit event and is_active == 0 and not initial release
    # (Initial release v1.0.0 was seeded during database initialization)
    is_initial_v1 = (version == "v1.0.0")
    has_been_deployed = has_activation_event or is_active or is_initial_v1

    # Determine ended_at and end_reason
    ended_at = None
    end_reason = None
    rollback_info = {
        "was_rolled_back": False,
        "rolled_back_at": None,
        "rolled_back_by": None,
        "rolled_back_to": None,
        "reason": None,
    }

    if has_been_deployed:
        # Check if superseding deployment or rollback happened after deployed_at
        superseding = conn.execute(
            """
            SELECT * FROM audit_events
            WHERE event_type IN ('RELEASE_DEPLOYED', 'RELEASE_ROLLED_BACK')
              AND created_at > ?
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """,
            (deployed_at,),
        ).fetchone()

        if superseding:
            ended_at = superseding["created_at"]
            try:
                sup_details = json.loads(superseding["details"])
                target_ver = sup_details.get("to_version")
                if superseding["event_type"] == "RELEASE_ROLLED_BACK":
                    end_reason = f"ROLLED_BACK_TO_{target_ver}"
                else:
                    end_reason = f"SUPERSEDED_BY_{target_ver}"
            except Exception:
                end_reason = superseding["event_type"]

        # Check if there is an explicit rollback event where from_version == version
        rb_cursor = conn.execute(
            """
            SELECT * FROM audit_events
            WHERE event_type = 'RELEASE_ROLLED_BACK'
            ORDER BY created_at DESC, id DESC
            """
        )
        for rb_row in rb_cursor.fetchall():
            try:
                rb_details = json.loads(rb_row["details"])
                if rb_details.get("from_version") == version:
                    rollback_info = {
                        "was_rolled_back": True,
                        "rolled_back_at": rb_row["created_at"],
                        "rolled_back_by": rb_row["actor"],
                        "rolled_back_to": rb_details.get("to_version"),
                        "reason": rb_details.get("reason"),
                    }
                    if not ended_at:
                        ended_at = rb_row["created_at"]
                        end_reason = f"ROLLED_BACK_TO_{rb_details.get('to_version')}"
                    break
            except Exception:
                continue

    # Window bounds
    window_start = deployed_at if has_been_deployed else 0.0
    window_end = ended_at if ended_at else (now if has_been_deployed else 0.0)
    active_duration_seconds = max(0.0, window_end - window_start) if has_been_deployed else 0.0

    if not has_been_deployed:
        return {
            "version": version,
            "description": release["description"],
            "is_active": False,
            "has_impact": False,
            "deployment": {
                "deployed_at": deployed_at,
                "deployed_by": deployed_by,
                "active_duration_seconds": 0.0,
                "ended_at": None,
                "end_reason": None,
            },
            "jobs": {
                "total": 0,
                "completed": 0,
                "failed": 0,
                "dead_letter": 0,
                "running": 0,
                "queued": 0,
                "retried": 0,
                "success_rate_percent": 100.0,
            },
            "failures": {
                "total_failures": 0,
                "dead_letter_count": 0,
                "failure_types": {},
                "sample_errors": [],
            },
            "workers": {
                "crashes_detected": 0,
                "restarts_observed": 0,
                "affected_workers": [],
                "events": [],
            },
            "rollback": rollback_info,
            "milestones": {
                "deployed_at": deployed_at,
                "first_job_enqueued_at": None,
                "first_failure_at": None,
                "first_dead_letter_at": None,
                "first_worker_restart_at": None,
                "rolled_back_at": None,
            },
            "timeline": [],
            "health": "NOT_DEPLOYED",
            "summary": f"Release {version} has not been deployed yet. No runtime behaviour recorded.",
        }

    # 3. Correlate Jobs tagged with this release
    job_rows = conn.execute(
        """
        SELECT * FROM jobs
        WHERE release_version = ?
        ORDER BY created_at ASC, id ASC
        """,
        (version,),
    ).fetchall()

    jobs_total = len(job_rows)
    completed_count = 0
    failed_count = 0
    dead_letter_count = 0
    running_count = 0
    queued_count = 0
    retried_count = 0

    first_job_enqueued_at: Optional[float] = None
    first_failure_at: Optional[float] = None
    first_dead_letter_at: Optional[float] = None
    failure_types: dict[str, int] = {}
    sample_errors: list[dict[str, Any]] = []

    for j in job_rows:
        status = j["status"]
        attempts = j["attempt_count"]
        created = j["created_at"]
        err = j["last_error"]

        if first_job_enqueued_at is None or created < first_job_enqueued_at:
            first_job_enqueued_at = created

        if status == "COMPLETED":
            completed_count += 1
            if attempts > 1:
                retried_count += 1
        elif status == "FAILED":
            failed_count += 1
            retried_count += 1
        elif status == "DEAD_LETTER":
            dead_letter_count += 1
            retried_count += 1
        elif status == "RUNNING":
            running_count += 1
            if attempts > 1:
                retried_count += 1
        elif status == "QUEUED":
            queued_count += 1
            if attempts > 0:
                retried_count += 1

        if err or status in ("FAILED", "DEAD_LETTER"):
            err_type = _extract_error_type(err)
            failure_types[err_type] = failure_types.get(err_type, 0) + 1
            if len(sample_errors) < 10:
                sample_errors.append({
                    "job_id": j["id"],
                    "job_type": j["job_type"],
                    "error": err or f"Job status {status}",
                    "occurred_at": j["updated_at"],
                    "is_dead_letter": (status == "DEAD_LETTER"),
                })

    # Success rate calculation
    terminal_jobs = completed_count + failed_count + dead_letter_count
    if terminal_jobs > 0:
        success_rate = round((completed_count / terminal_jobs) * 100.0, 1)
    else:
        success_rate = 100.0 if jobs_total == 0 else 0.0

    # 4. Fetch correlated audit events for jobs and system events in window
    job_ids = [j["id"] for j in job_rows]
    audit_events: list[sqlite3.Row] = []

    # Get job audit events
    if job_ids:
        placeholders = ",".join("?" for _ in job_ids)
        job_audit_rows = conn.execute(
            f"""
            SELECT * FROM audit_events
            WHERE job_id IN ({placeholders})
            ORDER BY created_at ASC, id ASC
            """,
            job_ids,
        ).fetchall()
        audit_events.extend(job_audit_rows)

    # Get worker and system events within the active deployment window
    window_events = conn.execute(
        """
        SELECT * FROM audit_events
        WHERE created_at >= ? AND created_at <= ?
          AND event_type IN ('WORKER_CRASHED', 'WORKER_STARTED', 'WORKER_STOPPED', 'RELEASE_DEPLOYED', 'RELEASE_ROLLED_BACK')
        ORDER BY created_at ASC, id ASC
        """,
        (window_start - 0.001, window_end + 0.001),
    ).fetchall()
    audit_events.extend(window_events)

    # De-duplicate by event id
    seen_ids = set()
    unique_audit = []
    for ev in audit_events:
        if ev["id"] not in seen_ids:
            seen_ids.add(ev["id"])
            unique_audit.append(ev)
    unique_audit.sort(key=lambda x: (x["created_at"], x["id"]))

    # 5. Extract Worker behaviour
    worker_crashes = 0
    worker_restarts = 0
    affected_workers: set[str] = set()
    worker_events_list: list[dict[str, Any]] = []
    first_worker_restart_at: Optional[float] = None

    for ev in unique_audit:
        etype = ev["event_type"]
        ev_time = ev["created_at"]
        details_obj = {}
        try:
            details_obj = json.loads(ev["details"]) if ev["details"] else {}
        except Exception:
            pass

        w_id = details_obj.get("worker_id")
        if not w_id and ev["actor"].startswith("worker:"):
            w_id = ev["actor"].split(":", 1)[1]

        if etype == "WORKER_CRASHED":
            worker_crashes += 1
            if w_id:
                affected_workers.add(w_id)
            worker_events_list.append({
                "event_type": etype,
                "worker_id": w_id,
                "timestamp": ev_time,
                "details": details_obj,
            })
        elif etype == "WORKER_STARTED":
            # Count worker restarts that occurred after deployment window start
            if ev_time >= window_start:
                worker_restarts += 1
                if w_id:
                    affected_workers.add(w_id)
                if first_worker_restart_at is None:
                    first_worker_restart_at = ev_time
                worker_events_list.append({
                    "event_type": etype,
                    "worker_id": w_id,
                    "timestamp": ev_time,
                    "details": details_obj,
                })

        # Milestone tracking from audit events
        if etype in ("JOB_FAILED", "RETRY_SCHEDULED") and first_failure_at is None:
            first_failure_at = ev_time
        if etype == "DEAD_LETTERED" and first_dead_letter_at is None:
            first_dead_letter_at = ev_time

    # 6. Build unified chronological timeline
    timeline: list[dict[str, Any]] = []

    # Add initial deployment event if not in audit events
    timeline.append({
        "timestamp": deployed_at,
        "event_type": "RELEASE_DEPLOYED",
        "severity": "INFO",
        "description": f"Release {version} deployed by {deployed_by}: {release['description']}",
        "job_id": None,
        "actor": deployed_by,
    })

    for ev in unique_audit:
        etype = ev["event_type"]
        details_obj = {}
        try:
            details_obj = json.loads(ev["details"]) if ev["details"] else {}
        except Exception:
            pass

        # Build readable description
        desc = etype
        if etype == "RELEASE_DEPLOYED":
            # Avoid duplicating initial deployment if timestamp matches
            if abs(ev["created_at"] - deployed_at) < 0.001 and details_obj.get("to_version") == version:
                continue
            desc = f"Deployed release {details_obj.get('to_version')}"
        elif etype == "RELEASE_ROLLED_BACK":
            desc = f"Rolled back from {details_obj.get('from_version')} to {details_obj.get('to_version')} ({details_obj.get('reason')})"
        elif etype == "JOB_ENQUEUED":
            desc = f"Job enqueued ({details_obj.get('job_type')})"
        elif etype == "JOB_COMPLETED":
            desc = "Job completed successfully"
        elif etype == "JOB_FAILED":
            desc = f"Job failed: {details_obj.get('error', 'Execution error')}"
        elif etype == "RETRY_SCHEDULED":
            desc = f"Retry scheduled (attempt {details_obj.get('attempt')}, delay {details_obj.get('delay_seconds')}s)"
        elif etype == "DEAD_LETTERED":
            desc = f"Job moved to dead-letter queue: {details_obj.get('error', 'Max retries exhausted')}"
        elif etype == "WORKER_CRASHED":
            desc = f"Worker {details_obj.get('worker_id')} crashed: {details_obj.get('reason', 'Crash detected')}"
        elif etype == "WORKER_STARTED":
            desc = f"Worker {ev['actor']} registered (PID {details_obj.get('pid')})"

        timeline.append({
            "timestamp": ev["created_at"],
            "event_type": etype,
            "severity": ev["severity"],
            "description": desc,
            "job_id": ev["job_id"],
            "actor": ev["actor"],
        })

    # Sort timeline chronologically
    timeline.sort(key=lambda x: x["timestamp"])

    # 7. Health assessment & concise summary
    total_failures = failed_count + dead_letter_count
    was_rolled_back = rollback_info["was_rolled_back"]

    if was_rolled_back or dead_letter_count > 0 or worker_crashes >= 2:
        health = "CRITICAL"
    elif total_failures > 0 or retried_count > 0 or worker_crashes == 1:
        health = "DEGRADED"
    else:
        health = "HEALTHY"

    # Operator summary generation
    dur_str = _format_duration(active_duration_seconds)
    summary_parts = []
    if jobs_total > 0:
        summary_parts.append(
            f"{completed_count}/{jobs_total} jobs completed ({success_rate}% success)"
        )
    else:
        summary_parts.append("0 jobs processed")

    if dead_letter_count > 0:
        summary_parts.append(f"{dead_letter_count} dead-lettered")
    elif total_failures > 0:
        summary_parts.append(f"{total_failures} failures")

    if worker_crashes > 0:
        summary_parts.append(f"{worker_crashes} worker crash{'es' if worker_crashes > 1 else ''}")

    summary_detail = ", ".join(summary_parts)
    if was_rolled_back:
        summary = (
            f"Release {version} ({health}) exhibited {summary_detail}, and was rolled back to "
            f"{rollback_info['rolled_back_to']} after {dur_str}."
        )
    elif is_active:
        summary = (
            f"Release {version} ({health}) is currently active ({dur_str}) with {summary_detail}."
        )
    else:
        summary = (
            f"Release {version} ({health}) ran for {dur_str} with {summary_detail}."
        )

    return {
        "version": version,
        "description": release["description"],
        "is_active": is_active,
        "has_impact": True,
        "deployment": {
            "deployed_at": deployed_at,
            "deployed_by": deployed_by,
            "active_duration_seconds": active_duration_seconds,
            "ended_at": ended_at,
            "end_reason": end_reason,
        },
        "jobs": {
            "total": jobs_total,
            "completed": completed_count,
            "failed": failed_count,
            "dead_letter": dead_letter_count,
            "running": running_count,
            "queued": queued_count,
            "retried": retried_count,
            "success_rate_percent": success_rate,
        },
        "failures": {
            "total_failures": total_failures,
            "dead_letter_count": dead_letter_count,
            "failure_types": failure_types,
            "sample_errors": sample_errors,
        },
        "workers": {
            "crashes_detected": worker_crashes,
            "restarts_observed": worker_restarts,
            "affected_workers": sorted(list(affected_workers)),
            "events": worker_events_list,
        },
        "rollback": rollback_info,
        "milestones": {
            "deployed_at": deployed_at,
            "first_job_enqueued_at": first_job_enqueued_at,
            "first_failure_at": first_failure_at,
            "first_dead_letter_at": first_dead_letter_at,
            "first_worker_restart_at": first_worker_restart_at,
            "rolled_back_at": rollback_info["rolled_back_at"],
        },
        "timeline": timeline,
        "health": health,
        "summary": summary,
    }
