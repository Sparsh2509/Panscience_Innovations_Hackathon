"""
Deterministic job handlers for local worker execution in NEXUS.
Supports testing of success, failures, delays, and echo without external dependencies.
"""

import time
from typing import Any


def execute_job_handler(job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Executes a job payload based on job_type.

    Supported types:
    - 'sleep': payload: {"seconds": float} -> sleeps, returns {"slept": seconds}
    - 'success': payload: {"message": str} -> returns {"status": "ok", "message": ...}
    - 'fail': payload: {"message": str} -> raises RuntimeError(message)
    - 'echo': payload: {"message": str} -> returns {"echo": message}
    """
    payload = payload or {}

    if job_type == "sleep":
        seconds = float(payload.get("seconds", 1.0))
        time.sleep(seconds)
        return {"status": "ok", "slept": seconds}

    elif job_type == "success":
        msg = payload.get("message", "Operation successful")
        return {"status": "ok", "message": msg}

    elif job_type == "fail":
        msg = payload.get("message", "Simulated job failure")
        raise RuntimeError(msg)

    elif job_type == "echo":
        msg = payload.get("message", "")
        return {"status": "ok", "echo": msg}

    else:
        # Generic fallback
        return {"status": "ok", "received": payload}
