"""
FastAPI dependency injection utilities for NEXUS.
Centralizes SQLite database connection lifecycle per request.
"""

from pathlib import Path
import sqlite3
from typing import Generator, Optional

from nexus.core.db import get_db, init_db

# Optional global override for testing or isolated executions
_DB_PATH_OVERRIDE: Optional[str | Path] = None


def set_db_override(db_path: Optional[str | Path]) -> None:
    """Sets an override database path for testing or specialized environments."""
    global _DB_PATH_OVERRIDE
    _DB_PATH_OVERRIDE = db_path
    if db_path:
        init_db(db_path)


def get_db_conn() -> Generator[sqlite3.Connection, None, None]:
    """
    Yields an active SQLite database connection with WAL mode enabled.
    Guarantees the connection is closed after request lifecycle finishes.
    """
    conn = get_db(_DB_PATH_OVERRIDE)
    try:
        yield conn
    finally:
        conn.close()
