"""
Server launcher for NEXUS FastAPI Control Plane.
Run:
    python scripts/run_server.py
"""

from pathlib import Path
import sys
import uvicorn

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from nexus.core.db import init_db

if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    print("Starting NEXUS Control Plane on http://127.0.0.1:8000...")
    print("Interactive API Docs available at http://127.0.0.1:8000/docs")
    uvicorn.run("nexus.api.app:app", host="127.0.0.1", port=8000, reload=True)
