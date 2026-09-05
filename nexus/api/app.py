"""
FastAPI Control Plane Application for NEXUS.
Modular monolith running on a single machine, exposing durable job submission,
worker fleet monitoring, release management, audit query, and chaos simulation.
"""

from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from nexus.api.dependencies import get_db_conn
from nexus.api.routes import audit, chaos, jobs, releases, workers
from nexus.core.db import get_db, init_db
from nexus.services.job_service import (
    JobNotFoundError,
    JobValidationError,
    LeaseAuthorizationError,
)
from nexus.services.release_service import (
    NoRollbackTargetError,
    ReleaseAlreadyExistsError,
    ReleaseNotFoundError,
    ReleaseValidationError,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes the database schema and default initial release on startup."""
    init_db()
    yield


app = FastAPI(
    title="NEXUS Reliability Platform Control Plane",
    version="1.0.0",
    description=(
        "Local, single-machine reliability platform for processing background jobs offline. "
        "Provides durable ingestion, lease claiming, bounded retries, worker crash recovery, "
        "and one-action release rollback."
    ),
    lifespan=lifespan,
)

# Minimal local-development CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception Handlers
@app.exception_handler(JobValidationError)
@app.exception_handler(ReleaseValidationError)
async def validation_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": str(exc)},
    )


@app.exception_handler(JobNotFoundError)
@app.exception_handler(ReleaseNotFoundError)
async def not_found_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": str(exc)},
    )


@app.exception_handler(ReleaseAlreadyExistsError)
@app.exception_handler(NoRollbackTargetError)
@app.exception_handler(LeaseAuthorizationError)
async def conflict_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"detail": str(exc)},
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "An internal server error occurred."},
    )


# Health Endpoint
@app.get("/health", tags=["Health"], summary="System health check")
def health_check():
    """
    Returns platform health status and verifies SQLite connectivity.
    Does not depend on worker processes being active.
    """
    db_status = "connected"
    try:
        conn = get_db()
        conn.execute("SELECT 1")
        conn.close()
    except Exception:
        db_status = "unreachable"

    return {
        "status": "ok" if db_status == "connected" else "degraded",
        "service": "nexus",
        "database": db_status,
    }


# Include Routers
app.include_router(jobs.router, prefix="/api/jobs", tags=["Jobs"])
app.include_router(workers.router, prefix="/api/workers", tags=["Workers"])
app.include_router(releases.router, prefix="/api/releases", tags=["Releases"])
app.include_router(audit.router, prefix="/api/audit", tags=["Audit"])
app.include_router(chaos.router, prefix="/api/chaos", tags=["Chaos & Failure Simulation"])

# Static Dashboard Mounting
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)

@app.get("/", include_in_schema=False)
def serve_root():
    """Serves the NEXUS Operator Dashboard control room."""
    return FileResponse(STATIC_DIR / "index.html")

@app.get("/dashboard", include_in_schema=False)
def serve_dashboard():
    """Alternative route for the operator dashboard."""
    return FileResponse(STATIC_DIR / "index.html")

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

