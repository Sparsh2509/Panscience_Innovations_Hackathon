# NEXUS — Reliable Local Work Platform

A local, single-machine reliability platform for processing background jobs offline without external cloud dependencies.

## Key Features
- **Durable Job Persistence:** Single source of truth via SQLite in WAL mode with busy timeout handling.
- **Idempotency by Design:** Deduplication via unique idempotency keys.
- **Atomic Lease Claims:** Workers claim jobs using transactional leases and visibility timeouts.
- **Worker Heartbeats & Crash Recovery:** Zombie reaper detects dead worker leases and re-arms orphaned jobs.
- **Exponential Backoff & Dead-Lettering:** Bounded retries with jitter and automated dead-letter quarantine.
- **Release Management & 1-Action Rollback:** Instant version switching and rollback.
- **Cache vs. DB Drift Detection:** Background reconciliation of in-memory cache against canonical SQLite data.
- **Chaos / Failure Simulation:** 1-click failure scenarios to verify reliability guarantees under test.

## Tech Stack
- **Backend:** Python, FastAPI, SQLite (WAL mode)
- **Workers:** Independent Python subprocesses
- **Frontend:** HTML/CSS/JavaScript (Vanilla dark-mode operator console)
- **Tests:** pytest

## Project Layout
```
nexus/
├── api/          # FastAPI REST endpoints
├── core/         # SQLite storage engine & models
├── workers/      # Worker process loop & supervisor
├── services/     # Queue, reaper, releases, audit, chaos
├── static/       # Web dashboard assets
└── tests/        # Automated test suite
data/             # Local SQLite database files
```

## Running Tests
```bash
pytest
```
