# NEXUS — Reliable Local Work Platform

[![Tests](https://img.shields.io/badge/tests-59%20passed-success?style=for-the-badge&logo=pytest)](nexus/tests/)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue?style=for-the-badge&logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com)
[![Storage](https://img.shields.io/badge/Storage-SQLite%20WAL-lightgrey?style=for-the-badge&logo=sqlite)](https://sqlite.org)
[![Cloud Dependencies](https://img.shields.io/badge/Cloud%20Dependencies-Zero%20(Offline%20First)-orange?style=for-the-badge)](.)

> **A self-contained, single-machine reliability platform for mission-critical background job processing.**  
> Zero cloud dependencies, zero external brokers (no Redis, no RabbitMQ). Pure local resilience with SQLite WAL mode, atomic worker leases, crash recovery, release impact correlation, and one-action instant rollback.

---

## Table of Contents

- [Overview](#overview)
- [Key Reliability Guarantees](#key-reliability-guarantees)
- [Quick Start (Run on Localhost)](#quick-start-run-on-localhost)
  - [Prerequisites](#prerequisites)
  - [1. Clone & Setup Virtual Environment](#1-clone--setup-virtual-environment)
  - [2. Install Dependencies](#2-install-dependencies)
  - [3. Launch the Platform](#3-launch-the-platform)
    - [Option A: One-Command All-in-One (Recommended)](#option-a-one-command-all-in-one-recommended)
    - [Option B: Separate Server and Worker Fleet](#option-b-separate-server-and-worker-fleet)
  - [4. Open the Operator Dashboard](#4-open-the-operator-dashboard)
- [Operator Dashboard Features](#operator-dashboard-features)
- [Running Automated Tests & Demos](#running-automated-tests--demos)
- [Core Architecture & How It Works](#core-architecture--how-it-works)
- [REST API Reference](#rest-api-reference)
- [Project Layout](#project-layout)

---

## Overview

Modern cloud systems often introduce brittle dependencies—cloud message brokers, external caches, and distributed coordination locks—that fail during network partitions or offline scenarios.

**NEXUS** proves that enterprise-grade background job reliability can run locally on a single machine with zero external infrastructure. Everything runs against a hardened local **SQLite** engine operating in **WAL (Write-Ahead Logging)** mode with busy timeouts, transactional worker leases, automated zombie-process recovery, and automatic release-to-behaviour tracking.

---

## Key Reliability Guarantees

| Guarantee | Mechanism | Benefit |
|---|---|---|
| **Durable Ingestion** | SQLite WAL Mode + `PRAGMA busy_timeout = 5000` | Zero job loss even under process crashes or disk stalls. |
| **Deduplication & Idempotency** | Cryptographic idempotency keys + unique DB index | Safe duplicate submissions; duplicate payloads return original result. |
| **Atomic Lease Claims** | `BEGIN IMMEDIATE` + Visibility Timeouts (30s) | No two workers can ever claim the same job simultaneously. |
| **Worker Crash Recovery** | Subprocess supervisor + zombie lease reaper | Re-spawns dead workers and re-arms orphaned in-flight jobs. |
| **Exponential Backoff** | $2^{\text{attempt}} + \text{jitter}$ with dead-letter queue | Prevents cascading retry storms on downstream failures. |
| **1-Action Atomic Rollback** | Versioned release pointers (`releases` table) | Instantly revert a buggy release to previous known good state. |
| **Release-to-Behaviour Correlation (R-07)** | Automated telemetry window per release | Connects jobs, failures, crashes, and rollbacks without matching timestamps by eye. |
| **Chaos & Failure Lab** | Interactive failure injection APIs | Live verification of system resilience under simulated crashes & locks. |

---

## Quick Start (Run on Localhost)

Follow these steps to run NEXUS on your local machine in under 2 minutes.

### Prerequisites
- **Python 3.10+** (Python 3.10, 3.11, 3.12, or 3.13)
- **Git**

---

### 1. Clone & Setup Virtual Environment

#### On Windows (PowerShell or CMD):
```powershell
# Clone the repository
git clone https://github.com/Sparsh2509/Panscience_Innovations_Hackathon.git
cd Panscience_Innovations_Hackathon

# Create virtual environment
python -m venv venv

# Activate virtual environment
venv\Scripts\activate
```

#### On macOS / Linux (Terminal):
```bash
# Clone the repository
git clone https://github.com/Sparsh2509/Panscience_Innovations_Hackathon.git
cd Panscience_Innovations_Hackathon

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate
```

---

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

*(Installs lightweight pure-Python dependencies: `fastapi`, `uvicorn`, `pydantic`, `pytest`, `httpx`)*

---

### 3. Launch the Platform

You have two convenient options to start NEXUS:

#### Option A: One-Command All-in-One (Recommended)
This runs both the **FastAPI Web Control Plane** and the **Worker Fleet Supervisor (2 workers)** in a single terminal window:

```bash
python scripts/start_production.py
```

*Output:*
```text
[RENDER] Starting NEXUS Production Platform on 0.0.0.0:8000...
[RENDER] Starting Worker Supervisor & Fleet...
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

---

#### Option B: Separate Server and Worker Fleet
If you want to view live worker process output in a dedicated terminal:

**Terminal 1 — API Server & Web UI:**
```bash
python scripts/run_server.py
```
*(Runs FastAPI on `http://127.0.0.1:8000`)*

**Terminal 2 — Worker Fleet & Reaper:**
```bash
python scripts/run_workers.py
```
*(Spawns `worker-1` and `worker-2`, performs periodic heartbeats, and reaps expired leases)*

---

### 4. Open the Operator Dashboard

Open your web browser and navigate to:

👉 **[http://localhost:8000](http://localhost:8000)**

*(Interactive Swagger OpenAPI documentation is also available at **[http://localhost:8000/docs](http://localhost:8000/docs)**)*

---

## Operator Dashboard Features

The NEXUS Web Console gives operators a live dark-mode interface:

1. **Header & Live Telemetry:**
   - **System Status Badge:** Real-time polling indicator (ONLINE / OFFLINE).
   - **Active Release Pill:** Displays active version (`v1.0.0`). Clicking it opens the Releases manager.
   - **Top Metrics:** Active Workers, Queued Jobs, In-Flight Jobs, Failed / Dead-Letter count.

2. **Job Operations Tab:**
   - **Submit Jobs:** Enter Job Type (`payment`, `email`, `sync`, etc.), priority (`0-10`), optional Idempotency Key, and JSON payload.
   - **Idempotency Guarantee Demo:** Click *"Send Duplicate Submission"* to observe how duplicate idempotency keys return identical results without running duplicate work.
   - **Live Jobs Table:** Displays Status (`QUEUED`, `CLAIMED`, `COMPLETED`, `FAILED`, `DEAD_LETTER`), active worker lease, retry count, and exponential backoff timers.
   - **Manual Retry Action:** Any failed or dead-letter job can be re-armed with a single click.

3. **Worker Fleet Tab:**
   - Visual cards for all worker subprocesses (`worker-1`, `worker-2`).
   - Displays real-time PID, status (`IDLE`, `BUSY`, `DEAD`), last heartbeat timestamp, and current job lease.
   - Live completed jobs counter per worker.

4. **Releases & Impact Correlation (R-07) Tab:**
   - **Deploy New Version:** Create and deploy releases (e.g., `v1.1.0`, `v2.0.0`).
   - **1-Action Atomic Rollback:** Instantly rollback to the previous active release with one click.
   - **Release Impact Analytics:** Automatically links releases to errors, retries, worker crashes, and job throughput during that release's active lifespan—no manual timestamp matching needed.

5. **Chaos & Resilience Lab:**
   - **Simulate Worker SIGKILL:** Crashes a worker process mid-execution; observe the supervisor automatically detect the crash, mark it dead, re-spawn a healthy worker, and re-arm the interrupted job lease without data loss.
   - **DB Lock Contention Simulation:** Verifies that SQLite WAL mode and busy handlers gracefully queue transactions without throwing database locked errors.
   - **Immutable Audit Log:** Chronological, tamper-evident log of all system transitions (claims, heartbeats, completions, crashes, rollbacks).

---

## Running Automated Tests & Demos

### Run Full Test Suite (59 Tests)
Run the automated pytest test suite covering API, SQLite persistence, concurrency, worker leases, crash recovery, release rollbacks, and release impact correlation:

```bash
pytest -v
```

*Expected output:*
```text
======================= 59 passed in 6.5s =======================
```

### Run Verification Demonstrations
Step-by-step interactive command-line demonstrations:

```bash
# Phase 2 Demo: Durable job submission & idempotency
python scripts/demo_phase2.py

# Phase 3 Demo: Worker lease claiming, heartbeats, and backoff
python scripts/demo_phase3.py

# Phase 4 Demo: Supervisor crash recovery & orphan lease reaping
python scripts/demo_phase4.py

# Phase 5 Demo: Versioned releases & 1-action atomic rollback
python scripts/demo_phase5.py

# Phase 6 Demo: Control-plane API, audit trail, and chaos injection
python scripts/demo_phase6.py

# Phase 7 Demo: R-07 Release-to-Behaviour Correlation & Timeline
python scripts/demo_phase7.py
```

---

## Core Architecture & How It Works

```
                        ┌──────────────────────────────┐
                        │   Operator Browser / Client  │
                        │    (Vanilla JS/CSS UI)       │
                        └──────────────┬───────────────┘
                                       │ HTTP / REST
                                       ▼
                        ┌──────────────────────────────┐
                        │   FastAPI Control Plane      │
                        │ (Job Ingestion / Releases)   │
                        └──────────────┬───────────────┘
                                       │
                         ACID Transactions (WAL Mode)
                                       │
                                       ▼
                        ┌──────────────────────────────┐
                        │  SQLite Storage Engine       │
                        │  - jobs (idempotency, leases)│
                        │  - workers (heartbeats, PIDs)│
                        │  - releases (active pointer) │
                        │  - audit_log (immutable)     │
                        └──────────────▲───────────────┘
                                       │
                         Atomic Claims & Heartbeats
                                       │
                        ┌──────────────┴───────────────┐
                        │  Worker Supervisor           │
                        │  ┌──────────┐  ┌──────────┐  │
                        │  │ Worker 1 │  │ Worker 2 │  │
                        │  └──────────┘  └──────────┘  │
                        │  + Zombie Lease Reaper       │
                        └──────────────────────────────┘
```

### 1. SQLite WAL Engine (`nexus/core/db.py`)
- Configured with `PRAGMA journal_mode = WAL` (Write-Ahead Logging), allowing concurrent readers without blocking writers.
- Configured with `PRAGMA busy_timeout = 5000` to automatically retry locked transactions for up to 5 seconds.
- Transaction context manager with explicit `BEGIN IMMEDIATE` isolation for lease claims.

### 2. Leases & Crash Recovery (`nexus/workers/` & `nexus/services/reaper.py`)
- Workers claim jobs by setting `claimed_by = worker_id`, `status = 'CLAIMED'`, and `lease_expires_at = NOW() + 30s`.
- While working, workers ping `PUT /api/workers/{id}/heartbeat` every 5 seconds to extend their lease.
- If a worker dies (power loss, SIGKILL, out-of-memory), its lease expires. The **Reaper** detects expired leases and transitions the job back to `QUEUED` (incrementing retry count) or `DEAD_LETTER`.

### 3. Release Impact Correlation (`nexus/services/release_impact.py`)
- Every job records the `release_version` active at the moment it was claimed.
- Audit events log every deployment, rollback, and worker crash with the active release tag.
- When an operator queries `GET /api/releases/{version}/impact`, NEXUS computes the exact duration, failure rate, retries, crashes, and event milestones that occurred under that specific release.

---

## REST API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | System health check and database connectivity |
| `POST` | `/api/jobs` | Submit a new durable job (with optional `idempotency_key`) |
| `GET` | `/api/jobs` | List background jobs with status filtering and pagination |
| `GET` | `/api/jobs/{id}` | Retrieve detailed job state and execution payload |
| `POST` | `/api/jobs/{id}/retry` | Manually re-arm a failed or dead-letter job |
| `GET` | `/api/workers` | List all registered worker processes and heartbeats |
| `POST` | `/api/workers/register` | Register a new worker in the fleet |
| `PUT` | `/api/workers/{id}/heartbeat` | Send worker heartbeat and extend active lease |
| `GET` | `/api/releases` | List all historical software releases |
| `GET` | `/api/releases/active` | Get currently active release pointer |
| `POST` | `/api/releases` | Create a new release candidate |
| `POST` | `/api/releases/{version}/deploy` | Atomically deploy a release version |
| `POST` | `/api/releases/rollback` | Perform atomic 1-action rollback to previous release |
| `GET` | `/api/releases/{version}/impact` | Get full release-to-behaviour correlation & timeline |
| `GET` | `/api/audit` | Query immutable audit log with event filtering |
| `POST` | `/api/chaos/kill-worker` | Inject chaos: terminate worker process to test recovery |
| `POST` | `/api/chaos/db-contention` | Inject chaos: simulate database write contention |

Full interactive API docs: **`http://localhost:8000/docs`**

---

## Project Layout

```
Panscience_Innovations_Hackathon/
├── nexus/
│   ├── api/                     # FastAPI control-plane REST endpoints
│   │   ├── routes/              # Modular routes (jobs, workers, releases, audit, chaos)
│   │   ├── app.py               # Main FastAPI application and static mount
│   │   └── dependencies.py      # Request database dependencies
│   ├── core/                    # SQLite engine and schema initialization
│   │   ├── db.py                # WAL mode, busy timeouts, transaction context manager
│   │   └── models.py            # Pydantic data schemas
│   ├── services/                # Business logic layer
│   │   ├── job_service.py       # Ingestion, idempotency, atomic lease claiming
│   │   ├── release_service.py   # Versioned releases and 1-action rollback
│   │   ├── release_impact.py    # R-07 Release-to-behaviour correlation engine
│   │   ├── reaper.py            # Orphaned lease detection and recovery
│   │   └── audit_service.py     # Immutable audit log recorder
│   ├── workers/                 # Worker fleet implementation
│   │   ├── worker.py            # Worker subprocess claim & heartbeat loop
│   │   └── supervisor.py        # Process supervisor and crash auto-restart
│   ├── static/                  # Vanilla Web Operator Console
│   │   ├── index.html           # Dark-mode dashboard layout
│   │   ├── style.css            # Custom CSS design system
│   │   └── app.js               # Reactive UI client
│   └── tests/                   # 59 automated unit and integration tests
├── data/                        # Local SQLite database directory (WAL / SHM / DB)
├── scripts/                     # Launchers and verification demos
│   ├── start_production.py      # All-in-one runner (API + Supervisor)
│   ├── run_server.py            # API server launcher
│   ├── run_workers.py           # Worker fleet launcher
│   └── demo_phase2.py ... 7.py  # Interactive requirement demos
├── pyproject.toml               # Project metadata
├── requirements.txt             # Python dependencies
└── README.md                    # Comprehensive documentation
```

---

## License

This project was built for the **Panscience Innovations Hackathon**. Open-source under the MIT License.
