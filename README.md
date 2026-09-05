# NEXUS — Reliable Local Work Platform

[![Tests](https://img.shields.io/badge/tests-59%20passed-success?style=for-the-badge&logo=pytest)](nexus/tests/)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue?style=for-the-badge&logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com)
[![Storage](https://img.shields.io/badge/Storage-SQLite%20WAL-lightgrey?style=for-the-badge&logo=sqlite)](https://sqlite.org)
[![Cloud Dependencies](https://img.shields.io/badge/Cloud%20Dependencies-Zero%20(Offline%20First)-orange?style=for-the-badge)](.)

> **🏆 Panscience Innovations Hackathon Submission**  
> **NEXUS** is a self-contained, offline-first reliability platform for mission-critical background job processing on a single machine.  
> **Zero external brokers (no Redis, no RabbitMQ, no Docker required).** Pure local resilience powered by SQLite in WAL mode, atomic worker leases, crash recovery, release impact correlation, and one-action instant rollback.

---

## ⚡ 1-Minute Quick Start for Judges & Evaluators

Want to test NEXUS on your PC immediately? Run these 3 simple commands in your terminal:

```bash
# 1. Clone & Enter Repository
git clone https://github.com/Sparsh2509/Panscience_Innovations_Hackathon.git
cd Panscience_Innovations_Hackathon

# 2. Install Dependencies (FastAPI, Uvicorn, Pytest)
pip install -r requirements.txt

# 3. Launch EVERYTHING (Server + Web UI + Worker Fleet) in ONE command:
python scripts/start_production.py
```

👉 Now open your browser: **[http://localhost:8000](http://localhost:8000)**  
👉 Interactive API documentation: **[http://localhost:8000/docs](http://localhost:8000/docs)**

---

## 📋 Table of Contents

- [Judge's Evaluation & Quick Verification Guide](#-judges-evaluation--quick-verification-guide)
- [How to Run (Server, Workers, Web UI)](#-how-to-run-server-workers-web-ui)
  - [Option 1: One-Command All-in-One (Recommended)](#option-1-one-command-all-in-one-recommended)
  - [Option 2: Running Server and Workers in Separate Terminals](#option-2-running-server-and-workers-in-separate-terminals)
- [System Architecture & Reliability Guarantees](#-system-architecture--reliability-guarantees)
- [Interactive Operator Dashboard Walkthrough](#-interactive-operator-dashboard-walkthrough)
- [Automated Test Suite (59/59 Passing)](#-automated-test-suite-5959-passing)
- [Interactive CLI Requirement Demos (Phase 2 to 7)](#-interactive-cli-requirement-demos)
- [REST API Endpoints](#-rest-api-endpoints)
- [Project Directory Layout](#-project-directory-layout)

---

## 🎯 Judge's Evaluation & Quick Verification Guide

Here is a 3-minute interactive checklist for judges to evaluate every core reliability requirement in the platform:

| Requirement | Where to Test in UI (`http://localhost:8000`) | What to Expect |
|---|---|---|
| **1. Durable Job Submission** | **Jobs Tab** → Click **"Submit Job"** | Job is saved immediately to SQLite WAL; workers pick it up and mark it `COMPLETED`. |
| **2. Deduplication & Idempotency** | **Jobs Tab** → Click **"Send Duplicate Submission"** | System detects the duplicate `idempotency_key` and returns the existing result without running duplicate work. |
| **3. Live Worker Fleets & Heartbeats** | **Worker Fleet Tab** | Displays `worker-1` and `worker-2` with active PIDs, heartbeat timestamps, and live lease assignments. |
| **4. Worker Crash Recovery & Lease Reaping** | **Chaos Lab Tab** → Click **"Kill Worker Process"** | Worker process is terminated mid-flight. The Supervisor detects the crash, restarts a new worker process, and the Reaper rescues the orphaned job lease. |
| **5. Versioned Releases & 1-Action Rollback** | **Releases Tab** → Deploy `v1.1.0` then click **"Rollback"** | Zero-downtime atomic version switch with instant one-click rollback back to `v1.0.0`. |
| **6. Release-to-Behaviour Correlation (R-07)** | **Releases Tab** → Select release → View **"Impact & Correlation"** | Automatically correlates failures, retries, worker crashes, and milestones directly to that release—no timestamp matching needed! |
| **7. 100% Automated Test Coverage** | Run `pytest` in terminal | **59 passed tests** in ~6 seconds covering all layers. |

---

## 🚀 How to Run (Server, Workers, Web UI)

### Prerequisites
- **Python 3.10+** (Tested on Python 3.10, 3.11, 3.12, 3.13)
- **Git**
- *No Docker, Redis, or external database required!*

### Step 1: Clone & Setup Virtual Environment (Optional but Recommended)

#### Windows (PowerShell / CMD):
```powershell
git clone https://github.com/Sparsh2509/Panscience_Innovations_Hackathon.git
cd Panscience_Innovations_Hackathon

python -m venv venv
venv\Scripts\activate
```

#### macOS / Linux (Terminal):
```bash
git clone https://github.com/Sparsh2509/Panscience_Innovations_Hackathon.git
cd Panscience_Innovations_Hackathon

python3 -m venv venv
source venv/bin/activate
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```
*(Installs pure-Python lightweight packages: `fastapi`, `uvicorn`, `pydantic`, `pytest`, `httpx`)*

---

### Step 3: Choose How to Run

#### Option 1: One-Command All-in-One (Recommended)
This runs the **FastAPI Web Server**, the **Web UI**, and the **Worker Fleet Supervisor** together in one terminal:

```bash
python scripts/start_production.py
```

*Terminal Output:*
```text
[RENDER] Starting NEXUS Production Platform on 0.0.0.0:8000...
[RENDER] Starting Worker Supervisor & Fleet...
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

Now open: **`http://localhost:8000`**

---

#### Option 2: Running Server and Workers in Separate Terminals
If you want to observe live logs from the Web Server and the Worker Fleet independently:

**Terminal 1 — Web Server & Operator Dashboard:**
```bash
python scripts/run_server.py
```
*(Serves the Control Plane API & Dashboard on `http://127.0.0.1:8000`)*

**Terminal 2 — Worker Fleet & Reaper Supervisor:**
```bash
python scripts/run_workers.py
```
*(Spawns 2 independent Python worker subprocesses, performs periodic heartbeats, claims leases, and reaps expired jobs)*

---

## 🛡️ System Architecture & Reliability Guarantees

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

1. **SQLite Storage Engine in WAL Mode (`nexus/core/db.py`)**:
   - `PRAGMA journal_mode = WAL` enables concurrent readers alongside writers without locks.
   - `PRAGMA busy_timeout = 5000` prevents `database is locked` errors during burst traffic.
   - Context-managed `BEGIN IMMEDIATE` guarantees atomic transaction boundaries.

2. **Atomic Worker Leases & Heartbeats (`nexus/workers/` & `nexus/services/reaper.py`)**:
   - Workers claim work atomically with a 30-second visibility lease window.
   - Active workers emit heartbeats every 5 seconds to renew their lease.
   - If a worker crashes or hangs, the **Zombie Reaper** detects the expired lease and safely returns the job to `QUEUED` or quarantines it to `DEAD_LETTER`.

3. **Release-to-Behaviour Correlation (Requirement R-07)**:
   - Tracks software changes alongside runtime system health.
   - Automatically computes error rates, retries, worker crashes, and chronological milestones during each release's active window.

---

## 🖥️ Interactive Operator Dashboard Walkthrough

When you open **`http://localhost:8000`**, you have access to a clean dark-mode control room:

- **Top Bar:** Shows live system status (`ONLINE`), active software release (`v1.0.0`), and key fleet metrics.
- **Jobs Tab:**
  - Submit jobs with custom payloads, priorities (`0-10`), and idempotency keys.
  - Test idempotency by clicking *"Send Duplicate Submission"*.
  - Live table showing job status, retry count, active worker lease, and backoff countdowns.
  - 1-click manual retry button for failed jobs.
- **Worker Fleet Tab:**
  - Real-time status cards for each worker subprocess.
  - Completed jobs counter, active job lease ID, and last heartbeat timestamp.
- **Releases Tab:**
  - Create new versions and deploy them.
  - 1-Action Rollback button to instantly revert to previous release.
  - Release impact telemetry viewer.
- **Chaos Lab Tab:**
  - Kill worker processes to test automatic supervisor respawn and lease recovery.
  - Trigger database contention simulations.
  - Inspect the tamper-evident Immutable Audit Log.

---

## 🧪 Automated Test Suite (59/59 Passing)

NEXUS includes an automated test suite verifying every component end-to-end.

To run all tests:
```bash
pytest -v
```

*Output:*
```text
nexus/tests/test_api.py .............                                    [ 22%]
nexus/tests/test_db.py ........                                          [ 35%]
nexus/tests/test_job_service.py ...........                              [ 54%]
nexus/tests/test_phase3.py .........                                     [ 69%]
nexus/tests/test_release_impact.py .........                             [ 84%]
nexus/tests/test_release_service.py .........                            [100%]

======================= 59 passed in 6.52s =======================
```

---

## 📽️ Interactive CLI Requirement Demos

You can also run automated, step-by-step terminal demonstrations for each phase:

```bash
# Phase 2: Durable Ingestion & Idempotency
python scripts/demo_phase2.py

# Phase 3: Worker Claims, Heartbeats & Backoff
python scripts/demo_phase3.py

# Phase 4: Supervisor Crash Recovery & Orphan Reaper
python scripts/demo_phase4.py

# Phase 5: Versioned Releases & Atomic 1-Action Rollback
python scripts/demo_phase5.py

# Phase 6: Control Plane API, Audit Log & Chaos Injection
python scripts/demo_phase6.py

# Phase 7: R-07 Release-to-Behaviour Correlation & Timeline
python scripts/demo_phase7.py
```

---

## 📡 REST API Endpoints

Interactive Swagger UI: **`http://localhost:8000/docs`**

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | System health check & SQLite connectivity |
| `POST` | `/api/jobs` | Submit durable background job (with optional `idempotency_key`) |
| `GET` | `/api/jobs` | List jobs with status filter and pagination |
| `GET` | `/api/jobs/{id}` | Get full job state, payload, and retry history |
| `POST` | `/api/jobs/{id}/retry` | Manually re-arm a failed/dead-letter job |
| `GET` | `/api/workers` | List worker subprocesses, heartbeats, and status |
| `POST` | `/api/workers/register` | Register new worker in the fleet |
| `PUT` | `/api/workers/{id}/heartbeat` | Send worker heartbeat and extend lease |
| `GET` | `/api/releases` | List software release history |
| `GET` | `/api/releases/active` | Get currently active release |
| `POST` | `/api/releases` | Register a new release candidate |
| `POST` | `/api/releases/{version}/deploy` | Atomically deploy release version |
| `POST` | `/api/releases/rollback` | Instant 1-action atomic rollback |
| `GET` | `/api/releases/{version}/impact` | Get full release-to-behaviour correlation report |
| `GET` | `/api/audit` | Query immutable audit ledger |
| `POST` | `/api/chaos/kill-worker` | Terminate worker process to test recovery |
| `POST` | `/api/chaos/db-contention` | Simulate heavy SQLite write lock contention |

---

## 📁 Project Directory Layout

```
Panscience_Innovations_Hackathon/
├── nexus/
│   ├── api/                     # FastAPI control-plane REST endpoints
│   │   ├── routes/              # Routes: jobs, workers, releases, audit, chaos
│   │   ├── app.py               # Main FastAPI app & static dashboard mount
│   │   └── dependencies.py      # Database dependency injection
│   ├── core/                    # Core SQLite engine
│   │   ├── db.py                # WAL mode, busy timeouts, ACID transactions
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
│   │   ├── style.css            # Responsive CSS design system
│   │   └── app.js               # Reactive UI client
│   └── tests/                   # 59 automated unit and integration tests
├── data/                        # Local SQLite database files
├── scripts/                     # Launchers and verification demos
│   ├── start_production.py      # All-in-one runner (Server + Workers + UI)
│   ├── run_server.py            # API server & UI launcher
│   ├── run_workers.py           # Worker fleet & reaper launcher
│   └── demo_phase2.py ... 7.py  # Interactive requirement demos
├── pyproject.toml               # Project metadata
├── requirements.txt             # Python dependencies
└── README.md                    # Comprehensive documentation
```

---

## 👥 Authors & License

Built for the **Panscience Innovations Hackathon**.  
Released under the **MIT License**.
