/**
 * NEXUS Operator Dashboard — Vanilla JavaScript Controller
 * Interacts exclusively with local Phase 5 FastAPI endpoints.
 */

// Centralized API Client
const api = {
  async get(endpoint) {
    try {
      const res = await fetch(endpoint);
      api.setConnected(true);
      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP Error ${res.status}`);
      }
      return await res.json();
    } catch (err) {
      if (err.name === "TypeError" && err.message.includes("fetch")) {
        api.setConnected(false);
      }
      throw err;
    }
  },

  async post(endpoint, body = {}) {
    try {
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      api.setConnected(true);
      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP Error ${res.status}`);
      }
      return await res.json();
    } catch (err) {
      if (err.name === "TypeError" && err.message.includes("fetch")) {
        api.setConnected(false);
      }
      throw err;
    }
  },

  setConnected(isConnected) {
    const dot = document.getElementById("connection-dot");
    const text = document.getElementById("connection-text");
    const banner = document.getElementById("api-disconnected-banner");

    if (banner) {
      banner.style.display = isConnected ? "none" : "block";
    }

    if (!dot || !text) return;

    if (isConnected) {
      dot.className = "status-dot connected";
      text.textContent = "API Connected";
    } else {
      dot.className = "status-dot disconnected";
      text.textContent = "API Disconnected";
    }
  },
};

// Global State
let currentStatusFilter = "ALL";
let pollCountdown = 3;
let pollTimer = null;
let auditPollTimer = null;
let allJobsCache = [];
let allWorkersCache = [];

// DOM Ready initialization
document.addEventListener("DOMContentLoaded", () => {
  initEventListeners();
  refreshAll();
  startPolling();
});

function initEventListeners() {
  // Manual refresh button
  document.getElementById("btn-manual-refresh")?.addEventListener("click", () => {
    refreshAll();
    showToast("info", "Dashboard refreshed.");
  });

  // Filter buttons for jobs
  document.querySelectorAll(".filter-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      document.querySelectorAll(".filter-btn").forEach((b) => b.classList.remove("active"));
      e.target.classList.add("active");
      currentStatusFilter = e.target.dataset.status;
      renderJobs(allJobsCache);
    });
  });

  // Submit Job Form
  const jobForm = document.getElementById("form-submit-job");
  jobForm?.addEventListener("submit", handleJobSubmit);

  // Pre-fill Payload Templates
  document.getElementById("job-template-select")?.addEventListener("change", (e) => {
    const payloadArea = document.getElementById("input-job-payload");
    const jobTypeInput = document.getElementById("input-job-type");
    const val = e.target.value;
    if (val === "sleep") {
      jobTypeInput.value = "sleep";
      payloadArea.value = JSON.stringify({ seconds: 2 }, null, 2);
    } else if (val === "echo") {
      jobTypeInput.value = "echo";
      payloadArea.value = JSON.stringify({ message: "Hello from NEXUS" }, null, 2);
    } else if (val === "success") {
      jobTypeInput.value = "success";
      payloadArea.value = JSON.stringify({ message: "Payment settled" }, null, 2);
    } else if (val === "fail") {
      jobTypeInput.value = "fail";
      payloadArea.value = JSON.stringify({ message: "Simulated upstream timeout" }, null, 2);
    }
  });

  // Create Release Form
  const releaseForm = document.getElementById("form-create-release");
  releaseForm?.addEventListener("submit", handleCreateRelease);

  // Rollback Button (R-06)
  document.getElementById("btn-trigger-rollback")?.addEventListener("click", () => {
    confirmAction(
      "Confirm One-Action Rollback (R-06)",
      "Rollback the current release to the previous deployment?",
      async () => {
        try {
          const res = await api.post("/api/releases/rollback", {
            actor: "operator",
            reason: "One-action rollback triggered from dashboard",
          });
          showToast("success", `Rollback successful: ${res.from_version} → ${res.to_version}`);
          refreshAll();
        } catch (err) {
          showToast("error", `Rollback failed: ${err.message}`);
        }
      },
      "Rollback"
    );
  });

  // Chaos: Fail Job
  document.getElementById("btn-chaos-fail")?.addEventListener("click", () => {
    const select = document.getElementById("chaos-job-select");
    const jobId = select?.value;
    if (!jobId) {
      showToast("warning", "Select a job to fail.");
      return;
    }
    confirmAction(
      "Force Job Failure",
      `Inject controlled failure into job ${jobId.substring(0, 8)}...?`,
      async () => {
        try {
          const res = await api.post(`/api/chaos/fail-job/${jobId}`);
          showToast("warning", `Failure injected. Job status: ${res.new_status} (attempt ${res.attempt_count}/${res.max_retries})`);
          refreshAll();
        } catch (err) {
          showToast("error", `Chaos failed: ${err.message}`);
        }
      }
    );
  });

  // Chaos: Crash Worker
  document.getElementById("btn-chaos-crash")?.addEventListener("click", () => {
    const select = document.getElementById("chaos-worker-select");
    const workerId = select?.value;
    if (!workerId) {
      showToast("warning", "Select a worker to terminate.");
      return;
    }
    confirmAction(
      "Simulate Worker Hard Crash",
      `Terminate worker '${workerId}' process (SIGTERM)?`,
      async () => {
        try {
          const res = await api.post(`/api/chaos/crash-worker/${workerId}`);
          showToast("warning", `Crash signal sent: ${res.result} (PID ${res.pid})`);
          refreshAll();
        } catch (err) {
          showToast("error", `Crash failed: ${err.message}`);
        }
      }
    );
  });

  // Chaos: Simulate Incident
  document.getElementById("btn-chaos-incident")?.addEventListener("click", () => {
    confirmAction(
      "Simulate Release Incident",
      "Deploy buggy release 'v1.1.0-buggy' to demonstrate zero-touch rollback?",
      async () => {
        try {
          const res = await api.post("/api/chaos/simulate-release-incident");
          showToast("error", `Incident active: ${res.active_release} deployed! Recommended: trigger Rollback.`);
          refreshAll();
        } catch (err) {
          showToast("error", `Incident simulation failed: ${err.message}`);
        }
      }
    );
  });

  // Modal Closers
  document.querySelectorAll(".modal-close, .modal-cancel").forEach((btn) => {
    btn.addEventListener("click", closeAllModals);
  });
}

// Master refresh
async function refreshAll() {
  await Promise.allSettled([
    loadOverviewAndJobs(),
    loadWorkers(),
    loadReleases(),
    loadAudit(),
  ]);
}

// Section 1 & 2: Overview & Jobs
async function loadOverviewAndJobs() {
  try {
    const jobs = await api.get("/api/jobs?limit=100");
    allJobsCache = jobs;
    renderOverview(jobs);
    renderJobs(jobs);
    updateChaosJobSelect(jobs);
  } catch (err) {
    console.error("Failed to load jobs:", err);
  }
}

function renderOverview(jobs) {
  const counts = {
    QUEUED: 0,
    RUNNING: 0,
    COMPLETED: 0,
    FAILED: 0,
    DEAD_LETTER: 0,
  };

  jobs.forEach((j) => {
    if (counts[j.status] !== undefined) counts[j.status]++;
  });

  document.getElementById("count-queued").textContent = counts.QUEUED;
  document.getElementById("count-running").textContent = counts.RUNNING;
  document.getElementById("count-completed").textContent = counts.COMPLETED;
  document.getElementById("count-failed").textContent = counts.FAILED;
  document.getElementById("count-dead-letter").textContent = counts.DEAD_LETTER;
}

function renderJobs(jobs) {
  const tbody = document.getElementById("jobs-table-body");
  if (!tbody) return;

  const filtered = currentStatusFilter === "ALL"
    ? jobs
    : jobs.filter((j) => j.status === currentStatusFilter);

  if (filtered.length === 0) {
    tbody.innerHTML = `<tr><td colspan="10" style="text-align: center; color: var(--text-muted); padding: 24px;">No jobs found with status '${currentStatusFilter}'.</td></tr>`;
    return;
  }

  tbody.innerHTML = filtered.map((j) => {
    const timeStr = j.created_at ? new Date(j.created_at * 1000).toLocaleTimeString() : "-";
    const statusBadge = getStatusBadge(j.status);
    const shortId = j.id ? j.id.substring(0, 8) : "";
    const lastErr = j.last_error ? escapeHtml(j.last_error.substring(0, 45) + (j.last_error.length > 45 ? "..." : "")) : "-";

    return `
      <tr class="clickable-row" onclick="showJobDetails('${j.id}')">
        <td class="cell-mono">${shortId}</td>
        <td><strong>${escapeHtml(j.job_type)}</strong></td>
        <td>${statusBadge}</td>
        <td>${j.priority || 0}</td>
        <td class="cell-mono">${j.attempt_count}</td>
        <td class="cell-mono">${j.max_retries}</td>
        <td class="cell-mono">${escapeHtml(j.release_version || "v1.0.0")}</td>
        <td style="color: var(--text-muted);">${timeStr}</td>
        <td class="cell-mono" style="color: var(--color-danger-bright);" title="${escapeHtml(j.last_error || '')}">${lastErr}</td>
        <td><button class="btn btn-sm" onclick="event.stopPropagation(); showJobDetails('${j.id}')">Details</button></td>
      </tr>
    `;
  }).join("");
}

// Section 3: Job Submission
async function handleJobSubmit(e) {
  e.preventDefault();
  const jobType = document.getElementById("input-job-type").value.trim();
  const payloadRaw = document.getElementById("input-job-payload").value.trim();
  const idempotencyKey = document.getElementById("input-job-idempotency").value.trim() || null;
  const priority = parseInt(document.getElementById("input-job-priority").value, 10) || 0;
  const maxRetries = parseInt(document.getElementById("input-job-retries").value, 10) || 3;

  let payload = {};
  if (payloadRaw) {
    try {
      payload = JSON.parse(payloadRaw);
    } catch {
      showToast("error", "Invalid JSON payload.");
      return;
    }
  }

  try {
    const res = await api.post("/api/jobs", {
      job_type: jobType,
      payload,
      idempotency_key: idempotencyKey,
      priority,
      max_retries: maxRetries,
    });

    if (res.deduplicated) {
      showToast("info", `Idempotency match: existing job ${res.job.id.substring(0, 8)} returned (Deduplicated: true).`);
    } else {
      showToast("success", `Job ${res.job.id.substring(0, 8)} enqueued successfully.`);
    }

    refreshAll();
  } catch (err) {
    showToast("error", `Failed to submit job: ${err.message}`);
  }
}

// Section 4: Workers
async function loadWorkers() {
  try {
    const workers = await api.get("/api/workers");
    allWorkersCache = workers;
    renderWorkers(workers);
    updateChaosWorkerSelect(workers);

    // Update worker counts in overview
    let total = workers.length;
    let healthy = workers.filter((w) => w.healthy).length;
    let dead = workers.filter((w) => w.status === "DEAD").length;

    document.getElementById("count-workers-total").textContent = total;
    document.getElementById("count-workers-healthy").textContent = healthy;
    document.getElementById("count-workers-dead").textContent = dead;
  } catch (err) {
    console.error("Failed to load workers:", err);
  }
}

function renderWorkers(workers) {
  const container = document.getElementById("workers-container");
  if (!container) return;

  if (workers.length === 0) {
    container.innerHTML = `<div style="color: var(--text-muted); padding: 16px;">No workers registered. Start workers with supervisor.</div>`;
    return;
  }

  container.innerHTML = workers.map((w) => {
    const badgeClass = w.status === "IDLE" ? "badge-idle" : w.status === "BUSY" ? "badge-busy" : "badge-dead";
    const healthPill = w.healthy
      ? `<span class="badge badge-completed">Healthy</span>`
      : `<span class="badge badge-failed">Dead / Stale</span>`;
    const startedStr = w.started_at ? new Date(w.started_at * 1000).toLocaleTimeString() : "-";
    const borderColor = w.healthy ? "var(--border-color)" : "var(--color-danger)";
    const restartBtn = !w.healthy
      ? `<button class="btn btn-sm" style="background:#238636;border-color:#238636;color:#fff;" onclick="restartWorker('${w.id}')">↺ Restart</button>`
      : "";

    return `
      <div style="background: rgba(0,0,0,0.25); border: 1px solid ${borderColor}; border-radius: var(--radius-sm); padding: 12px; display: flex; justify-content: space-between; align-items: center;">
        <div>
          <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 4px;">
            <strong class="cell-mono">${escapeHtml(w.id)}</strong>
            <span class="badge ${badgeClass}">${w.status}</span>
            ${healthPill}
          </div>
          <div style="font-size: 11px; color: var(--text-muted);" class="cell-mono">
            PID: ${w.pid} | Started: ${startedStr} | Heartbeat Age: ${w.heartbeat_age_seconds}s | Current Job: ${w.current_job_id ? w.current_job_id.substring(0, 8) : "None"}
          </div>
        </div>
        <div style="display:flex;gap:8px;align-items:center;">
          ${restartBtn}
          <button class="btn btn-sm btn-danger" onclick="quickCrashWorker('${w.id}')">Crash (Kill)</button>
        </div>
      </div>
    `;
  }).join("");
}

// Section 5: Releases & Rollback
async function loadReleases() {
  try {
    const [releases, active] = await Promise.all([
      api.get("/api/releases"),
      api.get("/api/releases/active"),
    ]);

    // Update active release badge
    const activeVer = active?.version || "v1.0.0";
    document.getElementById("overview-active-release").textContent = activeVer;
    document.getElementById("banner-active-release").textContent = activeVer;

    const tbody = document.getElementById("releases-table-body");
    if (!tbody) return;

    tbody.innerHTML = releases.map((r) => {
      const isActive = r.is_active === 1;
      const activeBadge = isActive
        ? `<span class="badge badge-completed">ACTIVE</span>`
        : `<span class="badge badge-idle">INACTIVE</span>`;
      const timeStr = r.deployed_at ? new Date(r.deployed_at * 1000).toLocaleString() : "-";

      const actionBtn = isActive
        ? `<span style="font-size: 11px; color: var(--color-success-bright); font-weight: 600;">Current Active</span>`
        : `<button class="btn btn-sm btn-primary" onclick="deployReleaseVersion('${r.version}')">Deploy</button>`;

      const impactBtn = `<button class="btn btn-sm" style="background:#21262d;border:1px solid #30363d;color:var(--text-main);padding:3px 8px;font-size:11px;" onclick="openReleaseImpactModal('${escapeHtml(r.version)}')">📊 Impact</button>`;

      return `
        <tr>
          <td class="cell-mono"><strong>${escapeHtml(r.version)}</strong></td>
          <td>${activeBadge}</td>
          <td>${escapeHtml(r.description || "")}</td>
          <td class="cell-mono">${escapeHtml(r.deployed_by || "-")}</td>
          <td style="color: var(--text-muted);">${timeStr}</td>
          <td>
            <div style="display: flex; gap: 6px; align-items: center;">
              ${impactBtn}
              ${actionBtn}
            </div>
          </td>
        </tr>
      `;
    }).join("");
  } catch (err) {
    console.error("Failed to load releases:", err);
  }
}

async function handleCreateRelease(e) {
  e.preventDefault();
  const version = document.getElementById("input-rel-version").value.trim();
  const description = document.getElementById("input-rel-desc").value.trim();
  const configRaw = document.getElementById("input-rel-config").value.trim();

  let config = {};
  if (configRaw) {
    try {
      config = JSON.parse(configRaw);
    } catch {
      showToast("error", "Invalid JSON config.");
      return;
    }
  }

  try {
    await api.post("/api/releases", {
      version,
      description,
      config,
      deployed_by: "operator-ui",
    });
    showToast("success", `Release ${version} created.`);
    document.getElementById("form-create-release").reset();
    refreshAll();
  } catch (err) {
    showToast("error", `Failed to create release: ${err.message}`);
  }
}

async function deployReleaseVersion(version) {
  confirmAction(
    "Confirm Deployment",
    `Deploy release '${version}' as the active release?`,
    async () => {
      try {
        await api.post(`/api/releases/${version}/deploy`, {
          actor: "operator",
          reason: "Manual deployment from control room",
        });
        showToast("success", `Release ${version} is now active.`);
        refreshAll();
      } catch (err) {
        showToast("error", `Deployment failed: ${err.message}`);
      }
    }
  );
}

// Section 6: Audit Log
async function loadAudit() {
  try {
    const events = await api.get("/api/audit?limit=25");
    const container = document.getElementById("audit-stream");
    if (!container) return;

    document.getElementById("overview-audit-count").textContent = events.length;

    if (events.length === 0) {
      container.innerHTML = `<div style="color: var(--text-muted); padding: 12px;">No audit events recorded.</div>`;
      return;
    }

    container.innerHTML = events.map((ev, idx) => {
      const timeStr = new Date(ev.created_at * 1000).toLocaleTimeString();
      const detailsJson = JSON.stringify(ev.details || {}, null, 2);

      return `
        <div class="audit-entry severity-${ev.severity}" onclick="toggleAuditDetail(${idx})" style="cursor: pointer;" title="Click to toggle full JSON details">
          <div class="audit-header">
            <span class="audit-type">${escapeHtml(ev.event_type)}</span>
            <span>${timeStr}</span>
          </div>
          <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 2px;">
            Actor: <strong>${escapeHtml(ev.actor)}</strong> ${ev.job_id ? `| Job: <span class="cell-mono">${ev.job_id.substring(0, 8)}</span>` : ""}
          </div>
          <div id="audit-preview-${idx}" class="audit-details">${escapeHtml(JSON.stringify(ev.details || {}))}</div>
          <pre id="audit-full-${idx}" class="cell-mono" style="display: none; font-size: 11px; background: rgba(0,0,0,0.3); padding: 6px; border-radius: 4px; margin-top: 4px; overflow-x: auto; white-space: pre-wrap;">${escapeHtml(detailsJson)}</pre>
        </div>
      `;
    }).join("");
  } catch (err) {
    console.error("Failed to load audit:", err);
  }
}

function toggleAuditDetail(idx) {
  const full = document.getElementById(`audit-full-${idx}`);
  const prev = document.getElementById(`audit-preview-${idx}`);
  if (!full || !prev) return;
  if (full.style.display === "none") {
    full.style.display = "block";
    prev.style.display = "none";
  } else {
    full.style.display = "none";
    prev.style.display = "block";
  }
}

// Section 7: Chaos Helpers
function updateChaosJobSelect(jobs) {
  const select = document.getElementById("chaos-job-select");
  if (!select) return;
  const currentVal = select.value;

  const claimable = jobs.filter((j) => j.status === "QUEUED" || j.status === "RUNNING");
  select.innerHTML = '<option value="">-- Select Job to Fail --</option>' + claimable.map((j) => {
    return `<option value="${j.id}">${j.job_type} (${j.status}) - ${j.id.substring(0, 8)}</option>`;
  }).join("");

  if (currentVal) select.value = currentVal;
}

function updateChaosWorkerSelect(workers) {
  const select = document.getElementById("chaos-worker-select");
  if (!select) return;
  const currentVal = select.value;

  const live = workers.filter((w) => w.status !== "DEAD");
  select.innerHTML = '<option value="">-- Select Worker to Crash --</option>' + live.map((w) => {
    return `<option value="${w.id}">${w.id} (PID ${w.pid}, ${w.status})</option>`;
  }).join("");

  if (currentVal) select.value = currentVal;
}

function quickCrashWorker(workerId) {
  confirmAction(
    "Crash Worker",
    `Immediately send SIGTERM to worker '${workerId}'?`,
    async () => {
      try {
        const res = await api.post(`/api/chaos/crash-worker/${workerId}`);
        showToast("warning", `Crash request dispatched: ${res.result} for ${workerId}`);
        refreshAll();
      } catch (err) {
        showToast("error", `Crash failed: ${err.message}`);
      }
    }
  );
}

async function restartWorker(workerId) {
  try {
    const res = await api.post(`/api/workers/${workerId}/start`);
    showToast("success", `Worker '${workerId}' restarted — PID ${res.pid}`);
    setTimeout(refreshAll, 1500);
  } catch (err) {
    showToast("error", `Restart failed: ${err.message}`);
  }
}

// Job Details Modal
async function showJobDetails(jobId) {
  try {
    const [job, auditEvents] = await Promise.all([
      api.get(`/api/jobs/${jobId}`),
      api.get(`/api/jobs/${jobId}/audit`),
    ]);

    document.getElementById("modal-job-id").textContent = job.id;
    document.getElementById("modal-job-type").textContent = job.job_type;
    document.getElementById("modal-job-status").innerHTML = getStatusBadge(job.status);
    document.getElementById("modal-job-priority").textContent = job.priority !== undefined ? job.priority : 0;
    document.getElementById("modal-job-attempts").textContent = job.attempt_count;
    document.getElementById("modal-job-max-retries").textContent = job.max_retries;
    document.getElementById("modal-job-release").textContent = job.release_version || "-";
    document.getElementById("modal-job-worker").textContent = job.leased_by || "None (Unassigned)";

    const leaseExpStr = job.lease_expires_at
      ? new Date(job.lease_expires_at * 1000).toLocaleString()
      : "None";
    document.getElementById("modal-job-lease-exp").textContent = leaseExpStr;

    const createdStr = job.created_at ? new Date(job.created_at * 1000).toLocaleString() : "-";
    const updatedStr = job.updated_at ? new Date(job.updated_at * 1000).toLocaleString() : "-";
    document.getElementById("modal-job-created").textContent = createdStr;
    document.getElementById("modal-job-updated").textContent = updatedStr;

    document.getElementById("modal-job-error").textContent = job.last_error || "None";
    document.getElementById("modal-job-payload").textContent = JSON.stringify(job.payload, null, 2);
    document.getElementById("modal-job-result").textContent = job.result ? JSON.stringify(job.result, null, 2) : "None (Pending/Uncompleted)";

    // Render audit timeline
    const timeline = document.getElementById("modal-job-timeline");
    if (auditEvents.length === 0) {
      timeline.innerHTML = `<div style="color: var(--text-muted);">No audit events recorded for this job.</div>`;
    } else {
      timeline.innerHTML = auditEvents.map((ev, i) => {
        const timeStr = new Date(ev.created_at * 1000).toLocaleTimeString();
        return `
          <div style="background: rgba(0,0,0,0.3); border-left: 3px solid var(--color-primary); padding: 8px 12px; margin-bottom: 6px; border-radius: 4px;">
            <div style="display: flex; justify-content: space-between; font-size: 11px;">
              <strong>${i + 1}. [${escapeHtml(ev.event_type)}]</strong>
              <span style="color: var(--text-muted);">${timeStr}</span>
            </div>
            <div style="font-size: 11px; color: var(--text-muted);">Actor: ${escapeHtml(ev.actor)}</div>
            <div class="cell-mono" style="font-size: 11px; margin-top: 4px; color: #c9d1d9;">${escapeHtml(JSON.stringify(ev.details))}</div>
          </div>
        `;
      }).join("");
    }

    document.getElementById("modal-job-details").classList.add("open");
  } catch (err) {
    showToast("error", `Failed to load job details: ${err.message}`);
  }
}

// Confirmation Dialog Modal
function confirmAction(title, message, onConfirm, confirmText = "Confirm") {
  const modal = document.getElementById("modal-confirm");
  document.getElementById("confirm-modal-title").textContent = title;
  document.getElementById("confirm-modal-message").textContent = message;

  const btnConfirm = document.getElementById("btn-confirm-action");
  btnConfirm.textContent = confirmText;

  // Replace button to remove existing click listeners
  const newBtn = btnConfirm.cloneNode(true);
  btnConfirm.parentNode.replaceChild(newBtn, btnConfirm);

  newBtn.addEventListener("click", () => {
    closeAllModals();
    onConfirm();
  });

  modal.classList.add("open");
}

function closeAllModals() {
  document.querySelectorAll(".modal-backdrop").forEach((m) => m.classList.remove("open"));
}

// Polling Loop
function startPolling() {
  // Main data poll (every 3 seconds)
  setInterval(() => {
    if (document.hidden) return;
    pollCountdown--;
    if (pollCountdown <= 0) {
      pollCountdown = 3;
      loadOverviewAndJobs();
      loadWorkers();
      loadReleases();
    }
    const cd = document.getElementById("refresh-timer");
    if (cd) cd.textContent = `${pollCountdown}s`;
  }, 1000);

  // Audit poll (every 5 seconds)
  setInterval(() => {
    if (!document.hidden) {
      loadAudit();
    }
  }, 5000);
}

// Helper: Status Badges
function getStatusBadge(status) {
  const map = {
    QUEUED: "badge-queued",
    RUNNING: "badge-running",
    COMPLETED: "badge-completed",
    FAILED: "badge-failed",
    DEAD_LETTER: "badge-dead-letter",
  };
  const cls = map[status] || "badge-idle";
  return `<span class="badge ${cls}">${status}</span>`;
}

// Toast Notifications
function showToast(type, message) {
  const container = document.getElementById("toast-container");
  if (!container) return;

  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span>${escapeHtml(message)}</span>`;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = "0";
    toast.style.transform = "translateY(10px)";
    toast.style.transition = "all 0.3s ease";
    setTimeout(() => toast.remove(), 300);
  }, 4000);
}

function escapeHtml(str) {
  if (str === null || str === undefined) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// Section: Release Impact Analysis Modal (R-07)
async function openReleaseImpactModal(version) {
  try {
    const impact = await api.get(`/api/releases/${encodeURIComponent(version)}/impact`);

    document.getElementById("impact-modal-version").textContent = impact.version;

    const badgeEl = document.getElementById("impact-health-badge");
    const health = impact.health || "HEALTHY";
    badgeEl.textContent = health;
    badgeEl.className = `badge badge-${health.toLowerCase()}`;

    // Summary Box
    const summaryBox = document.getElementById("impact-summary-box");
    summaryBox.className = `impact-summary-card health-${health}`;
    document.getElementById("impact-summary-text").textContent = impact.summary;

    // Metrics Cards
    document.getElementById("impact-metric-jobs").textContent = impact.jobs.total;
    document.getElementById("impact-metric-success-rate").textContent = `${impact.jobs.success_rate_percent}% Success (${impact.jobs.completed} done)`;

    document.getElementById("impact-metric-failures").textContent = impact.failures.total_failures;
    document.getElementById("impact-metric-deadletter").textContent = `${impact.failures.dead_letter_count} Dead-Letter (${impact.jobs.retried} retried)`;

    const crashes = impact.workers.crashes_detected;
    document.getElementById("impact-metric-workers").textContent = `${crashes} Crash${crashes === 1 ? "" : "es"}`;
    document.getElementById("impact-metric-restarts").textContent = `${impact.workers.restarts_observed} Restarts (${impact.workers.affected_workers.length} workers)`;

    const durSecs = Math.round(impact.deployment.active_duration_seconds || 0);
    const mins = Math.floor(durSecs / 60);
    const secs = durSecs % 60;
    const durStr = mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
    document.getElementById("impact-metric-duration").textContent = durStr;

    let statusSub = impact.is_active
      ? "● Currently Active"
      : (impact.rollback.was_rolled_back
          ? `↺ Rolled Back to ${impact.rollback.rolled_back_to}`
          : (impact.deployment.end_reason || "Concluded"));
    document.getElementById("impact-metric-status").textContent = statusSub;

    // Milestones Row
    const msContainer = document.getElementById("impact-milestones-row");
    const formatMs = (ts) => (ts ? new Date(ts * 1000).toLocaleTimeString() : "--");

    const msItems = [
      { label: "🚀 Deployed", time: impact.milestones.deployed_at, active: !!impact.milestones.deployed_at },
      { label: "⚡ 1st Job", time: impact.milestones.first_job_enqueued_at, active: !!impact.milestones.first_job_enqueued_at },
      { label: "⚠️ 1st Failure", time: impact.milestones.first_failure_at, active: !!impact.milestones.first_failure_at, warn: true },
      { label: "💀 1st Dead-Letter", time: impact.milestones.first_dead_letter_at, active: !!impact.milestones.first_dead_letter_at, danger: true },
      { label: "💥 1st Restart", time: impact.milestones.first_worker_restart_at, active: !!impact.milestones.first_worker_restart_at, warn: true },
      { label: "↺ Rolled Back", time: impact.milestones.rolled_back_at, active: !!impact.milestones.rolled_back_at, danger: true },
    ];

    msContainer.innerHTML = msItems.map((m) => {
      let colorStyle = "";
      if (m.danger && m.active) colorStyle = "border-color: var(--color-danger-bright); color: var(--color-danger-bright);";
      else if (m.warn && m.active) colorStyle = "border-color: var(--color-warning-bright); color: var(--color-warning-bright);";
      else if (m.active) colorStyle = "border-color: var(--color-primary);";

      return `
        <div class="impact-milestone-pill" style="${colorStyle}">
          <div class="impact-milestone-title">${m.label}</div>
          <div class="impact-milestone-time">${formatMs(m.time)}</div>
        </div>
      `;
    }).join("");

    // Failure Diagnostics Section
    const failSection = document.getElementById("impact-failure-section");
    const failTypesEl = document.getElementById("impact-failure-types");
    const sampleErrorsEl = document.getElementById("impact-sample-errors");

    if (impact.failures.total_failures > 0 || (impact.failures.sample_errors && impact.failures.sample_errors.length > 0)) {
      failSection.style.display = "block";

      const types = Object.entries(impact.failures.failure_types || {});
      failTypesEl.innerHTML = types.length > 0
        ? types.map(([t, cnt]) => `<span class="badge badge-failed" style="font-size: 11px;">${escapeHtml(t)} (${cnt})</span>`).join("")
        : `<span style="font-size: 11px; color: var(--text-muted);">No distinct error categories</span>`;

      sampleErrorsEl.innerHTML = (impact.failures.sample_errors || []).map((err) => `
        <div style="background: rgba(0,0,0,0.3); border: 1px solid var(--border-color); border-radius: var(--radius-sm); padding: 8px 10px; font-size: 11px; display: flex; justify-content: space-between; align-items: center;">
          <div>
            <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 2px;">
              <strong class="cell-mono">${escapeHtml(err.job_id.substring(0, 12))}...</strong>
              <span class="badge ${err.is_dead_letter ? 'badge-dead-letter' : 'badge-failed'}">${err.is_dead_letter ? 'DEAD_LETTER' : 'FAILED'}</span>
              <span style="color: var(--text-muted);">${escapeHtml(err.job_type)}</span>
            </div>
            <div style="color: var(--color-danger-bright); font-family: var(--font-mono);">${escapeHtml(err.error)}</div>
          </div>
          <button class="btn btn-sm" style="font-size: 10px; padding: 2px 6px;" onclick="openJobDetails('${err.job_id}')">Inspect</button>
        </div>
      `).join("");
    } else {
      failSection.style.display = "none";
    }

    // Timeline list
    const timelineEl = document.getElementById("impact-timeline-list");
    if (!impact.timeline || impact.timeline.length === 0) {
      timelineEl.innerHTML = `<div style="color: var(--text-muted); font-size: 12px; padding: 10px;">No events recorded in this release window.</div>`;
    } else {
      timelineEl.innerHTML = impact.timeline.map((ev) => {
        const timeStr = new Date(ev.timestamp * 1000).toLocaleTimeString();
        const sevClass = `sev-${ev.severity}`;
        const jobTag = ev.job_id
          ? `<span class="cell-mono" style="color: var(--color-primary-hover); cursor: pointer;" onclick="openJobDetails('${ev.job_id}')">job:${escapeHtml(ev.job_id.substring(0, 8))}</span>`
          : "";

        return `
          <div class="impact-timeline-item ${sevClass}">
            <div style="display: flex; justify-content: space-between; font-size: 11px; color: var(--text-muted); margin-bottom: 2px;">
              <span class="cell-mono">${timeStr}</span>
              <span><strong>${escapeHtml(ev.actor)}</strong> ${jobTag}</span>
            </div>
            <div style="display: flex; align-items: center; gap: 8px;">
              <span class="badge badge-${ev.event_type.toLowerCase()}" style="font-size: 10px; padding: 1px 6px;">${ev.event_type}</span>
              <span style="color: var(--text-main); font-size: 12px;">${escapeHtml(ev.description)}</span>
            </div>
          </div>
        `;
      }).join("");
    }

    document.getElementById("modal-release-impact")?.classList.add("open");
  } catch (err) {
    showToast("error", `Failed to load release impact: ${err.message}`);
  }
}

