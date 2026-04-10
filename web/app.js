const state = {
  view: "overview",
  chartView: "trend",
  datasetTabs: {
    clean: "orders",
  },
  recordOrderId: "",
  errorSearch: "",
  selectedBatchId: "",
  selectedBatchErrors: [],
  batchErrorsLoading: false,
  batchErrorsError: "",
  payload: null,
};

async function fetchState() {
  const response = await fetch("/api/state");
  state.payload = await response.json();
  if (!state.recordOrderId) {
    state.recordOrderId = findFirstTrackedOrderId() || "";
  }
  const history = getAllBatchHistory();
  const hasSelectedBatch = history.some(
    (batch) => (batch.batch_id || batch.cycle_id) === state.selectedBatchId
  );
  if (!hasSelectedBatch) {
    state.selectedBatchId = history[0]?.batch_id || history[0]?.cycle_id || "";
  }
  if (state.selectedBatchId) {
    await fetchBatchErrors(state.selectedBatchId, false);
  } else {
    state.selectedBatchErrors = [];
    state.batchErrorsError = "";
  }
  render();
}

async function postAction(payload) {
  await fetch("/api/action", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  await fetchState();
}

async function fetchBatchErrors(batchId, shouldRender = true) {
  if (!batchId) {
    state.selectedBatchErrors = [];
    state.batchErrorsError = "";
    if (shouldRender) render();
    return;
  }

  state.batchErrorsLoading = true;
  state.batchErrorsError = "";
  if (shouldRender) render();

  try {
    const response = await fetch(`/api/batch/${encodeURIComponent(batchId)}/errors`);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Unable to load batch errors.");
    }
    if (state.selectedBatchId === batchId) {
      state.selectedBatchErrors = payload.detailed_logs || [];
      state.batchErrorsError = "";
    }
  } catch (error) {
    if (state.selectedBatchId === batchId) {
      state.selectedBatchErrors = [];
      state.batchErrorsError = error.message;
    }
  } finally {
    if (state.selectedBatchId === batchId) {
      state.batchErrorsLoading = false;
    }
    if (shouldRender) render();
  }
}

function setText(id, value) {
  const element = document.getElementById(id);
  if (element) element.textContent = value;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function formatStatus(cycle) {
  if (!cycle) return "Waiting for first cycle";
  if (cycle.status === "healthy") return "Healthy batch";
  if (cycle.status === "degraded") return "Self-healed";
  return "Human attention required";
}

function getCurrentBatchLabel(cycle) {
  return cycle ? cycle.batch_id || cycle.cycle_id : "Waiting for batch";
}

function getDatasetRows(stage, datasetName) {
  return state.payload?.current_cycle?.datasets?.[stage]?.[datasetName] || [];
}

function getFilterValue(id) {
  return document.getElementById(id)?.value?.trim().toLowerCase() || "";
}

function filterRows(rows, filterValue) {
  if (!filterValue) return rows;
  return rows.filter((row) =>
    Object.values(row).some((value) => String(value ?? "").toLowerCase().includes(filterValue))
  );
}

function getAllLogs() {
  return (state.payload?.history || []).flatMap((batch) =>
    (batch.detailed_logs || []).map((item) => ({
      ...item,
      batch_id: batch.batch_id || batch.cycle_id,
      batch_status: batch.status,
      batch_started_at: batch.started_at,
    }))
  );
}

function getAllBatchHistory() {
  return state.payload?.history || [];
}

function getBatchErrorRowTone(status) {
  if (status === "resolved") return "rgba(117, 245, 200, 0.12)";
  if (status === "approved" || status === "manually_fixed") return "rgba(255, 209, 102, 0.16)";
  if (status === "escalated") return "rgba(255, 122, 144, 0.14)";
  return "rgba(255, 255, 255, 0.025)";
}

function mean(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function percentage(part, total) {
  if (!total) return 0;
  return (part / total) * 100;
}

function renderSummary(cycle) {
  const host = document.getElementById("summary-grid");
  host.innerHTML = "";
  if (!cycle) {
    host.innerHTML = `<div class="summary-item"><span>Status</span><strong>No batch yet</strong></div>`;
    return;
  }
  [
    ["Batch Size", cycle.batch_size],
    ["Failures Injected", cycle.summary.injected],
    ["Issues Detected", cycle.summary.detected],
    ["Escalations", cycle.summary.escalated],
  ].forEach(([label, value]) => {
    const div = document.createElement("div");
    div.className = "summary-item";
    div.innerHTML = `<span>${label}</span><strong>${value}</strong>`;
    host.appendChild(div);
  });
}

function renderRatios() {
  const host = document.getElementById("ratio-grid");
  host.innerHTML = "";
  const logs = getAllLogs();
  const history = getAllBatchHistory();
  const autoResolved = logs.filter((item) => item.status === "resolved").length;
  const humanClosed = logs.filter((item) => ["approved", "manually_fixed"].includes(item.status)).length;
  const escalatedOpen = logs.filter((item) => item.status === "escalated").length;
  const healthyBatches = history.filter((batch) => batch.status === "healthy").length;
  const metrics = [
    ["MTTD", `${mean(logs.map((item) => item.detection_latency_sec || 0)).toFixed(1)}s`],
    ["MTTR", `${mean(logs.map((item) => item.resolution_latency_sec || 0)).toFixed(1)}s`],
    ["Auto-Heal Ratio", `${percentage(autoResolved, logs.length).toFixed(0)}%`],
    ["Human Closure Ratio", `${percentage(humanClosed, logs.length).toFixed(0)}%`],
    ["Open Escalation Ratio", `${percentage(escalatedOpen, logs.length).toFixed(0)}%`],
    ["Batch Health Ratio", `${percentage(healthyBatches, history.length).toFixed(0)}%`],
  ];
  metrics.forEach(([label, value]) => {
    const div = document.createElement("div");
    div.className = "summary-item ratio-item";
    div.innerHTML = `<span>${label}</span><strong>${value}</strong>`;
    host.appendChild(div);
  });
}

function renderList(id, items, formatter) {
  const host = document.getElementById(id);
  host.innerHTML = "";
  if (!items || !items.length) {
    host.innerHTML = "<li>No items yet.</li>";
    return;
  }
  items.forEach((item) => {
    const li = document.createElement("li");
    li.innerHTML = formatter(item);
    host.appendChild(li);
  });
}

function getEventFeedTone(item) {
  const stage = String(item?.stage ?? "").toLowerCase();
  const message = String(item?.message ?? "").toLowerCase();

  if (stage.includes("approval") || stage.includes("review") || message.includes("human approval") || message.includes("manual fix")) {
    return "warning";
  }
  if (stage.includes("escalation") || message.includes("escalat") || message.includes("rejected")) {
    return "danger";
  }
  if (stage.includes("healing") || message.includes("resolved") || message.includes("restored")) {
    return "success";
  }
  return "neutral";
}

function renderDetailedLogGroups() {
  const host = document.getElementById("detailed-log-list");
  host.innerHTML = "";

  const history = getAllBatchHistory();
  if (!history.length) {
    host.innerHTML = "<li>No items yet.</li>";
    return;
  }

  history.forEach((batch) => {
    const batchId = batch.batch_id || batch.cycle_id || "-";
    const logs = batch.detailed_logs || [];
    const item = document.createElement("li");
    item.className = "batch-log-group";
    item.innerHTML = `
      <div class="batch-log-header">
        <div>
          <strong>${escapeHtml(batchId)}</strong>
          <div class="batch-log-subtitle">${escapeHtml(formatStatus(batch))}</div>
        </div>
        <div class="batch-log-count">${escapeHtml(logs.length)} errors</div>
      </div>
      <div class="batch-log-entries">
        ${
          logs.length
            ? logs
                .map(
                  (log) => `
                    <article class="batch-log-entry">
                      <div class="event-meta"><span>${escapeHtml(log.error_id)}</span><span>${escapeHtml(log.resolver)}</span></div>
                      <div><strong>${escapeHtml(log.dataset)}</strong> / ${escapeHtml(log.error_type)}</div>
                      <div>${escapeHtml(log.finding)}</div>
                      <div>Dirty: ${escapeHtml(log.dirty_value ?? "-")}</div>
                      <div>Original: ${escapeHtml(log.original_value ?? "-")}</div>
                      <div>Action: ${escapeHtml(log.resolution_action)}</div>
                      <div>Resolved: ${escapeHtml(log.resolved_value ?? "-")}</div>
                      <div>Agent Route: ${escapeHtml(log.agent_route ?? "-")}</div>
                      <div>MTTD: ${escapeHtml(log.detection_latency_sec)}s | MTTR: ${escapeHtml(log.resolution_latency_sec)}s</div>
                      <div>Status: ${escapeHtml(log.status)}</div>
                    </article>
                  `
                )
                .join("")
            : "<div class='batch-log-empty'>No detailed logs in this batch.</div>"
        }
      </div>
    `;
    host.appendChild(item);
  });
}

function renderDatasetTable(stage, target) {
  const datasetName = state.datasetTabs[target];
  const filterValue = getFilterValue(`${target}-filter`);
  const rows = filterRows(getDatasetRows(stage, datasetName), filterValue);
  const head = document.getElementById(`${target}-table-head`);
  const body = document.getElementById(`${target}-table-body`);
  head.innerHTML = "";
  body.innerHTML = "";

  if (!rows.length) {
    body.innerHTML = "<tr><td>No matching rows.</td></tr>";
    return;
  }

  const columns = Array.from(
    rows.reduce((set, row) => {
      Object.keys(row).forEach((key) => set.add(key));
      return set;
    }, new Set())
  );

  const tr = document.createElement("tr");
  tr.innerHTML = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  head.appendChild(tr);

  rows.slice(0, 60).forEach((row) => {
    const item = document.createElement("tr");
    item.innerHTML = columns.map((column) => `<td>${escapeHtml(row[column] ?? "")}</td>`).join("");
    body.appendChild(item);
  });
}

function stringifyComparisonValue(value) {
  if (value === null || value === undefined || value === "") return "";
  return String(value);
}

function getComparisonRows() {
  const cycle = state.payload?.current_cycle;
  if (!cycle) return [];

  const datasetName = state.datasetTabs.clean;
  const cleanRows = cycle.datasets.clean?.[datasetName] || [];
  const dirtyRows = cycle.datasets.dirty?.[datasetName] || [];
  const healedRows = cycle.datasets.healed?.[datasetName] || [];
  const rowCount = Math.max(cleanRows.length, dirtyRows.length, healedRows.length);
  const output = [];

  for (let index = 0; index < rowCount; index += 1) {
    const cleanRow = cleanRows[index] || {};
    const dirtyRow = dirtyRows[index] || {};
    const healedRow = healedRows[index] || {};
    const columns = Array.from(
      new Set([...Object.keys(cleanRow), ...Object.keys(dirtyRow), ...Object.keys(healedRow)])
    );
    const recordId =
      cleanRow.order_id || dirtyRow.order_id || healedRow.order_id || `${datasetName}-row-${index + 1}`;

    columns.forEach((field) => {
      const cleanValue = stringifyComparisonValue(cleanRow[field]);
      const dirtyValue = stringifyComparisonValue(dirtyRow[field]);
      const healedValue = stringifyComparisonValue(healedRow[field]);
      const dirtyChanged = dirtyValue !== cleanValue;
      const healedChanged = healedValue !== cleanValue || healedValue !== dirtyValue;

      if (!dirtyChanged && !healedChanged) return;

      output.push({
        record_id: recordId,
        field,
        clean_value: cleanValue || "-",
        dirty_value: dirtyValue || "-",
        healed_value: healedValue || "-",
        dirty_changed: dirtyChanged,
        healed_changed: healedChanged,
      });
    });
  }

  return filterRows(output, getFilterValue("clean-filter"));
}

function renderComparisonTable() {
  const head = document.getElementById("comparison-table-head");
  const body = document.getElementById("comparison-table-body");
  head.innerHTML = "";
  body.innerHTML = "";

  const rows = getComparisonRows();
  if (!rows.length) {
    body.innerHTML = "<tr><td>No side-by-side differences in the current dataset.</td></tr>";
    return;
  }

  const columns = [
    "record_id",
    "field",
    "clean_value",
    "dirty_value",
    "healed_value",
  ];

  const tr = document.createElement("tr");
  tr.innerHTML = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  head.appendChild(tr);

  rows.forEach((row) => {
    const item = document.createElement("tr");
    item.innerHTML = `
      <td>${escapeHtml(row.record_id)}</td>
      <td>${escapeHtml(row.field)}</td>
      <td>${escapeHtml(row.clean_value)}</td>
      <td class="${row.dirty_changed ? "compare-changed compare-dirty" : ""}">${escapeHtml(row.dirty_value)}</td>
      <td class="${row.healed_changed ? "compare-changed compare-healed" : ""}">${escapeHtml(row.healed_value)}</td>
    `;
    body.appendChild(item);
  });
}

function downloadComparisonCsv() {
  const rows = getComparisonRows();
  if (!rows.length) return;

  const columns = [
    "record_id",
    "field",
    "clean_value",
    "dirty_value",
    "healed_value",
    "dirty_changed",
    "healed_changed",
  ];
  const escapeCsv = (value) => `"${String(value ?? "").replaceAll("\"", "\"\"")}"`;
  const lines = [
    columns.join(","),
    ...rows.map((row) => columns.map((column) => escapeCsv(row[column])).join(",")),
  ];
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  const datasetName = state.datasetTabs.clean;
  link.href = url;
  link.download = `${datasetName}-comparison.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function getResolvedIssueRows() {
  const cycle = state.payload?.current_cycle;
  if (!cycle) return [];
  return (cycle.detailed_logs || [])
    .filter((item) => ["resolved", "approved", "manually_fixed"].includes(item.status))
    .map((item) => ({
      error_id: item.error_id,
      order_id: item.order_id || item.original_value || "-",
      dataset: item.dataset,
      error_type: item.error_type,
      field: item.column || "-",
      corrupted_value: item.dirty_value ?? "-",
      resolved_value: item.resolved_value ?? "-",
      status: item.status,
      resolver: item.resolver,
      action: item.resolution_action,
      rollback_action: item.error_id,
    }));
}

function renderResolvedIssuesTable() {
  const rows = filterRows(getResolvedIssueRows(), getFilterValue("resolved-filter"));
  const head = document.getElementById("resolved-table-head");
  const body = document.getElementById("resolved-table-body");
  head.innerHTML = "";
  body.innerHTML = "";

  if (!rows.length) {
    body.innerHTML = "<tr><td>No resolved records in this batch.</td></tr>";
    return;
  }

  const columns = [
    "error_id",
    "order_id",
    "dataset",
    "error_type",
    "field",
    "corrupted_value",
    "resolved_value",
    "status",
    "resolver",
    "action",
    "rollback_action",
  ];

  const tr = document.createElement("tr");
  tr.innerHTML = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  head.appendChild(tr);

  rows.forEach((row) => {
    const item = document.createElement("tr");
    item.innerHTML = columns
      .map((column) =>
        column === "rollback_action"
          ? `<td><button class="ghost rollback-error-btn" data-error-id="${escapeHtml(row.error_id)}">Undo</button></td>`
          : `<td>${escapeHtml(row[column] ?? "")}</td>`
      )
      .join("");
    body.appendChild(item);
  });

  document.querySelectorAll(".rollback-error-btn").forEach((button) => {
    button.addEventListener("click", () => {
      if (!window.confirm("Undo this resolved issue and reopen it for another healing attempt?")) return;
      postAction({ action: "rollback-error", error_id: button.dataset.errorId });
    });
  });
}

function findFirstTrackedOrderId() {
  const logs = state.payload?.current_cycle?.detailed_logs || [];
  const firstLog = logs.find((item) => item.order_id);
  return firstLog?.order_id || state.payload?.current_cycle?.datasets?.clean?.orders?.[0]?.order_id;
}

function getRecordSnapshot(orderId) {
  const cycle = state.payload?.current_cycle;
  if (!cycle || !orderId) return null;
  const findById = (rows) => rows.find((row) => row.order_id === orderId);
  return {
    cleanOrder: findById(cycle.datasets.clean.orders),
    cleanPayment: findById(cycle.datasets.clean.payments),
    cleanDelivery: findById(cycle.datasets.clean.delivery),
    dirtyOrder: findById(cycle.datasets.dirty.orders),
    dirtyPayment: findById(cycle.datasets.dirty.payments),
    dirtyDelivery: findById(cycle.datasets.dirty.delivery),
    healedOrder: findById(cycle.datasets.healed.orders),
    healedPayment: findById(cycle.datasets.healed.payments),
    healedDelivery: findById(cycle.datasets.healed.delivery),
  };
}

function renderRecordTrail() {
  const cycle = state.payload?.current_cycle;
  const summary = document.getElementById("record-summary");
  const host = document.getElementById("record-trail-list");
  const suggestions = document.getElementById("record-suggestions");
  host.innerHTML = "";
  suggestions.innerHTML = "";

  if (!cycle) {
    summary.innerHTML = "No batch available yet.";
    return;
  }

  const orderIds = cycle.datasets.clean.orders.slice(0, 20).map((row) => row.order_id);
  orderIds.forEach((orderId) => {
    const button = document.createElement("button");
    button.className = "suggestion-chip";
    button.textContent = orderId;
    button.addEventListener("click", () => {
      state.recordOrderId = orderId;
      document.getElementById("record-filter").value = orderId;
      renderRecordTrail();
    });
    suggestions.appendChild(button);
  });

  const snapshot = getRecordSnapshot(state.recordOrderId);
  if (!snapshot) {
    summary.innerHTML = "No record selected.";
    return;
  }

  summary.innerHTML = `
    <div class="summary-item"><span>Tracked order_id</span><strong>${escapeHtml(state.recordOrderId)}</strong></div>
    <div class="summary-item"><span>Clean order</span><strong>${snapshot.cleanOrder ? "Available" : "Missing"}</strong></div>
    <div class="summary-item"><span>Dirty order</span><strong>${snapshot.dirtyOrder ? "Present" : "Not matched"}</strong></div>
    <div class="summary-item"><span>Healed order</span><strong>${snapshot.healedOrder ? "Present" : "Pending"}</strong></div>
  `;

  const logs = getAllLogs().filter(
    (item) => item.order_id === state.recordOrderId || item.original_value === state.recordOrderId
  );
  if (!logs.length) {
    host.innerHTML = "<div class='history-card'>No error trail for this record in recent batches.</div>";
    return;
  }

  logs.forEach((log) => {
    const card = document.createElement("div");
    card.className = "history-card";
    card.innerHTML = `
      <strong>${escapeHtml(log.error_id)}</strong>
      <div>Batch: ${escapeHtml(log.batch_id ?? "-")}</div>
      <div>${escapeHtml(log.dataset)} / ${escapeHtml(log.error_type)}</div>
      <div>Finding: ${escapeHtml(log.finding)}</div>
      <div>Original: ${escapeHtml(log.original_value ?? "-")}</div>
      <div>Dirty: ${escapeHtml(log.dirty_value ?? "-")}</div>
      <div>Resolved: ${escapeHtml(log.resolved_value ?? "-")}</div>
      <div>Action: ${escapeHtml(log.resolution_action)}</div>
      <div>Agent Route: ${escapeHtml(log.agent_route ?? "-")}</div>
      <div>Status: ${escapeHtml(log.status)}</div>
    `;
    host.appendChild(card);
  });
}

function renderApprovals() {
  const host = document.getElementById("approval-cards");
  host.innerHTML = "";
  const approvals = state.payload?.all_escalations || [];
  if (!approvals.length) {
    host.innerHTML = "<div class='history-card'>No escalations have been recorded yet.</div>";
    return;
  }

  approvals.forEach((approval) => {
    const card = document.createElement("div");
    card.className = "history-card";
    const pending = approval.status === "pending";
    const manualId = `manual-${approval.error_id}`;
    card.innerHTML = `
      <strong>${escapeHtml(approval.error_id)}</strong>
      <div>Batch: ${escapeHtml(approval.batch_id ?? approval.cycle_id ?? "-")}</div>
      <div>${escapeHtml(approval.dataset)} / row ${escapeHtml(approval.row_index ?? "-")}</div>
      <div>Current: ${escapeHtml(approval.current_value ?? "-")}</div>
      <div>Proposed: ${escapeHtml(approval.proposed_value ?? "-")}</div>
      <div>Reason: ${escapeHtml(approval.reason)}</div>
      <div>Status: ${escapeHtml(approval.status)}</div>
      ${pending ? `
        <div class="approval-actions">
          <button class="approve-btn" data-error-id="${escapeHtml(approval.error_id)}">Approve</button>
          <button class="reject-btn" data-error-id="${escapeHtml(approval.error_id)}">Reject</button>
        </div>
        <div class="manual-fix-row">
          <input id="${escapeHtml(manualId)}" class="filter-input manual-input" placeholder="Enter manual fix value" />
          <button class="manual-fix-btn" data-error-id="${escapeHtml(approval.error_id)}" data-input-id="${escapeHtml(manualId)}">Apply Manual Fix</button>
        </div>
      ` : ""}
    `;
    host.appendChild(card);
  });

  document.querySelectorAll(".approve-btn").forEach((button) => {
    button.addEventListener("click", () => {
      postAction({ action: "approve-escalation", error_id: button.dataset.errorId });
    });
  });
  document.querySelectorAll(".reject-btn").forEach((button) => {
    button.addEventListener("click", () => {
      postAction({ action: "reject-escalation", error_id: button.dataset.errorId });
    });
  });
  document.querySelectorAll(".manual-fix-btn").forEach((button) => {
    button.addEventListener("click", () => {
      const input = document.getElementById(button.dataset.inputId);
      const value = input?.value?.trim();
      if (!value) return;
      postAction({
        action: "manual-fix-escalation",
        error_id: button.dataset.errorId,
        manual_value: value,
      });
    });
  });
}

function renderHistory(history) {
  const existing = document.getElementById("history-cards");
  existing.innerHTML = "";
  if (!history || !history.length) {
    existing.innerHTML = "<div class='history-card'>No completed batches yet.</div>";
    return;
  }
  history.slice(0, 6).forEach((cycle) => {
    const card = document.createElement("div");
    card.className = "history-card";
    card.innerHTML = `
      <strong>${escapeHtml(cycle.batch_id || cycle.cycle_id)}</strong>
      <div>${escapeHtml(cycle.started_at)}</div>
      <div>Status: ${escapeHtml(formatStatus(cycle))}</div>
      <div>Detected: ${cycle.summary.detected} | Resolved: ${cycle.summary.resolved} | Escalated: ${cycle.summary.escalated}</div>
    `;
    existing.appendChild(card);
  });
}

function renderBatchHistoryPanel() {
  const listHost = document.getElementById("batch-history-list");
  const title = document.getElementById("batch-error-title");
  const head = document.getElementById("batch-errors-table-head");
  const body = document.getElementById("batch-errors-table-body");
  const history = getAllBatchHistory();

  listHost.innerHTML = "";
  head.innerHTML = "";
  body.innerHTML = "";
  title.textContent = state.selectedBatchId ? `Batch Errors: ${state.selectedBatchId}` : "Batch Errors";

  if (!history.length) {
    listHost.innerHTML = "<div class='history-card'>No completed batches yet.</div>";
    body.innerHTML = "<tr><td>No batch selected.</td></tr>";
    return;
  }

  history.forEach((batch) => {
    const batchId = batch.batch_id || batch.cycle_id;
    const button = document.createElement("button");
    button.className = state.selectedBatchId === batchId ? "tab active" : "tab";
    button.textContent = batchId;
    button.style.width = "100%";
    button.style.textAlign = "left";
    button.addEventListener("click", async () => {
      if (state.selectedBatchId === batchId) return;
      state.selectedBatchId = batchId;
      state.selectedBatchErrors = [];
      await fetchBatchErrors(batchId);
    });
    listHost.appendChild(button);
  });

  const columns = [
    "error_id",
    "error_type",
    "dataset",
    "severity",
    "status",
    "resolution_action",
    "resolver",
  ];
  const tr = document.createElement("tr");
  tr.innerHTML = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  head.appendChild(tr);

  if (state.batchErrorsLoading) {
    body.innerHTML = "<tr><td>Loading batch errors...</td></tr>";
    return;
  }

  if (state.batchErrorsError) {
    body.innerHTML = `<tr><td>${escapeHtml(state.batchErrorsError)}</td></tr>`;
    return;
  }

  if (!state.selectedBatchErrors.length) {
    body.innerHTML = "<tr><td>No detailed errors found for this batch.</td></tr>";
    return;
  }

  state.selectedBatchErrors.forEach((row) => {
    const item = document.createElement("tr");
    item.style.background = getBatchErrorRowTone(row.status);
    item.innerHTML = columns
      .map((column) => `<td>${escapeHtml(row[column] ?? "-")}</td>`)
      .join("");
    body.appendChild(item);
  });
}

function renderBarChart(hostId, data, { color = "#5fe1ff", valueFormatter = (value) => value } = {}) {
  const host = document.getElementById(hostId);
  host.innerHTML = "";
  if (!data.length) {
    host.innerHTML = "<div class='chart-empty'>No chart data yet.</div>";
    return;
  }
  const max = Math.max(...data.map((item) => item.value), 1);
  data.forEach((item) => {
    const row = document.createElement("div");
    row.className = "bar-row";
    const width = Math.max(6, (item.value / max) * 100);
    row.innerHTML = `
      <div class="bar-label">${escapeHtml(item.label)}</div>
      <div class="bar-track" title="${escapeHtml(item.label)}: ${escapeHtml(valueFormatter(item.value))}">
        <div class="bar-fill" style="width:${width}%; background:${color}"></div>
      </div>
      <div class="bar-value">${escapeHtml(valueFormatter(item.value))}</div>
    `;
    host.appendChild(row);
  });
}

function renderTrendChart() {
  const history = [...getAllBatchHistory()].reverse();
  renderBarChart(
    "trend-chart",
    history.map((batch) => ({
      label: batch.batch_id || batch.cycle_id,
      value: batch.summary.detected,
    })),
    { color: "linear-gradient(135deg, #5fe1ff, #75f5c8)" }
  );
}

function renderDistributionChart() {
  const logs = getAllLogs();
  const counts = {};
  logs.forEach((item) => {
    counts[item.error_type] = (counts[item.error_type] || 0) + 1;
  });
  const data = Object.entries(counts)
    .map(([label, value]) => ({ label, value }))
    .sort((a, b) => b.value - a.value);
  renderBarChart("distribution-chart", data, { color: "#ffd166" });
}

function renderAnalyticsPage() {
  const analytics = state.payload?.analytics;
  if (!analytics) return;

  const cardsHost = document.getElementById("analytics-card-grid");
  cardsHost.innerHTML = "";
  [
    ["LLM Calls", analytics.agent_metrics.llm_calls_total],
    ["Local Rule Resolutions", analytics.agent_metrics.local_rule_resolved_total],
    ["Human Approval Requests", analytics.human_metrics.human_approval_requests_total],
    ["Human Full Resolution Requests", analytics.human_metrics.human_resolution_requests_total],
    ["MTTR", `${analytics.performance_metrics.mttr_seconds}s`],
    ["Crash-Free Uptime", `${analytics.system.crash_free_uptime_seconds}s`],
    ["Batch Success Rate", `${analytics.system.batch_success_rate_pct}%`],
    ["Open Human Queue", analytics.human_metrics.open_human_queue_total],
  ].forEach(([label, value]) => {
    const card = document.createElement("div");
    card.className = "summary-item";
    card.innerHTML = `<span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong>`;
    cardsHost.appendChild(card);
  });

  renderBarChart(
    "analytics-route-chart",
    Object.entries(analytics.recent_breakdowns.routes || {}).map(([label, value]) => ({ label, value })),
    { color: "#5fe1ff" }
  );
  renderBarChart(
    "analytics-human-chart",
    [
      { label: "approval requests", value: analytics.human_metrics.human_approval_requests_total },
      { label: "full human resolve", value: analytics.human_metrics.human_resolution_requests_total },
      { label: "approved", value: analytics.human_metrics.human_approved_total },
      { label: "rejected", value: analytics.human_metrics.human_rejected_total },
      { label: "manual fixes", value: analytics.human_metrics.manual_fix_total },
      { label: "open queue", value: analytics.human_metrics.open_human_queue_total },
    ],
    { color: "#ffd166" }
  );
  renderBarChart(
    "analytics-error-chart",
    Object.entries(analytics.recent_breakdowns.error_types || {}).map(([label, value]) => ({ label, value })),
    { color: "#75f5c8" }
  );
  renderBarChart(
    "analytics-health-chart",
    [
      { label: "successful batches", value: analytics.system.successful_batches_total },
      { label: "failed batches", value: analytics.system.failed_batches_total },
      { label: "crash count", value: analytics.system.crash_count },
      { label: "issues escalated", value: analytics.performance_metrics.issues_escalated_total },
    ],
    { color: "#ff7a90" }
  );

  const preview = document.getElementById("analytics-report-preview");
  preview.innerHTML = `
    <div class="history-card">
      <strong>Latest Report</strong>
      <div>Generated at: ${escapeHtml(analytics.generated_at)}</div>
      <div>OpenRouter enabled: ${escapeHtml(analytics.agent_metrics.openrouter_enabled ? "yes" : "no")}</div>
      <div>OpenRouter model: ${escapeHtml(analytics.agent_metrics.openrouter_model)}</div>
      <div>Report downloads: ${escapeHtml(analytics.downloads.report_download_total)}</div>
      <div>JSON: ${escapeHtml(analytics.downloads.json_path)}</div>
      <div>Markdown: ${escapeHtml(analytics.downloads.markdown_path)}</div>
      <div>Recent batches in report: ${escapeHtml(analytics.recent_batches.length)}</div>
      <div>Open escalations in report: ${escapeHtml(analytics.open_escalations.length)}</div>
    </div>
  `;
}

function findErrorMatch() {
  const term = state.errorSearch.trim().toLowerCase();
  if (!term) return null;
  return getAllLogs().find((item) =>
    [
      item.error_id,
      item.batch_id,
      item.error_type,
      item.dataset,
      item.status,
      item.finding,
    ]
      .filter(Boolean)
      .some((value) => String(value).toLowerCase().includes(term))
  );
}

function renderErrorSearch() {
  const host = document.getElementById("error-search-result");
  host.innerHTML = "";
  const match = findErrorMatch();
  if (!state.errorSearch.trim()) {
    host.innerHTML = "<div class='history-card'>Search by error id, type, dataset, or status.</div>";
    return;
  }
  if (!match) {
    host.innerHTML = "<div class='history-card'>No matching error found in recent batches.</div>";
    return;
  }
  host.innerHTML = `
    <div class="history-card print-card" id="printable-error-card">
      <strong>${escapeHtml(match.error_id)}</strong>
      <div>Batch: ${escapeHtml(match.batch_id)}</div>
      <div>Dataset: ${escapeHtml(match.dataset)}</div>
      <div>Error Type: ${escapeHtml(match.error_type)}</div>
      <div>Status: ${escapeHtml(match.status)}</div>
      <div>Severity: ${escapeHtml(match.severity)}</div>
      <div>Detected At: ${escapeHtml(match.detected_at)}</div>
      <div>Detection Latency: ${escapeHtml(match.detection_latency_sec)}s</div>
      <div>Resolution Latency: ${escapeHtml(match.resolution_latency_sec)}s</div>
      <div>Finding: ${escapeHtml(match.finding)}</div>
      <div>Original Value: ${escapeHtml(match.original_value ?? "-")}</div>
      <div>Dirty Value: ${escapeHtml(match.dirty_value ?? "-")}</div>
      <div>Resolved Value: ${escapeHtml(match.resolved_value ?? "-")}</div>
      <div>Action: ${escapeHtml(match.resolution_action)}</div>
      <div>Agent Route: ${escapeHtml(match.agent_route ?? "-")}</div>
      <div>Agent Reason: ${escapeHtml(match.agent_reason ?? "-")}</div>
      <div>Order ID: ${escapeHtml(match.order_id ?? "-")}</div>
    </div>
  `;
}

function printCurrentErrorCard() {
  const match = findErrorMatch();
  if (!match) return;
  const content = document.getElementById("printable-error-card")?.outerHTML;
  if (!content) return;
  const printWindow = window.open("", "_blank", "width=900,height=1200");
  printWindow.document.write(`
    <html>
      <head>
        <title>${escapeHtml(match.error_id)} PDF</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 32px; color: #111; }
          .print-card { border: 1px solid #ddd; border-radius: 14px; padding: 24px; }
          .print-card strong { display:block; font-size: 24px; margin-bottom: 16px; }
          .print-card div { margin: 8px 0; line-height: 1.4; }
        </style>
      </head>
      <body>${content}</body>
    </html>
  `);
  printWindow.document.close();
  printWindow.focus();
  printWindow.print();
}

function renderOverview() {
  const payload = state.payload;
  const cycle = payload.current_cycle;
  renderSummary(cycle);
  renderRatios();
  renderTrendChart();
  renderDistributionChart();
  renderErrorSearch();
  renderList("injected-list", cycle?.injected_errors, (item) => `
    <div class="event-meta"><span>${escapeHtml(item.error_id)}</span><span>${escapeHtml(item.severity)}</span></div>
    <div>${escapeHtml(item.dataset)}: ${escapeHtml(item.message)}</div>
  `);
  renderList("audit-list", cycle?.detailed_logs, (item) => `
    <div class="event-meta"><span>${escapeHtml(item.error_id)}</span><span>${escapeHtml(item.status)}</span></div>
    <div>${escapeHtml(item.resolution_action)}</div>
  `);
  renderList("event-feed", payload.event_feed, (item) => `
    <div class="event-feed-item event-feed-item--${escapeHtml(getEventFeedTone(item))}">
      <div class="event-meta"><span>${escapeHtml(item.stage)}</span><span>${escapeHtml(item.time)}</span></div>
      <div>${escapeHtml(item.message)}</div>
    </div>
  `);
  renderDetailedLogGroups();
  renderHistory(payload.history);
}

function applyView() {
  document.querySelectorAll(".view").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.viewPanel === state.view);
  });
  document.querySelectorAll(".nav-link").forEach((button) => {
    button.classList.toggle("active", button.dataset.view === state.view);
  });
  document.querySelectorAll(".chart-toggle").forEach((button) => {
    button.classList.toggle("active", button.dataset.chartView === state.chartView);
  });
  const trend = document.getElementById("trend-chart")?.closest(".chart-card");
  const distribution = document.getElementById("distribution-chart")?.closest(".chart-card");
  if (trend && distribution) {
    trend.classList.toggle("chart-focus", state.chartView === "trend");
    distribution.classList.toggle("chart-focus", state.chartView === "distribution");
  }
}

function render() {
  const payload = state.payload;
  if (!payload) return;

  setText("metric-cycles", payload.metrics.total_batches);
  setText("metric-issues", payload.metrics.detected_issues);
  setText("metric-resolved", payload.metrics.auto_resolved);
  setText("metric-escalated", payload.metrics.human_escalations);

  const cycle = payload.current_cycle;
  setText("system-status-label", payload.running ? formatStatus(cycle) : "Paused");
  setText("cycle-badge", getCurrentBatchLabel(cycle));

  applyView();
  renderOverview();
  renderAnalyticsPage();
  renderDatasetTable("healed", "clean");
  renderComparisonTable();
  renderResolvedIssuesTable();
  renderRecordTrail();
  renderApprovals();
}

function updateCountdown() {
  const payload = state.payload;
  if (!payload) return;
  if (!payload.running) {
    setText("countdown", "Paused");
    return;
  }
  const seconds = Math.max(0, Math.ceil(payload.next_run_at - Date.now() / 1000));
  setText("countdown", `${seconds}s`);
}

document.querySelectorAll("[data-action]").forEach((button) => {
  button.addEventListener("click", () => postAction({ action: button.dataset.action }));
});

document.querySelectorAll(".nav-link").forEach((button) => {
  button.addEventListener("click", () => {
    state.view = button.dataset.view;
    applyView();
  });
});

document.querySelectorAll("[data-dataset-target]").forEach((button) => {
  button.addEventListener("click", () => {
    const target = button.dataset.datasetTarget;
    state.datasetTabs[target] = button.dataset.tab;
    document
      .querySelectorAll(`[data-dataset-target="${target}"]`)
      .forEach((tab) => tab.classList.remove("active"));
    button.classList.add("active");
    renderDatasetTable("healed", target);
    if (target === "clean") {
      renderComparisonTable();
    }
  });
});

document.querySelectorAll(".chart-toggle").forEach((button) => {
  button.addEventListener("click", () => {
    state.chartView = button.dataset.chartView;
    applyView();
  });
});

document.getElementById("clean-filter").addEventListener("input", () => {
  renderDatasetTable("healed", "clean");
  renderComparisonTable();
});

document.getElementById("resolved-filter").addEventListener("input", () => {
  renderResolvedIssuesTable();
});

document.getElementById("record-filter").addEventListener("input", (event) => {
  state.recordOrderId = event.target.value.trim();
  renderRecordTrail();
});

document.getElementById("error-search").addEventListener("input", (event) => {
  state.errorSearch = event.target.value;
  renderErrorSearch();
});

document.getElementById("print-error-btn").addEventListener("click", () => {
  printCurrentErrorCard();
});

document.getElementById("download-report-json").addEventListener("click", () => {
  window.location.href = "/api/analysis-report.json";
});

document.getElementById("download-report-md").addEventListener("click", () => {
  window.location.href = "/api/analysis-report.md";
});

document.getElementById("download-comparison-btn").addEventListener("click", () => {
  downloadComparisonCsv();
});

document.getElementById("download-healed-orders").addEventListener("click", () => {
  window.location.href = "/api/healed-data/orders.csv";
});

document.getElementById("download-healed-payments").addEventListener("click", () => {
  window.location.href = "/api/healed-data/payments.csv";
});

document.getElementById("download-healed-delivery").addEventListener("click", () => {
  window.location.href = "/api/healed-data/delivery.csv";
});

document.getElementById("rollback-batch-btn").addEventListener("click", () => {
  const batchId = state.payload?.current_cycle?.batch_id || state.payload?.current_cycle?.cycle_id;
  if (!batchId) return;
  if (!window.confirm("Rollback this batch to its saved dirty state? This will reopen resolved issues in the current batch.")) return;
  postAction({ action: "rollback-batch", batch_id: batchId });
});

document.getElementById("reapply-batch-healing-btn").addEventListener("click", () => {
  const batchId = state.payload?.current_cycle?.batch_id || state.payload?.current_cycle?.cycle_id;
  if (!batchId) return;
  postAction({ action: "reapply-batch-healing", batch_id: batchId });
});

fetchState();
setInterval(fetchState, 2500);
setInterval(updateCountdown, 500);
