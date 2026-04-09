const state = {
  selectedTab: "orders",
  payload: null,
};

async function fetchState() {
  const response = await fetch("/api/state");
  state.payload = await response.json();
  render();
}

async function postAction(action) {
  await fetch("/api/action", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action }),
  });
  await fetchState();
}

function setText(id, value) {
  const element = document.getElementById(id);
  if (element) element.textContent = value;
}

function formatStatus(cycle) {
  if (!cycle) return "Waiting for first cycle";
  if (cycle.status === "healthy") return "Healthy batch";
  if (cycle.status === "degraded") return "Self-healed";
  return "Human attention required";
}

function renderSummary(cycle) {
  const host = document.getElementById("summary-grid");
  host.innerHTML = "";
  if (!cycle) {
    host.innerHTML = `<div class="summary-item"><span>Status</span><strong>No cycle yet</strong></div>`;
    return;
  }
  const items = [
    ["Batch Size", cycle.batch_size],
    ["Failures Injected", cycle.summary.injected],
    ["Issues Detected", cycle.summary.detected],
    ["Escalations", cycle.summary.escalated],
  ];
  items.forEach(([label, value]) => {
    const div = document.createElement("div");
    div.className = "summary-item";
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

function renderTable(cycle) {
  const thead = document.getElementById("table-head");
  const tbody = document.getElementById("table-body");
  thead.innerHTML = "";
  tbody.innerHTML = "";

  if (!cycle) {
    return;
  }

  const dirtyRows = cycle.datasets.dirty[state.selectedTab] || [];
  const healedRows = cycle.datasets.healed[state.selectedTab] || [];
  const columns = new Set(["order_id"]);
  dirtyRows.slice(0, 5).forEach((row) => Object.keys(row).forEach((key) => columns.add(key)));
  healedRows.slice(0, 5).forEach((row) => Object.keys(row).forEach((key) => columns.add(key)));
  const orderedColumns = Array.from(columns);

  const headRow = document.createElement("tr");
  headRow.innerHTML = orderedColumns.map((column) => `<th>${column}</th>`).join("") + "<th>dirty snapshot</th><th>healed snapshot</th>";
  thead.appendChild(headRow);

  for (let index = 0; index < Math.max(dirtyRows.length, healedRows.length, 5); index += 1) {
    const dirty = dirtyRows[index] || {};
    const healed = healedRows[index] || {};
    const tr = document.createElement("tr");
    const leftCells = orderedColumns.map((column) => {
      const value = healed[column] ?? dirty[column] ?? "";
      return `<td>${value}</td>`;
    }).join("");
    tr.innerHTML =
      leftCells +
      `<td class="dirty">${JSON.stringify(dirty)}</td>` +
      `<td class="healed">${JSON.stringify(healed)}</td>`;
    tbody.appendChild(tr);
  }
}

function renderHistory(history) {
  const host = document.getElementById("history-cards");
  host.innerHTML = "";
  if (!history || !history.length) {
    host.innerHTML = "<div class='history-card'>No completed cycles yet.</div>";
    return;
  }
  history.slice(0, 6).forEach((cycle) => {
    const card = document.createElement("div");
    card.className = "history-card";
    card.innerHTML = `
      <strong>${cycle.cycle_id}</strong>
      <div>${cycle.started_at}</div>
      <div>Status: ${formatStatus(cycle)}</div>
      <div>Detected: ${cycle.summary.detected} | Resolved: ${cycle.summary.resolved} | Escalated: ${cycle.summary.escalated}</div>
    `;
    host.appendChild(card);
  });
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
  setText("cycle-badge", cycle ? cycle.cycle_id : "Waiting for cycle");

  renderSummary(cycle);
  renderList("injected-list", cycle?.injected_errors, (item) => `
    <div class="event-meta"><span>${item.error_id}</span><span>${item.severity}</span></div>
    <div>${item.dataset}: ${item.message}</div>
  `);
  renderList("audit-list", cycle?.detailed_logs, (item) => `
    <div class="event-meta"><span>${item.error_id}</span><span>${item.status}</span></div>
    <div>${item.resolution_action}</div>
  `);
  renderList("event-feed", payload.event_feed, (item) => `
    <div class="event-meta"><span>${item.stage}</span><span>${item.time}</span></div>
    <div>${item.message}</div>
  `);
  renderList("detailed-log-list", cycle?.detailed_logs, (item) => `
    <div class="event-meta"><span>${item.error_id}</span><span>${item.resolver}</span></div>
    <div><strong>${item.dataset}</strong> / ${item.error_type}</div>
    <div>${item.finding}</div>
    <div>Dirty: ${item.dirty_value ?? "-"}</div>
    <div>Original: ${item.original_value ?? "-"}</div>
    <div>Action: ${item.resolution_action}</div>
    <div>Resolved: ${item.resolved_value ?? "-"}</div>
    <div>Status: ${item.status}</div>
  `);
  renderTable(cycle);
  renderHistory(payload.history);
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
  button.addEventListener("click", () => postAction(button.dataset.action));
});

document.querySelectorAll(".tab").forEach((button) => {
  button.addEventListener("click", () => {
    state.selectedTab = button.dataset.tab;
    document.querySelectorAll(".tab").forEach((tab) => tab.classList.remove("active"));
    button.classList.add("active");
    renderTable(state.payload?.current_cycle);
  });
});

fetchState();
setInterval(fetchState, 2500);
setInterval(updateCountdown, 500);
