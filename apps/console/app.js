const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const numberFormat = new Intl.NumberFormat("zh-CN");
const currencyFormat = new Intl.NumberFormat("zh-CN", {
  style: "currency",
  currency: "USD",
});

const state = {
  connected: false,
  loading: false,
  observations: [],
  findings: [],
  queue: null,
  deadLetters: [],
  usage: [],
  selectedEvidence: new Set(),
  config: {
    apiUrl: "http://127.0.0.1:8080",
    tenantId: "",
    apiKey: "",
    period: currentMonth(),
  },
};

const elements = {
  connectionForm: document.querySelector("#connection-form"),
  connectionPanel: document.querySelector("#connection-panel"),
  connectionMessage: document.querySelector("#connection-message"),
  connectionDot: document.querySelector("#connection-dot"),
  connectionLabel: document.querySelector("#connection-label"),
  toggleConnection: document.querySelector("#toggle-connection"),
  apiUrl: document.querySelector("#api-url"),
  tenantId: document.querySelector("#tenant-id"),
  apiKey: document.querySelector("#api-key"),
  period: document.querySelector("#usage-period"),
  refresh: document.querySelector("#refresh-data"),
  lastSync: document.querySelector("#last-sync"),
  observationsMetric: document.querySelector("#metric-observations"),
  errorRateMetric: document.querySelector("#metric-error-rate"),
  errorsCopy: document.querySelector("#metric-errors-copy"),
  pendingMetric: document.querySelector("#metric-pending"),
  processingCopy: document.querySelector("#metric-processing-copy"),
  deadMetric: document.querySelector("#metric-dead"),
  observationRows: document.querySelector("#observation-rows"),
  kindDistribution: document.querySelector("#kind-distribution"),
  statusFilter: document.querySelector("#status-filter"),
  selectionCount: document.querySelector("#selection-count"),
  evidencePreview: document.querySelector("#evidence-preview"),
  findingCount: document.querySelector("#finding-count"),
  findingList: document.querySelector("#finding-list"),
  queueMode: document.querySelector("#queue-mode"),
  queuePending: document.querySelector("#queue-pending"),
  queueProcessing: document.querySelector("#queue-processing"),
  queueDead: document.querySelector("#queue-dead"),
  deadLetterList: document.querySelector("#dead-letter-list"),
  investigationForm: document.querySelector("#investigation-form"),
  agentObjective: document.querySelector("#agent-objective"),
  agentMessage: document.querySelector("#agent-form-message"),
  runAgent: document.querySelector("#run-agent"),
  agentResult: document.querySelector("#agent-result"),
  usageLedger: document.querySelector("#usage-ledger"),
  quoteForm: document.querySelector("#quote-form"),
  planSelect: document.querySelector("#plan-select"),
  quoteOutput: document.querySelector("#quote-output"),
  toastRegion: document.querySelector("#toast-region"),
};

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

function currentMonth() {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function readStorage(storage, key, fallback = "") {
  try {
    return storage.getItem(key) ?? fallback;
  } catch {
    return fallback;
  }
}

function writeStorage(storage, key, value) {
  try {
    storage.setItem(key, value);
  } catch {
    // Storage can be disabled in hardened browsers. The in-memory config still works.
  }
}

function restoreConfig() {
  const query = new URLSearchParams(window.location.search);
  state.config.apiUrl = query.get("api") || readStorage(localStorage, "observability.apiUrl", state.config.apiUrl);
  state.config.tenantId = query.get("tenant") || readStorage(localStorage, "observability.tenantId");
  state.config.period = readStorage(localStorage, "observability.period", currentMonth());
  state.config.apiKey = readStorage(sessionStorage, "observability.apiKey");
  elements.apiUrl.value = state.config.apiUrl;
  elements.tenantId.value = state.config.tenantId;
  elements.period.value = state.config.period;
  elements.apiKey.value = state.config.apiKey;
}

function readAndValidateConfig() {
  const apiUrl = elements.apiUrl.value.trim().replace(/\/+$/, "");
  const tenantId = elements.tenantId.value.trim();
  const apiKey = elements.apiKey.value;
  const period = elements.period.value;
  let parsed;
  try {
    parsed = new URL(apiUrl);
  } catch {
    throw new Error("API 地址不是有效 URL。");
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    throw new Error("API 地址必须使用 http 或 https。");
  }
  if (!UUID_PATTERN.test(tenantId)) {
    throw new Error("Tenant ID 必须是有效 UUID。");
  }
  if (!/^\d{4}-\d{2}$/.test(period)) {
    throw new Error("请选择有效的用量周期。");
  }
  return { apiUrl, tenantId, apiKey, period };
}

function persistConfig(config) {
  writeStorage(localStorage, "observability.apiUrl", config.apiUrl);
  writeStorage(localStorage, "observability.tenantId", config.tenantId);
  writeStorage(localStorage, "observability.period", config.period);
  writeStorage(sessionStorage, "observability.apiKey", config.apiKey);
}

async function request(path, options = {}) {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 10_000);
  const headers = new Headers(options.headers || {});
  headers.set("accept", "application/json");
  if (options.body) headers.set("content-type", "application/json");
  if (state.config.apiKey) headers.set("x-api-key", state.config.apiKey);
  if (state.config.tenantId) headers.set("x-tenant-id", state.config.tenantId);

  try {
    const response = await fetch(`${state.config.apiUrl}${path}`, {
      ...options,
      headers,
      signal: controller.signal,
    });
    if (!response.ok) {
      const detail = (await response.text()).trim();
      throw new ApiError(response.status, detail || `API 返回 ${response.status}`);
    }
    if (response.status === 204 || response.headers.get("content-length") === "0") return null;
    const contentType = response.headers.get("content-type") || "";
    return contentType.includes("application/json") ? response.json() : response.text();
  } catch (error) {
    if (error.name === "AbortError") throw new Error("API 请求超过 10 秒。");
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}

async function optionalRequest(path) {
  try {
    return await request(path);
  } catch (error) {
    if (error instanceof ApiError && error.status === 409) return null;
    throw error;
  }
}

function setConnectionState(status, label) {
  elements.connectionDot.classList.toggle("connected", status === "connected");
  elements.connectionDot.classList.toggle("error", status === "error");
  elements.connectionLabel.textContent = label;
}

function setFormMessage(element, message = "", type = "") {
  element.textContent = message;
  element.classList.toggle("error", type === "error");
  element.classList.toggle("success", type === "success");
}

function showToast(message, type = "") {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`.trim();
  toast.textContent = message;
  elements.toastRegion.append(toast);
  window.setTimeout(() => toast.remove(), 4_500);
}

function setLoading(loading) {
  state.loading = loading;
  elements.connectionForm.querySelector("button[type='submit']").disabled = loading;
  elements.refresh.disabled = loading || !state.connected;
  elements.runAgent.disabled = loading || !state.connected || state.selectedEvidence.size === 0;
  elements.quoteForm.querySelector("button").disabled = loading || !state.connected;
  for (const metric of [elements.observationsMetric, elements.errorRateMetric, elements.pendingMetric, elements.deadMetric]) {
    metric.classList.toggle("loading-pulse", loading);
  }
  if (loading) {
    elements.observationRows.innerHTML = Array.from({ length: 5 }, () => `
      <tr aria-hidden="true">
        <td><span class="loading-pulse">□</span></td>
        <td><span class="loading-pulse">loading trace</span></td>
        <td><span class="loading-pulse">loading observation</span></td>
        <td><span class="loading-pulse">status</span></td>
        <td><span class="loading-pulse">000ms</span></td>
        <td><span class="loading-pulse">0000-00-00</span></td>
      </tr>`).join("");
  }
}

async function loadData({ announce = true } = {}) {
  if (state.loading) return;
  setLoading(true);
  setConnectionState("loading", "正在同步");
  setFormMessage(elements.connectionMessage, "正在读取 tenant 范围数据…");
  try {
    await request("/health");
    const status = elements.statusFilter.value;
    const query = new URLSearchParams({
      tenant_id: state.config.tenantId,
      page: "1",
      page_size: "100",
    });
    if (status) query.set("status", status);
    const observations = await request(`/v1/observations?${query}`);

    const [findingsResult, queueResult, usageResult] = await Promise.allSettled([
      request("/v1/diagnostics", {
        method: "POST",
        body: JSON.stringify({ tenant_id: state.config.tenantId }),
      }),
      optionalRequest(`/v1/ingestion/queue?tenant_id=${encodeURIComponent(state.config.tenantId)}`),
      request(`/v1/usage?tenant_id=${encodeURIComponent(state.config.tenantId)}&period=${encodeURIComponent(state.config.period)}`),
    ]);

    state.observations = Array.isArray(observations) ? observations : [];
    state.findings = findingsResult.status === "fulfilled" && Array.isArray(findingsResult.value) ? findingsResult.value : [];
    state.queue = queueResult.status === "fulfilled" ? queueResult.value : null;
    state.usage = usageResult.status === "fulfilled" && Array.isArray(usageResult.value) ? usageResult.value : [];
    state.deadLetters = state.queue
      ? await request(`/v1/ingestion/dead-letters?tenant_id=${encodeURIComponent(state.config.tenantId)}&limit=50`)
      : [];
    state.selectedEvidence = new Set(
      [...state.selectedEvidence].filter((id) => state.observations.some((item) => item.id === id)),
    );
    state.connected = true;
    renderAll();
    setConnectionState("connected", "已连接");
    setFormMessage(elements.connectionMessage, "连接成功，数据来自当前 Rust API。", "success");
    elements.lastSync.textContent = `同步于 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
    elements.connectionPanel.classList.add("collapsed");
    elements.toggleConnection.setAttribute("aria-expanded", "false");
    if (announce) showToast("数据已同步");

    const partialFailures = [findingsResult, queueResult, usageResult].filter((result) => result.status === "rejected");
    if (partialFailures.length > 0) showToast(`${partialFailures.length} 个辅助接口读取失败，主观测仍可用。`, "error");
  } catch (error) {
    state.connected = false;
    setConnectionState("error", "连接失败");
    setFormMessage(elements.connectionMessage, humanizeError(error), "error");
    elements.connectionPanel.classList.remove("collapsed");
    elements.toggleConnection.setAttribute("aria-expanded", "true");
    renderDisconnectedError(error);
  } finally {
    setLoading(false);
  }
}

function humanizeError(error) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "认证失败。检查 API key 与 X-Tenant-ID 对应关系。";
    if (error.status === 403) return "当前凭据无权访问这个 tenant。";
    if (error.status === 500) return `服务配置错误：${error.message}`;
    return `API ${error.status}：${error.message}`;
  }
  if (error instanceof TypeError) return "无法连接 API。检查地址、服务状态与 CORS allowlist。";
  return error.message || "连接失败。";
}

function renderDisconnectedError(error) {
  const message = escapeHtml(humanizeError(error));
  elements.observationRows.innerHTML = `<tr class="empty-row"><td colspan="6"><div class="empty-state"><span aria-hidden="true">×</span><strong>无法读取观测</strong><p>${message}</p></div></td></tr>`;
}

function renderAll() {
  renderMetrics();
  renderDistribution();
  renderObservations();
  renderFindings();
  renderQueue();
  renderUsage();
  renderEvidence();
  elements.refresh.disabled = false;
  elements.quoteForm.querySelector("button").disabled = false;
}

function renderMetrics() {
  const errors = state.observations.filter((item) => item.status === "Error").length;
  const rate = state.observations.length ? (errors / state.observations.length) * 100 : 0;
  elements.observationsMetric.textContent = numberFormat.format(state.observations.length);
  elements.errorRateMetric.textContent = `${rate.toFixed(1)}%`;
  elements.errorsCopy.textContent = `${numberFormat.format(errors)} 条错误观测`;
  elements.pendingMetric.textContent = state.queue ? numberFormat.format(state.queue.pending) : "—";
  elements.processingCopy.textContent = state.queue ? `${numberFormat.format(state.queue.processing)} 条处理中` : "durable 队列未启用";
  elements.deadMetric.textContent = state.queue ? numberFormat.format(state.queue.dead_letter) : "—";
}

function renderDistribution() {
  if (state.observations.length === 0) {
    elements.kindDistribution.innerHTML = '<div class="empty-inline">当前筛选没有观测</div>';
    return;
  }
  const counts = new Map();
  for (const item of state.observations) counts.set(item.kind, (counts.get(item.kind) || 0) + 1);
  elements.kindDistribution.innerHTML = [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([kind, count]) => {
      const share = Math.max(8, Math.round((count / state.observations.length) * 100));
      return `<div class="distribution-item" style="--share:${share}%"><i class="distribution-fill"></i><strong>${numberFormat.format(count)}</strong><span>${escapeHtml(kind)}</span></div>`;
    })
    .join("");
}

function renderObservations() {
  if (state.observations.length === 0) {
    elements.observationRows.innerHTML = '<tr class="empty-row"><td colspan="6"><div class="empty-state"><span aria-hidden="true">⌁</span><strong>当前筛选没有观测</strong><p>采集事件后刷新，或切换状态筛选。</p></div></td></tr>';
    return;
  }
  elements.observationRows.innerHTML = state.observations
    .map((item) => {
      const checked = state.selectedEvidence.has(item.id) ? "checked" : "";
      const statusClass = item.status === "Error" ? "error" : "ok";
      const trace = String(item.trace_id || "—");
      const span = String(item.span_id || "—");
      return `<tr>
        <td><input class="evidence-check" type="checkbox" data-evidence-id="${escapeHtml(item.id)}" aria-label="将 ${escapeHtml(item.name)} 加入证据" ${checked}></td>
        <td class="trace-cell"><strong class="mono" title="${escapeHtml(trace)}">${escapeHtml(shortId(trace))}</strong><span title="${escapeHtml(span)}">span ${escapeHtml(shortId(span))}</span></td>
        <td class="event-cell"><strong>${escapeHtml(item.name)}</strong><span>${escapeHtml(item.kind)}</span></td>
        <td><span class="status-label ${statusClass}">${escapeHtml(item.status)}</span></td>
        <td class="duration">${numberFormat.format(item.duration_ms || 0)} ms</td>
        <td class="timestamp">${escapeHtml(formatTimestamp(item.started_at_ms))}</td>
      </tr>`;
    })
    .join("");
}

function renderFindings() {
  elements.findingCount.textContent = numberFormat.format(state.findings.length);
  if (state.findings.length === 0) {
    elements.findingList.innerHTML = '<div class="empty-state small"><strong>当前没有 finding</strong><p>这表示 API 没有从当前观测中诊断出错误。</p></div>';
    return;
  }
  elements.findingList.innerHTML = state.findings
    .map((finding) => {
      const severity = String(finding.severity || "Info").toLowerCase();
      return `<article class="finding-item"><header><strong>${escapeHtml(finding.title)}</strong><span class="severity ${escapeHtml(severity)}">${escapeHtml(finding.severity)}</span></header><p>${numberFormat.format(finding.evidence_ids?.length || 0)} 条 evidence</p></article>`;
    })
    .join("");
}

function renderQueue() {
  if (!state.queue) {
    elements.queueMode.textContent = "direct / unavailable";
    elements.queuePending.textContent = "—";
    elements.queueProcessing.textContent = "—";
    elements.queueDead.textContent = "—";
    elements.deadLetterList.innerHTML = '<p class="empty-inline">API 未启用 durable ingestion</p>';
    return;
  }
  elements.queueMode.textContent = "durable";
  elements.queuePending.textContent = numberFormat.format(state.queue.pending);
  elements.queueProcessing.textContent = numberFormat.format(state.queue.processing);
  elements.queueDead.textContent = numberFormat.format(state.queue.dead_letter);
  if (!state.deadLetters.length) {
    elements.deadLetterList.innerHTML = '<p class="empty-inline">没有死信任务</p>';
    return;
  }
  elements.deadLetterList.innerHTML = state.deadLetters
    .map((item) => `<article class="dead-letter-item"><strong>${escapeHtml(item.observation?.name || item.id)}</strong><span title="${escapeHtml(item.last_error || "")}">${escapeHtml(item.last_error || "没有错误详情")}</span><button type="button" data-replay-id="${escapeHtml(item.id)}">重放 · attempt ${numberFormat.format(item.attempts || 0)}</button></article>`)
    .join("");
}

function renderUsage() {
  const kinds = ["Observation", "AgentRun", "ToolCall", "ModelToken"];
  const totals = new Map(state.usage.map((item) => [item.kind, item.quantity]));
  elements.usageLedger.innerHTML = kinds
    .map((kind) => `<div class="usage-item"><span>${escapeHtml(kind)}</span><strong>${numberFormat.format(totals.get(kind) || 0)}</strong></div>`)
    .join("");
}

function renderEvidence() {
  const selected = [...state.selectedEvidence];
  elements.selectionCount.textContent = `${numberFormat.format(selected.length)} 条证据`;
  elements.runAgent.disabled = !state.connected || state.loading || selected.length === 0;
  elements.evidencePreview.textContent = selected.length
    ? selected.map(shortId).join(" · ")
    : "尚未选择证据";
}

function shortId(value) {
  const text = String(value || "");
  return text.length > 12 ? `${text.slice(0, 8)}…` : text;
}

function formatTimestamp(value) {
  const date = new Date(Number(value));
  if (!Number.isFinite(date.getTime())) return "—";
  return date.toLocaleString("zh-CN", { hour12: false });
}

async function runAgent(event) {
  event.preventDefault();
  const objective = elements.agentObjective.value.trim();
  if (!objective) {
    setFormMessage(elements.agentMessage, "请填写调查目标。", "error");
    return;
  }
  if (!state.selectedEvidence.size) {
    setFormMessage(elements.agentMessage, "至少选择一条 evidence。", "error");
    return;
  }
  elements.runAgent.disabled = true;
  setFormMessage(elements.agentMessage, "正在生成证据约束的计划…");
  elements.agentResult.hidden = true;
  try {
    const decision = await request("/v1/agent/plan", {
      method: "POST",
      body: JSON.stringify({
        tenant_id: state.config.tenantId,
        objective,
        observation_ids: [...state.selectedEvidence],
      }),
    });
    const actions = Array.isArray(decision.actions) ? decision.actions : [];
    elements.agentResult.innerHTML = `<h3>调查计划</h3><p>${escapeHtml(decision.summary)}</p><ul>${actions.map((action) => `<li><strong>${escapeHtml(action.tool_name)}</strong> — ${escapeHtml(action.reason)}${action.requires_approval ? "（需要审批）" : ""}</li>`).join("")}</ul>`;
    elements.agentResult.hidden = false;
    setFormMessage(elements.agentMessage, `计划引用 ${numberFormat.format(decision.evidence_ids?.length || 0)} 条证据。`, "success");
  } catch (error) {
    setFormMessage(elements.agentMessage, humanizeError(error), "error");
  } finally {
    elements.runAgent.disabled = !state.connected || state.selectedEvidence.size === 0;
  }
}

async function calculateQuote(event) {
  event.preventDefault();
  const button = elements.quoteForm.querySelector("button");
  button.disabled = true;
  elements.quoteOutput.textContent = "计算中…";
  try {
    const quote = await request("/v1/billing/quote", {
      method: "POST",
      body: JSON.stringify({
        tenant_id: state.config.tenantId,
        period: state.config.period,
        plan: elements.planSelect.value,
      }),
    });
    elements.quoteOutput.textContent = `${currencyFormat.format(quote.total_cents / 100)} / 月 · 已用 ${numberFormat.format(quote.used_observations)} / 包含 ${numberFormat.format(quote.included_observations)}`;
  } catch (error) {
    elements.quoteOutput.textContent = humanizeError(error);
  } finally {
    button.disabled = !state.connected;
  }
}

async function replayDeadLetter(id, button) {
  button.disabled = true;
  try {
    await request("/v1/ingestion/dead-letters/replay", {
      method: "POST",
      body: JSON.stringify({ tenant_id: state.config.tenantId, observation_id: id }),
    });
    showToast("死信已重新进入队列");
    await loadData({ announce: false });
  } catch (error) {
    showToast(humanizeError(error), "error");
    button.disabled = false;
  }
}

function setupNavigation() {
  const links = [...document.querySelectorAll(".primary-nav a")];
  const sections = links.map((link) => document.querySelector(link.getAttribute("href"))).filter(Boolean);
  const observer = new IntersectionObserver((entries) => {
    const visible = entries.filter((entry) => entry.isIntersecting).sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];
    if (!visible) return;
    for (const link of links) link.classList.toggle("active", link.getAttribute("href") === `#${visible.target.id}`);
  }, { rootMargin: "-25% 0px -60%", threshold: [0.05, 0.35] });
  for (const section of sections) observer.observe(section);
}

elements.connectionForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    state.config = readAndValidateConfig();
    persistConfig(state.config);
    await loadData();
  } catch (error) {
    setConnectionState("error", "配置无效");
    setFormMessage(elements.connectionMessage, humanizeError(error), "error");
  }
});

elements.refresh.addEventListener("click", () => loadData());
elements.statusFilter.addEventListener("change", () => loadData({ announce: false }));
elements.investigationForm.addEventListener("submit", runAgent);
elements.quoteForm.addEventListener("submit", calculateQuote);

elements.observationRows.addEventListener("change", (event) => {
  const checkbox = event.target.closest("[data-evidence-id]");
  if (!checkbox) return;
  if (checkbox.checked) state.selectedEvidence.add(checkbox.dataset.evidenceId);
  else state.selectedEvidence.delete(checkbox.dataset.evidenceId);
  renderEvidence();
});

elements.deadLetterList.addEventListener("click", (event) => {
  const button = event.target.closest("[data-replay-id]");
  if (button) replayDeadLetter(button.dataset.replayId, button);
});

elements.toggleConnection.addEventListener("click", () => {
  const collapsed = elements.connectionPanel.classList.toggle("collapsed");
  elements.toggleConnection.setAttribute("aria-expanded", String(!collapsed));
});

elements.period.addEventListener("change", () => {
  state.config.period = elements.period.value;
  writeStorage(localStorage, "observability.period", state.config.period);
  if (state.connected) loadData({ announce: false });
});

restoreConfig();
setupNavigation();

if (state.config.tenantId && UUID_PATTERN.test(state.config.tenantId)) {
  loadData({ announce: false });
}
