const byId = (id) => document.getElementById(id);
const replicas = ["a", "b"];
const clock = new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
const formatNumber = new Intl.NumberFormat("zh-CN");
const latestReads = new Map();
let selectedDb = 0;
let current = null;
let connected = false;
let refreshing = null;
let pending = false;
let route = null;
let confirmedSource = "";
let historyWarning = "";
const memoryActivity = new Map();
let storagePromise;
let activityRenderToken = 0;
let lastActivitySignature = "";

const form = byId("marker-form");
const markerInput = byId("marker-value");

function nodeState(node, replica = false) {
  if (!node) return ["pending", "等待连接"];
  if (node.paused) return ["paused", "已暂停"];
  if (!node.running) return ["offline", "未运行"];
  if (node.ready) return ["ready", replica ? "可读" : "运行中"];
  if (node.error) return ["error", "状态异常"];
  return ["pending", "启动中"];
}

function drawNode(id, node, replica = false) {
  const card = byId(`node-${id}`);
  const [state, label] = nodeState(node, replica);
  card.dataset.state = connected ? state : (current ? "offline" : state);
  card.querySelector("[data-node-state]").textContent = !connected && current ? "状态过期" : label;
  card.querySelector("[data-endpoint]").textContent = node?.port ? `127.0.0.1:${node.port}` : "—";
}

function drawRead(id, node) {
  const card = byId(`read-${id}`);
  const latest = node?.dbs?.[selectedDb];
  const cacheKey = `${id}:${selectedDb}`;
  const available = connected && node?.running && (id === "primary" ? node.ready : latest?.ready);

  const reading = available ? latest : latestReads.get(cacheKey);
  const history = Boolean(reading && !available);
  const [state, label] = !connected && current ? ["offline", "状态过期"] : nodeState(node, id !== "primary");
  let readState = state;
  let readLabel = label;
  if (available && id !== "primary") {
    if (latest.last_error) {
      readState = "error";
      readLabel = "拉取异常";
    } else if (latest.caught_up === true) {
      readState = "ready";
      readLabel = "已追齐";
    } else if (latest.caught_up === false) {
      readState = "pending";
      readLabel = "等待追赶";
    }
  } else if (connected && node?.running && id !== "primary" && !latest?.ready && !node.error) {
    readState = "pending";
    readLabel = "等待初始化";
  }
  card.dataset.state = readState;
  card.querySelector("[data-read-state]").textContent = history ? `${readLabel} · 上次读取` : readLabel;
  const value = card.querySelector("[data-value]");
  value.dataset.empty = typeof reading?.value !== "string" ? "true" : "false";
  value.textContent = typeof reading?.value === "string" ? reading.value : (reading ? "尚无标记" : "—");
  const sequence = card.querySelector("[data-sequence]");
  if (sequence) sequence.textContent = reading?.sequence == null ? "—" : formatNumber.format(reading.sequence);
  const snapshot = card.querySelector("[data-snapshot]");
  if (snapshot) snapshot.textContent = reading?.snapshot == null ? "—" : formatNumber.format(reading.snapshot);
  const error = card.querySelector("[data-error]");
  error.textContent = connected ? (latest?.last_error || node?.error || "") : "";
  error.hidden = !error.textContent;
}

function render() {
  drawCommands();
  if (route?.view === "overview") drawOverview();
  if (route?.view === "replication") drawReplication();
  drawNode("primary", current?.primary);
  drawNode("source", current?.source);
  drawRead("primary", current?.primary);
  for (const id of replicas) {
    const node = current?.replicas?.[id];
    drawNode(id, node, true);
    drawRead(id, node);
    const button = byId(`control-${id}`);
    button.textContent = node?.running && !node.paused ? "暂停" : "恢复";
    button.setAttribute("aria-label", `${button.textContent}副本 ${id.toUpperCase()}`);
    button.disabled = !connected || !node || pending;
  }
  byId("write-db").textContent = `DB ${selectedDb}`;
  byId("marker-key").textContent = current?.marker_key || "—";
  byId("send-marker").disabled = !connected || !current?.primary?.ready || pending;
  const activeReplicas = replicas.filter((id) => current?.replicas?.[id]?.running);
  const caughtUp = activeReplicas.filter((id) => current.replicas[id].dbs?.[selectedDb]?.caught_up).length;
  const note = byId("compare-note");
  if (!connected) {
    note.textContent = current ? "无法刷新状态。上次读取的记录仅供参考。" : "连接成功后，可写入新标记并观察副本进度。";
  } else if (!current.primary?.ready) {
    note.textContent = "等主库就绪后即可写入标记。";
  } else if (activeReplicas.length === 0) {
    note.textContent = "当前没有运行中的副本。恢复副本后即可观察追赶。";
  } else {
    note.textContent = `DB ${selectedDb} 有 ${caughtUp} 个副本已追齐，${activeReplicas.length} 个副本正在运行。`;
  }
}

async function request(path, body) {
  const options = {
    cache: "no-store",
    signal: AbortSignal.timeout(8000),
    headers: { Accept: "application/json" }
  };
  if (body !== undefined) {
    options.method = "POST";
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(body);
  }
  const response = await fetch(path, options);
  const data = await response.json().catch(() => null);
  if (!response.ok || data === null) {
    throw new Error(data?.error || `请求失败（HTTP ${response.status}）`);
  }
  return data;
}

async function fetchStatus() {
  try {
    const status = await request("/api/status");
    if (!status.primary || !status.source || !status.replicas) throw new Error("响应缺少节点状态");
    if (typeof status.workbench_url === "string" && /^https?:\/\//i.test(status.workbench_url)) {
      byId("open-workbench").href = status.workbench_url;
    }
    const previousSource = confirmedSource;
    const source = sourceIdentity(status);
    if (source && previousSource && source !== previousSource) latestReads.clear();
    if (source) confirmedSource = source;
    current = status;
    connected = true;
    for (const [id, node] of Object.entries({ primary: status.primary, ...status.replicas })) {
      for (const [db, reading] of Object.entries(node.dbs || {})) {
        if (node.running && (id === "primary" ? node.ready : reading.ready)) {
          latestReads.set(`${id}:${db}`, reading);
        }
      }
    }
    byId("connection").dataset.state = "ready";
    byId("connection-label").textContent = "服务已连接";
    byId("connection-notice").hidden = true;
    byId("last-refresh").textContent = `更新于 ${clock.format(new Date())}`;
  } catch {
    connected = false;
    byId("connection").dataset.state = "offline";
    byId("connection-label").textContent = "连接中断";
    byId("connection-notice").hidden = false;
    byId("last-refresh").textContent = "等待重连";
  } finally {
    render();
    if (route?.view === "activity") void drawActivity();
  }
}

function refresh() {
  if (!refreshing) {
    refreshing = fetchStatus().finally(() => { refreshing = null; });
  }
  return refreshing;
}

function report(message, error = false) {
  const feedback = byId("action-feedback");
  feedback.textContent = message;
  feedback.dataset.state = error ? "error" : "ready";
  feedback.hidden = false;

}

async function act(path, body, message, errorLabel) {
  if (pending || !connected) return false;
  pending = true;
  render();
  const feedback = byId("action-feedback");
  feedback.textContent = "正在提交…";
  feedback.dataset.state = "pending";
  feedback.hidden = false;
  let succeeded = false;
  const source = sourceIdentity(current);
  try {
    const result = await request(path, body);
    if (!result.ok) throw new Error(result.error || "服务未确认操作");
    report(message);
    succeeded = true;
    try {
      await saveActivity(source, message);
    } catch {
      historyWarning = "操作已获确认，但无法记录到本地。";
    }
  } catch (error) {
    report(`${errorLabel}失败。${error.message}`, true);
  } finally {
    // If polling started before the mutation, wait for it and request fresh state.
    if (refreshing) await refreshing;
    await refresh();
    pending = false;
    render();
  }
  return succeeded;
}

byId("db-selector").addEventListener("change", (event) => {
  if (event.target.name !== "view-db") return;
  location.hash = `/databases/${event.target.value}`;
});

byId("generate").addEventListener("click", () => {
  const suffix = crypto.getRandomValues(new Uint16Array(1))[0].toString(16).padStart(4, "0");
  markerInput.value = `标记-${clock.format(new Date()).replaceAll(":", "")}-${suffix}`;
  markerInput.setCustomValidity("");
  markerInput.focus();
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const value = markerInput.value.trim();
  if (!value) {
    markerInput.setCustomValidity("请输入标记值");
    markerInput.reportValidity();
    return;
  }
  if (new TextEncoder().encode(value).length > 256) {
    markerInput.setCustomValidity("标记最多支持 256 个 UTF-8 字节，请缩短内容");
    markerInput.reportValidity();
    return;
  }
  const db = selectedDb;
  const original = markerInput.value;
  const written = await act("/api/marker", { db, value }, `DB ${db} 写入成功，标记为「${value}」。`, "写入");
  if (written && markerInput.value === original) markerInput.value = "";
});
markerInput.addEventListener("input", () => { markerInput.setCustomValidity(""); });

for (const id of replicas) {
  byId(`control-${id}`).addEventListener("click", () => {
    const node = current?.replicas?.[id];
    if (!node) return;
    const pause = node.running && !node.paused;
    const label = `副本 ${id.toUpperCase()}`;
    act(`/api/replicas/${id}/${pause ? "pause" : "resume"}`, {},
      pause ? `已暂停${label}。写入新标记后可恢复它并观察追赶。` : `已请求恢复${label}。追赶进度见分库读取对照。`,
      pause ? "暂停" : "恢复");
  });
}


function sourceIdentity(state) {
  const ids = [...new Set(Object.values(state?.source?.dbs || {})
    .map((db) => db?.source).filter((id) => typeof id === "string" && id))];
  return state?.source?.running && ids.length === 1 ? ids[0] : "";
}

function textElement(tag, value, className) {
  const node = document.createElement(tag);
  node.textContent = value;
  if (className) node.className = className;
  return node;
}

function dbRead(node, db) {
  return connected && node?.running ? node.dbs?.[db] : undefined;
}

function numberText(number) {
  return Number.isSafeInteger(number) && number >= 0 ? formatNumber.format(number) : "—";
}

function replicaProgress(node, db) {
  if (!connected) return current ? "状态过期" : "等待连接";
  if (node?.paused) return "已暂停";
  if (!node?.running) return node?.error ? "不可用" : "未运行";
  const status = node.dbs?.[db];
  if (status?.last_error || node.error) return "状态异常";
  if (!status?.ready) return "初始化中";
  const source = dbRead(current?.source, db);
  if (!source?.source || !Number.isSafeInteger(source.sequence)) return "可读 · 源未知";
  if (status.source !== source.source) return "来源不同";
  if (status.caught_up) return "已追齐";
  return Number.isSafeInteger(status.sequence) && status.sequence >= source.sequence ? "等待主库提交封存" : "等待追赶";
}

function drawOverview() {
  const grid = byId("overview-databases");
  // Keep navigation links stable during polling so keyboard focus stays on them.
  if (!grid.children.length) {
    for (const db of [0, 1]) {
      const card = textElement("a", "", "summary-card");
      card.href = `#/databases/${db}`;
      card.append(textElement("h3", `DB ${db}`), textElement("p", "", "summary-state"));
      grid.append(card);
    }
  }
  for (const db of [0, 1]) {
    const primary = dbRead(current?.primary, db);
    const a = replicaProgress(current?.replicas?.a, db);
    const b = replicaProgress(current?.replicas?.b, db);
    grid.children[db].querySelector("p").textContent =
      `主库提交 ${numberText(primary?.sequence)} · A ${a} · B ${b}`;
  }
}

function metricList(values) {
  const list = document.createElement("dl");
  for (const [label, value] of values) {
    const cell = document.createElement("div");
    cell.append(textElement("dt", label), textElement("dd", value));
    list.append(cell);
  }
  return list;
}

function drawReplication() {
  const blocks = [];
  for (const db of [0, 1]) {
    const block = textElement("section", "", "diagnostic-group");
    const primary = dbRead(current?.primary, db);
    const source = dbRead(current?.source, db);
    const sourceId = source?.source;
    block.append(textElement("h2", `DB ${db}`));
    block.append(metricList([
      ["主库提交序号", numberText(primary?.sequence)],
      ["源可取序号", numberText(source?.sequence)],
      ["来源 ID", sourceId || "未知"]
    ]));
    for (const [label, node] of [["主库", current?.primary], ["源侧车", current?.source]]) {
      if (connected && node?.error) block.append(textElement("p", `${label} · ${node.error}`, "read-error"));
    }
    const pair = textElement("div", "", "diagnostic-pair");
    for (const id of replicas) {
      const node = current?.replicas?.[id];
      const status = dbRead(node, db);
      const comparable = status?.ready && sourceId && status.source === sourceId &&
        Number.isSafeInteger(source.sequence) && Number.isSafeInteger(status.sequence) &&
        status.sequence <= source.sequence;
      const panel = textElement("article", "", "diagnostic-replica");
      panel.append(textElement("h3", `副本 ${id.toUpperCase()}`), textElement("p", replicaProgress(node, db), "muted"));
      panel.append(metricList([
        ["应用序号", numberText(status?.sequence)],
        ["落后条数", comparable ? numberText(source.sequence - status.sequence) : "—"],
        ["快照序号", numberText(status?.snapshot)],
        ["安装 installs", numberText(status?.installs)],
        ["引导 bootstraps", numberText(status?.bootstraps)],
        ["换代 rebases", numberText(status?.rebases)]
      ]));
      if (connected && (status?.last_error || node?.error)) {
        panel.append(textElement("p", status?.last_error || node.error, "read-error"));
      }
      pair.append(panel);
    }
    block.append(pair);
    blocks.push(block);
  }
  byId("replication-metrics").replaceChildren(...blocks);
}

function endpoint(node) {
  const port = node?.port;
  return connected && node.running && Number.isInteger(port) && port > 0 && port <= 65535 ? port : null;
}

function drawCommands() {
  if (route?.view !== "commands") return;
  const db = route.db;
  const key = current?.marker_key;
  const validKey = typeof key === "string" && /^[a-zA-Z0-9:_-]+$/.test(key);
  const port = endpoint(current?.primary);
  const primary = port ? `redis-cli -h 127.0.0.1 -p ${port} -n ${db}` : "";
  const commands = {
    "read-command": [primary && validKey ? `${primary} GET ${key}` : "", "主库"],
    "primary-info-command": [primary ? `${primary} INFO` : "", "主库"]
  };
  for (const id of replicas) {
    const replicaPort = endpoint(current?.replicas?.[id]);
    const base = replicaPort ? `http://127.0.0.1:${replicaPort}` : "";
    const name = `副本 ${id.toUpperCase()}`;
    commands[`${id}-status-command`] = [base ? `curl -fsS '${base}/status?db=${db}'` : "", name];
    commands[`${id}-get-command`] = [base && validKey ? `curl -fsS '${base}/get?db=${db}&key=${encodeURIComponent(key)}'` : "", name];
  }
  for (const button of document.querySelectorAll("[data-command-copy]")) {
    const id = button.dataset.commandCopy;
    const [command, label] = commands[id];
    const display = byId(id);
    const nextText = command || `等待${label}可用`;
    if (display.textContent !== nextText) {
      byId("copy-feedback").hidden = true;
      display.textContent = nextText;
    }
    button.disabled = !command;
  }
}

for (const button of document.querySelectorAll("[data-command-copy]")) {
  button.addEventListener("click", async () => {
    const display = byId(button.dataset.commandCopy);
    const feedback = byId("copy-feedback");
    try {
      await navigator.clipboard.writeText(display.textContent);
      feedback.textContent = "已复制命令。";
    } catch {
      const range = document.createRange();
      range.selectNodeContents(display);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      feedback.textContent = "无法访问剪贴板，已选中命令。";
    }
    feedback.hidden = false;
  });
}

const activityPageSize = 5;

function activityStorage() {
  if (!storagePromise) {
    storagePromise = new Promise((resolve, reject) => {
      const request = indexedDB.open("kerniva-replica-activity", 2);
      const timeout = setTimeout(() => reject(new Error("等待本地记录存储超时")), 2000);
      request.onupgradeneeded = () => {
        const storage = request.result;
        const operations = storage.objectStoreNames.contains("operations")
          ? request.transaction.objectStore("operations")
          : storage.createObjectStore("operations", { keyPath: ["source", "id"] });
        operations.createIndex("sourceOrder", ["source", "when", "id"]);
      };
      request.onblocked = () => { clearTimeout(timeout); reject(new Error("本地记录存储暂不可用")); };
      request.onerror = () => { clearTimeout(timeout); reject(request.error); };
      request.onsuccess = () => {
        clearTimeout(timeout);
        const storage = request.result;
        storage.onversionchange = () => {
          storage.close();
          storagePromise = undefined;
        };
        resolve(storage);
      };
    });
  }
  return storagePromise;
}

async function saveActivity(source, message) {
  if (!source) {
    historyWarning = "有操作已获确认，但当时来源身份未知，未写入实例记录。";
    return;
  }
  const entry = { source, id: crypto.randomUUID(), when: Date.now(), message };
  try {
    const storage = await activityStorage();
    await new Promise((resolve, reject) => {
      const transaction = storage.transaction("operations", "readwrite");
      transaction.oncomplete = resolve;
      transaction.onabort = () => reject(transaction.error);
      transaction.objectStore("operations").add(entry);
    });
  } catch {
    memoryActivity.set(entry.id, entry);
    historyWarning = "有操作未能写入本地存储，仅保留在当前页面。";
  }
}

function orderActivity(a, b) {
  return b.when - a.when || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0);
}

async function readActivity(source, requestedPage) {
  const temporary = [...memoryActivity.values()].filter((entry) => entry.source === source);
  const pageFor = (total) => Math.min(requestedPage, Math.max(1, Math.ceil(total / activityPageSize)));
  try {
    const storage = await activityStorage();
    return await new Promise((resolve, reject) => {
      const transaction = storage.transaction("operations");
      transaction.onabort = () => reject(transaction.error);
      const index = transaction.objectStore("operations").index("sourceOrder");
      // The empty array sorts after every numeric timestamp in this compound key.
      const range = IDBKeyRange.bound([source], [source, []], false, true);
      const count = index.count(range);
      count.onerror = () => reject(count.error);
      count.onsuccess = () => {
        const total = count.result + temporary.length;
        const page = pageFor(total);
        const offset = (page - 1) * activityPageSize;
        // Memory-only writes may precede any durable entry. In that fallback
        // case, widen the cursor window by at most the number of those writes.
        const skip = Math.max(0, offset - temporary.length);
        const limit = activityPageSize + offset - skip;
        const entries = [];
        const request = index.openCursor(range, "prev");
        let skipped = skip === 0;
        request.onerror = () => reject(request.error);
        const finish = () => {
          const start = offset - skip;
          const visible = temporary.length
            ? entries.concat(temporary).sort(orderActivity).slice(start, start + activityPageSize)
            : entries;
          resolve({ total, page, entries: visible });
        };
        request.onsuccess = () => {
          const cursor = request.result;
          if (!cursor) {
            finish();
            return;
          }
          if (!skipped) {
            skipped = true;
            cursor.advance(skip);
            return;
          }
          entries.push(cursor.value);
          if (entries.length < limit) cursor.continue();
          else finish();
        };
      };
    });
  } catch {
    historyWarning = "本地存储不可用，仅显示保留在当前页面的操作。";
    const page = pageFor(temporary.length);
    const offset = (page - 1) * activityPageSize;
    return { total: temporary.length, page, entries: temporary.sort(orderActivity).slice(offset, offset + activityPageSize) };
  }
}

async function drawActivity() {
  const token = ++activityRenderToken;
  const source = connected ? sourceIdentity(current) : "";
  const { entries, total, page } = source
    ? await readActivity(source, route.page)
    : { entries: [], total: 0, page: route.page };
  if (token !== activityRenderToken || route?.view !== "activity") return;
  const list = byId("activity-list");
  const note = byId("activity-note");
  note.textContent = source ? historyWarning : "等待当前来源身份，以免显示其他实例的记录。";
  note.hidden = !note.textContent;
  const pages = Math.max(1, Math.ceil(total / activityPageSize));
  if (source && page !== route.page) {
    history.replaceState(null, "", `#/activity?page=${page}`);
    route.page = page;
  }
  const signature = `${source}:${page}:${total}:${entries.map((entry) => entry.id).join(",")}`;
  if (signature === lastActivitySignature) return;
  lastActivitySignature = signature;
  const rows = entries.map((entry) => {
    const row = document.createElement("li");
    row.dataset.activityId = entry.id;
    const date = new Date(entry.when);
    const time = textElement("time", clock.format(date));
    time.dateTime = date.toISOString();
    time.title = date.toLocaleString("zh-CN");
    row.append(time, textElement("span", "完成", "result"), textElement("span", entry.message));
    return row;
  });
  list.replaceChildren(...rows);
  const empty = byId("activity-empty");
  empty.textContent = source ? "当前实例还没有已确认的面板操作。" : "等待读取操作记录…";
  empty.hidden = rows.length > 0;
  const pager = byId("activity-pagination");
  pager.replaceChildren();
  if (total === 0) return;
  for (const [label, target] of [["上一页", page - 1], [`${page} / ${pages} 页`, null], ["下一页", page + 1]]) {
    if (target === null || target < 1 || target > pages) pager.append(textElement("span", label, "muted"));
    else {
      const link = textElement("a", label, "button button-light");
      link.href = `#/activity?page=${target}`;
      pager.append(link);
    }
  }
}

const aliases = {
  "": "#/overview",
  "#top": "#/overview",
  "#topology": "#/overview",
  "#workspace": "#/databases/0",
  "#replication": "#/replication",
  "#commands": "#/commands/0",
  "#activity": "#/activity?page=1",
  "#experiments": "#/experiments?page=1"
};
const viewTitles = {
  overview: "概览", databases: "逻辑库", replication: "复制诊断", commands: "校验命令",
  activity: "操作记录", experiments: "实验数据", "not-found": "未找到页面"
};

function readRoute() {
  const known = aliases[location.hash];
  if (known) history.replaceState(null, "", known);
  const [path, query] = location.hash.slice(1).split("?", 2);
  const match = path.match(/^\/(databases|commands)\/([01])$/);
  if (match) return { view: match[1], db: Number(match[2]) };
  if (path === "/overview" || path === "/replication") return { view: path.slice(1) };
  if (path === "/activity" || path === "/experiments") {
    const params = new URLSearchParams(query);
    const page = Number(params.get("page"));
    return { view: path.slice(1), page: Number.isSafeInteger(page) && page > 0 ? page : 1, series: params.get("series") || "all" };
  }
  return { view: "not-found" };
}

const navigationToggle = byId("toggle-nav");
byId("panel-host").textContent = location.host;

function navigate(focus) {
  route = readRoute();
  const db = route.db;
  if (route.view === "databases") {
    selectedDb = db;
    byId("db-selector").querySelector(`[value="${db}"]`).checked = true;
    byId("compare-title").textContent = `DB ${db}`;
  }
  byId("nav-commands").href = `#/commands/${db ?? selectedDb}`;
  for (const link of document.querySelectorAll("[data-nav], [data-sidebar-db], [data-command-db]")) {
    const active = link.dataset.nav === route.view ||
      (route.view === "databases" && Number(link.dataset.sidebarDb) === db) ||
      (route.view === "commands" && Number(link.dataset.commandDb) === db);
    if (active) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  }
  const title = route.view === "databases" ? `DB ${db}` : viewTitles[route.view];
  byId("view-name").textContent = title;
  document.title = `${title} · Kerniva`;
  for (const view of document.querySelectorAll("[data-view]")) view.hidden = view.dataset.view !== route.view;
  navigationToggle.setAttribute("aria-expanded", "false");
  render();
  if (route.view === "activity") void drawActivity();
  if (route.view === "experiments" && typeof renderExperiments === "function") {
    renderExperiments({ container: byId("experiment-content"), page: route.page, series: route.series });
  }
  if (focus) {
    document.querySelector("[data-view]:not([hidden]) h1").focus({ preventScroll: true });
    window.scrollTo(0, 0);
  }
}

navigationToggle.addEventListener("click", () => {
  const expanded = navigationToggle.getAttribute("aria-expanded") === "true";
  navigationToggle.setAttribute("aria-expanded", String(!expanded));
});
byId("section-nav").addEventListener("click", (event) => {
  const link = event.target.closest("a");
  if (link?.hash === location.hash && !event.ctrlKey && !event.metaKey && !event.shiftKey && !event.altKey) {
    event.preventDefault();
    navigate(true);
  }
});
byId("skip-workspace").addEventListener("click", (event) => {
  event.preventDefault();
  document.querySelector("[data-view]:not([hidden]) h1")?.focus();
});
window.addEventListener("hashchange", () => navigate(true));
byId("retry").addEventListener("click", refresh);
navigate(false);
refresh();
window.setInterval(refresh, 850);


function renderExperiments({ container, page, series }) {
  const source = document.getElementById("experiment-data");
  const archive = JSON.parse(source.textContent);
  const titles = { shared: "固定总带宽", capacity: "按库独立限速" };
  const filter = Object.hasOwn(titles, series) ? series : "all";
  const groups = filter === "all" ? ["shared", "capacity"] : [filter];
  const records = groups.flatMap(group => archive.records.filter(record => record.series === group));
  const size = 5;
  const pages = Math.max(1, Math.ceil(records.length / size));
  const requested = Number(page);
  const index = Number.isSafeInteger(requested) ? Math.min(pages, Math.max(1, requested)) : 1;
  const url = (group, n) => "#/experiments?" + new URLSearchParams(
    group === "all" ? {page: String(n)} : {series: group, page: String(n)}
  );
  const canonical = url(filter, index);
  if (location.hash !== canonical) history.replaceState(history.state, "", canonical);
  const view = document.getElementById("experiment-template").content.cloneNode(true);
  const select = view.querySelector("#experiment-series");
  select.value = filter;
  select.addEventListener("change", () => { location.hash = url(select.value, 1); });
  const offset = (index - 1) * size;
  view.querySelector("#experiment-count").textContent =
    `第 ${offset + 1}–${Math.min(offset + size, records.length)} 条，共 ${records.length} 个有效窗口`;
  view.querySelector("#experiment-position").textContent = `${index} / ${pages}`;
  for (const [id, target] of [["prev", index - 1], ["next", index + 1]]) {
    const link = view.querySelector("#experiment-" + id);
    if (target >= 1 && target <= pages) link.href = url(filter, target);
    else link.setAttribute("aria-disabled", "true");
  }
  const methods = {
    all: "固定总带宽组让 16 个库争用同一条链路。按库限速组给每条慢流单独分配带宽。",
    shared: "15 个慢库与 1 个轻库共享 8 MiB/s 的非空载荷预算，分别放行 2、4、8、15 份快照。",
    capacity: "副本跟随 2、4、8 或 16 个库。每个慢库独享 512 KiB/s，轻库直接拉取增量。"
  };
  view.querySelector("[data-method]").textContent = methods[filter];
  const notes = view.querySelector("[data-notes]");
  const field = (list, label, text) => {
    const key = document.createElement("dt");
    const value = document.createElement("dd");
    key.textContent = label;
    value.textContent = text;
    list.append(key, value);
  };
  field(notes, "环境", `WSL 2 · ${archive.environment.fs} · 每库快照 ${archive.environment.snapshot_bytes_per_db.toLocaleString("zh-CN")} 字节`);
  for (const group of groups) {
    const info = archive.series[group];
    const skipped = info.excluded_runs.map(run => run.run).join("、");
    const reason = group === "shared" ? "收敛窗口检测到演示控制进程读取活动" : "数据校验完成后，中继关闭时出现 BrokenPipe";
    field(notes, titles[group], `${info.accepted_count} 个有效窗口，排除 ${skipped}（${reason}）。`);
  }
  const body = view.querySelector("tbody");
  for (const record of records.slice(offset, offset + size)) {
    const row = body.insertRow();
    row.dataset.run = record.run;
    row.dataset.series = record.series;
    const cell = row.insertCell();
    const expand = document.createElement("button");
    expand.type = "button";
    expand.className = "archive-run";
    expand.textContent = record.run;
    const label = document.createElement("small");
    label.textContent = `${titles[record.series]} · ${record.dbs} 库`;
    cell.append(expand, label);
    for (const text of [String(record.slow_concurrency), record.recovery_s.toFixed(2) + " s", record.b_peak_rss_mib.toFixed(1) + " MiB"]) {
      row.insertCell().textContent = text;
    }
    const detail = body.insertRow();
    detail.hidden = true;
    detail.id = "detail-" + record.run;
    expand.setAttribute("aria-controls", detail.id);
    expand.setAttribute("aria-expanded", "false");
    expand.setAttribute("aria-label", record.run + " 运行明细");
    const full = detail.insertCell();
    full.colSpan = 4;
    const facts = document.createElement("dl");
    facts.className = "archive-metadata";
    field(facts, "三次 GET 延迟上界", record.b_get_marker_upper_ms.map(ms => ms.toFixed(1) + " ms").join(" / "));
    field(facts, "结果文件（仓库内）", record.source_path);
    field(facts, "文件 SHA-256", record.source_sha256);
    full.append(facts);
    expand.addEventListener("click", () => {
      detail.hidden = !detail.hidden;
      expand.setAttribute("aria-expanded", String(!detail.hidden));
    });
  }
  container.replaceChildren(view);
}
