const byId = (id) => document.getElementById(id);
const replicas = ["a", "b"];
const labels = {primary: "主库", source: "复制源", a: "副本 A", b: "副本 B"};
const clock = new Intl.DateTimeFormat("zh-CN", {hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false});
const number = new Intl.NumberFormat("zh-CN", {maximumFractionDigits: 1});

function numeric(value, suffix = "") {
  return Number.isFinite(value) && value >= 0 ? number.format(value) + suffix : "—";
}

function bytes(value) {
  if (!Number.isFinite(value) || value < 0) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const index = value >= 1 ? Math.min(units.length - 1, Math.floor(Math.log(value) / Math.log(1024))) : 0;
  return numeric(value / 1024 ** index, " " + units[index]);
}

function uptime(value) {
  if (!Number.isFinite(value) || value < 0) return "—";
  const days = Math.floor(value / 86400);
  const hours = Math.floor(value / 3600) % 24;
  const minutes = Math.floor(value / 60) % 60;
  return [days && days + " 天", hours && hours + " 小时", minutes && minutes + " 分", Math.floor(value) % 60 + " 秒"].filter(Boolean).join(" ");
}

function sourceIdentity(snapshot) {
  const ids = new Set(Object.values(snapshot?.source?.dbs || {}).map((reading) => reading.source).filter(Boolean));
  return snapshot?.source?.running && ids.size === 1 ? [...ids][0] : "";
}

export function createOperations({state, request, routeLink, setCurrent, element, feedback, endpointLabel}) {
  let visibleRoute = "";
  let session = null;
  let statusRequest;
  let statusTimer;
  let snapshot;
  let snapshotTime;
  let replicaError = "";
  let replicaRequest;
  let replicaTimer;
  let sourceId = "";
  let reads = new Map();
  let pending = false;
  let tab = "live";
  let db = 0;
  let page = 1;
  let series = "all";
  let archive;
  let archiveRequest;
  let activityRequest;

  function metrics(entries) {
    const list = element("dl", undefined, "ops-metrics");
    for (const [label, value] of entries) {
      const pair = element("div");
      pair.append(element("dt", label), element("dd", value ?? "—"));
      list.append(pair);
    }
    return list;
  }

  function connectionChanged() {
    if (visibleRoute !== "status" || session === state.active?.session) return;
    statusRequest?.abort();
    statusRequest = null;
    clearTimeout(statusTimer);
    session = state.active?.session;
    const source = byId("status-source");
    byId("status-offline").hidden = Boolean(session);
    byId("status-refresh").hidden = !session;
    byId("status-message").textContent = "";
    byId("status-nodes").replaceChildren();
    source.textContent = state.active
      ? state.active.profile.name + " · " + endpointLabel(state.active.profile) + " · " +
        (state.active.profile.mode === "cluster" ? "Redis Cluster · 主节点" : "DB " + state.active.profile.db)
      : "当前连接的数据库节点";
    if (session) void refreshStatus();
  }

  async function refreshStatus() {
    if (visibleRoute !== "status" || !session || statusRequest || document.hidden) return;
    clearTimeout(statusTimer);
    const controller = new AbortController();
    statusRequest = controller;
    byId("status-refresh").disabled = true;
    const message = byId("status-message");
    if (!byId("status-nodes").children.length) message.textContent = "正在读取 INFO…";
    try {
      const active = state.active;
      const timeout = active.profile.timeout || 5;
      const result = await request("status", {session}, AbortSignal.any([controller.signal, AbortSignal.timeout((timeout * 3 + 2) * 1000)]));
      if (statusRequest !== controller) return;
      const cards = result.nodes.map((node) => {
        const panel = element("article", undefined, "panel");
        const heading = element("header", undefined, "panel-heading");
        const address = node.host && node.port ? endpointLabel(node) : node.label;
        heading.append(element("h2", address || node.id || "数据库节点"));
        const role = {master: "主节点", primary: "主节点", slave: "副本", replica: "副本"}[node.role] || node.role;
        if (role) heading.append(element("span", role, "quiet"));
        const body = element("div", undefined, "panel-body");
        if (node.error) body.append(element("p", node.error, "inline-error"));
        else body.append(metrics([
          ["版本", node.version ?? "—"],
          ["运行时间", uptime(node.uptimeSeconds)],
          ["已用内存", bytes(node.usedMemoryBytes)],
          ...(node.usedMemoryScope ? [["内存统计范围", node.usedMemoryScope === "db" ? "当前逻辑库" : "节点进程"]] : []),
          ["客户端连接", numeric(node.connectedClients)],
          ["每秒操作", numeric(node.opsPerSecond)],
          [result.mode === "cluster" ? "DB 0 键数" : `DB ${result.db} 键数`, numeric(node.keys)],
          ...(node.runtime ? [["运行环境", node.runtime]] : []),
          ...(node.heapBytes != null ? [["Go 堆分配", bytes(node.heapBytes)]] : []),
          ...(node.gcCount != null ? [["垃圾回收次数", numeric(node.gcCount)]] : []),
          ...(node.storageBackend ? [["存储后端", node.storageBackend]] : []),
          ...(node.durability ? [["持久化模式", node.durability]] : []),
          ...(node.sequence != null ? [["提交序号", numeric(node.sequence)]] : []),
        ]));
        panel.append(heading, body);
        return panel;
      });
      byId("status-nodes").replaceChildren(...cards);
      message.textContent = cards.length ? [result.server, "INFO", "更新于 " + clock.format(new Date())].filter(Boolean).join(" · ")
        : "当前连接未报告可查询的节点。";
    } catch (error) {
      if (statusRequest !== controller) return;
      byId("status-nodes").replaceChildren();
      message.textContent = "无法读取运行状况 · " + error.message;
    } finally {
      if (statusRequest === controller) {
        statusRequest = null;
        byId("status-refresh").disabled = false;
        if (!document.hidden) statusTimer = setTimeout(refreshStatus, 5000);
      }
    }
  }

  function replicaLink(nextTab, {page: nextPage, series: nextSeries} = {}) {
    return routeLink("replication", {tab: nextTab === "live" ? null : nextTab, db: db || null, page: nextPage, series: nextSeries === "all" ? null : nextSeries});
  }

  function nodeFor(id) {
    return replicas.includes(id) ? snapshot?.replicas?.[id] : snapshot?.[id];
  }

  function nodeState(id) {
    const node = nodeFor(id);
    if (replicaError) return snapshot ? "状态过期" : "不可用";
    if (!node) return "等待探测";
    if (node.paused) return "已暂停";
    if (!node.running) return "未运行";
    if (node.error) return "状态异常";
    return node.ready ? (replicas.includes(id) ? "可读" : "运行中") : "启动中";
  }

  function readCurrent(id, database) {
    const node = nodeFor(id);
    const reading = node?.dbs?.[database];
    if (!replicaError && node?.running && !node.paused && reading &&
        (replicas.includes(id) ? reading.ready : reading.sequence != null)) return reading;
    return null;
  }

  function renderReplica() {
    if (visibleRoute !== "replication") return;
    const summary = byId("replication-message");
    summary.textContent = replicaError ? "状态读取失败 · " + replicaError
      : snapshotTime ? "演示控制器 · 更新于 " + clock.format(snapshotTime) : "正在探测演示拓扑…";
    byId("replication-refresh").disabled = Boolean(replicaRequest) || pending;
    if (tab === "commands") {
      renderCommands();
      return;
    }
    if (tab !== "live") return;
    for (const id of Object.keys(labels)) {
      const card = byId("replication-topology-" + id);
      const node = nodeFor(id);
      card.querySelector("[data-endpoint]").textContent = node?.port ? "127.0.0.1:" + node.port : "—";
      card.querySelector("[data-node-state]").textContent = nodeState(id);
      card.dataset.health = !replicaError && node?.ready && !node.error ? "ready" : "unknown";
    }
    const source = readCurrent("source", db);
    byId("replication-source").textContent = source?.source
      ? `DB ${db} · 来源 ${source.source} · 源可取序号 ${numeric(source.sequence)}`
      : `DB ${db} · 当前来源未确认`;
    byId("replication-marker-key").textContent = snapshot?.marker_key || "";
    byId("replication-compare-heading").textContent = `DB ${db} · 标记读取对照`;
    byId("send-marker").disabled = pending || !readCurrent("primary", db);
    for (const id of ["primary", ...replicas]) drawReading(id, source);
  }

  function drawReading(id, source) {
    const node = nodeFor(id);
    const current = readCurrent(id, db);
    const reading = current || reads.get(id + ":" + db);
    const latest = node?.dbs?.[db];
    const waiting = latest?.last_error ? "拉取异常" : (current?.caught_up ? "已追齐" : "等待追赶");
    let status = nodeState(id);
    const primary = readCurrent("primary", db);
    const older = current && replicas.includes(id) && source?.source && current.source === source.source &&
      Number.isSafeInteger(current.sequence) && Number.isSafeInteger(primary?.sequence) && current.sequence < primary.sequence;
    if (current && replicas.includes(id)) {
      if (source?.source && current.source !== source.source) status = "来源不同";
      else status = source?.source ? waiting : "可读 · 源未知";
    } else if (current) {
      status = "已读取";
    }
    const card = byId("replication-read-" + id);
    const [heading, value, facts, error, action] = card.children;
    heading.lastElementChild.textContent = reading && !current ? status + " · 上次读取" : (older ? status + " · 旧值" : status);
    value.textContent = typeof reading?.value === "string" ? reading.value : (reading ? "未读到标记" : "—");
    value.dataset.history = Boolean((reading && !current) || older);
    const entries = [[id === "primary" ? "提交序号" : "应用序号", numeric(reading?.sequence)]];
    if (replicas.includes(id)) {
      const comparable = current && source?.source && current.source === source.source &&
        Number.isSafeInteger(source.sequence) && Number.isSafeInteger(current.sequence) && source.sequence >= current.sequence;
      entries.push(["来源 ID", reading?.source || "—"], ["落后源序号", comparable ? numeric(source.sequence - current.sequence) : "—"],
        ["快照序号", numeric(reading?.snapshot)], ["安装 / 引导 / 换代", [reading?.installs, reading?.bootstraps, reading?.rebases].map((value) => numeric(value)).join(" / ")]);
    }
    facts.replaceChildren(metrics(entries));
    error.textContent = replicaError ? "" : (latest?.last_error || node?.error || "");
    error.hidden = !error.textContent;
    if (action) {
      action.textContent = node?.running && !node.paused ? "暂停副本 " + id.toUpperCase() : "恢复副本 " + id.toUpperCase();
      action.disabled = !node || Boolean(replicaError) || pending;
    }
  }

  function canPollReplica() {
    return visibleRoute === "replication" && state.replicationAvailable && ["live", "commands"].includes(tab) && !document.hidden;
  }

  function refreshReplica() {
    if (!canPollReplica() || pending) return;
    clearTimeout(replicaTimer);
    if (!replicaRequest) replicaRequest = loadReplica();
    return replicaRequest;
  }

  async function loadReplica() {
    byId("replication-refresh").disabled = true;
    try {
      const result = await request("replication/status", undefined, AbortSignal.timeout(15000));
      if (!result.primary || !result.source || !result.replicas) throw new Error("响应缺少节点状态。");
      const newSource = sourceIdentity(result);
      if (newSource && sourceId && newSource !== sourceId) reads.clear();
      if (newSource) sourceId = newSource;
      snapshot = result;
      snapshotTime = new Date();
      replicaError = "";
      for (const id of ["primary", ...replicas]) {
        for (const database of [0, 1]) {
          const reading = readCurrent(id, database);
          if (reading) reads.set(id + ":" + database, reading);
        }
      }
    } catch (error) {
      replicaError = error.message;
    } finally {
      replicaRequest = null;
      renderReplica();
      if (canPollReplica()) replicaTimer = setTimeout(refreshReplica, 2000);
    }
  }

  async function mutate(path, body, message) {
    if (pending || replicaError || !snapshot) return false;
    pending = true;
    clearTimeout(replicaTimer);
    renderReplica();
    byId("replication-warning").textContent = "";
    feedback("replication-feedback", "正在提交…");
    let ok = false;
    try {
      const result = await request("replication/" + path, body, AbortSignal.timeout(45000));
      if (!result.ok) throw new Error(result.error || "控制器未确认操作。");
      feedback("replication-feedback", message);
      byId("replication-warning").textContent = result.warning || "";
      ok = true;
    } catch (error) {
      feedback("replication-feedback", error.message, true);
    } finally {
      await replicaRequest;
      if (canPollReplica() && !replicaRequest) {
        clearTimeout(replicaTimer);
        replicaRequest = loadReplica();
        await replicaRequest;
      }
      pending = false;
      renderReplica();
      if (visibleRoute === "replication" && tab === "activity") await loadActivity();
    }
    return ok;
  }

  function renderCommands() {
    const container = byId("replication-commands");
    const key = snapshot?.marker_key;
    const validKey = typeof key === "string" && /^[a-zA-Z0-9:_-]+$/.test(key);
    const endpoint = (id) => {
      const node = nodeFor(id);
      return !replicaError && node?.running && !node?.paused && Number.isInteger(node.port) && node.port > 0 && node.port <= 65535 ? node.port : null;
    };
    const main = endpoint("primary");
    const redis = main ? `redis-cli -h 127.0.0.1 -p ${main} -n ${db}` : "";
    const commands = [["主库 GET", redis && validKey ? redis + " GET " + key : ""], ["主库 INFO", redis ? redis + " INFO" : ""]];
    for (const id of replicas) {
      const port = endpoint(id);
      const url = port ? `http://127.0.0.1:${port}` : "";
      commands.push([labels[id] + " 状态", url ? `curl -fsS "${url}/status?db=${db}"` : ""],
        [labels[id] + " GET", url && validKey ? `curl -fsS "${url}/get?db=${db}&key=${encodeURIComponent(key)}"` : ""]);
    }
    const signature = JSON.stringify(commands);
    if (container.dataset.signature === signature) return;
    container.dataset.signature = signature;
    const note = element("p", "从演示拓扑所在主机执行以下命令。Windows PowerShell 可使用 curl.exe。", "quiet");
    const result = element("p", "", "quiet");
    result.setAttribute("role", "status");
    const blocks = commands.map(([title, command]) => {
      const block = element("div", undefined, "ops-command");
      const display = element("pre", command || "等待节点可用", "value-box");
      const copy = element("button", "复制", "button");
      copy.type = "button";
      copy.setAttribute("aria-label", "复制" + title + "命令");
      copy.disabled = !command;
      copy.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(command);
          result.textContent = "已复制" + title + "命令。";
        } catch {
          const range = document.createRange();
          range.selectNodeContents(display);
          const selection = window.getSelection();
          selection.removeAllRanges();
          selection.addRange(range);
          result.textContent = "无法访问剪贴板，已选中命令。";
        }
      });
      block.append(element("h3", title), copy, display);
      return block;
    });
    container.replaceChildren(note, ...blocks, result);
  }

  function pager(previous, next, position) {
    const row = element("div", undefined, "ops-toolbar");
    for (const [title, href] of [["上一页", previous], [position, null], ["下一页", next]]) {
      const cell = element(href ? "a" : "span", title, href ? "button" : "quiet");
      if (href) cell.href = href;
      row.append(cell);
    }
    return row;
  }

  async function loadActivity() {
    activityRequest?.abort();
    const controller = new AbortController();
    activityRequest = controller;
    const container = byId("replication-activity");
    container.replaceChildren(element("p", "正在读取操作记录…", "quiet"));
    try {
      const records = await request("replication/activity?page=" + page, undefined,
        AbortSignal.any([controller.signal, AbortSignal.timeout(15000)]));
      if (activityRequest !== controller || visibleRoute !== "replication" || tab !== "activity") return;
      const list = element("ol", undefined, "ops-activity");
      for (const record of records.items) {
        const entry = element("li");
        const date = new Date(record.when);
        const timestamp = element("time", date.toLocaleString("zh-CN", {hour12: false}), "quiet");
        if (Number.isFinite(date.getTime())) timestamp.dateTime = date.toISOString();
        entry.append(timestamp, element("p", record.message));
        if (record.source) entry.append(element("code", Array.isArray(record.source) ? record.source.join(" · ") : record.source, "quiet"));
        list.append(entry);
      }
      container.replaceChildren(element("p", records.total ? `演示控制器记录了 ${records.total} 次已确认操作。` : "此工作台的演示控制器尚无已确认操作。", "quiet"));
      if (records.items.length) container.append(list);
      if (records.pages > 1) container.append(pager(
        records.page > 1 ? replicaLink("activity", {page: records.page - 1}) : null,
        records.page < records.pages ? replicaLink("activity", {page: records.page + 1}) : null,
        `${records.page} / ${records.pages}`));
    } catch (error) {
      if (activityRequest === controller && !controller.signal.aborted) container.replaceChildren(element("p", error.message, "inline-error"));
    }
  }

  async function loadArchive() {
    const container = byId("replication-experiments");
    if (!archive) container.replaceChildren(element("p", "正在读取实验归档…", "quiet"));
    try {
      if (!archive) {
        if (!archiveRequest) {
          archiveRequest = request("replication/experiments", undefined, AbortSignal.timeout(8000)).finally(() => { archiveRequest = null; });
        }
        archive = await archiveRequest;
      }
      if (visibleRoute !== "replication" || tab !== "experiments") return;
      renderExperiments({container, page, series, archive, url: (filter, index) => replicaLink("experiments", {page: index, series: filter})});
    } catch (error) {
      if (visibleRoute === "replication" && tab === "experiments") container.replaceChildren(element("p", error.message, "inline-error"));
    }
  }

  function navigate() {
    visibleRoute = state.route.view;
    clearTimeout(statusTimer);
    clearTimeout(replicaTimer);
    activityRequest?.abort();
    if (visibleRoute === "status") {
      connectionChanged();
      void refreshStatus();
    } else {
      statusRequest?.abort();
      statusRequest = null;
      session = null;
    }
    if (visibleRoute !== "replication") return;
    const params = state.route.params;
    const activeTab = params.get("tab");
    tab = ["commands", "activity", "experiments"].includes(activeTab) ? activeTab : "live";
    db = params.get("db") === "1" ? 1 : 0;
    page = Math.max(1, Math.min(10000, Math.trunc(Number(params.get("page")) || 1)));
    series = params.get("series") || "all";
    byId("replication-db").value = db;
    byId("replication-unavailable").hidden = !state.ready || state.replicationAvailable;
    byId("replication-content").hidden = !state.replicationAvailable;
    byId("replication-refresh").hidden = !state.replicationAvailable;
    byId("replication-db-row").hidden = tab === "activity" || tab === "experiments";
    for (const link of document.querySelectorAll("[data-replication-tab]")) {
      const target = link.dataset.replicationTab;
      link.href = replicaLink(target);
      setCurrent(link, target === tab);
    }
    for (const pane of document.querySelectorAll("[data-replication-pane]")) pane.hidden = pane.dataset.replicationPane !== tab;
    if (!state.replicationAvailable) return;
    renderReplica();
    if (tab === "activity") void loadActivity();
    else if (tab === "experiments") void loadArchive();
    else void refreshReplica();
  }

  for (const id of ["primary", ...replicas]) {
    const card = element("article", undefined, "ops-reading");
    card.id = "replication-read-" + id;
    const heading = element("header");
    heading.append(element("h3", labels[id]), element("span", "", "quiet"));
    card.append(heading, element("pre", "", "value-box ops-reading-value"), element("div"), element("p", "", "inline-error"));
    if (id !== "primary") {
      const button = element("button", "暂停副本 " + id.toUpperCase(), "button");
      button.id = "control-" + id;
      button.type = "button";
      button.addEventListener("click", () => {
        const node = nodeFor(id);
        const pause = node?.running && !node.paused;
        void mutate(`replicas/${id}/${pause ? "pause" : "resume"}`, {}, (pause ? "已暂停" : "已请求恢复") + labels[id] + "。");
      });
      card.append(button);
    }
    byId("replication-reads").append(card);
  }

  const input = byId("marker-value");
  byId("generate-marker").addEventListener("click", () => {
    const suffix = crypto.getRandomValues(new Uint16Array(1))[0].toString(16).padStart(4, "0");
    input.value = "标记-" + clock.format(new Date()).replaceAll(":", "") + "-" + suffix;
    input.setCustomValidity("");
    input.focus();
  });
  input.addEventListener("input", () => input.setCustomValidity(""));
  byId("marker-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const original = input.value;
    const value = original.trim();
    if (!value || new TextEncoder().encode(value).length > 256) {
      input.setCustomValidity(value ? "标记最多支持 256 个 UTF-8 字节。" : "请输入标记值。");
      input.reportValidity();
      return;
    }
    if (!readCurrent("primary", db)) return;
    const written = await mutate("marker", {db, value}, `DB ${db} 已写入标记「${value}」。`);
    if (written && input.value === original) input.value = "";
  });
  byId("replication-db").addEventListener("change", (event) => {
    db = Number(event.target.value);
    location.hash = replicaLink(tab);
  });
  byId("status-refresh").addEventListener("click", refreshStatus);
  byId("replication-refresh").addEventListener("click", () => {
    if (tab === "activity") void loadActivity();
    else if (tab === "experiments") void loadArchive();
    else void refreshReplica();
  });
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      clearTimeout(statusTimer);
      clearTimeout(replicaTimer);
    } else {
      void refreshStatus();
      void refreshReplica();
    }
  });
  return {navigate, connectionChanged};
}

function renderExperiments({container, page, series, archive, url}) {
  const titles = { shared: "固定总带宽", capacity: "按库独立限速" };
  const filter = Object.hasOwn(titles, series) ? series : "all";
  const groups = filter === "all" ? ["shared", "capacity"] : [filter];
  const records = groups.flatMap(group => archive.records.filter(record => record.series === group));
  const size = 5;
  const pages = Math.max(1, Math.ceil(records.length / size));
  const requested = Number(page);
  const index = Number.isSafeInteger(requested) ? Math.min(pages, Math.max(1, requested)) : 1;
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
    else { link.setAttribute("aria-disabled", "true"); link.classList.add("is-disabled"); }
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
    const reason = group === "shared" ? "检测到演示控制进程活动或进程重启" : "数据校验完成后，中继关闭时出现 BrokenPipe";
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
