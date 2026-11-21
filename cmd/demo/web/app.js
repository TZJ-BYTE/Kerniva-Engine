"use strict";

const element = (id) => document.getElementById(id);
const colors = { get: "#087f72", set: "#5269b7", failures: "#bb4139" };
const number = (value, digits = 0) => Number(value).toLocaleString("zh-CN", { maximumFractionDigits: digits });
let currentStatus = null;
let view = "live";
let pollTimer = null;
let renderedKey = "";
let chartPoints = [];
let statusSequence = 0;

async function request(path, body) {
  const options = body === undefined ? {} : {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
  const response = await fetch(path, options);
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
  return result;
}

function activeRun(run) {
  return run && !run.finished_utc;
}

function setView(next) {
  view = next;
  element("live-tab").classList.toggle("active", view === "live");
  element("history-tab").classList.toggle("active", view === "history");
  element("history-controls").hidden = view !== "history";
  renderedKey = "";
  if (view === "live") renderLive();
}

function pointsFromMeasurements(measurements) {
  const points = [];
  let offset = 0;
  for (const measurement of measurements) {
    for (const window of measurement.windows || []) {
      points.push({
        second: offset + window.planned_second + 1,
        getCount: window.get.succeeded,
        setCount: window.set.succeeded,
        getP99: window.get.success_latency.count ? window.get.success_latency.p99_us : null,
        setP99: window.set.success_latency.count ? window.set.success_latency.p99_us : null,
        failures: window.get.failures + window.set.failures,
      });
    }
    offset += measurement.duration_seconds;
  }
  return points;
}

function renderMeasurement(measurement, title, source, measurements, document) {
  element("result-title").textContent = title;
  element("result-source").textContent = source;
  element("result-duration").textContent = `${number(measurement.duration_seconds, 1)} 秒测量`;
  element("metric-rate").textContent = number(measurement.actual_success_rate, 1);
  element("metric-get").textContent = measurement.get.success_latency.count ? number(measurement.get.success_latency.p99_us, 1) : "待采集";
  element("metric-set").textContent = measurement.set.success_latency.count ? number(measurement.set.success_latency.p99_us, 1) : "待采集";
  element("metric-get-count").textContent = `${number(measurement.get.succeeded)} 次成功 GET`;
  element("metric-set-count").textContent = `${number(measurement.set.succeeded)} 次成功 SET`;
  element("metric-failures").textContent = number(measurement.failures);
  element("metric-failures").style.color = measurement.failures ? colors.failures : "";
  element("metric-offered").textContent = `${number(measurement.offered)} 次计划请求`;
  element("metric-failure-rate").textContent = measurement.offered
    ? `失败率 ${(measurement.failures / measurement.offered * 100).toFixed(2)}% · failures / offered`
    : "失败率待采集";
  element("raw-json").textContent = JSON.stringify(document || measurement, null, 2);
  chartPoints = pointsFromMeasurements(measurements);
  drawCharts();
}

function renderLive() {
  const run = currentStatus?.run;
  const windows = run?.windows || [];
  if (!windows.length) {
    element("result-title").textContent = "等待完成首个测量窗口";
    element("result-source").textContent = "数据由 scripts/bench 实际请求产生。";
    element("result-duration").textContent = "";
    for (const id of ["metric-rate", "metric-get", "metric-set", "metric-failures"]) {
      element(id).textContent = "—";
    }
    element("metric-get-count").textContent = "成功请求延迟";
    element("metric-set-count").textContent = "成功请求延迟";
    element("metric-offered").textContent = "保留全部计划请求分母";
    element("metric-failure-rate").textContent = "失败率待采集";
    element("raw-json").textContent = "尚无已完成的测量。";
    chartPoints = [];
    drawCharts();
    return;
  }
  const key = `${run.id}:${windows.length}`;
  if (renderedKey === key) return;
  renderedKey = key;
  const last = windows[windows.length - 1];
  renderMeasurement(
    last.measurement,
    `最近完成的第 ${last.index} 个窗口`,
    `实时演示 · ${currentStatus.database} · ${last.source} · 曲线包含本次已完成窗口`,
    windows.map((window) => window.measurement),
  );
}

async function refreshStatus() {
  clearTimeout(pollTimer);
  const sequence = ++statusSequence;
  try {
    const status = await request("/api/status");
    if (sequence !== statusSequence) return;
    currentStatus = status;
    element("database").textContent = currentStatus.database;
    element("history-root").textContent = `结果目录 ${currentStatus.history_root}`;
    const run = currentStatus.run;
    const running = activeRun(run);
    const phases = {
      prefilling: "首次预填充 1000 个键，等待 bench 就绪",
      preparing: "准备下一个测量窗口",
      measuring: `正在测量第 ${run?.current_window || 1} 个窗口`,
      stopping: "正在停止自有 bench 进程",
      stopped: "已停止，保留已完成窗口",
      completed: "负载已完成",
      failed: "负载运行失败",
    };
    element("load-status").textContent = run ? `${phases[run.phase] || run.phase} · 已完成 ${run.windows.length} 个窗口` : "空闲，等待手动开始";
    element("phase-dot").className = `dot${run?.phase === "failed" ? " error" : running ? "" : " idle"}`;
    element("load-error").textContent = run?.error || "";
    element("start-load").disabled = Boolean(running);
    element("stop-load").disabled = !running || run.phase === "stopping";
    if (view === "live") renderLive();
    if (running) pollTimer = setTimeout(refreshStatus, 1000);
  } catch (error) {
    if (sequence !== statusSequence) return;
    element("load-error").textContent = error.message;
    element("load-status").textContent = "无法读取本地演示状态";
    element("phase-dot").className = "dot error";
    element("start-load").disabled = true;
    element("stop-load").disabled = true;
  }
}

element("command-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const buttons = event.currentTarget.querySelectorAll("button");
  buttons.forEach((button) => { button.disabled = true; });
  try {
    const result = await request("/api/command", {
      operation: event.submitter?.value || "GET",
      key: element("key").value,
      value: element("value").value,
    });
    element("command-reply").textContent = result.reply;
    element("command-reply").classList.toggle("error", result.reply_type === "-");
    element("command-time").textContent = `${result.operation} · ${number(result.elapsed_us, 1)} μs · ${result.source} · 包含建连和完整响应`;
  } catch (error) {
    element("command-reply").textContent = error.message;
    element("command-reply").classList.add("error");
    element("command-time").textContent = "请求失败";
  } finally {
    buttons.forEach((button) => { button.disabled = false; });
  }
});

element("load-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  element("start-load").disabled = true;
  element("load-error").textContent = "";
  try {
    await request("/api/load/start", {
      rate: Number(element("rate").value),
      clients: Number(element("clients").value),
      duration_seconds: Number(element("duration").value),
      read_percent: Number(element("read-percent").value),
    });
    setView("live");
    await refreshStatus();
  } catch (error) {
    element("load-error").textContent = error.message;
    element("start-load").disabled = false;
  }
});

element("stop-load").addEventListener("click", async () => {
  element("stop-load").disabled = true;
  try {
    await request("/api/load/stop", {});
    await refreshStatus();
  } catch (error) {
    element("load-error").textContent = error.message;
  }
});

function parseInfo(text) {
  const fields = new Map();
  let section = "";
  for (const line of text.split(/\r?\n/)) {
    if (line.startsWith("#")) {
      section = line.slice(1).trim().toLowerCase();
      continue;
    }
    const colon = line.indexOf(":");
    if (colon < 0) continue;
    const name = line.slice(0, colon).trim().toLowerCase();
    const qualified = section ? `${section}.${name}` : name;
    fields.set(qualified, line.slice(colon + 1).trim());
  }
  return fields;
}

function infoField(fields, names) {
  for (const source of names) {
    if (fields.has(source)) return { value: fields.get(source), source: `INFO ${source}` };
  }
  return { value: "未提供", source: "INFO 未提供该字段" };
}

element("refresh-info").addEventListener("click", async () => {
  const button = element("refresh-info");
  button.disabled = true;
  element("info-error").textContent = "";
  try {
    const result = await request("/api/info", {});
    const fields = parseInfo(result.info);
    let keys = infoField(fields, ["database.memory_keys", "memory_keys"]);
    let keysLabel = "内存键数";
    if (keys.value === "未提供") {
      const db0 = infoField(fields, ["keyspace.db0", "db0"]);
      const match = db0.value.match(/(?:^|,)keys=(\d+)(?:,|$)/);
      keys = match ? { value: match[1], source: db0.source } : keys;
      keysLabel = "db0 键数";
    }
    const memory = infoField(fields, ["database.used_memory_bytes", "used_memory_bytes", "memory.used_memory", "used_memory"]);
    const mode = infoField(fields, ["database.mode", "mode"]);
    const writeMode = infoField(fields, ["database.persistence_write_mode", "persistence.persistence_write_mode", "persistence_write_mode"]);
    const durability = infoField(fields, ["database.persistence_durability", "persistence.persistence_durability", "persistence_durability"]);
    element("info-keys-label").textContent = keysLabel;
    element("info-keys").textContent = keys.value;
    element("info-keys-source").textContent = keys.source;
    element("info-memory").textContent = memory.value;
    element("info-memory-source").textContent = memory.source;
    element("info-mode").textContent = mode.value;
    element("info-mode-source").textContent = mode.source;
    element("info-durability").textContent = `写入模式 ${writeMode.value}（${writeMode.source}），耐久性 ${durability.value}（${durability.source}）`;
    element("info-source").textContent = `手动 INFO · ${result.source} · ${result.measured_utc} · ${number(result.elapsed_us, 1)} μs`;
    element("raw-info").textContent = result.info;
  } catch (error) {
    element("info-error").textContent = error.message;
  } finally {
    button.disabled = false;
  }
});

async function refreshHistory() {
  element("history-error").textContent = "";
  try {
    const result = await request("/api/history");
    const select = element("history-files");
    select.replaceChildren(new Option("请选择含 mixed 测量的 JSON 文件", ""));
    for (const file of result.files) select.add(new Option(file.path, file.path));
    element("history-root").textContent = `结果目录 ${result.root}${result.truncated ? " · 已列出前 500 个文件" : ""}`;
  } catch (error) {
    element("history-error").textContent = error.message;
  }
}

element("live-tab").addEventListener("click", () => setView("live"));
element("history-tab").addEventListener("click", () => { setView("history"); refreshHistory(); });
element("refresh-history").addEventListener("click", refreshHistory);
element("open-history").addEventListener("click", async () => {
  const path = element("history-files").value;
  if (!path) return;
  element("history-error").textContent = "";
  try {
    const result = await request(`/api/history/file?path=${encodeURIComponent(path)}`);
    renderMeasurement(result.measurement, "历史测量结果", `历史文件 · ${result.source} · ${result.measurement.started_utc}`, [result.measurement], result.document);
  } catch (error) {
    element("history-error").textContent = error.message;
  }
});

function drawChart(id, series) {
  const canvas = element(id);
  const bounds = canvas.getBoundingClientRect();
  const ratio = Math.min(window.devicePixelRatio || 1, 3);
  canvas.width = Math.round(bounds.width * ratio);
  canvas.height = Math.round(bounds.height * ratio);
  const context = canvas.getContext("2d");
  context.scale(ratio, ratio);
  const width = bounds.width;
  const height = bounds.height;
  const left = 45;
  const right = width - 14;
  const top = 14;
  const bottom = height - 27;
  context.font = '10px "Segoe UI", sans-serif';
  context.fillStyle = "#829095";
  if (!chartPoints.length) {
    context.textAlign = "center";
    context.fillText("完成窗口后显示实际数据", width / 2, height / 2);
    return;
  }
  let maximum = 1;
  for (const point of chartPoints) {
    for (const line of series) {
      if (point[line.key] !== null) maximum = Math.max(maximum, point[line.key]);
    }
  }
  maximum *= 1.12;
  for (let grid = 0; grid <= 3; grid++) {
    const y = bottom - (bottom - top) * grid / 3;
    context.strokeStyle = "#e8eded";
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(left, y);
    context.lineTo(right, y);
    context.stroke();
    context.textAlign = "right";
    context.fillStyle = "#829095";
    const value = maximum * grid / 3;
    context.fillText(value >= 1000 ? `${number(value / 1000, 1)}k` : number(value, value < 10 ? 1 : 0), left - 8, y + 3);
  }
  const first = chartPoints[0].second;
  const last = chartPoints[chartPoints.length - 1].second;
  const xPosition = (point) => first === last ? (left + right) / 2 : left + (point.second - first) / (last - first) * (right - left);
  for (const line of series) {
    context.strokeStyle = line.color;
    context.fillStyle = line.color;
    context.lineWidth = 1.8;
    context.beginPath();
    let connected = false;
    for (const point of chartPoints) {
      const value = point[line.key];
      if (value === null) { connected = false; continue; }
      const x = xPosition(point);
      const y = bottom - value / maximum * (bottom - top);
      if (connected) context.lineTo(x, y);
      else context.moveTo(x, y);
      connected = true;
    }
    context.stroke();
    if (chartPoints.length <= 8) {
      for (const point of chartPoints) {
        if (point[line.key] === null) continue;
        context.beginPath();
        context.arc(xPosition(point), bottom - point[line.key] / maximum * (bottom - top), 2.5, 0, Math.PI * 2);
        context.fill();
      }
    }
  }
  context.fillStyle = "#829095";
  context.textAlign = "center";
  const tickStep = Math.max(1, Math.ceil(chartPoints.length / 5));
  for (let index = 0; index < chartPoints.length; index += tickStep) {
    context.fillText(`${chartPoints[index].second}s`, xPosition(chartPoints[index]), height - 9);
  }
}

function drawCharts() {
  drawChart("throughput-chart", [{ key: "getCount", color: colors.get }, { key: "setCount", color: colors.set }]);
  drawChart("latency-chart", [{ key: "getP99", color: colors.get }, { key: "setP99", color: colors.set }]);
  drawChart("failures-chart", [{ key: "failures", color: colors.failures }]);
}

new ResizeObserver(drawCharts).observe(element("throughput-chart").parentElement);
drawCharts();
refreshStatus();
