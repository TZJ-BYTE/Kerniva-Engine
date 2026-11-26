function node(tag, text, className) {
  const result = document.createElement(tag);
  if (text !== undefined) result.textContent = text;
  if (className) result.className = className;
  return result;
}

function button(text, click, primary = false) {
  const result = node("button", text, primary ? "button primary" : "button");
  result.type = "button";
  result.addEventListener("click", click);
  return result;
}

function input(label, name, value, type = "text") {
  const box = node("label", label);
  const edit = node("input");
  Object.assign(edit, {name, type, value, required: true});
  box.append(edit);
  return box;
}

function persistenceControls({goLogEnabled = true, snapshotEnabled = false}, disabled) {
  const group = node("fieldset", undefined, "instance-persistence");
  group.disabled = disabled;
  group.append(node("legend", "持久化"));
  const checks = {};
  for (const [key, label, checked] of [["goLogEnabled", "增量日志", goLogEnabled],
                                       ["snapshotEnabled", "定时快照", snapshotEnabled]]) {
    const field = node("label", undefined, "checkbox");
    const toggle = node("input");
    Object.assign(toggle, {type: "checkbox", name: key, checked});
    field.append(toggle, document.createTextNode(label));
    group.append(field);
    checks[key] = toggle;
  }
  const values = () => Object.fromEntries(Object.entries(checks).map(([key, toggle]) => [key, toggle.checked]));
  const memory = node("p", "仅内存运行。重启时不会加载旧持久化数据。", "quiet instance-memory-note");
  const showMemory = () => { memory.hidden = checks.goLogEnabled.checked || checks.snapshotEnabled.checked; };
  showMemory();
  group.addEventListener("change", showMemory);
  group.append(memory);
  return {group, values};
}

function pathList(record) {
  const result = node("dl", undefined, "instance-paths");
  for (const [name, value] of [["数据目录", record.dataDir], ["引擎目录", record.engineDir],
                               ["配置文件", record.configPath], ["日志文件", record.logPath]]) {
    result.append(node("dt", name), node("dd", value));
  }
  return result;
}

function newId() {
  const data = crypto.getRandomValues(new Uint8Array(16));
  data[6] = (data[6] & 15) | 64;
  data[8] = (data[8] & 63) | 128;
  const hex = Array.from(data, byte => byte.toString(16).padStart(2, "0")).join("");
  return [hex.slice(0,8), hex.slice(8,12), hex.slice(12,16), hex.slice(16,20), hex.slice(20)].join("-");
}

export async function renderInstances(container, {api, connect}) {
  const page = node("section", undefined, "instance-page");
  const message = node("p", "正在读取本机实例…", "quiet");
  message.setAttribute("role", "status");
  const content = node("div");
  page.append(message, content);
  container.replaceChildren(page);
  let busy = false;
  async function run(action) {
    if (busy) return;
    busy = true;
    content.inert = true;
    message.textContent = "正在处理…";
    try {
      await action();
      if (message.textContent === "正在处理…") message.textContent = "";
    } catch (error) {
      message.textContent = error.message;
    } finally {
      busy = false;
      content.inert = false;
    }
  }
  function form(state) {
    const id = newId();
    const panel = node("form", undefined, "panel instance-form");
    panel.append(node("h2", "部署 Kerniva 单机"));
    const row = node("div", undefined, "form-row host-row");
    const name = input("实例名称", "name", "本地 Kerniva");
    name.querySelector("input").maxLength = 100;
    const port = input("本机端口", "port", String(state.suggestedPort), "number");
    Object.assign(port.querySelector("input"), {min: 1, max: 65535});
    row.append(name, port);
    const persistence = persistenceControls({}, !state.persistenceConfigurable);
    panel.append(row, persistence.group, node("p", "仅监听 127.0.0.1 · 版本 " + state.version, "quiet"));
    if (!state.persistenceConfigurable) panel.append(node("p", "此引擎仅支持默认增量日志。", "quiet"));
    const paths = node("details");
    paths.append(node("summary", "安装和数据位置"));
    for (const field of [input("引擎安装目录", "engineRoot", state.engineRoot),
                         input("实例数据目录（空目录）", "dataDir", state.dataRoot + "\\" + id)]) {
      const row = node("div", undefined, "instance-directory");
      const edit = field.querySelector("input");
      const kind = edit.name === "engineRoot" ? "engine-root" : "data-dir";
      edit.id = "instance-" + kind;
      row.append(field);
      if (location.hostname === "wails.localhost" || location.protocol === "wails:") {
        const chooser = button("选择目录", () => run(async () => {
          const result = await api("/api/desktop/choose-directory", {});
          if (result.path) edit.value = result.path;
        }));
        chooser.id = "choose-" + kind;
        chooser.setAttribute("aria-label", "选择" + field.textContent);
        row.append(chooser);
      }
      paths.append(row);
    }
    const actions = node("div", undefined, "actions");
    const submit = node("button", "安装并启动", "button primary");
    submit.id = "deploy-instance";
    submit.type = "submit";
    actions.append(submit, button("取消", () => run(refresh)));
    panel.append(paths, actions);
    panel.addEventListener("submit", event => {
      event.preventDefault();
      const spec = Object.fromEntries(new FormData(panel));
      delete spec.goLogEnabled;
      delete spec.snapshotEnabled;
      const options = state.persistenceConfigurable ? persistence.values() : {};
      run(async () => {
        await api("/api/instances/deploy", {...spec, ...options, id, port: Number(spec.port)});
        await refresh();
        message.textContent = "实例已启动，可以浏览数据。";
      });
    });
    content.replaceChildren(panel);
    name.querySelector("input").focus();
  }
  function card(record) {
    const panel = node("section", undefined, "panel instance-card");
    const heading = node("div", undefined, "instance-summary");
    const title = node("div");
    title.append(node("h2", record.name), node("code", "127.0.0.1:" + record.port));
    const states = {running: "运行中", unhealthy: "健康检查失败", stopped: "已停止"};
    heading.append(title, node("span", states[record.state], "instance-state " + record.state));
    const actions = node("div", undefined, "actions");
    if (record.state === "running") actions.append(button("浏览数据", () => run(async () => {
      const profile = await api("/api/instances/connection", {id: record.id});
      await connect(profile.id);
    }), true));
    const mutations = record.state === "stopped" ? [["启动", "start"]] : [["停止", "stop"], ["重启", "restart"]];
    if (record.state === "stopped" && !record.installed) mutations.push(["取消未完成登记（保留文件）", "discard"]);
    for (const [label, method] of mutations) {
      actions.append(button(label, () => run(async () => {
        await api("/api/instances/" + method, {id: record.id});
        await refresh();
      })));
    }
    const logs = node("pre", "", "instance-logs");
    logs.hidden = true;
    const logButton = button("查看日志", () => run(async () => {
      if (logs.hidden) {
        const result = await api("/api/instances/logs", {id: record.id});
        logs.textContent = result.text || "暂无日志。";
      }
      logs.hidden = !logs.hidden;
      logButton.textContent = logs.hidden ? "查看日志" : "收起日志";
    }));
    actions.append(logButton);
    const details = node("details");
    details.append(node("summary", "位置与启动方式"), pathList(record),
                   node("p", "以当前用户运行，关闭工作台后继续提供服务。需要无人登录时启动，可由管理员配置 Windows 系统服务。", "quiet"));
    if (record.installed) details.append(button("打开数据目录", () => run(() => api("/api/instances/open-data", {id: record.id}))));
    const editable = record.persistenceConfigurable && record.state === "stopped";
    const persistence = persistenceControls(record, !editable);
    const persistenceHint = !record.persistenceConfigurable ? "此引擎仅支持默认增量日志。" :
      editable ? "保存后在下次启动时生效。" : "停止实例后可修改。";
    panel.append(heading, actions, persistence.group, node("p", persistenceHint, "quiet"));
    if (editable) {
      const save = button("保存持久化设置", () => run(async () => {
        await api("/api/instances/persistence", {id: record.id, ...persistence.values()});
        await refresh();
        message.textContent = "持久化设置已保存。";
      }));
      save.disabled = true;
      persistence.group.addEventListener("change", () => {
        save.disabled = Object.entries(persistence.values()).every(([key, value]) => value === record[key]);
      });
      panel.append(save);
    }
    panel.append(details, logs);
    return panel;
  }
  async function refresh() {
    const state = await api("/api/instances");
    if (!container.contains(page)) return;
    message.textContent = "";
    const bar = node("div", undefined, "instance-toolbar");
    bar.append(node("h2", "本机实例"));
    const controls = node("div", undefined, "actions");
    controls.append(button("刷新", () => run(refresh)));
    if (state.version) controls.append(button("部署 Kerniva", () => form(state), true));
    bar.append(controls);
    content.replaceChildren(bar);
    if (state.reason) content.append(node("p", state.reason, "quiet"));
    if (!state.instances.length && !state.reason) {
      content.append(node("p", "还没有受管实例。可在本机创建一个使用独立数据目录的数据库。", "quiet"));
    }
    for (const record of state.instances) content.append(card(record));
  }
  try {
    await refresh();
  } catch (error) {
    message.textContent = error.message;
    content.append(button("重试", () => run(refresh)));
  }
}

export async function renderDesktopStartup(container, {api}) {
  container.replaceChildren();
  container.hidden = true;
  const state = await api("/api/desktop/startup");
  if (!state.available) return;
  const box = node("label", undefined, "checkbox");
  const toggle = node("input");
  Object.assign(toggle, {type: "checkbox", checked: state.enabled, disabled: state.otherLocation});
  box.append(toggle, document.createTextNode("登录 Windows 时打开此工作台"));
  const status = node("span", state.otherLocation ? "启动项由另一处工作台安装管理。" : "", "quiet");
  status.setAttribute("role", "status");
  toggle.addEventListener("change", async () => {
    toggle.disabled = true;
    try {
      const result = await api("/api/desktop/startup", {enabled: toggle.checked});
      toggle.checked = result.enabled;
      status.textContent = result.enabled ? "已启用" : "已关闭";
    } catch (error) {
      toggle.checked = !toggle.checked;
      status.textContent = error.message;
    } finally {
      toggle.disabled = false;
    }
  });
  container.append(box, status);
  container.hidden = false;
}
