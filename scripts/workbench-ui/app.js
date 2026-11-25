import {createOperations} from "./ops.js";

const byId = (id) => document.getElementById(id);
const profileForm = byId("profile-form");
const connectForm = byId("connect-form");
const scanForm = byId("scan-form");
const exactKeyForm = byId("exact-key-form");
const settingsForm = byId("settings-form");
const initialProfile = {name: "", mode: "standalone", host: "127.0.0.1", port: 6379, db: 0, username: "", tls: false, caFile: "", timeout: 5};
const initialSettings = {pageSize: 50, previewBytes: 4096, formatJson: true, autoRefresh: false, refreshSeconds: 10};
const viewNames = {connections: "连接", keys: "数据浏览", status: "运行状况", replication: "复制控制台", settings: "设置"};
const state = {
  profiles: [], pins: [], settings: initialSettings, ready: false, active: null,
  route: null, editor: null, browser: null, connectionBusy: false, profileBusy: false,
  scan: freshPage(), detail: freshPage(), timer: null, replicationAvailable: false,
};
const operations = createOperations({state, request, routeLink, setCurrent, element, feedback, endpointLabel});

function freshPage() {
  return {history: [null], index: 0, data: null, busy: false, generation: 0, controller: null};
}

function element(tag, content, className) {
  const node = document.createElement(tag);
  if (content !== undefined) node.textContent = content;
  if (className) node.className = className;
  return node;
}

function field(form, key) {
  return form.elements.namedItem(key);
}

function routeLink(view, values = {}) {
  const params = new URLSearchParams();
  for (const [name, value] of Object.entries(values)) {
    if (value !== null && value !== undefined) params.set(name, value);
  }
  return "#/" + view + (params.size ? "?" + params : "");
}

function readRoute() {
  const [page, query] = location.hash.slice(2).split("?", 2);
  return {
    view: location.hash.startsWith("#/") && Object.hasOwn(viewNames, page) ? page : "connections",
    params: new URLSearchParams(query),
  };
}

function go(view, values) {
  const hash = routeLink(view, values);
  if (location.hash !== hash) location.hash = hash;
  else renderRoute();
}

function setCurrent(node, current) {
  if (current) node.setAttribute("aria-current", "page");
  else node.removeAttribute("aria-current");
}

function feedback(id, message = "", error = false) {
  const target = byId(id);
  target.textContent = message;
  target.dataset.kind = error ? "error" : "success";
}

async function request(path, data, signal) {
  const options = {cache: "no-store", signal};
  if (data !== undefined) {
    options.method = "POST";
    options.headers = {"Content-Type": "application/json"};
    options.body = JSON.stringify(data);
  }
  const response = await fetch("/api/" + path, options);
  let result;
  try {
    result = await response.json();
  } catch {
    throw new Error("工作台服务返回了无法读取的响应（HTTP " + response.status + "）。");
  }
  if (!response.ok) {
    const message = typeof result.error === "string" ? result.error : result.error?.message;
    const error = new Error(message || result.message || "请求失败（HTTP " + response.status + "）。");
    error.status = response.status;
    throw error;
  }
  return result;
}

function endpointLabel(profile) {
  const host = profile.host.includes(":") && !profile.host.startsWith("[") ? "[" + profile.host + "]" : profile.host;
  return host + ":" + profile.port;
}

function findProfile(id) {
  return state.profiles.find((profile) => profile.id === id) || null;
}

function selectedProfile() {
  const params = state.route.params;
  if (params.has("new")) return null;
  if (params.has("id")) return findProfile(params.get("id"));
  return findProfile(state.active?.id) || state.profiles[0] || null;
}

function profileLink(profile) {
  const link = element("a", undefined, "profile");
  link.id = "profile-" + profile.id;
  const name = element("span", undefined, "profile-name");
  const active = state.active?.id === profile.id;
  name.append(element("i", undefined, "profile-dot"), document.createTextNode(profile.name));
  name.firstChild.setAttribute("aria-hidden", "true");
  link.href = routeLink("connections", {id: profile.id});
  link.dataset.connected = active;
  setCurrent(link, state.route.view === "connections" && selectedProfile()?.id === profile.id);
  const endpoint = endpointLabel(profile) + " · " + (profile.mode === "cluster" ? "Redis Cluster" : "默认 DB " + profile.db);
  const meta = element("span", undefined, "profile-meta");
  meta.append(element("code", endpoint), document.createTextNode(active ? " · 已连接" : ""));
  link.append(name, meta);
  return link;
}

function renderProfiles() {
  byId("profile-count").textContent = state.ready ? state.profiles.length + " 项" : "";
  const placeholder = state.ready ? "还没有保存的连接。" : "正在读取连接配置…";
  const links = state.profiles.map(profileLink);
  byId("profile-list").replaceChildren(...(links.length ? links : [element("p", placeholder, "panel-body quiet")]));
}

function renderConnection() {
  const status = byId("connection-status");
  status.dataset.connected = !!state.active;
  const displayName = findProfile(state.active?.id)?.name || state.active?.profile.name;
  const label = state.connectionBusy ? "正在处理连接…" : (state.active ? "已连接「" + displayName + "」 · 当前库 DB " + state.active.db : "未连接");
  if (status.lastElementChild.textContent !== label) status.lastElementChild.textContent = label;
  const editorId = state.editor?.id;
  const active = !!editorId && state.active?.id === editorId;
  const busy = state.connectionBusy || state.profileBusy;
  byId("connect-fields").disabled = busy || active;
  byId("connect-fields").hidden = active;
  byId("connect-button").hidden = active;
  byId("connect-button").disabled = busy;
  byId("disconnect-button").hidden = !active;
  byId("disconnect-button").disabled = busy;
  byId("open-browser").hidden = !active;
  byId("open-browser").href = routeLink("keys", {id: editorId});
  operations.connectionChanged();
}

function fillForm(form, data) {
  for (const [name, value] of Object.entries(data)) {
    const input = field(form, name);
    if (!input) continue;
    if (input.type === "checkbox") input.checked = value;
    else input.value = value ?? "";
  }
}

function modeChanged() {
  const cluster = field(profileForm, "mode").value === "cluster";
  const db = field(profileForm, "db");
  if (cluster) db.value = "0";
  db.disabled = cluster;
}

function tlsChanged() {
  const enabled = field(profileForm, "tls").checked;
  byId("ca-field").hidden = !enabled;
  field(profileForm, "caFile").disabled = !enabled;
}

function renderEditor() {
  const profile = selectedProfile();
  const id = profile?.id || null;
  if (!state.editor || state.editor.id !== id) {
    state.editor = {id};
    byId("profile-options").open = !profile;
    fillForm(profileForm, {...initialProfile, ...profile});
    field(connectForm, "password").value = "";
    modeChanged();
    tlsChanged();
    byId("confirm-delete").hidden = true;
    feedback("profile-feedback");
    feedback("connection-feedback");
  }
  byId("profile-summary").hidden = !profile;
  byId("profile-heading").textContent = profile?.name || "新建连接";
  byId("profile-fields").disabled = !state.ready || state.profileBusy;
  byId("save-profile").disabled = !state.ready || state.profileBusy || state.connectionBusy;
  byId("delete-profile").disabled = state.profileBusy || state.connectionBusy;
  byId("delete-profile").hidden = !profile;
  connectForm.hidden = !profile;
  renderConnection();
}

let mountedPanel = null;

function isInstances(route) {
  return route?.view === "connections" && route.params.get("tab") === "instances";
}

async function mountLocalPanel(id) {
  if (mountedPanel?.id === id) return;
  mountedPanel?.container.replaceChildren();
  mountedPanel = null;
  if (!id || !state.ready) return;
  const container = byId(id);
  const panel = element("div");
  const mount = {id, container};
  mountedPanel = mount;
  container.replaceChildren(panel);
  container.hidden = false;
  panel.hidden = id === "desktop-startup";
  try {
    const {renderInstances, renderDesktopStartup} = await import("./instances.js");
    if (mountedPanel !== mount) return;
    const api = (path, body) => request(path.replace(/^\/api\//, ""), body);
    if (id === "local-instances") await renderInstances(panel, {api, connect: openManagedConnection});
    else await renderDesktopStartup(panel, {api});
  } catch (error) {
    if (mountedPanel !== mount) return;
    panel.hidden = false;
    const message = element("p", error.message, "inline-error");
    message.setAttribute("role", "alert");
    const retry = element("button", "重试", "button");
    retry.type = "button";
    retry.onclick = () => { mountedPanel = null; void mountLocalPanel(id); };
    panel.replaceChildren(message, retry);
  }
}

async function openManagedConnection(id) {
  const result = await request("state");
  if (!isInstances(state.route)) return;
  state.profiles = result.profiles;
  if (!findProfile(id)) throw new Error("未找到此实例的连接配置。");
  if (state.active?.id === id) return go("keys", {id, db: state.active.db});
  history.pushState(null, "", routeLink("connections", {id, browse: new URLSearchParams({id}).toString()}));
  renderRoute(true);
  field(connectForm, "password").value = "";
  await connectProfile();
  if (state.route.view === "connections" && state.editor?.id === id && state.active?.id !== id) field(connectForm, "password").focus();
}

function renderRoute(focus = false) {
  const oldRoute = state.route;
  state.route = readRoute();
  const {view} = state.route;
  for (const section of document.querySelectorAll("[data-view]")) section.hidden = section.dataset.view !== view;
  for (const link of document.querySelectorAll("[data-nav]")) setCurrent(link, link.dataset.nav === view);
  byId("view-name").textContent = viewNames[view];
  document.title = "Kerniva · " + viewNames[view];
  const instances = isInstances(state.route);
  byId("local-instances").hidden = !instances;
  byId("new-profile").hidden = instances;
  byId("saved-connections").hidden = instances;
  setCurrent(byId("instances-tab"), instances);
  setCurrent(byId("saved-tab"), !instances);
  void mountLocalPanel(instances ? "local-instances" : (view === "settings" ? "desktop-startup" : null));
  if (view === "connections") {
    if (!instances) {
      renderProfiles();
      renderEditor();
      if (focus && state.route.params.has("new")) field(profileForm, "name").focus();
    }
  } else if (view === "keys") {
    renderBrowser();
    if (state.ready && oldRoute?.view !== "keys") void refreshPins();
  }
  if (oldRoute?.view === "keys" && view !== "keys") {
    invalidate(state.scan);
    invalidate(state.detail);
  }
  if (oldRoute?.view === "connections" && (view !== "connections" || instances)) field(connectForm, "password").value = "";
  if (focus && oldRoute?.view !== view && !(view === "connections" && state.route.params.has("new"))) {
    document.querySelector("[data-view]:not([hidden]) h1")?.focus();
  }
  if (focus && oldRoute?.view === "keys" && view === "keys" &&
      (oldRoute.params.get("key") !== state.route.params.get("key") || oldRoute.params.get("pin") !== state.route.params.get("pin"))) {
    highlightKey();
  }
  operations.navigate();
  scheduleRefresh();
}

function highlightKey() {
  const detail = byId("detail-heading");
  detail.tabIndex = -1;
  detail.focus();
}

async function bootstrap() {
  byId("bootstrap-error").hidden = true;
  byId("bootstrap-retry").disabled = true;
  try {
    const result = await request("state");
    state.profiles = result.profiles;
    state.pins = result.pins || [];
    state.settings = {...initialSettings, ...result.settings};
    state.ready = true;
    fillForm(settingsForm, state.settings);
    byId("settings-fields").disabled = false;
    byId("save-settings").disabled = false;
    state.replicationAvailable = result.replicationAvailable === true;
  } catch (error) {
    byId("bootstrap-message").textContent = error.message;
    byId("bootstrap-error").hidden = false;
  } finally {
    byId("bootstrap-retry").disabled = false;
    renderRoute();
  }
}

function profilePayload() {
  const data = {};
  for (const name of Object.keys(initialProfile)) {
    const input = field(profileForm, name);
    if (input.type === "checkbox") data[name] = input.checked;
    else if (input.type === "number") data[name] = input.valueAsNumber;
    else data[name] = input.value.trim();
  }
  if (!data.tls) data.caFile = "";
  if (state.editor.id) data.id = state.editor.id;
  return data;
}

async function terminateSession(session) {
  await request("disconnect", {session});
}

function dropActive() {
  state.active = null;
  invalidate(state.scan);
  invalidate(state.detail);
  state.browser = null;
  renderConnection();
  if (state.route.view === "keys") renderBrowser();
  scheduleRefresh();
}

function sameProfile(one, other) {
  return Object.keys(initialProfile).every((key) => one[key] === other[key]);
}

profileForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (state.connectionBusy || state.profileBusy || !state.ready) return;
  const profile = profilePayload();
  if (!profile.name || !profile.host) {
    feedback("profile-feedback", "请填写连接名称和主机。", true);
    return;
  }
  const editor = state.editor;
  state.profileBusy = true;
  renderEditor();
  feedback("profile-feedback", "正在保存…");
  try {
    const result = await request("profile/save", {profile});
    const saved = result.profile;
    const index = state.profiles.findIndex((item) => item.id === saved.id);
    if (index === -1) state.profiles.push(saved);
    else state.profiles[index] = saved;
    const active = state.active;
    const updatedProfile = active?.id === saved.id && !sameProfile(active.profile, saved);
    if (updatedProfile) {
      dropActive();
      terminateSession(active.session).catch(() => {});
    }
    if (state.editor === editor) {
      const message = updatedProfile ? "配置已保存。请重新连接以应用更改。" : "配置已保存。";
      if (!editor.id && state.route.view === "connections") {
        history.replaceState(null, "", routeLink("connections", {id: saved.id}));
        renderRoute();
      } else if (state.route.view === "connections") {
        fillForm(profileForm, {...initialProfile, ...saved});
        modeChanged();
        tlsChanged();
      }
      if (state.route.view === "connections") feedback("profile-feedback", message);
    }
    renderProfiles();
    renderConnection();
  } catch (error) {
    if (state.editor === editor) feedback("profile-feedback", error.message, true);
  } finally {
    state.profileBusy = false;
    if (state.route.view === "connections") renderEditor();
  }
});

byId("delete-profile").addEventListener("click", () => {
  byId("confirm-delete").hidden = false;
  byId("confirm-delete-button").focus();
});
byId("cancel-delete").addEventListener("click", () => {
  byId("confirm-delete").hidden = true;
  byId("delete-profile").focus();
});
byId("confirm-delete-button").addEventListener("click", async () => {
  if (state.connectionBusy || state.profileBusy) return;
  const editor = state.editor;
  const id = editor?.id;
  if (!id) return;
  const confirm = byId("confirm-delete-button");
  confirm.disabled = true;
  state.profileBusy = true;
  renderEditor();
  feedback("profile-feedback", "正在删除…");
  try {
    const result = await request("profile/delete", {id});
    if (state.active?.id === id) {
      const {session} = state.active;
      dropActive();
      terminateSession(session).catch(() => {});
    }
    state.profiles = state.profiles.filter((profile) => profile.id !== id);
    state.pins = state.pins.filter((pin) => pin.profileId !== id);
    if (state.editor === editor && state.route.view === "connections") {
      history.replaceState(null, "", routeLink("connections", state.profiles.length ? {} : {new: 1}));
      renderRoute();
      feedback("profile-feedback", result.removedPins ? "连接配置及其 " + result.removedPins + " 个常用键已移除。" : "连接配置已删除。");
      document.querySelector("#profile-list a")?.focus();
    } else {
      renderProfiles();
    }
  } catch (error) {
    if (state.editor === editor) feedback("profile-feedback", error.message, true);
  } finally {
    confirm.disabled = false;
    state.profileBusy = false;
    if (state.route.view === "connections") renderEditor();
  }
});

function linkedBrowserRoute(profileId) {
  const desired = new URLSearchParams(location.hash.slice(2).split("?", 2)[1]).get("browse");
  if (desired) {
    const params = new URLSearchParams(desired);
    if (params.get("id") === profileId) return Object.fromEntries(params);
  }
  return {id: profileId};
}

async function connectProfile() {
  const profile = findProfile(state.editor?.id);
  if (state.connectionBusy || state.profileBusy || !profile || state.active?.id === profile.id) return;
  const editor = state.editor;
  const passwordField = field(connectForm, "password");
  const connectRequest = request("connect", {id: profile.id, password: passwordField.value});
  passwordField.value = "";
  state.connectionBusy = true;
  renderEditor();
  feedback("connection-feedback", "正在连接已保存的配置…");
  try {
    const result = await connectRequest;
    const old = state.active;
    state.active = {id: profile.id, profile, db: profile.db, session: result.session, info: result.info};
    if (old && old.session !== result.session) terminateSession(old.session).catch(() => {});
    if (state.route.view === "keys") renderBrowser();
    if (state.editor === editor && state.route.view === "connections" && !isInstances(state.route)) {
      const params = linkedBrowserRoute(profile.id);
      byId("open-browser").href = routeLink("keys", params);
      feedback("connection-feedback", "连接成功。");
      if (state.route.params.has("browse")) go("keys", params);
    }
  } catch (error) {
    if (state.editor === editor) feedback("connection-feedback", error.message, true);
  } finally {
    state.connectionBusy = false;
    renderProfiles();
    if (state.route.view === "connections") renderEditor();
    else renderConnection();
  }
}

connectForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void connectProfile();
});

byId("disconnect-button").addEventListener("click", async () => {
  const active = state.active;
  if (!active || state.connectionBusy || state.profileBusy) return;
  const editor = state.editor;
  state.connectionBusy = true;
  renderEditor();
  feedback("connection-feedback", "正在断开连接…");
  try {
    await terminateSession(active.session);
    if (state.active === active) dropActive();
    if (state.editor === editor) feedback("connection-feedback", "连接已断开。");
  } catch (error) {
    if (state.editor === editor) feedback("connection-feedback", error.message, true);
  } finally {
    state.connectionBusy = false;
    renderProfiles();
    if (state.route.view === "connections") renderEditor();
    else renderConnection();
  }
});

function invalidate(page) {
  page.generation++;
  page.controller?.abort();
  page.controller = null;
  page.busy = false;
}

function browserParams() {
  const params = state.route.params;
  const pin = state.pins.find((item) => item.id === params.get("pin"));
  return {id: pin?.profileId || params.get("id") || state.active?.id, pin, pattern: params.get("pattern") || "*",
    node: params.get("node") || "", key: pin?.key_b64 ?? params.get("key")};
}

function databaseRoute(active, db) {
  return routeLink("keys", {id: active.id, db, pattern: state.route.params.get("pattern"), node: state.route.params.get("node")});
}

async function selectDatabase(active, db, fromLink = false) {
  if (active.switching) return;
  const originalHash = location.hash;
  active.switching = true;
  invalidate(state.scan);
  invalidate(state.detail);
  byId("browser-results").hidden = true;
  byId("session-db").disabled = true;
  byId("switch-db").disabled = true;
  feedback("database-feedback", "正在打开 DB " + db + "…");
  try {
    const result = await request("select-db", {session: active.session, db});
    if (state.active !== active) return;
    active.db = result.db;
    active.info = result.info;
    active.dbMessage = "";
    if (!fromLink && location.hash === originalHash) history.pushState(null, "", databaseRoute(active, active.db));
  } catch (error) {
    if (state.active !== active) return;
    if (error.status === 401 || error.status === 410) {
      dropActive();
      return;
    }
    active.dbMessage = "无法打开 DB " + db + "。 " + error.message;
    if (fromLink && location.hash === originalHash) history.replaceState(null, "", databaseRoute(active, active.db));
  } finally {
    active.switching = false;
    if (state.active === active) {
      renderConnection();
      if (state.route.view === "keys") renderRoute();
    }
  }
}

function keyLabel(item) {
  if (item.key === null || /[\x00-\x1f\x7f-\x9f]/.test(item.key) || (item.key && !item.key.trim())) {
    return "Base64 · " + item.key_b64;
  }
  return item.key || "（空键名）";
}

function pinLabel(pin) {
  let key = null;
  try {
    const bytes = Uint8Array.from(atob(pin.key_b64), (ch) => ch.charCodeAt(0));
    key = new TextDecoder("utf-8", {fatal: true, ignoreBOM: true}).decode(bytes);
  } catch { /* A binary key is displayed in Base64. */ }
  return keyLabel({key, key_b64: pin.key_b64});
}

function pinRoute(pin) {
  return {id: pin.profileId, db: pin.db, pin: pin.id};
}

let pinGeneration = 0;
let pinListSignature;
let pinBusy = false;

function renderPins() {
  const pins = state.pins;
  const signature = JSON.stringify(pins.map((pin) => [pin.id, pin.key_b64, pin.db, findProfile(pin.profileId)?.name]));
  if (pinListSignature === signature) return;
  pinListSignature = signature;
  byId("pin-count").textContent = pins.length ? pins.length + " 项" : "";
  byId("pins-empty").hidden = !!pins.length;
  const items = pins.map((pin) => {
    const row = element("li");
    const profile = findProfile(pin.profileId);
    const name = pinLabel(pin);
    const open = element("a", undefined, "pin-open");
    open.id = "pin-open-" + pin.id;
    open.href = routeLink("keys", pinRoute(pin));
    open.dataset.pin = pin.id;
    open.append(element("code", name), element("small", (profile?.name || "已删除的连接") + " · DB " + pin.db));
    const remove = element("button", "移除", "pin-remove");
    remove.id = "pin-remove-" + pin.id;
    remove.type = "button";
    remove.dataset.removePin = pin.id;
    remove.setAttribute("aria-label", "移除常用键 " + name + "，DB " + pin.db);
    row.append(open, remove);
    return row;
  });
  byId("pin-list").replaceChildren(...items);
}

async function refreshPins() {
  const generation = ++pinGeneration;
  try {
    const data = await request("state");
    if (generation !== pinGeneration) return false;
    state.pins = data.pins || [];
    state.profiles = data.profiles;
    renderPins();
    if (state.route.view === "keys") renderPinButton();
    return true;
  } catch (error) {
    if (generation === pinGeneration) feedback("pin-feedback", error.message, true);
    return false;
  }
}

async function openPin(id) {
  feedback("pin-feedback");
  if (!await refreshPins() || state.route.view !== "keys") return;
  const pin = state.pins.find((item) => item.id === id);
  if (!pin || !findProfile(pin.profileId)) {
    feedback("pin-feedback", "该连接或常用键已删除。", true);
    return;
  }
  const target = pinRoute(pin);
  if (state.active?.id === pin.profileId && !sameProfile(state.active.profile, findProfile(pin.profileId))) {
    const session = state.active.session;
    dropActive();
    terminateSession(session).catch(() => {});
  }
  if (state.active?.id === pin.profileId) {
    invalidate(state.detail);
    state.detail = freshPage();
    go("keys", target);
    return;
  }
  history.pushState(null, "", routeLink("connections", {id: pin.profileId, browse: new URLSearchParams(target).toString()}));
  renderRoute(true);
  field(connectForm, "password").value = "";
  await connectProfile();
  if (state.route.view === "connections" && state.editor?.id === pin.profileId && state.active?.id !== pin.profileId) {
    field(connectForm, "password").focus();
  }
}

function currentPin() {
  const browse = state.browser;
  return browse && state.pins.find((pin) => pin.profileId === browse.id && pin.db === browse.db && pin.key_b64 === browse.key);
}

function renderPinButton() {
  const button = byId("pin-current");
  const page = state.detail;
  const browse = state.browser;
  button.hidden = !browse || browse.key === null || page.error || !page.data || page.data.type === "none" || !page.data.type;
  const pinned = Boolean(currentPin());
  button.textContent = pinned ? "已固定" : "固定";
  button.setAttribute("aria-pressed", String(pinned));
  button.disabled = pinBusy || page.busy;
}

async function mutatePin(id) {
  if (pinBusy) return;
  const browse = state.browser;
  const current = currentPin();
  pinBusy = true;
  ++pinGeneration;
  renderPinButton();
  try {
    const result = id ? await request("pins/remove", {id}) : await request("pins/add", {
      session: browse.session, profileId: browse.id, db: browse.db, key_b64: browse.key,
    });
    ++pinGeneration;
    state.pins = result.pins;
    if (id && id === state.route.params.get("pin") && current?.id === id && browse) {
      history.replaceState(null, "", routeLink("keys", {id: browse.id, db: browse.db, key: browse.key}));
      state.route = readRoute();
      state.browser = {...browse, pin: null};
    }
    renderPins();
    feedback("pin-feedback", id ? (result.removed ? "已移除常用键。" : "该常用键已不存在。") : "已加入常用键。");
  } catch (error) {
    feedback("pin-feedback", error.message, true);
  } finally {
    pinBusy = false;
    renderPinButton();
  }
}

byId("pin-list").addEventListener("click", (event) => {
  const open = event.target.closest("a[data-pin]");
  if (open && !event.ctrlKey && !event.metaKey && !event.shiftKey && !event.altKey) {
    event.preventDefault();
    void openPin(open.dataset.pin);
  }
  const remove = event.target.closest("button[data-remove-pin]");
  if (remove) void mutatePin(remove.dataset.removePin);
});
byId("pin-current").addEventListener("click", () => void mutatePin(currentPin()?.id));
byId("pins").addEventListener("toggle", () => {
  if (byId("pins").open && state.ready && state.route.view === "keys") void refreshPins();
});
window.addEventListener("focus", () => {
  if (state.ready && state.route.view === "keys") void refreshPins();
});

function renderBrowser() {
  renderPins();
  const params = browserParams();
  for (const link of byId("pin-list").querySelectorAll("a")) setCurrent(link, link.dataset.pin === params.pin?.id);
  const missingPin = state.ready && state.route.params.has("pin") && !params.pin;
  if (missingPin) feedback("pin-feedback", "该连接或常用键已删除。", true);
  const active = state.active && state.active.id === params.id ? state.active : null;
  byId("browser-offline").hidden = !!active;
  byId("browser-online").hidden = !active;
  byId("refresh").hidden = !active;
  if (!active) {
    invalidate(state.scan);
    invalidate(state.detail);
    state.browser = null;
    byId("browser-endpoint").textContent = "扫描并查看键值。";
    const profile = findProfile(params.id);
    byId("offline-message").textContent = profile ? "请连接「" + profile.name + "」后继续浏览。" : "选择并连接数据库后即可浏览键。";
    byId("offline-link").href = routeLink("connections", profile ? {id: profile.id, browse: state.route.params.toString()} : {});
    return;
  }
  const nodes = active.info?.nodes || [];
  const cluster = active.profile.mode === "cluster";
  byId("database-form").hidden = cluster;
  byId("database-cluster").hidden = !cluster;
  byId("browser-results").hidden = Boolean(active.switching);
  const input = byId("session-db");
  input.disabled = Boolean(active.switching);
  byId("switch-db").disabled = Boolean(active.switching);
  const requested = params.pin ? String(params.pin.db) : state.route.params.get("db");
  if (active.switching) return;
  if (requested !== null && requested !== String(active.db)) {
    if (/^\d{1,5}$/.test(requested) && Number(requested) <= 65535 && Number(requested) !== active.db) {
      void selectDatabase(active, Number(requested), true);
      return;
    }
    history.replaceState(null, "", databaseRoute(active, active.db));
    renderRoute();
    return;
  }
  if (state.browser?.db !== active.db || document.activeElement !== input) input.value = active.db;
  feedback("database-feedback", active.dbMessage || "", Boolean(active.dbMessage));
  const node = cluster && nodes.some((item) => item.id === params.node) ? params.node : "";
  const browser = state.browser;
  const changedSession = browser?.session !== active.session || browser.db !== active.db;
  const changedScan = changedSession || browser.pattern !== params.pattern || browser.node !== node;
  const changedKey = changedSession || browser.key !== params.key;
  state.browser = {...params, node, session: active.session, db: active.db};
  const endpoint = endpointLabel(active.profile);
  byId("browser-endpoint").textContent = "连接「" + active.profile.name + "」 · " + endpoint + " · " + (cluster ? "Redis Cluster" : "单机");
  if (changedScan) {
    field(scanForm, "pattern").value = params.pattern;
    const list = field(scanForm, "node");
    byId("node-field").hidden = !cluster || !nodes.length;
    const options = nodes.map((item) => {
      const option = element("option", item.label || endpointLabel(item));
      option.value = item.id;
      return option;
    });
    const all = element("option", "全部主节点");
    all.value = "";
    list.replaceChildren(all, ...options);
    list.value = node;
    invalidate(state.scan);
    state.scan = freshPage();
  }
  if (changedKey) {
    invalidate(state.detail);
    state.detail = freshPage();
  }
  renderScan();
  renderDetail();
  if (!params.pin && (changedScan || !state.scan.data)) void loadPage("scan", state.scan.index);
  if (params.key !== null && (changedKey || !state.detail.data)) void loadPage("detail", state.detail.index);
}

function paginate(kind) {
  const page = state[kind];
  const done = page.data?.done || page.data?.cursor == null;
  byId(kind + "-pages").hidden = !page.data || (done && page.index === 0);
  byId(kind + "-prev").disabled = page.busy || page.index === 0;
  byId(kind + "-next").disabled = page.busy || done;
  byId(kind + "-page").textContent = "第 " + (page.index + 1) + " 页";
}

function renderScan() {
  const page = state.scan;
  const area = byId("key-list");
  const items = [...new Map((page.data?.items || []).map((item) => [item.key_b64, item])).values()];
  const error = page.error || page.data?.unsupported;
  const existing = new Map([...area.querySelectorAll("a")].map((item) => [item.dataset.key, item]));
  const empty = !page.data ? "点击「扫描」查看此库中的键。" : page.data.done ? "此页没有匹配的键。" : "此页没有匹配的键，可继续扫描下一页。";
  const rendered = (error || !items.length) ? [
    element("p", error || (page.busy ? "正在扫描…" : empty), "panel-body quiet" + (error ? " inline-error" : "")),
  ] : items.map((item) => {
    const link = existing.get(item.key_b64) || element("a", keyLabel(item), "key-link");
    const params = state.browser;
    link.dataset.key = item.key_b64;
    link.href = routeLink("keys", {id: params.id, db: params.db, pattern: params.pattern, node: params.node || null, key: item.key_b64});
    setCurrent(link, params.key === item.key_b64);
    return link;
  });
  const status = byId("scan-status");
  const message = page.busy ? "正在扫描…" : error ? "扫描失败" : page.data ? items.length + " 个键" : "";
  if (status.textContent !== message) status.textContent = message;
  if (rendered.length !== area.childElementCount || rendered.some((node, index) => node !== area.children[index])) {
    const focusedKey = area.contains(document.activeElement) ? document.activeElement.dataset.key : null;
    area.replaceChildren(...rendered);
    if (focusedKey != null) rendered.find((node) => node.dataset.key === focusedKey)?.focus({preventScroll: true});
  }
  paginate("scan");
}

function detailPart(label, value) {
  const item = element("div");
  item.append(element("dt", label), element("dd", value));
  return item;
}

function presentation(value, allowJson = false) {
  if (typeof value?.text !== "string" || /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/.test(value.text)) {
    return {text: value?.b64 ?? "—", format: "Base64"};
  }
  if (allowJson && !value.truncated) {
    try {
      const parsed = JSON.parse(value.text);
      return {
        text: state.settings.formatJson && parsed !== null && typeof parsed === "object" ? formatJson(value.text) : value.text,
        format: "JSON",
      };
    } catch {
      // 普通文本也使用 Redis string 类型。
    }
  }
  return {text: value.text, format: "文本"};
}

function formatJson(text) {
  // 验证由调用者完成。逐个保留词法单元，以免大整数、转义或重复字段被重写。
  const tokens = text.match(/"(?:[^"\\]|\\.)*"|[^\s{}\[\],:]+|[{}\[\],:]/g) || [];
  let depth = 0;
  let output = "";
  const newline = () => "\n" + "  ".repeat(Math.min(depth, 100));
  for (const [index, token] of tokens.entries()) {
    const previous = tokens[index - 1];
    const next = tokens[index + 1];
    if (token === "{" || token === "[") {
      output += token;
      depth++;
      if (next !== "}" && next !== "]") output += newline();
    } else if (token === "}" || token === "]") {
      depth--;
      if (previous !== "{" && previous !== "[") output += newline();
      output += token;
    } else if (token === ",") {
      output += "," + newline();
    } else {
      output += token === ":" ? ": " : token;
    }
  }
  return output;
}

function valueBlock(value, options = {}) {
  const display = presentation(value, options.allowJson);
  const box = element("div", undefined, "value-box");
  const code = element("pre", display.text === "" ? "（空值）" : display.text, "detail-value");
  box.append(code);
  const qualifiers = [];
  if (display.format === "Base64") qualifiers.push("Base64 二进制");
  if (options.range) qualifiers.push(options.range);
  else if (value?.truncated) qualifiers.push("已截断 · 仅显示前 " + state.settings.previewBytes + " 字节内的数据");
  if (qualifiers.length) box.append(element("small", qualifiers.join(" · "), "preview-caption"));
  return box;
}

function pairValue(label, value) {
  const entry = element("div");
  entry.append(element("p", label, "entry-label"), valueBlock(value));
  return entry;
}

function hashEntry(item) {
  const pair = element("div", undefined, "entry-pair");
  pair.append(pairValue("字段", item.field), pairValue("值", item.value));
  return pair;
}

function collectionEntry(type, item) {
  const row = element("li");
  if (type === "hash") {
    row.append(hashEntry(item));
  } else if (type === "zset") {
    const pair = element("div", undefined, "entry-pair");
    const score = element("div");
    score.append(element("p", "分值", "entry-label"), element("code", item.score));
    pair.append(pairValue("成员", item.member), score);
    row.append(pair);
  } else if (type === "stream") {
    const fields = element("div", undefined, "stream-fields");
    fields.append(...item.fields.map(hashEntry));
    row.append(element("code", item.id), fields);
  } else {
    row.append(valueBlock(item.value));
  }
  return row;
}

function renderDetail() {
  const page = state.detail;
  const area = byId("key-detail");
  const status = byId("detail-status");
  const message = page.busy ? "正在读取…" : page.error ? "读取失败" : "";
  if (status.textContent !== message) status.textContent = message;
  if (page.error) {
    area.replaceChildren(element("p", page.error, "inline-error"));
  } else if (!page.data) {
    area.replaceChildren(element("p", page.busy ? "正在读取键…" : "从列表选择一个键查看详情。", "quiet"));
  } else {
    const data = page.data;
    const heading = element("h3", undefined, "detail-title");
    heading.append(element("code", keyLabel(data)));
    area.replaceChildren(heading);
    if (data.type === "none") {
      area.append(element("p", "该键不存在，可能已经过期或被删除。", "detail-note"));
    } else {
      const meta = element("dl", undefined, "detail-meta");
      const chunked = data.type === "string" && (data.offset > 0 || data.value?.truncated);
      const type = data.type === "string" && data.value ? "string · " + presentation(data.value, !chunked).format : data.type || "未知";
      const ttl = data.ttl_ms === -1 ? "永久" : Number.isFinite(data.ttl_ms) && data.ttl_ms >= 0 ? data.ttl_ms + " ms" : "—";
      meta.append(detailPart("类型", type), detailPart("TTL", ttl));
      if (Number.isFinite(data.length)) meta.append(detailPart("大小", data.length + " 字节"));
      area.append(meta);
      if (data.unsupported) {
        area.append(element("p", data.unsupported, "detail-note"));
      } else if (data.type === "string" && data.value) {
        const end = Math.min(data.length, data.offset + state.settings.previewBytes);
        const range = chunked ? "分段预览 · 第 " + (data.offset + 1) + "–" + end + " 字节" : "";
        area.append(valueBlock(data.value, {allowJson: !chunked, range}));
      } else if (data.items?.length) {
        const list = element("ul", undefined, "entries");
        list.append(...data.items.map((item) => collectionEntry(data.type, item)));
        area.append(list);
      } else {
        area.append(element("p", data.done ? "此页没有条目。" : "此页没有条目，可继续读取下一页。", "quiet"));
      }
    }
  }
  paginate("detail");
  renderPinButton();
}

async function loadPage(kind, index, cursor) {
  if (state.route.view !== "keys" || !state.browser || state.active?.switching) return;
  const page = state[kind];
  if (page.busy) return;
  const browser = state.browser;
  const detail = kind === "detail";
  if (detail && browser.key === null) return;
  const render = detail ? renderDetail : renderScan;
  const controller = new AbortController();
  const generation = ++page.generation;
  page.controller = controller;
  page.busy = true;
  page.error = null;
  render();
  const payload = {session: browser.session, count: state.settings.pageSize};
  const nextCursor = cursor ?? page.history[index];
  if (nextCursor != null) payload.cursor = nextCursor;
  if (detail) {
    payload.key_b64 = browser.key;
    payload.previewBytes = state.settings.previewBytes;
  } else {
    payload.pattern = browser.pattern;
    if (browser.node) payload.node = browser.node;
  }
  try {
    const data = await request(detail ? "key" : "scan", payload, controller.signal);
    if (page.generation !== generation) return;
    const newCursor = page.history[index] !== (nextCursor ?? null);
    if (newCursor || page.data?.cursor !== data.cursor) page.history.length = index + 1;
    page.history[index] = nextCursor ?? null;
    page.index = index;
    page.data = data;
  } catch (error) {
    if (page.generation !== generation) return;
    if (error.status === 401 || error.status === 410) {
      dropActive();
      byId("offline-message").textContent = "连接会话已失效，请重新连接。";
      return;
    }
    page.error = error.message;
  } finally {
    if (page.generation === generation) {
      page.busy = false;
      page.controller = null;
      render();
    }
  }
}

function refresh() {
  if (!state.browser) return;
  if (!state.browser.pin || state.scan.data) void loadPage("scan", state.scan.index);
  void loadPage("detail", state.detail.index);
}

function scheduleRefresh() {
  clearInterval(state.timer);
  if (state.route.view === "keys" && state.active && state.settings.autoRefresh) {
    state.timer = setInterval(() => {
      if (!document.hidden) refresh();
    }, state.settings.refreshSeconds * 1000);
  }
}

for (const kind of ["scan", "detail"]) {
  byId(kind + "-prev").addEventListener("click", () => {
    const page = state[kind];
    if (page.index > 0) void loadPage(kind, page.index - 1);
  });
  byId(kind + "-next").addEventListener("click", () => {
    const page = state[kind];
    if (!page.data?.done && page.data?.cursor != null) void loadPage(kind, page.index + 1, page.data.cursor);
  });
}

byId("database-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const active = state.active;
  const db = byId("session-db").valueAsNumber;
  if (active && active.db !== db) void selectDatabase(active, db);
});

scanForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const pattern = field(scanForm, "pattern").value || "*";
  const node = field(scanForm, "node").value || null;
  const current = state.browser;
  if (!current) return;
  if (current.pattern === pattern && current.node === (node || "")) {
    invalidate(state.scan);
    state.scan = freshPage();
    void loadPage("scan", 0);
  } else {
    go("keys", {id: current.id, db: current.db, pattern, node});
  }
});

function encodeKey(text) {
  const bytes = new TextEncoder().encode(text);
  const chunks = [];
  for (let offset = 0; offset < bytes.length; offset += 8192) {
    chunks.push(String.fromCharCode(...bytes.subarray(offset, offset + 8192)));
  }
  return btoa(chunks.join(""));
}

exactKeyForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const browser = state.browser;
  if (!browser) return;
  const key = encodeKey(field(exactKeyForm, "key").value);
  if (key === browser.key) {
    invalidate(state.detail);
    state.detail = freshPage();
    void loadPage("detail", 0);
    highlightKey();
    return;
  }
  go("keys", {id: browser.id, db: browser.db, pattern: browser.pattern, node: browser.node || null, key});
});

settingsForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const save = byId("save-settings");
  if (!state.ready || save.disabled) return;
  const settings = {};
  for (const key of Object.keys(initialSettings)) {
    const input = field(settingsForm, key);
    settings[key] = input.type === "checkbox" ? input.checked : input.valueAsNumber;
  }
  save.disabled = true;
  byId("settings-fields").disabled = true;
  feedback("settings-feedback", "正在保存…");
  try {
    const result = await request("settings", {settings});
    state.settings = {...state.settings, ...result.settings};
    fillForm(settingsForm, state.settings);
    invalidate(state.scan);
    invalidate(state.detail);
    state.browser = null;
    if (state.route.view === "keys") renderBrowser();
    scheduleRefresh();
    feedback("settings-feedback", "设置已保存。");
  } catch (error) {
    feedback("settings-feedback", error.message, true);
  } finally {
    byId("settings-fields").disabled = false;
    save.disabled = false;
  }
});

field(profileForm, "mode").addEventListener("change", modeChanged);
field(profileForm, "tls").addEventListener("change", tlsChanged);
byId("refresh").addEventListener("click", refresh);
byId("bootstrap-retry").addEventListener("click", bootstrap);
document.querySelector(".skip-link").addEventListener("click", (event) => {
  event.preventDefault();
  byId("main").focus();
});
window.addEventListener("hashchange", () => renderRoute(true));
window.addEventListener("pagehide", (event) => {
  if (!state.active || event.persisted) return;
  const payload = JSON.stringify({session: state.active.session});
  void fetch("/api/disconnect", {
    method: "POST", headers: {"Content-Type": "application/json"}, body: payload, keepalive: true,
  }).catch(() => {});
});
byId("settings-fields").disabled = true;
byId("save-settings").disabled = true;
renderRoute();
void bootstrap();
