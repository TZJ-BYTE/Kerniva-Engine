import { spawn } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const [browserPath, configPath] = process.argv.slice(2);
const {origin, profileId, marker, source} = JSON.parse(await readFile(configPath, "utf8"));
const profileDir = await mkdtemp(join(tmpdir(), 'kerniva-browser-'));
const args = [
  '--headless=new', '--no-first-run', '--no-default-browser-check',
  '--disable-extensions', '--disable-gpu', '--disable-background-networking', '--remote-debugging-port=0',
  '--user-data-dir=' + profileDir, '--window-size=1200,900', 'about:blank',
];
if (process.platform === 'win32' && /msedge\.exe$/i.test(browserPath)) args.unshift('--edge-skip-compat-layer-relaunch');
if (process.getuid?.() === 0) args.unshift('--no-sandbox');
const browser = spawn(browserPath, args, {windowsHide: true, stdio: 'ignore'});
const finished = new Promise(resolve => {
  browser.once('close', resolve);
  browser.once('error', resolve);
});
let browserError;
browser.on('error', error => { browserError = error.code || 'launch error'; });
let socket;
let sequence = 0;
const requests = new Map();
const exceptions = [];
const suiteDeadline = Date.now() + 90000;

async function waitFor(check, description) {
  const deadline = Math.min(suiteDeadline, Date.now() + 15000);
  while (Date.now() < deadline) {
    const value = await check();
    if (value) return value;
    await delay(100);
  }
  throw new Error('Timed out waiting for ' + description);
}

function cdp(method, params = {}) {
  const id = ++sequence;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      requests.delete(id);
      reject(new Error('CDP command timed out: ' + method));
    }, 5000);
    requests.set(id, {resolve, reject, timer});
    socket.send(JSON.stringify({id, method, params}));
  });
}

async function dom(fn, ...args) {
  const expression = '(' + fn + ')(...' + JSON.stringify(args) + ')';
  const result = await cdp('Runtime.evaluate', {expression, returnByValue: true});
  if (result.exceptionDetails) throw new Error('Browser evaluation failed');
  return result.result.value;
}

async function click(selector) {
  const point = await waitFor(() => dom(selector => {
    const target = document.querySelector(selector);
    if (!target?.checkVisibility() || target.disabled) return null;
    target.scrollIntoView({block: 'center'});
    const rect = target.getBoundingClientRect();
    const x = rect.left + rect.width / 2;
    const y = rect.top + rect.height / 2;
    return target.contains(document.elementFromPoint(x, y)) ? {x, y} : null;
  }, selector), selector);
  await cdp('Input.dispatchMouseEvent', {type: 'mousePressed', button: 'left', clickCount: 1, ...point});
  await cdp('Input.dispatchMouseEvent', {type: 'mouseReleased', button: 'left', clickCount: 1, ...point});
}

const contains = (selector, text) => dom((selector, text) => {
  const node = document.querySelector(selector);
  return node?.checkVisibility() && node.innerText.includes(text);
}, selector, text);

try {
  if (typeof WebSocket !== 'function') throw new Error('Node.js 22 or newer is required');
  const port = await waitFor(async () => {
    if (browserError) throw new Error('Browser failed to start: ' + browserError);
    if (browser.exitCode !== null) throw new Error('Browser exited during startup');
    try {
      return (await readFile(join(profileDir, 'DevToolsActivePort'), 'utf8')).split(/\s+/)[0];
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      return null;
    }
  }, 'browser debugger');
  const listTargets = async () => {
    const response = await fetch('http://127.0.0.1:' + port + '/json/list', {signal: AbortSignal.timeout(3000)});
    return (await response.json()).filter(item => item.type === 'page');
  };
  const page = await waitFor(async () => (await listTargets())[0], 'browser page');
  socket = new WebSocket(page.webSocketDebuggerUrl);
  const opened = Promise.withResolvers();
  socket.addEventListener('open', opened.resolve, {once: true});
  socket.addEventListener('error', () => opened.reject(new Error('Browser debugger connection failed')), {once: true});
  await Promise.race([opened.promise, delay(5000, null, {ref: false}).then(() => {
    throw new Error('Browser debugger connection timed out');
  })]);
  socket.addEventListener('message', ({data}) => {
    const event = JSON.parse(data);
    if (event.method === 'Runtime.exceptionThrown') exceptions.push(event.method);
    const pending = requests.get(event.id);
    if (!pending) return;
    requests.delete(event.id);
    clearTimeout(pending.timer);
    if (event.error) pending.reject(new Error('Browser debugger request failed'));
    else pending.resolve(event.result);
  });
  await cdp('Runtime.enable');
  await cdp('Page.navigate', {url: origin + '/#/connections?id=' + encodeURIComponent(profileId)});
  await click('#connect-button');
  await waitFor(() => contains('#connection-status', '已连接'), 'saved profile connection');
  await click('[data-nav="status"]');
  await waitFor(() => contains('#status-nodes', 'go-log'), 'live Go-log runtime status');
  await click('[data-nav="replication"]');
  await waitFor(() => dom(() => {
    const selector = document.querySelector('#replication-db');
    return selector?.checkVisibility();
  }), 'replication topology');
  await dom(() => {
    const selector = document.querySelector('#replication-db');
    selector.value = '1';
    selector.dispatchEvent(new Event('change', {bubbles: true}));
  });
  await waitFor(async () => (await contains('#replication-source', source))
    && (await contains('#replication-read-a', marker))
    && (await contains('#replication-read-b', marker)), 'same-source CLI marker on both replicas');
  await click('#control-b');
  await waitFor(() => contains('#replication-read-b', '已暂停'), 'replica B paused by UI');
  const samePage = await dom(origin => location.origin === origin && location.hash.startsWith('#/replication'), origin);
  if (!samePage || (await listTargets()).length !== 1) throw new Error('Workbench navigation opened a separate page');
  if (exceptions.length) throw new Error('Browser reported a JavaScript exception');
  console.log(JSON.stringify({browser: true, singlePage: true, bothReplicas: true, uiPause: true}));
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
} finally {
  if (socket && socket.readyState === WebSocket.OPEN) await cdp('Browser.close').catch(() => {});
  socket?.close();
  await Promise.race([finished, delay(3000, null, {ref: false})]);
  if (browser.exitCode === null && browser.signalCode === null) browser.kill();
  await Promise.race([finished, delay(3000, null, {ref: false})]);
  try {
    await rm(profileDir, {recursive: true, force: true, maxRetries: 6, retryDelay: 150});
  } catch {
    console.error('Could not remove temporary browser profile');
    process.exitCode = 1;
  }
}
