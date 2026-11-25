import {writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {connect} from 'node:net';

export async function testClusterPins({config, cdp, dom, click, contains, waitFor}) {
  const {origin, cluster, evidence} = config;
  const data = await fetch(origin + '/api/state').then(r => r.json());
  const nodes = Object.values(cluster.nodes);
  const pinLink = key => {
    const encoded = Buffer.from(key).toString('base64');
    const pin = data.pins.find(item => item.key_b64 === encoded);
    if (!pin || pin.db !== 0) throw new Error('Missing DB 0 cluster pin');
    return '.pin-open[data-pin="' + pin.id + '"]';
  };
  await cdp('Page.navigate', {url: origin + '/#/keys'});
  await click(pinLink(nodes[0].key));
  await waitFor(() => dom(() => document.getElementById('connection-feedback')?.dataset.kind === 'error'), 'login required');
  const login = async password => {
    await click('#connect-form input[name="password"]');
    await cdp('Input.insertText', {text: password});
    await click('#connect-button');
  };
  await login('incorrect-test-password');
  await waitFor(() => dom(() => document.getElementById('connection-feedback')?.dataset.kind === 'error' &&
    !document.querySelector('#connect-form input').value && !document.getElementById('connect-button').disabled), 'wrong password rejected');
  await login(cluster.password);
  for (const [index, node] of nodes.entries()) {
    if (index) await click(pinLink(node.key));
    await waitFor(() => contains('#key-detail', node.value), 'value from slot owner ' + node.port);
  }
  if (!await contains('#database-cluster', 'DB 0') || await dom(() => document.getElementById('session-db').checkVisibility())) {
    throw new Error('Unexpected cluster database selector');
  }
  const before = await dom(() => performance.getEntriesByType('resource').filter(e => e.name.endsWith('/api/scan')).length);
  if (before) throw new Error('Opening a saved key required SCAN');
  await click('#scan-form button');
  await waitFor(() => contains('#key-list', 'NOPERM'), 'ACL denied explicit SCAN');
  await click(pinLink('forbidden:demo'));
  await waitFor(() => contains('#key-detail', 'NOPERM'), 'ACL denied a key outside the allowed prefix');
  await click(pinLink(nodes[0].key));
  await waitFor(() => contains('#key-detail', nodes[0].value), 'authorized key after denial');
  const result = await cdp('Page.captureScreenshot', {format: 'png', captureBeyondViewport: false});
  await writeFile(join(evidence, 'cluster-exact-reader.png'), Buffer.from(result.data, 'base64'));
  return {passed: true, cluster: true, version: cluster.version, database: 0, wrongPasswordRejected: true,
          implicitScans: before, explicitScan: 'NOPERM', deniedKey: 'NOPERM', nodes, screenshot: 'cluster-exact-reader.png'};
}

async function editFixture(port, args, expected) {
  const commands = [['SELECT', '1'], args].map(parts => '*' + parts.length + '\r\n' +
    parts.map(s => '$' + Buffer.byteLength(s) + '\r\n' + s + '\r\n').join('')).join('');
  await new Promise((resolve, reject) => {
    const socket = connect({host: '127.0.0.1', port}, () => socket.write(commands));
    let received = '';
    socket.setTimeout(5000, () => socket.destroy(new Error('Fixture edit timed out')));
    socket.on('error', reject);
    socket.on('data', chunk => {
      received += chunk.toString();
      if (received.split('\r\n').length < 3) return;
      socket.destroy();
      if (received === '+OK\r\n' + expected + '\r\n') resolve();
      else reject(new Error('Fixture rejected edit'));
    });
  });
}

export async function testPins({config, cdp, dom, click, contains, waitFor}) {
  const {origin, profileId, evidence, dbPort} = config;
  const verify = (value, label) => { if (!value) throw new Error(label); };
  const text = (selector, value) => waitFor(() => contains(selector, value), selector + ' = ' + value);
  const state = () => fetch(origin + '/api/state').then(response => response.json());
  const type = async (selector, value) => {
    await dom(selector => { const e = document.querySelector(selector); e.focus(); e.select(); }, selector);
    await cdp('Input.insertText', {text: String(value)});
  };
  const screen = async (name, width, height) => {
    await cdp('Emulation.setDeviceMetricsOverride', {width, height, deviceScaleFactor: 1, mobile: false});
    await dom(() => document.getElementById('pins').scrollIntoView({block: 'start'}));
    verify(await dom(() => document.documentElement.scrollWidth <= innerWidth), 'layout wider than viewport');
    const result = await cdp('Page.captureScreenshot', {format: 'png', captureBeyondViewport: false});
    await writeFile(join(evidence, name + '.png'), Buffer.from(result.data, 'base64'));
  };
  async function exact(key, value) {
    if (!await dom(() => document.getElementById('exact-key').open)) await click('#exact-key summary');
    await type('#exact-key-form textarea', key);
    await click('#exact-key-form button');
    await text('#key-detail', value);
  }
  async function position(db, key) {
    const pin = (await state()).pins.find(pin => pin.profileId === profileId && pin.db === db && pin.key_b64 === key);
    verify(pin, 'Persisted position missing for DB ' + db);
    return pin;
  }
  async function open(pin, value) {
    await click('.pin-open[data-pin="' + pin.id + '"]');
    await waitFor(() => dom(id => new URLSearchParams(location.hash.split('?')[1]).get('pin') === id, pin.id), 'pin route');
    await text('#key-detail', value);
    await text('#connection-status', '当前库 DB ' + pin.db);
  }
  const b64 = key => Buffer.from(key).toString('base64');
  const route = origin + '/#/keys';
  await cdp('Page.navigate', {url: route});
  await text('#pin-list', '演示连接');
  await open(await position(0, '/w=='), 'binary-ff-value');
  await open(await position(1, '/g=='), 'binary-fe-value');
  await open(await position(0, ''), 'empty-name-value');
  verify(await contains('#pin-list', 'Base64 · /w==') && await contains('#pin-list', 'Base64 · /g==')
      && await contains('#pin-list', '（空键名）'), 'Raw key labels are not distinguishable');
  await exact('demo:marker', 'live-db0');
  await click('#pin-current');
  await text('#pin-current', '已固定');
  const zero = await position(0, b64('demo:marker'));
  await type('#session-db', 1);
  await click('#switch-db');
  await text('#connection-status', '当前库 DB 1');
  await exact('demo:marker', 'live-db1');
  await click('#pin-current');
  await text('#pin-current', '已固定');
  const one = await position(1, b64('demo:marker'));
  await exact('demo:list', 'first-page');
  await click('#pin-current');
  await text('#pin-current', '已固定');
  const list = await position(1, b64('demo:list'));
  await open(list, 'first-page');
  await click('#detail-next');
  await text('#key-detail', 'second-page');
  await text('#detail-page', '第 2 页');
  await click('#detail-prev');
  await text('#key-detail', 'first-page');
  await editFixture(dbPort, ['SET', 'demo:marker', 'updated-after-close'], '+OK');
  await cdp('Page.navigate', {url: 'about:blank'});
  await waitFor(() => dom(() => location.href === 'about:blank'), 'workbench closed');
  await cdp('Page.navigate', {url: route});
  await text('#browser-offline', '等待连接');
  await open(one, 'updated-after-close');
  await open(zero, 'live-db0');
  await open(one, 'updated-after-close');
  const implicitScans = await dom(() => performance.getEntriesByType('resource').filter(e => e.name.endsWith('/api/scan')).length);
  verify(implicitScans === 0, 'Fresh direct reads made an implicit SCAN');
  await text('#key-list', '点击「扫描」');
  await screen('pins-desktop', 1280, 900);
  await screen('pins-393px', 393, 852);
  await type('#scan-form input[name="pattern"]', 'never-matches-pins-*');
  await click('#scan-form button');
  await text('#key-list', '此页没有匹配的键');
  await text('#scan-status', '0 个键');
  await open(one, 'updated-after-close');
  await click('.pin-remove[data-remove-pin="' + one.id + '"]');
  await text('#pin-feedback', '已移除常用键');
  await exact('demo:marker', 'updated-after-close');
  await open(list, 'first-page');
  await editFixture(dbPort, ['PEXPIRE', 'demo:list', '1'], ':1');
  await open(list, '该键不存在');
  verify(!(await dom(() => document.querySelector('#pin-current').checkVisibility())), 'Expired key offered pin action');
  const post = async (path, body) => {
    const response = await fetch(origin + '/api/' + path, {method: 'POST', headers: {'Content-Type': 'application/json', Origin: origin},
                                                          body: JSON.stringify(body)});
    verify(response.ok, 'External client failed in ' + path);
    return response.json();
  };
  const extra = (await post('profile/save', {profile: {name: '临时连接', host: '127.0.0.1', port: dbPort, mode: 'standalone', db: 0}})).profile;
  const other = (await post('connect', {id: extra.id})).session;
  const saved = (await post('pins/add', {session: other, profileId: extra.id, db: 0, key_b64: b64('demo:marker')})).pin;
  await click('#pins summary');
  await click('#pins summary');
  await text('#pin-list', '临时连接');
  verify((await post('profile/delete', {id: extra.id})).removedPins === 1, 'Deleted profile left a pin');
  await click('#pin-open-' + saved.id);
  await text('#pin-feedback', '该连接或常用键已删除');
  return {passed: true, rawKeys: ['', '/w==', '/g=='], duplicateNameDBs: [zero.db, one.db], reopenedValue: 'updated-after-close',
          listPages: ['first-page', 'second-page'], afterTTL: 'none', savedDefaultDB: (await state()).profiles[0].db,
          removedKeyStillReadable: true, missingProfileFeedback: true, implicitScans, emptyScanObserved: true,
          screenshots: ['pins-desktop.png', 'pins-393px.png']};
}
