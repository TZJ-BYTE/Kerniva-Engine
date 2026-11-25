import {writeFile} from 'node:fs/promises';
import {join} from 'node:path';

export async function testNavigation({config, cdp, dom, click, contains, waitFor}) {
  const {origin, profileId, evidence} = config;
  const report = {journey: [], screenshots: []};
  const verify = (value, label) => { if (!value) throw new Error(label); };
  const visible = selector => dom(selector => document.querySelector(selector)?.checkVisibility(), selector);
  const untilVisible = selector => waitFor(() => visible(selector), selector);
  const text = (selector, value) => waitFor(() => contains(selector, value), selector + ' = ' + value);
  const screen = async (name) => {
    const result = await cdp('Page.captureScreenshot', {format: 'png', captureBeyondViewport: false});
    await writeFile(join(evidence, name + '.png'), Buffer.from(result.data, 'base64'));
    report.screenshots.push(name + '.png');
  };
  async function api(path, body) {
    const resp = await fetch(origin + '/api/' + path, body === undefined ? {} : {
      method: 'POST', headers: {'Content-Type': 'application/json', Origin: origin}, body: JSON.stringify(body),
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status + ' in ' + path);
    return await resp.json();
  }
  const resize = (width, height) => cdp('Emulation.setDeviceMetricsOverride', {width, height, deviceScaleFactor: 1, mobile: false});
  const type = async (selector, value) => {
    await untilVisible(selector);
    await dom(selector => { const e = document.querySelector(selector); e.focus(); e.select(); }, selector);
    await cdp('Input.insertText', {text: String(value)});
  };
  const chooseDb = async db => { await type('#session-db', db); await click('#switch-db'); };
  const openKey = async value => {
    if (!(await visible('#exact-key-form'))) await click('#exact-key summary');
    await type('#exact-key-form textarea', 'shared');
    await click('#exact-key-form button');
    await text('#key-detail', value);
  };
  const fixedMenu = async () => {
    const data = await dom(() => {
      const items = [...document.querySelectorAll('.sidebar a')].filter(x => x.checkVisibility());
      return {global: items.filter(x => x.dataset.nav).map(x => x.dataset.nav), total: items.length};
    });
    verify(data.total === 6 && data.global.join(',') === 'connections,keys,status,replication,settings', 'Unstable sidebar destinations');
    return data.global;
  };
  await resize(1280, 860);
  await cdp('Page.navigate', {url: origin + '/#/connections?id=' + encodeURIComponent(profileId)});
  await text('#profile-list', 'DB 1');
  report.globalNavigation = await fixedMenu();
  verify(await dom(() => [...document.querySelectorAll('#profile-list .profile-name')].map(x => x.textContent).sort().join(',') === 'DB 0,DB 1'), 'Named profiles missing from connections');
  await screen('connections');
  verify(!(await visible('#profile-form')) && await visible('#connect-button'), 'Saved profile does not prioritize connection');
  await click('#profile-summary');
  await untilVisible('#profile-fields');
  await click('#profile-summary');
  await click('#instances-tab');
  await text('#local-instances', '本机实例');
  verify(!(await visible('#saved-connections')) && !(await visible('#new-profile')), 'Instance tab exposes profile controls');
  await click('#saved-tab');
  await click('#connect-button');
  await text('#connection-status', '已连接「DB 0」 · 当前库 DB 0');
  const second = await api('connect', {id: profileId, password: ''});
  try {
    await click('#open-browser');
    await openKey('zero-only-value');
    const original = await dom(() => location.hash);
    verify(original.includes('db=0'), 'DB missing from key link');
    await chooseDb(1);
    await text('#connection-status', '已连接「DB 0」 · 当前库 DB 1');
    await text('#key-detail', '从列表选择');
    await openKey('one-only-value');
    const linked = await dom(() => location.hash);
    verify(linked.includes('db=1'), 'New DB missing from key link');
    const independentlyRead = await api('key', {session: second.session, key_b64: 'c2hhcmVk'});
    verify(independentlyRead.value.text === 'zero-only-value', 'Browser SELECT leaked to another session');
    verify((await api('state')).profiles.find(x => x.id === profileId).db === 0, 'Browser SELECT changed the saved default');
    report.values = {db0: 'zero-only-value', db1: 'one-only-value', otherSession: independentlyRead.value.text, defaultDB: 0};
    await screen('data-db1');
    await chooseDb(99);
    await text('#database-feedback', '无法打开 DB 99');
    await text('#key-detail', 'one-only-value');
    report.invalidDatabase = await dom(() => document.querySelector('#database-feedback').textContent);
    await click('[data-nav="status"]');
    await text('#status-source', 'DB 1');
    await text('#status-nodes', 'DB 1 键数');
    await click('[data-nav="replication"]');
    await text('#replication-unavailable', '复制演示未配置');
    verify(!(await visible('#replication-db')) && !(await visible('#session-db')), 'An unrelated DB selector leaked into replication');
    await click('[data-nav="settings"]');
    await untilVisible('#settings-fields');
    verify(!(await visible('#session-db')), 'DB selector leaked into settings');
    await dom(hash => { location.hash = hash; }, original);
    await text('#connection-status', '已连接「DB 0」 · 当前库 DB 0');
    await text('#key-detail', 'zero-only-value');
    await dom(hash => { location.hash = hash; }, linked);
    await text('#key-detail', 'one-only-value');
    await dom(() => history.back());
    await text('#key-detail', 'zero-only-value');
    report.journey.push('connections', 'local-instances', 'DB 0', 'DB 1', 'isolated session / saved default', 'rejected SELECT', 'status DB 1', 'replication empty state', 'settings', 'key deep link / history');
    const seen = new Set();
    const positions = [];
    for (let i = 0; i < 45; i++) {
      await waitFor(() => dom(() => !document.querySelector('#scan-status').innerText.includes('正在')), 'finished scan page');
      for (const key of await dom(() => [...document.querySelectorAll('#key-list a')].map(x => x.textContent))) seen.add(key);
      const position = await dom(() => document.querySelector('#scan-page').textContent);
      positions.push(position);
      if (await dom(() => document.querySelector('#scan-next').disabled)) break;
      await click('#scan-next');
      await waitFor(() => dom(before => document.querySelector('#scan-page').textContent !== before, position), 'next SCAN cursor');
    }
    verify(seen.size === 9 && positions.length > 1, 'Cursor pages did not traverse all 9 real keys');
    report.scan = {pages: positions.length, uniqueKeys: seen.size};
    await resize(393, 852);
    await fixedMenu();
    verify(await dom(() => document.documentElement.scrollWidth <= innerWidth), 'Mobile layout overflows horizontally');
    await screen('data-393px');
    await click('[data-nav="connections"]');
    await click('#instances-tab');
    await text('#local-instances', '本机实例');
    await fixedMenu();
    report.journey.push('real SCAN pages', '393px global and page navigation');
    return {...report, passed: true};
  } finally {
    await api('disconnect', {session: second.session});
  }
}

export async function testCluster({config, cdp, dom, click, contains, waitFor}) {
  const {origin, profileId, cluster, evidence} = config;
  const report = {mode: 'Redis Cluster', version: cluster.version, nodes: []};
  const waitText = (selector, value) => waitFor(() => contains(selector, value), selector + ' expected state');
  const options = '#node-field select';
  await cdp('Page.navigate', {url: origin + '/#/connections?id=' + encodeURIComponent(profileId)});
  const login = async password => {
    await click('#connect-form input[name="password"]');
    if (!(await dom(() => document.activeElement.type === 'password'))) throw new Error('Connection secret not masked');
    await cdp('Input.insertText', {text: password});
    await click('#connect-button');
  };
  await login('deliberately-wrong-credential');
  await waitFor(() => dom(() => {
    const form = document.querySelector('#connect-form');
    return form.querySelector('#connection-feedback').dataset.kind === 'error' && !form.querySelector('input').value;
  }), 'wrong login rejected and secret cleared');
  await login(cluster.password);
  await click('#open-browser');
  await waitText('#database-cluster', 'Redis Cluster 使用 DB 0');
  if (await dom(() => document.querySelector('#session-db').checkVisibility())) throw new Error('Cluster exposed editable database');
  const nativeOptions = await waitFor(() => dom(selector => {
    const items = [...document.querySelector(selector).options].map(x => x.value);
    return items.length === 4 && items;
  }, options), 'discovery of all three primaries');
  const seen = new Set();
  for (let page = 0; page < 20; page++) {
    await waitFor(() => dom(() => !document.querySelector('#scan-status').innerText.includes('正在')), 'complete cluster scan');
    for (const key of await dom(() => [...document.querySelectorAll('#key-list a')].map(x => x.textContent))) seen.add(key);
    if (await dom(() => document.querySelector('#scan-next').disabled)) { report.scanPages = page + 1; break; }
    const position = await dom(() => document.querySelector('#scan-page').textContent);
    await click('#scan-next');
    await waitFor(() => dom(before => document.querySelector('#scan-page').textContent !== before, position), 'cluster cursor advanced');
  }
  if (seen.size !== 3 || !report.scanPages) throw new Error('All-primary cursor omitted keys');
  const press = async (key, code) => {
    const input = {key, windowsVirtualKeyCode: code, nativeVirtualKeyCode: code};
    await cdp('Input.dispatchKeyEvent', {type: 'rawKeyDown', ...input});
    await cdp('Input.dispatchKeyEvent', {type: 'keyUp', ...input});
  };
  for (const id of nativeOptions.slice(1)) {
    const node = cluster.nodes[id];
    if (!node) throw new Error('Discovered unknown cluster node');
    await dom(selector => document.querySelector(selector).focus(), options);
    await press('ArrowDown', 40);
    if (await dom(selector => document.querySelector(selector).value, options) !== id) throw new Error('Keyboard did not select requested primary');
    await click('#scan-form button');
    await waitFor(() => dom((id, key) => new URLSearchParams(location.hash.split('?')[1]).get('node') === id &&
      [...document.querySelectorAll('#key-list a')].map(x => x.textContent).join('') === key, id, node.key), 'node-selected SCAN');
    await click('#key-list a');
    await waitText('#key-detail', node.value);
    report.nodes.push({id, port: node.port, key: node.key, read: node.value, slots: node.slots});
  }
  await click('[data-nav="status"]');
  for (const node of report.nodes) await waitText('#status-nodes', String(node.port));
  const shot = await cdp('Page.captureScreenshot', {format: 'png'});
  await writeFile(join(evidence, 'cluster-status.png'), Buffer.from(shot.data, 'base64'));
  return {...report, wrongPasswordRejected: true, maskedAndCleared: true, fixedDatabase: 0, passed: true};
}
