'use strict';

// Kubernetes-like pod test: several Node-RED processes, each with its own
// hostname (Linux UTS namespace through `unshare -u`, needs root) and so its
// own consumer name, read one stream through a consumer group while
//   1. one pod is killed with SIGKILL in the middle of its batches,
//   2. one pod is rolled out gracefully: the preStop drain on the internal
//      port, then SIGTERM, then a new pod,
//   3. one pod gets SIGTERM without preStop.
// Checks: every entry is processed (at least once), PEL empty at the end,
// the drained pod leaves no pending entries behind and exits in time, the
// duplicates come only from the SIGKILL.
// Needs Redis on REDIS_PORT (default 6390). Run: npm run test:pods

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const { spawn } = require('node:child_process');
const Redis = require('ioredis');

const REDIS_PORT = Number(process.env.REDIS_PORT || 6390);
const RED_JS = path.join(path.dirname(require.resolve('node-red/package.json')), 'red.js');
const STREAM = 'pods:jobs';
const RATE = 200;
const SECONDS = Number(process.argv[process.argv.indexOf('--seconds') + 1]) || 60;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`timed out: ${label}`);
    await sleep(100);
  }
}

const FLOW = [
  { id: 'tab', type: 'tab', label: 'pods' },
  { id: 'cfg', type: 'redkern-redis-config', mode: 'standalone', host: '127.0.0.1', port: REDIS_PORT },
  {
    id: 'sin', z: 'tab', type: 'redkern-redis-stream-in', server: 'cfg', streamKey: STREAM, group: 'g', startId: '0',
    blockMs: 1000, count: 20, maxPending: 200, reclaimIdleMs: 5000, reclaimIntervalMs: 2000, wires: [['work']]
  },
  {
    id: 'work', z: 'tab', type: 'function', outputs: 1, wires: [['record']],
    func: 'msg.args = ["pods:seen", msg.payload.n + ":" + env.get("POD")]; setTimeout(() => { node.send(msg); node.done(); }, 20 + Math.floor(Math.random() * 60)); return null;'
  },
  { id: 'record', z: 'tab', type: 'redkern-redis-command', server: 'cfg', command: 'RPUSH', wires: [['ack']] },
  { id: 'ack', z: 'tab', type: 'redkern-redis-stream-ack', server: 'cfg', wires: [] }
];

let podCounter = 0;
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'redkern-pods-'));

function startPod() {
  podCounter += 1;
  const name = `pod-${podCounter}`;
  const dir = path.join(root, name);
  fs.mkdirSync(path.join(dir, 'node_modules', '@redkern'), { recursive: true });
  fs.symlinkSync(path.join(__dirname, '..'), path.join(dir, 'node_modules', '@redkern', 'node-red-redis'));
  fs.writeFileSync(path.join(dir, 'package.json'), JSON.stringify({ name, dependencies: { '@redkern/node-red-redis': '*' } }));
  fs.writeFileSync(path.join(dir, 'flows.json'), JSON.stringify(FLOW));
  const internalPort = 9600 + podCounter;
  fs.writeFileSync(path.join(dir, 'settings.js'), `module.exports = {
    uiPort: ${18900 + podCounter}, uiHost: '127.0.0.1', flowFile: 'flows.json', credentialSecret: false,
    httpAdminRoot: false, editorTheme: { tours: false, palette: { catalogues: [] } }, diagnostics: { enabled: false },
    logging: { console: { level: 'warn' } }, redkernRedisConfigInternalPort: ${internalPort}
  };`);
  const child = spawn('unshare', ['-u', 'sh', '-c', `hostname ${name} && exec node ${RED_JS} -u ${dir} -s ${path.join(dir, 'settings.js')}`], {
    env: { ...process.env, POD: name }, stdio: ['ignore', 'pipe', 'pipe']
  });
  let log = '';
  child.stdout.on('data', (chunk) => { log += chunk; });
  child.stderr.on('data', (chunk) => { log += chunk; });
  const exited = new Promise((resolve) => child.on('exit', (code, signal) => resolve({ code, signal })));
  return { name, child, internalPort, exited, log: () => log };
}

function drain(pod, timeoutMs) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ drainTimeoutMs: timeoutMs });
    const request = http.request({
      host: '127.0.0.1', port: pod.internalPort, method: 'POST', path: '/redkern/redis/streams/drain',
      headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) }
    }, (response) => {
      let text = '';
      response.on('data', (chunk) => { text += chunk; });
      response.on('end', () => resolve({ status: response.statusCode, body: JSON.parse(text || '{}') }));
    });
    request.on('error', reject);
    request.end(body);
  });
}

async function consumerPending(client, consumer) {
  const consumers = await client.xinfo('CONSUMERS', STREAM, 'g').catch(() => []);
  for (const row of consumers) {
    const info = Object.fromEntries(row.reduce((pairs, value, index) => (index % 2 ? pairs : [...pairs, [value, row[index + 1]]]), []));
    if (info.name === consumer) return Number(info.pending);
  }
  return 0;
}

(async () => {
  const client = new Redis({ port: REDIS_PORT });
  await client.del(STREAM, 'pods:seen', `${STREAM}:dlq`);
  const pods = [startPod(), startPod(), startPod()];
  const report = { events: [] };
  try {
    await until(async () => (await client.xinfo('CONSUMERS', STREAM, 'g').catch(() => [])).length >= 0
      && (await client.exists(STREAM)) === 1, 60000, 'group created');
    let stop = false;
    let produced = 0;
    const producer = (async () => {
      while (!stop) {
        const started = Date.now();
        const pipeline = client.pipeline();
        for (let index = 0; index < RATE / 10; index += 1) pipeline.xadd(STREAM, '*', 'n', String(produced++));
        await pipeline.exec();
        const wait = 100 - (Date.now() - started);
        if (wait > 0) await sleep(wait);
      }
    })();

    const step = Math.floor(SECONDS / 4) * 1000;
    await sleep(step);
    const killed = pods[0];
    report.events.push(`SIGKILL ${killed.name}`);
    killed.child.kill('SIGKILL');
    await killed.exited;
    pods.push(startPod());

    await sleep(step);
    const rolled = pods[1];
    const started = Date.now();
    const result = await drain(rolled, 20000);
    report.drain = { pod: rolled.name, status: result.status, drained: result.body.drained, ms: Date.now() - started };
    const consumerName = result.body.consumers?.[0]?.consumer;
    report.drain.pendingAfterDrain = await consumerPending(client, consumerName);
    rolled.child.kill('SIGTERM');
    const exit = await Promise.race([rolled.exited, sleep(15000).then(() => ({ timeout: true }))]);
    report.drain.exit = exit;
    report.drain.pendingAfterExit = await consumerPending(client, consumerName);
    report.events.push(`preStop drain + SIGTERM ${rolled.name}`);
    pods.push(startPod());

    await sleep(step);
    const terminated = pods[2];
    report.events.push(`SIGTERM without preStop ${terminated.name}`);
    terminated.child.kill('SIGTERM');
    report.sigterm = await Promise.race([terminated.exited, sleep(15000).then(() => ({ timeout: true }))]);
    pods.push(startPod());

    await sleep(Math.max(3000, SECONDS * 1000 - 3 * step));
    stop = true;
    await producer;

    await until(async () => {
      if (Number((await client.xpending(STREAM, 'g'))[0]) !== 0) return false;
      const [group] = await client.xinfo('GROUPS', STREAM);
      const [last] = await client.xrevrange(STREAM, '+', '-', 'COUNT', 1);
      return group[group.indexOf('last-delivered-id') + 1] === last[0];
    }, 90000, 'all entries acknowledged');

    const seen = await client.lrange('pods:seen', 0, -1);
    const byEntry = new Map();
    for (const item of seen) {
      const [n, pod] = item.split(':');
      byEntry.set(n, [...(byEntry.get(n) || []), pod]);
    }
    const missing = [];
    for (let n = 0; n < produced; n += 1) if (!byEntry.has(String(n))) missing.push(n);
    const duplicates = [...byEntry.entries()].filter(([, list]) => list.length > 1);
    const duplicatePods = {};
    for (const [, list] of duplicates) for (const pod of list) duplicatePods[pod] = (duplicatePods[pod] || 0) + 1;
    report.produced = produced;
    report.processed = byEntry.size;
    report.missing = missing.length;
    report.duplicates = duplicates.length;
    report.duplicatesByPod = duplicatePods;
    report.dlq = Number(await client.exists(`${STREAM}:dlq`));
    console.log(JSON.stringify(report, null, 2));

    assert.equal(report.missing, 0, 'every entry processed');
    assert.equal(report.dlq, 0);
    assert.equal(report.drain.status, 200);
    assert.equal(report.drain.drained, true);
    assert.equal(report.drain.pendingAfterDrain, 0, 'drained pod leaves no pending entries');
    assert.equal(report.drain.exit.code, 0, 'drained pod exits cleanly');
    assert.ok(!report.sigterm.timeout, 'SIGTERM pod exits in time');
    // Duplicates may only involve the killed pod (its unacknowledged entries
    // are reclaimed) and, without preStop, the SIGTERM pod.
    for (const [, list] of duplicates) {
      assert.ok(list.includes(killed.name) || list.includes(terminated.name), `unexpected duplicate ${list}`);
    }
    console.log('pods test passed');
  } catch (error) {
    for (const pod of pods) console.error(`--- ${pod.name}\n${pod.log().split('\n').filter((line) => /error|warn/i.test(line)).slice(-15).join('\n')}`);
    throw error;
  } finally {
    for (const pod of pods) if (pod.child.exitCode === null && pod.child.signalCode === null) pod.child.kill('SIGKILL');
    client.disconnect();
    fs.rmSync(root, { recursive: true, force: true });
  }
})().then(() => process.exit(0), (error) => {
  console.error(error);
  process.exit(1);
});
