'use strict';

// Soak test: every node type of the palette runs under steady load in an
// embedded Node-RED runtime for --minutes (default 10), with a full redeploy
// every --redeploy seconds (default 30). Every 30 s the heap is measured after
// a forced GC, together with Redis client connections and active handles.
// Fails on: heap growth after warm-up above 20 MB, connections or handles
// growing across redeploys, any process warning (MaxListenersExceeded...),
// unhandled rejections, lost or stuck stream entries.
// Needs Redis on REDIS_PORT (default 6390).
// Run: node --expose-gc scripts/soak-test.js [--minutes 10] [--redeploy 30]

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const Redis = require('ioredis');

const NODE_RED = process.env.NODE_RED_PATH || require.resolve('node-red');
const RED = require(NODE_RED);
const REDIS_PORT = Number(process.env.REDIS_PORT || 6390);
const arg = (name, fallback) => (process.argv.includes(`--${name}`) ? Number(process.argv[process.argv.indexOf(`--${name}`) + 1]) : fallback);
const MINUTES = arg('minutes', 10);
const REDEPLOY_SECONDS = arg('redeploy', 30);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

if (typeof global.gc !== 'function') {
  console.error('run with node --expose-gc');
  process.exit(2);
}
const warnings = [];
process.on('warning', (warning) => warnings.push(`${warning.name}: ${warning.message}`));

const COUNT = 'context.set("n", (context.get("n") || 0) + 1); return null;';
function flows() {
  const cfg = { id: 'cfg', type: 'redkern-redis-config', host: '127.0.0.1', port: REDIS_PORT, mode: 'standalone' };
  const n = (id, extra) => ({ id, z: 'tab', ...extra });
  return [
    { id: 'tab', type: 'tab', label: 'soak' },
    cfg,
    n('cmd', { type: 'redkern-redis-command', server: 'cfg', command: 'INCR', errorOutput: true, outputs: 2, wires: [['cok'], ['cerr']] }),
    n('cok', { type: 'function', func: COUNT, outputs: 1, wires: [] }),
    n('cerr', { type: 'function', func: COUNT, outputs: 1, wires: [] }),
    n('multi', { type: 'redkern-redis-multi', server: 'cfg', commands: '[["INCR","soak:m"],["EXPIRE","soak:m","60"]]', wires: [[]] }),
    n('scan', { type: 'redkern-redis-scan', server: 'cfg', match: 'soak:*', count: 100, maxResults: 1000, wires: [[]] }),
    n('sub', { type: 'redkern-redis-subscribe', server: 'cfg', channels: 'soak:ch', wires: [['subc']] }),
    n('subc', { type: 'function', func: COUNT, outputs: 1, wires: [] }),
    n('lin', { type: 'redkern-redis-list-in', server: 'cfg', key: 'soak:q', timeout: 1, prefetch: 10, wires: [['linc']] }),
    n('linc', { type: 'function', func: COUNT, outputs: 1, wires: [] }),
    n('sout', { type: 'redkern-redis-stream-out', server: 'cfg', streamKey: 'soak:s', wires: [[]] }),
    n('sin', { type: 'redkern-redis-stream-in', server: 'cfg', streamKey: 'soak:s', group: 'g', startId: '0', blockMs: 500, count: 50, reclaimIdleMs: 10000, reclaimIntervalMs: 5000, wires: [['work']] }),
    n('work', { type: 'function', func: 'context.set("n", (context.get("n") || 0) + 1); setTimeout(() => { node.send(msg); node.done(); }, Math.floor(Math.random() * 30)); return null;', outputs: 1, wires: [['ack']] }),
    n('ack', { type: 'redkern-redis-stream-ack', server: 'cfg', wires: [] }),
    n('met', { type: 'redkern-redis-stream-metrics', server: 'cfg', streamKey: 'soak:s', group: 'g', wires: [[]] }),
    n('gc', { type: 'redkern-redis-stream-gc', server: 'cfg', streamKey: 'soak:s', group: 'g', minIdleMs: 600000, wires: [[]] }),
    n('ctl', { type: 'redkern-redis-stream-control', wires: [[]] })
  ];
}

function handles() {
  const resources = process.getActiveResourcesInfo();
  return resources.filter((name) => name !== 'Timeout' && name !== 'Immediate').length;
}

(async () => {
  const userDir = fs.mkdtempSync(path.join(os.tmpdir(), 'redkern-soak-'));
  fs.mkdirSync(path.join(userDir, 'node_modules', '@redkern'), { recursive: true });
  fs.symlinkSync(path.join(__dirname, '..'), path.join(userDir, 'node_modules', '@redkern', 'node-red-redis'));
  fs.writeFileSync(path.join(userDir, 'package.json'), JSON.stringify({ name: 'soak', dependencies: { '@redkern/node-red-redis': '*' } }));
  fs.writeFileSync(path.join(userDir, 'flows.json'), JSON.stringify(flows()));
  const client = new Redis({ port: REDIS_PORT });
  await client.flushall();
  RED.init(http.createServer(), {
    userDir, flowFile: 'flows.json', httpAdminRoot: false, httpNodeRoot: false, credentialSecret: false,
    logging: { console: { level: 'error', metrics: false } }, editorTheme: { palette: { catalogues: [] } }, diagnostics: { enabled: false }
  });
  const samples = [];
  let stop = false;
  let produced = 0;
  let redeploys = 0;
  try {
    await RED.start();
    await sleep(3000);
    const load = (async () => {
      let tick = 0;
      while (!stop) {
        const started = Date.now();
        tick += 1;
        const pipeline = client.pipeline();
        for (let index = 0; index < 50; index += 1) pipeline.xadd('soak:s', '*', 'n', String(produced++));
        pipeline.lpush('soak:q', ...Array.from({ length: 10 }, (_, index) => `${tick}:${index}`));
        pipeline.publish('soak:ch', String(tick));
        await pipeline.exec().catch(() => {});
        const node = (id) => RED.nodes.getNode(id);
        for (let index = 0; index < 50; index += 1) node('cmd')?.receive({ args: [`soak:c:${index % 10}`] });
        node('multi')?.receive({});
        node('sout')?.receive({ payload: { tick } });
        if (tick % 10 === 0) {
          node('scan')?.receive({});
          node('met')?.receive({});
          node('ctl')?.receive({ payload: { action: 'status' } });
        }
        if (tick % 100 === 0) node('gc')?.receive({});
        const wait = 100 - (Date.now() - started);
        if (wait > 0) await sleep(wait);
      }
    })();

    const deadline = Date.now() + MINUTES * 60000;
    let nextRedeploy = Date.now() + REDEPLOY_SECONDS * 1000;
    let nextSample = Date.now() + 30000;
    while (Date.now() < deadline) {
      await sleep(1000);
      if (Date.now() >= nextRedeploy) {
        await RED.runtime.flows.setFlows({ flows: { flows: flows() }, deploymentType: 'full' });
        redeploys += 1;
        nextRedeploy = Date.now() + REDEPLOY_SECONDS * 1000;
      }
      if (Date.now() >= nextSample) {
        global.gc();
        await sleep(200);
        global.gc();
        const connections = (await client.client('LIST')).split('\n').filter((line) => /name=redkern-redis-/.test(line)).length;
        samples.push({ t: Math.round((Date.now() - (deadline - MINUTES * 60000)) / 1000), heapMB: +(process.memoryUsage().heapUsed / 1048576).toFixed(1), rssMB: +(process.memoryUsage().rss / 1048576).toFixed(1), connections, handles: handles() });
        console.log(JSON.stringify(samples.at(-1)));
        nextSample = Date.now() + 30000;
      }
    }
    stop = true;
    await load;
    await (async () => {
      const limit = Date.now() + 60000;
      for (;;) {
        const pending = Number((await client.xpending('soak:s', 'g'))[0]);
        const [group] = await client.xinfo('GROUPS', 'soak:s');
        const [last] = await client.xrevrange('soak:s', '+', '-', 'COUNT', 1);
        if (pending === 0 && group[group.indexOf('last-delivered-id') + 1] === last[0]) return;
        if (Date.now() > limit) throw new Error(`stream not drained: pending ${pending}`);
        await sleep(500);
      }
    })();

    const warm = samples.slice(Math.min(2, samples.length - 1));
    const heapGrowth = warm.at(-1).heapMB - Math.min(...warm.map((s) => s.heapMB));
    const result = {
      minutes: MINUTES, redeploys, produced, dlq: Number(await client.exists('soak:s:dlq')),
      heapGrowthMB: +heapGrowth.toFixed(1), connections: [...new Set(samples.map((s) => s.connections))],
      handles: [Math.min(...samples.map((s) => s.handles)), Math.max(...samples.map((s) => s.handles))], warnings
    };
    console.log(JSON.stringify(result, null, 2));
    assert.deepEqual(warnings, [], 'no process warnings');
    assert.equal(result.dlq, 0);
    assert.ok(heapGrowth < 20, `heap grew ${heapGrowth.toFixed(1)} MB after warm-up`);
    assert.ok(Math.max(...samples.map((s) => s.connections)) <= 4, 'connections stay at the budget (shared, subscriber, 2 blocking)');
    assert.ok(result.handles[1] - result.handles[0] <= 10, 'active handles do not grow across redeploys');
    console.log('soak test passed');
  } finally {
    stop = true;
    await RED.stop();
    client.disconnect();
    fs.rmSync(userDir, { recursive: true, force: true });
  }
})().then(() => process.exit(0), (error) => {
  console.error(error);
  process.exit(1);
});
