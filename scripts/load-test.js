'use strict';

// Load tests for the acceptance criteria of the spec (section 9), required
// for 1.0.0-rc.1. They run in an embedded Node-RED runtime (the unit-test
// helper wraps every send in a sinon spy and is too slow for this).
//   1. command with concurrency 64 handles 10 000 msg/s without queue growth.
//   2. pause/resume with a 500 ms downstream at 1000 msg/s: 0 duplicates, 0 DLQ.
// Needs Redis on REDIS_PORT (default 6390).
// Run: npm run test:load [-- --seconds 10]

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const Redis = require('ioredis');

const NODE_RED = process.env.NODE_RED_PATH || require.resolve('node-red');
const RED = require(NODE_RED);
const REDIS_PORT = Number(process.env.REDIS_PORT || 6390);
const SECONDS = Number(process.argv[process.argv.indexOf('--seconds') + 1]) || 10;

const COUNT = 'context.set("n", (context.get("n") || 0) + 1); return null;';

function flows() {
  const cfg = { id: 'cfg', type: 'redkern-redis-config', host: '127.0.0.1', port: REDIS_PORT, mode: 'standalone' };
  return [
    { id: 'tab', type: 'tab', label: 'load' },
    cfg,
    { id: 'cmd', z: 'tab', type: 'redkern-redis-command', server: 'cfg', command: 'INCR', concurrency: 64, maxQueue: 10000, errorOutput: true, outputs: 2, wires: [['ok'], ['err']] },
    { id: 'ok', z: 'tab', type: 'function', func: COUNT, outputs: 1, wires: [] },
    { id: 'err', z: 'tab', type: 'function', func: COUNT, outputs: 1, wires: [] },
    { id: 'sin', z: 'tab', type: 'redkern-redis-stream-in', server: 'cfg', streamKey: 'load', group: 'g', startId: '0', blockMs: 200, count: 100, maxPending: 2000, startPaused: true, wires: [['slow']] },
    { id: 'slow', z: 'tab', type: 'function', func: 'setTimeout(() => { node.send(msg); node.done(); }, 500); return null;', outputs: 1, wires: [['ack', 'seen']] },
    { id: 'ack', z: 'tab', type: 'redkern-redis-stream-ack', server: 'cfg', wires: [] },
    { id: 'seen', z: 'tab', type: 'function', func: 'const ids = context.get("ids") || []; ids.push(msg.redkern.redis.id); context.set("ids", ids); return null;', outputs: 1, wires: [] },
    { id: 'ctl', z: 'tab', type: 'redkern-redis-stream-control', wires: [[]] }
  ];
}

const count = (id) => RED.nodes.getNode(id).context().get('n') || 0;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`timed out: ${label}`);
    await sleep(20);
  }
}

async function commandThroughput(client) {
  const node = RED.nodes.getNode('cmd');
  node.receive({ args: ['warmup'] });
  await until(() => count('ok') === 1, 30000, 'command ready');
  const base = count('ok');
  const rate = 10000;
  let sent = 0;
  let maxBacklog = 0;
  const started = Date.now();
  while (Date.now() - started < SECONDS * 1000) {
    const due = Math.floor(((Date.now() - started) / 1000) * rate);
    while (sent < due) {
      node.receive({ args: ['counter'] });
      sent += 1;
    }
    maxBacklog = Math.max(maxBacklog, sent - (count('ok') - base) - count('err'));
    await new Promise((resolve) => setImmediate(resolve));
  }
  const elapsed = (Date.now() - started) / 1000;
  await until(() => count('ok') - base + count('err') === sent, 60000, 'all commands done');
  const result = { sent, ratePerSecond: Math.round(sent / elapsed), failed: count('err'), maxBacklog };
  console.log('command throughput', JSON.stringify(result));
  assert.equal(result.failed, 0, 'no INPUT_QUEUE_FULL or Redis errors');
  assert.equal(Number(await client.get('counter')), sent);
  assert.ok(result.ratePerSecond >= 9500, 'about 10 000 msg/s offered');
  assert.ok(maxBacklog < 1000, 'the queue does not grow');
}

async function pauseResume(client) {
  const control = RED.nodes.getNode('ctl');
  const ids = () => RED.nodes.getNode('seen').context().get('ids') || [];
  control.receive({ payload: { action: 'resume' } });
  const total = 1000 * SECONDS;
  const producer = (async () => {
    const started = Date.now();
    for (let index = 0; index < total; index += 100) {
      const pipeline = client.pipeline();
      for (let offset = 0; offset < 100; offset += 1) pipeline.xadd('load', '*', 'n', String(index + offset));
      await pipeline.exec();
      const wait = started + (index + 100) - Date.now();
      if (wait > 0) await sleep(wait);
    }
  })();
  const toggler = (async () => {
    for (let round = 0; round < SECONDS; round += 1) {
      await sleep(700);
      control.receive({ payload: { action: 'pause' } });
      await sleep(300);
      control.receive({ payload: { action: 'resume' } });
    }
  })();
  await Promise.all([producer, toggler]);
  await until(() => ids().length >= total, 120000, 'all entries delivered');
  await sleep(1500);
  const delivered = ids();
  const result = { total, delivered: delivered.length, unique: new Set(delivered).size, dlq: Number(await client.exists('load:dlq')) };
  console.log('pause/resume', JSON.stringify(result));
  assert.equal(result.unique, total);
  assert.equal(result.delivered, total, 'no duplicates');
  assert.equal(result.dlq, 0);
}

(async () => {
  const userDir = fs.mkdtempSync(path.join(os.tmpdir(), 'redkern-load-'));
  fs.mkdirSync(path.join(userDir, 'node_modules', '@redkern'), { recursive: true });
  fs.symlinkSync(path.join(__dirname, '..'), path.join(userDir, 'node_modules', '@redkern', 'node-red-redis'));
  fs.writeFileSync(path.join(userDir, 'package.json'), JSON.stringify({ name: 'load', dependencies: { '@redkern/node-red-redis': '*' } }));
  fs.writeFileSync(path.join(userDir, 'flows.json'), JSON.stringify(flows()));
  const client = new Redis({ port: REDIS_PORT });
  await client.flushall();
  const server = http.createServer();
  RED.init(server, {
    userDir,
    flowFile: 'flows.json',
    httpAdminRoot: false,
    httpNodeRoot: false,
    credentialSecret: false,
    logging: { console: { level: 'warn', metrics: false } },
    editorTheme: { palette: { catalogues: [] } },
    diagnostics: { enabled: false }
  });
  try {
    await RED.start();
    await until(() => RED.nodes.getNode('cmd'), 30000, 'flows started');
    await commandThroughput(client);
    await pauseResume(client);
    console.log('load tests passed');
  } finally {
    await RED.stop();
    client.disconnect();
    fs.rmSync(userDir, { recursive: true, force: true });
  }
})().then(() => process.exit(process.exitCode || 0), (error) => {
  console.error(error);
  process.exit(1);
});
