'use strict';

// Load tests for the acceptance criteria of the spec (section 9), required
// for 1.0.0-rc.1. They run in an embedded Node-RED runtime (the unit-test
// helper wraps every send in a sinon spy and is too slow for this).
//   1. command with concurrency 64 handles 10 000 msg/s without queue growth.
//   2. pause/resume with a 500 ms downstream at 1000 msg/s: 0 duplicates, 0 DLQ.
// Needs Redis on REDIS_PORT (default 6390).
// Run: npm run test:load [-- --seconds 10] (at least 6 seconds)

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const { monitorEventLoopDelay } = require('node:perf_hooks');
const Redis = require('ioredis');
const kit = require('@redkern/node-red-kit');
const { summarizeQueue } = require('./load-queue.js');

const NODE_RED = process.env.NODE_RED_PATH || require.resolve('node-red');
const RED = require(NODE_RED);
const REDIS_PORT = Number(process.env.REDIS_PORT || 6390);
const secondsIndex = process.argv.indexOf('--seconds');
const SECONDS = secondsIndex === -1 ? 10 : Number(process.argv[secondsIndex + 1]);
if (!Number.isSafeInteger(SECONDS) || SECONDS < 6) throw new Error('--seconds must be an integer of at least 6');

const INCREMENT = 'context.set("n", (context.get("n") || 0) + 1);';
const COUNT = `${INCREMENT} return null;`;
const COUNT_ERRORS = `${INCREMENT}
const codes = context.get("codes") || Object.create(null);
const code = msg.error?.code || "UNKNOWN";
codes[code] = (codes[code] || 0) + 1;
context.set("codes", codes);
return null;`;

function flows() {
  const cfg = { id: 'cfg', type: 'redkern-redis-config', host: '127.0.0.1', port: REDIS_PORT, mode: 'standalone' };
  return [
    { id: 'tab', type: 'tab', label: 'load' },
    cfg,
    { id: 'cmd', z: 'tab', type: 'redkern-redis-command', server: 'cfg', command: 'INCR', concurrency: 64, maxQueue: 10000, errorOutput: true, outputs: 2, wires: [['ok'], ['err']] },
    { id: 'ok', z: 'tab', type: 'function', func: COUNT, outputs: 1, wires: [] },
    { id: 'err', z: 'tab', type: 'function', func: COUNT_ERRORS, outputs: 1, wires: [] },
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

async function commandThroughput(client, inputStats) {
  const node = RED.nodes.getNode('cmd');
  node.receive({ args: ['warmup'] });
  await until(() => count('ok') === 1, 30000, 'command ready');
  const base = count('ok');
  const requiredRate = 10000;
  const offeredRate = 10500;
  const batchLimit = 64;
  let sent = 0;
  let maxBacklog = 0;
  let maxQueued = 0;
  let maxBatch = 0;
  let maxScheduleDebt = 0;
  const loopDelay = monitorEventLoopDelay({ resolution: 10 });
  loopDelay.enable();
  const cpu = process.cpuUsage();
  const started = performance.now();
  const queueSamples = [{ ms: 0, queued: inputStats().queued }];
  while (performance.now() - started < SECONDS * 1000) {
    const due = Math.floor(((performance.now() - started) / 1000) * offeredRate);
    const debt = Math.max(0, due - sent);
    const batch = Math.min(debt, batchLimit);
    maxScheduleDebt = Math.max(maxScheduleDebt, debt);
    maxBatch = Math.max(maxBatch, batch);
    for (let i = 0; i < batch; i += 1) {
      node.receive({ args: ['counter'] });
      sent += 1;
    }
    const sampleTime = performance.now() - started;
    if (batch > 0 || sampleTime - queueSamples[queueSamples.length - 1].ms >= 20) {
      const { queued } = inputStats();
      if (sampleTime - queueSamples[queueSamples.length - 1].ms >= 20) {
        queueSamples.push({ ms: sampleTime, queued });
      }
      maxQueued = Math.max(maxQueued, queued);
    }
    if (batch > 0) {
      maxBacklog = Math.max(maxBacklog, sent - (count('ok') - base) - count('err'));
    }
    await new Promise((resolve) => setImmediate(resolve));
  }
  const elapsedMs = performance.now() - started;
  const elapsed = elapsedMs / 1000;
  const completed = count('ok') - base;
  const cpuUsed = process.cpuUsage(cpu);
  loopDelay.disable();
  queueSamples.push({ ms: elapsedMs, queued: inputStats().queued });
  const drainStarted = performance.now();
  let drainMs = null;
  while (true) {
    const waited = performance.now() - drainStarted;
    if (inputStats().queued === 0 && waited <= 1000) {
      drainMs = waited;
      break;
    }
    if (waited >= 1000) break;
    await sleep(Math.min(20, 1000 - waited));
  }
  const queue = summarizeQueue(queueSamples, drainMs);
  await until(() => count('ok') - base + count('err') === sent, 60000, 'all commands done');
  const result = {
    sent,
    completedDuringLoad: completed,
    elapsedSeconds: elapsed,
    offeredPerSecond: sent / elapsed,
    completedPerSecond: completed / elapsed,
    failed: count('err'),
    errorCodes: RED.nodes.getNode('err').context().get('codes') || {},
    maxBacklog,
    maxQueued,
    queue,
    maxBatch,
    maxScheduleDebt,
    cpuMs: (cpuUsed.user + cpuUsed.system) / 1000,
    eventLoopDelayP99Ms: loopDelay.percentile(99) / 1e6,
    eventLoopDelayMaxMs: loopDelay.max / 1e6
  };
  console.log('command throughput', JSON.stringify(result));
  assert.equal(result.failed, 0, 'no INPUT_QUEUE_FULL or Redis errors');
  assert.equal(Number(await client.get('counter')), sent);
  assert.ok(result.completedPerSecond >= requiredRate, 'at least 10 000 commands/s completed during the load');
  assert.ok(queue.growthWithinLimit, 'average limiter queue growth is at most 500');
  assert.ok(queue.drainedWithinLimit, 'limiter queue drains within one second');
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
  // Capture the public onInput statistics without changing the shipped nodes.
  let inputStats;
  const init = kit.init;
  kit.init = (...args) => {
    const palette = init(...args);
    const bind = palette.bind;
    palette.bind = (node) => {
      const bound = bind(node);
      if (node.id === 'cmd') {
        const onInput = bound.onInput;
        bound.onInput = (...inputArgs) => {
          const input = onInput(...inputArgs);
          inputStats = input.stats;
          return input;
        };
      }
      return bound;
    };
    return palette;
  };
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
    assert.equal(typeof inputStats, 'function', 'command limiter statistics available');
    await commandThroughput(client, inputStats);
    await pauseResume(client);
    console.log('load tests passed');
  } finally {
    kit.init = init;
    await RED.stop();
    client.disconnect();
    fs.rmSync(userDir, { recursive: true, force: true });
  }
})().then(() => process.exit(process.exitCode || 0), (error) => {
  console.error(error);
  process.exit(1);
});
