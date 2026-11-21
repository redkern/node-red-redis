'use strict';

process.env.REDKERN_REDIS_METRICS_TOKEN = 'metrics-token-0123456789abcdef0123456789';

const { describe, it, before, after, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { helper, config, redis, load, loadKeepingPalette, collect, waitFor } = require('./helper.js');

function flow(delayMs) {
  return [
    config(),
    { id: 'sin', type: 'redkern-redis-stream-in', server: 'cfg00001', streamKey: 'jobs', group: 'g1', startId: '0', blockMs: 200, count: 50, wires: [['slow', 'seen']] },
    { id: 'slow', type: 'function', func: `setTimeout(() => { node.send(msg); node.done(); }, ${delayMs}); return null;`, outputs: 1, wires: [['ack']] },
    { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfg00001', wires: [] },
    { id: 'met', type: 'redkern-redis-stream-metrics', server: 'cfg00001', streamKey: 'jobs', group: 'g1', wires: [[]] },
    { id: 'seen', type: 'helper' }
  ];
}

function get(port, path, token) {
  return new Promise((resolve, reject) => {
    const request = http.request({ host: '127.0.0.1', port, path, headers: token ? { authorization: `Bearer ${token}` } : {} }, (response) => {
      let text = '';
      response.on('data', (chunk) => { text += chunk; });
      response.on('end', () => resolve({ status: response.statusCode, text, type: response.headers['content-type'] }));
    });
    request.on('error', reject);
    request.end();
  });
}

describe('lifecycle', () => {
  let client;
  before(async () => {
    client = redis();
    await new Promise((resolve) => helper.startServer(resolve));
  });
  after(async () => {
    await helper.unload();
    await new Promise((resolve) => helper.stopServer(resolve));
    client.disconnect();
  });
  beforeEach(async () => { await client.flushall(); });

  it('RDS-STR-5 redeploy under load finishes under 2 s and does not resend entries in flight', async () => {
    const pipeline = client.pipeline();
    for (let index = 0; index < 300; index += 1) pipeline.xadd('jobs', '*', 'n', String(index));
    await pipeline.exec();
    await loadKeepingPalette(flow(3000));
    let seen = collect(helper.getNode('seen'));
    await waitFor(() => seen.length >= 50, 5000);
    const before = new Set(seen.map((msg) => msg.redkern.redis.id));

    const started = Date.now();
    await helper.unload();
    const elapsed = Date.now() - started;
    assert.ok(elapsed < 2000, `redeploy took ${elapsed} ms`);

    // Same node ids: the palette registry survives and the new instance
    // skips entries the previous flow still holds.
    await loadKeepingPalette(flow(3000));
    seen = collect(helper.getNode('seen'));
    await new Promise((resolve) => setTimeout(resolve, 1500));
    const resent = seen.filter((msg) => before.has(msg.redkern.redis.id));
    assert.equal(resent.length, 0);
    await helper.unload();
  });

  it('RDS-STR-4 RDS-STR-7 redeploys with an entry in flight do not raise its delivery count or move it to the DLQ (D1)', async () => {
    await client.xadd('jobs', '*', 'n', 'held');
    const hold = [
      config(),
      { id: 'sin', type: 'redkern-redis-stream-in', server: 'cfg00001', streamKey: 'jobs', group: 'g1', startId: '0', blockMs: 200, maxDeliveries: 2, wires: [['seen']] },
      { id: 'seen', type: 'helper' }
    ];
    for (let round = 0; round < 5; round += 1) {
      await loadKeepingPalette(hold);
      await waitFor(async () => (await client.xinfo('GROUPS', 'jobs')).length === 1, 5000, 50);
      await new Promise((resolve) => setTimeout(resolve, 300));
      await helper.unload();
    }
    const [[, , , count]] = await client.xpending('jobs', 'g1', '-', '+', 10);
    assert.equal(Number(count), 1, 'the entry was delivered once and only skipped by recovery');
    assert.equal(Number(await client.exists('jobs:dlq')), 0);
    // A restart loses the registry: the entry is sent again once, as delivery 2.
    await load([
      ...hold.slice(0, 2).map((node) => (node.id === 'sin' ? { ...node, wires: [['store']] } : node)),
      { id: 'store', type: 'function', func: 'const got = global.get("d1got") || []; got.push(msg.redkern.redis.deliveries); global.set("d1got", got); return null;', outputs: 1, wires: [] }
    ]);
    const got = () => helper.getNode("store").context().global.get("d1got") || [];
    await waitFor(() => got().length === 1, 5000, 50);
    assert.deepEqual([...got()], [2]);
    await helper.unload();
  });

  it('RDS-STR-14 metrics are served on the internal port with the metrics token', async () => {
    await load(flow(10));
    await waitFor(async () => (await get(9551, '/redkern/redis/metrics', 'metrics-token-0123456789abcdef0123456789')).status === 200, 5000, 100);
    const unauthorized = await get(9551, '/redkern/redis/metrics');
    assert.equal(unauthorized.status, 401);
    const response = await get(9551, '/redkern/redis/metrics', 'metrics-token-0123456789abcdef0123456789');
    assert.match(response.type, /version=0\.0\.4/);
    assert.match(response.text, /# TYPE redkern_redis_connections gauge/);
    await helper.unload();
  });

  function post(port, path, token) {
    return new Promise((resolve, reject) => {
      const body = '{"drainTimeoutMs":1000}';
      const request = http.request({ host: '127.0.0.1', port, path, method: 'POST', headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) } }, (response) => {
        response.resume();
        response.on('end', () => resolve(response.statusCode));
      });
      request.on('error', reject);
      request.end(body);
    });
  }

  it('RDS-CTL-3 with metrics on the internal port drain needs its own token; the metrics token does not open it', async () => {
    const drainToken = 'drain-token-0123456789abcdef0123456789abc';
    process.env.REDKERN_REDIS_DRAIN_TOKEN = drainToken;
    try {
      await load(flow(10));
      await waitFor(async () => (await get(9551, '/redkern/redis/metrics', 'metrics-token-0123456789abcdef0123456789')).status === 200, 5000, 100);
      assert.equal(await post(9551, '/redkern/redis/streams/drain'), 401);
      assert.equal(await post(9551, '/redkern/redis/streams/drain', 'metrics-token-0123456789abcdef0123456789'), 401);
      assert.equal(await post(9551, '/redkern/redis/streams/drain', drainToken), 200);
      await helper.unload();
    } finally {
      delete process.env.REDKERN_REDIS_DRAIN_TOKEN;
    }
  });

  it('RDS-CTL-3 with metrics on the internal port and no drain token the drain route is off and a warning says why', async () => {
    await load(flow(10));
    await waitFor(async () => (await get(9551, '/redkern/redis/metrics', 'metrics-token-0123456789abcdef0123456789')).status === 200, 5000, 100);
    assert.equal(await post(9551, '/redkern/redis/streams/drain'), 404);
    assert.ok(helper.log().args.some(([entry]) => /REDKERN_REDIS_DRAIN_TOKEN/.test(String(entry?.msg))), 'warning logged');
    await helper.unload();
  });
});
