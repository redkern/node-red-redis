'use strict';

const { describe, it, before, after, afterEach, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { helper, config, redis, load, nextMessage, collect, waitFor } = require('./helper.js');

function streamIn(extra = {}) {
  return {
    id: 'sin',
    type: 'redkern-redis-stream-in',
    server: 'cfg00001',
    streamKey: 'orders',
    group: 'g1',
    startId: '0',
    blockMs: 200,
    wires: [['out1']],
    ...extra
  };
}

describe('streams', () => {
  let client;
  before(async () => {
    client = redis();
    await new Promise((resolve) => helper.startServer(resolve));
  });
  after(async () => {
    await new Promise((resolve) => helper.stopServer(resolve));
    client.disconnect();
  });
  beforeEach(async () => { await client.flushall(); });
  afterEach(async () => { await helper.unload(); });

  it('RDS-STR-12 stream-out, stream-in and stream-ack round trip with msg.redkern.redis', async () => {
    await load([
      config(),
      { id: 'sout', type: 'redkern-redis-stream-out', server: 'cfg00001', streamKey: 'orders', wires: [['o0']] },
      streamIn({ wires: [['out1', 'ack']] }),
      { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfg00001', outputs: 1, wires: [['acked']] },
      { id: 'o0', type: 'helper' },
      { id: 'out1', type: 'helper' },
      { id: 'acked', type: 'helper' }
    ]);
    const received = nextMessage(helper.getNode('out1'));
    const acked = nextMessage(helper.getNode('acked'));
    helper.getNode('sout').receive({ payload: { order: 1, items: [1, 2] }, redkern: { kafka: { topic: 't', partition: 0 } } });
    const msg = await received;
    assert.deepEqual(msg.payload, { order: '1', items: [1, 2] });
    assert.equal(msg.redkern.redis.stream, 'orders');
    assert.equal(msg.redkern.redis.group, 'g1');
    assert.equal(msg.redkern.redis.deliveries, 1);
    assert.equal(msg.streamId, undefined, 'legacy fields are off for new nodes');
    const ackMsg = await acked;
    assert.equal(ackMsg.redkern.redis.ack.acked, 1);
    const summary = await client.xpending('orders', 'g1');
    assert.equal(Number(summary[0]), 0);
  });

  it('kafka -> redis keeps msg.redkern.kafka next to msg.redkern.redis', async () => {
    await load([
      config(),
      { id: 'sout', type: 'redkern-redis-stream-out', server: 'cfg00001', streamKey: 'orders', wires: [['o0']] },
      { id: 'o0', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('o0'));
    helper.getNode('sout').receive({ payload: { a: 1 }, redkern: { kafka: { topic: 't' } } });
    const msg = await got;
    assert.deepEqual(msg.redkern.kafka, { topic: 't' });
    assert.equal(msg.redkern.redis.xadd.stream, 'orders');
  });

  it('RDS-STR-12 batch ack acknowledges all ids in one call', async () => {
    for (let index = 0; index < 10; index += 1) await client.xadd('orders', '*', 'n', String(index));
    await load([
      config(),
      streamIn({ batchWindowMs: 100, count: 10, wires: [['out1', 'ack']] }),
      { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfg00001', wires: [] },
      { id: 'out1', type: 'helper' }
    ]);
    const msg = await nextMessage(helper.getNode('out1'));
    assert.equal(msg.payload.length, 10);
    assert.equal(msg.redkern.redis.id.length, 10);
    await waitFor(async () => Number((await client.xpending('orders', 'g1'))[0]) === 0);
  });

  it('legacy fields are set when legacyMsgFields is on', async () => {
    await client.xadd('orders', '*', 'n', '1');
    await load([config(), streamIn({ legacyMsgFields: true }), { id: 'out1', type: 'helper' }]);
    const msg = await nextMessage(helper.getNode('out1'));
    assert.equal(msg.streamId, msg.redkern.redis.id);
    assert.equal(msg._streamKey, 'orders');
    assert.equal(msg._streamGroup, 'g1');
  });

  it('poison message goes to the DLQ exactly after maxDeliveries', async () => {
    await client.xadd('orders', '*', 'poison', '1');
    await load([
      config(),
      streamIn({ maxDeliveries: 3, reclaimIdleMs: 1000, reclaimIntervalMs: 1000 }),
      { id: 'out1', type: 'helper' }
    ]);
    const deliveries = collect(helper.getNode('out1'));
    await waitFor(async () => Number(await client.xlen('orders:dlq')) === 1, 12000, 100);
    assert.equal(deliveries.length, 3, 'delivered exactly maxDeliveries times');
    assert.deepEqual(deliveries.map((msg) => msg.redkern.redis.deliveries), [1, 2, 3]);
    const [[, fields]] = await client.xrange('orders:dlq', '-', '+');
    const record = Object.fromEntries(fields.reduce((pairs, value, index) => (index % 2 ? pairs : [...pairs, [value, fields[index + 1]]]), []));
    assert.equal(record.sourceStream, 'orders');
    assert.equal(record.deliveries, '4');
    assert.equal(Number((await client.xpending('orders', 'g1'))[0]), 0);
  });

  it('pause/resume under load: no duplicates and nothing in the DLQ', async () => {
    const total = 600;
    await load([
      config(),
      streamIn({ count: 50, maxPending: 200, wires: [['slow']] }),
      // 500 ms downstream before ack.
      { id: 'slow', type: 'function', func: 'setTimeout(() => { node.send(msg); node.done(); }, 500); return null;', outputs: 1, wires: [['ack', 'out1']] },
      { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfg00001', wires: [] },
      { id: 'ctl', type: 'redkern-redis-stream-control', wires: [['ctlOut']] },
      { id: 'ctlOut', type: 'helper' },
      { id: 'out1', type: 'helper' }
    ]);
    const seen = collect(helper.getNode('out1'));
    const producer = (async () => {
      for (let index = 0; index < total; index += 1) {
        await client.xadd('orders', '*', 'n', String(index));
        if (index % 50 === 0) await new Promise((resolve) => setTimeout(resolve, 20));
      }
    })();
    const control = helper.getNode('ctl');
    for (let round = 0; round < 3; round += 1) {
      await new Promise((resolve) => setTimeout(resolve, 150));
      control.receive({ payload: { action: 'pause' } });
      await new Promise((resolve) => setTimeout(resolve, 300));
      control.receive({ payload: { action: 'resume' } });
    }
    await producer;
    await waitFor(() => seen.length >= total, 20000, 50);
    await new Promise((resolve) => setTimeout(resolve, 700));
    const ids = seen.map((msg) => msg.redkern.redis.id);
    assert.equal(new Set(ids).size, ids.length, 'no duplicates');
    assert.equal(ids.length, total);
    assert.equal(Number(await client.exists('orders:dlq')), 0);
  });

  it('kill mid-batch: another consumer gets every entry after XAUTOCLAIM', async () => {
    for (let index = 0; index < 20; index += 1) await client.xadd('orders', '*', 'n', String(index));
    // Consumer A reads everything and dies without ACK.
    await client.xgroup('CREATE', 'orders', 'g1', '0');
    await client.xreadgroup('GROUP', 'g1', 'dead-pod', 'COUNT', 100, 'STREAMS', 'orders', '>');
    await load([
      config(),
      streamIn({ consumer: 'live-pod', reclaimIdleMs: 1000, reclaimIntervalMs: 1000, wires: [['out1', 'ack']] }),
      { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfg00001', wires: [] },
      { id: 'out1', type: 'helper' }
    ]);
    const seen = collect(helper.getNode('out1'));
    await waitFor(() => seen.length === 20, 10000, 50);
    assert.ok(seen.every((msg) => msg.redkern.redis.deliveries === 2));
    await waitFor(async () => Number((await client.xpending('orders', 'g1'))[0]) === 0);
  });

  it('RDS-STR-1 restores a consumer group removed while running (NOGROUP)', async () => {
    await load([config(), streamIn(), { id: 'out1', type: 'helper' }]);
    await waitFor(async () => (await client.xinfo('GROUPS', 'orders')).length === 1);
    await client.xgroup('DESTROY', 'orders', 'g1');
    await waitFor(async () => (await client.xinfo('GROUPS', 'orders')).length === 1, 5000, 50);
    const got = nextMessage(helper.getNode('out1'));
    await client.xadd('orders', '*', 'after', 'restore');
    assert.deepEqual((await got).payload, { after: 'restore' });
  });

  it('RDS-STR-7 cluster mode rejects a DLQ in another slot', async () => {
    await load([
      { ...config(), mode: 'cluster' },
      streamIn({ streamKey: 'orders', deadLetterStream: 'orders-dlq' }),
      { id: 'out1', type: 'helper' }
    ]);
    const node = helper.getNode('sin');
    const statuses = [];
    node.on('call:status', (call) => statuses.push(call.args[0]));
    await waitFor(() => statuses.some((status) => status.fill === 'red'), 3000);
    assert.ok(statuses.some((status) => /DLQ_CROSS_SLOT|CONFIG/.test(status.text)));
  });

  it('RDS-CTL-3 internal drain route empties the own PEL', async () => {
    await client.xadd('orders', '*', 'n', '1');
    await load([
      config(),
      streamIn({ wires: [['slow']] }),
      { id: 'slow', type: 'function', func: 'setTimeout(() => { node.send(msg); node.done(); }, 300); return null;', outputs: 1, wires: [['ack']] },
      { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfg00001', wires: [] }
    ]);
    await waitFor(async () => Number((await client.xpending('orders', 'g1'))[0]) === 1, 3000);
    const result = await post(9551, '/redkern/redis/streams/drain', { drainTimeoutMs: 5000 });
    assert.equal(result.status, 200);
    assert.equal(result.body.drained, true);
    assert.equal(result.body.consumers[0].paused, true);
  });

  it('RDS-CTL-4 an entry that arrives while drain waits for a blocked read is delivered and drained', async () => {
    await load([
      config(),
      streamIn({ blockMs: 3000, wires: [['slow']] }),
      { id: 'slow', type: 'function', func: 'setTimeout(() => { node.send(msg); node.done(); }, 200); return null;', outputs: 1, wires: [['ack']] },
      { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfg00001', wires: [] }
    ]);
    await waitFor(async () => (await client.xinfo('GROUPS', 'orders')).length === 1, 5000, 50);
    await new Promise((resolve) => setTimeout(resolve, 300));
    const draining = post(9551, '/redkern/redis/streams/drain', { drainTimeoutMs: 8000 });
    await new Promise((resolve) => setTimeout(resolve, 100));
    await client.xadd('orders', '*', 'n', 'late');
    const result = await draining;
    assert.equal(result.status, 200);
    assert.equal(result.body.drained, true);
    assert.equal(Number((await client.xpending('orders', 'g1'))[0]), 0, 'nothing is stranded in the PEL');
  });

  it('RDS-CON-5 RDS-STR-14 connections metric matches the budget formula', async () => {
    const events = require('./helper.js').util.events;
    let source;
    const onRegister = (descriptor) => { if (descriptor.domain === 'redis') source = descriptor; };
    events.on('redkern:metrics:register', onRegister);
    try {
      await load([
        config(),
        streamIn(),
        streamIn({ id: 'sin2', group: 'g2' }),
        { id: 'li', type: 'redkern-redis-list-in', server: 'cfg00001', key: 'q', timeout: 1, wires: [[]] },
        { id: 'sub', type: 'redkern-redis-subscribe', server: 'cfg00001', channels: 'c1', wires: [[]] },
        { id: 'met', type: 'redkern-redis-stream-metrics', server: 'cfg00001', streamKey: 'orders', group: 'g1', wires: [['out1']] },
        { id: 'out1', type: 'helper' }
      ]);
      // 1 shared + 1 subscriber per config node, 1 blocking per stream-in and list-in.
      await waitFor(async () => (await client.client('LIST')).split('\n').filter((line) => /name=redkern-redis-/.test(line)).length === 5, 5000, 50);
      const got = nextMessage(helper.getNode('out1'));
      helper.getNode('met').receive({});
      assert.equal((await got).payload.group, 'g1');
      assert.ok(source, 'metrics source registered');
      assert.equal(source.contentType, 'text/plain; version=0.0.4; charset=utf-8');
      const text = source.metrics();
      assert.match(text, /redkern_redis_connections\{role="shared"\} 1/);
      assert.match(text, /redkern_redis_connections\{role="blocking"\} 3/);
      assert.match(text, /redkern_redis_connections\{role="subscriber"\} 1/);
      assert.match(text, /redkern_redis_stream_pending\{group="g1",stream="orders"\} 0/);
    } finally {
      events.removeListener('redkern:metrics:register', onRegister);
    }
  });
});

function post(port, path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const request = http.request({ host: '127.0.0.1', port, path, method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) } }, (response) => {
      let text = '';
      response.on('data', (chunk) => { text += chunk; });
      response.on('end', () => resolve({ status: response.statusCode, body: JSON.parse(text) }));
    });
    request.on('error', reject);
    request.end(data);
  });
}
