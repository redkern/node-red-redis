'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { StreamConsumer } = require('../../lib/stream-consumer.js');
const { InflightRegistry } = require('../../lib/inflight.js');

const entry = (id, n = id) => [id, ['n', String(n)]];
const compare = (a, b) => {
  const [am, as] = a.split('-').map(Number);
  const [bm, bs] = b.split('-').map(Number);
  return am - bm || as - bs;
};

/**
 * Fake clients: `reads` is a queue of XREADGROUP ">" replies; `own` models
 * this consumer's PEL as [id, deliveries, deleted?] rows for recovery
 * (XPENDING of the consumer and XCLAIM); `pending` is a queue of other
 * XPENDING range replies. `xclaimNull` makes XCLAIM answer like Redis 6.2
 * (nil for deleted entries) instead of Redis 7 (omitted). Every call is recorded.
 */
function setup({ reads = [], pending = [], claims = [], own, xclaimNull = false, beforeClaim, options = {}, registry = new InflightRegistry() } = {}) {
  const calls = [];
  const emitted = [];
  const controller = new AbortController();
  const blocking = {
    async xreadgroup(...args) {
      calls.push(['xreadgroup', args[args.length - 1], args]);
      const next = reads.shift();
      if (next === undefined) {
        controller.abort();
        return null;
      }
      if (next instanceof Error) throw next;
      return next;
    }
  };
  const transactions = [];
  const shared = {
    async xgroup(...args) { calls.push(['xgroup', ...args]); return 'OK'; },
    async xack(stream, group, ...ids) {
      calls.push(['xack', ...ids]);
      if (own) for (const id of ids) { const index = own.findIndex(([rowId]) => rowId === id); if (index >= 0) own.splice(index, 1); }
      return ids.length;
    },
    async xpending(...args) {
      calls.push(['xpending', ...args]);
      if (args.length === 2) return [0, null, null, []];
      if (own && args[3] === '+') {
        const start = args[2];
        const after = start === '-' ? null : start.slice(1);
        return own.filter(([id]) => after === null || compare(id, after) > 0).slice(0, args[4])
          .map(([id, count]) => [id, 'c1', 10, count]);
      }
      return pending.shift() || [];
    },
    async xrange(stream, start) {
      const row = (own || []).find(([id]) => id === start);
      return row && row[2] !== 'deleted' ? [entry(start)] : [];
    },
    async xclaim(stream, group, consumer, idle, ...ids) {
      calls.push(['xclaim', ...ids]);
      if (beforeClaim) beforeClaim(own);
      const reply = [];
      for (const id of ids) {
        const row = own.find(([rowId]) => rowId === id);
        if (!row) continue;
        if (row[2] === 'deleted') {
          if (xclaimNull) reply.push(null);
          else own.splice(own.indexOf(row), 1);
          continue;
        }
        row[1] += 1;
        reply.push(entry(id));
      }
      return reply;
    },
    async xautoclaim(...args) {
      calls.push(['xautoclaim', args[4]]);
      return claims.shift() || ['0-0', [], []];
    },
    multi() {
      const ops = [];
      const tx = {
        xadd: (...args) => { ops.push(['xadd', ...args]); return tx; },
        xack: (...args) => { ops.push(['xack', ...args]); return tx; },
        exec: async () => { transactions.push(ops); return ops.map(() => [null, 'OK']); }
      };
      return tx;
    }
  };
  const consumer = new StreamConsumer({
    shared,
    blocking,
    stream: 's',
    group: 'g',
    consumer: 'c1',
    registry,
    emit: (delivery) => emitted.push(delivery),
    clock: { now: () => Date.now(), sleep: () => new Promise((resolve) => setImmediate(resolve)) },
    options: { reclaimIntervalMs: 0, ...options }
  });
  return { consumer, calls, emitted, transactions, registry, run: () => consumer.run(controller.signal), controller };
}

describe('StreamConsumer', () => {
  it('RDS-STR-3 recovery walks the own PEL with a cursor and then switches to ">"', async () => {
    const { calls, emitted, run } = setup({
      own: [['1-0', 1], ['2-0', 1], ['3-0', 1]],
      reads: [[['s', [entry('4-0')]]]],
      options: { autoClaim: false, count: 2 }
    });
    await run();
    const starts = calls.filter(([name, ...args]) => name === 'xpending' && args[3] === '+').map(([, , , start]) => start);
    assert.deepEqual(starts, ['-', '(2-0', '(3-0']);
    assert.deepEqual(calls.filter(([name]) => name === 'xreadgroup').map(([, id]) => id), ['>', '>']);
    assert.deepEqual(emitted.map((delivery) => delivery.entries[0].id), ['1-0', '2-0', '3-0', '4-0']);
    assert.deepEqual(emitted.slice(0, 3).map((delivery) => delivery.entries[0].deliveries), [2, 2, 2]);
  });

  it('RDS-STR-4 entries in flight are not sent again by recovery and keep their delivery count (D1)', async () => {
    const registry = new InflightRegistry();
    registry.add('s', 'g', '1-0', 'c1', 60000);
    const own = [['1-0', 1], ['2-0', 1]];
    const { calls, emitted, run } = setup({ registry, own, options: { autoClaim: false } });
    await run();
    assert.deepEqual(emitted.map((delivery) => delivery.entries[0].id), ['2-0']);
    assert.equal(emitted[0].entries[0].deliveries, 2);
    assert.deepEqual(calls.find(([name]) => name === 'xclaim').slice(1), ['2-0'], 'only entries not in flight are claimed');
    assert.equal(own.find(([id]) => id === '1-0')[1], 1, 'recovery passes do not raise the count of entries in flight');
  });

  it('RDS-STR-4 recovery does not resend an entry acknowledged after XPENDING', async () => {
    const { emitted, run } = setup({
      own: [['1-0', 1]],
      beforeClaim: (own) => own.splice(0),
      options: { autoClaim: false }
    });
    await run();
    assert.equal(emitted.length, 0);
  });

  it('RDS-STR-6 one range XPENDING per batch and RDS-STR-7 atomic DLQ move', async () => {
    const { calls, emitted, transactions, run } = setup({
      own: [['1-0', 5], ['2-0', 1], ['3-0', 5]],
      options: { autoClaim: false, maxDeliveries: 5 }
    });
    await run();
    const ranges = calls.filter(([name, ...args]) => name === 'xpending' && args.length > 2);
    assert.equal(ranges.length, 2, 'one XPENDING page with the batch and one empty page');
    assert.deepEqual(emitted.map((delivery) => delivery.entries[0].id), ['2-0']);
    assert.equal(transactions.length, 1);
    assert.deepEqual(transactions[0].map(([op]) => op), ['xadd', 'xadd', 'xack']);
    assert.equal(transactions[0][0][1], 's:dlq');
    assert.deepEqual(transactions[0][2].slice(3), ['1-0', '3-0']);
  });

  it('RDS-STR-8 consumer backpressure sizes reads to the free capacity', async () => {
    const registry = new InflightRegistry();
    for (let index = 0; index < 8; index += 1) registry.add('s', 'g', `0-${index + 100}`, 'c1', 60000);
    const { calls, run } = setup({
      registry,
      reads: [[['s', []]], [['s', [entry('5-0')]]]],
      options: { autoClaim: false, maxPending: 10, count: 100 }
    });
    await run();
    const read = calls.find(([name, id]) => name === 'xreadgroup' && id === '>');
    const countIndex = read[2].indexOf('COUNT');
    assert.equal(read[2][countIndex + 1], 2);
  });

  it('RDS-STR-9 XAUTOCLAIM runs after recovery and keeps its cursor', async () => {
    const { calls, emitted, run } = setup({
      own: [],
      reads: [[['s', []]], [['s', []]]],
      claims: [['5-0', [entry('1-0')], []], ['0-0', [], []]],
      pending: [[['1-0', 'c1', 10, 2]]]
    });
    await run();
    const order = calls.filter(([name, ...args]) => name === 'xautoclaim' || (name === 'xpending' && args[3] === '+')).map(([name, arg]) => `${name}:${arg}`);
    assert.equal(order[0], 'xpending:s', 'own recovery first');
    assert.equal(order[1], 'xautoclaim:0-0');
    assert.ok(order.includes('xautoclaim:5-0'));
    assert.equal(emitted[0].entries[0].id, '1-0');
  });

  it('RDS-STR-2 auto ACK acknowledges after sending and keeps no registry entries', async () => {
    const { calls, registry, run } = setup({
      reads: [[['s', []]], [['s', [entry('1-0')]]]],
      options: { ackMode: 'auto' }
    });
    await run();
    assert.ok(calls.some(([name, id]) => name === 'xack' && id === '1-0'));
    assert.equal(registry.count('s', 'g'), 0);
    assert.ok(!calls.some(([name]) => name === 'xautoclaim'), 'auto ACK has no PEL to reclaim');
  });

  it('RDS-STR-10 batching sends arrays of entries', async () => {
    const { emitted, run } = setup({
      reads: [[['s', []]], [['s', [entry('1-0'), entry('2-0')]]]],
      options: { autoClaim: false, batchWindowMs: 50, count: 2 }
    });
    await run();
    assert.equal(emitted.length, 1);
    assert.equal(emitted[0].batched, true);
    assert.deepEqual(emitted[0].entries.map((item) => item.id), ['1-0', '2-0']);
  });

  it('RDS-STR-11 startPaused creates the group but reserves nothing until resume', async () => {
    const { consumer, calls, controller } = setup({ options: { startPaused: true, pausePollMs: 1 } });
    await consumer.ensureGroup();
    const running = consumer.run(controller.signal);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.ok(calls.some(([name]) => name === 'xgroup'));
    assert.ok(!calls.some(([name]) => name === 'xreadgroup'));
    consumer.resume();
    await running;
    assert.ok(calls.some(([name]) => name === 'xreadgroup'));
  });

  it('RDS-STR-1 restores the group after NOGROUP and recovers again', async () => {
    const { calls, run } = setup({
      own: [],
      reads: [new Error("NOGROUP No such key 's' or consumer group 'g'"), [['s', []]]],
      options: { autoClaim: false }
    });
    await run();
    assert.equal(calls.filter(([name]) => name === 'xgroup').length, 1);
    const order = calls.filter(([name, ...args]) => name === 'xreadgroup' || (name === 'xpending' && args[3] === '+')).map(([name]) => name);
    assert.deepEqual(order.slice(0, 4), ['xpending', 'xreadgroup', 'xpending', 'xreadgroup'], 'recovery runs again after the group is restored');
  });

  it('pause holds entries already read and sends them after resume without a new read', async () => {
    const { consumer, emitted, controller } = setup({ options: { autoClaim: false, rateLimitPerSecond: 0 } });
    consumer.signal = controller.signal;
    consumer.pause();
    await consumer.deliver([{ id: '1-0', payload: {}, deliveries: 1 }]);
    assert.equal(emitted.length, 0);
    assert.equal(consumer.snapshot().held, 1);
    consumer.resume();
    await consumer.iteration();
    assert.equal(emitted.length, 1);
  });

  it('RDS-STR-3 entries deleted while pending are acknowledged instead of blocking recovery', async () => {
    const { calls, emitted, run } = setup({
      own: [['1-0', 1, 'deleted'], ['2-0', 1]],
      xclaimNull: true,
      options: { autoClaim: false }
    });
    await run();
    assert.ok(calls.some(([name, id]) => name === 'xack' && id === '1-0'), 'Redis 6.2 keeps deleted entries in the PEL');
    assert.deepEqual(emitted.map((delivery) => delivery.entries[0].id), ['2-0']);
  });

  it('RDS-STR-3 entries deleted while pending on Redis 7 are dropped by XCLAIM itself', async () => {
    const { calls, emitted, run } = setup({ own: [['1-0', 1, 'deleted'], ['2-0', 1]], options: { autoClaim: false } });
    await run();
    assert.ok(!calls.some(([name]) => name === 'xack'));
    assert.deepEqual(emitted.map((delivery) => delivery.entries[0].id), ['2-0']);
  });

  it('RDS-STR-8 group backpressure reads the PEL of the whole group', async () => {
    const { calls, consumer, controller } = setup({ options: { autoClaim: false, backpressureScope: 'group', maxPending: 10 } });
    consumer.signal = controller.signal;
    consumer.shared.xpending = async (...args) => { calls.push(['xpending', ...args]); return [10, '1-0', '9-0', [['other', '10']]]; };
    assert.equal(await consumer.capacity(), 0);
    assert.ok(calls.some(([name, ...args]) => name === 'xpending' && args.length === 2));
  });

  it('RDS-STR-8 consumer backpressure reconciles the registry with the real PEL', async () => {
    const registry = new InflightRegistry();
    registry.add('s', 'g', '1-0', 'c1', 60000);
    registry.add('s', 'g', '2-0', 'c1', 60000);
    const { consumer, controller } = setup({ registry, options: { autoClaim: false, maxPending: 2 }, pending: [[['2-0', 'c1', 10, 1]]] });
    consumer.signal = controller.signal;
    assert.equal(await consumer.capacity(), 1, 'an entry acknowledged elsewhere frees its slot');
  });

  it('RDS-STR-8 reconcile with a full XPENDING page keeps newer in-flight IDs', async () => {
    const registry = new InflightRegistry();
    registry.add('s', 'g', '5-0', 'c1', 60000);
    registry.add('s', 'g', '6-0', 'c1', 60000);
    // Two stale entries fill the page (maxPending 2) and hide 5-0 and 6-0.
    const { consumer, controller } = setup({ registry, options: { autoClaim: false, maxPending: 2 }, pending: [[['1-0', 'c1', 10, 1], ['2-0', 'c1', 10, 1]]] });
    consumer.signal = controller.signal;
    assert.equal(await consumer.capacity(), 0);
    assert.ok(registry.has('s', 'g', '5-0') && registry.has('s', 'g', '6-0'));
  });

  it('RDS-STR-10 rate limit spaces deliveries', async () => {
    let now = 0;
    const slept = [];
    const { consumer, emitted, controller } = setup({ options: { autoClaim: false, rateLimitPerSecond: 2 } });
    consumer.clock = { ...consumer.clock, now: () => now, sleep: async (ms) => { slept.push(ms); now += ms; } };
    consumer.rateTokens = 0;
    consumer.rateRefillAt = 0;
    consumer.signal = controller.signal;
    await consumer.deliver([{ id: '1-0', payload: {}, deliveries: 1 }, { id: '2-0', payload: {}, deliveries: 1 }]);
    assert.equal(emitted.length, 2);
    assert.deepEqual(slept, [500, 500]);
  });

  it('RDS-STR-10 batch interval keeps the legacy minimum gap between batches', async () => {
    let now = 1000;
    const slept = [];
    const { consumer, emitted, controller } = setup({ options: { autoClaim: false, batchIntervalMs: 300, count: 1 } });
    consumer.clock = { ...consumer.clock, now: () => now, sleep: async (ms) => { slept.push(ms); now += ms; } };
    consumer.signal = controller.signal;
    await consumer.deliver([{ id: '1-0', payload: {}, deliveries: 1 }]);
    await consumer.deliver([{ id: '2-0', payload: {}, deliveries: 1 }]);
    assert.equal(emitted.length, 2);
    assert.deepEqual(slept, [300]);
  });

  it('RDS-CTL-1 resume after a long pause re-reads the own PEL instead of sending held entries', async () => {
    let now = 0;
    const { consumer, emitted, controller } = setup({ options: { autoClaim: false, reclaimIdleMs: 1000 } });
    consumer.clock = { ...consumer.clock, now: () => now };
    consumer.signal = controller.signal;
    consumer.recovering = false;
    consumer.pause();
    await consumer.deliver([{ id: '1-0', payload: {}, deliveries: 1 }]);
    now = 5000;
    consumer.resume();
    assert.equal(consumer.held.length, 0);
    assert.equal(consumer.recovering, true);
    assert.equal(emitted.length, 0);
  });

  it('RDS-CTL-4 drain sends reserved entries and reports the own PEL', async () => {
    const { consumer, emitted, controller } = setup({ options: { autoClaim: false, drainCheckIntervalMs: 1 } });
    consumer.signal = controller.signal;
    consumer.pause();
    await consumer.deliver([{ id: '1-0', payload: {}, deliveries: 1 }]);
    let polls = 0;
    consumer.shared.xpending = async () => { polls += 1; return [polls > 2 ? 0 : 1, null, null, polls > 2 ? [] : [['c1', '1']]]; };
    const result = await consumer.drain(5000);
    assert.equal(emitted.length, 1, 'held entry was sent before waiting');
    assert.equal(result.drained, true);
    assert.equal(result.paused, true);
  });

  it('RDS-CTL-4 an entry returned by a read blocked when drain started is sent, not stranded', async () => {
    const own = [];
    let release;
    const blockedRead = new Promise((resolve) => { release = resolve; });
    const { consumer, emitted, controller } = setup({ own, options: { autoClaim: false, drainCheckIntervalMs: 5, pausePollMs: 5 } });
    consumer.blocking.xreadgroup = async () => { await blockedRead; release = null; own.push(['9-0', 1]); return [['s', [entry('9-0')]]]; };
    const running = consumer.run(controller.signal);
    await new Promise((resolve) => setTimeout(resolve, 20));
    consumer.shared.xpending = (async (original, ...args) => (args.length === 2
      ? [own.length, null, null, own.length ? [['c1', String(own.length)]] : []]
      : original(...args))).bind(null, consumer.shared.xpending);
    const drained = consumer.drain(2000);
    await new Promise((resolve) => setTimeout(resolve, 20));
    release();
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(emitted.length, 1, 'the entry is sent while draining');
    own.splice(0);
    const result = await drained;
    assert.equal(result.drained, true);
    controller.abort();
    await running;
  });

  it('RDS-CTL-4 drain sends own PEL entries that are not in flight here', async () => {
    const own = [['3-0', 1]];
    const { consumer, emitted, controller } = setup({ own, options: { autoClaim: false, drainCheckIntervalMs: 5, startPaused: true, pausePollMs: 5 } });
    consumer.recovering = false;
    const running = consumer.run(controller.signal);
    consumer.shared.xpending = (async (original, ...args) => (args.length === 2
      ? [own.length, null, null, own.length ? [['c1', String(own.length)]] : []]
      : original(...args))).bind(null, consumer.shared.xpending);
    const drained = consumer.drain(2000);
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.deepEqual(emitted.map((delivery) => delivery.entries[0].id), ['3-0']);
    own.splice(0);
    assert.equal((await drained).drained, true);
    assert.equal(consumer.snapshot().paused, true, 'the consumer stays paused after drain');
    controller.abort();
    await running;
  });

  it('RDS-CTL-4 two overlapping drains do not end each other early', async () => {
    const own = [];
    const { consumer, controller } = setup({ own, options: { autoClaim: false, drainCheckIntervalMs: 5, pausePollMs: 5 } });
    consumer.blocking.xreadgroup = async () => { await new Promise((resolve) => setTimeout(resolve, 5)); return null; };
    const running = consumer.run(controller.signal);
    const [short, long] = await Promise.all([consumer.drain(0), consumer.drain(500)]);
    assert.equal(short.drained, true);
    assert.equal(long.drained, true);
    assert.equal(consumer.recovering, false, 'the recovery pass requested by drain finished');
    assert.equal(consumer.draining, 0);
    controller.abort();
    await running;
  });

  it('RDS-CTL-4 drain after a long pause drops held entries instead of sending them blindly', async () => {
    let now = 0;
    const { consumer, emitted, controller } = setup({ own: [], options: { autoClaim: false, reclaimIdleMs: 1000, drainCheckIntervalMs: 1 } });
    consumer.clock = { ...consumer.clock, now: () => now };
    consumer.signal = controller.signal;
    consumer.pause();
    await consumer.deliver([{ id: '1-0', payload: {}, deliveries: 1 }]);
    now = 5000;
    await consumer.drain(0);
    assert.equal(emitted.length, 0);
    assert.equal(consumer.held.length, 0);
  });

  it('RDS-STR-3 a recovery page from before a new recovery request is discarded', async () => {
    const own = [['1-0', 1]];
    const { consumer, controller } = setup({ own, options: { autoClaim: false } });
    consumer.signal = controller.signal;
    const original = consumer.shared.xpending;
    consumer.shared.xpending = async (...args) => { const rows = await original(...args); consumer.startRecovery(); return rows; };
    await consumer.recoverStep();
    assert.equal(consumer.cursor, '0', 'the newer pass starts from the beginning');
    assert.equal(consumer.recovering, true);
  });

  it('parseEntries skips the bare nil Redis 6.2 XAUTOCLAIM returns for deleted entries', () => {
    const { parseEntries } = require('../../lib/stream-consumer.js');
    assert.deepEqual(parseEntries([null, ['1-0', ['a', '1']], ['2-0', null]]).map((item) => item.id), ['1-0', '2-0']);
  });

  it('a consumer that never ran leaves no registry listener', () => {
    const registry = new InflightRegistry();
    for (let index = 0; index < 5; index += 1) setup({ registry });
    assert.equal(registry.listeners.size, 0);
  });

  it('RDS-CTL-4 drain reports drained false after its timeout', async () => {
    let now = 0;
    const { consumer, controller } = setup({ options: { autoClaim: false, drainCheckIntervalMs: 100 } });
    consumer.clock = { ...consumer.clock, now: () => now, sleep: async (ms) => { now += ms; } };
    consumer.signal = controller.signal;
    consumer.shared.xpending = async () => [1, null, null, [['c1', '1']]];
    const result = await consumer.drain(250);
    assert.equal(result.drained, false);
    assert.equal(result.pending, 1);
  });
});
