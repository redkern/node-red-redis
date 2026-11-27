'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { toTokens, checkCommand, DEFAULT_DENYLIST } = require('../../lib/denylist.js');
const { compact, decodeFields, encodeFields, setMeta } = require('../../lib/format.js');
const { keySlot } = require('../../lib/slot.js');
const { slotIssue } = require('../../lib/dlq.js');
const { InflightRegistry } = require('../../lib/inflight.js');
const { parseCommand, executeControl } = require('../../lib/control.js');
const { MetricsRegistry } = require('../../lib/metrics.js');
const { createCompletionTracker } = require('../../lib/completion.js');
const { parseCommands } = require('../../nodes/redkern-redis-multi.js');
const { resolveTarget } = require('../../nodes/redkern-redis-stream-ack.js');
const { scanAll } = require('../../nodes/redkern-redis-scan.js');
const { parseClusterNodes } = require('../../nodes/redkern-redis-config.js');

describe('denylist', () => {
  it('RDS-CMD-3 rejects every default entry, including two-word SCRIPT FLUSH', () => {
    for (const entry of DEFAULT_DENYLIST) {
      assert.throws(() => checkCommand(toTokens(entry)), { code: 'COMMAND_DENIED' });
    }
    assert.doesNotThrow(() => checkCommand(toTokens('SCRIPT LOAD', ['return 1'])));
    assert.throws(() => checkCommand(toTokens('config get', ['*'])), { code: 'COMMAND_DENIED' });
  });

  it('RDS-CMD-3 the explicit opt-out allows denied commands', () => {
    assert.doesNotThrow(() => checkCommand(toTokens('FLUSHDB'), { denylist: false }));
  });

  it('RDS-CMD-3 matches default subcommands and the current extra denylist', () => {
    const extraDenied = ['client id'];
    assert.throws(() => checkCommand(toTokens('script flush')), { code: 'COMMAND_DENIED', message: 'SCRIPT FLUSH is in the command denylist' });
    assert.doesNotThrow(() => checkCommand(toTokens('SCRIPT LOAD')));
    assert.throws(() => checkCommand(toTokens('CLIENT ID'), { extraDenied }), { code: 'COMMAND_DENIED', message: 'CLIENT ID is in the command denylist' });
    extraDenied[0] = 'get';
    assert.doesNotThrow(() => checkCommand(toTokens('CLIENT ID'), { extraDenied }));
    assert.throws(() => checkCommand(toTokens('GET', ['key']), { extraDenied }), { code: 'COMMAND_DENIED' });
  });

  it('RDS-CMD-3 scripts from msg cannot bypass the denylist; a configured script stays allowed', () => {
    for (const command of ['EVAL', 'evalsha', 'EVAL_RO', 'FCALL', 'FUNCTION', 'SCRIPT']) {
      assert.throws(() => checkCommand(toTokens(command, ["return redis.call('FLUSHALL')", 0]), { fromMsg: true }), { code: 'COMMAND_DENIED' });
    }
    assert.doesNotThrow(() => checkCommand(toTokens('EVAL', ['return 1', 0])));
    assert.doesNotThrow(() => checkCommand(toTokens('EVAL', ['return 1', 0]), { fromMsg: true, denylist: false }));
    for (const command of ['SWAPDB 0 1', 'FAILOVER', 'CLIENT KILL ID 1', 'client pause 1000', 'CLUSTER RESET', 'FUNCTION FLUSH', 'SAVE']) {
      assert.throws(() => checkCommand(toTokens(command)), { code: 'COMMAND_DENIED' }, command);
    }
    assert.doesNotThrow(() => checkCommand(toTokens('CLUSTER INFO')));
    assert.doesNotThrow(() => checkCommand(toTokens('CLIENT ID')));
  });

  it('commands that change the shared connection are rejected even without the denylist', () => {
    for (const command of ['CLIENT REPLY OFF', 'client tracking on', 'CLIENT SETNAME x', 'HELLO 3', 'AUTH u p', 'READONLY', 'READWRITE']) {
      assert.throws(() => checkCommand(toTokens(command), { denylist: false }), { code: 'COMMAND_BLOCKING' }, command);
    }
  });

  it('blocking commands are rejected even without the denylist', () => {
    assert.throws(() => checkCommand(toTokens('BLPOP', ['q', 0]), { denylist: false }), { code: 'COMMAND_BLOCKING' });
    assert.throws(() => checkCommand(toTokens(['XREADGROUP', 'GROUP', 'g', 'c', 'BLOCK', '0'])), { code: 'COMMAND_BLOCKING' });
  });

  it('RDS-CMD-1 accepts string or token array and validates args', () => {
    assert.deepEqual(toTokens('XINFO STREAM', ['s']), ['XINFO', 'STREAM', 's']);
    assert.deepEqual(toTokens(['HGET', 'k'], ['f']), ['HGET', 'k', 'f']);
    assert.throws(() => toTokens('GET', 'k'), { code: 'ARGS_INVALID' });
    assert.throws(() => toTokens(''), { code: 'COMMAND_MISSING' });
  });

  it('RDS-CMD-4 multi checks every command', () => {
    assert.throws(() => parseCommands([['SET', 'a', '1'], ['FLUSHALL']], true), { code: 'COMMAND_DENIED' });
    assert.equal(parseCommands('[["SET","a","1"],["INCR","a"]]', true).length, 2);
    assert.throws(() => parseCommands([['EVAL', "return redis.call('FLUSHALL')", 0]], true, true), { code: 'COMMAND_DENIED' });
    assert.equal(parseCommands([['EVAL', 'return 1', 0]], true, false).length, 1);
    assert.throws(() => parseCommands([], true), { code: 'COMMANDS_INVALID' });
  });
});

describe('format', () => {
  it('compact numbers for status texts', () => {
    assert.equal(compact(950), '950');
    assert.equal(compact(1234), '1.2k');
    assert.equal(compact(3400000), '3.4M');
  });

  it('stream field format is unchanged from the legacy palette', () => {
    const fields = encodeFields({ a: 1, b: { c: 2 }, d: [1], e: 'true', skip: undefined });
    assert.deepEqual(fields, ['a', '1', 'b', '{"c":2}', 'd', '[1]', 'e', 'true']);
    assert.deepEqual(decodeFields(fields), { a: '1', b: { c: 2 }, d: [1], e: 'true' });
    assert.throws(() => encodeFields([1]), { code: 'PAYLOAD_INVALID' });
  });

  it('metadata stays under msg.redkern.redis and keeps other palettes', () => {
    const msg = { redkern: { kafka: { offset: 1 } } };
    setMeta(msg, { id: '1-0' });
    setMeta(msg, { ack: { acked: 1 } });
    assert.deepEqual(msg.redkern, { kafka: { offset: 1 }, redis: { id: '1-0', ack: { acked: 1 } } });
  });
});

describe('cluster slots', () => {
  it('matches the Redis Cluster key slot algorithm', () => {
    assert.equal(keySlot('foo'), 12182);
    assert.equal(keySlot('{user1000}.following'), keySlot('{user1000}.followers'));
  });

  it('RDS-STR-7 requires source and DLQ in one slot', () => {
    assert.equal(slotIssue('orders:{eu}', 'orders:{eu}:dlq'), null);
    assert.equal(slotIssue('orders', 'orders:dlq').code, 'DLQ_CROSS_SLOT');
  });

  it('parses cluster seed nodes', () => {
    assert.deepEqual(parseClusterNodes('', 'h', 7000), [{ host: 'h', port: 7000 }]);
    assert.deepEqual(parseClusterNodes('a:7000, b:7001', 'h', 1), [{ host: 'a', port: 7000 }, { host: 'b', port: 7001 }]);
    assert.throws(() => parseClusterNodes('a', 'h', 1));
  });
});

describe('in-flight registry', () => {
  it('RDS-STR-4 entries expire after their TTL', () => {
    let now = 0;
    const registry = new InflightRegistry({ now: () => now });
    registry.add('s', 'g', '1-0', 'c1', 1000);
    assert.equal(registry.has('s', 'g', '1-0'), true);
    now = 1000;
    assert.equal(registry.has('s', 'g', '1-0'), false);
  });

  it('RDS-STR-5 removal notifies listeners and counts per consumer', () => {
    const registry = new InflightRegistry();
    let notified = 0;
    registry.onRemove(() => { notified += 1; });
    registry.add('s', 'g', '1-0', 'c1', 1000);
    registry.add('s', 'g', '2-0', 'c2', 1000);
    assert.equal(registry.count('s', 'g', 'c1'), 1);
    assert.equal(registry.remove('s', 'g', ['1-0', '9-0']), 1);
    assert.equal(notified, 1);
    assert.equal(registry.retain('s', 'g', 'c2', []), 1);
    assert.equal(registry.count('s', 'g'), 0);
  });
});

describe('control', () => {
  it('RDS-CTL-1 validates actions and filters', () => {
    assert.deepEqual(parseCommand({ action: 'Drain', stream: 's', drainTimeoutMs: '100' }), { action: 'drain', stream: 's', drainTimeoutMs: 100 });
    assert.throws(() => parseCommand({ action: 'delete' }), { code: 'CONTROL_ACTION_INVALID' });
    assert.throws(() => parseCommand({ action: 'drain', drainTimeoutMs: -1 }), { code: 'CONTROL_TIMEOUT_INVALID' });
  });

  it('RDS-CTL-1 applies the action to matching consumers only', async () => {
    const calls = [];
    const fake = (stream) => ({
      stream,
      group: 'g',
      pause: () => { calls.push(`pause ${stream}`); return { paused: true }; },
      status: () => ({ paused: false })
    });
    const result = await executeControl([
      { nodeId: 'a', consumer: fake('s1') },
      { nodeId: 'b', consumer: fake('s2') }
    ], { action: 'pause', stream: 's2' });
    assert.deepEqual(calls, ['pause s2']);
    assert.equal(result.consumers.length, 1);
  });
});

describe('metrics', () => {
  it('RDS-STR-14 renders Prometheus text 0.0.4', () => {
    const metrics = new MetricsRegistry();
    metrics.inc('redkern_redis_redeliveries_total', { stream: 's', group: 'g' }, 2);
    metrics.set('redkern_redis_connections', { role: 'shared' }, 1);
    const text = metrics.render();
    assert.match(text, /# TYPE redkern_redis_redeliveries_total counter/);
    assert.match(text, /redkern_redis_redeliveries_total\{group="g",stream="s"\} 2/);
    assert.match(text, /redkern_redis_connections\{role="shared"\} 1/);
  });
});

describe('completion tracker', () => {
  function fakeHooks() {
    const handlers = new Map();
    return {
      add(name, fn) { handlers.set(name.split('.')[0], fn); },
      remove() { handlers.clear(); },
      emit(name, event) { handlers.get(name)?.(event); },
      get size() { return handlers.size; }
    };
  }

  it('RDS-CMD-7 completes when every delivery of the message completed', () => {
    const hooks = fakeHooks();
    const tracker = createCompletionTracker({ hooks });
    let done = 0;
    tracker.track('m1', () => { done += 1; }, 60000);
    hooks.emit('preDeliver', { msg: { _msgid: 'm1' } });
    hooks.emit('preDeliver', { msg: { _msgid: 'm1' } });
    hooks.emit('onComplete', { msg: { _msgid: 'm1' } });
    assert.equal(done, 0);
    hooks.emit('onComplete', { msg: { _msgid: 'm1' } });
    assert.equal(done, 1);
    assert.equal(hooks.size, 0, 'hooks removed when nothing is tracked');
  });

  it('RDS-CMD-7 releases on timeout', () => {
    const timers = [];
    const tracker = createCompletionTracker({ hooks: fakeHooks() }, { setTimer: (fn) => { timers.push(fn); return 1; }, clearTimer() {} });
    let done = 0;
    tracker.track('m1', () => { done += 1; }, 10);
    timers[0]();
    assert.equal(done, 1);
  });
});

describe('stream ack target', () => {
  it('RDS-STR-12 reads msg.redkern.redis first, then legacy fields, then config', () => {
    assert.deepEqual(resolveTarget({ redkern: { redis: { id: ['1-0', '2-0'], stream: 's', group: 'g' } } }, {}), { ids: ['1-0', '2-0'], stream: 's', group: 'g' });
    assert.deepEqual(resolveTarget({ streamId: '1-0', _streamKey: 's', _streamGroup: 'g' }, {}), { ids: ['1-0'], stream: 's', group: 'g' });
    assert.deepEqual(resolveTarget({ streamId: '1-0' }, { streamKey: 'cs', group: 'cg' }), { ids: ['1-0'], stream: 'cs', group: 'cg' });
    assert.throws(() => resolveTarget({}, {}), { code: 'STREAM_ID_REQUIRED' });
  });
});

describe('scan', () => {
  it('RDS-CMD-5 stops at the timeout and visits every cluster master', async () => {
    let now = 0;
    const master = (keys) => ({ scan: async () => { now += 10; return ['0', keys]; } });
    const client = { nodes: () => [master(['a']), master(['b'])] };
    const all = await scanAll(client, { type: 'SCAN', count: 10, maxResults: 10, timeoutMs: 1000, cluster: true, now: () => now });
    assert.deepEqual(all.items, ['a', 'b']);
    assert.equal(all.complete, true);
    const slow = { scan: async () => { now += 600; return ['5', ['x']]; } };
    const cut = await scanAll(slow, { type: 'SCAN', count: 10, maxResults: 10, timeoutMs: 1000, cluster: false, now: () => now });
    assert.equal(cut.timedOut, true);
    assert.equal(cut.complete, false);
  });
});

describe('ListConsumer', () => {
  const { ListConsumer } = require('../../lib/list-consumer.js');
  function make(direction, onPop) {
    const pushed = [];
    const emitted = [];
    const controller = new AbortController();
    const consumer = new ListConsumer({
      client: {
        async blpop() { return onPop(controller); },
        async brpop() { return onPop(controller); },
        async lpush(key, value) { pushed.push(['lpush', key, value]); },
        async rpush(key, value) { pushed.push(['rpush', key, value]); }
      },
      key: 'q', direction, timeoutSec: 1, prefetch: 0, releaseTimeoutMs: 1000,
      emit: (key, value) => emitted.push(value), newId: () => 'm', wired: () => true,
      sleep: () => Promise.resolve(), backoff: { next: () => 1, reset() {} }
    });
    return { consumer, controller, pushed, emitted };
  }

  it('RDS-CMD-7 an element popped while closing goes back to the end it came from', async () => {
    for (const [direction, op] of [['BLPOP', 'lpush'], ['BRPOP', 'rpush']]) {
      const { consumer, controller, pushed, emitted } = make(direction, (ctl) => { ctl.abort(); return ['q', 'v1']; });
      await consumer.run(controller.signal);
      assert.deepEqual(emitted, []);
      assert.deepEqual(pushed, [[op, 'q', 'v1']]);
    }
  });
});
