'use strict';

const { describe, it, before, after, afterEach, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { util, helper, config, redis, load, nextMessage, collect, waitFor, REDIS_PORT } = require('./helper.js');

function statuses(node) {
  const list = [];
  node.on('call:status', (call) => list.push(call.args[0]));
  return list;
}

function catchErrors(id) {
  return { id: `catch-${id}`, type: 'catch', scope: [id], wires: [[`err-${id}`]] };
}

/** The preStop fragment exactly as published in README (folded YAML scalar). */
function preStopFragment() {
  const readme = fs.readFileSync(path.join(__dirname, '..', '..', 'README.md'), 'utf8').split('\n');
  const start = readme.findIndex((line) => line.trim() === '- >-') + 1;
  const lines = [];
  for (let index = start; index < readme.length && /^ {10}\S/.test(readme[index]); index += 1) lines.push(readme[index].trim());
  return lines.join(' ');
}

function runFragment(port) {
  return new Promise((resolve) => {
    execFile(process.execPath, ['-e', preStopFragment()], { env: { ...process.env, REDKERN_REDIS_INTERNAL_PORT: String(port) }, timeout: 60000 }, (error) => {
      resolve(error ? error.code : 0);
    });
  });
}

describe('nodes', () => {
  let client;
  before(async () => {
    client = redis();
    await new Promise((resolve) => helper.startServer(resolve));
  });
  after(async () => {
    await new Promise((resolve) => helper.stopServer(resolve));
    client.disconnect();
  });
  beforeEach(async () => {
    await client.flushall();
    await client.call('ACL', 'DELUSER', 'rk').catch(() => {});
  });
  afterEach(async () => {
    await helper.unload();
    helper.settings({});
  });

  it('RDS-CON-3 password comes from credentials and never reaches the log', async () => {
    await client.call('ACL', 'SETUSER', 'rk', 'on', '>s3cret-password-value', '~*', '&*', '+@all');
    await load([
      { ...config(), username: 'rk' },
      { id: 'cmd', type: 'redkern-redis-command', server: 'cfg00001', command: 'CLIENT INFO', wires: [['out']] },
      { id: 'out', type: 'helper' }
    ], { cfg00001: { password: 's3cret-password-value' } });
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('cmd').receive({});
    assert.match((await got).payload, / user=rk /);
    await helper.unload();

    await load([
      { ...config(), username: 'rk' },
      { id: 'cmd', type: 'redkern-redis-command', server: 'cfg00001', command: 'PING', wires: [[]] }
    ], { cfg00001: { password: 'wrong-password-value' } });
    const seen = statuses(helper.getNode('cmd'));
    await waitFor(() => seen.some((status) => status.fill === 'red'), 10000, 50);
    const log = JSON.stringify(helper.log().args);
    assert.ok(!log.includes('s3cret-password-value') && !log.includes('wrong-password-value'));
  });

  it('RDS-CON-4 connection state of the config node is visible through the admin API', async () => {
    helper.settings({ adminAuth: { type: 'credentials', users: [] } });
    await load([
      config(),
      { id: 'sin', type: 'redkern-redis-stream-in', server: 'cfg00001', streamKey: 's', group: 'g', blockMs: 200, wires: [[]] }
    ]);
    await waitFor(async () => {
      const response = await helper.request().get('/redkern/redis/connections');
      return response.status === 200 && response.body.configs[0].connections.blocking === 1 && response.body.configs[0].connections.shared === 1;
    }, 5000, 100);
  });

  it('RDS-CON-6 connection fields take ${VAR}; an unset variable stops without retries', async () => {
    process.env.RK_TEST_REDIS_PORT = String(REDIS_PORT);
    await load([
      { ...config(), port: '${RK_TEST_REDIS_PORT}' },
      { id: 'cmd', type: 'redkern-redis-command', server: 'cfg00001', command: 'PING', wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('cmd').receive({});
    assert.equal((await got).payload, 'PONG');
    await helper.unload();

    delete process.env.RK_TEST_MISSING_HOST;
    await load([
      { ...config(), host: '${RK_TEST_MISSING_HOST}' },
      { id: 'cmd', type: 'redkern-redis-command', server: 'cfg00001', command: 'PING', wires: [[]] }
    ]);
    const seen = statuses(helper.getNode('cmd'));
    await waitFor(() => seen.some((status) => status.fill === 'red'), 5000, 50);
    await new Promise((resolve) => setTimeout(resolve, 1500));
    assert.ok(!seen.some((status) => /try \d+/.test(status.text || '')), 'a config error is not retried');
    const log = JSON.stringify(helper.log().args);
    assert.match(log, /RK_TEST_MISSING_HOST/);
  });

  it('RDS-CMD-1 legacy msg.command and token arrays', async () => {
    await load([
      config(),
      { id: 'cmd', type: 'redkern-redis-command', server: 'cfg00001', command: 'GET', allowMsgCommand: true, legacyMsgFields: true, wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    await client.set('k', 'v');
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('cmd').receive({ command: ['STRLEN'], args: ['k'] });
    const msg = await got;
    assert.equal(msg.payload, 1);
    assert.deepEqual(msg.command, ['STRLEN']);
  });

  it('RDS-CMD-8 a full input queue fails with INPUT_QUEUE_FULL', async () => {
    await load([
      config(),
      // A Lua busy loop keeps the only slot busy for a while.
      { id: 'cmd', type: 'redkern-redis-command', server: 'cfg00001', command: 'EVAL', concurrency: 1, maxQueue: 0, wires: [['out']] },
      catchErrors('cmd'),
      { id: 'out', type: 'helper' },
      { id: 'err-cmd', type: 'helper' }
    ]);
    const busy = { args: ['local i = 0 while i < 30000000 do i = i + 1 end return i', 0] };
    // With maxQueue 0 even the start queue holds nothing: wait until ready.
    const seen = statuses(helper.getNode('cmd'));
    await waitFor(() => seen.at(-1)?.fill === 'green', 5000, 20);
    const failed = nextMessage(helper.getNode('err-cmd'));
    helper.getNode('cmd').receive(busy);
    await new Promise((resolve) => setTimeout(resolve, 5));
    helper.getNode('cmd').receive(busy);
    assert.match((await failed).error.message, /queue is full/i);
  });

  it('RDS-CMD-8 a message waiting longer than 30 s for start fails with NODE_NOT_READY', { timeout: 60000 }, async () => {
    await load([
      { ...config(), port: 6399, connectTimeout: 500 },
      { id: 'cmd', type: 'redkern-redis-command', server: 'cfg00001', command: 'PING', wires: [[]] },
      catchErrors('cmd'),
      { id: 'err-cmd', type: 'helper' }
    ]);
    const started = Date.now();
    const failed = nextMessage(helper.getNode('err-cmd'), 40000);
    helper.getNode('cmd').receive({});
    const msg = await failed;
    assert.match(msg.error.message, /not ready/i);
    assert.ok(Date.now() - started >= 29000);
  });

  it('RDS-CMD-4 multi uses the configured command list', async () => {
    await load([
      config(),
      { id: 'm', type: 'redkern-redis-multi', server: 'cfg00001', commands: '[["SET","a","1"],["GET","a"]]', wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('m').receive({});
    assert.deepEqual((await got).payload, ['OK', '1']);
  });

  it('RDS-CMD-5 scan returns HSCAN pairs and requires a key', async () => {
    await client.hset('h', 'a', '1', 'b', '2');
    await load([
      config(),
      { id: 's', type: 'redkern-redis-scan', server: 'cfg00001', scanType: 'HSCAN', legacyMsgFields: true, wires: [['out']] },
      catchErrors('s'),
      { id: 'out', type: 'helper' },
      { id: 'err-s', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('s').receive({ key: 'h' });
    const msg = await got;
    assert.deepEqual(msg.payload.map((item) => item.member).sort(), ['a', 'b']);
    assert.equal(msg.scanComplete, true);
    const failed = nextMessage(helper.getNode('err-s'));
    helper.getNode('s').receive({});
    assert.match((await failed).error.message, /requires msg.key/);
  });

  it('RDS-CMD-7 list-in pops from the tail with BRPOP and without limit at prefetch 0', async () => {
    await client.rpush('q', 'a', 'b', 'c');
    await load([
      config(),
      { id: 'li', type: 'redkern-redis-list-in', server: 'cfg00001', key: 'q', direction: 'BRPOP', timeout: 1, prefetch: 0, wires: [['slow']] },
      { id: 'slow', type: 'function', func: 'setTimeout(() => { node.send(msg); node.done(); }, 2000); return null;', outputs: 1, wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    await waitFor(async () => Number(await client.llen('q')) === 0, 1500, 20);
    const seen = collect(helper.getNode('out'));
    await waitFor(() => seen.length === 3, 5000);
    assert.deepEqual(seen.map((msg) => msg.payload), ['c', 'b', 'a']);
  });

  it('RDS-CMD-7 the legacy subscribe in BLPOP mode pops without waiting for the flow (prefetch 0 keeps this)', async () => {
    const legacy = require('@yroshcha/node-red-contrib-redis-full/redis.js');
    const fn = require(require.resolve('@node-red/nodes/core/function/10-function.js', { paths: [path.dirname(require.resolve('node-red'))] }));
    await client.rpush('legacy:q', 'a', 'b', 'c', 'd');
    await new Promise((resolve) => helper.load([legacy, fn], [
      { id: 'lcfg', type: 'yroshcha-redis-config', host: '127.0.0.1', port: REDIS_PORT },
      { id: 'lsub', type: 'yroshcha-redis-subscribe', server: 'lcfg', mode: 'blpop', topic: 'legacy:q', timeout: 1, wires: [['slow']] },
      { id: 'slow', type: 'function', func: 'setTimeout(() => { node.done(); }, 3000); return null;', outputs: 1, wires: [[]] }
    ], {}, resolve));
    // All elements leave the list long before the first message completes.
    await waitFor(async () => Number(await client.llen('legacy:q')) === 0, 1500, 20);
  });

  it('RDS-STR-12 stream-out honours allowMsgStream, MAXLEN opt-in and legacy fields', async () => {
    await load([
      config(),
      { id: 'o', type: 'redkern-redis-stream-out', server: 'cfg00001', streamKey: 'base', allowMsgStream: true, legacyMsgFields: true, wires: [['out']] },
      { id: 't', type: 'redkern-redis-stream-out', server: 'cfg00001', streamKey: 'trim', maxlen: 5, approxTrim: false, wires: [['out']] },
      { id: 'u', type: 'redkern-redis-stream-out', server: 'cfg00001', streamKey: 'trim', maxlen: 5, approxTrim: false, unsafeTrim: true, wires: [['out']] },
      catchErrors('t'),
      { id: 'out', type: 'helper' },
      { id: 'err-t', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('o').receive({ stream: 'other', payload: { a: 1 } });
    const msg = await got;
    assert.equal(msg.stream, 'other');
    assert.equal(msg.streamId, msg.redkern.redis.xadd.id);
    assert.equal(Number(await client.xlen('other')), 1);
    const failed = nextMessage(helper.getNode('err-t'));
    helper.getNode('t').receive({ payload: { a: 1 } });
    assert.match((await failed).error.message, /unsafe trim/);
    for (let index = 0; index < 8; index += 1) {
      const done = nextMessage(helper.getNode('out'));
      helper.getNode('u').receive({ payload: { n: index } });
      await done;
    }
    assert.equal(Number(await client.xlen('trim')), 5);
  });

  it('RDS-STR-12 stream-ack deletes after ACK and accepts legacy ids', async () => {
    const id = await client.xadd('s', '*', 'a', '1');
    await client.xgroup('CREATE', 's', 'g', '0');
    await client.xreadgroup('GROUP', 'g', 'c', 'STREAMS', 's', '>');
    await load([
      config(),
      { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfg00001', deleteAfterAck: true, legacyMsgFields: true, outputs: 1, wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('ack').receive({ streamId: id, _streamKey: 's', _streamGroup: 'g' });
    const msg = await got;
    assert.equal(msg.acked, 1);
    assert.equal(msg.deleted, 1);
    assert.equal(Number(await client.xlen('s')), 0);
  });

  it('RDS-STR-9 stream-claim takes idle entries of another consumer', async () => {
    await client.xgroup('CREATE', 's', 'g', '0', 'MKSTREAM');
    for (let index = 0; index < 5; index += 1) await client.xadd('s', '*', 'n', String(index));
    await client.xreadgroup('GROUP', 'g', 'dead', 'STREAMS', 's', '>');
    await load([
      config(),
      { id: 'cl', type: 'redkern-redis-stream-claim', server: 'cfg00001', streamKey: 's', group: 'g', consumer: 'rescuer', minIdleMs: 0, count: 2, legacyMsgFields: true, wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('cl').receive({});
    const msg = await got;
    assert.equal(msg.payload.length, 5);
    assert.equal(msg.payload[0].streamId, msg.payload[0].id);
    assert.equal(msg.redkern.redis.claim.complete, true);
    const [, , , owners] = await client.xpending('s', 'g');
    assert.deepEqual(owners, [['rescuer', '5']]);
  });

  it('RDS-STR-13 stream-gc removes only idle consumers with an empty PEL', async () => {
    await client.xgroup('CREATE', 's', 'g', '0', 'MKSTREAM');
    await client.xadd('s', '*', 'n', '1');
    await client.call('XGROUP', 'CREATECONSUMER', 's', 'g', 'idle-empty');
    await client.xreadgroup('GROUP', 'g', 'busy', 'STREAMS', 's', '>');
    await load([
      config(),
      { id: 'gc', type: 'redkern-redis-stream-gc', server: 'cfg00001', streamKey: 's', group: 'g', minIdleMs: 0, wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('gc').receive({});
    const msg = await got;
    assert.deepEqual(msg.payload.removed.map((item) => item.name), ['idle-empty']);
    assert.deepEqual(msg.payload.kept.map((item) => item.name), ['busy']);
    const consumers = await client.xinfo('CONSUMERS', 's', 'g');
    assert.equal(consumers.length, 1);
  });

  it('RDS-STR-15 repeated deliveries are counted in redkern_redis_redeliveries_total', async () => {
    let source;
    const onRegister = (descriptor) => { if (descriptor.domain === 'redis') source = descriptor; };
    util.events.on('redkern:metrics:register', onRegister);
    try {
      await client.xgroup('CREATE', 's', 'g', '0', 'MKSTREAM');
      for (let index = 0; index < 3; index += 1) await client.xadd('s', '*', 'n', String(index));
      await client.xreadgroup('GROUP', 'g', 'dead', 'STREAMS', 's', '>');
      await load([
        config(),
        { id: 'sin', type: 'redkern-redis-stream-in', server: 'cfg00001', streamKey: 's', group: 'g', blockMs: 200, reclaimIdleMs: 1000, reclaimIntervalMs: 1000, wires: [['out', 'ack']] },
        { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfg00001', wires: [] },
        { id: 'met', type: 'redkern-redis-stream-metrics', server: 'cfg00001', streamKey: 's', group: 'g', wires: [[]] },
        { id: 'out', type: 'helper' }
      ]);
      const seen = collect(helper.getNode('out'));
      await waitFor(() => seen.length === 3, 10000, 50);
      assert.match(source.metrics(), /redkern_redis_redeliveries_total\{group="g",stream="s"\} 3/);
    } finally {
      util.events.removeListener('redkern:metrics:register', onRegister);
    }
  });

  it('RDS-CTL-2 RDS-CTL-5 admin control validates input and answers with a public code', async () => {
    helper.settings({ adminAuth: { type: 'credentials', users: [] } });
    await load([config(), { id: 'sin', type: 'redkern-redis-stream-in', server: 'cfg00001', streamKey: 's', group: 'g', blockMs: 200, wires: [[]] }]);
    await waitFor(async () => (await helper.request().get('/redkern/redis/streams/status')).status === 200, 5000, 100);
    const bad = await helper.request().post('/redkern/redis/streams/control').send({ action: 'explode' });
    assert.equal(bad.status, 400);
    assert.equal(bad.body.code, 'CONTROL_ACTION_INVALID');
    const form = await helper.request().post('/redkern/redis/streams/control').type('form').send({ action: 'pause' });
    assert.equal(form.status, 415);
    const paused = await helper.request().post('/redkern/redis/streams/control').send({ action: 'pause' });
    assert.equal(paused.body.consumers[0].paused, true);
  });

  it('RDS-CTL-3 RDS-CTL-4 the README preStop fragment: 0 on a closed port, non-zero on drain timeout', { timeout: 60000 }, async () => {
    assert.equal(await runFragment(9559), 0, 'closed port means disabled on purpose');
    await client.xadd('s', '*', 'n', '1');
    await load([
      config(),
      { id: 'sin', type: 'redkern-redis-stream-in', server: 'cfg00001', streamKey: 's', group: 'g', startId: '0', blockMs: 200, drainTimeoutMs: 300, wires: [[]] }
    ]);
    await waitFor(async () => Number((await client.xpending('s', 'g'))[0]) === 1, 5000);
    // The entry is never acknowledged, so drain times out.
    assert.notEqual(await runFragment(9551), 0);
  });

  it('RDS-CTL-1 stream control rejects an unknown action', async () => {
    await load([
      { id: 'ctl', type: 'redkern-redis-stream-control', wires: [['out']] },
      catchErrors('ctl'),
      { id: 'out', type: 'helper' },
      { id: 'err-ctl', type: 'helper' }
    ]);
    const failed = nextMessage(helper.getNode('err-ctl'));
    helper.getNode('ctl').receive({ payload: { action: 'explode' } });
    assert.match((await failed).error.message, /pause, resume, drain or status/);
  });
});
