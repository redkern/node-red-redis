'use strict';

const { describe, it, before, after, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { helper, config, redis, load, nextMessage, waitFor } = require('./helper.js');

describe('commands', () => {
  let client;
  before(async () => {
    client = redis();
    await client.flushall();
    await new Promise((resolve) => helper.startServer(resolve));
  });
  after(async () => {
    await new Promise((resolve) => helper.stopServer(resolve));
    client.disconnect();
  });
  afterEach(async () => { await helper.unload(); });

  it('RDS-CMD-1 command runs config command with msg.args', async () => {
    await client.hset('h1', 'f', 'v');
    await load([
      config(),
      { id: 'cmd1', type: 'redkern-redis-command', server: 'cfg00001', command: 'HGET', wires: [['out1']] },
      { id: 'out1', type: 'helper' }
    ]);
    const out = helper.getNode('out1');
    const got = nextMessage(out);
    helper.getNode('cmd1').receive({ args: ['h1', 'f'] });
    const msg = await got;
    assert.equal(msg.payload, 'v');
    assert.equal(msg.redkern.redis.command, 'HGET');
  });

  it('RDS-CMD-2 msg.command is ignored unless allowMsgCommand', async () => {
    await client.set('k2', 'x');
    await load([
      config(),
      { id: 'cmd1', type: 'redkern-redis-command', server: 'cfg00001', command: 'GET', wires: [['out1']] },
      { id: 'out1', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out1'));
    helper.getNode('cmd1').receive({ command: 'DEL', args: ['k2'] });
    const msg = await got;
    assert.equal(msg.payload, 'x');
    assert.equal(await client.get('k2'), 'x');
  });

  it('RDS-CMD-3 denied command from msg is rejected', async () => {
    await load([
      config(),
      { id: 'cmd1', type: 'redkern-redis-command', server: 'cfg00001', command: 'GET', allowMsgCommand: true, wires: [['out1']] },
      { id: 'c1', type: 'catch', scope: ['cmd1'], wires: [['err']] },
      { id: 'out1', type: 'helper' },
      { id: 'err', type: 'helper' }
    ]);
    await client.set('keep', '1');
    const failed = nextMessage(helper.getNode('err'));
    helper.getNode('cmd1').receive({ command: 'FLUSHALL' });
    const msg = await failed;
    assert.match(msg.error.message, /denylist/);
    assert.equal(await client.get('keep'), '1');
  });

  it('RDS-CMD-8 errorOutput sends errors to the last port', async () => {
    await load([
      config(),
      { id: 'cmd1', type: 'redkern-redis-command', server: 'cfg00001', command: 'BLPOP', errorOutput: true, outputs: 2, wires: [['out1'], ['err']] },
      { id: 'out1', type: 'helper' },
      { id: 'err', type: 'helper' }
    ]);
    const failed = nextMessage(helper.getNode('err'));
    helper.getNode('cmd1').receive({ args: ['q', 0] });
    const msg = await failed;
    assert.equal(msg.error.code, 'COMMAND_BLOCKING');
  });

  it('RDS-CMD-4 multi returns each result and applies the denylist', async () => {
    await load([
      config(),
      { id: 'm1', type: 'redkern-redis-multi', server: 'cfg00001', wires: [['out1']] },
      { id: 'c1', type: 'catch', scope: ['m1'], wires: [['err']] },
      { id: 'out1', type: 'helper' },
      { id: 'err', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out1'));
    helper.getNode('m1').receive({ commands: [['set', 'mk', '1'], ['incr', 'mk']] });
    assert.deepEqual((await got).payload, ['OK', 2]);
    const failed = nextMessage(helper.getNode('err'));
    helper.getNode('m1').receive({ commands: [['set', 'mk', '1'], ['flushdb']] });
    assert.match((await failed).error.message, /denylist/);
  });

  it('RDS-CMD-5 scan honours the result limit', async () => {
    const pipeline = client.pipeline();
    for (let index = 0; index < 50; index += 1) pipeline.set(`scan:${index}`, index);
    await pipeline.exec();
    await load([
      config(),
      { id: 's1', type: 'redkern-redis-scan', server: 'cfg00001', match: 'scan:*', count: 10, maxResults: 20, wires: [['out1']] },
      { id: 'out1', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out1'));
    helper.getNode('s1').receive({});
    const msg = await got;
    assert.equal(msg.payload.length, 20);
    assert.equal(msg.redkern.redis.scan.truncated, true);
  });

  it('RDS-CMD-6 subscribe delivers Pub/Sub messages', async () => {
    await load([
      config(),
      { id: 'sub', type: 'redkern-redis-subscribe', server: 'cfg00001', mode: 'subscribe', channels: 'news', wires: [['out1']] },
      { id: 'out1', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out1'));
    await waitFor(async () => (await client.publish('news', 'hello')) > 0);
    const msg = await got;
    assert.equal(msg.topic, 'news');
    assert.equal(msg.payload, 'hello');
  });

  it('RDS-CMD-7 closing list-in during a long BLPOP is fast and loses nothing', async () => {
    await load([
      config(),
      { id: 'li', type: 'redkern-redis-list-in', server: 'cfg00001', key: 'slowq', timeout: 30, wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    await waitFor(async () => /cmd=blpop/.test(await client.client('LIST')), 5000, 50);
    const started = Date.now();
    await helper.unload();
    assert.ok(Date.now() - started < 2000, `close took ${Date.now() - started} ms`);
    await client.rpush('slowq', 'after-close');
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.deepEqual(await client.lrange('slowq', 0, -1), ['after-close'], 'no connection is still popping');
  });

  it('RDS-CMD-7 list-in pops with prefetch 1 only after completion', async () => {
    await client.del('jobs');
    await client.rpush('jobs', 'a', 'b', 'c');
    await load([
      config(),
      { id: 'li', type: 'redkern-redis-list-in', server: 'cfg00001', key: 'jobs', timeout: 1, prefetch: 1, wires: [['fn']] },
      // Completes 300 ms after it receives a message.
      { id: 'fn', type: 'function', func: 'setTimeout(() => { node.done(); }, 300); return null;', outputs: 1, wires: [[]] }
    ]);
    await waitFor(async () => (await client.llen('jobs')) === 2, 2000);
    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.equal(await client.llen('jobs'), 2, 'second element must wait for the first message to complete');
    await waitFor(async () => (await client.llen('jobs')) === 0, 4000);
  });
});
