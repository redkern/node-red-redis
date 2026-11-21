'use strict';

const { describe, it, before, after, afterEach, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { helper, redis, load, nextMessage, collect, waitFor, REDIS_PORT } = require('./helper.js');
const { createProxy } = require('./proxy.js');

const PROXY_PORT = 6395;

function proxiedConfig() {
  return { id: 'cfgproxy', type: 'redkern-redis-config', host: '127.0.0.1', port: PROXY_PORT, mode: 'standalone', commandTimeout: 2000, connectTimeout: 1000 };
}

function statuses(node) {
  const list = [];
  node.on('call:status', (call) => list.push(call.args[0]));
  return list;
}

async function redkernClients(client) {
  return (await client.client('LIST')).split('\n').filter((line) => /name=redkern-redis-/.test(line));
}

describe('network faults', () => {
  let client;
  let proxy;
  before(async () => {
    client = redis();
    proxy = createProxy({ port: PROXY_PORT, targetPort: REDIS_PORT });
    await new Promise((resolve) => helper.startServer(resolve));
  });
  after(async () => {
    await proxy.down();
    await new Promise((resolve) => helper.stopServer(resolve));
    client.disconnect();
  });
  beforeEach(async () => {
    await client.flushall();
    await proxy.up();
  });
  afterEach(async () => { await helper.unload(); });

  it('RDS-CON-1 Redis unavailable on start: node retries and becomes ready without redeploy, one connection per role', async () => {
    await proxy.down();
    await load([
      proxiedConfig(),
      { id: 'sout', type: 'redkern-redis-stream-out', server: 'cfgproxy', streamKey: 's', wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    const seen = statuses(helper.getNode('sout'));
    await waitFor(() => seen.some((status) => /try \d+/.test(status.text || '')), 8000, 50);
    await proxy.up();
    await waitFor(() => seen.at(-1)?.fill === 'green', 40000, 100);
    const shared = (await redkernClients(client)).filter((line) => /redkern-redis-shared-/.test(line));
    assert.equal(shared.length, 1);
    const got = nextMessage(helper.getNode('out'));
    helper.getNode('sout').receive({ payload: { a: 1 } });
    assert.ok((await got).redkern.redis.xadd.id);
  });

  it('RDS-CON-2 lost XADD reply does not create a duplicate (no auto-resend on the shared role)', async () => {
    await load([
      proxiedConfig(),
      { id: 'sout', type: 'redkern-redis-stream-out', server: 'cfgproxy', streamKey: 's', errorOutput: true, outputs: 2, wires: [['out'], ['err']] },
      { id: 'out', type: 'helper' },
      { id: 'err', type: 'helper' }
    ]);
    await waitFor(async () => (await redkernClients(client)).length === 1, 5000, 50);
    const failed = nextMessage(helper.getNode('err'), 10000);
    proxy.cutNextReply();
    helper.getNode('sout').receive({ payload: { order: 1 } });
    const msg = await failed;
    assert.ok(msg.error.code);
    await new Promise((resolve) => setTimeout(resolve, 1500));
    assert.equal(Number(await client.xlen('s')), 1, 'the command reached Redis once and was not resent');
  });

  it('RDS-CMD-6 subscribe resubscribes after the connection is lost', async () => {
    await load([
      proxiedConfig(),
      { id: 'sub', type: 'redkern-redis-subscribe', server: 'cfgproxy', channels: 'news', wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    await waitFor(async () => (await client.publish('news', 'first')) > 0, 5000, 50);
    await proxy.down();
    await proxy.up();
    const seen = collect(helper.getNode('out'));
    await waitFor(async () => {
      await client.publish('news', 'after');
      return seen.some((msg) => msg.payload === 'after');
    }, 15000, 200);
  });

  it('RDS-STR-3 a dropped connection during reads loses nothing: the own PEL is recovered', async () => {
    await load([
      proxiedConfig(),
      { id: 'sin', type: 'redkern-redis-stream-in', server: 'cfgproxy', streamKey: 's', group: 'g', startId: '0', blockMs: 200, wires: [['out', 'ack']] },
      { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfgproxy', wires: [] },
      { id: 'out', type: 'helper' }
    ]);
    const seen = collect(helper.getNode('out'));
    await waitFor(async () => (await client.xinfo('GROUPS', 's')).length === 1, 5000, 50);
    for (let index = 0; index < 20; index += 1) {
      await client.xadd('s', '*', 'n', String(index));
      if (index === 10) {
        proxy.cutNextReply();
      }
    }
    await waitFor(() => new Set(seen.map((msg) => msg.redkern.redis.id)).size === 20, 40000, 100);
    await waitFor(async () => Number((await client.xpending('s', 'g'))[0]) === 0, 10000, 100);
  });
});
