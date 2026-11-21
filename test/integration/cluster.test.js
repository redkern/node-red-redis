'use strict';

// Runs against a Redis Cluster: REDIS_CLUSTER_PORTS=7001,7002,7003 (see
// scripts/start-test-cluster.sh). Skipped when the variable is not set.

const { describe, it, before, after, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const Redis = require('ioredis');
const { helper, load, nextMessage, collect, waitFor } = require('./helper.js');

const PORTS = (process.env.REDIS_CLUSTER_PORTS || '').split(',').filter(Boolean).map(Number);
const skip = PORTS.length === 0 ? 'REDIS_CLUSTER_PORTS is not set' : false;
// REDIS_CLUSTER_TLS=1 runs the same tests against a TLS cluster
// (scripts/start-test-tls-cluster.sh, CA through NODE_EXTRA_CA_CERTS).
const TLS = process.env.REDIS_CLUSTER_TLS === '1';

function clusterConfig() {
  return {
    id: 'cfgclust',
    type: 'redkern-redis-config',
    mode: 'cluster',
    host: '127.0.0.1',
    port: PORTS[0],
    clusterNodes: PORTS.map((port) => `127.0.0.1:${port}`).join(','),
    commandTimeout: 5000,
    tls: TLS
  };
}

describe(TLS ? 'redis cluster over TLS' : 'redis cluster', { skip }, () => {
  let cluster;
  before(async () => {
    cluster = new Redis.Cluster(PORTS.map((port) => ({ host: '127.0.0.1', port })), TLS ? { redisOptions: { tls: {} } } : {});
    await new Promise((resolve) => helper.startServer(resolve));
  });
  after(async () => {
    await new Promise((resolve) => helper.stopServer(resolve));
    await cluster.quit();
  });
  afterEach(async () => {
    await helper.unload();
    await Promise.all(cluster.nodes('master').map((node) => node.flushall()));
  });

  it('RDS-CMD-5 scan visits every master in cluster mode', async () => {
    for (let index = 0; index < 60; index += 1) await cluster.set(`user:${index}`, index);
    const perNode = await Promise.all(cluster.nodes('master').map((node) => node.dbsize()));
    assert.ok(perNode.filter((size) => size > 0).length > 1, 'keys are spread over several masters');
    await load([
      clusterConfig(),
      { id: 's', type: 'redkern-redis-scan', server: 'cfgclust', match: 'user:*', count: 10, wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out'), 10000);
    helper.getNode('s').receive({});
    const msg = await got;
    assert.equal(msg.payload.length, 60);
    assert.equal(msg.redkern.redis.scan.complete, true);
  });

  it('RDS-STR-7 DLQ with a hash tag works in cluster mode', async () => {
    await cluster.xadd('orders:{eu}', '*', 'poison', '1');
    await load([
      clusterConfig(),
      {
        id: 'sin', type: 'redkern-redis-stream-in', server: 'cfgclust', streamKey: 'orders:{eu}', group: 'g', startId: '0',
        blockMs: 200, maxDeliveries: 2, reclaimIdleMs: 1000, reclaimIntervalMs: 1000, wires: [['out']]
      },
      { id: 'out', type: 'helper' }
    ]);
    const seen = collect(helper.getNode('out'));
    await waitFor(async () => Number(await cluster.xlen('orders:{eu}:dlq')) === 1, 15000, 100);
    assert.equal(seen.length, 2);
    assert.equal(Number((await cluster.xpending('orders:{eu}', 'g'))[0]), 0);
  });

  it('RDS-STR-12 stream round trip and RDS-CON-2 named connections per role on the owning node', async () => {
    await load([
      clusterConfig(),
      { id: 'sout', type: 'redkern-redis-stream-out', server: 'cfgclust', streamKey: 'jobs:{a}', wires: [[]] },
      { id: 'sin', type: 'redkern-redis-stream-in', server: 'cfgclust', streamKey: 'jobs:{a}', group: 'g', blockMs: 200, wires: [['out', 'ack']] },
      { id: 'ack', type: 'redkern-redis-stream-ack', server: 'cfgclust', wires: [] },
      { id: 'out', type: 'helper' }
    ]);
    await waitFor(async () => (await cluster.xinfo('GROUPS', 'jobs:{a}')).length === 1, 10000, 100);
    const got = nextMessage(helper.getNode('out'), 10000);
    helper.getNode('sout').receive({ payload: { a: 1 } });
    assert.deepEqual((await got).payload, { a: '1' });
    await waitFor(async () => Number((await cluster.xpending('jobs:{a}', 'g'))[0]) === 0, 5000, 100);
    // The node that owns the stream slot serves both roles under their names.
    const slot = Number(await cluster.cluster('KEYSLOT', 'jobs:{a}'));
    let owner;
    for (const node of cluster.nodes('master')) {
      if (Number(await node.cluster('COUNTKEYSINSLOT', slot)) > 0) owner = node;
    }
    const list = await owner.client('LIST');
    assert.match(list, /name=redkern-redis-shared-/);
    assert.match(list, /name=redkern-redis-blocking-/);
  });
});
