'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { createPalette } = require('../../lib/palette.js');
const { collectConsumers } = require('../../nodes/redkern-redis-stream-gc.js');
const paletteSet = require('../../nodes/redkern-redis.js');

function fakeRED() {
  const routes = [];
  const permissions = [];
  return {
    routes,
    permissions,
    settings: { adminAuth: { type: 'credentials' } },
    events: { on() {}, emit() {}, removeListener() {} },
    log: { warn() {}, info() {}, error() {}, debug() {} },
    httpAdmin: { get: (path) => routes.push(`GET ${path}`), post: (path) => routes.push(`POST ${path}`) },
    auth: { needsPermission: (permission) => { permissions.push(permission); return () => {}; } },
    nodes: { createNode() {}, registerType() {}, getNode() {} },
    util: { generateId: () => 'id' },
    hooks: { add() {}, remove() {} }
  };
}

describe('palette closure', () => {
  it('RDS-CTL-2 admin routes use the domain permissions: status read, control write', () => {
    const RED = fakeRED();
    createPalette(RED);
    assert.deepEqual(RED.routes, ['GET /redkern/redis/streams/status', 'POST /redkern/redis/streams/control', 'GET /redkern/redis/connections']);
    assert.deepEqual(RED.permissions, ['redkern.redis.read', 'redkern.redis.write', 'redkern.redis.read']);
  });

  it('RDS-STR-5 the node set creates one palette closure and shares it with every node type', () => {
    const RED = fakeRED();
    const types = [];
    RED.nodes.registerType = (type) => types.push(type);
    paletteSet(RED);
    assert.equal(types.length, 13);
    assert.equal(RED.routes.length, 3, 'routes are registered once per runtime load');
  });

  it('RDS-STR-5 two palette closures do not share state', () => {
    const first = createPalette(fakeRED());
    const second = createPalette(fakeRED());
    first.registry.add('s', 'g', '1-0', 'c', 60000);
    assert.equal(second.registry.has('s', 'g', '1-0'), false);
    assert.notEqual(first.metrics, second.metrics);
  });
});

describe('stream gc', () => {
  it('RDS-STR-13 keeps a consumer that read entries between XINFO and the delete', async () => {
    const client = {
      call: async () => [['name', 'late', 'pending', 0, 'idle', 999999, 'inactive', 999999]],
      eval: async () => -1
    };
    const result = await collectConsumers(client, 's', 'g', 1000);
    assert.deepEqual(result.removed, []);
    assert.equal(result.kept[0].raced, true);
  });
});
