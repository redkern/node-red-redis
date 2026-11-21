'use strict';

// Runs against Redis with TLS: REDIS_TLS_PORT=6391 and NODE_EXTRA_CA_CERTS
// pointing to the test CA, whose server certificate is issued for redis.test
// and 127.0.0.1 (see scripts/start-test-tls.sh). Skipped when not set.

const { describe, it, before, after, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { helper, load, nextMessage, waitFor } = require('./helper.js');

const PORT = Number(process.env.REDIS_TLS_PORT || 0);
const skip = PORT ? false : 'REDIS_TLS_PORT is not set';

function tlsConfig(extra) {
  return { id: 'cfgtls', type: 'redkern-redis-config', host: '127.0.0.1', port: PORT, mode: 'standalone', tls: true, commandTimeout: 5000, connectTimeout: 2000, ...extra };
}

function statuses(node) {
  const list = [];
  node.on('call:status', (call) => list.push(call.args[0]));
  return list;
}

describe('redis over TLS', { skip }, () => {
  before(async () => { await new Promise((resolve) => helper.startServer(resolve)); });
  after(async () => { await new Promise((resolve) => helper.stopServer(resolve)); });
  afterEach(async () => { await helper.unload(); });

  it('RDS-CON-6 TLS connection verifies the server certificate and runs commands', async () => {
    await load([
      tlsConfig({ tlsServername: 'redis.test' }),
      { id: 'cmd', type: 'redkern-redis-command', server: 'cfgtls', command: 'PING', wires: [['out']] },
      { id: 'out', type: 'helper' }
    ]);
    const got = nextMessage(helper.getNode('out'), 10000);
    helper.getNode('cmd').receive({});
    assert.equal((await got).payload, 'PONG');
  });

  it('RDS-CON-6 a certificate that does not match the server name gives a red status', async () => {
    await load([
      tlsConfig({ tlsServername: 'wrong.test' }),
      { id: 'cmd', type: 'redkern-redis-command', server: 'cfgtls', command: 'PING', errorOutput: true, outputs: 2, wires: [['out'], ['err']] },
      { id: 'out', type: 'helper' },
      { id: 'err', type: 'helper' }
    ]);
    const seen = statuses(helper.getNode('cmd'));
    await waitFor(() => seen.some((status) => status.fill === 'red'), 10000, 50);
    assert.match(seen.find((status) => status.fill === 'red').text, /ERR_TLS_CERT/);
    assert.ok(!seen.some((status) => status.fill === 'green'));
  });
});
