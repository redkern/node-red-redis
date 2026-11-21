'use strict';

const net = require('node:net');

/**
 * Small TCP proxy with fault injection, in place of toxiproxy (kit standard 8,
 * rule 4): down()/up() simulate Redis being unreachable, cutNextReply()
 * forwards the next command to Redis and closes the connection on the first
 * byte of the reply, like toxiproxy limit_data with bytes: 0.
 */
function createProxy({ port, targetPort, targetHost = '127.0.0.1' }) {
  const sockets = new Set();
  let server;
  let cutArmed = false;

  function handle(client) {
    const upstream = net.connect({ port: targetPort, host: targetHost });
    sockets.add(client);
    sockets.add(upstream);
    const drop = () => {
      client.destroy();
      upstream.destroy();
      sockets.delete(client);
      sockets.delete(upstream);
    };
    client.on('data', (chunk) => upstream.write(chunk));
    upstream.on('data', (chunk) => {
      if (cutArmed) {
        cutArmed = false;
        drop();
        return;
      }
      client.write(chunk);
    });
    client.on('error', drop);
    upstream.on('error', drop);
    client.on('close', drop);
    upstream.on('close', drop);
  }

  async function up() {
    if (server) return;
    server = net.createServer(handle);
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, '127.0.0.1', resolve);
    });
  }

  async function down() {
    if (!server) return;
    const closing = new Promise((resolve) => server.close(resolve));
    for (const socket of sockets) socket.destroy();
    sockets.clear();
    server = undefined;
    await closing;
  }

  return {
    up,
    down,
    /** The next reply chunk from Redis is dropped together with the connection. */
    cutNextReply() { cutArmed = true; },
    get connections() { return sockets.size / 2; }
  };
}

module.exports = { createProxy };
