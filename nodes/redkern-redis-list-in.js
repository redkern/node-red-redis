'use strict';

const { ListConsumer } = require('../lib/list-consumer.js');
const { replaceMeta, compact } = require('../lib/format.js');
const { requireConfig, classify } = require('../lib/node-helpers.js');

module.exports = function register(RED, palette) {
  const { rk, kit, completion } = palette;

  function RedisListInNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      key: { type: 'str', required: true },
      direction: { type: 'enum', values: ['BLPOP', 'BRPOP'], default: 'BLPOP' },
      timeout: { type: 'int', min: 0, max: 3600, default: 5 },
      prefetch: { type: 'int', min: 0, max: 100000, default: 1 },
      releaseTimeoutMs: { type: 'int', min: 1000, max: 86400000, default: 300000 }
    });
    const options = parsed.ok ? parsed.value : {};
    let unregister;
    let consumer;

    function updateStatus(state) {
      if (state.lastError) k.status('error', `retry ${state.lastError}`.toLowerCase());
      else if (state.prefetch > 0 && state.inflight >= state.prefetch) k.status('warn', `waiting ${state.inflight}/${state.prefetch}`);
      else k.status('ok', `received ${compact(state.received)}`);
    }

    let loop = null;
    k.onStart(async (attempt) => {
      const api = requireConfig(RED, config.server);
      if (!unregister) {
        unregister = api.register(node.id);
        k.onClose(() => unregister());
      }
      const blockMs = options.timeout * 1000;
      const blocking = api.createBlocking({
        blockMs,
        commandTimeout: options.timeout > 0 ? blockMs + 10000 : undefined
      });
      attempt.onClose(blocking.release, { force: blocking.release });
      await blocking.connect();
      let clientId = null;
      try { clientId = await blocking.client.client('ID'); } catch { /* without the ID close waits for the pop instead */ }
      // Runs before the release above (close stack is LIFO): end a BLPOP in
      // progress so an element popped at that moment still reaches the
      // pushback of the loop while the connection is open.
      attempt.onClose(async () => {
        if (!loop) return;
        if (clientId !== null) {
          try { await (await api.shared()).client('UNBLOCK', clientId); } catch { /* ACL without client|unblock */ }
        }
        await Promise.race([loop.catch(() => {}), kit.sleep(1500)]);
      });
      consumer = new ListConsumer({
        client: blocking.client,
        key: options.key,
        direction: options.direction,
        timeoutSec: options.timeout,
        prefetch: options.prefetch,
        releaseTimeoutMs: options.releaseTimeoutMs,
        tracker: completion,
        sleep: kit.sleep,
        backoff: kit.backoff({ initial: 500, max: 30000, jitter: 'equal' }),
        log: k.log,
        onState: updateStatus,
        newId: () => RED.util.generateId(),
        // An unwired output never completes; do not hold a prefetch slot.
        wired: () => Array.isArray(node.wires) && node.wires.some((port) => port.length > 0),
        emit: (key, value, msgId) => {
          const msg = { _msgid: msgId, topic: key, payload: value };
          replaceMeta(msg, { list: key });
          node.send(msg);
        }
      });
    }, { classify }).then(() => {
      if (!consumer || k.signal.aborted) return;
      updateStatus(consumer.snapshot());
      loop = consumer.run(k.signal);
      k.track(loop, { label: 'list-in loop' });
    });
  }

  RED.nodes.registerType('redkern-redis-list-in', RedisListInNode);
};
