'use strict';

const { splitList, replaceMeta } = require('../lib/format.js');
const { requireConfig, classify } = require('../lib/node-helpers.js');

module.exports = function register(RED, palette) {
  const { rk, kit } = palette;

  function RedisSubscribeNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      mode: { type: 'enum', values: ['subscribe', 'psubscribe'], default: 'subscribe' },
      channels: { type: 'str', required: true }
    });
    const options = parsed.ok ? parsed.value : {};
    const names = splitList(options.channels);
    let received = 0;
    let unregister;
    let started = false;

    k.onStart(async (attempt) => {
      if (names.length === 0) {
        throw new kit.ConfigError([{ field: 'channels', code: 'REQUIRED', message: 'At least one channel or pattern is required' }]);
      }
      const api = requireConfig(RED, config.server);
      if (!unregister) {
        unregister = api.register(node.id);
        k.onClose(() => unregister());
      }
      // RDS-CMD-6: ioredis resubscribes automatically after a reconnect
      // (subscriber role has autoResubscribe).
      const release = await api.subscribe(options.mode, names, ({ pattern, channel, message }) => {
        received += 1;
        const msg = { topic: channel, payload: message };
        if (pattern !== undefined) msg.pattern = pattern;
        replaceMeta(msg, pattern === undefined ? { channel } : { channel, pattern });
        node.send(msg);
        k.status('ok', `received ${received}`);
      });
      attempt.onClose(release);
      started = true;
    }, { classify }).then(() => {
      if (started) k.status('ok', `listening ${names.length}`);
    });
  }

  RED.nodes.registerType('redkern-redis-subscribe', RedisSubscribeNode);
};
