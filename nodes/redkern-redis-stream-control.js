'use strict';

const { setMeta } = require('../lib/format.js');

module.exports = function register(RED, palette) {
  const { rk } = palette;

  function RedisStreamControlNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    k.configure(config, {
      action: { type: 'enum', values: ['', 'pause', 'resume', 'drain', 'status'], default: '' }
    });

    k.onInput(async (msg, send) => {
      const body = msg.payload && typeof msg.payload === 'object' && !Array.isArray(msg.payload) ? msg.payload : {};
      const pick = (field) => (msg[field] !== undefined ? msg[field] : body[field]);
      const result = await palette.executeControl({
        action: pick('action') || config.action,
        stream: pick('stream'),
        group: pick('group'),
        nodeId: pick('nodeId'),
        drainTimeoutMs: pick('drainTimeoutMs')
      });
      msg.payload = result;
      setMeta(msg, { control: { action: result.action, consumers: result.consumers.length } });
      const state = result.action === 'drain' && !result.drained ? 'warn' : result.action === 'pause' ? 'paused' : 'ok';
      k.status(state, `${result.action} ${result.consumers.length}`);
      send(msg);
    }, { concurrency: 4, maxQueue: 100 });
  }

  RED.nodes.registerType('redkern-redis-stream-control', RedisStreamControlNode);
};
