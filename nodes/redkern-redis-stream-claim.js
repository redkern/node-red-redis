'use strict';

const { codeError, setMeta, decodeFields } = require('../lib/format.js');
const { startShared } = require('../lib/node-helpers.js');

module.exports = function register(RED, palette) {
  const { rk, kit } = palette;

  function RedisStreamClaimNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      streamKey: { type: 'str', default: '' },
      group: { type: 'str', default: '' },
      consumer: { type: 'str', default: '' },
      minIdleMs: { type: 'int', min: 0, max: 86400000, default: 60000 },
      count: { type: 'int', min: 1, max: 100000, default: 100 },
      maxMessages: { type: 'int', min: 1, max: 1000000, default: 1000 },
      legacyMsgFields: { type: 'bool', default: false }
    });
    const options = parsed.ok ? parsed.value : {};
    const shared = startShared(RED, k, node, config.server);

    k.onInput(async (msg, send) => {
      const meta = msg?.redkern?.redis || {};
      const stream = (typeof msg.stream === 'string' && msg.stream) || meta.stream || options.streamKey;
      const group = (typeof msg.group === 'string' && msg.group) || meta.group || options.group;
      const consumer = (typeof msg.consumer === 'string' && msg.consumer) || options.consumer || `${kit.instanceId()}-${node.id}`;
      const minIdleMs = Number.isSafeInteger(msg.minIdleMs) && msg.minIdleMs >= 0 ? msg.minIdleMs : options.minIdleMs;
      if (!stream || !group) throw codeError('STREAM_GROUP_REQUIRED', 'Stream key and group are required');
      const maxMessages = Math.max(options.count, options.maxMessages);
      const claimed = [];
      let cursor = '0-0';
      do {
        const remaining = maxMessages - claimed.length;
        const [next, entries] = await shared.client().xautoclaim(stream, group, consumer, minIdleMs, cursor, 'COUNT', Math.min(options.count, remaining));
        cursor = String(next);
        for (const [id, flat] of entries || []) {
          if (!Array.isArray(flat)) continue;
          const item = { id, payload: decodeFields(flat) };
          if (options.legacyMsgFields) item.streamId = id;
          claimed.push(item);
        }
      } while (cursor !== '0-0' && claimed.length < maxMessages);
      msg.payload = claimed;
      setMeta(msg, { claim: { stream, group, consumer, complete: cursor === '0-0', count: claimed.length } });
      if (options.legacyMsgFields) {
        msg.stream = stream;
        msg.group = group;
        msg.consumer = consumer;
      }
      k.status(cursor === '0-0' ? 'ok' : 'warn', `claimed ${claimed.length}${cursor === '0-0' ? '' : '+'}`);
      send(msg);
    }, { concurrency: 1, maxQueue: 100, startQueueTimeoutMs: 30000 });
  }

  RED.nodes.registerType('redkern-redis-stream-claim', RedisStreamClaimNode);
};
