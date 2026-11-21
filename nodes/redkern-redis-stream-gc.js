'use strict';

const { codeError, setMeta, flatToObject, compact } = require('../lib/format.js');
const { startShared } = require('../lib/node-helpers.js');

const DELETE_IF_EMPTY = `
if #redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]) > 0 then return -1 end
return redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])`;

/**
 * Removes inactive consumers whose PEL is empty (RDS-STR-13). A consumer with
 * pending entries is never deleted: XGROUP DELCONSUMER would drop its PEL.
 */
async function collectConsumers(client, stream, group, minIdleMs) {
  const raw = await client.call('XINFO', 'CONSUMERS', stream, group);
  const removed = [];
  const kept = [];
  for (const flat of raw || []) {
    const info = flatToObject(flat);
    const name = String(info.name);
    const pending = Number(info.pending) || 0;
    // Redis 7.2 reports "inactive" (time since the last successful read);
    // older versions only have "idle".
    const inactive = info.inactive !== undefined && Number(info.inactive) >= 0 ? Number(info.inactive) : Number(info.idle) || 0;
    if (pending === 0 && inactive >= minIdleMs) {
      // Check the PEL and delete in one script: DELCONSUMER drops the PEL of
      // a consumer that read entries after XINFO was taken.
      const deleted = Number(await client.eval(DELETE_IF_EMPTY, 1, stream, group, name));
      if (deleted === 0) removed.push({ name, idleMs: inactive });
      else kept.push({ name, pending: Math.max(0, deleted), idleMs: inactive, raced: true });
    } else {
      kept.push({ name, pending, idleMs: inactive });
    }
  }
  return { removed, kept };
}

module.exports = function register(RED, palette) {
  const { rk } = palette;

  function RedisStreamGcNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      streamKey: { type: 'str', default: '' },
      group: { type: 'str', default: '' },
      minIdleMs: { type: 'int', min: 0, max: 31536000000, default: 600000 },
      legacyMsgFields: { type: 'bool', default: false }
    });
    const options = parsed.ok ? parsed.value : {};
    const shared = startShared(RED, k, node, config.server);
    let total = 0;

    k.onInput(async (msg, send) => {
      const stream = (typeof msg.stream === 'string' && msg.stream) || options.streamKey;
      const group = (typeof msg.group === 'string' && msg.group) || options.group;
      const minIdleMs = Number.isSafeInteger(msg.minIdleMs) && msg.minIdleMs >= 0 ? msg.minIdleMs : options.minIdleMs;
      if (!stream || !group) throw codeError('STREAM_GROUP_REQUIRED', 'Stream key and group are required');
      const result = await collectConsumers(shared.client(), stream, group, minIdleMs);
      msg.payload = result;
      setMeta(msg, { gc: { stream, group, removed: result.removed.length, kept: result.kept.length } });
      if (options.legacyMsgFields) {
        msg.stream = stream;
        msg.group = group;
      }
      total += result.removed.length;
      k.status('ok', `removed ${compact(total)}`);
      send(msg);
    }, { concurrency: 1, maxQueue: 100, startQueueTimeoutMs: 30000 });
  }

  RED.nodes.registerType('redkern-redis-stream-gc', RedisStreamGcNode);
};

module.exports.collectConsumers = collectConsumers;
