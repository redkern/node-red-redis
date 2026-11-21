'use strict';

const { codeError, setMeta } = require('../lib/format.js');
const { startShared } = require('../lib/node-helpers.js');

/**
 * Reads the entry IDs and their stream/group from msg (RDS-STR-12): first
 * msg.redkern.redis, then the legacy fields, then the node configuration.
 */
function resolveTarget(msg, options) {
  const meta = msg?.redkern?.redis && typeof msg.redkern.redis === 'object' ? msg.redkern.redis : {};
  const rawIds = meta.id !== undefined ? meta.id : msg.streamId;
  const ids = (Array.isArray(rawIds) ? rawIds : [rawIds]).filter((id) => typeof id === 'string' && id !== '');
  if (ids.length === 0) throw codeError('STREAM_ID_REQUIRED', 'msg.redkern.redis.id (or legacy msg.streamId) is required');
  const stream = meta.stream || msg._streamKey || options.streamKey;
  const group = meta.group || msg._streamGroup || options.group;
  if (!stream || !group) throw codeError('STREAM_GROUP_REQUIRED', 'Stream key and group are required');
  return { ids, stream: String(stream), group: String(group) };
}

module.exports = function register(RED, palette) {
  const { rk, registry, metrics } = palette;

  function RedisStreamAckNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      streamKey: { type: 'str', default: '' },
      group: { type: 'str', default: '' },
      deleteAfterAck: { type: 'bool', default: false },
      legacyMsgFields: { type: 'bool', default: false }
    });
    const options = parsed.ok ? parsed.value : {};
    const shared = startShared(RED, k, node, config.server);
    let total = 0;

    k.onInput(async (msg, send) => {
      const { ids, stream, group } = resolveTarget(msg, options);
      // One XACK for the whole batch.
      const acked = Number(await shared.client().xack(stream, group, ...ids)) || 0;
      registry.remove(stream, group, ids);
      if (acked) metrics.inc('redkern_redis_acked_total', { stream, group }, acked);
      const deleteAfterAck = msg.deleteAfterAck === undefined ? options.deleteAfterAck : msg.deleteAfterAck === true;
      let deleted;
      if (deleteAfterAck && acked > 0) {
        // XDEL after XACK: a failed delete leaves an auditable entry, never an
        // unacknowledged loss.
        deleted = Number(await shared.client().xdel(stream, ...ids)) || 0;
      }
      setMeta(msg, { ack: deleted === undefined ? { acked } : { acked, deleted } });
      if (options.legacyMsgFields) {
        msg.acked = acked;
        if (deleted !== undefined) msg.deleted = deleted;
      }
      total += acked;
      k.status('ok', `acked ${total}`);
      if (Number(config.outputs) > 0) send(msg);
    }, { concurrency: 64, maxQueue: 10000, startQueueTimeoutMs: 30000 });
  }

  RED.nodes.registerType('redkern-redis-stream-ack', RedisStreamAckNode);
};

module.exports.resolveTarget = resolveTarget;
