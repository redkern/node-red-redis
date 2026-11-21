'use strict';

const { codeError, setMeta, flatToObject, compact } = require('../lib/format.js');
const { startShared } = require('../lib/node-helpers.js');

/** One snapshot of a consumer group: PEL, lag, consumers and DLQ length. */
async function sample(client, { stream, group, deadLetterStream }) {
  const groups = await client.call('XINFO', 'GROUPS', stream);
  const info = (groups || []).map(flatToObject).find((item) => String(item.name) === group);
  if (!info) throw codeError('GROUP_NOT_FOUND', 'Consumer group does not exist');
  const summary = await client.xpending(stream, group);
  const pending = Number(summary?.[0]) || 0;
  const deadLetterLength = deadLetterStream ? Number(await client.xlen(deadLetterStream)) || 0 : null;
  return {
    stream,
    group,
    pending,
    deadLetterStream: deadLetterStream || null,
    deadLetterLength,
    consumers: Number(info.consumers) || 0,
    lag: info.lag === undefined || info.lag === null ? null : Number(info.lag),
    entriesRead: info['entries-read'] === undefined || info['entries-read'] === null ? null : Number(info['entries-read']),
    lastDeliveredId: info['last-delivered-id'] ? String(info['last-delivered-id']) : null,
    sampledAt: new Date().toISOString()
  };
}

module.exports = function register(RED, palette) {
  const { rk, metrics } = palette;

  function RedisStreamMetricsNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      streamKey: { type: 'str', default: '' },
      group: { type: 'str', default: '' },
      deadLetterStream: { type: 'str', default: '' }
    });
    const options = parsed.ok ? parsed.value : {};
    const shared = startShared(RED, k, node, config.server);
    const sampled = new Set();

    // RDS-STR-14: the palette metrics source is announced while at least one
    // stream-metrics node holds a lease; without the prometheus palette the
    // same text is served on the internal port when a metrics token is set.
    if (rk.enabled && k.metrics) k.metrics.acquire();
    if (rk.enabled && k.internalServer && palette.metricsToken) {
      k.track(k.internalServer.acquire(`metrics:${node.id}`, { nodeId: node.id }).ready, { label: 'internal server' });
    }

    k.onInput(async (msg, send) => {
      const stream = (typeof msg.stream === 'string' && msg.stream) || options.streamKey;
      const group = (typeof msg.group === 'string' && msg.group) || options.group;
      const deadLetterStream = (typeof msg.deadLetterStream === 'string' && msg.deadLetterStream) || options.deadLetterStream;
      if (!stream || !group) throw codeError('STREAM_GROUP_REQUIRED', 'Stream key and group are required');
      const snapshot = await sample(shared.client(), { stream, group, deadLetterStream });
      const labels = { stream, group };
      sampled.add(JSON.stringify(labels));
      metrics.set('redkern_redis_stream_pending', labels, snapshot.pending);
      metrics.set('redkern_redis_stream_consumers', labels, snapshot.consumers);
      if (snapshot.lag !== null) metrics.set('redkern_redis_stream_lag', labels, snapshot.lag);
      if (snapshot.deadLetterLength !== null) metrics.set('redkern_redis_stream_dead_letter_length', labels, snapshot.deadLetterLength);
      msg.payload = snapshot;
      setMeta(msg, { metrics: { stream, group } });
      if (snapshot.deadLetterLength) k.status('warn', `dlq ${compact(snapshot.deadLetterLength)}`);
      else k.status('ok', `pending ${compact(snapshot.pending)}`);
      send(msg);
    }, { concurrency: 4, maxQueue: 100, startQueueTimeoutMs: 30000 });

    k.onClose(() => {
      for (const key of sampled) {
        const labels = JSON.parse(key);
        for (const name of ['redkern_redis_stream_pending', 'redkern_redis_stream_consumers', 'redkern_redis_stream_lag', 'redkern_redis_stream_dead_letter_length']) {
          metrics.delete(name, labels);
        }
      }
    });
  }

  RED.nodes.registerType('redkern-redis-stream-metrics', RedisStreamMetricsNode);
};

module.exports.sample = sample;
