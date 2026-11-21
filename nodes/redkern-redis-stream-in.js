'use strict';

const { StreamConsumer } = require('../lib/stream-consumer.js');
const { slotIssue } = require('../lib/dlq.js');
const { replaceMeta, compact } = require('../lib/format.js');
const { requireConfig, classify } = require('../lib/node-helpers.js');

const SCHEMA = Object.freeze({
  server: { type: 'str', required: true },
  streamKey: { type: 'str', required: true },
  group: { type: 'str', required: true },
  consumer: { type: 'str', default: '' },
  ackMode: { type: 'enum', values: ['manual', 'auto'], default: 'manual' },
  startId: { type: 'str', default: '$' },
  maxPending: { type: 'int', min: 1, max: 1000000, default: 1000 },
  backpressureScope: { type: 'enum', values: ['consumer', 'group'], default: 'consumer' },
  maxDeliveries: { type: 'int', min: 0, max: 1000, default: 5 },
  deadLetterStream: { type: 'str', default: '' },
  autoClaim: { type: 'bool', default: true },
  reclaimIdleMs: { type: 'int', min: 1000, max: 86400000, default: 60000 },
  reclaimIntervalMs: { type: 'int', min: 1000, max: 86400000, default: 30000 },
  reclaimCount: { type: 'int', min: 1, max: 100000, default: 100 },
  drainTimeoutMs: { type: 'int', min: 0, max: 1800000, default: 30000 },
  startPaused: { type: 'bool', default: false },
  count: { type: 'int', min: 1, max: 100000, default: 100 },
  blockMs: { type: 'int', min: 1, max: 600000, default: 5000 },
  readIntervalMs: { type: 'int', min: 0, max: 3600000, default: 0 },
  rateLimitPerSecond: { type: 'float', min: 0, max: 1000000, default: 0 },
  batchWindowMs: { type: 'int', min: 0, max: 3600000, default: 0 },
  batchIntervalMs: { type: 'int', min: 0, max: 3600000, default: 0 },
  capacityCheckIntervalMs: { type: 'int', min: 50, max: 600000, default: 250 },
  capacityPollMs: { type: 'int', min: 50, max: 600000, default: 500 },
  initialBackoffMs: { type: 'int', min: 50, max: 600000, default: 500 },
  maxBackoffMs: { type: 'int', min: 50, max: 3600000, default: 30000 },
  backoffMultiplier: { type: 'float', min: 1, max: 10, default: 2 },
  legacyMsgFields: { type: 'bool', default: false }
});

module.exports = function register(RED, palette) {
  const { rk, kit, registry, metrics } = palette;

  function RedisStreamInNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, SCHEMA);
    const options = parsed.ok ? parsed.value : {};
    const consumerName = options.consumer || `${kit.instanceId()}-${node.id}`;
    const labels = { stream: options.streamKey, group: options.group };
    let consumer;
    let unregister;
    let lastDelivered = 0;
    let lastRateAt = Date.now();
    let rate = 0;

    function updateStatus() {
      if (!consumer) return;
      const state = consumer.snapshot();
      metrics.set('redkern_redis_consumer_paused', { ...labels, node: node.id }, state.paused ? 1 : 0);
      metrics.set('redkern_redis_consumer_inflight', { ...labels, node: node.id }, state.inflight);
      if (state.lastError) k.status('error', `retry ${String(state.lastError).toLowerCase()}`);
      else if (state.paused) k.status('paused', `paused ${compact(state.inflight + state.held)} pending`);
      else if (state.backpressure) k.status('warn', `full ${compact(state.inflight)}/${compact(state.maxPending)}`);
      else if (state.recovering) k.status('starting', 'recovering');
      else if (rate > 0) k.status('ok', `consuming ${compact(rate)}/s`);
      else k.status('ok', `reading ${compact(state.inflight)}/${compact(state.maxPending)}`);
    }

    function emit({ entries, batched }) {
      const ids = entries.map((entry) => entry.id);
      const msg = {
        _msgid: RED.util.generateId(),
        payload: batched ? entries.map((entry) => entry.payload) : entries[0].payload
      };
      replaceMeta(msg, {
        stream: options.streamKey,
        group: options.group,
        id: batched ? ids : ids[0],
        consumer: consumerName,
        deliveries: batched ? entries.map((entry) => entry.deliveries) : entries[0].deliveries
      });
      if (options.legacyMsgFields) {
        msg.streamId = batched ? ids : ids[0];
        msg.stream = options.streamKey;
        msg.group = options.group;
        msg._streamKey = options.streamKey;
        msg._streamGroup = options.group;
        msg.consumer = consumerName;
      }
      node.send(msg);
    }

    k.onStart(async (attempt) => {
      const api = requireConfig(RED, config.server);
      const deadLetterStream = options.deadLetterStream || `${options.streamKey}:dlq`;
      if (api.mode === 'cluster' && options.maxDeliveries > 0) {
        const issue = slotIssue(options.streamKey, deadLetterStream);
        if (issue) throw new kit.ConfigError([issue]);
      }
      if (!unregister) {
        unregister = api.register(node.id);
        k.onClose(() => unregister());
      }
      const shared = await api.shared();
      const blocking = api.createBlocking({ blockMs: options.blockMs, commandTimeout: options.blockMs + 10000 });
      attempt.onClose(blocking.release, { force: blocking.release });
      await blocking.connect();
      consumer = new StreamConsumer({
        shared,
        blocking: blocking.client,
        stream: options.streamKey,
        group: options.group,
        consumer: consumerName,
        registry,
        metrics,
        log: k.log,
        emit,
        onState: updateStatus,
        clock: { now: Date.now, sleep: kit.sleep },
        options: { ...options, deadLetterStream }
      });
      // RDS-STR-1: the group exists (MKSTREAM) before the node reports ready.
      await consumer.ensureGroup();
    }, { classify, startAlertMs: 60000 }).then(() => {
      if (!consumer || k.signal.aborted) return;
      const entry = { nodeId: node.id, consumer, drainTimeoutMs: options.drainTimeoutMs };
      k.onClose(palette.addConsumer(entry));
      if (rk.enabled && k.internalServer) {
        const lease = k.internalServer.acquire(node.id, entry);
        k.track(lease.ready, { label: 'internal server' });
      }
      const timer = setInterval(() => {
        const now = Date.now();
        rate = (consumer.delivered - lastDelivered) * 1000 / Math.max(1, now - lastRateAt);
        lastDelivered = consumer.delivered;
        lastRateAt = now;
        updateStatus();
      }, 1000);
      timer.unref?.();
      k.onClose(() => clearInterval(timer));
      updateStatus();
      k.track(consumer.run(k.signal), { label: 'stream-in loop' });
    });

    k.onClose(({ removed }) => {
      metrics.delete('redkern_redis_consumer_paused', { ...labels, node: node.id });
      metrics.delete('redkern_redis_consumer_inflight', { ...labels, node: node.id });
      // On a redeploy the registry survives so the new instance does not
      // resend entries the flow is still processing; a removed node clears it.
      if (removed && parsed.ok) registry.clearConsumer(options.streamKey, options.group, consumerName);
    });
  }

  RED.nodes.registerType('redkern-redis-stream-in', RedisStreamInNode);
};
