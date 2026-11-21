'use strict';

const kit = require('@redkern/node-red-kit');
const { InflightRegistry } = require('./inflight.js');
const { MetricsRegistry, CONTENT_TYPE } = require('./metrics.js');
const { parseCommand, executeControl } = require('./control.js');
const { createCompletionTracker } = require('./completion.js');

const DOMAIN = 'redis';
const SETTINGS_TYPE = 'redkern-redis-config';
const INTERNAL_PORT = 9551;

function hasToken(RED, name) {
  const fromEnv = kit.readEnv(`REDKERN_REDIS_${name.toUpperCase()}_TOKEN`);
  if (typeof fromEnv === 'string' && fromEnv.trim() !== '') return true;
  const fromSettings = RED.settings?.[`redkernRedisConfig${name[0].toUpperCase()}${name.slice(1)}Token`];
  return typeof fromSettings === 'string' && fromSettings.trim() !== '';
}

/**
 * Palette closure shared by every node file of this package (KIT-ARCH-2,
 * RDS-STR-5):
 * one kit context, the in-flight registry (RDS-STR-5), the metrics registry
 * (RDS-STR-14), stream-in consumers for control, and the HTTP routes.
 * @param {any} RED
 */
function createPalette(RED) {
  let palette;

  const registry = new InflightRegistry();
  const metrics = new MetricsRegistry();
  /** @type {Map<string, { nodeId: string, consumer: any, drainTimeoutMs: number }>} */
  const consumers = new Map();
  /** @type {Map<string, () => object>} */
  const configs = new Map();
  const metricsToken = hasToken(RED, 'metrics');
  const drainToken = hasToken(RED, 'drain');
  // KIT-HTTP-6: one internal listener is either loopback-only (all routes
  // 'local', bound to 127.0.0.1) or token-only (bound to 0.0.0.0). Without the
  // metrics route drain stays loopback-only without a token; with it drain
  // needs its own token, REDKERN_REDIS_DRAIN_TOKEN.
  const drainAuth = metricsToken ? (drainToken ? { token: 'drain' } : null) : 'local';
  const secrets = {};
  if (metricsToken) secrets.metrics = { required: true };
  if (metricsToken && drainToken) secrets.drain = { required: true };

  metrics.collect(() => {
    const totals = { shared: 0, blocking: 0, subscriber: 0 };
    for (const describe of configs.values()) {
      const counts = describe().connections;
      for (const role of Object.keys(totals)) totals[role] += counts[role] || 0;
    }
    for (const [role, value] of Object.entries(totals)) metrics.set('redkern_redis_connections', { role }, value);
  });

  const internalRoutes = [];
  if (drainAuth) internalRoutes.push({
    method: 'POST',
    path: '/streams/drain',
    auth: drainAuth,
    handler: async (request, response, owners) => {
      const body = request.body && typeof request.body === 'object' ? request.body : {};
      const command = parseCommand({ ...body, action: 'drain' });
      const consumersOnly = [...owners.values()].filter((owner) => owner && owner.consumer);
      const result = await executeControl(consumersOnly, command);
      response.status(result.drained ? 200 : 504).json(result);
    }
  });
  if (metricsToken) {
    internalRoutes.push({
      method: 'GET',
      path: '/metrics',
      auth: { token: 'metrics' },
      handler: async (_request, response) => response.text(metrics.render(), CONTENT_TYPE)
    });
  }

  const rk = kit.init(RED, {
    domain: DOMAIN,
    settingsType: SETTINGS_TYPE,
    secrets,
    // Admin control can run drain, which waits up to drainTimeoutMs (30 min max).
    handlerTimeoutMs: 31 * 60 * 1000,
    metrics: { contentType: CONTENT_TYPE, metrics: () => metrics.render() },
    internalServer: {
      port: INTERNAL_PORT,
      // The drain route only waits; give it room beyond the longest sane drain.
      handlerTimeoutMs: 31 * 60 * 1000,
      routes: internalRoutes
    }
  });

  if (metricsToken && !drainToken) {
    RED.log.warn('[redkern:redis] The internal port serves /metrics with a token, so the loopback drain route is off: set REDKERN_REDIS_DRAIN_TOKEN for preStop drain, or scrape metrics through the prometheus palette instead');
  }

  function controlError(error) {
    if (error?.code?.startsWith('CONTROL_')) throw new kit.PublicError(400, error.message, { code: error.code });
    throw error;
  }

  const statusRoute = rk.createRouteRegistry({
    method: 'GET',
    path: '/streams/status',
    permission: rk.names.permRead,
    handler: async (_request, response, active) => {
      response.json(await executeControl(active.values(), { action: 'status' }));
    }
  });
  const controlRoute = rk.createRouteRegistry({
    method: 'POST',
    path: '/streams/control',
    permission: rk.names.permWrite,
    handler: async (request, response, active) => {
      let command;
      try { command = parseCommand(request.body); } catch (error) { controlError(error); }
      response.json(await executeControl(active.values(), command));
    }
  });
  const connectionsRoute = rk.createRouteRegistry({
    method: 'GET',
    path: '/connections',
    permission: rk.names.permRead,
    handler: async (request, response, active) => {
      const id = typeof request.query?.id === 'string' ? request.query.id : undefined;
      const list = [...active.entries()]
        .filter(([configId]) => !id || configId === id)
        .map(([configId, describe]) => ({ id: configId, ...describe() }));
      response.json({ configs: list });
    }
  });

  palette = {
    kit,
    rk,
    registry,
    metrics,
    metricsToken,
    consumers,
    configs,
    completion: createCompletionTracker(RED),
    /** Adds a stream-in consumer to node control and both admin routes. */
    addConsumer(entry) {
      consumers.set(entry.nodeId, entry);
      statusRoute.add(entry.nodeId, entry);
      controlRoute.add(entry.nodeId, entry);
      return () => {
        consumers.delete(entry.nodeId);
        statusRoute.remove(entry.nodeId);
        controlRoute.remove(entry.nodeId);
      };
    },
    addConfig(id, describe) {
      configs.set(id, describe);
      connectionsRoute.add(id, describe);
      return () => {
        configs.delete(id);
        connectionsRoute.remove(id);
      };
    },
    executeControl(command) {
      return executeControl(consumers.values(), parseCommand(command));
    }
  };
  return palette;
}

module.exports = { createPalette, DOMAIN, SETTINGS_TYPE, INTERNAL_PORT };
