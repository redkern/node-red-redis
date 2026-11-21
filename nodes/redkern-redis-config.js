'use strict';

const Redis = require('ioredis');
const { createRedisClient, classifyRedisError } = require('@redkern/node-red-kit/redis');
const { splitList } = require('../lib/format.js');

const TLS_REJECTED = /^(CERT_|ERR_TLS_|DEPTH_ZERO_SELF_SIGNED_CERT|SELF_SIGNED_CERT_IN_CHAIN|UNABLE_TO_)/;

const SCHEMA = Object.freeze({
  mode: { type: 'enum', values: ['standalone', 'cluster'], default: 'standalone' },
  host: { type: 'str', default: '127.0.0.1' },
  port: { type: 'int', min: 1, max: 65535, default: 6379 },
  clusterNodes: { type: 'str', default: '' },
  db: { type: 'int', min: 0, max: 1024, default: 0 },
  username: { type: 'str', default: '' },
  tls: { type: 'bool', default: false },
  tlsServername: { type: 'str', default: '' },
  connectTimeout: { type: 'int', min: 100, max: 600000, default: 10000 },
  commandTimeout: { type: 'int', min: 100, max: 600000, default: 5000 }
});

/**
 * Parses "host:port, host:port" into cluster seed nodes.
 * @param {string} value
 * @param {string} host fallback seed host
 * @param {number} port fallback seed port
 */
function parseClusterNodes(value, host, port) {
  const items = splitList(value);
  if (items.length === 0) return [{ host, port }];
  return items.map((item) => {
    const match = item.match(/^\[?([^\]]+?)\]?:(\d{1,5})$/);
    if (!match) throw Object.assign(new Error('Cluster nodes must be host:port, comma separated'), { code: 'CONFIG_INVALID' });
    const nodePort = Number(match[2]);
    if (nodePort < 1 || nodePort > 65535) throw Object.assign(new Error('Cluster node port is invalid'), { code: 'CONFIG_INVALID' });
    return { host: match[1], port: nodePort };
  });
}

module.exports = function register(RED, palette) {
  const { rk, kit } = palette;

  function RedisConfigNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, SCHEMA);
    const password = k.readSecret('password');
    const options = parsed.ok ? parsed.value : { mode: 'standalone' };
    const issues = parsed.ok ? [] : parsed.errors;
    let seeds = [];
    if (parsed.ok && options.mode === 'cluster') {
      try {
        seeds = parseClusterNodes(options.clusterNodes, options.host, options.port);
      } catch (error) {
        issues.push({ field: 'clusterNodes', code: 'CONFIG_INVALID', message: error.message });
        k.status('error', 'CONFIG_INVALID');
      }
    }

    const handles = { shared: undefined, subscriber: undefined, blocking: new Set() };
    const dependents = new Set();
    let dependentsIdle;
    let closing = false;

    function clientOptions(role, extra = {}) {
      if (issues.length) throw new kit.ConfigError(issues);
      return {
        ioredis: Redis,
        domain: 'redis',
        configId: node.id.replace(/[^A-Za-z0-9_-]/g, '_').padEnd(8, '_'),
        role,
        mode: options.mode,
        host: options.host,
        port: options.port,
        nodes: options.mode === 'cluster' ? seeds : undefined,
        db: options.mode === 'cluster' ? 0 : options.db,
        tls: options.tls ? (options.tlsServername ? { servername: options.tlsServername } : {}) : undefined,
        username: options.username || undefined,
        password: password || undefined,
        connectTimeout: options.connectTimeout,
        commandTimeout: role === 'shared' ? options.commandTimeout : extra.commandTimeout,
        blockMs: extra.blockMs,
        logger: k.log
      };
    }

    function connections() {
      const ready = (handle) => (handle && handle.client.status === 'ready' ? 1 : 0);
      let blocking = 0;
      for (const handle of handles.blocking) blocking += ready(handle);
      return { shared: ready(handles.shared), blocking, subscriber: ready(handles.subscriber) };
    }

    function describe() {
      return {
        name: node.name || undefined,
        mode: options.mode,
        state: issues.length ? 'invalid' : handles.shared ? handles.shared.client.status : 'idle',
        connections: connections(),
        dependents: dependents.size
      };
    }

    function updateStatus() {
      if (issues.length) return;
      const counts = connections();
      const total = counts.shared + counts.blocking + counts.subscriber;
      const state = handles.shared?.client.status;
      if (state && state !== 'ready' && state !== 'wait') k.status('warn', `${state} ${total}`);
      else k.status('ok', `connections ${total}`);
    }

    function watch(handle) {
      for (const event of ['ready', 'end', 'reconnecting', 'close']) handle.client.on(event, updateStatus);
      handle.client.on('error', (error) => { handle.lastError = error; });
      handle.client.on('ready', () => { handle.lastError = undefined; });
      return handle;
    }

    /**
     * ioredis rejects connect() with a generic "Connection is closed" after an
     * AUTH failure; surface the reply error so kit classifies it as auth
     * (red status right away, KIT-LIFE-3).
     */
    async function connect(handle) {
      try {
        await handle.connect();
      } catch (error) {
        const last = handle.lastError;
        if (last && classifyRedisError(last)) {
          const code = String(last.message || '').match(/^[A-Z]+/)?.[0] || 'REDIS_AUTH';
          throw Object.assign(new Error(last.message), { code, cause: error });
        }
        // A rejected server certificate does not heal on retry: keep its code
        // so kit classifies it as auth and shows a red status.
        if (last && typeof last.code === 'string' && TLS_REJECTED.test(last.code)) {
          throw Object.assign(new Error(last.message), { code: last.code, cause: error });
        }
        throw error;
      }
    }

    /** Lazily created shared client for non-blocking commands (RDS-CON-2). */
    async function shared() {
      if (closing) throw Object.assign(new Error('Redis config is closing'), { code: 'NODE_CLOSING' });
      if (!handles.shared) handles.shared = watch(createRedisClient(clientOptions('shared')));
      await connect(handles.shared);
      return handles.shared.client;
    }

    /** Dedicated blocking connection, one per stream-in / list-in node. */
    function createBlocking({ blockMs = 0, commandTimeout } = {}) {
      if (closing) throw Object.assign(new Error('Redis config is closing'), { code: 'NODE_CLOSING' });
      const handle = watch(createRedisClient(clientOptions('blocking', { blockMs, commandTimeout })));
      handles.blocking.add(handle);
      const release = async () => {
        if (!handles.blocking.delete(handle)) return;
        handle.force();
        updateStatus();
      };
      return { client: handle.client, connect: () => connect(handle), release };
    }

    /** Pub/Sub multiplexer on one subscriber connection per config node. */
    const channels = new Map();
    const patterns = new Map();
    async function subscriberClient() {
      if (!handles.subscriber) {
        const handle = watch(createRedisClient(clientOptions('subscriber')));
        handle.client.on('message', (channel, message) => {
          for (const listener of channels.get(channel) || []) listener({ channel, message });
        });
        handle.client.on('pmessage', (pattern, channel, message) => {
          for (const listener of patterns.get(pattern) || []) listener({ pattern, channel, message });
        });
        handles.subscriber = handle;
      }
      await connect(handles.subscriber);
      return handles.subscriber.client;
    }

    async function subscribe(mode, names, listener) {
      const map = mode === 'psubscribe' ? patterns : channels;
      const fresh = [];
      for (const name of names) {
        let set = map.get(name);
        if (!set) {
          set = new Set();
          map.set(name, set);
          fresh.push(name);
        }
        set.add(listener);
      }
      const release = async () => {
        const gone = [];
        for (const name of names) {
          const set = map.get(name);
          if (!set) continue;
          set.delete(listener);
          if (set.size === 0) {
            map.delete(name);
            gone.push(name);
          }
        }
        const client = handles.subscriber?.client;
        if (gone.length && client && client.status === 'ready') {
          try {
            if (mode === 'psubscribe') await client.punsubscribe(...gone);
            else await client.unsubscribe(...gone);
          } catch { /* the connection closes anyway when the last dependent leaves */ }
        }
      };
      try {
        const client = await subscriberClient();
        if (fresh.length) {
          if (mode === 'psubscribe') await client.psubscribe(...fresh);
          else await client.subscribe(...fresh);
        }
      } catch (error) {
        await release();
        throw error;
      }
      return release;
    }

    /** Dependent nodes register so the config closes its clients after them. */
    function register(id) {
      dependents.add(id);
      return () => {
        dependents.delete(id);
        if (dependents.size === 0 && dependentsIdle) dependentsIdle();
      };
    }

    k.onClose(async ({ deadline }) => {
      closing = true;
      if (dependents.size) {
        const idle = new Promise((resolve) => { dependentsIdle = resolve; });
        const wait = Math.max(0, deadline - Date.now() - 1000);
        await Promise.race([idle, new Promise((resolve) => setTimeout(resolve, wait).unref?.())]);
      }
      for (const handle of handles.blocking) handle.force();
      handles.blocking.clear();
      await Promise.allSettled([handles.shared?.close(), handles.subscriber?.close()]);
    }, {
      force() {
        handles.shared?.force();
        handles.subscriber?.force();
        for (const handle of handles.blocking) handle.force();
      }
    });
    k.onClose(palette.addConfig(node.id, describe));

    node.redkern = {
      mode: options.mode,
      issues,
      shared,
      createBlocking,
      subscribe,
      register,
      describe,
      classify: (error) => classifyRedisError(error)
    };
    if (!issues.length) k.status('idle', 'idle');
  }

  RED.nodes.registerType('redkern-redis-config', RedisConfigNode, {
    credentials: { password: { type: 'password' } }
  });
};

module.exports.parseClusterNodes = parseClusterNodes;
