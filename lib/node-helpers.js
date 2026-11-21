'use strict';

const kit = require('@redkern/node-red-kit');
const { classifyRedisError } = require('@redkern/node-red-kit/redis');

/** Schema fields shared by nodes that run input through k.onInput (RDS-CMD-8). */
const INPUT_SCHEMA = Object.freeze({
  concurrency: { type: 'int', min: 1, max: 10000, default: 64 },
  maxQueue: { type: 'int', min: 0, max: 1000000, default: 10000 },
  errorOutput: { type: 'bool', default: false }
});

const START_QUEUE_TIMEOUT_MS = 30000;

/**
 * Resolves the redkern-redis-config node referenced by `server`.
 * @returns {any} the config node's `redkern` API
 */
function requireConfig(RED, serverId) {
  const configNode = serverId ? RED.nodes.getNode(serverId) : null;
  if (!configNode || !configNode.redkern) {
    throw new kit.ConfigError([{ field: 'server', code: 'CONFIG_NODE_MISSING', message: 'Redis config node is not set or not deployed' }]);
  }
  if (configNode.redkern.issues.length) throw new kit.ConfigError(configNode.redkern.issues);
  return configNode.redkern;
}

function classify(error) {
  if (error?.name === 'ConfigError' || error?.code === 'CONFIG_INVALID') return 'config';
  // An ACL without the needed command or key, or a key of another type, does
  // not heal on retry by itself: red status, retries continue until fixed.
  if (/^(NOPERM|WRONGTYPE)\b/.test(String(error?.message || ''))) return 'auth';
  return classifyRedisError(error);
}

/**
 * Registers the node with its config node and connects the shared client in
 * onStart, retried by kit with backoff on transient failures.
 * @returns {{ ready: Promise<boolean>, client: () => any, config: () => any }}
 */
function startShared(RED, k, node, serverId) {
  let api;
  let client;
  let release;
  const ready = k.onStart(async () => {
    api = requireConfig(RED, serverId);
    if (!release) {
      release = api.register(node.id);
      k.onClose(() => release());
    }
    client = await api.shared();
  }, { classify });
  return { ready, client: () => client, config: () => api };
}

/**
 * Applies the native port layout: with errorOutput on, errors leave through
 * the last output (KIT-LOG-4) and the node has one extra output.
 */
function inputOptions(parsed, outputs) {
  const value = parsed.ok ? parsed.value : { concurrency: 1, maxQueue: 0, errorOutput: false };
  return {
    concurrency: value.concurrency,
    maxQueue: value.maxQueue,
    startQueueTimeoutMs: START_QUEUE_TIMEOUT_MS,
    errorOutput: value.errorOutput,
    outputCount: value.errorOutput ? outputs + 1 : outputs
  };
}

/** Sends to the first output when errorOutput adds an error port. */
function primary(send, errorOutput, msg) {
  if (errorOutput) send([msg, null]);
  else send(msg);
}

module.exports = { INPUT_SCHEMA, START_QUEUE_TIMEOUT_MS, requireConfig, classify, startShared, inputOptions, primary };
