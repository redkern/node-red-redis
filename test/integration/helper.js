'use strict';

const path = require('node:path');
const helper = require('node-red-node-test-helper');
const Redis = require('ioredis');

// NODE_RED_PATH lets CI run the same tests against Node-RED 4.1 and 5.x.
const NODE_RED = process.env.NODE_RED_PATH || require.resolve('node-red');
helper.init(NODE_RED);
// The runtime's own @node-red/util (hooks, events), not the devDependency's.
const util = require(require.resolve('@node-red/util', { paths: [path.dirname(NODE_RED)] }));

const ROOT = path.join(__dirname, '..', '..');
const palette = require(path.join(ROOT, 'nodes', 'redkern-redis.js'));
// The test helper's mock runtime has no RED.hooks; the real runtime does.
const NODE_FILES = [(RED) => {
  if (!RED.hooks) RED.hooks = util.hooks;
  return palette(RED);
}];
NODE_FILES.push(require(require.resolve('@node-red/nodes/core/function/10-function.js', { paths: [path.dirname(NODE_RED)] })));

const REDIS_PORT = Number(process.env.REDIS_PORT || 6390);

function config(id = 'cfg00001') {
  return { id, type: 'redkern-redis-config', host: '127.0.0.1', port: REDIS_PORT, mode: 'standalone', commandTimeout: 5000 };
}

function redis() {
  return new Redis({ port: REDIS_PORT, lazyConnect: false, maxRetriesPerRequest: 1 });
}

/**
 * REDIS_USERNAME and REDIS_PASSWORD run every config node of the suite as a
 * restricted ACL user (see README "Redis ACL"); test clients stay admin.
 */
function withUser(flow, credentials) {
  const username = process.env.REDIS_USERNAME;
  if (!username) return credentials;
  const merged = { ...credentials };
  for (const node of flow) {
    const clusterPorts = (process.env.REDIS_CLUSTER_PORTS || '').split(',').map(Number);
    const ours = node.mode === 'cluster' ? clusterPorts.includes(Number(node.port)) : Number(node.port) === REDIS_PORT;
    if (node.type !== 'redkern-redis-config' || !ours) continue;
    if (node.username && node.username !== username) continue;
    node.username = username;
    merged[node.id] = { password: process.env.REDIS_PASSWORD, ...(merged[node.id] || {}) };
  }
  return merged;
}

function load(flow, credentials = {}) {
  credentials = withUser(flow, credentials);
  return new Promise((resolve) => helper.load(NODE_FILES, flow, credentials, resolve));
}

/**
 * Like load, but keeps one palette closure across loads, as the Node-RED
 * runtime does across deploys (the node set module runs once per process).
 */
let persistent;
function loadKeepingPalette(flow, credentials = {}) {
  credentials = withUser(flow, credentials);
  const { createPalette } = require(path.join(ROOT, 'lib', 'palette.js'));
  const register = (RED) => {
    if (!RED.hooks) RED.hooks = util.hooks;
    if (!persistent) persistent = createPalette(RED);
    for (const node of palette.NODES) node(RED, persistent);
  };
  return new Promise((resolve) => helper.load([register, NODE_FILES[1]], flow, credentials, resolve));
}

function nextMessage(node, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timeout waiting for message')), timeoutMs);
    node.once('input', (msg) => {
      clearTimeout(timer);
      resolve(msg);
    });
  });
}

function collect(node) {
  const messages = [];
  node.on('input', (msg) => messages.push(msg));
  return messages;
}

async function waitFor(predicate, timeoutMs = 5000, stepMs = 20) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return true;
    } catch {
      // not ready yet
    }
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('waitFor timed out');
}

module.exports = { util, helper, config, redis, load, loadKeepingPalette, nextMessage, collect, waitFor, REDIS_PORT };
