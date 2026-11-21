'use strict';

const { codeError, setMeta } = require('../lib/format.js');
const { startShared } = require('../lib/node-helpers.js');

const TYPES = Object.freeze(['SCAN', 'HSCAN', 'SSCAN', 'ZSCAN']);

/**
 * Full cursor walk with a result limit and a deadline (RDS-CMD-5). In cluster
 * mode SCAN visits every master; key scans are routed by the key's slot.
 * @param {any} client
 * @param {{ type: string, key?: string, match?: string, count: number, maxResults: number, timeoutMs: number, cluster: boolean, now?: () => number }} options
 */
async function scanAll(client, options) {
  const now = options.now || (() => Date.now());
  const deadline = now() + options.timeoutMs;
  const method = options.type.toLowerCase();
  const pairs = options.type === 'HSCAN' || options.type === 'ZSCAN';
  const items = [];
  let complete = true;
  let timedOut = false;
  const targets = options.type === 'SCAN' && options.cluster && typeof client.nodes === 'function'
    ? client.nodes('master')
    : [client];

  for (const target of targets) {
    let cursor = '0';
    do {
      if (now() >= deadline) {
        timedOut = true;
        break;
      }
      const args = options.type === 'SCAN' ? [cursor] : [options.key, cursor];
      if (options.match) args.push('MATCH', options.match);
      args.push('COUNT', options.count);
      const [next, elements] = await target[method](...args);
      cursor = String(next);
      if (pairs) {
        for (let index = 0; index + 1 < elements.length && items.length < options.maxResults; index += 2) {
          items.push({ member: elements[index], value: elements[index + 1] });
        }
      } else {
        items.push(...elements.slice(0, options.maxResults - items.length));
      }
    } while (cursor !== '0' && items.length < options.maxResults);
    if (cursor !== '0') complete = false;
    if (timedOut || items.length >= options.maxResults) {
      if (targets.indexOf(target) < targets.length - 1) complete = false;
      break;
    }
  }
  return { items, complete: complete && !timedOut, timedOut };
}

module.exports = function register(RED, palette) {
  const { rk } = palette;

  function RedisScanNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      scanType: { type: 'enum', values: [...TYPES], default: 'SCAN' },
      match: { type: 'str', default: '' },
      count: { type: 'int', min: 1, max: 100000, default: 100 },
      maxResults: { type: 'int', min: 1, max: 1000000, default: 10000 },
      timeoutMs: { type: 'int', min: 100, max: 600000, default: 10000 },
      legacyMsgFields: { type: 'bool', default: false }
    });
    const options = parsed.ok ? parsed.value : {};
    const shared = startShared(RED, k, node, config.server);

    k.onInput(async (msg, send) => {
      const type = String(msg.scanType || options.scanType).toUpperCase();
      if (!TYPES.includes(type)) throw codeError('SCAN_TYPE_INVALID', `Unsupported scanType: ${type}`);
      const key = type === 'SCAN' ? undefined : msg.key;
      if (type !== 'SCAN' && (typeof key !== 'string' || key === '')) throw codeError('SCAN_KEY_REQUIRED', `${type} requires msg.key`);
      const match = typeof msg.match === 'string' && msg.match !== '' ? msg.match : options.match;
      const count = positive(msg.count, options.count, 100000);
      const maxResults = positive(msg.maxResults, options.maxResults, 1000000);
      const result = await scanAll(shared.client(), {
        type, key, match, count, maxResults, timeoutMs: options.timeoutMs, cluster: shared.config().mode === 'cluster'
      });
      msg.payload = result.items;
      setMeta(msg, { scan: { type, complete: result.complete, truncated: !result.complete, timedOut: result.timedOut, count: result.items.length } });
      if (options.legacyMsgFields) {
        msg.scanComplete = result.complete;
        msg.scanTruncated = !result.complete;
      }
      if (result.complete) k.status('ok', `found ${result.items.length}`);
      else k.status('warn', `${result.timedOut ? 'timeout' : 'limit'} ${result.items.length}`);
      send(msg);
    }, { concurrency: 4, maxQueue: 100, startQueueTimeoutMs: 30000 });
  }

  function positive(value, fallback, max) {
    const number = Number.parseInt(value, 10);
    return Number.isSafeInteger(number) && number >= 1 ? Math.min(number, max) : fallback;
  }

  RED.nodes.registerType('redkern-redis-scan', RedisScanNode);
};

module.exports.scanAll = scanAll;
