'use strict';

// Malformed and hostile messages to every node type: nothing may throw out of
// the node, leave the message unfinished or crash the process (the suite runs
// with --unhandled-rejections=strict). Every message must either leave on an
// output or fail through done(err).

const { describe, it, before, after, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { helper, config, redis, load, waitFor } = require('./helper.js');

const circular = { a: 1 };
circular.self = circular;

const INPUTS = [
  {},
  { payload: null },
  { payload: undefined },
  { payload: '' },
  { payload: 'text' },
  { payload: 42 },
  { payload: [1, 'two', null] },
  { payload: Buffer.from('bytes') },
  { payload: { nested: { deep: [1, { x: 2 }] }, n: null, u: undefined, f: () => 1 } },
  { payload: circular },
  { payload: 'x'.repeat(2 * 1024 * 1024) },
  { args: 'not-an-array' },
  { args: [{}, null, undefined, [1, 2], Symbol.for('s').toString()] },
  { args: new Array(10000).fill('a') },
  { command: 123 },
  { command: [] },
  { command: ['  '] },
  { command: 'FLUSHALL' },
  { command: 'EVAL', args: ["return redis.call('FLUSHALL')", 0] },
  { commands: 'garbage' },
  { commands: [[]] },
  { commands: [[null]] },
  { commands: new Array(5000).fill(['INCR', 'k']) },
  { redkern: 'string' },
  { redkern: { redis: 5 } },
  { redkern: { redis: { id: {}, stream: [], group: null, ids: [{}, 'not-an-id', '1-0'] } } },
  { redkern: { redis: { stream: 'other-stream', ids: ['0-1'] } } },
  { stream: 123, _streamKey: ['x'], streamKey: { a: 1 }, group: {} },
  { topic: { a: 1 }, key: [] },
  { payload: { action: 'bogus' } },
  { payload: { action: 'pause', stream: {}, group: [], nodeId: 5 } },
  { payload: { action: 'drain', drainTimeoutMs: -1 } },
  { payload: { action: 'drain', drainTimeoutMs: 'soon' } },
  { __proto__: { polluted: true }, payload: JSON.parse('{"__proto__": {"polluted": true}}') }
];

const NODES = [
  { type: 'redkern-redis-command', command: 'GET', allowMsgCommand: true, errorOutput: true, outputs: 2 },
  { type: 'redkern-redis-multi', commands: '[["INCR","fz:m"]]', errorOutput: true, outputs: 2 },
  { type: 'redkern-redis-scan', match: 'fz:*', count: 10, maxResults: 100 },
  { type: 'redkern-redis-stream-out', streamKey: 'fz:s', allowMsgStream: true, errorOutput: true, outputs: 2 },
  { type: 'redkern-redis-stream-ack', streamKey: 'fz:s', group: 'g' },
  { type: 'redkern-redis-stream-claim', streamKey: 'fz:s', group: 'g', consumer: 'c' },
  { type: 'redkern-redis-stream-gc', streamKey: 'fz:s', group: 'g' },
  { type: 'redkern-redis-stream-metrics', streamKey: 'fz:s', group: 'g' },
  { type: 'redkern-redis-stream-control' }
];

describe('fuzz', () => {
  let client;
  before(async () => {
    client = redis();
    await new Promise((resolve) => helper.startServer(resolve));
  });
  after(async () => {
    await new Promise((resolve) => helper.stopServer(resolve));
    client.disconnect();
  });
  afterEach(async () => { await helper.unload(); });

  for (const definition of NODES) {
    it(`${definition.type} completes every malformed message and never throws`, async () => {
      await client.flushall();
      await client.xgroup('CREATE', 'fz:s', 'g', '$', 'MKSTREAM');
      await client.set('fz:keep', '1');
      const { type, outputs = 1, ...rest } = definition;
      const wires = Array.from({ length: outputs }, (_, index) => [`out${index}`]);
      const flow = [config(), { id: 'n', type, server: type.endsWith('control') ? undefined : 'cfg00001', outputs, wires, ...rest }];
      for (let index = 0; index < outputs; index += 1) flow.push({ id: `out${index}`, type: 'helper' });
      await load(flow);
      const node = helper.getNode('n');
      let finished = 0;
      for (let index = 0; index < outputs; index += 1) helper.getNode(`out${index}`).on('input', () => { finished += 1; });
      node.on('call:error', () => { finished += 1; });
      await new Promise((resolve) => setTimeout(resolve, 300));
      for (const input of INPUTS) node.receive({ ...input });
      // stream-gc and control may answer one message with one output; every
      // message ends either on an output or as an error.
      await waitFor(() => finished >= INPUTS.length, 20000, 50);
      assert.equal(await client.get('fz:keep'), '1', 'no message managed to flush the database');
      assert.equal(({}).polluted, undefined, 'no prototype pollution');
    });
  }
});
