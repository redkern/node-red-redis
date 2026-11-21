'use strict';

// Chaos test for Redis Cluster: stream in/out/ack and command run in an
// embedded Node-RED runtime while the cluster goes through
//   1. a graceful failover (CLUSTER FAILOVER on a replica),
//   2. a hard kill of a master (SIGKILL, the replica takes over),
//   3. a live slot migration of one stream (MIGRATING/IMPORTING, ASK, MOVED).
// Checks: every entry present in a stream at the end was delivered and
// acknowledged, the PELs are empty, nothing went to a DLQ, the nodes are
// green again and command recovers. Entries whose XADD was acknowledged but
// that are missing from the stream after the hard kill are reported apart:
// Redis replication is asynchronous, that loss is not the palette's.
//
// Needs a 6-node cluster with replicas on CHAOS_PORTS (default 7101-7106):
//   scripts/start-chaos-cluster.sh (3 masters with one replica each).
// Run: npm run test:chaos [-- --seconds 45]

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const Redis = require('ioredis');

const NODE_RED = process.env.NODE_RED_PATH || require.resolve('node-red');
const RED = require(NODE_RED);
const PORTS = (process.env.CHAOS_PORTS || '7101,7102,7103,7104,7105,7106').split(',').map(Number);
const SECONDS = Number(process.argv[process.argv.indexOf('--seconds') + 1]) || 45;
const STREAMS = ['chaos:{a}', 'chaos:{b}', 'chaos:{c}'];
const RATE = 100; // entries per second per stream

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`timed out: ${label}`);
    await sleep(100);
  }
}

function flows() {
  const nodes = [
    { id: 'tab', type: 'tab', label: 'chaos' },
    {
      id: 'cfg', type: 'redkern-redis-config', mode: 'cluster', host: '127.0.0.1', port: PORTS[0],
      clusterNodes: PORTS.map((port) => `127.0.0.1:${port}`).join(','), commandTimeout: 3000, connectTimeout: 2000
    },
    { id: 'cmd', z: 'tab', type: 'redkern-redis-command', server: 'cfg', command: 'INCR', errorOutput: true, outputs: 2, concurrency: 8, wires: [['cok'], ['cerr']] },
    { id: 'cok', z: 'tab', type: 'function', func: 'context.set("n", (context.get("n") || 0) + 1); return null;', outputs: 1, wires: [] },
    { id: 'cerr', z: 'tab', type: 'function', func: 'context.set("n", (context.get("n") || 0) + 1); return null;', outputs: 1, wires: [] }
  ];
  STREAMS.forEach((stream, index) => {
    nodes.push(
      {
        id: `sin${index}`, z: 'tab', type: 'redkern-redis-stream-in', server: 'cfg', streamKey: stream, group: 'g', startId: '0',
        blockMs: 500, count: 50, maxPending: 500, reclaimIdleMs: 5000, reclaimIntervalMs: 2000, wires: [[`work${index}`]]
      },
      {
        id: `work${index}`, z: 'tab', type: 'function', outputs: 1, wires: [[`ack${index}`]],
        func: 'const seen = context.get("seen") || []; seen.push(msg.payload.n); context.set("seen", seen);' +
          'setTimeout(() => { node.send(msg); node.done(); }, Math.floor(Math.random() * 20)); return null;'
      },
      { id: `ack${index}`, z: 'tab', type: 'redkern-redis-stream-ack', server: 'cfg', wires: [] }
    );
  });
  return nodes;
}

async function masterOf(cluster, key) {
  const slot = Number(await cluster.cluster('KEYSLOT', key));
  const slots = await cluster.cluster('SLOTS');
  for (const [from, to, master, ...replicas] of slots) {
    if (slot >= from && slot <= to) return { slot, master: { host: master[0], port: master[1], id: master[2] }, replicas: replicas.map((r) => ({ host: r[0], port: r[1], id: r[2] })) };
  }
  throw new Error(`no owner for ${key}`);
}

async function migrateSlot(slot, source, target) {
  const src = new Redis({ port: source.port });
  const dst = new Redis({ port: target.port });
  try {
    await dst.cluster('SETSLOT', slot, 'IMPORTING', source.id);
    await src.cluster('SETSLOT', slot, 'MIGRATING', target.id);
    for (;;) {
      const keys = await src.cluster('GETKEYSINSLOT', slot, 100);
      if (keys.length === 0) break;
      await src.migrate(target.host, target.port, '', 0, 5000, 'KEYS', ...keys);
    }
    for (const port of PORTS) {
      const node = new Redis({ port, lazyConnect: true, maxRetriesPerRequest: 0, retryStrategy: () => null });
      node.on('error', () => {});
      try {
        await node.connect();
        const role = (await node.role())[0];
        if (role === 'master') await node.cluster('SETSLOT', slot, 'NODE', target.id);
      } catch { /* a killed node */ } finally { node.disconnect(); }
    }
  } finally {
    src.disconnect();
    dst.disconnect();
  }
}

const fills = new Map();
function watchStatus(id) {
  const node = RED.nodes.getNode(id);
  const original = node.status.bind(node);
  node.status = (status) => {
    fills.set(id, status?.fill || null);
    return original(status);
  };
}
function statuses() {
  return STREAMS.map((_, index) => fills.get(`sin${index}`) || null);
}

(async () => {
  const userDir = fs.mkdtempSync(path.join(os.tmpdir(), 'redkern-chaos-'));
  fs.mkdirSync(path.join(userDir, 'node_modules', '@redkern'), { recursive: true });
  fs.symlinkSync(path.join(__dirname, '..'), path.join(userDir, 'node_modules', '@redkern', 'node-red-redis'));
  fs.writeFileSync(path.join(userDir, 'package.json'), JSON.stringify({ name: 'chaos', dependencies: { '@redkern/node-red-redis': '*' } }));
  fs.writeFileSync(path.join(userDir, 'flows.json'), JSON.stringify(flows()));
  const cluster = new Redis.Cluster(PORTS.map((port) => ({ host: '127.0.0.1', port })), { scaleReads: 'master' });
  for (const node of cluster.nodes('master')) await node.flushall();

  const server = http.createServer();
  RED.init(server, {
    userDir, flowFile: 'flows.json', httpAdminRoot: false, httpNodeRoot: false, credentialSecret: false,
    logging: { console: { level: 'warn', metrics: false } }, editorTheme: { palette: { catalogues: [] } }, diagnostics: { enabled: false }
  });
  const produced = STREAMS.map(() => new Set());
  const producerErrors = { count: 0 };
  let commandsSent = 0;
  const report = { events: [] };
  try {
    await RED.start();
    await until(() => STREAMS.every((_, index) => RED.nodes.getNode(`sin${index}`)), 30000, 'flows started');
    await until(async () => {
      for (const stream of STREAMS) {
        try { if ((await cluster.xinfo('GROUPS', stream)).length !== 1) return false; } catch { return false; }
      }
      return true;
    }, 30000, 'groups created');
    STREAMS.forEach((_, index) => watchStatus(`sin${index}`));

    let stop = false;
    let counter = 0;
    const producer = (async () => {
      while (!stop) {
        const started = Date.now();
        await Promise.all(STREAMS.map(async (stream, index) => {
          const batch = [];
          for (let i = 0; i < RATE / 10; i += 1) batch.push(String(counter++));
          for (const n of batch) {
            try {
              await cluster.xadd(stream, '*', 'n', n);
              produced[index].add(n);
            } catch {
              producerErrors.count += 1;
            }
          }
        }));
        const command = RED.nodes.getNode('cmd');
        for (let i = 0; i < 20; i += 1) { command.receive({ args: [`chaos:counter:${i % 5}`] }); commandsSent += 1; }
        const wait = 100 - (Date.now() - started);
        if (wait > 0) await sleep(wait);
      }
    })();

    const third = Math.floor(SECONDS / 4) * 1000;
    await sleep(third);
    const a = await masterOf(cluster, STREAMS[0]);
    report.events.push(`graceful failover of ${a.master.port} (owner of ${STREAMS[0]}) via replica ${a.replicas[0].port}`);
    await new Redis({ port: a.replicas[0].port }).cluster('FAILOVER');

    await sleep(third);
    const b = await masterOf(cluster, STREAMS[1]);
    const victim = new Redis({ port: b.master.port });
    const pid = Number((await victim.info('server')).match(/process_id:(\d+)/)[1]);
    victim.disconnect();
    report.events.push(`SIGKILL of master ${b.master.port} (owner of ${STREAMS[1]}), pid ${pid}`);
    process.kill(pid, 'SIGKILL');

    await sleep(third);
    const c = await masterOf(cluster, STREAMS[2]);
    const masters = (await cluster.cluster('SLOTS')).map((row) => ({ host: row[2][0], port: row[2][1], id: row[2][2] }));
    const target = masters.find((node) => node.id !== c.master.id && node.port !== b.master.port);
    report.events.push(`live migration of slot ${c.slot} (${STREAMS[2]}) from ${c.master.port} to ${target.port}`);
    await migrateSlot(c.slot, c.master, target);

    await sleep(Math.max(3000, SECONDS * 1000 - 3 * third));
    stop = true;
    await producer;

    // Drain: every entry still in the stream must be delivered and acknowledged.
    await until(async () => {
      for (const stream of STREAMS) {
        if (Number((await cluster.xpending(stream, 'g'))[0]) !== 0) return false;
        // XINFO lag is an estimate that drifts after a failover; compare IDs.
        const info = await cluster.xinfo('GROUPS', stream);
        const delivered = info[0][info[0].indexOf('last-delivered-id') + 1];
        const [last] = await cluster.xrevrange(stream, '+', '-', 'COUNT', 1);
        if (last && last[0] !== delivered) return false;
      }
      return true;
    }, 120000, 'all entries acknowledged').catch(async (error) => {
      for (const stream of STREAMS) {
        console.error(stream, 'pending', JSON.stringify(await cluster.xpending(stream, 'g')),
          'groups', JSON.stringify(await cluster.xinfo('GROUPS', stream)),
          'first', JSON.stringify(await cluster.xpending(stream, 'g', '-', '+', 3)));
      }
      console.error('statuses', JSON.stringify(statuses()));
      throw error;
    });

    const seen = STREAMS.map((_, index) => RED.nodes.getNode(`work${index}`).context().get('seen') || []);
    const results = [];
    for (const [index, stream] of STREAMS.entries()) {
      const present = new Set((await cluster.xrange(stream, '-', '+')).map(([, fields]) => fields[1]));
      const delivered = new Set(seen[index]);
      const missing = [...present].filter((n) => !delivered.has(n));
      const lostByRedis = [...produced[index]].filter((n) => !present.has(n));
      results.push({
        stream, produced: produced[index].size, inStream: present.size, delivered: delivered.size,
        duplicates: seen[index].length - delivered.size, missing: missing.length, lostByRedisReplication: lostByRedis.length,
        dlq: Number(await cluster.exists(`${stream}:dlq`))
      });
    }
    const commandOk = RED.nodes.getNode('cok').context().get('n') || 0;
    const commandErr = RED.nodes.getNode('cerr').context().get('n') || 0;
    // command recovers: a fresh burst after the chaos succeeds completely.
    const okBefore = commandOk;
    for (let i = 0; i < 50; i += 1) RED.nodes.getNode('cmd').receive({ args: [`chaos:after:${i}`] });
    await until(() => (RED.nodes.getNode('cok').context().get('n') || 0) - okBefore === 50, 15000, 'command recovered');

    console.log(JSON.stringify({ events: report.events, producerErrors: producerErrors.count, command: { sent: commandsSent, ok: commandOk, failed: commandErr }, statuses: statuses(), results }, null, 2));
    for (const result of results) {
      assert.equal(result.missing, 0, `${result.stream}: entries in the stream that were never delivered`);
      assert.equal(result.dlq, 0, `${result.stream}: entries moved to the DLQ`);
    }
    assert.equal(commandOk + commandErr, commandsSent, 'every command message completed');
    await until(() => statuses().every((fill) => fill === 'green'), 15000, 'stream in nodes green again');
    console.log('chaos test passed');
  } finally {
    await RED.stop();
    cluster.disconnect();
    fs.rmSync(userDir, { recursive: true, force: true });
  }
})().then(() => process.exit(0), (error) => {
  console.error(error);
  process.exit(1);
});
