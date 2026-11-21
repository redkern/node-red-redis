'use strict';

// Source of node help (nodes/locales/en-US/<type>.html), Node-RED style guide:
// one sentence, Inputs, Outputs, Details, References.
// Properties: [name, type, description, optional?]

const META = ['redkern.redis', 'object', 'Palette metadata. Other palettes keep their own keys under msg.redkern.'];
const ERROR_PORT = { label: 'Error', props: [['error', 'object', 'With "Send errors to a separate output": { message, code, source }.']] };

module.exports = {
  'redkern-redis-config': {
    summary: 'Connection to a standalone Redis or a Redis Cluster, shared by the nodes that use it.',
    details: [
      'Each config node opens at most one shared connection (auto-pipelined, no offline queue), one subscriber connection when Pub/Sub nodes use it, and one blocking connection per stream in and list in node. The formula and the live numbers are in the Reliability tab and in <code>redkern_redis_connections{role}</code>.',
      'Host, port, username and timeouts can be <code>${VAR}</code>: the value comes from the environment while the flow in Git shows its source. The password is a credential and never leaves the runtime.',
      'In cluster mode the database is always 0, and a stream with a DLQ must share a hash tag with it, for example <code>orders:{eu}</code> and <code>orders:{eu}:dlq</code>.',
      'TLS always verifies the server certificate. A private CA is added with <code>NODE_EXTRA_CA_CERTS</code>; set TLS server name when the certificate is issued for another name than the host. A rejected certificate gives a red status.'
    ],
    ref: 'config'
  },
  'redkern-redis-command': {
    summary: 'Runs one Redis command on the shared connection.',
    inputs: [
      ['args', 'array', 'Command arguments, for example <code>["user:1", "name"]</code>.', true],
      ['command', 'string | array', 'Replaces the configured command when "Allow msg.command" is on.', true]
    ],
    outputs: [
      { label: 'Result', props: [['payload', 'any', 'Redis reply.'], META] },
      ERROR_PORT
    ],
    details: [
      'Commands from the denylist (<code>FLUSHALL</code>, <code>FLUSHDB</code>, <code>CONFIG</code>, <code>SHUTDOWN</code>, <code>SWAPDB</code>, <code>FAILOVER</code>, <code>CLIENT KILL</code>, admin <code>CLUSTER</code> subcommands and similar) are rejected unless the denylist is disabled. Scripts (<code>EVAL</code>, <code>FCALL</code>) are rejected from <code>msg.command</code>; set <code>EVAL</code> as the node command and pass the script in <code>msg.args</code>. Blocking commands such as <code>BLPOP</code> and commands that change the connection such as <code>CLIENT REPLY</code> are always rejected, because they would stall or break the shared connection.',
      'Up to Concurrency messages run at the same time and up to Max queue wait; a full queue fails the message with <code>INPUT_QUEUE_FULL</code>. With Concurrency above 1 the order of replies is not guaranteed.'
    ],
    ref: 'command'
  },
  'redkern-redis-multi': {
    summary: 'Runs several commands in one MULTI/EXEC transaction.',
    inputs: [['commands', 'array', 'Array of commands, each an array: <code>[["SET","k","v"],["INCR","c"]]</code>. Overrides the configured list.', true]],
    outputs: [
      { label: 'Results', props: [['payload', 'array', 'One reply per command.'], META] },
      ERROR_PORT
    ],
    details: ['The denylist applies to every command; scripts are rejected in <code>msg.commands</code>. If any command fails, the message fails with all errors listed.'],
    ref: 'multi'
  },
  'redkern-redis-scan': {
    summary: 'Walks SCAN, HSCAN, SSCAN or ZSCAN to the end, up to a result limit and a timeout.',
    inputs: [
      ['key', 'string', 'Key for HSCAN, SSCAN and ZSCAN.', true],
      ['match', 'string', 'Overrides the configured pattern.', true],
      ['scanType', 'string', 'Overrides the configured type.', true]
    ],
    outputs: [{ label: 'Items', props: [['payload', 'array', 'Keys or members; HSCAN and ZSCAN return <code>{ member, value }</code>.'], ['redkern.redis.scan', 'object', '<code>{ type, complete, truncated, timedOut, count }</code>.']] }],
    details: ['In cluster mode SCAN visits every master. A result cut by the limit or the timeout has <code>complete: false</code> and a yellow status.'],
    ref: 'scan'
  },
  'redkern-redis-subscribe': {
    summary: 'Receives Redis Pub/Sub messages from channels or patterns.',
    outputs: [{ label: 'Message', props: [['topic', 'string', 'Channel the message came from.'], ['payload', 'string', 'Message text.'], ['pattern', 'string', 'Matching pattern in pattern mode.'], META] }],
    details: ['All subscribe nodes of one config node share one subscriber connection. After a reconnect the subscriptions are restored automatically. Messages published while the connection is down are lost, as Pub/Sub has no history; use Streams when every message matters.'],
    ref: 'subscribe'
  },
  'redkern-redis-list-in': {
    summary: 'Pops elements from a Redis list with BLPOP or BRPOP on its own connection.',
    outputs: [{ label: 'Element', props: [['topic', 'string', 'List key.'], ['payload', 'string', 'Popped element.'], META] }],
    details: [
      'A transitional node for list queues; it will be deprecated once queues move to Streams. Popping removes the element from the list, so an element that is being processed when the pod dies is lost. There is no reliable BLMOVE mode: delivery guarantees come from stream in.',
      'Prefetch limits elements in the flow at once. With 1 the next element is popped only after the previous message completed in the flow; with 0 the node pops without limit, as the legacy subscribe node did. A message that never completes frees its slot after the release timeout.'
    ],
    ref: 'list-in'
  },
  'redkern-redis-stream-in': {
    summary: 'Reads a Redis stream through a consumer group with at-least-once delivery, DLQ and backpressure.',
    outputs: [{
      label: 'Entries',
      props: [
        ['payload', 'object | array', 'Entry fields; JSON objects and arrays written by stream out are restored. An array with batching.'],
        ['redkern.redis', 'object', '<code>{ stream, group, id, consumer, deliveries }</code>; <code>id</code> and <code>deliveries</code> are arrays with batching. stream ack reads it.']
      ]
    }],
    details: [
      'With manual ACK every entry stays in the PEL until stream ack confirms it. After a pod crash, or when processing takes longer than Reclaim idle, the entry is delivered again, so downstream processing must be idempotent; <code>redkern_redis_redeliveries_total</code> counts repeats. Only pause/resume and a regular redeploy are free of duplicates.',
      'On start the node recovers its own PEL through a cursor before reading new entries, then reclaims entries abandoned by dead consumers with XAUTOCLAIM. An entry delivered more than Max deliveries times moves to the dead-letter stream atomically with its ACK.',
      'Backpressure stops reading when the own PEL (or the group PEL) reaches Max pending. Pause, resume, drain and status come from stream control, the admin API and the preStop drain route on port 9551.'
    ],
    ref: 'stream-in'
  },
  'redkern-redis-stream-out': {
    summary: 'Adds msg.payload to a Redis stream with XADD.',
    inputs: [
      ['payload', 'object', 'Flat object of fields; nested objects and arrays are stored as JSON.'],
      ['stream', 'string', 'Stream key when "Allow msg.stream" is on.', true]
    ],
    outputs: [
      { label: 'Added', props: [['redkern.redis.xadd', 'object', '<code>{ stream, id }</code> of the new entry. Other keys of msg.redkern.redis stay, so the original entry can still be acknowledged.']] },
      ERROR_PORT
    ],
    details: ['MAXLEN trimming can remove entries still pending in a consumer group and needs the explicit "Allow trimming" option.'],
    ref: 'stream-out'
  },
  'redkern-redis-stream-ack': {
    summary: 'Acknowledges stream entries delivered by stream in.',
    inputs: [
      ['redkern.redis', 'object', '<code>id</code> (string or array for a batch), <code>stream</code> and <code>group</code> as set by stream in.'],
      ['streamId', 'string | array', 'Legacy ID field, used when msg.redkern.redis is missing.', true],
      ['deleteAfterAck', 'boolean', 'Overrides "XDEL after ACK".', true]
    ],
    outputs: [{ label: 'Acknowledged', props: [['redkern.redis.ack', 'object', '<code>{ acked, deleted }</code>. Only with "Pass the message on".']] }],
    details: ['A batch is acknowledged with one XACK. The node also releases the entries in the in-flight registry of stream in, which lets it read more under backpressure.'],
    ref: 'stream-ack'
  },
  'redkern-redis-stream-claim': {
    summary: 'Claims stream entries idle longer than a threshold with XAUTOCLAIM.',
    inputs: [['stream', 'string', 'Overrides the stream key.', true], ['group', 'string', 'Overrides the group.', true], ['consumer', 'string', 'Consumer that receives the entries.', true], ['minIdleMs', 'number', 'Overrides Min idle.', true]],
    outputs: [{ label: 'Claimed', props: [['payload', 'array', '<code>{ id, payload }</code> per claimed entry.'], ['redkern.redis.claim', 'object', '<code>{ stream, group, consumer, complete, count }</code>.']] }],
    details: ['stream in reclaims entries by itself; this node is for manual recovery flows.'],
    ref: 'stream-claim'
  },
  'redkern-redis-stream-gc': {
    summary: 'Removes idle consumers with an empty PEL from a consumer group.',
    inputs: [['stream', 'string', 'Overrides the stream key.', true], ['group', 'string', 'Overrides the group.', true], ['minIdleMs', 'number', 'Overrides Min idle.', true]],
    outputs: [{ label: 'Report', props: [['payload', 'object', '<code>{ removed, kept }</code> with consumer names and idle times.']] }],
    details: ['Every pod restart creates a new consumer name. The check for an empty PEL and the delete run in one Lua script, so a consumer that read entries in between is never deleted.'],
    ref: 'stream-gc'
  },
  'redkern-redis-stream-metrics': {
    summary: 'Samples a consumer group: PEL, lag, consumers and DLQ length.',
    inputs: [['stream', 'string', 'Overrides the stream key.', true], ['group', 'string', 'Overrides the group.', true], ['deadLetterStream', 'string', 'Overrides the DLQ stream.', true]],
    outputs: [{ label: 'Sample', props: [['payload', 'object', '<code>{ stream, group, pending, lag, consumers, deadLetterLength, lastDeliveredId, sampledAt }</code>.']] }],
    details: ['Each sample also updates the palette metrics registry. With the prometheus palette the metrics appear in its /metrics; without it they are served on the internal port 9551 when <code>REDKERN_REDIS_METRICS_TOKEN</code> is set.'],
    ref: 'metrics'
  },
  'redkern-redis-stream-control': {
    summary: 'Pauses, resumes, drains or reports stream in consumers of this runtime.',
    inputs: [
      ['payload.action', 'string', '<code>pause</code>, <code>resume</code>, <code>drain</code> or <code>status</code>, when the node action is "from msg".'],
      ['payload.stream', 'string', 'Only consumers of this stream.', true],
      ['payload.group', 'string', 'Only consumers of this group.', true],
      ['payload.nodeId', 'string', 'Only this stream in node.', true],
      ['payload.drainTimeoutMs', 'number', 'Drain timeout.', true]
    ],
    outputs: [{ label: 'Result', props: [['payload', 'object', '<code>{ action, target, consumers, drained }</code>.']] }],
    details: ['Drain stops reading new entries, sends entries already reserved and waits for an empty own PEL. The same commands are available over HTTP: GET/POST /redkern/redis/streams/status and /control on the admin API, and the preStop drain route on port 9551.'],
    ref: 'control'
  }
};
