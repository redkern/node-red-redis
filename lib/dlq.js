'use strict';

const { keySlot } = require('./slot.js');
const { codeError } = require('./format.js');

/**
 * Moves entries to the dead-letter stream atomically (RDS-STR-7): XADD to the
 * DLQ and XACK on the source run in one MULTI/EXEC. DLQ record fields are the
 * same as in the legacy palette.
 * @param {any} client shared ioredis client
 * @param {{ stream: string, group: string, dlq: string, consumer: string }} target
 * @param {Array<{ id: string, payload: unknown, deliveries: number }>} entries
 */
async function moveToDlq(client, { stream, group, dlq, consumer }, entries) {
  if (entries.length === 0) return 0;
  const transaction = client.multi();
  for (const entry of entries) {
    transaction.xadd(dlq, '*',
      'sourceStream', stream,
      'sourceGroup', group,
      'sourceId', entry.id,
      'consumer', consumer,
      'deliveries', String(entry.deliveries),
      'payload', JSON.stringify(entry.payload === undefined ? null : entry.payload));
  }
  transaction.xack(stream, group, ...entries.map((entry) => entry.id));
  const results = await transaction.exec();
  if (!Array.isArray(results)) throw codeError('DLQ_ABORTED', 'DLQ transaction was aborted');
  const failed = results.find(([error]) => error);
  if (failed) throw codeError('DLQ_FAILED', `DLQ transaction failed: ${failed[0].message}`);
  return entries.length;
}

/**
 * In Redis Cluster MULTI works only inside one hash slot. Returns an issue for
 * kit.ConfigError when the source stream and DLQ land in different slots.
 * @param {string} stream
 * @param {string} dlq
 */
function slotIssue(stream, dlq) {
  if (keySlot(stream) === keySlot(dlq)) return null;
  return {
    field: 'deadLetterStream',
    code: 'DLQ_CROSS_SLOT',
    message: `Stream and DLQ are in different cluster slots; use a hash tag such as orders:{eu} and orders:{eu}:dlq`
  };
}

module.exports = { moveToDlq, slotIssue };
