'use strict';

const { codeError } = require('./format.js');

const ACTIONS = Object.freeze(['pause', 'resume', 'drain', 'status']);
const MAX_DRAIN_TIMEOUT_MS = 30 * 60 * 1000;

/**
 * Validates a control command from a node message, an admin route or the
 * internal drain route (RDS-CTL-1). Unknown fields are ignored.
 * @param {unknown} input
 */
function parseCommand(input) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const action = typeof source.action === 'string' ? source.action.trim().toLowerCase() : '';
  if (!ACTIONS.includes(action)) throw codeError('CONTROL_ACTION_INVALID', 'action must be pause, resume, drain or status');
  const command = { action };
  for (const field of ['stream', 'group', 'nodeId']) {
    if (source[field] === undefined || source[field] === null || source[field] === '') continue;
    if (typeof source[field] !== 'string' || source[field].length > 1024) throw codeError('CONTROL_FILTER_INVALID', `${field} must be a string`);
    command[field] = source[field];
  }
  if (source.drainTimeoutMs !== undefined && source.drainTimeoutMs !== null && source.drainTimeoutMs !== '') {
    const timeout = Number(source.drainTimeoutMs);
    if (!Number.isSafeInteger(timeout) || timeout < 0 || timeout > MAX_DRAIN_TIMEOUT_MS) {
      throw codeError('CONTROL_TIMEOUT_INVALID', 'drainTimeoutMs must be an integer between 0 and 1800000');
    }
    command.drainTimeoutMs = timeout;
  }
  return command;
}

/**
 * Runs a control command over the stream-in consumers of this runtime.
 * @param {Iterable<{ nodeId: string, consumer: import('./stream-consumer.js').StreamConsumer, drainTimeoutMs: number }>} entries
 * @param {{ action: string, stream?: string, group?: string, nodeId?: string, drainTimeoutMs?: number }} command
 */
async function executeControl(entries, command) {
  const targets = [...entries].filter((entry) => (!command.stream || entry.consumer.stream === command.stream)
    && (!command.group || entry.consumer.group === command.group)
    && (!command.nodeId || entry.nodeId === command.nodeId));
  const results = await Promise.all(targets.map(async (entry) => {
    const base = { nodeId: entry.nodeId };
    switch (command.action) {
      case 'pause': return { ...base, ...entry.consumer.pause() };
      case 'resume': return { ...base, ...entry.consumer.resume() };
      case 'drain': return { ...base, ...(await entry.consumer.drain(command.drainTimeoutMs ?? entry.drainTimeoutMs)) };
      default: return { ...base, ...entry.consumer.status() };
    }
  }));
  return {
    action: command.action,
    target: { stream: command.stream || null, group: command.group || null, nodeId: command.nodeId || null },
    consumers: results,
    drained: command.action === 'drain' ? results.every((result) => result.drained) : undefined
  };
}

module.exports = { parseCommand, executeControl, ACTIONS };
