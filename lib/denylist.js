'use strict';

const { codeError } = require('./format.js');

/**
 * Commands rejected by default (RDS-CMD-3). Two-word entries match the first
 * two tokens. The spec list plus commands with the same blast radius: data
 * loss (SWAPDB, FUNCTION FLUSH), topology changes (FAILOVER, CLUSTER admin
 * subcommands), server stalls (SAVE, CLIENT PAUSE) and killing other clients.
 */
const DEFAULT_DENYLIST = Object.freeze([
  'FLUSHALL', 'FLUSHDB', 'CONFIG', 'SHUTDOWN', 'DEBUG', 'MODULE', 'ACL',
  'REPLICAOF', 'SLAVEOF', 'MIGRATE', 'SCRIPT FLUSH',
  'SWAPDB', 'FAILOVER', 'SAVE', 'BGSAVE', 'BGREWRITEAOF', 'LASTSAVE',
  'FUNCTION FLUSH', 'FUNCTION DELETE', 'FUNCTION RESTORE', 'SCRIPT KILL', 'FUNCTION KILL',
  'CLIENT KILL', 'CLIENT PAUSE', 'CLIENT UNPAUSE', 'CLIENT UNBLOCK',
  'CLUSTER RESET', 'CLUSTER FAILOVER', 'CLUSTER FORGET', 'CLUSTER MEET', 'CLUSTER REPLICATE',
  'CLUSTER ADDSLOTS', 'CLUSTER ADDSLOTSRANGE', 'CLUSTER DELSLOTS', 'CLUSTER DELSLOTSRANGE',
  'CLUSTER FLUSHSLOTS', 'CLUSTER SETSLOT', 'CLUSTER SAVECONFIG', 'CLUSTER BUMPEPOCH', 'CLUSTER SET-CONFIG-EPOCH'
]);
const DEFAULT_DENIED_BY_COMMAND = new Map();
for (const entry of DEFAULT_DENYLIST) {
  const [first, second] = entry.split(/\s+/);
  const commands = DEFAULT_DENIED_BY_COMMAND.get(first) || [];
  commands.push({ entry, second });
  DEFAULT_DENIED_BY_COMMAND.set(first, commands);
}

/**
 * Commands that run code chosen by the message. With the command or the
 * transaction taken from msg they would bypass the denylist entirely
 * (EVAL "return redis.call('FLUSHALL')" 0), so they are rejected for
 * msg.command and msg.commands unless the denylist is disabled. A script set
 * in the node configuration stays allowed.
 */
const SCRIPT_COMMANDS = Object.freeze(new Set([
  'EVAL', 'EVALSHA', 'EVAL_RO', 'EVALSHA_RO', 'FCALL', 'FCALL_RO', 'FUNCTION', 'SCRIPT'
]));

/**
 * Commands that block or switch the connection mode. They would stall the
 * shared auto-pipelined connection, so they are always rejected on command and
 * multi; list-in, subscribe and stream-in own dedicated connections for them.
 */
const CONNECTION_COMMANDS = Object.freeze(new Set([
  'BLPOP', 'BRPOP', 'BLMOVE', 'BRPOPLPUSH', 'BLMPOP', 'BZPOPMIN', 'BZPOPMAX', 'BZMPOP',
  'WAIT', 'WAITAOF', 'SUBSCRIBE', 'PSUBSCRIBE', 'SSUBSCRIBE', 'UNSUBSCRIBE', 'PUNSUBSCRIBE',
  'SUNSUBSCRIBE', 'MONITOR', 'SYNC', 'PSYNC', 'QUIT', 'RESET', 'SELECT', 'MULTI', 'EXEC', 'DISCARD', 'WATCH', 'UNWATCH',
  // Change the state of the shared connection for every node using it.
  'HELLO', 'AUTH', 'READONLY', 'READWRITE', 'ASKING'
]));

/** CLIENT subcommands that change the shared connection (reply mode, tracking, name). */
const CONNECTION_CLIENT_SUBCOMMANDS = Object.freeze(new Set([
  'REPLY', 'TRACKING', 'CACHING', 'SETNAME', 'SETINFO', 'NO-EVICT', 'NO-TOUCH'
]));

/**
 * Normalises a command into tokens. Accepts "HGET", "XINFO STREAM" or
 * ["XINFO", "STREAM"]; args must be an array.
 * @param {unknown} command
 * @param {unknown} [args]
 * @returns {Array<string|number|Buffer>}
 */
function toTokens(command, args) {
  let head;
  if (Array.isArray(command)) {
    head = command.map((token) => (Buffer.isBuffer(token) || typeof token === 'number' ? token : String(token)));
  } else if (typeof command === 'string') {
    head = command.trim().split(/\s+/).filter(Boolean);
  } else {
    throw codeError('COMMAND_MISSING', 'Redis command is not set (node config or msg.command)');
  }
  if (head.length === 0 || typeof head[0] !== 'string' || head[0].length === 0) {
    throw codeError('COMMAND_MISSING', 'Redis command is not set (node config or msg.command)');
  }
  if (args !== undefined && args !== null && !Array.isArray(args)) {
    throw codeError('ARGS_INVALID', 'msg.args must be an array of arguments, e.g. ["mykey", "myvalue"]');
  }
  return [...head, ...(args || [])];
}

/**
 * Throws when a command is denied (RDS-CMD-3, RDS-CMD-4) or would block the
 * shared connection.
 * @param {Array<string|number|Buffer>} tokens
 * @param {{ denylist?: boolean, extraDenied?: string[] }} [options]
 */
function checkCommand(tokens, { denylist = true, extraDenied = [], fromMsg = false } = {}) {
  const first = String(tokens[0]).toUpperCase();
  const second = tokens.length > 1 ? String(tokens[1]).toUpperCase() : '';
  if (CONNECTION_COMMANDS.has(first) || (first === 'CLIENT' && CONNECTION_CLIENT_SUBCOMMANDS.has(second))) {
    const name = first === 'CLIENT' ? `CLIENT ${second}` : first;
    throw codeError('COMMAND_BLOCKING', `${name} is not allowed on the shared connection; use the dedicated list-in, subscribe or stream-in nodes`);
  }
  if ((first === 'XREAD' || first === 'XREADGROUP') && tokens.some((token) => String(token).toUpperCase() === 'BLOCK')) {
    throw codeError('COMMAND_BLOCKING', `${first} with BLOCK is not allowed on the shared connection; use stream-in`);
  }
  if (!denylist) return;
  if (fromMsg && SCRIPT_COMMANDS.has(first)) {
    throw codeError('COMMAND_DENIED', `${first} from msg is denied: a script from the message bypasses the denylist; set the script in the node configuration`);
  }
  const extra = extraDenied.map((entry) => entry.toUpperCase());
  for (const { entry, second: deniedSubcommand } of DEFAULT_DENIED_BY_COMMAND.get(first) || []) {
    if (deniedSubcommand === undefined || deniedSubcommand === second) {
      throw codeError('COMMAND_DENIED', `${entry} is in the command denylist`);
    }
  }
  for (const entry of extra) {
    const parts = entry.split(/\s+/);
    if (parts[0] !== first) continue;
    if (parts.length === 1 || parts[1] === second) {
      throw codeError('COMMAND_DENIED', `${entry} is in the command denylist`);
    }
  }
}

module.exports = { DEFAULT_DENYLIST, CONNECTION_COMMANDS, SCRIPT_COMMANDS, toTokens, checkCommand };
