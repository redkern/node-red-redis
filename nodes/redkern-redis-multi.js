'use strict';

const { toTokens, checkCommand } = require('../lib/denylist.js');
const { codeError, setMeta } = require('../lib/format.js');
const { INPUT_SCHEMA, startShared, inputOptions, primary } = require('../lib/node-helpers.js');

const MAX_COMMANDS = 10000;

/**
 * Accepts [["SET","k","v"],["INCR","c"]] from msg.commands or the node
 * configuration (JSON). Each entry is normalised and checked against the
 * denylist (RDS-CMD-4).
 */
function parseCommands(value, denylist, fromMsg = false) {
  let commands = value;
  if (typeof commands === 'string') {
    if (commands.trim() === '') commands = [];
    else {
      try { commands = JSON.parse(commands); } catch {
        throw codeError('COMMANDS_INVALID', 'Commands must be a JSON array of arrays');
      }
    }
  }
  if (!Array.isArray(commands) || commands.length === 0) {
    throw codeError('COMMANDS_INVALID', 'msg.commands must be a non-empty array, e.g. [["set","k","v"],["incr","c"]]');
  }
  if (commands.length > MAX_COMMANDS) throw codeError('COMMANDS_TOO_MANY', `A transaction is limited to ${MAX_COMMANDS} commands`);
  return commands.map((entry) => {
    if (!Array.isArray(entry) || entry.length === 0) {
      throw codeError('COMMANDS_INVALID', 'Each entry in msg.commands must be an array: [command, ...args]');
    }
    const tokens = toTokens([entry[0]], entry.slice(1));
    checkCommand(tokens, { denylist, fromMsg });
    return tokens;
  });
}

module.exports = function register(RED, palette) {
  const { rk } = palette;

  function RedisMultiNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      commands: { type: 'str', default: '' },
      disableDenylist: { type: 'bool', default: false },
      ...INPUT_SCHEMA
    });
    const options = parsed.ok ? parsed.value : {};
    const shared = startShared(RED, k, node, config.server);
    let count = 0;

    k.onInput(async (msg, send) => {
      const fromMsg = msg.commands !== undefined;
      const commands = parseCommands(fromMsg ? msg.commands : options.commands, !options.disableDenylist, fromMsg);
      const results = await shared.client().multi(commands.map((tokens) => ['call', ...tokens])).exec();
      if (!Array.isArray(results)) throw codeError('MULTI_ABORTED', 'Transaction was aborted');
      const failed = results.filter(([error]) => error);
      if (failed.length) {
        throw codeError('MULTI_FAILED', `Transaction failed: ${failed.map(([error]) => error.message).join('; ')}`);
      }
      msg.payload = results.map(([, result]) => result);
      setMeta(msg, { multi: { commands: commands.length } });
      count += 1;
      k.status('ok', `exec ${count}`);
      primary(send, options.errorOutput, msg);
    }, inputOptions(parsed, 1));
  }

  RED.nodes.registerType('redkern-redis-multi', RedisMultiNode);
};

module.exports.parseCommands = parseCommands;
