'use strict';

const { toTokens, checkCommand } = require('../lib/denylist.js');
const { codeError, setMeta } = require('../lib/format.js');
const { INPUT_SCHEMA, startShared, inputOptions, primary } = require('../lib/node-helpers.js');

module.exports = function register(RED, palette) {
  const { rk } = palette;

  function RedisCommandNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      command: { type: 'str', default: '' },
      allowMsgCommand: { type: 'bool', default: false },
      disableDenylist: { type: 'bool', default: false },
      legacyMsgFields: { type: 'bool', default: false },
      ...INPUT_SCHEMA
    });
    const options = parsed.ok ? parsed.value : {};
    if (parsed.ok && options.disableDenylist) k.log.warn('Command denylist is disabled for this node', { key: 'DENYLIST_DISABLED' });
    const shared = startShared(RED, k, node, config.server);
    let count = 0;

    k.onInput(async (msg, send) => {
      const useMsg = options.allowMsgCommand && msg.command !== undefined && msg.command !== null && msg.command !== '';
      const tokens = toTokens(useMsg ? msg.command : options.command, msg.args);
      checkCommand(tokens, { denylist: !options.disableDenylist, fromMsg: useMsg });
      const name = String(tokens[0]).toUpperCase();
      let result;
      try {
        result = await shared.client().call(...tokens);
      } catch (error) {
        // Redis reply errors carry the server message only, never the arguments.
        throw codeError(error?.code || 'REDIS_COMMAND_FAILED', error?.message || 'Redis command failed');
      }
      msg.payload = result;
      setMeta(msg, { command: name });
      if (options.legacyMsgFields) msg.command = useMsg ? msg.command : options.command;
      count += 1;
      k.status('ok', `${name.toLowerCase()} ${count}`);
      primary(send, options.errorOutput, msg);
    }, inputOptions(parsed, 1));
  }

  RED.nodes.registerType('redkern-redis-command', RedisCommandNode);
};
