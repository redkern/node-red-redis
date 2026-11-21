'use strict';

const { encodeFields, codeError, setMeta } = require('../lib/format.js');
const { INPUT_SCHEMA, startShared, inputOptions, primary } = require('../lib/node-helpers.js');

module.exports = function register(RED, palette) {
  const { rk } = palette;

  function RedisStreamOutNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const k = rk.bind(node);
    const parsed = k.configure(config, {
      server: { type: 'str', required: true },
      streamKey: { type: 'str', default: '' },
      allowMsgStream: { type: 'bool', default: false },
      maxlen: { type: 'int', min: 0, max: Number.MAX_SAFE_INTEGER, default: 0 },
      approxTrim: { type: 'bool', default: true },
      unsafeTrim: { type: 'bool', default: false },
      legacyMsgFields: { type: 'bool', default: false },
      ...INPUT_SCHEMA
    });
    const options = parsed.ok ? parsed.value : {};
    const shared = startShared(RED, k, node, config.server);
    let count = 0;

    k.onInput(async (msg, send) => {
      const stream = options.allowMsgStream && typeof msg.stream === 'string' && msg.stream !== ''
        ? msg.stream
        : options.streamKey;
      if (!stream) throw codeError('STREAM_REQUIRED', 'Stream key is not set');
      const fields = encodeFields(msg.payload);
      const args = [stream];
      const maxlen = options.maxlen;
      if (maxlen > 0) {
        // Trimming can remove entries still pending in a consumer group, so it
        // stays behind an explicit opt-in (out of scope: automatic XTRIM).
        if (!options.unsafeTrim) throw codeError('TRIM_DISABLED', 'MAXLEN trimming needs the unsafe trim option');
        args.push('MAXLEN', options.approxTrim ? '~' : '=', String(maxlen));
      }
      args.push('*', ...fields);
      const id = await shared.client().xadd(...args);
      setMeta(msg, { xadd: { stream, id } });
      if (options.legacyMsgFields) {
        msg.streamId = id;
        msg.stream = stream;
      }
      count += 1;
      k.status('ok', `added ${count}`);
      primary(send, options.errorOutput, msg);
    }, inputOptions(parsed, 1));
  }

  RED.nodes.registerType('redkern-redis-stream-out', RedisStreamOutNode);
};
