'use strict';

const { createPalette } = require('../lib/palette.js');

// One node per file; this node set is the palette closure (KIT-ARCH-2):
// kit.init, the in-flight registry, metrics and routes are created once here
// and handed to every node type.
const NODES = [
  require('./redkern-redis-config.js'),
  require('./redkern-redis-command.js'),
  require('./redkern-redis-multi.js'),
  require('./redkern-redis-scan.js'),
  require('./redkern-redis-subscribe.js'),
  require('./redkern-redis-list-in.js'),
  require('./redkern-redis-stream-in.js'),
  require('./redkern-redis-stream-out.js'),
  require('./redkern-redis-stream-ack.js'),
  require('./redkern-redis-stream-claim.js'),
  require('./redkern-redis-stream-gc.js'),
  require('./redkern-redis-stream-metrics.js'),
  require('./redkern-redis-stream-control.js')
];

module.exports = function (RED) {
  const palette = createPalette(RED);
  for (const register of NODES) register(RED, palette);
};

// For tests that simulate a redeploy: the runtime loads the node set once and
// keeps its closure across deploys.
module.exports.NODES = NODES;
