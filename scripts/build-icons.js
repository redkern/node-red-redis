'use strict';

// Generates nodes/icons/*.svg from the redkern icon catalogue (kit standard
// 8.1): the palette motif (a stack of three flattened discs) in the top two
// thirds and a per-node action sign in the bottom third. 40x60, white strokes
// on a transparent background. Run: node scripts/build-icons.js [--check]

const fs = require('node:fs');
const path = require('node:path');

const MOTIF = 'M8 13a12 4 0 1 0 24 0a12 4 0 1 0 -24 0 M8 21a12 4 0 1 0 24 0a12 4 0 1 0 -24 0 M8 29a12 4 0 1 0 24 0a12 4 0 1 0 -24 0';

const SIGNS = {
  command: ['Command', 'M12 42l5 4l-5 4 M20 51h9'],
  multi: ['Transaction', 'M12 41h16v13h-16z M15 47l3 3l6 -6'],
  scan: ['Scan', 'M14 47a5 5 0 1 0 10 0a5 5 0 1 0 -10 0 M23 51l5 5'],
  subscribe: ['Subscribe', 'M19 49a1 1 0 1 0 2 0a1 1 0 1 0 -2 0 M15 44a7 7 0 0 0 0 10 M25 44a7 7 0 0 1 0 10 M11 41a12 12 0 0 0 0 16 M29 41a12 12 0 0 1 0 16'],
  'list-in': ['List in', 'M20 41v14 M15 50l5 5l5 -5'],
  'stream-in': ['Stream in', 'M7 48h1 M11 48h1 M15 48h14 M25 44l4 4l-4 4'],
  'stream-out': ['Stream out', 'M20 56v-14 M15 46l5 -5l5 5'],
  'stream-ack': ['Stream ack', 'M12 48l5 5l11 -11'],
  'stream-claim': ['Stream claim', 'M27 45a7 7 0 1 0 1 7 M28 40v5h-5'],
  'stream-gc': ['Stream gc', 'M12 52a3 3 0 1 0 6 0a3 3 0 1 0 -6 0 M22 52a3 3 0 1 0 6 0a3 3 0 1 0 -6 0 M17 50l11 -9 M23 50l-11 -9'],
  'stream-metrics': ['Stream metrics', 'M12 56v-5 M20 56v-11 M28 56v-8'],
  'stream-control': ['Stream control', 'M10 43v12 M15 43v12 M21 43l9 6l-9 6z']
};

function svg(title, d) {
  return [
    '<svg xmlns="http://www.w3.org/2000/svg" width="40" height="60" viewBox="0 0 40 60">',
    `  <title>${title}</title>`,
    `  <path fill="none" stroke="#ffffff" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" d="${d}"/>`,
    '</svg>',
    ''
  ].join('\n');
}

const dir = path.join(__dirname, '..', 'nodes', 'icons');
const files = { 'redkern-redis.svg': svg('redkern redis', MOTIF) };
for (const [name, [title, sign]] of Object.entries(SIGNS)) files[`redkern-redis-${name}.svg`] = svg(title, `${MOTIF} ${sign}`);

const check = process.argv.includes('--check');
let stale = 0;
fs.mkdirSync(dir, { recursive: true });
for (const [file, content] of Object.entries(files)) {
  const target = path.join(dir, file);
  if (check) {
    if (!fs.existsSync(target) || fs.readFileSync(target, 'utf8') !== content) {
      console.error(`stale icon: ${file}`);
      stale += 1;
    }
  } else {
    fs.writeFileSync(target, content);
  }
}
if (stale) process.exit(1);
