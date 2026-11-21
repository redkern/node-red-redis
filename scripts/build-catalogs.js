'use strict';

// Writes the message catalogs from scripts/catalog.en-US.js.
// Run: node scripts/build-catalogs.js [--check]

const fs = require('node:fs');
const path = require('node:path');
const { common, nodes } = require('./catalog.en-US.js');

const ROOT = path.join(__dirname, '..');
// One node set (KIT-ARCH-2), so one catalog for all node types.
const outputs = {
  [path.join(ROOT, 'nodes', 'locales', 'en-US', 'redkern-redis.json')]: { common, ...nodes }
};

const check = process.argv.includes('--check');
let stale = 0;
for (const [file, catalog] of Object.entries(outputs)) {
  const content = `${JSON.stringify(catalog, null, 2)}\n`;
  if (check) {
    if (!fs.existsSync(file) || fs.readFileSync(file, 'utf8') !== content) {
      console.error(`stale catalog: ${path.relative(ROOT, file)}`);
      stale += 1;
    }
    continue;
  }
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content);
}
if (stale) process.exit(1);
