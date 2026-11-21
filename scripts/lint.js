'use strict';

// Forbidden patterns of the redkern standard (kit standard 8, rule 5;
// KIT-ARCH-3, KIT-ARCH-4, KIT-HTTP-10) plus RDS-CON-1: Redis clients only from
// kit createRedisClient. Checks shipped code: lib/, nodes/, editor/.

const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const RULES = [
  [/Symbol\.for\(/, 'Symbol.for singletons are forbidden (KIT-ARCH-3)'],
  [/\bglobal(This)?\./, 'global state is forbidden (KIT-ARCH-3)'],
  [/\bprocess\.(on|once|addListener|env)\b/, 'process-level handlers and env reads are forbidden; use kit readEnv (KIT-ARCH-3)'],
  [/require\(['"]node-red['"]\)/, 'require("node-red") is forbidden (KIT-ARCH-4)'],
  [/require\.main/, 'require.main is forbidden (KIT-ARCH-4)'],
  [/@node-red\//, '@node-red/* internals are forbidden (KIT-ARCH-4)'],
  [/_router\b/, 'express internals are forbidden (KIT-ARCH-4)'],
  [/\b(RED|httpAdmin|httpNode|node)\._[A-Za-z0-9]/, 'private fields of RED, httpAdmin, httpNode and node are forbidden (KIT-ARCH-4)'],
  [/\bhttpNode\b/, 'routes on RED.httpNode are forbidden (KIT-HTTP-10)'],
  [/new\s+(Redis|IORedis|ioredis)\s*\(|\.Cluster\s*\(|new\s+Cluster\s*\(/, 'Redis clients only through kit createRedisClient (RDS-CON-1)']
];

function files(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return entry.name === 'locales' || entry.name === 'icons' ? [] : files(full);
    return /\.(js|html)$/.test(entry.name) ? [full] : [];
  });
}

const problems = [];
for (const dir of ['lib', 'nodes', 'editor']) {
  for (const file of files(path.join(ROOT, dir))) {
    const lines = fs.readFileSync(file, 'utf8').split('\n');
    lines.forEach((line, index) => {
      const code = line.replace(/\/\/.*$/, '').replace(/^\s*\*.*$/, '');
      for (const [pattern, message] of RULES) {
        if (pattern.test(code)) problems.push(`${path.relative(ROOT, file)}:${index + 1}: ${message}`);
      }
    });
  }
}

if (problems.length) {
  console.error(problems.join('\n'));
  process.exit(1);
}
console.log('lint: ok');
