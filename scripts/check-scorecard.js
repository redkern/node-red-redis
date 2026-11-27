'use strict';

// RDS-NR-1: `node-red-dev validate` reports failed checks in its scorecard but
// always exits with 0, so CI reads the scorecard and fails unless the run is
// complete and every check passed.
// Usage: npx --yes node-red-dev@0.1.6 validate -p . -o scorecard.json
//        node scripts/check-scorecard.js scorecard.json

const fs = require('node:fs');
const path = require('node:path');

// Every check node-red-dev 0.1.6 runs for a package with nodes (P02 is
// disabled in the tool). A scorecard without one of them is an interrupted run.
const REQUIRED = ['P01', 'P03', 'P04', 'P05', 'P06', 'P07', 'P08', 'N01', 'N02', 'D01', 'D02', 'D03'];

// Deviations recorded in the spec under RDS-NR-1 (KIT-NR-17). D03 fails for a
// dependency that is not at its latest version; ioredis is pinned on purpose
// (RDS-CON-2) and moves only with a tested release.
const ALLOWED = { D03: ['ioredis'] };

const file = process.argv[2];
if (!file) {
  console.error('usage: node scripts/check-scorecard.js <scorecard.json>');
  process.exit(1);
}

let scorecard;
try {
  scorecard = JSON.parse(fs.readFileSync(file, 'utf8'));
} catch (err) {
  console.error(`check-scorecard: cannot read ${file}: ${err.message}`);
  process.exit(1);
}

const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
const problems = [];

if (!scorecard || typeof scorecard !== 'object' || Array.isArray(scorecard)) {
  problems.push('the scorecard is not an object');
  scorecard = {};
}
if (scorecard.package?.name !== pkg.name || scorecard.package?.version !== pkg.version) {
  problems.push(`the scorecard is for ${JSON.stringify(scorecard.package)}, not ${pkg.name}@${pkg.version}`);
}

for (const id of REQUIRED) {
  const check = scorecard[id];
  if (!check || typeof check.test !== 'boolean') {
    problems.push(`${id} is missing: node-red-dev did not finish`);
    continue;
  }
  if (check.test) continue;
  const allowed = ALLOWED[id];
  const offending = Array.isArray(check.packages) ? check.packages.filter((name) => !allowed?.includes(name)) : null;
  if (allowed && offending && check.packages.length && !offending.length) {
    console.log(`check-scorecard: ${id} accepted as a recorded deviation for ${check.packages.join(', ')}`);
    continue;
  }
  problems.push(`${id} failed: ${JSON.stringify(check)}`);
}

// N02 passes vacuously when no node definitions were parsed from the editor
// HTML, so it must name the nodes it found in the examples.
if (scorecard.N02?.test && !(Array.isArray(scorecard.N02.nodes) && scorecard.N02.nodes.length)) {
  problems.push('N02 passed without any nodes: node definitions were not parsed');
}

const unknown = Object.keys(scorecard).filter((key) => key !== 'package' && !REQUIRED.includes(key));
for (const id of unknown) {
  if (scorecard[id]?.test === false) problems.push(`${id} failed: ${JSON.stringify(scorecard[id])}`);
}

if (problems.length) {
  console.error(`check-scorecard: node-red-dev validation not accepted\n${problems.join('\n')}`);
  process.exit(1);
}
console.log(`check-scorecard: all ${REQUIRED.length} node-red-dev checks complete and accepted`);
