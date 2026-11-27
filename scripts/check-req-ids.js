'use strict';

// Requirement check (kit standard 8, rule "Перевірка вимог"): every
// requirement of this package in docs/spec.md (a list item starting with
// "<PREFIX>-<AREA>-<N>.") is verified by a test with its ID in the test name,
// or is tagged [CI], [лінт] or [рев'ю]. IDs of other packages are ignored.
// Requirements under a heading that ends with a version, e.g.
// "### Повернення з DLQ (1.2.0)", are planned for that release: they are
// reported but not enforced until package.json reaches that version.

const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const prefix = require('../package.json').redkern?.reqPrefix;
if (!prefix) {
  console.error('package.json redkern.reqPrefix is required');
  process.exit(1);
}
const spec = fs.readFileSync(path.join(ROOT, 'docs', 'spec.md'), 'utf8');
const version = require('../package.json').version;
const problems = [];
const requirements = new Map();
const planned = new Map();

// Compares the release parts of two semver strings; pre-release tags are ignored.
function versionBefore(a, b) {
  const pa = a.split('-')[0].split('.').map(Number);
  const pb = b.split('-')[0].split('.').map(Number);
  for (let i = 0; i < 3; i++) if (pa[i] !== pb[i]) return pa[i] < pb[i];
  return false;
}

const headings = [];
for (const line of spec.split('\n')) {
  const heading = line.match(/^(#{1,6})\s+(.*?)\s*$/);
  if (heading) {
    const level = heading[1].length;
    while (headings.length && headings[headings.length - 1].level >= level) headings.pop();
    headings.push({ level, release: heading[2].match(/\((\d+\.\d+\.\d+)\)$/)?.[1] });
    continue;
  }
  // "RDS-CTL-6..14" is a range in prose, not a requirement: the ID must be
  // followed by a single dot.
  const item = line.match(/^\s*[-*+•]\s+([A-Z0-9]+-[A-Z0-9]+-[0-9A-Za-z]+)\.(?!\.)\s*(.*)$/);
  if (!item || !item[1].startsWith(`${prefix}-`)) continue;
  if (!/^[A-Z]+-[A-Z]+-[0-9]+$/.test(item[1])) problems.push(`malformed requirement ID ${item[1]}`);
  if (requirements.has(item[1]) || planned.has(item[1])) problems.push(`duplicate requirement ${item[1]}`);
  const release = headings.findLast((h) => h.release)?.release;
  if (release && versionBefore(version, release)) planned.set(item[1], release);
  else requirements.set(item[1], item[2]);
}

function testFiles(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    return entry.isDirectory() ? testFiles(full) : entry.name.endsWith('.test.js') ? [full] : [];
  });
}
const names = [];
for (const file of testFiles(path.join(ROOT, 'test'))) {
  for (const match of fs.readFileSync(file, 'utf8').matchAll(/\bit\(\s*(['"`])(.*?)\1/g)) names.push(match[2]);
}
const tested = new Set(names.flatMap((name) => name.match(new RegExp(`${prefix}-[A-Z]+-[0-9]+`, 'g')) || []));

const report = [];
for (const [id, text] of requirements) {
  const tag = text.match(/\[(CI|лінт|рев'ю)\]/)?.[1];
  if (tested.has(id)) report.push(`${id}: test`);
  else if (tag) report.push(`${id}: [${tag}]`);
  else problems.push(`${id} has no test and no [CI]/[лінт]/[рев'ю] tag`);
}
for (const [id, release] of planned) report.push(`${id}: planned ${release}`);
for (const id of tested) if (!requirements.has(id) && !planned.has(id)) problems.push(`tests reference unknown requirement ${id}`);

if (process.argv.includes('--verbose')) console.log(report.join('\n'));
if (problems.length) {
  console.error(problems.join('\n'));
  process.exit(1);
}
console.log(`check-req-ids: ${requirements.size} requirements verified, ${planned.size} planned for later releases`);
