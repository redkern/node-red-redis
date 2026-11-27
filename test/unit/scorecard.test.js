'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const pkg = require('../../package.json');

const SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'check-scorecard.js');

function complete() {
  const pass = { test: true };
  return {
    package: { name: pkg.name, version: pkg.version },
    P01: { test: true, license: pkg.license }, P03: pass, P04: pass, P08: pass, P05: pass,
    P06: { test: true, versions: ['4.1.18', '5.0.10'] }, P07: { test: true, version: '>=22' },
    N01: pass, N02: { test: true, nodes: ['redkern-redis-command'] },
    D01: { test: true, total: 2 }, D02: pass, D03: pass
  };
}

function run(scorecard) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'scorecard-'));
  const file = path.join(dir, 'scorecard.json');
  if (scorecard !== undefined) fs.writeFileSync(file, typeof scorecard === 'string' ? scorecard : JSON.stringify(scorecard));
  const result = spawnSync(process.execPath, [SCRIPT, file], { encoding: 'utf8' });
  fs.rmSync(dir, { recursive: true, force: true });
  return result;
}

describe('check-scorecard', () => {
  it('RDS-NR-1 accepts a complete scorecard with every check passed', () => {
    assert.equal(run(complete()).status, 0);
  });

  it('RDS-NR-1 rejects a partial scorecard even when its checks passed', () => {
    const partial = complete();
    for (const id of ['P06', 'P07', 'P08', 'N01', 'N02', 'D01', 'D02', 'D03']) delete partial[id];
    const result = run(partial);
    assert.equal(result.status, 1);
    assert.match(result.stderr, /P08 is missing/);
    assert.match(result.stderr, /D03 is missing/);
  });

  it('RDS-NR-1 rejects a missing, empty or unreadable scorecard', () => {
    assert.equal(run(undefined).status, 1);
    assert.equal(run({}).status, 1);
    assert.equal(run('{"P01":').status, 1);
  });

  it('RDS-NR-1 rejects a failed check and a scorecard for another package or version', () => {
    assert.equal(run({ ...complete(), N01: { test: false, nodes: ['other'] } }).status, 1);
    assert.equal(run({ ...complete(), package: { name: pkg.name, version: '0.0.1' } }).status, 1);
  });

  it('RDS-NR-1 accepts D03 only for the pinned ioredis', () => {
    assert.equal(run({ ...complete(), D03: { test: false, packages: ['ioredis'] } }).status, 0);
    assert.equal(run({ ...complete(), D03: { test: false, packages: ['ioredis', '@redkern/node-red-kit'] } }).status, 1);
    assert.equal(run({ ...complete(), D03: { test: false } }).status, 1);
  });

  it('RDS-NR-1 rejects N02 that passed without any parsed nodes', () => {
    assert.equal(run({ ...complete(), N02: { test: true, nodes: [] } }).status, 1);
  });
});
