'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { summarizeQueue } = require('../../scripts/load-queue.js');

describe('load queue acceptance', () => {
  it('accepts a short high peak when the queue returns to its baseline', () => {
    const result = summarizeQueue([
      { ms: 0, queued: 0 }, { ms: 4000, queued: 3000 },
      { ms: 4200, queued: 0 }, { ms: 10000, queued: 0 }
    ], 100);
    assert.equal(result.firstAverage, 0);
    assert.equal(result.lastAverage, 0);
    assert.equal(result.growthWithinLimit, true);
    assert.equal(result.drainedWithinLimit, true);
  });

  it('rejects sustained queue growth even when the queue eventually drains', () => {
    const result = summarizeQueue([
      { ms: 0, queued: 100 }, { ms: 3000, queued: 100 },
      { ms: 7000, queued: 1000 }, { ms: 10000, queued: 1000 }
    ], 100);
    assert.equal(result.growth, 900);
    assert.equal(result.growthWithinLimit, false);
    assert.equal(result.drainedWithinLimit, true);
  });

  it('weights queue samples by their duration within each window', () => {
    const result = summarizeQueue([
      { ms: 0, queued: 0 }, { ms: 1000, queued: 300 }, { ms: 2000, queued: 0 },
      { ms: 7000, queued: 100 }, { ms: 8500, queued: 300 }, { ms: 10000, queued: 0 }
    ], 0);
    assert.equal(result.firstAverage, 100);
    assert.equal(result.lastAverage, 200);
    assert.equal(result.growth, 100);
  });

  it('enforces the growth and drain boundaries', () => {
    const samples = [
      { ms: 0, queued: 100 }, { ms: 3000, queued: 100 },
      { ms: 7000, queued: 600 }, { ms: 10000, queued: 600 }
    ];
    assert.equal(summarizeQueue(samples, 1000).growthWithinLimit, true);
    assert.equal(summarizeQueue(samples, 1000).drainedWithinLimit, true);
    samples[2].queued = 601;
    assert.equal(summarizeQueue(samples, 1000).growthWithinLimit, false);
    assert.equal(summarizeQueue(samples, 1000.1).drainedWithinLimit, false);
    assert.equal(summarizeQueue(samples, null).drainedWithinLimit, false);
  });

  it('rejects incomplete or malformed observations', () => {
    assert.throws(() => summarizeQueue([{ ms: 0, queued: 0 }, { ms: 5000, queued: 0 }], 0));
    assert.throws(() => summarizeQueue([{ ms: 0, queued: 0 }, { ms: 7000, queued: -1 }], 0));
    assert.throws(() => summarizeQueue([{ ms: 0, queued: 0 }, { ms: 7000, queued: 0 }], -1));
  });
});
