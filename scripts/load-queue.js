'use strict';

const WINDOW_MS = 3000;
const MAX_GROWTH = 500;
const MAX_DRAIN_MS = 1000;

function summarizeQueue(samples, drainMs) {
  if (!Array.isArray(samples) || samples.length < 2 || samples[0].ms !== 0 ||
      samples[samples.length - 1].ms < 2 * WINDOW_MS) {
    throw new Error('queue samples must cover at least six seconds from zero');
  }
  if (drainMs !== null && (!Number.isFinite(drainMs) || drainMs < 0)) {
    throw new Error('queue drain time must be non-negative or null');
  }

  const end = samples[samples.length - 1].ms;
  let firstArea = 0;
  let lastArea = 0;
  for (let index = 0; index < samples.length; index += 1) {
    const sample = samples[index];
    if (!Number.isFinite(sample.ms) || !Number.isInteger(sample.queued) || sample.queued < 0 ||
        (index > 0 && sample.ms <= samples[index - 1].ms)) {
      throw new Error('queue samples must have increasing times and non-negative queue sizes');
    }
    if (index === samples.length - 1) break;
    const next = samples[index + 1].ms;
    firstArea += sample.queued * Math.max(0, Math.min(next, WINDOW_MS) - sample.ms);
    lastArea += sample.queued * Math.max(0, next - Math.max(sample.ms, end - WINDOW_MS));
  }

  const firstAverage = firstArea / WINDOW_MS;
  const lastAverage = lastArea / WINDOW_MS;
  const growth = lastAverage - firstAverage;
  return {
    firstAverage,
    lastAverage,
    growth,
    drainMs,
    growthWithinLimit: growth <= MAX_GROWTH,
    drainedWithinLimit: drainMs !== null && drainMs <= MAX_DRAIN_MS
  };
}

module.exports = { summarizeQueue };
