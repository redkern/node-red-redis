'use strict';

const CONTENT_TYPE = 'text/plain; version=0.0.4; charset=utf-8';

const DEFINITIONS = Object.freeze({
  redkern_redis_connections: { type: 'gauge', help: 'Open Redis connections by role.' },
  redkern_redis_delivered_total: { type: 'counter', help: 'Stream entries sent into flows by stream-in.' },
  redkern_redis_redeliveries_total: { type: 'counter', help: 'Stream entries delivered more than once (recovery, reclaim, XAUTOCLAIM).' },
  redkern_redis_acked_total: { type: 'counter', help: 'Stream entries acknowledged by stream-ack or auto ACK.' },
  redkern_redis_dead_letter_total: { type: 'counter', help: 'Stream entries moved to the dead-letter stream.' },
  redkern_redis_consumer_paused: { type: 'gauge', help: 'Whether a stream-in consumer is paused (1) or reading (0).' },
  redkern_redis_consumer_inflight: { type: 'gauge', help: 'Entries sent by stream-in and not yet acknowledged.' },
  redkern_redis_stream_pending: { type: 'gauge', help: 'Consumer group PEL size from the last stream-metrics sample.' },
  redkern_redis_stream_lag: { type: 'gauge', help: 'Consumer group lag from the last stream-metrics sample.' },
  redkern_redis_stream_dead_letter_length: { type: 'gauge', help: 'DLQ stream length from the last stream-metrics sample.' },
  redkern_redis_stream_consumers: { type: 'gauge', help: 'Consumers in the group from the last stream-metrics sample.' }
});

function escapeLabel(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
}

function labelKey(labels) {
  const keys = Object.keys(labels).sort();
  return keys.map((key) => `${key}="${escapeLabel(labels[key])}"`).join(',');
}

/**
 * Palette-owned metric registry (RDS-STR-14). Rendered once per scrape in
 * Prometheus text 0.0.4; values are plain numbers kept in memory.
 */
class MetricsRegistry {
  constructor() {
    /** @type {Map<string, Map<string, number>>} */
    this.series = new Map();
    /** @type {Set<() => void>} */
    this.collectors = new Set();
  }

  inc(name, labels = {}, value = 1) {
    const series = this.family(name);
    const key = labelKey(labels);
    series.set(key, (series.get(key) || 0) + value);
  }

  set(name, labels = {}, value = 0) {
    this.family(name).set(labelKey(labels), Number(value) || 0);
  }

  delete(name, labels = {}) {
    this.series.get(name)?.delete(labelKey(labels));
  }

  family(name) {
    if (!DEFINITIONS[name]) throw new TypeError(`Unknown metric ${name}`);
    let series = this.series.get(name);
    if (!series) {
      series = new Map();
      this.series.set(name, series);
    }
    return series;
  }

  /** Registers a function called before each render to refresh gauges. */
  collect(fn) {
    this.collectors.add(fn);
    return () => this.collectors.delete(fn);
  }

  render() {
    for (const collector of this.collectors) {
      try { collector(); } catch { /* a broken collector must not break the scrape */ }
    }
    const lines = [];
    for (const [name, definition] of Object.entries(DEFINITIONS)) {
      lines.push(`# HELP ${name} ${definition.help}`, `# TYPE ${name} ${definition.type}`);
      const series = this.series.get(name);
      if (!series) continue;
      for (const [labels, value] of series) lines.push(labels ? `${name}{${labels}} ${value}` : `${name} ${value}`);
    }
    return `${lines.join('\n')}\n`;
  }
}

module.exports = { MetricsRegistry, CONTENT_TYPE, DEFINITIONS };
