'use strict';

/**
 * In-flight registry (RDS-STR-4, RDS-STR-5). Holds stream entry IDs that a
 * stream-in node has sent into the flow and nobody has acknowledged yet.
 *
 * The registry lives in the palette closure, not in a node, so it survives a
 * redeploy of stream-in: the new instance has the same consumer name and
 * recovers the same PEL, and the registry stops it from sending entries the
 * old flow is still processing. stream-ack removes entries through the same
 * registry. An entry also leaves the registry when it moves to the DLQ or when
 * it is older than its TTL (reclaimIdleMs), after which it may be redelivered.
 */
class InflightRegistry {
  /** @param {{ now?: () => number }} [options] */
  constructor({ now = () => Date.now() } = {}) {
    this.now = now;
    /** @type {Map<string, Map<string, { consumer: string, expiresAt: number }>>} */
    this.groups = new Map();
    /** @type {Set<(key: string) => void>} */
    this.listeners = new Set();
  }

  static key(stream, group) {
    return `${stream}\u0000${group}`;
  }

  entries(stream, group, create = false) {
    const key = InflightRegistry.key(stream, group);
    let entries = this.groups.get(key);
    if (!entries && create) {
      entries = new Map();
      this.groups.set(key, entries);
    }
    return entries;
  }

  add(stream, group, id, consumer, ttlMs) {
    this.entries(stream, group, true).set(id, { consumer, expiresAt: this.now() + ttlMs });
  }

  /** True when the ID is in flight and has not expired. */
  has(stream, group, id) {
    const entries = this.entries(stream, group);
    const entry = entries?.get(id);
    if (!entry) return false;
    if (entry.expiresAt <= this.now()) {
      entries.delete(id);
      return false;
    }
    return true;
  }

  /** @returns {number} number of removed IDs */
  remove(stream, group, ids) {
    const entries = this.entries(stream, group);
    if (!entries) return 0;
    let removed = 0;
    for (const id of ids) if (entries.delete(String(id))) removed += 1;
    if (entries.size === 0) this.groups.delete(InflightRegistry.key(stream, group));
    if (removed) this.notify(InflightRegistry.key(stream, group));
    return removed;
  }

  /**
   * Keeps only IDs present in `ids` for the consumer (reconcile with XPENDING).
   * With `upTo`, IDs above it are kept: a truncated XPENDING page says nothing
   * about them.
   */
  retain(stream, group, consumer, ids, upTo) {
    const entries = this.entries(stream, group);
    if (!entries) return 0;
    const keep = new Set(ids.map(String));
    let removed = 0;
    for (const [id, entry] of entries) {
      if (upTo !== undefined && compareIds(id, upTo) > 0) continue;
      if (entry.consumer === consumer && !keep.has(id)) {
        entries.delete(id);
        removed += 1;
      }
    }
    if (removed) this.notify(InflightRegistry.key(stream, group));
    return removed;
  }

  count(stream, group, consumer) {
    const entries = this.entries(stream, group);
    if (!entries) return 0;
    const now = this.now();
    let count = 0;
    for (const [id, entry] of entries) {
      if (entry.expiresAt <= now) {
        entries.delete(id);
        continue;
      }
      if (consumer === undefined || entry.consumer === consumer) count += 1;
    }
    return count;
  }

  /** Clears a consumer's entries (used when a node is removed, not redeployed). */
  clearConsumer(stream, group, consumer) {
    const entries = this.entries(stream, group);
    if (!entries) return;
    for (const [id, entry] of entries) if (entry.consumer === consumer) entries.delete(id);
    if (entries.size === 0) this.groups.delete(InflightRegistry.key(stream, group));
  }

  onRemove(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  notify(key) {
    for (const listener of this.listeners) {
      try { listener(key); } catch { /* listeners are internal and must not throw */ }
    }
  }
}

/** Compares two stream IDs "ms-seq" numerically. */
function compareIds(a, b) {
  const [am, as = '0'] = String(a).split('-');
  const [bm, bs = '0'] = String(b).split('-');
  const ms = BigInt(am) - BigInt(bm);
  if (ms !== 0n) return ms > 0n ? 1 : -1;
  const seq = BigInt(as) - BigInt(bs);
  return seq === 0n ? 0 : seq > 0n ? 1 : -1;
}

module.exports = { compareIds, InflightRegistry };
