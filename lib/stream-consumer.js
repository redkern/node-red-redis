'use strict';

const { decodeFields, codeError } = require('./format.js');
const { moveToDlq } = require('./dlq.js');
const { compareIds } = require('./inflight.js');

const DEFAULTS = Object.freeze({
  count: 100,
  blockMs: 5000,
  startId: '$',
  ackMode: 'manual',
  maxPending: 1000,
  backpressureScope: 'consumer',
  capacityCheckIntervalMs: 250,
  capacityPollMs: 500,
  maxDeliveries: 5,
  deadLetterStream: '',
  autoClaim: true,
  reclaimIdleMs: 60000,
  reclaimIntervalMs: 30000,
  reclaimCount: 100,
  readIntervalMs: 0,
  rateLimitPerSecond: 0,
  batchWindowMs: 0,
  batchIntervalMs: 0,
  startPaused: false,
  drainCheckIntervalMs: 250,
  pausePollMs: 1000,
  initialBackoffMs: 500,
  maxBackoffMs: 30000,
  backoffMultiplier: 2
});

function isNoGroup(error) {
  return /NOGROUP/.test(String(error?.message || error));
}

function abortError() {
  return Object.assign(new Error('The operation was aborted'), { name: 'AbortError', code: 'ABORT_ERR' });
}

/** Default abortable sleep. */
function defaultSleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(abortError());
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    function onAbort() {
      clearTimeout(timer);
      reject(abortError());
    }
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/**
 * Parses an XREADGROUP / XAUTOCLAIM entry list into { id, fields } where
 * fields is null for entries deleted while still pending.
 * @param {unknown} list
 */
function parseEntries(list) {
  if (!Array.isArray(list)) return [];
  // Redis 6.2 XAUTOCLAIM answers a bare nil for an entry deleted while
  // pending; it stays in the PEL and recovery acknowledges it later.
  return list.filter(Array.isArray).map(([id, flat]) => ({ id: String(id), fields: Array.isArray(flat) ? flat : null }));
}

/**
 * Redis Streams consumer loop (RDS-STR-1..11). The class has no Node-RED
 * dependency: clients, the clock and the output are injected so the loop is
 * unit-testable.
 *
 * Delivery order of work in one iteration:
 *   1. entries held back by pause are delivered first after resume;
 *   2. own PEL recovery through a cursor over XPENDING of this consumer;
 *      only entries that are not local are claimed back with XCLAIM, so the
 *      delivery count grows only for entries really sent again (RDS-STR-3);
 *   3. XAUTOCLAIM of abandoned entries with a cursor kept between calls
 *      (RDS-STR-9), only after own recovery finished;
 *   4. XREADGROUP ">" sized to the free backpressure capacity (RDS-STR-8).
 */
class StreamConsumer {
  /**
   * @param {{
   *   shared: any, blocking: any, stream: string, group: string, consumer: string,
   *   registry: import('./inflight.js').InflightRegistry,
   *   emit: (delivery: { entries: Array<{ id: string, payload: Record<string, unknown>, deliveries: number }>, batched: boolean }) => void,
   *   options?: Partial<typeof DEFAULTS>,
   *   metrics?: { inc: Function, set: Function },
   *   log?: { warn: Function, error: Function },
   *   onState?: (state: object) => void,
   *   clock?: { now: () => number, sleep: (ms: number, signal?: AbortSignal) => Promise<void>, setTimeout?: Function, clearTimeout?: Function },
   *   backoff?: { next: () => number, reset: () => void }
   * }} params
   */
  constructor(params) {
    this.shared = params.shared;
    this.blocking = params.blocking;
    this.stream = params.stream;
    this.group = params.group;
    this.consumer = params.consumer;
    this.registry = params.registry;
    this.emit = params.emit;
    this.options = { ...DEFAULTS, ...(params.options || {}) };
    if (!this.options.deadLetterStream) this.options.deadLetterStream = `${this.stream}:dlq`;
    this.batching = this.options.batchWindowMs > 0 || this.options.batchIntervalMs > 0;
    if (this.batching && !this.options.batchWindowMs) this.options.batchWindowMs = this.options.batchIntervalMs;
    this.metrics = params.metrics || { inc() {}, set() {} };
    this.log = params.log || { warn() {}, error() {} };
    this.onState = params.onState || (() => {});
    const clock = params.clock || {};
    this.clock = {
      now: clock.now || (() => Date.now()),
      sleep: clock.sleep || defaultSleep,
      setTimeout: clock.setTimeout || setTimeout,
      clearTimeout: clock.clearTimeout || clearTimeout
    };
    this.backoff = params.backoff || createBackoff(this.options);
    this.labels = { stream: this.stream, group: this.group };

    this.paused = Boolean(this.options.startPaused);
    /** Number of drain() calls running; while above 0, reserved entries are sent even though paused. */
    this.draining = 0;
    this.recoveryGeneration = 0;
    this.readInFlight = null;
    this.running = false;
    this.pausedAt = this.paused ? this.clock.now() : 0;
    this.recovering = true;
    this.cursor = '0';
    this.claimCursor = '0-0';
    this.lastReclaimAt = 0;
    this.lastReadAt = 0;
    this.lastCapacityCheckAt = -Infinity;
    this.groupPending = 0;
    this.rateTokens = this.options.rateLimitPerSecond;
    this.rateRefillAt = this.clock.now();
    /** Entries read from Redis but not yet sent (pause or batch window). */
    this.held = [];
    this.buffer = [];
    this.batchTimer = null;
    this.lastBatchAt = 0;
    this.delivered = 0;
    this.lastError = null;
    this.backpressure = false;
    this.wakeResolve = null;
    this.signal = undefined;
    this.unsubscribe = () => {};
  }

  /** True when entries must not be sent now: paused (and not draining) or closing. */
  get stopped() {
    return (this.paused && !this.draining) || Boolean(this.signal?.aborted);
  }

  /** IDs read but not sent yet, or in flight in the flow. */
  isLocal(id) {
    if (this.held.some((entry) => entry.id === id) || this.buffer.some((entry) => entry.id === id)) return true;
    return this.registry.has(this.stream, this.group, id);
  }

  get manualAck() {
    return this.options.ackMode !== 'auto';
  }

  snapshot() {
    return {
      stream: this.stream,
      group: this.group,
      consumer: this.consumer,
      paused: this.paused,
      recovering: this.recovering,
      backpressure: this.backpressure,
      inflight: this.manualAck ? this.registry.count(this.stream, this.group, this.consumer) : 0,
      held: this.held.length + this.buffer.length,
      maxPending: this.options.maxPending,
      delivered: this.delivered,
      lastError: this.lastError
    };
  }

  state(extra = {}) {
    this.onState({ ...this.snapshot(), ...extra });
  }

  wake() {
    const resolve = this.wakeResolve;
    this.wakeResolve = null;
    if (resolve) resolve();
  }

  async waitWake(ms) {
    const woken = new Promise((resolve) => { this.wakeResolve = resolve; });
    try {
      await Promise.race([this.clock.sleep(ms, this.signal), woken]);
    } finally {
      this.wakeResolve = null;
    }
  }

  async ensureGroup() {
    try {
      await this.shared.xgroup('CREATE', this.stream, this.group, this.options.startId, 'MKSTREAM');
    } catch (error) {
      if (!/BUSYGROUP/.test(String(error?.message))) throw error;
    }
  }

  /**
   * Runs until the signal aborts. Never rejects for Redis errors: they are
   * logged, reported through onState and retried with backoff.
   * @param {AbortSignal} signal
   */
  async run(signal) {
    this.signal = signal;
    // Subscribed here, not in the constructor, so a consumer that never runs
    // (failed start) leaves no listener behind.
    this.unsubscribe = this.registry.onRemove(() => this.wake());
    this.running = true;
    const onAbort = () => this.wake();
    signal.addEventListener('abort', onAbort, { once: true });
    try {
      while (!signal.aborted) {
        try {
          await this.iteration();
        } catch (error) {
          if (signal.aborted || error?.name === 'AbortError') break;
          await this.handleError(error);
        }
      }
    } finally {
      this.running = false;
      signal.removeEventListener('abort', onAbort);
      if (this.batchTimer) this.clock.clearTimeout(this.batchTimer);
      this.batchTimer = null;
      this.unsubscribe();
    }
  }

  async handleError(error) {
    if (isNoGroup(error)) {
      try {
        await this.ensureGroup();
        this.log.warn(`Consumer group restored: ${this.group}@${this.stream}`, { key: 'NOGROUP_RESTORED' });
        this.startRecovery();
        return;
      } catch (restoreError) {
        error = restoreError;
      }
    }
    // A lost XREADGROUP reply leaves its entries in this consumer's PEL, so
    // every failure is followed by a recovery pass.
    this.startRecovery();
    this.lastError = error?.code || error?.name || 'ERROR';
    this.state({ error: this.lastError });
    this.log.error(error, { key: error?.code || 'STREAM_LOOP_ERROR' });
    await this.clock.sleep(this.backoff.next(), this.signal).catch(() => {});
  }

  startRecovery() {
    this.recoveryGeneration += 1;
    this.recovering = true;
    this.cursor = '0';
  }

  async iteration() {
    if (this.paused && !this.draining) {
      this.state();
      await this.waitWake(this.options.pausePollMs);
      return;
    }
    if (this.held.length) {
      const entries = this.held.splice(0);
      await this.deliver(entries);
      return;
    }
    if (this.recovering) {
      await this.recoverStep();
      return;
    }
    if (this.draining) {
      // No new reads while draining; reserved entries were sent above.
      await this.waitWake(this.options.drainCheckIntervalMs);
      return;
    }
    await this.maybeAutoclaim();
    if (this.paused || this.recovering) return;

    const room = await this.capacity();
    if (room <= 0) {
      if (!this.backpressure) {
        this.backpressure = true;
        this.state();
      }
      await this.waitWake(this.options.capacityPollMs);
      return;
    }
    if (this.backpressure) {
      this.backpressure = false;
      this.state();
    }
    await this.waitReadInterval();
    if (this.paused || this.signal.aborted) return;

    this.lastReadAt = this.clock.now();
    // Drain waits for this read: entries it returns are reserved by this pod.
    this.readInFlight = this.blocking.xreadgroup(
      'GROUP', this.group, this.consumer,
      'COUNT', Math.max(1, Math.min(this.options.count, room)),
      'BLOCK', this.options.blockMs,
      'STREAMS', this.stream, '>'
    );
    let reply;
    try {
      reply = await this.readInFlight;
    } finally {
      this.readInFlight = null;
    }
    this.backoff.reset();
    if (this.lastError) {
      this.lastError = null;
      this.state();
    }
    if (!reply) return;
    const entries = parseEntries(reply[0]?.[1]).filter((entry) => entry.fields !== null)
      .map((entry) => ({ id: entry.id, payload: decodeFields(entry.fields), deliveries: 1 }));
    await this.deliver(entries);
  }

  /**
   * One page of own PEL recovery (RDS-STR-3). XPENDING of this consumer does
   * not touch delivery counts, so entries still in flight are only skipped.
   * The rest are claimed back with XCLAIM, which returns their fields, adds
   * one delivery and skips entries acknowledged since the XPENDING call.
   */
  async recoverStep() {
    const generation = this.recoveryGeneration;
    const start = this.cursor === '0' ? '-' : `(${this.cursor}`;
    const rows = await this.shared.xpending(this.stream, this.group, start, '+', this.options.count, this.consumer);
    this.backoff.reset();
    // A new pass was requested meanwhile (drain, an error): it starts over.
    if (generation !== this.recoveryGeneration) return;
    if (!Array.isArray(rows) || rows.length === 0) {
      this.recovering = false;
      this.cursor = '0';
      if (this.lastError) this.lastError = null;
      this.state();
      return;
    }
    this.cursor = String(rows[rows.length - 1][0]);
    const counts = new Map();
    let minIdle = Infinity;
    for (const row of rows) {
      const id = String(row[0]);
      if (this.isLocal(id)) continue;
      counts.set(id, Number(row[3]) || 1);
      minIdle = Math.min(minIdle, Number(row[2]) || 0);
    }
    if (counts.size === 0) return;
    // min-idle = the smallest idle time XPENDING just reported: an entry that
    // another consumer claims after that call has its idle time reset and is
    // not taken back, while every own candidate is at least that idle.
    const reply = await this.shared.xclaim(this.stream, this.group, this.consumer, Number.isFinite(minIdle) ? minIdle : 0, ...counts.keys());
    const list = Array.isArray(reply) ? reply : [];
    const claimed = parseEntries(list.filter(Boolean));
    // Redis 6.2 answers nil for entries deleted from the stream while pending
    // and keeps them in the PEL; Redis 7 and Valkey drop them from the PEL
    // themselves. XACK of an ID acknowledged meanwhile is a no-op.
    if (list.some((item) => item === null || (Array.isArray(item) && item[1] === null))) {
      // The nils carry no ID: check which unreturned IDs are really gone from
      // the stream (others were acknowledged or claimed meanwhile).
      const returned = new Set(claimed.filter((entry) => entry.fields !== null).map((entry) => entry.id));
      const gone = [];
      for (const id of counts.keys()) {
        if (returned.has(id)) continue;
        const range = await this.shared.xrange(this.stream, id, id);
        if (!Array.isArray(range) || range.length === 0) gone.push(id);
      }
      if (gone.length) await this.ackMissing(gone);
    }
    const entries = claimed.filter((entry) => entry.fields !== null && counts.has(entry.id) && !this.isLocal(entry.id))
      .map((entry) => ({ ...entry, deliveries: counts.get(entry.id) + 1 }));
    const live = await this.applyDeliveries(entries, true);
    await this.deliver(live);
  }

  async ackMissing(ids) {
    await this.shared.xack(this.stream, this.group, ...ids);
    this.log.warn(`Acknowledged ${ids.length} pending entries deleted from ${this.stream}; do not trim streams below the PEL horizon`, { key: 'ENTRY_DELETED' });
  }

  /** XAUTOCLAIM of abandoned entries (RDS-STR-9). */
  async maybeAutoclaim() {
    if (!this.options.autoClaim || !this.manualAck) return;
    const now = this.clock.now();
    if (now - this.lastReclaimAt < this.options.reclaimIntervalMs) return;
    this.lastReclaimAt = now;
    const reply = await this.shared.xautoclaim(
      this.stream, this.group, this.consumer, this.options.reclaimIdleMs,
      this.claimCursor, 'COUNT', this.options.reclaimCount
    );
    this.claimCursor = reply?.[0] ? String(reply[0]) : '0-0';
    const candidates = parseEntries(reply?.[1]).filter((entry) => entry.fields !== null && !this.isLocal(entry.id));
    if (candidates.length === 0) return;
    const live = await this.applyDeliveries(candidates);
    await this.deliver(live);
  }

  /**
   * Reads delivery counts with one range XPENDING per batch (RDS-STR-6) and
   * moves entries over maxDeliveries to the DLQ atomically (RDS-STR-7).
   * @param {Array<{ id: string, fields: unknown[] }>} entries
   */
  async applyDeliveries(entries, known = false) {
    if (entries.length === 0) return [];
    const decoded = entries.map((entry) => ({ id: entry.id, payload: decodeFields(entry.fields), deliveries: entry.deliveries || 1 }));
    let counts = null;
    if (!known) {
      const first = entries[0].id;
      const last = entries[entries.length - 1].id;
      const bound = Math.min(50000, entries.length + this.registry.count(this.stream, this.group, this.consumer) + this.options.count);
      const rows = await this.shared.xpending(this.stream, this.group, first, last, bound, this.consumer);
      counts = new Map((rows || []).map((row) => [String(row[0]), Number(row[3]) || 1]));
    }
    const dead = [];
    const live = [];
    for (const entry of decoded) {
      if (counts) entry.deliveries = counts.get(entry.id) || 1;
      if (this.options.maxDeliveries > 0 && entry.deliveries > this.options.maxDeliveries) dead.push(entry);
      else live.push(entry);
    }
    if (dead.length) {
      await moveToDlq(this.shared, {
        stream: this.stream,
        group: this.group,
        dlq: this.options.deadLetterStream,
        consumer: this.consumer
      }, dead);
      this.registry.remove(this.stream, this.group, dead.map((entry) => entry.id));
      this.metrics.inc('redkern_redis_dead_letter_total', this.labels, dead.length);
      this.log.warn(`Moved ${dead.length} entries from ${this.stream} to ${this.options.deadLetterStream} after more than ${this.options.maxDeliveries} deliveries`, { key: 'DLQ_MOVED' });
    }
    return live;
  }

  /** Free backpressure capacity (RDS-STR-8). */
  async capacity() {
    if (!this.manualAck) return this.options.count;
    const now = this.clock.now();
    if (this.options.backpressureScope === 'group') {
      if (now - this.lastCapacityCheckAt >= this.options.capacityCheckIntervalMs) {
        const summary = await this.shared.xpending(this.stream, this.group);
        this.groupPending = Number(summary?.[0]) || 0;
        this.lastCapacityCheckAt = now;
      }
      return this.options.maxPending - this.groupPending;
    }
    let pending = this.registry.count(this.stream, this.group, this.consumer);
    if (pending >= this.options.maxPending && now - this.lastCapacityCheckAt >= this.options.capacityCheckIntervalMs) {
      // Entries acknowledged outside stream-ack still occupy the registry;
      // reconcile with the real PEL before waiting for their TTL.
      this.lastCapacityCheckAt = now;
      const limit = this.options.maxPending;
      const rows = (await this.shared.xpending(this.stream, this.group, '-', '+', limit, this.consumer)) || [];
      // A full page may hide newer in-flight IDs behind older stale ones:
      // reconcile only up to the last ID the page covers.
      const upTo = rows.length >= limit ? String(rows[rows.length - 1][0]) : undefined;
      this.registry.retain(this.stream, this.group, this.consumer, rows.map((row) => String(row[0])), upTo);
      pending = this.registry.count(this.stream, this.group, this.consumer);
    }
    return this.options.maxPending - pending;
  }

  async waitReadInterval() {
    if (!this.options.readIntervalMs || !this.lastReadAt) return;
    const wait = this.options.readIntervalMs - (this.clock.now() - this.lastReadAt);
    if (wait > 0) await this.clock.sleep(wait, this.signal);
  }

  /** Token bucket per entry. Returns false when paused or closing. */
  async rateSlot() {
    const limit = this.options.rateLimitPerSecond;
    if (!limit || this.draining) return !this.stopped;
    while (!this.stopped) {
      const now = this.clock.now();
      this.rateTokens = Math.min(limit, this.rateTokens + ((now - this.rateRefillAt) * limit / 1000));
      this.rateRefillAt = now;
      if (this.rateTokens >= 1) {
        this.rateTokens -= 1;
        return true;
      }
      await this.clock.sleep(Math.max(1, Math.ceil((1 - this.rateTokens) * 1000 / limit)), this.signal);
    }
    return false;
  }

  /** @param {Array<{ id: string, payload: Record<string, unknown>, deliveries: number }>} entries */
  async deliver(entries) {
    for (let index = 0; index < entries.length; index += 1) {
      const entry = entries[index];
      if (!(await this.rateSlot())) {
        if (!this.signal.aborted) this.held.push(...entries.slice(index));
        return;
      }
      if (this.batching) {
        this.buffer.push(entry);
        if (this.buffer.length >= this.options.count) await this.flushBatch(false);
        else this.scheduleBatch();
      } else {
        await this.send([entry]);
      }
    }
  }

  scheduleBatch() {
    if (this.batchTimer || this.buffer.length === 0) return;
    this.batchTimer = this.clock.setTimeout(() => {
      this.batchTimer = null;
      this.flushBatch(false).catch((error) => {
        if (!this.signal?.aborted) this.log.error(error, { key: error?.code || 'BATCH_FLUSH_FAILED' });
      });
    }, this.options.batchWindowMs);
    this.batchTimer?.unref?.();
  }

  async flushBatch(force) {
    if (this.buffer.length === 0) return;
    if (!force && this.stopped) return;
    if (!force && this.options.batchIntervalMs && this.lastBatchAt) {
      const wait = this.options.batchIntervalMs - (this.clock.now() - this.lastBatchAt);
      if (wait > 0) {
        await this.clock.sleep(wait, this.signal).catch(() => {});
        if (this.stopped || this.buffer.length === 0) return;
      }
    }
    if (this.batchTimer) {
      this.clock.clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    const entries = this.buffer.splice(0, this.buffer.length);
    this.lastBatchAt = this.clock.now();
    await this.send(entries);
  }

  async send(entries) {
    if (entries.length === 0) return;
    if (this.manualAck) {
      for (const entry of entries) this.registry.add(this.stream, this.group, entry.id, this.consumer, this.options.reclaimIdleMs);
    }
    const redelivered = entries.filter((entry) => entry.deliveries > 1).length;
    if (redelivered) this.metrics.inc('redkern_redis_redeliveries_total', this.labels, redelivered);
    this.metrics.inc('redkern_redis_delivered_total', this.labels, entries.length);
    this.delivered += entries.length;
    this.emit({ entries, batched: this.batching });
    if (!this.manualAck) {
      await this.shared.xack(this.stream, this.group, ...entries.map((entry) => entry.id));
      this.metrics.inc('redkern_redis_acked_total', this.labels, entries.length);
    }
  }

  pause() {
    if (!this.paused) {
      this.paused = true;
      this.pausedAt = this.clock.now();
      if (this.buffer.length) {
        this.held.push(...this.buffer.splice(0));
        if (this.batchTimer) this.clock.clearTimeout(this.batchTimer);
        this.batchTimer = null;
      }
    }
    this.state();
    return this.status();
  }

  resume() {
    if (this.paused) {
      // Held entries may have been claimed by another consumer during a long
      // pause; re-read the own PEL instead of sending them blindly.
      if (this.held.length && this.clock.now() - this.pausedAt >= this.options.reclaimIdleMs / 2) {
        this.held = [];
        this.startRecovery();
      }
      this.paused = false;
      this.wake();
    }
    this.state();
    return this.status();
  }

  /**
   * Stops reading new entries, sends entries already reserved and waits for
   * an empty own PEL (RDS-CTL-4).
   * @param {number} timeoutMs
   */
  async drain(timeoutMs) {
    // Held back during a long pause, these entries may have been reclaimed by
    // another consumer: drop them, recovery claims back what is still own.
    if (this.paused && this.held.length && this.clock.now() - this.pausedAt >= this.options.reclaimIdleMs / 2) this.held = [];
    if (!this.paused) this.pausedAt = this.clock.now();
    this.paused = true;
    this.draining += 1;
    // Own PEL entries that are not in flight here (a lost reply, a dropped
    // flow) would keep the PEL non-empty forever: send them again now.
    this.startRecovery();
    this.wake();
    try {
      this.state();
      const deadline = this.clock.now() + Math.max(0, timeoutMs);
      await this.flushReserved();
      let pending = await this.ownPending();
      const busy = () => this.running && (this.readInFlight || this.recovering);
      while ((pending > 0 || busy()) && this.clock.now() < deadline && !this.signal?.aborted) {
        await this.clock.sleep(Math.min(this.options.drainCheckIntervalMs, Math.max(1, deadline - this.clock.now())), this.signal).catch(() => {});
        // A read that was blocked when drain started can still return entries.
        await this.flushReserved();
        pending = await this.ownPending();
      }
      return { ...this.status(), drained: pending === 0, pending };
    } finally {
      this.draining -= 1;
      this.state();
    }
  }

  /** Sends entries held back by pause or waiting in a batch. */
  async flushReserved() {
    if (this.batchTimer) this.clock.clearTimeout(this.batchTimer);
    this.batchTimer = null;
    const reserved = [...this.held.splice(0), ...this.buffer.splice(0)];
    const size = this.batching ? this.options.count : 1;
    for (let index = 0; index < reserved.length; index += size) await this.send(reserved.slice(index, index + size));
  }

  async ownPending() {
    const summary = await this.shared.xpending(this.stream, this.group);
    const consumers = Array.isArray(summary?.[3]) ? summary[3] : [];
    const own = consumers.find(([name]) => name === this.consumer);
    return own ? Number(own[1]) || 0 : 0;
  }

  status() {
    const snapshot = this.snapshot();
    return {
      stream: snapshot.stream,
      group: snapshot.group,
      consumer: snapshot.consumer,
      paused: snapshot.paused,
      inflight: snapshot.inflight,
      held: snapshot.held
    };
  }
}

function createBackoff({ initialBackoffMs, maxBackoffMs, backoffMultiplier }) {
  let attempt = 0;
  return {
    next() {
      const ceiling = Math.min(maxBackoffMs, initialBackoffMs * backoffMultiplier ** attempt);
      attempt += 1;
      // Equal jitter: at least half of the ceiling, so the whole HPA pool does
      // not reconnect in lockstep after a Redis failover.
      return Math.floor(ceiling / 2 + Math.random() * ceiling / 2);
    },
    reset() { attempt = 0; }
  };
}

module.exports = { StreamConsumer, DEFAULTS, parseEntries, isNoGroup, codeError };
