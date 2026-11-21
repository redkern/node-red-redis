'use strict';

/**
 * BLPOP/BRPOP intake for list queues (RDS-CMD-7). Popping removes the element
 * from the list, exactly like the legacy subscribe node in list mode; there is
 * no reliable BLMOVE mode, delivery guarantees come from Streams.
 *
 * prefetch limits how many popped elements may be in the flow at once:
 * 1 (default for new nodes) pops the next element only after the previous
 * message completed; 0 means unlimited, which is the legacy behaviour.
 */
class ListConsumer {
  /**
   * @param {{
   *   client: any, key: string, direction: 'BLPOP'|'BRPOP', timeoutSec: number, prefetch: number,
   *   releaseTimeoutMs: number,
   *   emit: (key: string, value: string, msgId: string) => void,
   *   newId: () => string,
   *   wired: () => boolean,
   *   tracker?: { track: (id: string, done: () => void, timeoutMs: number) => () => void },
   *   sleep: (ms: number, signal?: AbortSignal) => Promise<void>,
   *   backoff: { next: () => number, reset: () => void },
   *   log?: { error: Function },
   *   onState?: (state: object) => void
   * }} params
   */
  constructor(params) {
    Object.assign(this, params);
    this.inflight = 0;
    this.received = 0;
    this.lastError = null;
    this.wakeResolve = null;
    this.cancels = new Set();
    this.log = params.log || { error() {} };
    this.onState = params.onState || (() => {});
  }

  wake() {
    const resolve = this.wakeResolve;
    this.wakeResolve = null;
    resolve?.();
  }

  async waitSlot(signal) {
    while (!signal.aborted && this.prefetch > 0 && this.inflight >= this.prefetch) {
      const woken = new Promise((resolve) => { this.wakeResolve = resolve; });
      await Promise.race([this.sleep(1000, signal).catch(() => {}), woken]);
    }
  }

  release() {
    this.inflight = Math.max(0, this.inflight - 1);
    this.onState(this.snapshot());
    this.wake();
  }

  snapshot() {
    return { inflight: this.inflight, prefetch: this.prefetch, received: this.received, lastError: this.lastError };
  }

  async pushBack(key, value) {
    try {
      if (this.direction === 'BRPOP') await this.client.rpush(key, value);
      else await this.client.lpush(key, value);
    } catch (error) {
      this.log.error(Object.assign(new Error(`Element popped from ${key} during close could not be returned to the list`), { code: 'LIST_PUSHBACK_FAILED', cause: error }), { key: 'LIST_PUSHBACK_FAILED' });
    }
  }

  /** @param {AbortSignal} signal */
  async run(signal) {
    const onAbort = () => this.wake();
    signal.addEventListener('abort', onAbort, { once: true });
    try {
      while (!signal.aborted) {
        await this.waitSlot(signal);
        if (signal.aborted) break;
        try {
          const method = this.direction === 'BRPOP' ? 'brpop' : 'blpop';
          const reply = await this.client[method](this.key, this.timeoutSec);
          this.backoff.reset();
          if (this.lastError) {
            this.lastError = null;
            this.onState(this.snapshot());
          }
          if (!reply) continue;
          const [key, value] = reply;
          if (signal.aborted) {
            // Popped while the node was closing: put it back where it came from.
            await this.pushBack(String(key), value);
            break;
          }
          this.received += 1;
          const msgId = this.newId();
          // Track before sending: runtime hooks may fire during send().
          if (this.prefetch > 0 && this.tracker && this.wired()) {
            this.inflight += 1;
            let released = false;
            const cancel = this.tracker.track(msgId, () => {
              if (released) return;
              released = true;
              this.cancels.delete(cancel);
              this.release();
            }, this.releaseTimeoutMs);
            this.cancels.add(cancel);
          }
          this.emit(String(key), value, msgId);
          this.onState(this.snapshot());
        } catch (error) {
          if (signal.aborted) break;
          this.lastError = error?.code || error?.name || 'ERROR';
          this.onState(this.snapshot());
          this.log.error(error, { key: error?.code || 'LIST_POP_FAILED' });
          await this.sleep(this.backoff.next(), signal).catch(() => {});
        }
      }
    } finally {
      signal.removeEventListener('abort', onAbort);
      for (const cancel of this.cancels) cancel();
      this.cancels.clear();
    }
  }
}

module.exports = { ListConsumer };
