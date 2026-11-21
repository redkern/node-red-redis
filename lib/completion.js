'use strict';

const HOOK_ID = 'redkern-redis-completion';

/**
 * Tracks when a message sent by a source node has been fully handled by the
 * flow, using the public Node-RED runtime hooks: every preDeliver of the
 * message's _msgid adds one pending delivery and every onComplete removes one.
 * When the count returns to zero, the message is done. A timeout releases the
 * slot for flows whose nodes never complete (RDS-CMD-7, list-in prefetch).
 *
 * Hooks are installed only while at least one message is tracked, so a
 * runtime without list-in pays nothing.
 */
function createCompletionTracker(RED, { setTimer = setTimeout, clearTimer = clearTimeout } = {}) {
  /** @type {Map<string, { count: number, seen: boolean, done: () => void, timer: any }>} */
  const tracked = new Map();
  let installed = false;

  function install() {
    if (installed || !RED.hooks || typeof RED.hooks.add !== 'function') return;
    installed = true;
    RED.hooks.add(`preDeliver.${HOOK_ID}`, (event) => {
      const entry = tracked.get(event?.msg?._msgid);
      if (entry) {
        entry.count += 1;
        entry.seen = true;
      }
    });
    RED.hooks.add(`onComplete.${HOOK_ID}`, (event) => {
      const id = event?.msg?._msgid;
      const entry = tracked.get(id);
      if (!entry) return;
      entry.count -= 1;
      if (entry.seen && entry.count <= 0) finish(id);
    });
  }

  function uninstall() {
    if (!installed || tracked.size > 0) return;
    installed = false;
    RED.hooks.remove(`*.${HOOK_ID}`);
  }

  function finish(id) {
    const entry = tracked.get(id);
    if (!entry) return;
    tracked.delete(id);
    clearTimer(entry.timer);
    uninstall();
    entry.done();
  }

  return {
    /**
     * @param {string} msgId
     * @param {() => void} done called once
     * @param {number} timeoutMs
     * @returns {() => void} cancel
     */
    track(msgId, done, timeoutMs) {
      install();
      const timer = setTimer(() => finish(msgId), timeoutMs);
      timer?.unref?.();
      tracked.set(msgId, { count: 0, seen: false, done, timer });
      return () => {
        const entry = tracked.get(msgId);
        if (!entry) return;
        tracked.delete(msgId);
        clearTimer(entry.timer);
        uninstall();
      };
    },
    get size() { return tracked.size; },
    get installed() { return installed; }
  };
}

module.exports = { createCompletionTracker, HOOK_ID };
