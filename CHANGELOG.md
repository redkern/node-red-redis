# Changelog

## 1.0.0 - 2026-11-21

First production release of the palette that replaces `@yroshcha/node-red-contrib-redis-full`. Covers the full scope of the spec (alpha.1 to rc.1); the acceptance criteria of section 9, including the load tests, pass on Node.js 22 and 24 with Node-RED 4.1 and 5.

- Config node on `@redkern/node-red-kit` `createRedisClient` with shared, blocking and subscriber roles, no offline queue, `${VAR}` fields and password credentials (RDS-CON-1..6).
- command with `allowMsgCommand` off by default, a default denylist and rejection of blocking commands; multi, scan with limit, timeout and all cluster masters; Pub/Sub-only subscribe; transitional list-in with prefetch (RDS-CMD-1..8).
- stream-in with cursor-based PEL recovery, a palette-level in-flight registry, one range `XPENDING` per batch, atomic DLQ moves, per-consumer or group backpressure, `XAUTOCLAIM` with a persistent cursor, batching, rate limit and start paused (RDS-STR-1..11).
- stream-out, stream-ack with batch ACK and legacy fields, stream-claim, stream-gc with an atomic empty-PEL check, stream-metrics with a palette metrics registry (RDS-STR-12..15).
- pause/resume/drain/status through stream-control, the admin API with permissions and a loopback drain route on port 9551 for preStop (RDS-CTL-1..5).
- Editor look per the kit standard: coral colour, disc-stack motif with an action sign per node, message catalogs, help, tabs for config and stream-in (RDS-UI-1).
- Manual migration guide from `@yroshcha/node-red-contrib-redis-full` in the README: type mapping and what to set by hand (RDS-MIG-1).
- One node set `redkern-redis` with a single palette closure per kit KIT-ARCH-2; editor sources in `editor/`, generated `nodes/redkern-redis.html` and one message catalog.
- Settings read as `redkernRedisConfig<Key>` (KIT-CFG-6); admin errors through kit `PublicError`; a wrong password shows a red auth status instead of endless retries.
- Recovery walks the own PEL with XPENDING and claims back only entries not in flight with XCLAIM, so redeploys no longer raise delivery counts and cannot send an in-flight entry to the DLQ (D1); an entry acknowledged during recovery is not sent again.
- drain sends entries returned by a read that was blocked when drain started and recovers the own PEL before waiting; admin drain no longer times out after 10 s.
- The denylist also covers SWAPDB, FAILOVER, SAVE, FUNCTION FLUSH, CLIENT KILL/PAUSE and CLUSTER admin subcommands; scripts are rejected from msg.command and msg.commands; commands that change the shared connection (CLIENT REPLY, HELLO, AUTH, READONLY) are always rejected.
- list in returns an element popped while the node closes to the list; backpressure reconcile keeps newer in-flight IDs; NOPERM and WRONGTYPE give a red status.
- Built and tested on `@redkern/node-red-kit` 1.1.0. Its internal listener is loopback-only or token-only (KIT-HTTP-6): with `REDKERN_REDIS_METRICS_TOKEN` the drain route now needs `REDKERN_REDIS_DRAIN_TOKEN`, otherwise it is off with a warning. Admin routes answer 503 `ADMIN_AUTH_REQUIRED` without `adminAuth`.
- TLS always verifies the server certificate; a rejected certificate gives a red auth status.
- Lint for forbidden patterns, requirement ID check against `docs/spec.md`, network fault tests through a TCP proxy, cluster, TLS, TLS cluster, fuzz and ACL tests, acceptance load tests, a cluster chaos test (failover, master kill, slot migration), a multi-pod test (SIGKILL, preStop drain, SIGTERM) and a soak test with redeploys.
