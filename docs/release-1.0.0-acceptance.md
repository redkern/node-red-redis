# 1.0.0 acceptance results

Run 2026-11-27 on the working tree of `b12dcb3` plus the uncommitted spec/CI changes (no runtime code changed). Not a GitHub CI run: Claude cloud sandbox, Linux 6.18, Node.js 22.23.5, npm 10.9.9, Node-RED from the lockfile, Redis 7.0.15 (standalone on 6390, cluster 3+3 on 7101-7106).

| Gate | Command | Result |
| --- | --- | --- |
| verify | `npm run verify` | pass: lint, editor, 38 requirement IDs (15 planned for 1.1.0-1.3.0), 55/55 unit tests; also passes on the workstation with Node.js 22.23.5 |
| pack | `npm run check:pack` | pass: 49 files, 61009 bytes |
| integration | `npm run test:integration` | pass: 57/57 |
| load | `npm run test:load` | pass: command 9999 msg/s, 0 failed, max backlog 208; pause/resume 10000 delivered, 10000 unique, 0 DLQ |
| chaos | `start-chaos-cluster.sh && npm run test:chaos -- --seconds 60` | pass: 3 streams, 0 duplicates, 0 missing, 0 DLQ |
| pods | `npm run test:pods -- --seconds 60` (root) | pass: 12040 produced, 12040 processed, 0 missing, 0 duplicates, 0 DLQ, SIGTERM exit 0 |
| soak | `npm run test:soak -- --minutes 15` | pass: 29 redeploys, 448950 produced, 0 DLQ, heap growth 2 MB, 4 connections, 6 handles, no warnings |
| node-red-dev | `node-red-dev@0.1.6 validate -p .` | not complete here: the sandbox network blocks `catalogue.nodered.org`, which P08 and N01 need. With a test-only stub for an empty catalogue the run completed: all checks passed except N02 (no example for stream-claim, now fixed) and D03 (ioredis pinned at 6.0.0, latest 6.1.0, recorded deviation). P08 and N01 still need the real catalogue: the CI job is the evidence. `scripts/check-scorecard.js` rejects a partial scorecard |

Notes:

- The soak log has 5 `[redkern-redis-stream-ack:ack] Error: Node is closing` lines during redeploys and no other errors; the run still ended with 0 DLQ and passed its checks. Worth a look before the tag: ack during close is expected to fail, but it fills the log.
- Not covered here: Node.js 24, Node-RED 4.1, Redis 6.2/8.0 and Valkey, TLS and TLS cluster. The CI matrix runs them.

## Follow-up verification

On 2026-11-27, the working tree was checked on macOS with Node.js 26.13.1 and a dedicated local Redis 8.10.1:

- `npm run verify`: 61 unit tests passed; `npm run check:pack` passed.
- `npm run test:integration`: 57 tests passed against standalone Redis; cluster and TLS suites were skipped because their test servers were not configured.
- `node-red-dev@0.1.6 validate -p .`: all 12 checks completed against the real catalogue; D03 was accepted for the documented ioredis deviation. The release commit still needs the CI run.
- `npm run test:load`: after tightening the throughput check to count successful completions during the load without rounding, 104,998 commands completed in 10.0000825 seconds (10,499.7/s), with zero failures and a maximum backlog of 635. Pause/resume delivered 10,000 unique entries with no DLQ entries. The earlier 9,999/s result above was from the less strict test and does not establish the 10,000/s threshold.
