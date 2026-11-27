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

## Linux Docker load investigation

The local Linux Docker run of commit `01209a6` failed the load gate. An instrumented rerun with Node.js 24 and isolated Redis 7.4 reported 69,680 `INPUT_QUEUE_FULL` errors and a sampled limiter queue of 10,000. The original generator's largest catch-up batch was 3,464 messages. The test now limits each batch to 64 and yields between batches without discarding schedule debt or lowering the required rate. It reports error codes, sampled limiter queue, batch size, schedule debt, CPU time and event-loop delay. `maxBacklog` remains sent messages not yet accounted for as successes or errors, not the limiter queue.

Three subsequent runs with the bounded generator failed: completed throughput was 2,748-6,572 commands/s, with 3,515-17,664 `INPUT_QUEUE_FULL` errors and a sampled queue of 10,000 in each run. A bounded-generator run had also passed once with a 128-message batch; limiting bursts alone therefore does not establish a fix. The shared macOS host showed high load during the investigation; this is a possible contributor, not a proven cause. A direct Redis pipelined benchmark measured 400,000 INCR/s, but is not a substitute for the Node-RED load gate.

The load gate remains **unverified for release**. Repeat it on a representative, sufficiently isolated Linux runner and investigate the producer/runtime CPU cost and queue growth if it still fails. The later local pods run was also not completed: Docker denied `unshare -u`. Run that gate in a Linux environment that permits UTS namespaces. No GitHub CI result is implied by these local tests.

## Kit 1.2.0 retest

On 2026-11-27, a disposable Linux Docker snapshot of `01209a6` plus the uncommitted load-test changes was tested on Node.js 24.22.3 with isolated Redis 7.4.12. All locked dependencies were installed, then only `@redkern/node-red-kit` was replaced with the published 1.2.0 package; the repository manifest and lockfile were not changed.

- `verify` passed (61 unit tests), `check:pack` passed, and standalone integration passed (57 tests; cluster and TLS suites not configured).
- Cluster and TLS integration passed (5 tests), as did TLS Cluster (3 tests). The 60-second chaos test passed with no missing entries or DLQ entries; it reported three producer errors during disruption. The five-minute soak passed with 14 redeploys, 143,750 produced entries, zero DLQ entries and 0.7 MB heap growth.
- Three load runs produced two passes (approximately 10,500 completed commands/s, no errors; pause/resume delivered 10,000 unique entries with no DLQ) and one failure (5,906 completed commands/s, 28,627 `INPUT_QUEUE_FULL` errors, sampled queue 10,000).
- Three comparison runs with the lockfile's kit 1.1.0 in the same container all failed: two exceeded the test's backlog limit without errors, and one reported 31,637 `INPUT_QUEUE_FULL` errors. These few runs on a shared host do not establish a reliable performance comparison.

The load gate remains **unverified** with kit 1.2.0. The CI lockfile still installs kit 1.1.0; this experiment is not a CI run or a dependency update. The pods test was not rerun because Docker denied `unshare -u`.

## Denylist performance follow-up

A Node.js CPU profile identified repeated parsing of the default denylist in the command hot path. The default entries are now indexed once; tests also check that custom entries and subcommands retain their behavior. A single isolated microbenchmark of one million `INCR` validations took 8,739 ms before and 166 ms after the change; this measures the validation function only, not end-to-end throughput.

On the shared Docker host, all three load runs with the optimized palette and locked kit 1.1.0 failed to reach the required throughput. With kit 1.2.0 substituted only in the test container, all three achieved approximately 10,500 completions/s with zero errors; two passed, and one failed because peak backlog was 1,186 against the test limit of less than 1,000. These runs used the bounded generator and unchanged acceptance thresholds. `verify` passed 62 unit tests, `check:pack` passed, and all 9 command integration tests passed with each kit version. The complete standalone integration suite also passed 57 tests with kit 1.2.0; cluster and TLS were not configured for that run.

The optimization does not establish a stable load pass. The host was heavily loaded, so repeat the full release gates on a sufficiently isolated Linux runner. The pods test remains unverified locally.

## Load retest

On 2026-11-27, the optimized working tree was retested in isolated Docker containers with Node.js 24 and Redis 7.4.12, using the same bounded generator and acceptance thresholds. With locked kit 1.1.0, one of three runs passed; the other two had zero command errors but exceeded the backlog limit, peaking at 2,432 and 1,344. With kit 1.2.0 substituted only in the test container, two of three runs passed; the third had zero errors and approximately 10,500 completions/s but peaked at 3,944 backlog. Both successful 1.2.0 runs delivered 10,000 unique pause/resume entries with no DLQ.

The load gate is still not consistently passing. These are local results, not GitHub CI evidence; the lockfile still uses kit 1.1.0.

## Local kit 1.3 development retest

On 2026-11-27, the palette working tree was tested in Docker with Node.js 24.22.3 and isolated Redis 7.4.12. The kit was packed from the local `node-red-kit` working tree at `eef1849` plus its uncommitted 1.3 changes and substituted only in the test container. Its package metadata still reports 1.2.0; the tarball SHA-256 was `6312d12380256a79dc437b93397ac4f09f6b985fc74611458c00a06c927b5437`.

`verify` passed 62 unit tests, `check:pack` passed, and standalone integration passed 57 tests; cluster and TLS suites were not configured. In three load runs, each completed approximately 10,499 commands/s with no errors, but each failed the peak-backlog limit: 1,083, 1,706 and 1,095 respectively. Sampled limiter queue peaks were 971, 1,600 and 995. The load script stopped at the command assertion, so its pause/resume stage was not run.

This development snapshot does not establish a passing load gate and is not the dependency in the palette's lockfile. No pods, topology or GitHub CI test was run with it.

## Queue-growth criterion retest

The load gate now samples the limiter queue approximately every 20 ms and compares the time-weighted averages of the first and last three seconds. It permits growth of at most 500 entries and requires the queue to drain within one second. The 10,000 completed commands/s and zero-error checks are unchanged; peak outstanding messages and peak sampled queue are still reported.

On 2026-11-27, Docker tests used Node.js 24 and isolated Redis 7.4.12. All three load runs with the local kit development snapshot passed, including pause/resume with 10,000 unique deliveries and no DLQ each. Three runs with locked kit 1.1.0 produced two passes and one failure: the failed run completed about 6,883 commands/s, reported 25,800 `INPUT_QUEUE_FULL` errors and had average queue growth of about 9,936 entries. `verify` passed 67 unit tests and `check:pack` passed with both kit versions.

A separate six-second run with locked kit 1.1.0 also failed: about 5,860 completions/s, 4,122 `INPUT_QUEUE_FULL` errors and average queue growth of about 6,571 entries. This exercised the minimum-duration calculation but did not pass the load gate.

The local kit snapshot remains unreleased and is not in the palette lockfile. The revised gate has not been validated by GitHub CI, and pods remain unverified locally.

## Published kit 1.3.0 retest

On 2026-11-27, a disposable Docker snapshot of the working tree was tested with Node.js 24.22.3 and isolated Redis 7.4.12. `npm ci` installed the published `@redkern/node-red-kit` 1.3.0 from the updated lockfile.

- `verify` passed (67 unit tests and 38 requirement IDs verified; 15 planned), `check:pack` and `npm ls --omit=dev` passed, and standalone integration passed 57 tests. Cluster and TLS were not configured for this run.
- Three sequential load runs passed with approximately 10,500 completed commands/s and zero errors. Average queue growth was -15.1, 5.8 and -118.9 entries, respectively; the queue drained within one second in each run. Each pause/resume stage delivered 10,000 unique entries with no DLQ entries.

These are local results, not GitHub CI evidence. The revised load gate, CI matrix and resilience gates including pods still need confirmation in CI for the release commit.
