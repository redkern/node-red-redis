#!/bin/sh
# Starts a Redis Cluster of 3 masters with one replica each on
# 127.0.0.1:7101-7106 for scripts/chaos-test.js. The chaos test kills one
# master, so start a fresh cluster before every run.
# Usage: scripts/start-chaos-cluster.sh [dir]
set -e
DIR=${1:-${TMPDIR:-/tmp}/redkern-redis-chaos}
for port in 7101 7102 7103 7104 7105 7106; do
  redis-cli -p "$port" shutdown nosave >/dev/null 2>&1 || true
done
rm -rf "$DIR"
for port in 7101 7102 7103 7104 7105 7106; do
  mkdir -p "$DIR/$port"
  redis-server --port "$port" --bind 127.0.0.1 --cluster-enabled yes --cluster-config-file nodes.conf \
    --dir "$DIR/$port" --save '' --appendonly no --daemonize yes --cluster-node-timeout 2000
done
sleep 1
redis-cli --cluster create 127.0.0.1:7101 127.0.0.1:7102 127.0.0.1:7103 127.0.0.1:7104 127.0.0.1:7105 127.0.0.1:7106 \
  --cluster-replicas 1 --cluster-yes >/dev/null
sleep 3
redis-cli -p 7101 cluster info | head -1
