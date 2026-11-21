#!/bin/sh
# Starts a 3-master Redis Cluster on 127.0.0.1:7001-7003 for
# test/integration/cluster.test.js. Usage: scripts/start-test-cluster.sh [dir]
set -e
DIR=${1:-${TMPDIR:-/tmp}/redkern-redis-cluster}
for port in 7001 7002 7003; do
  mkdir -p "$DIR/$port"
  redis-server --port "$port" --bind 127.0.0.1 --cluster-enabled yes --cluster-config-file nodes.conf \
    --dir "$DIR/$port" --save '' --appendonly no --daemonize yes --cluster-node-timeout 2000
done
sleep 1
redis-cli --cluster create 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 --cluster-yes
