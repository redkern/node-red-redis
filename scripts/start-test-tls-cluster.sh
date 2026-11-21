#!/bin/sh
# Starts a 3-master Redis Cluster with TLS on 127.0.0.1:7201-7203 (plain
# ports disabled, TLS on the cluster bus too) for test/integration/cluster.test.js.
# Uses the CA made by scripts/start-test-tls.sh in the same directory.
# Usage: scripts/start-test-tls.sh DIR && scripts/start-test-tls-cluster.sh DIR
# Then: REDIS_CLUSTER_PORTS=7201,7202,7203 REDIS_CLUSTER_TLS=1 NODE_EXTRA_CA_CERTS=DIR/ca.crt npm run test:integration
set -e
DIR=${1:-${TMPDIR:-/tmp}/redkern-redis-tls}
for port in 7201 7202 7203; do
  redis-cli -p "$port" --tls --cacert "$DIR/ca.crt" shutdown nosave >/dev/null 2>&1 || true
  rm -rf "$DIR/cluster-$port" && mkdir -p "$DIR/cluster-$port"
  redis-server --port 0 --tls-port "$port" --bind 127.0.0.1 --tls-cluster yes --tls-replication yes \
    --tls-cert-file "$DIR/redis.crt" --tls-key-file "$DIR/redis.key" --tls-ca-cert-file "$DIR/ca.crt" --tls-auth-clients no \
    --cluster-enabled yes --cluster-config-file nodes.conf --dir "$DIR/cluster-$port" --save '' --appendonly no \
    --daemonize yes --cluster-node-timeout 2000
done
sleep 1
redis-cli --tls --cacert "$DIR/ca.crt" --cluster create 127.0.0.1:7201 127.0.0.1:7202 127.0.0.1:7203 --cluster-yes >/dev/null
sleep 2
redis-cli -p 7201 --tls --cacert "$DIR/ca.crt" cluster info | head -1
