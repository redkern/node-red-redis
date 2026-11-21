#!/bin/sh
# Starts Redis with TLS on 127.0.0.1:6391 and a throwaway CA for
# test/integration/tls.test.js. The server certificate is issued for
# redis.test and 127.0.0.1. Usage: scripts/start-test-tls.sh [dir]
# Then: REDIS_TLS_PORT=6391 NODE_EXTRA_CA_CERTS=<dir>/ca.crt npm run test:integration
set -e
DIR=${1:-${TMPDIR:-/tmp}/redkern-redis-tls}
mkdir -p "$DIR" && cd "$DIR"
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj "/CN=redkern-test-ca" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout redis.key -out redis.csr -subj "/CN=redis.test" 2>/dev/null
printf "subjectAltName=DNS:redis.test,IP:127.0.0.1\n" > ext.cnf
openssl x509 -req -in redis.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out redis.crt -days 2 -extfile ext.cnf 2>/dev/null
redis-server --port 0 --tls-port 6391 --bind 127.0.0.1 --tls-cert-file redis.crt --tls-key-file redis.key \
  --tls-ca-cert-file ca.crt --tls-auth-clients no --save '' --appendonly no --daemonize yes
echo "$DIR/ca.crt"
