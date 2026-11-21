# Security Policy

Report vulnerabilities through GitHub Private Vulnerability Reporting or privately to security@redkern.com. Include the affected version, impact, and a minimal reproduction. Do not open a public issue for an unpatched vulnerability.

Security-relevant defaults of this palette:

- `msg.command` is ignored unless "Allow msg.command" is on, and the command denylist applies to command and multi unless it is disabled explicitly per node.
- Control and status routes are registered only on the Node-RED admin API with `redkern.redis.read` / `redkern.redis.write` permissions and stay unavailable without `adminAuth`. The drain route on the internal port accepts loopback connections only; the metrics route needs `REDKERN_REDIS_METRICS_TOKEN`.
- Redis passwords are Node-RED credentials and are redacted from logs and error messages.

General support requests belong in GitHub issues or can be sent to support@redkern.com.
