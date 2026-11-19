# Configuration

Configuration is provided through environment variables and, optionally, a
`.env` file at the repository root. Variable names are matched
case-insensitively, and unrecognized variables are ignored.

Settings are loaded once per process through the cached `get_settings()` helper
in `sre_shared.config.settings`.

## Variables

| Variable | Default | Constraint |
|---|---|---|
| `ENVIRONMENT` | `development` | `development`, `production`, or `test` |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` |
| `POSTGRES_USER` | `sre_swarm` | |
| `POSTGRES_PASSWORD` | `simplepassword` | |
| `POSTGRES_DB` | `sre_agent` | |
| `POSTGRES_HOST` | `localhost` | |
| `POSTGRES_PORT` | `5432` | 1 to 65535 |
| `POSTGRES_POOL_SIZE` | `10` | At least 1 |
| `POSTGRES_MAX_OVERFLOW` | `20` | At least 0 |
| `NATS_URL` | `nats://localhost:4222` | |
| `NATS_MAX_RECONNECT_ATTEMPTS` | `10` | At least 0 |
| `NATS_RECONNECT_TIME_WAIT` | `2.0` | Greater than 0 |
| `NATS_ACK_WAIT_SECONDS` | `30` | Greater than 10 |
| `NATS_MAX_DELIVER` | `5` | At least 1 |
| `AGENT_HEARTBEAT_INTERVAL_SECONDS` | `30` | Greater than 0 |
| `AGENT_HEARTBEAT_TIMEOUT_SECONDS` | `90` | Greater than 0 |

## Derived values

`POSTGRES_URL` is computed from `POSTGRES_USER`, `POSTGRES_PASSWORD`,
`POSTGRES_DB`, `POSTGRES_HOST`, and `POSTGRES_PORT`. Setting `POSTGRES_URL`
directly has no effect.

## Additional environment keys

`.env.example` is a broad template. It contains keys for components that are
not implemented yet (those are currently ignored), and a few values that differ
from the code defaults above:

- `POSTGRES_URL` is ignored because the URL is computed.
- `POSTGRES_DB=sre_swarm` overrides the `sre_agent` default if copied.
- `AGENT_HEARTBEAT_TIMEOUT_SECONDS=30` overrides the `90` default if copied.

Prefer the variables and defaults in the table above when exact values matter.
