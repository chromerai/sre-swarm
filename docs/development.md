# Development

## Repository layout

```
agents/<name>/src/            agent packages (skeletons only)
shared/sre_shared/            installable shared package
shared/tests/                 unit tests for the shared package
infra/docker-compose.yml      local Postgres + NATS
scripts/init_nats.py          NATS setup script
docs/                         reader-facing documentation
```

The shared package is organized by concern:

- `agents/base.py` — `BaseAgent` lifecycle.
- `config/settings.py` — environment-driven settings.
- `db/` — models, async sessions, and Alembic migrations.
- `logging/` — structlog setup.
- `messaging/` — message envelope, NATS client, subjects, and stream
  definitions.

## Environment

The development environment lives inside `shared/`. From the repository root:

```bash
cd shared
uv sync
cd ..
source shared/.venv/bin/activate
```

## Local NATS initialization

After starting the local containers, initialize JetStream:

```bash
make dev
make init
```

`make init` creates the four configured streams (`INCIDENTS`, `OPS`, `LEARNING`,
`AGENTS`) and the `agent_heartbeats` KV bucket. It is idempotent: re-running it
updates the streams and leaves an existing KV bucket as-is.

## Tests

Run the test suite from the repository root with the shared environment active:

```bash
pytest -v
```

Test configuration is defined in `shared/pyproject.toml`. `make test` runs the
same suite.

## Linting

The shared package is configured for Ruff in `shared/pyproject.toml`:

```bash
cd shared
ruff check .
```

## Documentation policy

The code is the source of truth. Documentation describes behavior that exists
in the code and is updated as that behavior changes. Components that are not yet
implemented are listed by name only; their behavior is documented when they
land.
