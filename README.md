# sre-swarm

A Kubernetes-native, self-healing incident response system built from a swarm of
six specialized AI agents. The goal is to watch a real cluster for anomalies,
diagnose root causes, verify every proposed fix against the live API server
before touching anything, gate risky actions on human approval, execute, and
record the outcome for future incidents.

Built as a solo learning project focused on distributed agent systems, NATS
JetStream, PostgreSQL, and production SRE patterns.

---

## Status

**Active development — currently in Phase 1, building the shared foundation.
No agents run yet.**

### Implemented

| Component | State |
|---|---|
| Local development infrastructure (Postgres 16 + NATS JetStream) | Complete |
| NATS streams + `agent_heartbeats` KV bucket (`make init`) | Verified, idempotent |
| `sre_shared` messaging (message envelope, NATS client, subjects, streams) | Implemented, unit tested |
| `sre_shared` database layer (six models, async sessions, Alembic migrations) | Implemented, model-definition tests |
| `sre_shared` configuration and logging | Implemented, unit tested |
| `BaseAgent` shared lifecycle (connect, subscribe, publish, heartbeat, shutdown) | Implemented, unit tested |

### Not yet implemented

- The six agents
- End-to-end incident flow
- Kubernetes integration
- LLM-based diagnosis
- Dashboard
- Chaos scenarios

---

## Project direction

The intended agent flow is:

```
Observer -> Diagnoser -> Remediator -> Safety -> Orchestrator
                                                       |
                                                    Learner
```

Agents are intended to communicate over NATS, with incident state stored in
Postgres. The target design enforces verify-before-execute: an incident cannot
reach execution without a passed verification and an approval decision. This is
the design direction, not current behavior.

---

## Stack

**Used today**

- Python 3.12 and uv
- NATS JetStream (nats-py)
- PostgreSQL 16, SQLAlchemy 2 async, Alembic
- Pydantic v2 and pydantic-settings
- structlog

**Planned**

- Kubernetes Python client
- OpenAI
- FastAPI + React dashboard
- kind, Helm, Chaos Mesh

---

## Repository layout

```
agents/<name>/src/            agent packages (skeletons only)
shared/sre_shared/
  agents/base.py              BaseAgent lifecycle (implemented)
  config/settings.py          environment-driven settings (implemented)
  db/                         models, sessions, Alembic migrations (implemented)
  logging/                    structlog setup (implemented)
  messaging/                  message envelope, NATS client, subjects, streams (implemented)
shared/tests/                 unit tests for the shared package
infra/docker-compose.yml      local Postgres + NATS
scripts/init_nats.py          NATS setup script
docs/                         reader-facing documentation
```

---

## Getting started

Prerequisites: Docker, Python 3.12, and uv.

```bash
cp .env.example .env          # then set POSTGRES_PASSWORD
cd shared
uv sync
cd ..
source shared/.venv/bin/activate

make dev                      # start Postgres + NATS
make init                     # create JetStream streams + heartbeat KV bucket
pytest -v                     # run the shared package tests
```

`POSTGRES_URL` is assembled from the individual Postgres settings and does not
need to be set manually.

Useful companion targets: `make status` to inspect the containers, `make init`
to (re)apply the NATS stream and KV configuration, and `make down` to stop them.
`make init` is safe to run repeatedly.

---

## Testing

The shared package test suite covers messaging, configuration, model
definitions, and the `BaseAgent` lifecycle. Run it from the repository root with
the `shared/.venv` environment active:

```bash
pytest -v
```

`make test` runs the same suite.

---

## Documentation

- [Overview](docs/overview.md)
- [Getting started](docs/getting-started.md)
- [Development](docs/development.md)
- [Agent reference](docs/agents/README.md)
- [Configuration](docs/reference/configuration.md)

---

## About

A solo learning project built incrementally: infrastructure and tests land
before agent logic. The current focus is the shared foundation that every agent
will use.
