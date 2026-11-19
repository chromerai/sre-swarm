# sre-swarm

A Kubernetes-native, self-healing incident response system built from a swarm
of six specialized AI agents. It watches a real cluster for anomalies,
diagnoses root causes with LLM reasoning, verifies every proposed fix against
the live API server before touching anything, gates risky actions on human
approval, executes, and records the outcome for future incidents.

Built as a solo learning project to go deep on Docker, distributed agent
systems, NATS JetStream, and production SRE patterns — not a tutorial clone.

---

## Status

**Phase 1 of 8 — Shared Package + Agent Skeletons (in progress)**

| Component | State |
|---|---|
| Infra (Postgres + NATS via Docker Compose) | ✅ Done |
| `sre_shared` package — messaging (NATS client, schema, subjects, streams) | ✅ Done, tested |
| `sre_shared` package — DB layer (7 SQLAlchemy models, Alembic migrations) | ✅ Done, tested |
| `sre_shared` package — `BaseAgent`, structured logging, settings | ✅ Implemented |
| `BaseAgent` test suite (`test_base_agent.py`) | 🚧 Drafted, not yet passing |
| Six agent skeletons (Observer, Diagnoser, Remediator, Safety, Orchestrator, Learner) | ⬜ Not started |
| End-to-end message flow through all 6 agents | ⬜ Not started |

No agent logic, Kubernetes integration, or LLM calls exist yet — that's
Phases 2–4. Right now this is the foundation layer: a tested, installable
Python package that every agent will import from, plus the messaging and
persistence infrastructure they'll run on.

Full phase-by-phase plan: [`roadmap.md`](./roadmap.md)
Full architectural reference: [`architecture.md`](./architecture.md), [`implementation.md`](./implementation.md)

---

## Why this exists

Most SRE-automation demos act on synthetic environments they built
themselves. This project targets infrastructure it doesn't control, and
structurally enforces a **verify-before-execute** guarantee — the incident
FSM's valid-transition map makes it impossible to reach `executing` without
a passed verification result. That's not a runtime check; it's baked into
the state machine itself.

The end target (Phase 5+) is chaos-testing this against a real Kubernetes
cluster running [Google Online Boutique](https://github.com/GoogleCloudPlatform/microservices-demo),
with measured MTTD/MTTR numbers, not a synthetic app built to be easy to fix.

---

## Architecture (target — most of this doesn't exist yet)

```
Observer → Diagnoser → Remediator → Safety → Orchestrator (owns the FSM)
                                                      ↓
                                                   Learner
```

Agents communicate exclusively over NATS JetStream — no shared memory, no
direct function calls. Incident state lives in Postgres. See
[`architecture.md`](./architecture.md) for the full file-by-file breakdown.

**Stack:** Python 3.12 · NATS JetStream · PostgreSQL 16 + SQLAlchemy/Alembic ·
structlog · Pydantic · Kubernetes Python client · OpenAI (gpt-4o-mini) ·
React/Vite dashboard (Phase 6+)

---

## Running it (current scope: shared package only)

```bash
# 1. Install the shared package
make install
source shared/.venv/bin/activate

# 2. Start Postgres + NATS
make dev

# 3. Create JetStream streams + KV buckets
make init

# 4. Run the shared package test suite
cd shared && python -m pytest tests/ -v
```

There is no agent to run yet — `make init` and the test suite above are the
current definition of "working."

---

## Repo layout

```
sre-swarm/
├── agents/          six agent directories (skeletons pending, Phase 1 Part C)
├── shared/          installable sre_shared package — messaging, db, config, base agent
├── dashboard/        FastAPI + React (Phase 6)
├── infra/            docker-compose (local dev), kind, Helm, Chaos Mesh (later phases)
├── scripts/          init_nats.py, fixtures, scenario runner
├── architecture.md   full structural reference
├── implementation.md design rationale and decisions log
└── roadmap.md        phased build plan with done-when gates
```

---

## Notes for reviewers

This repo is intentionally being built and documented phase-by-phase with a
running decision log (`SESSION_LOG.md`) rather than written after the fact —
if you're looking at the commit history you'll see infrastructure and tests
land before agent logic, on purpose. Phase 5 (working chaos scenarios against
a real cluster) is the milestone worth judging this project by; a completed
Phase 8 with a broken Phase 5 underneath it would be the wrong thing to
optimize for, so it isn't being rushed.