# Overview

`sre-swarm` is an incident response system designed to run against a real
Kubernetes cluster. The target workflow is: detect an anomaly, diagnose its root
cause, verify a proposed fix against the live API server, gate risky actions on
human approval, execute the fix, and record the outcome for future incidents.

The system is organized as six agents that communicate over NATS JetStream,
with PostgreSQL as the source of truth for incident state.

```
Observer ──INCIDENT_DETECTED──▶ Orchestrator
                                  ├─ DIAGNOSIS_REQUESTED ──▶ Diagnoser
                                  │      ◀─ DIAGNOSIS_COMPLETE
                                  ├─ REMEDIATION_REQUESTED ──▶ Remediator (plan + verify)
                                  │      ◀─ REMEDIATION_READY / VERIFICATION_PASSED
                                  ├─ SAFETY_REVIEW_REQUESTED ──▶ Safety
                                  │      ◀─ SAFETY_REVIEW_PASSED
                                  ├─ EXECUTION_REQUESTED ──▶ Remediator (execute)
                                  │      ◀─ EXECUTION_COMPLETE
                                  └─ resolved ──INCIDENT_RESOLVED──▶ Learner
```

The Orchestrator is the hub: it dispatches each command and receives every
result. Agents never hand off to each other directly. Diagnoser also queries
Learner during diagnosis via a `LEARNING_QUERY` request/reply, and Learner
consumes resolved incidents asynchronously.

## Current status

The project is in active development. Phase 1 — the shared foundation — is the
current focus.

Implemented today:

- Local development infrastructure: Postgres 16 and NATS JetStream via Docker
  Compose.
- NATS initialization via `make init`: four JetStream streams plus the
  `agent_heartbeats` KV bucket, verified idempotent.
- `sre_shared` messaging: the `AgentMessage` envelope, `NatsClient` operations
  (publish, subscribe, request, replay, key-value), subject constants, four
  stream definitions, and the heartbeat KV bucket.
- `sre_shared` database layer: six SQLAlchemy models, async session management,
  and Alembic migrations.
- `sre_shared` configuration and logging: environment-driven settings and
  structlog setup.
- `BaseAgent`: shared connection, subscription, publishing, heartbeat, and
  graceful shutdown lifecycle.

Not yet implemented:

- The six agents
- End-to-end incident flow
- Kubernetes integration
- LLM-based diagnosis
- Dashboard
- Chaos scenarios

## Component map

```
agents/<name>/src/            agent packages (skeletons only)
shared/sre_shared/agents/     BaseAgent lifecycle
shared/sre_shared/config/     environment-driven settings
shared/sre_shared/db/         models, sessions, Alembic migrations
shared/sre_shared/logging/    structlog setup
shared/sre_shared/messaging/  message envelope, NATS client, subjects, streams
infra/docker-compose.yml      local Postgres + NATS
scripts/init_nats.py          NATS setup script
```

## Design direction

The intended design keeps agent communication on NATS and incident state in
Postgres. Agents should not share memory or call each other directly. The
incident state machine is intended to make verification a structural
precondition for execution, rather than a runtime policy check.

These are design goals. Only the shared foundation described above exists today.
