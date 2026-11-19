# Getting started

This guide covers the current scope of the repository: the `sre_shared` package
and its local development dependencies. No agents run yet.

## Prerequisites

- Docker
- Python 3.12
- uv

## Setup

1. Create a local environment file from the template and set the Postgres
   password:

   ```bash
   cp .env.example .env
   ```

2. Install the shared package and its development dependencies, then activate
   the environment:

   ```bash
   cd shared
   uv sync
   cd ..
   source shared/.venv/bin/activate
   ```

3. Start Postgres and NATS:

   ```bash
   make dev
   ```

   Confirm the containers are running with `make status`, and stop them with
   `make down`.

4. Initialize NATS JetStream streams and the heartbeat KV bucket:

   ```bash
   make init
   ```

   This creates the `INCIDENTS`, `OPS`, `LEARNING`, and `AGENTS` streams plus
   the `agent_heartbeats` KV bucket. It is idempotent and safe to re-run.

5. Run the shared package tests:

   ```bash
   pytest -v
   ```

   `make test` runs the same command.

## Configuration

Settings are read from environment variables and the repository-root `.env`
file. The individual Postgres settings are combined into `POSTGRES_URL`
automatically, so that value does not need to be set manually.

See [Configuration](reference/configuration.md) for the variables currently read
by the application.

## Notes

`make init` must run after `make dev` and before starting any agent. It creates
the four configured JetStream streams and the heartbeat key-value bucket, and
is idempotent: re-running it updates the streams and leaves an existing KV
bucket as-is. No agents are implemented yet.
