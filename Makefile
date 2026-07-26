name: sre-swarm

SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

COMPOSE_FILE := infra/docker-compose.yml
COMPOSE_PROJECT_NAME := sre-swarm

COMPOSE_CMD := docker compose \
	-p $(COMPOSE_PROJECT_NAME) \
	-f $(COMPOSE_FILE)

.PHONY: dev up down clean status logs prune test init

dev: up
		@echo "✅ Postgres and NATS ready. Run 'make status' to verify."
#		@echo "Starting application process..."
#		@command -v mprocs >/dev/null 2>&1 || { \
			echo "ERROR: mprocs not installed. Run: brew install mprocs"; \
			exit 1; \
		}
#		@echo "Launching mprocs in 5 seconds ... ( Ctrl + C to cancel)"
#		@sleep 5
#		@trap '$(MAKE) down' EXIT INT TERM; \
		mprocs

up:	
	@echo "Starting Docker dependencies..."
	@$(COMPOSE_CMD) up -d --wait
	@echo "✅ All containers are ready and healthy!"

down:
	@echo "Shutting down Docker dependencies..."
	@$(COMPOSE_CMD) down

clean:
	@$(COMPOSE_CMD) down -v

status:
	@$(COMPOSE_CMD) ps

test: 
	@python -m pytest -v

logs: 
	@docker compose -f $(COMPOSE_FILE) logs -f

prune:
	@docker system prune -a --volumes

init:
	python3 scripts/init_nats.py
	
