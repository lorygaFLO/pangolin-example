# =====================================================================
# Pangolin Makefile (engine: podman)
#
# Run from a POSIX shell (Git Bash, WSL, macOS, Linux). On bare PowerShell
# use:  podman compose --env-file .env.docker up -d
# =====================================================================

SHELL := /bin/sh

GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
GIT_SHA    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
COMPOSE    ?= podman compose --env-file docker/.env.docker

export GIT_BRANCH
export GIT_SHA

.PHONY: help build up down logs ps restart bootstrap shell clean nuke

help:
	@echo "Targets:"
	@echo "  build      Build the image, baking in GIT_BRANCH=$(GIT_BRANCH) GIT_SHA=$(GIT_SHA)"
	@echo "  up         Start the full stack (server + bootstrap + worker + caddy)"
	@echo "  down       Stop the stack (keep volumes)"
	@echo "  restart    Restart worker only"
	@echo "  logs       Tail logs from all services"
	@echo "  ps         Show service status"
	@echo "  bootstrap  Re-run the bootstrap one-shot (idempotent)"
	@echo "  shell      Open a shell in the worker container"
	@echo "  clean      Stop stack and drop named volumes (WIPES Prefect DB)"
	@echo ""
	@echo "Build a specific branch:"
	@echo "    git checkout <branch> && make build"

build:
	$(COMPOSE) build \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg GIT_SHA=$(GIT_SHA)

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart worker

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

bootstrap:
	$(COMPOSE) run --rm bootstrap

shell:
	$(COMPOSE) exec worker /bin/bash

clean:
	$(COMPOSE) down -v

nuke: clean
	podman image rm pangolin:$(GIT_BRANCH) 2>/dev/null || true
