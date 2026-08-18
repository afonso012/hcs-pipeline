# If it does not start in five minutes, nobody runs it — including you, three months from
# now (§15).
#
# Targets marked TODO call scripts that do not exist yet. They are listed so the shape of
# the remaining work stays visible, and each names the phase that brings it.

.DEFAULT_GOAL := help
.PHONY: help up down migrate smoke logs ps test lint format check build-context storage \
        data-bbbc039 data-wsi seed seed-dry spike bench chaos golden

# Compose v5 routes builds through buildx bake, which puts the build-context path into a
# gRPC header that rejects non-ASCII bytes. This repository lives under "Células", so
# every compose build fails here with
#   header key "x-docker-expose-session-sharedkey" contains value with non-printable ASCII
# while a plain `docker build` in the same directory succeeds. An ASCII symlink to the
# same tree is enough to satisfy it. Renaming the directory removes the need for this.
BUILD_CONTEXT := $(HOME)/.cache/hcs-pipeline/src

# Read only the one value make needs, rather than including .env wholesale.
MINIO_DATA_PATH := $(shell sed -n 's/^MINIO_DATA_PATH=//p' .env 2>/dev/null)
MINIO_DATA_PATH := $(if $(MINIO_DATA_PATH),$(MINIO_DATA_PATH),/Volumes/SSD/hcs-pipeline/minio)
BBBC021_ROOT := $(shell sed -n 's/^BBBC021_ROOT=//p' .env 2>/dev/null)
BBBC021_ROOT := $(if $(BBBC021_ROOT),$(BBBC021_ROOT),/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/BBBC021)
# The two plates Phase 2 develops against: 480 fields, 3.5 GB, one replicate each.
PLATES ?= Week1_22141,Week1_22161
WSI_ROOT := $(shell sed -n 's/^WSI_ROOT=//p' .env 2>/dev/null)
WSI_ROOT := $(if $(WSI_ROOT),$(WSI_ROOT),/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/OpenSlide)
COMPOSE := docker compose -p hcs -f $(BUILD_CONTEXT)/docker-compose.yml \
           --project-directory $(BUILD_CONTEXT)

# `mkdir -p /Volumes/SSD/...` on an unmounted SSD does not fail — it creates the path on
# the internal disk, blocks the real mount, and quietly fills the wrong drive with 80 GB
# of TIFFs. So the mount point has to exist first; only the leaf is created.
storage:
	@case "$(MINIO_DATA_PATH)" in \
	  /Volumes/*) test -d "$$(echo '$(MINIO_DATA_PATH)' | cut -d/ -f1-3)" \
	    || { echo "$(MINIO_DATA_PATH): the volume is not mounted"; exit 1; } ;; \
	esac
	@mkdir -p "$(MINIO_DATA_PATH)"

build-context:
	@mkdir -p $(dir $(BUILD_CONTEXT))
	@ln -sfn "$(CURDIR)" "$(BUILD_CONTEXT)"

help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- the stack ------------------------------------------------------------------------

# An image cannot contain its own digest — the digest is of the content, and the content
# would include the digest. So `up` reads it after the build and injects it at start-up.
# The worker's digest, not the API's: the worker is what produces the numbers a run
# records (§12).
up: build-context storage  ## Bring up the core profile and run migrations
	@test -f .env || cp .env.example .env
	$(COMPOSE) --profile core build
	IMAGE_DIGEST="$$(docker image inspect hcs-worker --format '{{.Id}}')" \
		$(COMPOSE) --profile core up -d
	$(COMPOSE) run --rm --no-deps api alembic upgrade head

down: build-context  ## Stop everything and delete the volumes
	$(COMPOSE) --profile full down -v

migrate: build-context  ## Run migrations against the running stack
	$(COMPOSE) run --rm --no-deps api alembic upgrade head

smoke: build-context  ## Prove the stack works, not just that containers started
	@echo "--> API through Traefik"
	@curl -fsS http://localhost:$${HTTP_PORT:-8080}/healthz && echo
	@curl -fsS http://localhost:$${HTTP_PORT:-8080}/readyz && echo
	@echo "--> workers answer over the broker"
	@$(COMPOSE) exec -T worker celery -A hcs.worker.celery_app inspect ping
	@echo "--> a task is published and consumed"
	@$(COMPOSE) exec -T worker python -c \
		"from hcs.worker.celery_app import ping; ping.delay(); print('published')"
	@sleep 2
	@test "$$($(COMPOSE) exec -T redis redis-cli llen celery | tr -d '\r')" = "0" \
		&& echo "queue drained" \
		|| { echo "queue did not drain"; exit 1; }

logs: build-context  ## Follow logs from every running service
	$(COMPOSE) logs -f --tail=50

ps: build-context  ## Show services and their state
	$(COMPOSE) ps

# --- development ------------------------------------------------------------------------

test:  ## Run the whole suite (integration tests start their own containers)
	uv run pytest -q

lint:  ## Lint and type-check
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy hcs migrations

format:  ## Apply the formatter
	uv run ruff format .
	uv run ruff check --fix .

check: lint test  ## Everything CI runs

# --- data (Phase 2) -----------------------------------------------------------------------

data-bbbc039:  ## TODO Phase 2: fetch the ground truth masks missing from the SSD
	uv run python scripts/fetch_bbbc039_masks.py

data-wsi: ## Fetch the OpenSlide test slides (757 MB; resumable)
	uv run python scripts/fetch_openslide_slides.py --dest "$(WSI_ROOT)"

seed: ## Upload and register the development plates (PLATES=... BATCH_ID=... to override)
	uv run python scripts/seed_bbbc021.py --root "$(BBBC021_ROOT)" --plates "$(PLATES)" \
		--external-ref bbbc021-dev $(if $(BATCH_ID),--batch-id $(BATCH_ID))

seed-dry: ## Same, but check the CSV and the files on disk without uploading anything
	uv run python scripts/seed_bbbc021.py --root "$(BBBC021_ROOT)" --plates "$(PLATES)" --dry-run

# --- measurement (Phases 2, 4, 5) -----------------------------------------------------------

spike: ## Objective B: RSS against chunk and concurrency, bytes/row (§21.3)
	uv run python scripts/spike_memory.py --out docs/spike-memory.md

chaos:  ## TODO Phase 4: kill workers mid-flight and check the invariants
	uv run python scripts/chaos_test.py --units 500 --kill 2

bench:  ## TODO Phase 5: batching matrix, results to MLflow
	uv run python scripts/bench_batching.py --workers 1,4 --backends none,manual,ray

golden:  ## TODO Phase 2: reprocess the golden batch and compare against the reference
	uv run pytest tests/golden -v
