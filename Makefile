.PHONY: install test test-integration test-all lint type-check clean \
        test-infra test-infra-down test-infra-logs \
        prod-infra prod-infra-down prod-infra-logs \
        start-test start-prod \
        migrate-test migrate-prod \
        sync-to-prod \
        up down start stop migrate services-up

# ── Python environment ────────────────────────────────────────────────────────

install:
	pip install -e ".[dev]"
	python3 -m spacy download en_core_web_sm

# ── Tests / lint / type-check ─────────────────────────────────────────────────

test:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

test-all:
	pytest -v

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

format:
	ruff format src/ tests/
	ruff check --fix src/ tests/

type-check:
	mypy src/

# ── Test environment infrastructure ──────────────────────────────────────────
# Postgres :5432 / Redis :6379

test-infra:
	docker compose -p social_trading_test -f docker-compose.test.yml up -d postgres redis
	@echo "Test infra started — postgres :5432, redis :6379"

test-infra-down:
	docker compose -p social_trading_test -f docker-compose.test.yml down
	@echo "Test infra stopped (volumes preserved)"

test-infra-logs:
	docker compose -p social_trading_test -f docker-compose.test.yml logs -f

# ── Production environment infrastructure ────────────────────────────────────
# Postgres :5433 / Redis :6380

prod-infra:
	docker compose -p social_trading_prod -f docker-compose.prod.yml up -d postgres redis
	@echo "Prod infra started — postgres :5433, redis :6380"

prod-infra-down:
	docker compose -p social_trading_prod -f docker-compose.prod.yml down
	@echo "Prod infra stopped (volumes preserved)"

prod-infra-logs:
	docker compose -p social_trading_prod -f docker-compose.prod.yml logs -f

# ── Service launchers (honcho shortcuts) ─────────────────────────────────────

# Start all services for test environment (IB paper account, UI :8501)
start-test:
	source .venv/bin/activate && honcho start -e .env.test

# Start all services for production environment (IB live account, UI :8502)
start-prod:
	source .venv/bin/activate && honcho start -e .env.prod

# ── Database migrations ───────────────────────────────────────────────────────

migrate-test:
	DB_NAME=trading_test DB_PORT=5432 .venv/bin/python migrations/migrate.py

migrate-prod:
	DB_NAME=trading_prod DB_PORT=5433 .venv/bin/python migrations/migrate.py

# ── Data sync: promote test reference data to prod ───────────────────────────

sync-to-prod:
	@./scripts/sync_to_prod.sh

# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true

# ── Stop app services (any env) ───────────────────────────────────────────────

stop:
	@./stop.sh

# ── Legacy targets (deprecated — kept for backwards compatibility) ────────────
# Use test-infra / prod-infra / start-test / start-prod / migrate-test instead.

up:
	@echo "⚠️  'make up' is deprecated. Use 'make test-infra' or 'make prod-infra'."
	docker compose up -d postgres redis

down:
	@echo "⚠️  'make down' is deprecated. Use 'make test-infra-down' or 'make prod-infra-down'."
	docker compose down

start:
	@echo "⚠️  'make start' is deprecated. Use 'make start-test' or 'make start-prod'."
	honcho start

migrate:
	@echo "⚠️  'make migrate' is deprecated. Use 'make migrate-test' or 'make migrate-prod'."
	.venv/bin/python migrations/migrate.py

services-up:
	@echo "⚠️  'make services-up' is deprecated — services now run on host via honcho."
	docker compose --profile services up -d
