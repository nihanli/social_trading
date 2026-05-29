.PHONY: install test test-integration test-all lint type-check up down start migrate services-up clean

install:
	pip install -e ".[dev]"
	python3 -m spacy download en_core_web_sm

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

up:
	docker compose up -d postgres redis

down:
	docker compose down

# Start all app services in one terminal (requires: make up first)
start:
	honcho start

migrate:
	.venv/bin/python migrations/migrate.py

# Start all app services inside Docker (production / server deployment)
# For local development, run services directly: see docs/live-run-guide.md
services-up:
	docker compose --profile services up -d

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
