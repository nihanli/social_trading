.PHONY: install test test-integration test-all lint type-check up down migrate services-up clean

install:
	pip install -e ".[dev]"
	python -m spacy download en_core_web_sm

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

migrate:
	python migrations/migrate.py

services-up:
	docker compose up -d

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
