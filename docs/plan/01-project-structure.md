# 01 — Project Structure

## Guiding Principles

| Principle | Implementation |
|-----------|---------------|
| **Src layout** | All source under `src/social_trading/` — prevents accidental imports from project root |
| **Separation of concerns** | Each domain is a sub-package with no cross-domain imports except through `core/` |
| **Dependency inversion** | High-level modules (signal, risk, execution) depend on `core/protocols.py` abstractions, never on concrete implementations |
| **Pluggable data sources** | `ingest/` uses a `DataSource` protocol + `DataSourceRegistry` — adding a source is one file + one registration line |
| **Testability** | Pure functions wherever possible; stateful classes accept injected dependencies (Redis, Postgres, config) |
| **Single entry points** | Each service is a standalone runnable in `src/social_trading/services/` — Docker runs each one independently |

---

## Full File Tree

```
social_trading/
│
├── pyproject.toml                  # PEP 517/518 build config, all deps, tool settings
├── .env.example                    # env var template (never commit .env)
├── .gitignore
├── Makefile                        # dev shortcuts: make test, make lint, make up
├── docker-compose.yml              # all services + infra
├── docker-compose.override.yml     # local dev overrides (hot-reload, debug ports)
│
├── src/
│   └── social_trading/
│       ├── __init__.py
│       │
│       ├── core/                   # ── Shared primitives ──────────────────────
│       │   ├── __init__.py         #    No deps on other sub-packages.
│       │   ├── models.py           #    Pydantic models: SocialPost, Signal,
│       │   ├── protocols.py        #      Trade, Position, SentimentResult
│       │   ├── events.py           #    Redis Stream event schemas
│       │   └── exceptions.py       #    Domain exceptions
│       │
│       ├── config/                 # ── Configuration ──────────────────────────
│       │   ├── __init__.py
│       │   └── system_config.py    #    SystemConfig dataclass (see design §16)
│       │
│       ├── ingest/                 # ── Data Source Layer (PLUGGABLE) ──────────
│       │   ├── __init__.py
│       │   ├── base.py             #    DataSource protocol + BaseDataSource
│       │   ├── registry.py         #    DataSourceRegistry — add/remove sources
│       │   ├── sources/
│       │   │   ├── __init__.py
│       │   │   ├── twitter.py      #    TwitterDataSource
│       │   │   ├── reddit.py       #    RedditDataSource
│       │   │   ├── stocktwits.py   #    StockTwitsDataSource
│       │   │   └── lunarcrush.py   #    LunarCrushDataSource (crypto)
│       │   └── watchlist/
│       │       ├── __init__.py
│       │       └── manager.py      #    WatchlistManager (see design §3a)
│       │
│       ├── nlp/                    # ── NLP Pipeline ────────────────────────────
│       │   ├── __init__.py
│       │   ├── base.py             #    SentimentClassifier protocol
│       │   ├── classifiers/
│       │   │   ├── __init__.py
│       │   │   ├── vader.py        #    VaderClassifier (fast pre-filter)
│       │   │   └── finbert.py      #    FinBERTClassifier (primary)
│       │   ├── filters/
│       │   │   ├── __init__.py
│       │   │   ├── bot_filter.py   #    BotFilter
│       │   │   └── ticker_extractor.py  # TickerExtractor (regex + spaCy)
│       │   └── pipeline.py         #    NLPPipeline orchestrator
│       │
│       ├── signals/                # ── Signal Generation ───────────────────────
│       │   ├── __init__.py
│       │   ├── generator.py        #    SignalGenerator
│       │   ├── aggregator.py       #    SentimentAggregator (time-bucket)
│       │   └── decay.py            #    alpha decay math
│       │
│       ├── risk/                   # ── Risk Management ─────────────────────────
│       │   ├── __init__.py
│       │   ├── circuit_breaker.py  #    CircuitBreaker
│       │   ├── position_sizer.py   #    PositionSizer (Half-Kelly)
│       │   ├── exit_manager.py     #    PositionExitManager
│       │   └── liquidity_gate.py   #    LiquidityGate
│       │
│       ├── execution/              # ── Execution Layer ─────────────────────────
│       │   ├── __init__.py
│       │   ├── base.py             #    ExecutionEngine protocol
│       │   ├── paper.py            #    PaperTradingEngine
│       │   └── ibkr.py             #    IBKRExecutionEngine
│       │
│       ├── market_data/            # ── Market Data ─────────────────────────────
│       │   ├── __init__.py
│       │   ├── base.py             #    MarketDataProvider protocol
│       │   ├── ibkr.py             #    IBKRMarketData (live)
│       │   └── yfinance.py         #    YFinanceMarketData (watchlist/backtest)
│       │
│       ├── storage/                # ── Storage Abstractions ────────────────────
│       │   ├── __init__.py
│       │   ├── base.py             #    Repository protocols
│       │   ├── postgres.py         #    PostgresRepository
│       │   └── event_bus.py        #    TradingEventBus (Redis Streams)
│       │
│       ├── monitoring/             # ── Observability ───────────────────────────
│       │   ├── __init__.py
│       │   ├── metrics.py          #    Prometheus counters/gauges/histograms
│       │   └── streamlit/          #    Streamlit ops panel (see design §15)
│       │       ├── app.py
│       │       ├── pages/
│       │       │   ├── 1_positions.py
│       │       │   ├── 2_signals.py
│       │       │   ├── 3_trades.py
│       │       │   ├── 4_sentiment.py
│       │       │   ├── 5_config.py
│       │       │   └── 6_optimize.py
│       │       └── utils/
│       │           ├── db.py
│       │           └── redis_ctrl.py
│       │
│       └── services/               # ── Runnable Services ───────────────────────
│           ├── __init__.py
│           ├── ingest_service.py   #    Runs ingest loop (all registered sources)
│           ├── nlp_service.py      #    Consumes raw_social, produces sentiment
│           ├── signal_service.py   #    Consumes sentiment, produces signals
│           ├── risk_service.py     #    Consumes signals, applies risk checks
│           └── execution_service.py #   Consumes approved signals, executes
│
├── tests/
│   ├── conftest.py                 # shared fixtures: mock redis, mock postgres, cfg
│   ├── unit/
│   │   ├── config/
│   │   │   └── test_system_config.py
│   │   ├── ingest/
│   │   │   ├── test_registry.py
│   │   │   ├── test_watchlist_manager.py
│   │   │   ├── sources/
│   │   │   │   ├── test_twitter.py
│   │   │   │   ├── test_reddit.py
│   │   │   │   └── test_stocktwits.py
│   │   ├── nlp/
│   │   │   ├── test_bot_filter.py
│   │   │   ├── test_ticker_extractor.py
│   │   │   ├── test_vader.py
│   │   │   ├── test_finbert.py
│   │   │   └── test_pipeline.py
│   │   ├── signals/
│   │   │   ├── test_generator.py
│   │   │   ├── test_aggregator.py
│   │   │   └── test_decay.py
│   │   ├── risk/
│   │   │   ├── test_circuit_breaker.py
│   │   │   ├── test_position_sizer.py
│   │   │   ├── test_exit_manager.py
│   │   │   └── test_liquidity_gate.py
│   │   └── execution/
│   │       └── test_paper_engine.py
│   └── integration/
│       ├── test_ingest_to_nlp.py
│       ├── test_nlp_to_signal.py
│       ├── test_signal_to_execution.py
│       └── test_full_paper_flow.py
│
├── migrations/
│   ├── 001_initial_schema.sql
│   ├── 002_config_runs.sql
│   └── migrate.py
│
├── scripts/
│   ├── seed_watchlist.py           # pre-populate seed tickers
│   └── backfill_baselines.py       # build 7-day Z-score baselines
│
├── infrastructure/
│   ├── grafana/
│   │   └── provisioning/
│   │       ├── datasources/
│   │       └── dashboards/
│   └── prometheus/
│       └── prometheus.yml
│
└── docs/
    ├── design/                     # existing design docs
    └── plan/                       # this document set
```

---

## `pyproject.toml` Structure

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "social-trading"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    # Core
    "pydantic>=2.5.0",
    "redis>=4.6.0",
    "psycopg2-binary>=2.9.0",
    # Social APIs
    "tweepy>=4.14.0",
    "praw>=7.7.0",
    # NLP
    "transformers>=4.35.0",
    "torch>=2.0.0",
    "vaderSentiment>=3.3.2",
    "spacy>=3.7.0",
    # Broker
    "ib_async>=0.9.0",
    # Data
    "pandas>=2.0.0",
    "numpy>=1.24.0",
    "yfinance>=0.2.0",
    # Infrastructure
    "prometheus-client>=0.17.0",
    "exchange-calendars>=4.3.0",
    # Monitoring UI
    "streamlit>=1.32.0",
    "plotly>=5.18.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-asyncio>=0.21.0",
    "pytest-cov>=4.1.0",
    "fakeredis>=2.20.0",       # in-memory Redis for tests
    "respx>=0.20.0",           # mock httpx calls
    "ruff>=0.1.0",             # linting + formatting
    "mypy>=1.7.0",
]
backtest = [
    "vectorbt>=0.26.0",
]

[tool.hatch.build.targets.wheel]
packages = ["src/social_trading"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
addopts = "--cov=src/social_trading --cov-report=term-missing"

[tool.ruff]
src = ["src"]
line-length = 100
select = ["E", "F", "I", "UP"]

[tool.mypy]
python_version = "3.11"
strict = true
```

---

## `Makefile` — Developer Shortcuts

```makefile
.PHONY: install test lint type-check up down migrate

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
```

---

*[⬆ Back to plan index](README.md)*
