# 03 — Development Phases

## Overview

The system is built in 8 phases. Each phase delivers a runnable/testable increment.
Phases 1–5 are designed for parallel execution by independent developers:
each team only needs `core/` + their component's protocol.

```
Phase 0 — Foundation          (Week 1)    All phases depend on this
Phase 1 — Data Ingestion      (Weeks 2–3) Depends on: Phase 0
Phase 2 — NLP Pipeline        (Weeks 3–4) Depends on: Phase 0
Phase 3 — Signal Generation   (Week 5)    Depends on: Phase 1, 2
Phase 4 — Risk Management     (Week 5)    Depends on: Phase 0 (protocol only)
Phase 5 — Execution           (Week 6)    Depends on: Phase 4
Phase 6 — Infrastructure      (Week 7)    Depends on: Phase 1–5
Phase 7 — UI                  (Week 8)    Depends on: Phase 6
Phase 8 — Integration & QA    (Weeks 9–10) Depends on: All
```

---

## Phase 0 — Foundation

**Goal:** Project skeleton everyone can import from.

### Tasks

| # | Task | File | Notes |
|---|------|------|-------|
| 0.1 | Init project with pyproject.toml | `pyproject.toml` | See §01 |
| 0.2 | Create `src/social_trading/` layout | all `__init__.py` files | |
| 0.3 | Define Pydantic models | `core/models.py` | SocialPost, Signal, OrderResult, Position |
| 0.4 | Define Protocol interfaces | `core/protocols.py` | DataSource, SentimentClassifier, ExecutionEngine, etc. |
| 0.5 | Define Redis stream event schemas | `core/events.py` | Typed dicts for each stream |
| 0.6 | Define domain exceptions | `core/exceptions.py` | CircuitBreakerOpen, InsufficientLiquidity, etc. |
| 0.7 | Implement SystemConfig dataclass | `config/system_config.py` | Port from design §16 |
| 0.8 | Implement TradingEventBus | `storage/event_bus.py` | Thin wrapper on redis-py Streams |
| 0.9 | Write initial DB migrations | `migrations/001_initial_schema.sql` | Port from design §9 |
| 0.10 | Docker Compose: postgres + redis | `docker-compose.yml` | |
| 0.11 | CI setup | `.github/workflows/ci.yml` | ruff + mypy + pytest on push |

### Deliverable

```bash
make install && make up && make migrate
python -c "from social_trading.config.system_config import SystemConfig; print('OK')"
```

---

## Phase 1 — Data Ingestion

**Goal:** All social data sources collecting posts and publishing to `raw_social` stream.

### 1a — Base & Registry

| Task | File |
|------|------|
| Implement `BaseDataSource` ABC | `ingest/base.py` |
| Implement `DataSourceRegistry` | `ingest/registry.py` |
| Implement `WatchlistManager` | `ingest/watchlist/manager.py` |

`WatchlistManager` reads from Redis ZSET `watchlist:active`.
Expiry logic: ticker score = last-seen epoch; trim if `now - score > cfg.watchlist_stale_hours * 3600`.

### 1b — Twitter / X

| Task | File | Notes |
|------|------|-------|
| Implement `TwitterDataSource` | `ingest/sources/twitter.py` | |
| Tier-1: Counts polling every 5 min | | `GET /2/tweets/counts/recent` |
| Tier-2: Search on Z-score ≥ cfg.spike_zscore | | `GET /2/tweets/search/recent` |
| Z-score calculator (7-day rolling mean/std) | | Stored in Redis HASH `zscore:{ticker}` |
| Unit tests | `tests/unit/ingest/sources/test_twitter.py` | Mock tweepy with `respx` |

### 1c — Reddit

| Task | File | Notes |
|------|------|-------|
| Implement `RedditDataSource` | `ingest/sources/reddit.py` | |
| PRAW streaming on r/wallstreetbets, r/stocks, r/investing | | |
| Cashtag extraction → auto-add to watchlist | | |
| Unit tests | `tests/unit/ingest/sources/test_reddit.py` | Fake PRAW submission |

### 1d — StockTwits

| Task | File | Notes |
|------|------|-------|
| Implement `StockTwitsDataSource` | `ingest/sources/stocktwits.py` | |
| Trending endpoint every 5 min (free) | | `GET /api/2/trending/symbols.json` |
| Per-ticker stream with rate limiting | | |
| Unit tests | `tests/unit/ingest/sources/test_stocktwits.py` | |

### 1e — Ingest Service

```python
# services/ingest_service.py
async def main():
    redis = aioredis.from_url(settings.redis_url)
    cfg = SystemConfig.load(redis)

    registry = DataSourceRegistry()
    registry.register(TwitterDataSource(redis=redis, cfg=cfg))
    registry.register(RedditDataSource(reddit=praw_client(), redis=redis, cfg=cfg))
    registry.register(StockTwitsDataSource(redis=redis, cfg=cfg))

    watchlist = WatchlistManager(redis=redis, cfg=cfg)

    tasks = []
    for source in registry.active_sources():
        if source.is_streaming:
            tasks.append(asyncio.create_task(run_stream(source)))
        else:
            tasks.append(asyncio.create_task(run_poll_loop(source, watchlist, cfg)))

    await asyncio.gather(*tasks)
```

### Phase 1 Deliverable

```bash
docker compose up -d && python -m social_trading.services.ingest_service
redis-cli xlen raw_social   # > 0 within 5 minutes
```

---

## Phase 2 — NLP Pipeline

**Goal:** Classify every post in `raw_social` and publish scored results to `sentiment_signals`.

> Phase 2 can be developed in parallel with Phase 1 using `FakeEventBus`.

### 2a — Filters

| Task | File | Notes |
|------|------|-------|
| `BotFilter` | `nlp/filters/bot_filter.py` | account_age < 30d, follower/following > 10, velocity > 50/hr |
| `TickerExtractor` | `nlp/filters/ticker_extractor.py` | regex `\$[A-Z]{1,5}` + spaCy NER fallback |
| Unit tests | `tests/unit/nlp/test_bot_filter.py` | |

### 2b — VADER Pre-filter

| Task | File | Notes |
|------|------|-------|
| `VaderClassifier` | `nlp/classifiers/vader.py` | Wrap vaderSentiment, return `SentimentResult` |
| Drop if `abs(compound) < 0.05` | | neutral threshold |
| Unit tests | `tests/unit/nlp/test_vader.py` | |

### 2c — FinBERT Classifier

| Task | File | Notes |
|------|------|-------|
| `FinBERTClassifier` | `nlp/classifiers/finbert.py` | `yiyanghkust/finbert-tone` |
| Batch classification (configurable batch size) | | 16 posts/batch default |
| GPU → CPU fallback | | `device = "cuda" if torch.cuda.is_available() else "cpu"` |
| Unit tests | `tests/unit/nlp/test_finbert.py` | Mock HuggingFace model |

### 2d — NLP Pipeline Orchestrator

```python
# nlp/pipeline.py
class NLPPipeline:
    def __init__(
        self,
        bot_filter: BotFilter,
        ticker_extractor: TickerExtractor,
        prefilter: SentimentClassifier,   # VADER
        classifier: SentimentClassifier,  # FinBERT
        cfg: SystemConfig,
    ):
        ...

    async def process(self, post: SocialPost) -> SentimentResult | None:
        if self.bot_filter.is_bot(post):
            return None
        post.ticker = self.ticker_extractor.extract(post.text) or post.ticker
        pre = await self.prefilter.classify(post)
        if abs(pre.score) < self.cfg.vader_neutral_threshold:
            return None                     # drop neutral noise
        return await self.classifier.classify(post)
```

### 2e — NLP Service

Consumes `raw_social` (consumer group `nlp`), batches up to N posts, runs pipeline,
publishes `SentimentResult` to `sentiment_signals`.

### Phase 2 Deliverable

```bash
redis-cli xlen sentiment_signals   # > 0 after ingestion
```

---

## Phase 3 — Signal Generation

**Goal:** Aggregate sentiment time-buckets into actionable signals on `strategy_signals`.

### Tasks

| Task | File | Notes |
|------|------|-------|
| `SentimentAggregator` | `signals/aggregator.py` | 15-min buckets, volume-weighted mean |
| `SignalGenerator` | `signals/generator.py` | quality score, direction, threshold |
| `alpha_decay()` | `signals/decay.py` | exp decay λ=cfg.decay_lambda |
| Signal service | `services/signal_service.py` | consumes sentiment_signals |
| Unit tests | `tests/unit/signals/` | pure math; no I/O |

### Quality Score Formula

```python
def quality_score(v: float, s: float, p: float, m: float, c: float, cfg: SystemConfig) -> float:
    """
    v = volume_z_score (normalised to 0-1)
    s = sentiment_strength
    p = proactivity (original posts vs reposts)
    m = momentum (rate of change)
    c = convergence (fraction of sources agreeing)
    """
    return (
        cfg.w_volume       * v +
        cfg.w_sentiment    * s +
        cfg.w_proactivity  * p +
        cfg.w_momentum     * m +
        cfg.w_convergence  * c
    )
```

Fires signal only if: `quality >= cfg.signal_quality_threshold AND sentiment > cfg.min_sentiment AND price direction aligned`.

### Phase 3 Deliverable

```bash
redis-cli xlen strategy_signals   # > 0 during active hours
```

---

## Phase 4 — Risk Management

**Goal:** Gate every signal through risk checks before forwarding to execution.

> Phase 4 can be developed in parallel with Phases 1–3 using fake signals.

### Tasks

| Task | File | Notes |
|------|------|-------|
| `PositionSizer` | `risk/position_sizer.py` | Half-Kelly × volatility, capped at cfg.max_position_pct |
| `CircuitBreaker` | `risk/circuit_breaker.py` | trade/daily/weekly/drawdown halts |
| `PositionExitManager` | `risk/exit_manager.py` | time-stop, ATR stop, take-profit |
| `LiquidityGate` | `risk/liquidity_gate.py` | avg_volume > cfg.min_avg_volume, spread < cfg.max_spread_pct |
| Risk service | `services/risk_service.py` | gate + enrich signals → selected_signals |
| Unit tests | `tests/unit/risk/` | pure functions; inject AccountState |

### Circuit Breaker State Machine

```
NORMAL → [daily_loss > cfg.daily_halt_pct]  → DAILY_HALT  (reset next day)
NORMAL → [weekly_loss > cfg.weekly_reduce_pct] → REDUCED_50
NORMAL → [drawdown > cfg.drawdown_halt_pct] → FULL_HALT   (manual reset required)
```

Circuit breaker state stored in Redis key `circuit:state` as JSON.

### Phase 4 Deliverable

```bash
redis-cli xlen selected_signals   # only approved signals pass through
redis-cli get circuit:state       # shows current breaker state
```

---

## Phase 5 — Execution

**Goal:** Execute approved signals and manage positions.

### 5a — Paper Trading Engine

Built first — mirrors IBKR interface without real money.

```python
# execution/paper.py
class PaperTradingEngine:
    """In-memory paper trading — satisfies ExecutionEngine protocol."""

    def __init__(self, initial_cash: float = 100_000.0):
        self._cash = initial_cash
        self._positions: dict[str, Position] = {}
        self._trades: list[dict] = []
```

Simulates fills at last-known price from market_data stream.
Tests run entirely against paper engine.

### 5b — IBKR Execution Engine

| Task | File | Notes |
|------|------|-------|
| Implement `IBKRExecutionEngine` | `execution/ibkr.py` | ib_async bracket orders |
| `IBKRMarketData` | `market_data/ibkr.py` | reqMktData, reqHistoricalData |
| Port execution code from design §7 | | Already written in design docs |
| Unit tests (paper engine) | `tests/unit/execution/test_paper_engine.py` | |

### 5c — Execution Service

Consumes `selected_signals`, checks if position already open, sizes using `PositionSizer`,
submits bracket order via injected `ExecutionEngine`.

Tick on exit manager every 60 seconds — checks all open positions for exits.

### Phase 5 Deliverable

```bash
# Paper trading end-to-end:
python -m social_trading.services.execution_service --engine=paper
```

---

## Phase 6 — Infrastructure

**Goal:** Full system runnable with one command, metrics exported.

### Tasks

| Task | File | Notes |
|------|------|-------|
| Prometheus metrics | `monitoring/metrics.py` | Expose /metrics on port 8000 |
| Grafana provisioning | `infrastructure/grafana/` | Port dashboards from design §13 |
| Docker Compose (all services) | `docker-compose.yml` | Each service one container |
| `migrate.py` | `migrations/migrate.py` | Idempotent migration runner |
| `seed_watchlist.py` | `scripts/seed_watchlist.py` | Seed cfg.seed_tickers to Redis |

### Docker Compose Services

```yaml
services:
  postgres:     image: postgres:16-alpine
  redis:        image: redis:7-alpine
  ingest:       build: . command: python -m social_trading.services.ingest_service
  nlp:          build: . command: python -m social_trading.services.nlp_service
  signal:       build: . command: python -m social_trading.services.signal_service
  risk:         build: . command: python -m social_trading.services.risk_service
  execution:    build: . command: python -m social_trading.services.execution_service
  streamlit:    build: . command: streamlit run src/social_trading/monitoring/streamlit/app.py
  prometheus:   image: prom/prometheus
  grafana:      image: grafana/grafana
```

### Phase 6 Deliverable

```bash
make services-up
curl http://localhost:8501   # Streamlit UI
curl http://localhost:3000   # Grafana
curl http://localhost:8000/metrics   # Prometheus
```

---

## Phase 7 — UI

**Goal:** Streamlit control panel fully wired to live system.

### Tasks

| Task | File |
|------|------|
| `app.py` main entry + sidebar | `monitoring/streamlit/app.py` |
| Positions page | `pages/1_positions.py` |
| Signals feed page | `pages/2_signals.py` |
| Trade history page | `pages/3_trades.py` |
| Sentiment heatmap page | `pages/4_sentiment.py` |
| Config editor page | `pages/5_config.py` |
| Optimization page | `pages/6_optimize.py` |
| Grafana: 4 dashboards | `infrastructure/grafana/` |

Port all code from design §15. Pages already designed — this is implementation only.

---

## Phase 8 — Integration & QA

**Goal:** End-to-end paper trading run for minimum 5 days before live.

### Tasks

| Task | Notes |
|------|-------|
| Integration tests (Phases 1→5 chain) | Use `fakeredis` + in-memory postgres |
| End-to-end paper trade flow test | `tests/integration/test_full_paper_flow.py` |
| 5-day paper trading run | Record all signals, trades, P&L |
| Parameter sensitivity analysis | Use optimize page (design §17) |
| Review circuit breaker triggers | Ensure no false halts |
| IBKR live account dry run | 1 week, micro lot sizes |
| Sharpe > 0.5 gate before live | Per design §10 deployment pipeline |

---

## Milestone Summary

| Milestone | Phase | Completion Signal |
|-----------|-------|-----------------|
| M0 — Scaffold | 0 | `make install && make up` → OK |
| M1 — Data flowing | 1 | `xlen raw_social > 0` |
| M2 — Sentiment scored | 2 | `xlen sentiment_signals > 0` |
| M3 — Signals generated | 3 | `xlen strategy_signals > 0` |
| M4 — Risk gated | 4 | `xlen selected_signals > 0` |
| M5 — Paper trading | 5 | Bracket orders fill in paper engine |
| M6 — Full system | 6 | `make services-up` → all containers healthy |
| M7 — UI operational | 7 | Streamlit shows live data |
| M8 — Ready for live | 8 | 5-day paper run, Sharpe > 0.5 |

---

*[⬆ Back to plan index](README.md)*
