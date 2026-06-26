# Data Architecture

This document describes the PostgreSQL schema and Redis data model used by the social trading system.

---

## PostgreSQL Schema

### Entity-Relationship Diagram

```
social_raw ──────────────────────────────────────────────────────────┐
  id (PK)                                                             │
  source, post_id (UNIQUE)                                            │ post_id FK
  ticker, raw_text, author                                            │
  followers, likes, retweets, upvotes, flair                          ▼
  created_at, ingested_at                               sentiment_scores
                                                          id (PK)
                                                          post_id (FK → social_raw.post_id)
                                                          ticker
                                                          pos_prob, neg_prob, neu_prob
                                                          label, model, source
                                                          scored_at

sentiment_aggregates
  ticker + window_start + window_minutes (PK)
  avg_sentiment, weighted_score, post_count
  mention_zscore, signal_quality

signals ──────────────────────────────────────────────────────────────┐
  id (PK)                                                             │
  ticker, timestamp, generated_at                                     │ signal_id FK
  strategy, direction                                                 │
  confidence, sentiment_score, mention_zscore, quality_score          ▼
  signal_phase, momentum, convergence, proactivity, atr          trades
  approved, executed, contrarian                                    id (PK)
  rejection_reason                                                  signal_id (FK → signals.id)
                                                                    ticker, strategy, direction
                                                                    shares, entry_price, exit_price
                                                                    stop_price, target_price
                                                                    pnl, fees, net_pnl, pnl_pct
                                                                    entry_reason, exit_reason
                                                                    status, mode
                                                                    opened_at, closed_at
                                                                    stream_event_id, atr_at_entry

positions                          account_equity
  ticker (UNIQUE)                    id (PK)
  direction, shares, entry_price     timestamp, equity, mode
  stop_loss, take_profit
  high_water_mark, unrealized_pnl
  strategy, opened_at, updated_at

price_ohlc                         market_data (legacy)
  id (PK)                            id (PK)
  ticker + bar_datetime +            symbol + timestamp + timeframe (UNIQUE)
    timeframe + source (UNIQUE)      open, high, low, close, volume
  open, high, low, close, volume
  source, fetched_at

config_runs                        schema_migrations
  id (PK)                            filename (PK)
  run_date + mode (UNIQUE)           applied_at
  config_snapshot (JSONB), config_hash
  performance metrics (pnl, trades,
    win_rate, sharpe, drawdown, etc.)
  exit_type breakdowns
  created_at
```

---

### Tables

#### `social_raw`
Raw social media posts ingested from all sources (Reddit, Bluesky, StockTwits, etc.).

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | Auto-increment |
| `source` | varchar | `reddit`, `bluesky`, `stocktwits`, etc. |
| `post_id` | varchar UNIQUE | Source-native post ID |
| `ticker` | varchar | Extracted ticker symbol |
| `raw_text` | text | Original post content |
| `author` | varchar | |
| `followers`, `likes`, `retweets`, `upvotes` | int | Engagement metrics |
| `flair` | varchar | Reddit flair / post type |
| `created_at` | timestamptz | Original post time |
| `ingested_at` | timestamptz | When ingested |

---

#### `sentiment_scores`
NLP sentiment probabilities per post, linked to `social_raw`.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `post_id` | varchar FK → `social_raw.post_id` | |
| `ticker` | varchar | |
| `pos_prob`, `neg_prob`, `neu_prob` | float | FinBERT / VADER probabilities |
| `label` | varchar | `positive`, `negative`, `neutral` |
| `model` | varchar | `finbert`, `vader` |
| `source` | varchar | Ingest source name |
| `scored_at` | timestamptz | |

---

#### `sentiment_aggregates`
Rolling window aggregates written by `signal_service`, used for signal generation.

| Column | Type | Notes |
|--------|------|-------|
| `ticker` + `window_start` + `window_minutes` | composite PK | |
| `avg_sentiment`, `weighted_score` | float | |
| `post_count`, `mention_zscore` | float | |
| `signal_quality` | float | Combined quality score |

---

#### `signals`
Trade signal candidates, one row per signal generated. Approved signals become trades.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `ticker`, `timestamp`, `generated_at` | | |
| `strategy` | varchar | e.g. `social_momentum` |
| `direction` | varchar | `LONG` or `SHORT` |
| `confidence`, `quality_score` | float | |
| `sentiment_score`, `mention_zscore` | float | |
| `signal_phase` | varchar | `phase1` or `phase2` |
| `momentum`, `convergence`, `proactivity` | float | Quality factors |
| `atr` | float | ATR at signal time |
| `approved` | bool | Set by `risk_service` |
| `executed` | bool | Set by `execution_service` |
| `contrarian` | bool | True if generated in contrarian mode |
| `rejection_reason` | text | Why rejected (if not approved/executed) |

---

#### `trades`
Executed trade lifecycle — one row per position opened.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `signal_id` | bigint FK → `signals.id` | Source signal |
| `ticker`, `strategy`, `direction` | | |
| `shares` | int | Position size |
| `entry_price`, `exit_price` | float | |
| `stop_price`, `target_price` | float | At time of entry |
| `pnl`, `fees`, `net_pnl`, `pnl_pct` | float | |
| `entry_reason`, `exit_reason` | varchar | |
| `status` | varchar | `open`, `closed` |
| `mode` | varchar | `paper`, `live` |
| `opened_at`, `closed_at` | timestamptz | |
| `stream_event_id` | varchar UNIQUE | Dedup key from Redis stream |
| `atr_at_entry` | float | ATR when position opened |

---

#### `positions`
Current open positions (live state mirror of IB).

| Column | Type | Notes |
|--------|------|-------|
| `ticker` | varchar UNIQUE | |
| `direction` | varchar | `LONG` / `SHORT` |
| `shares`, `entry_price` | | |
| `stop_loss`, `take_profit` | float | Active levels |
| `high_water_mark` | float | Best price seen (for trailing stop) |
| `unrealized_pnl` | float | |
| `strategy`, `opened_at`, `updated_at` | | |

---

#### `account_equity`
Equity snapshots written periodically by `execution_service`.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `timestamp` | timestamptz | |
| `equity` | float | Net liquidation value |
| `mode` | varchar | `paper` / `live` |

---

#### `price_ohlc`
Historical OHLCV bars used by backtesting and optimizer.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `ticker` + `bar_datetime` + `timeframe` + `source` | UNIQUE | |
| `open`, `high`, `low`, `close` | float | |
| `volume` | bigint | |
| `source` | varchar | `yfinance`, `ibkr`, etc. |
| `fetched_at` | timestamptz | |

---

#### `config_runs`
Daily config snapshots with performance metrics, written at end-of-day.

| Column | Type | Notes |
|--------|------|-------|
| `run_date` + `mode` | UNIQUE | |
| `config_snapshot` | JSONB | Full `SystemConfig` at run time |
| `config_hash` | varchar | For change detection |
| `total_pnl`, `total_trades`, `win_count`, `win_rate` | | |
| `sharpe_ratio`, `max_drawdown`, `profit_factor` | | |
| `avg_hold_hours`, `avg_signal_quality`, `avg_mention_zscore` | | |
| `exits_take_profit` … `exits_manual` | int | Exit reason breakdown |

---

### Key Indexes

| Table | Index | Purpose |
|-------|-------|---------|
| `signals` | `(ticker, generated_at)` | Time-series lookup per ticker |
| `signals` | `approved`, `executed`, `signal_phase` | Fast filter for UI queries |
| `trades` | `signal_id` | JOIN to signals |
| `trades` | `status`, `ticker`, `opened_at` | Open position lookups |
| `sentiment_scores` | `ticker` | Aggregation queries |
| `social_raw` | `(ticker, created_at)` | Ingest dedup + time-range queries |
| `price_ohlc` | `(ticker, bar_datetime, timeframe)` | Backtesting range scans |
| `config_runs` | `(run_date, mode)`, `sharpe_ratio` | Optimizer queries |

---

## Redis Data Model

Redis is the real-time backbone of the pipeline. All inter-service communication uses Redis Streams. Supporting state (positions, config, market data) uses Hashes, Strings, Sorted Sets, and Lists.

### Stream Pipeline (Signal Flow)

```
[Ingest Sources]
  Reddit / Bluesky / ApeWisdom / YFinance / IB Scanner
        │
        │  xadd
        ▼
  raw_social  (stream)
  ─────────────────────────────────────────────────────
  Consumer group: nlp
        │
        │  NLP service: score sentiment, extract tickers
        │  xadd
        ▼
  sentiment_signals  (stream)
  ─────────────────────────────────────────────────────
  Consumer groups: signal, persist
        │
        │  signal_service: aggregate windows, compute quality score
        │  xadd
        ▼
  strategy_signals  (stream)
  ─────────────────────────────────────────────────────
  Consumer groups: risk, persist
        │
        │  risk_service: position sizing, liquidity gate, circuit breaker
        │    ├─ REJECTED ──► signal:rejections (stream)
        │    │                  Consumer group: persist
        │    │                  → persistence_service writes rejection_reason to DB
        │    └─ APPROVED ──► selected_signals (stream)
        │
        ▼
  selected_signals  (stream)
  ─────────────────────────────────────────────────────
  Consumer groups: execution, persist
        │
        │  execution_service: place OCA orders via IB
        │    ├─ REJECTED ──► signal:rejections (stream)
        │    └─ FILLED ───► execution:events (stream)
        │
        ▼
  execution:events  (stream)
  ─────────────────────────────────────────────────────
  Consumer group: persist
        │
        │  persistence_service: write trade open/close to DB
        ▼
  [PostgreSQL trades table]

  enrichment:requests  (stream)
  ─────────────────────────────────────────────────────
  Phase-1 signals that need Tier-2 enrichment
  Consumer group: ingest
  → ingest_service fetches deeper data for the ticker

  logs:*  (streams: logs:ingest, logs:nlp, logs:signal,
           logs:risk, logs:execution, logs:persistence)
  ─────────────────────────────────────────────────────
  Structured log entries streamed to UI (Logs page)
```

---

### Hashes (Structured State)

| Key | Fields | Written by | Read by |
|-----|--------|------------|---------|
| `market_data:{ticker}` | `last`, `bid`, `ask`, `adv_shares`, `adv_usd`, `market_cap_usd`, `atr_14`, `realised_vol`, `vix` | execution_service (market data loop) | risk_service (entry sizing), execution_service (exit eval) |
| `positions:live` | `{ticker}` → JSON position object | execution_service | UI (positions page), reconcile logic |
| `position:params` | `{ticker}` → JSON OCA params (entry_id, oca_group, stop_order_id, etc.) | execution_service | execution_service (exit modify, reattach) |
| `position:trail_orders` | `{ticker}` → JSON trail order IDs | execution_service | execution_service (trailing stop updates) |
| `orders:inflight` | `{order_id}` → JSON entry order metadata | execution_service | reconcile loop (fill detection) |
| `exits:inflight` | `{order_id}` → JSON exit order metadata | execution_service | reconcile loop (fill detection) |
| `account:state` | `equity`, `cash`, `margin`, `daily_pnl`, `drawdown`, `vix`, etc. | execution_service | risk_service, UI sidebar |
| `hwm:all` | `{ticker}` → float high-water-mark | execution_service | execution_service (trailing stop eval) |
| `alerts:fill_sync` | `{order_id}` → JSON alert payload | execution_service | UI (warning banner, Reconcile page) |
| `reconcile:conflicts` | `{ticker}` → JSON conflict data | execution_service | UI (warning banner), trade loop guard |
| `ingest:sources:registry` | `{source_name}` → JSON source metadata | ingest_service | UI (ingest stats) |

---

### Strings (Scalar State)

| Key | Value | Written by | TTL |
|-----|-------|------------|-----|
| `config:system` | JSON `SystemConfig` | config page / execution_service | none |
| `ib:connected` | `"1"` / `"0"` | execution_service | none |
| `market:vix` | float string | execution_service | none |
| `reconcile:state` | `collecting` / `awaiting_approval` / `approved` / `skipped_no_ib` | execution_service | 1h |
| `reconcile:data` | JSON reconcile payload | execution_service | 1h |
| `reconcile:last_run` | ISO timestamp | execution_service | 1h |
| `reconcile:full` | JSON full snapshot for UI | execution_service | 1h |
| `service:heartbeat` | timestamp | all services | short |
| `apewisdom:leaderboard_cache` | JSON leaderboard | ingest_service | varies |
| `google_trends:last_fetch_ts` | ISO timestamp | ingest_service | none |
| `discovery:last_poll_ts` | ISO timestamp | ingest_service | none |
| `ingest:tier2_active` | `"1"` / `"0"` | ingest_service | none |
| `position:adopted:{ticker}:{price}:{shares}` | `"1"` | execution_service | none |
| `stocktwits:cursor:{ticker}` | cursor string | ingest_service | none |

---

### Sorted Sets (Time-Window Data)

| Key | Score | Member | Purpose |
|-----|-------|--------|---------|
| `sentiment:window:{ticker}` | Unix timestamp | JSON sentiment event | Rolling sentiment window for signal aggregation. NLP service writes; signal_service reads and trims. |

---

### Lists (Rolling History)

| Key | Contents | Purpose |
|-----|----------|---------|
| `mention_history:{source}:{ticker}` | Timestamped mention counts (JSON) | Per-source mention rate history for mention_zscore calculation |

---

### Sets

| Key | Members | Purpose |
|-----|---------|---------|
| `watchlist:seed` | Ticker symbols | Base watchlist loaded at startup |
| `watchlist:ticker_sources:{ticker}` | Source names | Which ingest sources contributed this ticker to the watchlist |
| `price_fetch:queue` | Ticker symbols | Tickers queued for OHLC backfill |

---

### Stream Retention (MAXLEN)

All streams are capped to prevent unbounded memory growth:

| Stream | Max entries |
|--------|-------------|
| `raw_social` | 100,000 |
| `sentiment_signals` | 50,000 |
| `strategy_signals` | 50,000 |
| `selected_signals` | 10,000 |
| `execution:events` | 10,000 |
| `signal:rejections` | 50,000 |
| `enrichment:requests` | 5,000 |
| `logs:*` | 5,000 each |

---

### Consumer Groups Summary

| Stream | Consumer Group | Service |
|--------|---------------|---------|
| `raw_social` | `nlp` | nlp_service |
| `raw_social` | `persist` | persistence_service |
| `sentiment_signals` | `signal` | signal_service |
| `sentiment_signals` | `persist` | persistence_service |
| `strategy_signals` | `risk` | risk_service |
| `strategy_signals` | `persist` | persistence_service |
| `selected_signals` | `execution` | execution_service |
| `selected_signals` | `persist` | persistence_service |
| `signal:rejections` | `persist` | persistence_service |
| `execution:events` | `persist` | persistence_service |
| `enrichment:requests` | `ingest` | ingest_service |
