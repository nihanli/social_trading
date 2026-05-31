## 2. System Architecture

### High-Level Architecture

```mermaid
graph TD
    subgraph Data_Sources["📡 Data Sources"]
        subgraph Tier1["Tier 1 — Free / Always-On"]
            RD["Reddit PRAW\nr/wallstreetbets"]
            ST["StockTwits API\nSymbol Stream"]
            BS["Bluesky AT Proto\nFinance feed"]
            YF["Yahoo Finance\nScreener"]
        end
        subgraph Tier2["Tier 2 — Metered / On-Demand"]
            TW["X/Twitter API v2\n(cost-per-request)"]
        end
    end

    subgraph Ingestion["🔄 Ingest Layer (Python)"]
        SI["Social Ingest\nService"]
        EL["Enrichment Loop\n(consumes enrichment:requests\ncalls Tier-2 for candidates)"]
    end

    subgraph Stream["📨 Redis Streams Event Bus"]
        RS1["raw_social"]
        RS2["sentiment_signals"]
        RS3["market_data"]
        RS4["strategy_signals"]
        RS5["selected_signals"]
        RS6["enrichment:requests"]
    end

    subgraph NLP["🧠 NLP Layer"]
        NLP1["FinBERT / VADER\nSentiment Service"]
        TK["Ticker Extractor\n(Regex + spaCy NER)"]
        BF["Bot Filter\n(Account age, velocity)"]
    end

    subgraph Signal["📊 Two-Phase Signal Layer"]
        SE["Signal Engine\nPhase 1: free sources\n→ phase1_threshold\nPhase 2: +Tier-2 data\n→ phase2_threshold"]
        PF["Price Feed\n(IBKR reqMktData)"]
    end

    subgraph Risk["🛡️ Risk Layer"]
        RM["Risk Manager\n(Position limits,\nCircuit breakers,\nCorrelation checks)"]
    end

    subgraph Execution["⚡ Execution Layer"]
        PT["Paper Trading\nEngine (Stage 1)"]
        LT["Live Execution\nEngine (Stage 2)\nIBKR ib_async"]
    end

    subgraph Storage["💾 Storage"]
        PG["PostgreSQL 15\n(Trades, Signals,\nSentiment, Equity)"]
        RDB["Redis\n(Streams + Cache)"]
    end

    subgraph Observability["📈 Observability"]
        PR["Prometheus\nMetrics"]
        GF["Grafana\nDashboards"]
        AL["Alerting\n(PagerDuty/Email)"]
    end

    RD --> SI
    ST --> SI
    BS --> SI
    YF --> SI
    TW --> SI

    SI --> RS1
    RS1 --> BF
    BF --> TK
    TK --> NLP1
    NLP1 --> RS2

    RS2 --> SE
    SE -- "Phase-1 candidate\n(no open position)" --> RS6
    RS6 --> EL
    EL --> TW

    PF --> RS3
    RS3 --> SE
    SE -- "Phase-2 signal\n(or Phase-1 direct\nif no Tier-2)" --> RS4
    RS4 --> RM
    RM --> RS5

    RS5 --> PT
    RS5 --> LT

    PT --> PG
    LT --> PG
    SE --> PG
    NLP1 --> PG

    PR --> GF
    GF --> AL
```

### Component Responsibilities

| Component | Responsibility | Technology |
|-----------|---------------|------------|
| Social Ingest Service | Poll/stream Tier-1 sources (Reddit, StockTwits, Bluesky, YFinance) for all watchlist tickers | Python + asyncio |
| Enrichment Loop | Consume `enrichment:requests`, call Tier-2 (X/Twitter) only for Phase-1 signal candidates | Python coroutine inside ingest service |
| Bot Filter | Remove bots, spam, retweets before NLP processing | Python (account age, velocity checks) |
| Ticker Extractor | Extract $TICKER cashtags, company NER → validate against universe | spaCy NER + regex + S&P500 lookup |
| NLP Sentiment Service | Classify each post: POSITIVE/NEGATIVE/NEUTRAL + confidence score | HuggingFace FinBERT-Tone (yiyanghkust/finbert-tone) |
| Signal Engine (Two-Phase) | Phase 1: evaluate free-source stats against `signal_phase1_threshold`; Phase 2: re-evaluate with Tier-2 data against `signal_phase2_threshold` | Python + asyncio |
| Price Feed | Real-time bid/ask + OHLCV from IBKR | ib_async reqMktData |
| Risk Manager | Position size, daily loss limit, correlation, circuit breakers | Python microservice |
| Paper Trading Engine | Simulated execution with slippage; P&L tracking | Python + PostgreSQL |
| Live Execution Engine | IBKR order placement via ib_async; bracket orders | ib_async (ib-api-reloaded/ib_async) |

---

---

*[⬆ Back to main index](README.md)*
