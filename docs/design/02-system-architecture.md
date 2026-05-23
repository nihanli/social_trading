## 2. System Architecture

### High-Level Architecture

```mermaid
graph TD
    subgraph Data_Sources["📡 Data Sources"]
        TW["X/Twitter API\nFiltered Stream"]
        RD["Reddit PRAW\nr/wallstreetbets"]
        ST["StockTwits API\nSymbol Stream"]
        LC["LunarCrush API\n(Crypto)"]
    end

    subgraph Ingestion["🔄 Ingest Layer (Python)"]
        SI["Social Ingest\nService"]
    end

    subgraph Stream["📨 Redis Streams Event Bus"]
        RS1["raw_social_stream"]
        RS2["sentiment_signals_stream"]
        RS3["market_data_stream"]
        RS4["strategy_signals_stream"]
        RS5["selected_signals_stream"]
    end

    subgraph NLP["🧠 NLP Layer"]
        NLP1["FinBERT / VADER\nSentiment Service"]
        TK["Ticker Extractor\n(Regex + spaCy NER)"]
        BF["Bot Filter\n(Account age, velocity)"]
    end

    subgraph Signal["📊 Signal Layer"]
        SE["Strategy Engine\n(Z-score, Volume Spike,\nCross-Platform Convergence)"]
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

    TW --> SI
    RD --> SI
    ST --> SI
    LC --> SI

    SI --> RS1
    RS1 --> BF
    BF --> TK
    TK --> NLP1
    NLP1 --> RS2

    PF --> RS3
    RS2 --> SE
    RS3 --> SE
    SE --> RS4
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
| Social Ingest Service | Stream from X API, Reddit, StockTwits; deduplicate; push to Redis | Python + asyncio, tweepy, praw |
| Bot Filter | Remove bots, spam, retweets before NLP processing | Python (account age, velocity checks) |
| Ticker Extractor | Extract $TICKER cashtags, company NER → validate against universe | spaCy NER + regex + S&P500 lookup |
| NLP Sentiment Service | Classify each post: POSITIVE/NEGATIVE/NEUTRAL + confidence score | HuggingFace FinBERT-Tone (yiyanghkust/finbert-tone) |
| Strategy Engine | Aggregate to per-ticker Z-scores; generate LONG/SHORT/FLAT signals | Python + Pandas |
| Price Feed | Real-time bid/ask + OHLCV from IBKR | ib_async reqMktData |
| Risk Manager | Position size, daily loss limit, correlation, circuit breakers | Python microservice |
| Paper Trading Engine | Simulated execution with slippage; P&L tracking | Python + PostgreSQL |
| Live Execution Engine | IBKR order placement via ib_async; bracket orders | ib_async (ib-api-reloaded/ib_async) |

---

---

*[⬆ Back to main index](README.md)*
