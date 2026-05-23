## 9. Database Schema

```sql
-- ============================================================
-- SOCIAL MEDIA RAW DATA
-- ============================================================
CREATE TABLE social_raw (
    id          BIGSERIAL PRIMARY KEY,
    source      VARCHAR(20)  NOT NULL,   -- 'twitter', 'reddit', 'stocktwits'
    post_id     VARCHAR(100) UNIQUE NOT NULL,
    ticker      VARCHAR(20),
    raw_text    TEXT,
    author      VARCHAR(100),
    followers   INT DEFAULT 0,
    likes       INT DEFAULT 0,
    retweets    INT DEFAULT 0,
    upvotes     INT DEFAULT 0,
    flair       VARCHAR(50),             -- Reddit: DD, YOLO, Gain, Loss
    created_at  TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_social_raw_ticker_time ON social_raw(ticker, created_at DESC);

-- ============================================================
-- SENTIMENT SCORES (per post)
-- ============================================================
CREATE TABLE sentiment_scores (
    id          BIGSERIAL PRIMARY KEY,
    post_id     VARCHAR(100) REFERENCES social_raw(post_id),
    ticker      VARCHAR(20),
    pos_prob    FLOAT,
    neg_prob    FLOAT,
    neu_prob    FLOAT,
    label       VARCHAR(10),    -- positive/negative/neutral
    model       VARCHAR(50),    -- finbert-tone, vader, etc.
    scored_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- AGGREGATED SENTIMENT (time-bucketed signals)
-- ============================================================
CREATE TABLE sentiment_aggregates (
    ticker          VARCHAR(20)  NOT NULL,
    window_start    TIMESTAMPTZ  NOT NULL,
    window_minutes  INT          NOT NULL,   -- 15, 60, 240, 1440
    avg_sentiment   FLOAT,
    weighted_score  FLOAT,      -- engagement-weighted
    post_count      INT,
    mention_zscore  FLOAT,      -- Z-score vs 30-day baseline
    signal_quality  FLOAT,      -- composite quality score 0-1
    PRIMARY KEY (ticker, window_start, window_minutes)
);
CREATE INDEX idx_sent_agg_ticker_time ON sentiment_aggregates(ticker, window_start DESC);

-- ============================================================
-- MARKET DATA (OHLCV)
-- ============================================================
CREATE TABLE market_data (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL,
    timestamp   TIMESTAMPTZ  NOT NULL,
    timeframe   VARCHAR(10)  NOT NULL,   -- 1m, 5m, 15m, 1h, 1d
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      DOUBLE PRECISION,
    UNIQUE(symbol, timeframe, timestamp)
);
CREATE INDEX idx_market_data_sym_tf_ts ON market_data(symbol, timeframe, timestamp DESC);

-- ============================================================
-- TRADING SIGNALS
-- ============================================================
CREATE TABLE signals (
    id              BIGSERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ  NOT NULL,
    ticker          VARCHAR(20)  NOT NULL,
    strategy        VARCHAR(100),       -- e.g., 'reddit_wsb_momentum'
    direction       VARCHAR(10),        -- BUY / SELL / FLAT
    confidence      FLOAT,
    sentiment_score FLOAT,
    mention_zscore  FLOAT,
    approved        BOOLEAN DEFAULT FALSE,  -- Risk manager approval
    executed        BOOLEAN DEFAULT FALSE,
    generated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- TRADES
-- ============================================================
CREATE TABLE trades (
    id              BIGSERIAL PRIMARY KEY,
    signal_id       BIGINT REFERENCES signals(id),
    ticker          VARCHAR(20)  NOT NULL,
    strategy        VARCHAR(100),
    direction       VARCHAR(10),    -- LONG / SHORT
    shares          INT,
    entry_price     DOUBLE PRECISION,
    exit_price      DOUBLE PRECISION,
    stop_price      DOUBLE PRECISION,
    target_price    DOUBLE PRECISION,
    pnl             DOUBLE PRECISION,
    fees            DOUBLE PRECISION,
    net_pnl         DOUBLE PRECISION,
    entry_reason    VARCHAR(200),
    exit_reason     VARCHAR(200),
    status          VARCHAR(20) DEFAULT 'open',   -- open/closed/cancelled
    opened_at       TIMESTAMPTZ DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    mode            VARCHAR(10) DEFAULT 'paper'   -- paper / live
);

-- ============================================================
-- POSITIONS (live state — upsert)
-- ============================================================
CREATE TABLE positions (
    ticker          VARCHAR(20) UNIQUE,
    direction       VARCHAR(10),
    shares          INT,
    entry_price     DOUBLE PRECISION,
    unrealized_pnl  DOUBLE PRECISION DEFAULT 0,
    strategy        VARCHAR(100),
    opened_at       TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- EQUITY CURVE
-- ============================================================
CREATE TABLE account_equity (
    id          BIGSERIAL,
    timestamp   TIMESTAMPTZ DEFAULT NOW(),
    equity      DOUBLE PRECISION,
    mode        VARCHAR(10)    -- paper / live
);

-- ============================================================
-- CONFIG RUNS  (parameter optimization feedback loop — §17)
-- Snapshot of SystemConfig + session performance saved at EOD.
-- Powers the Optimize page in Streamlit (§15 pages/6_optimize.py).
-- ============================================================
CREATE TABLE config_runs (
    id                  SERIAL PRIMARY KEY,
    run_date            DATE          NOT NULL,
    mode                VARCHAR(10)   NOT NULL,          -- 'paper' | 'live'
    config_snapshot     JSONB         NOT NULL,          -- full SystemConfig as JSON
    config_hash         VARCHAR(16),                     -- first 16 chars of MD5 for quick lookup

    -- Session performance
    total_pnl           NUMERIC(12,2),
    total_trades        INT,
    win_count           INT,
    win_rate            NUMERIC(5,4),
    sharpe_ratio        NUMERIC(8,4),
    max_drawdown        NUMERIC(6,4),
    avg_hold_hours      NUMERIC(6,2),
    profit_factor       NUMERIC(8,4),   -- gross_profit / abs(gross_loss)

    -- Exit reason breakdown (used for auto-suggestions)
    exits_take_profit         INT DEFAULT 0,
    exits_time_stop           INT DEFAULT 0,
    exits_atr_stop            INT DEFAULT 0,
    exits_trailing_stop       INT DEFAULT 0,
    exits_sentiment_reversal  INT DEFAULT 0,
    exits_mention_decay       INT DEFAULT 0,
    exits_manual              INT DEFAULT 0,

    -- Signal funnel stats
    signals_generated   INT,
    signals_executed    INT,
    avg_signal_quality  NUMERIC(5,4),
    avg_mention_zscore  NUMERIC(6,2),

    created_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_config_runs_date      ON config_runs(run_date DESC);
CREATE INDEX idx_config_runs_mode      ON config_runs(mode);
CREATE INDEX idx_config_runs_sharpe    ON config_runs(sharpe_ratio DESC NULLS LAST);
CREATE INDEX idx_config_runs_hash      ON config_runs(config_hash);
```

[^21]: ashwini-singhh/crypto_trading_agent:db/init.sql (verified schema); extended for social signals

---

---

*[⬆ Back to main index](README.md)*
