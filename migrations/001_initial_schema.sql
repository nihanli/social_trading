-- ============================================================
-- 001 Initial Schema
-- Social Media Momentum Trading System
-- ============================================================

-- ── Social Media Raw Data ────────────────────────────────────
CREATE TABLE IF NOT EXISTS social_raw (
    id          BIGSERIAL PRIMARY KEY,
    source      VARCHAR(20)  NOT NULL,
    post_id     VARCHAR(100) UNIQUE NOT NULL,
    ticker      VARCHAR(20),
    raw_text    TEXT,
    author      VARCHAR(100),
    followers   INT DEFAULT 0,
    likes       INT DEFAULT 0,
    retweets    INT DEFAULT 0,
    upvotes     INT DEFAULT 0,
    flair       VARCHAR(50),
    created_at  TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_social_raw_ticker_time
    ON social_raw(ticker, created_at DESC);

-- ── Sentiment Scores (per post) ──────────────────────────────
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id          BIGSERIAL PRIMARY KEY,
    post_id     VARCHAR(100) REFERENCES social_raw(post_id),
    ticker      VARCHAR(20),
    pos_prob    FLOAT,
    neg_prob    FLOAT,
    neu_prob    FLOAT,
    label       VARCHAR(10),
    model       VARCHAR(50),
    scored_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sentiment_scores_ticker
    ON sentiment_scores(ticker, scored_at DESC);

-- ── Aggregated Sentiment (time-bucketed) ─────────────────────
CREATE TABLE IF NOT EXISTS sentiment_aggregates (
    ticker          VARCHAR(20)  NOT NULL,
    window_start    TIMESTAMPTZ  NOT NULL,
    window_minutes  INT          NOT NULL,
    avg_sentiment   FLOAT,
    weighted_score  FLOAT,
    post_count      INT,
    mention_zscore  FLOAT,
    signal_quality  FLOAT,
    PRIMARY KEY (ticker, window_start, window_minutes)
);
CREATE INDEX IF NOT EXISTS idx_sent_agg_ticker_time
    ON sentiment_aggregates(ticker, window_start DESC);

-- ── Market Data (OHLCV) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL,
    timestamp   TIMESTAMPTZ  NOT NULL,
    timeframe   VARCHAR(10)  NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      DOUBLE PRECISION,
    UNIQUE(symbol, timeframe, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_market_data_sym_tf_ts
    ON market_data(symbol, timeframe, timestamp DESC);

-- ── Trading Signals ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals (
    id              BIGSERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ  NOT NULL,
    ticker          VARCHAR(20)  NOT NULL,
    strategy        VARCHAR(100),
    direction       VARCHAR(10),
    confidence      FLOAT,
    sentiment_score FLOAT,
    mention_zscore  FLOAT,
    quality_score   FLOAT,
    approved        BOOLEAN DEFAULT FALSE,
    executed        BOOLEAN DEFAULT FALSE,
    generated_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_signals_ticker_time
    ON signals(ticker, generated_at DESC);

-- ── Trades ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL PRIMARY KEY,
    signal_id       BIGINT REFERENCES signals(id),
    ticker          VARCHAR(20)  NOT NULL,
    strategy        VARCHAR(100),
    direction       VARCHAR(10),
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
    status          VARCHAR(20) DEFAULT 'open',
    opened_at       TIMESTAMPTZ DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    mode            VARCHAR(10) DEFAULT 'paper'
);
CREATE INDEX IF NOT EXISTS idx_trades_ticker     ON trades(ticker);
CREATE INDEX IF NOT EXISTS idx_trades_status     ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_opened_at  ON trades(opened_at DESC);

-- ── Positions (live state — upsert) ─────────────────────────
CREATE TABLE IF NOT EXISTS positions (
    ticker          VARCHAR(20) UNIQUE,
    direction       VARCHAR(10),
    shares          INT,
    entry_price     DOUBLE PRECISION,
    unrealized_pnl  DOUBLE PRECISION DEFAULT 0,
    strategy        VARCHAR(100),
    opened_at       TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Equity Curve ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS account_equity (
    id          BIGSERIAL,
    timestamp   TIMESTAMPTZ DEFAULT NOW(),
    equity      DOUBLE PRECISION,
    mode        VARCHAR(10)
);
CREATE INDEX IF NOT EXISTS idx_equity_timestamp
    ON account_equity(timestamp DESC);
