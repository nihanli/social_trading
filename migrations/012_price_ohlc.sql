-- ============================================================
-- 012 price_ohlc — pre-stored OHLC bars for backtest engine
-- timeframe: '1d' (daily) | '5m' (intraday 5-minute bars)
-- Retained 90 days; pruned nightly when ticker has no live signals.
-- ============================================================

CREATE TABLE IF NOT EXISTS price_ohlc (
    id           BIGSERIAL PRIMARY KEY,
    ticker       VARCHAR(20)         NOT NULL,
    bar_datetime TIMESTAMPTZ         NOT NULL,
    timeframe    VARCHAR(10)         NOT NULL,
    open         DOUBLE PRECISION,
    high         DOUBLE PRECISION,
    low          DOUBLE PRECISION,
    close        DOUBLE PRECISION,
    volume       BIGINT,
    source       VARCHAR(20),
    fetched_at   TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_price_ohlc UNIQUE (ticker, bar_datetime, timeframe)
);

CREATE INDEX IF NOT EXISTS idx_price_ohlc_lookup
    ON price_ohlc (ticker, timeframe, bar_datetime DESC);
