-- ============================================================
-- 002 Config Runs Table
-- Stores daily SystemConfig snapshots + session performance.
-- Powers the §17 parameter optimization feedback loop.
-- ============================================================

CREATE TABLE IF NOT EXISTS config_runs (
    id                  SERIAL PRIMARY KEY,
    run_date            DATE          NOT NULL,
    mode                VARCHAR(10)   NOT NULL,
    config_snapshot     JSONB         NOT NULL,
    config_hash         VARCHAR(16),

    -- Session performance
    total_pnl               NUMERIC(12,2),
    total_trades            INT,
    win_count               INT,
    win_rate                NUMERIC(5,4),
    sharpe_ratio            NUMERIC(8,4),
    max_drawdown            NUMERIC(6,4),
    avg_hold_hours          NUMERIC(6,2),
    profit_factor           NUMERIC(8,4),

    -- Exit reason breakdown
    exits_take_profit         INT DEFAULT 0,
    exits_time_stop           INT DEFAULT 0,
    exits_atr_stop            INT DEFAULT 0,
    exits_trailing_stop       INT DEFAULT 0,
    exits_sentiment_reversal  INT DEFAULT 0,
    exits_mention_decay       INT DEFAULT 0,
    exits_manual              INT DEFAULT 0,

    -- Signal funnel
    signals_generated   INT,
    signals_executed    INT,
    avg_signal_quality  NUMERIC(5,4),
    avg_mention_zscore  NUMERIC(6,2),

    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_config_runs_date
    ON config_runs(run_date DESC);
CREATE INDEX IF NOT EXISTS idx_config_runs_mode
    ON config_runs(mode);
CREATE INDEX IF NOT EXISTS idx_config_runs_sharpe
    ON config_runs(sharpe_ratio DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_config_runs_hash
    ON config_runs(config_hash);
