-- ============================================================
-- 003 Add missing columns to positions table
-- stop_loss, take_profit, high_water_mark are tracked by the
-- execution engine but were missing from the initial schema.
-- Also add pnl_pct to trades for easier reporting.
-- ============================================================

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS stop_loss       DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS take_profit     DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS high_water_mark DOUBLE PRECISION DEFAULT 0.0;

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS pnl_pct DOUBLE PRECISION;
