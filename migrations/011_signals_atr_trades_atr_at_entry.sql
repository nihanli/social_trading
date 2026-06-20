-- ============================================================
-- 011 ATR columns for backtest engine
-- signals.atr          — ATR-14 at signal generation time
-- trades.atr_at_entry  — copied from linked signal on trade open
-- ============================================================

ALTER TABLE signals ADD COLUMN IF NOT EXISTS atr FLOAT;

ALTER TABLE trades ADD COLUMN IF NOT EXISTS atr_at_entry FLOAT;
