-- ============================================================
-- 006  Signals: partial indexes + signal_id in trades
--
-- 1. Partial indexes on signals.approved / signals.executed
--    for fast COUNT(*) FILTER (WHERE approved) queries in the
--    Streamlit UI (otherwise a full table scan).
--
-- 2. Index on trades.signal_id for JOIN performance.
-- ============================================================

-- Partial index: only rows where approved=TRUE (small, fast)
CREATE INDEX IF NOT EXISTS idx_signals_approved
    ON signals (ticker, generated_at DESC)
    WHERE approved = TRUE;

-- Partial index: only rows where executed=TRUE (very small)
CREATE INDEX IF NOT EXISTS idx_signals_executed
    ON signals (ticker, generated_at DESC)
    WHERE executed = TRUE;

-- Index on trades.signal_id (FK lookup)
CREATE INDEX IF NOT EXISTS idx_trades_signal_id
    ON trades (signal_id)
    WHERE signal_id IS NOT NULL;
