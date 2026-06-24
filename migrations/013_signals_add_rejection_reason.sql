-- Migration 013: Add rejection_reason column to signals table
-- Stores the human-readable reason a signal was not approved or not executed.
-- NULL means the signal was either approved+executed or rejection not yet recorded.

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS rejection_reason TEXT;

COMMENT ON COLUMN signals.rejection_reason IS
    'Why this signal was not approved or not executed. '
    'Set by risk_service (stale, cooldown, liquidity, sizer, adv_pct, atr_zero, sl_invalid) '
    'or execution_service (expired, halted, position_already_open).';
