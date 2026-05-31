-- Migration 007: add signal_phase column to signals table
-- "phase1" = fired by free/Tier-1 sources only (lower threshold)
-- "phase2" = fired after Tier-2 enrichment (higher threshold)
-- NULL = legacy rows created before two-phase pipeline

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS signal_phase VARCHAR(10);

COMMENT ON COLUMN signals.signal_phase IS
    'Two-phase pipeline: phase1 (free sources) or phase2 (all sources incl. paid). NULL = legacy.';

CREATE INDEX IF NOT EXISTS idx_signals_phase
    ON signals(signal_phase)
    WHERE signal_phase IS NOT NULL;
