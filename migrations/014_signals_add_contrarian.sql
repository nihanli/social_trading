-- Migration 014: Add contrarian column to signals table
-- Stores whether the signal was generated in contrarian mode (direction inverted vs sentiment).
-- NULL / FALSE means normal mode (direction follows sentiment).

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS contrarian BOOLEAN DEFAULT FALSE;

COMMENT ON COLUMN signals.contrarian IS
    'TRUE if the signal was generated with contrarian_mode=True '
    '(trade direction is the inverse of the social sentiment signal). '
    'FALSE / NULL = normal mode.';
