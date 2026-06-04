-- Migration 009: add proactivity column to signals table
-- Stores p-factor (1.0 = signal led price move, 0.0 = reactive/crowd following price).
--
-- NOTE: ALTER TABLE requires a brief exclusive lock on the signals table.
-- If the app is actively writing, run this during a quiet moment or restart
-- the signal service first. The lock_timeout prevents an indefinite hang.

SET lock_timeout = '5s';

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS proactivity FLOAT;
