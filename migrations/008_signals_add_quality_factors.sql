-- Migration 008: add momentum and convergence columns to signals table
-- These store the individual quality-score factors so the UI can display
-- a full breakdown (v, s, p, m, c) without re-deriving from raw values.

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS momentum    FLOAT,
    ADD COLUMN IF NOT EXISTS convergence FLOAT;
