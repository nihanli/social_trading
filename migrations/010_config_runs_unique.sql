-- ============================================================
-- 010 config_runs unique constraint
-- Prevents duplicate EOD snapshots on service restart.
-- UPSERT (INSERT … ON CONFLICT DO UPDATE) relies on this.
-- ============================================================

ALTER TABLE config_runs
    DROP CONSTRAINT IF EXISTS uq_config_runs_date_mode;

ALTER TABLE config_runs
    ADD CONSTRAINT uq_config_runs_date_mode UNIQUE (run_date, mode);
