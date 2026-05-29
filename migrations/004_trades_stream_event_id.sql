-- ============================================================
-- 004 Add stream_event_id to trades for idempotent inserts
-- The execution:events stream message ID is stored here so
-- the persistence service can skip duplicate deliveries on
-- crash-before-ack recovery.
-- ============================================================

ALTER TABLE trades ADD COLUMN IF NOT EXISTS stream_event_id VARCHAR(30) UNIQUE;
