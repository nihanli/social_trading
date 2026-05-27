-- ============================================================
-- 004 Add source column to sentiment_scores
-- Needed for "posts by source" breakdown in Streamlit UI.
-- ============================================================

ALTER TABLE sentiment_scores
    ADD COLUMN IF NOT EXISTS source VARCHAR(20);
