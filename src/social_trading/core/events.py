"""
Redis Stream event schemas — typed dictionaries for each stream.

These define the exact field names published to / consumed from each stream.
All values are strings (Redis Streams serialize everything to bytes/str).
Services must serialize to these shapes before publishing and
deserialize from them after consuming.
"""
from __future__ import annotations

from typing import TypedDict


# raw_social  ────────────────────────────────────────────────────────────────
class RawSocialEvent(TypedDict):
    id: str
    source: str           # twitter | reddit | stocktwits | lunarcrush
    ticker: str
    text: str
    author_id: str
    author_followers: str  # int as str
    author_account_age_days: str
    author_following: str
    post_count_30d: str
    likes: str
    reposts: str
    is_original: str       # "1" | "0"
    url: str
    collected_at: str      # ISO-8601


# sentiment_signals  ─────────────────────────────────────────────────────────
class SentimentEvent(TypedDict):
    post_id: str
    ticker: str
    positive: str          # float as str
    negative: str
    neutral: str
    score: str
    model: str
    latency_ms: str
    classified_at: str     # ISO-8601


# market_data  ───────────────────────────────────────────────────────────────
class MarketDataEvent(TypedDict):
    ticker: str
    last: str              # float as str
    bid: str
    ask: str
    volume: str
    avg_volume_30d: str
    atr_14: str
    vix: str
    timestamp: str         # ISO-8601


# strategy_signals  ──────────────────────────────────────────────────────────
class SignalEvent(TypedDict):
    ticker: str
    direction: str         # LONG | SHORT
    quality_score: str     # float as str
    sentiment_score: str
    volume_z_score: str
    momentum: str
    convergence: str
    source_post_count: str
    generated_at: str      # ISO-8601


# selected_signals  ──────────────────────────────────────────────────────────
class ApprovedSignalEvent(TypedDict):
    ticker: str
    direction: str
    quality_score: str
    sentiment_score: str
    volume_z_score: str
    momentum: str
    convergence: str
    source_post_count: str
    generated_at: str
    quantity: str          # int as str — added by risk service
    stop_loss: str         # float as str
    take_profit: str


# enrichment:requests  ────────────────────────────────────────────────────────
class EnrichmentRequestEvent(TypedDict):
    ticker: str
    phase1_score: str      # float as str — quality score that passed Phase 1
    requested_at: str      # ISO-8601


# ── Stream name constants ─────────────────────────────────────────────────────
STREAM_RAW_SOCIAL = "raw_social"
STREAM_SENTIMENT = "sentiment_signals"
STREAM_MARKET_DATA = "market_data"       # reserved — not yet published
STREAM_STRATEGY_SIGNALS = "strategy_signals"
STREAM_SELECTED_SIGNALS = "selected_signals"
STREAM_EXEC_EVENTS = "execution:events"  # position open/close lifecycle events
STREAM_ENRICHMENT_REQUESTS = "enrichment:requests"  # Phase-1 → Tier-2 enrichment triggers
STREAM_SIGNAL_REJECTIONS = "signal:rejections"  # risk/exec rejection reasons → persistence

ALL_STREAMS = [
    STREAM_RAW_SOCIAL,
    STREAM_SENTIMENT,
    STREAM_MARKET_DATA,
    STREAM_STRATEGY_SIGNALS,
    STREAM_SELECTED_SIGNALS,
    STREAM_EXEC_EVENTS,
    STREAM_ENRICHMENT_REQUESTS,
    STREAM_SIGNAL_REJECTIONS,
]

# Maximum entries to retain per stream (approximate trim — Redis `MAXLEN ~`).
# Sized to cover several hours of consumer lag at peak ingest rates.
#
#   raw_social / sentiment_signals : ~35K posts/hr (Bluesky peak) → 250K ≈ 7 hrs
#   strategy_signals               : 1 eval/ticker/min × 300 tickers → 25K ≈ 1.4 hrs
#   selected_signals               : risk-filtered subset → 10K
#   execution:events               : at most 1 event per position open/close;
#                                    50K covers thousands of round-trip trades
#   market_data                    : reserved stream, not yet published
STREAM_MAXLEN: dict[str, int] = {
    STREAM_RAW_SOCIAL:             250_000,
    STREAM_SENTIMENT:              250_000,
    STREAM_MARKET_DATA:             50_000,
    STREAM_STRATEGY_SIGNALS:        25_000,
    STREAM_SELECTED_SIGNALS:        10_000,
    STREAM_EXEC_EVENTS:             50_000,
    STREAM_ENRICHMENT_REQUESTS:      5_000,
    STREAM_SIGNAL_REJECTIONS:       50_000,
}
