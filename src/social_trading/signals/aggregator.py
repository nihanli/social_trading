"""
SentimentAggregator — rolling window aggregation of SentimentResult objects.

Each SentimentResult is stored in a Redis ZSET:
  key:   sentiment:window:{ticker}
  score: unix timestamp (epoch seconds)
  value: SentimentResult serialised as JSON

This lets the service recover its window on restart and share state
across multiple processes.

Aggregation formula (design §4f):
  weight_i = engagement_i × authority_i × time_decay_i
  where:
    engagement_i = log1p(likes + 2 × reposts)
    authority_i  = log1p(author_followers)
    time_decay_i = exp(−λ × hours_since_classified)
  weighted_score = Σ(score_i × weight_i) / Σ(weight_i)

If all weights are zero (no engagement data), falls back to simple mean.

Design reference: docs/design/05-signal-generation.md §5b
                  docs/design/04-nlp-pipeline.md §4f
"""
from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SentimentResult
from social_trading.ingest.base import MENTION_HISTORY_TIER1_SOURCES

logger = logging.getLogger(__name__)

WINDOW_KEY = "sentiment:window:{ticker}"
# Keep up to 24 hours of results even if window is shorter (serves history page)
MAX_WINDOW_HOURS = 24.0
# Auto-expire the ZSET key 2 hours after the last write so stale tickers
# that leave the watchlist don't accumulate in Redis memory indefinitely.
_WINDOW_KEY_TTL_SEC = int((MAX_WINDOW_HOURS + 2) * 3600)  # 26 hours


@dataclass
class SentimentStats:
    """
    Aggregated statistics for one ticker over the rolling window.
    Produced by SentimentAggregator.get_stats().
    """
    ticker: str
    mean_score: float            # engagement-weighted ∈ [-1, 1]
    post_count: int              # total posts in window
    positive_count: int
    negative_count: int
    neutral_count: int
    sources: set[str]            # platforms that contributed
    source_scores: dict[str, float]  # per-platform weighted mean (for convergence)
    oldest_age_hours: float      # age of oldest post in window
    newest_age_hours: float      # age of newest post in window
    window_hours: float          # effective window length requested

    @property
    def direction(self) -> str:
        """Dominant direction of the aggregated sentiment."""
        if self.mean_score > 0.05:
            return "LONG"
        if self.mean_score < -0.05:
            return "SHORT"
        return "FLAT"


class SentimentAggregator:
    """
    Rolling window aggregator for sentiment results.

    Thread/process safe because all state is in Redis.
    Designed to be called from the async signal service loop.
    """

    def __init__(self, redis: aioredis.Redis, cfg: SystemConfig) -> None:
        self._redis = redis
        self._cfg = cfg

    # ── Write ─────────────────────────────────────────────────────────────────

    async def add(self, result: SentimentResult) -> None:
        """
        Store a SentimentResult in the rolling window.
        Trims entries older than MAX_WINDOW_HOURS automatically.
        """
        key = WINDOW_KEY.format(ticker=result.ticker)
        now = time.time()
        score = result.classified_at.timestamp() if result.classified_at else now

        # Serialise to JSON (Pydantic handles datetime → ISO string)
        payload = result.model_dump_json()

        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.zadd(key, {payload: score})
            # Trim entries older than MAX_WINDOW_HOURS
            cutoff = now - MAX_WINDOW_HOURS * 3600
            pipe.zremrangebyscore(key, 0, cutoff)
            # Refresh TTL so the key auto-expires if the ticker leaves the watchlist
            pipe.expire(key, _WINDOW_KEY_TTL_SEC)
            await pipe.execute()

    # ── Read ──────────────────────────────────────────────────────────────────

    async def get_stats(
        self, ticker: str, hours: float | None = None
    ) -> SentimentStats | None:
        """
        Compute aggregated statistics for a ticker over the last `hours`.
        Returns None if there are no results in the window.

        Args:
            ticker: Stock symbol.
            hours:  Window length. Defaults to cfg.mention_window_minutes / 60.
        """
        window_hours = hours if hours is not None else self._cfg.mention_window_minutes / 60.0
        key = WINDOW_KEY.format(ticker=ticker)
        now = time.time()
        cutoff = now - window_hours * 3600

        raw_entries = await self._redis.zrangebyscore(key, cutoff, "+inf")
        if not raw_entries:
            return None

        results = _deserialise_entries(raw_entries)
        if not results:
            return None

        return _compute_stats(results, ticker, window_hours, now, self._cfg.signal_decay_lambda)

    async def get_volume_zscore(self, ticker: str) -> float:
        """
        Read per-source mention histories and return the equal-weight average
        Z-score across continuously-polled Tier-1 sources (Bluesky, StockTwits).

        Twitter is intentionally excluded: it is only polled on Phase-1 spike
        events, so its history is a biased sample of spike-only counts. Using
        it as a baseline comparator would produce unreliable Z-scores.

        Sources with fewer than 24 baseline samples are excluded from the
        average so warm-up periods don't corrupt the signal.
        """
        zscores: list[float] = []
        for source in MENTION_HISTORY_TIER1_SOURCES:
            key = f"mention_history:{source}:{ticker}"
            raw = await self._redis.lrange(key, 0, -1)
            if len(raw) < 25:  # need at least 24 baseline + 1 current
                continue
            values = [float(v) for v in raw]
            current = values[-1]
            history = values[:-1]  # baseline excludes current sample
            n = len(history)
            mean = sum(history) / n
            variance = sum((v - mean) ** 2 for v in history) / n
            std = math.sqrt(variance)
            std = max(std, mean * 0.10, 1.0)
            zscores.append((current - mean) / std)

        if not zscores:
            return 0.0
        return sum(zscores) / len(zscores)

    async def active_tickers(self) -> list[str]:
        """
        Return tickers that have at least one result in the window.
        Uses a SCAN to find all sentiment:window:* keys.
        """
        tickers: list[str] = []
        async for key in self._redis.scan_iter("sentiment:window:*"):
            decoded = key.decode() if isinstance(key, bytes) else key
            ticker = decoded.split(":")[-1]
            tickers.append(ticker)
        return tickers

    def update_cfg(self, cfg: SystemConfig) -> None:
        self._cfg = cfg


# ── Pure helpers ──────────────────────────────────────────────────────────────

def _deserialise_entries(raw_entries: list[Any]) -> list[SentimentResult]:
    """Deserialise Redis ZSET members back to SentimentResult objects."""
    results: list[SentimentResult] = []
    for raw in raw_entries:
        try:
            data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
            results.append(SentimentResult.model_validate(data))
        except Exception as exc:
            logger.warning("Failed to deserialise SentimentResult: %s", exc)
    return results


def _engagement_weight(result: SentimentResult, now: float, decay_lambda: float) -> float:
    """
    Compute engagement × authority × time-decay weight for one result.

    engagement = log1p(likes + 2 × reposts)
    authority  = log1p(author_followers)
    time_decay = exp(−λ × hours_since_classified)
    """
    hours_ago = (now - result.classified_at.timestamp()) / 3600.0
    hours_ago = max(hours_ago, 0.0)

    engagement = math.log1p(result.likes + 2 * result.reposts)
    authority = math.log1p(result.author_followers)
    time_decay = math.exp(-decay_lambda * hours_ago)

    return engagement * authority * time_decay


def _compute_stats(
    results: list[SentimentResult],
    ticker: str,
    window_hours: float,
    now: float,
    decay_lambda: float,
) -> SentimentStats:
    """Pure computation — no I/O."""
    scores = [r.score for r in results]
    weights = [_engagement_weight(r, now, decay_lambda) for r in results]
    total_weight = sum(weights)

    if total_weight > 0:
        mean_score = sum(s * w for s, w in zip(scores, weights, strict=True)) / total_weight
    else:
        mean_score = sum(scores) / len(scores)

    # Per-source scores (for convergence calculation in generator)
    source_sums: dict[str, list[float]] = {}
    source_weights: dict[str, float] = {}
    for r, w in zip(results, weights, strict=True):
        src = r.source or "unknown"
        source_sums.setdefault(src, []).append(r.score * w)
        source_weights[src] = source_weights.get(src, 0.0) + w

    source_scores: dict[str, float] = {}
    for src, score_list in source_sums.items():
        sw = source_weights[src]
        source_scores[src] = sum(score_list) / sw if sw > 0 else (
            sum(r.score for r in results if (r.source or "unknown") == src) / len(score_list)
        )

    # Age stats
    ages_hours = [(now - r.classified_at.timestamp()) / 3600.0 for r in results]

    positive_count = sum(1 for r in results if r.score > 0.05)
    negative_count = sum(1 for r in results if r.score < -0.05)
    neutral_count = len(results) - positive_count - negative_count

    return SentimentStats(
        ticker=ticker,
        mean_score=round(mean_score, 6),
        post_count=len(results),
        positive_count=positive_count,
        negative_count=negative_count,
        neutral_count=neutral_count,
        sources={r.source for r in results if r.source},
        source_scores=source_scores,
        oldest_age_hours=round(max(ages_hours), 3),
        newest_age_hours=round(min(ages_hours), 3),
        window_hours=window_hours,
    )
