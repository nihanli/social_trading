"""Unit tests for SentimentAggregator."""
from __future__ import annotations

import time
from datetime import UTC, datetime

import fakeredis.aioredis
import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SentimentResult
from social_trading.signals.aggregator import (
    SentimentAggregator,
    _compute_stats,
    _engagement_weight,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
async def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=False)


@pytest.fixture
def cfg() -> SystemConfig:
    return SystemConfig(
        mention_window_minutes=60,
        signal_decay_lambda=0.1,
    )


@pytest.fixture
async def agg(redis, cfg):
    return SentimentAggregator(redis=redis, cfg=cfg)


def make_result(
    ticker: str = "AAPL",
    score: float = 0.6,
    source: str = "twitter",
    likes: int = 10,
    reposts: int = 2,
    author_followers: int = 500,
    post_id: str = "p1",
) -> SentimentResult:
    return SentimentResult(
        post_id=post_id,
        ticker=ticker,
        positive=max(score, 0.0),
        negative=max(-score, 0.0),
        neutral=0.1,
        score=score,
        model="finbert",
        source=source,
        likes=likes,
        reposts=reposts,
        author_followers=author_followers,
        classified_at=datetime.now(UTC),
    )


# ── add / get_stats ───────────────────────────────────────────────────────────

async def test_add_and_get_stats_basic(agg: SentimentAggregator) -> None:
    r = make_result(ticker="AAPL", score=0.7)
    await agg.add(r)
    r2 = make_result(ticker="AAPL", score=0.5, post_id="p2")
    await agg.add(r2)

    stats = await agg.get_stats("AAPL")
    assert stats is not None
    assert stats.post_count == 2
    assert stats.ticker == "AAPL"
    assert 0.4 < stats.mean_score < 0.8  # weighted mean between 0.5 and 0.7


async def test_get_stats_returns_none_when_empty(agg: SentimentAggregator) -> None:
    stats = await agg.get_stats("TSLA")
    assert stats is None


async def test_get_stats_source_tracking(agg: SentimentAggregator) -> None:
    await agg.add(make_result(source="twitter", post_id="p1"))
    await agg.add(make_result(source="reddit", post_id="p2"))
    await agg.add(make_result(source="stocktwits", post_id="p3"))

    stats = await agg.get_stats("AAPL")
    assert stats is not None
    assert stats.sources == {"twitter", "reddit", "stocktwits"}


async def test_get_stats_direction_positive(agg: SentimentAggregator) -> None:
    for i in range(3):
        await agg.add(make_result(score=0.7, post_id=f"p{i}"))
    stats = await agg.get_stats("AAPL")
    assert stats is not None
    assert stats.direction == "LONG"


async def test_get_stats_direction_negative(agg: SentimentAggregator) -> None:
    for i in range(3):
        await agg.add(make_result(score=-0.7, post_id=f"p{i}"))
    stats = await agg.get_stats("AAPL")
    assert stats is not None
    assert stats.direction == "SHORT"


async def test_get_stats_positive_negative_counts(agg: SentimentAggregator) -> None:
    await agg.add(make_result(score=0.6, post_id="p1"))
    await agg.add(make_result(score=0.8, post_id="p2"))
    await agg.add(make_result(score=-0.5, post_id="p3"))
    stats = await agg.get_stats("AAPL")
    assert stats is not None
    assert stats.positive_count == 2
    assert stats.negative_count == 1


async def test_get_stats_per_source_scores(agg: SentimentAggregator) -> None:
    await agg.add(make_result(source="twitter", score=0.8, post_id="p1"))
    await agg.add(make_result(source="reddit", score=-0.4, post_id="p2"))
    stats = await agg.get_stats("AAPL")
    assert stats is not None
    assert "twitter" in stats.source_scores
    assert "reddit" in stats.source_scores
    assert stats.source_scores["twitter"] > 0
    assert stats.source_scores["reddit"] < 0


# ── Volume Z-score ────────────────────────────────────────────────────────────

async def test_get_volume_zscore_insufficient_history(
    agg: SentimentAggregator, redis
) -> None:
    # Only 5 data points → < 24 minimum
    for v in [100, 110, 105, 108, 102]:
        await redis.rpush("mention_history:AAPL", v)
    zscore = await agg.get_volume_zscore("AAPL")
    assert zscore == 0.0


async def test_get_volume_zscore_flat_history(
    agg: SentimentAggregator, redis
) -> None:
    # Flat history + tiny current → no spike
    for _ in range(30):
        await redis.rpush("mention_history:TSLA", 100)
    zscore = await agg.get_volume_zscore("TSLA")
    # With flat history and last=100 equal to mean → z=0
    assert abs(zscore) < 0.5


async def test_get_volume_zscore_spike(agg: SentimentAggregator, redis) -> None:
    # 30 values at 10, then spike to 500
    for _ in range(29):
        await redis.rpush("mention_history:NVDA", 10)
    await redis.rpush("mention_history:NVDA", 500)
    zscore = await agg.get_volume_zscore("NVDA")
    assert zscore > 2.0  # clearly a spike


async def test_get_volume_zscore_missing_key(agg: SentimentAggregator) -> None:
    zscore = await agg.get_volume_zscore("NOTEXIST")
    assert zscore == 0.0


# ── active_tickers ────────────────────────────────────────────────────────────

async def test_active_tickers(agg: SentimentAggregator) -> None:
    await agg.add(make_result(ticker="AAPL", post_id="p1"))
    await agg.add(make_result(ticker="TSLA", post_id="p2"))
    active = await agg.active_tickers()
    assert "AAPL" in active
    assert "TSLA" in active


# ── Pure helper tests ─────────────────────────────────────────────────────────

def test_engagement_weight_no_engagement() -> None:
    """Post with zero likes/reposts/followers still gets a weight from time decay."""
    r = make_result(likes=0, reposts=0, author_followers=0)
    now = r.classified_at.timestamp()
    w = _engagement_weight(r, now, decay_lambda=0.1)
    # log1p(0) * log1p(0) = 0 → weight = 0 (pure time decay × 0)
    assert w == 0.0


def test_engagement_weight_with_engagement() -> None:
    r = make_result(likes=100, reposts=20, author_followers=5000)
    now = r.classified_at.timestamp()
    w = _engagement_weight(r, now, decay_lambda=0.1)
    assert w > 0.0


def test_engagement_weight_decays_with_age() -> None:
    r = make_result(likes=50, reposts=10, author_followers=1000)
    now = r.classified_at.timestamp()
    w_fresh = _engagement_weight(r, now, decay_lambda=0.1)
    w_old = _engagement_weight(r, now + 7 * 3600, decay_lambda=0.1)
    assert w_fresh > w_old


def test_compute_stats_fallback_to_simple_mean() -> None:
    """When all weights are 0 (no engagement data), use simple mean."""
    results = [
        make_result(score=0.8, likes=0, reposts=0, author_followers=0, post_id="p1"),
        make_result(score=0.4, likes=0, reposts=0, author_followers=0, post_id="p2"),
    ]
    now = time.time()
    stats = _compute_stats(results, "AAPL", window_hours=1.0, now=now, decay_lambda=0.1)
    assert stats.mean_score == pytest.approx(0.6, abs=0.01)
