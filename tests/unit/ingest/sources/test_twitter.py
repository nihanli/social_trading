"""Unit tests for TwitterDataSource."""
from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.exceptions import RateLimitError
from social_trading.ingest.sources.twitter import (
    MENTION_HISTORY_KEY,
    TwitterDataSource,
)


def make_counts_response(count: int = 10) -> httpx.Response:
    return httpx.Response(
        200,
        json={"data": [], "meta": {"total_tweet_count": count}},
    )


def make_search_response(n: int = 3) -> httpx.Response:
    tweets = [
        {
            "id": f"t{i}",
            "text": f"$AAPL is great #{i}",
            "author_id": f"u{i}",
            "created_at": "2024-01-15T10:00:00Z",
            "public_metrics": {"like_count": i * 10, "retweet_count": i},
        }
        for i in range(n)
    ]
    users = [
        {
            "id": f"u{i}",
            "public_metrics": {"followers_count": 1000 * i, "following_count": 500},
            "created_at": "2020-01-01T00:00:00Z",
        }
        for i in range(n)
    ]
    return httpx.Response(
        200,
        json={"data": tweets, "includes": {"users": users}},
    )


def make_rate_limit_response() -> httpx.Response:
    return httpx.Response(
        429,
        headers={"x-rate-limit-reset": str(int(time.time()) + 60)},
        json={"title": "Too Many Requests"},
    )


@pytest.fixture
def mock_http():
    return AsyncMock(spec=httpx.AsyncClient)


@pytest.fixture
async def twitter(redis, cfg, mock_http):
    return TwitterDataSource(
        redis=redis, cfg=cfg, bearer_token="test-token", http_client=mock_http
    )


# ── poll — no spike ───────────────────────────────────────────────────────────

async def test_poll_no_spike_returns_empty(twitter, mock_http, redis, cfg):
    """Below Z-score threshold → no Tier-2 pull → no posts."""
    cfg.spike_zscore_threshold = 2.0
    mock_http.get = AsyncMock(return_value=make_counts_response(count=5))

    # Build 30+ history points with mean=5, std≈0 — zscore will be ~0
    key = MENTION_HISTORY_KEY.format(ticker="AAPL")
    for _ in range(30):
        await redis.rpush(key, 5)

    posts = await twitter.poll(["AAPL"])
    assert posts == []
    # Only Counts endpoint should have been called (1 GET)
    assert mock_http.get.call_count == 1


# ── poll — spike detected ─────────────────────────────────────────────────────

async def test_poll_spike_triggers_tier2(twitter, mock_http, redis, cfg):
    """Above Z-score threshold → Tier-2 search is called → posts returned."""
    cfg.spike_zscore_threshold = 2.0

    # Counts endpoint returns high spike value
    # Search endpoint returns 3 tweets
    mock_http.get = AsyncMock(
        side_effect=[make_counts_response(count=1000), make_search_response(n=3)]
    )

    # Build low baseline history → spike will be detected
    key = MENTION_HISTORY_KEY.format(ticker="AAPL")
    for _ in range(30):
        await redis.rpush(key, 5)

    posts = await twitter.poll(["AAPL"])
    assert len(posts) == 3
    assert all(p.ticker == "AAPL" for p in posts)
    assert all(p.source == "twitter" for p in posts)
    # Both Counts + Search were called
    assert mock_http.get.call_count == 2


# ── spike detection ───────────────────────────────────────────────────────────

async def test_check_spike_insufficient_history(twitter, redis):
    """< 24 history points → never fires (no baseline yet)."""
    key = MENTION_HISTORY_KEY.format(ticker="NVDA")
    for _ in range(10):
        await redis.rpush(key, 5)
    is_spike = await twitter._check_spike("NVDA", current_count=999)
    assert not is_spike


async def test_check_spike_fires_above_threshold(twitter, redis, cfg):
    cfg.spike_zscore_threshold = 2.0
    key = MENTION_HISTORY_KEY.format(ticker="TSLA")
    for _ in range(30):
        await redis.rpush(key, 10)
    # current count far above baseline → spike
    is_spike = await twitter._check_spike("TSLA", current_count=500)
    assert is_spike


async def test_check_spike_does_not_fire_below_threshold(twitter, redis, cfg):
    cfg.spike_zscore_threshold = 2.0
    key = MENTION_HISTORY_KEY.format(ticker="MSFT")
    for _ in range(30):
        await redis.rpush(key, 100)
    # current count close to baseline → no spike
    is_spike = await twitter._check_spike("MSFT", current_count=102)
    assert not is_spike


# ── rate limiting ─────────────────────────────────────────────────────────────

async def test_rate_limit_stops_polling(twitter, mock_http, redis):
    """On 429, poll should raise RateLimitError and stop processing tickers."""
    mock_http.get = AsyncMock(return_value=make_rate_limit_response())
    # Should not raise — error is handled internally, remaining tickers skipped
    posts = await twitter.poll(["AAPL", "TSLA", "NVDA"])
    assert posts == []
    # Only one GET call (stopped after 429)
    assert mock_http.get.call_count == 1


# ── post normalisation ────────────────────────────────────────────────────────

async def test_pull_spike_posts_normalises_fields(twitter, mock_http, redis):
    mock_http.get = AsyncMock(return_value=make_search_response(n=2))
    posts = await twitter._pull_spike_posts("AAPL")
    assert len(posts) == 2
    for post in posts:
        assert post.source == "twitter"
        assert post.ticker == "AAPL"
        assert post.id.startswith("t")


async def test_posts_published_to_stream(twitter, mock_http, redis):
    """Posts from Tier-2 pull should appear in raw_social Redis stream."""
    mock_http.get = AsyncMock(return_value=make_search_response(n=2))
    await twitter._pull_spike_posts("AAPL")
    stream_len = await redis.xlen("raw_social")
    assert stream_len == 2


# ── trending ──────────────────────────────────────────────────────────────────

async def test_get_trending_returns_empty(twitter):
    """X API has no free trending endpoint — always returns []."""
    tickers = await twitter.get_trending()
    assert tickers == []
