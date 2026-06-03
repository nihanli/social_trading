"""Unit tests for GoogleTrendsDataSource."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.ingest.sources.google_trends import (
    GoogleTrendsDataSource,
    _LAST_FETCH_KEY,
    _RATE_LIMIT_SEC,
)


@pytest.fixture
async def watchlist(redis, cfg):
    from social_trading.ingest.watchlist.manager import WatchlistManager
    return WatchlistManager(redis=redis, cfg=cfg)


@pytest.fixture
async def source(redis, cfg, watchlist):
    return GoogleTrendsDataSource(redis=redis, cfg=cfg, watchlist=watchlist)


# ── poll ──────────────────────────────────────────────────────────────────────

async def test_poll_always_returns_empty(source):
    """Discovery-only — poll() must never return SocialPost objects."""
    posts = await source.poll(["AAPL", "TSLA"])
    assert posts == []


# ── get_trending — rate limiting ──────────────────────────────────────────────

async def test_get_trending_skips_when_rate_limited(source, redis):
    """Should return [] immediately if called within the 55-min window."""
    await redis.set(_LAST_FETCH_KEY, str(time.time()))  # just fetched
    with patch(
        "social_trading.ingest.sources.google_trends._fetch_interest_sync"
    ) as mock_fetch:
        result = await source.get_trending()
    assert result == []
    mock_fetch.assert_not_called()


async def test_get_trending_proceeds_after_rate_limit_window(source, redis, cfg):
    """Should proceed when last fetch was > 55 minutes ago."""
    old_ts = time.time() - _RATE_LIMIT_SEC - 10
    await redis.set(_LAST_FETCH_KEY, str(old_ts))

    cfg.seed_tickers = ["AAPL", "TSLA"]
    await cfg.save(redis)

    with patch(
        "social_trading.ingest.sources.google_trends._fetch_interest_sync",
        return_value={"AAPL": 80, "TSLA": 30},
    ):
        result = await source.get_trending()

    assert "AAPL" in result   # above threshold (80 >= 70)
    assert "TSLA" not in result  # below threshold (30 < 70)


async def test_get_trending_no_seed_tickers_returns_empty(source, redis, cfg):
    """Empty seed_tickers → no queries, return []."""
    cfg.seed_tickers = []
    await cfg.save(redis)
    result = await source.get_trending()
    assert result == []


async def test_get_trending_updates_last_fetch_ts(source, redis, cfg):
    """After a successful fetch, the rate-limit timestamp should be set."""
    cfg.seed_tickers = ["AAPL"]
    await cfg.save(redis)

    with patch(
        "social_trading.ingest.sources.google_trends._fetch_interest_sync",
        return_value={"AAPL": 50},
    ):
        await source.get_trending()

    ts_raw = await redis.get(_LAST_FETCH_KEY)
    assert ts_raw is not None
    assert abs(float(ts_raw) - time.time()) < 5


async def test_get_trending_handles_pytrends_error(source, redis, cfg):
    """pytrends errors should be logged and not raise — return partial results."""
    cfg.seed_tickers = ["AAPL", "TSLA"]
    await cfg.save(redis)

    with patch(
        "social_trading.ingest.sources.google_trends._fetch_interest_sync",
        side_effect=Exception("Google blocked request"),
    ):
        result = await source.get_trending()

    assert result == []  # error handled gracefully


# ── threshold configuration ───────────────────────────────────────────────────

async def test_get_trending_respects_threshold(source, redis, cfg):
    """Only tickers at or above threshold should be proposed."""
    cfg.seed_tickers = ["AAPL", "TSLA", "NVDA"]
    await cfg.save(redis)

    interest = {"AAPL": 70, "TSLA": 69, "NVDA": 95}
    with patch(
        "social_trading.ingest.sources.google_trends._fetch_interest_sync",
        return_value=interest,
    ):
        result = await source.get_trending()

    assert "AAPL" in result   # exactly at threshold
    assert "TSLA" not in result  # one below
    assert "NVDA" in result   # well above
