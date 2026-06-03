"""Unit tests for ApeWisdomDataSource."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.exceptions import RateLimitError
from social_trading.ingest.base import MENTION_HISTORY_KEY
from social_trading.ingest.sources.apewisdom import ApeWisdomDataSource, _CACHE_KEY


_DUMMY_REQUEST = httpx.Request("GET", "https://apewisdom.io/api/v1.0/filter/all-stocks")


def make_leaderboard_response(
    tickers: list[str] | None = None,
    page: int = 1,
    total_pages: int = 1,
) -> httpx.Response:
    tickers = tickers or ["AAPL", "TSLA", "NVDA", "MSFT", "AMD"]
    results = [
        {"ticker": t, "mentions": (100 - i * 10), "upvotes": 50}
        for i, t in enumerate(tickers)
    ]
    return httpx.Response(
        200,
        json={"filter": "all-stocks", "page": page, "pages": total_pages, "results": results},
        request=_DUMMY_REQUEST,
    )


def make_rate_limit_response() -> httpx.Response:
    return httpx.Response(429, json={"error": "rate limited"}, request=_DUMMY_REQUEST)


@pytest.fixture
def mock_http():
    return AsyncMock(spec=httpx.AsyncClient)


@pytest.fixture
async def watchlist(redis, cfg):
    from social_trading.ingest.watchlist.manager import WatchlistManager
    wl = WatchlistManager(redis=redis, cfg=cfg)
    return wl


@pytest.fixture
async def source(redis, cfg, watchlist, mock_http):
    return ApeWisdomDataSource(
        redis=redis, cfg=cfg, watchlist=watchlist, http_client=mock_http
    )


# ── get_trending ──────────────────────────────────────────────────────────────

async def test_get_trending_proposes_top25(source, mock_http, redis):
    """Top tickers from leaderboard should be proposed to watchlist."""
    tickers = [f"T{i}" for i in range(30)]
    mock_http.get = AsyncMock(return_value=make_leaderboard_response(tickers, total_pages=2))

    result = await source.get_trending()
    assert len(result) == 25

    # Cache should be populated
    cached = await redis.get(_CACHE_KEY)
    assert cached is not None


async def test_get_trending_uses_cache(source, mock_http, redis):
    """Second call within TTL should not make another HTTP request."""
    tickers = ["AAPL", "TSLA"]
    mock_http.get = AsyncMock(return_value=make_leaderboard_response(tickers))

    await source.get_trending()
    await source.get_trending()

    assert mock_http.get.call_count == 1  # only fetched once due to cache


async def test_get_trending_returns_empty_on_error(source, mock_http):
    """HTTP error should return empty list without raising."""
    mock_http.get = AsyncMock(side_effect=Exception("network error"))
    result = await source.get_trending()
    assert result == []


# ── poll ──────────────────────────────────────────────────────────────────────

async def test_poll_records_spike_history_for_found_tickers(source, mock_http, redis, cfg):
    """Tickers found in leaderboard should have _check_spike called and history recorded."""
    mock_http.get = AsyncMock(
        return_value=make_leaderboard_response(["AAPL", "TSLA"])
    )

    await source.poll(["AAPL", "TSLA"])

    key_aapl = MENTION_HISTORY_KEY.format(source="apewisdom", ticker="AAPL")
    history = await redis.lrange(key_aapl, 0, -1)
    assert len(history) == 1
    assert float(history[0]) == 100.0  # first entry (mentions = 100)


async def test_poll_skips_missing_tickers(source, mock_http, redis):
    """Tickers absent from leaderboard should NOT get a zero injected into history."""
    mock_http.get = AsyncMock(
        return_value=make_leaderboard_response(["AAPL"])  # only AAPL in leaderboard
    )

    await source.poll(["AAPL", "UNKNOWN_TICKER"])

    key = MENTION_HISTORY_KEY.format(source="apewisdom", ticker="UNKNOWN_TICKER")
    history = await redis.lrange(key, 0, -1)
    assert len(history) == 0  # not injected as zero


async def test_poll_returns_no_posts(source, mock_http):
    """poll() is count-only — should never return SocialPost objects."""
    mock_http.get = AsyncMock(return_value=make_leaderboard_response())
    posts = await source.poll(["AAPL"])
    assert posts == []


async def test_poll_handles_rate_limit(source, mock_http):
    """429 from API should raise RateLimitError and return empty."""
    mock_http.get = AsyncMock(return_value=make_rate_limit_response())
    posts = await source.poll(["AAPL"])
    assert posts == []


async def test_poll_empty_tickers_returns_empty(source, mock_http):
    """Empty watchlist should short-circuit without any HTTP call."""
    posts = await source.poll([])
    assert posts == []
    mock_http.get.assert_not_called()


# ── pagination ────────────────────────────────────────────────────────────────

async def test_poll_paginates_until_ticker_found(source, mock_http, redis):
    """If watchlist ticker is not on page 1, should fetch page 2."""
    page1 = make_leaderboard_response(["AAPL", "TSLA"], page=1, total_pages=2)
    page2 = make_leaderboard_response(["RARE"], page=2, total_pages=2)
    mock_http.get = AsyncMock(side_effect=[page1, page2])

    await source.poll(["RARE"])

    key = MENTION_HISTORY_KEY.format(source="apewisdom", ticker="RARE")
    history = await redis.lrange(key, 0, -1)
    assert len(history) == 1


async def test_poll_stops_paginating_when_all_found(source, mock_http, redis):
    """Should stop after page 1 if all needed tickers found there."""
    mock_http.get = AsyncMock(
        return_value=make_leaderboard_response(["AAPL", "TSLA"], total_pages=5)
    )

    await source.poll(["AAPL"])

    # Only one page should be fetched (cache miss + all needed found)
    assert mock_http.get.call_count == 1
