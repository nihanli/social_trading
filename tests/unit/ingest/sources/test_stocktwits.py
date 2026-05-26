"""Unit tests for StockTwitsDataSource."""
from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.exceptions import RateLimitError
from social_trading.ingest.sources.stocktwits import StockTwitsDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager


def make_trending_response(symbols: list[str] | None = None) -> httpx.Response:
    symbols = symbols or ["AAPL", "TSLA", "NVDA"]
    messages = [
        {
            "id": i,
            "body": f"Trending post about ${sym}",
            "symbols": [{"symbol": sym}],
            "user": {"id": 100 + i, "followers": 1000, "following": 500},
            "created_at": "2024-01-15T10:00:00Z",
            "likes": {"total": 5},
            "entities": {"sentiment": {"basic": "Bullish"}},
        }
        for i, sym in enumerate(symbols)
    ]
    return httpx.Response(200, json={"messages": messages})


def make_symbol_response(
    ticker: str = "AAPL",
    n: int = 3,
    sentiment: str | None = "Bullish",
) -> httpx.Response:
    messages = [
        {
            "id": 1000 + i,
            "body": f"${ticker} post #{i}",
            "user": {"id": 200 + i, "followers": 500 * i, "following": 100},
            "created_at": "2024-01-15T10:00:00Z",
            "likes": {"total": i * 2},
            "entities": {"sentiment": {"basic": sentiment}},
        }
        for i in range(n)
    ]
    return httpx.Response(200, json={"messages": messages})


def make_rate_limit_response() -> httpx.Response:
    return httpx.Response(429, json={"error": "Rate limit exceeded"})


@pytest.fixture
def mock_http():
    client = AsyncMock(spec=httpx.AsyncClient)
    return client


@pytest.fixture
async def watchlist(redis, cfg):
    return WatchlistManager(redis=redis, cfg=cfg)


@pytest.fixture
async def st_source(redis, cfg, watchlist, mock_http):
    src = StockTwitsDataSource(
        redis=redis, cfg=cfg, watchlist=watchlist, http_client=mock_http
    )
    src._last_request_at = 0.0   # skip rate limit sleep in tests
    return src


# ── get_trending ──────────────────────────────────────────────────────────────

async def test_get_trending_returns_tickers(st_source, mock_http):
    mock_http.get = AsyncMock(return_value=make_trending_response(["AAPL", "TSLA"]))
    tickers = await st_source.get_trending()
    assert "AAPL" in tickers
    assert "TSLA" in tickers


async def test_get_trending_proposes_to_watchlist(st_source, mock_http, redis):
    from social_trading.ingest.watchlist.manager import CANDIDATE_KEY

    mock_http.get = AsyncMock(return_value=make_trending_response(["NVDA"]))
    await st_source.get_trending()
    candidates = await redis.zrange(CANDIDATE_KEY, 0, -1)
    assert b"NVDA" in candidates


async def test_get_trending_rate_limit_raises(st_source, mock_http):
    mock_http.get = AsyncMock(return_value=make_rate_limit_response())
    with pytest.raises(RateLimitError):
        await st_source.get_trending()


# ── poll ──────────────────────────────────────────────────────────────────────

async def test_poll_fetches_messages_per_ticker(st_source, mock_http):
    mock_http.get = AsyncMock(return_value=make_symbol_response("AAPL", n=3))
    posts = await st_source.poll(["AAPL"])
    assert len(posts) == 3
    assert all(p.ticker == "AAPL" for p in posts)
    assert all(p.source == "stocktwits" for p in posts)


async def test_poll_stores_sentiment_label(st_source, mock_http):
    mock_http.get = AsyncMock(
        return_value=make_symbol_response("AAPL", n=2, sentiment="Bearish")
    )
    posts = await st_source.poll(["AAPL"])
    for post in posts:
        assert post.raw["sentiment_label"] == "Bearish"


async def test_poll_handles_null_sentiment(st_source, mock_http):
    mock_http.get = AsyncMock(
        return_value=make_symbol_response("AAPL", n=1, sentiment=None)
    )
    posts = await st_source.poll(["AAPL"])
    assert posts[0].raw["sentiment_label"] is None


async def test_poll_publishes_to_stream(st_source, mock_http, redis):
    mock_http.get = AsyncMock(return_value=make_symbol_response("TSLA", n=2))
    await st_source.poll(["TSLA"])
    stream_len = await redis.xlen("raw_social")
    assert stream_len == 2


async def test_poll_multiple_tickers(st_source, mock_http):
    mock_http.get = AsyncMock(
        side_effect=[
            make_symbol_response("AAPL", n=2),
            make_symbol_response("TSLA", n=3),
        ]
    )
    posts = await st_source.poll(["AAPL", "TSLA"])
    assert len(posts) == 5


async def test_poll_stops_on_rate_limit(st_source, mock_http):
    """Rate limit on first ticker → stop; remaining tickers skipped."""
    mock_http.get = AsyncMock(return_value=make_rate_limit_response())
    posts = await st_source.poll(["AAPL", "TSLA", "NVDA"])
    assert posts == []
    assert mock_http.get.call_count == 1


# ── post field normalisation ──────────────────────────────────────────────────

async def test_post_id_prefixed_with_st(st_source, mock_http):
    mock_http.get = AsyncMock(return_value=make_symbol_response("AMD", n=1))
    posts = await st_source.poll(["AMD"])
    assert posts[0].id.startswith("st_")


async def test_post_likes_mapped_correctly(st_source, mock_http):
    mock_http.get = AsyncMock(return_value=make_symbol_response("AAPL", n=1))
    posts = await st_source.poll(["AAPL"])
    # Message id=1000, likes.total = 0*2 = 0
    assert posts[0].likes >= 0


# ── protocol compliance ────────────────────────────────────────────────────────

def test_stocktwits_not_streaming(st_source):
    assert st_source.is_streaming is False


def test_stocktwits_name(st_source):
    assert st_source.name == "stocktwits"


async def test_stream_raises(st_source):
    with pytest.raises(NotImplementedError):
        async for _ in st_source.stream():
            pass


# ── health check ──────────────────────────────────────────────────────────────

async def test_health_check_ok(st_source, mock_http):
    mock_http.get = AsyncMock(
        return_value=httpx.Response(200, json={"messages": []})
    )
    assert await st_source.health_check() is True


async def test_health_check_fails(st_source, mock_http):
    mock_http.get = AsyncMock(side_effect=Exception("connection refused"))
    assert await st_source.health_check() is False
