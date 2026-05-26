"""Unit tests for WatchlistManager."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.ingest.watchlist.manager import (
    CANDIDATE_KEY,
    SEED_KEY,
    WATCHLIST_KEY,
    WatchlistManager,
)


@pytest.fixture
async def wm(redis, cfg):
    return WatchlistManager(redis=redis, cfg=cfg)


# ── propose ───────────────────────────────────────────────────────────────────

async def test_propose_adds_to_candidates(wm, redis):
    await wm.propose("AAPL", source="reddit")
    candidates = await redis.zrange(CANDIDATE_KEY, 0, -1)
    assert b"AAPL" in candidates


async def test_propose_uppercases_ticker(wm, redis):
    await wm.propose("nvda", source="reddit")
    candidates = await redis.zrange(CANDIDATE_KEY, 0, -1)
    assert b"NVDA" in candidates


async def test_propose_ignores_long_tickers(wm, redis):
    await wm.propose("TOOLONG", source="reddit")
    candidates = await redis.zrange(CANDIDATE_KEY, 0, -1)
    assert b"TOOLONG" not in candidates


async def test_propose_active_ticker_touches_instead(wm, redis):
    """Active tickers should have their score refreshed, not re-added to candidates."""
    await redis.zadd(WATCHLIST_KEY, {"MSFT": time.time() - 100})
    await wm.propose("MSFT", source="reddit")
    # Still in watchlist with updated score
    score = await redis.zscore(WATCHLIST_KEY, "MSFT")
    assert score is not None
    # Not added to candidates
    candidates = await redis.zrange(CANDIDATE_KEY, 0, -1)
    assert b"MSFT" not in candidates


async def test_propose_duplicate_candidate_ignored(wm, redis):
    await wm.propose("AMD", source="reddit")
    await wm.propose("AMD", source="stocktwits")
    count = await redis.zcard(CANDIDATE_KEY)
    assert count == 1


# ── promote_candidates ────────────────────────────────────────────────────────

async def test_promote_passes_liquidity_gate(wm, redis):
    """Mock liquidity gate to return True — ticker should be promoted."""
    await redis.zadd(CANDIDATE_KEY, {"AAPL": time.time()})
    with patch(
        "social_trading.ingest.watchlist.manager._check_liquidity_sync",
        return_value=True,
    ):
        promoted = await wm.promote_candidates()
    assert promoted == 1
    active = await redis.zrange(WATCHLIST_KEY, 0, -1)
    assert b"AAPL" in active
    # Removed from candidates after promotion
    candidates = await redis.zrange(CANDIDATE_KEY, 0, -1)
    assert b"AAPL" not in candidates


async def test_promote_fails_liquidity_gate(wm, redis):
    """Ticker failing liquidity gate should be dropped from candidates."""
    await redis.zadd(CANDIDATE_KEY, {"PENNYSTOCK": time.time()})
    with patch(
        "social_trading.ingest.watchlist.manager._check_liquidity_sync",
        return_value=False,
    ):
        promoted = await wm.promote_candidates()
    assert promoted == 0
    active = await redis.zrange(WATCHLIST_KEY, 0, -1)
    assert b"PENNYSTOCK" not in active


# ── get_active ────────────────────────────────────────────────────────────────

async def test_get_active_includes_seeds(wm, redis):
    await redis.sadd(SEED_KEY, "SPY")
    tickers = await wm.get_active()
    assert "SPY" in tickers


async def test_get_active_returns_sorted_list(wm, redis):
    await redis.zadd(WATCHLIST_KEY, {"TSLA": time.time(), "AAPL": time.time()})
    tickers = await wm.get_active()
    assert tickers == sorted(tickers)


# ── expire_stale ──────────────────────────────────────────────────────────────

async def test_expire_stale_removes_old_tickers(wm, redis, cfg):
    cfg.watchlist_stale_hours = 1
    old_score = time.time() - 7200   # 2 hours ago
    await redis.zadd(WATCHLIST_KEY, {"STALE": old_score})
    removed = await wm.expire_stale()
    assert removed == 1
    active = await redis.zrange(WATCHLIST_KEY, 0, -1)
    assert b"STALE" not in active


async def test_expire_stale_keeps_seeds(wm, redis, cfg):
    cfg.watchlist_stale_hours = 1
    old_score = time.time() - 7200
    await redis.zadd(WATCHLIST_KEY, {"SPY": old_score})
    await redis.sadd(SEED_KEY, "SPY")
    removed = await wm.expire_stale()
    assert removed == 0
    active = await redis.zrange(WATCHLIST_KEY, 0, -1)
    assert b"SPY" in active


async def test_expire_stale_keeps_fresh_tickers(wm, redis, cfg):
    cfg.watchlist_stale_hours = 48
    await redis.zadd(WATCHLIST_KEY, {"FRESH": time.time()})
    removed = await wm.expire_stale()
    assert removed == 0


# ── pin / unpin ───────────────────────────────────────────────────────────────

async def test_pin_adds_to_seed_and_active(wm, redis):
    await wm.pin("NVDA")
    is_seed = await redis.sismember(SEED_KEY, "NVDA")
    is_active = await redis.zscore(WATCHLIST_KEY, "NVDA")
    assert is_seed
    assert is_active is not None


async def test_unpin_removes_from_seed(wm, redis):
    await wm.pin("NVDA")
    await wm.unpin("NVDA")
    is_seed = await redis.sismember(SEED_KEY, "NVDA")
    assert not is_seed


# ── seed_from_config ──────────────────────────────────────────────────────────

async def test_seed_from_config_pins_all_seeds(wm, redis, cfg):
    cfg.seed_tickers = ["SPY", "QQQ", "AAPL"]
    await wm.seed_from_config()
    for ticker in ["SPY", "QQQ", "AAPL"]:
        assert await redis.sismember(SEED_KEY, ticker)
        assert await redis.zscore(WATCHLIST_KEY, ticker) is not None
