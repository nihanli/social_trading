"""Unit tests for WatchlistManager."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.ingest.watchlist.manager import (
    CANDIDATE_KEY,
    MULTI_SOURCE_BONUS_SECS,
    SEED_KEY,
    TICKER_SOURCES_PREFIX,
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


# ── clear_non_pinned ──────────────────────────────────────────────────────────

async def test_clear_non_pinned_removes_unpinned(wm, redis):
    """Non-seed tickers are removed; seed tickers are kept."""
    await redis.zadd(WATCHLIST_KEY, {"NVDA": time.time(), "TSLA": time.time()})
    await redis.sadd(SEED_KEY, "NVDA")  # NVDA is pinned

    removed = await wm.clear_non_pinned()

    assert removed == 1
    assert await redis.zscore(WATCHLIST_KEY, "NVDA") is not None
    assert await redis.zscore(WATCHLIST_KEY, "TSLA") is None


async def test_clear_non_pinned_flushes_candidates(wm, redis):
    """Candidate pool is cleared so stale candidates don't re-promote."""
    await redis.zadd(CANDIDATE_KEY, {"MSFT": time.time(), "AMD": time.time()})
    await redis.zadd(WATCHLIST_KEY, {"AMD": time.time()})

    await wm.clear_non_pinned()

    assert await redis.zcard(CANDIDATE_KEY) == 0


async def test_clear_non_pinned_empty_watchlist(wm, redis):
    """Clearing an already-empty watchlist returns 0 without error."""
    removed = await wm.clear_non_pinned()
    assert removed == 0


async def test_clear_non_pinned_all_pinned(wm, redis):
    """If every active ticker is pinned, nothing is removed."""
    await redis.zadd(WATCHLIST_KEY, {"SPY": time.time(), "QQQ": time.time()})
    await redis.sadd(SEED_KEY, "SPY", "QQQ")

    removed = await wm.clear_non_pinned()

    assert removed == 0
    assert await redis.zscore(WATCHLIST_KEY, "SPY") is not None
    assert await redis.zscore(WATCHLIST_KEY, "QQQ") is not None


# ── source tracking ───────────────────────────────────────────────────────────

async def test_propose_tracks_source(wm, redis):
    """propose() should record the source in the ticker_sources SET."""
    await wm.propose("AAPL", source="reddit")
    src_count = await redis.scard(TICKER_SOURCES_PREFIX + "AAPL")
    assert src_count == 1


async def test_propose_multi_source_accumulates(wm, redis):
    """Multiple propose() calls from different sources accumulate in the SET."""
    await wm.propose("AAPL", source="reddit")
    await wm.propose("AAPL", source="bluesky")
    src_count = await redis.scard(TICKER_SOURCES_PREFIX + "AAPL")
    assert src_count == 2


async def test_propose_same_source_not_duplicated(wm, redis):
    """Same source proposed multiple times counts as 1."""
    await wm.propose("AAPL", source="reddit")
    await wm.propose("AAPL", source="reddit")
    src_count = await redis.scard(TICKER_SOURCES_PREFIX + "AAPL")
    assert src_count == 1


async def test_source_count_method(wm, redis):
    await redis.sadd(TICKER_SOURCES_PREFIX + "TSLA", "reddit", "bluesky")
    assert await wm.source_count("TSLA") == 2


# ── eviction algorithm ────────────────────────────────────────────────────────

async def test_evict_weakest_removes_stalest_single_source(wm, redis, cfg):
    """
    With no multi-source tickers, the one with the oldest last_seen is evicted.
    """
    now = time.time()
    await redis.zadd(WATCHLIST_KEY, {"OLD": now - 3000, "NEW": now - 100})
    # Neither has multi-source bonus

    evicted = await wm._evict_weakest()

    assert evicted is True
    assert await redis.zscore(WATCHLIST_KEY, "OLD") is None
    assert await redis.zscore(WATCHLIST_KEY, "NEW") is not None


async def test_evict_weakest_prefers_single_source_over_multi(wm, redis):
    """
    A ticker with ≥2 sources gets a 6-hour recency credit — it should survive
    even if it's older than a single-source ticker, within the bonus window.
    """
    now = time.time()
    # MULTI was last seen 4 hours ago but has 2 sources → effective score = now-4h+6h = now+2h
    # SINGLE was last seen 1 hour ago but has 1 source → effective score = now-1h
    await redis.zadd(WATCHLIST_KEY, {
        "MULTI": now - 4 * 3600,
        "SINGLE": now - 1 * 3600,
    })
    await redis.sadd(TICKER_SOURCES_PREFIX + "MULTI", "reddit", "bluesky")
    # SINGLE has no sources entry (0 sources → no bonus)

    evicted = await wm._evict_weakest()

    assert evicted is True
    # SINGLE should be evicted (lower effective score) despite being more recent
    assert await redis.zscore(WATCHLIST_KEY, "SINGLE") is None
    assert await redis.zscore(WATCHLIST_KEY, "MULTI") is not None


async def test_evict_weakest_skips_seeds(wm, redis):
    """Pinned seeds must never be evicted."""
    now = time.time()
    await redis.zadd(WATCHLIST_KEY, {"SPY": now - 9999})
    await redis.sadd(SEED_KEY, "SPY")

    evicted = await wm._evict_weakest()

    assert evicted is False  # Nothing to evict — only slot is a seed
    assert await redis.zscore(WATCHLIST_KEY, "SPY") is not None


async def test_evict_weakest_cleans_source_key(wm, redis):
    """Evicting a ticker should also delete its ticker_sources SET."""
    now = time.time()
    await redis.zadd(WATCHLIST_KEY, {"VICTIM": now - 5000})
    await redis.sadd(TICKER_SOURCES_PREFIX + "VICTIM", "reddit")

    await wm._evict_weakest()

    assert await redis.scard(TICKER_SOURCES_PREFIX + "VICTIM") == 0


async def test_promote_evicts_when_at_capacity(wm, redis, cfg):
    """When at capacity, a passing candidate should evict the weakest active ticker."""
    cfg.watchlist_max_size = 2
    await cfg.save(redis)

    now = time.time()
    # Fill watchlist to capacity with stale single-source tickers
    await redis.zadd(WATCHLIST_KEY, {"OLD1": now - 9000, "OLD2": now - 8000})

    # Queue a new candidate
    await redis.zadd(CANDIDATE_KEY, {"NEWT": now})

    with patch(
        "social_trading.ingest.watchlist.manager._check_liquidity_sync",
        return_value=True,
    ):
        promoted = await wm.promote_candidates()

    assert promoted == 1
    assert await redis.zscore(WATCHLIST_KEY, "NEWT") is not None
    # Watchlist should still be at max (one evicted, one promoted)
    assert await redis.zcard(WATCHLIST_KEY) == 2


async def test_promote_no_eviction_when_all_pinned(wm, redis, cfg):
    """When at capacity and all slots are seeds, candidate is kept in queue."""
    cfg.watchlist_max_size = 1
    await cfg.save(redis)

    now = time.time()
    await redis.zadd(WATCHLIST_KEY, {"SPY": now})
    await redis.sadd(SEED_KEY, "SPY")
    await redis.zadd(CANDIDATE_KEY, {"NEWT": now})

    with patch(
        "social_trading.ingest.watchlist.manager._check_liquidity_sync",
        return_value=True,
    ):
        promoted = await wm.promote_candidates()

    # Cannot evict a seed — NEWT stays in candidate queue, not promoted
    assert promoted == 0
    assert await redis.zscore(CANDIDATE_KEY, "NEWT") is not None


# ── expire_stale cleans up sources ────────────────────────────────────────────

async def test_expire_stale_cleans_source_keys(wm, redis, cfg):
    """expire_stale() should delete ticker_sources keys for removed tickers."""
    cfg.watchlist_stale_hours = 1
    old_score = time.time() - 7200
    await redis.zadd(WATCHLIST_KEY, {"STALE": old_score})
    await redis.sadd(TICKER_SOURCES_PREFIX + "STALE", "reddit")

    await wm.expire_stale()

    assert await redis.scard(TICKER_SOURCES_PREFIX + "STALE") == 0


# ── clear_non_pinned cleans up sources ────────────────────────────────────────

async def test_clear_non_pinned_cleans_source_keys(wm, redis):
    """clear_non_pinned() should delete ticker_sources keys for removed tickers."""
    await redis.zadd(WATCHLIST_KEY, {"TSLA": time.time()})
    await redis.sadd(TICKER_SOURCES_PREFIX + "TSLA", "reddit", "bluesky")

    await wm.clear_non_pinned()

    assert await redis.scard(TICKER_SOURCES_PREFIX + "TSLA") == 0
