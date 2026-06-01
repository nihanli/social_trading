"""
WatchlistManager — dynamic, self-updating watchlist backed by Redis.

Design (see docs/design/03-data-sources.md §3a):
  - Discovery layer: Reddit stream + StockTwits trending propose candidates
  - Liquidity gate: yfinance free data — ADV, market cap checks
  - Active watchlist: Redis ZSET `watchlist:active` (score = last_seen epoch)
  - Seeds: Redis SET `watchlist:seed` — trader-pinned, never expire
  - Candidates: Redis ZSET `watchlist:candidates` — awaiting liquidity check
  - Source tracking: Redis SET `watchlist:ticker_sources:{ticker}` — which
    discovery sources have mentioned each ticker (used for eviction scoring)

When the watchlist is at capacity, a smart eviction algorithm makes room for
new tickers: non-pinned tickers are scored as
    eviction_score = last_seen_epoch + (MULTI_SOURCE_BONUS if ≥2 sources else 0)
The ticker with the lowest eviction score (stalest, fewest sources) is removed.

The manager is async throughout so it can be called from asyncio service loops.
yfinance calls (I/O bound) are run in a thread executor to avoid blocking.
"""
from __future__ import annotations

import asyncio
import logging
import time
from functools import partial

import redis.asyncio as aioredis

from social_trading.config.system_config import SystemConfig

logger = logging.getLogger(__name__)

WATCHLIST_KEY = "watchlist:active"      # ZSET  score = last_seen epoch
CANDIDATE_KEY = "watchlist:candidates"  # ZSET  score = first_seen epoch
SEED_KEY = "watchlist:seed"             # SET   permanent trader pins
TICKER_SOURCES_PREFIX = "watchlist:ticker_sources:"  # SET  per-ticker source names

# Tickers mentioned by ≥2 sources receive an extra 6-hour recency credit in
# the eviction scoring, making them harder to evict than single-source tickers.
MULTI_SOURCE_BONUS_SECS: int = 6 * 3600


def _decode(v: bytes | str) -> str:
    return v.decode() if isinstance(v, bytes) else v


class WatchlistManager:
    """
    Central registry shared by all data sources and the signal engine.

    All reads/writes go through Redis so every microservice sees the same state.
    Config is reloaded at the start of each promote cycle so UI changes
    (liquidity thresholds, stale hours) take effect within one cycle.
    """

    def __init__(self, redis: aioredis.Redis, cfg: SystemConfig) -> None:
        self._redis = redis
        self._cfg = cfg

    # ── Discovery ─────────────────────────────────────────────────────────────

    async def propose(self, ticker: str, source: str) -> None:
        """
        Add a ticker to the candidate pool from a discovery source.
        Records which source mentioned the ticker (used for eviction scoring).
        If the ticker is already active, just refreshes the last-seen timestamp.
        """
        ticker = ticker.upper().strip()
        if not ticker or len(ticker) > 6:
            return

        # Track mentioning source — this drives the multi-source eviction bonus.
        src_key = TICKER_SOURCES_PREFIX + ticker
        await self._redis.sadd(src_key, source)
        await self._redis.expire(src_key, int(self._cfg.watchlist_stale_hours * 3600))

        already_active = await self._redis.zscore(WATCHLIST_KEY, ticker) is not None
        if already_active:
            await self.touch(ticker)
            return

        is_seed = await self._redis.sismember(SEED_KEY, ticker)
        already_candidate = await self._redis.zscore(CANDIDATE_KEY, ticker) is not None

        if not already_candidate and not is_seed:
            await self._redis.zadd(CANDIDATE_KEY, {ticker: time.time()})
            logger.debug("candidate +%s (source=%s)", ticker, source)

    async def promote_candidates(self) -> int:
        """
        Check all candidates against the liquidity gate and promote passing ones.
        When the watchlist is at capacity, the weakest non-pinned ticker is
        evicted to make room (favouring new multi-source tickers over stale
        single-source ones).  Seeds always promote regardless of capacity.

        Run periodically (every cfg.watchlist_promote_interval seconds).
        Reloads config first so UI changes to thresholds take effect.

        Returns number of tickers promoted.
        """
        # Reload config at the start of each promote cycle
        self._cfg = await SystemConfig.load(self._redis)

        candidates_raw = await self._redis.zrange(CANDIDATE_KEY, 0, -1)
        seeds_raw = await self._redis.smembers(SEED_KEY)

        candidates = [_decode(t) for t in candidates_raw]
        seeds = {_decode(t) for t in seeds_raw}

        promoted = 0
        for ticker in set(candidates) | seeds:
            # Already active — just clean up candidate pool entry if present
            if await self._redis.zscore(WATCHLIST_KEY, ticker) is not None:
                await self._redis.zrem(CANDIDATE_KEY, ticker)
                continue

            # Seeds always get in regardless of capacity (they never expire)
            if ticker not in seeds:
                current_size = await self._redis.zcard(WATCHLIST_KEY)
                if current_size >= self._cfg.watchlist_max_size:
                    evicted = await self._evict_weakest()
                    if not evicted:
                        # All active slots are pinned seeds — cannot evict
                        logger.debug(
                            "watchlist full (%d/%d), all slots pinned — cannot admit %s",
                            current_size, self._cfg.watchlist_max_size, ticker,
                        )
                        # Leave candidate in queue; retry next cycle
                        continue

            if await self._passes_liquidity_gate(ticker):
                await self._redis.zadd(WATCHLIST_KEY, {ticker: time.time()})
                await self._redis.zrem(CANDIDATE_KEY, ticker)
                logger.info("watchlist +%s (promoted)", ticker)
                promoted += 1
            else:
                await self._redis.zrem(CANDIDATE_KEY, ticker)
                logger.debug("watchlist skip %s (failed liquidity gate)", ticker)

        return promoted

    # ── Watchlist access ──────────────────────────────────────────────────────

    async def get_active(self) -> list[str]:
        """
        Return current active watchlist, always including seed tickers.
        Seeds are included even if they haven't passed the liquidity gate yet —
        the trader explicitly wants them monitored.
        """
        active_raw = await self._redis.zrange(WATCHLIST_KEY, 0, -1)
        seeds_raw = await self._redis.smembers(SEED_KEY)

        active = {_decode(t) for t in active_raw}
        seeds = {_decode(t) for t in seeds_raw}
        return sorted(active | seeds)

    async def touch(self, ticker: str) -> None:
        """Refresh last-seen timestamp to prevent expiry."""
        await self._redis.zadd(WATCHLIST_KEY, {ticker.upper(): time.time()}, xx=True)

    async def expire_stale(self) -> int:
        """
        Remove tickers not seen for cfg.watchlist_stale_hours.
        Seed tickers are never removed (trader wants them always active).
        Also cleans up the source-tracking SET for each expired ticker.
        Returns count of tickers expired.
        """
        cutoff = time.time() - self._cfg.watchlist_stale_hours * 3600
        seeds_raw = await self._redis.smembers(SEED_KEY)
        seeds = {_decode(t) for t in seeds_raw}

        stale_raw = await self._redis.zrangebyscore(WATCHLIST_KEY, 0, cutoff)
        stale = [_decode(t) for t in stale_raw]
        to_remove = [t for t in stale if t not in seeds]

        if to_remove:
            await self._redis.zrem(WATCHLIST_KEY, *to_remove)
            for t in to_remove:
                await self._redis.delete(TICKER_SOURCES_PREFIX + t)
            logger.info("watchlist expired: %s", to_remove)

        return len(to_remove)

    async def size(self) -> int:
        """Return count of active watchlist tickers."""
        return await self._redis.zcard(WATCHLIST_KEY)

    async def source_count(self, ticker: str) -> int:
        """Return how many distinct sources have mentioned this ticker."""
        return await self._redis.scard(TICKER_SOURCES_PREFIX + ticker.upper())

    # ── Trader controls ───────────────────────────────────────────────────────

    async def pin(self, ticker: str) -> None:
        """
        Permanently add a ticker the trader always wants monitored.
        Seeds bypass the liquidity gate and never expire.
        """
        ticker = ticker.upper().strip()
        await self._redis.sadd(SEED_KEY, ticker)
        await self._redis.zadd(WATCHLIST_KEY, {ticker: time.time()})
        logger.info("pinned seed ticker: %s", ticker)

    async def unpin(self, ticker: str) -> None:
        """Remove a ticker from the permanent seed list. It may still expire naturally."""
        await self._redis.srem(SEED_KEY, ticker.upper())

    async def seed_from_config(self) -> None:
        """
        Populate seeds from cfg.seed_tickers list.
        Called at service startup to ensure config-defined seeds are always active.
        """
        for ticker in self._cfg.seed_tickers:
            await self.pin(ticker)
        logger.info("seeded %d tickers from config", len(self._cfg.seed_tickers))

    async def get_seeds(self) -> list[str]:
        """Return all trader-pinned seed tickers."""
        raw = await self._redis.smembers(SEED_KEY)
        return sorted(_decode(t) for t in raw)

    async def clear_non_pinned(self) -> int:
        """
        Remove all tickers from the active watchlist except pinned seeds.
        Also clears the candidate pool so stale candidates don't re-promote,
        and removes source-tracking data for removed tickers.
        Returns count of tickers removed.
        """
        seeds_raw = await self._redis.smembers(SEED_KEY)
        seeds = {_decode(t) for t in seeds_raw}

        active_raw = await self._redis.zrange(WATCHLIST_KEY, 0, -1)
        active = [_decode(t) for t in active_raw]
        to_remove = [t for t in active if t not in seeds]

        if to_remove:
            await self._redis.zrem(WATCHLIST_KEY, *to_remove)
            for t in to_remove:
                await self._redis.delete(TICKER_SOURCES_PREFIX + t)

        await self._redis.delete(CANDIDATE_KEY)

        logger.info(
            "watchlist cleared: removed %d tickers, kept %d pinned seeds, "
            "candidate pool reset",
            len(to_remove), len(seeds),
        )
        return len(to_remove)

    # ── Eviction ──────────────────────────────────────────────────────────────

    async def _evict_weakest(self) -> bool:
        """
        Remove the non-pinned ticker with the lowest eviction score to free a slot.

        Eviction score (higher = keep):
            score = last_seen_epoch + (MULTI_SOURCE_BONUS_SECS if ≥2 sources else 0)

        A ticker mentioned by multiple sources gets a 6-hour recency credit,
        making it harder to evict than a similarly-aged single-source ticker.

        Tickers with an active enrichment request or fallback key are protected
        from eviction — they are mid-signal-pipeline and evicting them would cause
        the signal to be silently lost.

        Returns True if a ticker was evicted, False if all active slots are pinned.
        """
        seeds_raw = await self._redis.smembers(SEED_KEY)
        seeds = {_decode(t) for t in seeds_raw}

        # ZRANGE with WITHSCORES returns [(member, score), ...]
        entries = await self._redis.zrange(WATCHLIST_KEY, 0, -1, withscores=True)
        non_seed_entries = [
            (_decode(t), score) for t, score in entries if _decode(t) not in seeds
        ]

        if not non_seed_entries:
            return False  # all active tickers are pinned — cannot evict

        worst_ticker: str | None = None
        worst_score: float = float("inf")

        for ticker, last_seen in non_seed_entries:
            # Skip tickers mid-pipeline: they have an active enrichment request
            # or a pending fallback key.  Evicting them would silently drop a
            # signal that is in progress.
            is_enriching = await self._redis.exists(
                f"enrichment:sent:{ticker}",
                f"enrichment:fallback:{ticker}",
            )
            if is_enriching:
                logger.debug("watchlist eviction skip %s (active enrichment pipeline)", ticker)
                continue

            src_count = await self._redis.scard(TICKER_SOURCES_PREFIX + ticker)
            bonus = MULTI_SOURCE_BONUS_SECS if src_count >= 2 else 0
            composite = last_seen + bonus
            if composite < worst_score:
                worst_score = composite
                worst_ticker = ticker

        if worst_ticker:
            await self._redis.zrem(WATCHLIST_KEY, worst_ticker)
            await self._redis.delete(TICKER_SOURCES_PREFIX + worst_ticker)
            logger.info(
                "watchlist evicted %s (eviction_score=%.0f) to make room for new ticker",
                worst_ticker, worst_score,
            )
            return True

        return False  # pragma: no cover

    # ── Liquidity gate ────────────────────────────────────────────────────────

    async def _passes_liquidity_gate(self, ticker: str) -> bool:
        """
        Check ADV and market cap using yfinance (free, for watchlist admission only).
        Thresholds come from SystemConfig (editable via Streamlit Config page).
        Run in thread executor to avoid blocking the event loop.
        """
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                None, partial(_check_liquidity_sync, ticker, self._cfg)
            )
            return result
        except Exception as exc:
            logger.warning("liquidity gate error for %s: %s", ticker, exc)
            return False


def _check_liquidity_sync(ticker: str, cfg: SystemConfig) -> bool:
    """
    Synchronous yfinance lookup — called from thread executor.
    Separated so it can be easily mocked in tests.
    """
    import yfinance as yf

    info = yf.Ticker(ticker).fast_info
    avg_volume: float = getattr(info, "three_month_average_volume", 0) or 0
    last_price: float = getattr(info, "last_price", 0) or 0
    market_cap: float = getattr(info, "market_cap", 0) or 0
    adv_usd = avg_volume * last_price

    if adv_usd < cfg.watchlist_min_adv_usd:
        return False
    if market_cap < cfg.watchlist_min_mcap_usd:
        return False
    return True
