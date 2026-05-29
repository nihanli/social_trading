"""
WatchlistManager — dynamic, self-updating watchlist backed by Redis.

Design (see docs/design/03-data-sources.md §3a):
  - Discovery layer: Reddit stream + StockTwits trending propose candidates
  - Liquidity gate: yfinance free data — ADV, market cap checks
  - Active watchlist: Redis ZSET `watchlist:active` (score = last_seen epoch)
  - Seeds: Redis SET `watchlist:seed` — trader-pinned, never expire
  - Candidates: Redis ZSET `watchlist:candidates` — awaiting liquidity check

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
        If it's already active, just refresh the last-seen timestamp.
        """
        ticker = ticker.upper().strip()
        if not ticker or len(ticker) > 6:
            return

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
        Run periodically (every cfg.watchlist_promote_interval seconds).
        Reloads config first so UI changes to thresholds take effect.

        Returns number of tickers promoted.
        """
        # Reload config at the start of each promote cycle
        self._cfg = await SystemConfig.load(self._redis)

        candidates_raw = await self._redis.zrange(CANDIDATE_KEY, 0, -1)
        seeds_raw = await self._redis.smembers(SEED_KEY)

        candidates = [t.decode() if isinstance(t, bytes) else t for t in candidates_raw]
        seeds = [t.decode() if isinstance(t, bytes) else t for t in seeds_raw]

        promoted = 0
        for ticker in set(candidates + seeds):
            # Check if already active (seeds might already be there)
            if await self._redis.zscore(WATCHLIST_KEY, ticker) is not None:
                await self._redis.zrem(CANDIDATE_KEY, ticker)
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

        active = {t.decode() if isinstance(t, bytes) else t for t in active_raw}
        seeds = {t.decode() if isinstance(t, bytes) else t for t in seeds_raw}
        return sorted(active | seeds)

    async def touch(self, ticker: str) -> None:
        """Refresh last-seen timestamp to prevent expiry."""
        await self._redis.zadd(WATCHLIST_KEY, {ticker.upper(): time.time()}, xx=True)

    async def expire_stale(self) -> int:
        """
        Remove tickers not seen for cfg.watchlist_stale_hours.
        Seed tickers are never removed (trader wants them always active).
        Returns count of tickers expired.
        """
        cutoff = time.time() - self._cfg.watchlist_stale_hours * 3600
        seeds_raw = await self._redis.smembers(SEED_KEY)
        seeds = {t.decode() if isinstance(t, bytes) else t for t in seeds_raw}

        stale_raw = await self._redis.zrangebyscore(WATCHLIST_KEY, 0, cutoff)
        stale = [t.decode() if isinstance(t, bytes) else t for t in stale_raw]
        to_remove = [t for t in stale if t not in seeds]

        if to_remove:
            await self._redis.zrem(WATCHLIST_KEY, *to_remove)
            logger.info("watchlist expired: %s", to_remove)

        return len(to_remove)

    async def size(self) -> int:
        """Return count of active watchlist tickers."""
        return await self._redis.zcard(WATCHLIST_KEY)

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
        return sorted(t.decode() if isinstance(t, bytes) else t for t in raw)

    async def clear_non_pinned(self) -> int:
        """
        Remove all tickers from the active watchlist except pinned seeds.
        Also clears the candidate pool so stale candidates don't re-promote.
        Returns count of tickers removed.
        """
        seeds_raw = await self._redis.smembers(SEED_KEY)
        seeds = {t.decode() if isinstance(t, bytes) else t for t in seeds_raw}

        active_raw = await self._redis.zrange(WATCHLIST_KEY, 0, -1)
        active = [t.decode() if isinstance(t, bytes) else t for t in active_raw]
        to_remove = [t for t in active if t not in seeds]

        if to_remove:
            await self._redis.zrem(WATCHLIST_KEY, *to_remove)

        await self._redis.delete(CANDIDATE_KEY)

        logger.info(
            "watchlist cleared: removed %d tickers, kept %d pinned seeds, "
            "candidate pool reset",
            len(to_remove), len(seeds),
        )
        return len(to_remove)

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
