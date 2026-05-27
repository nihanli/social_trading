"""
YFinanceScreenerDataSource — discovery-only source using Yahoo Finance screeners.

Uses yfinance.screen() to surface trending / high-activity US stocks that are
candidates for the watchlist.  Three predefined screeners run each cycle:

  most_actives  — highest dollar volume today
  day_gainers   — biggest % movers up   (>3 %, market cap >= $2 B)
  day_losers    — biggest % movers down  (configurable)

This source produces **no SocialPost objects** — it is purely a ticker
discovery mechanism.  poll() always returns an empty list; only get_trending()
is meaningful.

yfinance calls are synchronous (Yahoo HTTP + pandas), so they are dispatched
to a thread executor to keep the asyncio event loop unblocked.

Rate limits: Yahoo imposes a soft ~100 req/hour.  Running three screener calls
once per cfg.signal_poll_interval_sec (default 60 s) is well within that budget.

No API key required.
"""
from __future__ import annotations

import asyncio
import logging
from functools import partial
from typing import TYPE_CHECKING, AsyncIterator

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SocialPost
from social_trading.ingest.base import BaseDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# Predefined Yahoo screeners to query — in priority order.
# See yfinance.screener.screener.PREDEFINED_SCREENER_QUERIES for the full list.
_SCREENERS = ["most_actives", "day_gainers", "day_losers"]


class YFinanceScreenerDataSource(BaseDataSource):
    """
    Discovery-only data source backed by Yahoo Finance screeners.

    Registers trending/active tickers as watchlist candidates each cycle.
    Does not emit any SocialPost objects.

    Usage:
        source = YFinanceScreenerDataSource(redis, cfg, watchlist)
        tickers = await source.get_trending()
    """

    def __init__(
        self,
        redis: "aioredis.Redis",
        cfg: SystemConfig,
        watchlist: WatchlistManager,
    ) -> None:
        super().__init__(redis, cfg)
        self._watchlist = watchlist

    @property
    def name(self) -> str:
        return "yfinance"

    @property
    def is_streaming(self) -> bool:
        return False

    # ── DataSource protocol ───────────────────────────────────────────────────

    async def stream(self) -> AsyncIterator[SocialPost]:
        raise NotImplementedError("YFinanceScreenerDataSource is a discovery-only source")
        yield  # pragma: no cover

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """Discovery-only source — no social posts to fetch."""
        return []

    async def get_trending(self) -> list[str]:
        """
        Query Yahoo Finance screeners for trending/active tickers.

        Runs up to cfg.yfinance_screener_count tickers through each screener,
        deduplicates, proposes all to the WatchlistManager, and returns the
        combined unique list.

        Returns empty list on any error so the ingest loop keeps running.
        """
        loop = asyncio.get_event_loop()
        try:
            tickers = await loop.run_in_executor(
                None,
                partial(_fetch_screener_tickers_sync, self._cfg.yfinance_screener_count),
            )
        except Exception as exc:
            await self._handle_error(exc)
            return []

        for ticker in tickers:
            await self._watchlist.propose(ticker, source="yfinance_screener")

        logger.info("yfinance screener: %d tickers discovered", len(tickers))
        self._reset_errors()
        return tickers

    async def health_check(self) -> bool:
        """Verify yfinance is importable and can reach Yahoo."""
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(None, _health_check_sync)
            return result
        except Exception:
            return False


# ── Pure synchronous helpers (run in thread executor) ────────────────────────

def _fetch_screener_tickers_sync(count: int) -> list[str]:
    """
    Fetch tickers from multiple Yahoo Finance screeners.

    Runs in a thread executor — must not use asyncio primitives.
    Returns deduplicated list preserving order (most_actives first).
    """
    import yfinance as yf  # local import — only needed here

    seen: dict[str, None] = {}  # ordered set via dict insertion order

    for screener in _SCREENERS:
        try:
            result = yf.screen(screener, size=count)
            quotes = result.get("quotes") or []
            for q in quotes:
                symbol = q.get("symbol", "").upper()
                if symbol and 1 <= len(symbol) <= 6:
                    seen[symbol] = None
        except Exception as exc:
            logger.warning("yfinance screener '%s' failed: %s", screener, exc)

    return list(seen.keys())


def _health_check_sync() -> bool:
    """Lightweight health check: can we import yfinance and get one ticker?"""
    import yfinance as yf  # local import

    result = yf.screen("most_actives", size=1)
    return bool(result.get("quotes"))
