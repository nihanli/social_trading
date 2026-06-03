"""
GoogleTrendsDataSource — Google Search interest as a discovery signal.

Uses the unofficial pytrends library to query Google Trends for search
interest (0-100 relative scale) across seed and watchlist tickers.

Role in the pipeline:
  - Discovery only: tickers with current Google search interest ≥
    cfg.google_trends_interest_threshold (default 70) are proposed as
    watchlist candidates.
  - NOT a mention-volume source: does not call _check_spike() and is NOT
    included in MENTION_HISTORY_TIER1_SOURCES.  Reasons:
      * Google Trends returns hourly data (not 5-min counts); 12 consecutive
        5-min polls return identical values → artificially small std deviation.
      * Interest is relative to the batch query, not an absolute count.
        Batching different sets of tickers shifts the 0-100 scale.

Rate limiting:
  - Google may throttle heavy pytrends usage.  An internal 55-minute Redis
    lock prevents re-fetching faster than hourly data updates.

Configuration:
  - No API key required.
  - Requires pytrends package: pip install pytrends
  - Threshold configurable via SystemConfig.google_trends_interest_threshold
    (default 70).  Tickers with latest interest ≥ threshold are proposed.

is_streaming = False — driven by the ingest service poll loop.
"""
from __future__ import annotations

import asyncio
import logging
import time
from functools import partial
from typing import TYPE_CHECKING, AsyncIterator

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SocialPost
from social_trading.ingest.base import BaseDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

_LAST_FETCH_KEY = "google_trends:last_fetch_ts"
_RATE_LIMIT_SEC = 3300   # 55 minutes — matches hourly data refresh cadence
_INTEREST_THRESHOLD = 70  # default; overridden by cfg if available
_MAX_TICKERS = 20         # check at most 20 seed tickers per cycle (4 batches × 5)


def _batched(lst: list, n: int):
    """Yield successive n-sized batches from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _fetch_interest_sync(tickers: list[str]) -> dict[str, int]:
    """
    Synchronous pytrends call — runs in a thread executor.

    Returns {ticker: latest_hourly_interest} for each ticker in the batch.
    Missing tickers (empty response) map to 0.
    """
    from pytrends.request import TrendReq  # type: ignore[import-untyped]

    pt = TrendReq(hl="en-US", tz=300, timeout=(10, 25))
    pt.build_payload(tickers, timeframe="now 7-d", geo="US")
    df = pt.interest_over_time()
    if df is None or df.empty:
        return {t: 0 for t in tickers}

    result: dict[str, int] = {}
    for ticker in tickers:
        if ticker in df.columns:
            result[ticker] = int(df[ticker].iloc[-1])
        else:
            result[ticker] = 0
    return result


class GoogleTrendsDataSource(BaseDataSource):
    """
    Discovery-only data source backed by Google Trends via pytrends.

    get_trending() checks the configured seed tickers for elevated search
    interest and proposes any with interest ≥ threshold to the watchlist.
    poll() is a no-op (no SocialPost objects are produced).

    Rate-limited internally to once per 55 minutes so that the poll loop
    cadence (5 min) does not hammer Google's unofficial endpoint.
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
        return "google_trends"

    @property
    def is_streaming(self) -> bool:
        return False

    # ── DataSource protocol ───────────────────────────────────────────────────

    async def stream(self) -> AsyncIterator[SocialPost]:
        raise NotImplementedError("GoogleTrendsDataSource is a discovery-only source")
        yield  # pragma: no cover

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """Discovery-only source — no posts, no spike detection."""
        return []

    async def get_trending(self) -> list[str]:
        """
        Query Google Trends for the configured seed tickers.

        Rate-limited to once per 55 minutes.  Tickers where the latest
        hourly Google search interest reaches cfg.google_trends_interest_threshold
        (default 70 / 100) are proposed to the WatchlistManager.

        Returns the list of proposed tickers, or an empty list on error or
        when the rate-limit window has not elapsed.
        """
        last_ts_raw = await self._redis.get(_LAST_FETCH_KEY)
        if last_ts_raw:
            elapsed = time.time() - float(last_ts_raw)
            if elapsed < _RATE_LIMIT_SEC:
                return []

        tickers_to_check = list(dict.fromkeys(self._cfg.seed_tickers))[:_MAX_TICKERS]
        if not tickers_to_check:
            return []

        threshold = getattr(self._cfg, "google_trends_interest_threshold", _INTEREST_THRESHOLD)
        loop = asyncio.get_event_loop()
        discovered: list[str] = []

        for batch in _batched(tickers_to_check, 5):
            try:
                interest = await loop.run_in_executor(
                    None,
                    partial(_fetch_interest_sync, batch),
                )
                for ticker, value in interest.items():
                    if value >= threshold:
                        await self._watchlist.propose(ticker, source="google_trends")
                        discovered.append(ticker)
            except Exception as exc:
                logger.warning("Google Trends error for batch %s: %s", batch, exc)
                await self._handle_error(exc)
                continue

        await self._redis.set(_LAST_FETCH_KEY, str(time.time()))
        if discovered:
            logger.info(
                "google_trends: proposed %d high-interest tickers (threshold=%d)",
                len(discovered), threshold,
            )
        self._reset_errors()
        return discovered
