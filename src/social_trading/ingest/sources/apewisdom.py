"""
ApeWisdomDataSource — Reddit/4chan stock mention tracking.

ApeWisdom aggregates stock mention counts from Reddit finance communities
(r/wallstreetbets, r/stocks, r/investing, r/options) and 4chan /biz/ over a
rolling 24-hour window.  It exposes a public, no-auth leaderboard API.

Role in the two-phase pipeline:
  - Volume signal:  24-hour rolling mention count feeds _check_spike() for
                    Z-score-based spike detection per ticker.
  - Discovery:      top-25 tickers by mention count are proposed as watchlist
                    candidates each poll cycle.

API: https://apewisdom.io/api/v1.0/filter/all-stocks?page=N
No API key required.

Caveats:
  - Mention counts are 24h rolling totals, not 5-min deltas like Bluesky.
    The Z-score baseline captures "normal daily attention" per ticker.
  - The leaderboard is paginated (25 results/page).  We paginate until all
    active watchlist tickers are found or a page cap is reached.
  - Tickers absent from every fetched page are skipped — never injected as
    zero — to avoid depressing the baseline with censored data.

is_streaming = False — driven by the ingest service poll loop.
"""
from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, AsyncIterator

import httpx

from social_trading.config.system_config import SystemConfig
from social_trading.core.exceptions import RateLimitError
from social_trading.core.models import SocialPost
from social_trading.ingest.base import BaseDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

_LEADERBOARD_URL = "https://apewisdom.io/api/v1.0/filter/all-stocks"
_MAX_PAGES = 5          # fetch up to 5 pages (≈125 tickers) per cycle
_CACHE_KEY = "apewisdom:leaderboard_cache"
_CACHE_TTL_SEC = 240    # 4-minute cache shared between get_trending() and poll()


class ApeWisdomDataSource(BaseDataSource):
    """
    Polling ApeWisdom data source.

    Poll cycle (driven by ingest service):
      1. get_trending()  → fetches leaderboard, proposes top-25 to watchlist
      2. poll(tickers)   → uses cached leaderboard, calls _check_spike() for
                           each ticker found in the leaderboard (skips missing)

    No SocialPost objects are produced — this is a volume-signal source only.
    """

    def __init__(
        self,
        redis: "aioredis.Redis",
        cfg: SystemConfig,
        watchlist: WatchlistManager,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(redis, cfg)
        self._watchlist = watchlist
        self._http = http_client or httpx.AsyncClient(timeout=15.0)

    @property
    def name(self) -> str:
        return "apewisdom"

    @property
    def is_streaming(self) -> bool:
        return False

    # ── DataSource protocol ───────────────────────────────────────────────────

    async def stream(self) -> AsyncIterator[SocialPost]:
        raise NotImplementedError("ApeWisdomDataSource is polled, not streamed")
        yield  # pragma: no cover

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """
        For each watchlist ticker present in the ApeWisdom leaderboard, record
        its 24h mention count in the per-source spike-detection history.
        Tickers absent from the leaderboard are silently skipped.
        """
        if not tickers:
            return []

        try:
            counts = await self._fetch_leaderboard(set(tickers))
        except RateLimitError as exc:
            await self._handle_error(exc)
            return []
        except Exception as exc:
            logger.warning("ApeWisdom poll error: %s", exc)
            await self._handle_error(exc)
            return []

        for ticker in tickers:
            if ticker not in counts:
                continue  # absent = missing data, not zero
            await self._check_spike(ticker, counts[ticker])

        self._reset_errors()
        return []

    async def get_trending(self) -> list[str]:
        """
        Fetch the ApeWisdom leaderboard and propose the top-25 tickers as
        watchlist candidates.  Populates the shared 4-minute Redis cache so
        the subsequent poll() call avoids a redundant HTTP request.
        """
        try:
            counts = await self._fetch_leaderboard(set())
        except Exception as exc:
            logger.warning("ApeWisdom get_trending error: %s", exc)
            await self._handle_error(exc)
            return []

        top_tickers = list(counts.keys())[:25]
        for ticker in top_tickers:
            await self._watchlist.propose(ticker, source="apewisdom")

        logger.info("apewisdom: proposed %d tickers as watchlist candidates", len(top_tickers))
        self._reset_errors()
        return top_tickers

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _fetch_leaderboard(
        self, needed_tickers: set[str]
    ) -> dict[str, int]:
        """
        Return ``{ticker: 24h_mention_count}`` for the fetched leaderboard.

        Checks the Redis cache first (TTL = 4 min).  On cache miss, paginates
        the ApeWisdom API until all *needed_tickers* are resolved or *_MAX_PAGES*
        is reached, then writes the result to cache.

        Ordering of the returned dict matches the leaderboard rank order
        (highest mentions first).
        """
        raw = await self._redis.get(_CACHE_KEY)
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                pass

        counts: dict[str, int] = {}
        for page in range(1, _MAX_PAGES + 1):
            try:
                resp = await self._http.get(
                    _LEADERBOARD_URL,
                    params={"filter": "all-stocks", "page": page},
                )
            except Exception as exc:
                err_str = str(exc).lower()
                if "429" in err_str or "rate" in err_str:
                    raise RateLimitError("apewisdom", retry_after_seconds=60.0) from exc
                raise

            if resp.status_code == 429:
                raise RateLimitError("apewisdom", retry_after_seconds=60.0)
            resp.raise_for_status()

            data = resp.json()
            results = data.get("results", [])
            if not results:
                break

            for item in results:
                ticker = str(item.get("ticker", "")).upper().strip()
                if ticker:
                    counts[ticker] = int(item.get("mentions", 0))

            # Stop early once all needed tickers have been resolved
            if needed_tickers and needed_tickers.issubset(counts.keys()):
                break

            total_pages = int(data.get("pages", 1))
            if page >= total_pages:
                break

        await self._redis.set(_CACHE_KEY, json.dumps(counts), ex=_CACHE_TTL_SEC)
        logger.debug("apewisdom: fetched %d tickers across up to %d pages", len(counts), page)
        return counts
