"""
AlphaVantageDataSource — discovery-only source using the Alpha Vantage
TOP_GAINERS_LOSERS endpoint.

Endpoint:
  GET https://www.alphavantage.co/query?function=TOP_GAINERS_LOSERS&apikey=KEY

Returns three lists of 20 tickers each: top gainers, top losers, and most
actively traded.  All three are proposed to the WatchlistManager.

Rate limit management:
  The free tier allows only **25 requests/day**.  To stay within budget the
  response is cached in Redis at key ``cache:alpha_vantage:trending`` with a
  TTL of cfg.alpha_vantage_cache_ttl_sec (default 3600 s).  Most cycles read
  from cache; a real API call is only made when the cache has expired.

This source produces **no SocialPost objects** — poll() always returns [].

Configuration:
  ALPHA_VANTAGE_API_KEY  — env var (required; free key at alphavantage.co)
"""
from __future__ import annotations

import json
import logging
import os
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

_AV_BASE = "https://www.alphavantage.co/query"
_CACHE_KEY = "cache:alpha_vantage:trending"
_CACHE_META_KEY = "cache:alpha_vantage:trending_meta"

# Categories returned by the endpoint, mapped to source labels for watchlist
_CATEGORIES: dict[str, str] = {
    "most_actively_traded": "alpha_vantage_active",
    "top_gainers":          "alpha_vantage_gainer",
    "top_losers":           "alpha_vantage_loser",
}


class AlphaVantageDataSource(BaseDataSource):
    """
    Discovery-only data source backed by the Alpha Vantage TOP_GAINERS_LOSERS
    endpoint.  Results are Redis-cached to respect the free tier quota.

    Usage:
        source = AlphaVantageDataSource(redis, cfg, watchlist)
        tickers = await source.get_trending()
    """

    def __init__(
        self,
        redis: "aioredis.Redis",
        cfg: SystemConfig,
        watchlist: WatchlistManager,
        api_key: str | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(redis, cfg)
        self._watchlist = watchlist
        self._api_key = api_key or os.getenv("ALPHA_VANTAGE_API_KEY", "")
        self._http = http_client or httpx.AsyncClient(timeout=20.0)

    @property
    def name(self) -> str:
        return "alpha_vantage"

    @property
    def is_streaming(self) -> bool:
        return False

    # ── DataSource protocol ───────────────────────────────────────────────────

    async def stream(self) -> AsyncIterator[SocialPost]:
        raise NotImplementedError("AlphaVantageDataSource is a discovery-only source")
        yield  # pragma: no cover

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """Discovery-only source — no social posts to fetch."""
        return []

    async def get_trending(self) -> list[str]:
        """
        Return trending tickers from Alpha Vantage, using the Redis cache when
        available to protect the 25 req/day free tier quota.

        On a cache hit:  returns cached tickers immediately (no HTTP call).
        On a cache miss: fetches live, repopulates cache, proposes to watchlist.

        Returns empty list on any error.
        """
        if not self._api_key:
            logger.warning("ALPHA_VANTAGE_API_KEY not set — skipping get_trending")
            return []

        cached = await self._load_cache()
        if cached is not None:
            logger.debug("alpha_vantage: cache hit (%d tickers)", len(cached))
            return cached

        try:
            tickers = await self._fetch_live()
        except RateLimitError:
            raise
        except Exception as exc:
            await self._handle_error(exc)
            return []

        await self._save_cache(tickers)
        for ticker in tickers:
            await self._watchlist.propose(ticker, source="alpha_vantage_trending")
        logger.info("alpha_vantage: fetched %d tickers (cache refreshed)", len(tickers))
        self._reset_errors()
        return tickers

    async def health_check(self) -> bool:
        """Verify API key is configured and the endpoint is reachable."""
        if not self._api_key:
            return False
        try:
            resp = await self._http.get(
                _AV_BASE,
                params={"function": "TOP_GAINERS_LOSERS", "apikey": self._api_key},
                timeout=10.0,
            )
            return resp.status_code == 200 and "top_gainers" in resp.text
        except Exception:
            return False

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _fetch_live(self) -> list[str]:
        """Perform the actual Alpha Vantage API call and extract all tickers."""
        resp = await self._http.get(
            _AV_BASE,
            params={"function": "TOP_GAINERS_LOSERS", "apikey": self._api_key},
        )
        self._handle_http_error(resp)
        data = resp.json()

        # Detect API-level errors (Alpha Vantage returns 200 with error body)
        if "Information" in data:
            # Rate limit or quota exceeded
            raise RateLimitError("alpha_vantage", retry_after_seconds=3600.0)
        if "Error Message" in data:
            raise RuntimeError(f"Alpha Vantage error: {data['Error Message']}")

        seen: dict[str, None] = {}
        for category, source_label in _CATEGORIES.items():
            for entry in data.get(category, []):
                ticker = entry.get("ticker", "").upper()
                if ticker and 1 <= len(ticker) <= 6:
                    seen[ticker] = None

        return list(seen.keys())

    async def _load_cache(self) -> list[str] | None:
        """
        Read cached tickers from Redis if still fresh.
        Returns None on cache miss or expiry.
        """
        ttl = self._cfg.alpha_vantage_cache_ttl_sec
        meta_raw = await self._redis.get(_CACHE_META_KEY)
        if not meta_raw:
            return None
        try:
            meta = json.loads(meta_raw)
            cached_at: float = meta.get("cached_at", 0.0)
        except (json.JSONDecodeError, KeyError):
            return None

        if time.time() - cached_at > ttl:
            return None  # cache expired

        raw = await self._redis.get(_CACHE_KEY)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    async def _save_cache(self, tickers: list[str]) -> None:
        """Persist tickers and a timestamp to Redis."""
        ttl = self._cfg.alpha_vantage_cache_ttl_sec
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.set(_CACHE_KEY, json.dumps(tickers), ex=ttl + 60)
            pipe.set(
                _CACHE_META_KEY,
                json.dumps({"cached_at": time.time(), "count": len(tickers)}),
                ex=ttl + 60,
            )
            await pipe.execute()

    def _handle_http_error(self, resp: httpx.Response) -> None:
        """Map HTTP errors to domain exceptions."""
        if resp.status_code == 429:
            raise RateLimitError("alpha_vantage", retry_after_seconds=3600.0)
        if resp.status_code >= 400:
            raise RuntimeError(
                f"Alpha Vantage HTTP {resp.status_code}: {resp.text[:200]}"
            )
