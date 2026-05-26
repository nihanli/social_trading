"""
StockTwitsDataSource — Finance-native discovery + directional sentiment.

StockTwits serves two roles (see docs/design/03-data-sources.md §3d):
  1. Discovery: /streams/trending.json reveals tickers the finance community is
     talking about right now → proposed to WatchlistManager every 5 minutes.
  2. Directional signal: users explicitly tag posts as Bullish/Bearish.
     This bypasses NLP entirely and provides a clean directional label.

Both endpoints are free tier with ~200 requests/hour limit.
Polling 25 tickers every 6 minutes = 250 req/hr — slightly over free limit.
Rate limiting ensures we stay safely within 200 req/hr.

is_streaming = False — driven by the ingest service poll loop.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
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

STOCKTWITS_BASE = "https://api.stocktwits.com/api/2"
TRENDING_URL = f"{STOCKTWITS_BASE}/streams/trending.json"
SYMBOL_URL = f"{STOCKTWITS_BASE}/streams/symbol/{{symbol}}.json"

# StockTwits free tier: ~200 requests/hour → 1 request every 18 seconds minimum
MIN_REQUEST_INTERVAL = 2.0   # seconds between requests (conservative)


class StockTwitsDataSource(BaseDataSource):
    """
    Polling StockTwits data source.

    Poll cycle (driven by ingest service):
      1. get_trending() → proposes tickers to watchlist
      2. poll(tickers) → fetches labelled messages per ticker → raw_social stream
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
        self._http = http_client or httpx.AsyncClient(timeout=20.0)
        self._last_request_at: float = 0.0

    @property
    def name(self) -> str:
        return "stocktwits"

    @property
    def is_streaming(self) -> bool:
        return False

    # ── DataSource protocol ───────────────────────────────────────────────────

    async def stream(self) -> AsyncIterator[SocialPost]:
        raise NotImplementedError("StockTwitsDataSource is polled, not streamed")
        yield  # pragma: no cover

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """
        Fetch the 30 most recent labelled messages for each ticker.
        Publishes to raw_social stream and returns posts.
        Respects rate limiting with MIN_REQUEST_INTERVAL between requests.
        """
        all_posts: list[SocialPost] = []
        for ticker in tickers:
            try:
                posts = await self._fetch_symbol_messages(ticker)
                all_posts.extend(posts)
                await self._watchlist.touch(ticker)
                self._reset_errors()
            except RateLimitError as exc:
                await self._handle_error(exc)
                break
            except Exception as exc:
                logger.warning("StockTwits poll error for %s: %s", ticker, exc)
                await self._handle_error(exc)
        return all_posts

    async def get_trending(self) -> list[str]:
        """
        Fetch trending tickers from StockTwits /streams/trending.json.
        Proposes each to the WatchlistManager for liquidity gating.
        Returns list of trending ticker symbols.
        """
        await self._rate_limit()
        try:
            resp = await self._http.get(TRENDING_URL)
            self._handle_http_error(resp, "trending")
            messages = resp.json().get("messages", [])
            tickers: list[str] = []
            for msg in messages:
                for sym in msg.get("symbols", []):
                    ticker = sym.get("symbol", "").upper()
                    if ticker:
                        tickers.append(ticker)
                        await self._watchlist.propose(ticker, source="stocktwits_trending")
            logger.debug("StockTwits trending: %d tickers discovered", len(tickers))
            self._reset_errors()
            return list(set(tickers))
        except RateLimitError:
            raise
        except Exception as exc:
            await self._handle_error(exc)
            return []

    async def health_check(self) -> bool:
        """Check StockTwits API reachability."""
        try:
            resp = await self._http.get(TRENDING_URL, timeout=10.0)
            return resp.status_code == 200
        except Exception:
            return False

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _fetch_symbol_messages(self, ticker: str) -> list[SocialPost]:
        """
        GET /streams/symbol/{symbol}.json — free endpoint.
        Retrieves up to 30 most recent messages with native Bullish/Bearish labels.
        """
        await self._rate_limit()
        url = SYMBOL_URL.format(symbol=ticker)
        resp = await self._http.get(url, params={"limit": 30})
        self._handle_http_error(resp, f"symbol/{ticker}")

        messages = resp.json().get("messages", [])
        posts: list[SocialPost] = []

        for msg in messages:
            sentiment_label = (
                msg.get("entities", {}).get("sentiment", {}).get("basic")
            )  # "Bullish", "Bearish", or None

            user = msg.get("user", {})
            created_str = msg.get("created_at", "")
            try:
                created_at = datetime.fromisoformat(
                    created_str.replace("Z", "+00:00")
                )
            except ValueError:
                created_at = datetime.now(timezone.utc)

            post = SocialPost(
                id=f"st_{msg['id']}",
                source="stocktwits",
                ticker=ticker,
                text=msg.get("body", ""),
                author_id=str(user.get("id", "")),
                author_followers=user.get("followers", 0),
                author_following=user.get("following", 0),
                author_account_age_days=0,  # not available in API
                likes=msg.get("likes", {}).get("total", 0),
                reposts=0,
                is_original=True,
                collected_at=datetime.now(timezone.utc),
                raw={
                    "stocktwits_id": msg["id"],
                    "sentiment_label": sentiment_label,  # "Bullish"/"Bearish"/None
                    "created_at": created_str,
                },
            )
            posts.append(post)

        if posts:
            await self._publish_batch(posts)
            logger.debug("StockTwits: %d posts for %s", len(posts), ticker)

        return posts

    async def _rate_limit(self) -> None:
        """Ensure at least MIN_REQUEST_INTERVAL seconds between API requests."""
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < MIN_REQUEST_INTERVAL:
            await asyncio.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_at = time.monotonic()

    def _handle_http_error(self, resp: httpx.Response, endpoint: str) -> None:
        """Map HTTP status codes to domain exceptions."""
        if resp.status_code == 429:
            raise RateLimitError("stocktwits", retry_after_seconds=60.0)
        if resp.status_code == 401:
            raise PermissionError(f"StockTwits 401 on {endpoint} — check credentials")
        if resp.status_code >= 400:
            raise RuntimeError(
                f"StockTwits {resp.status_code} on {endpoint}: {resp.text[:200]}"
            )
