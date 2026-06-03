"""
TwitterDataSource — Two-tier X (Twitter) API v2 integration.

Tier 1 (free): X Counts endpoint polled every cfg.counts_poll_interval_sec.
               Tracks mention volume for each watchlist ticker.
               Runs Z-score spike detection against 7-day rolling history.

Tier 2 (paid): X Search/recent endpoint called only when spike detected.
               Cost: cfg.x_search_max_results × $0.005 per spike.
               Default: 100 posts × $0.005 = $0.50 per spike.

See docs/design/03-data-sources.md §3b for full architecture.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, AsyncIterator

import httpx

from social_trading.config.system_config import SystemConfig
from social_trading.core.exceptions import RateLimitError
from social_trading.core.models import SocialPost
from social_trading.ingest.base import BaseDataSource

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

X_COUNTS_URL = "https://api.x.com/2/tweets/counts/recent"
X_SEARCH_URL = "https://api.x.com/2/tweets/search/recent"


class TwitterDataSource(BaseDataSource):
    """
    Two-tier X API data source.

    is_streaming = False — driven by the ingest service poll loop.
    poll() runs one Tier-1 counts check per ticker and triggers Tier-2
    on spike. Collected posts are published to the raw_social stream.
    """

    def __init__(
        self,
        redis: "aioredis.Redis",
        cfg: SystemConfig,
        bearer_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(redis, cfg)
        self._bearer_token = bearer_token or os.getenv("X_BEARER_TOKEN", "")
        # Accept injected client for testing (avoids real network calls)
        self._http = http_client or httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self._bearer_token}"},
            timeout=30.0,
        )

    @property
    def name(self) -> str:
        return "twitter"

    @property
    def tier(self) -> int:
        """Tier 2 — metered X API; only called for Phase-2 enrichment candidates."""
        return 2

    @property
    def is_streaming(self) -> bool:
        return False

    # ── DataSource protocol ───────────────────────────────────────────────────

    async def stream(self) -> AsyncIterator[SocialPost]:
        # Twitter source is polled, not streamed — this should never be called
        raise NotImplementedError("TwitterDataSource is polled, not streamed")
        yield  # pragma: no cover

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """
        For each ticker: check mention count, run spike detector,
        pull posts from Tier-2 if spike detected.
        Returns all posts collected (may be empty if no spikes).
        """
        all_posts: list[SocialPost] = []
        for ticker in tickers:
            try:
                count = await self._get_mention_count(ticker)
                is_spike = await self._check_spike(ticker, count)
                if is_spike:
                    logger.info("spike detected for %s (count=%d)", ticker, count)
                    posts = await self._pull_spike_posts(ticker)
                    all_posts.extend(posts)
                    self._reset_errors()
            except RateLimitError as exc:
                await self._handle_error(exc)
                break  # stop polling tickers until backoff expires
            except Exception as exc:
                await self._handle_error(exc)
        return all_posts

    async def get_trending(self) -> list[str]:
        """
        X API doesn't offer a free trending endpoint.
        Returns empty list — discovery handled by Reddit + StockTwits.
        """
        return []

    async def health_check(self) -> bool:
        """Quick check: verify bearer token is set and API is reachable."""
        if not self._bearer_token:
            logger.warning("X_BEARER_TOKEN not set")
            return False
        try:
            # Counts for a single known ticker — cheapest possible call
            await self._get_mention_count("AAPL")
            return True
        except Exception:
            return False

    # ── Tier 1: Mention count ─────────────────────────────────────────────────

    async def _get_mention_count(self, ticker: str) -> int:
        """
        GET /2/tweets/counts/recent for ticker cashtag.
        Free endpoint — no per-post charge.
        """
        start = (
            datetime.now(timezone.utc)
            - timedelta(minutes=self._cfg.mention_window_minutes)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        params = {
            "query": f"${ticker} lang:en -is:retweet",
            "start_time": start,
            "granularity": "minute",
        }
        resp = await self._http.get(X_COUNTS_URL, params=params)
        self._handle_http_error(resp, "counts")
        data = resp.json()
        return int(data.get("meta", {}).get("total_tweet_count", 0))

    # ── Tier 2: Content pull ──────────────────────────────────────────────────

    async def _pull_spike_posts(self, ticker: str) -> list[SocialPost]:
        """
        GET /2/tweets/search/recent — paid endpoint ($0.005/post).
        Only called after spike detected.
        Publishes to raw_social stream and returns posts list.
        """
        params = {
            "query": f"${ticker} lang:en -is:retweet",
            "max_results": min(self._cfg.x_search_max_results, 100),
            "tweet.fields": "created_at,public_metrics,author_id",
            "expansions": "author_id",
            "user.fields": "public_metrics,created_at",
        }
        resp = await self._http.get(X_SEARCH_URL, params=params)
        self._handle_http_error(resp, "search")

        data = resp.json()
        tweets = data.get("data", [])
        users = {
            u["id"]: u
            for u in data.get("includes", {}).get("users", [])
        }

        posts: list[SocialPost] = []
        for tweet in tweets:
            user = users.get(tweet.get("author_id", ""), {})
            user_metrics = user.get("public_metrics", {})
            created_str = tweet.get("created_at", "")
            try:
                created_at = datetime.fromisoformat(
                    created_str.replace("Z", "+00:00")
                )
            except ValueError:
                created_at = datetime.now(timezone.utc)

            # Approximate account age from user.created_at
            account_age_days = 0
            if user_created := user.get("created_at"):
                try:
                    user_dt = datetime.fromisoformat(
                        user_created.replace("Z", "+00:00")
                    )
                    account_age_days = (datetime.now(timezone.utc) - user_dt).days
                except ValueError:
                    pass

            metrics = tweet.get("public_metrics", {})
            post = SocialPost(
                id=tweet["id"],
                source="twitter",
                ticker=ticker,
                text=tweet["text"],
                author_id=tweet.get("author_id", ""),
                author_followers=user_metrics.get("followers_count", 0),
                author_following=user_metrics.get("following_count", 0),
                author_account_age_days=account_age_days,
                likes=metrics.get("like_count", 0),
                reposts=metrics.get("retweet_count", 0),
                is_original=True,
                collected_at=datetime.now(timezone.utc),
                raw={"tweet": tweet, "user": user},
            )
            posts.append(post)

        # Publish batch to stream
        await self._publish_batch(posts)
        logger.info(
            "twitter spike pull: %d posts for %s (cost ~$%.2f)",
            len(posts), ticker, len(posts) * 0.005,
        )
        return posts

    # ── HTTP error handling ───────────────────────────────────────────────────

    def _handle_http_error(self, resp: httpx.Response, endpoint: str) -> None:
        """Raise domain exceptions from HTTP status codes."""
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("x-rate-limit-reset", time.time() + 60))
            raise RateLimitError(
                "twitter",
                retry_after_seconds=max(retry_after - time.time(), 5.0),
            )
        if resp.status_code == 401:
            raise PermissionError(f"X API 401 on {endpoint} — check X_BEARER_TOKEN")
        if resp.status_code >= 400:
            raise RuntimeError(
                f"X API {resp.status_code} on {endpoint}: {resp.text[:200]}"
            )
