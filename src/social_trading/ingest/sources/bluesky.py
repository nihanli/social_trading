"""
BlueskyDataSource — AT Protocol / Bluesky social monitoring.

Bluesky uses the open AT Protocol, which provides an official, free API.
Authentication requires a free bsky.app account and an app password.

This source serves the same two-tier role as the former X API:
  Tier 1 — Volume signal: searchPosts($TICKER) returns posts in a rolling
            window; the count feeds the same Z-score spike detector used by
            StockTwitsDataSource (shared via BaseDataSource._check_spike).
  Tier 2 — Content: full post text is in the same search response, so no
            additional API call is required on spike detection.

Configuration (env vars):
    BLUESKY_HANDLE        Your Bluesky handle, e.g. "you.bsky.social"
    BLUESKY_APP_PASSWORD  App password from Settings → App Passwords

is_streaming = False — driven by the ingest service poll loop.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, AsyncIterator

from social_trading.config.system_config import SystemConfig
from social_trading.core.exceptions import RateLimitError
from social_trading.core.models import SocialPost
from social_trading.ingest.base import BaseDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class BlueskyDataSource(BaseDataSource):
    """
    Polling Bluesky data source using the AT Protocol SDK (atproto).

    Poll cycle:
      1. get_trending() → returns empty (Bluesky has no finance trending feed)
      2. poll(tickers)  → searchPosts($TICKER), count for spike detection,
                          publish posts on spike.
    """

    def __init__(
        self,
        redis: "aioredis.Redis",
        cfg: SystemConfig,
        watchlist: WatchlistManager,
        handle: str | None = None,
        app_password: str | None = None,
    ) -> None:
        super().__init__(redis, cfg)
        self._watchlist = watchlist
        self._handle = handle or os.getenv("BLUESKY_HANDLE", "")
        self._app_password = app_password or os.getenv("BLUESKY_APP_PASSWORD", "")
        self._client: object | None = None   # lazy-init atproto.Client

    @property
    def name(self) -> str:
        return "bluesky"

    @property
    def is_streaming(self) -> bool:
        return False

    # ── DataSource protocol ───────────────────────────────────────────────────

    async def stream(self) -> AsyncIterator[SocialPost]:
        raise NotImplementedError("BlueskyDataSource is polled, not streamed")
        yield  # pragma: no cover

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """
        For each ticker: search recent posts, count for spike detection,
        publish posts to raw_social stream on spike.
        """
        all_posts: list[SocialPost] = []
        for ticker in tickers:
            try:
                posts = await self._search_ticker(ticker)
                is_spike = await self._check_spike(ticker, len(posts))
                if is_spike:
                    logger.info(
                        "bluesky spike: %s count=%d — publishing %d posts",
                        ticker, len(posts), len(posts),
                    )
                    await self._publish_batch(posts)
                    all_posts.extend(posts)
                elif posts:
                    # Publish a small sample to keep NLP baseline warm
                    await self._publish_batch(posts[:3])
                    all_posts.extend(posts[:3])
                await self._watchlist.touch(ticker)
                self._reset_errors()
            except RateLimitError as exc:
                await self._handle_error(exc)
                break
            except Exception as exc:
                logger.warning("Bluesky poll error for %s: %s", ticker, exc)
                await self._handle_error(exc)
        return all_posts

    async def get_trending(self) -> list[str]:
        """Bluesky has no finance-specific trending feed — returns empty."""
        return []

    async def health_check(self) -> bool:
        """Verify credentials by performing a lightweight profile lookup."""
        try:
            client = await self._get_client()
            loop = __import__("asyncio").get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: client.app.bsky.actor.get_profile(params={"actor": self._handle}),
            )
            return True
        except Exception as exc:
            logger.warning("Bluesky health check failed: %s", exc)
            return False

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _get_client(self) -> object:
        """Return authenticated atproto Client, creating and logging in if needed."""
        if self._client is not None:
            return self._client

        try:
            from atproto import Client  # type: ignore[import-untyped]
        except ImportError as exc:
            raise RuntimeError(
                "atproto package not installed — run: pip install atproto"
            ) from exc

        import asyncio

        loop = asyncio.get_event_loop()
        client = Client()
        await loop.run_in_executor(
            None,
            lambda: client.login(self._handle, self._app_password),
        )
        self._client = client
        logger.info("Bluesky: authenticated as %s", self._handle)
        return self._client

    async def _search_ticker(self, ticker: str) -> list[SocialPost]:
        """
        Search Bluesky for posts mentioning $TICKER.
        Returns up to cfg.bluesky_search_count posts.
        """
        import asyncio

        client = await self._get_client()
        limit = min(self._cfg.bluesky_search_count, 100)
        query = f"${ticker}"

        loop = asyncio.get_event_loop()
        try:
            response = await loop.run_in_executor(
                None,
                lambda: client.app.bsky.feed.search_posts(
                    params={"q": query, "limit": limit, "sort": "latest"}
                ),
            )
        except Exception as exc:
            err_str = str(exc).lower()
            if "rate" in err_str or "429" in err_str:
                raise RateLimitError("bluesky", retry_after_seconds=60.0)
            raise

        posts: list[SocialPost] = []
        for bsky_post in getattr(response, "posts", []):
            record = getattr(bsky_post, "record", None)
            if record is None:
                continue

            text: str = getattr(record, "text", "")
            created_str: str = getattr(record, "created_at", "")
            try:
                created_at = datetime.fromisoformat(
                    created_str.replace("Z", "+00:00")
                )
            except ValueError:
                created_at = datetime.now(timezone.utc)

            author = getattr(bsky_post, "author", None)
            author_handle = getattr(author, "handle", "") if author else ""
            author_did = getattr(author, "did", "") if author else ""
            viewer = getattr(author, "viewer", None) if author else None
            followers = getattr(viewer, "followers_count", 0) if viewer else 0

            uri: str = getattr(bsky_post, "uri", "")
            cid: str = getattr(bsky_post, "cid", "")
            like_count: int = getattr(bsky_post, "like_count", 0) or 0
            repost_count: int = getattr(bsky_post, "repost_count", 0) or 0

            post = SocialPost(
                id=f"bsky_{cid or uri.split('/')[-1]}",
                source="bluesky",
                ticker=ticker,
                text=text,
                author_id=author_did or author_handle,
                author_followers=followers,
                author_following=0,
                author_account_age_days=0,
                likes=like_count,
                reposts=repost_count,
                is_original=True,
                url=f"https://bsky.app/profile/{author_handle}/post/{uri.split('/')[-1]}",
                collected_at=datetime.now(timezone.utc),
                raw={"uri": uri, "cid": cid, "created_at": created_str},
            )
            posts.append(post)

        logger.debug("Bluesky: %d posts for $%s", len(posts), ticker)
        return posts
