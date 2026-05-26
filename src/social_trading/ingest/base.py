"""
BaseDataSource — abstract base class for all social media data sources.

Subclasses implement stream() or poll() for their platform.
This base class provides:
  - Shared __init__ signature (redis + cfg injection)
  - _publish / _publish_batch helpers that write SocialPost → raw_social stream
  - Exponential backoff helper for transient API errors
  - Default health_check implementation

All concrete sources must satisfy the DataSource protocol defined in
core/protocols.py — the protocol is checked at registration time.
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, AsyncIterator

import redis.asyncio as aioredis

from social_trading.config.system_config import SystemConfig
from social_trading.core.events import STREAM_RAW_SOCIAL
from social_trading.core.exceptions import RateLimitError
from social_trading.core.models import SocialPost

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Maximum backoff between retries (seconds)
_MAX_BACKOFF = 300


class BaseDataSource(ABC):
    """
    Abstract base for all social data sources.

    Concrete implementations (TwitterDataSource, RedditDataSource, …) must
    implement: name, is_streaming, stream(), poll(), get_trending().

    The base handles publishing to Redis and exponential backoff so each
    concrete source can focus only on its API integration.
    """

    def __init__(self, redis: aioredis.Redis, cfg: SystemConfig) -> None:
        self._redis = redis
        self._cfg = cfg
        self._consecutive_errors: int = 0

    # ── Abstract interface ────────────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique source identifier e.g. "twitter", "reddit", "stocktwits"."""
        ...

    @property
    def is_streaming(self) -> bool:
        """
        True  → service calls stream() and iterates indefinitely.
        False → service calls poll() on a timer (default).
        Override in subclass if streaming.
        """
        return False

    @abstractmethod
    async def stream(self) -> AsyncIterator[SocialPost]:
        """
        Yield posts as they arrive (streaming sources only).
        Must be an async generator. Called when is_streaming=True.
        """
        # subclass must implement; pragma: no cover
        raise NotImplementedError
        yield  # make type checker happy — this is an async generator stub

    @abstractmethod
    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """
        Fetch recent posts for given tickers (polling sources).
        Called on a timer when is_streaming=False.
        """
        ...

    @abstractmethod
    async def get_trending(self) -> list[str]:
        """Return currently trending tickers on this platform."""
        ...

    # ── Publishing helpers ────────────────────────────────────────────────────

    async def _publish(self, post: SocialPost) -> str:
        """Publish a normalised SocialPost to the raw_social Redis Stream."""
        payload = _post_to_stream_dict(post)
        msg_id: bytes = await self._redis.xadd(STREAM_RAW_SOCIAL, payload)
        return msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)

    async def _publish_batch(self, posts: list[SocialPost]) -> int:
        """Publish multiple posts in one pipeline. Returns count published."""
        if not posts:
            return 0
        async with self._redis.pipeline(transaction=False) as pipe:
            for post in posts:
                pipe.xadd(STREAM_RAW_SOCIAL, _post_to_stream_dict(post))
            await pipe.execute()
        logger.debug("%s published %d posts", self.name, len(posts))
        return len(posts)

    # ── Error / backoff helpers ───────────────────────────────────────────────

    async def health_check(self) -> bool:
        """Default: try a poll with empty list; override for cheaper checks."""
        try:
            await self.poll([])
            return True
        except Exception:
            return False

    async def _handle_error(self, exc: Exception) -> None:
        """
        Record consecutive error count and sleep with exponential backoff.
        Resets on next successful call via _reset_errors().
        """
        self._consecutive_errors += 1
        if isinstance(exc, RateLimitError):
            backoff = exc.retry_after_seconds
        else:
            backoff = min(2 ** self._consecutive_errors, _MAX_BACKOFF)
        logger.warning(
            "%s error #%d — backing off %.0fs: %s",
            self.name, self._consecutive_errors, backoff, exc,
        )
        await asyncio.sleep(backoff)

    def _reset_errors(self) -> None:
        """Call after a successful operation to reset the backoff counter."""
        if self._consecutive_errors > 0:
            logger.info("%s recovered after %d errors", self.name, self._consecutive_errors)
        self._consecutive_errors = 0

    # ── Config reload ─────────────────────────────────────────────────────────

    async def reload_cfg(self) -> None:
        """Reload SystemConfig from Redis — pick up UI changes each cycle."""
        self._cfg = await SystemConfig.load(self._redis)


# ── Serialisation helper ──────────────────────────────────────────────────────

def _post_to_stream_dict(post: SocialPost) -> dict[str, str]:
    """
    Convert a SocialPost to a flat dict of str→str for Redis Streams.
    Redis Streams require all values to be bytes/str.
    """
    return {
        "id": post.id,
        "source": post.source,
        "ticker": post.ticker,
        "text": post.text[:2000],          # guard against huge posts
        "author_id": post.author_id,
        "author_followers": str(post.author_followers),
        "author_account_age_days": str(post.author_account_age_days),
        "author_following": str(post.author_following),
        "post_count_30d": str(post.post_count_30d),
        "likes": str(post.likes),
        "reposts": str(post.reposts),
        "is_original": "1" if post.is_original else "0",
        "url": post.url,
        "collected_at": post.collected_at.isoformat(),
    }
