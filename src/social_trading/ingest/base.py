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
from social_trading.core.events import STREAM_RAW_SOCIAL, STREAM_MAXLEN
from social_trading.core.exceptions import RateLimitError
from social_trading.core.models import SocialPost

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Redis key pattern for mention-volume history used by spike detection
MENTION_HISTORY_KEY = "mention_history:{ticker}"
MENTION_HISTORY_LEN = 168   # 7 days × 24 hourly samples

# Maximum backoff between retries (seconds)
_MAX_BACKOFF = 300

# Pre-publish dedup: tracks post IDs already written to the raw_social stream.
# Prevents re-publishing posts that appear in repeated API poll responses
# (common for Bluesky/StockTwits baseline samples on non-spike cycles).
_DEDUP_KEY_PREFIX = "ingest:seen:"
_DEDUP_TTL_SECS = 86_400  # 24 hours — posts older than this can safely re-appear


class BaseDataSource(ABC):
    """
    Abstract base for all social data sources.

    Concrete implementations (TwitterDataSource, RedditDataSource, …) must
    implement: name, is_streaming, stream(), poll(), get_trending().

    The base handles publishing to Redis and exponential backoff so each
    concrete source can focus only on its API integration.

    Source tiers control cost management in the two-phase signal pipeline:
        Tier 1 — free/low-cost sources polled on every cycle for all tickers.
                 (Bluesky, StockTwits, Reddit, yfinance screener, …)
        Tier 2 — metered/paid sources only called for Phase-1 candidates.
                 (Twitter/X API)
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
    def tier(self) -> int:
        """
        Cost tier for two-phase signal filtering.
        1 = free / always-on (default).
        2 = metered / paid — only called for Phase-1 signal candidates.
        Override in paid sources.
        """
        return 1

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

    async def _publish(self, post: SocialPost) -> str | None:
        """
        Publish a normalised SocialPost to the raw_social Redis Stream.

        Skips (and returns None) if the post has already been published within
        the last 24 hours — prevents re-publishing the same post across poll
        cycles when sources return overlapping recent results.
        """
        dedup_key = _DEDUP_KEY_PREFIX + post.id
        is_new = await self._redis.set(dedup_key, "1", nx=True, ex=_DEDUP_TTL_SECS)
        if not is_new:
            logger.debug("%s: skipping already-published post %s", self.name, post.id)
            return None
        payload = _post_to_stream_dict(post)
        maxlen = STREAM_MAXLEN.get(STREAM_RAW_SOCIAL)
        msg_id: bytes = await self._redis.xadd(STREAM_RAW_SOCIAL, payload, maxlen=maxlen, approximate=True)
        return msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)

    async def _publish_batch(self, posts: list[SocialPost]) -> int:
        """
        Publish multiple posts in one pipeline. Returns count of new posts published.

        Deduplicates against posts already sent to the stream within the last 24 hours
        so repeated API poll responses do not flood the stream with duplicate entries.
        """
        if not posts:
            return 0

        # Phase 1: check which post IDs are genuinely new via atomic SET NX
        async with self._redis.pipeline(transaction=False) as pipe:
            for post in posts:
                pipe.set(_DEDUP_KEY_PREFIX + post.id, "1", nx=True, ex=_DEDUP_TTL_SECS)
            results = await pipe.execute()

        # SET NX returns True when key was newly created (post not seen before)
        new_posts = [p for p, r in zip(posts, results) if r]
        skipped = len(posts) - len(new_posts)
        if skipped:
            logger.debug(
                "%s: dedup filtered %d/%d already-published posts",
                self.name, skipped, len(posts),
            )
        if not new_posts:
            return 0

        # Phase 2: publish only the new posts
        maxlen = STREAM_MAXLEN.get(STREAM_RAW_SOCIAL)
        async with self._redis.pipeline(transaction=False) as pipe:
            for post in new_posts:
                pipe.xadd(STREAM_RAW_SOCIAL, _post_to_stream_dict(post), maxlen=maxlen, approximate=True)
            await pipe.execute()
        logger.debug("%s published %d new posts", self.name, len(new_posts))
        return len(new_posts)

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

    # ── Spike detection (shared Z-score logic) ───────────────────────────────

    async def _check_spike(self, ticker: str, count: int) -> bool:
        """
        Z-score spike detector shared by all volume-counting sources.

        Appends *count* to the rolling 7-day history stored at
        ``mention_history:{ticker}`` and returns True when the current
        count's Z-score exceeds ``cfg.spike_zscore_threshold``.

        At least 24 samples are required before detection activates so a
        flat baseline can be established first.
        """
        import numpy as np

        key = MENTION_HISTORY_KEY.format(ticker=ticker)
        raw_history = await self._redis.lrange(key, 0, -1)
        history = [float(x) for x in raw_history]

        await self._redis.rpush(key, count)
        await self._redis.ltrim(key, -MENTION_HISTORY_LEN, -1)

        if len(history) < 24:
            return False

        mean = float(np.mean(history))
        raw_std = float(np.std(history))
        std = max(raw_std, mean * 0.10, 1.0)
        zscore = (count - mean) / std

        logger.debug(
            "%s %s count=%d mean=%.1f std=%.1f zscore=%.2f threshold=%.1f",
            self.name, ticker, count, mean, std, zscore,
            self._cfg.spike_zscore_threshold,
        )
        return zscore >= self._cfg.spike_zscore_threshold

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
        # StockTwits native Bullish/Bearish label — empty string for other sources
        "sentiment_label": str(post.raw.get("sentiment_label", "")),
    }
