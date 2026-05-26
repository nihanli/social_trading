"""
RedditDataSource — PRAW streaming integration.

Reddit serves dual roles:
  1. Discovery: every $TICKER cashtag found proposes the ticker to WatchlistManager
  2. Content: posts are published to raw_social stream immediately (free, no per-post cost)

Subreddits monitored: r/wallstreetbets, r/stocks, r/options, r/investing
Flair weighting is stored in the post metadata so the NLP service can
apply the signal weight multipliers defined in docs/design/03-data-sources.md §3c.

is_streaming = True — PRAW's submission stream is a blocking generator.
The ingest service runs this in an asyncio thread executor.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, AsyncIterator

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SocialPost
from social_trading.ingest.base import BaseDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

CASHTAG_RE = re.compile(r"\$([A-Z]{1,5})\b")

# Subreddits to monitor (+ separated for PRAW multi-reddit)
DEFAULT_SUBREDDITS = "wallstreetbets+stocks+options+investing"

# Flair → signal weight multiplier (stored in post metadata)
FLAIR_WEIGHTS: dict[str | None, float] = {
    "DD": 1.5,
    "Due Diligence": 1.5,
    "YOLO": 1.3,
    "Gain": 0.8,
    "Loss": 0.8,
    "Meme": 0.3,
    "Shitpost": 0.3,
    None: 1.0,
}


class RedditDataSource(BaseDataSource):
    """
    Streaming Reddit data source backed by PRAW.

    is_streaming = True — the service calls stream() which yields posts
    indefinitely via an async generator wrapping PRAW's blocking stream.
    """

    def __init__(
        self,
        redis: "aioredis.Redis",
        cfg: SystemConfig,
        watchlist: WatchlistManager,
        praw_reddit: Any | None = None,   # praw.Reddit instance or fake for tests
        subreddits: str = DEFAULT_SUBREDDITS,
    ) -> None:
        super().__init__(redis, cfg)
        self._watchlist = watchlist
        self._praw = praw_reddit
        self._subreddits = subreddits

    @property
    def name(self) -> str:
        return "reddit"

    @property
    def is_streaming(self) -> bool:
        return True

    # ── DataSource protocol ───────────────────────────────────────────────────

    async def stream(self) -> AsyncIterator[SocialPost]:
        """
        Yield SocialPosts from PRAW's submission stream indefinitely.
        PRAW's stream is synchronous/blocking so it runs in a thread executor.
        On error, backs off and reconnects.
        """
        loop = asyncio.get_event_loop()
        while True:
            try:
                reddit = self._get_praw()
                subreddit = reddit.subreddit(self._subreddits)
                # Run blocking stream in executor; yield posts as they arrive
                async for post in self._iter_praw_stream(loop, subreddit):
                    yield post
                    self._reset_errors()
            except Exception as exc:
                await self._handle_error(exc)

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """Reddit is streaming-only; poll is a no-op (returns empty)."""
        return []

    async def get_trending(self) -> list[str]:
        """
        Reddit doesn't have a clean trending tickers API.
        Discovery happens inline in the stream via cashtag extraction.
        Returns empty — tickers are proposed to watchlist from stream().
        """
        return []

    async def health_check(self) -> bool:
        """Verify PRAW credentials are configured."""
        try:
            reddit = self._get_praw()
            # Lightweight check — just read subreddit info
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: reddit.subreddit("stocks").display_name,
            )
            return True
        except Exception as exc:
            logger.warning("Reddit health check failed: %s", exc)
            return False

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _iter_praw_stream(
        self, loop: asyncio.AbstractEventLoop, subreddit: Any
    ) -> AsyncIterator[SocialPost]:
        """
        Bridge between PRAW's blocking generator and asyncio.
        Runs each next() call in thread executor to avoid blocking event loop.
        """
        gen = subreddit.stream.submissions(skip_existing=True)
        while True:
            try:
                submission = await loop.run_in_executor(None, next, gen)
                posts = self._submission_to_posts(submission)
                for post in posts:
                    await self._publish(post)
                    yield post
            except StopIteration:
                break

    def _submission_to_posts(self, submission: Any) -> list[SocialPost]:
        """
        Convert a PRAW Submission to one SocialPost per extracted ticker.
        A single submission mentioning $AAPL and $TSLA generates 2 posts.
        """
        full_text = f"{submission.title} {submission.selftext}"
        tickers = list(set(CASHTAG_RE.findall(full_text.upper())))

        # Propose all found tickers to watchlist (discovery side effect)
        # Schedule as fire-and-forget; don't block the stream
        for ticker in tickers:
            asyncio.ensure_future(
                self._watchlist.propose(ticker, source="reddit")
            )

        if not tickers:
            return []

        flair = submission.link_flair_text
        flair_weight = FLAIR_WEIGHTS.get(flair, 1.0)

        created_at = datetime.fromtimestamp(
            float(submission.created_utc), tz=timezone.utc
        )
        author_name = str(submission.author) if submission.author else "deleted"

        posts: list[SocialPost] = []
        for ticker in tickers:
            post = SocialPost(
                id=f"reddit_{submission.id}_{ticker}",
                source="reddit",
                ticker=ticker,
                text=full_text[:2000],
                author_id=author_name,
                author_followers=0,       # Reddit doesn't expose follower counts
                author_account_age_days=0,
                likes=max(0, submission.score),
                reposts=0,
                is_original=True,
                collected_at=datetime.now(timezone.utc),
                raw={
                    "reddit_id": submission.id,
                    "subreddit": str(submission.subreddit),
                    "flair": flair,
                    "flair_weight": flair_weight,
                    "upvote_ratio": float(submission.upvote_ratio or 0),
                    "num_comments": int(submission.num_comments or 0),
                    "created_utc": float(submission.created_utc),
                },
            )
            posts.append(post)
        return posts

    def _get_praw(self) -> Any:
        """
        Return the PRAW Reddit instance, creating one from env vars if needed.
        Accepting an injected instance allows tests to pass a fake.
        """
        if self._praw is not None:
            return self._praw

        import praw

        self._praw = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID", ""),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET", ""),
            user_agent=os.getenv(
                "REDDIT_USER_AGENT", "social-trading-bot/0.1"
            ),
            read_only=True,
        )
        return self._praw
