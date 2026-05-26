"""Unit tests for RedditDataSource."""
from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.ingest.sources.reddit import CASHTAG_RE, RedditDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager


def make_submission(
    reddit_id: str = "abc123",
    title: str = "$AAPL is flying today! Buy buy buy!",
    selftext: str = "Strong earnings for $AAPL",
    flair: str | None = "DD",
    score: int = 500,
    upvote_ratio: float = 0.95,
    num_comments: int = 42,
    author: str = "wsb_user",
    created_utc: float | None = None,
    subreddit: str = "wallstreetbets",
) -> Any:
    return SimpleNamespace(
        id=reddit_id,
        title=title,
        selftext=selftext,
        link_flair_text=flair,
        score=score,
        upvote_ratio=upvote_ratio,
        num_comments=num_comments,
        author=SimpleNamespace(__str__=lambda self: author),
        created_utc=created_utc or time.time(),
        subreddit=SimpleNamespace(__str__=lambda self: subreddit),
    )


@pytest.fixture
async def watchlist(redis, cfg):
    return WatchlistManager(redis=redis, cfg=cfg)


@pytest.fixture
async def reddit_source(redis, cfg, watchlist):
    fake_praw = MagicMock()
    return RedditDataSource(
        redis=redis, cfg=cfg, watchlist=watchlist, praw_reddit=fake_praw
    )


# ── cashtag extraction ────────────────────────────────────────────────────────

def test_cashtag_regex_extracts_tickers():
    text = "Buying $AAPL and $TSLA today, not $toolong or $1INVALID"
    found = CASHTAG_RE.findall(text.upper())
    assert "AAPL" in found
    assert "TSLA" in found
    assert "TOOLONG" not in found   # >5 chars filtered by propose()


def test_cashtag_regex_ignores_lowercase():
    text = "bought some $aapl shares"
    found = CASHTAG_RE.findall(text)
    assert not found  # regex requires uppercase


# ── _submission_to_posts ──────────────────────────────────────────────────────

async def test_submission_produces_one_post_per_ticker(reddit_source):
    sub = make_submission(title="$AAPL and $TSLA", selftext="")
    posts = reddit_source._submission_to_posts(sub)
    tickers = {p.ticker for p in posts}
    assert tickers == {"AAPL", "TSLA"}


async def test_submission_no_tickers_returns_empty(reddit_source):
    sub = make_submission(title="No tickers here", selftext="Just a meme")
    posts = reddit_source._submission_to_posts(sub)
    assert posts == []


async def test_post_fields_correctly_mapped(reddit_source):
    sub = make_submission(
        title="$NVDA earnings beat!",
        selftext="",
        flair="DD",
        score=1000,
        upvote_ratio=0.92,
    )
    posts = reddit_source._submission_to_posts(sub)
    assert len(posts) == 1
    p = posts[0]
    assert p.source == "reddit"
    assert p.ticker == "NVDA"
    assert p.likes == 1000
    assert p.raw["flair"] == "DD"
    assert p.raw["flair_weight"] == 1.5   # DD → 1.5×
    assert p.raw["upvote_ratio"] == 0.92


async def test_flair_weight_applied_correctly(reddit_source):
    cases = [
        ("DD", 1.5),
        ("YOLO", 1.3),
        ("Gain", 0.8),
        ("Meme", 0.3),
        (None, 1.0),
    ]
    for flair, expected_weight in cases:
        sub = make_submission(title="$AAPL", flair=flair)
        posts = reddit_source._submission_to_posts(sub)
        assert posts[0].raw["flair_weight"] == expected_weight, f"flair={flair}"


async def test_post_id_is_unique_per_ticker(reddit_source):
    sub = make_submission(reddit_id="XYZ", title="$AAPL and $MSFT")
    posts = reddit_source._submission_to_posts(sub)
    ids = [p.id for p in posts]
    assert len(ids) == len(set(ids))   # all unique


async def test_negative_score_clamped_to_zero(reddit_source):
    sub = make_submission(title="$GME", score=-50)
    posts = reddit_source._submission_to_posts(sub)
    assert posts[0].likes == 0


# ── poll ──────────────────────────────────────────────────────────────────────

async def test_poll_returns_empty(reddit_source):
    """Reddit is streaming-only — poll always returns []."""
    result = await reddit_source.poll(["AAPL"])
    assert result == []


# ── get_trending ──────────────────────────────────────────────────────────────

async def test_get_trending_returns_empty(reddit_source):
    """Reddit discovery happens inline in stream — get_trending() is []."""
    result = await reddit_source.get_trending()
    assert result == []


# ── is_streaming flag ─────────────────────────────────────────────────────────

def test_reddit_source_is_streaming(reddit_source):
    assert reddit_source.is_streaming is True


def test_reddit_source_name(reddit_source):
    assert reddit_source.name == "reddit"
