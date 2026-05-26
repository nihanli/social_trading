"""Unit tests for BotFilter."""
from __future__ import annotations

import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SocialPost
from social_trading.nlp.filters.bot_filter import BotFilter

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def cfg() -> SystemConfig:
    return SystemConfig(
        bot_min_account_age_days=30,
        bot_max_velocity_per_hour=50,
        bot_min_follower_following_ratio=0.1,
    )


@pytest.fixture
def bot_filter(cfg: SystemConfig) -> BotFilter:
    return BotFilter(cfg)


def make_post(**kwargs) -> SocialPost:
    defaults = dict(
        id="p1",
        source="twitter",
        ticker="AAPL",
        text="$AAPL to the moon!",
        author_id="user1",
        author_followers=1000,
        author_following=100,
        author_account_age_days=365,
        post_count_30d=30,
    )
    defaults.update(kwargs)
    return SocialPost(**defaults)


# ── Account age tests ─────────────────────────────────────────────────────────

def test_new_account_is_bot(bot_filter: BotFilter) -> None:
    post = make_post(author_account_age_days=5)
    assert bot_filter.is_bot(post) is True


def test_exactly_at_min_age_is_not_bot(bot_filter: BotFilter) -> None:
    post = make_post(author_account_age_days=30)
    assert bot_filter.is_bot(post) is False


def test_old_account_is_not_bot(bot_filter: BotFilter) -> None:
    post = make_post(author_account_age_days=500)
    assert bot_filter.is_bot(post) is False


def test_zero_age_skips_check(bot_filter: BotFilter) -> None:
    """Age=0 means source doesn't provide it (e.g. Reddit) — skip."""
    post = make_post(author_account_age_days=0)
    assert bot_filter.is_bot(post) is False


# ── Follower ratio tests ──────────────────────────────────────────────────────

def test_inverted_ratio_is_bot(bot_filter: BotFilter) -> None:
    # following/followers = 10000/100 = 100; ratio = 100/10000 = 0.01 < 0.1
    post = make_post(author_followers=100, author_following=10_000)
    assert bot_filter.is_bot(post) is True


def test_good_ratio_is_not_bot(bot_filter: BotFilter) -> None:
    # ratio = 5000/500 = 10; followers/following = 10 > 0.1
    post = make_post(author_followers=5000, author_following=500)
    assert bot_filter.is_bot(post) is False


def test_zero_followers_skips_ratio_check(bot_filter: BotFilter) -> None:
    post = make_post(author_followers=0, author_following=1000)
    assert bot_filter.is_bot(post) is False


def test_zero_following_skips_ratio_check(bot_filter: BotFilter) -> None:
    post = make_post(author_followers=1000, author_following=0)
    assert bot_filter.is_bot(post) is False


# ── Velocity tests ────────────────────────────────────────────────────────────

def test_high_velocity_is_bot(bot_filter: BotFilter) -> None:
    # 50/hr threshold → 50 * 720 = 36000/month; use 50000
    post = make_post(post_count_30d=50_000)
    assert bot_filter.is_bot(post) is True


def test_normal_velocity_is_not_bot(bot_filter: BotFilter) -> None:
    post = make_post(post_count_30d=100)  # ~0.14/hr
    assert bot_filter.is_bot(post) is False


def test_zero_post_count_skips_velocity(bot_filter: BotFilter) -> None:
    post = make_post(post_count_30d=0)
    assert bot_filter.is_bot(post) is False


# ── Config reload ─────────────────────────────────────────────────────────────

def test_update_cfg_changes_thresholds(bot_filter: BotFilter, cfg: SystemConfig) -> None:
    post = make_post(author_account_age_days=25)
    assert bot_filter.is_bot(post) is True  # below 30d threshold

    # Tighten threshold to 10d
    new_cfg = SystemConfig(bot_min_account_age_days=10)
    bot_filter.update_cfg(new_cfg)
    assert bot_filter.is_bot(post) is False  # now 25 >= 10, not a bot


# ── Source with no metadata ───────────────────────────────────────────────────

def test_reddit_post_with_no_author_metadata(bot_filter: BotFilter) -> None:
    """Reddit doesn't expose followers/age — should not be flagged."""
    post = make_post(
        source="reddit",
        author_account_age_days=0,
        author_followers=0,
        author_following=0,
        post_count_30d=0,
    )
    assert bot_filter.is_bot(post) is False
