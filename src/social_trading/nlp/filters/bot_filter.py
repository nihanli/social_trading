"""
BotFilter — removes bot and spam posts before NLP processing.

Thresholds come from SystemConfig and are adjustable via the Streamlit
Config page without service restarts.

Bot indicators (any one triggers exclusion):
  1. Account too new : account_age_days < cfg.bot_min_account_age_days (30d)
  2. Inverted ratio  : followers / following < cfg.bot_min_follower_following_ratio (0.1)
                       ⟹ the account follows 10× more than it is followed
  3. High velocity   : post_count_30d / 720 hr > cfg.bot_max_velocity_per_hour (50/hr)

StockTwits and Reddit do not expose follower counts or post velocity so
checks 2 and 3 are skipped when those fields are 0/missing.
"""
from __future__ import annotations

import logging

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SocialPost

logger = logging.getLogger(__name__)


class BotFilter:
    """
    Stateless bot/spam classifier.

    Pure function — no I/O, no Redis, no async.  Receives a SocialPost
    and returns True if the post should be dropped.
    """

    def __init__(self, cfg: SystemConfig) -> None:
        self._cfg = cfg

    # ── Public API ────────────────────────────────────────────────────────────

    def is_bot(self, post: SocialPost) -> bool:
        """
        Return True if the post should be treated as bot/spam.
        Any single indicator fires exclusion (conservative approach).
        """
        if self._too_new(post):
            logger.debug("bot filter: %s — account too new (%dd)", post.author_id, post.author_account_age_days)
            return True
        if self._inverted_ratio(post):
            logger.debug("bot filter: %s — inverted follower ratio", post.author_id)
            return True
        if self._high_velocity(post):
            logger.debug("bot filter: %s — high post velocity", post.author_id)
            return True
        return False

    def update_cfg(self, cfg: SystemConfig) -> None:
        """Replace config — called by service on each loop to pick up UI edits."""
        self._cfg = cfg

    # ── Checks ────────────────────────────────────────────────────────────────

    def _too_new(self, post: SocialPost) -> bool:
        """Account younger than minimum age threshold."""
        if post.author_account_age_days == 0:
            # Age not available for this source (Reddit, StockTwits) — skip check
            return False
        return post.author_account_age_days < self._cfg.bot_min_account_age_days

    def _inverted_ratio(self, post: SocialPost) -> bool:
        """
        followers / following < threshold.
        Skip if either value is 0 (field not populated by source).
        """
        followers = post.author_followers
        following = post.author_following
        if followers == 0 or following == 0:
            return False  # data not available — give benefit of the doubt
        ratio = followers / following
        return ratio < self._cfg.bot_min_follower_following_ratio

    def _high_velocity(self, post: SocialPost) -> bool:
        """
        Estimated hourly post rate exceeds threshold.
        post_count_30d / 720 hours ≈ posts per hour over 30-day window.
        """
        if post.post_count_30d == 0:
            return False  # field not populated — skip
        hourly_rate = post.post_count_30d / 720.0
        return hourly_rate > self._cfg.bot_max_velocity_per_hour
