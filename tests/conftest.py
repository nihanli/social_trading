"""
Shared pytest fixtures for all tests.

Key fakes that satisfy the core protocols without external dependencies:
  - redis      → fakeredis.FakeRedis (in-memory)
  - cfg        → default SystemConfig loaded into fake Redis
  - bus        → FakeEventBus (collects published events in memory)
  - sample_*   → pre-built model instances for test convenience
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import fakeredis.aioredis as fakeredis
import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import (
    AccountState,
    SentimentResult,
    Signal,
    SocialPost,
)

# ── Infrastructure Fakes ─────────────────────────────────────────────────────

@pytest.fixture
def redis():
    """In-memory Redis — no real Redis server required."""
    return fakeredis.FakeRedis()


@pytest.fixture
async def cfg(redis):
    """Default SystemConfig persisted to fake Redis."""
    c = SystemConfig()
    await c.save(redis)
    return c


class FakeEventBus:
    """In-memory event bus that records all published events."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []
        self._counter = 0

    async def publish(self, stream: str, event: dict[str, Any]) -> str:
        self._counter += 1
        self.published.append({"_stream": stream, **event})
        return f"fake-{self._counter}"

    async def consume(
        self, stream: str, group: str, consumer: str,
        count: int = 10, block_ms: int = 2000,
    ) -> list[tuple[str, dict[str, Any]]]:
        return []

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        pass

    async def create_group(self, stream: str, group: str) -> None:
        pass

    def events_on(self, stream: str) -> list[dict[str, Any]]:
        """Helper: return all events published to a given stream."""
        return [e for e in self.published if e.get("_stream") == stream]


@pytest.fixture
def bus() -> FakeEventBus:
    return FakeEventBus()


# ── Sample Data Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def sample_post() -> SocialPost:
    return SocialPost(
        id="post-001",
        source="twitter",
        ticker="AAPL",
        text="$AAPL absolutely crushing it today. Strong buy.",
        author_id="user-123",
        author_followers=5000,
        author_account_age_days=365,
        author_following=400,
        post_count_30d=20,
        likes=150,
        reposts=30,
        is_original=True,
        collected_at=datetime.now(UTC),
    )


@pytest.fixture
def sample_signal() -> Signal:
    return Signal(
        ticker="AAPL",
        direction="LONG",
        quality_score=0.75,
        sentiment_score=0.65,
        volume_z_score=2.5,
        momentum=0.04,
        convergence=0.80,
        source_post_count=42,
    )


@pytest.fixture
def sample_sentiment(sample_post: SocialPost) -> SentimentResult:
    return SentimentResult(
        post_id=sample_post.id,
        ticker=sample_post.ticker,
        positive=0.80,
        negative=0.05,
        neutral=0.15,
        score=0.75,
        model="finbert",
        latency_ms=28.0,
    )


@pytest.fixture
def sample_account() -> AccountState:
    return AccountState(
        net_liquidation=100_000.0,
        cash=100_000.0,
        daily_pnl=0.0,
        weekly_pnl=0.0,
        drawdown_pct=0.0,
        open_positions=[],
    )
