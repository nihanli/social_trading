"""Unit tests for _ticker_in_cooldown — per-ticker re-entry cooldown gate."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from social_trading.core.models import Signal
from social_trading.services.risk_service import _ticker_in_cooldown


def _make_signal(ticker: str = "POET", generated_at: datetime | None = None) -> Signal:
    return Signal(
        ticker=ticker,
        direction="LONG",
        quality_score=0.75,
        sentiment_score=0.6,
        volume_z_score=2.0,
        momentum=0.03,
        convergence=0.5,
        source_post_count=10,
        generated_at=generated_at or datetime.now(UTC),
    )


def _mock_redis(last_at: datetime | None) -> AsyncMock:
    """Build a mock Redis that returns last_at for trade:last_at:{ticker}."""
    r = AsyncMock()
    if last_at is None:
        r.get = AsyncMock(return_value=None)
    else:
        r.get = AsyncMock(return_value=last_at.isoformat().encode())
    return r


# ── No prior trade (no cooldown key) ─────────────────────────────────────────

def test_no_prior_trade_passes():
    """Ticker with no trade:last_at key is not in cooldown."""
    sig = _make_signal()
    redis = _mock_redis(last_at=None)
    in_cd, reason = asyncio.run(
        _ticker_in_cooldown(redis, sig)
    )
    assert not in_cd
    assert reason == ""


# ── Trade too recent ──────────────────────────────────────────────────────────

def test_trade_30min_ago_blocked():
    """Trade 30 minutes before signal time is within cooldown — rejected."""
    sig_time = datetime.now(UTC)
    last_at = sig_time - timedelta(minutes=30)
    sig = _make_signal(generated_at=sig_time)
    redis = _mock_redis(last_at=last_at)
    in_cd, reason = asyncio.run(
        _ticker_in_cooldown(redis, sig)
    )
    assert in_cd
    assert "cooldown" in reason
    assert "remaining" in reason


def test_trade_59min_ago_blocked():
    """Trade 59 minutes before signal time is still within the 1-hour cooldown."""
    sig_time = datetime.now(UTC)
    last_at = sig_time - timedelta(minutes=59)
    sig = _make_signal(generated_at=sig_time)
    redis = _mock_redis(last_at=last_at)
    in_cd, _ = asyncio.run(
        _ticker_in_cooldown(redis, sig)
    )
    assert in_cd


# ── Trade old enough ──────────────────────────────────────────────────────────

def test_trade_61min_ago_passes():
    """Trade 61 minutes before signal time is outside the cooldown — allowed."""
    sig_time = datetime.now(UTC)
    last_at = sig_time - timedelta(minutes=61)
    sig = _make_signal(generated_at=sig_time)
    redis = _mock_redis(last_at=last_at)
    in_cd, _ = asyncio.run(
        _ticker_in_cooldown(redis, sig)
    )
    assert not in_cd


def test_trade_2hours_ago_passes():
    """Trade 2 hours before signal time is well outside cooldown."""
    sig_time = datetime.now(UTC)
    last_at = sig_time - timedelta(hours=2)
    sig = _make_signal(generated_at=sig_time)
    redis = _mock_redis(last_at=last_at)
    in_cd, _ = asyncio.run(
        _ticker_in_cooldown(redis, sig)
    )
    assert not in_cd


# ── Uses signal time (not wall clock) ────────────────────────────────────────

def test_stale_signal_predating_trade_is_blocked():
    """A stale signal whose generated_at predates the last trade is blocked.

    Scenario: signal generated at T-90min, trade executed at T-80min (after the signal).
    The ticker was traded since this signal was generated → cooldown applies.
    """
    now = datetime.now(UTC)
    sig_time = now - timedelta(minutes=90)   # signal is 90 min old (stale)
    last_at  = now - timedelta(minutes=80)   # trade happened 80 min ago (after signal)
    # sig_time - last_at = -10 min → elapsed is negative
    # The ticker was traded after this signal was generated; block the signal.
    sig = _make_signal(generated_at=sig_time)
    redis = _mock_redis(last_at=last_at)
    in_cd, _ = asyncio.run(
        _ticker_in_cooldown(redis, sig)
    )
    assert in_cd


def test_signal_after_recent_trade_blocked():
    """Signal generated 10 min after a trade is within cooldown."""
    now = datetime.now(UTC)
    last_at  = now - timedelta(minutes=40)   # trade 40 min ago
    sig_time = now - timedelta(minutes=30)   # signal 30 min ago (10 min after trade)
    # elapsed = sig_time - last_at = 10 min → within 60 min cooldown
    sig = _make_signal(generated_at=sig_time)
    redis = _mock_redis(last_at=last_at)
    in_cd, _ = asyncio.run(
        _ticker_in_cooldown(redis, sig)
    )
    assert in_cd


# ── Custom cooldown window ────────────────────────────────────────────────────

def test_custom_cooldown_30min():
    """Custom 30-minute cooldown rejects a trade 20 minutes before signal."""
    sig_time = datetime.now(UTC)
    last_at = sig_time - timedelta(minutes=20)
    sig = _make_signal(generated_at=sig_time)
    redis = _mock_redis(last_at=last_at)
    in_cd, _ = asyncio.run(
        _ticker_in_cooldown(redis, sig, cooldown_secs=1800)
    )
    assert in_cd


def test_custom_cooldown_30min_passes_35min():
    """Custom 30-minute cooldown allows a trade 35 minutes before signal."""
    sig_time = datetime.now(UTC)
    last_at = sig_time - timedelta(minutes=35)
    sig = _make_signal(generated_at=sig_time)
    redis = _mock_redis(last_at=last_at)
    in_cd, _ = asyncio.run(
        _ticker_in_cooldown(redis, sig, cooldown_secs=1800)
    )
    assert not in_cd


# ── Redis error resilience ────────────────────────────────────────────────────

def test_redis_error_does_not_block():
    """If Redis raises an exception, the cooldown check fails open (not blocked)."""
    sig = _make_signal()
    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=Exception("redis down"))
    in_cd, _ = asyncio.run(
        _ticker_in_cooldown(redis, sig)
    )
    assert not in_cd


# ── Tz-naive last_at timestamp ────────────────────────────────────────────────

def test_tz_naive_last_at_handled():
    """Tz-naive timestamps in trade:last_at are treated as UTC."""
    from datetime import timezone  # noqa: PLC0415
    sig_time = datetime.now(UTC)
    # Tz-naive timestamp stored as ISO string
    last_at_naive = (sig_time - timedelta(minutes=20)).replace(tzinfo=None)
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=last_at_naive.isoformat().encode())
    sig = _make_signal(generated_at=sig_time)
    in_cd, _ = asyncio.run(
        _ticker_in_cooldown(redis, sig)
    )
    assert in_cd
