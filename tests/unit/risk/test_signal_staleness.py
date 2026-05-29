"""Unit tests for _signal_is_stale — signal approval age gate."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from social_trading.core.models import Signal
from social_trading.services.risk_service import _signal_is_stale


def _make_signal(generated_at: datetime) -> Signal:
    return Signal(
        ticker="NVDA",
        direction="LONG",
        quality_score=0.75,
        sentiment_score=0.6,
        volume_z_score=2.0,
        momentum=0.03,
        convergence=0.5,
        source_post_count=10,
        generated_at=generated_at,
    )


# ── Fresh signals (should pass) ────────────────────────────────────────────────

def test_fresh_signal_tz_aware_passes():
    """Signal generated 5 minutes ago with tz-aware timestamp is not stale."""
    sig = _make_signal(datetime.now(UTC) - timedelta(minutes=5))
    is_stale, age = _signal_is_stale(sig, max_age_minutes=10)
    assert not is_stale
    assert 290 <= age <= 310  # ~300s


def test_fresh_signal_tz_naive_passes():
    """Signal with tz-naive UTC timestamp is handled correctly."""
    sig = _make_signal(datetime.utcnow() - timedelta(minutes=3))
    is_stale, age = _signal_is_stale(sig, max_age_minutes=10)
    assert not is_stale
    assert age < 600


def test_signal_exactly_at_limit_passes():
    """Signal exactly at the limit (10 min) is not yet stale."""
    sig = _make_signal(datetime.now(UTC) - timedelta(minutes=10, seconds=-1))
    is_stale, _ = _signal_is_stale(sig, max_age_minutes=10)
    assert not is_stale


# ── Stale signals (should be rejected) ───────────────────────────────────────

def test_stale_signal_tz_aware_rejected():
    """Signal generated 15 minutes ago with tz-aware timestamp is stale."""
    sig = _make_signal(datetime.now(UTC) - timedelta(minutes=15))
    is_stale, age = _signal_is_stale(sig, max_age_minutes=10)
    assert is_stale
    assert age >= 900  # 15 min = 900s


def test_stale_signal_tz_naive_rejected():
    """Signal with tz-naive UTC timestamp older than limit is stale."""
    sig = _make_signal(datetime.utcnow() - timedelta(minutes=11))
    is_stale, _ = _signal_is_stale(sig, max_age_minutes=10)
    assert is_stale


def test_very_old_signal_rejected():
    """Signal from 1 hour ago is always stale."""
    sig = _make_signal(datetime.now(UTC) - timedelta(hours=1))
    is_stale, age = _signal_is_stale(sig, max_age_minutes=10)
    assert is_stale
    assert age >= 3600


# ── Configurable threshold ────────────────────────────────────────────────────

def test_custom_max_age_respected():
    """Custom threshold of 30 minutes accepts a 20-minute-old signal."""
    sig = _make_signal(datetime.now(UTC) - timedelta(minutes=20))
    is_stale, _ = _signal_is_stale(sig, max_age_minutes=30)
    assert not is_stale


def test_custom_max_age_rejects_old():
    """Custom threshold of 5 minutes rejects a 6-minute-old signal."""
    sig = _make_signal(datetime.now(UTC) - timedelta(minutes=6))
    is_stale, _ = _signal_is_stale(sig, max_age_minutes=5)
    assert is_stale


# ── Age value accuracy ─────────────────────────────────────────────────────────

def test_age_seconds_returned_accurately():
    """Returned age_seconds is within 2 seconds of expected."""
    sig = _make_signal(datetime.now(UTC) - timedelta(seconds=450))
    _, age = _signal_is_stale(sig, max_age_minutes=10)
    assert abs(age - 450) < 2
