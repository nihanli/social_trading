"""Unit tests for MarketHours.nth_session_close()."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from social_trading.core.market_hours import NYSE

_ET = ZoneInfo("America/New_York")


def _et(year: int, month: int, day: int, hour: int = 10, minute: int = 0) -> datetime:
    """Build a timezone-aware ET datetime."""
    return datetime(year, month, day, hour, minute, tzinfo=_ET)


class TestNthSessionClose:
    """nth_session_close(opened_at, n) returns the correct session close UTC datetime."""

    def test_n1_returns_same_day_close(self) -> None:
        """n=1 on a normal trading day returns that day's 4:00 PM ET close (as UTC)."""
        # 2024-01-03 is a Wednesday (regular session, no holiday)
        opened_at = _et(2024, 1, 3, 10, 30)
        result = NYSE.nth_session_close(opened_at, n=1)
        result_et = result.astimezone(_ET)
        assert result_et.date() == opened_at.date()
        assert result_et.hour == 16
        assert result_et.minute == 0

    def test_n2_returns_next_session_close(self) -> None:
        """n=2 returns the next trading day's close."""
        # 2024-01-03 Wed → n=2 → 2024-01-04 Thu
        opened_at = _et(2024, 1, 3, 10, 30)
        result = NYSE.nth_session_close(opened_at, n=2)
        result_et = result.astimezone(_ET)
        assert result_et.date() > opened_at.date()
        assert result_et.hour == 16

    def test_n1_monday_skips_weekend(self) -> None:
        """n=1 on a Monday returns Monday's close (not the preceding Friday)."""
        # 2024-01-08 is a Monday
        opened_at = _et(2024, 1, 8, 9, 35)
        result = NYSE.nth_session_close(opened_at, n=1)
        result_et = result.astimezone(_ET)
        assert result_et.weekday() == 0  # Monday

    def test_friday_n2_skips_weekend(self) -> None:
        """n=2 on a Friday returns the following Monday (skips weekend)."""
        # 2024-01-05 is a Friday
        opened_at = _et(2024, 1, 5, 10, 0)
        result = NYSE.nth_session_close(opened_at, n=2)
        result_et = result.astimezone(_ET)
        # Next session after Friday is Monday 2024-01-08
        assert result_et.weekday() == 0  # Monday
        assert result_et.date() == datetime(2024, 1, 8).date()

    def test_returns_utc_aware_datetime(self) -> None:
        """Result is always UTC-aware."""
        opened_at = _et(2024, 1, 3, 10, 0)
        result = NYSE.nth_session_close(opened_at, n=1)
        assert result.tzinfo is not None
        assert result.tzinfo == UTC or result.utcoffset() == timedelta(0)

    def test_deadline_is_before_now_for_past_session(self) -> None:
        """A deadline in the past (n=1, old date) is indeed before now."""
        opened_at = _et(2023, 1, 3, 10, 0)  # over a year ago
        deadline = NYSE.nth_session_close(opened_at, n=1)
        assert deadline < datetime.now(UTC)

    def test_n1_open_after_close_still_returns_same_date(self) -> None:
        """Even if opened 'after close' time on a session day, n=1 returns that day's close.
        The close is already in the past — this is expected and the exit evaluator will
        fire TIME_STOP immediately on first evaluation for such positions.
        """
        # Opened at 5 PM ET — after close
        opened_at = _et(2024, 1, 3, 17, 0)
        result = NYSE.nth_session_close(opened_at, n=1)
        result_et = result.astimezone(_ET)
        assert result_et.date() == opened_at.date()

    def test_n3(self) -> None:
        """n=3 on a Wednesday returns Friday's close."""
        # 2024-01-03 Wed → Wed, Thu, Fri → n=3 = Fri 2024-01-05
        opened_at = _et(2024, 1, 3, 10, 0)
        result = NYSE.nth_session_close(opened_at, n=3)
        result_et = result.astimezone(_ET)
        assert result_et.weekday() == 4  # Friday
        assert result_et.date() == datetime(2024, 1, 5).date()
