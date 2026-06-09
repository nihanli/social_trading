"""Unit tests for social_trading.core.market_hours."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from social_trading.core.market_hours import MarketHours

_UTC = timezone.utc


def _dt(iso: str) -> datetime:
    """Parse an ISO string that already contains UTC offset."""
    return datetime.fromisoformat(iso)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def mh() -> MarketHours:
    """Fresh MarketHours instance (NYSE, no cached_property pollution)."""
    return MarketHours()


# ── is_open ───────────────────────────────────────────────────────────────────

class TestIsOpen:
    def test_open_during_regular_hours(self, mh):
        # 2026-05-28 is a Thursday; 14:00 ET = 18:00 UTC
        dt = _dt("2026-05-28T18:00:00+00:00")
        assert mh.is_open(dt) is True

    def test_closed_before_open(self, mh):
        # 08:00 ET = 12:00 UTC — before 9:30 open
        dt = _dt("2026-05-28T12:00:00+00:00")
        assert mh.is_open(dt) is False

    def test_closed_after_close(self, mh):
        # 17:00 ET = 21:00 UTC — after 4:00 PM close
        dt = _dt("2026-05-28T21:00:00+00:00")
        assert mh.is_open(dt) is False

    def test_closed_on_weekend_saturday(self, mh):
        # 2026-05-30 is Saturday; midday ET
        dt = _dt("2026-05-30T17:00:00+00:00")
        assert mh.is_open(dt) is False

    def test_closed_on_weekend_sunday(self, mh):
        dt = _dt("2026-05-31T17:00:00+00:00")
        assert mh.is_open(dt) is False

    def test_closed_on_holiday_christmas(self, mh):
        # Christmas 2025 — NYSE closed
        dt = _dt("2025-12-25T18:00:00+00:00")
        assert mh.is_open(dt) is False

    def test_closed_on_holiday_independence_day(self, mh):
        # July 4 2025 is a Friday; NYSE closed
        dt = _dt("2025-07-04T17:00:00+00:00")
        assert mh.is_open(dt) is False

    def test_early_close_day_after_thanksgiving(self, mh):
        # Day after Thanksgiving 2025 (Nov 28): closes at 1 PM ET = 18:00 UTC
        before_early_close = _dt("2025-11-28T17:00:00+00:00")  # 12:00 ET, open
        after_early_close = _dt("2025-11-28T18:30:00+00:00")   # 13:30 ET, closed
        assert mh.is_open(before_early_close) is True
        assert mh.is_open(after_early_close) is False

    def test_open_at_exactly_930(self, mh):
        # 9:30 AM ET = 13:30 UTC
        dt = _dt("2026-05-28T13:30:00+00:00")
        assert mh.is_open(dt) is True

    def test_closed_at_exactly_4pm(self, mh):
        # 4:00 PM ET = 20:00 UTC
        dt = _dt("2026-05-28T20:00:00+00:00")
        assert mh.is_open(dt) is False

    def test_requires_tz_aware_datetime(self, mh):
        with pytest.raises(ValueError, match="timezone-aware"):
            mh.is_open(datetime(2026, 5, 28, 14, 0, 0))  # naive


# ── is_session_day ────────────────────────────────────────────────────────────

class TestIsSessionDay:
    def test_regular_weekday(self, mh):
        assert mh.is_session_day(_dt("2026-05-28T00:00:00+00:00")) is True

    def test_weekend(self, mh):
        assert mh.is_session_day(_dt("2026-05-30T00:00:00+00:00")) is False

    def test_holiday(self, mh):
        assert mh.is_session_day(_dt("2025-12-25T00:00:00+00:00")) is False


# ── next_open / next_close ────────────────────────────────────────────────────

class TestNextOpenClose:
    def test_next_open_when_closed_returns_future_datetime(self, mh):
        # Friday after market hours — next open should be Monday
        dt = _dt("2026-05-29T21:00:00+00:00")  # Friday 5 PM ET
        nxt = mh.next_open(dt)
        assert nxt.tzinfo is not None
        assert nxt > dt

    def test_next_close_when_open_returns_future_datetime(self, mh):
        dt = _dt("2026-05-28T16:00:00+00:00")  # Thursday noon ET
        nxt = mh.next_close(dt)
        assert nxt.tzinfo is not None
        assert nxt > dt

    def test_next_open_skips_weekend(self, mh):
        # Saturday — next open must be Monday (skip Sunday)
        saturday = _dt("2026-05-30T17:00:00+00:00")
        nxt = mh.next_open(saturday)
        # Monday 2026-06-01 open at 9:30 ET = 13:30 UTC
        assert nxt.weekday() == 0  # Monday


# ── seconds_until_open / close ────────────────────────────────────────────────

class TestSecondsUntil:
    def test_seconds_until_open_zero_when_market_open(self, mh):
        dt = _dt("2026-05-28T16:00:00+00:00")
        assert mh.seconds_until_open(dt) == 0.0

    def test_seconds_until_open_positive_when_closed(self, mh):
        dt = _dt("2026-05-29T21:00:00+00:00")
        secs = mh.seconds_until_open(dt)
        assert secs > 0

    def test_seconds_until_close_zero_when_closed(self, mh):
        dt = _dt("2026-05-29T21:00:00+00:00")
        assert mh.seconds_until_close(dt) == 0.0

    def test_seconds_until_close_positive_when_open(self, mh):
        dt = _dt("2026-05-28T16:00:00+00:00")
        secs = mh.seconds_until_close(dt)
        assert secs > 0


# ── status_str ────────────────────────────────────────────────────────────────

class TestStatusStr:
    def test_status_open_contains_closes(self, mh):
        dt = _dt("2026-05-28T16:00:00+00:00")
        s = mh.status_str(dt)
        assert "OPEN" in s
        assert "closes" in s

    def test_status_closed_contains_next_open(self, mh):
        dt = _dt("2026-05-29T21:00:00+00:00")
        s = mh.status_str(dt)
        assert "CLOSED" in s
        assert "next open" in s


# ── Error resilience ──────────────────────────────────────────────────────────

class TestErrorResilience:
    def test_is_open_returns_false_on_calendar_error(self, mh):
        """On calendar error during market hours, time-based fallback returns True.
        On calendar error outside market hours, time-based fallback returns False."""
        # 16:00 UTC = 12:00 PM ET Thursday — inside market hours → fallback True
        dt_open = _dt("2026-05-28T16:00:00+00:00")
        with patch.object(mh, "_cal", create=True) as mock_cal:
            mock_cal.is_open_at_time.side_effect = RuntimeError("network error")
            assert mh.is_open(dt_open) is True  # time-based fallback: OPEN

        # 23:00 UTC = 7:00 PM ET Thursday — outside market hours → fallback False
        dt_closed = _dt("2026-05-28T23:00:00+00:00")
        with patch.object(mh, "_cal", create=True) as mock_cal:
            mock_cal.is_open_at_time.side_effect = RuntimeError("network error")
            assert mh.is_open(dt_closed) is False  # time-based fallback: CLOSED

    def test_module_singleton_is_importable(self):
        from social_trading.core.market_hours import NYSE
        assert NYSE is not None
        assert NYSE.exchange == "XNYS"
