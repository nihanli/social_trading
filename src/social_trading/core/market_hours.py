"""
market_hours.py — NYSE/NASDAQ trading session guard.

Wraps exchange_calendars (XNYS) which correctly handles:
  - Regular hours: 9:30 AM – 4:00 PM US/Eastern, Monday–Friday
  - US market holidays (NYSE closed dates)
  - Early-close days (day after Thanksgiving, Christmas Eve, etc.)

Primary entry-point for callers::

    from social_trading.core.market_hours import MarketHours

    mh = MarketHours()
    if not mh.is_open():
        logger.info("Market closed — next open %s", mh.next_open())

The singleton ``NYSE`` is pre-built for the common case.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from functools import cached_property
from zoneinfo import ZoneInfo

import pandas as pd

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_UTC = timezone.utc


class MarketHours:
    """
    Trading session guard for a single exchange.

    Args:
        exchange: exchange_calendars mic code.  Default ``"XNYS"`` (NYSE/NASDAQ).
        tz:       Display timezone for log messages.  Defaults to US/Eastern.
    """

    def __init__(self, exchange: str = "XNYS", tz: ZoneInfo = _ET) -> None:
        self.exchange = exchange
        self.tz = tz

    @cached_property
    def _cal(self):
        """Lazy-load calendar (avoids slow import at module level)."""
        import exchange_calendars as ec  # noqa: PLC0415
        return ec.get_calendar(self.exchange)

    # ── Public API ────────────────────────────────────────────────────────────

    def is_open(self, dt: datetime | None = None) -> bool:
        """
        Return True if the exchange is currently open (or open at *dt*).

        Handles holidays, early closes, and weekends automatically.
        Falls back to a simple time-of-day/weekday heuristic when the
        calendar library throws (e.g. transient initialisation error) rather
        than unconditionally assuming closed which would block all trading.

        Args:
            dt: Timezone-aware datetime to check.  Defaults to ``datetime.now(UTC)``.
        """
        ts = self._to_pd_ts(dt)
        try:
            return bool(self._cal.is_open_at_time(ts))
        except Exception as exc:
            # Fallback: approximate NYSE hours Mon–Fri 9:30–16:00 ET.
            # Better than "assuming closed" which would incorrectly halt trading.
            now_et = (dt or datetime.now(_UTC)).astimezone(_ET)
            weekday = now_et.weekday()  # 0=Mon … 4=Fri
            open_time = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            close_time = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
            fallback = (weekday < 5) and (open_time <= now_et < close_time)
            logger.warning(
                "[MarketHours] is_open check failed (%s) — using time-based fallback: %s",
                exc,
                "OPEN" if fallback else "CLOSED",
            )
            return fallback

    def next_open(self, dt: datetime | None = None) -> datetime:
        """Return the next session open as a UTC-aware datetime."""
        ts = self._to_pd_ts(dt)
        try:
            nxt = self._cal.next_open(ts)
            return nxt.to_pydatetime().astimezone(_UTC)
        except Exception as exc:
            logger.warning("[MarketHours] next_open failed (%s)", exc)
            return datetime.now(_UTC) + timedelta(hours=1)

    def next_close(self, dt: datetime | None = None) -> datetime:
        """Return the current (or next) session close as a UTC-aware datetime."""
        ts = self._to_pd_ts(dt)
        try:
            nxt = self._cal.next_close(ts)
            return nxt.to_pydatetime().astimezone(_UTC)
        except Exception as exc:
            logger.warning("[MarketHours] next_close failed (%s)", exc)
            return datetime.now(_UTC) + timedelta(hours=1)

    def seconds_until_open(self, dt: datetime | None = None) -> float:
        """Seconds until the next session open (0.0 if market is already open)."""
        if self.is_open(dt):
            return 0.0
        now = datetime.now(_UTC) if dt is None else dt.astimezone(_UTC)
        delta = (self.next_open(dt) - now).total_seconds()
        return max(0.0, delta)

    def seconds_until_close(self, dt: datetime | None = None) -> float:
        """Seconds until current session close (0.0 if market is not open)."""
        if not self.is_open(dt):
            return 0.0
        now = datetime.now(_UTC) if dt is None else dt.astimezone(_UTC)
        delta = (self.next_close(dt) - now).total_seconds()
        return max(0.0, delta)

    def is_session_day(self, dt: datetime | None = None) -> bool:
        """Return True if *dt* falls on a trading day (even outside RTH)."""
        ts = self._to_pd_ts(dt)
        naive_date = pd.Timestamp(ts.date())
        try:
            return bool(self._cal.is_session(naive_date))
        except Exception:
            return False

    def trading_days_between(self, start: datetime, end: datetime) -> int:
        """
        Count completed NYSE trading days between *start* and *end*.

        Returns the number of session dates strictly between start.date() and
        end.date() (i.e. sessions_in_range minus the opening day itself).
        Same-day open → 0.  Start after end → 0.

        Degrades safely: returns 0 with a warning on any calendar error.
        """
        try:
            start_date = pd.Timestamp(start.date())
            end_date   = pd.Timestamp(end.date())
            if end_date <= start_date:
                return 0
            sessions = self._cal.sessions_in_range(start_date, end_date)
            # Subtract 1 so that opening and checking on the same session = 0 days held
            return max(0, len(sessions) - 1)
        except Exception as exc:
            logger.warning("[MarketHours] trading_days_between failed (%s) — returning 0", exc)
            return 0

    def nth_session_close(self, opened_at: datetime, n: int) -> datetime:
        """Return the UTC close of the *n*th trading session counting from the opening session.

        Session counting:
          n=1 — close of the session on ``opened_at.date()`` (same-day exit deadline).
                If that date is not a trading day (weekend/holiday), the first trading
                day on or after that date is used as session 1.
          n=2 — close of the session immediately after the opening session.
          … and so on.

        Early-close days (e.g. day before Thanksgiving) are handled correctly by
        ``exchange_calendars`` — their session close will be e.g. 13:00 ET, not 16:00 ET.

        Degrades safely: falls back to a weekday 16:00 ET heuristic on any calendar
        error so the exit loop never silently bypasses the time stop.

        Args:
            opened_at: Timezone-aware entry datetime.
            n:         Session count (>= 1).
        """
        try:
            start_date = pd.Timestamp(opened_at.date())
            # Extend window generously: n sessions + 30 extra days covers holidays
            end_date = start_date + pd.Timedelta(days=max(n * 2, 14) + 30)
            sessions = self._cal.sessions_in_range(start_date, end_date)
            if len(sessions) < n:
                raise ValueError(f"Only {len(sessions)} sessions found, need {n}")
            target_session = sessions[n - 1]
            close_ts = self._cal.session_close(target_session)
            return close_ts.to_pydatetime().astimezone(_UTC)
        except Exception as exc:
            logger.warning(
                "[MarketHours] nth_session_close(n=%d) failed (%s) — using 16:00 ET fallback",
                n, exc,
            )
            # Fallback: advance n-1 weekdays from opened_at's date, return 16:00 ET
            d = opened_at.astimezone(_ET).date()
            days_added = 0
            while days_added < n - 1:
                d += timedelta(days=1)
                if d.weekday() < 5:
                    days_added += 1
            return datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=_ET).astimezone(_UTC)

    def status_str(self, dt: datetime | None = None) -> str:
        """Human-readable status string for logging/UI."""
        if self.is_open(dt):
            secs = self.seconds_until_close(dt)
            h, rem = divmod(int(secs), 3600)
            m = rem // 60
            return f"OPEN — closes in {h}h {m:02d}m"
        next_open_dt = self.next_open(dt)
        local = next_open_dt.astimezone(self.tz)
        return f"CLOSED — next open {local.strftime('%a %b %-d %I:%M %p %Z')}"

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _to_pd_ts(dt: datetime | None) -> pd.Timestamp:
        """Convert an optional datetime to a UTC-aware pd.Timestamp."""
        if dt is None:
            return pd.Timestamp.now("UTC")
        if dt.tzinfo is None:
            raise ValueError("dt must be timezone-aware")
        return pd.Timestamp(dt).tz_convert("UTC")


# ── Module-level singleton ────────────────────────────────────────────────────

#: Pre-built NYSE/NASDAQ instance — import and use directly.
NYSE = MarketHours(exchange="XNYS")
