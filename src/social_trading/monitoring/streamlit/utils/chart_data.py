"""
chart_data.py — OHLCV data fetcher for the Streamlit chart page.

Strategy:
  1. Try Interactive Brokers (reqHistoricalData) — real-time, no rate limits.
  2. Fall back to yfinance if IB is unavailable or errors.

IB is accessed via a short-lived connection (clientId=21 by default) so this
module does not interfere with the execution service (clientId=10).  The async
IB call is run synchronously from Streamlit via asyncio.run().

Supported timeframes (period, interval):
  "5D"  → 5 days,  1-hour bars   (IB: "5 D"  / "1 hour")
  "1M"  → 1 month, daily bars    (IB: "1 M"  / "1 day")
  "3M"  → 3 months,daily bars    (IB: "3 M"  / "1 day")
  "6M"  → 6 months,daily bars    (IB: "6 M"  / "1 day")
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import pandas as pd

from social_trading.monitoring.streamlit.utils.db import LOCAL_TZ_NAME

logger = logging.getLogger(__name__)

# ── Timeframe definitions ──────────────────────────────────────────────────────

TIMEFRAMES: dict[str, dict[str, str]] = {
    "5D": {"period": "5d",  "interval": "1h",  "label": "5 Days (hourly)"},
    "1M": {"period": "1mo", "interval": "1d",  "label": "1 Month (daily)"},
    "3M": {"period": "3mo", "interval": "1d",  "label": "3 Months (daily)"},
    "6M": {"period": "6mo", "interval": "1d",  "label": "6 Months (daily)"},
}


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_ohlcv(ticker: str, timeframe: str = "1M") -> tuple[pd.DataFrame, str]:
    """
    Fetch OHLCV bars for *ticker* for the given *timeframe* key.

    Returns:
        (df, source) where df has columns Open/High/Low/Close/Volume with a
        DatetimeIndex, and source is "IB" or "yfinance".

    Raises:
        ValueError if no data could be fetched from either source.
    """
    tf = TIMEFRAMES.get(timeframe, TIMEFRAMES["1M"])
    period   = tf["period"]
    interval = tf["interval"]

    # ── 1. Try IB ─────────────────────────────────────────────────────────────
    try:
        bars = _fetch_ib(ticker, period=period, interval=interval)
        if bars:
            return _bars_to_df(bars), "IB"
        logger.debug("IB returned empty bars for %s — trying yfinance", ticker)
    except Exception as exc:
        logger.debug("IB fetch failed for %s (%s) — trying yfinance", ticker, exc)

    # ── 2. Fall back to yfinance ───────────────────────────────────────────────
    bars = _fetch_yfinance(ticker, period=period, interval=interval)
    if not bars:
        raise ValueError(f"No OHLCV data available for {ticker!r} ({timeframe})")
    return _bars_to_df(bars), "yfinance"


# ── IB fetcher ────────────────────────────────────────────────────────────────

def _fetch_ib(ticker: str, period: str, interval: str) -> list[dict[str, Any]]:
    """
    Connect to IB, fetch historical bars, and disconnect.
    Runs inside asyncio.run() — safe to call from Streamlit's sync context.
    Raises on any error so the caller can fall back to yfinance.
    """
    try:
        from ib_async import IB, Stock  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError("ib_async not installed") from exc

    async def _run() -> list[dict[str, Any]]:
        ib = IB()
        host = os.getenv("IBKR_HOST", "127.0.0.1")
        port = int(os.getenv("IBKR_PORT", "7497"))
        client_id = int(os.getenv("IBKR_CHART_CLIENT_ID", "21"))

        await asyncio.wait_for(
            ib.connectAsync(host, port, clientId=client_id),
            timeout=5.0,
        )
        try:
            contract = Stock(ticker, "SMART", "USD")
            await ib.qualifyContractsAsync(contract)

            from social_trading.market_data.ibkr import (  # noqa: PLC0415
                _period_to_ibkr,
                _interval_to_ibkr,
            )
            bars_raw = await asyncio.wait_for(
                ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime="",
                    durationStr=_period_to_ibkr(period),
                    barSizeSetting=_interval_to_ibkr(interval),
                    whatToShow="TRADES",
                    useRTH=True,
                    formatDate=1,
                ),
                timeout=10.0,
            )
            return [
                {
                    "timestamp": b.date.isoformat()
                    if hasattr(b.date, "isoformat")
                    else str(b.date),
                    "open":   float(b.open),
                    "high":   float(b.high),
                    "low":    float(b.low),
                    "close":  float(b.close),
                    "volume": float(b.volume),
                }
                for b in bars_raw
            ]
        finally:
            ib.disconnect()

    return asyncio.run(_run())


# ── yfinance fetcher ──────────────────────────────────────────────────────────

def _fetch_yfinance(ticker: str, period: str, interval: str) -> list[dict[str, Any]]:
    """Fetch OHLCV bars via yfinance (synchronous)."""
    try:
        import yfinance as yf  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError("yfinance not installed") from exc

    t = yf.Ticker(ticker)
    df = t.history(period=period, interval=interval, auto_adjust=True)
    if df is None or df.empty:
        return []

    bars = []
    for ts, row in df.iterrows():
        bars.append({
            "timestamp": ts.isoformat(),
            "open":   float(row["Open"]),
            "high":   float(row["High"]),
            "low":    float(row["Low"]),
            "close":  float(row["Close"]),
            "volume": float(row["Volume"]),
        })
    return bars


# ── Conversion ────────────────────────────────────────────────────────────────

def _bars_to_df(bars: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert list-of-dicts to a DataFrame with a tz-aware DatetimeIndex.

    Parses all timestamp strings via UTC (utc=True) to avoid the
    "Mixed timezones detected" error that arises when bars span a DST
    transition and some strings carry -0400 while others carry -0500.
    The index is then converted to LOCAL_TZ_NAME for display.
    """
    df = pd.DataFrame(bars)
    # utc=True normalises everything — tz-aware strings are converted to UTC,
    # tz-naive strings (e.g. IB daily "2024-06-24" date-only) are treated as UTC.
    # format='ISO8601' accepts both date-only and full ISO datetime strings in
    # the same series without raising "Mixed timezones detected".
    parsed = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    df["datetime"] = parsed.dt.tz_convert(LOCAL_TZ_NAME)
    df = df.set_index("datetime").sort_index()
    df = df.rename(columns={
        "open": "Open", "high": "High",
        "low": "Low", "close": "Close", "volume": "Volume",
    })
    df = df[["Open", "High", "Low", "Close", "Volume"]]
    return df
