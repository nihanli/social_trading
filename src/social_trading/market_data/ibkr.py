"""
IBKRMarketData — MarketDataProvider backed by ib_async.

Provides real-time and historical price data from Interactive Brokers
via reqMktData and reqHistoricalData.

This module wraps ib_async with an injectable IB client so that:
  - Unit tests can inject a fake IB object
  - The module imports gracefully even when ib_async is not installed

Design reference: docs/design/07-execution-ibkr.md §7b
Protocol reference: docs/plan/02-protocols-and-interfaces.md §3

Usage:
    from ib_async import IB
    ib = IB()
    await ib.connectAsync('127.0.0.1', 7497, clientId=11)  # paper
    provider = IBKRMarketData(ib=ib)
    quote = await provider.get_quote("AAPL")
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

_IB_AVAILABLE: bool
try:
    import ib_async as _ib_async_mod  # noqa: F401
    _IB_AVAILABLE = True
except ImportError:
    _IB_AVAILABLE = False


class IBKRMarketData:
    """
    Real-time market data provider via Interactive Brokers API.

    Requires an authenticated IB() connection. The caller is responsible
    for connecting and disconnecting the IB client.

    Args:
        ib: An ib_async.IB instance (or a duck-typed fake for tests).
    """

    def __init__(self, ib: Any) -> None:
        self._ib = ib

    async def get_quote(self, ticker: str) -> dict[str, Any]:
        """
        Return last price, bid, ask, and volume via reqMktData.

        Requests live data (type 1) first; falls back to delayed (type 3) when
        the live price is unavailable (no subscription, outside hours, etc.).
        Restores live mode after the call so the shared IB connection stays in
        live mode for other callers (e.g. get_market_prices in ibkr.py).
        """
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed")

        from ib_async import Stock  # noqa: PLC0415

        contract = Stock(ticker, "SMART", "USD")
        await self._ib.qualifyContractsAsync(contract)

        import math  # noqa: PLC0415

        def _safe(val: Any) -> float | None:
            """Return float if val is a real finite number, else None."""
            try:
                f = float(val)
                return f if math.isfinite(f) and f != 0.0 else None
            except (TypeError, ValueError):
                return None

        # Try live data first
        self._ib.reqMarketDataType(1)
        ticker_data = self._ib.reqMktData(contract, "", False, False)
        await asyncio.sleep(1.5)
        self._ib.cancelMktData(contract)

        last = _safe(ticker_data.last) or _safe(ticker_data.close)
        if last is None:
            # No live data (no subscription or outside hours) — retry with delayed
            self._ib.reqMarketDataType(3)
            ticker_data = self._ib.reqMktData(contract, "", False, False)
            await asyncio.sleep(1.5)
            self._ib.cancelMktData(contract)
            # Restore live mode so the shared connection stays in live mode
            self._ib.reqMarketDataType(1)
            last = _safe(ticker_data.last) or _safe(ticker_data.close)

        if last is None:
            # Error 10089 / no data received — raise so FallbackMarketData
            # can retry with yfinance.
            raise RuntimeError(f"IB returned no price for {ticker} (missing subscription?)")

        bid = _safe(ticker_data.bid) or last * 0.999
        ask = _safe(ticker_data.ask) or last * 1.001
        volume = _safe(ticker_data.volume) or 0.0

        return {
            "last": float(last),
            "bid": float(bid),
            "ask": float(ask),
            "volume": float(volume),
            "avg_volume_30d": float(_safe(ticker_data.avVolume) or 0.0),
            "market_cap": 0.0,   # not available via reqMktData; use YFinance for this
        }

    async def get_ohlcv(
        self,
        ticker: str,
        period: str = "1d",
        interval: str = "5m",
    ) -> list[dict[str, Any]]:
        """
        Return OHLCV bars via reqHistoricalData.

        Args:
            period:   '1d' | '5d' | '1mo' — converted to IBKR duration string
            interval: '1m' | '5m' | '1h' | '1d' — converted to IBKR bar size
        """
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed")

        from ib_async import Stock  # noqa: PLC0415
        contract = Stock(ticker, "SMART", "USD")
        await self._ib.qualifyContractsAsync(contract)

        duration = _period_to_ibkr(period)
        bar_size = _interval_to_ibkr(interval)

        bars_raw = await self._ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )
        return [
            {
                "timestamp": b.date.isoformat() if hasattr(b.date, "isoformat") else str(b.date),
                "open": float(b.open),
                "high": float(b.high),
                "low": float(b.low),
                "close": float(b.close),
                "volume": float(b.volume),
            }
            for b in bars_raw
        ]

    async def get_atr(self, ticker: str, period: int = 14) -> float:
        """Compute ATR from recent daily bars."""
        bars = await self.get_ohlcv(ticker, period=f"{period * 3}d", interval="1d")
        if len(bars) < period:
            return 0.0

        tr = []
        for i in range(1, len(bars)):
            h = bars[i]["high"]
            low = bars[i]["low"]
            pc = bars[i - 1]["close"]
            true_range = max(h - low, abs(h - pc), abs(low - pc))
            tr.append(true_range)

        if len(tr) < period:
            return 0.0

        atr = sum(tr[:period]) / period
        for i in range(period, len(tr)):
            atr = (atr * (period - 1) + tr[i]) / period
        return float(atr)

    async def get_vix(self) -> float:
        """Return current VIX via reqMktData on the VIX index contract."""
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed")

        from ib_async import Index  # noqa: PLC0415
        import math  # noqa: PLC0415

        contract = Index("VIX", "CBOE", "USD")
        # VIX is an index — live data (type 1) should always be available via CBOE
        self._ib.reqMarketDataType(1)
        ticker_data = self._ib.reqMktData(contract, "", False, False)
        await asyncio.sleep(1.5)
        self._ib.cancelMktData(contract)
        for candidate in (ticker_data.last, ticker_data.close):
            try:
                v = float(candidate)
                if math.isfinite(v) and v > 0:
                    return v
            except (TypeError, ValueError):
                pass
        raise RuntimeError("IB returned no VIX price")

    async def health_check(self) -> bool:
        return bool(self._ib.isConnected())


# ── Conversion helpers ────────────────────────────────────────────────────────

def _period_to_ibkr(period: str) -> str:
    """Convert yfinance-style period to IBKR durationStr."""
    mapping = {
        "1d": "1 D", "5d": "5 D", "1mo": "1 M",
        "3mo": "3 M", "6mo": "6 M", "1y": "1 Y",
        "2y": "2 Y", "5y": "5 Y",
    }
    return mapping.get(period, "1 D")


def _interval_to_ibkr(interval: str) -> str:
    """Convert yfinance-style interval to IBKR barSizeSetting."""
    mapping = {
        "1m": "1 min", "5m": "5 mins", "15m": "15 mins",
        "30m": "30 mins", "1h": "1 hour", "1d": "1 day",
    }
    return mapping.get(interval, "5 mins")
