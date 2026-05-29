"""
YFinanceMarketData — MarketDataProvider backed by yfinance.

Used for:
  - Paper trading (price feeds, ATR, ADV)
  - Backtesting / signal evaluation when IBKR is not connected
  - Watchlist liquidity checks

The yfinance download function is injectable so tests can run without
network access and without the yfinance package installed.

Design reference: docs/plan/02-protocols-and-interfaces.md §3

Usage:
    provider = YFinanceMarketData()
    quote = await provider.get_quote("AAPL")
    atr   = await provider.get_atr("AAPL", period=14)
    vix   = await provider.get_vix()
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# Type alias for the injectable download function
DownloadFn = Callable[..., Any]


def _default_download() -> DownloadFn:
    """Lazy import of yfinance.download — avoids hard dependency at import time."""
    try:
        import yfinance as yf  # noqa: PLC0415
        return yf.download
    except ImportError as exc:
        raise ImportError(
            "yfinance is not installed. Run: pip install yfinance"
        ) from exc


def _default_ticker_fn() -> Callable[[str], Any]:
    """Lazy import of yfinance.Ticker."""
    try:
        import yfinance as yf  # noqa: PLC0415
        return yf.Ticker
    except ImportError as exc:
        raise ImportError(
            "yfinance is not installed. Run: pip install yfinance"
        ) from exc


class YFinanceMarketData:
    """
    Async market data provider using yfinance.

    Runs blocking yfinance calls in an executor to avoid blocking the event loop.

    Args:
        download_fn:  Injectable replacement for yfinance.download (for tests).
        ticker_fn:    Injectable replacement for yfinance.Ticker (for tests).
    """

    def __init__(
        self,
        download_fn: DownloadFn | None = None,
        ticker_fn: Callable[[str], Any] | None = None,
    ) -> None:
        self._download_fn = download_fn
        self._ticker_fn = ticker_fn

    def _get_download(self) -> DownloadFn:
        return self._download_fn if self._download_fn is not None else _default_download()

    def _get_ticker(self, symbol: str) -> Any:
        fn = self._ticker_fn if self._ticker_fn is not None else _default_ticker_fn()
        return fn(symbol)

    async def get_quote(self, ticker: str) -> dict[str, Any]:
        """
        Return latest quote including last price, bid, ask, volume, and ADV.

        Uses fast_info (lightweight) first; falls back to full info dict only
        for fields not available there (avg_volume_30d, market_cap).

        Returns dict with keys:
            last, bid, ask, volume, avg_volume_30d, market_cap
        """
        loop = asyncio.get_event_loop()
        t = self._get_ticker(ticker)

        def _fetch() -> dict[str, Any]:
            def _f(val: Any, default: float = 0.0) -> float:
                try:
                    return float(val) if val is not None else default
                except (TypeError, ValueError):
                    return default

            # fast_info is a lightweight endpoint — avoids the heavy /v10/finance/quoteSummary
            # call that frequently returns HTTP 400 when called at high frequency.
            fi = t.fast_info
            last = _f(getattr(fi, "last_price", None)) or _f(getattr(fi, "previous_close", None))
            volume = _f(getattr(fi, "last_volume", None))
            market_cap = _f(getattr(fi, "market_cap", None))

            # bid/ask not in fast_info — synthesise from last
            bid = last * 0.999 if last else 0.0
            ask = last * 1.001 if last else 0.0

            # three_month_average_volume and market_cap are in fast_info on yfinance ≥ 0.2.x
            avg_volume_30d = _f(getattr(fi, "three_month_average_volume", None))

            return {
                "last": last,
                "bid": bid,
                "ask": ask,
                "volume": volume,
                "avg_volume_30d": avg_volume_30d,
                "market_cap": market_cap,
            }

        return await loop.run_in_executor(None, _fetch)

    async def get_ohlcv(
        self,
        ticker: str,
        period: str = "1d",
        interval: str = "5m",
    ) -> list[dict[str, Any]]:
        """
        Return OHLCV bars sorted ascending by timestamp.

        Args:
            period:   yfinance period string e.g. "1d", "5d", "1mo"
            interval: bar interval e.g. "1m", "5m", "1h", "1d"
        """
        loop = asyncio.get_event_loop()
        download = self._get_download()

        def _fetch() -> list[dict[str, Any]]:
            df = download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
            )
            if df is None or df.empty:
                return []
            # Newer yfinance returns MultiIndex columns (e.g. ('Close', 'MWC')); flatten them.
            if hasattr(df.columns, "levels"):
                df.columns = df.columns.droplevel(1)
            bars = []
            for ts, row in df.iterrows():
                bars.append({
                    "timestamp": ts.isoformat(),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": float(row["Volume"]),
                })
            return bars

        return await loop.run_in_executor(None, _fetch)

    async def get_atr(self, ticker: str, period: int = 14) -> float:
        """
        Compute ATR(period) from daily bars using the Wilder method.

        Returns price units (e.g. 3.50 means $3.50 ATR for a $150 stock).
        Returns 0.0 if insufficient data.
        """
        loop = asyncio.get_event_loop()
        download = self._get_download()

        def _fetch() -> float:
            df = download(
                ticker,
                period=f"{period * 3}d",  # extra days for warm-up
                interval="1d",
                auto_adjust=True,
                progress=False,
            )
            if df is None or len(df) < period:
                return 0.0
            # Newer yfinance returns MultiIndex columns; flatten them.
            if hasattr(df.columns, "levels"):
                df.columns = df.columns.droplevel(1)

            highs = df["High"].values
            lows = df["Low"].values
            closes = df["Close"].values

            # True range
            tr = []
            for i in range(1, len(closes)):
                true_range = max(
                    highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]),
                )
                tr.append(true_range)

            if len(tr) < period:
                return 0.0

            # Wilder smoothing
            atr = sum(tr[:period]) / period
            for i in range(period, len(tr)):
                atr = (atr * (period - 1) + tr[i]) / period

            return float(atr)

        return await loop.run_in_executor(None, _fetch)

    async def get_vix(self) -> float:
        """Return current VIX level from ^VIX. Returns 20.0 on failure."""
        try:
            loop = asyncio.get_event_loop()
            t = self._get_ticker("^VIX")

            def _fetch() -> float:
                info = t.info or {}
                val = info.get("regularMarketPrice") or info.get("previousClose", 20.0)
                return float(val) if val is not None else 20.0

            return await loop.run_in_executor(None, _fetch)
        except Exception as exc:
            logger.warning("VIX fetch failed: %s — returning 20.0", exc)
            return 20.0

    async def get_realised_vol(self, ticker: str, days: int = 30) -> float:
        """
        Compute annualised realised volatility from daily log returns.
        Returns 0.20 (20%) on failure.
        """
        try:
            bars = await self.get_ohlcv(ticker, period=f"{days + 5}d", interval="1d")
            if len(bars) < 10:
                return 0.20
            import math
            closes = [b["close"] for b in bars]
            log_returns = [
                math.log(closes[i] / closes[i - 1])
                for i in range(1, len(closes))
                if closes[i - 1] > 0
            ]
            if not log_returns:
                return 0.20
            mean = sum(log_returns) / len(log_returns)
            variance = sum((r - mean) ** 2 for r in log_returns) / len(log_returns)
            daily_vol = variance ** 0.5
            return daily_vol * (252 ** 0.5)  # annualise
        except Exception as exc:
            logger.warning("Realised vol fetch failed for %s: %s", ticker, exc)
            return 0.20

    async def health_check(self) -> bool:
        """Check connectivity by fetching SPY quote."""
        try:
            quote = await self.get_quote("SPY")
            return quote.get("last", 0.0) > 0
        except Exception:
            return False
