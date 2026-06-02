"""
FallbackMarketData — composite provider that tries a primary source first,
then transparently falls back to a secondary source on any failure.

Typical usage:
    provider = FallbackMarketData(
        primary=IBKRMarketData(ib),
        secondary=YFinanceMarketData(),
    )

When IBKR is connected, real-time IB data is used for quotes and VIX.
Historical data (ATR, realised vol) also comes from IB; yfinance covers
any ticker IB can't resolve (e.g., OTC names, delayed-data gaps).
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class FallbackMarketData:
    """
    Try *primary* provider; on any exception fall back to *secondary*.

    Both providers must implement the MarketDataProvider protocol:
        get_quote, get_ohlcv, get_atr, get_realised_vol, get_vix, health_check
    """

    def __init__(self, primary: Any, secondary: Any) -> None:
        self._primary = primary
        self._secondary = secondary

    async def get_quote(self, ticker: str) -> dict[str, Any]:
        try:
            return await self._primary.get_quote(ticker)
        except Exception as exc:
            logger.debug("IB get_quote failed for %s (%s) — using yfinance", ticker, exc)
            return await self._secondary.get_quote(ticker)

    async def get_ohlcv(
        self,
        ticker: str,
        period: str = "1d",
        interval: str = "5m",
    ) -> list[dict[str, Any]]:
        try:
            return await self._primary.get_ohlcv(ticker, period=period, interval=interval)
        except Exception as exc:
            logger.debug("IB get_ohlcv failed for %s (%s) — using yfinance", ticker, exc)
            return await self._secondary.get_ohlcv(ticker, period=period, interval=interval)

    async def get_atr(self, ticker: str, period: int = 14) -> float:
        try:
            result = await self._primary.get_atr(ticker, period=period)
            if result > 0:
                return result
        except Exception as exc:
            logger.debug("IB get_atr failed for %s (%s) — using yfinance", ticker, exc)
        return await self._secondary.get_atr(ticker, period=period)

    async def get_realised_vol(self, ticker: str, days: int = 30) -> float:
        try:
            result = await self._primary.get_realised_vol(ticker, days=days)
            if result > 0:
                return result
        except Exception as exc:
            logger.debug("IB get_realised_vol failed for %s (%s) — using yfinance", ticker, exc)
        return await self._secondary.get_realised_vol(ticker, days=days)

    async def get_vix(self) -> float:
        try:
            return await self._primary.get_vix()
        except Exception as exc:
            logger.debug("IB get_vix failed (%s) — using yfinance", exc)
            return await self._secondary.get_vix()

    async def health_check(self) -> bool:
        try:
            return await self._primary.health_check()
        except Exception:
            return await self._secondary.health_check()
