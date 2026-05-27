"""
IBKRScannerDataSource — discovery-only source using the Interactive Brokers
market scanner via ib_async.

The IBKR market scanner is the highest-quality trending signal available:
real-time, exchange-sourced, no rate limits.  It runs two scans each cycle:

  HOT_BY_VOLUME   — stocks with the highest volume spike vs 30-day average
  TOP_PERC_GAIN   — top % gainers today

Both scans target US major-exchange equities (STK.US.MAJOR).  Results are
proposed to the WatchlistManager as watchlist candidates.

This source produces **no SocialPost objects** — poll() always returns [].

Prerequisites:
  1. An IBKR Pro or paper-trading account (free to open at ibkr.com)
  2. TWS or IB Gateway running locally with API access enabled:
       TWS: Edit > Global Configuration > API > Settings > Enable ActiveX and Socket Clients
  3. ``ib_async`` installed (already in project dependencies)

Configuration — read from environment variables (same as execution layer):
  IBKR_HOST               — TWS/Gateway hostname (default: 127.0.0.1)
  IBKR_SCANNER_PORT       — port to connect on (default: 7497)
  IBKR_SCANNER_CLIENT_ID  — must differ from execution layer's IBKR_CLIENT_ID (default: 99)

Design notes:
  - ib_async uses asyncio natively; no thread executor needed.
  - A fresh IB() connection is opened and closed per get_trending() call so
    this source does not hold a persistent connection (avoids conflicts with
    the execution layer's own IB connection).
  - If TWS/Gateway is not running, get_trending() returns [] and the error
    backoff kicks in — the ingest loop continues normally with other sources.
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, AsyncIterator

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SocialPost
from social_trading.ingest.base import BaseDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# Scanner subscription parameters
_INSTRUMENT = "STK"
_LOCATION = "STK.US.MAJOR"
_SCAN_CODES: list[tuple[str, str]] = [
    ("HOT_BY_VOLUME", "ibkr_hot_volume"),   # (scan code, watchlist source label)
    ("TOP_PERC_GAIN", "ibkr_top_gainer"),
]
_CONNECT_TIMEOUT = 10  # seconds to wait for TWS connection
_MAX_ROWS = 50         # IBKR scanner hard cap


class IBKRScannerDataSource(BaseDataSource):
    """
    Discovery-only data source backed by the IBKR market scanner.

    Opens a fresh ib_async connection per call and closes it when done.
    Requires TWS or IB Gateway to be running locally.

    Usage:
        source = IBKRScannerDataSource(redis, cfg, watchlist)
        tickers = await source.get_trending()
    """

    def __init__(
        self,
        redis: "aioredis.Redis",
        cfg: SystemConfig,
        watchlist: WatchlistManager,
        host: str | None = None,
    ) -> None:
        super().__init__(redis, cfg)
        self._watchlist = watchlist
        self._host = host or os.getenv("IBKR_HOST", "127.0.0.1")

    @property
    def name(self) -> str:
        return "ibkr"

    @property
    def is_streaming(self) -> bool:
        return False

    # ── DataSource protocol ───────────────────────────────────────────────────

    async def stream(self) -> AsyncIterator[SocialPost]:
        raise NotImplementedError("IBKRScannerDataSource is a discovery-only source")
        yield  # pragma: no cover

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """Discovery-only source — no social posts to fetch."""
        return []

    async def get_trending(self) -> list[str]:
        """
        Run IBKR market scanner to discover trending tickers.

        Executes HOT_BY_VOLUME and TOP_PERC_GAIN scans, deduplicates,
        proposes all discovered tickers to the WatchlistManager.

        Returns empty list if TWS/Gateway is unreachable or scan fails.
        """
        port = int(os.getenv("IBKR_SCANNER_PORT", "7497"))
        client_id = int(os.getenv("IBKR_SCANNER_CLIENT_ID", "99"))

        try:
            tickers = await self._run_scans(self._host, port, client_id)
        except Exception as exc:
            await self._handle_error(exc)
            return []

        for ticker in tickers:
            await self._watchlist.propose(ticker, source="ibkr_scanner")

        logger.info("ibkr scanner: %d tickers discovered", len(tickers))
        self._reset_errors()
        return tickers

    async def health_check(self) -> bool:
        """Check whether TWS/Gateway is reachable on the configured port."""
        port = int(os.getenv("IBKR_SCANNER_PORT", "7497"))
        client_id = int(os.getenv("IBKR_SCANNER_CLIENT_ID", "99"))
        try:
            from ib_async import IB  # local import — soft dependency
            ib = IB()
            await ib.connectAsync(self._host, port, clientId=client_id,
                                  timeout=_CONNECT_TIMEOUT)
            connected = ib.isConnected()
            ib.disconnect()
            return connected
        except Exception:
            return False

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _run_scans(self, host: str, port: int, client_id: int) -> list[str]:
        """
        Connect to TWS, execute all configured scanner subscriptions, disconnect.

        Returns deduplicated list of ticker symbols.
        """
        from ib_async import IB, ScannerSubscription  # local import — soft dependency

        ib = IB()
        try:
            await ib.connectAsync(host, port, clientId=client_id,
                                  timeout=_CONNECT_TIMEOUT)

            seen: dict[str, None] = {}  # ordered set
            for scan_code, source_label in _SCAN_CODES:
                subscription = ScannerSubscription(
                    instrument=_INSTRUMENT,
                    locationCode=_LOCATION,
                    scanCode=scan_code,
                    numberOfRows=_MAX_ROWS,
                )
                try:
                    scan_results = await ib.reqScannerDataAsync(subscription)
                    for item in scan_results:
                        symbol = item.contractDetails.contract.symbol.upper()
                        if symbol and 1 <= len(symbol) <= 6:
                            seen[symbol] = None
                    logger.debug(
                        "ibkr scan %s: %d results", scan_code, len(scan_results)
                    )
                except Exception as exc:
                    logger.warning("ibkr scan %s failed: %s", scan_code, exc)
        finally:
            if ib.isConnected():
                ib.disconnect()

        return list(seen.keys())
