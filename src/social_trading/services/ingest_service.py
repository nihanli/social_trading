"""
Ingest Service — entry point for Phase 1.

Wires up all registered data sources and runs them concurrently:
  - Streaming sources (Reddit): run as persistent async tasks
  - Polling sources (Twitter, StockTwits): run in timed loops
  - Discovery sources (YFinance, AlphaVantage, IBKR): run in timed loops
  - Watchlist: runs promote_candidates + expire_stale periodically

Run:
    python -m social_trading.services.ingest_service

Environment variables (from .env):
    REDIS_URL             redis://localhost:6379/0
    X_BEARER_TOKEN        X API v2 bearer token
    REDDIT_CLIENT_ID      PRAW client ID
    REDDIT_CLIENT_SECRET  PRAW client secret
    REDDIT_USER_AGENT     PRAW user agent string
    STOCKTWITS_TOKEN      StockTwits OAuth token (optional — API closed to new accounts)
    ALPHA_VANTAGE_API_KEY Alpha Vantage free API key (optional)
    IBKR_HOST             TWS/Gateway host (default: 127.0.0.1)
    IBKR_SCANNER_PORT     TWS/Gateway port for scanner (default: 7497)
    IBKR_SCANNER_CLIENT_ID Client ID for scanner, must differ from execution (default: 99)
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

import redis.asyncio as aioredis
from dotenv import load_dotenv

from social_trading.config.system_config import SystemConfig
from social_trading.ingest.base import BaseDataSource
from social_trading.ingest.registry import DataSourceRegistry
from social_trading.ingest.sources.alpha_vantage import AlphaVantageDataSource
from social_trading.ingest.sources.ibkr_scanner import IBKRScannerDataSource
from social_trading.ingest.sources.reddit import RedditDataSource
from social_trading.ingest.sources.stocktwits import StockTwitsDataSource
from social_trading.ingest.sources.twitter import TwitterDataSource
from social_trading.ingest.sources.yfinance_screener import YFinanceScreenerDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager
from social_trading.monitoring.metrics import (
    ACTIVE_TICKERS,
    start_metrics_server,
)

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Maps source name → SystemConfig attribute for poll interval.
# Discovery-only sources (no social posts) share discovery_poll_interval_sec.
_POLL_INTERVAL_ATTR: dict[str, str] = {
    "twitter":       "counts_poll_interval_sec",
    "stocktwits":    "stocktwits_poll_interval_sec",
    "yfinance":      "discovery_poll_interval_sec",
    "alpha_vantage": "discovery_poll_interval_sec",
    "ibkr":          "discovery_poll_interval_sec",
}


async def run_streaming_source(source: BaseDataSource) -> None:
    """Run a streaming data source indefinitely."""
    logger.info("Starting streaming source: %s", source.name)
    async for _post in source.stream():
        pass  # posts are published to stream inside source.stream()


async def run_poll_loop(
    source: BaseDataSource,
    watchlist: WatchlistManager,
    cfg: SystemConfig,
    redis: aioredis.Redis,
) -> None:
    """
    Run a polling source in a timed loop.
    Reload config each cycle so UI changes take effect.
    """
    logger.info("Starting poll loop: %s", source.name)
    while True:
        cfg = await SystemConfig.load(redis)

        # Discover trending tickers on this source
        await source.get_trending()

        # Poll active watchlist for social posts (no-op for discovery-only sources)
        tickers = await watchlist.get_active()
        if tickers:
            await source.poll(tickers)

        interval_attr = _POLL_INTERVAL_ATTR.get(source.name, "discovery_poll_interval_sec")
        interval = getattr(cfg, interval_attr)
        await asyncio.sleep(interval)


async def run_watchlist_maintenance(
    watchlist: WatchlistManager,
    cfg: SystemConfig,
    redis: aioredis.Redis,
) -> None:
    """
    Periodically run watchlist housekeeping:
      - promote candidates that pass liquidity gate
      - expire stale tickers
    """
    logger.info("Starting watchlist maintenance loop")
    while True:
        cfg = await SystemConfig.load(redis)
        promoted = await watchlist.promote_candidates()
        expired = await watchlist.expire_stale()
        active_count = await watchlist.size()
        ACTIVE_TICKERS.set(active_count)
        if promoted or expired:
            logger.info(
                "watchlist: +%d promoted, -%d expired, %d active",
                promoted, expired, active_count,
            )
        await asyncio.sleep(cfg.watchlist_promote_interval)


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)

    start_metrics_server(port=int(os.getenv("METRICS_PORT", "8000")))

    cfg = await SystemConfig.load(redis)
    logger.info("SystemConfig loaded (hash=%s)", cfg.config_hash())

    watchlist = WatchlistManager(redis=redis, cfg=cfg)
    await watchlist.seed_from_config()

    registry = DataSourceRegistry()

    # ── Social sources (produce SocialPost objects) ───────────────────────────

    if os.getenv("X_BEARER_TOKEN"):
        registry.register(TwitterDataSource(redis=redis, cfg=cfg))
    else:
        logger.warning("X_BEARER_TOKEN not set — Twitter source disabled")

    if os.getenv("REDDIT_CLIENT_ID"):
        registry.register(RedditDataSource(redis=redis, cfg=cfg, watchlist=watchlist))
    else:
        logger.warning("REDDIT_CLIENT_ID not set — Reddit source disabled")

    if os.getenv("STOCKTWITS_TOKEN"):
        registry.register(StockTwitsDataSource(redis=redis, cfg=cfg, watchlist=watchlist))
    else:
        logger.warning("STOCKTWITS_TOKEN not set — StockTwits source disabled")

    # ── Discovery sources (trending ticker candidates only) ───────────────────

    # Yahoo Finance screener — always enabled, no key required
    registry.register(YFinanceScreenerDataSource(redis=redis, cfg=cfg, watchlist=watchlist))

    if os.getenv("ALPHA_VANTAGE_API_KEY"):
        registry.register(AlphaVantageDataSource(redis=redis, cfg=cfg, watchlist=watchlist))
    else:
        logger.warning("ALPHA_VANTAGE_API_KEY not set — Alpha Vantage source disabled")

    # IBKR scanner — enabled if TWS/Gateway is reachable (checked at first call)
    registry.register(IBKRScannerDataSource(redis=redis, cfg=cfg, watchlist=watchlist))

    logger.info("Registered sources: %s", registry.names)

    # ── Build task list ───────────────────────────────────────────────────────

    tasks: list[asyncio.Task] = []

    for source in registry.streaming_sources():
        tasks.append(asyncio.create_task(
            run_streaming_source(source),
            name=f"stream:{source.name}",
        ))

    for source in registry.polling_sources():
        tasks.append(asyncio.create_task(
            run_poll_loop(source, watchlist, cfg, redis),
            name=f"poll:{source.name}",
        ))

    tasks.append(asyncio.create_task(
        run_watchlist_maintenance(watchlist, cfg, redis),
        name="watchlist:maintenance",
    ))

    logger.info("Ingest service started with %d tasks", len(tasks))

    def _shutdown(sig: int, _frame: object) -> None:
        logger.info("Received signal %d — shutting down", sig)
        for task in tasks:
            task.cancel()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Ingest service stopped")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
