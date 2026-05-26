"""
Ingest Service — entry point for Phase 1.

Wires up all registered data sources and runs them concurrently:
  - Streaming sources (Reddit): run as persistent async tasks
  - Polling sources (Twitter, StockTwits): run in timed loops
  - Watchlist: runs promote_candidates + expire_stale periodically

Run:
    python -m social_trading.services.ingest_service

Environment variables (from .env):
    REDIS_URL           redis://localhost:6379/0
    X_BEARER_TOKEN      X API v2 bearer token
    REDDIT_CLIENT_ID    PRAW client ID
    REDDIT_CLIENT_SECRET PRAW client secret
    REDDIT_USER_AGENT   PRAW user agent string
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
from social_trading.ingest.registry import DataSourceRegistry
from social_trading.ingest.sources.reddit import RedditDataSource
from social_trading.ingest.sources.stocktwits import StockTwitsDataSource
from social_trading.ingest.sources.twitter import TwitterDataSource
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


async def run_streaming_source(source: RedditDataSource) -> None:
    """Run a streaming data source indefinitely."""
    logger.info("Starting streaming source: %s", source.name)
    async for _post in source.stream():
        pass  # posts are published to stream inside source.stream()


async def run_poll_loop(
    source: TwitterDataSource | StockTwitsDataSource,
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
        # Reload config each cycle
        cfg = await SystemConfig.load(redis)

        # Discover trending tickers on this source
        await source.get_trending()

        # Poll active watchlist
        tickers = await watchlist.get_active()
        if tickers:
            await source.poll(tickers)

        # Determine sleep interval from config
        if source.name == "twitter":
            interval = cfg.counts_poll_interval_sec
        else:
            interval = cfg.stocktwits_poll_interval_sec

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

    # Load initial config
    cfg = await SystemConfig.load(redis)
    logger.info("SystemConfig loaded (hash=%s)", cfg.config_hash())

    # Seed watchlist from config
    watchlist = WatchlistManager(redis=redis, cfg=cfg)
    await watchlist.seed_from_config()

    # Build registry
    registry = DataSourceRegistry()

    # Register Twitter (polled) — only if bearer token is configured
    if os.getenv("X_BEARER_TOKEN"):
        registry.register(TwitterDataSource(redis=redis, cfg=cfg))
    else:
        logger.warning("X_BEARER_TOKEN not set — Twitter source disabled")

    # Register Reddit (streaming) — only if credentials are configured
    if os.getenv("REDDIT_CLIENT_ID"):
        registry.register(
            RedditDataSource(redis=redis, cfg=cfg, watchlist=watchlist)
        )
    else:
        logger.warning("REDDIT_CLIENT_ID not set — Reddit source disabled")

    # Register StockTwits (polled) — only if token is configured
    if os.getenv("STOCKTWITS_TOKEN"):
        registry.register(StockTwitsDataSource(redis=redis, cfg=cfg, watchlist=watchlist))
    else:
        logger.warning("STOCKTWITS_TOKEN not set — StockTwits source disabled")

    logger.info("Registered sources: %s", registry.names)

    if not registry.names:
        logger.error(
            "No data sources registered — set at least X_BEARER_TOKEN, "
            "REDDIT_CLIENT_ID, or STOCKTWITS_TOKEN in .env"
        )

    # Build task list
    tasks: list[asyncio.Task] = []

    for source in registry.streaming_sources():
        tasks.append(asyncio.create_task(
            run_streaming_source(source),  # type: ignore[arg-type]
            name=f"stream:{source.name}",
        ))

    for source in registry.polling_sources():
        tasks.append(asyncio.create_task(
            run_poll_loop(source, watchlist, cfg, redis),  # type: ignore[arg-type]
            name=f"poll:{source.name}",
        ))

    tasks.append(asyncio.create_task(
        run_watchlist_maintenance(watchlist, cfg, redis),
        name="watchlist:maintenance",
    ))

    logger.info("Ingest service started with %d tasks", len(tasks))

    # Graceful shutdown on SIGTERM / SIGINT
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
