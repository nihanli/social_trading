"""
Seed watchlist script — populates the Redis active watchlist from config.

Run this once after starting the stack to ensure the ingest service has an
initial set of tickers to monitor, even before any organic trend detection
has occurred.

Usage:
    python -m scripts.seed_watchlist
    python scripts/seed_watchlist.py [--tickers AAPL TSLA NVDA]

Environment:
    REDIS_URL   redis://localhost:6379/0  (default)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

# Allow running from repo root without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import redis.asyncio as aioredis
from dotenv import load_dotenv

from social_trading.config.system_config import SystemConfig
from social_trading.ingest.watchlist.manager import WatchlistManager

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


async def seed(tickers: list[str] | None = None) -> int:
    """
    Seed the watchlist.

    If *tickers* is provided, those are added regardless of config.
    Otherwise the list is taken from ``cfg.seed_tickers``.

    Returns the number of tickers added.
    """
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True)

    try:
        cfg = await SystemConfig.load(r)
        effective_tickers = tickers or list(cfg.seed_tickers)

        if not effective_tickers:
            logger.warning(
                "No tickers to seed — set seed_tickers in SystemConfig or pass --tickers"
            )
            return 0

        watchlist = WatchlistManager(redis=r, cfg=cfg)

        # seed_from_config() only seeds from cfg.seed_tickers; for CLI-supplied
        # tickers we add them directly to the active set.
        if tickers:
            # Use the watchlist key directly (score = current unix timestamp)
            import time
            now = time.time()
            mapping = {t.upper(): now for t in tickers}
            await r.zadd("watchlist:active", mapping)
            added = len(mapping)
        else:
            added = await watchlist.seed_from_config()

        active = await watchlist.get_active()
        logger.info("Watchlist seeded: %d tickers added — active set: %s", added, active)
        return added

    finally:
        await r.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the trading watchlist in Redis")
    parser.add_argument(
        "--tickers",
        nargs="+",
        metavar="TICKER",
        help="Space-separated list of tickers to add (overrides config seed_tickers)",
    )
    args = parser.parse_args()

    added = asyncio.run(seed(args.tickers))
    if added == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
