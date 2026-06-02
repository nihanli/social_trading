"""
Ingest Service — entry point for Phase 1.

Wires up all registered data sources and runs them concurrently:
  - Streaming sources (Reddit): run as persistent async tasks
  - Polling sources (StockTwits, Bluesky): run in timed loops
  - Discovery sources (YFinance, AlphaVantage, IBKR): run in timed loops
  - Watchlist: runs promote_candidates + expire_stale periodically

Run:
    python -m social_trading.services.ingest_service

Environment variables (from .env):
    REDIS_URL             redis://localhost:6379/0
    BLUESKY_HANDLE        Bluesky handle, e.g. "you.bsky.social" (free account)
    BLUESKY_APP_PASSWORD  App password from bsky.app → Settings → App Passwords
    REDDIT_CLIENT_ID      PRAW client ID (optional — Reddit works without auth too)
    REDDIT_CLIENT_SECRET  PRAW client secret
    REDDIT_USER_AGENT     PRAW user agent string
    ALPHA_VANTAGE_API_KEY Alpha Vantage free API key (optional)
    IBKR_HOST             TWS/Gateway host (default: 127.0.0.1)
    IBKR_SCANNER_PORT     TWS/Gateway port for scanner (default: 7497)
    IBKR_SCANNER_CLIENT_ID Client ID for scanner, must differ from execution (default: 99)

    X_BEARER_TOKEN        X API bearer token — ONLY used when x_api_enabled=True
                          in SystemConfig. X API now charges $0.005/request;
                          50 tickers × 288 polls/day ≈ $72/day. Disabled by default.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time

import redis.asyncio as aioredis
from dotenv import load_dotenv

from social_trading.config.system_config import SystemConfig
from social_trading.ingest.base import BaseDataSource
from social_trading.ingest.registry import DataSourceRegistry
from social_trading.ingest.sources.alpha_vantage import AlphaVantageDataSource
from social_trading.ingest.sources.bluesky import BlueskyDataSource
from social_trading.ingest.sources.ibkr_scanner import IBKRScannerDataSource
from social_trading.ingest.sources.reddit import RedditDataSource
from social_trading.ingest.sources.stocktwits import StockTwitsDataSource
from social_trading.ingest.sources.twitter import TwitterDataSource
from social_trading.ingest.sources.yfinance_screener import YFinanceScreenerDataSource
from social_trading.ingest.watchlist.manager import WatchlistManager
from social_trading.core.events import STREAM_ENRICHMENT_REQUESTS
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

# Hard cap on how long an enrichment request may sit in the queue before it is
# considered stale and dropped.  Social-media signals are time-sensitive; there
# is no value processing a Phase-1 candidate that was queued more than 15 minutes
# ago — the market context will have changed.
_ENRICHMENT_MAX_AGE_SEC: int = 900  # 15 minutes

# Maps source name → SystemConfig attribute for poll interval.
# Discovery-only sources (no social posts) share discovery_poll_interval_sec.
_POLL_INTERVAL_ATTR: dict[str, str] = {
    "twitter":       "counts_poll_interval_sec",
    "stocktwits":    "stocktwits_poll_interval_sec",
    "bluesky":       "stocktwits_poll_interval_sec",   # same cadence as stocktwits
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

        # Resolve interval type early so timestamp gates below know which
        # category this source belongs to (discovery vs. sentiment/social).
        interval_attr = _POLL_INTERVAL_ATTR.get(source.name, "discovery_poll_interval_sec")
        interval = getattr(cfg, interval_attr)

        # Discover trending tickers on this source
        await source.get_trending()

        # Stamp the appropriate "last poll" key so the UI countdown reflects
        # the *actual* cadence of the source that owns each category:
        #   discovery_poll_interval_sec sources → discovery:last_poll_ts
        #   stocktwits_poll_interval_sec sources → sentiment:last_poll_ts
        # Keeping the keys separate prevents social-poll loops from resetting
        # the discovery countdown and vice versa.
        if interval_attr == "discovery_poll_interval_sec":
            await redis.set("discovery:last_poll_ts", str(time.time()))
        elif interval_attr == "stocktwits_poll_interval_sec":
            await redis.set("sentiment:last_poll_ts", str(time.time()))

        # Poll active watchlist for social posts (no-op for discovery-only sources)
        tickers = await watchlist.get_active()
        if tickers:
            await source.poll(tickers)

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


async def run_enrichment_loop(
    registry: DataSourceRegistry,
    redis: aioredis.Redis,
) -> None:
    """
    Consume enrichment:requests stream and call Tier-2 sources for each ticker.

    Published by signal_service when a ticker passes Phase-1 evaluation.
    Deduplication at the publisher side (enrichment:sent:{ticker} TTL key)
    ensures at most one request per signal cycle per ticker.

    Dynamically registers/deregisters TwitterDataSource when x_api_enabled
    is toggled via the Config UI — no service restart required.

    Stamps ``ingest:tier2_active`` ("1"/"0") in Redis each cycle so that
    signal_service and the UI use consistent tier-2 availability information
    rather than each computing it independently from config + env vars.
    """
    from social_trading.config.system_config import SystemConfig as _SC

    _group = "ingest"
    _consumer = "ingest-enrichment-0"
    _STALE_PENDING_MIN_IDLE_MS = 5 * 60 * 1000   # reclaim messages pending > 5 min

    try:
        await redis.xgroup_create(STREAM_ENRICHMENT_REQUESTS, _group, id="$", mkstream=True)
    except Exception:
        pass  # group already exists

    async def _reclaim_stale_pending() -> None:
        """Claim and discard pending messages idle longer than _STALE_PENDING_MIN_IDLE_MS."""
        try:
            claimed_id, claimed_msgs, _ = await redis.xautoclaim(
                STREAM_ENRICHMENT_REQUESTS, _group, _consumer,
                min_idle_time=_STALE_PENDING_MIN_IDLE_MS,
                start_id="0-0",
                count=100,
            )
            if claimed_msgs:
                for msg_id, fields in claimed_msgs:
                    ticker_raw = fields.get(b"ticker") or fields.get("ticker", b"")
                    ticker = ticker_raw.decode() if isinstance(ticker_raw, bytes) else str(ticker_raw)
                    logger.info(
                        "ENRICHMENT reclaimed stale pending message ticker=%s "
                        "(idle >5 min) — discarding",
                        ticker,
                    )
                    await redis.xack(STREAM_ENRICHMENT_REQUESTS, _group, msg_id)
        except Exception as exc:
            logger.debug("ENRICHMENT stale-message reclaim skipped: %s", exc)

    # Initial reclaim of stale messages from a previous consumer instance.
    await _reclaim_stale_pending()

    logger.info("Enrichment loop started (tier-2 sources: %s)",
                [s.name for s in registry.tier2_sources()])

    _last_reclaim_ts = time.time()

    while True:
        # ── Dynamic Twitter registration ─────────────────────────────────────
        # Re-read config each iteration so enabling/disabling X API via the UI
        # takes effect without restarting the ingest service.
        try:
            cfg = await _SC.load(redis)
            should_have_twitter = cfg.x_api_enabled and bool(os.getenv("X_BEARER_TOKEN"))
        except Exception:
            should_have_twitter = False

        has_twitter = registry.get("twitter") is not None
        if should_have_twitter and not has_twitter:
            registry.register(TwitterDataSource(redis=redis, cfg=cfg))
            logger.info(
                "ENRICHMENT Twitter source registered (x_api_enabled=True + X_BEARER_TOKEN set)"
            )
        elif not should_have_twitter and has_twitter:
            registry.unregister("twitter")
            logger.info(
                "ENRICHMENT Twitter source deregistered (x_api_enabled=False or token absent)"
            )

        tier2_sources = registry.tier2_sources()

        # Stamp tier2_active so signal_service and UI reflect the same truth.
        await redis.set("ingest:tier2_active", "1" if tier2_sources else "0")

        # Periodically reclaim pending messages that have gone stale since startup
        # (e.g., delivered to this consumer but never acked due to an error).
        now_ts = time.time()
        if now_ts - _last_reclaim_ts >= 300:  # every 5 minutes
            _last_reclaim_ts = now_ts
            await _reclaim_stale_pending()

        if not tier2_sources:
            # Drain any pending enrichment requests so the queue doesn't grow
            # unboundedly when Twitter is disabled after having been active.
            try:
                stale = await redis.xreadgroup(
                    _group, _consumer,
                    {STREAM_ENRICHMENT_REQUESTS: ">"},
                    count=50,
                    block=0,
                )
                if stale:
                    for _stream, entries in stale:
                        for msg_id, fields in entries:
                            ticker_raw = fields.get(b"ticker") or fields.get("ticker", b"")
                            ticker = ticker_raw.decode() if isinstance(ticker_raw, bytes) else str(ticker_raw)
                            logger.debug(
                                "ENRICHMENT DRAINED %s (no tier-2 source active — "
                                "enable X API via Config UI to process)",
                                ticker,
                            )
                            await redis.xack(STREAM_ENRICHMENT_REQUESTS, _group, msg_id)
            except Exception:
                pass
            await asyncio.sleep(30)
            continue

        try:
            messages = await redis.xreadgroup(
                _group, _consumer,
                {STREAM_ENRICHMENT_REQUESTS: ">"},
                count=10,
                block=5000,
            )
        except Exception as exc:
            logger.warning("enrichment loop xreadgroup error: %s", exc)
            await asyncio.sleep(5)
            continue

        if not messages:
            continue

        for _stream, entries in messages:
            for msg_id, fields in entries:
                try:
                    # Fields are bytes when decode_responses=False.
                    ticker = (
                        fields[b"ticker"].decode() if isinstance(fields.get(b"ticker"), bytes)
                        else fields.get("ticker", "")
                    )
                    if not ticker:
                        continue

                    # Skip messages that are too old — they represent a backlog from
                    # previous evaluate cycles.  Signal_service will re-request if the
                    # ticker still qualifies on the next evaluate cycle.  This prevents
                    # an ever-growing queue of stale requests from delaying current ones.
                    msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    msg_ts_ms = int(msg_id_str.split("-")[0])
                    msg_age_sec = (time.time() * 1000 - msg_ts_ms) / 1000
                    stale_threshold = min(2 * cfg.signal_poll_interval_sec, _ENRICHMENT_MAX_AGE_SEC)
                    if msg_age_sec > stale_threshold:
                        logger.debug(
                            "ENRICHMENT SKIP stale request ticker=%s age=%.0fs (threshold=%.0fs) — "
                            "signal_service will re-request if still needed",
                            ticker, msg_age_sec, stale_threshold,
                        )
                        # fall through to finally block to ACK
                        continue

                    phase1_score = float(
                        (fields.get(b"phase1_score") or fields.get("phase1_score", b"0")).decode()
                        if isinstance(fields.get(b"phase1_score") or fields.get("phase1_score"), bytes)
                        else fields.get("phase1_score", 0)
                    )

                    logger.info(
                        "ENRICHMENT ticker=%s phase1_score=%.3f (calling %d tier-2 source(s))",
                        ticker, phase1_score, len(tier2_sources),
                    )

                    for source in tier2_sources:
                        try:
                            posts = await source.poll([ticker])
                            if posts:
                                logger.info(
                                    "ENRICHMENT OK source=%s ticker=%s posts=%d "
                                    "phase1_score=%.3f — Phase-2 data now available",
                                    source.name, ticker, len(posts), phase1_score,
                                )
                                await redis.set("sentiment:last_poll_ts", str(time.time()))
                            else:
                                # Twitter returned no posts — this ticker has no Tier-2
                                # signal confirmation available.  Set the fallback key so
                                # signal_service fires a Phase-1 direct signal rather than
                                # looping indefinitely requesting enrichment that never yields
                                # data.  (Fallback TTL = 5 min to allow next cycle to retry.)
                                logger.info(
                                    "ENRICHMENT EMPTY source=%s ticker=%s — "
                                    "no posts found; signalling Phase-1 fallback",
                                    source.name, ticker,
                                )
                                await redis.set(
                                    f"enrichment:fallback:{ticker}", "1",
                                    ex=300,
                                )
                        except Exception as src_exc:
                            from social_trading.core.exceptions import RateLimitError
                            if isinstance(src_exc, RateLimitError):
                                logger.warning(
                                    "ENRICHMENT RATE_LIMITED source=%s ticker=%s — "
                                    "backing off: %s",
                                    source.name, ticker, src_exc,
                                )
                            else:
                                logger.error(
                                    "ENRICHMENT FAILED source=%s ticker=%s — "
                                    "unexpected error: %s",
                                    source.name, ticker, src_exc, exc_info=True,
                                )
                            # Signal the ticker should fall back to Phase-1 direct signal
                            # on the next evaluation cycle rather than silently staying
                            # stuck waiting for enrichment that will never arrive.
                            # TTL matches the dedup window so the next enrichment
                            # attempt can still be made after it expires.
                            await redis.set(
                                f"enrichment:fallback:{ticker}", "1",
                                ex=300,  # 5-minute fallback window
                            )

                except Exception as exc:
                    logger.warning("enrichment loop processing error: %s", exc)
                finally:
                    await redis.xack(STREAM_ENRICHMENT_REQUESTS, _group, msg_id)


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)

    start_metrics_server(port=int(os.getenv("METRICS_PORT", "8000")))

    cfg = await SystemConfig.load(redis)
    logger.info("SystemConfig loaded (hash=%s)", cfg.config_hash())

    # Clear poll-time stamps so the UI countdown resets on every service restart.
    await redis.delete("discovery:last_poll_ts", "sentiment:last_poll_ts")
    logger.info("Countdown timestamps cleared (service restart)")

    watchlist = WatchlistManager(redis=redis, cfg=cfg)
    await watchlist.seed_from_config()

    registry = DataSourceRegistry()

    # ── Social sources (produce SocialPost objects) ───────────────────────────

    # X / Twitter — DISABLED BY DEFAULT (pay-per-use: ~$0.005/request).
    # Enable via x_api_enabled=True in SystemConfig (Config UI → X API section).
    if os.getenv("X_BEARER_TOKEN") and cfg.x_api_enabled:
        registry.register(TwitterDataSource(redis=redis, cfg=cfg))
        logger.warning(
            "X API enabled — cost ~$%.2f/day for %d tickers at %ds interval",
            len(cfg.seed_tickers) * (86400 / cfg.counts_poll_interval_sec) * 0.005,
            len(cfg.seed_tickers),
            cfg.counts_poll_interval_sec,
        )
    elif os.getenv("X_BEARER_TOKEN") and not cfg.x_api_enabled:
        logger.warning(
            "X_BEARER_TOKEN set but x_api_enabled=False — X API is disabled to prevent "
            "surprise billing ($0.005/request). Enable via Config UI → X API section."
        )

    # Reddit — PRAW streaming (requires REDDIT_CLIENT_ID)
    if os.getenv("REDDIT_CLIENT_ID"):
        registry.register(RedditDataSource(redis=redis, cfg=cfg, watchlist=watchlist))
    else:
        logger.warning("REDDIT_CLIENT_ID not set — Reddit source disabled")

    # StockTwits — no authentication required; public endpoints always available
    registry.register(StockTwitsDataSource(redis=redis, cfg=cfg, watchlist=watchlist))
    logger.info("StockTwits source enabled (unauthenticated public API)")

    # Bluesky — free AT Protocol API; requires bsky.app account + app password
    if os.getenv("BLUESKY_HANDLE") and os.getenv("BLUESKY_APP_PASSWORD"):
        registry.register(BlueskyDataSource(redis=redis, cfg=cfg, watchlist=watchlist))
    else:
        logger.warning(
            "BLUESKY_HANDLE / BLUESKY_APP_PASSWORD not set — Bluesky source disabled. "
            "Create a free account at bsky.app and set an App Password."
        )

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

    # ── Enrichment loop (Phase 2 Tier-2 calls) ────────────────────────────────
    tasks.append(asyncio.create_task(
        run_enrichment_loop(registry, redis),
        name="enrichment:loop",
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

