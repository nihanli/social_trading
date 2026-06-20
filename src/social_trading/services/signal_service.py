"""
Signal Service — consumes sentiment_signals and publishes strategy_signals.

Two concurrent asyncio tasks:

  ingest_task:   Reads from sentiment_signals stream (consumer group "signal"),
                 feeds each SentimentResult into SentimentAggregator,
                 acks processed messages.

  evaluate_task: Every cfg.signal_poll_interval_sec (default 60s):
                   - Reload config
                   - Get active watchlist
                   - Phase 1: evaluate with Tier-1 (free) stats against
                              signal_phase1_threshold; publish enrichment
                              requests for qualifying tickers.
                   - Phase 2: for tickers that already have Tier-2 data in
                              the aggregator window, re-evaluate against
                              signal_phase2_threshold; publish to
                              strategy_signals stream.
                   - Tickers with no Tier-2 data evaluated against Phase 1 threshold;
                     when cfg.x_api_enabled is False, Phase 1 signals fire directly
                     to strategy_signals (no paid sources means Phase 1 IS the final signal).

Design reference: docs/design/05-signal-generation.md §5a

Run:
    python -m social_trading.services.signal_service
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal as os_signal
import sys
from datetime import datetime, timezone

import redis.asyncio as aioredis
from dotenv import load_dotenv

from social_trading.config.system_config import SystemConfig
from social_trading.core.events import (
    STREAM_ENRICHMENT_REQUESTS,
    STREAM_MAXLEN,
    STREAM_SENTIMENT,
    STREAM_STRATEGY_SIGNALS,
)
from social_trading.core.models import SentimentResult, Signal
from social_trading.ingest.watchlist.manager import WatchlistManager
from social_trading.monitoring.metrics import (
    SENTIMENT_SCORE,
    SIGNAL_QUALITY,
    SIGNALS_GENERATED,
    VOLUME_ZSCORE,
    start_metrics_server,
)
from social_trading.signals.aggregator import SentimentAggregator
from social_trading.signals.generator import SignalGenerator
from social_trading.storage.event_bus import TradingEventBus

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
_redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
from social_trading.monitoring.log_handler import RedisLogHandler  # noqa: E402
logging.getLogger().addHandler(RedisLogHandler("signal", _redis_url))
logger = logging.getLogger(__name__)

_GROUP = "signal"
_CONSUMER = "signal-0"
_INGEST_BATCH = 32

# Canonical set of Tier-2 source names (paid APIs).  Kept in sync with the
# `tier` property overrides in each DataSource implementation.
_TIER2_SOURCE_NAMES: frozenset[str] = frozenset({"twitter"})

# Redis key template for deduplicating enrichment requests within a cycle.
# TTL = signal_poll_interval_sec so at most one request is sent per cycle.
_ENRICHMENT_SENT_KEY = "enrichment:sent:{ticker}"

# Redis key template for deduplicating strategy signals within a cycle.
# Prevents re-emitting the same signal every poll cycle when the ticker remains
# above threshold.  TTL = signal_poll_interval_sec (same cadence as enrichment).
_SIGNAL_DEDUP_KEY = "signal:dedup:{ticker}"

# Fallback reactive threshold (fraction) used when ATR data is unavailable.
# When ATR is available, the threshold is 1.5 × (atr_14 / last_price) so
# high-volatility tickers require a proportionally larger move to be flagged
# as reactive.  3% covers typical mid-cap 1.5–2σ daily moves.
_REACTIVE_THRESHOLD = 0.03  # 3% daily move


# ── Deserialisation ───────────────────────────────────────────────────────────

def _stream_dict_to_result(fields: dict) -> SentimentResult | None:
    """Reconstruct SentimentResult from flat Redis Stream dict."""
    try:
        return SentimentResult(
            post_id=fields["post_id"],
            ticker=fields["ticker"],
            positive=float(fields["positive"]),
            negative=float(fields["negative"]),
            neutral=float(fields["neutral"]),
            score=float(fields["score"]),
            model=fields["model"],
            latency_ms=float(fields.get("latency_ms", 0)),
            source=fields.get("source", ""),
            likes=int(fields.get("likes", 0)),
            reposts=int(fields.get("reposts", 0)),
            author_followers=int(fields.get("author_followers", 0)),
        )
    except Exception as exc:
        logger.warning("malformed sentiment_signals message: %s", exc)
        return None


def _signal_to_stream_dict(sig: Signal) -> dict[str, str]:
    """Serialise Signal to flat str dict for Redis Streams."""
    return {
        "ticker": sig.ticker,
        "direction": sig.direction,
        "quality_score": str(sig.quality_score),
        "sentiment_score": str(sig.sentiment_score),
        "volume_z_score": str(sig.volume_z_score),
        "momentum": str(sig.momentum),
        "convergence": str(sig.convergence),
        "proactivity": str(sig.proactivity),
        "source_post_count": str(sig.source_post_count),
        "generated_at": sig.generated_at.isoformat(),
        "signal_phase": sig.signal_phase or "",
        "atr": str(sig.atr) if sig.atr is not None else "",
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _stats_has_tier2_data(sources: set[str] | frozenset[str]) -> bool:
    """Return True if the sentiment stats window includes any Tier-2 source."""
    return bool(sources & _TIER2_SOURCE_NAMES)


async def _is_open_position(redis: aioredis.Redis, ticker: str) -> bool:
    """Check whether there is an open position for *ticker* in Redis."""
    return bool(await redis.hexists("positions:live", ticker))


async def _request_enrichment(
    redis: aioredis.Redis,
    ticker: str,
    phase1_score: float,
    poll_interval_sec: int,
) -> None:
    """
    Publish an enrichment request for *ticker* to the enrichment:requests stream.

    Deduplication: a key with TTL = poll_interval_sec ensures at most one
    request is published per signal evaluation cycle.
    """
    dedup_key = _ENRICHMENT_SENT_KEY.format(ticker=ticker)
    already_sent = await redis.set(dedup_key, "1", ex=poll_interval_sec, nx=True)
    if not already_sent:
        logger.debug("enrichment already requested this cycle for %s", ticker)
        return

    event = {
        "ticker": ticker,
        "phase1_score": str(phase1_score),
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    await redis.xadd(
        STREAM_ENRICHMENT_REQUESTS,
        event,
        maxlen=STREAM_MAXLEN.get(STREAM_ENRICHMENT_REQUESTS),
        approximate=True,
    )
    logger.info("PHASE1 CANDIDATE %s score=%.3f → enrichment requested", ticker, phase1_score)


# ── Service tasks ─────────────────────────────────────────────────────────────

async def run_ingest_task(
    bus: TradingEventBus,
    aggregator: SentimentAggregator,
) -> None:
    """
    Continuously read from sentiment_signals and feed into aggregator.
    Runs until cancelled.
    """
    await bus.create_group(STREAM_SENTIMENT, _GROUP)
    logger.info("Signal ingest listening on %s (group=%s)", STREAM_SENTIMENT, _GROUP)

    while True:
        messages = await bus.consume(
            STREAM_SENTIMENT, _GROUP, _CONSUMER, count=_INGEST_BATCH
        )
        for msg_id, fields in messages:
            result = _stream_dict_to_result(fields)
            if result is not None:
                await aggregator.add(result)
            await bus.ack(STREAM_SENTIMENT, _GROUP, msg_id)


async def run_evaluate_task(
    aggregator: SentimentAggregator,
    generator: SignalGenerator,
    watchlist: WatchlistManager,
    redis: aioredis.Redis,
) -> None:
    """
    Periodically iterate active watchlist, compute stats, run two-phase
    signal evaluation, publish signals and enrichment requests.

    Two-phase logic:
      • Phase 1  — free/Tier-1 sources only (no Tier-2 data in window yet).
                   Threshold: cfg.signal_phase1_threshold.
                   score >= phase1_threshold → Phase-1 signal.
                   - If tier2_configured (x_api_enabled=True AND X_BEARER_TOKEN set):
                     request Tier-2 enrichment (signal fires after Phase 2).
                   - Otherwise: fire signal directly to strategy_signals
                     (Phase 1 IS the final signal when no paid sources active).
      • Phase 2  — Tier-2 data is present in the aggregator window (enrichment fulfilled).
                   Threshold: cfg.signal_phase2_threshold.
                   score >= phase2_threshold → fire signal to strategy_signals.
                   score < phase2_threshold → suppressed (enrichment confirmed weak signal).
    """
    logger.info("Signal evaluate task started (two-phase pipeline)")
    signals_generated = 0
    _tier2_warn_logged = False  # warn once if x_api_enabled but token missing

    while True:
        cfg = await SystemConfig.load(redis)
        aggregator.update_cfg(cfg)

        # Warn once if config says Tier-2 is enabled but the token is absent.
        if cfg.x_api_enabled and not os.getenv("X_BEARER_TOKEN") and not _tier2_warn_logged:
            logger.warning(
                "x_api_enabled=True but X_BEARER_TOKEN is not set — "
                "Tier-2 enrichment will NOT run; Phase 1 signals fire directly. "
                "Set X_BEARER_TOKEN in .env to enable paid X/Twitter enrichment."
            )
            _tier2_warn_logged = True

        tickers = await watchlist.get_active()
        if not tickers:
            await asyncio.sleep(cfg.signal_poll_interval_sec)
            continue

        batch_signals: list[Signal] = []
        phase1_direct = 0      # Phase-1 signals fired directly (no tier-2 configured)
        phase1_enrichment = 0  # Phase-1 candidates that requested enrichment
        phase2_evaluated = 0

        # Whether paid Tier-2 API (X/Twitter) is *actually* active in the ingest
        # service.  The enrichment loop stamps ingest:tier2_active ("1"/"0") each
        # cycle after dynamically registering/unregistering Twitter based on the
        # current config.  Reading this key avoids the race where the config UI
        # enables x_api_enabled but the ingest service hasn't registered Twitter yet
        # (or was started before the token was configured), which would cause
        # enrichment requests to accumulate in the stream without ever being processed.
        tier2_active_raw = await redis.get("ingest:tier2_active")
        tier2_configured = (tier2_active_raw == "1" or tier2_active_raw == b"1")

        # Apply phase2_max_tickers_per_cycle cap across the whole batch.
        enrichment_budget = cfg.phase2_max_tickers_per_cycle

        for ticker in tickers:
            try:
                # Skip this ticker entirely if a position is already open and
                # the config requests it — applies to ALL phases (Phase-1 direct,
                # Phase-2, enrichment requests) so no double-up can occur.
                if cfg.phase2_skip_open_positions and await _is_open_position(redis, ticker):
                    logger.debug("SKIP %s — open position (phase2_skip_open_positions=True)", ticker)
                    continue

                stats = await aggregator.get_stats(ticker)
                if stats is None:
                    continue

                volume_zscore = await aggregator.get_volume_zscore(ticker)
                has_tier2 = _stats_has_tier2_data(stats.sources)

                # ── Price momentum from execution-service market snapshot ────
                # execution_service writes market_data:{ticker} every ~5 min for
                # all watchlist tickers (IB primary → yfinance fallback).  The
                # "momentum" field holds the intraday return (open → current).
                # Missing = 0.0 (neutral — does not penalise quality score).
                price_momentum = 0.0
                mkt_raw = await redis.hgetall(f"market_data:{ticker}")
                if mkt_raw:
                    raw_mom = mkt_raw.get(b"momentum") or mkt_raw.get("momentum")
                    if raw_mom:
                        try:
                            price_momentum = float(raw_mom)
                        except (ValueError, TypeError):
                            pass

                # is_reactive threshold: ATR-relative so high-vol tickers need a
                # larger move to be considered "reactive" (crowd chasing an already-
                # extended move).  Falls back to _REACTIVE_THRESHOLD when ATR data
                # is absent (e.g. execution_service snapshot not yet written).
                reactive_threshold = _REACTIVE_THRESHOLD
                ticker_atr: float | None = None
                if mkt_raw:
                    try:
                        atr_14 = float(mkt_raw.get(b"atr_14") or mkt_raw.get("atr_14") or 0.0)
                        last_px = float(mkt_raw.get(b"last") or mkt_raw.get("last") or 0.0)
                        if atr_14 > 0:
                            ticker_atr = atr_14
                        if atr_14 > 0 and last_px > 0:
                            reactive_threshold = 1.5 * (atr_14 / last_px)
                    except (ValueError, TypeError):
                        pass

                # is_reactive: crowd is reacting to an existing price move rather
                # than front-running one — suppress proactivity credit (p=0).
                is_reactive = (
                    price_momentum != 0.0
                    and (
                        (stats.direction == "LONG" and price_momentum > reactive_threshold)
                        or (stats.direction == "SHORT" and price_momentum < -reactive_threshold)
                    )
                )

                if has_tier2:
                    # ── Phase 2 ─────────────────────────────────────────────
                    phase2_evaluated += 1
                    sig = generator.evaluate(
                        stats, cfg=cfg,
                        quality_threshold=cfg.signal_phase2_threshold,
                        volume_zscore=volume_zscore,
                        price_momentum=price_momentum,
                        is_reactive=is_reactive,
                    )
                    if sig is not None:
                        sig = sig.model_copy(update={"signal_phase": "phase2", "atr": ticker_atr})
                        batch_signals.append(sig)
                        SIGNALS_GENERATED.labels(ticker=ticker, direction=sig.direction).inc()
                        SIGNAL_QUALITY.observe(sig.quality_score)
                        SENTIMENT_SCORE.labels(ticker=ticker).set(sig.sentiment_score)
                        VOLUME_ZSCORE.labels(ticker=ticker).set(sig.volume_z_score)
                        logger.info(
                            "PHASE2 SIGNAL %s dir=%s score=%.3f",
                            ticker, sig.direction, sig.quality_score,
                        )
                    else:
                        # Phase-2 threshold not met — suppress signal per design §5a.
                        # Tier-2 enrichment confirmed the signal is weak; do NOT
                        # fall back to Phase-1 as that would promote a suppressed ticker.
                        logger.debug(
                            "PHASE2 SUPPRESSED %s (score below phase2_threshold=%.2f)",
                            ticker, cfg.signal_phase2_threshold,
                        )

                else:
                    # ── Phase 1 (free sources only) ─────────────────────────
                    sig = generator.evaluate(
                        stats, cfg=cfg,
                        quality_threshold=cfg.signal_phase1_threshold,
                        volume_zscore=volume_zscore,
                        price_momentum=price_momentum,
                        is_reactive=is_reactive,
                    )
                    if sig is not None:
                        SENTIMENT_SCORE.labels(ticker=ticker).set(sig.sentiment_score)
                        VOLUME_ZSCORE.labels(ticker=ticker).set(sig.volume_z_score)

                        if not tier2_configured:
                            # No paid sources — Phase 1 IS the final signal.
                            sig = sig.model_copy(update={"signal_phase": "phase1", "atr": ticker_atr})
                            batch_signals.append(sig)
                            phase1_direct += 1
                            SIGNALS_GENERATED.labels(ticker=ticker, direction=sig.direction).inc()
                            SIGNAL_QUALITY.observe(sig.quality_score)
                            logger.info(
                                "PHASE1 SIGNAL (direct) %s dir=%s score=%.3f",
                                ticker, sig.direction, sig.quality_score,
                            )
                        else:
                            # Tier-2 configured.  Check if the last enrichment attempt
                            # failed — if so, fire Phase-1 directly rather than letting
                            # the ticker stall indefinitely waiting for enrichment that
                            # keeps erroring.
                            fallback_key = f"enrichment:fallback:{ticker}"
                            enrichment_failed = await redis.exists(fallback_key)
                            if enrichment_failed:
                                await redis.delete(fallback_key)
                                sig = sig.model_copy(update={"signal_phase": "phase1", "atr": ticker_atr})
                                batch_signals.append(sig)
                                phase1_direct += 1
                                SIGNALS_GENERATED.labels(ticker=ticker, direction=sig.direction).inc()
                                SIGNAL_QUALITY.observe(sig.quality_score)
                                logger.warning(
                                    "PHASE1 SIGNAL (enrichment fallback) %s dir=%s score=%.3f "
                                    "— Tier-2 source error; firing Phase-1 directly",
                                    ticker, sig.direction, sig.quality_score,
                                )
                            else:
                                # Request enrichment; signal fires after Phase 2.
                                phase1_enrichment += 1
                                if enrichment_budget > 0:
                                    await _request_enrichment(
                                        redis, ticker, sig.quality_score,
                                        cfg.signal_poll_interval_sec,
                                    )
                                    enrichment_budget -= 1

            except Exception as exc:
                logger.warning("Error evaluating %s: %s", ticker, exc)

        if batch_signals:
            for sig in batch_signals:
                # Dedup: skip if an identical signal was already emitted this cycle.
                dedup_key = _SIGNAL_DEDUP_KEY.format(ticker=sig.ticker)
                is_new = await redis.set(dedup_key, "1", ex=cfg.signal_poll_interval_sec, nx=True)
                if not is_new:
                    logger.debug("SIGNAL_DEDUP %s — already emitted this cycle, skipping", sig.ticker)
                    continue
                await redis.xadd(
                    STREAM_STRATEGY_SIGNALS,
                    _signal_to_stream_dict(sig),
                    maxlen=STREAM_MAXLEN.get(STREAM_STRATEGY_SIGNALS),
                    approximate=True,
                )
                # Queue ticker for OHLC pre-fetch (price history task picks this up
                # at 4:30 PM ET to store complete entry-day intraday bars).
                await redis.sadd("price_fetch:queue", sig.ticker)
            signals_generated += len(batch_signals)

        logger.info(
            "evaluate cycle: signals=%d phase1_direct=%d phase1_enrichment=%d "
            "phase2_evaluated=%d tickers_scanned=%d tier2=%s",
            len(batch_signals), phase1_direct, phase1_enrichment,
            phase2_evaluated, len(tickers), tier2_configured,
        )

        await asyncio.sleep(cfg.signal_poll_interval_sec)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)

    start_metrics_server(port=int(os.getenv("METRICS_PORT", "8000")))

    cfg = await SystemConfig.load(redis)
    logger.info("SystemConfig loaded (hash=%s)", cfg.config_hash())

    aggregator = SentimentAggregator(redis=redis, cfg=cfg)
    generator = SignalGenerator()
    watchlist = WatchlistManager(redis=redis, cfg=cfg)
    bus = TradingEventBus(redis)

    tasks = [
        asyncio.create_task(run_ingest_task(bus, aggregator), name="signal:ingest"),
        asyncio.create_task(
            run_evaluate_task(aggregator, generator, watchlist, redis),
            name="signal:evaluate",
        ),
    ]

    def _shutdown(sig: int, _frame: object) -> None:
        logger.info("Received signal %d — shutting down signal service", sig)
        for task in tasks:
            task.cancel()

    os_signal.signal(os_signal.SIGTERM, _shutdown)
    os_signal.signal(os_signal.SIGINT, _shutdown)

    logger.info("Signal service started")
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Signal service stopped")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
