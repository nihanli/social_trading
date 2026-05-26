"""
Signal Service — consumes sentiment_signals and publishes strategy_signals.

Two concurrent asyncio tasks:

  ingest_task:   Reads from sentiment_signals stream (consumer group "signal"),
                 feeds each SentimentResult into SentimentAggregator,
                 acks processed messages.

  evaluate_task: Every cfg.signal_poll_interval_sec (default 60s):
                   - Reload config
                   - Get active watchlist
                   - For each ticker: compute stats + volume Z-score
                   - Call SignalGenerator.evaluate()
                   - Publish Signal → strategy_signals stream

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

import redis.asyncio as aioredis
from dotenv import load_dotenv

from social_trading.config.system_config import SystemConfig
from social_trading.core.events import STREAM_SENTIMENT, STREAM_STRATEGY_SIGNALS
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
logger = logging.getLogger(__name__)

_GROUP = "signal"
_CONSUMER = "signal-0"
_INGEST_BATCH = 32


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
        "source_post_count": str(sig.source_post_count),
        "generated_at": sig.generated_at.isoformat(),
    }


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
    Periodically iterate active watchlist, compute stats, generate signals.
    Runs until cancelled.
    """
    logger.info("Signal evaluate task started")
    signals_generated = 0

    while True:
        cfg = await SystemConfig.load(redis)
        aggregator.update_cfg(cfg)

        tickers = await watchlist.get_active()
        if not tickers:
            await asyncio.sleep(cfg.signal_poll_interval_sec)
            continue

        batch_signals: list[Signal] = []

        for ticker in tickers:
            try:
                stats = await aggregator.get_stats(ticker)
                if stats is None:
                    continue

                volume_zscore = await aggregator.get_volume_zscore(ticker)
                sig = generator.evaluate(
                    stats,
                    cfg=cfg,
                    volume_zscore=volume_zscore,
                    # price_momentum: 0.0 until market_data service is available (Phase 5)
                )
                if sig is not None:
                    batch_signals.append(sig)
                    SIGNALS_GENERATED.labels(ticker=ticker, direction=sig.direction).inc()
                    SIGNAL_QUALITY.observe(sig.quality_score)
                    SENTIMENT_SCORE.labels(ticker=ticker).set(sig.sentiment_score)
                    VOLUME_ZSCORE.labels(ticker=ticker).set(sig.volume_z_score)

            except Exception as exc:
                logger.warning("Error evaluating %s: %s", ticker, exc)

        if batch_signals:
            for sig in batch_signals:
                await redis.xadd(STREAM_STRATEGY_SIGNALS, _signal_to_stream_dict(sig))
            signals_generated += len(batch_signals)
            logger.info(
                "signals: generated=%d this_cycle=%d tickers_scanned=%d",
                signals_generated, len(batch_signals), len(tickers),
            )
        else:
            logger.debug("evaluate cycle: no signals (tickers=%d)", len(tickers))

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
