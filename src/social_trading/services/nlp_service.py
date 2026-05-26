"""
NLP Service — consumes raw_social stream and publishes sentiment_signals.

Consumer group: "nlp"
Consumer name:  "nlp-0" (single consumer; scale by adding nlp-1, nlp-2, …)

Processing loop:
  1. xreadgroup from raw_social  (blocks 2s waiting for messages)
  2. Deserialise flat dict → SocialPost
  3. Run NLPPipeline.process_batch()
  4. Publish each SentimentResult → sentiment_signals stream
  5. xack processed message IDs

Config is reloaded every `cfg.signal_poll_interval_sec` seconds so
UI changes to vader_neutral_threshold, finbert_batch_size, bot thresholds
take effect without restarting the service.

Run:
    python -m social_trading.services.nlp_service
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
from social_trading.core.events import STREAM_RAW_SOCIAL, STREAM_SENTIMENT
from social_trading.core.models import SentimentResult, SocialPost
from social_trading.nlp.classifiers.finbert import FinBERTClassifier
from social_trading.nlp.classifiers.vader import VaderClassifier
from social_trading.nlp.filters.bot_filter import BotFilter
from social_trading.nlp.filters.ticker_extractor import TickerExtractor
from social_trading.nlp.pipeline import NLPPipeline
from social_trading.storage.event_bus import TradingEventBus

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_GROUP = "nlp"
_CONSUMER = "nlp-0"
_BATCH_SIZE = 32  # messages read per xreadgroup call


# ── Serialisation helpers ─────────────────────────────────────────────────────

def _stream_dict_to_post(fields: dict) -> SocialPost | None:
    """
    Reconstruct a SocialPost from the flat str-keyed dict in a Redis Stream.
    Returns None if the message is malformed.
    """
    try:
        raw: dict = {}
        if label := fields.get("sentiment_label", ""):
            raw["sentiment_label"] = label

        return SocialPost(
            id=fields["id"],
            source=fields["source"],              # type: ignore[arg-type]
            ticker=fields["ticker"],
            text=fields["text"],
            author_id=fields["author_id"],
            author_followers=int(fields.get("author_followers", 0)),
            author_account_age_days=int(fields.get("author_account_age_days", 0)),
            author_following=int(fields.get("author_following", 0)),
            post_count_30d=int(fields.get("post_count_30d", 0)),
            likes=int(fields.get("likes", 0)),
            reposts=int(fields.get("reposts", 0)),
            is_original=fields.get("is_original", "1") == "1",
            url=fields.get("url", ""),
            raw=raw,
        )
    except Exception as exc:
        logger.warning("malformed stream message: %s — %s", fields.get("id", "?"), exc)
        return None


def _result_to_stream_dict(result: SentimentResult) -> dict[str, str]:
    """Serialise SentimentResult to flat str dict for Redis Streams."""
    return {
        "post_id": result.post_id,
        "ticker": result.ticker,
        "positive": str(round(result.positive, 6)),
        "negative": str(round(result.negative, 6)),
        "neutral": str(round(result.neutral, 6)),
        "score": str(round(result.score, 6)),
        "model": result.model,
        "latency_ms": str(round(result.latency_ms, 3)),
        "classified_at": result.classified_at.isoformat(),
    }


# ── Service main loop ─────────────────────────────────────────────────────────

async def run_nlp_service(
    bus: TradingEventBus,
    pipeline: NLPPipeline,
    redis: aioredis.Redis,
) -> None:
    """
    Main consume-process-publish loop.
    Runs indefinitely until cancelled (SIGTERM/SIGINT).
    """
    await bus.create_group(STREAM_RAW_SOCIAL, _GROUP)
    logger.info("NLP service listening on %s (group=%s)", STREAM_RAW_SOCIAL, _GROUP)

    processed = published = 0

    while True:
        # Reload config so UI changes take effect each cycle
        cfg = await SystemConfig.load(redis)
        pipeline.update_cfg(cfg)

        messages = await bus.consume(
            STREAM_RAW_SOCIAL, _GROUP, _CONSUMER, count=_BATCH_SIZE
        )
        if not messages:
            continue

        posts: list[SocialPost] = []
        msg_ids: list[str] = []
        for msg_id, fields in messages:
            post = _stream_dict_to_post(fields)
            if post is not None:
                posts.append(post)
                msg_ids.append(msg_id)

        if posts:
            results = await pipeline.process_batch(posts)
            for result in results:
                await redis.xadd(STREAM_SENTIMENT, _result_to_stream_dict(result))

            published += len(results)
            processed += len(posts)
            logger.debug(
                "batch: %d posts → %d results (total processed=%d published=%d)",
                len(posts), len(results), processed, published,
            )

        # Ack all messages (even filtered ones — they won't be retried)
        for msg_id in msg_ids:
            await bus.ack(STREAM_RAW_SOCIAL, _GROUP, msg_id)

        if processed % 500 == 0 and processed > 0:
            logger.info(
                "NLP checkpoint: processed=%d published=%d drop_rate=%.1f%%",
                processed, published,
                100 * (1 - published / processed) if processed else 0,
            )


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)

    cfg = await SystemConfig.load(redis)
    logger.info("SystemConfig loaded (hash=%s)", cfg.config_hash())

    # Build pipeline components
    bot_filter = BotFilter(cfg)
    ticker_extractor = TickerExtractor(use_spacy=False)  # spaCy optional
    vader = VaderClassifier()
    finbert = FinBERTClassifier(batch_size=cfg.finbert_batch_size)
    pipeline = NLPPipeline(
        bot_filter=bot_filter,
        ticker_extractor=ticker_extractor,
        prefilter=vader,
        classifier=finbert,
        cfg=cfg,
    )
    bus = TradingEventBus(redis)

    task = asyncio.create_task(
        run_nlp_service(bus, pipeline, redis),
        name="nlp:main",
    )

    def _shutdown(sig: int, _frame: object) -> None:
        logger.info("Received signal %d — shutting down NLP service", sig)
        task.cancel()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        await task
    except asyncio.CancelledError:
        logger.info("NLP service stopped")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
