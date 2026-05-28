"""
Persistence Service — writes Redis stream events to PostgreSQL.

Three concurrent consumer tasks:

  raw_social_task:       Consumes raw_social → inserts into social_raw table.
  sentiment_task:        Consumes sentiment_signals → inserts into sentiment_scores;
                         also periodically aggregates into sentiment_aggregates.
  signal_task:           Consumes strategy_signals → inserts into signals table.

This service is the bridge between the Redis-based event pipeline and the
PostgreSQL tables that power the Streamlit monitoring UI.

Run:
    python -m social_trading.services.persistence_service
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal as os_signal
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import psycopg2
import psycopg2.extras
import redis.asyncio as aioredis
from dotenv import load_dotenv

from social_trading.core.events import (
    STREAM_RAW_SOCIAL,
    STREAM_SENTIMENT,
    STREAM_STRATEGY_SIGNALS,
)
from social_trading.storage.event_bus import TradingEventBus

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_GROUP = "persist"
_CONSUMER = "persist-0"
_BATCH = 50

# How often (seconds) to run the sentiment aggregation roll-up
_AGGREGATE_INTERVAL_SEC = 60

# How often (seconds) to run DB pruning
_PRUNE_INTERVAL_SEC = 3600  # once per hour

# Retention windows
_RETAIN_SOCIAL_RAW_HOURS = 48
_RETAIN_SENTIMENT_SCORES_HOURS = 48
_RETAIN_SENTIMENT_AGGREGATES_DAYS = 7
_RETAIN_SIGNALS_HOURS = 24

# Thread pool for blocking psycopg2 calls
_executor = ThreadPoolExecutor(max_workers=4)


# ── Database connection ────────────────────────────────────────────────────────

def _get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "trading"),
        user=os.getenv("DB_USER", "trader"),
        password=os.getenv("DB_PASSWORD", "changeme"),
    )


async def _run_db(fn, *args):
    """Run a blocking DB function in the thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, fn, *args)


# ── DB write helpers ───────────────────────────────────────────────────────────

def _write_social_raw(rows: list[dict]) -> int:
    """Insert raw social posts. Returns count inserted."""
    if not rows:
        return 0
    conn = _get_conn()
    inserted = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for r in rows:
                    try:
                        cur.execute(
                            """
                            INSERT INTO social_raw
                                (source, post_id, ticker, raw_text, author,
                                 followers, likes, retweets, created_at)
                            VALUES (%(source)s, %(post_id)s, %(ticker)s, %(text)s,
                                    %(author_id)s, %(author_followers)s, %(likes)s,
                                    %(reposts)s, %(collected_at)s)
                            ON CONFLICT (post_id) DO NOTHING
                            """,
                            {
                                "source": r.get("source", ""),
                                "post_id": r.get("id", ""),
                                "ticker": r.get("ticker", ""),
                                "text": r.get("text", ""),
                                "author_id": r.get("author_id", ""),
                                "author_followers": _int(r.get("author_followers", 0)),
                                "likes": _int(r.get("likes", 0)),
                                "reposts": _int(r.get("reposts", 0)),
                                "collected_at": r.get("collected_at") or datetime.now(UTC).isoformat(),
                            },
                        )
                        inserted += cur.rowcount
                    except psycopg2.Error as exc:
                        logger.debug("social_raw insert skip (%s): %s", r.get("id"), exc)
                        conn.rollback()
    finally:
        conn.close()
    return inserted


def _write_sentiment_scores(rows: list[dict]) -> int:
    """Insert per-post sentiment scores. Returns count inserted."""
    if not rows:
        return 0
    conn = _get_conn()
    inserted = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for r in rows:
                    post_id = r.get("post_id") or None
                    # Validate that post_id exists in social_raw; if not, use NULL
                    if post_id:
                        cur.execute(
                            "SELECT 1 FROM social_raw WHERE post_id = %s LIMIT 1",
                            (post_id,),
                        )
                        if cur.fetchone() is None:
                            post_id = None
                    try:
                        cur.execute(
                            """
                            INSERT INTO sentiment_scores
                                (post_id, ticker, pos_prob, neg_prob, neu_prob,
                                 label, model, scored_at)
                            VALUES (%(post_id)s, %(ticker)s, %(pos_prob)s,
                                    %(neg_prob)s, %(neu_prob)s, %(label)s,
                                    %(model)s, %(scored_at)s)
                            """,
                            {
                                "post_id": post_id,
                                "ticker": r.get("ticker", ""),
                                "pos_prob": _float(r.get("positive", 0)),
                                "neg_prob": _float(r.get("negative", 0)),
                                "neu_prob": _float(r.get("neutral", 0)),
                                "label": _sentiment_label(r),
                                "model": r.get("model", ""),
                                "scored_at": r.get("classified_at") or datetime.now(UTC).isoformat(),
                            },
                        )
                        inserted += cur.rowcount
                    except psycopg2.Error as exc:
                        logger.debug("sentiment_scores insert skip: %s", exc)
                        conn.rollback()
    finally:
        conn.close()
    return inserted


def _write_signals(rows: list[dict]) -> int:
    """Insert trade signals. Returns count inserted."""
    if not rows:
        return 0
    conn = _get_conn()
    inserted = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for r in rows:
                    try:
                        cur.execute(
                            """
                            INSERT INTO signals
                                (timestamp, ticker, strategy, direction,
                                 confidence, sentiment_score, mention_zscore,
                                 quality_score, generated_at)
                            VALUES (%(ts)s, %(ticker)s, %(strategy)s,
                                    %(direction)s, %(confidence)s,
                                    %(sentiment_score)s, %(mention_zscore)s,
                                    %(quality_score)s, %(ts)s)
                            """,
                            {
                                "ts": r.get("generated_at") or datetime.now(UTC).isoformat(),
                                "ticker": r.get("ticker", ""),
                                "strategy": "social_momentum",
                                "direction": r.get("direction", ""),
                                "confidence": _float(r.get("quality_score", 0)),
                                "sentiment_score": _float(r.get("sentiment_score", 0)),
                                "mention_zscore": _float(r.get("volume_z_score", 0)),
                                "quality_score": _float(r.get("quality_score", 0)),
                            },
                        )
                        inserted += cur.rowcount
                    except psycopg2.Error as exc:
                        logger.debug("signals insert skip: %s", exc)
                        conn.rollback()
    finally:
        conn.close()
    return inserted


def _aggregate_sentiment() -> int:
    """
    Roll up sentiment_scores into 15-minute sentiment_aggregates buckets.
    Processes the last 2 hours of data (incremental roll-up).
    Returns number of rows upserted.
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO sentiment_aggregates
                        (ticker, window_start, window_minutes,
                         avg_sentiment, weighted_score, post_count)
                    SELECT
                        ticker,
                        DATE_TRUNC('hour', scored_at)
                            + INTERVAL '15 min'
                            * FLOOR(DATE_PART('minute', scored_at) / 15),
                        15,
                        AVG(pos_prob - neg_prob),
                        AVG(pos_prob - neg_prob),
                        COUNT(*)
                    FROM sentiment_scores
                    WHERE scored_at > NOW() - INTERVAL '2 hours'
                    GROUP BY ticker,
                             DATE_TRUNC('hour', scored_at)
                                 + INTERVAL '15 min'
                                 * FLOOR(DATE_PART('minute', scored_at) / 15)
                    ON CONFLICT (ticker, window_start, window_minutes)
                    DO UPDATE SET
                        avg_sentiment  = EXCLUDED.avg_sentiment,
                        weighted_score = EXCLUDED.weighted_score,
                        post_count     = EXCLUDED.post_count
                    """
                )
                rows = cur.rowcount
        return rows
    except psycopg2.Error as exc:
        logger.warning("sentiment aggregation error: %s", exc)
        return 0
    finally:
        conn.close()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _int(val, default: int = 0) -> int:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return default


def _float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _sentiment_label(r: dict) -> str:
    pos = _float(r.get("positive", 0))
    neg = _float(r.get("negative", 0))
    if pos - neg > 0.1:
        return "positive"
    if neg - pos > 0.1:
        return "negative"
    return "neutral"


def _prune_old_data() -> dict[str, int]:
    """
    Delete rows older than their retention window from all monitoring tables.
    Returns a dict of table → rows deleted.
    """
    conn = _get_conn()
    deleted: dict[str, int] = {}
    statements = [
        (
            "signals",
            f"DELETE FROM signals WHERE generated_at < NOW() - INTERVAL '{_RETAIN_SIGNALS_HOURS} hours'",
        ),
        (
            "sentiment_aggregates",
            f"DELETE FROM sentiment_aggregates WHERE window_start < NOW() - INTERVAL '{_RETAIN_SENTIMENT_AGGREGATES_DAYS} days'",
        ),
        (
            "sentiment_scores",
            f"DELETE FROM sentiment_scores WHERE scored_at < NOW() - INTERVAL '{_RETAIN_SENTIMENT_SCORES_HOURS} hours'",
        ),
        (
            "social_raw",
            f"DELETE FROM social_raw WHERE ingested_at < NOW() - INTERVAL '{_RETAIN_SOCIAL_RAW_HOURS} hours'",
        ),
    ]
    try:
        with conn:
            with conn.cursor() as cur:
                for table, sql in statements:
                    try:
                        cur.execute(sql)
                        deleted[table] = cur.rowcount
                    except psycopg2.Error as exc:
                        logger.warning("prune %s error: %s", table, exc)
                        conn.rollback()
    finally:
        conn.close()
    return deleted


# ── Consumer tasks ─────────────────────────────────────────────────────────────

async def run_raw_social_task(bus: TradingEventBus) -> None:
    """Consume raw_social stream and persist to social_raw table."""
    await bus.create_group(STREAM_RAW_SOCIAL, _GROUP)
    total = 0
    while True:
        messages = await bus.consume(
            STREAM_RAW_SOCIAL, _GROUP, _CONSUMER, count=_BATCH
        )
        if not messages:
            continue
        rows = [fields for _, fields in messages]
        n = await _run_db(_write_social_raw, rows)
        total += n
        for msg_id, _ in messages:
            await bus.ack(STREAM_RAW_SOCIAL, _GROUP, msg_id)
        if n:
            logger.info("social_raw: persisted %d posts (total=%d)", n, total)


async def run_sentiment_task(bus: TradingEventBus) -> None:
    """Consume sentiment_signals stream, persist scores, and aggregate."""
    await bus.create_group(STREAM_SENTIMENT, _GROUP)
    total = 0
    last_agg = 0.0
    loop = asyncio.get_event_loop()
    while True:
        messages = await bus.consume(
            STREAM_SENTIMENT, _GROUP, _CONSUMER, count=_BATCH
        )
        if messages:
            rows = [fields for _, fields in messages]
            n = await _run_db(_write_sentiment_scores, rows)
            total += n
            for msg_id, _ in messages:
                await bus.ack(STREAM_SENTIMENT, _GROUP, msg_id)
            if n:
                logger.info("sentiment_scores: persisted %d rows (total=%d)", n, total)

        # Periodic aggregation
        now = loop.time()
        if now - last_agg >= _AGGREGATE_INTERVAL_SEC:
            agg_rows = await _run_db(_aggregate_sentiment)
            if agg_rows:
                logger.info("sentiment_aggregates: upserted %d buckets", agg_rows)
            last_agg = now


async def run_signal_task(bus: TradingEventBus) -> None:
    """Consume strategy_signals stream and persist to signals table."""
    await bus.create_group(STREAM_STRATEGY_SIGNALS, _GROUP)
    total = 0
    while True:
        messages = await bus.consume(
            STREAM_STRATEGY_SIGNALS, _GROUP, _CONSUMER, count=_BATCH
        )
        if not messages:
            continue
        rows = [fields for _, fields in messages]
        n = await _run_db(_write_signals, rows)
        total += n
        for msg_id, _ in messages:
            await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
        if n:
            logger.info("signals: persisted %d signals (total=%d)", n, total)


async def run_prune_task() -> None:
    """Periodically delete stale rows from all monitoring DB tables."""
    loop = asyncio.get_event_loop()
    while True:
        await asyncio.sleep(_PRUNE_INTERVAL_SEC)
        result = await _run_db(_prune_old_data)
        total_deleted = sum(result.values())
        if total_deleted:
            parts = ", ".join(f"{t}={n}" for t, n in result.items() if n)
            logger.info("DB prune: deleted %d rows (%s)", total_deleted, parts)


# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)
    bus = TradingEventBus(redis)

    logger.info("Persistence service starting — Redis: %s", redis_url)

    # Test DB connection on startup
    try:
        conn = _get_conn()
        conn.close()
        logger.info("PostgreSQL connection OK")
    except Exception as exc:
        logger.error("PostgreSQL connection FAILED: %s", exc)
        sys.exit(1)

    stop_event = asyncio.Event()

    def _handle_signal(sig, frame):  # noqa: ARG001
        logger.info("Received %s, shutting down…", sig)
        stop_event.set()

    for sig in (os_signal.SIGINT, os_signal.SIGTERM):
        os_signal.signal(sig, _handle_signal)

    tasks = [
        asyncio.create_task(run_raw_social_task(bus), name="raw_social"),
        asyncio.create_task(run_sentiment_task(bus), name="sentiment"),
        asyncio.create_task(run_signal_task(bus), name="signal"),
        asyncio.create_task(run_prune_task(), name="prune"),
    ]

    try:
        await stop_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await redis.aclose()
        _executor.shutdown(wait=False)
        logger.info("Persistence service stopped.")


if __name__ == "__main__":
    asyncio.run(main())
