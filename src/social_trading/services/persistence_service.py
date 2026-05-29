"""
Persistence Service — writes Redis stream events to PostgreSQL.

Five concurrent consumer tasks:

  raw_social_task:         Consumes raw_social → inserts into social_raw table.
  sentiment_task:          Consumes sentiment_signals → inserts into sentiment_scores;
                           also periodically aggregates into sentiment_aggregates.
  signal_task:             Consumes strategy_signals → inserts into signals table.
  approved_signals_task:   Consumes selected_signals → marks signals.approved=TRUE.
  exec_events_task:        Consumes execution:events → writes trades table;
                           marks signals.executed=TRUE on position_opened.

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
    STREAM_SELECTED_SIGNALS,
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

# How often (seconds) to sync open positions and record equity
_POSITIONS_SYNC_INTERVAL_SEC = 30
_EQUITY_RECORD_INTERVAL_SEC = 60

# execution:events stream written by the execution service
_EXEC_EVENTS_STREAM = "execution:events"

# Retention windows
_RETAIN_SOCIAL_RAW_HOURS = 48
_RETAIN_SENTIMENT_SCORES_HOURS = 48
_RETAIN_SENTIMENT_AGGREGATES_DAYS = 7
_RETAIN_SIGNALS_DAYS = 7          # was 24h — extended for analytics UI
_RETAIN_ACCOUNT_EQUITY_DAYS = 365  # one year of equity history
_RETAIN_CONFIG_RUNS_DAYS = 365    # one year of parameter tuning history

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


def _mark_signal_approved(ticker: str, generated_at: str) -> None:
    """Set approved=TRUE on the signal matching (ticker, generated_at)."""
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE signals
                    SET    approved = TRUE
                    WHERE  ticker = %s
                      AND  generated_at = %s::timestamptz
                      AND  approved = FALSE
                    """,
                    (ticker, generated_at),
                )
    finally:
        conn.close()


def _mark_signal_executed(ticker: str, generated_at: str) -> None:
    """Set executed=TRUE (and approved=TRUE) on the signal matching (ticker, generated_at).

    Setting approved=TRUE here covers the race condition where the execution
    event is processed before the selected_signals consumer updates approved,
    and also backfills signals approved before run_approved_signals_task was deployed.
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE signals
                    SET    executed = TRUE,
                           approved = TRUE
                    WHERE  ticker = %s
                      AND  generated_at = %s::timestamptz
                    """,
                    (ticker, generated_at),
                )
    finally:
        conn.close()


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
            f"DELETE FROM signals WHERE generated_at < NOW() - INTERVAL '{_RETAIN_SIGNALS_DAYS} days'",
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
        (
            "account_equity",
            f"DELETE FROM account_equity WHERE timestamp < NOW() - INTERVAL '{_RETAIN_ACCOUNT_EQUITY_DAYS} days'",
        ),
        (
            "config_runs",
            f"DELETE FROM config_runs WHERE created_at < NOW() - INTERVAL '{_RETAIN_CONFIG_RUNS_DAYS} days'",
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


def _write_trade_opened(data: dict) -> None:
    """
    Insert an opening trade record into the trades table.
    Uses stream_event_id for idempotency (skip if already processed).
    Looks up signal_id from the signals table using (ticker, signal_generated_at).
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO trades
                        (ticker, direction, shares, entry_price, stop_price,
                         target_price, opened_at, status, mode, stream_event_id,
                         signal_id)
                    VALUES
                        (%(ticker)s, %(direction)s, %(shares)s, %(entry_price)s,
                         %(stop_price)s, %(target_price)s, %(opened_at)s,
                         'open', %(mode)s, %(stream_event_id)s,
                         (SELECT id FROM signals
                          WHERE ticker = %(ticker)s
                            AND generated_at = %(signal_generated_at)s::timestamptz
                          ORDER BY id DESC LIMIT 1))
                    ON CONFLICT (stream_event_id) DO NOTHING
                    """,
                    {
                        "ticker": data.get("ticker", ""),
                        "direction": data.get("direction", "LONG"),
                        "shares": _int(data.get("shares", 0)),
                        "entry_price": _float(data.get("entry_price", 0)),
                        "stop_price": _float(data.get("stop_loss", 0)) or None,
                        "target_price": _float(data.get("take_profit", 0)) or None,
                        "opened_at": data.get("opened_at") or datetime.now(UTC).isoformat(),
                        "mode": data.get("mode", "paper"),
                        "stream_event_id": data.get("stream_event_id"),
                        "signal_generated_at": data.get("signal_generated_at") or None,
                    },
                )
    except psycopg2.Error as exc:
        logger.warning("trade_opened insert failed (%s): %s", data.get("ticker"), exc)
    finally:
        conn.close()


def _write_trade_closed(data: dict) -> None:
    """
    Update the most recent open trade for this ticker with exit details.
    Matches on opened_at when available for precision; falls back to latest open.
    """
    ticker = data.get("ticker", "")
    exit_price = _float(data.get("exit_price", 0))
    closed_at = data.get("closed_at") or datetime.now(UTC).isoformat()
    exit_reason = data.get("exit_reason", "unknown")
    opened_at = data.get("opened_at") or None

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if opened_at:
                    cur.execute(
                        "SELECT id, entry_price, shares, direction FROM trades "
                        "WHERE ticker = %s AND status = 'open' AND opened_at = %s "
                        "ORDER BY id DESC LIMIT 1",
                        (ticker, opened_at),
                    )
                else:
                    cur.execute(
                        "SELECT id, entry_price, shares, direction FROM trades "
                        "WHERE ticker = %s AND status = 'open' ORDER BY id DESC LIMIT 1",
                        (ticker,),
                    )
                row = cur.fetchone()
                if not row:
                    logger.debug("No open trade found for %s to close", ticker)
                    return
                trade_id, entry_price, shares, direction = row

                # Calculate P&L (None when exit_price unknown, e.g. IB_EXTERNAL)
                if exit_price > 0 and entry_price and shares:
                    if direction == "LONG":
                        pnl = (exit_price - entry_price) * shares
                    else:
                        pnl = (entry_price - exit_price) * shares
                    pnl_pct = pnl / (entry_price * shares) * 100 if entry_price and shares else 0.0
                else:
                    pnl = None
                    pnl_pct = None

                cur.execute(
                    """
                    UPDATE trades
                    SET exit_price  = %(exit_price)s,
                        exit_reason = %(exit_reason)s,
                        closed_at   = %(closed_at)s,
                        net_pnl     = %(net_pnl)s,
                        pnl         = %(pnl)s,
                        pnl_pct     = %(pnl_pct)s,
                        status      = 'closed'
                    WHERE id = %(id)s
                    """,
                    {
                        "exit_price": exit_price if exit_price > 0 else None,
                        "exit_reason": exit_reason,
                        "closed_at": closed_at,
                        "net_pnl": pnl,
                        "pnl": pnl,
                        "pnl_pct": pnl_pct,
                        "id": trade_id,
                    },
                )
    except psycopg2.Error as exc:
        logger.warning("trade_closed update failed (%s): %s", ticker, exc)
    finally:
        conn.close()


def _sync_positions(positions: list[dict]) -> None:
    """
    Upsert current open positions into the positions table.
    Deletes any row whose ticker is not in the live list (closed positions).
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                live_tickers = {p["ticker"] for p in positions}
                if live_tickers:
                    cur.execute(
                        "DELETE FROM positions WHERE ticker NOT IN %s",
                        (tuple(live_tickers),),
                    )
                else:
                    cur.execute("DELETE FROM positions")
                for p in positions:
                    cur.execute(
                        """
                        INSERT INTO positions
                            (ticker, direction, shares, entry_price,
                             unrealized_pnl, stop_loss, take_profit,
                             high_water_mark, opened_at, updated_at)
                        VALUES
                            (%(ticker)s, %(direction)s, %(shares)s, %(entry_price)s,
                             %(unrealized_pnl)s, %(stop_loss)s, %(take_profit)s,
                             %(high_water_mark)s, %(opened_at)s, NOW())
                        ON CONFLICT (ticker) DO UPDATE SET
                            direction       = EXCLUDED.direction,
                            shares          = EXCLUDED.shares,
                            entry_price     = EXCLUDED.entry_price,
                            unrealized_pnl  = EXCLUDED.unrealized_pnl,
                            stop_loss       = EXCLUDED.stop_loss,
                            take_profit     = EXCLUDED.take_profit,
                            high_water_mark = EXCLUDED.high_water_mark,
                            opened_at       = EXCLUDED.opened_at,
                            updated_at      = NOW()
                        """,
                        {
                            "ticker": p["ticker"],
                            "direction": p.get("direction", "LONG"),
                            "shares": _int(p.get("shares", 0)),
                            "entry_price": _float(p.get("entry_price", 0)),
                            "unrealized_pnl": _float(p.get("unrealized_pnl", 0)),
                            "stop_loss": _float(p.get("stop_loss", 0)) or None,
                            "take_profit": _float(p.get("take_profit", 0)) or None,
                            "high_water_mark": _float(p.get("high_water_mark", 0)) or None,
                            "opened_at": p.get("opened_at"),
                        },
                    )
    except psycopg2.Error as exc:
        logger.warning("positions sync failed: %s", exc)
    finally:
        conn.close()


def _insert_account_equity(nlv: float, mode: str) -> None:
    """Insert an equity snapshot into account_equity table."""
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO account_equity (equity, mode) VALUES (%s, %s)",
                    (nlv, mode),
                )
    except psycopg2.Error as exc:
        logger.warning("account_equity insert failed: %s", exc)
    finally:
        conn.close()


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


async def run_approved_signals_task(bus: TradingEventBus) -> None:
    """
    Consume selected_signals stream and mark matching DB rows as approved=TRUE.

    The risk service writes to selected_signals after each signal passes all
    risk checks.  We update signals.approved here rather than in the risk service
    itself to keep DB writes in one process.
    """
    await bus.create_group(STREAM_SELECTED_SIGNALS, _GROUP)
    while True:
        messages = await bus.consume(
            STREAM_SELECTED_SIGNALS, _GROUP, "persist-approved-0", count=_BATCH
        )
        if not messages:
            continue
        for msg_id, fields in messages:
            ticker = fields.get("ticker", "")
            generated_at = fields.get("generated_at", "")
            if ticker and generated_at:
                try:
                    await _run_db(_mark_signal_approved, ticker, generated_at)
                    logger.debug("signal approved in DB: %s @ %s", ticker, generated_at)
                except Exception as exc:
                    logger.warning("Failed to mark signal approved (%s): %s", ticker, exc)
            await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)


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


async def run_execution_events_task(bus: TradingEventBus) -> None:
    """
    Consume execution:events stream and persist trade lifecycle to PostgreSQL.

    position_opened → INSERT into trades (status='open')
    position_closed → UPDATE trades (status='closed', set exit info + P&L)
    """
    await bus.create_group(_EXEC_EVENTS_STREAM, _GROUP)
    logger.info("Execution events consumer started (stream=%s)", _EXEC_EVENTS_STREAM)
    while True:
        try:
            messages = await bus.consume(
                _EXEC_EVENTS_STREAM, _GROUP, "persist-exec-0", count=_BATCH
            )
            for msg_id, fields in messages:
                event_type = fields.get("event", "")
                # Include stream message ID for idempotent inserts
                fields["stream_event_id"] = msg_id
                try:
                    if event_type == "position_opened":
                        await _run_db(_write_trade_opened, fields)
                        # Also mark the originating signal as executed
                        signal_ts = fields.get("signal_generated_at", "")
                        ticker = fields.get("ticker", "")
                        if ticker and signal_ts:
                            try:
                                await _run_db(_mark_signal_executed, ticker, signal_ts)
                            except Exception as exc:
                                logger.warning(
                                    "Failed to mark signal executed (%s): %s", ticker, exc
                                )
                        logger.info(
                            "[EXEC_EVENTS] Trade opened: %s %s",
                            fields.get("direction", ""), fields.get("ticker", ""),
                        )
                    elif event_type == "position_closed":
                        await _run_db(_write_trade_closed, fields)
                        logger.info(
                            "[EXEC_EVENTS] Trade closed: %s reason=%s",
                            fields.get("ticker", ""), fields.get("exit_reason", ""),
                        )
                    else:
                        logger.debug("Unknown execution event type: %s", event_type)
                except Exception as exc:
                    logger.warning(
                        "Failed to persist execution event %s (%s): %s",
                        event_type, fields.get("ticker", ""), exc,
                    )
                await bus.ack(_EXEC_EVENTS_STREAM, _GROUP, msg_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("[EXEC_EVENTS] Unhandled error: %s", exc, exc_info=True)
            await asyncio.sleep(5.0)


async def run_positions_sync_task(redis: aioredis.Redis) -> None:
    """
    Periodically sync positions:live Redis hash to the PostgreSQL positions table
    and record equity snapshots in account_equity.

    Runs every _POSITIONS_SYNC_INTERVAL_SEC seconds.
    """
    import json as _json  # noqa: PLC0415

    loop = asyncio.get_event_loop()
    last_equity = 0.0
    last_equity_ts = 0.0

    while True:
        try:
            await asyncio.sleep(_POSITIONS_SYNC_INTERVAL_SEC)

            # ── Sync open positions ───────────────────────────────────────────
            raw = await redis.hgetall("positions:live")
            positions: list[dict] = []
            for _, v in raw.items():
                try:
                    p = _json.loads(v.decode() if isinstance(v, bytes) else v)
                    positions.append(p)
                except Exception:
                    continue
            await _run_db(_sync_positions, positions)
            logger.debug(
                "positions: synced %d open position(s) to DB", len(positions)
            )

            # ── Record equity snapshot ────────────────────────────────────────
            now = loop.time()
            if now - last_equity_ts >= _EQUITY_RECORD_INTERVAL_SEC:
                account = await redis.hgetall("account:state")
                if account:
                    nlv_raw = account.get(b"net_liquidation", account.get("net_liquidation", b"0"))
                    nlv = _float(nlv_raw.decode() if isinstance(nlv_raw, bytes) else nlv_raw)
                    mode_raw = await redis.get("trading:mode")
                    mode = (mode_raw.decode() if isinstance(mode_raw, bytes) else (mode_raw or "paper"))
                    # Only write when equity has changed meaningfully
                    if nlv > 0 and abs(nlv - last_equity) > 0.01:
                        await _run_db(_insert_account_equity, nlv, mode)
                        last_equity = nlv
                last_equity_ts = now

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("[POSITIONS_SYNC] Unhandled error: %s", exc, exc_info=True)
            await asyncio.sleep(10.0)


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
        asyncio.create_task(run_approved_signals_task(bus), name="approved_signals"),
        asyncio.create_task(run_prune_task(), name="prune"),
        asyncio.create_task(run_execution_events_task(bus), name="exec_events"),
        asyncio.create_task(run_positions_sync_task(redis), name="positions_sync"),
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
