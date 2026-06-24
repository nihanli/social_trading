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
_redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
from social_trading.monitoring.log_handler import RedisLogHandler  # noqa: E402
logging.getLogger().addHandler(RedisLogHandler("persistence", _redis_url))
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
_RETAIN_SIGNALS_DAYS = 90          # unexecuted signals kept 90d for backtest; trade-linked kept forever
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
                        cur.execute("SAVEPOINT row_sp")
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
                        cur.execute("RELEASE SAVEPOINT row_sp")
                        inserted += cur.rowcount
                    except psycopg2.Error as exc:
                        logger.debug("social_raw insert skip (%s): %s", r.get("id"), exc)
                        cur.execute("ROLLBACK TO SAVEPOINT row_sp")
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
                        cur.execute("SAVEPOINT row_sp")
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
                        cur.execute("RELEASE SAVEPOINT row_sp")
                        inserted += cur.rowcount
                    except psycopg2.Error as exc:
                        logger.debug("sentiment_scores insert skip: %s", exc)
                        cur.execute("ROLLBACK TO SAVEPOINT row_sp")
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
                        cur.execute("SAVEPOINT row_sp")
                        cur.execute(
                            """
                            INSERT INTO signals
                                (timestamp, ticker, strategy, direction,
                                 confidence, sentiment_score, mention_zscore,
                                 quality_score, signal_phase, generated_at,
                                 momentum, convergence, proactivity, atr)
                            VALUES (%(ts)s, %(ticker)s, %(strategy)s,
                                    %(direction)s, %(confidence)s,
                                    %(sentiment_score)s, %(mention_zscore)s,
                                    %(quality_score)s, %(signal_phase)s, %(ts)s,
                                    %(momentum)s, %(convergence)s, %(proactivity)s,
                                    %(atr)s)
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
                                "signal_phase": r.get("signal_phase") or None,
                                # momentum=0 means "no market data" → store NULL to distinguish
                                # from a genuinely flat price; convergence/proactivity are always
                                # meaningful numbers so store them as-is (never coerce to NULL).
                                "momentum": _float(r.get("momentum")) or None,
                                "convergence": _float(r.get("convergence")),
                                "proactivity": _float(r.get("proactivity"), default=1.0),
                                "atr": _float(r.get("atr")) or None,
                            },
                        )
                        cur.execute("RELEASE SAVEPOINT row_sp")
                        inserted += cur.rowcount
                    except psycopg2.Error as exc:
                        logger.debug("signals insert skip: %s", exc)
                        cur.execute("ROLLBACK TO SAVEPOINT row_sp")
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


def _mark_signal_rejected(ticker: str, generated_at: str, reason: str) -> None:
    """Set rejection_reason on the most recent unrejected signal for (ticker, generated_at).

    Safe to call multiple times — only updates rows where rejection_reason IS NULL
    so a prior rejection reason is never overwritten.
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE signals
                    SET    rejection_reason = %s
                    WHERE  ticker = %s
                      AND  generated_at = %s::timestamptz
                      AND  rejection_reason IS NULL
                    """,
                    (reason, ticker, generated_at),
                )
    finally:
        conn.close()


def _mark_latest_signal_executed(ticker: str, within_hours: int = 48) -> bool:
    """
    Best-effort: mark the most recent approved-but-unexecuted signal for `ticker`
    as executed. Used when we have an open position (adopted from IB or prior session)
    but no precise signal_generated_at timestamp.

    Returns True if a row was updated, False otherwise.
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
                    WHERE  id = (
                        SELECT id FROM signals
                        WHERE  ticker = %s
                          AND  executed = FALSE
                          AND  generated_at >= NOW() - (%s || ' hours')::interval
                        ORDER  BY generated_at DESC
                        LIMIT  1
                    )
                    """,
                    (ticker, str(within_hours)),
                )
                return cur.rowcount > 0
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
            # signals are only pruned when no trade references them (FK RESTRICT).
            # Signals linked to trades are intentionally retained for optimization.
            "signals",
            f"DELETE FROM signals WHERE generated_at < NOW() - INTERVAL '{_RETAIN_SIGNALS_DAYS} days' AND id NOT IN (SELECT signal_id FROM trades WHERE signal_id IS NOT NULL)",
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
            # Pre-delete sentiment_scores that reference posts about to be pruned
            # but whose scored_at is recent (NLP backlog).  Without this the FK
            # constraint sentiment_scores_post_id_fkey blocks the social_raw DELETE.
            "sentiment_scores (orphaned by social_raw prune)",
            f"""
            DELETE FROM sentiment_scores
            WHERE post_id IN (
                SELECT post_id FROM social_raw
                WHERE ingested_at < NOW() - INTERVAL '{_RETAIN_SOCIAL_RAW_HOURS} hours'
            )
            """,
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
        (
            # price_ohlc: prune any ticker that has no signal within the retention
            # window.  Tickers with live signals retain all bars for backtest.
            "price_ohlc",
            f"""
            DELETE FROM price_ohlc
            WHERE ticker NOT IN (
                SELECT DISTINCT ticker FROM signals
                WHERE generated_at > NOW() - INTERVAL '{_RETAIN_SIGNALS_DAYS} days'
            )
            OR bar_datetime < NOW() - INTERVAL '{_RETAIN_SIGNALS_DAYS} days'
            """,
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
                         signal_id, atr_at_entry)
                    VALUES
                        (%(ticker)s, %(direction)s, %(shares)s, %(entry_price)s,
                         %(stop_price)s, %(target_price)s, %(opened_at)s,
                         'open', %(mode)s, %(stream_event_id)s,
                         (SELECT id FROM signals
                          WHERE ticker = %(ticker)s
                            AND generated_at = %(signal_generated_at)s::timestamptz
                          ORDER BY id DESC LIMIT 1),
                         (SELECT atr FROM signals
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
                        # Accept both naming conventions: trade loop uses stop_loss/take_profit;
                        # reconcile/adoption events use stop_price/target_price.
                        "stop_price": _float(data.get("stop_loss") or data.get("stop_price", 0)) or None,
                        "target_price": _float(data.get("take_profit") or data.get("target_price", 0)) or None,
                        "opened_at": data.get("opened_at") or datetime.now(UTC).isoformat(),
                        "mode": data.get("mode", "live"),
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
                    row = cur.fetchone()
                    if not row:
                        # Exact timestamp match failed (e.g. microsecond drift from
                        # a service restart). Fall back to the most recent open trade.
                        logger.debug(
                            "No exact opened_at match for %s (%s) — using latest open trade",
                            ticker, opened_at,
                        )
                        cur.execute(
                            "SELECT id, entry_price, shares, direction FROM trades "
                            "WHERE ticker = %s AND status = 'open' ORDER BY id DESC LIMIT 1",
                            (ticker,),
                        )
                        row = cur.fetchone()
                else:
                    cur.execute(
                        "SELECT id, entry_price, shares, direction FROM trades "
                        "WHERE ticker = %s AND status = 'open' ORDER BY id DESC LIMIT 1",
                        (ticker,),
                    )
                    row = cur.fetchone()
                if not row:
                    # No open trade row found — position was opened before persistence
                    # was tracking it (adopted from IB, prior session, etc.).
                    # Insert a synthetic closed record if we have enough data to compute P&L.
                    synth_entry = _float(data.get("entry_price", 0))
                    synth_shares = _int(data.get("shares", 0))
                    synth_dir = data.get("direction", "LONG")
                    synth_opened_at = data.get("opened_at") or closed_at
                    synth_pnl: float | None = None
                    synth_pnl_pct: float | None = None
                    if exit_price > 0 and synth_entry > 0 and synth_shares > 0:
                        synth_pnl = (
                            (exit_price - synth_entry) * synth_shares
                            if synth_dir == "LONG"
                            else (synth_entry - exit_price) * synth_shares
                        )
                        synth_pnl_pct = synth_pnl / (synth_entry * synth_shares) * 100
                    stream_id = data.get("stream_event_id", "")
                    cur.execute(
                        """
                        INSERT INTO trades
                            (ticker, direction, shares, entry_price,
                             opened_at, closed_at, exit_price, exit_reason,
                             net_pnl, pnl, pnl_pct, status, mode, stream_event_id)
                        VALUES
                            (%(ticker)s, %(direction)s, %(shares)s, %(entry_price)s,
                             %(opened_at)s, %(closed_at)s, %(exit_price)s, %(exit_reason)s,
                             %(net_pnl)s, %(pnl)s, %(pnl_pct)s, 'closed', %(mode)s,
                             %(stream_event_id)s)
                        ON CONFLICT (stream_event_id) DO NOTHING
                        """,
                        {
                            "ticker": ticker,
                            "direction": synth_dir,
                            "shares": synth_shares or None,
                            "entry_price": synth_entry or None,
                            "opened_at": synth_opened_at,
                            "closed_at": closed_at,
                            "exit_price": exit_price if exit_price > 0 else None,
                            "exit_reason": exit_reason,
                            "net_pnl": synth_pnl,
                            "pnl": synth_pnl,
                            "pnl_pct": synth_pnl_pct,
                            "mode": data.get("mode", "live"),
                            "stream_event_id": stream_id or None,
                        },
                    )
                    logger.info(
                        "No open trade found for %s — inserted synthetic closed record", ticker
                    )
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


def _update_entry_price(data: dict) -> None:
    """
    Correct entry_price=0 on a trade row after an async IB fill arrives.

    Idempotent: only updates rows where entry_price is currently 0 so
    replayed events cannot overwrite a previously corrected price.
    Recomputes P&L when the position is already closed (exit_price set).

    Matches by (ticker, opened_at) with a fallback to the latest row with
    entry_price=0 for the ticker.
    """
    ticker = data.get("ticker", "")
    entry_price = _float(data.get("entry_price", 0))
    opened_at = data.get("opened_at") or None
    if not ticker or entry_price <= 0:
        return
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if opened_at:
                    cur.execute(
                        "SELECT id, exit_price, shares, direction FROM trades "
                        "WHERE ticker = %s AND opened_at = %s::timestamptz AND entry_price = 0 "
                        "ORDER BY id DESC LIMIT 1",
                        (ticker, opened_at),
                    )
                    row = cur.fetchone()
                    if not row:
                        # Microsecond-drift fallback
                        cur.execute(
                            "SELECT id, exit_price, shares, direction FROM trades "
                            "WHERE ticker = %s AND entry_price = 0 ORDER BY id DESC LIMIT 1",
                            (ticker,),
                        )
                        row = cur.fetchone()
                else:
                    cur.execute(
                        "SELECT id, exit_price, shares, direction FROM trades "
                        "WHERE ticker = %s AND entry_price = 0 ORDER BY id DESC LIMIT 1",
                        (ticker,),
                    )
                    row = cur.fetchone()
                if not row:
                    logger.debug(
                        "entry_price update: no row with entry_price=0 for %s — already corrected", ticker
                    )
                    return
                trade_id, exit_price_db, shares, direction = row
                # Recompute P&L if position is already closed
                pnl = None
                pnl_pct = None
                ep_exit = float(exit_price_db) if exit_price_db else 0.0
                if ep_exit > 0 and shares and float(shares) > 0:
                    sh = float(shares)
                    pnl = (ep_exit - entry_price) * sh if direction == "LONG" else (entry_price - ep_exit) * sh
                    pnl_pct = pnl / (entry_price * sh) * 100
                cur.execute(
                    "UPDATE trades SET entry_price = %s, pnl = %s, net_pnl = %s, pnl_pct = %s WHERE id = %s",
                    (entry_price, pnl, pnl, pnl_pct, trade_id),
                )
                logger.info("entry_price corrected for %s: 0 → %.4f (trade id=%s)", ticker, entry_price, trade_id)
    except psycopg2.Error as exc:
        logger.warning("entry_price update failed (%s): %s", ticker, exc)
    finally:
        conn.close()


def _update_exit_price(data: dict) -> None:
    """
    Correct exit_price on a closed trade row after an async IB fill arrives.

    Rewrites exit_price, pnl, net_pnl, and pnl_pct.  Matches by
    (ticker, opened_at) with a fallback to the latest closed row.
    """
    ticker = data.get("ticker", "")
    exit_price = _float(data.get("exit_price", 0))
    opened_at = data.get("opened_at") or None
    if not ticker or exit_price <= 0:
        return
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if opened_at:
                    cur.execute(
                        "SELECT id, entry_price, shares, direction FROM trades "
                        "WHERE ticker = %s AND opened_at = %s::timestamptz AND status = 'closed' "
                        "ORDER BY id DESC LIMIT 1",
                        (ticker, opened_at),
                    )
                    row = cur.fetchone()
                    if not row:
                        cur.execute(
                            "SELECT id, entry_price, shares, direction FROM trades "
                            "WHERE ticker = %s AND status = 'closed' ORDER BY id DESC LIMIT 1",
                            (ticker,),
                        )
                        row = cur.fetchone()
                else:
                    cur.execute(
                        "SELECT id, entry_price, shares, direction FROM trades "
                        "WHERE ticker = %s AND status = 'closed' ORDER BY id DESC LIMIT 1",
                        (ticker,),
                    )
                    row = cur.fetchone()
                if not row:
                    return
                trade_id, entry_price_db, shares, direction = row
                pnl = None
                pnl_pct = None
                ep = float(entry_price_db) if entry_price_db else 0.0
                if ep > 0 and shares and float(shares) > 0:
                    sh = float(shares)
                    pnl = (exit_price - ep) * sh if direction == "LONG" else (ep - exit_price) * sh
                    pnl_pct = pnl / (ep * sh) * 100
                cur.execute(
                    "UPDATE trades SET exit_price = %s, pnl = %s, net_pnl = %s, pnl_pct = %s WHERE id = %s",
                    (exit_price, pnl, pnl, pnl_pct, trade_id),
                )
                logger.info("exit_price corrected for %s: → %.4f (trade id=%s)", ticker, exit_price, trade_id)
    except psycopg2.Error as exc:
        logger.warning("exit_price update failed (%s): %s", ticker, exc)
    finally:
        conn.close()


def _handle_position_deleted_db(data: dict) -> None:
    ticker = data.get("ticker", "")
    if not ticker:
        return
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE trades SET status = 'deleted', closed_at = NOW(), "
                    "exit_reason = 'USER_DELETED_AT_RECONCILE' "
                    "WHERE ticker = %s AND status = 'open'",
                    (ticker,),
                )
                cur.execute("DELETE FROM positions WHERE ticker = %s", (ticker,))
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
    _next_log_at = 0  # log summary every _LOG_INTERVAL new posts
    _LOG_INTERVAL = 50
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
            logger.debug("social_raw: batch persisted %d new posts", n)
            if total >= _next_log_at:
                logger.info("social_raw: persisted %d posts cumulative", total)
                _next_log_at = total + _LOG_INTERVAL


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


async def _handle_position_opened_event(fields: dict) -> None:
    await _run_db(_write_trade_opened, fields)
    signal_ts = fields.get("signal_generated_at", "")
    ticker = fields.get("ticker", "")
    if ticker:
        try:
            if signal_ts:
                await _run_db(_mark_signal_executed, ticker, signal_ts)
            else:
                updated = await _run_db(_mark_latest_signal_executed, ticker)
                if updated:
                    logger.info(
                        "[EXEC_EVENTS] Marked latest signal executed for adopted position %s",
                        ticker,
                    )
                else:
                    logger.debug(
                        "[EXEC_EVENTS] No unexecuted signal found for adopted position %s",
                        ticker,
                    )
        except Exception as exc:
            logger.warning("Failed to mark signal executed (%s): %s", ticker, exc)
    logger.info(
        "[EXEC_EVENTS] Trade opened: %s %s",
        fields.get("direction", ""), fields.get("ticker", ""),
    )


async def _handle_position_closed_event(fields: dict) -> None:
    await _run_db(_write_trade_closed, fields)
    logger.info(
        "[EXEC_EVENTS] Trade closed: %s reason=%s",
        fields.get("ticker", ""), fields.get("exit_reason", ""),
    )


async def _handle_position_entry_updated_event(fields: dict) -> None:
    await _run_db(_update_entry_price, fields)
    logger.info(
        "[EXEC_EVENTS] Entry price corrected: %s → %.4f",
        fields.get("ticker", ""), _float(fields.get("entry_price", 0)),
    )


async def _handle_position_exit_corrected_event(fields: dict) -> None:
    await _run_db(_update_exit_price, fields)
    logger.info(
        "[EXEC_EVENTS] Exit price corrected: %s → %.4f",
        fields.get("ticker", ""), _float(fields.get("exit_price", 0)),
    )


async def _handle_position_deleted(event_data: dict) -> None:
    ticker = event_data.get("ticker", "")
    if not ticker:
        return
    try:
        await _run_db(_handle_position_deleted_db, event_data)
        logger.info("[DB] position_deleted: cleaned DB records for %s", ticker)
    except Exception as exc:
        logger.warning("[DB] position_deleted handler failed for %s: %s", ticker, exc)


async def run_execution_events_task(bus: TradingEventBus) -> None:
    """
    Consume execution:events stream and persist trade lifecycle to PostgreSQL.

    position_opened → INSERT into trades (status='open')
    position_closed → UPDATE trades (status='closed', set exit info + P&L)
    """
    await bus.create_group(_EXEC_EVENTS_STREAM, _GROUP)
    logger.info("Execution events consumer started (stream=%s)", _EXEC_EVENTS_STREAM)
    handlers = {
        "position_opened": _handle_position_opened_event,
        "position_closed": _handle_position_closed_event,
        "position_entry_updated": _handle_position_entry_updated_event,
        "position_exit_corrected": _handle_position_exit_corrected_event,
        "position_deleted": _handle_position_deleted,
    }
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
                    handler = handlers.get(event_type)
                    if handler is not None:
                        await handler(fields)
                    else:
                        logger.debug("Unknown execution event type: %s", event_type)
                    # ACK only after confirmed DB write — not in the except branch.
                    await bus.ack(_EXEC_EVENTS_STREAM, _GROUP, msg_id)
                except Exception as exc:
                    logger.warning(
                        "Failed to persist execution event %s (%s): %s — will retry",
                        event_type, fields.get("ticker", ""), exc,
                    )
                    # Do NOT ACK — message stays in PEL and will be redelivered.
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
                    mode = (mode_raw.decode() if isinstance(mode_raw, bytes) else (mode_raw or "live"))
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


# ── Price history task ─────────────────────────────────────────────────────────

_PRICE_TASK_POLL_SEC = 300          # check queue every 5 min during market hours
_MARKET_CLOSE_HOUR_ET = 16          # 4 PM Eastern = end-of-day sweep trigger
_MARKET_CLOSE_MINUTE_ET = 30        # 4:30 PM ET


def _upsert_ohlc_bars(ticker: str, bars: list[dict], timeframe: str, source: str) -> int:
    """
    Upsert OHLC bars into price_ohlc table.
    ON CONFLICT: update if new source is 'ib' (overwrite yfinance with higher-quality data).
    Returns number of rows inserted/updated.
    """
    if not bars:
        return 0
    conn = _get_conn()
    upserted = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for bar in bars:
                    try:
                        cur.execute(
                            """
                            INSERT INTO price_ohlc
                                (ticker, bar_datetime, timeframe, open, high, low,
                                 close, volume, source, fetched_at)
                            VALUES
                                (%(ticker)s, %(bar_datetime)s, %(timeframe)s,
                                 %(open)s, %(high)s, %(low)s, %(close)s,
                                 %(volume)s, %(source)s, NOW())
                            ON CONFLICT (ticker, bar_datetime, timeframe) DO UPDATE
                                SET open      = EXCLUDED.open,
                                    high      = EXCLUDED.high,
                                    low       = EXCLUDED.low,
                                    close     = EXCLUDED.close,
                                    volume    = EXCLUDED.volume,
                                    source    = EXCLUDED.source,
                                    fetched_at = EXCLUDED.fetched_at
                                WHERE EXCLUDED.source = 'ib'
                                   OR price_ohlc.source != 'ib'
                            """,
                            {
                                "ticker": ticker,
                                "bar_datetime": bar["datetime"],
                                "timeframe": timeframe,
                                "open":   bar.get("open"),
                                "high":   bar.get("high"),
                                "low":    bar.get("low"),
                                "close":  bar.get("close"),
                                "volume": bar.get("volume"),
                                "source": source,
                            },
                        )
                        upserted += cur.rowcount
                    except psycopg2.Error as exc:
                        logger.debug("[PRICE] bar upsert error %s: %s", ticker, exc)
                        conn.rollback()
    finally:
        conn.close()
    return upserted


def _get_signal_tickers_last_90d() -> list[str]:
    """Return distinct tickers that had a signal in the last 90 days."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ticker FROM signals "
                "WHERE generated_at > NOW() - INTERVAL '90 days'"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def _get_missing_intraday_dates() -> list[tuple[str, datetime]]:
    """
    Return (ticker, signal_date_utc_close) pairs that have a signal in the
    last 90 days but no 5-min bars stored for that calendar date.

    signal_date_utc_close is set to 21:00 UTC (= 5 PM ET) on the signal date
    so IB ``endDateTime`` captures the full RTH session.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT
                    s.ticker,
                    s.generated_at::date AS sig_date
                FROM signals s
                WHERE s.generated_at > NOW() - INTERVAL '90 days'
                  AND NOT EXISTS (
                      SELECT 1 FROM price_ohlc p
                      WHERE p.ticker    = s.ticker
                        AND p.timeframe = '5m'
                        AND p.bar_datetime::date = s.generated_at::date
                  )
                ORDER BY sig_date
            """)
            rows = cur.fetchall()
        result = []
        from datetime import timezone as _tz  # noqa: PLC0415
        for ticker, sig_date in rows:
            # IB endDateTime = 21:00 UTC on the signal date (covers full 9:30–16:00 ET session)
            end_dt = datetime(
                sig_date.year, sig_date.month, sig_date.day,
                21, 0, 0, tzinfo=_tz.utc,
            )
            result.append((ticker, end_dt))
        return result
    finally:
        conn.close()


async def _backfill_missing_intraday(ib_engine) -> None:
    """
    For every (ticker, signal_date) pair that has no 5-min bars, fetch a
    single day of 5-min RTH bars from IB using the precise endDateTime.
    Falls back to yfinance recent data if IB is unavailable.

    Runs as part of the daily sweep so historical gaps are filled
    automatically without manual intervention.
    """
    import yfinance as yf  # noqa: PLC0415
    from datetime import timezone as _tz  # noqa: PLC0415

    missing = await _run_db(_get_missing_intraday_dates)
    if not missing:
        return

    logger.info("[PRICE] Intraday backfill: %d (ticker, date) pairs missing 5m bars", len(missing))

    for ticker, end_dt in missing:
        bars: list[dict] = []
        source = "yfinance"

        # ── IB: fetch exactly 1 day ending at end_dt ────────────────────
        if ib_engine is not None:
            try:
                bars = await ib_engine.get_historical_bars(
                    ticker, "5 mins", "1 D",
                    end_datetime=end_dt,
                )
                if bars:
                    source = "ib"
            except Exception as exc:
                logger.debug("[PRICE] IB intraday backfill failed %s %s: %s",
                             ticker, end_dt.date(), exc)

        # ── yfinance fallback: only useful for recent dates (≤7 days) ───
        if not bars:
            today_utc = datetime.now(_tz.utc).date()
            age_days = (today_utc - end_dt.date()).days
            if age_days <= 7:
                try:
                    df = yf.download(ticker, period="7d", interval="5m",
                                     progress=False, auto_adjust=True)
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0)
                    target_date = end_dt.date()
                    for ts, row in df.iterrows():
                        dt = ts.to_pydatetime()
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=_tz.utc)
                        if dt.date() != target_date:
                            continue
                        try:
                            bars.append({
                                "datetime": dt,
                                "open":   float(row["Open"]),
                                "high":   float(row["High"]),
                                "low":    float(row["Low"]),
                                "close":  float(row["Close"]),
                                "volume": int(row["Volume"]) if row.get("Volume") else None,
                            })
                        except (TypeError, ValueError):
                            continue
                except Exception as exc:
                    logger.debug("[PRICE] yfinance intraday backfill failed %s: %s", ticker, exc)

        if bars:
            n = await _run_db(_upsert_ohlc_bars, ticker, bars, "5m", source)
            logger.debug("[PRICE] backfill %s %s: %d bars (source=%s)",
                         ticker, end_dt.date(), n, source)
        else:
            logger.debug("[PRICE] backfill %s %s: no bars available (IB unavailable + too old for yfinance)",
                         ticker, end_dt.date())


async def _fetch_and_store_bars(
    ticker: str,
    ib_engine,          # IBKRExecutionEngine | None
    do_intraday: bool,
) -> None:
    """
    Fetch 5-min (intraday, entry-day) and/or daily bars for *ticker* and
    store in price_ohlc.  IB is tried first; yfinance is the fallback.

    Args:
        ticker:      Stock symbol.
        ib_engine:   IBKRExecutionEngine instance (or None if IB unavailable).
        do_intraday: True = also fetch 5-min bars for today.
    """
    import yfinance as yf  # noqa: PLC0415
    from datetime import timezone as _tz  # noqa: PLC0415

    # ── Daily bars (90 days) ────────────────────────────────────────────────
    daily_bars: list[dict] = []
    daily_source = "yfinance"

    if ib_engine is not None:
        try:
            daily_bars = await ib_engine.get_historical_bars(ticker, "1 day", "90 D")
            if daily_bars:
                daily_source = "ib"
        except Exception as exc:
            logger.debug("[PRICE] IB daily fetch failed for %s: %s", ticker, exc)

    if not daily_bars:
        try:
            df = yf.download(ticker, period="90d", interval="1d",
                             progress=False, auto_adjust=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if not df.empty:
                for ts, row in df.iterrows():
                    dt = ts.to_pydatetime()
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=_tz.utc)
                    try:
                        daily_bars.append({
                            "datetime": dt,
                            "open":  float(row["Open"]),
                            "high":  float(row["High"]),
                            "low":   float(row["Low"]),
                            "close": float(row["Close"]),
                            "volume": int(row["Volume"]) if row.get("Volume") else None,
                        })
                    except (TypeError, ValueError):
                        continue
        except Exception as exc:
            logger.warning("[PRICE] yfinance daily fetch failed for %s: %s", ticker, exc)

    if daily_bars:
        n = await _run_db(_upsert_ohlc_bars, ticker, daily_bars, "1d", daily_source)
        logger.debug("[PRICE] %s: upserted %d daily bars (source=%s)", ticker, n, daily_source)

    # ── Intraday 5-min bars (today only) ───────────────────────────────────
    if not do_intraday:
        return

    intra_bars: list[dict] = []
    intra_source = "yfinance"

    if ib_engine is not None:
        try:
            intra_bars = await ib_engine.get_historical_bars(ticker, "5 mins", "1 D")
            if intra_bars:
                intra_source = "ib"
        except Exception as exc:
            logger.debug("[PRICE] IB intraday fetch failed for %s: %s", ticker, exc)

    if not intra_bars:
        try:
            df = yf.download(ticker, period="1d", interval="5m",
                             progress=False, auto_adjust=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if not df.empty:
                for ts, row in df.iterrows():
                    dt = ts.to_pydatetime()
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=_tz.utc)
                    try:
                        intra_bars.append({
                            "datetime": dt,
                            "open":  float(row["Open"]),
                            "high":  float(row["High"]),
                            "low":   float(row["Low"]),
                            "close": float(row["Close"]),
                            "volume": int(row["Volume"]) if row.get("Volume") else None,
                        })
                    except (TypeError, ValueError):
                        continue
        except Exception as exc:
            logger.warning("[PRICE] yfinance intraday fetch failed for %s: %s", ticker, exc)

    if intra_bars:
        n = await _run_db(_upsert_ohlc_bars, ticker, intra_bars, "5m", intra_source)
        logger.debug("[PRICE] %s: upserted %d intraday bars (source=%s)", ticker, n, intra_source)


async def run_price_history_task(redis: aioredis.Redis) -> None:
    """
    Background task that pre-stores OHLC bars so the backtest engine never
    needs to hit an external API at query time.

    Two behaviours per tick:

    1. **Entry-day intraday sweep** (4:30 PM ET only):
       Pop all tickers from `price_fetch:queue` (populated by signal_service
       when a signal fires) and fetch complete entry-day 5-min bars.

    2. **Daily bar sweep** (every tick, ~5 min):
       Fetch daily OHLC for all tickers that have a signal in the last 90
       days, keeping the price_ohlc table up-to-date for multi-day backtest
       simulation.

    IB is the primary source; yfinance is the fallback for both.
    """
    from datetime import timezone as _tz  # noqa: PLC0415

    import pytz  # noqa: PLC0415

    _ET = pytz.timezone("America/New_York")
    _last_daily_sweep: datetime | None = None
    _daily_sweep_interval = 3600  # run daily sweep at most once per hour

    logger.info("[PRICE] Price history task started")

    # Lazy-import IB engine — only available at runtime, not in tests.
    ib_engine = None
    try:
        from social_trading.execution.ibkr import IBKRExecutionEngine  # noqa: PLC0415
        # Access shared IB connection via Redis key that holds connectivity state.
        # If no IB connection is available, ib_engine stays None and we use yfinance.
        ib_raw = await redis.get("ib:connected")
        if ib_raw and (ib_raw.decode() if isinstance(ib_raw, bytes) else ib_raw) == "1":
            # Re-use the execution service's IB connection via module-level reference.
            from social_trading.services import execution_service as _es  # noqa: PLC0415
            ib_engine = getattr(_es, "_ib_engine", None)
    except Exception as exc:
        logger.debug("[PRICE] IB engine not available: %s", exc)

    while True:
        try:
            await asyncio.sleep(_PRICE_TASK_POLL_SEC)

            now_et = datetime.now(_ET)
            is_eod = (
                now_et.hour == _MARKET_CLOSE_HOUR_ET
                and now_et.minute >= _MARKET_CLOSE_MINUTE_ET
            )

            # ── Entry-day intraday sweep (4:30 PM ET) ──────────────────────
            if is_eod:
                queued_raw = await redis.smembers("price_fetch:queue")
                queued = [
                    (t.decode() if isinstance(t, bytes) else t)
                    for t in queued_raw
                ]
                if queued:
                    logger.info(
                        "[PRICE] EOD intraday sweep: %d tickers in queue", len(queued)
                    )
                    for ticker in queued:
                        try:
                            await _fetch_and_store_bars(ticker, ib_engine, do_intraday=True)
                        except Exception as exc:
                            logger.warning("[PRICE] intraday fetch error %s: %s", ticker, exc)
                    await redis.delete("price_fetch:queue")

            # ── Daily bar sweep (every hour) ────────────────────────────────
            now_utc = datetime.now(tz=_tz.utc)
            if (
                _last_daily_sweep is None
                or (now_utc - _last_daily_sweep).total_seconds() >= _daily_sweep_interval
            ):
                tickers = await _run_db(_get_signal_tickers_last_90d)
                if tickers:
                    logger.info("[PRICE] Daily sweep: %d tickers", len(tickers))
                    for ticker in tickers:
                        try:
                            await _fetch_and_store_bars(ticker, ib_engine, do_intraday=False)
                        except Exception as exc:
                            logger.warning("[PRICE] daily fetch error %s: %s", ticker, exc)

                # Fill any (ticker, date) gaps in 5m intraday coverage using IB.
                try:
                    await _backfill_missing_intraday(ib_engine)
                except Exception as exc:
                    logger.warning("[PRICE] intraday backfill error: %s", exc)

                _last_daily_sweep = now_utc

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("[PRICE] Unhandled error: %s", exc, exc_info=True)
            await asyncio.sleep(30.0)



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
        asyncio.create_task(run_price_history_task(redis), name="price_history"),
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
