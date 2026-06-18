"""
Execution Service — submits approved signals and manages open positions via IBKR.

Three concurrent loops:

  trade_loop:   Consumes selected_signals (consumer group "execution").
                For each approved signal:
                  - Skip if halted or position already open for that ticker
                  - Submit to the execution engine
                  - Persist trade to Redis (and optionally PostgreSQL)

  exit_loop:    Every cfg.signal_poll_interval_sec (default 60s):
                  - Refresh prices for all open tickers
                  - Evaluate PositionExitManager for each position
                  - Close positions that hit any exit rule
                  - Write updated account state to Redis hash 'account:state'
                  - Write per-ticker market snapshot to Redis hash 'market_data:{ticker}'
                    (consumed by risk_service and signal_service)

  command_listener:  Subscribes to Redis pub/sub "trading:commands".
                     Handles UI commands: HALT_NEW, RESUME, CLOSE_ALL, CLOSE_TICKER.

Run:
    python -m social_trading.services.execution_service     # requires TWS/IB Gateway
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import signal as os_signal
import sys
from datetime import UTC, datetime
from typing import Optional

import redis.asyncio as aioredis
from dotenv import load_dotenv

from social_trading.config.system_config import SystemConfig
from social_trading.core.events import STREAM_MAXLEN, STREAM_SELECTED_SIGNALS
from social_trading.core.market_hours import NYSE as _NYSE
from social_trading.core.models import Signal
from social_trading.core.protocols import ExecutionEngine, MarketDataProvider
from social_trading.ingest.base import MENTION_HISTORY_TIER1_SOURCES
from social_trading.market_data.composite import FallbackMarketData
from social_trading.market_data.yfinance import YFinanceMarketData
from social_trading.monitoring.metrics import (
    DAILY_PNL_PCT,
    DRAWDOWN,
    OPEN_POSITIONS_COUNT,
    ORDERS_PLACED,
    PAPER_EQUITY,
    POSITION_PNL,
    POSITIONS_CLOSED,
    start_metrics_server,
)
from social_trading.risk.circuit_breaker import CircuitBreaker
from social_trading.risk.exit_manager import PositionExitManager
from social_trading.signals.decay import is_expired as _signal_is_expired
from social_trading.storage.event_bus import TradingEventBus

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
# Warning 10167 ("not subscribed, showing delayed data") can occur when market
# data subscriptions are unavailable; suppress ib_async wrapper noise to WARNING level.
logging.getLogger("ib_async.wrapper").setLevel(logging.WARNING)
_redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
from social_trading.monitoring.log_handler import RedisLogHandler  # noqa: E402
logging.getLogger().addHandler(RedisLogHandler("execution", _redis_url))
logger = logging.getLogger(__name__)

_GROUP = "execution"
_CONSUMER = "exec-0"
_INGEST_BATCH = 16

# Shared halt flag: set by HALT_NEW command, cleared by RESUME.
# run_trade_loop checks this before opening any new position.
_halt_flag = asyncio.Event()
_ACTIVE_ENGINE: Optional[ExecutionEngine] = None
_RUNTIME_TASKS: dict[str, asyncio.Task[None]] = {}
# Per-session reconcile flag.  False until the user approves reconcile for this
# process run.  Mutable list so closures (watcher, command handlers) can update it.
# Reset to [False] at the top of main() on every service start.
_RECONCILE_DONE: list[bool] = [False]


# ── Deserialisation ────────────────────────────────────────────────────────────

def _stream_dict_to_approved(fields: dict) -> tuple[Signal, int, float, float] | None:
    """
    Parse an approved signal from selected_signals stream.
    Returns (signal, quantity, stop_loss, take_profit) or None on parse error.
    """
    try:
        signal = Signal(
            ticker=fields["ticker"],
            direction=fields["direction"],
            quality_score=float(fields["quality_score"]),
            sentiment_score=float(fields["sentiment_score"]),
            volume_z_score=float(fields["volume_z_score"]),
            momentum=float(fields["momentum"]),
            convergence=float(fields["convergence"]),
            source_post_count=int(fields["source_post_count"]),
            generated_at=datetime.fromisoformat(fields["generated_at"]),
        )
        quantity = int(fields["quantity"])
        stop_loss = float(fields["stop_loss"])
        take_profit = float(fields["take_profit"])
        return signal, quantity, stop_loss, take_profit
    except Exception as exc:
        logger.warning("malformed selected_signals message: %s", exc)
        return None


# ── Redis state writers ───────────────────────────────────────────────────────

async def _write_account_state(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
) -> None:
    """Write account state to Redis hash 'account:state' for risk service.

    Skips the write when net_liquidation is 0 (IB hasn't yet pushed account
    data — e.g. just after reconnect or during a brief disconnect).  Preserving
    the last known-good value prevents the risk service from computing a $0
    position size and rejecting every signal.
    """
    state = await engine.get_account_state()
    if state.net_liquidation <= 0:
        logger.warning(
            "[EXEC] Skipping account:state write — net_liquidation=%.2f "
            "(IB account data not yet available)",
            state.net_liquidation,
        )
        return
    await redis.hset("account:state", mapping={
        "net_liquidation": str(state.net_liquidation),
        "cash": str(state.cash),
        "daily_pnl": str(state.daily_pnl),
        "weekly_pnl": str(state.weekly_pnl),
        "drawdown_pct": str(state.drawdown_pct),
        "updated_at": datetime.now(UTC).isoformat(),
    })


async def _publish_execution_event(
    redis: aioredis.Redis,
    event_type: str,
    data: dict,
) -> str | None:
    """Publish a position lifecycle event to the execution:events stream.

    Returns the Redis stream message ID on success, or None on failure.
    The message ID is useful for correction events that need to reference the
    original trade row (e.g. position_entry_updated, position_exit_corrected).

    Side-effect: for position_opened and position_closed events, also writes
    trade:last_at:{ticker} so the risk service can enforce the per-ticker
    cooldown window (reject new signals for a ticker traded within the last hour).
    """
    try:
        fields = {"event": event_type}
        fields.update({k: str(v) if v is not None else "" for k, v in data.items()})
        msg_id = await redis.xadd(
            _EXEC_EVENTS_STREAM, fields,
            maxlen=STREAM_MAXLEN.get(_EXEC_EVENTS_STREAM, 50_000),
            approximate=True,
        )
        # Keep a lightweight per-ticker "last traded at" marker so the risk
        # service can enforce the cooldown without scanning the full event stream.
        # TTL = 2 hours (well beyond the 1-hour cooldown window) so the key
        # is automatically cleaned up after it can no longer affect decisions.
        if event_type in ("position_opened", "position_closed"):
            _ticker = data.get("ticker", "")
            _ts = data.get("opened_at") or data.get("closed_at") or ""
            if not _ts:
                _ts = datetime.now(UTC).isoformat()
            if _ticker:
                try:
                    await redis.set(f"trade:last_at:{_ticker}", str(_ts), ex=7200)
                except Exception:
                    pass
        return msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id) if msg_id else None
    except Exception as exc:
        logger.warning("[EVENTS] Failed to publish %s event: %s", event_type, exc)
        return None


async def _write_positions_to_redis(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
    pending_close: set[str] | None = None,
) -> None:
    """
    Sync open positions to positions:live Redis hash.
    Called each exit-loop cycle so the persistence service and UI see current state.

    Merge strategy: shows the union of (IB cache positions) ∪ (params-known system
    positions).  If the IB cache is stale and missing a ticker that params says
    should be open, it is still shown (using params data) so it never silently
    disappears from the UI.  The reconcile at next startup will clean up any
    positions that are genuinely closed.
    """
    try:
        positions = await engine.get_positions()

        # Safety guard: if the engine reports 0 positions but position:params
        # still has entries, IB may have returned an empty cache due to a
        # transient disconnect (e.g. TWS nightly auto-restart at ~11:45 PM ET).
        # If positions:live already exists, preserve it (don't wipe live data).
        # If positions:live has expired, fall through to rebuild it from params
        # so the UI never shows a blank positions panel while IB is disconnected.
        if not positions:
            params_raw = await redis.hgetall(_POSITION_PARAMS_KEY)
            if params_raw:
                live_exists = await redis.exists(_POSITIONS_LIVE_KEY)
                if live_exists:
                    logger.warning(
                        "[POSITIONS] IB returned 0 open positions but %d ticker(s) in "
                        "position:params — possible transient disconnect; preserving "
                        "positions:live until IB cache is reseeded",
                        len(params_raw),
                    )
                    return
                # positions:live has expired — rebuild from params so the UI
                # doesn't show a blank panel while IB is disconnected.
                logger.warning(
                    "[POSITIONS] IB returned 0 positions and positions:live expired — "
                    "rebuilding from position:params (%d ticker(s))",
                    len(params_raw),
                )
                # Fall through: ib_by_ticker will be empty; the else branch below
                # writes each ticker using params data (ib_cache_missing=True).

        # Build index of IB positions for O(1) lookup.
        ib_by_ticker = {pos.ticker: pos for pos in positions}

        # Load all system-managed params from Redis.
        all_params_raw = await redis.hgetall(_POSITION_PARAMS_KEY)
        system_params: dict[str, dict] = {}
        for k, v in all_params_raw.items():
            ticker_key = k.decode() if isinstance(k, bytes) else k
            try:
                p = json.loads(v.decode() if isinstance(v, bytes) else v)
                if p.get("source", "system") == "system":
                    system_params[ticker_key] = p
            except Exception:
                pass

        # Union: all tickers that are either in IB cache OR in params (system).
        # IB-only tickers (manual) are excluded since they have no params entry.
        all_system_tickers = set(system_params)

        # Write to a temp key then atomically RENAME it to positions:live.
        # This eliminates the brief "empty key" window that occurred with the
        # old delete+hset pipeline approach, where readers could see 0 positions
        # between the delete and the first hset executing.
        _POSITIONS_LIVE_TMP = _POSITIONS_LIVE_KEY + ":tmp"
        pipe = redis.pipeline()
        pipe.delete(_POSITIONS_LIVE_TMP)
        for ticker in all_system_tickers:
            pos = ib_by_ticker.get(ticker)
            params = system_params[ticker]

            if pos is not None:
                # Normal path — IB cache has this position.
                current_price = engine.get_price(ticker) or pos.entry_price
                if pos.direction == "LONG":
                    computed_upnl = (current_price - pos.entry_price) * pos.shares
                else:
                    computed_upnl = (pos.entry_price - current_price) * pos.shares
                unrealized_pnl = pos.unrealized_pnl if pos.unrealized_pnl != 0.0 else computed_upnl
                _is_close_pending = bool(pending_close and ticker in pending_close)
                pipe.hset(_POSITIONS_LIVE_TMP, ticker, json.dumps({
                    "ticker": ticker,
                    "direction": pos.direction,
                    "shares": pos.shares,
                    "entry_price": pos.entry_price,
                    "stop_loss": pos.stop_loss,
                    "take_profit": pos.take_profit,
                    "unrealized_pnl": round(unrealized_pnl, 2),
                    "high_water_mark": pos.high_water_mark,
                    "opened_at": pos.opened_at.isoformat() if pos.opened_at else None,
                    "source": "system",
                    **({"close_pending": True} if _is_close_pending else {}),
                }))
            else:
                # IB cache is missing this ticker — params say it should be open.
                # Show it with params data so it doesn't silently vanish from the UI.
                # The periodic reqPositionsAsync() refresh will correct this within
                # _IB_CACHE_REFRESH_SECS; the next startup reconcile will clean up
                # any that are genuinely closed.
                logger.warning(
                    "[POSITIONS] %s in position:params (source=system) but absent from "
                    "IB position cache — showing with params data (cache may be stale)",
                    ticker,
                )
                entry_price = float(params.get("entry_price", 0.0))
                shares = int(params.get("shares", 0))
                _is_close_pending = bool(pending_close and ticker in pending_close)
                pipe.hset(_POSITIONS_LIVE_TMP, ticker, json.dumps({
                    "ticker": ticker,
                    "direction": params.get("direction", "LONG"),
                    "shares": shares,
                    "entry_price": entry_price,
                    "stop_loss": float(params.get("stop_loss", 0.0)),
                    "take_profit": float(params.get("take_profit", 0.0)),
                    "unrealized_pnl": None,
                    "high_water_mark": None,
                    "opened_at": params.get("opened_at"),
                    "source": "system",
                    "ib_cache_missing": True,
                    **({"close_pending": True} if _is_close_pending else {}),
                }))
        await pipe.execute()
        # RENAME is atomic in Redis — readers see either the old complete key or the
        # new complete key, never an empty intermediate state.
        if all_system_tickers:
            await redis.rename(_POSITIONS_LIVE_TMP, _POSITIONS_LIVE_KEY)
            # No TTL — positions:live is explicitly managed (deleted when positions
            # close or when the service confirms there are none).  A TTL caused
            # positions to silently disappear from the UI when IB disconnected
            # after market close and the price-eval loop stopped refreshing the key.
        else:
            # No system positions — explicitly clear the live key so stale entries
            # from a prior session don't linger if the execution service restarts
            # without positions.
            await redis.delete(_POSITIONS_LIVE_KEY)
            await redis.delete(_POSITIONS_LIVE_TMP)
    except Exception as exc:
        logger.warning("[POSITIONS] Failed to write positions:live: %s", exc)



# ── Service loops ─────────────────────────────────────────────────────────────

async def run_trade_loop(
    bus: TradingEventBus,
    engine: ExecutionEngine,
    redis: aioredis.Redis,
    market_data: MarketDataProvider | None = None,
    mode: str = "live",
    cfg: SystemConfig | None = None,
) -> None:
    """
    Consume selected_signals and submit to execution engine.
    Runs until cancelled.
    """
    if cfg is None:
        cfg = await SystemConfig.load(redis)
    await bus.create_group(STREAM_SELECTED_SIGNALS, _GROUP)
    logger.info("Execution trade loop listening on %s", STREAM_SELECTED_SIGNALS)

    submitted = 0
    skipped = 0

    while True:
        try:
            # Reload config each cycle so UI changes to sizing/TP/trailing params
            # take effect without a service restart.
            cfg = await SystemConfig.load(redis)

            # Block new entries if reconcile has unresolved conflicts.
            # Conflicts mean IB and app state cannot be automatically reconciled
            # (e.g. position missing from IB with no fill record).  Exits and
            # price-eval continue normally; only new entries are blocked.
            _conflicts_raw = await redis.hkeys(_RECONCILE_CONFLICTS_KEY)
            if _conflicts_raw:
                _conflict_tickers = [
                    k.decode() if isinstance(k, bytes) else k for k in _conflicts_raw
                ]
                logger.warning(
                    "[TRADE] Reconcile conflicts — halting new entries: %s", _conflict_tickers,
                )
                await asyncio.sleep(5)
                continue

            # Guard market hours BEFORE consuming from the stream.  If the market
            # is closed we don't consume anything — messages stay as NEW entries
            # and are delivered next time the market opens.  This avoids filling
            # the PEL with signals that cannot be executed until next session.
            if not _NYSE.is_open():
                logger.info(
                    "[EXEC] Market closed — not consuming signals. %s",
                    _NYSE.status_str(),
                )
                await asyncio.sleep(60.0)
                continue

            messages = await bus.consume(
                STREAM_SELECTED_SIGNALS, _GROUP, _CONSUMER, count=_INGEST_BATCH
            )

            for msg_id, fields in messages:
                try:
                    parsed = _stream_dict_to_approved(fields)
                    if parsed is None:
                        await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)
                        continue

                    signal, quantity, stop_loss, take_profit = parsed

                    # Discard stale signals that sat in the stream too long
                    # (e.g. market was closed for an extended period).
                    hours_elapsed = (
                        datetime.now(UTC) - signal.generated_at
                    ).total_seconds() / 3600
                    if _signal_is_expired(hours_elapsed, cfg.signal_age_max_hours):
                        logger.warning(
                            "[EXEC] Signal for %s expired (%.1fh old, max %dh) — discarding",
                            signal.ticker, hours_elapsed, cfg.signal_age_max_hours,
                        )
                        skipped += 1
                        await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)
                        continue

                    # Skip if new positions are halted via UI command
                    if _halt_flag.is_set():
                        logger.debug("Skip %s — new positions halted", signal.ticker)
                        skipped += 1
                        await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)
                        continue

                    # Skip if position already open — three-layer guard:
                    # 1. IB positions cache (primary)
                    # 2. Engine in-memory params (catches positions where IB cache is stale
                    #    after reconnect but params are already seeded)
                    # 3. Redis position:params (ground truth — survives reconnects/restarts)
                    try:
                        already_open = signal.ticker in engine.open_tickers
                    except Exception as exc:
                        logger.warning(
                            "[EXEC] Could not check open_tickers for %s (%s) — skipping signal",
                            signal.ticker, exc,
                        )
                        await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)
                        continue

                    if not already_open and hasattr(engine, "get_position_params"):
                        already_open = signal.ticker in engine.get_position_params()

                    if not already_open:
                        try:
                            already_open = bool(await redis.hexists(_POSITION_PARAMS_KEY, signal.ticker))
                        except Exception:
                            pass

                    if already_open:
                        logger.debug("Skip %s — position already open", signal.ticker)
                        skipped += 1
                        await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)
                        continue

                    # Ensure price is in cache; fetch on-demand if exit loop hasn't warmed it yet
                    if engine.get_price(signal.ticker) is None and market_data is not None:
                        try:
                            quote = await market_data.get_quote(signal.ticker)
                            last = quote.get("last", 0.0)
                            if last > 0:
                                engine.set_price(signal.ticker, last)
                                logger.debug("On-demand price fetch for %s: %.4f", signal.ticker, last)
                        except Exception as exc:
                            logger.debug("On-demand price fetch failed for %s: %s", signal.ticker, exc)

                    result = await engine.submit_signal(
                        signal=signal,
                        quantity=quantity,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        take_profit_pct=cfg.take_profit_pct,
                        trailing_stop_pct=cfg.trailing_stop_pct,
                        trailing_stop_min_pct=cfg.trailing_stop_min_pct,
                    )

                    if result.status in ("filled", "submitted"):
                        submitted += 1
                        ORDERS_PLACED.labels(ticker=signal.ticker, status=result.status).inc()
                        # Immediately push the new position to positions:live so the
                        # UI reflects it without waiting for the next exit-loop cycle.
                        # We write directly rather than calling _write_positions_to_redis
                        # because IB's position cache may not reflect the fill yet —
                        # _write_positions_to_redis would see 0 positions and either
                        # trigger the disconnect-guard (early return) or produce an
                        # empty hash, neither of which shows the new position in the UI.
                        fill_px = result.fill_price or 0.0
                        try:
                            await redis.hset(_POSITIONS_LIVE_KEY, signal.ticker, json.dumps({
                                "ticker": signal.ticker,
                                "direction": signal.direction,
                                "shares": quantity,
                                "entry_price": fill_px,
                                "stop_loss": stop_loss,
                                "take_profit": take_profit,
                                "unrealized_pnl": 0.0,
                                "high_water_mark": fill_px,
                                "opened_at": result.submitted_at.isoformat(),
                                "source": "system",
                            }))
                            # Also write to position:params immediately so that
                            # _write_positions_to_redis includes this ticker in
                            # system_params on the very next exit-loop cycle.
                            # Without this, the exit loop's pipe.delete(positions:live)
                            # + rebuild would drop the new position before
                            # _persist_position_params_to_redis has a chance to run.
                            await redis.hset(_POSITION_PARAMS_KEY, signal.ticker, json.dumps({
                                "stop_loss": stop_loss,
                                "take_profit": take_profit,
                                "opened_at": result.submitted_at.isoformat(),
                                "direction": signal.direction,
                                "source": "system",
                                "entry_price": fill_px,
                                "shares": quantity,
                            }))
                        except Exception as _write_exc:
                            logger.warning(
                                "[EXEC] Failed to write %s to positions:live: %s",
                                signal.ticker, _write_exc,
                            )
                        await _publish_execution_event(redis, "position_opened", {
                            "ticker": signal.ticker,
                            "direction": signal.direction,
                            "shares": quantity,
                            "entry_price": result.fill_price or 0.0,
                            "stop_loss": stop_loss,
                            "take_profit": take_profit,
                            "opened_at": result.submitted_at.isoformat(),
                            "signal_generated_at": signal.generated_at.isoformat(),
                            "mode": mode,
                        })
                        await redis.lpush("trades:recent", json.dumps({
                            "ticker": signal.ticker,
                            "direction": signal.direction,
                            "quantity": quantity,
                            "fill_price": result.fill_price,
                            "stop_loss": stop_loss,
                            "take_profit": take_profit,
                            "submitted_at": result.submitted_at.isoformat(),
                            "quality_score": signal.quality_score,
                        }))
                        await redis.ltrim("trades:recent", 0, 999)
                        logger.info(
                            "[EXEC] Submitted %s %s qty=%d fill=%.4f [total=%d]",
                            signal.direction, signal.ticker, quantity,
                            result.fill_price or 0.0, submitted,
                        )

                        # ── Async entry fill tracking ──────────────────────────
                        # When IB fill arrives after our 3s wait (paper / slow market),
                        # register a callback to correct entry_price=0 in Redis + DB.
                        if result.fill_price is None and hasattr(engine, "register_order_fill_callback"):
                            _entry_order_id = int(result.order_id) if result.order_id else 0
                            _entry_ticker   = signal.ticker
                            _entry_opened_at = result.submitted_at.isoformat()
                            if _entry_order_id:
                                try:
                                    await redis.hset(
                                        _INFLIGHT_ENTRY_KEY, str(_entry_order_id),
                                        json.dumps({
                                            "ticker": _entry_ticker,
                                            "direction": signal.direction,
                                            "quantity": quantity,
                                            "opened_at": _entry_opened_at,
                                            # Store sl/tp so params can be reconstructed
                                            # if they are deleted before the fill arrives
                                            # (e.g. by a reconcile cycle between pre-market
                                            # order submission and market-open fill).
                                            "stop_loss": stop_loss,
                                            "take_profit": take_profit,
                                            "mode": mode,
                                        }),
                                    )
                                    await redis.expire(_INFLIGHT_ENTRY_KEY, _INFLIGHT_TTL_SEC)
                                except Exception as _inf_exc:
                                    logger.debug("[EXEC] Failed to write inflight entry for %s: %s", _entry_ticker, _inf_exc)

                            async def _on_entry_fill_async(
                                actual_fill: float,
                                _t: str = _entry_ticker,
                                _oa: str = _entry_opened_at,
                                _oid: str = str(_entry_order_id),
                                _dir: str = signal.direction,
                                _qty: int = quantity,
                                _sl: float = stop_loss,
                                _tp: float = take_profit,
                                _md: str = mode,
                            ) -> None:
                                logger.info("[EXEC] Async entry fill received for %s: %.4f (orderId=%s)", _t, actual_fill, _oid)
                                try:
                                    # Correct Redis position:params entry_price.
                                    # If params were deleted (e.g. by reconcile between
                                    # pre-market submission and market-open fill), reconstruct
                                    # them here so the exit loop can track the position.
                                    _pr = await redis.hget(_POSITION_PARAMS_KEY, _t)
                                    if _pr:
                                        _pd = json.loads(_pr.decode() if isinstance(_pr, bytes) else _pr)
                                        if _pd.get("entry_price", 0) == 0:
                                            _pd["entry_price"] = actual_fill
                                            await redis.hset(_POSITION_PARAMS_KEY, _t, json.dumps(_pd))
                                    else:
                                        # params were deleted — reconstruct from closure values
                                        # so the exit loop can monitor this position.
                                        logger.warning(
                                            "[EXEC] %s: position:params missing at fill time — "
                                            "reconstructing from inflight data (sl=%.4f tp=%.4f)",
                                            _t, _sl, _tp,
                                        )
                                        _pd = {
                                            "stop_loss": _sl,
                                            "take_profit": _tp,
                                            "opened_at": _oa,
                                            "direction": _dir,
                                            "source": "system",
                                            "entry_price": actual_fill,
                                            "shares": _qty,
                                        }
                                        await redis.hset(_POSITION_PARAMS_KEY, _t, json.dumps(_pd))
                                        # Also seed positions:live so the UI shows this position
                                        await redis.hset(_POSITIONS_LIVE_KEY, _t, json.dumps({
                                            "ticker": _t,
                                            "direction": _dir,
                                            "shares": _qty,
                                            "entry_price": actual_fill,
                                            "stop_loss": _sl,
                                            "take_profit": _tp,
                                            "unrealized_pnl": 0.0,
                                            "high_water_mark": actual_fill,
                                            "opened_at": _oa,
                                            "source": "system",
                                        }))
                                        # Publish position_opened so the DB service records
                                        # this trade and the signal is marked as executed.
                                        await _publish_execution_event(redis, "position_opened", {
                                            "ticker": _t,
                                            "direction": _dir,
                                            "shares": _qty,
                                            "entry_price": actual_fill,
                                            "stop_loss": _sl,
                                            "take_profit": _tp,
                                            "opened_at": _oa,
                                            "mode": _md,
                                        })
                                        logger.warning(
                                            "[EXEC] %s: position_opened published from delayed fill (params reconstructed)",
                                            _t,
                                        )
                                    # Correct positions:live entry_price when params existed
                                    _lr = await redis.hget(_POSITIONS_LIVE_KEY, _t)
                                    if _lr:
                                        _ld = json.loads(_lr.decode() if isinstance(_lr, bytes) else _lr)
                                        if _ld.get("entry_price", 0) == 0:
                                            _ld["entry_price"] = actual_fill
                                            _ld["high_water_mark"] = max(actual_fill, _ld.get("high_water_mark") or 0)
                                            await redis.hset(_POSITIONS_LIVE_KEY, _t, json.dumps(_ld))
                                    # Publish DB correction event (corrects entry_price=0 if params existed)
                                    await _publish_execution_event(redis, "position_entry_updated", {
                                        "ticker": _t,
                                        "entry_price": actual_fill,
                                        "opened_at": _oa,
                                        "order_id": _oid,
                                    })
                                    # Remove from inflight ledger
                                    await redis.hdel(_INFLIGHT_ENTRY_KEY, _oid)
                                except Exception as _cb_exc:
                                    logger.warning("[EXEC] Error in entry fill callback for %s: %s", _t, _cb_exc)

                                engine.register_order_fill_callback(_entry_order_id, _on_entry_fill_async)  # type: ignore[union-attr]
                    else:
                        ORDERS_PLACED.labels(ticker=signal.ticker, status="rejected").inc()
                        logger.warning("[EXEC] Rejected %s: %s", signal.ticker, result.error)

                    await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)

                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error(
                        "[TRADE LOOP] Error processing msg %s — acking to avoid redelivery: %s",
                        msg_id, exc, exc_info=True,
                    )
                    await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)

            if not messages:
                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("[TRADE LOOP] Unhandled error — will retry: %s", exc, exc_info=True)
            await asyncio.sleep(5.0)


async def run_exit_loop(
    engine: ExecutionEngine,
    exit_manager: PositionExitManager,
    market_data: MarketDataProvider,
    breaker: CircuitBreaker,
    redis: aioredis.Redis,
    mode: str = "live",
) -> None:
    """
    Every poll_interval seconds:
      1. Guard: skip cycle if engine is disconnected
      2. Refresh market data snapshots for open tickers + watchlist
      3. Update engine price cache
      4. Evaluate exit rules for every open position
      5. Close positions that triggered an exit
      6. Persist HWM + position params; write account state to Redis
    """
    logger.info("Execution price eval loop started")

    WATCHLIST_REFRESH_SECS = 300  # 5 minutes
    POSITION_FULL_SNAPSHOT_SECS = 300  # full ATR/OHLCV refresh cadence for open positions
    _watchlist_last_refresh: dict[str, float] = {}
    _position_last_full_snapshot: dict[str, float] = {}
    # Tracks the last trailing_stop_pct applied per ticker (for change detection).
    # Seeded from persisted position_params so restarts don't abruptly tighten trails.
    _trailing_pct_applied: dict[str, float] = {}
    if hasattr(engine, "get_position_params"):
        for ticker, params in engine.get_position_params().items():  # type: ignore[union-attr]
            applied = params.get("trailing_stop_pct_applied")
            if applied is not None:
                try:
                    _trailing_pct_applied[ticker] = float(applied)
                except (ValueError, TypeError):
                    pass

    # Tickers with a close order submitted this session but fill not yet confirmed.
    # Prevents re-evaluation until run_reconcile_loop detects the fill next cycle.
    # Ephemeral (not persisted) — cleared by reconcile when closed_offline is confirmed.
    _closing_tickers: set[str] = set()
    _IB_CACHE_REFRESH_SECS = 300  # 5 minutes
    _last_ib_cache_refresh: float = 0.0

    # Record loop startup time so naked-position check can apply a grace period
    # while IB openOrder callbacks are still arriving after connect.
    _loop_start_ts: float = asyncio.get_event_loop().time()

    while True:
        try:
            cfg = await SystemConfig.load(redis)

            # ── 1. Connection guard ───────────────────────────────────────────
            _connected = await engine.health_check()

            if not _connected:
                # The persistent reconnect watcher (_run_ib_reconnect_watcher) is
                # responsible for detecting disconnects and running full reconcile.
                # This loop simply waits — the watcher will replace this task with a
                # fresh exit loop instance once the new IB connection is established.
                #
                # While disconnected, refresh positions:live from position:params so
                # the UI always shows current positions even when IB is offline.
                # Without this, positions:live would silently expire (its TTL was
                # refreshed only by this loop) and the positions panel would go blank.
                logger.warning(
                    "[SYNC] IB disconnected — waiting for reconnect watcher to restore connection"
                )
                try:
                    params_raw = await redis.hgetall(_POSITION_PARAMS_KEY)
                    if params_raw and not await redis.exists(_POSITIONS_LIVE_KEY):
                        pipe = redis.pipeline()
                        _tmp = _POSITIONS_LIVE_KEY + ":tmp"
                        pipe.delete(_tmp)
                        for _k, _v in params_raw.items():
                            _ticker = _k.decode() if isinstance(_k, bytes) else _k
                            try:
                                _p = json.loads(_v.decode() if isinstance(_v, bytes) else _v)
                                if _p.get("source", "system") == "system":
                                    pipe.hset(_tmp, _ticker, json.dumps({
                                        "ticker": _ticker,
                                        "direction": _p.get("direction", "LONG"),
                                        "shares": int(_p.get("shares", 0)),
                                        "entry_price": float(_p.get("entry_price", 0.0)),
                                        "stop_loss": float(_p.get("stop_loss", 0.0)),
                                        "take_profit": float(_p.get("take_profit", 0.0)),
                                        "unrealized_pnl": None,
                                        "high_water_mark": None,
                                        "opened_at": _p.get("opened_at"),
                                        "source": "system",
                                        "ib_disconnected": True,
                                    }))
                            except Exception:
                                pass
                        await pipe.execute()
                        if await redis.exists(_tmp):
                            await redis.rename(_tmp, _POSITIONS_LIVE_KEY)
                            logger.info(
                                "[SYNC] positions:live rebuilt from position:params (%d ticker(s)) while IB disconnected",
                                len(params_raw),
                            )
                except Exception as _rebuild_exc:
                    logger.debug("[SYNC] positions:live rebuild from params failed: %s", _rebuild_exc)
                await asyncio.sleep(cfg.signal_poll_interval_sec)
                continue

            # ── 1b. Periodic IB position cache refresh ────────────────────────
            # Force reqPositionsAsync() every 5 minutes so the local cache stays
            # current even if fill events were missed during a reconnect window.
            now_ts = asyncio.get_event_loop().time()
            if hasattr(engine, "_ib") and (now_ts - _last_ib_cache_refresh) > _IB_CACHE_REFRESH_SECS:
                try:
                    await engine._ib.reqPositionsAsync()  # type: ignore[union-attr]
                    _last_ib_cache_refresh = now_ts
                    logger.debug("[SYNC] IB position cache refreshed")
                except Exception as exc:
                    logger.debug("[SYNC] IB position cache refresh failed: %s", exc)

            # Fetch VIX once per cycle (shared across all ticker snapshots)
            vix = await market_data.get_vix()
            await redis.set("market:vix", str(vix))

            # ── 2. Refresh market data ────────────────────────────────────────
            # Only track market data for system-managed positions.
            all_ib_positions = await engine.get_positions()
            _sys_params = engine.get_position_params() if hasattr(engine, "get_position_params") else {}  # type: ignore[union-attr]
            open_positions = [p for p in all_ib_positions if p.ticker in _sys_params]
            open_tickers = {p.ticker for p in open_positions}

            watchlist_raw = await redis.zrange("watchlist:active", 0, -1)
            watchlist_tickers = {
                (t.decode() if isinstance(t, bytes) else t) for t in watchlist_raw
            }
            now_ts = datetime.now(UTC).timestamp()
            stale_watchlist = {
                t for t in watchlist_tickers
                if now_ts - _watchlist_last_refresh.get(t, 0) >= WATCHLIST_REFRESH_SECS
            }

            # ── 2a. Open position prices: portfolio feed first, then batch IB ──
            # get_portfolio_prices() reads ib.portfolio() — a streaming account
            # subscription pushed by IB every few seconds.  It requires NO market
            # data subscription and is always available for positions in the account.
            # This avoids error 10089 (subscription required) for the common case
            # where live market data API access hasn't been separately enabled.
            # Tickers not covered by the portfolio feed fall back to
            # get_market_prices() then yfinance.
            portfolio_prices: dict[str, float] = {}
            if open_tickers and hasattr(engine, "get_portfolio_prices"):
                try:
                    portfolio_prices = engine.get_portfolio_prices()  # type: ignore[union-attr]
                except Exception as exc:
                    logger.debug("Portfolio price fetch failed: %s", exc)

            ib_prices: dict[str, float] = {}
            remaining_tickers = open_tickers - set(portfolio_prices)
            if remaining_tickers:
                try:
                    ib_prices = await engine.get_market_prices(list(remaining_tickers))
                except Exception as exc:
                    logger.debug("IB batch price fetch failed: %s", exc)

            for ticker in open_tickers:
                price = portfolio_prices.get(ticker) or ib_prices.get(ticker)
                if price:
                    # IB returned a live price — update engine cache and Redis
                    engine.set_price(ticker, price)
                    source = "ib_portfolio" if ticker in portfolio_prices else "ib"
                    try:
                        await redis.hset(f"market_data:{ticker}", mapping={
                            "last": str(price),
                            "updated_at": datetime.now(UTC).isoformat(),
                            "source": source,
                        })
                        await redis.expire(f"market_data:{ticker}", 4 * 3600)
                    except Exception:
                        pass
                else:
                    # IB gave no price (e.g. outside hours, no subscription) —
                    # fall back to full yfinance snapshot for this ticker.
                    logger.debug(
                        "[SYNC] IB price unavailable for %s — using yfinance fallback",
                        ticker,
                    )
                    try:
                        snapshot = await _write_market_snapshot_and_get_price(
                            redis, ticker, market_data, vix=vix
                        )
                        if snapshot is not None:
                            engine.set_price(ticker, snapshot)
                    except Exception as exc:
                        logger.debug("yfinance fallback failed for %s: %s", ticker, exc)

                # Full snapshot (ATR, OHLCV, momentum) for open positions runs
                # every POSITION_FULL_SNAPSHOT_SECS so signal generation and
                # startup reconciliation always have fresh ATR data.
                if now_ts - _position_last_full_snapshot.get(ticker, 0) >= POSITION_FULL_SNAPSHOT_SECS:
                    try:
                        await _write_market_snapshot_and_get_price(
                            redis, ticker, market_data, vix=vix
                        )
                        _position_last_full_snapshot[ticker] = now_ts
                    except Exception as exc:
                        logger.debug("Full snapshot failed for %s: %s", ticker, exc)

            # ── 2b. Watchlist prices: full yfinance snapshot ──────────────────
            # Watchlist tickers are not open positions; use yfinance (or IB
            # FallbackMarketData) for their full snapshot on the slow cadence.
            for ticker in stale_watchlist - open_tickers:
                try:
                    snapshot = await _write_market_snapshot_and_get_price(
                        redis, ticker, market_data, vix=vix
                    )
                    if snapshot is not None:
                        engine.set_price(ticker, snapshot)
                    _watchlist_last_refresh[ticker] = now_ts
                except Exception as exc:
                    logger.debug("Watchlist price refresh failed for %s: %s", ticker, exc)

            # ── 3. Evaluate exit rules ────────────────────────────────────────
            # Re-fetch positions after price updates (HWM may have moved).
            # Write positions:live NOW (before any exits) so the UI always shows
            # accurate sl/tp values — clearing params after a close would make
            # sl/tp appear as 0 if we write after the evaluation loop.
            all_pos = await engine.get_positions()
            # Only manage positions opened by this system; manual IB positions
            # are managed by the user elsewhere and must not be touched.
            system_params = engine.get_position_params() if hasattr(engine, "get_position_params") else {}  # type: ignore[union-attr]
            open_positions = [p for p in all_pos if p.ticker in system_params]
            await _write_positions_to_redis(redis, engine, pending_close=_closing_tickers)
            now = datetime.now(UTC)
            just_closed: set[str] = set()

            # ── 3a. Naked position check ──────────────────────────────────────
            # Detect positions with no live server-side STP/TRAIL orders.
            # Attempt reattach; fall back to immediate close if that fails.
            # Tickers handled here are excluded from this cycle's exit evaluation
            # to avoid race conditions with just-placed orders.
            naked_closed, naked_reattached = await _check_naked_positions(
                redis, engine, open_positions, system_params, cfg, mode,
                startup_ts=_loop_start_ts,
            )
            if naked_closed:
                just_closed.update(naked_closed)
                await _write_positions_to_redis(redis, engine, pending_close=_closing_tickers)
            handled_this_cycle = naked_closed | naked_reattached
            open_positions_for_eval = [
                p for p in open_positions if p.ticker not in handled_this_cycle
            ]

            for pos in open_positions_for_eval:
                # Skip any position whose close order was submitted last cycle
                # but not yet fill-confirmed — don't re-evaluate or re-close it.
                if pos.ticker in _closing_tickers:
                    logger.debug("[EXIT] %s close order pending fill — skipping exit evaluation", pos.ticker)
                    continue
                current_price = engine.get_price(pos.ticker) or pos.entry_price
                sentiment, mention_ratio = await _get_sentiment_context(redis, pos.ticker, cfg=cfg)

                # ── Mention-decay trailing stop tightening (Rule 6) ───────────
                hours_held = (now - pos.opened_at.replace(tzinfo=UTC)).total_seconds() / 3600
                effective_pct = _effective_trailing_pct(mention_ratio, hours_held, cfg)
                last_applied = _trailing_pct_applied.get(pos.ticker, cfg.trailing_stop_pct)

                # Build effective cfg with tightened trailing stop for exit manager.
                # When decaying, also clear the activation gate so the tightened
                # trail fires unconditionally without requiring a profit threshold.
                decaying = mention_ratio < cfg.mention_decay_threshold
                if abs(effective_pct - cfg.trailing_stop_pct) > 1e-9:
                    effective_cfg = dataclasses.replace(
                        cfg,
                        trailing_stop_pct=effective_pct,
                        trailing_stop_activation_pct=(
                            0.0 if decaying else cfg.trailing_stop_activation_pct
                        ),
                    )
                else:
                    effective_cfg = cfg

                decision = exit_manager.evaluate(
                    pos, current_price, effective_cfg,
                    current_sentiment=sentiment,
                    mention_ratio=mention_ratio,
                    now=now,
                )
                if decision.should_exit:
                    # Capture position metadata before close_position may clear state.
                    _pos_opened_at = pos.opened_at.isoformat() if pos.opened_at else ""
                    _pos_entry_price = pos.entry_price
                    _pos_shares = pos.shares
                    _pos_direction = pos.direction

                    close_result = await engine.close_position(pos.ticker, reason=decision.reason)

                    if close_result and close_result.status == "rejected":
                        # Order rejected (e.g. no IB position found) — log and skip.
                        # Position stays tracked; loop will retry next cycle.
                        logger.warning(
                            "[EXIT] close_position for %s rejected: %s — position stays open",
                            pos.ticker, close_result.error or "unknown reason",
                        )
                        continue

                    if close_result and close_result.fill_price is not None:
                        # ── Immediate fill confirmed (within 0.5s) ────────────────
                        # Clean up tracking right away — we have the actual exit price.
                        just_closed.add(pos.ticker)
                        if hasattr(engine, "forget_position"):
                            engine.forget_position(pos.ticker)  # type: ignore[union-attr]
                        _trailing_pct_applied.pop(pos.ticker, None)

                        await _publish_execution_event(redis, "position_closed", {
                            "ticker": pos.ticker,
                            "exit_price": close_result.fill_price,
                            "exit_reason": decision.reason or "unknown",
                            "shares": _pos_shares,
                            "direction": _pos_direction,
                            "entry_price": _pos_entry_price,
                            "closed_at": datetime.now(UTC).isoformat(),
                            "opened_at": _pos_opened_at,
                            "mode": mode,
                        })
                        await redis.hdel(_HWM_REDIS_KEY, pos.ticker)
                        await redis.hdel(_POSITION_PARAMS_KEY, pos.ticker)
                        await redis.hdel(_TRAIL_ORDERS_KEY, pos.ticker)
                        POSITIONS_CLOSED.labels(reason=decision.reason or "unknown").inc()
                        logger.info(
                            "[EXIT] %s %s reason=%s fill=%.4f pnl_approx=%.2f",
                            _pos_direction, pos.ticker, decision.reason,
                            close_result.fill_price,
                            (close_result.fill_price - _pos_entry_price) * _pos_shares
                            if _pos_direction == "LONG"
                            else (_pos_entry_price - close_result.fill_price) * _pos_shares,
                        )
                    else:
                        # ── Close order submitted but fill NOT yet confirmed ───────
                        # Add to _closing_tickers to prevent re-evaluation next cycle.
                        # run_reconcile_loop will detect the fill via closed_offline
                        # state on the next 60-second reconcile cycle.
                        _closing_tickers.add(pos.ticker)
                        _trailing_pct_applied.pop(pos.ticker, None)
                        logger.info(
                            "[EXIT] %s close order submitted — awaiting fill; reconcile will confirm",
                            pos.ticker,
                        )
                else:
                    # Position is holding — update IB TRAIL order if tightening changed.
                    # Done AFTER exit evaluation to avoid placing a TRAIL order that
                    # would immediately fire and create a duplicate market order alongside
                    # close_position().
                    if abs(effective_pct - last_applied) >= 0.005:  # 0.5% noise gate
                        if hasattr(engine, "update_trailing_stop"):
                            await engine.update_trailing_stop(pos.ticker, effective_pct)
                        _trailing_pct_applied[pos.ticker] = effective_pct
                        logger.info(
                            "[TRAIL] %s tightened trailing stop %.1f%% → %.1f%% "
                            "(mention_ratio=%.2f)",
                            pos.ticker, last_applied * 100, effective_pct * 100, mention_ratio,
                        )

            # ── 4. Persist state + metrics ────────────────────────────────────
            await _persist_hwm_to_redis(redis, engine)
            await _persist_position_params_to_redis(redis, engine)
            await _persist_trail_orders_to_redis(redis, engine)
            await _write_account_state(redis, engine)

            state = await engine.get_account_state()
            PAPER_EQUITY.set(state.net_liquidation)
            DAILY_PNL_PCT.set(
                state.daily_pnl / state.net_liquidation if state.net_liquidation else 0
            )
            DRAWDOWN.set(state.drawdown_pct)
            remaining_positions = await engine.get_positions()
            OPEN_POSITIONS_COUNT.set(len(remaining_positions))
            for pos in remaining_positions:
                cur = engine.get_price(pos.ticker) or pos.entry_price
                pnl = (cur - pos.entry_price) * pos.shares if pos.direction == "LONG" \
                    else (pos.entry_price - cur) * pos.shares
                POSITION_PNL.labels(ticker=pos.ticker, direction=pos.direction).set(pnl)

            # ── 6. EOD snapshot ───────────────────────────────────────────────
            # Save session metrics once per day after NYSE close (≥ 16:00 ET).
            # Guard with a Redis key so restarts don't duplicate the write.
            # Window: market closed AND hour ≥ 20 UTC (≥ 16:00 ET / 15:00 CT)
            # — deliberately wide so we don't miss it even if the loop was slow.
            _eod_key = f"eod_snapshot_done:{datetime.now(UTC).date().isoformat()}:{mode}"
            if not _NYSE.is_open() and not await redis.exists(_eod_key):
                now_dt = datetime.now(UTC)
                # Avoid pre-market false trigger: only after 20:00 UTC (16:00 ET)
                if now_dt.hour >= 20:
                    await _save_eod_snapshot(cfg, mode)
                    await _prune_old_data()
                    await redis.setex(_eod_key, 86400, "1")  # expire after 24h

        except asyncio.CancelledError:
            raise  # let the task be cancelled normally on shutdown
        except Exception as exc:
            logger.error("[EXIT LOOP] Unhandled error — will retry next cycle: %s", exc, exc_info=True)

        await asyncio.sleep(cfg.signal_poll_interval_sec)  # type: ignore[possibly-undefined]


# Semantic alias — run_exit_loop is the price-evaluation + external-close detection
# loop.  The reconcile refactor (Phase 3) will fold the remaining logic and rename
# this function; until then, the alias keeps external references consistent.
run_price_eval_loop = run_exit_loop


async def run_reconcile_loop(
    engine: ExecutionEngine,
    redis: aioredis.Redis,
    mode: str = "live",
    ib_account: str = "",
    interval_secs: int = 60,
) -> None:
    """
    Periodic IB-state reconcile loop.  Runs every ``interval_secs`` (default 60 s)
    whenever IB is connected.

    Calls ``_reconcile_ib_state()`` on every iteration, which:
      - Classifies all positions into one of 9 states (matched, adopted, etc.)
      - Auto-resolves safe transitions immediately
      - Writes ``reconcile:conflicts`` (hash) for unresolvable issues
      - Writes ``reconcile:full`` JSON snapshot for the UI
      - Writes ``reconcile:last_run`` timestamp

    If IB is disconnected the cycle is silently skipped — app state is frozen until
    the reconnect watcher brings IB back up and the loop resumes.

    Non-empty ``reconcile:conflicts`` halts the trade loop (new entries) but does
    NOT stop this loop or the price-eval loop (exits always continue).
    """
    logger.info("Execution reconcile loop started (interval=%ds)", interval_secs)
    while True:
        try:
            cfg = await SystemConfig.load(redis)
            summary = await _reconcile_ib_state(
                redis, engine, mode=mode, ib_account=ib_account, cfg=cfg,
            )
            if summary.get("skipped"):
                logger.debug("[RECONCILE] Skipped: %s", summary.get("reason"))
            else:
                conflict_tickers = summary.get("conflicts", [])
                if conflict_tickers:
                    logger.warning(
                        "[RECONCILE] %d conflict(s) — trade loop halted: %s",
                        len(conflict_tickers), ", ".join(str(t) for t in conflict_tickers),
                    )
                else:
                    logger.info(
                        "[RECONCILE] OK — matched=%d adopted=%d closed_offline=%d "
                        "fill_fixed=%d naked_flagged=%d",
                        summary.get("matched", 0),
                        summary.get("adopted", 0),
                        summary.get("closed_offline", 0),
                        summary.get("fill_pending_fixed", 0),
                        summary.get("naked_flagged", 0),
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("[RECONCILE] Unhandled error: %s", exc, exc_info=True)
        await asyncio.sleep(interval_secs)


async def _get_sentiment_context(
    redis: aioredis.Redis,
    ticker: str,
    window_secs: float = 3600.0,
    cfg: "SystemConfig | None" = None,
) -> tuple[float, float]:
    """
    Read current sentiment score and mention ratio for a ticker from Redis.

    Returns:
        (current_sentiment, mention_ratio)
        current_sentiment: engagement-weighted avg score ∈ [-1, 1], 0.0 if no data
        mention_ratio: smoothed_recent_mentions / peak_mentions, 1.0 if no data
    """
    import json as _json  # noqa: PLC0415
    import time as _time  # noqa: PLC0415

    current_sentiment = 0.0
    mention_ratio = 1.0
    smooth_samples = cfg.mention_decay_smooth_samples if cfg is not None else 3

    try:
        # ── Sentiment: average score of posts in last window_secs ─────────────
        key = f"sentiment:window:{ticker}"
        cutoff = _time.time() - window_secs
        raw_entries = await redis.zrangebyscore(key, cutoff, "+inf")
        if raw_entries:
            scores = []
            for entry in raw_entries:
                try:
                    data = _json.loads(entry)
                    s = float(data.get("score", 0.0))
                    scores.append(s)
                except Exception:
                    pass
            if scores:
                current_sentiment = sum(scores) / len(scores)
    except Exception:
        pass

    try:
        # ── Mention ratio: max(per-source smoothed/peak) ──────────────────────
        # Only Tier-1 sources (Bluesky, StockTwits) are used here. Twitter is
        # excluded because it is only polled on Phase-1 events — after entry its
        # history receives no new samples, so smoothed_current ≈ peak ≈ 1.0
        # permanently, which would block decay exits indefinitely with max().
        source_ratios: list[float] = []
        for source in MENTION_HISTORY_TIER1_SOURCES:
            raw_history = await redis.lrange(f"mention_history:{source}:{ticker}", 0, -1)
            if not raw_history:
                continue
            counts = [float(v) for v in raw_history]
            peak = max(counts)
            if peak <= 2:  # ignore noise-floor sources (< 3 posts ever seen)
                continue
            recent = counts[-smooth_samples:]
            smoothed_current = sum(recent) / len(recent)
            source_ratios.append(smoothed_current / peak)

        if source_ratios:
            mention_ratio = max(source_ratios)
    except Exception:
        pass

    return current_sentiment, mention_ratio


def _effective_trailing_pct(
    mention_ratio: float,
    hours_held: float,
    cfg: SystemConfig,
) -> float:
    """
    Compute the effective trailing stop percentage for a position based on mention decay.

    Design §6b Rule 6: linearly interpolates trailing_stop_pct down to
    trailing_stop_min_pct as mentions decay from peak to threshold.

    Formula:
        t = clamp((mention_ratio - threshold) / (1 - threshold), 0, 1)
        effective_pct = min_pct + t * (max_pct - min_pct)

    Only applied after mention_decay_min_hold_hours to avoid false tightening
    from the natural decay of the entry spike in the first poll window.

    Returns cfg.trailing_stop_pct (no tightening) when not yet past the hold gate.
    """
    if hours_held < cfg.mention_decay_min_hold_hours:
        return cfg.trailing_stop_pct

    threshold = cfg.mention_decay_threshold
    max_pct = cfg.trailing_stop_pct
    min_pct = cfg.trailing_stop_min_pct

    if threshold >= 1.0:
        return max_pct  # degenerate config — avoid divide-by-zero

    t = (mention_ratio - threshold) / (1.0 - threshold)
    t = max(0.0, min(1.0, t))  # clamp to [0, 1]
    return min_pct + t * (max_pct - min_pct)


async def _write_market_snapshot_and_get_price(
    redis: aioredis.Redis,
    ticker: str,
    market_data: MarketDataProvider,
    vix: float = 20.0,
) -> float | None:
    """Fetch snapshot via IB (primary) → yfinance (fallback), write to Redis, return last price."""
    try:
        quote = await market_data.get_quote(ticker)
        last = quote.get("last", 0.0)
        if last > 0:
            atr = await market_data.get_atr(ticker)
            realised_vol = await market_data.get_realised_vol(ticker)

            key = f"market_data:{ticker}"

            # Always write price-level fields
            mapping: dict[str, str] = {
                "last": str(last),
                "bid": str(quote.get("bid", last * 0.999)),
                "ask": str(quote.get("ask", last * 1.001)),
                "atr_14": str(atr),
                "realised_vol": str(realised_vol),
                "vix": str(vix),
                "updated_at": datetime.now(UTC).isoformat(),
            }

            # Only write ADV when the provider returned a real value.
            # Outside trading hours IB returns NaN→0 and yfinance may return
            # None→0; writing 0 overwrites the last known good value in Redis
            # and causes the liquidity gate to reject every signal.
            adv_shares = quote.get("avg_volume_30d") or 0.0
            if adv_shares > 0:
                mapping["adv_shares"] = str(adv_shares)
                mapping["adv_usd"] = str(adv_shares * last)
            else:
                logger.debug("ADV not available for %s — keeping previous Redis value", ticker)

            # Same treatment for market cap (IB always returns 0.0)
            market_cap = quote.get("market_cap") or 0.0
            if market_cap > 0:
                mapping["market_cap_usd"] = str(market_cap)

            # Price momentum: intraday return (today's open → current price).
            # Captures the move during the *current trading session*, which is
            # directly contemporaneous with the social mention window.  Using
            # yesterday's close would include overnight gaps unrelated to today's
            # social activity.  Uses IB primary → yfinance fallback.
            # Field omitted (= 0.0 neutral) when open price unavailable.
            try:
                ohlcv = await market_data.get_ohlcv(ticker, period="1d", interval="1d")
                if ohlcv and ohlcv[0].get("open", 0) > 0:
                    mom = (last - ohlcv[0]["open"]) / ohlcv[0]["open"]
                    mapping["momentum"] = str(round(mom, 6))
            except Exception as _m_exc:
                logger.debug("momentum fetch failed for %s: %s", ticker, _m_exc)

            await redis.hset(key, mapping=mapping)

            # Clean up any stale zero-value ADV/market-cap fields that may have been
            # written by an older code version.  hset() never removes fields so a
            # legacy "adv_usd=0" persists indefinitely unless explicitly deleted.
            # Only delete when we did NOT write a fresh value (if we did write it,
            # it is already correct and the hdel would be redundant).
            stale_fields: list[str] = []
            if "adv_shares" not in mapping:
                existing_adv = await redis.hget(key, "adv_shares") or await redis.hget(key, b"adv_shares")
                if existing_adv is not None and float(existing_adv) == 0:
                    stale_fields.extend(["adv_shares", "adv_usd"])
            if "market_cap_usd" not in mapping:
                existing_mc = await redis.hget(key, "market_cap_usd") or await redis.hget(key, b"market_cap_usd")
                if existing_mc is not None and float(existing_mc) == 0:
                    stale_fields.append("market_cap_usd")
            if stale_fields:
                await redis.hdel(key, *stale_fields)
                logger.debug("Cleaned stale zero fields %s from market_data:%s", stale_fields, ticker)

            # Expire after 4 hours — prevents stale tickers accumulating as
            # the watchlist rotates.  Active tickers are refreshed every cycle
            # so they never actually expire while in the watchlist.
            await redis.expire(key, 4 * 3600)
            return float(last)
    except Exception as exc:
        logger.debug("Snapshot failed for %s: %s", ticker, exc)
    return None


_HWM_REDIS_KEY = "hwm:all"
_POSITION_PARAMS_KEY = "position:params"
_TRAIL_ORDERS_KEY = "position:trail_orders"
_EXEC_EVENTS_STREAM = "execution:events"
_POSITIONS_LIVE_KEY = "positions:live"

# In-flight order ledger: tracks entry/exit orders submitted to IB but not yet
# fill-confirmed.  Used to recover actual fill prices after a service restart.
_INFLIGHT_ENTRY_KEY   = "orders:inflight"   # entry MKT orders awaiting fill
_INFLIGHT_EXIT_KEY    = "exits:inflight"    # close MKT orders awaiting fill
_INFLIGHT_TTL_SEC     = 86400               # 24h TTL — auto-expires if never cleared

# Command-closed set: tracks tickers closed via UI command whose fill was
# confirmed immediately.  _reconcile_external_closes excludes these tickers so
# that the exit loop does not publish a second position_closed event on the same
# position within the same cycle.  Short TTL — only needed for 1 exit-loop cycle.
_CMD_CLOSED_KEY     = "positions:cmd_closed"  # Redis set: tickers
_CMD_CLOSED_TTL_SEC = 120                     # 2 minutes — covers any exit-loop cycle

# Fill-sync alert bus: unresolved inflight entries become user-visible alerts.
# Written by _reconcile_inflight_orders; read by the Streamlit positions page.
_FILL_SYNC_ALERTS_KEY = "alerts:fill_sync"  # hash: oid → JSON alert payload
_FILL_SYNC_ALERT_TTL  = 86400               # 24h — stale alerts auto-expire

# Pending-reconcile ledger: positions that are in position:params but cannot be
# automatically reconciled against IB (no current IB position AND no fill record
# found today).  These are surfaced in the UI so the user can take manual action
# instead of the service silently discarding them.
# Each field key is the ticker; value is a JSON object with diagnostic metadata.
_PENDING_RECONCILE_KEY = "positions:pending_reconcile"  # hash: ticker → JSON
_RECONCILE_STATE_KEY = "reconcile:state"   # str: collecting|awaiting_approval|approved|skipped_no_ib
_RECONCILE_DATA_KEY = "reconcile:data"     # JSON: full reconcile payload
_RECONCILE_CONFLICTS_KEY = "reconcile:conflicts"   # hash: ticker → conflict JSON (non-empty = execution halted)
_RECONCILE_LAST_RUN_KEY  = "reconcile:last_run"    # string: ISO timestamp of last reconcile
_RECONCILE_FULL_KEY      = "reconcile:full"        # JSON: full snapshot for UI display
_RECONCILE_TTL = 7200                      # 2h TTL


def _decode_text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value) if value is not None else ""


def _parse_json_value(raw: object, default: object) -> object:
    if raw is None:
        return default
    try:
        return json.loads(_decode_text(raw))
    except Exception:
        return default


def _ib_position_to_dict(position: object) -> dict:
    contract = getattr(position, "contract", None)
    qty_raw = float(getattr(position, "position", 0) or 0)
    avg_cost = float(getattr(position, "avgCost", 0) or 0)
    market_price = float(getattr(position, "marketPrice", 0) or 0)
    unrealized = float(getattr(position, "unrealizedPNL", 0) or 0)
    direction = "LONG" if qty_raw >= 0 else "SHORT"
    return {
        "ticker": getattr(contract, "symbol", ""),
        "direction": direction,
        "shares": int(abs(qty_raw)),
        "avg_cost": avg_cost,
        "entry_price": avg_cost,
        "market_price": market_price,
        "unrealized_pnl": unrealized,
    }


def _execution_fill_to_dict(fill: object) -> dict:
    contract = getattr(fill, "contract", None)
    execution = getattr(fill, "execution", None)
    order = getattr(fill, "order", None)
    order_type = getattr(order, "orderType", "") or getattr(execution, "orderType", "")
    ref = getattr(order, "orderRef", "") or getattr(execution, "orderRef", "")
    action = getattr(execution, "side", "") or getattr(order, "action", "")
    quantity = float(getattr(execution, "shares", 0) or 0)
    price = float(getattr(execution, "price", 0) or 0)
    fill_time = getattr(fill, "time", None) or getattr(execution, "time", None)
    exit_reason = {
        "STP": "STOP_LOSS",
        "STP LMT": "STOP_LOSS",
        "LMT": "TAKE_PROFIT",
        "TRAIL": "TRAILING_STOP",
    }.get(order_type, "STARTUP_RECONCILE_CLOSED")
    return {
        "ticker": getattr(contract, "symbol", ""),
        "action": action,
        "type": order_type,
        "fill_price": price,
        "quantity": int(abs(quantity)),
        "time": str(fill_time) if fill_time else "",
        "status": "Filled",
        "ref": ref,
        "exit_reason": exit_reason,
    }


async def _persist_reconcile_snapshot(redis: aioredis.Redis, payload: dict) -> None:
    payload = dict(payload or {})
    state = payload.get("state", "awaiting_approval")
    payload["state"] = state
    await redis.set(_RECONCILE_STATE_KEY, state, ex=_RECONCILE_TTL)
    await redis.set(_RECONCILE_DATA_KEY, json.dumps(payload, default=str), ex=_RECONCILE_TTL)


async def _collect_reconcile_data(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
    ib_account: str = "",
) -> dict:
    from social_trading.execution.ibkr import ORDER_REF  # noqa: PLC0415

    reconcile_data: dict = {
        "state": "awaiting_approval",
        "collected_at": datetime.now(UTC).isoformat(),
        "ib_account": ib_account,
        "app_positions": [],
        "app_oca_orders": [],
        "ib_positions": [],
        "ib_trades_today": [],
        "matches": [],
        "pending_manual": [],
        "auto_actions": [],
    }

    app_positions: list[dict] = []
    trail_orders: dict[str, object] = {}
    try:
        raw_params = await redis.hgetall(_POSITION_PARAMS_KEY)
        raw_trails = await redis.hgetall(_TRAIL_ORDERS_KEY)
        trail_orders = {
            _decode_text(k): _parse_json_value(v, _decode_text(v))
            for k, v in (raw_trails or {}).items()
        }
        for field, value in (raw_params or {}).items():
            ticker = _decode_text(field).upper()
            params = _parse_json_value(value, {}) if value is not None else {}
            if not isinstance(params, dict):
                params = {}
            params = dict(params)
            params["ticker"] = ticker
            if ticker in trail_orders:
                params["trail_order"] = trail_orders[ticker]
            app_positions.append(params)
    except Exception as exc:
        logger.warning("[RECONCILE] Failed to read app position state: %s", exc)
    reconcile_data["app_positions"] = app_positions

    ib_obj = getattr(engine, "_ib", None)
    ib_positions: list[dict] = []
    if ib_obj is not None:
        try:
            for position in ib_obj.positions() or []:
                record = _ib_position_to_dict(position)
                if record.get("ticker"):
                    ib_positions.append(record)
        except Exception as exc:
            logger.warning("[RECONCILE] Failed to read IB positions: %s", exc)
    reconcile_data["ib_positions"] = ib_positions

    app_oca_orders: list[dict] = []
    if ib_obj is not None:
        try:
            for trade in ib_obj.openTrades() or []:
                order = getattr(trade, "order", None)
                contract = getattr(trade, "contract", None)
                if getattr(order, "orderRef", "") != ORDER_REF:
                    continue
                app_oca_orders.append({
                    "ticker": getattr(contract, "symbol", ""),
                    "order_type": getattr(order, "orderType", ""),
                    "action": getattr(order, "action", ""),
                    "oca_group": getattr(order, "ocaGroup", ""),
                    "aux_price": float(getattr(order, "auxPrice", 0) or 0),
                    "status": getattr(getattr(trade, "orderStatus", None), "status", ""),
                })
        except Exception as exc:
            logger.warning("[RECONCILE] Failed to read IB open orders: %s", exc)
    reconcile_data["app_oca_orders"] = app_oca_orders

    ib_trades_today: list[dict] = []
    if ib_obj is not None:
        try:
            from ib_async import ExecutionFilter  # noqa: PLC0415
            executions = await ib_obj.reqExecutionsAsync(ExecutionFilter())
            for fill in executions or []:
                record = _execution_fill_to_dict(fill)
                if record.get("ticker"):
                    ib_trades_today.append(record)
        except Exception as exc:
            logger.warning("[RECONCILE] Failed to read IB executions: %s", exc)
    reconcile_data["ib_trades_today"] = ib_trades_today

    app_by_ticker = {str(p.get("ticker", "")).upper(): p for p in app_positions if p.get("ticker")}
    ib_by_ticker = {str(p.get("ticker", "")).upper(): p for p in ib_positions if p.get("ticker")}
    fills_by_ticker: dict[str, dict] = {}
    # Close fills only (SLD for LONG, BOT for SHORT) — keyed by ticker.
    # Used for "closed_offline" classification and timestamp checks.
    # Storing separately from fills_by_ticker so entry fills don't mask close fills.
    _close_fills_by_ticker: dict[str, dict] = {}
    # Track which sides were seen per ticker so we can detect round-trips (BOT + SLD both today)
    _fill_sides_by_ticker: dict[str, set[str]] = {}
    # Most-recent fill time per (ticker, side) for round-trip ordering.
    # A "round-trip" is only genuine if the SLD fill is NEWER than the BOT fill.
    # When a ticker is closed and then re-entered the same day, both sides exist but
    # the BOT fill (re-entry) is more recent than the SLD fill (prior close).
    # In that case the final state is an OPEN position, not a closed one.
    _latest_fill_time: dict[str, datetime | None] = {}   # f"{ticker}:{side}" → datetime
    for fill in ib_trades_today:
        ticker = str(fill.get("ticker", "")).upper()
        if not ticker:
            continue
        fills_by_ticker[ticker] = fill
        action = str(fill.get("action", "")).upper()
        if action:
            _fill_sides_by_ticker.setdefault(ticker, set()).add(action)
            # Parse and store the fill time for ordering detection
            _ft_raw = str(fill.get("time", "") or "")
            _ft_dt: datetime | None = None
            if _ft_raw:
                try:
                    _ft_parsed = datetime.fromisoformat(_ft_raw)
                    _ft_dt = _ft_parsed if _ft_parsed.tzinfo else _ft_parsed.replace(tzinfo=UTC)
                except Exception:
                    pass
            _key = f"{ticker}:{action}"
            # Keep the LATEST fill time for each ticker:side combination
            if _ft_dt is not None:
                _prev = _latest_fill_time.get(_key)
                if _prev is None or _ft_dt > _prev:
                    _latest_fill_time[_key] = _ft_dt
            # SLD = a sell execution (close for LONG, short entry — treat as potential close)
            # BOT = a buy execution (close for SHORT, long entry)
            # We store whichever represents a close.  For the timestamp guard we use
            # the most recent close-side fill; overwrite so the newest wins.
            if action == "SLD":
                _close_fills_by_ticker[ticker] = fill
    # Tickers that have BOTH a buy and sell fill today where the CLOSE (SLD) fill is
    # more recent than the ENTRY (BOT) fill — these genuinely round-tripped.
    # If SLD < BOT (close happened before re-entry), the final state is an open
    # position; do NOT mark as round-tripped to avoid wrongly closing a live position.
    _round_tripped_today: set[str] = set()
    for t, sides in _fill_sides_by_ticker.items():
        if "BOT" not in sides or "SLD" not in sides:
            continue
        _bot_time = _latest_fill_time.get(f"{t}:BOT")
        _sld_time = _latest_fill_time.get(f"{t}:SLD")
        if _bot_time is None or _sld_time is None:
            # No timestamps available — assume NOT round-tripped (conservative)
            logger.debug("[RECONCILE] %s: BOT+SLD fills but no timestamps — not marking as round-tripped", t)
            continue
        if _sld_time > _bot_time:
            # Close happened after entry → genuine round-trip
            _round_tripped_today.add(t)
            logger.debug("[RECONCILE] %s: round-tripped (SLD %s > BOT %s)", t, _sld_time, _bot_time)
        else:
            # Entry is more recent than close → re-entry after earlier close; position is still open
            logger.info(
                "[RECONCILE] %s: BOT+SLD fills today but BOT (%s) is more recent than SLD (%s) — "
                "position re-entered after close; NOT marking as round-tripped",
                t, _bot_time, _sld_time,
            )
    system_order_tickers = {
        str(o.get("ticker", "")).upper()
        for o in app_oca_orders
        if o.get("ticker")
    }

    # Extend system_order_tickers with tickers that had a system MKT entry order
    # filled in the current TWS session (reqCompletedOrdersAsync).  This catches
    # positions opened by this system whose OCA orders are fully filled/cancelled —
    # openTrades() shows nothing for them, so without this check they are
    # misclassified as "manual_ib" instead of "adopted".
    if ib_obj is not None:
        try:
            completed = await ib_obj.reqCompletedOrdersAsync(apiOnly=False)
            for order_state in (completed or []):
                sym = getattr(getattr(order_state, "contract", None), "symbol", "")
                ref = getattr(getattr(order_state, "order", None), "orderRef", "")
                ot  = getattr(getattr(order_state, "order", None), "orderType", "")
                st  = getattr(getattr(order_state, "orderStatus", None), "status", "")
                if sym and ref == "social_trading" and st == "Filled" and ot == "MKT":
                    system_order_tickers.add(sym.upper())
        except Exception as _coe:
            logger.debug("[RECONCILE] reqCompletedOrders unavailable for system_order check: %s", _coe)

    matches: list[dict] = []
    pending_manual: list[dict] = []
    auto_actions: list[dict] = []
    for ticker in sorted(set(app_by_ticker) | set(ib_by_ticker)):
        app_entry = app_by_ticker.get(ticker)
        ib_entry = ib_by_ticker.get(ticker)
        fill_entry = fills_by_ticker.get(ticker)
        if app_entry and ib_entry:
            app_direction = str(app_entry.get("direction", "")).upper()
            ib_direction = str(ib_entry.get("direction", "")).upper()
            app_shares = int(float(app_entry.get("shares", 0) or 0))
            ib_shares = int(float(ib_entry.get("shares", 0) or 0))
            # IB is the source of truth for open positions.  When IB reports a non-zero
            # position matching the app record, the position IS open regardless of fill
            # history.  Do NOT let the round-trip detection override a live IB position:
            # a re-entry on the same day produces both BOT and SLD fills but the IB
            # position reflects the current (new) entry, not a residual of the closed one.
            if app_direction == ib_direction and app_shares == ib_shares:
                status = "matched"
                reason = f"App and IB both show {ticker} {app_direction} {app_shares} shares."
            else:
                status = "shares_mismatch"
                reason = (
                    f"App tracks {app_direction or '?'} {app_shares} shares, "
                    f"IB shows {ib_direction or '?'} {ib_shares} shares."
                )
        elif app_entry and not ib_entry:
            if fill_entry:
                # Guard: if the fill time predates the position's opened_at, the fill
                # belongs to a PRIOR position (same ticker re-entered after a close).
                # Don't classify this new position as "closed_offline" — the fill is stale.
                # Use the close-side fill (SLD for LONG) specifically so an entry fill
                # (BOT) from a re-entry doesn't mask the stale close fill check.
                _close_fill = _close_fills_by_ticker.get(ticker, fill_entry)
                _fill_time_str = str(_close_fill.get("time", "") or "")
                _app_opened_at = str(app_entry.get("opened_at", "") or "")
                _fill_predates_open = False
                if _fill_time_str and _app_opened_at:
                    try:
                        from datetime import timezone as _tz  # noqa: PLC0415
                        _ft = datetime.fromisoformat(_fill_time_str)
                        _oa = datetime.fromisoformat(_app_opened_at)
                        if _ft.tzinfo is None:
                            _ft = _ft.replace(tzinfo=_tz.utc)
                        if _oa.tzinfo is None:
                            _oa = _oa.replace(tzinfo=_tz.utc)
                        if _ft < _oa:
                            _fill_predates_open = True
                    except Exception:
                        pass
                if _fill_predates_open:
                    status = "pending_manual"
                    reason = (
                        f"{ticker} is tracked by the app but the only IB fill record "
                        f"({_fill_time_str}) predates the position's opened_at ({_app_opened_at}). "
                        "This fill is likely from a prior same-day trade. Manual review required."
                    )
                else:
                    status = "closed_offline"
                    reason = f"{ticker} is missing from IB positions but has a same-day IB fill record."
                    auto_actions.append({"ticker": ticker, "action": "confirm_closed"})
            else:
                status = "pending_manual"
                reason = f"{ticker} is tracked by the app but no IB position or same-day fill was found."
        elif ib_entry and not app_entry:
            if ticker in system_order_tickers:
                status = "adopted"
                reason = f"{ticker} exists in IB and has system-managed orders; it will be adopted."
                auto_actions.append({"ticker": ticker, "action": "adopt_position"})
            else:
                status = "manual_ib"
                reason = f"{ticker} exists in IB without app state or system order markers."
        else:
            continue

        match = {
            "ticker": ticker,
            "status": status,
            "app": app_entry,
            "ib": ib_entry,
            "fill": fill_entry,
            "reason": reason,
        }
        matches.append(match)
        if status == "pending_manual":
            pending_manual.append(match)

    reconcile_data["matches"] = matches
    reconcile_data["pending_manual"] = pending_manual
    reconcile_data["auto_actions"] = auto_actions
    return reconcile_data


async def _reconcile_ib_state(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
    mode: str = "live",
    ib_account: str = "",
    cfg: "SystemConfig | None" = None,
) -> dict:
    if cfg is None:
        cfg = await SystemConfig.load(redis)
    ib = getattr(engine, "_ib", None)
    if ib is None or not ib.isConnected():
        return {"skipped": True, "reason": "IB not connected"}

    from ib_async import ExecutionFilter  # noqa: PLC0415
    from social_trading.execution.ibkr import ORDER_REF  # noqa: PLC0415

    def _to_float(value: object, default: float = 0.0) -> float:
        try:
            return float(value or 0)
        except Exception:
            return default

    def _to_int(value: object, default: int = 0) -> int:
        try:
            return int(float(value or 0))
        except Exception:
            return default

    def _as_dt(value: object) -> datetime | None:
        raw = str(value or "")
        if not raw:
            return None
        # Normalise IB's double-space format: "20250617  09:31:00" → "20250617 09:31:00"
        raw = " ".join(raw.split())
        try:
            parsed = datetime.fromisoformat(raw)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except Exception:
            pass
        # Handle IB's compact YYYYMMDD HH:MM:SS format after normalisation
        try:
            import re as _re  # noqa: PLC0415
            m = _re.match(r"^(\d{4})(\d{2})(\d{2})\s+(\d{2}):(\d{2}):(\d{2})$", raw)
            if m:
                return datetime(
                    int(m.group(1)), int(m.group(2)), int(m.group(3)),
                    int(m.group(4)), int(m.group(5)), int(m.group(6)),
                    tzinfo=UTC,
                )
        except Exception:
            pass
        return None

    def _update_latest_fill(
        store: dict[str, dict] | dict[str, dict[str, dict]],
        key: str,
        fill: dict,
        *,
        side: str | None = None,
    ) -> None:
        if side is None:
            existing = store.get(key)  # type: ignore[arg-type]
        else:
            store.setdefault(key, {})  # type: ignore[arg-type]
            existing = store[key].get(side)  # type: ignore[index]
        if existing is None:
            if side is None:
                store[key] = fill  # type: ignore[index]
            else:
                store[key][side] = fill  # type: ignore[index]
            return
        new_dt = _as_dt(fill.get("time"))
        old_dt = _as_dt(existing.get("time"))
        if old_dt is None or (new_dt is not None and new_dt >= old_dt):
            if side is None:
                store[key] = fill  # type: ignore[index]
            else:
                store[key][side] = fill  # type: ignore[index]

    def _entry_fill_price(ticker: str, direction: str) -> float:
        entry_side = "BOT" if direction == "LONG" else "SLD"
        if completed_by_ticker.get(ticker, 0.0) > 0:
            return completed_by_ticker[ticker]
        vwap = fill_vwap_by_ticker.get(ticker, {}).get(entry_side, 0.0)
        if vwap > 0:
            return vwap
        fill = fills_by_ticker_side.get(ticker, {}).get(entry_side) or fills_by_ticker.get(ticker)
        if fill and str(fill.get("action", "")).upper() == entry_side:
            return _to_float(fill.get("fill_price"))
        return 0.0

    def _close_fill_for_ticker(ticker: str, direction: str) -> dict | None:
        close_side = "SLD" if direction == "LONG" else "BOT"
        return (
            fills_by_ticker_side.get(ticker, {}).get(close_side)
            or _close_fills_by_ticker.get(ticker)
            or fills_by_ticker.get(ticker)
        )

    async def _persist_params(ticker: str, payload: dict) -> None:
        await redis.hset(_POSITION_PARAMS_KEY, ticker, json.dumps(payload, default=str))
        try:
            if hasattr(engine, "seed_position_params"):
                engine.seed_position_params(ticker, payload)  # type: ignore[union-attr]
            elif hasattr(engine, "_position_params"):
                engine._position_params[ticker] = payload  # type: ignore[union-attr]
        except Exception as exc:
            logger.debug("[RECONCILE] %s: engine param sync failed: %s", ticker, exc)

    now = datetime.now(UTC)
    collected_at = now.isoformat()
    new_conflicts: dict[str, dict] = {}
    matches: list[dict] = []
    auto_actions: list[dict] = []
    matched_tickers: set[str] = set()
    adopted_tickers: set[str] = set()
    closed_offline_tickers: set[str] = set()
    managed_naked_check_tickers: set[str] = set()
    naked_tickers: set[str] = set()
    entry_fix_tickers: set[str] = set()
    shares_synced_tickers: set[str] = set()

    try:
        await _prime_ib_open_orders(ib)
    except Exception as exc:
        logger.debug("[RECONCILE] open-order prime failed: %s", exc)
    try:
        await ib.reqPositionsAsync()
    except Exception as exc:
        logger.debug("[RECONCILE] reqPositionsAsync failed: %s", exc)

    current_positions: list = []
    try:
        current_positions = await engine.get_positions()
    except Exception as exc:
        logger.warning("[RECONCILE] Failed to load engine positions: %s", exc)
    current_by_ticker = {getattr(pos, "ticker", "").upper(): pos for pos in current_positions if getattr(pos, "ticker", "")}

    ib_positions: list[dict] = []
    try:
        for position in ib.positions() or []:
            record = _ib_position_to_dict(position)
            ticker = str(record.get("ticker", "")).upper()
            if not ticker:
                continue
            record["ticker"] = ticker
            ib_positions.append(record)
    except Exception as exc:
        logger.warning("[RECONCILE] Failed to read IB positions: %s", exc)

    open_trades = []
    try:
        open_trades = ib.openTrades() or []
    except Exception as exc:
        logger.warning("[RECONCILE] Failed to read IB open trades: %s", exc)

    app_oca_orders: list[dict] = []
    oca_groups: dict[str, str] = {}
    trail_order_ids: dict[str, int] = {}
    system_tickers: set[str] = set()
    protected_tickers: set[str] = set()
    # Tickers with a pending system MKT order (likely a close order submitted by
    # run_price_eval_loop that hasn't filled yet).  Reconcile skips the naked-position
    # check for these tickers to avoid conflicting with the in-flight close order.
    pending_mkt_tickers: set[str] = set()
    _protective_order_types = {"STP", "STP LMT", "TRAIL"}
    _done_statuses = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
    for trade in open_trades:
        try:
            order = getattr(trade, "order", None)
            contract = getattr(trade, "contract", None)
            status = getattr(getattr(trade, "orderStatus", None), "status", "")
            if getattr(order, "orderRef", "") != ORDER_REF:
                continue
            ticker = str(getattr(contract, "symbol", "") or "").upper()
            if not ticker:
                continue
            order_type = str(getattr(order, "orderType", "") or "")
            order_id = _to_int(getattr(order, "orderId", 0))
            oca_group = str(getattr(order, "ocaGroup", "") or "")
            app_oca_orders.append({
                "ticker": ticker,
                "order_type": order_type,
                "action": getattr(order, "action", ""),
                "oca_group": oca_group,
                "aux_price": float(getattr(order, "auxPrice", 0) or 0),
                "status": status,
            })
            system_tickers.add(ticker)
            if oca_group:
                oca_groups.setdefault(ticker, oca_group)
            if order_type == "TRAIL" and order_id:
                trail_order_ids[ticker] = order_id
            if order_type in _protective_order_types and status not in _done_statuses:
                protected_tickers.add(ticker)
            if order_type == "MKT" and status not in _done_statuses:
                # Pending system MKT order — likely a close order from price_eval_loop
                pending_mkt_tickers.add(ticker)
        except Exception as exc:
            logger.debug("[RECONCILE] open trade parse failed: %s", exc)

    ib_trades_today: list[dict] = []
    fills_by_ticker: dict[str, dict] = {}
    _close_fills_by_ticker: dict[str, dict] = {}
    fills_by_ticker_side: dict[str, dict[str, dict]] = {}
    _fill_sides_by_ticker: dict[str, set[str]] = {}
    _latest_fill_time: dict[str, datetime | None] = {}
    _fill_accum: dict[str, tuple[float, float]] = {}
    try:
        executions = await ib.reqExecutionsAsync(ExecutionFilter())
        for fill in executions or []:
            record = _execution_fill_to_dict(fill)
            ticker = str(record.get("ticker", "")).upper()
            if not ticker:
                continue
            record["ticker"] = ticker
            ib_trades_today.append(record)
            _update_latest_fill(fills_by_ticker, ticker, record)
            action = str(record.get("action", "")).upper()
            if action:
                _update_latest_fill(fills_by_ticker_side, ticker, record, side=action)
                _fill_sides_by_ticker.setdefault(ticker, set()).add(action)
                _ft_dt = _as_dt(record.get("time"))
                _key = f"{ticker}:{action}"
                if _ft_dt is not None:
                    _prev = _latest_fill_time.get(_key)
                    if _prev is None or _ft_dt > _prev:
                        _latest_fill_time[_key] = _ft_dt
                if action in {"SLD", "BOT"}:
                    _update_latest_fill(_close_fills_by_ticker, ticker, record)
                _px = _to_float(record.get("fill_price"))
                _qty = float(record.get("quantity", 0) or 0)
                if _px > 0 and _qty > 0:
                    _pv, _ps = _fill_accum.get(_key, (0.0, 0.0))
                    _fill_accum[_key] = (_pv + (_px * _qty), _ps + _qty)
    except Exception as exc:
        logger.warning("[RECONCILE] Failed to read IB executions: %s", exc)

    _round_tripped_today: set[str] = set()
    for t, sides in _fill_sides_by_ticker.items():
        if "BOT" not in sides or "SLD" not in sides:
            continue
        _bot_time = _latest_fill_time.get(f"{t}:BOT")
        _sld_time = _latest_fill_time.get(f"{t}:SLD")
        if _bot_time is None or _sld_time is None:
            logger.debug("[RECONCILE] %s: BOT+SLD fills but no timestamps — not marking as round-tripped", t)
            continue
        if _sld_time > _bot_time:
            _round_tripped_today.add(t)
            logger.debug("[RECONCILE] %s: round-tripped (SLD %s > BOT %s)", t, _sld_time, _bot_time)
        else:
            logger.info(
                "[RECONCILE] %s: BOT+SLD fills today but BOT (%s) is more recent than SLD (%s) — "
                "position re-entered after close; NOT marking as round-tripped",
                t, _bot_time, _sld_time,
            )

    fill_vwap_by_ticker: dict[str, dict[str, float]] = {}
    for key, (value, shares) in _fill_accum.items():
        if shares <= 0:
            continue
        ticker, side = key.split(":", 1)
        fill_vwap_by_ticker.setdefault(ticker, {})[side] = value / shares

    completed_by_ticker: dict[str, float] = {}
    system_entry_tickers: set[str] = set()
    try:
        for order_state in (await ib.reqCompletedOrdersAsync(apiOnly=False) or []):
            sym = str(getattr(getattr(order_state, "contract", None), "symbol", "") or "").upper()
            ref = getattr(getattr(order_state, "order", None), "orderRef", "")
            ot = getattr(getattr(order_state, "order", None), "orderType", "")
            st = getattr(getattr(order_state, "orderStatus", None), "status", "")
            avg_fill = _to_float(getattr(getattr(order_state, "orderStatus", None), "avgFillPrice", 0.0))
            if not sym or ref != ORDER_REF or st != "Filled":
                continue
            if ot == "MKT":
                system_entry_tickers.add(sym)
                if avg_fill > 0:
                    completed_by_ticker[sym] = avg_fill
    except Exception as exc:
        logger.debug("[RECONCILE] reqCompletedOrders unavailable: %s", exc)

    app_params = await redis.hgetall(_POSITION_PARAMS_KEY)
    params: dict[str, dict] = {}
    for key, value in (app_params or {}).items():
        ticker = _decode_text(key).upper()
        payload = _parse_json_value(value, {}) if value is not None else {}
        if not isinstance(payload, dict):
            payload = {}
        payload = dict(payload)
        payload["ticker"] = ticker
        params[ticker] = payload
    ib_by_ticker: dict[str, dict] = {str(pos["ticker"]).upper(): pos for pos in ib_positions}
    app_by_ticker: dict[str, dict] = {ticker: payload for ticker, payload in params.items()}

    async def _adopt_position(ticker: str, ib_entry: dict) -> tuple[bool, str]:
        pos = current_by_ticker.get(ticker)
        if pos is None:
            from social_trading.core.models import Position  # noqa: PLC0415

            pos = Position(
                ticker=ticker,
                direction=str(ib_entry.get("direction", "LONG") or "LONG"),
                shares=_to_int(ib_entry.get("shares", 0)),
                entry_price=_to_float(ib_entry.get("entry_price", 0.0)),
                opened_at=datetime.now(UTC),
                stop_loss=0.0,
                take_profit=0.0,
            )
        mkt_raw = await redis.hgetall(f"market_data:{ticker}")
        atr = 0.0
        try:
            atr_val = mkt_raw.get(b"atr_14") or mkt_raw.get("atr_14")
            if atr_val:
                atr = float(atr_val.decode() if isinstance(atr_val, bytes) else atr_val)
        except Exception:
            pass

        if atr > 0 and pos.entry_price > 0:
            if pos.direction == "LONG":
                stop_loss = round(pos.entry_price - cfg.atr_multiplier * atr, 2)
                take_profit = round(pos.entry_price * (1.0 + cfg.take_profit_pct), 2)
            else:
                stop_loss = round(pos.entry_price + cfg.atr_multiplier * atr, 2)
                take_profit = round(pos.entry_price * (1.0 - cfg.take_profit_pct), 2)
            seeded_params: dict = {
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "opened_at": pos.opened_at.isoformat() if pos.opened_at else datetime.now(UTC).isoformat(),
                "direction": pos.direction,
                "source": "system",
                "entry_price": pos.entry_price,
                "shares": pos.shares,
                "oca_group": oca_groups.get(ticker, ""),
                "trailing_stop_pct_applied": cfg.trailing_stop_pct,
            }
            await _persist_params(ticker, seeded_params)
            _adoption_fp = f"{pos.entry_price:.4f}:{abs(pos.shares)}"
            _adoption_flag = f"position:adopted:{ticker}:{_adoption_fp}"
            if await redis.set(_adoption_flag, "1", nx=True, ex=86400 * 90):
                await _publish_execution_event(redis, "position_opened", {
                    "ticker": ticker,
                    "direction": pos.direction,
                    "shares": pos.shares,
                    "entry_price": pos.entry_price,
                    "stop_price": stop_loss,
                    "target_price": take_profit,
                    "opened_at": seeded_params["opened_at"],
                    "mode": mode,
                })
            logger.warning(
                "[RECONCILE] %s: adopted system position — seeded sl=%.2f tp=%.2f from ATR=%.4f oca_group=%r",
                ticker, stop_loss, take_profit, atr, seeded_params["oca_group"],
            )
            if not seeded_params["oca_group"] and hasattr(engine, "reattach_oca_orders"):
                _ra_ok = False
                try:
                    _ra_ok = await engine.reattach_oca_orders(  # type: ignore[union-attr]
                        ticker=ticker,
                        direction=pos.direction,
                        quantity=pos.shares,
                        current_price=pos.entry_price,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        trailing_stop_pct=cfg.trailing_stop_pct,
                    )
                except Exception as exc:
                    logger.error("[RECONCILE] %s: OCA reattach exception during adoption: %s", ticker, exc)
                if _ra_ok:
                    if hasattr(engine, "_position_params") and ticker in engine._position_params:  # type: ignore[union-attr]
                        _new_ocag = engine._position_params[ticker].get("oca_group", "")  # type: ignore[union-attr]
                        if _new_ocag:
                            seeded_params["oca_group"] = _new_ocag
                    await redis.hset(_POSITION_PARAMS_KEY, ticker, json.dumps(seeded_params))
                    if hasattr(engine, "_ts_order_id") and ticker in engine._ts_order_id:  # type: ignore[union-attr]
                        await redis.hset(_TRAIL_ORDERS_KEY, ticker, str(engine._ts_order_id[ticker]))  # type: ignore[union-attr]
                    return True, f"{ticker} adopted and OCA bracket reattached."
                logger.error("[RECONCILE] %s: OCA reattach failed during adoption — closing for safety", ticker)
                await _close_adopted_no_oca(
                    redis, engine,
                    ticker=ticker,
                    direction=pos.direction,
                    entry_price=pos.entry_price,
                    shares=pos.shares,
                    opened_at=seeded_params["opened_at"],
                    mode=mode,
                )
                return True, f"{ticker} adopted but closed for safety after OCA reattach failure."
            return True, f"{ticker} adopted with ATR-derived protection."

        has_bracket = ticker in oca_groups or ticker in trail_order_ids
        if has_bracket:
            seeded_params = {
                "stop_loss": 0.0,
                "take_profit": 0.0,
                "opened_at": pos.opened_at.isoformat() if pos.opened_at else datetime.now(UTC).isoformat(),
                "direction": pos.direction,
                "source": "system",
                "entry_price": pos.entry_price,
                "shares": pos.shares,
                "oca_group": oca_groups.get(ticker, ""),
                "trailing_stop_pct_applied": cfg.trailing_stop_pct,
            }
            await _persist_params(ticker, seeded_params)
            _adoption_fp = f"{pos.entry_price:.4f}:{abs(pos.shares)}"
            _adoption_flag = f"position:adopted:{ticker}:{_adoption_fp}"
            if await redis.set(_adoption_flag, "1", nx=True, ex=86400 * 90):
                await _publish_execution_event(redis, "position_opened", {
                    "ticker": ticker,
                    "direction": pos.direction,
                    "shares": pos.shares,
                    "entry_price": pos.entry_price,
                    "stop_price": 0.0,
                    "target_price": 0.0,
                    "opened_at": seeded_params["opened_at"],
                    "mode": mode,
                })
            logger.warning(
                "[RECONCILE] %s: ATR unavailable but IB bracket is live (oca_group=%r) — seeding with stop_loss=0",
                ticker, seeded_params["oca_group"],
            )
            return True, f"{ticker} adopted with existing IB bracket."

        seeded_params = {
            "stop_loss": 0.0,
            "take_profit": 0.0,
            "opened_at": pos.opened_at.isoformat() if pos.opened_at else datetime.now(UTC).isoformat(),
            "direction": pos.direction,
            "source": "system",
            "entry_price": pos.entry_price,
            "shares": pos.shares,
            "oca_group": "",
        }
        await _persist_params(ticker, seeded_params)
        await _close_adopted_no_oca(
            redis, engine,
            ticker=ticker,
            direction=pos.direction,
            entry_price=pos.entry_price,
            shares=pos.shares,
            opened_at=seeded_params["opened_at"],
            mode=mode,
        )
        return True, f"{ticker} adopted without ATR/bracket; close submitted for safety."

    for ticker in sorted(set(app_by_ticker) | set(ib_by_ticker)):
        try:
            app_entry = app_by_ticker.get(ticker)
            ib_entry = ib_by_ticker.get(ticker)
            fill_entry = fills_by_ticker.get(ticker)
            state = ""
            reason = ""
            if app_entry and ib_entry:
                app_dir = str(app_entry.get("direction", "")).upper()
                ib_dir = str(ib_entry.get("direction", "")).upper()
                app_shares = _to_int(app_entry.get("shares", 0))
                ib_shares = _to_int(ib_entry.get("shares", 0))
                source_is_system = str(app_entry.get("source", "system")).lower() == "system"
                if not source_is_system:
                    state = "matched"
                    reason = f"{ticker}: app params source={app_entry.get('source')!r}; leaving manual position unchanged."
                    matched_tickers.add(ticker)
                    logger.info("[RECONCILE] %s: present in app params with source=%r", ticker, app_entry.get("source"))
                    matches.append({
                        "ticker": ticker,
                        "state": state,
                        "app": app_entry,
                        "ib": ib_entry,
                        "fill": fill_entry,
                        "reason": reason,
                    })
                    continue
                if app_dir and ib_dir and app_dir != ib_dir:
                    state = "direction_mismatch"
                    reason = f"{ticker}: app says {app_dir} but IB shows {ib_dir}."
                    new_conflicts[ticker] = {
                        "state": "direction_mismatch",
                        "ticker": ticker,
                        "app_direction": app_entry.get("direction"),
                        "ib_direction": ib_entry.get("direction"),
                        "app_shares": app_shares,
                        "ib_shares": ib_shares,
                        "reason": reason,
                        "options": ["use_ib_direction", "close_position"],
                        "detected_at": datetime.now(UTC).isoformat(),
                    }
                    matches.append({
                        "ticker": ticker,
                        "state": state,
                        "app": app_entry,
                        "ib": ib_entry,
                        "fill": fill_entry,
                        "reason": reason,
                    })
                    continue

                state = "matched"
                reason = f"App and IB both show {ticker} {ib_dir or app_dir} {ib_shares} shares."
                matched_tickers.add(ticker)
                managed_naked_check_tickers.add(ticker)
                if abs(app_shares - ib_shares) > 1 and ib_shares > 0:
                    synced = dict(app_entry)
                    synced["shares"] = ib_shares
                    if ib_dir:
                        synced["direction"] = ib_dir
                    await _persist_params(ticker, synced)
                    params[ticker] = synced
                    app_by_ticker[ticker] = synced
                    shares_synced_tickers.add(ticker)
                    state = "shares_synced"
                    reason = (
                        f"{ticker}: corrected shares {app_shares} → {ib_shares} from IB."
                    )
                    logger.warning("[RECONCILE] %s: auto-corrected shares %d → %d from IB", ticker, app_shares, ib_shares)
                    await _publish_execution_event(redis, "position_entry_updated", {
                        "ticker": ticker,
                        "shares": ib_shares,
                        "direction": ib_dir or app_dir,
                        "opened_at": synced.get("opened_at", datetime.now(UTC).isoformat()),
                    })
                    auto_actions.append({"ticker": ticker, "action": "sync_shares_from_ib"})
                    app_entry = synced

                if _to_float(app_entry.get("entry_price", 0.0)) <= 0:
                    fill_price = _entry_fill_price(ticker, ib_dir or app_dir or "LONG")
                    state = "fill_pending"
                    if fill_price > 0:
                        updated = dict(app_entry)
                        updated["entry_price"] = fill_price
                        await _persist_params(ticker, updated)
                        params[ticker] = updated
                        app_by_ticker[ticker] = updated
                        app_entry = updated
                        entry_fix_tickers.add(ticker)
                        reason = f"{ticker}: recovered entry VWAP {fill_price:.4f} from IB fills."
                        auto_actions.append({"ticker": ticker, "action": "fix_entry_price"})
                        await _publish_execution_event(redis, "position_entry_updated", {
                            "ticker": ticker,
                            "entry_price": fill_price,
                            "shares": _to_int(updated.get("shares", ib_shares)),
                            "direction": updated.get("direction", ib_dir or app_dir),
                            "opened_at": updated.get("opened_at", datetime.now(UTC).isoformat()),
                        })
                    else:
                        reason = f"{ticker}: entry fill still pending in IB history."

                if ticker not in protected_tickers and str(app_entry.get("source", "system")).lower() == "system":
                    if ticker in pending_mkt_tickers:
                        # A pending system MKT order is in IB — likely a close order submitted
                        # by run_price_eval_loop that hasn't filled yet.  Skip naked check;
                        # reconcile will detect the fill (closed_offline) next cycle.
                        logger.debug("[RECONCILE] %s: pending MKT order — skipping naked reattach", ticker)
                    else:
                        naked_tickers.add(ticker)
                        if state == "matched":
                            state = "naked"
                            reason = f"{ticker}: position is open but no active STP/TRAIL order is present."

            elif ib_entry and not app_entry:
                if ticker in (system_tickers | system_entry_tickers):
                    state = "adopted"
                    if ticker in _round_tripped_today:
                        reason = (
                            f"{ticker}: IB shows a system position marker but BOT/SLD fills indicate it round-tripped today; "
                            "skipping adoption."
                        )
                    else:
                        adopted_ok, reason = await _adopt_position(ticker, ib_entry)
                        if adopted_ok:
                            adopted_tickers.add(ticker)
                            managed_naked_check_tickers.add(ticker)
                            auto_actions.append({"ticker": ticker, "action": "adopt_position"})
                else:
                    state = "manual_ib"
                    reason = f"{ticker} exists in IB without app state or system order markers."
                    logger.info("[RECONCILE] %s: IB-only position treated as manual", ticker)
            elif app_entry and not ib_entry:
                app_dir = str(app_entry.get("direction", "")).upper() or "LONG"
                close_fill = _close_fill_for_ticker(ticker, app_dir)
                # close_is_valid: any close fill found today is authoritative.
                # We deliberately do NOT compare close_dt > opened_dt because IB fill
                # timestamps are wall-clock Eastern Time while opened_at is UTC —
                # mixing them would cause "09:35 ET" (treated as 09:35 UTC) to appear
                # earlier than "13:35 UTC" (the actual submission time), making every
                # OCA-fired close invisible to reconcile.
                # reqExecutionsAsync(ExecutionFilter()) already returns only today's fills
                # so there is no risk of a stale fill from a prior session being reused.
                close_is_valid = close_fill is not None
                if close_is_valid:
                    exit_price = _to_float(close_fill.get("fill_price", 0.0)) if close_fill else 0.0
                    exit_reason = close_fill.get("exit_reason", "STARTUP_RECONCILE_CLOSED") if close_fill else "STARTUP_RECONCILE_CLOSED"
                    closed_at = (
                        str(close_fill.get("time"))
                        if close_fill and close_fill.get("time")
                        else datetime.now(UTC).isoformat()
                    )
                    await _publish_execution_event(redis, "position_closed", {
                        "ticker": ticker,
                        "exit_price": exit_price,
                        "exit_reason": exit_reason,
                        "shares": _to_int(app_entry.get("shares", 0)),
                        "direction": app_entry.get("direction", "unknown"),
                        "entry_price": _to_float(app_entry.get("entry_price", 0.0)),
                        "closed_at": closed_at,
                        "opened_at": app_entry.get("opened_at", datetime.now(UTC).isoformat()),
                        "mode": mode,
                    })
                    await redis.hdel(_POSITION_PARAMS_KEY, ticker)
                    await redis.hdel(_HWM_REDIS_KEY, ticker)
                    await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
                    await redis.hdel(_PENDING_RECONCILE_KEY, ticker)
                    await redis.hdel(_POSITIONS_LIVE_KEY, ticker)
                    if hasattr(engine, "forget_position"):
                        engine.forget_position(ticker)  # type: ignore[union-attr]
                    params.pop(ticker, None)
                    app_by_ticker.pop(ticker, None)
                    closed_offline_tickers.add(ticker)
                    state = "closed_offline"
                    reason = f"{ticker} was closed while the service was offline; IB close fill confirmed."
                    auto_actions.append({"ticker": ticker, "action": "confirm_closed"})
                else:
                    # No IB position AND no close fill found.
                    # For positions opened very recently (< 5 min), the close fill
                    # might not have propagated to reqExecutionsAsync yet (IB latency).
                    # Classify as "fill_pending" for now and re-check next cycle
                    # rather than immediately raising a trading-halting conflict.
                    _recent_threshold_secs = 300  # 5 minutes
                    _is_recent = False
                    if opened_dt is not None:
                        _age_secs = (datetime.now(UTC) - opened_dt).total_seconds()
                        _is_recent = 0 < _age_secs < _recent_threshold_secs
                    if _is_recent:
                        state = "fill_pending"
                        reason = (
                            f"{ticker}: tracked by app, absent from IB, and no close fill yet — "
                            f"position is < 5 min old ({int((datetime.now(UTC) - opened_dt).total_seconds())}s); "  # type: ignore[operator]
                            "waiting for IB executions to propagate."
                        )
                        logger.info(
                            "[RECONCILE] %s: no IB position or close fill yet but position is recent (%ds old) — "
                            "classifying as fill_pending, will re-check next cycle",
                            ticker, int((datetime.now(UTC) - opened_dt).total_seconds()),  # type: ignore[operator]
                        )
                    else:
                        state = "missing"
                        new_conflicts[ticker] = {
                            "state": "missing",
                            "ticker": ticker,
                            "direction": app_entry.get("direction"),
                            "entry_price": app_entry.get("entry_price"),
                            "shares": app_entry.get("shares"),
                            "opened_at": app_entry.get("opened_at"),
                            "reason": (
                                f"{ticker}: tracked by app but absent from IB with no close fill record. "
                                "May be a prior-day close."
                            ),
                            "options": ["mark_closed", "force_adopt", "remove_app"],
                            "detected_at": datetime.now(UTC).isoformat(),
                        }
                        reason = new_conflicts[ticker]["reason"]
            else:
                continue

            matches.append({
                "ticker": ticker,
                "state": state,
                "app": app_entry,
                "ib": ib_entry,
                "fill": fill_entry,
                "reason": reason,
            })
        except Exception as exc:
            logger.warning("[RECONCILE] %s: reconcile processing failed: %s", ticker, exc)

    try:
        system_params = engine.get_position_params() if hasattr(engine, "get_position_params") else {}  # type: ignore[union-attr]
        if current_positions:
            managed_tickers = managed_naked_check_tickers | adopted_tickers
            managed_open_positions = [pos for pos in current_positions if getattr(pos, "ticker", "").upper() in managed_tickers]
            if managed_open_positions:
                await _check_naked_positions(
                    redis,
                    engine,
                    managed_open_positions,
                    system_params,
                    cfg,
                    mode,
                )
    except Exception as exc:
        logger.debug("[RECONCILE] naked-position follow-up failed: %s", exc)

    existing_conflicts_raw = await redis.hgetall(_RECONCILE_CONFLICTS_KEY)
    for ticker_raw in list(existing_conflicts_raw.keys()):
        decoded = ticker_raw.decode() if isinstance(ticker_raw, bytes) else ticker_raw
        if decoded not in new_conflicts:
            await redis.hdel(_RECONCILE_CONFLICTS_KEY, decoded)
    for ticker, conflict_data in new_conflicts.items():
        await redis.hset(_RECONCILE_CONFLICTS_KEY, ticker, json.dumps(conflict_data, default=str))

    try:
        await _write_positions_to_redis(redis, engine)
    except Exception as exc:
        logger.debug("[RECONCILE] positions:live refresh failed: %s", exc)

    latest_params_raw = await redis.hgetall(_POSITION_PARAMS_KEY)
    latest_params: dict[str, dict] = {}
    for key, value in (latest_params_raw or {}).items():
        ticker = _decode_text(key).upper()
        payload = _parse_json_value(value, {}) if value is not None else {}
        if not isinstance(payload, dict):
            payload = {}
        payload = dict(payload)
        payload["ticker"] = ticker
        latest_params[ticker] = payload

    summary = {
        "collected_at": collected_at,
        "ib_account": ib_account,
        "matched": len(matched_tickers),
        "adopted": len(adopted_tickers),
        "closed_offline": len(closed_offline_tickers),
        "fill_pending_fixed": len(entry_fix_tickers),
        "naked_flagged": len(naked_tickers),
        "conflicts": list(new_conflicts.keys()),
        "has_conflicts": bool(new_conflicts),
        "app_positions": [latest_params.get(t, {}) for t in sorted(latest_params)],
        "ib_positions": ib_positions,
        "app_oca_orders": app_oca_orders,
        "ib_trades_today": ib_trades_today,
        "matches": matches,
        "auto_actions": auto_actions,
        "states": {
            "matched": sorted(matched_tickers),
            "shares_synced": sorted(shares_synced_tickers),
            "fill_pending": sorted({m["ticker"] for m in matches if m.get("state") == "fill_pending"}),
            "naked": sorted(naked_tickers),
            "adopted": sorted(adopted_tickers),
            "manual_ib": sorted({m["ticker"] for m in matches if m.get("state") == "manual_ib"}),
            "closed_offline": sorted(closed_offline_tickers),
            "missing": sorted({m["ticker"] for m in matches if m.get("state") == "missing"}),
            "direction_mismatch": sorted({m["ticker"] for m in matches if m.get("state") == "direction_mismatch"}),
        },
        "protected_tickers": sorted(protected_tickers),
        "system_tickers": sorted(system_tickers),
        "system_entry_tickers": sorted(system_entry_tickers),
        "oca_groups": oca_groups,
        "trail_order_ids": trail_order_ids,
    }
    await redis.set(_RECONCILE_LAST_RUN_KEY, collected_at, ex=3600)
    await redis.set(_RECONCILE_FULL_KEY, json.dumps(summary, default=str), ex=3600)
    return summary


async def _load_hwm_from_redis(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
) -> None:
    """Seed engine HWM from Redis at startup so trailing stops survive restarts."""
    try:
        raw = await redis.hgetall(_HWM_REDIS_KEY)
        if not raw:
            return
        for field, value in raw.items():
            ticker = field.decode() if isinstance(field, bytes) else field
            try:
                hwm_value = float(value.decode() if isinstance(value, bytes) else value)
                engine.seed_hwm(ticker, hwm_value)
            except (ValueError, AttributeError):
                continue
        logger.info("[HWM] Loaded %d high-water marks from Redis", len(raw))
    except Exception as exc:
        logger.warning("[HWM] Failed to load from Redis: %s", exc)


async def _persist_hwm_to_redis(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
) -> None:
    """Persist engine HWM dict to Redis so trailing stops survive restarts."""
    try:
        hwm = engine.get_hwm()
        if not hwm:
            return
        mapping = {ticker: str(value) for ticker, value in hwm.items()}
        await redis.hset(_HWM_REDIS_KEY, mapping=mapping)
    except Exception as exc:
        logger.warning("[HWM] Failed to persist to Redis: %s", exc)


async def _load_position_params_from_redis(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
) -> None:
    """Restore position params (sl/tp/opened_at) from Redis so exit rules work after restart."""
    import json as _json  # noqa: PLC0415
    try:
        raw = await redis.hgetall(_POSITION_PARAMS_KEY)
        if not raw:
            return
        for field, value in raw.items():
            ticker = field.decode() if isinstance(field, bytes) else field
            try:
                params = _json.loads(value.decode() if isinstance(value, bytes) else value)
                engine.seed_position_params(ticker, params)  # type: ignore[union-attr]
            except Exception:
                continue
        logger.info("[PARAMS] Loaded position params for %d tickers from Redis", len(raw))
    except Exception as exc:
        logger.warning("[PARAMS] Failed to load from Redis: %s", exc)


async def _persist_position_params_to_redis(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
) -> None:
    """Persist position params (sl/tp/opened_at) to Redis so exit rules survive restarts.

    Performs a diff-and-delete: after writing current params, removes any Redis
    hash fields for tickers that are no longer in engine memory (i.e. closed
    positions whose hdel may have been missed due to a crash or command path gap).

    Safety rule: stale pruning is ONLY performed when the engine has at least one
    position in memory.  If the engine shows zero params (e.g. a loading failure
    after a crash), we skip pruning to avoid silently wiping persisted positions
    that the engine hasn't loaded yet.  This preserves the invariant that
    position:params can only shrink when IB confirms a close (via reconcile or
    inflight callback), not due to an engine memory miss.
    """
    import json as _json  # noqa: PLC0415
    try:
        params = engine.get_position_params()  # type: ignore[union-attr]
        if params:
            mapping = {ticker: _json.dumps(p) for ticker, p in params.items()}
            await redis.hset(_POSITION_PARAMS_KEY, mapping=mapping)
        # Prune stale tickers: any field in the hash that is not in current params
        # should be removed.  This catches positions closed via commands (CLOSE_ALL,
        # CLOSE_TICKER) that skip the explicit hdel, or any path where the engine
        # forgets the position but hdel was not called.
        #
        # Guard: only prune when engine params is non-empty.  An empty params dict
        # from a freshly-loaded engine that lost its state would otherwise wipe all
        # persisted positions.  If params is genuinely empty (all positions closed),
        # the individual hdel calls in the close paths have already removed the keys.
        if params:
            redis_keys_raw = await redis.hkeys(_POSITION_PARAMS_KEY)
            redis_keys = {(k.decode() if isinstance(k, bytes) else k) for k in redis_keys_raw}
            stale = redis_keys - set(params)
            if stale:
                await redis.hdel(_POSITION_PARAMS_KEY, *stale)
                logger.info("[PARAMS] Pruned %d stale ticker(s) from position:params: %s", len(stale), stale)
    except Exception as exc:
        logger.warning("[PARAMS] Failed to persist to Redis: %s", exc)


async def _load_trail_orders_from_redis(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
) -> None:
    """
    Seed engine TRAIL order IDs from Redis so update_trailing_stop() can cancel
    the correct order after a service restart without accumulating duplicates.
    """
    if not hasattr(engine, "seed_trail_order_id"):
        return
    try:
        raw = await redis.hgetall(_TRAIL_ORDERS_KEY)
        if not raw:
            return
        for field, value in raw.items():
            ticker = field.decode() if isinstance(field, bytes) else field
            try:
                order_id = int(value.decode() if isinstance(value, bytes) else value)
                engine.seed_trail_order_id(ticker, order_id)  # type: ignore[union-attr]
            except (ValueError, AttributeError):
                continue
        logger.info("[TRAIL] Loaded %d trail order IDs from Redis", len(raw))
    except Exception as exc:
        logger.warning("[TRAIL] Failed to load trail order IDs from Redis: %s", exc)


async def _persist_trail_orders_to_redis(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
) -> None:
    """Persist TRAIL order IDs so they survive service restarts."""
    if not hasattr(engine, "get_trail_orders"):
        return
    try:
        trail_orders = engine.get_trail_orders()  # type: ignore[union-attr]
        if not trail_orders:
            return
        mapping = {ticker: str(oid) for ticker, oid in trail_orders.items()}
        await redis.hset(_TRAIL_ORDERS_KEY, mapping=mapping)
    except Exception as exc:
        logger.warning("[TRAIL] Failed to persist trail order IDs to Redis: %s", exc)


async def _check_naked_positions(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
    open_positions: list,
    system_params: dict,
    cfg: "SystemConfig",
    mode: str,
    *,
    startup_ts: float = 0.0,
) -> tuple[set[str], set[str]]:
    """
    Detect system-tracked positions that have no live server-side OCA protective
    orders in IB (a "naked" position — open in IB with no stop/trail bracket).

    For each naked position:
      1. Reconstruct stop_loss / take_profit from entry_price + ATR if missing.
      2. Attempt to reattach OCA orders (STP, LMT, TRAIL) via
         engine.reattach_oca_orders().
      3. If reattach fails (or params are completely missing), close the position
         immediately to avoid running unprotected.

    Returns:
        (just_closed, just_reattached) — both sets should be excluded from
        exit rule evaluation for the remainder of this cycle.
    """
    just_closed: set[str] = set()
    just_reattached: set[str] = set()

    # Only applicable to live IBKR engine with active connection
    if not hasattr(engine, "_ib") or not hasattr(engine, "reattach_oca_orders"):
        return just_closed, just_reattached
    ib = engine._ib  # type: ignore[union-attr]
    if not ib.isConnected():
        return just_closed, just_reattached

    # Grace period after startup/reconnect: IB pushes openOrder callbacks
    # asynchronously.  If the exit loop fires within 30s of startup, openTrades()
    # may be incomplete — positions would look naked even though brackets are live.
    # We still run the check but skip reattach for positions that have a known
    # oca_group in their persisted params (proof that OCA was already placed).
    _STARTUP_GRACE_SECS = 30.0
    _in_grace = (startup_ts > 0) and ((asyncio.get_event_loop().time() - startup_ts) < _STARTUP_GRACE_SECS)

    # ── Build set of tickers with live *protective* OCA orders ───────────────
    # A ticker is "protected" only if IB has at least one active STP/TRAIL order
    # placed by this system (orderRef == ORDER_REF).  A bare LMT (TP-only) is not
    # a protective order — it cannot prevent unlimited loss.
    _protective_order_types = {"STP", "STP LMT", "TRAIL"}
    _done_statuses = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
    _active_statuses = {"PendingSubmit", "PreSubmitted", "Submitted"}
    protected_tickers: set[str] = set()
    # Tickers with a pending MKT close order — OCA was intentionally cancelled
    # before the close was submitted.  Do NOT reattach for these: doing so would
    # place new bracket orders that conflict with the in-flight MKT close.
    pending_mkt_close_tickers: set[str] = set()
    try:
        from social_trading.execution.ibkr import ORDER_REF  # noqa: PLC0415
        for trade in ib.openTrades():
            sym = getattr(getattr(trade, "contract", None), "symbol", "")
            ord_obj = getattr(trade, "order", None)
            ref = getattr(ord_obj, "orderRef", "")
            ot = getattr(ord_obj, "orderType", "")
            status = getattr(getattr(trade, "orderStatus", None), "status", "")
            if not sym or ref != ORDER_REF:
                continue
            if status in _done_statuses:
                continue
            if ot in _protective_order_types:
                protected_tickers.add(sym)
            elif ot == "MKT" and status in _active_statuses:
                # Active MKT order = a close submitted but not yet filled.
                # OCA brackets were intentionally cancelled before this order.
                pending_mkt_close_tickers.add(sym)
    except Exception as exc:
        logger.warning("[NAKED] Could not inspect IB open trades: %s — skipping check", exc)
        return just_closed, just_reattached  # cannot safely assess

    naked = [p for p in open_positions
             if p.ticker not in protected_tickers and p.ticker not in pending_mkt_close_tickers]
    for ticker in pending_mkt_close_tickers:
        if any(p.ticker == ticker for p in open_positions):
            logger.info(
                "[NAKED] %s: pending MKT close order active in IB — "
                "OCA intentionally absent; skipping naked check",
                ticker,
            )
    if not naked:
        return just_closed, just_reattached

    for pos in naked:
        ticker = pos.ticker
        params = system_params.get(ticker, {})
        current_price = engine.get_price(ticker) or pos.entry_price  # type: ignore[union-attr]
        entry_price = float(params.get("entry_price", 0.0)) or pos.entry_price
        stop_loss = float(params.get("stop_loss", 0.0))
        take_profit = float(params.get("take_profit", 0.0))
        direction = params.get("direction", pos.direction)
        quantity = int(params.get("shares", pos.shares)) or pos.shares
        known_oca_group = params.get("oca_group", "")

        # During the startup grace window, IB's openTrades() cache may be
        # incomplete (openOrder callbacks still arriving).  If this position
        # has a known OCA group from persisted params, the bracket was placed
        # in a prior session — skip reattach and let the next cycle confirm.
        if _in_grace and known_oca_group:
            logger.debug(
                "[NAKED] %s: startup grace period — skipping reattach "
                "(oca_group=%r already placed; openTrades cache may be incomplete)",
                ticker, known_oca_group,
            )
            continue

        logger.warning(
            "[NAKED] %s: no server-side protective orders — "
            "sl=%.4f tp=%.4f entry=%.4f current=%.4f",
            ticker, stop_loss, take_profit, entry_price, current_price,
        )

        # ── Reconstruct SL/TP from entry_price + ATR when persisted levels missing ──
        # Always use entry_price as anchor (not current_price) to preserve the
        # original risk envelope intended at order time.
        if (stop_loss <= 0 or take_profit <= 0) and entry_price > 0:
            atr = 0.0
            try:
                mkt_raw = await redis.hgetall(f"market_data:{ticker}")
                atr_val = mkt_raw.get(b"atr_14") or mkt_raw.get("atr_14")
                if atr_val:
                    atr = float(atr_val.decode() if isinstance(atr_val, bytes) else atr_val)
            except Exception:
                pass
            if atr > 0:
                if stop_loss <= 0:
                    stop_loss = round(
                        entry_price - cfg.atr_multiplier * atr
                        if direction == "LONG"
                        else entry_price + cfg.atr_multiplier * atr,
                        2,
                    )
                    logger.info(
                        "[NAKED] %s: reconstructed sl=%.4f from entry=%.4f ATR=%.4f",
                        ticker, stop_loss, entry_price, atr,
                    )
                if take_profit <= 0:
                    take_profit = round(
                        entry_price * (1.0 + cfg.take_profit_pct)
                        if direction == "LONG"
                        else entry_price * (1.0 - cfg.take_profit_pct),
                        2,
                    )
                    logger.info(
                        "[NAKED] %s: reconstructed tp=%.4f from entry=%.4f",
                        ticker, take_profit, entry_price,
                    )

        # If we have absolutely no usable params AND no entry price to reconstruct
        # from, close immediately — cannot define any risk envelope.
        if entry_price <= 0 and stop_loss <= 0 and take_profit <= 0:
            logger.error(
                "[NAKED] %s: no entry_price and no SL/TP — closing for safety",
                ticker,
            )
            try:
                await engine.close_position(ticker, reason="NAKED_NO_PARAMS")  # type: ignore[union-attr]
                just_closed.add(ticker)
                await _publish_execution_event(redis, "position_closed", {
                    "ticker": ticker,
                    "exit_price": current_price,
                    "exit_reason": "NAKED_NO_PARAMS",
                    "shares": quantity,
                    "direction": direction,
                    "entry_price": entry_price,
                    "closed_at": datetime.now(UTC).isoformat(),
                    "opened_at": params.get("opened_at", ""),
                    "mode": mode,
                })
                await redis.hdel(_HWM_REDIS_KEY, ticker)
                await redis.hdel(_POSITION_PARAMS_KEY, ticker)
                await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
            except Exception as exc:
                logger.error("[NAKED] %s: emergency close also failed: %s", ticker, exc)
            continue

        # ── Attempt reattach ──────────────────────────────────────────────────
        try:
            success = await engine.reattach_oca_orders(  # type: ignore[union-attr]
                ticker=ticker,
                direction=direction,
                quantity=quantity,
                current_price=current_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                trailing_stop_pct=cfg.trailing_stop_pct,
            )
        except Exception as exc:
            logger.error("[NAKED] %s: reattach raised exception: %s", ticker, exc)
            success = False

        if success:
            # Update in-memory params with possibly-reconstructed SL/TP so the
            # exit loop and step-5 persistence see the corrected values.
            if hasattr(engine, "_position_params") and ticker in engine._position_params:  # type: ignore[union-attr]
                engine._position_params[ticker]["stop_loss"] = stop_loss  # type: ignore[union-attr]
                engine._position_params[ticker]["take_profit"] = take_profit  # type: ignore[union-attr]
            # Immediately persist the new oca_group and trail order ID to Redis
            # so they survive a crash within the next cycle (60s window).
            try:
                params_raw_r = await redis.hget(_POSITION_PARAMS_KEY, ticker)
                _pdata: dict = {}
                if params_raw_r:
                    _pdata = json.loads(params_raw_r.decode() if isinstance(params_raw_r, bytes) else params_raw_r)
                if hasattr(engine, "_position_params") and ticker in engine._position_params:  # type: ignore[union-attr]
                    new_oca = engine._position_params[ticker].get("oca_group", "")  # type: ignore[union-attr]
                    if new_oca:
                        _pdata["oca_group"] = new_oca
                _pdata["stop_loss"]   = stop_loss
                _pdata["take_profit"] = take_profit
                await redis.hset(_POSITION_PARAMS_KEY, ticker, json.dumps(_pdata))
                if hasattr(engine, "_ts_order_id") and ticker in engine._ts_order_id:  # type: ignore[union-attr]
                    await redis.hset(_TRAIL_ORDERS_KEY, ticker, str(engine._ts_order_id[ticker]))  # type: ignore[union-attr]
            except Exception as _rpe:
                logger.debug("[NAKED] Failed to persist reattach params for %s: %s", ticker, _rpe)
            just_reattached.add(ticker)
            logger.warning(
                "[NAKED] %s: OCA bracket reattached — sl=%.4f tp=%.4f", ticker, stop_loss, take_profit,
            )
        else:
            logger.error(
                "[NAKED] %s: reattach failed — closing to avoid unprotected position", ticker,
            )
            try:
                await engine.close_position(ticker, reason="NAKED_REATTACH_FAILED")  # type: ignore[union-attr]
                just_closed.add(ticker)
                await _publish_execution_event(redis, "position_closed", {
                    "ticker": ticker,
                    "exit_price": current_price,
                    "exit_reason": "NAKED_REATTACH_FAILED",
                    "shares": quantity,
                    "direction": direction,
                    "entry_price": entry_price,
                    "closed_at": datetime.now(UTC).isoformat(),
                    "opened_at": params.get("opened_at", ""),
                    "mode": mode,
                })
                await redis.hdel(_HWM_REDIS_KEY, ticker)
                await redis.hdel(_POSITION_PARAMS_KEY, ticker)
                await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
            except Exception as exc:
                logger.error("[NAKED] %s: emergency close also failed: %s", ticker, exc)

    return just_closed, just_reattached


async def _close_adopted_no_oca(
    redis: "aioredis.Redis",
    engine: "ExecutionEngine",
    ticker: str,
    direction: str,
    entry_price: float,
    shares: int,
    opened_at: str,
    mode: str,
) -> None:
    """Close an adopted system position that cannot be protected with OCA orders.

    Submits a market close order and writes the orderId to exits:inflight so the
    periodic inflight poll can detect the fill and publish position_closed + clean
    up all tracking keys.  For immediate fills the event is published inline.

    This helper must only be called for positions opened by this system (source=system)
    — manual IB positions are never adopted and never reach this path.
    """
    logger.error(
        "[SYNC] %s: adopted position has no OCA protection — closing for safety "
        "(sl/tp unavailable; direction=%s entry=%.4f shares=%d)",
        ticker, direction, entry_price, shares,
    )
    current_price = engine.get_price(ticker) or entry_price  # type: ignore[union-attr]
    result = None
    try:
        result = await engine.close_position(ticker, reason="NO_OCA_ON_ADOPT")  # type: ignore[union-attr]
    except Exception as exc:
        logger.error("[SYNC] %s: close_position failed during adoption cleanup: %s", ticker, exc)
        return

    if result is None or result.status == "rejected":
        logger.warning("[SYNC] %s: close_position returned rejected/None — position may be already flat", ticker)
        # Clean up Redis params regardless so the exit loop doesn't track a ghost
        await redis.hdel(_POSITION_PARAMS_KEY, ticker)
        await redis.hdel(_HWM_REDIS_KEY, ticker)
        await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
        return

    fill_price = result.fill_price or 0.0

    if fill_price > 0:
        # Immediate fill — publish closed event and wipe all tracking keys
        await _publish_execution_event(redis, "position_closed", {
            "ticker": ticker,
            "exit_price": fill_price,
            "exit_reason": "NO_OCA_ON_ADOPT",
            "shares": shares,
            "direction": direction,
            "entry_price": entry_price,
            "closed_at": datetime.now(UTC).isoformat(),
            "opened_at": opened_at,
            "mode": mode,
        })
        await redis.hdel(_POSITION_PARAMS_KEY, ticker)
        await redis.hdel(_HWM_REDIS_KEY, ticker)
        await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
        logger.warning("[SYNC] %s: closed immediately (fill=%.4f) — all tracking keys removed", ticker, fill_price)
    else:
        # Deferred fill — write to exits:inflight so the periodic poll tracks it.
        # The params written before this call are intentionally kept in Redis until
        # the fill confirmation arrives via _reconcile_inflight_orders.
        order_id = str(result.order_id) if result.order_id else ""
        if order_id:
            await redis.hset(_INFLIGHT_EXIT_KEY, order_id, json.dumps({
                "ticker": ticker,
                "direction": direction,
                "entry_price": entry_price,
                "shares": shares,
                "provisional_exit": current_price,
                "opened_at": opened_at,
            }))
            await redis.expire(_INFLIGHT_EXIT_KEY, _INFLIGHT_TTL_SEC)
            logger.warning(
                "[SYNC] %s: MKT close submitted (deferred fill, orderId=%s) — "
                "exits:inflight written; periodic poll will publish position_closed on fill",
                ticker, order_id,
            )
        else:
            logger.warning("[SYNC] %s: close submitted but no orderId — cannot write exits:inflight; "
                           "manual intervention may be required", ticker)



async def _reconcile_startup(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
    mode: str = "live",
) -> None:
    """
    Compare Redis position:params against IB's current positions on startup.

    Any ticker present in Redis state but absent from IB was closed while the
    service was offline (bracket fill, manual close in TWS, etc.).  Clean up
    so stale sl/tp/hwm don't trigger false exits on the first cycle.

    For IB positions with no persisted params (opened in a prior session or
    manually in TWS), we check open orders for ORDER_REF = "social_trading"
    to decide whether they are system-managed ("system") or manual ("manual").
    Both are adopted into the exit loop — manual positions are labelled so the
    UI can display them differently.
    """
    params = engine.get_position_params()  # type: ignore[union-attr]
    cfg = await SystemConfig.load(redis)

    # Retry up to 3 times — IB may be briefly unavailable on startup
    # (TWS initialising, nightly restart window, etc.)
    current: list = []
    _max_attempts = 3
    _retry_delay = 5.0
    for _attempt in range(_max_attempts):
        try:
            current = await engine.get_positions()
            break
        except Exception as exc:
            if _attempt < _max_attempts - 1:
                logger.warning(
                    "[SYNC] IB unavailable (attempt %d/%d): %s — retrying in %.0fs",
                    _attempt + 1, _max_attempts, exc, _retry_delay,
                )
                await asyncio.sleep(_retry_delay)
            else:
                logger.error(
                    "[SYNC] Startup reconciliation aborted after %d attempts — IB unreachable: %s",
                    _max_attempts, exc,
                )
                return
    current_tickers = {p.ticker for p in current}

    # ── Build fill + order-type cache from IB for today's executions ────────
    # reqExecutionsAsync() fetches today's executions from IB's server (more
    # complete than ib.fills() for same-day offline gaps).
    # reqCompletedOrdersAsync() fetches filled/cancelled orders from the current
    # TWS session — may include recently filled bracket legs.
    # Both keyed by symbol → (exit_price, exit_reason, fill_time_utc).
    # fill_time_utc is stored so that fills that pre-date the position's opened_at
    # are not mistakenly used as the close of a NEW position opened after that fill.
    _offline_exit: dict[str, tuple[float, str, datetime | None]] = {}
    # Tickers where this system placed an entry (MKT) order, confirmed via
    # reqCompletedOrdersAsync.  Used to reclassify orphaned positions that
    # have no remaining open orders (e.g. OCA failed) but were system-opened.
    _system_entry_tickers: set[str] = set()
    # Tickers that have BOTH an entry AND a close fill today — they were already
    # round-tripped in this TWS session.  Do NOT re-adopt these as open positions.
    _fully_closed_today: set[str] = set()
    try:
        ib_obj = getattr(engine, "_ib", None)
        if ib_obj is not None:
            # Step 1: collect fill prices + timestamps from today's server-side executions
            # An order may be split into multiple partial fills.  Accumulate
            # shares × price per (sym, side) to compute a proper VWAP, and
            # keep the LATEST fill timestamp (last partial completes the position).
            _fill_prices: dict[str, float] = {}
            _fill_times: dict[str, datetime | None] = {}   # sym:side → fill time (UTC)
            # Track which sides (BOT / SLD) were seen per ticker today
            _fill_sides: dict[str, set[str]] = {}
            # Accumulators for VWAP: sym:side → (total_value, total_shares)
            _fill_accum_startup: dict[str, tuple[float, float]] = {}
            from ib_async import ExecutionFilter  # noqa: PLC0415
            executions = await ib_obj.reqExecutionsAsync(ExecutionFilter())
            for fill in executions:
                sym = getattr(getattr(fill, "contract", None), "symbol", "")
                side = getattr(getattr(fill, "execution", None), "side", "")
                price = getattr(getattr(fill, "execution", None), "price", 0.0)
                qty   = float(getattr(getattr(fill, "execution", None), "shares", 0) or 0)
                fill_time_raw = getattr(getattr(fill, "execution", None), "time", None)
                fill_dt: datetime | None = None
                if fill_time_raw:
                    try:
                        if isinstance(fill_time_raw, datetime):
                            fill_dt = fill_time_raw if fill_time_raw.tzinfo else fill_time_raw.replace(tzinfo=UTC)
                        else:
                            _ft = datetime.fromisoformat(str(fill_time_raw))
                            fill_dt = _ft if _ft.tzinfo else _ft.replace(tzinfo=UTC)
                    except Exception:
                        pass
                if sym and price > 0:
                    key = f"{sym}:{side}"
                    # Accumulate for VWAP
                    _pv, _ps = _fill_accum_startup.get(key, (0.0, 0.0))
                    _fill_accum_startup[key] = (_pv + price * max(qty, 1.0), _ps + max(qty, 1.0))
                    # Track latest fill time (last partial determines ordering)
                    _prev_dt = _fill_times.get(key)
                    if fill_dt is not None and (_prev_dt is None or fill_dt > _prev_dt):
                        _fill_times[key] = fill_dt
                    elif fill_dt is not None and key not in _fill_times:
                        _fill_times[key] = fill_dt
                    _fill_sides.setdefault(sym, set()).add(side.upper())
            # Compute VWAP for each sym:side
            _fill_prices = {k: v / s for k, (v, s) in _fill_accum_startup.items() if s > 0}

            # Detect round-trips: tickers with BOTH a buy fill AND a sell fill today
            # where the CLOSE (SLD) fill is MORE RECENT than the ENTRY (BOT) fill.
            # This correctly handles same-day re-entries: if a position was closed (SLD)
            # earlier and then re-entered (BOT) later, both sides exist but the final
            # state is an OPEN position — do NOT mark it as fully closed.
            for _sym, _sides in _fill_sides.items():
                if "BOT" not in _sides or "SLD" not in _sides:
                    continue
                _bot_t = _fill_times.get(f"{_sym}:BOT")
                _sld_t = _fill_times.get(f"{_sym}:SLD")
                if _bot_t is None or _sld_t is None:
                    # No timestamps — cannot determine ordering; assume NOT round-tripped (conservative)
                    logger.debug("[SYNC] %s: BOT+SLD fills but missing timestamps — skipping round-trip mark", _sym)
                    continue
                if _sld_t > _bot_t:
                    # Close happened after entry → genuine complete round-trip
                    _fully_closed_today.add(_sym)
                else:
                    # Entry is more recent than close → re-entered after earlier close;
                    # current IB position is the NEW entry, not a stale residual
                    logger.info(
                        "[SYNC] %s: BOT+SLD fills today but BOT (%s) is newer than SLD (%s) — "
                        "position was re-entered after close; NOT marking as fully-closed",
                        _sym, _bot_t.isoformat(), _sld_t.isoformat(),
                    )
            if _fully_closed_today:
                logger.info("[SYNC] Detected fully-closed today (SLD>BOT timestamps): %s", _fully_closed_today)

            # Step 2: classify by completed order type (most accurate); store fill time
            try:
                completed = await ib_obj.reqCompletedOrdersAsync(apiOnly=False)
                for order_state in completed:
                    sym = getattr(getattr(order_state, "contract", None), "symbol", "")
                    ref = getattr(getattr(order_state, "order", None), "orderRef", "")
                    ot = getattr(getattr(order_state, "order", None), "orderType", "")
                    status = getattr(getattr(order_state, "orderStatus", None), "status", "")
                    if ref != "social_trading" or status != "Filled" or not sym:
                        continue
                    avg_fill = getattr(getattr(order_state, "orderStatus", None), "avgFillPrice", 0.0)
                    # Try to extract the fill time from completedOrder (not always available)
                    _co_time_raw = getattr(getattr(order_state, "orderStatus", None), "lastFillTime", None)
                    _co_dt: datetime | None = None
                    if _co_time_raw:
                        try:
                            _co_t = datetime.fromisoformat(str(_co_time_raw))
                            _co_dt = _co_t if _co_t.tzinfo else _co_t.replace(tzinfo=UTC)
                        except Exception:
                            pass
                    # Fall back to reqExecutionsAsync timestamp for this symbol+side
                    if _co_dt is None:
                        _close_side = "SLD" if ot not in ("MKT",) else None
                        if _close_side:
                            _co_dt = _fill_times.get(f"{sym}:{_close_side}")
                    if ot in ("STP", "STP LMT"):
                        _offline_exit[sym] = (float(avg_fill), "STOP_LOSS", _co_dt)
                    elif ot == "LMT":
                        _offline_exit[sym] = (float(avg_fill), "TAKE_PROFIT", _co_dt)
                    elif ot == "TRAIL":
                        _offline_exit[sym] = (float(avg_fill), "TRAILING_STOP", _co_dt)
                    elif ot == "MKT":
                        # Entry market order placed by this system — remember the symbol
                        # so orphaned positions with no open orders are not misclassified
                        # as manual when OCA failed and left no remaining open orders.
                        _system_entry_tickers.add(sym)
            except Exception as exc:
                logger.debug("[SYNC] reqCompletedOrders unavailable: %s", exc)

            # Step 3: for symbols not classified by order type, use fill price + time
            # from executions but don't guess exit reason beyond SL/TP
            for key, price in _fill_prices.items():
                sym, side = key.split(":", 1)
                if sym not in _offline_exit and price > 0:
                    _offline_exit[sym] = (price, "", _fill_times.get(key))  # price known, reason unknown
    except Exception as exc:
        logger.debug("[SYNC] Could not prefetch today's executions: %s", exc)

    # Tickers in persisted params but no longer in IB were closed while offline.
    # IMPORTANT: positions are only cleared when we have confirmed evidence from IB.
    # If no fill/completion record is found, the position is moved to
    # positions:pending_reconcile so the user can review and act manually — we
    # never silently discard a persisted position without confirmation.
    orphaned = set(params) - current_tickers
    for ticker in orphaned:
        # Read params before any cleanup so we can include them in events/alerts.
        p = params.get(ticker, {})
        opened_at = p.get("opened_at", datetime.now(UTC).isoformat())
        direction = p.get("direction", "unknown")
        entry_price = float(p.get("entry_price", 0.0))
        shares = int(p.get("shares", 0))
        stop_loss = float(p.get("stop_loss", 0.0))
        take_profit = float(p.get("take_profit", 0.0))

        # Try to get exit price + reason from IB records
        exit_price, exit_reason, exit_fill_dt = _offline_exit.get(ticker, (0.0, "", None))

        # Guard: if the fill time is known and predates the position's opened_at,
        # the fill belongs to a PREVIOUS position with the same ticker (e.g. same
        # ticker re-entered right after a close during a reconnect window).
        # Using that fill as the close of the NEW position would incorrectly kill
        # a just-opened trade (it would be classified as "closed_offline" even
        # though the entry order is still working in IB).
        if exit_fill_dt is not None and exit_price > 0:
            try:
                _pos_opened_dt = datetime.fromisoformat(opened_at)
                if _pos_opened_dt.tzinfo is None:
                    _pos_opened_dt = _pos_opened_dt.replace(tzinfo=UTC)
                if exit_fill_dt < _pos_opened_dt:
                    logger.info(
                        "[SYNC] %s: fill at %s predates position opened_at %s — "
                        "fill is from a prior position; treating as no fill record",
                        ticker, exit_fill_dt.isoformat(), opened_at,
                    )
                    exit_price = 0.0
                    exit_reason = ""
            except Exception:
                pass  # If timestamp parse fails, keep the fill (conservative)

        if exit_price > 0 and not exit_reason:
            # Have fill price but no order-type classification.
            # Only classify against ATR SL / TP — don't guess TRAILING_STOP.
            if stop_loss > 0 and take_profit > 0:
                sl_dist = abs(exit_price - stop_loss)
                tp_dist = abs(exit_price - take_profit)
                tolerance = exit_price * 0.005
                if sl_dist <= tolerance or sl_dist <= tp_dist:
                    exit_reason = "STOP_LOSS"
                elif tp_dist <= tolerance or tp_dist < sl_dist:
                    exit_reason = "TAKE_PROFIT"
                else:
                    exit_reason = "IB_EXTERNAL"
            else:
                exit_reason = "IB_EXTERNAL"

        if exit_price > 0:
            # ── Confirmed close: IB has a fill record — proceed with cleanup ──
            # We have evidence the position was closed; record the event and
            # remove from all tracking structures.
            engine.forget_position(ticker)  # type: ignore[union-attr]
            await redis.hdel(_HWM_REDIS_KEY, ticker)
            await redis.hdel(_POSITION_PARAMS_KEY, ticker)
            await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
            await redis.hdel(_PENDING_RECONCILE_KEY, ticker)  # clear any prior pending entry
            logger.warning(
                "[SYNC] %s: closed while offline — reason=%s exit_price=%.4f; cleaned up",
                ticker, exit_reason, exit_price,
            )
            await _publish_execution_event(redis, "position_closed", {
                "ticker": ticker,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "direction": direction,
                "entry_price": entry_price,
                "shares": shares,
                "closed_at": datetime.now(UTC).isoformat(),
                "opened_at": opened_at,
                "mode": mode,
            })
        else:
            # ── No confirmation: park in pending_reconcile for manual review ──
            # The position is absent from IB and has no fill record for today.
            # This can happen for positions opened on a prior trading day that
            # were closed before today's session, or if IB's execution history
            # is unavailable.  Do NOT auto-discard — require explicit user action.
            pending_payload = json.dumps({
                "ticker": ticker,
                "direction": direction,
                "entry_price": entry_price,
                "shares": shares,
                "opened_at": opened_at,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "reason": "not_in_ib_no_fill_record",
                "message": (
                    f"{ticker}: present in app but not found in IB, and no "
                    f"fill record was found in today's IB execution history. "
                    f"This may be a position closed on a prior trading day. "
                    f"Use 'Mark as Closed' or 'Remove from App' to resolve."
                ),
                "last_checked_at": datetime.now(UTC).isoformat(),
            })
            await redis.hset(_PENDING_RECONCILE_KEY, ticker, pending_payload)
            logger.warning(
                "[SYNC] %s: in position:params but absent from IB with no fill record "
                "— moved to positions:pending_reconcile for manual review",
                ticker,
            )

    # ── Inflight order reconciliation ─────────────────────────────────────────
    # Always run BEFORE the orphaned_in_ib early-return so that entry/exit fill
    # recovery happens even when there are no orphaned IB positions (the common
    # case where all positions are already matched).  Previously this call lived
    # at the end of the function, which meant it was unreachable when
    # orphaned_in_ib was empty.
    await _reconcile_inflight_orders(redis, engine, current_tickers, mode=mode)

    orphaned_in_ib = current_tickers - set(params)

    # ── Scan all open IB orders once ─────────────────────────────────────────
    # Collect:
    #   system_tickers — positions opened by this system (have ORDER_REF)
    #   oca_groups     — OCA group name per ticker (Fix 3)
    #   trail_order_ids — TRAIL order ID per ticker (Fix 1 IB fallback)
    system_tickers: set[str] = set()
    oca_groups: dict[str, str] = {}
    trail_order_ids: dict[str, int] = {}
    try:
        from social_trading.execution.ibkr import ORDER_REF  # noqa: PLC0415
        open_trades = engine._ib.openTrades()  # type: ignore[union-attr]
        for trade in open_trades:
            sym = getattr(getattr(trade, "contract", None), "symbol", "")
            ord_obj = getattr(trade, "order", None)
            ref = getattr(ord_obj, "orderRef", "")
            ot = getattr(ord_obj, "orderType", "")
            oid = getattr(ord_obj, "orderId", 0)
            ocg = getattr(ord_obj, "ocaGroup", "")
            if not sym or ref != ORDER_REF:
                continue
            # System-managed marker — applies to orphaned positions only
            if sym in orphaned_in_ib:
                system_tickers.add(sym)
            # OCA group recovery — applies to any current system position
            if sym in current_tickers and ocg:
                oca_groups.setdefault(sym, ocg)
            # TRAIL order ID recovery for Fix 1 (covers ALL current positions)
            if sym in current_tickers and ot == "TRAIL" and oid:
                trail_order_ids[sym] = oid
    except Exception as exc:
        logger.debug("[SYNC] Could not check open orders for orderRef: %s", exc)

    # ── Orphaned pending entry orders ─────────────────────────────────────────
    # An entry MKT order placed by this system may still be working in IB even
    # though the service restarted with no matching position or params (the order
    # was submitted just before a crash/disconnect and we never saw the fill).
    # These orphaned entry orders must be cancelled: if left alive in IB, a fill
    # would open an unprotected position that is invisible to our exit loop.
    try:
        from social_trading.execution.ibkr import ORDER_REF as _ORDER_REF  # noqa: PLC0415
        tracked_tickers = current_tickers | set(params)
        _pending_entry_statuses = {"PendingSubmit", "PreSubmitted", "Submitted"}
        for trade in (getattr(engine, "_ib", None) or object()).openTrades():  # type: ignore[union-attr]
            sym = getattr(getattr(trade, "contract", None), "symbol", "")
            ord_obj = getattr(trade, "order", None)
            os_obj = getattr(trade, "orderStatus", None)
            ref = getattr(ord_obj, "orderRef", "")
            ot = getattr(ord_obj, "orderType", "")
            status = getattr(os_obj, "status", "")
            action = getattr(ord_obj, "action", "")
            if not sym or ref != _ORDER_REF:
                continue
            # Pending entry order: MKT BUY or SELL not linked to any tracked position
            if (
                ot == "MKT"
                and action in ("BUY", "SELL")
                and status in _pending_entry_statuses
                and sym not in tracked_tickers
            ):
                logger.warning(
                    "[SYNC] %s: orphaned pending entry order (orderId=%d status=%s) — cancelling",
                    sym, getattr(ord_obj, "orderId", 0), status,
                )
                try:
                    engine._ib.cancelOrder(ord_obj)  # type: ignore[union-attr]
                except Exception as _ce:
                    logger.warning("[SYNC] Failed to cancel orphaned entry for %s: %s", sym, _ce)
    except Exception as exc:
        logger.debug("[SYNC] Could not check for orphaned pending entries: %s", exc)

    # Recover TRAIL order IDs from IB for ALL current system positions.
    # seed_trail_order_id is in-memory only; this seeds it for positions whose
    # Redis key was already loaded and for newly adopted ones discovered below.
    if hasattr(engine, "seed_trail_order_id"):
        for sym, oid in trail_order_ids.items():
            engine.seed_trail_order_id(sym, oid)  # type: ignore[union-attr]
            logger.info("[SYNC] %s: recovered TRAIL order ID %d from IB open trades", sym, oid)

    # Reclassify orphaned positions that were opened by this system but have
    # no remaining open orders (e.g. OCA placement failed).  openTrades() alone
    # can't detect these; reqCompletedOrdersAsync() MKT fills with ORDER_REF confirm.
    newly_confirmed = (_system_entry_tickers & orphaned_in_ib) - system_tickers
    if newly_confirmed:
        system_tickers |= newly_confirmed
        logger.info(
            "[SYNC] Reclassified %s as system-managed via completed MKT entry fills "
            "(had no open orders — likely OCA failure)",
            newly_confirmed,
        )

    if not orphaned_in_ib:
        return

    for pos in current:
        if pos.ticker not in orphaned_in_ib:
            continue
        source = "system" if pos.ticker in system_tickers else "manual"

        # Manual positions are managed by the user elsewhere — do not adopt
        # them into system params, exit rules, or positions:live.
        if source == "manual":
            logger.info(
                "[SYNC] %s: open in IB but no system orderRef — treating as manual, skipping adoption",
                pos.ticker,
            )
            continue

        # If this ticker had BOTH an entry fill AND a close fill today, the position
        # was already round-tripped (opened and closed) this TWS session.  IB may
        # briefly still show it in positions() before settlement clears it.
        # Re-adopting it would create a ghost open row in the DB.  Skip it and
        # ensure any stale params are cleaned up.
        if pos.ticker in _fully_closed_today:
            logger.info(
                "[SYNC] %s: skipping re-adoption — both BOT and SLD fills found today "
                "(position already round-tripped; IB cache may not yet reflect close)",
                pos.ticker,
            )
            # Clean up any stale params that might have been left from a prior restart
            await redis.hdel(_POSITION_PARAMS_KEY, pos.ticker)
            await redis.hdel(_HWM_REDIS_KEY, pos.ticker)
            await redis.hdel(_TRAIL_ORDERS_KEY, pos.ticker)
            await redis.hdel(_PENDING_RECONCILE_KEY, pos.ticker)
            if hasattr(engine, "forget_position"):
                engine.forget_position(pos.ticker)  # type: ignore[union-attr]
            continue

        # Position is open in IB but has no persisted params (opened in a prior
        # session by this system).  Seed params from Redis market data so
        # the software exit loop can monitor stop-loss / take-profit.
        mkt_raw = await redis.hgetall(f"market_data:{pos.ticker}")
        atr = 0.0
        try:
            atr_val = mkt_raw.get(b"atr_14") or mkt_raw.get("atr_14")
            if atr_val:
                atr = float(atr_val.decode() if isinstance(atr_val, bytes) else atr_val)
        except (ValueError, AttributeError):
            pass

        if atr > 0 and pos.entry_price > 0:
            if pos.direction == "LONG":
                stop_loss = round(pos.entry_price - cfg.atr_multiplier * atr, 2)
                take_profit = round(pos.entry_price * (1.0 + cfg.take_profit_pct), 2)
            else:
                stop_loss = round(pos.entry_price + cfg.atr_multiplier * atr, 2)
                take_profit = round(pos.entry_price * (1.0 - cfg.take_profit_pct), 2)
            seeded_params: dict = {
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "opened_at": pos.opened_at.isoformat() if pos.opened_at else datetime.now(UTC).isoformat(),
                "direction": pos.direction,
                "source": "system",
                "entry_price": pos.entry_price,
                "shares": pos.shares,
                # Fix 3: seed OCA group and initial trail pct so update_trailing_stop()
                # links replacement TRAIL orders into the correct OCA bracket.
                "oca_group": oca_groups.get(pos.ticker, ""),
                "trailing_stop_pct_applied": cfg.trailing_stop_pct,
            }
            engine.seed_position_params(pos.ticker, seeded_params)  # type: ignore[union-attr]
            await redis.hset(
                _POSITION_PARAMS_KEY,
                pos.ticker,
                json.dumps(seeded_params),
            )
            # Publish position_opened so persistence_service creates a trades DB row.
            # Guard with a Redis NX key so repeated restarts don't create duplicate rows.
            # Use a stable fingerprint (entry_price + shares) rather than datetime.now()
            # since IB Position objects have no opened_at timestamp and now() changes
            # on every restart, defeating the nx=True dedup guard.
            _adoption_fp = f"{pos.entry_price:.4f}:{abs(pos.shares)}"
            _adoption_flag = f"position:adopted:{pos.ticker}:{_adoption_fp}"
            if await redis.set(_adoption_flag, "1", nx=True, ex=86400 * 90):
                await _publish_execution_event(redis, "position_opened", {
                    "ticker": pos.ticker,
                    "direction": pos.direction,
                    "shares": pos.shares,
                    "entry_price": pos.entry_price,
                    "stop_price": stop_loss,
                    "target_price": take_profit,
                    "opened_at": seeded_params["opened_at"],
                    "mode": mode,
                })
            logger.warning(
                "[SYNC] %s: prior-session system position — seeded sl=%.2f tp=%.2f from ATR=%.4f oca_group=%r",
                pos.ticker, stop_loss, take_profit, atr, seeded_params["oca_group"],
            )
            # If the position has no live OCA bracket (no open STP/TRAIL orders),
            # submit one now rather than waiting for the next exit-loop cycle.
            # This satisfies the requirement that app-submitted adopted positions
            # must always have IB-side risk protection immediately after adoption.
            if not seeded_params["oca_group"] and hasattr(engine, "reattach_oca_orders"):
                _ra_ok = False
                try:
                    _ra_ok = await engine.reattach_oca_orders(  # type: ignore[union-attr]
                        ticker=pos.ticker,
                        direction=pos.direction,
                        quantity=pos.shares,
                        current_price=pos.entry_price,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        trailing_stop_pct=cfg.trailing_stop_pct,
                    )
                except Exception as _roa_exc:
                    logger.error("[SYNC] %s: OCA reattach exception during adoption: %s", pos.ticker, _roa_exc)
                if _ra_ok:
                    # Persist the new OCA group and TRAIL order ID so the exit loop
                    # and any restart within this cycle has the full bracket context.
                    if hasattr(engine, "_position_params") and pos.ticker in engine._position_params:  # type: ignore[union-attr]
                        _new_ocag = engine._position_params[pos.ticker].get("oca_group", "")  # type: ignore[union-attr]
                        if _new_ocag:
                            seeded_params["oca_group"] = _new_ocag
                    await redis.hset(_POSITION_PARAMS_KEY, pos.ticker, json.dumps(seeded_params))
                    if hasattr(engine, "_ts_order_id") and pos.ticker in engine._ts_order_id:  # type: ignore[union-attr]
                        await redis.hset(_TRAIL_ORDERS_KEY, pos.ticker, str(engine._ts_order_id[pos.ticker]))  # type: ignore[union-attr]
                    logger.warning(
                        "[SYNC] %s: OCA bracket submitted during adoption — sl=%.4f tp=%.4f",
                        pos.ticker, stop_loss, take_profit,
                    )
                else:
                    # Reattach failed — close the position to avoid running unprotected.
                    # _close_adopted_no_oca writes exits:inflight for deferred fills and
                    # publishes position_closed for immediate fills.
                    logger.error(
                        "[SYNC] %s: OCA reattach failed during adoption — closing for safety",
                        pos.ticker,
                    )
                    await _close_adopted_no_oca(
                        redis, engine,
                        ticker=pos.ticker,
                        direction=pos.direction,
                        entry_price=pos.entry_price,
                        shares=pos.shares,
                        opened_at=seeded_params["opened_at"],
                        mode=mode,
                    )
        else:
            # Fix 2: Before closing, check whether IB still has live bracket orders
            # protecting this position.  If yes, let IB handle stops and defer to the
            # software exit loop for time/sentiment rules only (don't close prematurely).
            has_bracket = pos.ticker in oca_groups or pos.ticker in trail_order_ids
            if has_bracket:
                seeded_params = {
                    "stop_loss": 0.0,   # 0.0 = IB handles stop; software defers
                    "take_profit": 0.0,
                    "opened_at": pos.opened_at.isoformat() if pos.opened_at else datetime.now(UTC).isoformat(),
                    "direction": pos.direction,
                    "source": "system",
                    "entry_price": pos.entry_price,
                    "shares": pos.shares,
                    "oca_group": oca_groups.get(pos.ticker, ""),
                    "trailing_stop_pct_applied": cfg.trailing_stop_pct,
                }
                engine.seed_position_params(pos.ticker, seeded_params)  # type: ignore[union-attr]
                await redis.hset(_POSITION_PARAMS_KEY, pos.ticker, json.dumps(seeded_params))
                # Use stable fingerprint so repeated restarts don't create duplicate DB rows.
                _adoption_fp = f"{pos.entry_price:.4f}:{abs(pos.shares)}"
                _adoption_flag = f"position:adopted:{pos.ticker}:{_adoption_fp}"
                if await redis.set(_adoption_flag, "1", nx=True, ex=86400 * 90):
                    await _publish_execution_event(redis, "position_opened", {
                        "ticker": pos.ticker,
                        "direction": pos.direction,
                        "shares": pos.shares,
                        "entry_price": pos.entry_price,
                        "stop_price": 0.0,
                        "target_price": 0.0,
                        "opened_at": seeded_params["opened_at"],
                        "mode": mode,
                    })
                logger.warning(
                    "[SYNC] %s: ATR unavailable but IB bracket is live (oca_group=%r) — "
                    "seeding with stop_loss=0 (IB OCA handles stops; software monitors time/sentiment)",
                    pos.ticker, seeded_params["oca_group"],
                )
            else:
                # Truly unprotected (no ATR, no IB bracket) — must close.
                # Seed minimal params first so exits:inflight processing has
                # context (opened_at / direction / entry_price) when the fill arrives.
                seeded_params = {
                    "stop_loss": 0.0,
                    "take_profit": 0.0,
                    "opened_at": pos.opened_at.isoformat() if pos.opened_at else datetime.now(UTC).isoformat(),
                    "direction": pos.direction,
                    "source": "system",
                    "entry_price": pos.entry_price,
                    "shares": pos.shares,
                    "oca_group": "",
                }
                engine.seed_position_params(pos.ticker, seeded_params)  # type: ignore[union-attr]
                await redis.hset(_POSITION_PARAMS_KEY, pos.ticker, json.dumps(seeded_params))
                await _close_adopted_no_oca(
                    redis, engine,
                    ticker=pos.ticker,
                    direction=pos.direction,
                    entry_price=pos.entry_price,
                    shares=pos.shares,
                    opened_at=seeded_params["opened_at"],
                    mode=mode,
                )


async def _reconcile_inflight_orders(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
    current_tickers: set[str],
    mode: str = "live",
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> tuple[int, int]:
    """
    Attempt to recover actual fill prices for any entry/exit orders still in the
    inflight ledger (orders:inflight / exits:inflight).

    Called from:
      - _reconcile_startup() — on every service start
      - run_exit_loop()       — after every successful mid-session IB reconnect
      - REFRESH_SYNC command  — on-demand from the UI

    Fill prices are sourced from IB's server-side execution history
    (reqExecutionsAsync) AND completed order history (reqCompletedOrdersAsync).
    The query is retried up to max_retries times (2s apart) to handle IB startup lag.

    For inflight entries that cannot be resolved after all retries:
      - If the position is still open: writes a warning to alerts:fill_sync so the UI
        can prompt the user with action options (Attempt Reconcile / Close Position).
      - If the position is already gone: cleans up the ledger entry silently.

    Returns (entry_fixes, exit_fixes) counts.
    """
    ib_obj = getattr(engine, "_ib", None)
    if ib_obj is None:
        return 0, 0

    # ── Build fill price map from IB with retries ──────────────────────────────
    # Primary: reqExecutionsAsync — server-side fills for today's session.
    #   An order may execute as multiple partial fills (same orderId, different
    #   execIds).  Accumulate shares × price per orderId and compute VWAP.
    # Fallback: reqCompletedOrdersAsync — avgFillPrice is IB's own VWAP across
    #   all partials and is preferred over our manual accumulation when available.
    _all_fills: dict[int, float] = {}
    for _attempt in range(max_retries):
        try:
            from ib_async import ExecutionFilter as _EF  # noqa: PLC0415
            # Accumulate partial fills: orderId → (total_value, total_shares)
            _fill_accum: dict[int, tuple[float, float]] = {}
            for _f in await ib_obj.reqExecutionsAsync(_EF()):
                _oid = getattr(getattr(_f, "execution", None), "orderId", 0)
                _px  = getattr(getattr(_f, "execution", None), "price", 0.0)
                _sh  = float(getattr(getattr(_f, "execution", None), "shares", 0) or 0)
                if _oid and _px > 0:
                    _pv, _ps = _fill_accum.get(int(_oid), (0.0, 0.0))
                    _fill_accum[int(_oid)] = (_pv + _px * max(_sh, 1.0), _ps + max(_sh, 1.0))
            # Compute VWAP from accumulated partials
            _all_fills = {oid: v / s for oid, (v, s) in _fill_accum.items() if s > 0}
            # Override with avgFillPrice from completed orders — IB's own VWAP is
            # more accurate than our manual accumulation (handles edge cases like
            # lot-size rounding) and should be preferred when available.
            try:
                for _co in (await ib_obj.reqCompletedOrdersAsync(apiOnly=False) or []):
                    _oid = getattr(getattr(_co, "order", None), "orderId", 0)
                    _px  = getattr(getattr(_co, "orderStatus", None), "avgFillPrice", 0.0)
                    if _oid and _px > 0:
                        _all_fills[int(_oid)] = float(_px)  # prefer IB's computed avg
            except Exception:
                pass
            break  # success
        except Exception as _exc:
            if _attempt < max_retries - 1:
                logger.debug(
                    "[SYNC] Inflight reconcile: IB exec fetch attempt %d/%d failed: %s — retrying in %.0fs",
                    _attempt + 1, max_retries, _exc, retry_delay,
                )
                await asyncio.sleep(retry_delay)
            else:
                logger.warning(
                    "[SYNC] Inflight reconcile: could not fetch IB executions after %d attempts: %s",
                    max_retries, _exc,
                )

    now_utc = datetime.now(UTC)
    entry_fixes = 0
    exit_fixes = 0

    # ── Entry inflight reconcile ───────────────────────────────────────────────
    try:
        _entry_inf = await redis.hgetall(_INFLIGHT_ENTRY_KEY)
        for _k, _v in (_entry_inf or {}).items():
            _oid_str = _k.decode() if isinstance(_k, bytes) else str(_k)
            try:
                _d = json.loads(_v.decode() if isinstance(_v, bytes) else _v)
                _t  = _d.get("ticker", "")
                _oa = _d.get("opened_at", "")
                _fp = _all_fills.get(int(_oid_str), 0.0)

                if _fp > 0:
                    logger.info("[SYNC] Inflight entry %s orderId=%s: recovered fill %.4f", _t, _oid_str, _fp)
                    # If position:params are missing (e.g. deleted by a reconcile cycle
                    # between pre-market order submission and market-open fill), reconstruct
                    # them from the inflight data so the exit loop can track the position.
                    _pr_sync = await redis.hget(_POSITION_PARAMS_KEY, _t)
                    if not _pr_sync:
                        _sl_sync = float(_d.get("stop_loss", 0) or 0)
                        _tp_sync = float(_d.get("take_profit", 0) or 0)
                        _dir_sync = str(_d.get("direction", "") or "")
                        _qty_sync = int(_d.get("quantity", 0) or 0)
                        _md_sync = str(_d.get("mode", "live") or "live")
                        if _dir_sync and _qty_sync:
                            logger.warning(
                                "[SYNC] %s: position:params missing at fill recovery — "
                                "reconstructing (sl=%.4f tp=%.4f) and publishing position_opened",
                                _t, _sl_sync, _tp_sync,
                            )
                            await redis.hset(_POSITION_PARAMS_KEY, _t, json.dumps({
                                "stop_loss": _sl_sync,
                                "take_profit": _tp_sync,
                                "opened_at": _oa,
                                "direction": _dir_sync,
                                "source": "system",
                                "entry_price": _fp,
                                "shares": _qty_sync,
                            }))
                            await redis.hset(_POSITIONS_LIVE_KEY, _t, json.dumps({
                                "ticker": _t,
                                "direction": _dir_sync,
                                "shares": _qty_sync,
                                "entry_price": _fp,
                                "stop_loss": _sl_sync,
                                "take_profit": _tp_sync,
                                "unrealized_pnl": 0.0,
                                "high_water_mark": _fp,
                                "opened_at": _oa,
                                "source": "system",
                            }))
                            await _publish_execution_event(redis, "position_opened", {
                                "ticker": _t,
                                "direction": _dir_sync,
                                "shares": _qty_sync,
                                "entry_price": _fp,
                                "stop_loss": _sl_sync,
                                "take_profit": _tp_sync,
                                "opened_at": _oa,
                                "mode": _md_sync,
                            })
                    await _publish_execution_event(redis, "position_entry_updated", {
                        "ticker": _t, "entry_price": _fp,
                        "opened_at": _oa, "order_id": _oid_str,
                    })
                    await redis.hdel(_INFLIGHT_ENTRY_KEY, _oid_str)
                    await redis.hdel(_FILL_SYNC_ALERTS_KEY, _oid_str)
                    entry_fixes += 1

                elif _t not in current_tickers:
                    # No fill found AND ticker not in IB positions.
                    # Before discarding, check if position:params still exists — if so,
                    # this is likely a pre-market order queued for market open (submitted
                    # MKT order not yet an IB position).  Keep the inflight entry so the
                    # fill callback and periodic poll can handle it on fill.
                    _has_params_sync = await redis.hexists(_POSITION_PARAMS_KEY, _t)
                    if _has_params_sync:
                        logger.debug(
                            "[SYNC] Inflight entry %s orderId=%s: no IB position yet but "
                            "position:params exists — keeping (likely pre-market pending order)",
                            _t, _oid_str,
                        )
                    else:
                        # No params AND no IB position AND no fill — truly stale; discard.
                        logger.debug(
                            "[SYNC] Inflight entry %s orderId=%s: no position + no fill + no params — discarding",
                            _t, _oid_str,
                        )
                        await redis.hdel(_INFLIGHT_ENTRY_KEY, _oid_str)
                        await redis.hdel(_FILL_SYNC_ALERTS_KEY, _oid_str)

                else:
                    # Position still open but fill unconfirmed — escalate to alert
                    _age_min = 0
                    try:
                        _age_min = int((now_utc - datetime.fromisoformat(_oa)).total_seconds() / 60)
                    except Exception:
                        pass
                    severity = "error" if _age_min > 30 else "warning"
                    alert = {
                        "ticker": _t,
                        "order_id": _oid_str,
                        "type": "entry_fill_pending",
                        "opened_at": _oa,
                        "age_minutes": _age_min,
                        "severity": severity,
                        "message": (
                            f"**{_t}**: Entry fill price unconfirmed after {_age_min} min "
                            f"(IB orderId {_oid_str}). "
                            f"The position may be open without a recorded entry price. "
                            f"Options: **Attempt Reconcile** to retry IB lookup, "
                            f"**Close Position** to exit safely, "
                            f"or verify directly in Trader Workstation."
                        ),
                        "updated_at": now_utc.isoformat(),
                    }
                    await redis.hset(_FILL_SYNC_ALERTS_KEY, _oid_str, json.dumps(alert))
                    await redis.expire(_FILL_SYNC_ALERTS_KEY, _FILL_SYNC_ALERT_TTL)
                    logger.warning(
                        "[SYNC] Inflight entry %s orderId=%s unresolved after %dm — alert raised",
                        _t, _oid_str, _age_min,
                    )
            except Exception as _ie:
                logger.debug("[SYNC] Error processing inflight entry %s: %s", _oid_str, _ie)
    except Exception as _exc:
        logger.debug("[SYNC] Inflight entry reconcile failed: %s", _exc)

    # ── Exit inflight reconcile ────────────────────────────────────────────────
    # Build a map of orderId → status for currently active IB orders so we can
    # distinguish "fill not yet arrived" from "fill truly missing".
    _active_order_ids: set[int] = set()
    try:
        from social_trading.execution.ibkr import ORDER_REF as _ORD_REF  # noqa: PLC0415
        _active_statuses_inf = {"PendingSubmit", "PreSubmitted", "Submitted"}
        for _trade in (ib_obj.openTrades() or []):
            _oid_t = getattr(getattr(_trade, "order", None), "orderId", 0)
            _status_t = getattr(getattr(_trade, "orderStatus", None), "status", "")
            _ref_t = getattr(getattr(_trade, "order", None), "orderRef", "")
            if _oid_t and _status_t in _active_statuses_inf and _ref_t == _ORD_REF:
                _active_order_ids.add(int(_oid_t))
    except Exception:
        pass

    try:
        _exit_inf = await redis.hgetall(_INFLIGHT_EXIT_KEY)
        for _k, _v in (_exit_inf or {}).items():
            _oid_str = _k.decode() if isinstance(_k, bytes) else str(_k)
            try:
                _d   = json.loads(_v.decode() if isinstance(_v, bytes) else _v)
                _t   = _d.get("ticker", "")
                _oa  = _d.get("opened_at", "")
                _fp  = _all_fills.get(int(_oid_str), 0.0)
                _prov = float(_d.get("provisional_exit", 0))

                if _fp > 0:
                    logger.info("[SYNC] Inflight exit %s orderId=%s: recovered fill %.4f", _t, _oid_str, _fp)
                    # Full position cleanup — mirrors the _on_close_fill_confirmed callback.
                    # The fill arrived during a disconnect/restart window; the callback
                    # never fired on the dead engine object, so we do the cleanup here.
                    try:
                        if hasattr(engine, "forget_position"):
                            engine.forget_position(_t)
                        await redis.hdel(_POSITION_PARAMS_KEY, _t)
                        await redis.hdel(_HWM_REDIS_KEY, _t)
                        await redis.hdel(_TRAIL_ORDERS_KEY, _t)
                    except Exception as _clean_exc:
                        logger.debug("[SYNC] Position cleanup failed for %s: %s", _t, _clean_exc)
                    await _publish_execution_event(redis, "position_exit_corrected", {
                        "ticker": _t, "exit_price": _fp,
                        "opened_at": _oa, "order_id": _oid_str,
                    })
                    await redis.hdel(_INFLIGHT_EXIT_KEY, _oid_str)
                    await redis.hdel(_FILL_SYNC_ALERTS_KEY, _oid_str)
                    exit_fixes += 1
                elif int(_oid_str) in _active_order_ids:
                    # MKT close order is still active in IB (not yet filled).
                    # Keep the inflight entry — the fill callback or next reconcile
                    # cycle will resolve it.  Do NOT write an alert yet.
                    logger.info(
                        "[SYNC] Inflight exit %s orderId=%s still active in IB — "
                        "keeping pending; will resolve when fill arrives",
                        _t, _oid_str,
                    )
                else:
                    # Fill not found and order no longer active — clean up and write advisory alert.
                    _age_min = 0
                    try:
                        _age_min = int((now_utc - datetime.fromisoformat(_oa)).total_seconds() / 60)
                    except Exception:
                        pass
                    await redis.hdel(_INFLIGHT_EXIT_KEY, _oid_str)
                    if _age_min > 5:
                        alert = {
                            "ticker": _t,
                            "order_id": _oid_str,
                            "type": "exit_fill_pending",
                            "opened_at": _oa,
                            "provisional_exit": _prov,
                            "age_minutes": _age_min,
                            "severity": "warning",
                            "message": (
                                f"**{_t}**: Exit fill price could not be confirmed "
                                f"(IB orderId {_oid_str}, provisional ≈ ${_prov:.2f}). "
                                f"P&L shown may be approximate. "
                                f"Verify the actual fill price in Trader Workstation."
                            ),
                            "updated_at": now_utc.isoformat(),
                        }
                        await redis.hset(_FILL_SYNC_ALERTS_KEY, _oid_str, json.dumps(alert))
                        await redis.expire(_FILL_SYNC_ALERTS_KEY, _FILL_SYNC_ALERT_TTL)
            except Exception as _ie:
                logger.debug("[SYNC] Error processing inflight exit %s: %s", _oid_str, _ie)
    except Exception as _exc:
        logger.debug("[SYNC] Inflight exit reconcile failed: %s", _exc)

    if entry_fixes or exit_fixes:
        logger.info("[SYNC] Inflight reconcile complete: %d entry fix(es), %d exit fix(es)", entry_fixes, exit_fixes)
    return entry_fixes, exit_fixes


async def _prune_old_data() -> None:
    """
    Delete aged-out rows from high-volume tables.
    Retention policy:
      social_raw / sentiment_scores  — 14 days
      sentiment_aggregates / signals — 30 days  (signals linked to trades are kept)
      market_data                    — 180 days
      account_equity                 — 365 days
    trades / positions / config_runs are never pruned.
    """
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "trading"),
            user=os.getenv("DB_USER", "trader"),
            password=os.getenv("DB_PASSWORD", ""),
        )
        with conn, conn.cursor() as cur:
            # sentiment_scores must be deleted BEFORE social_raw (FK constraint)
            cur.execute("""
                DELETE FROM sentiment_scores
                WHERE scored_at < NOW() - INTERVAL '14 days'
            """)
            del_sent = cur.rowcount

            cur.execute("""
                DELETE FROM social_raw
                WHERE created_at < NOW() - INTERVAL '14 days'
            """)
            del_raw = cur.rowcount

            cur.execute("""
                DELETE FROM sentiment_aggregates
                WHERE window_start < NOW() - INTERVAL '30 days'
            """)
            del_agg = cur.rowcount

            # Keep signals that are referenced by any trade row
            cur.execute("""
                DELETE FROM signals
                WHERE generated_at < NOW() - INTERVAL '30 days'
                  AND id NOT IN (
                      SELECT signal_id FROM trades
                      WHERE signal_id IS NOT NULL
                  )
            """)
            del_sig = cur.rowcount

            cur.execute("""
                DELETE FROM market_data
                WHERE timestamp < NOW() - INTERVAL '180 days'
            """)
            del_md = cur.rowcount

            cur.execute("""
                DELETE FROM account_equity
                WHERE timestamp < NOW() - INTERVAL '365 days'
            """)
            del_eq = cur.rowcount

        conn.close()
        logger.info(
            "[PRUNE] Done — social_raw: %d, sentiment_scores: %d, "
            "sentiment_aggregates: %d, signals: %d, market_data: %d, account_equity: %d",
            del_raw, del_sent, del_agg, del_sig, del_md, del_eq,
        )
    except Exception as exc:
        logger.error("[PRUNE] Failed: %s", exc, exc_info=True)


async def _save_eod_snapshot(cfg: SystemConfig, mode: str) -> None:
    """
    Compute today's session metrics from the DB and write one config_runs row.
    Called once after market close (~16:05 ET) and again on clean shutdown.
    Safe to call multiple times — UPSERT on (run_date, mode).

    Exit-reason mapping (runtime names → config_runs columns):
        STOP_LOSS           → exits_atr_stop
        TAKE_PROFIT         → exits_take_profit
        TRAILING_STOP       → exits_trailing_stop
        SENTIMENT_REVERSAL  → exits_sentiment_reversal
        MENTION_DECAY       → exits_mention_decay
        TIME_STOP           → exits_time_stop
        everything else     → exits_manual
    """
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "trading"),
            user=os.getenv("DB_USER", "trader"),
            password=os.getenv("DB_PASSWORD", ""),
        )
        today = datetime.now(UTC).date().isoformat()
        with conn, conn.cursor() as cur:
            # ── Closed trades today ───────────────────────────────────────────
            cur.execute("""
                SELECT
                    COUNT(*)                                          AS total_trades,
                    SUM(net_pnl)                                      AS total_pnl,
                    SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END)     AS win_count,
                    AVG(EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600)
                                                                      AS avg_hold_hours,
                    -- Exit reason breakdown
                    SUM(CASE WHEN exit_reason = 'TAKE_PROFIT'         THEN 1 ELSE 0 END) AS tp,
                    SUM(CASE WHEN exit_reason = 'TIME_STOP'           THEN 1 ELSE 0 END) AS ts,
                    SUM(CASE WHEN exit_reason = 'STOP_LOSS'           THEN 1 ELSE 0 END) AS sl,
                    SUM(CASE WHEN exit_reason = 'TRAILING_STOP'       THEN 1 ELSE 0 END) AS tr,
                    SUM(CASE WHEN exit_reason = 'SENTIMENT_REVERSAL'  THEN 1 ELSE 0 END) AS sr,
                    SUM(CASE WHEN exit_reason = 'MENTION_DECAY'       THEN 1 ELSE 0 END) AS md,
                    SUM(CASE WHEN exit_reason NOT IN (
                        'TAKE_PROFIT','TIME_STOP','STOP_LOSS',
                        'TRAILING_STOP','SENTIMENT_REVERSAL','MENTION_DECAY'
                    ) THEN 1 ELSE 0 END)                              AS manual,
                    STDDEV(net_pnl)                                   AS pnl_std
                FROM trades
                WHERE closed_at::date = %(today)s
                  AND mode = %(mode)s
                  AND exit_price IS NOT NULL
            """, {"today": today, "mode": mode})
            row = cur.fetchone()
            total_trades   = int(row[0] or 0)
            total_pnl      = float(row[1] or 0)
            win_count      = int(row[2] or 0)
            avg_hold_hours = float(row[3] or 0)
            exits_tp       = int(row[4] or 0)
            exits_ts       = int(row[5] or 0)
            exits_sl       = int(row[6] or 0)
            exits_tr       = int(row[7] or 0)
            exits_sr       = int(row[8] or 0)
            exits_md       = int(row[9] or 0)
            exits_manual   = int(row[10] or 0)
            pnl_std        = float(row[11] or 0)

            win_rate     = (win_count / total_trades) if total_trades else None
            avg_daily_pnl = total_pnl / total_trades if total_trades else 0
            sharpe       = (avg_daily_pnl / pnl_std * (252 ** 0.5)) if pnl_std > 0 else None
            # Max drawdown from cumulative PnL curve
            cur.execute("""
                SELECT net_pnl FROM trades
                WHERE closed_at::date = %(today)s AND mode = %(mode)s AND exit_price IS NOT NULL
                ORDER BY closed_at
            """, {"today": today, "mode": mode})
            pnls = [r[0] for r in cur.fetchall() if r[0] is not None]
            max_dd = 0.0
            if pnls:
                peak = 0.0
                cumulative = 0.0
                for p in pnls:
                    cumulative += float(p)
                    peak = max(peak, cumulative)
                    max_dd = max(max_dd, (peak - cumulative) / (abs(peak) + 1e-9))
            profit_factor_val = None
            gross_wins = sum(float(p) for p in pnls if float(p) > 0)
            gross_losses = sum(abs(float(p)) for p in pnls if float(p) < 0)
            if gross_losses > 0:
                profit_factor_val = round(gross_wins / gross_losses, 4)

            # ── Signal funnel today ───────────────────────────────────────────
            cur.execute("""
                SELECT
                    COUNT(*)                                                AS generated,
                    SUM(CASE WHEN executed = TRUE THEN 1 ELSE 0 END)       AS executed,
                    AVG(quality_score)                                      AS avg_quality,
                    AVG(mention_zscore)                                     AS avg_zscore
                FROM signals
                WHERE generated_at::date = %(today)s
            """, {"today": today})
            sig_row = cur.fetchone()
            sig_generated = int(sig_row[0] or 0)
            sig_executed  = int(sig_row[1] or 0)
            avg_quality   = float(sig_row[2]) if sig_row[2] is not None else None
            avg_zscore    = float(sig_row[3]) if sig_row[3] is not None else None

        conn.close()

        metrics = {
            "total_pnl":                round(total_pnl, 2),
            "total_trades":             total_trades,
            "win_count":                win_count,
            "win_rate":                 round(win_rate, 4) if win_rate is not None else None,
            "sharpe_ratio":             round(sharpe, 4) if sharpe is not None else None,
            "max_drawdown":             round(max_dd, 4),
            "avg_hold_hours":           round(avg_hold_hours, 2),
            "profit_factor":            profit_factor_val,
            "exits_take_profit":        exits_tp,
            "exits_time_stop":          exits_ts,
            "exits_atr_stop":           exits_sl,   # STOP_LOSS → atr_stop column
            "exits_trailing_stop":      exits_tr,
            "exits_sentiment_reversal": exits_sr,
            "exits_mention_decay":      exits_md,
            "exits_manual":             exits_manual,
            "signals_generated":        sig_generated,
            "signals_executed":         sig_executed,
            "avg_signal_quality":       round(avg_quality, 4) if avg_quality is not None else None,
            "avg_mention_zscore":       round(avg_zscore, 2) if avg_zscore is not None else None,
        }
        await cfg.save_run_snapshot(metrics, mode=mode)
        logger.info(
            "[EOD] Snapshot saved: mode=%s trades=%d win_rate=%s total_pnl=%.2f",
            mode, total_trades,
            f"{win_rate:.1%}" if win_rate is not None else "—",
            total_pnl,
        )
    except Exception as exc:
        logger.error("[EOD] Failed to save session snapshot: %s", exc, exc_info=True)


async def _reconcile_external_closes(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
    prev_open: set[str],
    now_open: set[str],
    just_closed: set[str],
    mode: str = "live",
    pending_close: set[str] | None = None,
    all_ib_tickers: set[str] | None = None,
) -> None:
    """
    Detect tickers that disappeared from IB positions without this service closing them.

    These are positions filled by IB's bracket legs (stop-loss or take-profit
    executed natively) or closed manually in TWS.  Clean up Redis state and log.

    Attempts to infer the actual exit price and reason (STOP_LOSS, TAKE_PROFIT,
    TRAILING_STOP) from IB fill records for the current session.  Falls back to
    IB_EXTERNAL when no matching fill is found (e.g. position closed offline).
    """
    # Exclude tickers with a pending close order — their fill callback handles cleanup.
    # Also exclude tickers that were immediately filled by a UI command this cycle —
    # _command_close_position already published position_closed for them.
    _cmd_closed: set[str] = set()
    try:
        _cmd_raw = await redis.smembers(_CMD_CLOSED_KEY)
        _cmd_closed = {(k.decode() if isinstance(k, bytes) else k) for k in (_cmd_raw or [])}
    except Exception:
        pass
    _excluded = just_closed | (pending_close or set()) | _cmd_closed
    externally_closed = prev_open - now_open - _excluded
    for ticker in externally_closed:
        # Safety check: if IB still shows this position as open, it was NOT externally
        # closed — params may have been deleted without a close (e.g. bug or manual edit).
        # Skip cleanup and log a warning so the operator can run reconcile to re-adopt.
        if all_ib_tickers is not None and ticker in all_ib_tickers:
            logger.warning(
                "[SYNC] %s appears in externally_closed but is still open in IB — "
                "skipping cleanup to avoid false position_closed event; "
                "run Reconcile to re-adopt this position",
                ticker,
            )
            continue
        # Read params BEFORE cleanup so we can include them in the close event
        opened_at = datetime.now(UTC).isoformat()
        direction = "unknown"
        entry_price = 0.0
        shares = 0
        stop_loss = 0.0
        take_profit = 0.0
        params_raw = await redis.hget(_POSITION_PARAMS_KEY, ticker)
        if params_raw:
            try:
                params = json.loads(
                    params_raw.decode() if isinstance(params_raw, bytes) else params_raw
                )
                opened_at = params.get("opened_at", opened_at)
                direction = params.get("direction", direction)
                entry_price = float(params.get("entry_price", 0.0))
                shares = int(params.get("shares", 0))
                stop_loss = float(params.get("stop_loss", 0.0))
                take_profit = float(params.get("take_profit", 0.0))
            except Exception:
                pass

        # ── Infer exit price and reason from IB order/fill records ────────────
        # Method 1: filled OCA order in ib.trades() (most accurate, current session)
        # Method 2: closing-side fills in ib.fills() (current session cache)
        # Method 3: reqExecutionsAsync() — server-side, survives TWS restarts/reconnects
        #           (same approach as _reconcile_startup; handles the common case where
        #           the local session cache is empty due to a recent reconnect or the
        #           race between the position-gone update and the fill event arriving)
        exit_price = 0.0
        exit_reason = "IB_EXTERNAL"
        try:
            ib_obj = getattr(engine, "_ib", None)
            if ib_obj is not None:
                close_side = "SLD" if direction == "LONG" else "BOT"

                # ── Method 1: OCA order type (most accurate) ──────────────────
                # ib.trades() includes all orders this session: open, filled,
                # cancelled, inactive.  When an OCA leg fills, the other legs
                # become Inactive/Cancelled.  The filled leg has our orderRef.
                _oca_classified = False
                for trade in ib_obj.trades():
                    sym = getattr(getattr(trade, "contract", None), "symbol", "")
                    if sym != ticker:
                        continue
                    ref = getattr(getattr(trade, "order", None), "orderRef", "")
                    if ref != "social_trading":
                        continue
                    status = getattr(getattr(trade, "orderStatus", None), "status", "")
                    if status != "Filled":
                        continue
                    ot = getattr(getattr(trade, "order", None), "orderType", "")
                    trade_fills = getattr(trade, "fills", [])
                    if trade_fills:
                        # Compute VWAP across all partial fills
                        _tv = sum(
                            float(getattr(getattr(f, "execution", None), "price", 0)) *
                            max(float(getattr(getattr(f, "execution", None), "shares", 0) or 0), 1.0)
                            for f in trade_fills
                        )
                        _tq = sum(
                            max(float(getattr(getattr(f, "execution", None), "shares", 0) or 0), 1.0)
                            for f in trade_fills
                        )
                        exit_price = _tv / _tq if _tq > 0 else float(trade_fills[-1].execution.price)
                    if ot in ("STP", "STP LMT"):
                        exit_reason = "STOP_LOSS"
                    elif ot == "LMT":
                        exit_reason = "TAKE_PROFIT"
                    elif ot == "TRAIL":
                        exit_reason = "TRAILING_STOP"
                    else:
                        continue  # not a bracket leg — skip
                    _oca_classified = True
                    break

                # ── Method 2: fill-price vs ATR SL/TP levels (fallback) ───────
                if not _oca_classified:
                    fills = [
                        f for f in ib_obj.fills()
                        if getattr(getattr(f, "contract", None), "symbol", "") == ticker
                        and getattr(getattr(f, "execution", None), "side", "") == close_side
                    ]
                    if fills:
                        # Compute VWAP across all partial fills for this ticker+side
                        _fv = sum(
                            float(getattr(getattr(f, "execution", None), "price", 0)) *
                            max(float(getattr(getattr(f, "execution", None), "shares", 0) or 0), 1.0)
                            for f in fills
                        )
                        _fq = sum(
                            max(float(getattr(getattr(f, "execution", None), "shares", 0) or 0), 1.0)
                            for f in fills
                        )
                        exit_price = _fv / _fq if _fq > 0 else float(fills[-1].execution.price)
                        # Only classify against ATR SL / TP — avoid TRAILING_STOP
                        # as a catch-all since the trail level is not stored in params.
                        if stop_loss > 0 and take_profit > 0:
                            sl_dist = abs(exit_price - stop_loss)
                            tp_dist = abs(exit_price - take_profit)
                            tolerance = exit_price * 0.005  # 0.5%
                            if sl_dist <= tolerance or sl_dist <= tp_dist:
                                exit_reason = "STOP_LOSS"
                            elif tp_dist <= tolerance or tp_dist < sl_dist:
                                exit_reason = "TAKE_PROFIT"
                            # else: stays IB_EXTERNAL — don't guess TRAILING_STOP
                        else:
                            exit_reason = "IB_EXTERNAL"

                # ── Method 3: reqExecutionsAsync — server-side (survives reconnect) ──
                # Local caches (ib.trades/fills) are cleared on TWS restart or reconnect.
                # reqExecutionsAsync queries IB's server for today's executions and is
                # authoritative even when the session cache is empty.
                if exit_price == 0.0:
                    try:
                        from ib_async import ExecutionFilter  # noqa: PLC0415
                        executions = await ib_obj.reqExecutionsAsync(ExecutionFilter())
                        # Accumulate partial fills for this ticker+close_side → VWAP
                        _ev, _eq = 0.0, 0.0
                        for fill in executions:
                            sym = getattr(getattr(fill, "contract", None), "symbol", "")
                            side = getattr(getattr(fill, "execution", None), "side", "")
                            price = getattr(getattr(fill, "execution", None), "price", 0.0)
                            qty   = float(getattr(getattr(fill, "execution", None), "shares", 0) or 0)
                            if sym != ticker or side != close_side or not price:
                                continue
                            _ev += price * max(qty, 1.0)
                            _eq += max(qty, 1.0)
                        if _eq > 0:
                            exit_price = _ev / _eq
                            # Also try to classify via order type from completed orders
                            try:
                                completed = await ib_obj.reqCompletedOrdersAsync(apiOnly=False)
                                for order_state in completed:
                                    _sym = getattr(getattr(order_state, "contract", None), "symbol", "")
                                    _ref = getattr(getattr(order_state, "order", None), "orderRef", "")
                                    _ot  = getattr(getattr(order_state, "order", None), "orderType", "")
                                    _st  = getattr(getattr(order_state, "orderStatus", None), "status", "")
                                    if _sym != ticker or _ref != "social_trading" or _st != "Filled":
                                        continue
                                    if _ot in ("STP", "STP LMT"):
                                        exit_reason = "STOP_LOSS"
                                    elif _ot == "LMT":
                                        exit_reason = "TAKE_PROFIT"
                                    elif _ot == "TRAIL":
                                        exit_reason = "TRAILING_STOP"
                                    avg = getattr(getattr(order_state, "orderStatus", None), "avgFillPrice", 0.0)
                                    if avg:
                                        exit_price = float(avg)  # prefer avgFillPrice for VWAP accuracy
                                    break
                            except Exception:
                                pass
                        if exit_price > 0:
                            logger.info(
                                "[SYNC] %s exit price recovered via reqExecutionsAsync: %.4f (%s)",
                                ticker, exit_price, exit_reason,
                            )
                    except Exception as _ex3:
                        logger.debug("[SYNC] reqExecutionsAsync fallback failed for %s: %s", ticker, _ex3)
        except Exception as exc:
            logger.debug("[SYNC] Could not infer exit details for %s: %s", ticker, exc)

        # ── UI alert when exit price is still unknown ─────────────────────────
        # Write to alerts:fill_sync so the positions page shows a warning and the
        # user can trigger a manual reconcile from the UI.
        if exit_price == 0.0:
            import time as _time  # noqa: PLC0415
            try:
                alert_payload = json.dumps({
                    "order_id":   f"ext_{ticker}_{int(_time.time())}",
                    "ticker":     ticker,
                    "type":       "exit_fill_missing",
                    "severity":   "warning",
                    "message":    (
                        f"{ticker} was closed externally by IB (OCA bracket or TWS) "
                        f"but the exit fill price could not be retrieved. "
                        f"P&L for this trade will be inaccurate until resolved. "
                        f"Click 'Attempt Reconcile' to re-query IB."
                    ),
                    "created_at": datetime.now(UTC).isoformat(),
                })
                _alert_key = f"ext_{ticker}_{int(_time.time())}"
                await redis.hset(_FILL_SYNC_ALERTS_KEY, _alert_key, alert_payload)
                await redis.expire(_FILL_SYNC_ALERTS_KEY, _FILL_SYNC_ALERT_TTL)
                logger.warning(
                    "[SYNC] %s: exit fill price unknown — fill_sync alert written", ticker,
                )
            except Exception as _ae:
                logger.debug("[SYNC] Failed to write fill_sync alert for %s: %s", ticker, _ae)


        engine.forget_position(ticker)
        await redis.hdel(_HWM_REDIS_KEY, ticker)
        await redis.hdel(_POSITION_PARAMS_KEY, ticker)
        await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
        POSITIONS_CLOSED.labels(reason=exit_reason).inc()

        # Clean up any exits:inflight entry for this ticker.
        # The close_position callback may have written an inflight entry that
        # never fired (fill arrived during a disconnect window).  Since we've
        # now detected the close via _reconcile_external_closes we can remove
        # the stale ledger entry — the position_closed event we're about to
        # publish carries the actual exit price.
        try:
            _ei = await redis.hgetall(_INFLIGHT_EXIT_KEY)
            for _k, _v in (_ei or {}).items():
                _oid = _k.decode() if isinstance(_k, bytes) else str(_k)
                try:
                    _d = json.loads(_v.decode() if isinstance(_v, bytes) else _v)
                    if _d.get("ticker") == ticker:
                        await redis.hdel(_INFLIGHT_EXIT_KEY, _oid)
                        await redis.hdel(_FILL_SYNC_ALERTS_KEY, _oid)
                except Exception:
                    pass
        except Exception:
            pass

        logger.info(
            "[SYNC] %s closed externally by IB: reason=%s exit_price=%.4f",
            ticker, exit_reason, exit_price,
        )
        await _publish_execution_event(redis, "position_closed", {
            "ticker": ticker,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "direction": direction,
            "entry_price": entry_price,
            "shares": shares,
            "closed_at": datetime.now(UTC).isoformat(),
            "opened_at": opened_at,
            "mode": mode,
        })
        await redis.lpush("trades:recent", json.dumps({
            "ticker": ticker,
            "direction": direction,
            "exit_reason": exit_reason,
            "closed_at": datetime.now(UTC).isoformat(),
        }))
        await redis.ltrim("trades:recent", 0, 999)


async def _command_close_position(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
    ticker: str,
    reason: str,
    mode: str = "live",
) -> None:
    """
    Close a position from a UI command (CLOSE_ALL / CLOSE_TICKER).

    Unlike the exit loop, command-triggered closes must handle the deferred-fill
    case: if the market order is submitted but not immediately filled (e.g., after
    hours), we write to exits:inflight and register a fill callback so the position
    is not silently orphaned until the next startup reconcile.
    """
    # Capture params before close_position() may clear engine state.
    params: dict = {}
    if hasattr(engine, "get_position_params"):
        params = engine.get_position_params().get(ticker, {})  # type: ignore[union-attr]
    _pos_opened_at   = params.get("opened_at", datetime.now(UTC).isoformat())
    _pos_entry       = float(params.get("entry_price", 0.0))
    _pos_shares      = int(params.get("shares", 0))
    _pos_direction   = params.get("direction", "unknown")

    close_result = await engine.close_position(ticker, reason=reason)

    if close_result and close_result.status == "rejected":
        logger.warning("[CMD] close_position for %s rejected: %s", ticker, close_result.error or "unknown")
        return

    POSITIONS_CLOSED.labels(reason=reason).inc()

    if close_result and close_result.fill_price is not None:
        # Immediate fill confirmed — full cleanup now.
        if hasattr(engine, "forget_position"):
            engine.forget_position(ticker)
        await redis.hdel(_POSITION_PARAMS_KEY, ticker)
        await redis.hdel(_HWM_REDIS_KEY, ticker)
        await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
        # Mark as cmd-closed so _reconcile_external_closes skips it this cycle
        try:
            await redis.sadd(_CMD_CLOSED_KEY, ticker)
            await redis.expire(_CMD_CLOSED_KEY, _CMD_CLOSED_TTL_SEC)
        except Exception:
            pass
        await _publish_execution_event(redis, "position_closed", {
            "ticker": ticker,
            "exit_price": close_result.fill_price,
            "exit_reason": reason,
            "shares": _pos_shares,
            "direction": _pos_direction,
            "entry_price": _pos_entry,
            "closed_at": datetime.now(UTC).isoformat(),
            "opened_at": _pos_opened_at,
            "mode": mode,
        })
    else:
        # Deferred fill — write inflight ledger so startup reconcile can recover.
        _exit_order_id = int(close_result.order_id) if (close_result and close_result.order_id) else 0
        _provisional   = engine.get_price(ticker) or _pos_entry  # type: ignore[union-attr]
        if _exit_order_id and hasattr(engine, "register_order_fill_callback"):
            try:
                await redis.hset(
                    _INFLIGHT_EXIT_KEY, str(_exit_order_id),
                    json.dumps({
                        "ticker": ticker,
                        "opened_at": _pos_opened_at,
                        "entry_price": _pos_entry,
                        "shares": _pos_shares,
                        "direction": _pos_direction,
                        "provisional_exit": _provisional,
                    }),
                )
                await redis.expire(_INFLIGHT_EXIT_KEY, _INFLIGHT_TTL_SEC)
            except Exception as _inf_exc:
                logger.debug("[CMD] Failed to write inflight exit for %s: %s", ticker, _inf_exc)

            _ex_ticker    = ticker
            _ex_opened_at = _pos_opened_at
            _ex_entry     = _pos_entry
            _ex_shares    = _pos_shares
            _ex_dir       = _pos_direction
            _ex_oid_str   = str(_exit_order_id)

            async def _on_cmd_fill(
                actual_fill: float,
                _t:      str   = _ex_ticker,
                _oa:     str   = _ex_opened_at,
                _entry:  float = _ex_entry,
                _shares: int   = _ex_shares,
                _dir:    str   = _ex_dir,
                _r:      str   = reason,
                _oid:    str   = _ex_oid_str,
            ) -> None:
                logger.info("[CMD] Fill confirmed for %s: %.4f (orderId=%s)", _t, actual_fill, _oid)
                try:
                    if hasattr(engine, "forget_position"):
                        engine.forget_position(_t)
                    await redis.hdel(_POSITION_PARAMS_KEY, _t)
                    await redis.hdel(_HWM_REDIS_KEY, _t)
                    await redis.hdel(_TRAIL_ORDERS_KEY, _t)
                    await redis.hdel(_INFLIGHT_EXIT_KEY, _oid)
                    await _publish_execution_event(redis, "position_closed", {
                        "ticker": _t,
                        "exit_price": actual_fill,
                        "exit_reason": _r,
                        "shares": _shares,
                        "direction": _dir,
                        "entry_price": _entry,
                        "closed_at": datetime.now(UTC).isoformat(),
                        "opened_at": _oa,
                        "mode": mode,
                    })
                    POSITIONS_CLOSED.labels(reason=_r).inc()
                except Exception as _cb_exc:
                    logger.warning("[CMD] Error in fill callback for %s: %s", _t, _cb_exc)

            engine.register_order_fill_callback(_exit_order_id, _on_cmd_fill)  # type: ignore[union-attr]
        logger.info(
            "[CMD] %s close order submitted (orderId=%s) — awaiting fill",
            ticker, _exit_order_id or "?",
        )


# ── UI command listener ────────────────────────────────────────────────────────

async def _delete_hash_entries_for_ticker(redis: aioredis.Redis, key: str, ticker: str) -> None:
    try:
        raw = await redis.hgetall(key)
    except Exception:
        return
    if not raw:
        return
    to_delete: list[str] = []
    for field, value in raw.items():
        field_text = _decode_text(field)
        payload = _parse_json_value(value, {})
        if isinstance(payload, dict) and str(payload.get("ticker", "")).upper() == ticker:
            to_delete.append(field_text)
        elif field_text.upper() == ticker:
            to_delete.append(field_text)
    if to_delete:
        await redis.hdel(key, *to_delete)


async def run_command_listener(engine: Optional[ExecutionEngine], redis: aioredis.Redis) -> None:
    """
    Subscribe to the Redis pub/sub channel "trading:commands" and honour
    control messages published by the Streamlit UI.

    Supported commands (see streamlit/utils/redis_ctrl.py):
      HALT_NEW      — stop opening new positions (sets _halt_flag)
      RESUME        — re-enable new positions (clears _halt_flag)
      CLOSE_ALL     — immediately close every open position
      CLOSE_TICKER  — close one ticker  (payload: {"ticker": "AAPL"})
      CONFIG_UPDATED — hint only; no action needed (services reload on next cycle)
    """
    pubsub = redis.pubsub()
    await pubsub.subscribe("trading:commands")
    logger.info("Command listener subscribed to trading:commands")

    async for raw in pubsub.listen():
        if raw["type"] != "message":
            continue
        try:
            data = raw["data"]
            if isinstance(data, bytes):
                data = data.decode()
            msg = json.loads(data)
        except Exception as exc:
            logger.warning("Malformed command message: %s", exc)
            continue

        cmd = msg.get("cmd", "")
        payload = msg.get("payload", {})
        logger.info("UI command received: %s  payload=%s", cmd, payload)

        active_engine = engine if engine is not None else _ACTIVE_ENGINE

        if cmd == "HALT_NEW":
            _halt_flag.set()
            logger.warning("New positions HALTED via UI command")
        elif cmd == "RESUME":
            _halt_flag.clear()
            logger.info("New positions RESUMED via UI command")
        elif cmd == "CLOSE_ALL":
            if active_engine is None:
                logger.warning("Command %s ignored — no IB engine available", cmd)
                continue
            tickers = list(active_engine.open_tickers)
            logger.warning("CLOSE_ALL: closing %d positions", len(tickers))
            for ticker in tickers:
                try:
                    await _command_close_position(redis, active_engine, ticker, reason="UI:CLOSE_ALL", mode=mode)
                    logger.info("Closed %s via CLOSE_ALL", ticker)
                except Exception as exc:
                    logger.error("Failed to close %s: %s", ticker, exc)
        elif cmd == "CLOSE_TICKER":
            if active_engine is None:
                logger.warning("Command %s ignored — no IB engine available", cmd)
                continue
            ticker = payload.get("ticker", "")
            if not ticker:
                logger.warning("CLOSE_TICKER missing ticker in payload")
            elif ticker not in active_engine.open_tickers:
                logger.info("CLOSE_TICKER: %s not in open positions — ignoring", ticker)
            else:
                try:
                    await _command_close_position(redis, active_engine, ticker, reason="UI:CLOSE_TICKER", mode=mode)
                    logger.info("Closed %s via CLOSE_TICKER", ticker)
                except Exception as exc:
                    logger.error("Failed to close %s: %s", ticker, exc)
        elif cmd == "CONFIG_UPDATED":
            logger.info("CONFIG_UPDATED received — config will reload on next cycle")
        elif cmd == "REFRESH_SYNC":
            if active_engine is None:
                logger.warning("Command %s ignored — no IB engine available", cmd)
                continue
            logger.info("REFRESH_SYNC: running on-demand inflight order reconcile")
            try:
                _cur = active_engine.open_tickers if hasattr(active_engine, "open_tickers") else set()
                ef, xf = await _reconcile_inflight_orders(redis, active_engine, _cur)
                logger.info("REFRESH_SYNC complete: %d entry fix(es), %d exit fix(es)", ef, xf)
                await redis.setex(
                    "sync:last_reconcile", 300,
                    json.dumps({"entry_fixes": ef, "exit_fixes": xf, "ts": datetime.now(UTC).isoformat()}),
                )
            except Exception as exc:
                logger.warning("REFRESH_SYNC failed: %s", exc)
        elif cmd == "FULL_RECONCILE":
            if active_engine is None:
                logger.warning("Command %s ignored — no IB engine available", cmd)
                continue
            # Re-run the full startup reconcile on demand (not just inflight orders).
            # This re-queries IB positions + today's fills and re-evaluates every
            # persisted position:params entry, updating positions:pending_reconcile.
            logger.info("FULL_RECONCILE: running full startup reconcile on demand")
            try:
                _mode_raw = await redis.get("trading:mode") or "live"
                _mode = _mode_raw.decode() if isinstance(_mode_raw, bytes) else _mode_raw
                _ib_acct_raw = await redis.get("ib:account") or ""
                _ib_acct = _ib_acct_raw.decode() if isinstance(_ib_acct_raw, bytes) else _ib_acct_raw
                summary = await _reconcile_ib_state(
                    redis, active_engine, mode=_mode, ib_account=_ib_acct,
                )
                await redis.setex(
                    "sync:last_reconcile", 300,
                    json.dumps({"full": True, "ts": datetime.now(UTC).isoformat()}),
                )
                logger.info(
                    "FULL_RECONCILE complete — matched=%d adopted=%d conflicts=%d",
                    summary.get("matched", 0),
                    summary.get("adopted", 0),
                    len(summary.get("conflicts", [])),
                )
            except Exception as exc:
                logger.warning("FULL_RECONCILE failed: %s", exc)
        elif cmd == "RESOLVE_PENDING_CLOSE":
            # User confirms a pending-reconcile position was closed (no fill price known).
            # Publishes position_closed with exit_price=0, removes from all tracking.
            ticker = payload.get("ticker", "")
            if not ticker:
                logger.warning("RESOLVE_PENDING_CLOSE missing ticker in payload")
            else:
                try:
                    pending_raw = await redis.hget(_PENDING_RECONCILE_KEY, ticker)
                    p: dict = {}
                    if pending_raw:
                        try:
                            p = json.loads(pending_raw.decode() if isinstance(pending_raw, bytes) else pending_raw)
                        except Exception:
                            pass
                    # Also fall back to position:params for metadata
                    if not p:
                        params_raw = await redis.hget(_POSITION_PARAMS_KEY, ticker)
                        if params_raw:
                            try:
                                p = json.loads(params_raw.decode() if isinstance(params_raw, bytes) else params_raw)
                            except Exception:
                                pass
                    if active_engine is None:
                        logger.warning("Command %s ignored — no IB engine available", cmd)
                        continue
                    if hasattr(active_engine, "forget_position"):
                        active_engine.forget_position(ticker)  # type: ignore[union-attr]
                    await redis.hdel(_PENDING_RECONCILE_KEY, ticker)
                    await redis.hdel(_POSITION_PARAMS_KEY, ticker)
                    await redis.hdel(_HWM_REDIS_KEY, ticker)
                    await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
                    await _publish_execution_event(redis, "position_closed", {
                        "ticker": ticker,
                        "exit_price": 0.0,
                        "exit_reason": "USER_RESOLVED_NO_PRICE",
                        "direction": p.get("direction", "unknown"),
                        "entry_price": float(p.get("entry_price", 0.0)),
                        "shares": int(p.get("shares", 0)),
                        "closed_at": datetime.now(UTC).isoformat(),
                        "opened_at": p.get("opened_at", datetime.now(UTC).isoformat()),
                        "mode": await redis.get("trading:mode") or "live",
                    })
                    logger.info("RESOLVE_PENDING_CLOSE: %s marked as closed by user", ticker)
                except Exception as exc:
                    logger.warning("RESOLVE_PENDING_CLOSE failed for %s: %s", ticker, exc)
        elif cmd == "RESOLVE_PENDING_DELETE":
            # User removes a pending-reconcile position from the app entirely.
            # No position_closed event is published — this is a pure app-state cleanup.
            ticker = payload.get("ticker", "")
            if not ticker:
                logger.warning("RESOLVE_PENDING_DELETE missing ticker in payload")
            else:
                try:
                    if active_engine is None:
                        logger.warning("Command %s ignored — no IB engine available", cmd)
                        continue
                    if hasattr(active_engine, "forget_position"):
                        active_engine.forget_position(ticker)  # type: ignore[union-attr]
                    await redis.hdel(_PENDING_RECONCILE_KEY, ticker)
                    await redis.hdel(_POSITION_PARAMS_KEY, ticker)
                    await redis.hdel(_HWM_REDIS_KEY, ticker)
                    await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
                    logger.info("RESOLVE_PENDING_DELETE: %s removed from app by user", ticker)
                except Exception as exc:
                    logger.warning("RESOLVE_PENDING_DELETE failed for %s: %s", ticker, exc)
        elif cmd == "RECONCILE_SKIP":
            # Legacy command — reconcile no longer requires user approval.
            # The periodic reconcile loop (run_reconcile_loop) handles all state sync
            # automatically. This handler is kept for backward compat with older UI.
            logger.info("RECONCILE_SKIP received (legacy) — reconcile is now automatic; no-op")
            if active_engine is not None:
                try:
                    await _write_positions_to_redis(redis, active_engine)
                except Exception:
                    pass
        elif cmd == "RECONCILE_APPROVE":
            # Legacy command — reconcile no longer requires user approval.
            # The periodic reconcile loop handles all state sync automatically.
            logger.info("RECONCILE_APPROVE received (legacy) — reconcile is now automatic; no-op")
            if active_engine is not None:
                try:
                    await _write_positions_to_redis(redis, active_engine)
                except Exception:
                    pass
        elif cmd == "RESOLVE_CONFLICT":
            # Manually resolve a reconcile conflict for a specific ticker.
            # Payload: {ticker: str, action: "mark_closed"|"remove_app"|"use_ib_direction"}
            # After resolution, removes the ticker from reconcile:conflicts so the
            # trade loop is unblocked.
            ticker = str(payload.get("ticker", "")).upper()
            action = str(payload.get("action", "")).lower()
            if not ticker or not action:
                logger.warning("RESOLVE_CONFLICT: missing ticker or action in payload")
                continue
            if active_engine is None:
                logger.warning("RESOLVE_CONFLICT: no IB engine available")
                continue
            try:
                _rc_mode_raw = await redis.get("trading:mode") or "live"
                _rc_mode = _rc_mode_raw.decode() if isinstance(_rc_mode_raw, bytes) else _rc_mode_raw
                if action == "mark_closed":
                    params_raw_rc = await redis.hget(_POSITION_PARAMS_KEY, ticker)
                    p_rc: dict = {}
                    if params_raw_rc:
                        try:
                            p_rc = json.loads(params_raw_rc.decode() if isinstance(params_raw_rc, bytes) else params_raw_rc)
                        except Exception:
                            pass
                    if hasattr(active_engine, "forget_position"):
                        active_engine.forget_position(ticker)  # type: ignore[union-attr]
                    await redis.hdel(_POSITION_PARAMS_KEY, ticker)
                    await redis.hdel(_HWM_REDIS_KEY, ticker)
                    await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
                    await redis.hdel(_POSITIONS_LIVE_KEY, ticker)
                    await _publish_execution_event(redis, "position_closed", {
                        "ticker": ticker,
                        "exit_price": 0.0,
                        "exit_reason": "USER_RESOLVED_CONFLICT",
                        "direction": p_rc.get("direction", "unknown"),
                        "entry_price": float(p_rc.get("entry_price", 0.0)),
                        "shares": int(float(p_rc.get("shares", 0))),
                        "closed_at": datetime.now(UTC).isoformat(),
                        "opened_at": p_rc.get("opened_at", datetime.now(UTC).isoformat()),
                        "mode": _rc_mode,
                    })
                    logger.info("RESOLVE_CONFLICT: %s marked as closed by user", ticker)
                elif action == "remove_app":
                    if hasattr(active_engine, "forget_position"):
                        active_engine.forget_position(ticker)  # type: ignore[union-attr]
                    await redis.hdel(_POSITION_PARAMS_KEY, ticker)
                    await redis.hdel(_HWM_REDIS_KEY, ticker)
                    await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
                    await redis.hdel(_POSITIONS_LIVE_KEY, ticker)
                    logger.info("RESOLVE_CONFLICT: %s removed from app state by user", ticker)
                elif action == "use_ib_direction":
                    conflict_raw = await redis.hget(_RECONCILE_CONFLICTS_KEY, ticker)
                    conflict_data: dict = {}
                    if conflict_raw:
                        try:
                            conflict_data = json.loads(conflict_raw.decode() if isinstance(conflict_raw, bytes) else conflict_raw)
                        except Exception:
                            pass
                    ib_dir = str(conflict_data.get("ib_direction", "")).upper()
                    ib_shares = int(float(conflict_data.get("ib_shares", 0) or 0))
                    if ib_dir:
                        params_raw_rc = await redis.hget(_POSITION_PARAMS_KEY, ticker)
                        p_rc = {}
                        if params_raw_rc:
                            try:
                                p_rc = json.loads(params_raw_rc.decode() if isinstance(params_raw_rc, bytes) else params_raw_rc)
                            except Exception:
                                pass
                        p_rc["direction"] = ib_dir
                        if ib_shares > 0:
                            p_rc["shares"] = ib_shares
                        await redis.hset(_POSITION_PARAMS_KEY, ticker, json.dumps(p_rc, default=str))
                        _ep = getattr(active_engine, "_position_params", None)
                        if _ep and ticker in _ep:
                            _ep[ticker]["direction"] = ib_dir
                            if ib_shares > 0:
                                _ep[ticker]["shares"] = ib_shares
                        logger.info("RESOLVE_CONFLICT: %s direction corrected to %s", ticker, ib_dir)
                    else:
                        logger.warning("RESOLVE_CONFLICT: no IB direction for %s", ticker)
                        continue
                else:
                    logger.warning("RESOLVE_CONFLICT: unknown action %r for %s", action, ticker)
                    continue
                await redis.hdel(_RECONCILE_CONFLICTS_KEY, ticker)
                await _write_positions_to_redis(redis, active_engine)
                logger.info("RESOLVE_CONFLICT: conflict cleared for %s — trade loop may resume", ticker)
            except Exception as exc:
                logger.warning("RESOLVE_CONFLICT failed for %s: %s", ticker, exc)
        elif cmd == "RECONCILE_DELETE_POSITION":
            ticker = str(payload.get("ticker", "")).upper()
            if not ticker:
                logger.warning("RECONCILE_DELETE_POSITION missing ticker in payload")
                continue
            # Require IB engine: positions must only be modified when IB is available
            # so the deletion can be confirmed against live IB state (cancel open orders).
            # Without this guard, the user could silently wipe app state while offline
            # and leave an open IB position with no app tracking after reconnect.
            if active_engine is None:
                logger.warning("Command %s ignored — no IB engine available", cmd)
                continue
            try:
                if hasattr(active_engine, "_ib"):
                    from social_trading.execution.ibkr import ORDER_REF  # noqa: PLC0415
                    for trade in active_engine._ib.openTrades() or []:  # type: ignore[union-attr]
                        contract = getattr(trade, "contract", None)
                        order = getattr(trade, "order", None)
                        if (
                            getattr(contract, "symbol", "").upper() == ticker
                            and getattr(order, "orderRef", "") == ORDER_REF
                        ):
                            try:
                                active_engine._ib.cancelOrder(order)  # type: ignore[union-attr]
                            except Exception as exc:
                                logger.debug("RECONCILE_DELETE cancel failed for %s: %s", ticker, exc)
                await redis.hdel(_POSITION_PARAMS_KEY, ticker)
                await redis.hdel(_HWM_REDIS_KEY, ticker)
                await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
                await redis.hdel(_POSITIONS_LIVE_KEY, ticker)
                await redis.hdel(_PENDING_RECONCILE_KEY, ticker)
                await _delete_hash_entries_for_ticker(redis, _FILL_SYNC_ALERTS_KEY, ticker)
                await _delete_hash_entries_for_ticker(redis, _INFLIGHT_ENTRY_KEY, ticker)
                await _delete_hash_entries_for_ticker(redis, _INFLIGHT_EXIT_KEY, ticker)
                if hasattr(active_engine, "forget_position"):
                    active_engine.forget_position(ticker)  # type: ignore[union-attr]
                await _publish_execution_event(redis, "position_deleted", {
                    "ticker": ticker,
                    "deleted_at": datetime.now(UTC).isoformat(),
                    "reason": "USER_DELETED_AT_RECONCILE",
                })
                logger.info("RECONCILE_DELETE: %s deleted by user at reconcile", ticker)
            except Exception as exc:
                logger.warning("RECONCILE_DELETE_POSITION failed for %s: %s", ticker, exc)
        elif cmd == "ADOPT_IB_POSITION":
            # Adopt an IB position that has no system params (manual_ib / orphan).
            # Seeds position:params from IB portfolio data + ATR; places OCA bracket.
            ticker = str(payload.get("ticker", "")).upper()
            if not ticker:
                logger.warning("ADOPT_IB_POSITION missing ticker in payload")
                continue
            try:
                if active_engine is None or not hasattr(active_engine, "_ib"):
                    logger.warning("ADOPT_IB_POSITION: no IB engine available")
                    continue
                # Get live position data from IB
                ib_positions = active_engine._ib.positions()  # type: ignore[union-attr]
                ib_pos = next((p for p in ib_positions if getattr(getattr(p, "contract", None), "symbol", "") == ticker), None)
                if ib_pos is None:
                    logger.warning("ADOPT_IB_POSITION: %s not found in IB positions", ticker)
                    continue
                qty = int(ib_pos.position)
                direction: str = "LONG" if qty > 0 else "SHORT"
                avg_cost = float(ib_pos.avgCost)
                entry_price = round(avg_cost, 4) if avg_cost > 0 else 0.0
                # Get ATR for SL/TP reconstruction
                cfg_adopt = await SystemConfig.load(redis)
                atr = 0.0
                try:
                    mkt_raw = await redis.hgetall(f"market_data:{ticker}")
                    atr_val = mkt_raw.get(b"atr_14") or mkt_raw.get("atr_14")
                    if atr_val:
                        atr = float(atr_val.decode() if isinstance(atr_val, bytes) else atr_val)
                except Exception:
                    pass
                stop_loss_a = 0.0
                take_profit_a = 0.0
                if atr > 0 and entry_price > 0:
                    if direction == "LONG":
                        stop_loss_a = round(entry_price - cfg_adopt.atr_multiplier * atr, 2)
                        take_profit_a = round(entry_price * (1.0 + cfg_adopt.take_profit_pct), 2)
                    else:
                        stop_loss_a = round(entry_price + cfg_adopt.atr_multiplier * atr, 2)
                        take_profit_a = round(entry_price * (1.0 - cfg_adopt.take_profit_pct), 2)
                opened_at = datetime.now(UTC).isoformat()
                seeded: dict = {
                    "stop_loss": stop_loss_a,
                    "take_profit": take_profit_a,
                    "opened_at": opened_at,
                    "direction": direction,
                    "source": "system",
                    "entry_price": entry_price,
                    "shares": abs(qty),
                    "oca_group": "",
                    "trailing_stop_pct_applied": cfg_adopt.trailing_stop_pct,
                }
                active_engine.seed_position_params(ticker, seeded)  # type: ignore[union-attr]
                await redis.hset(_POSITION_PARAMS_KEY, ticker, json.dumps(seeded))
                # Use stable fingerprint (entry_price + qty) so repeated service
                # restarts don't create duplicate DB rows. The ADOPT_IB_POSITION
                # command can be re-sent safely (e.g. after reconnect).
                _adopt_fp = f"{entry_price:.4f}:{abs(qty)}"
                _adopt_flag = f"position:adopted:{ticker}:{_adopt_fp}"
                if await redis.set(_adopt_flag, "1", nx=True, ex=86400 * 90):
                    await _publish_execution_event(redis, "position_opened", {
                        "ticker": ticker,
                        "direction": direction,
                        "shares": abs(qty),
                        "entry_price": entry_price,
                        "stop_price": stop_loss_a,
                        "target_price": take_profit_a,
                        "opened_at": opened_at,
                        "mode": await redis.get("trading:mode") or "live",
                    })
                logger.info(
                    "ADOPT_IB_POSITION: %s adopted — entry=%.4f sl=%.2f tp=%.2f (user action)",
                    ticker, entry_price, stop_loss_a, take_profit_a,
                )
            except Exception as exc:
                logger.warning("ADOPT_IB_POSITION failed for %s: %s", ticker, exc)


# ── Entry point ────────────────────────────────────────────────────────────────

def _set_runtime_task(name: str, coro: object) -> asyncio.Task[None]:
    existing = _RUNTIME_TASKS.get(name)
    if existing is not None and not existing.done():
        existing.cancel()
    task = asyncio.create_task(coro, name=name)  # type: ignore[arg-type]
    _RUNTIME_TASKS[name] = task
    return task


async def _wait_for_runtime_tasks() -> None:
    while True:
        active = {name: task for name, task in _RUNTIME_TASKS.items() if not task.done()}
        if not active:
            return
        done, _ = await asyncio.wait(active.values(), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task_name = next((name for name, current in _RUNTIME_TASKS.items() if current is task), task.get_name())
            if task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                raise exc
            if task_name != "exec:reconnect":
                raise RuntimeError(f"{task_name} exited unexpectedly")


async def _warmup_market_data_task(
    redis: aioredis.Redis,
    market_data: MarketDataProvider,
) -> None:
    try:
        wl_raw = await redis.zrange("watchlist:active", 0, -1)
        wl_tickers: list[str] = [
            t.decode() if isinstance(t, bytes) else t for t in wl_raw
        ]
        if not wl_tickers:
            return
        logger.info("[EXEC] Warming up market data for %d watchlist tickers…", len(wl_tickers))
        count = 0
        for ticker in wl_tickers:
            try:
                snap = await _write_market_snapshot_and_get_price(redis, ticker, market_data)
                if snap is not None:
                    count += 1
            except Exception as exc:
                logger.debug("[EXEC] Warmup failed for %s: %s", ticker, exc)
            await asyncio.sleep(0.1)
        logger.info("[EXEC] Market data warmup complete: %d/%d tickers populated", count, len(wl_tickers))
    except Exception as exc:
        logger.warning("[EXEC] Market data warmup error: %s", exc)


async def _prime_ib_open_orders(ib: object) -> None:
    try:
        ib.reqAllOpenOrders()
        await asyncio.sleep(3)
        n_orders = len(ib.openTrades())
        logger.info("[IBKR] Open orders loaded at startup: %d order(s)", n_orders)
    except Exception as exc:
        logger.debug("[IBKR] reqAllOpenOrders at startup failed: %s", exc)


async def _initialize_connected_runtime(
    redis: aioredis.Redis,
    ib: object,
    port: int,
    client_id: int,
    ib_account: str,
    mode: str,
) -> tuple[ExecutionEngine, MarketDataProvider, PositionExitManager, CircuitBreaker, TradingEventBus, SystemConfig]:
    """
    Build and initialise all runtime components for a live IB connection.

    Called on EVERY successful IB connect (including reconnects after a disconnect).
    Always runs the full startup reconcile: collects a snapshot of app vs IB state,
    persists it for the UI, and sets reconcile:state to "awaiting_approval" so the
    trade and exit loops block until the user approves (or skips) reconcile.

    This means the user sees the reconcile screen once per IB connection session,
    not just once per app launch.  _RECONCILE_DONE is reset to False by the
    reconnect watcher before each connect attempt.
    """
    global _ACTIVE_ENGINE

    from social_trading.execution.ibkr import IBKRExecutionEngine  # noqa: PLC0415
    from social_trading.market_data.ibkr import IBKRMarketData  # noqa: PLC0415

    cfg = await SystemConfig.load(redis)
    engine = IBKRExecutionEngine(  # type: ignore[assignment]
        ib=ib,
        account=ib_account,
        host="127.0.0.1",
        port=port,
        client_id=client_id,
    )
    market_data: MarketDataProvider = FallbackMarketData(
        primary=IBKRMarketData(ib=ib),
        secondary=YFinanceMarketData(),
    )
    logger.info(
        "Connected to IBKR port=%d clientId=%d account=%s (IB market data primary, yfinance fallback)",
        port, client_id, ib_account or "(auto)",
    )

    exit_manager = PositionExitManager()
    breaker = CircuitBreaker(redis)
    bus = TradingEventBus(redis)

    await redis.set("trading:mode", mode)
    await _load_hwm_from_redis(redis, engine)
    await _load_position_params_from_redis(redis, engine)
    await _load_trail_orders_from_redis(redis, engine)

    # Run the full reconcile immediately on every IB connect/reconnect.
    # _reconcile_ib_state() auto-resolves all safe state transitions,
    # writes reconcile:full for the UI, and writes any unresolvable
    # conflicts to reconcile:conflicts (non-empty halts the trade loop).
    # This replaces the old _collect_reconcile_data + awaiting_approval flow.
    reconcile_summary = await _reconcile_ib_state(
        redis, engine, mode=mode, ib_account=ib_account,
    )
    if reconcile_summary.get("skipped"):
        logger.warning(
            "[RECONCILE] Startup reconcile skipped: %s", reconcile_summary.get("reason"),
        )
    else:
        conflict_list = reconcile_summary.get("conflicts", [])
        logger.info(
            "[RECONCILE] Startup complete — matched=%d adopted=%d "
            "closed_offline=%d fill_fixed=%d conflicts=%d",
            reconcile_summary.get("matched", 0),
            reconcile_summary.get("adopted", 0),
            reconcile_summary.get("closed_offline", 0),
            reconcile_summary.get("fill_pending_fixed", 0),
            len(conflict_list),
        )
        if conflict_list:
            logger.warning(
                "[RECONCILE] Conflicts detected — trade loop halted until resolved: %s",
                conflict_list,
            )

    await _prune_old_data()
    asyncio.create_task(_warmup_market_data_task(redis, market_data), name="exec:market_warmup")
    await _write_account_state(redis, engine)
    # Write positions:live immediately at startup so the UI shows open positions
    # during the reconcile screen and immediately after approval.  Without this,
    # positions:live (5-min TTL) can expire while the service is offline and only
    # gets re-written after the first exit-loop cycle (up to 60s after approval).
    await _write_positions_to_redis(redis, engine)
    _ACTIVE_ENGINE = engine
    return engine, market_data, exit_manager, breaker, bus, cfg


async def _run_ib_reconnect_watcher(
    redis: aioredis.Redis,
    port: int,
    client_id: int,
    ib_account: str,
    mode: str,
) -> None:
    """
    Persistent IB connection manager — runs for the lifetime of the service.

    Outer loop (connect loop): attempts IB connection with 30s retries until success.
    On every successful connect (first connect OR reconnect after disconnect):
      1. Resets _RECONCILE_DONE[0] = False
      2. Calls _initialize_connected_runtime — full reconcile, blocking UI
      3. Starts/replaces exec:trade, exec:exit, exec:price_push tasks
      4. Enters inner monitoring loop — polls ib.isConnected() every 15s

    On disconnect detection: cancels current task instances by replacing them,
    disconnects IB cleanly, and restarts the outer connect loop.

    This ensures the reconcile UI is shown on every IB connection session, not
    just once per app launch.
    """
    from ib_async import IB  # noqa: PLC0415

    _IB_RETRY_SECS = 30
    _MONITOR_SECS = 15

    while True:  # outer: reconnect on every disconnect
        ib = None
        _RECONCILE_DONE[0] = False

        # ── Connect phase (retry until success) ──────────────────────────────
        while True:
            try:
                ib = IB()
                await ib.connectAsync("127.0.0.1", port, clientId=client_id)
                await ib.reqPositionsAsync()
                await ib.reqAccountUpdatesAsync(account=ib_account or "")
                logger.info("[IBKR] Reconnect watcher: connected to port %d clientId=%d", port, client_id)
                break  # connected — proceed to initialize
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                try:
                    if ib is not None:
                        ib.disconnect()
                except Exception:
                    pass
                ib = None
                await redis.setex("service:heartbeat", 60, "1")
                await redis.set("ib:connected", "0")
                logger.warning(
                    "[IBKR] Reconnect watcher could not connect to port %d: %s — retrying in %ds",
                    port, exc, _IB_RETRY_SECS,
                )
                await asyncio.sleep(_IB_RETRY_SECS)

        # ── Initialize runtime with full blocking reconcile ───────────────────
        try:
            engine, market_data, exit_manager, breaker, bus, cfg = await _initialize_connected_runtime(
                redis=redis,
                ib=ib,
                port=port,
                client_id=client_id,
                ib_account=ib_account,
                mode=mode,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("[IBKR] Reconnect watcher: failed to initialize runtime: %s — will retry", exc)
            try:
                if ib is not None:
                    ib.disconnect()
            except Exception:
                pass
            await asyncio.sleep(_IB_RETRY_SECS)
            continue  # restart outer loop

        # Start/replace trade, price-eval, and reconcile tasks for this IB connection session
        _set_runtime_task(
            "exec:trade",
            run_trade_loop(bus, engine, redis, market_data, mode=mode, cfg=cfg),
        )
        _set_runtime_task(
            "exec:price_eval",
            run_price_eval_loop(engine, exit_manager, market_data, breaker, redis, mode=mode),
        )
        _set_runtime_task(
            "exec:reconcile",
            run_reconcile_loop(engine, redis, mode=mode, ib_account=ib_account),
        )
        _set_runtime_task("exec:price_push", _run_price_push(engine, redis))
        logger.info("[IBKR] Reconnect watcher: trade, price-eval, and reconcile loops started")

        # ── Monitor phase: wait for disconnect ────────────────────────────────
        while True:
            await asyncio.sleep(_MONITOR_SECS)
            try:
                still_connected = ib.isConnected()
            except Exception:
                still_connected = False
            if not still_connected:
                logger.warning(
                    "[IBKR] Reconnect watcher: IB disconnected — "
                    "cancelling current loops and reconnecting with full reconcile"
                )
                try:
                    ib.disconnect()
                except Exception:
                    pass
                # _set_runtime_task cancels the running tasks and replaces them
                # with placeholders that will be overwritten on the next connect.
                # The trade/exit loops block on "awaiting_approval" anyway, so
                # there is no risk of missed signals during the reconnect window.
                break  # exit monitor loop → restart outer connect loop


async def _run_price_push(engine: Optional[ExecutionEngine], redis: aioredis.Redis) -> None:
    """
    Lightweight task: update unrealized_pnl in positions:live every 5 seconds
    using the engine's in-memory price cache (already kept fresh by the exit loop).

    The exit loop writes positions:live once per full cycle (up to 60s cadence).
    This task fills the gap so the UI always shows prices that are at most 5s old,
    without the overhead of a full IB market-data batch fetch.
    """
    _PUSH_INTERVAL = 5
    while True:
        try:
            active_engine = engine if engine is not None else _ACTIVE_ENGINE
            if active_engine is not None:
                live_raw = await redis.hgetall(_POSITIONS_LIVE_KEY)
                if live_raw:
                    # Prefer the live IB portfolio subscription (real-time, pushed
                    # every few seconds by IB's account subscription) over the
                    # exit-loop price cache (updated only every ~60s).
                    portfolio_prices: dict[str, float] = {}
                    if hasattr(active_engine, "get_portfolio_prices"):
                        try:
                            portfolio_prices = active_engine.get_portfolio_prices()  # type: ignore[union-attr]
                        except Exception:
                            pass
                    updates: dict[str, str] = {}
                    for field, val in live_raw.items():
                        t = field.decode() if isinstance(field, bytes) else field
                        try:
                            pos_data = json.loads(val.decode() if isinstance(val, bytes) else val)
                            # Portfolio prices take priority; fall back to engine cache
                            current_price = portfolio_prices.get(t) or active_engine.get_price(t)  # type: ignore[union-attr]
                            if current_price and current_price > 0:
                                # Seed the engine price cache so the exit loop also
                                # benefits from the portfolio-fresh price.
                                if hasattr(active_engine, "set_price"):
                                    try:
                                        active_engine.set_price(t, current_price)  # type: ignore[union-attr]
                                    except Exception:
                                        pass
                                entry = float(pos_data.get("entry_price", 0) or 0)
                                shares = int(pos_data.get("shares", 0) or 0)
                                direction = pos_data.get("direction", "LONG")
                                if entry > 0 and shares > 0:
                                    if direction == "LONG":
                                        upnl = round((current_price - entry) * shares, 2)
                                    else:
                                        upnl = round((entry - current_price) * shares, 2)
                                    pos_data["unrealized_pnl"] = upnl
                                    updates[t] = json.dumps(pos_data)
                        except Exception:
                            pass
                    if updates:
                        await redis.hset(_POSITIONS_LIVE_KEY, mapping=updates)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[PRICE_PUSH] Error: %s", exc)
        await asyncio.sleep(_PUSH_INTERVAL)


async def _run_heartbeat(engine: Optional[ExecutionEngine], redis: aioredis.Redis) -> None:
    """
    Lightweight heartbeat task: runs every 10 seconds, writes two Redis keys:
      service:heartbeat  (TTL=30s) — presence means the service is alive
      ib:connected       (TTL=30s) — "1" connected, "0" disconnected

    Keeping this in a dedicated task (instead of the 60s exit loop) means
    the UI always gets a fresh status regardless of how long exit-loop cycles take.
    If the service dies, both keys expire within 30 seconds and the UI shows
    "Service offline" / "Unknown" immediately.
    """
    _HB_TTL = 30
    _HB_INTERVAL = 10
    while True:
        try:
            active_engine = engine if engine is not None else _ACTIVE_ENGINE
            connected = (await active_engine.health_check()) if active_engine is not None else False
            await redis.setex("service:heartbeat", _HB_TTL, "1")
            await redis.setex("ib:connected", _HB_TTL, "1" if connected else "0")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[HEARTBEAT] Error: %s", exc)
        await asyncio.sleep(_HB_INTERVAL)


async def main() -> None:
    global _ACTIVE_ENGINE, _RECONCILE_DONE

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)
    _ACTIVE_ENGINE = None
    _RECONCILE_DONE = [False]  # reset per-session flag on every service start
    _RUNTIME_TASKS.clear()

    start_metrics_server(port=int(os.getenv("METRICS_PORT", "8000")))

    # Demote known benign IB wrapper errors that are handled by our fallback
    # logic — keeps logs clean without losing genuinely unexpected errors.
    import logging as _logging  # noqa: PLC0415
    class _IBBenignFilter(_logging.Filter):
        def filter(self, record: _logging.LogRecord) -> bool:
            msg = record.getMessage()
            # Error 162 = HMDS no data — handled by yfinance fallback in composite.py
            if "Error 162" in msg and "Historical Market Data Service" in msg:
                record.levelno = _logging.DEBUG
                record.levelname = "DEBUG"
            # Error 10167 = delayed data warning — expected for non-subscribed tickers
            if "Error 10167" in msg:
                record.levelno = _logging.DEBUG
                record.levelname = "DEBUG"
            return True
    _logging.getLogger("ib_async.wrapper").addFilter(_IBBenignFilter())

    cfg = await SystemConfig.load(redis)
    logger.info("SystemConfig loaded (hash=%s)", cfg.config_hash())

    # Clear any stale reconcile state left over from a prior service run.
    # A previous session's "approved" must not suppress reconcile for this session.
    await redis.delete(_RECONCILE_STATE_KEY)

    from ib_async import IB as _IB_unused  # noqa: PLC0415, F401 — keep import for type checks elsewhere

    port = int(os.getenv("IBKR_PORT", "7497"))
    client_id = int(os.getenv("IBKR_CLIENT_ID", "10"))
    ib_account = os.getenv("IBKR_ACCOUNT", "").strip()
    if ib_account.upper().startswith("DFQ"):
        raise ValueError(
            f"IBKR_ACCOUNT={ib_account!r} is a Financial Advisor master account. "
            "This app only supports individual user accounts. "
            "Set IBKR_ACCOUNT to one of the sub-accounts (e.g. DUQ…)."
        )

    mode = "live"
    await redis.set("trading:mode", mode)

    # Always start the persistent reconnect watcher as the sole IB connection manager.
    # It handles first connect, full blocking reconcile, and all future reconnects.
    # heartbeat and cmd listener start immediately (heartbeat shows "disconnected"
    # until the watcher establishes the first connection).
    await redis.setex("service:heartbeat", 60, "1")
    await redis.set("ib:connected", "0")
    await redis.set(_RECONCILE_STATE_KEY, "skipped_no_ib", ex=_RECONCILE_TTL)
    _set_runtime_task("exec:heartbeat", _run_heartbeat(None, redis))
    _set_runtime_task("exec:cmd", run_command_listener(None, redis))
    _set_runtime_task("exec:reconnect", _run_ib_reconnect_watcher(redis, port, client_id, ib_account, mode))

    def _shutdown(sig: int, _frame: object) -> None:
        logger.info("Signal %d received — shutting down execution service", sig)
        for task in list(_RUNTIME_TASKS.values()):
            task.cancel()

    os_signal.signal(os_signal.SIGTERM, _shutdown)
    os_signal.signal(os_signal.SIGINT, _shutdown)

    logger.info("Execution service started")
    try:
        await _wait_for_runtime_tasks()
    except asyncio.CancelledError:
        logger.info("Execution service stopped")
    finally:
        # Save EOD snapshot on shutdown so every session is captured even if
        # the service is stopped before the 16:05 window fires.
        try:
            cfg_final = await SystemConfig.load(redis)
            await _save_eod_snapshot(cfg_final, mode)
        except Exception as exc:
            logger.error("[EOD] Shutdown snapshot failed: %s", exc, exc_info=True)
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
