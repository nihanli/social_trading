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
) -> None:
    """Publish a position lifecycle event to the execution:events stream."""
    try:
        fields = {"event": event_type}
        fields.update({k: str(v) if v is not None else "" for k, v in data.items()})
        await redis.xadd(
            _EXEC_EVENTS_STREAM, fields,
            maxlen=STREAM_MAXLEN.get(_EXEC_EVENTS_STREAM, 50_000),
            approximate=True,
        )
    except Exception as exc:
        logger.warning("[EVENTS] Failed to publish %s event: %s", event_type, exc)


async def _write_positions_to_redis(
    redis: aioredis.Redis,
    engine: ExecutionEngine,
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
        # Preserve the existing positions:live key rather than wiping it and
        # making positions vanish from the UI until the reconnect handler
        # reseeds the IB cache via reqPositionsAsync().
        if not positions:
            params_raw = await redis.hgetall(_POSITION_PARAMS_KEY)
            if params_raw:
                logger.warning(
                    "[POSITIONS] IB returned 0 open positions but %d ticker(s) in "
                    "position:params — possible transient disconnect; preserving "
                    "positions:live until IB cache is reseeded",
                    len(params_raw),
                )
                return

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

        # Any IB position whose ticker is in system_params is included.
        # Any system_params ticker NOT in IB cache is included with params data
        # and flagged so the UI knows the cache may be stale for that ticker.
        pipe = redis.pipeline()
        pipe.delete(_POSITIONS_LIVE_KEY)
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
                pipe.hset(_POSITIONS_LIVE_KEY, ticker, json.dumps({
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
                pipe.hset(_POSITIONS_LIVE_KEY, ticker, json.dumps({
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
                }))
        await pipe.execute()
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

                    # Skip if position already open; guard engine.open_tickers
                    # against IB disconnection errors
                    try:
                        already_open = signal.ticker in engine.open_tickers
                    except Exception as exc:
                        logger.warning(
                            "[EXEC] Could not check open_tickers for %s (%s) — skipping signal",
                            signal.ticker, exc,
                        )
                        await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)
                        continue

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
      6. Reconcile: detect tickers closed externally by IB (bracket fills / TWS)
      7. Persist HWM + position params; write account state to Redis
    """
    logger.info("Execution exit loop started")

    WATCHLIST_REFRESH_SECS = 300  # 5 minutes
    POSITION_FULL_SNAPSHOT_SECS = 300  # full ATR/OHLCV refresh cadence for open positions
    _watchlist_last_refresh: dict[str, float] = {}
    _position_last_full_snapshot: dict[str, float] = {}
    # Tracks tickers that were open at the end of the previous cycle.
    # Used to detect positions closed externally by IB between cycles.
    # Pre-seeded from startup positions so the first cycle can detect external closures
    # that happened while the service was offline (Fix 4).
    try:
        _initial_positions = await engine.get_positions()
        _prev_open_tickers: set[str] = {p.ticker for p in _initial_positions}
    except Exception:
        _prev_open_tickers = set()
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

    # Track when the IB position cache was last refreshed so we can force a
    # reqPositionsAsync() periodically to prevent cache drift.
    _IB_CACHE_REFRESH_SECS = 300  # 5 minutes
    _last_ib_cache_refresh: float = 0.0

    # Reconnect backoff: only attempt reconnect once every 60 seconds to avoid
    # hammering TWS during a prolonged outage.
    _RECONNECT_INTERVAL_SECS = 60
    _last_reconnect_attempt: float = 0.0

    while True:
        try:
            cfg = await SystemConfig.load(redis)

            # ── 1. Connection guard ───────────────────────────────────────────
            _connected = await engine.health_check()

            if not _connected:
                now_ts = asyncio.get_event_loop().time()
                if hasattr(engine, "reconnect") and (now_ts - _last_reconnect_attempt) >= _RECONNECT_INTERVAL_SECS:
                    _last_reconnect_attempt = now_ts
                    logger.info("[SYNC] IB disconnected — attempting reconnect…")
                    reconnected = await engine.reconnect()  # type: ignore[union-attr]
                    if reconnected:
                        logger.info("[SYNC] IB reconnected — resuming position evaluation")
                        _last_ib_cache_refresh = now_ts  # mark cache as fresh
                        # fall through to normal cycle
                    else:
                        logger.warning("[SYNC] IB reconnect failed — will retry in %ds", _RECONNECT_INTERVAL_SECS)
                        await asyncio.sleep(cfg.signal_poll_interval_sec)
                        continue
                else:
                    secs_until_retry = max(0, _RECONNECT_INTERVAL_SECS - int(now_ts - _last_reconnect_attempt))
                    logger.warning(
                        "[SYNC] Engine not connected — skipping position evaluation "
                        "(next reconnect attempt in %ds)",
                        secs_until_retry,
                    )
                    await asyncio.sleep(cfg.signal_poll_interval_sec)
                    continue

            # ── 1b. Periodic IB position cache refresh ────────────────────────
            # ib_async caches positions locally and updates them via fill events.
            # If fills are missed (e.g. during a reconnect window) the cache drifts.
            # Force reqPositionsAsync() every 5 minutes to stay current.
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

            # ── 2a. Open position prices: batch-fetch from IB ─────────────────
            # Use engine.get_market_prices() for a single concurrent IB snapshot
            # of all open positions.  This gives real-time prices without the
            # per-ticker 1.5s sleep of the yfinance/reqMktData approach.
            # Tickers where IB returns no price fall back to yfinance below.
            ib_prices: dict[str, float] = {}
            if open_tickers:
                try:
                    ib_prices = await engine.get_market_prices(list(open_tickers))
                except Exception as exc:
                    logger.debug("IB batch price fetch failed: %s", exc)

            for ticker in open_tickers:
                if ticker in ib_prices:
                    # IB returned a live price — update engine cache and Redis
                    engine.set_price(ticker, ib_prices[ticker])
                    # Write just the price fields to Redis (fast path).
                    # Full snapshot (ATR/OHLCV) runs on a slower cadence below.
                    try:
                        await redis.hset(f"market_data:{ticker}", mapping={
                            "last": str(ib_prices[ticker]),
                            "updated_at": datetime.now(UTC).isoformat(),
                            "source": "ib",
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
            await _write_positions_to_redis(redis, engine)
            now = datetime.now(UTC)
            just_closed: set[str] = set()

            # ── 3a. Naked position check ──────────────────────────────────────
            # Detect positions with no live server-side STP/TRAIL orders.
            # Attempt reattach; fall back to immediate close if that fails.
            # Tickers handled here are excluded from this cycle's exit evaluation
            # to avoid race conditions with just-placed orders.
            naked_closed, naked_reattached = await _check_naked_positions(
                redis, engine, open_positions, system_params, cfg, mode,
            )
            if naked_closed:
                just_closed.update(naked_closed)
                await _write_positions_to_redis(redis, engine)
            handled_this_cycle = naked_closed | naked_reattached
            open_positions_for_eval = [
                p for p in open_positions if p.ticker not in handled_this_cycle
            ]

            for pos in open_positions_for_eval:
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
                    await engine.close_position(pos.ticker, reason=decision.reason)
                    just_closed.add(pos.ticker)
                    _trailing_pct_applied.pop(pos.ticker, None)
                    await _publish_execution_event(redis, "position_closed", {
                        "ticker": pos.ticker,
                        "exit_price": current_price,
                        "exit_reason": decision.reason or "unknown",
                        "shares": pos.shares,
                        "direction": pos.direction,
                        "entry_price": pos.entry_price,
                        "closed_at": datetime.now(UTC).isoformat(),
                        "opened_at": pos.opened_at.isoformat() if pos.opened_at else "",
                        "mode": mode,
                    })
                    await redis.hdel(_HWM_REDIS_KEY, pos.ticker)
                    await redis.hdel(_POSITION_PARAMS_KEY, pos.ticker)
                    POSITIONS_CLOSED.labels(reason=decision.reason or "unknown").inc()
                    logger.info(
                        "[EXIT] %s %s reason=%s pnl_approx=%.2f",
                        pos.direction, pos.ticker, decision.reason,
                        (current_price - pos.entry_price) * pos.shares
                        if pos.direction == "LONG"
                        else (pos.entry_price - current_price) * pos.shares,
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

            # ── 4. Reconcile external IB closes ──────────────────────────────
            # Tickers that were open last cycle but are now gone (and we didn't
            # close them) were filled by IB's bracket legs or closed in TWS.
            # Use the full open_positions list (including naked-handled ones) so
            # external closes are tracked regardless of how the position ended.
            #
            # IMPORTANT: only run this when IB is connected.  If the IB cache
            # returned 0 positions due to a disconnect / TWS restart, all
            # previously open tickers would look "externally closed" and their
            # params would be wiped — leaving positions permanently untracked
            # even after reconnection.
            _ib_connected = (
                hasattr(engine, "_ib") and engine._ib.isConnected()  # type: ignore[union-attr]
            )
            now_open_tickers = {p.ticker for p in open_positions} - just_closed
            if _prev_open_tickers and _ib_connected:
                externally_closed = _prev_open_tickers - now_open_tickers - just_closed
                await _reconcile_external_closes(
                    redis, engine,
                    prev_open=_prev_open_tickers,
                    now_open=now_open_tickers,
                    just_closed=just_closed,
                    mode=mode,
                )
                # Immediately remove externally-closed tickers from positions:live
                # so the UI reflects the change without waiting for the next cycle.
                if externally_closed:
                    await _write_positions_to_redis(redis, engine)
            elif _prev_open_tickers and not _ib_connected:
                logger.warning(
                    "[EXIT] IB disconnected — skipping external-close reconcile "
                    "to avoid wiping %d position param(s)",
                    len(_prev_open_tickers),
                )
            # Only advance _prev_open_tickers when IB is connected so we don't
            # lose track of open positions during a disconnect window.
            if _ib_connected:
                _prev_open_tickers = now_open_tickers

            # ── 5. Persist state + metrics ────────────────────────────────────
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
    """Persist position params (sl/tp/opened_at) to Redis so exit rules survive restarts."""
    import json as _json  # noqa: PLC0415
    try:
        params = engine.get_position_params()  # type: ignore[union-attr]
        if not params:
            return
        mapping = {ticker: _json.dumps(p) for ticker, p in params.items()}
        await redis.hset(_POSITION_PARAMS_KEY, mapping=mapping)
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

    # ── Build set of tickers with live *protective* OCA orders ───────────────
    # A ticker is "protected" only if IB has at least one active STP/TRAIL order
    # placed by this system (orderRef == ORDER_REF).  A bare LMT (TP-only) is not
    # a protective order — it cannot prevent unlimited loss.
    _protective_order_types = {"STP", "STP LMT", "TRAIL"}
    _done_statuses = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
    protected_tickers: set[str] = set()
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
    except Exception as exc:
        logger.warning("[NAKED] Could not inspect IB open trades: %s — skipping check", exc)
        return just_closed, just_reattached  # cannot safely assess

    naked = [p for p in open_positions if p.ticker not in protected_tickers]
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
            except Exception as exc:
                logger.error("[NAKED] %s: emergency close also failed: %s", ticker, exc)

    return just_closed, just_reattached


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
    # Both keyed by symbol → (exit_price, exit_reason).
    _offline_exit: dict[str, tuple[float, str]] = {}
    # Tickers where this system placed an entry (MKT) order, confirmed via
    # reqCompletedOrdersAsync.  Used to reclassify orphaned positions that
    # have no remaining open orders (e.g. OCA failed) but were system-opened.
    _system_entry_tickers: set[str] = set()
    try:
        ib_obj = getattr(engine, "_ib", None)
        if ib_obj is not None:
            # Step 1: collect fill prices from today's server-side executions
            _fill_prices: dict[str, float] = {}
            from ib_async import ExecutionFilter  # noqa: PLC0415
            executions = await ib_obj.reqExecutionsAsync(ExecutionFilter())
            for fill in executions:
                sym = getattr(getattr(fill, "contract", None), "symbol", "")
                side = getattr(getattr(fill, "execution", None), "side", "")
                price = getattr(getattr(fill, "execution", None), "price", 0.0)
                if sym and price:
                    _fill_prices[f"{sym}:{side}"] = float(price)

            # Step 2: classify by completed order type (most accurate)
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
                    if ot in ("STP", "STP LMT"):
                        _offline_exit[sym] = (float(avg_fill), "STOP_LOSS")
                    elif ot == "LMT":
                        _offline_exit[sym] = (float(avg_fill), "TAKE_PROFIT")
                    elif ot == "TRAIL":
                        _offline_exit[sym] = (float(avg_fill), "TRAILING_STOP")
                    elif ot == "MKT":
                        # Entry market order placed by this system — remember the symbol
                        # so orphaned positions with no open orders are not misclassified
                        # as manual when OCA failed and left no remaining open orders.
                        _system_entry_tickers.add(sym)
            except Exception as exc:
                logger.debug("[SYNC] reqCompletedOrders unavailable: %s", exc)

            # Step 3: for symbols not classified by order type, use fill price
            # from executions but don't guess exit reason beyond SL/TP
            for key, price in _fill_prices.items():
                sym, side = key.split(":", 1)
                if sym not in _offline_exit and price > 0:
                    _offline_exit[sym] = (price, "")  # price known, reason unknown
    except Exception as exc:
        logger.debug("[SYNC] Could not prefetch today's executions: %s", exc)

    # Tickers in persisted params but no longer in IB were closed while offline.
    orphaned = set(params) - current_tickers
    for ticker in orphaned:
        # Read params before deleting so we can record the close event
        p = params.get(ticker, {})
        opened_at = p.get("opened_at", datetime.now(UTC).isoformat())
        direction = p.get("direction", "unknown")
        entry_price = float(p.get("entry_price", 0.0))
        shares = int(p.get("shares", 0))
        stop_loss = float(p.get("stop_loss", 0.0))
        take_profit = float(p.get("take_profit", 0.0))

        # Try to get exit price + reason from IB records
        exit_price, exit_reason = _offline_exit.get(ticker, (0.0, "IB_EXTERNAL"))
        if not exit_reason:
            # Have fill price but no order-type classification.
            # Only classify against ATR SL / TP — don't guess TRAILING_STOP.
            if exit_price > 0 and stop_loss > 0 and take_profit > 0:
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

        engine.forget_position(ticker)  # type: ignore[union-attr]
        await redis.hdel(_HWM_REDIS_KEY, ticker)
        await redis.hdel(_POSITION_PARAMS_KEY, ticker)
        await redis.hdel(_TRAIL_ORDERS_KEY, ticker)
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

    # Fix 1: Recover TRAIL order IDs from IB for ALL current system positions.
    # _ts_order_id is in-memory only; this seeds it for positions whose Redis key
    # was already loaded and for newly adopted ones discovered below.
    if hasattr(engine, "seed_trail_order_id"):
        for sym, oid in trail_order_ids.items():
            engine.seed_trail_order_id(sym, oid)  # type: ignore[union-attr]
            logger.info("[SYNC] %s: recovered TRAIL order ID %d from IB open trades", sym, oid)

    # Fix 4: Reclassify orphaned positions that were opened by this system but have
    # no remaining open orders (e.g. OCA placement failed).  openTrades() alone can't
    # detect these; reqCompletedOrdersAsync() MKT fills with ORDER_REF confirm origin.
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
            _adoption_flag = f"position:adopted:{pos.ticker}:{seeded_params['opened_at']}"
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
                # Publish position_opened so persistence_service creates a trades DB row.
                _adoption_flag = f"position:adopted:{pos.ticker}:{seeded_params['opened_at']}"
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
                # Truly unprotected (no ATR, no IB bracket) — close immediately.
                logger.error(
                    "[SYNC] %s: prior-session system position with no ATR and no IB bracket — "
                    "closing to avoid running unprotected",
                    pos.ticker,
                )
                try:
                    await engine.close_position(pos.ticker, reason="NO_STOP_ON_RESTART")  # type: ignore[union-attr]
                except Exception as exc:
                    logger.error("[SYNC] Failed to close unprotected position %s: %s", pos.ticker, exc)


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
) -> None:
    """
    Detect tickers that disappeared from IB positions without this service closing them.

    These are positions filled by IB's bracket legs (stop-loss or take-profit
    executed natively) or closed manually in TWS.  Clean up Redis state and log.

    Attempts to infer the actual exit price and reason (STOP_LOSS, TAKE_PROFIT,
    TRAILING_STOP) from IB fill records for the current session.  Falls back to
    IB_EXTERNAL when no matching fill is found (e.g. position closed offline).
    """
    externally_closed = prev_open - now_open - just_closed
    for ticker in externally_closed:
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
        # Primary: find the filled OCA order for this ticker — its orderType is
        # definitive (STP/STP LMT → STOP_LOSS, LMT → TAKE_PROFIT, TRAIL → TRAILING_STOP).
        # Fallback: match closing-side fills by price vs known ATR SL/TP levels.
        # Both methods only cover current-session data.
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
                        exit_price = float(trade_fills[-1].execution.price)
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
                        latest = max(fills, key=lambda f: getattr(f, "time", 0))
                        exit_price = float(latest.execution.price)
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
        except Exception as exc:
            logger.debug("[SYNC] Could not infer exit details for %s: %s", ticker, exc)

        engine.forget_position(ticker)
        await redis.hdel(_HWM_REDIS_KEY, ticker)
        await redis.hdel(_POSITION_PARAMS_KEY, ticker)
        POSITIONS_CLOSED.labels(reason=exit_reason).inc()
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


# ── UI command listener ────────────────────────────────────────────────────────

async def run_command_listener(engine: ExecutionEngine, redis: aioredis.Redis) -> None:
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

        if cmd == "HALT_NEW":
            _halt_flag.set()
            logger.warning("New positions HALTED via UI command")
        elif cmd == "RESUME":
            _halt_flag.clear()
            logger.info("New positions RESUMED via UI command")
        elif cmd == "CLOSE_ALL":
            tickers = list(engine.open_tickers)
            logger.warning("CLOSE_ALL: closing %d positions", len(tickers))
            for ticker in tickers:
                try:
                    await engine.close_position(ticker, reason="UI:CLOSE_ALL")
                    POSITIONS_CLOSED.labels(ticker=ticker, reason="UI:CLOSE_ALL").inc()
                    logger.info("Closed %s via CLOSE_ALL", ticker)
                except Exception as exc:
                    logger.error("Failed to close %s: %s", ticker, exc)
        elif cmd == "CLOSE_TICKER":
            ticker = payload.get("ticker", "")
            if not ticker:
                logger.warning("CLOSE_TICKER missing ticker in payload")
            elif ticker not in engine.open_tickers:
                logger.info("CLOSE_TICKER: %s not in open positions — ignoring", ticker)
            else:
                try:
                    await engine.close_position(ticker, reason="UI:CLOSE_TICKER")
                    POSITIONS_CLOSED.labels(ticker=ticker, reason="UI:CLOSE_TICKER").inc()
                    logger.info("Closed %s via CLOSE_TICKER", ticker)
                except Exception as exc:
                    logger.error("Failed to close %s: %s", ticker, exc)
        elif cmd == "CONFIG_UPDATED":
            logger.info("CONFIG_UPDATED received — config will reload on next cycle")


# ── Entry point ────────────────────────────────────────────────────────────────

async def _run_heartbeat(engine: ExecutionEngine, redis: aioredis.Redis) -> None:
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
            connected = await engine.health_check()
            await redis.setex("service:heartbeat", _HB_TTL, "1")
            await redis.setex("ib:connected", _HB_TTL, "1" if connected else "0")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[HEARTBEAT] Error: %s", exc)
        await asyncio.sleep(_HB_INTERVAL)


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)

    start_metrics_server(port=int(os.getenv("METRICS_PORT", "8000")))

    cfg = await SystemConfig.load(redis)
    logger.info("SystemConfig loaded (hash=%s)", cfg.config_hash())

    # ── Build engine ──────────────────────────────────────────────────────────
    try:
        from ib_async import IB  # noqa: PLC0415

        from social_trading.execution.ibkr import IBKRExecutionEngine  # noqa: PLC0415
        from social_trading.market_data.ibkr import IBKRMarketData  # noqa: PLC0415
        ib = IB()
        port = int(os.getenv("IBKR_PORT", "7497"))
        client_id = int(os.getenv("IBKR_CLIENT_ID", "10"))
        ib_account = os.getenv("IBKR_ACCOUNT", "").strip()
        if ib_account.upper().startswith("DFQ"):
            raise ValueError(
                f"IBKR_ACCOUNT={ib_account!r} is a Financial Advisor master account. "
                "This app only supports individual user accounts. "
                "Set IBKR_ACCOUNT to one of the sub-accounts (e.g. DUQ…)."
            )
        await ib.connectAsync("127.0.0.1", port, clientId=client_id)
        # Explicitly load all existing positions into the ib_async local cache.
        # ib_async does NOT auto-request positions on connect, so without this
        # call any positions opened by a previous session would be invisible to
        # ib.positions() and therefore absent from positions:live.
        await ib.reqPositionsAsync()
        engine: ExecutionEngine = IBKRExecutionEngine(ib=ib, account=ib_account, host="127.0.0.1", port=port, client_id=client_id)  # type: ignore[assignment]
        # Use IB for real-time prices; yfinance as fallback for any gaps
        market_data: MarketDataProvider = FallbackMarketData(  # type: ignore[assignment]
            primary=IBKRMarketData(ib=ib),     # IB: real-time quotes, ATR, OHLCV
            secondary=YFinanceMarketData(),    # fallback: missing subscriptions / off-hours
        )
        logger.info("Connected to IBKR port=%d clientId=%d account=%s (IB market data primary, yfinance fallback)", port, client_id, ib_account or "(auto)")
    except Exception as exc:
        message = f"IBKR connection failed; execution service requires Interactive Brokers: {exc}"
        logger.error(message)
        raise RuntimeError(message) from exc
    exit_manager = PositionExitManager()
    breaker = CircuitBreaker(redis)
    bus = TradingEventBus(redis)

    mode = "live"
    await redis.set("trading:mode", mode)

    # Restore HWM and position params from Redis so trailing stops survive restarts
    await _load_hwm_from_redis(redis, engine)
    await _load_position_params_from_redis(redis, engine)
    await _load_trail_orders_from_redis(redis, engine)

    # Reconcile: clean up Redis state for positions closed while service was offline
    await _reconcile_startup(redis, engine, mode=mode)

    # Prune aged-out DB rows once at startup
    await _prune_old_data()

    # Warm up market data for all active watchlist tickers so the risk service
    # has prices/ATR from startup rather than waiting for the slow per-ticker
    # cadence in the exit loop.  We fire-and-forget this in a background task
    # so it does not block service start.
    async def _warmup_market_data() -> None:
        try:
            wl_raw = await redis.zrange("watchlist:active", 0, -1)
            wl_tickers: list[str] = [
                t.decode() if isinstance(t, bytes) else t for t in wl_raw
            ]
            if not wl_tickers:
                return
            logger.info(
                "[EXEC] Warming up market data for %d watchlist tickers…", len(wl_tickers)
            )
            count = 0
            for ticker in wl_tickers:
                try:
                    snap = await _write_market_snapshot_and_get_price(
                        redis, ticker, market_data
                    )
                    if snap is not None:
                        count += 1
                except Exception as exc:
                    logger.debug("[EXEC] Warmup failed for %s: %s", ticker, exc)
                await asyncio.sleep(0.1)  # gentle rate-limit for yfinance
            logger.info("[EXEC] Market data warmup complete: %d/%d tickers populated", count, len(wl_tickers))
        except Exception as exc:
            logger.warning("[EXEC] Market data warmup error: %s", exc)

    asyncio.create_task(_warmup_market_data(), name="exec:market_warmup")

    # Write account state immediately at startup so the risk service has a
    # valid NLV before the first exit loop cycle.  This also refreshes any
    # stale 0.0 value left over from a previous disconnect.
    await _write_account_state(redis, engine)

    tasks = [
        asyncio.create_task(
            _run_heartbeat(engine, redis),
            name="exec:heartbeat",
        ),
        asyncio.create_task(
            run_trade_loop(bus, engine, redis, market_data, mode=mode, cfg=cfg),
            name="exec:trade",
        ),
        asyncio.create_task(
            run_exit_loop(engine, exit_manager, market_data, breaker, redis, mode=mode),
            name="exec:exit",
        ),
        asyncio.create_task(
            run_command_listener(engine, redis),
            name="exec:cmd",
        ),
    ]

    def _shutdown(sig: int, _frame: object) -> None:
        logger.info("Signal %d received — shutting down execution service", sig)
        for task in tasks:
            task.cancel()

    os_signal.signal(os_signal.SIGTERM, _shutdown)
    os_signal.signal(os_signal.SIGINT, _shutdown)

    logger.info("Execution service started")
    try:
        await asyncio.gather(*tasks)
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
