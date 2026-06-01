"""
Execution Service — submits approved signals and manages open positions.

Three concurrent loops:

  trade_loop:   Consumes selected_signals (consumer group "execution").
                For each approved signal:
                  - Skip if halted or position already open for that ticker
                  - Submit to ExecutionEngine (paper or IBKR)
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

The ExecutionEngine is injected — swap PaperTradingEngine for IBKRExecutionEngine
with no code changes in this file.

Run:
    python -m social_trading.services.execution_service            # paper mode
    python -m social_trading.services.execution_service --ibkr     # live (requires TWS/IB Gateway)
"""
from __future__ import annotations

import argparse
import asyncio
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
from social_trading.execution.paper import PaperTradingEngine
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
from social_trading.storage.event_bus import TradingEventBus

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
# Warning 10167 ("not subscribed, showing delayed data") is expected for paper
# accounts; suppress ib_async wrapper noise to WARNING level.
logging.getLogger("ib_async.wrapper").setLevel(logging.WARNING)
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
    engine: PaperTradingEngine,
) -> None:
    """Write account state to Redis hash 'account:state' for risk service."""
    state = await engine.get_account_state()
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
    engine: PaperTradingEngine,
) -> None:
    """
    Sync open positions to positions:live Redis hash.
    Called each exit-loop cycle so the persistence service and UI see current state.
    """
    try:
        positions = await engine.get_positions()
        pipe = redis.pipeline()
        pipe.delete(_POSITIONS_LIVE_KEY)
        for pos in positions:
            current_price = engine.get_price(pos.ticker) or pos.entry_price
            if pos.direction == "LONG":
                computed_upnl = (current_price - pos.entry_price) * pos.shares
            else:
                computed_upnl = (pos.entry_price - current_price) * pos.shares
            # Prefer IB's native unrealizedPNL when available (non-zero)
            unrealized_pnl = pos.unrealized_pnl if pos.unrealized_pnl != 0.0 else computed_upnl
            pipe.hset(_POSITIONS_LIVE_KEY, pos.ticker, json.dumps({
                "ticker": pos.ticker,
                "direction": pos.direction,
                "shares": pos.shares,
                "entry_price": pos.entry_price,
                "stop_loss": pos.stop_loss,
                "take_profit": pos.take_profit,
                "unrealized_pnl": round(unrealized_pnl, 2),
                "high_water_mark": pos.high_water_mark,
                "opened_at": pos.opened_at.isoformat() if pos.opened_at else None,
            }))
        await pipe.execute()
    except Exception as exc:
        logger.warning("[POSITIONS] Failed to write positions:live: %s", exc)



# ── Service loops ─────────────────────────────────────────────────────────────

async def run_trade_loop(
    bus: TradingEventBus,
    engine: PaperTradingEngine,
    redis: aioredis.Redis,
    market_data: YFinanceMarketData | None = None,
    mode: str = "paper",
) -> None:
    """
    Consume selected_signals and submit to execution engine.
    Runs until cancelled.
    """
    await bus.create_group(STREAM_SELECTED_SIGNALS, _GROUP)
    logger.info("Execution trade loop listening on %s", STREAM_SELECTED_SIGNALS)

    submitted = 0
    skipped = 0

    while True:
        try:
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

                    # Skip if market is closed — do NOT ack so the signal
                    # is redelivered when the market opens next session.
                    if not _NYSE.is_open():
                        logger.info(
                            "[EXEC] Market closed — holding %s signal. %s",
                            signal.ticker, _NYSE.status_str(),
                        )
                        # Sleep briefly to avoid a tight spin when the whole
                        # batch of pending signals is outside market hours.
                        await asyncio.sleep(5.0)
                        break  # re-consume on next loop iteration

                    result = await engine.submit_signal(
                        signal=signal,
                        quantity=quantity,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                    )

                    if result.status in ("filled", "submitted"):
                        submitted += 1
                        ORDERS_PLACED.labels(ticker=signal.ticker, status=result.status).inc()
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
    engine: PaperTradingEngine,
    exit_manager: PositionExitManager,
    market_data: YFinanceMarketData,
    breaker: CircuitBreaker,
    redis: aioredis.Redis,
    mode: str = "paper",
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
    _watchlist_last_refresh: dict[str, float] = {}
    # Tracks tickers that were open at the end of the previous cycle.
    # Used to detect positions closed externally by IB between cycles.
    _prev_open_tickers: set[str] = set()

    while True:
        try:
            cfg = await SystemConfig.load(redis)

            # ── 1. Connection guard ───────────────────────────────────────────
            if not await engine.health_check():
                logger.warning(
                    "[SYNC] Engine not connected — skipping position evaluation this cycle"
                )
                await asyncio.sleep(cfg.signal_poll_interval_sec)
                continue

            # Fetch VIX once per cycle (shared across all ticker snapshots)
            vix = await market_data.get_vix()
            await redis.set("market:vix", str(vix))

            # ── 2. Refresh market data ────────────────────────────────────────
            open_positions = await engine.get_positions()
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
            all_tickers = open_tickers | stale_watchlist

            for ticker in all_tickers:
                try:
                    snapshot = await _write_market_snapshot_and_get_price(
                        redis, ticker, market_data, vix=vix
                    )
                    if snapshot is not None:
                        engine.set_price(ticker, snapshot)
                    if ticker in stale_watchlist:
                        _watchlist_last_refresh[ticker] = now_ts
                except Exception as exc:
                    logger.debug("Price refresh failed for %s: %s", ticker, exc)

            # ── 3. Evaluate exit rules ────────────────────────────────────────
            # Re-fetch positions after price updates (HWM may have moved)
            open_positions = await engine.get_positions()
            now = datetime.now(UTC)
            just_closed: set[str] = set()

            for pos in open_positions:
                current_price = engine.get_price(pos.ticker) or pos.entry_price
                sentiment, mention_ratio = await _get_sentiment_context(redis, pos.ticker)
                decision = exit_manager.evaluate(
                    pos, current_price, cfg,
                    current_sentiment=sentiment,
                    mention_ratio=mention_ratio,
                    now=now,
                )
                if decision.should_exit:
                    await engine.close_position(pos.ticker, reason=decision.reason)
                    just_closed.add(pos.ticker)
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

            # ── 4. Reconcile external IB closes ──────────────────────────────
            # Tickers that were open last cycle but are now gone (and we didn't
            # close them) were filled by IB's bracket legs or closed in TWS.
            now_open_tickers = {p.ticker for p in open_positions} - just_closed
            if _prev_open_tickers:
                await _reconcile_external_closes(
                    redis, engine,
                    prev_open=_prev_open_tickers,
                    now_open=now_open_tickers,
                    just_closed=just_closed,
                    mode=mode,
                )
            _prev_open_tickers = now_open_tickers

            # ── 5. Persist state + metrics ────────────────────────────────────
            await _persist_hwm_to_redis(redis, engine)
            await _persist_position_params_to_redis(redis, engine)
            await _write_account_state(redis, engine)
            await _write_positions_to_redis(redis, engine)

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

        except asyncio.CancelledError:
            raise  # let the task be cancelled normally on shutdown
        except Exception as exc:
            logger.error("[EXIT LOOP] Unhandled error — will retry next cycle: %s", exc, exc_info=True)

        await asyncio.sleep(cfg.signal_poll_interval_sec)  # type: ignore[possibly-undefined]


async def _get_sentiment_context(
    redis: aioredis.Redis,
    ticker: str,
    window_secs: float = 3600.0,
) -> tuple[float, float]:
    """
    Read current sentiment score and mention ratio for a ticker from Redis.

    Returns:
        (current_sentiment, mention_ratio)
        current_sentiment: engagement-weighted avg score ∈ [-1, 1], 0.0 if no data
        mention_ratio: current_hour_mentions / peak_hour_mentions, 1.0 if no data
    """
    import json as _json  # noqa: PLC0415
    import time as _time  # noqa: PLC0415

    current_sentiment = 0.0
    mention_ratio = 1.0

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
        # ── Mention ratio: latest hourly count / peak count ───────────────────
        raw_history = await redis.lrange(f"mention_history:{ticker}", 0, -1)
        if raw_history:
            counts = [float(v) for v in raw_history]
            peak = max(counts)
            current = counts[-1]
            if peak > 0:
                mention_ratio = current / peak
    except Exception:
        pass

    return current_sentiment, mention_ratio


async def _write_market_snapshot_and_get_price(
    redis: aioredis.Redis,
    ticker: str,
    market_data: YFinanceMarketData,
    vix: float = 20.0,
) -> float | None:
    """Fetch snapshot, write to Redis, return last price."""
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
_EXEC_EVENTS_STREAM = "execution:events"
_POSITIONS_LIVE_KEY = "positions:live"


async def _load_hwm_from_redis(
    redis: aioredis.Redis,
    engine: PaperTradingEngine,
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
    engine: PaperTradingEngine,
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
    engine: PaperTradingEngine,
) -> None:
    """Restore position params (sl/tp/opened_at) from Redis so exit rules work after restart."""
    import json as _json  # noqa: PLC0415
    if not hasattr(engine, "seed_position_params"):
        return  # PaperTradingEngine doesn't need this (state is in-memory)
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
    engine: PaperTradingEngine,
) -> None:
    """Persist position params (sl/tp/opened_at) to Redis so exit rules survive restarts."""
    import json as _json  # noqa: PLC0415
    if not hasattr(engine, "get_position_params"):
        return  # PaperTradingEngine doesn't need this
    try:
        params = engine.get_position_params()  # type: ignore[union-attr]
        if not params:
            return
        mapping = {ticker: _json.dumps(p) for ticker, p in params.items()}
        await redis.hset(_POSITION_PARAMS_KEY, mapping=mapping)
    except Exception as exc:
        logger.warning("[PARAMS] Failed to persist to Redis: %s", exc)


async def _reconcile_startup(
    redis: aioredis.Redis,
    engine: PaperTradingEngine,
) -> None:
    """
    Compare Redis position:params against IB's current positions on startup.

    Any ticker present in Redis state but absent from IB was closed while the
    service was offline (bracket fill, manual close in TWS, etc.).  Clean up
    so stale sl/tp/hwm don't trigger false exits on the first cycle.
    """
    if not hasattr(engine, "get_position_params"):
        return  # Paper engine — no IB to reconcile against
    params = engine.get_position_params()  # type: ignore[union-attr]
    if not params:
        return
    try:
        current = await engine.get_positions()
        current_tickers = {p.ticker for p in current}
    except Exception as exc:
        logger.warning("[SYNC] Startup reconciliation skipped — IB unavailable: %s", exc)
        return

    orphaned = set(params) - current_tickers
    for ticker in orphaned:
        engine.forget_position(ticker)  # type: ignore[union-attr]
        await redis.hdel(_HWM_REDIS_KEY, ticker)
        await redis.hdel(_POSITION_PARAMS_KEY, ticker)
        logger.warning(
            "[SYNC] %s: found in persisted state but not in IB — "
            "likely closed while service was offline; cleaned up",
            ticker,
        )

    if current_tickers - set(params):
        for ticker in current_tickers - set(params):
            logger.warning(
                "[SYNC] %s: open in IB but no persisted params — "
                "orphaned position (opened manually or from prior session)",
                ticker,
            )


async def _reconcile_external_closes(
    redis: aioredis.Redis,
    engine: PaperTradingEngine,
    prev_open: set[str],
    now_open: set[str],
    just_closed: set[str],
    mode: str = "paper",
) -> None:
    """
    Detect tickers that disappeared from IB positions without this service closing them.

    These are positions filled by IB's bracket legs (stop-loss or take-profit
    executed natively) or closed manually in TWS.  Clean up Redis state and log.
    """
    externally_closed = prev_open - now_open - just_closed
    for ticker in externally_closed:
        # Read params BEFORE cleanup so we can include them in the close event
        opened_at = datetime.now(UTC).isoformat()
        direction = "unknown"
        params_raw = await redis.hget(_POSITION_PARAMS_KEY, ticker)
        if params_raw:
            try:
                params = json.loads(
                    params_raw.decode() if isinstance(params_raw, bytes) else params_raw
                )
                opened_at = params.get("opened_at", opened_at)
                direction = params.get("direction", direction)
            except Exception:
                pass

        engine.forget_position(ticker)
        await redis.hdel(_HWM_REDIS_KEY, ticker)
        await redis.hdel(_POSITION_PARAMS_KEY, ticker)
        POSITIONS_CLOSED.labels(reason="IB_EXTERNAL").inc()
        logger.info(
            "[SYNC] %s closed externally by IB (bracket fill or manual TWS close)", ticker
        )
        await _publish_execution_event(redis, "position_closed", {
            "ticker": ticker,
            "exit_price": 0.0,
            "exit_reason": "IB_EXTERNAL",
            "direction": direction,
            "closed_at": datetime.now(UTC).isoformat(),
            "opened_at": opened_at,
            "mode": mode,
        })
        await redis.lpush("trades:recent", json.dumps({
            "ticker": ticker,
            "direction": direction,
            "exit_reason": "IB_EXTERNAL",
            "closed_at": datetime.now(UTC).isoformat(),
        }))
        await redis.ltrim("trades:recent", 0, 999)


# ── UI command listener ────────────────────────────────────────────────────────

async def run_command_listener(engine: PaperTradingEngine, redis: aioredis.Redis) -> None:
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

async def main(use_ibkr: bool = False) -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)

    start_metrics_server(port=int(os.getenv("METRICS_PORT", "8000")))

    cfg = await SystemConfig.load(redis)
    logger.info("SystemConfig loaded (hash=%s)", cfg.config_hash())

    # ── Build engine ──────────────────────────────────────────────────────────
    if use_ibkr:
        try:
            from ib_async import IB  # noqa: PLC0415

            from social_trading.execution.ibkr import IBKRExecutionEngine  # noqa: PLC0415
            from social_trading.market_data.ibkr import IBKRMarketData  # noqa: PLC0415
            ib = IB()
            port = int(os.getenv("IBKR_PORT", "7497"))  # default paper
            client_id = int(os.getenv("IBKR_CLIENT_ID", "10"))
            ib_account = os.getenv("IBKR_ACCOUNT", "")
            await ib.connectAsync("127.0.0.1", port, clientId=client_id)
            engine: PaperTradingEngine = IBKRExecutionEngine(ib=ib, account=ib_account)  # type: ignore[assignment]
            # Use IB for real-time prices; yfinance as fallback for any gaps
            market_data: YFinanceMarketData = FallbackMarketData(  # type: ignore[assignment]
                primary=IBKRMarketData(ib=ib),
                secondary=YFinanceMarketData(),
            )
            logger.info("Connected to IBKR port=%d clientId=%d account=%s (IB market data primary)", port, client_id, ib_account or "(auto)")
        except Exception as exc:
            logger.error("IBKR connection failed: %s — falling back to paper mode", exc)
            engine = PaperTradingEngine(initial_cash=100_000.0)
            market_data = YFinanceMarketData()
    else:
        initial_cash = float(os.getenv("PAPER_INITIAL_CASH", "100000"))
        engine = PaperTradingEngine(initial_cash=initial_cash)
        market_data = YFinanceMarketData()
        logger.info("Paper trading mode — initial cash $%.2f", initial_cash)
    exit_manager = PositionExitManager()
    breaker = CircuitBreaker(redis)
    bus = TradingEventBus(redis)

    mode = "live" if use_ibkr else "paper"
    await redis.set("trading:mode", mode)

    # Restore HWM and position params from Redis so trailing stops survive restarts
    await _load_hwm_from_redis(redis, engine)
    await _load_position_params_from_redis(redis, engine)

    # Reconcile: clean up Redis state for positions closed while service was offline
    await _reconcile_startup(redis, engine)

    tasks = [
        asyncio.create_task(
            run_trade_loop(bus, engine, redis, market_data, mode=mode),
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
        await redis.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execution service")
    parser.add_argument("--ibkr", action="store_true", help="Use IBKR live engine")
    args = parser.parse_args()
    try:
        asyncio.run(main(use_ibkr=args.ibkr))
    except KeyboardInterrupt:
        sys.exit(0)
