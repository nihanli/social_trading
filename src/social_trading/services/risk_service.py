"""
Risk Service — gates strategy signals through risk checks before execution.

Pipeline (design §6):
    strategy_signals (stream)
       │
       ├─► CircuitBreaker.check()          → halt / reduce if breached
       ├─► LiquidityGate.check()           → reject illiquid tickers
       ├─► PositionSizer.compute()         → determine share quantity
       │     (uses VIX and realised_vol from market_data cache)
       └─► Publish to selected_signals     → execution service picks up

Market data (price, VIX, ATR, ADV) is read from Redis hash keys written by
the market_data service (Phase 5).  Until Phase 5 is implemented, sensible
defaults are used so the risk service remains operational before Phase 5 is implemented.

Circuit breaker state is polled every cfg.signal_poll_interval_sec seconds
(same cadence as signal evaluation) and persisted in Redis key "circuit:state".

Run:
    python -m social_trading.services.risk_service
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal as os_signal
import sys
from datetime import UTC, datetime

import redis.asyncio as aioredis
from dotenv import load_dotenv

from social_trading.config.system_config import SystemConfig
from social_trading.core.events import (
    STREAM_MAXLEN,
    STREAM_SELECTED_SIGNALS,
    STREAM_SIGNAL_REJECTIONS,
    STREAM_STRATEGY_SIGNALS,
)
import json

from social_trading.core.models import AccountState, Position, Signal
from social_trading.monitoring.metrics import (
    SIGNALS_APPROVED,
    SIGNALS_REJECTED,
    set_circuit_breaker_state,
    start_metrics_server,
)
from social_trading.risk.circuit_breaker import CircuitBreaker
from social_trading.risk.liquidity_gate import LiquidityGate, LiquidityQuote
from social_trading.risk.position_sizer import PositionSizer
from social_trading.storage.event_bus import TradingEventBus

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
_redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
from social_trading.monitoring.log_handler import RedisLogHandler  # noqa: E402
logging.getLogger().addHandler(RedisLogHandler("risk", _redis_url))
logger = logging.getLogger(__name__)

_GROUP = "risk"
_CONSUMER = "risk-0"
_INGEST_BATCH = 32

# Redis hash key pattern: market_data:{ticker}
_MARKET_DATA_KEY = "market_data:{ticker}"


# ── Deserialisation helpers ────────────────────────────────────────────────────

def _stream_dict_to_signal(fields: dict) -> Signal | None:
    try:
        return Signal(
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
    except Exception as exc:
        logger.warning("malformed strategy_signals message: %s", exc)
        return None


def _signal_is_stale(signal: Signal, max_age_minutes: int) -> tuple[bool, float]:
    """
    Return (is_stale, age_seconds).
    A signal is stale if it was generated more than max_age_minutes ago.
    Handles both tz-aware and tz-naive generated_at timestamps.
    """
    generated = signal.generated_at
    if generated.tzinfo is None:
        generated = generated.replace(tzinfo=UTC)
    age_sec = (datetime.now(UTC) - generated).total_seconds()
    return age_sec > max_age_minutes * 60, age_sec


_TRADE_COOLDOWN_SECS = 3600  # 1 hour


async def _ticker_in_cooldown(
    redis: aioredis.Redis,
    signal: Signal,
    cooldown_secs: int = _TRADE_COOLDOWN_SECS,
) -> tuple[bool, str]:
    """Return (in_cooldown, reason).

    A ticker is in cooldown if a position was opened or closed within
    cooldown_secs of the signal's generated_at time.  This prevents
    approving a re-entry signal for a ticker that was just traded,
    giving the market time to settle before taking a fresh position.

    The check compares the last trade timestamp (stored in Redis key
    trade:last_at:{ticker} by the execution service) against the
    signal's generated_at, not against wall-clock time, so stale
    signals that pile up in the queue don't bypass the cooldown.
    """
    try:
        raw = await redis.get(f"trade:last_at:{signal.ticker}")
        if not raw:
            return False, ""
        last_at_str = raw.decode() if isinstance(raw, bytes) else raw
        last_at = datetime.fromisoformat(last_at_str)
        if last_at.tzinfo is None:
            last_at = last_at.replace(tzinfo=UTC)
        sig_time = signal.generated_at
        if sig_time.tzinfo is None:
            sig_time = sig_time.replace(tzinfo=UTC)
        elapsed = (sig_time - last_at).total_seconds()
        if elapsed < cooldown_secs:
            remaining = int(cooldown_secs - elapsed)
            return True, (
                f"last trade at {last_at.strftime('%H:%M:%S')} UTC — "
                f"{int(elapsed)}s ago (cooldown {cooldown_secs}s, "
                f"{remaining}s remaining)"
            )
    except Exception as exc:
        logger.debug("[RISK] Cooldown check failed for %s: %s", signal.ticker, exc)
    return False, ""


def _approved_signal_to_stream_dict(
    signal: Signal,
    quantity: int,
    stop_loss: float,
    take_profit: float,
) -> dict[str, str]:
    return {
        "ticker": signal.ticker,
        "direction": signal.direction,
        "quality_score": str(signal.quality_score),
        "sentiment_score": str(signal.sentiment_score),
        "volume_z_score": str(signal.volume_z_score),
        "momentum": str(signal.momentum),
        "convergence": str(signal.convergence),
        "source_post_count": str(signal.source_post_count),
        "generated_at": signal.generated_at.isoformat(),
        "quantity": str(quantity),
        "stop_loss": str(stop_loss),
        "take_profit": str(take_profit),
        "approved_at": datetime.now(UTC).isoformat(),
    }


# ── Market data cache reader ──────────────────────────────────────────────────

async def _get_market_snapshot(
    redis: aioredis.Redis,
    ticker: str,
) -> dict:
    """
    Read market data hash written by Phase 5 market_data service.
    Returns sensible defaults if key not present (pre-Phase 5).
    """
    raw = await redis.hgetall(_MARKET_DATA_KEY.format(ticker=ticker))
    if not raw:
        # No market data hash at all — price is unknown so we return last=0
        # which causes the sizer to reject (entry_price <= 0) rather than
        # the liquidity gate, which gives a clearer rejection reason.
        # ADV/market-cap defaults are set to pass-through values so the only
        # blocker is the missing price.
        logger.debug("[RISK] No market data for %s — returning zero price to block signal", ticker)
        return {
            "last": 0.0,
            "bid": 0.0,
            "ask": 0.0,
            "adv_shares": 1_000_000.0,
            "adv_usd": 100_000_000.0,
            "market_cap_usd": 10_000_000_000.0,
            "atr_14": 0.0,
            "realised_vol": 0.20,  # conservative default (vol scalar will limit size)
            "vix": 15.0,           # conservative default (normal regime)
        }

    def _f(key: str, default: float) -> float:
        v = raw.get(key) or raw.get(key.encode())
        return float(v) if v is not None else default

    def _fpos(key: str, default: float) -> float:
        """Like _f but treats zero as missing — returns default if stored value <= 0.

        Used for fields where 0 means "data was never written" rather than a
        legitimate zero value.  A previous code version may have written 0 for
        adv_usd/adv_shares; hset() never deletes fields so those zeroes persist
        even after the field was removed from the write mapping.
        """
        val = _f(key, default)
        return val if val > 0 else default

    return {
        "last":           _f("last", 100.0),
        "bid":            _f("bid", 99.9),
        "ask":            _f("ask", 100.1),
        "adv_shares":     _fpos("adv_shares", 1_000_000.0),
        "adv_usd":        _fpos("adv_usd", 100_000_000.0),
        "market_cap_usd": _fpos("market_cap_usd", 10_000_000_000.0),
        "atr_14":         _f("atr_14", 2.0),
        "realised_vol":   _f("realised_vol", 0.20),
        "vix":            _f("vix", 15.0),
    }


async def _get_account_state(redis: aioredis.Redis) -> AccountState:
    """
    Read account state from Redis hash "account:state".
    Written by execution service (Phase 5). Falls back to empty defaults.
    Also loads open positions from positions:live so social_exposure is accurate.
    """
    raw = await redis.hgetall("account:state")
    if not raw:
        return AccountState(
            net_liquidation=100_000.0,
            cash=100_000.0,
            daily_pnl=0.0,
            weekly_pnl=0.0,
            drawdown_pct=0.0,
        )

    def _f(key: str, default: float) -> float:
        v = raw.get(key) or raw.get(key.encode())
        return float(v) if v is not None else default

    # Detect stale account state written on a prior trading day.
    # daily_pnl and (on a new week) weekly_pnl should be treated as 0 to avoid
    # spurious circuit-breaker trips on service restart before execution_service
    # has written today's fresh values.  drawdown_pct persists across days.
    today = datetime.now(UTC).date().isoformat()
    state_date_raw = raw.get("state_date") or raw.get(b"state_date")
    state_date = state_date_raw.decode() if isinstance(state_date_raw, bytes) else state_date_raw
    is_stale = state_date != today
    if is_stale:
        logger.debug(
            "_get_account_state: account:state is from %s (today=%s) — zeroing P&L fields "
            "to prevent stale circuit-breaker trips on restart",
            state_date,
            today,
        )

    is_new_week = datetime.now(UTC).weekday() == 0  # Monday

    # Load open positions so the social_exposure concentration check works
    open_positions: list[Position] = []
    try:
        pos_raw = await redis.hgetall("positions:live")
        for val in pos_raw.values():
            try:
                d = json.loads(val.decode() if isinstance(val, bytes) else val)
                open_positions.append(
                    Position(
                        ticker=d["ticker"],
                        direction=d["direction"],
                        shares=int(d["shares"]),
                        entry_price=float(d["entry_price"]),
                        stop_loss=float(d.get("stop_loss", 0.0)),
                        take_profit=float(d.get("take_profit", 0.0)),
                        opened_at=datetime.fromisoformat(d["opened_at"]) if d.get("opened_at") else datetime.now(UTC),
                        unrealized_pnl=float(d.get("unrealized_pnl", 0.0)),
                        high_water_mark=float(d.get("high_water_mark", 0.0)),
                    )
                )
            except Exception:
                pass  # skip malformed entries
    except Exception:
        pass  # Redis unavailable — proceed without position data

    return AccountState(
        net_liquidation=_f("net_liquidation", 100_000.0),
        cash=_f("cash", 100_000.0),
        daily_pnl=0.0 if is_stale else _f("daily_pnl", 0.0),
        weekly_pnl=0.0 if (is_stale and is_new_week) else _f("weekly_pnl", 0.0),
        drawdown_pct=_f("drawdown_pct", 0.0),  # persists across days — real measure of peak-to-trough
        open_positions=open_positions,
    )


# ── Service main loop ─────────────────────────────────────────────────────────

async def _publish_rejection(redis: aioredis.Redis, signal: "Signal", reason: str) -> None:
    """Publish a rejection event to STREAM_SIGNAL_REJECTIONS.

    persistence_service consumes this stream and writes rejection_reason to the
    signals table.  Using a stream (instead of a direct DB update here) keeps
    all DB writes in one process and avoids race conditions with the signals
    INSERT.
    """
    ts = signal.generated_at.isoformat() if signal.generated_at else ""
    if not ts:
        return
    try:
        await redis.xadd(
            STREAM_SIGNAL_REJECTIONS,
            {"ticker": signal.ticker, "generated_at": ts, "reason": reason},
            maxlen=STREAM_MAXLEN.get(STREAM_SIGNAL_REJECTIONS),
            approximate=True,
        )
    except Exception as exc:
        logger.warning(
            "Could not publish rejection for %s to stream: %s", signal.ticker, exc
        )


async def run_risk_service(
    bus: TradingEventBus,
    breaker: CircuitBreaker,
    gate: LiquidityGate,
    sizer: PositionSizer,
    redis: aioredis.Redis,
) -> None:
    """
    Continuously consume strategy_signals, run risk checks, and forward
    approved signals to selected_signals.  Runs until cancelled.
    """
    await bus.create_group(STREAM_STRATEGY_SIGNALS, _GROUP)
    logger.info("Risk service listening on %s (group=%s)", STREAM_STRATEGY_SIGNALS, _GROUP)

    approved_total = 0
    rejected_total = 0

    while True:
        cfg = await SystemConfig.load(redis)
        account = await _get_account_state(redis)

        # ── Circuit breaker check ─────────────────────────────────────────────
        cb_status = await breaker.check(account, cfg)
        set_circuit_breaker_state(cb_status.state.value)
        if not cb_status.allow:
            logger.warning(
                "CircuitBreaker %s — draining queue without forwarding",
                cb_status.state.value,
            )
            # Still consume messages so queue doesn't pile up
            messages = await bus.consume(
                STREAM_STRATEGY_SIGNALS, _GROUP, _CONSUMER, count=_INGEST_BATCH
            )
            for msg_id, fields in messages:
                _sig = _stream_dict_to_signal(fields)
                if _sig is not None:
                    await _publish_rejection(
                        redis,
                        _sig,
                        f"circuit_breaker: {cb_status.state.value} — new entries blocked",
                    )
                await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
            await asyncio.sleep(cfg.signal_poll_interval_sec)
            continue

        # ── Process batch of signals ──────────────────────────────────────────
        messages = await bus.consume(
            STREAM_STRATEGY_SIGNALS, _GROUP, _CONSUMER, count=_INGEST_BATCH
        )

        for msg_id, fields in messages:
            signal = _stream_dict_to_signal(fields)
            if signal is None:
                await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                continue

            try:
                # ── Signal age check ──────────────────────────────────────────
                # Reject signals that sat in the queue too long — the market
                # conditions that triggered them may no longer be valid.
                is_stale, signal_age_sec = _signal_is_stale(
                    signal, cfg.signal_approval_max_age_min
                )
                if is_stale:
                    logger.info(
                        "REJECTED (stale) %s: signal age %.0fs > max %ds",
                        signal.ticker, signal_age_sec,
                        cfg.signal_approval_max_age_min * 60,
                    )
                    rejected_total += 1
                    SIGNALS_REJECTED.labels(reason="stale").inc()
                    await _publish_rejection(redis, signal, f"stale: age {signal_age_sec:.0f}s > max {cfg.signal_approval_max_age_min * 60}s")
                    await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                    continue

                # ── Ticker cooldown check ──────────────────────────────────────
                # Reject signals for tickers traded within the last hour.
                # Prevents rapid re-entries that may chase the same move or
                # re-enter a position that was just stopped out.
                in_cooldown, cooldown_reason = await _ticker_in_cooldown(redis, signal)
                if in_cooldown:
                    logger.info(
                        "REJECTED (cooldown) %s: %s", signal.ticker, cooldown_reason,
                    )
                    rejected_total += 1
                    SIGNALS_REJECTED.labels(reason="cooldown").inc()
                    await _publish_rejection(redis, signal, f"cooldown: {cooldown_reason}")
                    await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                    continue

                market = await _get_market_snapshot(redis, signal.ticker)
                entry_price = market["last"]

                # ── Liquidity gate ────────────────────────────────────────────
                quote = LiquidityQuote(
                    ticker=signal.ticker,
                    last_price=entry_price,
                    bid=market["bid"],
                    ask=market["ask"],
                    adv_shares=market["adv_shares"],
                    adv_usd=market["adv_usd"],
                    market_cap_usd=market["market_cap_usd"],
                )
                gate_result = gate.check(signal, quote, cfg)
                if not gate_result.passed:
                    logger.info("REJECTED (liquidity) %s: %s", signal.ticker, gate_result.reason)
                    rejected_total += 1
                    SIGNALS_REJECTED.labels(reason="liquidity").inc()
                    await _publish_rejection(redis, signal, f"liquidity: {gate_result.reason}")
                    await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                    continue

                # ── Position sizing ───────────────────────────────────────────
                size_multiplier = cb_status.size_multiplier  # 0.5 if REDUCED_50
                effective_cfg = cfg
                if size_multiplier < 1.0:
                    import dataclasses
                    effective_cfg = dataclasses.replace(
                        cfg,
                        max_position_pct=cfg.max_position_pct * size_multiplier,
                    )

                shares, size_reason = sizer.compute(
                    signal=signal,
                    account=account,
                    entry_price=entry_price,
                    vix=market["vix"],
                    realised_vol=market["realised_vol"],
                    cfg=effective_cfg,
                )
                if shares == 0:
                    logger.info("REJECTED (sizer) %s: %s", signal.ticker, size_reason)
                    rejected_total += 1
                    SIGNALS_REJECTED.labels(reason="sizer").inc()
                    await _publish_rejection(redis, signal, f"sizer: {size_reason}")
                    await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                    continue

                # ── Re-check gate with actual order size ──────────────────────
                gate_result2 = gate.check(signal, quote, cfg, order_shares=shares)
                if not gate_result2.passed:
                    logger.info(
                        "REJECTED (adv_pct) %s: %s", signal.ticker, gate_result2.reason
                    )
                    rejected_total += 1
                    SIGNALS_REJECTED.labels(reason="adv_pct").inc()
                    await _publish_rejection(redis, signal, f"adv_pct: {gate_result2.reason}")
                    await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                    continue

                # ── Publish approved signal ───────────────────────────────────
                atr = market["atr_14"]
                if atr <= 0:
                    logger.info(
                        "REJECTED (atr_zero) %s: ATR=0 — cannot compute stop-loss safely",
                        signal.ticker,
                    )
                    rejected_total += 1
                    SIGNALS_REJECTED.labels(reason="atr_zero").inc()
                    await _publish_rejection(redis, signal, "atr_zero: ATR=0, cannot compute stop-loss")
                    await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                    continue

                stop_loss = sizer.stop_loss_price(
                    signal.direction, entry_price, atr, cfg
                )
                take_profit = sizer.take_profit_price(
                    signal.direction, entry_price, cfg
                )

                # Guard: stop-loss must be a valid positive price
                if stop_loss <= 0:
                    logger.info(
                        "REJECTED (sl_invalid) %s: entry=%.2f ATR=%.4f → sl=%.2f ≤ 0",
                        signal.ticker, entry_price, atr, stop_loss,
                    )
                    rejected_total += 1
                    SIGNALS_REJECTED.labels(reason="sl_invalid").inc()
                    await _publish_rejection(redis, signal, f"sl_invalid: entry={entry_price:.2f} ATR={atr:.4f} → sl={stop_loss:.2f} ≤ 0")
                    await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                    continue

                stream_dict = _approved_signal_to_stream_dict(
                    signal, shares, stop_loss, take_profit
                )
                await redis.xadd(STREAM_SELECTED_SIGNALS, stream_dict,
                                 maxlen=STREAM_MAXLEN.get(STREAM_SELECTED_SIGNALS), approximate=True)
                approved_total += 1
                SIGNALS_APPROVED.labels(ticker=signal.ticker, direction=signal.direction).inc()
                logger.info(
                    "APPROVED %s %s qty=%d entry=%.2f sl=%.2f tp=%.2f [approved=%d rejected=%d]",
                    signal.direction, signal.ticker, shares,
                    entry_price, stop_loss, take_profit,
                    approved_total, rejected_total,
                )

            except Exception as exc:
                logger.exception("Error processing signal for %s: %s", signal.ticker, exc)
                await _publish_rejection(redis, signal, f"error: {type(exc).__name__}: {exc}")

            await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)

        if not messages:
            await asyncio.sleep(1.0)  # back-off when queue empty


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)

    start_metrics_server(port=int(os.getenv("METRICS_PORT", "8000")))

    cfg = await SystemConfig.load(redis)
    logger.info("SystemConfig loaded (hash=%s)", cfg.config_hash())

    bus = TradingEventBus(redis)
    breaker = CircuitBreaker(redis)
    gate = LiquidityGate()
    sizer = PositionSizer()

    tasks = [
        asyncio.create_task(
            run_risk_service(bus, breaker, gate, sizer, redis),
            name="risk:service",
        ),
    ]

    def _shutdown(sig: int, _frame: object) -> None:
        logger.info("Received signal %d — shutting down risk service", sig)
        for task in tasks:
            task.cancel()

    os_signal.signal(os_signal.SIGTERM, _shutdown)
    os_signal.signal(os_signal.SIGINT, _shutdown)

    logger.info("Risk service started")
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Risk service stopped")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
