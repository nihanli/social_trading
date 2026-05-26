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
defaults are used so the risk service is fully operational in paper mode.

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
    STREAM_SELECTED_SIGNALS,
    STREAM_STRATEGY_SIGNALS,
)
from social_trading.core.models import AccountState, Signal
from social_trading.risk.circuit_breaker import CircuitBreaker
from social_trading.risk.liquidity_gate import LiquidityGate, LiquidityQuote
from social_trading.risk.position_sizer import PositionSizer
from social_trading.storage.event_bus import TradingEventBus

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
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
    Returns sensible defaults if key not present (paper mode / pre-Phase 5).
    """
    raw = await redis.hgetall(_MARKET_DATA_KEY.format(ticker=ticker))
    if not raw:
        return {
            "last": 100.0,
            "bid": 99.9,
            "ask": 100.1,
            "adv_shares": 1_000_000.0,
            "adv_usd": 100_000_000.0,
            "market_cap_usd": 10_000_000_000.0,
            "atr_14": 2.0,
            "realised_vol": 0.20,
            "vix": 15.0,
        }

    def _f(key: str, default: float) -> float:
        v = raw.get(key) or raw.get(key.encode())
        return float(v) if v is not None else default

    return {
        "last":           _f("last", 100.0),
        "bid":            _f("bid", 99.9),
        "ask":            _f("ask", 100.1),
        "adv_shares":     _f("adv_shares", 1_000_000.0),
        "adv_usd":        _f("adv_usd", 100_000_000.0),
        "market_cap_usd": _f("market_cap_usd", 10_000_000_000.0),
        "atr_14":         _f("atr_14", 2.0),
        "realised_vol":   _f("realised_vol", 0.20),
        "vix":            _f("vix", 15.0),
    }


async def _get_account_state(redis: aioredis.Redis) -> AccountState:
    """
    Read account state from Redis hash "account:state".
    Written by execution service (Phase 5). Falls back to empty defaults.
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

    return AccountState(
        net_liquidation=_f("net_liquidation", 100_000.0),
        cash=_f("cash", 100_000.0),
        daily_pnl=_f("daily_pnl", 0.0),
        weekly_pnl=_f("weekly_pnl", 0.0),
        drawdown_pct=_f("drawdown_pct", 0.0),
    )


# ── Service main loop ─────────────────────────────────────────────────────────

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
        if not cb_status.allow:
            logger.warning(
                "CircuitBreaker %s — draining queue without forwarding",
                cb_status.state.value,
            )
            # Still consume messages so queue doesn't pile up
            messages = await bus.consume(
                STREAM_STRATEGY_SIGNALS, _GROUP, _CONSUMER, count=_INGEST_BATCH
            )
            for msg_id, _ in messages:
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
                    await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                    continue

                # ── Re-check gate with actual order size ──────────────────────
                gate_result2 = gate.check(signal, quote, cfg, order_shares=shares)
                if not gate_result2.passed:
                    logger.info(
                        "REJECTED (adv_pct) %s: %s", signal.ticker, gate_result2.reason
                    )
                    rejected_total += 1
                    await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)
                    continue

                # ── Publish approved signal ───────────────────────────────────
                stop_loss = sizer.stop_loss_price(
                    signal.direction, entry_price, market["atr_14"], cfg
                )
                take_profit = sizer.take_profit_price(
                    signal.direction, entry_price, cfg
                )
                stream_dict = _approved_signal_to_stream_dict(
                    signal, shares, stop_loss, take_profit
                )
                await redis.xadd(STREAM_SELECTED_SIGNALS, stream_dict)
                approved_total += 1
                logger.info(
                    "APPROVED %s %s qty=%d entry=%.2f sl=%.2f tp=%.2f [approved=%d rejected=%d]",
                    signal.direction, signal.ticker, shares,
                    entry_price, stop_loss, take_profit,
                    approved_total, rejected_total,
                )

            except Exception as exc:
                logger.exception("Error processing signal for %s: %s", signal.ticker, exc)

            await bus.ack(STREAM_STRATEGY_SIGNALS, _GROUP, msg_id)

        if not messages:
            await asyncio.sleep(1.0)  # back-off when queue empty


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=False)

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
