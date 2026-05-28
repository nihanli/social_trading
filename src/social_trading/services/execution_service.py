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
from social_trading.core.events import STREAM_SELECTED_SIGNALS
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


async def _write_market_snapshot(
    redis: aioredis.Redis,
    ticker: str,
    market_data: YFinanceMarketData,
) -> None:
    """
    Fetch and write market snapshot to Redis hash 'market_data:{ticker}'.
    Used by risk_service and signal_service (price alignment check).
    """
    try:
        quote = await market_data.get_quote(ticker)
        atr = await market_data.get_atr(ticker)
        realised_vol = await market_data.get_realised_vol(ticker)
        vix = await market_data.get_vix()

        # ADV as shares from volume; use avg_volume_30d
        adv_shares = quote.get("avg_volume_30d", 1_000_000.0)
        last = quote.get("last", 0.0)
        adv_usd = adv_shares * last if last > 0 else 100_000_000.0

        await redis.hset(f"market_data:{ticker}", mapping={
            "last": str(last),
            "bid": str(quote.get("bid", 0.0)),
            "ask": str(quote.get("ask", 0.0)),
            "adv_shares": str(adv_shares),
            "adv_usd": str(adv_usd),
            "market_cap_usd": str(quote.get("market_cap", 0.0)),
            "atr_14": str(atr),
            "realised_vol": str(realised_vol),
            "vix": str(vix),
            "updated_at": datetime.now(UTC).isoformat(),
        })
        # Also update engine price cache
    except Exception as exc:
        logger.debug("Market snapshot failed for %s: %s", ticker, exc)


# ── Service loops ─────────────────────────────────────────────────────────────

async def run_trade_loop(
    bus: TradingEventBus,
    engine: PaperTradingEngine,
    redis: aioredis.Redis,
    market_data: YFinanceMarketData | None = None,
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
        messages = await bus.consume(
            STREAM_SELECTED_SIGNALS, _GROUP, _CONSUMER, count=_INGEST_BATCH
        )

        for msg_id, fields in messages:
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

            # Skip if position already open
            if signal.ticker in engine.open_tickers:
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
            )

            if result.status == "filled":
                submitted += 1
                ORDERS_PLACED.labels(ticker=signal.ticker, status="filled").inc()
                # Persist trade to Redis list for UI
                await redis.lpush("trades:recent", str({
                    "ticker": signal.ticker,
                    "direction": signal.direction,
                    "quantity": quantity,
                    "fill_price": result.fill_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "submitted_at": result.submitted_at.isoformat(),
                    "quality_score": signal.quality_score,
                }))
                await redis.ltrim("trades:recent", 0, 999)  # keep last 1000
                logger.info(
                    "[EXEC] Submitted %s %s qty=%d fill=%.4f [total=%d]",
                    signal.direction, signal.ticker, quantity,
                    result.fill_price or 0.0, submitted,
                )
            else:
                ORDERS_PLACED.labels(ticker=signal.ticker, status="rejected").inc()
                logger.warning(
                    "[EXEC] Rejected %s: %s", signal.ticker, result.error
                )

            await bus.ack(STREAM_SELECTED_SIGNALS, _GROUP, msg_id)

        if not messages:
            await asyncio.sleep(1.0)


async def run_exit_loop(
    engine: PaperTradingEngine,
    exit_manager: PositionExitManager,
    market_data: YFinanceMarketData,
    breaker: CircuitBreaker,
    redis: aioredis.Redis,
) -> None:
    """
    Every poll_interval seconds:
      1. Refresh market data snapshots for open tickers + watchlist
      2. Update engine price cache
      3. Evaluate exit rules for every open position
      4. Close positions that triggered an exit
      5. Write account state to Redis
    """
    logger.info("Execution exit loop started")

    # Watchlist tickers are refreshed at most once every WATCHLIST_REFRESH_SECS
    # to avoid hammering yfinance/DNS with hundreds of concurrent requests.
    WATCHLIST_REFRESH_SECS = 300  # 5 minutes
    _watchlist_last_refresh: dict[str, float] = {}

    while True:
        cfg = await SystemConfig.load(redis)

        # Fetch VIX once per cycle (shared across all ticker snapshots)
        vix = await market_data.get_vix()
        await redis.set("market:vix", str(vix))

        # Refresh market data for all open tickers
        open_positions = await engine.get_positions()
        open_tickers = {p.ticker for p in open_positions}

        # Also refresh watchlist tickers so risk/signal services have fresh data,
        # but throttle to at most once every WATCHLIST_REFRESH_SECS per ticker.
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

        # Re-fetch positions after price updates (HWM may have moved)
        open_positions = await engine.get_positions()
        now = datetime.now(UTC)

        for pos in open_positions:
            current_price = engine.get_price(pos.ticker) or pos.entry_price
            decision = exit_manager.evaluate(pos, current_price, cfg, now=now)
            if decision.should_exit:
                await engine.close_position(pos.ticker, reason=decision.reason)
                POSITIONS_CLOSED.labels(reason=decision.reason or "unknown").inc()
                logger.info(
                    "[EXIT] %s %s reason=%s pnl_approx=%.2f",
                    pos.direction, pos.ticker, decision.reason,
                    (current_price - pos.entry_price) * pos.shares
                    if pos.direction == "LONG"
                    else (pos.entry_price - current_price) * pos.shares,
                )

        # Write account state (read by risk service)
        await _write_account_state(redis, engine)

        # Update Prometheus account metrics
        state = await engine.get_account_state()
        PAPER_EQUITY.set(state.net_liquidation)
        DAILY_PNL_PCT.set(state.daily_pnl / state.net_liquidation if state.net_liquidation else 0)
        DRAWDOWN.set(state.drawdown_pct)
        remaining_positions = await engine.get_positions()
        OPEN_POSITIONS_COUNT.set(len(remaining_positions))
        for pos in remaining_positions:
            cur = engine.get_price(pos.ticker) or pos.entry_price
            pnl = (cur - pos.entry_price) * pos.shares if pos.direction == "LONG" \
                else (pos.entry_price - cur) * pos.shares
            POSITION_PNL.labels(ticker=pos.ticker, direction=pos.direction).set(pnl)

        await asyncio.sleep(cfg.signal_poll_interval_sec)


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
            adv_shares = quote.get("avg_volume_30d", 1_000_000.0)
            adv_usd = adv_shares * last

            await redis.hset(f"market_data:{ticker}", mapping={
                "last": str(last),
                "bid": str(quote.get("bid", last * 0.999)),
                "ask": str(quote.get("ask", last * 1.001)),
                "adv_shares": str(adv_shares),
                "adv_usd": str(adv_usd),
                "market_cap_usd": str(quote.get("market_cap", 0.0)),
                "atr_14": str(atr),
                "realised_vol": str(realised_vol),
                "vix": str(vix),
                "updated_at": datetime.now(UTC).isoformat(),
            })
            return float(last)
    except Exception as exc:
        logger.debug("Snapshot failed for %s: %s", ticker, exc)
    return None



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
            await ib.connectAsync("127.0.0.1", port, clientId=client_id)
            engine: PaperTradingEngine = IBKRExecutionEngine(ib=ib)  # type: ignore[assignment]
            # Use IB for real-time prices; yfinance as fallback for any gaps
            market_data: YFinanceMarketData = FallbackMarketData(  # type: ignore[assignment]
                primary=IBKRMarketData(ib=ib),
                secondary=YFinanceMarketData(),
            )
            logger.info("Connected to IBKR port=%d clientId=%d (IB market data primary)", port, client_id)
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

    tasks = [
        asyncio.create_task(
            run_trade_loop(bus, engine, redis, market_data),
            name="exec:trade",
        ),
        asyncio.create_task(
            run_exit_loop(engine, exit_manager, market_data, breaker, redis),
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
