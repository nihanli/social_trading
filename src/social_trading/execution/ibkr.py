"""
IBKRExecutionEngine — ExecutionEngine backed by Interactive Brokers via ib_async.

Places bracket orders (entry market order + stop-loss + take-profit limit order)
using the pattern documented in docs/design/07-execution-ibkr.md §7b.

The IB client is injectable so unit tests can pass a fake object without
requiring a live IBKR connection or the ib_async package.

Design reference: docs/design/07-execution-ibkr.md
Protocol reference: docs/plan/02-protocols-and-interfaces.md §4

Usage:
    from ib_async import IB
    ib = IB()
    await ib.connectAsync('127.0.0.1', 7497, clientId=10)   # paper
    engine = IBKRExecutionEngine(ib=ib)
    result = await engine.submit_signal(signal, quantity=50, stop_loss=170.0, take_profit=186.0)
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from social_trading.core.models import (
    AccountState,
    Direction,
    OrderResult,
    Position,
    Signal,
)

logger = logging.getLogger(__name__)

_IB_AVAILABLE: bool
try:
    import ib_async as _ib_async_mod  # noqa: F401
    _IB_AVAILABLE = True
except ImportError:
    _IB_AVAILABLE = False


class IBKRExecutionEngine:
    """
    Live execution via Interactive Brokers.

    Requires an authenticated ib_async.IB() connection.
    The caller is responsible for connecting/disconnecting.

    Args:
        ib:           ib_async.IB instance (or injectable fake).
        paper_prices: Optional price dict for fallback when market data is
                      temporarily unavailable (used in integration tests).
    """

    def __init__(
        self,
        ib: Any,
        paper_prices: dict[str, float] | None = None,
    ) -> None:
        self._ib = ib
        self._paper_prices = paper_prices or {}
        self._prices: dict[str, float] = {}
        self._hwm: dict[str, float] = {}  # high-water marks for trailing stops
        # Persisted position params: stop_loss, take_profit, opened_at per ticker.
        # Restored from Redis on restart so exit rules work correctly.
        self._position_params: dict[str, dict] = {}

    # ── Price cache (mirrors PaperTradingEngine interface) ────────────────────

    def set_price(self, ticker: str, price: float) -> None:
        """Cache latest price and update high-water mark for trailing stop."""
        self._prices[ticker] = price
        # Ratchet HWM: for LONG track maximum, for SHORT track minimum.
        # Direction unknown here, so we track both candidates; get_positions()
        # applies the correct one when building Position objects.
        prev = self._hwm.get(ticker)
        if prev is None:
            self._hwm[ticker] = price
        else:
            # Store the maximum seen — get_positions flips for SHORT
            self._hwm[ticker] = max(prev, price)

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_hwm(self) -> dict[str, float]:
        """Return a snapshot of all tracked high-water marks."""
        return dict(self._hwm)

    def seed_hwm(self, ticker: str, value: float) -> None:
        """Restore a persisted HWM value; only applies when not already tracked."""
        if ticker not in self._hwm:
            self._hwm[ticker] = value

    def get_position_params(self) -> dict[str, dict]:
        """Return a snapshot of persisted position params (sl/tp/opened_at)."""
        return dict(self._position_params)

    def seed_position_params(self, ticker: str, params: dict) -> None:
        """Restore position params from persistent store; only if not already set."""
        if ticker not in self._position_params:
            self._position_params[ticker] = params

    def forget_position(self, ticker: str) -> None:
        """Remove all in-memory state for a ticker that was closed externally (e.g. IB bracket fill)."""
        self._hwm.pop(ticker, None)
        self._position_params.pop(ticker, None)

    @property
    def open_tickers(self) -> set[str]:
        """Return set of tickers with open IBKR positions."""
        return {p.contract.symbol for p in self._ib.positions() if p.position != 0}

    async def submit_signal(
        self,
        signal: Signal,
        quantity: int,
        stop_loss: float,
        take_profit: float,
    ) -> OrderResult:
        """
        Place a bracket order for a signal.

        Order structure:
            parent  → MarketOrder (transmit=False)
            stop    → StopOrder (parentId=parent, transmit=False)
            target  → LimitOrder (parentId=parent, transmit=True — triggers whole bracket)
        """
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed — use PaperTradingEngine for testing")

        from ib_async import LimitOrder, MarketOrder, Stock, StopOrder  # noqa: PLC0415

        ticker = signal.ticker
        action = "BUY" if signal.direction == "LONG" else "SELL"
        close_action = "SELL" if action == "BUY" else "BUY"

        try:
            contract = Stock(ticker, "SMART", "USD")
            await self._ib.qualifyContractsAsync(contract)

            # Place parent market order (held until child legs are registered)
            parent = MarketOrder(action, quantity)
            parent.transmit = False
            parent_trade = self._ib.placeOrder(contract, parent)
            parent_id = parent_trade.order.orderId

            # Stop-loss child
            stop = StopOrder(close_action, quantity, stop_loss)
            stop.parentId = parent_id
            stop.transmit = False
            self._ib.placeOrder(contract, stop)

            # Take-profit child — transmit=True releases the entire bracket atomically
            target = LimitOrder(close_action, quantity, take_profit)
            target.parentId = parent_id
            target.transmit = True
            self._ib.placeOrder(contract, target)

            # Wait briefly for fill confirmation
            await asyncio.sleep(0.5)

            # Read fill price from parent trade
            fill_price: float | None = None
            if parent_trade.fills:
                fill_price = float(parent_trade.fills[0].execution.price)

            order_id = str(parent_id)
            # Save position params so exit rules (sl/tp/time) work correctly after restart
            self._position_params[ticker] = {
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "opened_at": datetime.now(UTC).isoformat(),
                "direction": signal.direction,
            }
            logger.info(
                "[IBKR] BRACKET %s %s qty=%d sl=%.4f tp=%.4f fill=%.4f",
                action, ticker, quantity, stop_loss, take_profit,
                fill_price or 0.0,
            )
            return OrderResult(
                order_id=order_id,
                ticker=ticker,
                direction=signal.direction,
                quantity=quantity,
                fill_price=fill_price,
                status="submitted",
            )

        except Exception as exc:
            logger.error("[IBKR] Order failed for %s: %s", ticker, exc)
            return OrderResult(
                order_id=str(uuid.uuid4()),
                ticker=ticker,
                direction=signal.direction,
                quantity=quantity,
                status="rejected",
                error=str(exc),
            )

    async def close_position(self, ticker: str, reason: str = "") -> OrderResult:
        """
        Cancel all open orders for ticker then submit a market close order.
        """
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed")

        from ib_async import MarketOrder, Stock  # noqa: PLC0415

        try:
            contract = Stock(ticker, "SMART", "USD")
            await self._ib.qualifyContractsAsync(contract)

            # Cancel bracket child orders (stop-loss + take-profit legs).
            # Must use openTrades() — it exposes both .order and .contract.
            # openOrders() returns Order objects which have no .contract attribute.
            for trade in self._ib.openTrades():
                if trade.contract.symbol == ticker:
                    self._ib.cancelOrder(trade.order)

            # Determine close action from existing position
            positions = self._ib.positions()
            pos_qty = 0
            for p in positions:
                if p.contract.symbol == ticker:
                    pos_qty = int(p.position)
                    break

            if pos_qty == 0:
                return OrderResult(
                    order_id=str(uuid.uuid4()),
                    ticker=ticker,
                    direction="LONG",
                    quantity=0,
                    status="rejected",
                    error=f"No open IBKR position for {ticker}",
                )

            close_action = "SELL" if pos_qty > 0 else "BUY"
            direction: Direction = "LONG" if pos_qty > 0 else "SHORT"
            close_order = MarketOrder(close_action, abs(pos_qty))
            close_order.transmit = True  # must be explicit — default varies by ib_async version
            trade = self._ib.placeOrder(contract, close_order)
            await asyncio.sleep(0.5)

            fill_price: float | None = None
            if trade.fills:
                fill_price = float(trade.fills[0].execution.price)

            # Clean up in-memory state so re-entry on same ticker starts fresh
            self._hwm.pop(ticker, None)
            self._position_params.pop(ticker, None)

            logger.info("[IBKR] CLOSE %s qty=%d reason=%s fill=%.4f",
                        ticker, abs(pos_qty), reason, fill_price or 0.0)
            return OrderResult(
                order_id=str(trade.order.orderId),
                ticker=ticker,
                direction=direction,
                quantity=abs(pos_qty),
                fill_price=fill_price,
                status="submitted",
            )

        except Exception as exc:
            logger.error("[IBKR] Close failed for %s: %s", ticker, exc)
            return OrderResult(
                order_id=str(uuid.uuid4()),
                ticker=ticker,
                direction="LONG",
                quantity=0,
                status="rejected",
                error=str(exc),
            )

    async def get_positions(self) -> list[Position]:
        """Return open positions from IBKR as Position objects."""
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed")

        positions = []
        for p in self._ib.positions():
            if p.position == 0:
                continue
            direction: Direction = "LONG" if p.position > 0 else "SHORT"
            entry_price = float(p.avgCost / abs(p.position)) if p.position != 0 else 0.0
            ticker = p.contract.symbol

            # Seed HWM on first encounter (before any set_price call has arrived)
            if ticker not in self._hwm:
                current = self._prices.get(ticker, entry_price)
                self._hwm[ticker] = current

            # Apply correct HWM polarity: LONG tracks max, SHORT tracks min
            raw_hwm = self._hwm.get(ticker, entry_price)
            if direction == "SHORT":
                # For shorts, HWM is the lowest price seen; invert the stored max
                current_price = self._prices.get(ticker, entry_price)
                raw_hwm = min(raw_hwm, current_price) if raw_hwm != entry_price else current_price
                self._hwm[ticker] = raw_hwm

            # Restore persisted position params (sl/tp/opened_at) if available.
            # Without these, exit rules 2 (STOP_LOSS) and 3 (TAKE_PROFIT) silently skip
            # on IBKR restart since IB doesn't expose bracket leg prices.
            params = self._position_params.get(ticker, {})
            stop_loss = float(params.get("stop_loss", 0.0))
            take_profit = float(params.get("take_profit", 0.0))
            try:
                opened_at = datetime.fromisoformat(params["opened_at"]) if "opened_at" in params else datetime.now(UTC)
            except (ValueError, KeyError):
                opened_at = datetime.now(UTC)

            positions.append(Position(
                ticker=ticker,
                direction=direction,
                shares=abs(int(p.position)),
                entry_price=entry_price,
                opened_at=opened_at,
                stop_loss=stop_loss,
                take_profit=take_profit,
                unrealized_pnl=float(p.unrealizedPNL or 0),
                high_water_mark=raw_hwm,
            ))
        return positions

    async def get_account_state(self) -> AccountState:
        """Return account state from IBKR portfolio summary."""
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed")

        account_values = self._ib.accountValues()
        summary: dict[str, float] = {}
        for av in account_values:
            if av.currency == "USD":
                import contextlib
                with contextlib.suppress(ValueError):
                    summary[av.tag] = float(av.value)

        nlv = summary.get("NetLiquidation", 0.0)
        cash = summary.get("TotalCashValue", 0.0)
        daily_pnl = summary.get("DailyPnL", 0.0)
        unrealized = summary.get("UnrealizedPnL", 0.0)

        positions = await self.get_positions()
        return AccountState(
            net_liquidation=nlv,
            cash=cash,
            daily_pnl=daily_pnl,
            weekly_pnl=unrealized,  # IBKR doesn't track weekly natively
            drawdown_pct=0.0,       # computed externally
            open_positions=positions,
        )

    async def health_check(self) -> bool:
        return bool(self._ib.isConnected())
