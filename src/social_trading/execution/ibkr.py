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

            parent_id = self._ib.client.getReqId()
            stop_id = self._ib.client.getReqId()
            target_id = self._ib.client.getReqId()

            parent = MarketOrder(action, quantity)
            parent.orderId = parent_id
            parent.transmit = False

            stop = StopOrder(close_action, quantity, stop_loss)
            stop.orderId = stop_id
            stop.parentId = parent_id
            stop.transmit = False

            target = LimitOrder(close_action, quantity, take_profit)
            target.orderId = target_id
            target.parentId = parent_id
            target.transmit = True  # releases the entire bracket

            trades = [
                self._ib.placeOrder(contract, parent),
                self._ib.placeOrder(contract, stop),
                self._ib.placeOrder(contract, target),
            ]

            # Wait briefly for fill confirmation
            await asyncio.sleep(0.5)

            # Read fill price from parent trade
            fill_price: float | None = None
            if trades[0].fills:
                fill_price = float(trades[0].fills[0].execution.price)

            order_id = str(parent_id)
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

            # Cancel open orders for this contract first
            open_orders = self._ib.openOrders()
            for order in open_orders:
                if hasattr(order, "contract") and order.contract.symbol == ticker:
                    self._ib.cancelOrder(order)

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
            trade = self._ib.placeOrder(contract, close_order)
            await asyncio.sleep(0.5)

            fill_price: float | None = None
            if trade.fills:
                fill_price = float(trade.fills[0].execution.price)

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
            positions.append(Position(
                ticker=p.contract.symbol,
                direction=direction,
                shares=abs(int(p.position)),
                entry_price=entry_price,
                opened_at=datetime.now(UTC),   # IBKR doesn't provide this directly
                stop_loss=0.0,
                take_profit=0.0,
                unrealized_pnl=float(p.unrealizedPNL or 0),
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
