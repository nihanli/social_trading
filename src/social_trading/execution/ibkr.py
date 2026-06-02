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
        account: str = "",
        paper_prices: dict[str, float] | None = None,
    ) -> None:
        self._ib = ib
        self._account = account  # IB account number, e.g. "DU123456" (paper) or "U123456" (live)
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
        Submit a market entry order, wait for IB acknowledgement, then attach
        OCA stop/limit orders as a server-side failsafe.

        Sequence:
            1. MarketOrder(transmit=True) — immediately live on IB side
            2. Poll until order reaches Submitted/PreSubmitted (IB accepted)
               OR Filled (paper fill arrived quickly).
               Reject only if order goes Inactive/Cancelled (IB hard-reject).
            3. Attach OCA stop + limit (both transmit=True).
               When fill_price is not yet known we trust the risk service's
               SL/TP values and skip the directional sanity-check.
            4. If OCA submission raises a Python exception: immediately close
               the position to avoid an unprotected open.

        Notes:
            - IB paper accounts may fill market orders with a delay if market
              data subscriptions are missing (Error 10089).  Treating
              "Submitted" as success avoids incorrectly cancelling live orders.
            - The software exit loop is the primary SL/TP/trailing-stop manager.
              OCA orders are a server-side failsafe if the service goes offline.
        """
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed — use PaperTradingEngine for testing")

        from ib_async import LimitOrder, MarketOrder, OrderStatus, Stock, StopOrder  # noqa: PLC0415

        _SUBMIT_TIMEOUT_SEC = 10   # time to wait for IB to acknowledge the order
        _POLL_INTERVAL      = 0.25

        ticker = signal.ticker
        action = "BUY" if signal.direction == "LONG" else "SELL"
        close_action = "SELL" if action == "BUY" else "BUY"

        entry_trade = None
        entry_id: int = 0

        try:
            contract = Stock(ticker, "SMART", "USD")
            await self._ib.qualifyContractsAsync(contract)

            # ── 1. Entry order — transmit immediately ────────────────────────
            entry = MarketOrder(action, quantity)
            entry.transmit = True
            if self._account:
                entry.account = self._account
            entry_trade = self._ib.placeOrder(contract, entry)
            entry_id = entry_trade.order.orderId
            logger.info(
                "[IBKR] ENTRY submitted %s %s qty=%d orderId=%d",
                action, ticker, quantity, entry_id,
            )

            # ── 2. Wait for IB acknowledgement or fill ───────────────────────
            # We accept Submitted/PreSubmitted (IB received the order) as well as
            # Filled (paper fill arrived quickly).  We only reject if IB actively
            # rejects the order (Inactive/Cancelled/ApiCancelled) or if the order
            # never leaves PendingSubmit within the timeout (connection stall).
            elapsed = 0.0
            order_accepted = False
            while elapsed < _SUBMIT_TIMEOUT_SEC:
                await asyncio.sleep(_POLL_INTERVAL)
                elapsed += _POLL_INTERVAL
                status = entry_trade.orderStatus.status
                if status in (
                    OrderStatus.Submitted,
                    OrderStatus.PreSubmitted,
                    OrderStatus.Filled,
                ):
                    order_accepted = True
                    break
                if status in OrderStatus.DoneStates:
                    # IB rejected the order outright
                    logger.error(
                        "[IBKR] ENTRY rejected by IB: %s orderId=%d status=%s",
                        ticker, entry_id, status,
                    )
                    return OrderResult(
                        order_id=str(entry_id),
                        ticker=ticker,
                        direction=signal.direction,
                        quantity=quantity,
                        status="rejected",
                        error=f"IB rejected order: {status}",
                    )

            if not order_accepted:
                # Order stuck in PendingSubmit — connection or TWS issue
                self._ib.cancelOrder(entry_trade.order)
                logger.error(
                    "[IBKR] ENTRY not acknowledged after %.1fs, cancelled: %s orderId=%d",
                    _SUBMIT_TIMEOUT_SEC, ticker, entry_id,
                )
                return OrderResult(
                    order_id=str(entry_id),
                    ticker=ticker,
                    direction=signal.direction,
                    quantity=quantity,
                    status="rejected",
                    error=f"order not acknowledged after {_SUBMIT_TIMEOUT_SEC}s",
                )

            # Use actual fill price if already available; None otherwise
            # (paper fills with delayed data may arrive after we return)
            fill_price: float | None = None
            # Capture opened_at ONCE here so position_params and the
            # position_opened event always carry the exact same timestamp.
            opened_at_dt = datetime.now(UTC)
            if entry_trade.fills:
                fill_price = float(entry_trade.fills[0].execution.price)
                logger.info(
                    "[IBKR] ENTRY filled %s %s qty=%d fill=%.4f",
                    action, ticker, quantity, fill_price,
                )
            else:
                logger.info(
                    "[IBKR] ENTRY accepted %s %s qty=%d status=%s (fill pending)",
                    action, ticker, quantity, entry_trade.orderStatus.status,
                )

            # ── 3. Attach OCA stop + limit ───────────────────────────────────
            # When fill_price is known, validate direction; otherwise trust the
            # risk service (SL/TP were computed from the same entry price).
            def _sl_valid(fp: float | None) -> bool:
                if stop_loss <= 0:
                    return False
                if fp is None:
                    return True   # trust risk service
                return (
                    (signal.direction == "LONG"  and stop_loss  < fp)
                    or (signal.direction == "SHORT" and stop_loss  > fp)
                )

            def _tp_valid(fp: float | None) -> bool:
                if take_profit <= 0:
                    return False
                if fp is None:
                    return True   # trust risk service
                return (
                    (signal.direction == "LONG"  and take_profit > fp)
                    or (signal.direction == "SHORT" and take_profit < fp)
                )

            oca_group = f"oca_{entry_id}"
            oca_errors: list[str] = []
            oca_trades = []

            if _sl_valid(fill_price):
                try:
                    stop_order = StopOrder(close_action, quantity, round(stop_loss, 2))
                    stop_order.ocaGroup   = oca_group
                    stop_order.ocaType    = 1     # cancel sibling on fill
                    stop_order.tif        = "GTC" # persist until triggered or cancelled
                    stop_order.outsideRth = True  # trigger in after-hours (protective stop)
                    stop_order.transmit   = True
                    if self._account:
                        stop_order.account = self._account
                    sl_trade = self._ib.placeOrder(contract, stop_order)
                    oca_trades.append(sl_trade)
                    logger.info(
                        "[IBKR] OCA stop placed: %s sl=%.4f orderId=%d",
                        ticker, stop_loss, sl_trade.order.orderId,
                    )
                except Exception as exc:
                    oca_errors.append(f"stop: {exc}")
                    logger.error("[IBKR] OCA stop failed for %s: %s", ticker, exc)
            else:
                logger.warning(
                    "[IBKR] stop_loss=%.4f invalid vs fill=%s (%s) — OCA stop skipped",
                    stop_loss, fill_price, signal.direction,
                )

            if _tp_valid(fill_price):
                try:
                    limit_order = LimitOrder(close_action, quantity, round(take_profit, 2))
                    limit_order.ocaGroup = oca_group
                    limit_order.ocaType  = 1
                    limit_order.tif      = "GTC"  # persist until triggered or cancelled
                    limit_order.transmit = True
                    if self._account:
                        limit_order.account = self._account
                    tp_trade = self._ib.placeOrder(contract, limit_order)
                    oca_trades.append(tp_trade)
                    logger.info(
                        "[IBKR] OCA limit placed: %s tp=%.4f orderId=%d",
                        ticker, take_profit, tp_trade.order.orderId,
                    )
                except Exception as exc:
                    oca_errors.append(f"limit: {exc}")
                    logger.error("[IBKR] OCA limit failed for %s: %s", ticker, exc)
            else:
                logger.warning(
                    "[IBKR] take_profit=%.4f invalid vs fill=%s (%s) — OCA limit skipped",
                    take_profit, fill_price, signal.direction,
                )

            # Flush OCA messages to IB and allow acknowledgement callbacks to fire
            if oca_trades:
                await asyncio.sleep(0.5)
                for t in oca_trades:
                    logger.debug(
                        "[IBKR] OCA order status %s orderId=%d status=%s",
                        ticker, t.order.orderId, t.orderStatus.status,
                    )

            # ── 4. If OCA raised a Python exception, close immediately ────────
            if oca_errors:
                logger.error(
                    "[IBKR] OCA submission failed (%s), closing position: %s",
                    oca_errors, ticker,
                )
                await self.close_position(ticker, reason="OCA_FAILED")
                return OrderResult(
                    order_id=str(entry_id),
                    ticker=ticker,
                    direction=signal.direction,
                    quantity=quantity,
                    fill_price=fill_price,
                    status="rejected",
                    error=f"OCA failed, position closed: {oca_errors}",
                )

            # Save position params so exit rules work correctly after restart
            self._position_params[ticker] = {
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "opened_at": opened_at_dt.isoformat(),
                "direction": signal.direction,
            }
            return OrderResult(
                order_id=str(entry_id),
                ticker=ticker,
                direction=signal.direction,
                quantity=quantity,
                fill_price=fill_price,
                submitted_at=opened_at_dt,
                status="submitted",
            )

        except Exception as exc:
            logger.error("[IBKR] submit_signal failed for %s: %s", ticker, exc)
            # If the entry was placed and accepted, attempt emergency close
            if entry_trade is not None and (
                entry_trade.fills or entry_trade.orderStatus.status in (
                    OrderStatus.Submitted, OrderStatus.Filled
                )
            ):
                logger.warning("[IBKR] Emergency close after submit error: %s", ticker)
                try:
                    await self.close_position(ticker, reason="SUBMIT_ERROR")
                except Exception as close_exc:
                    logger.error("[IBKR] Emergency close failed: %s", close_exc)
            return OrderResult(
                order_id=str(entry_id) if entry_id else str(uuid.uuid4()),
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
            if self._account:
                close_order.account = self._account
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
        """Return open positions from IBKR as Position objects.

        Only positions belonging to the configured account are returned.
        """
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed")

        positions = []
        for p in self._ib.positions():
            if p.position == 0:
                continue
            # Filter to configured account only
            if self._account and p.account != self._account:
                continue

            ticker = p.contract.symbol
            total_qty = float(p.position)
            # ib_async avgCost = per-share cost basis
            entry_price = round(float(p.avgCost), 4)
            direction: Direction = "LONG" if total_qty > 0 else "SHORT"
            shares = abs(int(round(total_qty)))

            # Seed HWM on first encounter (before any set_price call has arrived)
            if ticker not in self._hwm:
                current = self._prices.get(ticker, entry_price)
                self._hwm[ticker] = current

            # Apply correct HWM polarity: LONG tracks max, SHORT tracks min
            raw_hwm = self._hwm.get(ticker, entry_price)
            if direction == "SHORT":
                current_price = self._prices.get(ticker, entry_price)
                raw_hwm = min(raw_hwm, current_price) if raw_hwm != entry_price else current_price
                self._hwm[ticker] = raw_hwm

            # Restore persisted position params (sl/tp/opened_at)
            params = self._position_params.get(ticker, {})
            stop_loss = float(params.get("stop_loss", 0.0))
            take_profit = float(params.get("take_profit", 0.0))
            try:
                opened_at = datetime.fromisoformat(params["opened_at"]) if "opened_at" in params else datetime.now(UTC)
            except (ValueError, KeyError):
                opened_at = datetime.now(UTC)

            # Compute unrealised PnL from price cache
            current_price = self._prices.get(ticker, entry_price)
            if direction == "LONG":
                unrealized = (current_price - entry_price) * shares
            else:
                unrealized = (entry_price - current_price) * shares

            positions.append(Position(
                ticker=ticker,
                direction=direction,
                shares=shares,
                entry_price=entry_price,
                opened_at=opened_at,
                stop_loss=stop_loss,
                take_profit=take_profit,
                unrealized_pnl=round(unrealized, 4),
                high_water_mark=raw_hwm,
            ))
        return positions

    async def get_account_state(self) -> AccountState:
        """Return account state from IBKR for the configured single user account."""
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed")

        account_values = self._ib.accountValues()

        summary: dict[str, float] = {}
        for av in account_values:
            if self._account and av.account != self._account:
                continue
            if av.currency in ("USD", "BASE"):
                import contextlib
                with contextlib.suppress(ValueError):
                    summary[av.tag] = float(av.value)

        nlv = summary.get("NetLiquidation", 0.0)
        cash = summary.get("TotalCashValue", 0.0)
        daily_pnl = summary.get("DailyPnL", 0.0)
        unrealized = summary.get("UnrealizedPnL", 0.0)

        if nlv == 0.0:
            logger.warning(
                "[IBKR] NetLiquidation is 0 — accountValues returned %d entries "
                "(account filter=%r). Check IB connection / account subscription.",
                len(account_values), self._account or "(none)",
            )

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
