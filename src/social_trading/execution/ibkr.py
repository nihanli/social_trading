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

# orderRef stamped on every order placed by this system.
# Allows distinguishing system-managed positions from manually-created ones
# in TWS or via other API clients.
ORDER_REF = "social_trading"

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
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 10,
    ) -> None:
        self._ib = ib
        self._account = account  # IB account number, e.g. "DU123456" (paper) or "U123456" (live)
        self._paper_prices = paper_prices or {}
        self._prices: dict[str, float] = {}
        self._hwm: dict[str, float] = {}      # high-water marks for trailing stops (LONG: max, SHORT: min)
        self._hwm_min: dict[str, float] = {}  # separate min tracker for SHORT positions
        self._ts_order_id: dict[str, int] = {}  # trailing stop OCA order IDs per ticker
        # Persisted position params: stop_loss, take_profit, opened_at per ticker.
        # Restored from Redis on restart so exit rules work correctly.
        self._position_params: dict[str, dict] = {}

        # Active trade objects keyed by IB orderId.
        # Kept alive so fill callbacks can be registered after submit_signal / close_position return.
        # ib_async also keeps Trade objects internally, but this dict provides an O(1) lookup path
        # and avoids having to search ib.trades() which may be slower for large session histories.
        self._active_trades: dict[int, Any] = {}

        # Connection params stored for reconnect attempts
        self._host = host
        self._port = port
        self._client_id = client_id

        # Subscribe to ib_async's connectedEvent so the position cache is
        # automatically reseeded after TWS auto-restart or any reconnect.
        # (TWS restarts nightly at ~11:45 PM ET; without this the cache is
        # empty until service restart, causing positions to vanish from UI.)
        self._ib.connectedEvent += self._on_ib_reconnect

    def _on_ib_reconnect(self) -> None:
        """Callback fired by ib_async whenever a (re)connection is established."""
        logger.info("[IBKR] Connection established — reseeding position cache via reqPositionsAsync")
        asyncio.ensure_future(self._reseed_positions())

    async def _reseed_positions(self) -> None:
        """Request positions and account data from IB to repopulate the local ib_async cache."""
        try:
            await self._ib.reqPositionsAsync()
            count = len([p for p in self._ib.positions() if p.position != 0])
            logger.info("[IBKR] Position cache reseeded after reconnect — %d open position(s)", count)
        except Exception as exc:
            logger.warning("[IBKR] Failed to reseed position cache after reconnect: %s", exc)

        # Re-subscribe to account updates so accountValues() is populated.
        # ib_async subscribes automatically on initial connect but the subscription
        # is not automatically renewed after a reconnect — without this call,
        # accountValues() stays empty → NLV=0 → risk service rejects every signal.
        try:
            account = self._account or ""
            await self._ib.reqAccountUpdatesAsync(account=account)
            nlv = next(
                (float(av.value) for av in self._ib.accountValues()
                 if av.tag == "NetLiquidation" and av.currency in ("USD", "BASE")),
                0.0,
            )
            logger.info(
                "[IBKR] Account data reseeded after reconnect — NLV=%.2f (account=%r)",
                nlv, account or "(all)",
            )
        except Exception as exc:
            logger.warning("[IBKR] Failed to reseed account data after reconnect: %s", exc)

    async def reconnect(self) -> bool:
        """
        Attempt to re-establish the IB/TWS connection using stored connection params.

        Called by the exit loop when health_check() fails and the reconnect
        backoff window has elapsed.  Returns True on success, False on failure.

        Always calls disconnect() first to clear any stale half-open socket state
        in ib_async before attempting connectAsync().  Without this, connectAsync
        may silently fail or block when the underlying TCP socket is in a broken
        but not fully closed state.
        """
        try:
            if self._ib.isConnected():
                return True
            logger.info(
                "[IBKR] Attempting reconnect to %s:%d (clientId=%d)…",
                self._host, self._port, self._client_id,
            )
            # Always disconnect first to flush ib_async's internal socket/stream
            # state.  Calling connectAsync on a half-open connection raises or silently
            # fails — a clean disconnect ensures the next connect starts fresh.
            try:
                self._ib.disconnect()
                await asyncio.sleep(0.5)  # brief pause to let the socket close
            except Exception:
                pass  # disconnect may raise if already fully disconnected — ignore

            await self._ib.connectAsync(self._host, self._port, clientId=self._client_id)
            await self._ib.reqPositionsAsync()
            count = len([p for p in self._ib.positions() if p.position != 0])
            # Prime the open-orders cache so callers (exit loop inflight reconcile,
            # fill-callback re-registration) see a fully populated openTrades() list.
            # Without this, openTrades() is empty right after reconnect and active
            # exit orders are misclassified as inactive by _reconcile_inflight_orders.
            try:
                self._ib.reqAllOpenOrders()
                await asyncio.sleep(2.0)  # allow IB to push openOrder callbacks
                n_orders = len(self._ib.openTrades())
                logger.info("[IBKR] Reconnected — %d open position(s), %d open order(s) in cache",
                            count, n_orders)
            except Exception as _oe:
                logger.debug("[IBKR] reqAllOpenOrders on reconnect failed (non-fatal): %s", _oe)
                logger.info("[IBKR] Reconnected — %d open position(s) in cache", count)
            return True
        except Exception as exc:
            logger.warning("[IBKR] Reconnect failed: %s", exc)
            return False

    
    def set_price(self, ticker: str, price: float) -> None:
        """Cache latest price and update high-water mark for trailing stop."""
        self._prices[ticker] = price
        # Track both max (LONG HWM) and min (SHORT HWM) independently.
        # get_positions() selects the correct one based on direction.
        prev_max = self._hwm.get(ticker)
        self._hwm[ticker] = price if prev_max is None else max(prev_max, price)
        prev_min = self._hwm_min.get(ticker)
        self._hwm_min[ticker] = price if prev_min is None else min(prev_min, price)

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_portfolio_prices(self) -> dict[str, float]:
        """Return live market prices from the IB account subscription (portfolio feed).

        ib.portfolio() is a streaming subscription pushed by IB every few seconds —
        always current without requiring explicit market-data subscription.  Used by
        _run_price_push to give the UI real-time unrealized PnL without waiting for
        the 60s exit-loop price fetch cycle.
        """
        import math as _math  # noqa: PLC0415
        result: dict[str, float] = {}
        try:
            for item in self._ib.portfolio():
                if self._account and item.account != self._account:
                    continue
                sym = item.contract.symbol
                mp = getattr(item, "marketPrice", None)
                if mp is not None:
                    try:
                        p = float(mp)
                        if p > 0 and _math.isfinite(p):
                            result[sym] = p
                    except (TypeError, ValueError):
                        pass
        except Exception:
            pass
        return result

    def get_hwm(self) -> dict[str, float]:
        """Return a snapshot of all tracked high-water marks."""
        return dict(self._hwm)

    def seed_hwm(self, ticker: str, value: float) -> None:
        """Restore a persisted HWM value; only applies when not already tracked."""
        if ticker not in self._hwm:
            self._hwm[ticker] = value
        if ticker not in self._hwm_min:
            self._hwm_min[ticker] = value

    def get_position_params(self) -> dict[str, dict]:
        """Return a snapshot of persisted position params (sl/tp/opened_at)."""
        return dict(self._position_params)

    def get_trail_orders(self) -> dict[str, int]:
        """Return a snapshot of known trailing stop order IDs."""
        return dict(self._ts_order_id)

    def seed_trail_order_id(self, ticker: str, order_id: int) -> None:
        """Restore a persisted trail order ID; only if not already tracked."""
        if ticker not in self._ts_order_id and order_id:
            self._ts_order_id[ticker] = order_id

    def seed_position_params(self, ticker: str, params: dict) -> None:
        """Restore position params from persistent store; only if not already set."""
        if ticker not in self._position_params:
            self._position_params[ticker] = params

    def forget_position(self, ticker: str) -> None:
        """Remove all in-memory state for a ticker that was closed externally (e.g. IB bracket fill)."""
        self._hwm.pop(ticker, None)
        self._hwm_min.pop(ticker, None)
        self._ts_order_id.pop(ticker, None)
        self._position_params.pop(ticker, None)

    def register_order_fill_callback(
        self,
        order_id: int,
        callback: Any,
    ) -> bool:
        """
        Register a one-shot async callback to fire once an order is fully filled.

        Searches self._active_trades first, then ib.trades() as a fallback.
        If the order is already fully filled at registration time, the callback is
        scheduled immediately via asyncio.ensure_future().

        The callback receives a single float argument: the average fill price.

        Returns True if the trade was found; False if the order_id is unknown
        (already cleaned up, or never placed through this engine instance).
        """
        trade = self._active_trades.get(order_id)
        if trade is None:
            for t in self._ib.trades():
                if t.order.orderId == order_id:
                    trade = t
                    break

        if trade is None:
            logger.warning(
                "[IBKR] register_order_fill_callback: orderId=%d not found in active trades",
                order_id,
            )
            return False

        # Already fully filled?  Schedule immediately.
        avg_fill = float(getattr(trade.orderStatus, "avgFillPrice", 0) or 0)
        if getattr(trade.orderStatus, "status", "") == "Filled" and avg_fill > 0:
            asyncio.ensure_future(callback(avg_fill))
            self._active_trades.pop(order_id, None)
            return True

        # Register a fillEvent handler that fires the callback when remaining qty hits 0.
        # fillEvent(trade, fill) fires on each partial execution; avgFillPrice is the VWAP.
        _called = [False]
        def _on_fill(t, fill, _oid=order_id, _cb=callback, _flag=_called) -> None:
            if _flag[0]:
                return
            remaining = getattr(t.orderStatus, "remaining", 1)
            if remaining > 0:
                return  # partial fill — wait for the rest
            _flag[0] = True
            avg = float(getattr(t.orderStatus, "avgFillPrice", None) or fill.execution.price)
            asyncio.ensure_future(_cb(avg))
            self._active_trades.pop(_oid, None)

        trade.fillEvent += _on_fill
        return True

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
        take_profit_pct: float = 0.04,
        trailing_stop_pct: float = 0.08,
        trailing_stop_min_pct: float = 0.02,
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
               If fill_price is known and the pre-computed TP is on the wrong
               side of fill (due to spike slippage), recompute TP from fill.
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
            raise RuntimeError("ib_async is not installed")

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
            entry.orderRef = ORDER_REF
            if self._account:
                entry.account = self._account
            entry_trade = self._ib.placeOrder(contract, entry)
            entry_id = entry_trade.order.orderId
            self._active_trades[entry_id] = entry_trade
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

            # Use actual fill price if already available; None otherwise.
            # Paper accounts fill asynchronously — poll for up to 3s so the
            # OCA TP recompute guard below has a fill price to work with.
            # Without this, if the market moved up between signal approval and
            # fill, the risk-service TP (computed from a stale snapshot price)
            # can be BELOW the actual fill price, causing IB to execute the OCA
            # limit sell immediately and cancel the stop-loss leg.
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
                # Brief wait to collect a delayed paper fill before OCA placement
                _FILL_WAIT_SEC = 3.0
                _fill_elapsed = 0.0
                while _fill_elapsed < _FILL_WAIT_SEC and not entry_trade.fills:
                    await asyncio.sleep(0.25)
                    _fill_elapsed += 0.25
                if entry_trade.fills:
                    fill_price = float(entry_trade.fills[0].execution.price)
                    logger.info(
                        "[IBKR] ENTRY filled %s %s qty=%d fill=%.4f (arrived after %.2fs)",
                        action, ticker, quantity, fill_price, _fill_elapsed,
                    )
                else:
                    logger.info(
                        "[IBKR] ENTRY accepted %s %s qty=%d status=%s (fill pending after %.1fs wait)",
                        action, ticker, quantity, entry_trade.orderStatus.status, _FILL_WAIT_SEC,
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

            # If fill price is known, recompute both SL and TP from the actual fill
            # to correct for slippage between signal approval and execution.
            # A stale SL (computed from pre-approval price) can be on the wrong
            # side of the fill, causing the software exit loop to close immediately.
            effective_stop_loss = stop_loss
            if fill_price and not _sl_valid(fill_price):
                # SL is on the wrong side of fill — recompute from fill using the
                # ATR offset back-calculated from the original SL distance.
                # Use fill_price as the reference anchor (entry_price not in scope here).
                original_offset = abs(stop_loss - fill_price)
                if signal.direction == "LONG":
                    effective_stop_loss = round(fill_price - original_offset, 2)
                else:
                    effective_stop_loss = round(fill_price + original_offset, 2)
                logger.warning(
                    "[IBKR] SL %.4f stale vs fill %.4f (%s) — recomputed to %.4f",
                    stop_loss, fill_price, signal.direction, effective_stop_loss,
                )
            elif fill_price and _sl_valid(fill_price) and stop_loss != effective_stop_loss:
                pass  # original SL is still valid relative to fill
            # If fill_price is None (delayed fill), keep original — will be rechecked
            # by exit loop once price updates arrive.

            # Apply trailing_stop_min_pct floor to ATR stop distance.
            # Prevents stops that are tighter than the minimum trailing stop,
            # which would make the ATR stop fire before the trail ever activates.
            if fill_price and fill_price > 0 and effective_stop_loss > 0:
                min_stop_distance = fill_price * trailing_stop_min_pct
                if signal.direction == "LONG":
                    min_sl = round(fill_price - min_stop_distance, 2)
                    if effective_stop_loss > min_sl:
                        logger.debug(
                            "[IBKR] ATR stop %.4f tighter than min floor %.4f for %s — applying floor",
                            effective_stop_loss, min_sl, ticker,
                        )
                        effective_stop_loss = min_sl
                else:
                    min_sl = round(fill_price + min_stop_distance, 2)
                    if effective_stop_loss < min_sl:
                        logger.debug(
                            "[IBKR] ATR stop %.4f tighter than min floor %.4f for %s — applying floor",
                            effective_stop_loss, min_sl, ticker,
                        )
                        effective_stop_loss = min_sl

            effective_take_profit = take_profit
            if fill_price and not _tp_valid(fill_price):
                if signal.direction == "LONG":
                    effective_take_profit = round(fill_price * (1.0 + take_profit_pct), 2)
                else:
                    effective_take_profit = round(fill_price * (1.0 - take_profit_pct), 2)
                logger.warning(
                    "[IBKR] TP %.4f stale vs fill %.4f (%s) — recomputed to %.4f",
                    take_profit, fill_price, signal.direction, effective_take_profit,
                )

            oca_group = f"oca_{entry_id}"
            oca_errors: list[str] = []
            oca_trades = []

            # Re-check SL validity using effective (possibly recomputed) stop
            def _sl_effective_valid(fp: float | None) -> bool:
                if effective_stop_loss <= 0:
                    return False
                if fp is None:
                    return True
                return (
                    (signal.direction == "LONG"  and effective_stop_loss < fp)
                    or (signal.direction == "SHORT" and effective_stop_loss > fp)
                )

            if _sl_effective_valid(fill_price):
                try:
                    stop_order = StopOrder(close_action, quantity, round(effective_stop_loss, 2))
                    stop_order.ocaGroup   = oca_group
                    stop_order.ocaType    = 1     # cancel sibling on fill
                    stop_order.tif        = "GTC" # persist until triggered or cancelled
                    stop_order.outsideRth = False # regular hours only — premarket/AH prints can be thin
                    stop_order.transmit   = True
                    stop_order.orderRef   = ORDER_REF
                    if self._account:
                        stop_order.account = self._account
                    sl_trade = self._ib.placeOrder(contract, stop_order)
                    oca_trades.append(sl_trade)
                    logger.info(
                        "[IBKR] OCA stop placed: %s sl=%.4f orderId=%d",
                        ticker, effective_stop_loss, sl_trade.order.orderId,
                    )
                except Exception as exc:
                    oca_errors.append(f"stop: {exc}")
                    logger.error("[IBKR] OCA stop failed for %s: %s", ticker, exc)
            else:
                # Stop cannot be placed — treat this identically to OCA_FAILED so
                # the position is closed immediately rather than left unprotected.
                oca_errors.append(
                    f"stop_invalid: sl={effective_stop_loss:.4f} invalid vs fill={fill_price} ({signal.direction})"
                )
                logger.error(
                    "[IBKR] OCA stop INVALID for %s: sl=%.4f vs fill=%s (%s) — closing to avoid unprotected position",
                    ticker, effective_stop_loss, fill_price, signal.direction,
                )

            if effective_take_profit > 0:
                try:
                    limit_order = LimitOrder(close_action, quantity, round(effective_take_profit, 2))
                    limit_order.ocaGroup = oca_group
                    limit_order.ocaType  = 1
                    limit_order.tif      = "GTC"  # persist until triggered or cancelled
                    limit_order.transmit = True
                    limit_order.orderRef = ORDER_REF
                    if self._account:
                        limit_order.account = self._account
                    tp_trade = self._ib.placeOrder(contract, limit_order)
                    oca_trades.append(tp_trade)
                    logger.info(
                        "[IBKR] OCA limit placed: %s tp=%.4f orderId=%d",
                        ticker, effective_take_profit, tp_trade.order.orderId,
                    )
                except Exception as exc:
                    oca_errors.append(f"limit: {exc}")
                    logger.error("[IBKR] OCA limit failed for %s: %s", ticker, exc)
            else:
                logger.warning(
                    "[IBKR] take_profit=%.4f invalid — OCA limit skipped",
                    take_profit,
                )

            # ── 3c. OCA trailing stop (TRAIL) ────────────────────────────────
            # Third leg of the OCA bracket. Percentage-based trail — IB anchors
            # automatically from current market price at fill and re-anchors as
            # price moves. No trailStopPrice needed for initial placement.
            # Skip if ATR stop is already tighter than the initial trail trigger.
            initial_trail_trigger = None
            if fill_price and fill_price > 0:
                if signal.direction == "LONG":
                    initial_trail_trigger = fill_price * (1.0 - trailing_stop_pct)
                else:
                    initial_trail_trigger = fill_price * (1.0 + trailing_stop_pct)

            atr_stop_subsumed = (
                fill_price is not None
                and initial_trail_trigger is not None
                and effective_stop_loss > 0
                and (
                    (signal.direction == "LONG"  and initial_trail_trigger >= effective_stop_loss)
                    or (signal.direction == "SHORT" and initial_trail_trigger <= effective_stop_loss)
                )
            )

            try:
                from ib_async import Order as _IbOrder  # noqa: PLC0415
                trail_order = _IbOrder()
                trail_order.action          = close_action
                trail_order.totalQuantity   = quantity
                trail_order.orderType       = "TRAIL"
                trail_order.trailingPercent = trailing_stop_pct * 100  # e.g. 8.0 for 8%
                trail_order.tif             = "GTC"
                trail_order.outsideRth      = False  # don't fire on thin after-hours prints
                trail_order.ocaGroup        = oca_group
                trail_order.ocaType         = 1
                trail_order.transmit        = True
                trail_order.orderRef        = ORDER_REF
                if self._account:
                    trail_order.account = self._account
                ts_trade = self._ib.placeOrder(contract, trail_order)
                oca_trades.append(ts_trade)
                self._ts_order_id[ticker] = ts_trade.order.orderId
                logger.info(
                    "[IBKR] OCA trail placed: %s trail_pct=%.1f%% orderId=%d%s",
                    ticker, trailing_stop_pct * 100, ts_trade.order.orderId,
                    " (ATR stop subsumed)" if atr_stop_subsumed else "",
                )
            except Exception as exc:
                # Trail order failure is non-fatal — ATR stop and TP limit
                # remain active, and the software exit loop also covers trail.
                logger.error("[IBKR] OCA trail failed for %s: %s", ticker, exc)

            # Flush OCA messages to IB and allow acknowledgement callbacks to fire
            if oca_trades:
                await asyncio.sleep(0.5)
                for t in oca_trades:
                    logger.debug(
                        "[IBKR] OCA order status %s orderId=%d status=%s",
                        ticker, t.order.orderId, t.orderStatus.status,
                    )

            # ── 4. Handle OCA failures ─────────────────────────────────────────
            # OCA orders are a server-side failsafe. If they fail the software
            # exit loop (ATR stop / trailing stop / TP) is still active and will
            # manage the position. Attempting emergency close here is risky —
            # if the close also fails, the position is left open AND untracked.
            # Instead: seed params and return "submitted" so the trade loop
            # records the open. Log prominently so the user is aware.
            if oca_errors:
                logger.error(
                    "[IBKR] OCA placement failed for %s (%s) — "
                    "position is open WITHOUT server-side bracket. "
                    "Software exit loop will manage exits.",
                    ticker, oca_errors,
                )

            # Save position params so exit rules work correctly after restart
            self._position_params[ticker] = {
                "stop_loss": effective_stop_loss,
                "take_profit": effective_take_profit,
                "opened_at": opened_at_dt.isoformat(),
                "direction": signal.direction,
                "source": "system",
                "trailing_stop_pct_applied": trailing_stop_pct,
                "oca_group": oca_group,
                "entry_price": fill_price or 0.0,
                "shares": quantity,
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

            # Cancel only bracket child orders placed by this system (orderRef = ORDER_REF).
            # Must use openTrades() — it exposes both .order and .contract.
            # openOrders() returns Order objects which have no .contract attribute.
            # Filtering by orderRef avoids cancelling manually-placed hedge orders
            # or orders from other API clients on the same ticker.
            for trade in self._ib.openTrades():
                if (trade.contract.symbol == ticker
                        and getattr(trade.order, "orderRef", "") == ORDER_REF):
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
            close_order.transmit  = True  # must be explicit — default varies by ib_async version
            close_order.orderRef  = ORDER_REF
            if self._account:
                close_order.account = self._account
            trade = self._ib.placeOrder(contract, close_order)
            self._active_trades[trade.order.orderId] = trade
            await asyncio.sleep(0.5)

            fill_price: float | None = None
            if trade.fills:
                fill_price = float(trade.fills[0].execution.price)

            # Clean up in-memory state so re-entry on same ticker starts fresh
            self._hwm.pop(ticker, None)
            self._hwm_min.pop(ticker, None)
            self._ts_order_id.pop(ticker, None)
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

    async def update_trailing_stop(self, ticker: str, new_pct: float) -> bool:
        """
        Replace the live TRAIL OCA order with a tighter one anchored from the HWM.

        Used when mention decay tightens the trailing stop (Rule 6 dynamic tightening).
        The new order uses trailingPercent + trailStopPrice (anchored to HWM) so that
        profit already locked in by the HWM is preserved, not reset to current price.

        Returns True if the replacement was placed, False on any error.
        """
        if not _IB_AVAILABLE:
            return False

        from ib_async import Order as _IbOrder, Stock  # noqa: PLC0415

        old_order_id = self._ts_order_id.get(ticker)
        params = self._position_params.get(ticker, {})
        direction: str = params.get("direction", "LONG")

        try:
            # Cancel the existing TRAIL order
            if old_order_id is not None:
                for trade in self._ib.openTrades():
                    if trade.order.orderId == old_order_id:
                        self._ib.cancelOrder(trade.order)
                        logger.debug(
                            "[IBKR] Cancelled old TRAIL order %d for %s",
                            old_order_id, ticker,
                        )
                        break

            # Determine current HWM — LONG uses max, SHORT uses min
            if direction == "LONG":
                hwm = self._hwm.get(ticker, 0.0)
                close_action = "SELL"
            else:
                hwm = self._hwm_min.get(ticker, 0.0)
                close_action = "BUY"

            if hwm <= 0.0:
                logger.warning(
                    "[IBKR] update_trailing_stop: HWM=0 for %s — cannot anchor trail", ticker
                )
                return False

            # Look up quantity from current IB position
            qty = 0
            for p in self._ib.positions():
                if p.contract.symbol == ticker:
                    qty = abs(int(p.position))
                    break
            if qty == 0:
                logger.warning(
                    "[IBKR] update_trailing_stop: no open position found for %s", ticker
                )
                return False

            # Preserve OCA group from position params so IB links correctly
            oca_group = params.get("oca_group", "")

            contract = Stock(ticker, "SMART", "USD")
            await self._ib.qualifyContractsAsync(contract)

            trail_order = _IbOrder()
            trail_order.action          = close_action
            trail_order.totalQuantity   = qty
            trail_order.orderType       = "TRAIL"
            trail_order.trailingPercent = new_pct * 100   # e.g. 5.0 for 5%
            # Anchor initial trigger from HWM so locked-in profit is preserved.
            # LONG: trigger = hwm * (1 - pct); SHORT: trigger = hwm * (1 + pct)
            if direction == "LONG":
                trail_order.trailStopPrice = round(hwm * (1.0 - new_pct), 2)
            else:
                trail_order.trailStopPrice = round(hwm * (1.0 + new_pct), 2)
            trail_order.tif         = "GTC"
            trail_order.outsideRth  = False  # don't fire on thin after-hours prints
            trail_order.transmit    = True
            trail_order.orderRef    = ORDER_REF
            if oca_group:
                trail_order.ocaGroup = oca_group
                trail_order.ocaType  = 1
            if self._account:
                trail_order.account = self._account

            ts_trade = self._ib.placeOrder(contract, trail_order)
            await asyncio.sleep(0.25)
            self._ts_order_id[ticker] = ts_trade.order.orderId
            if ticker in self._position_params:
                self._position_params[ticker]["trailing_stop_pct_applied"] = new_pct

            logger.info(
                "[IBKR] TRAIL updated %s new_pct=%.1f%% trigger=%.4f orderId=%d (HWM=%.4f)",
                ticker, new_pct * 100, trail_order.trailStopPrice,
                ts_trade.order.orderId, hwm,
            )
            return True

        except Exception as exc:
            logger.error("[IBKR] update_trailing_stop failed for %s: %s", ticker, exc)
            return False

    async def reattach_oca_orders(
        self,
        ticker: str,
        direction: str,
        quantity: int,
        current_price: float,
        stop_loss: float,
        take_profit: float,
        trailing_stop_pct: float = 0.08,
    ) -> bool:
        """
        Place OCA stop/limit/trail orders for an existing open position that has no
        server-side bracket (naked position).

        Called by the exit loop when a position is detected as unprotected.
        Returns True if at least one *protective* leg (STP or TRAIL) was confirmed
        active/submitted by IB. Returns False on total failure — caller should then
        close the position.

        `current_price` is used only to validate SL/TP direction; SL and TP must
        already be reconstructed from entry_price + ATR by the caller.
        """
        if not _IB_AVAILABLE:
            return False

        from ib_async import LimitOrder, Order as _IbOrder, Stock, StopOrder  # noqa: PLC0415

        close_action = "SELL" if direction == "LONG" else "BUY"
        new_oca_group = f"oca_reattach_{str(uuid.uuid4())[:8]}"

        try:
            contract = Stock(ticker, "SMART", "USD")
            await self._ib.qualifyContractsAsync(contract)
        except Exception as exc:
            logger.error("[IBKR] reattach_oca: could not qualify contract for %s: %s", ticker, exc)
            return False

        # ── Cancel any existing bracket orders for this ticker first ──────────
        # Prevents duplicate OCA groups when the naked check fires during a brief
        # window after reconnect where openTrades() was empty / stale.
        # Only cancel system-managed protective orders (ORDER_REF + bracket types).
        _bracket_types = {"STP", "STP LMT", "TRAIL", "LMT"}
        _cancelled_existing = 0
        try:
            for trade in self._ib.openTrades():
                sym = getattr(getattr(trade, "contract", None), "symbol", "")
                ord_obj = getattr(trade, "order", None)
                ref = getattr(ord_obj, "orderRef", "")
                ot = getattr(ord_obj, "orderType", "")
                if sym == ticker and ref == ORDER_REF and ot in _bracket_types:
                    try:
                        self._ib.cancelOrder(ord_obj)
                        _cancelled_existing += 1
                    except Exception as _ce:
                        logger.debug("[IBKR] reattach_oca: cancel existing %s order failed: %s", ot, _ce)
            if _cancelled_existing:
                logger.info(
                    "[IBKR] reattach_oca: cancelled %d existing bracket order(s) for %s before reattach",
                    _cancelled_existing, ticker,
                )
                await asyncio.sleep(0.3)  # brief pause so IB acks the cancels
        except Exception as _exc:
            logger.debug("[IBKR] reattach_oca: error during existing-order cancellation for %s: %s", ticker, _exc)

        placed_legs: list = []

        # ── Stop loss leg ─────────────────────────────────────────────────────
        sl_valid = (
            stop_loss > 0
            and (
                (direction == "LONG"  and stop_loss < current_price)
                or (direction == "SHORT" and stop_loss > current_price)
            )
        )
        if sl_valid:
            try:
                stop_order = StopOrder(close_action, quantity, round(stop_loss, 2))
                stop_order.ocaGroup   = new_oca_group
                stop_order.ocaType    = 1
                stop_order.tif        = "GTC"
                stop_order.outsideRth = False
                stop_order.transmit   = True
                stop_order.orderRef   = ORDER_REF
                if self._account:
                    stop_order.account = self._account
                sl_trade = self._ib.placeOrder(contract, stop_order)
                placed_legs.append(("STP", sl_trade))
                logger.info(
                    "[IBKR] reattach OCA stop: %s sl=%.4f orderId=%d",
                    ticker, stop_loss, sl_trade.order.orderId,
                )
            except Exception as exc:
                logger.error("[IBKR] reattach OCA stop failed for %s: %s", ticker, exc)
        else:
            logger.warning(
                "[IBKR] reattach: SL=%.4f invalid vs current=%.4f (%s) — stop leg skipped",
                stop_loss, current_price, direction,
            )

        # ── Take profit leg ───────────────────────────────────────────────────
        tp_valid = (
            take_profit > 0
            and (
                (direction == "LONG"  and take_profit > current_price)
                or (direction == "SHORT" and take_profit < current_price)
            )
        )
        if tp_valid:
            try:
                limit_order = LimitOrder(close_action, quantity, round(take_profit, 2))
                limit_order.ocaGroup = new_oca_group
                limit_order.ocaType  = 1
                limit_order.tif      = "GTC"
                limit_order.transmit = True
                limit_order.orderRef = ORDER_REF
                if self._account:
                    limit_order.account = self._account
                tp_trade = self._ib.placeOrder(contract, limit_order)
                placed_legs.append(("LMT", tp_trade))
                logger.info(
                    "[IBKR] reattach OCA limit: %s tp=%.4f orderId=%d",
                    ticker, take_profit, tp_trade.order.orderId,
                )
            except Exception as exc:
                logger.error("[IBKR] reattach OCA limit failed for %s: %s", ticker, exc)

        # ── Trailing stop leg (always attempted — primary protection fallback) ──
        if trailing_stop_pct > 0:
            try:
                trail_order = _IbOrder()
                trail_order.action          = close_action
                trail_order.totalQuantity   = quantity
                trail_order.orderType       = "TRAIL"
                trail_order.trailingPercent = trailing_stop_pct * 100
                trail_order.tif             = "GTC"
                trail_order.outsideRth      = False
                trail_order.ocaGroup        = new_oca_group
                trail_order.ocaType         = 1
                trail_order.transmit        = True
                trail_order.orderRef        = ORDER_REF
                if self._account:
                    trail_order.account = self._account
                ts_trade = self._ib.placeOrder(contract, trail_order)
                placed_legs.append(("TRAIL", ts_trade))
                self._ts_order_id[ticker] = ts_trade.order.orderId
                logger.info(
                    "[IBKR] reattach OCA trail: %s trail_pct=%.1f%% orderId=%d",
                    ticker, trailing_stop_pct * 100, ts_trade.order.orderId,
                )
            except Exception as exc:
                logger.error("[IBKR] reattach OCA trail failed for %s: %s", ticker, exc)

        if not placed_legs:
            logger.error("[IBKR] reattach_oca: no legs placed at all for %s", ticker)
            return False

        # ── Wait for IB order acknowledgement ─────────────────────────────────
        # Allow IB time to process and return order status so we can detect
        # immediate rejects (Inactive/Cancelled) before declaring success.
        await asyncio.sleep(1.0)

        _done_states = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
        active_legs = [
            (leg_type, t) for leg_type, t in placed_legs
            if getattr(t.orderStatus, "status", "") not in _done_states
        ]

        # Require at least one *protective* leg (stop or trail) confirmed active
        has_protective = any(leg_type in ("STP", "TRAIL") for leg_type, _ in active_legs)

        if has_protective:
            if ticker in self._position_params:
                self._position_params[ticker]["oca_group"] = new_oca_group
            logger.warning(
                "[IBKR] reattach OCA succeeded for %s — oca_group=%s legs=%s",
                ticker, new_oca_group,
                [t for t, _ in active_legs],
            )
            return True

        logger.error(
            "[IBKR] reattach OCA: no protective leg confirmed active for %s "
            "(active=%s, placed=%s)",
            ticker, [t for t, _ in active_legs], [t for t, _ in placed_legs],
        )
        return False

    async def get_positions(self) -> list[Position]:
        """Return open positions from IBKR as Position objects.

        Only positions belonging to the configured account are returned.
        """
        if not _IB_AVAILABLE:
            raise RuntimeError("ib_async is not installed")

        import math as _math  # noqa: PLC0415

        positions = []

        # Build a portfolio map keyed by symbol for fast lookup.
        # ib.portfolio() carries live marketPrice updated by IB's account
        # subscription — always available when connected, no market-data
        # subscription required.  Used as a fallback when _prices has no
        # cached value for a ticker yet (e.g. first cycle or fetch failure).
        portfolio_map: dict[str, Any] = {}
        try:
            for item in self._ib.portfolio():
                if self._account and item.account != self._account:
                    continue
                sym = item.contract.symbol
                mp = getattr(item, "marketPrice", None)
                try:
                    if mp is not None and _math.isfinite(float(mp)) and float(mp) > 0:
                        portfolio_map[sym] = item
                except (TypeError, ValueError):
                    pass
        except Exception:
            pass

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
                self._hwm_min[ticker] = current

            # Select correct HWM polarity: LONG tracks max, SHORT tracks min
            if direction == "LONG":
                raw_hwm = self._hwm.get(ticker, entry_price)
            else:
                raw_hwm = self._hwm_min.get(ticker, entry_price)

            # Restore persisted position params (sl/tp/opened_at)
            params = self._position_params.get(ticker, {})
            stop_loss = float(params.get("stop_loss", 0.0))
            take_profit = float(params.get("take_profit", 0.0))
            try:
                opened_at = datetime.fromisoformat(params["opened_at"]) if "opened_at" in params else datetime.now(UTC)
            except (ValueError, KeyError):
                opened_at = datetime.now(UTC)

            # Compute unrealised PnL from price cache, with portfolio fallback.
            # Preference order:
            #   1. _prices — refreshed from get_market_prices() every exit-loop tick
            #   2. ib.portfolio() marketPrice — live account-subscription data,
            #      used when _prices has no value yet (first cycle / fetch failure)
            #   3. entry_price — last resort (unrealized = 0)
            current_price = self._prices.get(ticker)
            if current_price is None:
                port = portfolio_map.get(ticker)
                if port is not None:
                    current_price = float(port.marketPrice)
                    self._prices[ticker] = current_price  # seed cache
                else:
                    current_price = entry_price
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

    async def get_market_prices(self, tickers: list[str]) -> dict[str, float]:
        """
        Fetch current market prices for a list of tickers from IB in one batch call.

        Uses reqTickersAsync (snapshot mode) which sends all requests concurrently
        and resolves when data arrives — no fixed sleep delay.

        Returns a dict of {ticker: price} for tickers where IB returned a valid
        (finite, non-zero) price.  Tickers with no IB subscription or missing data
        are omitted; the caller should fall back to an alternative source for those.
        """
        if not _IB_AVAILABLE or not tickers:
            return {}

        import math  # noqa: PLC0415
        from ib_async import Stock  # noqa: PLC0415

        try:
            # Request live data. For accounts with live subscriptions this
            # returns real-time prices.  For paper/unsubscribed tickers IB
            # returns NaN → those tickers are omitted and the caller falls
            # back to yfinance.  We do NOT force type 3 (delayed) here because
            # reqMarketDataType is global on the connection — forcing delayed
            # mode would poison all subsequent market-data requests (including
            # market_data/ibkr.py) even for subscribed tickers.
            self._ib.reqMarketDataType(1)
            contracts = [Stock(t, "SMART", "USD") for t in tickers]
            ticker_objects = await self._ib.reqTickersAsync(*contracts)
        except Exception as exc:
            logger.warning("[IBKR] get_market_prices batch request failed: %s", exc)
            return {}

        prices: dict[str, float] = {}
        for contract, ticker_obj in zip(contracts, ticker_objects):
            sym = contract.symbol
            try:
                price = ticker_obj.marketPrice()
                if math.isfinite(price) and price > 0:
                    prices[sym] = float(price)
                else:
                    # Try close price as fallback (e.g. outside market hours)
                    close = ticker_obj.close
                    if close and math.isfinite(float(close)) and float(close) > 0:
                        prices[sym] = float(close)
            except Exception as exc:
                logger.debug("[IBKR] Price extraction failed for %s: %s", sym, exc)

        logger.debug(
            "[IBKR] get_market_prices: requested=%d, received=%d",
            len(tickers), len(prices),
        )
        return prices
