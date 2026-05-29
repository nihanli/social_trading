"""
PaperTradingEngine — in-memory simulated execution.

Satisfies the ExecutionEngine protocol without real money.
Fills are simulated at the last-known price supplied by an injected
MarketDataProvider (or a simple price lookup dict for unit tests).

Design reference: docs/plan/03-development-phases.md §5a
Protocol reference: docs/plan/02-protocols-and-interfaces.md §4

Usage:
    engine = PaperTradingEngine(initial_cash=100_000.0)
    engine.set_price("AAPL", 178.50)
    result = await engine.submit_signal(signal, quantity=10, stop_loss=172.0, take_profit=186.0)
"""
from __future__ import annotations

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


class PaperTradingEngine:
    """
    In-memory paper trading engine.

    Thread/task-safety: designed for use from a single asyncio event loop.
    State is not persisted — restart = fresh book.

    Attributes:
        _cash:           Available cash.
        _positions:      Open positions keyed by ticker.
        _trades:         Closed trade records for P&L reporting.
        _daily_pnl:      P&L since midnight UTC.
        _weekly_pnl:     P&L since Monday midnight UTC.
        _peak_equity:    Highest NLV ever (for drawdown calculation).
        _prices:         Last known prices per ticker (set via set_price).
    """

    def __init__(
        self,
        initial_cash: float = 100_000.0,
        slippage_bps: float = 5.0,       # 5 bps slippage on fills
        commission_per_share: float = 0.005,  # $0.005 / share (IBKR Tiered)
    ) -> None:
        self._initial_cash = initial_cash
        self._cash = initial_cash
        self._slippage_bps = slippage_bps
        self._commission = commission_per_share
        self._positions: dict[str, Position] = {}
        self._trades: list[dict[str, Any]] = []
        self._daily_pnl: float = 0.0
        self._weekly_pnl: float = 0.0
        self._peak_equity: float = initial_cash
        self._prices: dict[str, float] = {}

    # ── Price management (called by execution service from market data feed) ───

    def set_price(self, ticker: str, price: float) -> None:
        """Update last-known price for a ticker."""
        self._prices[ticker] = price
        # Update high-water marks on open positions
        if ticker in self._positions:
            self._positions[ticker].update_hwm(price)

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_hwm(self) -> dict[str, float]:
        """Return a snapshot of all tracked high-water marks (open positions only)."""
        return {t: p.high_water_mark for t, p in self._positions.items()}

    def seed_hwm(self, ticker: str, value: float) -> None:
        """Restore a persisted HWM; only applies to open positions not yet ratcheted."""
        if ticker in self._positions:
            pos = self._positions[ticker]
            # Only restore if HWM hasn't moved above entry price yet
            if pos.high_water_mark <= pos.entry_price:
                pos.high_water_mark = value

    def forget_position(self, ticker: str) -> None:
        """No-op for paper engine — external closes cannot happen in simulation."""

    # ── ExecutionEngine protocol ───────────────────────────────────────────────

    async def submit_signal(
        self,
        signal: Signal,
        quantity: int,
        stop_loss: float,
        take_profit: float,
    ) -> OrderResult:
        """
        Simulate a bracket order fill.

        Returns 'rejected' if:
          - ticker already has an open position
          - no price known for ticker
          - insufficient cash
        """
        ticker = signal.ticker

        if ticker in self._positions:
            return OrderResult(
                order_id=str(uuid.uuid4()),
                ticker=ticker,
                direction=signal.direction,
                quantity=quantity,
                status="rejected",
                error=f"Position already open for {ticker}",
            )

        price = self._prices.get(ticker)
        if price is None:
            return OrderResult(
                order_id=str(uuid.uuid4()),
                ticker=ticker,
                direction=signal.direction,
                quantity=quantity,
                status="rejected",
                error=f"No price available for {ticker}",
            )

        fill_price = _apply_slippage(price, signal.direction, self._slippage_bps)
        cost = fill_price * quantity
        commission = self._commission * quantity

        if cost + commission > self._cash:
            return OrderResult(
                order_id=str(uuid.uuid4()),
                ticker=ticker,
                direction=signal.direction,
                quantity=quantity,
                status="rejected",
                error=f"Insufficient cash: need ${cost + commission:.2f}, have ${self._cash:.2f}",
            )

        order_id = str(uuid.uuid4())
        self._cash -= cost + commission

        pos = Position(
            ticker=ticker,
            direction=signal.direction,
            shares=quantity,
            entry_price=fill_price,
            opened_at=datetime.now(UTC),
            stop_loss=stop_loss,
            take_profit=take_profit,
            high_water_mark=fill_price,
            signal_id=order_id,
        )
        self._positions[ticker] = pos

        logger.info(
            "[PAPER] OPEN %s %s qty=%d @ %.4f sl=%.4f tp=%.4f cash_remaining=$%.2f",
            signal.direction, ticker, quantity, fill_price,
            stop_loss, take_profit, self._cash,
        )

        return OrderResult(
            order_id=order_id,
            ticker=ticker,
            direction=signal.direction,
            quantity=quantity,
            fill_price=fill_price,
            status="filled",
        )

    async def close_position(self, ticker: str, reason: str = "") -> OrderResult:
        """
        Simulate a market close of an open position.
        Returns 'rejected' if no position exists.
        """
        pos = self._positions.get(ticker)
        if pos is None:
            return OrderResult(
                order_id=str(uuid.uuid4()),
                ticker=ticker,
                direction="LONG",
                quantity=0,
                status="rejected",
                error=f"No open position for {ticker}",
            )

        price = self._prices.get(ticker, pos.entry_price)
        # Slippage is adverse on close too
        close_direction: Direction = "SHORT" if pos.direction == "LONG" else "LONG"
        fill_price = _apply_slippage(price, close_direction, self._slippage_bps)
        commission = self._commission * pos.shares

        # P&L calculation
        if pos.direction == "LONG":
            pnl = (fill_price - pos.entry_price) * pos.shares - commission
            proceeds = fill_price * pos.shares - commission
        else:
            pnl = (pos.entry_price - fill_price) * pos.shares - commission
            proceeds = pos.entry_price * pos.shares + (pos.entry_price - fill_price) * pos.shares - commission

        self._cash += proceeds

        self._daily_pnl += pnl
        self._weekly_pnl += pnl

        order_id = str(uuid.uuid4())
        self._trades.append({
            "order_id": order_id,
            "ticker": ticker,
            "direction": pos.direction,
            "shares": pos.shares,
            "entry_price": pos.entry_price,
            "exit_price": fill_price,
            "pnl": pnl,
            "exit_reason": reason,
            "opened_at": pos.opened_at.isoformat(),
            "closed_at": datetime.now(UTC).isoformat(),
            "signal_id": pos.signal_id,
        })

        del self._positions[ticker]

        logger.info(
            "[PAPER] CLOSE %s %s qty=%d @ %.4f pnl=%+.2f reason=%s",
            pos.direction, ticker, pos.shares, fill_price, pnl, reason,
        )

        return OrderResult(
            order_id=order_id,
            ticker=ticker,
            direction=pos.direction,
            quantity=pos.shares,
            fill_price=fill_price,
            status="filled",
        )

    async def get_positions(self) -> list[Position]:
        """Return all currently open positions."""
        return list(self._positions.values())

    async def get_account_state(self) -> AccountState:
        """Compute current NLV, cash, P&L, and drawdown."""
        unrealised = sum(
            _unrealised_pnl(pos, self._prices.get(pos.ticker, pos.entry_price))
            for pos in self._positions.values()
        )
        nlv = self._cash + sum(
            pos.entry_price * pos.shares for pos in self._positions.values()
        ) + unrealised

        self._peak_equity = max(self._peak_equity, nlv)
        drawdown = (self._peak_equity - nlv) / self._peak_equity if self._peak_equity > 0 else 0.0

        return AccountState(
            net_liquidation=round(nlv, 2),
            cash=round(self._cash, 2),
            daily_pnl=round(self._daily_pnl, 2),
            weekly_pnl=round(self._weekly_pnl, 2),
            drawdown_pct=round(drawdown, 6),
            open_positions=list(self._positions.values()),
        )

    async def health_check(self) -> bool:
        return True

    # ── Convenience helpers ───────────────────────────────────────────────────

    def reset_daily_pnl(self) -> None:
        """Call at market open each day."""
        self._daily_pnl = 0.0

    def reset_weekly_pnl(self) -> None:
        """Call Monday morning."""
        self._weekly_pnl = 0.0

    @property
    def trades(self) -> list[dict[str, Any]]:
        """All closed trade records."""
        return list(self._trades)

    @property
    def open_tickers(self) -> set[str]:
        return set(self._positions.keys())


# ── Pure helpers ──────────────────────────────────────────────────────────────

def _apply_slippage(price: float, direction: Direction, slippage_bps: float) -> float:
    """
    Adverse slippage: buying pushes price up, selling pushes price down.
    """
    factor = slippage_bps / 10_000
    if direction == "LONG":
        return price * (1.0 + factor)
    return price * (1.0 - factor)


def _unrealised_pnl(pos: Position, current_price: float) -> float:
    if pos.direction == "LONG":
        return (current_price - pos.entry_price) * pos.shares
    return (pos.entry_price - current_price) * pos.shares
