"""
PositionExitManager — evaluates open positions for exit conditions.

Exit rules (design §6b, evaluated in priority order):
  1. Single-trade loss limit  → EMERGENCY close
  2. ATR stop-loss breached   → STOP_LOSS close
  3. Take-profit target hit   → TAKE_PROFIT close
  4. Trailing stop triggered  → TRAILING_STOP close
     (trailing_stop_pct may be tightened by mention decay in execution_service)
  5. Sentiment reversal       → SENTIMENT_REVERSAL — tightens trail to min_pct (no immediate close)
  6. Hard time stop           → TIME_STOP close

If multiple rules fire, the highest-priority one wins.

Note: SENTIMENT_REVERSAL does not produce an immediate close. Instead, execution_service
snaps the IB TRAIL order to trailing_stop_min_pct and lets the tightened trail manage the
exit. Mention decay similarly tightens the trail (linearly) without a hard exit.

Usage:
    manager = PositionExitManager()
    decision = manager.evaluate(position, current_price, current_sentiment,
                                mention_ratio, cfg)
    if decision.should_exit:
        await engine.close_position(position.ticker, decision.reason)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from social_trading.config.system_config import SystemConfig
from social_trading.core.market_hours import NYSE as _NYSE
from social_trading.core.models import Position

logger = logging.getLogger(__name__)

ExitReason = Literal[
    "EMERGENCY",
    "STOP_LOSS",
    "TAKE_PROFIT",
    "TRAILING_STOP",
    "SENTIMENT_REVERSAL",
    "TIME_STOP",
    "HOLD",
]


@dataclass(frozen=True)
class ExitDecision:
    """Result of evaluating exit conditions for one position."""

    should_exit: bool
    reason: ExitReason
    detail: str = ""


_HOLD = ExitDecision(should_exit=False, reason="HOLD")


class PositionExitManager:
    """
    Stateless exit evaluator — all context passed at call time.

    Designed to be called every 60 seconds by the execution service
    tick loop for each open position.
    """

    def evaluate(
        self,
        position: Position,
        current_price: float,
        cfg: SystemConfig,
        current_sentiment: float = 0.0,   # latest score ∈ [-1, 1]
        mention_ratio: float = 1.0,        # current_mentions / peak_mentions
        now: datetime | None = None,
    ) -> ExitDecision:
        """
        Evaluate all exit rules and return the highest-priority exit or HOLD.

        Args:
            position:           Open position to evaluate.
            current_price:      Current market price.
            cfg:                SystemConfig with exit thresholds.
            current_sentiment:  Latest aggregated sentiment score for ticker.
                                0.0 = data unavailable (won't trigger reversal).
            mention_ratio:      Fraction of peak mentions still active.
                                1.0 = full volume; <cfg.mention_decay_threshold → exit.
            now:                Override current time (for testing).
        """
        if now is None:
            now = datetime.now(UTC)

        pnl = _unrealised_pnl(position, current_price)
        entry_cost = position.cost_basis
        hours_held = (now - position.opened_at.replace(tzinfo=UTC)).total_seconds() / 3600
        trading_days_held = _NYSE.trading_days_between(position.opened_at, now)

        # ── 1. Emergency single-trade loss ────────────────────────────────────
        # Only activate when there is no valid ATR-based stop_loss already set.
        # If stop_loss > 0, the position has a proper ATR stop that will handle
        # exit at the intended price; applying an additional percentage cap on top
        # would fire on normal intraday fluctuations and undermine the strategy.
        # EMERGENCY is reserved for positions where ATR data was unavailable and
        # stop_loss defaults to 0 — a true last-resort safety net.
        if entry_cost > 0 and pnl < 0 and position.stop_loss <= 0:
            loss_pct = -pnl / entry_cost
            if loss_pct > cfg.loss_limit_single_trade:
                return ExitDecision(
                    should_exit=True,
                    reason="EMERGENCY",
                    detail=f"Single trade loss {loss_pct:.1%} > {cfg.loss_limit_single_trade:.1%} (no ATR stop)",
                )

        # ── 2. ATR stop-loss ─────────────────────────────────────────────────
        if _breaches_stop_loss(position, current_price):
            return ExitDecision(
                should_exit=True,
                reason="STOP_LOSS",
                detail=(
                    f"Price {current_price:.4f} breached stop "
                    f"{position.stop_loss:.4f}"
                ),
            )

        # ── 3. Take-profit ────────────────────────────────────────────────────
        if _breaches_take_profit(position, current_price):
            return ExitDecision(
                should_exit=True,
                reason="TAKE_PROFIT",
                detail=(
                    f"Price {current_price:.4f} reached take-profit "
                    f"{position.take_profit:.4f}"
                ),
            )

        # ── 4. Trailing stop ──────────────────────────────────────────────────
        if _breaches_trailing_stop(position, current_price, cfg):
            return ExitDecision(
                should_exit=True,
                reason="TRAILING_STOP",
                detail=(
                    f"Price {current_price:.4f} dropped "
                    f"{cfg.trailing_stop_pct:.1%} from HWM "
                    f"{position.high_water_mark:.4f}"
                ),
            )

        # ── 5. Sentiment reversal ─────────────────────────────────────────────
        if current_sentiment != 0.0:
            reversal = _sentiment_reversal(position, current_sentiment, cfg)
            if reversal:
                return ExitDecision(
                    should_exit=True,
                    reason="SENTIMENT_REVERSAL",
                    detail=f"Sentiment {current_sentiment:.3f} reversed against {position.direction}",
                )

        # ── 6. Hard time stop ─────────────────────────────────────────────────
        if trading_days_held >= cfg.max_hold_trading_days:
            return ExitDecision(
                should_exit=True,
                reason="TIME_STOP",
                detail=f"Held {trading_days_held} trading days >= max {cfg.max_hold_trading_days}",
            )

        return _HOLD


# ── Pure helpers ──────────────────────────────────────────────────────────────

def _unrealised_pnl(position: Position, current_price: float) -> float:
    """P&L before fees. Positive = profit."""
    if position.direction == "LONG":
        return (current_price - position.entry_price) * position.shares
    return (position.entry_price - current_price) * position.shares


def _breaches_stop_loss(position: Position, current_price: float) -> bool:
    """Returns True if current price crosses the ATR stop.
    Returns False when stop_loss is 0.0 (unset — e.g. IBKR position after restart
    where the bracket leg is live on IB's side and Python doesn't know the level).
    """
    if position.stop_loss == 0.0:
        return False
    if position.direction == "LONG":
        return current_price <= position.stop_loss
    return current_price >= position.stop_loss


def _breaches_take_profit(position: Position, current_price: float) -> bool:
    """Returns True if take-profit target is reached.
    Returns False when take_profit is 0.0 (unset — e.g. IBKR position after restart
    where the bracket leg is live on IB's side and Python doesn't know the level).
    """
    if position.take_profit == 0.0:
        return False
    if position.direction == "LONG":
        return current_price >= position.take_profit
    return current_price <= position.take_profit


def _breaches_trailing_stop(
    position: Position,
    current_price: float,
    cfg: SystemConfig,
) -> bool:
    """
    Returns True if price falls cfg.trailing_stop_pct from the high-water mark.

    For LONG: trails the maximum price seen since entry.
    For SHORT: trails the minimum price seen since entry.
    HWM of 0.0 means not yet initialised — skip check.

    The IB TRAIL order (placed immediately at entry) is the primary trailing-stop
    authority; this software check is a fallback for when the IB order is absent or
    failed (e.g. naked position, OCA placement error).  No activation gate is applied
    here — IB's TRAIL order has no such gate either.
    """
    hwm = position.high_water_mark
    if hwm == 0.0:
        return False

    entry = position.entry_price
    if entry <= 0:
        return False

    if position.direction == "LONG":
        trailing_stop = hwm * (1.0 - cfg.trailing_stop_pct)
        return current_price <= trailing_stop

    # SHORT
    trailing_stop = hwm * (1.0 + cfg.trailing_stop_pct)
    return current_price >= trailing_stop


def _sentiment_reversal(
    position: Position,
    current_sentiment: float,
    cfg: SystemConfig,
) -> bool:
    """
    Returns True if sentiment has reversed against the position direction.

    In normal mode:
      LONG:  reversal when current_sentiment < threshold  (strongly negative)
      SHORT: reversal when current_sentiment > -threshold (strongly positive)

    In contrarian mode the position direction is the *opposite* of the
    original sentiment, so the check is inverted: a contrarian LONG was
    opened on bearish buzz, and its reversal is when sentiment turns
    strongly *positive* (buzz drying up or flipping bullish).

    signal_reversal_threshold is stored as a negative number in cfg (e.g. -0.20).
    """
    threshold = cfg.signal_reversal_threshold  # e.g. -0.20
    # In contrarian mode flip the logical direction so we compare against the
    # *original* sentiment polarity rather than the trade direction.
    is_long = position.direction == "LONG"
    if cfg.contrarian_mode:
        is_long = not is_long
    if is_long:
        return current_sentiment < threshold
    return current_sentiment > -threshold
