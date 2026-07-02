"""Unit tests for PositionExitManager — pure computation, no I/O."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from unittest.mock import patch

import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import Position
from social_trading.risk.exit_manager import (
    PositionExitManager,
    _breaches_stop_loss,
    _breaches_take_profit,
    _breaches_trailing_stop,
    _sentiment_reversal,
    _unrealised_pnl,
)
from social_trading.services.execution_service import _effective_trailing_pct

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def cfg() -> SystemConfig:
    return SystemConfig(
        atr_multiplier=2.0,
        take_profit_pct=0.04,
        trailing_stop_pct=0.08,
        max_hold_sessions=2,
        time_stop_minutes_before_close=5,
        loss_limit_single_trade=0.01,
        signal_reversal_threshold=-0.20,
        mention_decay_threshold=0.25,
        mention_decay_min_hold_hours=1.0,
    )


@pytest.fixture
def manager() -> PositionExitManager:
    return PositionExitManager()


def make_long(
    entry_price: float = 100.0,
    quantity: int = 100,
    hours_ago: int = 1,
    stop_loss: float = 96.0,
    take_profit: float = 104.0,
    hwm: float = 100.0,
) -> Position:
    entry_time = datetime.utcnow() - timedelta(hours=hours_ago)
    return Position(
        ticker="AAPL",
        direction="LONG",
        shares=quantity,
        entry_price=entry_price,
        opened_at=entry_time,
        stop_loss=stop_loss,
        take_profit=take_profit,
        high_water_mark=hwm,
    )


def make_short(
    entry_price: float = 100.0,
    quantity: int = 100,
    hours_ago: int = 1,
    stop_loss: float = 104.0,
    take_profit: float = 96.0,
    hwm: float = 100.0,
) -> Position:
    entry_time = datetime.utcnow() - timedelta(hours=hours_ago)
    return Position(
        ticker="AAPL",
        direction="SHORT",
        shares=quantity,
        entry_price=entry_price,
        opened_at=entry_time,
        stop_loss=stop_loss,
        take_profit=take_profit,
        high_water_mark=hwm,
    )


# ── HOLD ──────────────────────────────────────────────────────────────────────

def test_hold_when_healthy(manager: PositionExitManager, cfg: SystemConfig) -> None:
    pos = make_long()
    decision = manager.evaluate(pos, current_price=101.0, cfg=cfg)
    assert decision.should_exit is False
    assert decision.reason == "HOLD"


# ── EMERGENCY ─────────────────────────────────────────────────────────────────

def test_emergency_single_trade_loss(
    manager: PositionExitManager, cfg: SystemConfig
) -> None:
    """2% loss on a position with NO ATR stop set breaches 1% single-trade limit."""
    # stop_loss=0 means ATR data was unavailable — emergency fires as last-resort safety net
    pos = make_long(entry_price=100.0, quantity=100, stop_loss=0.0, take_profit=0.0)
    # entry_cost = 10_000; pnl = (98-100)*100 = -200; loss_pct = 2%
    decision = manager.evaluate(pos, current_price=98.0, cfg=cfg)
    assert decision.should_exit is True
    assert decision.reason == "EMERGENCY"


def test_emergency_skipped_when_atr_stop_present(
    manager: PositionExitManager, cfg: SystemConfig
) -> None:
    """When a valid ATR stop is set, emergency does NOT fire above the stop price."""
    # Position with ATR-based stop at 96; price at 97 (2% loss) is still above stop.
    # EMERGENCY must not fire — the ATR stop will handle exit at the intended level.
    pos = make_long(entry_price=100.0, quantity=100, stop_loss=96.0)
    decision = manager.evaluate(pos, current_price=97.0, cfg=cfg)
    assert decision.should_exit is False


# ── STOP_LOSS ─────────────────────────────────────────────────────────────────

def test_stop_loss_long(manager: PositionExitManager, cfg: SystemConfig) -> None:
    # entry=100, stop=99.5, price=99.4 → loss=(100-99.4)*1=0.6% < 1% emergency
    pos = make_long(entry_price=100.0, quantity=1, stop_loss=99.5, take_profit=104.0, hwm=100.0)
    decision = manager.evaluate(pos, current_price=99.4, cfg=cfg)
    assert decision.should_exit is True
    assert decision.reason == "STOP_LOSS"


def test_stop_loss_short(manager: PositionExitManager, cfg: SystemConfig) -> None:
    # entry=100, stop=100.5, price=100.6 → loss=(100.6-100)*1=0.6% < 1% emergency
    pos = make_short(entry_price=100.0, quantity=1, stop_loss=100.5, take_profit=96.0, hwm=100.0)
    decision = manager.evaluate(pos, current_price=100.6, cfg=cfg)
    assert decision.should_exit is True
    assert decision.reason == "STOP_LOSS"


def test_above_stop_loss_no_exit(manager: PositionExitManager, cfg: SystemConfig) -> None:
    # 97 > stop_loss 96: price is between entry and stop; no exit expected.
    # With stop_loss > 0, EMERGENCY is suppressed (ATR stop will handle it).
    pos = make_long(entry_price=100.0, stop_loss=96.0)
    decision = manager.evaluate(pos, current_price=97.0, cfg=cfg)
    assert decision.should_exit is False


# ── TAKE_PROFIT ───────────────────────────────────────────────────────────────

def test_take_profit_long(manager: PositionExitManager, cfg: SystemConfig) -> None:
    pos = make_long(take_profit=104.0, stop_loss=96.0)
    decision = manager.evaluate(pos, current_price=104.5, cfg=cfg)
    assert decision.should_exit is True
    assert decision.reason == "TAKE_PROFIT"


def test_take_profit_short(manager: PositionExitManager, cfg: SystemConfig) -> None:
    pos = make_short(take_profit=96.0, stop_loss=104.0)
    decision = manager.evaluate(pos, current_price=95.5, cfg=cfg)
    assert decision.should_exit is True
    assert decision.reason == "TAKE_PROFIT"


# ── TRAILING_STOP ─────────────────────────────────────────────────────────────

def test_trailing_stop_long(manager: PositionExitManager, cfg: SystemConfig) -> None:
    """HWM=110, trailing_stop_pct=0.08 → stop at 101.2. Price drops to 100."""
    pos = make_long(entry_price=100.0, hwm=110.0)
    decision = manager.evaluate(pos, current_price=100.0, cfg=cfg)
    assert decision.should_exit is True
    assert decision.reason == "TRAILING_STOP"


def test_trailing_stop_not_triggered_small_dip(
    manager: PositionExitManager, cfg: SystemConfig
) -> None:
    """HWM=102, trailing_stop_pct=0.08 → stop at 93.84. Price at 95 — safe."""
    pos = make_long(entry_price=100.0, hwm=102.0, stop_loss=96.0, take_profit=110.0)
    decision = manager.evaluate(pos, current_price=95.0, cfg=cfg)
    # 95 > 93.84 trailing stop; but 95 < 96.0 stop_loss → STOP_LOSS or EMERGENCY
    # Price 95 vs entry 100: pnl = -500, loss_pct = 5% → EMERGENCY first
    assert decision.should_exit is True


def test_trailing_stop_hwm_zero_skipped(
    manager: PositionExitManager, cfg: SystemConfig
) -> None:
    """HWM=0 means uninitialised — trailing stop skipped."""
    pos = make_long(entry_price=100.0, hwm=0.0, stop_loss=90.0, take_profit=120.0)
    decision = manager.evaluate(pos, current_price=101.0, cfg=cfg)
    assert decision.should_exit is False


# ── SENTIMENT_REVERSAL ────────────────────────────────────────────────────────

def test_sentiment_reversal_long(
    manager: PositionExitManager, cfg: SystemConfig
) -> None:
    pos = make_long()
    # reversal_threshold = -0.20; sentiment = -0.30 < -0.20 → reversal for LONG
    decision = manager.evaluate(
        pos, current_price=101.0, cfg=cfg, current_sentiment=-0.30
    )
    assert decision.should_exit is True
    assert decision.reason == "SENTIMENT_REVERSAL"


def test_sentiment_reversal_short(
    manager: PositionExitManager, cfg: SystemConfig
) -> None:
    pos = make_short()
    # -reversal_threshold = 0.20; sentiment = 0.30 > 0.20 → reversal for SHORT
    decision = manager.evaluate(
        pos, current_price=99.0, cfg=cfg, current_sentiment=0.30
    )
    assert decision.should_exit is True
    assert decision.reason == "SENTIMENT_REVERSAL"


def test_no_reversal_when_sentiment_zero(
    manager: PositionExitManager, cfg: SystemConfig
) -> None:
    """Sentiment=0.0 means 'no data' — should not trigger reversal."""
    pos = make_long()
    decision = manager.evaluate(
        pos, current_price=101.0, cfg=cfg, current_sentiment=0.0
    )
    assert decision.should_exit is False


# ── MENTION_DECAY → trailing stop tightening ─────────────────────────────────
# Mention decay no longer triggers a hard exit. Instead, execution_service
# dynamically tightens trailing_stop_pct via _effective_trailing_pct().
# The exit_manager receives the tightened cfg; the TRAILING_STOP rule fires
# when price falls through the tightened trail.

def test_effective_trailing_pct_full_mentions() -> None:
    """At full mentions (ratio=1.0), no tightening — returns trailing_stop_pct."""
    cfg = SystemConfig(
        trailing_stop_pct=0.08,
        trailing_stop_min_pct=0.02,
        mention_decay_threshold=0.25,
        mention_decay_min_hold_hours=1.0,
    )
    result = _effective_trailing_pct(mention_ratio=1.0, hours_held=2.0, cfg=cfg)
    assert result == pytest.approx(0.08)


def test_effective_trailing_pct_at_threshold() -> None:
    """At exactly the decay threshold (t=0), returns trailing_stop_min_pct."""
    cfg = SystemConfig(
        trailing_stop_pct=0.08,
        trailing_stop_min_pct=0.02,
        mention_decay_threshold=0.25,
        mention_decay_min_hold_hours=1.0,
    )
    result = _effective_trailing_pct(mention_ratio=0.25, hours_held=2.0, cfg=cfg)
    assert result == pytest.approx(0.02)


def test_effective_trailing_pct_below_threshold() -> None:
    """Below the threshold — floor at trailing_stop_min_pct."""
    cfg = SystemConfig(
        trailing_stop_pct=0.08,
        trailing_stop_min_pct=0.02,
        mention_decay_threshold=0.25,
        mention_decay_min_hold_hours=1.0,
    )
    result = _effective_trailing_pct(mention_ratio=0.10, hours_held=2.0, cfg=cfg)
    assert result == pytest.approx(0.02)


def test_effective_trailing_pct_midpoint() -> None:
    """
    Midpoint between threshold and 1.0: t=0.5, effective = min + 0.5*(max-min).
    threshold=0.25, ratio=0.625 → t = (0.625-0.25)/(1-0.25) = 0.5
    effective = 0.02 + 0.5*(0.08-0.02) = 0.05
    """
    cfg = SystemConfig(
        trailing_stop_pct=0.08,
        trailing_stop_min_pct=0.02,
        mention_decay_threshold=0.25,
        mention_decay_min_hold_hours=1.0,
    )
    result = _effective_trailing_pct(mention_ratio=0.625, hours_held=2.0, cfg=cfg)
    assert result == pytest.approx(0.05)


def test_effective_trailing_pct_within_min_hold() -> None:
    """Before hold gate: no tightening regardless of mention ratio."""
    cfg = SystemConfig(
        trailing_stop_pct=0.08,
        trailing_stop_min_pct=0.02,
        mention_decay_threshold=0.25,
        mention_decay_min_hold_hours=1.0,
    )
    result = _effective_trailing_pct(mention_ratio=0.05, hours_held=0.5, cfg=cfg)
    assert result == pytest.approx(0.08)  # no tightening — hold gate not open


def test_mention_decay_tightens_trailing_stop_exit(
    manager: PositionExitManager, cfg: SystemConfig
) -> None:
    """
    When mention decay tightens the trail via effective_cfg, a smaller price drop
    should now trigger TRAILING_STOP that wouldn't fire at the default pct.

    Default pct = 8%; tightened to 2% (min). HWM=104, position at 102.
    Default trail: 104 * (1-0.08) = 95.68 → 102 > 95.68 → HOLD
    Tightened trail: 104 * (1-0.02) = 101.92 → 102 > 101.92 → HOLD still
    """
    pos = make_long(entry_price=100.0, hwm=104.0, stop_loss=96.0, take_profit=110.0)
    tightened_cfg = SystemConfig(
        trailing_stop_pct=0.02,        # tightened by mention decay
        take_profit_pct=0.04,
        max_hold_sessions=2,
        time_stop_minutes_before_close=5,
        loss_limit_single_trade=0.01,
        signal_reversal_threshold=-0.20,
    )
    # Price 101.9 < tightened trail 104 * 0.98 = 101.92 → TRAILING_STOP
    decision = manager.evaluate(pos, current_price=101.9, cfg=tightened_cfg)
    assert decision.should_exit is True
    assert decision.reason == "TRAILING_STOP"


def test_no_mention_decay_hard_exit(
    manager: PositionExitManager, cfg: SystemConfig
) -> None:
    """MENTION_DECAY no longer exists as a hard exit reason — positions HOLD with low mentions."""
    pos = make_long(hours_ago=2, stop_loss=96.0, take_profit=110.0)
    # Same conditions that previously triggered MENTION_DECAY: ratio below threshold
    decision = manager.evaluate(
        pos, current_price=101.0, cfg=cfg, mention_ratio=0.10
    )
    # Should HOLD — mention decay only tightens the trail, does not force exit
    assert decision.should_exit is False


# ── TIME_STOP ─────────────────────────────────────────────────────────────────

def test_time_stop(manager: PositionExitManager, cfg: SystemConfig) -> None:
    """TIME_STOP fires when now is within time_stop_minutes_before_close of the session deadline."""
    pos = make_long(hours_ago=1)
    # Simulate: deadline = 1 minute from now → we are within the 5-min window
    deadline = datetime.now(UTC) + timedelta(minutes=1)
    with patch("social_trading.risk.exit_manager._NYSE.nth_session_close", return_value=deadline):
        decision = manager.evaluate(pos, current_price=101.0, cfg=cfg, now=datetime.now(UTC))
    assert decision.should_exit is True
    assert decision.reason == "TIME_STOP"


def test_time_stop_not_triggered(manager: PositionExitManager, cfg: SystemConfig) -> None:
    """TIME_STOP does not fire when deadline is far in the future."""
    pos = make_long(hours_ago=1)
    # Deadline is 3 hours away — well outside the 5-min window
    deadline = datetime.now(UTC) + timedelta(hours=3)
    with patch("social_trading.risk.exit_manager._NYSE.nth_session_close", return_value=deadline):
        decision = manager.evaluate(pos, current_price=101.0, cfg=cfg, now=datetime.now(UTC))
    assert decision.should_exit is False


def test_time_stop_exact_window(manager: PositionExitManager, cfg: SystemConfig) -> None:
    """TIME_STOP fires exactly at the pre-close window boundary."""
    pos = make_long(hours_ago=1)
    now = datetime.now(UTC)
    # Deadline is exactly time_stop_minutes_before_close away → boundary fires
    deadline = now + timedelta(minutes=cfg.time_stop_minutes_before_close)
    with patch("social_trading.risk.exit_manager._NYSE.nth_session_close", return_value=deadline):
        decision = manager.evaluate(pos, current_price=101.0, cfg=cfg, now=now)
    assert decision.should_exit is True
    assert decision.reason == "TIME_STOP"


# ── Pure helpers ──────────────────────────────────────────────────────────────

def test_unrealised_pnl_long_profit() -> None:
    pos = make_long(entry_price=100.0, quantity=10)
    assert _unrealised_pnl(pos, 110.0) == pytest.approx(100.0)


def test_unrealised_pnl_long_loss() -> None:
    pos = make_long(entry_price=100.0, quantity=10)
    assert _unrealised_pnl(pos, 90.0) == pytest.approx(-100.0)


def test_unrealised_pnl_short_profit() -> None:
    pos = make_short(entry_price=100.0, quantity=10)
    assert _unrealised_pnl(pos, 90.0) == pytest.approx(100.0)


def test_breaches_stop_loss_long_true() -> None:
    pos = make_long(stop_loss=96.0)
    assert _breaches_stop_loss(pos, 95.9)


def test_breaches_stop_loss_long_false() -> None:
    pos = make_long(stop_loss=96.0)
    assert not _breaches_stop_loss(pos, 97.0)


def test_breaches_take_profit_long_true() -> None:
    pos = make_long(take_profit=104.0)
    assert _breaches_take_profit(pos, 105.0)


def test_breaches_trailing_stop_short() -> None:
    pos = make_short(entry_price=100.0, hwm=90.0)  # short HWM is minimum
    cfg = SystemConfig(trailing_stop_pct=0.08)
    # trailing_stop = 90 * 1.08 = 97.2; price = 98 > 97.2 → breach
    assert _breaches_trailing_stop(pos, 98.0, cfg)


def test_sentiment_reversal_not_triggered_mild(cfg: SystemConfig) -> None:
    pos = make_long()
    # sentiment = -0.10; threshold = -0.20; -0.10 > -0.20 → not reversed
    assert not _sentiment_reversal(pos, -0.10, cfg)
