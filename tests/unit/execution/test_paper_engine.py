"""Unit tests for PaperTradingEngine — simulated execution."""
from __future__ import annotations

from datetime import datetime

import pytest

from social_trading.core.models import Position, Signal
from social_trading.execution.paper import PaperTradingEngine, _apply_slippage, _unrealised_pnl

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def engine() -> PaperTradingEngine:
    return PaperTradingEngine(initial_cash=100_000.0, slippage_bps=0.0, commission_per_share=0.0)


@pytest.fixture
def engine_with_costs() -> PaperTradingEngine:
    """Engine with realistic slippage and commission."""
    return PaperTradingEngine(initial_cash=100_000.0, slippage_bps=5.0, commission_per_share=0.005)


def make_signal(
    ticker: str = "AAPL",
    direction: str = "LONG",
    quality: float = 0.75,
) -> Signal:
    return Signal(
        ticker=ticker,
        direction=direction,
        quality_score=quality,
        sentiment_score=0.7,
        volume_z_score=2.5,
        momentum=0.02,
        convergence=0.15,
        source_post_count=10,
    )


# ── submit_signal ─────────────────────────────────────────────────────────────

async def test_submit_long_fills(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 150.0)
    result = await engine.submit_signal(make_signal("AAPL"), quantity=10, stop_loss=144.0, take_profit=156.0)
    assert result.status == "filled"
    assert result.fill_price == pytest.approx(150.0)
    assert result.quantity == 10


async def test_submit_short_fills(engine: PaperTradingEngine) -> None:
    engine.set_price("TSLA", 200.0)
    result = await engine.submit_signal(
        make_signal("TSLA", direction="SHORT"), quantity=5, stop_loss=210.0, take_profit=190.0
    )
    assert result.status == "filled"


async def test_submit_deducts_cash(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    await engine.submit_signal(make_signal(), quantity=100, stop_loss=90.0, take_profit=110.0)
    state = await engine.get_account_state()
    assert state.cash == pytest.approx(90_000.0)


async def test_submit_rejected_no_price(engine: PaperTradingEngine) -> None:
    result = await engine.submit_signal(make_signal("NODATA"), quantity=10, stop_loss=90.0, take_profit=110.0)
    assert result.status == "rejected"
    assert "No price" in (result.error or "")


async def test_submit_rejected_duplicate_position(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    await engine.submit_signal(make_signal(), quantity=10, stop_loss=90.0, take_profit=110.0)
    result = await engine.submit_signal(make_signal(), quantity=5, stop_loss=90.0, take_profit=110.0)
    assert result.status == "rejected"
    assert "already open" in (result.error or "")


async def test_submit_rejected_insufficient_cash(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    # Try to buy 2000 shares × $100 = $200k > $100k initial cash
    result = await engine.submit_signal(make_signal(), quantity=2000, stop_loss=90.0, take_profit=110.0)
    assert result.status == "rejected"
    assert "Insufficient cash" in (result.error or "")


async def test_slippage_applied_on_buy(engine_with_costs: PaperTradingEngine) -> None:
    engine_with_costs.set_price("AAPL", 100.0)
    result = await engine_with_costs.submit_signal(
        make_signal(), quantity=10, stop_loss=90.0, take_profit=110.0
    )
    assert result.status == "filled"
    # 5 bps slippage: 100 × 1.0005 = 100.05
    assert result.fill_price == pytest.approx(100.05)


# ── close_position ────────────────────────────────────────────────────────────

async def test_close_position_long_profit(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    await engine.submit_signal(make_signal(), quantity=100, stop_loss=90.0, take_profit=110.0)
    engine.set_price("AAPL", 110.0)
    result = await engine.close_position("AAPL", reason="TAKE_PROFIT")
    assert result.status == "filled"
    assert result.fill_price == pytest.approx(110.0)


async def test_close_position_pnl_positive(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    await engine.submit_signal(make_signal(), quantity=100, stop_loss=90.0, take_profit=110.0)
    engine.set_price("AAPL", 105.0)
    await engine.close_position("AAPL", reason="TAKE_PROFIT")
    state = await engine.get_account_state()
    # Started with 100k, spent 10k on 100 shares, got back 10.5k → net +500
    assert state.daily_pnl == pytest.approx(500.0)


async def test_close_position_pnl_negative(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    await engine.submit_signal(make_signal(), quantity=100, stop_loss=90.0, take_profit=110.0)
    engine.set_price("AAPL", 95.0)
    await engine.close_position("AAPL", reason="STOP_LOSS")
    state = await engine.get_account_state()
    assert state.daily_pnl == pytest.approx(-500.0)


async def test_close_position_short_profit(engine: PaperTradingEngine) -> None:
    engine.set_price("TSLA", 200.0)
    await engine.submit_signal(
        make_signal("TSLA", direction="SHORT"), quantity=10, stop_loss=210.0, take_profit=190.0
    )
    engine.set_price("TSLA", 190.0)
    result = await engine.close_position("TSLA", reason="TAKE_PROFIT")
    assert result.status == "filled"
    state = await engine.get_account_state()
    assert state.daily_pnl == pytest.approx(100.0)  # 10 × (200-190)


async def test_close_position_no_position(engine: PaperTradingEngine) -> None:
    result = await engine.close_position("AAPL")
    assert result.status == "rejected"


async def test_trade_recorded_on_close(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    await engine.submit_signal(make_signal(), quantity=10, stop_loss=90.0, take_profit=110.0)
    await engine.close_position("AAPL", reason="TIME_STOP")
    assert len(engine.trades) == 1
    assert engine.trades[0]["reason"] == "TIME_STOP"
    assert engine.trades[0]["ticker"] == "AAPL"


# ── get_positions ─────────────────────────────────────────────────────────────

async def test_get_positions_empty(engine: PaperTradingEngine) -> None:
    positions = await engine.get_positions()
    assert positions == []


async def test_get_positions_after_open(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 150.0)
    await engine.submit_signal(make_signal(), quantity=10, stop_loss=140.0, take_profit=160.0)
    positions = await engine.get_positions()
    assert len(positions) == 1
    assert positions[0].ticker == "AAPL"
    assert positions[0].direction == "LONG"


async def test_positions_cleared_on_close(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 150.0)
    await engine.submit_signal(make_signal(), quantity=10, stop_loss=140.0, take_profit=160.0)
    await engine.close_position("AAPL")
    positions = await engine.get_positions()
    assert positions == []


# ── get_account_state ─────────────────────────────────────────────────────────

async def test_initial_account_state(engine: PaperTradingEngine) -> None:
    state = await engine.get_account_state()
    assert state.net_liquidation == pytest.approx(100_000.0)
    assert state.cash == pytest.approx(100_000.0)
    assert state.daily_pnl == 0.0
    assert state.drawdown_pct == 0.0


async def test_account_state_with_open_position(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    await engine.submit_signal(make_signal(), quantity=100, stop_loss=90.0, take_profit=110.0)
    engine.set_price("AAPL", 105.0)  # mark-to-market gain
    state = await engine.get_account_state()
    # NLV = 90_000 cash + 100 shares × 105 = 100_500
    assert state.net_liquidation == pytest.approx(100_500.0)


async def test_drawdown_computed(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    await engine.submit_signal(make_signal(), quantity=100, stop_loss=90.0, take_profit=110.0)
    engine.set_price("AAPL", 90.0)  # unrealised loss → drawdown
    state = await engine.get_account_state()
    assert state.drawdown_pct > 0.0


async def test_health_check(engine: PaperTradingEngine) -> None:
    assert await engine.health_check() is True


# ── High-water mark ───────────────────────────────────────────────────────────

async def test_hwm_updated_on_price_rise(engine: PaperTradingEngine) -> None:
    engine.set_price("AAPL", 100.0)
    await engine.submit_signal(make_signal(), quantity=10, stop_loss=90.0, take_profit=120.0)
    engine.set_price("AAPL", 115.0)  # new HWM
    positions = await engine.get_positions()
    assert positions[0].high_water_mark == pytest.approx(115.0)


# ── Pure helpers ──────────────────────────────────────────────────────────────

def test_apply_slippage_long() -> None:
    price = _apply_slippage(100.0, "LONG", 5.0)
    assert price == pytest.approx(100.05)


def test_apply_slippage_short() -> None:
    price = _apply_slippage(100.0, "SHORT", 5.0)
    assert price == pytest.approx(99.95)


def test_apply_slippage_zero() -> None:
    assert _apply_slippage(100.0, "LONG", 0.0) == 100.0


def test_unrealised_pnl_long() -> None:
    pos = Position(
        ticker="X", direction="LONG", quantity=10,
        entry_price=100.0, entry_time=datetime.utcnow(),
        stop_loss=90.0, take_profit=110.0,
    )
    assert _unrealised_pnl(pos, 105.0) == pytest.approx(50.0)


def test_unrealised_pnl_short() -> None:
    pos = Position(
        ticker="X", direction="SHORT", quantity=10,
        entry_price=100.0, entry_time=datetime.utcnow(),
        stop_loss=110.0, take_profit=90.0,
    )
    assert _unrealised_pnl(pos, 90.0) == pytest.approx(100.0)
