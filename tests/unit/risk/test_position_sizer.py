"""Unit tests for PositionSizer — pure math, no I/O."""
from __future__ import annotations

from datetime import datetime

import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import AccountState, Position, Signal
from social_trading.risk.position_sizer import PositionSizer, vix_scalar, vol_scalar

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def cfg() -> SystemConfig:
    return SystemConfig(
        max_position_pct=0.02,
        half_kelly_fraction=0.50,
        sigma_target=0.15,
        max_single_position=0.10,
        max_social_allocation=0.20,
        loss_limit_single_trade=0.01,
        vix_crisis=40.0,
        vix_high_fear=30.0,
        vix_elevated=25.0,
        vix_slightly_elevated=20.0,
        atr_multiplier=2.0,
        take_profit_pct=0.04,
    )


@pytest.fixture
def account() -> AccountState:
    return AccountState(
        net_liquidation=100_000.0,
        cash=100_000.0,
        daily_pnl=0.0,
        weekly_pnl=0.0,
        drawdown_pct=0.0,
    )


def make_signal(quality: float = 0.75, direction: str = "LONG") -> Signal:
    return Signal(
        ticker="AAPL",
        direction=direction,
        quality_score=quality,
        sentiment_score=0.7,
        volume_z_score=2.5,
        momentum=0.02,
        convergence=0.15,
        source_post_count=12,
    )


@pytest.fixture
def sizer() -> PositionSizer:
    return PositionSizer()


# ── vix_scalar ────────────────────────────────────────────────────────────────

def test_vix_normal(cfg: SystemConfig) -> None:
    assert vix_scalar(15.0, cfg) == 1.0


def test_vix_slightly_elevated(cfg: SystemConfig) -> None:
    assert vix_scalar(22.0, cfg) == 0.75


def test_vix_elevated(cfg: SystemConfig) -> None:
    assert vix_scalar(27.0, cfg) == 0.50


def test_vix_high_fear(cfg: SystemConfig) -> None:
    assert vix_scalar(32.0, cfg) == 0.25


def test_vix_crisis(cfg: SystemConfig) -> None:
    assert vix_scalar(42.0, cfg) == 0.0


def test_vix_exact_boundary(cfg: SystemConfig) -> None:
    # boundary: vix == vix_slightly_elevated → scaled
    assert vix_scalar(20.0, cfg) == 0.75


# ── vol_scalar ────────────────────────────────────────────────────────────────

def test_vol_scalar_at_target(cfg: SystemConfig) -> None:
    # sigma_target / realised_vol = 0.15 / 0.15 = 1.0
    assert vol_scalar(0.15, cfg) == pytest.approx(1.0)


def test_vol_scalar_high_vol(cfg: SystemConfig) -> None:
    # sigma_target / realised_vol = 0.15 / 0.60 = 0.25 (floor)
    assert vol_scalar(0.60, cfg) == pytest.approx(0.25)


def test_vol_scalar_very_low_vol(cfg: SystemConfig) -> None:
    # 0.15 / 0.05 = 3.0 → capped at 1.0
    assert vol_scalar(0.05, cfg) == pytest.approx(1.0)


def test_vol_scalar_zero_vol(cfg: SystemConfig) -> None:
    # zero vol → return 1.0 (no leverage)
    assert vol_scalar(0.0, cfg) == 1.0


def test_vol_scalar_floor_at_0_25(cfg: SystemConfig) -> None:
    assert vol_scalar(10.0, cfg) == pytest.approx(0.25)


# ── compute: basic cases ──────────────────────────────────────────────────────

def test_compute_basic_shares(
    sizer: PositionSizer, account: AccountState, cfg: SystemConfig
) -> None:
    shares, reason = sizer.compute(
        make_signal(quality=0.75),
        account,
        entry_price=100.0,
        vix=15.0,
        realised_vol=0.15,
        cfg=cfg,
    )
    assert reason == "ok"
    assert shares >= 1
    # Expected: 100_000 × 0.02 × 0.5 × 0.75 × 1.0 × 1.0 = 750 / 100 = 7 shares
    assert shares == 7


def test_compute_returns_zero_on_vix_crisis(
    sizer: PositionSizer, account: AccountState, cfg: SystemConfig
) -> None:
    shares, reason = sizer.compute(
        make_signal(), account, entry_price=100.0, vix=42.0, cfg=cfg
    )
    assert shares == 0
    assert "crisis" in reason.lower() or "no new trades" in reason.lower()


def test_compute_invalid_price(
    sizer: PositionSizer, account: AccountState, cfg: SystemConfig
) -> None:
    shares, reason = sizer.compute(make_signal(), account, entry_price=0.0, cfg=cfg)
    assert shares == 0
    assert "entry_price" in reason


def test_compute_nlv_zero(sizer: PositionSizer, cfg: SystemConfig) -> None:
    account = AccountState(
        net_liquidation=0.0, cash=0.0, daily_pnl=0.0, weekly_pnl=0.0, drawdown_pct=0.0
    )
    shares, reason = sizer.compute(make_signal(), account, entry_price=100.0, cfg=cfg)
    assert shares == 0


# ── Concentration limits ──────────────────────────────────────────────────────

def test_compute_social_exposure_exceeded(
    sizer: PositionSizer, cfg: SystemConfig
) -> None:
    """If already at max social allocation, sizer returns 0."""
    # Build account with 20% social exposure already
    pos = Position(
        ticker="TSLA",
        direction="LONG",
        shares=200,
        entry_price=100.0,
        opened_at=datetime.utcnow(),
        stop_loss=90.0,
        take_profit=110.0,
    )
    account = AccountState(
        net_liquidation=100_000.0,
        cash=80_000.0,
        daily_pnl=0.0,
        weekly_pnl=0.0,
        drawdown_pct=0.0,
        open_positions=[pos],  # cost_basis = 200 × 100 = 20_000 = 20% of NLV
    )
    shares, reason = sizer.compute(make_signal(), account, entry_price=100.0, cfg=cfg)
    assert shares == 0
    assert "social" in reason.lower() or "allocation" in reason.lower()


def test_compute_reduced_50_halves_position(
    sizer: PositionSizer, account: AccountState, cfg: SystemConfig
) -> None:
    """Simulates REDUCED_50 by halving max_position_pct."""
    import dataclasses
    reduced_cfg = dataclasses.replace(cfg, max_position_pct=cfg.max_position_pct * 0.5)
    shares_full, _ = sizer.compute(
        make_signal(quality=0.75), account, entry_price=100.0, vix=15.0, cfg=cfg
    )
    shares_reduced, _ = sizer.compute(
        make_signal(quality=0.75), account, entry_price=100.0, vix=15.0, cfg=reduced_cfg
    )
    assert shares_full == shares_reduced * 2 or shares_reduced <= shares_full


# ── stop_loss_price / take_profit_price ───────────────────────────────────────

def test_stop_loss_long(sizer: PositionSizer, cfg: SystemConfig) -> None:
    sl = sizer.stop_loss_price("LONG", 100.0, 2.0, cfg)
    assert sl == pytest.approx(96.0)  # 100 - 2*2


def test_stop_loss_short(sizer: PositionSizer, cfg: SystemConfig) -> None:
    sl = sizer.stop_loss_price("SHORT", 100.0, 2.0, cfg)
    assert sl == pytest.approx(104.0)  # 100 + 2*2


def test_take_profit_long(sizer: PositionSizer, cfg: SystemConfig) -> None:
    tp = sizer.take_profit_price("LONG", 100.0, cfg)
    assert tp == pytest.approx(104.0)  # 100 * 1.04


def test_take_profit_short(sizer: PositionSizer, cfg: SystemConfig) -> None:
    tp = sizer.take_profit_price("SHORT", 100.0, cfg)
    assert tp == pytest.approx(96.0)   # 100 * 0.96
