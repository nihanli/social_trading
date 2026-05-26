"""Unit tests for CircuitBreaker — uses fakeredis."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import fakeredis.aioredis
import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import AccountState
from social_trading.risk.circuit_breaker import (
    CIRCUIT_KEY,
    CircuitBreaker,
    CircuitState,
    _PersistedState,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
async def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=False)


@pytest.fixture
async def breaker(redis) -> CircuitBreaker:
    return CircuitBreaker(redis)


@pytest.fixture
def cfg() -> SystemConfig:
    return SystemConfig(
        loss_limit_daily=0.03,
        loss_limit_weekly=0.07,
        drawdown_halt=0.20,
        loss_limit_single_trade=0.01,
    )


def make_account(
    nlv: float = 100_000.0,
    daily_pnl: float = 0.0,
    weekly_pnl: float = 0.0,
    drawdown_pct: float = 0.0,
) -> AccountState:
    return AccountState(
        net_liquidation=nlv,
        cash=nlv,
        daily_pnl=daily_pnl,
        weekly_pnl=weekly_pnl,
        drawdown_pct=drawdown_pct,
    )


# ── Normal state ──────────────────────────────────────────────────────────────

async def test_normal_state_allows_trades(
    breaker: CircuitBreaker, cfg: SystemConfig
) -> None:
    status = await breaker.check(make_account(), cfg)
    assert status.allow is True
    assert status.state == CircuitState.NORMAL
    assert status.size_multiplier == 1.0


async def test_initial_state_is_normal(
    breaker: CircuitBreaker, redis
) -> None:
    ps = await breaker.load_state()
    assert ps.state == CircuitState.NORMAL.value


# ── DAILY_HALT ────────────────────────────────────────────────────────────────

async def test_daily_loss_triggers_halt(
    breaker: CircuitBreaker, cfg: SystemConfig
) -> None:
    # daily_pnl / nlv = -4000 / 100000 = -4% < -3% limit
    account = make_account(daily_pnl=-4_000.0)
    status = await breaker.check(account, cfg)
    assert status.state == CircuitState.DAILY_HALT
    assert status.allow is False
    assert status.size_multiplier == 0.0


async def test_daily_halt_persisted_to_redis(
    breaker: CircuitBreaker, cfg: SystemConfig, redis
) -> None:
    await breaker.check(make_account(daily_pnl=-4_000.0), cfg)
    raw = await redis.get(CIRCUIT_KEY)
    assert raw is not None
    import json
    data = json.loads(raw)
    assert data["state"] == CircuitState.DAILY_HALT.value


async def test_below_daily_limit_stays_normal(
    breaker: CircuitBreaker, cfg: SystemConfig
) -> None:
    account = make_account(daily_pnl=-2_000.0)  # -2% < 3% threshold
    status = await breaker.check(account, cfg)
    assert status.state == CircuitState.NORMAL


# ── REDUCED_50 ────────────────────────────────────────────────────────────────

async def test_weekly_loss_triggers_reduced(
    breaker: CircuitBreaker, cfg: SystemConfig
) -> None:
    account = make_account(weekly_pnl=-8_000.0)  # -8% > 7% limit
    status = await breaker.check(account, cfg)
    assert status.state == CircuitState.REDUCED_50
    assert status.allow is True
    assert status.size_multiplier == 0.5


async def test_daily_halt_takes_priority_over_weekly(
    breaker: CircuitBreaker, cfg: SystemConfig
) -> None:
    """Daily halt should trigger even if weekly is also breached."""
    account = make_account(daily_pnl=-4_000.0, weekly_pnl=-8_000.0)
    status = await breaker.check(account, cfg)
    assert status.state == CircuitState.DAILY_HALT


# ── FULL_HALT ─────────────────────────────────────────────────────────────────

async def test_drawdown_triggers_full_halt(
    breaker: CircuitBreaker, cfg: SystemConfig
) -> None:
    account = make_account(drawdown_pct=0.25)  # 25% > 20% halt
    status = await breaker.check(account, cfg)
    assert status.state == CircuitState.FULL_HALT
    assert status.allow is False


async def test_full_halt_blocks_all_checks(
    breaker: CircuitBreaker, cfg: SystemConfig
) -> None:
    """Once in FULL_HALT, even healthy account state is blocked."""
    await breaker.check(make_account(drawdown_pct=0.25), cfg)
    # Now call check with healthy account
    status = await breaker.check(make_account(), cfg)
    assert status.state == CircuitState.FULL_HALT
    assert status.allow is False


async def test_manual_reset_clears_full_halt(
    breaker: CircuitBreaker, cfg: SystemConfig
) -> None:
    await breaker.check(make_account(drawdown_pct=0.25), cfg)
    await breaker.manual_reset()
    status = await breaker.check(make_account(), cfg)
    assert status.state == CircuitState.NORMAL
    assert status.allow is True


# ── Auto-reset DAILY_HALT ──────────────────────────────────────────────────────

async def test_daily_halt_auto_resets_next_day(
    breaker: CircuitBreaker, cfg: SystemConfig, redis
) -> None:
    """DAILY_HALT with halt_date set to yesterday should auto-reset."""
    import json
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date().isoformat()
    ps = _PersistedState(
        state=CircuitState.DAILY_HALT.value,
        triggered_at=datetime.now(UTC).isoformat(),
        trigger_reason="test",
        halt_date=yesterday,
    )
    from dataclasses import asdict
    await redis.set(CIRCUIT_KEY, json.dumps(asdict(ps)))

    status = await breaker.check(make_account(), cfg)
    assert status.state == CircuitState.NORMAL
    assert status.allow is True


# ── single_trade_breached ─────────────────────────────────────────────────────

def test_single_trade_breached_true(cfg: SystemConfig) -> None:
    breaker = CircuitBreaker.__new__(CircuitBreaker)
    # loss_pct = 200/10000 = 2% > 1% limit
    assert breaker.single_trade_breached(pnl=-200.0, entry_cost=10_000.0, cfg=cfg)


def test_single_trade_not_breached(cfg: SystemConfig) -> None:
    breaker = CircuitBreaker.__new__(CircuitBreaker)
    # loss_pct = 50/10000 = 0.5% < 1% limit
    assert not breaker.single_trade_breached(pnl=-50.0, entry_cost=10_000.0, cfg=cfg)


def test_single_trade_profitable(cfg: SystemConfig) -> None:
    breaker = CircuitBreaker.__new__(CircuitBreaker)
    assert not breaker.single_trade_breached(pnl=500.0, entry_cost=10_000.0, cfg=cfg)


def test_single_trade_zero_cost(cfg: SystemConfig) -> None:
    breaker = CircuitBreaker.__new__(CircuitBreaker)
    assert not breaker.single_trade_breached(pnl=-100.0, entry_cost=0.0, cfg=cfg)
