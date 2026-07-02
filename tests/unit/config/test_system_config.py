"""Unit tests for SystemConfig — validation, persistence, and hash."""
from __future__ import annotations

import json

from social_trading.config.system_config import SystemConfig

# ── Validation ────────────────────────────────────────────────────────────────

async def test_default_config_is_valid(cfg: SystemConfig) -> None:
    errors = cfg.validate()
    assert errors == [], f"Default config has validation errors: {errors}"


async def test_weight_sum_must_equal_one(cfg: SystemConfig) -> None:
    cfg.w_volume = 0.50   # sum becomes 1.20
    errors = cfg.validate()
    assert any("weights" in e.lower() for e in errors)


async def test_daily_loss_less_than_weekly(cfg: SystemConfig) -> None:
    cfg.loss_limit_daily = 0.10
    cfg.loss_limit_weekly = 0.05   # weekly < daily — invalid
    errors = cfg.validate()
    assert any("daily" in e.lower() for e in errors)


async def test_drawdown_halt_greater_than_monthly(cfg: SystemConfig) -> None:
    cfg.drawdown_halt = 0.10
    cfg.loss_limit_monthly = 0.15   # drawdown < monthly — invalid
    errors = cfg.validate()
    assert any("drawdown" in e.lower() for e in errors)


async def test_max_position_cannot_exceed_single_position(cfg: SystemConfig) -> None:
    cfg.max_position_pct = 0.15
    cfg.max_single_position = 0.10   # per-signal > single-name cap — invalid
    errors = cfg.validate()
    assert any("position" in e.lower() for e in errors)


async def test_vix_crisis_must_exceed_high_fear(cfg: SystemConfig) -> None:
    cfg.vix_crisis = 25.0
    cfg.vix_high_fear = 30.0   # crisis < high_fear — invalid
    errors = cfg.validate()
    assert any("vix" in e.lower() for e in errors)


# ── Persistence ────────────────────────────────────────────────────────────────

async def test_save_and_reload_round_trips(redis) -> None:
    cfg = SystemConfig(signal_phase1_threshold=0.45, max_hold_sessions=5)
    await cfg.save(redis)

    loaded = await SystemConfig.load(redis)
    assert loaded.signal_phase1_threshold == 0.45
    assert loaded.max_hold_sessions == 5


async def test_load_migrates_legacy_max_hold_hours(redis) -> None:
    """Old configs with max_hold_hours should be migrated to max_hold_sessions on load."""
    import json
    from dataclasses import asdict
    legacy = asdict(SystemConfig())
    legacy.pop("max_hold_sessions", None)
    legacy["max_hold_hours"] = 48  # old field: 48h → ceil(48/6.5) = 8 sessions
    await redis.set(SystemConfig.REDIS_KEY, json.dumps(legacy))

    loaded = await SystemConfig.load(redis)
    import math
    assert loaded.max_hold_sessions == math.ceil(48 / 6.5)


async def test_load_migrates_legacy_max_hold_trading_days(redis) -> None:
    """Old configs with max_hold_trading_days should be migrated to max_hold_sessions."""
    import json
    from dataclasses import asdict
    legacy = asdict(SystemConfig())
    legacy.pop("max_hold_sessions", None)
    legacy["max_hold_trading_days"] = 4
    await redis.set(SystemConfig.REDIS_KEY, json.dumps(legacy))

    loaded = await SystemConfig.load(redis)
    assert loaded.max_hold_sessions == 4


async def test_load_returns_defaults_when_nothing_saved(redis) -> None:
    cfg = await SystemConfig.load(redis)
    default = SystemConfig()
    assert cfg.signal_phase1_threshold == default.signal_phase1_threshold


async def test_unknown_keys_in_redis_are_ignored(redis) -> None:
    """Forward-compatibility: new keys stored by future version don't crash load."""
    data = {"signal_phase1_threshold": "0.35", "future_param_xyz": "42"}
    await redis.set("config:system", json.dumps(data))
    cfg = await SystemConfig.load(redis)
    assert cfg.signal_phase1_threshold == 0.35


# ── Config Hash ────────────────────────────────────────────────────────────────

async def test_config_hash_is_16_chars(cfg: SystemConfig) -> None:
    h = cfg.config_hash()
    assert len(h) == 16


async def test_same_config_same_hash(cfg: SystemConfig) -> None:
    assert cfg.config_hash() == cfg.config_hash()


def test_different_configs_different_hashes() -> None:
    c1 = SystemConfig(signal_phase1_threshold=0.35)
    c2 = SystemConfig(signal_phase1_threshold=0.55)
    assert c1.config_hash() != c2.config_hash()
