"""
CircuitBreaker — four-state risk interlock.

State machine (design §6b):
    NORMAL
      │ daily_pnl / NLV < -loss_limit_daily        → DAILY_HALT   (auto-reset next day)
      │ weekly_pnl / NLV < -loss_limit_weekly       → REDUCED_50   (auto-reset Monday)
      │ drawdown_pct       > drawdown_halt           → FULL_HALT    (manual reset only)
      │ single-trade loss  > loss_limit_single_trade → close immediately (stay NORMAL)

REDUCED_50  → max_position_pct halved while in this state
DAILY_HALT  → no new signals accepted today
FULL_HALT   → no new signals accepted; manual reset required

State is persisted to Redis key "circuit:state" as JSON so every service
reads the same state.

Usage:
    breaker = CircuitBreaker(redis)
    state = await breaker.load_state()
    decision = await breaker.check(account, cfg)
    # decision.allow  → bool
    # decision.state  → CircuitState
    # decision.size_multiplier → 0.5 (REDUCED_50) or 1.0 (NORMAL)
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redis.asyncio as aioredis

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import AccountState

logger = logging.getLogger(__name__)

CIRCUIT_KEY = "circuit:state"


class CircuitState(StrEnum):
    NORMAL = "NORMAL"
    REDUCED_50 = "REDUCED_50"
    DAILY_HALT = "DAILY_HALT"
    FULL_HALT = "FULL_HALT"


@dataclass
class CircuitStatus:
    """Immutable snapshot returned by CircuitBreaker.check()."""

    state: CircuitState
    allow: bool                  # True = new trades permitted
    size_multiplier: float       # 1.0, 0.5, or 0.0
    reason: str                  # human-readable explanation
    triggered_at: datetime | None = None
    reset_at: datetime | None = None


@dataclass
class _PersistedState:
    """JSON-serialisable state kept in Redis."""

    state: str = CircuitState.NORMAL.value
    triggered_at: str | None = None
    trigger_reason: str = ""
    halt_date: str | None = None   # ISO date string for DAILY_HALT reset

    def to_status(self) -> CircuitStatus:
        cs = CircuitState(self.state)
        allow = cs not in (CircuitState.DAILY_HALT, CircuitState.FULL_HALT)
        multiplier = {
            CircuitState.NORMAL: 1.0,
            CircuitState.REDUCED_50: 0.5,
            CircuitState.DAILY_HALT: 0.0,
            CircuitState.FULL_HALT: 0.0,
        }[cs]
        return CircuitStatus(
            state=cs,
            allow=allow,
            size_multiplier=multiplier,
            reason=self.trigger_reason or cs.value,
            triggered_at=(
                datetime.fromisoformat(self.triggered_at)
                if self.triggered_at else None
            ),
        )


class CircuitBreaker:
    """
    Redis-backed circuit breaker.

    All state transitions are persisted atomically so multiple services
    read consistent state.
    """

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    # ── State I/O ─────────────────────────────────────────────────────────────

    async def load_state(self) -> _PersistedState:
        raw = await self._redis.get(CIRCUIT_KEY)
        if raw is None:
            return _PersistedState()
        data = json.loads(raw)
        return _PersistedState(**data)

    async def _save_state(self, ps: _PersistedState) -> None:
        await self._redis.set(CIRCUIT_KEY, json.dumps(asdict(ps)))

    # ── Main check ────────────────────────────────────────────────────────────

    async def check(
        self,
        account: AccountState,
        cfg: SystemConfig,
    ) -> CircuitStatus:
        """
        Evaluate account state against risk thresholds. Persists any state
        change to Redis and returns a CircuitStatus.

        Callers should re-check on every evaluate cycle.
        """
        ps = await self.load_state()
        cs = CircuitState(ps.state)
        now = datetime.now(UTC)
        today = date.today().isoformat()

        # ── Auto-reset DAILY_HALT at start of new day ─────────────────────────
        if cs == CircuitState.DAILY_HALT and ps.halt_date != today:
            logger.info("CircuitBreaker: auto-reset DAILY_HALT (new day)")
            ps = _PersistedState()
            await self._save_state(ps)
            cs = CircuitState.NORMAL

        # ── Auto-reset REDUCED_50 on Monday ──────────────────────────────────
        if cs == CircuitState.REDUCED_50 and datetime.now(UTC).weekday() == 0 and ps.halt_date != today:
            logger.info("CircuitBreaker: auto-reset REDUCED_50 (new week)")
            ps = _PersistedState()
            await self._save_state(ps)
            cs = CircuitState.NORMAL

        # ── FULL_HALT: only manual reset can clear ─────────────────────────
        if cs == CircuitState.FULL_HALT:
            return ps.to_status()

        nlv = account.net_liquidation or 1.0

        # ── Check drawdown → FULL_HALT ────────────────────────────────────────
        if account.drawdown_pct > cfg.drawdown_halt:
            reason = (
                f"Drawdown {account.drawdown_pct:.1%} > halt threshold "
                f"{cfg.drawdown_halt:.1%}"
            )
            logger.warning("CircuitBreaker → FULL_HALT: %s", reason)
            ps = _PersistedState(
                state=CircuitState.FULL_HALT.value,
                triggered_at=now.isoformat(),
                trigger_reason=reason,
                halt_date=today,
            )
            await self._save_state(ps)
            return ps.to_status()

        # ── Check daily loss → DAILY_HALT ────────────────────────────────────
        daily_loss_pct = account.daily_pnl / nlv  # negative if loss
        if daily_loss_pct < -cfg.loss_limit_daily:
            reason = (
                f"Daily P&L {daily_loss_pct:.1%} < "
                f"-{cfg.loss_limit_daily:.1%} limit"
            )
            logger.warning("CircuitBreaker → DAILY_HALT: %s", reason)
            ps = _PersistedState(
                state=CircuitState.DAILY_HALT.value,
                triggered_at=now.isoformat(),
                trigger_reason=reason,
                halt_date=today,
            )
            await self._save_state(ps)
            return ps.to_status()

        # ── Check weekly loss → REDUCED_50 ───────────────────────────────────
        if cs != CircuitState.REDUCED_50:
            weekly_loss_pct = account.weekly_pnl / nlv
            if weekly_loss_pct < -cfg.loss_limit_weekly:
                reason = (
                    f"Weekly P&L {weekly_loss_pct:.1%} < "
                    f"-{cfg.loss_limit_weekly:.1%} limit"
                )
                logger.warning("CircuitBreaker → REDUCED_50: %s", reason)
                ps = _PersistedState(
                    state=CircuitState.REDUCED_50.value,
                    triggered_at=now.isoformat(),
                    trigger_reason=reason,
                    halt_date=today,
                )
                await self._save_state(ps)
                return ps.to_status()

        return ps.to_status()

    async def manual_reset(self) -> None:
        """
        Operator-initiated reset — clears FULL_HALT.
        Only valid action that can exit FULL_HALT.
        """
        logger.warning("CircuitBreaker: manual reset performed")
        await self._save_state(_PersistedState())

    def single_trade_breached(
        self, pnl: float, entry_cost: float, cfg: SystemConfig
    ) -> bool:
        """
        Returns True if a single position's loss exceeds the per-trade limit.
        Caller should close the position immediately; circuit state stays NORMAL.

        Args:
            pnl:        current unrealised or realised P&L (negative = loss)
            entry_cost: original cost basis of the position
        """
        if entry_cost <= 0:
            return False
        loss_pct = -pnl / entry_cost
        return loss_pct > cfg.loss_limit_single_trade
