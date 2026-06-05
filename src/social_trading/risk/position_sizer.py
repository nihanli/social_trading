"""
PositionSizer — computes share quantity for a signal.

Sizing formula (design §6a):
    base_size   = NLV × max_position_pct × half_kelly_fraction × quality_score
    vix_scalar  = regime multiplier from cfg.vix_* thresholds
    vol_scalar  = sigma_target / realised_vol  (cap at 1.0, floor at 0.25)
    raw_shares  = (base_size × vix_scalar × vol_scalar) / entry_price
    shares      = max(1, round(raw_shares))

Additional concentration checks:
  - social_exposure + new_cost must not exceed max_social_allocation
  - position cost must not exceed max_single_position × NLV

All inputs are passed explicitly — no I/O, fully testable.

Design reference: docs/design §6 risk management
"""
from __future__ import annotations

import logging
import math

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import AccountState, Direction, Signal

logger = logging.getLogger(__name__)


# ── VIX regime scalar ─────────────────────────────────────────────────────────

def vix_scalar(vix: float, cfg: SystemConfig) -> float:
    """
    Map VIX level to a size multiplier.

    Regimes (from cfg):
        vix > vix_crisis            → 0.00  (full halt)
        vix > vix_high_fear         → 0.25
        vix > vix_elevated          → 0.50
        vix > vix_slightly_elevated → 0.75
        otherwise                   → 1.00  (normal)
    """
    if vix > cfg.vix_crisis:
        return 0.0
    if vix > cfg.vix_high_fear:
        return 0.25
    if vix > cfg.vix_elevated:
        return 0.50
    if vix > cfg.vix_slightly_elevated:
        return 0.75
    return 1.0


def vol_scalar(realised_vol: float, cfg: SystemConfig) -> float:
    """
    Scale inversely to realised volatility so that each position targets
    cfg.sigma_target annualised volatility.

    Capped at 1.0 (never lever up) and floored at 0.25 (never go to zero).

    Args:
        realised_vol: annualised realised vol (e.g. 0.30 = 30%)
        cfg:          SystemConfig with sigma_target

    Returns:
        scalar in [0.25, 1.0]
    """
    if realised_vol <= 0.0:
        return 1.0
    raw = cfg.sigma_target / realised_vol
    return min(max(raw, 0.25), 1.0)


# ── Main sizer ────────────────────────────────────────────────────────────────

class PositionSizer:
    """
    Stateless position sizer — all context passed at call time.

    Usage:
        sizer = PositionSizer()
        shares, reason = sizer.compute(signal, account, entry_price, vix, realised_vol, cfg)
    """

    def compute(
        self,
        signal: Signal,
        account: AccountState,
        entry_price: float,
        vix: float = 15.0,
        realised_vol: float = 0.20,
        cfg: SystemConfig | None = None,
    ) -> tuple[int, str]:
        """
        Compute share quantity for a signal.

        Returns:
            (shares, reason_str) where shares=0 means "blocked" and
            reason explains why (concentration / price checks).
        """
        if cfg is None:
            cfg = SystemConfig()

        if entry_price <= 0:
            return 0, "invalid entry_price <= 0"

        nlv = account.net_liquidation
        if nlv <= 0:
            return 0, "NLV <= 0"

        # ── Base dollar allocation ────────────────────────────────────────────
        v_scalar = vix_scalar(vix, cfg)
        if v_scalar == 0.0:
            return 0, f"VIX={vix:.1f} above crisis threshold — no new trades"

        base_dollars = (
            nlv
            * cfg.max_position_pct
            * cfg.half_kelly_fraction
            * signal.quality_score
        )
        adjusted_dollars = base_dollars * v_scalar * vol_scalar(realised_vol, cfg)

        # ── Hard size cap ─────────────────────────────────────────────────────
        max_dollars = nlv * cfg.max_single_position
        adjusted_dollars = min(adjusted_dollars, max_dollars)

        # ── Social exposure concentration check ───────────────────────────────
        current_exposure_dollars = account.social_exposure * nlv
        max_social_dollars = nlv * cfg.max_social_allocation
        available_social = max_social_dollars - current_exposure_dollars
        if available_social <= 0:
            return 0, (
                f"Social exposure {account.social_exposure:.1%} >= "
                f"max_social_allocation {cfg.max_social_allocation:.1%}"
            )
        adjusted_dollars = min(adjusted_dollars, available_social)

        # ── Convert to shares ─────────────────────────────────────────────────
        raw_shares = adjusted_dollars / entry_price
        shares = max(1, math.floor(raw_shares))

        actual_cost = shares * entry_price
        logger.debug(
            "%s %s: %d shares @ $%.2f = $%.0f "
            "(vix_scalar=%.2f vol_scalar=%.2f quality=%.3f)",
            signal.ticker, signal.direction, shares, entry_price, actual_cost,
            v_scalar, vol_scalar(realised_vol, cfg), signal.quality_score,
        )
        return shares, "ok"

    def stop_loss_price(
        self,
        direction: Direction,
        entry_price: float,
        atr: float,
        cfg: SystemConfig,
    ) -> float:
        """
        Compute initial stop-loss price from ATR.

        LONG:  entry - atr_multiplier × ATR
        SHORT: entry + atr_multiplier × ATR
        """
        offset = cfg.atr_multiplier * atr
        if direction == "LONG":
            return round(entry_price - offset, 2)
        return round(entry_price + offset, 2)

    def take_profit_price(
        self,
        direction: Direction,
        entry_price: float,
        cfg: SystemConfig,
    ) -> float:
        """
        Compute take-profit price.

        LONG:  entry × (1 + take_profit_pct)
        SHORT: entry × (1 - take_profit_pct)

        Rounded to 2 decimal places (US equity minimum price variation = $0.01).
        """
        if direction == "LONG":
            return round(entry_price * (1.0 + cfg.take_profit_pct), 2)
        return round(entry_price * (1.0 - cfg.take_profit_pct), 2)
