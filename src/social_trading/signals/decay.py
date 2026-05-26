"""
Signal decay — time-value erosion for trading signals.

Social sentiment signals decay rapidly: a spike on Monday is irrelevant
by Wednesday. Two functions are provided:

  apply_decay()   — scale a raw score by exp(-λ × hours)
  is_expired()    — hard cut-off after cfg.signal_age_max_hours

Both are pure functions with no I/O; all parameters passed explicitly
so they are trivially testable and configurable through SystemConfig.

Design reference: docs/design/05-signal-generation.md §5c
λ = cfg.signal_decay_lambda = 0.10  →  half-life ≈ 6.9 hours
"""
from __future__ import annotations

import math


def apply_decay(raw_score: float, hours_elapsed: float, decay_lambda: float) -> float:
    """
    Apply exponential time-decay to a raw signal score.

    Formula: decayed = raw × e^(−λ × t)

    Args:
        raw_score:     Original signal score (any range — sign preserved).
        hours_elapsed: Hours since the signal was generated.
        decay_lambda:  Decay rate λ (cfg.signal_decay_lambda).
                       Default 0.10 → half-life ≈ 6.9 hours.

    Returns:
        Decayed score; same sign as raw_score, magnitude approaches 0.
    """
    if hours_elapsed < 0:
        raise ValueError(f"hours_elapsed must be >= 0, got {hours_elapsed}")
    return raw_score * math.exp(-decay_lambda * hours_elapsed)


def is_expired(hours_elapsed: float, max_hours: int) -> bool:
    """
    Return True if the signal is older than the hard expiry threshold.

    Args:
        hours_elapsed: Hours since signal was generated.
        max_hours:     Hard expiry threshold (cfg.signal_age_max_hours).

    Returns:
        True = signal is stale and should be discarded.
    """
    return hours_elapsed > max_hours


def half_life_hours(decay_lambda: float) -> float:
    """
    Compute signal half-life in hours from decay λ.

    t½ = ln(2) / λ

    Useful for displaying "signal half-life" in the Streamlit Config page.
    """
    if decay_lambda <= 0:
        return float("inf")
    return math.log(2) / decay_lambda
