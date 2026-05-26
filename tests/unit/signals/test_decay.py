"""Unit tests for signal decay functions."""
from __future__ import annotations

import math

import pytest

from social_trading.signals.decay import apply_decay, half_life_hours, is_expired

# ── apply_decay ───────────────────────────────────────────────────────────────

def test_no_decay_at_t_zero() -> None:
    assert apply_decay(0.8, hours_elapsed=0.0, decay_lambda=0.1) == pytest.approx(0.8)


def test_decay_reduces_magnitude() -> None:
    decayed = apply_decay(1.0, hours_elapsed=7.0, decay_lambda=0.1)
    assert 0 < decayed < 1.0


def test_decay_preserves_sign_positive() -> None:
    assert apply_decay(0.5, 10.0, 0.1) > 0


def test_decay_preserves_sign_negative() -> None:
    assert apply_decay(-0.5, 10.0, 0.1) < 0


def test_decay_formula() -> None:
    """Verify exact formula: score × e^(−λ × t)."""
    raw, hours, lam = 1.0, 6.931, 0.1
    expected = raw * math.exp(-lam * hours)
    assert apply_decay(raw, hours, lam) == pytest.approx(expected, rel=1e-6)


def test_decay_at_large_t_approaches_zero() -> None:
    assert abs(apply_decay(1.0, hours_elapsed=1000.0, decay_lambda=0.1)) < 1e-40


def test_decay_negative_hours_raises() -> None:
    with pytest.raises(ValueError):
        apply_decay(1.0, hours_elapsed=-1.0, decay_lambda=0.1)


def test_decay_zero_lambda_no_change() -> None:
    """λ=0 → no decay."""
    assert apply_decay(0.7, 100.0, 0.0) == pytest.approx(0.7)


# ── is_expired ────────────────────────────────────────────────────────────────

def test_not_expired_below_threshold() -> None:
    assert is_expired(47.9, max_hours=48) is False


def test_expired_above_threshold() -> None:
    assert is_expired(48.1, max_hours=48) is True


def test_exactly_at_threshold_not_expired() -> None:
    assert is_expired(48.0, max_hours=48) is False


def test_zero_hours_not_expired() -> None:
    assert is_expired(0.0, max_hours=48) is False


# ── half_life_hours ───────────────────────────────────────────────────────────

def test_half_life_formula() -> None:
    """t½ = ln(2) / λ."""
    assert half_life_hours(0.1) == pytest.approx(math.log(2) / 0.1, rel=1e-6)


def test_half_life_default_lambda() -> None:
    """Default λ=0.1 should give ~6.93 hours."""
    hl = half_life_hours(0.1)
    assert 6.5 < hl < 7.5


def test_half_life_zero_lambda_infinite() -> None:
    assert half_life_hours(0.0) == float("inf")


def test_half_life_large_lambda_short() -> None:
    """Large λ → short half-life."""
    assert half_life_hours(1.0) < 1.0
