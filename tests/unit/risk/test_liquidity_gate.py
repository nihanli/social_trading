"""Unit tests for LiquidityGate — pure computation, no I/O."""
from __future__ import annotations

import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import Signal
from social_trading.risk.liquidity_gate import (
    LiquidityGate,
    LiquidityQuote,
    _spread_bps,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def cfg() -> SystemConfig:
    return SystemConfig(
        trade_min_adv_usd=500_000,
        trade_min_mcap_usd=50_000_000,
        trade_max_spread_bps=100,
        trade_max_order_adv_pct=0.005,
    )


@pytest.fixture
def gate() -> LiquidityGate:
    return LiquidityGate()


def make_signal(ticker: str = "AAPL") -> Signal:
    return Signal(
        ticker=ticker,
        direction="LONG",
        quality_score=0.75,
        sentiment_score=0.6,
        volume_z_score=2.5,
        momentum=0.02,
        convergence=0.15,
        source_post_count=10,
    )


def make_quote(
    ticker: str = "AAPL",
    adv_usd: float = 50_000_000.0,
    market_cap_usd: float = 2_000_000_000.0,
    bid: float = 149.9,
    ask: float = 150.1,
    adv_shares: float = 1_000_000.0,
) -> LiquidityQuote:
    return LiquidityQuote(
        ticker=ticker,
        last_price=(bid + ask) / 2,
        bid=bid,
        ask=ask,
        adv_shares=adv_shares,
        adv_usd=adv_usd,
        market_cap_usd=market_cap_usd,
    )


# ── Happy path ────────────────────────────────────────────────────────────────

def test_passes_all_checks(gate: LiquidityGate, cfg: SystemConfig) -> None:
    result = gate.check(make_signal(), make_quote(), cfg, order_shares=100)
    assert result.passed is True
    assert result.reason == "ok"


# ── ADV check ─────────────────────────────────────────────────────────────────

def test_fails_adv_too_low(gate: LiquidityGate, cfg: SystemConfig) -> None:
    quote = make_quote(adv_usd=100_000.0)  # < 500_000 minimum
    result = gate.check(make_signal(), quote, cfg)
    assert result.passed is False
    assert "ADV" in result.reason


def test_passes_adv_at_minimum(gate: LiquidityGate, cfg: SystemConfig) -> None:
    quote = make_quote(adv_usd=500_000.0)
    result = gate.check(make_signal(), quote, cfg)
    assert result.passed is True


# ── Market cap check ──────────────────────────────────────────────────────────

def test_fails_mcap_too_low(gate: LiquidityGate, cfg: SystemConfig) -> None:
    quote = make_quote(market_cap_usd=10_000_000.0)  # < 50_000_000 minimum
    result = gate.check(make_signal(), quote, cfg)
    assert result.passed is False
    assert "market cap" in result.reason.lower() or "cap" in result.reason.lower()


# ── Spread check ──────────────────────────────────────────────────────────────

def test_fails_spread_too_wide(gate: LiquidityGate, cfg: SystemConfig) -> None:
    # bid=100, ask=102 → spread = 2/101 ≈ 198 bps > 100 max
    quote = make_quote(bid=100.0, ask=102.0)
    result = gate.check(make_signal(), quote, cfg)
    assert result.passed is False
    assert "spread" in result.reason.lower() or "bps" in result.reason.lower()


def test_passes_tight_spread(gate: LiquidityGate, cfg: SystemConfig) -> None:
    # bid=100, ask=100.05 → spread ≈ 5 bps
    quote = make_quote(bid=100.0, ask=100.05)
    result = gate.check(make_signal(), quote, cfg)
    assert result.passed is True


# ── ADV% order size check ─────────────────────────────────────────────────────

def test_fails_order_too_large_vs_adv(gate: LiquidityGate, cfg: SystemConfig) -> None:
    # adv_shares=100_000; order=1_000 → 1% > 0.5% max
    quote = make_quote(adv_shares=100_000.0)
    result = gate.check(make_signal(), quote, cfg, order_shares=1_000)
    assert result.passed is False
    assert "adv" in result.reason.lower() or "%" in result.reason


def test_passes_small_order_vs_adv(gate: LiquidityGate, cfg: SystemConfig) -> None:
    # adv_shares=1_000_000; order=100 → 0.01% < 0.5% max
    quote = make_quote(adv_shares=1_000_000.0)
    result = gate.check(make_signal(), quote, cfg, order_shares=100)
    assert result.passed is True


def test_zero_order_shares_skips_adv_pct_check(
    gate: LiquidityGate, cfg: SystemConfig
) -> None:
    """order_shares=0 means check is skipped — always passes this sub-check."""
    quote = make_quote(adv_shares=1.0)  # tiny ADV
    result = gate.check(make_signal(), quote, cfg, order_shares=0)
    assert result.passed is True


# ── Batch check ───────────────────────────────────────────────────────────────

def test_batch_check_mixed_results(gate: LiquidityGate, cfg: SystemConfig) -> None:
    signals = [make_signal("AAPL"), make_signal("ILLIQUID")]
    quotes = {
        "AAPL": make_quote("AAPL"),
        "ILLIQUID": make_quote("ILLIQUID", adv_usd=1000.0),
    }
    results = gate.check_batch(signals, quotes, cfg)
    assert len(results) == 2
    aapl_result = next(r for s, r in results if s.ticker == "AAPL")
    illiquid_result = next(r for s, r in results if s.ticker == "ILLIQUID")
    assert aapl_result.passed is True
    assert illiquid_result.passed is False


def test_batch_check_missing_quote(gate: LiquidityGate, cfg: SystemConfig) -> None:
    signals = [make_signal("NODATA")]
    results = gate.check_batch(signals, quotes={}, cfg=cfg)
    assert len(results) == 1
    _, result = results[0]
    assert result.passed is False
    assert "no_quote" in result.reason


# ── _spread_bps helper ────────────────────────────────────────────────────────

def test_spread_bps_calculation() -> None:
    quote = make_quote(bid=100.0, ask=101.0)
    # spread = 1 / 100.5 × 10000 ≈ 99.5 bps
    bps = _spread_bps(quote)
    assert 95 < bps < 105


def test_spread_bps_zero_mid() -> None:
    quote = LiquidityQuote(
        ticker="X", last_price=0.0, bid=0.0, ask=0.0,
        adv_shares=0.0, adv_usd=0.0, market_cap_usd=0.0,
    )
    assert _spread_bps(quote) == 0.0


def test_spread_bps_tight() -> None:
    quote = make_quote(bid=100.0, ask=100.01)
    bps = _spread_bps(quote)
    assert bps < 5
