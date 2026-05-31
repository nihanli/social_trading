"""Unit tests for SignalGenerator — pure computation, no I/O."""
from __future__ import annotations

import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import Signal
from social_trading.signals.aggregator import SentimentStats
from social_trading.signals.generator import (
    SignalGenerator,
    _candidate_direction,
    _convergence,
    _normalise_momentum,
    _normalise_volume,
    quality_score,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def gen() -> SignalGenerator:
    return SignalGenerator()


@pytest.fixture
def cfg() -> SystemConfig:
    return SystemConfig(
        sentiment_strength_min=0.30,
        w_volume=0.30,
        w_sentiment=0.25,
        w_proactivity=0.20,
        w_momentum=0.15,
        w_convergence=0.10,
        convergence_bonus=0.20,
    )


# Default quality threshold used across evaluate() test calls.
_THRESHOLD = 0.60


def make_stats(
    ticker: str = "AAPL",
    mean_score: float = 0.7,
    post_count: int = 10,
    sources: set[str] | None = None,
    source_scores: dict[str, float] | None = None,
) -> SentimentStats:
    sources = sources or {"twitter", "reddit"}
    source_scores = source_scores or {
        "twitter": mean_score,
        "reddit": mean_score * 0.9,
    }
    direction_score = mean_score
    pos = max(0, direction_score)
    neg = max(0, -direction_score)
    return SentimentStats(
        ticker=ticker,
        mean_score=mean_score,
        post_count=post_count,
        positive_count=int(pos * post_count),
        negative_count=int(neg * post_count),
        neutral_count=0,
        sources=sources,
        source_scores=source_scores,
        oldest_age_hours=0.5,
        newest_age_hours=0.05,
        window_hours=1.0,
    )


# ── Happy path ────────────────────────────────────────────────────────────────

def test_generates_long_signal(gen: SignalGenerator, cfg: SystemConfig) -> None:
    stats = make_stats(mean_score=0.7)
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=3.0, price_momentum=0.03)
    assert sig is not None
    assert isinstance(sig, Signal)
    assert sig.direction == "LONG"
    assert sig.ticker == "AAPL"


def test_generates_short_signal(gen: SignalGenerator, cfg: SystemConfig) -> None:
    stats = make_stats(
        mean_score=-0.7,
        source_scores={"twitter": -0.7, "reddit": -0.6},
    )
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=3.0, price_momentum=-0.03)
    assert sig is not None
    assert sig.direction == "SHORT"


def test_signal_quality_within_bounds(gen: SignalGenerator, cfg: SystemConfig) -> None:
    stats = make_stats(mean_score=0.8)
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=4.0)
    assert sig is not None
    assert 0.0 <= sig.quality_score <= 1.0


# ── Filtering ─────────────────────────────────────────────────────────────────

def test_below_quality_threshold_returns_none(gen: SignalGenerator, cfg: SystemConfig) -> None:
    # Low volume_zscore + barely above sentiment min → low quality
    stats = make_stats(mean_score=0.31, source_scores={"twitter": 0.31})
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=0.1, price_momentum=0.0)
    assert sig is None


def test_below_sentiment_threshold_returns_none(gen: SignalGenerator, cfg: SystemConfig) -> None:
    stats = make_stats(mean_score=0.15)  # below 0.30 min
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=5.0)
    assert sig is None


def test_insufficient_posts_returns_none(gen: SignalGenerator, cfg: SystemConfig) -> None:
    stats = make_stats(post_count=1)  # < 2 required
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=3.0)
    assert sig is None


def test_price_misaligned_long_returns_none(gen: SignalGenerator, cfg: SystemConfig) -> None:
    """Strongly negative price momentum contradicts LONG direction."""
    stats = make_stats(mean_score=0.7)
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=3.0, price_momentum=-0.10)
    assert sig is None


def test_price_misaligned_short_returns_none(gen: SignalGenerator, cfg: SystemConfig) -> None:
    stats = make_stats(
        mean_score=-0.7,
        source_scores={"twitter": -0.7},
    )
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=3.0, price_momentum=0.10)
    assert sig is None


def test_slight_price_dip_does_not_block_long(gen: SignalGenerator, cfg: SystemConfig) -> None:
    """Small dip (−1%) is allowed for LONG — buying the dip."""
    stats = make_stats(mean_score=0.7)
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=3.0, price_momentum=-0.01)
    assert sig is not None
    assert sig.direction == "LONG"


def test_no_price_data_does_not_block_signal(gen: SignalGenerator, cfg: SystemConfig) -> None:
    """price_momentum=0.0 means 'not available' — should not filter."""
    stats = make_stats(mean_score=0.7)
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=3.0, price_momentum=0.0)
    assert sig is not None


# ── Signal fields ─────────────────────────────────────────────────────────────

def test_signal_carries_volume_zscore(gen: SignalGenerator, cfg: SystemConfig) -> None:
    stats = make_stats(mean_score=0.7)
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=2.5)
    assert sig is not None
    assert sig.volume_z_score == pytest.approx(2.5)


def test_signal_carries_source_count(gen: SignalGenerator, cfg: SystemConfig) -> None:
    stats = make_stats(mean_score=0.7, post_count=15)
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=3.0)
    assert sig is not None
    assert sig.source_post_count == 15


def test_signal_metadata_has_sources(gen: SignalGenerator, cfg: SystemConfig) -> None:
    stats = make_stats(mean_score=0.7, sources={"twitter", "reddit"})
    sig = gen.evaluate(stats, cfg, quality_threshold=_THRESHOLD, volume_zscore=3.0)
    assert sig is not None
    assert "sources" in sig.metadata
    assert set(sig.metadata["sources"]) == {"twitter", "reddit"}


# ── Pure helper unit tests ────────────────────────────────────────────────────

def test_candidate_direction_long(cfg: SystemConfig) -> None:
    assert _candidate_direction(0.5, cfg) == "LONG"


def test_candidate_direction_short(cfg: SystemConfig) -> None:
    assert _candidate_direction(-0.5, cfg) == "SHORT"


def test_candidate_direction_flat(cfg: SystemConfig) -> None:
    assert _candidate_direction(0.10, cfg) is None  # below 0.30 min


def test_normalise_volume_caps_at_one() -> None:
    assert _normalise_volume(10.0) == 1.0


def test_normalise_volume_zero() -> None:
    assert _normalise_volume(0.0) == 0.0


def test_normalise_volume_three() -> None:
    assert _normalise_volume(3.0) == pytest.approx(1.0)


def test_normalise_momentum_caps_at_one() -> None:
    assert _normalise_momentum(0.5) == 1.0


def test_normalise_momentum_zero() -> None:
    assert _normalise_momentum(0.0) == 0.0


def test_convergence_all_agree(cfg: SystemConfig) -> None:
    source_scores = {"twitter": 0.7, "reddit": 0.5}
    c = _convergence(source_scores, "LONG", cfg)
    assert c == pytest.approx(cfg.convergence_bonus)


def test_convergence_mixed(cfg: SystemConfig) -> None:
    source_scores = {"twitter": 0.7, "reddit": -0.3}
    c = _convergence(source_scores, "LONG", cfg)
    # Only 1/2 agree → fraction = 0.5
    assert c == pytest.approx(0.5 * cfg.convergence_bonus)


def test_convergence_empty_sources(cfg: SystemConfig) -> None:
    assert _convergence({}, "LONG", cfg) == 0.0


def test_quality_score_formula(cfg: SystemConfig) -> None:
    """Verify the formula matches the design doc."""
    v, s, p, m, c = 1.0, 0.8, 1.0, 0.5, 0.2
    expected = (
        cfg.w_volume * v
        + cfg.w_sentiment * s
        + cfg.w_proactivity * p
        + cfg.w_momentum * m
        + cfg.w_convergence * c
    )
    assert quality_score(v, s, p, m, c, cfg) == pytest.approx(expected)


def test_quality_score_weights_sum() -> None:
    """Default weights must sum to 1.0."""
    cfg = SystemConfig()
    total = cfg.w_volume + cfg.w_sentiment + cfg.w_proactivity + cfg.w_momentum + cfg.w_convergence
    assert total == pytest.approx(1.0, abs=0.001)
