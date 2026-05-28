"""
SignalGenerator — converts aggregated sentiment into a tradeable Signal.

Quality score formula (design §5b):
  quality = w_volume      × v   (volume Z-score, normalised)
           + w_sentiment  × s   (sentiment strength)
           + w_proactivity × p  (1 if not reactive, 0 if price already moved)
           + w_momentum   × m   (recent price momentum, normalised)
           + w_convergence × c  (cross-platform agreement fraction)

  The raw sum is then normalised by the sum of *active* weights so that
  unavailable factors (e.g. price_momentum=0.0 before Phase 5 market data
  service) do not permanently lower the score ceiling.

Signal fires when:
  quality >= cfg.signal_quality_threshold
  AND |mean_score| >= cfg.sentiment_strength_min
  AND price direction aligns (if market data available)

This module is pure Python with no I/O — every input is passed
explicitly, making it trivially testable.

Design reference: docs/design/05-signal-generation.md §5b
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Literal

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import Signal
from social_trading.signals.aggregator import SentimentStats

logger = logging.getLogger(__name__)

Direction = Literal["LONG", "SHORT"]


class SignalGenerator:
    """
    Stateless signal generator.

    Usage:
        gen = SignalGenerator()
        signal = gen.evaluate(stats, cfg, volume_zscore=2.5)
    """

    def evaluate(
        self,
        stats: SentimentStats,
        cfg: SystemConfig,
        volume_zscore: float = 0.0,
        price_momentum: float = 0.0,
        is_reactive: bool = False,
    ) -> Signal | None:
        """
        Evaluate a SentimentStats snapshot and produce a Signal or None.

        Args:
            stats:          Aggregated sentiment from SentimentAggregator.
            cfg:            Current SystemConfig (thresholds, weights).
            volume_zscore:  Mention volume Z-score vs 7-day baseline.
            price_momentum: Recent price change (fraction, e.g. 0.02 = +2%).
                            0.0 = no price data available (treated as neutral).
            is_reactive:    True if the price moved significantly BEFORE the
                            mention spike — suggests noise, not signal.

        Returns:
            Signal if quality threshold met, else None.
        """
        if stats.post_count < 2:
            logger.debug("%s: skip — only %d posts in window", stats.ticker, stats.post_count)
            return None

        # ── 1. Determine candidate direction ─────────────────────────────────
        direction = _candidate_direction(stats.mean_score, cfg)
        if direction is None:
            logger.debug(
                "%s: flat — |score|=%.3f < min=%.3f",
                stats.ticker, abs(stats.mean_score), cfg.sentiment_strength_min,
            )
            return None

        # ── 2. Price alignment check (skip if no market data) ────────────────
        if price_momentum != 0.0 and not _price_aligned(direction, price_momentum):
            logger.debug(
                "%s: price misaligned — direction=%s momentum=%.3f",
                stats.ticker, direction, price_momentum,
            )
            return None

        # ── 3. Compute quality factors ────────────────────────────────────────
        v = _normalise_volume(volume_zscore)
        s = min(abs(stats.mean_score), 1.0)
        p = 0.0 if is_reactive else 1.0
        m = _normalise_momentum(price_momentum)
        c = _convergence(stats.source_scores, direction, cfg)

        raw_quality = (
            cfg.w_volume       * v
            + cfg.w_sentiment  * s
            + cfg.w_proactivity * p
            + cfg.w_momentum   * m
            + cfg.w_convergence * c
        )

        # When price_momentum is unavailable (always 0.0 until Phase 5 market
        # data service), normalise by the sum of active weights so the missing
        # factor does not lower the ceiling below the threshold.
        active_weight_sum = (
            cfg.w_volume + cfg.w_sentiment + cfg.w_proactivity + cfg.w_convergence
            + (cfg.w_momentum if price_momentum != 0.0 else 0.0)
        )
        quality = raw_quality / max(active_weight_sum, 1e-9)

        logger.debug(
            "%s quality=%.3f (v=%.2f s=%.2f p=%.2f m=%.2f c=%.2f) threshold=%.2f",
            stats.ticker, quality, v, s, p, m, c, cfg.signal_quality_threshold,
        )

        if quality < cfg.signal_quality_threshold:
            return None

        # ── 4. Build Signal ───────────────────────────────────────────────────
        return Signal(
            ticker=stats.ticker,
            direction=direction,
            quality_score=round(quality, 4),
            sentiment_score=round(stats.mean_score, 4),
            volume_z_score=round(volume_zscore, 4),
            momentum=round(price_momentum, 4),
            convergence=round(c, 4),
            source_post_count=stats.post_count,
            generated_at=datetime.now(UTC),
            metadata={
                "positive_count": stats.positive_count,
                "negative_count": stats.negative_count,
                "sources": sorted(stats.sources),
                "window_hours": stats.window_hours,
                "is_reactive": is_reactive,
                "quality_factors": {"v": v, "s": s, "p": p, "m": m, "c": c},
            },
        )


# ── Pure helpers ──────────────────────────────────────────────────────────────

def quality_score(
    v: float, s: float, p: float, m: float, c: float, cfg: SystemConfig
) -> float:
    """
    Standalone quality score function (matches design §5b formula).
    Exported for use in parameter optimization and UI display.
    """
    return (
        cfg.w_volume       * v
        + cfg.w_sentiment  * s
        + cfg.w_proactivity * p
        + cfg.w_momentum   * m
        + cfg.w_convergence * c
    )


def _candidate_direction(mean_score: float, cfg: SystemConfig) -> Direction | None:
    """Determine direction from mean sentiment, or None if below threshold."""
    if mean_score >= cfg.sentiment_strength_min:
        return "LONG"
    if mean_score <= -cfg.sentiment_strength_min:
        return "SHORT"
    return None


def _price_aligned(direction: Direction, price_momentum: float) -> bool:
    """Price movement must not strongly contradict sentiment direction."""
    if direction == "LONG":
        return price_momentum >= -0.02   # allow slight dip (buying the dip)
    return price_momentum <= 0.02        # allow slight rise for SHORT


def _normalise_volume(zscore: float) -> float:
    """Normalise mention Z-score to [0, 1]. Z=3.0 → v=1.0."""
    return min(max(zscore / 3.0, 0.0), 1.0)


def _normalise_momentum(price_momentum: float) -> float:
    """Normalise price momentum to [0, 1]. 10% move → m=1.0."""
    return min(abs(price_momentum) / 0.10, 1.0)


def _convergence(
    source_scores: dict[str, float],
    direction: Direction,
    cfg: SystemConfig,
) -> float:
    """
    Fraction of platforms agreeing with `direction`, scaled by convergence_bonus.

    Example: Twitter bullish + Reddit bullish → c = 1.0 × convergence_bonus
             Twitter bullish + Reddit bearish → c = 0.5 × convergence_bonus
    """
    if not source_scores:
        return 0.0
    agreeing = sum(
        1 for score in source_scores.values()
        if (direction == "LONG" and score > 0) or (direction == "SHORT" and score < 0)
    )
    fraction = agreeing / len(source_scores)
    return fraction * cfg.convergence_bonus
