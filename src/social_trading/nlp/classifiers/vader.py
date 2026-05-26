"""
VaderClassifier — VADER-based fast sentiment pre-filter.

VADER (Valence Aware Dictionary and sEntiment Reasoner) runs in < 1ms
and requires no GPU. Used as a pre-filter: posts with |compound| below
cfg.vader_neutral_threshold are discarded before the expensive FinBERT
pass, cutting FinBERT load by ~40-60% in practice.

VADER output mapping → SentimentResult:
  positive  = vader 'pos' component  ∈ [0, 1]
  negative  = vader 'neg' component  ∈ [0, 1]
  neutral   = vader 'neu' component  ∈ [0, 1]
  score     = vader 'compound'       ∈ [-1, 1]
  (pos + neg + neu ≈ 1.0 by VADER's internal normalisation)
"""
from __future__ import annotations

import logging
import time
from typing import Any

from social_trading.core.models import SentimentResult, SocialPost

logger = logging.getLogger(__name__)


class VaderClassifier:
    """
    Thin async wrapper around vaderSentiment's SentimentIntensityAnalyzer.

    Satisfies the SentimentClassifier protocol.
    Synchronous internally (VADER has no I/O) — wrapped in async for
    protocol compatibility with the async NLP pipeline.
    """

    model_name: str = "vader"

    def __init__(self, analyzer: Any | None = None) -> None:
        """
        Args:
            analyzer: Optional pre-built SentimentIntensityAnalyzer.
                      If None, one is created on first use (lazy).
                      Inject in tests to avoid importing vaderSentiment.
        """
        self._analyzer = analyzer

    # ── SentimentClassifier protocol ──────────────────────────────────────────

    async def classify(self, post: SocialPost) -> SentimentResult:
        """Classify a single post. Runs synchronously (< 1ms)."""
        t0 = time.perf_counter()
        scores = self._get_analyzer().polarity_scores(post.text)
        latency_ms = (time.perf_counter() - t0) * 1000

        return SentimentResult(
            post_id=post.id,
            ticker=post.ticker,
            positive=float(scores["pos"]),
            negative=float(scores["neg"]),
            neutral=float(scores["neu"]),
            score=float(scores["compound"]),
            model=self.model_name,
            latency_ms=round(latency_ms, 3),
        )

    async def classify_batch(self, posts: list[SocialPost]) -> list[SentimentResult]:
        """Classify a batch. VADER is fast enough to run sequentially."""
        results: list[SentimentResult] = []
        for post in posts:
            results.append(await self.classify(post))
        return results

    # ── Internal ──────────────────────────────────────────────────────────────

    def _get_analyzer(self) -> Any:
        """Lazy-load the VADER analyzer."""
        if self._analyzer is None:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer  # noqa: PLC0415
            self._analyzer = SentimentIntensityAnalyzer()
        return self._analyzer
