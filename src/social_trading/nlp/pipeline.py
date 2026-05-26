"""
NLPPipeline — orchestrates the full NLP processing chain for one post.

Processing chain (design §4a):
  [1] Bot Filter         — drop spam/bot accounts
  [2] Ticker Extraction  — refine / confirm ticker from text
  [3] StockTwits shortcut— native Bullish/Bearish label → skip classifiers
  [4] VADER pre-filter   — drop clearly neutral posts (< 1ms)
  [5] FinBERT classify   — 3-class financial sentiment (GPU, ~30ms)

Returns SentimentResult or None (None = post was filtered out).

The pipeline is stateless and async-compatible. All heavy classifiers
are injected (testable with fakes).
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SentimentResult, SocialPost
from social_trading.core.protocols import SentimentClassifier
from social_trading.nlp.filters.bot_filter import BotFilter
from social_trading.nlp.filters.ticker_extractor import TickerExtractor

logger = logging.getLogger(__name__)

# StockTwits native label → synthetic SentimentResult scores
_STOCKTWITS_LABEL_MAP: dict[str, tuple[float, float, float]] = {
    # (positive, negative, neutral)
    "Bullish": (0.85, 0.05, 0.10),
    "Bearish": (0.05, 0.85, 0.10),
}


class NLPPipeline:
    """
    End-to-end NLP processing pipeline for a single SocialPost.

    Dependencies injected for testability:
      - BotFilter          (pure Python, fast)
      - TickerExtractor    (pure Python + optional spaCy)
      - VaderClassifier    (pre-filter, ~1ms)
      - FinBERTClassifier  (primary classifier, ~30ms GPU)
      - SystemConfig       (thresholds, reloaded externally each loop)

    Usage:
        result = await pipeline.process(post, valid_tickers={"AAPL", "TSLA"})
        # result is None if post was filtered out
    """

    def __init__(
        self,
        bot_filter: BotFilter,
        ticker_extractor: TickerExtractor,
        prefilter: SentimentClassifier,    # VADER
        classifier: SentimentClassifier,   # FinBERT
        cfg: SystemConfig,
    ) -> None:
        self._bot_filter = bot_filter
        self._ticker_extractor = ticker_extractor
        self._prefilter = prefilter
        self._classifier = classifier
        self._cfg = cfg

    # ── Public API ────────────────────────────────────────────────────────────

    async def process(
        self,
        post: SocialPost,
        valid_tickers: set[str] | None = None,
    ) -> SentimentResult | None:
        """
        Run the full pipeline on one post.

        Args:
            post:          The post to process. NOT mutated.
            valid_tickers: Set of active watchlist tickers for Pass-3 extraction.
                           If None, only cashtag extraction (Pass 1) is used.

        Returns:
            SentimentResult if the post passes all filters, else None.
        """
        # [1] Bot filter
        if self._bot_filter.is_bot(post):
            logger.debug("pipeline: drop bot post %s", post.id)
            return None

        # [2] Ticker extraction — verify / refine ticker
        extracted = self._ticker_extractor.extract(post.text, valid_tickers)
        if extracted and post.ticker not in extracted:
            # Use first extracted ticker if the original wasn't found in text
            logger.debug(
                "pipeline: ticker mismatch post=%s original=%s extracted=%s",
                post.id, post.ticker, extracted,
            )
            # Don't mutate the post; keep original ticker for signal routing

        # [3] StockTwits shortcut — native directional label bypasses classifiers
        stocktwits_result = self._try_stocktwits_shortcut(post)
        if stocktwits_result is not None:
            return stocktwits_result

        # [4] VADER pre-filter — cheap neutrality gate
        vader_result = await self._prefilter.classify(post)
        if abs(vader_result.score) < self._cfg.vader_neutral_threshold:
            logger.debug(
                "pipeline: drop neutral post %s (vader=%.3f < threshold=%.3f)",
                post.id, vader_result.score, self._cfg.vader_neutral_threshold,
            )
            return None

        # [5] FinBERT — primary classification
        return await self._classifier.classify(post)

    async def process_batch(
        self,
        posts: list[SocialPost],
        valid_tickers: set[str] | None = None,
    ) -> list[SentimentResult]:
        """
        Process a batch of posts, returning only non-None results.
        FinBERT is called once in batch mode for efficiency.
        """
        # Run bot filter and VADER pre-filter inline (fast)
        finbert_candidates: list[SocialPost] = []
        early_results: list[SentimentResult] = []

        for post in posts:
            if self._bot_filter.is_bot(post):
                continue

            stocktwits_result = self._try_stocktwits_shortcut(post)
            if stocktwits_result is not None:
                early_results.append(stocktwits_result)
                continue

            vader_result = await self._prefilter.classify(post)
            if abs(vader_result.score) >= self._cfg.vader_neutral_threshold:
                finbert_candidates.append(post)

        # Batch FinBERT call
        finbert_results = await self._classifier.classify_batch(
            finbert_candidates, batch_size=self._cfg.finbert_batch_size
        )

        return early_results + finbert_results

    def update_cfg(self, cfg: SystemConfig) -> None:
        """Reload config — called by service before each batch."""
        self._cfg = cfg
        self._bot_filter.update_cfg(cfg)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _try_stocktwits_shortcut(self, post: SocialPost) -> SentimentResult | None:
        """
        If the post has a native StockTwits Bullish/Bearish label, synthesise
        a SentimentResult directly, skipping VADER and FinBERT entirely.
        Returns None if no native label is available.
        """
        label = post.raw.get("sentiment_label", "")
        if not label or label not in _STOCKTWITS_LABEL_MAP:
            return None

        pos, neg, neu = _STOCKTWITS_LABEL_MAP[label]
        return SentimentResult(
            post_id=post.id,
            ticker=post.ticker,
            positive=pos,
            negative=neg,
            neutral=neu,
            score=pos - neg,
            model="stocktwits_native",
            latency_ms=0.0,
            classified_at=datetime.now(UTC),
        )
