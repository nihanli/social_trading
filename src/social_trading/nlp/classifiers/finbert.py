"""
FinBERTClassifier — primary financial sentiment classifier.

Model: yiyanghkust/finbert-tone (HuggingFace)
  Labels: negative (index 0), neutral (index 1), positive (index 2)
  Input: tokenized text, max 512 tokens
  Output: 3-class softmax probabilities

Design choices:
  - Model loaded lazily on first classify() call (expensive: ~400MB weights)
  - GPU used automatically when available; falls back to CPU silently
  - Batch classification (cfg.finbert_batch_size) amortises tokenisation cost
  - `_pipeline_fn` is injectable for tests (avoids loading real model)

Satisfies the SentimentClassifier protocol (core/protocols.py).
"""
from __future__ import annotations

import logging
import time
from typing import Any

from social_trading.core.models import SentimentResult, SocialPost

logger = logging.getLogger(__name__)

# HuggingFace model identifier
MODEL_ID = "yiyanghkust/finbert-tone"

# Label order for yiyanghkust/finbert-tone
# Verified from model card: id2label = {0: "Negative", 1: "Neutral", 2: "Positive"}
_LABEL_NEGATIVE = 0
_LABEL_NEUTRAL = 1
_LABEL_POSITIVE = 2


class FinBERTClassifier:
    """
    HuggingFace FinBERT-Tone sentiment classifier.

    Injectable pipeline for tests:
        fake_fn = lambda texts: [[{"label": "positive", "score": 0.9},
                                  {"label": "neutral",  "score": 0.08},
                                  {"label": "negative", "score": 0.02}]
                                 for _ in texts]
        clf = FinBERTClassifier(pipeline_fn=fake_fn)
    """

    model_name: str = "finbert"

    def __init__(
        self,
        pipeline_fn: Any | None = None,
        batch_size: int = 16,
        max_length: int = 512,
    ) -> None:
        """
        Args:
            pipeline_fn: Optional callable(texts: list[str], batch_size, truncation)
                         → list[list[dict{"label", "score"}]].
                         When None, the real HuggingFace pipeline is loaded lazily.
            batch_size:  Max posts per forward pass (overridden by cfg at call time).
            max_length:  Tokenizer truncation length.
        """
        self._pipeline_fn = pipeline_fn
        self._batch_size = batch_size
        self._max_length = max_length

    # ── SentimentClassifier protocol ──────────────────────────────────────────

    async def classify(self, post: SocialPost) -> SentimentResult:
        """Classify a single post."""
        results = await self.classify_batch([post])
        return results[0]

    async def classify_batch(
        self,
        posts: list[SocialPost],
        batch_size: int | None = None,
    ) -> list[SentimentResult]:
        """
        Classify a batch of posts. Groups into sub-batches of `batch_size`
        to avoid OOM on GPU with large inputs.
        """
        if not posts:
            return []

        effective_batch = batch_size or self._batch_size
        pipeline = self._get_pipeline()

        results: list[SentimentResult] = []
        for i in range(0, len(posts), effective_batch):
            chunk = posts[i : i + effective_batch]
            texts = [p.text[:1000] for p in chunk]  # guard against very long posts

            t0 = time.perf_counter()
            raw_outputs = pipeline(texts, truncation=True, max_length=self._max_length)
            latency_ms = (time.perf_counter() - t0) * 1000 / len(chunk)

            for post, label_scores in zip(chunk, raw_outputs, strict=True):
                result = self._parse_output(post, label_scores, latency_ms)
                results.append(result)

        return results

    # ── Internal ──────────────────────────────────────────────────────────────

    def _get_pipeline(self) -> Any:
        """
        Return the pipeline callable, loading the real model if not injected.
        Raises ImportError with helpful message if torch/transformers missing.
        """
        if self._pipeline_fn is not None:
            return self._pipeline_fn

        try:
            import torch  # noqa: PLC0415
            from transformers import pipeline  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "FinBERTClassifier requires torch and transformers. "
                "Install with: pip install torch transformers"
            ) from exc

        device = 0 if torch.cuda.is_available() else -1
        logger.info(
            "Loading FinBERT model %s on %s",
            MODEL_ID,
            "GPU" if device == 0 else "CPU",
        )

        self._pipeline_fn = pipeline(
            "text-classification",
            model=MODEL_ID,
            top_k=None,           # return all 3 label scores
            device=device,
            truncation=True,
            max_length=self._max_length,
        )
        return self._pipeline_fn

    @staticmethod
    def _parse_output(
        post: SocialPost,
        label_scores: list[dict[str, Any]],
        latency_ms: float,
    ) -> SentimentResult:
        """
        Convert HuggingFace pipeline output to SentimentResult.

        Expected input per post:
            [{"label": "positive", "score": 0.9},
             {"label": "neutral",  "score": 0.08},
             {"label": "negative", "score": 0.02}]
        Order is not guaranteed — look up by label name.
        """
        scores: dict[str, float] = {
            item["label"].lower(): float(item["score"])
            for item in label_scores
        }
        positive = scores.get("positive", 0.0)
        negative = scores.get("negative", 0.0)
        neutral = scores.get("neutral", 0.0)

        return SentimentResult(
            post_id=post.id,
            ticker=post.ticker,
            positive=positive,
            negative=negative,
            neutral=neutral,
            score=positive - negative,
            model="finbert",
            latency_ms=round(latency_ms, 3),
        )
