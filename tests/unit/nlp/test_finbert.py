"""Unit tests for FinBERTClassifier.

All tests use an injected fake pipeline — no real torch/transformers
download required in the test environment.
"""
from __future__ import annotations

from typing import Any

import pytest

from social_trading.core.models import SentimentResult, SocialPost
from social_trading.nlp.classifiers.finbert import FinBERTClassifier

# ── Fake pipeline ─────────────────────────────────────────────────────────────

def make_fake_pipeline(label: str = "positive", score: float = 0.85) -> Any:
    """
    Returns a callable mimicking HuggingFace text-classification pipeline
    with top_k=None.  Returns list[list[dict]] for a batch of texts.
    """
    other_score = round((1.0 - score) / 2, 4)
    labels = ["positive", "negative", "neutral"]
    scores = {
        "positive": score if label == "positive" else other_score,
        "negative": score if label == "negative" else other_score,
        "neutral":  score if label == "neutral"  else other_score,
    }

    def fake_fn(texts: list[str], truncation: bool = True, max_length: int = 512) -> list:
        return [
            [{"label": lbl, "score": scores[lbl]} for lbl in labels]
            for _ in texts
        ]

    return fake_fn


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def positive_clf() -> FinBERTClassifier:
    return FinBERTClassifier(pipeline_fn=make_fake_pipeline("positive", 0.9))


@pytest.fixture
def negative_clf() -> FinBERTClassifier:
    return FinBERTClassifier(pipeline_fn=make_fake_pipeline("negative", 0.88))


def make_post(text: str = "test text", ticker: str = "TSLA") -> SocialPost:
    return SocialPost(
        id="fb1",
        source="twitter",
        ticker=ticker,
        text=text,
        author_id="user1",
    )


# ── Single classify ───────────────────────────────────────────────────────────

async def test_classify_positive(positive_clf: FinBERTClassifier) -> None:
    result = await positive_clf.classify(make_post("$TSLA beats earnings"))
    assert isinstance(result, SentimentResult)
    assert result.positive > result.negative
    assert result.score > 0
    assert result.model == "finbert"


async def test_classify_negative(negative_clf: FinBERTClassifier) -> None:
    result = await negative_clf.classify(make_post("$TSLA misses earnings badly"))
    assert result.negative > result.positive
    assert result.score < 0
    assert result.model == "finbert"


async def test_classify_post_id_preserved(positive_clf: FinBERTClassifier) -> None:
    post = make_post()
    post_with_id = post.model_copy(update={"id": "custom-id-123"})
    result = await positive_clf.classify(post_with_id)
    assert result.post_id == "custom-id-123"


async def test_classify_ticker_preserved(positive_clf: FinBERTClassifier) -> None:
    result = await positive_clf.classify(make_post(ticker="NVDA"))
    assert result.ticker == "NVDA"


async def test_score_equals_positive_minus_negative(positive_clf: FinBERTClassifier) -> None:
    result = await positive_clf.classify(make_post())
    assert abs(result.score - (result.positive - result.negative)) < 1e-5


# ── Batch classify ────────────────────────────────────────────────────────────

async def test_classify_batch_count(positive_clf: FinBERTClassifier) -> None:
    posts = [make_post(f"text {i}", "AAPL") for i in range(5)]
    results = await positive_clf.classify_batch(posts)
    assert len(results) == 5


async def test_classify_batch_empty(positive_clf: FinBERTClassifier) -> None:
    results = await positive_clf.classify_batch([])
    assert results == []


async def test_classify_batch_respects_batch_size() -> None:
    """Verify that pipeline is called in chunks when batch_size is small."""
    calls: list[int] = []

    def counting_pipeline(texts: list[str], **_kwargs) -> list:
        calls.append(len(texts))
        return [
            [{"label": "positive", "score": 0.8},
             {"label": "neutral", "score": 0.1},
             {"label": "negative", "score": 0.1}]
            for _ in texts
        ]

    clf = FinBERTClassifier(pipeline_fn=counting_pipeline, batch_size=3)
    posts = [make_post(f"post {i}") for i in range(7)]
    results = await clf.classify_batch(posts)

    assert len(results) == 7
    # Should have been called at least twice with batch_size=3
    assert len(calls) >= 2
    assert all(n <= 3 for n in calls)


# ── Latency tracking ─────────────────────────────────────────────────────────

async def test_latency_is_non_negative(positive_clf: FinBERTClassifier) -> None:
    result = await positive_clf.classify(make_post())
    assert result.latency_ms >= 0.0


# ── Label parsing ─────────────────────────────────────────────────────────────

async def test_label_order_insensitive() -> None:
    """Pipeline may return labels in any order — parser must look up by name."""
    def shuffled_pipeline(texts: list[str], **_kwargs) -> list:
        # Return neutral first (unusual order)
        return [
            [{"label": "neutral", "score": 0.1},
             {"label": "positive", "score": 0.8},
             {"label": "negative", "score": 0.1}]
            for _ in texts
        ]

    clf = FinBERTClassifier(pipeline_fn=shuffled_pipeline)
    result = await clf.classify(make_post())
    assert result.positive == pytest.approx(0.8)
    assert result.neutral == pytest.approx(0.1)
    assert result.negative == pytest.approx(0.1)
