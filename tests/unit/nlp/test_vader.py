"""Unit tests for VaderClassifier."""
from __future__ import annotations

import pytest

from social_trading.core.models import SentimentResult, SocialPost
from social_trading.nlp.classifiers.vader import VaderClassifier

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def clf() -> VaderClassifier:
    """Real VADER analyzer — fast enough for unit tests (< 1ms each)."""
    return VaderClassifier()


def make_post(text: str, ticker: str = "AAPL") -> SocialPost:
    return SocialPost(
        id="p1",
        source="twitter",
        ticker=ticker,
        text=text,
        author_id="user1",
    )


# ── Classify single post ──────────────────────────────────────────────────────

async def test_classify_positive_text(clf: VaderClassifier) -> None:
    post = make_post("$AAPL is absolutely crushing it! Best quarter ever! 🚀")
    result = await clf.classify(post)
    assert isinstance(result, SentimentResult)
    assert result.score > 0.2
    assert result.positive > result.negative
    assert result.model == "vader"
    assert result.post_id == "p1"
    assert result.ticker == "AAPL"


async def test_classify_negative_text(clf: VaderClassifier) -> None:
    post = make_post("$AAPL terrible miss, disaster of a quarter, sell everything now")
    result = await clf.classify(post)
    assert result.score < -0.2
    assert result.negative > result.positive


async def test_classify_neutral_text(clf: VaderClassifier) -> None:
    post = make_post("$AAPL announced their quarterly earnings today")
    result = await clf.classify(post)
    assert abs(result.score) < 0.5  # near neutral, no strong sentiment


async def test_score_range(clf: VaderClassifier) -> None:
    """Compound score must be in [-1, 1]."""
    post = make_post("$TSLA to the moon!!!! 🚀🚀🚀🚀🚀")
    result = await clf.classify(post)
    assert -1.0 <= result.score <= 1.0


async def test_probabilities_sum_to_one(clf: VaderClassifier) -> None:
    """pos + neg + neu should approximately sum to 1.0 (VADER property)."""
    post = make_post("I am bullish on $AMD this week")
    result = await clf.classify(post)
    total = result.positive + result.negative + result.neutral
    assert abs(total - 1.0) < 0.01


async def test_latency_recorded(clf: VaderClassifier) -> None:
    post = make_post("$NVDA GPU sales beat expectations")
    result = await clf.classify(post)
    assert result.latency_ms >= 0.0


# ── Classify batch ────────────────────────────────────────────────────────────

async def test_classify_batch_returns_same_count(clf: VaderClassifier) -> None:
    posts = [
        make_post("$AAPL up!", "AAPL"),
        make_post("$TSLA down!", "TSLA"),
        make_post("$AMD flat today", "AMD"),
    ]
    results = await clf.classify_batch(posts)
    assert len(results) == 3


async def test_classify_batch_preserves_order(clf: VaderClassifier) -> None:
    posts = [
        make_post("Great news for $AAPL", "AAPL"),
        make_post("Terrible results for $TSLA", "TSLA"),
    ]
    results = await clf.classify_batch(posts)
    assert results[0].ticker == "AAPL"
    assert results[1].ticker == "TSLA"


async def test_classify_batch_empty(clf: VaderClassifier) -> None:
    results = await clf.classify_batch([])
    assert results == []


# ── Injectable analyzer ───────────────────────────────────────────────────────

async def test_injected_analyzer() -> None:
    """Verify injected analyzer is used (avoids importing vaderSentiment)."""

    class FakeAnalyzer:
        def polarity_scores(self, text: str) -> dict:
            return {"compound": 0.5, "pos": 0.6, "neg": 0.1, "neu": 0.3}

    clf = VaderClassifier(analyzer=FakeAnalyzer())
    post = make_post("doesn't matter")
    result = await clf.classify(post)
    assert result.score == 0.5
    assert result.positive == 0.6
    assert result.negative == 0.1
    assert result.neutral == 0.3
    assert result.model == "vader"
