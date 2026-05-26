"""Unit tests for NLPPipeline."""
from __future__ import annotations

from typing import Any

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import SentimentResult, SocialPost
from social_trading.nlp.filters.bot_filter import BotFilter
from social_trading.nlp.filters.ticker_extractor import TickerExtractor
from social_trading.nlp.pipeline import NLPPipeline

# ── Fake helpers ─────────────────────────────────────────────────────────────

class FakeVader:
    """VADER that always returns a fixed score."""

    model_name = "vader"

    def __init__(self, score: float = 0.6) -> None:
        self._score = score

    async def classify(self, post: SocialPost) -> SentimentResult:
        pos = max(0.0, self._score)
        neg = max(0.0, -self._score)
        neu = 1.0 - pos - neg
        return SentimentResult(
            post_id=post.id, ticker=post.ticker,
            positive=pos, negative=neg, neutral=neu,
            score=self._score, model="fake_vader", latency_ms=0.1,
        )

    async def classify_batch(self, posts: list[SocialPost]) -> list[SentimentResult]:
        return [await self.classify(p) for p in posts]


class FakeFinBERT:
    """FinBERT that always returns a fixed positive result."""

    model_name = "finbert"

    def __init__(self, score: float = 0.7) -> None:
        self._score = score

    async def classify(self, post: SocialPost) -> SentimentResult:
        return SentimentResult(
            post_id=post.id, ticker=post.ticker,
            positive=0.8, negative=0.1, neutral=0.1,
            score=self._score, model="fake_finbert", latency_ms=1.0,
        )

    async def classify_batch(
        self, posts: list[SocialPost], batch_size: int = 16
    ) -> list[SentimentResult]:
        return [await self.classify(p) for p in posts]


def make_cfg(**kwargs: Any) -> SystemConfig:
    defaults = dict(
        vader_neutral_threshold=0.05,
        bot_min_account_age_days=30,
        bot_max_velocity_per_hour=50,
        bot_min_follower_following_ratio=0.1,
        finbert_batch_size=16,
    )
    defaults.update(kwargs)
    return SystemConfig(**defaults)


def make_pipeline(
    vader_score: float = 0.6,
    finbert_score: float = 0.7,
    vader_threshold: float = 0.05,
) -> NLPPipeline:
    cfg = make_cfg(vader_neutral_threshold=vader_threshold)
    return NLPPipeline(
        bot_filter=BotFilter(cfg),
        ticker_extractor=TickerExtractor(use_spacy=False),
        prefilter=FakeVader(vader_score),
        classifier=FakeFinBERT(finbert_score),
        cfg=cfg,
    )


def make_post(**kwargs: Any) -> SocialPost:
    defaults = dict(
        id="p1",
        source="twitter",
        ticker="AAPL",
        text="$AAPL is going up!",
        author_id="user1",
        author_followers=1000,
        author_following=100,
        author_account_age_days=365,
        post_count_30d=30,
    )
    defaults.update(kwargs)
    return SocialPost(**defaults)


UNIVERSE: set[str] = {"AAPL", "TSLA", "AMD", "NVDA"}


# ── Happy path ────────────────────────────────────────────────────────────────

async def test_normal_post_returns_sentiment_result() -> None:
    pipeline = make_pipeline()
    result = await pipeline.process(make_post(), UNIVERSE)
    assert isinstance(result, SentimentResult)
    assert result.model == "fake_finbert"


async def test_result_ticker_matches_post(  ) -> None:
    pipeline = make_pipeline()
    result = await pipeline.process(make_post(ticker="TSLA"), UNIVERSE)
    assert result is not None
    assert result.ticker == "TSLA"


# ── Bot filter ────────────────────────────────────────────────────────────────

async def test_bot_post_returns_none() -> None:
    pipeline = make_pipeline()
    bot_post = make_post(author_account_age_days=5)  # too new
    result = await pipeline.process(bot_post, UNIVERSE)
    assert result is None


# ── VADER neutral filter ──────────────────────────────────────────────────────

async def test_neutral_vader_score_returns_none() -> None:
    """Posts with |score| < threshold should be dropped."""
    pipeline = make_pipeline(vader_score=0.02, vader_threshold=0.05)
    result = await pipeline.process(make_post(), UNIVERSE)
    assert result is None


async def test_barely_above_threshold_passes() -> None:
    pipeline = make_pipeline(vader_score=0.06, vader_threshold=0.05)
    result = await pipeline.process(make_post(), UNIVERSE)
    assert result is not None


async def test_negative_vader_passes_if_strong_enough() -> None:
    pipeline = make_pipeline(vader_score=-0.5, vader_threshold=0.05)
    result = await pipeline.process(make_post(), UNIVERSE)
    assert result is not None


# ── StockTwits shortcut ───────────────────────────────────────────────────────

async def test_stocktwits_bullish_label_skips_classifiers() -> None:
    """Bullish label should produce result without calling VADER or FinBERT."""
    pipeline = make_pipeline(vader_score=0.0)  # VADER would filter if called
    post = make_post(source="stocktwits", raw={"sentiment_label": "Bullish"})
    result = await pipeline.process(post, UNIVERSE)
    assert result is not None
    assert result.model == "stocktwits_native"
    assert result.score > 0


async def test_stocktwits_bearish_label() -> None:
    pipeline = make_pipeline()
    post = make_post(source="stocktwits", raw={"sentiment_label": "Bearish"})
    result = await pipeline.process(post, UNIVERSE)
    assert result is not None
    assert result.model == "stocktwits_native"
    assert result.score < 0


async def test_stocktwits_no_label_uses_pipeline() -> None:
    """Post without label falls through to full pipeline."""
    pipeline = make_pipeline(vader_score=0.6)
    post = make_post(source="stocktwits", raw={"sentiment_label": ""})
    result = await pipeline.process(post, UNIVERSE)
    assert result is not None
    assert result.model == "fake_finbert"


# ── Batch processing ─────────────────────────────────────────────────────────

async def test_process_batch_filters_bots() -> None:
    pipeline = make_pipeline()
    posts = [
        make_post(id="good1"),
        make_post(id="bot1", author_account_age_days=3),   # bot
        make_post(id="good2"),
    ]
    results = await pipeline.process_batch(posts, UNIVERSE)
    result_ids = [r.post_id for r in results]
    assert "good1" in result_ids
    assert "good2" in result_ids
    assert "bot1" not in result_ids


async def test_process_batch_filters_neutral() -> None:
    pipeline = make_pipeline(vader_score=0.02, vader_threshold=0.05)
    posts = [make_post(id=f"p{i}") for i in range(5)]
    results = await pipeline.process_batch(posts, UNIVERSE)
    # All posts have vader score 0.02 < threshold 0.05 → all filtered
    assert len(results) == 0


async def test_process_batch_mixes_stocktwits_and_finbert() -> None:
    pipeline = make_pipeline(vader_score=0.5)
    posts = [
        make_post(id="st1", source="stocktwits", raw={"sentiment_label": "Bullish"}),
        make_post(id="tw1", source="twitter"),
        make_post(id="st2", source="stocktwits", raw={"sentiment_label": "Bearish"}),
    ]
    results = await pipeline.process_batch(posts, UNIVERSE)
    assert len(results) == 3
    models = {r.post_id: r.model for r in results}
    assert models["st1"] == "stocktwits_native"
    assert models["tw1"] == "fake_finbert"
    assert models["st2"] == "stocktwits_native"


async def test_process_batch_empty() -> None:
    pipeline = make_pipeline()
    results = await pipeline.process_batch([], UNIVERSE)
    assert results == []


# ── Config reload ─────────────────────────────────────────────────────────────

async def test_update_cfg_raises_threshold() -> None:
    """After raising threshold, previously passing posts should be filtered."""
    pipeline = make_pipeline(vader_score=0.1, vader_threshold=0.05)
    post = make_post()

    result_before = await pipeline.process(post, UNIVERSE)
    assert result_before is not None  # passes with low threshold

    new_cfg = make_cfg(vader_neutral_threshold=0.5)
    pipeline.update_cfg(new_cfg)

    result_after = await pipeline.process(post, UNIVERSE)
    assert result_after is None  # fails with high threshold
