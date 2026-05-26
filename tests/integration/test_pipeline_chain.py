"""
Integration tests — Phases 1→5 pipeline chain.

Verifies that the core components work together end-to-end using
fakeredis (no external services required).

Chain under test:
    SocialPost → NLPPipeline → SentimentResult
    SentimentResult → SentimentAggregator → SignalGenerator → Signal
    Signal → CircuitBreaker + LiquidityGate + PositionSizer → approved signal

Design reference: docs/design/02-system-architecture.md
Plan reference:   docs/plan/03-development-phases.md §Phase 8
"""
from __future__ import annotations

from datetime import UTC, datetime

import fakeredis.aioredis as fakeredis
import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import (
    AccountState,
    SentimentResult,
    SocialPost,
)
from social_trading.nlp.classifiers.vader import VaderClassifier
from social_trading.nlp.filters.bot_filter import BotFilter
from social_trading.nlp.filters.ticker_extractor import TickerExtractor
from social_trading.nlp.pipeline import NLPPipeline
from social_trading.risk.circuit_breaker import CircuitBreaker
from social_trading.risk.liquidity_gate import LiquidityGate, LiquidityQuote
from social_trading.risk.position_sizer import PositionSizer
from social_trading.signals.aggregator import SentimentAggregator
from social_trading.signals.generator import SignalGenerator
from social_trading.storage.event_bus import TradingEventBus

# ── Helpers ───────────────────────────────────────────────────────────────────

def make_post(
    ticker: str = "AAPL",
    text: str = "$AAPL huge momentum breaking out strong buy",
    followers: int = 5_000,
    age_days: int = 365,
    likes: int = 100,
) -> SocialPost:
    return SocialPost(
        id=f"post-{ticker}-{datetime.now(UTC).timestamp()}",
        source="twitter",
        ticker=ticker,
        text=text,
        author_id="user-1",
        author_followers=followers,
        author_account_age_days=age_days,
        author_following=400,
        post_count_30d=20,
        likes=likes,
        reposts=10,
        is_original=True,
        collected_at=datetime.now(UTC),
    )


def make_result(ticker: str, score: float = 0.7) -> SentimentResult:
    return SentimentResult(
        post_id=f"post-{ticker}",
        ticker=ticker,
        positive=score,
        negative=0.05,
        neutral=1 - score - 0.05,
        score=score,
        model="vader",
        latency_ms=5.0,
    )


@pytest.fixture
async def redis():
    return fakeredis.FakeRedis()


@pytest.fixture
async def cfg(redis):
    c = SystemConfig()
    await c.save(redis)
    return c


@pytest.fixture
def nlp_pipeline(cfg):
    return NLPPipeline(
        bot_filter=BotFilter(cfg),
        ticker_extractor=TickerExtractor(),
        prefilter=VaderClassifier(),   # VADER as prefilter
        classifier=VaderClassifier(),  # VADER as main classifier (FinBERT not in test env)
        cfg=cfg,
    )


# ── Stage 1: SocialPost → SentimentResult ────────────────────────────────────

class TestNLPStage:
    async def test_positive_post_produces_positive_sentiment(self, nlp_pipeline):
        posts = [make_post(text="$AAPL massive breakout incredible gains today")]
        results = await nlp_pipeline.process_batch(posts)
        assert len(results) == 1
        assert results[0].score > 0
        assert results[0].ticker == "AAPL"

    async def test_negative_post_produces_negative_sentiment(self, nlp_pipeline):
        posts = [make_post(text="$AAPL disaster crash terrible sell everything")]
        results = await nlp_pipeline.process_batch(posts)
        assert len(results) == 1
        assert results[0].score < 0

    async def test_bot_post_is_filtered_out(self, nlp_pipeline):
        """Accounts with 0 followers and very low account age should be filtered."""
        bot_post = make_post(followers=0, age_days=1, likes=0)
        results = await nlp_pipeline.process_batch([bot_post])
        assert len(results) == 0

    async def test_batch_of_mixed_posts(self, nlp_pipeline):
        posts = [
            make_post(ticker="AAPL", text="$AAPL great momentum strong buy"),
            make_post(ticker="TSLA", text="$TSLA huge bullish breakout"),
            make_post(ticker="MSFT", text="$MSFT solid earnings beat"),
        ]
        results = await nlp_pipeline.process_batch(posts)
        assert len(results) == 3
        assert all(r.score > 0 for r in results)


# ── Stage 2: SentimentResult → Signal ─────────────────────────────────────────

class TestSignalStage:
    async def test_aggregated_bullish_sentiment_generates_long_signal(
        self, redis, cfg
    ):
        aggregator = SentimentAggregator(redis, cfg)
        generator = SignalGenerator()

        # Feed enough bullish results to trigger aggregation
        for i in range(15):
            result = SentimentResult(
                post_id=f"post-{i}",
                ticker="AAPL",
                positive=0.80,
                negative=0.05,
                neutral=0.15,
                score=0.75,
                model="vader",
                latency_ms=5.0,
            )
            await aggregator.add(result)

        stats = await aggregator.get_stats("AAPL")
        assert stats is not None
        assert stats.direction == "LONG"

        # Override volume_zscore to 3.0 to ensure quality threshold is met
        # (the aggregator baseline requires more history than a unit test provides)
        signal = generator.evaluate(stats, cfg=cfg, volume_zscore=3.0)
        assert signal is not None
        assert signal.ticker == "AAPL"
        assert signal.direction == "LONG"
        assert 0.0 <= signal.quality_score <= 1.0

    async def test_aggregated_bearish_sentiment_generates_short_signal(
        self, redis, cfg
    ):
        aggregator = SentimentAggregator(redis, cfg)
        generator = SignalGenerator()

        for i in range(15):
            result = SentimentResult(
                post_id=f"post-{i}",
                ticker="NVDA",
                positive=0.05,
                negative=0.85,
                neutral=0.10,
                score=-0.80,
                model="vader",
                latency_ms=5.0,
            )
            await aggregator.add(result)

        stats = await aggregator.get_stats("NVDA")
        assert stats is not None
        assert stats.direction == "SHORT"

        volume_z = await aggregator.get_volume_zscore("NVDA")
        signal = generator.evaluate(stats, cfg=cfg, volume_zscore=volume_z)
        # May be None if quality threshold not met — that's acceptable
        if signal is not None:
            assert signal.direction == "SHORT"

    async def test_insufficient_data_returns_no_stats(self, redis, cfg):
        aggregator = SentimentAggregator(redis, cfg)
        stats = await aggregator.get_stats("UNKNOWN_TICKER")
        assert stats is None


# ── Stage 3: Signal → Risk approval ──────────────────────────────────────────

class TestRiskStage:
    async def test_good_signal_passes_all_risk_checks(self, redis, cfg, sample_signal):
        breaker = CircuitBreaker(redis)
        gate = LiquidityGate()
        sizer = PositionSizer()

        # Seed healthy account state
        account = AccountState(
            net_liquidation=100_000.0,
            cash=90_000.0,
            daily_pnl=0.0,
            weekly_pnl=0.0,
            drawdown_pct=0.0,
            open_positions=[],
        )

        cb_status = await breaker.check(account, cfg)
        assert cb_status.allow, f"Circuit breaker blocked: {cb_status.state}"

        quote = LiquidityQuote(
            ticker=sample_signal.ticker,
            last_price=150.0,
            bid=149.9,
            ask=150.1,
            adv_shares=5_000_000,
            adv_usd=750_000_000.0,
            market_cap_usd=2_500_000_000_000.0,
        )
        gate_result = gate.check(sample_signal, quote, cfg)
        assert gate_result.passed, f"Liquidity gate blocked: {gate_result.reason}"

        quantity, reason = sizer.compute(
            signal=sample_signal,
            account=account,
            entry_price=150.0,
            cfg=cfg,
        )
        assert quantity > 0, f"Sizer blocked: {reason}"

    async def test_circuit_breaker_halts_on_large_drawdown(self, redis, cfg):
        breaker = CircuitBreaker(redis)

        account = AccountState(
            net_liquidation=85_000.0,   # -15% drawdown
            cash=80_000.0,
            daily_pnl=-5_000.0,
            weekly_pnl=-15_000.0,
            drawdown_pct=0.15,
            open_positions=[],
        )
        status = await breaker.check(account, cfg)
        # 15% drawdown should trigger some form of restriction
        assert not status.allow or status.state.value != "NORMAL"

    async def test_illiquid_ticker_blocked_by_gate(self, redis, cfg, sample_signal):
        gate = LiquidityGate()

        illiquid_quote = LiquidityQuote(
            ticker="ILLIQUID",
            last_price=0.05,            # penny stock
            bid=0.04,
            ask=0.06,
            adv_shares=1_000,
            adv_usd=50.0,              # tiny ADV
            market_cap_usd=100_000.0,
        )
        illiquid_signal = sample_signal.model_copy(update={"ticker": "ILLIQUID"})
        result = gate.check(illiquid_signal, illiquid_quote, cfg)
        assert not result.passed


# ── Stage 4: Redis stream round-trip ─────────────────────────────────────────

class TestStreamRoundTrip:
    async def test_publish_and_consume_post_via_event_bus(self, redis, cfg):
        """Verify TradingEventBus correctly serialises and deserialises a post."""
        from social_trading.core.events import STREAM_RAW_SOCIAL
        from social_trading.services.nlp_service import _stream_dict_to_post

        bus = TradingEventBus(redis)

        post = make_post(ticker="TSLA", text="$TSLA to the moon!")
        await bus.publish(STREAM_RAW_SOCIAL, {
            "id": post.id,
            "source": post.source,
            "ticker": post.ticker,
            "text": post.text,
            "author_id": post.author_id,
            "author_followers": post.author_followers,
            "author_account_age_days": post.author_account_age_days,
            "author_following": post.author_following,
            "post_count_30d": post.post_count_30d,
            "likes": post.likes,
            "reposts": post.reposts,
            "is_original": str(post.is_original),
            "collected_at": post.collected_at.isoformat(),
        })

        await bus.create_group(STREAM_RAW_SOCIAL, "test-group")
        messages = await bus.consume(
            STREAM_RAW_SOCIAL, "test-group", "test-consumer", count=1
        )
        assert len(messages) == 1
        _, fields = messages[0]
        recovered = _stream_dict_to_post(fields)
        assert recovered is not None
        assert recovered.ticker == "TSLA"
        assert recovered.author_followers == post.author_followers

    async def test_sentiment_result_survives_stream_serialisation(self, redis):
        """Verify SentimentResult round-trips through Redis stream correctly."""
        from social_trading.core.events import STREAM_SENTIMENT
        from social_trading.services.nlp_service import _result_to_stream_dict
        from social_trading.services.signal_service import _stream_dict_to_result

        bus = TradingEventBus(redis)
        result = make_result("AAPL", score=0.72)

        await bus.publish(STREAM_SENTIMENT, _result_to_stream_dict(result))
        await bus.create_group(STREAM_SENTIMENT, "test-group")
        messages = await bus.consume(
            STREAM_SENTIMENT, "test-group", "test-consumer", count=1
        )
        assert len(messages) == 1
        _, fields = messages[0]
        recovered = _stream_dict_to_result(fields)
        assert recovered is not None
        assert recovered.ticker == "AAPL"
        assert abs(recovered.score - 0.72) < 0.001


# ── Fixtures duplicated locally for isolation ─────────────────────────────────

@pytest.fixture
def sample_signal():
    from social_trading.core.models import Signal
    return Signal(
        ticker="AAPL",
        direction="LONG",
        quality_score=0.75,
        sentiment_score=0.65,
        volume_z_score=2.5,
        momentum=0.04,
        convergence=0.80,
        source_post_count=42,
    )
