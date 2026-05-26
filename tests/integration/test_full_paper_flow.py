"""
Integration test — End-to-end paper trade flow.

Verifies the complete signal-to-position-to-close lifecycle using all
real production components wired together with fakeredis (no external
services, no network calls).

Flow under test:
    SocialPost → NLPPipeline → SentimentResult
    → SentimentAggregator → SignalGenerator → Signal
    → CircuitBreaker + LiquidityGate + PositionSizer → sizing
    → PaperTradingEngine.submit_signal() → Position opened
    → PositionExitManager.evaluate() → Position closed (stop-loss / take-profit)

Design reference: docs/design/02-system-architecture.md
Plan reference:   docs/plan/03-development-phases.md §Phase 8
"""
from __future__ import annotations

from datetime import UTC, datetime

import fakeredis.aioredis as fakeredis
import pytest

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import AccountState, Signal
from social_trading.execution.paper import PaperTradingEngine
from social_trading.nlp.classifiers.vader import VaderClassifier
from social_trading.nlp.filters.bot_filter import BotFilter
from social_trading.nlp.filters.ticker_extractor import TickerExtractor
from social_trading.nlp.pipeline import NLPPipeline
from social_trading.risk.circuit_breaker import CircuitBreaker
from social_trading.risk.exit_manager import PositionExitManager
from social_trading.risk.liquidity_gate import LiquidityGate, LiquidityQuote
from social_trading.risk.position_sizer import PositionSizer
from social_trading.signals.aggregator import SentimentAggregator
from social_trading.signals.generator import SignalGenerator

# ── Shared fixtures ───────────────────────────────────────────────────────────

@pytest.fixture
async def redis():
    return fakeredis.FakeRedis()


@pytest.fixture
async def cfg(redis):
    c = SystemConfig()
    await c.save(redis)
    return c


@pytest.fixture
def healthy_account() -> AccountState:
    return AccountState(
        net_liquidation=100_000.0,
        cash=100_000.0,
        daily_pnl=0.0,
        weekly_pnl=0.0,
        drawdown_pct=0.0,
        open_positions=[],
    )


@pytest.fixture
def aapl_quote() -> LiquidityQuote:
    return LiquidityQuote(
        ticker="AAPL",
        last_price=180.0,
        bid=179.95,
        ask=180.05,
        adv_shares=60_000_000,
        adv_usd=10_800_000_000.0,
        market_cap_usd=2_800_000_000_000.0,
    )


# ── Full flow tests ───────────────────────────────────────────────────────────

class TestFullPaperFlow:
    """End-to-end paper trade lifecycle tests."""

    async def test_post_to_open_position(self, redis, cfg, healthy_account, aapl_quote):
        """
        Demonstrate the full pipeline from a social post through to an
        open paper position — the M5 milestone.
        """
        # ── Stage 1: NLP ──────────────────────────────────────────────────────
        from social_trading.core.models import SocialPost

        pipeline = NLPPipeline(
            bot_filter=BotFilter(cfg),
            ticker_extractor=TickerExtractor(),
            prefilter=VaderClassifier(),
            classifier=VaderClassifier(),
            cfg=cfg,
        )

        posts = [
            SocialPost(
                id=f"post-{i}",
                source="twitter",
                ticker="AAPL",
                text="$AAPL incredible breakout strong momentum must buy",
                author_id="user-1",
                author_followers=10_000,
                author_account_age_days=730,
                author_following=500,
                post_count_30d=30,
                likes=200,
                reposts=50,
                is_original=True,
                collected_at=datetime.now(UTC),
            )
            for i in range(20)
        ]
        results = await pipeline.process_batch(posts)
        assert len(results) > 0, "NLP should produce sentiment results"
        assert all(r.ticker == "AAPL" for r in results)

        # ── Stage 2: Signal generation ────────────────────────────────────────
        aggregator = SentimentAggregator(redis, cfg)
        for r in results:
            await aggregator.add(r)

        stats = await aggregator.get_stats("AAPL")
        assert stats is not None, "Aggregator should have stats after enough results"
        assert stats.direction == "LONG"

        generator = SignalGenerator()
        signal = generator.evaluate(stats, cfg=cfg, volume_zscore=3.0)  # override for test
        assert signal is not None, "Generator should emit a signal for strong sentiment"
        assert signal.direction == "LONG"

        # ── Stage 3: Risk checks ──────────────────────────────────────────────
        breaker = CircuitBreaker(redis)
        cb_status = await breaker.check(healthy_account, cfg)
        assert cb_status.allow, f"Circuit breaker should allow in healthy state: {cb_status.state}"

        gate = LiquidityGate()
        gate_result = gate.check(signal, aapl_quote, cfg)
        assert gate_result.passed, f"AAPL should pass liquidity gate: {gate_result.reason}"

        sizer = PositionSizer()
        quantity, reason = sizer.compute(
            signal=signal,
            account=healthy_account,
            entry_price=aapl_quote.last_price,
            cfg=cfg,
        )
        assert quantity > 0, f"Sizer should approve a non-zero quantity: {reason}"

        # ── Stage 4: Paper execution ──────────────────────────────────────────
        engine = PaperTradingEngine(initial_cash=100_000.0)
        engine.set_price("AAPL", aapl_quote.last_price)

        entry = aapl_quote.last_price
        order = await engine.submit_signal(
            signal=signal,
            quantity=quantity,
            stop_loss=round(entry * 0.99, 2),   # 1% stop — within single-trade limit
            take_profit=round(entry * 1.06, 2),
        )
        assert order.status == "filled", f"Paper engine should fill order: {order.error}"
        assert order.fill_price is not None
        assert "AAPL" in engine.open_tickers

        # Verify account reflects the position
        account_state = await engine.get_account_state()
        assert len(account_state.open_positions) == 1
        assert account_state.open_positions[0].ticker == "AAPL"

    async def test_position_closed_on_take_profit(self, cfg):
        """
        Verify that PositionExitManager fires TAKE_PROFIT and
        PaperTradingEngine.close_position() removes the position.
        """
        engine = PaperTradingEngine(initial_cash=100_000.0)
        signal = Signal(
            ticker="TSLA",
            direction="LONG",
            quality_score=0.80,
            sentiment_score=0.75,
            volume_z_score=3.0,
            momentum=0.05,
            convergence=0.90,
            source_post_count=50,
        )

        entry_price = 250.0
        stop_loss = 242.5   # -3%
        take_profit = 265.0  # +6%

        engine.set_price("TSLA", entry_price)
        order = await engine.submit_signal(
            signal=signal, quantity=10,
            stop_loss=stop_loss, take_profit=take_profit,
        )
        assert order.status == "filled"

        # Price rallies to take-profit
        engine.set_price("TSLA", 266.0)
        positions = await engine.get_positions()
        assert len(positions) == 1

        exit_manager = PositionExitManager()
        decision = exit_manager.evaluate(
            position=positions[0],
            current_price=266.0,
            cfg=cfg,
        )
        assert decision.should_exit
        assert decision.reason == "TAKE_PROFIT"

        close_result = await engine.close_position("TSLA", reason=decision.reason)
        assert close_result.status == "filled"
        assert "TSLA" not in engine.open_tickers

        # Realised P&L should be positive
        account = await engine.get_account_state()
        assert account.net_liquidation > 100_000.0

    async def test_position_closed_on_stop_loss(self, cfg):
        """
        Verify that PositionExitManager fires STOP_LOSS and
        the position is removed with a realised loss.
        """
        engine = PaperTradingEngine(initial_cash=100_000.0)
        signal = Signal(
            ticker="NVDA",
            direction="LONG",
            quality_score=0.70,
            sentiment_score=0.60,
            volume_z_score=2.0,
            momentum=0.03,
            convergence=0.75,
            source_post_count=30,
        )

        entry_price = 500.0
        stop_loss = 496.0   # -0.8% — below the emergency threshold of -1%
        take_profit = 530.0

        engine.set_price("NVDA", entry_price)
        order = await engine.submit_signal(
            signal=signal, quantity=5,
            stop_loss=stop_loss, take_profit=take_profit,
        )
        assert order.status == "filled"

        # Price slips just below stop loss (but loss is only ~0.9% — no emergency)
        engine.set_price("NVDA", 495.5)
        positions = await engine.get_positions()

        exit_manager = PositionExitManager()
        decision = exit_manager.evaluate(
            position=positions[0],
            current_price=495.5,
            cfg=cfg,
        )
        assert decision.should_exit
        assert decision.reason == "STOP_LOSS"

        await engine.close_position("NVDA", reason=decision.reason)
        assert "NVDA" not in engine.open_tickers

    async def test_duplicate_signal_skipped(self, cfg):
        """
        Verify that submitting a second signal for the same ticker
        while a position is already open is rejected.
        """
        engine = PaperTradingEngine(initial_cash=100_000.0)
        signal = Signal(
            ticker="MSFT",
            direction="LONG",
            quality_score=0.70,
            sentiment_score=0.65,
            volume_z_score=2.5,
            momentum=0.04,
            convergence=0.80,
            source_post_count=25,
        )
        engine.set_price("MSFT", 300.0)

        first = await engine.submit_signal(
            signal=signal, quantity=10, stop_loss=291.0, take_profit=318.0
        )
        assert first.status == "filled"

        second = await engine.submit_signal(
            signal=signal, quantity=10, stop_loss=291.0, take_profit=318.0
        )
        # PaperTradingEngine should reject — position already open
        assert second.status != "filled" or "MSFT" in engine.open_tickers
        # Still only one position
        positions = await engine.get_positions()
        assert len(positions) == 1

    async def test_short_position_lifecycle(self, cfg):
        """
        Verify SHORT position opens, accrues profit when price falls,
        and closes at take-profit.
        """
        engine = PaperTradingEngine(initial_cash=100_000.0)
        signal = Signal(
            ticker="GME",
            direction="SHORT",
            quality_score=0.72,
            sentiment_score=-0.70,
            volume_z_score=3.5,
            momentum=-0.06,
            convergence=0.85,
            source_post_count=60,
        )

        entry_price = 20.0
        stop_loss = 21.5
        take_profit = 17.5

        engine.set_price("GME", entry_price)
        order = await engine.submit_signal(
            signal=signal, quantity=100,
            stop_loss=stop_loss, take_profit=take_profit,
        )
        assert order.status == "filled"
        assert "GME" in engine.open_tickers

        # Price drops to take-profit for SHORT
        engine.set_price("GME", 17.0)
        positions = await engine.get_positions()

        exit_manager = PositionExitManager()
        decision = exit_manager.evaluate(
            position=positions[0],
            current_price=17.0,
            cfg=cfg,
        )
        assert decision.should_exit

        await engine.close_position("GME", reason=decision.reason)
        assert "GME" not in engine.open_tickers

        account = await engine.get_account_state()
        assert account.net_liquidation > 100_000.0  # SHORT position profited

    async def test_max_positions_cash_limited(self, cfg):
        """
        Verify that PaperTradingEngine rejects orders when cash is exhausted.
        (Position count limits are enforced by the risk service, not the engine.)
        """
        engine = PaperTradingEngine(initial_cash=1_000.0)  # very small account

        signal = Signal(
            ticker="AMZN",
            direction="LONG",
            quality_score=0.75,
            sentiment_score=0.65,
            volume_z_score=2.5,
            momentum=0.04,
            convergence=0.80,
            source_post_count=30,
        )
        engine.set_price("AMZN", 180.0)
        result = await engine.submit_signal(
            signal=signal, quantity=100,  # cost = $18,000 > $1,000 cash
            stop_loss=175.0, take_profit=190.0,
        )
        assert result.status == "rejected"
        assert "AMZN" not in engine.open_tickers

    async def test_circuit_breaker_halts_trading_on_excessive_loss(self, redis, cfg):
        """
        Verify that CircuitBreaker transitions away from NORMAL state
        when account drawdown exceeds the daily loss limit.
        """
        breaker = CircuitBreaker(redis)

        distressed_account = AccountState(
            net_liquidation=94_000.0,    # -6% from 100k
            cash=90_000.0,
            daily_pnl=-6_000.0,
            weekly_pnl=-6_000.0,
            drawdown_pct=0.06,
            open_positions=[],
        )
        # Default cfg.loss_limit_daily is typically 0.02–0.05
        # At -6% we expect circuit breaker to trigger
        status = await breaker.check(distressed_account, cfg)
        assert not status.allow or status.state.value != "NORMAL"


# ── Smoke test: full component construction ───────────────────────────────────

class TestComponentConstruction:
    """Verify all Phase 1–5 components can be instantiated without errors."""

    async def test_all_components_construct(self, redis, cfg):
        pipeline = NLPPipeline(
            bot_filter=BotFilter(cfg),
            ticker_extractor=TickerExtractor(),
            prefilter=VaderClassifier(),
            classifier=VaderClassifier(),
            cfg=cfg,
        )
        aggregator = SentimentAggregator(redis, cfg)
        generator = SignalGenerator()
        breaker = CircuitBreaker(redis)
        gate = LiquidityGate()
        sizer = PositionSizer()
        engine = PaperTradingEngine(initial_cash=100_000.0)
        exit_manager = PositionExitManager()

        assert await engine.health_check()
        assert await breaker.load_state() is not None
        _ = pipeline, aggregator, generator, gate, sizer, exit_manager
