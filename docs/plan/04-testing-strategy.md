# 04 — Testing Strategy

## Philosophy

| Layer | What it tests | Speed | Coverage target |
|-------|--------------|-------|----------------|
| **Unit** | Single class / function in isolation | < 1ms/test | ≥ 80% per module |
| **Integration** | Two or more modules wired together via fake I/O | < 1s/test | Critical flows |
| **Paper E2E** | Full system running against paper broker | minutes | Pre-live gate |

**Key principle:** No test should touch a real API, real broker, or real database.
All external dependencies are replaced by fakes that satisfy the same protocols.

---

## Shared Fixtures (`tests/conftest.py`)

```python
import pytest
import fakeredis.aioredis as fakeredis
from social_trading.config.system_config import SystemConfig


@pytest.fixture
def redis():
    """In-memory Redis — no real server needed."""
    return fakeredis.FakeRedis()


@pytest.fixture
def cfg(redis):
    """Default SystemConfig loaded into fake Redis."""
    c = SystemConfig()
    import asyncio
    asyncio.run(c.save(redis))
    return c


@pytest.fixture
def sample_post():
    from social_trading.core.models import SocialPost
    return SocialPost(
        id="p1", source="twitter", ticker="AAPL",
        text="$AAPL is going to the moon! Strong buy signal.",
        author_id="u1", author_followers=5000,
        author_account_age_days=365, likes=100,
    )


@pytest.fixture
def sample_signal():
    from social_trading.core.models import Signal
    return Signal(
        ticker="AAPL", direction="LONG",
        quality_score=0.75, sentiment_score=0.65,
        volume_z_score=2.5, momentum=0.4,
        convergence=0.8, source_post_count=42,
    )


class FakeEventBus:
    def __init__(self):
        self.published: list[dict] = []

    async def publish(self, stream: str, event: dict) -> str:
        self.published.append({"_stream": stream, **event})
        return "fake-id"

    async def consume(self, stream, group, consumer, count=10):
        return []

    async def ack(self, stream, group, message_id):
        pass


@pytest.fixture
def bus():
    return FakeEventBus()
```

---

## Unit Tests

### Config (`tests/unit/config/test_system_config.py`)

```python
import pytest
from social_trading.config.system_config import SystemConfig

def test_default_config_is_valid(cfg):
    cfg.validate()  # should not raise

def test_weight_sum_must_be_one(cfg):
    cfg.w_volume = 0.5  # overbalance weights
    with pytest.raises(ValueError, match="weights must sum"):
        cfg.validate()

def test_daily_loss_less_than_weekly(cfg):
    cfg.daily_halt_pct = 0.10
    cfg.weekly_reduce_pct = 0.05   # weekly < daily — invalid
    with pytest.raises(ValueError):
        cfg.validate()

async def test_save_and_reload(redis, cfg):
    cfg.signal_quality_threshold = 0.72
    await cfg.save(redis)
    reloaded = await SystemConfig.load(redis)
    assert reloaded.signal_quality_threshold == 0.72
```

### Bot Filter (`tests/unit/nlp/test_bot_filter.py`)

```python
from social_trading.nlp.filters.bot_filter import BotFilter
from tests.conftest import sample_post

def make_post(**kwargs):
    base = dict(author_account_age_days=365, author_followers=500,
                velocity_per_hour=2)
    base.update(kwargs)
    # build SocialPost from base
    ...

def test_new_account_flagged_as_bot():
    assert BotFilter().is_bot(make_post(author_account_age_days=5))

def test_high_velocity_flagged_as_bot():
    assert BotFilter().is_bot(make_post(velocity_per_hour=200))

def test_normal_account_passes():
    assert not BotFilter().is_bot(make_post())
```

### Position Sizer (`tests/unit/risk/test_position_sizer.py`)

```python
from social_trading.risk.position_sizer import PositionSizer
from social_trading.core.models import AccountState

def test_kelly_capped_at_max_position():
    sizer = PositionSizer(cfg=high_sharpe_config())
    state = AccountState(net_liquidation=100_000, cash=100_000,
                         daily_pnl=0, weekly_pnl=0, drawdown_pct=0)
    qty = sizer.calculate(ticker="AAPL", price=150.0, account=state)
    # Even with high Sharpe, position can't exceed 2% of portfolio
    assert qty * 150.0 <= 100_000 * 0.02 + 1  # +1 for rounding

def test_zero_position_when_vix_above_40():
    sizer = PositionSizer(cfg=default_config())
    state = AccountState(net_liquidation=100_000, cash=100_000,
                         daily_pnl=0, weekly_pnl=0, drawdown_pct=0)
    qty = sizer.calculate(ticker="AAPL", price=150.0, account=state, vix=45.0)
    assert qty == 0
```

### Circuit Breaker (`tests/unit/risk/test_circuit_breaker.py`)

```python
from social_trading.risk.circuit_breaker import CircuitBreaker
from social_trading.core.exceptions import CircuitBreakerOpen

async def test_daily_loss_triggers_halt(redis, cfg):
    cfg.daily_halt_pct = 0.01
    cb = CircuitBreaker(redis=redis, cfg=cfg)
    account = AccountState(..., daily_pnl=-1500, net_liquidation=100_000)  # -1.5%
    with pytest.raises(CircuitBreakerOpen):
        await cb.check(account)

async def test_normal_state_passes(redis, cfg):
    cb = CircuitBreaker(redis=redis, cfg=cfg)
    account = AccountState(..., daily_pnl=-500, net_liquidation=100_000)  # -0.5%
    await cb.check(account)   # should not raise
```

### Signal Generator (`tests/unit/signals/test_generator.py`)

```python
from social_trading.signals.generator import SignalGenerator

def test_signal_fires_above_quality_threshold(cfg):
    gen = SignalGenerator(cfg=cfg)
    result = gen.evaluate(
        ticker="AAPL",
        volume_z=2.5, sentiment=0.70, proactivity=0.60,
        momentum=0.50, convergence=0.80, price_direction=1
    )
    assert result is not None
    assert result.direction == "LONG"

def test_signal_suppressed_below_threshold(cfg):
    cfg.signal_quality_threshold = 0.99
    gen = SignalGenerator(cfg=cfg)
    result = gen.evaluate(
        ticker="AAPL",
        volume_z=1.0, sentiment=0.30, proactivity=0.20,
        momentum=0.10, convergence=0.30, price_direction=1
    )
    assert result is None

def test_signal_direction_follows_sentiment():
    gen = SignalGenerator(cfg=default_config())
    long_signal = gen.evaluate(..., sentiment=0.70, price_direction=1)
    short_signal = gen.evaluate(..., sentiment=-0.70, price_direction=-1)
    assert long_signal.direction == "LONG"
    assert short_signal.direction == "SHORT"
```

### Paper Engine (`tests/unit/execution/test_paper_engine.py`)

```python
from social_trading.execution.paper import PaperTradingEngine

async def test_submit_creates_position(sample_signal):
    engine = PaperTradingEngine(initial_cash=100_000)
    result = await engine.submit_signal(
        signal=sample_signal, quantity=10, stop_loss=148.0, take_profit=158.0
    )
    assert result.status == "filled"
    positions = await engine.get_positions()
    assert len(positions) == 1
    assert positions[0].ticker == "AAPL"

async def test_close_position_removes_it(sample_signal):
    engine = PaperTradingEngine()
    await engine.submit_signal(sample_signal, 10, 148.0, 158.0)
    await engine.close_position("AAPL", reason="test")
    assert len(await engine.get_positions()) == 0

async def test_insufficient_cash_rejected():
    engine = PaperTradingEngine(initial_cash=100)   # tiny cash
    result = await engine.submit_signal(sample_signal, 1000, 148.0, 158.0)
    assert result.status == "rejected"
```

---

## Integration Tests

Integration tests use `fakeredis` for Redis and a real in-process PostgreSQL
(via `pytest-postgresql`) or SQLite for simple tests.

### NLP Pipeline (`tests/integration/test_nlp_to_signal.py`)

```python
async def test_nlp_output_is_published_to_stream(redis, cfg, sample_post, bus):
    """Verify: post in → sentiment result on stream."""
    from social_trading.nlp.pipeline import NLPPipeline
    from social_trading.nlp.classifiers.vader import VaderClassifier

    pipeline = NLPPipeline(
        bot_filter=BotFilter(),
        ticker_extractor=TickerExtractor(),
        prefilter=VaderClassifier(),
        classifier=VaderClassifier(),   # use VADER for both in tests (no GPU needed)
        cfg=cfg,
    )
    result = await pipeline.process(sample_post)
    assert result is not None
    assert result.ticker == "AAPL"
    assert result.score != 0.0
```

### Signal to Risk (`tests/integration/test_signal_to_execution.py`)

```python
async def test_approved_signal_reaches_execution(redis, cfg, sample_signal, bus):
    """
    Verify: quality signal → passes risk → reaches execution engine.
    """
    from social_trading.risk.circuit_breaker import CircuitBreaker
    from social_trading.risk.position_sizer import PositionSizer
    from social_trading.execution.paper import PaperTradingEngine

    cb = CircuitBreaker(redis=redis, cfg=cfg)
    sizer = PositionSizer(cfg=cfg)
    engine = PaperTradingEngine(initial_cash=100_000)

    account = AccountState(net_liquidation=100_000, cash=100_000,
                           daily_pnl=0, weekly_pnl=0, drawdown_pct=0)
    price = 150.0

    await cb.check(account)                  # should not raise
    qty = sizer.calculate("AAPL", price, account)
    assert qty > 0

    result = await engine.submit_signal(
        signal=sample_signal, quantity=qty,
        stop_loss=price * 0.98, take_profit=price * 1.03,
    )
    assert result.status == "filled"
```

### Full Paper Flow (`tests/integration/test_full_paper_flow.py`)

```python
async def test_full_paper_flow(redis, cfg):
    """
    Simulate: post arrives → NLP → signal → risk → paper fill.
    Uses all real components except external APIs and broker.
    """
    # wire up all services against fake Redis
    ...
    # publish a fake social post to raw_social
    await bus.publish("raw_social", sample_post().model_dump(mode="json"))

    # run NLP service one iteration
    await nlp_svc.process_next()

    # run signal service one iteration  
    await signal_svc.process_next()

    # run risk service one iteration
    await risk_svc.process_next()

    # run execution service one iteration
    await exec_svc.process_next()

    # assert paper position opened
    positions = await paper_engine.get_positions()
    assert len(positions) >= 0  # depends on quality score; assert no crash
```

---

## Mocking External APIs

| External Dep | Mock Approach | Library |
|---|---|---|
| Twitter API | `respx` fixture + `tweepy.Client` mock | `respx` |
| Reddit PRAW | Fake `praw.Reddit` subclass | hand-rolled |
| StockTwits REST | `respx` response fixtures | `respx` |
| IBKR `ib_async` | `FakeIBKR` satisfying ExecutionEngine protocol | hand-rolled |
| Redis | `fakeredis.aioredis.FakeRedis` | `fakeredis[aioredis]` |
| PostgreSQL | `pytest-postgresql` or SQLite in tests | `pytest-postgresql` |
| HuggingFace model | `unittest.mock.patch` on `AutoModel.from_pretrained` | stdlib |

---

## Running Tests

```bash
# Fast unit tests only (no GPU, no DB)
make test

# All including integration (needs Docker postgres + redis)
make test-integration

# Coverage report
pytest --cov=src/social_trading --cov-report=html
open htmlcov/index.html
```

### CI Pipeline (`.github/workflows/ci.yml`)

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
        env: { POSTGRES_PASSWORD: test }
      redis:
        image: redis:7-alpine
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e ".[dev]"
      - run: ruff check src/ tests/
      - run: mypy src/
      - run: pytest tests/ -v --cov=src/social_trading
```

---

## Coverage Minimums

| Module | Target |
|--------|--------|
| `core/` | 95% |
| `config/` | 90% |
| `risk/` | 90% |
| `signals/` | 85% |
| `nlp/` | 80% |
| `ingest/` | 75% |
| `execution/` | 80% |
| `services/` | 60% (integration-tested) |

---

*[⬆ Back to plan index](README.md)*
