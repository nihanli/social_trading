## 6. Risk Management Framework

> All hardcoded values below are defaults. Every parameter is editable at runtime via the
> Streamlit Config page (§15) without restarting services. See §16 for the full parameter
> reference and `SystemConfig` class.

### 6a. Position Sizing (Volatility-Adjusted Half-Kelly)

The recommended approach combines Half-Kelly with volatility scaling from Lim et al. (2019)[^14]:

```python
from config.system_config import SystemConfig
import redis

def calculate_position_size(
    signal_score: float,       # 0.0 to 1.0
    expected_sharpe: float,    # estimated from backtest
    sigma_t: float,            # current realized volatility (rolling ATR)
    portfolio_value: float,
    entry_price: float,
    cfg: SystemConfig = None,
) -> int:
    """
    Volatility-scaled, Half-Kelly position sizing.
    Combines: Half-Kelly (Carver) + Volatility Scaling (Lim et al. 2019)
    All thresholds sourced from cfg (SystemConfig) — editable via UI.
    """
    if cfg is None:
        cfg = SystemConfig.load(redis.Redis())

    # Half-Kelly risk target = half_kelly_fraction × Expected Sharpe Ratio
    risk_target = cfg.half_kelly_fraction * expected_sharpe

    # Volatility-adjusted position size
    vol_scale = cfg.sigma_target / (sigma_t + 1e-9)

    # Signal-weighted: scale by signal quality (0.0 to 1.0)
    effective_risk_fraction = risk_target * signal_score * vol_scale

    # Cap at cfg.max_position_pct per single social media signal
    effective_risk_fraction = min(effective_risk_fraction, cfg.max_position_pct)

    position_value = portfolio_value * effective_risk_fraction
    shares = int(position_value / entry_price)

    return max(shares, 0)
```

[^14]: Lim, Zohren & Roberts (2019). "Enhancing Time-Series Momentum Strategies Using Deep Neural Networks." arXiv:1904.04912. *Journal of Financial Data Science*
[^15]: Carver, R. qoppac.blogspot.com — "Kelly versus Classical Portfolio Theory": s* = min(μ/σ, s_max); Half-Kelly = 0.5 × Sharpe Ratio

### 6b. Stop-Loss Strategy

```python
from config.system_config import SystemConfig
from datetime import datetime
import redis

class PositionExitManager:
    """Multi-trigger exit for social media momentum positions. All thresholds from SystemConfig."""

    def __init__(self, entry_price: float, entry_time: datetime,
                 entry_sentiment: float, entry_mentions: int,
                 cfg: SystemConfig = None):
        self.entry_price    = entry_price
        self.entry_time     = entry_time
        self.entry_sentiment = entry_sentiment
        self.peak_mentions  = entry_mentions
        self.high_water_mark = entry_price

        cfg = cfg or SystemConfig.load(redis.Redis())
        self.atr_multiplier            = cfg.atr_multiplier
        self.max_hold_hours            = cfg.max_hold_hours
        self.trailing_stop_pct         = cfg.trailing_stop_pct
        self.take_profit_pct           = cfg.take_profit_pct
        self.signal_reversal_threshold = cfg.signal_reversal_threshold
        self.mention_decay_threshold   = cfg.mention_decay_threshold

    def should_exit(self, current_price: float, current_atr: float,
                    current_sentiment: float, current_mentions: int,
                    current_time: datetime) -> tuple[bool, str]:

        # 1. ATR-based stop loss
        atr_stop = self.entry_price - self.atr_multiplier * current_atr
        if current_price <= atr_stop:
            return True, f"ATR_STOP (price={current_price:.2f} < stop={atr_stop:.2f})"

        # 2. Take profit
        profit_pct = (current_price - self.entry_price) / self.entry_price
        if profit_pct >= self.take_profit_pct:
            return True, f"TAKE_PROFIT (+{profit_pct:.1%})"

        # 3. Time-based stop (critical for social media alpha decay)
        hours_held = (current_time - self.entry_time).total_seconds() / 3600
        if hours_held >= self.max_hold_hours:
            return True, f"TIME_STOP ({hours_held:.0f}h ≥ {self.max_hold_hours}h)"

        # 4. Trailing stop (protect profits)
        self.high_water_mark = max(self.high_water_mark, current_price)
        trailing_stop = self.high_water_mark * (1 - self.trailing_stop_pct)
        if current_price <= trailing_stop and profit_pct > 0:
            return True, f"TRAILING_STOP (from HWM {self.high_water_mark:.2f})"

        # 5. Sentiment reversal
        if current_sentiment < self.signal_reversal_threshold and self.entry_sentiment > 0:
            return True, f"SENTIMENT_REVERSAL (was {self.entry_sentiment:.2f}, now {current_sentiment:.2f})"

        # 6. Mention decay — hype is dying
        if current_mentions < self.peak_mentions * self.mention_decay_threshold:
            return True, f"MENTION_DECAY ({current_mentions} < {self.peak_mentions * self.mention_decay_threshold:.0f})"

        return False, "HOLD"
```

### 6c. Circuit Breakers (Portfolio Level)

```python
from config.system_config import SystemConfig
import redis

class CircuitBreaker:
    """Tiered portfolio-level risk controls. All thresholds loaded from SystemConfig."""

    def __init__(self, cfg: SystemConfig = None):
        cfg = cfg or SystemConfig.load(redis.Redis())
        self.loss_limits = {
            'single_trade':  cfg.loss_limit_single_trade,
            'daily':         cfg.loss_limit_daily,
            'weekly':        cfg.loss_limit_weekly,
            'monthly':       cfg.loss_limit_monthly,
            'drawdown_halt': cfg.drawdown_halt,
        }
        self.MAX_SOCIAL_ALLOCATION = cfg.max_social_allocation
        self.MAX_SECTOR_ALLOCATION = cfg.max_sector_allocation
        self.MAX_SINGLE_POSITION   = cfg.max_single_position

    def check_and_apply(self, state: dict) -> str:
        """Returns: 'ALLOW' | 'REDUCE_25' | 'REDUCE_50' | 'HALT_NEW' | 'FULL_HALT'"""

        if abs(state['position_pnl'] / state['portfolio']) > self.loss_limits['single_trade']:
            return 'CLOSE_POSITION'

        if state['daily_pnl'] / state['portfolio'] < -self.loss_limits['daily']:
            return 'HALT_NEW'

        if state['weekly_pnl'] / state['portfolio'] < -self.loss_limits['weekly']:
            return 'REDUCE_50'

        drawdown = (state['hwm'] - state['portfolio']) / state['hwm']
        if drawdown > self.loss_limits['drawdown_halt']:
            return 'FULL_HALT'
        elif drawdown > 0.15:
            return 'HALT_NEW'
        elif drawdown > 0.10:
            return 'REDUCE_50'
        elif drawdown > 0.05:
            return 'REDUCE_25'

        if state['social_exposure'] / state['portfolio'] > self.MAX_SOCIAL_ALLOCATION:
            return 'HALT_NEW'

        return 'ALLOW'

[^16]: Synthesis of Carver pysystemtrade blog (half-compounding rule), verified code from ashwini-singhh/crypto_trading_agent:python-services/risk-service/

### 6d. Liquidity & Market Impact Pre-checks

```python
from config.system_config import SystemConfig
import redis

def is_tradeable(ticker: str, order_value: float, market_data: dict,
                 cfg: SystemConfig = None) -> tuple[bool, str]:
    """Pre-trade liquidity screening. Thresholds sourced from SystemConfig."""
    cfg = cfg or SystemConfig.load(redis.Redis())

    adv        = market_data['avg_daily_volume_usd']
    market_cap = market_data['market_cap']
    spread_bps = market_data['bid_ask_spread_bps']

    if adv < cfg.trade_min_adv_usd:
        return False, f"Illiquid: ADV ${adv:,.0f} < ${cfg.trade_min_adv_usd:,.0f}"
    if market_cap < cfg.trade_min_mcap_usd:
        return False, f"Micro-cap: mktcap ${market_cap:,.0f} < ${cfg.trade_min_mcap_usd:,.0f}"
    if spread_bps > cfg.trade_max_spread_bps:
        return False, f"Spread too wide: {spread_bps:.0f}bps > {cfg.trade_max_spread_bps}bps"

    max_order = adv * cfg.trade_max_order_adv_pct
    if order_value > max_order:
        return False, f"Order too large: ${order_value:,.0f} > {cfg.trade_max_order_adv_pct:.1%} ADV"

    return True, "OK"
```

[^17]: Almgren-Chriss market impact framework; Buz & de Melo (2021) — small-cap liquidity risk; "Realistic Market Impact Modeling" arXiv 2026 (MACE paper)

### 6e. VIX Market Regime Filter

```python
from config.system_config import SystemConfig
import redis

def get_market_regime_scalar(vix: float, cfg: SystemConfig = None) -> float:
    """Scale position sizes based on macro volatility regime. Thresholds from SystemConfig."""
    cfg = cfg or SystemConfig.load(redis.Redis())
    if vix > cfg.vix_crisis:            return 0.00
    elif vix > cfg.vix_high_fear:       return 0.25
    elif vix > cfg.vix_elevated:        return 0.50
    elif vix > cfg.vix_slightly_elevated: return 0.75
    else:                               return 1.00
```

---

---

*[⬆ Back to main index](README.md)*
