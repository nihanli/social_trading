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

### 6b. Exit Rules

Position exit is managed at two levels: a **hardware bracket** placed on the IB server at entry
(operates even if the service is offline), and a **software exit loop** that runs every 60 s and
evaluates additional rules that IB cannot natively express.

---

#### OCA Bracket (Server-side, placed at entry)

When a position is filled, three closing orders are placed into a single OCA (One-Cancels-All)
group on the IB server. If any one fires, IB cancels the other two automatically.

| Leg | Order type | Purpose |
|-----|-----------|---------|
| ATR stop | `STOP` at `fill_price ± N × ATR` | Hard floor; fires immediately if price gaps through stop |
| Take-profit | `LIMIT` at `fill_price × (1 + take_profit_pct)` | Lock in profit target |
| Trailing stop | `TRAIL` with `trailingPercent` | Protect profits as price rises |

**ATR stop distance floor (`trailing_stop_min_pct`):**  
If the ATR-derived stop distance is narrower than `cfg.trailing_stop_min_pct`, the ATR stop is
raised to exactly `fill_price × (1 − trailing_stop_min_pct)` for LONG (lowered for SHORT).  
This prevents overly tight stops on low-volatility days.

**ATR stop leg skipped when trailing stop is already tighter:**  
If the initial trailing stop trigger  
`fill_price × (1 − trailing_stop_pct)` ≥ `effective_stop_loss` (LONG),  
the ATR stop leg is omitted — the trailing stop subsumes it from the start.

**Trailing stop — initial placement:**

```python
trail_order.orderType       = "TRAIL"
trail_order.trailingPercent = cfg.trailing_stop_pct * 100   # e.g. 8.0 for 8%
# No trailStopPrice needed — IB anchors automatically from current market price at fill
```

IB internally tracks `trigger = best_price_seen × (1 − pct/100)` for SELL TRAIL.  
The percentage re-anchors automatically as price moves — no stale dollar amount.

**Fill-price recomputation:**  
All three legs are recomputed from the actual IB fill price, not the signal's estimated entry.  
For the ATR stop: `effective_stop_loss = fill_price ± original_ATR_offset`.

---

#### Software Exit Rules (evaluated every 60 s per position)

Exit rules are evaluated in priority order. The first match wins.

**Rule 1 — Emergency single-trade loss limit**

> Only active when `stop_loss = 0` (no ATR stop could be placed at entry). Acts as a last-resort
> safety net for otherwise unprotected positions.

```
condition:  stop_loss == 0  AND  loss_pct > cfg.loss_limit_single_trade
action:     EMERGENCY close
default:    loss_limit_single_trade = 1%
```

**Rule 2 — ATR stop-loss**

> Software mirror of the IB OCA stop leg. Fires if price crosses the ATR stop level recorded in
> `position.stop_loss`. Skipped if `stop_loss = 0` (position reconciled after restart without ATR
> data — the IB OCA order on the server handles it).

```
LONG:   condition  current_price <= position.stop_loss
SHORT:  condition  current_price >= position.stop_loss
action: STOP_LOSS close
params: atr_multiplier = 2.0 (stop = entry ± N × ATR)
```

**Rule 3 — Take-profit**

> Software mirror of the IB OCA limit leg. Fires if price reaches or exceeds the take-profit level
> recorded in `position.take_profit`. Skipped if `take_profit = 0`.

```
LONG:   condition  current_price >= position.take_profit
SHORT:  condition  current_price <= position.take_profit
action: TAKE_PROFIT close
params: take_profit_pct = 4%  (take_profit = fill × (1 + take_profit_pct))
```

**Rule 4 — Trailing stop (software)**

> Software mirror / paper-mode equivalent of the IB OCA trailing stop leg.
> The trailing stop **only activates** once the position has moved at least
> `trailing_stop_activation_pct` into profit — preventing it from acting as an
> alternative first stop on positions that never moved in the intended direction
> (Rule 2 handles those). HWM = high-water mark.

```
LONG:   activate when  HWM >= entry × (1 + trailing_stop_activation_pct)
        trigger at     HWM × (1 − trailing_stop_pct_applied)
SHORT:  activate when  HWM <= entry × (1 − trailing_stop_activation_pct)
        trigger at     HWM × (1 + trailing_stop_pct_applied)
action: TRAILING_STOP close
params: trailing_stop_pct = 8%   (default; tightened dynamically by Rule 6 below)
        trailing_stop_activation_pct = 1%
        trailing_stop_min_pct = 2%   (floor — neither ATR nor trailing stop can be tighter than this)
```

`trailing_stop_pct_applied` is the **effective** trailing pct for this position, which may be
narrower than `cfg.trailing_stop_pct` if mention decay has tightened it (see Rule 6).

**Rule 5 — Sentiment reversal**

> Exits when aggregated sentiment has crossed from bullish to strongly bearish (LONG) or vice-versa.
> Skipped when sentiment data is unavailable (`current_sentiment = 0.0`).

```
LONG:   condition  current_sentiment < cfg.signal_reversal_threshold   (e.g. < −0.20)
SHORT:  condition  current_sentiment > −cfg.signal_reversal_threshold  (e.g. >  +0.20)
action: SENTIMENT_REVERSAL close
params: signal_reversal_threshold = −0.20
```

**Rule 6 — Mention-decay trailing stop tightening**

> When social media mentions decay, the position's trailing stop is tightened dynamically.
> Rather than hard-exiting on mention decay, the trailing stop % is reduced proportionally —
> locking in more profit as the catalyst fades, while letting a strong price trend continue.
>
> Rule 6 does **not** produce a direct exit. Instead it feeds a tighter `trailing_stop_pct_applied`
> into Rule 4 (and updates the live IB TRAIL order). The position exits naturally through Rule 4
> once price falls through the tightened trail.

**Tightening formula:**

```
mention_ratio = smoothed_current_mentions / peak_mentions
                (smoothed over cfg.mention_decay_smooth_samples windows)

t = clamp((mention_ratio − mention_decay_threshold) / (1 − mention_decay_threshold), 0, 1)
    # t = 1.0  → mentions at full peak  (use max/default trailing pct)
    # t = 0.0  → mentions at or below threshold (use min trailing pct)

effective_pct = trailing_stop_min_pct + t × (trailing_stop_pct − trailing_stop_min_pct)
```

Rule 6 is only evaluated after `mention_decay_min_hold_hours` to avoid false triggers from the
natural decay of the entry spike.

An update is applied when `|effective_pct − trailing_stop_pct_applied| >= 0.5%` (hardcoded
noise gate to avoid excessive IB order churn).

**IB live mode — updating the TRAIL order:**

When the trailing stop tightens, the existing TRAIL order is cancelled and a new one placed,
anchored from the current HWM to preserve locked-in profit:

```python
trail_order.trailingPercent = new_pct * 100             # e.g. 5.0 for 5%
trail_order.trailStopPrice  = hwm * (1 - new_pct)       # LONG: anchor initial trigger from HWM
# SHORT: hwm * (1 + new_pct)
```

Using both fields is intentional: `trailingPercent` keeps the trail percentage-based going forward;
`trailStopPrice` ensures IB starts the trigger from the HWM (not from the current lower price),
so profit already locked in by the HWM is preserved.

When mention decay is active (`t < 1`), `trailing_stop_activation_pct` is set to `0.0` in the
effective config — the tightened trailing stop is unconditional and does not require the activation
threshold.

```
params: mention_decay_threshold         = 0.25  (fraction of peak; below this → maximum tightening)
        mention_decay_min_hold_hours    = 1.0   (don't fire until held this long)
        mention_decay_smooth_samples    = 3     (windows to average mention ratio)
        trailing_stop_min_pct           = 2%    (floor for tightened trailing stop)
```

**Rule 7 — Hard time stop**

> Unconditional exit after `max_hold_hours`. Social media alpha is ephemeral; positions held too
> long are driven by fundamentals, not the original catalyst.

```
condition:  hours_held >= cfg.max_hold_hours
action:     TIME_STOP close
params:     max_hold_hours = 48
```

---

#### Exit Rule Summary

| # | Name | Trigger | IB hardware? | Software? |
|---|------|---------|:---:|:---:|
| 1 | Emergency loss | `loss_pct > 1%` AND no ATR stop | — | ✓ |
| 2 | ATR stop-loss | price crosses `fill ± N×ATR` | ✓ OCA STOP | ✓ mirror |
| 3 | Take-profit | price reaches `fill × (1+TP%)` | ✓ OCA LIMIT | ✓ mirror |
| 4 | Trailing stop | price drops `trail_pct%` from HWM | ✓ OCA TRAIL | ✓ mirror |
| 5 | Sentiment reversal | sentiment crosses threshold | — | ✓ |
| 6 | Mention decay tightening | tightens Rule 4 trail dynamically | TRAIL update | param update |
| 7 | Hard time stop | held > `max_hold_hours` | — | ✓ |

Rules 2–4 are mirrored: IB OCA fires if the service is offline; software closes if IB already
cancelled the OCA (e.g. partial fill). The OCA group name `oca_{entry_id}` is preserved across
TRAIL order replacements so IB can cancel the other legs correctly.

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
