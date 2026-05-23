## 16. System Configuration

All tunable parameters are centralised in a single `SystemConfig` dataclass. An instance
is stored in Redis as JSON, so every microservice and the Streamlit UI share one consistent
view — no config files scattered across services, no environment variables for business logic.

### Config Storage Principle

```
SystemConfig defaults (code)
        │
        ▼
Redis  "config:system"  ◄──── Streamlit §15 Config page (trader edits)
        │
        ├──► WatchlistManager   (§3a)
        ├──► tier1_poll_loop    (§3b)
        ├──► signal_engine_loop (§5a)
        ├──► PositionExitManager(§6b)
        └──► CircuitBreaker     (§6c)
```

Every service calls `SystemConfig.load(rc)` at the start of each loop iteration so that
parameter changes made in the UI take effect within one cycle — no restarts required.

---

### `config/system_config.py`

```python
import json, redis
from dataclasses import dataclass, asdict, field
from typing import Dict

@dataclass
class SystemConfig:
    """
    Single source of truth for all tunable system parameters.
    Edit via the Streamlit Config page (§15, pages/5_config.py).
    Changes are live within one service loop cycle (~1 minute).
    """

    # ── Watchlist & Discovery ───────────────────────────────────────────────
    watchlist_stale_hours:      int   = 48      # remove ticker silent for this many hours
    watchlist_promote_interval: int   = 600     # seconds between liquidity gate checks
    counts_poll_interval_sec:   int   = 300     # X Counts polling cadence (seconds)
    stocktwits_poll_interval_sec: int = 300     # StockTwits trending poll cadence

    # ── Spike Detection ─────────────────────────────────────────────────────
    spike_zscore_threshold:     float = 2.0     # Z-score to trigger Tier 2 content pull
    mention_window_minutes:     int   = 60      # rolling window for mention count
    x_search_max_results:       int   = 100     # posts pulled per X spike ($0.005 each)

    # ── Liquidity Gate (watchlist admission) ────────────────────────────────
    watchlist_min_adv_usd:      int   = 500_000
    watchlist_min_mcap_usd:     int   = 50_000_000
    watchlist_max_spread_pct:   float = 0.01

    # ── Signal Generation ───────────────────────────────────────────────────
    signal_quality_threshold:   float = 0.60    # minimum score to fire a signal
    sentiment_strength_min:     float = 0.30    # |sentiment| must exceed this
    price_momentum_min_pct:     float = 0.02    # minimum price move for momentum factor
    reactive_price_threshold:   float = 0.10    # >10% pre-spike move = reactive, penalised
    convergence_bonus:          float = 0.20    # added when Twitter + Reddit agree
    signal_age_max_hours:       int   = 48      # discard signals older than this
    signal_decay_lambda:        float = 0.10    # hyperbolic decay λ (half-life ~7 hrs)
    signal_poll_interval_sec:   int   = 60      # signal engine loop cadence

    # Signal quality factor weights (must sum to 1.0)
    w_volume:       float = 0.30
    w_sentiment:    float = 0.25
    w_proactivity:  float = 0.20
    w_momentum:     float = 0.15
    w_convergence:  float = 0.10

    # ── Position Sizing ─────────────────────────────────────────────────────
    max_position_pct:       float = 0.02    # cap per trade as fraction of portfolio
    half_kelly_fraction:    float = 0.50    # Kelly multiplier (0.5 = half-Kelly)
    sigma_target:           float = 0.15    # target annual volatility (15%)

    # ── Exit Rules ──────────────────────────────────────────────────────────
    atr_multiplier:             float = 2.0     # ATR stop: entry ± N × ATR
    max_hold_hours:             int   = 48      # time stop (hard limit)
    trailing_stop_pct:          float = 0.08    # trailing stop from high-water mark
    take_profit_pct:            float = 0.04    # take profit target
    signal_reversal_threshold:  float = -0.20   # sentiment level triggering reversal exit
    mention_decay_threshold:    float = 0.25    # exit when mentions drop to this fraction of peak

    # ── Circuit Breakers ────────────────────────────────────────────────────
    loss_limit_single_trade:    float = 0.01    # 1%  — close position immediately
    loss_limit_daily:           float = 0.03    # 3%  — halt new trades today
    loss_limit_weekly:          float = 0.07    # 7%  — reduce position sizes 50%
    loss_limit_monthly:         float = 0.15    # 15% — warning threshold
    drawdown_halt:              float = 0.20    # 20% drawdown from HWM = full halt

    # ── Concentration Limits ────────────────────────────────────────────────
    max_social_allocation:  float = 0.20    # max % of portfolio in social media positions
    max_sector_allocation:  float = 0.15    # max % in any one sector
    max_single_position:    float = 0.10    # max % in any one name

    # ── VIX Regime Scalars ──────────────────────────────────────────────────
    vix_crisis:             float = 40.0    # above this → 0% position size
    vix_high_fear:          float = 30.0    # above this → 25% position size
    vix_elevated:           float = 25.0    # above this → 50% position size
    vix_slightly_elevated:  float = 20.0    # above this → 75% position size
    # (below vix_slightly_elevated → 100% position size)

    # ── Trade Execution Liquidity Gate ──────────────────────────────────────
    trade_min_adv_usd:      int   = 500_000
    trade_min_mcap_usd:     int   = 50_000_000
    trade_max_spread_bps:   int   = 100         # 1% max bid-ask spread
    trade_max_order_adv_pct: float = 0.005      # max 0.5% of ADV per order

    # ── Redis key ───────────────────────────────────────────────────────────
    REDIS_KEY: str = "config:system"

    # ------------------------------------------------------------------ #

    @classmethod
    def load(cls, rc: redis.Redis) -> "SystemConfig":
        """Load from Redis. Falls back to defaults if no config saved yet."""
        raw = rc.get(cls.REDIS_KEY)
        if raw:
            stored = json.loads(raw)
            # Only pass recognised fields — forwards-compatible with new defaults
            valid = {k: v for k, v in stored.items()
                     if k in cls.__dataclass_fields__}
            return cls(**valid)
        return cls()

    def save(self, rc: redis.Redis) -> None:
        """Persist current config to Redis. All services pick it up next cycle."""
        rc.set(self.REDIS_KEY, json.dumps(asdict(self)))

    def validate(self) -> list[str]:
        """Return list of validation errors. Empty list = valid."""

    def config_hash(self) -> str:
        """Short identifier for this config — useful for run history lookup."""
        import hashlib
        return hashlib.md5(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()[:16]

    def save_run_snapshot(self, metrics: dict, mode: str = "paper") -> None:
        """
        Called at end-of-day (or end-of-session) to record this config alongside
        the session's performance metrics into PostgreSQL config_runs table.
        Powers the §17 parameter optimization feedback loop.

        metrics dict keys (all optional — None stored as NULL):
            total_pnl, total_trades, win_count, win_rate, sharpe_ratio,
            max_drawdown, avg_hold_hours, profit_factor,
            exits_take_profit, exits_time_stop, exits_atr_stop,
            exits_trailing_stop, exits_sentiment_reversal, exits_mention_decay,
            exits_manual, signals_generated, signals_executed,
            avg_signal_quality, avg_mention_zscore
        """
        import psycopg2, os
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            dbname=os.getenv("DB_NAME", "trading"),
            user=os.getenv("DB_USER", "trader"),
            password=os.getenv("DB_PASSWORD", ""),
        )
        cols = ["run_date", "mode", "config_snapshot", "config_hash"] + list(metrics.keys())
        vals = ["CURRENT_DATE", f"'{mode}'",
                f"'{json.dumps(asdict(self))}'::jsonb",
                f"'{self.config_hash()}'"]
        vals += [str(v) if v is not None else "NULL" for v in metrics.values()]

        sql = f"""
            INSERT INTO config_runs ({', '.join(cols)})
            VALUES ({', '.join(vals)})
        """
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        conn.close()
        errors = []
        weight_sum = self.w_volume + self.w_sentiment + self.w_proactivity \
                   + self.w_momentum + self.w_convergence
        if abs(weight_sum - 1.0) > 0.001:
            errors.append(f"Signal weights must sum to 1.0 (currently {weight_sum:.3f})")
        if self.loss_limit_daily >= self.loss_limit_weekly:
            errors.append("Daily loss limit must be < weekly loss limit")
        if self.drawdown_halt <= self.loss_limit_monthly:
            errors.append("Drawdown halt must be > monthly loss limit")
        if self.max_position_pct > self.max_single_position:
            errors.append("Max position % per signal cannot exceed max single position %")
        return errors
```

---

### Parameter Reference Table

| Parameter | Default | Description | Section |
|-----------|---------|-------------|---------|
| `spike_zscore_threshold` | 2.0 | Z-score to trigger X post pull | §3b |
| `mention_window_minutes` | 60 | Rolling window for mention count | §3b |
| `x_search_max_results` | 100 | Posts pulled per spike ($0.50 cost) | §3b |
| `watchlist_stale_hours` | 48 | Expiry for silent watchlist tickers | §3a |
| `watchlist_min_adv_usd` | $500K | Min avg daily volume for admission | §3a |
| `watchlist_min_mcap_usd` | $50M | Min market cap for admission | §3a |
| `signal_quality_threshold` | 0.60 | Minimum composite score to fire | §5b |
| `w_volume` | 0.30 | Weight: mention volume Z-score | §5b |
| `w_sentiment` | 0.25 | Weight: sentiment strength | §5b |
| `w_proactivity` | 0.20 | Weight: proactivity bonus | §5b |
| `w_momentum` | 0.15 | Weight: price momentum | §5b |
| `w_convergence` | 0.10 | Weight: cross-platform agreement | §5b |
| `max_hold_hours` | 48 | Hard time stop | §6b |
| `take_profit_pct` | 4% | Profit target | §6b |
| `trailing_stop_pct` | 8% | Trailing stop from HWM | §6b |
| `atr_multiplier` | 2.0 | ATR stop distance | §6b |
| `max_position_pct` | 2% | Max size per trade | §6a |
| `loss_limit_daily` | 3% | Daily halt threshold | §6c |
| `drawdown_halt` | 20% | Full halt threshold | §6c |
| `max_social_allocation` | 20% | Max portfolio in social trades | §6c |
| `vix_crisis` | 40 | VIX above → no trades | §6e |

> ⚠️ **Medium-confidence parameters** (signal weights, VIX thresholds, Kelly fraction) are
> synthesised from academic literature and practitioner convention. Tune all values through
> backtesting (§12) before live deployment. The Config page in Streamlit (§15) makes this
> iterative process straightforward.

---

*[⬆ Back to main index](README.md)*
