"""
SystemConfig — single source of truth for all tunable system parameters.

Stored in Redis at key "config:system" as JSON.
Every service calls SystemConfig.load(redis) at the start of each loop
iteration so that changes made via the Streamlit Config page take effect
within one cycle — no service restarts required.

See design §16 for full parameter reference.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field

import redis.asyncio as aioredis


@dataclass
class SystemConfig:
    """All tunable parameters for the social media momentum trading system."""

    # ── Watchlist & Discovery ─────────────────────────────────────────────────
    watchlist_stale_hours: int = 48          # remove ticker silent for N hours
    watchlist_max_size: int = 50             # max active tickers (seeds are exempt)
    watchlist_promote_interval: int = 600   # seconds between liquidity checks
    counts_poll_interval_sec: int = 300     # X Counts polling cadence (legacy; X disabled by default)
    stocktwits_poll_interval_sec: int = 300
    discovery_poll_interval_sec: int = 300      # yfinance / alpha_vantage / ibkr scanner cadence
    yfinance_screener_count: int = 50           # tickers to fetch per screener call
    alpha_vantage_cache_ttl_sec: int = 3600     # cache TTL — protects 25 req/day quota

    # ── Spike Detection ───────────────────────────────────────────────────────
    spike_zscore_threshold: float = 2.0     # Z-score to trigger Tier-2 pull
    mention_window_minutes: int = 60        # rolling window for mention count
    x_search_max_results: int = 100         # posts per spike ($0.005 each) — X only
    bluesky_search_count: int = 25          # posts per Bluesky search call

    # ── X API (disabled by default — pay-per-use, ~$0.005/request) ───────────
    x_api_enabled: bool = False             # explicit opt-in required to prevent surprise billing

    # ── Seed tickers (never expire from watchlist) ────────────────────────────
    seed_tickers: list[str] = field(default_factory=lambda: [
        "AAPL", "TSLA", "NVDA", "AMD", "MSFT",
        "META", "AMZN", "GOOGL", "SPY", "QQQ",
    ])

    # ── Liquidity Gate (watchlist admission) ──────────────────────────────────
    watchlist_min_adv_usd: int = 500_000
    watchlist_min_mcap_usd: int = 50_000_000
    watchlist_max_spread_pct: float = 0.01

    # ── Signal Generation ─────────────────────────────────────────────────────
    sentiment_strength_min: float = 0.30    # |sentiment| must exceed this
    price_momentum_min_pct: float = 0.02    # min price move for momentum factor
    reactive_price_threshold: float = 0.10  # >10% pre-spike move = reactive
    convergence_bonus: float = 0.20         # bonus when Twitter + Reddit agree
    signal_age_max_hours: int = 48          # discard signals older than this
    signal_approval_max_age_min: int = 10   # reject signals older than this many minutes at approval
    signal_decay_lambda: float = 0.10       # hyperbolic decay λ (half-life ~7h)
    signal_poll_interval_sec: int = 60

    # ── Two-Phase Signal Pipeline ─────────────────────────────────────────────
    # Phase 1: evaluated using only free/Tier-1 sources (lower threshold).
    #          Tickers passing Phase 1 trigger Tier-2 enrichment calls.
    # Phase 2: re-evaluated with all sources (higher threshold).
    #          Only Phase-2 signals are forwarded to execution.
    signal_phase1_threshold: float = 0.40       # coarse filter — free sources only
    signal_phase2_threshold: float = 0.65       # fine filter — all sources required
    phase2_max_tickers_per_cycle: int = 10      # cost cap: max Tier-2 calls per cycle
    phase2_skip_open_positions: bool = True     # skip enrichment if position already open

    # Signal quality factor weights — must sum to 1.0
    w_volume: float = 0.30
    w_sentiment: float = 0.25
    w_proactivity: float = 0.20
    w_momentum: float = 0.15
    w_convergence: float = 0.10

    # ── NLP Pipeline ─────────────────────────────────────────────────────────
    vader_neutral_threshold: float = 0.05   # drop posts with |compound| < this
    finbert_batch_size: int = 16
    bot_min_account_age_days: int = 30
    bot_max_velocity_per_hour: int = 50
    bot_min_follower_following_ratio: float = 0.1

    # ── Position Sizing ───────────────────────────────────────────────────────
    max_position_pct: float = 0.02          # max per trade as fraction of NLV
    half_kelly_fraction: float = 0.50       # Kelly multiplier
    sigma_target: float = 0.15             # target annual volatility (15%)

    # ── Exit Rules ────────────────────────────────────────────────────────────
    atr_multiplier: float = 2.0             # stop: entry ± N × ATR
    max_hold_trading_days: int = 3          # hard time stop in NYSE trading days
    trailing_stop_pct: float = 0.08        # trailing stop from high-water mark
    trailing_stop_min_pct: float = 0.02    # floor for ATR stop distance and tightened trailing stop (mention decay)
    take_profit_pct: float = 0.04          # take profit target
    signal_reversal_threshold: float = -0.20
    mention_decay_threshold: float = 0.25  # exit when smoothed mentions drop to this fraction of peak
    mention_decay_min_hold_hours: float = 1.0  # don't fire MENTION_DECAY until held this long
    mention_decay_smooth_samples: int = 3      # number of recent poll samples to average for mention ratio

    # ── Circuit Breakers ──────────────────────────────────────────────────────
    loss_limit_single_trade: float = 0.01  # 1%  — close position immediately
    loss_limit_daily: float = 0.03         # 3%  — halt new trades today
    loss_limit_weekly: float = 0.07        # 7%  — reduce sizes 50%
    loss_limit_monthly: float = 0.15       # 15% — warning threshold
    drawdown_halt: float = 0.20            # 20% from HWM = full halt

    # ── Concentration Limits ──────────────────────────────────────────────────
    max_social_allocation: float = 0.20    # max portfolio % in social trades
    max_sector_allocation: float = 0.15
    max_single_position: float = 0.10

    # ── VIX Regime Scalars ────────────────────────────────────────────────────
    vix_crisis: float = 40.0               # → 0% size
    vix_high_fear: float = 30.0            # → 25% size
    vix_elevated: float = 25.0             # → 50% size
    vix_slightly_elevated: float = 20.0   # → 75% size

    # ── Trade Execution Liquidity Gate ────────────────────────────────────────
    trade_min_adv_usd: int = 500_000
    trade_min_mcap_usd: int = 50_000_000
    trade_max_spread_bps: int = 100        # 1% max bid-ask spread
    trade_max_order_adv_pct: float = 0.005  # max 0.5% of ADV per order

    # ── Internal ─────────────────────────────────────────────────────────────
    REDIS_KEY: str = "config:system"

    # ── Methods ──────────────────────────────────────────────────────────────

    @classmethod
    async def load(cls, rc: aioredis.Redis) -> SystemConfig:
        """
        Load from Redis. Falls back to defaults if no config saved yet.
        Coerces values to declared field types so string values stored by
        older versions are handled correctly.
        """
        raw = await rc.get(cls.REDIS_KEY)  # type: ignore[attr-defined]
        if raw:
            stored: dict = json.loads(raw)
            fields = cls.__dataclass_fields__  # type: ignore[attr-defined]
            coerced: dict = {}
            for k, v in stored.items():
                if k not in fields:
                    continue
                field_type = fields[k].type
                try:
                    if field_type in ("float", float):
                        coerced[k] = float(v)
                    elif field_type in ("int", int):
                        coerced[k] = int(float(v))
                    elif field_type in ("bool", bool):
                        coerced[k] = v if isinstance(v, bool) else str(v).lower() == "true"
                    else:
                        coerced[k] = v
                except (ValueError, TypeError):
                    pass  # skip malformed values; field keeps its default
            # Backward-compat: migrate old max_hold_hours → max_hold_trading_days
            if "max_hold_hours" in stored and "max_hold_trading_days" not in stored:
                old_hours = int(float(stored["max_hold_hours"]))
                import math
                coerced["max_hold_trading_days"] = max(1, math.ceil(old_hours / 6.5))
            return cls(**coerced)
        return cls()

    async def save(self, rc: aioredis.Redis) -> None:
        """Persist current config to Redis. All services pick it up next cycle."""
        await rc.set(self.REDIS_KEY, json.dumps(asdict(self)))  # type: ignore[attr-defined]

    def validate(self) -> list[str]:
        """Return list of validation errors. Empty list = config is valid."""
        errors: list[str] = []

        weight_sum = (
            self.w_volume + self.w_sentiment + self.w_proactivity
            + self.w_momentum + self.w_convergence
        )
        if abs(weight_sum - 1.0) > 0.001:
            errors.append(
                f"Signal weights must sum to 1.0 (currently {weight_sum:.4f})"
            )

        if self.loss_limit_daily >= self.loss_limit_weekly:
            errors.append("Daily loss limit must be < weekly loss limit")

        if self.drawdown_halt <= self.loss_limit_monthly:
            errors.append("Drawdown halt must be > monthly loss limit")

        if self.max_position_pct > self.max_single_position:
            errors.append(
                "max_position_pct per signal cannot exceed max_single_position"
            )

        if self.vix_crisis <= self.vix_high_fear:
            errors.append("vix_crisis must be > vix_high_fear")

        if self.max_hold_trading_days < 1:
            errors.append("max_hold_trading_days must be >= 1")

        return errors

    def config_hash(self) -> str:
        """Short 16-char MD5 identifier — used for run history lookup."""
        return hashlib.md5(
            json.dumps(asdict(self), sort_keys=True).encode()
        ).hexdigest()[:16]

    async def save_run_snapshot(
        self,
        metrics: dict,
        mode: str = "live",
    ) -> None:
        """
        Save EOD config + session metrics to config_runs table.
        Powers the §17 parameter optimization feedback loop.
        Called by execution_service at end of each trading day.
        """
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "trading"),
            user=os.getenv("DB_USER", "trader"),
            password=os.getenv("DB_PASSWORD", ""),
        )
        config_json = json.dumps(asdict(self))
        # UPSERT: unique constraint on (run_date, mode) prevents duplicates on restart.
        sql = """
            INSERT INTO config_runs (
                run_date, mode, config_snapshot, config_hash,
                total_pnl, total_trades, win_count, win_rate,
                sharpe_ratio, max_drawdown, avg_hold_hours, profit_factor,
                exits_take_profit, exits_time_stop, exits_atr_stop,
                exits_trailing_stop, exits_sentiment_reversal,
                exits_mention_decay, exits_manual,
                signals_generated, signals_executed,
                avg_signal_quality, avg_mention_zscore
            ) VALUES (
                CURRENT_DATE, %(mode)s, %(config_snapshot)s::jsonb, %(config_hash)s,
                %(total_pnl)s, %(total_trades)s, %(win_count)s, %(win_rate)s,
                %(sharpe_ratio)s, %(max_drawdown)s, %(avg_hold_hours)s, %(profit_factor)s,
                %(exits_take_profit)s, %(exits_time_stop)s, %(exits_atr_stop)s,
                %(exits_trailing_stop)s, %(exits_sentiment_reversal)s,
                %(exits_mention_decay)s, %(exits_manual)s,
                %(signals_generated)s, %(signals_executed)s,
                %(avg_signal_quality)s, %(avg_mention_zscore)s
            )
            ON CONFLICT (run_date, mode) DO UPDATE SET
                config_snapshot     = EXCLUDED.config_snapshot,
                config_hash         = EXCLUDED.config_hash,
                total_pnl           = EXCLUDED.total_pnl,
                total_trades        = EXCLUDED.total_trades,
                win_count           = EXCLUDED.win_count,
                win_rate            = EXCLUDED.win_rate,
                sharpe_ratio        = EXCLUDED.sharpe_ratio,
                max_drawdown        = EXCLUDED.max_drawdown,
                avg_hold_hours      = EXCLUDED.avg_hold_hours,
                profit_factor       = EXCLUDED.profit_factor,
                exits_take_profit        = EXCLUDED.exits_take_profit,
                exits_time_stop          = EXCLUDED.exits_time_stop,
                exits_atr_stop           = EXCLUDED.exits_atr_stop,
                exits_trailing_stop      = EXCLUDED.exits_trailing_stop,
                exits_sentiment_reversal = EXCLUDED.exits_sentiment_reversal,
                exits_mention_decay      = EXCLUDED.exits_mention_decay,
                exits_manual             = EXCLUDED.exits_manual,
                signals_generated   = EXCLUDED.signals_generated,
                signals_executed    = EXCLUDED.signals_executed,
                avg_signal_quality  = EXCLUDED.avg_signal_quality,
                avg_mention_zscore  = EXCLUDED.avg_mention_zscore
        """
        def _clamp(v, lo, hi, decimals):
            if v is None:
                return None
            return round(max(lo, min(hi, float(v))), decimals)

        params = {
            "mode": mode,
            "config_snapshot": config_json,
            "config_hash": self.config_hash(),
            "total_pnl":      metrics.get("total_pnl"),
            "total_trades":   metrics.get("total_trades"),
            "win_count":      metrics.get("win_count"),
            "win_rate":       _clamp(metrics.get("win_rate"),          0,    1,    4),
            "sharpe_ratio":   _clamp(metrics.get("sharpe_ratio"),   -999,  999,   4),
            "max_drawdown":   _clamp(metrics.get("max_drawdown"),      0,    1,    4),
            "avg_hold_hours": _clamp(metrics.get("avg_hold_hours"),    0, 9999,    2),
            "profit_factor":  _clamp(metrics.get("profit_factor"),     0, 9999,    4),
            **{k: metrics.get(k) for k in [
                "exits_take_profit", "exits_time_stop", "exits_atr_stop",
                "exits_trailing_stop", "exits_sentiment_reversal",
                "exits_mention_decay", "exits_manual",
                "signals_generated", "signals_executed",
            ]},
            "avg_signal_quality": _clamp(metrics.get("avg_signal_quality"), 0,    9, 4),
            "avg_mention_zscore": _clamp(metrics.get("avg_mention_zscore"), -9999, 9999, 2),
        }
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
        conn.close()
