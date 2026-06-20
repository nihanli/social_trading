"""
backtest.py — Walk-forward backtest engine for the optimization page.

Three modes:

  Fast Mode:   Replays executed trades (from trades table) with varied
               exit parameters. Uses real ATR from trades.atr_at_entry and
               pre-stored OHLC bars from price_ohlc. Entry price is the
               actual fill price.

  Full Mode:   Replays all signals from the signals table through both
               an entry filter (quality_score threshold) and the exit
               simulation. Entry price is the 5-min bar open following
               the signal timestamp; falls back to daily open.

  Walk-Forward Optimization (WFO):
               Rolls a fixed IS window (default 28 days) + OOS window
               (default 14 days) across the full history. Each window:
               1. Grid search on IS data → find best params by Sharpe
               2. Apply best params to OOS data → record honest OOS metrics
               Aggregates OOS results to produce WFO efficiency ratio,
               parameter stability analysis, and a combined OOS equity curve.

Both Fast and Full use intraday 5-min bars for entry-day simulation and
daily bars for subsequent days.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ── Parameter grid definition ─────────────────────────────────────────────────

PARAM_DEFS: dict[str, dict[str, Any]] = {
    "take_profit_pct": {
        "label":   "Take-profit %",
        "modes":   {"fast", "full"},
        "default": [0.03, 0.04, 0.05, 0.06],
        "options": [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10],
        "fmt":     lambda v: f"{v:.0%}",
    },
    "atr_multiplier": {
        "label":   "ATR multiplier",
        "modes":   {"fast", "full"},
        "default": [1.5, 2.0, 2.5],
        "options": [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0],
        "fmt":     lambda v: str(v),
    },
    "trailing_stop_pct": {
        "label":   "Trailing stop %",
        "modes":   {"fast", "full"},
        "default": [0.05, 0.07, 0.09],
        "options": [0.03, 0.05, 0.07, 0.09, 0.11],
        "fmt":     lambda v: f"{v:.0%}",
    },
    "max_hold_days": {
        "label":   "Max hold days",
        "modes":   {"fast", "full"},
        "default": [2, 3],
        "options": [1, 2, 3, 4, 5],
        "fmt":     lambda v: str(v),
    },
    "signal_phase1_threshold": {
        "label":   "Signal quality threshold",
        "modes":   {"full"},
        "default": [0.50, 0.55, 0.60],
        "options": [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80],
        "fmt":     lambda v: f"{v:.2f}",
    },
    "mention_decay_threshold": {
        "label":   "Mention decay exit",
        "modes":   {"full"},
        "default": [0.20, 0.30],
        "options": [0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50],
        "fmt":     lambda v: f"{v:.2f}",
    },
    "sentiment_reversal_threshold": {
        "label":   "Sentiment reversal threshold",
        "modes":   {"full"},
        "default": [-0.20, -0.30],
        "options": [-0.40, -0.35, -0.30, -0.25, -0.20, -0.15, -0.10],
        "fmt":     lambda v: f"{v:.2f}",
    },
}

COST_BPS = 0.002   # 20 bps round-trip commission estimate


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class BacktestResult:
    params: dict[str, Any]
    total_pnl: float
    win_rate: float
    sharpe: float
    trades: int
    avg_hold_days: float
    exit_breakdown: dict[str, int] = field(default_factory=dict)


# ── OHLC data loader ──────────────────────────────────────────────────────────

def load_ohlc(tickers: list[str], conn) -> dict[str, dict[str, pd.DataFrame]]:
    """
    Load price_ohlc bars for the given tickers from the DB.

    Returns:
        {ticker: {"1d": daily_df, "5m": intraday_df}}

    DataFrames are indexed by bar_datetime (UTC, sorted ascending).
    """
    if not tickers:
        return {}

    placeholders = ",".join(["%s"] * len(tickers))
    daily_sql = f"""
        SELECT ticker, bar_datetime, open, high, low, close
        FROM price_ohlc
        WHERE ticker IN ({placeholders})
          AND timeframe = '1d'
        ORDER BY ticker, bar_datetime
    """
    intra_sql = f"""
        SELECT ticker, bar_datetime, open, high, low, close
        FROM price_ohlc
        WHERE ticker IN ({placeholders})
          AND timeframe = '5m'
        ORDER BY ticker, bar_datetime
    """

    result: dict[str, dict[str, pd.DataFrame]] = {}

    try:
        with conn.cursor() as cur:
            cur.execute(daily_sql, tickers)
            rows = cur.fetchall()
            cols = ["ticker", "bar_datetime", "open", "high", "low", "close"]
            df_all = pd.DataFrame(rows, columns=cols)
            for ticker, grp in df_all.groupby("ticker"):
                df = (
                    grp.drop("ticker", axis=1)
                    .set_index("bar_datetime")
                    .sort_index()
                )
                df.index = pd.to_datetime(df.index, utc=True)
                result.setdefault(ticker, {})["1d"] = df

            cur.execute(intra_sql, tickers)
            rows = cur.fetchall()
            df_all = pd.DataFrame(rows, columns=cols)
            for ticker, grp in df_all.groupby("ticker"):
                df = (
                    grp.drop("ticker", axis=1)
                    .set_index("bar_datetime")
                    .sort_index()
                )
                df.index = pd.to_datetime(df.index, utc=True)
                result.setdefault(ticker, {})["5m"] = df
    except Exception as exc:
        logger.warning("[BACKTEST] OHLC load error: %s", exc)

    return result


# ── Core simulation ───────────────────────────────────────────────────────────

def _simulate_trade(
    *,
    ticker: str,
    direction: str,
    entry_price: float,
    entry_dt: datetime,
    atr: float | None,
    shares: float,
    ohlc: dict[str, pd.DataFrame],
    params: dict[str, Any],
    mention_ratio_series: pd.Series | None = None,
    sentiment_series: pd.Series | None = None,
) -> dict[str, Any] | None:
    """
    Simulate a single trade walk-forward using stored OHLC bars.

    Returns a dict with keys: pnl, exit_reason, hold_days.
    Returns None if insufficient price data is available.
    """
    tp_pct      = params["take_profit_pct"]
    atr_mult    = params["atr_multiplier"]
    trail_pct   = params["trailing_stop_pct"]
    max_hold    = int(params["max_hold_days"])
    trail_min   = params.get("trailing_stop_min_pct", 0.02)
    mention_thr = params.get("mention_decay_threshold", 0.25)
    sent_thr    = params.get("sentiment_reversal_threshold", -0.25)

    ep = entry_price
    atr_val = atr if atr and atr > 0 else ep * 0.02   # fallback: 2% proxy

    if direction == "LONG":
        stop_price   = ep - atr_mult * atr_val
        target_price = ep * (1 + tp_pct)
    else:
        stop_price   = ep + atr_mult * atr_val
        target_price = ep * (1 - tp_pct)

    hwm = ep
    trail_applied = trail_pct   # current active trail pct
    days_held = 0
    exit_price: float | None = None
    exit_reason = "TIME_STOP"

    daily_df: pd.DataFrame = ohlc.get("1d", pd.DataFrame())
    intra_df: pd.DataFrame = ohlc.get("5m", pd.DataFrame())

    # Ensure indices are proper DatetimeIndex so Timestamp comparisons work
    if not daily_df.empty and not isinstance(daily_df.index, pd.DatetimeIndex):
        daily_df.index = pd.to_datetime(daily_df.index, utc=True)
    if not intra_df.empty and not isinstance(intra_df.index, pd.DatetimeIndex):
        intra_df.index = pd.to_datetime(intra_df.index, utc=True)

    # Normalised Timestamps for filtering (all comparisons stay in Timestamp-land,
    # avoiding .date on tz-aware indices which behaves inconsistently across pandas versions)
    entry_ts      = pd.Timestamp(entry_dt).tz_convert("UTC") if entry_dt.tzinfo else pd.Timestamp(entry_dt, tz="UTC")
    next_day_ts   = (entry_ts + pd.Timedelta(days=1)).normalize()

    # ── Entry-day intraday simulation ─────────────────────────────────────────
    if not intra_df.empty:
        entry_day_bars = intra_df[(intra_df.index >= entry_ts) & (intra_df.index < next_day_ts)]
        for _, bar in entry_day_bars.iterrows():
            lo = float(bar["low"])
            hi = float(bar["high"])
            cl = float(bar["close"])

            if direction == "LONG":
                hwm = max(hwm, hi)
                trail_stop = hwm * (1 - trail_applied)
                if lo <= stop_price:
                    exit_price = stop_price; exit_reason = "STOP_LOSS"; break
                if hi >= target_price:
                    exit_price = target_price; exit_reason = "TAKE_PROFIT"; break
                if lo <= trail_stop:
                    exit_price = trail_stop; exit_reason = "TRAILING_STOP"; break
            else:  # SHORT
                hwm = min(hwm, lo)   # HWM for shorts = lowest low
                trail_stop = hwm * (1 + trail_applied)
                if hi >= stop_price:
                    exit_price = stop_price; exit_reason = "STOP_LOSS"; break
                if lo <= target_price:
                    exit_price = target_price; exit_reason = "TAKE_PROFIT"; break
                if hi >= trail_stop:
                    exit_price = trail_stop; exit_reason = "TRAILING_STOP"; break

        if exit_price is not None:
            days_held = 0
        else:
            days_held = 1   # survived entry day, move to daily

    # ── Subsequent daily bars ─────────────────────────────────────────────────
    if exit_price is None and not daily_df.empty:
        subsequent = daily_df[daily_df.index >= next_day_ts]
        for bar_dt, bar in subsequent.iterrows():
            if days_held >= max_hold:
                exit_price = float(bar["open"])
                exit_reason = "TIME_STOP"
                break

            lo = float(bar["low"])
            hi = float(bar["high"])
            cl = float(bar["close"])
            bar_date = bar_dt.date()

            # Mention decay / sentiment reversal — tighten trail to min
            if mention_ratio_series is not None:
                try:
                    mr = float(mention_ratio_series.get(bar_date, 1.0))
                    if mr < mention_thr:
                        trail_applied = min(trail_applied, trail_min)
                except Exception:
                    pass
            if sentiment_series is not None:
                try:
                    sv = float(sentiment_series.get(bar_date, 0.0))
                    if (direction == "LONG" and sv < sent_thr) or \
                       (direction == "SHORT" and sv > -sent_thr):
                        trail_applied = min(trail_applied, trail_min)
                except Exception:
                    pass

            if direction == "LONG":
                hwm = max(hwm, hi)
                trail_stop = hwm * (1 - trail_applied)
                if lo <= stop_price:
                    exit_price = stop_price; exit_reason = "STOP_LOSS"; break
                if hi >= target_price:
                    exit_price = target_price; exit_reason = "TAKE_PROFIT"; break
                if lo <= trail_stop:
                    exit_price = trail_stop; exit_reason = "TRAILING_STOP"; break
            else:
                hwm = min(hwm, lo)
                trail_stop = hwm * (1 + trail_applied)
                if hi >= stop_price:
                    exit_price = stop_price; exit_reason = "STOP_LOSS"; break
                if lo <= target_price:
                    exit_price = target_price; exit_reason = "TAKE_PROFIT"; break
                if hi >= trail_stop:
                    exit_price = trail_stop; exit_reason = "TRAILING_STOP"; break

            days_held += 1

        if exit_price is None:
            # Reached max_hold without hitting exit: close at last close
            if not subsequent.empty:
                last_close = float(subsequent.iloc[min(max_hold - 1, len(subsequent) - 1)]["close"])
                exit_price = last_close
            else:
                # No subsequent bars: use entry_price (no P&L)
                exit_price = ep

    if exit_price is None:
        return None   # insufficient data

    if direction == "LONG":
        raw_pnl = (exit_price - ep) * shares
    else:
        raw_pnl = (ep - exit_price) * shares

    commission = abs(ep * shares) * COST_BPS
    net_pnl = raw_pnl - commission

    return {
        "pnl":        net_pnl,
        "exit_reason": exit_reason,
        "hold_days":  max(days_held, 0),
    }


# ── Fast Mode engine ──────────────────────────────────────────────────────────

def run_fast_backtest(
    trades_df: pd.DataFrame,
    ohlc_data: dict[str, dict[str, pd.DataFrame]],
    param_grid: dict[str, list],
    fixed_params: dict[str, Any],
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[BacktestResult]:
    """
    Fast Mode: replay executed trades with varied parameters.

    trades_df columns required:
        ticker, direction, shares, entry_price, atr_at_entry, opened_at

    date_from / date_to: optional inclusive date bounds to filter trades
    (used by WFO engine to restrict to IS or OOS windows).

    Returns list of BacktestResult sorted by Sharpe descending.
    """
    import itertools  # noqa: PLC0415

    # Apply date filter
    df = trades_df.copy()
    if not df.empty and (date_from is not None or date_to is not None):
        def _to_date(v) -> date:
            if isinstance(v, str):
                v = datetime.fromisoformat(v)
            if isinstance(v, datetime):
                return v.date()
            if isinstance(v, pd.Timestamp):
                return v.date()
            return v
        df["_d"] = df["opened_at"].apply(_to_date)
        if date_from:
            df = df[df["_d"] >= date_from]
        if date_to:
            df = df[df["_d"] < date_to]
        df = df.drop(columns=["_d"])

    keys = list(param_grid.keys())
    values = list(param_grid.values())
    combos = list(itertools.product(*values))

    results: list[BacktestResult] = []

    for combo in combos:
        params = {**fixed_params, **dict(zip(keys, combo))}
        pnls: list[float] = []
        hold_days: list[float] = []
        exit_counts: dict[str, int] = {}

        for _, row in df.iterrows():
            ticker    = row["ticker"]
            direction = row["direction"]
            ep        = float(row["entry_price"])
            shares    = float(row.get("shares", 1))
            atr_val   = row.get("atr_at_entry")
            opened_at = row["opened_at"]

            if ep <= 0:
                continue

            if isinstance(opened_at, str):
                opened_at = datetime.fromisoformat(opened_at)
            if opened_at.tzinfo is None:
                opened_at = opened_at.replace(tzinfo=UTC)

            ticker_ohlc = ohlc_data.get(ticker, {})
            result = _simulate_trade(
                ticker=ticker,
                direction=direction,
                entry_price=ep,
                entry_dt=opened_at,
                atr=float(atr_val) if atr_val else None,
                shares=shares,
                ohlc=ticker_ohlc,
                params=params,
            )
            if result is None:
                continue

            pnls.append(result["pnl"])
            hold_days.append(result["hold_days"])
            exit_counts[result["exit_reason"]] = exit_counts.get(result["exit_reason"], 0) + 1

        if len(pnls) < 3:
            continue

        arr = pd.Series(pnls)
        results.append(BacktestResult(
            params=dict(zip(keys, combo)),
            total_pnl=round(float(arr.sum()), 2),
            win_rate=round(float((arr > 0).mean()), 3),
            sharpe=round(float(arr.mean() / (arr.std() + 1e-9)) * (252 ** 0.5), 3),
            trades=len(pnls),
            avg_hold_days=round(sum(hold_days) / max(len(hold_days), 1), 2),
            exit_breakdown=exit_counts,
        ))

    return sorted(results, key=lambda r: r.sharpe, reverse=True)


# ── Full Mode engine ──────────────────────────────────────────────────────────

def run_full_backtest(
    signals_df: pd.DataFrame,
    ohlc_data: dict[str, dict[str, pd.DataFrame]],
    param_grid: dict[str, list],
    fixed_params: dict[str, Any],
    sentiment_df: pd.DataFrame | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[BacktestResult]:
    """
    Full Signal Mode: replay all signals through entry filter + exit sim.

    signals_df columns required:
        ticker, direction, quality_score, atr, generated_at, sentiment_score

    sentiment_df (optional): daily aggregated sentiment, columns:
        ticker, window_date, sentiment_score, mention_ratio

    Returns list of BacktestResult sorted by Sharpe descending.
    """
    import itertools  # noqa: PLC0415

    keys = list(param_grid.keys())
    values = list(param_grid.values())
    combos = list(itertools.product(*values))

    # Pre-index sentiment by (ticker, date) for fast lookup
    sent_index: dict[str, dict[date, tuple[float, float]]] = {}
    if sentiment_df is not None and not sentiment_df.empty:
        for _, row in sentiment_df.iterrows():
            t = row["ticker"]
            d = pd.Timestamp(row["window_date"]).date()
            sent_index.setdefault(t, {})[d] = (
                float(row.get("sentiment_score", 0)),
                float(row.get("mention_ratio", 1.0)),
            )

    results: list[BacktestResult] = []

    for combo in combos:
        params = {**fixed_params, **dict(zip(keys, combo))}
        signal_threshold = params.get("signal_phase1_threshold",
                                      fixed_params.get("signal_phase1_threshold", 0.0))

        pnls: list[float] = []
        hold_days: list[float] = []
        exit_counts: dict[str, int] = {}

        for _, sig in signals_df.iterrows():
            # Entry filter
            if float(sig.get("quality_score", 0)) < signal_threshold:
                continue

            ticker      = sig["ticker"]
            direction   = sig["direction"]
            generated_at = sig["generated_at"]
            atr_val     = sig.get("atr")

            if isinstance(generated_at, str):
                generated_at = datetime.fromisoformat(generated_at)
            if generated_at.tzinfo is None:
                generated_at = generated_at.replace(tzinfo=UTC)

            # Date window filter
            if date_from is not None and generated_at.date() < date_from:
                continue
            if date_to is not None and generated_at.date() >= date_to:
                continue

            # Determine entry price from 5-min bar open after signal time
            ticker_ohlc = ohlc_data.get(ticker, {})
            intra_df    = ticker_ohlc.get("5m", pd.DataFrame())
            daily_df    = ticker_ohlc.get("1d", pd.DataFrame())

            entry_price: float | None = None
            entry_dt    = generated_at

            if not intra_df.empty:
                future_bars = intra_df[intra_df.index >= generated_at]
                if not future_bars.empty:
                    first_bar  = future_bars.iloc[0]
                    entry_price = float(first_bar["open"])
                    entry_dt    = future_bars.index[0]

            if entry_price is None and not daily_df.empty:
                gen_ts   = pd.Timestamp(generated_at).tz_convert("UTC") if generated_at.tzinfo else pd.Timestamp(generated_at, tz="UTC")
                day_bars = daily_df[daily_df.index >= gen_ts.normalize()]
                if not day_bars.empty:
                    first_day   = day_bars.iloc[0]
                    entry_price = float(first_day["open"])
                    entry_dt    = day_bars.index[0]

            if entry_price is None or entry_price <= 0:
                continue

            # Build sentiment series for this ticker
            ticker_sent = sent_index.get(ticker, {})
            mention_series = pd.Series(
                {d: v[1] for d, v in ticker_sent.items()}
            ) if ticker_sent else None
            sentiment_series = pd.Series(
                {d: v[0] for d, v in ticker_sent.items()}
            ) if ticker_sent else None

            result = _simulate_trade(
                ticker=ticker,
                direction=direction,
                entry_price=entry_price,
                entry_dt=entry_dt,
                atr=float(atr_val) if atr_val else None,
                shares=1.0,   # Full mode: 1 share (normalised P&L per share)
                ohlc=ticker_ohlc,
                params=params,
                mention_ratio_series=mention_series,
                sentiment_series=sentiment_series,
            )
            if result is None:
                continue

            pnls.append(result["pnl"])
            hold_days.append(result["hold_days"])
            exit_counts[result["exit_reason"]] = exit_counts.get(result["exit_reason"], 0) + 1

        if len(pnls) < 3:
            continue

        arr = pd.Series(pnls)
        results.append(BacktestResult(
            params=dict(zip(keys, combo)),
            total_pnl=round(float(arr.sum()), 2),
            win_rate=round(float((arr > 0).mean()), 3),
            sharpe=round(float(arr.mean() / (arr.std() + 1e-9)) * (252 ** 0.5), 3),
            trades=len(pnls),
            avg_hold_days=round(sum(hold_days) / max(len(hold_days), 1), 2),
            exit_breakdown=exit_counts,
        ))

    return sorted(results, key=lambda r: r.sharpe, reverse=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def results_to_dataframe(results: list[BacktestResult]) -> pd.DataFrame:
    """Convert BacktestResult list to a flat DataFrame for display."""
    if not results:
        return pd.DataFrame()
    rows = []
    for r in results:
        row = {**r.params,
               "total_pnl":    r.total_pnl,
               "win_rate":     r.win_rate,
               "sharpe":       r.sharpe,
               "trades":       r.trades,
               "avg_hold_days": r.avg_hold_days}
        row.update({f"exit_{k.lower()}": v for k, v in r.exit_breakdown.items()})
        rows.append(row)
    return pd.DataFrame(rows)


def count_combinations(param_grid: dict[str, list]) -> int:
    """Count total grid search combinations."""
    n = 1
    for v in param_grid.values():
        n *= len(v)
    return n


# ── Walk-Forward Optimization ─────────────────────────────────────────────────

@dataclass
class WFOWindow:
    """Results for one IS+OOS window in a walk-forward run."""
    window_num: int
    is_start:   date
    is_end:     date       # exclusive
    oos_start:  date
    oos_end:    date       # exclusive
    best_params:          dict[str, Any]
    is_sharpe:            float
    is_annualised_return: float   # mean_pnl/std_pnl×√252 on IS trades
    oos_result:           BacktestResult | None
    oos_annualised_return: float  # same metric on OOS trades
    wfe: float | None     # WFO efficiency = oos_ann_ret / is_ann_ret × 100


@dataclass
class WFOResult:
    """Aggregated walk-forward optimization results."""
    windows:            list[WFOWindow]
    avg_wfe:            float        # mean WFE across windows with valid IS+OOS
    wfe_std:            float        # WFE std dev (target < 30)
    positive_wfe_pct:   float        # % windows where OOS > 0
    oos_sharpe:         float        # Sharpe of concatenated OOS PnLs
    is_sharpe:          float        # mean IS Sharpe across windows
    oos_win_rate:       float
    oos_total_pnl:      float
    oos_trades:         int
    param_stability:    dict[str, dict]  # per param: {cv, values, min, max}
    warnings:           list[str]


def _build_wfo_windows(
    earliest: date,
    latest:   date,
    is_days:  int,
    oos_days: int,
) -> list[tuple[date, date, date, date]]:
    """
    Build non-overlapping WFO windows rolling by oos_days.

    Returns list of (is_start, is_end, oos_start, oos_end) tuples
    where *_end dates are exclusive (Python slice convention).
    """
    windows = []
    is_start = earliest
    while True:
        is_end   = is_start + timedelta(days=is_days)
        oos_start = is_end
        oos_end   = oos_start + timedelta(days=oos_days)
        if oos_end > latest:
            break
        windows.append((is_start, is_end, oos_start, oos_end))
        is_start = is_start + timedelta(days=oos_days)   # roll by OOS period
    return windows


def _annualised_return(pnls: list[float]) -> float:
    """Annualised return proxy: mean/std × √252 (same formula as Sharpe here)."""
    if len(pnls) < 2:
        return 0.0
    arr = pd.Series(pnls)
    std = float(arr.std())
    return float(arr.mean() / (std + 1e-9)) * (252 ** 0.5)


def run_wfo(
    data_df:     pd.DataFrame,
    ohlc_data:   dict[str, dict[str, pd.DataFrame]],
    param_grid:  dict[str, list],
    fixed_params: dict[str, Any],
    mode:        str = "fast",   # "fast" | "full"
    is_days:     int = 28,
    oos_days:    int = 14,
    total_days:  int = 90,
    sentiment_df: pd.DataFrame | None = None,
) -> WFOResult:
    """
    Walk-Forward Optimization orchestrator.

    For each rolling window:
      1. Grid search on IS slice → best params by Sharpe
      2. Apply best params (as fixed, single-combo grid) to OOS slice
      3. Record IS and OOS metrics

    Aggregates all OOS windows to produce WFOResult with efficiency
    ratio, parameter stability analysis, and combined equity curve.

    Args:
        data_df:     trades_df (fast) or signals_df (full)
        mode:        "fast" or "full"
        is_days:     in-sample window length in calendar days
        oos_days:    out-of-sample window length in calendar days
        total_days:  total lookback from today
    """
    from datetime import datetime as _dt  # noqa: PLC0415
    import statistics  # noqa: PLC0415

    today    = date.today()
    earliest = today - timedelta(days=total_days)
    windows  = _build_wfo_windows(earliest, today, is_days, oos_days)

    date_col = "opened_at" if mode == "fast" else "generated_at"

    wfo_windows: list[WFOWindow] = []
    all_oos_pnls: list[float] = []
    warnings: list[str] = []

    # Pre-compute warnings
    n_windows = len(windows)
    is_oos_ratio = is_days / oos_days
    if n_windows < 5:
        warnings.append(
            f"Only {n_windows} windows — results have limited statistical significance "
            f"(minimum recommended: 5). Consider extending total history."
        )
    if is_oos_ratio < 4.0:
        warnings.append(
            f"IS:OOS ratio is {is_oos_ratio:.1f}:1 (recommended ≥4:1). "
            f"Optimization may be noisy with insufficient IS data."
        )

    for idx, (is_start, is_end, oos_start, oos_end) in enumerate(windows):
        # ── IS: grid search ──────────────────────────────────────────────────
        if mode == "fast":
            is_results = run_fast_backtest(
                trades_df=data_df, ohlc_data=ohlc_data,
                param_grid=param_grid, fixed_params=fixed_params,
                date_from=is_start, date_to=is_end,
            )
        else:
            is_results = run_full_backtest(
                signals_df=data_df, ohlc_data=ohlc_data,
                param_grid=param_grid, fixed_params=fixed_params,
                sentiment_df=sentiment_df,
                date_from=is_start, date_to=is_end,
            )

        if not is_results:
            wfo_windows.append(WFOWindow(
                window_num=idx + 1,
                is_start=is_start, is_end=is_end,
                oos_start=oos_start, oos_end=oos_end,
                best_params={}, is_sharpe=0.0,
                is_annualised_return=0.0,
                oos_result=None, oos_annualised_return=0.0, wfe=None,
            ))
            continue

        best_is = is_results[0]

        # ── OOS: single run with best params ─────────────────────────────────
        oos_grid = {k: [v] for k, v in best_is.params.items()}
        if mode == "fast":
            oos_results = run_fast_backtest(
                trades_df=data_df, ohlc_data=ohlc_data,
                param_grid=oos_grid, fixed_params=fixed_params,
                date_from=oos_start, date_to=oos_end,
            )
        else:
            oos_results = run_full_backtest(
                signals_df=data_df, ohlc_data=ohlc_data,
                param_grid=oos_grid, fixed_params=fixed_params,
                sentiment_df=sentiment_df,
                date_from=oos_start, date_to=oos_end,
            )

        oos_result   = oos_results[0] if oos_results else None
        oos_ann_ret  = oos_result.sharpe if oos_result else 0.0
        is_ann_ret   = best_is.sharpe

        wfe: float | None = None
        if is_ann_ret != 0 and oos_result is not None:
            wfe = (oos_ann_ret / abs(is_ann_ret)) * 100.0

        if oos_result is not None:
            # Warn if OOS window has too few trades
            if oos_result.trades < 30:
                warnings.append(
                    f"Window {idx+1}: only {oos_result.trades} OOS trades "
                    f"(min 30 recommended for reliable metrics)."
                )
            # Accumulate OOS PnLs (via total_pnl as proxy — actual per-trade
            # PnLs not stored in BacktestResult, but total_pnl suffices for
            # combined equity tracking)
            all_oos_pnls.append(oos_result.total_pnl)

        wfo_windows.append(WFOWindow(
            window_num=idx + 1,
            is_start=is_start, is_end=is_end,
            oos_start=oos_start, oos_end=oos_end,
            best_params=best_is.params,
            is_sharpe=best_is.sharpe,
            is_annualised_return=is_ann_ret,
            oos_result=oos_result,
            oos_annualised_return=oos_ann_ret,
            wfe=wfe,
        ))

    # ── Aggregate ─────────────────────────────────────────────────────────────
    valid_wfe    = [w.wfe for w in wfo_windows if w.wfe is not None]
    avg_wfe      = float(statistics.mean(valid_wfe)) if valid_wfe else 0.0
    wfe_std      = float(statistics.stdev(valid_wfe)) if len(valid_wfe) >= 2 else 0.0
    positive_pct = sum(1 for v in valid_wfe if v > 0) / max(len(valid_wfe), 1) * 100.0

    valid_oos    = [w.oos_result for w in wfo_windows if w.oos_result is not None]
    oos_sharpe   = float(pd.Series([r.sharpe for r in valid_oos]).mean()) if valid_oos else 0.0
    is_sharpe_m  = float(pd.Series([w.is_sharpe for w in wfo_windows if w.is_sharpe]).mean()) if wfo_windows else 0.0
    oos_win_rate = float(pd.Series([r.win_rate for r in valid_oos]).mean()) if valid_oos else 0.0
    oos_pnl      = sum(r.total_pnl for r in valid_oos)
    oos_trades   = sum(r.trades for r in valid_oos)

    if avg_wfe < 50 and valid_wfe:
        warnings.append(
            f"WFE {avg_wfe:.1f}% < 50%: strategy shows significant IS→OOS degradation. "
            f"Consider reducing the number of optimized parameters."
        )
    if wfe_std > 50 and len(valid_wfe) >= 2:
        warnings.append(
            f"WFE std dev {wfe_std:.1f}% > 50%: efficiency is highly inconsistent "
            f"across windows — strategy may not generalise across market regimes."
        )

    # ── Parameter stability ───────────────────────────────────────────────────
    param_stability: dict[str, dict] = {}
    param_keys = list(param_grid.keys())
    for k in param_keys:
        values_list = [w.best_params.get(k) for w in wfo_windows if w.best_params.get(k) is not None]
        if not values_list:
            continue
        try:
            vals_f = [float(v) for v in values_list]
            mean_v = statistics.mean(vals_f)
            std_v  = statistics.stdev(vals_f) if len(vals_f) >= 2 else 0.0
            cv     = std_v / abs(mean_v) if mean_v != 0 else 0.0
            param_stability[k] = {
                "values": values_list,
                "mean":   round(mean_v, 4),
                "std":    round(std_v, 4),
                "cv":     round(cv, 3),
                "min":    min(vals_f),
                "max":    max(vals_f),
                "stable": cv <= 0.30,
            }
            if cv > 0.30:
                fmt = PARAM_DEFS.get(k, {}).get("fmt", str)
                warnings.append(
                    f"Parameter '{PARAM_DEFS.get(k, {}).get('label', k)}' is unstable "
                    f"(CV={cv:.2f}, values: {[fmt(v) for v in values_list]}). "
                    f"Consider fixing this parameter."
                )
        except (TypeError, ValueError):
            pass

    # De-duplicate warnings
    seen: set[str] = set()
    deduped_warnings = []
    for w in warnings:
        if w not in seen:
            seen.add(w)
            deduped_warnings.append(w)

    return WFOResult(
        windows=wfo_windows,
        avg_wfe=round(avg_wfe, 1),
        wfe_std=round(wfe_std, 1),
        positive_wfe_pct=round(positive_pct, 1),
        oos_sharpe=round(oos_sharpe, 3),
        is_sharpe=round(is_sharpe_m, 3),
        oos_win_rate=round(oos_win_rate, 3),
        oos_total_pnl=round(oos_pnl, 2),
        oos_trades=oos_trades,
        param_stability=param_stability,
        warnings=deduped_warnings,
    )
