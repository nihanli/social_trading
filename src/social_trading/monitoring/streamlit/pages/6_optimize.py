"""
Page 6 — Parameter Optimization.

Four tabs:
  1. Run History      — compare all recorded sessions, cumulative P&L
  2. Sensitivity      — scatter: any config parameter vs any performance metric
  3. Auto-Suggestions — rule-based recommendations from design §17c
  4. Grid Search      — walk-forward re-simulation of stop/take-profit params

Design reference: docs/design/17-parameter-optimization.md
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import itertools
import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from social_trading.monitoring.streamlit.utils.db import query
from social_trading.monitoring.streamlit.utils.redis_ctrl import load_config, save_config

st.set_page_config(page_title="Optimization", page_icon="🔬", layout="wide")
st.title("Parameter Optimization")
st.caption("Analyze past trading sessions to improve configuration settings.")

# ── Manual snapshot trigger ───────────────────────────────────────────────────
from social_trading.monitoring.streamlit.utils.db import get_connection  # noqa: E402
from social_trading.monitoring.streamlit.utils.redis_ctrl import load_config  # noqa: E402

with st.expander("💾 Force Save Today's Snapshot", expanded=False):
    st.caption(
        "The execution service writes a snapshot automatically after market close. "
        "Use this button if the service was stopped before the EOD window fired."
    )
    snap_mode = "live"
    st.caption("Mode: live")

    # Warn if today is not a NYSE trading day
    from social_trading.core.market_hours import NYSE as _NYSE_ui  # noqa: E402
    from datetime import datetime, timezone as _tz  # noqa: E402
    _today_is_session = _NYSE_ui.is_session_day(datetime.now(_tz.utc))
    if not _today_is_session:
        st.warning("⚠️ Today is not a NYSE trading day (weekend or holiday). Saving a snapshot will record zero-trade metrics and pollute run history.")

    if st.button("Save Snapshot Now", disabled=not _today_is_session):
        try:
            import os, json as _json, hashlib  # noqa: E401
            from datetime import datetime, timezone
            from dataclasses import asdict

            # Load current config from Redis (sync helper used by all Streamlit pages)
            cfg_obj = load_config()

            # Compute today's metrics from DB
            conn = get_connection()
            today = datetime.now(timezone.utc).date().isoformat()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*), SUM(net_pnl),
                           SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END),
                           AVG(EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600),
                           SUM(CASE WHEN exit_reason='TAKE_PROFIT'        THEN 1 ELSE 0 END),
                           SUM(CASE WHEN exit_reason='TIME_STOP'          THEN 1 ELSE 0 END),
                           SUM(CASE WHEN exit_reason='STOP_LOSS'          THEN 1 ELSE 0 END),
                           SUM(CASE WHEN exit_reason='TRAILING_STOP'      THEN 1 ELSE 0 END),
                           SUM(CASE WHEN exit_reason='SENTIMENT_REVERSAL' THEN 1 ELSE 0 END),
                           SUM(CASE WHEN exit_reason='MENTION_DECAY'      THEN 1 ELSE 0 END),
                           SUM(CASE WHEN exit_reason NOT IN (
                               'TAKE_PROFIT','TIME_STOP','STOP_LOSS',
                               'TRAILING_STOP','SENTIMENT_REVERSAL','MENTION_DECAY'
                           ) THEN 1 ELSE 0 END),
                           STDDEV(net_pnl)
                    FROM trades
                    WHERE closed_at::date = %s AND mode = %s AND exit_price IS NOT NULL
                """, (today, snap_mode))
                r = cur.fetchone()
                total_trades = int(r[0] or 0)
                total_pnl    = float(r[1] or 0)
                win_count    = int(r[2] or 0)
                avg_hold_hrs = float(r[3] or 0)
                pnl_std      = float(r[11] or 0)
                win_rate     = win_count / total_trades if total_trades else None
                sharpe       = (total_pnl / total_trades / pnl_std * (252**0.5)) if total_trades and pnl_std else None

                # P&L list for max drawdown
                cur.execute("SELECT net_pnl FROM trades WHERE closed_at::date=%s AND mode=%s AND exit_price IS NOT NULL ORDER BY closed_at", (today, snap_mode))
                pnls = [float(x[0]) for x in cur.fetchall() if x[0] is not None]
                peak = max_dd = 0.0
                cum = 0.0
                for p in pnls:
                    cum += p; peak = max(peak, cum)
                    max_dd = max(max_dd, (peak - cum) / (abs(peak) + 1e-9))
                pf_val = None
                gw = sum(p for p in pnls if p > 0); gl = sum(abs(p) for p in pnls if p < 0)
                if gl > 0: pf_val = round(gw / gl, 4)

                cur.execute("""
                    SELECT COUNT(*), SUM(CASE WHEN executed THEN 1 ELSE 0 END),
                           AVG(quality_score), AVG(mention_zscore)
                    FROM signals WHERE generated_at::date=%s
                """, (today,))
                s = cur.fetchone()

                # Write to config_runs
                cfg_json = _json.dumps(asdict(cfg_obj))
                cfg_hash = hashlib.md5(cfg_json.encode()).hexdigest()[:16]

                def _clamp(v, lo, hi, decimals):
                    if v is None: return None
                    return round(max(lo, min(hi, v)), decimals)

                db_win_rate    = _clamp(win_rate,       0,    1,    4)
                db_sharpe      = _clamp(sharpe,        -999, 999,   4)
                db_max_dd      = _clamp(max_dd,         0,    1,    4)
                db_hold_hrs    = _clamp(avg_hold_hrs,   0, 9999,    2)
                db_pf          = _clamp(pf_val,         0, 9999,    4)
                db_avg_qual    = _clamp(float(s[2]) if s[2] else None, 0, 9, 4)
                db_avg_zscore  = _clamp(float(s[3]) if s[3] else None, -9999, 9999, 2)

                vals = (
                    snap_mode, cfg_json, cfg_hash,
                    round(total_pnl, 2), total_trades, win_count, db_win_rate,
                    db_sharpe, db_max_dd, db_hold_hrs, db_pf,
                    int(r[4] or 0), int(r[5] or 0), int(r[6] or 0),
                    int(r[7] or 0), int(r[8] or 0), int(r[9] or 0), int(r[10] or 0),
                    int(s[0] or 0), int(s[1] or 0), db_avg_qual, db_avg_zscore,
                )
                cur.execute("""
                    INSERT INTO config_runs (
                        run_date, mode, config_snapshot, config_hash,
                        total_pnl, total_trades, win_count, win_rate,
                        sharpe_ratio, max_drawdown, avg_hold_hours, profit_factor,
                        exits_take_profit, exits_time_stop, exits_atr_stop,
                        exits_trailing_stop, exits_sentiment_reversal, exits_mention_decay,
                        exits_manual, signals_generated, signals_executed,
                        avg_signal_quality, avg_mention_zscore
                    ) VALUES (
                        CURRENT_DATE, %s, %s::jsonb, %s,
                        %s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    ON CONFLICT (run_date, mode) DO UPDATE SET
                        config_snapshot=EXCLUDED.config_snapshot,
                        config_hash=EXCLUDED.config_hash,
                        total_pnl=EXCLUDED.total_pnl,
                        total_trades=EXCLUDED.total_trades,
                        win_count=EXCLUDED.win_count,
                        win_rate=EXCLUDED.win_rate,
                        sharpe_ratio=EXCLUDED.sharpe_ratio,
                        max_drawdown=EXCLUDED.max_drawdown,
                        avg_hold_hours=EXCLUDED.avg_hold_hours,
                        profit_factor=EXCLUDED.profit_factor,
                        exits_take_profit=EXCLUDED.exits_take_profit,
                        exits_time_stop=EXCLUDED.exits_time_stop,
                        exits_atr_stop=EXCLUDED.exits_atr_stop,
                        exits_trailing_stop=EXCLUDED.exits_trailing_stop,
                        exits_sentiment_reversal=EXCLUDED.exits_sentiment_reversal,
                        exits_mention_decay=EXCLUDED.exits_mention_decay,
                        exits_manual=EXCLUDED.exits_manual,
                        signals_generated=EXCLUDED.signals_generated,
                        signals_executed=EXCLUDED.signals_executed,
                        avg_signal_quality=EXCLUDED.avg_signal_quality,
                        avg_mention_zscore=EXCLUDED.avg_mention_zscore
                """, vals)
            conn.close()
            st.success(f"✅ Snapshot saved for {today} ({snap_mode}): {total_trades} trades, P&L ${total_pnl:+,.2f}")
            st.rerun()
        except Exception as _snap_exc:
            st.error(f"Failed: {_snap_exc}")

st.divider()


# ── Shared data — all config runs ─────────────────────────────────────────────
runs_df = query("""
    SELECT id, run_date, mode, config_hash,
           total_pnl, total_trades, win_count, win_rate, sharpe_ratio,
           max_drawdown, avg_hold_hours, profit_factor,
           exits_take_profit, exits_time_stop, exits_atr_stop,
           exits_trailing_stop, exits_sentiment_reversal, exits_mention_decay,
           signals_generated, signals_executed, avg_signal_quality,
           config_snapshot
    FROM config_runs
    ORDER BY run_date DESC
""")

_no_data = runs_df.empty

# Flatten config JSON into columns for analysis
def _safe_json(v):
    try:
        return json.loads(v) if v else {}
    except Exception:
        return {}

try:
    parsed = runs_df["config_snapshot"].apply(_safe_json)
    cfg_cols = pd.json_normalize(parsed)
    if cfg_cols.columns.empty:
        cfg_cols = pd.DataFrame()
except Exception:
    cfg_cols = pd.DataFrame()

perf_cols = [
    "run_date", "mode", "config_hash", "total_pnl", "win_rate",
    "sharpe_ratio", "max_drawdown", "avg_hold_hours", "profit_factor",
    "exits_take_profit", "exits_time_stop", "exits_atr_stop",
    "exits_trailing_stop", "exits_sentiment_reversal", "exits_mention_decay",
    "signals_generated", "signals_executed", "avg_signal_quality",
]
if _no_data:
    analysis_df = pd.DataFrame(columns=perf_cols)
else:
    available_perf_cols = [c for c in perf_cols if c in runs_df.columns]
    analysis_df = pd.concat(
        [runs_df[available_perf_cols].reset_index(drop=True), cfg_cols.reset_index(drop=True)],
        axis=1,
    )

tab_history, tab_sensitivity, tab_suggest, tab_grid = st.tabs([
    "Run History",
    "Sensitivity Analysis",
    "Auto-Suggestions",
    "Grid Search",
])

# ════════════════════════════════════════════════════════════════
# TAB 1 — RUN HISTORY
# ════════════════════════════════════════════════════════════════
with tab_history:
    if _no_data:
        st.info(
            "No run history yet. End-of-day snapshots are saved automatically "
            "by the execution service after each trading session."
        )
    else:
        st.subheader("All Recorded Sessions")

        mode_filter = st.radio("Mode", ["All", "live"], horizontal=True, key="hist_mode")
        df = analysis_df if mode_filter == "All" else analysis_df[analysis_df["mode"] == mode_filter]

        if df.empty:
            st.info("No sessions match this filter")
        else:
            # KPIs
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Sessions recorded", len(df))
            c2.metric("Best Sharpe", f"{df['sharpe_ratio'].max():.3f}" if df["sharpe_ratio"].notna().any() else "—")
            c3.metric("Best win rate", f"{df['win_rate'].max():.1%}" if df["win_rate"].notna().any() else "—")
            c4.metric("Best single-session P&L", f"${df['total_pnl'].max():,.0f}" if df["total_pnl"].notna().any() else "—")

            # Cumulative P&L line
            pnl_ts = df[["run_date", "total_pnl"]].dropna().sort_values("run_date")
            if not pnl_ts.empty:
                pnl_ts["cumulative_pnl"] = pnl_ts["total_pnl"].cumsum()
                fig = go.Figure(go.Scatter(
                    x=pnl_ts["run_date"],
                    y=pnl_ts["cumulative_pnl"],
                    fill="tozeroy",
                    line={"color": "#28a745" if pnl_ts["cumulative_pnl"].iloc[-1] >= 0 else "#dc3545"},
                    name="Cumulative P&L",
                ))
                fig.update_layout(title="Cumulative P&L across all sessions",
                                  height=240, margin={"t": 30, "b": 20})
                st.plotly_chart(fig, width='stretch')

            # Full table
            display_cols = ["run_date", "mode", "config_hash", "total_pnl", "win_rate",
                            "sharpe_ratio", "avg_hold_hours", "signals_generated", "signals_executed"]
            existing = [c for c in display_cols if c in df.columns]
            st.dataframe(df[existing].sort_values("run_date", ascending=False),
                         width='stretch', hide_index=True)


# ════════════════════════════════════════════════════════════════
# TAB 2 — SENSITIVITY ANALYSIS
# ════════════════════════════════════════════════════════════════
with tab_sensitivity:
    if _no_data:
        st.info("No run history yet — sensitivity analysis requires at least one recorded session.")
    elif cfg_cols.empty:
        st.warning("Config snapshot data not available")
    else:
        st.subheader("Parameter vs Performance Metric")
        st.caption("Select any config parameter and performance metric to see correlation.")

        numeric_cfg_cols = cfg_cols.select_dtypes("number").columns.tolist()
        perf_metrics = [c for c in ["total_pnl", "win_rate", "sharpe_ratio",
                                    "max_drawdown", "avg_hold_hours", "profit_factor"]
                        if c in analysis_df.columns]

        col1, col2 = st.columns(2)
        param_col = col1.selectbox("Config parameter (X axis)", numeric_cfg_cols,
                                   index=numeric_cfg_cols.index("signal_phase1_threshold")
                                   if "signal_phase1_threshold" in numeric_cfg_cols else 0)
        metric_col = col2.selectbox("Performance metric (Y axis)", perf_metrics)

        scatter_df = analysis_df[[param_col, metric_col, "mode", "run_date"]].dropna()
        if not scatter_df.empty:
            fig = px.scatter(
                scatter_df,
                x=param_col,
                y=metric_col,
                color="mode",
                hover_data=["run_date"],
                trendline="ols",
                title=f"{param_col} vs {metric_col}",
            )
            fig.update_layout(height=380, margin={"t": 40, "b": 20})
            st.plotly_chart(fig, width='stretch')

            # Correlation coefficient
            if len(scatter_df) >= 3:
                corr = scatter_df[[param_col, metric_col]].corr().iloc[0, 1]
                strength = "strong" if abs(corr) > 0.6 else "moderate" if abs(corr) > 0.3 else "weak"
                direction = "positive" if corr > 0 else "negative"
                st.metric(f"Pearson correlation ({param_col} → {metric_col})",
                          f"{corr:.3f}",
                          help=f"{strength.capitalize()} {direction} correlation")
        else:
            st.info("Not enough data for this combination")


# ════════════════════════════════════════════════════════════════
# TAB 3 — AUTO-SUGGESTIONS
# ════════════════════════════════════════════════════════════════
with tab_suggest:
    if _no_data:
        st.info("No run history yet — auto-suggestions require at least one recorded session.")
    else:
        st.subheader("Automatic Parameter Suggestions")
        st.caption("Rule-based analysis of the last N sessions. Design §17c.")

        n_sessions = st.slider("Sessions to analyse", 5, 50, 10, key="suggest_n")
        mode_s = st.radio("Mode", ["live", "All"], horizontal=True, key="suggest_mode")

        sub = analysis_df if mode_s == "All" else analysis_df[analysis_df["mode"] == mode_s]
        recent = sub.sort_values("run_date", ascending=False).head(n_sessions)

        if len(recent) < 3:
            st.info(f"Need at least 3 sessions to generate suggestions (have {len(recent)}).")
        else:
            suggestions: list[dict] = []
            cfg_now = load_config()

            # Shared variable used by multiple rules — compute once
            avg_total = recent["total_trades"].mean() if "total_trades" in recent.columns else 0

            # ── Rule set from design §17c ─────────────────────────────────────
            if "win_rate" in recent.columns:
                avg_wr = recent["win_rate"].mean()
                if pd.notna(avg_wr) and avg_wr < 0.45:
                    suggestions.append({
                        "symptom": f"Low win rate ({avg_wr:.1%})",
                        "suggestion": "Raise Phase 1 signal quality threshold",
                        "parameter": "signal_phase1_threshold",
                        "current": cfg_now.signal_phase1_threshold,
                        "recommended": round(min(cfg_now.signal_phase1_threshold + 0.05, 0.90), 2),
                        "severity": "warning",
                    })

            if "exits_time_stop" in recent.columns and avg_total > 0:
                avg_ts = recent["exits_time_stop"].mean()
                if pd.notna(avg_ts) and avg_ts / avg_total > 0.5:
                    suggestions.append({
                        "symptom": f"Time stop dominates exits ({avg_ts / avg_total:.0%})",
                        "suggestion": "Signals decay before target — reduce take_profit_pct or max_hold_trading_days",
                        "parameter": "take_profit_pct",
                        "current": cfg_now.take_profit_pct,
                        "recommended": round(max(cfg_now.take_profit_pct * 0.8, 0.01), 3),
                        "severity": "info",
                    })

            if "exits_atr_stop" in recent.columns and avg_total > 0:
                avg_atr_exits = recent["exits_atr_stop"].mean()
                if pd.notna(avg_atr_exits) and avg_atr_exits / avg_total > 0.4:
                    suggestions.append({
                        "symptom": f"ATR stops dominate exits ({avg_atr_exits / avg_total:.0%})",
                        "suggestion": "Stops too tight — increase ATR multiplier",
                        "parameter": "atr_multiplier",
                        "current": cfg_now.atr_multiplier,
                        "recommended": round(cfg_now.atr_multiplier + 0.5, 1),
                        "severity": "warning",
                    })

            if "exits_sentiment_reversal" in recent.columns and avg_total > 0:
                avg_sr = recent["exits_sentiment_reversal"].mean()
                if pd.notna(avg_sr) and avg_sr / avg_total > 0.25:
                    suggestions.append({
                        "symptom": f"Sentiment reversals common ({avg_sr / avg_total:.0%})",
                        "suggestion": "Tighten entry bar — raise Phase 1 signal quality threshold",
                        "parameter": "signal_phase1_threshold",
                        "current": cfg_now.signal_phase1_threshold,
                        "recommended": round(min(cfg_now.signal_phase1_threshold + 0.05, 0.90), 2),
                        "severity": "warning",
                    })

            if "sharpe_ratio" in recent.columns:
                avg_sharpe = recent["sharpe_ratio"].mean()
                if pd.notna(avg_sharpe) and avg_sharpe < 0.0:
                    suggestions.append({
                        "symptom": f"Negative average Sharpe ({avg_sharpe:.3f})",
                        "suggestion": "Strategy not working — halt live trading and review all parameters",
                        "parameter": "N/A (manual review required)",
                        "current": "—",
                        "recommended": "Pause live trading and review settings",
                        "severity": "critical",
                    })

            if "signals_executed" in recent.columns and "signals_generated" in recent.columns:
                exec_rate = (recent["signals_executed"].sum() / max(recent["signals_generated"].sum(), 1))
                if exec_rate < 0.10:
                    suggestions.append({
                        "symptom": f"Very low signal execution rate ({exec_rate:.1%})",
                        "suggestion": "Circuit breaker firing too often — review loss_limit_daily",
                        "parameter": "loss_limit_daily",
                        "current": cfg_now.loss_limit_daily,
                        "recommended": round(min(cfg_now.loss_limit_daily * 1.25, 0.10), 3),
                        "severity": "info",
                    })

            if not suggestions:
                st.success(f"No issues detected across the last {len(recent)} sessions. Settings look healthy.")
            else:
                for s in suggestions:
                    icon = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}.get(s["severity"], "•")
                    st.markdown(f"**{icon} {s['symptom']}**")
                    st.markdown(
                        f"- Suggestion: {s['suggestion']}\n"
                        f"- Parameter: `{s['parameter']}`\n"
                        f"- Current: `{s['current']}` → Recommended: `{s['recommended']}`"
                    )
                    st.divider()


# ════════════════════════════════════════════════════════════════
# TAB 4 — WALK-FORWARD BACKTEST (replaces old grid search)
# ════════════════════════════════════════════════════════════════
with tab_grid:
    import itertools  # noqa: E402

    from social_trading.monitoring.streamlit.utils.backtest import (  # noqa: E402
        PARAM_DEFS,
        BacktestResult,
        count_combinations,
        load_ohlc,
        results_to_dataframe,
        run_fast_backtest,
        run_full_backtest,
    )

    st.subheader("Walk-Forward Backtest")
    st.caption(
        "Re-simulates trades using pre-stored OHLC bars and real ATR. "
        "**Fast Mode** replays executed trades only. "
        "**Full Mode** replays all signals through an entry filter and exit simulation."
    )

    # Pre-compute OHLC coverage (needed by expander expanded= and coverage bar)
    ohlc_count_df = query("""
        SELECT COUNT(DISTINCT ticker) AS n FROM price_ohlc WHERE timeframe = '1d'
    """)
    ohlc_tickers = int(ohlc_count_df.iloc[0]["n"]) if not ohlc_count_df.empty else 0

    # ── OHLC backfill ─────────────────────────────────────────────────────────
    with st.expander("📥 Backfill OHLC price data", expanded=(ohlc_tickers == 0)):
        st.caption(
            "The background price task populates OHLC bars for new signals automatically. "
            "Use this to backfill historical tickers that existed before this feature was added."
        )
        if st.button("Fetch OHLC for all signal tickers (last 90 days)", key="bt_backfill"):
            with st.spinner("Checking coverage and fetching missing OHLC data…"):
                try:
                    import yfinance as yf  # noqa: PLC0415
                    from datetime import timezone as _tz  # noqa: PLC0415

                    conn_bf = get_connection()

                    with conn_bf.cursor() as cur:
                        # Tickers needing daily bars: no 1d coverage or stale (> 1 day old)
                        cur.execute("""
                            SELECT s.ticker
                            FROM (
                                SELECT DISTINCT ticker FROM signals
                                WHERE generated_at > NOW() - INTERVAL '90 days'
                            ) s
                            LEFT JOIN (
                                SELECT ticker, MAX(bar_datetime::date) AS last_date
                                FROM price_ohlc WHERE timeframe = '1d'
                                GROUP BY ticker
                            ) p ON s.ticker = p.ticker
                            WHERE p.last_date IS NULL OR p.last_date < CURRENT_DATE - 1
                        """)
                        tickers_need_daily = [r[0] for r in cur.fetchall()]

                        # (ticker, signal_date) pairs missing 5m intraday bars
                        cur.execute("""
                            SELECT DISTINCT s.ticker, s.generated_at::date AS sig_date
                            FROM signals s
                            WHERE s.generated_at > NOW() - INTERVAL '90 days'
                              AND NOT EXISTS (
                                  SELECT 1 FROM price_ohlc p
                                  WHERE p.ticker    = s.ticker
                                    AND p.timeframe = '5m'
                                    AND p.bar_datetime::date = s.generated_at::date
                              )
                            ORDER BY sig_date
                        """)
                        missing_5m = [(r[0], r[1]) for r in cur.fetchall()]
                    conn_bf.commit()

                    total_daily = len(tickers_need_daily)
                    total_5m    = len(missing_5m)
                    st.write(
                        f"**{total_daily}** tickers need daily bars · "
                        f"**{total_5m}** signal dates need 5-min bars"
                    )

                    if not tickers_need_daily and not missing_5m:
                        st.success("✅ Coverage complete — nothing to fetch.")
                    else:
                        def _flatten(df):
                            if isinstance(df.columns, pd.MultiIndex):
                                df.columns = df.columns.get_level_values(0)
                            return df

                        upserted_total = 0
                        progress_bf = st.progress(0)
                        total_work = total_daily + total_5m

                        # ── Daily bars ────────────────────────────────────────
                        for idx, ticker in enumerate(tickers_need_daily):
                            progress_bf.progress(
                                (idx + 1) / total_work, text=f"Daily: {ticker}"
                            )
                            try:
                                df_d = _flatten(yf.download(
                                    ticker, period="90d", interval="1d",
                                    progress=False, auto_adjust=True,
                                ))
                                if df_d.empty:
                                    continue
                                with conn_bf.cursor() as cur:
                                    cur.execute("SAVEPOINT ticker_sp")
                                    try:
                                        for ts, row in df_d.iterrows():
                                            dt = ts.to_pydatetime()
                                            if dt.tzinfo is None:
                                                dt = dt.replace(tzinfo=_tz.utc)
                                            try:
                                                cur.execute(
                                                    """
                                                    INSERT INTO price_ohlc
                                                        (ticker, bar_datetime, timeframe,
                                                         open, high, low, close, volume, source)
                                                    VALUES (%s,%s,'1d',%s,%s,%s,%s,%s,'yfinance')
                                                    ON CONFLICT (ticker, bar_datetime, timeframe)
                                                    DO UPDATE SET
                                                        open=EXCLUDED.open, high=EXCLUDED.high,
                                                        low=EXCLUDED.low, close=EXCLUDED.close,
                                                        volume=EXCLUDED.volume, fetched_at=NOW()
                                                    WHERE price_ohlc.source != 'ib'
                                                    """,
                                                    (ticker, dt,
                                                     float(row["Open"]), float(row["High"]),
                                                     float(row["Low"]),  float(row["Close"]),
                                                     int(row["Volume"]) if row.get("Volume") else None),
                                                )
                                                upserted_total += cur.rowcount
                                            except (TypeError, ValueError):
                                                continue
                                        cur.execute("RELEASE SAVEPOINT ticker_sp")
                                        conn_bf.commit()
                                    except Exception as exc:
                                        cur.execute("ROLLBACK TO SAVEPOINT ticker_sp")
                                        conn_bf.commit()
                                        raise exc
                            except Exception as exc:
                                st.warning(f"{ticker} (daily): {exc}")

                        # ── 5-min bars (only dates within yfinance 7-day window) ──
                        import datetime as _dt_mod  # noqa: PLC0415
                        today_utc = _dt_mod.datetime.now(_tz.utc).date()

                        # Group missing dates by ticker to minimise yfinance calls
                        from collections import defaultdict  # noqa: PLC0415
                        ticker_dates: dict = defaultdict(list)
                        for ticker, sig_date in missing_5m:
                            age = (today_utc - sig_date).days
                            if age <= 7:
                                ticker_dates[ticker].append(sig_date)

                        skipped_old = total_5m - sum(len(v) for v in ticker_dates.values())
                        if skipped_old:
                            st.info(
                                f"ℹ️ {skipped_old} signal date(s) older than 7 days skipped "
                                "(IB backfill runs automatically via background service)."
                            )

                        for idx, (ticker, dates) in enumerate(ticker_dates.items()):
                            progress_bf.progress(
                                (total_daily + idx + 1) / total_work,
                                text=f"5m: {ticker}",
                            )
                            try:
                                df_5m = _flatten(yf.download(
                                    ticker, period="7d", interval="5m",
                                    progress=False, auto_adjust=True,
                                ))
                                if df_5m.empty:
                                    continue
                                target_dates = set(dates)
                                with conn_bf.cursor() as cur:
                                    cur.execute("SAVEPOINT ticker_5m_sp")
                                    try:
                                        for ts, row in df_5m.iterrows():
                                            dt = ts.to_pydatetime()
                                            if dt.tzinfo is None:
                                                dt = dt.replace(tzinfo=_tz.utc)
                                            if dt.date() not in target_dates:
                                                continue
                                            try:
                                                cur.execute(
                                                    """
                                                    INSERT INTO price_ohlc
                                                        (ticker, bar_datetime, timeframe,
                                                         open, high, low, close, volume, source)
                                                    VALUES (%s,%s,'5m',%s,%s,%s,%s,%s,'yfinance')
                                                    ON CONFLICT (ticker, bar_datetime, timeframe)
                                                    DO UPDATE SET
                                                        open=EXCLUDED.open, high=EXCLUDED.high,
                                                        low=EXCLUDED.low, close=EXCLUDED.close,
                                                        volume=EXCLUDED.volume, fetched_at=NOW()
                                                    WHERE price_ohlc.source != 'ib'
                                                    """,
                                                    (ticker, dt,
                                                     float(row["Open"]), float(row["High"]),
                                                     float(row["Low"]),  float(row["Close"]),
                                                     int(row["Volume"]) if row.get("Volume") else None),
                                                )
                                                upserted_total += cur.rowcount
                                            except (TypeError, ValueError):
                                                continue
                                        cur.execute("RELEASE SAVEPOINT ticker_5m_sp")
                                        conn_bf.commit()
                                    except Exception as exc:
                                        cur.execute("ROLLBACK TO SAVEPOINT ticker_5m_sp")
                                        conn_bf.commit()
                                        raise exc
                            except Exception as exc:
                                st.warning(f"{ticker} (5m): {exc}")

                        progress_bf.empty()
                        st.success(
                            f"✅ Backfill complete: **{upserted_total}** bars upserted. "
                            "Reload the page to see updated coverage."
                        )
                except Exception as exc:
                    st.error(f"Backfill failed: {exc}")

    # ── Mode & date range ─────────────────────────────────────────────────────
    col_m1, col_m2, col_m3 = st.columns([2, 2, 2])
    bt_mode = col_m1.radio(
        "Backtest mode",
        [
            "Fast (executed trades only)",
            "Full (all signals, ~30s)",
            "WFO-Fast (walk-forward, executed trades)",
            "WFO-Full (walk-forward, all signals)",
        ],
        horizontal=False,
        key="bt_mode",
    )
    is_full = bt_mode.startswith("Full")
    is_wfo  = bt_mode.startswith("WFO")
    is_wfo_full = bt_mode == "WFO-Full (walk-forward, all signals)"

    days_bt = col_m2.slider("Days of history", 14, 90, 90, key="bt_days")
    bt_trade_mode = col_m3.radio("Trading mode filter", ["live", "All"], horizontal=True, key="bt_tm")

    mode_clause = "" if bt_trade_mode == "All" else f"AND mode = '{bt_trade_mode}'"

    # ── Contrarian population filter ──────────────────────────────────────────
    st.caption(
        "**Signal population**: choose which trades/signals to include. "
        "Mixing normal and contrarian trades in one optimisation produces "
        "misleading results because the two strategies have opposite price dynamics."
    )
    contrarian_filter = st.radio(
        "Signal population",
        ["Normal only", "Contrarian only", "All (mixed — not recommended)"],
        horizontal=True,
        key="bt_contrarian_filter",
        help=(
            "Normal = signals generated before contrarian mode was enabled (direction follows sentiment). "
            "Contrarian = signals generated with contrarian_mode=True (direction is inverted). "
            "Mixing both will optimise for a blended population that may not reflect either strategy."
        ),
    )
    if contrarian_filter == "Normal only":
        contrarian_clause_signals = "AND contrarian IS NOT TRUE"
        contrarian_clause_trades  = (
            "AND (SELECT s.contrarian "
            "     FROM signals s WHERE s.id = t.signal_id) IS NOT TRUE"
        )
        bt_is_contrarian = False
    elif contrarian_filter == "Contrarian only":
        contrarian_clause_signals = "AND contrarian = TRUE"
        contrarian_clause_trades  = (
            "AND (SELECT s.contrarian "
            "     FROM signals s WHERE s.id = t.signal_id) = TRUE"
        )
        bt_is_contrarian = True
    else:
        contrarian_clause_signals = ""
        contrarian_clause_trades  = ""
        bt_is_contrarian = None  # mixed — warn below
        st.warning(
            "⚠️ **Mixed population** — optimisation results will reflect a blend of "
            "normal and contrarian trades. Parameter recommendations may not be valid "
            "for either strategy independently."
        )

    # ── WFO window config (shown only for WFO modes) ──────────────────────────
    if is_wfo:
        st.markdown("#### Walk-Forward Window Configuration")
        wfo_col1, wfo_col2, wfo_col3 = st.columns(3)
        wfo_is_days  = wfo_col1.number_input("In-sample (days)", min_value=7, max_value=60, value=28, step=7, key="wfo_is")
        wfo_oos_days = wfo_col2.number_input("Out-of-sample (days)", min_value=7, max_value=30, value=14, step=7, key="wfo_oos")

        from social_trading.monitoring.streamlit.utils.backtest import _build_wfo_windows  # noqa: E402
        from datetime import date as _date  # noqa: E402
        _today    = _date.today()
        _earliest = _today - __import__("datetime").timedelta(days=days_bt)
        _est_wins = len(_build_wfo_windows(_earliest, _today, int(wfo_is_days), int(wfo_oos_days)))

        ratio = wfo_is_days / wfo_oos_days
        ratio_color = "🟢" if ratio >= 4 else ("🟡" if ratio >= 2 else "🔴")
        win_color   = "🟢" if _est_wins >= 5 else ("🟡" if _est_wins >= 3 else "🔴")
        wfo_col3.metric("Estimated windows", f"{win_color} {_est_wins}")
        st.caption(
            f"IS:OOS ratio: {ratio_color} **{ratio:.1f}:1** "
            f"{'(recommended ≥4:1)' if ratio < 4 else '✅'} · "
            f"OOS coverage: **{_est_wins * int(wfo_oos_days)} days** of honest out-of-sample data"
        )

    # ── Data coverage summary ─────────────────────────────────────────────────
    if is_full or is_wfo_full:
        sig_count_df = query(f"""
            SELECT COUNT(*) AS n FROM signals
            WHERE generated_at > NOW() - INTERVAL '{days_bt} days'
              {contrarian_clause_signals}
        """)
        sig_count = int(sig_count_df.iloc[0]["n"]) if not sig_count_df.empty else 0
    else:
        sig_count = 0

    trade_count_df = query(f"""
        SELECT COUNT(*) AS n FROM trades t
        WHERE closed_at > NOW() - INTERVAL '{days_bt} days'
          AND exit_price IS NOT NULL {mode_clause}
          {contrarian_clause_trades}
    """)
    trade_count = int(trade_count_df.iloc[0]["n"]) if not trade_count_df.empty else 0

    if is_full or is_wfo_full:
        coverage_txt = (
            f"✅ **{sig_count}** signals · **{trade_count}** trades in range · "
            f"**{ohlc_tickers}** tickers with OHLC data"
        )
    else:
        coverage_txt = (
            f"✅ **{trade_count}** trades in range · "
            f"**{ohlc_tickers}** tickers with OHLC data"
        )
    st.info(coverage_txt)

    # ── Parameter checkboxes ──────────────────────────────────────────────────
    st.markdown("#### Parameters to optimise")
    col_hdr1, col_hdr2, col_hdr3 = st.columns([1, 4, 1])
    col_hdr1.markdown("**Include**")
    col_hdr2.markdown("**Values to test**")
    col_hdr3.markdown("**Mode**")

    selected_params: dict[str, list] = {}
    use_full_params = is_full or is_wfo_full

    for param_key, defn in PARAM_DEFS.items():
        modes_txt = "Fast+Full" if defn["modes"] == {"fast", "full"} else "Full only"
        available = use_full_params or "fast" in defn["modes"]
        col1, col2, col3 = st.columns([1, 4, 1])
        enabled = col1.checkbox(
            " ", key=f"bt_chk_{param_key}",
            value=available,
            disabled=not available,
            label_visibility="collapsed",
        )
        vals = col2.multiselect(
            defn["label"],
            defn["options"],
            default=defn["default"],
            format_func=defn["fmt"],
            key=f"bt_vals_{param_key}",
            disabled=not (available and enabled),
            label_visibility="visible",
        )
        col3.caption(modes_txt)
        if available and enabled and vals:
            selected_params[param_key] = vals

    # ── Combination count warning ─────────────────────────────────────────────
    n_combos = count_combinations(selected_params) if selected_params else 0
    if n_combos == 0:
        st.warning("Select at least one parameter to test.")
    elif n_combos > 10000:
        st.error(f"**{n_combos} combinations** — reduce to ≤ 10,000 before running.")
    elif n_combos > 1000:
        st.warning(f"⚠️ {n_combos} combinations — may take 30–60 s in Full mode.")
    else:
        st.success(f"✅ **{n_combos} combinations**")

    cfg_bt = load_config()
    fixed_params = {
        "trailing_stop_min_pct": getattr(cfg_bt, "trailing_stop_min_pct", 0.02),
        "signal_phase1_threshold": cfg_bt.signal_phase1_threshold,
        "mention_decay_threshold": getattr(cfg_bt, "mention_decay_threshold", 0.25),
        "sentiment_reversal_threshold": getattr(cfg_bt, "sentiment_reversal_threshold", -0.25),
        # These serve as "current value" when not in the grid
        "take_profit_pct":  cfg_bt.take_profit_pct,
        "atr_multiplier":   cfg_bt.atr_multiplier,
        "trailing_stop_pct": getattr(cfg_bt, "trailing_stop_pct", 0.07),
        "max_hold_days":    getattr(cfg_bt, "max_hold_trading_days", 3),
        # Pass contrarian flag so _simulate_trade inverts sentiment-reversal check correctly.
        # None (mixed) defaults to False — least surprising behaviour for blended data.
        "contrarian": bool(bt_is_contrarian) if bt_is_contrarian is not None else False,
    }

    can_run = 0 < n_combos <= 10000 and (trade_count >= 3 or ((is_full or is_wfo_full) and sig_count >= 3))

    if st.button("▶ Run Backtest", type="primary", disabled=not can_run, key="bt_run"):
        conn_bt = get_connection()
        progress = st.progress(0, text="Loading data…")

        # Load trade / signal data
        if is_full or is_wfo_full:
            raw_df = query(f"""
                SELECT ticker, direction, quality_score, atr, generated_at,
                       sentiment_score
                FROM signals
                WHERE generated_at > NOW() - INTERVAL '{days_bt} days'
                  {contrarian_clause_signals}
                ORDER BY generated_at
            """)
        else:
            raw_df = query(f"""
                SELECT t.ticker, t.direction, t.shares, t.entry_price,
                       t.atr_at_entry, t.opened_at
                FROM trades t
                WHERE t.closed_at > NOW() - INTERVAL '{days_bt} days'
                  AND t.exit_price IS NOT NULL
                  {mode_clause}
                  {contrarian_clause_trades}
                ORDER BY t.opened_at
            """)

        progress.progress(20, text="Loading OHLC bars…")
        tickers = raw_df["ticker"].unique().tolist() if not raw_df.empty else []
        ohlc_data = load_ohlc(tickers, conn_bt)

        progress.progress(40, text="Running simulation…")

        if raw_df.empty or len(raw_df) < 3:
            st.warning("Not enough data for the selected range and mode.")
            progress.empty()
        elif is_wfo:
            from social_trading.monitoring.streamlit.utils.backtest import run_wfo  # noqa: E402
            wfo_mode = "full" if is_wfo_full else "fast"
            wfo_result = run_wfo(
                data_df=raw_df,
                ohlc_data=ohlc_data,
                param_grid=selected_params,
                fixed_params=fixed_params,
                mode=wfo_mode,
                is_days=int(wfo_is_days),
                oos_days=int(wfo_oos_days),
                total_days=days_bt,
            )
            progress.progress(100, text="Done.")
            progress.empty()
            if wfo_result.windows:
                st.session_state["wfo_result"] = wfo_result
                st.session_state.pop("bt_results", None)
                st.success(f"WFO complete — {len(wfo_result.windows)} windows, {wfo_result.oos_trades} OOS trades.")
            else:
                st.warning("No valid WFO windows produced. Try increasing total history or reducing window sizes.")
        else:
            if not (is_full or is_wfo_full):
                bt_results = run_fast_backtest(
                    trades_df=raw_df,
                    ohlc_data=ohlc_data,
                    param_grid=selected_params,
                    fixed_params=fixed_params,
                )
            else:
                bt_results = run_full_backtest(
                    signals_df=raw_df,
                    ohlc_data=ohlc_data,
                    param_grid=selected_params,
                    fixed_params=fixed_params,
                )

            progress.progress(100, text="Done.")
            progress.empty()

            if bt_results:
                st.session_state["bt_results"] = bt_results
                st.session_state["bt_selected_params"] = list(selected_params.keys())
                st.session_state.pop("wfo_result", None)
                st.success(f"Completed {len(bt_results)} combinations.")
            else:
                st.warning("No valid combinations (need ≥ 3 simulated trades per combo).")

    # ── WFO Results ───────────────────────────────────────────────────────────
    if "wfo_result" in st.session_state:
        from social_trading.monitoring.streamlit.utils.backtest import WFOResult  # noqa: E402
        wfo: WFOResult = st.session_state["wfo_result"]

        st.subheader("Walk-Forward Optimization Results")

        # Warnings
        for warn in wfo.warnings:
            icon = "🔴" if "significant" in warn.lower() or "only" in warn.lower() else "⚠️"
            st.warning(f"{icon} {warn}")

        # Summary metrics
        wfe_color = "🟢" if wfo.avg_wfe >= 60 else ("🟡" if wfo.avg_wfe >= 40 else "🔴")
        std_color = "🟢" if wfo.wfe_std < 30 else ("🟡" if wfo.wfe_std < 50 else "🔴")
        pos_color = "🟢" if wfo.positive_wfe_pct >= 60 else ("🟡" if wfo.positive_wfe_pct >= 40 else "🔴")

        mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
        mc1.metric("Avg WFE", f"{wfe_color} {wfo.avg_wfe:.1f}%", help="Target ≥60%")
        mc2.metric("WFE Std Dev", f"{std_color} {wfo.wfe_std:.1f}%", help="Target <30%")
        mc3.metric("Positive OOS", f"{pos_color} {wfo.positive_wfe_pct:.0f}%", help="Target ≥60%")
        mc4.metric("OOS Sharpe", f"{wfo.oos_sharpe:.3f}", delta=f"IS: {wfo.is_sharpe:.3f}")
        mc5.metric("OOS P&L", f"${wfo.oos_total_pnl:,.2f}")
        mc6.metric("OOS Trades", str(wfo.oos_trades))

        st.divider()

        # Per-window table
        st.markdown("#### Per-Window Results")
        win_rows = []
        for w in wfo.windows:
            row = {
                "Window":     w.window_num,
                "IS Period":  f"{w.is_start} → {w.is_end}",
                "OOS Period": f"{w.oos_start} → {w.oos_end}",
                "IS Sharpe":  f"{w.is_sharpe:.3f}",
                "OOS Sharpe": f"{w.oos_result.sharpe:.3f}" if w.oos_result else "—",
                "OOS Trades": w.oos_result.trades if w.oos_result else 0,
                "WFE %":      f"{w.wfe:.1f}%" if w.wfe is not None else "—",
            }
            for k, v in w.best_params.items():
                fmt = PARAM_DEFS.get(k, {}).get("fmt", str)
                row[PARAM_DEFS.get(k, {}).get("label", k)] = fmt(v)
            win_rows.append(row)
        st.dataframe(pd.DataFrame(win_rows), use_container_width=True, hide_index=True)

        # OOS equity curve (cumulative OOS P&L per window)
        oos_pnls = [w.oos_result.total_pnl if w.oos_result else 0.0 for w in wfo.windows]
        if any(p != 0 for p in oos_pnls):
            st.markdown("#### Combined OOS Equity Curve")
            eq_df = pd.DataFrame({
                "Window": [f"W{w.window_num}\n{w.oos_start}" for w in wfo.windows],
                "OOS P&L": oos_pnls,
                "Cumulative P&L": pd.Series(oos_pnls).cumsum().tolist(),
            })
            fig_eq = px.bar(
                eq_df, x="Window", y="OOS P&L",
                color="OOS P&L",
                color_continuous_scale=["red", "lightgray", "green"],
                color_continuous_midpoint=0,
                title="OOS P&L per Window",
            )
            fig_line = go.Figure()
            fig_line.add_trace(go.Scatter(
                x=eq_df["Window"], y=eq_df["Cumulative P&L"],
                mode="lines+markers", name="Cumulative OOS P&L",
                line={"color": "steelblue", "width": 2},
            ))
            fig_line.update_layout(title="Cumulative OOS Equity", height=280)
            col_eq1, col_eq2 = st.columns(2)
            col_eq1.plotly_chart(fig_eq, use_container_width=True)
            col_eq2.plotly_chart(fig_line, use_container_width=True)

        # Parameter stability
        if wfo.param_stability:
            st.markdown("#### Parameter Stability Across Windows")
            stab_rows = []
            for k, info in wfo.param_stability.items():
                fmt = PARAM_DEFS.get(k, {}).get("fmt", str)
                stab_rows.append({
                    "Parameter": PARAM_DEFS.get(k, {}).get("label", k),
                    "Stability": "✅ Stable" if info["stable"] else "⚠️ Unstable",
                    "CV": f"{info['cv']:.2f}",
                    "Values": " → ".join(fmt(v) for v in info["values"]),
                    "Range": f"{fmt(info['min'])} – {fmt(info['max'])}",
                })
            st.dataframe(pd.DataFrame(stab_rows), use_container_width=True, hide_index=True)

        # Apply — use mode (most frequent) params across windows
        st.divider()
        st.markdown("#### Apply Recommended Settings")

        # Modal params: most frequent value per param across windows
        from collections import Counter  # noqa: E402
        param_map = {
            "take_profit_pct":            "take_profit_pct",
            "atr_multiplier":             "atr_multiplier",
            "trailing_stop_pct":          "trailing_stop_pct",
            "max_hold_days":              "max_hold_trading_days",
            "signal_phase1_threshold":    "signal_phase1_threshold",
            "mention_decay_threshold":    "mention_decay_threshold",
            "sentiment_reversal_threshold": "sentiment_reversal_threshold",
        }
        modal_params = {}
        for k in (wfo.param_stability or {}):
            if k in param_map:
                vals = [w.best_params.get(k) for w in wfo.windows if w.best_params.get(k) is not None]
                if vals:
                    modal_params[k] = Counter(vals).most_common(1)[0][0]

        if modal_params:
            st.caption("Recommended = most frequent optimal value across all windows (more robust than single-window best).")
            changes_md = "\n".join(
                f"- **{PARAM_DEFS.get(k, {}).get('label', k)}**: "
                f"`{getattr(cfg_bt, param_map[k], '?')}` → `{PARAM_DEFS.get(k, {}).get('fmt', str)(v)}`"
                for k, v in modal_params.items()
            )
            with st.expander("✅ Apply recommended settings to live config", expanded=False):
                st.warning("**This will update live trading parameters immediately. Running positions are not affected.**")
                st.markdown(changes_md)
                confirm_wfo = st.checkbox("I understand this changes live trading behaviour", key="wfo_confirm")
                if st.button("Apply", type="primary", key="wfo_apply", disabled=not confirm_wfo):
                    cfg_apply = load_config()
                    for k, v in modal_params.items():
                        setattr(cfg_apply, param_map[k], v)
                    errs = save_config(cfg_apply)
                    if errs:
                        for e in errs:
                            st.error(e)
                    else:
                        st.success("Settings applied. Services pick up within ~1 minute.")
                        del st.session_state["wfo_result"]
                        st.rerun()

    # ── Standard Grid Search Results ──────────────────────────────────────────
    if "bt_results" in st.session_state:
        bt_results: list[BacktestResult] = st.session_state["bt_results"]
        bt_keys: list[str] = st.session_state.get("bt_selected_params", [])
        res_df = results_to_dataframe(bt_results)

        # Auto-select top-2 most impactful params for heatmap axes
        if len(bt_keys) >= 2 and not res_df.empty:
            # Impact = Pearson correlation with Sharpe
            impacts = {}
            for k in bt_keys:
                if k in res_df.columns and res_df[k].nunique() > 1:
                    try:
                        impacts[k] = abs(res_df[k].astype(float).corr(res_df["sharpe"]))
                    except Exception:
                        impacts[k] = 0.0
            top2 = sorted(impacts, key=lambda x: impacts[x], reverse=True)[:2]
        elif len(bt_keys) == 1:
            top2 = bt_keys[:1]
        else:
            top2 = []

        if len(top2) == 2:
            pivot = res_df.groupby(top2)["sharpe"].mean().unstack()
            if not pivot.empty:
                fig_heat = px.imshow(
                    pivot,
                    title=f"Sharpe Heatmap ({top2[0]} × {top2[1]})",
                    color_continuous_scale="RdYlGn",
                    aspect="auto",
                    text_auto=".2f",
                )
                fig_heat.update_layout(height=350)
                st.plotly_chart(fig_heat, use_container_width=True)

        # Full results table
        display_cols = bt_keys + ["sharpe", "win_rate", "total_pnl", "trades", "avg_hold_days"]
        display_cols = [c for c in display_cols if c in res_df.columns]
        st.dataframe(
            res_df[display_cols].style.format({
                "sharpe": "{:.3f}", "win_rate": "{:.1%}",
                "total_pnl": "${:.2f}", "avg_hold_days": "{:.1f}",
            }),
            use_container_width=True,
            hide_index=True,
        )

        # Best result summary
        best = bt_results[0]
        st.subheader("Best combination found")
        best_cols = st.columns(len(best.params) + 3)
        for i, (k, v) in enumerate(best.params.items()):
            fmt = PARAM_DEFS.get(k, {}).get("fmt", str)
            best_cols[i].metric(PARAM_DEFS.get(k, {}).get("label", k), fmt(v))
        best_cols[-3].metric("Sharpe", f"{best.sharpe:.3f}")
        best_cols[-2].metric("Win rate", f"{best.win_rate:.1%}")
        best_cols[-1].metric("Trades", str(best.trades))

        # ── Apply with confirmation ───────────────────────────────────────────
        param_map = {
            "take_profit_pct":            "take_profit_pct",
            "atr_multiplier":             "atr_multiplier",
            "trailing_stop_pct":          "trailing_stop_pct",
            "max_hold_days":              "max_hold_trading_days",
            "signal_phase1_threshold":    "signal_phase1_threshold",
            "mention_decay_threshold":    "mention_decay_threshold",
            "sentiment_reversal_threshold": "sentiment_reversal_threshold",
        }
        applicable = {k: v for k, v in best.params.items() if k in param_map}

        if applicable:
            changes_md = "\n".join(
                f"- **{PARAM_DEFS.get(k, {}).get('label', k)}**: "
                f"`{getattr(cfg_bt, param_map[k], '?')}` → `{PARAM_DEFS.get(k, {}).get('fmt', str)(v)}`"
                for k, v in applicable.items()
            )

            with st.expander("✅ Apply best settings to live config", expanded=False):
                st.warning(
                    "**This will update the following live parameters immediately. "
                    "Running positions are not affected — only future trades.**"
                )
                st.markdown(changes_md)
                confirm = st.checkbox(
                    "I understand this changes live trading behaviour", key="bt_confirm"
                )
                if st.button(
                    "Apply", type="primary", key="bt_apply",
                    disabled=not confirm,
                ):
                    cfg_apply = load_config()
                    for k, v in applicable.items():
                        setattr(cfg_apply, param_map[k], v)
                    errs = save_config(cfg_apply)
                    if errs:
                        for e in errs:
                            st.error(e)
                    else:
                        st.success(
                            "Settings applied to Redis config. "
                            "Services will pick up within ~1 minute."
                        )
                        del st.session_state["bt_results"]
                        st.rerun()
