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

tab_history, tab_sensitivity, tab_suggest, tab_grid = st.tabs([
    "Run History",
    "Sensitivity Analysis",
    "Auto-Suggestions",
    "Grid Search",
])

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
try:
    cfg_cols = pd.json_normalize(runs_df["config_snapshot"].apply(json.loads))
except Exception:
    cfg_cols = pd.DataFrame()

perf_cols = [
    "run_date", "mode", "config_hash", "total_pnl", "win_rate",
    "sharpe_ratio", "max_drawdown", "avg_hold_hours", "profit_factor",
    "exits_take_profit", "exits_time_stop", "exits_atr_stop",
    "exits_trailing_stop", "exits_sentiment_reversal", "exits_mention_decay",
    "signals_generated", "signals_executed", "avg_signal_quality",
]
analysis_df = pd.concat(
    [runs_df[perf_cols].reset_index(drop=True), cfg_cols.reset_index(drop=True)],
    axis=1,
)

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

        mode_filter = st.radio("Mode", ["All", "paper", "live"], horizontal=True, key="hist_mode")
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
        mode_s = st.radio("Mode", ["paper", "live", "All"], horizontal=True, key="suggest_mode")

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
                        "suggestion": "Signals decay before target — reduce take_profit_pct or max_hold_hours",
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
                        "recommended": "Switch to paper mode",
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
# TAB 4 — GRID SEARCH
# ════════════════════════════════════════════════════════════════
with tab_grid:
    st.subheader("Walk-Forward Grid Search")
    st.caption(
        "Re-simulates recently executed trades with different stop/take-profit settings. "
        "Only uses trades that were actually executed — no look-ahead bias. "
        "Design §17d."
    )

    col_gf1, col_gf2 = st.columns(2)
    days_grid = col_gf1.slider("Use last N days of trades", 7, 90, 30, key="grid_days")
    grid_mode = col_gf2.radio("Mode", ["paper", "live", "All"], horizontal=True, key="grid_mode")

    mode_clause = "" if grid_mode == "All" else f"AND mode = '{grid_mode}'"
    trades_df = query(f"""
        SELECT ticker, direction, shares, entry_price,
               exit_price, net_pnl, opened_at, closed_at
        FROM trades
        WHERE closed_at IS NOT NULL
          AND exit_price IS NOT NULL
          AND closed_at > NOW() - INTERVAL '{days_grid} days'
          {mode_clause}
        ORDER BY closed_at
    """)

    if trades_df.empty or len(trades_df) < 5:
        st.info(f"Need at least 5 trades in the last {days_grid} days to run grid search")
    else:
        st.write(f"Base dataset: **{len(trades_df)} trades**")

        # Grid parameters
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            tp_range = st.multiselect(
                "Take-profit % values to test",
                [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10],
                default=[0.03, 0.04, 0.05],
                format_func=lambda x: f"{x:.0%}",
            )
        with col_g2:
            sl_range = st.multiselect(
                "ATR multiplier values to test",
                [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0],
                default=[1.5, 2.0, 2.5],
            )

        combos = list(itertools.product(tp_range, sl_range))
        st.write(f"**{len(combos)} combinations** to test")

        if len(combos) > 500:
            st.error("Reduce to ≤ 500 combinations before running")
        elif combos and st.button("Run Grid Search", type="primary"):
            results = []
            progress = st.progress(0)

            for i, (tp, atr_mult) in enumerate(combos):
                progress.progress((i + 1) / len(combos))

                pnls = []
                for _, row in trades_df.iterrows():
                    ep = float(row["entry_price"])
                    if ep <= 0:
                        continue
                    direction = row["direction"]
                    exit_p = float(row["exit_price"])
                    qty = float(row["shares"])
                    cost_bps = 0.002  # 20 bps round-trip

                    # Simulated stop & target prices
                    # Use 2% of entry as ATR proxy (actual ATR not stored per-trade).
                    atr_approx = ep * 0.02
                    if direction == "LONG":
                        stop   = ep - atr_mult * atr_approx
                        target = ep * (1 + tp)
                        # Clamp exit to [stop, target]: hit stop if went lower, hit target if went higher
                        simulated_exit = max(min(exit_p, target), stop)
                        raw_pnl = (simulated_exit - ep) * qty
                    else:
                        stop   = ep + atr_mult * atr_approx
                        target = ep * (1 - tp)
                        # SHORT clamp: target is below entry, stop is above entry
                        simulated_exit = min(max(exit_p, target), stop)
                        raw_pnl = (ep - simulated_exit) * qty

                    pnls.append(raw_pnl - abs(ep * qty) * cost_bps)

                if len(pnls) < 5:
                    continue

                arr = pd.Series(pnls)
                results.append({
                    "take_profit": f"{tp:.0%}",
                    "atr_mult": atr_mult,
                    "total_pnl": round(arr.sum(), 2),
                    "win_rate": round((arr > 0).mean(), 3),
                    "sharpe": round((arr.mean() / (arr.std() + 1e-9)) * (252**0.5), 3),
                    "trades": len(pnls),
                })

            progress.empty()

            if results:
                res_df = pd.DataFrame(results).sort_values("sharpe", ascending=False)
                st.session_state["grid_results"] = res_df
                st.success(f"Completed {len(results)} valid combinations")
            else:
                st.warning("No valid combinations produced (need ≥ 5 trades per combination)")

        # Results and Apply button rendered outside the run-button block so
        # clicking "Apply" doesn't wipe results on rerun.
        if "grid_results" in st.session_state:
            res_df = st.session_state["grid_results"]

            # Heatmap
            pivot = res_df.pivot_table(
                values="sharpe",
                index="atr_mult",
                columns="take_profit",
                aggfunc="mean",
            )
            if not pivot.empty:
                fig = px.imshow(
                    pivot,
                    title="Sharpe Ratio Heatmap (ATR mult × Take-profit)",
                    color_continuous_scale="RdYlGn",
                    aspect="auto",
                    text_auto=".2f",
                )
                fig.update_layout(height=350)
                st.plotly_chart(fig, width='stretch')

            st.dataframe(res_df, width='stretch', hide_index=True)

            best = res_df.iloc[0]
            st.subheader("Best combination found")
            st.write(f"- Take-profit: **{best['take_profit']}**")
            st.write(f"- ATR multiplier: **{best['atr_mult']}**")
            st.write(f"- Sharpe: **{best['sharpe']:.3f}** | Win rate: **{best['win_rate']:.1%}**")

            if st.button("Apply best settings to config", type="primary", key="apply_grid"):
                cfg_apply = load_config()
                cfg_apply.take_profit_pct = float(best["take_profit"].rstrip("%")) / 100
                cfg_apply.atr_multiplier = float(best["atr_mult"])
                errs = save_config(cfg_apply)
                if errs:
                    for e in errs:
                        st.error(e)
                else:
                    st.success("Settings applied to Redis config. Services will pick up within ~1 minute.")
                    del st.session_state["grid_results"]

    mode_filter = st.radio("Mode", ["All", "paper", "live"], horizontal=True, key="hist_mode")
