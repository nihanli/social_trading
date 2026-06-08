"""
Social Trading Monitor — Main Dashboard (app.py)

Entry point for the Streamlit control panel.
Run: streamlit run src/social_trading/monitoring/streamlit/app.py

Displays:
  - Sidebar: system controls, circuit breaker, halt/resume, emergency close
  - KPI row: equity, daily P&L, open positions, win rate, signals→trades
  - 30-day equity curve
  - Open positions table + per-position close buttons
  - Recent signals feed
  - Sentiment heatmap (last hour)
  - Recent closed trades

Design reference: docs/design/15-ui-monitoring.md §15b
"""
from __future__ import annotations

import os
import sys
import subprocess

# Ensure src/ is on the path when running via `streamlit run`
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../"))

import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.db import query, localize_datetimes
from social_trading.monitoring.streamlit.utils.redis_ctrl import (
    close_all_positions,
    close_position,
    get_phase_pipeline_stats,
    get_system_state,
    halt_new_trades,
    resume_trading,
)
from social_trading.monitoring.streamlit.utils.refresh_countdown import (
    sidebar_refresh_countdown,
)
from social_trading.monitoring.streamlit.utils.company_info import enrich_tickers
from social_trading.monitoring.streamlit.utils.table_html import render_table

st.set_page_config(
    page_title="Social Trading Monitor",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Auto-refresh this page every 15 seconds (stays on current page — no browser redirect)
st_autorefresh(interval=15_000, key="main_dashboard_refresh")

# ═══════════════════════════════════════
# SIDEBAR — System Controls
# ═══════════════════════════════════════
sidebar_refresh_countdown()

with st.sidebar:
    st.title("System Controls")

    state = get_system_state()

    mode_color = "🟢" if state["mode"] == "live" else "🟡"
    st.markdown(f"**Mode:** {mode_color} {state['mode'].upper()}")

    ib_raw = state.get("ib_connected")
    if state["mode"] == "live":
        if ib_raw is None:
            ib_icon, ib_label = "⚪", "Unknown (service offline)"
        elif ib_raw == "1":
            ib_icon, ib_label = "🟢", "Connected"
        else:
            ib_icon, ib_label = "🔴", "Disconnected"
        st.markdown(f"**IB Status:** {ib_icon} {ib_label}")

    cb_color = {
        "NORMAL": "🟢",
        "REDUCED_50": "🟠",
        "DAILY_HALT": "🔴",
        "FULL_HALT": "🚨",
    }.get(state["circuit"], "⚪")
    st.markdown(f"**Circuit Breaker:** {cb_color} {state['circuit']}")

    st.divider()

    # Halt / Resume toggle
    if state["halt_new"]:
        st.warning("New trades HALTED")
        if st.button("Resume Trading", width='stretch'):
            resume_trading()
            st.success("Resume command sent")
            st.rerun()
    else:
        if st.button("Halt New Trades", width='stretch', type="primary"):
            halt_new_trades()
            st.warning("Halt command sent")
            st.rerun()

    st.divider()

    with st.expander("Emergency Actions", expanded=False):
        st.warning("Immediate and irreversible.")
        if st.button("Close ALL Positions", width='stretch'):
            close_all_positions()
            st.error("Close-all command sent to execution engine")

        st.divider()
        st.markdown("**Stop All Services**")
        if st.button("🛑 Stop All Services", width='stretch', type="secondary"):
            st.session_state["_confirm_stop_services"] = True

        if st.session_state.get("_confirm_stop_services"):
            st.error("This will terminate ALL services including this UI. Continue?")
            col_yes, col_no = st.columns(2)
            if col_yes.button("Yes, stop everything", type="primary"):
                st.session_state.pop("_confirm_stop_services", None)
                _stop_script = os.path.normpath(os.path.join(
                    os.path.dirname(__file__), "../../../../stop.sh"
                ))
                try:
                    subprocess.Popen(["bash", _stop_script])
                    st.error("☠️ Shutdown signal sent — all services terminating…")
                except Exception as _e:
                    st.error(f"Failed to run stop.sh: {_e}")
            if col_no.button("Cancel"):
                st.session_state.pop("_confirm_stop_services", None)
                st.rerun()

    st.divider()

    pnl_color = "normal" if state["daily_pnl_pct"] > -2 else "inverse"
    st.metric("Daily P&L", f"{state['daily_pnl_pct']:+.2f}%", delta_color=pnl_color)

    st.markdown("**Drawdown from HWM**")
    st.progress(
        min(abs(state["drawdown"]) / 0.20, 1.0),
        text=f"{abs(state['drawdown']):.1%}",
    )

    st.metric("VIX", f"{state['vix']:.1f}",
              help="Live VIX — affects position size scalars")

# ═══════════════════════════════════════
# MAIN — KPI Row
# ═══════════════════════════════════════
st.title("Social Trading Monitor")

equity_df = query(
    "SELECT equity FROM account_equity ORDER BY timestamp DESC LIMIT 1"
)
daily_pnl_df = query("""
    SELECT COALESCE(SUM(net_pnl), 0) AS pnl,
           COUNT(*) FILTER (WHERE net_pnl > 0) AS wins,
           COUNT(*) AS total
    FROM trades
    WHERE opened_at::date = CURRENT_DATE
""")
open_pos_df = query("SELECT COUNT(*) AS cnt FROM positions")
signals_today_df = query("""
    SELECT COUNT(*) FILTER (WHERE executed)               AS executed,
           COUNT(*)                                       AS total,
           COUNT(*) FILTER (WHERE signal_phase = 'phase1') AS phase1,
           COUNT(*) FILTER (WHERE signal_phase = 'phase2') AS phase2
    FROM signals
    WHERE generated_at::date = CURRENT_DATE
""")

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric(
    "Portfolio Equity",
    f"${equity_df.iloc[0, 0]:,.0f}" if not equity_df.empty else f"${state['net_liquidation']:,.0f}",
)
col2.metric(
    "Today's P&L",
    f"${daily_pnl_df.iloc[0]['pnl']:+,.0f}" if not daily_pnl_df.empty else "—",
)
col3.metric("Open Positions", int(open_pos_df.iloc[0, 0]) if not open_pos_df.empty else 0)
col4.metric(
    "Win Rate Today",
    f"{100 * daily_pnl_df.iloc[0]['wins'] / max(daily_pnl_df.iloc[0]['total'], 1):.0f}%"
    if not daily_pnl_df.empty and daily_pnl_df.iloc[0]["total"] > 0 else "—",
)
if not signals_today_df.empty:
    _s = signals_today_df.iloc[0]
    col5.metric(
        "Signals Today",
        f"{int(_s['total'])}  (P1:{int(_s['phase1'] or 0)} P2:{int(_s['phase2'] or 0)})",
        help="Total signals today — breakdown by phase in parentheses",
    )
    col6.metric(
        "Signals Executed",
        f"{int(_s['executed'])}/{int(_s['total'])}",
    )
else:
    col5.metric("Signals Today", "—")
    col6.metric("Signals Executed", "—")

# ═══════════════════════════════════════
# EQUITY CURVE
# ═══════════════════════════════════════
eq_hist = query("""
    SELECT timestamp, equity
    FROM account_equity
    WHERE timestamp > NOW() - INTERVAL '30 days'
    ORDER BY timestamp
""")
if not eq_hist.empty:
    localize_datetimes(eq_hist)
    fig = go.Figure(go.Scatter(
        x=eq_hist["timestamp"],
        y=eq_hist["equity"],
        fill="tozeroy",
        line={"color": "#2196F3", "width": 2},
        name="Equity",
    ))
    fig.update_layout(
        title="Portfolio Equity — 30 days",
        height=220,
        margin={"t": 30, "b": 20, "l": 10, "r": 10},
        xaxis_title=None,
        yaxis_title="USD",
    )
    st.plotly_chart(fig, width='stretch')
else:
    st.info("No equity history yet — starts recording after first trade.")

# ═══════════════════════════════════════
# OPEN POSITIONS + RECENT SIGNALS
# ═══════════════════════════════════════
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Open Positions")
    positions = query("""
        SELECT ticker,
               '/chart?ticker=' || ticker || '&tf=6M' AS chart,
               direction, shares, entry_price,
               ROUND(unrealized_pnl::numeric, 2)      AS unrealized_pnl,
               ROUND((unrealized_pnl /
                 NULLIF(entry_price * shares, 0) * 100)::numeric, 1) AS pnl_pct,
               TO_CHAR(opened_at, 'HH24:MI') AS opened
        FROM positions
        ORDER BY opened_at DESC
    """)
    if positions.empty:
        st.info("No open positions")
    else:
        _pos_tips = enrich_tickers(positions["ticker"].tolist())
        render_table(
            positions,
            tooltips={"ticker": _pos_tips},
            link_cols={"chart": ("📈", "_blank")},
        )
        for _, row in positions.iterrows():
            if st.button(f"Close {row['ticker']}", key=f"close_{row['ticker']}"):
                close_position(row["ticker"])
                st.warning(f"Close order sent for {row['ticker']}")

with col_right:
    st.subheader("Recent Signals")
    signals = query("""
        SELECT ticker,
               '/chart?ticker=' || ticker || '&tf=6M' AS chart,
               direction,
               COALESCE(signal_phase, 'legacy')    AS phase,
               ROUND(confidence::numeric, 2)        AS quality,
               ROUND(sentiment_score::numeric, 2)   AS sentiment,
               ROUND(mention_zscore::numeric, 1)    AS vol_z,
               approved, executed,
               TO_CHAR(generated_at, 'HH24:MI:SS') AS time
        FROM signals
        ORDER BY generated_at DESC
        LIMIT 20
    """)
    if not signals.empty:
        _sig_tips = enrich_tickers(signals["ticker"].unique().tolist())
        render_table(
            signals,
            tooltips={"ticker": _sig_tips},
            link_cols={"chart": ("📈", "_blank")},
            cell_styles={"phase": {
                "phase1": "color:#4A90D9;font-weight:bold",
                "phase2": "color:#2ECC71;font-weight:bold",
                "legacy": "color:#95A5A6",
            }},
        )
    else:
        st.info("No signals recorded yet")

    # Live enrichment queue indicator
    pipeline = get_phase_pipeline_stats()
    eq = pipeline.get("enrichment_queue", 0)
    if eq > 0:
        st.caption(f"⏳ {eq} ticker(s) queued for Tier-2 enrichment (Phase 1 → Phase 2)")

# ═══════════════════════════════════════
# SENTIMENT HEATMAP
# ═══════════════════════════════════════
st.subheader("Sentiment Heatmap — Top Tickers (Last Hour)")
heatmap_df = query("""
    WITH agg AS (
        SELECT ticker,
               SUM(post_count)     AS mentions,
               AVG(weighted_score) AS avg_sentiment
        FROM sentiment_aggregates
        WHERE window_start > NOW() - INTERVAL '1 hour'
          AND window_minutes = 15
        GROUP BY ticker
    )
    SELECT ticker, mentions, avg_sentiment,
           (mentions - AVG(mentions) OVER ())
               / NULLIF(STDDEV_SAMP(mentions) OVER (), 0) AS vol_z
    FROM agg
    ORDER BY mentions DESC
    LIMIT 20
""")
if not heatmap_df.empty:
    fig2 = go.Figure(go.Bar(
        x=heatmap_df["ticker"],
        y=heatmap_df["mentions"],
        marker={
            "color": heatmap_df["avg_sentiment"],
            "colorscale": "RdYlGn",
            "cmin": -1,
            "cmax": 1,
            "colorbar": {"title": "Sentiment", "thickness": 12},
        },
        text=heatmap_df["vol_z"].fillna(0).round(1).astype(str) + "σ",
        textposition="outside",
    ))
    fig2.update_layout(
        title="Bar height = mention count  |  Colour = sentiment  |  Label = volume Z-score",
        height=280,
        margin={"t": 40, "b": 20, "l": 10, "r": 10},
        xaxis_title=None,
        yaxis_title="Mentions",
    )
    st.plotly_chart(fig2, width='stretch')
else:
    st.info("No sentiment data in the last hour")

# ═══════════════════════════════════════
# RECENT CLOSED TRADES
# ═══════════════════════════════════════
st.subheader("Recent Closed Trades")
trades = query("""
    SELECT ticker,
           '/chart?ticker=' || ticker || '&tf=6M' AS chart,
           direction, shares, entry_price, exit_price,
           ROUND(net_pnl::numeric, 2) AS net_pnl,
           exit_reason,
           TO_CHAR(opened_at,  'MM-DD HH24:MI') AS opened,
           TO_CHAR(closed_at,  'MM-DD HH24:MI') AS closed
    FROM trades
    WHERE closed_at IS NOT NULL
    ORDER BY closed_at DESC
    LIMIT 30
""")
if not trades.empty:
    _trade_tips = enrich_tickers(trades["ticker"].unique().tolist())
    render_table(
        trades,
        tooltips={"ticker": _trade_tips},
        link_cols={"chart": ("📈", "_blank")},
    )
else:
    st.info("No closed trades yet")

# ═══════════════════════════════════════
# FOOTNOTE — DB size
# ═══════════════════════════════════════
st.divider()
db_size = query("SELECT ROUND(pg_database_size(current_database()) / 1024.0 / 1024.0, 2) AS size_mb")
if not db_size.empty:
    st.caption(f"🗄️ Database size: {db_size.iloc[0]['size_mb']:.2f} MB")
