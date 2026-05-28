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

# Ensure src/ is on the path when running via `streamlit run`
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../"))

import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.db import query, localize_datetimes
from social_trading.monitoring.streamlit.utils.redis_ctrl import (
    close_all_positions,
    close_position,
    get_system_state,
    halt_new_trades,
    resume_trading,
)
from social_trading.monitoring.streamlit.utils.refresh_countdown import (
    sidebar_refresh_countdown,
)

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
        if st.button("Resume Trading", use_container_width=True):
            resume_trading()
            st.success("Resume command sent")
            st.rerun()
    else:
        if st.button("Halt New Trades", use_container_width=True, type="primary"):
            halt_new_trades()
            st.warning("Halt command sent")
            st.rerun()

    st.divider()

    with st.expander("Emergency Actions", expanded=False):
        st.warning("Immediate and irreversible.")
        if st.button("Close ALL Positions", use_container_width=True):
            close_all_positions()
            st.error("Close-all command sent to execution engine")

    st.divider()

    st.markdown("**Daily P&L**")
    pnl_color = "normal" if state["daily_pnl_pct"] > -2 else "inverse"
    st.metric("", f"{state['daily_pnl_pct']:+.2f}%", delta_color=pnl_color)

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
    SELECT COUNT(*) FILTER (WHERE executed) AS executed,
           COUNT(*) AS total
    FROM signals
    WHERE generated_at::date = CURRENT_DATE
""")

col1, col2, col3, col4, col5 = st.columns(5)
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
col5.metric(
    "Signals Executed",
    f"{signals_today_df.iloc[0]['executed']}/{signals_today_df.iloc[0]['total']}"
    if not signals_today_df.empty else "—",
)

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
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No equity history yet — starts recording after first trade.")

# ═══════════════════════════════════════
# OPEN POSITIONS + RECENT SIGNALS
# ═══════════════════════════════════════
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Open Positions")
    positions = query("""
        SELECT ticker, direction, shares, entry_price,
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
        st.dataframe(positions, use_container_width=True, hide_index=True)
        for _, row in positions.iterrows():
            if st.button(f"Close {row['ticker']}", key=f"close_{row['ticker']}"):
                close_position(row["ticker"])
                st.warning(f"Close order sent for {row['ticker']}")

with col_right:
    st.subheader("Recent Signals")
    signals = query("""
        SELECT ticker, direction,
               ROUND(confidence::numeric, 2)      AS quality,
               ROUND(sentiment_score::numeric, 2) AS sentiment,
               ROUND(mention_zscore::numeric, 1)  AS vol_z,
               approved, executed,
               TO_CHAR(generated_at, 'HH24:MI:SS') AS time
        FROM signals
        ORDER BY generated_at DESC
        LIMIT 20
    """)
    if not signals.empty:
        st.dataframe(signals, use_container_width=True, hide_index=True)
    else:
        st.info("No signals recorded yet")

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
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No sentiment data in the last hour")

# ═══════════════════════════════════════
# RECENT CLOSED TRADES
# ═══════════════════════════════════════
st.subheader("Recent Closed Trades")
trades = query("""
    SELECT ticker, direction, shares, entry_price, exit_price,
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
    st.dataframe(trades, use_container_width=True, hide_index=True)
else:
    st.info("No closed trades yet")

# ═══════════════════════════════════════
# FOOTNOTE — DB size
# ═══════════════════════════════════════
st.divider()
db_size = query("SELECT ROUND(pg_database_size(current_database()) / 1024.0 / 1024.0, 2) AS size_mb")
if not db_size.empty:
    st.caption(f"🗄️ Database size: {db_size.iloc[0]['size_mb']:.2f} MB")
