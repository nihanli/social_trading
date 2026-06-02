"""
Page 1 — Open Positions detail.

Shows all currently open positions with:
  - P&L bar chart per ticker
  - Detailed table (entry, stop, target, hold time)
  - Per-position close button
  - Manual circuit breaker reset option
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

from datetime import UTC, datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.redis_ctrl import (
    close_all_positions,
    close_position,
    get_live_positions,
    get_system_state,
)
from social_trading.monitoring.streamlit.utils.refresh_countdown import (
    sidebar_refresh_countdown,
)

st.set_page_config(page_title="Positions", page_icon="📂", layout="wide")
st_autorefresh(interval=5_000, key="positions_refresh")   # 5s — Redis is real-time
sidebar_refresh_countdown()
st.title("Open Positions")

state = get_system_state()

# ── Circuit breaker status banner ─────────────────────────────────────────────
if state["circuit"] != "NORMAL":
    st.error(f"Circuit breaker active: **{state['circuit']}** — new trades may be blocked")

# ── Load positions from Redis (real-time) ────────────────────────────────────
raw_positions = get_live_positions()

if not raw_positions:
    st.info("No open positions")
    st.stop()

# Build DataFrame from Redis data
now_utc = datetime.now(UTC)
rows = []
for p in raw_positions:
    try:
        opened_at = datetime.fromisoformat(p.get("opened_at", ""))
        hold_hrs = round((now_utc - opened_at).total_seconds() / 3600, 1)
    except Exception:
        hold_hrs = 0.0
    entry = float(p.get("entry_price", 0))
    shares = int(p.get("shares", 0))
    unreal = float(p.get("unrealized_pnl", 0))
    pnl_pct = round(unreal / (entry * shares) * 100, 2) if entry and shares else 0.0
    rows.append({
        "ticker":         p.get("ticker", ""),
        "direction":      p.get("direction", ""),
        "shares":         shares,
        "entry_price":    round(entry, 2),
        "stop_loss":      round(float(p.get("stop_loss", 0)), 2),
        "take_profit":    round(float(p.get("take_profit", 0)), 2),
        "unrealized_pnl": round(unreal, 2),
        "pnl_pct":        pnl_pct,
        "hold_hrs":       hold_hrs,
    })

positions = pd.DataFrame(rows).sort_values("unrealized_pnl", ascending=False)

# KPI row
total_unrealized = positions["unrealized_pnl"].sum()
c1, c2, c3 = st.columns(3)
c1.metric("Open Positions", len(positions))
c2.metric("Total Unrealized P&L", f"${total_unrealized:+,.2f}")
c3.metric(
    "Longest Hold",
    f"{positions['hold_hrs'].max():.1f} hrs" if not positions.empty else "—",
)

# P&L bar chart
fig = px.bar(
    positions,
    x="ticker",
    y="unrealized_pnl",
    color="direction",
    color_discrete_map={"LONG": "#28a745", "SHORT": "#dc3545"},
    text="pnl_pct",
    title="Unrealized P&L by Position",
    labels={"unrealized_pnl": "P&L (USD)", "ticker": "Ticker"},
)
fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
fig.update_layout(height=300, margin={"t": 40, "b": 10})
st.plotly_chart(fig, width='stretch')

# Positions table
st.dataframe(
    positions,
    width='stretch',
    hide_index=True,
)

# ── Per-position close buttons ────────────────────────────────────────────────
st.subheader("Close Individual Positions")
cols = st.columns(min(len(positions), 6))
for i, (_, row) in enumerate(positions.iterrows()):
    with cols[i % 6]:
        pnl_label = f"${row['unrealized_pnl']:+.0f}"
        btn_type = "primary" if row["unrealized_pnl"] < 0 else "secondary"
        if st.button(
            f"Close {row['ticker']} ({pnl_label})",
            key=f"close_{row['ticker']}",
            type=btn_type,
            width='stretch',
        ):
            close_position(row["ticker"])
            st.warning(f"Close command sent for {row['ticker']}")

st.divider()

# ── Emergency close all ───────────────────────────────────────────────────────
with st.expander("Emergency Actions"):
    st.warning("These are immediate and irreversible.")
    if st.button("Close ALL Positions Now", type="primary"):
        close_all_positions()
        st.error("Close-all command published to execution engine")
