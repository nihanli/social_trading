"""
Page 2 — Signal Feed.

Shows:
  - Two-phase pipeline status (live enrichment queue + phase breakdown)
  - Signal quality distribution (7 days) split by phase
  - Signal volume per hour (3 days) by direction
  - Phase funnel: phase1 candidates → phase2 signals → approved → executed
  - Full searchable/filterable signal table with phase column
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.db import query, localize_datetimes
from social_trading.monitoring.streamlit.utils.redis_ctrl import (
    get_phase_pipeline_stats,
)
from social_trading.monitoring.streamlit.utils.refresh_countdown import (
    sidebar_refresh_countdown,
)

st.set_page_config(page_title="Signal Feed", page_icon="⚡", layout="wide")
st_autorefresh(interval=15_000, key="signals_refresh")
sidebar_refresh_countdown()
st.title("Signal Feed")

# ── Live two-phase pipeline status ────────────────────────────────────────────
pipeline = get_phase_pipeline_stats()

p1_today = query("""
    SELECT COUNT(*) AS cnt FROM signals
    WHERE generated_at::date = CURRENT_DATE AND signal_phase = 'phase1'
""")
p2_today = query("""
    SELECT COUNT(*) AS cnt FROM signals
    WHERE generated_at::date = CURRENT_DATE AND signal_phase = 'phase2'
""")
p1_cnt = int(p1_today.iloc[0]["cnt"]) if not p1_today.empty else 0
p2_cnt = int(p2_today.iloc[0]["cnt"]) if not p2_today.empty else 0

st.subheader("Two-Phase Pipeline — Today")
kc1, kc2, kc3, kc4 = st.columns(4)
kc1.metric(
    "Phase 1 Signals",
    p1_cnt,
    help="Signals from free/Tier-1 sources (lower threshold). "
         "When Tier-2 is configured these become enrichment candidates.",
)
kc2.metric(
    "Phase 2 Signals",
    p2_cnt,
    help="Signals re-evaluated after Tier-2 (paid) enrichment. Subset of Phase 1 candidates.",
)
kc3.metric(
    "Enrichment Queue",
    pipeline["enrichment_queue"],
    help="Phase-1 tickers currently queued for Tier-2 enrichment (pending backlog).",
)
tier2_label = "✅ Enabled" if pipeline.get("tier2_configured") else "⚠️ Disabled (Phase 1 only)"
kc4.metric("Tier-2 Sources", tier2_label)

st.divider()

# ── Quality distribution + volume timeline ────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    quality_df = query("""
        SELECT ROUND(confidence::numeric, 1) AS quality_bucket,
               COALESCE(signal_phase, 'legacy') AS phase,
               COUNT(*) AS count
        FROM signals
        WHERE generated_at > NOW() - INTERVAL '7 days'
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if not quality_df.empty:
        fig = px.bar(
            quality_df,
            x="quality_bucket",
            y="count",
            color="phase",
            barmode="stack",
            title="Signal Quality Distribution (7 days) by Phase",
            labels={"quality_bucket": "Quality Score", "count": "Count", "phase": "Phase"},
            color_discrete_map={
                "phase1": "#4A90D9",
                "phase2": "#2ECC71",
                "legacy": "#95A5A6",
            },
        )
        fig.update_layout(height=300, margin={"t": 40, "b": 10})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No signal history yet")

with col_right:
    signal_ts = query("""
        SELECT DATE_TRUNC('hour', generated_at) AS hour,
               direction, COUNT(*) AS count
        FROM signals
        WHERE generated_at > NOW() - INTERVAL '3 days'
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if not signal_ts.empty:
        localize_datetimes(signal_ts)
        fig2 = px.bar(
            signal_ts,
            x="hour",
            y="count",
            color="direction",
            title="Signals per Hour (3 days)",
            color_discrete_map={"LONG": "#28a745", "SHORT": "#dc3545", "FLAT": "#6c757d"},
        )
        fig2.update_layout(height=300, margin={"t": 40, "b": 10})
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No signal history yet")

# ── Two-phase funnel (7 days) ─────────────────────────────────────────────────
st.subheader("Two-Phase Funnel (7 days)")
funnel_df = query("""
    SELECT
        COUNT(*)                                                     AS total,
        COUNT(*) FILTER (WHERE signal_phase = 'phase1')             AS phase1,
        COUNT(*) FILTER (WHERE signal_phase = 'phase2')             AS phase2,
        COUNT(*) FILTER (WHERE approved)                            AS approved,
        COUNT(*) FILTER (WHERE executed)                            AS executed,
        ROUND(AVG(confidence)::numeric, 3)                         AS avg_quality,
        ROUND(AVG(confidence) FILTER (WHERE signal_phase = 'phase1')::numeric, 3) AS avg_p1,
        ROUND(AVG(confidence) FILTER (WHERE signal_phase = 'phase2')::numeric, 3) AS avg_p2
    FROM signals
    WHERE generated_at > NOW() - INTERVAL '7 days'
""")

if not funnel_df.empty and funnel_df.iloc[0]["total"] > 0:
    f = funnel_df.iloc[0]
    fc1, fc2, fc3, fc4, fc5, fc6, fc7 = st.columns(7)
    fc1.metric("Total Signals", int(f["total"]))
    fc2.metric("Phase 1", int(f["phase1"] or 0),
               help="Free-source signals (passed phase1_threshold)")
    fc3.metric("Phase 2", int(f["phase2"] or 0),
               help="Enriched signals (passed phase2_threshold)")
    fc4.metric("Approved", int(f["approved"]))
    fc5.metric("Executed", int(f["executed"]))
    fc6.metric("Avg Quality P1", f"{f['avg_p1']:.3f}" if f["avg_p1"] else "—")
    fc7.metric("Avg Quality P2", f"{f['avg_p2']:.3f}" if f["avg_p2"] else "—")

    # Funnel chart
    funnel_stages = ["Total", "Phase 1", "Phase 2", "Approved", "Executed"]
    funnel_values = [
        int(f["total"]),
        int(f["phase1"] or 0),
        int(f["phase2"] or 0),
        int(f["approved"]),
        int(f["executed"]),
    ]
    funnel_fig = go.Figure(go.Funnel(
        y=funnel_stages,
        x=funnel_values,
        textinfo="value+percent initial",
        marker={
            "color": ["#6C757D", "#4A90D9", "#2ECC71", "#F39C12", "#E74C3C"],
        },
    ))
    funnel_fig.update_layout(
        title="Signal Pipeline Funnel (7 days)",
        height=300,
        margin={"t": 40, "b": 10, "l": 10, "r": 10},
    )
    st.plotly_chart(funnel_fig, use_container_width=True)

st.divider()

# ── Phase breakdown over time (daily, 14 days) ────────────────────────────────
phase_trend = query("""
    SELECT DATE_TRUNC('day', generated_at)::date AS day,
           COALESCE(signal_phase, 'legacy')       AS phase,
           COUNT(*)                               AS count
    FROM signals
    WHERE generated_at > NOW() - INTERVAL '14 days'
    GROUP BY 1, 2
    ORDER BY 1
""")
if not phase_trend.empty:
    fig3 = px.bar(
        phase_trend,
        x="day",
        y="count",
        color="phase",
        barmode="stack",
        title="Daily Signal Volume by Phase (14 days)",
        labels={"day": "Date", "count": "Signals", "phase": "Phase"},
        color_discrete_map={
            "phase1": "#4A90D9",
            "phase2": "#2ECC71",
            "legacy": "#95A5A6",
        },
    )
    fig3.update_layout(height=250, margin={"t": 40, "b": 10})
    st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Full searchable signal table ──────────────────────────────────────────────
st.subheader("All Signals")
col1, col2, col3, col4 = st.columns(4)
ticker_filter = col1.text_input("Filter ticker")
direction_filter = col2.selectbox("Direction", ["All", "LONG", "SHORT", "FLAT"])
phase_filter = col3.selectbox("Phase", ["All", "phase1", "phase2", "legacy"])
days_filter = col4.selectbox("Time window", [1, 3, 7, 30], index=1)

where = f"WHERE generated_at > NOW() - INTERVAL '{days_filter} days'"
if ticker_filter:
    where += f" AND ticker ILIKE '%{ticker_filter}%'"
if direction_filter != "All":
    where += f" AND direction = '{direction_filter}'"
if phase_filter == "legacy":
    where += " AND signal_phase IS NULL"
elif phase_filter != "All":
    where += f" AND signal_phase = '{phase_filter}'"

full_signals = query(f"""
    SELECT ticker, direction,
           COALESCE(signal_phase, 'legacy')        AS phase,
           ROUND(confidence::numeric, 3)           AS quality,
           ROUND(sentiment_score::numeric, 3)      AS sentiment,
           ROUND(mention_zscore::numeric, 2)       AS vol_z,
           approved, executed,
           TO_CHAR(generated_at, 'YYYY-MM-DD HH24:MI:SS') AS time
    FROM signals
    {where}
    ORDER BY generated_at DESC
    LIMIT 300
""")

if not full_signals.empty:
    # Colour-code the phase column via styling
    def _phase_style(val: str) -> str:
        return {
            "phase1": "color: #4A90D9; font-weight: bold",
            "phase2": "color: #2ECC71; font-weight: bold",
            "legacy": "color: #95A5A6",
        }.get(val, "")

    styled = full_signals.style.map(_phase_style, subset=["phase"])
    st.dataframe(styled, use_container_width=True, hide_index=True)
    st.caption(f"{len(full_signals)} signals shown (max 300)")
else:
    st.info("No signals match the current filters")
