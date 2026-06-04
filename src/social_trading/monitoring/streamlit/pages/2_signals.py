"""
Page 2 — Signal Feed.

Shows:
  - Two-phase pipeline status (live enrichment queue + phase breakdown)
  - Signal quality distribution (7 days) split by phase
  - Signal volume per hour (3 days) by direction
  - Phase funnel: phase1 candidates → phase2 signals → approved → executed
  - Quality score factor breakdown (v, s, p, m, c) for recent signals
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
        st.plotly_chart(fig, width='stretch')
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
        st.plotly_chart(fig2, width='stretch')
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
    st.plotly_chart(funnel_fig, width='stretch')

st.divider()

# ── Quality score factor breakdown ────────────────────────────────────────────
st.subheader("Quality Score Factor Breakdown")
st.caption(
    "**Quality = w_volume × V + w_sentiment × S + w_proactivity × P "
    "+ w_momentum × M + w_convergence × C** (each factor normalised to [0, 1], "
    "then divided by active-weight sum)"
)

# Pull recent signals with all stored factor columns
factor_df = query("""
    SELECT ticker,
           COALESCE(signal_phase, 'legacy')                    AS phase,
           direction,
           ROUND(confidence::numeric, 3)                       AS quality,
           -- v: vol Z-score normalised to [0,1] (Z=3 → 1.0)
           ROUND(LEAST(GREATEST(mention_zscore / 3.0, 0), 1)::numeric, 3) AS v_volume,
           -- s: |sentiment_score| capped at 1
           ROUND(LEAST(ABS(sentiment_score), 1)::numeric, 3)   AS s_sentiment,
           -- p: proactivity (1=led price, 0=reactive); NULL for pre-migration rows → show as 1
           ROUND(COALESCE(proactivity, 1)::numeric, 3)         AS p_proactivity,
           -- m: |momentum| / 0.10 capped at 1 (NULL → market data unavailable)
           ROUND(LEAST(ABS(COALESCE(momentum, 0)) / 0.10, 1)::numeric, 3) AS m_momentum,
           -- c: convergence (fraction × bonus, 0 = single source or no agreement)
           ROUND(COALESCE(convergence, 0)::numeric, 3)         AS c_convergence,
           momentum IS NULL                                     AS no_market_data,
           TO_CHAR(generated_at, 'MM-DD HH24:MI')              AS time
    FROM signals
    WHERE generated_at > NOW() - INTERVAL '2 days'
    ORDER BY generated_at DESC
    LIMIT 50
""")

if not factor_df.empty:
    # ── Average factor contributions chart ───────────────────────────────────
    factor_cols = ["v_volume", "s_sentiment", "p_proactivity", "m_momentum", "c_convergence"]
    factor_means = factor_df[factor_cols].mean()
    factor_labels = {
        "v_volume":      "V — Volume Z",
        "s_sentiment":   "S — Sentiment",
        "p_proactivity": "P — Proactivity",
        "m_momentum":    "M — Momentum",
        "c_convergence": "C — Convergence",
    }
    factor_means_display = {factor_labels[k]: float(v) for k, v in factor_means.items()}

    import pandas as pd
    bar_df = pd.DataFrame({
        "Factor": list(factor_means_display.keys()),
        "Avg Value (0–1)": list(factor_means_display.values()),
    })
    no_mkt = factor_df["no_market_data"].sum() if "no_market_data" in factor_df.columns else 0
    title_note = f" — ⚠️ {no_mkt}/{len(factor_df)} signals had no market data (M=0)" if no_mkt else ""
    bar_fig = px.bar(
        bar_df,
        x="Factor",
        y="Avg Value (0–1)",
        color="Factor",
        title=f"Average Factor Values — Last 2 Days (up to 50 signals){title_note}",
        color_discrete_sequence=["#4A90D9", "#2ECC71", "#F39C12", "#E74C3C", "#9B59B6"],
        text_auto=".2f",
    )
    bar_fig.update_layout(
        height=300,
        margin={"t": 40, "b": 10},
        showlegend=False,
        yaxis={"range": [0, 1.05]},
    )
    st.plotly_chart(bar_fig, use_container_width=True)

    # ── Per-signal factor heatmap (most recent 30) ────────────────────────────
    with st.expander("Per-signal factor heatmap (latest 30)", expanded=False):
        heatmap_df = factor_df.head(30).copy()
        heatmap_df["label"] = heatmap_df["ticker"] + " " + heatmap_df["time"]
        heat_data = heatmap_df.set_index("label")[
            ["v_volume", "s_sentiment", "p_proactivity", "m_momentum", "c_convergence"]
        ].rename(columns={
            "v_volume":      "V Volume",
            "s_sentiment":   "S Sentiment",
            "p_proactivity": "P Proactivity",
            "m_momentum":    "M Momentum",
            "c_convergence": "C Convergence",
        })
        heat_fig = px.imshow(
            heat_data,
            color_continuous_scale="Blues",
            zmin=0, zmax=1,
            aspect="auto",
            title="Quality Factor Heatmap (0=low, 1=high)",
            labels={"color": "Factor value"},
        )
        heat_fig.update_layout(height=max(300, 20 * len(heat_data)), margin={"t": 40, "b": 10})
        st.plotly_chart(heat_fig, use_container_width=True)

    # ── Factor table ──────────────────────────────────────────────────────────
    with st.expander("Factor detail table", expanded=False):
        st.dataframe(
            factor_df.drop(columns=["no_market_data"], errors="ignore").rename(columns={
                "v_volume":      "V (vol-z)",
                "s_sentiment":   "S (sentiment)",
                "p_proactivity": "P (proactivity)",
                "m_momentum":    "M (momentum)",
                "c_convergence": "C (convergence)",
            }),
            use_container_width=True,
            hide_index=True,
        )
else:
    st.info("No signal data in the last 2 days")

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
    st.plotly_chart(fig3, width='stretch')

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
    SELECT ticker,
           '/chart?ticker=' || ticker || '&tf=1M' AS chart,
           direction,
           COALESCE(signal_phase, 'legacy')             AS phase,
           ROUND(confidence::numeric, 3)                AS quality,
           ROUND(sentiment_score::numeric, 3)           AS sentiment,
           ROUND(mention_zscore::numeric, 2)            AS vol_z,
           ROUND(COALESCE(proactivity, 1)::numeric, 1)      AS proactivity,
           ROUND(COALESCE(momentum, 0)::numeric, 4)         AS momentum,
           ROUND(COALESCE(convergence, 0)::numeric, 3)      AS convergence,
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
    st.dataframe(
        styled,
        width='stretch',
        hide_index=True,
        column_config={
            "chart": st.column_config.LinkColumn(
                "📈",
                help="Open chart in new tab",
                display_text="📈",
            ),
        },
    )
    st.caption(f"{len(full_signals)} signals shown (max 300)")
else:
    st.info("No signals match the current filters")
