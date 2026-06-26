from __future__ import annotations

import os
import sys
from datetime import UTC, datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.redis_ctrl import (
    adopt_ib_position,
    delete_reconcile_position,
    get_reconcile_conflicts,
    get_reconcile_full,
    get_reconcile_last_run,
    get_system_state,
    resolve_conflict,
    trigger_reconcile_now,
)

st.set_page_config(page_title="Reconcile Monitor", page_icon="🔄", layout="wide")

# ── Auto-refresh every 60 s (matching the reconcile loop cadence) ──────────────
_refresh_count = st_autorefresh(interval=60_000, key="reconcile_autorefresh")

# ── Colour-coded state badges ──────────────────────────────────────────────────
_STATE_ICON = {
    "matched":      "✅",
    "shares_synced": "🔧",
    "fill_pending": "⏳",
    "naked":        "⚠️",
    "adopted":      "📥",
    "manual_ib":    "ℹ️",
    "closed_offline": "🔴",
    "missing":      "❌",
    "direction_mismatch": "❌",
}
_STATE_COLOR = {
    "matched": "#1a7a4a",
    "shares_synced": "#b45309",
    "fill_pending": "#1e40af",
    "naked": "#b45309",
    "adopted": "#1e40af",
    "manual_ib": "#4b5563",
    "closed_offline": "#4b5563",
    "missing": "#b91c1c",
    "direction_mismatch": "#b91c1c",
}


def _badge(state: str) -> str:
    icon = _STATE_ICON.get(state, "•")
    color = _STATE_COLOR.get(state, "#888")
    return f'<span style="background:{color};color:#fff;border-radius:4px;padding:1px 7px;font-size:0.75rem;font-weight:600">{icon} {state}</span>'


def _age_str(iso: str) -> str:
    """Return human-readable age from an ISO timestamp."""
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        secs = int((datetime.now(UTC) - dt).total_seconds())
        if secs < 60:
            return f"{secs}s ago"
        if secs < 3600:
            return f"{secs // 60}m ago"
        return f"{secs // 3600}h {(secs % 3600) // 60}m ago"
    except Exception:
        return iso


def _df(rows: list[dict], cols: list[tuple[str, str]]) -> pd.DataFrame:
    normalized = [
        {label: (row.get(key, "") if isinstance(row, dict) else "") for key, label in cols}
        for row in (rows or [])
    ]
    return pd.DataFrame(normalized, columns=[label for _, label in cols])


# ── Load data ──────────────────────────────────────────────────────────────────
sys_state   = get_system_state()
last_run    = get_reconcile_last_run()
conflicts   = get_reconcile_conflicts()
data        = get_reconcile_full()
matches     = data.get("matches", []) or []
ib_account  = data.get("ib_account", "—")

ib_connected = sys_state.get("ib_connected") == "1"

# ── Title + status bar ─────────────────────────────────────────────────────────
st.title("🔄 Reconcile Monitor")

ib_badge = (
    '<span style="color:#22c55e;font-weight:700">● Connected</span>'
    if ib_connected
    else '<span style="color:#ef4444;font-weight:700">● Disconnected</span>'
)
conflict_badge = (
    f'<span style="background:#b91c1c;color:#fff;border-radius:4px;padding:1px 7px;font-weight:700">⚠️ {len(conflicts)} conflict(s)</span>'
    if conflicts
    else '<span style="background:#1a7a4a;color:#fff;border-radius:4px;padding:1px 7px;font-weight:700">✅ No conflicts</span>'
)

c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
c1.markdown(f"**IB** {ib_badge}", unsafe_allow_html=True)
c2.markdown(f"**Account** `{ib_account}`")
c3.markdown(f"**Status** {conflict_badge}", unsafe_allow_html=True)
c4.markdown(f"**Last run** {_age_str(last_run) if last_run else '_never_'}")

if st.button("▶ Run Reconcile Now", type="secondary"):
    trigger_reconcile_now()
    st.success("Reconcile triggered — results will appear within ~60 s.")

st.divider()

# ── Conflict section ───────────────────────────────────────────────────────────
if conflicts:
    st.error(
        f"⛔ **TRADING HALTED** — {len(conflicts)} conflict(s) require manual resolution before new entries resume.",
        icon="🚨",
    )
    for ticker, cdata in conflicts.items():
        state   = cdata.get("state", "unknown")
        reason  = cdata.get("reason", "No details available.")
        options = cdata.get("options", [])
        icon    = _STATE_ICON.get(state, "❌")
        with st.expander(f"{icon} **{ticker}** — {state}", expanded=True):
            st.markdown(f"**Reason:** {reason}")
            lcol, rcol = st.columns(2)
            with lcol:
                st.markdown("**App state**")
                _app = {k: v for k, v in cdata.items() if k not in {"state", "reason", "options", "detected_at"}}
                st.json(_app)
            with rcol:
                st.markdown("**Detected**")
                st.caption(_age_str(cdata.get("detected_at", "")))
                st.markdown("**Available actions**")

                if "mark_closed" in options:
                    if st.button(f"✅ Mark Closed (no fill)", key=f"mc_{ticker}"):
                        resolve_conflict(ticker, "mark_closed")
                        st.success(f"mark_closed sent for {ticker}")
                        st.rerun()
                if "remove_app" in options:
                    if st.button(f"🗑 Remove from App", key=f"ra_{ticker}"):
                        resolve_conflict(ticker, "remove_app")
                        st.success(f"remove_app sent for {ticker}")
                        st.rerun()
                if "use_ib_direction" in options:
                    if st.button(f"🔄 Use IB Direction ({cdata.get('ib_direction', '?')})", key=f"uid_{ticker}"):
                        resolve_conflict(ticker, "use_ib_direction")
                        st.success(f"use_ib_direction sent for {ticker}")
                        st.rerun()
                if "close_position" in options:
                    if st.button(f"⛔ Close IB Position", key=f"cp_{ticker}", type="primary"):
                        resolve_conflict(ticker, "close_position")
                        st.success(f"close_position sent for {ticker}")
                        st.rerun()
    st.divider()

# ── Summary metrics ────────────────────────────────────────────────────────────
state_counts: dict[str, int] = {}
for m in matches:
    s = m.get("state", "unknown")
    state_counts[s] = state_counts.get(s, 0) + 1

_metric_states = [
    ("matched",      "✅ Matched"),
    ("shares_synced","🔧 Shares Synced"),
    ("fill_pending", "⏳ Fill Pending"),
    ("naked",        "⚠️ Naked"),
    ("adopted",      "📥 Adopted"),
    ("closed_offline","🔴 Closed Offline"),
    ("manual_ib",    "ℹ️ Manual IB"),
    ("missing",      "❌ Missing"),
    ("direction_mismatch", "❌ Dir Mismatch"),
]
cols = st.columns(len(_metric_states))
for col, (state_key, label) in zip(cols, _metric_states):
    col.metric(label, state_counts.get(state_key, 0))

st.divider()

# ── Detail tabs ────────────────────────────────────────────────────────────────
if not data:
    st.info("No reconcile data yet. Click **Run Reconcile Now** or wait for the next automatic cycle (every 60 s).")
    st.stop()

tab_pos, tab_ib, tab_fills, tab_oca = st.tabs(
    ["📋 Positions", "📊 IB State", "💰 Fills Today", "🛡 OCA Orders"]
)

# ── Tab 1: Positions ────────────────────────────────────────────────────────────
with tab_pos:
    st.subheader(f"Position States ({len(matches)} total)")
    if not matches:
        st.info("No positions tracked.")
    else:
        # Summary table
        rows = []
        for m in matches:
            t = m.get("ticker", "?")
            s = m.get("state", "?")
            app = m.get("app") or {}
            ib  = m.get("ib") or {}
            rows.append({
                "": _STATE_ICON.get(s, "•"),
                "Ticker":     t,
                "State":      s,
                "Direction":  app.get("direction") or ib.get("direction") or "—",
                "Shares (App)": app.get("shares", "—"),
                "Shares (IB)":  ib.get("shares", "—"),
                "Entry $":    app.get("entry_price", "—"),
                "Stop $":     app.get("stop_loss", "—"),
                "Target $":   app.get("take_profit", "—"),
                "Source":     app.get("source", "—"),
                "Opened At":  app.get("opened_at", "—"),
                "Reason":     (m.get("reason") or "")[:80],
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("Per-Position Details")
        for m in matches:
            t   = m.get("ticker", "?")
            s   = m.get("state", "?")
            ico = _STATE_ICON.get(s, "•")
            expanded = s in {"missing", "direction_mismatch", "naked", "fill_pending", "adopted"}
            with st.expander(f"{ico} **{t}** — {s}", expanded=expanded):
                st.markdown(f"**Reason:** {m.get('reason', '—')}")
                l, r = st.columns(2)
                with l:
                    st.markdown("**App params**")
                    st.json(m.get("app") or {})
                with r:
                    st.markdown("**IB position**")
                    st.json(m.get("ib") or {})
                if m.get("fill"):
                    st.markdown("**Latest fill**")
                    st.json(m.get("fill"))
                # Manual IB — offer adopt button
                if s == "manual_ib":
                    st.warning(
                        "IB position with no system marker. "
                        "Click **Adopt** if this was opened by this app."
                    )
                    if st.button("📥 Adopt into System", key=f"adopt_{t}"):
                        adopt_ib_position(t)
                        st.success(f"Adopt command sent for {t}.")
                        st.rerun()
                # Pending reconcile — offer delete
                if s == "pending_manual":
                    if st.button("🗑 Delete from App", key=f"del_{t}"):
                        delete_reconcile_position(t)
                        st.success(f"Delete sent for {t}.")
                        st.rerun()

# ── Tab 2: IB State ─────────────────────────────────────────────────────────────
with tab_ib:
    ib_positions = data.get("ib_positions", []) or []
    st.subheader(f"IB Positions ({len(ib_positions)})")
    st.dataframe(
        _df(ib_positions, [
            ("ticker",        "Ticker"),
            ("direction",     "Direction"),
            ("shares",        "Shares"),
            ("avg_cost",      "Avg Cost"),
            ("market_price",  "Market Price"),
            ("unrealized_pnl","Unrealized P&L"),
        ]),
        use_container_width=True, hide_index=True,
    )

    app_positions = data.get("app_positions", []) or []
    st.subheader(f"App Params ({len(app_positions)})")
    st.dataframe(
        _df(app_positions, [
            ("ticker",      "Ticker"),
            ("direction",   "Direction"),
            ("shares",      "Shares"),
            ("entry_price", "Entry $"),
            ("stop_loss",   "Stop $"),
            ("take_profit", "Target $"),
            ("oca_group",   "OCA Group"),
            ("source",      "Source"),
            ("opened_at",   "Opened At"),
        ]),
        use_container_width=True, hide_index=True,
    )

# ── Tab 3: Fills ────────────────────────────────────────────────────────────────
with tab_fills:
    fills = data.get("ib_trades_today") or data.get("ib_positions", [])
    # ib_trades_today is the raw executions list
    raw_fills = data.get("ib_trades_today", []) or []
    st.subheader(f"IB Executions Today ({len(raw_fills)})")
    if raw_fills:
        st.dataframe(
            _df(raw_fills, [
                ("ticker",     "Ticker"),
                ("action",     "Action"),
                ("fill_price", "Fill Price"),
                ("quantity",   "Quantity"),
                ("time",       "Time"),
                ("ref",        "Ref"),
                ("type",       "Type"),
            ]),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No fills recorded today (or data not yet available).")

# ── Tab 4: OCA Orders ───────────────────────────────────────────────────────────
with tab_oca:
    oca_orders = data.get("app_oca_orders", []) or []
    st.subheader(f"Active OCA / Bracket Orders ({len(oca_orders)})")
    if oca_orders:
        st.dataframe(
            _df(oca_orders, [
                ("ticker",     "Ticker"),
                ("order_type", "Type"),
                ("action",     "Action"),
                ("oca_group",  "OCA Group"),
                ("aux_price",  "Aux Price"),
                ("status",     "Status"),
            ]),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No OCA orders in IB openTrades with system orderRef.")

# ── Footer ─────────────────────────────────────────────────────────────────────
st.divider()
collected_at = data.get("collected_at", "")
auto_actions = data.get("auto_actions", []) or []
st.caption(
    f"Data collected: {collected_at or '—'}  |  "
    f"Auto-actions applied this cycle: {len(auto_actions)}  |  "
    f"Page auto-refreshes every 60 s (refresh #{_refresh_count})"
)
