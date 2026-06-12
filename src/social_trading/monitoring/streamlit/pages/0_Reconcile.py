from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.redis_ctrl import (
    adopt_ib_position,
    approve_reconcile,
    delete_reconcile_position,
    get_reconcile_data,
    get_reconcile_state,
    skip_reconcile,
)

st.set_page_config(page_title="Startup Reconcile", page_icon="⚠️", layout="wide")
st_autorefresh(interval=3_000, key="reconcile_refresh")


def _frame(rows: list[dict], columns: list[tuple[str, str]]) -> pd.DataFrame:
    normalized = []
    for row in rows or []:
        item = row if isinstance(row, dict) else {}
        normalized.append({label: item.get(key, "") for key, label in columns})
    return pd.DataFrame(normalized, columns=[label for _, label in columns])


def _summary_counts(matches: list[dict]) -> tuple[int, int, int]:
    matched = sum(1 for item in matches if item.get("status") == "matched")
    auto_closed = sum(1 for item in matches if item.get("status") == "closed_offline")
    pending_manual = sum(1 for item in matches if item.get("status") == "pending_manual")
    return matched, auto_closed, pending_manual


ICON_MAP = {
    "matched": "✅",
    "shares_mismatch": "⚠️",
    "closed_offline": "🔴",
    "pending_manual": "❌",
    "adopted": "📥",
    "manual_ib": "ℹ️",
}


state = get_reconcile_state()
data = get_reconcile_data()
matches = data.get("matches", []) if isinstance(data, dict) else []

st.title("Startup Reconcile")

if state == "awaiting_approval":
    st.title("⚠️ Startup Reconcile Required")
    st.caption("IB is connected. Review open positions before trading begins.")

    app_positions = _frame(
        data.get("app_positions", []),
        [
            ("ticker", "Ticker"),
            ("direction", "Direction"),
            ("shares", "Shares"),
            ("entry_price", "Entry $"),
            ("stop_loss", "Stop $"),
            ("take_profit", "Target $"),
            ("opened_at", "Opened At"),
        ],
    )
    app_orders = _frame(
        data.get("app_oca_orders", []),
        [
            ("ticker", "Ticker"),
            ("order_type", "Order Type"),
            ("action", "Action"),
            ("oca_group", "OCA Group"),
            ("aux_price", "Aux Price"),
            ("status", "Status"),
        ],
    )
    ib_positions = _frame(
        data.get("ib_positions", []),
        [
            ("ticker", "Ticker"),
            ("shares", "Shares"),
            ("avg_cost", "Avg Cost"),
            ("market_price", "Market Price"),
            ("unrealized_pnl", "Unrealized P&L"),
        ],
    )
    ib_trades = _frame(
        data.get("ib_trades_today", []),
        [
            ("ticker", "Ticker"),
            ("action", "Action"),
            ("type", "Type"),
            ("fill_price", "Fill Price"),
            ("quantity", "Quantity"),
            ("time", "Time"),
            ("status", "Status"),
            ("ref", "Ref"),
        ],
    )

    st.subheader("App Internal State")
    st.markdown("**App Tracked Positions**")
    st.dataframe(app_positions, use_container_width=True)
    st.markdown("**App OCA Orders**")
    st.dataframe(app_orders, use_container_width=True)

    st.subheader("IB Current State")
    st.markdown("**IB Open Positions**")
    st.dataframe(ib_positions, use_container_width=True)
    st.markdown("**IB Trades Today**")
    st.dataframe(ib_trades, use_container_width=True)

    st.subheader("Reconcile Results")
    for item in matches:
        ticker = item.get("ticker", "?")
        status = item.get("status", "")
        icon = ICON_MAP.get(status, "•")
        with st.expander(f"{icon} {ticker} — {status}", expanded=status in {"pending_manual", "shares_mismatch"}):
            left, right = st.columns(2)
            with left:
                st.markdown("**App Data**")
                st.json(item.get("app") or {})
            with right:
                st.markdown("**IB Data**")
                st.json(item.get("ib") or {})
            if item.get("fill"):
                st.markdown("**Fill Data**")
                st.json(item.get("fill") or {})
            st.info(item.get("reason", ""))
            if status == "shares_mismatch":
                st.caption("Share count mismatch detected. IB position size will remain the source of truth.")
            if status == "pending_manual":
                if st.button("🗑 Delete from App", key=f"reconcile_delete_{ticker}"):
                    delete_reconcile_position(ticker)
                    st.success(f"Delete command sent for {ticker}.")
                    st.rerun()
            if status == "manual_ib":
                st.warning(
                    "This position is in IB but has no system marker. "
                    "If it was opened by this app (e.g., from a prior TWS session), "
                    "click **Adopt** to bring it under system management. "
                    "Otherwise ignore — it will not be tracked or auto-exited."
                )
                if st.button("📥 Adopt into System", key=f"reconcile_adopt_{ticker}"):
                    adopt_ib_position(ticker)
                    st.success(f"Adopt command sent for {ticker}. Position will be tracked after next cycle.")
                    st.rerun()

    st.info(
        f"Auto-actions: {len(data.get('auto_actions', []))} positions will be confirmed closed. "
        "Approve to apply."
    )
    col1, col2 = st.columns(2)
    if col1.button("✅ Approve & Start Trading", type="primary", width="stretch"):
        approve_reconcile()
        st.success("Approval command sent.")
        st.rerun()
    if col2.button("⏭ Skip Reconcile", width="stretch"):
        skip_reconcile()
        st.info("Reconcile skipped.")
        st.rerun()
    st.stop()

if state in {"approved", "skipped_no_ib"}:
    matched, auto_closed, pending_manual = _summary_counts(matches if isinstance(matches, list) else [])
    if state == "skipped_no_ib":
        st.info(
            "Reconcile skipped (IB unavailable at startup)\n\n"
            f"Summary: {matched} matched, {auto_closed} auto-closed, {pending_manual} pending manual"
        )
    else:
        collected_at = data.get("collected_at", "unknown")
        st.info(
            f"Last reconcile: {collected_at}\n\n"
            f"Summary: {matched} matched, {auto_closed} auto-closed, {pending_manual} pending manual"
        )
    st.stop()

if state == "collecting":
    st.info("Collecting startup reconcile data…")
    st.stop()

st.info("No reconcile data available.")
