"""
Page 8 — Service Logs

Real-time log viewer for all background services.  Logs are written to Redis
streams (``logs:{service}``) by each service's RedisLogHandler and expire
automatically 10 minutes after the last entry.

Features
--------
* Per-service tabs: ingest, nlp, signal, risk, execution, persistence
* Enable/disable toggle per service (persisted in Redis so it survives refresh)
* Level filter (ALL / INFO+ / WARNING+ / ERROR)
* Max-rows slider
* Clear button per tab
* Auto-refreshes every 3 seconds
"""

from __future__ import annotations

import os
from datetime import datetime

import redis
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Service Logs", page_icon="📋", layout="wide")
st.title("📋 Service Logs")

_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_SERVICES = ["ingest", "nlp", "signal", "risk", "execution", "persistence"]
_LEVEL_COLORS = {
    "DEBUG":    "#555555",
    "INFO":     "#000000",
    "WARNING":  "#f0c040",
    "ERROR":    "#ff6060",
    "CRITICAL": "#ff2020",
}
_LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}

# ── Redis connection (sync — Streamlit is sync) ───────────────────────────────
@st.cache_resource
def _get_redis() -> redis.Redis:
    return redis.from_url(_REDIS_URL, decode_responses=True, socket_connect_timeout=2)


def _safe_redis() -> redis.Redis | None:
    try:
        r = _get_redis()
        r.ping()
        return r
    except Exception:
        return None


# ── Auto-refresh ─────────────────────────────────────────────────────────────
st_autorefresh(interval=3_000, key="logs_refresh")

# ── Sidebar controls ─────────────────────────────────────────────────────────
st.sidebar.header("Log Controls")

r = _safe_redis()
if r is None:
    st.error("❌ Redis unavailable — cannot load logs.")
    st.stop()

# --- Enable/disable toggles per service (state persisted in Redis) ---
st.sidebar.subheader("Enable logging per service")
for svc in _SERVICES:
    flag_key = f"logs:enabled:{svc}"
    current = r.get(flag_key)
    is_on = (current is None) or (current != "0")
    new_val = st.sidebar.toggle(svc, value=is_on, key=f"toggle_{svc}")
    if new_val != is_on:
        r.set(flag_key, "1" if new_val else "0")

st.sidebar.divider()

# --- Level filter ---
level_filter = st.sidebar.selectbox(
    "Minimum log level",
    options=["DEBUG", "INFO", "WARNING", "ERROR"],
    index=1,  # default INFO
    key="level_filter",
)
min_level = _LEVEL_ORDER.get(level_filter, 1)

# --- Max rows ---
max_rows = st.sidebar.slider("Max rows per tab", min_value=50, max_value=500, value=200, step=50)

# ── Helper: fetch and render one service tab ──────────────────────────────────
def _render_service(svc: str) -> None:
    stream_key = f"logs:{svc}"
    flag_key = f"logs:enabled:{svc}"

    # Enabled state badge
    enabled_raw = r.get(flag_key)
    is_enabled = (enabled_raw is None) or (enabled_raw != "0")
    if not is_enabled:
        st.warning("⏸ Logging is **disabled** for this service. Use the sidebar toggle to enable.")

    col_clear, col_ttl, col_spacer = st.columns([1, 2, 6])

    with col_clear:
        if st.button("🗑 Clear", key=f"clear_{svc}"):
            r.delete(stream_key)
            st.rerun()

    # Show TTL so user knows when stream will auto-expire
    ttl = r.ttl(stream_key)
    with col_ttl:
        if ttl == -2:
            st.caption("Stream: expired / empty")
        elif ttl == -1:
            st.caption("Stream: no expiry set")
        else:
            st.caption(f"Stream expires in: {ttl}s")

    # Fetch entries (newest first)
    try:
        raw_entries = r.xrevrange(stream_key, count=max_rows)
    except Exception as exc:
        st.error(f"Redis error: {exc}")
        return

    if not raw_entries:
        st.info("No log entries yet — start the service or wait for the first log line.")
        return

    # Build display rows
    rows: list[dict] = []
    for _entry_id, fields in raw_entries:
        level = fields.get("level", "INFO")
        if _LEVEL_ORDER.get(level, 0) < min_level:
            continue
        ts_ms = fields.get("ts", "0")
        try:
            ts_dt = datetime.fromtimestamp(int(ts_ms) / 1000)  # local time
            ts_str = ts_dt.strftime("%m-%d %H:%M:%S.%f")[:-3]  # MM-DD HH:MM:SS.mmm
        except Exception:
            ts_str = ts_ms
        rows.append({
            "time":    ts_str,
            "level":   level,
            "logger":  fields.get("logger", ""),
            "message": fields.get("msg", ""),
            "exc":     fields.get("exc", ""),
        })

    if not rows:
        st.info(f"No entries at or above {level_filter} level.")
        return

    # Render as styled HTML table for color coding
    lines: list[str] = []
    lines.append(
        "<style>"
        "table.logtable { width:100%; border-collapse:collapse; font-family:monospace; font-size:12px; }"
        "table.logtable td { padding:2px 6px; vertical-align:top; border-bottom:1px solid #333; }"
        "table.logtable td.time { white-space:nowrap; color:#aaa; width:80px; }"
        "table.logtable td.level { white-space:nowrap; font-weight:bold; width:70px; }"
        "table.logtable td.logger { color:#8abcf0; width:220px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }"
        "table.logtable td.msg { word-break:break-all; }"
        "table.logtable td.exc { color:#ff9090; font-size:11px; word-break:break-all; }"
        "</style>"
        '<table class="logtable">'
    )
    for row in rows:
        color = _LEVEL_COLORS.get(row["level"], "#e0e0e0")
        exc_row = (
            f'<tr><td></td><td></td><td></td>'
            f'<td class="exc">{row["exc"]}</td></tr>'
            if row["exc"] else ""
        )
        lines.append(
            f'<tr style="color:{color}">'
            f'<td class="time">{row["time"]}</td>'
            f'<td class="level">{row["level"]}</td>'
            f'<td class="logger">{row["logger"]}</td>'
            f'<td class="msg">{row["message"]}</td>'
            f'</tr>'
            f'{exc_row}'
        )
    lines.append("</table>")
    st.markdown("\n".join(lines), unsafe_allow_html=True)


# ── Tabs ──────────────────────────────────────────────────────────────────────
tabs = st.tabs([s.upper() for s in _SERVICES])
for tab, svc in zip(tabs, _SERVICES):
    with tab:
        _render_service(svc)
