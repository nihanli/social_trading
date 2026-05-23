## 15. UI Monitoring — Grafana + Streamlit

The monitoring stack for this solo trading system uses two complementary layers:

| Layer | Tool | Role | Access |
|-------|------|------|--------|
| **Passive monitoring** | Grafana | Always-on dashboards, metrics history, automated alerts | Browser (port 3000) |
| **Active control panel** | Streamlit | Interactive ops: halt trading, close positions, view live feeds | Browser (port 8501) |

Both read from the same PostgreSQL database and Redis instance already in the stack — no extra infrastructure required.

```
PostgreSQL ──┬──▶ Grafana  (port 3000)  — read-only dashboards + alerts
             │
Redis ───────┤
             │
             └──▶ Streamlit (port 8501) — read + control panel
```

---

### 15a. Grafana — Four Dashboards

Grafana is always running in the background. It connects to PostgreSQL via the
**Grafana PostgreSQL data source** plugin (built-in, no install needed).

#### Docker Compose addition

```yaml
# docker-compose.yml — add to existing services
grafana:
  image: grafana/grafana:latest
  ports:
    - "3000:3000"
  environment:
    - GF_SECURITY_ADMIN_PASSWORD=yourpassword
    - GF_USERS_ALLOW_SIGN_UP=false
  volumes:
    - grafana_data:/var/lib/grafana
    - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
  depends_on:
    - postgres

prometheus:
  image: prom/prometheus:latest
  ports:
    - "9090:9090"
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
```

#### Grafana PostgreSQL data source config

```yaml
# monitoring/grafana/provisioning/datasources/postgres.yaml
apiVersion: 1
datasources:
  - name: TradingDB
    type: postgres
    url: postgres:5432
    database: trading
    user: trader
    secureJsonData:
      password: yourdbpassword
    jsonData:
      sslmode: disable
      maxOpenConns: 5
      maxIdleConns: 2
      timescaledb: false
```

---

#### Dashboard 1 — Signal Pipeline Health

Answers: *Is data flowing? Are we ingesting posts? Is NLP keeping up?*

| Panel | Type | Query |
|-------|------|-------|
| Posts ingested / min | Stat | Prometheus: `rate(posts_ingested_total[1m])` |
| Bot filter drop rate | Gauge | Prometheus: `rate(posts_filtered_total[5m]) / rate(posts_ingested_total[5m])` |
| NLP latency p99 | Stat | Prometheus: `histogram_quantile(0.99, sentiment_latency_ms_bucket)` |
| Posts by source (Twitter/Reddit/StockTwits) | Pie | Prometheus: `posts_ingested_total` by `source` label |
| Ticker mention volume — last 4 hours | Bar chart | PostgreSQL: |

```sql
-- Panel: Top mentioned tickers (last 4 hours)
SELECT ticker, SUM(post_count) AS mentions
FROM sentiment_aggregates
WHERE window_start > NOW() - INTERVAL '4 hours'
  AND window_minutes = 15
GROUP BY ticker
ORDER BY mentions DESC
LIMIT 15
```

```sql
-- Panel: Mention volume over time (time series per ticker)
SELECT
  window_start AS time,
  ticker,
  post_count AS mentions
FROM sentiment_aggregates
WHERE window_start > NOW() - INTERVAL '4 hours'
  AND window_minutes = 15
  AND ticker IN ('AAPL','TSLA','NVDA','GME','AMC')
ORDER BY window_start
```

---

#### Dashboard 2 — Portfolio P&L

Answers: *How is the portfolio doing? What are my open positions?*

```sql
-- Panel: Equity curve (time series)
SELECT timestamp AS time, equity
FROM account_equity
WHERE timestamp > NOW() - INTERVAL '30 days'
  AND mode = $mode          -- variable: paper or live
ORDER BY timestamp

-- Panel: Daily P&L bar chart
SELECT
  DATE_TRUNC('day', closed_at) AS day,
  SUM(net_pnl)                  AS daily_pnl
FROM trades
WHERE mode = $mode
  AND closed_at > NOW() - INTERVAL '30 days'
GROUP BY 1 ORDER BY 1

-- Panel: Current open positions table
SELECT
  ticker,
  direction,
  shares,
  entry_price,
  unrealized_pnl,
  ROUND(unrealized_pnl / (entry_price * shares) * 100, 2) AS pnl_pct,
  opened_at
FROM positions
ORDER BY opened_at DESC

-- Panel: Win rate (stat panel)
SELECT
  ROUND(100.0 * SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) / COUNT(*), 1)
    AS win_rate_pct
FROM trades
WHERE mode = $mode
  AND closed_at > NOW() - INTERVAL '30 days'

-- Panel: Closed trades table
SELECT ticker, direction, entry_price, exit_price,
       net_pnl, exit_reason, closed_at
FROM trades
WHERE mode = $mode AND closed_at IS NOT NULL
ORDER BY closed_at DESC
LIMIT 50
```

---

#### Dashboard 3 — Risk & Circuit Breaker Status

Answers: *Are any risk limits being approached? Is the system safe to keep running?*

```sql
-- Panel: Drawdown from high water mark (time series)
SELECT
  timestamp AS time,
  1 - equity / MAX(equity) OVER (ORDER BY timestamp) AS drawdown
FROM account_equity
WHERE mode = $mode
ORDER BY timestamp

-- Panel: Daily P&L % (gauge — thresholds at -2% warn, -3% critical)
SELECT
  ROUND(SUM(net_pnl) /
    (SELECT equity FROM account_equity ORDER BY timestamp DESC LIMIT 1) * 100, 2)
    AS daily_pnl_pct
FROM trades
WHERE mode = $mode AND opened_at::date = CURRENT_DATE

-- Panel: Social media exposure % (stat)
SELECT
  ROUND(SUM(ABS(shares * entry_price)) /
    (SELECT equity FROM account_equity ORDER BY timestamp DESC LIMIT 1) * 100, 1)
    AS social_exposure_pct
FROM positions

-- Panel: Signals approved vs rejected today (pie)
SELECT approved, COUNT(*) AS cnt
FROM signals
WHERE generated_at::date = CURRENT_DATE
GROUP BY approved
```

**Thresholds to set in Grafana panels:**

| Metric | Warning | Critical |
|--------|---------|---------|
| Daily P&L % | < -2% | < -3% |
| Drawdown from HWM | > 8% | > 15% |
| Social exposure | > 15% | > 20% |
| NLP latency p99 | > 300ms | > 500ms |

---

#### Dashboard 4 — Live Signal Feed

Answers: *What signals are being generated right now? What quality are they?*

```sql
-- Panel: Recent signals table (refresh every 10s)
SELECT
  ticker,
  direction,
  ROUND(confidence::numeric, 2)      AS quality,
  ROUND(sentiment_score::numeric, 2) AS sentiment,
  ROUND(mention_zscore::numeric, 1)  AS vol_z,
  approved,
  executed,
  AGE(NOW(), generated_at)           AS age
FROM signals
ORDER BY generated_at DESC
LIMIT 30

-- Panel: Signal direction breakdown today (bar)
SELECT direction, COUNT(*) AS count
FROM signals
WHERE generated_at::date = CURRENT_DATE
GROUP BY direction

-- Panel: Average signal quality over time
SELECT
  DATE_TRUNC('hour', generated_at) AS time,
  AVG(confidence) AS avg_quality
FROM signals
WHERE generated_at > NOW() - INTERVAL '7 days'
GROUP BY 1 ORDER BY 1

-- Panel: Top tickers by signal count this week
SELECT ticker, COUNT(*) AS signals,
       AVG(confidence) AS avg_quality
FROM signals
WHERE generated_at > NOW() - INTERVAL '7 days'
GROUP BY ticker
ORDER BY signals DESC
LIMIT 10
```

---

#### Grafana Alerting Rules

```yaml
# monitoring/grafana/provisioning/alerting/rules.yaml
apiVersion: 1
groups:
  - orgId: 1
    name: trading-alerts
    interval: 30s
    rules:
      - uid: daily-loss-breach
        title: Daily Loss Limit Breached
        condition: C
        data:
          - refId: A
            datasourceUid: TradingDB
            model:
              rawSql: |
                SELECT COALESCE(SUM(net_pnl), 0) /
                  (SELECT equity FROM account_equity ORDER BY timestamp DESC LIMIT 1)
                FROM trades WHERE opened_at::date = CURRENT_DATE AND mode = 'live'
        noDataState: OK
        execErrState: Alerting
        for: 1m
        annotations:
          summary: Daily P&L below -3% threshold
        labels:
          severity: critical

      - uid: drawdown-warning
        title: Drawdown Warning 10%
        condition: C
        for: 5m
        annotations:
          summary: Portfolio drawdown exceeded 10% from high water mark
        labels:
          severity: warning

      - uid: ibkr-connection-lost
        title: IBKR Connection Lost
        condition: C
        data:
          - refId: A
            datasourceUid: prometheus
            model:
              expr: up{job="ibkr_connection"} == 0
        for: 2m
        annotations:
          summary: Interactive Brokers connection dropped
        labels:
          severity: critical
```

---

### 15b. Streamlit — Ops Control Panel

The Streamlit app is the **active control surface** — opened during market hours to monitor
live activity and send commands. It reads from PostgreSQL and Redis, and writes control
signals back to Redis for the execution engine to act on.

#### File structure

```
monitoring/
├── streamlit/
│   ├── app.py              ← main entry point
│   ├── pages/
│   │   ├── 1_positions.py  ← open positions detail
│   │   ├── 2_signals.py    ← live signal feed
│   │   ├── 3_trades.py     ← trade history & analytics
│   │   ├── 4_sentiment.py  ← social media sentiment view
│   │   ├── 5_config.py     ← ⚙️ system configuration (all parameters)
│   │   └── 6_optimize.py   ← 🔬 parameter optimization & run history
│   └── utils/
│       ├── db.py           ← PostgreSQL helpers
│       └── redis_ctrl.py   ← Redis control commands + config load/save
```

#### `monitoring/streamlit/utils/db.py`

```python
import psycopg2, pandas as pd
import streamlit as st

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost", dbname="trading",
        user="trader", password="yourpassword"
    )

def query(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn, params=params)
```

#### `monitoring/streamlit/utils/redis_ctrl.py`

```python
import redis, json
from datetime import datetime
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))
from config.system_config import SystemConfig

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def get_system_state() -> dict:
    return {
        "circuit":         r.get("trading:circuit_state") or "ALLOW",
        "halt_new":        r.get("trading:halt_new") == "1",
        "daily_pnl_pct":   float(r.get("trading:daily_pnl_pct") or 0),
        "drawdown":        float(r.get("trading:drawdown") or 0),
        "social_exposure": float(r.get("trading:social_exposure") or 0),
        "vix":             float(r.get("market:vix") or 0),
        "mode":            r.get("trading:mode") or "paper",
    }

def send_command(cmd: str, payload: dict = None):
    message = json.dumps({"cmd": cmd, "payload": payload or {},
                          "ts": datetime.utcnow().isoformat()})
    r.publish("trading:commands", message)

def halt_new_trades():
    r.set("trading:halt_new", "1")
    send_command("HALT_NEW")

def resume_trading():
    r.delete("trading:halt_new")
    send_command("RESUME")

def close_all_positions():
    send_command("CLOSE_ALL")

def close_position(ticker: str):
    send_command("CLOSE_TICKER", {"ticker": ticker})

# ── Config helpers ───────────────────────────────────────────────────────────

def load_config() -> SystemConfig:
    return SystemConfig.load(r)

def save_config(cfg: SystemConfig) -> list[str]:
    """Validate then persist. Returns list of errors (empty = success)."""
    errors = cfg.validate()
    if not errors:
        cfg.save(r)
        send_command("CONFIG_UPDATED", {"ts": datetime.utcnow().isoformat()})
    return errors

def get_watchlist() -> list[str]:
    active = r.zrange("watchlist:active", 0, -1)
    seeds  = r.smembers("watchlist:seed")
    return sorted(set(active) | set(seeds))

def pin_ticker(ticker: str):
    r.sadd("watchlist:seed", ticker.upper())
    r.zadd("watchlist:active", {ticker.upper(): __import__("time").time()})

def unpin_ticker(ticker: str):
    r.srem("watchlist:seed", ticker.upper())
```

#### `monitoring/streamlit/app.py` — Main Dashboard

```python
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.db import query
from utils.redis_ctrl import get_system_state, halt_new_trades, resume_trading, close_all_positions

st.set_page_config(
    page_title="Social Trading Monitor",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Auto-refresh every 10 seconds ──
st.markdown(
    '<meta http-equiv="refresh" content="10">',
    unsafe_allow_html=True
)

# ═══════════════════════════════════════
# SIDEBAR — System Controls
# ═══════════════════════════════════════
with st.sidebar:
    st.title("⚙️ System Controls")
    state = get_system_state()

    # Trading mode badge
    mode_color = "🟢" if state["mode"] == "live" else "🟡"
    st.markdown(f"**Mode:** {mode_color} {state['mode'].upper()}")

    # Circuit breaker status
    cb_color = {"ALLOW": "🟢", "REDUCE_25": "🟡", "REDUCE_50": "🟠",
                "HALT_NEW": "🔴", "FULL_HALT": "🚨"}.get(state["circuit"], "⚪")
    st.markdown(f"**Circuit Breaker:** {cb_color} {state['circuit']}")

    st.divider()

    # Halt / Resume toggle
    if state["halt_new"]:
        st.warning("⛔ New trades HALTED")
        if st.button("▶️ Resume Trading", use_container_width=True):
            resume_trading()
            st.success("Resume command sent")
            st.rerun()
    else:
        if st.button("⏸ Halt New Trades", use_container_width=True, type="primary"):
            halt_new_trades()
            st.warning("Halt command sent")
            st.rerun()

    st.divider()

    # Emergency close
    with st.expander("🚨 Emergency Actions", expanded=False):
        st.warning("These actions are immediate and irreversible.")
        if st.button("🔴 Close ALL Positions", use_container_width=True):
            close_all_positions()
            st.error("Close-all command sent to execution engine")

    st.divider()

    # Risk gauges
    st.markdown("**Daily P&L**")
    pnl_color = "normal" if state["daily_pnl_pct"] > -2 else "inverse"
    st.metric("", f"{state['daily_pnl_pct']:+.2f}%",
              delta_color=pnl_color)

    st.markdown("**Drawdown from HWM**")
    st.progress(min(state["drawdown"] / 0.20, 1.0),
                text=f"{state['drawdown']:.1%}")

    st.markdown("**Social Exposure**")
    st.progress(min(state["social_exposure"] / 0.20, 1.0),
                text=f"{state['social_exposure']:.1%} / 20%")

    st.metric("VIX", f"{state['vix']:.1f}",
              help="Position size scalar applied at current VIX level")


# ═══════════════════════════════════════
# MAIN — KPI Row
# ═══════════════════════════════════════
st.title("📡 Social Trading Monitor")

equity_df = query(
    "SELECT equity FROM account_equity ORDER BY timestamp DESC LIMIT 1"
)
daily_pnl_df = query("""
    SELECT COALESCE(SUM(net_pnl), 0) AS pnl,
           COUNT(*) FILTER (WHERE net_pnl > 0) AS wins,
           COUNT(*) AS total
    FROM trades
    WHERE opened_at::date = CURRENT_DATE AND mode = 'live'
""")
open_pos_df = query("SELECT COUNT(*) AS cnt FROM positions")
signals_today_df = query("""
    SELECT COUNT(*) FILTER (WHERE executed) AS executed,
           COUNT(*) AS total
    FROM signals WHERE generated_at::date = CURRENT_DATE
""")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Portfolio Equity",
            f"${equity_df.iloc[0,0]:,.0f}" if not equity_df.empty else "—")
col2.metric("Today's P&L",
            f"${daily_pnl_df.iloc[0]['pnl']:+,.0f}" if not daily_pnl_df.empty else "—")
col3.metric("Open Positions",
            int(open_pos_df.iloc[0,0]))
col4.metric("Win Rate Today",
            f"{100*daily_pnl_df.iloc[0]['wins'] / max(daily_pnl_df.iloc[0]['total'],1):.0f}%"
            if not daily_pnl_df.empty else "—")
col5.metric("Signals → Trades",
            f"{signals_today_df.iloc[0]['executed']}/{signals_today_df.iloc[0]['total']}"
            if not signals_today_df.empty else "—")


# ═══════════════════════════════════════
# EQUITY CURVE
# ═══════════════════════════════════════
eq_hist = query("""
    SELECT timestamp, equity FROM account_equity
    WHERE timestamp > NOW() - INTERVAL '30 days'
    ORDER BY timestamp
""")
if not eq_hist.empty:
    fig = go.Figure(go.Scatter(
        x=eq_hist["timestamp"], y=eq_hist["equity"],
        fill="tozeroy", line=dict(color="#2196F3", width=2),
        name="Equity"
    ))
    fig.update_layout(
        title="Portfolio Equity (30 days)",
        height=220, margin=dict(t=30, b=20, l=10, r=10),
        xaxis_title=None, yaxis_title="USD"
    )
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════
# OPEN POSITIONS + RECENT SIGNALS
# ═══════════════════════════════════════
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📂 Open Positions")
    positions = query("""
        SELECT ticker, direction, shares, entry_price,
               ROUND(unrealized_pnl::numeric, 2)      AS unrealized_pnl,
               ROUND((unrealized_pnl /
                 NULLIF(entry_price * shares, 0) * 100)::numeric, 1) AS pnl_pct,
               TO_CHAR(opened_at, 'HH24:MI') AS opened
        FROM positions ORDER BY opened_at DESC
    """)
    if positions.empty:
        st.info("No open positions")
    else:
        # Colour rows by direction
        def color_direction(val):
            return "background-color: #d4edda" if val == "LONG" \
                else "background-color: #f8d7da"
        st.dataframe(
            positions.style.applymap(color_direction, subset=["direction"]),
            use_container_width=True, hide_index=True
        )

        # Per-position close buttons
        for _, row in positions.iterrows():
            if st.button(f"Close {row['ticker']}", key=f"close_{row['ticker']}"):
                from utils.redis_ctrl import close_position
                close_position(row["ticker"])
                st.warning(f"Close order sent for {row['ticker']}")

with col_right:
    st.subheader("⚡ Recent Signals")
    signals = query("""
        SELECT ticker, direction,
               ROUND(confidence::numeric, 2)      AS quality,
               ROUND(sentiment_score::numeric, 2) AS sentiment,
               ROUND(mention_zscore::numeric, 1)  AS vol_z,
               approved, executed,
               TO_CHAR(generated_at, 'HH24:MI:SS') AS time
        FROM signals
        ORDER BY generated_at DESC LIMIT 20
    """)
    if not signals.empty:
        def color_signal(val):
            if val == "LONG":  return "background-color: #d4edda"
            if val == "SHORT": return "background-color: #f8d7da"
            return ""
        st.dataframe(
            signals.style.applymap(color_signal, subset=["direction"]),
            use_container_width=True, hide_index=True
        )


# ═══════════════════════════════════════
# SENTIMENT HEATMAP
# ═══════════════════════════════════════
st.subheader("🔥 Sentiment Heatmap — Top Tickers (Last Hour)")
heatmap_df = query("""
    SELECT ticker,
           SUM(post_count)          AS mentions,
           AVG(weighted_score)      AS avg_sentiment,
           AVG(mention_zscore)      AS vol_z
    FROM sentiment_aggregates
    WHERE window_start > NOW() - INTERVAL '1 hour'
      AND window_minutes = 15
    GROUP BY ticker
    ORDER BY mentions DESC LIMIT 20
""")
if not heatmap_df.empty:
    fig2 = go.Figure(go.Bar(
        x=heatmap_df["ticker"],
        y=heatmap_df["mentions"],
        marker=dict(
            color=heatmap_df["avg_sentiment"],
            colorscale="RdYlGn",
            cmin=-1, cmax=1,
            colorbar=dict(title="Sentiment", thickness=12)
        ),
        text=heatmap_df["vol_z"].round(1).astype(str) + "σ",
        textposition="outside"
    ))
    fig2.update_layout(
        title="Bar height = mention count  |  Colour = sentiment  |  Label = volume Z-score",
        height=280, margin=dict(t=40, b=20, l=10, r=10),
        xaxis_title=None, yaxis_title="Mentions"
    )
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No sentiment data in the last hour")


# ═══════════════════════════════════════
# RECENT CLOSED TRADES
# ═══════════════════════════════════════
st.subheader("📒 Recent Closed Trades")
trades = query("""
    SELECT ticker, direction, shares, entry_price, exit_price,
           ROUND(net_pnl::numeric, 2) AS net_pnl,
           exit_reason,
           TO_CHAR(opened_at,  'MM-DD HH24:MI') AS opened,
           TO_CHAR(closed_at,  'MM-DD HH24:MI') AS closed
    FROM trades
    WHERE closed_at IS NOT NULL
    ORDER BY closed_at DESC LIMIT 30
""")
if not trades.empty:
    def color_pnl(val):
        try:
            return "color: green; font-weight: bold" if float(val) > 0 \
                else "color: red; font-weight: bold"
        except Exception:
            return ""
    st.dataframe(
        trades.style.applymap(color_pnl, subset=["net_pnl"]),
        use_container_width=True, hide_index=True
    )
```

#### `monitoring/streamlit/pages/2_signals.py` — Signal Feed Page

```python
import streamlit as st
from utils.db import query
import plotly.express as px

st.title("⚡ Signal Feed")

# Signal quality distribution
quality_df = query("""
    SELECT ROUND(confidence::numeric, 1) AS quality_bucket, COUNT(*) AS count
    FROM signals
    WHERE generated_at > NOW() - INTERVAL '7 days'
    GROUP BY 1 ORDER BY 1
""")
if not quality_df.empty:
    fig = px.bar(quality_df, x="quality_bucket", y="count",
                 title="Signal Quality Distribution (7 days)",
                 labels={"quality_bucket": "Quality Score", "count": "Count"})
    st.plotly_chart(fig, use_container_width=True)

# Signal volume over time
signal_ts = query("""
    SELECT DATE_TRUNC('hour', generated_at) AS hour,
           direction, COUNT(*) AS count
    FROM signals
    WHERE generated_at > NOW() - INTERVAL '3 days'
    GROUP BY 1, 2 ORDER BY 1
""")
if not signal_ts.empty:
    fig2 = px.bar(signal_ts, x="hour", y="count", color="direction",
                  title="Signals per Hour",
                  color_discrete_map={"LONG": "green", "SHORT": "red", "FLAT": "gray"})
    st.plotly_chart(fig2, use_container_width=True)

# Full signal table with filters
st.subheader("All Signals")
col1, col2 = st.columns(2)
ticker_filter = col1.text_input("Filter by ticker")
direction_filter = col2.selectbox("Direction", ["All", "LONG", "SHORT", "FLAT"])

where = "WHERE generated_at > NOW() - INTERVAL '7 days'"
if ticker_filter:
    where += f" AND ticker ILIKE '%{ticker_filter}%'"
if direction_filter != "All":
    where += f" AND direction = '{direction_filter}'"

full_signals = query(f"""
    SELECT ticker, direction, confidence, sentiment_score,
           mention_zscore, approved, executed, generated_at
    FROM signals {where}
    ORDER BY generated_at DESC LIMIT 200
""")
st.dataframe(full_signals, use_container_width=True, hide_index=True)
```

#### `monitoring/streamlit/pages/5_config.py` — System Configuration Page

```python
import streamlit as st
from utils.redis_ctrl import load_config, save_config, get_watchlist, pin_ticker, unpin_ticker
from config.system_config import SystemConfig

st.set_page_config(page_title="System Configuration", page_icon="⚙️", layout="wide")
st.title("⚙️ System Configuration")
st.caption("Changes take effect within one service loop cycle (~1 min). No restarts required.")

cfg = load_config()

# ═══════════════════════════════════════════════════════════════
# WATCHLIST MANAGEMENT
# ═══════════════════════════════════════════════════════════════
st.header("📋 Watchlist Management")
col_wl, col_seed = st.columns(2)

with col_wl:
    st.subheader("Active Watchlist")
    watchlist = get_watchlist()
    st.write(f"**{len(watchlist)} tickers currently monitored**")
    st.dataframe({"Ticker": watchlist}, use_container_width=True, hide_index=True)

with col_seed:
    st.subheader("Pinned (Seed) Tickers")
    st.caption("Pinned tickers are never auto-expired, even if silent for 48h.")
    new_pin = st.text_input("Pin a ticker", placeholder="e.g. NVDA").upper()
    if st.button("📌 Pin Ticker") and new_pin:
        pin_ticker(new_pin)
        st.success(f"{new_pin} pinned")
        st.rerun()

    unpin_ticker_input = st.text_input("Unpin a ticker", placeholder="e.g. AAPL").upper()
    if st.button("🗑 Unpin Ticker") and unpin_ticker_input:
        unpin_ticker(unpin_ticker_input)
        st.warning(f"{unpin_ticker_input} unpinned")
        st.rerun()

st.divider()

# ═══════════════════════════════════════════════════════════════
# DISCOVERY & SPIKE DETECTION
# ═══════════════════════════════════════════════════════════════
st.header("📡 Discovery & Spike Detection")
col1, col2, col3 = st.columns(3)

with col1:
    cfg.spike_zscore_threshold = st.slider(
        "Spike Z-score threshold", 1.0, 4.0, cfg.spike_zscore_threshold, 0.1,
        help="Higher = fewer, stronger signals. Lower = more signals, more noise."
    )
    cfg.mention_window_minutes = st.number_input(
        "Mention count window (min)", 15, 240, cfg.mention_window_minutes, 15
    )

with col2:
    cfg.x_search_max_results = st.slider(
        "X posts pulled per spike", 10, 100, cfg.x_search_max_results, 10,
        help=f"Cost: ${cfg.x_search_max_results * 0.005:.2f} per spike at current setting."
    )
    cfg.counts_poll_interval_sec = st.select_slider(
        "X Counts poll interval (sec)", [60, 120, 300, 600], cfg.counts_poll_interval_sec
    )

with col3:
    cfg.watchlist_stale_hours = st.number_input(
        "Watchlist stale expiry (hours)", 6, 168, cfg.watchlist_stale_hours, 6
    )
    cfg.watchlist_min_adv_usd = st.number_input(
        "Min ADV for watchlist ($)", 100_000, 5_000_000,
        cfg.watchlist_min_adv_usd, 100_000, format="%d"
    )

st.divider()

# ═══════════════════════════════════════════════════════════════
# SIGNAL QUALITY
# ═══════════════════════════════════════════════════════════════
st.header("⚡ Signal Quality")
col_s1, col_s2 = st.columns(2)

with col_s1:
    cfg.signal_quality_threshold = st.slider(
        "Minimum signal quality score", 0.3, 0.9, cfg.signal_quality_threshold, 0.05,
        help="Signals below this score are discarded. Higher = fewer but higher-conviction trades."
    )
    cfg.sentiment_strength_min = st.slider(
        "Min |sentiment| to fire", 0.1, 0.8, cfg.sentiment_strength_min, 0.05
    )
    cfg.reactive_price_threshold = st.slider(
        "Reactive price threshold", 0.05, 0.25, cfg.reactive_price_threshold, 0.01,
        help="Price move % before mention that classifies a signal as 'reactive' (penalised)."
    )

with col_s2:
    st.subheader("Factor Weights (must sum to 1.0)")
    cfg.w_volume      = st.slider("Volume Z-score weight",      0.0, 0.6, cfg.w_volume,      0.05)
    cfg.w_sentiment   = st.slider("Sentiment strength weight",  0.0, 0.6, cfg.w_sentiment,   0.05)
    cfg.w_proactivity = st.slider("Proactivity weight",         0.0, 0.5, cfg.w_proactivity, 0.05)
    cfg.w_momentum    = st.slider("Price momentum weight",      0.0, 0.4, cfg.w_momentum,    0.05)
    cfg.w_convergence = st.slider("Cross-platform weight",      0.0, 0.3, cfg.w_convergence, 0.05)
    weight_sum = cfg.w_volume + cfg.w_sentiment + cfg.w_proactivity + cfg.w_momentum + cfg.w_convergence
    color = "green" if abs(weight_sum - 1.0) < 0.01 else "red"
    st.markdown(f"**Weight sum: :{color}[{weight_sum:.2f}]** (must equal 1.00)")

st.divider()

# ═══════════════════════════════════════════════════════════════
# POSITION SIZING & EXIT RULES
# ═══════════════════════════════════════════════════════════════
st.header("📐 Position Sizing & Exit Rules")
col_p1, col_p2, col_p3 = st.columns(3)

with col_p1:
    st.subheader("Position Sizing")
    cfg.max_position_pct = st.slider(
        "Max position size (%)", 0.5, 5.0, cfg.max_position_pct * 100, 0.25
    ) / 100
    cfg.half_kelly_fraction = st.slider(
        "Kelly fraction", 0.1, 1.0, cfg.half_kelly_fraction, 0.1,
        help="0.5 = Half-Kelly (recommended). Lower = more conservative."
    )
    cfg.sigma_target = st.slider(
        "Target annual volatility", 0.05, 0.40, cfg.sigma_target, 0.01,
        help="Position sizes scale inversely with realized volatility relative to this target."
    )

with col_p2:
    st.subheader("Exit Triggers")
    cfg.take_profit_pct = st.slider(
        "Take profit (%)", 1.0, 15.0, cfg.take_profit_pct * 100, 0.5
    ) / 100
    cfg.trailing_stop_pct = st.slider(
        "Trailing stop (%)", 2.0, 20.0, cfg.trailing_stop_pct * 100, 0.5
    ) / 100
    cfg.atr_multiplier = st.slider(
        "ATR stop multiplier", 0.5, 5.0, cfg.atr_multiplier, 0.25
    )

with col_p3:
    st.subheader("Time & Sentiment Exits")
    cfg.max_hold_hours = st.number_input(
        "Max hold time (hours)", 4, 120, cfg.max_hold_hours, 4
    )
    cfg.signal_reversal_threshold = st.slider(
        "Sentiment reversal threshold", -0.8, -0.05,
        cfg.signal_reversal_threshold, 0.05
    )
    cfg.mention_decay_threshold = st.slider(
        "Mention decay exit (fraction of peak)", 0.05, 0.5,
        cfg.mention_decay_threshold, 0.05,
        help="Exit when current mentions fall below this fraction of peak mentions."
    )

st.divider()

# ═══════════════════════════════════════════════════════════════
# RISK & CIRCUIT BREAKERS
# ═══════════════════════════════════════════════════════════════
st.header("🛡️ Risk & Circuit Breakers")
col_r1, col_r2 = st.columns(2)

with col_r1:
    st.subheader("Loss Limits")
    cfg.loss_limit_daily = st.slider(
        "Daily loss limit (%) — halt new trades", 1.0, 10.0,
        cfg.loss_limit_daily * 100, 0.5
    ) / 100
    cfg.loss_limit_weekly = st.slider(
        "Weekly loss limit (%) — reduce 50%", 2.0, 20.0,
        cfg.loss_limit_weekly * 100, 0.5
    ) / 100
    cfg.drawdown_halt = st.slider(
        "Max drawdown (%) — full halt", 5.0, 40.0,
        cfg.drawdown_halt * 100, 1.0
    ) / 100
    cfg.max_social_allocation = st.slider(
        "Max social media allocation (%)", 5.0, 50.0,
        cfg.max_social_allocation * 100, 5.0
    ) / 100

with col_r2:
    st.subheader("VIX Regime Thresholds")
    cfg.vix_crisis            = st.number_input("VIX crisis (→ 0% size)",   20, 80, int(cfg.vix_crisis))
    cfg.vix_high_fear         = st.number_input("VIX high fear (→ 25%)",    15, 60, int(cfg.vix_high_fear))
    cfg.vix_elevated          = st.number_input("VIX elevated (→ 50%)",     10, 50, int(cfg.vix_elevated))
    cfg.vix_slightly_elevated = st.number_input("VIX slightly elevated (→ 75%)", 10, 40, int(cfg.vix_slightly_elevated))

st.divider()

# ═══════════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════════
col_save, col_reset = st.columns([3, 1])

with col_save:
    if st.button("💾 Save Configuration", type="primary", use_container_width=True):
        errors = save_config(cfg)
        if errors:
            for e in errors:
                st.error(e)
        else:
            st.success("✅ Configuration saved. All services will pick up changes within 1 minute.")
            st.balloons()

with col_reset:
    if st.button("↩️ Reset to Defaults", use_container_width=True):
        save_config(SystemConfig())   # save fresh defaults
        st.warning("Reset to defaults. Reloading...")
        st.rerun()
```

#### `monitoring/streamlit/pages/3_trades.py` — Trade Analytics Page

```python
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.db import query

st.title("📊 Trade Analytics")

mode = st.radio("Mode", ["paper", "live"], horizontal=True)

# Performance stats
stats = query(f"""
    SELECT
      COUNT(*)                                              AS total_trades,
      COUNT(*) FILTER (WHERE net_pnl > 0)                  AS wins,
      ROUND(AVG(net_pnl)::numeric, 2)                      AS avg_pnl,
      ROUND(MAX(net_pnl)::numeric, 2)                      AS best_trade,
      ROUND(MIN(net_pnl)::numeric, 2)                      AS worst_trade,
      ROUND(SUM(net_pnl)::numeric, 2)                      AS total_pnl,
      ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at))
            / 3600)::numeric, 1)                           AS avg_hold_hrs
    FROM trades
    WHERE mode = '{mode}' AND closed_at IS NOT NULL
""")
if not stats.empty:
    s = stats.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Trades", int(s["total_trades"]))
    c2.metric("Win Rate",
              f"{100*s['wins']/max(s['total_trades'],1):.1f}%")
    c3.metric("Total P&L", f"${s['total_pnl']:+,.2f}")
    c4.metric("Avg Hold Time", f"{s['avg_hold_hrs']} hrs")

# Cumulative P&L curve
cum_pnl = query(f"""
    SELECT closed_at AS time,
           SUM(net_pnl) OVER (ORDER BY closed_at) AS cumulative_pnl
    FROM trades
    WHERE mode = '{mode}' AND closed_at IS NOT NULL
    ORDER BY closed_at
""")
if not cum_pnl.empty:
    fig = go.Figure(go.Scatter(
        x=cum_pnl["time"], y=cum_pnl["cumulative_pnl"],
        fill="tozeroy",
        line=dict(color="green" if cum_pnl["cumulative_pnl"].iloc[-1] > 0 else "red")
    ))
    fig.update_layout(title="Cumulative P&L", height=250,
                      margin=dict(t=30, b=20))
    st.plotly_chart(fig, use_container_width=True)

# P&L by exit reason
by_exit = query(f"""
    SELECT exit_reason,
           COUNT(*)                         AS trades,
           ROUND(SUM(net_pnl)::numeric, 2)  AS total_pnl,
           ROUND(AVG(net_pnl)::numeric, 2)  AS avg_pnl
    FROM trades
    WHERE mode = '{mode}' AND closed_at IS NOT NULL
    GROUP BY exit_reason ORDER BY total_pnl DESC
""")
if not by_exit.empty:
    st.subheader("P&L by Exit Reason")
    st.dataframe(by_exit, use_container_width=True, hide_index=True)
```

#### `monitoring/streamlit/pages/6_optimize.py` — Parameter Optimization Page

Four tabs: run history, sensitivity analysis, auto-suggestions, and walk-forward grid search.

```python
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from utils.db import query
from utils.redis_ctrl import load_config, save_config

st.set_page_config(page_title="Parameter Optimization", page_icon="🔬", layout="wide")
st.title("🔬 Parameter Optimization")
st.caption("Analyze past runs to identify which config settings produce the best results.")

tab_history, tab_sensitivity, tab_suggest, tab_grid = st.tabs([
    "📋 Run History", "📈 Sensitivity Analysis", "💡 Auto-Suggestions", "⚙️ Grid Search"
])

# ── Shared data: all config runs ────────────────────────────────────────────
runs_df = query("""
    SELECT id, run_date, mode, config_hash,
           total_pnl, total_trades, win_rate, sharpe_ratio, max_drawdown,
           avg_hold_hours, profit_factor,
           exits_take_profit, exits_time_stop, exits_atr_stop,
           exits_trailing_stop, exits_sentiment_reversal, exits_mention_decay,
           signals_generated, signals_executed, avg_signal_quality,
           config_snapshot
    FROM config_runs
    ORDER BY run_date DESC
""")

if runs_df.empty:
    st.info("No run history yet. Snapshots are saved automatically at end of each trading session.")
    st.stop()

# Parse config snapshots into flat columns for analysis
cfg_cols = pd.json_normalize(runs_df["config_snapshot"].apply(json.loads))
analysis_df = pd.concat([
    runs_df[["run_date","mode","config_hash","total_pnl","win_rate",
             "sharpe_ratio","max_drawdown","avg_hold_hours","profit_factor",
             "exits_take_profit","exits_time_stop","exits_atr_stop",
             "exits_trailing_stop","exits_sentiment_reversal","exits_mention_decay",
             "signals_generated","signals_executed","avg_signal_quality"]].reset_index(drop=True),
    cfg_cols.reset_index(drop=True)
], axis=1)


# ════════════════════════════════════════════════════════════════
# TAB 1 — RUN HISTORY
# ════════════════════════════════════════════════════════════════
with tab_history:
    st.subheader("All Recorded Sessions")

    mode_filter = st.radio("Mode", ["All", "paper", "live"], horizontal=True)
    df = analysis_df if mode_filter == "All" else analysis_df[analysis_df["mode"] == mode_filter]

    # Summary KPIs
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sessions recorded", len(df))
    c2.metric("Best Sharpe",       f"{df['sharpe_ratio'].max():.3f}")
    c3.metric("Best win rate",     f"{df['win_rate'].max():.1%}")
    c4.metric("Best single-day P&L", f"${df['total_pnl'].max():,.0f}")

    # Equity-equivalent line chart
    pnl_ts = df[["run_date","total_pnl"]].dropna().sort_values("run_date")
    pnl_ts["cumulative_pnl"] = pnl_ts["total_pnl"].cumsum()
    fig = go.Figure(go.Scatter(
        x=pnl_ts["run_date"], y=pnl_ts["cumulative_pnl"],
        fill="tozeroy",
        line=dict(color="green" if pnl_ts["cumulative_pnl"].iloc[-1] > 0 else "red")
    ))
    fig.update_layout(title="Cumulative P&L across all recorded sessions",
                      height=220, margin=dict(t=30,b=20))
    st.plotly_chart(fig, use_container_width=True)

    # Full table
    display_cols = ["run_date","mode","config_hash","total_pnl","win_rate",
                    "sharpe_ratio","max_drawdown","avg_hold_hours","total_trades"]
    st.dataframe(df[display_cols].style.format({
        "total_pnl":    "${:,.2f}",
        "win_rate":     "{:.1%}",
        "sharpe_ratio": "{:.3f}",
        "max_drawdown": "{:.1%}",
        "avg_hold_hours": "{:.1f}h",
    }), use_container_width=True, hide_index=True)

    # Config diff viewer
    st.subheader("Config Diff Viewer")
    hashes = df["config_hash"].dropna().unique().tolist()
    if len(hashes) >= 2:
        col_a, col_b = st.columns(2)
        hash_a = col_a.selectbox("Run A", hashes, index=0)
        hash_b = col_b.selectbox("Run B", hashes, index=1)
        snap_a = json.loads(runs_df[runs_df["config_hash"]==hash_a]["config_snapshot"].iloc[0])
        snap_b = json.loads(runs_df[runs_df["config_hash"]==hash_b]["config_snapshot"].iloc[0])
        diffs = [(k, snap_a.get(k), snap_b.get(k))
                 for k in set(snap_a) | set(snap_b) if snap_a.get(k) != snap_b.get(k)]
        if diffs:
            st.dataframe(pd.DataFrame(diffs, columns=["Parameter", f"Run A ({hash_a})", f"Run B ({hash_b})"]),
                         use_container_width=True, hide_index=True)
        else:
            st.success("Configs are identical.")


# ════════════════════════════════════════════════════════════════
# TAB 2 — SENSITIVITY ANALYSIS
# ════════════════════════════════════════════════════════════════
with tab_sensitivity:
    st.subheader("How does each parameter correlate with performance?")
    st.caption("Requires at least 5 sessions with varied configs for meaningful results.")

    TUNABLE_PARAMS = [
        "signal_quality_threshold", "spike_zscore_threshold", "sentiment_strength_min",
        "w_volume", "w_sentiment", "w_proactivity", "w_momentum", "w_convergence",
        "max_position_pct", "take_profit_pct", "trailing_stop_pct", "atr_multiplier",
        "max_hold_hours", "signal_reversal_threshold", "mention_decay_threshold",
        "loss_limit_daily", "drawdown_halt", "max_social_allocation",
        "half_kelly_fraction", "sigma_target",
    ]
    PERF_METRICS = ["sharpe_ratio", "win_rate", "total_pnl", "max_drawdown", "avg_hold_hours"]

    available_params = [p for p in TUNABLE_PARAMS if p in analysis_df.columns
                        and analysis_df[p].nunique() > 1]

    col_p, col_m = st.columns(2)
    param   = col_p.selectbox("Parameter (X axis)", available_params)
    metric  = col_m.selectbox("Performance metric (Y axis)", PERF_METRICS)

    plot_df = analysis_df[[param, metric, "mode"]].dropna()
    if len(plot_df) >= 3:
        fig = px.scatter(
            plot_df, x=param, y=metric, color="mode",
            trendline="ols",
            title=f"{param}  vs  {metric}  ({len(plot_df)} sessions)",
            labels={param: param, metric: metric},
        )
        st.plotly_chart(fig, use_container_width=True)

        # Correlation table across all param pairs
        if st.checkbox("Show full correlation matrix"):
            corr_df = analysis_df[available_params + PERF_METRICS].corr()[PERF_METRICS].loc[available_params]
            fig2 = px.imshow(corr_df, text_auto=".2f", color_continuous_scale="RdYlGn",
                             zmin=-1, zmax=1, title="Parameter → Metric correlations")
            st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info(f"Need at least 3 sessions with varied {param} values. Currently have {len(plot_df)}.")


# ════════════════════════════════════════════════════════════════
# TAB 3 — AUTO-SUGGESTIONS
# ════════════════════════════════════════════════════════════════
with tab_suggest:
    st.subheader("💡 Rule-Based Suggestions from Recent Sessions")

    N = st.slider("Analyse last N sessions", 3, 30, 10)
    recent = analysis_df.head(N)

    suggestions = []

    # Win rate too low → tighten signal quality
    if recent["win_rate"].mean() < 0.45:
        suggestions.append({
            "Issue": f"Win rate averaging {recent['win_rate'].mean():.1%} (below 45%)",
            "Suggestion": "Raise `signal_quality_threshold`",
            "Direction": "↑ increase",
            "Parameter": "signal_quality_threshold",
        })

    # TIME_STOP dominates exits → take profit is too high or hold too long
    if "exits_time_stop" in recent and "total_trades" in recent:
        time_stop_pct = recent["exits_time_stop"].sum() / max(recent["total_trades"].sum(), 1)
        if time_stop_pct > 0.50:
            suggestions.append({
                "Issue": f"Time stop is {time_stop_pct:.0%} of all exits — signal decays before target",
                "Suggestion": "Lower `take_profit_pct` or `max_hold_hours`",
                "Direction": "↓ decrease",
                "Parameter": "take_profit_pct",
            })

    # ATR_STOP dominates → stops too tight
    if "exits_atr_stop" in recent and "total_trades" in recent:
        atr_pct = recent["exits_atr_stop"].sum() / max(recent["total_trades"].sum(), 1)
        if atr_pct > 0.40:
            suggestions.append({
                "Issue": f"ATR stop fires on {atr_pct:.0%} of trades — stops may be too tight",
                "Suggestion": "Lower `atr_multiplier` or widen `trailing_stop_pct`",
                "Direction": "↑ increase",
                "Parameter": "atr_multiplier",
            })

    # SENTIMENT_REVERSAL exits are common → threshold too loose
    if "exits_sentiment_reversal" in recent and "total_trades" in recent:
        rev_pct = recent["exits_sentiment_reversal"].sum() / max(recent["total_trades"].sum(), 1)
        if rev_pct > 0.25:
            suggestions.append({
                "Issue": f"Sentiment reversal exits: {rev_pct:.0%} — signals are reversing before exit",
                "Suggestion": "Raise `signal_quality_threshold` or tighten `signal_reversal_threshold`",
                "Direction": "↑ increase",
                "Parameter": "signal_reversal_threshold",
            })

    # Sharpe is negative
    if recent["sharpe_ratio"].mean() < 0:
        suggestions.append({
            "Issue": f"Negative average Sharpe ({recent['sharpe_ratio'].mean():.3f}) over last {N} sessions",
            "Suggestion": "Switch to paper trading; review signal weights and quality threshold",
            "Direction": "—",
            "Parameter": "signal_quality_threshold",
        })

    # Signal execution rate very low
    if "signals_generated" in recent and "signals_executed" in recent:
        exec_rate = recent["signals_executed"].sum() / max(recent["signals_generated"].sum(), 1)
        if exec_rate < 0.10:
            suggestions.append({
                "Issue": f"Only {exec_rate:.0%} of signals are executed — circuit breaker firing often?",
                "Suggestion": "Check daily loss / drawdown limits; consider loosening `loss_limit_daily`",
                "Direction": "↑ increase",
                "Parameter": "loss_limit_daily",
            })

    if suggestions:
        sugg_df = pd.DataFrame(suggestions)
        st.dataframe(sugg_df[["Issue","Suggestion","Direction"]], use_container_width=True, hide_index=True)

        # One-click apply
        st.subheader("Apply a suggestion")
        cfg = load_config()
        sel = st.selectbox("Select suggestion to apply", [s["Issue"] for s in suggestions])
        matched = next(s for s in suggestions if s["Issue"] == sel)
        param_key = matched["Parameter"]
        current_val = getattr(cfg, param_key, None)
        if current_val is not None:
            step = 0.01 if isinstance(current_val, float) else 1
            new_val = st.number_input(
                f"New value for `{param_key}` (current: {current_val})",
                value=float(current_val), step=float(step)
            )
            if st.button("💾 Apply and Save", type="primary"):
                setattr(cfg, param_key, type(current_val)(new_val))
                errors = save_config(cfg)
                if errors:
                    for e in errors: st.error(e)
                else:
                    st.success(f"`{param_key}` updated to {new_val}. Active within 1 minute.")
    else:
        st.success(f"✅ No issues detected in the last {N} sessions. Parameters look reasonable.")


# ════════════════════════════════════════════════════════════════
# TAB 4 — GRID SEARCH (walk-forward)
# ════════════════════════════════════════════════════════════════
with tab_grid:
    st.subheader("Walk-Forward Grid Search")
    st.caption("""
    Runs a parameter grid search against recent historical data using vectorbt.
    Uses the same backtest logic as §12. Results are ranked by Sharpe ratio.
    **Run post-market only — CPU intensive.**
    """)

    col_g1, col_g2 = st.columns(2)
    with col_g1:
        qt_min, qt_max = st.slider("signal_quality_threshold range", 0.3, 0.9, (0.5, 0.75), 0.05)
        zs_min, zs_max = st.slider("spike_zscore_threshold range",   1.0, 4.0, (1.5, 3.0),  0.25)
    with col_g2:
        tp_min, tp_max = st.slider("take_profit_pct range (%)",      1.0, 15.0, (3.0, 8.0), 0.5)
        mh_options = st.multiselect("max_hold_hours values", [12,24,36,48,72], default=[24,48])

    lookback_days = st.slider("Lookback window (days)", 7, 90, 30)
    n_combinations = (
        len([x/100 for x in range(int(qt_min*100), int(qt_max*100)+1, 5)]) *
        len([x/4  for x in range(int(zs_min*4),   int(zs_max*4)+1,  1)]) *
        len([x/2  for x in range(int(tp_min*2),   int(tp_max*2)+1,  1)]) *
        max(len(mh_options), 1)
    )
    st.info(f"Estimated combinations: **{n_combinations}**")

    if st.button("▶️ Run Grid Search", type="primary", disabled=n_combinations > 500):
        if n_combinations > 500:
            st.error("Too many combinations (>500). Narrow the ranges.")
        else:
            import itertools, numpy as np
            from utils.db import query as dbq

            # Load recent signals + prices from DB
            signals = dbq(f"""
                SELECT ticker, generated_at AS timestamp, direction, confidence AS quality_score
                FROM signals
                WHERE generated_at > NOW() - INTERVAL '{lookback_days} days'
                  AND executed = true
                ORDER BY generated_at
            """)
            prices = dbq(f"""
                SELECT symbol AS ticker, timestamp, open, high, low, close, volume
                FROM market_data
                WHERE timestamp > NOW() - INTERVAL '{lookback_days + 3} days'
                ORDER BY ticker, timestamp
            """)

            if signals.empty or prices.empty:
                st.warning("Not enough historical data for grid search.")
                st.stop()

            qt_vals = [round(x, 2) for x in
                       list(pd.Series(range(int(qt_min*100), int(qt_max*100)+1, 5)) / 100)]
            zs_vals = [round(x, 2) for x in
                       list(pd.Series(range(int(zs_min*4), int(zs_max*4)+1)) / 4)]
            tp_vals = [round(x, 3) for x in
                       list(pd.Series(range(int(tp_min*2), int(tp_max*2)+1)) / 200)]

            results = []
            progress = st.progress(0)
            combos   = list(itertools.product(qt_vals, zs_vals, tp_vals, mh_options or [48]))

            for i, (qt, zs, tp, mh) in enumerate(combos):
                filtered = signals[signals["quality_score"] >= qt]
                if len(filtered) < 5:
                    results.append({"qt": qt, "zs": zs, "tp": tp, "mh": mh,
                                    "sharpe": np.nan, "win_rate": np.nan, "trades": 0})
                    continue

                trade_pnls = []
                for _, sig in filtered.iterrows():
                    px = prices[(prices["ticker"] == sig["ticker"]) &
                                (prices["timestamp"] > sig["timestamp"])].head(mh)
                    if px.empty: continue
                    entry = px.iloc[0]["open"]
                    # simulate exit: take_profit or time stop
                    direction = 1 if sig["direction"] == "LONG" else -1
                    pnl = 0.0
                    for _, bar in px.iterrows():
                        ret = direction * (bar["close"] - entry) / entry
                        if ret >= tp:
                            pnl = tp; break
                        if ret <= -tp * 0.5:      # simplified ATR stop
                            pnl = -tp * 0.5; break
                    else:
                        pnl = direction * (px.iloc[-1]["close"] - entry) / entry
                    trade_pnls.append(pnl - 0.002)   # 20bps cost

                if len(trade_pnls) < 3:
                    results.append({"qt": qt, "zs": zs, "tp": tp, "mh": mh,
                                    "sharpe": np.nan, "win_rate": np.nan, "trades": 0})
                    continue

                s = pd.Series(trade_pnls)
                sharpe   = s.mean() / (s.std() + 1e-9) * (252 ** 0.5)
                win_rate = (s > 0).mean()
                results.append({"qt": qt, "zs": zs, "tp": tp, "mh": mh,
                                 "sharpe": round(sharpe, 3), "win_rate": round(win_rate, 3),
                                 "trades": len(trade_pnls)})
                progress.progress((i + 1) / len(combos))

            res_df = pd.DataFrame(results).dropna().sort_values("sharpe", ascending=False)
            st.success(f"Grid search complete — {len(res_df)} valid combinations.")

            # Show top 20
            st.subheader("Top 20 by Sharpe Ratio")
            st.dataframe(res_df.head(20).style.format({
                "sharpe":   "{:.3f}", "win_rate": "{:.1%}",
                "qt": "{:.2f}", "tp": "{:.1%}",
            }), use_container_width=True, hide_index=True)

            # Pareto: return vs drawdown proxy
            fig = px.scatter(res_df.head(50), x="win_rate", y="sharpe",
                             size="trades", color="qt", hover_data=["tp","mh","zs"],
                             title="Top 50 results — Sharpe vs Win Rate (size = # trades)")
            st.plotly_chart(fig, use_container_width=True)

            # One-click apply best
            best = res_df.iloc[0]
            st.subheader("Apply Best Config")
            st.write(f"**Best:** quality={best['qt']}, z-score={best['zs']}, "
                     f"take_profit={best['tp']:.1%}, max_hold={best['mh']}h "
                     f"→ Sharpe {best['sharpe']:.3f}, Win rate {best['win_rate']:.1%}")
            if st.button("✅ Apply Best Parameters", type="primary"):
                cfg = load_config()
                cfg.signal_quality_threshold = best["qt"]
                cfg.spike_zscore_threshold   = best["zs"]
                cfg.take_profit_pct          = best["tp"]
                cfg.max_hold_hours           = int(best["mh"])
                errors = save_config(cfg)
                if errors:
                    for e in errors: st.error(e)
                else:
                    st.success("Best parameters applied. Active within 1 minute.")
```

#### Docker Compose service addition

```yaml
# Add to docker-compose.yml
streamlit:
  build:
    context: ./monitoring/streamlit
    dockerfile: Dockerfile
  ports:
    - "8501:8501"
  environment:
    - DB_HOST=postgres
    - DB_NAME=trading
    - DB_USER=trader
    - DB_PASSWORD=yourpassword
    - REDIS_HOST=redis
  depends_on:
    - postgres
    - redis
  command: streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

```dockerfile
# monitoring/streamlit/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8501
```

```txt
# monitoring/streamlit/requirements.txt
streamlit>=1.32.0
plotly>=5.18.0
psycopg2-binary>=2.9.0
redis>=4.6.0
pandas>=2.0.0
```

---

### 15c. Combined Access URLs

| Interface | URL | Purpose |
|-----------|-----|---------|
| Streamlit — main dashboard | `http://localhost:8501` | Live monitoring + controls during market hours |
| Streamlit — **Configuration** | `http://localhost:8501/config` | Edit all system parameters (no restart needed) |
| Grafana — Signal Pipeline | `http://localhost:3000/d/pipeline` | Always-on pipeline health |
| Grafana — Portfolio P&L | `http://localhost:3000/d/pnl` | Equity curve + trade history |
| Grafana — Risk Status | `http://localhost:3000/d/risk` | Circuit breaker + exposure |
| Grafana — Signal Feed | `http://localhost:3000/d/signals` | Signal quality over time |
| Prometheus metrics | `http://localhost:9090` | Raw metrics query (debug) |

---

### 15d. Typical Daily Workflow

```
Pre-market (8:00–9:30 AM ET)
  └── Open Streamlit → verify circuit breaker ALLOW, check overnight signals
      └── Check Config page: confirm thresholds appropriate for today's VIX regime

Market open (9:30 AM)
  ├── Streamlit: monitor signal feed + positions in real time
  └── Grafana: pipeline health dashboard on second monitor (always-on)

During trading (9:30 AM–4:00 PM)
  ├── Grafana alerts → email/PagerDuty if daily loss > cfg.loss_limit_daily or IBKR drops
  ├── Streamlit sidebar: drawdown gauge, social exposure gauge
  ├── Streamlit: per-position "Close" buttons for manual override
  └── Config page: tune spike_zscore_threshold or signal_quality_threshold mid-session if needed

Market close (3:45 PM)
  └── EOD flush fires automatically; Streamlit shows final P&L

Post-market review
  ├── Grafana → Trade Analytics page → P&L by exit reason, win rate, equity curve
  ├── Config page: adjust weights / thresholds for next session based on today's results
  └── Optimize page → auto-suggestions + sensitivity analysis
      └── If ≥10 sessions recorded: run grid search, apply best params for tomorrow
```

---

*Updated 2026-05-23: Added §15 UI Monitoring (Grafana + Streamlit) for solo trader operations.*

---

*Report generated: 2026-05-21. Research draws on 7 dispatched research agents covering academic literature (Bollen 2011, Sul 2017, Agrawal 2018, Buz & de Melo 2021, Lim et al. 2019, Lee 2025, Goyal et al. 2025), verified API documentation (X/Twitter, Reddit, StockTwits, IBKR, LunarCrush, Santiment), and production open-source codebases (ib-api-reloaded/ib_async, ashwini-singhh/crypto_trading_agent, alosti/maotrade-fintech-showcase, HemantBK/Algorithmic-Trading-AI, PSURI1894/stock_sentiment_realtime, galafis/rust-sentiment-analysis-trading, schowd3/MlTradingBot).*

---

*[⬆ Back to main index](README.md)*
