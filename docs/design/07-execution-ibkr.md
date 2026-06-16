## 7. Execution Layer (Interactive Brokers)

### 7a. API Setup — ib_async

```
pip install ib_async  # Modern maintained fork of ib_insync
```

**Connection ports:**

| Environment | Port |
|-------------|------|
| TWS Paper Trading | `7497` |
| TWS Live | `7496` |
| IB Gateway Paper | `4002` |
| IB Gateway Live | `4001` |

Set via environment variable: `IBKR_PORT` (default `7497`), `IBKR_CLIENT_ID` (default `10`), `IBKR_ACCOUNT` (e.g. `DU123456`).

**For production:** Use **IB Gateway Stable** (headless, ~200MB RAM vs TWS ~600MB). Enable "Download open orders on connection" and set memory to 4096MB minimum.

All orders stamped with `orderRef = "social_trading"` so the system can identify its own orders in TWS among other API clients or manual trades.

---

### 7b. Service Architecture

The execution layer runs as a single async process (`execution_service.py`) with six concurrent asyncio tasks:

| Task name | Purpose |
|-----------|---------|
| `exec:reconnect` | Persistent IB connection manager — handles first connect, reconcile, and all reconnects |
| `exec:heartbeat` | Writes `service:heartbeat` and `ib:connected` to Redis every 10s |
| `exec:cmd` | Redis pub/sub command listener (`trading:commands`) |
| `exec:trade` | Consumes approved signals from `selected_signals` stream and submits orders |
| `exec:price_eval` | Every ~60s: evaluates exit rules for all open positions, updates trailing stops |
| `exec:reconcile` | Every 60s: queries IB ground truth; auto-resolves all position/fill discrepancies |
| `exec:price_push` | Every 5s: updates `unrealized_pnl` in `positions:live` from live IB portfolio feed |

The `exec:trade`, `exec:price_eval`, and `exec:reconcile` tasks are created/replaced by `exec:reconnect` on every IB connection session. The other tasks run for the lifetime of the service.

**Key global state:**
- `_ACTIVE_ENGINE` — the current `IBKRExecutionEngine` instance; accessed by heartbeat and cmd tasks
- `_halt_flag` — asyncio.Event; set by `HALT_NEW` command, cleared by `RESUME`

---

### 7c. Connection Management (`_run_ib_reconnect_watcher`)

The reconnect watcher is a **persistent daemon** (never returns) that owns 100% of IB connection management:

```
Outer loop (runs forever):
  1. Connect loop (retry every 30s until success):
       ib.connectAsync() → reqPositionsAsync() → reqAccountUpdatesAsync()
  2. _initialize_connected_runtime() — runs _reconcile_ib_state() immediately (see §7f)
  3. Start/replace exec:trade, exec:price_eval, exec:reconcile, exec:price_push tasks
  4. Inner monitor loop (check ib.isConnected() every 15s):
       On disconnect → break inner loop
  5. ib.disconnect(), cancel tasks, restart outer loop
```

This guarantees that **every IB connection session** (startup or mid-session reconnect after a TWS restart or network drop) always runs a full reconcile before any trading resumes.

**IBKRExecutionEngine.reconnect()** (lightweight reconnect used by the engine itself):
- Calls `ib.disconnect()` first to flush stale socket state
- Calls `connectAsync()` → `reqPositionsAsync()` → `reqAllOpenOrders()` + 2s sleep
- The 2s sleep is critical: it allows IB to push `openOrder` callbacks so `openTrades()` is fully populated before callers use it

**Position cache reseeding** (`_on_ib_reconnect` callback):
- Registered on `ib.connectedEvent` at engine construction
- On every (re)connect: calls `reqPositionsAsync()` + `reqAccountUpdatesAsync()`
- Prevents NLV=0 (risk service rejects every signal) after reconnect

---

### 7d. Order Submission (`submit_signal`)

Called by `run_trade_loop` for each approved signal. Full sequence:

**Step 1 — Guard checks (in trade loop before calling engine):**
- Market hours: skip if NYSE is closed
- Reconcile conflicts: block if `reconcile:conflicts` is non-empty (unresolved conflicts halt new entries)
- Halt flag: skip if `_halt_flag` is set
- Duplicate position: three-layer check — IB cache → engine `_position_params` → Redis `position:params`
- Signal expiry: discard if `hours_elapsed > cfg.signal_age_max_hours`

**Step 2 — Entry order:**
```python
entry = MarketOrder(action, quantity)
entry.transmit = True          # immediately live
entry.orderRef = "social_trading"
```
- Polls for IB acknowledgement (max 10s, 0.25s intervals)
- Accepts `Submitted`, `PreSubmitted`, or `Filled`
- Rejects on `Inactive`/`Cancelled` (IB hard-reject) or timeout (connection stall)
- After acknowledgement, polls up to 3s for a fill price (paper accounts fill with delay)

**Step 3 — Slippage correction:**
- If fill price known and the risk-service SL/TP is on the wrong side of fill (spike slippage): recomputes both SL and TP anchored from actual fill price
- ATR stop floor: ensures `|fill - sl| >= fill * trailing_stop_min_pct` (prevents trail firing before ATR stop)

**Step 4 — OCA bracket (three legs, all GTC, all `orderRef = "social_trading"`):**

| Order | Type | Condition | Notes |
|-------|------|-----------|-------|
| Stop-loss | `STP` | `outsideRth=False` | ATR-derived; skipped if invalid vs fill |
| Take-profit | `LMT` | `outsideRth` unset | Limit at TP price; skipped if TP ≤ 0 |
| Trailing stop | `TRAIL` | `outsideRth=False` | % trail anchored from current market price at fill |

All three use `ocaGroup = f"oca_{entry_order_id}"` and `ocaType=1` (cancel sibling on fill). IB handles bracket execution entirely server-side — the software exit loop is a secondary failsafe.

**OCA failure handling:** If OCA placement raises a Python exception, the position is left open (no emergency close attempted). The software exit loop will manage exits. A prominent error is logged.

**Step 5 — Persistence:**
- `position:params` (Redis hash): `stop_loss`, `take_profit`, `opened_at`, `direction`, `shares`, `entry_price`, `oca_group`, `trailing_stop_pct_applied`, `source="system"`
- `positions:live` (Redis hash): immediate write so UI sees the new position without waiting for the exit loop
- `execution:events` stream: `position_opened` event consumed by persistence service → PostgreSQL `trades` table
- `trades:recent` (Redis list, max 1000): last 1000 trades for UI

**Async entry fill tracking:**
If fill price was not known at submission (slow paper fill), `_reconcile_ib_state()` corrects `entry_price` on the next 60-second reconcile cycle via the `fill_pending` state. The pending entry is tracked in `orders:inflight` (Redis hash) as a fallback until reconcile picks it up.

---

### 7e. Price Eval Loop (`run_price_eval_loop`)

Runs every `cfg.signal_poll_interval_sec` (default 60s). Full cycle:

**1. Connection guard:** If `engine.health_check()` fails, skip the cycle and wait — the reconnect watcher handles recovery.

**2. Periodic IB cache refresh:** Every 5 minutes, calls `reqPositionsAsync()` to prevent drift when fills are missed during reconnect windows.

**3. Price refresh:**
- Open positions: batch-fetched from IB via `get_market_prices()` (real-time)
- IB fallback: yfinance full snapshot per ticker (ATR, OHLCV, momentum) if IB returns no price
- Full snapshot (ATR) every 5 minutes per open ticker
- Watchlist tickers: yfinance only, 5-minute cadence

**4. Naked position check (`_check_naked_positions`):**
Before exit evaluation, detects positions with no live STP or TRAIL orders in IB. For each naked position:
- Attempts `reattach_oca_orders()` — places fresh OCA bracket using ATR-derived SL/TP
- If reattach fails (no ATR, no IB bracket): immediately closes the position
- Positions handled here are excluded from exit evaluation this cycle to avoid race conditions

**5. Exit rule evaluation (per open position):**

| Rule | Trigger |
|------|---------|
| ATR stop-loss | `current_price ≤ stop_loss` (LONG) or `≥ stop_loss` (SHORT) |
| Take-profit | `current_price ≥ take_profit` (LONG) or `≤ take_profit` (SHORT) |
| Trailing stop | `current_price ≤ hwm * (1 - trail_pct)` (LONG) |
| Time-based exit | Position held > `cfg.max_hold_hours` |
| Sentiment decay | Mention ratio drops; trailing stop tightens dynamically |

**Mention-decay trail tightening (Rule 6):** When `mention_ratio < cfg.mention_decay_threshold`, `_effective_trailing_pct()` linearly tightens the trail percentage based on how far below threshold. The tightened trail is passed to the exit manager via a `dataclasses.replace(cfg)` copy — the base config is never mutated. If tightening changes the pct, `update_trailing_stop()` is called to replace the live TRAIL order anchored from HWM.

**Pending close guard:** Positions with a close order submitted but not yet fill-confirmed (`_closing_tickers` in-memory set) are skipped during exit evaluation to prevent duplicate orders.

**6. Position close (`close_position`):**
- Cancels all open bracket orders for the ticker (filtered by `orderRef`)
- Submits a market close order
- Waits 0.5s for immediate fill
- If fill confirmed immediately: cleans up `position:params`, `hwm`, `trail:orders`, publishes `position_closed` event, writes trade to DB
- If fill pending: adds ticker to `_closing_tickers` (in-memory); `run_reconcile_loop` detects fill via `closed_offline` state on the next 60s cycle

**7. Persistence:** After each cycle, writes `position:params` and HWM to Redis; writes `account:state` to Redis hash.

---

### 7f. Reconcile Loop (`run_reconcile_loop` + `_reconcile_ib_state`)

Runs every 60 seconds when IB is connected. Replaces the old blocking startup reconcile and inflight-order polling. When IB is disconnected the loop skips entirely — app state is frozen (read-only).

**`_reconcile_ib_state()` — per cycle:**
1. Refreshes IB position cache (`reqPositionsAsync`) and open orders (`reqAllOpenOrders`)
2. Fetches fills (`reqExecutionsAsync`), completed orders (`reqCompletedOrdersAsync`), and all open trades
3. Classifies every tracked ticker into one of 9 states:

| State | Condition | Resolved |
|-------|-----------|---------|
| `matched` | IB ✓, app ✓, direction + shares agree | Sync entry_price/shares if needed |
| `fill_pending` | matched, `entry_price=0` | Look up fill VWAP, update params |
| `naked` | matched, no STP/TRAIL orders | `_check_naked_positions()` — reattach or close |
| `shares_synced` | matched, share count differs | IB is source of truth; app params updated |
| `adopted` | IB ✓, app ✗, system orderRef | Seed params + OCA; publish `position_opened` |
| `manual_ib` | IB ✓, app ✗, no system orderRef | Log only; no action |
| `closed_offline` | IB ✗, app ✓, close fill found | Publish `position_closed`; clean up params |
| `missing` | IB ✗, app ✓, no fill | **Conflict** — writes to `reconcile:conflicts`; halts trade loop |
| `direction_mismatch` | IB/app direction differ | **Conflict** — writes to `reconcile:conflicts`; halts trade loop |

4. Writes `reconcile:conflicts` hash — non-empty halts the trade loop (new entries blocked)
5. Writes `reconcile:full` JSON snapshot (UI display) and `reconcile:last_run` timestamp
6. Refreshes `positions:live`

**Conflict resolution (`RESOLVE_CONFLICT` command):**
- `mark_closed` — treat app position as closed (no fill price); publish `position_closed`
- `remove_app` — silently remove from app state (no event)
- `use_ib_direction` — override app direction with IB's direction
- All actions clear the ticker from `reconcile:conflicts`, unblocking the trade loop

**First run on IB connect:** `_initialize_connected_runtime()` calls `_reconcile_ib_state()` directly (no approval gate). Conflicts halt the trade loop immediately but do not prevent the service from starting.

**UI command changes vs. old reconcile flow:**

| Command | Status | Notes |
|---|---|---|
| `FULL_RECONCILE` | Kept | Now calls `_reconcile_ib_state()` directly |
| `RECONCILE_APPROVE` | Legacy no-op | Reconcile is now automatic |
| `RECONCILE_SKIP` | Legacy no-op | No approval gate in new flow |
| `RESOLVE_CONFLICT {ticker} {action}` | **New** | Resolves a conflict; clears from `reconcile:conflicts` |

---

### 7g. Position Persistence Model

All open-position state survives service restarts through Redis. At startup the engine is seeded from Redis before the reconnect watcher runs.

| Redis key | Type | Contents |
|-----------|------|----------|
| `position:params` | Hash | Per-ticker: `stop_loss`, `take_profit`, `entry_price`, `shares`, `direction`, `opened_at`, `oca_group`, `trailing_stop_pct_applied`, `source` |
| `positions:live` | Hash | Per-ticker live snapshot (same fields + `unrealized_pnl`, `high_water_mark`) — 5-min TTL, refreshed each exit-loop cycle; atomic RENAME avoids empty-window reads |
| `position:hwm` | Hash | Per-ticker high-water mark for trailing stop (max price seen for LONG, min for SHORT) |
| `trail:orders` | Hash | Per-ticker trailing stop IB order ID (restored on reconnect to enable `update_trailing_stop`) |
| `orders:inflight` | Hash | Per-order-id entry fill tracking (TTL 3600s); cleaned when fill arrives |
| `exits:inflight` | Hash | Per-order-id exit fill tracking; seeded into `_pending_close` on restart |
| `positions:pending_reconcile` | Hash | Tickers that cannot be auto-reconciled; shown in UI for manual action |
| `reconcile:state` | String | `awaiting_approval` / `approved` / `skipped_no_ib` — gates `exec:trade` and `exec:exit` |
| `reconcile:data` | JSON | Full reconcile snapshot for UI display |
| `account:state` | Hash | NLV, cash, daily PnL, drawdown — consumed by risk service |
| `exits:deferred` | Set | Tickers with pending after-hours close; processed at next market open |
| `positions:cmd_closed` | Set (TTL 120s) | Tickers closed by software command this cycle; prevents double-close detection |

**PostgreSQL persistence** (via `execution:events` stream → persistence service):

| Event | DB write |
|-------|----------|
| `position_opened` | INSERT into `trades` (open row) |
| `position_entry_updated` | UPDATE `trades.entry_price` |
| `position_closed` | UPDATE `trades` with `exit_price`, `exit_reason`, `closed_at`, `pnl` |
| `position_deleted` | UPDATE `trades` status = deleted |

---

### 7h. Price Feed Architecture

Three price sources, in priority order:

1. **IB portfolio subscription** (`ib.portfolio()`): Pushed by IB every few seconds for all held positions as part of the account subscription — always current, no explicit market-data request needed. Used by `exec:price_push` to update `unrealized_pnl` every 5s.

2. **IB batch market data** (`get_market_prices()`): Uses `reqMktData()` for all open-position tickers in parallel. Used by the exit loop each 60s cycle for authoritative prices before exit rule evaluation.

3. **yfinance fallback**: Used for tickers where IB returns no price (outside hours, no subscription), for watchlist tickers, and for full ATR/OHLCV snapshots every 5 minutes.

**High-water mark (HWM) tracking:**
The engine maintains two independent trackers per ticker: `_hwm` (max price, for LONG trailing stops) and `_hwm_min` (min price, for SHORT trailing stops). Both are updated on every `set_price()` call and persisted to Redis `position:hwm` each exit-loop cycle.

---

### 7i. UI Commands (`trading:commands` pub/sub)

| Command | Effect |
|---------|--------|
| `HALT_NEW` | Sets `_halt_flag`; no new positions opened |
| `RESUME` | Clears `_halt_flag` |
| `CLOSE_ALL` | Closes all system-managed open positions |
| `CLOSE_TICKER {ticker}` | Closes one position; deferred if after-hours |
| `RECONCILE_APPROVE` | Processes reconcile results; unblocks trading |
| `RECONCILE_SKIP` | Skips reconcile UI; still recovers inflight fills |
| `RECONCILE_DELETE_POSITION {ticker}` | Deletes all state for ticker; cancels IB orders |
| `RESOLVE_PENDING_CLOSE {ticker}` | Marks a pending-reconcile position as closed |
| `RESOLVE_PENDING_DELETE {ticker}` | Removes pending-reconcile position from app |
| `ADOPT_IB_POSITION {ticker}` | Adopts a manual IB position into system tracking |
| `REFRESH_SYNC` | On-demand inflight order reconciliation |

---

### 7j. Important IBKR Gotchas

| Pitfall | Mitigation in this codebase |
|---------|----------------------------|
| `openTrades()` empty right after reconnect | `reqAllOpenOrders()` + 2s sleep in `reconnect()` |
| NLV=0 after reconnect (risk service rejects signals) | `reqAccountUpdatesAsync()` called in both watcher and `_on_ib_reconnect` |
| OCA TP on wrong side of fill due to slippage | SL/TP recomputed from actual fill price if pre-computed values are invalid |
| Paper account delayed fill → OCA placed with stale price | 3s fill-wait before OCA placement; async fill callback corrects entry_price |
| TWS nightly restart (~11:45 PM ET) — position cache empty | `connectedEvent` handler reseeds via `reqPositionsAsync()` |
| Duplicate `position_opened` events on repeated restarts | NX-flag adoption key (`position:adopted:{ticker}:{fp}`, TTL 90 days) |
| `transmit=True` required on close orders | Explicit `close_order.transmit = True` — default varies by ib_async version |
| Bracket orders fired prematurely | ATR stop floor: `|fill - sl| >= fill * trailing_stop_min_pct` |
| Manual IB positions adopted accidentally | Only positions with `orderRef = "social_trading"` are treated as system-managed |
| Client ID conflict | Set via `IBKR_CLIENT_ID`; each process needs a unique ID (0–31) |
| Financial Advisor master account | Blocked at startup with clear error message |

### 7k. IBKR Rate Limits

| Category | Limit |
|----------|-------|
| Simultaneous market data lines | 100 (shared with TWS GUI) |
| Historical data requests | Max 50 open, 60 per 10-minute window |
| API client connections | Max 32 simultaneous |
| Order rate | ~50 orders/sec practical limit |
| `reqPositionsAsync` | Throttled internally by ib_async; safe to call periodically |

---

*[⬆ Back to main index](README.md)*
