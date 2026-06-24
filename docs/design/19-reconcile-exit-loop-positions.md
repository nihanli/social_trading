# Reconcile / Exit Loop / Position Tracking — Interaction Guide

## Overview

Three concurrent async tasks manage open positions in `execution_service.py`.
Each has a distinct role; together they form a closed feedback loop that keeps
Redis state, the IB account, and the UI perfectly in sync.

```
┌─────────────────────────────────────────────────────────────────────┐
│                       execution_service.py                          │
│                                                                     │
│  ┌──────────────┐   ┌──────────────────┐   ┌────────────────────┐  │
│  │  trade_loop  │   │   exit_loop      │   │  reconcile_loop    │  │
│  │  (new trades)│   │  (15 s cadence)  │   │  (60 s cadence)    │  │
│  └──────┬───────┘   └────────┬─────────┘   └─────────┬──────────┘  │
│         │                   │                        │              │
│         ▼                   ▼                        ▼              │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                   Redis State Layer                          │   │
│  │  position:params   positions:live   reconcile:*              │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │             IB / TWS (source of truth for fills)             │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Cadence rationale

| Loop | Interval | Config key | Reason |
|---|---|---|---|
| exit_loop | **15 s** | `exit_eval_interval_sec` | Price-sensitive: faster SL/TP detection |
| reconcile_loop | **60 s** | `signal_poll_interval_sec` | Heavy IB queries (`reqExecutionsAsync`) — avoid pacing violations |
| trade_loop | event-driven | — | Blocks on Redis stream; no polling overhead |

---

## Key Redis Keys

| Key | Type | Purpose |
|---|---|---|
| `position:params` | Hash | **Ground truth** — per-ticker stop/take-profit/entry/direction/shares, survives restarts |
| `positions:live` | Hash | **UI feed** — enriched snapshot written each exit-loop cycle; includes unrealised P&L |
| `orders:inflight` | Hash | Entry MKT orders submitted but not yet fill-confirmed |
| `exits:inflight` | Hash | Close MKT orders submitted but not yet fill-confirmed |
| `reconcile:full` | String (JSON) | Full reconcile snapshot consumed by the UI reconcile panel |
| `reconcile:conflicts` | Hash | Unresolvable conflicts; non-empty halts the trade loop |
| `reconcile:state` | String | Workflow state: `collecting` / `awaiting_approval` / `approved` / `skipped_no_ib` |

---

## 1. trade_loop — Opening a Position

**Purpose:** Consume approved signals from `selected_signals`, submit entry orders to IB, and seed position state in Redis.

### Steps (per approved signal)

```
Signal arrives on Redis stream selected_signals
            │
            ▼
   Already open? ──YES──► Skip (circuit breaker)
            │
            NO
            ▼
   Submit MKT entry + OCA bracket (stop-loss / take-profit) to IB
            │
            ▼
   Write orders:inflight[orderId] = {ticker, sl, tp, direction, qty, …}
            │
            ▼
   Write position:params[ticker]  = {sl, tp, entry_price, direction,
                                      shares, opened_at, source="system"}
            │
            ▼
   Write positions:live[ticker]   = {partial snapshot — no P&L yet}
            │
            ▼
   Await fill callback (ib_async event)
            │
            ▼
   Fill confirmed → update position:params.entry_price with actual fill price
                 → update positions:live with corrected entry_price
                 → remove from orders:inflight
```

### Example

```
Signal: TOYO LONG 450 shares, SL=24.50, TP=28.00
  → IB order: MKT BUY 450 + OCA{STP SELL 450 @ 24.50, LMT SELL 450 @ 28.00}
  → position:params["TOYO"] = {shares:450, sl:24.50, tp:28.00, direction:LONG, …}
  → orders:inflight[orderId=101] = {ticker:"TOYO", sl:24.50, tp:28.00, …}
  → Fill @ 25.10 → entry_price corrected to 25.10
  → orders:inflight[101] removed
```

---

## 2. exit_loop (run_exit_loop) — Ongoing Position Monitoring

**Purpose:** Every `exit_eval_interval_sec` (default **15 s**), refresh prices, evaluate exit rules, close positions that trigger an exit, and write `positions:live` for the UI. Also owns naked-position detection and OCA reattach.

### Full Cycle

```
Every 15 seconds:
│
├─ 1. Connection guard
│     IB disconnected? → rebuild positions:live from position:params, sleep, continue
│
├─ 1b. Periodic IB position cache refresh (every 5 min)
│      reqPositionsAsync() to keep local cache current
│
├─ 2. Refresh market data
│     ├─ get_portfolio_prices()   ← IB account subscription (always available)
│     ├─ get_market_prices()      ← IB batch for any tickers not in portfolio feed
│     └─ yfinance fallback        ← for tickers with no IB price
│     └─ Write market_data:{ticker} to Redis + engine price cache
│
│     Watchlist tickers: yfinance full snapshot every 5 min
│
├─ 3. Evaluate exit rules (PositionExitManager)
│     ├─ Write positions:live FIRST (captures current sl/tp before any changes)
│     ├─ Naked position check → reattach OCA or force-close  [EXIT LOOP ONLY]
│     └─ For each open system position:
│           ├─ Skip if close order pending fill (_closing_tickers)
│           ├─ Evaluate: SL hit? TP hit? Trailing stop? Time decay? Sentiment decay?
│           └─ Exit triggered? → close position → add to just_closed + _closing_tickers
│
├─ 4. Persist state + write account metrics
│     → HWM, position params, account equity to Redis
│     → Prometheus metrics (PnL, drawdown, open count)
│
└─ 5. Clear stale _closing_tickers
      For tickers in _closing_tickers no longer present in IB positions:
      → remove from _closing_tickers (fill confirmed by IB)
      → reconcile_loop will publish position_closed event next cycle
```

> **Note:** External-close detection (IB bracket fills, manual TWS closes) is owned by
> `reconcile_loop` via the `CLOSED_OFFLINE` state in `_reconcile_ib_state()`.
> The exit loop does NOT scan for disappeared positions — this avoids the race
> condition where both loops independently detect and act on the same external close.

### Exit Rules (PositionExitManager)

| Rule | Trigger |
|---|---|
| Stop-loss | Current price ≤ stop_loss (LONG) or ≥ stop_loss (SHORT) |
| Take-profit | Current price ≥ take_profit (LONG) or ≤ take_profit (SHORT) |
| Trailing stop | Price retreats `trailing_stop_pct` from high-water mark |
| Mention decay | Social mentions drop below threshold → trailing stop tightens |
| Sentiment flip | Sentiment crosses threshold in opposite direction |
| Max hold time | Position held beyond `max_hold_hours` |

### positions:live Write Protocol

`positions:live` is written **twice** per cycle to maintain UI accuracy:
1. **Before** exit evaluation — captures current sl/tp (so the UI shows correct values even if a close is about to be processed)
2. **After** any exits — removes closed positions

---

## 3. reconcile_loop (run_reconcile_loop) — IB State Auditor

**Purpose:** Every 60 s, compare Redis `position:params` against IB's live positions. Detect and auto-resolve mismatches. Surface unresolvable conflicts to the UI.

### The 9 Position States

```
For each ticker in (position:params ∪ IB positions):

  Redis params?  IB position?  Open orders?   State
  ─────────────  ────────────  ────────────   ──────────────────────────
      YES            YES           YES        MATCHED         → no action
      YES            YES           NO         NAKED_POSITION  → reattach OCA or close
      YES            NO            —          CLOSED_OFFLINE  → publish closed, clean params
      NO             YES      ref=social_trd  ADOPTED_SYSTEM  → create params, adopt into exit loop
      NO             YES      no matching ref ADOPTED_MANUAL  → adopt with source=manual (UI only)
      NO             NO            —          —               (should not occur)
  (partial states for fill-pending, size-mismatch, etc. also classified)
```

### Auto-Resolution Flow

```
_reconcile_ib_state() called every 60s
│
├─ 1. Fetch IB positions + open orders + today's executions
│
├─ 2. Classify every ticker into one of 9 states
│
├─ 3. Auto-resolve safe states:
│     ├─ CLOSED_OFFLINE  → clean Redis, publish position_closed event, log P&L  [RECONCILE ONLY]
│     ├─ ADOPTED_SYSTEM  → create position:params from IB data, adopt into exit loop
│     └─ ADOPTED_MANUAL  → adopt with source=manual flag
│
├─ 4. Write reconcile:full JSON snapshot (consumed by UI)
│     Write reconcile:last_run timestamp
│
├─ 5. Conflicts (non-auto-resolvable) written to reconcile:conflicts
│     → trade loop checks this key before every new entry
│     → non-empty = trade loop halted (exits always continue)
│
└─ 6. If startup: _reconcile_inflight_orders()
      → recover fill prices for entry/exit orders still in the inflight ledgers
      → update entry_price in position:params if fill recovered
      → surface unrecoverable inflight entries as alerts:fill_sync alerts in UI
```

> **Note:** Naked-position reattach is NOT performed by reconcile_loop.
> It is classified as `state = "naked"` in the summary (for UI visibility) but
> the actual reattach is delegated to exit_loop which runs every 15 s.

---

## 4. Interaction Between the Three Loops

```
                   ┌──────────────────────────────────────────────────┐
                   │               IB / TWS                           │
                   │  - Positions (reqPositionsAsync)                 │
                   │  - Fills (reqExecutionsAsync)                    │
                   │  - Open orders                                   │
                   └──────────────────────────────────────────────────┘
                          ▲              ▲               ▲
                          │              │               │
             ┌────────────┴──┐   ┌───────┴───┐   ┌──────┴────────────┐
             │  trade_loop   │   │ exit_loop  │   │ reconcile_loop    │
             │               │   │            │   │                   │
             │ opens position│   │ monitors   │   │ audits every 60s  │
             │ writes params │   │ prices     │   │ detects offline   │
             │ seeds live    │   │ evaluates  │   │ closes & orphans  │
             │               │   │ exits      │   │                   │
             └───────────────┘   └────────────┘   └───────────────────┘
                          │              │               │
                          └──────────────┴───────────────┘
                                         │
                          ┌──────────────▼───────────────┐
                          │        Redis State            │
                          │  position:params (ground truth│
                          │  positions:live  (UI feed)    │
                          │  reconcile:*     (audit logs) │
                          └──────────────────────────────┘
                                         │
                          ┌──────────────▼───────────────┐
                          │      Streamlit UI             │
                          │  Positions panel              │
                          │  Reconcile panel              │
                          └──────────────────────────────┘
```

### Responsibility Division (after refactor)

| Concern | Owner | Notes |
|---|---|---|
| Opening new positions | trade_loop | |
| Refreshing prices | exit_loop | Portfolio feed → IB batch → yfinance fallback |
| Evaluating software exits (SL/TP/trailing/sentiment) | exit_loop | Every 15 s |
| Naked position check + OCA reattach | **exit_loop exclusively** | Removed from reconcile to eliminate double-call |
| Clearing `_closing_tickers` when fill confirmed | exit_loop | Cleared once IB position disappears |
| Detecting IB native bracket fills (CLOSED_OFFLINE) | **reconcile_loop exclusively** | `_reconcile_ib_state` CLOSED_OFFLINE state |
| Detecting positions closed while offline | reconcile_loop | Same CLOSED_OFFLINE path |
| Adopting orphaned / manual positions | reconcile_loop | |
| Recovering fill prices after restart | reconcile_loop (startup) | `_reconcile_inflight_orders` |
| Writing positions:live for the UI | exit_loop (primary) / trade_loop (on open) | |
| Halting trade_loop on conflicts | reconcile_loop | Via `reconcile:conflicts` key |

---

## 5. Worked Example: Typical Trade Lifecycle

### T=0:00 — Signal fires

```
trade_loop receives TOYO LONG 450
  → IB: MKT BUY 450 + OCA{STP@24.50, LMT@28.00}
  → position:params["TOYO"] = {shares:450, sl:24.50, tp:28.00, entry:0 (pending)}
  → orders:inflight[orderId=101] = {ticker:TOYO, …}
  → positions:live["TOYO"] = {partial, no P&L}
```

### T=0:01 — Fill confirmed

```
IB fill callback: TOYO filled @ 25.10
  → position:params["TOYO"].entry_price = 25.10
  → positions:live["TOYO"].entry_price = 25.10
  → orders:inflight[101] removed
```

### T=1:00 — First exit loop cycle

```
exit_loop:
  → price = 25.80 (from IB portfolio feed)
  → engine.set_price("TOYO", 25.80)
  → positions:live["TOYO"] = {unrealized_pnl: +315.00, hwm: 25.80, …}
  → exit rules: SL=24.50 not hit, TP=28.00 not hit → no close
  → _reconcile_external_closes: no changes
```

### T=45:00 — TP hit by IB bracket (OCA)

```
exit_loop:
  → price = 28.15 (above TP)
  → IB bracket LMT order fills natively — position gone from IB
  → _reconcile_external_closes detects TOYO disappeared
  → reads position:params["TOYO"] for entry/direction/sl/tp
  → infers reason = TAKE_PROFIT from filled OCA order type
  → publishes position_closed event {ticker:TOYO, reason:TAKE_PROFIT, exit:28.00}
  → deletes position:params["TOYO"]
  → deletes positions:live["TOYO"]
```

---

## 6. Offline / Restart Scenario

### Service restarts while a position is open

```
On startup:
  _reconcile_startup() runs BEFORE exit/reconcile loops start
  │
  ├─ Reads position:params (Redis persisted → has TOYO params)
  ├─ Reads IB positions (TOYO still open @ 25.10)
  ├─ Classifies: MATCHED → no action needed
  ├─ Runs _reconcile_inflight_orders() to recover any pending fills
  └─ exit_loop and reconcile_loop start normally
```

### IB bracket fires while service is offline

```
On startup:
  _reconcile_startup() runs
  │
  ├─ Reads position:params: TOYO present
  ├─ Reads IB positions: TOYO NOT present (bracket filled while offline)
  ├─ Classifies: CLOSED_OFFLINE
  ├─ Searches reqExecutionsAsync for a TOYO:SLD fill today
  ├─ Found fill @ 28.00 → exit_price=28.00, reason=TAKE_PROFIT
  ├─ Publishes position_closed event
  └─ Cleans up position:params["TOYO"]
```

---

## 7. Contrarian Mode Interaction

When `contrarian_mode = true`, the trade_loop submits **the opposite direction** of the signal:

- A LONG signal → SHORT trade submitted to IB
- The actual direction is recorded in `position:params["TOYO"].direction = "SHORT"`
- `positions:live` and all exit evaluations use the **actual** direction
- Reconcile and exit loop are unaware of contrarian mode — they always operate on the stored direction

This means the full lifecycle above is identical regardless of contrarian mode; the only difference is which direction was stored at open time.

---

## 8. IB Disconnection Handling

```
IB disconnects mid-session:
  │
  ├─ exit_loop: health_check() fails
  │     → rebuilds positions:live from position:params (no P&L, ib_disconnected=true)
  │     → sleeps poll_interval, retries health_check
  │
  ├─ reconcile_loop: skips cycle (logs "IB disconnected")
  │
  └─ _run_ib_reconnect_watcher: monitors connection
        → detects reconnect
        → runs _reconcile_startup() (catches offline fills/closes)
        → cancels old exit_loop task
        → starts fresh exit_loop task
        → runs _reconcile_inflight_orders() (recovers pending fills)
```

---

## 9. Common Failure Modes

| Symptom | Root cause | Detection |
|---|---|---|
| OCA orders still open after position closed | Exit loop missed external close; bracket cancel not propagated | `reconcile:conflicts` NAKED_POSITION; check IB open orders |
| Duplicate OCA sets | Trade loop submitted twice (e.g. signal redelivered after crash) | Position size = 2× expected; check `orders:inflight` |
| Position in params but not in IB | Closed offline, reconcile hasn't run yet | Resolved on next `_reconcile_startup` or reconcile loop cycle |
| positions:live blank | IB disconnected and `position:params` empty or expired | `ib_disconnected=true` in live key; reconcile on reconnect |
| Trade loop halted | `reconcile:conflicts` non-empty | UI reconcile panel shows conflict; approve or skip |
