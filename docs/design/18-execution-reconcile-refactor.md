## 18. Execution Service Refactor — Periodic Reconcile Architecture

> **Status: Implemented** (Phases 1–3 complete; Phase 4 doc update in progress)
> This document describes the periodic reconcile architecture that replaced the
> callback + inflight + exit-loop system. See §18j for the migration plan and
> `07-execution-ibkr.md` for the live architecture reference.

---

### 18a. Motivation

The current execution service tracks position state through three independent
mechanisms that can fall out of sync:

1. **Fill callbacks** — registered in-memory per order; lost on service restart
2. **Inflight ledgers** (`orders:inflight`, `exits:inflight`) — Redis TTL-based tracking; can be prematurely discarded or deadlocked by stale entries
3. **Exit loop external-close detection** (`_reconcile_external_closes`) — detects positions gone from IB by diffing cycle-to-cycle snapshots; sensitive to seed ordering bugs

When these three mechanisms disagree — callback fires after inflight was discarded,
reconcile runs while callback is pending, false-positive external-close on first cycle — app state becomes inconsistent. Specific bugs caused by this design:

| Bug observed | Root cause |
|---|---|
| Signal shows "not executed" but order was filled | Fill callback lost on restart; inflight entry discarded for pre-market order |
| Position stays in app after IB OCA bracket fires | External close detection racing with `_pending_close` |
| Duplicate trade DB records | Callback + reconcile both publish `position_opened` |
| Position disappears without close event | `_reconcile_external_closes` false-positive on first cycle |
| Entry price stuck at 0 after market open fill | Inflight entry discarded before fill arrived |
| Stale inflight deadlock | Stale orderId in `exits:inflight` blocks external close detection indefinitely |

**Root cause**: the design is event-driven with no authoritative reconciliation.
Any missed event leaves state inconsistent until the next manual reconcile.

---

### 18b. Design Principles

**1. IB is always ground truth.**
The app's Redis state is a cache derived from IB. All state transitions are computed by
comparing current IB state against cached app state. There are no event-driven state
machines — only periodic comparison.

**2. When IB is disconnected, app state is frozen.**
No position is created, modified, or deleted while IB is not connected. `position:params`,
`positions:live`, HWM, trail orders, and execution events are **strictly read-only** during
disconnection. The reconcile loop does not run. App state from the last connected session
is preserved exactly as-is until IB reconnects.

**3. Reconcile is automatic — user action only for genuine conflicts.**
The periodic reconcile applies all safe state transitions silently every 60 seconds.
The user is not asked to approve routine operations. Only genuine conflicts (position in app
but not in IB with no fill record, share count disagreement) halt execution and surface
for manual resolution.

---

### 18c. Two-Loop Architecture

The existing single `run_exit_loop()` is replaced by two focused loops:

```
┌──────────────────────────────────────────────────────────┐
│  RECONCILE LOOP  (run_reconcile_loop)                    │
│  Cadence: every 60s                                      │
│  Guard:   IB must be connected — skip entirely if not    │
│                                                          │
│  Queries IB:                                             │
│    ib.positions()         — current open positions       │
│    reqExecutionsAsync()   — fills for today's session    │
│    ib.openTrades()        — active orders + OCA groups   │
│                                                          │
│  Classifies each position and applies state transitions  │
│  (see §18d). Writes results to Redis. Publishes events.  │
│  On conflict: writes to reconcile:conflicts, halts exec. │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  PRICE EVALUATION LOOP  (run_price_eval_loop)            │
│  Cadence: every 5–10s                                    │
│  Scope:   OPEN POSITIONS ONLY (tickers in sys_params)    │
│                                                          │
│  Uses cached market prices — no IB API calls.            │
│                                                          │
│  Per open position:                                      │
│    • Evaluate software exit rules (trailing stop,        │
│      sentiment reversal, time-based exit)                │
│    • Update IB TRAIL order if trail% changed             │
│      (requires IB connection)                            │
│    • Submit MKT close if exit rule triggered             │
│      (requires IB connection)                            │
│    • Mark position as "closing" in-memory to prevent     │
│      double-submission before next reconcile             │
│                                                          │
│  IB disconnected: evaluate rules, record triggers        │
│  in-memory. Do NOT submit orders or write Redis state.   │
│  Act on recorded triggers when IB reconnects.            │
└──────────────────────────────────────────────────────────┘
```

**Why two loops?** Reconcile needs network I/O (IB API calls) and can tolerate 60s cadence.
Price evaluation needs to be fast (~5s) but only reads from the in-memory price cache —
no network I/O. Combining them would either slow the price loop or waste IB API calls.

---

### 18d. Reconcile State Transitions

Each tracked position is classified on every reconcile cycle:

| IB position | App params | Additional check | State | Auto-resolved? |
|---|---|---|---|---|
| ✓ | ✓ | — | `matched` | ✓ Yes |
| ✓ | ✓ | `entry_price = 0` | `fill_pending` | ✓ Yes |
| ✓ | ✓ | no live STP/TRAIL orders | `naked` | ✓ Yes |
| ✓ | ✗ | system `orderRef` found | `adopted` | ✓ Yes |
| ✓ | ✗ | no system `orderRef` | `manual_ib` | ✓ Yes (no-op) |
| ✗ | ✓ | close fill found today | `closed_offline` | ✓ Yes |
| ✗ | ✓ | no close fill found | `missing` | ✗ **Conflict** |
| ✓ | ✓ | share count disagrees | `shares_mismatch` | ✗ **Conflict** |
| ✓ | ✓ | direction disagrees | `direction_mismatch` | ✗ **Conflict** |

#### Actions per state

**`matched`**
Sync `entry_price` from fill record if currently 0 (`fill_pending` sub-case handled inline).
Sync share count if minor rounding difference (within 1 share). No event published.

**`fill_pending`** (`matched` where `entry_price = 0`)
Look up fills via `reqExecutionsAsync` for this ticker. If found: compute VWAP, update
`entry_price` in `position:params` and `positions:live`, publish `position_entry_updated`.
If no fill found: position is still pending (pre-market order queued) — leave as-is, no action.

**`naked`** (matched, but no live STP or TRAIL order with `orderRef = "social_trading"`)
Attempt `reattach_oca_orders()` using ATR-derived SL/TP. If reattach succeeds: persist new
`oca_group` to params. If reattach fails: close position via `close_position()`, publish
`position_closed`, clean up all tracking keys.
*Only applies to app-submitted fills (`source = "system"`) — manual IB positions are never naked-checked.*

**`adopted`** (in IB, not in app; system order identified)
Seed `position:params` from ATR. Immediately attempt `reattach_oca_orders()` if no live
bracket. Publish `position_opened` (with NX dedup guard). If no ATR and no bracket: close
immediately via `_close_adopted_no_oca()`.

**`manual_ib`** (in IB, not in app; no system order)
Log only. Not adopted into system tracking. User manages this position independently.

**`closed_offline`** (in app, not in IB; close fill found today)
Compute VWAP fill price from execution records. Publish `position_closed`. Delete
`position:params`, `hwm`, `trail_orders` entries for this ticker.

#### Conflict states (halt execution, require user action)

When any conflict state is detected, `reconcile:conflicts` is written to Redis and the
trade loop stops accepting new signals until all conflicts are resolved.

| State | Condition | User options on reconcile page |
|---|---|---|
| `missing` | In app, not in IB, no fill record | "Mark as Closed" · "Force Re-Adopt" · "Remove from App" |
| `shares_mismatch` | IB and app share count differ by > 1 share | "Use IB Quantity" · "Close Position" |
| `direction_mismatch` | IB and app disagree on direction | "Use IB Direction" · "Close Position" |

---

### 18e. Reconcile Approval Model

#### Automatic reconcile (no user action required)

Every 60 seconds, the reconcile loop runs and applies all non-conflict state transitions
silently. The **reconcile page in the UI is purely informational**: it shows the current
classification of every position (updated each cycle) for monitoring. No button click is
required for normal operation.

#### Conflict — execution halted

When a conflict is detected:
1. `reconcile:conflicts` (Redis hash) is written with the conflicting tickers and reasons
2. The trade loop checks this key before processing any signal — if non-empty, the signal
   is rejected with a `RECONCILE_CONFLICT` warning
3. The reconcile page shows the conflict with resolution action buttons
4. Once the user resolves all conflicts, `reconcile:conflicts` is cleared
5. On the next reconcile cycle (or via the "Run Reconcile" button), execution resumes

Signals arriving while execution is halted are **rejected** (not queued). The signal
service will regenerate them on the next poll if the conditions still hold.

#### User-triggered reconcile

The "Run Reconcile" button on the reconcile page triggers an immediate reconcile cycle
outside the 60-second timer. The result is displayed the same way as an automatic cycle.
This replaces the current `RECONCILE` → `RECONCILE_APPROVE` two-step flow.

---

### 18f. IB Disconnected Invariant

> **App state must never be modified while IB is disconnected.**

This is a hard invariant, not a best-effort guideline.

| Component | Behaviour when IB disconnected |
|---|---|
| Reconcile loop | Skipped entirely. No Redis writes. No events published. |
| Price eval loop | Continues evaluating exit rules against cached prices for monitoring. Does **not** submit close orders. Does **not** write Redis state. Exit rule triggers are recorded in-memory only. |
| Trade loop | Unchanged — already requires IB connection to submit orders. |
| On reconnect | Reconcile fires immediately (does not wait for 60s timer) to catch any fills or position changes that occurred during the gap. In-memory triggers from the disconnect window are re-evaluated against fresh IB state. |

**Why this matters:**
- A position deleted from app state during disconnect cannot be recovered if IB shows it still open
- Events published without confirmed IB fills create orphaned DB records
- The first post-reconnect reconcile must compare against a clean frozen baseline

---

### 18g. Redis Key Changes

#### Keys removed

| Key | Why removed |
|---|---|
| `orders:inflight` | Reconcile detects `entry_price=0` and recovers fill directly from `reqExecutionsAsync` |
| `exits:inflight` | Reconcile detects position gone from IB and publishes close event |
| `reconcile:state` (`awaiting_approval`) | No approval gate — reconcile is fully automatic |
| `reconcile:data` (full snapshot JSON) | Replaced by lightweight `reconcile:conflicts` list |
| `alerts:fill_sync` | Subsumed by conflict display on reconcile page |

#### Keys added

| Key | Type | Contents |
|---|---|---|
| `reconcile:conflicts` | Hash | Per-ticker conflict state and reason; non-empty = execution halted |
| `reconcile:last_run` | String | ISO timestamp of last successful reconcile cycle |

#### Keys kept (unchanged)

| Key | Purpose |
|---|---|
| `position:params` | Authoritative app-side position state |
| `positions:live` | Written each reconcile cycle; read by UI |
| `hwm:all` | High-water marks for trailing stop |
| `trail_orders` | TRAIL order IDs for `update_trailing_stop()` |
| `trade:last_at:{ticker}` | Per-ticker cooldown for risk service |
| `execution:events` | Stream consumed by persistence service |
| `positions:pending_reconcile` | Manual-review positions (kept for `missing` conflicts) |
| `exits:deferred` | After-hours deferred closes |
| `positions:cmd_closed` | Software-closed tickers (prevents double-close event) |

---

### 18h. UI Command Changes

| Command | Status | Notes |
|---|---|---|
| `RECONCILE` | **Removed** | Reconcile is automatic; UI button triggers immediate cycle via direct call |
| `RECONCILE_APPROVE` | **Removed** | No approval gate |
| `RECONCILE_SKIP` | **Removed** | No skip needed — automatic reconcile replaces the blocking startup flow |
| `RESOLVE_CONFLICT {ticker} {action}` | **New** | Resolves a conflict; clears from `reconcile:conflicts` |
| `CLOSE_TICKER {ticker}` | Kept | Unchanged |
| `CLOSE_ALL` | Kept | Unchanged |
| `HALT_NEW` / `RESUME` | Kept | Unchanged |
| `RECONCILE_DELETE_POSITION {ticker}` | Kept | Manual removal; unchanged |
| `ADOPT_IB_POSITION {ticker}` | Kept | Force-adopt a manual IB position |

---

### 18i. Function Changes

#### New functions

| Function | Role |
|---|---|
| `run_reconcile_loop()` | 60s timer loop; IB-connected guard; calls `_reconcile_ib_state()` |
| `_reconcile_ib_state()` | Core reconcile: fetch IB data, classify all positions, apply transitions |
| `run_price_eval_loop()` | Fast loop (5–10s); open positions only; software exit rules + TRAIL updates |

#### Modified functions

| Function | Change |
|---|---|
| `run_exit_loop()` | Replaced by `run_reconcile_loop()` + `run_price_eval_loop()` |
| `_reconcile_startup()` | Replaced by first call to `_reconcile_ib_state()` at startup |
| `_collect_reconcile_data()` | Merged into `_reconcile_ib_state()` |
| `run_command_listener()` | Remove `RECONCILE_APPROVE` / `RECONCILE_SKIP` handlers; add `RESOLVE_CONFLICT` |
| `_initialize_connected_runtime()` | Remove blocking reconcile gate; call `_reconcile_ib_state()` directly |

#### Removed functions

| Function | Replaced by |
|---|---|
| `_reconcile_inflight_orders()` | `fill_pending` + `closed_offline` states in `_reconcile_ib_state()` |
| `_reconcile_external_closes()` | `closed_offline` state in `_reconcile_ib_state()` |

#### Kept unchanged

`_check_naked_positions()`, `_publish_execution_event()`, `_write_positions_to_redis()`,
`_write_account_state()`, `_effective_trailing_pct()`, `_close_adopted_no_oca()`,
`run_trade_loop()`, `_get_sentiment_context()`, `_save_eod_snapshot()`.

---

### 18j. Migration Plan

Designed to be executed in four independent phases, each mergeable without breaking
the running system.

**Phase 1 — Extract `_reconcile_ib_state()`**
Refactor `_reconcile_startup()` + `_collect_reconcile_data()` state-transition logic into
a single `_reconcile_ib_state()` function. Keep existing callbacks and inflight tracking
in place (they still run in parallel). Validate all existing reconcile unit tests pass.

**Phase 2 — Add `run_reconcile_loop()`**
Wrap `_reconcile_ib_state()` in a 60-second loop. Run it in parallel with the existing
exit loop. Monitor for duplicate events (position_closed published twice) — add dedup guard
using a Redis NX key per ticker per close event.

**Phase 3 — Remove callbacks and inflight tracking**
Remove `register_order_fill_callback()` call sites. Remove `orders:inflight` writes.
Remove `exits:inflight` writes. Remove `_reconcile_inflight_orders()`. Remove
`_reconcile_external_closes()`. Remove the `RECONCILE_APPROVE` / `RECONCILE_SKIP` flow.

**Phase 4 — Split the exit loop**
Extract price evaluation from `run_exit_loop()` into `run_price_eval_loop()`. Replace
`run_exit_loop()` with `run_reconcile_loop()` in the task startup table. Update `07-execution-ibkr.md` sections 7b, 7e, 7f, 7i to reflect the new architecture.

---

### 18k. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Duplicate `position_opened` events | Low | Medium | NX dedup key per `{ticker}:{entry_price}:{shares}` (already in use for adoptions) |
| Duplicate `position_closed` events | Low | Medium | Check `position:params` existence before publishing; NX close-event key |
| `reqExecutionsAsync` unavailable at reconcile time | Low | Low | Graceful skip; retry next cycle; log warning |
| 60s latency for entry price correction | Medium | Low | IB OCA bracket always protects during gap; trail is secondary |
| Software exit triggers missed during IB disconnect | Low | Low | Triggers recorded in-memory; acted on at first post-reconnect cycle |
| Race: price eval submits close during reconcile write | Low | Low | In-memory `_closing_tickers` guard prevents double-submission |
| App state modified during IB disconnect | **None** | **High** | Hard invariant: reconcile skipped; price eval is read-only when disconnected |
| Incoming signals during conflict halt | Medium | Low | Signals rejected with `RECONCILE_CONFLICT` reason; signal service regenerates on next poll |

---

### 18l. Open Questions

1. **Trailing stop update frequency**: Current trail updates fire every ~5s when price
   moves significantly. In the new design, trail updates still happen in `run_price_eval_loop()`
   at 5s cadence. Confirm this is acceptable vs. the current behaviour.

2. **EOD snapshot (`_save_eod_snapshot`)**: Currently triggered from within `run_exit_loop()`.
   Needs a home in the new loop structure — likely a time-check inside `run_reconcile_loop()`.

3. **Paper trading mode**: `reqExecutionsAsync` behaviour differs in paper accounts. The
   `fill_pending` and `closed_offline` detection paths need paper-mode testing.

4. **Signals during conflict halt**: Confirmed that signals are **rejected** (not queued)
   when `reconcile:conflicts` is non-empty. The signal service regenerates them on the next
   poll cycle if market conditions still warrant entry.

---

*[⬆ Back to main index](README.md)*
