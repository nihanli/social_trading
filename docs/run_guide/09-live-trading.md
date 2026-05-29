## Part 9 — Transitioning to Live Trading

### 9.1 Prerequisites Checklist

Complete **all** before switching:

- [ ] 5-day paper run: Sharpe ≥ 0.5, Win rate ≥ 45%, ≥ 20 trades
- [ ] No unexpected EMERGENCY exits during paper run
- [ ] Maximum drawdown during paper run < 5%
- [ ] IBKR live account funded (≥ $25,000 recommended for PDT rule compliance in US)
- [ ] 2FA enabled on IBKR account
- [ ] Written emergency procedure ready (what to do if internet goes down)

### 9.2 Risk Parameter Tightening

Update in **Streamlit → Config** before switching:

| Parameter | Paper | Live |
|-----------|-------|------|
| Max position % of NLV | 2% | **0.5%** |
| Half-Kelly fraction | 0.5 | **0.25** |
| Signal quality threshold | 0.60 | **0.70** |
| Daily loss limit | 3% | **1%** |
| Single trade loss limit | 1% | **0.5%** |
| Weekly loss limit | 7% | **3%** |
| Drawdown halt | 15% | **8%** |
| Max bid-ask spread (bps) | 100 | **25** |
| Max hold hours | 48 | **24** |

Click **Save Configuration** after changes.

### 9.3 Switch Procedure

1. **Update `.env`:**
   ```dotenv
   IBKR_PORT=4001
   TRADING_MODE=live
   ```

2. **Switch IB Gateway to live login:**
   - Quit Gateway → relaunch → log in with live account credentials
   - Confirm port = **4001** in Gateway settings

3. **Restart execution:**
   ```bash
   # Ctrl+C to stop current execution service, then:
   python -m social_trading.services.execution_service --ibkr
   ```
   Verify log: `Connected to IBKR port=4001 clientId=10`

4. **Verify in Streamlit:** Main Dashboard equity should match your IBKR live balance.

5. **Start with conservative watchlist:**
   ```bash
   python scripts/seed_watchlist.py --tickers AAPL MSFT SPY QQQ
   ```

### 9.4 First Week Monitoring Protocol

**Every morning (9:00–9:30 AM ET):**
- Streamlit main dashboard — equity correct?
- Circuit breaker state = `NORMAL`?
- IB Gateway still connected?

**During market hours:**
- Check Streamlit every 30 minutes for the first week
- No single position should exceed 0.5% of equity
- Keep honcho and execution terminals visible

**Market close (4:00–4:30 PM ET):**
- Review all closed trades on Trades page
- Record daily P&L in a trading journal
- Run Sharpe query weekly

### 9.5 Scaling Up

Do **not** increase position sizes until:
- 4 consecutive profitable weeks
- Sharpe ≥ 0.8 over 20+ trades
- Maximum single-day loss < 0.5% of equity

When ready: increase `max_position_pct` by 0.25% increments, one week at each level.
Never exceed 2% per position in the first 6 months.

---

---

← [08-paper-trading-checklist.md](08-paper-trading-checklist.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [10-reference.md](10-reference.md) →
