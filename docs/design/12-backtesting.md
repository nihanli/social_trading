## 12. Backtesting Framework

### Critical Pitfalls to Avoid

| Pitfall | Solution |
|---------|---------|
| Look-ahead bias | Social media timestamps must be verified — post at 3:47pm only affects next-day open |
| Survivorship bias | Include delisted stocks in historical universe |
| Zero slippage assumption | Model: `fill_price = mid × (1 + spread/2 + √(order_size/ADV) × sigma)` |
| Signal autocorrelation | Overlapping windows double-count signal strength — use non-overlapping periods |
| Reactive signal contamination | Filter out posts where price moved >10% BEFORE the post was made |

[^26]: Buz & de Melo (2021) arXiv:2105.02728 — proactive vs. reactive signal finding; "Realistic Market Impact Modeling" arXiv 2026 (MACE paper)

### Walk-Forward Backtesting Pattern

```python
def backtest_social_strategy(
    signals_df: pd.DataFrame,   # Columns: ticker, timestamp, direction, quality_score
    prices_df: pd.DataFrame,    # OHLCV historical data
    train_end: str = "2022-01-01",
    valid_end: str = "2023-01-01",
    test_start: str = "2023-01-01",
    transaction_cost_bps: float = 20.0,  # 20bps round trip (realistic for small caps)
) -> dict:
    
    equity_curve = [1.0]
    trades_log = []
    
    for idx, signal in signals_df.iterrows():
        # Enforce look-ahead prevention: use open of NEXT bar after signal
        entry_bar = prices_df[
            (prices_df['ticker'] == signal['ticker']) &
            (prices_df['timestamp'] > signal['timestamp'])
        ].iloc[0]
        
        # Apply realistic transaction costs
        spread_cost = entry_bar['close'] * transaction_cost_bps / 10000
        fill_price = entry_bar['open'] + spread_cost
        
        # Position size: 2% risk per trade
        shares = int((equity_curve[-1] * 0.02) / fill_price)
        
        # Simulate hold period (max 48 hours)
        exit_bar = get_exit_bar(prices_df, signal['ticker'],
                                entry_bar['timestamp'], max_hours=48)
        
        pnl = (exit_bar['close'] - fill_price) * shares
        pnl -= transaction_cost_bps / 10000 * fill_price * shares * 2  # round trip
        equity_curve.append(equity_curve[-1] + pnl)
    
    returns = pd.Series(equity_curve).pct_change().dropna()
    return {
        "sharpe": returns.mean() / returns.std() * (252**0.5),
        "max_drawdown": (returns.cumsum() - returns.cumsum().cummax()).min(),
        "win_rate": (pd.Series([t['pnl'] for t in trades_log]) > 0).mean(),
        "total_return": equity_curve[-1] - 1.0,
    }
```

---

---

*[⬆ Back to main index](README.md)*
