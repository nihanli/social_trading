## 10. Paper Trading → Live Deployment Pipeline

```
Phase 1: BACKTESTING (historical data, no real money)
├── Strategy parameters tuned on 2+ years historical data
├── Walk-forward validation (train/validate/test splits)
├── Gate: Sharpe > 1.0, max drawdown < 15%, profit factor > 1.3
└── Output: parameter set + expected performance metrics

Phase 2: PAPER TRADING (live signals, simulated money, 4–8 weeks)
├── Full system running in production mode
├── Paper trading engine simulates fills with slippage
├── Risk manager active and enforcing all limits
├── Gate: Paper Sharpe ≥ backtest estimate ± 30%, no circuit breakers triggered
└── Output: confidence in live deployment

Phase 3: LIVE TRADING (start small, scale up)
├── Start with 10% of intended capital
├── Monitor for 2+ weeks, verify execution quality
├── Scale to 50%, then 100% if metrics hold
└── Kill switch: TRADING_ENABLE=false env var → immediate halt
```

The single key swap from paper to live is changing which service consumes `selected_signals_stream`:

```yaml
# docker-compose.yml — swap comment to go live
# paper-trading-engine:  # Phase 2
execution-engine:         # Phase 3
  environment:
    - TRADING_ENABLE=true
    - IBKR_PORT=4001        # IB Gateway live
    - MAX_RISK_PER_TRADE=0.01
    - DAILY_LOSS_LIMIT=0.03
```

[^22]: ashwini-singhh/crypto_trading_agent:python-services/paper-trading/main.py (simulate_fill_price pattern)

---

---

*[⬆ Back to main index](README.md)*
