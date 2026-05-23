## 14. Confidence Assessment & Key Risks

### High Confidence (Directly Verified)

- ✅ FinBERT-Tone sentiment classification code (verified from HemantBK/Algorithmic-Trading-AI)
- ✅ X API v2 endpoints, operators, rate limits (verified from docs.x.com)
- ✅ Reddit PRAW API access patterns (verified from praw.readthedocs.io + live WSB JSON)
- ✅ StockTwits public REST API (verified from api.stocktwits.com live response)
- ✅ ib_async connection, order placement, bracket orders (verified from ib-api-reloaded/ib_async README)
- ✅ Redis Streams architecture (verified from ashwini-singhh/crypto_trading_agent production code)
- ✅ PostgreSQL schema patterns (verified from ashwini-singhh:db/init.sql)
- ✅ Kelly criterion formulas (verified from qoppac.blogspot.com, quant.stackexchange.com)
- ✅ Fear & Greed Index = 29 (Fear) at time of research (live API response)
- ✅ LunarCrush v4 API endpoints (verified from lunarcrush.com/api4 docs)

### Medium Confidence (Inferred / Synthesized)

- ⚠️ Specific signal quality scoring weights (0.30/0.25/0.20/0.15/0.10) — practitioner synthesis, not from a single source; tune in backtesting
- ⚠️ VIX thresholds for regime scaling — conventional practitioner values, not from single academic source
- ⚠️ 48-hour maximum hold period — synthesis of multiple papers; Buz & de Melo found 1–3 day signal window
- ⚠️ Alpha decay lambda = 0.10 (7-hour half-life) — from Lee (2025) model parameters; actual λ varies by instrument

### Key Risks to Mitigate

| Risk | Severity | Mitigation |
|------|---------|-----------|
| **API cost overrun (X API)** | High | Use Counts endpoint (free) for volume; only stream when actually trading |
| **Regulatory risk (market manipulation)** | High | Avoid positions based on posts from accounts you control; consult securities counsel |
| **Mean-reversion wipeout** | High | Enforce hard 48-hour time stop; never hold through the mean-reversion window |
| **Crowded trade — simultaneous bots** | High | Cross-platform convergence filter; size down when many instruments signal at once |
| **IBKR connection drop** | Medium | Reconnect handler with exponential backoff; IB Gateway Stable release |
| **Bot/coordinated campaign signals** | Medium | Multi-factor bot detection; minimum account age 30 days |
| **Liquidity cliff (meme stocks)** | Medium | ADV filter; max 0.5% of ADV per order |
| **Small-cap slippage in backtests** | Medium | Include Almgren-Chriss impact model; use 20bps round-trip cost floor |

---

---

*[⬆ Back to main index](README.md)*
