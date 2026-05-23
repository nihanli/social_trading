## 1. Academic Foundation

### Validated Research Findings

| Paper | Key Finding | Horizon |
|-------|------------|---------|
| Bollen, Mao & Zeng (2011) — "Twitter Mood Predicts the Stock Market" | Twitter "Calm" mood dimension Granger-causes DJIA moves; 87.6% directional accuracy | 3–4 days |
| Sul, Dennis & Yuan (2017) — "Trading on Twitter" | Twitter sentiment predicts individual stock returns with statistical significance | 1–2 days |
| Agrawal, Azar & Lo (2018) — "Momentum, Mean-Reversion, and Social Media" | **Critical**: Short-term momentum is followed by mean-reversion at 1–4 weeks — go long then exit before reversal | 1–3 days long, fade at 1–4 weeks |
| Buz & de Melo (2021) — "Should You Take Investment Advice From WallStreetBets?" | WSB portfolio +200% over 3 years (2019–2021); signal accuracy NOT significantly better than random UNLESS proactive signals are filtered | 1–3 days |
| Goyal et al. (2025) — "Leveraging Social Media Sentiment for Predictive Algorithmic Trading Strategies" | BERTweet-based Reddit signals produced higher returns with lower risk vs. buy-and-hold | 1–5 days |
| Choi et al. (2024) — "Stock Price Momentum Modeling Using Social Media Data" | Combined social + technical momentum achieves statistically significant edge for small-cap stocks | Intraday–3 days |

[^1]: Bollen, J., Mao, H., & Zeng, X. (2011). Twitter mood predicts the stock market. *Journal of Computational Science*, 2(1), 1–8. arXiv:1010.3003
[^2]: Agrawal, S., Azar, P., & Lo, A. (2018). Momentum, Mean-Reversion, and Social Media: Evidence from StockTwits and Twitter. *Journal of Portfolio Management*
[^3]: ashwini-singhh/crypto_trading_agent — microservices trading platform (GitHub)
[^4]: Lee (2025). Factor Alpha Decay. arXiv:2512.08267 — hyperbolic decay model: α(t) = K/(1 + λ·t)
[^5]: Agrawal, Azar & Lo (2018) — mean-reversion begins at week 1–4 post-signal

### Financial Instruments Most Responsive to Social Media

| Instrument | Responsiveness | Reason |
|------------|---------------|--------|
| Small-cap US equities | ⭐⭐⭐⭐⭐ | Low institutional coverage; thin float; retail-dominated |
| Meme stocks (GME, AMC) | ⭐⭐⭐⭐⭐ | WallStreetBets-driven; demonstrated multi-100% moves |
| Cryptocurrency (BTC, DOGE) | ⭐⭐⭐⭐⭐ | 24/7 trading; high retail participation; influencer-sensitive |
| Biotech stocks | ⭐⭐⭐⭐ | Binary FDA events; high tweet volume around clinical results |
| Mid-cap tech | ⭐⭐⭐ | CEO tweets; moderate retail activity |
| Large-cap / S&P 500 | ⭐⭐ | Aggregate mood signal only; hard to move due to institutions |

### Critical Time Horizon Rules

```
Signal detected → Entry within 1–2 hours (alpha decays rapidly)
Hold window:     1–3 trading days (momentum phase)
Maximum hold:    Never beyond day 5 (mean-reversion begins)
Exit triggers:   1) Time limit, 2) Price stop-loss, 3) Sentiment reversal
```

---

---

*[⬆ Back to main index](README.md)*
