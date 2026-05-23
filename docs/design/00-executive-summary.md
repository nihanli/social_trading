## Executive Summary

Social media momentum trading is a quantitatively validated strategy with academic foundations
dating to Bollen et al. (2011), who demonstrated 87.6% accuracy predicting DJIA direction from
Twitter sentiment.[^1] The strategy involves monitoring platforms (X/Twitter, Reddit, StockTwits)
for unusual spikes in mentions and positive sentiment around financial instruments, entering
short-term long/short positions on the signal, and exiting within 1–3 trading days before
the well-documented mean-reversion effect occurs.[^2]

The system requires five integrated layers: **data ingestion** (social APIs), **NLP processing**
(FinBERT sentiment analysis), **signal generation** (aggregation and scoring), **risk management**
(position sizing and circuit breakers), and **execution** (Interactive Brokers via `ib_async`).
A production deployment uses an event-driven microservices architecture with Redis Streams as
the messaging backbone, PostgreSQL for persistence, and Prometheus/Grafana for observability.[^3]

**Key constraint:** Social media signals have extremely short alpha half-lives — the alpha typically
halves within 4–12 hours, requiring fast execution.[^4] The 1–3 day momentum window eventually
inverts to mean-reversion at 1–4 weeks, so position exits must be time-controlled.[^5]

---

---

*[⬆ Back to main index](README.md)*
