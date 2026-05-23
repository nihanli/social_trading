# Development Plan

## Index

| § | Document | Summary |
|---|----------|---------|
| [01](01-project-structure.md) | Project Structure | File tree, pyproject.toml, Makefile |
| [02](02-protocols-and-interfaces.md) | Protocols & Interfaces | Core models, Protocol definitions, DI pattern |
| [03](03-development-phases.md) | Development Phases | 8-phase roadmap with tasks and deliverables |
| [04](04-testing-strategy.md) | Testing Strategy | Unit/integration/E2E, fixtures, CI config |

---

## At a Glance

**Stack:** Python 3.11, asyncio, Pydantic v2, Redis Streams, PostgreSQL, ib_async, FinBERT, Streamlit, Grafana

**Structure:** `src/` layout — `social_trading/{core, config, ingest, nlp, signals, risk, execution, market_data, storage, monitoring, services}`

**Key design decisions:**

- `core/protocols.py` defines all cross-component contracts via `typing.Protocol` — no circular imports
- Each service is a standalone asyncio process; all inter-service communication via Redis Streams
- Data sources are pluggable: implement `BaseDataSource` + register in `DataSourceRegistry`
- All external dependencies injected → tests replace them with fakes satisfying the same protocols
- Paper trading engine runs against the same `ExecutionEngine` protocol as IBKR — identical code path

---

## Developer Quick Start

```bash
git clone <repo>
cd social_trading
pip install -e ".[dev]"
python -m spacy download en_core_web_sm
make up          # start postgres + redis
make migrate     # run DB migrations
make test        # verify unit tests pass
```

---

## Phase Roadmap (10 weeks)

```
Week 1:   Phase 0 — Foundation (project scaffold, models, protocols, Docker)
Week 2-3: Phase 1 — Data Ingestion  ┐ can run in parallel
Week 3-4: Phase 2 — NLP Pipeline    ┘
Week 5:   Phase 3 — Signal Generation + Phase 4 — Risk Management
Week 6:   Phase 5 — Execution (paper engine first, then IBKR)
Week 7:   Phase 6 — Infrastructure (Docker Compose, Prometheus, Grafana)
Week 8:   Phase 7 — UI (Streamlit pages)
Week 9-10: Phase 8 — Integration QA + Paper Trading Run
```

---

## Go-Live Gate

Paper trading ≥ 5 days, then Streamlit optimize page (design §17):
- Sharpe ratio > 0.5
- Win rate > 45%
- No unexpected circuit breaker triggers
- IBKR live account dry run (micro lots) ≥ 1 week

---

*See [design docs](../design/README.md) for full system design.*
