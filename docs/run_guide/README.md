# Social Trading — Operations Guide

## Quick Reference

| Goal | Command |
|------|---------|
| Start infrastructure | `make up` |
| Start all services | `make start` |
| Stop all services | `Ctrl+C` in honcho terminal |
| Emergency: close all positions | Streamlit sidebar → **Close ALL Positions** |
| Open UI | http://localhost:8501 |
| Check pipeline health | `redis-cli xlen raw_social` |
| View logs | honcho terminal (colour-coded by service) |

---

## Contents

| # | File | Description |
|---|------|-------------|
| 1 | [01-environment-setup.md](01-environment-setup.md) | Machine prerequisites, API keys, IBKR, `.env`, first-time DB init |
| 2 | [02-run-workflows.md](02-run-workflows.md) | Debug · QA/Paper · Production run step-by-step |
| 3 | [03-starting-the-system.md](03-starting-the-system.md) | Startup sequence, partial starts |
| 4 | [04-stopping-the-system.md](04-stopping-the-system.md) | Normal stop, emergency stop, full reset |
| 5 | [05-streamlit-ui.md](05-streamlit-ui.md) | All pages and controls documented |
| 6 | [06-monitoring.md](06-monitoring.md) | Redis health checks, logs, Grafana, API rate limits |
| 7 | [07-debugging.md](07-debugging.md) | Common problems, Redis inspection, per-service isolation |
| 8 | [08-paper-trading-checklist.md](08-paper-trading-checklist.md) | 5-day paper run checklist + Sharpe gate |
| 9 | [09-live-trading.md](09-live-trading.md) | Prerequisites, risk tightening, switch procedure, scaling |
| 10 | [10-reference.md](10-reference.md) | Ports, network, market hours, data persistence |

---

## Where to Start

**New machine?** → Start at [Part 1 — Environment Setup](01-environment-setup.md)

**Daily paper run?** → [Part 2 — Run Workflows](02-run-workflows.md) (Workflow B)

**Something broken?** → [Part 7 — Debugging](07-debugging.md)

**Ready to go live?** → [Part 9 — Live Trading](09-live-trading.md)
