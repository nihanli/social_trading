"""
LiquidityGate — pre-trade liquidity and spread checks.

A signal is only forwarded to execution if it passes all gate checks:
  1. Average Daily Volume (ADV) ≥ cfg.trade_min_adv_usd
  2. Market cap             ≥ cfg.trade_min_mcap_usd
  3. Bid-ask spread         ≤ cfg.trade_max_spread_bps  (basis points)
  4. Order size             ≤ cfg.trade_max_order_adv_pct × ADV

Gate inputs are injected — no direct market data calls here.
The execution service fetches quotes and passes them in.

Usage:
    gate = LiquidityGate()
    result = gate.check(signal, quote, cfg)
    if result.passed:
        # forward signal
    else:
        logger.info("Blocked: %s", result.reason)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from social_trading.config.system_config import SystemConfig
from social_trading.core.models import Signal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiquidityQuote:
    """
    Market microstructure snapshot for a ticker.
    Provided by the MarketDataProvider and injected into LiquidityGate.
    """

    ticker: str
    last_price: float
    bid: float
    ask: float
    adv_shares: float       # average daily volume in shares
    adv_usd: float          # average daily volume in USD
    market_cap_usd: float   # approximate market cap


@dataclass(frozen=True)
class GateResult:
    """Result of a liquidity gate check."""

    passed: bool
    reason: str


_PASS = GateResult(passed=True, reason="ok")


class LiquidityGate:
    """
    Stateless gate — all context passed at call time.

    All four checks must pass; first failure short-circuits.
    """

    def check(
        self,
        signal: Signal,
        quote: LiquidityQuote,
        cfg: SystemConfig,
        order_shares: int = 0,
    ) -> GateResult:
        """
        Gate a signal against liquidity constraints.

        Args:
            signal:       Signal being evaluated.
            quote:        Current market microstructure for signal.ticker.
            cfg:          SystemConfig with liquidity thresholds.
            order_shares: Proposed order size in shares (0 = skip ADV% check).

        Returns:
            GateResult(passed=True, reason='ok') or GateResult(passed=False, reason=...)
        """
        # ── 1. ADV check ──────────────────────────────────────────────────────
        if quote.adv_usd < cfg.trade_min_adv_usd:
            return GateResult(
                passed=False,
                reason=(
                    f"{signal.ticker} ADV ${quote.adv_usd:,.0f} < "
                    f"minimum ${cfg.trade_min_adv_usd:,.0f}"
                ),
            )

        # ── 2. Market cap check ───────────────────────────────────────────────
        if quote.market_cap_usd < cfg.trade_min_mcap_usd:
            return GateResult(
                passed=False,
                reason=(
                    f"{signal.ticker} market cap ${quote.market_cap_usd:,.0f} < "
                    f"minimum ${cfg.trade_min_mcap_usd:,.0f}"
                ),
            )

        # ── 3. Spread check ───────────────────────────────────────────────────
        spread_bps = _spread_bps(quote)
        if spread_bps > cfg.trade_max_spread_bps:
            return GateResult(
                passed=False,
                reason=(
                    f"{signal.ticker} spread {spread_bps:.1f} bps > "
                    f"max {cfg.trade_max_spread_bps} bps"
                ),
            )

        # ── 4. Order-size as fraction of ADV ─────────────────────────────────
        if order_shares > 0 and quote.adv_shares > 0:
            order_adv_pct = order_shares / quote.adv_shares
            if order_adv_pct > cfg.trade_max_order_adv_pct:
                return GateResult(
                    passed=False,
                    reason=(
                        f"{signal.ticker} order {order_shares} shares = "
                        f"{order_adv_pct:.2%} of ADV > "
                        f"max {cfg.trade_max_order_adv_pct:.2%}"
                    ),
                )

        return _PASS

    def check_batch(
        self,
        signals: list[Signal],
        quotes: dict[str, LiquidityQuote],
        cfg: SystemConfig,
        order_shares: dict[str, int] | None = None,
    ) -> list[tuple[Signal, GateResult]]:
        """
        Gate a batch of signals. Returns list of (signal, result) pairs.
        Signals without a quote entry are rejected with reason 'no_quote'.
        """
        order_shares = order_shares or {}
        results = []
        for sig in signals:
            if sig.ticker not in quotes:
                results.append((sig, GateResult(passed=False, reason=f"{sig.ticker}: no_quote")))
                continue
            result = self.check(
                sig,
                quotes[sig.ticker],
                cfg,
                order_shares=order_shares.get(sig.ticker, 0),
            )
            results.append((sig, result))
        return results


# ── Pure helpers ──────────────────────────────────────────────────────────────

def _spread_bps(quote: LiquidityQuote) -> float:
    """Compute bid-ask spread in basis points."""
    mid = (quote.bid + quote.ask) / 2.0
    if mid <= 0:
        return 0.0
    return ((quote.ask - quote.bid) / mid) * 10_000
