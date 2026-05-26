"""
TickerExtractor — multi-pass ticker symbol extraction from free text.

Pass 1 (always): cashtag regex  $AAPL  → highest precision
Pass 2 (optional): spaCy NER ORG entities → company name → symbol lookup
Pass 3 (optional): standalone UPPER words matched against a valid-ticker set

The extractor is stateless and synchronous — no I/O, no async.
Callers pass in the valid-ticker universe (from WatchlistManager.get_active())
so the extractor doesn't depend on Redis.

spaCy (Pass 2) is loaded lazily and degrades gracefully when the model
is not installed (test environments, Docker images without the model).
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Pass 1: cashtag regex — $AAPL, $tsla (case insensitive, 1-5 alpha chars)
_CASHTAG_RE = re.compile(r"\$([A-Za-z]{1,5})\b")

# Pass 3: standalone uppercase 2-5 char word (not preceded/followed by $)
_UPPER_WORD_RE = re.compile(r"(?<!\$)\b([A-Z]{2,5})\b")

# Common english words to exclude from pass-3 matching (avoid false positives)
_STOP_WORDS: frozenset[str] = frozenset({
    "I", "A", "IS", "IT", "BE", "DO", "GO", "IN", "ON", "AT", "BY", "TO",
    "UP", "OR", "IF", "SO", "NO", "MY", "HE", "WE", "US", "AN", "AS", "OF",
    "THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL", "CAN", "HAS",
    "HER", "HIS", "HOW", "MAN", "NEW", "NOW", "OLD", "OUR", "OWN", "TOO",
    "TWO", "USE", "WAY", "WHO", "WHY", "YES", "YET", "WITH", "FROM", "THAT",
    "THIS", "THEY", "HAVE", "WILL", "MORE", "BEEN", "WHEN", "WELL", "ALSO",
    "BACK", "SOME", "TIME", "VERY", "EACH", "JUST", "OVER", "SUCH", "THEN",
    "THEM", "EVEN", "MOST", "TELL", "DOES", "THAN", "ONLY", "INTO",
    "SAID", "MAKE", "LOOK", "LIKE", "COME", "KNOW", "TAKE", "GOOD", "GIVE",
    # Finance/Reddit jargon that isn't a ticker
    "ATH", "ATL", "AMA", "TIL", "YOLO", "FOMO", "HODL", "MOON", "BEAR",
    "BULL", "LONG", "PUTS", "CALL", "SELL", "HOLD", "BUY",
    "PNL", "WTF", "IMO", "IMHO", "DD", "EPS", "IPO", "ETF", "OTC",
    "CEO", "CFO", "SEC", "FED", "GDP", "CPI", "ECB", "USD", "EUR",
})


class TickerExtractor:
    """
    Multi-pass ticker extractor.

    Usage:
        extractor = TickerExtractor(use_spacy=True)
        tickers = extractor.extract("$AAPL is up, sell TSLA puts",
                                    valid_tickers={"AAPL", "TSLA", "AMD"})
        # → ["AAPL", "TSLA"]
    """

    def __init__(self, use_spacy: bool = False) -> None:
        self._use_spacy = use_spacy
        self._nlp: Any | None = None  # lazy-loaded spaCy model

    # ── Public API ────────────────────────────────────────────────────────────

    def extract(
        self,
        text: str,
        valid_tickers: set[str] | None = None,
    ) -> list[str]:
        """
        Extract ticker symbols from text.

        Args:
            text: Raw post text.
            valid_tickers: Set of known-valid symbols to validate against.
                           If None, only cashtags (Pass 1) are returned.

        Returns:
            Deduplicated list of ticker symbols in uppercase.
        """
        found: set[str] = set()

        # Pass 1 — cashtags (always run, highest precision)
        for m in _CASHTAG_RE.finditer(text):
            ticker = m.group(1).upper()
            if valid_tickers is None or ticker in valid_tickers:
                found.add(ticker)

        if valid_tickers is None:
            return sorted(found)

        # Pass 2 — spaCy NER (optional, degrades gracefully)
        if self._use_spacy:
            for ticker in self._spacy_extract(text, valid_tickers):
                found.add(ticker)

        # Pass 3 — standalone uppercase words vs watchlist
        for m in _UPPER_WORD_RE.finditer(text):
            word = m.group(1)
            if word not in _STOP_WORDS and word in valid_tickers:
                found.add(word)

        return sorted(found)

    # ── spaCy helpers ─────────────────────────────────────────────────────────

    def _load_spacy(self) -> Any | None:
        """Lazy-load spaCy en_core_web_sm. Returns None if not available."""
        if self._nlp is not None:
            return self._nlp
        try:
            import spacy  # noqa: PLC0415
            self._nlp = spacy.load("en_core_web_sm")
            return self._nlp
        except Exception as exc:
            logger.debug("spaCy not available — Pass 2 skipped: %s", exc)
            self._use_spacy = False  # don't try again
            return None

    def _spacy_extract(self, text: str, valid_tickers: set[str]) -> list[str]:
        """
        Run spaCy NER and match ORG entities against valid ticker set.
        Returns empty list if spaCy is unavailable.
        """
        nlp = self._load_spacy()
        if nlp is None:
            return []
        tickers: list[str] = []
        doc = nlp(text)
        for ent in doc.ents:
            if ent.label_ == "ORG":
                candidate = ent.text.upper().replace(" ", "")[:5]
                if candidate in valid_tickers:
                    tickers.append(candidate)
        return tickers
