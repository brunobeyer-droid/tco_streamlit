from __future__ import annotations

import re
from typing import Iterable


_STOPWORDS = {
    "app",
    "application",
    "service",
    "prod",
    "production",
    "tool",
    "system",
}


def normalize_app_name(s: str, *, stopwords: Iterable[str] = _STOPWORDS) -> str:
    """Normalize an ADO application name for matching.

    Rules:
    - lowercase
    - strip punctuation/symbols
    - collapse whitespace
    - remove common stopwords (e.g., "app", "application", "service", "prod", "production")
    """
    raw = (s or "").strip().lower()
    if not raw:
        return ""
    # Replace punctuation with spaces; keep alphanumerics.
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    parts = [p for p in raw.split() if p and p not in set(stopwords)]
    return " ".join(parts).strip()


__all__ = ["normalize_app_name"]
