from __future__ import annotations

from typing import Any, List, Optional

import pandas as pd


def list_pis(fetch_df) -> List[Any]:
    try:
        df = fetch_df("SELECT DISTINCT PI FROM VW_TCO_WORKFORCE_SPLIT WHERE PI IS NOT NULL ORDER BY PI")
    except Exception:
        return []
    if df is None or df.empty or "PI" not in df.columns:
        return []
    return df["PI"].dropna().tolist()


def list_years(fetch_df) -> List[int]:
    try:
        df = fetch_df("SELECT DISTINCT YEAR FROM VW_TCO_WORKFORCE_SPLIT ORDER BY YEAR DESC")
    except Exception:
        return []
    if df is None or df.empty or "YEAR" not in df.columns:
        return []
    years = pd.to_numeric(df["YEAR"], errors="coerce").dropna().astype(int).tolist()
    return sorted(set(years), reverse=True)


__all__ = ["list_pis", "list_years"]
