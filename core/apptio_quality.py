from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio

from core.data import fetch_apptio_actuals_monthly_breakdown


@cache_data_portfolio(ttl=180, show_spinner=False)
def compute_apptio_mapping_coverage(
    db: Any,
    fiscal_year: int,
    program_ids: Optional[list[str]] = None,
    rev: Optional[int] = None,
) -> Dict[str, Any]:
    """Compute mapping coverage for Apptio actuals based on enriched monthly breakdown rows."""
    _ = db  # kept for signature compatibility; data access goes via `core.data` wrappers.
    df = fetch_apptio_actuals_monthly_breakdown(int(fiscal_year), list(program_ids or []), rev=rev)
    if df is None or df.empty:
        empty = pd.DataFrame()
        return {
            "total_amount": 0.0,
            "mapped_amount": 0.0,
            "default_other_amount": 0.0,
            "coverage_pct_spend": 0.0,
            "rows_total": 0,
            "rows_mapped": 0,
            "coverage_pct_rows": 0.0,
            "top_defaulted_by_program": empty,
            "top_defaulted_by_type": empty,
        }

    w = df.copy()
    w["MAPPING_STATUS"] = w.get("MAPPING_STATUS", "").fillna("").astype(str).str.upper().str.strip()
    w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    w["COST_TYPE"] = w.get("COST_TYPE", "").fillna("").astype(str).str.strip()
    w["SUBTYPE"] = w.get("SUBTYPE", "").fillna("").astype(str).str.strip()
    w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)

    total_amount = float(w["AMOUNT"].sum())
    is_default = w["MAPPING_STATUS"].eq("DEFAULT_OTHER")
    default_other_amount = float(w.loc[is_default, "AMOUNT"].sum())
    mapped_amount = float(w.loc[~is_default, "AMOUNT"].sum())
    coverage_pct_spend = float(mapped_amount / total_amount) if total_amount else 0.0

    rows_total = int(len(w.index))
    rows_mapped = int((~is_default).sum())
    coverage_pct_rows = float(rows_mapped / rows_total) if rows_total else 0.0

    by_program = (
        w.groupby("PROGRAMNAME", dropna=False)
        .agg(
            default_other_amount=("AMOUNT", lambda s: float(s[w.loc[s.index, "MAPPING_STATUS"].eq("DEFAULT_OTHER")].sum())),
            total_amount=("AMOUNT", "sum"),
        )
        .reset_index()
    )
    by_program["pct_defaulted"] = by_program.apply(
        lambda r: float(r["default_other_amount"] / r["total_amount"]) if float(r["total_amount"] or 0.0) else 0.0,
        axis=1,
    )
    top_defaulted_by_program = by_program.sort_values("default_other_amount", ascending=False).head(25)

    top_defaulted_by_type = (
        w.loc[is_default]
        .groupby(["COST_TYPE", "SUBTYPE"], dropna=False)["AMOUNT"]
        .sum()
        .reset_index()
        .rename(columns={"AMOUNT": "default_other_amount"})
        .sort_values("default_other_amount", ascending=False)
        .head(50)
    )

    return {
        "total_amount": total_amount,
        "mapped_amount": mapped_amount,
        "default_other_amount": default_other_amount,
        "coverage_pct_spend": coverage_pct_spend,
        "rows_total": rows_total,
        "rows_mapped": rows_mapped,
        "coverage_pct_rows": coverage_pct_rows,
        "top_defaulted_by_program": top_defaulted_by_program,
        "top_defaulted_by_type": top_defaulted_by_type,
    }
