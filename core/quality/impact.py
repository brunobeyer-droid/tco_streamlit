from __future__ import annotations

from typing import Any, Optional, Tuple

import pandas as pd

from core.quality.context import QualityContext


def _find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    by_lower = {str(c).lower(): str(c) for c in cols}
    for cand in candidates:
        if cand in cols:
            return cand
        hit = by_lower.get(str(cand).lower())
        if hit:
            return hit
    return None


def _as_float_series(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df.get(col), errors="coerce").fillna(0.0).astype(float)


def _filter_workforce(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    # Prefer COST_CATEGORY if present; else use COST_BUCKET.
    cat_col = _find_col(df, ["COST_CATEGORY"])
    if cat_col:
        cat = df[cat_col].fillna("").astype(str).str.upper().str.strip()
        return df[cat.isin({"WORK_FORCE", "WORKFORCE", "WF"})].copy()
    bucket_col = _find_col(df, ["COST_BUCKET"])
    if bucket_col:
        bucket = df[bucket_col].fillna("").astype(str).str.upper().str.strip()
        return df[bucket.eq("WF")].copy()
    return df.copy()


def estimate_usd_from_fte(
    ctx: QualityContext,
    *,
    year: Optional[int] = None,
    program: Optional[str] = None,
    team: Optional[str] = None,
) -> Tuple[float, str, str]:
    """Estimate USD per FTE using blended workforce $/FTE from canonical cost lines.

    Returns: (usd_per_fte, basis_label, confidence_label)
    """
    df = ctx.cost_lines
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return (0.0, "No rate available", "LOW")

    work = df.copy()

    year_i = int(year or ctx.year or 0) if (year or ctx.year) else None
    year_col = _find_col(work, ["YEAR", "FISCAL_YEAR", "ADO_YEAR"])
    if year_col and year_i:
        work = work[pd.to_numeric(work[year_col], errors="coerce").fillna(-1).astype(int).eq(int(year_i))].copy()

    scen_col = _find_col(work, ["SCENARIO"])
    if scen_col:
        scen = work[scen_col].fillna("").astype(str).str.upper().str.strip()
        # Prefer BASELINE workforce costs; fall back to EXPECTED if no baseline rows exist.
        baseline = work[scen.eq("BASELINE")].copy()
        expected = work[scen.eq("EXPECTED")].copy()
    else:
        baseline = work
        expected = pd.DataFrame()

    def _best_slice(frame: pd.DataFrame, label: str) -> Tuple[Optional[float], str]:
        if frame is None or frame.empty:
            return (None, "")
        wf = _filter_workforce(frame)
        if wf.empty:
            return (None, "")
        amt_col = _find_col(wf, ["AMOUNT", "COST_USD", "AMOUNT_USD", "COST"])
        fte_col = _find_col(wf, ["FTE", "fte", "FTE_EQUIV", "STAFF_FTE"])
        if not amt_col or not fte_col:
            return (None, "")
        amt = _as_float_series(wf, amt_col)
        fte = _as_float_series(wf, fte_col)
        mask = fte > 0
        if not mask.any():
            return (None, "")
        usd_per_fte = float(amt.loc[mask].sum() / max(float(fte.loc[mask].sum()), 1e-9))
        return (usd_per_fte, f"{label} WF blended $/FTE ({'Baseline' if label=='Program' else 'Baseline'} WF costs, {year_i})")

    # Try program-level, then team-level, then global.
    def _apply_scope(frame: pd.DataFrame) -> pd.DataFrame:
        out = frame
        if program:
            col = _find_col(out, ["PROGRAMNAME", "PROGRAM"])
            if col:
                out = out[out[col].fillna("").astype(str).str.upper().str.strip().eq(str(program).upper().strip())].copy()
        if team:
            col = _find_col(out, ["TEAMNAME", "TEAM"])
            if col:
                out = out[out[col].fillna("").astype(str).str.upper().str.strip().eq(str(team).upper().strip())].copy()
        return out

    for base_frame in [baseline, expected]:
        scoped = _apply_scope(base_frame)
        if program and not scoped.empty:
            rate, basis = _best_slice(scoped, "Program")
            if rate and rate > 0:
                return (rate, f"Est. using Program blended rate $/FTE (WF costs, {year_i})", "HIGH")
        if team and not scoped.empty:
            rate, basis = _best_slice(scoped, "Team")
            if rate and rate > 0:
                return (rate, f"Est. using Team blended rate $/FTE (WF costs, {year_i})", "MEDIUM")
        rate, basis = _best_slice(scoped if (program or team) else base_frame, "Global")
        if rate and rate > 0:
            return (rate, f"Est. using Global blended rate $/FTE (WF costs, {year_i})", "MEDIUM")

    return (0.0, "No rate available", "LOW")

