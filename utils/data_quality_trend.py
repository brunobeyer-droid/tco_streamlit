from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Tuple

import pandas as pd


def _safe_fetch(fetch_df, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[pd.DataFrame]:
    try:
        return fetch_df(sql, params)
    except Exception:
        return None


def _base_where(filters: Dict[str, Optional[str]]) -> Tuple[str, list]:
    where = ["1=1"]
    params: list = []
    if filters.get("program"):
        where.append("UPPER(PROGRAMNAME) = UPPER(%s)")
        params.append(filters["program"])
    if filters.get("team"):
        where.append("UPPER(TEAMNAME) = UPPER(%s)")
        params.append(filters["team"])
    if filters.get("app_group"):
        where.append("UPPER(GROUPNAME) = UPPER(%s)")
        params.append(filters["app_group"])
    return " AND ".join(where), params


def compute_score_timeseries(fetch_df, scope, filters: Dict[str, Optional[str]]) -> pd.DataFrame:
    """Return PERIOD (int), SCORE, LEVEL, N_GROUPS respecting filters."""
    where, params = _base_where(filters)
    sql = f"""
      SELECT
        TRY_CONVERT(INT, YEAR) AS PERIOD,
        PROGRAMNAME,
        TEAMNAME,
        GROUPNAME,
        TRY_CONVERT(FLOAT, completeness_score) AS SCORE,
        ownership_ok,
        workforce_ok,
        rates_ok,
        nonworkforce_ok,
        enhancements_ok
      FROM VW_TCO_COMPLETENESS
      WHERE {where}
    """
    df = _safe_fetch(fetch_df, sql, tuple(params))
    if df is None or df.empty or "PERIOD" not in df.columns:
        return pd.DataFrame(columns=["PERIOD", "SCORE", "LEVEL", "N_GROUPS"])

    df["PERIOD"] = pd.to_numeric(df["PERIOD"], errors="coerce")
    df = df.dropna(subset=["PERIOD"])
    df["SCORE"] = pd.to_numeric(df.get("SCORE"), errors="coerce").fillna(0.0)

    # Aggregate by period (average score)
    ts = (
        df.groupby("PERIOD")
        .agg(
            SCORE=("SCORE", "mean"),
            N_GROUPS=("GROUPNAME", "nunique"),
        )
        .reset_index()
    )
    def _lvl(s: float) -> str:
        if s >= 90:
            return "HIGH"
        if s >= 70:
            return "MED"
        return "LOW"
    ts["LEVEL"] = ts["SCORE"].apply(_lvl)
    ts = ts.sort_values("PERIOD")
    return ts[["PERIOD", "SCORE", "LEVEL", "N_GROUPS"]]


def compute_driver_timeseries(fetch_df, scope, filters: Dict[str, Optional[str]]) -> pd.DataFrame:
    """Return counts of missing drivers per period."""
    where, params = _base_where(filters)
    sql = f"""
      SELECT
        TRY_CONVERT(INT, YEAR) AS PERIOD,
        ownership_ok,
        workforce_ok,
        rates_ok,
        nonworkforce_ok,
        enhancements_ok
      FROM VW_TCO_COMPLETENESS
      WHERE {where}
    """
    df = _safe_fetch(fetch_df, sql, tuple(params))
    if df is None or df.empty or "PERIOD" not in df.columns:
        return pd.DataFrame(columns=["PERIOD", "OWNERSHIP_MISSING", "WORKFORCE_MISSING", "RATES_MISSING", "NONWORKFORCE_MISSING", "ADO_MISSING"])

    df["PERIOD"] = pd.to_numeric(df["PERIOD"], errors="coerce")
    df = df.dropna(subset=["PERIOD"])

    def _missing(col: str) -> pd.Series:
        vals = df.get(col)
        if vals is None:
            return pd.Series([0] * len(df))
        bools = vals.astype(str).str.lower().isin(["0", "false", "none", "nan", ""])
        return bools.astype(int)

    out = (
        pd.DataFrame(
            {
                "PERIOD": df["PERIOD"],
                "OWNERSHIP_MISSING": _missing("ownership_ok"),
                "WORKFORCE_MISSING": _missing("workforce_ok"),
                "RATES_MISSING": _missing("rates_ok"),
                "NONWORKFORCE_MISSING": _missing("nonworkforce_ok"),
                "ADO_MISSING": _missing("enhancements_ok"),
            }
        )
        .groupby("PERIOD", as_index=False)
        .sum()
        .sort_values("PERIOD")
    )
    return out


__all__ = ["compute_score_timeseries", "compute_driver_timeseries"]
