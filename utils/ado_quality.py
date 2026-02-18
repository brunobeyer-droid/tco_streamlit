from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd


def load_ado_quality(fetch_df, year: int, program: Optional[str] = None, team: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return ADO detail and summary dataframes for mapping + SWAG demand readiness.

    Demand driver contract:
    - Expected/Forecast uses SWAG-derived `DERIVED_FTE_FEATURE` only (no points/manual-FTE fallbacks).
    - A feature is considered SWAG-ready when `DERIVED_FTE_FEATURE > 0`.
    """
    params: list[Any] = [year]
    if program:
        params.extend([program, program])
    if team:
        params.append(team)

    sql_detail = f"""
        SELECT
          d.FEATURE_ID,
          COALESCE(d.TEAMNAME, '(Unmapped Team)') AS TEAM,
          COALESCE(d.GROUPNAME, '(Unmapped Application)') AS APP_GROUP,
          COALESCE(d.PROGRAMID, '') AS PROGRAMID,
          TRY_CONVERT(FLOAT, d.DERIVED_FTE_FEATURE) AS DERIVED_FTE,
          CASE WHEN UPPER(COALESCE(d.MAPPING_STATUS, '')) = 'OUT_OF_SCOPE_PROGRAM' THEN 1 ELSE 0 END AS MISSING_PROGRAM_TEAM_MAP,
          CASE WHEN UPPER(COALESCE(d.MAPPING_STATUS, '')) = 'UNMAPPED_APP_GROUP' THEN 1 ELSE 0 END AS MISSING_APP_GROUP_MAP,
          CASE WHEN COALESCE(TRY_CONVERT(FLOAT, d.DERIVED_FTE_FEATURE), 0) <= 0 THEN 1 ELSE 0 END AS MISSING_SWAG_DEMAND
        FROM VW_TCO_FEATURE_DEMAND d
        WHERE TRY_CONVERT(INT, d.YEAR) = %s
          AND COALESCE(d.IN_SCOPE_FOR_ROADMAP, 0) = 1
          AND UPPER(COALESCE(d.FEATURE_STATE, '')) <> 'REMOVED'
    """
    # Optional filters (best-effort; depends on mapping completeness in the demand view)
    if program:
        sql_detail += " AND (UPPER(LTRIM(RTRIM(COALESCE(d.PROGRAMID, '')))) = UPPER(%s) OR UPPER(LTRIM(RTRIM(COALESCE(d.PROGRAMNAME, '')))) = UPPER(%s))"
    if team:
        sql_detail += " AND UPPER(LTRIM(RTRIM(COALESCE(d.TEAMNAME, '')))) = UPPER(%s)"
    detail = fetch_df(sql_detail, tuple(params))
    if detail is None:
        detail = pd.DataFrame()
    if not detail.empty:
        detail = detail.copy()
        detail["TEAM"] = detail.get("TEAM", "").astype(str).str.strip()
        detail["APP_GROUP"] = detail.get("APP_GROUP", "").astype(str).str.strip().replace({"": "(Unmapped Application)"})
        detail["DERIVED_FTE"] = pd.to_numeric(detail.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
        for c in ["MISSING_PROGRAM_TEAM_MAP", "MISSING_APP_GROUP_MAP", "MISSING_SWAG_DEMAND"]:
            detail[c] = pd.to_numeric(detail.get(c), errors="coerce").fillna(0).astype(int)
        detail["MISSING_ANY"] = (
            detail["MISSING_PROGRAM_TEAM_MAP"].astype(int)
            | detail["MISSING_APP_GROUP_MAP"].astype(int)
            | detail["MISSING_SWAG_DEMAND"].astype(int)
        ).astype(int)

    summary = pd.DataFrame()
    if not detail.empty:
        def _as_int(s: Any) -> pd.Series:
            return pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

        detail["FEATURE_ID"] = detail["FEATURE_ID"]
        gcols = ["TEAM", "APP_GROUP"]
        group_keys = [c for c in gcols if c in detail.columns]
        if group_keys:
            summary = detail.groupby(group_keys).agg(
                FEATURES_TOTAL=("FEATURE_ID", "nunique"),
                FEATURES_MISSING_PROGRAM_TEAM_MAP=("MISSING_PROGRAM_TEAM_MAP", lambda x: _as_int(x).sum()),
                FEATURES_MISSING_APP_GROUP_MAP=("MISSING_APP_GROUP_MAP", lambda x: _as_int(x).sum()),
                FEATURES_MISSING_SWAG_DEMAND=("MISSING_SWAG_DEMAND", lambda x: _as_int(x).sum()),
                FEATURES_MISSING_ANY=("MISSING_ANY", lambda x: _as_int(x).sum()),
                PROGRAM_FIRST=("PROGRAMID", "first"),
            ).reset_index()
    return detail, summary


__all__ = ["load_ado_quality"]
