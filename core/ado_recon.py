from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence, Set

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio
from core.name_resolution import apply_display_scope_names

_SQL_INT32_MAX = 2_147_483_647

try:
    from db import fetch_df_active as fetch_df  # type: ignore
except Exception:  # pragma: no cover
    fetch_df = None  # type: ignore
try:
    from db import ensure_tco_team_velocity_snapshot_table, refresh_tco_team_velocity_snapshot  # type: ignore
except Exception:  # pragma: no cover
    ensure_tco_team_velocity_snapshot_table = None  # type: ignore
    refresh_tco_team_velocity_snapshot = None  # type: ignore
try:
    from db import _fq as _db_fq  # type: ignore
except Exception:  # pragma: no cover
    _db_fq = None  # type: ignore


def _fq(name: str) -> str:
    try:
        return _db_fq(name) if _db_fq else name
    except Exception:
        return name


def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _is_timeout_error(exc: Exception) -> bool:
    msg = str(exc or "").strip().lower()
    return ("timed out" in msg) or ("hyt00" in msg) or ("timeout" in msg)


def _resolve_program_scope_tokens(programs: Sequence[str]) -> List[str]:
    vals = [str(v).strip() for v in (programs or []) if str(v).strip()]
    if not vals or fetch_df is None:
        return vals
    try:
        ph = ", ".join(["%s"] * len(vals))
        up_vals = [v.upper() for v in vals]
        df = fetch_df(
            f"""
            SELECT PROGRAMNAME, PROGRAM_DISPLAY_NAME
            FROM {_fq('PROGRAMS')}
            WHERE UPPER(LTRIM(RTRIM(COALESCE(PROGRAMNAME, '')))) IN ({ph})
               OR UPPER(LTRIM(RTRIM(COALESCE(PROGRAM_DISPLAY_NAME, '')))) IN ({ph})
            """,
            tuple(up_vals + up_vals),
        )
    except Exception:
        return vals
    out = set(vals)
    if isinstance(df, pd.DataFrame) and not df.empty:
        for c in ("PROGRAMNAME", "PROGRAM_DISPLAY_NAME"):
            if c in df.columns:
                out.update(
                    {
                        str(v).strip()
                        for v in df[c].dropna().astype(str).tolist()
                        if str(v).strip()
                    }
                )
    return sorted(out)


def _resolve_team_scope_tokens(teams: Sequence[str]) -> List[str]:
    vals = [str(v).strip() for v in (teams or []) if str(v).strip()]
    if not vals or fetch_df is None:
        return vals
    try:
        ph = ", ".join(["%s"] * len(vals))
        up_vals = [v.upper() for v in vals]
        df = fetch_df(
            f"""
            SELECT TEAMNAME, TEAM_DISPLAY_NAME
            FROM {_fq('TEAMS')}
            WHERE UPPER(LTRIM(RTRIM(COALESCE(TEAMNAME, '')))) IN ({ph})
               OR UPPER(LTRIM(RTRIM(COALESCE(TEAM_DISPLAY_NAME, '')))) IN ({ph})
            """,
            tuple(up_vals + up_vals),
        )
    except Exception:
        return vals
    out = set(vals)
    if isinstance(df, pd.DataFrame) and not df.empty:
        for c in ("TEAMNAME", "TEAM_DISPLAY_NAME"):
            if c in df.columns:
                out.update(
                    {
                        str(v).strip()
                        for v in df[c].dropna().astype(str).tolist()
                        if str(v).strip()
                    }
                )
    return sorted(out)


@cache_data_portfolio(ttl=300, show_spinner=False)
def _velocity_snapshot_lookup(years_key: tuple[int, ...]) -> pd.DataFrame:
    if fetch_df is None:
        return pd.DataFrame()
    years_i = sorted({int(y) for y in years_key if y is not None and int(y) > 0})
    if not years_i:
        return pd.DataFrame()
    ph = ", ".join(["%s"] * len(years_i))
    sql = f"""
      SELECT
        COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMID)), ''), 'UNKNOWN') AS TEAMID,
        TRY_CONVERT(INT, v.YEAR) AS YEAR,
        TRY_CONVERT(INT, v.PI) AS PI,
        MAX(TRY_CONVERT(FLOAT, v.EFFECTIVE_BASELINE_POINTS)) AS EFFECTIVE_BASELINE_POINTS
      FROM {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')} v
      WHERE TRY_CONVERT(INT, v.YEAR) IN ({ph})
        AND TRY_CONVERT(INT, v.PI) IS NOT NULL
      GROUP BY
        COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMID)), ''), 'UNKNOWN'),
        TRY_CONVERT(INT, v.YEAR),
        TRY_CONVERT(INT, v.PI)
      HAVING MAX(COALESCE(TRY_CONVERT(FLOAT, v.EFFECTIVE_BASELINE_POINTS), 0.0)) > 0
    """
    try:
        df = fetch_df(sql, tuple(years_i))
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    needed = {"TEAMID", "YEAR", "PI", "EFFECTIVE_BASELINE_POINTS"}
    present = {str(c).strip().upper() for c in df.columns.tolist()}
    if not needed.issubset(present):
        return pd.DataFrame()
    out = df.copy()
    out["TEAMID"] = out.get("TEAMID", "").astype(str).str.strip().str.upper().replace({"": "UNKNOWN"})
    out["YEAR"] = pd.to_numeric(out.get("YEAR"), errors="coerce").astype("Int64")
    out["PI"] = pd.to_numeric(out.get("PI"), errors="coerce").astype("Int64")
    out["EFFECTIVE_BASELINE_POINTS"] = pd.to_numeric(out.get("EFFECTIVE_BASELINE_POINTS"), errors="coerce")
    out = out.dropna(subset=["YEAR", "PI", "EFFECTIVE_BASELINE_POINTS"])
    if out.empty:
        return pd.DataFrame()
    return out[["TEAMID", "YEAR", "PI", "EFFECTIVE_BASELINE_POINTS"]]


def _apply_velocity_from_snapshot(work: pd.DataFrame, *, years: Sequence[int]) -> pd.DataFrame:
    if work is None or work.empty:
        return work
    if "SWAG_POINTS" not in work.columns or "DERIVED_FTE_FEATURE" not in work.columns:
        return work
    if "TEAMID" not in work.columns or "ADO_YEAR" not in work.columns or "PI_NUM" not in work.columns:
        return work
    lookup = _velocity_snapshot_lookup(tuple(int(y) for y in years if y is not None and int(y) > 0))
    if lookup is None or lookup.empty:
        return work

    src = work.copy()
    src["_TEAM_KEY"] = src.get("TEAMID", "").astype(str).str.strip().str.upper().replace({"": "UNKNOWN"})
    src["_YEAR_KEY"] = pd.to_numeric(src.get("ADO_YEAR"), errors="coerce").astype("Int64")
    src["_PI_KEY"] = pd.to_numeric(src.get("PI_NUM"), errors="coerce").astype("Int64")

    lk = lookup.copy()
    lk["_TEAM_KEY"] = lk.get("TEAMID", "").astype(str).str.strip().str.upper().replace({"": "UNKNOWN"})
    lk["_YEAR_KEY"] = pd.to_numeric(lk.get("YEAR"), errors="coerce").astype("Int64")
    lk["_PI_KEY"] = pd.to_numeric(lk.get("PI"), errors="coerce").astype("Int64")

    merged = src.merge(
        lk[["_TEAM_KEY", "_YEAR_KEY", "_PI_KEY", "EFFECTIVE_BASELINE_POINTS"]],
        on=["_TEAM_KEY", "_YEAR_KEY", "_PI_KEY"],
        how="left",
    )
    swag = pd.to_numeric(merged.get("SWAG_POINTS"), errors="coerce")
    baseline = pd.to_numeric(merged.get("EFFECTIVE_BASELINE_POINTS"), errors="coerce")
    fallback = pd.to_numeric(merged.get("DERIVED_FTE_FEATURE"), errors="coerce").fillna(0.0)
    velocity = swag.divide(baseline.where(baseline > 0))

    is_msp = pd.to_numeric(merged.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int) > 0
    velocity = velocity.where(~is_msp, fallback)
    merged["DERIVED_FTE_FEATURE_VELOCITY"] = pd.to_numeric(velocity, errors="coerce").fillna(fallback)
    merged = merged.drop(columns=["_TEAM_KEY", "_YEAR_KEY", "_PI_KEY"], errors="ignore")
    return merged


@cache_data_portfolio(ttl=300, show_spinner=False)
def _object_columns(obj_name: str) -> Set[str]:
    try:
        df_cols = fetch_df(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s",
            (obj_name,),
        )
    except Exception:
        return set()
    if df_cols is None or df_cols.empty:
        return set()
    return {str(c).strip().upper() for c in df_cols["COLUMN_NAME"].tolist()}


def _agg_explorer_fte_by_group(
    *,
    years: Sequence[int],
    programs: Sequence[str] = (),
    teams: Sequence[str] = (),
    groups: Sequence[str] = (),
    pi_nums: Sequence[int] = (),
) -> Optional[pd.DataFrame]:
    if fetch_df is None:
        return None
    years_i = sorted({int(y) for y in years if y is not None and int(y) > 0})
    if not years_i:
        return pd.DataFrame()

    view_cols = _object_columns("VW_TCO_FEATURE_DEMAND")
    if not view_cols:
        return None

    year_expr = "TRY_CONVERT(INT, d.YEAR)"
    pi_expr = "TRY_CONVERT(INT, d.PI)"

    derived_col = None
    for cand in ("DERIVED_FTE_FEATURE", "DERIVED_FTE"):
        if cand in view_cols:
            derived_col = f"d.{cand}"
            break
    if derived_col is None:
        derived_col = "CAST(0 AS FLOAT)"

    if "IS_SWAG_READY" in view_cols:
        swag_ready_expr = "CASE WHEN COALESCE(d.IS_SWAG_READY, 0) <> 0 THEN 1 ELSE 0 END"
    elif "SWAG_POINTS" in view_cols:
        swag_ready_expr = "CASE WHEN COALESCE(d.SWAG_POINTS, 0) > 0 THEN 1 ELSE 0 END"
    else:
        swag_ready_expr = f"CASE WHEN COALESCE({derived_col}, 0) > 0 THEN 1 ELSE 0 END"

    if "PI_LABEL" in view_cols:
        pi_label_expr = (
            "COALESCE(NULLIF(LTRIM(RTRIM(d.PI_LABEL)), ''), "
            f"CAST({year_expr} AS VARCHAR(4)) + ' I' + CAST({pi_expr} AS VARCHAR(2)))"
        )
    else:
        pi_label_expr = f"CAST({year_expr} AS VARCHAR(4)) + ' I' + CAST({pi_expr} AS VARCHAR(2))"

    where: List[str] = [f"{year_expr} IN ({', '.join(['%s'] * len(years_i))})"]
    params: List[Any] = list(years_i)
    where.append("COALESCE(d.IN_SCOPE_FOR_ROADMAP, 0) = 1")

    if programs:
        ph = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(d.PROGRAMNAME) IN ({ph})")
        params.extend([str(p).upper() for p in programs])
    if teams:
        ph = ", ".join(["%s"] * len(teams))
        where.append(f"UPPER(d.TEAMNAME) IN ({ph})")
        params.extend([str(t).upper() for t in teams])
    if groups:
        ph = ", ".join(["%s"] * len(groups))
        where.append(f"UPPER(COALESCE(d.GROUPNAME, '(UNMAPPED APP GROUP)')) IN ({ph})")
        params.extend([str(g).upper() for g in groups])
    if pi_nums:
        pi_i = sorted({int(p) for p in pi_nums if p is not None and 1 <= int(p) <= 4})
        if pi_i:
            ph = ", ".join(["%s"] * len(pi_i))
            where.append(f"{pi_expr} IN ({ph})")
            params.extend(pi_i)

    sql = f"""
      WITH base AS (
        SELECT
          d.FEATURE_ID,
          {year_expr} AS ADO_YEAR,
          {pi_expr} AS PI_NUM,
          {pi_label_expr} AS PI_LABEL,
          CASE
            WHEN {year_expr} IS NOT NULL AND {pi_expr} IS NOT NULL THEN {year_expr} * 10 + {pi_expr}
            ELSE NULL
          END AS PI_ORDER,
          COALESCE(NULLIF(LTRIM(RTRIM(d.PROGRAMNAME)), ''), '(Unassigned)') AS PROGRAMNAME,
          COALESCE(NULLIF(LTRIM(RTRIM(d.TEAMNAME)), ''), '(Unassigned)') AS TEAMNAME,
          COALESCE(NULLIF(LTRIM(RTRIM(d.GROUPNAME)), ''), '(Unmapped Application)') AS GROUPNAME,
          TRY_CONVERT(FLOAT, {derived_col}) AS DERIVED_FTE,
          {swag_ready_expr} AS SWAG_READY,
          ROW_NUMBER() OVER (
            PARTITION BY d.FEATURE_ID, {year_expr}, {pi_expr}
            ORDER BY d.FEATURE_ID
          ) AS rn
        FROM {_fq('VW_TCO_FEATURE_DEMAND')} d
        WHERE {" AND ".join(where)}
      )
      SELECT
        ADO_YEAR,
        PI_NUM,
        MIN(PI_ORDER) AS PI_ORDER,
        MIN(PI_LABEL) AS PI_LABEL,
        PROGRAMNAME,
        TEAMNAME,
        GROUPNAME,
        SUM(COALESCE(DERIVED_FTE, 0)) AS DERIVED_FTE_SUM,
        SUM(COALESCE(SWAG_READY, 0)) AS SWAG_READY_FEATURES,
        COUNT(DISTINCT FEATURE_ID) AS FEATURE_COUNT
      FROM base
      WHERE rn = 1
      GROUP BY ADO_YEAR, PI_NUM, PROGRAMNAME, TEAMNAME, GROUPNAME
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        if df is None:
            return pd.DataFrame()
        return apply_display_scope_names(df)
    except Exception:
        return None


@cache_data_portfolio(ttl=180, show_spinner=False)
def load_explorer_feature_rows(
    *,
    years: Sequence[int],
    programs: Sequence[str] = (),
    teams: Sequence[str] = (),
    groups: Sequence[str] = (),
    pi_nums: Sequence[int] = (),
    cache_bust: int = 0,
    data_version: Optional[int] = None,
    include_ado_enrichment: bool = True,
    include_velocity_column: bool = True,
) -> pd.DataFrame:
    """
    Ground-truth (Explorer v2) feature-level rows for recon/debug.

    This is intentionally aligned with Explorer v2 (PO view) semantics and is the canonical
    feature-level demand dataset used across the app:
    - Roadmap inclusion: feature has PI and State != Removed
    - Demand driver: DERIVED_FTE (SWAG-derived) from DB view `VW_TCO_FEATURE_DEMAND`
    - MSP features have 0 Derived FTE (no internal demand contribution)
    """
    if fetch_df is None:
        return pd.DataFrame()
    _ = cache_bust, data_version

    years_i = sorted({int(y) for y in years if y is not None and int(y) > 0})
    if not years_i:
        return pd.DataFrame()

    view_cols = _object_columns("VW_TCO_FEATURE_DEMAND")
    if "DERIVED_FTE_FEATURE" in view_cols:
        derived_feature_expr = "TRY_CONVERT(FLOAT, d.DERIVED_FTE_FEATURE)"
    elif "DERIVED_FTE" in view_cols:
        derived_feature_expr = "TRY_CONVERT(FLOAT, d.DERIVED_FTE)"
    else:
        derived_feature_expr = "CAST(0 AS FLOAT)"
    use_snapshot_velocity = bool(include_velocity_column) and _env_flag(
        "TCO_EXPLORER_VELOCITY_FROM_SNAPSHOT",
        True,
    )
    if (not use_snapshot_velocity) and ("DERIVED_FTE_FEATURE_VELOCITY" in view_cols):
        velocity_feature_expr = "TRY_CONVERT(FLOAT, d.DERIVED_FTE_FEATURE_VELOCITY)"
    else:
        velocity_feature_expr = derived_feature_expr

    where: List[str] = [f"TRY_CONVERT(INT, d.YEAR) IN ({', '.join(['%s'] * len(years_i))})"]
    params: List[Any] = list(years_i)

    if programs:
        ph = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(d.PROGRAMNAME) IN ({ph})")
        params.extend([str(p).upper() for p in programs])
    if teams:
        ph = ", ".join(["%s"] * len(teams))
        where.append(f"UPPER(d.TEAMNAME) IN ({ph})")
        params.extend([str(t).upper() for t in teams])
    if groups:
        ph = ", ".join(["%s"] * len(groups))
        where.append(f"UPPER(COALESCE(d.GROUPNAME, '(UNMAPPED APP GROUP)')) IN ({ph})")
        params.extend([str(g).upper() for g in groups])
    if pi_nums:
        pi_i = sorted({int(p) for p in pi_nums if p is not None and 1 <= int(p) <= 4})
        if pi_i:
            ph = ", ".join(["%s"] * len(pi_i))
            where.append(f"TRY_CONVERT(INT, d.PI) IN ({ph})")
            params.extend(pi_i)

    include_ado = bool(include_ado_enrichment)
    if include_ado:
        ado_select = """
        /* Back-compat: APP_NAME_RAW/CHANGED_AT may not exist in VW_TCO_FEATURE_DEMAND; enrich from ADO_FEATURES. */
        af.APP_NAME_RAW AS APP_NAME_RAW,
        TRY_CONVERT(DATETIME2, af.CHANGED_AT) AS CHANGED_AT,
        af.PARENT_ID,
        af.EPIC_ID,
        af.EPIC_TITLE,
        af.EPIC_STATE,
        af.PROGRAM_RAW,
        af.TEAM_RAW,
        af.AREA_LEVEL1_RAW,
        af.AREA_LEVEL2_RAW,
        af.AREA_LEVEL3_RAW,
        af.AREA_LEVEL4_RAW,
        af.AREA_PATH_RAW,
        TRY_CONVERT(FLOAT, af.STORY_POINTS) AS STORY_POINTS,
        """
        ado_join = f"LEFT JOIN {_fq('ADO_FEATURES')} af ON af.FEATURE_ID = d.FEATURE_ID"
    else:
        ado_select = """
        CAST(NULL AS NVARCHAR(512)) AS APP_NAME_RAW,
        CAST(NULL AS DATETIME2) AS CHANGED_AT,
        CAST(NULL AS NVARCHAR(64)) AS PARENT_ID,
        CAST(NULL AS NVARCHAR(64)) AS EPIC_ID,
        CAST(NULL AS NVARCHAR(512)) AS EPIC_TITLE,
        CAST(NULL AS NVARCHAR(100)) AS EPIC_STATE,
        CAST(NULL AS NVARCHAR(512)) AS PROGRAM_RAW,
        CAST(NULL AS NVARCHAR(512)) AS TEAM_RAW,
        CAST(NULL AS NVARCHAR(512)) AS AREA_LEVEL1_RAW,
        CAST(NULL AS NVARCHAR(512)) AS AREA_LEVEL2_RAW,
        CAST(NULL AS NVARCHAR(512)) AS AREA_LEVEL3_RAW,
        CAST(NULL AS NVARCHAR(512)) AS AREA_LEVEL4_RAW,
        CAST(NULL AS NVARCHAR(1024)) AS AREA_PATH_RAW,
        CAST(NULL AS FLOAT) AS STORY_POINTS,
        """
        ado_join = ""

    sql = f"""
      SELECT
        d.FEATURE_ID,
        d.FEATURE_TITLE AS TITLE,
        d.FEATURE_STATE AS STATE,
        {ado_select}
        d.TEAMID,
        d.TEAMNAME,
        d.PROGRAMID,
        d.PROGRAMNAME,
        COALESCE(d.GROUPNAME, '(Unmapped Application)') AS GROUPNAME,
        TRY_CONVERT(INT, d.YEAR) AS ADO_YEAR,
        TRY_CONVERT(INT, d.PI) AS PI_NUM,
        d.PI_LABEL,
        TRY_CONVERT(FLOAT, d.SWAG_POINTS) AS SWAG_POINTS,
        TRY_CONVERT(INT, d.IS_SWAG_READY) AS IS_SWAG_READY,
        {derived_feature_expr} AS DERIVED_FTE,
        {derived_feature_expr} AS DERIVED_FTE_FEATURE,
        {velocity_feature_expr} AS DERIVED_FTE_FEATURE_VELOCITY,
        TRY_CONVERT(INT, d.IS_MSP_FEATURE) AS IS_MSP_FEATURE,
        TRY_CONVERT(INT, d.IN_SCOPE_FOR_ROADMAP) AS COUNTS_FOR_ROADMAP,
        d.MAPPING_STATUS,
        d.INVESTMENT_DIMENSION,
        d.FEATURE_STATE AS FEATURE_STATE
      FROM {_fq('VW_TCO_FEATURE_DEMAND')} d
      {ado_join}
      WHERE {" AND ".join(where)}
        AND COALESCE(d.IN_SCOPE_FOR_ROADMAP, 0) = 1
    """

    df = fetch_df(sql, tuple(params) if params else None)
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    work = apply_display_scope_names(work)
    work["PROGRAMNAME"] = work.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    work["TEAMNAME"] = work.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    work["GROUPNAME"] = work.get("GROUPNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    work["ADO_YEAR"] = pd.to_numeric(work.get("ADO_YEAR"), errors="coerce").astype("Int64")
    work["PI_NUM"] = pd.to_numeric(work.get("PI_NUM"), errors="coerce").astype("Int64")

    work["COUNTS_FOR_ROADMAP"] = pd.to_numeric(work.get("COUNTS_FOR_ROADMAP"), errors="coerce").fillna(0).astype(int) > 0
    work["IS_MSP_FEATURE"] = pd.to_numeric(work.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int) > 0
    if "SWAG_POINTS" in work.columns:
        work["SWAG_POINTS"] = pd.to_numeric(work.get("SWAG_POINTS"), errors="coerce")
    if "STORY_POINTS" in work.columns:
        work["STORY_POINTS"] = pd.to_numeric(work.get("STORY_POINTS"), errors="coerce")
    work["DERIVED_FTE"] = pd.to_numeric(work.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
    work["DERIVED_FTE_FEATURE"] = pd.to_numeric(
        work.get("DERIVED_FTE_FEATURE", work.get("DERIVED_FTE")), errors="coerce"
    ).fillna(work["DERIVED_FTE"])
    work["DERIVED_FTE_FEATURE_VELOCITY"] = pd.to_numeric(
        work.get("DERIVED_FTE_FEATURE_VELOCITY", work.get("DERIVED_FTE_FEATURE")), errors="coerce"
    ).fillna(work["DERIVED_FTE_FEATURE"])
    if use_snapshot_velocity:
        try:
            work = _apply_velocity_from_snapshot(work, years=years_i)
        except Exception:
            pass
    if "IS_SWAG_READY" in work.columns:
        work["SWAG_READY"] = pd.to_numeric(work.get("IS_SWAG_READY"), errors="coerce").fillna(0).astype(int) > 0
    elif "SWAG_POINTS" in work.columns:
        work["SWAG_READY"] = work["SWAG_POINTS"].fillna(0.0) > 0
    else:
        work["SWAG_READY"] = work["DERIVED_FTE"] > 0
    work = work.drop_duplicates(subset=["FEATURE_ID", "ADO_YEAR", "PI_NUM"], keep="first").copy()
    work["PI_LABEL"] = work.get("PI_LABEL", "").astype(str).str.strip()
    work["PI_LABEL"] = work["PI_LABEL"].where(
        work["PI_LABEL"].ne(""),
        work.apply(
            lambda r: f"{int(r['ADO_YEAR'])} I{int(r['PI_NUM'])}"
            if pd.notna(r["ADO_YEAR"]) and pd.notna(r["PI_NUM"])
            else "",
            axis=1,
        ),
    )
    work["PI_ORDER"] = work.apply(
        lambda r: int(r["ADO_YEAR"]) * 10 + int(r["PI_NUM"])
        if pd.notna(r["ADO_YEAR"]) and pd.notna(r["PI_NUM"])
        else pd.NA,
        axis=1,
    )

    out = pd.DataFrame(
        {
            "FEATURE_ID": work.get("FEATURE_ID"),
            "FEATURE_TITLE": work.get("TITLE"),
            "ADO_YEAR": work.get("ADO_YEAR"),
            "PI_NUM": work.get("PI_NUM"),
            "PI_ORDER": work.get("PI_ORDER"),
            "PI_LABEL": work.get("PI_LABEL"),
            "PROGRAMNAME": work.get("PROGRAMNAME"),
            "TEAMNAME": work.get("TEAMNAME"),
            "GROUPNAME": work.get("GROUPNAME"),
            "PROGRAM_RAW": work.get("PROGRAM_RAW"),
            "TEAM_RAW": work.get("TEAM_RAW"),
            "AREA_LEVEL1_RAW": work.get("AREA_LEVEL1_RAW"),
            "AREA_LEVEL2_RAW": work.get("AREA_LEVEL2_RAW"),
            "AREA_LEVEL3_RAW": work.get("AREA_LEVEL3_RAW"),
            "AREA_LEVEL4_RAW": work.get("AREA_LEVEL4_RAW"),
            "AREA_PATH_RAW": work.get("AREA_PATH_RAW"),
            "PARENT_ID": work.get("PARENT_ID"),
            "EPIC_ID": work.get("EPIC_ID"),
            "EPIC_TITLE": work.get("EPIC_TITLE"),
            "EPIC_STATE": work.get("EPIC_STATE"),
            "APP_NAME_RAW": work.get("APP_NAME_RAW"),
            "CHANGED_AT": work.get("CHANGED_AT"),
            "SWAG_POINTS": work.get("SWAG_POINTS"),
            "DERIVED_FTE": work.get("DERIVED_FTE_FEATURE", work.get("DERIVED_FTE")),
            "DERIVED_FTE_FEATURE": work.get("DERIVED_FTE_FEATURE", work.get("DERIVED_FTE")),
            "DERIVED_FTE_FEATURE_VELOCITY": work.get("DERIVED_FTE_FEATURE_VELOCITY", work.get("DERIVED_FTE_FEATURE")),
            "DERIVED_FTE_EXPLORER": work.get("DERIVED_FTE_FEATURE", work.get("DERIVED_FTE")),
            "SWAG_READY": work.get("SWAG_READY"),
            "COUNTS_FOR_ROADMAP": work.get("COUNTS_FOR_ROADMAP"),
            "IS_MSP_FEATURE": work.get("IS_MSP_FEATURE"),
            "MAPPING_STATUS": work.get("MAPPING_STATUS"),
            "STATE": work.get("FEATURE_STATE"),
            "INVESTMENT_DIMENSION": work.get("INVESTMENT_DIMENSION"),
        }
    )
    out["DERIVED_FTE_FEATURE"] = pd.to_numeric(out.get("DERIVED_FTE_FEATURE"), errors="coerce").fillna(0.0)
    out["DERIVED_FTE"] = out["DERIVED_FTE_FEATURE"]
    out["DERIVED_FTE_FEATURE_VELOCITY"] = pd.to_numeric(
        out.get("DERIVED_FTE_FEATURE_VELOCITY"), errors="coerce"
    ).fillna(out["DERIVED_FTE_FEATURE"])
    out["DERIVED_FTE_EXPLORER"] = out["DERIVED_FTE_FEATURE"]
    out["SWAG_READY"] = out.get("SWAG_READY", False).fillna(False).astype(bool)
    if "SWAG_POINTS" in out.columns:
        out["SWAG_POINTS"] = pd.to_numeric(out.get("SWAG_POINTS"), errors="coerce")
    out["CHANGED_AT"] = pd.to_datetime(out.get("CHANGED_AT"), errors="coerce")
    return apply_display_scope_names(out)


@cache_data_portfolio(ttl=180, show_spinner=False)
def load_velocity_fidelity_metrics(
    *,
    years: Sequence[int],
    programs: Sequence[str] = (),
    teams: Sequence[str] = (),
    groups: Sequence[str] = (),
    pi_nums: Sequence[int] = (),
    cache_bust: int = 0,
    data_version: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Compare velocity demand from view column vs snapshot-based velocity calculation.

    Returns a dict with summary metrics and a by-team breakdown dataframe.
    """
    _ = cache_bust, data_version
    years_i = sorted({int(y) for y in years if y is not None and int(y) > 0})
    if not years_i:
        return {
            "status": "empty",
            "message": "No valid year in scope.",
            "total_rows": 0,
            "aligned_rows": 0,
            "mismatched_rows": 0,
            "aligned_pct": 0.0,
            "delta_total": 0.0,
            "view_total": 0.0,
            "snapshot_total": 0.0,
            "by_team": pd.DataFrame(),
        }

    programs_q = _resolve_program_scope_tokens(programs)
    teams_q = _resolve_team_scope_tokens(teams)
    groups_q = [str(v).strip() for v in (groups or []) if str(v).strip()]
    pi_q = sorted({int(p) for p in (pi_nums or []) if p is not None and 1 <= int(p) <= 4})

    try:
        snap_df = load_explorer_feature_rows(
            years=years_i,
            programs=tuple(programs_q),
            teams=tuple(teams_q),
            groups=tuple(groups_q),
            pi_nums=tuple(pi_q),
            cache_bust=int(cache_bust or 0),
            data_version=data_version,
            include_ado_enrichment=False,
            include_velocity_column=True,
        )
    except Exception as exc:
        return {
            "status": "timeout" if _is_timeout_error(exc) else "unavailable",
            "message": str(exc),
            "total_rows": 0,
            "aligned_rows": 0,
            "mismatched_rows": 0,
            "aligned_pct": 0.0,
            "delta_total": 0.0,
            "view_total": 0.0,
            "snapshot_total": 0.0,
            "by_team": pd.DataFrame(),
        }
    try:
        view_df = load_explorer_feature_rows(
            years=years_i,
            programs=tuple(programs_q),
            teams=tuple(teams_q),
            groups=tuple(groups_q),
            pi_nums=tuple(pi_q),
            cache_bust=int(cache_bust or 0),
            data_version=data_version,
            include_ado_enrichment=False,
            include_velocity_column=False,
        )
    except Exception as exc:
        return {
            "status": "timeout" if _is_timeout_error(exc) else "unavailable",
            "message": str(exc),
            "total_rows": 0,
            "aligned_rows": 0,
            "mismatched_rows": 0,
            "aligned_pct": 0.0,
            "delta_total": 0.0,
            "view_total": 0.0,
            "snapshot_total": 0.0,
            "by_team": pd.DataFrame(),
        }

    if snap_df is None or snap_df.empty or view_df is None or view_df.empty:
        return {
            "status": "empty",
            "message": "No feature rows in scope for fidelity comparison.",
            "total_rows": 0,
            "aligned_rows": 0,
            "mismatched_rows": 0,
            "aligned_pct": 0.0,
            "delta_total": 0.0,
            "view_total": 0.0,
            "snapshot_total": 0.0,
            "by_team": pd.DataFrame(),
        }

    key_cols = ["FEATURE_ID", "ADO_YEAR", "PI_NUM"]
    left = snap_df.copy()
    right = view_df.copy()
    for c in key_cols:
        left[c] = left.get(c).astype(str).str.strip()
        right[c] = right.get(c).astype(str).str.strip()
    left = left.rename(
        columns={
            "DERIVED_FTE_FEATURE_VELOCITY": "VELOCITY_FTE_SNAPSHOT",
        }
    )
    right = right.rename(
        columns={
            "DERIVED_FTE_FEATURE_VELOCITY": "VELOCITY_FTE_VIEW",
        }
    )
    merged = left.merge(
        right[key_cols + ["VELOCITY_FTE_VIEW"]],
        on=key_cols,
        how="inner",
    )
    if merged.empty:
        return {
            "status": "empty",
            "message": "No overlapping feature rows between snapshot/view velocity datasets.",
            "total_rows": 0,
            "aligned_rows": 0,
            "mismatched_rows": 0,
            "aligned_pct": 0.0,
            "delta_total": 0.0,
            "view_total": 0.0,
            "snapshot_total": 0.0,
            "by_team": pd.DataFrame(),
        }

    merged["VELOCITY_FTE_SNAPSHOT"] = pd.to_numeric(merged.get("VELOCITY_FTE_SNAPSHOT"), errors="coerce").fillna(0.0)
    merged["VELOCITY_FTE_VIEW"] = pd.to_numeric(merged.get("VELOCITY_FTE_VIEW"), errors="coerce").fillna(0.0)
    merged["ABS_DIFF"] = (merged["VELOCITY_FTE_VIEW"] - merged["VELOCITY_FTE_SNAPSHOT"]).abs()
    merged["ALIGNED"] = merged["ABS_DIFF"] < 1e-6

    total_rows = int(len(merged.index))
    aligned_rows = int(merged["ALIGNED"].sum())
    mismatched_rows = int(total_rows - aligned_rows)
    aligned_pct = (100.0 * aligned_rows / total_rows) if total_rows else 0.0
    delta_total = float((merged["VELOCITY_FTE_VIEW"] - merged["VELOCITY_FTE_SNAPSHOT"]).sum())
    view_total = float(merged["VELOCITY_FTE_VIEW"].sum())
    snapshot_total = float(merged["VELOCITY_FTE_SNAPSHOT"].sum())

    by_team = (
        merged.groupby(["PROGRAMNAME", "TEAMNAME"], dropna=False)
        .agg(
            TOTAL_ROWS=("FEATURE_ID", "count"),
            MISMATCHED_ROWS=("ALIGNED", lambda s: int((~s.astype(bool)).sum())),
            ALIGNED_PCT=("ALIGNED", lambda s: float(100.0 * s.astype(bool).mean()) if len(s.index) else 0.0),
            DELTA_TOTAL=("VELOCITY_FTE_VIEW", "sum"),
            SNAPSHOT_TOTAL=("VELOCITY_FTE_SNAPSHOT", "sum"),
        )
        .reset_index()
    )
    by_team["DELTA_TOTAL"] = pd.to_numeric(by_team.get("DELTA_TOTAL"), errors="coerce").fillna(0.0) - pd.to_numeric(
        by_team.get("SNAPSHOT_TOTAL"), errors="coerce"
    ).fillna(0.0)
    by_team["PROGRAMNAME"] = by_team.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    by_team["TEAMNAME"] = by_team.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    by_team = by_team.drop(columns=["SNAPSHOT_TOTAL"], errors="ignore")
    by_team = by_team.sort_values(["MISMATCHED_ROWS", "ALIGNED_PCT"], ascending=[False, True], kind="mergesort")

    return {
        "status": "ok",
        "message": "",
        "total_rows": total_rows,
        "aligned_rows": aligned_rows,
        "mismatched_rows": mismatched_rows,
        "aligned_pct": aligned_pct,
        "delta_total": delta_total,
        "view_total": view_total,
        "snapshot_total": snapshot_total,
        "by_team": by_team,
    }


@cache_data_portfolio(ttl=180, show_spinner=False)
def load_explorer_fte_by_group(
    *,
    years: Sequence[int],
    programs: Sequence[str] = (),
    teams: Sequence[str] = (),
    groups: Sequence[str] = (),
    pi_nums: Sequence[int] = (),
    data_version: Optional[int] = None,
    allow_feature_fallback: bool = True,
) -> pd.DataFrame:
    """Aggregate Explorer v2 feature-level rows to demand per (Year, PI, Program, Team, App Group)."""
    agg = _agg_explorer_fte_by_group(
        years=years,
        programs=programs,
        teams=teams,
        groups=groups,
        pi_nums=pi_nums,
    )
    if agg is None:
        if not bool(allow_feature_fallback):
            return pd.DataFrame()
        feats = load_explorer_feature_rows(
            years=years,
            programs=programs,
            teams=teams,
            groups=groups,
            pi_nums=pi_nums,
            data_version=data_version,
            include_ado_enrichment=False,
            include_velocity_column=False,
        )
        if feats is None or feats.empty:
            return pd.DataFrame()
        gcols = ["ADO_YEAR", "PI_NUM", "PI_ORDER", "PI_LABEL", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"]
        agg = feats.groupby(gcols, dropna=False).agg(
            DERIVED_FTE_SUM=("DERIVED_FTE_EXPLORER", "sum"),
            SWAG_READY_FEATURES=("SWAG_READY", "sum"),
            FEATURE_COUNT=("FEATURE_ID", pd.Series.nunique),
        ).reset_index()
    if agg is None or agg.empty:
        return pd.DataFrame()
    agg = apply_display_scope_names(agg)
    for c in ["DERIVED_FTE_SUM", "SWAG_READY_FEATURES"]:
        agg[c] = pd.to_numeric(agg.get(c), errors="coerce").fillna(0.0)
    agg["FEATURE_COUNT"] = pd.to_numeric(agg.get("FEATURE_COUNT"), errors="coerce").fillna(0).astype(int)
    agg["SWAG_READY_FEATURES"] = pd.to_numeric(agg.get("SWAG_READY_FEATURES"), errors="coerce").fillna(0).astype(int)
    # Back-compat for earlier callers: "FTE_EXPLORER" = derived FTE sum.
    agg["FTE_EXPLORER"] = agg["DERIVED_FTE_SUM"]
    return agg


def _norm_token_set(values: Sequence[str] | Sequence[Any]) -> set[str]:
    return {str(v).strip().upper() for v in (values or []) if str(v).strip()}


@cache_data_portfolio(ttl=300, show_spinner=False)
def _group_scope_rows() -> pd.DataFrame:
    if fetch_df is None:
        return pd.DataFrame(columns=["GROUPID", "GROUPNAME", "TEAMID"])
    try:
        df = fetch_df(
            f"""
            SELECT GROUPID, GROUPNAME, TEAMID
            FROM {_fq('APPLICATION_GROUPS')}
            """,
            None,
        )
        if df is None:
            return pd.DataFrame(columns=["GROUPID", "GROUPNAME", "TEAMID"])
        return df
    except Exception:
        return pd.DataFrame(columns=["GROUPID", "GROUPNAME", "TEAMID"])


def _to_version_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        if pd.isna(value):
            return 0
    except Exception:
        pass
    try:
        out = int(value)
        if out < 0:
            return 0
        return min(out, _SQL_INT32_MAX)
    except Exception:
        pass
    try:
        ts = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(ts):
            return 0
        out = int(ts.timestamp())
        if out < 0:
            return 0
        return min(out, _SQL_INT32_MAX)
    except Exception:
        return 0


@cache_data_portfolio(ttl=120, show_spinner=False)
def _velocity_source_version_token() -> int:
    if fetch_df is None:
        return 0

    tokens: list[int] = []
    try:
        if _object_columns("ADO_SYNC_STATE"):
            df = fetch_df(
                f"""
                SELECT MAX(COALESCE(UPDATED_AT, LAST_RUN_AT, LAST_DELTA_CHANGED_AT, LAST_FULL_SYNC_AT)) AS TS
                FROM {_fq('ADO_SYNC_STATE')}
                """,
                None,
            )
            if df is not None and not df.empty:
                tokens.append(_to_version_int(df.iloc[0].get("TS")))
    except Exception:
        pass

    try:
        if _object_columns("ADO_FEATURES"):
            df = fetch_df(
                f"""
                SELECT MAX(COALESCE(CHANGED_AT, LAST_SEEN_AT, CREATED_AT)) AS TS
                FROM {_fq('ADO_FEATURES')}
                """,
                None,
            )
            if df is not None and not df.empty:
                tokens.append(_to_version_int(df.iloc[0].get("TS")))
    except Exception:
        pass

    try:
        if _object_columns("ADO_WORKITEM_LOOKUP"):
            df = fetch_df(
                f"""
                SELECT MAX(COALESCE(UPDATED_AT, CHANGED_DATE)) AS TS
                FROM {_fq('ADO_WORKITEM_LOOKUP')}
                """,
                None,
            )
            if df is not None and not df.empty:
                tokens.append(_to_version_int(df.iloc[0].get("TS")))
    except Exception:
        pass

    tokens = [int(t) for t in tokens if int(t or 0) > 0]
    return max(tokens) if tokens else 0


def _load_velocity_baseline_window_uncached(
    *,
    year: int,
    include_prior_year: bool = True,
    reference_year_only: bool = False,
    data_version: Optional[int] = None,
) -> pd.DataFrame:
    if fetch_df is None:
        return pd.DataFrame()
    try:
        year_i = int(year)
    except Exception:
        return pd.DataFrame()
    if year_i <= 0:
        return pd.DataFrame()
    target_ver = _to_version_int(_velocity_source_version_token() or 0)
    if target_ver <= 0:
        target_ver = _to_version_int(data_version or 0)

    year_floor = year_i if bool(reference_year_only) else (max(0, year_i - 1) if bool(include_prior_year) else year_i)
    base_cols = [
        "TEAMID",
        "PROGRAMID",
        "YEAR",
        "PI",
        "PI_LABEL",
        "PROGRAMNAME",
        "TEAMNAME",
        "EFFECTIVE_BASELINE_POINTS",
        "BASELINE_SOURCE",
        "BASELINE_CHANGED_FLAG",
        "BASELINE_PREV",
        "PI_POINTS_DONE",
    ]

    snapshot_ready = False
    if ensure_tco_team_velocity_snapshot_table is not None:
        try:
            ensure_tco_team_velocity_snapshot_table()
            snapshot_ready = True
        except Exception:
            snapshot_ready = False

    def _snapshot_window_meta() -> tuple[int, int, int, int]:
        try:
            meta = fetch_df(
                f"""
                SELECT
                  COUNT(1) AS ROWS_N,
                  MAX(COALESCE(DATA_VERSION, 0)) AS MAX_DATA_VERSION,
                  MIN(COALESCE(DATA_VERSION, 0)) AS MIN_DATA_VERSION,
                  SUM(
                    CASE
                      WHEN %s > 0 AND COALESCE(DATA_VERSION, 0) < %s THEN 1
                      ELSE 0
                    END
                  ) AS ROWS_BELOW_TARGET
                FROM {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')}
                WHERE TRY_CONVERT(INT, YEAR) <= %s
                  AND TRY_CONVERT(INT, YEAR) >= %s
                """,
                (target_ver, target_ver, year_i, year_floor),
            )
            rows_n = int(meta.iloc[0].get("ROWS_N") or 0) if meta is not None and not meta.empty else 0
            max_ver = int(meta.iloc[0].get("MAX_DATA_VERSION") or 0) if meta is not None and not meta.empty else 0
            min_ver = int(meta.iloc[0].get("MIN_DATA_VERSION") or 0) if meta is not None and not meta.empty else 0
            rows_below_target = int(meta.iloc[0].get("ROWS_BELOW_TARGET") or 0) if meta is not None and not meta.empty else 0
            return rows_n, max_ver, min_ver, rows_below_target
        except Exception:
            return 0, 0, 0, 0

    def _is_snapshot_stale(rows_n: int, max_ver: int, min_ver: int, rows_below_target: int) -> bool:
        stale_now = rows_n <= 0
        if target_ver > 0:
            # Using only MAX(DATA_VERSION) can hide stale slices when a subset of
            # rows carries an oversized token (for example clamped int32 values).
            # Require the full window to be at-or-above the target version.
            stale_now = stale_now or (rows_below_target > 0) or (max_ver < target_ver)
        elif min_ver <= 0:
            stale_now = stale_now or (rows_n > 0 and max_ver <= 0)
        return stale_now

    snapshot_stale = False
    snapshot_rows_n = 0
    snapshot_max_ver = 0
    snapshot_min_ver = 0
    snapshot_rows_below_target = 0
    snapshot_refresh_attempted = False
    snapshot_refresh_ok = False
    auto_refresh_snapshot = _env_flag("TCO_VELOCITY_SNAPSHOT_AUTO_REFRESH", False)
    allow_stale_snapshot = _env_flag("TCO_VELOCITY_ALLOW_STALE_SNAPSHOT", True)
    if snapshot_ready:
        snapshot_rows_n, snapshot_max_ver, snapshot_min_ver, snapshot_rows_below_target = _snapshot_window_meta()
        snapshot_stale = _is_snapshot_stale(
            snapshot_rows_n,
            snapshot_max_ver,
            snapshot_min_ver,
            snapshot_rows_below_target,
        )
        # Availability-first policy:
        # only trigger expensive inline refresh when the snapshot window is empty.
        # If rows already exist (even stale), keep fail-open behavior and avoid
        # blocking interactive pages with long INSERT...SELECT refreshes.
        if (
            snapshot_stale
            and auto_refresh_snapshot
            and snapshot_rows_n <= 0
            and refresh_tco_team_velocity_snapshot is not None
        ):
            snapshot_refresh_attempted = True
            try:
                refresh_tco_team_velocity_snapshot(
                    data_version=target_ver or None,
                    year=year_i,
                    include_prior_year=include_prior_year,
                    reference_year_only=reference_year_only,
                )
                snapshot_refresh_ok = True
            except Exception:
                snapshot_refresh_ok = False

    snapshot_has_rows = snapshot_rows_n > 0
    snapshot_usable = bool(snapshot_ready and snapshot_has_rows)
    if snapshot_usable and snapshot_stale:
        if snapshot_refresh_attempted and snapshot_refresh_ok:
            rows_n2, max_ver2, min_ver2, rows_below_target2 = _snapshot_window_meta()
            snapshot_has_rows = rows_n2 > 0
            snapshot_usable = not _is_snapshot_stale(rows_n2, max_ver2, min_ver2, rows_below_target2)
            if (not snapshot_usable) and allow_stale_snapshot and snapshot_has_rows:
                # Availability guardrail: stale snapshot is preferable to expensive view fallback.
                snapshot_usable = True
        else:
            # Keep a stale snapshot as a fail-open source to avoid page stalls/timeouts.
            snapshot_usable = bool(allow_stale_snapshot and snapshot_has_rows)

    if snapshot_usable:
        try:
            df_snapshot = fetch_df(
                f"""
                SELECT
                  COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMID)), ''), 'UNKNOWN') AS TEAMID,
                  NULLIF(LTRIM(RTRIM(v.PROGRAMID)), '') AS PROGRAMID,
                  TRY_CONVERT(INT, v.YEAR) AS YEAR,
                  TRY_CONVERT(INT, v.PI) AS PI,
                  COALESCE(
                    NULLIF(LTRIM(RTRIM(v.PI_LABEL)), ''),
                    CAST(TRY_CONVERT(INT, v.YEAR) AS VARCHAR(4)) + ' I' + CAST(TRY_CONVERT(INT, v.PI) AS VARCHAR(10))
                  ) AS PI_LABEL,
                  COALESCE(NULLIF(LTRIM(RTRIM(v.PROGRAMNAME)), ''), '(Unassigned)') AS PROGRAMNAME,
                  COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMNAME)), ''), '(Unassigned)') AS TEAMNAME,
                  TRY_CONVERT(FLOAT, v.EFFECTIVE_BASELINE_POINTS) AS EFFECTIVE_BASELINE_POINTS,
                  UPPER(LTRIM(RTRIM(COALESCE(v.BASELINE_SOURCE, 'GLOBAL')))) AS BASELINE_SOURCE,
                  TRY_CONVERT(INT, COALESCE(v.BASELINE_CHANGED_FLAG, 0)) AS BASELINE_CHANGED_FLAG,
                  TRY_CONVERT(FLOAT, v.BASELINE_PREV) AS BASELINE_PREV,
                  TRY_CONVERT(FLOAT, v.PI_POINTS_DONE) AS PI_POINTS_DONE
                FROM {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')} v
                WHERE TRY_CONVERT(INT, v.YEAR) <= %s
                  AND TRY_CONVERT(INT, v.YEAR) >= %s
                ORDER BY TRY_CONVERT(INT, v.YEAR), TRY_CONVERT(INT, v.PI), PROGRAMNAME, TEAMNAME
                """,
                (year_i, year_floor),
            )
            if df_snapshot is not None and not df_snapshot.empty:
                return df_snapshot.reindex(columns=base_cols)
        except Exception:
            pass

    # Fail open on the canonical view to preserve correctness when snapshot is missing/unavailable.
    try:
        df_view = fetch_df(
            f"""
            SELECT
              COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMID)), ''), 'UNKNOWN') AS TEAMID,
              NULLIF(LTRIM(RTRIM(COALESCE(v.PROGRAMID, t.PROGRAMID))), '') AS PROGRAMID,
              TRY_CONVERT(INT, v.YEAR) AS YEAR,
              TRY_CONVERT(INT, v.PI) AS PI,
              COALESCE(
                NULLIF(LTRIM(RTRIM(v.PI_LABEL)), ''),
                CAST(TRY_CONVERT(INT, v.YEAR) AS VARCHAR(4)) + ' I' + CAST(TRY_CONVERT(INT, v.PI) AS VARCHAR(10))
              ) AS PI_LABEL,
              COALESCE(NULLIF(LTRIM(RTRIM(v.PROGRAMNAME)), ''), NULLIF(LTRIM(RTRIM(p.PROGRAMNAME)), ''), '(Unassigned)') AS PROGRAMNAME,
              COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMNAME)), ''), NULLIF(LTRIM(RTRIM(t.TEAMNAME)), ''), '(Unassigned)') AS TEAMNAME,
              TRY_CONVERT(FLOAT, v.EFFECTIVE_BASELINE_POINTS) AS EFFECTIVE_BASELINE_POINTS,
              UPPER(LTRIM(RTRIM(COALESCE(v.BASELINE_SOURCE, 'GLOBAL')))) AS BASELINE_SOURCE,
              TRY_CONVERT(INT, COALESCE(v.BASELINE_CHANGED_FLAG, 0)) AS BASELINE_CHANGED_FLAG,
              TRY_CONVERT(FLOAT, v.BASELINE_PREV) AS BASELINE_PREV,
              TRY_CONVERT(FLOAT, v.PI_POINTS_DONE) AS PI_POINTS_DONE
            FROM {_fq('VW_TCO_TEAM_VELOCITY_BASELINE')} v
            LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = v.TEAMID
            LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = COALESCE(v.PROGRAMID, t.PROGRAMID)
            WHERE TRY_CONVERT(INT, v.YEAR) <= %s
              AND TRY_CONVERT(INT, v.YEAR) >= %s
            ORDER BY TRY_CONVERT(INT, v.YEAR), TRY_CONVERT(INT, v.PI), PROGRAMNAME, TEAMNAME
            """,
            (year_i, year_floor),
        )
        if df_view is None:
            return pd.DataFrame(columns=base_cols)
        return df_view.reindex(columns=base_cols)
    except Exception:
        return pd.DataFrame(columns=base_cols)


@cache_data_portfolio(ttl=120, show_spinner=False)
def _load_velocity_baseline_window(
    *,
    year: int,
    include_prior_year: bool = True,
    reference_year_only: bool = False,
    data_version: Optional[int] = None,
) -> pd.DataFrame:
    return _load_velocity_baseline_window_uncached(
        year=year,
        include_prior_year=include_prior_year,
        reference_year_only=reference_year_only,
        data_version=data_version,
    )


def _apply_velocity_scope_filters(
    *,
    df: pd.DataFrame,
    programs: Sequence[str],
    teams: Sequence[str],
    groups: Sequence[str],
    program_ids: Sequence[str],
    team_ids: Sequence[str],
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    w = df.copy()

    prog_ids = _norm_token_set(program_ids)
    team_id_set = _norm_token_set(team_ids)
    prog_names = _norm_token_set(programs)
    team_names = _norm_token_set(teams)
    group_names = _norm_token_set(groups)

    if prog_ids and "PROGRAMID" in w.columns:
        pid = w["PROGRAMID"].fillna("").astype(str).str.strip().str.upper()
        w = w[pid.isin(prog_ids)].copy()
    if team_id_set and "TEAMID" in w.columns:
        tid = w["TEAMID"].fillna("").astype(str).str.strip().str.upper()
        w = w[tid.isin(team_id_set)].copy()

    if prog_names and "PROGRAMNAME" in w.columns:
        pn = w["PROGRAMNAME"].fillna("").astype(str).str.strip().str.upper()
        w = w[pn.isin(prog_names)].copy()
    if team_names and "TEAMNAME" in w.columns:
        tn = w["TEAMNAME"].fillna("").astype(str).str.strip().str.upper()
        w = w[tn.isin(team_names)].copy()

    if group_names:
        g = _group_scope_rows()
        if g is None or g.empty:
            return w.iloc[0:0].copy()
        gw = g.copy()
        gw["GROUPID"] = gw.get("GROUPID", "").fillna("").astype(str).str.strip().str.upper()
        gw["GROUPNAME"] = gw.get("GROUPNAME", "").fillna("").astype(str).str.strip().str.upper()
        gw["TEAMID"] = gw.get("TEAMID", "").fillna("").astype(str).str.strip().str.upper()
        gm = gw[gw["GROUPID"].isin(group_names) | gw["GROUPNAME"].isin(group_names)].copy()
        team_from_groups = {str(v).strip().upper() for v in gm.get("TEAMID", pd.Series(dtype=str)).tolist() if str(v).strip()}
        if not team_from_groups:
            return w.iloc[0:0].copy()
        if "TEAMID" in w.columns:
            tid = w["TEAMID"].fillna("").astype(str).str.strip().str.upper()
            w = w[tid.isin(team_from_groups)].copy()
        else:
            return w.iloc[0:0].copy()
    return w


@cache_data_portfolio(ttl=120, show_spinner=False)
def load_velocity_baseline(
    *,
    year: int,
    programs: Sequence[str] = (),
    teams: Sequence[str] = (),
    groups: Sequence[str] = (),
    data_version: Optional[int] = None,
    include_prior_year: bool = True,
    program_ids: Sequence[str] = (),
    team_ids: Sequence[str] = (),
    reference_year_only: bool = False,
) -> pd.DataFrame:
    """Load recent Team/Program velocity baseline rows for Welcome/Insights UI."""
    cols = [
        "YEAR",
        "PI",
        "PI_LABEL",
        "PROGRAMNAME",
        "TEAMNAME",
        "EFFECTIVE_BASELINE_POINTS",
        "BASELINE_SOURCE",
        "BASELINE_CHANGED_FLAG",
        "BASELINE_PREV",
        "PI_POINTS_DONE",
    ]
    try:
        year_i = int(year)
    except Exception:
        return pd.DataFrame(columns=cols)
    if year_i <= 0:
        return pd.DataFrame(columns=cols)

    base = _load_velocity_baseline_window(
        year=year_i,
        include_prior_year=include_prior_year,
        reference_year_only=reference_year_only,
        data_version=data_version,
    )
    if base is None or base.empty:
        return pd.DataFrame(columns=cols)

    scoped = _apply_velocity_scope_filters(
        df=base,
        programs=programs,
        teams=teams,
        groups=groups,
        program_ids=program_ids,
        team_ids=team_ids,
    )
    if scoped is None or scoped.empty:
        return pd.DataFrame(columns=cols)

    scoped["YEAR"] = pd.to_numeric(scoped.get("YEAR"), errors="coerce")
    scoped["PI"] = pd.to_numeric(scoped.get("PI"), errors="coerce")
    scoped = scoped[scoped["YEAR"].notna() & scoped["PI"].notna()].copy()
    if scoped.empty:
        return pd.DataFrame(columns=cols)

    scoped["PI_ORDER"] = (scoped["YEAR"].astype(int) * 100) + scoped["PI"].astype(int)
    # Keep the full requested year window. Truncating here can hide older GLOBAL rows
    # needed for deterministic fallback at PI/year boundaries (e.g., selected year PI1).
    scoped = scoped.sort_values(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"])

    out = apply_display_scope_names(scoped)
    for col in ("YEAR", "PI", "BASELINE_CHANGED_FLAG"):
        out[col] = pd.to_numeric(out.get(col), errors="coerce").fillna(0).astype(int)
    for col in ("EFFECTIVE_BASELINE_POINTS", "BASELINE_PREV", "PI_POINTS_DONE"):
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    out["TEAMNAME"] = out.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    out["PI_LABEL"] = out.get("PI_LABEL", "").fillna("").astype(str).str.strip()
    out["BASELINE_SOURCE"] = out.get("BASELINE_SOURCE", "GLOBAL").fillna("GLOBAL").astype(str).str.upper().str.strip()
    return out.reindex(columns=cols).copy()


# Back-compat friendly alias for callers: "derived FTE" is the Explorer v2 derived FTE aggregation.
load_derived_fte_by_bucket = load_explorer_fte_by_group
