from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from utils.ado import normalize_pi_key

from core.cache_utils import cache_data_portfolio
from core.name_resolution import apply_display_scope_names

from db import (
    _fq,
    fetch_apptio_actuals_by_program,
    fetch_apptio_actuals_by_program_breakdown,
)
from core.db_session import db_fetch_df as fetch_df


def _json_in(where: List[str], params: List, col: str, values):
    if not values:
        return
    vals = [v for v in values if v is not None and str(v).strip() != ""]
    if not vals:
        return
    placeholders = ", ".join(["%s"] * len(vals))
    where.append(f"{col} IN ({placeholders})")
    params.extend(vals)


def _json_in_pi(where: List[str], params: List, col: str, values):
    if not values:
        return
    digits = []
    for v in values:
        if v is None:
            continue
        s = "".join(ch for ch in str(v) if ch.isdigit())
        if s:
            digits.append(s)
    if not digits:
        return
    placeholders = ", ".join(["%s"] * len(digits))
    where.append(f"{col} IN ({placeholders})")
    params.extend(digits)


def _program_ids_for_names(names: List[str]) -> List[str]:
    if not names:
        return []
    placeholders = ", ".join(["%s"] * len(names))
    df = fetch_df(f"SELECT PROGRAMID FROM PROGRAMS WHERE PROGRAMNAME IN ({placeholders})", tuple(names))
    if df is None or df.empty:
        return []
    return df["PROGRAMID"].dropna().astype(str).tolist()


@cache_data_portfolio(ttl=600, show_spinner=False)
def dominant_iteration_root(year: int) -> Optional[str]:
    """Return the dominant iteration root (first path segment) in `dbo.ADO_FEATURES` for a year.

    Root is extracted as the first segment before '\\' from `ITERATION_PATH`.
    Returns None when no usable rows exist.
    """
    try:
        y = int(year)
    except Exception:
        return None
    try:
        df = fetch_df(
            r"""
            SELECT TOP 1
              ROOT,
              COUNT(*) AS N
            FROM (
              SELECT
                LEFT(
                  TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH),
                  NULLIF(CHARINDEX('\', TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH) + '\'), 0) - 1
                ) AS ROOT
              FROM dbo.ADO_FEATURES
              WHERE TRY_CONVERT(INT, ADO_YEAR) = %s
                AND ITERATION_PATH IS NOT NULL
                AND LTRIM(RTRIM(TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH))) <> ''
            ) x
            WHERE ROOT IS NOT NULL AND LTRIM(RTRIM(ROOT)) <> ''
            GROUP BY ROOT
            ORDER BY N DESC, ROOT ASC
            """,
            (y,),
        )
    except Exception:
        return None
    if df is None or df.empty:
        return None
    try:
        root = str(df.iloc[0]["ROOT"]).strip()
        return root if root else None
    except Exception:
        return None


@cache_data_portfolio(ttl=600, show_spinner=False)
def list_iteration_roots(year: int) -> List[str]:
    """Return distinct iteration roots in `dbo.ADO_FEATURES` for a year."""
    try:
        y = int(year)
    except Exception:
        return []
    try:
        df = fetch_df(
            r"""
            SELECT DISTINCT
              LEFT(
                TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH),
                NULLIF(CHARINDEX('\', TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH) + '\'), 0) - 1
              ) AS ROOT
            FROM dbo.ADO_FEATURES
            WHERE TRY_CONVERT(INT, ADO_YEAR) = %s
              AND ITERATION_PATH IS NOT NULL
              AND LTRIM(RTRIM(TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH))) <> ''
            """,
            (y,),
        )
    except Exception:
        return []
    if df is None or df.empty or "ROOT" not in df.columns:
        return []
    roots = df["ROOT"].dropna().astype(str).map(lambda s: s.strip()).tolist()
    roots = [r for r in roots if r]
    return sorted(set(roots))

@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_team_allocated_headcount_by_pi(team_id: str, year: int) -> pd.DataFrame:
    """Allocated headcount (capacity anchor) for a single team by PI, from `VW_TEAM_ALLOCATED_HEADCOUNT_PI`."""
    try:
        df = fetch_df(
            """
            SELECT
              ITERATION_NUM,
              ALLOCATED_HEADCOUNT,
              DELIVERY_HEADCOUNT,
              CONTRACTOR_CS_HEADCOUNT,
              CONTRACTOR_C_HEADCOUNT
            FROM VW_TEAM_ALLOCATED_HEADCOUNT_PI
            WHERE TEAMID = %s AND ADO_YEAR = %s
            """,
            (str(team_id), int(year)),
        )
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_program_allocated_headcount_by_pi(program_id: str, year: int) -> pd.DataFrame:
    """Allocated headcount (capacity anchor) for a program by PI, summed across teams."""
    try:
        df = fetch_df(
            """
            SELECT
              ITERATION_NUM,
              SUM(ALLOCATED_HEADCOUNT) AS ALLOCATED_HEADCOUNT
            FROM VW_TEAM_ALLOCATED_HEADCOUNT_PI
            WHERE PROGRAMID = %s AND ADO_YEAR = %s
            GROUP BY ITERATION_NUM
            """,
            (str(program_id), int(year)),
        )
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _normalize_location_bucket(val: Any) -> str:
    loc = str(val or "").strip().upper()
    if not loc:
        return "OTHER"
    if loc in {"GBC", "US-0970+HC", "US-0910", "OTHER"}:
        return loc
    return "OTHER"


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_allocated_headcount_by_team(
    *,
    year: int,
    programs: Tuple[str, ...] = (),
    teams: Tuple[str, ...] = (),
    groups: Tuple[str, ...] = (),
    rev: Optional[int] = None,
) -> pd.DataFrame:
    """Headcount-by-location for teams (capacity view), matching Teams/Programs 'Headcount' logic.

    Returns long-form rows: TEAMNAME, PROGRAMNAME, LOCATION, ALLOCATED_HEADCOUNT.
    Notes:
    - Uses `TEAM_HEADCOUNT_HISTORY` (PI=0) and includes TEAM+DELIVERY classes.
    - `groups` is accepted for signature compatibility; filtering is applied via TEAM/PROGRAM names.
    """
    _ = rev, groups

    where: List[str] = ["h.YEAR = %s", "ISNULL(h.PI, 0) = 0", "UPPER(h.CLASS) IN ('TEAM','DELIVERY')", "t.TEAMID IS NOT NULL"]
    params: List[Any] = [int(year)]
    if programs:
        _json_in(where, params, "UPPER(p.PROGRAMNAME)", [str(x).upper() for x in programs if str(x).strip()])
    if teams:
        _json_in(where, params, "UPPER(t.TEAMNAME)", [str(x).upper() for x in teams if str(x).strip()])

    sql = f"""
      SELECT
        t.TEAMNAME,
        p.PROGRAMNAME,
        h.LOCATION,
        SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS ALLOCATED_HEADCOUNT
      FROM { _fq('TEAM_HEADCOUNT_HISTORY') } h
      LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = h.TEAMID
      LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = t.PROGRAMID
      WHERE {' AND '.join(where)}
      GROUP BY t.TEAMNAME, p.PROGRAMNAME, h.LOCATION
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        if df is None or df.empty:
            return pd.DataFrame(columns=["TEAMNAME", "PROGRAMNAME", "LOCATION", "ALLOCATED_HEADCOUNT"])
        out = df.copy()
        out["TEAMNAME"] = out.get("TEAMNAME", "").astype(str).str.strip()
        out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").astype(str).str.strip()
        out["LOCATION"] = out.get("LOCATION", "").apply(_normalize_location_bucket)
        out["ALLOCATED_HEADCOUNT"] = pd.to_numeric(out.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
        out = (
            out.groupby(["TEAMNAME", "PROGRAMNAME", "LOCATION"], dropna=False)["ALLOCATED_HEADCOUNT"]
            .sum()
            .reset_index()
        )
        out = out[(out["TEAMNAME"].ne("")) & (out["ALLOCATED_HEADCOUNT"] > 0)].copy()
        return out
    except Exception:
        return pd.DataFrame(columns=["TEAMNAME", "PROGRAMNAME", "LOCATION", "ALLOCATED_HEADCOUNT"])


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_allocated_headcount_by_program(
    *,
    year: int,
    programs: Tuple[str, ...] = (),
    teams: Tuple[str, ...] = (),
    groups: Tuple[str, ...] = (),
    rev: Optional[int] = None,
) -> pd.DataFrame:
    """Headcount-by-location aggregated to program level for the given scope."""
    _ = rev, groups
    team_long = fetch_allocated_headcount_by_team(year=year, programs=programs, teams=teams, groups=(), rev=None)
    if team_long is None or team_long.empty:
        return pd.DataFrame(columns=["PROGRAMNAME", "LOCATION", "ALLOCATED_HEADCOUNT"])
    out = (
        team_long.groupby(["PROGRAMNAME", "LOCATION"], dropna=False)["ALLOCATED_HEADCOUNT"]
        .sum()
        .reset_index()
    )
    out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    return out


def _set_preview_error(key: str, err: Optional[Exception]) -> None:
    bucket = st.session_state.setdefault("_preview_query_errors", {})
    if err is None:
        bucket.pop(key, None)
    else:
        bucket[key] = str(err)


def _cost_query_scope_key(
    *,
    fiscal_year: Optional[int],
    scenario: Optional[str],
    programs: Tuple[str, ...],
    teams: Tuple[str, ...],
    app_groups: Tuple[str, ...],
) -> str:
    def _norm(values: Tuple[str, ...]) -> Tuple[str, ...]:
        out = [str(v).strip().upper() for v in values if str(v).strip()]
        return tuple(sorted(out))

    year_norm = int(fiscal_year) if fiscal_year is not None else 0
    scen_norm = str(scenario or "").strip().upper()
    return "|".join(
        [
            f"Y={year_norm}",
            f"S={scen_norm}",
            f"P={','.join(_norm(programs))}",
            f"T={','.join(_norm(teams))}",
            f"G={','.join(_norm(app_groups))}",
        ]
    )


def _set_cost_query_warning(scope_key: str, message: Optional[str]) -> None:
    try:
        bucket = st.session_state.setdefault("_cost_query_warnings", {})
        if message is None or not str(message).strip():
            bucket.pop(scope_key, None)
        else:
            bucket[scope_key] = str(message).strip()
    except Exception:
        pass


def get_cost_query_warning(
    *,
    fiscal_year: Optional[int] = None,
    scenario: Optional[str] = None,
    programs: Tuple[str, ...] = (),
    teams: Tuple[str, ...] = (),
    app_groups: Tuple[str, ...] = (),
) -> Optional[str]:
    scope_key = _cost_query_scope_key(
        fiscal_year=fiscal_year,
        scenario=scenario,
        programs=programs,
        teams=teams,
        app_groups=app_groups,
    )
    try:
        bucket = st.session_state.get("_cost_query_warnings", {})
        msg = bucket.get(scope_key) if isinstance(bucket, dict) else None
        return str(msg).strip() if msg else None
    except Exception:
        return None


@cache_data_portfolio(ttl=300, show_spinner=False)
def fetch_pi_calendar_lookup(*, years: Optional[Tuple[int, ...]] = None) -> pd.DataFrame:
    """PI-level iteration calendar lookup for YEAR×PI, used as a fallback to attach real PI dates."""
    where = ["UPPER(COALESCE(ITERATION_GRAIN, 'PI')) = 'PI'"]
    params: List[Any] = []
    if years:
        placeholders = ", ".join(["%s"] * len(years))
        where.append(f"TRY_CONVERT(INT, YEAR) IN ({placeholders})")
        params.extend([int(y) for y in years])
    sql = f"""
      SELECT
        TRY_CONVERT(INT, YEAR) AS YEAR,
        ITERATION_LEVEL3,
        START_DATE,
        END_DATE
      FROM { _fq('ADO_ITERATION_CALENDAR') }
      WHERE {' AND '.join(where)}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        if df is None or df.empty:
            return pd.DataFrame(columns=["YEAR", "PI_NUM", "PERIOD_START_DATE", "PERIOD_END_DATE", "PERIOD_LABEL"])
        work = df.copy()
        work["YEAR"] = pd.to_numeric(work.get("YEAR"), errors="coerce").astype("Int64")
        it3 = work.get("ITERATION_LEVEL3", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
        pi_extracted = it3.str.extract(r"\\b(?:PI|I)\\s*[-_ ]*0*([0-9]{1,2})\\b", expand=False)
        if pi_extracted.isna().any():
            pi_extracted2 = it3.str.extract(r"\\bINCREMENT\\s*0*([0-9]{1,2})\\b", expand=False)
            pi_extracted = pi_extracted.combine_first(pi_extracted2)
        work["PI_NUM"] = pd.to_numeric(pi_extracted, errors="coerce").astype("Int64")
        work["PERIOD_START_DATE"] = pd.to_datetime(work.get("START_DATE"), errors="coerce")
        work["PERIOD_END_DATE"] = pd.to_datetime(work.get("END_DATE"), errors="coerce")
        ok = work["YEAR"].notna() & work["PI_NUM"].notna() & work["PERIOD_START_DATE"].notna() & work["PERIOD_END_DATE"].notna()
        work = work[ok].copy()
        if work.empty:
            return pd.DataFrame(columns=["YEAR", "PI_NUM", "PERIOD_START_DATE", "PERIOD_END_DATE", "PERIOD_LABEL"])
        work["PERIOD_LABEL"] = (
            work["YEAR"].astype("Int64").astype(str)
            + " I"
            + work["PI_NUM"].astype("Int64").astype(str)
            + " ("
            + work["PERIOD_START_DATE"].dt.strftime("%d%b")
            + "–"
            + work["PERIOD_END_DATE"].dt.strftime("%d%b")
            + ")"
        )
        return work[["YEAR", "PI_NUM", "PERIOD_START_DATE", "PERIOD_END_DATE", "PERIOD_LABEL"]].drop_duplicates()
    except Exception:
        return pd.DataFrame(columns=["YEAR", "PI_NUM", "PERIOD_START_DATE", "PERIOD_END_DATE", "PERIOD_LABEL"])


@cache_data_portfolio(ttl=300, show_spinner=False)
def fetch_pi_calendar_resolved_dates(*, years: Optional[Tuple[int, ...]] = None, data_version: Optional[int] = None) -> pd.DataFrame:
    _ = data_version
    """
    PI-level calendar rows keyed by the resolved level3 label (e.g. "2025 I4") with start/end dates.

    This does not parse PI numbers; it is intended for joining via a pre-built label like "YYYY I{PI}".
    """
    where = [
        "UPPER(COALESCE(ITERATION_GRAIN, 'PI')) = 'PI'",
        "ITERATION_LEVEL3 IS NOT NULL",
        "END_DATE IS NOT NULL",
    ]
    params: List[Any] = []
    if years:
        placeholders = ", ".join(["%s"] * len(years))
        where.append(f"TRY_CONVERT(INT, YEAR) IN ({placeholders})")
        params.extend([int(y) for y in years])
    sql = f"""
      SELECT
        TRY_CONVERT(INT, YEAR) AS CAL_YEAR,
        LTRIM(RTRIM(ITERATION_LEVEL3)) AS CAL_ITERATION_LEVEL3,
        MIN(START_DATE) AS CAL_START_DATE,
        MAX(END_DATE) AS CAL_END_DATE
      FROM { _fq('ADO_ITERATION_CALENDAR') }
      WHERE {' AND '.join(where)}
      GROUP BY
        TRY_CONVERT(INT, YEAR),
        LTRIM(RTRIM(ITERATION_LEVEL3))
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        if df is None or df.empty:
            return pd.DataFrame(columns=["CAL_YEAR", "CAL_ITERATION_LEVEL3", "CAL_PI_KEY", "CAL_START_DATE", "CAL_END_DATE"])
        work = df.copy()
        work["CAL_YEAR"] = pd.to_numeric(work.get("CAL_YEAR"), errors="coerce").astype("Int64")
        work["CAL_ITERATION_LEVEL3"] = work.get("CAL_ITERATION_LEVEL3", "").fillna("").astype(str).str.strip()
        work["CAL_START_DATE"] = pd.to_datetime(work.get("CAL_START_DATE"), errors="coerce")
        work["CAL_END_DATE"] = pd.to_datetime(work.get("CAL_END_DATE"), errors="coerce")
        work = work.loc[
            work["CAL_YEAR"].notna()
            & work["CAL_ITERATION_LEVEL3"].ne("")
            & work["CAL_END_DATE"].notna()
        ].copy()
        work["CAL_PI_KEY"] = work["CAL_ITERATION_LEVEL3"].map(normalize_pi_key)
        return work[
            ["CAL_YEAR", "CAL_ITERATION_LEVEL3", "CAL_PI_KEY", "CAL_START_DATE", "CAL_END_DATE"]
        ].drop_duplicates()
    except Exception:
        return pd.DataFrame(columns=["CAL_YEAR", "CAL_ITERATION_LEVEL3", "CAL_PI_KEY", "CAL_START_DATE", "CAL_END_DATE"])


def attach_pi_period_columns(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Attach PERIOD_* columns to a cost dataframe using PI calendar when missing."""
    if df is None or df.empty:
        return df
    work = df.copy()
    if (
        "PERIOD_END_DATE" in work.columns
        and pd.to_datetime(work.get("PERIOD_END_DATE"), errors="coerce").notna().any()
        and "PERIOD_LABEL" in work.columns
    ):
        return work
    year_num = pd.to_numeric(work.get("YEAR"), errors="coerce").astype("Int64")
    pi_num_raw = pd.to_numeric(work.get("PI"), errors="coerce")
    if pi_num_raw.isna().any():
        pi_str = work.get("PI", pd.Series(dtype=object)).astype(str).fillna("")
        pi_fallback = pd.to_numeric(pi_str.str.extract(r"(\\d+)", expand=False), errors="coerce")
        pi_num_raw = pi_num_raw.fillna(pi_fallback)
    pi_num = pi_num_raw.astype("Int64")
    work["YEAR"] = year_num
    work["PI_NUM"] = pi_num
    years = tuple(sorted({int(y) for y in year_num.dropna().astype(int).tolist()})) if year_num.notna().any() else None
    cal = fetch_pi_calendar_lookup(years=years)
    if cal is not None and not cal.empty:
        if "YEAR" not in cal.columns or "PI_NUM" not in cal.columns:
            # Defensive: tolerate unexpected calendar schema without hard fail.
            if "CAL_YEAR" in cal.columns and "YEAR" not in cal.columns:
                cal = cal.rename(columns={"CAL_YEAR": "YEAR"})
            if "CAL_PI_KEY" in cal.columns and "PI_NUM" not in cal.columns:
                cal = cal.rename(columns={"CAL_PI_KEY": "PI_NUM"})
        if "YEAR" in cal.columns and "PI_NUM" in cal.columns:
            cal = cal.copy()
            cal["YEAR"] = pd.to_numeric(cal.get("YEAR"), errors="coerce").astype("Int64")
            cal["PI_NUM"] = pd.to_numeric(cal.get("PI_NUM"), errors="coerce").astype("Int64")
            keep_cols = [c for c in ["YEAR", "PI_NUM", "PERIOD_START_DATE", "PERIOD_END_DATE", "PERIOD_LABEL"] if c in cal.columns]
            work = work.merge(cal[keep_cols], on=["YEAR", "PI_NUM"], how="left")
    if "PERIOD_START_DATE" not in work.columns:
        work["PERIOD_START_DATE"] = pd.NaT
    if "PERIOD_END_DATE" not in work.columns:
        work["PERIOD_END_DATE"] = pd.NaT
    if "PERIOD_LABEL" not in work.columns:
        work["PERIOD_LABEL"] = None
    label = work["PERIOD_LABEL"].astype("object")
    has_pi = work["YEAR"].notna() & work["PI_NUM"].notna()
    fill_pi = label.isna() & has_pi
    if fill_pi.any():
        label.loc[fill_pi] = (
            work.loc[fill_pi, "YEAR"].astype("Int64").astype(str) + " I" + work.loc[fill_pi, "PI_NUM"].astype("Int64").astype(str)
        )
    fill_year = label.isna() & work["YEAR"].notna()
    if fill_year.any():
        label.loc[fill_year] = work.loc[fill_year, "YEAR"].astype("Int64").astype(str)
    work["PERIOD_LABEL"] = label
    return work


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_cost_for_movers(
    years,
    programs,
    teams,
    groups,
    rev: Optional[int] = None,
) -> pd.DataFrame:
    """Mover-focused cost aggregate matching Welcome (includes SUBCOMPONENT + TOTAL_COST)."""
    sql_where, params = ["1=1"], []
    _json_in(sql_where, params, "YEAR", years)
    _json_in(sql_where, params, "PROGRAMNAME", programs)
    _json_in(sql_where, params, "TEAMNAME", teams)
    _json_in(sql_where, params, "GROUPNAME", groups)
    sql = f"""
      SELECT
        YEAR,
        PI,
        PROGRAMNAME,
        TEAMNAME,
        GROUPNAME,
        COST_CATEGORY,
        SUBCOMPONENT,
        SUM(AMOUNT) AS TOTAL_COST
      FROM { _fq('VW_TCO_WORKFORCE_SPLIT') }
      WHERE {' AND '.join(sql_where)}
        AND UPPER(COALESCE(SOURCE, '')) <> 'PROGRAM_ADDITIONAL'
      GROUP BY YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, COST_CATEGORY, SUBCOMPONENT
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        if df is None or df.empty:
            _set_preview_error("fetch_cost_for_movers", None)
            return pd.DataFrame(columns=[
                "YEAR","PI","PROGRAMNAME","TEAMNAME","GROUPNAME","COST_CATEGORY","SUBCOMPONENT","TOTAL_COST",
            ])
        df = apply_display_scope_names(df.copy())
        df["TOTAL_COST"] = pd.to_numeric(df.get("TOTAL_COST"), errors="coerce").fillna(0.0)
        _set_preview_error("fetch_cost_for_movers", None)
        return df
    except Exception as exc:
        _set_preview_error("fetch_cost_for_movers", exc)
        return pd.DataFrame(columns=[
            "YEAR","PI","PROGRAMNAME","TEAMNAME","GROUPNAME","COST_CATEGORY","SUBCOMPONENT","TOTAL_COST",
        ])


@cache_data_portfolio(ttl=300, show_spinner=False)
def fetch_filter_options(data_version: Optional[int] = None) -> pd.DataFrame:
    _ = data_version
    # Performance guardrail:
    # Filter options are used to populate UI selectors and do not require cost-grain rows.
    # Use feature-demand grain (much smaller) instead of workforce split to avoid blocking
    # app startup under local SQL pressure.
    sql = f"""
      SELECT DISTINCT
        TRY_CONVERT(INT, d.YEAR) AS YEAR,
        TRY_CONVERT(INT, d.PI) AS PI,
        d.PROGRAMNAME,
        d.TEAMNAME,
        COALESCE(d.GROUPNAME, '(Unmapped Application)') AS GROUPNAME,
        CAST(NULL AS NVARCHAR(100)) AS SOURCE,
        CAST(NULL AS NVARCHAR(200)) AS FEATURE_INVESTMENT_DIMENSION
      FROM { _fq('VW_TCO_FEATURE_DEMAND') } d
      WHERE COALESCE(d.IN_SCOPE_FOR_ROADMAP, 0) = 1
    """
    try:
        out = fetch_df(sql, None)
        out_df = out if isinstance(out, pd.DataFrame) else pd.DataFrame()
        if out_df.empty:
            # Lightweight structural fallback (no cost math): keep selectors usable.
            p = fetch_df(f"SELECT PROGRAMNAME FROM { _fq('PROGRAMS') }", None)
            t = fetch_df(f"SELECT TEAMNAME FROM { _fq('TEAMS') }", None)
            g = fetch_df(f"SELECT GROUPNAME FROM { _fq('APPLICATION_GROUPS') }", None)
            pvals = [str(x).strip() for x in p.get("PROGRAMNAME", pd.Series(dtype=str)).dropna().tolist()] if isinstance(p, pd.DataFrame) else []
            tvals = [str(x).strip() for x in t.get("TEAMNAME", pd.Series(dtype=str)).dropna().tolist()] if isinstance(t, pd.DataFrame) else []
            gvals = [str(x).strip() for x in g.get("GROUPNAME", pd.Series(dtype=str)).dropna().tolist()] if isinstance(g, pd.DataFrame) else []
            rows = []
            for v in pvals:
                if v:
                    rows.append({"YEAR": None, "PI": None, "PROGRAMNAME": v, "TEAMNAME": None, "GROUPNAME": None, "SOURCE": None, "FEATURE_INVESTMENT_DIMENSION": None})
            for v in tvals:
                if v:
                    rows.append({"YEAR": None, "PI": None, "PROGRAMNAME": None, "TEAMNAME": v, "GROUPNAME": None, "SOURCE": None, "FEATURE_INVESTMENT_DIMENSION": None})
            for v in gvals:
                if v:
                    rows.append({"YEAR": None, "PI": None, "PROGRAMNAME": None, "TEAMNAME": None, "GROUPNAME": v, "SOURCE": None, "FEATURE_INVESTMENT_DIMENSION": None})
            out_df = pd.DataFrame(rows, columns=[
                "YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "SOURCE", "FEATURE_INVESTMENT_DIMENSION"
            ])
        return apply_display_scope_names(out_df)
    except Exception:
        return pd.DataFrame(columns=[
            "YEAR","PI","PROGRAMNAME","TEAMNAME","GROUPNAME","SOURCE","FEATURE_INVESTMENT_DIMENSION"
        ])


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_ado_team_options(
    years: Tuple[int, ...],
    programs: Optional[List[str]] = None,
    data_version: Optional[int] = None,
) -> pd.DataFrame:
    _ = data_version
    where = ["COALESCE(d.IN_SCOPE_FOR_ROADMAP, 0) = 1", "UPPER(COALESCE(d.FEATURE_STATE, '')) <> 'REMOVED'"]
    params: List[Any] = []
    if years:
        placeholders = ", ".join(["%s"] * len(years))
        where.append(f"TRY_CONVERT(INT, d.YEAR) IN ({placeholders})")
        params.extend([int(y) for y in years])
    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(d.PROGRAMNAME) IN ({placeholders})")
        params.extend([p.upper() for p in programs])
    sql = f"""
      SELECT DISTINCT
        TRY_CONVERT(INT, d.YEAR) AS YEAR,
        d.PROGRAMNAME,
        d.TEAMNAME,
        COALESCE(d.GROUPNAME, '(Unmapped Application)') AS GROUPNAME
      FROM { _fq('VW_TCO_FEATURE_DEMAND') } d
      WHERE {' AND '.join(where)}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        if df is None:
            return pd.DataFrame(columns=["YEAR", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"])
        return apply_display_scope_names(df)
    except Exception:
        return pd.DataFrame(columns=["YEAR", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"])


@cache_data_portfolio(ttl=600, show_spinner=False)
def fetch_cost_events_by_pi(
    *,
    fiscal_year: Optional[int] = None,
    program: Optional[str] = None,
    team: Optional[str] = None,
    app_group: Optional[str] = None,
) -> pd.DataFrame:
    """Return cost events grouped by PI and iteration, with WF/NWF split."""
    where: List[str] = ["1=1"]
    params: List[Any] = []
    if fiscal_year is not None:
        where.append("FISCAL_YEAR = %s")
        params.append(int(fiscal_year))
    if program:
        where.append("PROGRAM = %s")
        params.append(str(program))
    if team:
        where.append("TEAM = %s")
        params.append(str(team))
    if app_group:
        where.append("APP_GROUP = %s")
        params.append(str(app_group))

    sql = f"""
      SELECT
        FISCAL_YEAR,
        PI_NAME,
        ITERATION_NAME,
        PROGRAM,
        TEAM,
        APP_GROUP,
        COST_BUCKET,
        EVENT_TYPE,
        SUM(AMOUNT_DELTA) AS AMOUNT_DELTA,
        SUM(HEADCOUNT_DELTA) AS HEADCOUNT_DELTA
      FROM { _fq('TCO_COST_EVENTS') }
      WHERE {' AND '.join(where)}
      GROUP BY
        FISCAL_YEAR,
        PI_NAME,
        ITERATION_NAME,
        PROGRAM,
        TEAM,
        APP_GROUP,
        COST_BUCKET,
        EVENT_TYPE
      ORDER BY
        FISCAL_YEAR,
        PI_NAME,
        COALESCE(ITERATION_NAME, '')
    """
    cols = [
        "FISCAL_YEAR",
        "PI_NAME",
        "ITERATION_NAME",
        "PROGRAM",
        "TEAM",
        "APP_GROUP",
        "COST_BUCKET",
        "EVENT_TYPE",
        "AMOUNT_DELTA",
        "HEADCOUNT_DELTA",
    ]
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        if df is None or df.empty:
            return pd.DataFrame(columns=cols)
        out = df.copy()
        out["FISCAL_YEAR"] = pd.to_numeric(out.get("FISCAL_YEAR"), errors="coerce").fillna(0).astype(int)
        out["AMOUNT_DELTA"] = pd.to_numeric(out.get("AMOUNT_DELTA"), errors="coerce").fillna(0.0)
        out["HEADCOUNT_DELTA"] = pd.to_numeric(out.get("HEADCOUNT_DELTA"), errors="coerce").fillna(0.0)
        return out
    except Exception:
        return pd.DataFrame(columns=cols)


def _norm_cost_bucket(val: Any) -> str:
    raw = str(val or "").strip().upper().replace(" ", "_")
    if raw in {"WF", "WORK_FORCE", "WORKFORCE", "WORK"}:
        return "WF"
    if raw in {"NWF", "NON_WORK_FORCE", "NONWORK_FORCE", "NONWORKFORCE", "NON_WORK"}:
        return "NWF"
    return "NWF"


def _pi_name_from_num(pi: Any) -> Optional[str]:
    try:
        n = int(pd.to_numeric(pi, errors="coerce"))
    except Exception:
        return None
    if n in {1, 2, 3, 4}:
        return f"I{n}"
    return None


def _roadmap_capacity_fraction(default: float = 0.8) -> float:
    raw: Any = None
    try:
        raw = st.secrets.get("model", {}).get("roadmap_capacity_fraction", None)  # type: ignore[attr-defined]
    except Exception:
        raw = None
    try:
        val = float(raw) if raw is not None and str(raw).strip() != "" else float(default)
    except Exception:
        val = float(default)
    if not (val == val) or val in (float("inf"), float("-inf")):
        return float(default)
    return max(0.3, min(1.0, float(val)))


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_allocated_capacity_team_pi_years(
    *,
    years: Tuple[int, ...],
    programs: Tuple[str, ...],
    teams: Tuple[str, ...],
) -> pd.DataFrame:
    years_norm = tuple(sorted({int(y) for y in years or () if y is not None and int(y) > 0}))
    if not years_norm:
        return pd.DataFrame()
    where = ["ADO_YEAR IN (" + ", ".join(["%s"] * len(years_norm)) + ")"]
    params: List[Any] = [int(y) for y in years_norm]
    if programs:
        _json_in(where, params, "UPPER(PROGRAMNAME)", [str(p).upper() for p in programs if str(p).strip()])
    if teams:
        _json_in(where, params, "UPPER(TEAMNAME)", [str(t).upper() for t in teams if str(t).strip()])
    sql = f"""
      SELECT
        TEAMID,
        TEAMNAME,
        PROGRAMID,
        PROGRAMNAME,
        ADO_YEAR,
        ITERATION_NUM,
        DELIVERY_HEADCOUNT,
        CONTRACTOR_C_HEADCOUNT,
        CONTRACTOR_CS_HEADCOUNT,
        ALLOCATED_HEADCOUNT
      FROM { _fq('VW_TEAM_ALLOCATED_HEADCOUNT_PI') }
      WHERE {" AND ".join(where)}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_program_overhead_people_cost_inputs(
    *,
    year: int,
    programs: Tuple[str, ...],
    location: str = "GBC",
) -> pd.DataFrame:
    where = ["TRY_CONVERT(INT, c.YEAR) = %s"]
    params: List[Any] = [int(year)]
    if programs:
        _json_in(where, params, "UPPER(p.PROGRAMNAME)", [str(p).upper() for p in programs if str(p).strip()])
    sql = f"""
      SELECT
        c.PROGRAMID,
        p.PROGRAMNAME,
        TRY_CONVERT(INT, c.YEAR) AS YEAR,
        TRY_CONVERT(INT, c.PI) AS PI,
        TRY_CONVERT(FLOAT, c.PROGRAMFTE) AS PROGRAMFTE,
        COALESCE(TRY_CONVERT(FLOAT, pr.PROGRAM_XOM_RATE), 0) AS PROGRAM_XOM_RATE
      FROM { _fq('VW_PROGRAM_COMPOSITION_EFFECTIVE') } c
      LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = c.PROGRAMID
      LEFT JOIN { _fq('VW_PROGRAM_RATE_EFFECTIVE') } pr
        ON pr.PROGRAMID = c.PROGRAMID
       AND pr.YEAR = TRY_CONVERT(INT, c.YEAR)
       AND pr.PI = TRY_CONVERT(INT, c.PI)
       AND UPPER(LTRIM(RTRIM(pr.LOCATION))) = UPPER(LTRIM(RTRIM(%s)))
      WHERE {" AND ".join(where)}
    """
    try:
        df = fetch_df(sql, tuple([str(location)] + params))
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_team_rate_history(
    *,
    team_ids: Tuple[str, ...],
    location_preference: str = "GBC",
) -> pd.DataFrame:
    ids = tuple([str(t).strip() for t in (team_ids or ()) if str(t).strip()])
    if not ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(ids))
    sql = f"""
      SELECT
        TEAMID,
        TRY_CONVERT(INT, YEAR) AS YEAR,
        TRY_CONVERT(INT, PI) AS PI,
        LOCATION,
        TRY_CONVERT(FLOAT, XOM_RATE) AS XOM_RATE,
        UPDATED_AT
      FROM { _fq('TEAM_RATE_HISTORY') }
      WHERE TEAMID IN ({placeholders})
    """
    try:
        df = fetch_df(sql, tuple(ids))
    except Exception:
        df = None
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["TEAMID"] = out.get("TEAMID", "").astype(str).str.strip()
    out["YEAR"] = pd.to_numeric(out.get("YEAR"), errors="coerce").astype("Int64")
    out["PI"] = pd.to_numeric(out.get("PI"), errors="coerce").astype("Int64")
    out["LOCATION"] = out.get("LOCATION", "").astype(str).str.strip()
    out["XOM_RATE"] = pd.to_numeric(out.get("XOM_RATE"), errors="coerce")
    out["UPDATED_AT"] = pd.to_datetime(out.get("UPDATED_AT"), errors="coerce")
    pref = str(location_preference or "GBC").strip().upper()
    out["_LOC_RANK"] = out["LOCATION"].astype(str).str.upper().ne(pref).astype(int)
    return out


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_program_rate_history(
    *,
    program_ids: Tuple[str, ...],
    location_preference: str = "GBC",
) -> pd.DataFrame:
    ids = tuple([str(p).strip() for p in (program_ids or ()) if str(p).strip()])
    if not ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(ids))
    sql = f"""
      SELECT
        PROGRAMID,
        TRY_CONVERT(INT, YEAR) AS YEAR,
        TRY_CONVERT(INT, PI) AS PI,
        LOCATION,
        TRY_CONVERT(FLOAT, PROGRAM_XOM_RATE) AS PROGRAM_XOM_RATE,
        UPDATED_AT
      FROM { _fq('PROGRAM_RATE_HISTORY') }
      WHERE PROGRAMID IN ({placeholders})
    """
    try:
        df = fetch_df(sql, tuple(ids))
    except Exception:
        df = None
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["PROGRAMID"] = out.get("PROGRAMID", "").astype(str).str.strip()
    out["YEAR"] = pd.to_numeric(out.get("YEAR"), errors="coerce").astype("Int64")
    out["PI"] = pd.to_numeric(out.get("PI"), errors="coerce").astype("Int64")
    out["LOCATION"] = out.get("LOCATION", "").astype(str).str.strip()
    out["PROGRAM_XOM_RATE"] = pd.to_numeric(out.get("PROGRAM_XOM_RATE"), errors="coerce")
    out["UPDATED_AT"] = pd.to_datetime(out.get("UPDATED_AT"), errors="coerce")
    pref = str(location_preference or "GBC").strip().upper()
    out["_LOC_RANK"] = out["LOCATION"].astype(str).str.upper().ne(pref).astype(int)
    return out


def _best_pi_and_year_rates(
    df_rates: pd.DataFrame,
    *,
    id_col: str,
    rate_col: str,
    out_rate_col: str,
    years: Tuple[int, ...],
    pi_list: Tuple[int, ...],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Match Budget's 'best rate' fallback logic: prefer exact year+PI, then year+PI0, then latest year for that id."""
    if df_rates is None or df_rates.empty:
        return pd.DataFrame(columns=[id_col, "YEAR", "PI", out_rate_col]), pd.DataFrame(columns=[id_col, "YEAR", out_rate_col])

    work_all = df_rates.copy()
    work_all[id_col] = work_all.get(id_col, "").astype(str).str.strip()
    work_all["YEAR"] = pd.to_numeric(work_all.get("YEAR"), errors="coerce").astype("Int64")
    work_all["PI"] = pd.to_numeric(work_all.get("PI"), errors="coerce").astype("Int64")
    work_all[rate_col] = pd.to_numeric(work_all.get(rate_col), errors="coerce")
    if "_LOC_RANK" not in work_all.columns:
        work_all["_LOC_RANK"] = 0
    if "UPDATED_AT" not in work_all.columns:
        work_all["UPDATED_AT"] = pd.NaT

    years_pref = tuple(sorted({int(y) for y in years or () if y is not None and int(y) > 0}))
    pi_vals = tuple([int(p) for p in (pi_list or ()) if int(p) in {1, 2, 3, 4}])
    if not years_pref or not pi_vals:
        return pd.DataFrame(columns=[id_col, "YEAR", "PI", out_rate_col]), pd.DataFrame(columns=[id_col, "YEAR", out_rate_col])

    pref_pi = (
        work_all.loc[work_all["PI"].isin(pi_vals)]
        .sort_values([id_col, "YEAR", "PI", "_LOC_RANK", "UPDATED_AT"], ascending=[True, False, True, True, False])
        .drop_duplicates([id_col, "YEAR", "PI"], keep="first")[[id_col, "YEAR", "PI", rate_col]]
        .rename(columns={rate_col: out_rate_col})
    )
    pref_0 = (
        work_all.loc[work_all["PI"].fillna(0).astype(int) == 0]
        .sort_values([id_col, "YEAR", "_LOC_RANK", "UPDATED_AT"], ascending=[True, False, True, False])
        .drop_duplicates([id_col, "YEAR"], keep="first")[[id_col, "YEAR", rate_col]]
        .rename(columns={rate_col: out_rate_col})
    )
    latest_pi = (
        work_all.loc[work_all["PI"].isin(pi_vals)]
        .sort_values([id_col, "PI", "YEAR", "_LOC_RANK", "UPDATED_AT"], ascending=[True, True, False, True, False])
        .drop_duplicates([id_col, "PI"], keep="first")[[id_col, "PI", rate_col]]
        .rename(columns={rate_col: f"{out_rate_col}__FALLBACK"})
    )
    latest_0 = (
        work_all.loc[work_all["PI"].fillna(0).astype(int) == 0]
        .sort_values([id_col, "YEAR", "_LOC_RANK", "UPDATED_AT"], ascending=[True, False, True, False])
        .drop_duplicates([id_col], keep="first")[[id_col, rate_col]]
        .rename(columns={rate_col: f"{out_rate_col}__FALLBACK"})
    )
    ids = sorted(set(work_all[id_col].dropna().astype(str).tolist()))
    idx_pi = pd.MultiIndex.from_product([ids, years_pref, pi_vals], names=[id_col, "YEAR", "PI"]).to_frame(index=False)
    best_pi = idx_pi.merge(pref_pi, on=[id_col, "YEAR", "PI"], how="left")
    best_pi = best_pi.merge(latest_pi, on=[id_col, "PI"], how="left")
    best_pi[out_rate_col] = pd.to_numeric(best_pi.get(out_rate_col), errors="coerce").fillna(
        pd.to_numeric(best_pi.get(f"{out_rate_col}__FALLBACK"), errors="coerce")
    )
    best_pi = best_pi.drop(columns=[f"{out_rate_col}__FALLBACK"], errors="ignore")
    best_pi[out_rate_col] = pd.to_numeric(best_pi.get(out_rate_col), errors="coerce")
    best_pi = best_pi[best_pi[out_rate_col].notna() & (best_pi[out_rate_col] > 0)].copy()
    best_pi["YEAR"] = pd.to_numeric(best_pi.get("YEAR"), errors="coerce").astype("Int64")
    best_pi["PI"] = pd.to_numeric(best_pi.get("PI"), errors="coerce").astype("Int64")

    idx_0 = pd.MultiIndex.from_product([ids, years_pref], names=[id_col, "YEAR"]).to_frame(index=False)
    best_0 = idx_0.merge(pref_0, on=[id_col, "YEAR"], how="left")
    best_0 = best_0.merge(latest_0, on=[id_col], how="left")
    best_0[out_rate_col] = pd.to_numeric(best_0.get(out_rate_col), errors="coerce").fillna(
        pd.to_numeric(best_0.get(f"{out_rate_col}__FALLBACK"), errors="coerce")
    )
    best_0 = best_0.drop(columns=[f"{out_rate_col}__FALLBACK"], errors="ignore")
    best_0[out_rate_col] = pd.to_numeric(best_0.get(out_rate_col), errors="coerce")
    best_0 = best_0[best_0[out_rate_col].notna() & (best_0[out_rate_col] > 0)].copy()
    best_0["YEAR"] = pd.to_numeric(best_0.get("YEAR"), errors="coerce").astype("Int64")
    return best_pi, best_0


@cache_data_portfolio(ttl=180, show_spinner=False)
def _fetch_contractor_baseline_costs(
    *,
    years: Tuple[int, ...],
    programs: Tuple[str, ...],
    teams: Tuple[str, ...],
    pi_nums: Tuple[int, ...],
) -> pd.DataFrame:
    years_norm = tuple(sorted({int(y) for y in years or () if y is not None and int(y) > 0}))
    pi_norm = tuple([int(p) for p in (pi_nums or ()) if int(p) in {1, 2, 3, 4}])
    where: List[str] = []
    params: List[Any] = []
    if years_norm:
        where.append("TRY_CONVERT(INT, h.YEAR) IN (" + ", ".join(["%s"] * len(years_norm)) + ")")
        params.extend([int(y) for y in years_norm])
    if pi_norm:
        where.append("TRY_CONVERT(INT, h.PI) IN (" + ", ".join(["%s"] * len(pi_norm)) + ")")
        params.extend([int(p) for p in pi_norm])
    if programs:
        _json_in(where, params, "UPPER(p.PROGRAMNAME)", [str(x).upper() for x in programs if str(x).strip()])
    if teams:
        _json_in(where, params, "UPPER(t.TEAMNAME)", [str(x).upper() for x in teams if str(x).strip()])
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
      SELECT
        TRY_CONVERT(INT, h.YEAR) AS YEAR,
        TRY_CONVERT(INT, h.PI) AS PI,
        p.PROGRAMNAME,
        t.TEAMNAME,
        h.CLASS,
        cc.NAME AS PROVIDER,
        TRY_CONVERT(FLOAT, h.HEADCOUNT) AS HEADCOUNT,
        TRY_CONVERT(FLOAT, r.RATE) AS RATE
      FROM { _fq('VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE') } h
      LEFT JOIN { _fq('VW_CONTRACTOR_RATE_EFFECTIVE') } r
        ON r.COMPANYID = h.COMPANYID AND r.YEAR = h.YEAR AND r.PI = h.PI AND r.CLASS = h.CLASS
      LEFT JOIN { _fq('CONTRACTOR_COMPANY') } cc ON cc.COMPANYID = h.COMPANYID
      LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = h.TEAMID
      LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = t.PROGRAMID
      {where_sql}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_cost_lines(
    *,
    fiscal_year: Optional[int] = None,
    scenario: Optional[str] = None,
    programs: Tuple[str, ...] = (),
    teams: Tuple[str, ...] = (),
    app_groups: Tuple[str, ...] = (),
    rev: Optional[int] = None,
    data_version: Optional[int] = None,
) -> pd.DataFrame:
    _ = data_version
    """Scenario-aware cost lines used across pages (BASELINE, REVISED_PLAN, EXPECTED, ACTUAL).

    Output keeps the repo's existing naming (YEAR/PI/PROGRAMNAME/TEAMNAME/GROUPNAME/SUBCOMPONENT/COST_CATEGORY/SOURCE),
    and also includes scenario columns (SCENARIO, COST_BUCKET, PI_NAME, APP_GROUP, FISCAL_YEAR).
    """
    # Canonical adapter: keep all authoritative cost logic in `core/canonical_costs.py`.
    # This function adapts canonical outputs to a stable, page-friendly schema.
    empty_cols = [
        "FISCAL_YEAR",
        "YEAR",
        "PI",
        "PI_NAME",
        "PROGRAM",
        "TEAM",
        "APP_GROUP",
        "PROGRAMNAME",
        "TEAMNAME",
        "GROUPNAME",
        "COST_BUCKET",
        "COST_CATEGORY",
        "WF_LAYER2",
        "WF_SUBTYPE",
        "SOD_LAYER2",
        "SOD_SUBTYPE",
        "SUBCOMPONENT",
        "SUBCOMPONENT_ROLLUP",
        "SOURCE",
        "SCENARIO",
        "AMOUNT",
        "FTE",
        "MAPPING_STATUS",
        "ALLOCATION_DRIVER",
        "SHARE",
    ]
    scope_key = _cost_query_scope_key(
        fiscal_year=fiscal_year,
        scenario=scenario,
        programs=programs,
        teams=teams,
        app_groups=app_groups,
    )

    def _ok(df: pd.DataFrame) -> pd.DataFrame:
        _set_cost_query_warning(scope_key, None)
        return df

    try:
        from core.canonical_costs import get_cost_lines  # local import to avoid cycles

        scen0 = str(scenario or "").strip().upper() or None
        if scen0 not in {None, "BASELINE", "REVISED_PLAN", "EXPECTED", "ACTUAL"}:
            scen0 = None

        filters: dict[str, Any] = {}
        if fiscal_year is not None:
            filters["year"] = [int(fiscal_year)]
        if programs:
            filters["program"] = [str(p) for p in programs if str(p).strip()]
        if teams:
            filters["team"] = [str(t) for t in teams if str(t).strip()]
        if app_groups:
            filters["app_group"] = [str(g) for g in app_groups if str(g).strip()]

        def _to_int_series_compat(x: Any, default: int = 0):
            s = pd.to_numeric(x, errors="coerce")
            if not isinstance(s, pd.Series):
                try:
                    return int(default) if pd.isna(s) else int(s)
                except Exception:
                    return int(default)
            try:
                return s.astype(pd.Int64Dtype())
            except Exception:
                return s.fillna(default).astype("int64")

        def _adapt(df: pd.DataFrame, scen_label: str) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame(columns=empty_cols)
            out = df.copy()
            out["YEAR"] = _to_int_series_compat(out.get("YEAR"))
            out["PI"] = _to_int_series_compat(out.get("PI"))
            out["PI_NAME"] = out["PI"].apply(_pi_name_from_num)
            out["FISCAL_YEAR"] = out["YEAR"]
            out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
            out["TEAMNAME"] = out.get("TEAMNAME", "").fillna("").astype(str).str.strip()
            out["GROUPNAME"] = out.get("GROUPNAME", "").fillna("").astype(str).str.strip()
            out["PROGRAM"] = out["PROGRAMNAME"]
            out["TEAM"] = out["TEAMNAME"]
            out["APP_GROUP"] = out["GROUPNAME"]
            out["COST_CATEGORY"] = out.get("COST_CATEGORY", "").fillna("").astype(str).str.strip()
            out["COST_BUCKET"] = out["COST_CATEGORY"].apply(_norm_cost_bucket)
            out["SUBCOMPONENT"] = out.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
            out["SUBCOMPONENT_ROLLUP"] = out.get("SUBCOMPONENT_ROLLUP", out.get("SUBCOMPONENT", "")).fillna("").astype(str).str.strip()
            out["WF_LAYER2"] = out.get("WF_LAYER2", "").fillna("").astype(str).str.strip()
            out["WF_SUBTYPE"] = out.get("WF_SUBTYPE", "").fillna("").astype(str).str.strip()
            out["SOD_LAYER2"] = out.get("SOD_LAYER2", "").fillna("").astype(str).str.strip()
            out["SOD_SUBTYPE"] = out.get("SOD_SUBTYPE", "").fillna("").astype(str).str.strip()
            out["SOURCE"] = out.get("SOURCE", "").fillna("").astype(str).str.strip()
            out["AMOUNT"] = pd.to_numeric(out.get("AMOUNT"), errors="coerce").fillna(0.0)
            out["SCENARIO"] = scen_label
            if "FTE" not in out.columns:
                out["FTE"] = pd.NA
            out["FTE"] = pd.to_numeric(out.get("FTE"), errors="coerce")
            for c in ["MAPPING_STATUS", "ALLOCATION_DRIVER", "SHARE"]:
                if c not in out.columns:
                    out[c] = "" if c != "SHARE" else pd.NA
            out = apply_display_scope_names(out)
            # Enforce a stable, page-friendly schema and drop any legacy/extra columns.
            return out.reindex(columns=empty_cols)

        def _load_one(scen_key: str) -> pd.DataFrame:
            base = get_cost_lines(fetch_df, scenario=scen_key, filters=filters)
            return _adapt(base, str(scen_key).strip().upper())

        if scen0 == "REVISED_PLAN":
            base = _load_one("BASELINE")
            try:
                ev = fetch_cost_events_by_pi(fiscal_year=int(fiscal_year)) if fiscal_year is not None else pd.DataFrame()
            except Exception:
                ev = pd.DataFrame()
            if ev is not None and not ev.empty:
                ev = ev.copy()
                ev["YEAR"] = _to_int_series_compat(ev.get("FISCAL_YEAR"))
                ev["PI_NAME"] = ev.get("PI_NAME", "").fillna("").astype(str).str.strip()
                ev["PI"] = _to_int_series_compat(ev["PI_NAME"].str.extract(r"(?i)I\\s*([1-4])", expand=False), default=0)
                ev["PROGRAMNAME"] = ev.get("PROGRAM", "").fillna("").astype(str).str.strip()
                ev["TEAMNAME"] = ev.get("TEAM", "").fillna("").astype(str).str.strip()
                ev["GROUPNAME"] = ev.get("APP_GROUP", "").fillna("").astype(str).str.strip()
                ev["COST_BUCKET"] = ev.get("COST_BUCKET", "").apply(_norm_cost_bucket)
                ev["COST_CATEGORY"] = ev["COST_BUCKET"].map({"WF": "WORK_FORCE", "NWF": "NON_WORK_FORCE"}).fillna("NON_WORK_FORCE")
                ev["SUBCOMPONENT"] = ev.get("EVENT_TYPE", "").fillna("").astype(str).str.strip().replace({"": "Event"})
                ev["SUBCOMPONENT_ROLLUP"] = ev["SUBCOMPONENT"]
                ev["AMOUNT"] = pd.to_numeric(ev.get("AMOUNT_DELTA"), errors="coerce").fillna(0.0)
                ev["SOURCE"] = "EVENT"
                ev["SCENARIO"] = "REVISED_PLAN"
                ev["FISCAL_YEAR"] = ev["YEAR"]
                ev["PROGRAM"] = ev["PROGRAMNAME"]
                ev["TEAM"] = ev["TEAMNAME"]
                ev["APP_GROUP"] = ev["GROUPNAME"]
                ev["PI_NAME"] = ev.get("PI_NAME", ev["PI"].apply(_pi_name_from_num))
                ev["FTE"] = pd.to_numeric(ev.get("HEADCOUNT_DELTA"), errors="coerce")
                for c in ["MAPPING_STATUS", "ALLOCATION_DRIVER", "SHARE"]:
                    if c not in ev.columns:
                        ev[c] = "" if c != "SHARE" else pd.NA
                base = pd.concat([base, ev[empty_cols]], ignore_index=True, sort=False)
            base["SCENARIO"] = "REVISED_PLAN"
            return _ok(base)

        if scen0:
            return _ok(_load_one(scen0))

        frames: list[pd.DataFrame] = []
        partial_errors: list[str] = []
        for scen_key in ("BASELINE", "EXPECTED", "ACTUAL"):
            try:
                frame = _load_one(scen_key)
            except Exception as scen_exc:
                err_txt = str(scen_exc).strip()
                if len(err_txt) > 140:
                    err_txt = err_txt[:137] + "..."
                partial_errors.append(f"{scen_key}: {err_txt or type(scen_exc).__name__}")
                frame = pd.DataFrame(columns=empty_cols)
            if frame is not None and not frame.empty:
                frames.append(frame)

        out_all = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame(columns=empty_cols)
        if partial_errors:
            warning = (
                "Cost model partially loaded for this scope. Some scenarios timed out or failed: "
                + "; ".join(partial_errors)
            )
            _set_cost_query_warning(scope_key, warning)
            return out_all
        return _ok(out_all if out_all is not None else pd.DataFrame(columns=empty_cols))
    except Exception as exc:
        err_name = type(exc).__name__
        err_msg = str(exc).strip()
        if len(err_msg) > 180:
            err_msg = err_msg[:177] + "..."
        warning = (
            "Cost model query failed for this scope; placeholders are shown. "
            "Check database latency/query timeout and refresh."
        )
        if err_msg:
            warning = f"{warning} ({err_name}: {err_msg})"
        _set_cost_query_warning(scope_key, warning)
        # Fail-open for UI availability: callers receive an empty canonical-shaped frame
        # instead of crashing the page on transient DB/query timeout issues.
        return pd.DataFrame(columns=empty_cols)


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_forecast_monthly(fiscal_year: int, program_ids: List[str], rev: Optional[int] = None) -> pd.DataFrame:
    def _run(sql: str, params: Tuple[Any, ...]) -> pd.DataFrame:
        df = fetch_df(sql, params)
        return df if df is not None else pd.DataFrame()

    fy = int(fiscal_year)
    base_where = ["COALESCE(i.FISCAL_YEAR, YEAR(i.RENEWALDATE)) = %s"]
    params: List[Any] = [fy]
    if program_ids:
        placeholders = ", ".join(["%s"] * len(program_ids))
        base_where.append(f"p.PROGRAMID IN ({placeholders})")
        params.extend(program_ids)
    where_sql = "WHERE " + " AND ".join(base_where)

    try:
        df_view = _run(
            f"""
            SELECT
              p.PROGRAMID,
              p.PROGRAMNAME,
              MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(i.FISCAL_YEAR, 1, 1))) AS MONTH,
              SUM(ISNULL(i.AMOUNT, 0)) AS AMOUNT
            FROM VW_INVOICES_ACTUAL_AND_FORECAST i
            LEFT JOIN TEAMS t ON t.TEAMID = i.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = ISNULL(i.PROGRAMID_AT_BOOKING, t.PROGRAMID)
            {where_sql}
            GROUP BY p.PROGRAMID, p.PROGRAMNAME, MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(i.FISCAL_YEAR, 1, 1)))
            """,
            tuple(params),
        )
        if df_view is not None and not df_view.empty:
            return df_view
    except Exception:
        pass

    try:
        df_fallback = _run(
            f"""
            SELECT
              p.PROGRAMID,
              p.PROGRAMNAME,
              MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(i.FISCAL_YEAR, 1, 1))) AS MONTH,
              SUM(ISNULL(i.AMOUNT, 0)) AS AMOUNT
            FROM INVOICES i
            LEFT JOIN TEAMS t ON t.TEAMID = i.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = ISNULL(i.PROGRAMID_AT_BOOKING, t.PROGRAMID)
            {where_sql}
            GROUP BY p.PROGRAMID, p.PROGRAMNAME, MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(i.FISCAL_YEAR, 1, 1)))
            """,
            tuple(params),
        )
        return df_fallback
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_apptio_actuals_monthly(fiscal_year: int, program_ids: List[str], rev: Optional[int] = None) -> pd.DataFrame:
    df = fetch_apptio_actuals_by_program(fiscal_year, program_ids)
    if df is None:
        return pd.DataFrame()
    return df


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_apptio_actuals_monthly_breakdown(
    fiscal_year: int,
    program_ids: List[str],
    rev: Optional[int] = None,
    data_version: Optional[int] = None,
) -> pd.DataFrame:
    _ = data_version
    df = fetch_apptio_actuals_by_program_breakdown(fiscal_year, program_ids)
    if df is None:
        return pd.DataFrame()
    return df


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_ado_features(
    year: int,
    programs: List[str],
    teams: List[str],
    groups: List[str],
    rev: Optional[int] = None,
    data_version: Optional[int] = None,
) -> pd.DataFrame:
    _ = data_version
    # Canonical inclusion rule for roadmap scope across the app:
    # - Roadmap inclusion is determined by the canonical Explorer v2 view (`VW_TCO_FEATURE_DEMAND`)
    #   via `IN_SCOPE_FOR_ROADMAP`.
    where = [
        "TRY_CONVERT(INT, v.ADO_YEAR) = %s",
        "COALESCE(v.IN_SCOPE_FOR_ROADMAP, 0) = 1",
        "UPPER(COALESCE(v.STATE, '')) <> 'REMOVED'",
    ]
    params: List[Any] = [int(year)]
    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(v.PROGRAMNAME) IN ({placeholders})")
        params.extend([p.upper() for p in programs])
    if teams:
        placeholders = ", ".join(["%s"] * len(teams))
        where.append(f"UPPER(v.TEAMNAME) IN ({placeholders})")
        params.extend([t.upper() for t in teams])
    if groups:
        placeholders = ", ".join(["%s"] * len(groups))
        where.append(f"UPPER(COALESCE(v.GROUPNAME, '(UNMAPPED APP GROUP)')) IN ({placeholders})")
        params.extend([g.upper() for g in groups])

    @cache_data_portfolio(ttl=300, show_spinner=False)
    def _object_columns(obj_name: str) -> set:
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

    def _sel_or_null(col: str, alias: str, cast_type: str) -> str:
        if col.upper() in view_cols:
            return f"v.{col} AS {alias}"
        return f"CAST(NULL AS {cast_type}) AS {alias}"

    view_cols = _object_columns("VW_ADO_FEATURES_ENRICHED")
    feat_prog_cols = _object_columns("ADO_FEATURE_PROGRESS")
    epic_prog_cols = _object_columns("ADO_EPIC_PROGRESS")
    has_feature_progress = "FEATURE_ID" in feat_prog_cols
    has_epic_progress = "EPIC_ID" in epic_prog_cols
    has_epic_feature_counts = (
        "FEATURE_COUNT_TOTAL" in epic_prog_cols
        and "FEATURE_COUNT_WITH_STORIES" in epic_prog_cols
        and "FEATURE_COUNT_WITH_SP" in epic_prog_cols
    )
    feature_progress_join = (
        f"LEFT JOIN {_fq('ADO_FEATURE_PROGRESS')} fp ON TRY_CONVERT(BIGINT, fp.FEATURE_ID) = TRY_CONVERT(BIGINT, v.FEATURE_ID)"
        if has_feature_progress
        else ""
    )
    epic_progress_join = (
        f"LEFT JOIN {_fq('ADO_EPIC_PROGRESS')} ep ON TRY_CONVERT(BIGINT, ep.EPIC_ID) = TRY_CONVERT(BIGINT, v.EPIC_ID)"
        if has_epic_progress
        else ""
    )
    feature_progress_select = (
        "COALESCE(TRY_CONVERT(FLOAT, fp.PCT_COMPLETE), 0.0) AS FEATURE_PROGRESS_PCT,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.PROPOSED_SP), 0.0) AS FEATURE_PROGRESS_PROPOSED_SP,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.INPROGRESS_SP), 0.0) AS FEATURE_PROGRESS_INPROGRESS_SP,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.COMPLETED_SP), 0.0) AS FEATURE_PROGRESS_COMPLETED_SP,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.TOTAL_SP), 0.0) AS FEATURE_PROGRESS_TOTAL_SP"
        if has_feature_progress
        else "CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_PCT,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_PROPOSED_SP,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_INPROGRESS_SP,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_COMPLETED_SP,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_TOTAL_SP"
    )
    epic_progress_select = (
        (
            "COALESCE(TRY_CONVERT(FLOAT, ep.PCT_COMPLETE), 0.0) AS EPIC_PROGRESS_PCT,\n"
            "        COALESCE(TRY_CONVERT(INT, ep.FEATURE_COUNT_TOTAL), 0) AS EPIC_FEATURE_COUNT_TOTAL,\n"
            "        COALESCE(TRY_CONVERT(INT, ep.FEATURE_COUNT_WITH_STORIES), 0) AS EPIC_FEATURE_COUNT_WITH_STORIES,\n"
            "        COALESCE(TRY_CONVERT(INT, ep.FEATURE_COUNT_WITH_SP), 0) AS EPIC_FEATURE_COUNT_WITH_SP"
            if has_epic_feature_counts
            else "COALESCE(TRY_CONVERT(FLOAT, ep.PCT_COMPLETE), 0.0) AS EPIC_PROGRESS_PCT,\n"
                 "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_TOTAL,\n"
                 "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_STORIES,\n"
                 "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_SP"
        )
        if has_epic_progress
        else "CAST(NULL AS FLOAT) AS EPIC_PROGRESS_PCT,\n"
             "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_TOTAL,\n"
             "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_STORIES,\n"
             "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_SP"
    )

    sql_view = f"""
      SELECT
        v.FEATURE_ID,
        v.TITLE,
        v.STATE,
        v.BUSINESS_VALUE,
        v.STORY_POINTS,
        v.ADO_YEAR,
        v.ITERATION_NUM,
        v.PI_LABEL,
        {_sel_or_null("PI_LABEL_RAW", "PI_LABEL_RAW", "NVARCHAR(400)")},
        v.TEAMNAME,
        v.PROGRAMNAME,
        v.GROUPNAME,
        v.MAPPING_STATUS,
        v.IS_BASE,
        CAST(COALESCE(v.IN_SCOPE_FOR_ROADMAP, 0) AS BIT) AS IN_SCOPE_FOR_ROADMAP,
        {_sel_or_null("PARENT_ID", "PARENT_ID", "BIGINT")},
        {_sel_or_null("EPIC_ID", "EPIC_ID", "BIGINT")},
        {_sel_or_null("EPIC_TITLE", "EPIC_TITLE", "NVARCHAR(500)")},
        {_sel_or_null("EPIC_STATE", "EPIC_STATE", "NVARCHAR(100)")},
        {_sel_or_null("FEATURE_URL", "FEATURE_URL", "NVARCHAR(500)")},
        {_sel_or_null("EPIC_URL", "EPIC_URL", "NVARCHAR(500)")},
        {feature_progress_select},
        {epic_progress_select}
      FROM {_fq('VW_ADO_FEATURES_ENRICHED')} v
      {feature_progress_join}
      {epic_progress_join}
      WHERE {' AND '.join(where)}
    """

    try:
        df = fetch_df(sql_view, tuple(params) if params else None)
        st.session_state["ado_features_used_fallback"] = False
        return df if df is not None else pd.DataFrame()
    except Exception:
        st.session_state["ado_features_used_fallback"] = True
        return _fetch_ado_features_fallback(year, programs, teams, groups)


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_ado_features_for_roadmap(
    year: int,
    programs: List[str],
    teams: List[str],
    groups: List[str],
    rev: Optional[int] = None,
    data_version: Optional[int] = None,
) -> pd.DataFrame:
    _ = data_version
    where = [
        "TRY_CONVERT(INT, v.ADO_YEAR) = %s",
        "UPPER(COALESCE(v.STATE, '')) <> 'REMOVED'",
    ]
    params: List[Any] = [int(year)]
    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(v.PROGRAMNAME) IN ({placeholders})")
        params.extend([p.upper() for p in programs])
    if teams:
        placeholders = ", ".join(["%s"] * len(teams))
        where.append(f"UPPER(v.TEAMNAME) IN ({placeholders})")
        params.extend([t.upper() for t in teams])
    if groups:
        placeholders = ", ".join(["%s"] * len(groups))
        where.append(f"UPPER(COALESCE(v.GROUPNAME, '(UNMAPPED APP GROUP)')) IN ({placeholders})")
        params.extend([g.upper() for g in groups])

    @cache_data_portfolio(ttl=300, show_spinner=False)
    def _object_columns(obj_name: str) -> set:
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

    def _sel_or_null(col: str, alias: str, cast_type: str) -> str:
        if col.upper() in view_cols:
            return f"v.{col} AS {alias}"
        return f"CAST(NULL AS {cast_type}) AS {alias}"

    view_cols = _object_columns("VW_ADO_FEATURES_ENRICHED")
    feat_prog_cols = _object_columns("ADO_FEATURE_PROGRESS")
    epic_prog_cols = _object_columns("ADO_EPIC_PROGRESS")
    has_feature_progress = "FEATURE_ID" in feat_prog_cols
    has_epic_progress = "EPIC_ID" in epic_prog_cols
    has_epic_feature_counts = (
        "FEATURE_COUNT_TOTAL" in epic_prog_cols
        and "FEATURE_COUNT_WITH_STORIES" in epic_prog_cols
        and "FEATURE_COUNT_WITH_SP" in epic_prog_cols
    )
    feature_progress_join = (
        f"LEFT JOIN {_fq('ADO_FEATURE_PROGRESS')} fp ON TRY_CONVERT(BIGINT, fp.FEATURE_ID) = TRY_CONVERT(BIGINT, v.FEATURE_ID)"
        if has_feature_progress
        else ""
    )
    epic_progress_join = (
        f"LEFT JOIN {_fq('ADO_EPIC_PROGRESS')} ep ON TRY_CONVERT(BIGINT, ep.EPIC_ID) = TRY_CONVERT(BIGINT, v.EPIC_ID)"
        if has_epic_progress
        else ""
    )
    feature_progress_select = (
        "COALESCE(TRY_CONVERT(FLOAT, fp.PCT_COMPLETE), 0.0) AS FEATURE_PROGRESS_PCT,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.PROPOSED_SP), 0.0) AS FEATURE_PROGRESS_PROPOSED_SP,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.INPROGRESS_SP), 0.0) AS FEATURE_PROGRESS_INPROGRESS_SP,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.COMPLETED_SP), 0.0) AS FEATURE_PROGRESS_COMPLETED_SP,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.TOTAL_SP), 0.0) AS FEATURE_PROGRESS_TOTAL_SP"
        if has_feature_progress
        else "CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_PCT,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_PROPOSED_SP,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_INPROGRESS_SP,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_COMPLETED_SP,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_TOTAL_SP"
    )
    epic_progress_select = (
        (
            "COALESCE(TRY_CONVERT(FLOAT, ep.PCT_COMPLETE), 0.0) AS EPIC_PROGRESS_PCT,\n"
            "        COALESCE(TRY_CONVERT(INT, ep.FEATURE_COUNT_TOTAL), 0) AS EPIC_FEATURE_COUNT_TOTAL,\n"
            "        COALESCE(TRY_CONVERT(INT, ep.FEATURE_COUNT_WITH_STORIES), 0) AS EPIC_FEATURE_COUNT_WITH_STORIES,\n"
            "        COALESCE(TRY_CONVERT(INT, ep.FEATURE_COUNT_WITH_SP), 0) AS EPIC_FEATURE_COUNT_WITH_SP"
            if has_epic_feature_counts
            else "COALESCE(TRY_CONVERT(FLOAT, ep.PCT_COMPLETE), 0.0) AS EPIC_PROGRESS_PCT,\n"
                 "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_TOTAL,\n"
                 "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_STORIES,\n"
                 "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_SP"
        )
        if has_epic_progress
        else "CAST(NULL AS FLOAT) AS EPIC_PROGRESS_PCT,\n"
             "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_TOTAL,\n"
             "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_STORIES,\n"
             "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_SP"
    )

    sql_view = f"""
      SELECT
        v.FEATURE_ID,
        v.TITLE,
        v.STATE,
        v.BUSINESS_VALUE,
        v.STORY_POINTS,
        v.ADO_YEAR,
        v.ITERATION_NUM,
        v.PI_LABEL,
        {_sel_or_null("PI_LABEL_RAW", "PI_LABEL_RAW", "NVARCHAR(400)")},
        v.TEAMNAME,
        v.PROGRAMNAME,
        v.GROUPNAME,
        v.MAPPING_STATUS,
        v.IS_BASE,
        CAST(COALESCE(v.IN_SCOPE_FOR_ROADMAP, 0) AS BIT) AS IN_SCOPE_FOR_ROADMAP,
        {_sel_or_null("PARENT_ID", "PARENT_ID", "BIGINT")},
        {_sel_or_null("EPIC_ID", "EPIC_ID", "BIGINT")},
        {_sel_or_null("EPIC_TITLE", "EPIC_TITLE", "NVARCHAR(500)")},
        {_sel_or_null("EPIC_STATE", "EPIC_STATE", "NVARCHAR(100)")},
        {_sel_or_null("FEATURE_URL", "FEATURE_URL", "NVARCHAR(500)")},
        {_sel_or_null("EPIC_URL", "EPIC_URL", "NVARCHAR(500)")},
        {feature_progress_select},
        {epic_progress_select}
      FROM {_fq('VW_ADO_FEATURES_ENRICHED')} v
      {feature_progress_join}
      {epic_progress_join}
      WHERE {' AND '.join(where)}
    """

    try:
        df = fetch_df(sql_view, tuple(params) if params else None)
        st.session_state["ado_features_used_fallback"] = False
        return apply_display_scope_names(df if isinstance(df, pd.DataFrame) else pd.DataFrame())
    except Exception:
        st.session_state["ado_features_used_fallback"] = True
        return _fetch_ado_features_fallback(year, programs, teams, groups)


def _fetch_ado_features_fallback(
    year: int,
    programs: List[str],
    teams: List[str],
    groups: List[str],
) -> pd.DataFrame:
    where = [
        "TRY_CONVERT(INT, af.ADO_YEAR) = %s",
        "UPPER(COALESCE(af.STATE, '')) <> 'REMOVED'",
    ]
    params: List[Any] = [int(year)]
    program_expr = "COALESCE(NULLIF(LTRIM(RTRIM(af.PROGRAM_RAW)), ''), NULLIF(LTRIM(RTRIM(af.AREA_LEVEL3_RAW)), ''), NULLIF(LTRIM(RTRIM(af.TEAM_RAW)), ''))"
    team_expr = "COALESCE(NULLIF(LTRIM(RTRIM(af.AREA_LEVEL4_RAW)), ''), NULLIF(LTRIM(RTRIM(af.AREA_LEVEL3_RAW)), ''), NULLIF(LTRIM(RTRIM(af.TEAM_RAW)), ''), NULLIF(LTRIM(RTRIM(af.PROGRAM_RAW)), ''), NULLIF(LTRIM(RTRIM(af.AREA_PATH_RAW)), ''))"
    group_expr = "COALESCE(ag.GROUPNAME, '(Unmapped Application)')"

    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER({program_expr}) IN ({placeholders})")
        params.extend([p.upper() for p in programs])
    if teams:
        placeholders = ", ".join(["%s"] * len(teams))
        where.append(f"UPPER({team_expr}) IN ({placeholders})")
        params.extend([t.upper() for t in teams])
    if groups:
        placeholders = ", ".join(["%s"] * len(groups))
        where.append(f"UPPER({group_expr}) IN ({placeholders})")
        params.extend([g.upper() for g in groups])

    try:
        df_cols = fetch_df(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s",
            ("ADO_FEATURES",),
        )
    except Exception:
        df_cols = None
    table_cols = (
        {str(c).strip().upper() for c in df_cols["COLUMN_NAME"].tolist()}
        if df_cols is not None and not df_cols.empty
        else set()
    )
    try:
        fp_cols_df = fetch_df(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s",
            ("ADO_FEATURE_PROGRESS",),
        )
    except Exception:
        fp_cols_df = None
    fp_cols = (
        {str(c).strip().upper() for c in fp_cols_df["COLUMN_NAME"].tolist()}
        if fp_cols_df is not None and not fp_cols_df.empty
        else set()
    )
    try:
        ep_cols_df = fetch_df(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s",
            ("ADO_EPIC_PROGRESS",),
        )
    except Exception:
        ep_cols_df = None
    ep_cols = (
        {str(c).strip().upper() for c in ep_cols_df["COLUMN_NAME"].tolist()}
        if ep_cols_df is not None and not ep_cols_df.empty
        else set()
    )
    has_feature_progress = "FEATURE_ID" in fp_cols
    has_epic_progress = "EPIC_ID" in ep_cols
    has_epic_feature_counts = (
        "FEATURE_COUNT_TOTAL" in ep_cols
        and "FEATURE_COUNT_WITH_STORIES" in ep_cols
        and "FEATURE_COUNT_WITH_SP" in ep_cols
    )
    feature_progress_join = (
        f"LEFT JOIN {_fq('ADO_FEATURE_PROGRESS')} fp ON TRY_CONVERT(BIGINT, fp.FEATURE_ID) = TRY_CONVERT(BIGINT, af.FEATURE_ID)"
        if has_feature_progress
        else ""
    )
    epic_progress_join = (
        f"LEFT JOIN {_fq('ADO_EPIC_PROGRESS')} ep ON TRY_CONVERT(BIGINT, ep.EPIC_ID) = TRY_CONVERT(BIGINT, af.EPIC_ID)"
        if has_epic_progress
        else ""
    )
    feature_progress_select = (
        "COALESCE(TRY_CONVERT(FLOAT, fp.PCT_COMPLETE), 0.0) AS FEATURE_PROGRESS_PCT,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.PROPOSED_SP), 0.0) AS FEATURE_PROGRESS_PROPOSED_SP,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.INPROGRESS_SP), 0.0) AS FEATURE_PROGRESS_INPROGRESS_SP,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.COMPLETED_SP), 0.0) AS FEATURE_PROGRESS_COMPLETED_SP,\n"
        "        COALESCE(TRY_CONVERT(FLOAT, fp.TOTAL_SP), 0.0) AS FEATURE_PROGRESS_TOTAL_SP"
        if has_feature_progress
        else "CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_PCT,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_PROPOSED_SP,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_INPROGRESS_SP,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_COMPLETED_SP,\n"
             "        CAST(NULL AS FLOAT) AS FEATURE_PROGRESS_TOTAL_SP"
    )
    epic_progress_select = (
        (
            "COALESCE(TRY_CONVERT(FLOAT, ep.PCT_COMPLETE), 0.0) AS EPIC_PROGRESS_PCT,\n"
            "        COALESCE(TRY_CONVERT(INT, ep.FEATURE_COUNT_TOTAL), 0) AS EPIC_FEATURE_COUNT_TOTAL,\n"
            "        COALESCE(TRY_CONVERT(INT, ep.FEATURE_COUNT_WITH_STORIES), 0) AS EPIC_FEATURE_COUNT_WITH_STORIES,\n"
            "        COALESCE(TRY_CONVERT(INT, ep.FEATURE_COUNT_WITH_SP), 0) AS EPIC_FEATURE_COUNT_WITH_SP"
            if has_epic_feature_counts
            else "COALESCE(TRY_CONVERT(FLOAT, ep.PCT_COMPLETE), 0.0) AS EPIC_PROGRESS_PCT,\n"
                 "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_TOTAL,\n"
                 "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_STORIES,\n"
                 "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_SP"
        )
        if has_epic_progress
        else "CAST(NULL AS FLOAT) AS EPIC_PROGRESS_PCT,\n"
             "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_TOTAL,\n"
             "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_STORIES,\n"
             "        CAST(NULL AS INT) AS EPIC_FEATURE_COUNT_WITH_SP"
    )

    def _tbl_or_null(col: str, alias: str, cast_type: str) -> str:
        if col.upper() in table_cols:
            return f"af.{col} AS {alias}"
        return f"CAST(NULL AS {cast_type}) AS {alias}"

    pi_expr = (
        "COALESCE(af.PI_LABEL, af.ITERATION_LEVEL3) AS PI_LABEL"
        if "PI_LABEL" in table_cols
        else "COALESCE(af.ITERATION_LEVEL3) AS PI_LABEL"
    )
    sql = f"""
      SELECT
        af.FEATURE_ID,
        af.TITLE,
        af.STATE,
        TRY_CONVERT(FLOAT, af.BUSINESS_VALUE) AS BUSINESS_VALUE,
        COALESCE(TRY_CONVERT(FLOAT, af.STORY_POINTS), TRY_CONVERT(FLOAT, af.EFFORT_POINTS), 0) AS STORY_POINTS,
        TRY_CONVERT(INT, af.ADO_YEAR) AS ADO_YEAR,
        CAST(NULL AS INT) AS ITERATION_NUM,
        {pi_expr},
        {team_expr} AS TEAMNAME,
        {program_expr} AS PROGRAMNAME,
        {group_expr} AS GROUPNAME,
        CAST(NULL AS NVARCHAR(50)) AS MAPPING_STATUS,
        CAST(0 AS INT) AS IS_BASE,
        CAST(1 AS BIT) AS IN_SCOPE_FOR_ROADMAP,
        {_tbl_or_null("PARENT_ID", "PARENT_ID", "BIGINT")},
        {_tbl_or_null("EPIC_ID", "EPIC_ID", "BIGINT")},
        {_tbl_or_null("EPIC_TITLE", "EPIC_TITLE", "NVARCHAR(500)")},
        {_tbl_or_null("EPIC_STATE", "EPIC_STATE", "NVARCHAR(100)")},
        {_tbl_or_null("FEATURE_URL", "FEATURE_URL", "NVARCHAR(500)")},
        {_tbl_or_null("EPIC_URL", "EPIC_URL", "NVARCHAR(500)")},
        {feature_progress_select},
        {epic_progress_select}
      FROM {_fq('ADO_FEATURES')} af
      LEFT JOIN {_fq('MAP_ADO_APP_TO_TCO_GROUP')} mag
        ON UPPER(mag.ADO_APP) = UPPER(LTRIM(RTRIM(af.APP_NAME_RAW)))
      LEFT JOIN {_fq('APPLICATION_GROUPS')} ag ON ag.GROUPID = mag.APP_GROUP
      {feature_progress_join}
      {epic_progress_join}
      WHERE {' AND '.join(where)}
    """

    try:
        df = fetch_df(sql, tuple(params) if params else None)
        return apply_display_scope_names(df if isinstance(df, pd.DataFrame) else pd.DataFrame())
    except Exception:
        return pd.DataFrame()


def compute_ado_coverage(df: pd.DataFrame, data_version: Optional[int] = None) -> Tuple[Optional[float], Optional[float]]:
    _ = data_version
    if df is None or df.empty:
        return None, None
    out = df.copy()
    out["BUSINESS_VALUE"] = pd.to_numeric(out.get("BUSINESS_VALUE"), errors="coerce")
    total = len(out.index)
    if total == 0:
        return None, None
    bv_cov = float((out["BUSINESS_VALUE"].notna() & (out["BUSINESS_VALUE"] > 0)).sum()) / total
    # Demand readiness is evaluated from Explorer v2 demand rows (`core/ado_recon.load_explorer_feature_rows`),
    # not from ADO effort/points columns.
    return bv_cov, None
