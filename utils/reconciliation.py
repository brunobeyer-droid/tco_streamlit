# Summary: update reconciliation UI caption to NEXT branding.
from __future__ import annotations

from datetime import date as _date
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import quote

import pandas as pd
import streamlit as st


def _ado_org_proj_defaults() -> Tuple[str, str]:
    """Best-effort org/project resolution from session or secrets for ADO links."""
    try:
        _ado_cfg = getattr(st, "secrets", {}).get("ado", {})  # type: ignore
    except Exception:
        _ado_cfg = {}
    org = (st.session_state.get("ado_org") or _ado_cfg.get("org") or "").strip()
    proj = (st.session_state.get("ado_proj") or _ado_cfg.get("project") or "").strip()
    return org, proj


def _ado_query_url(org: str, proj: str, feature_ids: List[Any]) -> str:
    """Build a WIQL query URL to list the given feature IDs in ADO."""
    if not org or not proj or not feature_ids:
        return ""
    ids = [str(int(float(i))) for i in feature_ids if pd.notna(i)]
    if not ids:
        return ""
    ids = ids[:200]  # keep URL reasonable
    wiql = f"Select [System.Id], [System.Title] From WorkItems Where [System.Id] In ({','.join(ids)})"
    return f"https://dev.azure.com/{org}/{proj}/_workitems/query?wiql={quote(wiql)}"


def _in_clause(values: Sequence[Any]) -> str:
    return ", ".join(["%s"] * len(values))


def reconciliation_details(
    fetch_df,
    *,
    program: Optional[str] = None,
    team: Optional[str] = None,
    app_group: Optional[str] = None,
    years: Optional[Sequence[int]] = None,
    pis: Optional[Sequence[int]] = None,
    scope_programs: Optional[Sequence[str]] = None,
    scope_teams: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Return reconciliation gap tables and counts without rendering UI."""
    prog_clause = ""
    prog_params: List[Any] = []
    if program:
        prog_clause = "AND UPPER(p.PROGRAMNAME) = UPPER(%s)"
        prog_params.append(program)
    elif scope_programs:
        prog_clause = f"AND UPPER(p.PROGRAMNAME) IN ({_in_clause(scope_programs)})"
        prog_params.extend([p.upper() for p in scope_programs])

    team_clause = ""
    team_params: List[Any] = []
    if team:
        team_clause = "AND UPPER(t.TEAMNAME) = UPPER(%s)"
        team_params.append(team)
    elif scope_teams:
        team_clause = f"AND UPPER(t.TEAMNAME) IN ({_in_clause(scope_teams)})"
        team_params.extend([t.upper() for t in scope_teams])

    group_clause = ""
    group_params: List[Any] = []
    if app_group:
        group_clause = "AND UPPER(g.GROUPNAME) = UPPER(%s)"
        group_params.append(app_group)

    years = list(years or [])
    year_clause_v = ""
    year_clause_af = ""
    year_params: List[Any] = []
    if years:
        placeholders = _in_clause(years)
        year_clause_v = f"AND COALESCE(v.ADO_YEAR, YEAR(COALESCE(v.CHANGED_AT, v.CREATED_AT))) IN ({placeholders})"
        year_clause_af = f"AND COALESCE(af.ADO_YEAR, YEAR(COALESCE(af.CHANGED_AT, af.CREATED_AT))) IN ({placeholders})"
        year_params.extend(years)

    pis = list(pis or [])
    pi_clause_v = ""
    pi_clause_af = ""
    pi_params: List[Any] = []
    if pis and len(pis) < 4:
        placeholders = _in_clause(pis)
        pi_clause_v = f"""
          AND COALESCE(
            v.ITERATION_NUM,
            CASE WHEN v.ITERATION_PATH IS NULL OR CHARINDEX('I', UPPER(v.ITERATION_PATH)) = 0 THEN NULL
                 ELSE TRY_CONVERT(INT,
                   SUBSTRING(
                     SUBSTRING(UPPER(v.ITERATION_PATH), CHARINDEX('I', UPPER(v.ITERATION_PATH)), 10),
                     NULLIF(PATINDEX('%[1-4]%', SUBSTRING(UPPER(v.ITERATION_PATH), CHARINDEX('I', UPPER(v.ITERATION_PATH)), 10)), 0),
                     1
                   )
                 )
            END
          ) IN ({placeholders})
        """
        pi_clause_af = f"""
          AND COALESCE(
            af.ITERATION_NUM,
            CASE WHEN af.ITERATION_PATH IS NULL OR CHARINDEX('I', UPPER(af.ITERATION_PATH)) = 0 THEN NULL
                 ELSE TRY_CONVERT(INT,
                   SUBSTRING(
                     SUBSTRING(UPPER(af.ITERATION_PATH), CHARINDEX('I', UPPER(af.ITERATION_PATH)), 10),
                     NULLIF(PATINDEX('%[1-4]%', SUBSTRING(UPPER(af.ITERATION_PATH), CHARINDEX('I', UPPER(af.ITERATION_PATH)), 10)), 0),
                     1
                   )
                 )
            END
          ) IN ({placeholders})
        """
        pi_params.extend(pis)

    shared_params: List[Any] = []
    shared_params.extend(prog_params + team_params + group_params + year_params + pi_params)
    scope_params = tuple(shared_params) if shared_params else None

    # Missing program mapping counts
    missing_programs = 0
    total_programs = 0
    try:
        prog_map_sql = f"""
        WITH progs AS (
          SELECT DISTINCT af.PROGRAM_RAW
          FROM ADO_FEATURES af
          WHERE af.PROGRAM_RAW IS NOT NULL AND LTRIM(RTRIM(af.PROGRAM_RAW)) <> ''
            {year_clause_af}
            {pi_clause_af}
            {prog_clause.replace('p.', '')}
        )
        SELECT
          SUM(CASE WHEN mp.PROGRAMID IS NULL THEN 1 ELSE 0 END) AS MISSING_PROG,
          COUNT(*) AS TOTAL_PROG
        FROM progs pr
        LEFT JOIN MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp ON UPPER(mp.ADO_PROGRAM) = UPPER(pr.PROGRAM_RAW)
        """
        prog_params_sql: List[Any] = []
        prog_params_sql.extend(year_params)
        prog_params_sql.extend(pi_params)
        prog_params_sql.extend(prog_params)
        df_prog_status = fetch_df(prog_map_sql, tuple(prog_params_sql) if prog_params_sql else None)
        if df_prog_status is not None and not df_prog_status.empty:
            missing_programs = int(df_prog_status.iloc[0]["MISSING_PROG"] or 0)
            total_programs = int(df_prog_status.iloc[0]["TOTAL_PROG"] or 0)
    except Exception:
        missing_programs = 0
        total_programs = 0

    # Missing overview
    df_missing = None
    df_missing_detail = None
    try:
        missing_sql = f"""
        SELECT
          COALESCE(p.PROGRAMNAME, '(Unknown Program)') AS PROGRAM,
          COALESCE(t.TEAMNAME, '(Unknown Team)')       AS TEAM,
          COALESCE(g.GROUPNAME, '(Unmapped Application)') AS APP_GROUP,
          COUNT(*) AS FEATURES,
          SUM(CASE WHEN COALESCE(TRY_CONVERT(INT, g.IS_BASE), 0) = 0 THEN 1 ELSE 0 END) AS FEATURES_NON_BASE,
          SUM(CASE WHEN mt.TEAMID IS NULL THEN 1 ELSE 0 END) AS MISSING_TEAM,
          SUM(CASE WHEN g.GROUPID IS NULL THEN 1 ELSE 0 END) AS MISSING_APP_GROUP,
          -- BV is required only for non-BASE app groups.
          SUM(
            CASE
              WHEN COALESCE(TRY_CONVERT(INT, g.IS_BASE), 0) = 0
               AND (af.BUSINESS_VALUE IS NULL OR af.BUSINESS_VALUE <= 0)
              THEN 1 ELSE 0
            END
          ) AS MISSING_BV
        FROM ADO_FEATURES af
        LEFT JOIN MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp ON UPPER(mp.ADO_PROGRAM) = UPPER(af.PROGRAM_RAW)
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = mp.PROGRAMID
        LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM mt ON mt.ADO_TEAM_KEY = af.TEAM_VARIANT_KEY
        LEFT JOIN TEAMS t ON t.TEAMID = mt.TEAMID
        LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag ON mag.ADO_APP = af.APP_NAME_RAW
        LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = mag.APP_GROUP
        WHERE 1=1
          {prog_clause}
          {team_clause}
          {group_clause}
          {year_clause_af}
          {pi_clause_af}
        GROUP BY COALESCE(p.PROGRAMNAME, '(Unknown Program)'), COALESCE(t.TEAMNAME, '(Unknown Team)'), COALESCE(g.GROUPNAME, '(Unmapped Application)')
        """
        df_missing = fetch_df(missing_sql, scope_params)
    except Exception:
        df_missing = pd.DataFrame()

    try:
        detail_sql = f"""
        SELECT
          af.FEATURE_ID,
          af.BUSINESS_VALUE,
          COALESCE(p.PROGRAMNAME, '(Unknown Program)') AS PROGRAM,
          COALESCE(t.TEAMNAME, '(Unknown Team)')       AS TEAM,
          COALESCE(g.GROUPNAME, '(Unmapped Application)') AS APP_GROUP,
          COALESCE(TRY_CONVERT(INT, g.IS_BASE), 0) AS IS_BASE,
          CASE WHEN mt.TEAMID IS NULL THEN 1 ELSE 0 END AS MISSING_TEAM,
          CASE WHEN g.GROUPID IS NULL THEN 1 ELSE 0 END AS MISSING_APP_GROUP,
          CASE WHEN af.BUSINESS_VALUE IS NULL OR af.BUSINESS_VALUE <= 0 THEN 1 ELSE 0 END AS BV_IS_MISSING,
          CASE
            WHEN COALESCE(TRY_CONVERT(INT, g.IS_BASE), 0) = 0
             AND (af.BUSINESS_VALUE IS NULL OR af.BUSINESS_VALUE <= 0)
            THEN 1 ELSE 0
          END AS MISSING_BV
        FROM ADO_FEATURES af
        LEFT JOIN MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp ON UPPER(mp.ADO_PROGRAM) = UPPER(af.PROGRAM_RAW)
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = mp.PROGRAMID
        LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM mt ON mt.ADO_TEAM_KEY = af.TEAM_VARIANT_KEY
        LEFT JOIN TEAMS t ON t.TEAMID = mt.TEAMID
        LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag ON mag.ADO_APP = af.APP_NAME_RAW
        LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = mag.APP_GROUP
        WHERE 1=1
          {prog_clause}
          {team_clause}
          {group_clause}
          {year_clause_af}
          {pi_clause_af}
          AND (
            mt.TEAMID IS NULL
            OR g.GROUPID IS NULL
            OR (
              COALESCE(TRY_CONVERT(INT, g.IS_BASE), 0) = 0
              AND (af.BUSINESS_VALUE IS NULL OR af.BUSINESS_VALUE <= 0)
            )
          )
        """
        df_missing_detail = fetch_df(detail_sql, scope_params)
    except Exception:
        df_missing_detail = pd.DataFrame()

    total_features_all = int(pd.to_numeric(df_missing["FEATURES"], errors="coerce").fillna(0).sum()) if (df_missing is not None and not df_missing.empty) else 0

    try:
        # Canonical feature universe for scope: Explorer v2 demand view.
        prog_clause_d = prog_clause.replace("p.PROGRAMNAME", "d.PROGRAMNAME") if prog_clause else ""
        team_clause_d = team_clause.replace("t.TEAMNAME", "d.TEAMNAME") if team_clause else ""
        group_clause_d = group_clause.replace("g.GROUPNAME", "d.GROUPNAME") if group_clause else ""
        year_clause_d = f"AND TRY_CONVERT(INT, d.YEAR) IN ({_in_clause(years)})" if years else ""
        pi_clause_d = f"AND TRY_CONVERT(INT, d.PI) IN ({_in_clause(pis)})" if pis and len(pis) < 4 else ""

        scope_total_df = fetch_df(
            f"""
            SELECT COUNT(*) AS N
            FROM VW_TCO_FEATURE_DEMAND d
            WHERE COALESCE(TRY_CONVERT(INT, d.IN_SCOPE_FOR_ROADMAP), 0) = 1
              {prog_clause_d}
              {team_clause_d}
              {group_clause_d}
              {year_clause_d}
              {pi_clause_d}
            """,
            scope_params,
        )
        total_features_scope = int(scope_total_df.iloc[0]["N"]) if scope_total_df is not None and not scope_total_df.empty else 0
    except Exception:
        total_features_scope = 0

    return {
        "missing_programs": missing_programs,
        "total_programs": total_programs,
        "missing_summary": df_missing,
        "missing_detail": df_missing_detail,
        "total_features": total_features_all,
        "total_features_scope": total_features_scope,
    }


def reconciliation_kpis(
    fetch_df,
    *,
    program: Optional[str] = None,
    team: Optional[str] = None,
    app_group: Optional[str] = None,
    year: Optional[Union[int, Sequence[int]]] = None,
    pis: Optional[Sequence[int]] = None,
    scope_programs: Optional[Sequence[str]] = None,
    scope_teams: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Return KPI-friendly coverage metrics for ADO mappings and Business Value."""
    years = [year] if isinstance(year, int) else list(year or [])
    details = reconciliation_details(
        fetch_df,
        program=program,
        team=team,
        app_group=app_group,
        years=years,
        pis=pis,
        scope_programs=scope_programs,
        scope_teams=scope_teams,
    )
    summary = details.get("missing_summary")
    if summary is None:
        summary = pd.DataFrame()
    missing_programs = int(details.get("missing_programs") or 0)
    total_programs = int(details.get("total_programs") or 0)

    total_features = int(details.get("total_features") or 0)
    denom = total_features if total_features > 0 else 1
    total_missing_team = int(pd.to_numeric(summary.get("MISSING_TEAM", []), errors="coerce").fillna(0).sum()) if not summary.empty else 0
    total_missing_app = int(pd.to_numeric(summary.get("MISSING_APP_GROUP", []), errors="coerce").fillna(0).sum()) if not summary.empty else 0
    total_missing_bv = int(pd.to_numeric(summary.get("MISSING_BV", []), errors="coerce").fillna(0).sum()) if not summary.empty else 0
    bv_required_total = (
        int(pd.to_numeric(summary.get("FEATURES_NON_BASE", []), errors="coerce").fillna(0).sum())
        if (summary is not None and not summary.empty and "FEATURES_NON_BASE" in summary.columns)
        else total_features
    )
    bv_required_denom = bv_required_total if bv_required_total > 0 else 0

    pct_team_ok = max(0.0, (1 - (total_missing_team / denom)) * 100)
    pct_app_ok = max(0.0, (1 - (total_missing_app / denom)) * 100)
    pct_bv_ok = 100.0 if bv_required_denom == 0 else max(0.0, (1 - (total_missing_bv / bv_required_denom)) * 100)
    pct_prog_ok = 100.0 if total_programs == 0 else max(0.0, (1 - (missing_programs / max(total_programs, 1))) * 100)

    return {
        "coverage": {
            "program_mapping_pct": pct_prog_ok,
            "team_mapping_pct": pct_team_ok,
            "app_group_mapping_pct": pct_app_ok,
            "bv_pct": pct_bv_ok,
            "bv_required_total": bv_required_total,
            "bv_required_missing": total_missing_bv,
            "bv_required_present": max(0, bv_required_total - total_missing_bv),
        },
        "missing_counts": {
            "program": missing_programs,
            "team": total_missing_team,
            "app_group": total_missing_app,
            "bv": total_missing_bv,
        },
        "detail": details,
    }


def render_reconciliation_tab(fetch_df, scope, year: Optional[int] = None) -> None:
    """Render the Reconciliation tab (ADO mappings + Business Value)."""
    st.subheader("Reconciliation: ADO mappings + Business Value")
    st.caption("Shows ADO→NEXT mapping gaps and Business Value coverage for non-BASE app groups.")

    # Scope filters (Program -> Team -> Application)
    prog_df = fetch_df(
        """
        SELECT DISTINCT COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME) AS PROGRAMNAME
        FROM PROGRAMS
        ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME)
        """
    )
    prog_options = ["(All)"] + (prog_df["PROGRAMNAME"].dropna().astype(str).tolist() if prog_df is not None and not prog_df.empty else [])
    scope_prog = st.selectbox("Program (optional)", prog_options, index=0, key="recon_prog")
    prog_filter_clause = ""
    prog_param: List[str] = []
    if scope_prog and scope_prog != "(All)":
        prog_filter_clause = "AND UPPER(COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME)) = UPPER(%s)"
        prog_param = [scope_prog]

    scope_team_options = ["(All)"]
    try:
        df_team_opts = fetch_df(
            "SELECT DISTINCT COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME "
            "FROM TEAMS t JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID WHERE 1=1 "
            + (" AND UPPER(COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME))=UPPER(%s)" if prog_param else "")
            + " ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME)",
            tuple(prog_param) if prog_param else None,
        )
        if df_team_opts is not None and not df_team_opts.empty:
            scope_team_options += df_team_opts["TEAMNAME"].dropna().astype(str).tolist()
    except Exception:
        pass
    scope_team = st.selectbox("Team (optional)", scope_team_options, index=0, key="recon_team")
    team_filter_clause = ""
    team_param = []
    if scope_team and scope_team != "(All)":
        team_filter_clause = "AND UPPER(COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME)) = UPPER(%s)"
        team_param = [scope_team]

    scope_group_options = ["(All)"]
    try:
        df_group_opts = fetch_df(
            """
            SELECT DISTINCT g.GROUPNAME
            FROM APPLICATION_GROUPS g
            LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
            WHERE 1=1
            """
            + (prog_filter_clause.replace("AND", "") if prog_filter_clause else "")
            + " "
            + (team_filter_clause.replace("AND", "") if team_filter_clause else "")
            + """
            ORDER BY g.GROUPNAME
        """,
            tuple(prog_param + team_param) if (prog_param or team_param) else None,
        )
        if df_group_opts is not None and not df_group_opts.empty:
            scope_group_options += df_group_opts["GROUPNAME"].dropna().astype(str).tolist()
    except Exception:
        pass
    scope_group = st.selectbox("Application (optional)", scope_group_options, index=0, key="recon_app_group")
    group_filter_clause = ""
    group_param = []
    if scope_group and scope_group != "(All)":
        group_filter_clause = "AND UPPER(g.GROUPNAME) = UPPER(%s)"
        group_param = [scope_group]

    # Year (multi) and PI (multi) filters
    year_options: List[int] = []
    try:
        df_years = fetch_df(
            "SELECT DISTINCT TRY_CONVERT(INT, YEAR) AS YR FROM VW_TCO_FEATURE_DEMAND ORDER BY TRY_CONVERT(INT, YEAR) DESC"
        )
        if df_years is not None and not df_years.empty:
            year_options = [int(y) for y in df_years["YR"].dropna().tolist()]
    except Exception:
        year_options = []

    default_years = (
        [year] if (year is not None and year in year_options) else ([ _date.today().year ] if _date.today().year in year_options else (year_options[:1] if year_options else []))
    )
    selected_years = st.multiselect("Year (optional, multi-select)", year_options, default=default_years)
    year_filter_clause = ""
    year_params: List[int] = []
    if selected_years:
        placeholders = ", ".join(["%s"] * len(selected_years))
        year_filter_clause = f"AND TRY_CONVERT(INT, d.YEAR) IN ({placeholders})"
        year_params = selected_years

    pi_options = [1, 2, 3, 4]
    selected_pis = st.multiselect("Program Increment (PI)", pi_options, default=pi_options)
    pi_filter_clause = ""
    pi_params: List[int] = []
    if selected_pis and len(selected_pis) < len(pi_options):
        placeholders = ", ".join(["%s"] * len(selected_pis))
        pi_filter_clause = f"AND TRY_CONVERT(INT, d.PI) IN ({placeholders})"
        pi_params = selected_pis

    # ADO link settings (use ADO tab/session defaults; no extra inputs here)
    ado_org_for_links, ado_proj_for_links = _ado_org_proj_defaults()

    params_for_missing = prog_param + team_param + group_param + year_params + pi_params
    scope_params = tuple(params_for_missing) if params_for_missing else None
    missing_params = scope_params

    # Compute program mapping gaps (distinct ADO programs)
    missing_programs = 0
    total_programs = 0
    try:
        year_clause_prog = ""
        pi_clause_prog = ""
        if selected_years:
            placeholders = ", ".join(["%s"] * len(selected_years))
            year_clause_prog = f"AND COALESCE(af.ADO_YEAR, YEAR(COALESCE(af.CHANGED_AT, af.CREATED_AT))) IN ({placeholders})"
        if selected_pis and len(selected_pis) < len(pi_options):
            placeholders = ", ".join(["%s"] * len(selected_pis))
            pi_clause_prog = (
                f"AND COALESCE(af.ITERATION_NUM, CASE WHEN MONTH(COALESCE(af.CHANGED_AT, af.CREATED_AT)) BETWEEN 1 AND 3 THEN 1 "
                f"WHEN MONTH(COALESCE(af.CHANGED_AT, af.CREATED_AT)) BETWEEN 4 AND 6 THEN 2 "
                f"WHEN MONTH(COALESCE(af.CHANGED_AT, af.CREATED_AT)) BETWEEN 7 AND 9 THEN 3 "
                f"WHEN MONTH(COALESCE(af.CHANGED_AT, af.CREATED_AT)) BETWEEN 10 AND 12 THEN 4 END) IN ({placeholders})"
            )
        prog_clause_prog = ""
        prog_params_prog: List[Any] = []
        if prog_filter_clause:
            prog_clause_prog = "AND UPPER(p.PROGRAMNAME) = UPPER(%s)"
            prog_params_prog.extend(prog_param)
        prog_map_sql = f"""
        WITH progs AS (
          SELECT DISTINCT af.PROGRAM_RAW
          FROM ADO_FEATURES af
          WHERE af.PROGRAM_RAW IS NOT NULL AND LTRIM(RTRIM(af.PROGRAM_RAW)) <> ''
            {year_clause_prog}
            {pi_clause_prog}
            {prog_clause_prog}
        )
        SELECT
          SUM(CASE WHEN mp.PROGRAMID IS NULL THEN 1 ELSE 0 END) AS MISSING_PROG,
          COUNT(*) AS TOTAL_PROG
        FROM progs pr
        LEFT JOIN MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp ON UPPER(mp.ADO_PROGRAM) = UPPER(pr.PROGRAM_RAW)
        """
        prog_params_sql: List[Any] = []
        prog_params_sql.extend(year_params)
        prog_params_sql.extend(pi_params)
        prog_params_sql.extend(prog_params_prog)
        df_prog_status = fetch_df(prog_map_sql, tuple(prog_params_sql) if prog_params_sql else None)
        if df_prog_status is not None and not df_prog_status.empty:
            missing_programs = int(df_prog_status.iloc[0]["MISSING_PROG"] or 0)
            total_programs = int(df_prog_status.iloc[0]["TOTAL_PROG"] or 0)
    except Exception:
        missing_programs = 0
        total_programs = 0

    # Defaults for legacy KPI (disabled)
    missing_team_mapped_program = 0
    total_with_mapped_program = 0
    df_prog_unmapped = pd.DataFrame()

    # Missing KPI filter state (drives detail queries)
    missing_filter = st.session_state.get("recon_missing_filter", "")
    missing_filter_clause = ""  # reset filtering for clean slate

    # Missing data overview by Program/Team/Application (features + program map, team map)
    try:
        year_filter_af = ""
        pi_filter_af = ""
        if selected_years:
            placeholders = ", ".join(["%s"] * len(selected_years))
            year_filter_af = f"AND COALESCE(af.ADO_YEAR, YEAR(COALESCE(af.CHANGED_AT, af.CREATED_AT))) IN ({placeholders})"
        if selected_pis and len(selected_pis) < len(pi_options):
            placeholders = ", ".join(["%s"] * len(selected_pis))
            pi_filter_af = (
                f"AND COALESCE(af.ITERATION_NUM, CASE WHEN MONTH(COALESCE(af.CHANGED_AT, af.CREATED_AT)) BETWEEN 1 AND 3 THEN 1 "
                f"WHEN MONTH(COALESCE(af.CHANGED_AT, af.CREATED_AT)) BETWEEN 4 AND 6 THEN 2 "
                f"WHEN MONTH(COALESCE(af.CHANGED_AT, af.CREATED_AT)) BETWEEN 7 AND 9 THEN 3 "
                f"WHEN MONTH(COALESCE(af.CHANGED_AT, af.CREATED_AT)) BETWEEN 10 AND 12 THEN 4 END) IN ({placeholders})"
            )
        missing_sql = f"""
        SELECT
          COALESCE(p.PROGRAMNAME, '(Unknown Program)') AS PROGRAM,
          COALESCE(t.TEAMNAME, '(Unknown Team)')       AS TEAM,
          COALESCE(g.GROUPNAME, '(Unmapped Application)') AS APP_GROUP,
          COUNT(*) AS FEATURES,
          SUM(CASE WHEN mt.TEAMID IS NULL THEN 1 ELSE 0 END) AS MISSING_TEAM,
          SUM(CASE WHEN g.GROUPID IS NULL THEN 1 ELSE 0 END) AS MISSING_APP_GROUP
        FROM ADO_FEATURES af
        LEFT JOIN MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp ON UPPER(mp.ADO_PROGRAM) = UPPER(af.PROGRAM_RAW)
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = mp.PROGRAMID
        LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM mt ON mt.ADO_TEAM_KEY = af.TEAM_VARIANT_KEY
        LEFT JOIN TEAMS t ON t.TEAMID = mt.TEAMID
        LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag ON mag.ADO_APP = af.APP_NAME_RAW
        LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = mag.APP_GROUP
        WHERE 1=1
          {prog_filter_clause}
          {team_filter_clause}
          {group_filter_clause}
          {year_filter_af}
          {pi_filter_af}
        GROUP BY COALESCE(p.PROGRAMNAME, '(Unknown Program)'), COALESCE(t.TEAMNAME, '(Unknown Team)'), COALESCE(g.GROUPNAME, '(Unmapped Application)')
        """
        df_missing = fetch_df(missing_sql, scope_params)
    except Exception as e:
        df_missing = pd.DataFrame()
        st.warning(f"Missing-data overview failed: {e}")

    if df_missing is not None and not df_missing.empty:
        # Pre-fetch detailed missing feature IDs for clickable ADO links
        org_default, proj_default = ado_org_for_links, ado_proj_for_links
        try:
            detail_sql = f"""
            SELECT
              af.FEATURE_ID,
              COALESCE(p.PROGRAMNAME, '(Unknown Program)') AS PROGRAM,
              COALESCE(t.TEAMNAME, '(Unknown Team)')       AS TEAM,
              COALESCE(g.GROUPNAME, '(Unmapped Application)') AS APP_GROUP,
              CASE WHEN mt.TEAMID IS NULL THEN 1 ELSE 0 END AS MISSING_TEAM,
              CASE WHEN g.GROUPID IS NULL THEN 1 ELSE 0 END AS MISSING_APP_GROUP
            FROM ADO_FEATURES af
            LEFT JOIN MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp ON UPPER(mp.ADO_PROGRAM) = UPPER(af.PROGRAM_RAW)
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = mp.PROGRAMID
            LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM mt ON mt.ADO_TEAM_KEY = af.TEAM_VARIANT_KEY
            LEFT JOIN TEAMS t ON t.TEAMID = mt.TEAMID
            LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag ON mag.ADO_APP = af.APP_NAME_RAW
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = mag.APP_GROUP
            WHERE 1=1
              {prog_filter_clause}
              {team_filter_clause}
              {group_filter_clause}
              {year_filter_af}
              {pi_filter_af}
              AND (mt.TEAMID IS NULL OR g.GROUPID IS NULL)
            """
            df_missing_detail = fetch_df(detail_sql, scope_params)
        except Exception:
            df_missing_detail = pd.DataFrame()

        # Totals within current scope
        try:
            scope_total_df = fetch_df(
                f"""
                SELECT COUNT(*) AS N
                FROM VW_TCO_FEATURE_DEMAND d
                LEFT JOIN TEAMS t ON t.TEAMID = d.TEAMID
                LEFT JOIN PROGRAMS p ON p.PROGRAMID = d.PROGRAMID
                LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = d.GROUPID
                WHERE 1=1
                  {prog_filter_clause}
                  {team_filter_clause}
                  {group_filter_clause}
                  {year_filter_clause}
                  {pi_filter_clause}
                """,
                scope_params,
            )
            total_features_scope = int(scope_total_df.iloc[0]["N"]) if scope_total_df is not None and not scope_total_df.empty else 0
        except Exception:
            total_features_scope = 0

        total_missing_team = int(pd.to_numeric(df_missing["MISSING_TEAM"], errors="coerce").fillna(0).sum())
        total_missing_app = int(pd.to_numeric(df_missing["MISSING_APP_GROUP"], errors="coerce").fillna(0).sum())
        total_missing_rate = 0
        total_features_all = int(pd.to_numeric(df_missing["FEATURES"], errors="coerce").fillna(0).sum())
        denom = total_features_all if total_features_all else 1
        pct_team_ok = max(0.0, (1 - (total_missing_team / denom)) * 100)
        pct_app_ok = max(0.0, (1 - (total_missing_app / denom)) * 100)
        pct_rate_ok = 0.0
        if total_programs > 0:
            pct_prog_ok = 100.0 if missing_programs == 0 else max(0.0, (1 - (missing_programs / total_programs)) * 100)
        else:
            pct_prog_ok = 100.0  # no programs in scope ⇒ treat as fully mapped to avoid 0% confusion

        def _pct_color(pct: float) -> str:
            if pct >= 100.0:
                return "#2ecc71"  # green
            if pct >= 70.0:
                return "#f1c40f"  # yellow
            return "#e74c3c"     # red

        # Allow clicking KPIs to filter detail tables
        colProg, colX, colY, colClear = st.columns([1, 1, 1, 0.7])

        def _kpi_button(col, key: str, label: str, missing_count: int, pct_ok: float, suffix: str):
            clicked = col.button(f"{label}: {missing_count:,}", key=key, use_container_width=True)
            col.markdown(
                f"""
                <div style="margin-top:6px;">
                  <span style="background:{_pct_color(pct_ok)}; color:#111; padding:4px 10px; border-radius:999px; font-weight:700; font-size:13px;">
                    {pct_ok:,.1f}% {suffix}
                  </span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            return clicked

        if _kpi_button(colProg, "btn_missing_program", "Missing Program mapping", missing_programs, pct_prog_ok, "mapped"):
            st.session_state["recon_missing_filter"] = "PROGRAM"
            st.rerun()
        if _kpi_button(colX, "btn_missing_team", "Missing Team mapping", total_missing_team, pct_team_ok, "mapped"):
            st.session_state["recon_missing_filter"] = "TEAM"
            st.rerun()
        if _kpi_button(colY, "btn_missing_app", "Missing Application mapping", total_missing_app, pct_app_ok, "mapped"):
            st.session_state["recon_missing_filter"] = "APP"
            st.rerun()
        if colClear.button("Clear KPI filter", key="btn_clear_missing_filter", use_container_width=True):
            st.session_state["recon_missing_filter"] = ""
            st.rerun()

        st.markdown("#### Programs/Teams/Applications with missing data")
        df_show = df_missing.copy()
        # Attach ADO query links where org/project and feature IDs are available
        feature_lookup: Dict[Tuple[str, str, str], List[Any]] = {}
        if df_missing_detail is not None and not df_missing_detail.empty:
            try:
                grouped = df_missing_detail.groupby(["PROGRAM", "TEAM", "APP_GROUP"], dropna=False)["FEATURE_ID"].apply(list).reset_index()
                for _, r in grouped.iterrows():
                    key = (str(r["PROGRAM"]), str(r["TEAM"]), str(r["APP_GROUP"]))
                    feature_lookup[key] = list(r["FEATURE_ID"])
            except Exception:
                feature_lookup = {}
        org_default, proj_default = ado_org_for_links, ado_proj_for_links
        df_show["FEATURES_LINK"] = df_show.apply(
            lambda r: _ado_query_url(
                org_default,
                proj_default,
                feature_lookup.get((str(r["PROGRAM"]), str(r["TEAM"]), str(r["APP_GROUP"])), []),
            ),
            axis=1,
        )

        def _link_or_count(r):
            url = r.get("FEATURES_LINK")
            cnt = int(r.get("FEATURES") or 0)
            if url:
                return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{cnt}</a>'
            return str(cnt)

        df_show["FEATURES"] = df_show.apply(_link_or_count, axis=1)
        display_cols = ["PROGRAM", "TEAM", "APP_GROUP", "FEATURES", "MISSING_TEAM", "MISSING_APP_GROUP"]
        if org_default and proj_default:
            st.markdown(df_show[display_cols].to_html(index=False, escape=False), unsafe_allow_html=True)
        else:
            st.caption("Set ADO org/project on the ADO tab to enable clickable counts.")
            st.dataframe(
                df_show[display_cols],
                use_container_width=True,
                height=280,
                column_config={
                    "PROGRAM": "Program",
                    "TEAM": "ADO Team",
                    "APP_GROUP": "Application",
                    "FEATURES": st.column_config.NumberColumn("Features", format="%d"),
                    "MISSING_TEAM": st.column_config.NumberColumn("Missing Team", format="%d"),
                    "MISSING_APP_GROUP": st.column_config.NumberColumn("Missing Application", format="%d"),
                },
            )
    else:
        st.info("No missing mapping issues detected in the current scope.")

    # Unmapped teams table respects filters
    try:
        unmapped_team = fetch_df(
            f"""
            SELECT af.TEAM_RAW, COUNT(*) AS N
            FROM VW_TCO_FEATURE_DEMAND d
            LEFT JOIN ADO_FEATURES af ON af.FEATURE_ID = d.FEATURE_ID
            LEFT JOIN TEAMS t ON t.TEAMID = d.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = d.PROGRAMID
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = d.GROUPID
            WHERE d.TEAMID IS NULL
              {prog_filter_clause}
              {team_filter_clause}
              {group_filter_clause}
              {year_filter_clause}
              {pi_filter_clause}
              {missing_filter_clause}
            GROUP BY af.TEAM_RAW
            ORDER BY N DESC
            """,
            scope_params,
        )
    except Exception:
        unmapped_team = pd.DataFrame()

    st.markdown("#### Unmapped ADO Teams (with counts)")
    st.dataframe(unmapped_team, use_container_width=True, height=220)
