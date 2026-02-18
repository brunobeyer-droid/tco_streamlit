from __future__ import annotations

import hashlib
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from db import execute, fetch_df, _fq
from utils.admin_safety import render_preview_apply_audit_block
from utils.admin_shell import render_empty_state, render_section, render_perf_info

# Raw SQL console helpers.
# To add a query library entry, append to _QUERY_LIBRARY with name/description/sql/requires_tables.


def _db_label() -> str:
    try:
        df = fetch_df("SELECT DB_NAME() AS DB_NAME")
        if isinstance(df, pd.DataFrame) and not df.empty:
            name = str(df.iloc[0].get("DB_NAME") or "").strip()
            if name:
                return name
    except Exception:
        pass
    return "Unknown"


def _env_label() -> str:
    for key in ("ENV", "ENVIRONMENT", "APP_ENV"):
        val = os.getenv(key)
        if val:
            return val
    return ""


def _is_production_like() -> bool:
    env = str(_env_label() or "").strip().lower()
    if env in {"prod", "production", "live"}:
        return True
    return str(os.getenv("RAW_SQL_PROD_MODE", "0")).strip().lower() in {"1", "true", "yes", "on"}


def _write_sql_enabled() -> bool:
    # Secure-by-default: in production-like envs, writes require explicit opt-in.
    if _is_production_like():
        return str(os.getenv("ENABLE_RAW_SQL_WRITES", "0")).strip().lower() in {"1", "true", "yes", "on"}
    return True


def _table_exists(name: str) -> bool:
    try:
        df = fetch_df(
            "SELECT COUNT(*) CNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = %s",
            (name,),
        )
        return int(df.iloc[0]["CNT"]) > 0 if isinstance(df, pd.DataFrame) and not df.empty else False
    except Exception:
        return False


def _view_exists(name: str) -> bool:
    try:
        df = fetch_df(
            "SELECT COUNT(*) CNT FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = %s",
            (name,),
        )
        return int(df.iloc[0]["CNT"]) > 0 if isinstance(df, pd.DataFrame) and not df.empty else False
    except Exception:
        return False


def _query_library() -> List[Dict[str, Any]]:
    return [
        {
            "name": "Recent admin audit (last 50)",
            "description": "Latest admin audit rows.",
            "sql": f"SELECT TOP 50 * FROM {_fq('ADMIN_AUDIT_LOG')} ORDER BY CREATED_AT DESC",
            "requires_tables": ["ADMIN_AUDIT_LOG"],
        },
        {
            "name": "Unmapped ADO apps (count)",
            "description": "Count of unmapped ADO app names.",
            "sql": f"""SELECT COUNT(*) AS CNT
FROM {_fq('MAP_ADO_APP_TO_TCO_GROUP')}
WHERE APP_GROUP IS NULL OR LTRIM(RTRIM(APP_GROUP)) = ''""",
            "requires_tables": ["MAP_ADO_APP_TO_TCO_GROUP"],
        },
        {
            "name": "Largest tables (row counts)",
            "description": "Top tables by row count.",
            "sql": "SELECT TOP 20 TABLE_NAME, TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_ROWS DESC",
            "requires_tables": [],
        },
        {
            "name": "Views health (VW_*)",
            "description": "List VW_* views present.",
            "sql": "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME LIKE 'VW_%' ORDER BY TABLE_NAME",
            "requires_tables": [],
        },
        {
            "name": "Programs without teams",
            "description": "Programs with no team rows.",
            "sql": f"""SELECT p.PROGRAMNAME
FROM {_fq('PROGRAMS')} p
LEFT JOIN {_fq('TEAMS')} t ON t.PROGRAMID = p.PROGRAMID
WHERE t.TEAMID IS NULL
ORDER BY p.PROGRAMNAME""",
            "requires_tables": ["PROGRAMS", "TEAMS"],
        },
        {
            "name": "Teams without applications",
            "description": "Teams with no application groups.",
            "sql": f"""SELECT t.TEAMNAME
FROM {_fq('TEAMS')} t
LEFT JOIN {_fq('APPLICATION_GROUPS')} g ON g.TEAMID = t.TEAMID
WHERE g.GROUPID IS NULL
ORDER BY t.TEAMNAME""",
            "requires_tables": ["TEAMS", "APPLICATION_GROUPS"],
        },
        {
            "name": "Latest invoice updates",
            "description": "Most recently updated invoices.",
            "sql": f"""SELECT TOP 50 INVOICEID, UPDATED_AT, UPDATED_BY
FROM {_fq('INVOICES')}
ORDER BY UPDATED_AT DESC""",
            "requires_tables": ["INVOICES"],
        },
        {
            "name": "ADO features by program (top 20)",
            "description": "Feature count by ADO program.",
            "sql": f"""SELECT TOP 20 PROGRAM_RAW, COUNT(*) AS FEATURE_COUNT
FROM {_fq('ADO_FEATURES')}
GROUP BY PROGRAM_RAW
ORDER BY FEATURE_COUNT DESC""",
            "requires_tables": ["ADO_FEATURES"],
        },
        {
            "name": "Capacity by portfolio/program/team (yearly)",
            "description": (
                "Active portfolio capacity table by year with scope rows: "
                "PORTFOLIO, PROGRAM, TEAM."
            ),
            "sql": f"""
WITH staff AS (
  SELECT
    h.TEAMID,
    TRY_CONVERT(INT, h.YEAR) AS YEAR,
    TRY_CONVERT(INT, h.PI) AS PI,
    UPPER(LTRIM(RTRIM(COALESCE(h.CLASS, '')))) AS CLASS,
    SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0.0)) AS HEADCOUNT
  FROM {_fq('VW_TEAM_HEADCOUNT_EFFECTIVE')} h
  WHERE TRY_CONVERT(INT, h.YEAR) IS NOT NULL
    AND TRY_CONVERT(INT, h.PI) BETWEEN 1 AND 4
  GROUP BY
    h.TEAMID,
    TRY_CONVERT(INT, h.YEAR),
    TRY_CONVERT(INT, h.PI),
    UPPER(LTRIM(RTRIM(COALESCE(h.CLASS, ''))))
),
contractors AS (
  SELECT
    h.TEAMID,
    TRY_CONVERT(INT, h.YEAR) AS YEAR,
    TRY_CONVERT(INT, h.PI) AS PI,
    UPPER(LTRIM(RTRIM(COALESCE(h.CLASS, '')))) AS CLASS,
    SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0.0)) AS HEADCOUNT
  FROM {_fq('VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE')} h
  WHERE TRY_CONVERT(INT, h.YEAR) IS NOT NULL
    AND TRY_CONVERT(INT, h.PI) BETWEEN 1 AND 4
  GROUP BY
    h.TEAMID,
    TRY_CONVERT(INT, h.YEAR),
    TRY_CONVERT(INT, h.PI),
    UPPER(LTRIM(RTRIM(COALESCE(h.CLASS, ''))))
),
pi_level AS (
  SELECT
    COALESCE(s.TEAMID, c.TEAMID) AS TEAMID,
    COALESCE(s.YEAR, c.YEAR) AS YEAR,
    COALESCE(s.PI, c.PI) AS PI,
    SUM(CASE WHEN s.CLASS = 'TEAM' THEN COALESCE(s.HEADCOUNT, 0.0) ELSE 0.0 END) AS TEAM_OVERHEAD_FTE,
    SUM(CASE WHEN s.CLASS = 'DELIVERY' THEN COALESCE(s.HEADCOUNT, 0.0) ELSE 0.0 END) AS DELIVERY_FTE,
    SUM(CASE WHEN c.CLASS = 'CONTRACTOR_C' THEN COALESCE(c.HEADCOUNT, 0.0) ELSE 0.0 END) AS CONTRACTOR_C_FTE,
    SUM(CASE WHEN c.CLASS = 'CONTRACTOR_CS' THEN COALESCE(c.HEADCOUNT, 0.0) ELSE 0.0 END) AS CONTRACTOR_CS_FTE
  FROM staff s
  FULL OUTER JOIN contractors c
    ON c.TEAMID = s.TEAMID
   AND c.YEAR = s.YEAR
   AND c.PI = s.PI
  GROUP BY
    COALESCE(s.TEAMID, c.TEAMID),
    COALESCE(s.YEAR, c.YEAR),
    COALESCE(s.PI, c.PI)
),
team_year AS (
  SELECT
    pl.YEAR,
    COALESCE(t.PROGRAMID, '(Unassigned)') AS PROGRAMID,
    COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAMNAME)), ''), '(Unassigned)') AS PROGRAMNAME,
    COALESCE(pl.TEAMID, '(Unassigned)') AS TEAMID,
    COALESCE(NULLIF(LTRIM(RTRIM(t.TEAMNAME)), ''), '(Unassigned)') AS TEAMNAME,
    COUNT(DISTINCT pl.PI) AS PIS_WITH_DATA,
    AVG(COALESCE(pl.TEAM_OVERHEAD_FTE, 0.0)) AS TEAM_OVERHEAD_FTE_PER_PI,
    AVG(COALESCE(pl.DELIVERY_FTE, 0.0)) AS DELIVERY_FTE_PER_PI,
    AVG(COALESCE(pl.CONTRACTOR_C_FTE, 0.0)) AS CONTRACTOR_C_FTE_PER_PI,
    AVG(COALESCE(pl.CONTRACTOR_CS_FTE, 0.0)) AS CONTRACTOR_CS_FTE_PER_PI,
    AVG(COALESCE(pl.DELIVERY_FTE, 0.0) + COALESCE(pl.CONTRACTOR_C_FTE, 0.0) + COALESCE(pl.CONTRACTOR_CS_FTE, 0.0)) AS DELIVERY_CAPACITY_FTE_PER_PI
  FROM pi_level pl
  LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = pl.TEAMID
  LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
  GROUP BY
    pl.YEAR,
    COALESCE(t.PROGRAMID, '(Unassigned)'),
    COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAMNAME)), ''), '(Unassigned)'),
    COALESCE(pl.TEAMID, '(Unassigned)'),
    COALESCE(NULLIF(LTRIM(RTRIM(t.TEAMNAME)), ''), '(Unassigned)')
)
,
scoped AS (
  SELECT
    DB_NAME() AS PORTFOLIO_DB,
    'PORTFOLIO' AS SCOPE_LEVEL,
    YEAR,
    '(All Programs)' AS PROGRAMNAME,
    '(All Teams)' AS TEAMNAME,
    COUNT(*) AS TEAMS_IN_SCOPE,
    SUM(TEAM_OVERHEAD_FTE_PER_PI) AS TEAM_OVERHEAD_FTE_PER_PI,
    SUM(DELIVERY_FTE_PER_PI) AS DELIVERY_FTE_PER_PI,
    SUM(CONTRACTOR_C_FTE_PER_PI) AS CONTRACTOR_C_FTE_PER_PI,
    SUM(CONTRACTOR_CS_FTE_PER_PI) AS CONTRACTOR_CS_FTE_PER_PI,
    SUM(DELIVERY_CAPACITY_FTE_PER_PI) AS DELIVERY_CAPACITY_FTE_PER_PI,
    CAST(1 AS INT) AS SCOPE_SORT
  FROM team_year
  GROUP BY YEAR

  UNION ALL

  SELECT
    DB_NAME() AS PORTFOLIO_DB,
    'PROGRAM' AS SCOPE_LEVEL,
    YEAR,
    PROGRAMNAME,
    '(All Teams)' AS TEAMNAME,
    COUNT(*) AS TEAMS_IN_SCOPE,
    SUM(TEAM_OVERHEAD_FTE_PER_PI) AS TEAM_OVERHEAD_FTE_PER_PI,
    SUM(DELIVERY_FTE_PER_PI) AS DELIVERY_FTE_PER_PI,
    SUM(CONTRACTOR_C_FTE_PER_PI) AS CONTRACTOR_C_FTE_PER_PI,
    SUM(CONTRACTOR_CS_FTE_PER_PI) AS CONTRACTOR_CS_FTE_PER_PI,
    SUM(DELIVERY_CAPACITY_FTE_PER_PI) AS DELIVERY_CAPACITY_FTE_PER_PI,
    CAST(2 AS INT) AS SCOPE_SORT
  FROM team_year
  GROUP BY YEAR, PROGRAMNAME

  UNION ALL

  SELECT
    DB_NAME() AS PORTFOLIO_DB,
    'TEAM' AS SCOPE_LEVEL,
    YEAR,
    PROGRAMNAME,
    TEAMNAME,
    1 AS TEAMS_IN_SCOPE,
    TEAM_OVERHEAD_FTE_PER_PI,
    DELIVERY_FTE_PER_PI,
    CONTRACTOR_C_FTE_PER_PI,
    CONTRACTOR_CS_FTE_PER_PI,
    DELIVERY_CAPACITY_FTE_PER_PI,
    CAST(3 AS INT) AS SCOPE_SORT
  FROM team_year
)
SELECT
  PORTFOLIO_DB,
  SCOPE_LEVEL,
  YEAR,
  PROGRAMNAME,
  TEAMNAME,
  TEAMS_IN_SCOPE,
  TEAM_OVERHEAD_FTE_PER_PI,
  DELIVERY_FTE_PER_PI,
  CONTRACTOR_C_FTE_PER_PI,
  CONTRACTOR_CS_FTE_PER_PI,
  DELIVERY_CAPACITY_FTE_PER_PI
FROM scoped
ORDER BY
  YEAR DESC,
  SCOPE_SORT,
  PROGRAMNAME,
  TEAMNAME
""",
            "requires_tables": [],
        },
        {
            "name": "Velocity baseline by portfolio/program/team (year+PI)",
            "description": (
                "Delivery velocity baseline table by Year/PI with scope rows: "
                "PORTFOLIO, PROGRAM, TEAM."
            ),
            "sql": f"""
WITH src AS (
  SELECT
    TRY_CONVERT(INT, v.YEAR) AS YEAR,
    TRY_CONVERT(INT, v.PI) AS PI,
    COALESCE(
      NULLIF(LTRIM(RTRIM(v.PI_LABEL)), ''),
      CONCAT(TRY_CONVERT(INT, v.YEAR), ' PI', TRY_CONVERT(INT, v.PI))
    ) AS PI_LABEL,
    COALESCE(NULLIF(LTRIM(RTRIM(v.PROGRAMNAME)), ''), '(Unassigned)') AS PROGRAMNAME,
    COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMNAME)), ''), '(Unassigned)') AS TEAMNAME,
    TRY_CONVERT(FLOAT, v.EFFECTIVE_BASELINE_POINTS) AS VELOCITY_PTS_PER_PI,
    TRY_CONVERT(FLOAT, v.BASELINE_PREV) AS BASELINE_PREV,
    UPPER(LTRIM(RTRIM(COALESCE(v.BASELINE_SOURCE, 'GLOBAL')))) AS BASELINE_SOURCE,
    TRY_CONVERT(INT, COALESCE(v.BASELINE_CHANGED_FLAG, 0)) AS BASELINE_CHANGED_FLAG,
    TRY_CONVERT(FLOAT, v.PI_POINTS_DONE) AS PI_POINTS_DONE,
    TRY_CONVERT(FLOAT, v.SAMPLE_PIS) AS SAMPLE_PIS
  FROM {_fq('VW_TCO_TEAM_VELOCITY_BASELINE')} v
  WHERE TRY_CONVERT(INT, v.YEAR) IS NOT NULL
    AND TRY_CONVERT(INT, v.PI) BETWEEN 1 AND 4
),
scoped AS (
  SELECT
    DB_NAME() AS PORTFOLIO_DB,
    'PORTFOLIO' AS SCOPE_LEVEL,
    s.YEAR,
    s.PI,
    MAX(s.PI_LABEL) AS PI_LABEL,
    '(All Programs)' AS PROGRAMNAME,
    '(All Teams)' AS TEAMNAME,
    COUNT(DISTINCT s.TEAMNAME) AS TEAMS_IN_SCOPE,
    COUNT(*) AS ROWS_IN_SCOPE,
    SUM(COALESCE(s.PI_POINTS_DONE, 0.0)) AS PI_POINTS_DONE,
    CAST(
      CASE
        WHEN SUM(COALESCE(s.PI_POINTS_DONE, 0.0)) > 0
          THEN SUM(COALESCE(s.VELOCITY_PTS_PER_PI, 0.0) * COALESCE(s.PI_POINTS_DONE, 0.0))
               / NULLIF(SUM(COALESCE(s.PI_POINTS_DONE, 0.0)), 0)
        ELSE AVG(COALESCE(s.VELOCITY_PTS_PER_PI, 0.0))
      END AS FLOAT
    ) AS VELOCITY_PTS_PER_PI,
    CAST(
      CASE
        WHEN SUM(COALESCE(s.PI_POINTS_DONE, 0.0)) > 0
          THEN SUM(COALESCE(s.BASELINE_PREV, 0.0) * COALESCE(s.PI_POINTS_DONE, 0.0))
               / NULLIF(SUM(COALESCE(s.PI_POINTS_DONE, 0.0)), 0)
        ELSE AVG(s.BASELINE_PREV)
      END AS FLOAT
    ) AS BASELINE_PREV,
    CASE
      WHEN COUNT(DISTINCT COALESCE(NULLIF(s.BASELINE_SOURCE, ''), 'GLOBAL')) > 1 THEN 'MIXED'
      ELSE MAX(COALESCE(NULLIF(s.BASELINE_SOURCE, ''), 'GLOBAL'))
    END AS BASELINE_SOURCE,
    MAX(COALESCE(s.BASELINE_CHANGED_FLAG, 0)) AS BASELINE_CHANGED_FLAG,
    AVG(COALESCE(s.SAMPLE_PIS, 0.0)) AS SAMPLE_PIS_AVG,
    CAST(1 AS INT) AS SCOPE_SORT
  FROM src s
  GROUP BY s.YEAR, s.PI

  UNION ALL

  SELECT
    DB_NAME() AS PORTFOLIO_DB,
    'PROGRAM' AS SCOPE_LEVEL,
    s.YEAR,
    s.PI,
    MAX(s.PI_LABEL) AS PI_LABEL,
    s.PROGRAMNAME,
    '(All Teams)' AS TEAMNAME,
    COUNT(DISTINCT s.TEAMNAME) AS TEAMS_IN_SCOPE,
    COUNT(*) AS ROWS_IN_SCOPE,
    SUM(COALESCE(s.PI_POINTS_DONE, 0.0)) AS PI_POINTS_DONE,
    CAST(
      CASE
        WHEN SUM(COALESCE(s.PI_POINTS_DONE, 0.0)) > 0
          THEN SUM(COALESCE(s.VELOCITY_PTS_PER_PI, 0.0) * COALESCE(s.PI_POINTS_DONE, 0.0))
               / NULLIF(SUM(COALESCE(s.PI_POINTS_DONE, 0.0)), 0)
        ELSE AVG(COALESCE(s.VELOCITY_PTS_PER_PI, 0.0))
      END AS FLOAT
    ) AS VELOCITY_PTS_PER_PI,
    CAST(
      CASE
        WHEN SUM(COALESCE(s.PI_POINTS_DONE, 0.0)) > 0
          THEN SUM(COALESCE(s.BASELINE_PREV, 0.0) * COALESCE(s.PI_POINTS_DONE, 0.0))
               / NULLIF(SUM(COALESCE(s.PI_POINTS_DONE, 0.0)), 0)
        ELSE AVG(s.BASELINE_PREV)
      END AS FLOAT
    ) AS BASELINE_PREV,
    CASE
      WHEN COUNT(DISTINCT COALESCE(NULLIF(s.BASELINE_SOURCE, ''), 'GLOBAL')) > 1 THEN 'MIXED'
      ELSE MAX(COALESCE(NULLIF(s.BASELINE_SOURCE, ''), 'GLOBAL'))
    END AS BASELINE_SOURCE,
    MAX(COALESCE(s.BASELINE_CHANGED_FLAG, 0)) AS BASELINE_CHANGED_FLAG,
    AVG(COALESCE(s.SAMPLE_PIS, 0.0)) AS SAMPLE_PIS_AVG,
    CAST(2 AS INT) AS SCOPE_SORT
  FROM src s
  GROUP BY s.YEAR, s.PI, s.PROGRAMNAME

  UNION ALL

  SELECT
    DB_NAME() AS PORTFOLIO_DB,
    'TEAM' AS SCOPE_LEVEL,
    s.YEAR,
    s.PI,
    s.PI_LABEL,
    s.PROGRAMNAME,
    s.TEAMNAME,
    1 AS TEAMS_IN_SCOPE,
    1 AS ROWS_IN_SCOPE,
    COALESCE(s.PI_POINTS_DONE, 0.0) AS PI_POINTS_DONE,
    s.VELOCITY_PTS_PER_PI,
    s.BASELINE_PREV,
    COALESCE(NULLIF(s.BASELINE_SOURCE, ''), 'GLOBAL') AS BASELINE_SOURCE,
    COALESCE(s.BASELINE_CHANGED_FLAG, 0) AS BASELINE_CHANGED_FLAG,
    COALESCE(s.SAMPLE_PIS, 0.0) AS SAMPLE_PIS_AVG,
    CAST(3 AS INT) AS SCOPE_SORT
  FROM src s
)
SELECT
  PORTFOLIO_DB,
  SCOPE_LEVEL,
  YEAR,
  PI,
  PI_LABEL,
  PROGRAMNAME,
  TEAMNAME,
  TEAMS_IN_SCOPE,
  ROWS_IN_SCOPE,
  PI_POINTS_DONE,
  VELOCITY_PTS_PER_PI,
  BASELINE_PREV,
  BASELINE_SOURCE,
  BASELINE_CHANGED_FLAG,
  SAMPLE_PIS_AVG
FROM scoped
ORDER BY
  YEAR DESC,
  PI DESC,
  SCOPE_SORT,
  PROGRAMNAME,
  TEAMNAME
""",
            "requires_tables": [],
        },
    ]


def _filter_library(library: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered = []
    for entry in library:
        req = entry.get("requires_tables") or []
        ok = True
        for t in req:
            if not _table_exists(t):
                ok = False
                break
        if ok:
            filtered.append(entry)
    return filtered


def _classify_sql(sql: str) -> str:
    s = sql.strip().lstrip(";")
    if not s:
        return "UNKNOWN"
    tokens = s.split()
    first = tokens[0].upper()
    if first == "WITH" and len(tokens) > 1:
        first = tokens[1].upper()
    if first in ("SELECT", "WITH"):
        return "SELECT"
    if first in ("INSERT", "UPDATE", "DELETE", "MERGE"):
        return "WRITE"
    if first in ("CREATE", "ALTER", "DROP", "TRUNCATE"):
        return "DDL"
    return "OTHER"


def _apply_limit(sql: str, limit: Optional[int]) -> str:
    if not limit:
        return sql
    s = sql.strip()
    if not s.upper().startswith("SELECT"):
        return sql
    upper = s.upper()
    if " TOP " in upper:
        return sql
    return f"SELECT TOP {int(limit)} " + s[len("SELECT "):]


def _estimate_write(sql: str) -> Dict[str, Any]:
    s = sql.strip()
    counts: Dict[str, Any] = {}
    msg = "Impact estimate unavailable."
    try:
        upper = s.upper()
        if upper.startswith("UPDATE"):
            table = s.split()[1]
            where_idx = upper.find(" WHERE ")
            where = s[where_idx:] if where_idx >= 0 else ""
            df_cnt = fetch_df(f"SELECT COUNT(*) CNT FROM {table} {where}")
            if isinstance(df_cnt, pd.DataFrame) and not df_cnt.empty:
                counts["rows_affected_est"] = int(df_cnt.iloc[0]["CNT"])
                msg = "Estimated rows to update."
        elif upper.startswith("DELETE"):
            parts = s.split()
            table = parts[2] if len(parts) > 2 else ""
            where_idx = upper.find(" WHERE ")
            where = s[where_idx:] if where_idx >= 0 else ""
            df_cnt = fetch_df(f"SELECT COUNT(*) CNT FROM {table} {where}")
            if isinstance(df_cnt, pd.DataFrame) and not df_cnt.empty:
                counts["rows_affected_est"] = int(df_cnt.iloc[0]["CNT"])
                msg = "Estimated rows to delete."
        elif upper.startswith("INSERT"):
            msg = "Insert detected; unable to estimate rows."
        else:
            msg = "DDL detected; impact estimate unavailable."
    except Exception:
        msg = "Could not estimate impact."
    return {"ok": True, "message": msg, "counts": counts}


def _feature_demand_view_health() -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "view_exists": False,
        "marker": "(unknown)",
        "row_count": None,
        "baseline_rows": None,
    }
    try:
        df_exists = fetch_df(
            "SELECT 1 AS OK FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = %s",
            ("VW_TCO_FEATURE_DEMAND",),
        )
        out["view_exists"] = bool(isinstance(df_exists, pd.DataFrame) and not df_exists.empty)
    except Exception:
        out["view_exists"] = False

    try:
        mod = fetch_df(
            """
            SELECT TOP 1 m.definition AS DEFN
            FROM sys.sql_modules m
            WHERE m.object_id = OBJECT_ID('dbo.VW_TCO_FEATURE_DEMAND')
            """,
            None,
        )
        defn = str(mod.iloc[0].get("DEFN") or "") if isinstance(mod, pd.DataFrame) and not mod.empty else ""
        for marker in ("TCO_FEATURE_DEMAND_VIEW_V5", "TCO_FEATURE_DEMAND_VIEW_V4", "TCO_FEATURE_DEMAND_VIEW_V3"):
            if marker in defn:
                out["marker"] = marker
                break
    except Exception:
        pass

    try:
        cnt = fetch_df("SELECT COUNT(*) AS N FROM dbo.VW_TCO_FEATURE_DEMAND", None)
        if isinstance(cnt, pd.DataFrame) and not cnt.empty:
            out["row_count"] = int(cnt.iloc[0].get("N") or 0)
    except Exception:
        out["row_count"] = None

    try:
        vcnt = fetch_df("SELECT COUNT(*) AS N FROM dbo.VW_TCO_TEAM_VELOCITY_BASELINE", None)
        if isinstance(vcnt, pd.DataFrame) and not vcnt.empty:
            out["baseline_rows"] = int(vcnt.iloc[0].get("N") or 0)
    except Exception:
        out["baseline_rows"] = None
    return out


def _render_feature_demand_rebuild_block() -> None:
    render_section("Targeted View Repair")
    st.caption(
        "Use this when Welcome/Dashboard/Insights look empty due to stale feature-demand view definition. "
        "This action forces a rebuild of `VW_TCO_TEAM_VELOCITY_BASELINE` and `VW_TCO_FEATURE_DEMAND`."
    )
    st.caption(
        "Notes: keeps canonical cost logic unchanged; only view definitions are refreshed and DATA_VERSION is bumped."
    )

    health = _feature_demand_view_health()
    chips = [
        f"Feature demand marker: {health.get('marker')}",
        f"Feature demand rows: {health.get('row_count') if health.get('row_count') is not None else 'n/a'}",
        f"Velocity baseline rows: {health.get('baseline_rows') if health.get('baseline_rows') is not None else 'n/a'}",
    ]
    st.info(" | ".join(chips))

    context = {
        "target_views": ["VW_TCO_TEAM_VELOCITY_BASELINE", "VW_TCO_FEATURE_DEMAND"],
        "force_feature_demand": True,
    }

    def _preview() -> Dict[str, Any]:
        before = _feature_demand_view_health()
        return {
            "ok": True,
            "message": "Ready to force-rebuild Derived FTE views.",
            "counts": {
                "feature_demand_rows_before": before.get("row_count"),
                "velocity_baseline_rows_before": before.get("baseline_rows"),
            },
            "details": before,
        }

    def _apply() -> Dict[str, Any]:
        from db.mssql_backend import (
            ensure_tco_feature_demand_view,
            ensure_tco_team_velocity_baseline_view,
        )
        from core.freshness import post_write_refresh

        start = time.perf_counter()
        ensure_tco_team_velocity_baseline_view()
        ensure_tco_feature_demand_view(force=True)
        elapsed = round(time.perf_counter() - start, 2)
        after = _feature_demand_view_health()
        post_write_refresh("admin_raw_sql_force_feature_demand_view", ensure_views=False, rerun=False, bump_version=True)
        return {
            "ok": True,
            "message": "Derived FTE views rebuilt successfully.",
            "counts": {
                "seconds": elapsed,
                "feature_demand_rows_after": after.get("row_count"),
                "velocity_baseline_rows_after": after.get("baseline_rows"),
            },
            "details": after,
        }

    render_preview_apply_audit_block(
        title="Force rebuild Derived FTE views",
        area="raw_sql",
        action_type="rebuild_views",
        entity="VW_TCO_FEATURE_DEMAND",
        context=context,
        preview_fn=_preview,
        apply_fn=_apply,
        require_typed_confirm=True,
        confirm_phrase="REBUILD",
        danger_level="danger",
    )


def render_raw_sql_tab() -> None:
    render_section("Raw SQL Console")
    db_label = _db_label()
    env_label = _env_label()
    st.info(f"Connected to: {db_label}" + (f" | {env_label}" if env_label else ""))
    st.caption(
        "Ops note: in production-like mode (`ENV=prod` or `RAW_SQL_PROD_MODE=1`), "
        "write SQL stays disabled unless `ENABLE_RAW_SQL_WRITES=1` is set."
    )
    _render_feature_demand_rebuild_block()

    render_section("Query Library")
    library = _filter_library(_query_library())
    if not library:
        render_empty_state(
            "No saved queries available.",
            "Add queries in the library list when new diagnostics are needed.",
        )
    else:
        names = [q["name"] for q in library]
        selected = st.selectbox("Load a saved query", options=[""] + names, key="sql_lib_pick")
        chosen = next((q for q in library if q["name"] == selected), None)
        if chosen:
            st.caption(chosen.get("description") or "")
            if st.button("Insert into editor", key="sql_lib_insert"):
                st.session_state["sql_editor_text"] = chosen.get("sql") or ""

    render_section("SQL Editor")
    limit_choice = st.selectbox("Row limit", options=[100, 500, 2000, "No limit"], index=1, key="sql_row_limit")
    writes_enabled = _write_sql_enabled()
    read_only_default = True if writes_enabled else True
    read_only = st.checkbox(
        "Read-only mode (SELECT only)",
        value=read_only_default,
        key="sql_readonly",
        disabled=not writes_enabled,
    )
    allow_write = st.checkbox(
        "Allow write queries",
        value=False,
        key="sql_allow_write",
        disabled=(read_only or not writes_enabled),
    )
    if not writes_enabled:
        st.warning("Write queries are disabled in production mode. Set `ENABLE_RAW_SQL_WRITES=1` for break-glass access.")
    sql_text = st.text_area(
        "SQL",
        value=st.session_state.get("sql_editor_text", ""),
        key="sql_editor_text",
        height=180,
    )
    if st.button("Clear", key="sql_clear"):
        st.session_state["sql_editor_text"] = ""
        st.rerun()

    qtype = _classify_sql(sql_text)
    st.caption(f"Detected type: {qtype}")

    render_section("Results")
    if qtype == "SELECT":
        if st.button("Run query", key="sql_run_select"):
            limit = None if limit_choice == "No limit" else int(limit_choice)
            sql_run = _apply_limit(sql_text, limit)
            start = time.perf_counter()
            try:
                df = fetch_df(sql_run)
                elapsed = time.perf_counter() - start
                render_perf_info("Query", elapsed, len(df) if isinstance(df, pd.DataFrame) else None)
                if isinstance(df, pd.DataFrame):
                    st.write(f"Rows: {len(df)} | Columns: {len(df.columns)} | Time: {elapsed:.2f}s")
                    if limit and len(df) >= limit:
                        st.warning(f"Showing first {limit} rows (limit applied).")
                    st.dataframe(df, use_container_width=True, height=320)
                    if not df.empty:
                        csv = df.to_csv(index=False)
                        st.download_button("Export CSV", data=csv, file_name="query_results.csv", mime="text/csv")
                else:
                    st.info("Query returned no rows.")
            except Exception as exc:
                st.error(f"Query failed: {exc}")
                with st.expander("Error details"):
                    st.code(str(exc))
    elif qtype in ("WRITE", "DDL", "OTHER"):
        if read_only:
            st.warning("Read-only mode is enabled. Disable it to run write queries.")
        elif not allow_write:
            st.warning("Enable 'Allow write queries' to proceed.")
        else:
            qhash = hashlib.sha256(sql_text.encode("utf-8")).hexdigest()[:12]

            def _preview() -> Dict[str, Any]:
                base = _estimate_write(sql_text)
                base["details"] = {"query": sql_text, "query_hash": qhash}
                return base

            def _apply() -> Dict[str, Any]:
                execute(sql_text)
                return {"ok": True, "message": "Write query executed.", "counts": {"query_hash": qhash}}

            render_preview_apply_audit_block(
                title="Run write query",
                area="raw_sql",
                action_type="sql_write",
                entity="database",
                context={"query_hash": qhash, "query": sql_text},
                preview_fn=_preview,
                apply_fn=_apply,
                require_typed_confirm=True,
                confirm_phrase="RUN",
                danger_level="danger",
            )
    else:
        st.info("Enter a query to see results.")
