#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.mssql_backend import (
    fetch_df,
    execute,
    ensure_analytics_views_ok,
    bump_data_version,
    recompute_team_composition_from_headcount_year,
    recompute_program_composition_from_headcount_year,
)


def run_refresh_pipeline(changed_by: str = "automation_worker") -> Dict[str, Any]:
    teams_recomputed = 0
    programs_recomputed = 0

    try:
        df_team_scope = fetch_df(
            """
            SELECT DISTINCT TEAMID, YEAR
            FROM TEAM_HEADCOUNT_HISTORY
            WHERE TEAMID IS NOT NULL AND YEAR IS NOT NULL
            UNION
            SELECT DISTINCT TEAMID, YEAR
            FROM TEAM_CONTRACTOR_HEADCOUNT
            WHERE TEAMID IS NOT NULL AND YEAR IS NOT NULL
            """
        )
        if df_team_scope is not None and not df_team_scope.empty:
            for _, r in df_team_scope.iterrows():
                team_id = str(r.get("TEAMID") or "").strip()
                year_v = pd.to_numeric(r.get("YEAR"), errors="coerce")
                if not team_id or pd.isna(year_v):
                    continue
                recompute_team_composition_from_headcount_year(
                    team_id=team_id,
                    year=int(year_v),
                    changed_by=changed_by,
                    source="AUTOMATION_REFRESH_PIPELINE",
                )
                teams_recomputed += 1
    except Exception:
        pass

    try:
        df_program_scope = fetch_df(
            """
            SELECT DISTINCT TEAMID AS PROGRAMID, YEAR
            FROM TEAM_HEADCOUNT_HISTORY
            WHERE UPPER(LTRIM(RTRIM(CLASS)))='PROGRAM'
              AND TEAMID IS NOT NULL
              AND YEAR IS NOT NULL
            """
        )
        if df_program_scope is not None and not df_program_scope.empty:
            for _, r in df_program_scope.iterrows():
                program_id = str(r.get("PROGRAMID") or "").strip()
                year_v = pd.to_numeric(r.get("YEAR"), errors="coerce")
                if not program_id or pd.isna(year_v):
                    continue
                recompute_program_composition_from_headcount_year(
                    program_id=program_id,
                    year=int(year_v),
                    changed_by=changed_by,
                    source="AUTOMATION_REFRESH_PIPELINE",
                )
                programs_recomputed += 1
    except Exception:
        pass

    try:
        execute(
            """
            ;WITH latest AS (
              SELECT
                TEAMID, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE,
                ROW_NUMBER() OVER (
                  PARTITION BY TEAMID
                  ORDER BY YEAR DESC,
                           CASE WHEN COALESCE(PI, 0) = 0 THEN 1 ELSE 0 END DESC,
                           COALESCE(PI, 0) DESC,
                           UPDATED_AT DESC
                ) AS RN
              FROM TEAM_COMPOSITION_HISTORY
            )
            UPDATE t
               SET t.TEAMFTE = l.TEAMFTE,
                   t.DELIVERY_TEAM_FTE = l.DELIVERY_TEAM_FTE,
                   t.CONTRACTOR_C_FTE = l.CONTRACTOR_C_FTE,
                   t.CONTRACTOR_CS_FTE = l.CONTRACTOR_CS_FTE,
                   t.UPDATED_AT = SYSDATETIME()
            FROM TEAMS t
            JOIN latest l ON l.TEAMID = t.TEAMID AND l.RN = 1
            """
        )
    except Exception:
        pass

    try:
        execute(
            """
            ;WITH latest AS (
              SELECT
                PROGRAMID, PROGRAMFTE,
                ROW_NUMBER() OVER (
                  PARTITION BY PROGRAMID
                  ORDER BY YEAR DESC,
                           CASE WHEN COALESCE(PI, 0) = 0 THEN 1 ELSE 0 END DESC,
                           COALESCE(PI, 0) DESC,
                           UPDATED_AT DESC
                ) AS RN
              FROM PROGRAM_COMPOSITION_HISTORY
            )
            UPDATE p
               SET p.PROGRAMFTE = l.PROGRAMFTE,
                   p.UPDATED_AT = SYSDATETIME()
            FROM PROGRAMS p
            JOIN latest l ON l.PROGRAMID = p.PROGRAMID AND l.RN = 1
            """
        )
    except Exception:
        pass

    # Freshness contract: successful pipeline refresh must advance DATA_VERSION
    # so cache_data_portfolio readers invalidate deterministically across sessions.
    ensure_analytics_views_ok()
    bump_data_version("automation_refresh_pipeline")
    return {
        "teams_recomputed": teams_recomputed,
        "programs_recomputed": programs_recomputed,
    }


def main() -> int:
    out = run_refresh_pipeline()
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
