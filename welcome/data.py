import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence

import pandas as pd

from db import _fq
from core.data import fetch_cost_lines
from welcome.state import UserScope


logger = logging.getLogger(__name__)

FetchFn = Callable[[str, Optional[Sequence]], Optional[pd.DataFrame]]


@dataclass
class CostData:
    costs: Optional[pd.DataFrame]
    effort_costs: Optional[pd.DataFrame] = None
    invoice_count: Optional[int] = None
    errors: List[str] = field(default_factory=list)


def load_cost_data(
    scope: UserScope,
    fiscal_year: int,
    fetch_df: FetchFn,
    rev: Optional[int] = None,
    *,
    cost_lines: Optional[pd.DataFrame] = None,
) -> CostData:
    """Build Welcome's cost & effort datasets for the given scope/year.

    - `cost_lines` is preferred and should come from the shared Budget context.
    - When `cost_lines` is omitted, falls back to BASELINE cost lines.
    """
    errors: List[str] = []
    try:
        df_lines = cost_lines
        if df_lines is None:
            df_lines = fetch_cost_lines(
                fiscal_year=int(fiscal_year),
                scenario="BASELINE",
                programs=tuple(scope.programs or ()),
                teams=tuple(scope.teams or ()),
                app_groups=tuple(scope.groups or ()),
                rev=rev,
            )
        if df_lines is None or df_lines.empty:
            df_cost = pd.DataFrame(columns=["YEAR", "PI", "GROUPNAME", "INVESTMENT_DIMENSION", "COST_CATEGORY", "SUBCOMPONENT", "TOTAL_COST"])
        else:
            work = df_lines.copy()
            work["YEAR"] = pd.to_numeric(work.get("YEAR"), errors="coerce").astype("Int64")
            work["PI"] = pd.to_numeric(work.get("PI"), errors="coerce").astype("Int64")
            work["GROUPNAME"] = work.get("GROUPNAME", "").fillna("").astype(str).str.strip()
            work["COST_CATEGORY"] = work.get("COST_CATEGORY", "").fillna("").astype(str).str.strip()
            work["SUBCOMPONENT"] = work.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
            work["TOTAL_COST"] = pd.to_numeric(work.get("AMOUNT"), errors="coerce").fillna(0.0)
            work["INVESTMENT_DIMENSION"] = "Not Applicable"
            df_cost = (
                work.groupby(["YEAR", "PI", "GROUPNAME", "INVESTMENT_DIMENSION", "COST_CATEGORY", "SUBCOMPONENT"], dropna=False)["TOTAL_COST"]
                .sum()
                .reset_index()
                .sort_values(["YEAR", "PI", "TOTAL_COST"], ascending=[False, False, False])
            )
    except Exception:
        logger.exception("Failed to fetch cost data", extra={"email": scope.email, "role": scope.role})
        return CostData(costs=None, effort_costs=None, errors=["Could not load cost data for your account."])

    # Demand vs cost aggregates (YEAR/GROUPNAME/PROGRAMNAME/TEAMNAME)
    df_effort = None

    # Build WHERE and params based on fiscal_year and scope
    where: List[str] = []
    params: List[object] = []

    if fiscal_year:
        where.append("YEAR = %s")
        params.append(int(fiscal_year))

    programs = [str(p).strip() for p in (scope.programs or []) if str(p).strip()]
    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(PROGRAMNAME) IN ({placeholders})")
        params.extend([p.upper() for p in programs])

    teams = [str(t).strip() for t in (scope.teams or []) if str(t).strip()]
    if teams:
        placeholders = ", ".join(["%s"] * len(teams))
        where.append(f"UPPER(TEAMNAME) IN ({placeholders})")
        params.extend([t.upper() for t in teams])

    groups = [str(g).strip() for g in (getattr(scope, "groups", None) or []) if str(g).strip()]
    if groups:
        placeholders = ", ".join(["%s"] * len(groups))
        where.append(f"UPPER(GROUPNAME) IN ({placeholders})")
        params.extend([g.upper() for g in groups])
    try:
        sql_effort = f"""
          WITH FTE_TEAM AS (
            SELECT YEAR,
                   UPPER(LTRIM(RTRIM(p.PROGRAMNAME))) AS PROG_KEY,
                   UPPER(LTRIM(RTRIM(t.TEAMNAME)))    AS TEAM_KEY,
                   COALESCE(e.TEAMFTE,0) + COALESCE(e.DELIVERY_TEAM_FTE,0) + COALESCE(e.CONTRACTOR_CS_FTE,0) + COALESCE(e.CONTRACTOR_C_FTE,0) AS HEADCOUNT
            FROM {_fq('VW_TEAM_COMPOSITION_EFFECTIVE')} e
            LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = e.TEAMID
            LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
          ),
          BASE AS (
            SELECT
              YEAR,
              GROUPNAME,
              PROGRAMNAME,
              TEAMNAME,
              UPPER(LTRIM(RTRIM(PROGRAMNAME))) AS PROG_KEY,
              UPPER(LTRIM(RTRIM(TEAMNAME)))    AS TEAM_KEY,
              SUM(AMOUNT) AS TOTAL_COST,
              SUM(COALESCE(FTE, 0)) AS TOTAL_DERIVED_FTE,
              COUNT(DISTINCT CASE WHEN ADO_FEATURE_ID IS NOT NULL THEN ADO_FEATURE_ID END) AS FEATURE_COUNT
            FROM {_fq('VW_TCO_WORKFORCE_SPLIT')}
            {('WHERE ' + ' AND '.join(where)) if where else ''}
            GROUP BY YEAR, GROUPNAME, PROGRAMNAME, TEAMNAME
          )
          SELECT
            b.YEAR,
            b.GROUPNAME,
            b.PROGRAMNAME,
            b.TEAMNAME,
            b.TOTAL_COST,
            b.TOTAL_DERIVED_FTE,
            b.FEATURE_COUNT,
            COALESCE(h.HC, 0) AS HEADCOUNT
          FROM BASE b
          LEFT JOIN (
            SELECT YEAR, PROG_KEY, TEAM_KEY, AVG(HEADCOUNT) AS HC
            FROM FTE_TEAM
            GROUP BY YEAR, PROG_KEY, TEAM_KEY
          ) h
            ON h.YEAR = b.YEAR AND h.PROG_KEY = b.PROG_KEY AND h.TEAM_KEY = b.TEAM_KEY
          ORDER BY b.YEAR DESC, b.TOTAL_COST DESC
        """
        df_effort = fetch_df(sql_effort, tuple(params) if params else None)
    except Exception:
        # Retry without headcount if column is missing
        try:
            sql_effort = f"""
              SELECT
                YEAR,
                GROUPNAME,
                PROGRAMNAME,
                TEAMNAME,
                SUM(AMOUNT) AS TOTAL_COST,
                SUM(COALESCE(FTE, 0)) AS TOTAL_DERIVED_FTE,
                COUNT(DISTINCT CASE WHEN ADO_FEATURE_ID IS NOT NULL THEN ADO_FEATURE_ID END) AS FEATURE_COUNT
              FROM {_fq('VW_TCO_WORKFORCE_SPLIT')}
              {('WHERE ' + ' AND '.join(where)) if where else ''}
              GROUP BY YEAR, GROUPNAME, PROGRAMNAME, TEAMNAME
              ORDER BY YEAR DESC, TOTAL_COST DESC
            """
            df_effort = fetch_df(sql_effort, tuple(params) if params else None)
            if df_effort is not None:
                df_effort["HEADCOUNT"] = 0
        except Exception:
            logger.exception("Failed to fetch demand/cost aggregates", extra={"email": scope.email, "role": scope.role})
            errors.append("Could not load demand vs cost data for your account.")

    inv_count: Optional[int] = None
    try:
        today = dt.date.today()
        inv_where = [
            "COALESCE(i.INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'",
            "i.RENEWALDATE IS NOT NULL",
            "MONTH(i.RENEWALDATE) = %s",
            "YEAR(i.RENEWALDATE) = %s",
        ]
        inv_params: list = [today.month, today.year]
        scope_sql = ""
        if scope.teams:
            placeholders = ", ".join(["%s"] * len(scope.teams))
            scope_sql = f" AND UPPER(t.TEAMNAME) IN ({placeholders})"
            inv_params.extend([t.upper() for t in scope.teams])
        elif scope.programs:
            placeholders = ", ".join(["%s"] * len(scope.programs))
            scope_sql = f" AND UPPER(p.PROGRAMNAME) IN ({placeholders})"
            inv_params.extend([p.upper() for p in scope.programs])
        sql_inv = f"""
            SELECT COUNT(*) AS N
            FROM {_fq('INVOICES')} i
            LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = i.TEAMID
            LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
            WHERE {' AND '.join(inv_where)}{scope_sql}
        """
        dfi = fetch_df(sql_inv, tuple(inv_params))
        if dfi is not None and not dfi.empty:
            inv_count = int(dfi.iloc[0]["N"]) if dfi.iloc[0]["N"] is not None else 0
    except Exception:
        logger.exception("Failed to fetch invoice count", extra={"email": scope.email, "role": scope.role})
        inv_count = None

    return CostData(costs=df_cost, effort_costs=df_effort, invoice_count=inv_count, errors=errors)
