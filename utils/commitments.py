from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

import pandas as pd

from db import execute, fetch_df


def ensure_commitments_table() -> None:
    """Ensure commitments table exists (idempotent)."""
    from db.mssql_backend import ensure_tables  # type: ignore

    try:
        ensure_tables()
    except Exception:
        # If ensure_tables is not available in this context, fall through silently.
        pass


def _scope_clause(year: Optional[int], pi: Optional[str], program: Optional[str], team: Optional[str], group: Optional[str] = None):
    where = []
    params: list[Any] = []
    if year is not None:
        where.append("YEAR = %s")
        params.append(int(year))
    else:
        where.append("YEAR IS NULL")
    if pi:
        where.append("PI = %s")
        params.append(pi)
    else:
        where.append("PI IS NULL")
    if program:
        where.append("PROGRAMNAME = %s")
        params.append(program)
    else:
        where.append("PROGRAMNAME IS NULL")
    if team:
        where.append("TEAMNAME = %s")
        params.append(team)
    else:
        where.append("TEAMNAME IS NULL")
    if group:
        where.append("GROUPNAME = %s")
        params.append(group)
    else:
        where.append("GROUPNAME IS NULL")
    return " AND ".join(where), params


def load_commitment(year: Optional[int], pi: Optional[str], program: Optional[str], team: Optional[str]) -> Optional[Dict[str, Any]]:
    where, params = _scope_clause(year, pi, program, team, None)
    sql = f"""
      SELECT TOP 1 *
      FROM TCO_COMMITMENTS
      WHERE {where} AND GROUPNAME IS NULL
      ORDER BY UPDATED_AT DESC
    """
    try:
        df = fetch_df(sql, tuple(params))
    except Exception:
        return None
    if df is None or df.empty:
        return None
    row = df.iloc[0].to_dict()
    return row


def upsert_commitment(
    *,
    year: Optional[int],
    pi: Optional[str],
    program: Optional[str],
    team: Optional[str],
    status: str,
    target_date: Optional[date],
    notes: str,
    updated_by: Optional[str],
    owner_email: Optional[str],
    owner_name: Optional[str],
) -> None:
    where, params = _scope_clause(year, pi, program, team, None)
    sql_update = f"""
      UPDATE TCO_COMMITMENTS
         SET STATUS=%s,
             TARGET_DATE=%s,
             NOTES=%s,
             UPDATED_AT=SYSUTCDATETIME(),
             UPDATED_BY=%s,
             OWNER_EMAIL=%s,
             OWNER_NAME=%s
      WHERE {where}
    """
    upd_params = [status, target_date, notes, updated_by, owner_email, owner_name] + params
    try:
        execute(sql_update, upd_params)
    except Exception:
        return

    sql_insert = """
      INSERT INTO TCO_COMMITMENTS
        (YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, STATUS, TARGET_DATE, NOTES, UPDATED_BY, OWNER_EMAIL, OWNER_NAME)
      SELECT %s,%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s
      WHERE NOT EXISTS (
        SELECT 1 FROM TCO_COMMITMENTS WHERE YEAR %s %s AND PI %s %s AND PROGRAMNAME %s %s AND TEAMNAME %s %s AND GROUPNAME %s %s
      )
    """
    def _null_clause(val: Optional[Any]) -> tuple[str, list[Any]]:
        if val is None:
            return "IS", ["NULL"]
        return "=", [val]

    clauses_params: list[Any] = []
    y_clause, y_param = _null_clause(year)
    p_clause, p_param = _null_clause(pi)
    prog_clause, prog_param = _null_clause(program)
    team_clause, team_param = _null_clause(team)
    grp_clause, grp_param = _null_clause(None)
    clauses_params.extend(y_param + p_param + prog_param + team_param + grp_param)

    sql_insert = sql_insert % (
        ("%s",) * 11
        + (y_clause, "%s", p_clause, "%s", prog_clause, "%s", team_clause, "%s", grp_clause, "%s")
    )
    insert_params = [year, pi, program, team, status, target_date, notes, updated_by, owner_email, owner_name] + clauses_params
    try:
        execute(sql_insert, insert_params)
    except Exception:
        return


def summarize_commitments(year: Optional[int], pi: Optional[str], program: Optional[str] = None, team: Optional[str] = None) -> Dict[str, Any]:
    where = []
    params: list[Any] = []
    if year is not None:
        where.append("YEAR = %s")
        params.append(int(year))
    if pi:
        where.append("PI = %s")
        params.append(pi)
    if program:
        where.append("PROGRAMNAME = %s")
        params.append(program)
    if team:
        where.append("TEAMNAME = %s")
        params.append(team)
    where.append("GROUPNAME IS NULL")
    where_sql = " AND ".join(where) if where else "GROUPNAME IS NULL"
    sql = f"""
      SELECT
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN STATUS = 'Committed' THEN 1 ELSE 0 END) AS COMMITTED,
        SUM(CASE WHEN STATUS = 'Blocked' THEN 1 ELSE 0 END) AS BLOCKED,
        SUM(CASE WHEN STATUS <> 'Committed' AND TARGET_DATE IS NOT NULL AND TARGET_DATE < CAST(GETUTCDATE() AS DATE) THEN 1 ELSE 0 END) AS OVERDUE
      FROM TCO_COMMITMENTS
      WHERE {where_sql}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
    except Exception:
        return {"total_records": 0, "committed_count": 0, "blocked_count": 0, "overdue_count": 0, "committed_pct": 0.0}
    if df is None or df.empty:
        return {"total_records": 0, "committed_count": 0, "blocked_count": 0, "overdue_count": 0, "committed_pct": 0.0}
    row = df.iloc[0]
    total = int(row.get("TOTAL", 0) or 0)
    committed = int(row.get("COMMITTED", 0) or 0)
    blocked = int(row.get("BLOCKED", 0) or 0)
    overdue = int(row.get("OVERDUE", 0) or 0)
    pct = (committed / total * 100) if total > 0 else 0.0
    return {
        "total_records": total,
        "committed_count": committed,
        "blocked_count": blocked,
        "overdue_count": overdue,
        "committed_pct": pct,
    }


__all__ = [
    "ensure_commitments_table",
    "load_commitment",
    "upsert_commitment",
    "summarize_commitments",
]
