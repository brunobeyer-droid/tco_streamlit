from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio
from db import fetch_df, _fq

# Admin context helpers for global scope filters and display toggles.

_DEFAULT_CONTEXT: Dict[str, object] = {
    "program": None,
    "program_name": None,
    "team": None,
    "team_name": None,
    "year": None,
    "pi": None,
    "show_ids": False,
    "row_limit": 200,
    "show_advanced": False,
    "show_perf": False,
}


def get_default_admin_context() -> Dict[str, object]:
    return dict(_DEFAULT_CONTEXT)


def get_admin_context() -> Dict[str, object]:
    ctx = st.session_state.get("admin_context")
    if not isinstance(ctx, dict):
        ctx = dict(_DEFAULT_CONTEXT)
        st.session_state["admin_context"] = ctx
    else:
        for k, v in _DEFAULT_CONTEXT.items():
            ctx.setdefault(k, v)
    return ctx


def set_admin_context(updates: Dict[str, object]) -> None:
    ctx = get_admin_context()
    ctx.update(updates)
    st.session_state["admin_context"] = ctx


@cache_data_portfolio(ttl=120)
def load_program_options() -> List[Tuple[str, str]]:
    try:
        df = fetch_df(f"SELECT PROGRAMID, PROGRAMNAME FROM { _fq('PROGRAMS') } ORDER BY PROGRAMNAME")
    except Exception:
        return []
    if df is None or df.empty:
        return []
    return [(str(r["PROGRAMID"]), str(r["PROGRAMNAME"])) for _, r in df.iterrows()]


@cache_data_portfolio(ttl=120)
def load_team_options(program_id: Optional[str] = None) -> List[Tuple[str, str]]:
    try:
        if program_id:
            df = fetch_df(
                f"""
                SELECT TEAMID, TEAMNAME
                FROM { _fq('TEAMS') }
                WHERE PROGRAMID = %s
                ORDER BY TEAMNAME
                """,
                (program_id,),
            )
        else:
            df = fetch_df(f"SELECT TEAMID, TEAMNAME FROM { _fq('TEAMS') } ORDER BY TEAMNAME")
    except Exception:
        return []
    if df is None or df.empty:
        return []
    return [(str(r["TEAMID"]), str(r["TEAMNAME"])) for _, r in df.iterrows()]


@cache_data_portfolio(ttl=120)
def load_year_options() -> List[int]:
    years = set()
    for table, col in [
        ("TEAM_RATE_HISTORY", "YEAR"),
        ("PROGRAM_RATE_HISTORY", "YEAR"),
        ("INVOICES", "FISCAL_YEAR"),
        ("CONTRACTS", "START_FY"),
        ("CONTRACTS", "END_FY"),
    ]:
        try:
            df = fetch_df(f"SELECT DISTINCT {col} AS Y FROM { _fq(table) } WHERE {col} IS NOT NULL")
        except Exception:
            df = None
        if df is None or df.empty or "Y" not in df.columns:
            continue
        for val in df["Y"].dropna().tolist():
            try:
                years.add(int(val))
            except Exception:
                continue
    return sorted(years)


@cache_data_portfolio(ttl=120)
def load_pi_options() -> List[int]:
    pis = set()
    for table in ["TEAM_RATE_HISTORY", "PROGRAM_RATE_HISTORY"]:
        try:
            df = fetch_df(f"SELECT DISTINCT PI FROM { _fq(table) } WHERE PI IS NOT NULL")
        except Exception:
            df = None
        if df is None or df.empty or "PI" not in df.columns:
            continue
        for val in df["PI"].dropna().tolist():
            try:
                pis.add(int(val))
            except Exception:
                continue
    return sorted(pis)


def apply_admin_context_filters(df: pd.DataFrame, ctx: Dict[str, object], entity_id: Optional[str] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    filtered = df.copy()
    program_id = ctx.get("program")
    program_name = ctx.get("program_name")
    team_id = ctx.get("team")
    team_name = ctx.get("team_name")
    year = ctx.get("year")
    pi = ctx.get("pi")

    if program_id:
        if "PROGRAMID" in filtered.columns:
            filtered = filtered[filtered["PROGRAMID"].astype(str) == str(program_id)]
        elif "PROGRAMNAME" in filtered.columns and program_name:
            filtered = filtered[filtered["PROGRAMNAME"].astype(str) == str(program_name)]
        elif entity_id == "Rates" and {"RATE_TYPE", "OWNER_ID"} <= set(filtered.columns):
            filtered = filtered[
                (filtered["RATE_TYPE"].astype(str) == "PROGRAM") &
                (filtered["OWNER_ID"].astype(str) == str(program_id))
            ]

    if team_id:
        if "TEAMID" in filtered.columns:
            filtered = filtered[filtered["TEAMID"].astype(str) == str(team_id)]
        elif "TEAMNAME" in filtered.columns and team_name:
            filtered = filtered[filtered["TEAMNAME"].astype(str) == str(team_name)]
        elif entity_id == "Rates" and {"RATE_TYPE", "OWNER_ID"} <= set(filtered.columns):
            filtered = filtered[
                (filtered["RATE_TYPE"].astype(str) == "TEAM") &
                (filtered["OWNER_ID"].astype(str) == str(team_id))
            ]

    if year is not None:
        if "YEAR" in filtered.columns:
            filtered = filtered[filtered["YEAR"].astype(str) == str(year)]
        elif "FISCAL_YEAR" in filtered.columns:
            filtered = filtered[filtered["FISCAL_YEAR"].astype(str) == str(year)]
        elif {"START_FY", "END_FY"} <= set(filtered.columns):
            filtered = filtered[
                (pd.to_numeric(filtered["START_FY"], errors="coerce") <= int(year)) &
                (pd.to_numeric(filtered["END_FY"], errors="coerce") >= int(year))
            ]

    if pi is not None and "PI" in filtered.columns:
        filtered = filtered[filtered["PI"].astype(str) == str(pi)]

    return filtered
