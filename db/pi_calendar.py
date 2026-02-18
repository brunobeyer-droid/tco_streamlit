from __future__ import annotations

from datetime import date
from typing import Any, Optional, Tuple

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio
from db import ensure_ado_iteration_calendar_table, fetch_df, _fq


def _s(val: Any) -> str:
    return str(val or "").strip()


def _parse_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            try:
                return pd.to_datetime(s).date()
            except Exception:
                return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def _parent_iteration_path(iteration_path: str) -> str:
    p = _s(iteration_path)
    if not p:
        return ""
    if "\\" in p:
        return p.rsplit("\\", 1)[0]
    if "/" in p:
        return p.rsplit("/", 1)[0]
    return ""


@cache_data_portfolio(ttl=600, show_spinner=False)
def map_iteration_path_to_pi(conn: Any, iteration_path: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (pi_name, iteration_name) for a given ADO iteration path."""
    _ = conn
    try:
        ensure_ado_iteration_calendar_table()
    except Exception:
        pass

    path = _s(iteration_path)
    if not path:
        return None, None

    def _query(path_val: str):
        return fetch_df(
            f"""
            SELECT TOP 1
              PI_NAME,
              ITERATION_NAME,
              ITERATION_GRAIN
            FROM { _fq('ADO_ITERATION_CALENDAR') }
            WHERE UPPER(LTRIM(RTRIM(ITERATION_PATH))) = UPPER(LTRIM(RTRIM(%s)))
            """,
            (path_val,),
        )

    try:
        df = _query(path)
        if df is not None and not df.empty:
            r = df.iloc[0]
            grain = _s(r.get("ITERATION_GRAIN")).upper()
            pi_name = _s(r.get("PI_NAME")) or None
            it_name = _s(r.get("ITERATION_NAME")) or None
            if grain == "PI":
                return pi_name, it_name
            # SPRINT rows: map to parent PI path when possible.
            parent = _parent_iteration_path(path)
            if parent:
                df2 = _query(parent)
                if df2 is not None and not df2.empty:
                    r2 = df2.iloc[0]
                    return (_s(r2.get("PI_NAME")) or pi_name), (_s(r.get("ITERATION_NAME")) or _s(r2.get("ITERATION_NAME")) or None)
            return pi_name, it_name
    except Exception:
        pass

    parent = _parent_iteration_path(path)
    if parent:
        try:
            df2 = _query(parent)
            if df2 is not None and not df2.empty:
                r2 = df2.iloc[0]
                return (_s(r2.get("PI_NAME")) or None), (_s(r2.get("ITERATION_NAME")) or None)
        except Exception:
            pass

    return None, None


@cache_data_portfolio(ttl=600, show_spinner=False)
def map_date_to_pi(conn: Any, fiscal_year: int, event_date: date) -> Tuple[Optional[str], Optional[str]]:
    """Return (pi_name, iteration_name) for a given date using the existing ADO iteration calendar."""
    _ = conn
    try:
        ensure_ado_iteration_calendar_table()
    except Exception:
        pass

    d = _parse_date(event_date)
    if d is None:
        return None, None

    fy = int(fiscal_year) if fiscal_year is not None else 0
    year_filter = "1=1"
    params = [d, d]
    if fy:
        # Existing repo uses `YEAR` on ADO_ITERATION_CALENDAR.
        year_filter = "YEAR = %s"
        params.append(fy)

    try:
        df = fetch_df(
            f"""
            SELECT TOP 1
              PI_NAME,
              ITERATION_NAME
            FROM { _fq('ADO_ITERATION_CALENDAR') }
            WHERE
              START_DATE IS NOT NULL AND END_DATE IS NOT NULL
              AND %s BETWEEN START_DATE AND END_DATE
              AND %s BETWEEN START_DATE AND END_DATE
              AND UPPER(COALESCE(ITERATION_GRAIN, 'PI')) = 'PI'
              AND {year_filter}
            ORDER BY START_DATE ASC
            """,
            tuple(params),
        )
        if df is not None and not df.empty:
            r = df.iloc[0]
            return (_s(r.get("PI_NAME")) or None), (_s(r.get("ITERATION_NAME")) or None)
    except Exception:
        pass

    return None, None


def sync_iteration_calendar_from_ado_metadata(conn: Any) -> None:
    """TODO: Populate/refresh `ADO_ITERATION_CALENDAR` from ADO Analytics iteration metadata."""
    _ = conn
    raise NotImplementedError("TODO: implement ADO iteration calendar sync from ADO metadata.")
