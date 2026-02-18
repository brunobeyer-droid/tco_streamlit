from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, List, Optional, Sequence

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio

from db import fetch_df_active as fetch_df, ensure_invoice_forecast_views
from welcome.state import normalize_display_name


def _to_int_opt(val) -> Optional[int]:
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(val)
    except Exception:
        return None


def _months_until_contract_due_start(year: int) -> Optional[int]:
    """Months from today to Jan 1 of `year`."""
    try:
        today = date.today()
        return (year - today.year) * 12 + (1 - today.month)
    except Exception:
        return None


def compute_invoice_alerts(
    df: pd.DataFrame,
    due_soon_days: int,
    contract_months: int,
    *,
    today: Optional[date] = None,
) -> pd.DataFrame:
    """
    Priority:
      1) Critical: Renewal past & Status=Planned
      2) Contract: CONTRACT_DUE set and months to Jan 1 of that year < contract_months (>=0) — any Status
      3) Pending: Renewal within due_soon_days & Status=Planned
      4) OK: everything else
    """
    if df is None or df.empty:
        return df
    today = today or date.today()

    def _days_to(d):
        try:
            if pd.isna(d):
                return None
            return (pd.to_datetime(d).date() - today).days
        except Exception:
            return None

    out = df.copy()
    out["Days to Renewal"] = out.get("Renewal Date").apply(_days_to)

    def _status(row):
        status = (row.get("Status") or "").strip()
        dtr = row.get("Days to Renewal")
        contract_due_year = _to_int_opt(row.get("Contract Due"))

        if dtr is not None and dtr < 0 and status == "Planned":
            return "Critical"

        if contract_due_year is not None:
            mu = _months_until_contract_due_start(contract_due_year)
            if mu is not None and 0 <= mu < contract_months:
                return "Contract"

        if dtr is not None and 0 <= dtr <= due_soon_days and status == "Planned":
            return "Pending"

        return "OK"

    out["Tracking Status"] = out.apply(_status, axis=1)
    return out


def summarize_alerts(df: pd.DataFrame) -> dict:
    if df is None or df.empty or "Tracking Status" not in df.columns:
        return {
            "critical": 0,
            "pending": 0,
            "contract": 0,
            "ok": 0,
            "alerts_n": 0,
            "missing_dates_n": 0,
            "top_alerts": pd.DataFrame(),
        }

    counts = df["Tracking Status"].value_counts().to_dict()
    critical = int(counts.get("Critical", 0))
    pending = int(counts.get("Pending", 0))
    contract = int(counts.get("Contract", 0))
    ok = int(counts.get("OK", 0))
    alerts_n = critical + pending + contract

    status_order = {"Critical": 0, "Pending": 1, "Contract": 2, "OK": 3}
    work = df.copy()
    work["_status_rank"] = work["Tracking Status"].map(status_order).fillna(9)
    work["_renewal_sort"] = pd.to_numeric(work.get("Days to Renewal"), errors="coerce")

    def _contract_days(val):
        try:
            if pd.isna(val):
                return None
            year = _to_int_opt(val)
            if year is None:
                return None
            target = date(int(year), 1, 1)
            return (target - date.today()).days
        except Exception:
            return None

    work["_contract_sort"] = work.get("Contract Due").apply(_contract_days)
    work["_sort_days"] = work.apply(
        lambda r: r["_renewal_sort"]
        if r["Tracking Status"] in ("Critical", "Pending")
        else r["_contract_sort"]
        if r["Tracking Status"] == "Contract"
        else 999999,
        axis=1,
    )

    top_alerts = (
        work[work["Tracking Status"].isin(["Critical", "Pending", "Contract"])]
        .sort_values(by=["_status_rank", "_sort_days"], ascending=[True, True])
        .copy()
    )

    missing_dates_n = 0
    if "Status" in work.columns and "Renewal Date" in work.columns:
        planned = work["Status"].fillna("").astype(str).str.strip().eq("Planned")
        missing_dates_n = int((planned & work["Renewal Date"].isna()).sum())

    return {
        "critical": critical,
        "pending": pending,
        "contract": contract,
        "ok": ok,
        "alerts_n": alerts_n,
        "missing_dates_n": missing_dates_n,
        "top_alerts": top_alerts,
    }


def filter_invoices_by_scope(
    df: pd.DataFrame,
    *,
    programs: list[str],
    teams: list[str],
    groups: list[str],
    apps: list[str],
) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if programs and "Program" in out.columns:
        prog_set = {str(p).strip().lower() for p in programs if str(p).strip()}
        out = out[out["Program"].astype(str).str.strip().str.lower().isin(prog_set)]
    if teams and "Team" in out.columns:
        team_set = {str(t).strip().lower() for t in teams if str(t).strip()}
        out = out[out["Team"].astype(str).str.strip().str.lower().isin(team_set)]
    if groups and "Application" in out.columns:
        grp_set = {str(g).strip().lower() for g in groups if str(g).strip()}
        out = out[out["Application"].astype(str).str.strip().str.lower().isin(grp_set)]
    if apps and "Application" in out.columns:
        app_set = {str(a).strip().lower() for a in apps if str(a).strip()}
        out = out[out["Application"].astype(str).str.strip().str.lower().isin(app_set)]
    return out


def _search_invoices_enriched(
    fiscal_year: Optional[int],
    program_ids: Sequence[str],
    team_ids: Sequence[str],
    group_ids: Sequence[str],
    application_ids: Sequence[str],
    status_filter: Optional[str] = None,
    invoice_type_filter: Optional[str] = None,
    agreement_like: Optional[str] = None,
    vendor_id: Optional[str] = None,
    order_status: Optional[str] = None,
    order_date: Optional[str] = None,
    include_forecast: bool = False,
) -> pd.DataFrame:
    if include_forecast:
        try:
            ensure_invoice_forecast_views()
            src = "VW_INVOICES_ACTUAL_AND_FORECAST"
            try:
                st.session_state.pop("warn_actuals_only", None)
            except Exception:
                pass
        except Exception as e:
            try:
                if not st.session_state.get("warn_forecast_view_fail"):
                    st.session_state["warn_forecast_view_fail"] = True
                    st.warning(f"Forecast view unavailable; showing actuals only. ({e})")
                st.session_state["warn_actuals_only"] = "Forecast view unavailable; showing actuals only."
            except Exception:
                pass
            src = "INVOICES"
    else:
        src = "INVOICES"

    where = []
    params: List[Any] = []
    if fiscal_year:
        where.append("i.FISCAL_YEAR = %s")
        params.append(int(fiscal_year))
    if team_ids:
        # Prefer current group/team ownership for scope filtering.
        # Fall back to invoice TEAMID when group/team linkage is missing.
        where.append(f"COALESCE(g.TEAMID, t.TEAMID, i.TEAMID) IN ({','.join(['%s'] * len(team_ids))})")
        params.extend(list(team_ids))
    if application_ids:
        where.append(f"i.APPLICATIONID IN ({','.join(['%s'] * len(application_ids))})")
        params.extend(list(application_ids))
    if group_ids:
        where.append(f"a.GROUPID IN ({','.join(['%s'] * len(group_ids))})")
        params.extend(list(group_ids))
    if program_ids:
        # Program scope should follow the effective program (group/team), not only team join.
        where.append(f"p.PROGRAMID IN ({','.join(['%s'] * len(program_ids))})")
        params.extend(list(program_ids))
    if status_filter and status_filter in ("Planned", "Completed"):
        where.append("i.STATUS = %s")
        params.append(status_filter)
    if invoice_type_filter and invoice_type_filter in ("Recurring Invoice", "Ad Hoc Invoice"):
        where.append("COALESCE(i.INVOICE_TYPE, 'Recurring Invoice') = %s")
        params.append(invoice_type_filter)
    if agreement_like and agreement_like.strip():
        where.append("i.AGREEMENT_NUMBER LIKE %s")
        params.append(f"%{agreement_like.strip()}%")
    if vendor_id:
        where.append("a.VENDORID = %s")
        params.append(vendor_id)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    order_parts = []
    if order_status == "Planned first":
        order_parts.append("CASE WHEN i.STATUS='Planned' THEN 0 ELSE 1 END ASC")
    elif order_status == "Completed first":
        order_parts.append("CASE WHEN i.STATUS='Completed' THEN 0 ELSE 1 END ASC")
    if order_date == "Newest first":
        order_parts.append("CASE WHEN i.RENEWALDATE IS NULL THEN 1 ELSE 0 END ASC, i.RENEWALDATE DESC")
    elif order_date == "Oldest first":
        order_parts.append("CASE WHEN i.RENEWALDATE IS NULL THEN 1 ELSE 0 END ASC, i.RENEWALDATE ASC")
    order_parts.extend(
        [
            "COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME)",
            "g.GROUPNAME",
            "a.APPLICATIONNAME",
        ]
    )
    order_sql = "ORDER BY " + ", ".join(order_parts)

    sql = f"""
        SELECT
            i.INVOICEID,
            i.FISCAL_YEAR,
            i.RENEWALDATE,
            i.AMOUNT,
            i.STATUS,
            i.CONTRACT_ACTIVE,
            i.CONTRACT_DUE,
            COALESCE(i.INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE,
            i.AGREEMENT_NUMBER,
            t.TEAMID,
            COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
            t.PRODUCTOWNER,
            g.GROUPID, g.GROUPNAME,
            a.APPLICATIONID, a.APPLICATIONNAME, a.ADD_INFO, a.VENDORID,
            p.PROGRAMID, p.PROGRAMNAME, p.PROGRAMOWNER
        FROM {src} i
        LEFT JOIN TEAMS t              ON t.TEAMID  = i.TEAMID
        LEFT JOIN APPLICATIONS a       ON a.APPLICATIONID = i.APPLICATIONID
        LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
        LEFT JOIN PROGRAMS p           ON p.PROGRAMID = ISNULL(g.PROGRAMID, t.PROGRAMID)
        {where_sql}
        {order_sql}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
    except Exception as e:
        if include_forecast and "VW_INVOICES_ACTUAL_AND_FORECAST" in str(e):
            sql_actuals = sql.replace("VW_INVOICES_ACTUAL_AND_FORECAST", "INVOICES")
            df = fetch_df(sql_actuals, tuple(params) if params else None)
            try:
                if not st.session_state.get("warn_forecast_view_missing"):
                    st.session_state["warn_forecast_view_missing"] = True
                    st.warning("Forecasts not available (view missing). Showing actuals.")
                st.session_state["warn_actuals_only"] = "Forecasts not available (view missing). Showing actuals."
            except Exception:
                pass
        else:
            raise
    if df is None or df.empty:
        return pd.DataFrame()

    def _mk_label(r):
        amt = r.get("AMOUNT") or 0
        dte = r.get("RENEWALDATE")
        try:
            dte_str = pd.to_datetime(dte).date().isoformat() if pd.notna(dte) else "n/a"
        except Exception:
            dte_str = str(dte)
        typ = r.get("INVOICE_TYPE") or "Recurring Invoice"
        return f"{typ} - {r.get('STATUS','')} - {float(amt):.2f} USD - {dte_str}"

    df["INVOICE_NAME"] = df.apply(_mk_label, axis=1)
    df["APP_LABEL"] = df.apply(
        lambda r: f"{r['APPLICATIONNAME']}" + (f" — {r['ADD_INFO']}" if r.get("ADD_INFO") else ""),
        axis=1,
    )

    df = df.rename(
        columns={
            "FISCAL_YEAR": "Year",
            "RENEWALDATE": "Renewal Date",
            "STATUS": "Status",
            "AMOUNT": "Amount (USD)",
            "TEAMNAME": "Team",
            "GROUPNAME": "Application",
            "APPLICATIONNAME": "Application Instance",
            "INVOICE_NAME": "Invoice Name",
            "INVOICE_TYPE": "Invoice Type",
            "AGREEMENT_NUMBER": "Agreement Number",
            "PRODUCTOWNER": "Responsible Email",
            "CONTRACT_ACTIVE": "Contract Active",
            "CONTRACT_DUE": "Contract Due",
            "PROGRAMNAME": "Program",
            "PROGRAMOWNER": "Program Manager Email",
        }
    )
    resp_email_col = df.get("Responsible Email", pd.Series(dtype=str)).fillna("").astype(str)

    def _friendly_name(val: str) -> str:
        s = str(val or "").strip()
        if not s:
            return "—"
        disp = normalize_display_name(s)
        if "@" in disp and disp == s:
            local = s.split("@", 1)[0]
            parts = [p for p in local.replace("-", "_").replace(".", "_").split("_") if p]
            if parts:
                disp = " ".join(p.title() for p in parts)
        return disp

    df["Responsible"] = resp_email_col.apply(_friendly_name)
    return df


@cache_data_portfolio(ttl=60, show_spinner=False)
def cached_search_invoices(**kwargs):
    # Accept both singular and plural ids for backwards compatibility.
    program_id = kwargs.pop("program_id", None)
    team_id = kwargs.pop("team_id", None)
    group_id = kwargs.pop("group_id", None)
    application_id = kwargs.pop("application_id", None)

    if "program_ids" not in kwargs:
        kwargs["program_ids"] = tuple([program_id]) if program_id else tuple()
    if "team_ids" not in kwargs:
        kwargs["team_ids"] = tuple([team_id]) if team_id else tuple()
    if "group_ids" not in kwargs:
        kwargs["group_ids"] = tuple([group_id]) if group_id else tuple()
    if "application_ids" not in kwargs:
        kwargs["application_ids"] = tuple([application_id]) if application_id else tuple()

    return _search_invoices_enriched(**kwargs)


def get_filtered_invoice_alerts(
    *,
    fiscal_year: int,
    program_ids: Sequence[str],
    team_ids: Sequence[str],
    group_ids: Sequence[str],
    application_ids: Sequence[str],
    due_soon_days: int,
    contract_months: int,
    include_forecast: bool = True,
    only_active: bool = True,
    invoice_type_filter: str = "Recurring Invoice",
) -> pd.DataFrame:
    df = cached_search_invoices(
        fiscal_year=int(fiscal_year),
        program_ids=tuple(program_ids),
        team_ids=tuple(team_ids),
        group_ids=tuple(group_ids),
        application_ids=tuple(application_ids),
        status_filter=None,
        invoice_type_filter=invoice_type_filter,
        agreement_like=None,
        vendor_id=None,
        order_status=None,
        order_date=None,
        include_forecast=include_forecast,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    if only_active and "Contract Active" in df.columns:
        df = df[df["Contract Active"] == True].copy()
    track_df = compute_invoice_alerts(df, due_soon_days, contract_months)
    return track_df
