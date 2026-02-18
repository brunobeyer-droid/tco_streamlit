# Summary: update invoice email UI text to NEXT branding.
# pages/invoice_tracking.py
import html
import inspect
import math
import re
import time
import base64
import uuid
from contextlib import nullcontext
from datetime import date
from typing import Optional, List, Any, Tuple, Sequence, Dict

import pandas as pd
import streamlit as st
from core.init import init_page, page_loader
from utils.theme import THEME_PALETTES

from db import (
    list_programs,
    list_teams,
    list_application_groups,
    list_applications,
    upsert_invoice,
    fetch_df,
    execute,
    list_vendors,
    _fq,
    ensure_contracts_table,
    upsert_contract,
    get_email_alert_config,
)
from core.cache_utils import cache_data_portfolio
from core.freshness import post_write_refresh
from utils.email import EmailSendError, send_graph_mail
from core.canonical_costs import get_unassigned_breakdown
from core.kpi_cards import render_kpi_card as _kpi_card
from core.invoices_alerts import compute_invoice_alerts, cached_search_invoices
from core.scope import infer_role, read_scope_from_session, scope_label, ROLE_PROGRAM, ROLE_PO
from core.ui import render_page_header, touch_last_updated_status
from utils.ui_patterns import render_section_picker

from utils.app_shell import bootstrap_page
bootstrap_page()

try:
    from db.tco_events import record_cost_event_from_invoice
except Exception:
    record_cost_event_from_invoice = None  # type: ignore[assignment]

# Events are currently disabled for normal planning edits.
# We keep the table/helpers for future explicit change logging, but do not write automatically.
ENABLE_COST_EVENTS = False

_prev_page_path_before_init = str(st.session_state.get("_tco_page_path", "") or "")
page_theme = init_page("Invoices", page_path=__file__)
_page_theme = page_theme
_palette = THEME_PALETTES.get(_page_theme, THEME_PALETTES["light"])
user = st.session_state.get("auth_user") or {}

# --- Back-compat helpers (added non-UI) ---
def list_groups_for_team(team_id: str) -> pd.DataFrame:
    return fetch_df("SELECT GROUPID, GROUPNAME FROM APPLICATION_GROUPS WHERE TEAMID=%s ORDER BY GROUPNAME", (team_id,))

def delete_invoice(invoice_id: str) -> None:
    try:
        execute("DELETE FROM INVOICE_ATTACHMENTS WHERE INVOICEID=%s", (invoice_id,))
    except Exception:
        pass
    try:
        execute("DELETE FROM INVOICE_NOTES WHERE INVOICEID=%s", (invoice_id,))
    except Exception:
        pass
    execute("DELETE FROM INVOICES WHERE INVOICEID=%s", (invoice_id,))

# --- Session state ---
if "create_upload_key" not in st.session_state:
    st.session_state["create_upload_key"] = 0
# Modal editor (global)
if "invoice_edit_id" not in st.session_state:
    st.session_state["invoice_edit_id"] = None
if "invoice_edit_open" not in st.session_state:
    st.session_state["invoice_edit_open"] = False
# If user navigated away and came back, do not auto-reopen stale edit dialog.
if _prev_page_path_before_init and ("2_Invoices.py" not in _prev_page_path_before_init):
    st.session_state["invoice_edit_open"] = False
    st.session_state["invoice_edit_id"] = None
# Tracking editor
if "track_open_editor" not in st.session_state:
    st.session_state["track_open_editor"] = None  # (section_key, invoice_id)
if "track_editor_expanded" not in st.session_state:
    st.session_state["track_editor_expanded"] = False
if "track_filter_sig" not in st.session_state:
    st.session_state["track_filter_sig"] = None
# Search editor
if "search_open_editor" not in st.session_state:
    st.session_state["search_open_editor"] = None  # invoice_id
if "search_editor_expanded" not in st.session_state:
    st.session_state["search_editor_expanded"] = False
if "search_filter_sig" not in st.session_state:
    st.session_state["search_filter_sig"] = None
if "search_defaults_applied" not in st.session_state:
    st.session_state["search_defaults_applied"] = False
if "invoices_op_feedback" not in st.session_state:
    st.session_state["invoices_op_feedback"] = None

# Current user (for audit / updated_by)
current_user_email = str(st.session_state.get("auth_user", {}).get("email", "")).strip() or None

# --------------------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------------------
def _safe_years(start: int = 2020, end: int = 2036) -> List[int]:
    return list(range(start, end + 1))

def _ensure_str(x) -> Optional[str]:
    s = str(x).strip() if x is not None else ""
    return s if s else None

def _to_int_opt(x) -> Optional[int]:
    """Return int(x) or None for '', None, NaN, or non-numeric."""
    try:
        if x is None:
            return None
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))  # tolerate '2027.0'
    except Exception:
        return None


def _normalize_programs_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    if not out.empty:
        cols = {str(c).strip().upper(): c for c in out.columns}
        if "PROGRAMID" not in out.columns and cols.get("PROGRAMID") in out.columns:
            out["PROGRAMID"] = out[cols["PROGRAMID"]]
        if "PROGRAMNAME" not in out.columns and cols.get("PROGRAMNAME") in out.columns:
            out["PROGRAMNAME"] = out[cols["PROGRAMNAME"]]
        if "PROGRAMNAME_RAW" not in out.columns and cols.get("PROGRAMNAME_RAW") in out.columns:
            out["PROGRAMNAME_RAW"] = out[cols["PROGRAMNAME_RAW"]]
    if "PROGRAMID" not in out.columns:
        out["PROGRAMID"] = ""
    if "PROGRAMNAME" not in out.columns:
        out["PROGRAMNAME"] = out.get("PROGRAMNAME_RAW", "")
    out["PROGRAMID"] = out["PROGRAMID"].astype(str).str.strip()
    out["PROGRAMNAME"] = out["PROGRAMNAME"].astype(str).str.strip()
    if "PROGRAMNAME_RAW" in out.columns:
        blank = out["PROGRAMNAME"] == ""
        out.loc[blank, "PROGRAMNAME"] = out.loc[blank, "PROGRAMNAME_RAW"].astype(str).str.strip()
    out = out[(out["PROGRAMID"] != "") & (out["PROGRAMNAME"] != "")]
    if not out.empty:
        out = out.drop_duplicates(subset=["PROGRAMID", "PROGRAMNAME"]).sort_values("PROGRAMNAME")
    return out


def _normalize_teams_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    if not out.empty:
        cols = {str(c).strip().upper(): c for c in out.columns}
        if "TEAMID" not in out.columns and cols.get("TEAMID") in out.columns:
            out["TEAMID"] = out[cols["TEAMID"]]
        if "TEAM_DISPLAY_NAME" not in out.columns and cols.get("TEAM_DISPLAY_NAME") in out.columns:
            out["TEAM_DISPLAY_NAME"] = out[cols["TEAM_DISPLAY_NAME"]]
        if "TEAMNAME" not in out.columns and cols.get("TEAMNAME") in out.columns:
            out["TEAMNAME"] = out[cols["TEAMNAME"]]
        if "TEAMNAME_RAW" not in out.columns and cols.get("TEAMNAME_RAW") in out.columns:
            out["TEAMNAME_RAW"] = out[cols["TEAMNAME_RAW"]]
        if "PROGRAMID" not in out.columns and cols.get("PROGRAMID") in out.columns:
            out["PROGRAMID"] = out[cols["PROGRAMID"]]
    if "TEAMID" not in out.columns:
        out["TEAMID"] = ""
    if "TEAMNAME" not in out.columns:
        out["TEAMNAME"] = out.get("TEAMNAME_RAW", "")
    if "PROGRAMID" not in out.columns:
        out["PROGRAMID"] = ""
    out["TEAMID"] = out["TEAMID"].astype(str).str.strip()
    if "TEAM_DISPLAY_NAME" in out.columns:
        out["TEAM_DISPLAY_NAME"] = out["TEAM_DISPLAY_NAME"].astype(str).str.strip()
    out["TEAMNAME"] = out["TEAMNAME"].astype(str).str.strip()
    if "TEAM_DISPLAY_NAME" in out.columns:
        disp = out["TEAM_DISPLAY_NAME"].astype(str).str.strip()
        out.loc[disp != "", "TEAMNAME"] = disp.loc[disp != ""]
    if "TEAMNAME_RAW" in out.columns:
        blank = out["TEAMNAME"] == ""
        out.loc[blank, "TEAMNAME"] = out.loc[blank, "TEAMNAME_RAW"].astype(str).str.strip()
    out["PROGRAMID"] = out["PROGRAMID"].astype(str).str.strip()
    out = out[(out["TEAMID"] != "") & (out["TEAMNAME"] != "")]
    return out


def _normalize_groups_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    if not out.empty:
        cols = {str(c).strip().upper(): c for c in out.columns}
        if "GROUPID" not in out.columns and cols.get("GROUPID") in out.columns:
            out["GROUPID"] = out[cols["GROUPID"]]
        if "GROUPNAME" not in out.columns and cols.get("GROUPNAME") in out.columns:
            out["GROUPNAME"] = out[cols["GROUPNAME"]]
    if "GROUPID" not in out.columns:
        out["GROUPID"] = ""
    if "GROUPNAME" not in out.columns:
        out["GROUPNAME"] = out["GROUPID"]
    out["GROUPID"] = out["GROUPID"].astype(str).str.strip()
    out["GROUPNAME"] = out["GROUPNAME"].astype(str).str.strip()
    out.loc[out["GROUPNAME"] == "", "GROUPNAME"] = out.loc[out["GROUPNAME"] == "", "GROUPID"]
    out = out[(out["GROUPID"] != "") & (out["GROUPNAME"] != "")]
    if not out.empty:
        out = out.drop_duplicates(subset=["GROUPID", "GROUPNAME"]).sort_values("GROUPNAME")
    return out


def _normalize_apps_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    if not out.empty:
        cols = {str(c).strip().upper(): c for c in out.columns}
        if "APPLICATIONID" not in out.columns and cols.get("APPLICATIONID") in out.columns:
            out["APPLICATIONID"] = out[cols["APPLICATIONID"]]
        if "APPLICATIONNAME" not in out.columns and cols.get("APPLICATIONNAME") in out.columns:
            out["APPLICATIONNAME"] = out[cols["APPLICATIONNAME"]]
        if "ADD_INFO" not in out.columns and cols.get("ADD_INFO") in out.columns:
            out["ADD_INFO"] = out[cols["ADD_INFO"]]
        if "GROUPID" not in out.columns and cols.get("GROUPID") in out.columns:
            out["GROUPID"] = out[cols["GROUPID"]]
    if "APPLICATIONID" not in out.columns:
        out["APPLICATIONID"] = ""
    if "APPLICATIONNAME" not in out.columns:
        out["APPLICATIONNAME"] = out["APPLICATIONID"]
    if "ADD_INFO" not in out.columns:
        out["ADD_INFO"] = ""
    if "GROUPID" not in out.columns:
        out["GROUPID"] = ""
    out["APPLICATIONID"] = out["APPLICATIONID"].astype(str).str.strip()
    out["APPLICATIONNAME"] = out["APPLICATIONNAME"].astype(str).str.strip()
    out.loc[out["APPLICATIONNAME"] == "", "APPLICATIONNAME"] = out.loc[out["APPLICATIONNAME"] == "", "APPLICATIONID"]
    out["ADD_INFO"] = out["ADD_INFO"].astype(str).str.strip()
    out["GROUPID"] = out["GROUPID"].astype(str).str.strip()
    out = out[(out["APPLICATIONID"] != "") & (out["APPLICATIONNAME"] != "")]
    return out


def _normalize_vendors_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    if not out.empty:
        cols = {str(c).strip().upper(): c for c in out.columns}
        if "VENDORID" not in out.columns and cols.get("VENDORID") in out.columns:
            out["VENDORID"] = out[cols["VENDORID"]]
        if "VENDORNAME" not in out.columns and cols.get("VENDORNAME") in out.columns:
            out["VENDORNAME"] = out[cols["VENDORNAME"]]
    if "VENDORID" not in out.columns:
        out["VENDORID"] = ""
    if "VENDORNAME" not in out.columns:
        out["VENDORNAME"] = out["VENDORID"]
    out["VENDORID"] = out["VENDORID"].astype(str).str.strip()
    out["VENDORNAME"] = out["VENDORNAME"].astype(str).str.strip()
    out.loc[out["VENDORNAME"] == "", "VENDORNAME"] = out.loc[out["VENDORNAME"] == "", "VENDORID"]
    out = out[(out["VENDORID"] != "") & (out["VENDORNAME"] != "")]
    if not out.empty:
        out = out.drop_duplicates(subset=["VENDORID", "VENDORNAME"]).sort_values("VENDORNAME")
    return out


def _canon_id(v: Any) -> str:
    return str(v or "").strip().upper()


def _with_upper_aliases(df: Optional[pd.DataFrame], cols_needed: List[str]) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    if out.empty:
        return out
    cols = {str(c).strip().upper(): c for c in out.columns}
    for c in cols_needed:
        src = cols.get(c)
        if c not in out.columns and src in out.columns:
            out[c] = out[src]
    return out


def _set_op_feedback(level: str, message: str, target: str = "invoices_active") -> None:
    st.session_state["invoices_op_feedback"] = {
        "level": str(level or "info"),
        "message": str(message or ""),
        "target": str(target or "invoices_active"),
    }


def _render_op_feedback(target: str) -> None:
    payload = st.session_state.pop("invoices_op_feedback", None)
    if not isinstance(payload, dict):
        return
    payload_target = str(payload.get("target") or "invoices_active")
    if payload_target != str(target):
        st.session_state["invoices_op_feedback"] = payload
        return
    level = str(payload.get("level") or "info").lower()
    message = str(payload.get("message") or "").strip()
    if not message:
        return
    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)

# --------------------------------------------------------------------------------------
# Caching wrappers (speed!)
# --------------------------------------------------------------------------------------
@cache_data_portfolio(ttl=300, show_spinner=False)
def cached_list_programs():
    return list_programs()

@cache_data_portfolio(ttl=300, show_spinner=False)
def cached_list_teams():
    return list_teams()

@cache_data_portfolio(ttl=300, show_spinner=False)
def cached_list_groups_for_team(team_id: str):
    return list_groups_for_team(team_id)

@cache_data_portfolio(ttl=300, show_spinner=False)
def cached_list_application_groups():
    return list_application_groups()

@cache_data_portfolio(ttl=300, show_spinner=False)
def cached_list_applications(team_id: Optional[str] = None, group_id: Optional[str] = None):
    # list_applications supports only team_id; filter by group_id here if present
    df = list_applications(team_id=team_id)
    if group_id and not df.empty and "GROUPID" in df.columns:
        df = df[df["GROUPID"] == group_id]
    return df

@cache_data_portfolio(ttl=300, show_spinner=False)
def cached_list_vendors():
    return list_vendors()

@cache_data_portfolio(ttl=120, show_spinner=False)
def cached_email_alert_config() -> Dict[str, Any]:
    try:
        return get_email_alert_config()
    except Exception:
        return {
            "pending_window_days": 30,
            "contract_window_months": 9,
            "subject_prefix": "[NEXT]",
            "subject_template": "{prefix} Action needed: {count} invoice(s) {status} for {program}",
            "body_intro": "The following invoices are currently flagged as {status_title}. Please review and take action.",
            "body_footer": "Update the invoice status, add notes, or attach approvals directly in the Invoice Tracking page. This email was generated automatically from the NEXT Invoice Tracking dashboard.",
            "footnote_template": "Pending = due within {pending_window_days} day(s). Critical = past due and still planned.",
            "save_to_sent_items": False,
            "additional_recipients": "",
            "additional_cc": "",
            "schedule_enabled": False,
            "schedule_day": 1,
            "schedule_hour_utc": 13,
            "mail_sender": "",
        }

@cache_data_portfolio(ttl=60, show_spinner=False)
def cached_fetch_invoice_by_id(invoice_id: str) -> pd.DataFrame:
    return fetch_df("""
        SELECT
            INVOICEID,
            APPLICATIONID,
            TEAMID,
            GROUPID,
            GROUPID_AT_BOOKING,
            FISCAL_YEAR,
            RENEWALDATE,
            AMOUNT,
            STATUS,
            AMOUNT_NEXT_YEAR,
            CONTRACT_ACTIVE,
            COMPANY_CODE,
            COST_CENTER,
            SERIAL_NUMBER,
            WORK_ORDER,
            AGREEMENT_NUMBER,
            CONTRACT_DUE,
            SERVICE_TYPE,
            NOTES,
            COALESCE(INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE
        FROM INVOICES
        WHERE INVOICEID = %s
        """, (invoice_id,)
    )


def _team_name_for_id(team_id: Optional[str]) -> str:
    tid = str(team_id or "").strip()
    if not tid:
        return ""
    try:
        teams = _normalize_teams_df(cached_list_teams())
        if teams is not None and not teams.empty:
            match = teams.loc[teams["TEAMID"].astype(str) == tid]
            if match is not None and not match.empty:
                return str(match.iloc[0].get("TEAMNAME") or "").strip()
    except Exception:
        pass
    return ""


def _group_name_for_id(group_id: Optional[str]) -> str:
    gid = str(group_id or "").strip()
    if not gid:
        return ""
    try:
        groups = _normalize_groups_df(cached_list_application_groups())
        if groups is not None and not groups.empty:
            match = groups.loc[groups["GROUPID"].astype(str) == gid]
            if match is not None and not match.empty:
                return str(match.iloc[0].get("GROUPNAME") or "").strip()
    except Exception:
        pass
    return ""


def _app_label_for_id(group_id: Optional[str], app_id: Optional[str]) -> str:
    gid = str(group_id or "").strip()
    aid = str(app_id or "").strip()
    if not gid or not aid:
        return ""
    try:
        apps = cached_list_applications(group_id=gid)
        if apps is None or apps.empty:
            return ""
        row = apps.loc[apps["APPLICATIONID"].astype(str) == aid]
        if row is None or row.empty:
            return ""
        r = row.iloc[0]
        name = str(r.get("APPLICATIONNAME") or "").strip()
        add = str(r.get("ADD_INFO") or "").strip()
        if name and add:
            return f"{name} — {add}"
        return name
    except Exception:
        return ""


def _invoice_context_label(team_id: Optional[str], group_id: Optional[str], app_id: Optional[str]) -> str:
    team_name = _team_name_for_id(team_id)
    group_name = _group_name_for_id(group_id)
    return " — ".join([x for x in [team_name, group_name] if x])

def clear_reference_caches():
    """Manual cache clear for reference data/lookups."""
    for fn in [
        cached_list_programs,
        cached_list_teams,
        cached_list_groups_for_team,
        cached_list_application_groups,
        cached_list_applications,
        cached_list_vendors,
        cached_email_alert_config,
    ]:
        try:
            fn.clear()
        except Exception:
            pass

def _user_default_program_team() -> tuple[Optional[str], Optional[str]]:
    """
    Infer defaults from central scope logic.
    - Product Owner: default Program + Team.
    - Program Manager/Owner: default Program only.
    - Unassigned/Portfolio: no defaults.
    Returns (program_name, team_name) or (None, None).
    """
    try:
        scope = read_scope_from_session(fetch_df)
        role = infer_role(scope)
    except Exception:
        scope = None
        role = None
    if scope is None:
        return None, None
    def _resolve_program_name(name: Optional[str]) -> Optional[str]:
        raw_name = str(name or "").strip()
        if not raw_name:
            return None
        progs = _normalize_programs_df(cached_list_programs())
        if progs is None or progs.empty:
            return raw_name
        if "PROGRAMNAME_RAW" in progs.columns:
            m = progs.loc[
                progs["PROGRAMNAME_RAW"].fillna("").astype(str).str.strip().str.upper()
                == raw_name.upper()
            ]
            if not m.empty:
                return str(m.iloc[0].get("PROGRAMNAME") or raw_name).strip() or raw_name
        m2 = progs.loc[
            progs["PROGRAMNAME"].fillna("").astype(str).str.strip().str.upper()
            == raw_name.upper()
        ]
        if not m2.empty:
            return str(m2.iloc[0].get("PROGRAMNAME") or raw_name).strip() or raw_name
        return raw_name

    def _resolve_team_name(name: Optional[str]) -> Optional[str]:
        raw_name = str(name or "").strip()
        if not raw_name:
            return None
        teams = _normalize_teams_df(cached_list_teams())
        if teams is None or teams.empty:
            return raw_name
        if "TEAMNAME_RAW" in teams.columns:
            m = teams.loc[
                teams["TEAMNAME_RAW"].fillna("").astype(str).str.strip().str.upper()
                == raw_name.upper()
            ]
            if not m.empty:
                return str(m.iloc[0].get("TEAMNAME") or raw_name).strip() or raw_name
        m2 = teams.loc[
            teams["TEAMNAME"].fillna("").astype(str).str.strip().str.upper()
            == raw_name.upper()
        ]
        if not m2.empty:
            return str(m2.iloc[0].get("TEAMNAME") or raw_name).strip() or raw_name
        return raw_name

    if role == ROLE_PO:
        prog_name = _resolve_program_name((scope.programs or [None])[0] if scope.programs else None)
        team_name = _resolve_team_name((scope.teams or [None])[0] if scope.teams else None)
        return prog_name, team_name
    if role == ROLE_PROGRAM:
        prog_name = _resolve_program_name((scope.programs or [None])[0] if scope.programs else None)
        return prog_name, None
    return None, None

# --------------------------------------------------------------------------------------
# Notes & Attachments helpers
# --------------------------------------------------------------------------------------
def _note_append(invoice_id: str, note_text: str, created_by: Optional[str] = None):
    if not note_text.strip():
        return
    note_id = str(uuid.uuid4())
    execute(f"""
        INSERT INTO {_fq('INVOICE_NOTES')}
        (NOTE_ID, INVOICEID, NOTE_TEXT, CREATED_BY)
        VALUES (%s, %s, %s, %s)
    """, (note_id, invoice_id, note_text, created_by))

def _list_notes(invoice_id: str) -> pd.DataFrame:
    return fetch_df(f"""
        SELECT NOTE_ID, NOTE_TEXT, CREATED_AT, CREATED_BY
        FROM {_fq('INVOICE_NOTES')}
        WHERE INVOICEID = %s
        ORDER BY CREATED_AT DESC
    """, (invoice_id,))

def _save_attachment(invoice_id: str, filename: str, mimetype: str, content: bytes):
    attach_id = str(uuid.uuid4())
    execute(f"""
        INSERT INTO {_fq('INVOICE_ATTACHMENTS')}
        (ATTACHMENT_ID, INVOICEID, FILENAME, MIMETYPE, CONTENT)
        VALUES (%s, %s, %s, %s, %s)
    """, (attach_id, invoice_id, filename, mimetype, content))

def _list_attachments(invoice_id: str) -> pd.DataFrame:
    return fetch_df(f"""
        SELECT ATTACHMENT_ID, FILENAME, MIMETYPE, UPLOADED_AT
        FROM {_fq('INVOICE_ATTACHMENTS')}
        WHERE INVOICEID = %s
        ORDER BY UPLOADED_AT DESC
    """, (invoice_id,))


def _delete_attachment(attachment_id: str) -> None:
    execute(
        f"DELETE FROM {_fq('INVOICE_ATTACHMENTS')} WHERE ATTACHMENT_ID = %s",
        (str(attachment_id),),
    )

# --------------------------------------------------------------------------------------
# Reset helpers
# --------------------------------------------------------------------------------------
def _reset_create_form_state():
    keys = [
        "create_year_sel","create_invoice_type","create_prog_sel","create_team_sel","create_group_sel","create_app_sel",
        "create_contract_active","create_renewal_dt","create_amount","create_status","create_amount_ny","create_company_code",
        "create_cost_center","create_serial","create_workorder","create_agreement","create_contract_due","create_service_type",
        "create_notes_inline",
    ]
    for k in keys:
        st.session_state.pop(k, None)
    st.session_state["create_upload_key"] = st.session_state.get("create_upload_key", 0) + 1

def _clear_inline_edit_state():
    for k in [
        "search_edit_invoice_id","search_edit_ctx","search_show_delete_confirm","search_confirm_delete_checkbox",
        "search_edit_contract_active","search_edit_renewal_dt","search_edit_amount","search_edit_status",
        "search_edit_amount_ny","search_edit_company_code","search_edit_cost_center","search_edit_serial",
        "search_edit_workorder","search_edit_agreement","search_edit_contract_due","search_edit_service_type",
        "search_edit_notes_inline",
        # tracking editor
        "track_open_editor","track_editor_expanded",
        # search editor
        "search_open_editor","search_editor_expanded",
    ]:
        st.session_state.pop(k, None)

# --------------------------------------------------------------------------------------
# Cascading selector
# --------------------------------------------------------------------------------------
def _select_team_group_app(prefix: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    programs_df = _with_upper_aliases(cached_list_programs(), ["PROGRAMID", "PROGRAMNAME", "PROGRAMNAME_RAW"])
    teams_df = _with_upper_aliases(cached_list_teams(), ["TEAMID", "TEAMNAME", "TEAMNAME_RAW", "PROGRAMID"])
    teams_df = _normalize_teams_df(teams_df)

    # Fallback: if PROGRAMS names are missing/empty, derive selectable program labels from team-linked PROGRAMIDs.
    if programs_df is None or programs_df.empty or ("PROGRAMNAME" not in programs_df.columns):
        # First try to resolve real program names via TEAM→PROGRAM join.
        try:
            p_from_teams = fetch_df(
                """
                SELECT DISTINCT
                    p.PROGRAMID,
                    COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME,
                    p.PROGRAMNAME AS PROGRAMNAME_RAW
                FROM TEAMS t
                JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                WHERE LTRIM(RTRIM(COALESCE(NULLIF(p.PROGRAM_DISPLAY_NAME, ''), p.PROGRAMNAME, ''))) <> ''
                ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME)
                """,
                None,
            )
            p_from_teams = _with_upper_aliases(p_from_teams, ["PROGRAMID", "PROGRAMNAME", "PROGRAMNAME_RAW"])
            if isinstance(p_from_teams, pd.DataFrame) and not p_from_teams.empty:
                programs_df = p_from_teams
        except Exception:
            pass
        if teams_df is not None and not teams_df.empty and "PROGRAMID" in teams_df.columns:
            pids = (
                teams_df["PROGRAMID"]
                .dropna()
                .astype(str)
                .str.strip()
            )
            pids = pids[pids != ""].drop_duplicates().sort_values()
            if len(pids) > 0:
                programs_df = pd.DataFrame({
                    "PROGRAMID": pids.values,
                    "PROGRAMNAME": [""] * len(pids.values),
                })
        if programs_df is None:
            programs_df = pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME"])
    programs_df = _normalize_programs_df(programs_df)

    prog_names = programs_df["PROGRAMNAME"].tolist() if programs_df is not None and not programs_df.empty else []
    sel_prog_name = st.selectbox("Program", options=["Select Program"] + prog_names, index=0, key=f"{prefix}_prog_sel")
    sel_program_id = None
    if sel_prog_name != "Select Program" and programs_df is not None and not programs_df.empty:
        sel_program_id = programs_df.loc[programs_df["PROGRAMNAME"] == sel_prog_name, "PROGRAMID"].iloc[0]

    if sel_program_id:
        sel_pid = _canon_id(sel_program_id)
        teams_df = teams_df.loc[teams_df["PROGRAMID"].map(_canon_id) == sel_pid]
        # Fallback by program name for schema/data variants where PROGRAMID matching fails.
        if teams_df.empty and sel_prog_name != "Select Program":
            try:
                teams_df = fetch_df(
                    """
                    SELECT
                        t.TEAMID,
                        COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
                        t.TEAMNAME AS TEAMNAME_RAW,
                        t.PROGRAMID
                    FROM TEAMS t
                    JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                    WHERE UPPER(LTRIM(RTRIM(COALESCE(NULLIF(p.PROGRAM_DISPLAY_NAME, ''), p.PROGRAMNAME)))) = UPPER(LTRIM(RTRIM(%s)))
                    ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME)
                    """,
                    (sel_prog_name,),
                )
                teams_df = _with_upper_aliases(teams_df, ["TEAMID", "TEAMNAME", "PROGRAMID"])
                teams_df = _normalize_teams_df(teams_df)
            except Exception:
                pass
    teams_df = teams_df[(teams_df["TEAMID"] != "") & (teams_df["TEAMNAME"] != "")]
    team_enabled = bool(sel_program_id)
    if team_enabled and teams_df.empty:
        st.warning("No Teams found for this selection.")
        return sel_program_id, None, None, None, None

    team_names = teams_df["TEAMNAME"].tolist() if team_enabled else []
    sel_team_name = st.selectbox(
        "Team",
        options=(["Select Team"] + team_names) if team_enabled else ["Select Program first"],
        index=0,
        key=f"{prefix}_team_sel",
        disabled=not team_enabled,
        help="Select Program first to load Teams.",
    )
    if not team_enabled:
        return sel_program_id, None, None, None, None
    if sel_team_name == "Select Team":
        return sel_program_id, None, None, None, None
    sel_team_row = teams_df.loc[teams_df["TEAMNAME"] == sel_team_name].iloc[0]
    sel_team_id = sel_team_row["TEAMID"]

    groups_df = _normalize_groups_df(cached_list_groups_for_team(sel_team_id))
    if groups_df.empty:
        st.info("This team has no Applications yet.")
        return sel_program_id, sel_team_id, sel_team_name, None, None
    group_names = groups_df["GROUPNAME"].tolist()
    sel_group_name = st.selectbox("Application", options=["Select Group"] + group_names, index=0, key=f"{prefix}_group_sel")
    if sel_group_name == "Select Group":
        return sel_program_id, sel_team_id, sel_team_name, None, None
    sel_group_row = groups_df.loc[groups_df["GROUPNAME"] == sel_group_name].iloc[0]
    sel_group_id = sel_group_row["GROUPID"]

    apps_df = _normalize_apps_df(cached_list_applications(group_id=sel_group_id))
    if apps_df.empty:
        st.info("No instances in this Application. Create one in the Applications page.")
        return sel_program_id, sel_team_id, sel_team_name, sel_group_id, None

    apps_df = apps_df.copy()
    apps_df["APPLICATIONID"] = apps_df["APPLICATIONID"].fillna("").astype(str).str.strip()
    apps_df["LABEL"] = apps_df.apply(
        lambda r: f"{r.get('APPLICATIONNAME') or ''}" + (f" — {r.get('ADD_INFO')}" if r.get("ADD_INFO") else ""),
        axis=1,
    )

    def _resolve_default_instance(df: pd.DataFrame, group_id: str) -> Optional[pd.Series]:
        gid = str(group_id or "").strip()
        if gid:
            exact = df.loc[df["APPLICATIONID"].astype(str).str.upper() == f"{gid}__DEFAULT".upper()]
            if not exact.empty:
                return exact.iloc[0]
        if "IS_DEFAULT" in df.columns:
            try:
                is_def = pd.to_numeric(df["IS_DEFAULT"], errors="coerce").fillna(0).astype(int) == 1
                if is_def.any():
                    return df.loc[is_def].iloc[0]
            except Exception:
                pass
        if len(df) == 1:
            return df.iloc[0]
        return None

    default_row = _resolve_default_instance(apps_df, str(sel_group_id))
    multi_instance = len(apps_df) > 1
    show_instance_advanced = st.checkbox(
        "Advanced: choose instance (site/location)",
        value=False,
        key=f"{prefix}_show_instance_advanced",
    )
    show_instance_picker = multi_instance or show_instance_advanced

    if not show_instance_picker and default_row is not None:
        sel_app_id = str(default_row.get("APPLICATIONID") or "").strip() or None
        st.caption("Using the default instance for this Application.")
        return sel_program_id, sel_team_id, sel_team_name, sel_group_id, sel_app_id

    app_labels = apps_df["LABEL"].tolist()
    default_idx = 0
    if default_row is not None:
        default_label = str(default_row.get("LABEL") or "").strip()
        if default_label in app_labels:
            default_idx = 1 + app_labels.index(default_label)
    sel_app_label = st.selectbox(
        "Instance (site/location)",
        options=["Select Instance"] + app_labels,
        index=default_idx,
        key=f"{prefix}_app_sel",
        help="Only needed for Applications with multiple instances.",
    )
    if sel_app_label == "Select Instance":
        return sel_program_id, sel_team_id, sel_team_name, sel_group_id, None
    sel_app_row = apps_df.iloc[app_labels.index(sel_app_label)]
    sel_app_id = sel_app_row["APPLICATIONID"]

    return sel_program_id, sel_team_id, sel_team_name, sel_group_id, sel_app_id

# --------------------------------------------------------------------------------------
# Field renderer (used by Create, Search edit, and Tracking edit)
# --------------------------------------------------------------------------------------
def _edit_create_fields(prefix: str, loaded_invoice: Optional[pd.Series],
                        selected_team_name: Optional[str],
                        invoice_type: Optional[str] = "Recurring Invoice",
                        seed_amount: Optional[float] = None,
                        contract_meta: Optional[dict] = None,
                        show_attachments_uploader: bool = True,
                        compact_layout: bool = False) -> dict:
    show_all = (invoice_type != "Ad Hoc Invoice")
    def _d(col):
        return loaded_invoice.get(col) if loaded_invoice is not None else None

    notes_inline: Optional[str] = _ensure_str(_d("NOTES")) if loaded_invoice is not None else ""

    if compact_layout:
        # Compact modal layout: keep core fields visible and collapse secondary metadata.
        if show_all:
            default_contract_active = bool(_d("CONTRACT_ACTIVE")) if loaded_invoice is not None else True
            contract_active = st.checkbox("Contract Active", value=default_contract_active, key=f"{prefix}_contract_active")
        else:
            contract_active = True

        core1, core2, core3 = st.columns(3, gap="small")
        with core1:
            if loaded_invoice is not None and pd.notna(_d("RENEWALDATE")):
                default_date = pd.to_datetime(_d("RENEWALDATE")).date()
            else:
                if isinstance(contract_meta, dict) and contract_meta.get("DEFAULT_RENEWAL_DATE") is not None:
                    default_date = contract_meta.get("DEFAULT_RENEWAL_DATE")
                else:
                    default_date = date.today()
            renewal_date = st.date_input("Renewal Date", value=default_date, key=f"{prefix}_renewal_dt")
        with core2:
            if loaded_invoice is not None and _d("AMOUNT") is not None:
                default_amount = float(_d("AMOUNT"))
            elif seed_amount is not None:
                default_amount = float(seed_amount)
            else:
                default_amount = 0.0
            amount = st.number_input("Amount (USD)", min_value=0.0, step=0.1, value=default_amount, key=f"{prefix}_amount")
        with core3:
            default_status = _d("STATUS") if loaded_invoice is not None else "Planned"
            status = st.selectbox(
                "Status",
                options=["Planned", "Completed"],
                index=["Planned", "Completed"].index(default_status if default_status in ["Planned", "Completed"] else "Planned"),
                key=f"{prefix}_status",
            )

        core4, core5, core6 = st.columns(3, gap="small")
        with core4:
            if loaded_invoice is not None:
                cc_val = _ensure_str(_d("COMPANY_CODE"))
            else:
                cc_val = (contract_meta.get("COMPANY_CODE") if isinstance(contract_meta, dict) else "") or ""
            company_code = st.text_input("Company Code", value=cc_val, key=f"{prefix}_company_code")
        with core5:
            if loaded_invoice is not None:
                cc_val2 = _ensure_str(_d("COST_CENTER"))
            else:
                cc_val2 = (contract_meta.get("COST_CENTER") if isinstance(contract_meta, dict) else "") or ""
            cost_center = st.text_input("Cost Center", value=cc_val2, key=f"{prefix}_cost_center_compact")
        with core6:
            if invoice_type == "Recurring Invoice":
                st.caption("Amount Next Year comes from the Contract forecast. Update it in Contracts page.")
                amount_next_year = None
            elif show_all:
                default_amount_ny = float(_d("AMOUNT_NEXT_YEAR")) if (loaded_invoice is not None and _d("AMOUNT_NEXT_YEAR") is not None) else 0.0
                amount_next_year = st.number_input("Amount Next Year (USD)", min_value=0.0, step=0.1, value=default_amount_ny, key=f"{prefix}_amount_ny_compact")
            else:
                amount_next_year = None

        with st.expander("Additional metadata", expanded=False):
            m1, m2, m3 = st.columns(3, gap="small")
            with m1:
                work_order = st.text_input("Work Order", value=_ensure_str(_d("WORK_ORDER")) if loaded_invoice is not None else "", key=f"{prefix}_workorder_compact")
            with m2:
                if show_all:
                    serial_number = st.text_input("Serial Number", value=_ensure_str(_d("SERIAL_NUMBER")) if loaded_invoice is not None else "", key=f"{prefix}_serial_compact")
                else:
                    serial_number = None
                    st.empty()
            with m3:
                if show_all:
                    svc_types = ["OnPrem", "SaaS", "IaaS", "Other"]
                    if loaded_invoice is not None:
                        default_svc = _d("SERVICE_TYPE")
                    else:
                        default_svc = (contract_meta.get("SERVICE_TYPE") if isinstance(contract_meta, dict) else None) or "SaaS"
                    service_type = st.selectbox(
                        "Service Type",
                        options=svc_types,
                        index=svc_types.index(default_svc) if default_svc in svc_types else svc_types.index("SaaS"),
                        key=f"{prefix}_service_type_compact",
                    )
                else:
                    service_type = None
                    st.empty()

            m4, m5, m6 = st.columns(3, gap="small")
            with m4:
                if invoice_type == "Recurring Invoice":
                    agr = None
                    if isinstance(contract_meta, dict):
                        agr = contract_meta.get("AGREEMENT_NUMBER")
                    if agr is None:
                        agr = _ensure_str(_d("AGREEMENT_NUMBER")) if loaded_invoice is not None else None
                    st.text_input("Agreement Number", value=str(agr) if agr is not None else "", disabled=True, key=f"{prefix}_agreement_ro_compact")
                    agreement_number = agr
                else:
                    agreement_number = st.text_input("Agreement Number", value=_ensure_str(_d("AGREEMENT_NUMBER")) if loaded_invoice is not None else "", key=f"{prefix}_agreement_compact")
            with m5:
                if invoice_type == "Recurring Invoice":
                    end_fy_val = None
                    if isinstance(contract_meta, dict):
                        end_fy_val = contract_meta.get("END_FY")
                    if end_fy_val is None:
                        end_fy_val = _to_int_opt(_d("CONTRACT_DUE"))
                    st.text_input("Contract Due (FY)", value=str(end_fy_val) if end_fy_val is not None else "", disabled=True, key=f"{prefix}_contract_due_ro_compact")
                    contract_due = end_fy_val
                elif show_all:
                    contract_due_opts = ["undefined"] + [str(y) for y in _safe_years(2026, 2034)]
                    raw_due = _d("CONTRACT_DUE")
                    due_val = _to_int_opt(raw_due)
                    default_due = str(due_val) if due_val is not None else "undefined"
                    due_index = contract_due_opts.index(default_due) if default_due in contract_due_opts else 0
                    contract_due_sel = st.selectbox("Contract Due", options=contract_due_opts, index=due_index, key=f"{prefix}_contract_due_compact")
                    contract_due = None if contract_due_sel == "undefined" else int(contract_due_sel)
                else:
                    contract_due = None
                    st.empty()
            with m6:
                notes_inline = st.text_area("Notes", value=notes_inline or "", key=f"{prefix}_notes_inline_compact", height=84)

        uploaded_files = None
        if show_attachments_uploader:
            st.markdown("**Attachments (optional)**")
            upload_key = f"{prefix}_attachments"
            if prefix.startswith("create"):
                upload_key = f"{upload_key}_{st.session_state.get('create_upload_key', 0)}"
            uploaded_files = st.file_uploader("Upload files", accept_multiple_files=True, key=upload_key)

        return {
            "renewal_date": renewal_date,
            "amount": amount,
            "status": status,
            "amount_next_year": amount_next_year,
            "contract_active": contract_active if show_all else True,
            "company_code": company_code,
            "cost_center": cost_center,
            "serial_number": serial_number,
            "work_order": work_order,
            "agreement_number": agreement_number,
            "contract_due": contract_due if show_all else None,
            "service_type": service_type,
            "notes_inline": notes_inline,
            "uploaded_files": uploaded_files,
            "product_owner_implicit": selected_team_name or None,
        }

    # Row 0: Contract Active (left only)
    r0c1, r0c2 = st.columns(2, gap="small")
    if show_all:
        default_contract_active = bool(_d("CONTRACT_ACTIVE")) if loaded_invoice is not None else True
        with r0c1:
            contract_active = st.checkbox("Contract Active", value=default_contract_active, key=f"{prefix}_contract_active")
    else:
        contract_active = True
    with r0c2:
        st.empty()

    # Row 1: Renewal Date | Company Code
    r1c1, r1c2 = st.columns(2, gap="small")
    with r1c1:
        if loaded_invoice is not None and pd.notna(_d("RENEWALDATE")):
            default_date = pd.to_datetime(_d("RENEWALDATE")).date()
        else:
            if isinstance(contract_meta, dict) and contract_meta.get("DEFAULT_RENEWAL_DATE") is not None:
                default_date = contract_meta.get("DEFAULT_RENEWAL_DATE")
            else:
                default_date = date.today()
        renewal_date = st.date_input("Renewal Date", value=default_date, key=f"{prefix}_renewal_dt")
    with r1c2:
        if loaded_invoice is not None:
            cc_val = _ensure_str(_d("COMPANY_CODE"))
        else:
            cc_val = (contract_meta.get("COMPANY_CODE") if isinstance(contract_meta, dict) else "") or ""
        company_code = st.text_input("Company Code", value=cc_val, key=f"{prefix}_company_code")

    # Row 2: Amount | Amount Next Year (Recurring)  OR  Amount | Cost Center (Ad Hoc)
    r2c1, r2c2 = st.columns(2, gap="small")
    with r2c1:
        # Amount seeded from contract when creating a Recurring Invoice, still editable by user
        if loaded_invoice is not None and _d("AMOUNT") is not None:
            default_amount = float(_d("AMOUNT"))
        elif seed_amount is not None:
            default_amount = float(seed_amount)
        else:
            default_amount = 0.0
        amount = st.number_input("Amount (USD)", min_value=0.0, step=0.1, value=default_amount, key=f"{prefix}_amount")
    with r2c2:
        # For Recurring, do not ask for Amount Next Year (forecasts cover it)
        if invoice_type == "Recurring Invoice":
            st.caption("Amount Next Year comes from the Contract forecast. Update it in Contracts page.")
            amount_next_year = None
            cost_center_r2 = None
        elif show_all:
            default_amount_ny = float(_d("AMOUNT_NEXT_YEAR")) if (loaded_invoice is not None and _d("AMOUNT_NEXT_YEAR") is not None) else 0.0
            amount_next_year = st.number_input("Amount Next Year (USD)", min_value=0.0, step=0.1, value=default_amount_ny, key=f"{prefix}_amount_ny")
            cost_center_r2 = None
        else:
            amount_next_year = None
            cost_center_r2 = st.text_input("Cost Center", value=_ensure_str(_d("COST_CENTER")) if loaded_invoice is not None else "", key=f"{prefix}_cost_center")

    # Row 3: Status | Cost Center (Recurring)  OR  Status | Notes (Ad Hoc)
    r3c1, r3c2 = st.columns(2, gap="small")
    with r3c1:
        default_status = _d("STATUS") if loaded_invoice is not None else "Planned"
        status = st.selectbox(
            "Status",
            options=["Planned", "Completed"],
            index=["Planned", "Completed"].index(default_status if default_status in ["Planned", "Completed"] else "Planned"),
            key=f"{prefix}_status"
        )
    with r3c2:
        if show_all:
            if loaded_invoice is not None:
                cc_val2 = _ensure_str(_d("COST_CENTER"))
            else:
                cc_val2 = (contract_meta.get("COST_CENTER") if isinstance(contract_meta, dict) else "") or ""
            cost_center = st.text_input("Cost Center", value=cc_val2, key=f"{prefix}_cost_center_rec")
        else:
            cost_center = cost_center_r2
            notes_inline = st.text_area("Notes (inline, short)", value=notes_inline or "", key=f"{prefix}_notes_inline", height=84)

    # Row 4: Work Order | Contract Due (Recurring: always read-only)
    r4c1, r4c2 = st.columns(2, gap="small")
    with r4c1:
        work_order = st.text_input("Work Order", value=_ensure_str(_d("WORK_ORDER")) if loaded_invoice is not None else "", key=f"{prefix}_workorder")
    with r4c2:
        if invoice_type == "Recurring Invoice":
            end_fy_val = None
            if isinstance(contract_meta, dict):
                end_fy_val = contract_meta.get("END_FY")
            if end_fy_val is None:
                # fallback to existing invoice field if present
                end_fy_val = _to_int_opt(_d("CONTRACT_DUE"))
            st.text_input("Contract Due (FY)", value=str(end_fy_val) if end_fy_val is not None else "", disabled=True, key=f"{prefix}_contract_due_ro")
            contract_due = end_fy_val
        elif show_all:
            contract_due_opts = ["undefined"] + [str(y) for y in _safe_years(2026, 2034)]
            raw_due = _d("CONTRACT_DUE")
            due_val = _to_int_opt(raw_due)
            default_due = str(due_val) if due_val is not None else "undefined"
            due_index = contract_due_opts.index(default_due) if default_due in contract_due_opts else 0
            contract_due_sel = st.selectbox("Contract Due", options=contract_due_opts, index=due_index, key=f"{prefix}_contract_due")
            contract_due = None if contract_due_sel == "undefined" else int(contract_due_sel)
        else:
            contract_due = None
            st.empty()

    # Row 5: Serial Number | Service Type (Recurring only)
    if show_all:
        r5c1, r5c2 = st.columns(2, gap="small")
        with r5c1:
            serial_number = st.text_input("Serial Number", value=_ensure_str(_d("SERIAL_NUMBER")) if loaded_invoice is not None else "", key=f"{prefix}_serial")
        with r5c2:
            svc_types = ["OnPrem", "SaaS", "IaaS", "Other"]
            if loaded_invoice is not None:
                default_svc = _d("SERVICE_TYPE")
            else:
                default_svc = (contract_meta.get("SERVICE_TYPE") if isinstance(contract_meta, dict) else None) or "SaaS"
            service_type = st.selectbox(
                "Service Type",
                options=svc_types,
                index=svc_types.index(default_svc) if default_svc in svc_types else svc_types.index("SaaS"),
                key=f"{prefix}_service_type"
            )
    else:
        serial_number = None
        service_type = None

    # Row 6: Agreement Number | Notes (Recurring: always read-only; Ad Hoc notes already in Row 3)
    if show_all:
        r6c1, r6c2 = st.columns(2, gap="small")
        with r6c1:
            if invoice_type == "Recurring Invoice":
                agr = None
                if isinstance(contract_meta, dict):
                    agr = contract_meta.get("AGREEMENT_NUMBER")
                if agr is None:
                    agr = _ensure_str(_d("AGREEMENT_NUMBER")) if loaded_invoice is not None else None
                st.text_input("Agreement Number", value=str(agr) if agr is not None else "", disabled=True, key=f"{prefix}_agreement_ro")
                agreement_number = agr
            else:
                agreement_number = st.text_input("Agreement Number", value=_ensure_str(_d("AGREEMENT_NUMBER")) if loaded_invoice is not None else "", key=f"{prefix}_agreement")
        with r6c2:
            notes_inline = st.text_area("Notes (inline, short)", value=notes_inline or "", key=f"{prefix}_notes_inline", height=84)
    else:
        agreement_number = None

    uploaded_files = None
    if show_attachments_uploader:
        # Attachments input
        st.markdown("**Attachments (optional)**")
        upload_key = f"{prefix}_attachments"
        if prefix.startswith("create"):
            upload_key = f"{upload_key}_{st.session_state.get('create_upload_key', 0)}"
        uploaded_files = st.file_uploader("Upload files", accept_multiple_files=True, key=upload_key)

    return {
        "renewal_date": renewal_date,
        "amount": amount,
        "status": status,
        "amount_next_year": amount_next_year,
        "contract_active": contract_active if show_all else True,
        "company_code": company_code,
        "cost_center": cost_center,
        "serial_number": serial_number,
        "work_order": work_order,
        "agreement_number": agreement_number,
        "contract_due": contract_due if show_all else None,
        "service_type": service_type,
        "notes_inline": notes_inline,
        "uploaded_files": uploaded_files,
        "product_owner_implicit": selected_team_name or None,
    }

# --------------------------------------------------------------------------------------
# Save helper
# --------------------------------------------------------------------------------------
def _save_invoice_and_attachments(invoice_id: str, values: dict):
    old_row = None
    try:
        old_df = fetch_df(
            """
            SELECT
              INVOICEID,
              APPLICATIONID,
              TEAMID,
              GROUPID,
              GROUPID_AT_BOOKING,
              PROGRAMID_AT_BOOKING,
              FISCAL_YEAR,
              INVOICEDATE,
              RENEWALDATE,
              AMOUNT,
              SERVICE_TYPE,
              INVOICE_TYPE,
              STATUS
            FROM INVOICES
            WHERE INVOICEID = %s
            """,
            (str(invoice_id),),
        )
        if old_df is not None and not old_df.empty:
            old_row = old_df.iloc[0].to_dict()
    except Exception:
        old_row = None

    upsert_invoice(
        invoice_id=invoice_id,
        application_id=values["application_id"],
        team_id=values["team_id"],
        renewal_date=values["renewal_date"].isoformat() if hasattr(values["renewal_date"], "isoformat") else values["renewal_date"],
        amount=float(values["amount"]),
        status=values["status"],
        fiscal_year=int(values["fiscal_year"]),
        product_owner=_ensure_str(values["product_owner_implicit"]),
        amount_next_year=float(values["amount_next_year"]) if values["amount_next_year"] is not None else None,
        contract_active=bool(values["contract_active"]) if values["contract_active"] is not None else None,
        company_code=_ensure_str(values["company_code"]),
        cost_center=_ensure_str(values["cost_center"]),
        serial_number=_ensure_str(values["serial_number"]),
        work_order=_ensure_str(values["work_order"]),
        agreement_number=_ensure_str(values["agreement_number"]),
        contract_due=values["contract_due"] if values["contract_due"] is not None else None,
        service_type=_ensure_str(values["service_type"]),
        notes=_ensure_str(values["notes_inline"]),
        group_id=_ensure_str(values["group_id"]),
        programid_at_booking=_ensure_str(values.get("programid_at_booking")),
        vendorid_at_booking=_ensure_str(values.get("vendorid_at_booking")),
        groupid_at_booking=_ensure_str(values.get("groupid_at_booking")),
        rollover_batch_id=None,
        rolled_over_from_year=None,
        invoice_type=values.get("invoice_type"),
        updated_by=str(st.session_state.get("auth_user", {}).get("email", "")),
    )

    try:
        new_df = fetch_df(
            """
            SELECT
              INVOICEID,
              APPLICATIONID,
              TEAMID,
              GROUPID,
              GROUPID_AT_BOOKING,
              PROGRAMID_AT_BOOKING,
              FISCAL_YEAR,
              INVOICEDATE,
              RENEWALDATE,
              AMOUNT,
              SERVICE_TYPE,
              INVOICE_TYPE,
              STATUS
            FROM INVOICES
            WHERE INVOICEID = %s
            """,
            (str(invoice_id),),
        )
        if ENABLE_COST_EVENTS and record_cost_event_from_invoice and new_df is not None and not new_df.empty:
            record_cost_event_from_invoice(None, old_row, new_df.iloc[0].to_dict())
    except Exception:
        # Event logging must not block invoice saves.
        pass

    uploads = values.get("uploaded_files")
    if uploads:
        for f in uploads:
            try:
                _save_attachment(invoice_id, f.name, f.type or "application/octet-stream", f.read())
            except Exception as e:
                st.warning(f"Attachment '{f.name}' could not be saved: {e}")

# --------------------------------------------------------------------------------------
# Tracking helpers
# --------------------------------------------------------------------------------------
def _send_threshold_alerts(
    alert_df: pd.DataFrame,
    pending_window_days: int,
    email_cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[int, List[str], List[str]]:
    """Send email alerts for Critical and Pending invoices.

    Returns a tuple of (emails_sent, skipped_due_to_missing_owner, missing_program_manager_cc).
    """
    if alert_df.empty or "Tracking Status" not in alert_df.columns:
        return 0, [], []

    working = alert_df[alert_df["Tracking Status"].isin(["Critical", "Pending"])]
    if working.empty:
        return 0, [], []
    working = working.copy()

    cfg = dict(email_cfg or {})

    def _cfg_bool(val: Any, default: bool = False) -> bool:
        if isinstance(val, str):
            return val.strip().lower() in ("1", "true", "yes", "y", "on")
        if val is None:
            return default
        return bool(val)

    def _split_emails(raw: Any) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, (list, tuple, set)):
            items = raw
        else:
            items = str(raw).replace(";", ",").split(",")
        out: List[str] = []
        for item in items:
            addr = str(item).strip()
            if not addr:
                continue
            out.append(addr)
        return out

    def _merge_addresses(addresses: Iterable[str]) -> List[str]:
        seen = set()
        result: List[str] = []
        for addr in addresses:
            if not addr:
                continue
            key = addr.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(addr)
        return result

    def _clean_email(val: Any) -> Optional[str]:
        try:
            if pd.isna(val):
                return None
        except Exception:
            pass
        s = str(val or "").strip()
        if not s or s.lower() == "nan":
            return None
        return s

    def _fmt_text(val: Any, default: str = "—") -> str:
        try:
            if pd.isna(val):
                return default
        except Exception:
            pass
        s = str(val or "").strip()
        return s if s else default

    def _fmt_date(val: Any) -> str:
        try:
            if pd.isna(val):
                return "—"
        except Exception:
            pass
        try:
            return pd.to_datetime(val).date().isoformat()
        except Exception:
            return _fmt_text(val)

    def _fmt_amount(val: Any) -> str:
        try:
            return f"{float(val):,.2f}"
        except Exception:
            return "0.00"

    subject_prefix = str(cfg.get("subject_prefix") or "[NEXT]").strip() or "[NEXT]"
    subject_template = str(cfg.get("subject_template") or "{prefix} Action needed: {count} invoice(s) {status} for {program}")
    body_intro_template = str(cfg.get("body_intro") or "The following invoices are currently flagged as {status_title}. Please review and take action.")
    body_footer_template = str(cfg.get("body_footer") or "Update the invoice status, add notes, or attach approvals directly in the Invoice Tracking page. This email was generated automatically from the NEXT Invoice Tracking dashboard.")
    footnote_template = str(cfg.get("footnote_template") or "Pending = due within {pending_window_days} day(s). Critical = past due and still planned.")
    save_to_sent_items = _cfg_bool(cfg.get("save_to_sent_items"), False)
    extra_to_global = _split_emails(cfg.get("additional_recipients"))
    extra_cc_global = _split_emails(cfg.get("additional_cc"))
    sender_override = str(cfg.get("mail_sender") or "").strip() or None
    if not sender_override:
        raise EmailSendError("Email sender (mail_sender) is not configured in Email Alerts.")

    if "Responsible Email" not in working.columns:
        raise EmailSendError("Product Owner email column missing from invoice data.")

    working["Product Owner Email"] = working["Responsible Email"].apply(_clean_email)
    missing_owner_df = working[working["Product Owner Email"].isna()].copy()
    working = working[working["Product Owner Email"].notna()].copy()
    if working.empty:
        raise EmailSendError("No product owner email addresses found for the selected invoices.")

    if "Program Manager Email" not in working.columns:
        working["Program Manager Email"] = None
    working["Program Manager Email"] = working["Program Manager Email"].apply(_clean_email)

    if "Program" not in working.columns:
        working["Program"] = None
    working["Program"] = working["Program"].apply(lambda v: _fmt_text(v, "(Unassigned program)"))

    missing_owner_msgs: List[str] = []
    for _, row in missing_owner_df.iterrows():
        team = _fmt_text(row.get("Team"), "Unknown team")
        app = _fmt_text(row.get("Application"), "Unknown application")
        program_name = _fmt_text(row.get("Program"), "(Unassigned program)")
        status = _fmt_text(row.get("Tracking Status"), "?")
        missing_owner_msgs.append(f"{team} – {app} ({program_name}, {status})")

    missing_cc_msgs: List[str] = []
    sent = 0

    def _status_phrase(values: Sequence[str]) -> str:
        unique_vals = [v for v in dict.fromkeys([_fmt_text(v) for v in values]) if v != "—"]
        if not unique_vals:
            return "Critical/Pending"
        if len(unique_vals) == 1:
            return unique_vals[0]
        return ", ".join(unique_vals[:-1]) + f" & {unique_vals[-1]}"

    group_fields = ["Product Owner Email", "Program Manager Email", "Program"]
    for (po_email, pm_email, program_name), grp in working.groupby(group_fields, dropna=False):
        program_label = _fmt_text(program_name, "(Unassigned program)")
        statuses = grp["Tracking Status"].tolist()
        status_phrase = _status_phrase(statuses)
        format_args = {
            "prefix": subject_prefix,
            "program": program_label,
            "program_label": program_label,
            "status": status_phrase.lower(),
            "status_lower": status_phrase.lower(),
            "status_title": status_phrase,
            "count": len(grp),
            "pending_window_days": pending_window_days,
        }

        def _apply_template(raw: str) -> str:
            try:
                return raw.format(**format_args)
            except Exception:
                return raw

        subject = _apply_template(subject_template) or subject_template
        if not subject.strip():
            subject = f"{subject_prefix} Action needed: {len(grp)} invoice(s) {status_phrase.lower()} for {program_label}"

        display_df = grp.sort_values(by=["Renewal Date"], na_position="last").copy()
        display_df["Renewal Date"] = display_df["Renewal Date"].apply(_fmt_date)
        display_df["Amount (USD)"] = display_df["Amount (USD)"].apply(_fmt_amount)
        for col in ["Team", "Application", "Tracking Status"]:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(_fmt_text)

        table_columns = [
            col for col in ["Tracking Status", "Team", "Application", "Renewal Date", "Amount (USD)", "Agreement Number"]
            if col in display_df.columns
        ]
        table_html = display_df[table_columns].to_html(index=False, border=0, justify="left")

        meta_parts = [f"<strong>Program:</strong> {program_label}"]
        if pm_email:
            meta_parts.append(f"<strong>Program Manager (cc):</strong> {pm_email}")

        def _render_block(raw: Optional[str], italic: bool = False) -> str:
            if raw is None:
                return ""
            text = _apply_template(str(raw))
            stripped = text.strip()
            if not stripped:
                return ""
            escaped = html.escape(stripped).replace("\n", "<br />")
            if italic:
                return f"<p><em>{escaped}</em></p>"
            return f"<p>{escaped}</p>"

        intro_html = _render_block(body_intro_template)
        footer_html = _render_block(body_footer_template)
        footnote_html = _render_block(footnote_template, italic=True)

        hello_html = "<p>Hello,</p>"
        meta_html = f"<p>{' | '.join(meta_parts)}</p>" if meta_parts else ""

        lines = [
            hello_html,
            intro_html or f"<p>The following invoices are currently flagged as <strong>{status_phrase}</strong> in the NEXT Invoice Tracking tool. Please review and take action.</p>",
            meta_html,
            table_html,
            footnote_html,
            footer_html,
        ]
        body_html = "\n".join([line for line in lines if line])

        to_list = _merge_addresses([po_email] + extra_to_global)
        cc_list = _merge_addresses(([pm_email] if pm_email else []) + extra_cc_global)

        try:
            send_graph_mail(
                subject=subject,
                body_html=body_html,
                to_recipients=to_list,
                cc_recipients=cc_list or None,
                save_to_sent=save_to_sent_items,
                sender_override=sender_override,
            )
        except EmailSendError as exc:
            raise EmailSendError(f"Failed to send alert to {po_email} ({program_label}): {exc}") from exc

        if not pm_email:
            missing_cc_msgs.append(f"{program_label} → {po_email}")
        sent += 1

    return sent, missing_owner_msgs, missing_cc_msgs

def _badge_html(text: str) -> str:
    colors = {
        "Critical": "#E74C3C",   # red
        "Pending": "#F1C40F",    # yellow
        "Contract": "#E67E22",   # orange
        "OK": "#2ECC71",         # green
        # Keep 'Completed' green for Search tab raw status display
        "Completed": "#2ECC71",
    }
    color = colors.get(text, "#95A5A6")
    fg = "#000" if text in ("Pending", "Contract") else "#fff"
    return (
        '<span style="display:inline-block;white-space:nowrap;'
        'padding:2px 8px;border-radius:999px;vertical-align:middle;'
        f'background:{color};color:{fg};font-weight:700;font-size:0.85em;line-height:1;">'
        f'{text}</span>'
    )

def _dataframe_supports_selection() -> bool:
    try:
        sig = inspect.signature(st.dataframe)
        return "selection_mode" in sig.parameters and "on_select" in sig.parameters
    except Exception:
        return False


def _get_dataframe_selected_row(key: str) -> Optional[int]:
    sel = st.session_state.get(key)
    if sel is None:
        return None
    if isinstance(sel, dict):
        rows = None
        if isinstance(sel.get("selection"), dict):
            rows = sel["selection"].get("rows")
        if rows is None:
            rows = sel.get("rows")
        try:
            if rows and len(rows) > 0:
                return int(rows[0])
        except Exception:
            return None
    return None


@cache_data_portfolio(ttl=60, show_spinner=False)
def cached_fetch_attachment_by_id(attachment_id: str) -> pd.DataFrame:
    return fetch_df(
        f"""
        SELECT ATTACHMENT_ID, INVOICEID, FILENAME, MIMETYPE, CONTENT, UPLOADED_AT
        FROM {_fq('INVOICE_ATTACHMENTS')}
        WHERE ATTACHMENT_ID = %s
        """,
        (attachment_id,),
    )


def _close_invoice_edit_modal() -> None:
    st.session_state["invoice_edit_open"] = False
    st.session_state["invoice_edit_id"] = None


def _open_invoice_edit_modal(invoice_id: str) -> None:
    st.session_state["invoice_edit_id"] = str(invoice_id)
    st.session_state["invoice_edit_open"] = True
    st.rerun()


def _invoice_dialog_wide_css() -> None:
    st.markdown(
        """
        <style>
        /* Streamlit dialog width (best-effort; harmless if selectors don't match) */
        div[data-testid="stDialog"] div[role="dialog"],
        div[data-testid="stModal"] div[role="dialog"],
        div[role="dialog"][aria-modal="true"]{
          width: 96vw !important;
          max-width: 1600px !important;
        }
        div[data-testid="stDialog"] div[role="dialog"] > div,
        div[data-testid="stModal"] div[role="dialog"] > div{
          max-width: 1600px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _resolve_team_app_labels(loaded: pd.Series) -> tuple[str, str]:
    team_label = "—"
    app_label = "—"
    try:
        team_id = str(loaded.get("TEAMID") or "").strip()
        if team_id:
            teams_df = _normalize_teams_df(cached_list_teams())
            if teams_df is not None and not teams_df.empty and "TEAMID" in teams_df.columns:
                m = teams_df.loc[teams_df["TEAMID"].astype(str) == team_id]
                if not m.empty:
                    team_label = str(m.iloc[0].get("TEAMNAME") or "").strip() or team_label
    except Exception:
        pass
    try:
        group_id = str(loaded.get("GROUPID") or "").strip()
        group_book_id = str(loaded.get("GROUPID_AT_BOOKING") or "").strip()
        app_id = str(loaded.get("APPLICATIONID") or "").strip()
        group_name = _group_name_for_id(group_id) if group_id else ""
        if (not group_name) and group_book_id:
            group_name = _group_name_for_id(group_book_id)
        if group_name:
            app_label = group_name
        elif app_id:
            app_fallback = _app_label_for_id(group_id or group_book_id, app_id)
            if app_fallback:
                app_label = app_fallback
    except Exception:
        pass
    return team_label, app_label


def _render_invoice_edit_body(invoice_id: str) -> None:
    inv_df = cached_fetch_invoice_by_id(str(invoice_id))
    if inv_df is None or inv_df.empty:
        st.warning("Selected invoice no longer exists.")
        if st.button("Close", use_container_width=True):
            _close_invoice_edit_modal()
        return

    loaded = inv_df.iloc[0]
    team_label, app_label = _resolve_team_app_labels(loaded)
    status = str(loaded.get("STATUS") or "").strip() or "—"
    amount = float(pd.to_numeric(loaded.get("AMOUNT", 0), errors="coerce") or 0.0)
    renewal = loaded.get("RENEWALDATE")
    try:
        renewal_str = pd.to_datetime(renewal).date().isoformat() if pd.notna(renewal) else "—"
    except Exception:
        renewal_str = "—"

    header_cols = st.columns([1.2, 1.2, 1.4], gap="small")
    with header_cols[0]:
        st.caption("Invoice ID")
        st.code(str(invoice_id), language=None)
    with header_cols[1]:
        st.caption("Status")
        st.markdown(_status_chip_sr(status), unsafe_allow_html=True)
    with header_cols[2]:
        st.caption("Renewal / Amount")
        st.write(f"{renewal_str} · ${amount:,.2f}")
    st.caption(f"{team_label} · {app_label}")

    with st.form(f"invoice_edit_form_{invoice_id}"):
        cmeta = None
        try:
            if (loaded.get("INVOICE_TYPE") or "Recurring Invoice") == "Recurring Invoice":
                cdf2 = fetch_df(
                    """
                    SELECT TOP 1 END_FY, AGREEMENT_NUMBER
                    FROM CONTRACTS
                    WHERE APPLICATIONID = %s AND TEAMID = %s
                    ORDER BY CREATED_AT DESC
                    """,
                    (loaded["APPLICATIONID"], loaded["TEAMID"]),
                )
                if cdf2 is not None and not cdf2.empty:
                    cmeta = {
                        "END_FY": _to_int_opt(cdf2.iloc[0]["END_FY"]),
                        "AGREEMENT_NUMBER": cdf2.iloc[0]["AGREEMENT_NUMBER"],
                    }
        except Exception:
            cmeta = None

        values = _edit_create_fields(
            f"invoice_modal_{invoice_id}",
            loaded_invoice=loaded,
            selected_team_name=None,
            invoice_type=loaded.get("INVOICE_TYPE") or "Recurring Invoice",
            contract_meta=cmeta,
            show_attachments_uploader=False,
            compact_layout=True,
        )
        values.update(
            {
                "application_id": loaded["APPLICATIONID"],
                "team_id": loaded["TEAMID"],
                "group_id": loaded["GROUPID"],
                "fiscal_year": int(loaded["FISCAL_YEAR"]),
                "product_owner_implicit": values["product_owner_implicit"] or None,
                "invoice_type": loaded.get("INVOICE_TYPE") or "Recurring Invoice",
            }
        )

        save = st.form_submit_button("Save", type="primary", use_container_width=True)

    modal_upload_key = f"invoice_modal_attach_upload_{invoice_id}"
    modal_uploaded_files = None
    with st.expander("Attachments", expanded=False):
        st.caption("Uploaded files linked to this invoice.")
        modal_uploaded_files = st.file_uploader(
            "Upload files (optional)",
            accept_multiple_files=True,
            key=modal_upload_key,
        )
        att_df = _list_attachments(str(invoice_id))
        if att_df is None or att_df.empty:
            st.caption("No attachments uploaded yet.")
        else:
            att_view = att_df.copy()
            att_view["FILENAME"] = att_view.get("FILENAME", "").astype(str)
            att_view["MIMETYPE"] = att_view.get("MIMETYPE", "").astype(str)
            att_view["UPLOADED_AT"] = pd.to_datetime(att_view.get("UPLOADED_AT"), errors="coerce")
            st.dataframe(
                att_view[["FILENAME", "MIMETYPE", "UPLOADED_AT"]],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "FILENAME": "File",
                    "MIMETYPE": "Type",
                    "UPLOADED_AT": st.column_config.DatetimeColumn("Uploaded"),
                },
                height=min(320, 56 + 35 * max(1, len(att_view.index))),
            )

            selector_df = att_view.reset_index(drop=True)
            selector_df["__label"] = selector_df.apply(
                lambda r: f"{str(r.get('FILENAME') or 'attachment')} · {str(r.get('MIMETYPE') or 'unknown')}",
                axis=1,
            )
            sel_idx = st.selectbox(
                "Selected attachment",
                options=list(range(len(selector_df))),
                index=0,
                format_func=lambda i: str(selector_df.iloc[int(i)].get("__label") or "attachment"),
                key=f"invoice_modal_attachment_sel_{invoice_id}",
            )
            sel_row = selector_df.iloc[int(sel_idx)]
            aid = str(sel_row.get("ATTACHMENT_ID") or "")
            fname = str(sel_row.get("FILENAME") or "attachment")
            mime = str(sel_row.get("MIMETYPE") or "application/octet-stream")

            data = None
            open_supported = False
            open_too_large = False
            try:
                blob_df = cached_fetch_attachment_by_id(aid)
                if blob_df is not None and not blob_df.empty:
                    data = blob_df.iloc[0].get("CONTENT")
                if isinstance(data, memoryview):
                    data = data.tobytes()
                elif isinstance(data, bytearray):
                    data = bytes(data)
                if data:
                    mime_l = str(mime or "").lower()
                    if len(data) > 1_500_000:
                        open_too_large = True
                    elif mime_l == "application/pdf" or mime_l.startswith("image/"):
                        open_supported = True
            except Exception:
                data = None
                open_supported = False
                open_too_large = False

            action_c1, action_c2, action_c3 = st.columns([0.34, 0.33, 0.33], gap="small")
            with action_c1:
                if data is None:
                    st.caption("File content unavailable.")
                else:
                    st.download_button(
                        "Download",
                        data=data,
                        file_name=fname,
                        mime=mime,
                        use_container_width=True,
                        key=f"invoice_modal_dl_{aid}",
                    )
            with action_c2:
                if open_supported and not open_too_large:
                    if st.button(
                        "Open preview",
                        use_container_width=True,
                        type="secondary",
                        key=f"invoice_modal_preview_{aid}",
                    ):
                        try:
                            st.query_params["attachment_preview_id"] = aid
                        except Exception:
                            try:
                                st.experimental_set_query_params(attachment_preview_id=aid)
                            except Exception:
                                pass
                        st.rerun()
                else:
                    st.button(
                        "Open preview",
                        disabled=True,
                        use_container_width=True,
                        key=f"invoice_modal_preview_disabled_{aid}",
                        help=(
                            "File is too large for browser preview. Download instead."
                            if open_too_large
                            else "Preview supported for PDF/JPG/PNG."
                        ),
                    )
            with action_c3:
                confirm_key = f"invoice_modal_del_confirm_{aid}"
                if st.button(
                    "Delete attachment",
                    type="secondary",
                    disabled=not bool(st.session_state.get(confirm_key, False)),
                    use_container_width=True,
                    key=f"invoice_modal_del_btn_{aid}",
                ):
                    del_status = st.empty()
                    try:
                        _delete_attachment(aid)
                        del_status.success("Attachment deleted permanently.")
                        time.sleep(0.25)
                        post_write_refresh("invoice_attachment_delete", rerun=True, bump_version=True)
                    except Exception as e:
                        del_status.error(f"Delete failed: {e}")
            _ = st.checkbox(
                "Confirm permanent delete",
                value=False,
                key=confirm_key,
                help="Deleted attachments cannot be recovered.",
            )

    if save:
        inline_status = st.empty()
        try:
            inline_status.info("Saving invoice...")
            values["uploaded_files"] = modal_uploaded_files
            if (values.get("invoice_type") or "Recurring Invoice") == "Recurring Invoice":
                if cmeta is not None:
                    values["agreement_number"] = cmeta.get("AGREEMENT_NUMBER")
                    values["contract_due"] = cmeta.get("END_FY")
                values["amount_next_year"] = None
            _save_invoice_and_attachments(str(invoice_id), values)
            label = _invoice_context_label(values.get("team_id"), values.get("group_id"), values.get("application_id"))
            _set_op_feedback("success", f"Invoice updated{': ' + label if label else ''}.", target="invoices_active")
            inline_status.success(f"Invoice updated{': ' + label if label else ''}.")
            time.sleep(0.45)
            st.session_state["invoice_edit_open"] = False
            st.session_state["invoice_edit_id"] = None
            post_write_refresh("invoice_update", rerun=True, bump_version=True)
        except Exception as e:
            _set_op_feedback("error", f"Error saving invoice: {e}", target="invoices_active")
            inline_status.error(f"Error saving invoice: {e}")

    with st.expander("Saved notes history", expanded=False):
        modal_note_key = f"invoice_modal_note_text_{invoice_id}"
        modal_note_save_key = f"invoice_modal_note_save_{invoice_id}"
        modal_note_text = st.text_area(
            "Add a note to history",
            value="",
            height=80,
            key=modal_note_key,
        )
        if st.button("Save note to history", type="primary", key=modal_note_save_key):
            note_status = st.empty()
            try:
                _note_append(str(invoice_id), modal_note_text, created_by=current_user_email)
                note_status.success("Note saved.")
                time.sleep(0.25)
                post_write_refresh("invoice_note_save", rerun=True, bump_version=True)
            except Exception as e:
                note_status.error(f"Could not save note: {e}")
        modal_notes_df = _list_notes(str(invoice_id))
        if modal_notes_df is None or modal_notes_df.empty:
            st.caption("No saved notes yet.")
        else:
            modal_notes_df = modal_notes_df.copy()
            modal_notes_df["CREATED_AT"] = pd.to_datetime(modal_notes_df.get("CREATED_AT"), errors="coerce")
            st.dataframe(
                modal_notes_df[["NOTE_TEXT", "CREATED_AT", "CREATED_BY"]],
                hide_index=True,
                use_container_width=True,
                height=220,
                column_config={
                    "NOTE_TEXT": "Note",
                    "CREATED_AT": st.column_config.DatetimeColumn("Created"),
                    "CREATED_BY": "By",
                },
            )


def _maybe_render_invoice_edit_modal() -> None:
    if not bool(st.session_state.get("invoice_edit_open", False)):
        return
    invoice_id = st.session_state.get("invoice_edit_id")
    if not invoice_id:
        return

    if hasattr(st, "dialog"):
        _invoice_dialog_wide_css()
        @st.dialog("Edit invoice")  # type: ignore[misc]
        def _dlg():
            _render_invoice_edit_body(str(invoice_id))

        _dlg()
    else:
        with st.expander("Edit invoice", expanded=True):
            _render_invoice_edit_body(str(invoice_id))


def render_invoice_table(
    df: pd.DataFrame,
    *,
    section_key: str,
    title: str,
    editable: bool,
    context: str,
    show_tracking_status: bool = False,
    show_responsible_column: bool = True,
) -> None:
    """
    UI-only invoices list renderer:
      1) sorts (same as current behavior)
      2) paginates
      3) displays st.dataframe with selection (if supported)
      4) reads selected row → extracts INVOICEID
      5) shows details + existing edit form for that invoice
    """
    st.markdown(f"### {title}")

    if df is None or df.empty:
        st.info("No rows.")
        return

    df_sorted = df.copy()
    # Tracking list: sort by Days to Renewal (None last), matching existing UI behavior.
    if "Days to Renewal" in df_sorted.columns:
        df_sorted = df_sorted.sort_values(
            by=["Days to Renewal"],
            key=lambda s: s.apply(lambda x: 10**9 if x is None or (isinstance(x, float) and pd.isna(x)) else x),
        )

    total_rows = int(len(df_sorted))
    total_usd = float(pd.to_numeric(df_sorted.get("Amount (USD)", 0), errors="coerce").fillna(0).sum())

    # One-line pagination controls: Rows | Page summary | Prev | Next
    page_size_key = f"{context}_page_size_{section_key}"
    rows_col, summary_col, prev_col, next_col = st.columns([0.16, 0.54, 0.15, 0.15], gap="small")
    with rows_col:
        st.markdown("Rows")
        current_size = int(st.session_state.get(page_size_key, 25) or 25)
        size_options = [10, 25, 50, 100]
        size_index = size_options.index(current_size) if current_size in size_options else 1
        page_size = st.selectbox(
            "Rows",
            options=size_options,
            index=size_index,
            key=page_size_key,
            label_visibility="collapsed",
        )
    page_size = int(page_size or 25)
    max_page = max(1, int(math.ceil(total_rows / float(page_size or 1))))
    page_key = f"{context}_page_{section_key}"
    cur_page = int(st.session_state.get(page_key, 1) or 1)
    cur_page = max(1, min(cur_page, max_page))
    st.session_state[page_key] = cur_page

    start_idx = (cur_page - 1) * int(page_size)
    end_idx = min(total_rows, start_idx + int(page_size))
    with summary_col:
        st.caption(
            f"Page {cur_page}/{max_page} - "
            f"Showing {start_idx + 1:,d}–{end_idx:,d} of {total_rows:,d} - "
            f"Total: ${total_usd:,.2f}"
        )
    with prev_col:
        if st.button("◀ Prev", use_container_width=True, key=f"{context}_prev_{section_key}", disabled=(cur_page <= 1)):
            st.session_state[page_key] = max(1, cur_page - 1)
            st.rerun()
    with next_col:
        if st.button("Next ▶", use_container_width=True, key=f"{context}_next_{section_key}", disabled=(cur_page >= max_page)):
            st.session_state[page_key] = min(max_page, cur_page + 1)
            st.rerun()

    df_page = df_sorted.iloc[start_idx:end_idx].reset_index(drop=True)

    # Build display dataframe (do not mutate the original)
    view = df_page.copy()
    view["Amount (USD)"] = pd.to_numeric(view.get("Amount (USD)", 0), errors="coerce").fillna(0.0).astype(float)
    if "Renewal Date" in view.columns:
        view["Renewal Date"] = pd.to_datetime(view["Renewal Date"], errors="coerce").dt.date
    if "Days to Renewal" in view.columns:
        view["Days to Renewal"] = pd.to_numeric(view["Days to Renewal"], errors="coerce").astype("Int64")
    display_cols: List[str] = []
    if show_tracking_status and "Tracking Status" in view.columns:
        display_cols.append("Tracking Status")
    elif "Status" in view.columns:
        display_cols.append("Status")
    display_cols += ["Application", "Amount (USD)", "Renewal Date"]
    if "Days to Renewal" in view.columns:
        display_cols.append("Days to Renewal")
    display_cols = [c for c in display_cols if c in view.columns]

    # Table + details panel
    table_col, panel_col = st.columns([0.58, 0.42], gap="large")

    selection_state_key = f"{context}_table_sel_{section_key}"
    selected_id_key = f"{context}_selected_invoice_id_{section_key}"
    last_selected_id_key = f"{context}_last_selected_invoice_id_{section_key}"
    attach_key = f"{context}_attach_open_{section_key}"
    note_key = f"{context}_note_open_{section_key}"

    with table_col:
        col_cfg: Dict[str, Any] = {
            "Amount (USD)": st.column_config.NumberColumn("Amount", format="$%.2f"),
            "Renewal Date": st.column_config.DateColumn("Renewal date"),
        }
        if "Days to Renewal" in display_cols:
            col_cfg["Days to Renewal"] = st.column_config.NumberColumn("Days to renewal", format="%d")
        if _dataframe_supports_selection():
            st.dataframe(
                view[display_cols],
                hide_index=True,
                use_container_width=True,
                column_config=col_cfg,
                selection_mode="single-row",
                on_select="rerun",
                key=selection_state_key,
            )
        else:
            st.dataframe(
                view[display_cols],
                hide_index=True,
                use_container_width=True,
                column_config=col_cfg,
            )

        # Update selection from dataframe event (if any), and clear when unselected.
        sel_state = st.session_state.get(selection_state_key)
        rows = None
        selection = None
        if isinstance(sel_state, dict):
            selection = sel_state.get("selection") if isinstance(sel_state.get("selection"), dict) else None
            if selection is not None and "rows" in selection:
                rows = selection.get("rows")
            if rows is None and "rows" in sel_state:
                rows = sel_state.get("rows")
        if rows is not None:
            if len(rows) == 0:
                st.session_state.pop(selected_id_key, None)
            else:
                try:
                    sel_row = int(rows[0])
                except Exception:
                    sel_row = None
                if sel_row is None or not (0 <= sel_row < len(df_page)):
                    st.session_state.pop(selected_id_key, None)
                else:
                    inv_raw = df_page.iloc[int(sel_row)].get("INVOICEID")
                    inv_id = str(inv_raw) if inv_raw is not None and not pd.isna(inv_raw) else None
                    prev_id = str(st.session_state.get(selected_id_key) or "").strip()
                    st.session_state[selected_id_key] = inv_id
                    if str(inv_id or "").strip() != prev_id:
                        st.session_state[attach_key] = False
                        st.session_state[note_key] = False
                    st.session_state[last_selected_id_key] = str(inv_id or "").strip()
        else:
            # If Streamlit reports an explicit "empty selection" without rows, clear the persisted selection.
            if isinstance(sel_state, dict) and (
                ("selection" in sel_state and isinstance(selection, dict) and len(selection) == 0)
                or ("rows" in sel_state and (sel_state.get("rows") in (None, [])))
            ):
                st.session_state.pop(selected_id_key, None)
                st.session_state[attach_key] = False
                st.session_state[note_key] = False

    # Resolve selected invoice row in current scope (best-effort)
    selected_invoice_id = st.session_state.get(selected_id_key)
    selected_row = None
    if selected_invoice_id:
        m = df_sorted[df_sorted.get("INVOICEID").astype(str) == str(selected_invoice_id)]
        if not m.empty:
            selected_row = m.iloc[0]
        else:
            # Avoid stale preview when filters/pagination change.
            st.session_state.pop(selected_id_key, None)
            st.session_state.pop(selection_state_key, None)
            selected_invoice_id = None
            st.session_state[attach_key] = False
            st.session_state[note_key] = False

    with panel_col:
        st.markdown("#### Invoice details")
        if not selected_invoice_id:
            st.info("Select a row to view details.")
            return
        if st.button("Clear selection", use_container_width=True, key=f"{context}_clear_sel_{section_key}"):
            st.session_state.pop(selected_id_key, None)
            st.session_state.pop(selection_state_key, None)
            st.session_state[attach_key] = False
            st.session_state[note_key] = False
            st.rerun()
        if selected_row is None:
            st.warning("Selected invoice is not available in the current results.")
            return

        detail_df = cached_fetch_invoice_by_id(str(selected_invoice_id))
        detail = detail_df.iloc[0] if detail_df is not None and not detail_df.empty else pd.Series(dtype=object)

        team = str(selected_row.get("Team") or "").strip() or "—"
        app = str(selected_row.get("Application") or "").strip() or "—"
        if app == "—":
            gid = str(detail.get("GROUPID") or "").strip()
            gidb = str(detail.get("GROUPID_AT_BOOKING") or "").strip()
            app = _group_name_for_id(gid) or _group_name_for_id(gidb) or "—"

        program = str(selected_row.get("Program") or "").strip() or "—"
        resp = str(selected_row.get("Responsible") or "—")
        amt = float(pd.to_numeric(selected_row.get("Amount (USD)", 0), errors="coerce") or 0.0)
        rdate = selected_row.get("Renewal Date")
        try:
            rdate_str = pd.to_datetime(rdate).date().isoformat() if pd.notna(rdate) else "—"
        except Exception:
            rdate_str = "—"
        inv_type = str(detail.get("INVOICE_TYPE") or selected_row.get("Invoice Type") or "—").strip() or "—"
        agreement = str(detail.get("AGREEMENT_NUMBER") or selected_row.get("Agreement Number") or "—").strip() or "—"
        company_code = str(detail.get("COMPANY_CODE") or "—").strip() or "—"
        cost_center = str(detail.get("COST_CENTER") or "—").strip() or "—"
        service_type = str(detail.get("SERVICE_TYPE") or "—").strip() or "—"
        work_order = str(detail.get("WORK_ORDER") or "—").strip() or "—"
        serial_number = str(detail.get("SERIAL_NUMBER") or "—").strip() or "—"
        contract_due = str(detail.get("CONTRACT_DUE") or selected_row.get("Contract Due") or "—").strip() or "—"
        notes_inline = str(detail.get("NOTES") or "").strip()

        badge_html = ""
        if show_tracking_status and "Tracking Status" in selected_row.index:
            badge_html = _badge_html(str(selected_row.get("Tracking Status") or ""))
        elif "Status" in selected_row.index:
            badge_html = _status_chip_sr(str(selected_row.get("Status") or ""))

        if badge_html:
            st.markdown(badge_html, unsafe_allow_html=True)
        st.markdown(f"**Team:** {team}")
        st.markdown(f"**Program:** {program}")
        st.markdown(f"**Application:** {app}")
        st.markdown(f"**Invoice type:** {inv_type}")
        st.markdown(f"**Amount:** ${amt:,.2f}")
        st.markdown(f"**Renewal date:** {rdate_str}")
        st.markdown(f"**Responsible:** {resp}")
        meta_left, meta_right = st.columns(2, gap="small")
        with meta_left:
            st.markdown(f"**Agreement #:** {agreement}")
            st.markdown(f"**Company code:** {company_code}")
            st.markdown(f"**Cost center:** {cost_center}")
            st.markdown(f"**Service type:** {service_type}")
        with meta_right:
            st.markdown(f"**Work order:** {work_order}")
            st.markdown(f"**Serial #:** {serial_number}")
            st.markdown(f"**Contract due:** {contract_due}")
        if notes_inline:
            st.markdown(f"**Notes:** {notes_inline}")

        btn1, btn2, btn3 = st.columns(3, gap="small")

        with btn1:
            if st.button("Edit invoice", disabled=(not editable), use_container_width=True, key=f"{context}_edit_btn_{section_key}"):
                _open_invoice_edit_modal(str(selected_invoice_id))
        with btn2:
            if st.button("Preview attachments", use_container_width=True, key=f"{attach_key}_btn"):
                st.session_state[attach_key] = not bool(st.session_state.get(attach_key, False))
        with btn3:
            if st.button("Add note", use_container_width=True, key=f"{note_key}_btn"):
                st.session_state[note_key] = not bool(st.session_state.get(note_key, False))

        if bool(st.session_state.get(attach_key, False)):
            with st.expander("Attachments", expanded=True):
                att = _list_attachments(str(selected_invoice_id))
                if att is None or att.empty:
                    st.info("No attachments found for this invoice.")
                else:
                    att = att.copy()
                    att["FILENAME"] = att.get("FILENAME", "").astype(str)
                    att["MIMETYPE"] = att.get("MIMETYPE", "").astype(str)
                    att["UPLOADED_AT"] = pd.to_datetime(att.get("UPLOADED_AT"), errors="coerce")
                    att = att.reset_index(drop=True)

                    st.dataframe(
                        att[["FILENAME", "MIMETYPE", "UPLOADED_AT"]],
                        hide_index=True,
                        use_container_width=True,
                        column_config={
                            "FILENAME": "File",
                            "MIMETYPE": "Type",
                            "UPLOADED_AT": st.column_config.DatetimeColumn("Uploaded"),
                        },
                        height=min(260, 56 + 35 * max(1, len(att.index))),
                    )

                    att["__label"] = att.apply(
                        lambda r: f"{str(r.get('FILENAME') or 'attachment')} · {str(r.get('MIMETYPE') or 'unknown')}",
                        axis=1,
                    )
                    sel_idx = st.selectbox(
                        "Selected attachment",
                        options=list(range(len(att))),
                        index=0,
                        format_func=lambda i: str(att.iloc[int(i)].get("__label") or "attachment"),
                        key=f"{context}_att_sel_{section_key}_{selected_invoice_id}",
                    )
                    sel = att.iloc[int(sel_idx)]
                    aid = str(sel.get("ATTACHMENT_ID") or "")
                    fname = str(sel.get("FILENAME") or "attachment")
                    mime = str(sel.get("MIMETYPE") or "application/octet-stream")

                    data = None
                    open_supported = False
                    open_too_large = False
                    try:
                        blob_df = cached_fetch_attachment_by_id(aid)
                        if blob_df is not None and not blob_df.empty:
                            data = blob_df.iloc[0].get("CONTENT")
                        if isinstance(data, memoryview):
                            data = data.tobytes()
                        elif isinstance(data, bytearray):
                            data = bytes(data)
                        if data:
                            mime_l = str(mime or "").lower()
                            if len(data) > 1_500_000:
                                open_too_large = True
                            elif mime_l == "application/pdf" or mime_l.startswith("image/"):
                                open_supported = True
                    except Exception:
                        data = None

                    a1, a2 = st.columns([0.6, 0.4], gap="small")
                    with a1:
                        if open_supported and not open_too_large:
                            if st.button(
                                "Preview selected",
                                use_container_width=True,
                                type="primary",
                                key=f"{context}_att_preview_{section_key}_{aid}",
                            ):
                                try:
                                    st.query_params["attachment_preview_id"] = aid
                                except Exception:
                                    try:
                                        st.experimental_set_query_params(attachment_preview_id=aid)
                                    except Exception:
                                        pass
                                st.rerun()
                        else:
                            st.button(
                                "Preview selected",
                                disabled=True,
                                use_container_width=True,
                                key=f"{context}_att_preview_disabled_{section_key}_{aid}",
                                help=(
                                    "File is too large for browser preview. Use Download."
                                    if open_too_large
                                    else "Preview supported for PDF/JPG/PNG."
                                ),
                            )
                    with a2:
                        if data is None:
                            st.caption("Download unavailable")
                        else:
                            st.download_button(
                                "Download selected",
                                data=data,
                                file_name=fname,
                                mime=mime,
                                use_container_width=True,
                                key=f"dl_{context}_{section_key}_{aid}",
                            )

        if bool(st.session_state.get(note_key, False)):
            with st.expander("Notes", expanded=True):
                note_text = st.text_area("Add a note", value="", height=80, key=f"note_text_{context}_{section_key}_{selected_invoice_id}")
                if st.button("Save note", type="primary", key=f"note_save_{context}_{section_key}_{selected_invoice_id}"):
                    save_status = st.status("Saving note...", expanded=False) if hasattr(st, "status") else None
                    try:
                        _note_append(str(selected_invoice_id), note_text, created_by=current_user_email)
                        if save_status is not None:
                            save_status.update(label="Note saved.", state="complete")
                        st.success("Note added.")
                        post_write_refresh("invoice_note_save", rerun=True, bump_version=True)
                    except Exception as e:
                        if save_status is not None:
                            save_status.update(label="Save failed.", state="error")
                        st.error(f"Could not save note: {e}")
                notes_df = _list_notes(str(selected_invoice_id))
                if notes_df is not None and not notes_df.empty:
                    notes_df = notes_df.copy()
                    notes_df["CREATED_AT"] = pd.to_datetime(notes_df.get("CREATED_AT"), errors="coerce")
                    st.dataframe(
                        notes_df[["NOTE_TEXT", "CREATED_AT", "CREATED_BY"]],
                        hide_index=True,
                        use_container_width=True,
                        height=220,
                        column_config={
                            "NOTE_TEXT": "Note",
                            "CREATED_AT": st.column_config.DatetimeColumn("Created"),
                            "CREATED_BY": "By",
                        },
                    )


if hasattr(st, "fragment"):
    @st.fragment
    def render_invoice_table_adaptive(
        df: pd.DataFrame,
        *,
        section_key: str,
        title: str,
        editable: bool,
        context: str,
        show_tracking_status: bool = False,
        show_responsible_column: bool = True,
    ) -> None:
        # Fragment rerun keeps page chrome stable while selecting rows.
        render_invoice_table(
            df,
            section_key=section_key,
            title=title,
            editable=editable,
            context=context,
            show_tracking_status=show_tracking_status,
            show_responsible_column=show_responsible_column,
        )
else:
    def render_invoice_table_adaptive(
        df: pd.DataFrame,
        *,
        section_key: str,
        title: str,
        editable: bool,
        context: str,
        show_tracking_status: bool = False,
        show_responsible_column: bool = True,
    ) -> None:
        render_invoice_table(
            df,
            section_key=section_key,
            title=title,
            editable=editable,
            context=context,
            show_tracking_status=show_tracking_status,
            show_responsible_column=show_responsible_column,
        )

def _status_chip_sr(status: str) -> str:
    """Badge for Search tab raw invoice Status (Planned/Completed)."""
    if str(status).strip() == "Completed":
        return _badge_html("Completed")
    return (
        '<span style="display:inline-block;padding:2px 8px;border-radius:999px;'
        f'background:{_palette["border"]};color:{_palette["text"]};font-weight:700;font-size:0.85em;">Planned</span>'
    )

# --------------------------------------------------------------------------------------
# UI
# --------------------------------------------------------------------------------------
_scope = read_scope_from_session(fetch_df)
_role = infer_role(_scope)
render_page_header(
    "Invoices",
    "Track recurring and ad hoc invoice commitments.",
    scope_label(_scope, _role),
    _role,
    scope_status_right=touch_last_updated_status("invoices"),
)
# Keep dialog width styling persistent across reruns to avoid close-time width flicker.
_invoice_dialog_wide_css()


def _preview_mode_attachment() -> bool:
    """Same-tab attachment preview mode via query param."""
    try:
        qp = st.query_params
        attachment_id = str(qp.get("attachment_preview_id", "") or "").strip()
    except Exception:
        try:
            q2 = st.experimental_get_query_params()
            v = q2.get("attachment_preview_id")
            attachment_id = str(v[0] if isinstance(v, list) and v else (v or "")).strip()
        except Exception:
            attachment_id = ""
    if not attachment_id:
        return False

    if st.button("Back to Invoices", key="invoice_preview_back"):
        try:
            del st.query_params["attachment_preview_id"]
        except Exception:
            try:
                st.experimental_set_query_params()
            except Exception:
                pass
        st.rerun()

    st.title("Attachment Preview")
    blob_df = cached_fetch_attachment_by_id(attachment_id)
    if blob_df is None or blob_df.empty:
        st.error("Attachment not found.")
        return True
    row = blob_df.iloc[0]
    fname = str(row.get("FILENAME") or "attachment")
    mime = str(row.get("MIMETYPE") or "application/octet-stream")
    data = row.get("CONTENT")
    if isinstance(data, memoryview):
        data = data.tobytes()
    elif isinstance(data, bytearray):
        data = bytes(data)
    if not data:
        st.error("Attachment content is unavailable.")
        return True

    st.caption(f"{fname} · {mime}")
    st.download_button(
        "Download file",
        data=data,
        file_name=fname,
        mime=mime,
        use_container_width=False,
        key=f"preview_dl_{attachment_id}",
    )

    mime_l = mime.lower()
    if mime_l.startswith("image/"):
        st.markdown(
            "<div style='border:1px solid #D9D9D9;border-radius:10px;padding:10px;margin-top:6px;'>",
            unsafe_allow_html=True,
        )
        st.image(data, caption=fname, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    elif mime_l == "application/pdf":
        # Render PDF directly in the page to avoid srcdoc/sandbox blank-preview issues.
        if len(data) > 6_000_000:
            st.info("PDF is large. Use Download for a more reliable open.")
        else:
            b64 = base64.b64encode(data).decode("ascii")
            st.markdown(
                f"""
                <div style="border:1px solid #D9D9D9;border-radius:10px;padding:10px;margin-top:6px;">
                    <iframe
                        src="data:application/pdf;base64,{b64}"
                        width="100%"
                        height="900"
                        style="border:0;border-radius:6px;"
                        title="Attachment PDF preview">
                    </iframe>
                </div>
                """,
                unsafe_allow_html=True,
            )
    else:
        st.info("Inline preview is supported for PDF and images. Use Download for this file type.")

    return True


if _preview_mode_attachment():
    st.stop()

def _is_selection_rerun() -> bool:
    for k, v in st.session_state.items():
        if not (isinstance(k, str) and "_table_sel_" in k):
            continue
        if isinstance(v, dict):
            sel = v.get("selection")
            if isinstance(sel, dict) and isinstance(sel.get("rows"), list):
                return True
            if isinstance(v.get("rows"), list):
                return True
    return False


if _is_selection_rerun():
    # Avoid loader UI flash for row-selection reruns; still warm caches silently.
    _ = cached_list_programs()
    _ = cached_list_teams()
    _ = cached_list_application_groups()
    _ = cached_list_vendors()
else:
    with page_loader(
        messages=[
            "Loading scope and filters...",
            "Building the cost model...",
            "Aggregating...",
            "Preparing charts...",
            "Finalizing view...",
        ]
    ) as loader:
        loader.step("Loading scope and filters...")
        _ = cached_list_programs()
        _ = cached_list_teams()
        _ = cached_list_application_groups()
        _ = cached_list_vendors()
        loader.step("Finalizing view...")


_, top_notice_col = st.columns([1.2, 3.8])
with top_notice_col:
    warn_msg = st.session_state.get("warn_actuals_only")
    if warn_msg:
        st.info(warn_msg)

invoice_sections = ["Tracking", "Search", "Create New"]
active_invoice_section = render_section_picker(
    key="invoices_active_section",
    options=invoice_sections,
    label="Section",
    default="Tracking",
    label_visibility="collapsed",
)
if active_invoice_section not in invoice_sections:
    active_invoice_section = "Tracking"

# Global invoice editor modal (opened from list buttons)
_maybe_render_invoice_edit_modal()

# =======================================
# TAB — Create new
# =======================================
if active_invoice_section == "Create New":
    _render_op_feedback("create")
    _render_op_feedback("invoices_active")
    st.subheader("Create new invoice")

    if st.session_state.pop("do_reset_create_form", False):
        _reset_create_form_state()

    this_year = date.today().year
    fy_list_new = _safe_years(2020, 2036)
    fy_idx_new = fy_list_new.index(this_year) if this_year in fy_list_new else 0
    fy_choice_new = st.selectbox("Year", options=[str(y) for y in fy_list_new], index=fy_idx_new, key="create_year_sel")
    fiscal_year_new = int(fy_choice_new)

    itype_options = ["Select Invoice Type", "Recurring Invoice", "Ad Hoc Invoice"]
    itype_choice = st.selectbox("Invoice Type", options=itype_options, index=0, key="create_invoice_type")
    invoice_type_new = None if itype_choice == "Select Invoice Type" else itype_choice

    program_id_c, team_id_c, team_name_c, group_id_c, app_id_c = _select_team_group_app("create")

    # Prevent duplicate recurring invoice (same Application/instance + FY)
    duplicate_exists = False
    if invoice_type_new == "Recurring Invoice" and app_id_c and fiscal_year_new:
        try:
            chk = fetch_df(
                """
                SELECT COUNT(*) AS CNT
                FROM INVOICES
                WHERE APPLICATIONID=%s AND FISCAL_YEAR=%s
                  AND COALESCE(INVOICE_TYPE,'Recurring Invoice')='Recurring Invoice'
                """,
                (app_id_c, int(fiscal_year_new))
            )
            duplicate_exists = (not chk.empty) and int(chk.iloc[0]["CNT"] or 0) > 0
        except Exception:
            duplicate_exists = False

    # For Recurring Invoice, require an existing Contract for (App × Team)
    contract_row = None
    contract_seed_amount = None
    contract_meta = None
    contract_block_reason = None
    if invoice_type_new == "Recurring Invoice" and app_id_c and team_id_c:
        try:
            cdf = fetch_df(
                """
                SELECT TOP 1 CONTRACT_ID, APPLICATIONID, TEAMID, START_FY, END_FY, RENEWAL_MONTH,
                       ANNUAL_AMOUNT, ESCALATION_PCT, STATUS, AGREEMENT_NUMBER,
                       COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE
                FROM CONTRACTS
                WHERE APPLICATIONID = %s AND TEAMID = %s
                ORDER BY CREATED_AT DESC
                """,
                (app_id_c, team_id_c)
            )
            if cdf is None or cdf.empty:
                contract_block_reason = "No contract found for this Application × Team. Create it in the Contracts tab before booking a Recurring Invoice."
            else:
                r = cdf.iloc[0]
                start_fy = _to_int_opt(r.get("START_FY"))
                end_fy = _to_int_opt(r.get("END_FY"))
                base = float(r.get("ANNUAL_AMOUNT") or 0.0)
                esc = float(r.get("ESCALATION_PCT") or 0.0)
                if start_fy is None or end_fy is None:
                    contract_block_reason = "Contract is missing START_FY/END_FY. Edit the contract."
                elif not (start_fy <= fiscal_year_new <= end_fy):
                    contract_block_reason = f"Selected Year {fiscal_year_new} is outside contract window ({start_fy}..{end_fy})."
                else:
                    years_delta = fiscal_year_new - int(start_fy)
                    contract_seed_amount = round(base * ((1 + esc/100.0) ** years_delta), 2)
                    # Compute a default renewal date for the selected FY
                    inv_rd = r.get("INVOICE_RENEWAL_DATE")
                    con_rd = r.get("CONTRACT_RENEWAL_DATE")
                    try:
                        inv_rd = pd.to_datetime(inv_rd).date() if pd.notna(inv_rd) else None
                    except Exception:
                        inv_rd = None
                    try:
                        con_rd = pd.to_datetime(con_rd).date() if pd.notna(con_rd) else None
                    except Exception:
                        con_rd = None
                    # Prefer invoice renewal day/month, then contract renewal, else renewal_month/1
                    if inv_rd is not None:
                        default_renewal_date = date(int(fiscal_year_new), int(inv_rd.month), int(inv_rd.day))
                    elif con_rd is not None:
                        default_renewal_date = date(int(fiscal_year_new), int(con_rd.month), int(con_rd.day))
                    else:
                        rm = _to_int_opt(r.get("RENEWAL_MONTH")) or 1
                        default_renewal_date = date(int(fiscal_year_new), int(rm), 1)

                    contract_meta = {
                        "END_FY": end_fy,
                        "AGREEMENT_NUMBER": r.get("AGREEMENT_NUMBER"),
                        "COMPANY_CODE": r.get("COMPANY_CODE"),
                        "COST_CENTER": r.get("COST_CENTER"),
                        "SERVICE_TYPE": r.get("SERVICE_TYPE"),
                        "DEFAULT_RENEWAL_DATE": default_renewal_date,
                    }
        except Exception as e:
            contract_block_reason = f"Could not read contract: {e}"

    if duplicate_exists:
        st.error("A **Recurring Invoice** already exists for this Application in the selected Year. "
                 "Use the **Search** tab to view/edit the existing invoice.")
    elif invoice_type_new == "Recurring Invoice" and contract_block_reason:
        st.error(contract_block_reason)
        st.info("Go to the Contracts tab to create or update the contract.")
    elif all([fiscal_year_new, invoice_type_new, team_id_c, group_id_c, app_id_c]):
        with st.expander("Start entering details", expanded=True):
            with st.form("create_form"):
                values_c = _edit_create_fields(
                    "create",
                    loaded_invoice=None,
                    selected_team_name=team_name_c,
                    invoice_type=invoice_type_new,
                    seed_amount=contract_seed_amount,
                    contract_meta=contract_meta,
                )
                values_c.update({
                    "application_id": app_id_c,
                    "team_id": team_id_c,
                    "group_id": group_id_c,
                    "fiscal_year": fiscal_year_new,
                    "product_owner_implicit": values_c["product_owner_implicit"] or None,
                    "invoice_type": invoice_type_new,
                })

                # Contract / recurrence UI
                st.markdown("---")
                if invoice_type_new == "Recurring Invoice":
                    create_contract = False
                    st.info("Recurring Invoices require an existing Contract. Edit Contract values in the Contracts tab.")
                    # Placeholders for variables referenced below
                    contract_start_fy = fiscal_year_new
                    contract_end_fy = fiscal_year_new
                    renewal_month = 1
                    escalation_pct = 0.0
                else:
                    st.markdown("**Contract / Recurrence (optional)**")
                    create_contract = st.checkbox(
                        "Create/Update Contract for this Application × Team (use for forecasts)",
                        value=True,
                        help="Stores contract metadata (start/end fiscal year, renewal month, escalation) to generate future forecast rows.",
                        key="create_contract_toggle"
                    )
                    colc1, colc2, colc3, colc4 = st.columns([1,1,1,1])
                    with colc1:
                        contract_start_fy = st.number_input("Contract Start FY", value=int(fiscal_year_new), step=1, min_value=2000, max_value=2100, key="contract_start_fy")
                    with colc2:
                        contract_end_fy = st.number_input("Contract End FY", value=int(fiscal_year_new), step=1, min_value=2000, max_value=2100, key="contract_end_fy")
                    with colc3:
                        renewal_month = st.number_input("Renewal Month (1-12)", value=1, min_value=1, max_value=12, step=1, key="contract_renewal_month")
                    with colc4:
                        escalation_pct = st.number_input("Escalation % per year", value=0.0, min_value=0.0, max_value=100.0, step=0.1, key="contract_escalation")

                invalid_contract_window = create_contract and (contract_start_fy > contract_end_fy)
                if invalid_contract_window:
                    st.error("Contract Start FY must be less than or equal to End FY.")

                row = st.columns([0.22, 0.22, 0.56])
                with row[0]:
                    save_new_clicked = st.form_submit_button("Save Invoice", type="primary")
                with row[1]:
                    reset_clicked = st.form_submit_button("Reset form", type="secondary")

            if save_new_clicked:
                try:
                    if invalid_contract_window:
                        st.error("Fix the contract window before saving.")
                    else:
                        contract_warn: Optional[str] = None
                        status_ctx = st.status("Saving invoice...", expanded=False) if hasattr(st, "status") else nullcontext()
                        with status_ctx as op:
                            invoice_id_new = str(uuid.uuid4())
                            # Ensure contract values are applied for Recurring: agreement, due, and drop amount_next_year
                            if invoice_type_new == "Recurring Invoice":
                                if contract_meta is not None:
                                    values_c["agreement_number"] = contract_meta.get("AGREEMENT_NUMBER")
                                    values_c["contract_due"] = contract_meta.get("END_FY")
                                values_c["amount_next_year"] = None
                            _save_invoice_and_attachments(invoice_id_new, values_c)
                            # Optionally upsert a contract for dynamic forecasts
                            if (invoice_type_new == "Recurring Invoice") and values_c.get("application_id") and values_c.get("team_id") and create_contract:
                                try:
                                    ensure_contracts_table()
                                    # Use invoice amount as base annual amount; fallback to 0
                                    base_amount = float(values_c.get("amount") or 0.0)
                                    # Stable contract id per App×Team
                                    contract_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"contract:{values_c['application_id']}:{values_c['team_id']}"))
                                    upsert_contract(
                                        contract_id=contract_id,
                                        application_id=values_c["application_id"],
                                        team_id=values_c["team_id"],
                                        start_fy=int(contract_start_fy),
                                        end_fy=int(contract_end_fy),
                                        renewal_month=int(renewal_month),
                                        annual_amount=base_amount,
                                        escalation_pct=float(escalation_pct or 0.0),
                                        status='Active',
                                        agreement_number=None,
                                        updated_by=current_user_email,
                                    )
                                except Exception as e:
                                    contract_warn = f"Contract upsert failed (invoice saved): {e}"
                                    st.warning(contract_warn)
                            if op is not None:
                                op.update(label="Invoice saved.", state="complete")
                        label = _invoice_context_label(team_id_c, group_id_c, app_id_c)
                        if contract_warn:
                            _set_op_feedback("warning", f"Invoice saved{': ' + label if label else ''}. {contract_warn}", target="create")
                        else:
                            _set_op_feedback("success", f"Invoice saved{': ' + label if label else ''}.", target="create")
                        st.success(f"Invoice saved{': ' + label if label else ''}.")
                        post_write_refresh("invoice_create", rerun=True, bump_version=True)
                except Exception as e:
                    _set_op_feedback("error", f"Error saving invoice: {e}", target="create")
                    st.error(f"Error saving invoice: {e}")
            if reset_clicked:
                st.session_state["do_reset_create_form"] = True
                st.rerun()
    else:
        st.info("Select Invoice Type → Program → Team → Application to create an invoice.")

# =======================================
# TAB — Search (filters + compact list + inline expander)
# =======================================
if active_invoice_section == "Search":
    _render_op_feedback("search")
    _render_op_feedback("invoices_active")
    st.subheader("Search invoices")
    search_defaults_applied = bool(st.session_state.get("search_defaults_applied", False))
    user_prog_default_s, user_team_default_s = _user_default_program_team()

    # First filter row
    sc1, sc2, sc3, sc4, sc5 = st.columns([1.0, 1.2, 1.4, 1.7, 2.0])

    with sc1:
        years = ["All"] + [str(y) for y in _safe_years(2020, 2036)]
        default_year = str(date.today().year)
        fy_default_idx = years.index(default_year) if default_year in years else 0
        fy_val = st.selectbox("Year", options=years, index=fy_default_idx, key="search_fy")
        fy_filter = None if fy_val == "All" else int(fy_val)

    with sc2:
        progs = _normalize_programs_df(cached_list_programs())
        prog_names = ["All"] + (progs["PROGRAMNAME"].tolist() if not progs.empty else [])
        if not search_defaults_applied:
            cur_prog = st.session_state.get("search_prog")
            if (cur_prog is None) or (cur_prog not in prog_names) or (cur_prog == "All"):
                desired_prog = user_prog_default_s if user_prog_default_s in prog_names else "All"
                st.session_state["search_prog"] = desired_prog
        else:
            cur_prog = st.session_state.get("search_prog")
            if cur_prog is not None and cur_prog not in prog_names:
                st.session_state["search_prog"] = "All"
        sel_prog_name = st.selectbox("Program", options=prog_names, index=0, key="search_prog")
        prog_id_s = None
        if sel_prog_name != "All" and not progs.empty:
            prog_id_s = progs.loc[progs["PROGRAMNAME"] == sel_prog_name, "PROGRAMID"].iloc[0]

    with sc3:
        team_filter_enabled_s = bool(prog_id_s)
        teams = _normalize_teams_df(cached_list_teams()) if team_filter_enabled_s else pd.DataFrame()
        if team_filter_enabled_s and prog_id_s:
            teams = teams.loc[teams["PROGRAMID"].map(_canon_id) == _canon_id(prog_id_s)]
            if teams.empty and sel_prog_name != "All":
                try:
                    teams = _normalize_teams_df(
                        fetch_df(
                            """
                            SELECT
                                t.TEAMID,
                                COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
                                t.TEAMNAME AS TEAMNAME_RAW,
                                t.PROGRAMID
                            FROM TEAMS t
                            JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                            WHERE UPPER(LTRIM(RTRIM(COALESCE(NULLIF(p.PROGRAM_DISPLAY_NAME, ''), p.PROGRAMNAME)))) = UPPER(LTRIM(RTRIM(%s)))
                            ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME)
                            """,
                            (sel_prog_name,),
                        )
                    )
                except Exception:
                    pass
        if team_filter_enabled_s:
            team_names = ["All"] + (teams["TEAMNAME"].tolist() if not teams.empty else [])
        else:
            team_names = ["Select Program first"]
        if not search_defaults_applied:
            cur_team = st.session_state.get("search_team")
            if (cur_team is None) or (cur_team not in team_names) or (cur_team == "All"):
                desired_team = user_team_default_s if user_team_default_s in team_names else "All"
                st.session_state["search_team"] = desired_team
        else:
            cur_team = st.session_state.get("search_team")
            if cur_team is not None and cur_team not in team_names:
                st.session_state["search_team"] = "All"
        sel_team_name = st.selectbox(
            "Team",
            options=team_names,
            index=0,
            key="search_team",
            disabled=not team_filter_enabled_s,
            help="Select Program first to load Teams.",
        )
        team_id_s = None
        if team_filter_enabled_s and sel_team_name != "All" and not teams.empty:
            team_id_s = teams.loc[teams["TEAMNAME"] == sel_team_name, "TEAMID"].iloc[0]
    if not search_defaults_applied:
        st.session_state["search_defaults_applied"] = True

    with sc4:
        team_id_s_norm = str(team_id_s or "").strip()
        groups = _normalize_groups_df(cached_list_groups_for_team(team_id_s_norm) if team_id_s_norm else cached_list_application_groups())
        if groups.empty and team_id_s_norm:
            try:
                groups = _normalize_groups_df(
                    fetch_df(
                        "SELECT GROUPID, GROUPNAME FROM APPLICATION_GROUPS WHERE TEAMID=%s ORDER BY GROUPNAME",
                        (team_id_s_norm,),
                    )
                )
            except Exception:
                pass
        group_filter_enabled = bool(team_id_s_norm)
        if group_filter_enabled:
            group_names = ["All"] + (groups["GROUPNAME"].tolist() if not groups.empty else [])
            if len(group_names) <= 1:
                group_names = ["All"]
        else:
            group_names = ["Select Team first"]
        sel_group_name = st.selectbox(
            "Application",
            options=group_names,
            index=0,
            key="search_group",
            disabled=not group_filter_enabled,
            help="Select Program and Team first to load Applications.",
        )
        group_id_s = None
        if group_filter_enabled and sel_group_name != "All" and not groups.empty:
            group_id_s = groups.loc[groups["GROUPNAME"] == sel_group_name, "GROUPID"].iloc[0]

    with sc5:
        group_id_s_norm = str(group_id_s or "").strip()
        team_id_s_norm = str(team_id_s or "").strip()
        if group_id_s_norm:
            apps = _normalize_apps_df(cached_list_applications(group_id=group_id_s_norm))
        elif team_id_s_norm:
            apps = _normalize_apps_df(cached_list_applications(team_id=team_id_s_norm))
        else:
            apps = _normalize_apps_df(cached_list_applications())
        if apps.empty and group_id_s_norm:
            try:
                apps = _normalize_apps_df(
                    fetch_df(
                        """
                        SELECT APPLICATIONID, APPLICATIONNAME, ADD_INFO, GROUPID
                        FROM APPLICATIONS
                        WHERE GROUPID=%s
                        ORDER BY APPLICATIONNAME
                        """,
                        (group_id_s_norm,),
                    )
                )
            except Exception:
                pass
        elif apps.empty and team_id_s_norm:
            try:
                apps = _normalize_apps_df(
                    fetch_df(
                        """
                        SELECT a.APPLICATIONID, a.APPLICATIONNAME, a.ADD_INFO, a.GROUPID
                        FROM APPLICATIONS a
                        JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
                        WHERE g.TEAMID=%s
                        ORDER BY a.APPLICATIONNAME
                        """,
                        (team_id_s_norm,),
                    )
                )
            except Exception:
                pass
        apps = apps.copy()
        if not apps.empty:
            apps["LABEL"] = apps.apply(lambda r: f"{r.get('APPLICATIONNAME') or ''}" + (f" — {r.get('ADD_INFO')}" if r.get("ADD_INFO") else ""), axis=1)
        app_filter_enabled = bool(team_id_s_norm)
        if app_filter_enabled:
            app_labels = ["All"] + (apps["LABEL"].tolist() if not apps.empty else [])
            if len(app_labels) <= 1:
                app_labels = ["All"]
        else:
            app_labels = ["Select Team first"]
        sel_app_label = st.selectbox(
            "Instance (site/location)",
            options=app_labels,
            index=0,
            key="search_app",
            disabled=not app_filter_enabled,
            help="Select Program and Team first to load instances.",
        )
        app_id_s = None
        if app_filter_enabled and sel_app_label != "All" and not apps.empty:
            app_id_s = apps.iloc[apps["LABEL"].tolist().index(sel_app_label)]["APPLICATIONID"]

    # Second row filters
    sc6, sc7, sc8, sc9, sc10, sc11 = st.columns([1.0, 1.2, 1.2, 1.4, 1.2, 1.2])

    with sc6:
        status_opt = st.selectbox("Status", ["All", "Planned", "Completed"], index=0, key="search_status")
        status_filter = None if status_opt == "All" else status_opt

    with sc7:
        itype_opt = st.selectbox("Invoice Type", ["All", "Recurring Invoice", "Ad Hoc Invoice"], index=0, key="search_itype")
        invoice_type_filter = None if itype_opt == "All" else itype_opt

    with sc8:
        agreement_like = st.text_input("Agreement # contains", value="", key="search_agreement_like")
        agreement_like = agreement_like.strip() or None

    with sc9:
        vends = _normalize_vendors_df(cached_list_vendors())
        vend_names = ["All"] + (vends["VENDORNAME"].tolist() if not vends.empty else [])
        sel_vend_name = st.selectbox("Vendor", options=vend_names, index=0, key="search_vendor")
        vendor_id_s = None
        if sel_vend_name != "All" and not vends.empty:
            vendor_id_s = vends.loc[vends["VENDORNAME"] == sel_vend_name, "VENDORID"].iloc[0]

    with sc10:
        order_status = st.selectbox("Order by Status", ["None", "Planned first", "Completed first"], index=0, key="search_order_status")
        order_status = None if order_status == "None" else order_status

    with sc11:
        order_date = st.selectbox("Order by Date", ["None", "Newest first", "Oldest first"], index=1, key="search_order_date")
        order_date = None if order_date == "None" else order_date

    st.markdown("<hr style='margin:8px 0;'/>", unsafe_allow_html=True)

    lcol_s, rcol_s = st.columns([0.85, 0.15])
    with lcol_s:
        st.markdown(
            '<div style="display:flex;gap:16px;align-items:center;white-space:nowrap;">'
            '<div style="opacity:0.8;">Columns: Status, Application, Amount, Renewal, Action</div>'
            '</div>',
            unsafe_allow_html=True
        )
    with rcol_s:
        only_active_s = st.checkbox("Only Active", value=False, key="search_only_active")

    # Reset search editor when filters change
    search_sig = repr([fy_filter, prog_id_s, team_id_s, group_id_s, app_id_s, status_filter, invoice_type_filter,
                       agreement_like, vendor_id_s, order_status, order_date, only_active_s])
    if search_sig != st.session_state.get("search_filter_sig"):
        st.session_state["search_filter_sig"] = search_sig
        st.session_state["search_open_editor"] = None
        st.session_state["search_editor_expanded"] = False

    res = cached_search_invoices(
        fiscal_year=fy_filter,
        program_id=prog_id_s,
        team_id=team_id_s,
        group_id=group_id_s,
        application_id=app_id_s,
        status_filter=status_filter,
        invoice_type_filter=invoice_type_filter,
        agreement_like=agreement_like,
        vendor_id=vendor_id_s,
        order_status=order_status,
        order_date=order_date,
    )

    if only_active_s and not res.empty and "Contract Active" in res.columns:
        res = res[res["Contract Active"] == True].copy()

    # Reset selection/editor when filters change
    search_sig = repr([fy_filter, prog_id_s, team_id_s, group_id_s, app_id_s, status_filter, invoice_type_filter, agreement_like, vendor_id_s, order_status, order_date, only_active_s])
    if search_sig != st.session_state.get("search_filter_sig"):
        st.session_state["search_filter_sig"] = search_sig
        st.session_state.pop("search_selected_invoice_id_results", None)
        st.session_state.pop("search_edit_open_results", None)
        st.session_state.pop("search_attach_open_results", None)
        st.session_state.pop("search_note_open_results", None)

    render_invoice_table_adaptive(
        res,
        section_key="results",
        title="Results",
        editable=True,
        context="search",
        show_tracking_status=False,
        show_responsible_column=False,
    )

# =======================================
# TAB — Tracking
# =======================================
if active_invoice_section == "Tracking":
    _render_op_feedback("tracking")
    _render_op_feedback("invoices_active")
    st.subheader("Tracking – Recurring invoices that need attention")

    email_alert_cfg_full = cached_email_alert_config()
    email_alert_cfg = {k: v for k, v in email_alert_cfg_full.items() if not str(k).startswith("_")}
    alert_meta = email_alert_cfg_full.get("_meta") or {}

    def _safe_int(val: Any, default: int) -> int:
        try:
            return int(val)
        except Exception:
            try:
                return int(float(val))
            except Exception:
                return default

    pending_default = _safe_int(email_alert_cfg.get("pending_window_days"), 30)
    contract_default = _safe_int(email_alert_cfg.get("contract_window_months"), 9)

    pending_default = min(max(pending_default, 7), 120)
    contract_default = min(max(contract_default, 1), 24)

    # Default program/team from user assignment (PRODUCTOWNER)
    user_prog_default, user_team_default = _user_default_program_team()
    defaults_applied = st.session_state.get("track_defaults_applied", False)

    if alert_meta.get("updated_at") or alert_meta.get("updated_by"):
        meta_parts = []
        if alert_meta.get("updated_at"):
            try:
                meta_ts = pd.to_datetime(alert_meta.get("updated_at"))
                meta_parts.append(f"updated {meta_ts.strftime('%Y-%m-%d %H:%M')}" )
            except Exception:
                meta_parts.append(f"updated {alert_meta.get('updated_at')}")
        if alert_meta.get("updated_by"):
            meta_parts.append(f"by {alert_meta.get('updated_by')}")
        if meta_parts:
            st.caption("Email alert defaults " + " ".join(meta_parts))

    # Filters (locked to Recurring Invoice)
    tc1, tc2, tc3, tc4, tc5 = st.columns([1.0, 1.2, 1.4, 1.7, 2.0])

    with tc1:
        years = ["All"] + [str(y) for y in _safe_years(2020, 2036)]
        default_year = str(date.today().year)
        fy_default_idx = years.index(default_year) if default_year in years else 0
        fy_val_t = st.selectbox("Year", options=years, index=fy_default_idx, key="track_fy")
        fy_filter_t = None if fy_val_t == "All" else int(fy_val_t)

    with tc2:
        progs = _normalize_programs_df(cached_list_programs())
        # Keep Tracking selectors consistent with Create/Search fallback behavior:
        # if program names are sparse, derive program options from TEAM->PROGRAM join.
        if progs.empty:
            try:
                p_from_teams = fetch_df(
                    """
                    SELECT DISTINCT
                        p.PROGRAMID,
                        COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME,
                        p.PROGRAMNAME AS PROGRAMNAME_RAW
                    FROM TEAMS t
                    JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                    WHERE LTRIM(RTRIM(COALESCE(NULLIF(p.PROGRAM_DISPLAY_NAME, ''), p.PROGRAMNAME, ''))) <> ''
                    ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME)
                    """,
                    None,
                )
                p_from_teams = _with_upper_aliases(p_from_teams, ["PROGRAMID", "PROGRAMNAME", "PROGRAMNAME_RAW"])
                progs = _normalize_programs_df(p_from_teams)
            except Exception:
                pass
        if progs.empty:
            try:
                t_for_prog = _normalize_teams_df(cached_list_teams())
                pids = (
                    t_for_prog.get("PROGRAMID", pd.Series(dtype=str))
                    .dropna()
                    .astype(str)
                    .str.strip()
                )
                pids = pids[pids != ""].drop_duplicates().sort_values()
                if len(pids) > 0:
                    progs = _normalize_programs_df(
                        pd.DataFrame({"PROGRAMID": pids.values, "PROGRAMNAME": [""] * len(pids.values)})
                    )
            except Exception:
                pass
        prog_names = ["All"] + (progs["PROGRAMNAME"].tolist() if not progs.empty else [])
        # Apply user defaults:
        # - first visit for this session
        # - or when current value is still "All" and a scoped default exists
        cur_prog = st.session_state.get("track_prog")
        if (
            (not defaults_applied)
            or (cur_prog in (None, "All") and user_prog_default in prog_names)
        ):
            desired_prog = user_prog_default if user_prog_default in prog_names else "All"
            st.session_state["track_prog"] = desired_prog
        elif cur_prog is not None and cur_prog not in prog_names:
            st.session_state["track_prog"] = "All"
        sel_prog_name_t = st.selectbox("Program", options=prog_names, index=0, key="track_prog")
        prog_id_t = None
        if sel_prog_name_t != "All" and not progs.empty:
            prog_id_t = progs.loc[progs["PROGRAMNAME"] == sel_prog_name_t, "PROGRAMID"].iloc[0]

    with tc3:
        team_filter_enabled_t = bool(prog_id_t)
        teams = _normalize_teams_df(cached_list_teams()) if team_filter_enabled_t else pd.DataFrame()
        if team_filter_enabled_t and prog_id_t:
            teams = teams.loc[teams["PROGRAMID"].map(_canon_id) == _canon_id(prog_id_t)]
            # Fallback by program name (display/raw) for schema/data variants where PROGRAMID matching fails.
            if teams.empty and sel_prog_name_t != "All":
                try:
                    teams = _normalize_teams_df(
                        fetch_df(
                            """
                            SELECT
                                t.TEAMID,
                                COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
                                t.TEAMNAME AS TEAMNAME_RAW,
                                t.PROGRAMID
                            FROM TEAMS t
                            JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                            WHERE UPPER(LTRIM(RTRIM(COALESCE(NULLIF(p.PROGRAM_DISPLAY_NAME, ''), p.PROGRAMNAME)))) = UPPER(LTRIM(RTRIM(%s)))
                               OR UPPER(LTRIM(RTRIM(p.PROGRAMNAME))) = UPPER(LTRIM(RTRIM(%s)))
                            ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME)
                            """,
                            (sel_prog_name_t, sel_prog_name_t),
                        )
                    )
                except Exception:
                    pass
        if team_filter_enabled_t:
            team_names = ["All"] + (teams["TEAMNAME"].tolist() if not teams.empty else [])
        else:
            team_names = ["Select Program first"]
        cur_team = st.session_state.get("track_team")
        if (
            (not defaults_applied)
            or (cur_team in (None, "All") and user_team_default in team_names)
        ):
            desired_team = user_team_default if user_team_default in team_names else "All"
            st.session_state["track_team"] = desired_team
        elif cur_team is not None and cur_team not in team_names:
            st.session_state["track_team"] = "All"
        sel_team_name_t = st.selectbox(
            "Team",
            options=team_names,
            index=0,
            key="track_team",
            disabled=not team_filter_enabled_t,
            help="Select Program first to load Teams.",
        )
        team_id_t = None
        if team_filter_enabled_t and sel_team_name_t != "All" and not teams.empty:
            team_id_t = teams.loc[teams["TEAMNAME"] == sel_team_name_t, "TEAMID"].iloc[0]
    if not defaults_applied:
        st.session_state["track_defaults_applied"] = True

    with tc4:
        team_id_t_norm = str(team_id_t or "").strip()
        groups = _normalize_groups_df(cached_list_groups_for_team(team_id_t_norm) if team_id_t_norm else cached_list_application_groups())
        if groups.empty and team_id_t_norm:
            try:
                groups = _normalize_groups_df(
                    fetch_df(
                        "SELECT GROUPID, GROUPNAME FROM APPLICATION_GROUPS WHERE TEAMID=%s ORDER BY GROUPNAME",
                        (team_id_t_norm,),
                    )
                )
            except Exception:
                pass
        group_filter_enabled_t = bool(team_id_t_norm)
        if group_filter_enabled_t:
            group_names = ["All"] + (groups["GROUPNAME"].tolist() if not groups.empty else [])
            if len(group_names) <= 1:
                group_names = ["All"]
        else:
            group_names = ["Select Team first"]
        cur_group = st.session_state.get("track_group")
        if cur_group is not None and cur_group not in group_names:
            st.session_state["track_group"] = "All"
        sel_group_name_t = st.selectbox(
            "Application",
            options=group_names,
            index=0,
            key="track_group",
            disabled=not group_filter_enabled_t,
            help="Select Program and Team first to load Applications.",
        )
        group_id_t = None
        if group_filter_enabled_t and sel_group_name_t != "All" and not groups.empty:
            group_id_t = groups.loc[groups["GROUPNAME"] == sel_group_name_t, "GROUPID"].iloc[0]

    with tc5:
        group_id_t_norm = str(group_id_t or "").strip()
        team_id_t_norm = str(team_id_t or "").strip()
        if group_id_t_norm:
            apps = _normalize_apps_df(cached_list_applications(group_id=group_id_t_norm))
        elif team_id_t_norm:
            apps = _normalize_apps_df(cached_list_applications(team_id=team_id_t_norm))
        else:
            apps = _normalize_apps_df(cached_list_applications())
        if apps.empty and group_id_t_norm:
            try:
                apps = _normalize_apps_df(
                    fetch_df(
                        """
                        SELECT APPLICATIONID, APPLICATIONNAME, ADD_INFO, GROUPID
                        FROM APPLICATIONS
                        WHERE GROUPID=%s
                        ORDER BY APPLICATIONNAME
                        """,
                        (group_id_t_norm,),
                    )
                )
            except Exception:
                pass
        elif apps.empty and team_id_t_norm:
            try:
                apps = _normalize_apps_df(
                    fetch_df(
                        """
                        SELECT a.APPLICATIONID, a.APPLICATIONNAME, a.ADD_INFO, a.GROUPID
                        FROM APPLICATIONS a
                        JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
                        WHERE g.TEAMID=%s
                        ORDER BY a.APPLICATIONNAME
                        """,
                        (team_id_t_norm,),
                    )
                )
            except Exception:
                pass
        apps = apps.copy()
        if not apps.empty:
            apps["LABEL"] = apps.apply(lambda r: f"{r.get('APPLICATIONNAME') or ''}" + (f" — {r.get('ADD_INFO')}" if r.get("ADD_INFO") else ""), axis=1)
        app_filter_enabled_t = bool(team_id_t_norm)
        if app_filter_enabled_t:
            app_labels = ["All"] + (apps["LABEL"].tolist() if not apps.empty else [])
            if len(app_labels) <= 1:
                app_labels = ["All"]
        else:
            app_labels = ["Select Team first"]
        cur_app = st.session_state.get("track_app")
        if cur_app is not None and cur_app not in app_labels:
            st.session_state["track_app"] = "All"
        sel_app_label_t = st.selectbox(
            "Instance (site/location)",
            options=app_labels,
            index=0,
            key="track_app",
            disabled=not app_filter_enabled_t,
            help="Select Program and Team first to load instances.",
        )
        app_id_t = None
        if app_filter_enabled_t and sel_app_label_t != "All" and not apps.empty:
            app_id_t = apps.iloc[apps["LABEL"].tolist().index(sel_app_label_t)]["APPLICATIONID"]

    st.markdown("<hr style='margin:8px 0;'/>", unsafe_allow_html=True)

    # Use configured defaults (cleaner UX: hide operational knobs from main tracking view).
    due_soon_days = int(pending_default)
    contract_months = int(contract_default)
    include_forecast = True
    only_active = True
    st.markdown(
        f"""
        <div style="display:flex;gap:14px;align-items:center;white-space:nowrap;overflow-x:auto;">
          <div><b>Status:</b></div>
          <div>{_badge_html("Critical")} Past due & <i>Planned</i></div>
          <div>{_badge_html("Pending")} Due ≤ {due_soon_days}d & <i>Planned</i></div>
          <div>{_badge_html("OK")} Completed</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    current_sig = repr([fy_filter_t, prog_id_t, team_id_t, group_id_t, app_id_t, due_soon_days, contract_months, only_active, include_forecast])
    if current_sig != st.session_state.get("track_filter_sig"):
        st.session_state["track_filter_sig"] = current_sig
        st.session_state["track_open_editor"] = None
        st.session_state["track_editor_expanded"] = False
        for sk in ["crit", "pend", "contract", "ok"]:
            st.session_state.pop(f"track_selected_invoice_id_{sk}", None)
            st.session_state.pop(f"track_edit_open_{sk}", None)
            st.session_state.pop(f"track_attach_open_{sk}", None)
            st.session_state.pop(f"track_note_open_{sk}", None)
            st.session_state.pop(f"track_page_{sk}", None)
            st.session_state.pop(f"track_page_size_{sk}", None)

    res_t = cached_search_invoices(
        fiscal_year=fy_filter_t,
        program_id=prog_id_t,
        team_id=team_id_t,
        group_id=group_id_t,
        application_id=app_id_t,
        status_filter=None,
        invoice_type_filter="Recurring Invoice",
        agreement_like=None,
        vendor_id=None,
        order_status=None,
        order_date=None,
        include_forecast=include_forecast,
    )

    if res_t.empty:
        st.info("No recurring invoices found for the current filters.")
    else:
        if only_active and "Contract Active" in res_t.columns:
            res_t = res_t[res_t["Contract Active"] == True].copy()

        track_df = compute_invoice_alerts(res_t, due_soon_days, contract_months)
        alert_filter = st.session_state.pop("invoice_alert_filter", None)
        missing_date_filter = st.session_state.pop("invoice_missing_date_filter", None)
        if alert_filter == "alerts":
            track_df = track_df[track_df["Tracking Status"].isin(["Critical", "Pending", "Contract"])].copy()
        if missing_date_filter:
            planned = track_df["Status"].fillna("").astype(str).str.strip().eq("Planned")
            missing = track_df["Renewal Date"].isna()
            track_df = track_df[planned & missing].copy()

        # Buckets
        crit_df  = track_df[track_df["Tracking Status"] == "Critical"].copy()
        pend_df  = track_df[track_df["Tracking Status"] == "Pending"].copy()
        ok_df    = track_df[track_df["Tracking Status"] == "OK"].copy()

        # KPIs (USD)
        def _sum_usd(d: pd.DataFrame) -> float:
            if d.empty:
                return 0.0
            return float(d["Amount (USD)"].fillna(0).sum())

        st.markdown("<div style='height:14px;'></div>", unsafe_allow_html=True)
        k1, k2, k3 = st.columns(3)
        with k1:
            _kpi_card("Critical (count)", f"{len(crit_df)}", "#E74C3C")
            _kpi_card("Critical (USD)", f"{_sum_usd(crit_df):,.2f}", "#E74C3C")
        with k2:
            _kpi_card("Pending (count)", f"{len(pend_df)}", "#F1C40F")
            _kpi_card("Pending (USD)", f"{_sum_usd(pend_df):,.2f}", "#F1C40F")
        with k3:
            _kpi_card("OK (count)", f"{len(ok_df)}", "#2ECC71")
            _kpi_card("OK (USD)", f"{_sum_usd(ok_df):,.2f}", "#2ECC71")

        sched_enabled = bool(email_alert_cfg.get("schedule_enabled"))
        sched_day = int(email_alert_cfg.get("schedule_day") or 1)
        sched_hour = int(email_alert_cfg.get("schedule_hour_utc") or 13)
        sched_hour = max(0, min(23, sched_hour))
        if sched_enabled:
            st.info(
                f"Automatic alerts are scheduled for day {sched_day} of each month at {sched_hour:02d}:00 UTC. "
                "You can adjust this in Edit → Email Alerts."
            )

        st.markdown("---")
        render_invoice_table_adaptive(
            crit_df,
            section_key="crit",
            title="Critical (Past due & Planned)",
            editable=True,
            context="track",
            show_tracking_status=True,
        )
        st.markdown("---")
        render_invoice_table_adaptive(
            pend_df,
            section_key="pend",
            title=f"Pending (Due ≤ {due_soon_days} days & Planned)",
            editable=True,
            context="track",
            show_tracking_status=True,
        )
        st.markdown("---")
        render_invoice_table_adaptive(
            ok_df,
            section_key="ok",
            title="OK",
            editable=False,
            context="track",
            show_tracking_status=True,
        )

    # Data quality diagnostics using canonical cost API (unassigned invoice costs)
    with st.expander("Data quality – unassigned invoice costs", expanded=False):
        filters: Dict[str, Any] = {}
        if fy_filter_t is not None:
            filters["year"] = int(fy_filter_t)
        if sel_prog_name_t != "All":
            filters["program"] = sel_prog_name_t
        if sel_team_name_t != "All":
            filters["team"] = sel_team_name_t
        if sel_group_name_t != "All":
            filters["app_group"] = sel_group_name_t

        df_unassigned = get_unassigned_breakdown(fetch_df, scenario="Projected", filters=filters or None)
        if df_unassigned is None or df_unassigned.empty:
            st.caption("No unassigned invoice-related costs found for the current filters.")
        else:
            df_inv = df_unassigned.copy()
            if "SOURCE" in df_inv.columns:
                df_inv = df_inv[df_inv["SOURCE"].fillna("").astype(str).str.upper() == "INVOICE"].copy()
            else:
                df_inv = pd.DataFrame()

            if df_inv.empty:
                st.caption("No unassigned costs from invoices for the current filters.")
            else:
                df_inv["TOTAL_COST"] = pd.to_numeric(df_inv.get("TOTAL_COST"), errors="coerce").fillna(0.0)
                df_inv["RECORDS"] = pd.to_numeric(df_inv.get("RECORDS"), errors="coerce").fillna(0).astype(int)

                total_unassigned = float(df_inv["TOTAL_COST"].sum())
                st.markdown(f"**Total unassigned invoice cost:** ${total_unassigned:,.2f}")

                grouping_cols = [c for c in ["ISSUE", "COST_CATEGORY", "SUBCOMPONENT", "YEAR", "PI"] if c in df_inv.columns]
                if grouping_cols:
                    summary = (
                        df_inv.groupby(grouping_cols, dropna=False, as_index=False)[["RECORDS", "TOTAL_COST"]]
                        .sum()
                        .sort_values("TOTAL_COST", ascending=False)
                    )
                    st.dataframe(summary, use_container_width=True, hide_index=True)
                else:
                    st.dataframe(df_inv, use_container_width=True, hide_index=True)

                st.caption(
                    "These are invoice-related costs that are missing some mapping (app group, team/program, PI/year). "
                    "Fixing the underlying master data mappings reduces the “Unassigned” buckets shown across the app."
                )
