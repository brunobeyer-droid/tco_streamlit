# pages/Applications.py
from __future__ import annotations

import uuid
from typing import Optional, List, Tuple, Any
from datetime import date

import pandas as pd
import streamlit as st
from core.init import init_page

from core.debug import _is_admin, is_debug_enabled
from core.ado_recon import load_explorer_feature_rows
from core.ado_candidates import get_unmapped_ado_apps_for_team
from core.ado_mapping_service import (
    get_unmapped_ado_apps,
    list_ado_app_mappings,
    list_ado_program_mappings,
    list_ado_team_mappings,
    map_ado_app_to_app_group,
)
from core.cache_utils import cache_data_portfolio, cache_resource_portfolio
from core.freshness import post_write_refresh
from core.authorization import (
    can,
    filter_application_groups_by_scope,
    filter_df_by_scope,
    get_current_principal,
    get_effective_scope,
)
from core.app_instances import ensure_default_instances
from db import (
    ensure_tables,
    fetch_df,
    execute,
    list_applications,
    list_teams,
    upsert_vendor,
    upsert_application_group,
    upsert_application_instance,
)
from utils.admin_delete_guard import (
    get_app_group_dependencies,
    get_app_instance_dependencies,
    render_dependency_summary,
    can_delete,
    safe_delete_app_group,
    safe_delete_app_instance,
)
from utils.ado_mapping_ui import render_ado_mapping_panel, render_embedded_onboard_flow
from utils.ui_patterns import render_scope_banner, render_section_picker, render_page_frame

from utils.app_shell import bootstrap_page
bootstrap_page()

page_theme = init_page("Applications", page_path=__file__)
_page_theme = page_theme
user = st.session_state.get("auth_user") or {}
is_admin = _is_admin()
show_debug = bool(is_debug_enabled(label="Debug"))
principal = get_current_principal()
effective_scope = get_effective_scope(fetch_df, principal)


can_manage_ado_app_mapping = bool(
    can(principal, "map", "application", scope=effective_scope)
)
can_manage_application_edit = bool(
    can(principal, "edit", "application", scope=effective_scope)
)

# --- Back-compat helpers (non-UI DB ops) ---
def delete_application_group(group_id: str) -> dict:
    return safe_delete_app_group(group_id, source="pages/6_Applications.py")

def delete_application(application_id: str) -> dict:
    return safe_delete_app_instance(application_id, source="pages/6_Applications.py")

# =========================================================
# One-time setup per portfolio context
# =========================================================
@cache_resource_portfolio(show_spinner=False)
def _ensure_applications_page_schema() -> None:
    ensure_tables()

    # Ensure many-to-many links: Teams <-> Applications (idempotent)
    execute("""
    CREATE TABLE IF NOT EXISTS APP_GROUP_TEAM_LINKS (
      GROUPID    STRING NOT NULL,
      TEAMID     STRING NOT NULL,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      CONSTRAINT PK_APP_GROUP_TEAM_LINKS PRIMARY KEY (GROUPID, TEAMID)
    )
    """)


_ensure_applications_page_schema()

# Version tokens to invalidate specific cached reads
st.session_state.setdefault("ver_programs_teams", 0)
st.session_state.setdefault("ver_groups_listing", 0)
st.session_state.setdefault("ver_apps_listing", 0)
st.session_state.setdefault("apps_op_feedback", None)

def bump_version(which: str):
    st.session_state[which] += 1


def _set_op_feedback(level: str, message: str) -> None:
    st.session_state["apps_op_feedback"] = {"level": str(level or "info"), "message": str(message or "")}


def _render_op_feedback() -> None:
    payload = st.session_state.pop("apps_op_feedback", None)
    if not isinstance(payload, dict):
        return
    lvl = str(payload.get("level") or "info").lower()
    msg = str(payload.get("message") or "").strip()
    if not msg:
        return
    if lvl == "success":
        st.success(msg)
    elif lvl == "warning":
        st.warning(msg)
    elif lvl == "error":
        st.error(msg)
    else:
        st.info(msg)


def _run_post_save_refresh(
    context: str,
    feedback_message: Optional[str] = None,
    feedback_level: str = "success",
    *,
    bump_version: Optional[bool] = True,
) -> None:
    if feedback_message:
        _set_op_feedback(feedback_level, feedback_message)
    post_write_refresh(context, ensure_views=True, rerun=True, bump_version=bump_version)

ver_pt = st.session_state["ver_programs_teams"]
ver_groups = st.session_state["ver_groups_listing"]
ver_apps = st.session_state["ver_apps_listing"]

render_page_frame(
    title="Applications",
    purpose="Maintain mappings, rates, and reference data",
    status_line="Master data only: mappings and instance ownership.",
)
_render_op_feedback()


# KPI row intentionally removed per UX guidance.

# -----------------------------
# Helpers
# -----------------------------
def _df_or_empty(df: Any) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame):
        return df
    if df is None:
        return pd.DataFrame()
    try:
        return pd.DataFrame(df)
    except Exception:
        return pd.DataFrame()


def _render_base_explainer(*, compact: bool = False) -> None:
    msg = "Use for bugs, break-ins, and unplanned work; demand/cost is shared across this team's mapped apps for the PI."
    if compact:
        st.caption(msg)
    else:
        st.info(f"Shared Unplanned Pool (BASE): {msg}")


def _render_onboard_how_it_works() -> None:
    with st.container(border=True):
        st.markdown("**How onboarding works**")
        st.markdown(
            "\n".join(
                [
                    "1. Select one or more ADO apps from the list.",
                    "2. Create one Application group.",
                    "3. Each selected ADO app is linked and becomes one instance.",
                    "4. The first selected ADO app becomes the primary/default instance name.",
                ]
            )
        )

@cache_data_portfolio(ttl=180, show_spinner=False)
def _vendors_df(_ver: int):
    try:
        df = fetch_df("SELECT VENDORID, VENDORNAME FROM VENDORS ORDER BY VENDORNAME")
    except Exception:
        try:
            df = fetch_df("SELECT * FROM VENDORS", None)
        except Exception:
            df = pd.DataFrame()
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _normalize_vendors_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    base = df if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    out = base.copy()
    if out.empty:
        return out
    # Case-insensitive column normalization first.
    col_map_upper = {str(c).strip().upper(): c for c in out.columns}
    if "VENDORID" not in out.columns and "VENDORID" in col_map_upper:
        out = out.rename(columns={col_map_upper["VENDORID"]: "VENDORID"})
    if "VENDORNAME" not in out.columns and "VENDORNAME" in col_map_upper:
        out = out.rename(columns={col_map_upper["VENDORNAME"]: "VENDORNAME"})
    if "VENDORID" not in out.columns:
        for alt in ["VENDOR_ID", "vendor_id", "ID", "id", "vendorid", "VendorID"]:
            if alt in out.columns:
                out = out.rename(columns={alt: "VENDORID"})
                break
    if "VENDORNAME" not in out.columns:
        for alt in ["VENDOR_NAME", "vendor_name", "NAME", "name", "vendorname", "VendorName"]:
            if alt in out.columns:
                out = out.rename(columns={alt: "VENDORNAME"})
                break
    return out


def _vendors_for_ui(_ver: int) -> pd.DataFrame:
    # 1) Cached vendor list
    out = _normalize_vendors_df(_vendors_df(_ver))
    # 2) Direct query fallback (uncached) to avoid stale/shape issues in widgets.
    if out.empty or "VENDORNAME" not in out.columns or "VENDORID" not in out.columns:
        try:
            direct = fetch_df("SELECT VENDORID, VENDORNAME FROM VENDORS ORDER BY VENDORNAME", None)
            out = _normalize_vendors_df(direct)
        except Exception:
            pass
    # 3) Last fallback from current app-group joins if vendor table shape is unusual.
    if out.empty or "VENDORNAME" not in out.columns:
        try:
            fallback = fetch_df(
                """
                SELECT DISTINCT v.VENDORID, v.VENDORNAME
                FROM APPLICATION_GROUPS g
                JOIN VENDORS v ON v.VENDORID = g.DEFAULT_VENDORID
                WHERE v.VENDORNAME IS NOT NULL AND LTRIM(RTRIM(v.VENDORNAME)) <> ''
                ORDER BY v.VENDORNAME
                """,
                None,
            )
            out = _normalize_vendors_df(fallback)
        except Exception:
            pass
    if not out.empty and "VENDORNAME" in out.columns:
        out["VENDORNAME"] = out["VENDORNAME"].astype(str).str.strip()
    if not out.empty and "VENDORID" in out.columns:
        out["VENDORID"] = out["VENDORID"].astype(str).str.strip()
    return out


def _normalize_instances_df(df: Any) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    if out.empty:
        return out
    col_upper = {str(c).strip().upper(): c for c in out.columns}
    for canon in ["APPLICATIONID", "APPLICATIONNAME", "GROUPID", "GROUPNAME", "TEAMID", "TEAMNAME", "PROGRAMID", "PROGRAMNAME", "VENDORID", "ADD_INFO", "IS_DEFAULT"]:
        if canon not in out.columns and canon in col_upper:
            out = out.rename(columns={col_upper[canon]: canon})
    for src, dst in [
        ("APPID", "APPLICATIONID"),
        ("APP_ID", "APPLICATIONID"),
        ("APPLICATION_ID", "APPLICATIONID"),
        ("ID", "APPLICATIONID"),
        ("APPNAME", "APPLICATIONNAME"),
        ("APP_NAME", "APPLICATIONNAME"),
        ("APPLICATION_NAME", "APPLICATIONNAME"),
        ("NAME", "APPLICATIONNAME"),
        ("GROUP_ID", "GROUPID"),
        ("APP_GROUP", "GROUPID"),
        ("APP_GROUP_ID", "GROUPID"),
        ("GROUP_NAME", "GROUPNAME"),
        ("TEAM_ID", "TEAMID"),
        ("TEAM_NAME", "TEAMNAME"),
        ("PROGRAM_ID", "PROGRAMID"),
        ("PROGRAM_NAME", "PROGRAMNAME"),
        ("VENDOR_ID", "VENDORID"),
        ("SITE", "ADD_INFO"),
        ("ADDINFO", "ADD_INFO"),
        ("ISDEFAULT", "IS_DEFAULT"),
    ]:
        if dst not in out.columns and src in out.columns:
            out = out.rename(columns={src: dst})
    return out

@cache_data_portfolio(ttl=180, show_spinner=False)
def _programs_df(_ver: int):
    return _df_or_empty(fetch_df(
        """
        SELECT
            PROGRAMID,
            PROGRAMNAME AS PROGRAMNAME_RAW,
            COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME) AS PROGRAMNAME
        FROM PROGRAMS
        ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME)
        """
    ))

@cache_data_portfolio(ttl=180, show_spinner=False)
def _teams_all(_ver: int):
    return _df_or_empty(list_teams())

@cache_data_portfolio(ttl=180, show_spinner=False)
def _teams_for_program(program_id: Optional[str], _ver: int):
    if not program_id:
        return list_teams()
    try:
        q = """
        SELECT
            TEAMID,
            TEAMNAME AS TEAMNAME_RAW,
            COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME) AS TEAMNAME,
            PROGRAMID
        FROM TEAMS
        WHERE PROGRAMID = %s
        ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME)
        """
        df = fetch_df(q, (program_id,))
        if df is not None and not df.empty:
            return _df_or_empty(df)
    except Exception:
        pass
    return _df_or_empty(list_teams())

@cache_data_portfolio(ttl=180, show_spinner=False)
def _ado_years_cached() -> list[int]:
    try:
        df = fetch_df(
            "SELECT DISTINCT TRY_CONVERT(INT, ADO_YEAR) AS ADO_YEAR FROM ADO_FEATURES WHERE ADO_YEAR IS NOT NULL",
            None,
        )
        if df is None or df.empty:
            return []
        years = pd.to_numeric(df["ADO_YEAR"], errors="coerce").dropna().astype(int).tolist()
        return sorted(set(years))
    except Exception:
        return []

@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_ado_features_candidates_cached(years: tuple[int, ...]) -> pd.DataFrame:
    if not years:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(years))
    sql = f"""
        SELECT
          FEATURE_ID,
          PROGRAM_RAW,
          TEAM_VARIANT_KEY AS ADO_TEAM_KEY,
          TEAM_RAW,
          APP_NAME_RAW,
          TRY_CONVERT(FLOAT, STORY_POINTS) AS STORY_POINTS,
          TRY_CONVERT(DATETIME2, COALESCE(CHANGED_AT, CREATED_AT)) AS LAST_SEEN
        FROM ADO_FEATURES
        WHERE TRY_CONVERT(INT, ADO_YEAR) IN ({placeholders})
    """
    df = fetch_df(sql, tuple(years))
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_explorer_feature_rows_cached(years: tuple[int, ...], programs: tuple[str, ...], teams: tuple[str, ...]) -> pd.DataFrame:
    try:
        df = load_explorer_feature_rows(years=years, programs=programs, teams=teams)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

# cached wrapper that calls the non-cached DB helper
@cache_data_portfolio(ttl=180, show_spinner=False)
def _groups_for_team_cached(team_id: Optional[str], _ver: int) -> pd.DataFrame:
    if not team_id:
        return pd.DataFrame()
    return _groups_for_team(team_id)

@cache_data_portfolio(ttl=120, show_spinner=False)
def _groups_listing(_ver_groups_only: int) -> pd.DataFrame:
    """Canonical listing of groups with vendor coming from DEFAULT_VENDORID."""
    return fetch_df(
        """
        SELECT
          g.GROUPID,
          g.GROUPNAME,
          g.TEAMID,
          COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
          t.PROGRAMID,
          COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME,
          g.DEFAULT_VENDORID AS VENDORID,
          v.VENDORNAME,
          TRY_CONVERT(BIT, JSON_VALUE((SELECT g.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), '$.IS_BASE')) AS IS_BASE,
          g.CREATED_AT,
          g.UPDATED_AT,
          g.UPDATED_BY
        FROM APPLICATION_GROUPS g
        LEFT JOIN TEAMS t    ON t.TEAMID = g.TEAMID
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        LEFT JOIN VENDORS v  ON v.VENDORID = g.DEFAULT_VENDORID
        ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME),
                 COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME),
                 g.GROUPNAME
        """
    )

@cache_data_portfolio(ttl=120, show_spinner=False)
def _apps_listing(team_id: Optional[str], _ver_apps_only: int):
    if not team_id:
        return fetch_df(
            """
            SELECT a.APPLICATIONID, a.APPLICATIONNAME, a.ADD_INFO, a.GROUPID, a.VENDORID,
                   g.GROUPNAME, g.TEAMID,
                   COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
                   t.PROGRAMID,
                   COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME
            FROM APPLICATIONS a
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
            LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        """
        )
    return list_applications(team_id=team_id)


def _instances_listing_uncached() -> pd.DataFrame:
    has_default = False
    try:
        has_default = _table_has_column("APPLICATIONS", "IS_DEFAULT")
    except Exception:
        has_default = False
    is_default_sql = "COALESCE(a.IS_DEFAULT, 0) AS IS_DEFAULT" if has_default else "0 AS IS_DEFAULT"
    try:
        df = fetch_df(
            f"""
            SELECT
              a.APPLICATIONID,
              a.APPLICATIONNAME,
              a.ADD_INFO,
              a.GROUPID,
              a.VENDORID,
              {is_default_sql},
              g.GROUPNAME,
              g.TEAMID,
              COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
              t.PROGRAMID,
              COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME
            FROM APPLICATIONS a
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
            LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
            """,
            None,
        )
    except Exception:
        # Schema-tolerant fallback for legacy APPID/APPNAME style tables.
        try:
            df = fetch_df(
                """
                SELECT
                  a.*,
                  g.GROUPNAME,
                  g.TEAMID,
                  COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
                  t.PROGRAMID,
                  COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME
                FROM APPLICATIONS a
                LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = COALESCE(a.GROUPID, a.APP_GROUP)
                LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
                LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                """,
                None,
            )
        except Exception:
            df = pd.DataFrame()
    return _normalize_instances_df(df)


@cache_data_portfolio(ttl=120, show_spinner=False)
def _instances_listing(_ver_apps_only: int) -> pd.DataFrame:
    return _instances_listing_uncached()


@cache_data_portfolio(ttl=120, show_spinner=False)
def _instance_link_counts() -> pd.DataFrame:
    try:
        inv = fetch_df("SELECT APPLICATIONID, COUNT(*) AS INVOICE_COUNT FROM INVOICES GROUP BY APPLICATIONID", None)
    except Exception:
        inv = pd.DataFrame(columns=["APPLICATIONID", "INVOICE_COUNT"])
    try:
        con = fetch_df("SELECT APPLICATIONID, COUNT(*) AS CONTRACT_COUNT FROM CONTRACTS GROUP BY APPLICATIONID", None)
    except Exception:
        con = pd.DataFrame(columns=["APPLICATIONID", "CONTRACT_COUNT"])
    inv = inv if inv is not None else pd.DataFrame(columns=["APPLICATIONID", "INVOICE_COUNT"])
    con = con if con is not None else pd.DataFrame(columns=["APPLICATIONID", "CONTRACT_COUNT"])
    out = pd.merge(inv, con, how="outer", on="APPLICATIONID")
    if out is None:
        out = pd.DataFrame(columns=["APPLICATIONID", "INVOICE_COUNT", "CONTRACT_COUNT"])
    out["INVOICE_COUNT"] = pd.to_numeric(out.get("INVOICE_COUNT"), errors="coerce").fillna(0).astype(int)
    out["CONTRACT_COUNT"] = pd.to_numeric(out.get("CONTRACT_COUNT"), errors="coerce").fillna(0).astype(int)
    return out

@cache_data_portfolio(ttl=120, show_spinner=False)
def _invoice_contract_rows() -> pd.DataFrame:
    try:
        inv = fetch_df("SELECT INVOICEID AS DOC_ID, APPLICATIONID, AMOUNT AS AMOUNT_USD FROM INVOICES", None)
    except Exception:
        inv = pd.DataFrame(columns=["DOC_ID", "APPLICATIONID", "AMOUNT_USD"])
    try:
        con = fetch_df("SELECT CONTRACT_ID AS DOC_ID, APPLICATIONID, TOTAL_CONTRACT_COST AS AMOUNT_USD FROM CONTRACTS", None)
    except Exception:
        con = pd.DataFrame(columns=["DOC_ID", "APPLICATIONID", "AMOUNT_USD"])

    inv = inv if isinstance(inv, pd.DataFrame) else pd.DataFrame(columns=["DOC_ID", "APPLICATIONID", "AMOUNT_USD"])
    con = con if isinstance(con, pd.DataFrame) else pd.DataFrame(columns=["DOC_ID", "APPLICATIONID", "AMOUNT_USD"])
    if not inv.empty:
        inv = inv.copy()
        inv["DOC_TYPE"] = "Invoice"
    if not con.empty:
        con = con.copy()
        con["DOC_TYPE"] = "Contract"
    docs = pd.concat([inv, con], ignore_index=True) if not inv.empty or not con.empty else pd.DataFrame()
    return docs if isinstance(docs, pd.DataFrame) else pd.DataFrame()

@cache_data_portfolio(ttl=120, show_spinner=False)
def _all_group_names_upper(_ver: int) -> set[str]:
    df = fetch_df("SELECT GROUPNAME FROM APPLICATION_GROUPS")
    if df is None or df.empty or "GROUPNAME" not in df.columns:
        return set()
    return set(df["GROUPNAME"].dropna().astype(str).str.strip().str.upper().tolist())

@cache_data_portfolio(ttl=120, show_spinner=False)
def _all_application_names_upper(_ver: int) -> set[str]:
    df = fetch_df("SELECT APPLICATIONNAME FROM APPLICATIONS")
    if df is None or df.empty or "APPLICATIONNAME" not in df.columns:
        return set()
    return set(df["APPLICATIONNAME"].dropna().astype(str).str.strip().str.upper().tolist())

def _teams_in_program(program_id: Optional[str]) -> pd.DataFrame:
    if not program_id:
        return pd.DataFrame()
    df = fetch_df(
        """
        SELECT
            TEAMID,
            TEAMNAME AS TEAMNAME_RAW,
            COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME) AS TEAMNAME
        FROM TEAMS
        WHERE PROGRAMID = %s
        ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME)
        """,
        (program_id,),
    )
    return _df_or_empty(df)

def _groups_for_team(team_id: str) -> pd.DataFrame:
    """Real DB helper (vendor from DEFAULT_VENDORID)."""
    try:
        return _df_or_empty(fetch_df(
            """
            SELECT
              g.GROUPID, g.GROUPNAME, g.TEAMID,
              COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
              t.PROGRAMID,
              COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME,
              g.DEFAULT_VENDORID AS VENDORID, v.VENDORNAME,
              TRY_CONVERT(BIT, JSON_VALUE((SELECT g.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), '$.IS_BASE')) AS IS_BASE
            FROM APPLICATION_GROUPS g
            LEFT JOIN TEAMS t    ON t.TEAMID = g.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
            LEFT JOIN VENDORS v  ON v.VENDORID = g.DEFAULT_VENDORID
            WHERE g.TEAMID = %s
            ORDER BY g.GROUPNAME
            """,
            (team_id,),
        ))
    except Exception:
        return pd.DataFrame()

def _groups_in_program(program_id: Optional[str]) -> pd.DataFrame:
    if not program_id:
        return pd.DataFrame()
    df = fetch_df(
        """
        SELECT g.GROUPID, g.GROUPNAME,
               t.TEAMID   AS OWNER_TEAMID,
               COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS OWNER_TEAMNAME
        FROM APPLICATION_GROUPS g
        JOIN TEAMS t ON t.TEAMID = g.TEAMID
        WHERE COALESCE(g.PROGRAMID, t.PROGRAMID) = %s
        ORDER BY g.GROUPNAME
        """,
        (program_id,),
    )
    return _df_or_empty(df)

def _groups_summary_for_program(program_id: Optional[str]) -> pd.DataFrame:
    if not program_id:
        return pd.DataFrame()
    df = fetch_df(
        """
        WITH base AS (
          SELECT g.GROUPID, g.GROUPNAME, t.TEAMID AS OWNER_TEAMID, t.TEAMNAME AS OWNER_TEAMNAME
          FROM APPLICATION_GROUPS g
          JOIN TEAMS t ON t.TEAMID = g.TEAMID
          WHERE COALESCE(g.PROGRAMID, t.PROGRAMID) = %s
        ),
        pairs AS (
          SELECT b.GROUPID, b.GROUPNAME, b.OWNER_TEAMNAME AS TEAMNAME
          FROM base b
          UNION
          SELECT g.GROUPID, g.GROUPNAME, t.TEAMNAME
          FROM APP_GROUP_TEAM_LINKS l
          JOIN APPLICATION_GROUPS g ON g.GROUPID = l.GROUPID
          JOIN TEAMS t ON t.TEAMID = l.TEAMID
          WHERE t.PROGRAMID = %s
            AND EXISTS (
              SELECT 1
              FROM APPLICATION_GROUPS gx JOIN TEAMS tx ON tx.TEAMID = gx.TEAMID
              WHERE gx.GROUPID = g.GROUPID
                AND COALESCE(gx.PROGRAMID, tx.PROGRAMID) = %s
            )
        )
        SELECT
          b.GROUPID,
          b.GROUPNAME,
          b.OWNER_TEAMNAME,
          STRING_AGG(p2.TEAMNAME, ', ') WITHIN GROUP (ORDER BY p2.TEAMNAME) AS ASSIGNED_TEAMS
        FROM base b
        LEFT JOIN (
          SELECT DISTINCT GROUPID, TEAMNAME FROM pairs
        ) p2 ON p2.GROUPID = b.GROUPID
        GROUP BY b.GROUPID, b.GROUPNAME, b.OWNER_TEAMNAME
        ORDER BY b.GROUPNAME
        """,
        (program_id, program_id, program_id),
    )
    return _df_or_empty(df)

def _linked_groups_for_team(program_id: Optional[str], team_id: Optional[str]) -> pd.DataFrame:
    if not program_id or not team_id:
        return pd.DataFrame()
    df = fetch_df(
        """
        SELECT g.GROUPID, g.GROUPNAME
        FROM APP_GROUP_TEAM_LINKS l
        JOIN APPLICATION_GROUPS g ON g.GROUPID = l.GROUPID
        JOIN TEAMS t ON t.TEAMID = l.TEAMID
        WHERE l.TEAMID = %s
          AND t.PROGRAMID = %s
          AND EXISTS (
            SELECT 1
            FROM APPLICATION_GROUPS gx JOIN TEAMS tx ON tx.TEAMID = gx.TEAMID
            WHERE gx.GROUPID = g.GROUPID
              AND COALESCE(gx.PROGRAMID, tx.PROGRAMID) = %s
          )
        ORDER BY g.GROUPNAME
        """,
        (team_id, program_id, program_id),
    )
    return _df_or_empty(df)

def _linked_teams_for_group(program_id: Optional[str], group_id: Optional[str]) -> pd.DataFrame:
    if not program_id or not group_id:
        return pd.DataFrame()
    df = fetch_df(
        """
        SELECT l.GROUPID, l.TEAMID, t.TEAMNAME
        FROM APP_GROUP_TEAM_LINKS l
        JOIN TEAMS t ON t.TEAMID = l.TEAMID
        WHERE l.GROUPID = %s
          AND t.PROGRAMID = %s
        ORDER BY t.TEAMNAME
        """,
        (group_id, program_id),
    )
    return _df_or_empty(df)

def _assignments_for_groups(group_ids: List[str]) -> pd.DataFrame:
    if not group_ids:
        return pd.DataFrame(columns=["GROUPNAME", "ASSIGNED_TEAMS"])
    placeholders = ", ".join(["%s"] * len(group_ids))
    df = fetch_df(
        f"""
        WITH pairs AS (
          SELECT g.GROUPID, g.GROUPNAME, t.TEAMNAME
          FROM APPLICATION_GROUPS g
          JOIN TEAMS t ON t.TEAMID = g.TEAMID
          WHERE g.GROUPID IN ({placeholders})
          UNION
          SELECT g.GROUPID, g.GROUPNAME, t.TEAMNAME
          FROM APP_GROUP_TEAM_LINKS l
          JOIN APPLICATION_GROUPS g ON g.GROUPID = l.GROUPID
          JOIN TEAMS t ON t.TEAMID = l.TEAMID
          WHERE g.GROUPID IN ({placeholders})
        )
        SELECT
          GROUPNAME,
          STRING_AGG(TEAMNAME, ', ') WITHIN GROUP (ORDER BY TEAMNAME) AS ASSIGNED_TEAMS
        FROM (
          SELECT DISTINCT GROUPNAME, TEAMNAME FROM pairs
        ) d
        GROUP BY GROUPNAME
        ORDER BY GROUPNAME
        """,
        tuple(group_ids) + tuple(group_ids),
    )
    return _df_or_empty(df)

def _safe_first_value(df, col) -> Optional[str]:
    if df is None or df.empty or col not in df.columns:
        return None
    try:
        return df[col].iloc[0]
    except Exception:
        return None

def _lookup_id_by_name(df, name_col: str, id_col: str, name_value: str) -> Optional[str]:
    if df is None or df.empty or name_col not in df.columns or id_col not in df.columns:
        return None
    rows = df[df[name_col] == name_value]
    if rows.empty:
        return None
    try:
        return rows[id_col].iloc[0]
    except Exception:
        return None


def _resolve_team_id_by_name(team_name: Any, teams_df: pd.DataFrame) -> Optional[str]:
    team_name_s = str(team_name or "").strip()
    if not team_name_s:
        return None
    # 1) Try in-memory dataframe first (normalized comparison).
    if isinstance(teams_df, pd.DataFrame) and not teams_df.empty and "TEAMNAME" in teams_df.columns and "TEAMID" in teams_df.columns:
        norm = teams_df["TEAMNAME"].fillna("").astype(str).str.strip().str.upper()
        rows = teams_df.loc[norm == team_name_s.upper()]
        if not rows.empty:
            team_id = str(rows.iloc[0].get("TEAMID") or "").strip()
            if team_id:
                return team_id
    # 2) Fallback to direct DB lookup to avoid stale/shape issues in cached team lists.
    try:
        df = fetch_df(
            """
            SELECT TOP 1 TEAMID
            FROM TEAMS
            WHERE UPPER(LTRIM(RTRIM(TEAMNAME))) = UPPER(LTRIM(RTRIM(%s)))
               OR UPPER(LTRIM(RTRIM(COALESCE(NULLIF(TEAM_DISPLAY_NAME, ''), TEAMNAME)))) = UPPER(LTRIM(RTRIM(%s)))
            """,
            (team_name_s, team_name_s),
        )
        if isinstance(df, pd.DataFrame) and not df.empty:
            team_id = str(df.iloc[0].get("TEAMID") or "").strip()
            if team_id:
                return team_id
    except Exception:
        pass
    return None

def _select_with_placeholder(label: str, options: List[str], key: str, disabled: bool = False, index: int = 0) -> Optional[str]:
    opts = ["(select)"] + options
    choice = st.selectbox(label, options=opts, key=key, disabled=disabled, index=index if index < len(opts) else 0)
    if choice == "(select)":
        return None
    return choice

@cache_data_portfolio(ttl=120, show_spinner=False)
def _orphan_instances(_ver_apps_only: int) -> pd.DataFrame:
    # Applications with no group link at all
    df = fetch_df(
        """
        SELECT
          a.APPLICATIONID,
          a.APPLICATIONNAME,
          a.ADD_INFO,
          a.GROUPID
        FROM APPLICATIONS a
        WHERE a.GROUPID IS NULL OR TRIM(COALESCE(a.GROUPID,'')) = ''
        ORDER BY a.APPLICATIONNAME
        """
    )
    return _df_or_empty(df)

@cache_data_portfolio(ttl=180, show_spinner=False)
def _table_has_column(table_name: str, column_name: str) -> bool:
    df = _df_or_empty(fetch_df(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = UPPER(%s)
          AND COLUMN_NAME = UPPER(%s)
        LIMIT 1
        """,
        (table_name, column_name),
    ))
    return not df.empty

# --- MSP helpers (use TEAM_MSP_RATE like Teams.py) ---

def _ensure_team_msp_rate_table_exists():
    # Safe, idempotent: in case Applications runs before Teams page
    try:
        execute("""
            CREATE TABLE IF NOT EXISTS TEAM_MSP_RATE (
              TEAMID STRING NOT NULL,
              MSP_ENABLED BOOLEAN DEFAULT FALSE,
              MSP_SIZE STRING,
              MSP_RATE_PER_PI NUMBER(18,2),
              UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
              CONSTRAINT PK_TEAM_MSP_RATE PRIMARY KEY (TEAMID)
            )
        """)
    except Exception:
        pass

@cache_data_portfolio(ttl=180, show_spinner=False)
def _msp_teams_in_program(program_id: Optional[str]) -> pd.DataFrame:
    """
    MSP teams are TEAMS joined with TEAM_MSP_RATE where MSP_ENABLED is true.
    """
    if not program_id:
        return pd.DataFrame()

    _ensure_team_msp_rate_table_exists()  # no-op if already created
    df = fetch_df(
        """
        SELECT t.TEAMID, t.TEAMNAME
        FROM TEAMS t
        LEFT JOIN TEAM_MSP_RATE m ON m.TEAMID = t.TEAMID
        WHERE t.PROGRAMID = %s
          AND COALESCE(m.MSP_ENABLED, 0) = 1
        ORDER BY t.TEAMNAME
        """,
        (program_id,),
    )
    return df if (df is not None and not df.empty) else pd.DataFrame()



# =========================================================
# Shared data + scope
# =========================================================
groups_df = _df_or_empty(_groups_listing(ver_groups))
# Use uncached read for instances to avoid rare cache-shape drift across sections.
instances_df = _normalize_instances_df(_instances_listing_uncached())
if (
    not instances_df.empty
    and "APPLICATIONID" not in instances_df.columns
    and "APPLICATIONNAME" not in instances_df.columns
):
    # Last-ditch retry through cached path (kept for parity) in case of transient DB/read issues.
    instances_df = _normalize_instances_df(_instances_listing(ver_apps))
link_counts = _df_or_empty(_instance_link_counts())
if not instances_df.empty and not link_counts.empty and "APPLICATIONID" in instances_df.columns:
    instances_df = instances_df.merge(link_counts, how="left", on="APPLICATIONID")
if "INVOICE_COUNT" not in instances_df.columns:
    instances_df["INVOICE_COUNT"] = 0
if "CONTRACT_COUNT" not in instances_df.columns:
    instances_df["CONTRACT_COUNT"] = 0

if (
    not instances_df.empty
    and "GROUPID" in instances_df.columns
    and "APPLICATIONID" in instances_df.columns
):
    instance_counts = instances_df.groupby("GROUPID")["APPLICATIONID"].count().to_dict()
elif not instances_df.empty and "GROUPID" in instances_df.columns:
    # Schema-tolerant fallback: count rows per group when APPLICATIONID is absent.
    instance_counts = instances_df.groupby("GROUPID").size().to_dict()
else:
    instance_counts = {}

mapping_df = _df_or_empty(fetch_df("SELECT DISTINCT APP_GROUP FROM MAP_ADO_APP_TO_TCO_GROUP", None))
mapping_full_df = _df_or_empty(fetch_df("SELECT ADO_APP, APP_GROUP FROM MAP_ADO_APP_TO_TCO_GROUP", None))
mapped_groups = (
    set(mapping_df["APP_GROUP"].dropna().astype(str).tolist())
    if (not mapping_df.empty and "APP_GROUP" in mapping_df.columns)
    else set()
)
mapping_conflict_groups = set()
if not mapping_full_df.empty and "ADO_APP" in mapping_full_df.columns and "APP_GROUP" in mapping_full_df.columns:
    map_work = mapping_full_df.copy()
    map_work["ADO_APP"] = map_work["ADO_APP"].astype(str).str.strip().str.upper()
    map_work["APP_GROUP"] = map_work["APP_GROUP"].astype(str).str.strip()
    dup_apps = (
        map_work.groupby("ADO_APP")["APP_GROUP"]
        .nunique()
        .reset_index(name="N")
    )
    dup_apps = dup_apps[dup_apps["N"] > 1]
    if not dup_apps.empty:
        dup_keys = set(dup_apps["ADO_APP"].tolist())
        mapping_conflict_groups = set(
            map_work.loc[map_work["ADO_APP"].isin(dup_keys), "APP_GROUP"].tolist()
        )
msp_links_df = _df_or_empty(fetch_df("SELECT DISTINCT GROUPID FROM APP_GROUP_TEAM_LINKS", None))
msp_linked = (
    set(msp_links_df["GROUPID"].dropna().astype(str).tolist())
    if (not msp_links_df.empty and "GROUPID" in msp_links_df.columns)
    else set()
)

groups_df = groups_df.copy()
if not groups_df.empty:
    grp_col_upper = {str(c).strip().upper(): c for c in groups_df.columns}
    if "GROUPID" not in groups_df.columns and "GROUPID" in grp_col_upper:
        groups_df = groups_df.rename(columns={grp_col_upper["GROUPID"]: "GROUPID"})
    if "GROUPNAME" not in groups_df.columns and "GROUPNAME" in grp_col_upper:
        groups_df = groups_df.rename(columns={grp_col_upper["GROUPNAME"]: "GROUPNAME"})
    if "TEAMID" not in groups_df.columns and "TEAMID" in grp_col_upper:
        groups_df = groups_df.rename(columns={grp_col_upper["TEAMID"]: "TEAMID"})
    if "TEAMNAME" not in groups_df.columns and "TEAMNAME" in grp_col_upper:
        groups_df = groups_df.rename(columns={grp_col_upper["TEAMNAME"]: "TEAMNAME"})
    if "PROGRAMID" not in groups_df.columns and "PROGRAMID" in grp_col_upper:
        groups_df = groups_df.rename(columns={grp_col_upper["PROGRAMID"]: "PROGRAMID"})
    if "PROGRAMNAME" not in groups_df.columns and "PROGRAMNAME" in grp_col_upper:
        groups_df = groups_df.rename(columns={grp_col_upper["PROGRAMNAME"]: "PROGRAMNAME"})
    if "VENDORID" not in groups_df.columns and "VENDORID" in grp_col_upper:
        groups_df = groups_df.rename(columns={grp_col_upper["VENDORID"]: "VENDORID"})
    if "VENDORNAME" not in groups_df.columns and "VENDORNAME" in grp_col_upper:
        groups_df = groups_df.rename(columns={grp_col_upper["VENDORNAME"]: "VENDORNAME"})
    for src, dst in [
        ("GROUP_ID", "GROUPID"),
        ("GROUP_NAME", "GROUPNAME"),
        ("TEAM_ID", "TEAMID"),
        ("TEAM_NAME", "TEAMNAME"),
        ("PROGRAM_ID", "PROGRAMID"),
        ("PROGRAM_NAME", "PROGRAMNAME"),
        ("DEFAULT_VENDORID", "VENDORID"),
        ("VENDOR_NAME", "VENDORNAME"),
    ]:
        if dst not in groups_df.columns and src in groups_df.columns:
            groups_df = groups_df.rename(columns={src: dst})
    groups_df["INSTANCE_COUNT"] = groups_df["GROUPID"].map(instance_counts).fillna(0).astype(int)
    groups_df["HAS_MAPPING"] = groups_df["GROUPID"].astype(str).isin(mapped_groups)
    groups_df["HAS_MSP_LINK"] = groups_df["GROUPID"].astype(str).isin(msp_linked)

st.markdown("### Scope")
render_scope_banner(
    principal=principal,
    scope=effective_scope,
    entity_label="Programs, Teams, and Applications",
)
if "apps_filter_program" not in st.session_state:
    st.session_state["apps_filter_program"] = "Select…"
if "apps_filter_team" not in st.session_state:
    st.session_state["apps_filter_team"] = "Select…"
if "apps_search" not in st.session_state:
    st.session_state["apps_search"] = ""

programs_df = _df_or_empty(_programs_df(ver_pt))
teams_all_df = _df_or_empty(_teams_all(ver_pt))
groups_df_scoped = groups_df.copy()
programs_df_scoped = programs_df.copy()
teams_all_df_scoped = teams_all_df.copy()

if principal.is_contributor and not principal.is_admin:
    programs_df_scoped = filter_df_by_scope(
        programs_df_scoped,
        resource_type="program",
        id_col="PROGRAMID",
        principal=principal,
        scope=effective_scope,
    )
    teams_all_df_scoped = filter_df_by_scope(
        teams_all_df_scoped,
        resource_type="team",
        id_col="TEAMID",
        principal=principal,
        scope=effective_scope,
    )
    groups_df_scoped = filter_application_groups_by_scope(
        groups_df_scoped,
        principal=principal,
        scope=effective_scope,
        group_id_col="GROUPID",
        team_id_col="TEAMID",
        program_id_col="PROGRAMID",
    )
programs_df = programs_df_scoped
teams_all_df = teams_all_df_scoped
groups_df = groups_df_scoped

instances_df_scoped = instances_df.copy()
if principal.is_contributor and not principal.is_admin:
    if (
        not instances_df_scoped.empty
        and "GROUPID" in instances_df_scoped.columns
        and "GROUPID" in groups_df.columns
    ):
        allowed_group_ids = set(groups_df["GROUPID"].fillna("").astype(str).str.strip().tolist())
        instances_df_scoped = instances_df_scoped[
            instances_df_scoped["GROUPID"].fillna("").astype(str).str.strip().isin(allowed_group_ids)
        ].copy()
    else:
        instances_df_scoped = instances_df_scoped.iloc[0:0].copy()
instances_df = instances_df_scoped

if not teams_all_df.empty:
    # Normalize team columns defensively across schema/view variants.
    if "TEAMNAME" not in teams_all_df.columns:
        for alt in ["TEAM_NAME", "NAME"]:
            if alt in teams_all_df.columns:
                teams_all_df = teams_all_df.rename(columns={alt: "TEAMNAME"})
                break
    if "TEAMID" not in teams_all_df.columns:
        for alt in ["TEAM_ID", "ID"]:
            if alt in teams_all_df.columns:
                teams_all_df = teams_all_df.rename(columns={alt: "TEAMID"})
                break
    if "PROGRAMID" not in teams_all_df.columns:
        for alt in ["PROGRAM_ID"]:
            if alt in teams_all_df.columns:
                teams_all_df = teams_all_df.rename(columns={alt: "PROGRAMID"})
                break
program_opts = (
    sorted(programs_df["PROGRAMNAME"].dropna().astype(str).str.strip().unique().tolist())
    if not programs_df.empty and "PROGRAMNAME" in programs_df.columns
    else []
)
team_all_opts = (
    sorted(teams_all_df["TEAMNAME"].dropna().astype(str).str.strip().unique().tolist())
    if not teams_all_df.empty and "TEAMNAME" in teams_all_df.columns
    else []
)

def _on_program_change() -> None:
    prog_val = st.session_state.get("apps_filter_program")
    if prog_val in ("Select…", "", None):
        st.session_state["apps_filter_team"] = "Select…"
    else:
        st.session_state["apps_filter_team"] = "All"

def _on_team_change(programs_df: pd.DataFrame, teams_all_df: pd.DataFrame) -> None:
    team_val = st.session_state.get("apps_filter_team")
    if team_val in ("Select…", "All", "", None):
        return
    prog_val = st.session_state.get("apps_filter_program")
    if prog_val in ("Select…", "All", "", None):
        if teams_all_df is None or teams_all_df.empty or "TEAMNAME" not in teams_all_df.columns:
            return
        team_row = teams_all_df.loc[teams_all_df["TEAMNAME"].astype(str) == str(team_val)]
        if team_row.empty:
            return
        if "PROGRAMID" not in teams_all_df.columns:
            return
        team_program_id = str(team_row.iloc[0].get("PROGRAMID") or "").strip()
        if not team_program_id or programs_df is None or programs_df.empty or "PROGRAMID" not in programs_df.columns:
            return
        prog_row = programs_df.loc[programs_df["PROGRAMID"].astype(str) == team_program_id]
        if prog_row.empty:
            return
        team_program = str(prog_row.iloc[0].get("PROGRAMNAME") or "").strip()
        if team_program:
            st.session_state["apps_filter_program"] = team_program

scope_selected = (
    (st.session_state.get("apps_filter_program") not in ("Select…", "All", "", None))
    or (st.session_state.get("apps_filter_team") not in ("Select…", "All", "", None))
)
search_term = str(st.session_state.get("apps_search") or "").strip()
if not scope_selected and search_term:
    matches = groups_df[
        groups_df["GROUPNAME"].astype(str).str.contains(search_term, case=False, na=False)
    ]
    if len(matches) == 1:
        row = matches.iloc[0]
        program_name = str(row.get("PROGRAMNAME") or "").strip()
        team_name = str(row.get("TEAMNAME") or "").strip()
        if program_name:
            st.session_state["apps_filter_program"] = program_name
        if team_name:
            st.session_state["apps_filter_team"] = team_name

f1, f2, f3 = st.columns([1.2, 1.2, 1.6])
with f1:
    selected_program = st.selectbox(
        "Program",
        options=["Select…"] + program_opts,
        key="apps_filter_program",
        on_change=_on_program_change,
    )
with f2:
    if selected_program and selected_program != "Select…":
        program_id = _lookup_id_by_name(programs_df, "PROGRAMNAME", "PROGRAMID", selected_program) if not programs_df.empty else None
        teams_in_prog = _df_or_empty(_teams_for_program(program_id, ver_pt)) if program_id else pd.DataFrame()
        team_opts = (
            sorted(teams_in_prog["TEAMNAME"].dropna().astype(str).str.strip().unique().tolist())
            if not teams_in_prog.empty and "TEAMNAME" in teams_in_prog.columns
            else []
        )
        team_options = ["All"] + team_opts
        current_team = st.session_state.get("apps_filter_team", "All")
        if current_team not in team_options:
            st.session_state["apps_filter_team"] = "All"
        team_disabled = False
    else:
        team_options = ["Select…"]
        st.session_state["apps_filter_team"] = "Select…"
        team_disabled = True
    selected_team = st.selectbox(
        "Team",
        options=team_options,
        key="apps_filter_team",
        on_change=lambda: _on_team_change(programs_df, teams_all_df),
        disabled=team_disabled,
    )
with f3:
    search_term = st.text_input("Search by name", value=search_term, key="apps_search")

f4, f5, f6 = st.columns([1.2, 1.2, 1])
with f4:
    mapping_status = st.selectbox("ADO mapping", ["All", "Mapped", "Unmapped"], key="app_filter_mapping")
with f5:
    msp_status = st.selectbox("MSP assignment", ["All", "Assigned", "Unassigned"], key="app_filter_msp")
with f6:
    multi_instance_only = st.checkbox("Multi-instance only", value=False, key="app_filter_multi")

scope_selected = (
    (st.session_state.get("apps_filter_program") not in ("Select…", "All", "", None))
    or (st.session_state.get("apps_filter_team") not in ("Select…", "All", "", None))
)

apps_scope_df = groups_df.copy()
selected_program = st.session_state.get("apps_filter_program")
selected_team = st.session_state.get("apps_filter_team")
if selected_program and selected_program not in ("Select…", "All") and "PROGRAMNAME" in apps_scope_df.columns:
    apps_scope_df = apps_scope_df[apps_scope_df["PROGRAMNAME"] == selected_program]
if selected_team and selected_team not in ("Select…", "All") and "TEAMNAME" in apps_scope_df.columns:
    apps_scope_df = apps_scope_df[apps_scope_df["TEAMNAME"] == selected_team]

apps_view_df = apps_scope_df.copy() if scope_selected else pd.DataFrame()
if not apps_view_df.empty:
    if mapping_status == "Mapped":
        apps_view_df = apps_view_df[apps_view_df["HAS_MAPPING"]]
    elif mapping_status == "Unmapped":
        apps_view_df = apps_view_df[~apps_view_df["HAS_MAPPING"]]
    if msp_status == "Assigned":
        apps_view_df = apps_view_df[apps_view_df["HAS_MSP_LINK"]]
    elif msp_status == "Unassigned":
        apps_view_df = apps_view_df[~apps_view_df["HAS_MSP_LINK"]]
    if multi_instance_only:
        apps_view_df = apps_view_df[apps_view_df["INSTANCE_COUNT"] > 1]
    if search_term:
        apps_view_df = apps_view_df[apps_view_df["GROUPNAME"].astype(str).str.contains(search_term, case=False, na=False)]

selected_group_id = st.session_state.get("selected_app_group_id")
if not scope_selected or apps_view_df.empty:
    st.session_state["selected_app_group_id"] = None
elif selected_group_id and "GROUPID" in apps_view_df.columns:
    valid_ids = set(apps_view_df["GROUPID"].astype(str).tolist())
    if str(selected_group_id) not in valid_ids:
        st.session_state["selected_app_group_id"] = None

show_advanced_instances = False
if is_admin:
    show_advanced_instances = st.checkbox(
        "Show advanced instance tools",
        value=False,
        key="apps_show_advanced_instances",
    )
    app_sections = ["Applications", "App Assignment"]
    if show_advanced_instances:
        app_sections.append("Instances (sites/locations)")
else:
    app_sections = ["Applications", "App Assignment"]
active_app_section = render_section_picker(
    key="applications_active_section",
    options=app_sections,
    label="Section",
    default="Applications",
    label_visibility="collapsed",
)
if active_app_section not in app_sections:
    active_app_section = app_sections[0]

years_available = _ado_years_cached()
if years_available:
    years_to_check = tuple(sorted(years_available)[-3:])
else:
    years_to_check = (date.today().year,)
program_scope = (selected_program,) if selected_program and selected_program not in ("Select…", "All") else tuple()
team_scope = (selected_team,) if selected_team and selected_team not in ("Select…", "All") else tuple()

ado_features_df = pd.DataFrame()
if active_app_section == "Applications":
    ado_features_df = _load_explorer_feature_rows_cached(years_to_check, program_scope, team_scope)
    if ado_features_df is None or not isinstance(ado_features_df, pd.DataFrame):
        ado_features_df = pd.DataFrame()
    if not ado_features_df.empty and "APP_NAME_RAW" not in ado_features_df.columns:
        for col in ("ADO_APP", "APP_NAME"):
            if col in ado_features_df.columns:
                ado_features_df = ado_features_df.rename(columns={col: "APP_NAME_RAW"})
                break

invoices_view_df = pd.DataFrame()
if is_admin and active_app_section == "Instances (sites/locations)":
    docs_df = _df_or_empty(_invoice_contract_rows())
    if not docs_df.empty and not instances_df.empty:
        merge_cols = [
            c
            for c in ["APPLICATIONID", "GROUPID", "GROUPNAME", "TEAMNAME", "PROGRAMNAME", "APPLICATIONNAME"]
            if c in instances_df.columns
        ]
        if "APPLICATIONID" in merge_cols and "APPLICATIONID" in docs_df.columns:
            docs_df = docs_df.merge(
                instances_df[merge_cols],
                how="left",
                on="APPLICATIONID",
            )
    invoices_view_df = docs_df.copy()
    if not invoices_view_df.empty and selected_program and selected_program not in ("Select…", "All") and "PROGRAMNAME" in invoices_view_df.columns:
        invoices_view_df = invoices_view_df[invoices_view_df["PROGRAMNAME"] == selected_program]
    if not invoices_view_df.empty and selected_team and selected_team not in ("Select…", "All") and "TEAMNAME" in invoices_view_df.columns:
        invoices_view_df = invoices_view_df[invoices_view_df["TEAMNAME"] == selected_team]

# =========================================================
# TAB 1: Applications (Create left, Edit right; listing below)
# =========================================================
if active_app_section == "Applications":
    st.subheader("Applications")
    def _get_unmapped_ado_scope_candidates(include_global: bool) -> tuple[pd.DataFrame, Optional[str], int, int]:
        scope_program = st.session_state.get("apps_filter_program")
        scope_team = st.session_state.get("apps_filter_team")
        scope_has_program = scope_program not in ("Select…", "All", "", None)
        scope_has_team = scope_team not in ("Select…", "All", "", None)
        if not scope_has_program and not scope_has_team:
            return pd.DataFrame(), "Next: select a Program or Team to see unmapped ADO apps.", 0, 0
        ado_all_df = _load_ado_features_candidates_cached(years_to_check)
        if ado_all_df is None or not isinstance(ado_all_df, pd.DataFrame) or ado_all_df.empty:
            return pd.DataFrame(), "No ADO rows were loaded. Load ADO features first to discover candidates.", 0, 0
        ado_scope_df = ado_all_df.copy()

        if scope_has_team:
            team_id = str(_resolve_team_id_by_name(scope_team, teams_all_df) or "").strip()
            team_map_df = list_ado_team_mappings()
            ado_team_keys = []
            if team_map_df is not None and not team_map_df.empty and team_id:
                ado_team_keys = (
                    team_map_df.loc[team_map_df["TEAMID"].astype(str) == team_id, "ADO_TEAM_KEY"]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
            if ado_team_keys:
                team_keys = {k.strip().upper() for k in ado_team_keys if str(k).strip()}
                if "ADO_TEAM_KEY" in ado_scope_df.columns:
                    ado_scope_df = ado_scope_df[
                        ado_scope_df["ADO_TEAM_KEY"].astype(str).str.strip().str.upper().isin(team_keys)
                    ]
            else:
                # Fallback: team may be valid even without explicit ADO team-key mappings.
                ado_scope_df = ado_scope_df.iloc[0:0]
        elif scope_has_program:
            program_id = _lookup_id_by_name(programs_df, "PROGRAMNAME", "PROGRAMID", scope_program) if not programs_df.empty else None
            prog_map = list_ado_program_mappings()
            ado_programs = []
            if prog_map is not None and not prog_map.empty and program_id:
                ado_programs = (
                    prog_map.loc[prog_map["PROGRAMID"].astype(str) == str(program_id), "ADO_PROGRAM"]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
            if ado_programs:
                prog_keys = {p.strip().upper() for p in ado_programs if str(p).strip()}
                if "PROGRAM_RAW" in ado_scope_df.columns:
                    ado_scope_df = ado_scope_df[
                        ado_scope_df["PROGRAM_RAW"].astype(str).str.strip().str.upper().isin(prog_keys)
                    ]
            else:
                ado_scope_df = ado_scope_df.iloc[0:0]
        else:
            ado_scope_df = ado_scope_df.iloc[0:0]

        app_map_df = list_ado_app_mappings()
        scope_candidates = get_unmapped_ado_apps(ado_scope_df, app_map_df)
        global_candidates = get_unmapped_ado_apps(ado_all_df, app_map_df)

        # Team-level fallback: merge in candidates discovered by TEAMID logic so
        # partial/missing key mappings do not hide valid unmapped apps.
        if scope_has_team:
            team_id = str(_resolve_team_id_by_name(scope_team, teams_all_df) or "").strip()
            if team_id:
                fallback_df = get_unmapped_ado_apps_for_team(fetch_df, team_id=team_id, year=None, lookback_days=3650)
                if isinstance(fallback_df, pd.DataFrame) and not fallback_df.empty:
                    fb = fallback_df.copy()
                    if "ADO_APP_RAW" in fb.columns and "APP_NAME_RAW" not in fb.columns:
                        fb = fb.rename(columns={"ADO_APP_RAW": "APP_NAME_RAW"})
                    if "STORY_POINTS_SUM" not in fb.columns and "DERIVED_FTE_SUM" in fb.columns:
                        fb["STORY_POINTS_SUM"] = pd.to_numeric(
                            fb["DERIVED_FTE_SUM"], errors="coerce"
                        ).fillna(0.0)
                    if scope_candidates is None or scope_candidates.empty:
                        scope_candidates = fb
                    else:
                        base = scope_candidates.copy()
                        if "ADO_APP" in base.columns and "APP_NAME_RAW" not in base.columns:
                            base = base.rename(columns={"ADO_APP": "APP_NAME_RAW"})
                        base["_APP_KEY"] = base.get("APP_NAME_RAW", pd.Series(dtype=str)).astype(str).str.strip().str.upper()
                        fb["_APP_KEY"] = fb.get("APP_NAME_RAW", pd.Series(dtype=str)).astype(str).str.strip().str.upper()
                        missing_fb = fb[~fb["_APP_KEY"].isin(set(base["_APP_KEY"].tolist()))].copy()
                        base.drop(columns=["_APP_KEY"], inplace=True, errors="ignore")
                        missing_fb.drop(columns=["_APP_KEY"], inplace=True, errors="ignore")
                        scope_candidates = pd.concat([base, missing_fb], ignore_index=True)

        if scope_candidates is None:
            scope_candidates = pd.DataFrame()
        if global_candidates is None:
            global_candidates = pd.DataFrame()

        if "ADO_APP" in scope_candidates.columns:
            scope_candidates = scope_candidates.rename(columns={"ADO_APP": "APP_NAME_RAW"})
        if "ADO_APP" in global_candidates.columns:
            global_candidates = global_candidates.rename(columns={"ADO_APP": "APP_NAME_RAW"})

        if include_global:
            scope_keys = set(scope_candidates.get("APP_NAME_RAW", pd.Series(dtype=str)).astype(str).str.strip().str.upper().tolist())
            global_candidates = global_candidates.copy()
            app_name_series = (
                global_candidates["APP_NAME_RAW"]
                if "APP_NAME_RAW" in global_candidates.columns
                else pd.Series([""] * len(global_candidates), index=global_candidates.index)
            )
            global_candidates["IN_SCOPE"] = app_name_series.astype(str).str.strip().str.upper().isin(scope_keys)
            if "STORY_POINTS_SUM" in global_candidates.columns:
                global_candidates = global_candidates.sort_values(
                    ["IN_SCOPE", "STORY_POINTS_SUM"], ascending=[False, False], na_position="last"
                )
            scope_count = int(len(scope_candidates))
            global_count = int(len(global_candidates))
            if global_candidates.empty:
                return pd.DataFrame(), "No unmapped ADO apps detected in this scope.", scope_count, global_count
            return global_candidates, None, scope_count, global_count

        scope_count = int(len(scope_candidates))
        global_count = int(len(global_candidates))
        if scope_candidates.empty:
            empty_msg = "No unmapped ADO apps detected in this scope."
            if scope_has_program and not scope_has_team:
                empty_msg = "No ADO rows found for this program scope. Check Program-to-ADO mapping."
            elif scope_has_team:
                empty_msg = "No ADO rows found for this team scope. Check Team-to-ADO mapping."
            return pd.DataFrame(), empty_msg, scope_count, global_count
        return scope_candidates, None, scope_count, global_count

    if not scope_selected:
        st.markdown("### Step 1 — Choose your scope")
        st.info("Applications are managed within a Program or Team. Select a scope above to get started.")
    else:
        if st.session_state.get("apps_mode_next"):
            st.session_state["apps_mode"] = st.session_state.pop("apps_mode_next")
        if "apps_mode" not in st.session_state:
            st.session_state["apps_mode"] = "Discover"
        can_manage_apps_scope = bool(can_manage_application_edit or can_manage_ado_app_mapping)
        mode_options = ["Discover", "Onboard"] + (["Maintain"] if can_manage_apps_scope else [])
        if st.session_state.get("apps_mode") not in mode_options:
            st.session_state["apps_mode"] = "Discover"
        try:
            apps_mode = st.segmented_control(
                "Mode",
                mode_options,
                key="apps_mode",
                label_visibility="collapsed",
            )
        except Exception:
            apps_mode = st.radio(
                "Mode",
                mode_options,
                horizontal=True,
                key="apps_mode",
                label_visibility="collapsed",
            )
        if apps_mode != "Onboard":
            st.session_state.pop("apps_onboard_last_summary", None)
            st.session_state.pop("apps_onboard_last_summary_token", None)
            st.session_state.pop("apps_onboard_last_summary_rendered", None)
        if not can_manage_apps_scope:
            st.info("🔒 Maintain mode and instance management are available to Admin or scoped contributors.")

        if apps_mode == "Discover":
            st.markdown("### Applications (read-only)")
            with st.container(border=True):
                st.markdown("**Onboarding checklist**")
                candidates_df, candidates_msg, _, _ = _get_unmapped_ado_scope_candidates(False)
                app_not_onboarded_val = "—" if candidates_msg else str(int(len(candidates_df)))
                missing_owner_count = 0
                no_mapping_count = 0
                scope_team_selected = (
                    st.session_state.get("apps_filter_team") not in ("Select…", "All", "", None)
                )
                if apps_view_df is not None and not apps_view_df.empty:
                    team_series = (
                        apps_view_df["TEAMNAME"].fillna("").astype(str)
                        if "TEAMNAME" in apps_view_df.columns
                        else pd.Series([], dtype=str)
                    )
                    missing_owner_count = int(team_series.str.strip().eq("").sum())
                    no_mapping_count = int(
                        (~apps_view_df["GROUPID"].astype(str).isin(mapped_groups)).sum()
                        if "GROUPID" in apps_view_df.columns
                        else 0
                    )
                missing_owner_label = (
                    "N/A (team scope selected)"
                    if scope_team_selected
                    else str(missing_owner_count)
                )
                lines = [
                    f"- Applications not onboarded (ADO) in this scope: {app_not_onboarded_val}",
                    f"- Applications missing Accountable Team (in scope): {missing_owner_label}",
                    f"- Applications with no ADO mapping (in scope): {no_mapping_count}",
                ]
                st.markdown("\n".join(lines))
                if candidates_msg:
                    st.caption(candidates_msg)
            if apps_view_df.empty:
                st.info("No Applications in this scope for the current filters. Adjust Program/Team or search.")
            else:
                group_ids = apps_view_df["GROUPID"].dropna().astype(str).unique().tolist()
                supporting_map = {}
                if group_ids:
                    placeholders = ", ".join(["%s"] * len(group_ids))
                    link_df = _df_or_empty(
                        fetch_df(
                            f"""
                            SELECT l.GROUPID, t.TEAMNAME
                            FROM APP_GROUP_TEAM_LINKS l
                            JOIN TEAMS t ON t.TEAMID = l.TEAMID
                            WHERE l.GROUPID IN ({placeholders})
                            """,
                            tuple(group_ids),
                        )
                    )
                    if not link_df.empty:
                        for gid, sub in link_df.groupby("GROUPID", dropna=False):
                            supporting_map[str(gid)] = sorted(sub["TEAMNAME"].dropna().astype(str).tolist())

                discover_df = apps_view_df.copy()
                team_series = discover_df["TEAMNAME"] if "TEAMNAME" in discover_df.columns else pd.Series([""] * len(discover_df), index=discover_df.index)
                discover_df["Accountable Team"] = team_series.astype(str).str.strip()
                discover_df["Supporting Teams"] = discover_df["GROUPID"].astype(str).map(
                    lambda gid: ", ".join(supporting_map.get(str(gid), []))
                )
                discover_df["ADO Mapping"] = discover_df["HAS_MAPPING"].map(lambda v: "Yes" if bool(v) else "No")
                discover_df["Instances"] = discover_df.get("INSTANCE_COUNT", 0)
                show_cols = [
                    "GROUPNAME",
                    "PROGRAMNAME",
                    "Accountable Team",
                    "Supporting Teams",
                    "ADO Mapping",
                    "Instances",
                ]
                st.dataframe(
                    discover_df[show_cols].rename(columns={"GROUPNAME": "Application", "PROGRAMNAME": "Program"}),
                    use_container_width=True,
                    hide_index=True,
                    height=360,
                )

                label_df = apps_view_df.copy()
                label_df["APP_LABEL"] = label_df.apply(
                    lambda r: f"{r.get('PROGRAMNAME', '')} • {r.get('TEAMNAME', '')} • {r.get('GROUPNAME', '')}".strip(" •"),
                    axis=1,
                )
                dup_labels = label_df["APP_LABEL"].duplicated(keep=False)
                if dup_labels.any():
                    label_df.loc[dup_labels, "APP_LABEL"] = label_df.loc[dup_labels].apply(
                        lambda r: f"{r['APP_LABEL']} [{r['GROUPID']}]",
                        axis=1,
                    )
                app_labels = label_df["APP_LABEL"].astype(str).tolist()
                label_to_gid = dict(zip(label_df["APP_LABEL"].astype(str), label_df["GROUPID"].astype(str)))
                app_select = st.selectbox(
                    "Application summary",
                    options=["(Select)"] + app_labels,
                    index=0,
                    key="apps_discover_select",
                )
                if app_select and app_select != "(Select)":
                    selected_group_id = label_to_gid.get(app_select)
                    st.session_state["selected_app_group_id"] = selected_group_id
                    selected_row = groups_df.loc[
                        groups_df["GROUPID"].astype(str) == str(selected_group_id)
                    ] if selected_group_id else pd.DataFrame()
                    if selected_row is not None and not selected_row.empty:
                        link_count = len(supporting_map.get(str(selected_group_id), []))
                        app_group_id = str(_safe_first_value(selected_row, "GROUPID") or "").strip()
                        mapped_rows = (
                            mapping_df[mapping_df["APP_GROUP"].astype(str) == app_group_id]
                            if not mapping_df.empty and "APP_GROUP" in mapping_df.columns and app_group_id
                            else pd.DataFrame()
                        )
                        mapped_status = "Mapped" if not mapped_rows.empty else "Not mapped"
                        st.markdown("#### Application Summary")
                        st.markdown(
                            "\n".join(
                                [
                                    f"- **Application:** {_safe_first_value(selected_row, 'GROUPNAME')}",
                                    f"- **Program:** {_safe_first_value(selected_row, 'PROGRAMNAME')}",
                                    f"- **Accountable Team:** {_safe_first_value(selected_row, 'TEAMNAME')}",
                                    f"- **Supporting Teams:** {link_count}",
                                    f"- **ADO Mapping:** {mapped_status}",
                                    f"- **Instances:** {int(_safe_first_value(selected_row, 'INSTANCE_COUNT') or 0)}",
                                ]
                            )
                        )
                        def _switch_to_maintain() -> None:
                            st.session_state["apps_mode_next"] = "Maintain"

                        if st.button("Maintain this application", key="apps_go_maintain", on_click=_switch_to_maintain):
                            st.rerun()
                else:
                    st.caption("Select an application to view a summary.")

        elif apps_mode == "Onboard":
            st.markdown("### Onboard")
            st.caption(
                "These are ADO app names used by features in this scope that are not yet mapped to an Application."
            )
            _render_onboard_how_it_works()
            last_summary = st.session_state.get("apps_onboard_last_summary")
            summary_token = str(st.session_state.get("apps_onboard_last_summary_token") or "")
            rendered_token = str(st.session_state.get("apps_onboard_last_summary_rendered") or "")
            show_last_summary = isinstance(last_summary, dict) and bool(summary_token) and summary_token != rendered_token
            if show_last_summary:
                linked_count = int(last_summary.get("linked_count") or 0)
                instance_count = int(last_summary.get("instance_count") or 0)
                st.success(
                    "Onboarding completed: "
                    f"{linked_count} ADO app(s) linked, "
                    f"{instance_count} instance row(s) in group."
                )
                linked_vals = last_summary.get("linked_values") or []
                if linked_vals:
                    st.caption("Linked ADO apps: " + ", ".join([str(x) for x in linked_vals]))
                with st.expander("Post-create checklist", expanded=True):
                    st.markdown(
                        "\n".join(
                            [
                                f"- Application group created: **{str(last_summary.get('group_name') or 'N/A')}**",
                                f"- ADO apps linked: **{linked_count}**",
                                f"- Application instances created: **{instance_count}**",
                                f"- Instance count matches selected ADO apps: **{'Yes' if linked_count == instance_count else 'Check required'}**",
                            ]
                        )
                    )
                    if linked_count != instance_count:
                        st.warning(
                            "Linked ADO app count and instance count are different. "
                            "Open Maintain mode and validate the instance list."
                        )
                if st.button("Dismiss onboarding summary", key="apps_onboard_summary_dismiss"):
                    st.session_state.pop("apps_onboard_last_summary", None)
                    st.session_state.pop("apps_onboard_last_summary_token", None)
                    st.session_state.pop("apps_onboard_last_summary_rendered", None)
                    st.rerun()
                else:
                    st.session_state["apps_onboard_last_summary_rendered"] = summary_token
            include_global = st.checkbox(
                "Include global unmapped ADO apps (outside current scope)",
                value=False,
                key="apps_unmapped_include_global",
            )
            selected_ado_apps = st.session_state.get("apps_onboard_selected_ado_apps") or []
            candidates_df, candidates_msg, scope_rows, global_rows = _get_unmapped_ado_scope_candidates(include_global)
            st.markdown("#### Unmapped ADO Apps in this scope")
            if candidates_msg:
                st.caption(candidates_msg)
                st.session_state["apps_onboard_selected_ado_apps"] = []
            else:
                view_df = candidates_df.copy()
                view_df["__pick"] = False
                rename = {
                    "__pick": "Select",
                    "APP_NAME_RAW": "ADO App Name",
                    "FEATURE_COUNT": "Features",
                    "STORY_POINTS_SUM": "Story Points",
                    "LAST_SEEN": "Last Seen",
                }
                show_cols = ["__pick", "APP_NAME_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "LAST_SEEN"]
                if include_global and "IN_SCOPE" in view_df.columns:
                    rename["IN_SCOPE"] = "In scope?"
                    show_cols.append("IN_SCOPE")
                editor_view = view_df[show_cols].rename(columns=rename)
                edit = st.data_editor(
                    editor_view,
                    use_container_width=True,
                    hide_index=True,
                    height=260,
                    column_config={
                        "Select": st.column_config.CheckboxColumn("Select"),
                    },
                    disabled=[c for c in editor_view.columns if c != "Select"],
                    key="apps_unmapped_pick",
                )
                selected_rows = edit[edit["Select"]].index.tolist() if "Select" in edit.columns else []
                selected_ado_apps = []
                if selected_rows:
                    selected_ado_apps = [
                        str(edit.loc[idx, "ADO App Name"]).strip()
                        for idx in selected_rows
                        if str(edit.loc[idx, "ADO App Name"]).strip()
                    ]
                    selected_ado_apps = list(dict.fromkeys(selected_ado_apps))
                st.session_state["apps_onboard_selected_ado_apps"] = selected_ado_apps
            selected_ado_apps = st.session_state.get("apps_onboard_selected_ado_apps") or []
            selected_ado_app = selected_ado_apps[0] if selected_ado_apps else None
            if show_debug:
                with st.expander("Debug – unmapped ADO scope", expanded=False):
                    resolved_team_debug = _resolve_team_id_by_name(
                        st.session_state.get("apps_filter_team"),
                        teams_all_df,
                    )
                    st.write(
                        {
                            "scope_program": st.session_state.get("apps_filter_program"),
                            "scope_team": st.session_state.get("apps_filter_team"),
                            "resolved_team_id": resolved_team_debug,
                            "selected_ado_apps": selected_ado_apps,
                            "include_global": bool(include_global),
                            "ado_all_rows": int(len(_load_ado_features_candidates_cached(years_to_check))),
                            "ado_scope_rows": int(scope_rows),
                            "global_unmapped_rows": int(global_rows),
                            "ado_columns": list(_load_ado_features_candidates_cached(years_to_check).columns),
                        }
                    )

            if selected_ado_app:
                st.markdown("### Create Application from selected ADO app")
                programs = _df_or_empty(_programs_df(ver_pt))
                vendors = _vendors_for_ui(ver_pt)
                vendor_opts = (
                    vendors["VENDORNAME"].dropna().astype(str).tolist()
                    if (not vendors.empty and "VENDORNAME" in vendors.columns)
                    else []
                )
                team_df = _df_or_empty(_teams_for_program(_lookup_id_by_name(programs, "PROGRAMNAME", "PROGRAMID", selected_program), ver_pt))
                team_opts = team_df["TEAMNAME"].dropna().astype(str).tolist() if not team_df.empty else []
                team_default = selected_team if selected_team in team_opts else ""

                def _group_name_conflict(name: str) -> Optional[str]:
                    name_upper = name.strip().upper()
                    if name_upper in _all_group_names_upper(ver_groups):
                        return f"Application '{name.strip()}' already exists."
                    if name_upper in _all_application_names_upper(ver_apps):
                        return f"Application '{name.strip()}' conflicts with an existing instance name."
                    return None

                def _create_group_onboard(*, name: str, team_id: str, vendor_id: Optional[str], is_base: bool) -> Optional[str]:
                    new_group_id = str(uuid.uuid4())
                    upsert_application_group(
                        group_id=new_group_id,
                        group_name=name.strip(),
                        team_id=team_id,
                        default_vendor_id=vendor_id,
                        owner=None,
                        is_base=is_base,
                        updated_by=str(user.get("email") or ""),
                    )
                    # `upsert_application_group` already creates the single default instance when needed.
                    st.session_state["last_group_saved_id"] = new_group_id
                    st.session_state["apps_selected"] = name.strip()
                    st.session_state["selected_app_group_id"] = new_group_id
                    bump_version("ver_groups_listing")
                    bump_version("ver_apps_listing")
                    return new_group_id

                def _sync_instances_and_summary(new_gid: str, group_name: str, ado_values: List[str]) -> None:
                    names = [str(x or "").strip() for x in ado_values if str(x or "").strip()]
                    names = list(dict.fromkeys(names))
                    if not names:
                        return
                    try:
                        inst_existing = _df_or_empty(
                            fetch_df(
                                """
                                SELECT APPLICATIONID, APPLICATIONNAME
                                FROM APPLICATIONS
                                WHERE GROUPID = %s
                                ORDER BY APPLICATIONNAME
                                """,
                                (str(new_gid),),
                            )
                        )
                        existing_names = (
                            set(inst_existing["APPLICATIONNAME"].fillna("").astype(str).str.strip().str.upper().tolist())
                            if (not inst_existing.empty and "APPLICATIONNAME" in inst_existing.columns)
                            else set()
                        )
                        if (
                            not inst_existing.empty
                            and "APPLICATIONID" in inst_existing.columns
                            and "APPLICATIONNAME" in inst_existing.columns
                        ):
                            primary_id = str(inst_existing.iloc[0].get("APPLICATIONID") or "").strip()
                            current_name = str(inst_existing.iloc[0].get("APPLICATIONNAME") or "").strip()
                            first_name = names[0]
                            if (
                                primary_id
                                and first_name
                                and current_name.upper() != first_name.upper()
                                and first_name.upper() not in existing_names
                            ):
                                execute(
                                    "UPDATE APPLICATIONS SET APPLICATIONNAME=%s WHERE APPLICATIONID=%s",
                                    (first_name, primary_id),
                                )
                                existing_names.discard(current_name.upper())
                                existing_names.add(first_name.upper())
                        for nm in names[1:]:
                            if nm.upper() in existing_names:
                                continue
                            upsert_application_instance(
                                application_id=str(uuid.uuid4()),
                                group_id=str(new_gid),
                                application_name=nm,
                                add_info=None,
                                vendor_id=None,
                                updated_by=str(user.get("email") or ""),
                            )
                            existing_names.add(nm.upper())
                        count_df = _df_or_empty(
                            fetch_df("SELECT COUNT(*) AS N FROM APPLICATIONS WHERE GROUPID=%s", (str(new_gid),))
                        )
                        n_instances = int(count_df.iloc[0]["N"]) if not count_df.empty else len(names)
                    except Exception:
                        n_instances = len(names)
                    st.session_state["apps_onboard_last_summary"] = {
                        "group_id": new_gid,
                        "group_name": group_name,
                        "linked_count": len(names),
                        "linked_values": names,
                        "instance_count": n_instances,
                    }
                    st.session_state["apps_onboard_last_summary_token"] = str(uuid.uuid4())
                    st.session_state.pop("apps_onboard_last_summary_rendered", None)

                name_val = st.text_input(
                    "Application Group Name",
                    value=selected_ado_app,
                    key="apps_onboard_name",
                    help=(
                        "This name is shown across NEXT interfaces (Dashboards, Insights, Invoices, Contracts). "
                        "Renaming changes how users see this application everywhere."
                    ),
                )
                st.caption(f"Selected ADO apps for onboarding: **{len(selected_ado_apps)}**")
                st.code("\\n".join(selected_ado_apps[:20]), language="text")
                if len(selected_ado_apps) > 20:
                    st.caption(f"... and {len(selected_ado_apps) - 20} more")
                if selected_ado_apps:
                    st.info(
                        f"{len(selected_ado_apps)} ADO app(s) selected. "
                        f"NEXT will create **{len(selected_ado_apps)} instance(s)** under this application group "
                        "(1 selected app = 1 instance, 2 selected apps = 2 instances, etc.)."
                    )
                team_val = st.selectbox(
                    "Owner Team",
                    options=["(Select)"] + team_opts,
                    index=(team_opts.index(team_default) + 1) if team_default in team_opts else 0,
                    key="apps_onboard_team",
                )
                if st.session_state.get("apps_onboard_vendor_next"):
                    st.session_state["apps_onboard_vendor"] = st.session_state.pop("apps_onboard_vendor_next")
                is_base_val = st.checkbox(
                    "Mark as Shared Unplanned Pool (BASE) for this team",
                    value=False,
                    key="apps_onboard_base",
                )
                vcol1, vcol2 = st.columns([3, 1])
                with vcol1:
                    vendor_val = st.selectbox(
                        "Default Vendor",
                        options=["(Select)", "(None)"] + vendor_opts,
                        index=0,
                        key="apps_onboard_vendor",
                        disabled=bool(is_base_val),
                    )
                with vcol2:
                    if st.button("+ New vendor", key="apps_onboard_vendor_new"):
                        st.session_state["apps_onboard_show_new_vendor"] = True
                if st.session_state.get("apps_onboard_show_new_vendor"):
                    new_vendor_name = st.text_input("Vendor Name", value="", key="apps_onboard_vendor_name_new")
                    if st.button("Create vendor", icon=":material/add:", key="apps_onboard_vendor_create"):
                        save_status = st.status("Creating vendor...", expanded=False) if hasattr(st, "status") else None
                        name = str(new_vendor_name or "").strip()
                        if not name:
                            if save_status is not None:
                                save_status.update(label="Create failed.", state="error")
                            st.error("Vendor name is required.")
                        else:
                            try:
                                existing = fetch_df(
                                    "SELECT TOP 1 VENDORID, VENDORNAME FROM VENDORS WHERE UPPER(VENDORNAME)=UPPER(%s)",
                                    (name,),
                                )
                                if existing is not None and not existing.empty:
                                    existing_name = str(existing.iloc[0].get("VENDORNAME") or name).strip()
                                    st.session_state["apps_onboard_vendor_next"] = existing_name
                                    if save_status is not None:
                                        save_status.update(label="Vendor already exists. Using existing vendor.", state="complete")
                                    st.info(f"Vendor already exists; using '{existing_name}'.")
                                else:
                                    vid = str(uuid.uuid4())
                                    upsert_vendor(vid, name, updated_by=str(user.get("email") or ""))
                                    st.session_state["apps_onboard_vendor_next"] = name
                                    if save_status is not None:
                                        save_status.update(label="Vendor created.", state="complete")
                                    st.success("Vendor created.")
                                bump_version("ver_programs_teams")
                                st.session_state["apps_onboard_show_new_vendor"] = False
                                _run_post_save_refresh(
                                    "applications_onboard_vendor_create",
                                    feedback_message="Vendor prepared for onboarding.",
                                    feedback_level="success",
                                )
                            except Exception as e:
                                if save_status is not None:
                                    save_status.update(label="Create failed.", state="error")
                                st.error(f"Create vendor failed: {e}")
                if is_base_val:
                    _render_base_explainer(compact=True)

                effective_vendor = "(None)" if is_base_val else (vendor_val if vendor_val and vendor_val != "(Select)" else "(Select)")
                st.caption(
                    f"Effective config: Type = **{'Shared Unplanned Pool (BASE)' if is_base_val else 'Standard'}** | "
                    f"Vendor = **{effective_vendor}** | "
                    f"Allocation = **{'Team shared' if is_base_val else 'Direct app/vendor'}**"
                )
                with st.container(border=True):
                    st.markdown("**Ready to create and map**")
                    st.markdown(
                        "\n".join(
                            [
                                f"- Selected ADO apps: **{len(selected_ado_apps)}**",
                                f"- Target group name: **{name_val or '(required)'}**",
                                f"- Owner team: **{team_val or '(required)'}**",
                                f"- Vendor mode: **{'Shared Unplanned Pool (BASE)' if is_base_val else 'Standard'}**",
                                f"- Predicted instances: **{len(selected_ado_apps)}**",
                            ]
                        )
                    )
                    if not selected_ado_apps:
                        st.warning("Select at least one ADO app above to enable onboarding.")

                if st.button("Create & map selected ADO apps", icon=":material/add:", key="apps_onboard_create_map"):
                    save_status = st.status("Creating application and mapping ADO apps...", expanded=False) if hasattr(st, "status") else None
                    final_name = (name_val or "").strip()
                    if not final_name:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Application name is required.")
                        st.stop()
                    team_id = _lookup_id_by_name(team_df, "TEAMNAME", "TEAMID", team_val) if team_val and team_val != "(Select)" else None
                    if not team_id:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Owner Team is required.")
                        st.stop()
                    conflict = _group_name_conflict(final_name)
                    if conflict:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error(conflict)
                        st.stop()
                    vendor_id = (
                        _lookup_id_by_name(vendors, "VENDORNAME", "VENDORID", vendor_val)
                        if vendor_val and vendor_val not in ("(Select)", "(None)")
                        else None
                    )
                    if is_base_val:
                        vendor_id = None
                    if not is_base_val and not vendor_id:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Vendor is required unless the application is marked as Shared Unplanned Pool (BASE).")
                        st.stop()
                    new_gid = _create_group_onboard(
                        name=final_name,
                        team_id=team_id,
                        vendor_id=vendor_id,
                        is_base=bool(is_base_val),
                    )
                    if new_gid:
                        if save_status is not None:
                            save_status.update(label="Mapping selected ADO apps...", state="running")
                        map_ado_app_to_app_group(selected_ado_apps, str(new_gid))
                        if save_status is not None:
                            save_status.update(label="Creating/syncing instances...", state="running")
                        _sync_instances_and_summary(str(new_gid), final_name, selected_ado_apps)
                        st.session_state["apps_mode_next"] = "Maintain"
                        if save_status is not None:
                            save_status.update(label="Application created and mapped.", state="complete")
                        _run_post_save_refresh(
                            "application_create_map",
                            feedback_message=f"Application '{final_name}' created and mapped ({len(selected_ado_apps)} ADO app(s)).",
                        )
                else:
                    st.markdown("#### Create application manually")
                    with st.expander("Create application manually", expanded=False):
                        if selected_program and selected_program not in ("Select…", "All") and not st.session_state.get("onboard_group_program"):
                            st.session_state["onboard_group_program"] = selected_program
                        if selected_team and selected_team not in ("Select…", "All") and not st.session_state.get("onboard_group_team"):
                            st.session_state["onboard_group_team"] = selected_team
                        programs = programs_df.copy() if isinstance(programs_df, pd.DataFrame) else pd.DataFrame()
                        vendors = _vendors_for_ui(ver_pt)

                        def _group_name_conflict(name: str) -> Optional[str]:
                            name_upper = name.strip().upper()
                            if name_upper in _all_group_names_upper(ver_groups):
                                return f"Application '{name.strip()}' already exists."
                            if name_upper in _all_application_names_upper(ver_apps):
                                return f"Application '{name.strip()}' conflicts with an existing instance name."
                            return None

                        def _create_group_onboard(*, name: str, team_id: str, vendor_id: Optional[str], is_base: bool) -> Optional[str]:
                            new_group_id = str(uuid.uuid4())
                            upsert_application_group(
                                group_id=new_group_id,
                                group_name=name.strip(),
                                team_id=team_id,
                                default_vendor_id=vendor_id,
                                owner=None,
                                is_base=is_base,
                                updated_by=str(user.get("email") or ""),
                            )
                            # `upsert_application_group` already creates the single default instance when needed.
                            st.session_state["last_group_saved_id"] = new_group_id
                            st.session_state["apps_selected"] = name.strip()
                            st.session_state["selected_app_group_id"] = new_group_id
                            bump_version("ver_groups_listing")
                            bump_version("ver_apps_listing")
                            return new_group_id

                        def _sync_instances_from_ado(new_gid: str, _name: str, _linked_count: int, ado_values: Optional[List[str]] = None) -> None:
                            names = []
                            seen = set()
                            for raw in (ado_values or []):
                                nm = str(raw or "").strip()
                                if not nm:
                                    continue
                                key = nm.upper()
                                if key in seen:
                                    continue
                                seen.add(key)
                                names.append(nm)
                            if not names:
                                st.session_state["apps_selected"] = _name
                                st.session_state["apps_onboard_last_summary"] = {
                                    "group_id": new_gid,
                                    "group_name": _name,
                                    "linked_count": int(_linked_count or 0),
                                    "linked_values": [],
                                    "instance_count": 1,
                                }
                                st.session_state["apps_onboard_last_summary_token"] = str(uuid.uuid4())
                                st.session_state.pop("apps_onboard_last_summary_rendered", None)
                                return
                            try:
                                inst_existing = _df_or_empty(
                                    fetch_df(
                                        """
                                        SELECT APPLICATIONID, APPLICATIONNAME, COALESCE(IS_DEFAULT, 0) AS IS_DEFAULT
                                        FROM APPLICATIONS
                                        WHERE GROUPID = %s
                                        ORDER BY COALESCE(IS_DEFAULT, 0) DESC, APPLICATIONNAME
                                        """,
                                        (str(new_gid),),
                                    )
                                )
                                existing_names = (
                                    set(inst_existing["APPLICATIONNAME"].fillna("").astype(str).str.strip().str.upper().tolist())
                                    if (not inst_existing.empty and "APPLICATIONNAME" in inst_existing.columns)
                                    else set()
                                )
                                # Align the primary/default instance name with the first selected ADO app.
                                first_name = names[0]
                                if (
                                    not inst_existing.empty
                                    and "APPLICATIONID" in inst_existing.columns
                                    and "APPLICATIONNAME" in inst_existing.columns
                                ):
                                    primary_row = inst_existing.iloc[0]
                                    current_primary_name = str(primary_row.get("APPLICATIONNAME") or "").strip()
                                    primary_id = str(primary_row.get("APPLICATIONID") or "").strip()
                                    if (
                                        primary_id
                                        and first_name
                                        and current_primary_name.upper() != first_name.upper()
                                        and first_name.upper() not in existing_names
                                    ):
                                        execute(
                                            "UPDATE APPLICATIONS SET APPLICATIONNAME=%s WHERE APPLICATIONID=%s",
                                            (first_name, primary_id),
                                        )
                                        existing_names.discard(current_primary_name.upper())
                                        existing_names.add(first_name.upper())
                                # Create additional instances for remaining selected ADO apps.
                                for nm in names[1:]:
                                    if nm.upper() in existing_names:
                                        continue
                                    upsert_application_instance(
                                        application_id=str(uuid.uuid4()),
                                        group_id=str(new_gid),
                                        application_name=nm,
                                        add_info=None,
                                        vendor_id=None,  # resolve from group default vendor
                                        updated_by=str(user.get("email") or ""),
                                    )
                                    existing_names.add(nm.upper())
                            except Exception:
                                pass
                            st.session_state["apps_selected"] = _name
                            try:
                                count_df = _df_or_empty(
                                    fetch_df(
                                        "SELECT COUNT(*) AS N FROM APPLICATIONS WHERE GROUPID=%s",
                                        (str(new_gid),),
                                    )
                                )
                                instance_count = int(count_df.iloc[0]["N"]) if not count_df.empty else len(names)
                            except Exception:
                                instance_count = len(names) if names else 1
                            st.session_state["apps_onboard_last_summary"] = {
                                "group_id": new_gid,
                                "group_name": _name,
                                "linked_count": int(_linked_count or 0),
                                "linked_values": names,
                                "instance_count": int(instance_count),
                            }
                            st.session_state["apps_onboard_last_summary_token"] = str(uuid.uuid4())
                            st.session_state.pop("apps_onboard_last_summary_rendered", None)

                        render_embedded_onboard_flow(
                            "app_group",
                            {
                                "programs_df": programs,
                                "teams_for_program_fn": lambda pid: _teams_for_program(pid, ver_pt),
                                "groups_for_team_fn": lambda tid: _groups_for_team_cached(tid, ver_pt),
                                "vendors_df": vendors,
                                "name_exists_fn": _group_name_conflict,
                                "create_fn": _create_group_onboard,
                                "on_created": _sync_instances_from_ado,
                                "updated_by": str(user.get("email") or ""),
                            },
                            user_is_admin=can_manage_ado_app_mapping,
                            ado_features_df=ado_features_df,
                        )

        else:
            st.markdown("### Maintain")
            st.caption("Step-by-step maintenance for naming, ownership, and ADO mapping confidence.")
            st.markdown("#### Step 1 — Select Application (in your scope)")
            scoped_total = int(len(apps_view_df)) if isinstance(apps_view_df, pd.DataFrame) else 0
            scoped_mapped = int(apps_view_df["HAS_MAPPING"].sum()) if (scoped_total and "HAS_MAPPING" in apps_view_df.columns) else 0
            scoped_unmapped = max(scoped_total - scoped_mapped, 0)
            scoped_conflicts = (
                int(apps_view_df["GROUPID"].astype(str).isin(mapping_conflict_groups).sum())
                if (scoped_total and "GROUPID" in apps_view_df.columns)
                else 0
            )
            st.caption(
                f"In scope: **{scoped_total}** | Mapped: **{scoped_mapped}** | "
                f"Unmapped: **{scoped_unmapped}** | Mapping conflicts: **{scoped_conflicts}**"
            )
            if apps_view_df.empty:
                st.info("No Applications in this scope for the current filters. Adjust Program/Team or search.")
                selected_group_id = None
                selected_row = pd.DataFrame()
            else:
                list_df = apps_view_df.copy()
                list_df["__selected"] = list_df["GROUPID"].astype(str) == str(st.session_state.get("selected_app_group_id") or "")
                list_df["ADO Mapped"] = list_df["HAS_MAPPING"].map(lambda v: "Yes" if bool(v) else "No")
                list_df["MSP"] = list_df["HAS_MSP_LINK"].map(lambda v: "Yes" if bool(v) else "No")
                list_df = list_df.set_index("GROUPID")
                show_cols = [
                    c for c in ["__selected", "GROUPNAME", "PROGRAMNAME", "TEAMNAME", "INSTANCE_COUNT", "ADO Mapped", "MSP"]
                    if c in list_df.columns
                ]
                alias_map = {
                    "__selected": "Select",
                    "GROUPNAME": "Application",
                    "PROGRAMNAME": "Program",
                    "TEAMNAME": "Team",
                    "INSTANCE_COUNT": "Instances",
                }
                editable = {"Select": True}
                view_df = list_df[show_cols].rename(columns=alias_map)
                edit_df = st.data_editor(
                    view_df,
                    use_container_width=True,
                    hide_index=True,
                    height=360,
                    column_config={
                        "Select": st.column_config.CheckboxColumn("Select"),
                    },
                    disabled=[c for c in view_df.columns if c not in editable],
                    key="apps_list_select",
                )
                selected_ids = edit_df.index[edit_df["Select"]].tolist() if "Select" in edit_df.columns else []
                selected_group_id = str(selected_ids[-1]) if selected_ids else None
                st.session_state["selected_app_group_id"] = selected_group_id
                selected_row = groups_df.loc[groups_df["GROUPID"].astype(str) == str(selected_group_id)] if selected_group_id else pd.DataFrame()
                if selected_group_id is None:
                    st.caption("Select an application from the list to view details.")

            current_name = (_safe_first_value(selected_row, "GROUPNAME") or "").strip() if selected_row is not None and not selected_row.empty else ""

            prev_gid = st.session_state.get("apps_prev_selected_gid")
            if selected_group_id and str(selected_group_id) != str(prev_gid):
                st.session_state["apps_prev_selected_gid"] = str(selected_group_id)
                if current_name:
                    st.session_state["inst_filter_app"] = current_name
            if not selected_group_id:
                st.session_state["apps_prev_selected_gid"] = None
                st.session_state["inst_filter_app"] = "(All)"

            if show_debug:
                with st.expander("Debug – selection", expanded=False):
                    st.write(
                        {
                            "total_apps": int(groups_df["GROUPID"].nunique() if not groups_df.empty else 0),
                            "filtered_apps": int(apps_view_df["GROUPID"].nunique() if not apps_view_df.empty and "GROUPID" in apps_view_df.columns else 0),
                            "selected_app_group_id": selected_group_id if apps_view_df is not None else None,
                            "selected_in_filtered": bool(
                                selected_group_id
                                and not apps_view_df.empty
                                and "GROUPID" in apps_view_df.columns
                                and str(selected_group_id) in apps_view_df["GROUPID"].astype(str).tolist()
                            ),
                        }
                    )

            if selected_row is None or selected_row.empty:
                st.info("Select an application from the list to view details.")
            else:
                st.markdown("#### Step 2 — Ownership & Display Name")
                edit_gid = _safe_first_value(selected_row, "GROUPID")
                current_name = (_safe_first_value(selected_row, "GROUPNAME") or "").strip()
                current_vendor_name = _safe_first_value(selected_row, "VENDORNAME") if selected_row is not None else None
                current_vendor_id = _safe_first_value(selected_row, "VENDORID") if selected_row is not None else None
                current_is_base = bool(_safe_first_value(selected_row, "IS_BASE") or 0)
                selected_team_id = _safe_first_value(selected_row, "TEAMID")
                selected_program_label = _safe_first_value(selected_row, "PROGRAMNAME")

                vend_df = _vendors_for_ui(ver_pt)
                if not vend_df.empty:
                    if "VENDORID" in vend_df.columns:
                        vend_df["VENDORID"] = vend_df["VENDORID"].astype(str).str.strip()
                    if "VENDORNAME" in vend_df.columns:
                        vend_df["VENDORNAME"] = vend_df["VENDORNAME"].astype(str).str.strip()
                vend_options = (
                    sorted(vend_df["VENDORNAME"].dropna().astype(str).str.strip().unique().tolist())
                    if (not vend_df.empty and "VENDORNAME" in vend_df.columns)
                    else []
                )
                if not current_is_base and not vend_options:
                    st.warning(
                        "No vendors found in VENDORS. Create a vendor first (Vendors page) "
                        "or mark this app as Shared Unplanned Pool (BASE)."
                    )
                # If group default vendor is missing, try inferring from any instance vendor in this group.
                if (not current_vendor_id) and (not instances_df.empty) and "GROUPID" in instances_df.columns and "VENDORID" in instances_df.columns:
                    inst_vendor_rows = instances_df.loc[instances_df["GROUPID"].astype(str) == str(edit_gid)]
                    inst_vendor_rows = inst_vendor_rows[inst_vendor_rows["VENDORID"].notna()] if not inst_vendor_rows.empty else inst_vendor_rows
                    if not inst_vendor_rows.empty:
                        inferred_vendor_id = str(inst_vendor_rows.iloc[0].get("VENDORID") or "").strip()
                        if inferred_vendor_id:
                            current_vendor_id = inferred_vendor_id
                if (not current_vendor_name) and current_vendor_id and (not vend_df.empty) and "VENDORID" in vend_df.columns and "VENDORNAME" in vend_df.columns:
                    vrow = vend_df.loc[
                        vend_df["VENDORID"].astype(str).str.strip().str.upper()
                        == str(current_vendor_id).strip().upper()
                    ]
                    if not vrow.empty:
                        current_vendor_name = str(vrow.iloc[0].get("VENDORNAME") or "").strip()
                pre_idx = 0
                if current_vendor_name and vend_options:
                    current_vendor_name_norm = str(current_vendor_name).strip().upper()
                    by_name = {str(v).strip().upper(): str(v) for v in vend_options}
                    selected_vendor_opt = by_name.get(current_vendor_name_norm)
                    if selected_vendor_opt:
                        pre_idx = vend_options.index(selected_vendor_opt) + 1

                form_key = f"grp_form_edit_{edit_gid}"
                name_key = f"grp_edit_name_{edit_gid}"
                vendor_key = f"grp_edit_vendor_{edit_gid}"
                base_key = f"grp_edit_is_base_{edit_gid}"

                with st.form(key=form_key, clear_on_submit=False):
                    new_name = st.text_input("Application name", value=current_name, key=name_key)
                    new_is_base = st.checkbox(
                        "Mark as Shared Unplanned Pool (BASE) for this team",
                        value=current_is_base,
                        key=base_key,
                    )
                    new_vendor_name = _select_with_placeholder(
                        "Default Vendor",
                        vend_options,
                        key=vendor_key,
                        index=pre_idx,
                        disabled=bool(new_is_base),
                    )
                    if bool(new_is_base):
                        _render_base_explainer(compact=True)
                    new_vendor_id = (
                        _lookup_id_by_name(vend_df, "VENDORNAME", "VENDORID", new_vendor_name)
                        if (new_vendor_name and not bool(new_is_base))
                        else None
                    )

                    submit_edit = st.form_submit_button(
                        "Save changes",
                        icon=":material/save:",
                        disabled=(not can_manage_application_edit),
                    )
                    if not can_manage_application_edit:
                        st.caption("You can edit only applications in your assigned scope.")
                    if submit_edit and edit_gid:
                        if not can(
                            principal,
                            "edit",
                            "application",
                            resource_id=str(edit_gid),
                            scope=effective_scope,
                        ):
                            st.error("You do not have permission to edit this application.")
                            st.stop()
                        save_status = st.status("Saving application...", expanded=False) if hasattr(st, "status") else None
                        try:
                            final_name = (new_name or current_name).strip()
                            final_is_base = bool(new_is_base)
                            if final_name != current_name:
                                name_upper = final_name.upper()
                                group_names = _all_group_names_upper(ver_groups)
                                app_names = _all_application_names_upper(ver_apps)
                                if current_name.upper() in group_names:
                                    group_names.remove(current_name.upper())
                                if name_upper in group_names:
                                    st.error(f"Application '{final_name}' already exists.")
                                    st.stop()
                                if name_upper in app_names:
                                    st.error(f"Application '{final_name}' conflicts with an existing instance name.")
                                    st.stop()
                                execute("UPDATE APPLICATION_GROUPS SET GROUPNAME=%s WHERE GROUPID=%s", (final_name, edit_gid))

                            if final_is_base:
                                # Enforce BASE semantics in maintain flow too.
                                execute("UPDATE APPLICATION_GROUPS SET DEFAULT_VENDORID=NULL WHERE GROUPID=%s", (edit_gid,))
                            elif new_vendor_id and new_vendor_id != current_vendor_id:
                                execute("UPDATE APPLICATION_GROUPS SET DEFAULT_VENDORID=%s WHERE GROUPID=%s", (new_vendor_id, edit_gid))

                            if final_is_base != bool(current_is_base):
                                execute(
                                    "UPDATE APPLICATION_GROUPS SET IS_BASE=%s, UPDATED_AT=SYSDATETIME() WHERE GROUPID=%s",
                                    (1 if final_is_base else 0, edit_gid),
                                )

                            bump_version("ver_groups_listing")
                            if save_status is not None:
                                save_status.update(label="Application saved.", state="complete")
                            st.success("Application updated.")
                            _run_post_save_refresh(
                                "application_update",
                                feedback_message="Application updated.",
                            )
                        except Exception as e:
                            if save_status is not None:
                                save_status.update(label="Application save failed.", state="error")
                            st.error(f"Update failed: {e}")

                ado_row_count = int(len(ado_features_df)) if isinstance(ado_features_df, pd.DataFrame) else 0
                ado_status = "loaded" if ado_row_count else "empty or unavailable"
                st.caption(f"ADO Data Status: {ado_status} ({ado_row_count:,} rows)")

                st.markdown("#### Step 3 — ADO Mapping & Impact")
                st.caption(
                    "Map/unmap ADO app values for this application. Mapping drives how demand rolls up into NEXT application KPIs."
                )
                team_variant_keys = []
                try:
                    team_map_df = list_ado_team_mappings()
                    if (
                        team_map_df is not None
                        and not team_map_df.empty
                        and selected_team_id
                        and "TEAMID" in team_map_df.columns
                        and "ADO_TEAM_KEY" in team_map_df.columns
                    ):
                        team_variant_keys = (
                            team_map_df.loc[team_map_df["TEAMID"].astype(str) == str(selected_team_id), "ADO_TEAM_KEY"]
                            .dropna()
                            .astype(str)
                            .tolist()
                        )
                except Exception:
                    team_variant_keys = []
                render_ado_mapping_panel(
                    "app_group",
                    str(edit_gid),
                    context_filters={
                        "team_id": selected_team_id,
                        "program_raws": [selected_program_label] if selected_program_label else None,
                        "team_variant_keys": team_variant_keys,
                        "require_team_scope": bool(selected_team_id),
                        "updated_by": str(user.get("email") or ""),
                    },
                    user_is_admin=can_manage_ado_app_mapping,
                    allow_create_toggle=False,
                    show_unmapped_candidates=True,
                    ado_features_df=ado_features_df,
                    allow_unmap=can_manage_ado_app_mapping,
                )
                with st.container(border=True):
                    st.markdown("**Impact on NEXT charts/KPIs**")
                    st.markdown(
                        "\n".join(
                            [
                                "- ADO mappings define which feature demand is attributed to this NEXT application.",
                                "- Unmapping removes demand attribution from this application.",
                                "- Remapping moves demand attribution to the newly mapped application.",
                                "- Shared Unplanned Pool (BASE) apps spread unplanned demand/cost across mapped apps in the team for the PI.",
                            ]
                        )
                    )

                st.markdown("### Instances Summary")
                st.caption("Most Applications use the Primary Instance. Create additional instances only when invoices differ by site.")
                inst_rows = instances_df[instances_df["GROUPID"].astype(str) == str(edit_gid)] if not instances_df.empty else pd.DataFrame()
                inst_count = len(inst_rows) if inst_rows is not None else 0
                primary_name = "—"
                primary_id = None
                if inst_rows is not None and not inst_rows.empty:
                    app_name_col = "APPLICATIONNAME" if "APPLICATIONNAME" in inst_rows.columns else None
                    app_id_col = "APPLICATIONID" if "APPLICATIONID" in inst_rows.columns else None
                    if app_name_col and app_id_col and "IS_DEFAULT" in inst_rows.columns and (inst_rows["IS_DEFAULT"] == 1).any():
                        primary_name = str(inst_rows.loc[inst_rows["IS_DEFAULT"] == 1, app_name_col].iloc[0])
                        primary_id = str(inst_rows.loc[inst_rows["IS_DEFAULT"] == 1, app_id_col].iloc[0])
                    elif app_name_col and app_id_col:
                        primary_name = str(inst_rows[app_name_col].iloc[0])
                        primary_id = str(inst_rows[app_id_col].iloc[0])
                link_counts = _instance_link_counts()
                inv_count = con_count = 0
                if primary_id and link_counts is not None and not link_counts.empty:
                    row = link_counts.loc[link_counts["APPLICATIONID"].astype(str) == str(primary_id)]
                    if row is not None and not row.empty:
                        inv_count = int(row.iloc[0].get("INVOICE_COUNT") or 0)
                        con_count = int(row.iloc[0].get("CONTRACT_COUNT") or 0)
                st.caption(f"Primary instance: **{primary_name}** • Instances: **{inst_count}** • Invoices: **{inv_count}** • Contracts: **{con_count}**")
                if inst_count == 1 and str(primary_id or "").endswith("__DEFAULT"):
                    st.caption("No additional site/location instances are needed for this Application.")

                st.info("Use the App Assignment tab to manage ownership and MSP team links in bulk.")
                if st.button("Manage instances (advanced)", key="btn_manage_instances"):
                    st.session_state["inst_filter_app"] = current_name
                    st.info("Enable advanced instance tools and switch to the Instances (sites/locations) tab.")
                    st.rerun()

                edit_gid = _safe_first_value(selected_row, "GROUPID") if selected_row is not None and not selected_row.empty else None
                current_name = (_safe_first_value(selected_row, "GROUPNAME") or "").strip() if selected_row is not None and not selected_row.empty else ""
                st.markdown("---")
                if show_debug:
                    with st.expander("Danger zone: Delete an Application", expanded=False):
                        if selected_row is None or selected_row.empty:
                            st.info("Select an application first.")
                        else:
                            deps = get_app_group_dependencies(str(edit_gid or ""))
                            render_dependency_summary(deps)
                            confirm = st.text_input(
                                "Type DELETE to confirm",
                                key="grp_delete_confirm_name",
                                disabled=not can_delete(deps),
                            )
                            st.markdown("**⚠️ This action cannot be undone.**")
                            if st.button(
                                "Delete Application",
                                icon=":material/delete:",
                                key="grp_delete_btn_name",
                                disabled=not can_delete(deps),
                            ):
                                save_status = st.status("Deleting application...", expanded=False) if hasattr(st, "status") else None
                                if confirm.strip().upper() != "DELETE":
                                    if save_status is not None:
                                        save_status.update(label="Delete failed.", state="error")
                                    st.error("Type DELETE to confirm.")
                                else:
                                    try:
                                        if not edit_gid:
                                            if save_status is not None:
                                                save_status.update(label="Delete failed.", state="error")
                                            st.error("Could not find the selected application.")
                                        else:
                                            result = delete_application_group(str(edit_gid))
                                            if result.get("ok"):
                                                bump_version("ver_groups_listing")
                                                if save_status is not None:
                                                    save_status.update(label="Application deleted.", state="complete")
                                                st.success(f"✅ Application '{current_name}' deleted.")
                                                st.rerun()
                                            else:
                                                if save_status is not None:
                                                    save_status.update(label="Delete blocked.", state="error")
                                                st.error(result.get("message") or "Delete blocked.")
                                    except Exception as e:
                                        if save_status is not None:
                                            save_status.update(label="Delete failed.", state="error")
                                        st.error(f"Delete failed: {e}")
# =========================================================
# TAB 2: Instances (sites/locations) (advanced)
# =========================================================
if is_admin and active_app_section == "Instances (sites/locations)":
        st.subheader("Instances (sites/locations)")
        st.caption("Most Applications use the Primary Instance. Create additional instances only when invoices/contracts differ by site.")

        instances_scope_df = instances_df.copy()
        if (
            selected_program
            and selected_program not in ("All", "Select…")
            and "PROGRAMNAME" in instances_scope_df.columns
        ):
            instances_scope_df = instances_scope_df[
                instances_scope_df["PROGRAMNAME"].astype(str).str.strip().str.upper()
                == str(selected_program).strip().upper()
            ]
        if (
            selected_team
            and selected_team not in ("All", "Select…")
            and "TEAMNAME" in instances_scope_df.columns
        ):
            instances_scope_df = instances_scope_df[
                instances_scope_df["TEAMNAME"].astype(str).str.strip().str.upper()
                == str(selected_team).strip().upper()
            ]

        if (
            not instances_scope_df.empty
            and "GROUPID" in instances_scope_df.columns
            and "APPLICATIONID" in instances_scope_df.columns
        ):
            group_counts = instances_scope_df.groupby("GROUPID")["APPLICATIONID"].count().to_dict()
        elif not instances_scope_df.empty and "GROUPID" in instances_scope_df.columns:
            group_counts = instances_scope_df.groupby("GROUPID").size().to_dict()
        else:
            group_counts = {}

        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1.4, 1, 1, 1])
        with filter_col1:
            if not instances_scope_df.empty and "GROUPNAME" in instances_scope_df.columns:
                app_options = sorted(instances_scope_df["GROUPNAME"].dropna().astype(str).str.strip().unique().tolist())
            elif not apps_scope_df.empty and "GROUPNAME" in apps_scope_df.columns:
                app_options = sorted(apps_scope_df["GROUPNAME"].dropna().astype(str).str.strip().unique().tolist())
            else:
                app_options = []
            app_filter = st.selectbox(
                "Application",
                options=["(All)"] + app_options,
                index=0,
                key="inst_filter_app",
            )
        with filter_col2:
            site_filter = st.text_input("Site/Location", value="", key="inst_filter_site")
        with filter_col3:
            multi_only = st.checkbox("Only multi-instance", value=False, key="inst_filter_multi")
        with filter_col4:
            linked_only = st.checkbox("Only with invoices/contracts", value=False, key="inst_filter_linked")

        filtered = instances_scope_df.copy()
        if app_filter and app_filter != "(All)" and "GROUPNAME" in filtered.columns:
            filtered = filtered[
                filtered["GROUPNAME"].astype(str).str.strip().str.upper()
                == str(app_filter).strip().upper()
            ]
        if site_filter:
            add_info_series = filtered["ADD_INFO"] if "ADD_INFO" in filtered.columns else pd.Series([""] * len(filtered), index=filtered.index)
            filtered = filtered[add_info_series.astype(str).str.contains(site_filter, case=False, na=False)]
        if multi_only:
            filtered = filtered[filtered["GROUPID"].map(group_counts).fillna(0).astype(int) > 1]
        if linked_only:
            filtered = filtered[(filtered["INVOICE_COUNT"] + filtered["CONTRACT_COUNT"]) > 0]

        if show_debug:
            with st.expander("Debug – instances filters", expanded=False):
                try:
                    scope_group_names = (
                        sorted(instances_scope_df["GROUPNAME"].dropna().astype(str).str.strip().unique().tolist())
                        if (not instances_scope_df.empty and "GROUPNAME" in instances_scope_df.columns)
                        else []
                    )
                except Exception:
                    scope_group_names = []
                st.write(
                    {
                        "selected_program": selected_program,
                        "selected_team": selected_team,
                        "app_filter": app_filter,
                        "site_filter": site_filter,
                        "multi_only": bool(multi_only),
                        "linked_only": bool(linked_only),
                        "instances_df_rows": int(len(instances_df)) if isinstance(instances_df, pd.DataFrame) else 0,
                        "instances_scope_df_rows": int(len(instances_scope_df)) if isinstance(instances_scope_df, pd.DataFrame) else 0,
                        "filtered_rows": int(len(filtered)) if isinstance(filtered, pd.DataFrame) else 0,
                        "instances_df_columns": list(instances_df.columns) if isinstance(instances_df, pd.DataFrame) else [],
                        "scope_group_names": scope_group_names[:20],
                    }
                )

        colC, colD = st.columns([1.2, 1.2])
        with colC:
            st.markdown("### Instances")
            if filtered.empty:
                st.info("No instances in this scope for the current filters.")
            else:
                display = filtered.copy()
                is_default_series = (
                    display["IS_DEFAULT"]
                    if "IS_DEFAULT" in display.columns
                    else pd.Series([0] * len(display), index=display.index)
                )
                display["Primary"] = is_default_series.apply(
                    lambda v: "Yes" if int(float(v or 0)) == 1 else ""
                )
                show_cols = [
                    c for c in [
                        "GROUPNAME",
                        "APPLICATIONNAME",
                        "ADD_INFO",
                        "Primary",
                        "INVOICE_COUNT",
                        "CONTRACT_COUNT",
                    ] if c in display.columns
                ]
                display = display[show_cols].rename(
                    columns={
                        "GROUPNAME": "Application",
                        "APPLICATIONNAME": "Instance",
                        "ADD_INFO": "Site/Location",
                        "INVOICE_COUNT": "Invoices",
                        "CONTRACT_COUNT": "Contracts",
                    }
                )
                st.dataframe(display, use_container_width=True, hide_index=True, height=360)

            inst_options = (
                filtered["APPLICATIONNAME"].dropna().astype(str).tolist()
                if (not filtered.empty and "APPLICATIONNAME" in filtered.columns)
                else []
            )
            inst_choice = st.selectbox(
                "Instance to edit",
                options=["(Select)"] + inst_options,
                index=0,
                key="inst_instance_select",
            )

        with colD:
            st.markdown("### Instance Details")
            inst_row = (
                filtered[filtered["APPLICATIONNAME"].astype(str) == str(inst_choice)]
                if (inst_choice and inst_choice != "(Select)" and "APPLICATIONNAME" in filtered.columns)
                else pd.DataFrame()
            )
            if inst_row.empty:
                st.info("Select an instance to edit.")
            else:
                inst_row = inst_row.iloc[0]
                edit_aid = str(inst_row.get("APPLICATIONID") or "")
                group_id = str(inst_row.get("GROUPID") or "")
                group_label = str(inst_row.get("GROUPNAME") or "").strip()
                current_name = str(inst_row.get("APPLICATIONNAME") or "").strip()
                current_addinfo = str(inst_row.get("ADD_INFO") or "")
                suffix_default = current_name
                prefix = f"{group_label} - "
                if group_label and current_name.startswith(prefix):
                    suffix_default = current_name[len(prefix):]

                st.caption(f"Application: **{group_label}**")
                with st.form(key="inst_form_edit", clear_on_submit=False):
                    new_suffix = st.text_input("Instance name (suffix)", value=suffix_default, key="inst_edit_suffix")
                    new_addinfo = st.text_input("Site/Location", value=current_addinfo, key="inst_edit_addinfo")
                    submit_inst_edit = st.form_submit_button("Save instance", icon=":material/save:")
                    if submit_inst_edit and edit_aid:
                        save_status = st.status("Saving instance...", expanded=False) if hasattr(st, "status") else None
                        try:
                            final_suffix = (new_suffix or suffix_default).strip()
                            if not final_suffix:
                                st.error("Instance name cannot be empty.")
                                st.stop()
                            final_appname = f"{group_label} - {final_suffix}"
                            upper_final = final_appname.upper()
                            if upper_final != current_name.upper() and upper_final in _all_application_names_upper(ver_apps):
                                st.error(f"Application instance '{final_appname}' already exists.")
                                st.stop()
                            if upper_final in _all_group_names_upper(ver_groups):
                                st.error(f"Instance name '{final_appname}' conflicts with an Application name.")
                                st.stop()
                            update_sqls = []
                            if final_appname != current_name:
                                update_sqls.append((
                                    "UPDATE APPLICATIONS SET APPLICATIONNAME=%s WHERE APPLICATIONID=%s",
                                    (final_appname, edit_aid),
                                ))
                            if (new_addinfo or "") != current_addinfo:
                                update_sqls.append((
                                    "UPDATE APPLICATIONS SET ADD_INFO=%s WHERE APPLICATIONID=%s",
                                    (new_addinfo.strip() or None, edit_aid),
                                ))
                            for sql, params in update_sqls:
                                execute(sql, params)
                            if not update_sqls:
                                if save_status is not None:
                                    save_status.update(label="No changes to save.", state="complete")
                                st.info("No changes to save.")
                            else:
                                bump_version("ver_apps_listing")
                                if save_status is not None:
                                    save_status.update(label="Instance saved.", state="complete")
                                _run_post_save_refresh(
                                    "application_instance_update",
                                    feedback_message="Application instance updated.",
                                )
                        except Exception as e:
                            if save_status is not None:
                                save_status.update(label="Instance save failed.", state="error")
                            st.error(f"Instance update failed: {e}")

                st.markdown("#### Linked invoices/contracts")
                inv_val = inst_row.get("INVOICE_COUNT")
                con_val = inst_row.get("CONTRACT_COUNT")
                inv_count = int(0 if pd.isna(inv_val) else inv_val)
                con_count = int(0 if pd.isna(con_val) else con_val)
                st.caption(f"Invoices: **{inv_count}** • Contracts: **{con_count}**")
                if not invoices_view_df.empty and edit_aid:
                    docs_inst = invoices_view_df.loc[invoices_view_df["APPLICATIONID"].astype(str) == str(edit_aid)]
                    if not docs_inst.empty:
                        show_cols = [c for c in ["DOC_TYPE", "DOC_ID", "AMOUNT_USD"] if c in docs_inst.columns]
                        preview = docs_inst[show_cols].rename(
                            columns={"DOC_TYPE": "Type", "DOC_ID": "Doc ID", "AMOUNT_USD": "Amount (USD)"}
                        )
                        st.dataframe(preview, use_container_width=True, hide_index=True, height=200)

                st.markdown("#### Add site instance")
                add_suffix = st.text_input("New instance suffix", value="", key="inst_add_suffix")
                add_info = st.text_input("Site/Location (optional)", value="", key="inst_add_info")
                if st.button("Add site instance", icon=":material/add:", key="inst_add_btn"):
                    save_status = st.status("Creating instance...", expanded=False) if hasattr(st, "status") else None
                    if not add_suffix.strip():
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Instance suffix is required.")
                    else:
                        try:
                            new_name = f"{group_label} - {add_suffix.strip()}"
                            if new_name.upper() in _all_application_names_upper(ver_apps):
                                if save_status is not None:
                                    save_status.update(label="Create failed.", state="error")
                                st.error(f"Application instance '{new_name}' already exists.")
                            else:
                                upsert_application_instance(
                                    application_id=str(uuid.uuid4()),
                                    group_id=group_id,
                                    application_name=new_name,
                                    add_info=add_info.strip() or None,
                                    vendor_id=None,
                                    updated_by=str(user.get("email") or ""),
                                )
                                bump_version("ver_apps_listing")
                                if save_status is not None:
                                    save_status.update(label="Instance created.", state="complete")
                                _run_post_save_refresh(
                                    "application_instance_create",
                                    feedback_message="Application instance created.",
                                )
                        except Exception as e:
                            if save_status is not None:
                                save_status.update(label="Create failed.", state="error")
                            st.error(f"Create failed: {e}")

        st.markdown("---")
        if show_debug:
            with st.expander("Danger zone: Delete an Application Instance", expanded=False):
                if not instances_df.empty:
                    if "APPLICATIONNAME" not in instances_df.columns or "APPLICATIONID" not in instances_df.columns:
                        st.info("Instance delete UI is unavailable for this schema (missing APPLICATIONNAME/APPLICATIONID).")
                    else:
                        app_name_options = instances_df["APPLICATIONNAME"].dropna().astype(str).sort_values().unique().tolist()
                        del_app_name = _select_with_placeholder("Select Application Instance to delete", app_name_options, key="inst_delete_by_name")
                        app_row_del = instances_df[instances_df["APPLICATIONNAME"] == del_app_name] if del_app_name else None
                        app_id = _safe_first_value(app_row_del, "APPLICATIONID") if app_row_del is not None else None
                        deps = get_app_instance_dependencies(str(app_id or ""))
                        if del_app_name:
                            render_dependency_summary(deps)
                        confirm = st.text_input(
                            "Type DELETE to confirm",
                            key="inst_delete_confirm_name",
                            disabled=del_app_name is None or not can_delete(deps),
                        )
                        st.markdown("**⚠️ This action cannot be undone.**")
                        if st.button(
                            "Delete Application Instance",
                            icon=":material/delete:",
                            key="inst_delete_btn_name",
                            disabled=del_app_name is None or not can_delete(deps),
                        ):
                            if confirm.strip().upper() != "DELETE":
                                st.error("Type DELETE to confirm.")
                            else:
                                try:
                                    if not app_id:
                                        st.error("Could not find the selected instance.")
                                    else:
                                        result = delete_application(str(app_id))
                                        if result.get("ok"):
                                            bump_version("ver_apps_listing")
                                            _run_post_save_refresh(
                                                "application_instance_delete",
                                                feedback_message=f"Application instance '{del_app_name}' deleted.",
                                                bump_version=True,
                                            )
                                        else:
                                            st.error(result.get("message") or "Delete blocked.")
                                except Exception as e:
                                    st.error(f"Delete failed: {e}")
                else:
                    st.info("No instances available in this scope.")

        with st.expander("Orphan Instances (no application)", expanded=False):
            orphans = _orphan_instances(ver_apps)
            if orphans is None or orphans.empty:
                st.caption("No orphan instances detected.")
            else:
                st.dataframe(orphans[["APPLICATIONNAME", "ADD_INFO"]], use_container_width=True, hide_index=True, height=200)
                st.caption("Attach selected orphan instances to an Application in the selected Team above.")
                groups_for_attach = _df_or_empty(_groups_listing(ver_groups))
                target_options = groups_for_attach["GROUPNAME"].dropna().astype(str).tolist() if not groups_for_attach.empty else []
                if not target_options:
                    st.warning("No Applications available in this scope to attach orphan instances.")
                else:
                    target_group_name = st.selectbox("Target Application", options=target_options, key="inst_orphan_target_group")
                    target_group_row = groups_for_attach[groups_for_attach["GROUPNAME"] == target_group_name]
                    target_group_id = _safe_first_value(target_group_row, "GROUPID") if target_group_row is not None else None
                    target_group_vendor = _safe_first_value(target_group_row, "VENDORID") if target_group_row is not None else None
                    orphan_names = orphans["APPLICATIONNAME"].dropna().astype(str).tolist()
                    sel_orphans = st.multiselect("Orphan Instances to attach", orphan_names, key="inst_orphan_pick")
                    auto_rename = st.checkbox("Auto-rename to 'Application - Suffix' if not already canonical", value=True, key="inst_orphan_rename")
                    align_vendor = st.checkbox("Align instance vendor to the application's default vendor", value=True, key="inst_orphan_align_vendor")

                    can_attach = bool(sel_orphans) and bool(target_group_id)
                    if st.button("Attach selected orphans", icon=":material/link:", disabled=not can_attach, key="btn_attach_orphans"):
                        save_status = st.status("Attaching orphan instances...", expanded=False) if hasattr(st, "status") else None
                        try:
                            orphan_rows = orphans[orphans["APPLICATIONNAME"].isin(sel_orphans)].copy()
                            updates = []
                            target_group_label = target_group_name or ""
                            prefix = f"{target_group_label} - "
                            existing_upper = _all_application_names_upper(ver_apps)

                            for _, r in orphan_rows.iterrows():
                                app_id = str(r["APPLICATIONID"])
                                old_name = str(r["APPLICATIONNAME"]).strip()
                                new_name = old_name

                                if auto_rename:
                                    if target_group_label and not old_name.upper().startswith(prefix.upper()):
                                        if " - " in old_name:
                                            suffix = old_name.split(" - ", 1)[-1].strip()
                                            new_name = f"{target_group_label} - {suffix}"
                                        else:
                                            new_name = f"{target_group_label} - {old_name}"
                                    if new_name.upper() != old_name.upper() and new_name.upper() in existing_upper:
                                        new_name = f"{new_name} ({app_id[:6]})"

                                if new_name != old_name:
                                    updates.append((
                                        "UPDATE APPLICATIONS SET APPLICATIONNAME=%s WHERE APPLICATIONID=%s", (new_name, app_id)
                                    ))

                                updates.append((
                                    "UPDATE APPLICATIONS SET GROUPID=%s WHERE APPLICATIONID=%s", (target_group_id, app_id)
                                ))

                                if align_vendor and target_group_vendor:
                                    updates.append((
                                        "UPDATE APPLICATIONS SET VENDORID=%s WHERE APPLICATIONID=%s", (target_group_vendor, app_id)
                                    ))

                            for sql, params in updates:
                                execute(sql, params)

                            bump_version("ver_apps_listing")
                            if save_status is not None:
                                save_status.update(label="Orphan instances attached.", state="complete")
                            st.success(f"Attached {len(orphan_rows)} orphan instance(s) to **{target_group_label}**.")
                            _run_post_save_refresh(
                                "application_attach_orphans",
                                feedback_message=f"Attached {len(orphan_rows)} orphan instance(s) to '{target_group_label}'.",
                            )
                        except Exception as e:
                            if save_status is not None:
                                save_status.update(label="Attach failed.", state="error")
                            st.error(f"Attach failed: {e}")

        if is_admin:
            with st.expander("Admin: Fix missing primary instances", expanded=False):
                st.caption("Create a Primary instance for any Application missing one.")
                if st.button("Fix missing Primary instances", icon=":material/build:", key="btn_fix_primary_instances"):
                    save_status = st.status("Verifying primary instances...", expanded=False) if hasattr(st, "status") else None
                    try:
                        ensure_default_instances(execute, fetch_df)
                        bump_version("ver_apps_listing")
                        if save_status is not None:
                            save_status.update(label="Primary instances verified/created.", state="complete")
                        st.success("Primary instances verified/created.")
                        _run_post_save_refresh(
                            "application_fix_primary_instances",
                            feedback_message="Primary instances verified/created.",
                        )
                    except Exception as e:
                        if save_status is not None:
                            save_status.update(label="Fix failed.", state="error")
                        st.error(f"Fix failed: {e}")
# =========================================================
# TAB 3: MSP Assignment (bulk)
# =========================================================
if active_app_section == "App Assignment":
    st.subheader("Application Ownership & MSP Links")
    st.caption("This editor is scoped by the filters above. Use it to update ownership and MSP assignment.")
    if st.session_state.get("msp_bulk_flash"):
        st.success(str(st.session_state.get("msp_bulk_flash")))
        st.session_state.pop("msp_bulk_flash", None)

    if apps_view_df is None or apps_view_df.empty:
        st.info("Next: select a Program or Team above to load scoped Applications.")
    elif not can_manage_application_edit:
        st.info("🔒 Editing is available only for applications in your assigned scope. Current ownership and MSP assignment are shown below.")
        group_ids = apps_view_df["GROUPID"].dropna().astype(str).unique().tolist()
        msp_map = {}
        if group_ids:
            placeholders = ", ".join(["%s"] * len(group_ids))
            link_df = _df_or_empty(
                fetch_df(
                    f"""
                    SELECT l.GROUPID, t.TEAMNAME
                    FROM APP_GROUP_TEAM_LINKS l
                    JOIN TEAMS t ON t.TEAMID = l.TEAMID
                    JOIN TEAM_MSP_RATE m ON m.TEAMID = t.TEAMID
                    WHERE COALESCE(m.MSP_ENABLED, 0) = 1
                      AND l.GROUPID IN ({placeholders})
                    """,
                    tuple(group_ids),
                )
            )
            if not link_df.empty:
                for gid, sub in link_df.groupby("GROUPID", dropna=False):
                    names = sorted(sub["TEAMNAME"].dropna().astype(str).tolist())
                    msp_map[str(gid)] = names[0] if names else ""
        view_df = apps_view_df.copy()
        team_series = view_df["TEAMNAME"] if "TEAMNAME" in view_df.columns else pd.Series([""] * len(view_df), index=view_df.index)
        view_df["Accountable Team"] = team_series.astype(str).str.strip()
        view_df["MSP Team"] = view_df["GROUPID"].astype(str).map(lambda gid: msp_map.get(str(gid), ""))
        show_cols = ["GROUPNAME", "PROGRAMNAME", "Accountable Team", "MSP Team"]
        st.dataframe(
            view_df[show_cols].rename(columns={"GROUPNAME": "Application", "PROGRAMNAME": "Program"}),
            use_container_width=True,
            hide_index=True,
            height=420,
        )
    else:
        st.caption(
            "This editor updates Application ownership and MSP assignment for the applications in the current scope. "
            "Leave ‘New’ fields blank if you don’t want to change them. Use Apply to commit changes."
        )
        program_ids = (
            apps_view_df.get("PROGRAMID", pd.Series(dtype=str))
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        team_frames = []
        msp_team_frames = []
        for pid in program_ids:
            df_team = _teams_in_program(pid)
            if df_team is not None and not df_team.empty:
                team_frames.append(df_team)
            df_msp = _msp_teams_in_program(pid)
            if df_msp is not None and not df_msp.empty:
                msp_team_frames.append(df_msp)
        teams_all = pd.concat(team_frames, ignore_index=True) if team_frames else pd.DataFrame()
        if not teams_all.empty:
            teams_all = teams_all.drop_duplicates(subset=["TEAMID"]).copy()
        team_opts = teams_all["TEAMNAME"].astype(str).tolist() if not teams_all.empty else []
        team_by_name = (
            {str(r.TEAMNAME): str(r.TEAMID) for _, r in teams_all.iterrows()}
            if not teams_all.empty
            else {}
        )

        msp_teams_all = pd.concat(msp_team_frames, ignore_index=True) if msp_team_frames else pd.DataFrame()
        if not msp_teams_all.empty:
            msp_teams_all = msp_teams_all.drop_duplicates(subset=["TEAMID"]).copy()
        msp_team_opts = msp_teams_all["TEAMNAME"].astype(str).tolist() if not msp_teams_all.empty else []
        msp_team_by_name = (
            {str(r.TEAMNAME): str(r.TEAMID) for _, r in msp_teams_all.iterrows()}
            if not msp_teams_all.empty
            else {}
        )

        group_ids = apps_view_df["GROUPID"].dropna().astype(str).unique().tolist()
        msp_map = {}
        if group_ids:
            placeholders = ", ".join(["%s"] * len(group_ids))
            link_df = _df_or_empty(
                fetch_df(
                    f"""
                    SELECT l.GROUPID, t.TEAMNAME
                    FROM APP_GROUP_TEAM_LINKS l
                    JOIN TEAMS t ON t.TEAMID = l.TEAMID
                    JOIN TEAM_MSP_RATE m ON m.TEAMID = t.TEAMID
                    WHERE COALESCE(m.MSP_ENABLED, 0) = 1
                      AND l.GROUPID IN ({placeholders})
                    """,
                    tuple(group_ids),
                )
            )
            if not link_df.empty:
                for gid, sub in link_df.groupby("GROUPID", dropna=False):
                    names = sorted(sub["TEAMNAME"].dropna().astype(str).tolist())
                    msp_map[str(gid)] = names[0] if names else ""

        editor_base = apps_view_df.copy()
        team_series = editor_base["TEAMNAME"] if "TEAMNAME" in editor_base.columns else pd.Series([""] * len(editor_base), index=editor_base.index)
        editor_base["Owner Team (Current)"] = team_series.astype(str).str.strip()
        editor_base["Owner Team (New)"] = ""
        editor_base["MSP Team (Current)"] = editor_base["GROUPID"].astype(str).map(
            lambda gid: msp_map.get(str(gid), "")
        )
        editor_base["MSP Team (New)"] = ""
        editor_base["Apply"] = False

        display_cols = [
            "GROUPID",
            "GROUPNAME",
            "PROGRAMNAME",
            "Owner Team (Current)",
            "Owner Team (New)",
            "MSP Team (Current)",
            "MSP Team (New)",
            "Apply",
        ]
        editor_view = editor_base[display_cols].rename(
            columns={
                "GROUPID": "GROUPID",
                "GROUPNAME": "Application",
                "PROGRAMNAME": "Program",
            }
        )
        editor_view = editor_view.set_index("GROUPID")

        edit = st.data_editor(
            editor_view,
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                "Owner Team (New)": st.column_config.SelectboxColumn(
                    "Owner Team (New)",
                    options=[""] + team_opts,
                ),
                "MSP Team (New)": st.column_config.SelectboxColumn(
                    "MSP Team (New)",
                    options=[""] + msp_team_opts,
                ),
                "Apply": st.column_config.CheckboxColumn("Apply"),
            },
            disabled=[
                "Application",
                "Program",
                "Owner Team (Current)",
                "MSP Team (Current)",
            ],
            key="app_assign_editor",
        )

        if st.button("Apply changes", icon=":material/save:", key="app_assign_apply"):
            save_status = st.status("Applying assignment changes...", expanded=False) if hasattr(st, "status") else None
            updated_owner = 0
            updated_msp = 0
            removed_msp = 0
            skipped_rows = []
            errors = []

            for gid, row in edit.iterrows():
                if not bool(row.get("Apply")):
                    continue
                app_name = str(row.get("Application") or "").strip()
                current_owner = str(row.get("Owner Team (Current)") or "").strip()
                current_msp = str(row.get("MSP Team (Current)") or "").strip()
                new_owner = str(row.get("Owner Team (New)") or "").strip()
                new_msp = str(row.get("MSP Team (New)") or "").strip()

                owner_change = bool(new_owner) and new_owner != current_owner
                msp_change = (new_msp != current_msp) and (new_msp or current_msp)
                if not owner_change and not msp_change:
                    skipped_rows.append((app_name, "No changes detected"))
                    continue

                if owner_change and new_owner not in team_by_name:
                    skipped_rows.append((app_name, "Unknown owner team"))
                    continue
                if new_msp and new_msp not in msp_team_by_name:
                    skipped_rows.append((app_name, "Unknown MSP team"))
                    continue

                try:
                    if owner_change:
                        owner_id = team_by_name.get(new_owner)
                        if owner_id:
                            execute("UPDATE APPLICATION_GROUPS SET TEAMID=%s WHERE GROUPID=%s", (owner_id, str(gid)))
                            execute(
                                """
                                UPDATE g
                                   SET g.PROGRAMID = t.PROGRAMID
                                FROM APPLICATION_GROUPS g
                                JOIN TEAMS t ON t.TEAMID = g.TEAMID
                                WHERE g.GROUPID = %s
                                """,
                                (str(gid),),
                            )
                            updated_owner += 1

                    if msp_change:
                        execute(
                            """
                            DELETE FROM APP_GROUP_TEAM_LINKS
                            WHERE GROUPID=%s
                              AND TEAMID IN (
                                SELECT TEAMID FROM TEAM_MSP_RATE WHERE COALESCE(MSP_ENABLED, 0)=1
                              )
                            """,
                            (str(gid),),
                        )
                        if not new_msp:
                            removed_msp += 1
                        else:
                            msp_team_id = msp_team_by_name.get(new_msp)
                            if msp_team_id:
                                execute(
                                    """
                                    MERGE INTO APP_GROUP_TEAM_LINKS l
                                    USING (SELECT %s GROUPID, %s TEAMID) s
                                    ON l.GROUPID = s.GROUPID AND l.TEAMID = s.TEAMID
                                    WHEN NOT MATCHED THEN INSERT (GROUPID, TEAMID) VALUES (s.GROUPID, s.TEAMID);
                                    """,
                                    (str(gid), str(msp_team_id)),
                                )
                                updated_msp += 1
                except Exception as e:
                    errors.append((app_name, str(e)))

            if updated_owner or updated_msp or removed_msp:
                st.session_state["msp_bulk_flash"] = (
                    f"Applied changes — Owners updated: {updated_owner}, "
                    f"MSP updated: {updated_msp}, MSP removed: {removed_msp}."
                )
                st.session_state.pop("app_assign_editor", None)
                if save_status is not None:
                    save_status.update(label="Assignment changes applied.", state="complete")
                _run_post_save_refresh(
                    "application_bulk_owner_msp",
                    feedback_message=(
                        f"Applied changes — Owners updated: {updated_owner}, "
                        f"MSP updated: {updated_msp}, MSP removed: {removed_msp}."
                    ),
                )
            else:
                if save_status is not None:
                    save_status.update(label="No changes to apply.", state="complete")
                st.info("No changes applied.")

            if skipped_rows:
                skipped_df = pd.DataFrame(skipped_rows, columns=["Application", "Reason"])
                st.caption("Skipped rows")
                st.dataframe(skipped_df, use_container_width=True, hide_index=True, height=200)

            if errors:
                if save_status is not None:
                    save_status.update(label="Completed with errors.", state="error")
                err_df = pd.DataFrame(errors, columns=["Application", "Error"])
                st.caption("Errors")
                st.dataframe(err_df, use_container_width=True, hide_index=True, height=200)
