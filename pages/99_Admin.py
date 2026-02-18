# Summary: update email alert UI defaults to NEXT branding.
# Admin.py — Advanced admin page (danger zone)
import os
import re
import time
import uuid
import traceback
import datetime as dt
from typing import Optional, Tuple, List, Dict, Any, Iterable, Sequence, Callable

import pandas as pd
import streamlit as st

st.set_page_config(page_title="NEXT - Admin", layout="wide")
st.session_state["_page_config_set"] = True


def safe_import(label: str, importer: Callable[[], Any]):
    try:
        return importer(), None
    except Exception as e:
        return None, e


def _render_missing_dependency(label: str, e: Exception) -> None:
    st.error(f"{label} is not available in this environment.")
    st.caption(str(e))
    st.exception(e)

def portfolio_db_ready() -> bool:
    try:
        try:
            from db.control_db import control_db_available, control_fetch_df
            from core.db_passwords import resolve_portfolio_db_password
        except Exception:
            control_db_available = None  # type: ignore
            control_fetch_df = None  # type: ignore
            resolve_portfolio_db_password = None  # type: ignore

        if control_db_available and control_db_available():
            try:
                df_ports = control_fetch_df("SELECT * FROM CONTROL_PORTFOLIOS ORDER BY PORTFOLIO_KEY", None)
            except Exception:
                df_ports = None
            if df_ports is None or df_ports.empty:
                return False
            active_key = str(st.session_state.get("active_portfolio_key") or "").strip()
            if not active_key:
                return False
            row = df_ports[df_ports["PORTFOLIO_KEY"].astype(str) == active_key]
            if row is None or row.empty:
                return False
            r0 = row.iloc[0]
            password_key = str(r0.get("DB_PASSWORD_KEY") or "").strip()
            db_password = ""
            if resolve_portfolio_db_password is not None:
                db_password = resolve_portfolio_db_password(
                    password_key,
                    str(r0.get("PORTFOLIO_KEY") or "").strip(),
                    str(r0.get("PROFILE_KEY") or "").strip(),
                ) or ""
            required_vals = {
                "server": str(r0.get("DB_SERVER") or "").strip(),
                "database": str(r0.get("DB_DATABASE") or "").strip(),
                "user": str(r0.get("DB_USER") or "").strip(),
                "driver": str(r0.get("DB_DRIVER") or "").strip(),
                "password": db_password,
            }
            missing = [k for k, v in required_vals.items() if not v]
            if missing:
                st.session_state["_portfolio_db_error_details"] = {
                    "portfolio_key": active_key,
                    "server": required_vals.get("server"),
                    "database": required_vals.get("database"),
                    "user": required_vals.get("user"),
                    "missing": missing,
                }
                return False
            return True

        required = [
            "MSSQL_SERVER",
            "MSSQL_DATABASE",
            "MSSQL_USER",
            "MSSQL_PASSWORD",
            "MSSQL_DRIVER",
        ]
        if all(str(os.getenv(k, "")).strip() for k in required):
            return True
        from db.mssql_backend import _default_cfg  # local import to avoid early DB init

        _default_cfg()
        return True
    except Exception:
        st.session_state["admin_startup_error"] = traceback.format_exc()
        return False


def _render_setup_mode() -> None:
    try:
        from db.control_db import (
            control_db_available,
            create_control_schema,
            control_schema_status,
            control_fetch_df,
            control_execute,
        )
        from core.db_passwords import resolve_portfolio_db_password
        from db.mssql_backend import MssqlConfig
    except Exception as e:
        st.title("Admin (Setup Mode)")
        st.warning(
            "Setup Mode could not load Control DB helpers. "
            "Admin setup tools are unavailable in this environment."
        )
        st.code(str(e))
        return

    st.title("Admin (Setup Mode)")
    st.info(
        "No active portfolio connection is ready yet. "
        "Setup Mode shows only Control DB onboarding and diagnostics."
    )
    st.markdown("### Admin Diagnostics")
    active_key = str(st.session_state.get("active_portfolio_key") or "").strip()
    st.caption(f"Active portfolio key: {active_key or 'None'}")

    control_ok = False
    try:
        control_ok = bool(control_db_available())
    except Exception:
        control_ok = False
    st.caption(f"Control DB available: {'Yes' if control_ok else 'No'}")
    st.caption("Portfolio DB name: Unavailable in Setup Mode")
    st.caption("Data version: Unavailable in Setup Mode")
    startup_error = str(st.session_state.get("admin_startup_error") or "").strip()
    if startup_error:
        with st.expander("Startup error details", expanded=False):
            st.code(startup_error)

    if not control_ok:
        st.error("Control DB is not configured or unavailable. Set CONTROL_MSSQL_* env vars.")
        return

    status = {}
    try:
        status = control_schema_status() or {}
    except Exception as e:
        st.error(f"Control DB status check failed: {e}")
        return

    if st.button("Create Control DB schema", type="primary"):
        try:
            create_control_schema()
            st.success("Control DB schema created.")
        except Exception as e:
            st.error(f"Failed to create Control DB schema: {e}")

    st.markdown("### Portfolios")
    df_ports = None
    try:
        df_ports = control_fetch_df("SELECT * FROM CONTROL_PORTFOLIOS ORDER BY PORTFOLIO_KEY", None)
        if df_ports is not None and not df_ports.empty:
            df_show = df_ports.copy()
            if "DB_PASSWORD" in df_show.columns:
                df_show["DB_PASSWORD"] = None
            if "DB_PASSWORD_KEY" in df_show.columns:
                df_show["DB_PASSWORD_KEY"] = df_show["DB_PASSWORD_KEY"].fillna("").astype(str).str.strip()
            st.dataframe(df_show, use_container_width=True)
        else:
            st.caption("No portfolios registered yet.")
    except Exception as e:
        st.warning(f"Could not load portfolios: {e}")

    if not active_key:
        st.info("Select a portfolio in the sidebar to enable full Admin tools.")
    else:
        st.caption(f"Active portfolio: {active_key}")
        if df_ports is not None and not df_ports.empty:
            row = df_ports[df_ports["PORTFOLIO_KEY"].astype(str) == active_key]
            if row is not None and not row.empty:
                r0 = row.iloc[0]
                password_key = str(r0.get("DB_PASSWORD_KEY") or "").strip()
                db_password = resolve_portfolio_db_password(
                    password_key,
                    str(r0.get("PORTFOLIO_KEY") or "").strip(),
                    str(r0.get("PROFILE_KEY") or "").strip(),
                ) or ""
                missing = [
                    k
                    for k, v in {
                        "server": str(r0.get("DB_SERVER") or "").strip(),
                        "database": str(r0.get("DB_DATABASE") or "").strip(),
                        "user": str(r0.get("DB_USER") or "").strip(),
                        "driver": str(r0.get("DB_DRIVER") or "").strip(),
                        "password": db_password,
                    }.items()
                    if not v
                ]
                if missing:
                    st.warning(f"Active portfolio connection incomplete. Missing: {', '.join(missing)}")
                else:
                    try:
                        from db import fetch_df

                        cfg = MssqlConfig(
                            server=str(r0.get("DB_SERVER") or "").strip(),
                            database=str(r0.get("DB_DATABASE") or "").strip(),
                            user=str(r0.get("DB_USER") or "").strip(),
                            password=db_password,
                            driver=str(r0.get("DB_DRIVER") or "ODBC Driver 18 for SQL Server").strip(),
                            encrypt="yes" if bool(r0.get("DB_ENCRYPT", True)) else "no",
                            trust_server_certificate="yes"
                            if bool(r0.get("DB_TRUST_SERVER_CERTIFICATE", False))
                            else "no",
                            schema=str(r0.get("DB_SCHEMA") or "dbo").strip() or "dbo",
                        )
                        db_df = fetch_df("SELECT DB_NAME() AS DB_NAME", cfg=cfg)
                        if db_df is not None and not db_df.empty:
                            st.caption(f"Portfolio DB name: {db_df.iloc[0].get('DB_NAME')}")
                        dv_df = fetch_df(
                            "SELECT TOP 1 VERSION AS DATA_VERSION, UPDATED_AT FROM DATA_VERSION ORDER BY UPDATED_AT DESC",
                            cfg=cfg,
                        )
                        if dv_df is not None and not dv_df.empty:
                            st.caption(
                                "Data version: "
                                f"{dv_df.iloc[0].get('DATA_VERSION')} "
                                f"(updated {dv_df.iloc[0].get('UPDATED_AT')})"
                            )
                    except Exception:
                        pass

    with st.form("control_portfolio_create_form"):
        col_a, col_b = st.columns(2)
        with col_a:
            p_key = st.text_input("Portfolio key").strip()
            p_name = st.text_input("Display name").strip()
            db_server = st.text_input("DB server").strip()
            db_name = st.text_input("DB database").strip()
            db_user = st.text_input("DB user").strip()
            db_password_key = st.text_input("DB password key").strip()
        with col_b:
            db_schema = st.text_input("DB schema", value="dbo").strip()
            db_driver = st.text_input("DB driver", value="ODBC Driver 18 for SQL Server").strip()
            db_encrypt = st.checkbox("DB encrypt", value=True)
            db_trust = st.checkbox("DB trust server certificate", value=False)
            profile_key = st.text_input("Profile key").strip()
            status_val = st.selectbox("Status", ["ACTIVE", "ONBOARDING", "INACTIVE"], index=0)
        submitted = st.form_submit_button("Create portfolio", type="primary")
    if submitted:
        if not p_key:
            st.error("Portfolio key is required.")
        else:
            try:
                control_execute(
                    """
                    INSERT INTO CONTROL_PORTFOLIOS (
                      PORTFOLIO_KEY, DISPLAY_NAME, DB_SERVER, DB_DATABASE, DB_USER,
                      DB_PASSWORD_KEY, DB_SCHEMA, DB_DRIVER, DB_ENCRYPT, DB_TRUST_SERVER_CERTIFICATE,
                      PROFILE_KEY, STATUS
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        p_key,
                        p_name or p_key,
                        db_server or None,
                        db_name or None,
                        db_user or None,
                        db_password_key or None,
                        db_schema or "dbo",
                        db_driver or None,
                        1 if db_encrypt else 0,
                        1 if db_trust else 0,
                        profile_key or None,
                        status_val,
                    ),
                )
                st.success("Portfolio created.")
            except Exception as e:
                st.error(f"Failed to create portfolio: {e}")


if not portfolio_db_ready():
    _render_setup_mode()
    st.stop()
    raise SystemExit(0)

try:
    from utils.email import EmailConfigError, EmailSendError, get_mail_status, send_graph_mail
    from utils.toast import toast_error, toast_success
    from utils.admin_audit import get_recent_admin_actions, log_admin_action
    from utils.admin_shell import render_admin_header, render_context_bar, render_audit_panel, render_section, render_empty_state, render_perf_info
    from utils.admin_entity_registry import list_entities_for, get_registry_diagnostics, get_entity_by_id, table_exists
    from utils.admin_safety import render_preview_apply_audit_block
    from utils.admin_context import get_admin_context, apply_admin_context_filters
    from utils.admin_delete_guard import (
        get_program_dependencies,
        get_team_dependencies,
        get_app_group_dependencies,
        get_app_instance_dependencies,
        render_dependency_summary,
        can_delete,
        safe_delete_program,
        safe_delete_team,
        safe_delete_app_group,
        safe_delete_app_instance,
        scan_for_orphans,
    )
    from utils.pages_catalog import permissions_matrix_df
    from core.portfolio_context import get_portfolio_ctx
    from core.db_session import db_fetch_df as fetch_df_active, db_execute as execute_active, get_active_cfg

    from db import (
        ensure_tables as _ensure_tables,
        ensure_ado_minimal_tables,
        ensure_apptio_actuals_lines_table,
        ensure_map_apptio_to_cost_type_table,
        ensure_all_views_ok,
        ensure_analytics_views_ok,
        fetch_df,
        upsert_program,
        upsert_program_additional_cost,
        upsert_team,
        upsert_vendor,
        upsert_application_group,
        upsert_application_instance,
        upsert_invoice,
        upsert_team_rate_history,
        upsert_program_rate_history,
        upsert_team_headcount,
        upsert_contractor_company,
        upsert_contractor_rate,
        upsert_team_contractor_headcount,
        upsert_program_apptio_workid,
        upsert_apptio_actuals,
        load_ado_portfolio_settings,
        ensure_ado_portfolio_settings_table,
        ensure_ado_iteration_calendar_table,
        ensure_team_msp_assignments_table,
        ensure_team_composition_history,
        ensure_program_composition_history,
        ensure_user_membership_tables,
        _fq,
        ensure_email_alert_config_table,
        get_email_alert_config,
        save_email_alert_config,
        get_data_version_info,
        bump_data_version,
        ensure_access_control_tables,
        list_app_users,
        upsert_app_user,
    )
    from db.control_db import (
        ensure_control_schema,
        create_control_schema,
        control_fetch_df,
        control_execute,
        control_db_available,
        is_control_global_admin,
    )
    from core.db_passwords import resolve_portfolio_db_password
    from db.bootstrap_portfolio_db import bootstrap_portfolio_db
    from db.mssql_backend import MssqlConfig, get_default_db_config_dict
    from core.portfolio_runtime import get_active_portfolio_key
except Exception as _admin_full_import_error:
    st.title("Admin")
    _render_missing_dependency("Admin full-mode dependencies", _admin_full_import_error)
    with st.expander("Startup traceback", expanded=False):
        st.code(traceback.format_exc())
    st.stop()
    raise SystemExit(1)

EXPECTED_TABLES: List[Dict[str, Any]] = []
EXPECTED_VIEWS: List[Dict[str, Any]] = []
EXPECTED_EDGES: List[Tuple[str, str]] = []

def _schema_introspect_unavailable_df(*args, **kwargs):
    return pd.DataFrame()

list_db_tables = _schema_introspect_unavailable_df
list_db_views = _schema_introspect_unavailable_df
list_db_columns = _schema_introspect_unavailable_df
list_view_dependencies = _schema_introspect_unavailable_df
list_deprecated_objects = _schema_introspect_unavailable_df
get_row_count = lambda *args, **kwargs: 0  # type: ignore
schema_object_exists = lambda *args, **kwargs: False  # type: ignore
mark_deprecated = lambda *args, **kwargs: None  # type: ignore
dot_available = lambda *args, **kwargs: False  # type: ignore
render_dot_to_svg = lambda *args, **kwargs: None  # type: ignore
render_svg_scrollable = lambda *args, **kwargs: None  # type: ignore
download_svg_button = lambda *args, **kwargs: None  # type: ignore

def _load_schema_helpers() -> Optional[Exception]:
    global EXPECTED_TABLES, EXPECTED_VIEWS, EXPECTED_EDGES
    global list_db_tables, list_db_views, list_db_columns, list_view_dependencies
    global get_row_count, schema_object_exists, mark_deprecated, list_deprecated_objects
    global dot_available, render_dot_to_svg, render_svg_scrollable, download_svg_button

    if EXPECTED_TABLES:
        return None

    bundle, err = safe_import(
        "Schema & Dependencies helpers",
        lambda: (
            __import__("utils.admin_schema_manifest", fromlist=["EXPECTED_TABLES", "EXPECTED_VIEWS", "EXPECTED_EDGES"]),
            __import__("utils.admin_schema_introspect", fromlist=[
                "list_db_tables", "list_db_views", "list_db_columns", "list_view_dependencies",
                "get_row_count", "object_exists", "mark_deprecated", "list_deprecated_objects"
            ]),
            __import__("utils.admin_graphviz_cli", fromlist=[
                "dot_available", "render_dot_to_svg", "render_svg_scrollable", "download_svg_button"
            ]),
        ),
    )
    if err is not None:
        return err

    manifest_mod, introspect_mod, graphviz_mod = bundle
    EXPECTED_TABLES = list(getattr(manifest_mod, "EXPECTED_TABLES", []) or [])
    EXPECTED_VIEWS = list(getattr(manifest_mod, "EXPECTED_VIEWS", []) or [])
    EXPECTED_EDGES = list(getattr(manifest_mod, "EXPECTED_EDGES", []) or [])
    list_db_tables = getattr(introspect_mod, "list_db_tables")
    list_db_views = getattr(introspect_mod, "list_db_views")
    list_db_columns = getattr(introspect_mod, "list_db_columns")
    list_view_dependencies = getattr(introspect_mod, "list_view_dependencies")
    get_row_count = getattr(introspect_mod, "get_row_count")
    schema_object_exists = getattr(introspect_mod, "object_exists")
    mark_deprecated = getattr(introspect_mod, "mark_deprecated")
    list_deprecated_objects = getattr(introspect_mod, "list_deprecated_objects")
    dot_available = getattr(graphviz_mod, "dot_available")
    render_dot_to_svg = getattr(graphviz_mod, "render_dot_to_svg")
    render_svg_scrollable = getattr(graphviz_mod, "render_svg_scrollable")
    download_svg_button = getattr(graphviz_mod, "download_svg_button")
    return None
try:
    from db import upsert_ado_features  # ADO features upsert
except Exception:
    def upsert_ado_features(*args, **kwargs):  # type: ignore
        raise RuntimeError("ADO_FEATURES upsert helper not available")
try:
    from db import upsert_ado_iteration_calendar  # Iteration calendar upsert
except Exception:
    def upsert_ado_iteration_calendar(*args, **kwargs):  # type: ignore
        raise RuntimeError("ADO_ITERATION_CALENDAR upsert helper not available")
try:
    from db import recompute_feature_iteration_mapping as _recompute_feature_iteration_mapping  # type: ignore
except Exception:
    _recompute_feature_iteration_mapping = None
try:
    from utils.azure_ad import acquire_graph_token_device, search_users_basic
except Exception:
    def acquire_graph_token_device(*args, **kwargs):
        raise RuntimeError("Azure AD support not configured. Set [azuread] client_id/tenant_id and install msal.")
    def search_users_basic(*args, **kwargs):
        return []

# Auth guard: contributors only for this page
try:
    from utils.auth import ensure_sso
except Exception:
    st.error("Authentication is not available. Contact an admin.")
    st.stop()

# Optional sidebar helper; fall back safely if not present
def _warn_if_empty_feature_demand() -> None:
    try:
        cnt = fetch_df_active("SELECT COUNT(*) AS N FROM VW_TCO_FEATURE_DEMAND", None)
        n = int(cnt.iloc[0]["N"]) if cnt is not None and not cnt.empty else 0
    except Exception as e:
        st.warning(f"Could not read VW_TCO_FEATURE_DEMAND row count: {e}")
        return
    if n <= 0:
        st.warning(
            "VW_TCO_FEATURE_DEMAND has 0 rows. Iteration labels may not match the PI parser "
            "or the iteration calendar may be missing."
        )
    else:
        st.info(f"VW_TCO_FEATURE_DEMAND rows: {n:,d}")


def _render_view_diagnostics() -> None:
    st.markdown("#### View diagnostics")
    try:
        db_df = fetch_df_active("SELECT DB_NAME() AS DB_NAME", None)
        db_name = str(db_df.iloc[0]["DB_NAME"]) if db_df is not None and not db_df.empty else "Unknown"
        st.caption(f"DB: {db_name}")
    except Exception as e:
        st.warning(f"Could not read DB_NAME(): {e}")
        db_name = "Unknown"

    def _safe_count(obj: str) -> Optional[int]:
        try:
            df = fetch_df_active(f"SELECT COUNT(*) AS N FROM {obj}", None)
            return int(df.iloc[0]["N"]) if df is not None and not df.empty else 0
        except Exception:
            return None

    base_tables = [
        "TEAM_HEADCOUNT_HISTORY",
        "TEAM_RATE_HISTORY",
        "ADO_FEATURES",
        "ADO_ITERATION_CALENDAR",
    ]
    view_objects = [
        "VW_TEAM_HEADCOUNT_EFFECTIVE",
        "VW_TEAM_WEIGHTED_RATES",
        "VW_TCO_FEATURE_DEMAND",
        "VW_TCO_WORKFORCE_SPLIT",
    ]
    base_rows = [{"object": t, "rows": _safe_count(t)} for t in base_tables]
    view_rows = [{"object": v, "rows": _safe_count(v)} for v in view_objects]
    st.caption("Base tables (current DB)")
    st.dataframe(pd.DataFrame(base_rows), use_container_width=True)
    st.caption("Key views (current DB)")
    st.dataframe(pd.DataFrame(view_rows), use_container_width=True)

    base_has_rows = any((r.get("rows") or 0) > 0 for r in base_rows if r.get("rows") is not None)
    view_zero = any((r.get("rows") or 0) == 0 for r in view_rows if r.get("rows") is not None)
    if base_has_rows and view_zero:
        st.warning("View definitions may still reference another DB; check for NEXT_LOCAL prefixes.")

_init_mod, _init_err = safe_import("Admin page initialization", lambda: __import__("core.init", fromlist=["init_page"]))
if _init_err is not None:
    st.warning("Admin theme/init helpers are unavailable. Rendering with safe defaults.")
    st.caption(str(_init_err))
else:
    page_theme = _init_mod.init_page("Admin", page_path=__file__)
    _page_theme = page_theme
user = st.session_state.get("auth_user") or {}
role = str(
    user.get("role", "")
    or st.session_state.get("role")
    or st.session_state.get("user_role")
    or ""
).upper()
if role != "ADMIN":
    st.error("Admin access required.")
    st.stop()

# Admin-only data version badge (portfolio DB only)
ctx = get_portfolio_ctx()
active_portfolio_key = str(get_active_portfolio_key() or "").strip()
try:
    control_available_now = bool(control_db_available())
except Exception:
    control_available_now = False
if ctx is not None:
    try:
        db_df = fetch_df_active("SELECT DB_NAME() AS DB_NAME", None)
        db_name = str(db_df.iloc[0]["DB_NAME"]) if db_df is not None and not db_df.empty else "Unknown"
    except Exception:
        db_name = "Unknown"
    try:
        dv_info = get_data_version_info()
    except Exception:
        dv_info = {}
    dv_val = dv_info.get("version", 0)
    dv_at = dv_info.get("updated_at")
else:
    db_name = "Unavailable"
    dv_val = "Unavailable"
    dv_at = "Unavailable"
st.caption(
    f"Admin diagnostics • Active portfolio: {active_portfolio_key or 'None'} • "
    f"Control DB: {'Yes' if control_available_now else 'No'} • "
    f"DB: {db_name} • DATA_VERSION: {dv_val} • UPDATED_AT: {dv_at}"
)

def _display_name_for(email: str) -> str:
    em = (email or "").strip()
    if not em:
        return "Unknown"
    try:
        df = fetch_df_active("SELECT TOP 1 DISPLAY_NAME FROM APP_USERS WHERE UPPER(EMAIL)=UPPER(%s)", (em,))
        if df is not None and not df.empty:
            dn = str(df.iloc[0].get("DISPLAY_NAME") or "").strip()
            if dn:
                return dn
    except Exception:
        pass
    return em

def _display_name_map() -> Dict[str, str]:
    try:
        df = fetch_df_active("SELECT EMAIL, DISPLAY_NAME FROM APP_USERS")
        if df is None or df.empty:
            return {}
        return {str(r["EMAIL"]).strip().lower(): str(r.get("DISPLAY_NAME") or "").strip() or str(r["EMAIL"]).strip()
                for _, r in df.iterrows()}
    except Exception:
        return {}

# --- Back-compat helpers ---
def delete_program(program_id: str) -> Dict[str, Any]:
    return safe_delete_program(program_id, source="pages/99_Admin.py")

def delete_team(team_id: str) -> Dict[str, Any]:
    return safe_delete_team(team_id, source="pages/99_Admin.py")

def delete_vendor(vendor_id: str) -> None:
    try:
        execute_active("UPDATE APPLICATION_GROUPS SET DEFAULT_VENDORID=NULL WHERE DEFAULT_VENDORID=%s", (vendor_id,))
    except Exception:
        pass
    try:
        execute_active("UPDATE APPLICATIONS SET VENDORID=NULL WHERE VENDORID=%s", (vendor_id,))
    except Exception:
        pass
    execute_active("DELETE FROM VENDORS WHERE VENDORID=%s", (vendor_id,))

def delete_application(application_id: str) -> Dict[str, Any]:
    return safe_delete_app_instance(application_id, source="pages/99_Admin.py")


# Friendly DB error helper
def _safe_fetch(query: str, params: Optional[Sequence] = None) -> pd.DataFrame:
    try:
        return fetch_df_active(query, params)
    except Exception as e:
        msg = str(e)
        if "Login timeout expired" in msg or "HYT00" in msg:
            st.error("Database connection timed out (Azure SQL may be paused). Please refresh/re-login to wake the database.")
            st.stop()
        raise

# Do not mutate schema on page load. Initialization is explicit from Admin actions.
if "_admin_init_done" not in st.session_state:
    st.session_state["_admin_init_done"] = True
    st.session_state["_tco_init"] = True

# ----------------------------
# Local helpers for rollover
# ----------------------------
# Note: MSSQL backend uses configured DB/schema; use `_fq` where needed.

def rollover_year(from_year: int, to_year: int, created_by: Optional[str] = None) -> Tuple[str, int]:
    batch_id = str(uuid.uuid4())
    year_delta = int(to_year) - int(from_year)
    insert_sql = f"""
        INSERT INTO { _fq('INVOICES') } (
            INVOICEID, APPLICATIONID, TEAMID,
            INVOICEDATE, RENEWALDATE, AMOUNT, STATUS, FISCAL_YEAR,
            PRODUCT_OWNER, AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE,
            COMPANY_CODE, COST_CENTER, SERIAL_NUMBER, WORK_ORDER, AGREEMENT_NUMBER,
            CONTRACT_DUE, SERVICE_TYPE, NOTES,
            GROUPID, PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING, GROUPID_AT_BOOKING,
            ROLLOVER_BATCH_ID, ROLLED_OVER_FROM_YEAR,
            INVOICE_TYPE
        )
        SELECT
            CONVERT(NVARCHAR(36), NEWID()), i.APPLICATIONID, i.TEAMID,
            NULL,
            CASE WHEN i.RENEWALDATE IS NOT NULL THEN DATEADD(year, {year_delta}, i.RENEWALDATE) ELSE NULL END,
            COALESCE(i.AMOUNT_NEXT_YEAR, i.AMOUNT),
            'Planned',
            %s,
            i.PRODUCT_OWNER,
            i.AMOUNT_NEXT_YEAR,
            COALESCE(i.CONTRACT_ACTIVE, 1),
            i.COMPANY_CODE, i.COST_CENTER, i.SERIAL_NUMBER, i.WORK_ORDER, i.AGREEMENT_NUMBER,
            i.CONTRACT_DUE, i.SERVICE_TYPE, i.NOTES,
            i.GROUPID, i.PROGRAMID_AT_BOOKING, i.VENDORID_AT_BOOKING, i.GROUPID_AT_BOOKING,
            %s, %s,
            'Recurring Invoice'
        FROM { _fq('INVOICES') } i
        WHERE i.FISCAL_YEAR = %s
          AND COALESCE(i.INVOICE_TYPE, 'Recurring Invoice') = 'Recurring Invoice'
    """
    execute_active(insert_sql, (int(to_year), batch_id, int(from_year), int(from_year)))
    cnt_df = fetch_df_active(f"SELECT COUNT(*) AS CNT FROM { _fq('INVOICES') } WHERE ROLLOVER_BATCH_ID = %s", (batch_id,))
    inserted = int(cnt_df.iloc[0]["CNT"]) if not cnt_df.empty else 0
    log_sql = f"""
        INSERT INTO { _fq('ROLLOVER_LOG') } (BATCH_ID, FROM_YEAR, TO_YEAR, ROWS_INSERTED, CREATED_BY)
        VALUES (%s, %s, %s, %s, %s)
    """
    execute_active(log_sql, (batch_id, int(from_year), int(to_year), inserted, created_by))
    return batch_id, inserted

def list_rollovers():
    return fetch_df_active(f"""
        SELECT BATCH_ID, FROM_YEAR, TO_YEAR, ROWS_INSERTED, CREATED_AT, CREATED_BY
        FROM { _fq('ROLLOVER_LOG') }
        ORDER BY CREATED_AT DESC
    """)

def rollback_rollover(batch_id: str) -> int:
    cnt_df = fetch_df_active(f"SELECT COUNT(*) AS CNT FROM { _fq('INVOICES') } WHERE ROLLOVER_BATCH_ID = %s", (batch_id,))
    to_delete = int(cnt_df.iloc[0]["CNT"]) if not cnt_df.empty else 0
    execute_active(f"DELETE FROM { _fq('INVOICES') } WHERE ROLLOVER_BATCH_ID = %s", (batch_id,))
    execute_active(f"DELETE FROM { _fq('ROLLOVER_LOG') } WHERE BATCH_ID = %s", (batch_id,))
    return to_delete

# ----------------------------
# Robust cascade helpers (used by UI)
# ----------------------------
def _delete_team_cascade(team_id: str) -> Tuple[int, int, int]:
    inv_count_df = fetch_df_active(f"""
        SELECT COUNT(*) CNT
        FROM { _fq('INVOICES') }
        WHERE TEAMID = %s
           OR APPLICATIONID IN (
                SELECT a.APPLICATIONID
                FROM { _fq('APPLICATIONS') } a
                JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = a.GROUPID
                WHERE g.TEAMID = %s
           )
    """, (team_id, team_id))
    inv_count = int(inv_count_df.iloc[0]["CNT"]) if inv_count_df is not None and not inv_count_df.empty else 0
    execute_active(f"""
        DELETE FROM { _fq('INVOICES') }
        WHERE TEAMID = %s
           OR APPLICATIONID IN (
                SELECT a.APPLICATIONID
                FROM { _fq('APPLICATIONS') } a
                JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = a.GROUPID
                WHERE g.TEAMID = %s
           )
    """, (team_id, team_id))

    app_count_df = fetch_df_active(f"""
        SELECT COUNT(*) CNT
        FROM { _fq('APPLICATIONS') } a
        WHERE a.GROUPID IN (SELECT g.GROUPID FROM { _fq('APPLICATION_GROUPS') } g WHERE g.TEAMID = %s)
    """, (team_id,))
    app_count = int(app_count_df.iloc[0]["CNT"]) if app_count_df is not None and not app_count_df.empty else 0
    execute_active(f"""
        DELETE FROM { _fq('APPLICATIONS') }
        WHERE GROUPID IN (SELECT g.GROUPID FROM { _fq('APPLICATION_GROUPS') } g WHERE g.TEAMID = %s)
    """, (team_id,))

    grp_count_df = fetch_df_active(f"SELECT COUNT(*) CNT FROM { _fq('APPLICATION_GROUPS') } WHERE TEAMID = %s", (team_id,))
    grp_count = int(grp_count_df.iloc[0]["CNT"]) if grp_count_df is not None and not grp_count_df.empty else 0
    execute_active(f"DELETE FROM { _fq('APPLICATION_GROUPS') } WHERE TEAMID = %s", (team_id,))
    execute_active(f"DELETE FROM { _fq('TEAMS') } WHERE TEAMID = %s", (team_id,))
    return inv_count, app_count, grp_count

def _delete_program_cascade(program_id: str) -> Tuple[int, int, int, int]:
    teams = fetch_df_active(f"SELECT TEAMID FROM { _fq('TEAMS') } WHERE PROGRAMID = %s", (program_id,))
    if teams is None or teams.empty:
        execute_active(f"DELETE FROM { _fq('PROGRAMS') } WHERE PROGRAMID = %s", (program_id,))
        return (0, 0, 0, 0)
    t_count = 0; inv_total = 0; app_total = 0; grp_total = 0
    for _, tr in teams.iterrows():
        team_id = tr["TEAMID"]
        inv_c, app_c, grp_c = _delete_team_cascade(team_id)
        inv_total += inv_c; app_total += app_c; grp_total += grp_c; t_count += 1
    execute_active(f"DELETE FROM { _fq('PROGRAMS') } WHERE PROGRAMID = %s", (program_id,))
    return (t_count, inv_total, app_total, grp_total)

# ----------------------------
# Finders / maps (by name)
# ----------------------------
def _choices(table: str, name_col: str) -> List[str]:
    df = _safe_fetch(f"SELECT {name_col} FROM { _fq(table) } ORDER BY {name_col}")
    if df.empty:
        return []
    return [str(x) for x in df[name_col].dropna().tolist()]

def _find_one(table: str, name_col: str, id_col: str, name_val: str):
    q = f"SELECT TOP 1 * FROM { _fq(table) } WHERE UPPER({name_col}) = UPPER(%s)"
    df = _safe_fetch(q, (name_val,))
    return None if df.empty else df.iloc[0].to_dict()

def _map_name_to_id(table: str, name_col: str, id_col: str) -> Dict[str, str]:
    df = _safe_fetch(f"SELECT {id_col}, {name_col} FROM { _fq(table) }")
    if df.empty:
        return {}
    return {str(r[name_col]).strip(): str(r[id_col]).strip() for _, r in df.iterrows()
            if pd.notna(r[name_col]) and pd.notna(r[id_col])}

def _teams_under_program(program_id: str):
    return _safe_fetch(f"SELECT TEAMID, TEAMNAME FROM { _fq('TEAMS') } WHERE PROGRAMID = %s ORDER BY TEAMNAME", (program_id,))

def _groups_under_team(team_id: str):
    return _safe_fetch(f"SELECT GROUPID, GROUPNAME FROM { _fq('APPLICATION_GROUPS') } WHERE TEAMID = %s ORDER BY GROUPNAME", (team_id,))

def _apps_under_group(group_id: str):
    return _safe_fetch(f"SELECT APPLICATIONID, APPLICATIONNAME FROM { _fq('APPLICATIONS') } WHERE GROUPID = %s ORDER BY APPLICATIONNAME", (group_id,))

def _invoices_for_team_or_children(team_id: str):
    return fetch_df_active(f"""
        SELECT i.INVOICEID, i.FISCAL_YEAR, i.RENEWALDATE, i.AMOUNT, i.STATUS, COALESCE(i.INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE
        FROM { _fq('INVOICES') } i
        WHERE i.TEAMID = %s
           OR i.APPLICATIONID IN (
                SELECT a.APPLICATIONID FROM { _fq('APPLICATIONS') } a
                JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = a.GROUPID
                WHERE g.TEAMID = %s
           )
        ORDER BY COALESCE(i.RENEWALDATE, i.INVOICEDATE) DESC
    """, (team_id, team_id))


def _to_int_opt(x) -> Optional[int]:
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
        return int(float(s))
    except Exception:
        return None


def _months_until_contract_due_start(year: int) -> Optional[int]:
    try:
        today = dt.date.today()
        return (int(year) - today.year) * 12 + (1 - today.month)
    except Exception:
        return None


def _classify_tracking(df: pd.DataFrame, due_soon_days: int, contract_months: int) -> pd.DataFrame:
    if df.empty:
        return df

    today = dt.date.today()

    def _days_to(d):
        try:
            if pd.isna(d):
                return None
            return (pd.to_datetime(d).date() - today).days
        except Exception:
            return None

    out = df.copy()
    out["Days to Renewal"] = out["Renewal Date"].apply(_days_to)

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

def _invoices_for_app(application_id: str):
    return fetch_df_active(f"""
        SELECT INVOICEID, FISCAL_YEAR, RENEWALDATE, AMOUNT, STATUS, COALESCE(INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE
        FROM { _fq('INVOICES') } WHERE APPLICATIONID = %s ORDER BY COALESCE(RENEWALDATE, INVOICEDATE) DESC
    """, (application_id,))

def _orphan_apps_for_vendor(vendor_id: str):
    return fetch_df_active(f"""
        SELECT a.APPLICATIONID, a.APPLICATIONNAME
        FROM { _fq('APPLICATIONS') } a
        LEFT JOIN { _fq('INVOICES') } i ON i.APPLICATIONID = a.APPLICATIONID
        WHERE a.VENDORID = %s AND i.INVOICEID IS NULL
        ORDER BY a.APPLICATIONNAME
    """, (vendor_id,))

def _new_uuid() -> str:
    return str(uuid.uuid4())

def _to_bool(val: Any) -> Optional[bool]:
    if pd.isna(val):
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("true", "t", "yes", "y", "1"):
        return True
    if s in ("false", "f", "no", "n", "0"):
        return False
    return None

def _to_float(val: Any) -> Optional[float]:
    if pd.isna(val) or val is None or str(val).strip() == "":
        return None
    try:
        return float(val)
    except Exception:
        try:
            return float(str(val).replace(",", "."))
        except Exception:
            return None

def _to_int(val: Any) -> Optional[int]:
    if pd.isna(val) or val is None or str(val).strip() == "":
        return None
    try:
        return int(val)
    except Exception:
        try:
            return int(float(str(val).replace(",", ".")))
        except Exception:
            return None

def _to_date(val: Any) -> Optional[dt.date]:
    if val is None or (isinstance(val, float) and pd.isna(val)) or (isinstance(val, str) and not val.strip()):
        return None
    if isinstance(val, dt.date) and not isinstance(val, dt.datetime):
        return val
    if isinstance(val, (pd.Timestamp, dt.datetime)):
        return val.date()
    try:
        # Accept common formats
        return pd.to_datetime(val).date()
    except Exception:
        return None



def _read_only_schema_status(cfg: MssqlConfig) -> Dict[str, Any]:
    """Read-only checks only; no DDL/mutations."""
    key_tables = [
        "PROGRAMS",
        "TEAMS",
        "APPLICATION_GROUPS",
        "APPLICATIONS",
        "INVOICES",
        "APP_USERS",
        "APP_USER_MEMBERSHIP",
    ]
    key_views = [
        "VW_TEAM_WEIGHTED_RATES",
        "VW_TCO_FEATURE_DEMAND",
        "VW_TCO_WORKFORCE_SPLIT",
    ]
    out: Dict[str, Any] = {"tables": {}, "views": {}}
    schema = str(cfg.schema or "dbo").strip() or "dbo"
    for t in key_tables:
        try:
            df = fetch_df(
                """
                SELECT COUNT(*) AS N
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (schema, t),
                cfg=cfg,
            )
            out["tables"][t] = bool(int(df.iloc[0]["N"])) if df is not None and not df.empty else False
        except Exception:
            out["tables"][t] = False
    for v in key_views:
        try:
            df = fetch_df(
                """
                SELECT COUNT(*) AS N
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (schema, v),
                cfg=cfg,
            )
            out["views"][v] = bool(int(df.iloc[0]["N"])) if df is not None and not df.empty else False
        except Exception:
            out["views"][v] = False
    return out


def render_portfolios_tab() -> None:
    render_section("Portfolio Registry", "Control DB registry and onboarding.")
    available = control_db_available()
    if not available:
        st.warning("Control DB config not found in secrets/env.")
    else:
        st.success("Control DB config detected.")

    try:
        schema_status = ensure_control_schema()
    except Exception as e:
        schema_status = {"ok": False, "error": str(e), "missing": []}

    user_email = st.session_state.get("auth_user", {}).get("email") or st.session_state.get("user_email") or "ADMIN"
    is_global_admin = is_control_global_admin(user_email) if available else False

    with st.container(border=True):
        st.markdown("### Control DB Setup")
        status_line = "Available" if available else "Missing config"
        st.write(f"Control DB status: {status_line}")
        if schema_status.get("ok"):
            st.write("Schema: Ready")
        else:
            missing = schema_status.get("missing") or []
            st.write("Schema: Missing tables")
            if missing:
                st.caption("Missing: " + ", ".join(missing))
        if (not schema_status.get("ok")) or is_global_admin:
            if st.button("Create Control DB schema", key="btn_control_schema_bootstrap", disabled=not available):
                try:
                    create_control_schema()
                    try:
                        df_cnt = control_fetch_df("SELECT COUNT(*) AS N FROM CONTROL_GLOBAL_ADMINS")
                        n = int(df_cnt.iloc[0]["N"]) if df_cnt is not None and not df_cnt.empty else 0
                    except Exception:
                        n = 0
                    if n == 0 and user_email:
                        control_execute(
                            "INSERT INTO CONTROL_GLOBAL_ADMINS (USER_EMAIL) VALUES (%s)",
                            (user_email,),
                        )
                    st.success("Control DB bootstrapped.")
                except Exception as e:
                    st.error(f"Failed to bootstrap Control DB: {e}")

    df_ports = pd.DataFrame()
    df_users = pd.DataFrame()
    if schema_status.get("ok"):
        try:
            df_ports = control_fetch_df("SELECT * FROM CONTROL_PORTFOLIOS ORDER BY PORTFOLIO_KEY")
        except Exception:
            df_ports = pd.DataFrame()
        try:
            df_users = control_fetch_df("SELECT * FROM CONTROL_PORTFOLIO_USERS ORDER BY USER_EMAIL, PORTFOLIO_KEY")
        except Exception:
            df_users = pd.DataFrame()

    st.subheader("Control DB Status")
    if not df_ports.empty:
        df_show = df_ports.copy()
        if "DB_PASSWORD" in df_show.columns:
            df_show["DB_PASSWORD"] = None
        if "DB_PASSWORD_KEY" in df_show.columns:
            df_show["DB_PASSWORD_KEY"] = df_show["DB_PASSWORD_KEY"].fillna("").astype(str).str.strip()
        st.dataframe(df_show, use_container_width=True)
    else:
        st.caption("No CONTROL_PORTFOLIOS rows.")
        if not available:
            st.info("Configure Control DB first, then bootstrap schema, then register portfolios.")
        else:
            st.info("No portfolios registered yet. Add your first portfolio below.")
    if not df_users.empty:
        st.dataframe(df_users, use_container_width=True)
    else:
        st.caption("No CONTROL_PORTFOLIO_USERS rows.")

    st.subheader("Onboard New Portfolio (Wizard)")
    st.caption("Follow the steps below to register a portfolio and initialize its database.")
    st.info("This is the portfolio’s own DB (NOT the Control DB).")

    # Step 1 — Identity
    st.markdown("#### Step 1 — Portfolio Identity")
    display_name = st.text_input(
        "Display name",
        key="ob_display_name",
        help="User-facing label. Example: Portfolio – MOIT",
    )
    suggested_key = ""
    if display_name:
        suggested_key = (
            "".join([c if c.isalnum() or c in "-_" else "-" for c in display_name.lower()])
            .replace("--", "-")
            .strip("-")
        )
    pending_key = st.session_state.pop("ob_portfolio_key_pending", None)
    if pending_key:
        st.session_state["ob_portfolio_key"] = pending_key
    portfolio_key = st.text_input(
        "Portfolio key",
        key="ob_portfolio_key",
        help=(
            "Internal stable slug. Lowercase, no spaces. Example: moit, deepwater-sbd. "
            "Don’t change after creation."
        ),
    )
    if suggested_key and not portfolio_key:
        st.caption(f"Suggested key: `{suggested_key}`")
        if st.button("Use suggested key", key="btn_use_suggested_key"):
            st.session_state["ob_portfolio_key_pending"] = suggested_key
            st.rerun()

    description = st.text_area(
        "Description (optional)",
        key="ob_description",
        help="Short summary of this portfolio.",
    )

    key_valid = False
    if portfolio_key:
        import re
        key_valid = bool(re.match(r"^[a-z0-9][a-z0-9_-]{1,40}$", portfolio_key))
        if not key_valid:
            st.error("PORTFOLIO_KEY must match: ^[a-z0-9][a-z0-9_-]{1,40}$")

    # Step 2 — DB Connection
    st.markdown("#### Step 2 — Portfolio Database Connection")
    with st.expander("Advanced DB settings", expanded=False):
        db_server = st.text_input("DB server", key="ob_db_server", help="Example: tcp:myserver.database.windows.net")
        db_database = st.text_input("DB database", key="ob_db_database", help="Example: NEXT_MOIT")
        db_schema = st.text_input("DB schema", value="dbo", key="ob_db_schema", help="Default: dbo")
        db_user = st.text_input("DB user", key="ob_db_user", help="DB login user")
        db_password_key = st.text_input(
            "DB password key",
            key="ob_db_password_key",
            help="Env var name used to resolve password. Example: PORTFOLIO_DB_PASSWORD_MOIT",
        )
        db_driver = st.text_input(
            "DB driver",
            value="ODBC Driver 18 for SQL Server",
            key="ob_db_driver",
            help="Default: ODBC Driver 18 for SQL Server",
        )
        db_encrypt = st.checkbox("Encrypt", value=True, key="ob_db_encrypt")
        db_trust = st.checkbox("Trust server certificate", value=False, key="ob_db_trust")

    # Step 3 — ADO Profile
    st.markdown("#### Step 3 — ADO Profile (structure template)")
    try:
        from core.ado_profile import _load_profiles_config  # type: ignore
        profiles_cfg = _load_profiles_config() or {}
    except Exception:
        profiles_cfg = {}
    profile_keys = sorted([k for k in profiles_cfg.keys()])
    profile_key = st.selectbox(
        "ADO profile",
        options=profile_keys or [""],
        index=0,
        help="Select the ADO profile template used for area/iteration mappings.",
    )
    if profile_key and profile_key in profiles_cfg:
        prof = profiles_cfg.get(profile_key) or {}
        area_map = prof.get("area_mapping") if isinstance(prof.get("area_mapping"), dict) else {}
        notes = prof.get("notes") or prof.get("note") or ""
        st.caption(
            f"Area mapping: program={area_map.get('program_level','')}, team={area_map.get('team_level','')}, "
            f"subteam={area_map.get('subteam_level','')}."
        )
        if notes:
            st.caption(f"Notes: {notes}")

    st.markdown("**Next:** open Settings → ADO inside this portfolio to configure org/project and PAT.")
    store_ado_now = st.checkbox("Store ADO connection now (optional)", value=False, key="ob_store_ado_now")
    ado_org_url = ""
    ado_project = ""
    pat_env_key = ""
    if store_ado_now:
        ado_org_url = st.text_input("ADO org URL", key="ob_ado_org_url", help="Example: https://dev.azure.com/<org>")
        ado_project = st.text_input("ADO project", key="ob_ado_project")
        pat_env_key = st.text_input("PAT env key", key="ob_pat_env_key", help="Example: ADO_PAT_MOIT")

    def _build_onboard_cfg() -> Tuple[MssqlConfig, str]:
        pw_key = (db_password_key or "").strip() or f"PORTFOLIO_DB_PASSWORD_{portfolio_key.upper()}"
        resolved_pw = resolve_portfolio_db_password(pw_key, portfolio_key, profile_key) or ""
        if not resolved_pw:
            raise RuntimeError(f"Missing DB password (env key: {pw_key}).")
        return (
            MssqlConfig(
                server=db_server,
                database=db_database,
                user=str(db_user or ""),
                password=str(resolved_pw or ""),
                driver=db_driver or "ODBC Driver 18 for SQL Server",
                encrypt="yes" if db_encrypt else "no",
                trust_server_certificate="yes" if db_trust else "no",
                schema=db_schema or "dbo",
            ),
            pw_key,
        )

    st.markdown("### A) Register portfolio")
    if st.button("Register portfolio", key="ob_register", disabled=not is_global_admin):
        try:
            if not key_valid:
                st.error("Fix Portfolio key before registering.")
            elif not display_name:
                st.error("Display name is required.")
            else:
                # Optional columns are added only on explicit register action.
                for col_name, col_type in (
                    ("DESCRIPTION", "NVARCHAR(512) NULL"),
                    ("DB_PASSWORD_KEY", "NVARCHAR(128) NULL"),
                    ("DB_DRIVER", "NVARCHAR(128) NULL"),
                    ("DB_ENCRYPT", "BIT NULL"),
                    ("DB_TRUST_SERVER_CERTIFICATE", "BIT NULL"),
                ):
                    control_execute(
                        f"IF COL_LENGTH('CONTROL_PORTFOLIOS','{col_name}') IS NULL ALTER TABLE CONTROL_PORTFOLIOS ADD {col_name} {col_type};"
                    )
                pw_key_store = (db_password_key or "").strip() or f"PORTFOLIO_DB_PASSWORD_{portfolio_key.upper()}"
                control_execute(
                    """
                    MERGE CONTROL_PORTFOLIOS AS tgt
                    USING (SELECT %s AS PORTFOLIO_KEY) AS src
                    ON tgt.PORTFOLIO_KEY = src.PORTFOLIO_KEY
                    WHEN MATCHED THEN UPDATE SET
                      DISPLAY_NAME = %s,
                      DESCRIPTION = %s,
                      DB_SERVER = %s,
                      DB_DATABASE = %s,
                      DB_USER = %s,
                      DB_PASSWORD_KEY = %s,
                      DB_DRIVER = %s,
                      DB_ENCRYPT = %s,
                      DB_TRUST_SERVER_CERTIFICATE = %s,
                      DB_SCHEMA = %s,
                      PROFILE_KEY = %s,
                      ADO_ORG_URL = %s,
                      ADO_PROJECT = %s,
                      PAT_ENV_KEY = %s,
                      STATUS = %s,
                      UPDATED_AT = SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN
                      INSERT (PORTFOLIO_KEY, DISPLAY_NAME, DESCRIPTION, DB_SERVER, DB_DATABASE, DB_USER, DB_PASSWORD_KEY, DB_DRIVER,
                              DB_ENCRYPT, DB_TRUST_SERVER_CERTIFICATE, DB_SCHEMA, PROFILE_KEY, ADO_ORG_URL, ADO_PROJECT, PAT_ENV_KEY, STATUS)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        portfolio_key,
                        display_name,
                        description or None,
                        db_server,
                        db_database,
                        db_user,
                        pw_key_store,
                        db_driver or "ODBC Driver 18 for SQL Server",
                        1 if db_encrypt else 0,
                        1 if db_trust else 0,
                        db_schema or "dbo",
                        profile_key,
                        ado_org_url or None,
                        ado_project or None,
                        pat_env_key or None,
                        "ONBOARDING",
                        portfolio_key,
                        display_name,
                        description or None,
                        db_server,
                        db_database,
                        db_user,
                        pw_key_store,
                        db_driver or "ODBC Driver 18 for SQL Server",
                        1 if db_encrypt else 0,
                        1 if db_trust else 0,
                        db_schema or "dbo",
                        profile_key,
                        ado_org_url or None,
                        ado_project or None,
                        pat_env_key or None,
                        "ONBOARDING",
                    ),
                )
                st.success("Portfolio registered in Control DB.")
        except Exception as e:
            st.error(f"Register failed: {e}")

    st.markdown("### B) Test connection")
    st.caption("Read-only checks only. No writes are performed.")
    if st.button("Test connection", key="ob_test_connection"):
        try:
            cfg, _pw_key = _build_onboard_cfg()
            df_ok = fetch_df("SELECT 1 AS OK, DB_NAME() AS DB_NAME", cfg=cfg)
            db_name = str(df_ok.iloc[0].get("DB_NAME") or cfg.database) if df_ok is not None and not df_ok.empty else cfg.database
            st.success(f"Connection successful: {db_name}")
            status = _read_only_schema_status(cfg)
            st.json(status)
        except Exception as e:
            st.error(f"Connection failed: {e}")

    st.markdown("### C) Initialize schema (advanced / dangerous)")
    with st.expander("Initialize schema controls", expanded=False):
        st.warning("Writes happen only when you explicitly execute with confirmation.")
        init_confirm = st.checkbox(
            "I understand this can create/alter objects in the portfolio database.",
            value=False,
            key="ob_bootstrap_confirm",
        )
        init_mode = st.radio(
            "Initialization mode",
            ["Dry-run (preview changes)", "Execute changes"],
            index=0,
            key="ob_bootstrap_mode",
        )
        if st.button(
            "Run initialization",
            key="ob_bootstrap_run",
            disabled=(init_mode == "Execute changes" and not init_confirm),
            type="primary",
        ):
            try:
                cfg, _pw_key = _build_onboard_cfg()
                admin_email = st.session_state.get("auth_user", {}).get("email") or "ADMIN"
                dry = init_mode == "Dry-run (preview changes)"
                result = bootstrap_portfolio_db(cfg, profile_key, admin_email, dry_run=dry)
                if dry:
                    st.info("Dry-run completed. No changes were applied.")
                else:
                    st.success(f"Schema initialization executed ({result.get('executed', 0)} action(s)).")
                summary = result.get("summary") or {}
                if summary:
                    st.write("Summary:", summary)
                warnings = result.get("warnings") or []
                for w in warnings:
                    st.warning(w)
                actions = result.get("actions") or []
                if actions:
                    st.markdown("#### Planned actions")
                    df_actions = pd.DataFrame(
                        [{"kind": a.get("kind"), "name": a.get("name")} for a in actions]
                    )
                    st.dataframe(df_actions, use_container_width=True)
                    for i, a in enumerate(actions):
                        with st.expander(f"SQL {i+1}: {a.get('kind')} • {a.get('name')}", expanded=False):
                            st.code(str(a.get("sql") or "").strip(), language="sql")
                if not dry:
                    st.markdown("#### Post-run read-only schema status")
                    st.json(_read_only_schema_status(cfg))
            except Exception as e:
                st.error(f"Bootstrap failed: {e}")

    st.subheader("Edit Existing Portfolio")
    if df_ports.empty:
        st.caption("No portfolios to edit yet.")
    else:
        edit_key = st.selectbox("Portfolio", df_ports["PORTFOLIO_KEY"].astype(str).tolist(), key="edit_portfolio_key")
        edit_row = df_ports[df_ports["PORTFOLIO_KEY"].astype(str) == str(edit_key)].iloc[0]
        st.warning("Changing PORTFOLIO_KEY is not allowed.")
        edit_display = st.text_input("Display name", value=str(edit_row.get("DISPLAY_NAME") or ""), key="edit_display")
        edit_desc = st.text_area("Description", value=str(edit_row.get("DESCRIPTION") or ""), key="edit_desc")
        edit_status = st.selectbox("Status", ["ACTIVE", "ONBOARDING", "DISABLED"], index=0, key="edit_status")
        st.markdown("**Connection**")
        edit_server = st.text_input("DB server", value=str(edit_row.get("DB_SERVER") or ""), key="edit_db_server")
        edit_database = st.text_input("DB database", value=str(edit_row.get("DB_DATABASE") or ""), key="edit_db_database")
        edit_user = st.text_input("DB user", value=str(edit_row.get("DB_USER") or ""), key="edit_db_user")
        edit_schema = st.text_input("DB schema", value=str(edit_row.get("DB_SCHEMA") or "dbo"), key="edit_db_schema")
        edit_driver = st.text_input(
            "DB driver",
            value=str(edit_row.get("DB_DRIVER") or "ODBC Driver 18 for SQL Server"),
            key="edit_db_driver",
        )
        edit_encrypt = st.checkbox(
            "Encrypt",
            value=bool(edit_row.get("DB_ENCRYPT", True)) if edit_row.get("DB_ENCRYPT") is not None else True,
            key="edit_db_encrypt",
        )
        edit_trust = st.checkbox(
            "Trust server certificate",
            value=bool(edit_row.get("DB_TRUST_SERVER_CERTIFICATE", False))
            if edit_row.get("DB_TRUST_SERVER_CERTIFICATE") is not None
            else False,
            key="edit_db_trust",
        )
        edit_password_key = st.text_input(
            "DB password key",
            value=str(edit_row.get("DB_PASSWORD_KEY") or ""),
            key="edit_db_password_key",
            help="Env var name used to resolve password (e.g., PORTFOLIO_DB_PASSWORD_MOIT).",
        )

        col_e1, col_e2 = st.columns(2)
        with col_e1:
            if st.button("Save changes", key="btn_edit_portfolio", disabled=not is_global_admin):
                try:
                    control_execute(
                        """
                        UPDATE CONTROL_PORTFOLIOS
                        SET DISPLAY_NAME = %s,
                            DESCRIPTION = %s,
                            STATUS = %s,
                            DB_SERVER = %s,
                            DB_DATABASE = %s,
                            DB_USER = %s,
                            DB_PASSWORD_KEY = %s,
                            DB_DRIVER = %s,
                            DB_ENCRYPT = %s,
                            DB_TRUST_SERVER_CERTIFICATE = %s,
                            DB_SCHEMA = %s,
                            UPDATED_AT = SYSUTCDATETIME()
                        WHERE PORTFOLIO_KEY = %s
                        """,
                        (
                            edit_display,
                            edit_desc or None,
                            edit_status,
                            edit_server,
                            edit_database,
                            edit_user,
                            (edit_password_key or "").strip() or f"PORTFOLIO_DB_PASSWORD_{str(edit_key).upper()}",
                            edit_driver or "ODBC Driver 18 for SQL Server",
                            1 if edit_encrypt else 0,
                            1 if edit_trust else 0,
                            edit_schema or "dbo",
                            edit_key,
                        ),
                    )
                    st.success("Portfolio updated.")
                except Exception as e:
                    st.error(f"Update failed: {e}")
        with col_e2:
            if st.button("Test connection", key="btn_edit_portfolio_test"):
                try:
                    pw_key = (edit_password_key or "").strip() or f"PORTFOLIO_DB_PASSWORD_{str(edit_key).upper()}"
                    resolved_pw = resolve_portfolio_db_password(pw_key, edit_key, str(edit_row.get("PROFILE_KEY") or ""))
                    if not resolved_pw:
                        raise RuntimeError(f"Missing DB password (env key: {pw_key}).")
                    cfg = MssqlConfig(
                        server=edit_server,
                        database=edit_database,
                        user=edit_user,
                        password=str(resolved_pw or ""),
                        driver=edit_driver or "ODBC Driver 18 for SQL Server",
                        encrypt="yes" if edit_encrypt else "no",
                        trust_server_certificate="yes" if edit_trust else "no",
                        schema=edit_schema or "dbo",
                    )
                    fetch_df("SELECT 1 AS OK", cfg=cfg)
                    st.success("Connection successful.")
                except Exception as e:
                    st.error(
                        "Connection failed. "
                        f"Server={edit_server} DB={edit_database} User={edit_user}. Error={e}"
                    )

    st.subheader("Assign Portfolio Admins")
    if not is_global_admin:
        st.info("Only Control DB global admins can assign portfolio users.")
    admin_emails = st.text_input("Admin emails (comma-separated)", key="ob_admin_emails")
    if st.button("Assign Admins", key="ob_assign_admins", disabled=not is_global_admin):
        emails = [e.strip() for e in (admin_emails or "").split(",") if e.strip()]
        if not emails or not portfolio_key:
            st.error("Enter at least one email and a portfolio key.")
        else:
            try:
                for e in emails:
                    control_execute(
                        """
                        MERGE CONTROL_PORTFOLIO_USERS AS tgt
                        USING (SELECT %s AS USER_EMAIL, %s AS PORTFOLIO_KEY) AS src
                        ON tgt.USER_EMAIL = src.USER_EMAIL AND tgt.PORTFOLIO_KEY = src.PORTFOLIO_KEY
                        WHEN MATCHED THEN UPDATE SET ROLE = 'ADMIN'
                        WHEN NOT MATCHED THEN
                          INSERT (USER_EMAIL, PORTFOLIO_KEY, ROLE) VALUES (%s, %s, 'ADMIN');
                        """,
                        (e, portfolio_key, e, portfolio_key),
                    )
                st.success("Admin(s) assigned.")
            except Exception as e:
                st.error(f"Assign failed: {e}")

# ----------------------------
# UI
# ----------------------------
render_admin_header()
render_context_bar()

active_key = get_active_portfolio_key()
if not active_key:
    st.info("Select a portfolio to manage portfolio tables.")
    tab_portfolios, tab_visual_lab = st.tabs(["Portfolios", "Visual Lab"])
    with tab_portfolios:
        render_portfolios_tab()
    with tab_visual_lab:
        try:
            import importlib
            from viz import legacy_visual_lab_theme as _visual_lab

            importlib.reload(_visual_lab)
        except Exception as e:
            st.error(f"Visual Lab failed to load: {e}")
    st.stop()

tab_home, tab_browse_manage, tab_bulkedit, tab_sql, tab_cleanup, tab_access, tab_backup, tab_portfolios, tab_ado_profile, tab_changelog, tab_email, tab_schema, tab_visual_lab = st.tabs(
    [
        "Admin Home",
        "Browse & Manage",
        "Bulk Edit",
        "Raw SQL",
        "Clean Up",
        "Access Control",
        "Backup & Restore",
        "Portfolios",
        "ADO Profile Extractor",
        "Change Logs",
        "Email Alerts",
        "Schema & Dependencies",
        "Visual Lab",
    ]
)

with tab_home:
    render_section("Admin Home", "Quick setup and diagnostics for the active portfolio.")
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        if st.button("Ensure core tables", key="admin_home_ensure_tables"):
            try:
                _ensure_tables()
                st.success("Core tables ensured.")
            except Exception as e:
                st.error(f"Core table init failed: {e}")
    with col_b:
        if st.button("Ensure access tables", key="admin_home_ensure_access_tables"):
            try:
                ensure_access_control_tables()
                st.success("Access tables ensured.")
            except Exception as e:
                st.error(f"Access table init failed: {e}")
    with col_c:
        if st.button("Ensure ADO tables", key="admin_home_ensure_ado_tables"):
            try:
                ensure_ado_minimal_tables()
                st.success("ADO tables ensured.")
            except Exception as e:
                st.error(f"ADO table init failed: {e}")

    st.markdown("#### Entity diagnostics")
    try:
        diag_df = get_registry_diagnostics()
        st.dataframe(diag_df, use_container_width=True)
    except Exception as e:
        st.warning(f"Diagnostics unavailable: {e}")

    st.markdown("#### Page permissions")
    try:
        st.dataframe(permissions_matrix_df(), use_container_width=True)
    except Exception as e:
        st.warning(f"Permissions matrix unavailable: {e}")

with tab_browse_manage:
    _browse_mod, _browse_err = safe_import(
        "Browse & Manage",
        lambda: __import__("utils.admin_browse_manage", fromlist=["render_browse_manage_tab"]),
    )
    if _browse_err is not None:
        _render_missing_dependency("Browse & Manage", _browse_err)
    else:
        _browse_mod.render_browse_manage_tab()

with tab_access:
    render_section("Access Control", "Manage app users and roles.")
    try:
        ensure_access_control_tables()
    except Exception as e:
        st.error(f"Access control tables not available: {e}")
    df_users = None
    try:
        df_users = list_app_users()
    except Exception as e:
        st.error(f"Could not load users: {e}")
    if df_users is not None:
        st.dataframe(df_users, use_container_width=True)

    def _save_access_user(email: str, display: str, role_val: str, active_val: bool) -> bool:
        try:
            upsert_app_user(
                email=(email or "").strip().lower(),
                display_name=(display or "").strip()[:200] or None,
                role=role_val,
                is_active=active_val,
                provider=None,
            )
            log_admin_action(
                action_type="upsert",
                area="access_control",
                entity="APP_USERS",
                summary=f"Upsert user {(email or '').strip().lower()}",
                extra_json={"email": (email or "").strip().lower(), "role": role_val, "active": active_val},
            )
            return True
        except Exception as ex:
            st.error(f"Failed to save user: {ex}")
            return False

    with st.expander("Azure AD search (optional)", expanded=False):
        tenant_id = str(os.getenv("AZUREAD_TENANT_ID", "") or "").strip()
        client_id = str(os.getenv("AZUREAD_CLIENT_ID", "") or "").strip()
        if not tenant_id or not client_id:
            st.info(
                "Azure AD search not configured. Set AZUREAD_TENANT_ID and "
                "AZUREAD_CLIENT_ID to enable directory lookup."
            )
        else:
            azure_ad_mod, azure_ad_err = safe_import(
                "Azure AD helpers",
                lambda: __import__(
                    "utils.azure_ad",
                    fromlist=["acquire_graph_token_device", "search_users_basic"],
                ),
            )
            if azure_ad_err is not None:
                st.info("Azure AD search unavailable in this environment (missing dependency).")
            else:
                acquire_graph_token_device_local = azure_ad_mod.acquire_graph_token_device
                search_users_basic_local = azure_ad_mod.search_users_basic
                now_ts = time.time()
                token_obj = st.session_state.get("graph_token") or {}
                expires_at = float(st.session_state.get("graph_expires_at") or 0.0)
                token_active = bool(token_obj.get("access_token")) and (expires_at <= 0 or now_ts < expires_at)

                col_g1, col_g2 = st.columns([2, 1])
                with col_g1:
                    if st.button("Sign in to Azure AD (device code)", key="admin_access_graph_signin"):
                        try:
                            token_result = acquire_graph_token_device_local(client_id, tenant_id)
                            if token_result and token_result.get("access_token"):
                                st.session_state["graph_token"] = token_result
                                expires_in = token_result.get("expires_in")
                                if expires_in is not None:
                                    try:
                                        st.session_state["graph_expires_at"] = time.time() + float(expires_in) - 60.0
                                    except Exception:
                                        st.session_state["graph_expires_at"] = 0.0
                                else:
                                    st.session_state["graph_expires_at"] = 0.0
                                st.success("Signed in to Azure AD.")
                            else:
                                st.warning("Azure AD sign-in did not return an access token.")
                        except Exception as ex:
                            st.error(f"Azure AD sign-in failed: {ex}")
                with col_g2:
                    if st.button("Sign out", key="admin_access_graph_signout"):
                        st.session_state.pop("graph_token", None)
                        st.session_state.pop("graph_expires_at", None)
                        st.session_state.pop("admin_access_graph_results", None)
                        st.session_state.pop("admin_access_graph_last_query", None)
                        st.success("Signed out from Azure AD session.")

                token_obj = st.session_state.get("graph_token") or {}
                expires_at = float(st.session_state.get("graph_expires_at") or 0.0)
                token_active = bool(token_obj.get("access_token")) and (expires_at <= 0 or time.time() < expires_at)
                if token_active:
                    st.success("Azure AD session active.")
                else:
                    st.warning("Not signed in.")

                search_q = st.text_input("Search user (name or email)", key="admin_access_graph_query").strip()
                if token_active and len(search_q) >= 3:
                    last_q = str(st.session_state.get("admin_access_graph_last_query") or "")
                    if search_q != last_q:
                        try:
                            rows = search_users_basic_local(token_obj["access_token"], search_q, top=10) or []
                            st.session_state["admin_access_graph_results"] = rows
                            st.session_state["admin_access_graph_last_query"] = search_q
                        except Exception as ex:
                            st.error(f"Azure AD search failed: {ex}")
                            st.session_state["admin_access_graph_results"] = []
                            st.session_state["admin_access_graph_last_query"] = search_q
                elif token_active and search_q and len(search_q) < 3:
                    st.caption("Type at least 3 characters to search.")

                graph_results = st.session_state.get("admin_access_graph_results") or []
                if graph_results:
                    df_graph = pd.DataFrame(graph_results)
                    show_cols = [c for c in ["displayName", "mail", "userPrincipalName", "id"] if c in df_graph.columns]
                    if show_cols:
                        st.dataframe(df_graph[show_cols], use_container_width=True)

                    def _graph_label(r: Dict[str, Any]) -> str:
                        dn = str(r.get("displayName") or "").strip() or "(no name)"
                        em = str(r.get("mail") or "").strip()
                        upn = str(r.get("userPrincipalName") or "").strip()
                        return f"{dn} — {em or upn or '(no email/upn)'}"

                    idx = st.selectbox(
                        "Pick a user",
                        options=list(range(len(graph_results))),
                        format_func=lambda i: _graph_label(graph_results[i]),
                        key="admin_access_graph_pick_idx",
                    )
                    add_now = st.checkbox("Add user immediately after selecting", value=False, key="admin_access_graph_add_now")
                    if st.button("Use this user", key="admin_access_graph_use"):
                        picked = graph_results[int(idx)]
                        picked_email = str(picked.get("mail") or picked.get("userPrincipalName") or "").strip().lower()
                        picked_name = str(picked.get("displayName") or "").strip()[:200]
                        if not picked_email:
                            st.error("Selected user has no mail/userPrincipalName.")
                        else:
                            st.session_state["admin_access_email"] = picked_email
                            st.session_state["admin_access_display"] = picked_name
                            st.success("Manual form prefilled from Azure AD selection.")
                            if add_now:
                                role_now = str(st.session_state.get("admin_access_role") or "CONTRIBUTOR")
                                active_now = bool(st.session_state.get("admin_access_active", True))
                                if _save_access_user(picked_email, picked_name, role_now, active_now):
                                    st.success("User added.")
                                    st.rerun()

    with st.form("admin_access_user_form"):
        col_u1, col_u2 = st.columns(2)
        with col_u1:
            user_email = st.text_input("Email", key="admin_access_email").strip().lower()
            display_name = st.text_input("Display Name", key="admin_access_display")
        with col_u2:
            role_pick = st.selectbox("Role", ["VIEWER", "CONTRIBUTOR", "ADMIN"], index=1, key="admin_access_role")
            is_active = st.checkbox("Active", value=True, key="admin_access_active")
        submitted = st.form_submit_button("Save user", type="primary")
    if submitted:
        if not user_email:
            st.error("Email is required.")
        else:
            if _save_access_user(user_email, display_name, role_pick, is_active):
                st.success("User saved.")

with tab_backup:
    _backup_mod, _backup_err = safe_import(
        "Backup & Restore",
        lambda: __import__("utils.admin_backup_restore", fromlist=["render_backup_restore_tab"]),
    )
    if _backup_err is not None:
        _render_missing_dependency("Backup & Restore", _backup_err)
    else:
        _backup_mod.render_backup_restore_tab()

with tab_portfolios:
    render_portfolios_tab()

with tab_ado_profile:
    _ado_mod, _ado_err = safe_import(
        "ADO Profile Extractor",
        lambda: __import__(
            "utils.ado_profile_extractor",
            fromlist=[
                "build_ado_base_url",
                "build_wiql",
                "query_work_item_ids",
                "fetch_work_items_batch",
                "build_iterations_profile",
                "build_summary",
                "export_profile_files",
                "get_valid_field_refs",
                "list_secret_portfolios",
                "get_secret_pat",
            ],
        ),
    )
    if _ado_err is not None:
        _render_missing_dependency("ADO Profile Extractor", _ado_err)
        build_ado_base_url = lambda *args, **kwargs: ("", "")  # type: ignore
        build_wiql = lambda *args, **kwargs: ""  # type: ignore
        query_work_item_ids = lambda *args, **kwargs: ([], "")  # type: ignore
        fetch_work_items_batch = lambda *args, **kwargs: pd.DataFrame()  # type: ignore
        build_iterations_profile = lambda *args, **kwargs: pd.DataFrame()  # type: ignore
        build_summary = lambda *args, **kwargs: {}  # type: ignore
        export_profile_files = lambda *args, **kwargs: {}  # type: ignore
        get_valid_field_refs = lambda *args, **kwargs: []  # type: ignore
        list_secret_portfolios = lambda *args, **kwargs: []  # type: ignore
        get_secret_pat = lambda *args, **kwargs: ""  # type: ignore
    else:
        build_ado_base_url = _ado_mod.build_ado_base_url
        build_wiql = _ado_mod.build_wiql
        query_work_item_ids = _ado_mod.query_work_item_ids
        fetch_work_items_batch = _ado_mod.fetch_work_items_batch
        build_iterations_profile = _ado_mod.build_iterations_profile
        build_summary = _ado_mod.build_summary
        export_profile_files = _ado_mod.export_profile_files
        get_valid_field_refs = _ado_mod.get_valid_field_refs
        list_secret_portfolios = _ado_mod.list_secret_portfolios
        get_secret_pat = _ado_mod.get_secret_pat

    render_section("ADO Profile Extractor", "Read-only sampling from Azure DevOps to define portfolio profiles.")
    st.caption(
        "Use this tool to pull a small, safe sample of work items directly from ADO. "
        "No DB writes, no schema changes."
    )

    with st.form("ado_profile_extractor_form"):
        col_a, col_b = st.columns(2)
        with col_a:
            org_url = st.text_input(
                "Organization URL",
                placeholder="https://dev.azure.com/<org>",
                help="Base ADO org URL. You can also provide org name below.",
            )
            project = st.text_input("Project name", placeholder="ADO Project")
            org_override = st.text_input(
                "Org / collection name (optional)",
                placeholder="org name (if not included in URL)",
            )
        with col_b:
            auth_source = st.selectbox(
                "Auth source",
                ["Paste PAT for this run", "Use saved PAT from secrets"],
                index=0,
            )
            pat_input = ""
            saved_key = ""
            if auth_source == "Use saved PAT from secrets":
                saved_keys = list_secret_portfolios(st.secrets)
                if not saved_keys:
                    st.warning("No [ado_portfolios] keys found in secrets.")
                if saved_keys:
                    saved_key = st.selectbox("Saved portfolio key", saved_keys, index=0)
                    pat_input = get_secret_pat(st.secrets, saved_key) if saved_key else ""
            else:
                pat_input = st.text_input("Personal Access Token (PAT)", type="password")
            portfolio_label = st.text_input("Portfolio label (for filenames)", placeholder="Friendly name")
            application_field_ref = st.text_input(
                "Application field reference (optional)",
                value="Custom.ApplicationName",
                placeholder="Custom.ApplicationName or your custom field ref",
                help="If set, the extractor will request this field and use it as Application.",
            )

        st.markdown("**Sampling controls**")
        col_c, col_d = st.columns(2)
        with col_c:
            sample_size = int(
                st.number_input("Sample size", min_value=50, max_value=2000, value=300, step=50)
            )
            work_item_types = st.multiselect(
                "Work item types",
                ["Feature", "Epic", "User Story", "Bug"],
                default=["Feature"],
            )
            include_story_points = st.checkbox("Include Story Points field", value=True)
            include_effort = st.checkbox("Include Effort field", value=True)
        with col_d:
            states = st.multiselect(
                "States to include (blank = all)",
                ["New", "Active", "In Progress", "Resolved", "Closed", "Done", "Removed"],
                default=[],
            )
            include_iterations = st.checkbox("Include Iteration Calendar metadata", value=True)
            include_path_dist = st.checkbox("Also export unique Area/Iteration paths distribution", value=True)

        submitted = st.form_submit_button("Run extractor", type="primary")

    if submitted:
        if not project:
            st.error("Project name is required.")
        elif not pat_input:
            st.error("PAT is required to connect to Azure DevOps.")
        else:
            st.session_state["ado_last_wiql"] = ""
            st.session_state["ado_last_wiql_url"] = ""
            st.session_state["ado_last_fields"] = []
            status = st.empty()
            try:
                status.info("Resolving organization...")
                base_url, org = build_ado_base_url(org_url, org_override)
                if not base_url:
                    raise RuntimeError("Organization URL could not be resolved.")
                if "dev.azure.com" in base_url and not org:
                    raise RuntimeError("Organization name is required (URL missing org segment).")
                if not portfolio_label:
                    portfolio_label = project

                status.info("Running WIQL query...")
                ids, wiql = query_work_item_ids(
                    base_url=base_url,
                    project=project,
                    pat=pat_input,
                    sample_size=sample_size,
                    work_item_types=work_item_types,
                    states=states,
                )
                st.session_state["ado_last_wiql"] = wiql
                status.info(f"Fetched {len(ids)} work item IDs. Downloading details...")

                status.info("Fetching valid field references...")
                valid_fields = get_valid_field_refs(base_url, pat_input)
                requested_fields = [
                    "System.Id",
                    "System.Title",
                    "System.State",
                    "System.WorkItemType",
                    "System.AreaPath",
                    "System.IterationPath",
                    "System.ChangedDate",
                ]
                if include_story_points:
                    requested_fields.append("Microsoft.VSTS.Scheduling.StoryPoints")
                if include_effort:
                    requested_fields.append("Microsoft.VSTS.Scheduling.Effort")
                if application_field_ref:
                    requested_fields.append(application_field_ref)
                fields_to_request = [f for f in requested_fields if f in valid_fields]
                dropped = [f for f in requested_fields if f not in valid_fields]
                st.session_state["ado_last_fields"] = fields_to_request
                if dropped:
                    st.warning(
                        "Some requested fields were not available and were skipped: "
                        + ", ".join(sorted(set(dropped)))
                    )

                df_features = fetch_work_items_batch(
                    base_url=base_url,
                    project=project,
                    pat=pat_input,
                    ids=ids,
                    fields_to_request=fields_to_request,
                    application_field_ref=application_field_ref,
                )
                dup_mask = pd.Index(df_features.columns).duplicated()
                if dup_mask.any():
                    dup_names = pd.Index(df_features.columns)[dup_mask].tolist()
                    st.warning(
                        "Duplicate columns were renamed: "
                        + ", ".join([f"{name} -> {name}__2" for name in dup_names[:10]])
                    )
                status.info("Building iteration profile...")
                df_iterations = build_iterations_profile(df_features) if include_iterations else pd.DataFrame()
                summary = build_summary(df_features)
                summary.update(
                    {
                        "org": org or "",
                        "project": project,
                        "portfolio_label": portfolio_label,
                        "requested_sample_size": int(sample_size),
                    }
                )

                path_dist_df = pd.DataFrame()
                if include_path_dist and not df_features.empty:
                    area_dist = (
                        df_features.get("AreaPath", pd.Series(dtype=str))
                        .fillna("")
                        .loc[lambda s: s != ""]
                        .value_counts()
                        .rename_axis("Path")
                        .reset_index(name="count")
                    )
                    area_dist["PathType"] = "AreaPath"
                    iter_dist = (
                        df_features.get("IterationPath", pd.Series(dtype=str))
                        .fillna("")
                        .loc[lambda s: s != ""]
                        .value_counts()
                        .rename_axis("Path")
                        .reset_index(name="count")
                    )
                    iter_dist["PathType"] = "IterationPath"
                    path_dist_df = pd.concat([area_dist, iter_dist], ignore_index=True)
                    summary["path_distribution_top"] = (
                        path_dist_df.sort_values("count", ascending=False).head(50).to_dict(orient="records")
                    )

                paths = export_profile_files(
                    df_features,
                    df_iterations,
                    summary,
                    portfolio_label=portfolio_label,
                )
                if include_path_dist and not path_dist_df.empty:
                    dist_path = os.path.join(
                        "exports/ado_profile_extractor",
                        f"{re.sub(r'[^a-zA-Z0-9_-]+', '_', portfolio_label.strip()) or 'ado_profile'}"
                        f"__profile_path_distributions__{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    )
                    path_dist_df.to_csv(dist_path, index=False)
                    paths["path_distribution_csv"] = dist_path

                status.success("Extractor finished.")
                st.subheader("Summary")
                st.json(summary)

                st.subheader("Downloads")
                for label, path in (
                    ("Sample features CSV", paths.get("features_csv", "")),
                    ("Sample iterations CSV", paths.get("iterations_csv", "")),
                    ("Summary JSON", paths.get("summary_json", "")),
                    ("Path distributions CSV", paths.get("path_distribution_csv", "")),
                ):
                    if path:
                        try:
                            with open(path, "rb") as fh:
                                data = fh.read()
                            st.download_button(label, data, file_name=os.path.basename(path))
                            st.caption(path)
                        except Exception as e:
                            st.warning(f"Could not load {label}: {e}")

                st.subheader("Sample rows")
                st.dataframe(df_features.head(200), use_container_width=True)
                if include_iterations:
                    st.dataframe(df_iterations.head(200), use_container_width=True)
            except Exception as e:
                status.error(f"Extractor failed: {e}")
                last_wiql = st.session_state.get("ado_last_wiql") or ""
                last_url = st.session_state.get("ado_last_wiql_url") or ""
                last_fields = st.session_state.get("ado_last_fields") or []
                if last_wiql or last_url or last_fields:
                    with st.expander("Debug – WIQL sent", expanded=False):
                        if last_wiql:
                            st.code(last_wiql, language="sql")
                        if last_url:
                            st.code(last_url, language="text")
                        if last_fields:
                            st.code(", ".join(last_fields), language="text")

    last_wiql = st.session_state.get("ado_last_wiql") or ""
    last_url = st.session_state.get("ado_last_wiql_url") or ""
    last_fields = st.session_state.get("ado_last_fields") or []
    if last_wiql or last_url or last_fields:
        with st.expander("Debug – WIQL sent", expanded=False):
            if last_wiql:
                st.code(last_wiql, language="sql")
            if last_url:
                st.code(last_url, language="text")
            if last_fields:
                st.code(", ".join(last_fields), language="text")

with tab_changelog:
    render_section("Change Logs (audit)", "Recently updated rows by table. Shows last updater and timestamp.")
    choices = {
        "Programs": """
            SELECT TOP 500 PROGRAMNAME AS Program, PROGRAMOWNER AS Owner,
                   UPDATED_AT AS [Updated At],
                   COALESCE(
                     NULLIF(LTRIM(RTRIM(au.DISPLAY_NAME)), ''),
                     NULLIF(LTRIM(RTRIM(PROGRAMS.UPDATED_BY)), ''),
                     'Unknown'
                   ) AS [Updated By]
            FROM PROGRAMS
            OUTER APPLY (
              SELECT TOP 1 DISPLAY_NAME
              FROM APP_USERS au
              WHERE UPPER(LTRIM(RTRIM(au.EMAIL))) = UPPER(LTRIM(RTRIM(PROGRAMS.UPDATED_BY)))
                 OR UPPER(LTRIM(RTRIM(au.DISPLAY_NAME))) = UPPER(LTRIM(RTRIM(PROGRAMS.UPDATED_BY)))
            ) au
            ORDER BY UPDATED_AT DESC
        """,
        "Teams": """
            SELECT TOP 500 t.TEAMNAME AS Team, p.PROGRAMNAME AS Program, t.PRODUCTOWNER AS ProductOwner,
                           t.UPDATED_AT AS [Updated At],
                           COALESCE(
                             NULLIF(LTRIM(RTRIM(au.DISPLAY_NAME)), ''),
                             NULLIF(LTRIM(RTRIM(t.UPDATED_BY)), ''),
                             NULLIF(LTRIM(RTRIM(h.UPDATED_BY)), ''),
                             'Unknown'
                           ) AS [Updated By]
            FROM TEAMS t
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
            OUTER APPLY (
              SELECT TOP 1 DISPLAY_NAME
              FROM APP_USERS au
              WHERE UPPER(LTRIM(RTRIM(au.EMAIL))) = UPPER(LTRIM(RTRIM(t.UPDATED_BY)))
                 OR UPPER(LTRIM(RTRIM(au.DISPLAY_NAME))) = UPPER(LTRIM(RTRIM(t.UPDATED_BY)))
            ) au
            OUTER APPLY (
              SELECT TOP 1 UPDATED_BY
              FROM TEAM_COMPOSITION_HISTORY h
              WHERE h.TEAMID = t.TEAMID
                AND UPDATED_BY IS NOT NULL
                AND LTRIM(RTRIM(UPDATED_BY)) <> ''
              ORDER BY UPDATED_AT DESC
            ) h
            ORDER BY t.UPDATED_AT DESC
        """,
        "Vendors": """
            SELECT TOP 500 VENDORNAME AS Vendor,
                           UPDATED_AT AS [Updated At],
                           COALESCE(
                             NULLIF(LTRIM(RTRIM(au.DISPLAY_NAME)), ''),
                             NULLIF(LTRIM(RTRIM(VENDORS.UPDATED_BY)), ''),
                             'Unknown'
                           ) AS [Updated By]
            FROM VENDORS
            OUTER APPLY (
              SELECT TOP 1 DISPLAY_NAME
              FROM APP_USERS au
              WHERE UPPER(LTRIM(RTRIM(au.EMAIL))) = UPPER(LTRIM(RTRIM(VENDORS.UPDATED_BY)))
                 OR UPPER(LTRIM(RTRIM(au.DISPLAY_NAME))) = UPPER(LTRIM(RTRIM(VENDORS.UPDATED_BY)))
            ) au
            ORDER BY UPDATED_AT DESC
        """,
        "Application Groups": """
            SELECT TOP 500 g.GROUPNAME AS [Group], t.TEAMNAME AS Team, p.PROGRAMNAME AS Program, v.VENDORNAME AS Vendor,
                           g.UPDATED_AT AS [Updated At],
                           COALESCE(
                             NULLIF(LTRIM(RTRIM(au.DISPLAY_NAME)), ''),
                             NULLIF(LTRIM(RTRIM(g.UPDATED_BY)), ''),
                             'Unknown'
                           ) AS [Updated By]
            FROM APPLICATION_GROUPS g
            LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = COALESCE(g.PROGRAMID, t.PROGRAMID)
            LEFT JOIN VENDORS v ON v.VENDORID = g.DEFAULT_VENDORID
            OUTER APPLY (
              SELECT TOP 1 DISPLAY_NAME
              FROM APP_USERS au
              WHERE UPPER(LTRIM(RTRIM(au.EMAIL))) = UPPER(LTRIM(RTRIM(g.UPDATED_BY)))
                 OR UPPER(LTRIM(RTRIM(au.DISPLAY_NAME))) = UPPER(LTRIM(RTRIM(g.UPDATED_BY)))
            ) au
            ORDER BY g.UPDATED_AT DESC
        """,
        "Applications": """
            SELECT TOP 500 a.APPLICATIONNAME AS Application, g.GROUPNAME AS [Group], t.TEAMNAME AS Team, v.VENDORNAME AS Vendor,
                           a.UPDATED_AT AS [Updated At],
                           COALESCE(
                             NULLIF(LTRIM(RTRIM(au.DISPLAY_NAME)), ''),
                             NULLIF(LTRIM(RTRIM(a.UPDATED_BY)), ''),
                             'Unknown'
                           ) AS [Updated By]
            FROM APPLICATIONS a
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
            LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
            LEFT JOIN VENDORS v ON v.VENDORID = COALESCE(a.VENDORID, g.DEFAULT_VENDORID)
            OUTER APPLY (
              SELECT TOP 1 DISPLAY_NAME
              FROM APP_USERS au
              WHERE UPPER(LTRIM(RTRIM(au.EMAIL))) = UPPER(LTRIM(RTRIM(a.UPDATED_BY)))
                 OR UPPER(LTRIM(RTRIM(au.DISPLAY_NAME))) = UPPER(LTRIM(RTRIM(a.UPDATED_BY)))
            ) au
            ORDER BY a.UPDATED_AT DESC
        """,
        "Contracts": """
            SELECT TOP 500
                c.CONTRACT_ID AS ContractID,
                app.APPLICATIONNAME AS Application,
                t.TEAMNAME AS Team,
                c.START_FY AS StartFY,
                c.END_FY AS EndFY,
                c.RENEWAL_MONTH AS RenewalMonth,
                c.ANNUAL_AMOUNT AS AnnualAmount,
                c.STATUS AS Status,
                c.UPDATED_AT AS [Updated At],
                COALESCE(
                  NULLIF(LTRIM(RTRIM(au.DISPLAY_NAME)), ''),
                  NULLIF(LTRIM(RTRIM(c.UPDATED_BY)), ''),
                  'Unknown'
                ) AS [Updated By]
            FROM CONTRACTS c
            LEFT JOIN APPLICATIONS app ON app.APPLICATIONID = c.APPLICATIONID
            LEFT JOIN TEAMS t ON t.TEAMID = c.TEAMID
            OUTER APPLY (
              SELECT TOP 1 DISPLAY_NAME
              FROM APP_USERS au
              WHERE UPPER(LTRIM(RTRIM(au.EMAIL))) = UPPER(LTRIM(RTRIM(c.UPDATED_BY)))
                 OR UPPER(LTRIM(RTRIM(au.DISPLAY_NAME))) = UPPER(LTRIM(RTRIM(c.UPDATED_BY)))
            ) au
            ORDER BY c.UPDATED_AT DESC
        """,
        "Invoices": """
            SELECT TOP 500 a.APPLICATIONNAME AS Application, t.TEAMNAME AS Team, i.FISCAL_YEAR AS [Year], i.STATUS AS Status,
                           i.UPDATED_AT AS [Updated At],
                           COALESCE(
                             NULLIF(LTRIM(RTRIM(au.DISPLAY_NAME)), ''),
                             NULLIF(LTRIM(RTRIM(i.UPDATED_BY)), ''),
                             'Unknown'
                           ) AS [Updated By]
            FROM INVOICES i
            LEFT JOIN APPLICATIONS a ON a.APPLICATIONID = i.APPLICATIONID
            LEFT JOIN TEAMS t ON t.TEAMID = i.TEAMID
            OUTER APPLY (
              SELECT TOP 1 DISPLAY_NAME
              FROM APP_USERS au
              WHERE UPPER(LTRIM(RTRIM(au.EMAIL))) = UPPER(LTRIM(RTRIM(i.UPDATED_BY)))
                 OR UPPER(LTRIM(RTRIM(au.DISPLAY_NAME))) = UPPER(LTRIM(RTRIM(i.UPDATED_BY)))
            ) au
            ORDER BY i.UPDATED_AT DESC
        """,
        "Team MSP Rate": """
            SELECT TOP 500 t.TEAMNAME AS Team, m.MSP_ENABLED, m.MSP_SIZE, m.MSP_RATE_PER_PI,
                           m.UPDATED_AT AS [Updated At],
                           COALESCE(
                             NULLIF(LTRIM(RTRIM(au.DISPLAY_NAME)), ''),
                             NULLIF(LTRIM(RTRIM(m.UPDATED_BY)), ''),
                             'Unknown'
                           ) AS [Updated By]
            FROM TEAM_MSP_RATE m
            LEFT JOIN TEAMS t ON t.TEAMID = m.TEAMID
            OUTER APPLY (
              SELECT TOP 1 DISPLAY_NAME
              FROM APP_USERS au
              WHERE UPPER(LTRIM(RTRIM(au.EMAIL))) = UPPER(LTRIM(RTRIM(m.UPDATED_BY)))
                 OR UPPER(LTRIM(RTRIM(au.DISPLAY_NAME))) = UPPER(LTRIM(RTRIM(m.UPDATED_BY)))
            ) au
            ORDER BY m.UPDATED_AT DESC
        """,
    }
    pick = st.selectbox("Dataset", list(choices.keys()), index=0)
    try:
        df_log = fetch_df_active(choices[pick])
        if df_log is None or df_log.empty:
            st.info("No rows to show.")
        else:
            st.dataframe(df_log, use_container_width=True)
    except Exception as e:
        st.error(f"Could not load logs: {e}")

    render_audit_panel(area="change_logs")

with tab_bulkedit:
    # Developer notes: to add a new entity editor, define key/required/numeric/editable cols,
    # load the dataframe, then use render_preview_apply_audit_block with preview/apply fns.
    render_section("Bulk Edit", "Fast, consistent bulk editors with preview and audit.")

    bulk_entities = [e["id"] for e in list_entities_for("bulk_edit")]
    tab_labels = bulk_entities or ["Programs", "Teams", "Vendors", "Application Groups", "Applications", "Invoices", "Rates"]
    tabs = st.tabs(tab_labels)
    tab_map = {label: tab for label, tab in zip(tab_labels, tabs)}

    def _render_summary(summary: dict) -> None:
        cols = st.columns(4)
        cols[0].metric("Invalid", summary.get("invalid", 0))
        cols[1].metric("New", summary.get("new", 0))
        cols[2].metric("Updated", summary.get("updated", 0))
        cols[3].metric("Unchanged", summary.get("unchanged", 0))

    def _bulk_available(entity_id: str) -> bool:
        ent = get_entity_by_id(entity_id)
        tables = ent.get("tables") if ent else []
        missing = [t for t in (tables or []) if not table_exists(t)]
        if missing:
            render_empty_state(
                "Unavailable: table not found",
                f"Missing tables: {', '.join(missing)}",
                tips=["Run schema initialization in Admin Home.", "Check database connectivity."],
            )
            return False
        return True

    def _apply_ctx(df: pd.DataFrame, entity_id: str) -> pd.DataFrame:
        return apply_admin_context_filters(df, get_admin_context(), entity_id)

    def _maybe_cap_df(df: pd.DataFrame, key: str, label: str, limit: Optional[int] = None) -> pd.DataFrame:
        ctx = get_admin_context()
        effective_limit = ctx.get("row_limit") if limit is None else limit
        if effective_limit is None:
            return df
        if df is None or df.empty or len(df) <= effective_limit:
            return df
        flag_key = f"{key}_load_all"
        if st.session_state.get(flag_key):
            return df
        st.warning(f"{label} has {len(df)} rows. Showing first {effective_limit}.")
        if st.button("Load all rows", key=f"{key}_load_all_btn"):
            st.session_state[flag_key] = True
        return df.head(int(effective_limit))

    @st.cache_data(ttl=90)
    def _load_bulk_programs(cache_bust: int = 0) -> pd.DataFrame:
        try:
            return fetch_df_active(f"""
                SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE, PROGRAM_XOM_RATE
                FROM { _fq('PROGRAMS') }
                ORDER BY PROGRAMNAME
            """)
        except Exception:
            return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE", "PROGRAM_XOM_RATE"])

    @st.cache_data(ttl=90)
    def _load_bulk_teams(cache_bust: int = 0) -> pd.DataFrame:
        try:
            return fetch_df_active(f"""
                SELECT t.TEAMID, t.TEAMNAME, t.PROGRAMID, p.PROGRAMNAME, t.PRODUCTOWNER,
                       t.TEAMFTE, t.DELIVERY_TEAM_FTE, t.CONTRACTOR_C_FTE, t.CONTRACTOR_CS_FTE
                FROM { _fq('TEAMS') } t
                LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = t.PROGRAMID
                ORDER BY t.TEAMNAME
            """)
        except Exception:
            return pd.DataFrame(columns=[
                "TEAMID", "TEAMNAME", "PROGRAMID", "PROGRAMNAME", "PRODUCTOWNER",
                "TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE",
            ])

    @st.cache_data(ttl=90)
    def _load_bulk_vendors(cache_bust: int = 0) -> pd.DataFrame:
        try:
            return fetch_df_active(f"""
                SELECT VENDORID, VENDORNAME
                FROM { _fq('VENDORS') }
                ORDER BY VENDORNAME
            """)
        except Exception:
            return pd.DataFrame(columns=["VENDORID", "VENDORNAME"])

    @st.cache_data(ttl=90)
    def _load_bulk_groups(cache_bust: int = 0) -> pd.DataFrame:
        try:
            return fetch_df_active(f"""
                SELECT g.GROUPID, g.GROUPNAME, g.TEAMID, t.TEAMNAME,
                       g.DEFAULT_VENDORID AS VENDORID, v.VENDORNAME, g.OWNER,
                       TRY_CONVERT(BIT, JSON_VALUE((SELECT g.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), '$.IS_BASE')) AS IS_BASE
                FROM { _fq('APPLICATION_GROUPS') } g
                LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = g.TEAMID
                LEFT JOIN { _fq('VENDORS') } v ON v.VENDORID = g.DEFAULT_VENDORID
                ORDER BY g.GROUPNAME
            """)
        except Exception:
            return pd.DataFrame(columns=["GROUPID", "GROUPNAME", "TEAMNAME", "VENDORNAME", "OWNER", "IS_BASE"])

    @st.cache_data(ttl=90)
    def _load_bulk_apps(cache_bust: int = 0) -> pd.DataFrame:
        try:
            return fetch_df_active(f"""
                SELECT a.APPLICATIONID, a.APPLICATIONNAME, a.ADD_INFO,
                       a.GROUPID, g.GROUPNAME,
                       COALESCE(a.VENDORID, g.DEFAULT_VENDORID) AS VENDORID,
                       v.VENDORNAME
                FROM { _fq('APPLICATIONS') } a
                LEFT JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = a.GROUPID
                LEFT JOIN { _fq('VENDORS') } v ON v.VENDORID = COALESCE(a.VENDORID, g.DEFAULT_VENDORID)
                ORDER BY a.APPLICATIONNAME
            """)
        except Exception:
            return pd.DataFrame(columns=["APPLICATIONID", "APPLICATIONNAME", "GROUPNAME", "ADD_INFO", "VENDORNAME"])

    @st.cache_data(ttl=90)
    def _load_bulk_invoices(cache_bust: int = 0) -> pd.DataFrame:
        try:
            return fetch_df_active(f"""
                SELECT INVOICEID, APPLICATIONID, TEAMID, FISCAL_YEAR, INVOICEDATE, RENEWALDATE,
                       AMOUNT, STATUS, COALESCE(INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE
                FROM { _fq('INVOICES') }
            """)
        except Exception:
            return pd.DataFrame(columns=["INVOICEID", "APPLICATIONID", "TEAMID", "FISCAL_YEAR", "INVOICEDATE", "RENEWALDATE", "AMOUNT", "STATUS", "INVOICE_TYPE"])

    @st.cache_data(ttl=90)
    def _load_team_rates(cache_bust: int = 0) -> pd.DataFrame:
        try:
            return fetch_df_active(f"""
                SELECT TEAMID, YEAR, PI, LOCATION, XOM_RATE
                FROM { _fq('TEAM_RATE_HISTORY') }
                ORDER BY TEAMID, YEAR, PI, LOCATION
            """)
        except Exception:
            return pd.DataFrame(columns=["TEAMID", "YEAR", "PI", "LOCATION", "XOM_RATE"])

    @st.cache_data(ttl=90)
    def _load_program_rates(cache_bust: int = 0) -> pd.DataFrame:
        try:
            return fetch_df_active(f"""
                SELECT PROGRAMID, YEAR, PI, LOCATION, PROGRAM_XOM_RATE
                FROM { _fq('PROGRAM_RATE_HISTORY') }
                ORDER BY PROGRAMID, YEAR, PI, LOCATION
            """)
        except Exception:
            return pd.DataFrame(columns=["PROGRAMID", "YEAR", "PI", "LOCATION", "PROGRAM_XOM_RATE"])

    def _bulk_state(entity: str, original_df: pd.DataFrame, edited_df: pd.DataFrame, *,
                    key_cols: list[str], editable_cols: list[str], required_cols: list[str], numeric_cols: list[str]):
        from utils.admin_bulk_edit import normalize_editor_df, validate_bulk_df, compute_change_summary
        config = {
            "key_cols": key_cols,
            "required_cols": required_cols,
            "numeric_cols": numeric_cols,
        }
        norm_df = normalize_editor_df(edited_df)
        valid_df, val_summary = validate_bulk_df(entity, norm_df, config)
        action_df, change_summary = compute_change_summary(original_df, valid_df, key_cols, editable_cols)
        action_df.loc[action_df["_VALID"] == False, "_ACTION"] = "INVALID"
        summary = {
            "invalid": int(action_df["_ACTION"].eq("INVALID").sum()),
            "new": int(action_df["_ACTION"].eq("NEW").sum()),
            "updated": int(action_df["_ACTION"].eq("UPDATE").sum()),
            "unchanged": int(action_df["_ACTION"].eq("UNCHANGED").sum()),
        }
        return action_df, summary

    if "Programs" in tab_map:
        with tab_map["Programs"]:
            render_section("Programs — bulk edit", "Edit program names, owners, FTE, and program rates.")
            _bulk_available("Programs")
            prog_cache_ver = int(st.session_state.get("bulk_prog_cache_ver", 0))
            t0 = time.perf_counter()
            prog_df = _load_bulk_programs(prog_cache_ver)
            render_perf_info("Programs load", time.perf_counter() - t0, len(prog_df) if isinstance(prog_df, pd.DataFrame) else None)
            prog_df = _apply_ctx(prog_df, "Programs")
            if prog_df is None or prog_df.empty:
                prog_df = pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE", "PROGRAM_XOM_RATE"])
            prog_df = _maybe_cap_df(prog_df, "bulk_prog", "Programs")
            st.session_state.setdefault("bulk_prog_df", prog_df)
            if st.button("↻ Reload Programs", key="reload_prog"):
                st.session_state["bulk_prog_df"] = prog_df
                st.session_state["bulk_prog_cache_ver"] = prog_cache_ver + 1
            edited = st.data_editor(
                st.session_state["bulk_prog_df"],
                use_container_width=True,
                num_rows="dynamic",
                disabled=["PROGRAMID"],
                key="edit_progs",
            )
            st.session_state["bulk_prog_df"] = edited
            action_df, summary = _bulk_state(
                "Programs",
                prog_df,
                edited,
                key_cols=["PROGRAMID"],
                editable_cols=["PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE", "PROGRAM_XOM_RATE"],
                required_cols=["PROGRAMNAME"],
                numeric_cols=["PROGRAMFTE", "PROGRAM_XOM_RATE"],
            )
            _render_summary(summary)
            invalid_rows = action_df[action_df["_ACTION"] == "INVALID"]
            if not invalid_rows.empty:
                with st.expander("Validation details"):
                    st.dataframe(invalid_rows[["PROGRAMID", "PROGRAMNAME", "_ERRORS"]], use_container_width=True)
            with st.expander("Changes preview"):
                st.dataframe(action_df[action_df["_ACTION"].isin(["NEW", "UPDATE"])], use_container_width=True)

            def _prog_preview():
                from utils.admin_bulk_edit import build_preview_payload
                return build_preview_payload("Programs", action_df, summary, ["PROGRAMID"])

            def _prog_apply():
                errors = []
                applied = 0
                for _, r in action_df.iterrows():
                    if r.get("_ACTION") not in ("NEW", "UPDATE") or not r.get("_VALID"):
                        continue
                    pid = str(r.get("PROGRAMID") or "").strip() or _new_uuid()
                    pname = str(r.get("PROGRAMNAME") or "").strip()
                    try:
                        upsert_program(
                            pid,
                            pname,
                            (str(r.get("PROGRAMOWNER")) if pd.notna(r.get("PROGRAMOWNER")) else None),
                            (float(r.get("PROGRAMFTE")) if pd.notna(r.get("PROGRAMFTE")) else None),
                            (float(r.get("PROGRAM_XOM_RATE")) if pd.notna(r.get("PROGRAM_XOM_RATE")) else None),
                        )
                        applied += 1
                    except Exception as e:
                        errors.append(f"{pname or pid}: {e}")
                if errors:
                    return {"ok": False, "message": "Some updates failed.", "counts": {"applied": applied, "errors": len(errors)}}
                st.session_state["bulk_prog_cache_ver"] = prog_cache_ver + 1
                refreshed = _load_bulk_programs(prog_cache_ver + 1)
                refreshed = _apply_ctx(refreshed, "Programs")
                refreshed = _maybe_cap_df(refreshed, "bulk_prog", "Programs")
                st.session_state["bulk_prog_df"] = refreshed or pd.DataFrame(
                    columns=["PROGRAMID", "PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE", "PROGRAM_XOM_RATE"]
                )
                return {"ok": True, "message": "Programs updated.", "counts": {"applied": applied}}

            render_preview_apply_audit_block(
                title="Apply Program bulk changes",
                area="bulk_edit",
                action_type="bulk_upsert",
                entity="PROGRAMS",
                context={"key_cols": ["PROGRAMID"]},
                preview_fn=_prog_preview,
                apply_fn=_prog_apply,
                require_typed_confirm=False,
            )

    if "Teams" in tab_map:
        with tab_map["Teams"]:
            render_section("Teams — bulk edit", "Edit team names, program assignment, and headcount fields.")
            _bulk_available("Teams")
            team_cache_ver = int(st.session_state.get("bulk_team_cache_ver", 0))
            t0 = time.perf_counter()
            teams_df = _load_bulk_teams(team_cache_ver)
            render_perf_info("Teams load", time.perf_counter() - t0, len(teams_df) if isinstance(teams_df, pd.DataFrame) else None)
            teams_df = _apply_ctx(teams_df, "Teams")
            if teams_df is None or teams_df.empty:
                teams_df = pd.DataFrame(columns=["TEAMID", "TEAMNAME", "PROGRAMNAME", "TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE", "PRODUCTOWNER"])
            teams_df = _maybe_cap_df(teams_df, "bulk_team", "Teams")
            prog_map = _map_name_to_id("PROGRAMS", "PROGRAMNAME", "PROGRAMID")
            prog_names = sorted(prog_map.keys())
            st.session_state.setdefault("bulk_team_df", teams_df)
            if st.button("↻ Reload Teams", key="reload_teams"):
                st.session_state["bulk_team_df"] = teams_df
                st.session_state["bulk_team_cache_ver"] = team_cache_ver + 1
            edited = st.data_editor(
                st.session_state["bulk_team_df"],
                use_container_width=True,
                num_rows="dynamic",
                disabled=["TEAMID"],
                key="edit_teams",
                column_config={
                    "PROGRAMNAME": st.column_config.SelectboxColumn("Program", options=prog_names, required=False),
                },
            )
            st.session_state["bulk_team_df"] = edited
            action_df, summary = _bulk_state(
                "Teams",
                teams_df,
                edited,
                key_cols=["TEAMID"],
                editable_cols=["TEAMNAME", "PROGRAMNAME", "TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE", "PRODUCTOWNER"],
                required_cols=["TEAMNAME"],
                numeric_cols=["TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE"],
            )
            _render_summary(summary)
            invalid_rows = action_df[action_df["_ACTION"] == "INVALID"]
            if not invalid_rows.empty:
                with st.expander("Validation details"):
                    st.dataframe(invalid_rows[["TEAMID", "TEAMNAME", "_ERRORS"]], use_container_width=True)
            with st.expander("Changes preview"):
                st.dataframe(action_df[action_df["_ACTION"].isin(["NEW", "UPDATE"])], use_container_width=True)

            def _team_preview():
                from utils.admin_bulk_edit import build_preview_payload
                return build_preview_payload("Teams", action_df, summary, ["TEAMID"])

            def _team_apply():
                errors = []
                applied = 0
                for _, r in action_df.iterrows():
                    if r.get("_ACTION") not in ("NEW", "UPDATE") or not r.get("_VALID"):
                        continue
                    tid = str(r.get("TEAMID") or "").strip() or _new_uuid()
                    tname = str(r.get("TEAMNAME") or "").strip()
                    prog_name = str(r.get("PROGRAMNAME") or "").strip() or None
                    prog_id = prog_map.get(prog_name) if prog_name else None
                    try:
                        upsert_team(
                            tid,
                            tname,
                            prog_id,
                            (float(r.get("TEAMFTE")) if pd.notna(r.get("TEAMFTE")) else None),
                            (float(r.get("DELIVERY_TEAM_FTE")) if pd.notna(r.get("DELIVERY_TEAM_FTE")) else None),
                            (float(r.get("CONTRACTOR_C_FTE")) if pd.notna(r.get("CONTRACTOR_C_FTE")) else None),
                            (float(r.get("CONTRACTOR_CS_FTE")) if pd.notna(r.get("CONTRACTOR_CS_FTE")) else None),
                        )
                        execute_active(f"UPDATE { _fq('TEAMS') } SET PRODUCTOWNER=%s WHERE TEAMID=%s",
                                (str(r.get("PRODUCTOWNER")) if pd.notna(r.get("PRODUCTOWNER")) else None, tid))
                        applied += 1
                    except Exception as e:
                        errors.append(f"{tname or tid}: {e}")
                if errors:
                    return {"ok": False, "message": "Some updates failed.", "counts": {"applied": applied, "errors": len(errors)}}
                st.session_state["bulk_team_cache_ver"] = team_cache_ver + 1
                refreshed = _load_bulk_teams(team_cache_ver + 1)
                refreshed = _apply_ctx(refreshed, "Teams")
                refreshed = _maybe_cap_df(refreshed, "bulk_team", "Teams")
                st.session_state["bulk_team_df"] = refreshed or pd.DataFrame(
                    columns=["TEAMID", "TEAMNAME", "PROGRAMNAME", "TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE", "PRODUCTOWNER"]
                )
                return {"ok": True, "message": "Teams updated.", "counts": {"applied": applied}}

            render_preview_apply_audit_block(
                title="Apply Team bulk changes",
                area="bulk_edit",
                action_type="bulk_upsert",
                entity="TEAMS",
                context={"key_cols": ["TEAMID"]},
                preview_fn=_team_preview,
                apply_fn=_team_apply,
                require_typed_confirm=False,
            )

    if "Vendors" in tab_map:
        with tab_map["Vendors"]:
            render_section("Vendors — bulk edit", "Edit vendor names; IDs are generated automatically for new rows.")
            _bulk_available("Vendors")
            vendor_cache_ver = int(st.session_state.get("bulk_vendor_cache_ver", 0))
            t0 = time.perf_counter()
            vendors_df = _load_bulk_vendors(vendor_cache_ver)
            render_perf_info("Vendors load", time.perf_counter() - t0, len(vendors_df) if isinstance(vendors_df, pd.DataFrame) else None)
            if vendors_df is None or vendors_df.empty:
                vendors_df = pd.DataFrame(columns=["VENDORID", "VENDORNAME"])
            vendors_df = _maybe_cap_df(vendors_df, "bulk_vendor", "Vendors")
            st.session_state.setdefault("bulk_vendor_df", vendors_df)
            if st.button("↻ Reload Vendors", key="reload_vendors"):
                st.session_state["bulk_vendor_df"] = vendors_df
                st.session_state["bulk_vendor_cache_ver"] = vendor_cache_ver + 1
            edited = st.data_editor(
                st.session_state["bulk_vendor_df"],
                use_container_width=True,
                num_rows="dynamic",
                disabled=["VENDORID"],
                key="edit_vendors",
            )
            st.session_state["bulk_vendor_df"] = edited
            action_df, summary = _bulk_state(
                "Vendors",
                vendors_df,
                edited,
                key_cols=["VENDORID"],
                editable_cols=["VENDORNAME"],
                required_cols=["VENDORNAME"],
                numeric_cols=[],
            )
            _render_summary(summary)
            invalid_rows = action_df[action_df["_ACTION"] == "INVALID"]
            if not invalid_rows.empty:
                with st.expander("Validation details"):
                    st.dataframe(invalid_rows[["VENDORID", "VENDORNAME", "_ERRORS"]], use_container_width=True)
            with st.expander("Changes preview"):
                st.dataframe(action_df[action_df["_ACTION"].isin(["NEW", "UPDATE"])], use_container_width=True)

            def _vendor_preview():
                from utils.admin_bulk_edit import build_preview_payload
                return build_preview_payload("Vendors", action_df, summary, ["VENDORID"])

            def _vendor_apply():
                errors = []
                applied = 0
                for _, r in action_df.iterrows():
                    if r.get("_ACTION") not in ("NEW", "UPDATE") or not r.get("_VALID"):
                        continue
                    vid = str(r.get("VENDORID") or "").strip() or _new_uuid()
                    vname = str(r.get("VENDORNAME") or "").strip()
                    try:
                        upsert_vendor(vid, vname)
                        applied += 1
                    except Exception as e:
                        errors.append(f"{vname or vid}: {e}")
                if errors:
                    return {"ok": False, "message": "Some updates failed.", "counts": {"applied": applied, "errors": len(errors)}}
                st.session_state["bulk_vendor_cache_ver"] = vendor_cache_ver + 1
                st.session_state["bulk_vendor_df"] = _load_bulk_vendors(vendor_cache_ver + 1) or pd.DataFrame(
                    columns=["VENDORID", "VENDORNAME"]
                )
                return {"ok": True, "message": "Vendors updated.", "counts": {"applied": applied}}

            render_preview_apply_audit_block(
                title="Apply Vendor bulk changes",
                area="bulk_edit",
                action_type="bulk_upsert",
                entity="VENDORS",
                context={"key_cols": ["VENDORID"]},
                preview_fn=_vendor_preview,
                apply_fn=_vendor_apply,
                require_typed_confirm=False,
            )

    if "Application Groups" in tab_map:
        with tab_map["Application Groups"]:
            render_section("Application Groups — bulk edit", "Edit application groups (owners, team, default vendor).")
            _bulk_available("Application Groups")
            group_cache_ver = int(st.session_state.get("bulk_group_cache_ver", 0))
            t0 = time.perf_counter()
            groups_df = _load_bulk_groups(group_cache_ver)
            render_perf_info("Groups load", time.perf_counter() - t0, len(groups_df) if isinstance(groups_df, pd.DataFrame) else None)
            groups_df = _apply_ctx(groups_df, "Application Groups")
            team_map = _map_name_to_id("TEAMS", "TEAMNAME", "TEAMID")
            vendor_map = _map_name_to_id("VENDORS", "VENDORNAME", "VENDORID")
            team_names = sorted(team_map.keys())
            vendor_names = sorted(vendor_map.keys())
            if groups_df is None or groups_df.empty:
                groups_df = pd.DataFrame(columns=["GROUPID", "GROUPNAME", "TEAMNAME", "VENDORNAME", "OWNER", "IS_BASE"])
            groups_df = _maybe_cap_df(groups_df, "bulk_group", "Application Groups")
            st.session_state.setdefault("bulk_group_df", groups_df)
            if st.button("↻ Reload Applications", key="reload_groups"):
                st.session_state["bulk_group_df"] = groups_df
                st.session_state["bulk_group_cache_ver"] = group_cache_ver + 1
            edited = st.data_editor(
                st.session_state["bulk_group_df"],
                use_container_width=True,
                num_rows="dynamic",
                disabled=["GROUPID"],
                key="edit_groups",
                column_config={
                    "TEAMNAME": st.column_config.SelectboxColumn("Accountable Team", options=team_names, required=False),
                    "VENDORNAME": st.column_config.SelectboxColumn("Default Vendor", options=vendor_names, required=False),
                    "IS_BASE": st.column_config.CheckboxColumn("BASE", default=False, help="Only one BASE per team"),
                },
            )
            st.session_state["bulk_group_df"] = edited
            action_df, summary = _bulk_state(
                "Applications",
                groups_df,
                edited,
                key_cols=["GROUPID"],
                editable_cols=["GROUPNAME", "TEAMNAME", "VENDORNAME", "OWNER", "IS_BASE"],
                required_cols=["GROUPNAME"],
                numeric_cols=[],
            )
            _render_summary(summary)
            invalid_rows = action_df[action_df["_ACTION"] == "INVALID"]
            if not invalid_rows.empty:
                with st.expander("Validation details"):
                    st.dataframe(invalid_rows[["GROUPID", "GROUPNAME", "_ERRORS"]], use_container_width=True)
            with st.expander("Changes preview"):
                st.dataframe(action_df[action_df["_ACTION"].isin(["NEW", "UPDATE"])], use_container_width=True)

            def _group_preview():
                from utils.admin_bulk_edit import build_preview_payload
                return build_preview_payload("Applications", action_df, summary, ["GROUPID"])

            def _group_apply():
                errors = []
                applied = 0
                for _, r in action_df.iterrows():
                    if r.get("_ACTION") not in ("NEW", "UPDATE") or not r.get("_VALID"):
                        continue
                    gid = str(r.get("GROUPID") or "").strip() or _new_uuid()
                    gname = str(r.get("GROUPNAME") or "").strip()
                    teamname = str(r.get("TEAMNAME") or "").strip() or None
                    team_id = team_map.get(teamname) if teamname else None
                    vendorname = str(r.get("VENDORNAME") or "").strip() or None
                    vendor_id = vendor_map.get(vendorname) if vendorname else None
                    owner = str(r.get("OWNER")) if pd.notna(r.get("OWNER")) else None
                    is_base = bool(r.get("IS_BASE")) if "IS_BASE" in r else False
                    try:
                        upsert_application_group(
                            group_id=gid,
                            group_name=gname,
                            team_id=team_id,
                            default_vendor_id=vendor_id,
                            owner=owner,
                            is_base=is_base,
                        )
                        applied += 1
                    except Exception as e:
                        errors.append(f"{gname or gid}: {e}")
                if errors:
                    return {"ok": False, "message": "Some updates failed.", "counts": {"applied": applied, "errors": len(errors)}}
                st.session_state["bulk_group_cache_ver"] = group_cache_ver + 1
                refreshed = _load_bulk_groups(group_cache_ver + 1)
                refreshed = _apply_ctx(refreshed, "Application Groups")
                refreshed = _maybe_cap_df(refreshed, "bulk_group", "Application Groups")
                st.session_state["bulk_group_df"] = refreshed or pd.DataFrame(
                    columns=["GROUPID", "GROUPNAME", "TEAMNAME", "VENDORNAME", "OWNER", "IS_BASE"]
                )
                return {"ok": True, "message": "Applications updated.", "counts": {"applied": applied}}

            render_preview_apply_audit_block(
                title="Apply Application bulk changes",
                area="bulk_edit",
                action_type="bulk_upsert",
                entity="APPLICATION_GROUPS",
                context={"key_cols": ["GROUPID"]},
                preview_fn=_group_preview,
                apply_fn=_group_apply,
                require_typed_confirm=False,
            )

    if "Applications" in tab_map:
        with tab_map["Applications"]:
            render_section("Applications — bulk edit", "Edit application instances and vendor overrides.")
            _bulk_available("Applications")
            app_cache_ver = int(st.session_state.get("bulk_app_cache_ver", 0))
            t0 = time.perf_counter()
            apps_df = _load_bulk_apps(app_cache_ver)
            render_perf_info("Applications load", time.perf_counter() - t0, len(apps_df) if isinstance(apps_df, pd.DataFrame) else None)
            apps_df = _apply_ctx(apps_df, "Applications")
            group_map = _map_name_to_id("APPLICATION_GROUPS", "GROUPNAME", "GROUPID")
            vendor_map2 = _map_name_to_id("VENDORS", "VENDORNAME", "VENDORID")
            group_names = sorted(group_map.keys())
            vendor_names2 = sorted(vendor_map2.keys())
            if apps_df is None or apps_df.empty:
                apps_df = pd.DataFrame(columns=["APPLICATIONID", "APPLICATIONNAME", "GROUPNAME", "ADD_INFO", "VENDORNAME"])
            apps_df = _maybe_cap_df(apps_df, "bulk_app", "Applications")
            st.session_state.setdefault("bulk_app_df", apps_df)
            if st.button("↻ Reload Application Instances", key="reload_apps"):
                st.session_state["bulk_app_df"] = apps_df
                st.session_state["bulk_app_cache_ver"] = app_cache_ver + 1
            edited = st.data_editor(
                st.session_state["bulk_app_df"],
                use_container_width=True,
                num_rows="dynamic",
                disabled=["APPLICATIONID"],
                key="edit_apps",
                column_config={
                    "GROUPNAME": st.column_config.SelectboxColumn("Application", options=group_names, required=False),
                    "VENDORNAME": st.column_config.SelectboxColumn("Vendor", options=vendor_names2, required=False),
                },
            )
            st.session_state["bulk_app_df"] = edited
            action_df, summary = _bulk_state(
                "Applications (Instances)",
                apps_df,
                edited,
                key_cols=["APPLICATIONID"],
                editable_cols=["APPLICATIONNAME", "GROUPNAME", "ADD_INFO", "VENDORNAME"],
                required_cols=["APPLICATIONNAME"],
                numeric_cols=[],
            )
            _render_summary(summary)
            invalid_rows = action_df[action_df["_ACTION"] == "INVALID"]
            if not invalid_rows.empty:
                with st.expander("Validation details"):
                    st.dataframe(invalid_rows[["APPLICATIONID", "APPLICATIONNAME", "_ERRORS"]], use_container_width=True)
            with st.expander("Changes preview"):
                st.dataframe(action_df[action_df["_ACTION"].isin(["NEW", "UPDATE"])], use_container_width=True)

            def _app_preview():
                from utils.admin_bulk_edit import build_preview_payload
                return build_preview_payload("Applications", action_df, summary, ["APPLICATIONID"])

            def _app_apply():
                errors = []
                applied = 0
                for _, r in action_df.iterrows():
                    if r.get("_ACTION") not in ("NEW", "UPDATE") or not r.get("_VALID"):
                        continue
                    aid = str(r.get("APPLICATIONID") or "").strip() or _new_uuid()
                    aname = str(r.get("APPLICATIONNAME") or "").strip()
                    groupname = str(r.get("GROUPNAME") or "").strip() or None
                    group_id = group_map.get(groupname) if groupname else None
                    add_info = str(r.get("ADD_INFO")) if pd.notna(r.get("ADD_INFO")) else None
                    vendorname = str(r.get("VENDORNAME") or "").strip() or None
                    vendor_id = vendor_map2.get(vendorname) if vendorname else None
                    try:
                        upsert_application_instance(aid, group_id, aname, add_info, vendor_id)
                        applied += 1
                    except Exception as e:
                        errors.append(f"{aname or aid}: {e}")
                if errors:
                    return {"ok": False, "message": "Some updates failed.", "counts": {"applied": applied, "errors": len(errors)}}
                st.session_state["bulk_app_cache_ver"] = app_cache_ver + 1
                refreshed = _load_bulk_apps(app_cache_ver + 1)
                refreshed = _apply_ctx(refreshed, "Applications")
                refreshed = _maybe_cap_df(refreshed, "bulk_app", "Applications")
                st.session_state["bulk_app_df"] = refreshed or pd.DataFrame(
                    columns=["APPLICATIONID", "APPLICATIONNAME", "GROUPNAME", "ADD_INFO", "VENDORNAME"]
                )
                return {"ok": True, "message": "Applications updated.", "counts": {"applied": applied}}

            render_preview_apply_audit_block(
                title="Apply Application Instance bulk changes",
                area="bulk_edit",
                action_type="bulk_upsert",
                entity="APPLICATIONS",
                context={"key_cols": ["APPLICATIONID"]},
                preview_fn=_app_preview,
                apply_fn=_app_apply,
                require_typed_confirm=False,
            )

    if "Invoices" in tab_map:
        with tab_map["Invoices"]:
            render_section("Invoices — bulk edit", "Edit invoices. Changes are applied via preview and audit.")
            _bulk_available("Invoices")

            @st.cache_data(ttl=90)
            def _load_invoice_table(cache_bust: int = 0) -> pd.DataFrame:
                return fetch_df_active(f"""
                SELECT
                  i.INVOICEID,
                  i.APPLICATIONID, a.APPLICATIONNAME,
                  i.TEAMID, t.TEAMNAME,
                  COALESCE(i.GROUPID, a.GROUPID) AS GROUPID,
                  g.GROUPNAME,
                  i.FISCAL_YEAR,
                  i.INVOICEDATE,
                  i.RENEWALDATE,
                  i.AMOUNT,
                  i.STATUS,
                  COALESCE(i.INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE,
                  i.PRODUCT_OWNER,
                  i.AMOUNT_NEXT_YEAR,
                  i.CONTRACT_ACTIVE,
                  i.COMPANY_CODE,
                  i.COST_CENTER,
                  i.SERIAL_NUMBER,
                  i.WORK_ORDER,
                  i.AGREEMENT_NUMBER,
                  i.CONTRACT_DUE,
                  i.SERVICE_TYPE,
                  i.NOTES
                FROM { _fq('INVOICES') } i
                LEFT JOIN { _fq('APPLICATIONS') } a ON a.APPLICATIONID = i.APPLICATIONID
                LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = i.TEAMID
                LEFT JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = COALESCE(i.GROUPID, a.GROUPID)
                ORDER BY COALESCE(i.RENEWALDATE, i.INVOICEDATE) DESC
            """)

            app_map = _map_name_to_id("APPLICATIONS", "APPLICATIONNAME", "APPLICATIONID")
            team_map = _map_name_to_id("TEAMS", "TEAMNAME", "TEAMID")
            grp_map = _map_name_to_id("APPLICATION_GROUPS", "GROUPNAME", "GROUPID")
            app_names = sorted(app_map.keys())
            team_names = sorted(team_map.keys())
            group_names = sorted(grp_map.keys())
            status_vals_df = fetch_df_active(f"SELECT DISTINCT STATUS FROM { _fq('INVOICES') } WHERE STATUS IS NOT NULL")
            if status_vals_df is None or status_vals_df.empty or "STATUS" not in status_vals_df.columns:
                status_options = ["Planned", "Completed", "Canceled"]
            else:
                status_options = sorted(set([str(x).strip() for x in status_vals_df["STATUS"].dropna().tolist()] + ["Planned", "Completed", "Canceled"]))
            type_vals_df = fetch_df_active(f"SELECT DISTINCT COALESCE(INVOICE_TYPE,'Recurring Invoice') AS IT FROM { _fq('INVOICES') }")
            if type_vals_df is None or type_vals_df.empty or "IT" not in type_vals_df.columns:
                itype_options = ["Recurring Invoice", "Adhoc invoice"]
            else:
                itype_options = sorted(set([str(x).strip() for x in type_vals_df["IT"].dropna().tolist()] + ["Recurring Invoice", "Adhoc invoice"]))
            show_cols = [
                "INVOICEID",
                "APPLICATIONNAME", "TEAMNAME", "GROUPNAME",
                "FISCAL_YEAR", "INVOICEDATE", "RENEWALDATE",
                "AMOUNT", "STATUS", "INVOICE_TYPE",
                "PRODUCT_OWNER", "AMOUNT_NEXT_YEAR", "CONTRACT_ACTIVE",
                "COMPANY_CODE", "COST_CENTER", "SERIAL_NUMBER",
                "WORK_ORDER", "AGREEMENT_NUMBER", "CONTRACT_DUE",
                "SERVICE_TYPE", "NOTES",
            ]
            inv_cache_ver = int(st.session_state.get("bulk_invoice_cache_ver", 0))
            if "inv_orig" not in st.session_state or "inv_edit" not in st.session_state:
                t0 = time.perf_counter()
                df_loaded = _load_invoice_table(inv_cache_ver)
                render_perf_info("Invoices load", time.perf_counter() - t0, len(df_loaded) if isinstance(df_loaded, pd.DataFrame) else None)
                df_loaded = _apply_ctx(df_loaded, "Invoices")
                df_loaded = _maybe_cap_df(df_loaded, "bulk_invoice", "Invoices")
                inv_orig = df_loaded[show_cols].copy() if not df_loaded.empty else pd.DataFrame(columns=show_cols)
                st.session_state["inv_orig"] = inv_orig
                st.session_state["inv_edit"] = inv_orig.copy()

            colX, colY = st.columns([1, 1])
            with colX:
                if st.button("Reset unsaved changes", key="reset_invoices"):
                    st.session_state["bulk_invoice_cache_ver"] = inv_cache_ver + 1
                    t0 = time.perf_counter()
                    fresh = _load_invoice_table(inv_cache_ver + 1)
                    render_perf_info("Invoices reload", time.perf_counter() - t0, len(fresh) if isinstance(fresh, pd.DataFrame) else None)
                    fresh = _apply_ctx(fresh, "Invoices")
                    fresh = _maybe_cap_df(fresh, "bulk_invoice", "Invoices")
                    st.session_state["inv_orig"] = fresh[show_cols].copy() if not fresh.empty else pd.DataFrame(columns=show_cols)
                    st.session_state["inv_edit"] = st.session_state["inv_orig"].copy()
                    st.success("Reset complete. Re-loaded from database.")
            with colY:
                st.caption("Edits persist during reruns; apply writes to the database.")

            edited = st.data_editor(
                st.session_state["inv_edit"],
                use_container_width=True,
                num_rows="dynamic",
                disabled=["INVOICEID"],
                key="edit_invoices_stable",
                column_config={
                    "APPLICATIONNAME": st.column_config.SelectboxColumn("Application", options=app_names, required=False),
                    "TEAMNAME": st.column_config.SelectboxColumn("Team", options=team_names, required=False),
                    "GROUPNAME": st.column_config.SelectboxColumn("Group (optional)", options=group_names, required=False),
                    "STATUS": st.column_config.SelectboxColumn("Status", options=status_options, required=False),
                    "INVOICE_TYPE": st.column_config.SelectboxColumn("Invoice Type", options=itype_options, required=False),
                    "CONTRACT_ACTIVE": st.column_config.CheckboxColumn("Contract Active"),
                },
            )
            st.session_state["inv_edit"] = edited
            action_df, summary = _bulk_state(
                "Invoices",
                st.session_state["inv_orig"],
                edited,
                key_cols=["INVOICEID"],
                editable_cols=[c for c in show_cols if c != "INVOICEID"],
                required_cols=["APPLICATIONNAME", "TEAMNAME"],
                numeric_cols=["AMOUNT", "AMOUNT_NEXT_YEAR"],
            )
            _render_summary(summary)
            invalid_rows = action_df[action_df["_ACTION"] == "INVALID"]
            if not invalid_rows.empty:
                with st.expander("Validation details"):
                    st.dataframe(invalid_rows[["INVOICEID", "APPLICATIONNAME", "TEAMNAME", "_ERRORS"]], use_container_width=True)
            with st.expander("Changes preview"):
                st.dataframe(action_df[action_df["_ACTION"].isin(["NEW", "UPDATE"])], use_container_width=True)

            def _invoice_preview():
                from utils.admin_bulk_edit import build_preview_payload
                return build_preview_payload("Invoices", action_df, summary, ["INVOICEID"])

            def _invoice_apply():
                errors = []
                saved = 0
                for _, r in action_df.iterrows():
                    if r.get("_ACTION") not in ("NEW", "UPDATE") or not r.get("_VALID"):
                        continue
                    if all((str(r.get(c) or "").strip() == "") for c in ["APPLICATIONNAME", "TEAMNAME", "FISCAL_YEAR", "AMOUNT"]):
                        continue
                    iid = str(r.get("INVOICEID") or "").strip()
                    is_new = iid == ""
                    appname = r.get("APPLICATIONNAME")
                    teamname = r.get("TEAMNAME")
                    groupname = r.get("GROUPNAME")
                    application_id = app_map.get(appname) if appname else None
                    team_id = team_map.get(teamname) if teamname else None
                    group_id = grp_map.get(groupname) if groupname else None
                    if not application_id or not team_id:
                        errors.append(f"{('[new]' if is_new else iid)}: Missing/unknown Application or Team.")
                        continue
                    if is_new:
                        iid = _new_uuid()
                    fiscal_year = _to_int(r.get("FISCAL_YEAR"))
                    invoicedate = _to_date(r.get("INVOICEDATE"))
                    renewaldate = _to_date(r.get("RENEWALDATE"))
                    amount = _to_float(r.get("AMOUNT")) or 0.0
                    status = (r.get("STATUS") or "Planned") or "Planned"
                    invoice_type = (r.get("INVOICE_TYPE") or "Recurring Invoice") or "Recurring Invoice"
                    product_owner = (r.get("PRODUCT_OWNER") or None) or None
                    amount_next_year = _to_float(r.get("AMOUNT_NEXT_YEAR"))
                    contract_active = _to_bool(r.get("CONTRACT_ACTIVE"))
                    company_code = (r.get("COMPANY_CODE") or None) or None
                    cost_center = (r.get("COST_CENTER") or None) or None
                    serial_number = (r.get("SERIAL_NUMBER") or None) or None
                    work_order = (r.get("WORK_ORDER") or None) or None
                    agreement_number = (r.get("AGREEMENT_NUMBER") or None) or None
                    contract_due = _to_int(r.get("CONTRACT_DUE"))
                    service_type = (r.get("SERVICE_TYPE") or None) or None
                    notes = (r.get("NOTES") or None) or None
                    if fiscal_year is None and renewaldate:
                        fiscal_year = renewaldate.year
                    try:
                        upsert_invoice(
                            invoice_id=iid,
                            application_id=application_id,
                            team_id=team_id,
                            renewal_date=renewaldate,
                            amount=amount,
                            status=status,
                            fiscal_year=fiscal_year,
                            product_owner=product_owner,
                            amount_next_year=amount_next_year,
                            contract_active=contract_active,
                            company_code=company_code,
                            cost_center=cost_center,
                            serial_number=serial_number,
                            work_order=work_order,
                            agreement_number=agreement_number,
                            contract_due=contract_due,
                            service_type=service_type,
                            notes=notes,
                            group_id=group_id,
                            programid_at_booking=None,
                            vendorid_at_booking=None,
                            groupid_at_booking=None,
                            rollover_batch_id=None,
                            rolled_over_from_year=None,
                            invoice_type=invoice_type,
                        )
                        if invoicedate:
                            try:
                                execute_active(f"UPDATE { _fq('INVOICES') } SET INVOICEDATE=%s WHERE INVOICEID=%s", (invoicedate, iid))
                            except Exception:
                                pass
                        saved += 1
                    except Exception as e:
                        errors.append(f"{iid}: {e}")
                if errors:
                    return {"ok": False, "message": "Some invoice updates failed.", "counts": {"saved": saved, "errors": len(errors)}}
                st.session_state["bulk_invoice_cache_ver"] = inv_cache_ver + 1
                fresh = _load_invoice_table(inv_cache_ver + 1)
                fresh = _apply_ctx(fresh, "Invoices")
                fresh = _maybe_cap_df(fresh, "bulk_invoice", "Invoices")
                st.session_state["inv_orig"] = fresh[show_cols].copy() if not fresh.empty else pd.DataFrame(columns=show_cols)
                st.session_state["inv_edit"] = st.session_state["inv_orig"].copy()
                return {"ok": True, "message": "Invoices updated.", "counts": {"saved": saved}}

            render_preview_apply_audit_block(
                title="Apply Invoice bulk changes",
                area="bulk_edit",
                action_type="bulk_upsert",
                entity="INVOICES",
                context={"key_cols": ["INVOICEID"]},
                preview_fn=_invoice_preview,
                apply_fn=_invoice_apply,
                require_typed_confirm=False,
            )

    if "Rates" in tab_map:
        with tab_map["Rates"]:
            render_section("Rates — bulk edit", "Rates are per PI (iteration).")
            if not _bulk_available("Rates"):
                st.info("Rates are unavailable because required tables are missing.")
            else:
                st.markdown("#### Team rates")
                team_rate_ver = int(st.session_state.get("bulk_team_rate_ver", 0))
                t0 = time.perf_counter()
                team_rates = _load_team_rates(team_rate_ver)
                render_perf_info("Team rates load", time.perf_counter() - t0, len(team_rates) if isinstance(team_rates, pd.DataFrame) else None)
                team_rates = _apply_ctx(team_rates, "Rates")
                team_rates = _maybe_cap_df(team_rates, "bulk_team_rates", "Team rates")
                st.session_state.setdefault("bulk_team_rates_df", team_rates)
                edited_team_rates = st.data_editor(
                    st.session_state["bulk_team_rates_df"],
                    use_container_width=True,
                    num_rows="dynamic",
                    key="edit_team_rates",
                )
                st.session_state["bulk_team_rates_df"] = edited_team_rates
                team_action_df, team_summary = _bulk_state(
                    "TeamRates",
                    team_rates,
                    edited_team_rates,
                    key_cols=["TEAMID", "LOCATION", "YEAR", "PI"],
                    editable_cols=["XOM_RATE"],
                    required_cols=["TEAMID", "LOCATION", "YEAR", "PI"],
                    numeric_cols=["XOM_RATE"],
                )
                _render_summary(team_summary)

                def _team_rate_preview():
                    from utils.admin_bulk_edit import build_preview_payload
                    return build_preview_payload("Team Rates", team_action_df, team_summary, ["TEAMID", "LOCATION", "YEAR", "PI"])

                def _team_rate_apply():
                    applied = 0
                    errors = []
                    for _, r in team_action_df.iterrows():
                        if r.get("_ACTION") not in ("NEW", "UPDATE") or not r.get("_VALID"):
                            continue
                        try:
                            upsert_team_rate_history(
                                str(r.get("TEAMID") or ""),
                                int(r.get("YEAR") or 0),
                                int(r.get("PI") or 0),
                                str(r.get("LOCATION") or ""),
                                float(r.get("XOM_RATE") or 0.0),
                                updated_by=str(user.get("email") or ""),
                            )
                            applied += 1
                        except Exception as e:
                            errors.append(str(e))
                    if errors:
                        return {"ok": False, "message": "Some team rate updates failed.", "counts": {"applied": applied, "errors": len(errors)}}
                    st.session_state["bulk_team_rate_ver"] = team_rate_ver + 1
                    refreshed = _load_team_rates(team_rate_ver + 1)
                    refreshed = _apply_ctx(refreshed, "Rates")
                    refreshed = _maybe_cap_df(refreshed, "bulk_team_rates", "Team rates")
                    st.session_state["bulk_team_rates_df"] = refreshed
                    return {"ok": True, "message": "Team rates updated.", "counts": {"applied": applied}}

                render_preview_apply_audit_block(
                    title="Apply Team rate changes",
                    area="bulk_edit",
                    action_type="bulk_upsert",
                    entity="TEAM_RATE_HISTORY",
                    context={"key_cols": ["TEAMID", "LOCATION", "YEAR", "PI"]},
                    preview_fn=_team_rate_preview,
                    apply_fn=_team_rate_apply,
                    require_typed_confirm=False,
                )

                st.markdown("#### Program rates")
                prog_rate_ver = int(st.session_state.get("bulk_prog_rate_ver", 0))
                t0 = time.perf_counter()
                prog_rates = _load_program_rates(prog_rate_ver)
                render_perf_info("Program rates load", time.perf_counter() - t0, len(prog_rates) if isinstance(prog_rates, pd.DataFrame) else None)
                prog_rates = _apply_ctx(prog_rates, "Rates")
                prog_rates = _maybe_cap_df(prog_rates, "bulk_program_rates", "Program rates")
                st.session_state.setdefault("bulk_prog_rates_df", prog_rates)
                edited_prog_rates = st.data_editor(
                    st.session_state["bulk_prog_rates_df"],
                    use_container_width=True,
                    num_rows="dynamic",
                    key="edit_prog_rates",
                )
                st.session_state["bulk_prog_rates_df"] = edited_prog_rates
                prog_action_df, prog_summary = _bulk_state(
                    "ProgramRates",
                    prog_rates,
                    edited_prog_rates,
                    key_cols=["PROGRAMID", "LOCATION", "YEAR", "PI"],
                    editable_cols=["PROGRAM_XOM_RATE"],
                    required_cols=["PROGRAMID", "LOCATION", "YEAR", "PI"],
                    numeric_cols=["PROGRAM_XOM_RATE"],
                )
                _render_summary(prog_summary)

                def _prog_rate_preview():
                    from utils.admin_bulk_edit import build_preview_payload
                    return build_preview_payload("Program Rates", prog_action_df, prog_summary, ["PROGRAMID", "LOCATION", "YEAR", "PI"])

                def _prog_rate_apply():
                    applied = 0
                    errors = []
                    for _, r in prog_action_df.iterrows():
                        if r.get("_ACTION") not in ("NEW", "UPDATE") or not r.get("_VALID"):
                            continue
                        try:
                            upsert_program_rate_history(
                                str(r.get("PROGRAMID") or ""),
                                int(r.get("YEAR") or 0),
                                int(r.get("PI") or 0),
                                str(r.get("LOCATION") or ""),
                                float(r.get("PROGRAM_XOM_RATE") or 0.0),
                                updated_by=str(user.get("email") or ""),
                            )
                            applied += 1
                        except Exception as e:
                            errors.append(str(e))
                    if errors:
                        return {"ok": False, "message": "Some program rate updates failed.", "counts": {"applied": applied, "errors": len(errors)}}
                    st.session_state["bulk_prog_rate_ver"] = prog_rate_ver + 1
                    refreshed = _load_program_rates(prog_rate_ver + 1)
                    refreshed = _apply_ctx(refreshed, "Rates")
                    refreshed = _maybe_cap_df(refreshed, "bulk_program_rates", "Program rates")
                    st.session_state["bulk_prog_rates_df"] = refreshed
                    return {"ok": True, "message": "Program rates updated.", "counts": {"applied": applied}}

                render_preview_apply_audit_block(
                    title="Apply Program rate changes",
                    area="bulk_edit",
                    action_type="bulk_upsert",
                    entity="PROGRAM_RATE_HISTORY",
                    context={"key_cols": ["PROGRAMID", "LOCATION", "YEAR", "PI"]},
                    preview_fn=_prog_rate_preview,
                    apply_fn=_prog_rate_apply,
                    require_typed_confirm=False,
                )

    render_audit_panel(area="bulk_edit")

with tab_sql:
    _raw_sql_mod, _raw_sql_err = safe_import(
        "Raw SQL",
        lambda: __import__("utils.admin_raw_sql", fromlist=["render_raw_sql_tab"]),
    )
    if _raw_sql_err is not None:
        _render_missing_dependency("Raw SQL", _raw_sql_err)
    else:
        _raw_sql_mod.render_raw_sql_tab()

    st.markdown("---")
    render_section("Analytics Views", "Use if new rate/cost schemas were added and charts aren’t updating.")
    col_fast, col_slow = st.columns([1, 1])
    with col_fast:
        if st.button("Rebuild analytics views (fast)", icon=":material/refresh:", key="rebuild_views_fast_btn"):
            try:
                ensure_analytics_views_ok()
                try:
                    st.session_state["_tco_analytics_ready"] = True
                    st.cache_data.clear()
                except Exception:
                    pass
                bump_data_version("rebuild_analytics_views")
                st.success("Analytics views rebuilt.")
                _warn_if_empty_feature_demand()
                _render_view_diagnostics()
            except Exception as e:
                st.error(f"Analytics rebuild failed: {e}")
    with col_slow:
        if st.button("Rebuild ALL views (slow)", icon=":material/refresh:", key="rebuild_views_slow_btn"):
            try:
                ensure_all_views_ok()
                try:
                    st.session_state["_tco_analytics_ready"] = True
                    st.cache_data.clear()
                except Exception:
                    pass
                bump_data_version("rebuild_all_views")
                st.success("All views rebuilt.")
                _render_view_diagnostics()
            except Exception as e:
                st.error(f"Full rebuild failed: {e}")

    render_audit_panel(area="raw_sql")

# =========================================
# CLEAN UP (Danger zone)
# =========================================

with tab_cleanup:
    render_section("Clean Up (Danger Zone)", "These actions permanently delete data. This cannot be undone.")
    st.warning("Proceed with extreme care. Consider taking a backup before continuing.")

    st.markdown("### Orphan scan (read-only)")
    st.caption("Scan for missing parent references (no fixes applied).")
    if st.button("Run orphan scan", key="admin_orphan_scan_btn"):
        samples = scan_for_orphans(limit=50)
        if not samples:
            st.success("No orphan rows detected in core tables.")
        else:
            for name, df in samples.items():
                st.markdown(f"**{name.replace('_', ' ').title()}**")
                st.dataframe(df, use_container_width=True, height=200)

    def _cleanup_schema_name() -> str:
        fq = _fq("TEAMS")
        parts = fq.split(".")
        return parts[-2] if len(parts) >= 2 else "dbo"

    @st.cache_data(ttl=90)
    def _load_schema_tables() -> List[str]:
        schema = _cleanup_schema_name()
        try:
            df = fetch_df_active(
                """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                """,
                (schema,),
            )
        except Exception:
            return []
        if df is None or df.empty:
            return []
        return [str(x).strip() for x in df["TABLE_NAME"].tolist() if str(x).strip()]

    @st.cache_data(ttl=90)
    def _load_schema_views() -> List[str]:
        schema = _cleanup_schema_name()
        try:
            df = fetch_df_active(
                """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME
                """,
                (schema,),
            )
        except Exception:
            return []
        if df is None or df.empty:
            return []
        return [str(x).strip() for x in df["TABLE_NAME"].tolist() if str(x).strip()]

    schema_name = _cleanup_schema_name()
    tables_available = _load_schema_tables()
    views_all = _load_schema_views()
    views_discovered = bool(views_all)
    st.caption(
        f"Schema: {schema_name} | Tables detected: {len(tables_available) or 'unknown'} | Views detected: {len(views_all) or 'unknown'}"
    )
    if not tables_available:
        st.info("Table discovery failed. Using the curated list below; some tables may be missing.")

    access_tables = [
        "APP_USER_MEMBERSHIP",
        "APP_USERS",
        "ADO_PORTFOLIO_SETTINGS",
        "EMAIL_ALERT_CONFIG",
    ]
    core_tables = [
        "APP_GROUP_TEAM_LINKS",
        "APPLICATIONS",
        "APPLICATION_GROUPS",
        "TEAMS",
        "PROGRAMS",
        "VENDORS",
    ]
    workforce_tables = [
        "TEAM_MSP_ASSIGNMENTS",
        "TEAM_MSP_RATE",
        "TEAM_RATE_HISTORY",
        "PROGRAM_RATE_HISTORY",
        "TEAM_HEADCOUNT_HISTORY",
        "TEAM_COMPOSITION_HISTORY",
        "PROGRAM_COMPOSITION_HISTORY",
        "CONTRACTOR_RATE_HISTORY",
        "TEAM_CONTRACTOR_HEADCOUNT",
        "CONTRACTOR_COMPANY",
    ]
    finance_tables = [
        "INVOICE_ATTACHMENTS",
        "INVOICE_NOTES",
        "ROLLOVER_LOG",
        "INVOICES",
        "CONTRACTS",
        "PROGRAM_ADDITIONAL_COSTS",
        "PROGRAM_APPTIO_WORKIDS",
        "APPTIO_ACTUALS",
    ]
    ado_tables = [
        "ADO_FEATURES_PREVIEW",
        "ADO_FEATURES",
        "MAP_ADO_APP_TO_TCO_GROUP",
        "MAP_ADO_PROGRAM_TO_TCO_PROGRAM",
        "MAP_ADO_TEAM_TO_TCO_TEAM",
    ]
    pi_tables = ["TCO_COMMITMENTS"]

    tables_in_order = [
        "INVOICE_ATTACHMENTS",
        "INVOICE_NOTES",
        "ROLLOVER_LOG",
        "APP_GROUP_TEAM_LINKS",
        "TEAM_MSP_ASSIGNMENTS",
        "TEAM_CONTRACTOR_HEADCOUNT",
        "CONTRACTOR_RATE_HISTORY",
        "TEAM_HEADCOUNT_HISTORY",
        "TEAM_RATE_HISTORY",
        "PROGRAM_RATE_HISTORY",
        "TEAM_COMPOSITION_HISTORY",
        "PROGRAM_COMPOSITION_HISTORY",
        "APPTIO_ACTUALS",
        "PROGRAM_APPTIO_WORKIDS",
        "PROGRAM_ADDITIONAL_COSTS",
        "TCO_COMMITMENTS",
        "INVOICES",
        "CONTRACTS",
        "APPLICATIONS",
        "APPLICATION_GROUPS",
        "TEAM_MSP_RATE",
        "TEAMS",
        "PROGRAMS",
        "VENDORS",
        "CONTRACTOR_COMPANY",
        "ADO_FEATURES_PREVIEW",
        "ADO_FEATURES",
        "MAP_ADO_APP_TO_TCO_GROUP",
        "MAP_ADO_PROGRAM_TO_TCO_PROGRAM",
        "MAP_ADO_TEAM_TO_TCO_TEAM",
        "APP_USER_MEMBERSHIP",
        "APP_USERS",
        "ADO_PORTFOLIO_SETTINGS",
        "EMAIL_ALERT_CONFIG",
    ]
    if tables_available:
        def _filter_existing(items: Sequence[str]) -> List[str]:
            return [t for t in items if t in tables_available]

        access_tables = _filter_existing(access_tables)
        core_tables = _filter_existing(core_tables)
        workforce_tables = _filter_existing(workforce_tables)
        finance_tables = _filter_existing(finance_tables)
        ado_tables = _filter_existing(ado_tables)
        pi_tables = _filter_existing(pi_tables)
        tables_in_order = _filter_existing(tables_in_order)

    known_tables = set(access_tables + core_tables + workforce_tables + finance_tables + ado_tables + pi_tables)
    other_tables = [t for t in tables_available if t not in known_tables] if tables_available else []
    for t in other_tables:
        if t not in tables_in_order:
            tables_in_order.append(t)

    table_groups = {
        "Core data": core_tables,
        "Workforce & rates": workforce_tables,
        "Finance": finance_tables,
        "ADO & mappings": ado_tables,
        "PI readiness": pi_tables,
        "Access & settings": access_tables,
        "Other tables": other_tables,
    }

    fallback_views_vw = [
        "VW_TEAM_WEIGHTED_RATES",
        "VW_INVOICE_SPEND",
        "VW_INVOICE_SPEND_PI",
        "VW_COSTS_AND_INVOICES",
        "VW_TCO_FEATURE_DEMAND",
        "VW_TCO_WF_LABOR_SPLIT",
        "VW_TCO_WORKFORCE_SPLIT",
        "VW_TEAM_COMPOSITION_EFFECTIVE",
        "VW_MSP_COSTS",
        "VW_INVOICE_FORECAST",
        "VW_INVOICES_ACTUAL_AND_FORECAST",
    ]
    if not views_all:
        views_all = fallback_views_vw.copy()
    if not views_discovered:
        st.info("View discovery failed. Using the default VW_* list for view actions.")
    views_vw = [v for v in views_all if str(v).upper().startswith("VW_")]

    if "cleanup_pick" not in st.session_state:
        st.session_state["cleanup_pick"] = []
    st.session_state["cleanup_pick"] = [t for t in st.session_state["cleanup_pick"] if t in tables_in_order]

    def _set_cleanup_pick(values: Sequence[str]) -> None:
        st.session_state["cleanup_pick"] = [t for t in values if t in tables_in_order]

    st.markdown("### Quick presets")
    preset_cols = st.columns(4)
    with preset_cols[0]:
        if st.button("Core data", key="cleanup_preset_core"):
            _set_cleanup_pick(core_tables)
    with preset_cols[1]:
        if st.button("Workforce + rates", key="cleanup_preset_workforce"):
            _set_cleanup_pick(workforce_tables)
    with preset_cols[2]:
        if st.button("Finance", key="cleanup_preset_finance"):
            _set_cleanup_pick(finance_tables)
    with preset_cols[3]:
        if st.button("ADO only", key="cleanup_preset_ado"):
            _set_cleanup_pick(ado_tables)
    preset_cols2 = st.columns(2)
    with preset_cols2[0]:
        if st.button("Fresh start (no access)", key="cleanup_preset_fresh"):
            _set_cleanup_pick(core_tables + workforce_tables + finance_tables + ado_tables + pi_tables)
    with preset_cols2[1]:
        if st.button("Clear selection", key="cleanup_preset_clear"):
            _set_cleanup_pick([])

    st.markdown("### Truncate Selected Tables")
    cols = st.columns(2)
    with cols[0]:
        pick = st.multiselect("Tables to truncate", options=tables_in_order, key="cleanup_pick")
        st.caption(f"{len(pick)} table(s) selected.")
        if any(t in pick for t in access_tables):
            st.warning("Access tables selected. This can lock users out until APP_USERS is restored.")
        with st.expander("Table groups (detected)", expanded=False):
            for label, tables in table_groups.items():
                if not tables:
                    continue
                st.write(f"{label}: {', '.join(tables)}")
    with cols[1]:
        confirm = st.text_input("Type TRUNCATE to confirm", key="cleanup_trunc_confirm")
        if st.button("Truncate Selected", type="primary", disabled=(not pick or confirm.strip().upper() != "TRUNCATE")):
            results: List[str] = []
            for t in pick:
                fq = _fq(t)
                try:
                    execute_active(f"TRUNCATE TABLE {fq}")
                    results.append(f"Truncated {t}")
                except Exception:
                    try:
                        execute_active(f"DELETE FROM {fq}")
                        results.append(f"Deleted rows from {t}")
                    except Exception as e:
                        results.append(f"Failed {t}: {e}")
            st.success("\n".join(results))
            st.cache_data.clear()

    st.markdown("### Reset ADO Sync Tables")
    st.caption("Remove all imported ADO features and mappings so you can reload from backups.")
    include_ado_settings = st.checkbox(
        "Also clear ADO portfolio settings",
        value=False,
        key="cleanup_reset_ado_settings",
    )
    ado_reset_tables = ado_tables + (["ADO_PORTFOLIO_SETTINGS"] if include_ado_settings else [])
    def _ado_reset_preview() -> Dict[str, Any]:
        counts: Dict[str, int] = {}
        for t in ado_reset_tables:
            fq = _fq(t)
            try:
                df_cnt = fetch_df_active(f"SELECT COUNT(*) CNT FROM {fq}")
                if isinstance(df_cnt, pd.DataFrame) and not df_cnt.empty:
                    counts[t] = int(df_cnt.iloc[0]["CNT"])
            except Exception:
                counts[t] = 0
        return {
            "ok": True,
            "message": "Previewed ADO tables to clear.",
            "counts": counts,
        }

    def _ado_reset_apply() -> Dict[str, Any]:
        msgs: List[str] = []
        for t in ado_reset_tables:
            fq = _fq(t)
            try:
                execute_active(f"SELECT TOP 1 1 FROM {fq}")
            except Exception:
                msgs.append(f"Skipped {t}: table not found")
                continue
            try:
                execute_active(f"TRUNCATE TABLE {fq}")
                msgs.append(f"Truncated {t}")
            except Exception:
                try:
                    execute_active(f"DELETE FROM {fq}")
                    msgs.append(f"Deleted rows from {t}")
                except Exception as e:
                    msgs.append(f"Failed {t}: {e}")
        st.cache_data.clear()
        return {"ok": True, "message": " | ".join(msgs), "counts": {"tables": len(ado_reset_tables)}}

    render_preview_apply_audit_block(
        title="Clear ADO tables",
        area="cleanup",
        action_type="clear_ado_tables",
        entity="ADO tables",
        context={"tables": ado_reset_tables},
        preview_fn=_ado_reset_preview,
        apply_fn=_ado_reset_apply,
        require_typed_confirm=True,
        confirm_phrase="DELETE",
        danger_level="danger",
    )

    st.markdown("### Drop Analytical Views (VW_*)")
    view_scope = st.radio(
        "View scope",
        ["VW_* only", "All views in schema"],
        horizontal=True,
        key="cleanup_view_scope",
    )
    views_to_drop = views_vw if view_scope == "VW_* only" else views_all
    st.caption(f"{len(views_to_drop)} view(s) detected for this scope.")
    with st.expander("Views in scope", expanded=False):
        st.write(", ".join(views_to_drop) if views_to_drop else "None")
    drop_views = st.checkbox("Drop selected scope", value=False, key="cleanup_drop_views")
    rebuild_after = st.checkbox("Rebuild views after drop (ensure_all_views_ok)", value=True, key="cleanup_rebuild_views")
    def _views_preview() -> Dict[str, Any]:
        return {
            "ok": True,
            "message": "Previewed views in scope.",
            "counts": {"views_in_scope": len(views_to_drop), "drop_selected": int(drop_views)},
            "details": {"views": views_to_drop[:50]},
        }

    def _views_apply() -> Dict[str, Any]:
        msgs: List[str] = []
        if drop_views:
            for v in views_to_drop:
                try:
                    sch = _cleanup_schema_name()
                    execute_active(f"IF OBJECT_ID('{sch}.{v}', 'V') IS NOT NULL DROP VIEW {sch}.{v}")
                    msgs.append(f"Dropped view {v}")
                except Exception as e:
                    msgs.append(f"Failed to drop {v}: {e}")
        if rebuild_after:
            try:
                ensure_all_views_ok()
                msgs.append("Rebuilt views.")
            except Exception as e:
                msgs.append(f"Rebuild failed: {e}")
        if msgs:
            st.success("\n".join(msgs))
        return {"ok": True, "message": " | ".join(msgs) if msgs else "No view changes applied."}

    render_preview_apply_audit_block(
        title="Apply view changes",
        area="cleanup",
        action_type="apply_view_changes",
        entity="analytics_views",
        context={"drop_views": drop_views, "rebuild_after": rebuild_after, "views": views_to_drop},
        preview_fn=_views_preview,
        apply_fn=_views_apply,
        require_typed_confirm=True,
        confirm_phrase="DELETE",
        danger_level="danger",
    )

    st.markdown("### Truncate ALL Data (fresh start)")
    st.caption("This clears data from all key tables above but keeps the schema. Views can be rebuilt afterwards.")
    group_options = [k for k, v in table_groups.items() if v] or list(table_groups.keys())
    default_groups = [g for g in ["Core data", "Workforce & rates", "Finance", "ADO & mappings", "PI readiness"] if g in group_options]
    selected_groups = st.multiselect(
        "Groups to clear",
        options=group_options,
        default=default_groups,
        key="cleanup_all_groups",
    )
    selected_set: set = set()
    for grp in selected_groups:
        selected_set.update(table_groups.get(grp, []))
    tables_all = [t for t in tables_in_order if t in selected_set]
    st.caption(f"{len(tables_all)} table(s) across {len(selected_groups)} group(s).")
    with st.expander("Tables to clear (ordered)", expanded=False):
        st.write(", ".join(tables_all) if tables_all else "None")
    if "Access & settings" in selected_groups:
        st.warning("Access tables selected. Make sure you can restore APP_USERS or use admin auto-login.")
    do_drop_views = st.checkbox("Also drop views before truncate", value=True, key="cleanup_all_drop_views")
    do_rebuild_views = st.checkbox("Rebuild views after truncate", value=True, key="cleanup_all_rebuild_views")
    def _truncate_preview() -> Dict[str, Any]:
        counts: Dict[str, int] = {}
        for t in tables_all:
            fq = _fq(t)
            try:
                df_cnt = fetch_df_active(f"SELECT COUNT(*) CNT FROM {fq}")
                if isinstance(df_cnt, pd.DataFrame) and not df_cnt.empty:
                    counts[t] = int(df_cnt.iloc[0]["CNT"])
            except Exception:
                counts[t] = 0
        ok = bool(tables_all)
        return {
            "ok": ok,
            "message": "Previewed tables selected for truncate." if ok else "No tables selected to truncate.",
            "counts": counts,
            "details": {"tables": tables_all},
        }

    def _truncate_apply() -> Dict[str, Any]:
        logs: List[str] = []
        try:
            if do_drop_views:
                for v in views_to_drop:
                    try:
                        sch = _cleanup_schema_name()
                        execute_active(f"IF OBJECT_ID('{sch}.{v}', 'V') IS NOT NULL DROP VIEW {sch}.{v}")
                        logs.append(f"Dropped view {v}")
                    except Exception as e:
                        logs.append(f"Failed to drop {v}: {e}")
            for t in tables_all:
                fq = _fq(t)
                try:
                    execute_active(f"TRUNCATE TABLE {fq}")
                    logs.append(f"Truncated {t}")
                except Exception:
                    try:
                        execute_active(f"DELETE FROM {fq}")
                        logs.append(f"Deleted rows from {t}")
                    except Exception as e:
                        logs.append(f"Failed {t}: {e}")
            if do_rebuild_views:
                try:
                    ensure_all_views_ok()
                    logs.append("Rebuilt views.")
                except Exception as e:
                    logs.append(f"Rebuild failed: {e}")
            st.success("\n".join(logs))
            return {"ok": True, "message": " | ".join(logs), "counts": {"tables": len(tables_all)}}
        except Exception as e:
            st.error(f"Clean up failed: {e}")
            return {"ok": False, "message": f"Clean up failed: {e}"}

    render_preview_apply_audit_block(
        title="Truncate all selected data",
        area="cleanup",
        action_type="truncate_all",
        entity="tables",
        context={"tables": tables_all, "drop_views": do_drop_views, "rebuild_views": do_rebuild_views},
        preview_fn=_truncate_preview,
        apply_fn=_truncate_apply,
        require_typed_confirm=True,
        confirm_phrase="DELETE",
        danger_level="danger",
    )

    render_audit_panel(area="cleanup")

# =========================================
# EMAIL ALERTS CONFIGURATION
# =========================================

with tab_email:
    render_section("Email Alerts", "Manage alert defaults, verify Microsoft Graph connectivity, and send test messages.")

    ensure_email_alert_config_table()

    cfg_full = get_email_alert_config()
    cfg = {k: v for k, v in cfg_full.items() if not str(k).startswith("_")}
    cfg_meta = cfg_full.get("_meta") or {}

    status_key = "email_status_result"

    def _default_status() -> Dict[str, Any]:
        try:
            return get_mail_status(test_token=False, sender_override=cfg.get("mail_sender"))
        except Exception as exc:
            return {"ok": False, "checks": [{"name": "config", "ok": False, "detail": str(exc)}]}

    if status_key not in st.session_state:
        st.session_state[status_key] = _default_status()

    status_col, info_col = st.columns([1, 1])

    with status_col:
        st.markdown("#### Mail Status")
        if st.button("Run status check", key="email_status_refresh", help="Attempts to acquire a Microsoft Graph token."):
            try:
                st.session_state[status_key] = get_mail_status(test_token=True, sender_override=cfg.get("mail_sender"))
            except Exception as exc:
                st.session_state[status_key] = {"ok": False, "checks": [{"name": "status", "ok": False, "detail": str(exc)}]}

        status_res = st.session_state.get(status_key) or {}
        if status_res.get("ok"):
            st.success("Mail configuration looks good.")
        else:
            st.error("Configuration issues detected.")
        for chk in status_res.get("checks", []):
            icon = "✅" if chk.get("ok") else "⚠️"
            detail = chk.get("detail") or chk.get("name")
            st.write(f"{icon} {detail}")

    with info_col:
        st.markdown("#### Current Defaults")
        st.metric("Pending window (days)", value=str(cfg.get("pending_window_days", 30)))
        st.metric("Contract window (months)", value=str(cfg.get("contract_window_months", 9)))
        st.caption(f"Subject prefix: {cfg.get('subject_prefix', '[NEXT]')}")
        if cfg.get("schedule_enabled"):
            day = int(cfg.get("schedule_day") or 1)
            hour = int(cfg.get("schedule_hour_utc") or 13)
            hour = max(0, min(23, hour))
            st.caption(f"Auto-send schedule: day {day} @ {hour:02d}:00 UTC")
        else:
            st.caption("Auto-send schedule: disabled")
        sender_override = cfg.get("mail_sender")
        st.caption(f"Sender override: {sender_override or 'using secrets.mail_sender/email.sender'}")
        if cfg_meta.get("updated_at") or cfg_meta.get("updated_by"):
            meta_bits = []
            if cfg_meta.get("updated_at"):
                try:
                    meta_ts = pd.to_datetime(cfg_meta.get("updated_at"))
                    meta_bits.append(meta_ts.strftime("%Y-%m-%d %H:%M"))
                except Exception:
                    meta_bits.append(str(cfg_meta.get("updated_at")))
            if cfg_meta.get("updated_by"):
                meta_bits.append(f"by {cfg_meta.get('updated_by')}")
            st.caption("Last updated " + " ".join(meta_bits))

    st.markdown("---")
    st.markdown("#### Alert Defaults")

    def _safe_int(val: Any, default: int) -> int:
        try:
            return int(val)
        except Exception:
            try:
                return int(float(val))
            except Exception:
                return default

    pending_val = _safe_int(cfg.get("pending_window_days"), 30)
    contract_val = _safe_int(cfg.get("contract_window_months"), 9)
    sched_day_val = max(1, min(28, _safe_int(cfg.get("schedule_day"), 1)))
    sched_hour_val = max(0, min(23, _safe_int(cfg.get("schedule_hour_utc"), 13)))

    with st.form("email_alert_cfg_form"):
        schedule_enabled_input = st.checkbox("Enable monthly auto-send", value=bool(cfg.get("schedule_enabled")))
        sched_cols = st.columns(2)
        with sched_cols[0]:
            schedule_day_input = st.number_input(
                "Send on day of month",
                min_value=1,
                max_value=28,
                value=sched_day_val,
                step=1,
                disabled=not schedule_enabled_input,
            )
        with sched_cols[1]:
            schedule_hour_input = st.number_input(
                "Send at (UTC hour)",
                min_value=0,
                max_value=23,
                value=sched_hour_val,
                step=1,
                disabled=not schedule_enabled_input,
            )

        st.caption("Scheduling metadata is stored for your automation jobs; configure your scheduler to read these settings and trigger the alert email task accordingly.")

        col_int_1, col_int_2 = st.columns(2)
        with col_int_1:
            pending_input = st.number_input("Pending window (days)", min_value=7, max_value=120, value=pending_val, step=1)
        with col_int_2:
            contract_input = st.number_input("Contract window (months)", min_value=1, max_value=24, value=contract_val, step=1)

        subject_prefix_input = st.text_input("Subject prefix", value=str(cfg.get("subject_prefix", "[NEXT]")))
        subject_template_input = st.text_input(
            "Subject template",
            value=str(cfg.get("subject_template", "{prefix} Action needed: {count} invoice(s) {status} for {program}")),
            help="Placeholders: {prefix}, {count}, {status}, {status_title}, {program}"
        )
        body_intro_input = st.text_area(
            "Intro paragraph",
            value=str(cfg.get("body_intro", "The following invoices are currently flagged as {status_title}. Please review and take action.")),
            help="Placeholders: {status_title}, {status}, {program}, {count}, {pending_window_days}"
        )
        footnote_input = st.text_area(
            "Footnote",
            value=str(cfg.get("footnote_template", "Pending = due within {pending_window_days} day(s). Critical = past due and still planned.")),
            help="Rendered in italics. Placeholders: {pending_window_days}, {status_title}, {program}"
        )
        body_footer_input = st.text_area(
            "Footer",
            value=str(cfg.get("body_footer", "Update the invoice status, add notes, or attach approvals directly in the Invoice Tracking page. This email was generated automatically from the NEXT Invoice Tracking dashboard.")),
            help="Placeholders: {program}, {count}"
        )
        save_to_sent_input = st.checkbox("Save to Sent Items", value=bool(cfg.get("save_to_sent_items")))
        col_rec_to, col_rec_cc = st.columns(2)
        with col_rec_to:
            extra_to_input = st.text_input(
                "Additional To recipients",
                value=str(cfg.get("additional_recipients", "")),
                help="Comma or semicolon separated emails added to every alert."
            )
        with col_rec_cc:
            extra_cc_input = st.text_input(
                "Additional CC",
                value=str(cfg.get("additional_cc", "")),
                help="Comma or semicolon separated emails cc'd on every alert."
            )
        mail_sender_input = st.text_input(
            "Override sender (optional)",
            value=str(cfg.get("mail_sender", "")),
            help="Leave blank to use secrets.email.sender or azuread.mail_sender."
        )

        save_cfg = st.form_submit_button("Save configuration", type="primary")

        if save_cfg:
            payload = {
                "pending_window_days": int(pending_input),
                "contract_window_months": int(contract_input),
                "subject_prefix": subject_prefix_input.strip() or "[NEXT]",
                "subject_template": subject_template_input.strip() or "{prefix} Action needed: {count} invoice(s) {status} for {program}",
                "body_intro": body_intro_input.strip(),
                "footnote_template": footnote_input.strip(),
                "body_footer": body_footer_input.strip(),
                "save_to_sent_items": bool(save_to_sent_input),
                "additional_recipients": extra_to_input.strip(),
                "additional_cc": extra_cc_input.strip(),
                "schedule_enabled": bool(schedule_enabled_input),
                "schedule_day": int(schedule_day_input) if schedule_enabled_input else int(sched_day_val),
                "schedule_hour_utc": int(schedule_hour_input) if schedule_enabled_input else int(sched_hour_val),
                "mail_sender": mail_sender_input.strip(),
            }
            try:
                save_email_alert_config(payload, user.get("email"))
                st.cache_data.clear()
                toast_success("Email alert defaults saved.")
            except Exception as exc:
                toast_error(f"Save failed: {exc}")

    if st.button("Restore defaults", key="email_alert_restore", help="Reset to built-in defaults"):
        try:
            save_email_alert_config({}, user.get("email"))
            st.cache_data.clear()
            toast_success("Email alert defaults restored.")
        except Exception as exc:
            toast_error(f"Restore failed: {exc}")

    st.markdown("---")

    def _split_emails(raw: str) -> List[str]:
        if not raw:
            return []
        items = raw.replace(";", ",").split(",")
        out: List[str] = []
        for item in items:
            addr = item.strip()
            if addr:
                out.append(addr)
        return out

    def _merge_addresses(addrs: Iterable[str]) -> List[str]:
        seen = set()
        result: List[str] = []
        for val in addrs:
            if not val:
                continue
            addr = str(val).strip()
            if not addr:
                continue
            key = addr.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(addr)
        return result

    def _preview_recipients(program_id: Optional[str], team_id: Optional[str]) -> Tuple[pd.DataFrame, List[str]]:
        where = ["COALESCE(i.INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'"]
        params: List[Any] = []
        if program_id:
            where.append("COALESCE(p.PROGRAMID, t.PROGRAMID) = %s")
            params.append(program_id)
        if team_id:
            where.append("t.TEAMID = %s")
            params.append(team_id)
        where_sql = " WHERE " + " AND ".join(where) if where else ""

        sql = f"""
            SELECT
                i.INVOICEID,
                i.RENEWALDATE,
                i.AMOUNT,
                i.STATUS,
                i.CONTRACT_DUE,
                COALESCE(i.INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE,
                t.TEAMID,
                t.TEAMNAME,
                t.PRODUCTOWNER,
                COALESCE(p.PROGRAMID, t.PROGRAMID) AS PROGRAM_ID_EFFECTIVE,
                COALESCE(p.PROGRAMNAME, 'Unassigned Program') AS PROGRAMNAME,
                p.PROGRAMOWNER,
                a.APPLICATIONNAME,
                a.ADD_INFO
            FROM { _fq('INVOICES') } i
            LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = i.TEAMID
            LEFT JOIN { _fq('APPLICATIONS') } a ON a.APPLICATIONID = i.APPLICATIONID
            LEFT JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = a.GROUPID
            LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = COALESCE(g.PROGRAMID, t.PROGRAMID)
            {where_sql}
        """
        df = fetch_df_active(sql, tuple(params) if params else None)
        if df is None or df.empty:
            return pd.DataFrame(), []

        df = df.rename(columns={
            "RENEWALDATE": "Renewal Date",
            "AMOUNT": "Amount (USD)",
            "STATUS": "Status",
            "CONTRACT_DUE": "Contract Due",
            "TEAMNAME": "Team",
            "PRODUCTOWNER": "Responsible",
            "PROGRAMNAME": "Program",
            "PROGRAMOWNER": "Program Manager Email",
            "APPLICATIONNAME": "Application",
        })
        df["Program"] = df["Program"].fillna("Unassigned Program")
        if "ADD_INFO" in df.columns:
            df["Application"] = df.apply(
                lambda r: f"{r['Application']}" + (f" — {r['ADD_INFO']}" if r.get("ADD_INFO") else ""),
                axis=1
            )

        due_days = int(cfg.get("pending_window_days") or pending_val)
        contract_months = int(cfg.get("contract_window_months") or contract_val)
        track_df = _classify_tracking(df, due_days, contract_months)
        working = track_df[track_df["Tracking Status"].isin(["Critical", "Pending"])].copy()
        if working.empty:
            return pd.DataFrame(), []

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

        missing_owner_msgs: List[str] = []
        extra_to_global = _split_emails(str(cfg.get("additional_recipients", "")))
        extra_cc_global = _split_emails(str(cfg.get("additional_cc", "")))

        rows: List[Dict[str, Any]] = []

        def _status_phrase(values: Sequence[str]) -> str:
            unique_vals = [v for v in dict.fromkeys([_fmt_text(v) for v in values]) if v != "—"]
            if not unique_vals:
                return "Critical/Pending"
            if len(unique_vals) == 1:
                return unique_vals[0]
            return ", ".join(unique_vals[:-1]) + f" & {unique_vals[-1]}"

        for (po_email_raw, pm_email_raw, program_name), grp in working.groupby(["Responsible", "Program Manager Email", "Program"], dropna=False):
            program_label = _fmt_text(program_name, "Unassigned Program")
            po_email = _clean_email(po_email_raw)
            pm_email = _clean_email(pm_email_raw)

            if not po_email:
                teams_desc = ", ".join(sorted({
                    _fmt_text(val, "Unknown team")
                    for val in grp.get("Team", [])
                })) or "Unknown team"
                missing_owner_msgs.append(f"{program_label} · {teams_desc}")
                continue

            statuses = grp["Tracking Status"].tolist()
            status_phrase = _status_phrase(statuses)
            to_list = _merge_addresses([po_email] + extra_to_global)
            cc_list = _merge_addresses(([pm_email] if pm_email else []) + extra_cc_global)
            teams_list = ", ".join(sorted({
                _fmt_text(val, "Unknown team")
                for val in grp.get("Team", [])
            })) or "Unknown team"
            total_amount = float(grp["Amount (USD)"].fillna(0).sum())

            rows.append({
                "Program": program_label,
                "Product Owner": po_email,
                "Program Manager": pm_email or "(none)",
                "Teams": teams_list,
                "Invoices": len(grp),
                "Amount (USD)": round(total_amount, 2),
                "Statuses": status_phrase,
                "To": ", ".join(to_list) if to_list else "(none)",
                "CC": ", ".join(cc_list) if cc_list else "(none)",
            })

        rows.sort(key=lambda r: (r["Program"], r["Product Owner"].lower()))
        return pd.DataFrame(rows), missing_owner_msgs

    st.markdown("#### Recipient Preview")
    prog_map = _map_name_to_id('PROGRAMS', 'PROGRAMNAME', 'PROGRAMID')
    prog_options = ["All"] + sorted(prog_map.keys())
    sel_prog = st.selectbox("Program", options=prog_options, key="email_preview_program")
    program_id_sel = prog_map.get(sel_prog) if sel_prog != "All" else None

    if program_id_sel:
        teams_df = _teams_under_program(program_id_sel)
    else:
        teams_df = fetch_df_active(f"SELECT TEAMID, TEAMNAME FROM { _fq('TEAMS') } ORDER BY TEAMNAME")
    if teams_df is None or teams_df.empty:
        team_options = ["All"]
        team_map: Dict[str, str] = {}
    else:
        team_map = {str(r["TEAMNAME"]).strip(): str(r["TEAMID"]).strip() for _, r in teams_df.iterrows() if pd.notna(r["TEAMNAME"]) and pd.notna(r["TEAMID"]) }
        team_options = ["All"] + sorted(team_map.keys())

    sel_team = st.selectbox("Team", options=team_options, key="email_preview_team")
    team_id_sel = team_map.get(sel_team) if sel_team != "All" else None

    preview_df, missing_owner_msgs = _preview_recipients(program_id_sel, team_id_sel)

    extra_to_list = _split_emails(str(cfg.get("additional_recipients", "")))
    extra_cc_list = _split_emails(str(cfg.get("additional_cc", "")))
    st.caption(
        f"Global To recipients: {', '.join(extra_to_list) if extra_to_list else 'None'} · Global CC: {', '.join(extra_cc_list) if extra_cc_list else 'None'}"
    )

    if preview_df.empty:
        st.info("No Critical or Pending recurring invoices for the current filters.")
    else:
        st.dataframe(preview_df, use_container_width=True)

    if missing_owner_msgs:
        st.warning("Missing product owner emails for: " + "; ".join(missing_owner_msgs))

    with st.expander("Send test email", expanded=False):
        st.caption("Use this to validate delivery. HTML content is supported.")
        default_to = user.get("email") or ""
        test_to = st.text_input("To", value=default_to, key="email_test_to")
        test_cc = st.text_input("CC (optional)", value="", key="email_test_cc")
        test_subject = st.text_input(
            "Subject",
            value=f"{cfg.get('subject_prefix', '[NEXT]')} Test message",
            key="email_test_subject"
        )
        test_body = st.text_area(
            "Body (HTML)",
            value="<p>This is a test message from the NEXT Invoice Tracking tool.</p>",
            height=160,
            key="email_test_body"
        )
        if st.button("Send test email", key="email_test_send_manual"):
            to_list = _split_emails(test_to)
            cc_list = _split_emails(test_cc)
            if not to_list:
                st.error("Provide at least one recipient.")
            else:
                try:
                    send_graph_mail(
                        subject=test_subject.strip() or "NEXT Test",
                        body_html=test_body,
                        to_recipients=to_list,
                        cc_recipients=cc_list or None,
                        save_to_sent=bool(cfg.get("save_to_sent_items")),
                        sender_override=cfg.get("mail_sender"),
                    )
                    st.success("Test email sent.")
                except EmailConfigError as exc:
                    st.error(f"Configuration error: {exc}")
                except EmailSendError as exc:
                    st.error(f"Send failed: {exc}")

        st.caption(
            "Placeholders available in templates: {prefix}, {program}, {program_label}, {status}, {status_title}, {count}, {pending_window_days}."
        )

    render_audit_panel(area="email_alerts")

# =========================================
# SCHEMA
# =========================================

with tab_schema:
    _schema_err = _load_schema_helpers()
    if _schema_err is not None:
        _render_missing_dependency("Schema & Dependencies helpers", _schema_err)

    render_section("Schema & Dependencies", "Manifest-driven schema overview with drift detection and safe cleanup.")

    expected_tables_df = pd.DataFrame(EXPECTED_TABLES)
    expected_views_df = pd.DataFrame(EXPECTED_VIEWS)
    actual_tables_df = list_db_tables()
    actual_views_df = list_db_views()

    expected_table_names = set(expected_tables_df["name"].tolist()) if not expected_tables_df.empty else set()
    expected_view_names = set(expected_views_df["name"].tolist()) if not expected_views_df.empty else set()
    actual_table_names = set(actual_tables_df["OBJECT_NAME"].tolist()) if not actual_tables_df.empty else set()
    actual_view_names = set(actual_views_df["OBJECT_NAME"].tolist()) if not actual_views_df.empty else set()

    missing_tables = sorted(expected_table_names - actual_table_names)
    missing_views = sorted(expected_view_names - actual_view_names)
    extra_tables = sorted(actual_table_names - expected_table_names)
    extra_views = sorted(actual_view_names - expected_view_names)

    st.markdown("#### Overview")
    o1, o2, o3, o4 = st.columns(4)
    with o1:
        st.metric("Expected tables", len(expected_table_names))
    with o2:
        st.metric("Expected views", len(expected_view_names))
    with o3:
        st.metric("Missing objects", len(missing_tables) + len(missing_views))
    with o4:
        st.metric("Extra objects", len(extra_tables) + len(extra_views))

    st.markdown("#### Ensure schema")
    b1, b2, b3, b4, b5 = st.columns(5)
    with b1:
        if st.button("Ensure core tables", key="schema_ensure_core"):
            try:
                if get_portfolio_ctx() is None:
                    raise RuntimeError("No active portfolio selected.")
                _ensure_tables()
                log_admin_action(action_type="ensure_schema", area="schema", entity="system", summary="Ensured core tables.")
                st.success("Core tables ensured.")
            except Exception as e:
                st.error(f"Ensure core tables failed: {e}")
    with b2:
        if st.button("Ensure ADO tables", key="schema_ensure_ado"):
            try:
                if get_portfolio_ctx() is None:
                    raise RuntimeError("No active portfolio selected.")
                ensure_ado_minimal_tables()
                log_admin_action(action_type="ensure_schema", area="schema", entity="system", summary="Ensured ADO tables.")
                st.success("ADO tables ensured.")
            except Exception as e:
                st.error(f"Ensure ADO tables failed: {e}")
    with b3:
        if st.button("Ensure Apptio tables", key="schema_ensure_apptio"):
            try:
                if get_portfolio_ctx() is None:
                    raise RuntimeError("No active portfolio selected.")
                ensure_apptio_actuals_lines_table()
                ensure_map_apptio_to_cost_type_table()
                log_admin_action(action_type="ensure_schema", area="schema", entity="system", summary="Ensured Apptio tables.")
                st.success("Apptio tables ensured.")
            except Exception as e:
                st.error(f"Ensure Apptio tables failed: {e}")
    with b4:
        if st.button("Ensure views", key="schema_ensure_views"):
            try:
                if get_portfolio_ctx() is None:
                    raise RuntimeError("No active portfolio selected.")
                ensure_all_views_ok()
                log_admin_action(action_type="ensure_schema", area="schema", entity="system", summary="Ensured views.")
                st.success("Views ensured.")
            except Exception as e:
                st.error(f"Ensure views failed: {e}")
    with b5:
        if st.button("Ensure analytics views", key="schema_ensure_analytics"):
            try:
                ensure_analytics_views_ok()
                log_admin_action(action_type="ensure_schema", area="schema", entity="system", summary="Ensured analytics views.")
                st.success("Analytics views ensured.")
                _warn_if_empty_feature_demand()
            except Exception as e:
                st.error(f"Ensure analytics views failed: {e}")

    st.markdown("#### Expected tables (by category)")
    if expected_tables_df.empty:
        st.info("No manifest tables defined.")
    else:
        for category, grp in expected_tables_df.groupby("category"):
            with st.expander(category.title(), expanded=False):
                st.dataframe(grp[["name", "description", "keys"]], use_container_width=True)

    st.markdown("#### Expected views")
    if expected_views_df.empty:
        st.info("No manifest views defined.")
    else:
        st.dataframe(expected_views_df[["name", "description", "depends_on"]], use_container_width=True)

    st.markdown("#### Missing objects")
    missing_rows = [{"name": n, "type": "table"} for n in missing_tables] + [{"name": n, "type": "view"} for n in missing_views]
    if missing_rows:
        st.dataframe(pd.DataFrame(missing_rows), use_container_width=True)
    else:
        st.success("No missing objects detected.")

    st.markdown("#### Extra objects (drift candidates)")
    deps_df = list_view_dependencies()
    deps_map = {}
    if deps_df is not None and not deps_df.empty:
        for _, row in deps_df.iterrows():
            ref = str(row.get("REFERENCED_OBJECT") or "").strip()
            view = str(row.get("VIEW_NAME") or "").strip()
            if ref:
                deps_map.setdefault(ref, set()).add(view)
    deprecated_df = list_deprecated_objects()
    deprecated_set = set(deprecated_df["OBJECT_NAME"].tolist()) if not deprecated_df.empty else set()

    extra_entries = []
    for name in extra_tables:
        extra_entries.append(
            {
                "name": name,
                "type": "table",
                "row_count": get_row_count(name),
                "referenced_by": ", ".join(sorted(deps_map.get(name, []))) or None,
                "deprecated": name in deprecated_set,
            }
        )
    for name in extra_views:
        extra_entries.append(
            {
                "name": name,
                "type": "view",
                "row_count": None,
                "referenced_by": ", ".join(sorted(deps_map.get(name, []))) or None,
                "deprecated": name in deprecated_set,
            }
        )
    extra_df = pd.DataFrame(extra_entries)
    if extra_df.empty:
        st.success("No extra objects detected.")
    else:
        st.dataframe(extra_df, use_container_width=True)

    st.markdown("#### Columns")
    all_objects = sorted(list(expected_table_names | expected_view_names | actual_table_names | actual_view_names))
    selected_obj = st.selectbox("Select table/view", options=[""] + all_objects, key="schema_columns_select")
    if selected_obj:
        cols_df = list_db_columns(selected_obj)
        if cols_df.empty:
            st.info("Columns unavailable or object not found.")
        else:
            st.dataframe(cols_df, use_container_width=True)

    st.markdown("#### Dependency diagram")
    mode = st.radio(
        "Diagram mode",
        ["Full diagram", "Mini diagrams by domain"],
        horizontal=True,
        key="schema.diagram.mode",
    )
    include_views = st.checkbox("Include views", value=True, key="schema.diagram.include_views")
    include_legacy = st.checkbox("Include legacy", value=False, key="schema.diagram.include_legacy")
    height_full = st.selectbox("Full diagram height", [700, 900, 1200], index=1, key="schema.diagram.height_full")
    height_mini = st.selectbox("Mini diagram height", [450, 550, 650], index=1, key="schema.diagram.height_mini")

    all_categories = sorted({t.get("category") for t in EXPECTED_TABLES if t.get("category")})
    if not include_legacy and "legacy" in all_categories:
        all_categories.remove("legacy")

    def _view_domain(view: dict) -> str:
        deps = set(view.get("depends_on", []) or [])
        if not deps:
            return "views"
        dep_cats = []
        for t in EXPECTED_TABLES:
            if t.get("name") in deps:
                dep_cats.append(t.get("category"))
        if not dep_cats:
            return "views"
        return max(set(dep_cats), key=dep_cats.count)

    def build_schema_dot(
        *,
        included_categories: Optional[set[str]] = None,
        include_tables: bool = True,
        include_views: bool = True,
        title: Optional[str] = None,
    ) -> str:
        categories = included_categories or set(all_categories)
        tables = [t for t in EXPECTED_TABLES if t.get("category") in categories]
        table_names = {t["name"] for t in tables}
        views = []
        if include_views:
            for v in EXPECTED_VIEWS:
                deps = set(v.get("depends_on", []) or [])
                if not include_tables:
                    views.append(v)
                elif deps & table_names:
                    views.append(v)
        view_names = {v["name"] for v in views}
        edges = [
            (p, c)
            for p, c in EXPECTED_EDGES
            if (p in table_names or p in view_names) and (c in table_names or c in view_names)
        ]

        dot_lines = [
            "digraph G {",
            "  graph [rankdir=LR, nodesep=0.7, ranksep=1.1, splines=true, fontname=\"Inter\"];",
            "  node  [shape=box, fontsize=15, style=\"rounded,filled\", fontname=\"Inter\", color=\"#cbd5e1\", penwidth=1.1];",
            "  edge  [color=\"#94a3b8\", arrowsize=0.7, penwidth=0.9];",
        ]
        if title:
            dot_lines.append(f"  label=\"{title}\";")
            dot_lines.append("  labelloc=t;")
            dot_lines.append("  fontsize=16;")
        color_map = {
            "core": "#eef2ff",
            "invoices": "#fef2f2",
            "rates": "#ecfeff",
            "workforce": "#f0fdf4",
            "msp": "#f3e8ff",
            "ado": "#f5f3ff",
            "apptio": "#fffbeb",
            "security": "#e0f2fe",
            "audit": "#f8fafc",
            "legacy": "#fff7ed",
        }
        categories_map: Dict[str, List[dict]] = {}
        for t in tables:
            categories_map.setdefault(t["category"], []).append(t)
        for cat in sorted(categories_map.keys()):
            items = categories_map[cat]
            dot_lines.append(f"  subgraph cluster_{cat.replace(' ', '_')} {{")
            dot_lines.append(f"    label=\"{cat.title()}\";")
            dot_lines.append("    labelloc=t;")
            dot_lines.append("    style=\"rounded,dashed\";")
            dot_lines.append("    color=\"#e2e8f0\";")
            for entry in items:
                name = entry.get("name")
                desc = entry.get("description") or ""
                label = f"{name}\\n{desc}"
                fill = color_map.get(cat, "#f8fafc")
                dot_lines.append(f"    {name} [label=\"{label}\", fillcolor=\"{fill}\"];")
            dot_lines.append("  }")
        if view_names:
            dot_lines.append("  subgraph cluster_views {")
            dot_lines.append("    label=\"Views\";")
            dot_lines.append("    labelloc=t;")
            dot_lines.append("    style=\"rounded,dashed\";")
            dot_lines.append("    color=\"#e2e8f0\";")
            for v in sorted(view_names):
                dot_lines.append(f"    {v} [shape=ellipse, fillcolor=\"#f8fafc\", color=\"#e2e8f0\", label=\"{v}\" ];")
            dot_lines.append("  }")
        for parent, child in edges:
            dot_lines.append(f"  {parent} -> {child};")
        dot_lines.append("}")
        return "\n".join(dot_lines)

    has_dot = dot_available()
    status = "✅ SVG rendering enabled (dot found)" if has_dot else "⚠️ Falling back to basic chart (dot not found)"
    st.caption(status)

    def _render_dot(dot_str: str, height_px: int, filename: str) -> None:
        svg = render_dot_to_svg(dot_str) if has_dot else None
        if svg:
            render_svg_scrollable(svg, height_px=height_px, border=True)
            download_svg_button(svg, filename)
        else:
            st.warning("SVG rendering unavailable. Falling back to basic chart.")
            st.graphviz_chart(dot_str)

    if mode == "Full diagram":
        dot = build_schema_dot(
            included_categories=set(all_categories),
            include_views=include_views,
            include_tables=True,
            title="Full schema",
        )
        _render_dot(dot, height_full, "schema_full.svg")
    else:
        cols = st.columns(2)
        for idx, cat in enumerate(all_categories):
            dot = build_schema_dot(
                included_categories={cat},
                include_views=include_views,
                include_tables=True,
                title=f"{cat.title()} domain",
            )
            with cols[idx % 2]:
                st.markdown(f"**{cat.title()}**")
                _render_dot(dot, height_mini, f"schema_{cat}.svg")

    st.markdown("#### Cleanup workflow")
    if extra_df.empty:
        st.info("No extra objects to review.")
    else:
        selection = st.selectbox(
            "Select an extra object",
            options=[""] + [f"{r['name']} ({r['type']})" for r in extra_entries],
            key="schema_extra_select",
        )
        if selection:
            obj_name = selection.split(" (", 1)[0]
            obj_type = "view" if "(view)" in selection else "table"
            st.caption("Backup recommended before destructive changes. Use Admin → Backup & Restore.")
            notes = st.text_input("Deprecation notes (optional)", key="schema_deprecate_notes")
            if st.button("Mark deprecated", key="schema_mark_deprecated"):
                mark_deprecated(obj_name, obj_type, str(user.get("email") or ""), notes or None)
                log_admin_action(
                    action_type="deprecate_object",
                    area="schema",
                    entity=obj_name,
                    summary=f"Marked {obj_name} as deprecated.",
                    extra_json={"object": obj_name, "type": obj_type, "notes": notes},
                )
                st.success("Marked as deprecated.")

            def _drop_preview():
                exists = schema_object_exists(obj_name, obj_type)
                row_count = get_row_count(obj_name) if obj_type == "table" else None
                ref = deps_map.get(obj_name, set())
                return {
                    "ok": True,
                    "message": "Ready to drop object." if exists else "Object not found.",
                    "counts": {"row_count": row_count, "referenced_by": len(ref)},
                    "details": {"referenced_by": sorted(ref)},
                }

            def _drop_apply():
                try:
                    ddl = f"DROP VIEW { _fq(obj_name) }" if obj_type == "view" else f"DROP TABLE { _fq(obj_name) }"
                    execute_active(ddl)
                    return {"ok": True, "message": f"Dropped {obj_type} {obj_name}.", "counts": {}}
                except Exception as e:
                    return {"ok": False, "message": f"Drop failed: {e}", "counts": {}}

            render_preview_apply_audit_block(
                title=f"Drop {obj_type} {obj_name}",
                area="schema",
                action_type="drop_object",
                entity=obj_name,
                context={"object": obj_name, "type": obj_type},
                preview_fn=_drop_preview,
                apply_fn=_drop_apply,
                require_typed_confirm=True,
                confirm_phrase="DROP VIEW" if obj_type == "view" else "DROP TABLE",
                danger_level="danger",
            )

    st.markdown("#### Repo usage scan")
    if st.button("Scan repo for object usage", key="schema_repo_scan"):
        hits = []
        from pathlib import Path

        base = Path(__file__).resolve().parent.parent
        candidates = [r["name"] for r in extra_entries] if extra_entries else []
        for path in base.rglob("*.py"):
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for name in candidates:
                if name and name in text:
                    hits.append({"object": name, "file": str(path), "count": text.count(name)})
        if hits:
            st.dataframe(pd.DataFrame(hits), use_container_width=True)
        else:
            st.info("No references found in codebase.")

    render_audit_panel(area="schema_dependencies")

with tab_visual_lab:
    try:
        import importlib
        from viz import legacy_visual_lab_theme as _visual_lab

        importlib.reload(_visual_lab)
    except Exception as e:
        st.error(f"Visual Lab failed to load: {e}")
