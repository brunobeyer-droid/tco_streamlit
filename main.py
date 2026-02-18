# Summary: update page config to NEXT branding (title + icon path when available).
from pathlib import Path

import logging
import os
import time
import pandas as pd
import streamlit as st
from core.branding import APP_NAME
from utils.theme import use_theme, apply_sidebar_nav_compact_style
from core.debug import install_debug_expander_guard

try:
    _qp = st.query_params  # type: ignore[attr-defined]
    _safe_val = _qp.get("safe", "0")  # type: ignore[call-arg]
except Exception:
    try:
        _qp2 = st.experimental_get_query_params()  # type: ignore[attr-defined]
        _safe_val = (_qp2.get("safe") or ["0"])[0]
    except Exception:
        _safe_val = "0"

SAFE_MODE = str(_safe_val) == "1"

if SAFE_MODE:
    st.set_page_config(page_title="TCO SAFE MODE", layout="wide")
    st.write("SAFE MODE ACTIVE ✅")
    st.write("File:", __file__)
    st.write("Session keys:", sorted(list(st.session_state.keys())))
    st.write("_app_block_reason:", st.session_state.get("_app_block_reason"))
    with st.sidebar:
        st.write("SAFE MODE SIDEBAR ✅")
        st.write("Session keys:", sorted(list(st.session_state.keys())))
        st.write("_app_block_reason:", st.session_state.get("_app_block_reason"))
    st.stop()

_icon_default = "▣"
_icon_path = Path(__file__).resolve().parent / "assets" / "next_icon.png"
_page_icon = str(_icon_path) if _icon_path.exists() else _icon_default

st.set_page_config(page_title=APP_NAME, page_icon=_page_icon, layout="wide")
install_debug_expander_guard()

# Per-run nonce (used for debug toggle render gating).
st.session_state["_tco_run_nonce"] = int(st.session_state.get("_tco_run_nonce", 0)) + 1

try:
    from utils.auth import (
        _secret_get,
        _try_complete_authcode,
        _try_front_channel_logout,
        _resolve_admin_membership,
        current_user,
        get_current_user_email,
        get_identity_groups,
        is_admin_user,
        is_authenticated,
    )
except Exception:
    st.error("Authentication is not available. Contact an admin.")
    st.stop()

from utils.pages_catalog import build_navigation
from utils.sidebar import render_sidebar
from core.portfolio_runtime import get_active_portfolio_key
from core.portfolio_context import get_portfolio_ctx
from db.control_db import (
    control_db_available,
    control_schema_status,
    create_control_schema,
    seed_first_global_admin,
    is_control_global_admin,
    resolve_allowed_portfolios_debug,
    control_fetch_df,
    control_execute,
)
from db import get_user_by_email

TRACE_STARTUP = str(os.getenv("TCO_STARTUP_TRACE", "0")).strip().lower() in {"1", "true", "yes", "on"}
_trace_t0 = time.perf_counter()
_trace_last = _trace_t0
_startup_trace: list[dict] = []


def _trace_mark(label: str) -> None:
    global _trace_last
    if not TRACE_STARTUP:
        return
    now = time.perf_counter()
    entry = {
        "label": str(label),
        "since_start_ms": round((now - _trace_t0) * 1000.0, 1),
        "step_ms": round((now - _trace_last) * 1000.0, 1),
    }
    _trace_last = now
    _startup_trace.append(entry)
    if len(_startup_trace) > 80:
        del _startup_trace[:-80]
    try:
        st.session_state["_startup_trace_main"] = _startup_trace
    except Exception:
        pass

use_native_navigation = hasattr(st, "navigation")
st.session_state["use_native_navigation"] = bool(use_native_navigation)
_routing_logger = logging.getLogger("tco.routing")

def _bootstrap_control_db_first_run(admin_email: str) -> None:
    if not control_db_available():
        return
    try:
        create_control_schema()
    except Exception as e:
        st.error(f"Control DB unavailable during bootstrap: {e}")
        st.stop()
    email = str(admin_email or "").strip().lower()
    if not email:
        return
    try:
        df = control_fetch_df("SELECT COUNT(*) AS N FROM CONTROL_GLOBAL_ADMINS", None)
        n = int(df.iloc[0]["N"]) if df is not None and not df.empty else 0
    except Exception:
        n = 0
    if n != 0:
        return
    try:
        control_execute(
            """
            IF NOT EXISTS (
              SELECT 1 FROM CONTROL_GLOBAL_ADMINS
              WHERE UPPER(LTRIM(RTRIM(USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
            )
            INSERT INTO CONTROL_GLOBAL_ADMINS (USER_EMAIL, UPDATED_AT)
            VALUES (%s, SYSUTCDATETIME())
            """,
            (email, email),
        )
        try:
            chk = control_fetch_df(
                """
                SELECT COUNT(*) AS N
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = 'CONTROL_AUDIT_LOG'
                """,
                None,
            )
            has_audit = bool(int(chk.iloc[0]["N"])) if chk is not None and not chk.empty else False
        except Exception:
            has_audit = False
        if has_audit:
            control_execute(
                """
                INSERT INTO CONTROL_AUDIT_LOG (USER_EMAIL, ACTION, DETAILS)
                VALUES (%s, %s, %s)
                """,
                (email, "seed_first_global_admin", "Seeded first global admin on empty CONTROL_GLOBAL_ADMINS"),
            )
    except Exception:
        pass

# Process any incoming SSO callback before computing auth/nav state.
try:
    _try_front_channel_logout()
    _try_complete_authcode()
except Exception as e:
    st.session_state["_sso_callback_failed"] = str(e)
    if bool(_secret_get("auth", "sso_debug", False)):
        st.error(f"SSO callback failed in main: {e}")
_trace_mark("auth_callback")

user = current_user() or {}
authed = bool(is_authenticated())
if not user or not authed:
    st.session_state["_app_block_reason"] = "NOT_AUTHENTICATED"
else:
    st.session_state.pop("_app_block_reason", None)
# Canonical email for CONTROL DB access checks.
email = get_current_user_email()
if not authed:
    # Not logged in yet — allow UI to render a login screen later (after sidebar renders).
    email = None
else:
    # Logged in but no email resolved -> treat as auth failure.
    if not email:
        st.session_state["_app_block_reason"] = "NOT_AUTHENTICATED"

# Base role must come from authenticated identity (auth_user/current_user), not previewed session role.
# This prevents feedback loops when admin role-preview sets session role to CONTRIBUTOR/VIEWER.
_ss_user = st.session_state.get("auth_user") or {}
role = str((_ss_user or {}).get("role") or (user or {}).get("role") or st.session_state.get("user_role") or "").upper()
if not str(role or "").strip():
    role = str(st.session_state.get("role") or "").upper()


def _normalize_nav_role(raw: str) -> str:
    r = str(raw or "").strip().upper()
    if r == "EDITOR":
        return "CONTRIBUTOR"
    if r not in {"ADMIN", "CONTRIBUTOR", "VIEWER"}:
        return "VIEWER"
    return r

control_status: dict = {}

# Control DB schema readiness.
# IMPORTANT: do not st.stop() here; we render sidebar first and then show a visible block-screen.
if email and control_db_available():
    try:
        control_status = control_schema_status() or {}
    except Exception as e:
        control_status = {"ok": False, "error": str(e), "missing": []}
    if not bool(control_status.get("ok")):
        st.session_state["_app_block_reason"] = "CONTROL_DB_INCOMPLETE"
_trace_mark("control_db_status")

active_key = get_active_portfolio_key()

accessible = []
any_portfolios_exist = False
df_access_debug = None
is_global_admin_user = False
if email:
    if control_db_available():
        try:
            try:
                is_global_admin_user = bool(is_control_global_admin(email or ""))
            except Exception:
                is_global_admin_user = False
            df_any = control_fetch_df(
                """
                SELECT TOP 1 PORTFOLIO_KEY
                FROM CONTROL_PORTFOLIOS
                WHERE STATUS IN ('ACTIVE','ONBOARDING')
                """,
                None,
            )
            any_portfolios_exist = df_any is not None and not df_any.empty

            manual_keys: list[str] = []
            rule_keys: list[str] = []
            final_keys: list[str] = []
            try:
                groups = get_identity_groups() or []
            except Exception:
                groups = []
            try:
                eligibility = resolve_allowed_portfolios_debug(str(email or ""), groups)
                manual_keys = [str(k).strip() for k in (eligibility.get("manual_portfolios") or []) if str(k).strip()]
                rule_keys = [str(k).strip() for k in (eligibility.get("rule_portfolios") or []) if str(k).strip()]
            except Exception:
                manual_keys = []
                rule_keys = []

            if is_global_admin_user:
                # Global admins can route to all active/onboarding portfolios.
                df_access = control_fetch_df(
                    """
                    SELECT PORTFOLIO_KEY, DISPLAY_NAME, STATUS
                    FROM CONTROL_PORTFOLIOS
                    WHERE COALESCE(STATUS,'ACTIVE') IN ('ACTIVE','ONBOARDING')
                    ORDER BY DISPLAY_NAME
                    """,
                    None,
                )
            else:
                allowed_keys = sorted(set(manual_keys) | set(rule_keys))
                if allowed_keys:
                    placeholders = ",".join(["%s"] * len(allowed_keys))
                    df_access = control_fetch_df(
                        f"""
                        SELECT PORTFOLIO_KEY, DISPLAY_NAME, STATUS
                        FROM CONTROL_PORTFOLIOS
                        WHERE UPPER(LTRIM(RTRIM(PORTFOLIO_KEY))) IN ({placeholders})
                          AND COALESCE(STATUS,'ACTIVE') IN ('ACTIVE','ONBOARDING')
                        ORDER BY DISPLAY_NAME
                        """,
                        tuple([str(k).strip().upper() for k in allowed_keys]),
                    )
                else:
                    df_access = pd.DataFrame(columns=["PORTFOLIO_KEY", "DISPLAY_NAME", "STATUS"])

            final_keys = (
                [str(x).strip() for x in df_access["PORTFOLIO_KEY"].astype(str).tolist()]
                if df_access is not None and not df_access.empty
                else []
            )

            if str(os.getenv("APP_ENV", "prod") or "prod").strip().lower() == "prod":
                _routing_logger.info(
                    "routing_access email=%s manual_count=%s rule_count=%s portfolios=%s",
                    str(email or "").strip().lower(),
                    len(manual_keys),
                    len(rule_keys),
                    final_keys,
                )
            df_access_debug = df_access
            if df_access is not None and not df_access.empty:
                if st.session_state.get("debug_mode") is True:
                    try:
                        cols = [c for c in ["PORTFOLIO_KEY", "STATUS"] if c in df_access.columns]
                        st.write("accessible portfolios:", df_access[cols].to_dict("records"))
                    except Exception:
                        st.write("accessible portfolios: (debug print failed)")
                accessible = [
                    {"key": str(r.get("PORTFOLIO_KEY") or ""), "name": str(r.get("DISPLAY_NAME") or r.get("PORTFOLIO_KEY") or "")}
                    for _, r in df_access.iterrows()
                ]
        except Exception:
            accessible = []
            df_access_debug = None
else:
    accessible = []
    any_portfolios_exist = False
    is_global_admin_user = False
_trace_mark("portfolio_access_resolution")

if is_global_admin_user and role != "ADMIN":
    role = "ADMIN"
    st.session_state["role"] = "ADMIN"
    try:
        if isinstance(st.session_state.get("auth_user"), dict):
            st.session_state["auth_user"]["role"] = "ADMIN"
    except Exception:
        pass
elif email:
    # Recompute effective role each run from control membership first, then APP_USERS.
    # This keeps nav consistent with auth guards and avoids stale session elevation.
    try:
        global_admin_now, portfolio_role_now = _resolve_admin_membership(str(email or "").strip().lower())
        pr = str(portfolio_role_now or "").strip().upper()
        if bool(global_admin_now):
            role = "ADMIN"
        elif pr in {"CONTRIBUTOR", "VIEWER"}:
            role = pr
        else:
            rec = get_user_by_email(str(email or "").strip().lower())
            if rec and bool(rec.get("IS_ACTIVE", True)):
                resolved = _normalize_nav_role(str(rec.get("ROLE") or "VIEWER"))
                role = resolved
            else:
                role = _normalize_nav_role(str(role or "VIEWER"))
    except Exception:
        role = _normalize_nav_role(str(role or "VIEWER"))

# Optional admin strict scope-preview: apply impersonated role to navigation only.
try:
    auth_user_now = st.session_state.get("auth_user") or {}
    base_admin_now = str((auth_user_now.get("role") if isinstance(auth_user_now, dict) else "") or "").strip().upper() == "ADMIN"
    preview_email = str(st.session_state.get("_admin_scope_preview_email") or "").strip().lower()
    strict_preview = bool(st.session_state.get("admin_preview_tools_enabled", False)) and bool(
        st.session_state.get("_admin_scope_preview_strict_role", False)
    )
    if base_admin_now and strict_preview and preview_email:
        ga_imp, pr_imp = _resolve_admin_membership(preview_email)
        if bool(ga_imp):
            role = "ADMIN"
        elif str(pr_imp or "").strip().upper() in {"CONTRIBUTOR", "VIEWER"}:
            role = str(pr_imp).strip().upper()
        else:
            rec_imp = get_user_by_email(preview_email)
            if rec_imp and bool(rec_imp.get("IS_ACTIVE", True)):
                resolved_imp = _normalize_nav_role(str(rec_imp.get("ROLE") or "VIEWER"))
                role = resolved_imp
            else:
                role = "VIEWER"
        st.session_state["_admin_scope_preview_effective_role"] = role
    else:
        st.session_state.pop("_admin_scope_preview_effective_role", None)
except Exception:
    st.session_state.pop("_admin_scope_preview_effective_role", None)

# Admin-only scope preview (read-only). Base role must come from authenticated identity.
_auth_user_for_base = st.session_state.get("auth_user") or {}
base_role = str((_auth_user_for_base.get("role") if isinstance(_auth_user_for_base, dict) else "") or "").strip().upper()
if not base_role:
    base_role = str(role or "").strip().upper()
preview_tools_enabled = bool(st.session_state.get("admin_preview_tools_enabled", False))
st.session_state["_admin_role_preview"] = ""
if base_role != "ADMIN":
    st.session_state["_admin_scope_preview_email"] = ""
    st.session_state["_admin_scope_preview_name"] = ""
elif not preview_tools_enabled:
    st.session_state["_admin_scope_preview_email"] = ""
    st.session_state["_admin_scope_preview_name"] = ""
st.session_state["_role_base_effective"] = base_role
st.session_state["role"] = role
st.session_state["_is_control_global_admin"] = bool(is_global_admin_user)

# Restore portfolio context from URL on refresh/deep links (local dev convenience).
if email and not active_key:
    try:
        if hasattr(st, "query_params"):
            qp_port = str(st.query_params.get("portfolio", "") or "").strip()  # type: ignore[attr-defined]
        else:
            qp = st.experimental_get_query_params()  # type: ignore[attr-defined]
            qp_port = str((qp.get("portfolio") or [""])[0] or "").strip()
    except Exception:
        qp_port = ""

    if qp_port:
        accessible_keys = {str(p.get("key") or "").strip() for p in accessible if isinstance(p, dict)}
        if is_admin_user() or is_global_admin_user or (qp_port in accessible_keys):
            try:
                from core.portfolio_runtime import get_active_portfolio_key, set_active_portfolio
                set_active_portfolio(qp_port, email=email, role=role)
                active_now = str(get_active_portfolio_key() or "").strip()
                active_key = active_now or active_key
                blocked_reason = str(st.session_state.get("_app_block_reason") or "").strip()
                has_db_error = bool(st.session_state.get("_portfolio_db_error"))
                if active_now == qp_port and not blocked_reason and not has_db_error:
                    st.rerun()
            except Exception:
                pass

if st.session_state.get("debug_mode") is True:
    st.session_state["_routing_debug"] = {
        "resolved_email": str(email or "").strip().lower(),
        "global_admin": bool(is_global_admin_user),
        "allowed_portfolios": [str(p.get("key") or "") for p in accessible],
        "selected_portfolio": str(active_key or ""),
    }

# Render sidebar before any auth/portfolio gating stops.
if email and not active_key and not accessible:
    # Allow admins to continue into Admin setup mode even when no portfolios exist yet.
    if is_admin_user() or is_global_admin_user:
        st.session_state["_app_block_reason"] = "ADMIN_SETUP_MODE"
    elif not any_portfolios_exist:
        st.session_state["_app_block_reason"] = "NO_PORTFOLIO"
    else:
        st.session_state["_app_block_reason"] = "NO_PORTFOLIO_ACCESS"

pages_map, pages_meta = build_navigation(role)
st.session_state["_pages_meta"] = pages_meta
if not SAFE_MODE:
    use_theme(render_toggle=False, default="light")
    apply_sidebar_nav_compact_style()

nav = None
if use_native_navigation and authed:
    nav = st.navigation(pages_map)
    st.session_state["_native_nav_ready"] = True
else:
    st.session_state["_native_nav_ready"] = False
_trace_mark("navigation_built")

render_sidebar(pages_meta)
st.session_state["_sidebar_rendered_by_main"] = True
_trace_mark("sidebar_rendered")

if TRACE_STARTUP and bool(st.session_state.get("debug_mode")):
    with st.sidebar.expander("Debug - startup trace", expanded=False):
        st.dataframe(pd.DataFrame(_startup_trace), use_container_width=True, hide_index=True)
        perf_rows = []
        for k, label in (
            ("_perf_welcome_post_loader_cost_ms", "welcome_post_loader_cost_ms"),
            ("_perf_dashboard_post_loader_kpi_ms", "dashboard_post_loader_kpi_ms"),
            ("_perf_insights_post_loader_signals_ms", "insights_post_loader_signals_ms"),
        ):
            v = st.session_state.get(k)
            if v is not None:
                perf_rows.append({"metric": label, "value_ms": v})
        if perf_rows:
            st.markdown("**Page post-loader timings**")
            st.dataframe(pd.DataFrame(perf_rows), use_container_width=True, hide_index=True)

sso_callback_failed = st.session_state.pop("_sso_callback_failed", None)
if sso_callback_failed:
    if not authed:
        st.error("SSO callback failed. Please sign in again.")
        if bool(_secret_get("auth", "sso_debug", False)):
            st.code(str(sso_callback_failed), language="text")
    elif bool(_secret_get("auth", "sso_debug", False)):
        st.info("A transient SSO callback warning occurred but authentication completed.")
        st.code(str(sso_callback_failed), language="text")

sso_nonfatal_warning = st.session_state.pop("_sso_nonfatal_warning", None)
if sso_nonfatal_warning and bool(_secret_get("auth", "sso_debug", False)):
    st.info("SSO non-fatal warning")
    st.code(str(sso_nonfatal_warning), language="text")

reason = st.session_state.get("_app_block_reason")

if reason == "NOT_AUTHENTICATED":
    try:
        from utils.auth import _secret_get
        admin_email = _secret_get("auth", "admin_email", None)
    except Exception:
        admin_email = None
    _bootstrap_control_db_first_run(str(admin_email or ""))
    st.stop()

if reason == "CONTROL_DB_INCOMPLETE":
    st.title("Admin setup required")
    err = (control_status or {}).get("error")
    if err:
        st.error("Control DB connection/schema check failed")
        st.code(str(err), language="text")
    st.info("Go to Admin → Portfolios and click 'Create Control DB schema'.")
    st.page_link("pages/99_Admin.py", label="Open Admin → Portfolios", icon=":material/admin_panel_settings:")
    st.stop()

if reason == "NO_PORTFOLIO":
    st.title("Setup required")
    st.page_link("pages/99_Admin.py", label="Open Admin → Portfolios", icon=":material/admin_panel_settings:")
    if not control_db_available():
        st.error(
            "Control DB is not configured (missing CONTROL_MSSQL_* env vars or st.secrets['control_db'])."
        )
        st.code(
            """# .streamlit/secrets.toml
[control_db]
server = "your-sql-server"
database = "your-control-db"
user = "your-user"
password = "your-password"
driver = "ODBC Driver 17 for SQL Server"
encrypt = true
trust_server_certificate = false
schema = "dbo"
""",
            language="toml",
        )
        st.page_link("pages/99_Admin.py", label="Open Admin", icon=":material/admin_panel_settings:")
    else:
        st.info("No portfolios registered yet. Go to Admin → Portfolios.")
    st.stop()

if reason == "NO_PORTFOLIO_ACCESS":
    st.title("No portfolio access")
    st.error("No portfolio access granted. Contact an admin.")
    if role == "ADMIN" or is_global_admin_user:
        st.info("You are a global admin. Open Admin from the sidebar navigation to manage portfolio access.")
        st.page_link("pages/99_Admin.py", label="Open Admin → Portfolios", icon=":material/admin_panel_settings:")
    st.stop()

if reason in {"PORTFOLIO_DB_INCOMPLETE", "PORTFOLIO_DB_CONNECT_FAILED", "PORTFOLIO_DB_RESOLVE_FAILED"}:
    st.title("Portfolio configuration required")
    st.error("Selected portfolio DB configuration is invalid or unreachable.")
    err = str(st.session_state.get("_portfolio_db_error") or "").strip()
    details = st.session_state.get("_portfolio_db_error_details") or {}
    if err:
        st.caption(f"Error: {err}")
    if details and bool(st.session_state.get("debug_mode")):
        st.json(details)
    st.page_link("pages/99_Admin.py", label="Open Admin → Portfolios", icon=":material/admin_panel_settings:")
    st.stop()

if not active_key and len(accessible) == 1:
    only_key = str((accessible[0] or {}).get("key") or "").strip()
    if only_key:
        try:
            from core.portfolio_runtime import get_active_portfolio_key, set_active_portfolio
            set_active_portfolio(only_key, email=email, role=role)
            active_now = str(get_active_portfolio_key() or "").strip()
            active_key = active_now or active_key
            blocked_reason = str(st.session_state.get("_app_block_reason") or "").strip()
            has_db_error = bool(st.session_state.get("_portfolio_db_error"))
            if active_now == only_key and not blocked_reason and not has_db_error:
                st.rerun()
            if not active_now:
                st.error("Failed to activate selected portfolio. Open Admin → Portfolios to verify DB settings.")
                st.stop()
        except Exception as e:
            st.error(f"Failed to auto-select portfolio: {e}")
            st.stop()

if not active_key and len(accessible) > 1:
    st.title("Select Portfolio")
    st.info("Select a portfolio from the sidebar selector to continue.")
    st.stop()

if active_key and get_portfolio_ctx() is None:
    try:
        reason_now = str(st.session_state.get("_app_block_reason") or "").strip()
        if not reason_now.startswith("PORTFOLIO_DB_"):
            from core.portfolio_runtime import set_active_portfolio
            set_active_portfolio(active_key)
    except Exception:
        pass

if st.session_state.get("_app_block_reason") == "PORTFOLIO_ROLE_MISSING":
    st.title("Portfolio role required")
    st.error("You have routing access but no role in this portfolio. Ask portfolio admin.")
    if st.session_state.get("debug_mode") is True:
        st.caption(
            "Role lookup status: "
            + str(st.session_state.get("_portfolio_role_lookup_status") or "unknown")
        )
    st.stop()

if st.session_state.get("_app_block_reason") in {
    "NOT_AUTHENTICATED",
    "CONTROL_DB_INCOMPLETE",
    "NO_PORTFOLIO",
    "NO_PORTFOLIO_ACCESS",
    "PORTFOLIO_ROLE_MISSING",
}:
    st.stop()

if nav is not None:
    nav.run()
    st.stop()

# Pick a default landing page from pages_meta
pages_meta_list = []
for _section, _items in (pages_meta or {}).items():
    for _p in _items or []:
        if isinstance(_p, dict):
            pages_meta_list.append(_p)

default_page = None
for p in pages_meta_list:
    title = (p.get("title") or p.get("name") or "").lower()
    path = p.get("path") or p.get("page_path") or p.get("file") or p.get("filename")
    if "welcome" in title and path:
        default_page = path
        break
if default_page is None:
    # fallback to first page that has a path
    for p in pages_meta_list:
        path = p.get("path") or p.get("page_path") or p.get("file") or p.get("filename")
        if path:
            default_page = path
            break

if nav is None and default_page and not st.session_state.get("_did_default_switch"):
    app_root = Path(__file__).resolve().parent
    target = str(default_page).strip()
    target_ok = False
    try:
        target_path = (app_root / target).resolve()
        target_path.relative_to(app_root)
        target_ok = target_path.is_file()
    except Exception:
        target_ok = False

    if target_ok:
        st.session_state["_did_default_switch"] = True
        st.switch_page(target)
    else:
        st.error(f"Default page not found: {target}")
        st.stop()

st.write("No default page path found in pages_meta")
st.write("pages_meta keys:", [list(p.keys()) for p in pages_meta_list])
st.stop()
