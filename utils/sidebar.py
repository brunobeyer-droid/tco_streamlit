# utils/sidebar.py
import streamlit as st
try:
    from streamlit.runtime.scriptrunner import get_script_run_ctx
except Exception:  # pragma: no cover
    get_script_run_ctx = None  # type: ignore

try:
    from core.debug import is_debug_enabled
except Exception:
    def is_debug_enabled(*, label: str = "Debug", key: str = "debug_mode") -> bool:
        return False

try:
    from utils.auth import render_auth_sidebar
except Exception:
    def render_auth_sidebar():
        with st.sidebar:
            st.caption("Auth module unavailable")

try:
    from utils.sidebar_materials import render_materials_sidebar
except Exception:
    def render_materials_sidebar(pages_meta, *, title: str = "Materials") -> None:  # type: ignore[no-redef]
        _ = pages_meta, title


SIDEBAR_ICON_CSS = ""


def _nav_icon_links() -> list[tuple[str, str, str]]:
    """Monochrome glyph, label, target slug (without .py)."""
    return [
        ("▣", "Dashboard", "1_Dashboard"),
        ("▢", "Invoices", "2_Invoices"),
        ("▭", "Contracts", "2_Contracts"),
        ("▤", "Programs", "3_Programs"),
        ("▥", "Teams", "4_Teams"),
        ("▧", "Vendors", "5_Vendors"),
        ("▩", "Rates", "7_Rates"),
        ("▦", "Settings", "10_Settings"),
        ("❔", "How-To", "9_How_To"),
    ]


def render_global_actions():
    run_id = None
    if get_script_run_ctx is not None:
        try:
            ctx = get_script_run_ctx()
            run_id = getattr(ctx, "run_id", None)
        except Exception:
            run_id = None
    last_run = st.session_state.get("_tco_sidebar_rendered_run_id")
    if run_id is not None and last_run == run_id:
        return
    if run_id is not None:
        st.session_state["_tco_sidebar_rendered_run_id"] = run_id
    if SIDEBAR_ICON_CSS:
        with st.sidebar:
            st.markdown(SIDEBAR_ICON_CSS, unsafe_allow_html=True)
    # Keep portfolio selector rendering in exactly one place:
    # `render_sidebar()` -> `_render_portfolio_selector_inline()`.
    # Do not render another selector from global actions to avoid duplicates.
    # Auth controls near bottom of sidebar
    with st.sidebar:
        st.markdown("<div class='tco-sidebar-footer'>", unsafe_allow_html=True)
        render_auth_sidebar()
        st.markdown("</div>", unsafe_allow_html=True)


def _render_portfolio_selector_inline() -> None:
    # Reuse the same portfolio selection logic, but render in the current container
    # (no st.sidebar calls here; the compositor owns sidebar rendering).
    from core.portfolio_runtime import (
        user_accessible_portfolios,
        get_active_portfolio_key,
        get_portfolio_by_key,
        set_active_portfolio,
        clear_portfolio_caches_and_rerun,
    )
    from core.portfolio_context import get_portfolio_ctx

    user_email = st.session_state.get("auth_user", {}).get("email") or st.session_state.get("user_email") or ""
    user_role = st.session_state.get("auth_user", {}).get("role") or st.session_state.get("user_role") or ""
    portfolios = user_accessible_portfolios(user_email, user_role)
    if not portfolios:
        from db.control_db import control_db_available, control_schema_status, is_control_global_admin

        is_ga = bool(user_email) and control_db_available() and is_control_global_admin(user_email)
        local_admin_mode = False
        try:
            auth_cfg = getattr(st, "secrets", {}).get("auth", {})  # type: ignore
            local_admin_mode = str(auth_cfg.get("enable_local_admin_ui", "") or "").lower() in {"1", "true", "yes"}
        except Exception:
            local_admin_mode = False

        schema_ok = True
        if control_db_available():
            try:
                schema_ok = bool(control_schema_status().get("ok"))
            except Exception:
                schema_ok = False

        if is_ga or local_admin_mode or not schema_ok:
            st.info("No portfolios registered yet. Go to Admin → Portfolios.")
            try:
                if hasattr(st, "switch_page"):
                    if st.button("Open Admin → Portfolios", key="btn_open_admin_portfolios_sidebar"):
                        st.switch_page("pages/99_Admin.py")
                else:
                    st.page_link("pages/99_Admin.py", label="Open Admin → Portfolios", icon="🛠️")
            except Exception:
                pass
            st.session_state["active_portfolio_display_name"] = "No portfolio selected"
            return

        st.session_state["_app_block_reason"] = "NO_PORTFOLIO"
        return

    active_key = get_active_portfolio_key()
    if active_key and get_portfolio_ctx() is None:
        reason_now = str(st.session_state.get("_app_block_reason") or "").strip()
        if not reason_now.startswith("PORTFOLIO_DB_"):
            set_active_portfolio(active_key)
    # Manual test checklist:
    # 1. Global admin with 2+ portfolios sees ONE dropdown and can switch.
    # 2. Regular user with 1 portfolio sees NO dropdown; app auto-scopes correctly.
    # 3. Regular user with 0 portfolios gets a clear "no portfolio access" message.
    # 4. No duplicate reruns / flicker.
    if len(portfolios) == 1:
        only = portfolios[0]
        key = str(only.get("portfolio_key") or "").strip()
        display = str(only.get("display_name") or key or "Default")
        if key and key != active_key:
            set_active_portfolio(key)
        st.session_state["active_portfolio_display_name"] = display
        return

    options = {p["portfolio_key"]: p for p in portfolios if p.get("portfolio_key")}
    if not options:
        st.session_state["_app_block_reason"] = "NO_PORTFOLIO"
        return

    option_keys = list(options.keys())
    # Initialize selector state once from active portfolio.
    # Do not force-sync on every rerun, otherwise a fresh user selection can be
    # overwritten by the previous active_key before we process the change.
    cur_selector = str(st.session_state.get("portfolio_selector_sidebar") or "").strip()
    if not cur_selector and active_key in options:
        st.session_state["portfolio_selector_sidebar"] = active_key
        cur_selector = active_key
    default_index = option_keys.index(active_key) if active_key in options else 0
    sel_key = st.selectbox(
        "Portfolio",
        options=option_keys,
        format_func=lambda k: options[k].get("display_name") or k,
        index=default_index,
        key="portfolio_selector_sidebar",
    )
    if sel_key and sel_key != active_key:
        set_active_portfolio(sel_key)
        clear_portfolio_caches_and_rerun()

    selected = get_portfolio_by_key(sel_key) or options.get(sel_key) or {}
    display = str(selected.get("display_name") or sel_key or "Default")
    st.session_state["active_portfolio_display_name"] = display


def render_sidebar(pages_meta) -> None:
    with st.sidebar:
        st.session_state["debug_toggle_enable_render"] = True
        _render_debug_toggle()
        reason = st.session_state.get("_app_block_reason")

        if st.session_state.get("use_native_navigation"):
            if reason == "NOT_AUTHENTICATED":
                render_auth_sidebar()
                st.info("Sign in using the sidebar.")
                return

            if reason == "NO_PORTFOLIO":
                render_auth_sidebar()
                st.info("No portfolios registered yet. Go to Admin → Portfolios.")
                return

            if reason == "NO_PORTFOLIO_ACCESS":
                st.info("No portfolio access. Use Admin → Portfolios to onboard access.")
                render_auth_sidebar()
                return

            if reason == "CONTROL_DB_INCOMPLETE":
                st.warning("Admin setup required before using the app.")
                return

            # Native navigation: only portfolio selector + auth.
            _render_portfolio_selector_inline()
            _render_perf_diagnostics()
            render_auth_sidebar()
            return

        if reason == "NOT_AUTHENTICATED":
            render_auth_sidebar()
            st.info("Sign in using the sidebar.")
            return

        if reason == "NO_PORTFOLIO":
            render_auth_sidebar()
            st.info("No portfolios registered yet. Go to Admin → Portfolios.")
            return

        if reason == "NO_PORTFOLIO_ACCESS":
            st.info("No portfolio access. Use Admin → Portfolios to onboard access.")
            render_materials_sidebar(pages_meta)
            render_auth_sidebar()
            return

        if reason == "CONTROL_DB_INCOMPLETE":
            st.warning("Admin setup required before using the app.")
            return

        # Normal application state
        _render_portfolio_selector_inline()
        render_materials_sidebar(pages_meta)
        _render_perf_diagnostics()
        render_auth_sidebar()


def _render_perf_diagnostics() -> None:
    try:
        if not is_debug_enabled(label="Debug", key="debug_mode"):
            return
    except Exception:
        return

    with st.expander("Debug – perf diagnostics", expanded=False):
        trace_on = bool(st.session_state.get("_tco_sql_trace", False))
        st.checkbox("Enable SQL trace (this run)", value=trace_on, key="_tco_sql_trace")

        info = {}
        try:
            from utils.query_params import get_qp

            info["query_params"] = get_qp()
        except Exception:
            info["query_params"] = {}

        try:
            info["page_path"] = st.session_state.get("_tco_page_path")
            info["page_title"] = st.session_state.get("_tco_page_title")
            info["page_init_ms"] = st.session_state.get("_tco_page_init_ms")
        except Exception:
            pass

        info["run_id"] = None
        if get_script_run_ctx is not None:
            try:
                ctx = get_script_run_ctx()
                info["run_id"] = getattr(ctx, "run_id", None)
                info["script_path"] = getattr(ctx, "script_path", None)
            except Exception:
                pass

        info["active_portfolio_key"] = st.session_state.get("active_portfolio_key")
        info["active_portfolio_display_name"] = st.session_state.get("active_portfolio_display_name")
        info["portfolio_cache_key"] = st.session_state.get("_portfolio_cache_key")
        info["portfolio_changed_ts"] = st.session_state.get("portfolio_changed_ts")
        info["portfolio_access_unknown"] = st.session_state.get("_portfolio_access_unknown", False)

        user = st.session_state.get("auth_user") or {}
        info["user_email"] = user.get("email") or st.session_state.get("user_email")
        info["user_role"] = user.get("role") or st.session_state.get("role") or st.session_state.get("user_role")
        info["app_block_reason"] = st.session_state.get("_app_block_reason")
        info["use_native_navigation"] = st.session_state.get("use_native_navigation")
        info["native_nav_ready"] = st.session_state.get("_native_nav_ready")
        info["theme_css_sig"] = st.session_state.get("_tco_theme_css_sig")
        info["sidebar_nav_css_sig"] = st.session_state.get("_tco_sidebar_nav_css_sig")

        try:
            from core.portfolio_context import get_portfolio_ctx

            ctx = get_portfolio_ctx()
            if ctx:
                info["db_database"] = ctx.db_cfg.get("database") if hasattr(ctx, "db_cfg") else None
                info["db_schema"] = ctx.db_cfg.get("schema") if hasattr(ctx, "db_cfg") else None
        except Exception:
            pass

        try:
            from db.control_db import control_db_available

            info["control_db_available"] = bool(control_db_available())
        except Exception:
            pass

        info["session_state_keys"] = len(getattr(st, "session_state", {}))
        st.json(info)

        try:
            total_q = int(st.session_state.get("total_queries", 0))
            total_ms = float(st.session_state.get("total_sql_time_ms", 0.0))
            start_q = int(st.session_state.get("_tco_page_sql_start", 0))
            start_ms = float(st.session_state.get("_tco_page_sql_ms_start", 0.0))
            page_q = max(0, total_q - start_q)
            page_ms = max(0.0, total_ms - start_ms)
            avg_ms = (total_ms / total_q) if total_q else 0.0
            st.markdown("**SQL timings**")
            st.json(
                {
                    "total_queries": total_q,
                    "total_sql_time_ms": round(total_ms, 2),
                    "avg_query_ms": round(avg_ms, 2),
                    "page_queries": page_q,
                    "page_sql_time_ms": round(page_ms, 2),
                }
            )
        except Exception:
            pass


def _render_debug_toggle() -> None:
    try:
        is_debug_enabled(label="Debug", key="debug_mode")
    except Exception:
        pass
