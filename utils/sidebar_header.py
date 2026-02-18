from __future__ import annotations

from typing import Any, Dict, List

import streamlit as st

from core.portfolio_runtime import (
    user_accessible_portfolios,
    get_active_portfolio_key,
    get_portfolio_by_key,
    set_active_portfolio,
    clear_portfolio_caches_and_rerun,
)
from core.portfolio_context import get_portfolio_ctx


def _render_portfolio_selector_inner() -> None:
    user_email = (
        st.session_state.get("auth_user", {}).get("email")
        or st.session_state.get("user_email")
        or ""
    )
    user_role = (
        st.session_state.get("auth_user", {}).get("role")
        or st.session_state.get("user_role")
        or ""
    )
    portfolios = user_accessible_portfolios(user_email, user_role)
    if not portfolios:
        from db.control_db import control_db_available, control_schema_status, is_control_global_admin

        is_ga = bool(user_email) and control_db_available() and is_control_global_admin(user_email)
        local_admin_mode = False
        try:
            auth_cfg = getattr(st, "secrets", {}).get("auth", {})  # type: ignore
            local_admin_mode = str(auth_cfg.get("enable_local_admin_ui", "") or "").lower() in {
                "1",
                "true",
                "yes",
            }
        except Exception:
            local_admin_mode = False

        schema_ok = True
        if control_db_available():
            try:
                schema_ok = bool(control_schema_status().get("ok"))
            except Exception:
                schema_ok = False

        if is_ga or local_admin_mode or not schema_ok:
            st.sidebar.info("No portfolios registered yet. Go to Admin → Portfolios.")
            st.session_state["active_portfolio_display_name"] = "No portfolio selected"
            return

        st.sidebar.warning("No portfolio access. Contact admin.")
        st.stop()

    active_key = get_active_portfolio_key()
    if active_key and get_portfolio_ctx() is None:
        set_active_portfolio(active_key)
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
        st.sidebar.warning("No portfolio access. Contact admin.")
        st.stop()
    option_keys = list(options.keys())
    default_index = 0
    if active_key in options:
        default_index = option_keys.index(active_key)
    sel_key = st.sidebar.selectbox(
        "Portfolio",
        options=option_keys,
        format_func=lambda k: options[k].get("display_name") or k,
        index=default_index,
        key="portfolio_selector",
    )
    if sel_key and sel_key != active_key:
        set_active_portfolio(sel_key)
        clear_portfolio_caches_and_rerun()

    selected = get_portfolio_by_key(sel_key) or options.get(sel_key) or {}
    display = str(selected.get("display_name") or sel_key or "Default")
    st.session_state["active_portfolio_display_name"] = display


def render_portfolio_selector() -> None:
    # Ensure this UI always renders in the sidebar, even if called from main/page contexts.
    with st.sidebar:
        _render_portfolio_selector_inner()
