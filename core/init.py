from __future__ import annotations

import json
import math
import numbers
import time
from contextlib import contextmanager
from typing import Any, Optional

import pandas as pd
import streamlit as st
from core.cache_utils import cache_resource_portfolio

try:
    from utils.auth import ensure_sso
except Exception:
    ensure_sso = None  # type: ignore[assignment]

try:
    from utils.sidebar import render_global_actions
except Exception:
    def render_global_actions():
        return None

from utils.theme import use_theme
from welcome.layout import apply_echarts_spotify_theme


def ensure_cost_infrastructure() -> None:
    """Ensure core cost-related DB infrastructure exists (safe to call multiple times)."""
    try:
        from db import (
            ensure_cost_events_table,
            ensure_invoice_forecast_views,
            ensure_location_and_contractor_tables,
        )
    except Exception:
        return

    for fn in (ensure_location_and_contractor_tables, ensure_cost_events_table, ensure_invoice_forecast_views):
        try:
            fn()
        except Exception:
            pass


ANALYTICS_VIEWS_VERSION = "2026-02-16_bootstrap_hotfix_v1"


def _view_has_column(fetch_df, *, view_fq: str, column_name: str) -> bool:
    try:
        view = str(view_fq or "").strip()
        col = str(column_name or "").strip()
        if not view or not col:
            return False
        # This intentionally validates the exact view reference the app will query, including schema.
        # `TOP 0` yields an empty dataframe with column metadata when successful.
        df0 = fetch_df(f"SELECT TOP 0 {col} FROM {view}")
        return df0 is not None and col in getattr(df0, "columns", [])
    except Exception:
        return False


def ensure_analytics_views() -> None:
    """Ensure DB analytics views exist and match the current demand-driver contract."""
    try:
        from db import ensure_analytics_views_ok, fetch_df, _fq  # type: ignore
    except Exception:
        st.error("Database backend is not available; cannot prepare analytics views.")
        st.stop()

    try:
        ensure_analytics_views_ok()
    except Exception as e:
        st.error(f"Analytics view build failed: {e}")
        st.stop()

    vw = _fq("VW_TCO_WORKFORCE_SPLIT")
    if not _view_has_column(fetch_df, view_fq=vw, column_name="FTE"):
        st.error(
            f"Database analytics views are out of date (missing column `FTE` in `{vw}`). "
            "Rebuild analytics views from the Admin page, or ensure the DB user has permission to create/alter views."
        )
        st.stop()

    return


@cache_resource_portfolio(show_spinner=False)
def _init_db_infrastructure(version: str) -> bool:
    """Initialize DB infrastructure once per app instance."""
    ensure_cost_infrastructure()
    ensure_analytics_views()
    return True


def init_page(title: str, *, layout: str = "wide", page_path: Optional[str] = None) -> str:
    if "_tco_run_nonce" not in st.session_state:
        st.session_state["_tco_run_nonce"] = 1
    page_start = time.perf_counter()
    st.session_state["_tco_page_path"] = str(page_path or "")
    st.session_state["_tco_page_title"] = str(title or "")
    st.session_state["_tco_page_start_ts"] = time.time()
    try:
        st.session_state["_tco_page_sql_start"] = int(st.session_state.get("total_queries", 0))
        st.session_state["_tco_page_sql_ms_start"] = float(st.session_state.get("total_sql_time_ms", 0.0))
    except Exception:
        pass
    # Set page config once per script run (Streamlit raises if called multiple times).
    if not st.session_state.get("_page_config_set"):
        st.set_page_config(page_title=f"NEXT — {title}", layout=layout)
        st.session_state["_page_config_set"] = True
    page_theme = use_theme(render_toggle=False)
    # Consistent small top spacer to avoid content feeling clipped under Streamlit chrome.
    st.markdown('<div style="height: 0.25rem;"></div>', unsafe_allow_html=True)
    # Resolve minimum role from the canonical pages catalog (keeps per-page permissions consistent),
    # without requiring every page module to call ensure_sso(...) itself.
    min_role = "VIEWER"
    try:
        from utils.pages_catalog import PAGES_CATALOG  # local import to avoid circulars

        page_ref = str(page_path or "").replace("\\", "/")
        for _section, rel_path, _t, _icon, _url_path, _min_role in PAGES_CATALOG:
            rel_ref = str(rel_path or "").replace("\\", "/")
            if rel_ref and page_ref.endswith(rel_ref):
                min_role = str(_min_role or "VIEWER").upper()
                break
    except Exception:
        min_role = "VIEWER"

    main_bootstrap_ready = bool(st.session_state.get("use_native_navigation")) and bool(
        st.session_state.get("_sidebar_rendered_by_main")
    )

    # Preflight auth before rendering sidebar so role/identity are available to nav + portfolio selector.
    # Skip when main.py already completed native-nav bootstrap for this run.
    if not main_bootstrap_ready:
        try:
            from utils.auth import preflight_auth

            preflight_auth(page_path=page_path, autostart=False)
        except Exception:
            pass

    # NOTE: pages_meta may be missing on deep-link refresh (e.g., session expired + reload on /Welcome).
    role_guess = (
        st.session_state.get("role")
        or st.session_state.get("user_role")
        or (st.session_state.get("auth_user") or {}).get("role")
        or "VIEWER"
    )
    pages_meta = st.session_state.get("_pages_meta")
    pages_meta_role = st.session_state.get("_pages_meta_role")
    if (not main_bootstrap_ready) and (
        not isinstance(pages_meta, dict) or not pages_meta or pages_meta_role != str(role_guess or "VIEWER")
    ):
        # Rebuild a minimal navigation map so the sidebar can still mount and show something.
        try:
            from utils.pages_catalog import build_navigation

            _pages_map, pages_meta = build_navigation(str(role_guess or "VIEWER"))
            st.session_state["_pages_meta"] = pages_meta
            st.session_state["_pages_meta_role"] = str(role_guess or "VIEWER")
        except Exception:
            pages_meta = {}
    if not main_bootstrap_ready:
        try:
            from utils.sidebar import render_sidebar
            render_sidebar(pages_meta if isinstance(pages_meta, dict) else {})
        except Exception:
            pass

    auth_user = st.session_state.get("auth_user")
    try:
        from utils.auth import current_user, get_current_user_email

        auth_user = current_user() or auth_user

        email = get_current_user_email()
    except Exception:
        email = st.session_state.get("user_email") or st.session_state.get("current_user_email")
    role_now = (
        st.session_state.get("role")
        or st.session_state.get("user_role")
        or (auth_user or {}).get("role")
        or "VIEWER"
    )

    if (not main_bootstrap_ready) and (not auth_user and not email):
        try:
            from utils.auth import preflight_auth, render_login_ui, current_user

            preflight_auth(page_path=page_path, autostart=True)
            auth_user = current_user() or st.session_state.get("auth_user")
            email = (
                st.session_state.get("user_email")
                or (auth_user or {}).get("email")
                or st.session_state.get("current_user_email")
            )
        except Exception:
            pass
    if not auth_user and not email:
        st.session_state["_app_block_reason"] = "NOT_AUTHENTICATED"
        rendered_sidebar = False
        try:
            from utils.auth import render_login_ui
            rendered_sidebar = bool(render_login_ui(location="sidebar"))
        except Exception:
            rendered_sidebar = False
        st.title("Sign in")
        if not rendered_sidebar:
            st.info("Sign in below.")
            try:
                from utils.auth import render_login_ui
                render_login_ui(location="main")
            except Exception:
                st.info("Sign in using the sidebar.")
        st.stop()

    if st.session_state.get("_portfolio_db_error"):
        details = st.session_state.get("_portfolio_db_error_details") or {}
        st.error(
            "Active portfolio configuration is invalid or unreachable. "
            "Open Admin → Portfolios to fix DB settings for this portfolio."
        )
        if bool(st.session_state.get("debug_mode")) and details:
            st.caption(f"Portfolio DB details: {details}")
        st.stop()

    # Restore active portfolio from URL on refresh/deep links before showing a NO_PORTFOLIO blocker.
    try:
        from core.portfolio_runtime import (
            get_active_portfolio_key,
            set_active_portfolio,
        )  # local import to avoid circulars

        if not get_active_portfolio_key():
            try:
                if hasattr(st, "query_params"):
                    qp_port = str(st.query_params.get("portfolio", "") or "").strip()  # type: ignore[attr-defined]
                else:
                    qp = st.experimental_get_query_params()  # type: ignore[attr-defined]
                    qp_port = str((qp.get("portfolio") or [""])[0] or "").strip()
            except Exception:
                qp_port = ""

            if qp_port:
                try:
                    set_active_portfolio(
                        qp_port,
                        email=str(email or ""),
                        role=str(st.session_state.get("role") or st.session_state.get("user_role") or "VIEWER"),
                    )
                    active_now = str(get_active_portfolio_key() or "").strip()
                    blocked_reason = str(st.session_state.get("_app_block_reason") or "").strip()
                    has_db_error = bool(st.session_state.get("_portfolio_db_error"))
                    if active_now == qp_port and not blocked_reason and not has_db_error:
                        st.rerun()
                except Exception as e:
                    st.session_state["_portfolio_restore_error"] = str(e)
    except Exception:
        pass

    # Only enforce role / complete SSO flows once we have a user in session.
    if ensure_sso and not main_bootstrap_ready:
        ensure_sso(min_role, page_name=title, page_path=page_path, render_sidebar=False)
    try:
        from core.portfolio_runtime import get_active_portfolio_key  # local import to avoid circulars

        has_any_portfolio = bool(st.session_state.get("active_portfolio_key") or get_active_portfolio_key())
    except Exception:
        has_any_portfolio = bool(st.session_state.get("active_portfolio_key"))
    if not has_any_portfolio:
        st.session_state.pop("_app_block_reason", None)
        if str(page_path or "").endswith("pages/99_Admin.py"):
            st.info("Select a portfolio in the sidebar to unlock full Admin tools.")
        else:
            st.title("Select a portfolio in the sidebar")
            st.info("Select a portfolio in the sidebar.")
            st.stop()

    with page_loader() as loader:
        loader.step("Loading your workspace...")
        active_key = str(st.session_state.get("active_portfolio_key") or "").strip()
        infra_marker = f"{active_key}:{ANALYTICS_VIEWS_VERSION}"
        if st.session_state.get("_tco_db_infra_marker") != infra_marker:
            _init_db_infrastructure(ANALYTICS_VIEWS_VERSION)
            st.session_state["_tco_db_infra_marker"] = infra_marker
        loader.step("Preparing analytics views...")
        loader.step("Loading reference data...")
        loader.step("Finalizing page...")
    st.session_state["_tco_page_init_ms"] = round((time.perf_counter() - page_start) * 1000, 2)
    return page_theme


def _theme_name(page_theme: str) -> str:
    return str(st.session_state.get("ui_theme", page_theme)).lower()

ENTERPRISE_LOADING_MESSAGES = [
    "Loading your workspace...",
    "Preparing analytics views...",
    "Loading reference data...",
    "Finalizing page...",
]


class PageLoader:
    def __init__(self, messages: Optional[list[str]] = None, *, show_status: bool = True) -> None:
        self._messages = messages or list(ENTERPRISE_LOADING_MESSAGES)
        self._index = 0
        self._holder = st.empty() if show_status else None
        self._container = self._holder.container() if self._holder is not None else None
        _ensure_loader_styles()
        self._status = None
        if self._container is not None:
            if hasattr(self._container, "status"):
                self._status = self._container.status(self._messages[0], expanded=False)
            else:
                self._container.info(self._messages[0])

    def step(self, label: Optional[str] = None, progress: Optional[float] = None) -> None:
        if label is None:
            if self._index < len(self._messages):
                label = self._messages[self._index]
            else:
                label = "Working..."
        self._index += 1
        if self._status is not None:
            self._status.update(label=label, state="running", expanded=False)
        elif self._container is not None:
            self._container.info(label)

    def done(self, label: str = "Ready") -> None:
        if self._status is not None:
            try:
                self._status.update(label=label, state="complete", expanded=False)
            except Exception:
                pass
        if self._holder is not None:
            self._holder.empty()


@contextmanager
def page_loader(messages: Optional[list[str]] = None, *, show_status: bool = True):
    loader = PageLoader(messages=messages, show_status=show_status)
    try:
        yield loader
    finally:
        loader.done()


def _ensure_loader_styles() -> None:
    key = "_tco_loader_style_applied"
    if st.session_state.get(key):
        return
    st.session_state[key] = True
    st.markdown(
        """
        <style>
        div[data-testid="stStatus"],
        div[data-testid="stStatus"] * ,
        div[data-testid="stAlert"],
        div[data-testid="stAlert"] * {
          border: none !important;
          box-shadow: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _sanitize_option(value: Any):
    # streamlit-echarts supports embedding JS functions by wrapping strings with a placeholder.
    # Convert wrapper objects (e.g., `streamlit_echarts.JsCode`) into the underlying string.
    try:
        js_code = getattr(value, "js_code", None)
        if isinstance(js_code, str) and js_code:
            return js_code.strip()
    except Exception:
        pass
    if isinstance(value, dict):
        return {k: _sanitize_option(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_option(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize_option(v) for v in value]
    if isinstance(value, set):
        return [_sanitize_option(v) for v in value]
    if isinstance(value, numbers.Real):
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    if isinstance(value, (pd.Series, pd.Index)):
        return [_sanitize_option(v) for v in value.tolist()]
    try:
        if not isinstance(value, str) and pd.isna(value):
            return None
    except Exception:
        pass
    return value


def render_echart(
    options: dict,
    *,
    height: str = "420px",
    key: Optional[str] = None,
    page_theme: str = "dark",
    events: Optional[dict] = None,
):
    try:
        from streamlit_echarts import st_echarts  # type: ignore
    except Exception:
        st.warning("ECharts renderer not available. Install `streamlit-echarts` to display charts.")
        return None
    opt = apply_echarts_spotify_theme(options or {}, _theme_name(page_theme))
    opt = _sanitize_option(opt)
    try:
        if events:
            return st_echarts(options=opt, height=height, width="100%", renderer="canvas", theme=None, key=key, events=events)
        return st_echarts(options=opt, height=height, width="100%", renderer="canvas", theme=None, key=key)
    except TypeError:
        if events:
            return st_echarts(options=opt, height=height, theme=None, key=key, events=events)
        return st_echarts(options=opt, height=height, theme=None, key=key)
