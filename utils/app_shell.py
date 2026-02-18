from __future__ import annotations

import streamlit as st
import time
import os


def _is_safe_mode() -> bool:
    try:
        if hasattr(st, "query_params"):
            return str(st.query_params.get("safe", "0") or "0") == "1"  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        qp = st.experimental_get_query_params()  # type: ignore[attr-defined]
        return str((qp.get("safe") or ["0"])[0] or "0") == "1"
    except Exception:
        return False


def bootstrap_page() -> None:
    trace_on = str(os.getenv("TCO_STARTUP_TRACE", "0")).strip().lower() in {"1", "true", "yes", "on"}
    t0 = time.perf_counter()
    last = t0

    def _trace(label: str) -> None:
        nonlocal last
        if not trace_on:
            return
        now = time.perf_counter()
        try:
            arr = st.session_state.setdefault("_startup_trace_bootstrap", [])
            if isinstance(arr, list):
                arr.append(
                    {
                        "label": str(label),
                        "since_start_ms": round((now - t0) * 1000.0, 1),
                        "step_ms": round((now - last) * 1000.0, 1),
                    }
                )
                if len(arr) > 80:
                    del arr[:-80]
        except Exception:
            pass
        last = now

    if _is_safe_mode():
        return

    # When pages are launched via main.py native navigation, core auth/routing/sidebar
    # work has already been done. Skip repeated bootstrap logic in page scripts.
    try:
        if bool(st.session_state.get("_sidebar_rendered_by_main")):
            try:
                from core.debug import install_debug_expander_guard
                install_debug_expander_guard()
            except Exception:
                pass
            _trace("main_sidebar_fastpath")
            return
    except Exception:
        pass
    _trace("begin_full_bootstrap")

    try:
        from core.portfolio_context import get_portfolio_ctx

        active_key = str(st.session_state.get("active_portfolio_key") or "").strip()
        ctx = get_portfolio_ctx()
        ctx_key = str(getattr(ctx, "portfolio_key", "") or "").strip() if ctx is not None else ""
        if active_key and ctx_key and active_key != ctx_key:
            # Fail closed on context mismatch to avoid showing data from a previous portfolio.
            st.session_state.pop("portfolio_ctx", None)
            st.session_state["portfolio_changed_ts"] = time.time()
            # Avoid forced rerun loops during normal navigation; DB helpers already
            # prefer active portfolio runtime config and will rehydrate context lazily.
    except Exception:
        pass
    _trace("portfolio_ctx_check")

    try:
        from core.debug import install_debug_expander_guard

        install_debug_expander_guard()
    except Exception:
        pass
    _trace("debug_guard")

    try:
        # Optional auto-invalidation via DATA_VERSION polling.
        # Disabled by default to keep page startup responsive.
        dv_poll_enabled = str(os.getenv("TCO_ENABLE_DV_POLL", "0")).strip().lower() in {"1", "true", "yes", "on"}
        dv_poll_interval = float(os.getenv("TCO_DV_POLL_INTERVAL_SEC", "60") or "60")
        if dv_poll_enabled and dv_poll_interval > 0:
            now = time.time()
            last_check = float(st.session_state.get("_dv_check_ts") or 0.0)
            if now - last_check >= dv_poll_interval:
                st.session_state["_dv_check_ts"] = now
                active_key = str(st.session_state.get("active_portfolio_key") or "").strip() or "__default__"
                seen = st.session_state.setdefault("_dv_seen_by_portfolio", {})
                if not isinstance(seen, dict):
                    seen = {}
                    st.session_state["_dv_seen_by_portfolio"] = seen
                try:
                    from db import get_data_version_info

                    dv_info = get_data_version_info() or {}
                    dv = int(dv_info.get("version") or 0)
                    prev = seen.get(active_key)
                    if prev is None:
                        seen[active_key] = dv
                    elif dv > int(prev):
                        try:
                            st.session_state["_cache_epoch"] = int(st.session_state.get("_cache_epoch", 0) or 0) + 1
                        except Exception:
                            pass
                        # Clear session-scoped cost model cache used by core pages
                        # (it is not affected by st.cache_data/resource clear).
                        for k in (
                            "COST_MODEL",
                            "COST_MODEL_KEY",
                            "COST_MODEL_CACHE",
                            "COST_MODEL_DEBUG_SCENARIOS",
                            "COST_MODEL_DEBUG_COUNTS",
                        ):
                            try:
                                if k in st.session_state:
                                    del st.session_state[k]
                            except Exception:
                                pass
                        seen[active_key] = dv
                except Exception:
                    # Non-fatal in setup/control-only contexts.
                    pass
    except Exception:
        pass
    _trace("dv_poll_block")

    try:
        from utils.theme import use_theme, apply_sidebar_nav_compact_style

        use_theme(render_toggle=False, default="light")
        apply_sidebar_nav_compact_style()
    except Exception:
        pass
    _trace("theme")

    try:
        from utils.auth import _try_complete_authcode, _try_front_channel_logout
        has_auth_qp = False
        try:
            if hasattr(st, "query_params"):
                qp = st.query_params  # type: ignore[attr-defined]
                has_auth_qp = bool(qp.get("code") or qp.get("state") or qp.get("logout"))
            else:
                qp = st.experimental_get_query_params()  # type: ignore[attr-defined]
                has_auth_qp = bool(
                    (qp.get("code") or [""])[0]
                    or (qp.get("state") or [""])[0]
                    or (qp.get("logout") or [""])[0]
                )
        except Exception:
            has_auth_qp = False

        if has_auth_qp:
            _try_front_channel_logout()
            try:
                _try_complete_authcode()
            except Exception:
                pass
    except Exception:
        pass
    _trace("auth_qp_handler")

    role = str(
        st.session_state.get("role")
        or st.session_state.get("user_role")
        or (st.session_state.get("auth_user") or {}).get("role")
        or ""
    ).upper()

    pages_meta = {}
    try:
        from utils.pages_catalog import build_navigation

        _pages_map, pages_meta = build_navigation(role)
        st.session_state["_pages_meta"] = pages_meta
    except Exception:
        pages_meta = st.session_state.get("_pages_meta") or {}
    _trace("build_navigation")

    try:
        from utils.sidebar import render_sidebar

        render_sidebar(pages_meta if isinstance(pages_meta, dict) else {})
    except Exception:
        pass
    _trace("render_sidebar")
