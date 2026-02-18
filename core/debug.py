from __future__ import annotations

from typing import Optional

import streamlit as st
from contextlib import nullcontext

from core.scope import infer_role, infer_role_from_user, read_scope_from_session


def _is_admin() -> bool:
    try:
        role = st.session_state.get("role") or st.session_state.get("user_role") or ""
        if str(role or "").strip().upper() == "ADMIN":
            return True
    except Exception:
        pass

    user = st.session_state.get("auth_user") or {}
    try:
        explicit = infer_role_from_user(user) if isinstance(user, dict) else None
        key = str(explicit or "").strip().upper()
        if key == "ADMIN" or ("ADMIN" in key and key):
            return True
    except Exception:
        pass

    try:
        from db import get_user_by_email  # type: ignore

        email = str(user.get("email", "") or "").strip()
        if email:
            rec = get_user_by_email(email)
            role = (rec or {}).get("ROLE") or (rec or {}).get("role")
            if str(role or "").strip().upper() == "ADMIN":
                return True
    except Exception:
        pass

    try:
        from db import fetch_df_active as fetch_df  # type: ignore

        scope = read_scope_from_session(fetch_df)
        role = getattr(scope, "inferred_role", None) or infer_role(scope)
        return str(role or "").strip().upper() == "ADMIN"
    except Exception:
        return False


def is_debug_enabled(*, label: str = "Debug", key: str = "debug_mode") -> bool:
    """
    Global debug gating for UI-only debug panels.

    Returns True only when:
      - current user is ADMIN, and
      - admin has enabled the sidebar toggle.

    For non-admin users, this returns False and does not render any toggle.
    """
    if not _is_admin():
        return False

    if not st.session_state.get("debug_toggle_enable_render", False):
        return bool(st.session_state.get(key, False))

    # Render exactly once per script run to avoid duplicate toggles if the helper is called multiple times.
    rendered_flag = f"__debug_toggle_rendered__{key}"
    run_nonce = st.session_state.get("_tco_run_nonce")
    rendered_val = st.session_state.get(rendered_flag)
    already_rendered = (rendered_val == run_nonce) if run_nonce is not None else bool(rendered_val)
    if not already_rendered:
        st.sidebar.checkbox(label, value=bool(st.session_state.get(key, False)), key=key)
        st.session_state[rendered_flag] = run_nonce if run_nonce is not None else True

    return bool(st.session_state.get(key, False))


def _is_debug_expander_label(label: object) -> bool:
    try:
        txt = str(label or "").strip().lower()
    except Exception:
        return False
    return "debug" in txt


class _HiddenExpander:
    """Context manager that swallows rendered content by clearing a placeholder on exit."""

    def __init__(self) -> None:
        self._placeholder = None
        self._ctx = nullcontext()

    def __enter__(self):
        try:
            self._placeholder = st.empty()
            self._ctx = self._placeholder.container()
            return self._ctx.__enter__()
        except Exception:
            self._ctx = nullcontext()
            return self._ctx.__enter__()

    def __exit__(self, exc_type, exc, tb):
        try:
            self._ctx.__exit__(exc_type, exc, tb)
        finally:
            try:
                if self._placeholder is not None:
                    self._placeholder.empty()
            except Exception:
                pass
        return False


def install_debug_expander_guard() -> None:
    """
    Hide any expander containing 'debug' in its label unless debug mode is enabled.

    This is a defensive global guard to keep debug UI off for non-debug sessions
    without requiring per-page conditional blocks around every debug expander.
    """
    try:
        if getattr(st, "_tco_debug_expander_patched", False):
            return
        original = getattr(st, "expander", None)
        if original is None:
            return

        def _patched_expander(label, *args, **kwargs):
            if _is_debug_expander_label(label) and not bool(st.session_state.get("debug_mode", False)):
                return _HiddenExpander()
            return original(label, *args, **kwargs)

        setattr(st, "_tco_original_expander", original)
        setattr(st, "expander", _patched_expander)
        setattr(st, "_tco_debug_expander_patched", True)
    except Exception:
        pass
