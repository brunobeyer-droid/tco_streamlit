from __future__ import annotations

from typing import Optional, Dict, Any

import streamlit as st

from utils.theme import _merged_theme


def _ensure_toast_css(theme: Dict[str, Any], *, force: bool = False) -> None:
    bg = str(theme.get("toast_bg") or "")
    border = str(theme.get("toast_border") or "")
    text = str(theme.get("toast_text") or "")
    sig = f"{bg}|{border}|{text}"
    if not force and st.session_state.get("_tco_toast_css_sig") == sig:
        return
    css = f"""<style>
div[data-baseweb="toast"][data-testid="stToast"],
div[data-baseweb="toast"],
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"],
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[role="alert"][data-baseweb="toast"] {{
  background: {bg} !important;
  background-color: {bg} !important;
  border: 1px solid {border} !important;
  color: {text} !important;
  opacity: 1 !important;
  filter: none !important;
}}
div[data-baseweb="toast"][data-testid="stToast"]::before,
div[data-baseweb="toast"][data-testid="stToast"]::after,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"]::before,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"]::after {{
  background: {bg} !important;
  background-color: {bg} !important;
}}
div[data-baseweb="toast"][data-testid="stToast"] > div,
div[data-baseweb="toast"][data-testid="stToast"] > div > div,
div[data-baseweb="toast"][data-testid="stToast"] > div > div > div,
div[data-baseweb="toast"][data-testid="stToast"] div[data-testid="stMarkdownContainer"],
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] > div,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] > div > div,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] > div > div > div,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] div[data-testid="stMarkdownContainer"] {{
  background: {bg} !important;
  background-color: {bg} !important;
  color: {text} !important;
}}
div[data-baseweb="toast"][data-testid="stToast"] *,
div[data-baseweb="toast"][data-testid="stToast"] svg,
div[data-baseweb="toast"][data-testid="stToast"] svg path,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] *,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] svg,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] svg path {{
  color: {text} !important;
  fill: {text} !important;
}}
</style>"""
    st.markdown(css, unsafe_allow_html=True)
    st.session_state["_tco_toast_css_sig"] = sig


def _theme_overrides() -> Optional[Dict[str, Any]]:
    if st.session_state.get("theme_overrides_live_enabled") and isinstance(st.session_state.get("theme_overrides_live"), dict):
        return st.session_state.get("theme_overrides_live")
    return None


def _toast_prefix(theme_key: str, fallback: str) -> str:
    theme = _merged_theme(_theme_overrides())
    prefix = str(theme.get(theme_key, fallback) or "").strip()
    if not prefix:
        return ""
    # Toast "icon" parameter only accepts emoji. To keep the app monochrome,
    # we render a 1-char prefix inside the toast body instead.
    return prefix[0]


def _toast_body(message: str, prefix: str) -> str:
    msg = str(message or "")
    if prefix:
        return f"{prefix} {msg}"
    return msg


def toast_success(message: str, *, duration: int = 8) -> None:
    theme = _merged_theme(_theme_overrides())
    body = _toast_body(message, _toast_prefix("toast_icon_success", "✓"))
    try:
        st.toast(body, duration=duration)
    except TypeError:
        st.toast(body)
    _ensure_toast_css(theme, force=True)


def toast_error(message: str, *, duration: int = 8) -> None:
    theme = _merged_theme(_theme_overrides())
    body = _toast_body(message, _toast_prefix("toast_icon_error", "×"))
    try:
        st.toast(body, duration=duration)
    except TypeError:
        st.toast(body)
    _ensure_toast_css(theme, force=True)


def toast_warning(message: str, *, duration: int = 8) -> None:
    theme = _merged_theme(_theme_overrides())
    body = _toast_body(message, _toast_prefix("toast_icon_warning", "!"))
    try:
        st.toast(body, duration=duration)
    except TypeError:
        st.toast(body)
    _ensure_toast_css(theme, force=True)


def toast_info(message: str, *, duration: int = 8) -> None:
    theme = _merged_theme(_theme_overrides())
    body = _toast_body(message, _toast_prefix("toast_icon_info", "i"))
    try:
        st.toast(body, duration=duration)
    except TypeError:
        st.toast(body)
    _ensure_toast_css(theme, force=True)
