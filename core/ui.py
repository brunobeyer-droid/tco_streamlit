# Summary: update headline source chip labels to NEXT branding.
from __future__ import annotations

import base64
import html
import mimetypes
from pathlib import Path
from typing import Dict, Optional
import datetime as dt

import streamlit as st
import streamlit.components.v1 as components

from core.insights_headlines import Headline


def toast(message: str, icon: str = "✅") -> None:
    """Show a toast confirmation message with a safe fallback.

    Streamlit `st.toast` may not exist in older versions. In that case, fall back to
    `st.success` so confirmation feedback still appears.
    """
    body = str(message or "")
    try:
        st.toast(body, icon=icon)
    except Exception:
        try:
            st.success(body)
        except Exception:
            st.write(body)


def touch_last_updated_status(tag: str) -> str:
    """Update + return a compact last-updated label for header scope rows."""
    key = f"_last_loaded_at_{str(tag or 'page').strip().lower()}"
    now = dt.datetime.now().strftime("%H:%M:%S")
    st.session_state[key] = now
    return f"Last updated: {now}"


def render_last_updated_status(tag: str) -> None:
    """Backward-compatible status renderer (standalone caption)."""
    st.caption(touch_last_updated_status(tag))


def _logo_data_uri(path: str) -> Optional[str]:
    try:
        blob = Path(path).read_bytes()
    except Exception:
        return None
    mime, _ = mimetypes.guess_type(path)
    if Path(path).suffix.lower() == ".svg":
        mime = "image/svg+xml"
    mime = mime or "application/octet-stream"
    encoded = base64.b64encode(blob).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def render_page_header(
    title: str,
    subtitle: str,
    scope_label: str,
    role_label: str,
    *,
    right_logo_path: Optional[str] = None,
    right_logo_ratio: float = 0.15,
    right_logo_size_px: Optional[int] = None,
    right_logo_vertical_alignment: str = "top",
    scope_status_right: Optional[str] = None,
) -> None:
    st.markdown(
        """<style>
.tco-header-logo-noshrink { flex: 0 0 auto; overflow: visible; }
.tco-header-logo-noshrink img { max-width: none !important; }
</style>""",
        unsafe_allow_html=True,
    )
    if right_logo_path:
        valign = str(right_logo_vertical_alignment or "top").lower().strip()
        if valign not in {"top", "center", "bottom"}:
            valign = "top"

        cols = st.columns([1 - right_logo_ratio, right_logo_ratio], vertical_alignment=valign)
        with cols[0]:
            st.title(title)
            st.caption(subtitle)
        with cols[1]:
            if right_logo_size_px:
                data_uri = _logo_data_uri(right_logo_path)
                if data_uri:
                    size_px = int(right_logo_size_px)
                    align_items = "center" if valign == "center" else ("flex-end" if valign == "bottom" else "flex-start")
                    st.markdown(
                        f"""
<div style="display:flex;justify-content:flex-end;align-items:{align_items};">
  <div class="tco-header-logo-noshrink" style="width:{size_px}px; padding-bottom:18px;">
    <img src="{data_uri}" style="width:100%;height:auto;object-fit:contain;display:block;" />
  </div>
</div>
                        """,
                        unsafe_allow_html=True,
                    )
                else:
                    st.image(right_logo_path, width=int(right_logo_size_px))
            else:
                st.image(right_logo_path, use_container_width=True)
    else:
        st.title(title)
        st.caption(subtitle)

    # Row 2: scope context only (do not display role in header UI).
    base_role = str(st.session_state.get("_role_base_effective") or "").strip().upper()
    scope_preview_email = str(st.session_state.get("_admin_scope_preview_email") or "").strip().lower()
    if "@" in scope_preview_email:
        _lp, _dp = scope_preview_email.split("@", 1)
        scope_preview_email_masked = f"{(_lp[:3] if _lp else '')}***@{_dp}"
    else:
        scope_preview_email_masked = scope_preview_email
    scope_preview_active = base_role == "ADMIN" and bool(scope_preview_email)
    scope_badge_html = (
        (
            "<span style='display:inline-block; margin-left:8px; padding:2px 8px; "
            "border-radius:999px; font-size:0.75rem; font-weight:600; "
            "background:rgba(14,165,233,0.15); color:#38bdf8; border:1px solid rgba(56,189,248,0.35);'>"
            f"Scope preview as: {html.escape(scope_preview_email_masked)}</span>"
        )
        if scope_preview_active
        else ""
    )
    label = str(scope_label or "").strip()
    if label.lower().startswith("viewing:"):
        label = label.split(":", 1)[-1].strip() or label
    if str(scope_status_right or "").strip():
        sc1, sc2 = st.columns([0.72, 0.28], vertical_alignment="center")
        with sc1:
            st.markdown(f"**{label}**{scope_badge_html}", unsafe_allow_html=True)
        with sc2:
            st.markdown(
                (
                    "<div style='text-align:right; font-size:0.82rem; "
                    "color:rgba(255,255,255,0.65); margin-top:0.15rem;'>"
                    f"{html.escape(str(scope_status_right))}</div>"
                ),
                unsafe_allow_html=True,
            )
    else:
        st.markdown(f"**{label}**{scope_badge_html}", unsafe_allow_html=True)

    # If a fixed-size right logo is used, keep the header row from shrinking/clipping on narrow widths.
    if right_logo_path and right_logo_size_px:
        size_px = int(right_logo_size_px)
        components.html(
            f"""
<script>
(function() {{
  const doc = window.parent.document;
  function apply() {{
    const logo = doc.querySelector('.tco-header-logo-noshrink');
    if (!logo) return false;
    let col = logo;
    while (col && !(col.classList && col.classList.contains('stColumn'))) col = col.parentElement;
    if (!col) return false;
    let row = col;
    while (row && !(row.classList && row.classList.contains('stHorizontalBlock'))) row = row.parentElement;
    if (!row) return false;

    // Avoid creating a scrollable header row (mouse wheel can get "trapped" here).
    // Prefer wrapping the logo beneath the title on narrow screens instead.
    row.style.flexWrap = 'wrap';
    row.style.overflowX = 'visible';
    row.style.overflowY = 'visible';
    row.style.maxWidth = '100%';

    col.style.flex = '0 0 auto';
    col.style.minWidth = '{size_px}px';
    col.style.overflow = 'visible';
    return true;
  }}

  let tries = 0;
  const timer = setInterval(() => {{
    tries += 1;
    try {{
      if (apply() || tries > 20) clearInterval(timer);
    }} catch (e) {{
      if (tries > 20) clearInterval(timer);
    }}
  }}, 150);
}})();
</script>
""",
            height=1,
        )


def _switch_page(path: str) -> bool:
    try:
        st.switch_page(path)
        return True
    except Exception:
        return False


def render_next_actions(role: str, scope: Dict[str, str]) -> None:
    st.subheader("Next actions")
    cols = st.columns(4)
    buttons = [
        ("Go to Dashboard", "pages/1_Dashboard.py"),
        ("Go to Insights", "pages/1_Insights.py"),
        ("Go to Data Quality", "pages/11_Data_Quality.py"),
        ("Go to Invoices", "pages/2_Invoices.py"),
    ]
    for col, (label, path) in zip(cols, buttons):
        with col:
            if st.button(label, use_container_width=True):
                if not _switch_page(path):
                    st.info(f"Open `{path}` from the sidebar.")

    st.markdown("**Helpful links**")
    st.page_link("pages/0_Welcome.py", label="Welcome")
    st.page_link("pages/1_Dashboard.py", label="Dashboard")
    st.page_link("pages/1_Insights.py", label="Insights")
    st.page_link("pages/11_Data_Quality.py", label="Data Quality")
    st.page_link("pages/2_Invoices.py", label="Invoices")


_HEADLINE_SOURCE_COLORS: dict[str, str] = {
    "NEXT (WF+NWF)": "#2563eb",
    "NEXT Forecast": "#f59e0b",
    "Apptio": "#16a34a",
    "ADO": "#7c3aed",
    "NEXT + ADO": "#0ea5e9",
    "Apptio + NEXT Forecast": "#0891b2",
    "Apptio + ADO": "#0d9488",
}


def _source_chip(label: str) -> str:
    text = html.escape(str(label or "").strip())
    color = _HEADLINE_SOURCE_COLORS.get(str(label or "").strip(), "#64748b")
    return (
        "<span style='display:inline-block; padding:2px 8px; border-radius:10px; "
        f"font-size:11px; background:{color}; color:#fff; margin-right:6px;'>{text}</span>"
    )


def _headline_help_lines(headline: Headline) -> list[str]:
    mode = str(headline.mode or "").strip().lower()
    driver_dim = str(headline.driver_dim or "").strip().upper()
    driver_lens = "Programs" if driver_dim == "PROGRAM" else ("Teams" if driver_dim == "TEAM" else "Applications")

    lines: list[str] = []
    if mode == "confidence":
        lines.append("What this means: forecast inputs are incomplete.")
    elif mode == "trend":
        lines.append("What this means: forecast is moving vs plan.")
    else:
        lines.append("What this means: forecast is broadly aligned to plan.")

    lines.append(f"Driver lens: {driver_lens}.")
    lines.append("Concentrated = one driver explains ≥40% of change.")
    if headline.sources:
        lines.append("Sources: " + ", ".join([str(s) for s in headline.sources if str(s).strip()]))
    return lines


def render_headline_with_help(headline: Headline) -> None:
    """Render a headline with a reusable help panel."""
    cols = st.columns([24, 1], vertical_alignment="center")
    with cols[0]:
        st.markdown(f"**{headline.text}**")
    with cols[1]:
        popover = getattr(st, "popover", None)
        if callable(popover):
            with popover("ℹ️"):
                st.markdown("**What does this insight mean?**")
                for line in _headline_help_lines(headline):
                    st.markdown(f"- {line}")
                if headline.sources:
                    chips = "".join([_source_chip(s) for s in headline.sources if str(s).strip()])
                    if chips:
                        st.markdown(chips, unsafe_allow_html=True)
        else:
            with st.expander("ℹ️ What does this insight mean?", expanded=False):
                for line in _headline_help_lines(headline):
                    st.markdown(f"- {line}")
                if headline.sources:
                    chips = "".join([_source_chip(s) for s in headline.sources if str(s).strip()])
                    if chips:
                        st.markdown(chips, unsafe_allow_html=True)
