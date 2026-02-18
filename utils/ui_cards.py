from __future__ import annotations

import html
import re
from typing import Callable, Optional

import streamlit as st


def _sanitize_key(raw: str) -> str:
    key = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(raw or "").strip())
    return key.strip("-") or "chart"


def render_chart_card(
    title: str,
    subtitle: Optional[str],
    *,
    body_height_px: int,
    key: str,
    render_body: Callable[[], None],
    render_head_right: Optional[Callable[[], None]] = None,
) -> None:
    """Render a reusable chart shell with consistent border/padding across pages."""
    css_flag = "_chart_card_shell_css_loaded"
    if not st.session_state.get(css_flag):
        st.session_state[css_flag] = True
        st.markdown(
            """
            <style>
            .chart-card {
              border: 1px solid rgba(255,255,255,0.12);
              border-radius: 11px;
              overflow: hidden;
              background: rgba(0,0,0,0.06);
              box-shadow: 0 8px 18px rgba(2,6,23,0.16), 0 1px 0 rgba(255,255,255,0.03) inset;
            }
            .chart-card__head { padding: 12px 14px 8px 14px; }
            .chart-card__title-row { display:flex; align-items:center; justify-content:space-between; gap:10px; }
            .chart-card__title {
              margin: 0;
              font-size: 2.3rem;
              line-height: 1.1;
              font-weight: 700;
              color: rgba(248,250,252,0.96);
            }
            .chart-card__subtitle {
              opacity: 0.75;
              margin-top: 4px;
              margin-bottom: 0;
              font-size: 1rem;
              font-weight: 600;
              color: rgba(248,250,252,0.88);
            }
            .chart-card__body { padding: 0 10px 10px 10px; }
            .chart-card iframe { display: block; }
            </style>
            """,
            unsafe_allow_html=True,
        )

    safe_key = _sanitize_key(key)
    subtitle_html = (
        f"<p class='chart-card__subtitle'>{html.escape(str(subtitle or '').strip())}</p>"
        if str(subtitle or "").strip()
        else ""
    )
    title_text = html.escape(str(title or "").strip())
    has_head = bool(title_text or subtitle_html)

    if has_head and render_head_right is None:
        head_html = (
            "<div class='chart-card__head'>"
            "<div class='chart-card__title-row'>"
            f"<h3 class='chart-card__title'>{title_text}</h3>"
            "</div>"
            f"{subtitle_html}"
            "</div>"
        )
        st.markdown(
            (
                "<div class='chart-card' data-chart-card-key='"
                f"{safe_key}"
                "'>"
                f"{head_html}"
                "<div class='chart-card__body'>"
            ),
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            (
                "<div class='chart-card' data-chart-card-key='"
                f"{safe_key}"
                "'>"
            ),
            unsafe_allow_html=True,
        )
        if has_head:
            st.markdown("<div class='chart-card__head'>", unsafe_allow_html=True)
            head_left_col, head_right_col = st.columns([0.72, 0.28], gap="small", vertical_alignment="top")
            with head_left_col:
                if title_text:
                    st.markdown(f"<h3 class='chart-card__title'>{title_text}</h3>", unsafe_allow_html=True)
                if subtitle_html:
                    st.markdown(subtitle_html, unsafe_allow_html=True)
            with head_right_col:
                if render_head_right is not None:
                    render_head_right()
            st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("<div class='chart-card__body'>", unsafe_allow_html=True)
    try:
        _ = int(body_height_px)
        with st.container(border=False):
            render_body()
    finally:
        st.markdown("</div></div>", unsafe_allow_html=True)
