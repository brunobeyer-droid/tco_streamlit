from __future__ import annotations

from typing import Dict, List, Any

import streamlit as st


def _label_with_icon(title: str, icon: str) -> str:
    icon = str(icon or "").strip()
    if not icon:
        return title
    # If material icon syntax, strip it for a clean label
    if icon.startswith(":material/") and icon.endswith(":"):
        return title
    return f"{icon} {title}".strip()


def render_materials_nav(
    pages_meta: Dict[str, List[Dict[str, Any]]],
    *,
    active_portfolio_key: str,
    role: str,
) -> None:
    _ = active_portfolio_key, role
    with st.sidebar:
        st.markdown("### Materials")
        last_path = st.session_state.get("last_page_path")
        for section, items in pages_meta.items():
            if not items:
                continue
            st.caption(section)
            for item in items:
                title = str(item.get("title") or "")
                icon = str(item.get("icon") or "")
                path = str(item.get("path") or "")
                if not path:
                    continue
                label = _label_with_icon(title if title else path, icon)
                if path == last_path:
                    label = f"• {label}"
                if st.button(label, key=f"nav_{section}_{path}", use_container_width=True):
                    st.session_state["last_page_path"] = path
                    st.switch_page(path)
