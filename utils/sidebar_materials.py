from __future__ import annotations

from typing import Any, Dict, List

import streamlit as st


def render_materials_sidebar(pages_meta: Dict[str, List[Dict[str, Any]]], *, title: str = "Materials") -> None:
    _ = title
    if not pages_meta:
        st.info("No pages available for this role/portfolio.")
        return

    for section, items in pages_meta.items():
        if not items:
            continue
        with st.expander(section, expanded=True):
            for page in items:
                path = str(page.get("path") or "").strip()
                if not path:
                    continue
                label = str(page.get("title") or path)
                icon = page.get("icon") or None
                st.page_link(path, label=label, icon=icon)
