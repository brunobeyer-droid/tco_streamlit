# Summary: expose Visual Lab as an Admin page entry.
from __future__ import annotations

import streamlit as st

from core.init import init_page

from utils.app_shell import bootstrap_page
bootstrap_page()

page_theme = init_page("Visual Lab", page_path=__file__)
_ = page_theme

try:
    from viz import legacy_visual_lab_theme as _visual_lab
except Exception as e:
    st.error(f"Visual Lab failed to load: {e}")
