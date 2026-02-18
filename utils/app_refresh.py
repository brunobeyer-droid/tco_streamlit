# utils/app_refresh.py
import streamlit as st
from db import ensure_tables
from core.freshness import post_write_refresh

# Initialize schema once per session
if "_tco_init" not in st.session_state:
    ensure_tables()
    st.session_state["_tco_init"] = True


def refresh_all_data():
    """
    Legacy compatibility helper for manual refresh actions.
    Uses centralized freshness flow instead of direct global cache clears.
    """
    post_write_refresh("legacy_refresh_all_data", ensure_views=False, rerun=False, bump_version=False)
