# utils/sidebar.py
import streamlit as st
from utils.app_refresh import refresh_all_data

def render_global_actions():
    """
    Render a single 'Refresh All Data' button in the left sidebar.
    Safe to call from any page; it will only render once per run.
    """
    # Only render once per script run
    if st.session_state.get("_sidebar_rendered", False):
        return
    st.session_state["_sidebar_rendered"] = True

    st.session_state.setdefault("db_rev", 0)

    with st.sidebar:
        st.header("Actions")
        # Fixed key so it's stable across pages
        if st.button("🔄 Refresh All Data", use_container_width=True, key="refresh_all"):
            refresh_all_data()
            st.success("Caches cleared. Data will be reloaded from Snowflake.")
            st.rerun()
