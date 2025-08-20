# utils/app_refresh.py
import streamlit as st
from snowflake_db import ensure_tables

# Initialize schema once per session
if "_tco_init" not in st.session_state:
    ensure_tables()
    st.session_state["_tco_init"] = True


def refresh_all_data():
    """
    Global refresh: increment a shared revision and clear all @st.cache_data caches.
    Every cached loader should depend on this revision value (e.g., get_*_df(REV)).
    """
    st.session_state.setdefault("db_rev", 0)
    st.session_state["db_rev"] += 1
    try:
        st.cache_data.clear()
    except Exception:
        # Older/newer Streamlit versions: failing to clear should not break the app
        pass
