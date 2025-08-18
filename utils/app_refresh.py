# utils/app_refresh.py
import streamlit as st

def refresh_all_data():
    """
    Global refresh: increment a shared revision and clear all @st.cache_data caches.
    Every cached loader should depend on this revision value (e.g., get_*_df(REV)).
    """
    # Initialize the key if missing
    st.session_state.setdefault("db_rev", 0)
    # Bump the revision
    st.session_state["db_rev"] += 1
    # Clear all cached data functions across the app
    try:
        st.cache_data.clear()
    except Exception:
        # Older/newer Streamlit versions: failing to clear should not break the app
        pass
