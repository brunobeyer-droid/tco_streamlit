# utils/sidebar.py
import streamlit as st

def render_global_actions():
    with st.sidebar:
        st.header("Global")
        # Optional: add a refresh button to clear cached data
        if st.button("🔄 Refresh All Data"):
            st.cache_data.clear()
            st.success("Caches cleared.")
