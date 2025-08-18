
import streamlit as st
import snowflake.connector
import os
from utils.app_refresh import refresh_all_data

# Load credentials from environment variables
conn = snowflake.connector.connect(
    user="brunobeyer",
    password="BernardoMatheus.1981",
    account="UDWQRGU-TV52099",
    warehouse="COMPUTE_WH",
    database="TCODB",
    schema="PUBLIC"
)

st.title("MOIT COST")

cur = conn.cursor()
cur.execute("SELECT CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_DATABASE();")
row = cur.fetchone()

st.write("Connected to Snowflake ✅")
st.write(f"User: {row[1]}, Database: {row[2]}")

st.set_page_config(
    page_title="TCO Calculator",
    page_icon="📊",
)

st.session_state.setdefault("db_rev", 0)

with st.sidebar:
    st.header("Actions")
    if st.button("🔄 Refresh All Data", use_container_width=True):
        refresh_all_data()
        st.success("Caches cleared. Data will be reloaded from Snowflake.")
        st.rerun()
        
st.title("Application Total Cost of Ownership (TCO) Calculator")

st.sidebar.success("Select a page above.")

st.markdown(
    """
    Welcome to the Application Total Cost of Ownership (TCO) Calculator!

    This application helps product owners estimate and manage the Total Cost of Ownership for their applications by breaking down costs by programs and teams.

    **👈 Select a page from the sidebar** to get started:

    *   **Detailed TCO Summary:** View a detailed summary of the TCO calculation results.
    *   **Summary and Edit Data:** View an overview of your data and navigate to edit sections.
    *   **Programs and Workforce:** Define your programs and the overall workforce composition.
    *   **Team Composition:** Define the teams within each program and their specific roles.
    *   **Cost Details & Calculation:** Input various cost categories and calculate the overall TCO.

    """
)


