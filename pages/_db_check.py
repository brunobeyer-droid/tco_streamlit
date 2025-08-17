import streamlit as st
from snowflake_db import fetch_df, ensure_tables

st.title("DB Check")

try:
    ensure_tables()  # runs CREATE ... IF NOT EXISTS and any ALTERs you added
    info = fetch_df("SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();")
    st.write("Session:", info.iloc[0].tolist())
    st.code("""
Env/secrets the app sees:
SNOWFLAKE_DATABASE = {}
SNOWFLAKE_SCHEMA   = {}
SNOWFLAKE_WAREHOUSE= {}
""".format(
        st.secrets.get("snowflake", {}).get("database", None) if hasattr(st, "secrets") else None,
        st.secrets.get("snowflake", {}).get("schema", None) if hasattr(st, "secrets") else None,
        st.secrets.get("snowflake", {}).get("warehouse", None) if hasattr(st, "secrets") else None,
    ))
    desc = fetch_df("DESC TABLE PROGRAMS;")
    st.subheader("PROGRAMS columns")
    st.dataframe(desc)
except Exception as e:
    st.error(f"{type(e).__name__}: {e}")
