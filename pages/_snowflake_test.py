import streamlit as st
from snowflake_db import fetch_df

st.title("Snowflake Connection Test")
try:
  df = fetch_df("SELECT CURRENT_ACCOUNT() acct, CURRENT_USER() usr, CURRENT_DATABASE() db, CURRENT_SCHEMA() sch;")
  st.success("Connected to Snowflake ✅")
  st.dataframe(df)
except Exception as e:
  st.error(f"Connection failed: {e}")
