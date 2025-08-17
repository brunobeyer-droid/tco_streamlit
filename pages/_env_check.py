import os, streamlit as st
keys = ["SNOWFLAKE_USER","SNOWFLAKE_ACCOUNT","SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA","SNOWFLAKE_ROLE"]
st.title("Env Check")
st.write({k: os.getenv(k, "<missing>") for k in keys})
