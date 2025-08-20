import streamlit as st
import pandas as pd
from snowflake_db import fetch_df, execute, ensure_ado_tables

st.set_page_config(page_title="TCO Setup: Rates, Capacity & Mappings", layout="wide")
st.title("⚙️ TCO Setup: FTE Rates, Team Capacity & ADO Mappings")

# --- Bootstrap ADO tables to avoid 'does not exist' errors ---
with st.spinner("Bootstrapping ADO tables…"):
    ensure_ado_tables()

# --- Quick diagnostics (DB/Schema/Role and table visibility) ---
diag = fetch_df("""
    SELECT current_role() AS ROLE,
           current_warehouse() AS WH,
           current_database() AS DB,
           current_schema() AS SCH
""").reset_index(drop=True)
cdb = f"{diag.iloc[0]['DB']}.{diag.iloc[0]['SCH']}" if not diag.empty else "(unknown)"

cols = st.columns(4)
cols[0].metric("Role", str(diag.iloc[0]["ROLE"]) if not diag.empty else "n/a")
cols[1].metric("Warehouse", str(diag.iloc[0]["WH"]) if not diag.empty else "n/a")
cols[2].metric("Database", str(diag.iloc[0]["DB"]) if not diag.empty else "n/a")
cols[3].metric("Schema", str(diag.iloc[0]["SCH"]) if not diag.empty else "n/a")

exists = fetch_df("""
SELECT LISTAGG(TABLE_NAME, ', ') AS TBLs
FROM (
  SELECT TABLE_NAME
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_NAME IN ('FTE_RATES','TEAM_CAPACITY','MAP_ADO_TEAM_TO_TCO_TEAM','MAP_ADO_APP_TO_TCO_GROUP','TEAM_FTE_COMPOSITION')
)
""")
st.caption(f"Visible in {cdb}: " + (exists.iloc[0]["TBLS"] if not exists.empty and exists.iloc[0]["TBLS"] else "(none)"))

def save_truncate_insert(df: pd.DataFrame, table: str, cols: list[str]):
    df = (df.copy() if df is not None else pd.DataFrame(columns=cols))
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].drop_duplicates().reset_index(drop=True)
    execute(f"TRUNCATE TABLE {table}")
    if df.empty:
        return
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    params = [tuple(None if pd.isna(v) else v for v in row) for _, row in df.iterrows()]
    execute(sql, params, many=True)

# 1) FTE Rates per PI
st.header("1) FTE Rates per PI")
rates = fetch_df("""
    SELECT EMPLOYEE_TYPE, EFFECTIVE_FROM, RATE_PER_PI
    FROM FTE_RATES
    ORDER BY EMPLOYEE_TYPE, EFFECTIVE_FROM DESC
""").reset_index(drop=True)
rates_edit = st.data_editor(rates, num_rows="dynamic", use_container_width=True, key="rates")
if st.button("💾 Save FTE Rates"):
    save_truncate_insert(rates_edit, "FTE_RATES", ["EMPLOYEE_TYPE","EFFECTIVE_FROM","RATE_PER_PI"])
    st.success("Saved FTE rates.")

st.divider()

# 2) Team Capacity
st.header("2) Team Capacity (Points per FTE per PI)")
cap = fetch_df("""
    SELECT TEAMID, EFFECTIVE_FROM, POINTS_PER_FTE_PER_PI
    FROM TEAM_CAPACITY
    ORDER BY TEAMID, EFFECTIVE_FROM DESC
""").reset_index(drop=True)
cap_edit = st.data_editor(cap, num_rows="dynamic", use_container_width=True, key="cap")
if st.button("💾 Save Team Capacity"):
    save_truncate_insert(cap_edit, "TEAM_CAPACITY", ["TEAMID","EFFECTIVE_FROM","POINTS_PER_FTE_PER_PI"])
    st.success("Saved team capacity.")

st.divider()
c1, c2 = st.columns(2)

with c1:
    st.subheader("3) Map ADO Team → TCO TEAMID")
    mteam = fetch_df("""
        SELECT ADO_TEAM, TEAMID
        FROM MAP_ADO_TEAM_TO_TCO_TEAM
        ORDER BY ADO_TEAM
    """).reset_index(drop=True)
    mteam_edit = st.data_editor(mteam, num_rows="dynamic", use_container_width=True, key="mteam")
    if st.button("💾 Save Team Mapping"):
        save_truncate_insert(mteam_edit, "MAP_ADO_TEAM_TO_TCO_TEAM", ["ADO_TEAM","TEAMID"])
        st.success("Saved team mapping.")

with c2:
    st.subheader("4) Map ADO Application → TCO App Group")
    mapp = fetch_df("""
        SELECT ADO_APP, APP_GROUP
        FROM MAP_ADO_APP_TO_TCO_GROUP
        ORDER BY ADO_APP
    """).reset_index(drop=True)
    mapp_edit = st.data_editor(mapp, num_rows="dynamic", use_container_width=True, key="mapp")
    if st.button("💾 Save App Mapping"):
        save_truncate_insert(mapp_edit, "MAP_ADO_APP_TO_TCO_GROUP", ["ADO_APP","APP_GROUP"])
        st.success("Saved app mapping.")

st.divider()
st.header("5) Team FTE Composition (for blended rate)")
comp = fetch_df("""
    SELECT TEAMID, ROLE_NAME, EMPLOYEE_TYPE, FTE_COUNT
    FROM TEAM_FTE_COMPOSITION
    ORDER BY TEAMID, ROLE_NAME, EMPLOYEE_TYPE
""").reset_index(drop=True)
comp_edit = st.data_editor(comp, num_rows="dynamic", use_container_width=True, key="comp")
if st.button("💾 Save FTE Composition"):
    save_truncate_insert(comp_edit, "TEAM_FTE_COMPOSITION", ["TEAMID","ROLE_NAME","EMPLOYEE_TYPE","FTE_COUNT"])
    st.success("Saved team composition.")
