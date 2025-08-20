import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake_db import fetch_df

st.set_page_config(page_title="TCO Dashboard", layout="wide")
st.title("📊 TCO Dashboard (Working + Non‑Working)")

programs = fetch_df("SELECT PROGRAMID, PROGRAMNAME FROM PROGRAMS ORDER BY PROGRAMNAME").reset_index(drop=True)
teams    = fetch_df("SELECT TEAMID, TEAMNAME, PROGRAMID FROM TEAMS ORDER BY TEAMNAME").reset_index(drop=True)

colf1, colf2 = st.columns(2)
program_pick = colf1.multiselect("Program(s)", programs["PROGRAMNAME"].tolist())
team_pick    = colf2.multiselect("Team(s)", teams["TEAMNAME"].tolist())

# Working Force (from estimate table)
wf_sql = """
SELECT
  e.FEATURE_ID, e.TITLE, e.STATE,
  e.TEAMID, t.TEAMNAME,
  COALESCE(ag.PROGRAMID, t.PROGRAMID) AS PROGRAMID,
  p.PROGRAMNAME,
  e.APP_GROUP,
  e.EFFORT_POINTS, e.EST_FTE_PI, e.EST_COST_PI, e.ITERATION_PATH
FROM ADO_FEATURE_COST_ESTIMATE e
LEFT JOIN TEAMS t ON t.TEAMID = e.TEAMID
LEFT JOIN APPLICATION_GROUPS ag ON ag.GROUPNAME = e.APP_GROUP
LEFT JOIN PROGRAMS p ON p.PROGRAMID = COALESCE(ag.PROGRAMID, t.PROGRAMID)
"""
wf = fetch_df(wf_sql).fillna({"PROGRAMNAME":"(Unassigned)","TEAMNAME":"(Unmapped)","APP_GROUP":"(Unmapped)"}).reset_index(drop=True)

# Non‑Working (Invoices)
nwf_sql = """
SELECT
  COALESCE(ag.PROGRAMID, i.PROGRAMID_AT_BOOKING) AS PROGRAMID,
  i.FISCAL_YEAR,
  i.AMOUNT
FROM INVOICES i
LEFT JOIN APPLICATIONS a ON a.APPLICATIONID = i.APPLICATIONID
LEFT JOIN APPLICATION_GROUPS ag ON ag.GROUPID = a.GROUPID
"""
nwf = fetch_df(nwf_sql).reset_index(drop=True)

# Apply filters
if program_pick:
    prog_ids = programs[programs["PROGRAMNAME"].isin(program_pick)]["PROGRAMID"].tolist()
    wf = wf[wf["PROGRAMID"].isin(prog_ids)]
    nwf = nwf[nwf["PROGRAMID"].isin(prog_ids)]
if team_pick:
    wf = wf[wf["TEAMNAME"].isin(team_pick)]

# KPIs
wf_cost = float(wf["EST_COST_PI"].sum()) if "EST_COST_PI" in wf else 0.0
nwf_cost = float(nwf["AMOUNT"].sum()) if "AMOUNT" in nwf else 0.0
k1, k2, k3 = st.columns(3)
k1.metric("Working Force (Estimated)", f"${wf_cost:,.0f}")
k2.metric("Non‑Working (Invoices)", f"${nwf_cost:,.0f}")
k3.metric("Total Cost of Ownership", f"${wf_cost + nwf_cost:,.0f}")

st.divider()
st.subheader("Working Force")
if not wf.empty:
    wf_prog = wf.groupby(["PROGRAMNAME"], as_index=False)["EST_COST_PI"].sum()
    st.plotly_chart(px.bar(wf_prog, x="PROGRAMNAME", y="EST_COST_PI", title="WF Cost by Program"), use_container_width=True)
    wf_team = wf.groupby(["TEAMNAME"], as_index=False)["EST_COST_PI"].sum()
    st.plotly_chart(px.bar(wf_team, x="TEAMNAME", y="EST_COST_PI", title="WF Cost by Team"), use_container_width=True)
    wf_app = wf.groupby(["APP_GROUP"], as_index=False)["EST_COST_PI"].sum()
    st.plotly_chart(px.bar(wf_app, x="APP_GROUP", y="EST_COST_PI", title="WF Cost by App Group"), use_container_width=True)
else:
    st.info("No Working Force data yet. Run the estimation on the ADO page.")

st.divider()
st.subheader("Non‑Working (Invoices) per Fiscal Year")
if not nwf.empty:
    inv_year = nwf.groupby(["FISCAL_YEAR"], as_index=False)["AMOUNT"].sum()
    st.plotly_chart(px.bar(inv_year, x="FISCAL_YEAR", y="AMOUNT", title="Invoices per Year"), use_container_width=True)
else:
    st.info("No invoice data found.")
