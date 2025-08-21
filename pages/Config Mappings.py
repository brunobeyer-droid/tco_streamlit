# pages/1_Config_Mappings.py
import streamlit as st
import pandas as pd
from datetime import date
from uuid import uuid4
from typing import Dict, Optional

from snowflake_db import fetch_df, execute, ensure_ado_tables

st.set_page_config(page_title="Config • Rates & Effort Rules", layout="wide")
st.title("⚙️ Config • FTE Rates & Effort Split Rules")

# ------------------------------------------------------------------
# Bootstrap: make sure required tables/views exist (idempotent)
# ------------------------------------------------------------------
def ensure_effort_rule_objects() -> None:
    # Base ADO objects (you already have this)
    ensure_ado_tables()

    # New: EFFORT_SPLIT_RULES table
    execute("""
    CREATE TABLE IF NOT EXISTS EFFORT_SPLIT_RULES (
      RULE_ID STRING,
      RULE_NAME STRING,
      EFFECTIVE_FROM DATE,
      DENOMINATOR STRING,                -- 'TEAMFTE' | 'SUM_FTE'
      TEAM_WEIGHT NUMBER(18,6),
      DELIVERY_WEIGHT NUMBER(18,6),
      CONTRACTOR_CS_WEIGHT NUMBER(18,6),
      CONTRACTOR_C_WEIGHT NUMBER(18,6),
      NORMALIZE BOOLEAN,
      NOTES STRING,
      IS_ACTIVE BOOLEAN,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)

    # View: live team composition from TEAMS
    execute("""
    CREATE OR REPLACE VIEW V_TEAM_COMPOSITION_FROM_TEAMS AS
    SELECT
      t.TEAMID,
      TO_DECIMAL(t.TEAMFTE,           18, 6) AS TEAMFTE,
      TO_DECIMAL(t.DELIVERY_TEAM_FTE, 18, 6) AS DELIVERY_TEAM_FTE,
      TO_DECIMAL(t.CONTRACTOR_CS,     18, 6) AS CONTRACTOR_CS,
      TO_DECIMAL(t.CONTRACTOR_C_FTE,  18, 6) AS CONTRACTOR_C_FTE,
      (COALESCE(t.TEAMFTE,0)+COALESCE(t.DELIVERY_TEAM_FTE,0)+COALESCE(t.CONTRACTOR_CS,0)+COALESCE(t.CONTRACTOR_C_FTE,0)) AS SUM_FTE
    FROM TEAMS t
    """)

    # View: pick the active rule (latest active, fallback to latest by date)
    execute("""
    CREATE OR REPLACE VIEW V_ACTIVE_EFFORT_RULE AS
    SELECT *
    FROM (
      SELECT r.*,
             ROW_NUMBER() OVER (ORDER BY (CASE WHEN r.IS_ACTIVE THEN 0 ELSE 1 END),
                                          r.EFFECTIVE_FROM DESC, r.CREATED_AT DESC) AS rn
      FROM EFFORT_SPLIT_RULES r
    ) WHERE rn=1
    """)

    # View: per-team split multipliers m_* (sum to 1 if normalize=TRUE)
    execute("""
    CREATE OR REPLACE VIEW V_EFFORT_SPLIT_ASOF AS
    WITH base AS (
      SELECT
        c.TEAMID,
        c.TEAMFTE, c.DELIVERY_TEAM_FTE, c.CONTRACTOR_CS, c.CONTRACTOR_C_FTE, c.SUM_FTE,
        r.DENOMINATOR, r.TEAM_WEIGHT, r.DELIVERY_WEIGHT, r.CONTRACTOR_CS_WEIGHT, r.CONTRACTOR_C_WEIGHT, r.NORMALIZE
      FROM V_TEAM_COMPOSITION_FROM_TEAMS c
      CROSS JOIN V_ACTIVE_EFFORT_RULE r
    ),
    denom AS (
      SELECT TEAMID,
             CASE WHEN DENOMINATOR='TEAMFTE'
                  THEN NULLIF(TEAMFTE,0)
                  ELSE NULLIF(SUM_FTE,0)
             END AS DEN
      FROM base
    ),
    raw AS (
      SELECT
        b.TEAMID,
        (COALESCE(b.TEAMFTE,0)           / d.DEN) * COALESCE(b.TEAM_WEIGHT,1)          AS raw_team,
        (COALESCE(b.DELIVERY_TEAM_FTE,0) / d.DEN) * COALESCE(b.DELIVERY_WEIGHT,1)      AS raw_delivery,
        (COALESCE(b.CONTRACTOR_CS,0)     / d.DEN) * COALESCE(b.CONTRACTOR_CS_WEIGHT,1) AS raw_cs,
        (COALESCE(b.CONTRACTOR_C_FTE,0)  / d.DEN) * COALESCE(b.CONTRACTOR_C_WEIGHT,1)  AS raw_c
      FROM base b
      JOIN denom d USING (TEAMID)
    ),
    norm AS (
      SELECT TEAMID,
             CASE WHEN (SELECT NORMALIZE FROM V_ACTIVE_EFFORT_RULE)
                  THEN (raw_team+raw_delivery+raw_cs+raw_c)
                  ELSE 1
             END AS Z
      FROM raw
    )
    SELECT
      r.TEAMID,
      CASE WHEN n.Z=0 THEN 0 ELSE r.raw_team     / n.Z END AS M_TEAM,
      CASE WHEN n.Z=0 THEN 0 ELSE r.raw_delivery / n.Z END AS M_DELIVERY,
      CASE WHEN n.Z=0 THEN 0 ELSE r.raw_cs       / n.Z END AS M_CS,
      CASE WHEN n.Z=0 THEN 0 ELSE r.raw_c        / n.Z END AS M_C
    FROM raw r
    JOIN norm n USING (TEAMID)
    """)

with st.spinner("Bootstrapping config tables & views..."):
    ensure_effort_rule_objects()

# Diagnostics: show context
diag = fetch_df("SELECT current_role() AS ROLE, current_database() AS DB, current_schema() AS SCH, current_warehouse() AS WH")
if not diag.empty:
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Role",        str(diag.iloc[0]["ROLE"]))
    c2.metric("Database",    str(diag.iloc[0]["DB"]))
    c3.metric("Schema",      str(diag.iloc[0]["SCH"]))
    c4.metric("Warehouse",   str(diag.iloc[0]["WH"]))

st.divider()

# ------------------------------------------------------------------
# 1) FTE Rates per PI
# ------------------------------------------------------------------
st.header("1) FTE Rates per PI (by role)")

def latest_rates_map() -> Dict[str, float]:
    r = fetch_df("""
        WITH r AS (
          SELECT EMPLOYEE_TYPE, RATE_PER_PI,
                 ROW_NUMBER() OVER (PARTITION BY EMPLOYEE_TYPE ORDER BY EFFECTIVE_FROM DESC) rn
          FROM FTE_RATES
        )
        SELECT EMPLOYEE_TYPE, RATE_PER_PI FROM r WHERE rn=1
    """)
    return {str(x.EMPLOYEE_TYPE): float(x.RATE_PER_PI) for _, x in r.iterrows()} if not r.empty else {}

curr = latest_rates_map()
col1,col2,col3,col4 = st.columns(4)
rate_team     = col1.number_input("TEAM (per FTE per PI)",          value=curr.get("TEAM", 0.0),          min_value=0.0, step=100.0)
rate_delivery = col2.number_input("DELIVERY_TEAM (per FTE per PI)", value=curr.get("DELIVERY_TEAM", 0.0), min_value=0.0, step=100.0)
rate_cs       = col3.number_input("CONTRACTOR_CS (per FTE per PI)", value=curr.get("CONTRACTOR_CS", 0.0), min_value=0.0, step=100.0)
rate_c        = col4.number_input("CONTRACTOR_C (per FTE per PI)",  value=curr.get("CONTRACTOR_C", 0.0),  min_value=0.0, step=100.0)

eff_date = st.date_input("Effective from", value=date.today())
if st.button("💾 Save rates"):
    execute("DELETE FROM FTE_RATES WHERE EFFECTIVE_FROM = %s", (eff_date,))
    execute("INSERT INTO FTE_RATES (EMPLOYEE_TYPE, EFFECTIVE_FROM, RATE_PER_PI) VALUES (%s,%s,%s)",
            [("TEAM", eff_date, rate_team),
             ("DELIVERY_TEAM", eff_date, rate_delivery),
             ("CONTRACTOR_CS", eff_date, rate_cs),
             ("CONTRACTOR_C", eff_date, rate_c)],
            many=True)
    st.success("Rates saved.")

st.divider()

# ------------------------------------------------------------------
# 2) Effort Split Rule (uses TEAMS FTE live)
# ------------------------------------------------------------------
st.header("2) Effort Split Rule (uses live FTEs from TEAMS)")

denom = st.selectbox("Denominator", ["SUM_FTE", "TEAMFTE"], help="Divide by total FTEs or only by Team FTE.")
w1,w2,w3,w4 = st.columns(4)
w_team     = w1.number_input("Weight • TEAM",          value=1.0, step=0.1, min_value=0.0)
w_delivery = w2.number_input("Weight • DELIVERY",      value=1.0, step=0.1, min_value=0.0)
w_cs       = w3.number_input("Weight • CONTRACTOR CS", value=1.0, step=0.1, min_value=0.0)
w_c        = w4.number_input("Weight • CONTRACTOR C",  value=1.0, step=0.1, min_value=0.0)
normalize  = st.checkbox("Normalize weights so final m_* sum to 1", value=True)

rule_name  = st.text_input("Rule name", value="Default effort split")
notes      = st.text_area("Notes (optional)", value="Split = (FTE_role / denominator) * weight; optional normalization.")

def save_rule_active() -> str:
    rid = "rule-" + uuid4().hex[:8]
    execute("""
        INSERT INTO EFFORT_SPLIT_RULES
        (RULE_ID, RULE_NAME, EFFECTIVE_FROM, DENOMINATOR, TEAM_WEIGHT, DELIVERY_WEIGHT, CONTRACTOR_CS_WEIGHT, CONTRACTOR_C_WEIGHT, NORMALIZE, NOTES, IS_ACTIVE)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (rid, rule_name, date.today(), denom, w_team, w_delivery, w_cs, w_c, normalize, notes, True))
    execute("UPDATE EFFORT_SPLIT_RULES SET IS_ACTIVE = FALSE WHERE RULE_ID <> %s", (rid,))
    # Refresh dependent views
    ensure_effort_rule_objects()
    return rid

cA, cB = st.columns(2)
if cA.button("💾 Save as ACTIVE rule"):
    rid = save_rule_active()
    st.success(f"Saved and activated: {rid}")

active = fetch_df("SELECT * FROM V_ACTIVE_EFFORT_RULE")
if not active.empty:
    st.info(f"Active rule: **{active.iloc[0]['RULE_NAME']}** (denom={active.iloc[0]['DENOMINATOR']}, normalize={active.iloc[0]['NORMALIZE']})")

st.divider()

# ------------------------------------------------------------------
# 3) Live Preview (no upload) — pick a team, type Effort points
# ------------------------------------------------------------------
st.header("3) Live Preview • Split & Cost per Team")

teams = fetch_df("SELECT TEAMID, TEAMNAME FROM TEAMS ORDER BY TEAMNAME").reset_index(drop=True)
team_options = teams["TEAMNAME"].tolist() if not teams.empty else []
picked_team = st.selectbox("Team", team_options) if team_options else None
effort_pts = st.number_input("ADO Effort (points)", value=2.0, step=0.25, min_value=0.0)

if picked_team:
    team_id = teams.loc[teams["TEAMNAME"] == picked_team, "TEAMID"].iloc[0]
    split = fetch_df("SELECT * FROM V_EFFORT_SPLIT_ASOF WHERE TEAMID = %s", (team_id,))
    comp  = fetch_df("SELECT * FROM V_TEAM_COMPOSITION_FROM_TEAMS WHERE TEAMID = %s", (team_id,))
    cap   = fetch_df("SELECT TEAMID, POINTS_PER_FTE_PER_PI FROM V_TEAM_BLENDED_RATE_ASOF WHERE TEAMID = %s", (team_id,))
    rates = fetch_df("""
        WITH r AS (
          SELECT EMPLOYEE_TYPE, RATE_PER_PI,
                 ROW_NUMBER() OVER (PARTITION BY EMPLOYEE_TYPE ORDER BY EFFECTIVE_FROM DESC) rn
          FROM FTE_RATES
        )
        SELECT EMPLOYEE_TYPE, RATE_PER_PI FROM r WHERE rn=1
    """)
    rmap: Dict[str,float] = {str(x.EMPLOYEE_TYPE): float(x.RATE_PER_PI) for _, x in rates.iterrows()} if not rates.empty else {}

    if split.empty or cap.empty:
        st.warning("Missing split or capacity for this team. Make sure you set a rule and at least one capacity entry.")
    else:
        m_team = float(split.iloc[0]["M_TEAM"]); m_del = float(split.iloc[0]["M_DELIVERY"])
        m_cs   = float(split.iloc[0]["M_CS"]);   m_c   = float(split.iloc[0]["M_C"])
        ppfp   = float(cap.iloc[0]["POINTS_PER_FTE_PER_PI"] or 0.0)

        eff_team = effort_pts * m_team
        eff_del  = effort_pts * m_del
        eff_cs   = effort_pts * m_cs
        eff_c    = effort_pts * m_c

        def fte_pi(x: float) -> float:
            return (x / ppfp) if ppfp else 0.0

        rows = [
            {"Role":"TEAM",          "EffortPts":eff_team, "EstFTE-PI": fte_pi(eff_team), "EstCostPI": fte_pi(eff_team)*rmap.get("TEAM",0.0)},
            {"Role":"DELIVERY_TEAM", "EffortPts":eff_del,  "EstFTE-PI": fte_pi(eff_del),  "EstCostPI": fte_pi(eff_del)*rmap.get("DELIVERY_TEAM",0.0)},
            {"Role":"CONTRACTOR_CS", "EffortPts":eff_cs,   "EstFTE-PI": fte_pi(eff_cs),   "EstCostPI": fte_pi(eff_cs)*rmap.get("CONTRACTOR_CS",0.0)},
            {"Role":"CONTRACTOR_C",  "EffortPts":eff_c,    "EstFTE-PI": fte_pi(eff_c),    "EstCostPI": fte_pi(eff_c)*rmap.get("CONTRACTOR_C",0.0)},
        ]
        st.subheader("Split & Cost Preview")
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
