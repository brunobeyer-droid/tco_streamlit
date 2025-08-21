# pages/Config – Team Calc & Rates.py
import streamlit as st
import pandas as pd
from snowflake_db import (
    list_teams,
    list_team_calc,
    upsert_team_calc_rates,
    fetch_df,
)

st.set_page_config(page_title="Config – Team Calc & Rates", layout="wide")
st.title("⚙️ Team Cost Rates & Per‑Feature Calc")

tab_edit, tab_preview = st.tabs(["Edit Rates", "Preview Calculations"])

# ------------------------
# Tab 1: Edit Rates
# ------------------------
with tab_edit:
    st.subheader("Per‑Team Rates (editable)")

    teams = list_teams()
    if teams.empty:
        st.info("No teams found. Create teams first.")
    else:
        # Left selector
        team_names = teams["TEAMNAME"].tolist()
        team_ids = teams["TEAMID"].tolist()
        team_map = dict(zip(team_names, team_ids))

        col_sel, col_r1, col_r2, col_r3 = st.columns([2, 1, 1, 1])
        with col_sel:
            team_name_sel = st.selectbox("Team", team_names, index=0)
        team_id_sel = team_map[team_name_sel]

        # Pull current rates
        tc = list_team_calc()
        tc_row = tc[tc["TEAMID"] == team_id_sel]
        xom_rate_cur = float(tc_row["XOM_RATE"].iloc[0]) if not tc_row.empty and pd.notna(tc_row["XOM_RATE"].iloc[0]) else 0.0
        cs_rate_cur  = float(tc_row["CONTRACTOR_CS_RATE"].iloc[0]) if not tc_row.empty and pd.notna(tc_row["CONTRACTOR_CS_RATE"].iloc[0]) else 0.0
        c_rate_cur   = float(tc_row["CONTRACTOR_C_RATE"].iloc[0]) if not tc_row.empty and pd.notna(tc_row["CONTRACTOR_C_RATE"].iloc[0]) else 0.0

        with col_r1:
            xom_rate = st.number_input("XOM_RATE", min_value=0.0, step=100.0, value=xom_rate_cur, help="Per-PI rate used in TEAM_COST_PERPI and Delivery calc")
        with col_r2:
            contractor_cs_rate = st.number_input("CONTRACTOR_CS_RATE", min_value=0.0, step=100.0, value=cs_rate_cur)
        with col_r3:
            contractor_c_rate = st.number_input("CONTRACTOR_C_RATE", min_value=0.0, step=100.0, value=c_rate_cur)

        if st.button("💾 Save rates for team", type="primary"):
            upsert_team_calc_rates(team_id_sel, xom_rate, contractor_cs_rate, contractor_c_rate)
            st.success("Saved!")

        st.markdown("### Current Rates")
        st.dataframe(list_team_calc(), use_container_width=True, height=300)

# ------------------------
# Tab 2: Preview Calculations
# ------------------------
with tab_preview:
    st.subheader("Per‑Feature Cost (computed with your math)")
    st.caption("Based on TEAMS composition, TEAM_CALC rates, and ADO_FEATURES effort points.")

    # Optional filters
    col_a, col_b = st.columns([2, 2])
    team_filter = col_a.text_input("Filter by Team name (contains):", "")
    app_filter  = col_b.text_input("Filter by App name (contains):", "")

    base_sql = """
        SELECT
          TEAMNAME, TEAMID, FEATURE_ID, TITLE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH,
          TEAM_COST_PERPI,
          DEL_TEAM_COST_PERPI,
          TEAM_CONTRACTOR_CS_COST_PERPI,
          TEAM_CONTRACTOR_C_COST_PERPI
        FROM VW_TEAM_COSTS_PER_FEATURE
    """

    where = []
    params = []
    if team_filter.strip():
        where.append("UPPER(TEAMNAME) LIKE UPPER(%s)")
        params.append(f"%{team_filter.strip()}%")
    if app_filter.strip():
        where.append("UPPER(APP_NAME_RAW) LIKE UPPER(%s)")
        params.append(f"%{app_filter.strip()}%")
    if where:
        base_sql += " WHERE " + " AND ".join(where)
    base_sql += " ORDER BY TEAMNAME, FEATURE_ID"

    df = fetch_df(base_sql, tuple(params) if params else None)
    if df.empty:
        st.info("No rows match the current filters.")
    else:
        # Totals per team
        totals = df.groupby(["TEAMNAME", "TEAMID"], dropna=False)[
            ["TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]
        ].sum().reset_index()
        totals["TOTAL_COST_PERPI"] = totals[
            ["TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]
        ].sum(axis=1)

        st.markdown("#### Totals by Team")
        st.dataframe(totals, use_container_width=True, height=280)

        st.markdown("#### Detail by Feature")
        st.dataframe(df, use_container_width=True, height=420)
