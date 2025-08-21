# pages/Config – Team Calc & Rates.py
import streamlit as st
import pandas as pd

from snowflake_db import (
    list_teams,
    list_team_calc,
    upsert_team_calc_rates,
    fetch_df,
    ensure_team_calc_table,
    ensure_team_cost_view,
)

st.set_page_config(page_title="Config – Team Calc & Rates", layout="wide")
st.title("⚙️ Team Cost Rates & Per‑Feature Calc")

# ------------------------------------------------------
# Ensure required objects exist (safe/idempotent)
# ------------------------------------------------------
with st.spinner("Preparing rate tables & views..."):
    try:
        ensure_team_calc_table()
    except Exception as e:
        st.error(f"Could not ensure TEAM_CALC: {e}")
    try:
        ensure_team_cost_view()
    except Exception as e:
        st.error(f"Could not ensure VW_TEAM_COSTS_PER_FEATURE: {e}")

tab_edit, tab_preview = st.tabs(["Edit Rates", "Preview Calculations"])

def _safe_float(val, default=0.0) -> float:
    try:
        if pd.isna(val):
            return float(default)
        return float(val)
    except Exception:
        return float(default)

# ------------------------
# Tab 1: Edit Rates
# ------------------------
with tab_edit:
    st.subheader("Per‑Team Rates (annual per FTE)")

    teams = list_teams()
    if teams.empty:
        st.info("No teams found. Create teams first (Teams page).")
    else:
        team_names = teams["TEAMNAME"].astype(str).tolist()
        team_ids   = teams["TEAMID"].astype(str).tolist()
        name_to_id = dict(zip(team_names, team_ids))

        col_sel, col_r1, col_r2, col_r3 = st.columns([2, 1, 1, 1])
        with col_sel:
            team_name_sel = st.selectbox("Team", team_names, index=0, key="rate_team_select")
        team_id_sel = name_to_id.get(team_name_sel)

        # Pull current rates safely (tc may be empty or lack columns)
        try:
            tc = list_team_calc()
        except Exception as e:
            st.error(f"Could not read TEAM_CALC: {e}")
            tc = pd.DataFrame()

        # If tc is empty or missing TEAMID column, use a safe empty frame with expected columns
        expected_cols = {"TEAMID","TEAMNAME","XOM_RATE","CONTRACTOR_CS_RATE","CONTRACTOR_C_RATE","UPDATED_AT"}
        if tc.empty or not set(tc.columns).issuperset(expected_cols):
            tc = pd.DataFrame(columns=list(expected_cols))

        tc_row = tc.loc[tc["TEAMID"] == team_id_sel] if "TEAMID" in tc.columns else pd.DataFrame()

        xom_rate_cur = _safe_float(tc_row["XOM_RATE"].iloc[0] if not tc_row.empty else None, 0.0)
        cs_rate_cur  = _safe_float(tc_row["CONTRACTOR_CS_RATE"].iloc[0] if not tc_row.empty else None, 0.0)
        c_rate_cur   = _safe_float(tc_row["CONTRACTOR_C_RATE"].iloc[0] if not tc_row.empty else None, 0.0)

        with col_r1:
            xom_rate = st.number_input(
                "XOM_RATE (annual per FTE)",
                min_value=0.0, step=100.0, value=xom_rate_cur,
                help="Annual cost per FTE. View divides by 4 to get per‑PI."
            )
        with col_r2:
            contractor_cs_rate = st.number_input(
                "CONTRACTOR_CS_RATE (annual per FTE)",
                min_value=0.0, step=100.0, value=cs_rate_cur,
                help="Annual rate for Contractor CS FTE share."
            )
        with col_r3:
            contractor_c_rate = st.number_input(
                "CONTRACTOR_C_RATE (annual per FTE)",
                min_value=0.0, step=100.0, value=c_rate_cur,
                help="Annual rate for Contractor C FTE share."
            )

        c1, c2 = st.columns([1,1])
        with c1:
            if st.button("💾 Save rates for team", type="primary", key="save_team_rates"):
                try:
                    upsert_team_calc_rates(team_id_sel, xom_rate, contractor_cs_rate, contractor_c_rate)
                    st.success(f"Saved rates for {team_name_sel}.")
                except Exception as e:
                    st.error(f"Save failed: {e}")
        with c2:
            if st.button("Seed default rates for ALL teams (only where missing)", key="seed_all"):
                try:
                    # Only set defaults if NULL; change the numbers to your defaults if needed
                    seed_sql = """
                    MERGE INTO TEAM_CALC t
                    USING (SELECT TEAMID FROM TEAMS) s
                    ON t.TEAMID = s.TEAMID
                    WHEN MATCHED THEN UPDATE SET
                      XOM_RATE           = COALESCE(t.XOM_RATE,           120000),
                      CONTRACTOR_CS_RATE = COALESCE(t.CONTRACTOR_CS_RATE, 100000),
                      CONTRACTOR_C_RATE  = COALESCE(t.CONTRACTOR_C_RATE,   90000),
                      UPDATED_AT = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (TEAMID, XOM_RATE, CONTRACTOR_CS_RATE, CONTRACTOR_C_RATE)
                    VALUES (s.TEAMID, 120000, 100000, 90000)
                    """
                    fetch_df(seed_sql)  # use fetch_df to run and swallow result
                    st.success("Seeded defaults where missing.")
                except Exception as e:
                    st.error(f"Seeding failed: {e}")

        st.markdown("### Current Rates")
        try:
            st.dataframe(list_team_calc(), use_container_width=True, height=300)
        except Exception as e:
            st.error(f"Could not load current rates: {e}")

    st.caption("If Preview still shows zeros, set non‑zero FTE composition on Teams and ensure ADO features are mapped to teams.")

# ------------------------
# Tab 2: Preview Calculations
# ------------------------
with tab_preview:
    st.subheader("Per‑Feature Cost (computed with your math)")
    st.caption("Based on TEAMS composition, TEAM_CALC rates, and ADO_FEATURES effort points.")

    col_a, col_b = st.columns([2, 2])
    team_filter = col_a.text_input("Filter by Team name (contains):", "", key="prev_team_filter")
    app_filter  = col_b.text_input("Filter by App name (contains):", "", key="prev_app_filter")

    base_sql = """
        SELECT
          TEAMNAME, TEAMID, FEATURE_ID, TITLE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH,
          TEAM_COST_PERPI,
          DEL_TEAM_COST_PERPI,
          TEAM_CONTRACTOR_CS_COST_PERPI,
          TEAM_CONTRACTOR_C_COST_PERPI,
          TEAMFTE, XOM_RATE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE
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

    try:
        df = fetch_df(base_sql, tuple(params) if params else None)
    except Exception as e:
        st.error(f"Could not query the preview view: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info("No rows match the filters, or inputs (rates/FTEs/mappings) are missing.")
    else:
        totals = df.groupby(["TEAMNAME", "TEAMID"], dropna=False)[
            ["TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]
        ].sum().reset_index()
        totals["TOTAL_COST_PERPI"] = totals[
            ["TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]
        ].sum(axis=1)

        st.markdown("#### Totals by Team")
        st.dataframe(totals, use_container_width=True, height=280)

        st.markdown("#### Detail by Feature (with inputs)")
        st.dataframe(df, use_container_width=True, height=460)

        with st.expander("Diagnostics: missing inputs"):
            q = """
            SELECT
              SUM(CASE WHEN COALESCE(XOM_RATE,0)=0 THEN 1 ELSE 0 END) AS xom_rate_zero,
              SUM(CASE WHEN COALESCE(CONTRACTOR_CS_RATE,0)=0 THEN 1 ELSE 0 END) AS cs_rate_zero,
              SUM(CASE WHEN COALESCE(CONTRACTOR_C_RATE,0)=0 THEN 1 ELSE 0 END) AS c_rate_zero,
              SUM(CASE WHEN COALESCE(DELIVERY_TEAM_FTE,0)+COALESCE(CONTRACTOR_CS_FTE,0)+COALESCE(CONTRACTOR_C_FTE,0)=0 THEN 1 ELSE 0 END) AS zero_denom,
              SUM(CASE WHEN COALESCE(TEAMFTE,0)=0 THEN 1 ELSE 0 END) AS teamfte_zero
            FROM VW_TEAM_COSTS_PER_FEATURE
            """
            try:
                miss = fetch_df(q)
                st.dataframe(miss, use_container_width=True)
            except Exception as e:
                st.warning(f"Diagnostics query failed: {e}")
