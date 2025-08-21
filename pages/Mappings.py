# pages/Mappings & Reconciliation.py
import streamlit as st
import pandas as pd
from typing import Optional

from snowflake_db import (
    fetch_df, execute,
    list_teams, list_application_groups,
    ensure_ado_minimal_tables, ensure_team_calc_table, ensure_team_cost_view,
    # existing helpers
    repair_ado_effort_points_precision,
    upsert_map_ado_team_to_tco_team,
    upsert_map_ado_app_to_tco_group,
)

# -------------------------------------------------------
# Page config
# -------------------------------------------------------
st.set_page_config(page_title="Mappings & Reconciliation", layout="wide")
st.title("🔗 ADO ↔️ TCO Mappings & Reconciliation")

# Ensure minimal dependencies exist (idempotent)
ensure_ado_minimal_tables()
ensure_team_calc_table()
ensure_team_cost_view()

tab_map, tab_explore, tab_recon = st.tabs(["Mappings", "ADO Explorer", "Reconciliation"])

# =========================================================
# Shared helpers
# =========================================================
def load_team_mapping_df() -> pd.DataFrame:
    return fetch_df("""
      SELECT m.ADO_TEAM,
             m.TEAMID,
             t.TEAMNAME
      FROM MAP_ADO_TEAM_TO_TCO_TEAM m
      LEFT JOIN TEAMS t ON t.TEAMID = m.TEAMID
      ORDER BY ADO_TEAM
    """)

def load_app_mapping_df() -> pd.DataFrame:
    return fetch_df("""
      SELECT m.ADO_APP,
             m.APP_GROUP,
             g.GROUPNAME,
             g.TEAMID,
             t.TEAMNAME
      FROM MAP_ADO_APP_TO_TCO_GROUP m
      LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = m.APP_GROUP
      LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
      ORDER BY ADO_APP
    """)

def load_ado_distincts():
    teams = fetch_df("""
        SELECT DISTINCT TEAM_RAW
        FROM ADO_FEATURES
        WHERE TEAM_RAW IS NOT NULL AND TRIM(TEAM_RAW) <> ''
        ORDER BY TEAM_RAW
    """)
    apps  = fetch_df("""
        SELECT DISTINCT APP_NAME_RAW
        FROM ADO_FEATURES
        WHERE APP_NAME_RAW IS NOT NULL AND TRIM(APP_NAME_RAW) <> ''
        ORDER BY APP_NAME_RAW
    """)
    iters = fetch_df("""
        SELECT DISTINCT ITERATION_PATH
        FROM ADO_FEATURES
        WHERE ITERATION_PATH IS NOT NULL AND TRIM(ITERATION_PATH) <> ''
        ORDER BY ITERATION_PATH
    """)
    return teams, apps, iters

def ado_features_base_query(where_sql: str = "", params: Optional[tuple] = None) -> pd.DataFrame:
    sql = f"""
      SELECT FEATURE_ID, TITLE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH, CHANGED_AT
      FROM ADO_FEATURES
      {where_sql}
      ORDER BY COALESCE(CHANGED_AT, TO_TIMESTAMP_NTZ('1900-01-01')) DESC, FEATURE_ID
    """
    df = fetch_df(sql, params)
    if "EFFORT_POINTS" in df.columns:
        df["EFFORT_POINTS"] = pd.to_numeric(df["EFFORT_POINTS"], errors="coerce")
    return df

def mapping_coverage() -> pd.DataFrame:
    return fetch_df("""
      WITH a AS (
        SELECT
          COUNT(*) AS TOTAL_FEATURES,
          COUNT(DISTINCT TEAM_RAW) AS ADO_TEAMS,
          COUNT(DISTINCT APP_NAME_RAW) AS ADO_APPS
        FROM ADO_FEATURES
      ),
      tm AS (
        SELECT COUNT(*) AS MAPPED_TEAMS FROM MAP_ADO_TEAM_TO_TCO_TEAM
      ),
      am AS (
        SELECT COUNT(*) AS MAPPED_APPS FROM MAP_ADO_APP_TO_TCO_GROUP
      )
      SELECT
        a.TOTAL_FEATURES,
        a.ADO_TEAMS,
        tm.MAPPED_TEAMS,
        a.ADO_APPS,
        am.MAPPED_APPS,
        CASE WHEN a.ADO_TEAMS=0 THEN 0 ELSE ROUND(tm.MAPPED_TEAMS / a.ADO_TEAMS * 100, 1) END AS TEAM_MAP_PCT,
        CASE WHEN a.ADO_APPS=0  THEN 0 ELSE ROUND(am.MAPPED_APPS / a.ADO_APPS * 100, 1) END AS APP_MAP_PCT
      FROM a, tm, am
    """)

def save_team_mapping(ado_team: str, team_id: str):
    upsert_map_ado_team_to_tco_team(ado_team, team_id)

def save_app_mapping(ado_app: str, group_id: str):
    upsert_map_ado_app_to_tco_group(ado_app, group_id)

# =========================================================
# MAPPINGS (no sub-tabs: stacked sections)
# =========================================================
with tab_map:
    # ---------- Coverage at the top
    st.subheader("Coverage")
    cov = mapping_coverage()
    if cov.empty:
        st.info("No ADO features found.")
    else:
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Total ADO Features", int(cov.iloc[0]["TOTAL_FEATURES"]))
        m2.metric("ADO Teams", int(cov.iloc[0]["ADO_TEAMS"]))
        m3.metric("Mapped Teams", int(cov.iloc[0]["MAPPED_TEAMS"]))
        m4.metric("ADO Apps", int(cov.iloc[0]["ADO_APPS"]))
        m5.metric("Mapped Apps", int(cov.iloc[0]["MAPPED_APPS"]))
        m6.metric("Team/App Map %", f"{float(cov.iloc[0]['TEAM_MAP_PCT'])}% / {float(cov.iloc[0]['APP_MAP_PCT'])}%")

    st.markdown("---")

    # ---------- Section 1: ADO Team → TCO Team
    st.subheader("ADO Team → TCO Team")
    # Unmapped list with filter
    df_ado_teams = fetch_df("""
      SELECT
        a.TEAM_RAW,
        CASE WHEN m.TEAMID IS NULL THEN 'Unmapped' ELSE 'Mapped' END AS MAP_STATUS,
        m.TEAMID,
        t.TEAMNAME
      FROM (SELECT DISTINCT TEAM_RAW FROM ADO_FEATURES WHERE TEAM_RAW IS NOT NULL AND TRIM(TEAM_RAW) <> '') a
      LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM m ON m.ADO_TEAM = a.TEAM_RAW
      LEFT JOIN TEAMS t ON t.TEAMID = m.TEAMID
      ORDER BY MAP_STATUS DESC, TEAM_RAW
    """)
    f1, f2 = st.columns([2, 1])
    with f1:
        team_filter = st.text_input("Filter ADO Team contains", "", key="tm_filter")
    df_ado_teams_view = df_ado_teams.copy()
    if team_filter.strip():
        df_ado_teams_view = df_ado_teams_view[df_ado_teams_view["TEAM_RAW"].str.contains(team_filter, case=False, na=False)]
    st.dataframe(df_ado_teams_view, use_container_width=True, height=260)

    # Add / Update mapping form
    st.markdown("##### Add / Update Mapping")
    c1, c2, c3 = st.columns([2,2,1])
    with c1:
        ado_team_sel = st.selectbox(
            "ADO Team (TEAM_RAW)",
            df_ado_teams["TEAM_RAW"].tolist() if not df_ado_teams.empty else [],
            key="tm_ado_team_sel"
        )
    with c2:
        teams_df = list_teams()
        if teams_df.empty:
            st.info("No teams found. Create teams first.")
            chosen_team_id = ""
        else:
            team_display = teams_df["TEAMNAME"] + "  —  " + teams_df["TEAMID"]
            team_choice = st.selectbox("TCO Team", team_display.tolist(), key="tm_tco_team_select")
            idx = team_display.tolist().index(team_choice)
            chosen_team_id = teams_df.iloc[idx]["TEAMID"]
    with c3:
        if st.button("Save Mapping", type="primary", key="tm_btn_save", use_container_width=True, disabled=(not ado_team_sel or not chosen_team_id)):
            try:
                save_team_mapping(ado_team_sel, chosen_team_id)
                st.success("Team mapping saved.")
                st.rerun()
            except Exception as e:
                st.error(f"Save failed: {e}")

    st.markdown("##### Current Team Mappings")
    st.dataframe(load_team_mapping_df(), use_container_width=True, height=220)

    st.markdown("---")

    # ---------- Section 2: ADO App → TCO App Group
    st.subheader("ADO App → TCO App Group")
    # Unmapped list with filter
    df_ado_apps = fetch_df("""
      SELECT
        a.APP_NAME_RAW,
        CASE WHEN m.APP_GROUP IS NULL THEN 'Unmapped' ELSE 'Mapped' END AS MAP_STATUS,
        m.APP_GROUP,
        g.GROUPNAME,
        g.TEAMID,
        t.TEAMNAME
      FROM (SELECT DISTINCT APP_NAME_RAW FROM ADO_FEATURES WHERE APP_NAME_RAW IS NOT NULL AND TRIM(APP_NAME_RAW) <> '') a
      LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP m ON m.ADO_APP = a.APP_NAME_RAW
      LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = m.APP_GROUP
      LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
      ORDER BY MAP_STATUS DESC, APP_NAME_RAW
    """)
    g1, g2 = st.columns([2, 1])
    with g1:
        app_filter = st.text_input("Filter ADO App contains", "", key="am_filter")
    df_ado_apps_view = df_ado_apps.copy()
    if app_filter.strip():
        df_ado_apps_view = df_ado_apps_view[df_ado_apps_view["APP_NAME_RAW"].str.contains(app_filter, case=False, na=False)]
    st.dataframe(df_ado_apps_view, use_container_width=True, height=260)

    # Add / Update mapping form
    st.markdown("##### Add / Update Mapping")
    a1, a2, a3 = st.columns([2,3,1])
    with a1:
        ado_app_sel = st.selectbox(
            "ADO App (APP_NAME_RAW)",
            df_ado_apps["APP_NAME_RAW"].tolist() if not df_ado_apps.empty else [],
            key="am_ado_app_sel"
        )
    with a2:
        groups_df = list_application_groups()
        if groups_df.empty:
            st.info("No Application Groups found. Create groups first.")
            chosen_group_id = ""
        else:
            # In case TEAMNAME column is absent in list_application_groups(), guard with fillna
            teamname = groups_df["TEAMNAME"] if "TEAMNAME" in groups_df.columns else pd.Series([""] * len(groups_df))
            group_display = groups_df["GROUPNAME"] + "  —  " + groups_df["GROUPID"] + "  (Team: " + teamname.fillna("") + ")"
            group_choice = st.selectbox("TCO App Group", group_display.tolist(), key="am_tco_group_select")
            idx = group_display.tolist().index(group_choice)
            chosen_group_id = groups_df.iloc[idx]["GROUPID"]
    with a3:
        if st.button("Save Mapping", type="primary", key="am_btn_save", use_container_width=True, disabled=(not ado_app_sel or not chosen_group_id)):
            try:
                save_app_mapping(ado_app_sel, chosen_group_id)
                st.success("App mapping saved.")
                st.rerun()
            except Exception as e:
                st.error(f"Save failed: {e}")

    st.markdown("##### Current App Mappings")
    st.dataframe(load_app_mapping_df(), use_container_width=True, height=220)

# =========================================================
# ADO Explorer (unchanged structure; minor tidy)
# =========================================================
with tab_explore:
    st.subheader("What’s coming from ADO (raw import)")

    b1, _ = st.columns([1, 5])
    with b1:
        if st.button("🧹 Repair Effort Points Precision", key="btn_repair_effort_explorer"):
            try:
                repair_ado_effort_points_precision()
                st.success("Effort points precision repaired.")
                st.rerun()
            except Exception as e:
                st.error(f"Repair failed: {e}")

    df_teams, df_apps, df_iters = load_ado_distincts()
    c1, c2, c3 = st.columns(3)
    c1.metric("Distinct ADO Teams", len(df_teams))
    c2.metric("Distinct ADO Apps", len(df_apps))
    c3.metric("Distinct Iterations", len(df_iters))

    st.markdown("#### Filter")
    f1, f2, f3 = st.columns([2,2,2])
    team_like = f1.text_input("Team contains", "", key="exp_team_contains")
    app_like  = f2.text_input("App contains", "", key="exp_app_contains")
    iter_like = f3.text_input("Iteration contains", "", key="exp_iter_contains")

    where = []
    params: list = []
    if team_like.strip():
        where.append("UPPER(TEAM_RAW) LIKE UPPER(%s)")
        params.append(f"%{team_like.strip()}%")
    if app_like.strip():
        where.append("UPPER(APP_NAME_RAW) LIKE UPPER(%s)")
        params.append(f"%{app_like.strip()}%")
    if iter_like.strip():
        where.append("UPPER(ITERATION_PATH) LIKE UPPER(%s)")
        params.append(f"%{iter_like.strip()}%")
    where_sql = " WHERE " + " AND ".join(where) if where else ""

    df_raw = ado_features_base_query(where_sql, tuple(params) if params else None)

    st.markdown("#### Latest Features from ADO")
    st.dataframe(df_raw, use_container_width=True, height=340)

    st.markdown("#### Effort Points by Iteration")
    df_iter_sum = fetch_df(f"""
      SELECT ITERATION_PATH,
             SUM(COALESCE(EFFORT_POINTS,0)) AS EFFORT_POINTS_SUM,
             COUNT(*) AS FEATURES
      FROM ADO_FEATURES
      {where_sql}
      GROUP BY ITERATION_PATH
      ORDER BY ITERATION_PATH
    """, tuple(params) if params else None)
    st.dataframe(df_iter_sum, use_container_width=True, height=240)

    st.markdown("#### Effort Points by Team & Iteration (raw)")
    df_team_iter = fetch_df(f"""
      SELECT TEAM_RAW, ITERATION_PATH,
             SUM(COALESCE(EFFORT_POINTS,0)) AS EFFORT_POINTS_SUM,
             COUNT(*) AS FEATURES
      FROM ADO_FEATURES
      {where_sql}
      GROUP BY TEAM_RAW, ITERATION_PATH
      ORDER BY TEAM_RAW, ITERATION_PATH
    """, tuple(params) if params else None)
    st.dataframe(df_team_iter, use_container_width=True, height=300)

# =========================================================
# Reconciliation (kept, minor tidy)
# =========================================================
with tab_recon:
    st.subheader("Reconciliation: mappings + effort + calculated costs")
    st.caption("This joins ADO features with your TCO Teams, App Groups and the cost formulas in VW_TEAM_COSTS_PER_FEATURE.")

    colA, colB, colC = st.columns(3)
    unmapped_team = fetch_df("""
      SELECT TEAM_RAW, COUNT(*) AS N
      FROM ADO_FEATURES af
      LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM m ON m.ADO_TEAM = af.TEAM_RAW
      WHERE m.TEAMID IS NULL
      GROUP BY TEAM_RAW
      ORDER BY N DESC
    """)
    colA.metric("Features missing TEAM mapping", int(unmapped_team["N"].sum()) if not unmapped_team.empty else 0)

    zero_denom = fetch_df("""
      SELECT COUNT(*) AS N
      FROM VW_TEAM_COSTS_PER_FEATURE
      WHERE (TEAMID IS NOT NULL) AND
            (COALESCE(DELIVERY_TEAM_FTE,0) + COALESCE(CONTRACTOR_CS_FTE,0) + COALESCE(CONTRACTOR_C_FTE,0)) = 0
    """)
    colB.metric("Rows with zero composition denominator", int(zero_denom.iloc[0]["N"]) if not zero_denom.empty else 0)

    no_rate = fetch_df("""
      SELECT COUNT(*) AS N FROM VW_TEAM_COSTS_PER_FEATURE
      WHERE (TEAMID IS NOT NULL) AND
            (COALESCE(XOM_RATE,0)=0 OR COALESCE(CONTRACTOR_CS_RATE,0)=0 OR COALESCE(CONTRACTOR_C_RATE,0)=0)
    """)
    colC.metric("Rows with a missing rate", int(no_rate.iloc[0]["N"]) if not no_rate.empty else 0)

    st.markdown("#### Filters")
    fc1, fc2, fc3 = st.columns([2,2,2])
    team_like2 = fc1.text_input("TCO Team contains", "", key="recon_team_contains")
    app_like2  = fc2.text_input("ADO App contains", "", key="recon_app_contains")
    iter_like2 = fc3.text_input("Iteration contains", "", key="recon_iter_contains")

    where2 = []
    params2: list = []
    if team_like2.strip():
        where2.append("UPPER(TEAMNAME) LIKE UPPER(%s)")
        params2.append(f"%{team_like2.strip()}%")
    if app_like2.strip():
        where2.append("UPPER(APP_NAME_RAW) LIKE UPPER(%s)")
        params2.append(f"%{app_like2.strip()}%")
    if iter_like2.strip():
        where2.append("UPPER(ITERATION_PATH) LIKE UPPER(%s)")
        params2.append(f"%{iter_like2.strip()}%")
    where_sql2 = " WHERE " + " AND ".join(where2) if where2 else ""

    base_sql = f"""
      SELECT
        TEAMNAME, TEAMID, FEATURE_ID, TITLE, APP_NAME_RAW, ITERATION_PATH,
        EFFORT_POINTS,
        TEAM_COST_PERPI,
        DEL_TEAM_COST_PERPI,
        TEAM_CONTRACTOR_CS_COST_PERPI,
        TEAM_CONTRACTOR_C_COST_PERPI
      FROM VW_TEAM_COSTS_PER_FEATURE
      {where_sql2}
      ORDER BY TEAMNAME, ITERATION_PATH, FEATURE_ID
    """
    df_calc = fetch_df(base_sql, tuple(params2) if params2 else None)

    if df_calc.empty:
        st.info("No rows found with the current filters.")
    else:
        st.markdown("#### Effort & Cost by Team & Iteration")
        ag = df_calc.groupby(["TEAMNAME","ITERATION_PATH"], dropna=False).agg(
            FEATURES=("FEATURE_ID","count"),
            EFFORT_POINTS_SUM=("EFFORT_POINTS","sum"),
            TEAM_COST_PERPI=("TEAM_COST_PERPI","sum"),
            DEL_TEAM_COST_PERPI=("DEL_TEAM_COST_PERPI","sum"),
            TEAM_CONTRACTOR_CS_COST_PERPI=("TEAM_CONTRACTOR_CS_COST_PERPI","sum"),
            TEAM_CONTRACTOR_C_COST_PERPI=("TEAM_CONTRACTOR_C_COST_PERPI","sum"),
        ).reset_index()
        ag["TOTAL_COST_PERPI"] = ag[
            ["TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]
        ].sum(axis=1)
        st.dataframe(ag, use_container_width=True, height=320)

        st.markdown("#### Per-Feature Detail (to spot anomalies)")
        st.dataframe(df_calc, use_container_width=True, height=420)

        st.markdown("#### Unmapped ADO Teams (with counts)")
        st.dataframe(unmapped_team, use_container_width=True, height=220)

        st.markdown("#### Rows with missing rates or zero composition (diagnostics)")
        diag = fetch_df("""
          SELECT
            TEAMNAME, FEATURE_ID, APP_NAME_RAW, ITERATION_PATH, EFFORT_POINTS,
            DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE,
            XOM_RATE, CONTRACTOR_CS_RATE, CONTRACTOR_C_RATE
          FROM VW_TEAM_COSTS_PER_FEATURE
          WHERE (TEAMID IS NOT NULL) AND
                (
                 (COALESCE(DELIVERY_TEAM_FTE,0) + COALESCE(CONTRACTOR_CS_FTE,0) + COALESCE(CONTRACTOR_C_FTE,0)) = 0
                 OR COALESCE(XOM_RATE,0)=0
                 OR COALESCE(CONTRACTOR_CS_RATE,0)=0
                 OR COALESCE(CONTRACTOR_C_RATE,0)=0
                )
          ORDER BY TEAMNAME, FEATURE_ID
        """)
        st.dataframe(diag, use_container_width=True, height=260)
