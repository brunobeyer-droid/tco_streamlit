import streamlit as st
import pandas as pd
from snowflake_db import fetch_df, execute
from datetime import datetime

st.set_page_config(page_title="ADO Sync (Excel/CSV or API) & Estimation", layout="wide")
st.title("🔄 ADO Feature Sync & Cost Estimation")

src = st.radio("Choose data source", ["Excel/CSV (OData export)", "ADO API (later)"], horizontal=True)
df = None

def insert_features(df_norm: pd.DataFrame):
    """Append rows to ADO_FEATURES then de-dup by FEATURE_ID keeping latest CHANGED_AT."""
    if df_norm is None or df_norm.empty:
        return
    cols = ["FEATURE_ID","TITLE","STATE","TEAM_RAW","APP_NAME_RAW","EFFORT_POINTS","ITERATION_PATH","CREATED_AT","CHANGED_AT"]
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO ADO_FEATURES ({','.join(cols)}) VALUES ({placeholders})"
    params = []
    for _, r in df_norm.iterrows():
        params.append(tuple(None if (pd.isna(r.get(c))) else r.get(c) for c in cols))
    execute(sql, params, many=True)

    # De-duplicate
    execute("""
    CREATE OR REPLACE TEMP TABLE _dedup AS
    SELECT *
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY FEATURE_ID ORDER BY CHANGED_AT DESC NULLS LAST) AS rn
      FROM ADO_FEATURES
    ) WHERE rn=1;
    TRUNCATE TABLE ADO_FEATURES;
    INSERT INTO ADO_FEATURES
      (FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH, CREATED_AT, CHANGED_AT)
    SELECT FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH, CREATED_AT, CHANGED_AT
    FROM _dedup;
    """)

# ---------- Excel/CSV path (NOW) ----------
if src == "Excel/CSV (OData export)":
    st.subheader("Upload OData Excel/CSV")
    upl = st.file_uploader("Drop your .xlsx or .csv here", type=["xlsx","csv"])
    if upl:
        # Try Excel first if extension is xlsx, else CSV.
        try:
            if upl.name.lower().endswith(".xlsx"):
                df_raw = pd.read_excel(upl, sheet_name=0)
            else:
                df_raw = pd.read_csv(upl)
        except Exception:
            upl.seek(0)
            df_raw = pd.read_csv(upl, sep=";")

        st.caption("Preview")
        st.dataframe(df_raw.head(30), use_container_width=True)

        st.markdown("### Map columns → TCO fields (defaults filled for your file)")
        cols = ["(none)"] + list(df_raw.columns)

        def idx(colname: str):
            """safe default index for selectbox"""
            return cols.index(colname) if colname in cols else 0

        # Defaults tailored to your uploaded file
        default_FEATURE_ID     = "WorkItemId"
        default_TITLE          = "Title"
        default_STATE          = "State"
        default_TEAM_RAW       = "Team"
        default_APP_NAME_RAW   = "Custom_ApplicationName"
        default_EFFORT_POINTS  = "Effort"
        default_ITERATION_PATH = "Iteration.IterationLevel3.2"
        default_CHANGED_AT     = "(none)"  # not present in your sheet

        c1, c2, c3 = st.columns(3)
        sel_id    = c1.selectbox("Feature ID", cols, index=idx(default_FEATURE_ID))
        sel_title = c2.selectbox("Title", cols, index=idx(default_TITLE))
        sel_state = c3.selectbox("State", cols, index=idx(default_STATE))

        c4, c5, c6 = st.columns(3)
        sel_team   = c4.selectbox("Team (raw)", cols, index=idx(default_TEAM_RAW), help="Your sheet has a 'Team' column")
        sel_app    = c5.selectbox("Application (raw)", cols, index=idx(default_APP_NAME_RAW), help="Using 'Custom_ApplicationName'")
        sel_effort = c6.selectbox("Effort (points)", cols, index=idx(default_EFFORT_POINTS))

        c7, c8 = st.columns(2)
        sel_iter    = c7.selectbox("Iteration Path (optional)", cols, index=idx(default_ITERATION_PATH))
        sel_changed = c8.selectbox("Changed Date (optional)", cols, index=idx(default_CHANGED_AT))

        def pick(name):
            return df_raw[name] if (name and name != "(none)") else None

        df = pd.DataFrame({
            "FEATURE_ID": (pick(sel_id) if sel_id != "(none)" else pd.Series(range(1, len(df_raw)+1))).astype(str),
            "TITLE": pick(sel_title),
            "STATE": pick(sel_state),
            "TEAM_RAW": pick(sel_team),
            "APP_NAME_RAW": pick(sel_app),
            "EFFORT_POINTS": pd.to_numeric(pick(sel_effort), errors="coerce") if sel_effort != "(none)" else None,
            "ITERATION_PATH": pick(sel_iter),
            "CREATED_AT": pd.NaT,
            # your file has no changed date → stamp now (UTC) so dedup works
            "CHANGED_AT": pd.to_datetime(pick(sel_changed), errors="coerce") if sel_changed != "(none)" else pd.Timestamp.utcnow(),
        }).reset_index(drop=True)

        st.markdown("### Normalized features (ready to save)")
        st.dataframe(df.head(50), use_container_width=True)

        if st.button("📥 Save to Snowflake (upsert & dedup)"):
            insert_features(df)
            st.success("Features saved & de‑duplicated in ADO_FEATURES.")

# ---------- API path (LATER) ----------
else:
    st.info("When you’re ready, add ADO secrets and switch to API. For now you can stick with Excel/CSV.")
    changed_since = st.date_input("Changed since (optional)")
    if st.button("Fetch from API"):
        from ado_client import ADOClient   # import only when you really use it
        client = ADOClient()
        ids = client.wiql_features(changed_since=changed_since.isoformat() if changed_since else None)
        feats = client.get_features(ids)
        df = pd.DataFrame(feats).reset_index(drop=True)
        st.success(f"Fetched {len(df)} features.")
        st.dataframe(df.head(50), use_container_width=True)

        if st.button("📥 Save API results"):
            insert_features(df)
            st.success("Features saved & de‑duplicated in ADO_FEATURES.")

st.divider()
st.header("🧮 Estimate Working Force Cost (store snapshot)")

st.caption("""
Effort points → **FTE‑PI** via `TEAM_CAPACITY` (latest), multiplied by blended rate from `V_TEAM_BLENDED_RATE_ASOF`.
Mappings from `MAP_ADO_TEAM_TO_TCO_TEAM` and `MAP_ADO_APP_TO_TCO_GROUP` are applied first.
""")

if st.button("Run estimation & store to ADO_FEATURE_COST_ESTIMATE"):
    sql = """
    WITH src AS (
      SELECT a.FEATURE_ID, a.TITLE, a.STATE,
             COALESCE(m1.TEAMID, a.TEAM_RAW) AS TEAMID_MAPPED,
             COALESCE(m2.APP_GROUP, a.APP_NAME_RAW) AS APP_GROUP_MAPPED,
             a.EFFORT_POINTS, a.ITERATION_PATH, a.CREATED_AT, a.CHANGED_AT
      FROM ADO_FEATURES a
      LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM m1 ON m1.ADO_TEAM = a.TEAM_RAW
      LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP m2 ON m2.ADO_APP = a.APP_NAME_RAW
    ),
    rate AS (
      SELECT * FROM V_TEAM_BLENDED_RATE_ASOF
    )
    SELECT
      s.FEATURE_ID, s.TITLE, s.STATE,
      s.TEAMID_MAPPED AS TEAMID,
      s.APP_GROUP_MAPPED AS APP_GROUP,
      s.EFFORT_POINTS,
      r.POINTS_PER_FTE_PER_PI,
      r.BLENDED_RATE_PER_FTE_PI,
      CASE
        WHEN r.POINTS_PER_FTE_PER_PI IS NULL OR r.POINTS_PER_FTE_PER_PI=0 OR s.EFFORT_POINTS IS NULL
          THEN NULL
        ELSE s.EFFORT_POINTS / r.POINTS_PER_FTE_PER_PI
      END AS EST_FTE_PI,
      CASE
        WHEN r.POINTS_PER_FTE_PER_PI IS NULL OR r.BLENDED_RATE_PER_FTE_PI IS NULL OR s.EFFORT_POINTS IS NULL
          THEN NULL
        ELSE (s.EFFORT_POINTS / r.POINTS_PER_FTE_PER_PI) * r.BLENDED_RATE_PER_FTE_PI
      END AS EST_COST_PI,
      s.ITERATION_PATH, s.CREATED_AT, s.CHANGED_AT
    FROM src s
    LEFT JOIN rate r ON r.TEAMID = s.TEAMID_MAPPED
    """
    est = fetch_df(sql).reset_index(drop=True)
    st.dataframe(est, use_container_width=True)

    execute("TRUNCATE TABLE ADO_FEATURE_COST_ESTIMATE")
    if not est.empty:
        cols = ["FEATURE_ID","TITLE","STATE","TEAMID","APP_GROUP","EFFORT_POINTS",
                "POINTS_PER_FTE_PER_PI","BLENDED_RATE_PER_FTE_PI","EST_FTE_PI","EST_COST_PI",
                "ITERATION_PATH","CREATED_AT","CHANGED_AT"]
        placeholders = ",".join(["%s"] * len(cols))
        ins = f"INSERT INTO ADO_FEATURE_COST_ESTIMATE ({','.join(cols)}) VALUES ({placeholders})"
        params = [tuple(None if pd.isna(v) else v for v in row) for _, row in est[cols].iterrows()]
        execute(ins, params, many=True)
    st.success("Estimation saved to ADO_FEATURE_COST_ESTIMATE.")
    st.download_button("⬇️ Download estimation CSV", est.to_csv(index=False), "ado_feature_cost_estimation.csv", "text/csv")
