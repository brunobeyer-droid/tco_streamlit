# pages/Sync ADO Features.py
import io
from typing import Optional, List, Dict, Tuple

import pandas as pd
import streamlit as st

from snowflake_db import execute, fetch_df, ensure_ado_minimal_tables

st.set_page_config(page_title="Sync ADO Features", layout="wide")
st.title("🔄 Sync ADO Features (Minimal)")

# Ensure minimal schema
with st.spinner("Ensuring minimal ADO schema..."):
    ensure_ado_minimal_tables()

# -------------------------
# Helpers to read ADO files
# -------------------------
EXPECTED = {
    "Effort": ["Effort", "Story Points", "Effort Points", "EFFORT_POINTS"],
    "Team": ["Team", "System.Team", "Area Team"],
    "Custom_ApplicationName": ["Custom_ApplicationName", "Application", "App Name"],
    "Iteration": ["Iteration", "Iteration Path", "System.IterationPath", "Iteration.IterationLevel3.2"],
    "Title": ["Title", "System.Title"],
    "State": ["State", "System.State"],
    "ID": ["ID", "Work Item ID", "System.Id", "WorkItemId", "Work Item Id"],
    "CreatedDate": ["Created Date", "System.CreatedDate", "CreatedDate"],
    "ChangedDate": ["Changed Date", "System.ChangedDate", "ChangedDate"],
}

def _try_csv(data: bytes) -> Optional[pd.DataFrame]:
    encodings: List[str] = ["utf-8-sig", "cp1252", "latin-1", "utf-16", "utf-16le", "utf-16be"]
    seps: List[Optional[str]] = [None, ";", ",", "\t"]
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(io.BytesIO(data), encoding=enc, sep=sep, engine="python")
                if df.shape[1] >= 2:
                    return df
            except Exception:
                continue
    return None

def _try_excel(data: bytes) -> Optional[pd.DataFrame]:
    try:
        return pd.read_excel(io.BytesIO(data), sheet_name=0)
    except Exception:
        return None

def read_ado_upload(upl) -> pd.DataFrame:
    if upl is None:
        raise ValueError("No file uploaded.")
    raw = upl.read()
    name = (upl.name or "").lower()

    df: Optional[pd.DataFrame] = None
    if name.endswith(".xlsx") or name.endswith(".xls"):
        df = _try_excel(raw) or _try_csv(raw)
    else:
        df = _try_csv(raw) or _try_excel(raw)

    if df is None or df.empty:
        raise ValueError("Could not parse the ADO file. Try CSV (Windows encoding) or XLSX.")

    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]

    rename: Dict[str, str] = {}
    for canonical, candidates in EXPECTED.items():
        for c in candidates:
            for col in df.columns:
                if col.lower() == c.lower():
                    rename[col] = canonical
                    break
            if canonical in rename.values():
                break
    df = df.rename(columns=rename)

    # Clean up
    for col in ("Team", "Custom_ApplicationName", "Iteration", "Title", "State"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    if "Effort" in df.columns:
        df["Effort"] = pd.to_numeric(df["Effort"], errors="coerce")

    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str)

    for dcol in ("CreatedDate", "ChangedDate"):
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce")

    return df

def normalize_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["FEATURE_ID"]     = df.get("ID", pd.Series(dtype=str)).astype(str)
    out["TITLE"]          = df.get("Title")
    out["STATE"]          = df.get("State")
    out["TEAM_RAW"]       = df.get("Team")
    out["APP_NAME_RAW"]   = df.get("Custom_ApplicationName")
    out["EFFORT_POINTS"]  = df.get("Effort")
    out["ITERATION_PATH"] = df.get("Iteration")
    out["CREATED_AT"]     = df.get("CreatedDate")
    out["CHANGED_AT"]     = df.get("ChangedDate")

    out["EFFORT_POINTS"]  = pd.to_numeric(out["EFFORT_POINTS"], errors="coerce")
    out["CREATED_AT"]     = pd.to_datetime(out["CREATED_AT"], errors="coerce")
    out["CHANGED_AT"]     = pd.to_datetime(out["CHANGED_AT"], errors="coerce")

    out = out[~out["FEATURE_ID"].isna() & (out["FEATURE_ID"].astype(str).str.len() > 0)].copy()
    return out.reset_index(drop=True)

def _to_py(v):
    import pandas as _pd, numpy as _np
    if v is None:
        return None
    if isinstance(v, _pd._libs.tslibs.nattype.NaTType):
        return None
    if isinstance(v, _pd.Timestamp):
        return v.to_pydatetime()
    if _pd.isna(v):
        return None
    if isinstance(v, _np.floating):
        return None if _np.isnan(v) else float(v)
    if isinstance(v, _np.integer):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    return v

def upsert_ado_features(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows: List[Tuple] = []
    for _, r in df.iterrows():
        rows.append((
            _to_py(r.get("FEATURE_ID")),
            _to_py(r.get("TITLE")),
            _to_py(r.get("STATE")),
            _to_py(r.get("TEAM_RAW")),
            _to_py(r.get("APP_NAME_RAW")),
            _to_py(r.get("EFFORT_POINTS")),
            _to_py(r.get("ITERATION_PATH")),
            _to_py(r.get("CREATED_AT")),
            _to_py(r.get("CHANGED_AT")),
        ))
    rows = [t for t in rows if t[0] is not None]
    if not rows:
        return 0

    sql = """
    MERGE INTO ADO_FEATURES t
    USING (
      SELECT %s AS FEATURE_ID, %s AS TITLE, %s AS STATE,
             %s AS TEAM_RAW, %s AS APP_NAME_RAW, %s AS EFFORT_POINTS,
             %s AS ITERATION_PATH, %s AS CREATED_AT, %s AS CHANGED_AT
    ) s
    ON t.FEATURE_ID = s.FEATURE_ID
    WHEN MATCHED THEN UPDATE SET
      TITLE = s.TITLE,
      STATE = s.STATE,
      TEAM_RAW = s.TEAM_RAW,
      APP_NAME_RAW = s.APP_NAME_RAW,
      EFFORT_POINTS = s.EFFORT_POINTS,
      ITERATION_PATH = s.ITERATION_PATH,
      CREATED_AT = s.CREATED_AT,
      CHANGED_AT = s.CHANGED_AT
    WHEN NOT MATCHED THEN INSERT
      (FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH, CREATED_AT, CHANGED_AT)
    VALUES
      (s.FEATURE_ID, s.TITLE, s.STATE, s.TEAM_RAW, s.APP_NAME_RAW, s.EFFORT_POINTS, s.ITERATION_PATH, s.CREATED_AT, s.CHANGED_AT)
    """
    execute(sql, rows, many=True)
    return len(rows)

# -------------------------
# UI
# -------------------------
tab_load, tab_map = st.tabs(["📥 Load Data", "🧭 Map Values"])

with tab_load:
    st.caption("Only raw ADO fields are stored. No calculations.")
    upl = st.file_uploader("Upload ADO OData export (CSV/XLSX)", type=["csv", "xlsx"])
    if upl:
        upl.seek(0)
        try:
            df_raw = read_ado_upload(upl)
        except Exception as e:
            st.error(f"Could not read file: {e}")
            st.stop()

        st.success(f"Parsed {len(df_raw)} rows from {upl.name}")
        with st.expander("Preview (first 50 rows)"):
            st.dataframe(df_raw.head(50), use_container_width=True)

        df_norm = normalize_to_canonical(df_raw)
        st.subheader("Normalized columns (to Snowflake)")
        st.dataframe(df_norm.head(30), use_container_width=True)

        if st.button("⬆️ Upsert into ADO_FEATURES"):
            with st.spinner("Loading into Snowflake..."):
                n = upsert_ado_features(df_norm)
            st.success(f"Upserted {n} rows into ADO_FEATURES.")

with tab_map:
    st.subheader("Map ADO values to TCO (no calculations)")
    # Distinct raw values
    ado_teams = fetch_df("""
        SELECT DISTINCT TEAM_RAW
        FROM ADO_FEATURES
        WHERE TEAM_RAW IS NOT NULL AND TEAM_RAW <> ''
        ORDER BY TEAM_RAW
    """)
    ado_apps = fetch_df("""
        SELECT DISTINCT APP_NAME_RAW
        FROM ADO_FEATURES
        WHERE APP_NAME_RAW IS NOT NULL AND APP_NAME_RAW <> ''
        ORDER BY APP_NAME_RAW
    """)

    # Existing mappings
    team_maps = fetch_df("SELECT ADO_TEAM, TEAMID FROM MAP_ADO_TEAM_TO_TCO_TEAM ORDER BY ADO_TEAM")
    app_maps  = fetch_df("SELECT ADO_APP, APP_GROUP FROM MAP_ADO_APP_TO_TCO_GROUP ORDER BY ADO_APP")

    teams_df  = fetch_df("SELECT TEAMID, TEAMNAME FROM TEAMS ORDER BY TEAMNAME")
    groups_df = fetch_df("SELECT GROUPID, GROUPNAME FROM APPLICATION_GROUPS ORDER BY GROUPNAME")

    # ---- Teams mapping
    st.markdown("### ADO → TCO Team")
    if ado_teams.empty or teams_df.empty:
        st.info("Load features and create Teams first.")
    else:
        base_tm = ado_teams.rename(columns={"TEAM_RAW": "ADO_TEAM"}).copy()
        if not team_maps.empty:
            base_tm = base_tm.merge(team_maps, how="left", on="ADO_TEAM")
        else:
            base_tm["TEAMID"] = None

        id_to_name = {r.TEAMID: r.TEAMNAME for _, r in teams_df.iterrows()}
        name_to_id = {r.TEAMNAME: r.TEAMID for _, r in teams_df.iterrows()}
        base_tm["TCO_TEAMNAME"] = base_tm["TEAMID"].map(id_to_name)

        edited = st.data_editor(
            base_tm[["ADO_TEAM","TCO_TEAMNAME"]],
            use_container_width=True,
            height=360,
            num_rows="fixed",
            column_config={
                "ADO_TEAM": st.column_config.TextColumn("ADO Team", disabled=True),
                "TCO_TEAMNAME": st.column_config.SelectboxColumn(
                    "TCO Team",
                    options=teams_df["TEAMNAME"].tolist(),
                    required=False,
                ),
            },
            key="tm_editor",
        )
        if st.button("💾 Save Team Mappings"):
            rows = []
            for _, r in edited.iterrows():
                ado_val = str(r["ADO_TEAM"]).strip()
                tname = r.get("TCO_TEAMNAME")
                if not ado_val:
                    continue
                if tname and tname in name_to_id:
                    rows.append((ado_val, name_to_id[tname]))
                else:
                    execute("DELETE FROM MAP_ADO_TEAM_TO_TCO_TEAM WHERE ADO_TEAM = %s", (ado_val,))
            if rows:
                merge_sql = """
                MERGE INTO MAP_ADO_TEAM_TO_TCO_TEAM t
                USING (SELECT %s AS ADO_TEAM, %s AS TEAMID) s
                ON t.ADO_TEAM = s.ADO_TEAM
                WHEN MATCHED THEN UPDATE SET TEAMID = s.TEAMID
                WHEN NOT MATCHED THEN INSERT (ADO_TEAM, TEAMID) VALUES (s.ADO_TEAM, s.TEAMID)
                """
                execute(merge_sql, rows, many=True)
            st.success("Team mappings saved.")

    st.markdown("---")

    # ---- App group mapping
    st.markdown("### ADO → TCO App Group")
    if ado_apps.empty or groups_df.empty:
        st.info("Load features and create Application Groups first.")
    else:
        base_am = ado_apps.rename(columns={"APP_NAME_RAW": "ADO_APP"}).copy()
        if not app_maps.empty:
            base_am = base_am.merge(app_maps, how="left", on="ADO_APP")
        else:
            base_am["APP_GROUP"] = None

        grp_names = groups_df["GROUPNAME"].tolist()
        base_am["TCO_GROUPNAME"] = base_am["APP_GROUP"]

        edited2 = st.data_editor(
            base_am[["ADO_APP","TCO_GROUPNAME"]],
            use_container_width=True,
            height=360,
            num_rows="fixed",
            column_config={
                "ADO_APP": st.column_config.TextColumn("ADO App Name", disabled=True),
                "TCO_GROUPNAME": st.column_config.SelectboxColumn(
                    "TCO App Group",
                    options=grp_names,
                    required=False,
                ),
            },
            key="am_editor",
        )
        if st.button("💾 Save App Group Mappings"):
            rows = []
            for _, r in edited2.iterrows():
                ado_val = str(r["ADO_APP"]).strip()
                gname = r.get("TCO_GROUPNAME")
                if not ado_val:
                    continue
                if gname:
                    rows.append((ado_val, gname))
                else:
                    execute("DELETE FROM MAP_ADO_APP_TO_TCO_GROUP WHERE ADO_APP = %s", (ado_val,))
            if rows:
                merge_sql = """
                MERGE INTO MAP_ADO_APP_TO_TCO_GROUP t
                USING (SELECT %s AS ADO_APP, %s AS APP_GROUP) s
                ON t.ADO_APP = s.ADO_APP
                WHEN MATCHED THEN UPDATE SET APP_GROUP = s.APP_GROUP
                WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP)
                """
                execute(merge_sql, rows, many=True)
            st.success("App Group mappings saved.")
