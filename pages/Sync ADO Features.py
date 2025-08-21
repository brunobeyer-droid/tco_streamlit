# pages/Sync ADO Features.py
import io
from typing import Optional, List, Dict, Tuple

import pandas as pd
import streamlit as st

from snowflake_db import execute, fetch_df, ensure_ado_minimal_tables

# Optional helpers (safe if not present in snowflake_db.py)
try:
    from snowflake_db import (
        ensure_team_calc_table,
        ensure_team_cost_view,
        repair_ado_effort_points_precision,
    )
except Exception:
    ensure_team_calc_table = None
    ensure_team_cost_view = None
    repair_ado_effort_points_precision = None

st.set_page_config(page_title="Sync ADO Features", layout="wide")
st.title("🔄 Sync ADO Features (XLSX‑friendly, persistent upload)")

# Ensure minimal schema is ready
with st.spinner("Ensuring minimal ADO schema..."):
    ensure_ado_minimal_tables()
    # Best effort: prepare calc/view for the Reconciliation tab if available
    if callable(ensure_team_calc_table):
        ensure_team_calc_table()
    if callable(ensure_team_cost_view):
        ensure_team_cost_view()

# -------------------------
# Session state
# -------------------------
if "ado_parsed_raw" not in st.session_state:
    st.session_state["ado_parsed_raw"] = None
if "ado_parsed_norm" not in st.session_state:
    st.session_state["ado_parsed_norm"] = None

# -------------------------
# Column expectations
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
    # New: explicit Year column from Excel (will become ADO_YEAR)
    "Year": ["Year", "ADO Year", "ADO_YEAR"],
}

# -------------------------
# Utilities
# -------------------------
def _auto_header_index(df_no_header: pd.DataFrame, expected_samples: List[str], max_scan: int = 20) -> Optional[int]:
    """If the first rows aren't the header, scan to locate a row that looks like the header."""
    for i in range(min(max_scan, len(df_no_header))):
        row_vals = df_no_header.iloc[i].astype(str).str.strip().str.lower().tolist()
        hits = 0
        for e in expected_samples:
            if e.lower() in row_vals:
                hits += 1
        if hits >= 2:
            return i
    return None

def _list_excel_sheets(data: bytes) -> List[str]:
    """Best effort sheet enumeration across engines."""
    sheets: List[str] = []
    # openpyxl for xlsx/xlsm
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        return list(wb.sheetnames)
    except Exception:
        pass
    # xlrd for legacy xls
    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=data)
        return wb.sheet_names()
    except Exception:
        pass
    # pyxlsb for xlsb
    try:
        from pyxlsb import open_workbook
        with open_workbook(fileobj=io.BytesIO(data)) as wb:
            return [s.name for s in wb.sheets]
    except Exception:
        pass
    return sheets

def _read_excel_any(data: bytes, sheet_name: Optional[str], diag: Dict) -> Optional[pd.DataFrame]:
    """Try to read Excel with multiple engines; fall back to auto header detection."""
    errors = []
    # Try explicit engines first (if installed)
    for eng in ("openpyxl", "xlrd", "pyxlsb"):
        try:
            __import__(eng)
            df = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0), engine=eng)
            diag.setdefault("excel_engines_used", []).append(eng)
            return df
        except ModuleNotFoundError as e:
            errors.append(f"{eng} not installed: {e}")
        except Exception as e:
            errors.append(f"{eng} failed: {e}")

    # Try letting pandas pick
    try:
        df = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0))
        diag.setdefault("excel_engines_used", []).append("auto")
        return df
    except Exception as e:
        errors.append(f"pandas auto engine failed: {e}")

    # Last resort: header None then detect header row
    try:
        df_raw = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0), header=None)
        hi = _auto_header_index(df_raw, expected_samples=["Title", "ID", "Team", "Iteration", "State", "Effort"])
        if hi is not None:
            df = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0), header=hi)
            diag.setdefault("header_autodetected", True)
            return df
        else:
            errors.append("Header auto-detect failed.")
    except Exception as e:
        errors.append(f"header=None strategy failed: {e}")

    diag["excel_errors"] = errors
    return None

def _read_csv_any(data: bytes, diag: Dict) -> Optional[pd.DataFrame]:
    """Try several encodings and separators; returns the first wide-enough parse."""
    encodings: List[str] = ["utf-8-sig", "utf-8", "cp1252", "latin-1", "utf-16", "utf-16le", "utf-16be"]
    seps: List[Optional[str]] = [",", ";", "\t", None]
    errors = []
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(io.BytesIO(data), encoding=enc, sep=sep, engine="python")
                if df.shape[1] >= 2:
                    diag.setdefault("csv_attempts", []).append({"encoding": enc, "sep": sep or "auto"})
                    return df
            except Exception as e:
                errors.append(f"csv {enc}/{sep or 'auto'} failed: {e}")
    diag["csv_errors"] = errors
    return None

def _auto_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]
    rename: Dict[str, str] = {}
    for canonical, candidates in EXPECTED.items():
        found = False
        # exact case-insensitive match
        for c in candidates:
            match = [col for col in df.columns if col.lower() == c.lower()]
            if match:
                rename[match[0]] = canonical
                found = True
                break
        if not found:
            # looser: contains canonical token
            match2 = [col for col in df.columns if canonical.lower() in col.lower()]
            if match2:
                rename[match2[0]] = canonical
    return df.rename(columns=rename)

def read_ado_upload_any(upl, sheet_name: Optional[str], effort_uses_comma: bool, diag: Dict) -> pd.DataFrame:
    """Robust reader for Excel/CSV. Heuristic: if there are NUL bytes, treat as Excel first."""
    if upl is None:
        raise ValueError("No file uploaded.")
    raw = upl.read()
    name_lower = (upl.name or "").lower()

    looks_like_excel = any(ext in name_lower for ext in (".xlsx", ".xlsm", ".xls", ".xlsb")) or (b"\x00" in raw)

    df: Optional[pd.DataFrame] = None
    if looks_like_excel:
        df = _read_excel_any(raw, sheet_name, diag)
        if df is None:
            df = _read_csv_any(raw, diag)
    else:
        df = _read_csv_any(raw, diag)
        if df is None:
            df = _read_excel_any(raw, sheet_name, diag)

    if df is None or df.empty:
        raise ValueError("Could not parse the ADO file. Try a clean XLSX (preferred) or CSV UTF‑8.")

    # Normalize common fields
    df = _auto_rename_columns(df)

    # Clean strings
    for col in ("Team", "Custom_ApplicationName", "Iteration", "Title", "State"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Effort numeric (support comma decimals)
    if "Effort" in df.columns:
        if effort_uses_comma:
            df["Effort"] = df["Effort"].astype(str).str.replace(",", ".", regex=False)
        df["Effort"] = pd.to_numeric(df["Effort"], errors="coerce")

    # ID as string
    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str).str.strip()

    # Dates
    for dcol in ("CreatedDate", "ChangedDate"):
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce")

    # Year (keep numeric if present)
    if "Year" in df.columns:
        df["Year"] = df["Year"].astype(str).str.replace(",", ".", regex=False)
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")

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
    # New: ADO_YEAR from Excel "Year"
    out["ADO_YEAR"]       = df.get("Year")

    out["EFFORT_POINTS"]  = pd.to_numeric(out["EFFORT_POINTS"], errors="coerce")
    out["CREATED_AT"]     = pd.to_datetime(out["CREATED_AT"], errors="coerce")
    out["CHANGED_AT"]     = pd.to_datetime(out["CHANGED_AT"], errors="coerce")
    try:
        out["ADO_YEAR"] = pd.to_numeric(out["ADO_YEAR"], errors="coerce").astype("Int64")
    except Exception:
        pass

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
    """Batch MERGE into ADO_FEATURES; returns number of rows passed. Ensures ADO_YEAR column exists."""
    if df.empty:
        return 0
    try:
        execute("ALTER TABLE ADO_FEATURES ADD COLUMN IF NOT EXISTS ADO_YEAR NUMBER(4)")
    except Exception:
        pass

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
            _to_py(r.get("ADO_YEAR")),
        ))
    rows = [t for t in rows if t[0] is not None]
    if not rows:
        return 0

    sql = """
    MERGE INTO ADO_FEATURES t
    USING (
      SELECT %s AS FEATURE_ID, %s AS TITLE, %s AS STATE,
             %s AS TEAM_RAW, %s AS APP_NAME_RAW, %s AS EFFORT_POINTS,
             %s AS ITERATION_PATH, %s AS CREATED_AT, %s AS CHANGED_AT,
             %s AS ADO_YEAR
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
      CHANGED_AT = s.CHANGED_AT,
      ADO_YEAR = s.ADO_YEAR
    WHEN NOT MATCHED THEN INSERT
      (FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH, CREATED_AT, CHANGED_AT, ADO_YEAR)
    VALUES
      (s.FEATURE_ID, s.TITLE, s.STATE, s.TEAM_RAW, s.APP_NAME_RAW, s.EFFORT_POINTS, s.ITERATION_PATH, s.CREATED_AT, s.CHANGED_AT, s.ADO_YEAR)
    """
    execute(sql, rows, many=True)
    return len(rows)

# -------------------------
# UI: Tabs (top-level)
# -------------------------
tab_load, tab_map, tab_explore, tab_recon = st.tabs([
    "📥 Load Data", "🧭 Map Values", "🔎 ADO Explorer", "📊 Reconciliation"
])

# =========================
# Tab: Load Data
# =========================
with tab_load:
    # DB sanity check
    try:
        info = fetch_df("SELECT COUNT(*) AS N FROM ADO_FEATURES")
        n_rows = int(info.iloc[0]["N"]) if not info.empty else 0
        st.info(f"Current rows in ADO_FEATURES: **{n_rows}**")
    except Exception:
        st.warning("Could not query ADO_FEATURES row count (check connection/secrets).")

    st.caption("Upload **XLSX/XLSM/XLSB/XLS** or **CSV**. Only raw ADO fields are stored (including Year if present).")
    upl = st.file_uploader("Upload ADO export", type=["xlsx", "xlsm", "xlsb", "xls", "csv"], key="upl_ado")

    sheet_name: Optional[str] = None
    effort_uses_comma = False

    if upl:
        # keep bytes so we can list sheets before parse
        upl.seek(0)
        file_bytes = upl.read()
        upl.seek(0)

        # Try to list sheets (best effort; harmless if CSV)
        sheets = _list_excel_sheets(file_bytes)
        if sheets:
            sheet_name = st.selectbox("Worksheet", sheets, index=0, help="Choose the tab to import", key="sheet_select")
        else:
            st.info("Could not list sheets. I will try to read the first sheet automatically (or parse as CSV).")

        effort_uses_comma = st.checkbox(
            "Effort uses comma as decimal (e.g., 1,5)",
            value=False,
            help="Enable if your Effort column has commas instead of dots.",
            key="effort_comma"
        )

        diag: Dict = {}
        if st.button("📄 Parse file", key="btn_parse"):
            upl.seek(0)
            try:
                df_raw = read_ado_upload_any(upl, sheet_name, effort_uses_comma, diag)
            except Exception as e:
                with st.expander("Diagnostics", expanded=True):
                    st.write("**Why it failed**")
                    st.exception(e)
                    st.write("**Parse attempts**")
                    st.json(diag)
                st.error("Could not parse the ADO file. Please install Excel engines (openpyxl/xlrd/pyxlsb) or re‑export as clean XLSX/CSV UTF‑8.")
                st.stop()

            # Normalize and persist
            df_norm = normalize_to_canonical(df_raw)
            st.session_state["ado_parsed_raw"] = df_raw
            st.session_state["ado_parsed_norm"] = df_norm

            st.success(f"Parsed {len(df_raw)} rows from {upl.name} and normalized {len(df_norm)} rows.")
            with st.expander("Preview (raw + normalized)"):
                show_all = st.checkbox("Show ALL rows (may be large)", value=False, key="show_all_rows")
                preview = pd.concat(
                    [df_raw.reset_index(drop=True), df_norm.reset_index(drop=True)],
                    axis=1
                )
                if show_all:
                    st.dataframe(preview, use_container_width=True, height=600)
                else:
                    st.dataframe(preview.head(300), use_container_width=True, height=600)

    # Upsert button: ONLY in this tab, guarded by session_state
    if st.session_state["ado_parsed_norm"] is not None:
        if st.button("⬆️ Upsert into ADO_FEATURES", type="primary", key="btn_upsert"):
            with st.spinner("Loading into Snowflake..."):
                n = upsert_ado_features(st.session_state["ado_parsed_norm"])
            # Re-check DB count after insert
            info2 = fetch_df("SELECT COUNT(*) AS N FROM ADO_FEATURES")
            n_rows2 = int(info2.iloc[0]["N"]) if not info2.empty else 0
            st.success(f"Upserted **{n}** rows into ADO_FEATURES. New total rows: **{n_rows2}**.")
    else:
        st.caption("Parse a file first to enable upsert.")

# Helper: DISTINCTs for ADO Explorer
def load_ado_distincts() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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

# =========================
# Tab: Map Values
# =========================
with tab_map:
    st.subheader("Map ADO values to TCO (no calculations)")

    # Live count so user sees if table has data
    cnt = fetch_df("SELECT COUNT(*) AS N FROM ADO_FEATURES")
    current_n = int(cnt.iloc[0]["N"]) if not cnt.empty else 0
    if current_n == 0:
        st.warning("ADO_FEATURES is empty. Load and upsert data in the first tab.")
    else:
        st.info(f"ADO_FEATURES currently has **{current_n}** rows.")

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

    team_maps = fetch_df("SELECT ADO_TEAM, TEAMID FROM MAP_ADO_TEAM_TO_TCO_TEAM ORDER BY ADO_TEAM")
    app_maps  = fetch_df("SELECT ADO_APP, APP_GROUP FROM MAP_ADO_APP_TO_TCO_GROUP ORDER BY ADO_APP")

    teams_df  = fetch_df("SELECT TEAMID, TEAMNAME FROM TEAMS ORDER BY TEAMNAME")
    groups_df = fetch_df("SELECT GROUPID, GROUPNAME FROM APPLICATION_GROUPS ORDER BY GROUPNAME")

    # ---- Teams mapping (Display TEAMNAME, store TEAMID)
    st.markdown("### ADO → TCO Team")
    if ado_teams.empty or teams_df.empty:
        st.info("Load features and create Teams first.")
    else:
        base_tm = ado_teams.rename(columns={"TEAM_RAW": "ADO_TEAM"}).copy()
        if not team_maps.empty:
            base_tm = base_tm.merge(team_maps, how="left", on="ADO_TEAM")
        else:
            base_tm["TEAMID"] = None

        id_to_name_team = {r.TEAMID: r.TEAMNAME for _, r in teams_df.iterrows()}
        name_to_id_team = {r.TEAMNAME: r.TEAMID for _, r in teams_df.iterrows()}
        base_tm["TCO_TEAMNAME"] = base_tm["TEAMID"].map(id_to_name_team)

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
        if st.button("💾 Save Team Mappings", key="btn_save_team_mappings"):
            rows = []
            for _, r in edited.iterrows():
                ado_val = str(r["ADO_TEAM"]).strip()
                tname = r.get("TCO_TEAMNAME")
                if not ado_val:
                    continue
                if tname and tname in name_to_id_team:
                    rows.append((ado_val, name_to_id_team[tname]))
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

    # ---- App group mapping (Display GROUPNAME, store GROUPID)
    st.markdown("### ADO → TCO App Group")
    if ado_apps.empty or groups_df.empty:
        st.info("Load features and create Application Groups first.")
    else:
        base_am = ado_apps.rename(columns={"APP_NAME_RAW": "ADO_APP"}).copy()
        if not app_maps.empty:
            base_am = base_am.merge(app_maps, how="left", on="ADO_APP")
        else:
            base_am["APP_GROUP"] = None  # should hold GROUPID when present

        # Build maps
        id_to_name_group = {r.GROUPID: r.GROUPNAME for _, r in groups_df.iterrows()}
        name_to_id_group = {r.GROUPNAME: r.GROUPID for _, r in groups_df.iterrows()}

        # Show name for current mapping (if APP_GROUP contains GROUPID)
        base_am["TCO_GROUPNAME"] = base_am["APP_GROUP"].map(id_to_name_group)

        edited2 = st.data_editor(
            base_am[["ADO_APP","TCO_GROUPNAME"]],
            use_container_width=True,
            height=360,
            num_rows="fixed",
            column_config={
                "ADO_APP": st.column_config.TextColumn("ADO App Name", disabled=True),
                "TCO_GROUPNAME": st.column_config.SelectboxColumn(
                    "TCO App Group",
                    options=groups_df["GROUPNAME"].tolist(),
                    required=False,
                    help="This picker shows group names. The mapping will store the group ID."
                ),
            },
            key="am_editor",
        )

        if st.button("💾 Save App Group Mappings", key="btn_save_app_group_mappings"):
            rows = []
            for _, r in edited2.iterrows():
                ado_val = str(r["ADO_APP"]).strip()
                gname = r.get("TCO_GROUPNAME")
                if not ado_val:
                    continue
                if gname and gname in name_to_id_group:
                    gid = name_to_id_group[gname]  # store the ID
                    rows.append((ado_val, gid))
                else:
                    # No selection → delete existing mapping (if any)
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

# =========================
# Tab: 🔎 ADO Explorer
# =========================
with tab_explore:
    st.subheader("What’s coming from ADO (raw import)")

    cfix, _ = st.columns([1, 5])
    with cfix:
        if callable(repair_ado_effort_points_precision):
            if st.button("🧹 Repair Effort Points Precision", key="btn_repair_effort_explorer"):
                try:
                    repair_ado_effort_points_precision()
                    st.success("Effort points precision repaired.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Repair failed: {e}")
        else:
            st.caption("Repair Effort Points action unavailable (helper not found).")

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

    # Raw latest features (ADO_FEATURES)
    st.markdown("#### Latest Features from ADO")
    df_raw = ado_features_base_query(where_sql, tuple(params) if params else None)
    st.dataframe(df_raw, use_container_width=True, height=340)

    # Legacy raw aggregation by ITERATION_PATH (keep)
    st.markdown("#### Effort Points by Iteration (raw path)")
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

    # Effort by Year + Iteration (Excel Year + parsed Iteration)
    st.markdown("#### Effort Points by Year & Iteration (Excel-based Year + parsed Iteration)")
    df_year_iter = fetch_df(f"""
      WITH base AS (
        SELECT
          ADO_YEAR,
          ITERATION_PATH,
          EFFORT_POINTS
        FROM ADO_FEATURES
        {where_sql}
      ),
      labeled AS (
        SELECT
          ADO_YEAR,
          COALESCE(
            REGEXP_SUBSTR(ITERATION_PATH, 'I[[:space:]]*([0-9]+)', 1, 1, 'i', 1),
            REGEXP_SUBSTR(ITERATION_PATH, 'ITERATION[[:space:]]*([0-9]+)', 1, 1, 'i', 1)
          ) AS ITER_NUM,
          EFFORT_POINTS
        FROM base
      )
      SELECT
        ADO_YEAR AS YEAR,
        CASE WHEN ITER_NUM IS NOT NULL THEN 'I' || ITER_NUM ELSE NULL END AS ITERATION,
        SUM(COALESCE(EFFORT_POINTS,0)) AS EFFORT_POINTS_SUM,
        COUNT(*) AS FEATURES
      FROM labeled
      GROUP BY ADO_YEAR, ITER_NUM
      ORDER BY YEAR, TRY_TO_NUMBER(ITER_NUM)
    """, tuple(params) if params else None)
    st.dataframe(df_year_iter, use_container_width=True, height=300)

    # Raw by Team & Iteration (keep)
    st.markdown("#### Effort Points by Team & Iteration (raw path)")
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

# =========================
# Tab: 📊 Reconciliation
# =========================
with tab_recon:
    st.subheader("Reconciliation: mappings + effort + calculated costs")
    st.caption("Now grouped by **Team, ADO Year & Iteration**. Costs per PI reflect the values used for those rows in the view.")

    # Quick issues panel
    colA, colB, colC = st.columns(3)
    try:
        unmapped_team = fetch_df("""
          SELECT TEAM_RAW, COUNT(*) AS N
          FROM ADO_FEATURES af
          LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM m ON m.ADO_TEAM = af.TEAM_RAW
          WHERE m.TEAMID IS NULL
          GROUP BY TEAM_RAW
          ORDER BY N DESC
        """)
        colA.metric("Features missing TEAM mapping", int(unmapped_team["N"].sum()) if not unmapped_team.empty else 0)
    except Exception as e:
        colA.warning(f"Unmapped calc failed: {e}")
        unmapped_team = pd.DataFrame()

    try:
        zero_denom = fetch_df("""
          SELECT COUNT(*) AS N
          FROM VW_TEAM_COSTS_PER_FEATURE
          WHERE (TEAMID IS NOT NULL) AND
                (COALESCE(DELIVERY_TEAM_FTE,0) + COALESCE(CONTRACTOR_CS_FTE,0) + COALESCE(CONTRACTOR_C_FTE,0)) = 0
        """)
        colB.metric("Rows with zero composition denominator", int(zero_denom.iloc[0]["N"]) if not zero_denom.empty else 0)
    except Exception as e:
        colB.warning(f"Zero-denom check failed: {e}")

    try:
        no_rate = fetch_df("""
          SELECT COUNT(*) AS N FROM VW_TEAM_COSTS_PER_FEATURE
          WHERE (TEAMID IS NOT NULL) AND
                (COALESCE(XOM_RATE,0)=0 OR COALESCE(CONTRACTOR_CS_RATE,0)=0 OR COALESCE(CONTRACTOR_C_RATE,0)=0)
        """)
        colC.metric("Rows with a missing rate", int(no_rate.iloc[0]["N"]) if not no_rate.empty else 0)
    except Exception as e:
        colC.warning(f"Rate check failed: {e}")

    # ===== Filters (add ADO Year) =====
    st.markdown("#### Filters")
    fc1, fc2, fc3, fc4 = st.columns([2,2,2,2])

    # Year options from data
    year_opts_df = fetch_df("SELECT DISTINCT ADO_YEAR FROM ADO_FEATURES WHERE ADO_YEAR IS NOT NULL ORDER BY ADO_YEAR")
    year_options = year_opts_df["ADO_YEAR"].dropna().astype(int).tolist() if not year_opts_df.empty else []
    year_sel = fc1.multiselect("ADO Year", options=year_options, default=[], key="recon_years")

    team_like2 = fc2.text_input("TCO Team contains", "", key="recon_team_contains")
    app_like2  = fc3.text_input("ADO App contains", "", key="recon_app_contains")
    iter_like2 = fc4.text_input("Iteration contains", "", key="recon_iter_contains")

    # Build WHERE using aliases v (view) and f (features)
    where2 = []
    params2: list = []
    if team_like2.strip():
        where2.append("UPPER(v.TEAMNAME) LIKE UPPER(%s)")
        params2.append(f"%{team_like2.strip()}%")
    if app_like2.strip():
        where2.append("UPPER(v.APP_NAME_RAW) LIKE UPPER(%s)")
        params2.append(f"%{app_like2.strip()}%")
    if iter_like2.strip():
        where2.append("UPPER(v.ITERATION_PATH) LIKE UPPER(%s)")
        params2.append(f"%{iter_like2.strip()}%")
    if year_sel:
        where2.append("f.ADO_YEAR IN (" + ",".join(["%s"] * len(year_sel)) + ")")
        params2.extend(year_sel)

    where_sql2 = " WHERE " + " AND ".join(where2) if where2 else ""

    # Pull detail with ADO_YEAR added (join view to features)
    try:
        base_sql = f"""
          SELECT
            v.TEAMNAME, v.TEAMID, v.FEATURE_ID, v.TITLE, v.APP_NAME_RAW,
            f.ADO_YEAR,
            v.ITERATION_PATH,
            v.EFFORT_POINTS,
            v.TEAM_COST_PERPI,
            v.DEL_TEAM_COST_PERPI,
            v.TEAM_CONTRACTOR_CS_COST_PERPI,
            v.TEAM_CONTRACTOR_C_COST_PERPI
          FROM VW_TEAM_COSTS_PER_FEATURE v
          LEFT JOIN ADO_FEATURES f
            ON f.FEATURE_ID = v.FEATURE_ID
          {where_sql2}
          ORDER BY v.TEAMNAME, f.ADO_YEAR, v.ITERATION_PATH, v.FEATURE_ID
        """
        df_calc = fetch_df(base_sql, tuple(params2) if params2 else None)
    except Exception as e:
        st.error(f"Could not read reconciliation data: {e}")
        df_calc = pd.DataFrame()

    if df_calc.empty:
        st.info("No rows found with the current filters.")
    else:
        # ===== Aggregates by Team, Year & Iteration =====
        st.markdown("#### Effort & Cost by Team, Year & Iteration")
        ag = df_calc.groupby(["TEAMNAME","ADO_YEAR","ITERATION_PATH"], dropna=False).agg(
            FEATURES=("FEATURE_ID","count"),
            EFFORT_POINTS_SUM=("EFFORT_POINTS","sum"),
            TEAM_COST_PERPI=("TEAM_COST_PERPI","sum"),
            DEL_TEAM_COST_PERPI=("DEL_TEAM_COST_PERPI","sum"),
            TEAM_CONTRACTOR_CS_COST_PERPI=("TEAM_CONTRACTOR_CS_COST_PERPI","sum"),
            TEAM_CONTRACTOR_C_COST_PERPI=("TEAM_CONTRACTOR_C_COST_PERPI","sum"),
        ).reset_index()

        # Ensure numeric types are floats (avoid Decimal/float mix)
        for col in [
            "TEAM_COST_PERPI",
            "DEL_TEAM_COST_PERPI",
            "TEAM_CONTRACTOR_CS_COST_PERPI",
            "TEAM_CONTRACTOR_C_COST_PERPI",
            "EFFORT_POINTS_SUM",
        ]:
            if col in ag.columns:
                ag[col] = pd.to_numeric(ag[col], errors="coerce").astype(float)

        ag["TOTAL_COST_PERPI"] = ag[
            ["TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]
        ].sum(axis=1)

        st.dataframe(ag, use_container_width=True, height=360)

        # ===== Per-Feature Detail (includes ADO_YEAR) =====
        st.markdown("#### Per-Feature Detail (to spot anomalies)")
        st.dataframe(df_calc, use_container_width=True, height=420)

        # ===== Unmapped teams list (unchanged) =====
        st.markdown("#### Unmapped ADO Teams (with counts)")
        st.dataframe(unmapped_team, use_container_width=True, height=220)

        # ===== Diagnostics for missing rates / zero composition =====
        st.markdown("#### Rows with missing rates or zero composition (diagnostics)")
        try:
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
        except Exception as e:
            st.warning(f"Diagnostics view failed: {e}")
