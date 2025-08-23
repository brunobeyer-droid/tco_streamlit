# pages/Sync ADO Features.py
import io
import uuid
import time
from datetime import date
from typing import Optional, List, Dict, Tuple, Any

import pandas as pd
import streamlit as st

from snowflake_db import (
    execute,
    fetch_df,
    ensure_ado_minimal_tables,
    ensure_team_calc_table,
    ensure_team_cost_view,
    repair_ado_effort_points_precision,

    # Lookups + upserts (align with your schema)
    list_programs,
    list_teams,
    list_vendors,
    list_application_groups,
    list_applications,
    upsert_program,
    upsert_team,
    upsert_vendor,
    upsert_application_group,
    upsert_application_instance,
    upsert_invoice,
)

# -------------------------
# Page setup
# -------------------------
st.set_page_config(page_title="Sync ADO Features", layout="wide")
st.title("🔄 Sync ADO Features (XLSX‑friendly, persistent upload)")

# Ensure minimal schema is ready
with st.spinner("Ensuring minimal ADO schema..."):
    ensure_ado_minimal_tables()
    if callable(ensure_team_calc_table):
        ensure_team_calc_table()
    if callable(ensure_team_cost_view):
        ensure_team_cost_view()

# -------------------------
# Session state
# -------------------------
for state_key, default in [
    ("ado_parsed_raw", None),
    ("ado_parsed_norm", None),
    ("one_sheet_df", None),
    ("colmap", {}),
    ("previews", {}),
]:
    if state_key not in st.session_state:
        st.session_state[state_key] = default

# -------------------------
# Column expectations (ADO)
# -------------------------
EXPECTED = {
    "Effort": ["Effort", "Story Points", "Effort Points", "EFFORT_POINTS"],
    "Team": ["Team", "System.Team", "Area Team"],
    "Custom_ApplicationName": ["Custom_ApplicationName", "Application", "App Name"],
    "Custom_InvestmentDimension": [
        "Custom_InvestmentDimension", 
        "Investment Dimension", 
        "INVESTMENT_DIMENSION"
    ],
    "Iteration": ["Iteration", "Iteration Path", "System.IterationPath", "Iteration.IterationLevel3.2"],
    "Title": ["Title", "System.Title"],
    "State": ["State", "System.State"],
    "ID": ["ID", "Work Item ID", "System.Id", "WorkItemId", "Work Item Id"],
    "CreatedDate": ["Created Date", "System.CreatedDate", "CreatedDate"],
    "ChangedDate": ["Changed Date", "System.ChangedDate", "ChangedDate"],
    "Year": ["Year", "ADO Year", "ADO_YEAR"],
}



# -------------------------
# Utilities
# -------------------------
def _table_count(table: str) -> int:
    try:
        df = fetch_df(f"SELECT COUNT(*) AS N FROM {table}")
        return int(df.iloc[0]["N"]) if df is not None and not df.empty else 0
    except Exception:
        return 0

def _get_preview(name: str) -> pd.DataFrame:
    obj = st.session_state.get("previews", {}).get(name)
    return obj if isinstance(obj, pd.DataFrame) else pd.DataFrame()

def _blank_or_nan(s) -> bool:
    """True for None, '', 'nan' (string), and values that stringify to blank."""
    if s is None:
        return True
    try:
        txt = str(s).strip()
    except Exception:
        return True
    return (txt == "") or (txt.lower() == "nan")

# -------------------------
# File parsing helpers
# -------------------------
def _auto_header_index(df_no_header: pd.DataFrame, expected_samples: List[str], max_scan: int = 20) -> Optional[int]:
    for i in range(min(max_scan, len(df_no_header))):
        row_vals = df_no_header.iloc[i].astype(str).str.strip().str.lower().tolist()
        hits = sum(1 for e in expected_samples if e.lower() in row_vals)
        if hits >= 2:
            return i
    return None

def _list_excel_sheets(data: bytes) -> List[str]:
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        return list(wb.sheetnames)
    except Exception:
        pass
    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=data)
        return wb.sheet_names()
    except Exception:
        pass
    try:
        from pyxlsb import open_workbook
        with open_workbook(fileobj=io.BytesIO(data)) as wb:
            return [s.name for s in wb.sheets]
    except Exception:
        pass
    return []

def _read_excel_any(data: bytes, sheet_name: Optional[str], diag: Dict[str, Any]) -> Optional[pd.DataFrame]:
    errors: List[str] = []
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
    try:
        df = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0))
        diag.setdefault("excel_engines_used", []).append("auto")
        return df
    except Exception as e:
        errors.append(f"pandas auto engine failed: {e}")
    try:
        df_raw = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0), header=None)
        hi = _auto_header_index(df_raw, expected_samples=["Title", "ID", "Team", "Iteration", "State", "Effort"])
        if hi is not None:
            df = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0), header=hi)
            diag.setdefault("header_autodetected", True)
            return df
        errors.append("Header auto-detect failed.")
    except Exception as e:
        errors.append(f"header=None strategy failed: {e}")

    diag["excel_errors"] = errors
    return None

def _read_csv_any(data: bytes, diag: Dict[str, Any]) -> Optional[pd.DataFrame]:
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1", "utf-16", "utf-16le", "utf-16be"]
    seps: List[Optional[str]] = [",", ";", "\t", None]
    errors: List[str] = []
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

def _read_file_any(upl, sheet_name: Optional[str], diag: Dict[str, Any]) -> pd.DataFrame:
    """
    IMPORTANT: never use `A or B` with DataFrames. Use explicit None/empty checks.
    """
    if upl is None:
        raise ValueError("No file uploaded.")
    upl.seek(0)
    raw = upl.read()
    upl.seek(0)
    name_lower = (upl.name or "").lower()
    looks_like_excel = any(ext in name_lower for ext in (".xlsx", ".xlsm", ".xls", ".xlsb")) or (b"\x00" in raw)

    df: Optional[pd.DataFrame] = None
    if looks_like_excel:
        df = _read_excel_any(raw, sheet_name, diag)
        if df is None or df.empty:
            df = _read_csv_any(raw, diag)
    else:
        df = _read_csv_any(raw, diag)
        if df is None or df.empty:
            df = _read_excel_any(raw, sheet_name, diag)

    if df is None or df.empty:
        raise ValueError("Could not parse the file. Try a clean XLSX (preferred) or CSV UTF‑8.")
    # Normalize column header BOM/whitespace
    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]
    return df

# -------------------------
# ADO parsing (core)
# -------------------------
def _auto_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]
    rename: Dict[str, str] = {}
    for canonical, candidates in EXPECTED.items():
        for cand in candidates:
            match = [col for col in df.columns if col.lower() == cand.lower()]
            if match:
                rename[match[0]] = canonical
                break
        else:
            match2 = [col for col in df.columns if canonical.lower() in col.lower()]
            if match2:
                rename[match2[0]] = canonical
    return df.rename(columns=rename)

def read_ado_upload_any(upl, sheet_name: Optional[str], effort_uses_comma: bool, diag: Dict[str, Any]) -> pd.DataFrame:
    if upl is None:
        raise ValueError("No file uploaded.")
    df = _read_file_any(upl, sheet_name, diag)
    df = _auto_rename_columns(df)

    for col in ("Team", "Custom_ApplicationName", "Iteration", "Title", "State", "Custom_InvestmentDimension"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()


    if "Effort" in df.columns:
        if effort_uses_comma:
            df["Effort"] = df["Effort"].astype(str).str.replace(",", ".", regex=False)
        df["Effort"] = pd.to_numeric(df["Effort"], errors="coerce")

    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str).str.strip()

    for dcol in ("CreatedDate", "ChangedDate"):
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce")

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
    out["ITERATION_PATH"] = df.get("Iteration") or df.get("Iteration.IterationLevel3.2")
    out["CREATED_AT"]     = df.get("CreatedDate")
    out["CHANGED_AT"]     = df.get("ChangedDate")
    out["ADO_YEAR"]       = df.get("Year")
    # NEW: Investment dimension (canonical)
    out["INVESTMENT_DIM"] = df.get("Custom_InvestmentDimension")

    # numeric & datetime coercion
    out["EFFORT_POINTS"]  = pd.to_numeric(out["EFFORT_POINTS"], errors="coerce")
    out["CREATED_AT"]     = pd.to_datetime(out["CREATED_AT"], errors="coerce")
    out["CHANGED_AT"]     = pd.to_datetime(out["CHANGED_AT"], errors="coerce")
    try:
        out["ADO_YEAR"] = pd.to_numeric(out["ADO_YEAR"], errors="coerce").astype("Int64")
    except Exception:
        pass

    # strip strings
    for c in ["TITLE","STATE","TEAM_RAW","APP_NAME_RAW","ITERATION_PATH","INVESTMENT_DIM"]:
        if c in out.columns:
            out[c] = out[c].astype(str).str.strip()

    # keep only non-empty FEATURE_ID
    out = out[~out["FEATURE_ID"].isna() & (out["FEATURE_ID"].astype(str).str.len() > 0)].copy()
    return out.reset_index(drop=True)


def _to_py(v: Any):
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

    # Ensure columns exist
    try:
        execute("ALTER TABLE ADO_FEATURES ADD COLUMN IF NOT EXISTS ADO_YEAR NUMBER(4)")
    except Exception:
        pass
    try:
        execute("ALTER TABLE ADO_FEATURES ADD COLUMN IF NOT EXISTS INVESTMENT_DIMENSION STRING")
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
            _to_py(r.get("INVESTMENT_DIM")),  # NEW
        ))
    rows = [t for t in rows if t[0] is not None]
    if not rows:
        return 0

    # IMPORTANT: use underscore-only aliases; no dots in identifiers.
    sql = """
    MERGE INTO ADO_FEATURES t
    USING (
      SELECT
        %s AS FEATURE_ID,
        %s AS TITLE,
        %s AS STATE,
        %s AS TEAM_RAW,
        %s AS APP_NAME_RAW,
        %s AS EFFORT_POINTS,
        %s AS ITERATION_PATH,
        %s AS CREATED_AT,
        %s AS CHANGED_AT,
        %s AS ADO_YEAR,
        %s AS INVESTMENT_DIMENSION
    ) s
    ON t.FEATURE_ID = s.FEATURE_ID
    WHEN MATCHED THEN UPDATE SET
        TITLE                = s.TITLE,
        STATE                = s.STATE,
        TEAM_RAW             = s.TEAM_RAW,
        APP_NAME_RAW         = s.APP_NAME_RAW,
        EFFORT_POINTS        = s.EFFORT_POINTS,
        ITERATION_PATH       = s.ITERATION_PATH,
        CREATED_AT           = s.CREATED_AT,
        CHANGED_AT           = s.CHANGED_AT,
        ADO_YEAR             = s.ADO_YEAR,
        INVESTMENT_DIMENSION = s.INVESTMENT_DIMENSION
    WHEN NOT MATCHED THEN INSERT (
        FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS,
        ITERATION_PATH, CREATED_AT, CHANGED_AT, ADO_YEAR, INVESTMENT_DIMENSION
    ) VALUES (
        s.FEATURE_ID, s.TITLE, s.STATE, s.TEAM_RAW, s.APP_NAME_RAW, s.EFFORT_POINTS,
        s.ITERATION_PATH, s.CREATED_AT, s.CHANGED_AT, s.ADO_YEAR, s.INVESTMENT_DIMENSION
    )
    """
    execute(sql, rows, many=True)
    return len(rows)


def ado_features_base_query(where_sql: str = "", params: Optional[tuple] = None) -> pd.DataFrame:
    sql = f"""
      SELECT FEATURE_ID, TITLE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH, CHANGED_AT, INVESTMENT_DIMENSION
      FROM ADO_FEATURES
      {where_sql}
      ORDER BY COALESCE(CHANGED_AT, TO_TIMESTAMP_NTZ('1900-01-01')) DESC, FEATURE_ID
    """
    df = fetch_df(sql, params)
    if df is not None and "EFFORT_POINTS" in df.columns:
        df["EFFORT_POINTS"] = pd.to_numeric(df["EFFORT_POINTS"], errors="coerce")
    return df if df is not None else pd.DataFrame()


# -------------------------
# Tabs (top-level)
# -------------------------
tab_load, tab_map, tab_explore, tab_recon, tab_bulk = st.tabs([
    "📥 Load Data", "🧭 Map Values", "🔎 ADO Explorer", "📊 Reconciliation", "📦 Load Invoices & Teams"
])

# =========================
# Tab: Load Data (ADO)
# =========================
with tab_load:
    try:
        info = fetch_df("SELECT COUNT(*) AS N FROM ADO_FEATURES")
        n_rows = int(info.iloc[0]["N"]) if info is not None and not info.empty else 0
        st.info(f"Current rows in ADO_FEATURES: **{n_rows}**")
    except Exception:
        st.warning("Could not query ADO_FEATURES row count (check connection/secrets).")

    st.caption("Upload **XLSX/XLSM/XLSB/XLS** or **CSV**. Only raw ADO fields are stored (including Year if present).")
    upl = st.file_uploader("Upload ADO export", type=["xlsx", "xlsm", "xlsb", "xls", "csv"], key="upl_ado")

    sheet_name: Optional[str] = None
    effort_uses_comma = False

    if upl:
        upl.seek(0); file_bytes = upl.read(); upl.seek(0)
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

        diag: Dict[str, Any] = {}
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
                st.dataframe((preview if show_all else preview.head(300)), use_container_width=True, height=600)

    if st.session_state["ado_parsed_norm"] is not None:
        if st.button("⬆️ Upsert into ADO_FEATURES", type="primary", key="btn_upsert"):
            with st.spinner("Loading into Snowflake..."):
                n = upsert_ado_features(st.session_state["ado_parsed_norm"])
            info2 = fetch_df("SELECT COUNT(*) AS N FROM ADO_FEATURES")
            n_rows2 = int(info2.iloc[0]["N"]) if info2 is not None and not info2.empty else 0
            st.success(f"Upserted **{n}** rows into ADO_FEATURES. New total rows: **{n_rows2}**.")
    else:
        st.caption("Parse a file first to enable upsert.")

# =========================
# Tab: Map Values
# =========================
with tab_map:
    st.subheader("Map ADO values to TCO (no calculations)")

    cnt = fetch_df("SELECT COUNT(*) AS N FROM ADO_FEATURES")
    current_n = int(cnt.iloc[0]["N"]) if cnt is not None and not cnt.empty else 0
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

    st.markdown("### ADO → TCO Team")
    if ado_teams is None or ado_teams.empty or teams_df is None or teams_df.empty:
        st.info("Load features and create Teams first.")
    else:
        base_tm = ado_teams.rename(columns={"TEAM_RAW": "ADO_TEAM"}).copy()
        if team_maps is not None and not team_maps.empty:
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
                    options=teams_df["TEAMNAME"].tolist() if teams_df is not None and not teams_df.empty else [],
                    required=False,
                ),
            },
            key="tm_editor",
        )
        if st.button("💾 Save Team Mappings", key="btn_save_team_mappings"):
            rows_to_upsert: List[Tuple[str, str]] = []
            for _, row in edited.iterrows():
                ado_val = str(row["ADO_TEAM"]).strip()
                tname = row.get("TCO_TEAMNAME")
                if _blank_or_nan(ado_val):
                    continue
                if tname and tname in name_to_id_team:
                    rows_to_upsert.append((ado_val, name_to_id_team[tname]))
                else:
                    execute("DELETE FROM MAP_ADO_TEAM_TO_TCO_TEAM WHERE ADO_TEAM = %s", (ado_val,))
            if rows_to_upsert:
                merge_sql = """
                MERGE INTO MAP_ADO_TEAM_TO_TCO_TEAM t
                USING (SELECT %s AS ADO_TEAM, %s AS TEAMID) s
                ON t.ADO_TEAM = s.ADO_TEAM
                WHEN MATCHED THEN UPDATE SET TEAMID = s.TEAMID
                WHEN NOT MATCHED THEN INSERT (ADO_TEAM, TEAMID) VALUES (s.ADO_TEAM, s.TEAMID)
                """
                execute(merge_sql, rows_to_upsert, many=True)
            st.success("Team mappings saved.")

    st.markdown("---")

    st.markdown("### ADO → TCO App Group")
    if ado_apps is None or ado_apps.empty or groups_df is None or groups_df.empty:
        st.info("Load features and create Application Groups first.")
    else:
        base_am = ado_apps.rename(columns={"APP_NAME_RAW": "ADO_APP"}).copy()
        if app_maps is not None and not app_maps.empty:
            base_am = base_am.merge(app_maps, how="left", on="ADO_APP")
        else:
            base_am["APP_GROUP"] = None

        id_to_name_group = {r.GROUPID: r.GROUPNAME for _, r in groups_df.iterrows()}
        name_to_id_group = {r.GROUPNAME: r.GROUPID for _, r in groups_df.iterrows()}
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
                    options=groups_df["GROUPNAME"].tolist() if groups_df is not None and not groups_df.empty else [],
                    required=False,
                    help="This picker shows group names. The mapping will store the group ID."
                ),
            },
            key="am_editor",
        )
        if st.button("💾 Save App Group Mappings", key="btn_save_app_group_mappings"):
            rows_to_upsert: List[Tuple[str, str]] = []
            for _, row in edited2.iterrows():
                ado_val = str(row["ADO_APP"]).strip()
                gname = row.get("TCO_GROUPNAME")
                if _blank_or_nan(ado_val):
                    continue
                if gname and gname in name_to_id_group:
                    gid = name_to_id_group[gname]
                    rows_to_upsert.append((ado_val, gid))
                else:
                    execute("DELETE FROM MAP_ADO_APP_TO_TCO_GROUP WHERE ADO_APP = %s", (ado_val,))
            if rows_to_upsert:
                merge_sql = """
                MERGE INTO MAP_ADO_APP_TO_TCO_GROUP t
                USING (SELECT %s AS ADO_APP, %s AS APP_GROUP) s
                ON t.ADO_APP = s.ADO_APP
                WHEN MATCHED THEN UPDATE SET APP_GROUP = s.APP_GROUP
                WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP)
                """
                execute(merge_sql, rows_to_upsert, many=True)
            st.success("App Group mappings saved.")
# -------------------------
# Distinct lookups from ADO_FEATURES (used by Explorer tab)
# -------------------------
from typing import Tuple

def load_ado_distincts() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (teams, apps, iterations) distincts from ADO_FEATURES."""
    teams = fetch_df("""
        SELECT DISTINCT TEAM_RAW
        FROM ADO_FEATURES
        WHERE TEAM_RAW IS NOT NULL AND TRIM(TEAM_RAW) <> ''
        ORDER BY TEAM_RAW
    """)
    apps = fetch_df("""
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

def load_ado_investment_dimensions() -> pd.DataFrame:
    """Optional: distinct Investment Dimensions for filters/metrics."""
    return fetch_df("""
        SELECT DISTINCT INVESTMENT_DIMENSION
        FROM ADO_FEATURES
        WHERE INVESTMENT_DIMENSION IS NOT NULL AND TRIM(INVESTMENT_DIMENSION) <> ''
        ORDER BY INVESTMENT_DIMENSION
    """)

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
    invdim_like = f3.text_input("Investment Dimension contains", "", key="exp_invdim_contains")  # reuse a column or add a new one

    where: List[str] = []
    params: List[str] = []
    if team_like.strip():
        where.append("UPPER(TEAM_RAW) LIKE UPPER(%s)")
        params.append(f"%{team_like.strip()}%")
    if app_like.strip():
        where.append("UPPER(APP_NAME_RAW) LIKE UPPER(%s)")
        params.append(f"%{app_like.strip()}%")
    if iter_like.strip():
        where.append("UPPER(ITERATION_PATH) LIKE UPPER(%s)")
        params.append(f"%{iter_like.strip()}%")
    if invdim_like.strip():
        where.append("UPPER(INVESTMENT_DIMENSION) LIKE UPPER(%s)")
        params.append(f"%{invdim_like.strip()}%")
    where_sql = " WHERE " + " AND ".join(where) if where else ""

    st.markdown("#### Latest Features from ADO")
    df_raw = ado_features_base_query(where_sql, tuple(params) if params else None)
    st.dataframe(df_raw, use_container_width=True, height=340)

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

    st.markdown("#### Effort Points by Year & Iteration (Excel-based Year + parsed Iteration)")
    df_year_iter = fetch_df(f"""
      WITH base AS (
        SELECT ADO_YEAR, ITERATION_PATH, EFFORT_POINTS
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

# =========================================================
# 📊 Reconciliation
# =========================================================
with tab_recon:
    st.subheader("Reconciliation: mappings + effort + calculated costs")
    st.caption("TEAM_COST_PERPI is split equally across all features within each Team × Year × Iteration.")

    colA, colB, colC = st.columns(3)
    try:
        unmapped_team = fetch_df("""
          SELECT TEAM_RAW, COUNT(*) AS N
          FROM VW_TEAM_COSTS_PER_FEATURE v
          WHERE v.TEAMID IS NULL
          GROUP BY TEAM_RAW
          ORDER BY N DESC
        """)
        colA.metric("Features missing TEAM mapping", int(unmapped_team["N"].sum()) if unmapped_team is not None and not unmapped_team.empty else 0)
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
        colB.metric("Rows with zero composition denominator", int(zero_denom.iloc[0]["N"]) if zero_denom is not None and not zero_denom.empty else 0)
    except Exception as e:
        colB.warning(f"Zero-denom check failed: {e}")

    try:
        no_rate = fetch_df("""
          SELECT COUNT(*) AS N FROM VW_TEAM_COSTS_PER_FEATURE
          WHERE (TEAMID IS NOT NULL) AND
                (COALESCE(XOM_RATE,0)=0 OR COALESCE(CONTRACTOR_CS_RATE,0)=0 OR COALESCE(CONTRACTOR_C_RATE,0)=0)
        """)
        colC.metric("Rows with a missing rate", int(no_rate.iloc[0]["N"]) if no_rate is not None and not no_rate.empty else 0)
    except Exception as e:
        colC.warning(f"Rate check failed: {e}")

    st.markdown("#### Filters")
    fc1, fc2, fc3 = st.columns([2,2,2])
    team_like2 = fc1.text_input("TCO Team contains", "", key="recon_team_contains")
    app_like2  = fc2.text_input("ADO App contains", "", key="recon_app_contains")
    iter_like2 = fc3.text_input("Iteration contains", "", key="recon_iter_contains")

    where2: List[str] = []
    params2: List[str] = []
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

    try:
        base_sql = f"""
        WITH v AS (
          SELECT
            TEAMNAME, TEAMID,
            COALESCE(ADO_YEAR, YEAR(COALESCE(CHANGED_AT, CREATED_AT))) AS ADO_YEAR,
            COALESCE(ITERATION_NUM, TRY_TO_NUMBER(REGEXP_SUBSTR(ITERATION_PATH,'I[[:space:]]*([0-9]+)',1,1,'i',1))) AS ITERATION_NUM,
            ITERATION_PATH,
            FEATURE_ID, TITLE, APP_NAME_RAW, EFFORT_POINTS,
            TEAMFTE, XOM_RATE,
            DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE,
            TEAM_COST_PERPI,
            DEL_TEAM_COST_PERPI,
            TEAM_CONTRACTOR_CS_COST_PERPI,
            TEAM_CONTRACTOR_C_COST_PERPI
          FROM VW_TEAM_COSTS_PER_FEATURE
          {where_sql2}
        ),
        tp AS (
          SELECT
            TEAMID, ADO_YEAR, ITERATION_NUM,
            COUNT(*) AS FEATURES_IN_PI,
            MAX(COALESCE(TEAMFTE,0) * COALESCE(XOM_RATE,0) / 4.0) AS TEAM_PI_FIXED_COST
          FROM v
          GROUP BY TEAMID, ADO_YEAR, ITERATION_NUM
        )
        SELECT
          v.*,
          tp.FEATURES_IN_PI,
          tp.TEAM_PI_FIXED_COST,
          CASE WHEN tp.FEATURES_IN_PI > 0
               THEN tp.TEAM_PI_FIXED_COST / tp.FEATURES_IN_PI
               ELSE 0
          END AS TEAM_COST_PERPI_EQSPLIT
        FROM v
        LEFT JOIN tp
          ON tp.TEAMID = v.TEAMID
         AND tp.ADO_YEAR = v.ADO_YEAR
         AND tp.ITERATION_NUM = v.ITERATION_NUM
        ORDER BY v.TEAMNAME, v.ADO_YEAR, v.ITERATION_NUM, v.FEATURE_ID
        """
        df_calc = fetch_df(base_sql, tuple(params2) if params2 else None)
    except Exception as e:
        st.error(f"Could not read VW_TEAM_COSTS_PER_FEATURE: {e}")
        df_calc = pd.DataFrame()

    if df_calc is None or df_calc.empty:
        st.info("No rows found with the current filters.")
        st.stop()

    if "TEAM_COST_PERPI_EQSPLIT" in df_calc.columns:
        df_calc["TEAM_COST_PERPI"] = pd.to_numeric(df_calc["TEAM_COST_PERPI_EQSPLIT"], errors="coerce").fillna(0.0)

    st.markdown("#### Effort & Cost by Team, Year & Iteration")
    ag = df_calc.groupby(["TEAMNAME","ADO_YEAR","ITERATION_NUM"], dropna=False).agg(
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
    cols_order = [
        "TEAMNAME","ADO_YEAR","ITERATION_NUM","ITERATION_PATH",
        "FEATURE_ID","TITLE","APP_NAME_RAW","EFFORT_POINTS",
        "FEATURES_IN_PI","TEAM_PI_FIXED_COST","TEAM_COST_PERPI",
        "DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI",
        "INVESTMENT_DIMENSION"
    ]
    cols_present = [c for c in cols_order if c in df_calc.columns]
    st.dataframe(df_calc[cols_present], use_container_width=True, height=460)

    st.markdown("#### Unmapped ADO Teams (with counts)")
    st.dataframe(unmapped_team, use_container_width=True, height=220)

# =========================================================
# 📦 Bulk Load (ONE sheet): Programs, Vendors, App Groups, Applications, Teams & Invoices
# =========================================================
with tab_bulk:
    st.subheader("Bulk Load (one sheet): Programs, Vendors, App Groups, Applications, Teams & Invoices")
    st.caption("Upload a single sheet that contains the columns for all sections. Map once, preview auto‑generates, then MERGE in a safe order with UUIDs.")

    upl_one = st.file_uploader("Upload workbook (XLSX preferred; CSV allowed)", type=["xlsx","xlsm","xlsb","xls","csv"], key="one_workbook")
    sheet = None
    if upl_one:
        upl_one.seek(0); raw = upl_one.read(); upl_one.seek(0)
        sheets = _list_excel_sheets(raw)
        if sheets:
            sheet = st.selectbox("Worksheet (one sheet for everything)", sheets, index=0, key="one_sheet_select")
        else:
            st.info("No sheet list available (CSV or detection failed). I will parse the first/only sheet.")

        diag: Dict[str, Any] = {}
        try:
            df_src = _read_file_any(upl_one, sheet, diag)
            # De-duplicate column labels for display logic
            seen: Dict[str, int] = {}
            newcols: List[str] = []
            for col_lbl in df_src.columns:
                key_lbl = col_lbl
                if key_lbl in seen:
                    seen[key_lbl] += 1
                    key_lbl = f"{col_lbl}__{seen[col_lbl]}"
                else:
                    seen[key_lbl] = 0
                newcols.append(key_lbl)
            df_src.columns = newcols

            st.session_state["one_sheet_df"] = df_src
            st.success(f"Parsed {len(df_src)} rows from {upl_one.name}.")
        except Exception as e:
            with st.expander("Diagnostics", expanded=True):
                st.write("**Why it failed**")
                st.exception(e)
                st.write("**Parse attempts**")
                st.json(diag)
            st.error("Could not parse the workbook. Try a clean XLSX/CSV with headers.")
            st.stop()

    # -------------------------
    # Column mapping (single source)
    # -------------------------
    if st.session_state["one_sheet_df"] is not None:
        df_src = st.session_state["one_sheet_df"]
        cols = df_src.columns.tolist()

        def _default_pick(name: str) -> Optional[str]:
            lower = {c.lower(): c for c in cols}
            if name.lower() in lower:
                return lower[name.lower()]
            matches = [c for c in cols if name.lower() in c.lower()]
            return matches[0] if matches else None

        # Initialize mapping once
        if not st.session_state["colmap"]:
            st.session_state["colmap"] = {
                # Programs
                "PROGRAMNAME": _default_pick("PROGRAMNAME") or _default_pick("PROGRAM"),
                # Vendors
                "VENDORNAME": _default_pick("VENDORNAME") or _default_pick("VENDOR"),
                # App Groups
                "GROUPNAME": _default_pick("APP GROUP NAME") or _default_pick("GROUPNAME") or _default_pick("GROUP"),
                # Applications
                "APPNAME": _default_pick("APPNAME") or _default_pick("APPLICATION"),
                # Teams
                "TEAMNAME": _default_pick("TEAMNAME") or _default_pick("TEAM"),
                # Invoices (required)
                "AMOUNT_NEXT_YEAR": _default_pick("AMOUNT_NEXT_YEAR") or _default_pick("AMOUNT NEXT YEAR"),
                "AMOUNT": _default_pick("AMOUNT"),
                "FISCAL_YEAR": _default_pick("FISCAL_YEAR") or _default_pick("YEAR"),
                "RENEWAL_MONTH": _default_pick("RENEWAL_MONTH") or _default_pick("RENEWAL MONTH"),
                # Invoices (optional)
                "CONTRACT_ACTIVE": _default_pick("CONTRACT_ACTIVE"),
                "SERIAL_NUMBER": _default_pick("SERIAL_NUMBER"),
                "WORK_ORDER": _default_pick("WORK_ORDER"),
                "COMPANY_CODE": _default_pick("COMPANY_CODE"),
                "COST_CENTER": _default_pick("COST_CENTER"),
                "PRODUCT_OWNER": _default_pick("PRODUCT_OWNER"),
                "NOTES": _default_pick("NOTES"),
            }

        cm: Dict[str, Optional[str]] = st.session_state["colmap"]

        st.markdown("### Column Mapping")
        c1, c2, c3 = st.columns(3)

        with c1:
            st.markdown("**Program**")
            cm["PROGRAMNAME"] = st.selectbox("PROGRAMNAME (required)", options=["(none)"] + cols,
                                             index=(cols.index(cm["PROGRAMNAME"]) + 1) if cm.get("PROGRAMNAME") in cols else 0,
                                             key="map_programname")
            st.markdown("**Vendor**")
            cm["VENDORNAME"] = st.selectbox("VENDORNAME (required)", options=["(none)"] + cols,
                                            index=(cols.index(cm["VENDORNAME"]) + 1) if cm.get("VENDORNAME") in cols else 0,
                                            key="map_vendorname")
            st.markdown("**Application Group**")
            cm["GROUPNAME"] = st.selectbox("APP GROUP NAME (required)", options=["(none)"] + cols,
                                           index=(cols.index(cm["GROUPNAME"]) + 1) if cm.get("GROUPNAME") in cols else 0,
                                           key="map_groupname")

        with c2:
            st.markdown("**Applications**")
            cm["APPNAME"] = st.selectbox("APPNAME (required)", options=["(none)"] + cols,
                                         index=(cols.index(cm["APPNAME"]) + 1) if cm.get("APPNAME") in cols else 0,
                                         key="map_appname")
            st.markdown("**Teams**")
            cm["TEAMNAME"] = st.selectbox("TEAMNAME (required)", options=["(none)"] + cols,
                                          index=(cols.index(cm["TEAMNAME"]) + 1) if cm.get("TEAMNAME") in cols else 0,
                                          key="map_teamname")

        with c3:
            st.markdown("**Invoices (required)**")
            cm["AMOUNT_NEXT_YEAR"] = st.selectbox("AMOUNT_NEXT_YEAR", options=["(none)"] + cols,
                                                  index=(cols.index(cm["AMOUNT_NEXT_YEAR"]) + 1) if cm.get("AMOUNT_NEXT_YEAR") in cols else 0,
                                                  key="map_inv_amt_next")
            cm["AMOUNT"] = st.selectbox("AMOUNT", options=["(none)"] + cols,
                                        index=(cols.index(cm["AMOUNT"]) + 1) if cm.get("AMOUNT") in cols else 0,
                                        key="map_inv_amt")
            cm["FISCAL_YEAR"] = st.selectbox("FISCAL_YEAR (Year)", options=["(none)"] + cols,
                                             index=(cols.index(cm["FISCAL_YEAR"]) + 1) if cm.get("FISCAL_YEAR") in cols else 0,
                                             key="map_inv_fy")
            cm["RENEWAL_MONTH"] = st.selectbox("RENEWAL_MONTH (1-12)", options=["(none)"] + cols,
                                               index=(cols.index(cm["RENEWAL_MONTH"]) + 1) if cm.get("RENEWAL_MONTH") in cols else 0,
                                               key="map_inv_rmonth")

            st.markdown("**Invoices (optional)**")
            for opt_key, label in [
                ("CONTRACT_ACTIVE", "CONTRACT_ACTIVE"),
                ("SERIAL_NUMBER", "SERIAL_NUMBER"),
                ("WORK_ORDER", "WORK_ORDER"),
                ("COMPANY_CODE", "COMPANY_CODE"),
                ("COST_CENTER", "COST_CENTER"),
                ("PRODUCT_OWNER", "PRODUCT_OWNER"),
                ("NOTES", "NOTES"),
            ]:
                current = cm.get(opt_key)
                cm[opt_key] = st.selectbox(label, options=["(none)"] + cols,
                                           index=(cols.index(current) + 1) if current in cols else 0,
                                           key=f"map_opt_{opt_key}")

        st.markdown("---")

        # -------------------------
        # Build auto-previews from one sheet using the mapping
        # -------------------------
        def _col(name: str) -> Optional[str]:
            val = cm.get(name)
            return val if val and val != "(none)" and val in df_src.columns else None

        def _safe_num(series: pd.Series) -> pd.Series:
            return pd.to_numeric(series.astype(str).str.replace(",", ".", regex=False), errors="coerce")

        # Programs preview
        dfP = pd.DataFrame()
        if _col("PROGRAMNAME"):
            tmp = df_src[[_col("PROGRAMNAME")]].rename(columns={_col("PROGRAMNAME"): "PROGRAMNAME"})
            tmp["PROGRAMNAME"] = tmp["PROGRAMNAME"].astype(str).str.strip()
            tmp = tmp[~tmp["PROGRAMNAME"].apply(_blank_or_nan)]
            dfP = tmp.drop_duplicates(subset=["PROGRAMNAME"]).reset_index(drop=True)

        # Vendors preview
        dfV = pd.DataFrame()
        if _col("VENDORNAME"):
            tmp = df_src[[_col("VENDORNAME")]].rename(columns={_col("VENDORNAME"): "VENDORNAME"})
            tmp["VENDORNAME"] = tmp["VENDORNAME"].astype(str).str.strip()
            tmp = tmp[~tmp["VENDORNAME"].apply(_blank_or_nan)]
            dfV = tmp.drop_duplicates(subset=["VENDORNAME"]).reset_index(drop=True)

        # Groups preview
        dfG = pd.DataFrame()
        if _col("GROUPNAME"):
            tmp = df_src[[_col("GROUPNAME")]].rename(columns={_col("GROUPNAME"): "GROUPNAME"})
            tmp["GROUPNAME"] = tmp["GROUPNAME"].astype(str).str.strip()
            tmp = tmp[~tmp["GROUPNAME"].apply(_blank_or_nan)]
            if _col("TEAMNAME"):
                tmp["TEAMNAME"] = df_src[_col("TEAMNAME")].astype(str).str.strip()
            if _col("PROGRAMNAME"):
                tmp["PROGRAMNAME"] = df_src[_col("PROGRAMNAME")].astype(str).str.strip()
            if _col("VENDORNAME"):
                tmp["DEFAULT_VENDORNAME"] = df_src[_col("VENDORNAME")].astype(str).str.strip()
            keep_cols = ["GROUPNAME"] + [c for c in ["TEAMNAME","PROGRAMNAME","DEFAULT_VENDORNAME"] if c in tmp.columns]
            dfG = tmp[keep_cols].drop_duplicates("GROUPNAME").reset_index(drop=True)

        # Applications preview
        dfA = pd.DataFrame()
        if _col("APPNAME"):
            tmp = df_src[[_col("APPNAME")]].rename(columns={_col("APPNAME"): "APPNAME"})
            tmp["APPNAME"] = tmp["APPNAME"].astype(str).str.strip()
            tmp = tmp[~tmp["APPNAME"].apply(_blank_or_nan)]
            if _col("GROUPNAME"):
                tmp["GROUPNAME"] = df_src[_col("GROUPNAME")].astype(str).str.strip()
            dfA = tmp.drop_duplicates(subset=["APPNAME","GROUPNAME"] if "GROUPNAME" in tmp.columns else ["APPNAME"]).reset_index(drop=True)

        # Teams preview
        dfT = pd.DataFrame()
        if _col("TEAMNAME"):
            tmp = df_src[[_col("TEAMNAME")]].rename(columns={_col("TEAMNAME"): "TEAMNAME"})
            tmp["TEAMNAME"] = tmp["TEAMNAME"].astype(str).str.strip()
            tmp = tmp[~tmp["TEAMNAME"].apply(_blank_or_nan)]
            if _col("PROGRAMNAME"):
                tmp["PROGRAMNAME"] = df_src[_col("PROGRAMNAME")].astype(str).str.strip()
            dfT = tmp.drop_duplicates(subset=["TEAMNAME"]).reset_index(drop=True)

        # -------------------------
        # Invoices preview (row-level requireds + validity)
        # -------------------------
        REQUIRED_KEYS = [
            "PROGRAMNAME",      # Program Name
            "TEAMNAME",         # Team Name
            "GROUPNAME",        # Application Group
            "APPNAME",          # Application Instance
            "VENDORNAME",       # Vendor Name
            "AMOUNT",           # Amount
            "AMOUNT_NEXT_YEAR", # Amount Next Year
            "FISCAL_YEAR",      # Year
            "RENEWAL_MONTH",    # Renewal Month
        ]

        def _has_required_mapping() -> Dict[str, bool]:
            return {key: bool(_col(key)) for key in REQUIRED_KEYS}

        def _row_is_missing(series_val: Any) -> bool:
            if series_val is None:
                return True
            s = str(series_val).strip()
            return s == "" or s.lower() == "nan"

        dfI = pd.DataFrame()
        invoice_diag = {"column_mapping": _has_required_mapping(), "row_counts": {}}
        missing_invoice_reqs = [key for key, ok in invoice_diag["column_mapping"].items() if not ok]

        if not missing_invoice_reqs:
            out = pd.DataFrame()
            out["AMOUNT_NEXT_YEAR"] = _safe_num(df_src[_col("AMOUNT_NEXT_YEAR")])
            out["AMOUNT"] = _safe_num(df_src[_col("AMOUNT")])
            out["FISCAL_YEAR"] = pd.to_numeric(df_src[_col("FISCAL_YEAR")], errors="coerce").astype("Int64")
            out["RENEWAL_MONTH"] = pd.to_numeric(df_src[_col("RENEWAL_MONTH")], errors="coerce").astype("Int64")

            # required name fields
            out["PROGRAMNAME"] = df_src[_col("PROGRAMNAME")].astype(str).str.strip()
            out["TEAMNAME"] = df_src[_col("TEAMNAME")].astype(str).str.strip()
            out["GROUPNAME"] = df_src[_col("GROUPNAME")].astype(str).str.strip()
            out["APPNAME"] = df_src[_col("APPNAME")].astype(str).str.strip()
            out["VENDORNAME"] = df_src[_col("VENDORNAME")].astype(str).str.strip()

            # optional extras
            for opt_name in ["CONTRACT_ACTIVE","SERIAL_NUMBER","WORK_ORDER","COMPANY_CODE","COST_CENTER","PRODUCT_OWNER","NOTES"]:
                out[opt_name] = df_src[_col(opt_name)] if _col(opt_name) else None

            # Derived date for preview
            def _mk_date(row: pd.Series) -> Optional[date]:
                fy = row.get("FISCAL_YEAR")
                m  = row.get("RENEWAL_MONTH")
                try:
                    if pd.isna(fy) or pd.isna(m): 
                        return None
                    mm = int(m); yy = int(fy)
                    if 1 <= mm <= 12:
                        return date(yy, mm, 1)
                    return None
                except Exception:
                    return None
            out["RENEWALDATE"] = out.apply(_mk_date, axis=1)

            # Row-level reasons
            def _row_reasons(r: pd.Series) -> List[str]:
                reasons: List[str] = []
                if _row_is_missing(r["PROGRAMNAME"]): reasons.append("missing PROGRAMNAME")
                if _row_is_missing(r["TEAMNAME"]): reasons.append("missing TEAMNAME")
                if _row_is_missing(r["GROUPNAME"]): reasons.append("missing GROUPNAME")
                if _row_is_missing(r["APPNAME"]): reasons.append("missing APPNAME")
                if _row_is_missing(r["VENDORNAME"]): reasons.append("missing VENDORNAME")
                if pd.isna(r["FISCAL_YEAR"]): reasons.append("missing FISCAL_YEAR")
                if pd.isna(r["RENEWAL_MONTH"]): reasons.append("missing RENEWAL_MONTH")
                if pd.isna(r["AMOUNT"]): reasons.append("missing AMOUNT")
                if pd.isna(r["AMOUNT_NEXT_YEAR"]): reasons.append("missing AMOUNT_NEXT_YEAR")
                try:
                    mm = int(r["RENEWAL_MONTH"]) if pd.notna(r["RENEWAL_MONTH"]) else None
                    if mm is not None and (mm < 1 or mm > 12):
                        reasons.append("RENEWAL_MONTH out of range")
                except Exception:
                    reasons.append("RENEWAL_MONTH invalid")
                return reasons

            out["_REASONS"] = out.apply(_row_reasons, axis=1)
            out["_IS_VALID"] = out["_REASONS"].apply(lambda lst: len(lst) == 0)

            dfI = out

        # Store previews
        st.session_state["previews"] = {
            "Programs": dfP, "Vendors": dfV, "Groups": dfG, "Apps": dfA, "Teams": dfT, "Invoices": dfI
        }

        # ----- Show previews + preflight -----
        st.markdown("#### Previews")
        p1, p2 = st.columns(2)
        with p1:
            st.markdown("**Programs (unique)**")
            st.dataframe(dfP.head(400), use_container_width=True, height=220)
            st.markdown("**Vendors (unique)**")
            st.dataframe(dfV.head(400), use_container_width=True, height=220)
            st.markdown("**Application Groups (unique)**")
            st.dataframe(dfG.head(400), use_container_width=True, height=240)
        with p2:
            st.markdown("**Applications (unique)**")
            st.dataframe(dfA.head(400), use_container_width=True, height=240)
            st.markdown("**Teams (unique)**")
            st.dataframe(dfT.head(400), use_container_width=True, height=220)

        st.markdown("**Invoices — Row‑level Preflight**")
        if missing_invoice_reqs:
            st.error(
                "Missing column mappings for required field(s): "
                + ", ".join(missing_invoice_reqs)
                + ". Map these columns to proceed."
            )
            df_valid_preview = pd.DataFrame()
            df_invalid_preview = pd.DataFrame()
        else:
            c_valid, c_invalid = st.columns(2)
            with c_valid:
                st.caption("✅ Will be imported (valid rows)")
                df_valid_preview = dfI[dfI["_IS_VALID"]].copy() if ("_IS_VALID" in dfI.columns) else pd.DataFrame()
                show_cols_valid = ["PROGRAMNAME","TEAMNAME","GROUPNAME","APPNAME","VENDORNAME","FISCAL_YEAR","RENEWAL_MONTH","RENEWALDATE","AMOUNT","AMOUNT_NEXT_YEAR"]
                show_cols_valid = [c for c in show_cols_valid if c in df_valid_preview.columns]
                st.dataframe(df_valid_preview[show_cols_valid].head(400), use_container_width=True, height=260)
            with c_invalid:
                st.caption("❌ Will be skipped (invalid rows) — with reasons")
                df_invalid_preview = dfI[~dfI["_IS_VALID"]].copy() if ("_IS_VALID" in dfI.columns) else pd.DataFrame()
                if not df_invalid_preview.empty and "_REASONS" in df_invalid_preview.columns:
                    df_invalid_preview["_REASONS_STR"] = df_invalid_preview["_REASONS"].apply(lambda xs: "; ".join(xs))
                show_cols_invalid = ["PROGRAMNAME","TEAMNAME","GROUPNAME","APPNAME","VENDORNAME","FISCAL_YEAR","RENEWAL_MONTH","AMOUNT","AMOUNT_NEXT_YEAR","_REASONS_STR"]
                show_cols_invalid = [c for c in show_cols_invalid if c in df_invalid_preview.columns]
                st.dataframe(df_invalid_preview[show_cols_invalid].head(400), use_container_width=True, height=260)

        with st.expander("Preflight Summary", expanded=True):
            if missing_invoice_reqs:
                st.warning("Map all required invoice columns to enable import.")
                total_rows = int(dfI.shape[0])
                st.write(f"- Total invoice rows detected: **{total_rows}**")
            else:
                total_rows = int(dfI.shape[0])
                valid_rows_n = int(df_valid_preview.shape[0])
                invalid_rows_n = int(df_invalid_preview.shape[0])
                st.write(
                    f"- Total invoice rows detected: **{total_rows}**  \n"
                    f"- Valid rows (will import): **{valid_rows_n}**  \n"
                    f"- Invalid rows (skipped): **{invalid_rows_n}**"
                )
                if invalid_rows_n > 0 and "_REASONS" in df_invalid_preview.columns:
                    agg: Dict[str, int] = {}
                    for reasons in df_invalid_preview["_REASONS"].tolist():
                        for reason in reasons:
                            agg[reason] = agg.get(reason, 0) + 1
                    if agg:
                        st.write("**Top issues:**")
                        for reason, n in sorted(agg.items(), key=lambda x: -x[1]):
                            st.write(f"- {reason}: **{n}**")

        # -------------------------
        # Import ALL in safe order
        # -------------------------
        prog_widget = st.progress(0, text="Ready.")
        status_txt = st.empty()

        def _tick(pct: int, msg: str, start_time: float) -> None:
            elapsed = time.time() - start_time
            prog_widget.progress(pct, text=f"{msg}  ⏱ {elapsed:,.1f}s")
            status_txt.caption(f"⏱ Elapsed: **{elapsed:,.1f} s**")

        disabled_btn = df_valid_preview.empty or bool(missing_invoice_reqs)

        if st.button("📥 Import ALL", type="primary", use_container_width=True, disabled=disabled_btn):
            start_time = time.time()
            import_batch_id = str(uuid.uuid4())
            try:
                pre = {
                    "vendors": _table_count("VENDORS"),
                    "programs": _table_count("PROGRAMS"),
                    "teams": _table_count("TEAMS"),
                    "groups": _table_count("APPLICATION_GROUPS"),
                    "apps": _table_count("APPLICATIONS"),
                    "invoices": _table_count("INVOICES"),
                }

                # Refresh previews (safe)
                dfP = _get_preview("Programs")
                dfV = _get_preview("Vendors")
                dfG = _get_preview("Groups")
                dfA = _get_preview("Apps")
                dfT = _get_preview("Teams")
                dfI = _get_preview("Invoices")

                # ---------- Vendors ----------
                _tick(5, "Importing Vendors…", start_time)
                existing_vendors = list_vendors()
                vmap: Dict[str, str] = {}
                if existing_vendors is not None and not existing_vendors.empty:
                    for _, r0 in existing_vendors.iterrows():
                        vmap[str(r0.VENDORNAME).strip().upper()] = r0.VENDORID
                inserted_v = updated_v = 0
                if dfV is not None and not dfV.empty:
                    for _, row in dfV.iterrows():
                        vname = str(row["VENDORNAME"]).strip()
                        if _blank_or_nan(vname):
                            continue
                        existed = vname.upper() in vmap
                        vid = vmap.get(vname.upper()) or str(uuid.uuid4())
                        upsert_vendor(vid, vname)
                        vmap[vname.upper()] = vid
                        inserted_v += 0 if existed else 1
                        updated_v  += 1 if existed else 0

                # ---------- Programs ----------
                _tick(12, "Importing Programs…", start_time)
                existing_programs = list_programs()
                pmap: Dict[str, str] = {}
                if existing_programs is not None and not existing_programs.empty:
                    for _, r0 in existing_programs.iterrows():
                        pmap[str(r0.PROGRAMNAME).strip().upper()] = r0.PROGRAMID
                inserted_p = updated_p = 0
                if dfP is not None and not dfP.empty:
                    for _, row in dfP.iterrows():
                        pname = str(row["PROGRAMNAME"]).strip()
                        if _blank_or_nan(pname):
                            continue
                        existed = pname.upper() in pmap
                        pid = pmap.get(pname.upper()) or str(uuid.uuid4())
                        upsert_program(pid, pname, owner=None, fte=None)
                        pmap[pname.upper()] = pid
                        inserted_p += 0 if existed else 1
                        updated_p  += 1 if existed else 0

                # ---------- Teams ----------
                _tick(22, "Importing Teams…", start_time)
                existing_teams = list_teams()
                tmap: Dict[str, str] = {}
                if existing_teams is not None and not existing_teams.empty:
                    for _, r0 in existing_teams.iterrows():
                        tmap[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID
                inserted_t = updated_t = 0
                if dfT is not None and not dfT.empty:
                    for _, row in dfT.iterrows():
                        tname = str(row["TEAMNAME"]).strip()
                        if _blank_or_nan(tname):
                            continue
                        pid = None
                        if "PROGRAMNAME" in row and not _blank_or_nan(row["PROGRAMNAME"]):
                            pid = pmap.get(str(row["PROGRAMNAME"]).strip().upper())
                        existed = tname.upper() in tmap
                        tid = tmap.get(tname.upper()) or str(uuid.uuid4())
                        upsert_team(
                            team_id=tid,
                            name=tname,
                            program_id=pid,
                            team_fte=None, delivery_team_fte=None,
                            contractor_c_fte=None, contractor_cs_fte=None
                        )
                        tmap[tname.upper()] = tid
                        inserted_t += 0 if existed else 1
                        updated_t  += 1 if existed else 0

                # ---------- Groups ----------
                _tick(35, "Importing Application Groups…", start_time)
                existing_groups = list_application_groups()
                gmap: Dict[str, str] = {}
                if existing_groups is not None and not existing_groups.empty:
                    for _, r0 in existing_groups.iterrows():
                        gmap[str(r0.GROUPNAME).strip().upper()] = r0.GROUPID
                inserted_g = updated_g = 0
                if dfG is not None and not dfG.empty:
                    # refresh maps
                    existing_teams2 = list_teams()
                    if existing_teams2 is not None and not existing_teams2.empty:
                        for _, r0 in existing_teams2.iterrows():
                            tmap[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID
                    existing_vendors2 = list_vendors()
                    if existing_vendors2 is not None and not existing_vendors2.empty:
                        for _, r0 in existing_vendors2.iterrows():
                            vmap[str(r0.VENDORNAME).strip().upper()] = r0.VENDORID

                    for _, row in dfG.iterrows():
                        gname = str(row["GROUPNAME"]).strip()
                        if _blank_or_nan(gname):
                            continue
                        teamid = None
                        if "TEAMNAME" in row and not _blank_or_nan(row["TEAMNAME"]):
                            teamid = tmap.get(str(row["TEAMNAME"]).strip().upper())
                        default_vendor_id = None
                        if "DEFAULT_VENDORNAME" in row and not _blank_or_nan(row["DEFAULT_VENDORNAME"]):
                            default_vendor_id = vmap.get(str(row["DEFAULT_VENDORNAME"]).strip().upper())

                        existed = gname.upper() in gmap
                        gid = gmap.get(gname.upper()) or str(uuid.uuid4())
                        upsert_application_group(gid, gname, team_id=(teamid or ""), default_vendor_id=default_vendor_id, owner=None)
                        gmap[gname.upper()] = gid
                        inserted_g += 0 if existed else 1
                        updated_g  += 1 if existed else 0

                # ---------- Applications ----------
                _tick(48, "Importing Application Instances…", start_time)
                existing_apps = list_applications()
                amap: Dict[str, str] = {}
                if existing_apps is not None and not existing_apps.empty:
                    for _, r0 in existing_apps.iterrows():
                        amap[str(r0.APPLICATIONNAME).strip().upper()] = r0.APPLICATIONID
                inserted_a = updated_a = 0
                if dfA is not None and not dfA.empty:
                    cur_groups = list_application_groups()
                    if cur_groups is not None and not cur_groups.empty:
                        for _, r0 in cur_groups.iterrows():
                            gmap[str(r0.GROUPNAME).strip().upper()] = r0.GROUPID

                    for _, row in dfA.iterrows():
                        aname = str(row["APPNAME"]).strip()
                        if _blank_or_nan(aname):
                            continue
                        gid = None
                        if "GROUPNAME" in row and not _blank_or_nan(row["GROUPNAME"]):
                            gid = gmap.get(str(row["GROUPNAME"]).strip().upper())

                        existed = aname.upper() in amap
                        aid = amap.get(aname.upper()) or str(uuid.uuid4())
                        upsert_application_instance(
                            application_id=aid,
                            group_id=(gid or ""),
                            application_name=aname,
                            add_info=None,
                            vendor_id=None
                        )
                        amap[aname.upper()] = aid
                        inserted_a += 0 if existed else 1
                        updated_a  += 1 if existed else 0

                # ---------- Invoices ----------
                _tick(62, "Importing Invoices…", start_time)
                created_i = 0

                # refresh all maps just before invoicing
                cur_programs = list_programs();  pmap = {}
                if cur_programs is not None and not cur_programs.empty:
                    for _, r0 in cur_programs.iterrows():
                        pmap[str(r0.PROGRAMNAME).strip().upper()] = r0.PROGRAMID

                cur_teams = list_teams(); tmap = {}
                if cur_teams is not None and not cur_teams.empty:
                    for _, r0 in cur_teams.iterrows():
                        tmap[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID

                cur_groups2 = list_application_groups(); gmap = {}
                if cur_groups2 is not None and not cur_groups2.empty:
                    for _, r0 in cur_groups2.iterrows():
                        gmap[str(r0.GROUPNAME).strip().upper()] = r0.GROUPID

                cur_vendors = list_vendors(); vmap = {}
                if cur_vendors is not None and not cur_vendors.empty:
                    for _, r0 in cur_vendors.iterrows():
                        vmap[str(r0.VENDORNAME).strip().upper()] = r0.VENDORID

                cur_apps2 = list_applications(); amap = {}
                if cur_apps2 is not None and not cur_apps2.empty:
                    for _, r0 in cur_apps2.iterrows():
                        amap[str(r0.APPLICATIONNAME).strip().upper()] = r0.APPLICATIONID

                required_present = (
                    dfI is not None and not dfI.empty and
                    all(c in dfI.columns for c in
                        ["PROGRAMNAME","TEAMNAME","GROUPNAME","APPNAME","VENDORNAME","FISCAL_YEAR","RENEWAL_MONTH","AMOUNT","AMOUNT_NEXT_YEAR"])
                )

                if required_present and (df_valid_preview is not None) and (not df_valid_preview.empty):
                    for i, row in enumerate(df_valid_preview.itertuples(index=False), start=1):
                        pname = str(getattr(row, "PROGRAMNAME", "") or "").strip()
                        tname = str(getattr(row, "TEAMNAME", "") or "").strip()
                        gname = str(getattr(row, "GROUPNAME", "") or "").strip()
                        aname = str(getattr(row, "APPNAME", "") or "").strip()
                        vname = str(getattr(row, "VENDORNAME", "") or "").strip()
                        fy    = getattr(row, "FISCAL_YEAR", None)
                        rmonth= getattr(row, "RENEWAL_MONTH", None)
                        amt   = getattr(row, "AMOUNT", None)
                        amt_n = getattr(row, "AMOUNT_NEXT_YEAR", None)

                        if any(_blank_or_nan(x) for x in [pname, tname, gname, aname, vname]):
                            continue
                        if any(pd.isna(x) for x in [fy, rmonth, amt, amt_n]):
                            continue

                        vendorid_at_booking = vmap.get(vname.upper())
                        if not vendorid_at_booking:
                            continue

                        teamid = tmap.get(tname.upper())
                        if not teamid:
                            continue

                        groupid = gmap.get(gname.upper()) if gname else None
                        appid = amap.get(aname.upper()) if aname else None

                        try:
                            d_renew = date(int(fy), int(rmonth), 1)
                        except Exception:
                            continue

                        inv_id = str(uuid.uuid4())
                        upsert_invoice(
                            invoice_id=inv_id,
                            application_id=str(appid or ""),
                            team_id=str(teamid or ""),
                            renewal_date=d_renew,
                            amount=float(amt) if not pd.isna(amt) else None,
                            status="Planned",
                            fiscal_year=int(fy) if not pd.isna(fy) else None,
                            product_owner=(str(getattr(row, "PRODUCT_OWNER", None)) if pd.notna(getattr(row, "PRODUCT_OWNER", None)) else None),
                            amount_next_year=float(amt_n) if not pd.isna(amt_n) else None,
                            contract_active=bool(getattr(row, "CONTRACT_ACTIVE", True)) if pd.notna(getattr(row, "CONTRACT_ACTIVE", True)) else True,
                            company_code=(str(getattr(row, "COMPANY_CODE", None)) if pd.notna(getattr(row, "COMPANY_CODE", None)) else None),
                            cost_center=(str(getattr(row, "COST_CENTER", None)) if pd.notna(getattr(row, "COST_CENTER", None)) else None),
                            serial_number=(str(getattr(row, "SERIAL_NUMBER", None)) if pd.notna(getattr(row, "SERIAL_NUMBER", None)) else None),
                            work_order=(str(getattr(row, "WORK_ORDER", None)) if pd.notna(getattr(row, "WORK_ORDER", None)) else None),
                            agreement_number=None,
                            contract_due=None,   # keep NUMBER(4,0) separate from RENEWALDATE
                            service_type=None,
                            notes=(str(getattr(row, "NOTES", None)) if pd.notna(getattr(row, "NOTES", None)) else None),
                            group_id=str(groupid or ""),
                            programid_at_booking=None,
                            vendorid_at_booking=str(vendorid_at_booking or ""),
                            groupid_at_booking=str(groupid or ""),
                            rollover_batch_id=import_batch_id,
                            rolled_over_from_year=None,
                            invoice_type="Recurring Invoice",
                        )

                        if i % 50 == 0:
                            _tick(62 + min(30, int(i / max(len(df_valid_preview), 1) * 30)), f"Importing Invoices… {i} processed", start_time)
                        created_i += 1

                _tick(96, "Finalizing…", start_time)
                post = {
                    "vendors": _table_count("VENDORS"),
                    "programs": _table_count("PROGRAMS"),
                    "teams": _table_count("TEAMS"),
                    "groups": _table_count("APPLICATION_GROUPS"),
                    "apps": _table_count("APPLICATIONS"),
                    "invoices": _table_count("INVOICES"),
                }

                _tick(100, "Done.", start_time)
                st.success(
                    "Import complete ✅\n\n"
                    f"- Vendors: **{post['vendors'] - pre['vendors']} new** (total: {post['vendors']})\n"
                    f"- Programs: **{post['programs'] - pre['programs']} new** (total: {post['programs']})\n"
                    f"- Teams: **{post['teams'] - pre['teams']} new** (total: {post['teams']})\n"
                    f"- App Groups: **{post['groups'] - pre['groups']} new** (total: {post['groups']})\n"
                    f"- Applications: **{post['apps'] - pre['apps']} new** (total: {post['apps']})\n"
                    f"- Invoices: **{post['invoices'] - pre['invoices']} new** (total: {post['invoices']})\n\n"
                    f"**Batch ID:** `{import_batch_id}`"
                )

            except Exception as e:
                st.error(f"Import failed: {e}")
                st.exception(e)

        # --------------------------------------------
        # 🔁 Rollback imported invoices by Batch ID
        # --------------------------------------------
        with st.expander("🔁 Rollback imported invoices by Batch ID", expanded=False):
            st.caption("This will **DELETE** all invoices with the selected `ROLLOVER_BATCH_ID`. It does not touch vendors/programs/teams/apps/groups.")

            def list_invoice_batches(limit: int = 50) -> pd.DataFrame:
                try:
                    return fetch_df(f"""
                        SELECT
                          ROLLOVER_BATCH_ID AS BATCH_ID,
                          COUNT(*) AS N
                        FROM INVOICES
                        WHERE ROLLOVER_BATCH_ID IS NOT NULL
                        GROUP BY ROLLOVER_BATCH_ID
                        ORDER BY MAX(COALESCE(RENEWALDATE, TO_DATE('1900-01-01'))) DESC, BATCH_ID DESC
                        LIMIT {int(limit)}
                    """)
                except Exception:
                    return pd.DataFrame(columns=["BATCH_ID","N"])

            batches_df = list_invoice_batches(limit=100)
            if batches_df is None or batches_df.empty:
                st.info("No batches found yet.")
            else:
                st.dataframe(batches_df, use_container_width=True, height=220)

                batch_choices = ["(type a batch id)"] + batches_df["BATCH_ID"].astype(str).tolist()
                picked = st.selectbox("Pick a recent Batch ID", options=batch_choices, index=0, key="rollback_pick")
                typed = st.text_input("…or paste a Batch ID exactly", value="" if picked == "(type a batch id)" else picked, key="rollback_typed").strip()

                col_prev, col_del = st.columns([1,1])

                with col_prev:
                    if st.button("Preview rows in this batch", key="btn_preview_batch", use_container_width=True, disabled=(typed == "")):
                        try:
                            prev = fetch_df("""
                                SELECT
                                  INVOICEID, TEAMID, APPLICATIONID, GROUPID, VENDORID_AT_BOOKING,
                                  FISCAL_YEAR, RENEWALDATE, AMOUNT, AMOUNT_NEXT_YEAR, STATUS, INVOICE_TYPE,
                                  ROLLOVER_BATCH_ID
                                FROM INVOICES
                                WHERE ROLLOVER_BATCH_ID = %s
                                ORDER BY FISCAL_YEAR DESC, RENEWALDATE DESC NULLS LAST, INVOICEID
                                LIMIT 500
                            """, (typed,))
                            if prev is None or prev.empty:
                                st.warning("No invoices found for that Batch ID.")
                            else:
                                st.success(f"Found {len(prev)} row(s) (showing up to 500).")
                                st.dataframe(prev, use_container_width=True, height=320)
                        except Exception as e:
                            st.error(f"Preview failed: {e}")

                with col_del:
                    danger = st.checkbox("I understand this **permanently deletes** invoices in this batch.", value=False, key="confirm_delete_batch")
                    if st.button("🗑️ Rollback (Delete Invoices in Batch)", type="secondary", use_container_width=True,
                                 disabled=(typed == "" or not danger), key="btn_delete_batch"):
                        try:
                            cnt = fetch_df("SELECT COUNT(*) AS N FROM INVOICES WHERE ROLLOVER_BATCH_ID = %s", (typed,))
                            n = int(cnt.iloc[0]["N"]) if cnt is not None and not cnt.empty else 0

                            execute("DELETE FROM INVOICES WHERE ROLLOVER_BATCH_ID = %s", (typed,))
                            st.success(f"Deleted **{n}** invoice(s) for batch `{typed}`.")
                        except Exception as e:
                            st.error(f"Rollback failed: {e}")

    else:
        st.info("Upload your workbook above to proceed.")
