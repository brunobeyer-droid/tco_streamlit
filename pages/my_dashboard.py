# pages/my_dashboard.py
from __future__ import annotations
import json
import io
from typing import Dict, List
import pandas as pd
import streamlit as st

# -----------------------------------------------------------------------------
# Page config
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="My TCO Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -----------------------------------------------------------------------------
# ECharts renderer (theme-aware)
# -----------------------------------------------------------------------------
try:
    from streamlit_echarts import st_echarts
    _ECHARTS_OK = True
except Exception:
    _ECHARTS_OK = False

def get_theme_name() -> str:
    # Persisted in session (defaults to Light)
    return st.session_state.get("ui_theme", "Light")

def set_theme_name(name: str) -> None:
    st.session_state["ui_theme"] = name

def _kpi(label: str, value: str, dark: bool = False):
    # Slightly different label color for dark vs light
    label_color = "#bbb" if dark else "#888"
    st.markdown(
        f"""
        <div style="text-align:right; line-height:1.2; margin-bottom:6px;">
            <div style="font-size:22px; font-weight:600;">{value}</div>
            <div style="font-size:12px; color:{label_color};">{label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def render_echart(options: dict, height: str = "420px"):
    """
    Wrapper to render ECharts with the page theme.
    - Light  -> default (theme=None)
    - Dark   -> theme="dark"
    """
    if not _ECHARTS_OK:
        st.warning("ECharts renderer not available. Install `streamlit-echarts` to display charts.")
        return
    theme_name = get_theme_name()
    theme_arg = "dark" if theme_name == "Dark" else None
    st_echarts(options=options, height=height, theme=theme_arg)

# -----------------------------------------------------------------------------
# Data access
# -----------------------------------------------------------------------------
try:
    from snowflake_db import fetch_df, ensure_all_views_ok
    try:
        ensure_all_views_ok()
    except Exception:
        pass
except ImportError:
    from snowflake_db import fetch_df  # type: ignore

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _json_in(where: List[str], params: List, col: str, values, as_number: bool = False):
    if not values:
        return
    where.append(
        f"""{col} IN (
              SELECT VALUE::{'NUMBER' if as_number else 'STRING'}
              FROM TABLE(FLATTEN(input=>PARSE_JSON(%s)))
            )"""
    )
    params.append(json.dumps(values))

def _to_kusd(x) -> float:
    try:
        return float(x) / 1_000.0
    except Exception:
        return 0.0

def _str_strip(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

# -----------------------------------------------------------------------------
# Data fetch (VW_TCO_WORKFORCE_SPLIT already includes ADO fields)
# -----------------------------------------------------------------------------
@st.cache_data(ttl=180, show_spinner=False)
def fetch_workforce_split(years, pis, programs, teams, groups) -> pd.DataFrame:
    sql_where, params = ["1=1"], []
    _json_in(sql_where, params, "YEAR", years, as_number=True)
    _json_in(sql_where, params, "PI", pis, as_number=True)
    _json_in(sql_where, params, "PROGRAMNAME", programs)
    _json_in(sql_where, params, "TEAMNAME", teams)
    _json_in(sql_where, params, "GROUPNAME", groups)

    sql = f"""
      SELECT
        SOURCE, YEAR, PI,
        PROGRAMNAME, TEAMNAME, GROUPNAME,
        COST_CATEGORY, SUBCOMPONENT,
        AMOUNT,
        FEATURE_TITLE, FEATURE_STATE, EFFORT_POINTS,
        INVESTMENT_DIMENSION 
      FROM VW_TCO_WORKFORCE_SPLIT
      WHERE {' AND '.join(sql_where)}
    """
    df = fetch_df(sql, tuple(params) if params else None)
    if df.empty:
        return pd.DataFrame(columns=[
            "SOURCE","YEAR","PI","PROGRAMNAME","TEAMNAME","GROUPNAME",
            "COST_CATEGORY","SUBCOMPONENT","AMOUNT","FEATURE_TITLE","FEATURE_STATE","EFFORT_POINTS"
        ])
    # Normalize
    for c in ["YEAR","PI","AMOUNT","EFFORT_POINTS"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df = _str_strip(df, [
        "PROGRAMNAME","TEAMNAME","GROUPNAME","SOURCE","COST_CATEGORY",
        "SUBCOMPONENT","FEATURE_TITLE","FEATURE_STATE"
    ])
    return df

# -----------------------------------------------------------------------------
# Load data with filters
# -----------------------------------------------------------------------------
flt_years     = list(st.session_state.get("flt_years", []))
flt_pis       = list(st.session_state.get("flt_pis", []))
flt_programs  = list(st.session_state.get("flt_programs", []))
flt_teams     = list(st.session_state.get("flt_teams", []))
flt_groups    = list(st.session_state.get("flt_groups", []))
flt_sources   = list(st.session_state.get("flt_sources", []))

with st.spinner("Loading data..."):
    df_split = fetch_workforce_split(flt_years, flt_pis, flt_programs, flt_teams, flt_groups)

# KPI values (KUSD)
total_amount = float(df_split["AMOUNT"].sum()) if not df_split.empty else 0.0
total_kusd   = _to_kusd(total_amount)
programs_ct  = df_split["PROGRAMNAME"].replace(["", "nan", "None"], pd.NA).dropna().nunique()
teams_ct     = df_split["TEAMNAME"].replace(["", "nan", "None"], pd.NA).dropna().nunique()
groups_ct    = df_split["GROUPNAME"].replace(["", "nan", "None"], pd.NA).dropna().nunique()

# -----------------------------------------------------------------------------
# ROW 1: Title + Theme switch + KPIs (3 columns; middle used for theme)
# -----------------------------------------------------------------------------
col1, col2, col3 = st.columns([2.6, 0.8, 1.8], gap="large")
with col1:
    st.title("My TCO Dashboard")

with col2:
    st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)
    theme_choice = st.segmented_control(
        "Theme",
        options=["Light", "Dark"],
        selection_mode="single",
        default=get_theme_name(),
        help="Applies to all ECharts on this page",
        key="seg_theme",
    )
    # Persist selection
    set_theme_name(theme_choice)

with col3:
    st.markdown('<div style="height:6px;"></div>', unsafe_allow_html=True)
    kpi_cols = st.columns([1,1,1,1])
    dark_mode = (get_theme_name() == "Dark")
    with kpi_cols[0]:
        _kpi("Total (KUSD)", f"${total_kusd:,.0f}K", dark=dark_mode)
    with kpi_cols[1]:
        _kpi("Programs", f"{programs_ct}", dark=dark_mode)
    with kpi_cols[2]:
        _kpi("Teams", f"{teams_ct}", dark=dark_mode)
    with kpi_cols[3]:
        _kpi("App Groups", f"{groups_ct}", dark=dark_mode)

# -----------------------------------------------------------------------------
# ROW 2: Tabs (left) + Filters (right)
# -----------------------------------------------------------------------------
left_col, right_col = st.columns([4, 0.98], gap="large")

with left_col:
    tabs = st.tabs(["Main", "R&M", "Process Ops", "QM", "WFE", "Central Services", "Data Sheet"])

    # ---------------- MAIN ----------------
    with tabs[0]:
        st.subheader("Main")
        st.caption("Grid layout: 2×2 (cells A1, A2, B1, B2). Use `render_echart(...)` to respect the page theme.")

        # Example placeholder cells (replace with your ECharts when ready)
        rowA = st.columns(2)
        with rowA[0]:
            st.info("Cell A1")
            # Example usage once you have options:
            # render_echart(opt, height="420px")
        with rowA[1]:
            st.info("Cell A2")

        rowB = st.columns(2)
        with rowB[0]:
            st.info("Cell B1")
        with rowB[1]:
            st.info("Cell B2")

    # ---------------- DATA SHEET ----------------
    with tabs[6]:
        st.subheader("Data Sheet")
        st.caption("Group by columns (values in KUSD). Toggle to hide blank Program/Team/Group rows.")

        if not df_split.empty:
            work_df = df_split.copy()
            work_df["KUSD"] = work_df["AMOUNT"].apply(_to_kusd)

            groupable = [c for c in [
                "YEAR","PI","PROGRAMNAME","TEAMNAME","GROUPNAME","SOURCE",
                "COST_CATEGORY","SUBCOMPONENT",
                "FEATURE_TITLE","FEATURE_STATE","INVESTMENT_DIMENSION"
            ] if c in work_df.columns]
            group_by = st.multiselect("Group by columns", options=groupable, key="ds_groupby")

            if group_by:
                agg_dict = {"KUSD": "sum"}
                if "EFFORT_POINTS" in work_df.columns:
                    agg_dict["EFFORT_POINTS"] = "sum"
                display_df = (
                    work_df.groupby(group_by, dropna=False, as_index=False)
                           .agg(agg_dict)
                           .sort_values("KUSD", ascending=False)
                )
            else:
                display_df = work_df

            # Toggle: hide blank Program/Team/Group
            hide_blank = st.checkbox("Hide blank Program/Team/Group rows", value=False, key="ds_hide_blank")
            if hide_blank:
                key_cols = [c for c in ["PROGRAMNAME","TEAMNAME","GROUPNAME"] if c in display_df.columns]
                for c in key_cols:
                    display_df[c] = (
                        display_df[c]
                        .astype(str)
                        .str.strip()
                        .replace({"": pd.NA, "None": pd.NA, "none": pd.NA, "NaN": pd.NA, "nan": pd.NA})
                    )
                display_df = display_df.dropna(subset=key_cols, how="any")

            st.dataframe(display_df, use_container_width=True, hide_index=True)

            csv_buf = io.StringIO()
            display_df.to_csv(csv_buf, index=False)
            st.download_button(
                "Download CSV (current view)",
                data=csv_buf.getvalue(),
                file_name="tco_data_sheet.csv",
                mime="text/csv",
            )
        else:
            st.info("No rows for the selected global filters.")

# -----------------------------------------------------------------------------
# Filters panel (right)
# -----------------------------------------------------------------------------
with right_col:
    base_df = fetch_workforce_split([], [], [], [], [])
    years_opts = sorted(pd.to_numeric(base_df["YEAR"], errors="coerce").dropna().astype(int).unique().tolist())
    pis_opts   = sorted(pd.to_numeric(base_df["PI"], errors="coerce").dropna().astype(int).unique().tolist())
    prog_opts  = sorted(base_df["PROGRAMNAME"].dropna().astype(str).str.strip().unique().tolist())
    team_opts  = sorted(base_df["TEAMNAME"].dropna().astype(str).str.strip().unique().tolist())
    group_opts = sorted(base_df["GROUPNAME"].dropna().astype(str).str.strip().unique().tolist())
    src_opts   = sorted(base_df["SOURCE"].dropna().astype(str).str.strip().unique().tolist())

    with st.expander("Filters", expanded=False):
        years    = st.multiselect("Year (FY)", years_opts, default=flt_years, key="ui_years")
        pis      = st.multiselect("PI (Iterations)", pis_opts, default=flt_pis, key="ui_pis")
        programs = st.multiselect("Programs", prog_opts, default=flt_programs, key="ui_programs")
        teams    = st.multiselect("Teams", team_opts, default=flt_teams, key="ui_teams")
        groups   = st.multiselect("Application Groups", group_opts, default=flt_groups, key="ui_groups")
        sources  = st.multiselect("Source (Invoice/ADO)", src_opts, default=flt_sources, key="ui_sources")

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Apply", use_container_width=True):
                st.session_state.update(
                    flt_years=years, flt_pis=pis, flt_programs=programs,
                    flt_teams=teams, flt_groups=groups, flt_sources=sources
                )
                st.rerun()
        with c2:
            if st.button("Clear", use_container_width=True):
                for k in ["flt_years","flt_pis","flt_programs","flt_teams","flt_groups","flt_sources",
                          "ui_years","ui_pis","ui_programs","ui_teams","ui_groups","ui_sources",
                          "ds_groupby","ds_hide_blank","seg_theme"]:
                    st.session_state.pop(k, None)
                st.rerun()

# Footer
st.caption(f"Loaded • Rows = {len(df_split):,} • Total = ${total_kusd:,.0f}K (KUSD)")
