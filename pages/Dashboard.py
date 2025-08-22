# pages/Dashboard.py
from __future__ import annotations
import sys, subprocess, json
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------
# Page config FIRST (avoid Streamlit warnings)
# ---------------------------------------------------------------------
st.set_page_config(page_title="TCO Dashboard", layout="wide")

# ---------------------------------------------------------------------
# Global style tweaks: slimmer sidebar, tighter content (no font-size/logo)
# ---------------------------------------------------------------------
st.markdown("""
<style>
/* Sidebar width only */
[data-testid="stSidebar"] {
  min-width: 190px;
  max-width: 190px;
}
/* Denser layout for 3-up rows */
.block-container {
  padding-top: 0.75rem;
  padding-bottom: 0.75rem;
}
[data-baseweb="tab-list"] { margin-bottom: 0.25rem; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------
# Self-heal Plotly if missing (still list in requirements.txt ideally)
# ---------------------------------------------------------------------
def _ensure_plotly():
    try:
        import plotly.express as px
        return px
    except ModuleNotFoundError:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "plotly>=5.20"])
            import plotly.express as px
            return px
        except Exception as e:
            raise RuntimeError("Plotly is required. Install with: pip install 'plotly>=5.20'") from e

px = _ensure_plotly()

# ---------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------
try:
    from snowflake_db import fetch_df, ensure_all_views_ok
    try:
        ensure_all_views_ok()
    except Exception:
        # Non-fatal; page still works if this pre-check fails
        pass
except ImportError:
    from snowflake_db import fetch_df

# =========================
# Query helpers
# =========================
@st.cache_data(ttl=120, show_spinner=False)
def _choices(col: str) -> list:
    """Return distinct values for a column from VW_COSTS_AND_INVOICES."""
    sql = f"SELECT DISTINCT {col} AS V FROM VW_COSTS_AND_INVOICES ORDER BY 1"
    df = fetch_df(sql)
    if df is None or df.empty or "V" not in df.columns:
        return []
    vals = [v for v in df["V"].tolist() if v is not None and v != ""]

    key = col.upper()
    if key in ("YEAR", "PI"):
        try:
            vals = sorted({int(float(v)) for v in vals})
        except Exception:
            vals = sorted(set(map(str, vals)))
    else:
        seen, dedup = set(), []
        for v in vals:
            k = str(v).strip().lower()
            if k and k not in seen:
                seen.add(k); dedup.append(v)
        vals = sorted(dedup, key=lambda x: str(x).lower())
    return vals


def _json_in_builder(where: list[str], params: list, col: str, values, as_number=False):
    if not values:
        return
    where.append(
        f"""{col} IN (
              SELECT VALUE::{'NUMBER' if as_number else 'STRING'}
              FROM TABLE(FLATTEN(input=>PARSE_JSON(%s)))
            )"""
    )
    params.append(json.dumps(values))


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_filtered(years, pis, programs, teams, groups, sources, emp_types) -> pd.DataFrame:
    """
    Fetch filtered rows from VW_COSTS_AND_INVOICES (union of features+invoices).
    """
    where: list[str] = ["1=1"]
    params: list = []

    _json_in_builder(where, params, "YEAR", years, as_number=True)
    _json_in_builder(where, params, "PI", pis, as_number=True)
    _json_in_builder(where, params, "PROGRAMNAME", programs)
    _json_in_builder(where, params, "TEAMNAME", teams)
    _json_in_builder(where, params, "GROUPNAME", groups)
    _json_in_builder(where, params, "SOURCE", sources)
    _json_in_builder(where, params, "EMPLOYEE_TYPE", emp_types)

    sql = f"""
        SELECT
            SOURCE, YEAR, PI,
            PROGRAMID, PROGRAMNAME,
            TEAMID, TEAMNAME,
            GROUPID, GROUPNAME,
            EMPLOYEE_TYPE, AMOUNT
        FROM VW_COSTS_AND_INVOICES
        WHERE {' AND '.join(where)}
    """
    df = fetch_df(sql, tuple(params) if params else None)

    expected_cols = [
        "SOURCE","YEAR","PI","PROGRAMID","PROGRAMNAME","TEAMID","TEAMNAME",
        "GROUPID","GROUPNAME","EMPLOYEE_TYPE","AMOUNT"
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=expected_cols)

    # Ensure columns exist and coerce types
    for c in expected_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce")
    df["PI"]   = pd.to_numeric(df["PI"], errors="coerce")
    df["AMOUNT"] = pd.to_numeric(df["AMOUNT"], errors="coerce").fillna(0.0)
    for c in ["SOURCE","PROGRAMNAME","TEAMNAME","GROUPNAME","EMPLOYEE_TYPE"]:
        df[c] = df[c].astype(str).str.strip()

    return df


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_feature_components(years, pis, programs, teams, groups) -> pd.DataFrame:
    """
    Pull per-feature cost components + effort from VW_TEAM_COSTS_PER_FEATURE and attach Program/Group,
    then aggregate to grain:
      (YEAR, PI, PROGRAMID, TEAMID, GROUPID, PROGRAMNAME, TEAMNAME, GROUPNAME)
    Use this dataset for component-based visuals to avoid duplication.
    """
    where: list[str] = ["1=1"]
    params: list = []

    # Filters map: ADO_YEAR -> YEAR, ITERATION_NUM -> PI
    _json_in_builder(where, params, "COALESCE(v.ADO_YEAR, YEAR(v.CHANGED_AT))", years, as_number=True)
    _json_in_builder(where, params, "v.ITERATION_NUM", pis, as_number=True)
    _json_in_builder(where, params, "p.PROGRAMNAME", programs)
    _json_in_builder(where, params, "t.TEAMNAME", teams)
    _json_in_builder(where, params, "g.GROUPNAME", groups)

    sql = f"""
        SELECT
          COALESCE(v.ADO_YEAR, YEAR(v.CHANGED_AT)) AS YEAR,
          v.ITERATION_NUM                           AS PI,
          t.PROGRAMID, p.PROGRAMNAME,
          v.TEAMID, v.TEAMNAME,
          g.GROUPID, g.GROUPNAME,
          CAST(v.EFFORT_POINTS AS FLOAT)           AS EFFORT_POINTS,
          CAST(v.TEAM_COST_PERPI               AS NUMBER(18,2)) AS TEAM_COST_PERPI,
          CAST(v.DEL_TEAM_COST_PERPI           AS NUMBER(18,2)) AS DEL_TEAM_COST_PERPI,
          CAST(v.TEAM_CONTRACTOR_CS_COST_PERPI AS NUMBER(18,2)) AS TEAM_CONTRACTOR_CS_COST_PERPI,
          CAST(v.TEAM_CONTRACTOR_C_COST_PERPI  AS NUMBER(18,2)) AS TEAM_CONTRACTOR_C_COST_PERPI
        FROM VW_TEAM_COSTS_PER_FEATURE v
        LEFT JOIN TEAMS t  ON t.TEAMID = v.TEAMID
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag ON mag.ADO_APP = v.APP_NAME_RAW
        LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = mag.APP_GROUP
        WHERE {' AND '.join(where)}
    """
    comp = fetch_df(sql, tuple(params) if params else None)
    if comp is None or comp.empty:
        return pd.DataFrame(columns=[
            "YEAR","PI","PROGRAMID","PROGRAMNAME","TEAMID","TEAMNAME","GROUPID","GROUPNAME",
            "EFFORT_POINTS","TEAM_COST_PERPI","DEL_TEAM_COST_PERPI",
            "TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"
        ])

    # Coerce types & clean
    comp["YEAR"] = pd.to_numeric(comp["YEAR"], errors="coerce")
    comp["PI"]   = pd.to_numeric(comp["PI"], errors="coerce")
    comp["EFFORT_POINTS"] = pd.to_numeric(comp["EFFORT_POINTS"], errors="coerce").fillna(0.0)
    for c in ["TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]:
        comp[c] = pd.to_numeric(comp[c], errors="coerce").fillna(0.0)

    # Aggregate to merge grain (sum effort and costs)
    keys = ["YEAR","PI","PROGRAMID","TEAMID","GROUPID","PROGRAMNAME","TEAMNAME","GROUPNAME"]
    num_cols = ["EFFORT_POINTS","TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]
    comp_agg = comp.groupby(keys, as_index=False)[num_cols].sum()

    return comp_agg


def _nunique_by_name(s: pd.Series | None) -> int:
    if s is None or s.empty:
        return 0
    return (
        s.dropna()
         .astype(str)
         .str.strip()
         .str.lower()
         .replace({"": pd.NA})
         .dropna()
         .nunique()
    )

# =========================
# Title + Layout (Left: Charts; Right: Filters)
# Keep filter column narrow (5:1)
# =========================
st.title("Cost & Invoice Dashboard")

left, right = st.columns([5, 1], gap="large")

# ---------- Filters (right) ----------
with right:
    st.subheader("Filters")

    with st.spinner("Loading filter options..."):
        years_all      = _choices("YEAR")
        pis_all        = _choices("PI")
        sources_all    = _choices("SOURCE")
        programs_all   = _choices("PROGRAMNAME")
        teams_all      = _choices("TEAMNAME")
        groups_all     = _choices("GROUPNAME")
        emp_types_all  = _choices("EMPLOYEE_TYPE")

    years      = st.multiselect("Year", years_all, default=[])
    pis        = st.multiselect("Iteration (PI)", pis_all, default=[])
    sources    = st.multiselect("Source (FEATURE / INVOICE)", sources_all, default=[])
    programs   = st.multiselect("Program", programs_all, default=[])
    teams      = st.multiselect("Team", teams_all, default=[])
    groups     = st.multiselect("Application Group", groups_all, default=[])
    emp_types  = st.multiselect("Employee Type (Feature rows)", emp_types_all, default=[])

    if st.button("Clear filters", use_container_width=True):
        st.rerun()

# ---------- Data (uses right-side selections) ----------
with st.spinner("Loading data..."):
    df_main = _fetch_filtered(years, pis, programs, teams, groups, sources, emp_types)  # union view (for AMOUNT visuals)
    comp    = _fetch_feature_components(years, pis, programs, teams, groups)            # component grain (for TEAM/DELIVERY/CS/C/EFFORT visuals)

with left:
    tab_dash, tab_sheet = st.tabs(["📊 Dashboard", "📄 Data Sheet"])

    # =========================
    # Tab 1: Dashboard (3 visuals per row)
    # =========================
    with tab_dash:
        if df_main.empty and comp.empty:
            st.info("No data for the current filters.")
        else:
            # KPIs (from union view)
            total_amount = float(df_main["AMOUNT"].sum()) if "AMOUNT" in df_main else 0.0
            prog_count_name = _nunique_by_name(df_main.get("PROGRAMNAME"))
            team_count_name = _nunique_by_name(df_main.get("TEAMNAME"))

            k1, k2, k3 = st.columns(3)
            k1.metric("Total Amount", f"${total_amount:,.0f}")
            k2.metric("Programs", f"{prog_count_name:,}")
            k3.metric("Teams", f"{team_count_name:,}")

            # ---------------- Row 1 (Visual 1.1, 1.2, 1.3) ----------------
            c11, c12, c13 = st.columns(3)

            with c11:
                # [Visual 1.1] Total Amount by Year
                if {"YEAR","AMOUNT"}.issubset(df_main.columns) and not df_main.empty:
                    d = df_main.groupby("YEAR", as_index=False)["AMOUNT"].sum().sort_values("YEAR", kind="mergesort")
                    fig = px.bar(d, x="YEAR", y="AMOUNT", title="[Visual 1.1] Total Amount by Year")
                    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0))
                    st.plotly_chart(fig, use_container_width=True)

            with c12:
                # [Visual 1.2] Amount by Year & Source
                needed = {"YEAR","SOURCE","AMOUNT"}
                if needed.issubset(df_main.columns) and not df_main.empty:
                    d = df_main.groupby(["YEAR","SOURCE"], as_index=False)["AMOUNT"].sum().sort_values(["YEAR","SOURCE"], kind="mergesort")
                    fig = px.bar(d, x="YEAR", y="AMOUNT", color="SOURCE", barmode="stack",
                                 title="[Visual 1.2] Amount by Year & Source")
                    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), legend_title_text="")
                    st.plotly_chart(fig, use_container_width=True)

            with c13:
                # [Visual 1.3] Top Programs by Amount
                if {"PROGRAMNAME","AMOUNT"}.issubset(df_main.columns) and not df_main.empty:
                    d = (df_main.groupby("PROGRAMNAME", as_index=False)["AMOUNT"].sum()
                                 .sort_values("AMOUNT", ascending=False).head(12))
                    fig = px.bar(d, x="AMOUNT", y="PROGRAMNAME", orientation="h",
                                 title="[Visual 1.3] Top Programs by Amount")
                    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), yaxis_title="")
                    st.plotly_chart(fig, use_container_width=True)

            # ---------------- Row 2 (Visual 2.1, 2.2, 2.3) ----------------
            c21, c22, c23 = st.columns(3)

            with c21:
                # [Visual 2.1] Top Teams by Amount
                if {"TEAMNAME","AMOUNT"}.issubset(df_main.columns) and not df_main.empty:
                    d = (df_main.groupby("TEAMNAME", as_index=False)["AMOUNT"].sum()
                                 .sort_values("AMOUNT", ascending=False).head(12))
                    fig = px.bar(d, x="AMOUNT", y="TEAMNAME", orientation="h",
                                 title="[Visual 2.1] Top Teams by Amount")
                    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), yaxis_title="")
                    st.plotly_chart(fig, use_container_width=True)

            with c22:
                # [Visual 2.2] Top Application Groups by Amount
                if {"GROUPNAME","AMOUNT"}.issubset(df_main.columns) and not df_main.empty:
                    d = (df_main.groupby("GROUPNAME", as_index=False)["AMOUNT"].sum()
                                 .sort_values("AMOUNT", ascending=False).head(12))
                    fig = px.bar(d, x="AMOUNT", y="GROUPNAME", orientation="h",
                                 title="[Visual 2.2] Top Application Groups by Amount")
                    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), yaxis_title="")
                    st.plotly_chart(fig, use_container_width=True)

            with c23:
                # [Visual 2.3] TEAM Cost per PI (Eq-Split) by Application Group  — USE comp
                if not comp.empty and {"GROUPNAME","TEAM_COST_PERPI"}.issubset(comp.columns):
                    top_n = st.slider(
                        "Top Application Groups (by TEAM eq-split cost)",
                        5, 50, 20, key="top_groups_team_eq"
                    )
                    d = comp.groupby("GROUPNAME", as_index=False)["TEAM_COST_PERPI"].sum()
                    d = d.sort_values("TEAM_COST_PERPI", ascending=False).head(top_n)
                    fig = px.bar(
                        d, x="TEAM_COST_PERPI", y="GROUPNAME", orientation="h",
                        title="[Visual 2.3] TEAM Cost per PI (Eq-Split) by Application Group"
                    )
                    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), yaxis_title="", xaxis_title="TEAM Cost (Eq-Split)")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No TEAM eq-split cost available for current filters.")

            # ---------------- Row 3 (Visual 3.1, 3.2, 3.3) ----------------
            c31, c32, c33 = st.columns(3)

            with c31:
                # [Visual 3.1] Feature Cost by Year & PI (stacked) – union view (features only)
                needed = {"SOURCE","PI","YEAR","AMOUNT"}
                if needed.issubset(df_main.columns):
                    d = df_main[(df_main["SOURCE"] == "FEATURE") & df_main["PI"].notna()].copy()
                    if not d.empty:
                        d["PI"] = d["PI"].astype(int).astype(str)
                        d2 = d.groupby(["YEAR","PI"], as_index=False)["AMOUNT"].sum()
                        fig = px.bar(d2, x="YEAR", y="AMOUNT", color="PI",
                                     title="[Visual 3.1] Feature Cost by Year & PI (Stacked)")
                        fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), legend_title_text="PI")
                        st.plotly_chart(fig, use_container_width=True)

            with c32:
                # [Visual 3.2] SOURCE Split (Feature vs Invoice)
                if {"SOURCE","AMOUNT"}.issubset(df_main.columns) and not df_main.empty:
                    d = df_main.groupby("SOURCE", as_index=False)["AMOUNT"].sum()
                    fig = px.pie(d, values="AMOUNT", names="SOURCE", hole=0.5,
                                 title="[Visual 3.2] SOURCE Split (Feature vs Invoice)")
                    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0))
                    st.plotly_chart(fig, use_container_width=True)

            with c33:
                # [Visual 3.3] Feature Cost Components by Year (TEAM/DELIVERY/CS/C) – USE comp
                needed_cols = {"YEAR","TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"}
                if not comp.empty and needed_cols.issubset(comp.columns):
                    by_year = comp.groupby("YEAR", as_index=False)[
                        ["TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]
                    ].sum()
                    m = by_year.melt(id_vars="YEAR", var_name="Component", value_name="Amount")
                    comp_map = {
                        "TEAM_COST_PERPI": "TEAM (PI fixed eq-split)",
                        "DEL_TEAM_COST_PERPI": "DELIVERY (effort share)",
                        "TEAM_CONTRACTOR_CS_COST_PERPI": "CONTRACTOR_CS (effort share)",
                        "TEAM_CONTRACTOR_C_COST_PERPI": "CONTRACTOR_C (effort share)",
                    }
                    m["Component"] = m["Component"].map(comp_map)
                    fig = px.bar(m, x="YEAR", y="Amount", color="Component",
                                 title="[Visual 3.3] Feature Cost Components by Year (Stacked)")
                    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), legend_title_text="")
                    st.plotly_chart(fig, use_container_width=True)

            # ---------------- Row 4 (Visual 4.1, 4.2, 4.3) ----------------
            c41, c42, c43 = st.columns(3)

            with c41:
                # [Visual 4.1] Feature Cost Components by PI – USE comp
                needed_cols = {"PI","TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"}
                if not comp.empty and needed_cols.issubset(comp.columns):
                    d = comp.dropna(subset=["PI"]).copy()
                    if not d.empty:
                        d["PI"] = d["PI"].astype(int)
                        by_pi = d.groupby("PI", as_index=False)[
                            ["TEAM_COST_PERPI","DEL_TEAM_COST_PERPI","TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"]
                        ].sum()
                        m = by_pi.melt(id_vars="PI", var_name="Component", value_name="Amount")
                        comp_map = {
                            "TEAM_COST_PERPI": "TEAM (PI fixed eq-split)",
                            "DEL_TEAM_COST_PERPI": "DELIVERY (effort share)",
                            "TEAM_CONTRACTOR_CS_COST_PERPI": "CONTRACTOR_CS (effort share)",
                            "TEAM_CONTRACTOR_C_COST_PERPI": "CONTRACTOR_C (effort share)",
                        }
                        m["Component"] = m["Component"].map(comp_map)
                        fig = px.bar(m, x="PI", y="Amount", color="Component",
                                     title="[Visual 4.1] Feature Cost Components by PI (Stacked)")
                        fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), legend_title_text="Component")
                        st.plotly_chart(fig, use_container_width=True)

            with c42:
                # [Visual 4.2] Effort Points by Year (Features) – USE comp
                if not comp.empty and {"YEAR","EFFORT_POINTS"}.issubset(comp.columns):
                    e = comp.groupby("YEAR", as_index=False)["EFFORT_POINTS"].sum().sort_values("YEAR", kind="mergesort")
                    fig = px.bar(e, x="YEAR", y="EFFORT_POINTS", title="[Visual 4.2] Effort Points by Year (Features)")
                    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0))
                    st.plotly_chart(fig, use_container_width=True)

            with c43:
                # [Visual 4.3] Effort Points by PI (Features) – USE comp
                if not comp.empty and {"PI","EFFORT_POINTS"}.issubset(comp.columns):
                    d = comp.dropna(subset=["PI"]).copy()
                    if not d.empty:
                        d["PI"] = d["PI"].astype(int)
                        e = d.groupby("PI", as_index=False)["EFFORT_POINTS"].sum().sort_values("PI", kind="mergesort")
                        fig = px.bar(e, x="PI", y="EFFORT_POINTS", title="[Visual 4.3] Effort Points by PI (Features)")
                        fig.update_layout(margin=dict(l=0,r=0,t=40,b=0))
                        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # Tab 2: Data Sheet
    # =========================
    with tab_sheet:
        st.write("Use this sheet to validate rows/columns from `VW_COSTS_AND_INVOICES` (union) and feature components from `VW_TEAM_COSTS_PER_FEATURE` (no duplication in component visuals).")

        if df_main.empty and comp.empty:
            st.info("No rows match the current filters.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Rows (union view)", f"{len(df_main):,}")
            c2.metric("Distinct Programs (by name)", f"{_nunique_by_name(df_main.get('PROGRAMNAME')):,}")
            c3.metric("Sum(Amount)", f"${df_main['AMOUNT'].sum():,.0f}")

            # Union view table
            default_cols = ["SOURCE","YEAR","PI","PROGRAMNAME","TEAMNAME","GROUPNAME","EMPLOYEE_TYPE","AMOUNT"]
            cols = st.multiselect(
                "Columns to display (Union View)",
                options=list(df_main.columns),
                default=[c for c in default_cols if c in df_main.columns],
            )
            st.dataframe(df_main[cols] if cols else df_main, use_container_width=True, hide_index=True)

            # Components table (basis for component visuals)
            with st.expander("Feature Components (from VW_TEAM_COSTS_PER_FEATURE)"):
                st.caption("This table drives component visuals and avoids duplication.")
                comp_cols_default = [
                    "YEAR","PI","PROGRAMNAME","TEAMNAME","GROUPNAME",
                    "EFFORT_POINTS","TEAM_COST_PERPI","DEL_TEAM_COST_PERPI",
                    "TEAM_CONTRACTOR_CS_COST_PERPI","TEAM_CONTRACTOR_C_COST_PERPI"
                ]
                comp_cols = st.multiselect(
                    "Columns to display (Components View)",
                    options=list(comp.columns),
                    default=[c for c in comp_cols_default if c in comp.columns],
                    key="comp_cols_selector"
                )
                st.dataframe(comp[comp_cols] if comp_cols else comp, use_container_width=True, hide_index=True)

            # Download buttons
            csv_union = (df_main[cols] if cols else df_main).to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download Union View CSV",
                data=csv_union,
                file_name="dashboard_union_view.csv",
                mime="text/csv",
                use_container_width=True,
            )

            csv_comp = (comp[comp_cols] if comp_cols else comp).to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download Components View CSV",
                data=csv_comp,
                file_name="dashboard_components_view.csv",
                mime="text/csv",
                use_container_width=True,
            )
