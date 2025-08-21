# pages/Dashboard.py
import sys, subprocess, json
import pandas as pd
import streamlit as st

# ---- self-heal Plotly if missing ----
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

# ---- Snowflake helpers ----
try:
    from snowflake_db import fetch_df, ensure_all_views_ok
    try:
        ensure_all_views_ok()
    except Exception:
        pass
except ImportError:
    from snowflake_db import fetch_df

st.set_page_config(page_title="TCO Dashboard", layout="wide")

# =========================
# Query helpers
# =========================
@st.cache_data(ttl=120)
def _choices(col: str) -> list:
    sql = f"SELECT DISTINCT {col} AS V FROM VW_COSTS_AND_INVOICES ORDER BY 1"
    df = fetch_df(sql)
    if df.empty:
        return []
    return [v for v in df["V"].tolist() if v is not None and v != ""]

@st.cache_data(ttl=120)
def _fetch_filtered(years, programs, teams, groups, sources, emp_types) -> pd.DataFrame:
    where = ["1=1"]
    params = []

    def _json_in(col, values, as_number=False):
        if not values:
            return
        where.append(
            f"""{col} IN (
                  SELECT VALUE::{'NUMBER' if as_number else 'STRING'}
                  FROM TABLE(FLATTEN(input=>PARSE_JSON(%s)))
                )"""
        )
        params.append(json.dumps(values))

    _json_in("YEAR", years, as_number=True)
    _json_in("PROGRAMNAME", programs)
    _json_in("TEAMNAME", teams)
    _json_in("GROUPNAME", groups)
    _json_in("SOURCE", sources)
    _json_in("EMPLOYEE_TYPE", emp_types)

    sql = f"""
        SELECT
            SOURCE, YEAR, PI,
            PROGRAMID, PROGRAMNAME, TEAMID, TEAMNAME, GROUPID, GROUPNAME,
            EMPLOYEE_TYPE, AMOUNT
        FROM VW_COSTS_AND_INVOICES
        WHERE {' AND '.join(where)}
    """
    df = fetch_df(sql, tuple(params) if params else None)
    return df if not df.empty else pd.DataFrame(
        columns=["SOURCE","YEAR","PI","PROGRAMID","PROGRAMNAME","TEAMID","TEAMNAME",
                 "GROUPID","GROUPNAME","EMPLOYEE_TYPE","AMOUNT"]
    )

# =========================
# Layout
# =========================
st.title("Cost & Invoice Dashboard")

left, right = st.columns([3, 1], gap="large")

# ---------- Filters (right) ----------
with right:
    st.subheader("Filters")

    years_all      = _choices("YEAR")
    sources_all    = _choices("SOURCE")
    programs_all   = _choices("PROGRAMNAME")
    teams_all      = _choices("TEAMNAME")
    groups_all     = _choices("GROUPNAME")
    emp_types_all  = _choices("EMPLOYEE_TYPE")

    years      = st.multiselect("Year", years_all, default=[])
    sources    = st.multiselect("Source (FEATURE / INVOICE)", sources_all, default=[])
    programs   = st.multiselect("Program", programs_all, default=[])
    teams      = st.multiselect("Team", teams_all, default=[])
    groups     = st.multiselect("Application Group", groups_all, default=[])
    emp_types  = st.multiselect("Employee Type (Feature rows)", emp_types_all, default=[])

    if st.button("Clear filters", use_container_width=True):
        st.experimental_rerun()

# ---------- Data (uses right-side selections) ----------
df = _fetch_filtered(years, programs, teams, groups, sources, emp_types)

with left:
    if df.empty:
        st.info("No data for the current filters.")
    else:
        # ==== KPI strip (Amount, Programs, Teams) ====
        total_amount = float(df["AMOUNT"].sum()) if "AMOUNT" in df else 0.0
        prog_count   = int(df["PROGRAMID"].nunique()) if "PROGRAMID" in df else 0
        team_count   = int(df["TEAMID"].nunique()) if "TEAMID" in df else 0

        k1, k2, k3 = st.columns(3)
        with k1:
            st.metric("Total Amount", f"${total_amount:,.0f}")
        with k2:
            st.metric("Programs", f"{prog_count:,}")
        with k3:
            st.metric("Teams", f"{team_count:,}")

        # Optional sanity note if IDs vs names don’t align (can happen if invoices use a different TEAMID)
        if df["TEAMID"].nunique() != df["TEAMNAME"].nunique():
            st.caption(
                "⚠️ Team count differs by **ID** vs **Name**. "
                "This usually means some rows reference a different TEAMID for the same team "
                "(e.g., features vs invoices). Check mappings."
            )

        # ---------------- Row 1 ----------------
        r1c1, r1c2 = st.columns(2)
        with r1c1:
            # Total by Year
            d = df.groupby("YEAR", as_index=False)["AMOUNT"].sum().sort_values("YEAR")
            fig = px.bar(d, x="YEAR", y="AMOUNT", title="Total Amount by Year")
            fig.update_layout(margin=dict(l=0,r=0,t=40,b=0))
            st.plotly_chart(fig, use_container_width=True)

        with r1c2:
            # Source by Year (stacked)
            d = df.groupby(["YEAR","SOURCE"], as_index=False)["AMOUNT"].sum()
            fig = px.bar(d, x="YEAR", y="AMOUNT", color="SOURCE", barmode="stack",
                         title="Amount by Year & Source")
            fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), legend_title_text="")
            st.plotly_chart(fig, use_container_width=True)

        # ---------------- Row 2 ----------------
        r2c1, r2c2 = st.columns(2)
        with r2c1:
            # Top Programs
            d = (df.groupby("PROGRAMNAME", as_index=False)["AMOUNT"].sum()
                   .sort_values("AMOUNT", ascending=False)
                   .head(12))
            fig = px.bar(d, x="AMOUNT", y="PROGRAMNAME", orientation="h",
                         title="Top Programs by Amount")
            fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)

        with r2c2:
            # Top Teams
            d = (df.groupby("TEAMNAME", as_index=False)["AMOUNT"].sum()
                   .sort_values("AMOUNT", ascending=False)
                   .head(12))
            fig = px.bar(d, x="AMOUNT", y="TEAMNAME", orientation="h",
                         title="Top Teams by Amount")
            fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)

        # ---------------- Row 3 ----------------
        r3c1, r3c2 = st.columns(2)
        with r3c1:
            # Top Groups
            d = (df.groupby("GROUPNAME", as_index=False)["AMOUNT"].sum()
                   .sort_values("AMOUNT", ascending=False)
                   .head(12))
            fig = px.bar(d, x="AMOUNT", y="GROUPNAME", orientation="h",
                         title="Top Application Groups by Amount")
            fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)

        with r3c2:
            # Employee Type mix (feature rows only)
            d = df[df["SOURCE"] == "FEATURE"].copy()
            if not d.empty:
                d = d.groupby("EMPLOYEE_TYPE", as_index=False)["AMOUNT"].sum()
                fig = px.pie(d, values="AMOUNT", names="EMPLOYEE_TYPE",
                             title="Employee Type Mix (Features)")
                fig.update_layout(margin=dict(l=0,r=0,t=40,b=0))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No feature rows in current selection for Employee Type chart.")

        # ---------------- Row 4 ----------------
        r4c1, r4c2 = st.columns(2)
        with r4c1:
            # Year & PI (features only)
            d = df[(df["SOURCE"] == "FEATURE") & df["PI"].notna()].copy()
            if not d.empty:
                d["PI"] = d["PI"].astype(str)
                d2 = d.groupby(["YEAR","PI"], as_index=False)["AMOUNT"].sum()
                fig = px.bar(d2, x="YEAR", y="AMOUNT", color="PI",
                             title="Feature Cost by Year & PI (Stacked)")
                fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), legend_title_text="PI")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No PI data in current selection.")

        with r4c2:
            # Source split donut
            d = df.groupby("SOURCE", as_index=False)["AMOUNT"].sum()
            fig = px.pie(d, values="AMOUNT", names="SOURCE", hole=0.5,
                         title="SOURCE Split (Feature vs Invoice)")
            fig.update_layout(margin=dict(l=0,r=0,t=40,b=0))
            st.plotly_chart(fig, use_container_width=True)

        # ---- Optional: quick peek at distinct programs/teams under current filters
        with st.expander("Distinct Programs & Teams in current selection"):
            a, b = st.columns(2)
            with a:
                st.write("Programs")
                prog_list = (df[["PROGRAMID","PROGRAMNAME"]]
                             .drop_duplicates()
                             .sort_values(["PROGRAMNAME","PROGRAMID"]))
                st.dataframe(prog_list, hide_index=True, use_container_width=True)
            with b:
                st.write("Teams")
                team_list = (df[["TEAMID","TEAMNAME"]]
                             .drop_duplicates()
                             .sort_values(["TEAMNAME","TEAMID"]))
                st.dataframe(team_list, hide_index=True, use_container_width=True)
