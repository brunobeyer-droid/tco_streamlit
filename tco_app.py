# tco_app.py
import streamlit as st
import pandas as pd
import datetime as dt

# Adjust this import if snowflake_db.py is under utils/
from snowflake_db import ensure_tables, fetch_df

# Optional: sidebar helper
try:
    from utils.sidebar import render_global_actions
except Exception:
    def render_global_actions():
        pass

st.set_page_config(page_title="TCO Home", page_icon="📊", layout="wide")
render_global_actions()

# Ensure schema once per session
if "_tco_init" not in st.session_state:
    ensure_tables()
    st.session_state["_tco_init"] = True

st.title("📊 TCO – Total Cost Overview")

# -----------------------------------
# Data loader
# -----------------------------------
@st.cache_data(show_spinner=False)
def load_overview_df() -> pd.DataFrame:
    # Use snapshots (PROGRAMID_AT_BOOKING/VENDORID_AT_BOOKING) when present, so history is stable per year
    sql = """
        SELECT
          i.INVOICEID,
          i.FISCAL_YEAR,
          i.INVOICEDATE,
          i.RENEWALDATE,
          i.AMOUNT,
          i.STATUS,

          -- Team/program/application/vendor (prefer frozen snapshots where available)
          t.TEAMID, t.TEAMNAME,
          COALESCE(i.PROGRAMID_AT_BOOKING, t.PROGRAMID) AS PROGRAMID,
          p.PROGRAMNAME,
          a.APPLICATIONID, a.APPLICATIONNAME,
          COALESCE(i.VENDORID_AT_BOOKING, a.VENDORID) AS VENDORID,
          v.VENDORNAME
        FROM INVOICES i
        LEFT JOIN TEAMS t ON t.TEAMID = i.TEAMID
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = COALESCE(i.PROGRAMID_AT_BOOKING, t.PROGRAMID)
        LEFT JOIN APPLICATIONS a ON a.APPLICATIONID = i.APPLICATIONID
        LEFT JOIN VENDORS v ON v.VENDORID = COALESCE(i.VENDORID_AT_BOOKING, a.VENDORID)
    """
    return fetch_df(sql)

df_all = load_overview_df()

# -----------------------------------
# Filters
# -----------------------------------
with st.expander("🔎 Filters", expanded=True):
    # Year first
    current_year = dt.date.today().year
    years = sorted(df_all["FISCAL_YEAR"].dropna().astype(int).unique().tolist()) if not df_all.empty else [current_year]
    default_year = current_year if current_year in years else (years[-1] if years else current_year)
    year = st.number_input("Fiscal Year", value=int(default_year), step=1, format="%d")

    # Build options safely even if frame is empty
    prog_opts = sorted(df_all["PROGRAMNAME"].dropna().unique().tolist()) if not df_all.empty else []
    team_opts = sorted(df_all["TEAMNAME"].dropna().unique().tolist()) if not df_all.empty else []
    vend_opts = sorted(df_all["VENDORNAME"].dropna().unique().tolist()) if not df_all.empty else []
    app_opts  = sorted(df_all["APPLICATIONNAME"].dropna().unique().tolist()) if not df_all.empty else []

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        sel_prog = st.multiselect("Program", prog_opts, key="flt_prog")
    with c2:
        sel_team = st.multiselect("Team", team_opts, key="flt_team")
    with c3:
        sel_vend = st.multiselect("Vendor", vend_opts, key="flt_vend")
    with c4:
        sel_app = st.multiselect("Application", app_opts, key="flt_app")

    cc1, cc2 = st.columns([1, 5])
    with cc1:
        if st.button("Clear filters"):
            st.session_state.pop("flt_prog", None)
            st.session_state.pop("flt_team", None)
            st.session_state.pop("flt_vend", None)
            st.session_state.pop("flt_app", None)
            st.experimental_rerun()
    with cc2:
        st.caption("Filters apply to the selected fiscal year.")

# Apply filters in pandas
df = df_all.copy()
if not df.empty:
    df = df[df["FISCAL_YEAR"].fillna(0).astype(int) == int(year)]
    if sel_prog:
        df = df[df["PROGRAMNAME"].isin(sel_prog)]
    if sel_team:
        df = df[df["TEAMNAME"].isin(sel_team)]
    if sel_vend:
        df = df[df["VENDORNAME"].isin(sel_vend)]
    if sel_app:
        df = df[df["APPLICATIONNAME"].isin(sel_app)]

# -----------------------------------
# KPI cards based on filtered set
# -----------------------------------
col1, col2, col3, col4, col5, col6 = st.columns(6)
if df.empty:
    col1.metric("Programs", 0)
    col2.metric("Teams", 0)
    col3.metric("Vendors", 0)
    col4.metric("Applications", 0)
    col5.metric(f"Invoices ({int(year)})", 0)
    col6.metric("Amount (sum)", "0.00")
else:
    col1.metric("Programs", df["PROGRAMNAME"].nunique())
    col2.metric("Teams", df["TEAMNAME"].nunique())
    col3.metric("Vendors", df["VENDORNAME"].nunique())
    col4.metric("Applications", df["APPLICATIONNAME"].nunique())
    col5.metric(f"Invoices ({int(year)})", df["INVOICEID"].nunique())
    col6.metric("Amount (sum)", f"{df['AMOUNT'].fillna(0).sum():,.2f}")

st.divider()

# -----------------------------------
# Table + CSV download
# -----------------------------------
st.subheader(f"Invoices (Fiscal Year {int(year)})")
if df.empty:
    st.info("No data for the current filters.")
else:
    # Friendly column order for display
    display_cols = [
        "INVOICEID", "FISCAL_YEAR", "INVOICEDATE", "RENEWALDATE", "STATUS", "AMOUNT",
        "PROGRAMNAME", "TEAMNAME", "VENDORNAME", "APPLICATIONNAME"
    ]
    show_cols = [c for c in display_cols if c in df.columns]
    table = df[show_cols].sort_values(by=["INVOICEDATE"], ascending=False)
    st.dataframe(table, use_container_width=True)

    # Download filtered data
    csv = table.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download filtered CSV",
        data=csv,
        file_name=f"tco_filtered_invoices_{int(year)}.csv",
        mime="text/csv",
    )

# Helpful tips
with st.expander("ℹ️ Tips"):
    st.markdown(
        "- Use **Fiscal Year** + the filters to slice the data.\n"
        "- Metrics reflect your current selections.\n"
        "- To add/rename Applications, use the **Applications** page.\n"
        "- To create invoices, use **Invoices**; for deletions/rollover, see **Edit (advanced)**."
    )
