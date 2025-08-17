import streamlit as st
import pandas as pd
import uuid
from datetime import date
from snowflake_db import ensure_tables as _ensure_tables, fetch_df, upsert_invoice

st.set_page_config(page_title="Invoices", page_icon="🧾", layout="wide")
st.title("🧾 Invoice Tracking")

# Init once
if not st.session_state.get("_init_invoices_done"):
    _ensure_tables()
    st.session_state["_init_invoices_done"] = True

@st.cache_data(show_spinner=False)
def get_programs_df():
    return fetch_df("""
        SELECT PROGRAMID, PROGRAMNAME
        FROM TCODB.PUBLIC.PROGRAMS
        ORDER BY PROGRAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_teams_df():
    return fetch_df("""
        SELECT TEAMID, TEAMNAME, PROGRAMID
        FROM TCODB.PUBLIC.TEAMS
        ORDER BY TEAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_vendors_df():
    return fetch_df("""
        SELECT VENDORID, VENDORNAME
        FROM TCODB.PUBLIC.VENDORS
        ORDER BY VENDORNAME;
    """)

@st.cache_data(show_spinner=False)
def get_invoices_df():
    return fetch_df("""
        SELECT
          i.INVOICEID,
          i.VENDOR,
          i.APPLICATIONID,
          i.TEAMID,
          t.TEAMNAME,
          p.PROGRAMNAME,
          i.INVOICEDATE,
          i.RENEWALDATE,
          i.AMOUNT,
          i.STATUS
        FROM TCODB.PUBLIC.INVOICES i
        LEFT JOIN TCODB.PUBLIC.TEAMS t
          ON t.TEAMID = i.TEAMID
        LEFT JOIN TCODB.PUBLIC.PROGRAMS p
          ON p.PROGRAMID = t.PROGRAMID
        ORDER BY i.RENEWALDATE DESC NULLS LAST, i.INVOICEDATE DESC NULLS LAST, p.PROGRAMNAME, t.TEAMNAME;
    """)

def invalidate_caches():
    get_invoices_df.clear()
    get_programs_df.clear()
    get_teams_df.clear()
    get_vendors_df.clear()

# ---------------- Add Invoice (create only) ----------------
st.subheader("Add Invoice")

prog_df = get_programs_df()
team_df = get_teams_df()
vend_df = get_vendors_df()

if prog_df.empty or team_df.empty or vend_df.empty:
    st.warning("To add an invoice you need at least one Program, one Team, and one Vendor.")
else:
    c1, c2, c3 = st.columns([2.2, 2.2, 1.2], gap="small")
    with c1:
        prog_name = st.selectbox("Program", options=prog_df["PROGRAMNAME"].tolist())
        prog_id = prog_df.set_index("PROGRAMNAME").loc[prog_name, "PROGRAMID"]
    with c2:
        teams_filtered = team_df[team_df["PROGRAMID"] == prog_id]
        team_name = st.selectbox("Team", options=teams_filtered["TEAMNAME"].tolist())
        team_id = teams_filtered.set_index("TEAMNAME").loc[team_name, "TEAMID"]
    with c3:
        vendor_name = st.selectbox("Vendor", options=vend_df["VENDORNAME"].tolist())

    c4, c5, c6 = st.columns([1.4, 1.4, 1.2], gap="small")
    with c4:
        application_id = st.text_input("Application (optional)", placeholder="app code / id")
    with c5:
        invoice_date = st.date_input("Invoice Date", value=date.today())
    with c6:
        renewal_date = st.date_input("Renewal Date", value=date.today())

    c7, c8 = st.columns([1.2, 2.8], gap="small")
    with c7:
        amount = st.number_input("Amount", min_value=0.0, step=100.0, value=0.0)
    with c8:
        status = st.selectbox("Status", ["Planned", "Approved", "Paid", "Cancelled"], index=0)

    if st.button("Save Invoice", type="primary"):
        upsert_invoice(
            invoice_id=str(uuid.uuid4()),
            application_id=application_id.strip() if application_id else None,
            team_id=str(team_id),
            invoice_date=invoice_date,
            renewal_date=renewal_date,
            amount=float(amount or 0.0),
            status=status,
            vendor=vendor_name,
        )
        st.success(f"Invoice saved for team '{team_name}' ({prog_name}).")
        invalidate_caches()
        st.rerun()

st.divider()

# ---------------- Filters + List ----------------
st.subheader("Invoices")

raw = get_invoices_df().copy()
for col in ["INVOICEDATE", "RENEWALDATE"]:
    if col in raw.columns:
        raw[col] = pd.to_datetime(raw[col], errors="coerce").dt.date

with st.expander("Filters", expanded=True):
    c1, c2 = st.columns([3, 2])
    with c1:
        programs = sorted(raw["PROGRAMNAME"].dropna().unique().tolist()) if not raw.empty else []
        sel_programs = st.multiselect("Program(s)", options=programs, default=programs)

        if sel_programs:
            team_opts = sorted(raw[raw["PROGRAMNAME"].isin(sel_programs)]["TEAMNAME"].dropna().unique().tolist())
        else:
            team_opts = sorted(raw["TEAMNAME"].dropna().unique().tolist()) if not raw.empty else []
        sel_teams = st.multiselect("Team(s)", options=team_opts, default=team_opts)

    with c2:
        vendors = sorted(raw["VENDOR"].dropna().unique().tolist()) if not raw.empty else []
        sel_vendors = st.multiselect("Vendor(s)", options=vendors, default=vendors)

        statuses = sorted(raw["STATUS"].dropna().unique().tolist()) if not raw.empty else []
        sel_status = st.multiselect("Status", options=statuses, default=statuses)

    d1, d2, d3 = st.columns(3)
    with d1:
        min_date = raw["RENEWALDATE"].min() if ("RENEWALDATE" in raw and not raw["RENEWALDATE"].isna().all()) else None
        start = st.date_input("Renewal start", value=min_date or date(2000,1,1))
    with d2:
        max_date = raw["RENEWALDATE"].max() if ("RENEWALDATE" in raw and not raw["RENEWALDATE"].isna().all()) else None
        end = st.date_input("Renewal end", value=max_date or date.today())
    with d3:
        search = st.text_input("Search (vendor/app/status)", placeholder="text contains...")

df = raw.copy()
if not df.empty:
    if sel_programs:
        df = df[df["PROGRAMNAME"].isin(sel_programs)]
    if sel_teams:
        df = df[df["TEAMNAME"].isin(sel_teams)]
    if sel_vendors:
        df = df[df["VENDOR"].isin(sel_vendors)]
    if sel_status:
        df = df[df["STATUS"].isin(sel_status)]
    if "RENEWALDATE" in df.columns:
        df = df[(df["RENEWALDATE"].isna()) | ((df["RENEWALDATE"] >= start) & (df["RENEWALDATE"] <= end))]
    if search:
        q = search.lower()
        df = df[df.apply(lambda r: any(q in str(r.get(k, "")).lower() for k in ["VENDOR","APPLICATIONID","STATUS"]), axis=1)]

m1, m2, m3 = st.columns(3)
with m1: st.metric("Invoices", int(df.shape[0]) if not df.empty else 0)
with m2: 
    total_amt = float(df["AMOUNT"].fillna(0).sum()) if ("AMOUNT" in df and not df.empty) else 0.0
    st.metric("Total Amount", f"{total_amt:,.2f}")
with m3: st.metric("Vendors", df["VENDOR"].nunique() if ("VENDOR" in df and not df.empty) else 0)

st.divider()

if df.empty:
    st.info("No invoices found for the selected filters.")
else:
    show = df.rename(columns={
        "PROGRAMNAME": "Program",
        "TEAMNAME": "Team",
        "INVOICEDATE": "Invoice Date",
        "RENEWALDATE": "Renewal Date",
        "AMOUNT": "Amount",
        "STATUS": "Status",
        "VENDOR": "Vendor",
        "APPLICATIONID": "Application",
    })[["Program", "Team", "Vendor", "Application", "Invoice Date", "Renewal Date", "Amount", "Status"]]
    st.dataframe(show, use_container_width=True)
