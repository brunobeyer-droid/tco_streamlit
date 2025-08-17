# filename: pages/edit.py
import streamlit as st
import pandas as pd
from datetime import date
from snowflake_db import (
    ensure_tables as _ensure_tables,
    fetch_df,
    upsert_program, delete_program,
    upsert_team, delete_team,
    upsert_vendor, delete_vendor,
    upsert_invoice, delete_invoice,
)

st.set_page_config(page_title="Edit", page_icon="✏️", layout="wide")
st.title("✏️ Edit Data")

# ---------- Compact UI CSS ----------
st.markdown(
    """
    <style>
      .stTextInput > div > div > input,
      .stNumberInput input,
      .stDateInput input { padding-top: 6px !important; padding-bottom: 6px !important; height: 32px !important; }
      .stSelectbox > div > div > div[data-baseweb="select"] { min-height: 34px !important; }
      .stButton > button { padding: 0.3rem 0.6rem !important; }
      .thin-sep { border-bottom: 1px solid rgba(0,0,0,0.06); margin: 6px 0 10px 0; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- Init once ----------
if not st.session_state.get("_init_edit_done"):
    _ensure_tables()
    st.session_state["_init_edit_done"] = True

# ---------- Cached loaders ----------
@st.cache_data(show_spinner=False)
def get_programs_df():
    return fetch_df("""
        SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE
        FROM TCODB.PUBLIC.PROGRAMS
        ORDER BY PROGRAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_teams_df():
    return fetch_df("""
        SELECT
          t.TEAMID,
          t.TEAMNAME,
          t.PROGRAMID,
          p.PROGRAMNAME,
          t.TEAMFTE,
          t.COSTPERFTE
        FROM TCODB.PUBLIC.TEAMS t
        LEFT JOIN TCODB.PUBLIC.PROGRAMS p
          ON p.PROGRAMID = t.PROGRAMID
        ORDER BY p.PROGRAMNAME, t.TEAMNAME;
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
          t.PROGRAMID,
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

def invalidate_all():
    get_programs_df.clear()
    get_teams_df.clear()
    get_vendors_df.clear()
    get_invoices_df.clear()

# ---------- Tabs ----------
tab_prog, tab_team, tab_vendor, tab_invoice = st.tabs(["Programs", "Teams", "Vendors", "Invoices"])

# =========================================================
# Programs (compact fields & buttons)
# =========================================================
with tab_prog:
    st.subheader("Programs")
    pdf = get_programs_df()
    if pdf.empty:
        st.info("No programs found.")
    else:
        hc1, hc2, hc3, hc4 = st.columns([2.6, 2, 1.1, 1.2], gap="small")
        with hc1: st.markdown("**Program Name**")
        with hc2: st.markdown("**Owner**")
        with hc3: st.markdown("**FTE**")
        with hc4: st.markdown("**Actions**")
        st.markdown("<div class='thin-sep'></div>", unsafe_allow_html=True)

        for _, row in pdf.iterrows():
            pid   = row["PROGRAMID"]
            pname = row["PROGRAMNAME"] or ""
            pown  = row["PROGRAMOWNER"] or ""
            pfte  = float(row["PROGRAMFTE"] or 0.0)

            with st.form(f"prog_{pid}", clear_on_submit=False):
                c1, c2, c3, c4 = st.columns([2.6, 2, 1.1, 1.2], gap="small")
                with c1:
                    new_name = st.text_input("Program Name", value=pname, key=f"pn_{pid}", label_visibility="collapsed")
                with c2:
                    new_owner = st.text_input("Owner", value=pown, key=f"po_{pid}", label_visibility="collapsed")
                with c3:
                    new_fte = st.number_input("FTE", min_value=0.0, step=0.5, value=pfte, key=f"pf_{pid}", label_visibility="collapsed")
                with c4:
                    u = st.form_submit_button("Update", type="primary", use_container_width=True)
                    d = st.form_submit_button("Delete", use_container_width=True)

                if u:
                    upsert_program(pid, new_name.strip(), new_owner.strip() if new_owner else None, float(new_fte or 0.0))
                    st.success(f"Updated program '{new_name.strip()}'.")
                    invalidate_all(); st.rerun()
                if d:
                    delete_program(pid)
                    st.success(f"Deleted program '{pname}'.")
                    invalidate_all(); st.rerun()

# =========================================================
# Teams (Program filter; Program immutable)
# =========================================================
with tab_team:
    st.subheader("Teams")
    tdf = get_teams_df()
    pdf = get_programs_df()
    if tdf.empty or pdf.empty:
        st.info("No teams/programs found.")
    else:
        # Filter which program's teams to edit
        prog_options = pdf["PROGRAMNAME"].tolist()
        sel_prog = st.selectbox("Filter by Program", options=prog_options, index=0 if prog_options else None)
        sel_prog_id = pdf.set_index("PROGRAMNAME").loc[sel_prog, "PROGRAMID"] if sel_prog else None

        fdf = tdf[tdf["PROGRAMID"] == sel_prog_id] if sel_prog_id else tdf.copy()

        hc1, hc2, hc3, hc4, hc5 = st.columns([2.4, 2, 1.1, 1.1, 1.4], gap="small")
        with hc1: st.markdown("**Team Name**")
        with hc2: st.markdown("**Program**")
        with hc3: st.markdown("**FTE**")
        with hc4: st.markdown("**Cost / FTE**")
        with hc5: st.markdown("**Actions**")
        st.markdown("<div class='thin-sep'></div>", unsafe_allow_html=True)

        for _, row in fdf.iterrows():
            tid   = row["TEAMID"]
            tname = row["TEAMNAME"] or ""
            tpfte = float(row["TEAMFTE"] or 0.0)
            tcost = float(row["COSTPERFTE"] or 0.0)
            prog_name = row["PROGRAMNAME"] or ""

            with st.form(f"team_{tid}", clear_on_submit=False):
                c1, c2, c3, c4, c5 = st.columns([2.4, 2, 1.1, 1.1, 1.4], gap="small")
                with c1:
                    new_tname = st.text_input("Team", value=tname, key=f"tn_{tid}", label_visibility="collapsed")
                with c2:
                    # Program is immutable after creation
                    st.text_input("Program", value=prog_name, key=f"tp_ro_{tid}", label_visibility="collapsed", disabled=True)
                with c3:
                    new_fte = st.number_input("FTE", min_value=0.0, step=0.5, value=tpfte, key=f"tf_{tid}", label_visibility="collapsed")
                with c4:
                    new_cost = st.number_input("Cost / FTE", min_value=0.0, step=100.0, value=tcost, key=f"tc_{tid}", label_visibility="collapsed")
                with c5:
                    u = st.form_submit_button("Update", type="primary", use_container_width=True)
                    d = st.form_submit_button("Delete", use_container_width=True)

                if u:
                    # Keep original PROGRAMID (immutable)
                    upsert_team(tid, new_tname.strip(), row["PROGRAMID"], float(new_cost or 0.0), float(new_fte or 0.0))
                    st.success(f"Updated team '{new_tname.strip()}'.")
                    invalidate_all(); st.rerun()
                if d:
                    delete_team(tid)
                    st.success(f"Deleted team '{tname}'.")
                    invalidate_all(); st.rerun()

# =========================================================
# Vendors (compact buttons)
# =========================================================
with tab_vendor:
    st.subheader("Vendors")
    vdf = get_vendors_df()
    if vdf.empty:
        st.info("No vendors found.")
    else:
        hc1, hc2 = st.columns([3, 1.2], gap="small")
        with hc1: st.markdown("**Vendor**")
        with hc2: st.markdown("**Actions**")
        st.markdown("<div class='thin-sep'></div>", unsafe_allow_html=True)

        for _, row in vdf.iterrows():
            vid = row["VENDORID"]
            vnm = row["VENDORNAME"] or ""
            with st.form(f"vendor_{vid}", clear_on_submit=False):
                c1, c2 = st.columns([3, 1.2], gap="small")
                with c1:
                    new_vnm = st.text_input("Vendor", value=vnm, key=f"vn_{vid}", label_visibility="collapsed")
                with c2:
                    u = st.form_submit_button("Update", type="primary", use_container_width=True)
                    d = st.form_submit_button("Delete", use_container_width=True)
                if u:
                    upsert_vendor(vid, new_vnm.strip())
                    st.success(f"Updated vendor '{new_vnm.strip()}'.")
                    invalidate_all(); st.rerun()
                if d:
                    delete_vendor(vid)
                    st.success(f"Deleted vendor '{vnm}'.")
                    invalidate_all(); st.rerun()

# =========================================================
# Invoices (Program/Team filters; Program/Team immutable)
# =========================================================
with tab_invoice:
    st.subheader("Invoices")
    idf = get_invoices_df()
    tdf = get_teams_df()
    pdf = get_programs_df()
    vdf = get_vendors_df()

    if idf.empty:
        st.info("No invoices found.")
    else:
        # Normalize dates
        for col in ["INVOICEDATE", "RENEWALDATE"]:
            if col in idf.columns:
                idf[col] = pd.to_datetime(idf[col], errors="coerce").dt.date

        # Top filters (only control which invoices appear; do NOT change relationships)
        prog_opts = pdf["PROGRAMNAME"].tolist() if not pdf.empty else []
        sel_prog = st.selectbox("Filter by Program", options=["(All)"] + prog_opts, index=0)
        if sel_prog != "(All)":
            sel_prog_id = pdf.set_index("PROGRAMNAME").loc[sel_prog, "PROGRAMID"]
            idf_f = idf[idf["PROGRAMID"] == sel_prog_id].copy()
        else:
            idf_f = idf.copy()

        # Team options depend on Program filter
        if sel_prog != "(All)":
            team_opts = sorted(idf_f["TEAMNAME"].dropna().unique().tolist())
        else:
            team_opts = sorted(idf["TEAMNAME"].dropna().unique().tolist())
        sel_team = st.selectbox("Filter by Team", options=["(All)"] + team_opts, index=0)

        if sel_team != "(All)":
            idf_f = idf_f[idf_f["TEAMNAME"] == sel_team].copy()

        hc1, hc2, hc3, hc4, hc5, hc6, hc7 = st.columns([1.6, 1.8, 1.6, 1.4, 1.2, 1.1, 1.5], gap="small")
        with hc1: st.markdown("**Program**")
        with hc2: st.markdown("**Team**")
        with hc3: st.markdown("**Vendor**")
        with hc4: st.markdown("**Application**")
        with hc5: st.markdown("**Invoice Date**")
        with hc6: st.markdown("**Renewal Date**")
        with hc7: st.markdown("**Amount / Status / Actions**")
        st.markdown("<div class='thin-sep'></div>", unsafe_allow_html=True)

        vendor_opts = vdf["VENDORNAME"].tolist() if not vdf.empty else []

        for _, row in idf_f.iterrows():
            iid   = row["INVOICEID"]
            prog  = row["PROGRAMNAME"] or ""
            team  = row["TEAMNAME"] or ""
            team_id = row["TEAMID"]
            vend  = row["VENDOR"] or ""
            app   = row["APPLICATIONID"] or ""
            invd  = row["INVOICEDATE"] or date.today()
            rend  = row["RENEWALDATE"] or date.today()
            amt   = float(row["AMOUNT"] or 0.0)
            stat  = row["STATUS"] or "Planned"

            with st.form(f"inv_{iid}", clear_on_submit=False):
                c1, c2, c3, c4, c5, c6, c7 = st.columns([1.6, 1.8, 1.6, 1.4, 1.2, 1.1, 1.5], gap="small")

                # Program/Team are immutable → show as disabled text inputs
                with c1:
                    st.text_input("Program", value=prog, key=f"ip_ro_{iid}", label_visibility="collapsed", disabled=True)
                with c2:
                    st.text_input("Team", value=team, key=f"it_ro_{iid}", label_visibility="collapsed", disabled=True)

                with c3:
                    vend_idx = vendor_opts.index(vend) if vend in vendor_opts else 0 if vendor_opts else 0
                    new_vendor = st.selectbox("Vendor", options=vendor_opts, index=vend_idx, key=f"iv_{iid}", label_visibility="collapsed")
                with c4:
                    new_app = st.text_input("Application", value=app, key=f"ia_{iid}", label_visibility="collapsed")
                with c5:
                    new_invd = st.date_input("Invoice Date", value=invd, key=f"id_{iid}", label_visibility="collapsed")
                with c6:
                    new_rend = st.date_input("Renewal Date", value=rend, key=f"rd_{iid}", label_visibility="collapsed")
                with c7:
                    r1, r2, r3 = st.columns([0.9, 1.1, 1.0], gap="small")
                    with r1:
                        new_amt = st.number_input("Amount", min_value=0.0, step=100.0, value=amt, key=f"iamt_{iid}", label_visibility="collapsed")
                    with r2:
                        new_stat = st.selectbox(
                            "Status",
                            ["Planned", "Approved", "Paid", "Cancelled"],
                            index=["Planned","Approved","Paid","Cancelled"].index(stat) if stat in ["Planned","Approved","Paid","Cancelled"] else 0,
                            key=f"istat_{iid}",
                            label_visibility="collapsed"
                        )
                    with r3:
                        u = st.form_submit_button("Update", type="primary", use_container_width=True)
                        d = st.form_submit_button("Delete", use_container_width=True)

                if u:
                    # Keep original TEAMID (immutable relationship)
                    upsert_invoice(
                        invoice_id=iid,
                        application_id=new_app.strip() if new_app else None,
                        team_id=str(team_id),
                        invoice_date=new_invd,
                        renewal_date=new_rend,
                        amount=float(new_amt or 0.0),
                        status=new_stat,
                        vendor=new_vendor,
                    )
                    st.success(f"Updated invoice for team '{team}'.")
                    invalidate_all(); st.rerun()

                if d:
                    delete_invoice(iid)
                    st.success("Deleted invoice.")
                    invalidate_all(); st.rerun()
