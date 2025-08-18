# filename: pages/edit.py
import streamlit as st
import pandas as pd
from datetime import date
from utils.sidebar import render_global_actions
render_global_actions()
from snowflake_db import (
    ensure_tables as _ensure_tables,
    fetch_df,
    execute,  # raw SQL helper
    upsert_program, delete_program,
    upsert_team,    delete_team,
    upsert_vendor,  delete_vendor,
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

# Global rev for cache-busting when we refresh
st.session_state.setdefault("db_rev", 0)
REV = st.session_state["db_rev"]

# Pending delete confirmation (two-step safety)
st.session_state.setdefault("pending_delete", None)

# ---------- Cached loaders (keyed by REV) ----------
@st.cache_data(show_spinner=False)
def get_programs_df(_rev: int):
    return fetch_df("""
        SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE
        FROM TCODB.PUBLIC.PROGRAMS
        ORDER BY PROGRAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_teams_df(_rev: int):
    return fetch_df("""
        SELECT
          t.TEAMID,
          t.TEAMNAME,
          t.PROGRAMID,
          COALESCE(p.PROGRAMNAME,'(Unknown Program)') AS PROGRAMNAME,
          t.TEAMFTE,
          t.COSTPERFTE
        FROM TCODB.PUBLIC.TEAMS t
        LEFT JOIN TCODB.PUBLIC.PROGRAMS p
          ON p.PROGRAMID = t.PROGRAMID
        ORDER BY PROGRAMNAME, t.TEAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_vendors_df(_rev: int):
    return fetch_df("""
        SELECT VENDORID, VENDORNAME
        FROM TCODB.PUBLIC.VENDORS
        ORDER BY VENDORNAME;
    """)

@st.cache_data(show_spinner=False)
def get_invoices_df(_rev: int):
    return fetch_df("""
        SELECT
          i.INVOICEID,
          i.APPLICATIONID,
          a.APPLICATIONNAME,
          i.VENDORID,
          v.VENDORNAME,
          i.TEAMID,
          t.TEAMNAME,
          t.PROGRAMID,
          p.PROGRAMNAME,
          i.INVOICEDATE,
          i.RENEWALDATE,
          i.AMOUNT,
          i.STATUS
        FROM TCODB.PUBLIC.INVOICES i
        LEFT JOIN TCODB.PUBLIC.APPLICATIONS a ON a.APPLICATIONID = i.APPLICATIONID
        LEFT JOIN TCODB.PUBLIC.VENDORS v     ON v.VENDORID      = i.VENDORID
        LEFT JOIN TCODB.PUBLIC.TEAMS t       ON t.TEAMID        = i.TEAMID
        LEFT JOIN TCODB.PUBLIC.PROGRAMS p    ON p.PROGRAMID     = t.PROGRAMID
        ORDER BY i.RENEWALDATE DESC NULLS LAST, i.INVOICEDATE DESC NULLS LAST, p.PROGRAMNAME, t.TEAMNAME;
    """)

def _invalidate_all():
    get_programs_df.clear(); get_teams_df.clear()
    get_vendors_df.clear();  get_invoices_df.clear()
    st.session_state["db_rev"] = st.session_state.get("db_rev", 0) + 1

# ---------- Helpers: Cascading deletes ----------
def _cascade_delete_program(program_id: str, program_name: str):
    # collect team ids
    teams = fetch_df("SELECT TEAMID FROM TCODB.PUBLIC.TEAMS WHERE PROGRAMID = %s;", (program_id,))
    team_ids = teams["TEAMID"].tolist() if not teams.empty else []

    # collect application ids referenced by any invoice of those teams
    if team_ids:
        app_ids_df = fetch_df(
            f"""
            SELECT DISTINCT APPLICATIONID
            FROM TCODB.PUBLIC.INVOICES
            WHERE TEAMID IN ({",".join(["%s"]*len(team_ids))}) AND APPLICATIONID IS NOT NULL
            """,
            tuple(team_ids),
        )
        app_ids = app_ids_df["APPLICATIONID"].tolist() if not app_ids_df.empty else []
    else:
        app_ids = []

    # delete invoices of teams
    if team_ids:
        execute(
            f"DELETE FROM TCODB.PUBLIC.INVOICES WHERE TEAMID IN ({','.join(['%s']*len(team_ids))});",
            tuple(team_ids),
        )
    # delete apps used only by those invoices (now invoices are gone, we can purge those app ids we collected)
    if app_ids:
        execute(
            f"DELETE FROM TCODB.PUBLIC.APPLICATIONS WHERE APPLICATIONID IN ({','.join(['%s']*len(app_ids))});",
            tuple(app_ids),
        )
    # delete teams
    execute("DELETE FROM TCODB.PUBLIC.TEAMS WHERE PROGRAMID = %s;", (program_id,))
    # delete program
    delete_program(program_id)

def _cascade_delete_team(team_id: str, team_name: str):
    # collect application ids referenced by this team's invoices
    app_ids_df = fetch_df(
        "SELECT DISTINCT APPLICATIONID FROM TCODB.PUBLIC.INVOICES WHERE TEAMID = %s AND APPLICATIONID IS NOT NULL;",
        (team_id,),
    )
    app_ids = app_ids_df["APPLICATIONID"].tolist() if not app_ids_df.empty else []

    # delete invoices for team
    execute("DELETE FROM TCODB.PUBLIC.INVOICES WHERE TEAMID = %s;", (team_id,))
    # delete apps referenced by those invoices
    if app_ids:
        execute(
            f"DELETE FROM TCODB.PUBLIC.APPLICATIONS WHERE APPLICATIONID IN ({','.join(['%s']*len(app_ids))});",
            tuple(app_ids),
        )
    # delete team
    delete_team(team_id)

def _cascade_delete_vendor(vendor_id: str, vendor_name: str):
    # delete invoices by vendor
    execute("DELETE FROM TCODB.PUBLIC.INVOICES WHERE VENDORID = %s;", (vendor_id,))
    # delete apps by vendor
    execute("DELETE FROM TCODB.PUBLIC.APPLICATIONS WHERE VENDORID = %s;", (vendor_id,))
    # delete vendor
    delete_vendor(vendor_id)

# ---------- Load data ----------
pdf = get_programs_df(REV)
tdf = get_teams_df(REV)
vdf = get_vendors_df(REV)
idf = get_invoices_df(REV)

# Normalize dates for invoices
for col in ["INVOICEDATE", "RENEWALDATE"]:
    if col in idf.columns:
        idf[col] = pd.to_datetime(idf[col], errors="coerce").dt.date

# ---------- Global confirmation banner ----------
if st.session_state["pending_delete"]:
    p = st.session_state["pending_delete"]
    with st.container(border=True):
        st.error(p["message"])
        c1, c2 = st.columns([1,1])
        with c1:
            if st.button("✅ Confirm delete"):
                kind = p["kind"]
                if kind == "program":
                    _cascade_delete_program(p["id"], p["name"])
                    st.success(f"Program '{p['name']}' and all related data were deleted.")
                elif kind == "team":
                    _cascade_delete_team(p["id"], p["name"])
                    st.success(f"Team '{p['name']}' and related invoices/applications were deleted.")
                elif kind == "vendor":
                    _cascade_delete_vendor(p["id"], p["name"])
                    st.success(f"Vendor '{p['name']}', its applications and invoices were deleted.")
                elif kind == "invoice":
                    delete_invoice(p["id"])
                    st.success("Invoice deleted.")
                st.session_state["pending_delete"] = None
                _invalidate_all(); st.rerun()
        with c2:
            if st.button("❌ Cancel"):
                st.session_state["pending_delete"] = None
                st.info("Delete cancelled.")

st.markdown("---")

# ---------- Tabs ----------
tab_prog, tab_team, tab_vendor, tab_invoice = st.tabs(["Programs", "Teams", "Vendors", "Invoices"])

# =========================================================
# Programs (Name immutable; edit Owner/FTE; cascade delete)
# =========================================================
with tab_prog:
    st.subheader("Programs")
    if pdf.empty:
        st.info("No programs found.")
    else:
        hc1, hc2, hc3, hc4 = st.columns([2.6, 2, 1.1, 1.6], gap="small")
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
                c1, c2, c3, c4 = st.columns([2.6, 2, 1.1, 1.6], gap="small")
                with c1:
                    st.text_input("Program Name", value=pname, key=f"pn_ro_{pid}",
                                  label_visibility="collapsed", disabled=True)
                with c2:
                    new_owner = st.text_input("Owner", value=pown, key=f"po_{pid}", label_visibility="collapsed")
                with c3:
                    new_fte = st.number_input("FTE", min_value=0.0, step=0.5, value=pfte,
                                              key=f"pf_{pid}", label_visibility="collapsed")
                with c4:
                    r_u, r_d = st.columns([1, 1], gap="small")
                    with r_u:
                        u = st.form_submit_button("Update", type="primary", use_container_width=True)
                    with r_d:
                        d = st.form_submit_button("Delete", use_container_width=True)

                if u:
                    upsert_program(pid, pname, new_owner.strip() if new_owner else None, float(new_fte or 0.0))
                    st.success(f"Updated program '{pname}'.")
                    _invalidate_all(); st.rerun()

                if d:
                    st.session_state["pending_delete"] = {
                        "kind": "program",
                        "id": pid,
                        "name": pname,
                        "message": f"⚠️ Deleting program **{pname}** will also delete **all teams and invoices** under it, and any **applications referenced by those invoices**. This cannot be undone."
                    }
                    st.rerun()

# =========================================================
# Teams (Program & Team Name immutable; cascade delete)
# =========================================================
with tab_team:
    st.subheader("Teams")

    if tdf.empty or pdf.empty:
        st.info("No teams/programs found.")
    else:
        prog_opts = pdf["PROGRAMNAME"].tolist()
        sel_prog = st.selectbox("Filter by Program", options=prog_opts, index=0 if prog_opts else None)
        sel_prog_id = pdf.set_index("PROGRAMNAME").loc[sel_prog, "PROGRAMID"] if sel_prog else None
        fdf = tdf[tdf["PROGRAMID"] == sel_prog_id] if sel_prog_id else tdf.copy()

        hc1, hc2, hc3, hc4, hc5 = st.columns([2.4, 2, 1.1, 1.1, 1.6], gap="small")
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
                c1, c2, c3, c4, c5 = st.columns([2.4, 2, 1.1, 1.1, 1.6], gap="small")
                with c1:
                    st.text_input("Team", value=tname, key=f"tn_ro_{tid}",
                                  label_visibility="collapsed", disabled=True)
                with c2:
                    st.text_input("Program", value=prog_name, key=f"tp_ro_{tid}",
                                  label_visibility="collapsed", disabled=True)
                with c3:
                    new_fte = st.number_input("FTE", min_value=0.0, step=0.5, value=tpfte,
                                              key=f"tf_{tid}", label_visibility="collapsed")
                with c4:
                    new_cost = st.number_input("Cost / FTE", min_value=0.0, step=100.0, value=tcost,
                                               key=f"tc_{tid}", label_visibility="collapsed")
                with c5:
                    r_u, r_d = st.columns([1, 1], gap="small")
                    with r_u:
                        u = st.form_submit_button("Update", type="primary", use_container_width=True)
                    with r_d:
                        d = st.form_submit_button("Delete", use_container_width=True)

                if u:
                    upsert_team(tid, tname, row["PROGRAMID"], float(new_cost or 0.0), float(new_fte or 0.0))
                    st.success(f"Updated team '{tname}'.")
                    _invalidate_all(); st.rerun()

                if d:
                    st.session_state["pending_delete"] = {
                        "kind": "team",
                        "id": tid,
                        "name": tname,
                        "message": f"⚠️ Deleting team **{tname}** will also delete **all invoices** for this team and any **applications referenced by those invoices**. This cannot be undone."
                    }
                    st.rerun()

# =========================================================
# Vendors (rename; cascade delete on remove)
# =========================================================
with tab_vendor:
    st.subheader("Vendors")

    if vdf.empty:
        st.info("No vendors found.")
    else:
        hc1, hc2 = st.columns([3, 1.6], gap="small")
        with hc1: st.markdown("**Vendor**")
        with hc2: st.markdown("**Actions**")
        st.markdown("<div class='thin-sep'></div>", unsafe_allow_html=True)

        for _, row in vdf.iterrows():
            vid = row["VENDORID"]
            vnm = row["VENDORNAME"] or ""
            with st.form(f"vendor_{vid}", clear_on_submit=False):
                c1, c2 = st.columns([3, 1.6], gap="small")
                with c1:
                    new_vnm = st.text_input("Vendor", value=vnm, key=f"vn_{vid}", label_visibility="collapsed")
                with c2:
                    r_u, r_d = st.columns([1, 1], gap="small")
                    with r_u:
                        u = st.form_submit_button("Update", type="primary", use_container_width=True)
                    with r_d:
                        d = st.form_submit_button("Delete", use_container_width=True)

                if u:
                    upsert_vendor(vid, new_vnm.strip())
                    st.success(f"Updated vendor '{new_vnm.strip()}'.")
                    _invalidate_all(); st.rerun()

                if d:
                    st.session_state["pending_delete"] = {
                        "kind": "vendor",
                        "id": vid,
                        "name": vnm,
                        "message": f"⚠️ Deleting vendor **{vnm}** will also delete **all applications for this vendor** and **all invoices tied to this vendor**. This cannot be undone."
                    }
                    st.rerun()

# =========================================================
# Invoices (relationships immutable; edit only fields)
# =========================================================
with tab_invoice:
    st.subheader("Invoices")

    if idf.empty:
        st.info("No invoices found.")
    else:
        # Filters: Program -> Team
        prog_opts = ["(All)"] + sorted(idf["PROGRAMNAME"].dropna().unique().tolist())
        f_prog = st.selectbox("Filter by Program", options=prog_opts, index=0)
        inv_f = idf.copy()
        team_opts = ["(All)"]

        if f_prog != "(All)":
            inv_f = inv_f[inv_f["PROGRAMNAME"] == f_prog].copy()
            team_opts += sorted(inv_f["TEAMNAME"].dropna().unique().tolist())
        else:
            team_opts += sorted(idf["TEAMNAME"].dropna().unique().tolist())

        f_team = st.selectbox("Filter by Team", options=team_opts, index=0)
        if f_team != "(All)":
            inv_f = inv_f[inv_f["TEAMNAME"] == f_team].copy()

        # Header row
        hc1, hc2, hc3, hc4, hc5, hc6, hc7 = st.columns([1.6, 1.8, 1.6, 1.6, 1.2, 1.1, 1.5], gap="small")
        with hc1: st.markdown("**Program**")
        with hc2: st.markdown("**Team**")
        with hc3: st.markdown("**Vendor**")
        with hc4: st.markdown("**Application**")
        with hc5: st.markdown("**Invoice Date**")
        with hc6: st.markdown("**Renewal Date**")
        with hc7: st.markdown("**Amount / Status / Actions**")
        st.markdown("<div class='thin-sep'></div>", unsafe_allow_html=True)

        for _, row in inv_f.iterrows():
            iid   = row["INVOICEID"]
            prog  = row["PROGRAMNAME"] or ""
            team  = row["TEAMNAME"] or ""
            vend  = row["VENDORNAME"] or ""
            app   = row["APPLICATIONNAME"] or ""
            invd  = row["INVOICEDATE"] or date.today()
            rend  = row["RENEWALDATE"] or date.today()
            amt   = float(row["AMOUNT"] or 0.0)
            stat  = row["STATUS"] or "Planned"

            with st.form(f"inv_{iid}", clear_on_submit=False):
                c1, c2, c3, c4, c5, c6, c7 = st.columns([1.6, 1.8, 1.6, 1.6, 1.2, 1.1, 1.5], gap="small")

                # Immutable relationships (read-only)
                with c1:
                    st.text_input("Program", value=prog, key=f"ip_ro_{iid}", label_visibility="collapsed", disabled=True)
                with c2:
                    st.text_input("Team", value=team, key=f"it_ro_{iid}", label_visibility="collapsed", disabled=True)
                with c3:
                    st.text_input("Vendor", value=vend, key=f"iv_ro_{iid}", label_visibility="collapsed", disabled=True)
                with c4:
                    st.text_input("Application", value=app, key=f"ia_ro_{iid}", label_visibility="collapsed", disabled=True)

                # Editable fields
                with c5:
                    e_invdate = st.date_input("Invoice Date", value=invd, key=f"id_{iid}", label_visibility="collapsed")
                with c6:
                    e_rendate = st.date_input("Renewal Date", value=rend, key=f"rd_{iid}", label_visibility="collapsed")
                with c7:
                    r1, r2, r3 = st.columns([0.9, 1.1, 1.0], gap="small")
                    with r1:
                        e_amount = st.number_input("Amount", min_value=0.0, step=100.0, value=amt, key=f"iamt_{iid}", label_visibility="collapsed")
                    with r2:
                        e_status = st.selectbox(
                            "Status",
                            ["Planned", "Approved", "Paid", "Cancelled"],
                            index=["Planned","Approved","Paid","Cancelled"].index(stat) if stat in ["Planned","Approved","Paid","Cancelled"] else 0,
                            key=f"istat_{iid}", label_visibility="collapsed"
                        )
                    with r3:
                        u = st.form_submit_button("Update", type="primary", use_container_width=True)
                        d = st.form_submit_button("Delete", use_container_width=True)

                if u:
                    upsert_invoice(
                        invoice_id=iid,
                        application_id=row["APPLICATIONID"],  # immutable
                        vendor_id=row["VENDORID"],            # immutable
                        team_id=row["TEAMID"],                # immutable
                        invoice_date=e_invdate,
                        renewal_date=e_rendate,
                        amount=float(e_amount or 0.0),
                        status=e_status,
                    )
                    st.success("Invoice updated.")
                    _invalidate_all(); st.rerun()

                if d:
                    st.session_state["pending_delete"] = {
                        "kind": "invoice",
                        "id": iid,
                        "name": f"{app or '(No App)'} / {vend or '(No Vendor)'}",
                        "message": "⚠️ Deleting this invoice cannot be undone."
                    }
                    st.rerun()
