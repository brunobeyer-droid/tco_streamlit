# filename: pages/summary_edit.py
import streamlit as st

# If you ever run the page directly:
# import sys, os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from snowflake_db import ensure_tables as _ensure_tables, fetch_df


st.set_page_config(page_title="Summary & Quick Edit Links", page_icon="🧭", layout="wide")
st.title("🧭 Summary & Quick Edit Links")

# -------------------------------------------------------------------
# Initialize (only once per session)
# -------------------------------------------------------------------
if not st.session_state.get("_init_summary_done"):
    _ensure_tables()
    st.session_state["_init_summary_done"] = True

# -------------------------------------------------------------------
# Cached readers
# -------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def get_programs_df():
    return fetch_df("""
        SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, COALESCE(PROGRAMFTE,0) AS PROGRAMFTE
        FROM PROGRAMS
        ORDER BY PROGRAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_teams_df():
    return fetch_df("""
        SELECT t.TEAMID, t.TEAMNAME,
               p.PROGRAMID, p.PROGRAMNAME,
               COALESCE(t.TEAMFTE,0)    AS TEAMFTE,
               COALESCE(t.COSTPERFTE,0) AS COSTPERFTE
        FROM TEAMS t
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        ORDER BY p.PROGRAMNAME, t.TEAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_invoices_df():
    return fetch_df("""
        SELECT i.INVOICEID,
               t.TEAMNAME,
               p.PROGRAMNAME,
               COALESCE(i.AMOUNT,0) AS AMOUNT,
               i.INVOICEDATE,
               i.RENEWALDATE,
               i.STATUS,
               i.VENDOR
        FROM INVOICES i
        LEFT JOIN TEAMS t ON t.TEAMID = i.TEAMID
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        ORDER BY i.INVOICEDATE DESC;
    """)

# -------------------------------------------------------------------
# Load data
# -------------------------------------------------------------------
prog_df = get_programs_df()
teams_df = get_teams_df()
inv_df   = get_invoices_df()

# -------------------------------------------------------------------
# Top tiles
# -------------------------------------------------------------------
p_count = 0 if prog_df.empty else prog_df["PROGRAMID"].nunique()
t_count = 0 if teams_df.empty else teams_df["TEAMID"].nunique()
i_total = 0.0 if inv_df.empty   else float(inv_df["AMOUNT"].sum())
fte_total = 0.0 if prog_df.empty else float(prog_df["PROGRAMFTE"].sum())

c1, c2, c3, c4 = st.columns(4)
with c1: st.metric("Programs", p_count)
with c2: st.metric("Teams", t_count)
with c3: st.metric("Total Program FTE", f"{fte_total:,.2f}")
with c4: st.metric("Total Invoices", f"{i_total:,.2f}")

st.divider()

# -------------------------------------------------------------------
# Programs Overview
# -------------------------------------------------------------------
st.subheader("Programs Overview")
if prog_df.empty:
    st.info("No programs defined yet. Go to the **Programs** page to add programs.")
else:
    st.dataframe(
        prog_df[["PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE"]].rename(
            columns={"PROGRAMNAME": "Program", "PROGRAMOWNER": "Owner", "PROGRAMFTE": "FTE"}
        ),
        use_container_width=True
    )

st.divider()

# -------------------------------------------------------------------
# Teams Overview
# -------------------------------------------------------------------
st.subheader("Teams Overview")
if teams_df.empty:
    st.info("No teams defined yet. Go to the **Teams** page to add teams.")
else:
    st.dataframe(
        teams_df[["PROGRAMNAME", "TEAMNAME", "TEAMFTE", "COSTPERFTE"]].rename(
            columns={
                "PROGRAMNAME": "Program",
                "TEAMNAME": "Team",
                "TEAMFTE": "Team FTE",
                "COSTPERFTE": "Cost per FTE Hour"
            }
        ),
        use_container_width=True
    )

st.divider()

# -------------------------------------------------------------------
# Cost Details Overview
# -------------------------------------------------------------------
st.subheader("Cost Details Overview")
if inv_df.empty:
    st.info("No invoices recorded yet. Use the **Invoices / Licenses** page to add them.")
else:
    by_prog = inv_df.groupby("PROGRAMNAME", dropna=False)["AMOUNT"].sum().reset_index().rename(
        columns={"PROGRAMNAME": "Program", "AMOUNT": "Invoice Total"}
    ).sort_values("Invoice Total", ascending=False)
    st.dataframe(by_prog, use_container_width=True)

st.divider()

# -------------------------------------------------------------------
# Quick Links to Edit Pages
# -------------------------------------------------------------------
st.subheader("Quick Links to Edit Pages")

# Prefer st.page_link (Streamlit ≥ 1.32). If unavailable, show plain text.
try:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.page_link("tco_app.py", label="🏢 Programs", icon="🏢")
    with col2:
        st.page_link("pages/teams.py", label="👥 Teams", icon="👥")
    with col3:
        st.page_link("pages/invoice_tracking.py", label="🧾 Invoices / Licenses", icon="🧾")
    with col4:
        st.page_link("pages/cost_details.py", label="📊 TCO Summary", icon="📊")
except Exception:
    st.write("- 🏢 **Programs**: open the *Programs* page in the sidebar")
    st.write("- 👥 **Teams**: open the *Teams* page in the sidebar")
    st.write("- 🧾 **Invoices / Licenses**: open the *Invoices / Licenses* page in the sidebar")
    st.write("- 📊 **TCO Summary**: open the *TCO Summary* page in the sidebar")

st.caption("This page provides a read-only snapshot from Snowflake. Use the links above to add or edit records.")
