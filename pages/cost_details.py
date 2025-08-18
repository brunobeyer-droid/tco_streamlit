# filename: pages/cost_details.py
import streamlit as st

# If you sometimes run the page directly, uncomment the next two lines:
# import sys, os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from snowflake_db import ensure_tables as _ensure_tables, fetch_df
from utils.sidebar import render_global_actions
render_global_actions()

st.set_page_config(page_title="TCO Summary", page_icon="📊", layout="wide")
st.title("📊 TCO Summary")

# -------------------------------------------------------------------
# Ensure base tables exist ONCE per session (cheap & idempotent)
# -------------------------------------------------------------------
if not st.session_state.get("_init_cost_summary_done"):
    _ensure_tables()
    st.session_state["_init_cost_summary_done"] = True

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
               COALESCE(t.TEAMFTE,0)     AS TEAMFTE,
               COALESCE(t.COSTPERFTE,0)  AS COSTPERFTE
        FROM TEAMS t
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        ORDER BY p.PROGRAMNAME, t.TEAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_invoices_df():
    return fetch_df("""
        SELECT i.INVOICEID,
               i.TEAMID,
               t.TEAMNAME,
               p.PROGRAMID,
               p.PROGRAMNAME,
               COALESCE(i.AMOUNT,0) AS AMOUNT,
               i.INVOICEDATE,
               i.RENEWALDATE,
               i.STATUS,
               i.VENDOR
        FROM INVOICES i
        LEFT JOIN TEAMS t   ON t.TEAMID   = i.TEAMID
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        ORDER BY i.INVOICEDATE DESC;
    """)

# -------------------------------------------------------------------
# Load data
# -------------------------------------------------------------------
prog_df = get_programs_df()
teams_df = get_teams_df()
inv_df = get_invoices_df()

# Optional program filter to segment reports
program_options = ["All Programs"] + (prog_df["PROGRAMNAME"].tolist() if not prog_df.empty else [])
selected_program = st.selectbox("Filter by Program", options=program_options, index=0)

if selected_program != "All Programs":
    # Filter all frames by selected program
    if not prog_df.empty:
        prog_df = prog_df[prog_df["PROGRAMNAME"] == selected_program]
    if not teams_df.empty:
        teams_df = teams_df[teams_df["PROGRAMNAME"] == selected_program]
    if not inv_df.empty:
        inv_df = inv_df[inv_df["PROGRAMNAME"] == selected_program]

# -------------------------------------------------------------------
# Top metrics
# -------------------------------------------------------------------
num_programs = 0 if prog_df.empty else prog_df["PROGRAMID"].nunique()
num_teams    = 0 if teams_df.empty else teams_df["TEAMID"].nunique()
total_prog_fte = 0.0 if prog_df.empty else float(prog_df["PROGRAMFTE"].sum())
total_team_fte = 0.0 if teams_df.empty else float(teams_df["TEAMFTE"].sum())
total_invoices = 0.0 if inv_df.empty   else float(inv_df["AMOUNT"].sum())

m1, m2, m3, m4 = st.columns(4)
with m1: st.metric("Programs", num_programs)
with m2: st.metric("Teams", num_teams)
with m3: st.metric("Total Program FTE", f"{total_prog_fte:,.2f}")
with m4: st.metric("Total Invoices", f"{total_invoices:,.2f}")

st.divider()

# -------------------------------------------------------------------
# Program FTE table & chart
# -------------------------------------------------------------------
st.subheader("Program FTE")
if prog_df.empty:
    st.info("No programs found.")
else:
    st.dataframe(
        prog_df[["PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE"]].rename(
            columns={"PROGRAMNAME": "Program", "PROGRAMOWNER": "Owner", "PROGRAMFTE": "FTE"}
        ),
        use_container_width=True
    )
    try:
        st.bar_chart(
            prog_df.set_index("PROGRAMNAME")["PROGRAMFTE"],
            height=240
        )
    except Exception:
        pass

st.divider()

# -------------------------------------------------------------------
# Team FTE & Cost
# -------------------------------------------------------------------
st.subheader("Teams — FTE & Cost/FTE")
if teams_df.empty:
    st.info("No teams found.")
else:
    st.dataframe(
        teams_df[["PROGRAMNAME", "TEAMNAME", "TEAMFTE", "COSTPERFTE"]].rename(
            columns={"PROGRAMNAME": "Program", "TEAMNAME": "Team", "TEAMFTE": "Team FTE", "COSTPERFTE": "Cost per FTE Hour"}
        ),
        use_container_width=True
    )
    # Optional: total hourly cost proxy (TeamFTE * CostPerFTE)
    try:
        cost_proxy = (teams_df["TEAMFTE"] * teams_df["COSTPERFTE"]).sum()
        st.caption(f"Cost proxy (TeamFTE × Cost per FTE Hour): **{cost_proxy:,.2f}**")
    except Exception:
        pass

st.divider()

# -------------------------------------------------------------------
# Invoices by Program and Team
# -------------------------------------------------------------------
st.subheader("Invoices")

# By Program
if inv_df.empty:
    st.info("No invoices found.")
else:
    by_prog = inv_df.groupby("PROGRAMNAME", dropna=False)["AMOUNT"].sum().reset_index().rename(
        columns={"PROGRAMNAME": "Program", "AMOUNT": "Invoice Total"}
    ).sort_values("Invoice Total", ascending=False)

    colA, colB = st.columns(2, gap="large")
    with colA:
        st.markdown("**By Program**")
        st.dataframe(by_prog, use_container_width=True)
        try:
            st.bar_chart(by_prog.set_index("Program")["Invoice Total"], height=240)
        except Exception:
            pass

    # By Program & Team
    by_team = inv_df.groupby(["PROGRAMNAME", "TEAMNAME"], dropna=False)["AMOUNT"].sum().reset_index().rename(
        columns={"PROGRAMNAME": "Program", "TEAMNAME": "Team", "AMOUNT": "Invoice Total"}
    ).sort_values(["Program", "Invoice Total"], ascending=[True, False])

    with colB:
        st.markdown("**By Program & Team**")
        st.dataframe(by_team, use_container_width=True)

# -------------------------------------------------------------------
# Footnotes / Help
# -------------------------------------------------------------------
st.caption(
    "Data source: Snowflake (TCODB.PUBLIC). Program FTE comes from **PROGRAMS.PROGRAMFTE**, "
    "Team FTE/Cost from **TEAMS.TEAMFTE / TEAMS.COSTPERFTE**, and invoice totals from **INVOICES.AMOUNT** "
    "joined through Teams → Programs. Use the program filter above to segment reports."
)
