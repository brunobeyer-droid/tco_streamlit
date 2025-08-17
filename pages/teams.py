# filename: pages/teams.py
import streamlit as st
import uuid
from typing import Dict

# If you sometimes run the page directly, uncomment:
# import sys, os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from snowflake_db import ensure_tables, fetch_df, upsert_team, delete_team

st.set_page_config(page_title="Teams", page_icon="👥", layout="wide")
st.title("👥 Team Composition")

# -----------------------------------------------------------------------------
# Initialize (ensure tables once per session)
# -----------------------------------------------------------------------------
if not st.session_state.get("_init_teams_done"):
    ensure_tables()
    st.session_state["_init_teams_done"] = True

# Session-only MSP flags (since schema doesn't have an MSP column)
# key by TEAMID -> bool
st.session_state.setdefault("msp_flags", {})

# -----------------------------------------------------------------------------
# Cached readers
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def get_programs_df():
    return fetch_df("SELECT PROGRAMID, PROGRAMNAME FROM PROGRAMS ORDER BY PROGRAMNAME;")

@st.cache_data(show_spinner=False)
def get_teams_df():
    return fetch_df("""
        SELECT t.TEAMID, t.TEAMNAME,
               p.PROGRAMID, p.PROGRAMNAME,
               COALESCE(t.COSTPERFTE,0) AS COSTPERFTE,
               COALESCE(t.TEAMFTE,0)    AS TEAMFTE
        FROM TEAMS t
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        ORDER BY p.PROGRAMNAME, t.TEAMNAME;
    """)

def invalidate_programs_cache():
    get_programs_df.clear()

def invalidate_teams_cache():
    get_teams_df.clear()

# -----------------------------------------------------------------------------
# Load references
# -----------------------------------------------------------------------------
prog_df = get_programs_df()
name_to_program_id: Dict[str, str] = dict(zip(prog_df["PROGRAMNAME"], prog_df["PROGRAMID"])) if not prog_df.empty else {}
program_names = list(name_to_program_id.keys())

if not program_names:
    st.warning("No Programs found. Please create Programs first.")
    st.stop()

# -----------------------------------------------------------------------------
# Add Team (form)
# -----------------------------------------------------------------------------
st.subheader("Add Team")

a1, a2, a3, a4 = st.columns([2.2, 2, 1.2, 1.2])
with a1:
    new_team = st.text_input("Team Name")
with a2:
    new_prog = st.selectbox("Program", options=program_names, index=0)
with a3:
    new_cost = st.number_input("Cost per FTE Hour", min_value=0.0, step=10.0, value=0.0)
with a4:
    mode = st.radio("FTE Input Mode", options=["Single number", "By role breakdown"], horizontal=True, index=0)

if mode == "Single number":
    team_fte = st.number_input("Team FTE", min_value=0.0, step=0.5, value=0.0, key="new_team_fte_single")
else:
    st.markdown("**Workforce (FTE) by role** — we’ll sum these and save as Team FTE.")
    r1, r2, r3, r4, r5 = st.columns(5)
    with r1:
        eng = st.number_input("Engineers", min_value=0.0, step=0.5, value=0.0, key="role_eng")
    with r2:
        pm = st.number_input("Product Managers", min_value=0.0, step=0.5, value=0.0, key="role_pm")
    with r3:
        des = st.number_input("Designers", min_value=0.0, step=0.5, value=0.0, key="role_des")
    with r4:
        qa = st.number_input("QA Engineers", min_value=0.0, step=0.5, value=0.0, key="role_qa")
    with r5:
        devops = st.number_input("DevOps Engineers", min_value=0.0, step=0.5, value=0.0, key="role_devops")
    team_fte = float(eng + pm + des + qa + devops)

# Optional MSP flag (session-only)
msp_flag = st.checkbox("Is this an MSP team? (session-only flag)")

save_btn = st.button("Save Team", type="primary")
if save_btn:
    if not new_team.strip():
        st.warning("Please provide a Team Name.")
    else:
        tid = str(uuid.uuid4())
        upsert_team(
            team_id=tid,
            team_name=new_team.strip(),
            program_id=name_to_program_id[new_prog],
            cost_per_fte=float(new_cost or 0.0),
            team_fte=float(team_fte or 0.0),
        )
        # store MSP flag in session
        st.session_state["msp_flags"][tid] = bool(msp_flag)

        invalidate_teams_cache()
        st.success(f"Saved team '{new_team}' under program '{new_prog}'.")
        st.rerun()

st.divider()

# -----------------------------------------------------------------------------
# Current Teams (aligned grid)
# -----------------------------------------------------------------------------
st.subheader("Current Teams")

teams_df = get_teams_df()
if teams_df.empty:
    st.info("No teams yet. Add a team above.")
else:
    COLS = [2.2, 2, 1.2, 1.0, 0.9, 0.9, 1.0]  # Team | Program | Cost/FTE | Team FTE | MSP | Update | Delete
    h1, h2, h3, h4, h5, h6, h7 = st.columns(COLS, gap="small")
    with h1: st.markdown("**Team Name**")
    with h2: st.markdown("**Program**")
    with h3: st.markdown("**Cost/FTE**")
    with h4: st.markdown("**Team FTE**")
    with h5: st.markdown("**MSP (session)**")
    with h6: st.markdown("** **")
    with h7: st.markdown("** **")
    st.divider()

    for _, row in teams_df.iterrows():
        tid         = row["TEAMID"]
        tname       = row["TEAMNAME"]
        prog_name   = row["PROGRAMNAME"]
        cost_fte    = float(row["COSTPERFTE"] or 0.0)
        team_fte    = float(row["TEAMFTE"] or 0.0)
        msp_current = bool(st.session_state["msp_flags"].get(tid, False))

        with st.form(f"row_{tid}", clear_on_submit=False):
            c1, c2, c3, c4, c5, c6, c7 = st.columns(COLS, gap="small")
            with c1:
                e_name = st.text_input("Team Name", value=tname, key=f"name_{tid}", label_visibility="hidden")
            with c2:
                idx = program_names.index(prog_name) if prog_name in program_names else 0
                e_prog = st.selectbox("Program", options=program_names, index=idx, key=f"prog_{tid}", label_visibility="hidden")
            with c3:
                e_cost = st.number_input("Cost/FTE", min_value=0.0, step=10.0, value=cost_fte, key=f"cost_{tid}", label_visibility="hidden")
            with c4:
                e_fte = st.number_input("Team FTE", min_value=0.0, step=0.5, value=team_fte, key=f"fte_{tid}", label_visibility="hidden")
            with c5:
                e_msp = st.checkbox("MSP", value=msp_current, key=f"msp_{tid}", label_visibility="hidden")
            with c6:
                update_clicked = st.form_submit_button("Update", type="primary", use_container_width=True)
            with c7:
                delete_clicked = st.form_submit_button("Delete", use_container_width=True)

            if update_clicked:
                upsert_team(
                    team_id=tid,
                    team_name=e_name.strip(),
                    program_id=name_to_program_id[e_prog],
                    cost_per_fte=float(e_cost or 0.0),
                    team_fte=float(e_fte or 0.0),
                )
                # update MSP session flag
                st.session_state["msp_flags"][tid] = bool(e_msp)

                invalidate_teams_cache()
                st.success(f"Updated team '{e_name}'.")
                st.rerun()

            if delete_clicked:
                delete_team(tid)
                st.session_state["msp_flags"].pop(tid, None)
                invalidate_teams_cache()
                st.success(f"Deleted team '{tname}'.")
                st.rerun()

    with st.expander("View Teams table (live)"):
        st.dataframe(get_teams_df(), use_container_width=True)

st.caption(
    "Team changes are saved to Snowflake only when you click **Save**, **Update**, or **Delete**. "
    "MSP is a session-only flag (not persisted). If you want MSP in the database, we can add a `MSP BOOLEAN` "
    "column to the TEAMS table and wire it through."
)
