import streamlit as st
import uuid
from snowflake_db import ensure_tables as _ensure_tables, fetch_df, upsert_team

st.set_page_config(page_title="Teams", page_icon="👥", layout="wide")
st.title("👥 Teams")

# Init once
if not st.session_state.get("_init_teams_done"):
    _ensure_tables()
    st.session_state["_init_teams_done"] = True

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

def invalidate_caches():
    get_programs_df.clear()
    get_teams_df.clear()

# ---------------- Add Team (create only) ----------------
st.subheader("Add Team")

prog_df = get_programs_df()
if prog_df.empty:
    st.warning("No programs found. Please add a Program first.")
else:
    c1, c2, c3, c4 = st.columns([2.4, 2, 1.2, 1.2], gap="small")
    with c1:
        team_name = st.text_input("Team Name")
    with c2:
        prog_display = prog_df["PROGRAMNAME"].tolist()
        prog_pick = st.selectbox("Program", options=prog_display)
        program_id = prog_df.set_index("PROGRAMNAME").loc[prog_pick, "PROGRAMID"]
    with c3:
        team_fte = st.number_input("Team FTE", min_value=0.0, step=0.5, value=0.0)
    with c4:
        cost_per_fte = st.number_input("Cost / FTE", min_value=0.0, step=100.0, value=0.0)

    if st.button("Save Team", type="primary"):
        if not team_name.strip():
            st.warning("Please provide a Team Name.")
        else:
            upsert_team(
                team_id=str(uuid.uuid4()),
                team_name=team_name.strip(),
                program_id=str(program_id),
                cost_per_fte=float(cost_per_fte or 0.0),
                team_fte=float(team_fte or 0.0),
            )
            st.success(f"Team '{team_name.strip()}' added to program '{prog_pick}'.")
            invalidate_caches()
            st.rerun()

st.divider()

# ---------------- Filters + List ----------------
st.subheader("Teams")

df = get_teams_df()

with st.expander("Filters", expanded=True):
    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        programs = sorted(df["PROGRAMNAME"].dropna().unique().tolist()) if not df.empty else []
        sel_programs = st.multiselect("Program(s)", options=programs, default=programs)
    with col2:
        text = st.text_input("Team name contains", placeholder="e.g. Core, Payments...")
    with col3:
        fte_filter = st.selectbox("FTE filter", ["All", "> 0", "= 0"])

fdf = df.copy()
if not fdf.empty:
    if sel_programs:
        fdf = fdf[fdf["PROGRAMNAME"].isin(sel_programs)]
    if text:
        fdf = fdf[fdf["TEAMNAME"].str.contains(text, case=False, na=False)]
    if fte_filter == "> 0":
        fdf = fdf[(fdf["TEAMFTE"].fillna(0) > 0)]
    elif fte_filter == "= 0":
        fdf = fdf[(fdf["TEAMFTE"].fillna(0) == 0)]

m1, m2, m3 = st.columns(3)
with m1: st.metric("Teams", int(fdf.shape[0]) if not fdf.empty else 0)
with m2: st.metric("Programs", fdf["PROGRAMNAME"].nunique() if not fdf.empty else 0)
with m3: st.metric("Total FTE", f"{float(fdf['TEAMFTE'].fillna(0).sum()) if not fdf.empty else 0.0:,.2f}")

st.divider()

if fdf.empty:
    st.info("No teams found for the selected filters.")
else:
    show = fdf.rename(columns={
        "PROGRAMNAME": "Program",
        "TEAMNAME": "Team",
        "TEAMFTE": "FTE",
        "COSTPERFTE": "Cost / FTE",
    })[["Program", "Team", "FTE", "Cost / FTE"]]
    st.dataframe(show, use_container_width=True)
