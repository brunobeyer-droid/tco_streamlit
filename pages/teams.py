# filename: pages/teams.py
import uuid
import streamlit as st
from snowflake_db import ensure_tables as _ensure_tables, fetch_df, upsert_team
from utils.sidebar import render_global_actions
render_global_actions()
st.set_page_config(page_title="Teams", page_icon="👥", layout="wide")
st.title("👥 Teams")

# One-time init
if not st.session_state.get("_init_teams_done"):
    _ensure_tables()
    st.session_state["_init_teams_done"] = True

# Global revision used for cache-busting across pages
st.session_state.setdefault("db_rev", 0)
REV = st.session_state["db_rev"]

# ---------------- Cache loaders (include REV so they refetch after writes) ----------------
@st.cache_data(show_spinner=False)
def get_programs_df(_rev: int):
    return fetch_df("""
        SELECT PROGRAMID, PROGRAMNAME
        FROM TCODB.PUBLIC.PROGRAMS
        ORDER BY PROGRAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_teams_df_joined(_rev: int):
    # Show '(Unknown Program)' instead of NULL so rows aren't silently filtered away
    return fetch_df("""
        SELECT
          t.TEAMID,
          t.TEAMNAME,
          t.PROGRAMID,
          COALESCE(p.PROGRAMNAME, '(Unknown Program)') AS PROGRAMNAME,
          t.TEAMFTE,
          t.COSTPERFTE
        FROM TCODB.PUBLIC.TEAMS t
        LEFT JOIN TCODB.PUBLIC.PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        ORDER BY PROGRAMNAME, t.TEAMNAME;
    """)

@st.cache_data(show_spinner=False)
def get_teams_df_raw(_rev: int):
    # Raw table without join (for debugging)
    return fetch_df("""
        SELECT TEAMID, TEAMNAME, PROGRAMID, TEAMFTE, COSTPERFTE
        FROM TCODB.PUBLIC.TEAMS
        ORDER BY TEAMNAME;
    """)

def _invalidate_all():
    get_programs_df.clear()
    get_teams_df_joined.clear()
    get_teams_df_raw.clear()
    st.session_state["db_rev"] = st.session_state.get("db_rev", 0) + 1

# ---------------- Manual refresh ----------------
if st.button("🔄 Refresh data (force reload)"):
    _invalidate_all()
    st.rerun()

# ---------------- Add Team (create only) ----------------
# Add Team (aligned)
st.subheader("Add Team")

prog_df = get_programs_df(st.session_state["db_rev"])
if prog_df.empty:
    st.warning("No programs found. Please add a Program first.")
else:
    with st.form("add_team_form", clear_on_submit=True):
        c1, c2, c3, c4, c5 = st.columns([2.4, 2, 1.2, 1.2, 1.0], gap="small")
        with c1:
            team_name_in = st.text_input("Team Name", placeholder="e.g. Core Platform", label_visibility="collapsed")
        with c2:
            prog_display = prog_df["PROGRAMNAME"].tolist()
            prog_pick = st.selectbox("Program", options=prog_display, label_visibility="collapsed")
            program_id = prog_df.set_index("PROGRAMNAME").loc[prog_pick, "PROGRAMID"]
        with c3:
            team_fte = st.number_input("Team FTE", min_value=0.0, step=0.5, value=0.0, label_visibility="collapsed")
        with c4:
            cost_per_fte = st.number_input("Cost / FTE", min_value=0.0, step=100.0, value=0.0, label_visibility="collapsed")
        with c5:
            save_team = st.form_submit_button("Save Team", type="primary", use_container_width=True)

        if save_team:
            team_name = (team_name_in or "").strip()
            if not team_name:
                st.warning("Please provide a Team Name.")
            else:
                # Duplicate check (case/trim-insensitive)
                existing = fetch_df("""
                    SELECT 1
                    FROM TCODB.PUBLIC.TEAMS
                    WHERE PROGRAMID = %s
                      AND UPPER(TRIM(TEAMNAME)) = UPPER(TRIM(%s))
                    LIMIT 1;
                """, (str(program_id), team_name))
                if not existing.empty:
                    st.warning(f"Team '{team_name}' already exists in program '{prog_pick}'.")
                else:
                    upsert_team(
                        team_id=str(uuid.uuid4()),
                        team_name=team_name,
                        program_id=str(program_id),
                        cost_per_fte=float(cost_per_fte or 0.0),
                        team_fte=float(team_fte or 0.0),
                    )
                    st.success(f"Team '{team_name}' added to program '{prog_pick}'.")
                    _invalidate_all()
                    st.rerun()


st.divider()

# ---------------- Filters + List ----------------
st.subheader("Teams")

df = get_teams_df_joined(st.session_state["db_rev"])

with st.expander("Filters", expanded=True):
    col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
    with col1:
        programs = sorted(df["PROGRAMNAME"].dropna().unique().tolist()) if not df.empty else []
        # By default select ALL visible programs (including '(Unknown Program)')
        sel_programs = st.multiselect("Program(s)", options=programs, default=programs)
    with col2:
        text = st.text_input("Team name contains", placeholder="e.g. Core, Payments...")
    with col3:
        fte_filter = st.selectbox("FTE filter", ["All", "> 0", "= 0"])
    with col4:
        show_unknown = st.checkbox("Show '(Unknown Program)' rows", value=True,
                                   help="These indicate teams whose PROGRAMID doesn't match any PROGRAMS row.")

fdf = df.copy()
if not fdf.empty:
    if not show_unknown:
        fdf = fdf[fdf["PROGRAMNAME"] != "(Unknown Program)"]
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
    })[["Program", "Team", "FTE"]]
    st.dataframe(show, use_container_width=True, hide_index=True)


