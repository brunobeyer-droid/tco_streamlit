# filename: pages/programs.py
import streamlit as st
import uuid
from typing import Dict
from snowflake.connector.errors import ProgrammingError
from utils.sidebar import render_global_actions
from snowflake_db import (
    ensure_tables as _ensure_tables,
    fetch_df,
    upsert_program,
    execute,
)
render_global_actions()
st.set_page_config(page_title="Programs", page_icon="🏢", layout="wide")
st.title("🏢 Programs")

# ---------- Init once ----------
if not st.session_state.get("_init_programs_done"):
    _ensure_tables()  # create/repair schema
    st.session_state["_init_programs_done"] = True

st.session_state.setdefault("programs", [])

# ---------- Data access ----------
@st.cache_data(show_spinner=False)
def _select_programs():
    return fetch_df("""
        SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE
        FROM TCODB.PUBLIC.PROGRAMS
        ORDER BY PROGRAMNAME;
    """)

def get_programs_df():
    try:
        return _select_programs()
    except ProgrammingError as e:
        msg = str(e).lower()
        if "invalid identifier" in msg and "programfte" in msg:
            execute(
                "ALTER TABLE IF EXISTS TCODB.PUBLIC.PROGRAMS "
                "ADD COLUMN IF NOT EXISTS PROGRAMFTE NUMBER(18,2) DEFAULT 0;"
            )
            _select_programs.clear()
            return _select_programs()
        raise

def invalidate_programs_cache():
    _select_programs.clear()

def _sync_program_names(df):
    names = df["PROGRAMNAME"].tolist() if not df.empty else []
    for n in names:
        if n not in st.session_state["programs"]:
            st.session_state["programs"].append(n)

# ---------- Load ----------
prog_df = get_programs_df()
name_to_id: Dict[str, str] = dict(zip(prog_df["PROGRAMNAME"], prog_df["PROGRAMID"])) if not prog_df.empty else {}
_sync_program_names(prog_df)

st.caption("Writes happen only when you click **Save Program**. Reads are cached.")

# ---------- Add Program ----------
# Add Program (aligned)
st.subheader("Add Program")

with st.form("add_program_form", clear_on_submit=True):
    c1, c2, c3, c4 = st.columns([2.4, 2, 1.1, 0.9], gap="small")
    with c1:
        new_name = st.text_input("Program Name", placeholder="e.g. R&D", label_visibility="collapsed")
    with c2:
        new_owner = st.text_input("Program Owner (optional)", placeholder="e.g. Jane Doe", label_visibility="collapsed")
    with c3:
        new_fte = st.number_input("Program FTE", min_value=0.0, step=0.5, value=0.0, label_visibility="collapsed")
    with c4:
        save_prog = st.form_submit_button("Save Program", type="primary", use_container_width=True)

    if save_prog:
        if not (new_name or "").strip():
            st.warning("Please provide a Program Name.")
        else:
            pname = new_name.strip()
            pid = name_to_id.get(pname, str(uuid.uuid4()))
            upsert_program(
                program_id=pid,
                name=pname,
                owner=new_owner.strip() if new_owner else None,
                program_fte=float(new_fte or 0.0),
            )
            if pname not in st.session_state["programs"]:
                st.session_state["programs"].append(pname)
            invalidate_programs_cache()
            st.success(f"Saved program '{pname}'.")
            st.rerun()


st.divider()

# ---------- Current Programs (read-only) ----------
st.subheader("Current Programs")

prog_df = get_programs_df()
if prog_df.empty:
    st.info("No programs yet. Add one above.")
else:
    # Just show a clean table with the three columns users care about
    to_show = prog_df[["PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE"]].rename(
        columns={
            "PROGRAMNAME": "Program",
            "PROGRAMOWNER": "Owner",
            "PROGRAMFTE": "FTE",
        }
    )
    st.dataframe(to_show, use_container_width=True)
