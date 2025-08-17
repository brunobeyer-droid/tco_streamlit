# filename: pages/programs.py
import streamlit as st
import uuid
from typing import Dict
from snowflake.connector.errors import ProgrammingError

from snowflake_db import (
    ensure_tables as _ensure_tables,
    ensure_programs_schema,   # <-- make sure this exists in snowflake_db.py
    fetch_df,
    upsert_program,
    delete_program,
    execute,                  # used for one-time ALTER if needed
)

st.set_page_config(page_title="Programs", page_icon="🏢", layout="wide")
st.title("🏢 Programs")

# -----------------------------
# Init (only once per session)
# -----------------------------
if not st.session_state.get("_init_programs_done"):
    _ensure_tables()  # create base tables if missing; will also try to add PROGRAMFTE
    st.session_state["_init_programs_done"] = True

st.session_state.setdefault("programs", [])

# -----------------------------
# Cached raw SELECT
# -----------------------------
@st.cache_data(show_spinner=False)
def _select_programs():
    return fetch_df(
        """
        SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE
        FROM PROGRAMS
        ORDER BY PROGRAMNAME;
        """
    )

def get_programs_df():
    """Try select; if PROGRAMFTE missing, add it and retry once."""
    try:
        return _select_programs()
    except ProgrammingError as e:
        msg = str(e).lower()
        if "invalid identifier" in msg and "programfte" in msg:
            # Self-heal: add the column, clear cache, retry once
            execute("ALTER TABLE IF EXISTS PROGRAMS ADD COLUMN IF NOT EXISTS PROGRAMFTE NUMBER(18,2) DEFAULT 0;")
            _select_programs.clear()
            return _select_programs()
        raise

def invalidate_programs_cache():
    _select_programs.clear()

# Keep session list of program names in sync (for other pages)
def _sync_program_names(df):
    names = df["PROGRAMNAME"].tolist() if not df.empty else []
    for n in names:
        if n not in st.session_state["programs"]:
            st.session_state["programs"].append(n)

# -----------------------------
# Load (with auto-repair)
# -----------------------------
prog_df = get_programs_df()
name_to_id: Dict[str, str] = dict(zip(prog_df["PROGRAMNAME"], prog_df["PROGRAMID"])) if not prog_df.empty else {}
_sync_program_names(prog_df)

st.caption(
    "Nothing is written to Snowflake until you click **Save Program**, **Update**, or **Delete**. "
    "Reads are cached for a snappy UI."
)

# (Optional) quick connection info for debugging
with st.expander("Connection info (debug)"):
    try:
        info = fetch_df("SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();")
        st.write(dict(zip(["ACCOUNT", "DATABASE", "SCHEMA", "WAREHOUSE"], info.iloc[0].tolist())))
        desc = fetch_df("DESC TABLE PROGRAMS;")
        st.dataframe(desc, use_container_width=True)
    except Exception as e:
        st.error(e)

# -----------------------------
# Add Program
# -----------------------------
st.subheader("Add Program")

c1, c2, c3, c4 = st.columns([2.4, 2, 1.1, 0.9])
with c1:
    new_name = st.text_input("Program Name")
with c2:
    new_owner = st.text_input("Program Owner (optional)")
with c3:
    new_fte = st.number_input("Program FTE", min_value=0.0, step=0.5, value=0.0)
with c4:
    if st.button("Save Program", type="primary", use_container_width=True):
        if not new_name.strip():
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

# -----------------------------
# Current Programs (aligned grid with header)
# -----------------------------
st.subheader("Current Programs")

prog_df = get_programs_df()
if prog_df.empty:
    st.info("No programs yet. Add one above.")
else:
    COLS = [2.4, 2, 1.1, 0.9, 0.9]  # Name | Owner | FTE | Update | Delete
    h1, h2, h3, h4, h5 = st.columns(COLS, gap="small")
    with h1: st.markdown("**Program Name**")
    with h2: st.markdown("**Owner**")
    with h3: st.markdown("**FTE**")
    with h4: st.markdown("** **")
    with h5: st.markdown("** **")
    st.divider()

    for _, row in prog_df.iterrows():
        pid = row["PROGRAMID"]
        pname = row["PROGRAMNAME"]
        powner = row["PROGRAMOWNER"] or ""
        pfte = float(row["PROGRAMFTE"] or 0.0)

        with st.form(f"row_{pid}", clear_on_submit=False):
            c1, c2, c3, c4, c5 = st.columns(COLS, gap="small")
            with c1:
                edited_name = st.text_input("Program Name", value=pname, key=f"name_{pid}", label_visibility="hidden")
            with c2:
                edited_owner = st.text_input("Owner", value=powner, key=f"owner_{pid}", label_visibility="hidden")
            with c3:
                edited_fte = st.number_input("FTE", min_value=0.0, step=0.5, value=pfte, key=f"fte_{pid}", label_visibility="hidden")
            with c4:
                update_clicked = st.form_submit_button("Update", type="primary", use_container_width=True)
            with c5:
                delete_clicked = st.form_submit_button("Delete", use_container_width=True)

            if update_clicked:
                upsert_program(
                    program_id=pid,
                    name=edited_name.strip(),
                    owner=edited_owner.strip() if edited_owner else None,
                    program_fte=float(edited_fte or 0.0),
                )
                if edited_name.strip() != pname:
                    if "programs" in st.session_state:
                        st.session_state["programs"] = [
                            edited_name.strip() if p == pname else p
                            for p in st.session_state["programs"]
                        ]
                    if "teams_data" in st.session_state:
                        td = st.session_state["teams_data"]
                        if pname in td:
                            td[edited_name.strip()] = td.pop(pname)
                invalidate_programs_cache()
                st.success(f"Updated '{edited_name.strip()}'.")
                st.rerun()

            if delete_clicked:
                delete_program(pid)
                if pname in st.session_state["programs"]:
                    st.session_state["programs"].remove(pname)
                invalidate_programs_cache()
                st.success(f"Deleted '{pname}'.")
                st.rerun()

    with st.expander("View table (live)"):
        st.dataframe(get_programs_df(), use_container_width=True)
