import streamlit as st
import uuid
from typing import Dict

from snowflake_db import ensure_tables, fetch_df, upsert_program, delete_program

st.set_page_config(page_title="Programs", page_icon="🏢", layout="wide")
st.title("🏢 Programs")

# -----------------------------
# Ensure base tables exist
# -----------------------------
ensure_tables()

# -----------------------------
# Helpers
# -----------------------------
def _migrate_session_keys_on_rename(old_name: str, new_name: str):
    """When a program is renamed, migrate related session_state mirrors."""
    if "programs" in st.session_state:
        st.session_state["programs"] = [
            new_name if p == old_name else p for p in st.session_state["programs"]
        ]
    if "program_fte" in st.session_state:
        pf = st.session_state["program_fte"]
        if old_name in pf:
            pf[new_name] = pf.pop(old_name)
    if "teams_data" in st.session_state:
        td = st.session_state["teams_data"]
        if old_name in td:
            td[new_name] = td.pop(old_name)

def _sync_session_from_db(df):
    names = df["PROGRAMNAME"].tolist() if not df.empty else []
    if "programs" not in st.session_state:
        st.session_state["programs"] = names
    else:
        for n in names:
            if n not in st.session_state["programs"]:
                st.session_state["programs"].append(n)

# -----------------------------
# Load Programs from Snowflake
# -----------------------------
prog_df = fetch_df("SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER FROM PROGRAMS ORDER BY PROGRAMNAME;")
name_to_id: Dict[str, str] = dict(zip(prog_df["PROGRAMNAME"], prog_df["PROGRAMID"])) if not prog_df.empty else {}

_sync_session_from_db(prog_df)

if "program_fte" not in st.session_state:
    st.session_state["program_fte"] = {}

st.caption("Create, edit, and delete **Programs**. Saved to Snowflake and mirrored in session state.")

# -----------------------------
# Add / Update (Upsert) Program
# -----------------------------
st.subheader("Add / Update Program")
c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
with c1:
    new_name = st.text_input("Program Name", key="add_prog_name")
with c2:
    new_owner = st.text_input("Program Owner (optional)", key="add_prog_owner")
with c3:
    new_fte = st.number_input("Program FTE (optional)", min_value=0.0, step=0.5, key="add_prog_fte")
with c4:
    if st.button("Save Program", type="primary"):
        if not new_name.strip():
            st.warning("Please provide a Program Name.")
        else:
            pname = new_name.strip()
            pid = name_to_id.get(pname, str(uuid.uuid4()))
            upsert_program(pid, pname, new_owner.strip() if new_owner else None)

            if pname not in st.session_state["programs"]:
                st.session_state["programs"].append(pname)
            st.session_state["program_fte"][pname] = float(new_fte) if new_fte else 0.0

            st.success(f"Saved program '{pname}'.")
            st.rerun()

st.divider()

# -----------------------------
# Existing Programs
# -----------------------------
st.subheader("Current Programs")
if prog_df.empty:
    st.info("No programs found yet. Add one above.")
else:
    for idx, row in prog_df.iterrows():
        pid = row["PROGRAMID"]
        pname = row["PROGRAMNAME"]
        powner = row["PROGRAMOWNER"]
        current_fte = st.session_state["program_fte"].get(pname, 0.0)

        e1, e2, e3, e4, e5 = st.columns([2, 2, 1, 1, 1])
        with e1:
            edited_name = st.text_input("Program Name", value=pname, key=f"name_{pid}", label_visibility="collapsed")
        with e2:
            edited_owner = st.text_input("Owner", value=(powner or ""), key=f"owner_{pid}", label_visibility="collapsed")
        with e3:
            edited_fte = st.number_input("FTE", min_value=0.0, step=0.5, value=current_fte, key=f"fte_{pid}", label_visibility="collapsed")
        with e4:
            if st.button("Update", key=f"update_{pid}"):
                upsert_program(pid, edited_name.strip(), edited_owner.strip() if edited_owner else None)

                st.session_state["program_fte"][edited_name.strip()] = float(edited_fte)

                if edited_name.strip() != pname:
                    _migrate_session_keys_on_rename(pname, edited_name.strip())

                st.success(f"Updated '{edited_name.strip()}'.")
                st.rerun()
        with e5:
            if st.button("Delete", key=f"delete_{pid}"):
                delete_program(pid)
                if "programs" in st.session_state and pname in st.session_state["programs"]:
                    st.session_state["programs"].remove(pname)
                st.session_state["program_fte"].pop(pname, None)
                if "teams_data" in st.session_state and pname in st.session_state["teams_data"]:
                    st.session_state["teams_data"].pop(pname, None)
                st.success(f"Deleted '{pname}'.")
                st.rerun()

    with st.expander("View table"):
        st.dataframe(prog_df, use_container_width=True)

st.caption("Tip: Other pages (Teams, Cost Summary) use session_state mirrors.")

