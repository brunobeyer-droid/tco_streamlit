# pages/Teams.py
import streamlit as st
import uuid
from typing import Optional

from snowflake_db import ensure_tables, fetch_df, execute

try:
    from utils.sidebar import render_global_actions
except Exception:
    def render_global_actions():
        pass

st.set_page_config(page_title="Teams", page_icon="👥", layout="wide")
render_global_actions()
ensure_tables()

# --- Ensure columns exist (idempotent) ---
execute("ALTER TABLE TEAMS ADD COLUMN IF NOT EXISTS DELIVERY_TEAM_FTE FLOAT")
execute("ALTER TABLE TEAMS ADD COLUMN IF NOT EXISTS CONTRACTOR_C_FTE FLOAT")
execute("ALTER TABLE TEAMS ADD COLUMN IF NOT EXISTS CONTRACTOR_CS_FTE FLOAT")
execute("ALTER TABLE TEAMS ADD COLUMN IF NOT EXISTS PRODUCTOWNER STRING")

st.title("👥 Teams")

@st.cache_data(ttl=180, show_spinner=False)
def _programs_df():
    return fetch_df("SELECT PROGRAMID, PROGRAMNAME FROM PROGRAMS ORDER BY PROGRAMNAME")

@st.cache_data(ttl=60, show_spinner=False)
def _teams_df():
    return fetch_df(
        """
        SELECT TEAMID, TEAMNAME, PROGRAMID, TEAMFTE,
               DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE,
               PRODUCTOWNER
        FROM TEAMS
        ORDER BY TEAMNAME
        """
    )

def _team_id_for_name_ci(name: str) -> Optional[str]:
    if not name:
        return None
    df = fetch_df("SELECT TEAMID FROM TEAMS WHERE UPPER(TEAMNAME)=UPPER(%s) LIMIT 1", (name.strip(),))
    if df is not None and not df.empty:
        return str(df.iloc[0]["TEAMID"])
    return None

programs = _programs_df()

# -------------------------------------------------------------------
# Add New Team
# -------------------------------------------------------------------
with st.expander("➕ Add New Team", expanded=False):
    teamname = st.text_input("Team Name", key="add_teamname")
    programid = st.selectbox(
        "Program",
        options=(programs["PROGRAMID"].tolist() if programs is not None and not programs.empty else []),
        format_func=lambda x: programs.loc[programs["PROGRAMID"] == x, "PROGRAMNAME"].values[0]
        if programs is not None and not programs.empty else "",
        key="add_programid",
    )
    product_owner = st.text_input("Product Owner (required)", key="add_product_owner")
    teamfte = st.number_input("Team FTE", min_value=0.0, step=0.1, key="add_teamfte")
    delivery_team_fte = st.number_input("Delivery Team FTE", min_value=0.0, step=0.1, key="add_delivery_team_fte")
    contractor_c_fte = st.number_input("Contractor C FTE", min_value=0.0, step=0.1, key="add_contractor_c_fte")
    CONTRACTOR_CS_FTE = st.number_input("Contractor CS", min_value=0.0, step=0.1, key="add_CONTRACTOR_CS_FTE")

    if st.button("Create Team", key="add_create_btn"):
        name = (teamname or "").strip()
        po = (product_owner or "").strip()
        if not name:
            st.error("Team Name is required.")
        elif not programid:
            st.error("Program is required.")
        elif not po:
            st.error("Product Owner is required.")
        else:
            # Uniqueness on TEAMNAME (case-insensitive)
            existing_id = _team_id_for_name_ci(name)
            if existing_id:
                st.error(f"A Team named '{name}' already exists. Team names must be unique.")
            else:
                try:
                    teamid = str(uuid.uuid4())
                    execute(
                        """
                        INSERT INTO TEAMS (TEAMID, TEAMNAME, PROGRAMID, TEAMFTE,
                                           DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE,
                                           PRODUCTOWNER)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (teamid, name, programid, teamfte, delivery_team_fte, contractor_c_fte, CONTRACTOR_CS_FTE, po),
                    )
                    st.success("Team created.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Create failed: {e}")

# -------------------------------------------------------------------
# Edit Existing Team
# -------------------------------------------------------------------
with st.expander("✏️ Edit Existing Team", expanded=True):
    teams = _teams_df()
    if teams is not None and not teams.empty:
        selected = st.selectbox("Select Team", teams["TEAMNAME"], key="edit_select_team")
        row = teams[teams["TEAMNAME"] == selected].iloc[0]

        new_name = st.text_input("Team Name", value=row["TEAMNAME"], key="edit_teamname")
        new_product_owner = st.text_input("Product Owner (required)", value=row.get("PRODUCTOWNER") or "", key="edit_product_owner")
        new_fte = st.number_input("Team FTE", value=float(row["TEAMFTE"] or 0), step=0.1, key="edit_teamfte")
        new_delivery = st.number_input("Delivery Team FTE", value=float(row["DELIVERY_TEAM_FTE"] or 0), step=0.1, key="edit_delivery_team_fte")
        new_cc = st.number_input("Contractor C FTE", value=float(row["CONTRACTOR_C_FTE"] or 0), step=0.1, key="edit_contractor_c_fte")
        new_cs = st.number_input("Contractor CS FTE", value=float(row["CONTRACTOR_CS_FTE"] or 0), step=0.1, key="edit_CONTRACTOR_CS_FTE")

        if st.button("Update Team", key="edit_update_btn"):
            name = (new_name or "").strip()
            po = (new_product_owner or "").strip()
            if not name:
                st.error("Team Name is required.")
            elif not po:
                st.error("Product Owner is required.")
            else:
                # Ensure uniqueness against other rows
                existing_id = _team_id_for_name_ci(name)
                if existing_id and existing_id != row["TEAMID"]:
                    st.error(f"A Team named '{name}' already exists. Team names must be unique.")
                else:
                    try:
                        execute(
                            """
                            UPDATE TEAMS
                            SET TEAMNAME=%s, TEAMFTE=%s, DELIVERY_TEAM_FTE=%s, CONTRACTOR_C_FTE=%s, CONTRACTOR_CS_FTE=%s,
                                PRODUCTOWNER=%s
                            WHERE TEAMID=%s
                            """,
                            (name, new_fte, new_delivery, new_cc, new_cs, po, row["TEAMID"]),
                        )
                        st.success("Team updated.")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Update failed: {e}")
    else:
        st.info("No teams yet.")

# -------------------------------------------------------------------
# Delete Team (blocked if linked groups or invoices exist)
# -------------------------------------------------------------------
with st.expander("🗑️ Delete Team", expanded=False):
    teams = _teams_df()
    if teams is not None and not teams.empty:
        selected = st.selectbox("Select Team to Delete", teams["TEAMNAME"], key="delete_select_team")
        row = teams[teams["TEAMNAME"] == selected].iloc[0]

        # Check dependencies: application groups linked to this team
        dep_groups_df = fetch_df("SELECT COUNT(*) AS CNT FROM APPLICATION_GROUPS WHERE TEAMID=%s", (row["TEAMID"],))
        group_count = int(dep_groups_df.iloc[0]["CNT"]) if dep_groups_df is not None and not dep_groups_df.empty else 0

        # Check dependencies: invoices linked to this team
        dep_inv_df = fetch_df("SELECT COUNT(*) AS CNT FROM INVOICES WHERE TEAMID=%s", (row["TEAMID"],))
        inv_count = int(dep_inv_df.iloc[0]["CNT"]) if dep_inv_df is not None and not dep_inv_df.empty else 0

        if group_count > 0 or inv_count > 0:
            reasons = []
            if group_count > 0:
                reasons.append(f"{group_count} application group(s)")
            if inv_count > 0:
                reasons.append(f"{inv_count} invoice(s)")
            reason_text = " and ".join(reasons)
            st.warning(
                f"Cannot delete **{row['TEAMNAME']}** because it has **{reason_text}** linked. "
                f"Delete or reassign those dependencies first."
            )
            st.button("Delete Selected Team", type="secondary", key="delete_btn_disabled", disabled=True)
        else:
            if st.button("Delete Selected Team", type="secondary", key="delete_btn"):
                try:
                    execute("DELETE FROM TEAMS WHERE TEAMID=%s", (row["TEAMID"],))
                    st.warning("Team deleted.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")
    else:
        st.info("No teams to delete.")

# -------------------------------------------------------------------
# Existing Teams (separate section after Delete)
# -------------------------------------------------------------------
with st.expander("📋 Existing Teams", expanded=False):
    teams_table = _teams_df()
    if teams_table is not None and not teams_table.empty:
        st.dataframe(teams_table, use_container_width=True, hide_index=True)
    else:
        st.info("No teams yet.")
