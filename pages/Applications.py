# pages/applications.py
import streamlit as st
import uuid
import pandas as pd  # used for lightweight row reordering after save
from typing import Optional

# If your DB lives under utils/, change to: from utils.snowflake_db import ...
from snowflake_db import (
    ensure_tables, fetch_df, execute,
    list_application_groups, list_applications, list_teams, list_groups_for_team,
    upsert_application_group, upsert_application_instance,
    delete_application_group, delete_application,
)

try:
    from utils.sidebar import render_global_actions
except Exception:
    def render_global_actions(): pass

st.set_page_config(page_title="Applications", page_icon="🗂️", layout="wide")
render_global_actions()

# =========================================================
# One-time setup per session (avoid running on every rerun)
# =========================================================
if "tables_ok" not in st.session_state:
    ensure_tables()
    st.session_state["tables_ok"] = True

# Version tokens so we can invalidate *specific* cached reads
st.session_state.setdefault("ver_programs_teams", 0)  # programs/teams/groups-for-team lists
st.session_state.setdefault("ver_groups_listing", 0)  # only the groups table at the bottom
st.session_state.setdefault("ver_apps_listing", 0)    # instances table

def bump_version(which: str):
    st.session_state[which] += 1

ver_pt = st.session_state["ver_programs_teams"]
ver_groups = st.session_state["ver_groups_listing"]
ver_apps = st.session_state["ver_apps_listing"]

st.title("🗂️ Applications")

# -----------------------------
# Cached helpers (fast reads)
# -----------------------------
@st.cache_data(ttl=180, show_spinner=False)
def _vendors_df(_ver: int):
    return fetch_df("SELECT VENDORID, VENDORNAME FROM VENDORS ORDER BY VENDORNAME")

@st.cache_data(ttl=180, show_spinner=False)
def _programs_df(_ver: int):
    return fetch_df("SELECT PROGRAMID, PROGRAMNAME FROM PROGRAMS ORDER BY PROGRAMNAME")

@st.cache_data(ttl=180, show_spinner=False)
def _teams_for_program(program_id: Optional[str], _ver: int):
    """
    Prefer narrowing Teams to the selected Program (assumes TEAMS.PROGRAMID exists).
    Falls back to list_teams() if no program or query fails.
    """
    if not program_id:
        return list_teams()
    try:
        q = f"""
        SELECT TEAMID, TEAMNAME, PROGRAMID
        FROM TEAMS
        WHERE PROGRAMID = '{program_id}'
        ORDER BY TEAMNAME
        """
        df = fetch_df(q)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    return list_teams()

@st.cache_data(ttl=180, show_spinner=False)
def _groups_for_team(team_id: str, _ver: int):
    return list_groups_for_team(team_id)

@st.cache_data(ttl=120, show_spinner=False)
def _groups_listing(_ver_groups_only: int):
    # This cache is invalidated *only* when we bump ver_groups
    return list_application_groups()

@st.cache_data(ttl=120, show_spinner=False)
def _apps_listing(team_id: str, _ver_apps_only: int):
    # This cache is invalidated *only* when we bump ver_apps
    return list_applications(team_id=team_id)

# ---- Uniqueness helpers (global, case-insensitive) ----
@st.cache_data(ttl=120, show_spinner=False)
def _all_group_names_upper(_ver: int) -> set[str]:
    df = fetch_df("SELECT GROUPNAME FROM APPLICATION_GROUPS")
    if df is None or df.empty or "GROUPNAME" not in df.columns:
        return set()
    return set(df["GROUPNAME"].dropna().astype(str).str.strip().str.upper().tolist())

@st.cache_data(ttl=120, show_spinner=False)
def _all_application_names_upper(_ver: int) -> set[str]:
    df = fetch_df("SELECT APPLICATIONNAME FROM APPLICATIONS")
    if df is None or df.empty or "APPLICATIONNAME" not in df.columns:
        return set()
    return set(df["APPLICATIONNAME"].dropna().astype(str).str.strip().str.upper().tolist())

# -----------------------------
# Small utilities
# -----------------------------
def _safe_first_value(df, col) -> Optional[str]:
    if df is None or df.empty or col not in df.columns:
        return None
    try:
        return df[col].iloc[0]
    except Exception:
        return None

def _lookup_id_by_name(df, name_col: str, id_col: str, name_value: str) -> Optional[str]:
    if df is None or df.empty or name_col not in df.columns or id_col not in df.columns:
        return None
    # expect unique labels; if duplicates exist, take the first
    rows = df[df[name_col] == name_value]
    if rows.empty:
        return None
    try:
        return rows[id_col].iloc[0]
    except Exception:
        return None

# selectbox with "(select)" placeholder
def _select_with_placeholder(label: str, options: list[str], key: str, disabled: bool = False, index: int = 0) -> Optional[str]:
    opts = ["(select)"] + options
    choice = st.selectbox(label, options=opts, key=key, disabled=disabled, index=index if index < len(opts) else 0)
    if choice == "(select)":
        return None
    return choice

# Tabs
grp_tab, inst_tab, links_tab = st.tabs([
    "Groups (Program → Team)",
    "Instances (Program → Team → Group)",
    "Links (Team ↔ Groups)"
])

# =========================
# Groups tab
# =========================
with grp_tab:
    st.subheader("Create / Edit Group")

    programs = _programs_df(ver_pt)
    if programs is None or programs.empty:
        st.warning("Create Programs first (see Programs page).")
        selected_program_label = None
        selected_program_id = None
    else:
        programs["LABEL"] = programs["PROGRAMNAME"].astype(str)
        selected_program_label = _select_with_placeholder(
            "Program (required)",
            programs["LABEL"].tolist(),
            key="grp_program_select",
        )
        selected_program_id = _lookup_id_by_name(programs, "LABEL", "PROGRAMID", selected_program_label) if selected_program_label else None

    teams = _teams_for_program(selected_program_id, ver_pt) if selected_program_id else None
    if teams is not None and not teams.empty:
        teams["LABEL"] = teams["TEAMNAME"].astype(str)
    selected_team_label = _select_with_placeholder(
        "Team (required)",
        teams["LABEL"].tolist() if teams is not None and not teams.empty else [],
        key="grp_team_select",
        disabled=selected_program_id is None,
    )
    selected_team_id = _lookup_id_by_name(teams, "LABEL", "TEAMID", selected_team_label) if selected_team_label else None

    vendors = _vendors_df(ver_pt)
    vendor_label = _select_with_placeholder(
        "Vendor (required)",
        vendors["VENDORNAME"].dropna().astype(str).tolist() if vendors is not None and not vendors.empty else [],
        key="grp_vendor_select",
        disabled=selected_team_id is None,
    )
    vendor_id = _lookup_id_by_name(vendors, "VENDORNAME", "VENDORID", vendor_label) if vendor_label else None

    # Context chip (what you're filtering by) — now includes Vendor
    st.caption(
        "You’re viewing Groups for: "
        f"**{selected_program_label or '—'}** → **{selected_team_label or '—'}** → **{vendor_label or '—'}**"
    )

    # Use a form so typing/selecting doesn't cause mid-edit reruns
    with st.form(key="grp_form", clear_on_submit=True):
        group_name = st.text_input("Group Name (e.g., MHM)", key="grp_name_input", disabled=selected_team_id is None)

        submitted = st.form_submit_button("💾 Save Group")
        if submitted:
            if not group_name.strip():
                st.error("Group Name is required.")
            elif not selected_program_id:
                st.error("Program is required.")
            elif not selected_team_id:
                st.error("Team is required.")
            elif not vendor_id:
                st.error("Vendor is required.")
            else:
                # Uniqueness checks (case-insensitive, global)
                name_upper = group_name.strip().upper()
                group_names = _all_group_names_upper(ver_groups)          # current groups
                app_names = _all_application_names_upper(ver_apps)        # current applications
                if name_upper in group_names:
                    st.error(f"Group name '{group_name.strip()}' already exists.")
                elif name_upper in app_names:
                    st.error(f"Group name '{group_name.strip()}' conflicts with an existing Application name.")
                else:
                    try:
                        new_group_id = str(uuid.uuid4())
                        upsert_application_group(
                            group_id=new_group_id,
                            group_name=group_name.strip(),
                            team_id=selected_team_id,       # Team belongs to chosen Program
                            default_vendor_id=vendor_id,
                            owner=None,                     # moved to Teams; no Group owner
                        )
                        st.session_state["last_group_saved_id"] = new_group_id
                        bump_version("ver_groups_listing")
                        st.success("Group saved.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Save failed: {e}")

    st.markdown("---")
    st.subheader("Existing Groups")

    groups = _groups_listing(ver_groups)
    if groups is not None and not groups.empty:
        # Filter by Program and Team (existing behavior)
        if selected_program_label is not None and "PROGRAMNAME" in groups.columns:
            groups = groups[groups["PROGRAMNAME"] == selected_program_label]
        if selected_team_label is not None and "TEAMNAME" in groups.columns:
            groups = groups[groups["TEAMNAME"] == selected_team_label]
        # NEW: Also filter by selected Vendor (refreshes on vendor change)
        if vendor_label is not None and "VENDORNAME" in groups.columns:
            groups = groups[groups["VENDORNAME"] == vendor_label]

        # Drop GROUPID as requested
        show = ["GROUPNAME", "TEAMNAME", "VENDORNAME", "CREATED_AT"]
        show = [c for c in show if c in groups.columns]

        # If we just created a group, bring it to the top for immediate feedback
        last_id = st.session_state.get("last_group_saved_id")
        if last_id and "GROUPID" in groups.columns:
            try:
                top = groups[groups["GROUPID"] == last_id]
                rest = groups[groups["GROUPID"] != last_id]
                groups = pd.concat([top, rest], ignore_index=True)
            except Exception:
                pass

        if groups.empty:
            st.info("No groups match the current selection.")
        else:
            st.dataframe(groups[show], use_container_width=True, hide_index=True)
    else:
        st.info("No groups yet.")

    # ---------- Danger zone: delete by Group NAME (not ID) ----------
    with st.expander("Danger zone: Delete a Group"):
        st.caption("⚠️ Deleting a group is permanent. You can only delete a group with no application instances.")
        # Limit deletion choices to groups in the currently selected Team to avoid duplicates across teams
        team_groups = _groups_for_team(selected_team_id, ver_pt) if selected_team_id else None

        if team_groups is None or team_groups.empty:
            st.info("No groups available for this Team.")
        else:
            team_groups = team_groups.sort_values(by="GROUPNAME")
            del_group_name = _select_with_placeholder(
                "Select Application Group to delete",
                team_groups["GROUPNAME"].astype(str).tolist(),
                key="grp_delete_by_name",
            )
            confirm = st.text_input("Type DELETE to confirm", key="grp_delete_confirm_name", disabled=del_group_name is None)
            st.markdown("**⚠️ This action cannot be undone.**")
            if st.button("🗑️ Delete Group", key="grp_delete_btn_name", disabled=del_group_name is None):
                if confirm.strip().upper() != "DELETE":
                    st.error("Type DELETE to confirm.")
                else:
                    try:
                        gid = _safe_first_value(team_groups[team_groups["GROUPNAME"] == del_group_name], "GROUPID")
                        if not gid:
                            st.error("Could not find the selected group.")
                        else:
                            delete_application_group(str(gid))
                            bump_version("ver_groups_listing")
                            st.success(f"✅ Group '{del_group_name}' deleted.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Delete failed: {e}")

# =========================
# Instances tab
# =========================
with inst_tab:
    st.subheader("Create / Edit Application Instance")

    programs = _programs_df(ver_pt)
    if programs is None or programs.empty:
        st.warning("Create Programs first (see Programs page).")
    else:
        programs["LABEL"] = programs["PROGRAMNAME"].astype(str)
        programs = programs.drop_duplicates(subset=["LABEL"])
    selected_program_label_i = _select_with_placeholder(
        "Program (required)",
        programs["LABEL"].tolist() if programs is not None and not programs.empty else [],
        key="inst_program_select",
        disabled=(programs is None or programs.empty),
    )
    selected_program_id_i = _lookup_id_by_name(programs, "LABEL", "PROGRAMID", selected_program_label_i) if selected_program_label_i else None

    teams = _teams_for_program(selected_program_id_i, ver_pt) if selected_program_id_i else None
    if teams is not None and not teams.empty:
        teams["LABEL"] = teams["TEAMNAME"].astype(str)
        teams = teams.drop_duplicates(subset=["LABEL"])
    selected_team_label_i = _select_with_placeholder(
        "Team (required)",
        teams["LABEL"].tolist() if teams is not None and not teams.empty else [],
        key="inst_team_select",
        disabled=(selected_program_id_i is None),
    )
    selected_team_id_i = _lookup_id_by_name(teams, "LABEL", "TEAMID", selected_team_label_i) if selected_team_label_i else None

    groups_for_team = _groups_for_team(selected_team_id_i, ver_pt) if selected_team_id_i else None
    if groups_for_team is not None and not groups_for_team.empty:
        groups_for_team["LABEL"] = groups_for_team["GROUPNAME"].astype(str)
        groups_for_team = groups_for_team.drop_duplicates(subset=["LABEL"])
    selected_group_label = _select_with_placeholder(
        "Group (owned by Team)",
        groups_for_team["LABEL"].tolist() if groups_for_team is not None and not groups_for_team.empty else [],
        key="inst_group_select",
        disabled=(selected_team_id_i is None),
    )
    group_row = groups_for_team[groups_for_team["LABEL"] == selected_group_label] if (groups_for_team is not None and selected_group_label) else None
    group_id = _safe_first_value(group_row, "GROUPID") if group_row is not None else None

    # Pull group's default vendor if available; otherwise None
    vendor_id = _safe_first_value(group_row, "VENDORID") if group_row is not None else None

    # Context chip
    st.caption(
        "You’re viewing Instances for: "
        f"**{selected_program_label_i or '—'}** → **{selected_team_label_i or '—'}** → **{selected_group_label or '—'}**"
    )

    # Use a form so typing doesn't trigger expensive reruns
    with st.form(key="inst_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            # Label only; we generate the canonical Application Name from Group + Instance
            application_instance = st.text_input("Application Instance (e.g., Baytown)", key="inst_app_name", disabled=(group_id is None))
        with c2:
            add_info = st.text_input("Additional Info (optional)", placeholder="e.g., Sarnia", key="inst_add_info", disabled=(group_id is None))

        submitted = st.form_submit_button("💾 Save Instance", disabled=(group_id is None))
        if submitted:
            if not application_instance.strip():
                st.error("Application Instance is required.")
            else:
                # Canonical Application Name: "<GroupName> - <Instance>"
                application_name_canonical = f"{selected_group_label} - {application_instance.strip()}"
                app_upper = application_name_canonical.strip().upper()

                # Uniqueness checks (case-insensitive, global)
                group_names = _all_group_names_upper(ver_groups)          # all groups
                app_names = _all_application_names_upper(ver_apps)        # all apps
                if app_upper in app_names:
                    st.error(f"Application name '{application_name_canonical}' already exists.")
                elif app_upper in group_names:
                    st.error(f"Application name '{application_name_canonical}' conflicts with an existing Group name.")
                else:
                    try:
                        upsert_application_instance(
                            application_id=str(uuid.uuid4()),
                            group_id=group_id,
                            application_name=application_name_canonical,  # generated canonical name
                            add_info=add_info.strip() or None,
                            vendor_id=vendor_id,  # may be None if group doesn't expose it
                        )
                        bump_version("ver_apps_listing")  # only touch instances table cache
                        st.success(f"Application instance saved as **{application_name_canonical}**.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Save failed: {e}")

    st.markdown("---")
    st.subheader("Instances for this Team")
    if selected_team_id_i:
        apps = _apps_listing(selected_team_id_i, ver_apps)
    else:
        apps = pd.DataFrame()

    if apps is not None and not apps.empty:
        # Filter to the selected group for a tighter view
        if group_id and "GROUPID" in apps.columns:
            apps = apps[apps["GROUPID"] == group_id]
        elif selected_group_label and "GROUPNAME" in apps.columns:
            apps = apps[apps["GROUPNAME"] == selected_group_label]

        # Remove Application ID and Vendor Name; show ADD_INFO
        show = ["GROUPNAME", "APPLICATIONNAME", "ADD_INFO"]
        show = [c for c in show if c in apps.columns]

        if apps.empty:
            st.info("No application instances for this selection.")
        else:
            st.dataframe(apps[show], use_container_width=True, hide_index=True)
    else:
        st.info("No application instances yet for this Team.")

    # ---------- Danger zone: delete by Application INSTANCE NAME (not ID) ----------
    with st.expander("Danger zone: Delete an Application Instance"):
        st.caption("⚠️ Deleting an application instance is permanent and cannot be undone.")
        apps_for_group = _apps_listing(selected_team_id_i, ver_apps) if selected_team_id_i else pd.DataFrame()
        if apps_for_group is not None and not apps_for_group.empty:
            if group_id and "GROUPID" in apps_for_group.columns:
                apps_for_group = apps_for_group[apps_for_group["GROUPID"] == group_id]
            elif selected_group_label and "GROUPNAME" in apps_for_group.columns:
                apps_for_group = apps_for_group[apps_for_group["GROUPNAME"] == selected_group_label]

        if apps_for_group is None or apps_for_group.empty:
            st.info("No application instances available for this Group.")
        else:
            app_name_options = (
                apps_for_group["APPLICATIONNAME"]
                .dropna().astype(str)
                .sort_values().unique().tolist()
            )
            del_app_name = _select_with_placeholder(
                "Select Application Instance to delete",
                app_name_options,
                key="inst_delete_by_name",
            )
            confirm = st.text_input("Type DELETE to confirm", key="inst_delete_confirm_name", disabled=del_app_name is None)
            st.markdown("**⚠️ This action cannot be undone.**")
            if st.button("🗑️ Delete Application", key="inst_delete_btn_name", disabled=del_app_name is None):
                if confirm.strip().upper() != "DELETE":
                    st.error("Type DELETE to confirm.")
                else:
                    try:
                        app_row = apps_for_group[apps_for_group["APPLICATIONNAME"] == del_app_name] if del_app_name else None
                        app_id = _safe_first_value(app_row, "APPLICATIONID") if app_row is not None else None
                        if not app_id:
                            st.error("Could not find the selected application instance.")
                        else:
                            delete_application(str(app_id))
                            bump_version("ver_apps_listing")
                            st.success(f"✅ Application instance '{del_app_name}' deleted.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Delete failed: {e}")

# =========================
# Links tab — Relink Groups to a Team
# =========================
with links_tab:
    st.subheader("Link Group(s) to a Team")

    # --- flash success message if we have one from the prior run
    _links_flash = st.session_state.pop("links_success_msg", None)
    if _links_flash:
        st.success(_links_flash)

    # 1) Pick Program (to narrow the Teams list)
    programs_l = _programs_df(ver_pt)
    if programs_l is None:
        programs_l = pd.DataFrame()

    programs_l = programs_l.copy()
    if not programs_l.empty:
        programs_l["PLABEL"] = programs_l["PROGRAMNAME"].astype(str)
        programs_l = programs_l.drop_duplicates(subset=["PLABEL"])
        prog_options = programs_l["PLABEL"].tolist()
    else:
        prog_options = []

    sel_prog_label = _select_with_placeholder(
        "Program (to choose the target Team)",
        prog_options,
        key="links_prog_select",
        disabled=(len(prog_options) == 0)
    )
    # Map without renaming (avoid duplicate columns)
    sel_prog_id = None
    if sel_prog_label:
        try:
            sel_prog_id = programs_l.loc[programs_l["PLABEL"] == sel_prog_label, "PROGRAMID"].iloc[0]
        except Exception:
            sel_prog_id = None

    # 2) Pick Team (within selected Program)
    teams_l = _teams_for_program(sel_prog_id, ver_pt) if sel_prog_id else pd.DataFrame()
    teams_l = teams_l.copy()
    if not teams_l.empty:
        teams_l["TLABEL"] = teams_l["TEAMNAME"].astype(str)
        teams_l = teams_l.drop_duplicates(subset=["TLABEL"])
        team_options = teams_l["TLABEL"].tolist()
    else:
        team_options = []

    sel_team_label = _select_with_placeholder(
        "Team (new owner of the selected group(s))",
        team_options,
        key="links_team_select",
        disabled=(sel_prog_id is None or len(team_options) == 0)
    )
    sel_team_id = None
    if sel_team_label:
        try:
            sel_team_id = teams_l.loc[teams_l["TLABEL"] == sel_team_label, "TEAMID"].iloc[0]
        except Exception:
            sel_team_id = None

    # 3) Group picker(s) — allow multi-select for ANY groups (orphaned or not)
    groups_all = _groups_listing(ver_groups)
    groups_all = groups_all.copy() if groups_all is not None else pd.DataFrame()

    if groups_all.empty:
        st.info("No groups found.")
        st.multiselect("Select group(s) to link — you can pick multiple", [], key="link_groups_multi_all_disabled")
        with st.form("link_form_disabled"):
            st.form_submit_button("🔗 Link Group(s) to Team", disabled=True)
    else:
        # Orphan detection once
        orphan_mask = (
            groups_all["TEAMID"].isna()
            | (groups_all["TEAMID"].astype(str).str.strip() == "")
            | groups_all["TEAMNAME"].isna()
        )

        # Pretty label that shows current team/program to avoid ambiguity
        groups_all["GLABEL"] = groups_all.apply(
            lambda r: f"{r.get('GROUPNAME') or ''} — Team: {r.get('TEAMNAME') or '—'} — Program: {r.get('PROGRAMNAME') or '—'}",
            axis=1
        )

        show_orphans = st.checkbox(
            "Only show orphaned groups (no current team)", key="show_orphans_only", value=False
        )

        # Options for multiselect (all groups or just orphans)
        options_df = groups_all[orphan_mask].copy() if show_orphans else groups_all

        sel_group_labels = st.multiselect(
            "Select group(s) to link — you can pick multiple",
            options=options_df["GLABEL"].tolist(),
            key="link_groups_multi_all",
            default=[]
        )

        # Map labels → GROUPID (use the full groups_all to tolerate switching the filter after selection)
        sel_group_ids = (
            groups_all.loc[groups_all["GLABEL"].isin(sel_group_labels), "GROUPID"]
            .astype(str)
            .tolist()
            if sel_group_labels else []
        )

        # Context chips
        context_groups_text = ", ".join(
            groups_all.loc[groups_all["GLABEL"].isin(sel_group_labels), "GROUPNAME"]
            .astype(str)
            .tolist()
        ) or "—"

        st.caption(
            f"New owner: Program **{sel_prog_label or '—'}** → Team **{sel_team_label or '—'}**"
        )
        st.caption(
            f"Group(s) selected: **{context_groups_text}**"
        )

        # Action (form)
        with st.form("link_form_multi_any", clear_on_submit=False):
            st.write("This will update each selected group’s **TEAMID** and re-sync its **PROGRAMID** from the selected team.")
            submit_ok = bool(sel_group_ids) and (sel_team_id is not None)
            submitted_link = st.form_submit_button("🔗 Link Group(s) to Team", disabled=not submit_ok)

        if submitted_link:
            try:
                # 1) Update TEAMID for all selected groups
                execute(
                    "UPDATE APPLICATION_GROUPS SET TEAMID=%s WHERE GROUPID=%s",
                    [(sel_team_id, gid) for gid in sel_group_ids],
                    many=True
                )
                # 2) Re-sync PROGRAMID for all selected groups
                execute(
                    """
                    UPDATE APPLICATION_GROUPS g
                    SET PROGRAMID = t.PROGRAMID
                    FROM TEAMS t
                    WHERE g.GROUPID = %s
                      AND g.TEAMID = t.TEAMID
                    """,
                    [(gid,) for gid in sel_group_ids],
                    many=True
                )

                bump_version("ver_programs_teams")
                bump_version("ver_groups_listing")

                st.session_state["links_success_msg"] = (
                    f"Linked {len(sel_group_ids)} group(s) to Team '{sel_team_label}'."
                )
                st.rerun()

            except Exception as e:
                st.error(f"Link failed: {e}")
