# pages/Applications.py
import streamlit as st
import uuid
import pandas as pd
from typing import Optional, List

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
# One-time setup per session
# =========================================================
if "tables_ok" not in st.session_state:
    ensure_tables()
    st.session_state["tables_ok"] = True

# Version tokens so we can invalidate *specific* cached reads
st.session_state.setdefault("ver_programs_teams", 0)   # programs / teams / groups-for-team lists
st.session_state.setdefault("ver_groups_listing", 0)   # groups table
st.session_state.setdefault("ver_apps_listing", 0)     # apps table

def bump_version(which: str):
    st.session_state[which] += 1

ver_pt = st.session_state["ver_programs_teams"]
ver_groups = st.session_state["ver_groups_listing"]
ver_apps = st.session_state["ver_apps_listing"]

st.title("🗂️ Applications")

# -----------------------------
# Small helper to normalize None → empty DF
# -----------------------------
def _df_or_empty(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df if df is not None else pd.DataFrame()

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
def _teams_all(_ver: int):
    return list_teams()

@st.cache_data(ttl=180, show_spinner=False)
def _teams_for_program(program_id: Optional[str], _ver: int):
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
    return list_application_groups()

@st.cache_data(ttl=120, show_spinner=False)
def _apps_listing(team_id: Optional[str], _ver_apps_only: int):
    if not team_id:
        # return all apps across teams if needed by explorer
        return fetch_df("""
            SELECT a.APPLICATIONID, a.APPLICATIONNAME, a.ADD_INFO, a.GROUPID, a.VENDORID,
                   g.GROUPNAME, g.TEAMID, t.TEAMNAME, t.PROGRAMID, p.PROGRAMNAME
            FROM APPLICATIONS a
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
            LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        """)
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
    rows = df[df[name_col] == name_value]
    if rows.empty:
        return None
    try:
        return rows[id_col].iloc[0]
    except Exception:
        return None

def _select_with_placeholder(label: str, options: List[str], key: str, disabled: bool = False, index: int = 0) -> Optional[str]:
    opts = ["(select)"] + options
    choice = st.selectbox(label, options=opts, key=key, disabled=disabled, index=index if index < len(opts) else 0)
    if choice == "(select)":
        return None
    return choice

# =========================================================
# Tabs (UX refactor)
# =========================================================
build_tab, explore_tab = st.tabs([
    "Build (Groups & Instances)",
    "Explore & Link (Program → Team → Group → Vendor)",
])

# =========================================================
# TAB 1: Build — Groups & Instances
# =========================================================
with build_tab:
    st.subheader("Create / Edit — Application Groups")
    colA, colB = st.columns([1,1])

    # ------ Create / Edit Group ------
    with colA:
        programs = _df_or_empty(_programs_df(ver_pt))

        if programs.empty:
            st.warning("Create Programs first.")
            selected_program_label = None
            selected_program_id = None
        else:
            programs = programs.copy()
            programs["LABEL"] = programs["PROGRAMNAME"].astype(str)
            selected_program_label = _select_with_placeholder(
                "Program (required)",
                programs["LABEL"].tolist(),
                key="grp_program_select",
            )
            selected_program_id = _lookup_id_by_name(programs, "LABEL", "PROGRAMID", selected_program_label) if selected_program_label else None

        teams = _teams_for_program(selected_program_id, ver_pt) if selected_program_id else None
        if teams is not None and not teams.empty:
            teams = teams.copy()
            teams["LABEL"] = teams["TEAMNAME"].astype(str)
        selected_team_label = _select_with_placeholder(
            "Team (required)",
            teams["LABEL"].tolist() if teams is not None and not teams.empty else [],
            key="grp_team_select",
            disabled=selected_program_id is None,
        )
        selected_team_id = _lookup_id_by_name(teams, "LABEL", "TEAMID", selected_team_label) if selected_team_label else None

        vendors = _df_or_empty(_vendors_df(ver_pt))
        vendor_label = _select_with_placeholder(
            "Default Vendor (required)",
            vendors["VENDORNAME"].dropna().astype(str).tolist() if not vendors.empty else [],
            key="grp_vendor_select",
            disabled=selected_team_id is None,
        )
        vendor_id = _lookup_id_by_name(vendors, "VENDORNAME", "VENDORID", vendor_label) if vendor_label else None

        st.caption(
            "Context: "
            f"**{selected_program_label or '—'}** → **{selected_team_label or '—'}** → **{vendor_label or '—'}**"
        )

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
                    # Uniqueness checks
                    name_upper = group_name.strip().upper()
                    group_names = _all_group_names_upper(ver_groups)
                    app_names = _all_application_names_upper(ver_apps)
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
                                team_id=selected_team_id,
                                default_vendor_id=vendor_id,
                                owner=None,
                            )
                            st.session_state["last_group_saved_id"] = new_group_id
                            bump_version("ver_groups_listing")
                            st.success("Group saved.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Save failed: {e}")

    # ------ Existing Groups (filter by context) ------
    with colB:
        st.markdown("**Existing Groups (filtered by selection)**")
        groups = _df_or_empty(_groups_listing(ver_groups))
        if groups.empty:
            st.info("No groups yet.")
        else:
            groups = groups.copy()
            if selected_program_label is not None and "PROGRAMNAME" in groups.columns:
                groups = groups[groups["PROGRAMNAME"] == selected_program_label]
            if selected_team_label is not None and "TEAMNAME" in groups.columns:
                groups = groups[groups["TEAMNAME"] == selected_team_label]
            if vendor_label is not None and "VENDORNAME" in groups.columns:
                groups = groups[groups["VENDORNAME"] == vendor_label]

            show = ["GROUPNAME", "TEAMNAME", "VENDORNAME", "CREATED_AT"]
            show = [c for c in show if c in groups.columns]

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

        with st.expander("Danger zone: Delete a Group"):
            st.caption("⚠️ You can only delete a group with no application instances.")
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

    st.markdown("---")
    st.subheader("Create / Edit — Application Instances")

    programs_i = _df_or_empty(_programs_df(ver_pt))
    if not programs_i.empty:
        programs_i = programs_i.copy()
        programs_i["LABEL"] = programs_i["PROGRAMNAME"].astype(str)
        programs_i = programs_i.drop_duplicates(subset=["LABEL"])

    selected_program_label_i = _select_with_placeholder(
        "Program (required)",
        programs_i["LABEL"].tolist() if not programs_i.empty else [],
        key="inst_program_select",
        disabled=(programs_i is None or programs_i.empty),
    )
    selected_program_id_i = _lookup_id_by_name(programs_i, "LABEL", "PROGRAMID", selected_program_label_i) if selected_program_label_i else None

    teams_i = _teams_for_program(selected_program_id_i, ver_pt) if selected_program_id_i else None
    if teams_i is not None and not teams_i.empty:
        teams_i = teams_i.copy()
        teams_i["LABEL"] = teams_i["TEAMNAME"].astype(str)
        teams_i = teams_i.drop_duplicates(subset=["LABEL"])
    selected_team_label_i = _select_with_placeholder(
        "Team (required)",
        teams_i["LABEL"].tolist() if teams_i is not None and not teams_i.empty else [],
        key="inst_team_select",
        disabled=(selected_program_id_i is None),
    )
    selected_team_id_i = _lookup_id_by_name(teams_i, "LABEL", "TEAMID", selected_team_label_i) if selected_team_label_i else None

    groups_for_team_i = _groups_for_team(selected_team_id_i, ver_pt) if selected_team_id_i else None
    if groups_for_team_i is not None and not groups_for_team_i.empty:
        groups_for_team_i = groups_for_team_i.copy()
        groups_for_team_i["LABEL"] = groups_for_team_i["GROUPNAME"].astype(str)
        groups_for_team_i = groups_for_team_i.drop_duplicates(subset=["LABEL"])
    selected_group_label_i = _select_with_placeholder(
        "Group (owned by Team)",
        groups_for_team_i["LABEL"].tolist() if groups_for_team_i is not None and not groups_for_team_i.empty else [],
        key="inst_group_select",
        disabled=(selected_team_id_i is None),
    )
    group_row_i = groups_for_team_i[groups_for_team_i["LABEL"] == selected_group_label_i] if (groups_for_team_i is not None and selected_group_label_i) else None
    group_id_i = _safe_first_value(group_row_i, "GROUPID") if group_row_i is not None else None
    # default vendor derived from group (if present)
    vendor_id_i = _safe_first_value(group_row_i, "VENDORID") if group_row_i is not None else None

    st.caption(
        "Context: "
        f"**{selected_program_label_i or '—'}** → **{selected_team_label_i or '—'}** → **{selected_group_label_i or '—'}**"
    )

    with st.form(key="inst_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            application_instance = st.text_input("Application Instance (e.g., Baytown)", key="inst_app_name", disabled=(group_id_i is None))
        with c2:
            add_info = st.text_input("Additional Info (optional)", placeholder="e.g., Sarnia", key="inst_add_info", disabled=(group_id_i is None))

        submitted_i = st.form_submit_button("💾 Save Instance", disabled=(group_id_i is None))
        if submitted_i:
            if not application_instance.strip():
                st.error("Application Instance is required.")
            else:
                # Canonical Application Name: "<GroupName> - <Instance>"
                application_name_canonical = f"{selected_group_label_i} - {application_instance.strip()}"
                app_upper = application_name_canonical.strip().upper()

                group_names = _all_group_names_upper(ver_groups)
                app_names = _all_application_names_upper(ver_apps)
                if app_upper in app_names:
                    st.error(f"Application name '{application_name_canonical}' already exists.")
                elif app_upper in group_names:
                    st.error(f"Application name '{application_name_canonical}' conflicts with an existing Group name.")
                else:
                    try:
                        upsert_application_instance(
                            application_id=str(uuid.uuid4()),
                            group_id=group_id_i,
                            application_name=application_name_canonical,
                            add_info=add_info.strip() or None,
                            vendor_id=vendor_id_i,
                        )
                        bump_version("ver_apps_listing")
                        st.success(f"Application instance saved as **{application_name_canonical}**.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Save failed: {e}")

    st.markdown("**Instances for this Team**")
    apps_i = _apps_listing(selected_team_id_i, ver_apps) if selected_team_id_i else pd.DataFrame()
    if apps_i is not None and not apps_i.empty:
        apps_i = apps_i.copy()
        if group_id_i and "GROUPID" in apps_i.columns:
            apps_i = apps_i[apps_i["GROUPID"] == group_id_i]
        elif selected_group_label_i and "GROUPNAME" in apps_i.columns:
            apps_i = apps_i[apps_i["GROUPNAME"] == selected_group_label_i]

        show_i = ["GROUPNAME", "APPLICATIONNAME", "ADD_INFO"]
        show_i = [c for c in show_i if c in apps_i.columns]

        if apps_i.empty:
            st.info("No application instances for this selection.")
        else:
            st.dataframe(apps_i[show_i], use_container_width=True, hide_index=True)
    else:
        st.info("No application instances yet for this Team.")

    with st.expander("Danger zone: Delete an Application Instance"):
        st.caption("⚠️ Permanent.")
        apps_for_group = _apps_listing(selected_team_id_i, ver_apps) if selected_team_id_i else pd.DataFrame()
        if apps_for_group is not None and not apps_for_group.empty:
            apps_for_group = apps_for_group.copy()
            if group_id_i and "GROUPID" in apps_for_group.columns:
                apps_for_group = apps_for_group[apps_for_group["GROUPID"] == group_id_i]
            elif selected_group_label_i and "GROUPNAME" in apps_for_group.columns:
                apps_for_group = apps_for_group[apps_for_group["GROUPNAME"] == selected_group_label_i]

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

# =========================================================
# TAB 2: Explore & Link — 4-column cascading selectors
# =========================================================
with explore_tab:
    st.subheader("Explore & Link (cascading selectors)")

    # Load base tables (avoid boolean ambiguity)
    programs_all = _df_or_empty(_programs_df(ver_pt))
    teams_all = _df_or_empty(_teams_all(ver_pt))
    groups_all = _df_or_empty(_groups_listing(ver_groups))
    vendors_all = _df_or_empty(_vendors_df(ver_pt))

    # Column 1: Programs (multi)
    c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.6, 1.2])
    with c1:
        prog_opts = programs_all["PROGRAMNAME"].dropna().astype(str).tolist() if not programs_all.empty else []
        sel_programs = st.multiselect("Programs", prog_opts, key="x_prog_multi")

    # Column 2: Teams (multi) filtered by Programs
    with c2:
        teams_filtered = teams_all.copy()
        if sel_programs and not programs_all.empty and "PROGRAMID" in teams_filtered.columns:
            # join to program names for filter
            pmap = programs_all[["PROGRAMID", "PROGRAMNAME"]].copy()
            teams_filtered = teams_filtered.merge(pmap, how="left", on="PROGRAMID")
            teams_filtered = teams_filtered[teams_filtered["PROGRAMNAME"].isin(sel_programs)]
        t_opts = teams_filtered["TEAMNAME"].dropna().astype(str).tolist() if not teams_filtered.empty else []
        sel_teams = st.multiselect("Teams", t_opts, key="x_team_multi")

    # Column 3: Groups (multi) filtered by Teams
    with c3:
        groups_filtered = groups_all.copy()
        if not groups_filtered.empty and sel_teams:
            groups_filtered = groups_filtered[groups_filtered["TEAMNAME"].isin(sel_teams)]
        g_opts = groups_filtered["GROUPNAME"].dropna().astype(str).tolist() if not groups_filtered.empty else []
        sel_groups = st.multiselect("Application Groups", g_opts, key="x_group_multi")

    # Column 4: Vendors (multi, for exploration; we’ll pick a single vendor for linking action below)
    with c4:
        v_opts = vendors_all["VENDORNAME"].dropna().astype(str).tolist() if not vendors_all.empty else []
        sel_vendors = st.multiselect("Vendors (filter view)", v_opts, key="x_vendor_multi")

    # Reactive inspection table
    st.markdown("#### Current Links (filtered)")
    view = groups_all.copy()
    if not view.empty:
        if sel_programs and "PROGRAMNAME" in view.columns:
            view = view[view["PROGRAMNAME"].isin(sel_programs)]
        if sel_teams and "TEAMNAME" in view.columns:
            view = view[view["TEAMNAME"].isin(sel_teams)]
        if sel_groups and "GROUPNAME" in view.columns:
            view = view[view["GROUPNAME"].isin(sel_groups)]
        if sel_vendors and "VENDORNAME" in view.columns:
            view = view[view["VENDORNAME"].isin(sel_vendors)]

    if view is None or view.empty:
        st.info("No rows for the current selection.")
    else:
        show_cols = ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "VENDORNAME", "CREATED_AT"]
        show_cols = [c for c in show_cols if c in view.columns]
        st.dataframe(view[show_cols].sort_values(show_cols[:3]), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Linking Actions")

    # ---- Action 1: Link selected Groups → a Team
    st.markdown("**Relink Groups to a Team**")
    cA, cB = st.columns([2, 1])
    with cA:
        # Pick target Program then Team (single target)
        target_prog = _select_with_placeholder(
            "Target Program",
            programs_all["PROGRAMNAME"].astype(str).tolist() if not programs_all.empty else [],
            key="link_prog_target",
            disabled=(programs_all.empty),
        )
        if target_prog:
            prog_row = programs_all[programs_all["PROGRAMNAME"] == target_prog]
            target_prog_id = _safe_first_value(prog_row, "PROGRAMID")
            teams_for_target = _teams_for_program(target_prog_id, ver_pt)
        else:
            teams_for_target = pd.DataFrame()

        teams_for_target = teams_for_target.copy() if teams_for_target is not None else pd.DataFrame()
        t_opts_target = teams_for_target["TEAMNAME"].dropna().astype(str).tolist() if not teams_for_target.empty else []
        target_team = _select_with_placeholder(
            "Target Team",
            t_opts_target,
            key="link_team_target",
            disabled=(teams_for_target.empty),
        )
        if target_team:
            target_team_id = _safe_first_value(teams_for_target[teams_for_target["TEAMNAME"] == target_team], "TEAMID")
        else:
            target_team_id = None

    with cB:
        # Which groups to relink? Use the current selection (sel_groups from column 3)
        st.caption("Groups to relink come from your current selection in column 3.")
        can_relink = bool(sel_groups) and bool(target_team_id)
        if st.button("🔗 Link selected Groups → Target Team", disabled=not can_relink, key="btn_link_groups_team"):
            try:
                # Convert group names → ids
                g_ids = []
                if not groups_all.empty:
                    g_ids = (
                        groups_all.loc[groups_all["GROUPNAME"].isin(sel_groups), "GROUPID"]
                        .dropna().astype(str).unique().tolist()
                    )
                if not g_ids:
                    st.warning("No group IDs found for the current selection.")
                else:
                    # 1) Update TEAMID on groups
                    execute(
                        "UPDATE APPLICATION_GROUPS SET TEAMID=%s WHERE GROUPID=%s",
                        [(target_team_id, gid) for gid in g_ids],
                        many=True
                    )
                    # 2) Re-sync PROGRAMID from the selected team
                    execute(
                        """
                        UPDATE APPLICATION_GROUPS g
                        SET PROGRAMID = t.PROGRAMID
                        FROM TEAMS t
                        WHERE g.GROUPID = %s
                          AND g.TEAMID = t.TEAMID
                        """,
                        [(gid,) for gid in g_ids],
                        many=True
                    )
                    bump_version("ver_programs_teams")
                    bump_version("ver_groups_listing")
                    st.success(f"Linked {len(g_ids)} group(s) to team '{target_team}'.")
                    st.rerun()
            except Exception as e:
                st.error(f"Team link failed: {e}")

    st.markdown("---")
    # ---- Action 2: Link selected Groups → a Vendor (optional cascade)
    st.markdown("**Set Vendor for selected Groups**")
    cC, cD = st.columns([2, 1])
    with cC:
        target_vendor = _select_with_placeholder(
            "Target Vendor",
            vendors_all["VENDORNAME"].astype(str).tolist() if not vendors_all.empty else [],
            key="link_vendor_target",
            disabled=(vendors_all.empty),
        )
        if target_vendor:
            target_vendor_id = _safe_first_value(vendors_all[vendors_all["VENDORNAME"] == target_vendor], "VENDORID")
        else:
            target_vendor_id = None
        cascade = st.checkbox("Also set this Vendor on all Applications in the selected group(s)", value=False, key="x_vendor_cascade")

    with cD:
        can_set_vendor = bool(sel_groups) and bool(target_vendor_id)
        if st.button("🔗 Link selected Groups → Vendor", disabled=not can_set_vendor, key="btn_link_groups_vendor"):
            try:
                g_ids = []
                if not groups_all.empty:
                    g_ids = (
                        groups_all.loc[groups_all["GROUPNAME"].isin(sel_groups), "GROUPID"]
                        .dropna().astype(str).unique().tolist()
                    )
                if not g_ids:
                    st.warning("No group IDs found for current selection.")
                else:
                    # Update default vendor on groups
                    execute(
                        "UPDATE APPLICATION_GROUPS SET DEFAULT_VENDORID=%s WHERE GROUPID=%s",
                        [(target_vendor_id, gid) for gid in g_ids],
                        many=True
                    )
                    # Optional cascade to applications
                    if cascade:
                        execute(
                            "UPDATE APPLICATIONS SET VENDORID=%s WHERE GROUPID=%s",
                            [(target_vendor_id, gid) for gid in g_ids],
                            many=True
                        )
                        bump_version("ver_apps_listing")
                    bump_version("ver_groups_listing")
                    st.success(
                        f"Linked {len(g_ids)} group(s) to vendor '{target_vendor}'."
                        + (" Also cascaded to applications." if cascade else "")
                    )
                    st.rerun()
            except Exception as e:
                st.error(f"Vendor link failed: {e}")

    st.markdown("---")
    # Quick peek: Applications filtered by current cascading selection
    st.subheader("Applications under current selection")
    apps_view = _df_or_empty(_apps_listing(None, ver_apps))
    if not apps_view.empty:
        apps_view = apps_view.copy()
        if sel_programs and "PROGRAMNAME" in apps_view.columns:
            apps_view = apps_view[apps_view["PROGRAMNAME"].isin(sel_programs)]
        if sel_teams and "TEAMNAME" in apps_view.columns:
            apps_view = apps_view[apps_view["TEAMNAME"].isin(sel_teams)]
        if sel_groups and "GROUPNAME" in apps_view.columns:
            apps_view = apps_view[apps_view["GROUPNAME"].isin(sel_groups)]
        if sel_vendors and "VENDORNAME" in apps_view.columns:
            apps_view = apps_view[apps_view["VENDORNAME"].isin(sel_vendors)]

        show_app_cols = ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "APPLICATIONNAME", "ADD_INFO", "VENDORNAME"]
        show_app_cols = [c for c in show_app_cols if c in apps_view.columns]
        if apps_view.empty:
            st.info("No applications for the current selection.")
        else:
            st.dataframe(
                apps_view[show_app_cols].sort_values(["PROGRAMNAME","TEAMNAME","GROUPNAME","APPLICATIONNAME"]),
                use_container_width=True,
                hide_index=True
            )
    else:
        st.info("No applications in the system yet.")
