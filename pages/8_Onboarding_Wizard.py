# Summary: update onboarding UI labels to NEXT branding.
from __future__ import annotations

import pandas as pd
import streamlit as st

from core.init import init_page
# Deprecated: onboarding is now embedded in Programs/Teams/Applications pages.
from core.debug import _is_admin
from core.ado_mapping_service import (
    list_ado_program_mappings,
    list_ado_team_mappings,
)
from db import fetch_df
from utils.ado_mapping_ui import render_ado_mapping_panel

from utils.app_shell import bootstrap_page
bootstrap_page()
page_theme = init_page("Onboarding", page_path=__file__)
_page_theme = page_theme
user = st.session_state.get("auth_user") or {}
is_admin = _is_admin()

if not is_admin:
    st.info(
        "This wizard is deprecated. Use embedded onboarding in Programs -> Onboard, Teams -> Onboard, and "
        "Applications -> Onboard for the current supported flow."
    )
    st.stop()

st.title("Onboarding Wizard")
st.caption("Guided ADO mapping for Programs → Teams → Applications. Use Settings → ADO for bulk edits or diagnostics.")


def _load_programs() -> pd.DataFrame:
    try:
        return fetch_df(
            """
            SELECT
              PROGRAMID,
              COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME) AS PROGRAMNAME
            FROM PROGRAMS
            ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME)
            """,
            None,
        )
    except Exception:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME"])


def _load_teams() -> pd.DataFrame:
    try:
        return fetch_df(
            """
            SELECT
              TEAMID,
              COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME) AS TEAMNAME,
              PROGRAMID
            FROM TEAMS
            ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME)
            """,
            None,
        )
    except Exception:
        return pd.DataFrame(columns=["TEAMID", "TEAMNAME", "PROGRAMID"])


def _load_groups() -> pd.DataFrame:
    try:
        return fetch_df("SELECT GROUPID, GROUPNAME, TEAMID FROM APPLICATION_GROUPS ORDER BY GROUPNAME", None)
    except Exception:
        return pd.DataFrame(columns=["GROUPID", "GROUPNAME", "TEAMID"])


def _mapping_counts() -> dict[str, dict[str, int]]:
    stats = {
        "programs": {"total": 0, "mapped": 0},
        "teams": {"total": 0, "mapped": 0},
        "apps": {"total": 0, "mapped": 0},
    }
    try:
        df = fetch_df(
            """
            SELECT
              COUNT(DISTINCT CASE WHEN PROGRAM_RAW IS NOT NULL AND LTRIM(RTRIM(PROGRAM_RAW)) <> '' THEN PROGRAM_RAW END) AS PROGRAM_TOTAL,
              COUNT(DISTINCT CASE WHEN TEAM_VARIANT_KEY IS NOT NULL AND LTRIM(RTRIM(TEAM_VARIANT_KEY)) <> '' THEN TEAM_VARIANT_KEY END) AS TEAM_TOTAL,
              COUNT(DISTINCT CASE WHEN APP_NAME_RAW IS NOT NULL AND LTRIM(RTRIM(APP_NAME_RAW)) <> '' THEN APP_NAME_RAW END) AS APP_TOTAL
            FROM ADO_FEATURES
            """,
            None,
        )
        if df is not None and not df.empty:
            stats["programs"]["total"] = int(df.iloc[0].get("PROGRAM_TOTAL") or 0)
            stats["teams"]["total"] = int(df.iloc[0].get("TEAM_TOTAL") or 0)
            stats["apps"]["total"] = int(df.iloc[0].get("APP_TOTAL") or 0)
    except Exception:
        pass
    try:
        df2 = fetch_df(
            """
            SELECT
              (SELECT COUNT(DISTINCT ADO_PROGRAM) FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM) AS PROGRAM_MAPPED,
              (SELECT COUNT(DISTINCT ADO_TEAM_KEY) FROM MAP_ADO_TEAM_TO_TCO_TEAM) AS TEAM_MAPPED,
              (SELECT COUNT(DISTINCT ADO_APP) FROM MAP_ADO_APP_TO_TCO_GROUP) AS APP_MAPPED
            """,
            None,
        )
        if df2 is not None and not df2.empty:
            stats["programs"]["mapped"] = int(df2.iloc[0].get("PROGRAM_MAPPED") or 0)
            stats["teams"]["mapped"] = int(df2.iloc[0].get("TEAM_MAPPED") or 0)
            stats["apps"]["mapped"] = int(df2.iloc[0].get("APP_MAPPED") or 0)
    except Exception:
        pass
    return stats


tab1, tab2, tab3, tab4 = st.tabs(["1. Programs", "2. Teams", "3. Applications", "4. Review"])

with tab1:
    st.subheader("Step 1 — Map ADO Programs to NEXT Programs")
    st.caption("Choose a NEXT Program, then map incoming ADO program values to it.")
    programs = _load_programs()
    if programs.empty:
        st.warning("No Programs found. Create one in the Programs page before mapping.")
    else:
        prog_id = st.selectbox(
            "Program",
            options=programs["PROGRAMID"].astype(str).tolist(),
            format_func=lambda pid: programs.loc[programs["PROGRAMID"] == pid, "PROGRAMNAME"].iloc[0],
            key="wizard_program_pick",
        )
        render_ado_mapping_panel(
            "program",
            prog_id,
            context_filters={"updated_by": str(user.get("email") or "")},
            user_is_admin=is_admin,
            wizard_mode=True,
        )

with tab2:
    st.subheader("Step 2 — Map ADO Teams to NEXT Teams")
    st.caption("Pick a NEXT Team, then link ADO team variants for that program.")
    teams = _load_teams()
    if teams.empty:
        st.warning("No Teams found. Create one in the Teams page before mapping.")
    else:
        team_id = st.selectbox(
            "Team",
            options=teams["TEAMID"].astype(str).tolist(),
            format_func=lambda tid: teams.loc[teams["TEAMID"] == tid, "TEAMNAME"].iloc[0],
            key="wizard_team_pick",
        )
        team_row = teams[teams["TEAMID"] == team_id]
        program_id = str(team_row.iloc[0].get("PROGRAMID") or "") if not team_row.empty else ""
        ado_programs = []
        if program_id:
            prog_map = list_ado_program_mappings()
            if prog_map is not None and not prog_map.empty:
                ado_programs = (
                    prog_map.loc[prog_map["PROGRAMID"].astype(str) == program_id, "ADO_PROGRAM"]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
        render_ado_mapping_panel(
            "team",
            team_id,
            context_filters={"program_id": program_id, "program_raws": ado_programs, "updated_by": str(user.get("email") or "")},
            user_is_admin=is_admin,
            wizard_mode=True,
        )

with tab3:
    st.subheader("Step 3 — Map ADO App Names to Applications")
    st.caption("Pick an Application, then link ADO app names from its team.")
    groups = _load_groups()
    if groups.empty:
        st.warning("No Applications found. Create one in Applications before mapping.")
    else:
        group_id = st.selectbox(
            "Application",
            options=groups["GROUPID"].astype(str).tolist(),
            format_func=lambda gid: groups.loc[groups["GROUPID"] == gid, "GROUPNAME"].iloc[0],
            key="wizard_group_pick",
        )
        group_row = groups[groups["GROUPID"] == group_id]
        team_id = str(group_row.iloc[0].get("TEAMID") or "") if not group_row.empty else ""
        ado_team_keys = []
        if team_id:
            team_map = list_ado_team_mappings()
            if team_map is not None and not team_map.empty:
                ado_team_keys = (
                    team_map.loc[team_map["TEAMID"].astype(str) == team_id, "ADO_TEAM_KEY"]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
        render_ado_mapping_panel(
            "app_group",
            group_id,
            context_filters={"team_id": team_id, "team_variant_keys": ado_team_keys, "updated_by": str(user.get("email") or "")},
            user_is_admin=is_admin,
            wizard_mode=True,
        )

with tab4:
    st.subheader("Step 4 — Review coverage")
    stats = _mapping_counts()
    c1, c2, c3 = st.columns(3)
    for col, label in zip([c1, c2, c3], ["programs", "teams", "apps"]):
        total = stats[label]["total"]
        mapped = stats[label]["mapped"]
        pct = (100.0 * mapped / total) if total else 0.0
        col.metric(f"{label.title()} mapped", f"{mapped:,} / {total:,}", f"{pct:.1f}%")
    st.caption("Use Settings → ADO for bulk edits or to resolve complex conflicts.")
