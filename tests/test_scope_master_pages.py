import sys
import types

import pandas as pd

# Provide a minimal Streamlit shim when the real package is not installed.
if "streamlit" not in sys.modules:
    fake_st_module = types.ModuleType("streamlit")

    def _noop_deco(*args, **kwargs):
        def deco(fn):
            return fn

        return deco

    fake_st_module.session_state = {}
    fake_st_module.cache_data = _noop_deco
    fake_st_module.cache_resource = _noop_deco
    sys.modules["streamlit"] = fake_st_module

from core.authorization import (  # noqa: E402
    EffectiveScope,
    PrincipalContext,
    filter_application_groups_by_scope,
    filter_df_by_scope,
)


def test_programs_scope_filter_hides_out_of_scope_programs_for_contributor():
    principal = PrincipalContext(email="pm@example.com", display_name="PM", role="CONTRIBUTOR")
    scope = EffectiveScope(program_ids={"P1"})
    df = pd.DataFrame(
        [
            {"PROGRAMID": "P1", "PROGRAMNAME": "Program One"},
            {"PROGRAMID": "P2", "PROGRAMNAME": "Program Two"},
        ]
    )

    scoped = filter_df_by_scope(
        df,
        resource_type="program",
        id_col="PROGRAMID",
        principal=principal,
        scope=scope,
    )
    assert scoped["PROGRAMID"].tolist() == ["P1"]


def test_teams_scope_filter_hides_out_of_scope_teams_for_contributor():
    principal = PrincipalContext(email="po@example.com", display_name="PO", role="CONTRIBUTOR")
    scope = EffectiveScope(team_ids={"T2"})
    df = pd.DataFrame(
        [
            {"TEAMID": "T1", "TEAMNAME": "Team One"},
            {"TEAMID": "T2", "TEAMNAME": "Team Two"},
        ]
    )

    scoped = filter_df_by_scope(
        df,
        resource_type="team",
        id_col="TEAMID",
        principal=principal,
        scope=scope,
    )
    assert scoped["TEAMID"].tolist() == ["T2"]


def test_app_groups_scope_prefers_group_ids_then_team_then_program():
    principal = PrincipalContext(email="po@example.com", display_name="PO", role="CONTRIBUTOR")
    df = pd.DataFrame(
        [
            {"GROUPID": "G1", "TEAMID": "T1", "PROGRAMID": "P1"},
            {"GROUPID": "G2", "TEAMID": "T2", "PROGRAMID": "P2"},
        ]
    )

    by_group = filter_application_groups_by_scope(
        df,
        principal=principal,
        scope=EffectiveScope(app_group_ids={"G2"}, team_ids={"T1"}, program_ids={"P1"}),
    )
    assert by_group["GROUPID"].tolist() == ["G2"]

    by_team = filter_application_groups_by_scope(
        df.drop(columns=["GROUPID"]),
        principal=principal,
        scope=EffectiveScope(team_ids={"T1"}, program_ids={"P2"}),
        group_id_col="GROUPID",
        team_id_col="TEAMID",
        program_id_col="PROGRAMID",
    )
    assert by_team["TEAMID"].tolist() == ["T1"]

    by_program = filter_application_groups_by_scope(
        df.drop(columns=["GROUPID", "TEAMID"]),
        principal=principal,
        scope=EffectiveScope(program_ids={"P2"}),
        group_id_col="GROUPID",
        team_id_col="TEAMID",
        program_id_col="PROGRAMID",
    )
    assert by_program["PROGRAMID"].tolist() == ["P2"]


def test_app_groups_scope_returns_empty_for_unscoped_contributor():
    principal = PrincipalContext(email="contrib@example.com", display_name="Contributor", role="CONTRIBUTOR")
    df = pd.DataFrame([{"GROUPID": "G1", "TEAMID": "T1", "PROGRAMID": "P1"}])
    scoped = filter_application_groups_by_scope(
        df,
        principal=principal,
        scope=EffectiveScope(),
    )
    assert scoped.empty


def test_admin_and_viewer_bypass_scope_filtering():
    df = pd.DataFrame([{"GROUPID": "G1"}, {"GROUPID": "G2"}])
    admin = PrincipalContext(email="admin@example.com", display_name="Admin", role="ADMIN")
    viewer = PrincipalContext(email="viewer@example.com", display_name="Viewer", role="VIEWER")
    empty_scope = EffectiveScope()

    admin_out = filter_application_groups_by_scope(df, principal=admin, scope=empty_scope)
    viewer_out = filter_application_groups_by_scope(df, principal=viewer, scope=empty_scope)

    assert len(admin_out.index) == 2
    assert len(viewer_out.index) == 2
