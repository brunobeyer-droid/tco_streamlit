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
    SOURCE_AUTO_OWNER,
    SOURCE_MANUAL_OVERRIDE,
    build_name_to_id_map,
    can,
    explain_access,
    get_effective_scope,
    normalize_role,
    role_action_matrix_df,
)


def test_normalize_role_maps_editor():
    assert normalize_role("editor") == "CONTRIBUTOR"
    assert normalize_role("ADMIN") == "ADMIN"
    assert normalize_role("unknown") == "VIEWER"


def test_can_scoped_contributor_access():
    principal = PrincipalContext(email="user@example.com", display_name="User", role="CONTRIBUTOR")
    scope = EffectiveScope(program_ids={"P1"}, team_ids={"T1"}, app_group_ids={"G1"})
    assert can(principal, "edit", "program", resource_id="P1", scope=scope) is True
    assert can(principal, "edit", "program", resource_id="P2", scope=scope) is False
    assert can(principal, "map", "application", resource_id="G1", scope=scope) is True


def test_can_application_scoped_action_without_resource_id_from_team_scope():
    principal = PrincipalContext(email="user@example.com", display_name="User", role="CONTRIBUTOR")
    scope = EffectiveScope(team_ids={"T1"})
    assert can(principal, "onboard", "application", scope=scope) is True
    assert can(principal, "edit", "application", scope=scope) is True
    assert can(principal, "map", "application", scope=scope) is True


def test_can_application_scoped_action_without_resource_id_denied_when_no_scope():
    principal = PrincipalContext(email="user@example.com", display_name="User", role="CONTRIBUTOR")
    scope = EffectiveScope()
    assert can(principal, "onboard", "application", scope=scope) is False
    assert can(principal, "edit", "application", scope=scope) is False
    assert can(principal, "map", "application", scope=scope) is False


def test_can_application_resource_id_remains_strict():
    principal = PrincipalContext(email="user@example.com", display_name="User", role="CONTRIBUTOR")
    scope = EffectiveScope(program_ids={"P1"}, team_ids={"T1"}, app_group_ids={"G1"})
    assert can(principal, "edit", "application", resource_id="G1", scope=scope) is True
    assert can(principal, "edit", "application", resource_id="G9", scope=scope) is False


def test_explain_access_out_of_scope():
    principal = PrincipalContext(email="user@example.com", display_name="User", role="CONTRIBUTOR")
    scope = EffectiveScope(program_ids={"P1"})
    decision = explain_access(principal, action="edit", resource_type="program", resource_id="P2", scope=scope)
    assert decision.allowed is False
    assert decision.reason == "out_of_scope"


def test_get_effective_scope_unions_membership_and_owner():
    def fake_fetch(sql, params=None):
        text = " ".join(str(sql).upper().split())
        if "FROM APP_USER_MEMBERSHIP" in text:
            return pd.DataFrame([{"PROGRAMID": "P1", "TEAMID": "T1"}])
        if "SELECT TEAMID FROM TEAMS WHERE PROGRAMID IN" in text:
            return pd.DataFrame([{"TEAMID": "T1"}, {"TEAMID": "T2"}])
        if "SELECT TEAMID, PROGRAMID FROM TEAMS WHERE TEAMID IN" in text:
            return pd.DataFrame([{"TEAMID": "T1", "PROGRAMID": "P1"}, {"TEAMID": "T2", "PROGRAMID": "P2"}])
        if "FROM PROGRAMS WHERE" in text and "PROGRAMOWNER" in text:
            return pd.DataFrame([{"PROGRAMID": "P3"}])
        if "FROM TEAMS WHERE" in text and "PRODUCTOWNER" in text:
            return pd.DataFrame([{"TEAMID": "T3", "PROGRAMID": "P3"}])
        if "FROM APPLICATION_GROUPS WHERE" in text and "OWNER" in text:
            return pd.DataFrame([{"GROUPID": "G3"}])
        if "SELECT GROUPID FROM APPLICATION_GROUPS WHERE TEAMID IN" in text:
            return pd.DataFrame([{"GROUPID": "G1"}, {"GROUPID": "G2"}])
        return pd.DataFrame()

    principal = PrincipalContext(
        email="pm@example.com",
        display_name="PM User",
        role="CONTRIBUTOR",
        user={"email": "pm@example.com", "display_name": "PM User"},
    )
    scope = get_effective_scope(fake_fetch, principal)
    assert "P1" in scope.program_ids
    assert "P2" in scope.program_ids
    assert "P3" in scope.program_ids
    assert "T1" in scope.team_ids
    assert "T3" in scope.team_ids
    assert "G3" in scope.app_group_ids
    assert SOURCE_MANUAL_OVERRIDE in set(scope.program_sources.get("P1") or set())
    assert SOURCE_AUTO_OWNER in set(scope.program_sources.get("P3") or set())


def test_explain_access_includes_scope_source():
    principal = PrincipalContext(email="user@example.com", display_name="User", role="CONTRIBUTOR")
    scope = EffectiveScope(
        program_ids={"P1"},
        program_sources={"P1": {SOURCE_AUTO_OWNER, SOURCE_MANUAL_OVERRIDE}},
    )
    decision = explain_access(principal, action="edit", resource_type="program", resource_id="P1", scope=scope)
    assert decision.allowed is True
    assert decision.matched_scope_source == "AUTO_OWNER+MANUAL_OVERRIDE"


def test_build_name_to_id_map_counts_duplicates():
    df = pd.DataFrame(
        [
            {"PROGRAMID": "P1", "PROGRAM_LABEL": "Alpha"},
            {"PROGRAMID": "P2", "PROGRAM_LABEL": "Alpha"},
            {"PROGRAMID": "P3", "PROGRAM_LABEL": "Beta"},
        ]
    )
    mapping, counts = build_name_to_id_map(
        df,
        id_col="PROGRAMID",
        primary_name_col="PROGRAM_LABEL",
    )
    assert mapping.get("Alpha") in {"P1", "P2"}
    assert counts.get("Alpha") == 2
    assert mapping.get("Beta") == "P3"


def test_role_action_matrix_df_contains_expected_actions():
    df = role_action_matrix_df()
    assert not df.empty
    actions = set(df["Action"].astype(str).tolist())
    assert {"view", "onboard", "edit", "map", "headcount_edit", "manage_access"}.issubset(actions)
    view_row = df.loc[df["Action"] == "view"].iloc[0]
    assert str(view_row["VIEWER"]) == "Allowed"
