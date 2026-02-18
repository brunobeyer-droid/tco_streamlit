import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from welcome.state import derive_user_scope


def test_derive_user_scope_uses_membership_scope_first():
    calls: list[str] = []

    def fake_fetch(sql, params=None):
        text = " ".join(str(sql).upper().split())
        calls.append(text)
        if "FROM APP_USER_MEMBERSHIP" in text:
            return pd.DataFrame(
                [
                    {
                        "MEMBERSHIP_PROGRAMID": "P1",
                        "PROGRAMID": "P1",
                        "PROGRAMNAME": "Program Alpha",
                        "MEMBERSHIP_TEAMID": "T1",
                        "TEAMNAME": "Team One",
                    }
                ]
            )
        if "FROM APPLICATION_GROUPS" in text and "WHERE TEAMID IN" in text:
            return pd.DataFrame([{"GROUPNAME": "Application A"}])
        return pd.DataFrame()

    user = {"email": "viewer@example.com", "name": "Viewer User", "role": "VIEWER"}
    scope = derive_user_scope(user, fake_fetch)

    assert scope.program_ids == ["P1"]
    assert scope.programs == ["Program Alpha"]
    assert scope.teams == ["Team One"]
    assert scope.groups == ["Application A"]
    assert not any("PROGRAMOWNER" in s for s in calls), "Owner-based program lookup should be skipped when membership scope exists."


def test_derive_user_scope_falls_back_to_owner_scope_when_membership_empty():
    def fake_fetch(sql, params=None):
        text = " ".join(str(sql).upper().split())
        if "FROM APP_USER_MEMBERSHIP" in text:
            return pd.DataFrame()
        if "FROM PROGRAMS" in text and "PROGRAMOWNER" in text:
            return pd.DataFrame([{"PROGRAMID": "P2", "PROGRAMNAME": "Program Beta"}])
        if "FROM TEAMS" in text and "PRODUCTOWNER" in text:
            return pd.DataFrame([{"TEAMNAME": "Team Beta"}])
        if "FROM TEAMS WHERE PROGRAMID IN" in text:
            return pd.DataFrame([{"TEAMNAME": "Team Beta"}])
        if "FROM APPLICATION_GROUPS" in text and "COALESCE(OWNER" in text:
            return pd.DataFrame([{"GROUPNAME": "Group Owner"}])
        if "FROM APPLICATION_GROUPS G JOIN TEAMS T" in text:
            return pd.DataFrame([{"GROUPNAME": "Group Team"}])
        return pd.DataFrame()

    user = {"email": "po@example.com", "name": "PO User", "role": "VIEWER"}
    scope = derive_user_scope(user, fake_fetch)

    assert scope.program_ids == ["P2"]
    assert scope.programs == ["Program Beta"]
    assert scope.teams == ["Team Beta"]
    assert scope.groups == ["Group Owner", "Group Team"]


def test_derive_user_scope_owner_match_supports_delimited_owner_tokens():
    captured_program_params: list[tuple] = []

    def fake_fetch(sql, params=None):
        text = " ".join(str(sql).upper().split())
        if "FROM APP_USER_MEMBERSHIP" in text:
            return pd.DataFrame()
        if "FROM PROGRAMS" in text and "PROGRAMOWNER" in text:
            captured_program_params.append(tuple(params or ()))
            params_u = [str(x).upper() for x in (params or ())]
            # New owner-match clause passes both exact and delimiter-token patterns.
            if "VIEWER@EXAMPLE.COM" in params_u and "%,VIEWER@EXAMPLE.COM,%" in params_u:
                return pd.DataFrame([{"PROGRAMID": "P3", "PROGRAMNAME": "Program Gamma"}])
            return pd.DataFrame()
        if "FROM TEAMS WHERE PROGRAMID IN" in text:
            return pd.DataFrame([{"TEAMNAME": "Team Gamma"}])
        if "FROM APPLICATION_GROUPS G JOIN TEAMS T" in text:
            return pd.DataFrame([{"GROUPNAME": "Group Gamma"}])
        return pd.DataFrame()

    user = {"email": "viewer@example.com", "name": "Viewer User", "role": "VIEWER"}
    scope = derive_user_scope(user, fake_fetch)

    assert captured_program_params, "Program-owner lookup should be attempted."
    assert scope.program_ids == ["P3"]
    assert scope.programs == ["Program Gamma"]
    assert scope.teams == ["Team Gamma"]
