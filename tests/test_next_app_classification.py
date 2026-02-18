import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from core.next_app_classification import (
    build_next_app_candidates_df,
    get_application_entity,
    safe_onboard_next_apps,
    suggest_vendor_from_history,
)


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame()


def test_suggest_vendor_from_history_exact_name_match():
    def fake_fetch(sql, params=None):
        text = " ".join(str(sql).upper().split())
        if "FROM APPLICATION_GROUPS G" in text:
            return pd.DataFrame(
                [
                    {"GROUPNAME": "Atlas Platform", "VENDORID": "V1", "VENDORNAME": "Contoso"},
                    {"GROUPNAME": "Billing Core", "VENDORID": "V2", "VENDORNAME": "Fabrikam"},
                ]
            )
        return _empty_df()

    vid, vname, score = suggest_vendor_from_history(fake_fetch, "Atlas Platform", threshold=90)
    assert vid == "V1"
    assert vname == "Contoso"
    assert score >= 90


def test_build_next_app_candidates_df_marks_mapped_and_vendor_flags():
    def fake_fetch(sql, params=None):
        text = " ".join(str(sql).upper().split())
        if "FROM ADO_APP_CANDIDATES" in text:
            return pd.DataFrame(
                [
                    {"ADO_APP_RAW": "Atlas App", "FEATURE_COUNT": 4, "FIRST_SEEN": "2025-01-01", "LAST_SEEN": "2025-02-01"},
                    {"ADO_APP_RAW": "Greenfield Tool", "FEATURE_COUNT": 2, "FIRST_SEEN": "2025-01-03", "LAST_SEEN": "2025-02-03"},
                ]
            )
        if "GROUP BY AF.APP_NAME_RAW" in text and "EFFORT_POINTS_SUM" in text:
            return pd.DataFrame(
                [
                    {"ADO_APP_RAW": "Atlas App", "FEATURE_COUNT_FEATURES": 4, "EFFORT_POINTS_SUM": 20.0, "LAST_SEEN_FEATURES": "2025-02-01"},
                    {"ADO_APP_RAW": "Greenfield Tool", "FEATURE_COUNT_FEATURES": 2, "EFFORT_POINTS_SUM": 5.0, "LAST_SEEN_FEATURES": "2025-02-03"},
                ]
            )
        if "WITH TEAM_EFFORT AS" in text:
            return pd.DataFrame(
                [
                    {"ADO_APP_RAW": "Atlas App", "SUGGESTED_TEAM": "Team A"},
                    {"ADO_APP_RAW": "Greenfield Tool", "SUGGESTED_TEAM": "Team B"},
                ]
            )
        if "FROM MAP_ADO_APP_TO_TCO_GROUP MA" in text:
            return pd.DataFrame(
                [
                    {
                        "ADO_APP": "Atlas App",
                        "APP_GROUP": "G1",
                        "GROUPNAME": "Atlas",
                        "TEAMID": "T1",
                        "IS_BASE": 0,
                        "DEFAULT_VENDORID": "V1",
                        "VENDORNAME": "Contoso",
                    }
                ]
            )
        if "FROM TEAMS" in text:
            return pd.DataFrame([{"TEAMID": "T1", "TEAMNAME": "Team A"}, {"TEAMID": "T2", "TEAMNAME": "Team B"}])
        if "IS_BASE = 1" in text:
            return pd.DataFrame([{"TEAMID": "T1"}])
        if "FROM APPLICATION_GROUPS G" in text:
            return pd.DataFrame([{"GROUPNAME": "Atlas", "VENDORID": "V1", "VENDORNAME": "Contoso"}])
        return _empty_df()

    out = build_next_app_candidates_df(fake_fetch, years=[2025], programs=[], teams=[])
    assert not out.empty
    assert {"ADO_APP_RAW", "status", "is_existing_app", "needs_vendor", "next_app_display_name", "vendor"}.issubset(set(out.columns))

    atlas = out.loc[out["ADO_APP_RAW"] == "Atlas App"].iloc[0]
    green = out.loc[out["ADO_APP_RAW"] == "Greenfield Tool"].iloc[0]

    assert atlas["status"] == "Mapped"
    assert bool(atlas["is_existing_app"]) is True
    assert green["status"] == "Unmapped"
    assert bool(green["is_existing_app"]) is False


def test_safe_onboard_next_apps_creates_group_vendor_and_maps():
    state = {
        "vendors": [],
        "groups": [],
        "mapped": [],
        "ensure_called": 0,
    }

    def fake_fetch(sql, params=None):
        text = " ".join(str(sql).upper().split())
        if "SELECT VENDORID, VENDORNAME FROM VENDORS" in text:
            return pd.DataFrame(state["vendors"])
        if "SELECT GROUPID, GROUPNAME, TEAMID FROM APPLICATION_GROUPS" in text:
            return pd.DataFrame(state["groups"])
        if "SELECT TOP 1 PRODUCTOWNER FROM TEAMS WHERE TEAMID=%S" in text:
            return pd.DataFrame([{"PRODUCTOWNER": "po@example.com"}])
        return _empty_df()

    def fake_upsert_vendor(vendor_id, vendor_name, updated_by=None):
        state["vendors"].append({"VENDORID": vendor_id, "VENDORNAME": vendor_name})

    def fake_upsert_group(group_id, group_name, team_id="", default_vendor_id=None, owner=None, is_base=False, updated_by=None):
        state["groups"].append({"GROUPID": group_id, "GROUPNAME": group_name, "TEAMID": team_id})

    def fake_map(raws, group_id):
        for r in raws:
            state["mapped"].append((r, group_id))

    def fake_ensure_defaults(execute, fetch_df):
        state["ensure_called"] += 1
        return None

    df = pd.DataFrame(
        [
            {
                "ADO_APP_RAW": "Greenfield Tool",
                "next_app_display_name": "Greenfield",
                "vendor": "New Vendor",
                "is_base_pool": False,
                "create_new_app": True,
                "existing_group_id": "",
                "suggested_team_id": "T1",
            }
        ]
    )

    result = safe_onboard_next_apps(
        fake_fetch,
        lambda sql, params=None, many=False: None,
        fake_upsert_vendor,
        fake_upsert_group,
        fake_map,
        fake_ensure_defaults,
        df,
        updated_by="tester@example.com",
    )

    assert result.mapped_rows == 1
    assert result.created_groups == 1
    assert result.created_vendors == 1
    assert result.skipped_rows == 0
    assert len(state["mapped"]) == 1
    assert state["ensure_called"] == 1


def test_safe_onboard_next_apps_allows_base_pool_without_vendor():
    state = {
        "vendors": [],
        "groups": [],
        "mapped": [],
        "ensure_called": 0,
    }

    def fake_fetch(sql, params=None):
        text = " ".join(str(sql).upper().split())
        if "SELECT VENDORID, VENDORNAME FROM VENDORS" in text:
            return pd.DataFrame(state["vendors"])
        if "SELECT GROUPID, GROUPNAME, TEAMID FROM APPLICATION_GROUPS" in text:
            return pd.DataFrame(state["groups"])
        if "SELECT TOP 1 PRODUCTOWNER FROM TEAMS WHERE TEAMID=%S" in text:
            return pd.DataFrame([{"PRODUCTOWNER": "po@example.com"}])
        return _empty_df()

    def fake_upsert_vendor(vendor_id, vendor_name, updated_by=None):
        state["vendors"].append({"VENDORID": vendor_id, "VENDORNAME": vendor_name})

    def fake_upsert_group(group_id, group_name, team_id="", default_vendor_id=None, owner=None, is_base=False, updated_by=None):
        state["groups"].append({"GROUPID": group_id, "GROUPNAME": group_name, "TEAMID": team_id, "DEFAULT_VENDORID": default_vendor_id})

    def fake_map(raws, group_id):
        for r in raws:
            state["mapped"].append((r, group_id))

    def fake_ensure_defaults(execute, fetch_df):
        state["ensure_called"] += 1
        return None

    df = pd.DataFrame(
        [
            {
                "ADO_APP_RAW": "Shared Unplanned",
                "next_app_display_name": "Shared Base Pool",
                "vendor": "",
                "is_base_pool": True,
                "create_new_app": True,
                "existing_group_id": "",
                "suggested_team_id": "T1",
            }
        ]
    )

    result = safe_onboard_next_apps(
        fake_fetch,
        lambda sql, params=None, many=False: None,
        fake_upsert_vendor,
        fake_upsert_group,
        fake_map,
        fake_ensure_defaults,
        df,
        updated_by="tester@example.com",
    )

    assert result.mapped_rows == 1
    assert result.created_groups == 1
    assert result.created_vendors == 0
    assert result.skipped_rows == 0
    assert len(state["mapped"]) == 1
    assert state["ensure_called"] == 1
    assert state["groups"][0]["DEFAULT_VENDORID"] is None


def test_get_application_entity_returns_group_view():
    def fake_fetch(sql, params=None):
        text = " ".join(str(sql).upper().split())
        if "FROM APPLICATION_GROUPS G" in text:
            return pd.DataFrame(
                [
                    {
                        "APPLICATION_ID": "G1",
                        "APPLICATION_NAME": "Atlas",
                        "TEAMID": "T1",
                        "PROGRAMID": "P1",
                        "DEFAULT_VENDORID": "V1",
                        "IS_BASE": 0,
                        "DEFAULT_APPLICATION_INSTANCE_ID": "G1__DEFAULT",
                    }
                ]
            )
        return _empty_df()

    out = get_application_entity(fake_fetch, team_id="T1")
    assert not out.empty
    assert "APPLICATION_ID" in out.columns
    assert "DEFAULT_APPLICATION_INSTANCE_ID" in out.columns
