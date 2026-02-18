from __future__ import annotations

import math

import pandas as pd

from core.insights_cost_scope import (
    build_initiative_feature_detail_df,
    build_initiative_plot_df,
    resolve_show_default,
    rollup_scope_plot_df,
)


def test_resolve_show_default_by_scope_granularity() -> None:
    assert resolve_show_default([], [], []) == "Programs"
    assert resolve_show_default(["P1"], [], []) == "Teams"
    assert resolve_show_default(["P1"], ["T1"], []) == "Applications"
    assert resolve_show_default(["P1"], [], ["G1"]) == "Applications"
    assert resolve_show_default([], ["T1"], ["G1"]) == "Applications"


def test_rollup_scope_programs_and_teams() -> None:
    app_df = pd.DataFrame(
        [
            {
                "GROUPNAME": "G1",
                "TEAMNAME": "T1",
                "PROGRAMNAME": "P1",
                "DEMAND_AXIS": 2.0,
                "ACTUAL_COST": 100.0,
                "BENEFITS_SELECTED": 250.0,
                "Y_VALUE_PLOT": 2.5,
                "IS_BASE": 0,
            },
            {
                "GROUPNAME": "G2",
                "TEAMNAME": "T1",
                "PROGRAMNAME": "P1",
                "DEMAND_AXIS": 3.0,
                "ACTUAL_COST": 200.0,
                "BENEFITS_SELECTED": 300.0,
                "Y_VALUE_PLOT": 1.5,
                "IS_BASE": 0,
            },
        ]
    )
    teams = rollup_scope_plot_df(app_df, "Teams")
    assert len(teams.index) == 1
    t1 = teams.iloc[0]
    assert str(t1["ENTITY_LABEL"]) == "T1"
    assert math.isclose(float(t1["DEMAND_AXIS"]), 5.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(t1["ACTUAL_COST"]), 300.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(t1["Y_VALUE_PLOT"]), 550.0 / 300.0, rel_tol=1e-9, abs_tol=1e-9)

    programs = rollup_scope_plot_df(app_df, "Programs")
    assert len(programs.index) == 1
    p1 = programs.iloc[0]
    assert str(p1["ENTITY_LABEL"]) == "P1"
    assert math.isclose(float(p1["DEMAND_AXIS"]), 5.0, rel_tol=1e-9, abs_tol=1e-9)


def test_initiative_allocation_conserves_group_cost_and_handles_zero_demand_fallback() -> None:
    ado_curr = pd.DataFrame(
        [
            {"FEATURE_ID": 1, "GROUPNAME": "G1", "PROGRAMNAME": "P1", "TEAMNAME": "T1", "TITLE": "F1", "EPIC_ID": 10, "EPIC_TITLE": "E1", "BENEFIT_SELECTED": 100.0, "IS_BASE": 0},
            {"FEATURE_ID": 2, "GROUPNAME": "G1", "PROGRAMNAME": "P1", "TEAMNAME": "T1", "TITLE": "F2", "EPIC_ID": 10, "EPIC_TITLE": "E1", "BENEFIT_SELECTED": 80.0, "IS_BASE": 0},
            {"FEATURE_ID": 3, "GROUPNAME": "G2", "PROGRAMNAME": "P1", "TEAMNAME": "T2", "TITLE": "F3", "EPIC_ID": None, "EPIC_TITLE": "", "BENEFIT_SELECTED": 30.0, "IS_BASE": 0},
            {"FEATURE_ID": 4, "GROUPNAME": "G2", "PROGRAMNAME": "P1", "TEAMNAME": "T2", "TITLE": "F4", "EPIC_ID": None, "EPIC_TITLE": "", "BENEFIT_SELECTED": 10.0, "IS_BASE": 0},
        ]
    )
    cost_curr = pd.DataFrame(
        [
            {"GROUPNAME": "G1", "AMOUNT": 100.0},
            {"GROUPNAME": "G2", "AMOUNT": 60.0},
        ]
    )
    df_explorer = pd.DataFrame(
        [
            {"FEATURE_ID": 1, "GROUPNAME": "G1", "PROGRAMNAME": "P1", "TEAMNAME": "T1", "DERIVED_FTE": 2.0, "EPIC_ID": 10, "EPIC_TITLE": "E1", "FEATURE_TITLE": "F1"},
            {"FEATURE_ID": 2, "GROUPNAME": "G1", "PROGRAMNAME": "P1", "TEAMNAME": "T1", "DERIVED_FTE": 1.0, "EPIC_ID": 10, "EPIC_TITLE": "E1", "FEATURE_TITLE": "F2"},
            {"FEATURE_ID": 3, "GROUPNAME": "G2", "PROGRAMNAME": "P1", "TEAMNAME": "T2", "DERIVED_FTE": 0.0, "EPIC_ID": None, "EPIC_TITLE": "", "FEATURE_TITLE": "F3"},
            {"FEATURE_ID": 4, "GROUPNAME": "G2", "PROGRAMNAME": "P1", "TEAMNAME": "T2", "DERIVED_FTE": 0.0, "EPIC_ID": None, "EPIC_TITLE": "", "FEATURE_TITLE": "F4"},
        ]
    )

    detail = build_initiative_feature_detail_df(ado_curr, cost_curr, df_explorer)
    assert not detail.empty
    by_group = detail.groupby("GROUPNAME", dropna=False)["ALLOCATED_COST"].sum().to_dict()
    assert math.isclose(float(by_group.get("G1", 0.0)), 100.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(by_group.get("G2", 0.0)), 60.0, rel_tol=1e-9, abs_tol=1e-9)

    g2_rows = detail[detail["GROUPNAME"].eq("G2")]
    assert len(g2_rows.index) == 2
    assert all(math.isclose(float(v), 30.0, rel_tol=1e-9, abs_tol=1e-9) for v in g2_rows["ALLOCATED_COST"].tolist())

    initiatives = build_initiative_plot_df(ado_curr, cost_curr, df_explorer)
    assert not initiatives.empty
    assert initiatives["ENTITY_LABEL"].astype(str).str.contains("E1").any()
