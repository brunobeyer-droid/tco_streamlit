from __future__ import annotations

import math

import pandas as pd

from core.program_maturity import (
    ProgramMaturityBundle,
    build_maturity_risk_signals,
    calculate_program_maturity,
    calculate_program_positioning_maturity,
    calculate_program_maturity_trend,
)


def _bundle_default() -> ProgramMaturityBundle:
    baseline = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program A", "PI": 1, "GROUPNAME": "", "COST_CATEGORY": "NON_WORK_FORCE", "SUBCOMPONENT": "Invoices", "TOTAL_COST": 100.0},
            {"PROGRAMNAME": "Program B", "PI": 1, "GROUPNAME": "", "COST_CATEGORY": "NON_WORK_FORCE", "SUBCOMPONENT": "Invoices", "TOTAL_COST": 200.0},
        ]
    )
    projected = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program A", "PI": 1, "GROUPNAME": "App A1", "COST_CATEGORY": "WORK_FORCE", "SUBCOMPONENT": "Delivery", "TOTAL_COST": 400.0},
            {"PROGRAMNAME": "Program A", "PI": 1, "GROUPNAME": "", "COST_CATEGORY": "WORK_FORCE", "SUBCOMPONENT": "Program", "TOTAL_COST": 100.0},
            {"PROGRAMNAME": "Program A", "PI": 1, "GROUPNAME": "App A1", "COST_CATEGORY": "NON_WORK_FORCE", "SUBCOMPONENT": "Invoices", "TOTAL_COST": 100.0},
            {"PROGRAMNAME": "Program B", "PI": 1, "GROUPNAME": "App B1", "COST_CATEGORY": "WORK_FORCE", "SUBCOMPONENT": "Delivery", "TOTAL_COST": 200.0},
            {"PROGRAMNAME": "Program B", "PI": 1, "GROUPNAME": "", "COST_CATEGORY": "WORK_FORCE", "SUBCOMPONENT": "Program", "TOTAL_COST": 100.0},
            {"PROGRAMNAME": "Program B", "PI": 1, "GROUPNAME": "", "COST_CATEGORY": "NON_WORK_FORCE", "SUBCOMPONENT": "Invoices", "TOTAL_COST": 300.0},
        ]
    )
    actual = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program A", "PI": 1, "GROUPNAME": "(Program-level)", "COST_CATEGORY": "NON_WORK_FORCE", "SUBCOMPONENT": "Invoices", "TOTAL_COST": 130.0},
            {"PROGRAMNAME": "Program B", "PI": 1, "GROUPNAME": "(Program-level)", "COST_CATEGORY": "NON_WORK_FORCE", "SUBCOMPONENT": "Invoices", "TOTAL_COST": 260.0},
        ]
    )

    ado = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program A", "ITERATION_NUM": 1, "STORY_POINTS": 50.0},
            {"PROGRAMNAME": "Program B", "ITERATION_NUM": 1, "STORY_POINTS": 30.0},
        ]
    )
    explorer = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program A", "PI_NUM": 1, "GROUPNAME": "App A1", "DERIVED_FTE_EXPLORER": 8.0, "MAPPING_STATUS": "MAPPED"},
            {"PROGRAMNAME": "Program A", "PI_NUM": 1, "GROUPNAME": "", "DERIVED_FTE_EXPLORER": 2.0, "MAPPING_STATUS": "UNMAPPED_APP_GROUP"},
            {"PROGRAMNAME": "Program B", "PI_NUM": 1, "GROUPNAME": "App B1", "DERIVED_FTE_EXPLORER": 9.0, "MAPPING_STATUS": "MAPPED"},
            {"PROGRAMNAME": "Program B", "PI_NUM": 1, "GROUPNAME": "", "DERIVED_FTE_EXPLORER": 1.0, "MAPPING_STATUS": "UNMAPPED_APP_GROUP"},
        ]
    )
    capacity = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program A", "PI": 1, "CAPACITY_SOD_FTE": 9.0},
            {"PROGRAMNAME": "Program B", "PI": 1, "CAPACITY_SOD_FTE": 11.0},
        ]
    )

    return ProgramMaturityBundle(
        cost_baseline_df=baseline,
        cost_projected_df=projected,
        cost_actual_df=actual,
        ado_df=ado,
        explorer_df=explorer,
        capacity_df=capacity,
        meta={"year": 2026},
    )


def test_program_maturity_scores_within_bounds() -> None:
    df = calculate_program_maturity(_bundle_default())
    assert not df.empty
    for col in [
        "financial_score",
        "transparency_score",
        "demand_score",
        "efficiency_score",
        "planning_score",
        "maturity_score",
    ]:
        assert df[col].between(0.0, 100.0).all()


def test_program_maturity_weighted_score_exactness() -> None:
    df = calculate_program_maturity(_bundle_default())
    row = df.iloc[0]
    expected = (
        0.30 * float(row["financial_score"])
        + 0.20 * float(row["transparency_score"])
        + 0.20 * float(row["demand_score"])
        + 0.20 * float(row["efficiency_score"])
        + 0.10 * float(row["planning_score"])
    )
    assert math.isclose(float(row["maturity_score"]), expected, rel_tol=1e-9, abs_tol=1e-9)


def test_program_maturity_forecast_only_uses_projected_vs_baseline() -> None:
    df = calculate_program_maturity(_bundle_default(), financial_mode="forecast_only")
    assert not df.empty
    row_a = df.loc[df["program"].eq("Program A")].iloc[0]
    row_b = df.loc[df["program"].eq("Program B")].iloc[0]
    assert math.isclose(float(row_a["variance_pct"]), 0.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(row_b["variance_pct"]), 50.0, rel_tol=1e-9, abs_tol=1e-9)
    assert (df["financial_mode_used"] == "forecast_only").all()


def test_program_maturity_actual_based_preserves_existing_financial_logic() -> None:
    df = calculate_program_maturity(_bundle_default(), financial_mode="actual_based")
    assert not df.empty
    row_a = df.loc[df["program"].eq("Program A")].iloc[0]
    row_b = df.loc[df["program"].eq("Program B")].iloc[0]
    assert math.isclose(float(row_a["variance_pct"]), 30.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(row_b["variance_pct"]), 30.0, rel_tol=1e-9, abs_tol=1e-9)
    assert (df["financial_mode_used"] == "actual_based").all()


def test_program_maturity_mode_default_is_forecast_only() -> None:
    df = calculate_program_maturity(_bundle_default())
    assert not df.empty
    row_a = df.loc[df["program"].eq("Program A")].iloc[0]
    assert math.isclose(float(row_a["variance_pct"]), 0.0, rel_tol=1e-9, abs_tol=1e-9)
    assert (df["financial_mode_used"] == "forecast_only").all()


def test_program_maturity_cost_per_sp_normalization_equal_values_returns_neutral() -> None:
    bundle = _bundle_default()
    proj = bundle.cost_projected_df.copy()
    ado = bundle.ado_df.copy()
    ado.loc[ado["PROGRAMNAME"].eq("Program B"), "STORY_POINTS"] = 50.0

    df = calculate_program_maturity(
        ProgramMaturityBundle(
            cost_baseline_df=bundle.cost_baseline_df,
            cost_projected_df=proj,
            cost_actual_df=bundle.cost_actual_df,
            ado_df=ado,
            explorer_df=bundle.explorer_df,
            capacity_df=bundle.capacity_df,
            meta=bundle.meta,
        )
    )
    assert not df.empty
    assert (df["cost_efficiency_score"] == 50.0).all()


def test_program_maturity_missing_scenarios_no_crash() -> None:
    bundle = ProgramMaturityBundle(
        cost_baseline_df=pd.DataFrame(),
        cost_projected_df=_bundle_default().cost_projected_df,
        cost_actual_df=pd.DataFrame(),
        ado_df=_bundle_default().ado_df,
        explorer_df=_bundle_default().explorer_df,
        capacity_df=_bundle_default().capacity_df,
        meta={"year": 2026},
    )
    df = calculate_program_maturity(bundle)
    assert not df.empty
    assert set(["program", "maturity_score", "variance_pct"]).issubset(set(df.columns))


def test_build_maturity_risk_signals_thresholds() -> None:
    maturity_df = pd.DataFrame(
        [
            {
                "program": "P1",
                "variance_pct": 25.0,
                "unmapped_effort_pct": 20.0,
                "overhead_pct": 35.0,
                "transparency_pct": 40.0,
            },
            {
                "program": "P2",
                "variance_pct": 2.0,
                "unmapped_effort_pct": 1.0,
                "overhead_pct": 10.0,
                "transparency_pct": 95.0,
            },
        ]
    )
    trend_df = pd.DataFrame(
        [
            {"program": "P1", "efficiency_delta": -7.0},
            {"program": "P2", "efficiency_delta": 2.0},
        ]
    )
    out = build_maturity_risk_signals(maturity_df, trend_df)
    assert not out.empty
    assert ((out["program"] == "P1") & (out["severity"] == "Red")).any()
    assert ((out["program"] == "P1") & (out["severity"] == "Yellow")).any()
    assert ((out["program"] == "P2") & (out["severity"] == "Green")).any()


def test_program_maturity_trend_output_shape() -> None:
    bundle = _bundle_default()
    # Duplicate rows into PI 2 to enable delta computation.
    projected = pd.concat(
        [bundle.cost_projected_df, bundle.cost_projected_df.assign(PI=2, TOTAL_COST=bundle.cost_projected_df["TOTAL_COST"] * 1.1)],
        ignore_index=True,
    )
    ado = pd.concat(
        [bundle.ado_df, bundle.ado_df.assign(ITERATION_NUM=2, STORY_POINTS=bundle.ado_df["STORY_POINTS"] * 1.05)],
        ignore_index=True,
    )
    explorer = pd.concat(
        [bundle.explorer_df, bundle.explorer_df.assign(PI_NUM=2, DERIVED_FTE_EXPLORER=bundle.explorer_df["DERIVED_FTE_EXPLORER"] * 1.1)],
        ignore_index=True,
    )
    capacity = pd.concat(
        [bundle.capacity_df, bundle.capacity_df.assign(PI=2, CAPACITY_SOD_FTE=bundle.capacity_df["CAPACITY_SOD_FTE"] * 1.05)],
        ignore_index=True,
    )
    trend = calculate_program_maturity_trend(
        ProgramMaturityBundle(
            cost_baseline_df=bundle.cost_baseline_df,
            cost_projected_df=projected,
            cost_actual_df=bundle.cost_actual_df,
            ado_df=ado,
            explorer_df=explorer,
            capacity_df=capacity,
            meta=bundle.meta,
        )
    )
    assert not trend.empty
    assert set(["program", "latest_efficiency_score", "efficiency_delta"]).issubset(set(trend.columns))


def test_program_positioning_maturity_weighted_shares_and_averages() -> None:
    app_group = pd.DataFrame(
        [
            {
                "PROGRAMNAME": "Program A",
                "GROUPNAME": "G1",
                "DEMAND_AXIS": 2.0,
                "Y_VALUE_PLOT": 3.0,
                "ACTUAL_COST": 100.0,
                "quadrant_label": "Invest",
            },
            {
                "PROGRAMNAME": "Program A",
                "GROUPNAME": "G2",
                "DEMAND_AXIS": 4.0,
                "Y_VALUE_PLOT": 1.0,
                "ACTUAL_COST": 100.0,
                "quadrant_label": "Reassess",
            },
            {
                "PROGRAMNAME": "Program B",
                "GROUPNAME": "G3",
                "DEMAND_AXIS": 5.0,
                "Y_VALUE_PLOT": 2.0,
                "ACTUAL_COST": 200.0,
                "quadrant_label": "Optimize",
            },
        ]
    )
    out = calculate_program_positioning_maturity(app_group, spend_col="ACTUAL_COST")
    assert not out.empty
    a = out.loc[out["program"].eq("Program A")].iloc[0]
    assert math.isclose(float(a["weighted_demand_avg"]), 3.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(a["weighted_value_avg"]), 2.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(a["investment_pct"]), 50.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(a["chaos_pct"]), 50.0, rel_tol=1e-9, abs_tol=1e-9)


def test_program_positioning_maturity_clamps_score_bounds() -> None:
    app_group = pd.DataFrame(
        [
            {
                "PROGRAMNAME": "Program C",
                "GROUPNAME": "G4",
                "DEMAND_AXIS": 1.0,
                "Y_VALUE_PLOT": 0.2,
                "ACTUAL_COST": 100.0,
                "quadrant_label": "Reassess",
            }
        ]
    )
    out = calculate_program_positioning_maturity(app_group, spend_col="ACTUAL_COST")
    assert not out.empty
    assert out["maturity_score"].between(0.0, 100.0).all()
    c = out.iloc[0]
    assert math.isclose(float(c["maturity_score"]), 0.0, rel_tol=1e-9, abs_tol=1e-9)


def test_program_positioning_maturity_handles_invalid_input() -> None:
    out = calculate_program_positioning_maturity(pd.DataFrame({"PROGRAMNAME": ["P1"]}))
    assert out.empty
    expected_cols = {
        "program",
        "weighted_value_avg",
        "weighted_demand_avg",
        "investment_pct",
        "maintain_pct",
        "reassess_pct",
        "chaos_pct",
        "maturity_score",
        "total_spend",
        "app_group_count",
    }
    assert expected_cols.issubset(set(out.columns))


def test_program_positioning_maturity_quadrant_bucket_mapping() -> None:
    app_group = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program D", "GROUPNAME": "G1", "DEMAND_AXIS": 1.0, "Y_VALUE_PLOT": 3.0, "ACTUAL_COST": 25.0, "quadrant_label": "Invest"},
            {"PROGRAMNAME": "Program D", "GROUPNAME": "G2", "DEMAND_AXIS": 2.0, "Y_VALUE_PLOT": 2.5, "ACTUAL_COST": 25.0, "quadrant_label": "Optimize"},
            {"PROGRAMNAME": "Program D", "GROUPNAME": "G3", "DEMAND_AXIS": 3.0, "Y_VALUE_PLOT": 1.0, "ACTUAL_COST": 25.0, "quadrant_label": "Monitor"},
            {"PROGRAMNAME": "Program D", "GROUPNAME": "G4", "DEMAND_AXIS": 4.0, "Y_VALUE_PLOT": 0.5, "ACTUAL_COST": 25.0, "quadrant_label": "Reassess"},
        ]
    )
    out = calculate_program_positioning_maturity(app_group)
    d = out.iloc[0]
    assert math.isclose(float(d["investment_pct"]), 25.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(d["maintain_pct"]), 25.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(d["reassess_pct"]), 25.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(d["chaos_pct"]), 25.0, rel_tol=1e-9, abs_tol=1e-9)
