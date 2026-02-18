from __future__ import annotations

import math

import pandas as pd

from core.program_strategic_maturity import (
    StrategicMaturityBundle,
    build_program_strategic_drivers,
    build_strategic_maturity_bundle,
    calculate_program_positioning_maturity,
    calculate_program_strategic_maturity,
)


def _position_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "PROGRAMNAME": "Program A",
                "GROUPNAME": "G1",
                "DEMAND_AXIS": 2.0,
                "Y_VALUE_PLOT": 3.0,
                "PROJECTED_SPEND": 100.0,
                "quadrant_label": "Invest",
            },
            {
                "PROGRAMNAME": "Program A",
                "GROUPNAME": "G2",
                "DEMAND_AXIS": 4.0,
                "Y_VALUE_PLOT": 1.0,
                "PROJECTED_SPEND": 300.0,
                "quadrant_label": "Reassess",
            },
            {
                "PROGRAMNAME": "Program B",
                "GROUPNAME": "G3",
                "DEMAND_AXIS": 1.0,
                "Y_VALUE_PLOT": 5.0,
                "PROJECTED_SPEND": 200.0,
                "quadrant_label": "Optimize",
            },
        ]
    )


def _feature_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"PROGRAMNAME": "Program A", "TEAMNAME": "T1", "IS_MSP_FEATURE": 0, "INVESTMENT_DIMENSION": "New Opportunities", "DEMAND_FTE": 60.0},
            {"PROGRAMNAME": "Program A", "TEAMNAME": "T1", "IS_MSP_FEATURE": 0, "INVESTMENT_DIMENSION": "Operating", "DEMAND_FTE": 40.0},
            {"PROGRAMNAME": "Program A", "TEAMNAME": "T2", "IS_MSP_FEATURE": 1, "INVESTMENT_DIMENSION": "Operating", "DEMAND_FTE": 30.0},
            {"PROGRAMNAME": "Program A", "TEAMNAME": "T2", "IS_MSP_FEATURE": 1, "INVESTMENT_DIMENSION": "Product Health", "DEMAND_FTE": 10.0},
            {"PROGRAMNAME": "Program B", "TEAMNAME": "T3", "IS_MSP_FEATURE": 0, "INVESTMENT_DIMENSION": "New Opportunities", "DEMAND_FTE": 40.0},
            {"PROGRAMNAME": "Program B", "TEAMNAME": "T4", "IS_MSP_FEATURE": 1, "INVESTMENT_DIMENSION": "New Opportunities", "DEMAND_FTE": 20.0},
        ]
    )


def test_program_positioning_weighted_shares_and_averages() -> None:
    out = calculate_program_positioning_maturity(_position_df(), spend_col="PROJECTED_SPEND")
    assert not out.empty
    a = out.loc[out["program"].eq("Program A")].iloc[0]
    assert math.isclose(float(a["weighted_demand_avg"]), 3.5, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(a["weighted_value_avg"]), 1.5, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(a["invest_pct"]), 25.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(a["reassess_pct"]), 75.0, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(float(a["positioning_score"]), 0.0, rel_tol=1e-9, abs_tol=1e-9)


def test_program_strategic_scores_are_clamped_to_bounds() -> None:
    bundle = build_strategic_maturity_bundle(_position_df(), _feature_df(), meta={"spend_col": "PROJECTED_SPEND"})
    out = calculate_program_strategic_maturity(bundle)
    assert not out.empty
    for col in ["positioning_score", "roi_efficiency_score", "workforce_alignment_score", "strategic_maturity_score"]:
        assert out[col].between(0.0, 100.0).all()


def test_roi_normalization_equal_values_returns_neutral_50() -> None:
    pos = pd.DataFrame(
        [
            {"PROGRAMNAME": "P1", "GROUPNAME": "G1", "DEMAND_AXIS": 10.0, "Y_VALUE_PLOT": 2.0, "PROJECTED_SPEND": 100.0, "quadrant_label": "Invest"},
            {"PROGRAMNAME": "P2", "GROUPNAME": "G2", "DEMAND_AXIS": 20.0, "Y_VALUE_PLOT": 1.0, "PROJECTED_SPEND": 400.0, "quadrant_label": "Optimize"},
        ]
    )
    feat = pd.DataFrame(
        [
            {"PROGRAMNAME": "P1", "TEAMNAME": "T1", "IS_MSP_FEATURE": 0, "INVESTMENT_DIMENSION": "New Opportunities", "DEMAND_FTE": 1.0},
            {"PROGRAMNAME": "P2", "TEAMNAME": "T2", "IS_MSP_FEATURE": 1, "INVESTMENT_DIMENSION": "Operating", "DEMAND_FTE": 1.0},
        ]
    )
    out = calculate_program_strategic_maturity(build_strategic_maturity_bundle(pos, feat))
    assert not out.empty
    assert (out["roi_efficiency_score"] == 50.0).all()


def test_empty_or_invalid_input_returns_empty_schema() -> None:
    out = calculate_program_strategic_maturity(
        StrategicMaturityBundle(app_group_position_df=pd.DataFrame(), feature_work_df=pd.DataFrame(), meta={})
    )
    assert out.empty
    expected_cols = {
        "program",
        "weighted_demand_avg",
        "weighted_value_avg",
        "total_spend",
        "app_group_count",
        "invest_pct",
        "optimize_pct",
        "monitor_pct",
        "reassess_pct",
        "positioning_score",
        "roi_per_fte",
        "roi_efficiency_score",
        "delivery_newopp_pct",
        "delivery_operating_pct",
        "delivery_prodhealth_pct",
        "msp_operating_pct",
        "msp_newopp_pct",
        "msp_prodhealth_pct",
        "workforce_alignment_score",
        "strategic_maturity_score",
    }
    assert expected_cols.issubset(set(out.columns))


def test_workforce_alignment_penalizes_delivery_operating_bias() -> None:
    pos = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program X", "GROUPNAME": "GX", "DEMAND_AXIS": 5.0, "Y_VALUE_PLOT": 2.0, "PROJECTED_SPEND": 100.0, "quadrant_label": "Optimize"}
        ]
    )
    feat_bad = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program X", "TEAMNAME": "T1", "IS_MSP_FEATURE": 0, "INVESTMENT_DIMENSION": "Operating", "DEMAND_FTE": 80.0},
            {"PROGRAMNAME": "Program X", "TEAMNAME": "T1", "IS_MSP_FEATURE": 0, "INVESTMENT_DIMENSION": "New Opportunities", "DEMAND_FTE": 20.0},
        ]
    )
    feat_good = pd.DataFrame(
        [
            {"PROGRAMNAME": "Program X", "TEAMNAME": "T1", "IS_MSP_FEATURE": 0, "INVESTMENT_DIMENSION": "Operating", "DEMAND_FTE": 10.0},
            {"PROGRAMNAME": "Program X", "TEAMNAME": "T1", "IS_MSP_FEATURE": 0, "INVESTMENT_DIMENSION": "New Opportunities", "DEMAND_FTE": 90.0},
        ]
    )
    bad = calculate_program_strategic_maturity(build_strategic_maturity_bundle(pos, feat_bad))
    good = calculate_program_strategic_maturity(build_strategic_maturity_bundle(pos, feat_good))
    assert float(good.iloc[0]["workforce_alignment_score"]) > float(bad.iloc[0]["workforce_alignment_score"])


def test_final_score_matches_weighted_composition() -> None:
    bundle = build_strategic_maturity_bundle(_position_df(), _feature_df(), meta={"spend_col": "PROJECTED_SPEND"})
    out = calculate_program_strategic_maturity(bundle)
    assert not out.empty
    row = out.iloc[0]
    expected = (
        0.40 * float(row["positioning_score"])
        + 0.30 * float(row["roi_efficiency_score"])
        + 0.30 * float(row["workforce_alignment_score"])
    )
    assert math.isclose(float(row["strategic_maturity_score"]), expected, rel_tol=1e-9, abs_tol=1e-9)


def test_model_output_remains_canonical_quadrant_columns() -> None:
    out = calculate_program_strategic_maturity(build_strategic_maturity_bundle(_position_df(), _feature_df()))
    assert not out.empty
    assert "invest_pct" in out.columns
    assert "optimize_pct" in out.columns
    assert "monitor_pct" in out.columns
    assert "reassess_pct" in out.columns
    assert "grow_pct" not in out.columns
    assert "scale_pct" not in out.columns
    assert "watch_pct" not in out.columns
    assert "stop_pct" not in out.columns


def test_strategic_drivers_rules_trigger() -> None:
    strategic_df = pd.DataFrame(
        [
            {
                "program": "P1",
                "delivery_operating_pct": 30.0,
                "msp_newopp_pct": 2.0,
                "roi_efficiency_score": 55.0,
                "positioning_score": 50.0,
                "workforce_alignment_score": 40.0,
            },
            {
                "program": "P2",
                "delivery_operating_pct": 5.0,
                "msp_newopp_pct": 15.0,
                "roi_efficiency_score": 55.0,
                "positioning_score": 70.0,
                "workforce_alignment_score": 60.0,
            },
        ]
    )
    out = build_program_strategic_drivers(strategic_df)
    assert not out.empty
    p1 = out.loc[out["program"].eq("P1")].iloc[0]
    p2 = out.loc[out["program"].eq("P2")].iloc[0]
    assert "Delivery over-indexed on Operating" in str(p1["biggest_driver"])
    assert "MSP doing too much New Opportunities" in str(p2["biggest_driver"])
