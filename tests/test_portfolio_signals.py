from __future__ import annotations

import pandas as pd

from core.portfolio_signals import (
    build_delivery_signals,
    build_financial_signals,
    build_signal_summary,
    build_top_signals_table,
    make_signal,
    rank_signals,
)


def test_financial_signals_are_program_level_only() -> None:
    apptio_df = pd.DataFrame(
        [
            {"EFFECTIVE_NWF_TYPE": "Invoices", "AMOUNT": 300.0, "MONTH_KEY": "2025-01"},
            {"EFFECTIVE_NWF_TYPE": "Azure", "AMOUNT": 100.0, "MONTH_KEY": "2025-01"},
            {"EFFECTIVE_NWF_TYPE": "Azure", "AMOUNT": 100.0, "MONTH_KEY": "2025-02"},
            {"EFFECTIVE_NWF_TYPE": "Azure", "AMOUNT": 100.0, "MONTH_KEY": "2025-03"},
            {"EFFECTIVE_NWF_TYPE": "Azure", "AMOUNT": 120.0, "MONTH_KEY": "2025-04"},
            {"EFFECTIVE_NWF_TYPE": "Azure", "AMOUNT": 120.0, "MONTH_KEY": "2025-05"},
            {"EFFECTIVE_NWF_TYPE": "Azure", "AMOUNT": 120.0, "MONTH_KEY": "2025-06"},
        ]
    )

    signals, meta = build_financial_signals(apptio_df, programs_used=["Program A"])

    assert meta["financial_available"] is True
    assert signals
    assert {s.get("id") for s in signals}.issuperset({"nwf_invoices_share", "azure_qoq_actual"})
    assert all(str(s.get("level")) == "Program" for s in signals)


def test_financial_signals_unavailable_without_program_scope() -> None:
    apptio_df = pd.DataFrame([{"EFFECTIVE_NWF_TYPE": "Invoices", "AMOUNT": 100.0, "MONTH_KEY": "2025-01"}])
    signals, meta = build_financial_signals(apptio_df, programs_used=[])

    assert signals == []
    assert meta["financial_available"] is False
    assert meta["reason"] == "no_programs"


def test_rank_and_top_table_sort_risk_first_then_impact() -> None:
    signals = [
        make_signal(
            id="watch_signal",
            level="Program",
            severity="watch",
            icon="↑",
            title="Watch",
            text="watch",
            impact=500.0,
            action="Watch",
            source="delivery",
        ),
        make_signal(
            id="risk_small",
            level="Program",
            severity="risk",
            icon="⚠",
            title="Risk small",
            text="small",
            impact=10.0,
            action="Investigate",
            source="delivery",
        ),
        make_signal(
            id="risk_big",
            level="Program",
            severity="risk",
            icon="⚠",
            title="Risk big",
            text="big",
            impact=100.0,
            action="Investigate",
            source="financial",
        ),
    ]

    ranked = rank_signals(signals)
    assert ranked[0]["id"] == "risk_big"
    assert ranked[1]["id"] == "risk_small"
    assert ranked[2]["id"] == "watch_signal"

    top_df = build_top_signals_table(signals)
    assert str(top_df.iloc[0]["SIGNAL"]).startswith("Risk big")


def test_delivery_signal_level_degrades_application_team_program() -> None:
    cm_expected = pd.DataFrame(
        [
            {"PROGRAMNAME": "P1", "TEAMNAME": "T1", "GROUPNAME": "G1", "AMOUNT": 1000.0, "WF_LAYER2": "SOD"},
            {"PROGRAMNAME": "P1", "TEAMNAME": "T2", "GROUPNAME": "G2", "AMOUNT": 800.0, "WF_LAYER2": "SOD"},
        ]
    )
    cm = {"EXPECTED": cm_expected}
    cap_df = pd.DataFrame(
        [
            {
                "PROGRAMNAME": "P1",
                "TEAMNAME": "T1",
                "ITERATION_LEVEL3": "2025 I2",
                "PI_ORDER": 20252,
                "CAPACITY_SOD_FTE": 10.0,
                "DEMAND_SOD_FTE": 12.0,
                "CONTRACTOR_C_FTE": 4.0,
                "DELIVERY_CAPACITY_FTE": 8.0,
            },
            {
                "PROGRAMNAME": "P1",
                "TEAMNAME": "T1",
                "ITERATION_LEVEL3": "2025 I1",
                "PI_ORDER": 20251,
                "CAPACITY_SOD_FTE": 10.0,
                "DEMAND_SOD_FTE": 9.0,
                "CONTRACTOR_C_FTE": 2.0,
                "DELIVERY_CAPACITY_FTE": 8.0,
            },
        ]
    )
    explorer_df = pd.DataFrame(
        [
            {"PROGRAMNAME": "P1", "TEAMNAME": "T1", "GROUPNAME": "G1", "DERIVED_FTE_EXPLORER": 3.0},
            {"PROGRAMNAME": "P1", "TEAMNAME": "T2", "GROUPNAME": "G2", "DERIVED_FTE_EXPLORER": 2.0},
        ]
    )

    app_signals, _ = build_delivery_signals(cm, cap_df, explorer_df, eff_programs=["P1"], eff_teams=["T1"], eff_groups=["G1", "G2"])
    team_signals, _ = build_delivery_signals(cm, cap_df, explorer_df, eff_programs=["P1"], eff_teams=["T1"], eff_groups=[])

    no_team_cap = cap_df.drop(columns=["TEAMNAME"])
    prog_signals, _ = build_delivery_signals(cm, no_team_cap, explorer_df, eff_programs=["P1"], eff_teams=[], eff_groups=[])

    assert any(s.get("id") == "highest_cost_per_delivered_fte" and s.get("level") == "Application" for s in app_signals)
    assert any(s.get("id") == "highest_cost_per_delivered_fte" and s.get("level") == "Team" for s in team_signals)
    assert any(s.get("id") == "highest_cost_per_delivered_fte" and s.get("level") == "Program" for s in prog_signals)


def test_summary_falls_back_to_delivery_risks_when_no_application_risks() -> None:
    signals = [
        make_signal(
            id="capacity_pressure_last_pi",
            level="Program",
            severity="risk",
            icon="⚠",
            title="Capacity pressure",
            text="risk",
            action="Investigate",
            source="delivery",
        ),
        make_signal(
            id="contractor_mix_shift_last_pi",
            level="Team",
            severity="risk",
            icon="⚠",
            title="Contractor mix",
            text="risk",
            action="Watch",
            source="delivery",
        ),
    ]

    summary = build_signal_summary(signals)
    assert summary["apps_title"] == "Delivery risks"
    assert int(summary["apps_value"]) == 2
