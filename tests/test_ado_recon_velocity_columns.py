from __future__ import annotations

import pandas as pd

import core.ado_recon as ado_recon


def _sample_row(**overrides) -> dict:
    row = {
        "FEATURE_ID": "1001",
        "TITLE": "Sample feature",
        "STATE": "Active",
        "APP_NAME_RAW": "Sample App",
        "CHANGED_AT": "2025-01-15T00:00:00",
        "PARENT_ID": None,
        "EPIC_ID": None,
        "EPIC_TITLE": None,
        "EPIC_STATE": None,
        "PROGRAM_RAW": "Program Raw",
        "TEAM_RAW": "Team Raw",
        "AREA_LEVEL1_RAW": "A1",
        "AREA_LEVEL2_RAW": "A2",
        "AREA_LEVEL3_RAW": "A3",
        "AREA_LEVEL4_RAW": "A4",
        "AREA_PATH_RAW": "A1\\A2\\A3\\A4",
        "STORY_POINTS": 40.0,
        "TEAMID": "TEAM-1",
        "TEAMNAME": "Team 1",
        "PROGRAMID": "PROG-1",
        "PROGRAMNAME": "Program 1",
        "GROUPNAME": "Group 1",
        "ADO_YEAR": 2025,
        "PI_NUM": 1,
        "PI_LABEL": "2025 I1",
        "SWAG_POINTS": 40.0,
        "IS_SWAG_READY": 1,
        "DERIVED_FTE": 0.8,
        "DERIVED_FTE_FEATURE": 0.8,
        "DERIVED_FTE_FEATURE_VELOCITY": 0.6,
        "IS_MSP_FEATURE": 0,
        "COUNTS_FOR_ROADMAP": 1,
        "MAPPING_STATUS": "MAPPED",
        "INVESTMENT_DIMENSION": "RUN",
        "FEATURE_STATE": "Active",
    }
    row.update(overrides)
    return row


def test_load_explorer_feature_rows_surfaces_velocity_column(monkeypatch):
    monkeypatch.setattr(
        ado_recon,
        "_object_columns",
        lambda _name: {
            "DERIVED_FTE_FEATURE",
            "DERIVED_FTE_FEATURE_VELOCITY",
            "IS_SWAG_READY",
            "IN_SCOPE_FOR_ROADMAP",
        },
    )
    monkeypatch.setattr(
        ado_recon,
        "fetch_df",
        lambda sql, params=None: pd.DataFrame([_sample_row(DERIVED_FTE_FEATURE=1.25, DERIVED_FTE_FEATURE_VELOCITY=0.95)]),
    )
    monkeypatch.setattr(ado_recon, "apply_display_scope_names", lambda df: df)

    out = ado_recon.load_explorer_feature_rows(years=[2025], cache_bust=101)

    assert not out.empty
    assert "DERIVED_FTE_FEATURE" in out.columns
    assert "DERIVED_FTE_FEATURE_VELOCITY" in out.columns
    assert float(out.iloc[0]["DERIVED_FTE"]) == 1.25
    assert float(out.iloc[0]["DERIVED_FTE_FEATURE"]) == 1.25
    assert float(out.iloc[0]["DERIVED_FTE_FEATURE_VELOCITY"]) == 0.95


def test_load_explorer_feature_rows_velocity_falls_back_to_canonical_expression(monkeypatch):
    monkeypatch.setenv("TCO_EXPLORER_VELOCITY_FROM_SNAPSHOT", "0")
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        ado_recon,
        "_object_columns",
        lambda _name: {
            "DERIVED_FTE_FEATURE",
            "IS_SWAG_READY",
            "IN_SCOPE_FOR_ROADMAP",
        },
    )

    def _fake_fetch(sql, params=None):
        captured["sql"] = str(sql)
        return pd.DataFrame(
            [
                _sample_row(
                    ADO_YEAR=2026,
                    PI_NUM=2,
                    PI_LABEL="2026 I2",
                    DERIVED_FTE_FEATURE=1.1,
                    DERIVED_FTE=1.1,
                    DERIVED_FTE_FEATURE_VELOCITY=None,
                )
            ]
        )

    monkeypatch.setattr(ado_recon, "fetch_df", _fake_fetch)
    monkeypatch.setattr(ado_recon, "apply_display_scope_names", lambda df: df)

    out = ado_recon.load_explorer_feature_rows(years=[2026], cache_bust=102)
    normalized = " ".join(captured.get("sql", "").upper().split())

    assert "TRY_CONVERT(FLOAT, D.DERIVED_FTE_FEATURE) AS DERIVED_FTE_FEATURE_VELOCITY" in normalized
    assert not out.empty
    assert float(out.iloc[0]["DERIVED_FTE"]) == 1.1
    assert float(out.iloc[0]["DERIVED_FTE_FEATURE_VELOCITY"]) == 1.1


def test_load_explorer_feature_rows_velocity_enriched_from_snapshot_lookup(monkeypatch):
    monkeypatch.setenv("TCO_EXPLORER_VELOCITY_FROM_SNAPSHOT", "1")
    monkeypatch.setattr(
        ado_recon,
        "_object_columns",
        lambda _name: {
            "DERIVED_FTE_FEATURE",
            "DERIVED_FTE_FEATURE_VELOCITY",
            "IS_SWAG_READY",
            "IN_SCOPE_FOR_ROADMAP",
        },
    )

    def _fake_fetch(sql, params=None):
        s = " ".join(str(sql).upper().split())
        if "FROM TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            return pd.DataFrame(
                [
                    {
                        "TEAMID": "TEAM-1",
                        "YEAR": 2025,
                        "PI": 1,
                        "EFFECTIVE_BASELINE_POINTS": 80.0,
                    }
                ]
            )
        return pd.DataFrame(
            [
                _sample_row(
                    TEAMID="TEAM-1",
                    ADO_YEAR=2025,
                    PI_NUM=1,
                    SWAG_POINTS=40.0,
                    DERIVED_FTE=1.25,
                    DERIVED_FTE_FEATURE=1.25,
                    DERIVED_FTE_FEATURE_VELOCITY=1.25,
                )
            ]
        )

    monkeypatch.setattr(ado_recon, "fetch_df", _fake_fetch)
    monkeypatch.setattr(ado_recon, "apply_display_scope_names", lambda df: df)

    out = ado_recon.load_explorer_feature_rows(years=[2025], cache_bust=103, include_velocity_column=True)

    assert not out.empty
    assert float(out.iloc[0]["DERIVED_FTE_FEATURE"]) == 1.25
    # 40 swag / 80 baseline = 0.5 velocity-derived FTE
    assert float(out.iloc[0]["DERIVED_FTE_FEATURE_VELOCITY"]) == 0.5
