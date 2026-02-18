from __future__ import annotations

import pandas as pd

import db.mssql_backend as mssql


def test_ensure_team_velocity_baseline_view_emits_create_or_alter(monkeypatch):
    statements: list[str] = []

    monkeypatch.setattr(mssql, "ensure_ado_minimal_tables", lambda *args, **kwargs: True)
    monkeypatch.setattr(mssql, "ensure_ado_iteration_calendar_table", lambda *args, **kwargs: True)
    monkeypatch.setattr(mssql, "ensure_team_msp_rate_table", lambda *args, **kwargs: True)
    monkeypatch.setattr(mssql, "execute", lambda sql, *args, **kwargs: statements.append(str(sql)))

    assert mssql.ensure_tco_team_velocity_baseline_view() is True

    assert any(
        "CREATE OR ALTER VIEW" in s.upper() and "VW_TCO_TEAM_VELOCITY_BASELINE" in s.upper()
        for s in statements
    )
    baseline_sql = " ".join(" ".join(statements).upper().split())
    assert "IN ('CLOSED', 'RESOLVED')" in baseline_sql
    assert "DATEADD(DAY, -7, SYSUTCDATETIME())" in baseline_sql


def test_feature_demand_view_joins_velocity_baseline_and_uses_safe_divisor(monkeypatch):
    captured: dict[str, str] = {}
    calls = {"baseline": 0}

    monkeypatch.setattr(mssql, "fetch_df", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(mssql, "ensure_ado_minimal_tables", lambda *args, **kwargs: True)
    monkeypatch.setattr(mssql, "ensure_ado_iteration_calendar_table", lambda *args, **kwargs: True)
    monkeypatch.setattr(mssql, "ensure_team_msp_rate_table", lambda *args, **kwargs: True)

    def _baseline_call(*args, **kwargs):
        calls["baseline"] += 1
        return True

    monkeypatch.setattr(mssql, "ensure_tco_team_velocity_baseline_view", _baseline_call)

    def _capture_view(name: str, body_sql: str) -> None:
        captured[str(name)] = str(body_sql)

    monkeypatch.setattr(mssql, "_create_or_alter_view", _capture_view)

    assert mssql.ensure_tco_feature_demand_view(force=True) is True
    assert calls["baseline"] == 1

    sql = captured.get("VW_TCO_FEATURE_DEMAND", "")
    normalized = " ".join(sql.upper().split())

    assert "VW_TCO_TEAM_VELOCITY_BASELINE" in normalized
    assert "LEFT JOIN" in normalized
    assert "DERIVED_FTE_FEATURE" in normalized
    assert "COALESCE(SWAG_POINTS, 0)" in normalized
    assert "NULLIF(COALESCE(EFFECTIVE_BASELINE_POINTS, 65.0), 0)" in normalized
    assert "TCO_FEATURE_DEMAND_VIEW_V3" in normalized
