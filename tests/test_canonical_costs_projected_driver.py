from __future__ import annotations

import pandas as pd

from core import canonical_costs as cc


def _norm_sql(sql: str) -> str:
    return " ".join(str(sql or "").upper().split())


def test_projected_sql_uses_swag_by_default_without_profile_or_env(monkeypatch):
    monkeypatch.delenv("TCO_PROJECTED_FTE_DRIVER", raising=False)
    monkeypatch.delenv("TCO_PROJECTED_VELOCITY_ROW_FALLBACK", raising=False)

    sql, params = cc._projected_cost_lines_sql({"year": [2026], "pi": [1]})
    normalized = _norm_sql(sql)

    assert "TCO_TEAM_VELOCITY_SNAPSHOT" not in normalized
    assert "CROSS JOIN GLOBAL_BASELINE" not in normalized
    assert "SUM(COALESCE(TRY_CONVERT(FLOAT, D.DERIVED_FTE_FEATURE), 0.0)) AS FLOAT" in normalized
    assert isinstance(params, list)


def test_projected_sql_uses_snapshot_when_env_requests_it(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SNAPSHOT_VELOCITY")
    monkeypatch.delenv("TCO_PROJECTED_VELOCITY_ROW_FALLBACK", raising=False)

    sql, _ = cc._projected_cost_lines_sql({"year": [2026], "pi": [1]})
    normalized = _norm_sql(sql)

    assert "TCO_TEAM_VELOCITY_SNAPSHOT" in normalized
    assert "SWAG_POINTS" in normalized
    assert "CROSS JOIN GLOBAL_BASELINE" in normalized
    assert "LEFT JOIN SNAPSHOT SV" in normalized
    assert "NULLIF(COALESCE(SV.EFFECTIVE_BASELINE_POINTS, GB.GLOBAL_BASELINE_POINTS), 0.0)" in normalized


def test_projected_sql_can_fallback_to_legacy_swag_mode(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SWAG")

    sql, _ = cc._projected_cost_lines_sql({"year": [2026], "pi": [1]})
    normalized = _norm_sql(sql)

    assert "TCO_TEAM_VELOCITY_SNAPSHOT" not in normalized
    assert "CROSS JOIN GLOBAL_BASELINE" not in normalized
    assert "SUM(COALESCE(TRY_CONVERT(FLOAT, D.DERIVED_FTE_FEATURE), 0.0)) AS FLOAT" in normalized


def test_projected_sql_snapshot_mode_supports_strict_no_row_fallback(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SNAPSHOT_VELOCITY")
    monkeypatch.setenv("TCO_PROJECTED_VELOCITY_ROW_FALLBACK", "0")

    sql, _ = cc._projected_cost_lines_sql({"year": [2026], "pi": [1]})
    normalized = _norm_sql(sql)

    assert "NULLIF(SV.EFFECTIVE_BASELINE_POINTS, 0.0)" in normalized
    assert "COALESCE(TRY_CONVERT(FLOAT, SV.EFFECTIVE_BASELINE_POINTS), 0.0) > 0" in normalized
    assert "COALESCE(SV.EFFECTIVE_BASELINE_POINTS, GB.GLOBAL_BASELINE_POINTS)" not in normalized


def test_projected_driver_mode_reads_active_profile_settings(monkeypatch):
    monkeypatch.delenv("TCO_PROJECTED_FTE_DRIVER", raising=False)
    monkeypatch.delenv("TCO_PROJECTED_VELOCITY_ROW_FALLBACK", raising=False)

    def fake_fetch(_sql, _params=None):
        return pd.DataFrame(
            [
                {
                    "DRIVER_MODE": "SNAPSHOT_VELOCITY",
                    "ROW_FALLBACK": "false",
                }
            ]
        )

    assert cc._projected_fte_driver_mode(fake_fetch) == "SNAPSHOT_VELOCITY"
    assert cc._projected_velocity_row_fallback_enabled(fake_fetch) is False


def test_projected_driver_mode_defaults_when_profile_query_fails(monkeypatch):
    monkeypatch.delenv("TCO_PROJECTED_FTE_DRIVER", raising=False)
    monkeypatch.delenv("TCO_PROJECTED_VELOCITY_ROW_FALLBACK", raising=False)

    def failing_fetch(_sql, _params=None):
        raise RuntimeError("db unavailable")

    assert cc._projected_fte_driver_mode(failing_fetch) == "SWAG"
    assert cc._projected_velocity_row_fallback_enabled(failing_fetch) is True


def test_cost_lines_sql_prepares_snapshot_table_in_snapshot_mode(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SNAPSHOT_VELOCITY")
    calls: list[str] = []

    monkeypatch.setattr(cc, "_ensure_projected_velocity_snapshot_ready", lambda: calls.append("called"))

    cc._cost_lines_sql(lambda _sql, _params=None: pd.DataFrame(), "Projected", {})
    assert calls == ["called"]


def test_cost_lines_sql_skips_snapshot_guard_in_legacy_mode(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SWAG")
    calls: list[str] = []

    monkeypatch.setattr(cc, "_ensure_projected_velocity_snapshot_ready", lambda: calls.append("called"))

    cc._cost_lines_sql(lambda _sql, _params=None: pd.DataFrame(), "Projected", {})
    assert calls == []


def test_projected_sql_pushes_program_team_scope_into_demand_extract(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SNAPSHOT_VELOCITY")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE", "0")
    monkeypatch.delenv("TCO_PROJECTED_VELOCITY_ROW_FALLBACK", raising=False)

    sql, _ = cc._projected_cost_lines_sql(
        {
            "year": [2026],
            "pi": [1],
            "program": ["Program Alpha"],
            "team": ["Team A"],
        }
    )
    normalized = _norm_sql(sql)

    assert "D.PROGRAMID IN (SELECT P.PROGRAMID FROM PROGRAMS P WHERE UPPER(P.PROGRAMNAME) IN" in normalized
    assert "D.TEAMID IN (SELECT T.TEAMID FROM TEAMS T WHERE UPPER(T.TEAMNAME) IN" in normalized


def test_projected_sql_prefers_program_team_ids_for_demand_scope(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SNAPSHOT_VELOCITY")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE", "0")
    monkeypatch.delenv("TCO_PROJECTED_VELOCITY_ROW_FALLBACK", raising=False)

    sql, _ = cc._projected_cost_lines_sql(
        {
            "year": [2026],
            "pi": [1],
            "program": ["Program Alpha"],
            "team": ["Team A"],
            "program_id": ["P1"],
            "team_id": ["T1"],
        }
    )
    normalized = _norm_sql(sql)

    assert "D.PROGRAMID IN" in normalized
    assert "D.TEAMID IN" in normalized
    assert "UPPER(D.PROGRAMNAME) IN" not in normalized
    assert "UPPER(D.TEAMNAME) IN" not in normalized


def test_resolve_scope_filters_adds_program_team_ids():
    def fake_fetch(sql, _params=None):
        sql_up = " ".join(str(sql).upper().split())
        if "FROM PROGRAMS" in sql_up:
            return pd.DataFrame(
                [
                    {"ENTITY_ID": "P1", "RAW_NAME": "Program Alpha", "DISPLAY_NAME": "Program A"},
                    {"ENTITY_ID": "P2", "RAW_NAME": "Program Beta", "DISPLAY_NAME": "Program B"},
                ]
            )
        if "FROM TEAMS" in sql_up:
            return pd.DataFrame(
                [
                    {"ENTITY_ID": "T1", "RAW_NAME": "Team A", "DISPLAY_NAME": "Team One"},
                    {"ENTITY_ID": "T2", "RAW_NAME": "Team B", "DISPLAY_NAME": "Team Two"},
                ]
            )
        return pd.DataFrame()

    out = cc._resolve_scope_filters_with_aliases(
        fake_fetch,
        {"program": ["Program Alpha"], "team": ["Team One"]},
    )

    assert out.get("program_id") == ["P1"]
    assert out.get("team_id") == ["T1"]


def test_resolve_scope_filters_ids_fallback_when_display_columns_missing():
    def fake_fetch(sql, _params=None):
        sql_up = " ".join(str(sql).upper().split())
        if "FROM PROGRAMS" in sql_up:
            if "PROGRAM_DISPLAY_NAME" in sql_up:
                raise RuntimeError("invalid column PROGRAM_DISPLAY_NAME")
            return pd.DataFrame(
                [
                    {"ENTITY_ID": "P1", "RAW_NAME": "Program Alpha", "DISPLAY_NAME": None},
                ]
            )
        if "FROM TEAMS" in sql_up:
            if "TEAM_DISPLAY_NAME" in sql_up:
                raise RuntimeError("invalid column TEAM_DISPLAY_NAME")
            return pd.DataFrame(
                [
                    {"ENTITY_ID": "T1", "RAW_NAME": "Team A", "DISPLAY_NAME": None},
                ]
            )
        return pd.DataFrame()

    out = cc._resolve_scope_filters_with_aliases(
        fake_fetch,
        {"program": ["Program Alpha"], "team": ["Team A"]},
    )

    assert out.get("program_id") == ["P1"]
    assert out.get("team_id") == ["T1"]


def test_projected_sql_prefers_persisted_demand_snapshot_when_available(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SNAPSHOT_VELOCITY")
    monkeypatch.setenv("TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG", "0")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED", "1")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH", "0")

    def fake_fetch(sql, _params=None):
        sql_up = " ".join(str(sql).upper().split())
        if "SELECT TOP 1 1 AS OK" in sql_up and "TCO_PROJECTED_DEMAND_SNAPSHOT" in sql_up:
            return pd.DataFrame([{"OK": 1}])
        if "FROM ADO_PROFILES" in sql_up:
            return pd.DataFrame()
        return pd.DataFrame()

    sql, _ = cc._projected_cost_lines_sql(
        {"year": [2026], "pi": [1]},
        db=fake_fetch,
    )
    normalized = _norm_sql(sql)

    assert "TCO_PROJECTED_DEMAND_SNAPSHOT" in normalized
    assert "FROM" in normalized and "DS" in normalized
    assert "COALESCE(TRY_CONVERT(FLOAT, DS.SWAG_POINTS_SUM), 0.0) > 0" in normalized


def test_projected_sql_swag_mode_can_use_persisted_demand_snapshot(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SWAG")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED", "1")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH", "0")

    def fake_fetch(sql, _params=None):
        sql_up = " ".join(str(sql).upper().split())
        if "SELECT TOP 1 1 AS OK" in sql_up and "TCO_PROJECTED_DEMAND_SNAPSHOT" in sql_up:
            return pd.DataFrame([{"OK": 1}])
        if "FROM ADO_PROFILES" in sql_up:
            return pd.DataFrame()
        return pd.DataFrame()

    sql, _ = cc._projected_cost_lines_sql(
        {"year": [2026], "pi": [1]},
        db=fake_fetch,
    )
    normalized = _norm_sql(sql)

    assert "TCO_PROJECTED_DEMAND_SNAPSHOT" in normalized
    assert "FROM" in normalized and "DS" in normalized
    assert "COALESCE(TRY_CONVERT(FLOAT, DS.DERIVED_FTE_SUM), 0.0) > 0" in normalized


def test_projected_sql_forces_snapshot_on_team_scope_when_strict_enabled(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SWAG")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED", "1")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE", "1")

    monkeypatch.setattr(cc, "_projected_demand_snapshot_available", lambda *args, **kwargs: False)

    sql, _ = cc._projected_cost_lines_sql(
        {"year": [2026], "pi": [1], "team": ["Team A"]},
        db=lambda _sql, _params=None: pd.DataFrame(),
    )
    normalized = _norm_sql(sql)

    assert "TCO_PROJECTED_DEMAND_SNAPSHOT" in normalized
    assert "VW_TCO_FEATURE_DEMAND" not in normalized


def test_projected_sql_team_scope_can_fallback_when_strict_disabled(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SWAG")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED", "1")
    monkeypatch.setenv("TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE", "0")

    monkeypatch.setattr(cc, "_projected_demand_snapshot_available", lambda *args, **kwargs: False)

    sql, _ = cc._projected_cost_lines_sql(
        {"year": [2026], "pi": [1], "team": ["Team A"]},
        db=lambda _sql, _params=None: pd.DataFrame(),
    )
    normalized = _norm_sql(sql)

    assert "TCO_PROJECTED_DEMAND_SNAPSHOT" not in normalized
    assert "VW_TCO_FEATURE_DEMAND" in normalized


def test_projected_sql_velocity_failopen_to_swag_when_snapshot_unavailable(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SNAPSHOT_VELOCITY")
    monkeypatch.setenv("TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG", "1")

    monkeypatch.setattr(cc, "_projected_velocity_snapshot_available", lambda *args, **kwargs: False)

    sql, _ = cc._projected_cost_lines_sql(
        {"year": [2026], "pi": [1]},
        db=lambda _sql, _params=None: pd.DataFrame(),
    )
    normalized = _norm_sql(sql)

    assert "TCO_TEAM_VELOCITY_SNAPSHOT" not in normalized
    assert "DERIVED_FTE_FEATURE" in normalized


def test_projected_sql_velocity_failopen_can_be_disabled(monkeypatch):
    monkeypatch.setenv("TCO_PROJECTED_FTE_DRIVER", "SNAPSHOT_VELOCITY")
    monkeypatch.setenv("TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG", "0")

    monkeypatch.setattr(cc, "_projected_velocity_snapshot_available", lambda *args, **kwargs: False)

    sql, _ = cc._projected_cost_lines_sql(
        {"year": [2026], "pi": [1]},
        db=lambda _sql, _params=None: pd.DataFrame(),
    )
    normalized = _norm_sql(sql)

    assert "TCO_TEAM_VELOCITY_SNAPSHOT" in normalized
