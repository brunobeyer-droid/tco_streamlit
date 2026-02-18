from __future__ import annotations

import datetime as dt

import pandas as pd

import core.ado_recon as ado_recon
import core.velocity_baseline as velocity_baseline


def _calendar_2026() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2025 I4",
                "CAL_START_DATE": "2026-01-14",
                "CAL_END_DATE": "2026-02-11",
            },
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2026 I1",
                "CAL_START_DATE": "2026-02-11",
                "CAL_END_DATE": "2026-05-06",
            },
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2026 I2",
                "CAL_START_DATE": "2026-05-06",
                "CAL_END_DATE": "2026-08-12",
            },
        ]
    )


def test_select_velocity_snapshot_avoids_future_team_pi():
    df = pd.DataFrame(
        [
            {
                "YEAR": 2026,
                "PI": 1,
                "PI_LABEL": "2026 PI1",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 65.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 120.0,
            },
            {
                "YEAR": 2026,
                "PI": 4,
                "PI_LABEL": "2026 PI4",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 95.0,
                "BASELINE_SOURCE": "TEAM",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": 95.0,
                "PI_POINTS_DONE": 140.0,
            },
        ]
    )

    snap = velocity_baseline.select_velocity_snapshot(
        df_velocity=df,
        selected_year=2026,
        as_of=dt.date(2026, 2, 16),
        programs=["R&M"],
        teams=["ERNE WEST"],
        calendar_df=_calendar_2026(),
    )

    assert snap["source"] == "GLOBAL"
    assert float(snap["value"]) == 65.0
    assert "PI1" in str(snap["pi_label"]).upper()


def test_select_velocity_snapshot_program_fallbacks_to_global_same_pi():
    df = pd.DataFrame(
        [
            {
                "YEAR": 2026,
                "PI": 1,
                "PI_LABEL": "2026 PI1",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 65.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 100.0,
            },
            {
                "YEAR": 2026,
                "PI": 1,
                "PI_LABEL": "2026 PI1",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE EAST",
                "EFFECTIVE_BASELINE_POINTS": 65.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 80.0,
            },
            {
                "YEAR": 2026,
                "PI": 4,
                "PI_LABEL": "2026 PI4",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 95.0,
                "BASELINE_SOURCE": "TEAM",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": 95.0,
                "PI_POINTS_DONE": 140.0,
            },
        ]
    )

    snap = velocity_baseline.select_velocity_snapshot(
        df_velocity=df,
        selected_year=2026,
        as_of=dt.date(2026, 2, 16),
        programs=["R&M"],
        teams=[],
        calendar_df=_calendar_2026(),
    )

    assert snap["source"] == "GLOBAL"
    assert float(snap["value"]) == 65.0
    assert "PI1" in str(snap["pi_label"]).upper()


def test_select_velocity_snapshot_anchors_on_last_closed_pi():
    cal = pd.DataFrame(
        [
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2026 I1",
                "CAL_START_DATE": "2026-01-01",
                "CAL_END_DATE": "2026-01-31",
            },
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2026 I2",
                "CAL_START_DATE": "2026-02-01",
                "CAL_END_DATE": "2026-03-31",
            },
        ]
    )
    df = pd.DataFrame(
        [
            {
                "YEAR": 2026,
                "PI": 1,
                "PI_LABEL": "2026 PI1",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 60.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 1,
                "BASELINE_PREV": 50.0,
                "PI_POINTS_DONE": 100.0,
            },
            {
                "YEAR": 2026,
                "PI": 2,
                "PI_LABEL": "2026 PI2",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 70.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 1,
                "BASELINE_PREV": 60.0,
                "PI_POINTS_DONE": 100.0,
            },
        ]
    )

    snap = velocity_baseline.select_velocity_snapshot(
        df_velocity=df,
        selected_year=2026,
        as_of=dt.date(2026, 2, 15),
        programs=["R&M"],
        teams=[],
        calendar_df=cal,
    )

    assert "PI1" in str(snap["pi_label"]).upper()
    assert float(snap["value"]) == 60.0
    assert float(snap["prev"]) == 50.0


def test_select_velocity_snapshot_uses_prior_year_last_closed_pi_for_pi1():
    cal = pd.DataFrame(
        [
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2025 I4",
                "CAL_START_DATE": "2026-01-14",
                "CAL_END_DATE": "2026-02-11",
            },
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2026 I1",
                "CAL_START_DATE": "2026-02-11",
                "CAL_END_DATE": "2026-05-06",
            },
        ]
    )
    df = pd.DataFrame(
        [
            {
                "YEAR": 2025,
                "PI": 4,
                "PI_LABEL": "2025 PI4",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 95.0,
                "BASELINE_SOURCE": "TEAM",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": 95.0,
                "PI_POINTS_DONE": 140.0,
            },
            {
                "YEAR": 2026,
                "PI": 1,
                "PI_LABEL": "2026 PI1",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 65.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 120.0,
            },
        ]
    )

    snap = velocity_baseline.select_velocity_snapshot(
        df_velocity=df,
        selected_year=2026,
        as_of=dt.date(2026, 2, 16),
        programs=["R&M"],
        teams=["ERNE WEST"],
        calendar_df=cal,
    )

    assert snap["source"] == "TEAM"
    assert float(snap["value"]) == 95.0
    assert "2025" in str(snap["pi_label"])
    assert "PI4" in str(snap["pi_label"]).upper()
    assert "2025" in str(snap["reference_pi_label"])


def test_select_velocity_snapshot_uses_prior_year_last_closed_pi_for_any_year_boundary():
    cal = pd.DataFrame(
        [
            {
                "CAL_YEAR": 2027,
                "CAL_ITERATION_LEVEL3": "2026 I4",
                "CAL_START_DATE": "2027-01-10",
                "CAL_END_DATE": "2027-02-08",
            },
            {
                "CAL_YEAR": 2027,
                "CAL_ITERATION_LEVEL3": "2027 I1",
                "CAL_START_DATE": "2027-02-08",
                "CAL_END_DATE": "2027-05-01",
            },
        ]
    )
    df = pd.DataFrame(
        [
            {
                "YEAR": 2026,
                "PI": 4,
                "PI_LABEL": "2026 PI4",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 88.0,
                "BASELINE_SOURCE": "TEAM",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": 84.0,
                "PI_POINTS_DONE": 100.0,
            },
            {
                "YEAR": 2027,
                "PI": 1,
                "PI_LABEL": "2027 PI1",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 61.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 100.0,
            },
        ]
    )

    snap = velocity_baseline.select_velocity_snapshot(
        df_velocity=df,
        selected_year=2027,
        as_of=dt.date(2027, 2, 16),
        programs=["R&M"],
        teams=["ERNE WEST"],
        calendar_df=cal,
    )

    assert snap["source"] == "TEAM"
    assert float(snap["value"]) == 88.0
    assert "2026" in str(snap["pi_label"])
    assert "PI4" in str(snap["pi_label"]).upper()


def test_select_velocity_snapshot_program_scope_falls_back_order_when_reference_has_only_team():
    cal = pd.DataFrame(
        [
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2025 I4",
                "CAL_START_DATE": "2026-01-14",
                "CAL_END_DATE": "2026-02-11",
            },
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2026 I1",
                "CAL_START_DATE": "2026-02-11",
                "CAL_END_DATE": "2026-05-06",
            },
        ]
    )
    df = pd.DataFrame(
        [
            {
                "YEAR": 2025,
                "PI": 4,
                "PI_LABEL": "2025 PI4",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 95.0,
                "BASELINE_SOURCE": "TEAM",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": 95.0,
                "PI_POINTS_DONE": 140.0,
            },
            {
                "YEAR": 2025,
                "PI": 2,
                "PI_LABEL": "2025 PI2",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 65.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 120.0,
            },
        ]
    )

    snap = velocity_baseline.select_velocity_snapshot(
        df_velocity=df,
        selected_year=2026,
        as_of=dt.date(2026, 2, 16),
        programs=["R&M"],
        teams=[],
        calendar_df=cal,
    )

    assert snap["source"] == "GLOBAL"
    assert float(snap["value"]) == 65.0
    assert "2025" in str(snap["pi_label"])
    assert "PI2" in str(snap["pi_label"]).upper()
    assert "global" in str(snap["fallback_chain_used"]).lower()


def test_select_velocity_snapshot_portfolio_scope_falls_back_order_when_reference_has_only_team():
    cal = pd.DataFrame(
        [
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2025 I4",
                "CAL_START_DATE": "2026-01-14",
                "CAL_END_DATE": "2026-02-11",
            },
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2026 I1",
                "CAL_START_DATE": "2026-02-11",
                "CAL_END_DATE": "2026-05-06",
            },
        ]
    )
    df = pd.DataFrame(
        [
            {
                "YEAR": 2025,
                "PI": 4,
                "PI_LABEL": "2025 PI4",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 95.0,
                "BASELINE_SOURCE": "TEAM",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": 95.0,
                "PI_POINTS_DONE": 140.0,
            },
            {
                "YEAR": 2025,
                "PI": 1,
                "PI_LABEL": "2025 PI1",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 65.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 120.0,
            },
        ]
    )

    snap = velocity_baseline.select_velocity_snapshot(
        df_velocity=df,
        selected_year=2026,
        as_of=dt.date(2026, 2, 16),
        programs=[],
        teams=[],
        calendar_df=cal,
    )

    assert snap["source"] == "GLOBAL"
    assert float(snap["value"]) == 65.0
    assert "2025" in str(snap["pi_label"])
    assert "PI1" in str(snap["pi_label"]).upper()
    assert "global" in str(snap["fallback_chain_used"]).lower()


def test_select_velocity_snapshot_program_scope_can_use_global_fallback_outside_program_rows():
    cal = pd.DataFrame(
        [
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2025 I4",
                "CAL_START_DATE": "2026-01-14",
                "CAL_END_DATE": "2026-02-11",
            },
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2026 I1",
                "CAL_START_DATE": "2026-02-11",
                "CAL_END_DATE": "2026-05-06",
            },
        ]
    )
    df = pd.DataFrame(
        [
            {
                "YEAR": 2025,
                "PI": 4,
                "PI_LABEL": "2025 PI4",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 95.0,
                "BASELINE_SOURCE": "TEAM",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": 95.0,
                "PI_POINTS_DONE": 140.0,
            },
            {
                "YEAR": 2025,
                "PI": 2,
                "PI_LABEL": "2025 PI2",
                "PROGRAMNAME": "(Unassigned)",
                "TEAMNAME": "(Unassigned)",
                "EFFECTIVE_BASELINE_POINTS": 65.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 120.0,
            },
        ]
    )

    snap = velocity_baseline.select_velocity_snapshot(
        df_velocity=df,
        selected_year=2026,
        as_of=dt.date(2026, 2, 16),
        programs=["R&M"],
        teams=[],
        calendar_df=cal,
    )

    assert snap["source"] == "GLOBAL"
    assert float(snap["value"]) == 65.0
    assert "PI2" in str(snap["pi_label"]).upper()


def test_load_velocity_baseline_keeps_full_window_without_order_truncation(monkeypatch):
    rows = []
    for year, pi in [(2025, 1), (2025, 2), (2025, 3), (2025, 4), (2026, 1), (2026, 2), (2026, 3), (2026, 4)]:
        rows.append(
            {
                "TEAMID": "T1",
                "PROGRAMID": "P1",
                "YEAR": year,
                "PI": pi,
                "PI_LABEL": f"{year} PI{pi}",
                "PROGRAMNAME": "R&M",
                "TEAMNAME": "ERNE WEST",
                "EFFECTIVE_BASELINE_POINTS": 65.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 100.0,
            }
        )
    base = pd.DataFrame(rows)

    monkeypatch.setattr(ado_recon, "_load_velocity_baseline_window", lambda **kwargs: base.copy())
    monkeypatch.setattr(ado_recon, "apply_display_scope_names", lambda df: df)

    out = ado_recon.load_velocity_baseline(
        year=2026,
        programs=(),
        teams=(),
        groups=(),
        data_version=1,
        include_prior_year=True,
        reference_year_only=False,
    )

    orders = sorted(
        {
            int(r["YEAR"]) * 100 + int(r["PI"])
            for _, r in out[["YEAR", "PI"]].dropna().iterrows()
        }
    )
    assert len(orders) == 8
    assert orders[0] == 202501
    assert orders[-1] == 202604


def test_team_scope_without_program_filter_ignores_unrelated_program_rows():
    cal = pd.DataFrame(
        [
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2025 I4",
                "CAL_START_DATE": "2026-01-14",
                "CAL_END_DATE": "2026-02-11",
            },
            {
                "CAL_YEAR": 2026,
                "CAL_ITERATION_LEVEL3": "2026 I1",
                "CAL_START_DATE": "2026-02-11",
                "CAL_END_DATE": "2026-05-06",
            },
        ]
    )
    df = pd.DataFrame(
        [
            {
                "YEAR": 2025,
                "PI": 2,
                "PI_LABEL": "2025 PI2",
                "PROGRAMNAME": "Program B",
                "TEAMNAME": "Team X",
                "EFFECTIVE_BASELINE_POINTS": 70.0,
                "BASELINE_SOURCE": "TEAM",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": 68.0,
                "PI_POINTS_DONE": 100.0,
            },
            {
                "YEAR": 2025,
                "PI": 4,
                "PI_LABEL": "2025 PI4",
                "PROGRAMNAME": "Program A",
                "TEAMNAME": "Team A",
                "EFFECTIVE_BASELINE_POINTS": 95.0,
                "BASELINE_SOURCE": "PROGRAM",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": 95.0,
                "PI_POINTS_DONE": 130.0,
            },
            {
                "YEAR": 2025,
                "PI": 3,
                "PI_LABEL": "2025 PI3",
                "PROGRAMNAME": "(Unassigned)",
                "TEAMNAME": "(Unassigned)",
                "EFFECTIVE_BASELINE_POINTS": 65.0,
                "BASELINE_SOURCE": "GLOBAL",
                "BASELINE_CHANGED_FLAG": 0,
                "BASELINE_PREV": None,
                "PI_POINTS_DONE": 150.0,
            },
        ]
    )

    snap = velocity_baseline.select_velocity_snapshot(
        df_velocity=df,
        selected_year=2026,
        as_of=dt.date(2026, 2, 16),
        programs=[],
        teams=["Team X"],
        calendar_df=cal,
    )

    assert snap["source"] == "GLOBAL"
    assert float(snap["value"]) == 65.0
    assert "PI3" in str(snap["pi_label"]).upper()


def test_velocity_window_refreshes_snapshot_when_stale(monkeypatch):
    calls: list[int] = []
    refreshed = {"ok": False}

    def _fake_fetch(sql, params=None):
        s = " ".join(str(sql).split()).upper()
        if "COUNT(1) AS ROWS_N" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            if refreshed["ok"]:
                return pd.DataFrame([{"ROWS_N": 10, "MAX_DATA_VERSION": 5, "MIN_DATA_VERSION": 5, "ROWS_BELOW_TARGET": 0}])
            return pd.DataFrame([{"ROWS_N": 10, "MAX_DATA_VERSION": 1, "MIN_DATA_VERSION": 1, "ROWS_BELOW_TARGET": 10}])
        if "FROM" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            return pd.DataFrame(
                [
                    {
                        "TEAMID": "T1",
                        "PROGRAMID": "P1",
                        "YEAR": 2026,
                        "PI": 1,
                        "PI_LABEL": "2026 PI1",
                        "PROGRAMNAME": "R&M",
                        "TEAMNAME": "ERNE WEST",
                        "EFFECTIVE_BASELINE_POINTS": 65.0,
                        "BASELINE_SOURCE": "GLOBAL",
                        "BASELINE_CHANGED_FLAG": 0,
                        "BASELINE_PREV": None,
                        "PI_POINTS_DONE": 120.0,
                    }
                ]
            )
        raise AssertionError(f"Unexpected SQL path: {sql}")

    monkeypatch.setattr(ado_recon, "fetch_df", _fake_fetch)
    monkeypatch.setattr(ado_recon, "ensure_tco_team_velocity_snapshot_table", lambda: True)
    monkeypatch.setattr(ado_recon, "_velocity_source_version_token", lambda: 0)

    def _fake_refresh(**kwargs):
        calls.append(int(kwargs.get("data_version") or 0))
        refreshed["ok"] = True
        return True

    monkeypatch.setattr(ado_recon, "refresh_tco_team_velocity_snapshot", _fake_refresh)

    out = ado_recon._load_velocity_baseline_window_uncached(
        year=2026,
        include_prior_year=False,
        reference_year_only=True,
        data_version=5,
    )

    assert calls == [5]
    assert out is not None and not out.empty
    assert int(out.iloc[0]["YEAR"]) == 2026
    assert int(out.iloc[0]["PI"]) == 1


def test_velocity_scope_filter_matches_ids_and_names(monkeypatch):
    src = pd.DataFrame(
        [
            {"PROGRAMID": "P1", "TEAMID": "T1", "PROGRAMNAME": "R&M", "TEAMNAME": "ERNE WEST", "YEAR": 2026, "PI": 1},
            {"PROGRAMID": "P2", "TEAMID": "T2", "PROGRAMNAME": "CS", "TEAMNAME": "TEAM B", "YEAR": 2026, "PI": 1},
        ]
    )
    group_rows = pd.DataFrame(
        [
            {"GROUPID": "G1", "GROUPNAME": "APP1", "TEAMID": "T1"},
            {"GROUPID": "G2", "GROUPNAME": "APP2", "TEAMID": "T2"},
        ]
    )
    monkeypatch.setattr(ado_recon, "_group_scope_rows", lambda: group_rows)

    out = ado_recon._apply_velocity_scope_filters(
        df=src,
        programs=("R&M",),
        teams=("ERNE WEST",),
        groups=("APP1",),
        program_ids=("P1",),
        team_ids=("T1",),
    )

    assert out is not None and len(out.index) == 1
    assert str(out.iloc[0]["PROGRAMID"]) == "P1"
    assert str(out.iloc[0]["TEAMID"]) == "T1"


def test_velocity_window_refresh_passes_year_scope_to_refresh(monkeypatch):
    calls = {"kwargs": None}
    refreshed = {"ok": False}

    def _fake_fetch(sql, params=None):
        s = " ".join(str(sql).split()).upper()
        if "COUNT(1) AS ROWS_N" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            if refreshed["ok"]:
                return pd.DataFrame([{"ROWS_N": 10, "MAX_DATA_VERSION": 9, "MIN_DATA_VERSION": 9, "ROWS_BELOW_TARGET": 0}])
            return pd.DataFrame([{"ROWS_N": 0, "MAX_DATA_VERSION": 0, "MIN_DATA_VERSION": 0, "ROWS_BELOW_TARGET": 0}])
        if "FROM" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            return pd.DataFrame(
                [
                    {
                        "TEAMID": "T1",
                        "PROGRAMID": "P1",
                        "YEAR": 2026,
                        "PI": 1,
                        "PI_LABEL": "2026 PI1",
                        "PROGRAMNAME": "R&M",
                        "TEAMNAME": "ERNE WEST",
                        "EFFECTIVE_BASELINE_POINTS": 65.0,
                        "BASELINE_SOURCE": "GLOBAL",
                        "BASELINE_CHANGED_FLAG": 0,
                        "BASELINE_PREV": None,
                        "PI_POINTS_DONE": 120.0,
                    }
                ]
            )
        raise AssertionError(f"Unexpected SQL path: {sql}")

    monkeypatch.setattr(ado_recon, "fetch_df", _fake_fetch)
    monkeypatch.setattr(ado_recon, "ensure_tco_team_velocity_snapshot_table", lambda: True)
    monkeypatch.setattr(ado_recon, "_velocity_source_version_token", lambda: 0)

    def _fake_refresh(**kwargs):
        calls["kwargs"] = kwargs
        refreshed["ok"] = True
        return True

    monkeypatch.setattr(ado_recon, "refresh_tco_team_velocity_snapshot", _fake_refresh)

    out = ado_recon._load_velocity_baseline_window_uncached(
        year=2026,
        include_prior_year=False,
        reference_year_only=True,
        data_version=9,
    )

    assert out is not None and not out.empty
    assert calls["kwargs"] is not None
    assert calls["kwargs"]["year"] == 2026
    assert calls["kwargs"]["include_prior_year"] is False
    assert calls["kwargs"]["reference_year_only"] is True


def test_velocity_window_prefers_source_version_token(monkeypatch):
    calls: list[int] = []
    refreshed = {"ok": False}

    def _fake_fetch(sql, params=None):
        s = " ".join(str(sql).split()).upper()
        if "COUNT(1) AS ROWS_N" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            if refreshed["ok"]:
                return pd.DataFrame([{"ROWS_N": 10, "MAX_DATA_VERSION": 777, "MIN_DATA_VERSION": 777, "ROWS_BELOW_TARGET": 0}])
            return pd.DataFrame([{"ROWS_N": 10, "MAX_DATA_VERSION": 1, "MIN_DATA_VERSION": 1, "ROWS_BELOW_TARGET": 10}])
        if "FROM" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            return pd.DataFrame(
                [
                    {
                        "TEAMID": "T1",
                        "PROGRAMID": "P1",
                        "YEAR": 2026,
                        "PI": 1,
                        "PI_LABEL": "2026 PI1",
                        "PROGRAMNAME": "R&M",
                        "TEAMNAME": "ERNE WEST",
                        "EFFECTIVE_BASELINE_POINTS": 65.0,
                        "BASELINE_SOURCE": "GLOBAL",
                        "BASELINE_CHANGED_FLAG": 0,
                        "BASELINE_PREV": None,
                        "PI_POINTS_DONE": 120.0,
                    }
                ]
            )
        raise AssertionError(f"Unexpected SQL path: {sql}")

    monkeypatch.setattr(ado_recon, "fetch_df", _fake_fetch)
    monkeypatch.setattr(ado_recon, "ensure_tco_team_velocity_snapshot_table", lambda: True)
    monkeypatch.setattr(ado_recon, "_velocity_source_version_token", lambda: 777)
    def _fake_refresh(**kwargs):
        calls.append(int(kwargs.get("data_version") or 0))
        refreshed["ok"] = True
        return True

    monkeypatch.setattr(ado_recon, "refresh_tco_team_velocity_snapshot", _fake_refresh)

    out = ado_recon._load_velocity_baseline_window_uncached(
        year=2026,
        include_prior_year=False,
        reference_year_only=True,
        data_version=5,
    )

    assert out is not None and not out.empty
    assert calls == [777]


def test_velocity_window_clamps_source_version_token_to_sql_int(monkeypatch):
    calls: list[int] = []
    refreshed = {"ok": False}

    def _fake_fetch(sql, params=None):
        s = " ".join(str(sql).split()).upper()
        if "COUNT(1) AS ROWS_N" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            if refreshed["ok"]:
                return pd.DataFrame(
                    [{"ROWS_N": 10, "MAX_DATA_VERSION": 2_147_483_647, "MIN_DATA_VERSION": 2_147_483_647, "ROWS_BELOW_TARGET": 0}]
                )
            return pd.DataFrame([{"ROWS_N": 10, "MAX_DATA_VERSION": 1, "MIN_DATA_VERSION": 1, "ROWS_BELOW_TARGET": 10}])
        if "FROM" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            return pd.DataFrame(
                [
                    {
                        "TEAMID": "T1",
                        "PROGRAMID": "P1",
                        "YEAR": 2026,
                        "PI": 1,
                        "PI_LABEL": "2026 PI1",
                        "PROGRAMNAME": "R&M",
                        "TEAMNAME": "ERNE WEST",
                        "EFFECTIVE_BASELINE_POINTS": 65.0,
                        "BASELINE_SOURCE": "GLOBAL",
                        "BASELINE_CHANGED_FLAG": 0,
                        "BASELINE_PREV": None,
                        "PI_POINTS_DONE": 120.0,
                    }
                ]
            )
        raise AssertionError(f"Unexpected SQL path: {sql}")

    monkeypatch.setattr(ado_recon, "fetch_df", _fake_fetch)
    monkeypatch.setattr(ado_recon, "ensure_tco_team_velocity_snapshot_table", lambda: True)
    monkeypatch.setattr(ado_recon, "_velocity_source_version_token", lambda: 99_999_999_999_999)
    def _fake_refresh(**kwargs):
        calls.append(int(kwargs.get("data_version") or 0))
        refreshed["ok"] = True
        return True

    monkeypatch.setattr(ado_recon, "refresh_tco_team_velocity_snapshot", _fake_refresh)

    out = ado_recon._load_velocity_baseline_window_uncached(
        year=2026,
        include_prior_year=False,
        reference_year_only=True,
        data_version=5,
    )

    assert out is not None and not out.empty
    assert calls == [2_147_483_647]


def test_velocity_window_refreshes_when_partial_snapshot_is_stale(monkeypatch):
    calls: list[int] = []
    refreshed = {"ok": False}

    def _fake_fetch(sql, params=None):
        s = " ".join(str(sql).split()).upper()
        if "COUNT(1) AS ROWS_N" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            # Simulate mixed versions in the same year window:
            # a poisoned MAX version plus stale rows below target.
            if refreshed["ok"]:
                return pd.DataFrame([{"ROWS_N": 10, "MAX_DATA_VERSION": 777, "MIN_DATA_VERSION": 777, "ROWS_BELOW_TARGET": 0}])
            return pd.DataFrame(
                [{"ROWS_N": 10, "MAX_DATA_VERSION": 2_147_483_647, "MIN_DATA_VERSION": 100, "ROWS_BELOW_TARGET": 6}]
            )
        if "FROM" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            return pd.DataFrame(
                [
                    {
                        "TEAMID": "T1",
                        "PROGRAMID": "P1",
                        "YEAR": 2026,
                        "PI": 1,
                        "PI_LABEL": "2026 PI1",
                        "PROGRAMNAME": "R&M",
                        "TEAMNAME": "ERNE WEST",
                        "EFFECTIVE_BASELINE_POINTS": 65.0,
                        "BASELINE_SOURCE": "GLOBAL",
                        "BASELINE_CHANGED_FLAG": 0,
                        "BASELINE_PREV": None,
                        "PI_POINTS_DONE": 120.0,
                    }
                ]
            )
        raise AssertionError(f"Unexpected SQL path: {sql}")

    monkeypatch.setattr(ado_recon, "fetch_df", _fake_fetch)
    monkeypatch.setattr(ado_recon, "ensure_tco_team_velocity_snapshot_table", lambda: True)
    monkeypatch.setattr(ado_recon, "_velocity_source_version_token", lambda: 777)
    def _fake_refresh(**kwargs):
        calls.append(int(kwargs.get("data_version") or 0))
        refreshed["ok"] = True
        return True

    monkeypatch.setattr(ado_recon, "refresh_tco_team_velocity_snapshot", _fake_refresh)

    out = ado_recon._load_velocity_baseline_window_uncached(
        year=2026,
        include_prior_year=False,
        reference_year_only=True,
        data_version=5,
    )

    assert out is not None and not out.empty
    assert calls == [777]


def test_velocity_window_falls_back_to_view_when_stale_refresh_fails(monkeypatch):
    calls: list[int] = []

    def _fake_fetch(sql, params=None):
        s = " ".join(str(sql).split()).upper()
        if "COUNT(1) AS ROWS_N" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            return pd.DataFrame(
                [
                    {
                        "ROWS_N": 10,
                        "MAX_DATA_VERSION": 100,
                        "MIN_DATA_VERSION": 1,
                        "ROWS_BELOW_TARGET": 10,
                    }
                ]
            )
        if "FROM" in s and "VW_TCO_TEAM_VELOCITY_BASELINE" in s:
            return pd.DataFrame(
                [
                    {
                        "TEAMID": "T1",
                        "PROGRAMID": "P1",
                        "YEAR": 2026,
                        "PI": 1,
                        "PI_LABEL": "2026 PI1",
                        "PROGRAMNAME": "R&M",
                        "TEAMNAME": "ERNE WEST",
                        "EFFECTIVE_BASELINE_POINTS": 67.0,
                        "BASELINE_SOURCE": "GLOBAL",
                        "BASELINE_CHANGED_FLAG": 0,
                        "BASELINE_PREV": 65.0,
                        "PI_POINTS_DONE": 120.0,
                    }
                ]
            )
        if "FROM" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            raise AssertionError("Snapshot rows should not be read when stale refresh fails.")
        raise AssertionError(f"Unexpected SQL path: {sql}")

    monkeypatch.setattr(ado_recon, "fetch_df", _fake_fetch)
    monkeypatch.setattr(ado_recon, "ensure_tco_team_velocity_snapshot_table", lambda: True)
    monkeypatch.setattr(ado_recon, "_velocity_source_version_token", lambda: 777)

    def _fail_refresh(**kwargs):
        calls.append(int(kwargs.get("data_version") or 0))
        raise RuntimeError("refresh timeout")

    monkeypatch.setattr(ado_recon, "refresh_tco_team_velocity_snapshot", _fail_refresh)

    out = ado_recon._load_velocity_baseline_window_uncached(
        year=2026,
        include_prior_year=False,
        reference_year_only=True,
        data_version=5,
    )

    assert out is not None and not out.empty
    assert calls == [777]
    assert float(out.iloc[0]["EFFECTIVE_BASELINE_POINTS"]) == 67.0


def test_velocity_window_falls_back_to_view_when_still_stale_after_refresh(monkeypatch):
    calls: list[int] = []
    meta_calls = {"n": 0}

    def _fake_fetch(sql, params=None):
        s = " ".join(str(sql).split()).upper()
        if "COUNT(1) AS ROWS_N" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            meta_calls["n"] += 1
            return pd.DataFrame(
                [
                    {
                        "ROWS_N": 10,
                        "MAX_DATA_VERSION": 100,
                        "MIN_DATA_VERSION": 1,
                        "ROWS_BELOW_TARGET": 10,
                    }
                ]
            )
        if "FROM" in s and "VW_TCO_TEAM_VELOCITY_BASELINE" in s:
            return pd.DataFrame(
                [
                    {
                        "TEAMID": "T1",
                        "PROGRAMID": "P1",
                        "YEAR": 2026,
                        "PI": 1,
                        "PI_LABEL": "2026 PI1",
                        "PROGRAMNAME": "R&M",
                        "TEAMNAME": "ERNE WEST",
                        "EFFECTIVE_BASELINE_POINTS": 68.0,
                        "BASELINE_SOURCE": "GLOBAL",
                        "BASELINE_CHANGED_FLAG": 0,
                        "BASELINE_PREV": 66.0,
                        "PI_POINTS_DONE": 110.0,
                    }
                ]
            )
        if "FROM" in s and "TCO_TEAM_VELOCITY_SNAPSHOT" in s:
            raise AssertionError("Snapshot rows should not be read when stale remains after refresh.")
        raise AssertionError(f"Unexpected SQL path: {sql}")

    monkeypatch.setattr(ado_recon, "fetch_df", _fake_fetch)
    monkeypatch.setattr(ado_recon, "ensure_tco_team_velocity_snapshot_table", lambda: True)
    monkeypatch.setattr(ado_recon, "_velocity_source_version_token", lambda: 777)
    monkeypatch.setattr(
        ado_recon,
        "refresh_tco_team_velocity_snapshot",
        lambda **kwargs: calls.append(int(kwargs.get("data_version") or 0)) or True,
    )

    out = ado_recon._load_velocity_baseline_window_uncached(
        year=2026,
        include_prior_year=False,
        reference_year_only=True,
        data_version=5,
    )

    assert out is not None and not out.empty
    assert calls == [777]
    assert meta_calls["n"] >= 2
    assert float(out.iloc[0]["EFFECTIVE_BASELINE_POINTS"]) == 68.0
