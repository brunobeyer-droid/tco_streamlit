from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_roadmap_page_module():
    page_path = Path(__file__).resolve().parents[1] / "pages" / "2_Roadmap.py"
    spec = importlib.util.spec_from_file_location("roadmap_page", str(page_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load pages/2_Roadmap.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resolve_rows_by_mode_defaults_to_epics():
    try:
        mod = _load_roadmap_page_module()
    except ModuleNotFoundError as exc:
        pytest.skip(f"Dependency missing for roadmap page import: {exc}")
    assert mod._resolve_rows_by_mode(None, default_mode="Epics") == "Epics"
    assert mod._resolve_rows_by_mode("", default_mode="Epics") == "Epics"
    assert mod._resolve_rows_by_mode("Epic", default_mode="Epics") == "Epics"
    assert mod._resolve_rows_by_mode("Applications (App Groups)", default_mode="Epics") == "Applications"


def test_apply_display_name_maps_remaps_team_program_and_group():
    pandas = pytest.importorskip("pandas")
    try:
        mod = _load_roadmap_page_module()
    except ModuleNotFoundError as exc:
        pytest.skip(f"Dependency missing for roadmap page import: {exc}")
    df = pandas.DataFrame(
        [
            {"PROGRAMNAME": "PROGRAM_RAW", "TEAMNAME": "TEAM_RAW", "GROUPNAME": "APP_RAW"},
            {"PROGRAMNAME": "Program X", "TEAMNAME": "Team Y", "GROUPNAME": "App Z"},
        ]
    )
    out = mod._apply_display_name_maps(
        df,
        program_label_map={"PROGRAM_RAW": "Program Display"},
        team_label_map={"TEAM_RAW": "Team Display"},
        group_label_map={"APP_RAW": "App Display"},
    )
    assert out.iloc[0]["PROGRAMNAME"] == "Program Display"
    assert out.iloc[0]["TEAMNAME"] == "Team Display"
    assert out.iloc[0]["GROUPNAME"] == "App Display"
    assert out.iloc[1]["PROGRAMNAME"] == "Program X"
    assert out.iloc[1]["TEAMNAME"] == "Team Y"
    assert out.iloc[1]["GROUPNAME"] == "App Z"


def test_apply_exact_scope_filter_keeps_only_selected_team():
    pandas = pytest.importorskip("pandas")
    try:
        mod = _load_roadmap_page_module()
    except ModuleNotFoundError as exc:
        pytest.skip(f"Dependency missing for roadmap page import: {exc}")
    df = pandas.DataFrame(
        [
            {"TEAMNAME": "Team A", "FEATURE_ID": 1},
            {"TEAMNAME": "Team B", "FEATURE_ID": 2},
            {"TEAMNAME": "team a", "FEATURE_ID": 3},
        ]
    )
    out = mod._apply_exact_scope_filter(df, col="TEAMNAME", selected=["Team A"])
    ids = sorted(out["FEATURE_ID"].tolist())
    assert ids == [1, 3]
