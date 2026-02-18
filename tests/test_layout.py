import pandas as pd

from welcome.layout import (
    build_treemap_data,
    shorten_label,
    shade_color,
    PaletteHelper,
)


def test_shorten_label_truncates_and_adds_ellipsis():
    assert shorten_label("abcdefghijkl", max_len=6) == "abcde…"
    assert shorten_label("short", max_len=6) == "short"


def test_shade_color_returns_hex():
    shaded = shade_color("#4e79a7", 0.8)
    assert shaded.startswith("#") and len(shaded) == 7


def test_build_treemap_data_handles_basic_frame():
    df = pd.DataFrame(
        {
            "YEAR": [2024, 2024],
            "GROUPNAME": ["App A", "App B"],
            "SUBCOMPONENT": ["Delivery", "Contractor"],
            "COST_CATEGORY": ["WORK_FORCE", "NON_WORK_FORCE"],
            "TOTAL_COST": [1000.0, 500.0],
        }
    )
    data = build_treemap_data(df, PaletteHelper())
    assert isinstance(data, list)
    assert data and data[0]["name"] == "2024"
    assert any(child.get("name") == "App A" for child in data[0].get("children", []))
