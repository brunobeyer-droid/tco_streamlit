from __future__ import annotations

from core.echarts_quadrant_theme import build_quadrant_theme


def test_build_quadrant_theme_returns_markline_and_markarea() -> None:
    labels = {"Invest": "Grow", "Optimize": "Scale", "Monitor": "Watch", "Reassess": "Stop"}
    colors = {"Invest": "#22c55e", "Optimize": "#f59e0b", "Monitor": "#3b82f6", "Reassess": "#ef4444"}
    out = build_quadrant_theme(
        x_min=0.0,
        x_split=10.0,
        x_max=20.0,
        y_min=0.0,
        y_split=5.0,
        y_max=10.0,
        labels=labels,
        colors=colors,
    )
    assert "markLine" in out
    assert "markArea" in out
    assert out["markLine"]["lineStyle"]["type"] == "dashed"
    assert len(out["markLine"]["data"]) == 2
    assert len(out["markArea"]["data"]) == 4


def test_build_quadrant_theme_uses_passed_labels_and_colors() -> None:
    out = build_quadrant_theme(
        x_min=1.0,
        x_split=2.0,
        x_max=3.0,
        y_min=4.0,
        y_split=5.0,
        y_max=6.0,
        labels={"Invest": "Grow", "Optimize": "Scale", "Monitor": "Watch", "Reassess": "Stop"},
        colors={"Invest": "#11aa11", "Optimize": "#aa6600", "Monitor": "#2244ff", "Reassess": "#cc1111"},
    )
    names = [q[0]["name"] for q in out["markArea"]["data"]]
    assert names == ["Watch", "Stop", "Grow", "Scale"]
    colors = [q[0]["itemStyle"]["color"] for q in out["markArea"]["data"]]
    assert colors == ["#2244ff", "#cc1111", "#11aa11", "#aa6600"]


def test_build_quadrant_theme_uses_split_coordinates() -> None:
    out = build_quadrant_theme(
        x_min=0.0,
        x_split=50.0,
        x_max=100.0,
        y_min=10.0,
        y_split=20.0,
        y_max=40.0,
        labels={},
        colors={},
    )
    assert out["markLine"]["data"] == [{"xAxis": 50.0}, {"yAxis": 20.0}]
    ll = out["markArea"]["data"][0]
    lr = out["markArea"]["data"][1]
    ul = out["markArea"]["data"][2]
    ur = out["markArea"]["data"][3]
    assert ll[0]["xAxis"] == 0.0 and ll[0]["yAxis"] == 10.0 and ll[1]["xAxis"] == 50.0 and ll[1]["yAxis"] == 20.0
    assert lr[0]["xAxis"] == 50.0 and lr[0]["yAxis"] == 10.0 and lr[1]["xAxis"] == 100.0 and lr[1]["yAxis"] == 20.0
    assert ul[0]["xAxis"] == 0.0 and ul[0]["yAxis"] == 20.0 and ul[1]["xAxis"] == 50.0 and ul[1]["yAxis"] == 40.0
    assert ur[0]["xAxis"] == 50.0 and ur[0]["yAxis"] == 20.0 and ur[1]["xAxis"] == 100.0 and ur[1]["yAxis"] == 40.0


def test_build_quadrant_theme_default_style_values() -> None:
    out = build_quadrant_theme(
        x_min=0.0,
        x_split=1.0,
        x_max=2.0,
        y_min=0.0,
        y_split=1.0,
        y_max=2.0,
        labels={},
        colors={},
    )
    assert out["markLine"]["lineStyle"]["color"] == "#64748b"
    assert out["markLine"]["lineStyle"]["width"] == 2
    first_quad = out["markArea"]["data"][0][0]
    assert first_quad["itemStyle"]["opacity"] == 0.08
    assert first_quad["label"]["fontSize"] == 20
    assert first_quad["label"]["fontWeight"] == 700
