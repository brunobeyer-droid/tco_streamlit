from __future__ import annotations

from typing import Any


def build_quadrant_theme(
    x_min: float,
    x_split: float,
    x_max: float,
    y_min: float,
    y_split: float,
    y_max: float,
    labels: dict[str, str],
    colors: dict[str, str],
    *,
    line_color: str = "#64748b",
    line_width: int = 2,
    area_opacity: float = 0.08,
    label_position: str = "inside",
    label_font_size: int = 20,
    label_font_weight: int = 700,
    label_colors: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build shared ECharts markLine + markArea quadrant styling."""
    q_order = ["Monitor", "Reassess", "Invest", "Optimize"]
    label_colors = dict(label_colors or {})

    default_label_colors = {
        "Monitor": "rgba(191,219,254,0.35)",
        "Reassess": "rgba(254,202,202,0.35)",
        "Invest": "rgba(134,239,172,0.35)",
        "Optimize": "rgba(253,230,138,0.35)",
    }
    for q in q_order:
        if q not in label_colors:
            label_colors[q] = default_label_colors.get(q, "rgba(203,213,225,0.35)")

    mark_line = {
        "silent": True,
        "lineStyle": {"type": "dashed", "width": line_width, "color": line_color},
        "data": [{"xAxis": x_split}, {"yAxis": y_split}],
        "label": {"show": False},
    }

    def _quad(name: str, x0: float, y0: float, x1: float, y1: float) -> list[dict[str, Any]]:
        return [
            [
                {
                    "name": labels.get(name, name),
                    "xAxis": x0,
                    "yAxis": y0,
                    "itemStyle": {"color": colors.get(name, "#94a3b8"), "opacity": area_opacity},
                    "label": {
                        "show": True,
                        "position": label_position,
                        "fontSize": label_font_size,
                        "fontWeight": label_font_weight,
                        "color": label_colors.get(name, "rgba(203,213,225,0.35)"),
                    },
                },
                {"xAxis": x1, "yAxis": y1},
            ]
        ]

    mark_area_data: list[list[dict[str, Any]]] = []
    mark_area_data.extend(_quad("Monitor", x_min, y_min, x_split, y_split))
    mark_area_data.extend(_quad("Reassess", x_split, y_min, x_max, y_split))
    mark_area_data.extend(_quad("Invest", x_min, y_split, x_split, y_max))
    mark_area_data.extend(_quad("Optimize", x_split, y_split, x_max, y_max))

    mark_area = {
        "silent": True,
        "data": mark_area_data,
        "label": {"show": True, "position": label_position, "formatter": "{b}"},
        "z": 0,
    }

    return {"markLine": mark_line, "markArea": mark_area}
