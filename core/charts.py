from __future__ import annotations

from typing import Sequence


def option_top_movers_bar(
    labels: Sequence[str],
    values: Sequence[float],
    *,
    title: str = "",
    x_name: str = "",
) -> dict:
    bar_data = []
    for val in values:
        color = "#22c55e" if val >= 0 else "#ef4444"
        try:
            v = float(val)
        except Exception:
            v = 0.0
        bar_data.append({"value": int(round(v, 0)), "itemStyle": {"color": color}})
    x_axis = {"type": "value"}
    if x_name:
        x_axis["name"] = x_name
    return {
        "title": {"text": title, "left": "center"} if title else {},
        "grid": {"left": "10%", "right": "6%", "bottom": "12%", "containLabel": True},
        "xAxis": {**x_axis, "axisLabel": {"formatter": "${value}"}},
        "yAxis": {"type": "category", "data": list(labels)},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}, "formatter": "{b0}<br/>Cost delta: ${c0}"},
        "series": [{"type": "bar", "data": bar_data}],
    }


def option_cost_trend_line(labels: Sequence[str], values: Sequence[float], *, name: str = "Cost") -> dict:
    return {
        "tooltip": {"trigger": "axis"},
        "grid": {"left": "8%", "right": "4%", "bottom": "12%", "containLabel": True},
        "xAxis": {"type": "category", "data": list(labels)},
        "yAxis": {"type": "value"},
        "series": [{"name": name, "type": "line", "smooth": True, "data": [float(v) for v in values]}],
    }
