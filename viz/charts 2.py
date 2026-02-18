from __future__ import annotations
import pandas as pd
from typing import Optional, List, Dict, Any
from viz.utils import to_kusd, nonempty, topn

# NOTE: These functions are PURE: they just return ECharts option dicts.
# You can use them both in the Visual Lab and in your main dashboard.

def opt_bar_top_spend_by(
    df: pd.DataFrame,
    group_col: str = "PROGRAMNAME",
    value_col: str = "AMOUNT",
    title: str = "Top Spend",
    top_n: int = 10,
    sort_desc: bool = True,
    label_suffix: str = " KUSD",
) -> Dict[str, Any]:
    """
    Groups df by group_col, sums value_col (converted to KUSD), takes top N, and
    returns an ECharts bar option.
    """
    work = df.copy()
    if work.empty or group_col not in work.columns or value_col not in work.columns:
        return {"title": {"text": f"{title} (no data)"}}

    work["KUSD"] = to_kusd(work[value_col])

    g = work.groupby(group_col, as_index=False)["KUSD"].sum()
    g = g[g[group_col].astype(str).str.strip().ne("")]  # drop empty labels
    g = g.sort_values("KUSD", ascending=not sort_desc).head(top_n)

    x = g[group_col].astype(str).tolist()
    y = g["KUSD"].round(1).tolist()

    return {
        "title": {"text": title},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "grid": {"left": "2%", "right": "2%", "top": 40, "bottom": 10, "containLabel": True},
        "xAxis": {"type": "category", "data": x, "axisLabel": {"rotate": 30}},
        "yAxis": {"type": "value", "name": "KUSD"},
        "series": [
            {
                "name": "KUSD",
                "type": "bar",
                "data": y,
                "label": {"show": True, "position": "top", "formatter": f"{{c}}{label_suffix}"},
            }
        ],
    }

def opt_stacked_by_category(
    df: pd.DataFrame,
    category_col: str = "COST_CATEGORY",     # WORK_FORCE / NON_WORK_FORCE
    group_col: str = "PROGRAMNAME",
    value_col: str = "AMOUNT",
    title: str = "Spend by Category",
    top_n_groups: int = 12,
) -> Dict[str, Any]:
    """
    Stacked bar: x = top groups by total, stacks = categories, values = KUSD.
    """
    work = df.copy()
    if work.empty:
        return {"title": {"text": f"{title} (no data)"}}

    for c in (category_col, group_col, value_col):
        if c not in work.columns:
            return {"title": {"text": f"{title} (missing columns)"}}

    work["KUSD"] = to_kusd(work[value_col])
    pivot = (
        work.groupby([group_col, category_col], as_index=False)["KUSD"].sum()
        .pivot(index=group_col, columns=category_col, values="KUSD")
        .fillna(0.0)
    )

    # Select top groups by total
    pivot["__TOTAL__"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("__TOTAL__", ascending=False).head(top_n_groups)
    categories = [c for c in pivot.columns if c != "__TOTAL__"]

    x = pivot.index.tolist()
    series = []
    for cat in categories:
        series.append({
            "name": str(cat),
            "type": "bar",
            "stack": "total",
            "emphasis": {"focus": "series"},
            "data": [round(float(v), 1) for v in pivot[cat].tolist()],
        })

    return {
        "title": {"text": title},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "legend": {"top": 24},
        "grid": {"left": "2%", "right": "2%", "top": 60, "bottom": 10, "containLabel": True},
        "xAxis": {"type": "category", "data": x, "axisLabel": {"rotate": 20}},
        "yAxis": {"type": "value", "name": "KUSD"},
        "series": series,
    }

def opt_pie_share_by(
    df: pd.DataFrame,
    group_col: str = "SOURCE",  # 'ADO' vs 'INVOICE'
    value_col: str = "AMOUNT",
    title: str = "Share",
) -> Dict[str, Any]:
    if df.empty or group_col not in df.columns or value_col not in df.columns:
        return {"title": {"text": f"{title} (no data)"}}
    g = df.copy()
    g["KUSD"] = to_kusd(g[value_col])
    s = g.groupby(group_col, as_index=False)["KUSD"].sum()
    s = s[s[group_col].astype(str).str.strip().ne("")]
    data = [{"name": str(r[group_col]), "value": float(r["KUSD"])} for _, r in s.iterrows()]
    return {
        "title": {"text": title, "left": "center"},
        "tooltip": {"trigger": "item"},
        "legend": {"orient": "horizontal", "bottom": 0},
        "series": [
            {
                "name": "KUSD",
                "type": "pie",
                "radius": ["35%", "65%"],
                "avoidLabelOverlap": True,
                "data": data,
                "label": {"formatter": "{b}: {c} KUSD ({d}%)"},
            }
        ],
    }
