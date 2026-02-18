# Summary: update welcome UI labels to NEXT branding.
import re
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from utils.theme import (
    _merged_theme,
    apply_echarts_theme as apply_echarts_spotify_theme,
    echarts_semantic_colors,
)

# Extended palette for app pie chart
APP_COLORS: List[str] = [
    "#5470C6", "#91CC75", "#EE6666", "#73C0DE", "#FAC858", "#3BA272", "#FC8452",
    "#9A60B4", "#EA7CCC", "#2f4554", "#61a0a8", "#d48265", "#749f83", "#ca8622",
    "#bda29a", "#6e7074", "#546570", "#c4ccd3", "#4cabce", "#f05b72", "#ef5b9c",
    "#f47920", "#905a3d", "#fab27b", "#2a5caa", "#444693", "#726930", "#b2d235",
    "#6d8346", "#ac6767", "#1d953f", "#6950a1", "#918597",
]

BASE_PALETTE: List[str] = [
    "#4e79a7", "#f28e2c", "#e15759", "#76b7b2", "#59a14f",
    "#edc949", "#af7aa1", "#ff9da7", "#9c755f", "#bab0ab",
]

SUBCOMPONENT_CATEGORY_OVERRIDES: Dict[str, str] = {
    "Recurring Invoice": "NON_WORK_FORCE",
    "Ad Hoc Invoice": "NON_WORK_FORCE",
    "Contractor CS": "NON_WORK_FORCE",
    "Contractor C": "WORK_FORCE",
    "Delivery Team": "WORK_FORCE",
    "Team": "WORK_FORCE",
    "MSP": "NON_WORK_FORCE",
}

NAVIGATION_MD = (
    "- Dashboard: Visualize spend and workforce splits.\n"
    "- Invoice Tracking: Create and manage invoices.\n"
    "- Programs / Teams / Vendor / Applications: Maintain master data.\n"
    "- Rates: Set team rates and rebuild analytics views.\n"
    "- Sync ADO Features: Import ADO data (cloud or mock) and map to NEXT.\n"
    "- MSSQL Health Check: Check DB connectivity and rebuild views if needed."
)


def alias_category(lbl: str) -> str:
    if not lbl:
        return lbl
    m = lbl.strip()
    mp = {
        "NON_WORK_FORCE": "Non Work Force",
        "Non_work_force": "Non Work Force",
        "WORK_FORCE": "Work Force",
        "Work_Force": "Work Force",
    }
    return mp.get(m, m.replace("_", " "))


def category_abbrev(cat_key: Optional[str]) -> str:
    key = (cat_key or "").strip()
    if not key:
        return "UNK"
    normalized = key.upper().replace(" ", "_")
    mapping = {
        "NON_WORK_FORCE": "NWF",
        "WORK_FORCE": "WF",
        "UNKNOWN": "UNK",
    }
    if normalized in mapping:
        return mapping[normalized]
    letters = "".join(part[0] for part in normalized.split("_") if part)
    return letters[:3].upper() if letters else normalized[:3].upper()


def normalize_subcomponent(name: str) -> str:
    raw = (name or "").strip()
    if not raw:
        return "(Other)"
    cleaned = re.sub(r"[_]+", " ", raw).strip()
    key = cleaned.upper()
    mapping = {
        "RECURRING INVOICE": "Recurring Invoice",
        "AD HOC INVOICE": "Ad Hoc Invoice",
        "CONTRACTOR CS": "Contractor CS",
        "CONTRACTOR C": "Contractor C",
        "DELIVERY": "Delivery Team",
        "DELIVERY TEAM": "Delivery Team",
        "TEAM": "Team",
        "MSP": "MSP",
        "MSP SMALL": "MSP",
        "MSP MEDIUM": "MSP",
        "MSP LARGE": "MSP",
    }
    # Handle Additional cost buckets and any strings containing these keywords
    if "CLOUD" in key:
        return "NWF - Cloud"
    if "TRAVEL" in key:
        return "NWF - Travel"
    if "INFRASTRUCTURE" in key or "INFRA" in key:
        return "NWF - Infrastructure"
    if "ADDITIONAL" in key:
        return "NWF - Additional"
    return mapping.get(key, cleaned.title())


def shorten_label(text: str, max_len: int = 10) -> str:
    s = str(text or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max(0, max_len - 1)] + "…"


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    hex_color = hex_color.lstrip("#")
    if len(hex_color) == 3:
        hex_color = "".join([c * 2 for c in hex_color])
    return tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def shade_color(base_color: str, factor: float) -> str:
    """Lighten or darken a hex color by multiplying its RGB components."""
    r, g, b = _hex_to_rgb(base_color)
    r = max(0, min(255, int(r * factor)))
    g = max(0, min(255, int(g * factor)))
    b = max(0, min(255, int(b * factor)))
    return _rgb_to_hex((r, g, b))


@dataclass
class PaletteHelper:
    palette_sequence: List[str] = field(default_factory=lambda: BASE_PALETTE.copy())
    subcomponent_color_map: Dict[str, str] = field(default_factory=dict)
    category_color_map: Dict[str, str] = field(default_factory=dict)
    subcomponent_category_map: Dict[str, str] = field(default_factory=dict)
    _palette_index: int = 0

    def _normalized_category_key(self, raw: Optional[str]) -> str:
        key = (raw or "UNKNOWN").strip()
        if not key:
            return "UNKNOWN"
        return key.upper().replace(" ", "_")

    def _next_palette_color(self) -> str:
        color = self.palette_sequence[self._palette_index % len(self.palette_sequence)]
        self._palette_index += 1
        return color

    def color_for_subcomponent(self, subcomponent: str, cost_category_key: Optional[str]) -> str:
        name_key = (str(subcomponent) or "").strip() or "(Other)"
        if name_key in self.subcomponent_color_map:
            return self.subcomponent_color_map[name_key]
        cat_key = self._normalized_category_key(cost_category_key)
        color = self._next_palette_color()
        self.category_color_map.setdefault(cat_key, color)
        self.subcomponent_color_map[name_key] = color
        self.subcomponent_category_map.setdefault(name_key, cat_key)
        return color

    def category_for_subcomponent(self, subcomponent: str) -> Optional[str]:
        return self.subcomponent_category_map.get((str(subcomponent) or "").strip() or "(Other)")


def build_treemap_data(
    df: pd.DataFrame,
    palette: PaletteHelper,
    group_colors: Optional[Dict[str, str]] = None,
    app_colors: Optional[List[str]] = None,
) -> List[dict]:
    if df is None or df.empty or "YEAR" not in df.columns:
        return []
    app_colors = app_colors or APP_COLORS
    group_colors = group_colors or {}
    shade_steps = [1.0, 0.9, 0.8, 1.1, 1.2, 0.7]
    years_numeric = pd.to_numeric(df["YEAR"], errors="coerce")
    years = sorted(years_numeric.dropna().astype(int).unique().tolist(), reverse=True)
    tree: List[dict] = []
    for year in years:
        mask = years_numeric == year
        year_df = df.loc[mask]
        if year_df.empty:
            continue
        year_total = float(year_df["TOTAL_COST"].sum())
        year_node = {
            "name": f"{int(year)}",
            "value": max(year_total, 0.0),
            "children": [],
        }
        group_totals = (
            year_df.groupby("GROUPNAME", dropna=False)["TOTAL_COST"].sum().reset_index()
            if "GROUPNAME" in year_df.columns else pd.DataFrame()
        )
        if group_totals.empty:
            continue
        group_totals = group_totals.sort_values("TOTAL_COST", ascending=False)
        for _, g_row in group_totals.iterrows():
            g_raw = g_row.get("GROUPNAME")
            group_mask = year_df["GROUPNAME"].isna() if pd.isna(g_raw) else (year_df["GROUPNAME"] == g_raw)
            rows = year_df.loc[group_mask]
            group_total = float(g_row.get("TOTAL_COST") or 0.0)
            group_name = str(g_raw).strip() if pd.notna(g_raw) and str(g_raw).strip() else "(Unassigned)"
            group_share = (group_total / year_total) if year_total else 0.0
            group_label = group_name
            if len(group_name) > 12 and group_share < 0.08:
                group_label = shorten_label(group_name, max_len=10)
            base_color = group_colors.get(group_name)
            if base_color is None:
                idx = sum(ord(ch) for ch in group_name) % len(app_colors)
                base_color = app_colors[idx]
                group_colors[group_name] = base_color
            node = {
                "name": group_label,
                "value": max(group_total, 0.0),
                "children": [],
                "itemStyle": {"color": base_color},
                "fullName": group_name,
                "label": {"overflow": "truncate", "ellipsis": "…"},
            }
            if not rows.empty and {"SUBCOMPONENT", "COST_CATEGORY"}.issubset(rows.columns):
                detail = rows.groupby(["SUBCOMPONENT", "COST_CATEGORY"], dropna=False)["TOTAL_COST"].sum().reset_index()
                detail = detail.sort_values("TOTAL_COST", ascending=False)
                for _, d_row in detail.iterrows():
                    sub_raw = d_row.get("SUBCOMPONENT")
                    sub_name = sub_raw if pd.notna(sub_raw) and str(sub_raw).strip() else "(Other)"
                    cat_raw = d_row.get("COST_CATEGORY")
                    cat_name = alias_category(str(cat_raw)) if pd.notna(cat_raw) else ""
                    sub_display = str(sub_name).strip()
                    cat_display = str(cat_name).strip() if cat_name else ""
                    cat_short = category_abbrev(cat_display) if cat_display else ""
                    sub_short = sub_display
                    value = float(d_row.get("TOTAL_COST") or 0.0)
                    sub_share = (value / group_total) if group_total else 0.0
                    if len(sub_display) > 14 and sub_share < 0.08:
                        sub_short = shorten_label(sub_display, max_len=12)
                    label_lines = [l for l in (cat_short or cat_display, sub_short) if l]
                    label = " — ".join(label_lines) if label_lines else sub_display
                    cell_label = "\n".join(label_lines) if label_lines else sub_display
                    node["children"].append({
                        "name": label,
                        "value": max(value, 0.0),
                        "label": {
                            "show": True,
                            "formatter": cell_label,
                            "overflow": "truncate",
                            "ellipsis": "…",
                            "lineHeight": 14,
                        },
                        "costCategory": cat_display,
                        "costType": sub_display,
                        "costFormatted": f"${value:,.2f}",
                        "itemStyle": {"color": shade_color(base_color, shade_steps[len(node["children"]) % len(shade_steps)])},
                    })
            if node["children"]:
                tree_node = node
            else:
                tree_node = {
                    "name": node["name"],
                    "value": node["value"],
                    "itemStyle": {"color": base_color},
                    "label": {"overflow": "truncate", "ellipsis": "…"},
                }
            year_node["children"].append(tree_node)
        if year_node["children"]:
            tree.append(year_node)
    return tree


from typing import Optional, List, Dict


def build_pie_option(title: str, items: List[dict], colors: List[str], ui_theme: Optional[str] = None) -> dict:
    sem = echarts_semantic_colors(_merged_theme())
    semantic_key_colors = {
        "baseline": sem["baseline"],
        "forecast": sem["forecast"],
        "expected": sem["forecast"],
        "demand": sem["forecast"],
        "actual": sem["actual"],
        "risk": sem["risk"],
        "unassigned": sem["risk"],
        "overhead": sem["risk"],
    }
    resolved_colors = list(colors or [])
    for idx, item in enumerate(items or []):
        if not isinstance(item, dict):
            continue
        label = str(item.get("name") or "").strip().lower()
        if not label:
            continue
        for key, color in semantic_key_colors.items():
            if key in label:
                if idx < len(resolved_colors):
                    resolved_colors[idx] = color
                else:
                    resolved_colors.append(color)
                break
    if not resolved_colors:
        resolved_colors = colors
    styled_items: List[dict] = []
    for idx, raw in enumerate(items or []):
        item = dict(raw) if isinstance(raw, dict) else {"name": str(raw), "value": 0}
        base = None
        if isinstance(item.get("itemStyle"), dict):
            base = item["itemStyle"].get("color")
        if not base and idx < len(resolved_colors):
            base = resolved_colors[idx]
        base = str(base or APP_COLORS[idx % len(APP_COLORS)])
        top = shade_color(base, 1.20)
        bottom = shade_color(base, 0.78)
        item["itemStyle"] = {
            "color": {
                "type": "linear",
                "x": 0,
                "y": 0,
                "x2": 0,
                "y2": 1,
                "colorStops": [
                    {"offset": 0, "color": top},
                    {"offset": 1, "color": bottom},
                ],
                "global": False,
            },
            "borderColor": "rgba(10,18,34,0.45)",
            "borderWidth": 1,
            "shadowBlur": 4,
            "shadowColor": "rgba(0,0,0,0.14)",
        }
        styled_items.append(item)

    option = {
        "title": {"text": title, "left": "center", "top": 6, "textStyle": {"fontSize": 12}},
        "color": resolved_colors,
        "tooltip": {"trigger": "item", "formatter": "{b}: ${c} ({d}%)"},
        "legend": {
            "bottom": 0,
            "type": "scroll",
            "left": "center",
            "width": "92%",
            "padding": [0, 24, 0, 24],
            "textStyle": {"overflow": "truncate", "width": 160},
        },
        "series": [
            {
                "type": "pie",
                "radius": ["44%", "74%"],
                "center": ["50%", "49%"],
                "avoidLabelOverlap": True,
                "minAngle": 3,
                "data": styled_items,
                "label": {
                    "show": True,
                    "formatter": "{d}%",
                    "fontSize": 11,
                    "fontWeight": 700,
                    "color": "#F8FAFC",
                },
                "labelLayout": {"hideOverlap": True},
                "labelLine": {"show": True, "length": 9, "length2": 8, "lineStyle": {"color": "rgba(203,213,225,0.45)"}},
                "emphasis": {"label": {"show": True, "fontSize": 12, "fontWeight": "bold"}},
            },
        ],
    }
    return apply_echarts_spotify_theme(option, ui_theme or st.session_state.get("ui_theme", "dark"))


def build_demand_vs_cost_option(
    df: pd.DataFrame,
    selected_year: int,
    palette: PaletteHelper,
    app_color_map: Optional[Dict[str, str]] = None,
    ui_theme: Optional[str] = None,
) -> Optional[dict]:
    required = {"YEAR", "GROUPNAME", "TOTAL_COST", "DEMAND_FTE"}
    if df is None or df.empty or not required.issubset(df.columns):
        return None

    work = df.copy()
    work["YEAR"] = pd.to_numeric(work["YEAR"], errors="coerce")
    work = work.dropna(subset=["YEAR"])
    work = work[work["YEAR"] == selected_year]
    if work.empty:
        return None

    for col in ("TOTAL_COST", "DEMAND_FTE", "FEATURE_COUNT"):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0)
    if "FEATURE_COUNT" not in work.columns:
        work["FEATURE_COUNT"] = 0

    work = work[(work["TOTAL_COST"] != 0) | (work["DEMAND_FTE"] != 0)]
    if work.empty:
        return None

    # Drop rows without an app/group/team/program (treat blank/None as missing)
    work["TEAMNAME"] = work.get("TEAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    work["PROGRAMNAME"] = work.get("PROGRAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    work["GROUPNAME"] = work.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()

    forbidden = {"", "(unassigned)", "none", "nan", "na", "null"}
    mask_group_ok = ~work["GROUPNAME"].str.lower().isin(forbidden)
    mask_team_ok = ~work["TEAMNAME"].str.lower().isin(forbidden)
    mask_prog_ok = ~work["PROGRAMNAME"].str.lower().isin(forbidden)
    work = work[mask_group_ok & mask_team_ok & mask_prog_ok]
    if work.empty:
        return None

    work.loc[work["GROUPNAME"].eq(""), "GROUPNAME"] = "(Unassigned)"

    # Combine rows for the same app group across teams/MSP so each app has one bubble
    def _uniq_concat(series: pd.Series) -> str:
        vals = [str(v).strip() for v in series.dropna().unique() if str(v).strip()]
        return ", ".join(sorted(vals)) if vals else ""

    combined = (
        work.groupby("GROUPNAME", dropna=False)
        .agg({
            "TOTAL_COST": "sum",
            "DEMAND_FTE": "sum",
            "FEATURE_COUNT": "sum",
            "TEAMNAME": _uniq_concat,
            "PROGRAMNAME": _uniq_concat,
        })
        .reset_index()
    )
    work = combined

    app_totals = work.set_index("GROUPNAME")["TOTAL_COST"].sort_values(ascending=False)
    app_order = app_totals.index.tolist()
    if not app_order:
        return None

    series: List[dict] = []
    color_map = app_color_map or {}
    sem = echarts_semantic_colors(_merged_theme())
    max_cost = max(float(work["TOTAL_COST"].max() or 0.0), 1.0)
    max_fte = max(float(work["DEMAND_FTE"].max() or 0.0), 1.0)

    for app in app_order:
        row = work[work["GROUPNAME"] == app].iloc[0]
        total_cost = float(row.get("TOTAL_COST") or 0.0)
        demand_fte = float(row.get("DEMAND_FTE") or 0.0)
        features = float(row.get("FEATURE_COUNT") or 0.0)
        team_name = row.get("TEAMNAME") or ""
        program_name = row.get("PROGRAMNAME") or ""
        app_label = app if pd.notna(app) and str(app).strip() else "(Unassigned)"
        if app_label not in color_map:
            idx_color = (sum(ord(c) for c in str(app_label)) if app_label else 0) % len(APP_COLORS)
            color_map[app_label] = APP_COLORS[idx_color]
        if "unassigned" in str(app_label).strip().lower():
            item_color = sem["risk"]
        else:
            item_color = color_map.get(app_label) or APP_COLORS[0]
        tooltip_lines = [
            f"<b>{app_label}</b>",
            f"Application: {app_label}",
            f"Team: {team_name}" if team_name else "",
            f"Program: {program_name}" if program_name else "",
            f"Cost: ${total_cost:,.0f}",
            f"Demand (Derived FTE): {demand_fte:,.2f}",
            f"Features: {features:,.0f}",
        ]
        tooltip_html = "<br/>".join([ln for ln in tooltip_lines if ln])
        cost_norm = (total_cost / max_cost) if max_cost > 0 else 0.0
        fte_norm = (max(demand_fte, 0.0) / max_fte) if max_fte > 0 else 0.0
        cost_weight = math.pow(cost_norm, 0.65)
        fte_weight = math.pow(fte_norm, 0.65)
        size_val = 12.0 + (cost_weight * fte_weight * 140.0)
        size_val = max(12.0, min(size_val, 200.0))
        points = [{
            "name": app_label,
            "value": [total_cost, demand_fte, features, app_label, team_name, program_name],
            "groupName": app_label,
            "team": team_name,
            "program": program_name,
            "totalCost": total_cost,
            "demandFte": demand_fte,
            "features": features,
            "tooltip": {"formatter": tooltip_html},
            "itemStyle": {"color": item_color},
            "symbolSize": size_val,
        }]
        series.append({
            "name": app_label,
            "type": "scatter",
            "data": points,
            "emphasis": {"focus": "series"},
            "itemStyle": {"color": item_color},
        })

    if not series:
        return None

    option = {
        "title": {
            "text": "Demand vs App Cost",
            "left": "center",
            "top": 6,
            "textStyle": {"fontSize": 12},
        },
        "legend": {"type": "scroll", "bottom": 10},
        "grid": {"left": "8%", "right": 40, "bottom": 70, "containLabel": True},
        "dimensions": ["cost", "demand_fte", "features", "app", "team", "program"],
        "encode": {"x": "cost", "y": "demand_fte", "tooltip": ["app", "team", "program", "cost", "demand_fte", "features"]},
        "xAxis": {
            "type": "value",
            "name": "Total Cost",
            "axisLabel": {"formatter": "${value}"},
        },
        "yAxis": {
            "type": "value",
            "name": "Demand (Derived FTE)",
            "axisLabel": {"formatter": "{value}"},
        },
        "series": series,
        "animationDuration": 600,
        "dataZoom": [{"type": "inside"}],
        "tooltip": {"trigger": "item"},
    }
    return apply_echarts_spotify_theme(option, ui_theme or st.session_state.get("ui_theme", "dark"))


def build_iteration_mix_option(df: pd.DataFrame, selected_year: int, palette: PaletteHelper, ui_theme: Optional[str] = None) -> Optional[dict]:
    required = {"YEAR", "PI", "SUBCOMPONENT", "TOTAL_COST"}
    if df is None or df.empty or not required.issubset(df.columns):
        return None

    work = df.copy()
    work["YEAR"] = pd.to_numeric(work["YEAR"], errors="coerce")
    work = work.dropna(subset=["YEAR"])
    work = work[work["YEAR"] == selected_year]
    if work.empty:
        return None

    pi_col = "PI"
    if "PI" not in work.columns and "ITERATION_NUM" in work.columns:
        work = work.rename(columns={"ITERATION_NUM": "PI"})
    elif "PI" not in work.columns:
        work["PI"] = None

    def _pi_label(row) -> str:
        raw = row.get("PI")
        if pd.isna(raw):
            return "Unscheduled"
        text = str(raw).strip()
        if not text:
            return "Unscheduled"
        match = re.search(r"(\\d+)", text)
        if match:
            return f"I{int(match.group(1))}"
        return text

    work["PI_LABEL"] = work.apply(_pi_label, axis=1)
    work = work[work["PI_LABEL"].str.upper() != "UNSCHEDULED"]
    if work.empty:
        return None

    work["SUBCOMPONENT"] = work["SUBCOMPONENT"].fillna("").astype(str).str.strip()
    work.loc[work["SUBCOMPONENT"].eq(""), "SUBCOMPONENT"] = "(Other)"
    work["SUBCOMPONENT"] = work["SUBCOMPONENT"].apply(normalize_subcomponent)
    work["COST_CATEGORY"] = work["COST_CATEGORY"].fillna("Unknown").astype(str).apply(alias_category)
    work["COST_CATEGORY_KEY"] = work["COST_CATEGORY"].str.upper().str.replace(" ", "_")

    grouped = work.groupby(["PI_LABEL", "SUBCOMPONENT"], dropna=False)["TOTAL_COST"].sum().reset_index()
    grouped["TOTAL_COST"] = grouped["TOTAL_COST"].astype(float)
    if grouped.empty:
        return None

    pivot = grouped.pivot(index="PI_LABEL", columns="SUBCOMPONENT", values="TOTAL_COST").fillna(0.0)
    pivot = pivot.loc[pivot.sum(axis=1) > 0]
    pivot = pivot.loc[:, pivot.sum(axis=0) > 0]
    if pivot.empty:
        return None

    category_map = (
        work.groupby("SUBCOMPONENT")["COST_CATEGORY_KEY"].agg(lambda s: s.value_counts().idxmax())
        .to_dict()
    )

    def _label_key(lbl: str):
        m = re.search(r"I(\\d+)", str(lbl) or "")
        if m:
            return (int(m.group(1)), str(lbl))
        return (999, str(lbl))

    ordered_labels = sorted(pivot.index.tolist(), key=_label_key)
    pivot = pivot.loc[ordered_labels]
    column_order = pivot.sum(axis=0).sort_values(ascending=False).index.tolist()
    pivot = pivot[column_order]

    totals = pivot.sum(axis=1)

    series = []
    for subcomponent in column_order:
        cat_key = SUBCOMPONENT_CATEGORY_OVERRIDES.get(subcomponent)
        if not cat_key:
            cat_key = category_map.get(subcomponent, palette.category_for_subcomponent(subcomponent)) or "UNKNOWN"
        cat_key = palette._normalized_category_key(cat_key)
        color = palette.color_for_subcomponent(subcomponent, cat_key)
        cat_label = alias_category(cat_key)
        display_name = f"{subcomponent} ({category_abbrev(cat_key)})"
        data = []
        for label in ordered_labels:
            cost_val = float(pivot.loc[label, subcomponent])
            total_val = float(totals.loc[label]) if label in totals.index else 0.0
            pct_val = round((cost_val / total_val * 100.0) if total_val else 0.0, 1)
            data.append({
                "value": pct_val,
                "label": {
                    "show": pct_val >= 3.0,
                    "position": "inside",
                    "formatter": "{c}%",
                    "color": "#fff",
                    "fontSize": 10,
                },
                "costCategory": cat_label,
            })
        series.append({
            "name": display_name,
            "type": "bar",
            "stack": "total",
            "stackStrategy": "positive",
            "emphasis": {"focus": "series"},
            "itemStyle": {"color": color},
            "data": data,
        })

    total_series = [round(float(totals.loc[label]), 2) for label in ordered_labels]
    series.append({
        "name": "Total Cost",
        "type": "line",
        "yAxisIndex": 1,
        "smooth": True,
        "symbol": "circle",
        "symbolSize": 8,
        "itemStyle": {"color": "#000000"},
        "lineStyle": {"width": 2, "color": "#000000"},
        "data": total_series,
    })

    option: dict = {
        "title": {
            "text": "Iteration Cost Mix",
            "left": "center",
            "top": 6,
            "textStyle": {"fontSize": 12},
        },
        "tooltip": {"trigger": "axis"},
        "legend": {
            "top": None,
            "bottom": 10,
            "type": "scroll",
            "orient": "horizontal",
        },
        "grid": {"left": "6%", "right": 56, "bottom": 60, "containLabel": True},
        "xAxis": {"type": "category", "data": ordered_labels, "axisLabel": {"rotate": 20}},
        "yAxis": [
            {
                "type": "value",
                "name": "Contribution %",
                "max": 100,
                "min": 0,
                "axisLabel": {"formatter": "{value}%"},
            },
            {
                "type": "value",
                "name": "Total Cost",
                "position": "right",
                "axisLabel": {"formatter": "${value}"},
                "splitLine": {"show": False},
            },
        ],
        "series": series,
    }

    if len(ordered_labels) > 8:
        option["dataZoom"] = [
            {"type": "slider", "bottom": 20, "height": 20},
            {"type": "inside"},
        ]

    return apply_echarts_spotify_theme(option, ui_theme or st.session_state.get("ui_theme", "dark"))


def build_investment_mix_option(df: pd.DataFrame, selected_year: int, palette: PaletteHelper, ui_theme: Optional[str] = None):
    if df is None or df.empty:
        return None
    dim_col = None
    for candidate in ("FEATURE_INVESTMENT_DIMENSION", "INVESTMENT_DIMENSION"):
        if candidate in df.columns:
            dim_col = candidate
            break
    if dim_col is None or "COST_CATEGORY" not in df.columns or "TOTAL_COST" not in df.columns or "YEAR" not in df.columns:
        return None

    work = df.copy()
    work["YEAR"] = pd.to_numeric(work["YEAR"], errors="coerce")
    work = work.dropna(subset=["YEAR"])
    work = work[work["YEAR"] == selected_year]
    if work.empty:
        return None

    work[dim_col] = work[dim_col].astype(str).str.strip()
    mask_valid = ~work[dim_col].str.lower().isin({"", "unknown", "none", "nan"})
    work = work.loc[mask_valid]
    if work.empty:
        return None

    if "SUBCOMPONENT" not in work.columns:
        return None

    work["SUBCOMPONENT"] = work["SUBCOMPONENT"].fillna("").astype(str).str.strip()
    work.loc[work["SUBCOMPONENT"].eq(""), "SUBCOMPONENT"] = "(Other)"
    work["SUBCOMPONENT"] = work["SUBCOMPONENT"].apply(normalize_subcomponent)
    work["COST_CATEGORY"] = work["COST_CATEGORY"].fillna("Unknown").astype(str).apply(alias_category)
    work["COST_CATEGORY_KEY"] = work["COST_CATEGORY"].str.upper().str.replace(" ", "_")

    totals = work.groupby([dim_col, "SUBCOMPONENT"], dropna=False)["TOTAL_COST"].sum().reset_index()
    if totals.empty:
        return None
    totals["TOTAL_COST"] = totals["TOTAL_COST"].astype(float)

    pivot = totals.pivot(index=dim_col, columns="SUBCOMPONENT", values="TOTAL_COST").fillna(0.0)
    pivot = pivot.loc[:, (pivot != 0).any(axis=0)]
    if pivot.empty:
        return None

    categories = pivot.index.tolist()
    column_order = pivot.sum(axis=0).sort_values(ascending=False).index.tolist()
    totals_by_category = pivot.sum(axis=1)

    category_map_mix = (
        work.groupby("SUBCOMPONENT")["COST_CATEGORY_KEY"].agg(lambda s: s.value_counts().idxmax())
        .to_dict()
    )

    series_defs = []
    for subcomponent in column_order:
        name_key = (str(subcomponent) or "").strip() or "(Other)"
        cat_key = SUBCOMPONENT_CATEGORY_OVERRIDES.get(name_key)
        if not cat_key:
            cat_key = category_map_mix.get(name_key, palette.category_for_subcomponent(name_key))
        cat_key = palette._normalized_category_key(cat_key)
        color = palette.color_for_subcomponent(name_key, cat_key)
        cat_label = alias_category(cat_key)
        display_name = f"{name_key} ({category_abbrev(cat_key)})"
        data_points = []
        for label in categories:
            cost_val = float(pivot.loc[label, subcomponent])
            total_val = float(totals_by_category.loc[label]) if label in totals_by_category.index else 0.0
            pct_val = round((cost_val / total_val * 100.0) if total_val else 0.0, 1)
            cost_str = f"{int(round(cost_val)):,}" if cost_val else "0"
            data_points.append({
                "value": pct_val,
                "costCategory": cat_label,
                "name": f"{label} — {display_name} (Cost: ${cost_str})",
                "label": {
                    "show": pct_val >= 3.0,
                    "position": "inside",
                    "formatter": "{c}%",
                    "color": "#fff",
                    "fontSize": 10,
                },
            })
        series_defs.append({
            "name": display_name,
            "type": "bar",
            "stack": "total",
            "stackStrategy": "positive",
            "emphasis": {"focus": "series"},
            "data": data_points,
            "itemStyle": {"color": color},
        })

    option = {
        "title": {
            "text": "Investment Dimension",
            "left": "center",
            "top": 6,
            "textStyle": {"fontSize": 12},
        },
        "tooltip": {
            "trigger": "item",
            "formatter": "{b}<br/>Contribution: {c}%",
        },
        "legend": {
            "top": None,
            "bottom": 10,
            "type": "scroll",
            "orient": "horizontal",
        },
        "grid": {"left": "12%", "right": "6%", "bottom": 60, "containLabel": True},
        "xAxis": {"type": "category", "data": categories, "axisLabel": {"rotate": 30}},
        "yAxis": {
            "type": "value",
            "name": "Contribution %",
            "max": 100,
            "axisLabel": {"formatter": "{value}%"},
        },
        "series": series_defs,
    }
    return apply_echarts_spotify_theme(option, ui_theme or st.session_state.get("ui_theme", "dark"))


def summarize_costs(df_cost: Optional[pd.DataFrame]) -> dict:
    """Compute basic KPI values from the cost dataframe."""
    if df_cost is None or df_cost.empty:
        return {
            "total_cost": 0.0,
            "app_count": 0,
            "workforce_total": 0.0,
            "non_workforce_total": 0.0,
        }
    total_cost = float(df_cost["TOTAL_COST"].sum()) if "TOTAL_COST" in df_cost.columns else 0.0
    app_count = int(df_cost["GROUPNAME"].nunique()) if "GROUPNAME" in df_cost.columns else 0
    workforce_total = 0.0
    non_workforce_total = 0.0
    if "COST_CATEGORY" in df_cost.columns and "TOTAL_COST" in df_cost.columns:
        cats = df_cost.groupby("COST_CATEGORY", as_index=False)["TOTAL_COST"].sum()
        cats_up = cats.copy()
        cats_up["_CAT"] = cats_up["COST_CATEGORY"].astype(str).str.upper()
        workforce_total = float(cats_up.loc[cats_up["_CAT"] == "WORK_FORCE", "TOTAL_COST"].sum())
        non_workforce_total = float(cats_up.loc[cats_up["_CAT"] == "NON_WORK_FORCE", "TOTAL_COST"].sum())
    return {
        "total_cost": total_cost,
        "app_count": app_count,
        "workforce_total": workforce_total,
        "non_workforce_total": non_workforce_total,
    }


def build_cost_breakdown(df_cost: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Format cost breakdown table."""
    if df_cost is None or df_cost.empty:
        return None
    work = df_cost.copy()
    if "INVESTMENT_DIMENSION" in work.columns and "FEATURE_INVESTMENT_DIMENSION" not in work.columns:
        work = work.rename(columns={"INVESTMENT_DIMENSION": "FEATURE_INVESTMENT_DIMENSION"})

    required_cols = [
        "GROUPNAME",
        "SUBCOMPONENT",
        "COST_CATEGORY",
        "FEATURE_INVESTMENT_DIMENSION",
        "TOTAL_COST",
    ]

    if "PI" not in work.columns and "ITERATION_NUM" in work.columns:
        work = work.rename(columns={"ITERATION_NUM": "PI"})

    if not all(col in work.columns for col in required_cols):
        return None

    breakdown = work[required_cols].copy()
    breakdown = breakdown.fillna({
        "GROUPNAME": "(Unassigned)",
        "SUBCOMPONENT": "(Other)",
        "COST_CATEGORY": "Unknown",
        "FEATURE_INVESTMENT_DIMENSION": "Not Applicable",
    })
    breakdown["FEATURE_INVESTMENT_DIMENSION"] = (
        breakdown["FEATURE_INVESTMENT_DIMENSION"]
        .astype(str)
        .str.strip()
        .replace({"": "Not Applicable", "None": "Not Applicable", "nan": "Not Applicable"})
    )
    breakdown["TOTAL_COST"] = breakdown["TOTAL_COST"].astype(float)
    breakdown = (
        breakdown.groupby(
            ["GROUPNAME", "SUBCOMPONENT", "COST_CATEGORY", "FEATURE_INVESTMENT_DIMENSION"],
            dropna=False,
        )["TOTAL_COST"].sum().reset_index()
    )
    breakdown = breakdown.sort_values("TOTAL_COST", ascending=False)
    breakdown = breakdown.rename(
        columns={
            "GROUPNAME": "Application",
            "SUBCOMPONENT": "Cost Type",
            "COST_CATEGORY": "Category",
            "FEATURE_INVESTMENT_DIMENSION": "Feature Dimenstion",
            "TOTAL_COST": "Total Cost",
        }
    )
    breakdown["Category"] = breakdown["Category"].apply(lambda x: alias_category(str(x)))
    breakdown["Total Cost"] = breakdown["Total Cost"].map(lambda x: f"${x:,.2f}")
    return breakdown


def render_kpis(st, df_cost: Optional[pd.DataFrame], invoice_count: Optional[int]) -> None:
    """Render KPI cards."""
    summary = summarize_costs(df_cost)
    k1, k2, k3, k4, k5, kb = st.columns([1.2, 0.9, 1.2, 1.2, 1.2, 1.5])

    def _mini_kpi(col, label: str, value: str):
        col.markdown(
            f"<div style='line-height:1.1; margin-bottom:6px;'>"
            f"<div style='font-size:22px; font-weight:600'>{value}</div>"
            f"<div style='font-size:12px; color:#888'>{label}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    _mini_kpi(k1, "Total NEXT", f"${summary['total_cost']:,.2f}")
    _mini_kpi(k2, "Apps", f"{summary['app_count']}")
    _mini_kpi(k3, "Work Force", f"${summary['workforce_total']:,.2f}")
    _mini_kpi(k4, "Non Work Force", f"${summary['non_workforce_total']:,.2f}")
    _mini_kpi(k5, "Invoices this month", f"{invoice_count}" if invoice_count is not None else "—")

    return k1, k2, k3, k4, k5, kb
