from __future__ import annotations

from typing import Any, Optional, Tuple
import json
import re

import pandas as pd
import streamlit as st
import colorsys

try:
    from streamlit_echarts import JsCode  # type: ignore

except Exception:  # pragma: no cover
    JsCode = None  # type: ignore

from core.cost_model import get_cost_model_warning, load_cost_model
from core.canonical_costs import get_cost_lines as _get_cost_lines_raw, get_pi_costs as _get_pi_costs_raw
from core.cache_utils import cache_data_portfolio
from core.data import (
    fetch_filter_options,
    fetch_apptio_actuals_monthly_breakdown,
    _program_ids_for_names,
)
from core.capacity_data import fetch_capacity_demand_pi
from core.init import init_page, page_loader, render_echart
from core.scope import infer_role, read_scope_from_session, scope_label
from core.ui import render_page_header, touch_last_updated_status
from core.debug import is_debug_enabled
from core.perf import (
    filters_signature,
    get_portfolio_cache_buster,
    mark_first_kpi_render,
    perf_step,
    show_perf_panel,
    user_scope_signature,
)
from core.portfolio_runtime import get_active_portfolio_key
from core.name_resolution import apply_display_scope_names
from welcome.layout import APP_COLORS
from utils.theme import _merged_theme, echarts_semantic_colors
from utils.finops_kpi_cards import build_finops_kpi_card_html
from visuals.cost_anatomy import build_cost_anatomy_from_cost_lines
from utils.ui_patterns import render_active_filters_summary
try:
    from utils.auth import ensure_sso
except Exception:
    st.error("Authentication is not available. Contact an admin.")
    st.stop()
from db import fetch_df_active as _fetch_df_raw, get_data_version, list_programs, list_teams, list_application_groups  # type: ignore

from utils.app_shell import bootstrap_page
bootstrap_page()


def fetch_df(sql, params=None):  # type: ignore[override]
    df = _fetch_df_raw(sql, params)
    return apply_display_scope_names(df) if isinstance(df, pd.DataFrame) else df


_PI_COSTS_MEMO: dict = {}
_COST_LINES_MEMO: dict = {}


def _freeze_cache_key(obj):
    if isinstance(obj, dict):
        return tuple((str(k), _freeze_cache_key(v)) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0])))
    if isinstance(obj, (list, tuple)):
        return tuple(_freeze_cache_key(v) for v in obj)
    if isinstance(obj, set):
        frozen = [_freeze_cache_key(v) for v in obj]
        return tuple(sorted(frozen, key=lambda x: str(x)))
    if isinstance(obj, pd.Series):
        return ("series", len(obj.index), str(obj.dtype))
    if isinstance(obj, pd.DataFrame):
        return ("df", id(obj), tuple(obj.shape))
    try:
        hash(obj)
        return obj
    except Exception:
        return str(obj)


def get_pi_costs(*args, **kwargs):  # type: ignore[override]
    base = args[0] if args else None
    key = None
    if isinstance(base, pd.DataFrame):
        key = (
            "pi",
            id(base),
            tuple(base.shape),
            _freeze_cache_key(args[1:]),
            _freeze_cache_key(kwargs),
        )
        hit = _PI_COSTS_MEMO.get(key)
        if isinstance(hit, pd.DataFrame):
            return hit.copy()
    out = apply_display_scope_names(_get_pi_costs_raw(*args, **kwargs))
    if key is not None and isinstance(out, pd.DataFrame):
        if len(_PI_COSTS_MEMO) > 256:
            _PI_COSTS_MEMO.clear()
        _PI_COSTS_MEMO[key] = out.copy()
    return out


def get_cost_lines(*args, **kwargs):  # type: ignore[override]
    base = args[0] if args else None
    key = None
    if isinstance(base, pd.DataFrame):
        key = (
            "lines",
            id(base),
            tuple(base.shape),
            _freeze_cache_key(args[1:]),
            _freeze_cache_key(kwargs),
        )
        hit = _COST_LINES_MEMO.get(key)
        if isinstance(hit, pd.DataFrame):
            return hit.copy()
    out = apply_display_scope_names(_get_cost_lines_raw(*args, **kwargs))
    if key is not None and isinstance(out, pd.DataFrame):
        if len(_COST_LINES_MEMO) > 256:
            _COST_LINES_MEMO.clear()
        _COST_LINES_MEMO[key] = out.copy()
    return out

page_theme = init_page("Dashboard", page_path=__file__)

DASHBOARD_KPI_EXTRA_LINES = 3
SECTION_DIVIDER_MARGIN = "8px 0 10px 0"


def _safe_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, pd.Series):
        values = values.dropna().tolist()
    cleaned = [str(v).strip() for v in values if str(v).strip()]
    return list(dict.fromkeys(cleaned))


_UNASSIGNED_FILTER_TOKENS = {"UNASSIGNED", "(UNASSIGNED)"}


def _drop_unassigned_if_mixed(values: list[str]) -> list[str]:
    cleaned = [str(v).strip() for v in (values or []) if str(v).strip()]
    if not cleaned:
        return []
    has_real = any(v.upper() not in _UNASSIGNED_FILTER_TOKENS for v in cleaned)
    if not has_real:
        return cleaned
    return [v for v in cleaned if v.upper() not in _UNASSIGNED_FILTER_TOKENS]


def _render_section_divider() -> None:
    st.markdown(f"<hr style='margin:{SECTION_DIVIDER_MARGIN};'>", unsafe_allow_html=True)


def render_chart_title_with_infotip(title: str, tip: str) -> None:
    st.subheader(title, help=tip or None)


def render_finops_kpi_card(
    title: str,
    value: str,
    *,
    tip: str,
    chips: Optional[list[str]],
    lines: Optional[list[str]],
    accent_color: str,
    icon: str = "insights",
    status: str = "neutral",
    status_label: str = "",
    trend_text: str = "",
    trend_tone: str = "neutral",
    spark_values: Optional[list[float]] = None,
    spark_tone: str = "neutral",
    spark_motif: str = "bars",
) -> None:
    st.markdown(
        build_finops_kpi_card_html(
            title=title,
            value=value,
            tip=tip,
            chips=chips,
            lines=lines,
            accent_color=accent_color,
            icon=icon,
            status=status,
            status_label=status_label,
            trend_text=trend_text,
            trend_tone=trend_tone,
            spark_values=spark_values,
            spark_tone=spark_tone,
            spark_motif=spark_motif,
            extra_lines=DASHBOARD_KPI_EXTRA_LINES,
        ),
        unsafe_allow_html=True,
    )


def _strip_chart_title(option: dict) -> dict:
    if not isinstance(option, dict):
        return option
    title = option.get("title")
    if isinstance(title, dict):
        title["text"] = ""
        option["title"] = title
    return option


def _to_amount(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or "AMOUNT" not in df.columns:
        return pd.Series([], dtype=float)
    return pd.to_numeric(df.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)


def _wf_nwf_mask(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([])), pd.Series(False, index=pd.Index([]))
    if "COST_CATEGORY" in df.columns:
        cat = df["COST_CATEGORY"].fillna("").astype(str).str.upper()
        wf = cat.str.contains("WORK") & ~cat.str.contains("NON")
        nwf = cat.str.contains("NWF") | cat.str.contains("NON")
        return wf, nwf
    if "WF_NWF" in df.columns:
        val = df["WF_NWF"].fillna("").astype(str).str.upper().str.strip()
        return val.eq("WF"), val.eq("NWF")
    if "IS_WORKFORCE" in df.columns:
        val = pd.to_numeric(df["IS_WORKFORCE"], errors="coerce").fillna(0)
        wf = val.eq(1)
        return wf, ~wf
    wf = pd.Series(True, index=df.index)
    nwf = pd.Series(False, index=df.index)
    return wf, nwf


def _wf_sod_overhead_split(df_wf: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    if df_wf is None or not isinstance(df_wf, pd.DataFrame) or df_wf.empty:
        return pd.Series(False, index=pd.Index([])), pd.Series(False, index=pd.Index([]))
    if "WF_LAYER2" in df_wf.columns:
        layer = df_wf["WF_LAYER2"].fillna("").astype(str).str.upper().str.strip()
        sod = layer.eq("SOD")
        overhead = layer.eq("OVERHEAD")
        if "WORKFORCE_TYPE" in df_wf.columns:
            wf_type = df_wf["WORKFORCE_TYPE"].fillna("").astype(str).str.upper().str.strip()
            is_team = wf_type.eq("TEAM")
            sod = sod | is_team
            overhead = overhead & ~is_team
        return sod, overhead
    if "WORKFORCE_TYPE" in df_wf.columns:
        wf_type = df_wf["WORKFORCE_TYPE"].fillna("").astype(str).str.upper().str.strip()
        overhead = wf_type.eq("PROGRAM")
        sod = ~overhead
        return sod, overhead
    sod = pd.Series(True, index=df_wf.index)
    overhead = pd.Series(False, index=df_wf.index)
    return sod, overhead


def _series_text(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(dtype=str)
    out = pd.Series("", index=df.index, dtype=str)
    for col in cols:
        if col in df.columns:
            vals = df[col].fillna("").astype(str).str.strip()
            out = out.where(out.ne(""), vals)
    return out.fillna("").astype(str).str.lower()


def _is_wf(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([]))
    if "COST_CATEGORY" in df.columns:
        cat = df["COST_CATEGORY"].fillna("").astype(str).str.upper().str.strip()
        exact = cat.eq("WORK_FORCE")
        if exact.any():
            return exact
        return cat.str.contains("WORK") & ~cat.str.contains("NON")
    if "IS_WORKFORCE" in df.columns:
        return pd.to_numeric(df["IS_WORKFORCE"], errors="coerce").fillna(0).eq(1)
    return pd.Series(False, index=df.index)


def _is_program_overhead(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([]))
    wf_type = _series_text(df, ["WORKFORCE_TYPE"])
    teamname = _series_text(df, ["TEAMNAME"])
    subcomponent = _series_text(df, ["SUBCOMPONENT"])
    return wf_type.eq("program") | teamname.str.contains("program overhead") | subcomponent.eq("program")


def _sod_component_breakdown(df: pd.DataFrame) -> dict:
    out = {"Delivery": 0.0, "Contractor C": 0.0, "Contractor CS": 0.0, "Team OH": 0.0}
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or "AMOUNT" not in df.columns:
        return out
    amt = pd.to_numeric(df.get("AMOUNT"), errors="coerce").fillna(0.0)
    df = df.loc[amt > 0].copy()
    amt = amt.loc[df.index]
    if df.empty:
        return out

    if "SOD_LAYER2" in df.columns and "SOD_SUBTYPE" in df.columns:
        layer = df["SOD_LAYER2"].fillna("").astype(str).str.upper().str.strip()
        subtype = df["SOD_SUBTYPE"].fillna("").astype(str).str.upper().str.strip()
        is_sod = layer.eq("SOD")
        df_sod = df.loc[is_sod].copy()
        sod_amt = amt.loc[df_sod.index] if not df_sod.empty else pd.Series([], dtype=float)
        if not df_sod.empty:
            text_cols = ["SOD_LAYER2", "SOD_SUBTYPE", "WF_SUBTYPE", "SUBCOMPONENT", "SUBCOMPONENT_ROLLUP", "COST_BUCKET"]
            text = _series_text(df_sod, text_cols)
            sub_u = df_sod.get("SUBCOMPONENT", pd.Series("", index=df_sod.index)).fillna("").astype(str).str.upper().str.strip()
            wf_sub_u = df_sod.get("WF_SUBTYPE", pd.Series("", index=df_sod.index)).fillna("").astype(str).str.upper().str.strip()

            is_team = (
                text.str.contains("team overhead")
                | subtype.eq("TEAM OVERHEAD")
                | (sub_u.eq("TEAM") & layer.loc[df_sod.index].eq("SOD") & wf_sub_u.str.contains("OVERHEAD", na=False))
            )
            is_contractor_cs = text.str.contains("contractor cs|cs contractor")
            is_contractor_c = text.str.contains("contractor c") | (text.str.contains("contractor") & ~text.str.contains("cs|msp"))
            is_delivery = text.str.contains("delivery")

            out["Team OH"] = float(sod_amt.loc[is_team].sum() or 0.0)
            out["Contractor C"] = float(sod_amt.loc[is_contractor_c & ~is_team].sum() or 0.0)
            out["Contractor CS"] = float(sod_amt.loc[is_contractor_cs & ~is_team].sum() or 0.0)
            delivered = float(sod_amt.loc[is_delivery & ~is_team & ~is_contractor_c & ~is_contractor_cs].sum() or 0.0)
            remainder = float(sod_amt.sum() or 0.0) - (out["Team OH"] + out["Contractor C"] + out["Contractor CS"] + delivered)
            out["Delivery"] = delivered + max(remainder, 0.0)
            return out

    wf_mask = _is_wf(df)
    df_wf = df.loc[wf_mask].copy()
    wf_amt = amt.loc[df_wf.index] if not df_wf.empty else pd.Series([], dtype=float)
    if df_wf.empty:
        return out

    prog_oh = _is_program_overhead(df_wf)
    df_sod = df_wf.loc[~prog_oh].copy()
    sod_amt = wf_amt.loc[df_sod.index] if not df_sod.empty else pd.Series([], dtype=float)
    if df_sod.empty:
        return out

    text_cols = ["WF_SUBTYPE", "LABOR_TYPE", "COST_SUBTYPE", "SUBCOMPONENT", "SUBCOMPONENT_ROLLUP", "COST_BUCKET", "TEAMNAME", "RESOURCE_TYPE", "CLASS"]
    series = _series_text(df_sod, text_cols)
    sub_u = df_sod.get("SUBCOMPONENT", pd.Series("", index=df_sod.index)).fillna("").astype(str).str.upper().str.strip()
    wf_sub_u = df_sod.get("WF_SUBTYPE", pd.Series("", index=df_sod.index)).fillna("").astype(str).str.upper().str.strip()

    is_team = (
        _series_text(df_sod, ["WORKFORCE_TYPE"]).eq("team")
        | series.str.contains("team overhead")
        | (sub_u.eq("TEAM") & wf_sub_u.str.contains("OVERHEAD", na=False))
    )
    is_contractor_cs = series.str.contains("contractor cs|cs contractor")
    is_contractor = series.str.contains("contractor") & ~series.str.contains("contractor cs|cs contractor|msp")
    is_delivery = series.str.contains("delivery")

    out["Team OH"] = float(sod_amt.loc[is_team].sum() or 0.0)
    out["Contractor C"] = float(sod_amt.loc[is_contractor & ~is_team].sum() or 0.0)
    out["Contractor CS"] = float(sod_amt.loc[is_contractor_cs & ~is_team].sum() or 0.0)
    delivered = sod_amt.loc[is_delivery & ~is_team & ~is_contractor & ~is_contractor_cs].sum() if not sod_amt.empty else 0.0
    remainder = float(sod_amt.sum() or 0.0) - (out["Team OH"] + out["Contractor C"] + out["Contractor CS"] + float(delivered or 0.0))
    out["Delivery"] = float(delivered or 0.0) + max(remainder, 0.0)
    return out


def _has_data(df: pd.DataFrame) -> bool:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False
    amt = _to_amount(df)
    return bool(amt.size) and float(amt.sum() or 0.0) != 0.0


def _fmt_currency(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"${x:,.0f}"


def _fmt_currency_compact(x: Optional[float]) -> str:
    if x is None:
        return "—"
    v = float(x)
    av = abs(v)
    if av >= 1_000_000_000:
        return f"${v/1_000_000_000:.1f}B"
    if av >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if av >= 1_000:
        return f"${v/1_000:.0f}K"
    return f"${v:,.0f}"


def _fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x:.1f}%"


def _fmt_pp(x: Optional[float]) -> str:
    if x is None:
        return "—"
    sign = "+" if x > 0 else ""
    return f"{sign}{x:.1f} pp"


def _scenario_metrics(df: pd.DataFrame) -> dict:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return {"total": None, "wf_total": None, "nwf_total": None, "sod_total": None, "contractor_cs_total": None}
    amt = _to_amount(df)
    if amt.empty:
        return {"total": None, "wf_total": None, "nwf_total": None, "sod_total": None, "contractor_cs_total": None}

    wf_mask, nwf_mask = _wf_nwf_mask(df)
    total = float(amt.sum() or 0.0)
    wf_total = float(amt.loc[wf_mask].sum() or 0.0)
    nwf_total = float(amt.loc[nwf_mask].sum() or 0.0)

    contractor_cs_total = 0.0
    if "SOD_LAYER2" in df.columns and "SOD_SUBTYPE" in df.columns:
        layer = df["SOD_LAYER2"].fillna("").astype(str).str.upper().str.strip()
        subtype = df["SOD_SUBTYPE"].fillna("").astype(str).str.upper().str.strip()
        sod_mask = layer.eq("SOD")
        cs_mask = sod_mask & subtype.eq("CONTRACTOR CS")
        sod_wf_mask = sod_mask & ~cs_mask
        sod_total = float(amt.loc[sod_wf_mask].sum() or 0.0)
        contractor_cs_total = float(amt.loc[cs_mask].sum() or 0.0)
    else:
        df_wf = df.loc[wf_mask].copy()
        wf_amt = amt.loc[df_wf.index] if not df_wf.empty else pd.Series([], dtype=float)
        sod_mask, _overhead_mask = _wf_sod_overhead_split(df_wf)
        sod_total = float(wf_amt.loc[sod_mask].sum() or 0.0) if not wf_amt.empty else 0.0

    return {
        "total": total,
        "wf_total": wf_total,
        "nwf_total": nwf_total,
        "sod_total": sod_total,
        "contractor_cs_total": contractor_cs_total,
    }


def _option_base(theme: dict) -> dict:
    text_col = theme.get("echarts_text", theme.get("text", "#f5f5f5"))
    axis_col = theme.get("echarts_axis_text_dim", theme.get("echarts_axis_text", theme.get("muted", "#b3b3b3")))
    tooltip_bg = theme.get("echarts_tooltip_glass_bg", theme.get("echarts_tooltip_bg", "#3B3B3B"))
    tooltip_border = theme.get("echarts_tooltip_glass_border", theme.get("echarts_tooltip_border", "#2a2a2a"))
    sem = echarts_semantic_colors(theme)
    return {
        "textStyle": {"color": text_col},
        "color": [sem["baseline"], sem["forecast"], sem["actual"], sem["risk"], sem["neutral"]],
        "animationDuration": int(float(str(theme.get("echarts_anim_duration_enter_ms", "450")))),
        "animationDurationUpdate": int(float(str(theme.get("echarts_anim_duration_update_ms", "300")))),
        "tooltip": {
            "trigger": "item",
            "backgroundColor": tooltip_bg,
            "borderColor": tooltip_border,
            "borderWidth": 1,
            "textStyle": {"color": text_col},
            "extraCssText": (
                "backdrop-filter: blur(6px); border-radius:10px; "
                f"box-shadow:{theme.get('echarts_tooltip_shadow', '0 10px 30px rgba(0,0,0,0.35)')};"
            ),
        },
        "xAxis": {
            "axisLabel": {"color": axis_col},
            "axisLine": {"lineStyle": {"color": axis_col}},
            "splitLine": {"lineStyle": {"color": theme.get("echarts_gridline_soft", "rgba(148,163,184,0.10)")}},
        },
        "yAxis": {
            "axisLabel": {"color": axis_col},
            "axisLine": {"lineStyle": {"color": axis_col}},
            "splitLine": {"lineStyle": {"color": theme.get("echarts_gridline_soft", "rgba(148,163,184,0.10)")}},
        },
    }


def _placeholder_bar(theme: dict, title: str) -> dict:
    base = _option_base(theme)
    base["tooltip"]["trigger"] = "axis"
    return {
        **base,
        "title": {"text": title, "left": "left", "textStyle": {"color": base["textStyle"]["color"]}},
        "grid": {"left": 40, "right": 20, "top": 60, "bottom": 30},
        "xAxis": {"type": "category", "data": [f"App {i}" for i in range(1, 11)]},
        "yAxis": {"type": "value"},
        "series": [
            {
                "type": "bar",
                "data": [92, 86, 80, 73, 66, 59, 53, 47, 42, 38],
                "itemStyle": {"color": "#5B7BE3"},
            }
        ],
    }


def _pick_app_col(df: pd.DataFrame) -> Optional[str]:
    for col in ["GROUPNAME", "APP_GROUP", "APPGROUP", "APP_NAME", "APPNAME", "APPLICATION", "APPLICATION_NAME"]:
        if col in df.columns:
            return col
    return None


def _top_cost_drivers_by_app_option(df: pd.DataFrame, *, top_n: int = 10, theme: dict) -> tuple[dict, Optional[str], pd.DataFrame]:
    option = _placeholder_bar(theme, "")
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or "AMOUNT" not in df.columns:
        return option, None, pd.DataFrame()

    app_col = _pick_app_col(df)
    if not app_col:
        app_col = None

    df_work = df.copy()
    df_work["AMOUNT"] = pd.to_numeric(df_work.get("AMOUNT"), errors="coerce").fillna(0.0)
    df_work = df_work.loc[df_work["AMOUNT"] > 0].copy()
    if df_work.empty:
        return option, app_col, pd.DataFrame()

    if app_col:
        df_work["APP_KEY"] = df_work[app_col].fillna("").astype(str).str.strip()
        df_work = df_work.loc[df_work["APP_KEY"].ne("")].copy()
    else:
        return option, app_col, pd.DataFrame()

    agg = df_work.groupby("APP_KEY", dropna=False)["AMOUNT"].sum().reset_index()
    agg = agg.sort_values("AMOUNT", ascending=False).head(top_n)
    if agg.empty:
        return option, app_col, pd.DataFrame()

    app_labels = agg["APP_KEY"].astype(str).tolist()
    app_values = agg["AMOUNT"].astype(float).tolist()

    def _color_for_app(label: str) -> str:
        if not APP_COLORS:
            return option["series"][0].get("itemStyle", {}).get("color", "#5B7BE3")
        idx_color = (sum(ord(c) for c in str(label)) if label else 0) % len(APP_COLORS)
        return APP_COLORS[idx_color]

    option["xAxis"]["data"] = app_labels
    option["series"][0]["data"] = [
        {"value": v, "itemStyle": {"color": _color_for_app(app)}} for app, v in zip(app_labels, app_values)
    ]
    axis_label = option["xAxis"].get("axisLabel", {})
    axis_label.update(
        {
            "interval": 1,
            "rotate": 0,
            "hideOverlap": False,
            "margin": 18,
            "overflow": "truncate",
            "width": 120,
        }
    )
    option["xAxis"]["axisLabel"] = axis_label
    grid = option.get("grid", {})
    grid.update({"left": 40, "right": 40, "top": 30, "bottom": 90, "containLabel": True})
    option["grid"] = grid
    return option, app_col, agg


def _period_key_frame(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    cols = [c for c in ["YEAR", "PI", value_col] if c in (df.columns if isinstance(df, pd.DataFrame) else [])]
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or len(cols) < 3:
        return pd.DataFrame(columns=["PERIOD_KEY", "PERIOD_LABEL", value_col])
    work = df[["YEAR", "PI", value_col]].copy()
    work["YEAR"] = pd.to_numeric(work.get("YEAR"), errors="coerce")
    work["PI"] = pd.to_numeric(work.get("PI"), errors="coerce")
    work[value_col] = pd.to_numeric(work.get(value_col), errors="coerce").fillna(0.0).astype(float)
    work = work.loc[work["YEAR"].notna() & work["PI"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=["PERIOD_KEY", "PERIOD_LABEL", value_col])
    work["YEAR"] = work["YEAR"].astype(int)
    work["PI"] = work["PI"].astype(int)
    work["PERIOD_KEY"] = (work["YEAR"] * 10) + work["PI"]
    work["PERIOD_LABEL"] = work["YEAR"].astype(str) + " I" + work["PI"].astype(str)
    out = (
        work.groupby(["PERIOD_KEY", "PERIOD_LABEL"], dropna=False)[value_col]
        .sum()
        .reset_index()
        .sort_values("PERIOD_KEY")
    )
    return out


def _to_period_series(df: pd.DataFrame, period_col: str, value_col: str = "TOTAL_COST") -> tuple[list[str], list[float]]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or period_col not in df.columns or value_col not in df.columns:
        return [], []
    work = df[[period_col, value_col] + [c for c in ["PERIOD_KEY", "PERIOD_SORT"] if c in df.columns]].copy()
    work[value_col] = pd.to_numeric(work.get(value_col), errors="coerce").fillna(0.0).astype(float)
    if "PERIOD_KEY" in work.columns:
        grouped = (
            work.groupby(period_col, dropna=False)
            .agg({value_col: "sum", "PERIOD_KEY": "min"})
            .reset_index()
            .sort_values("PERIOD_KEY")
        )
    elif "PERIOD_SORT" in work.columns:
        grouped = (
            work.groupby(period_col, dropna=False)
            .agg({value_col: "sum", "PERIOD_SORT": "min"})
            .reset_index()
            .sort_values("PERIOD_SORT")
        )
    else:
        grouped = work.groupby(period_col, dropna=False)[value_col].sum().reset_index()
        try:
            grouped = grouped.sort_values(period_col)
        except Exception:
            pass
    labels = grouped[period_col].fillna("").astype(str).tolist()
    values = pd.to_numeric(grouped.get(value_col), errors="coerce").fillna(0.0).astype(float).tolist()
    return labels, values


def _sparkline_option(labels: list[str], values: list[float], tooltip_suffix: str = "") -> dict:
    base = _option_base(theme if isinstance(theme, dict) else _merged_theme())
    sem = echarts_semantic_colors(theme if isinstance(theme, dict) else _merged_theme())
    labels = [str(x) for x in (labels or [])]
    values = [float(v or 0.0) for v in (values or [])]
    if not labels or not values:
        return {
            **base,
            "grid": {"left": 6, "right": 6, "top": 8, "bottom": 6, "containLabel": False},
            "xAxis": {"type": "category", "data": [], "show": False},
            "yAxis": {"type": "value", "show": False},
            "series": [{"type": "line", "showSymbol": False, "data": []}],
            "tooltip": {"show": False},
        }
    finite_vals = [float(v) for v in values if pd.notna(v)]
    v_min = min(finite_vals) if finite_vals else 0.0
    v_max = max(finite_vals) if finite_vals else 0.0
    if v_max == v_min:
        pad = max(1.0, abs(v_max) * 0.15)
        y_min = v_min - pad
        y_max = v_max + pad
    else:
        span = (v_max - v_min) * 0.15
        y_min = v_min - span
        y_max = v_max + span
    return {
        **base,
        "animationDuration": 260,
        "animationDurationUpdate": 180,
        "grid": {"left": 6, "right": 6, "top": 10, "bottom": 8, "containLabel": False},
        "xAxis": {"type": "category", "data": labels, "show": False, "boundaryGap": False},
        "yAxis": {"type": "value", "show": False, "min": y_min, "max": y_max, "splitLine": {"show": False}},
        "tooltip": {
            "trigger": "axis",
            "formatter": "{b}<br/>{c}" + str(tooltip_suffix or ""),
        },
        "series": [
            {
                "type": "line",
                "smooth": True,
                "showSymbol": False,
                "data": values,
                "lineStyle": {"width": 2, "color": sem["forecast"]},
            }
        ],
    }


NWF_ALIASES: dict[str, list[str]] = {
    "invoices": ["invoice", "invoices"],
    "cloud_azure": ["azure"],
    "cloud_aws": ["aws"],
    "msp": ["msp", "managed service", "managed services"],
    "contractor_cs": ["contractor cs", "cs contractor", "contractor"],
    "travel": ["travel"],
}

FG_CATEGORY_COL_HINTS = (
    "CATEGORY",
    "COST_TYPE",
    "COST_CATEGORY",
    "SERVICE",
    "TOWER",
    "NWF",
    "EXPENSE",
    "COST_NAME",
    "SUBCOMPONENT",
)
FG_PLAN_COL_PRIORITY = (
    "COST_SOURCE",
    "COST_BUCKET",
    "COST_GROUP",
    "IS_ADDITIONAL",
    "ALLOC_TYPE",
    "SOURCE",
    "SUBCOMPONENT",
)
FG_PLAN_KEYWORDS = ("additional", "program additional", "addl", "program-level", "program level", "program_nwf")


def _fg_norm_text(val: Any) -> str:
    txt = str(val or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "", txt)


def _fg_match_nwf_alias(val: Any) -> Optional[str]:
    raw = str(val or "").strip().lower()
    norm = _fg_norm_text(val)
    if not raw and not norm:
        return None
    if "invoice" in raw or "invoice" in norm:
        return "invoices"
    if "azure" in raw:
        return "cloud_azure"
    if re.search(r"\baws\b", raw) or "amazonwebservices" in norm:
        return "cloud_aws"
    if "msp" in raw or "managedservice" in norm:
        return "msp"
    if ("contractor" in raw and "cs" in raw) or "cscontractor" in norm:
        return "contractor_cs"
    if "travel" in raw:
        return "travel"
    return None


def _fg_pick_value_col(df: pd.DataFrame) -> Optional[str]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    for col in ["TOTAL_COST", "AMOUNT"]:
        if col in df.columns:
            return col
    numeric_cols = [
        c
        for c in df.columns
        if c not in {"YEAR", "PI", "PERIOD_KEY", "PERIOD_SORT"}
        and pd.api.types.is_numeric_dtype(df[c])
    ]
    return str(numeric_cols[0]) if numeric_cols else None


def _fg_infer_periods_per_year(pi_values: list[int]) -> int:
    pis = sorted({int(p) for p in pi_values if pd.notna(p) and int(p) > 0})
    if pis and min(pis) >= 1 and max(pis) <= 4:
        return 4
    if pis and min(pis) >= 1 and max(pis) <= 12:
        return 12
    return 12


def _fg_period_frame(df: pd.DataFrame, value_col: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    empty = pd.DataFrame(columns=["PERIOD_KEY", "PERIOD_LABEL", "VALUE"])
    meta: dict[str, Any] = {"period_col": None, "period_mode": "none", "pi_values": []}
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or value_col not in df.columns:
        return empty, meta

    if "PERIOD_END_DATE" in df.columns:
        work = df.copy()
        work["VALUE"] = pd.to_numeric(work.get(value_col), errors="coerce").fillna(0.0).astype(float)
        work["_PERIOD_DT"] = pd.to_datetime(work.get("PERIOD_END_DATE"), errors="coerce")
        work = work.loc[work["_PERIOD_DT"].notna()].copy()
        if not work.empty:
            work["PERIOD_KEY"] = work["_PERIOD_DT"].astype("int64")
            work["PERIOD_LABEL"] = work["_PERIOD_DT"].dt.strftime("%Y-%m")
            out = (
                work.groupby(["PERIOD_KEY", "PERIOD_LABEL"], dropna=False)["VALUE"]
                .sum()
                .reset_index()
                .sort_values("PERIOD_KEY")
            )
            meta.update({"period_col": "PERIOD_END_DATE", "period_mode": "date"})
            return out, meta

    if {"YEAR", "PI"}.issubset(set(df.columns)):
        pi_work = _period_key_frame(df.rename(columns={value_col: "TOTAL_COST"}) if value_col != "TOTAL_COST" else df, "TOTAL_COST")
        if not pi_work.empty:
            out = pi_work.rename(columns={"TOTAL_COST": "VALUE"})[["PERIOD_KEY", "PERIOD_LABEL", "VALUE"]].copy()
            pi_vals = pd.to_numeric(df.get("PI"), errors="coerce").dropna().astype(int).tolist()
            meta.update({"period_col": "PI", "period_mode": "pi", "pi_values": pi_vals})
            return out, meta

    if "PERIOD_KEY" in df.columns:
        work = df.copy()
        work["VALUE"] = pd.to_numeric(work.get(value_col), errors="coerce").fillna(0.0).astype(float)
        work["PERIOD_KEY"] = pd.to_numeric(work.get("PERIOD_KEY"), errors="coerce")
        work = work.loc[work["PERIOD_KEY"].notna()].copy()
        if not work.empty:
            work["PERIOD_KEY"] = work["PERIOD_KEY"].astype(int)
            label_col = "PERIOD_LABEL" if "PERIOD_LABEL" in work.columns else ("PI_NAME" if "PI_NAME" in work.columns else "PERIOD_KEY")
            work["PERIOD_LABEL"] = work.get(label_col, "").fillna("").astype(str).replace({"": pd.NA})
            work["PERIOD_LABEL"] = work["PERIOD_LABEL"].fillna(work["PERIOD_KEY"].astype(str))
            out = (
                work.groupby(["PERIOD_KEY", "PERIOD_LABEL"], dropna=False)["VALUE"]
                .sum()
                .reset_index()
                .sort_values("PERIOD_KEY")
            )
            meta.update({"period_col": "PERIOD_KEY", "period_mode": "key"})
            return out, meta

    for col in ["PERIOD", "PI_NAME", "PERIOD_LABEL"]:
        if col in df.columns:
            work = df.copy()
            work["VALUE"] = pd.to_numeric(work.get(value_col), errors="coerce").fillna(0.0).astype(float)
            work["PERIOD_LABEL"] = work.get(col, "").fillna("").astype(str).str.strip()
            work = work.loc[work["PERIOD_LABEL"].ne("")].copy()
            if not work.empty:
                out = work.groupby("PERIOD_LABEL", dropna=False)["VALUE"].sum().reset_index()
                out["PERIOD_KEY"] = range(1, len(out.index) + 1)
                out = out[["PERIOD_KEY", "PERIOD_LABEL", "VALUE"]]
                meta.update({"period_col": col, "period_mode": "label"})
                return out, meta

    return empty, meta


def _fg_detect_category_column(df: pd.DataFrame) -> tuple[Optional[str], dict[str, Any]]:
    debug: dict[str, Any] = {"candidates": {}, "selected": None, "selected_unique_sample": []}
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None, debug
    string_cols = [c for c in df.columns if pd.api.types.is_string_dtype(df[c]) or df[c].dtype == object]
    best_col = None
    best_score = -1
    best_hits = -1
    for col in string_cols:
        series = df[col].fillna("").astype(str).str.strip()
        if series.eq("").all():
            continue
        col_u = str(col).upper()
        name_score = sum(2 for hint in FG_CATEGORY_COL_HINTS if hint in col_u)
        if col_u in {"SUBCOMPONENT", "SUBCOMPONENT_ROLLUP"}:
            name_score += 3
        sample = series.loc[series.ne("")].head(240)
        value_hits = int(sample.map(lambda x: _fg_match_nwf_alias(x) is not None).sum())
        score = name_score + min(value_hits, 8)
        debug["candidates"][str(col)] = {"score": int(score), "name_score": int(name_score), "value_hits": int(value_hits)}
        if score > best_score or (score == best_score and value_hits > best_hits):
            best_col = str(col)
            best_score = int(score)
            best_hits = int(value_hits)
    if not best_col or best_hits <= 0:
        return None, debug
    uniques = (
        df[best_col]
        .fillna("")
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
        .drop_duplicates()
        .head(16)
        .tolist()
    )
    debug["selected"] = best_col
    debug["selected_unique_sample"] = uniques
    return best_col, debug


def _fg_detect_plan_rows(df_baseline: pd.DataFrame) -> tuple[pd.Series, dict[str, Any]]:
    if df_baseline is None or not isinstance(df_baseline, pd.DataFrame) or df_baseline.empty:
        return pd.Series(False, index=pd.Index([])), {
            "plan_col": None,
            "additional_detected": False,
            "fallback_used": True,
            "candidate_hits": {},
        }
    best_col = None
    best_mask = pd.Series(False, index=df_baseline.index)
    best_hits = 0
    candidate_hits: dict[str, int] = {}
    for col in FG_PLAN_COL_PRIORITY:
        if col not in df_baseline.columns:
            continue
        ser = df_baseline[col]
        mask = pd.Series(False, index=df_baseline.index)
        if str(col).upper() == "IS_ADDITIONAL":
            num = pd.to_numeric(ser, errors="coerce").fillna(0.0)
            txt = ser.fillna("").astype(str).str.strip().str.lower()
            mask = num.gt(0) | txt.isin({"true", "yes", "y", "1"})
        else:
            txt = ser.fillna("").astype(str).str.strip().str.lower()
            norm = txt.map(_fg_norm_text)
            mask = (
                txt.str.contains("additional", na=False)
                | txt.str.contains("program-level", na=False)
                | txt.str.contains("program level", na=False)
                | txt.str.contains("addl", na=False)
                | norm.str.contains("programadditional", na=False)
                | norm.str.contains("programnwf", na=False)
            )
        hits = int(mask.sum())
        candidate_hits[str(col)] = hits
        if hits > best_hits:
            best_hits = hits
            best_col = str(col)
            best_mask = mask
    return best_mask, {
        "plan_col": best_col,
        "additional_detected": bool(best_hits > 0),
        "fallback_used": bool(best_hits <= 0),
        "candidate_hits": candidate_hits,
    }


@cache_data_portfolio(ttl=180, show_spinner=False)
def _compute_financial_governance_kpis(
    *,
    year: int,
    scope_key: str,
    filters_sig: str,
    filters_scope: dict[str, Any],
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> dict[str, Any]:
    _ = (filters_sig, user_scope_sig, cache_buster)
    scope = json.loads(scope_key)
    cm = load_cost_model(int(year), scope)
    filters = {**(filters_scope or {}), "group_by": ["COST_CATEGORY", "SUBCOMPONENT", "SOURCE"]}
    df_actual = get_pi_costs(cm.get("ACTUAL", pd.DataFrame()), scenario="Actual", filters=filters)
    df_baseline = get_pi_costs(cm.get("BASELINE", pd.DataFrame()), scenario="Baseline", filters=filters)

    value_col_actual = _fg_pick_value_col(df_actual)
    value_col_plan = _fg_pick_value_col(df_baseline)
    category_col, category_debug = _fg_detect_category_column(df_actual if isinstance(df_actual, pd.DataFrame) else pd.DataFrame())

    debug: dict[str, Any] = {
        "actual_rows": int(df_actual.shape[0]) if isinstance(df_actual, pd.DataFrame) else 0,
        "baseline_rows": int(df_baseline.shape[0]) if isinstance(df_baseline, pd.DataFrame) else 0,
        "actual_columns": list(df_actual.columns) if isinstance(df_actual, pd.DataFrame) else [],
        "baseline_columns": list(df_baseline.columns) if isinstance(df_baseline, pd.DataFrame) else [],
        "value_col_actual": value_col_actual,
        "value_col_plan": value_col_plan,
        "category_detection": category_debug,
        "nwf_aliases": NWF_ALIASES,
        "plan_keywords": list(FG_PLAN_KEYWORDS),
    }

    warn_message = None
    actual_nwf = pd.DataFrame()
    alias_counts: dict[str, int] = {}
    if (
        isinstance(df_actual, pd.DataFrame)
        and not df_actual.empty
        and value_col_actual
        and category_col
        and category_col in df_actual.columns
    ):
        actual_work = df_actual.copy()
        actual_work["_FG_ALIAS"] = actual_work[category_col].map(_fg_match_nwf_alias)
        actual_nwf = actual_work.loc[actual_work["_FG_ALIAS"].notna()].copy()
        if not actual_nwf.empty:
            alias_counts = (
                actual_nwf["_FG_ALIAS"]
                .fillna("")
                .astype(str)
                .value_counts(dropna=False)
                .to_dict()
            )
    elif category_col is None:
        warn_message = "NWF category dimension not detected in this scope. Please verify Apptio export/category mapping."

    run_period, period_meta = _fg_period_frame(actual_nwf, value_col_actual) if value_col_actual else (pd.DataFrame(), {"pi_values": []})
    run_labels = run_period.get("PERIOD_LABEL", pd.Series(dtype=str)).fillna("").astype(str).tolist()
    run_values_raw = run_period["VALUE"] if "VALUE" in run_period.columns else pd.Series(dtype=float)
    run_values = pd.to_numeric(run_values_raw, errors="coerce").fillna(0.0).astype(float).tolist()
    ytd_actual = float(sum(run_values) or 0.0)
    elapsed_periods = int(sum(1 for v in run_values if float(v) > 0.0))
    periods_per_year = _fg_infer_periods_per_year(list(period_meta.get("pi_values") or []))
    avg_per_period = (ytd_actual / elapsed_periods) if elapsed_periods > 0 else None
    run_rate = (avg_per_period * periods_per_year) if avg_per_period is not None else None
    outlook = run_rate

    plan_mask, plan_debug = _fg_detect_plan_rows(df_baseline if isinstance(df_baseline, pd.DataFrame) else pd.DataFrame())
    debug["plan_detection"] = plan_debug
    debug["actual_nwf_alias_counts"] = alias_counts
    debug["period_meta"] = period_meta

    plan_fallback = True
    plan_label = "Plan (Baseline total, no additional-cost breakdown available)"
    plan_df = pd.DataFrame()
    if isinstance(df_baseline, pd.DataFrame) and not df_baseline.empty and value_col_plan:
        if plan_debug.get("additional_detected"):
            plan_fallback = False
            plan_label = "Plan (Additional Costs)"
            plan_df = df_baseline.loc[plan_mask].copy()
        else:
            plan_df = df_baseline.copy()
    plan_total = (
        float(
            pd.to_numeric(
                plan_df[value_col_plan] if value_col_plan in plan_df.columns else pd.Series(dtype=float),
                errors="coerce",
            )
            .fillna(0.0)
            .sum()
            or 0.0
        )
        if not plan_df.empty and value_col_plan
        else 0.0
    )

    variance_amt = (float(outlook) - float(plan_total)) if (outlook is not None) else None
    variance_pct = (float(variance_amt) / float(plan_total) * 100.0) if (variance_amt is not None and plan_total > 0) else None

    variance_labels = list(run_labels)
    variance_values = list(run_values)
    variance_mode = "actual_series"
    if not plan_fallback and value_col_plan and not plan_df.empty:
        plan_period, _ = _fg_period_frame(plan_df, value_col_plan)
        if not plan_period.empty and not run_period.empty:
            actual_map = {
                int(k): float(v)
                for k, v in zip(run_period.get("PERIOD_KEY", []), run_period.get("VALUE", []))
            }
            plan_map = {
                int(k): float(v)
                for k, v in zip(plan_period.get("PERIOD_KEY", []), plan_period.get("VALUE", []))
            }
            drift_vals: list[float] = []
            for key in run_period.get("PERIOD_KEY", []):
                a = float(actual_map.get(int(key), 0.0) or 0.0)
                p = float(plan_map.get(int(key), 0.0) or 0.0)
                drift_vals.append((a - p) / p * 100.0 if p > 0 else 0.0)
            variance_values = drift_vals
            variance_mode = "drift_pct"

    debug["warning"] = warn_message
    return {
        "run_rate": run_rate,
        "outlook": outlook,
        "plan_total": plan_total,
        "variance_amt": variance_amt,
        "variance_pct": variance_pct,
        "run_labels": run_labels,
        "run_values": run_values,
        "outlook_labels": list(run_labels),
        "outlook_values": list(run_values),
        "variance_labels": variance_labels,
        "variance_values": variance_values,
        "variance_mode": variance_mode,
        "ytd_actual": ytd_actual,
        "elapsed_periods": elapsed_periods,
        "periods_per_year": periods_per_year,
        "plan_label": plan_label,
        "plan_fallback": plan_fallback,
        "warning_message": warn_message,
        "debug": debug,
    }


@cache_data_portfolio(ttl=180, show_spinner=False)
def _compute_cost_driver_kpis(
    *,
    year: int,
    scope_key: str,
    filters_kpi_signature: str,
    filters_kpi: dict[str, Any],
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> dict[str, Any]:
    _ = (filters_kpi_signature, user_scope_sig, cache_buster)
    scope = json.loads(scope_key)
    cm = load_cost_model(int(year), scope)
    pi_proj = get_pi_costs(cm.get("EXPECTED", pd.DataFrame()), scenario="Projected", filters=filters_kpi)
    pi_act = get_pi_costs(cm.get("ACTUAL", pd.DataFrame()), scenario="Actual", filters=filters_kpi)
    pi_base = get_pi_costs(cm.get("BASELINE", pd.DataFrame()), scenario="Baseline", filters=filters_kpi)
    # KPI #2 risk must always be computed in-scope and not hidden by unmapped-visibility settings.
    lines_proj = get_cost_lines(
        cm.get("EXPECTED", pd.DataFrame()),
        scenario="Projected",
        filters={**filters_kpi, "include_unmapped": True},
    )

    proj_by_period = _period_key_frame(pi_proj, "TOTAL_COST")
    act_by_period = _period_key_frame(pi_act, "TOTAL_COST")
    base_by_period = _period_key_frame(pi_base, "TOTAL_COST")

    proj_map = {int(k): float(v) for k, v in zip(proj_by_period.get("PERIOD_KEY", []), proj_by_period.get("TOTAL_COST", []))}
    act_map = {int(k): float(v) for k, v in zip(act_by_period.get("PERIOD_KEY", []), act_by_period.get("TOTAL_COST", []))}
    base_map = {int(k): float(v) for k, v in zip(base_by_period.get("PERIOD_KEY", []), base_by_period.get("TOTAL_COST", []))}
    period_keys = sorted(set(proj_map.keys()) | set(act_map.keys()) | set(base_map.keys()))
    period_labels = {int(k): str(v) for k, v in zip(proj_by_period.get("PERIOD_KEY", []), proj_by_period.get("PERIOD_LABEL", []))}
    period_labels.update({int(k): str(v) for k, v in zip(act_by_period.get("PERIOD_KEY", []), act_by_period.get("PERIOD_LABEL", []))})
    period_labels.update({int(k): str(v) for k, v in zip(base_by_period.get("PERIOD_KEY", []), base_by_period.get("PERIOD_LABEL", []))})

    # KPI #1: Cost Efficiency Index = Actual total / Projected total.
    proj_total = float(pd.to_numeric(pi_proj.get("TOTAL_COST"), errors="coerce").fillna(0.0).sum() or 0.0)
    act_total = float(pd.to_numeric(pi_act.get("TOTAL_COST"), errors="coerce").fillna(0.0).sum() or 0.0)
    eff_keys = sorted(set(proj_map.keys()) & set(act_map.keys()))
    eff_ratio = (act_total / proj_total) if (proj_total > 0 and eff_keys) else None
    eff_rows = []
    for key in eff_keys:
        den = float(proj_map.get(key, 0.0) or 0.0)
        num = float(act_map.get(key, 0.0) or 0.0)
        eff_rows.append({"PERIOD_KEY": key, "PERIOD_LABEL": period_labels.get(key, str(key)), "TOTAL_COST": (num / den) if den > 0 else 0.0})
    eff_df = pd.DataFrame(eff_rows)
    eff_labels, eff_values = _to_period_series(eff_df, "PERIOD_LABEL", value_col="TOTAL_COST")

    proj_lines = lines_proj.copy() if isinstance(lines_proj, pd.DataFrame) else pd.DataFrame()
    if "AMOUNT" not in proj_lines.columns:
        proj_lines["AMOUNT"] = 0.0
    proj_lines["AMOUNT"] = pd.to_numeric(proj_lines.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    grp = proj_lines.get("GROUPNAME", pd.Series("", index=proj_lines.index)).fillna("").astype(str).str.strip()
    grp_u = grp.str.upper()
    map_status = (
        proj_lines.get("MAPPING_STATUS", pd.Series("", index=proj_lines.index))
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )
    # Treat canonical unmapped labels/statuses as "unassigned" risk, not only blank group names.
    unassigned_mask = (
        grp.eq("")
        | map_status.eq("UNMAPPED_APP_GROUP")
        | grp_u.isin({"(NEEDS MAPPING)", "(UNASSIGNED)", "UNASSIGNED"})
    )
    unassigned_total = float(proj_lines.loc[unassigned_mask, "AMOUNT"].sum() or 0.0)
    proj_lines_total = float(proj_lines["AMOUNT"].sum() or 0.0)
    # KPI #2: Cost at Risk = Projected unmapped cost / Projected total cost.
    risk_pct = (unassigned_total / proj_lines_total * 100.0) if proj_lines_total > 0 else None

    line_period = _period_key_frame(proj_lines.rename(columns={"AMOUNT": "TOTAL_COST"}), "TOTAL_COST")
    line_total_map = {int(k): float(v) for k, v in zip(line_period.get("PERIOD_KEY", []), line_period.get("TOTAL_COST", []))}
    unassigned_period = _period_key_frame(
        proj_lines.loc[unassigned_mask].rename(columns={"AMOUNT": "TOTAL_COST"}),
        "TOTAL_COST",
    )
    unassigned_map = {int(k): float(v) for k, v in zip(unassigned_period.get("PERIOD_KEY", []), unassigned_period.get("TOTAL_COST", []))}
    risk_keys = sorted(set(line_total_map.keys()) | set(unassigned_map.keys()))
    risk_rows = []
    for key in risk_keys:
        den = float(line_total_map.get(key, 0.0) or 0.0)
        num = float(unassigned_map.get(key, 0.0) or 0.0)
        risk_rows.append({"PERIOD_KEY": key, "PERIOD_LABEL": period_labels.get(key, str(key)), "TOTAL_COST": (num / den * 100.0) if den > 0 else 0.0})
    risk_df = pd.DataFrame(risk_rows)
    risk_labels, risk_values = _to_period_series(risk_df, "PERIOD_LABEL", value_col="TOTAL_COST")

    # KPI #3: Cost Volatility = avg(last 6) of |Actual - Baseline| / Baseline.
    vol_rows = []
    vol_keys = sorted(set(act_map.keys()) & set(base_map.keys()))
    for key in vol_keys:
        base_val = float(base_map.get(key, 0.0) or 0.0)
        act_val = float(act_map.get(key, 0.0) or 0.0)
        drift = (abs(act_val - base_val) / base_val * 100.0) if base_val > 0 else 0.0
        vol_rows.append({"PERIOD_KEY": key, "PERIOD_LABEL": period_labels.get(key, str(key)), "TOTAL_COST": drift})
    vol_df = pd.DataFrame(vol_rows)
    vol_labels, vol_values = _to_period_series(vol_df, "PERIOD_LABEL", value_col="TOTAL_COST")
    tail_vals = vol_values[-6:] if len(vol_values) >= 6 else vol_values
    vol_avg = (sum(tail_vals) / len(tail_vals)) if tail_vals else None

    return {
        "eff_ratio": eff_ratio,
        "eff_labels": eff_labels,
        "eff_values": eff_values,
        "risk_pct": risk_pct,
        "risk_unassigned": unassigned_total,
        "risk_labels": risk_labels,
        "risk_values": risk_values,
        "vol_avg": vol_avg,
        "vol_labels": vol_labels,
        "vol_values": vol_values,
    }


def _placeholder_stacked_bar(theme: dict, title: str) -> dict:
    base = _option_base(theme)
    sem = echarts_semantic_colors(theme)
    base["tooltip"]["trigger"] = "axis"
    return {
        **base,
        "title": {"text": title, "left": "left", "textStyle": {"color": base["textStyle"]["color"]}},
        "legend": {"top": 12, "left": "center", "textStyle": {"color": base["textStyle"]["color"]}},
        "grid": {"left": 50, "right": 20, "top": 70, "bottom": 30},
        "xAxis": {"type": "value"},
        "yAxis": {"type": "category", "data": ["Composition"]},
        "series": [
            {"name": "Delivery", "type": "bar", "stack": "total", "data": [55], "itemStyle": {"color": sem["forecast"]}},
            {"name": "Contractor C", "type": "bar", "stack": "total", "data": [25], "itemStyle": {"color": sem["neutral"]}},
            {"name": "Team Overhead", "type": "bar", "stack": "total", "data": [20], "itemStyle": {"color": sem["risk"]}},
        ],
    }


def _placeholder_area(theme: dict, title: str) -> dict:
    base = _option_base(theme)
    sem = echarts_semantic_colors(theme)
    base["tooltip"]["trigger"] = "axis"
    months = ["M1", "M2", "M3", "M4", "M5", "M6"]
    return {
        **base,
        "title": {"text": title, "left": "left", "textStyle": {"color": base["textStyle"]["color"]}},
        "grid": {"left": 40, "right": 20, "top": 60, "bottom": 30},
        "xAxis": {"type": "category", "data": months},
        "yAxis": {"type": "value"},
        "series": [
            {"name": "Invoices", "type": "line", "stack": "total", "areaStyle": {}, "lineStyle": {"color": sem["actual"]}, "itemStyle": {"color": sem["actual"]}, "data": [62, 64, 63, 66, 68, 70]},
            {"name": "Cloud Azure", "type": "line", "stack": "total", "areaStyle": {}, "lineStyle": {"color": sem["baseline"]}, "itemStyle": {"color": sem["baseline"]}, "data": [8, 9, 10, 11, 11, 12]},
            {"name": "Cloud AWS", "type": "line", "stack": "total", "areaStyle": {}, "lineStyle": {"color": sem["neutral"]}, "itemStyle": {"color": sem["neutral"]}, "data": [5, 6, 6, 7, 8, 8]},
            {"name": "MSP", "type": "line", "stack": "total", "areaStyle": {}, "lineStyle": {"color": sem["forecast"]}, "itemStyle": {"color": sem["forecast"]}, "data": [7, 7, 7, 6, 6, 6]},
            {"name": "Contractor CS", "type": "line", "stack": "total", "areaStyle": {}, "lineStyle": {"color": sem["risk"]}, "itemStyle": {"color": sem["risk"]}, "data": [10, 9, 8, 8, 7, 6]},
            {"name": "Travel", "type": "line", "stack": "total", "areaStyle": {}, "lineStyle": {"color": sem["neutral"]}, "itemStyle": {"color": sem["neutral"]}, "data": [2, 2, 2, 3, 3, 3]},
        ],
    }


def _short_label(name: str, max_len: int) -> str:
    s = (name or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _color_for_app_group(label: str) -> str:
    if not APP_COLORS:
        return "#5B7BE3"
    idx_color = (sum(ord(c) for c in str(label)) if label else 0) % len(APP_COLORS)
    return APP_COLORS[idx_color]


def _color_for_team(label: str) -> str:
    base_colors = ["#4C78A8", "#5B8DB8", "#6AA0C8", "#3E6F99", "#5E88B5"]
    idx_color = (sum(ord(c) for c in str(label)) if label else 0) % len(base_colors)
    return base_colors[idx_color]


def _get_fte_demand_df(df_expected: pd.DataFrame) -> pd.DataFrame:
    if df_expected is None or df_expected.empty:
        return pd.DataFrame()

    df = df_expected.copy()
    if "SCENARIO" in df.columns:
        scen = df["SCENARIO"].fillna("").astype(str).str.upper()
        df = df.loc[scen.eq("EXPECTED")].copy()
    if df.empty:
        return pd.DataFrame()

    wf_mask, _ = _wf_nwf_mask(df)
    df = df.loc[wf_mask].copy()
    if df.empty:
        return pd.DataFrame()

    group_col = "GROUPNAME" if "GROUPNAME" in df.columns else None
    if not group_col:
        return pd.DataFrame()

    group_val = df[group_col].fillna("").astype(str).str.strip()
    df = df.loc[group_val.ne("")].copy()
    df = df.loc[~group_val.str.upper().isin({"UNASSIGNED", "(UNASSIGNED)"})].copy()
    if "MAPPING_STATUS" in df.columns:
        mapped = df["MAPPING_STATUS"].fillna("").astype(str).str.upper()
        df = df.loc[mapped.eq("MAPPED")].copy()
    if df.empty:
        return pd.DataFrame()

    sod_col = None
    for col in ["SOD_LAYER2", "WF_LAYER2", "WORKFORCE_LAYER2"]:
        if col in df.columns:
            sod_col = col
            break
    if sod_col:
        sod_val = df[sod_col].fillna("").astype(str).str.upper()
        df = df.loc[sod_val.eq("SOD")].copy()

    if df.empty:
        return pd.DataFrame()

    fte_col = None
    for col in ["DERIVED_FTE", "FTE"]:
        if col in df.columns:
            fte_col = col
            break
    if not fte_col and "DERIVED_FTE_SWAG" in df.columns:
        fte_col = "DERIVED_FTE_SWAG"

    if fte_col:
        df["FTE"] = pd.to_numeric(df.get(fte_col), errors="coerce").fillna(0.0)
    else:
        unit = df.get("UNIT", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
        metric = df.get("METRIC", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
        is_fte = unit.eq("FTE") | metric.eq("FTE")
        df = df.loc[is_fte].copy()
        df["FTE"] = pd.to_numeric(df.get("AMOUNT"), errors="coerce").fillna(0.0)

    df = df.loc[df["FTE"] > 0].copy()
    if df.empty:
        return pd.DataFrame()

    df["TEAMNAME"] = df.get("TEAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    df["GROUPNAME"] = df[group_col].fillna("").astype(str).str.strip()
    df["PROGRAMNAME"] = df.get("PROGRAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    df = df.loc[df["TEAMNAME"].ne("")].copy()
    return df[["TEAMNAME", "GROUPNAME", "PROGRAMNAME", "FTE"]]


def _build_chord_links(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    return (
        df.groupby(["TEAMNAME", "GROUPNAME", "PROGRAMNAME"], dropna=False)["FTE"]
        .sum()
        .reset_index()
        .sort_values("FTE", ascending=False)
    )


def _collapse_top_n(df_links: pd.DataFrame, team_top_n: int = 8, app_top_n: int = 10) -> pd.DataFrame:
    if df_links is None or df_links.empty:
        return pd.DataFrame()
    df = df_links.copy()
    top_teams = df.groupby("TEAMNAME")["FTE"].sum().sort_values(ascending=False).head(team_top_n).index
    top_apps = df.groupby("GROUPNAME")["FTE"].sum().sort_values(ascending=False).head(app_top_n).index
    df.loc[~df["TEAMNAME"].isin(top_teams), "TEAMNAME"] = "Other Teams"
    df.loc[~df["GROUPNAME"].isin(top_apps), "GROUPNAME"] = "Other Apps"
    return df.groupby(["TEAMNAME", "GROUPNAME", "PROGRAMNAME"], dropna=False)["FTE"].sum().reset_index()


def _build_sod_demand_chord_option(theme: dict, df_expected: pd.DataFrame) -> tuple[dict, dict]:
    base = _option_base(theme)
    debug: dict[str, Any] = {
        "reason": "",
        "total_fte": 0.0,
        "teams": 0,
        "apps": 0,
        "links": 0,
        "df_rows": 0,
        "df_filtered_rows": 0,
        "df_columns": list(df_expected.columns) if isinstance(df_expected, pd.DataFrame) else [],
    }
    if df_expected is None or df_expected.empty:
        debug["reason"] = "df_expected_empty"
        return {"series": []}, debug

    df = _get_fte_demand_df(df_expected)
    debug["df_rows"] = int(df_expected.shape[0])
    debug["df_filtered_rows"] = int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0
    links_df = _build_chord_links(df)
    links_df = _collapse_top_n(links_df, team_top_n=8, app_top_n=10)
    if links_df is None or links_df.empty:
        debug["reason"] = "no_links_after_filters"
        return {"series": []}, debug

    total_fte = float(links_df["FTE"].sum() or 0.0)
    teams = links_df["TEAMNAME"].unique().tolist()
    apps = links_df["GROUPNAME"].unique().tolist()
    team_totals = links_df.groupby("TEAMNAME")["FTE"].sum().to_dict()
    app_totals = links_df.groupby("GROUPNAME")["FTE"].sum().to_dict()

    nodes = []
    for t in teams:
        nodes.append(
            {
                "name": f"TEAM::{t}",
                "fullName": t,
                "displayLabel": _short_label(t, 14),
                "category": 0,
                "value": float(team_totals.get(t, 0.0)),
                "itemStyle": {"color": _color_for_team(t)},
            }
        )
    for a in apps:
        nodes.append(
            {
                "name": f"APP::{a}",
                "fullName": a,
                "displayLabel": _short_label(a, 16),
                "category": 1,
                "value": float(app_totals.get(a, 0.0)),
                "itemStyle": {"color": _color_for_app_group(a)},
            }
        )

    links_data = []
    max_link = float(links_df["FTE"].max() or 0.0)
    max_node = float(max(max(team_totals.values(), default=0.0), max(app_totals.values(), default=0.0)))
    for _, row in links_df.iterrows():
        fte_val = float(row["FTE"] or 0.0)
        width = max(1, 1 + (6 * fte_val / max_link if max_link else 1))
        links_data.append(
            {
                "source": f"TEAM::{row['TEAMNAME']}",
                "target": f"APP::{row['GROUPNAME']}",
                "value": fte_val,
                "teamFull": row["TEAMNAME"],
                "appFull": row["GROUPNAME"],
                "fte": fte_val,
                "lineStyle": {"width": width, "opacity": 0.28},
            }
        )

    node_names = {n["name"] for n in nodes}
    missing = [
        {"source": link["source"], "target": link["target"]}
        for link in links_data
        if link["source"] not in node_names or link["target"] not in node_names
    ]

    for node in nodes:
        node_val = float(node.get("value") or 0.0)
        base_size = 12 + (28 * (node_val / max_node) ** 0.5 if max_node else 8)
        node["symbolSize"] = max(12, min(46, base_size))
        node["symbol"] = "circle"

    tooltip_fn = """
    function (params) {
      if (params.dataType === 'edge') {
        var team = params.data.teamFull || params.data.source;
        var app = params.data.appFull || params.data.target;
        var val = params.data.value || 0;
        var pct = val / TOTAL_FTE * 100;
        return team + " → " + app + "<br/>FTE: " + val.toFixed(2) + "<br/>Share: " + pct.toFixed(1) + "%";
      }
      var full = params.data.fullName || params.name;
      var val = params.data.value || 0;
      return full + "<br/>Total FTE: " + val.toFixed(2);
    }
    """.replace("TOTAL_FTE", f"{total_fte:.6f}")

    option = {
        **base,
        "tooltip": {
            **base.get("tooltip", {}),
            "formatter": JsCode(tooltip_fn) if JsCode else tooltip_fn,
        },
        "series": [
            {
                "type": "graph",
                "layout": "circular",
                "circular": {"rotateLabel": True},
                "data": nodes,
                "links": links_data,
                "categories": [{"name": "Teams"}, {"name": "Apps"}],
                "roam": True,
                "focusNodeAdjacency": True,
                "label": {"show": True, "formatter": JsCode("function(p){return p.data.displayLabel||'';}") if JsCode else "{@displayLabel}"},
                "labelLayout": {"hideOverlap": True},
                "lineStyle": {"curveness": 0.15, "opacity": 0.28},
                "emphasis": {"focus": "adjacency", "lineStyle": {"opacity": 0.85}},
            }
        ],
    }
    debug.update(
        {
            "total_fte": total_fte,
            "teams": len(teams),
            "apps": len(apps),
            "links": len(links_data),
            "missing_links": missing[:50],
            "links_df": links_df.sort_values("FTE", ascending=False),
            "nodes_preview": pd.DataFrame(nodes),
        }
    )
    if missing:
        debug["reason"] = "missing_link_nodes"
    return option, debug


def _pick_group_col(df: pd.DataFrame) -> Optional[str]:
    for col in ["GROUPNAME", "APP_GROUP", "APPGROUP", "APP_NAME", "APPNAME", "APPLICATION", "APPLICATION_NAME"]:
        if col in df.columns:
            return col
    return None


def _pick_period_label(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=str)
    if "YEAR" in df.columns and "PI_NAME" in df.columns:
        year = pd.to_numeric(df.get("YEAR"), errors="coerce").fillna(0).astype(int)
        pi_name = df.get("PI_NAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        out = year.astype(str) + " " + pi_name
        return out.where(pi_name.ne(""), pd.NA)
    if "PI_NAME" in df.columns:
        return df["PI_NAME"].fillna("").astype(str).str.strip()
    if "PI" in df.columns and "YEAR" in df.columns:
        year = pd.to_numeric(df.get("YEAR"), errors="coerce").fillna(0).astype(int)
        pi = pd.to_numeric(df.get("PI"), errors="coerce").fillna(0).astype(int)
        out = year.astype(str) + " I" + pi.astype(str)
        return out.where(pi.gt(0), pd.NA)
    for col in ["PERIOD", "MONTH_KEY", "MONTH"]:
        if col in df.columns:
            return df[col].fillna("").astype(str).str.strip()
    return pd.Series(dtype=str)


def _get_sod_demand_tidy_df(df_expected: pd.DataFrame) -> pd.DataFrame:
    empty_cols = ["TEAMNAME", "GROUPNAME", "PROGRAMNAME", "PERIOD", "DEMAND_FTE"]
    if df_expected is None or df_expected.empty:
        return pd.DataFrame(columns=empty_cols)
    df = df_expected.copy()
    if "SCENARIO" in df.columns:
        scen = df["SCENARIO"].fillna("").astype(str).str.upper()
        df = df.loc[scen.eq("EXPECTED")].copy()
    if df.empty:
        return pd.DataFrame(columns=empty_cols)
    wf_mask, _ = _wf_nwf_mask(df)
    df = df.loc[wf_mask].copy()
    if df.empty:
        return pd.DataFrame(columns=empty_cols)
    group_col = _pick_group_col(df)
    if not group_col:
        return pd.DataFrame(columns=empty_cols)
    group_val = df[group_col].fillna("").astype(str).str.strip()
    df = df.loc[group_val.ne("")].copy()
    df = df.loc[~group_val.str.upper().isin({"UNASSIGNED", "(UNASSIGNED)"})].copy()
    if "MAPPING_STATUS" in df.columns:
        mapped = df["MAPPING_STATUS"].fillna("").astype(str).str.upper()
        df = df.loc[mapped.eq("MAPPED")].copy()
    if df.empty:
        return pd.DataFrame(columns=empty_cols)
    sod_col = None
    for col in ["SOD_LAYER2", "WF_LAYER2", "WORKFORCE_LAYER2"]:
        if col in df.columns:
            sod_col = col
            break
    if sod_col:
        sod_val = df[sod_col].fillna("").astype(str).str.upper()
        df = df.loc[sod_val.eq("SOD")].copy()
    if df.empty:
        return pd.DataFrame(columns=empty_cols)
    overhead_mask = _is_program_overhead(df) | _is_team_overhead(df)
    df = df.loc[~overhead_mask].copy()
    if df.empty:
        return pd.DataFrame(columns=empty_cols)
    fte_col = None
    for col in ["DERIVED_FTE", "FTE"]:
        if col in df.columns:
            fte_col = col
            break
    if not fte_col and "DERIVED_FTE_SWAG" in df.columns:
        fte_col = "DERIVED_FTE_SWAG"
    if fte_col:
        df["DEMAND_FTE"] = pd.to_numeric(df.get(fte_col), errors="coerce").fillna(0.0)
    else:
        unit = df.get("UNIT", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
        metric = df.get("METRIC", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
        is_fte = unit.eq("FTE") | metric.eq("FTE")
        df = df.loc[is_fte].copy()
        df["DEMAND_FTE"] = pd.to_numeric(df.get("AMOUNT"), errors="coerce").fillna(0.0)
    df = df.loc[df["DEMAND_FTE"] > 0].copy()
    if df.empty:
        return pd.DataFrame(columns=empty_cols)
    df["TEAMNAME"] = df.get("TEAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    df["PROGRAMNAME"] = df.get("PROGRAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    df["GROUPNAME"] = df[group_col].fillna("").astype(str).str.strip()
    df["PERIOD"] = _pick_period_label(df)
    df = df.loc[df["TEAMNAME"].ne("") & df["GROUPNAME"].ne("")].copy()
    return df[empty_cols]


def _get_sod_staffing_mix_df(df_expected: pd.DataFrame) -> pd.DataFrame:
    if df_expected is None or df_expected.empty:
        return pd.DataFrame()
    df = df_expected.copy()
    wf_mask, _ = _wf_nwf_mask(df)
    df = df.loc[wf_mask].copy()
    if df.empty:
        return pd.DataFrame()
    sod_col = None
    for col in ["SOD_LAYER2", "WF_LAYER2", "WORKFORCE_LAYER2"]:
        if col in df.columns:
            sod_col = col
            break
    if sod_col:
        sod_val = df[sod_col].fillna("").astype(str).str.upper()
        df = df.loc[sod_val.eq("SOD")].copy()
    if df.empty:
        return pd.DataFrame()
    df = df.loc[~_is_program_overhead(df)].copy()
    if df.empty:
        return pd.DataFrame()
    fte_col = None
    for col in ["DERIVED_FTE", "FTE"]:
        if col in df.columns:
            fte_col = col
            break
    if not fte_col and "DERIVED_FTE_SWAG" in df.columns:
        fte_col = "DERIVED_FTE_SWAG"
    if fte_col:
        df["VALUE"] = pd.to_numeric(df.get(fte_col), errors="coerce").fillna(0.0)
        metric = "FTE"
    else:
        df["VALUE"] = pd.to_numeric(df.get("AMOUNT"), errors="coerce").fillna(0.0)
        metric = "COST"
    df = df.loc[df["VALUE"] > 0].copy()
    if df.empty:
        return pd.DataFrame()
    df["BUCKET"] = "Delivery"
    df.loc[_is_team_overhead(df), "BUCKET"] = "Team Overhead"
    df.loc[_is_sod_contractor_c(df), "BUCKET"] = "Contractor C"
    df.loc[~(_is_team_overhead(df) | _is_sod_contractor_c(df) | _is_sod_delivery(df)), "BUCKET"] = "Delivery"
    df["PROGRAMNAME"] = df.get("PROGRAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    df["TEAMNAME"] = df.get("TEAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    df["METRIC"] = metric
    return df[["PROGRAMNAME", "TEAMNAME", "BUCKET", "VALUE", "METRIC"]]


def _apptio_nwf_evolution_series(
    fiscal_year: int,
    sel_programs: list[str],
    sel_teams: list[str],
    base_df: pd.DataFrame,
) -> tuple[list[str], dict[str, list[float]], dict]:
    buckets = ["Invoices", "Cloud Azure", "Cloud AWS", "MSP", "Contractor CS", "Travel"]
    debug_meta: dict[str, Any] = {
        "status": "no_data",
        "programs_used": [],
        "program_ids_used": [],
        "rows": 0,
    }

    program_names: list[str] = []
    if sel_programs:
        program_names = [str(p).strip() for p in sel_programs if str(p).strip()]
    elif sel_teams:
        program_names = _infer_programs_for_selected_teams(base_df, sel_teams)
    else:
        if base_df is not None and isinstance(base_df, pd.DataFrame) and not base_df.empty:
            if "PROGRAMNAME" in base_df.columns:
                df_year = base_df
                if "YEAR" in base_df.columns:
                    df_year = base_df.loc[pd.to_numeric(base_df["YEAR"], errors="coerce").fillna(0).astype(int).eq(int(fiscal_year))]
                program_names = sorted({p for p in df_year["PROGRAMNAME"].fillna("").astype(str).str.strip().tolist() if p})

    program_ids = _program_ids_for_names(program_names) if program_names else []
    debug_meta["programs_used"] = program_names
    debug_meta["program_ids_used"] = program_ids
    if not program_ids:
        return [], {name: [] for name in buckets}, debug_meta

    df = fetch_apptio_actuals_monthly_breakdown(int(fiscal_year), program_ids, data_version=data_version)
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return [], {name: [] for name in buckets}, debug_meta

    debug_meta["rows"] = int(df.shape[0])
    type_col = None
    for col in ["EFFECTIVE_NWF_TYPE", "COST_TYPE", "SUBTYPE", "COST_SUBTYPE"]:
        if col in df.columns:
            type_col = col
            break
    if type_col is None or "MONTH_KEY" not in df.columns or "AMOUNT" not in df.columns:
        return [], {name: [] for name in buckets}, debug_meta

    types = df[type_col].fillna("").astype(str).str.strip().str.lower()
    bucket = pd.Series("", index=df.index, dtype=str)
    bucket = bucket.mask(types.eq("invoices") | types.str.contains("invoice", na=False), "Invoices")
    bucket = bucket.mask(bucket.eq("") & types.str.contains("cloud azure|azure", na=False), "Cloud Azure")
    bucket = bucket.mask(bucket.eq("") & types.str.contains("cloud aws|aws", na=False), "Cloud AWS")
    bucket = bucket.mask(bucket.eq("") & types.str.contains("msp", na=False), "MSP")
    bucket = bucket.mask(
        bucket.eq("")
        & (types.str.contains("contractor cs", na=False) | (types.str.contains("contractor", na=False) & types.str.contains("cs", na=False))),
        "Contractor CS",
    )
    bucket = bucket.mask(bucket.eq("") & types.str.contains("travel", na=False), "Travel")

    df_series = pd.DataFrame(
        {
            "MONTH_KEY": df["MONTH_KEY"].fillna("").astype(str).str.strip(),
            "BUCKET": bucket,
            "AMOUNT": pd.to_numeric(df["AMOUNT"], errors="coerce").fillna(0.0),
        }
    )
    df_series = df_series.loc[df_series["BUCKET"].ne("")]
    if df_series.empty:
        return [], {name: [] for name in buckets}, debug_meta

    debug_meta["status"] = "ok"
    grouped = df_series.groupby(["MONTH_KEY", "BUCKET"], as_index=False)["AMOUNT"].sum()
    grouped = grouped.rename(columns={"AMOUNT": "AMOUNT_RAW"})
    grouped["AMOUNT_POS"] = grouped["AMOUNT_RAW"].clip(lower=0)
    neg_rows = grouped.loc[grouped["AMOUNT_RAW"] < 0].copy()
    neg_total = float(neg_rows["AMOUNT_RAW"].abs().sum() or 0.0)

    pivot = grouped.pivot(index="MONTH_KEY", columns="BUCKET", values="AMOUNT_POS").reindex(columns=buckets).fillna(0.0)
    month_labels = sorted(df_series["MONTH_KEY"].unique().tolist())
    pivot = pivot.reindex(month_labels, fill_value=0.0)
    series_map = {bucket: pivot[bucket].astype(float).tolist() for bucket in buckets}
    debug_meta["neg_total"] = neg_total
    debug_meta["neg_rows"] = neg_rows
    return month_labels, series_map, debug_meta


def _infer_programs_for_selected_teams(base_df: pd.DataFrame, sel_teams: list[str]) -> list[str]:
    if (
        base_df is None
        or not isinstance(base_df, pd.DataFrame)
        or base_df.empty
        or not sel_teams
        or "TEAMNAME" not in base_df.columns
        or "PROGRAMNAME" not in base_df.columns
    ):
        return []
    teams = [str(t).strip() for t in sel_teams if str(t).strip()]
    if not teams:
        return []
    programs = (
        base_df.loc[base_df["TEAMNAME"].fillna("").astype(str).isin(teams), "PROGRAMNAME"]
        .fillna("")
        .astype(str)
        .str.strip()
    )
    return sorted({p for p in programs.tolist() if p})


def _placeholder_line(theme: dict, title: str) -> dict:
    base = _option_base(theme)
    base["tooltip"]["trigger"] = "axis"
    return {
        **base,
        "title": {"text": title, "left": "left", "textStyle": {"color": base["textStyle"]["color"]}},
        "grid": {"left": 40, "right": 20, "top": 60, "bottom": 30},
        "xAxis": {"type": "category", "data": ["PI-1", "PI-2", "PI-3", "PI-4"]},
        "yAxis": {"type": "value"},
        "series": [
            {"type": "line", "data": [72, 78, 75, 82], "itemStyle": {"color": "#5B7BE3"}},
            {"type": "line", "data": [68, 70, 69, 73], "itemStyle": {"color": "#F4C358"}},
        ],
    }


def _pi_label(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(dtype=str)
    if "PI_NAME" in df.columns and df["PI_NAME"].fillna("").astype(str).str.strip().ne("").any():
        return df["PI_NAME"].fillna("").astype(str).str.strip()
    if "PI" in df.columns:
        pi = pd.to_numeric(df.get("PI"), errors="coerce").fillna(0).astype(int)
        return pi.astype(str)
    if "MONTH_KEY" in df.columns:
        return df["MONTH_KEY"].fillna("").astype(str).str.strip()
    if "MONTH" in df.columns:
        return df["MONTH"].fillna("").astype(str).str.strip()
    return pd.Series(dtype=str)


def _pi_label_year_prefix(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(dtype=str)
    if "YEAR" not in df.columns:
        return pd.Series(dtype=str)
    year = pd.to_numeric(df.get("YEAR"), errors="coerce").fillna(0).astype(int)
    if "PI_NAME" in df.columns and df["PI_NAME"].notna().any():
        pi_name = df["PI_NAME"].fillna("").astype(str).str.strip()
        out = year.astype(str) + " " + pi_name
        return out.where(pi_name.ne(""), pd.NA)
    if "PI" in df.columns:
        pi = pd.to_numeric(df.get("PI"), errors="coerce").fillna(0).astype(int)
        out = year.astype(str) + " I" + pi.astype(str)
        return out.where(pi.gt(0), pd.NA)
    return pd.Series(dtype=str)


def _norm_text(val: Any) -> str:
    return str(val or "").strip().lower()


def _sod_text(df: pd.DataFrame) -> pd.Series:
    return _series_text(
        df,
        [
            "SOD_BUCKET",
            "WF_LAYER2",
            "COST_BUCKET",
            "LABOR_TYPE",
            "SUBCOMPONENT_ROLLUP",
            "SUBCOMPONENT",
            "COST_TYPE",
            "EFFECTIVE_NWF_TYPE",
        ],
    )


def _is_program_overhead(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([]))
    text = _sod_text(df)
    return text.str.contains("program") & text.str.contains("overhead")


def _is_team_overhead(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([]))
    text = _sod_text(df)
    return text.str.contains("team") & text.str.contains("overhead")


def _is_sod_delivery(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([]))
    text = _sod_text(df)
    return text.str.contains("delivery")


def _is_sod_contractor_c(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([]))
    text = _sod_text(df)
    return text.str.contains("contractor c") | (text.str.contains("contractor") & ~text.str.contains("cs|msp"))


def _is_ado_demand(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([]))
    text = _series_text(df, ["SOURCE", "SUBCOMPONENT", "ALLOCATION_DRIVER", "COST_BUCKET"])
    return text.str.contains("ado")


def _is_sod_contractor_cs(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([]))
    text = _sod_text(df)
    return text.str.contains("contractor cs") | (text.str.contains("contractor") & text.str.contains("cs"))


def _is_capacity_component(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=pd.Index([]))
    prog_oh = _is_program_overhead(df)
    team_oh = _is_team_overhead(df)
    base = _is_sod_delivery(df) | _is_sod_contractor_c(df) | _is_sod_contractor_cs(df)
    comp_text = _series_text(
        df,
        [
            "WORKFORCE_TYPE",
            "WF_SUBTYPE",
            "SOD_SUBTYPE",
            "LABOR_TYPE",
            "SUBCOMPONENT",
            "SUBCOMPONENT_ROLLUP",
            "SOD_BUCKET",
            "WF_LAYER2",
            "COST_BUCKET",
            "COST_TYPE",
            "EFFECTIVE_NWF_TYPE",
        ],
    )
    non_capacity = comp_text.str.contains("team") | comp_text.str.contains("overhead")
    return base & ~prog_oh & ~team_oh & ~non_capacity


def _pick_fte_series(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series([], dtype=float)
    for col in ["FTE", "DERIVED_FTE", "DERIVED_FTE_SWAG"]:
        if col in df.columns and df[col].notna().any():
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return pd.Series([0.0] * len(df.index), index=df.index, dtype=float)


def _looks_like_timeout_error(exc: Exception) -> bool:
    msg = str(exc or "").strip().lower()
    return ("timeout" in msg) or ("timed out" in msg) or ("hyt00" in msg)


@cache_data_portfolio(ttl=180, show_spinner=False)
def _build_staffing_capacity_df(
    year: int,
    programs: tuple[str, ...],
    teams: tuple[str, ...],
    *,
    filters_sig: str = "",
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> pd.DataFrame:
    _ = (filters_sig, user_scope_sig, cache_buster)
    prog_expr = "COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME)"
    team_expr = "COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME)"
    where = ["h.YEAR = %s", "UPPER(h.CLASS) IN ('TEAM','DELIVERY')"]
    params: list[Any] = [int(year)]
    if programs:
        where.append(f"UPPER({prog_expr}) IN ({', '.join(['%s'] * len(programs))})")
        params.extend([str(p).strip().upper() for p in programs])
    if teams:
        where.append(f"UPPER({team_expr}) IN ({', '.join(['%s'] * len(teams))})")
        params.extend([str(t).strip().upper() for t in teams])
    where_sql = " AND ".join(where)
    sql = f"""
      SELECT
        h.YEAR,
        h.PI,
        {prog_expr} AS PROGRAMNAME,
        {team_expr} AS TEAMNAME,
        UPPER(h.CLASS) AS COMPONENT,
        CAST(COALESCE(h.HEADCOUNT, 0) AS FLOAT) AS FTE
      FROM VW_TEAM_HEADCOUNT_EFFECTIVE h
      LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
      LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
      WHERE {where_sql}
    """
    df1 = fetch_df(sql, tuple(params))
    where2 = ["h.YEAR = %s", "UPPER(h.CLASS) IN ('CONTRACTOR_C','CONTRACTOR_CS')"]
    params2: list[Any] = [int(year)]
    if programs:
        where2.append(f"UPPER({prog_expr}) IN ({', '.join(['%s'] * len(programs))})")
        params2.extend([str(p).strip().upper() for p in programs])
    if teams:
        where2.append(f"UPPER({team_expr}) IN ({', '.join(['%s'] * len(teams))})")
        params2.extend([str(t).strip().upper() for t in teams])
    where2_sql = " AND ".join(where2)
    sql2 = f"""
      SELECT
        h.YEAR,
        h.PI,
        {prog_expr} AS PROGRAMNAME,
        {team_expr} AS TEAMNAME,
        UPPER(h.CLASS) AS COMPONENT,
        CAST(COALESCE(h.HEADCOUNT, 0) AS FLOAT) AS FTE
      FROM VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE h
      LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
      LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
      WHERE {where2_sql}
    """
    df2 = fetch_df(sql2, tuple(params2))
    frames = [df for df in [df1, df2] if isinstance(df, pd.DataFrame) and not df.empty]
    if not frames:
        return pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "COMPONENT", "FTE"])
    # fetch_df already applies display-name mapping; avoid repeating it on large merged frames.
    out = pd.concat(frames, ignore_index=True, sort=False)
    out["YEAR"] = pd.to_numeric(out.get("YEAR"), errors="coerce").astype("Int64")
    out["PI"] = pd.to_numeric(out.get("PI"), errors="coerce").astype("Int64")
    out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    out["TEAMNAME"] = out.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    out["COMPONENT"] = out.get("COMPONENT", "").fillna("").astype(str).str.strip()
    out["FTE"] = pd.to_numeric(out.get("FTE"), errors="coerce").fillna(0.0).astype(float)
    return out


@cache_data_portfolio(ttl=180, show_spinner=False)
def _build_demand_fte_df(
    year: int,
    programs: tuple[str, ...],
    teams: tuple[str, ...],
    *,
    filters_sig: str = "",
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> pd.DataFrame:
    _ = (filters_sig, user_scope_sig, cache_buster)
    prog_expr = "COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME, d.PROGRAMNAME)"
    team_expr = "COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME, d.TEAMNAME)"
    where = ["d.YEAR = %s"]
    params: list[Any] = [int(year)]
    if programs:
        where.append(f"UPPER({prog_expr}) IN ({', '.join(['%s'] * len(programs))})")
        params.extend([str(p).strip().upper() for p in programs])
    if teams:
        where.append(f"UPPER({team_expr}) IN ({', '.join(['%s'] * len(teams))})")
        params.extend([str(t).strip().upper() for t in teams])
    where_sql = " AND ".join(where)
    sql = f"""
      SELECT
        d.YEAR,
        d.PI,
        {prog_expr} AS PROGRAMNAME,
        {team_expr} AS TEAMNAME,
        CAST(SUM(COALESCE(d.DERIVED_FTE_FEATURE, 0)) AS FLOAT) AS DEMAND_FTE
      FROM VW_TCO_FEATURE_DEMAND d
      LEFT JOIN PROGRAMS p ON p.PROGRAMID = d.PROGRAMID
      LEFT JOIN TEAMS t ON t.TEAMID = d.TEAMID
      WHERE {where_sql}
      GROUP BY d.YEAR, d.PI, {prog_expr}, {team_expr}
    """
    df = fetch_df(sql, tuple(params))
    if df is None or df.empty:
        return pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "DEMAND_FTE"])
    df["YEAR"] = pd.to_numeric(df.get("YEAR"), errors="coerce").astype("Int64")
    df["PI"] = pd.to_numeric(df.get("PI"), errors="coerce").astype("Int64")
    df["PROGRAMNAME"] = df.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    df["TEAMNAME"] = df.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    df["DEMAND_FTE"] = pd.to_numeric(df.get("DEMAND_FTE"), errors="coerce").fillna(0.0).astype(float)
    # fetch_df already applies display-name mapping.
    return df


def _build_staffing_series(df_baseline: pd.DataFrame) -> tuple[list[str], pd.Series, pd.Series, dict]:
    debug = {
        "rows": 0,
        "capacity_rows": 0,
        "capacity_total": 0.0,
        "capacity_fte_total": 0.0,
        "team_overhead_rows": 0,
    }
    if df_baseline is None or not isinstance(df_baseline, pd.DataFrame) or df_baseline.empty:
        return [], pd.Series(dtype=float), pd.Series(dtype=float), debug
    df = df_baseline.copy()
    df["PI_LABEL"] = _pi_label(df)
    df = df.loc[df["PI_LABEL"].fillna("").astype(str).str.strip().ne("")].copy()
    if df.empty:
        return [], pd.Series(dtype=float), pd.Series(dtype=float), debug
    df["AMOUNT"] = pd.to_numeric(df.get("AMOUNT"), errors="coerce").fillna(0.0)
    df["FTE_USED"] = _pick_fte_series(df)
    debug["rows"] = int(df.shape[0])
    team_oh = _is_team_overhead(df)
    debug["team_overhead_rows"] = int(team_oh.sum())
    prog_oh = _is_program_overhead(df)
    ado = _is_ado_demand(df)
    capacity_mask = _is_capacity_component(df) & (~ado)
    debug["capacity_rows"] = int(capacity_mask.sum())
    debug["capacity_total"] = float(df.loc[capacity_mask, "AMOUNT"].sum() or 0.0)
    debug["capacity_fte_total"] = float(df.loc[capacity_mask, "FTE_USED"].sum() or 0.0)
    grouped = df.groupby("PI_LABEL", as_index=False).agg(
        capacity_cost=("AMOUNT", lambda s: float(s.loc[capacity_mask.loc[s.index]].sum() if not s.empty else 0.0)),
        capacity_fte=("FTE_USED", lambda s: float(s.loc[capacity_mask.loc[s.index]].sum() if not s.empty else 0.0)),
    )
    labels = sorted(df["PI_LABEL"].unique().tolist())
    grouped = grouped.set_index("PI_LABEL").reindex(labels, fill_value=0.0)
    return labels, grouped["capacity_cost"].astype(float), grouped["capacity_fte"].astype(float), debug


def _build_demand_series(df_expected: pd.DataFrame) -> tuple[list[str], pd.Series, pd.Series, dict]:
    debug = {
        "rows": 0,
        "demand_rows": 0,
        "ado_rows": 0,
        "demand_total": 0.0,
        "demand_fte_total": 0.0,
        "ado_total": 0.0,
        "contractor_cs_rows": 0,
        "contractor_cs_total": 0.0,
        "contractor_cs_in_demand_rows": 0,
        "contractor_cs_in_demand_total": 0.0,
    }
    if df_expected is None or not isinstance(df_expected, pd.DataFrame) or df_expected.empty:
        return [], pd.Series(dtype=float), pd.Series(dtype=float), debug
    df = df_expected.copy()
    df["PI_LABEL"] = _pi_label(df)
    df = df.loc[df["PI_LABEL"].fillna("").astype(str).str.strip().ne("")].copy()
    if df.empty:
        return [], pd.Series(dtype=float), pd.Series(dtype=float), debug
    df["AMOUNT"] = pd.to_numeric(df.get("AMOUNT"), errors="coerce").fillna(0.0)
    df["FTE_USED"] = _pick_fte_series(df)
    debug["rows"] = int(df.shape[0])
    prog_oh = _is_program_overhead(df)
    team_oh = _is_team_overhead(df)
    ado_mask = _is_ado_demand(df) & ~prog_oh & ~team_oh
    cs_mask = _is_sod_contractor_cs(df)
    sod_mask = (_is_sod_delivery(df) | _is_sod_contractor_c(df) | _is_sod_contractor_cs(df)) & ~prog_oh & ~team_oh
    demand_cost_mask = ado_mask
    demand_fte_mask = ado_mask
    debug["ado_rows"] = int(ado_mask.sum())
    debug["demand_rows"] = int(demand_cost_mask.sum())
    debug["ado_total"] = float(df.loc[ado_mask, "AMOUNT"].sum() or 0.0)
    debug["demand_total"] = float(df.loc[demand_cost_mask, "AMOUNT"].sum() or 0.0)
    debug["demand_fte_total"] = float(df.loc[demand_fte_mask, "FTE_USED"].sum() or 0.0)
    debug["contractor_cs_rows"] = int(cs_mask.sum())
    debug["contractor_cs_total"] = float(df.loc[cs_mask, "AMOUNT"].sum() or 0.0)
    cs_in_demand = cs_mask & demand_cost_mask
    debug["contractor_cs_in_demand_rows"] = int(cs_in_demand.sum())
    debug["contractor_cs_in_demand_total"] = float(df.loc[cs_in_demand, "AMOUNT"].sum() or 0.0)
    grouped = df.groupby("PI_LABEL", as_index=False).agg(
        demand_cost=("AMOUNT", lambda s: float(s.loc[demand_cost_mask.loc[s.index]].sum() if not s.empty else 0.0)),
        demand_fte=("FTE_USED", lambda s: float(s.loc[demand_fte_mask.loc[s.index]].sum() if not s.empty else 0.0)),
    )
    labels = sorted(df["PI_LABEL"].unique().tolist())
    grouped = grouped.set_index("PI_LABEL").reindex(labels, fill_value=0.0)
    return labels, grouped["demand_cost"].astype(float), grouped["demand_fte"].astype(float), debug


def _merge_to_axis(
    staff_labels: list[str],
    staff_cost: pd.Series,
    staff_fte: pd.Series,
    demand_labels: list[str],
    demand_cost: pd.Series,
    demand_fte: pd.Series,
) -> tuple[list[str], list[float], list[float], list[float], list[float], dict]:
    x_labels = sorted(set(staff_labels) | set(demand_labels))
    if not x_labels:
        return [], [], [], [], [], {"neg_cost_total": 0.0, "neg_fte_total": 0.0}
    staff_cost = staff_cost.reindex(x_labels, fill_value=0.0)
    staff_fte = staff_fte.reindex(x_labels, fill_value=0.0)
    demand_cost = demand_cost.reindex(x_labels, fill_value=0.0)
    demand_fte = demand_fte.reindex(x_labels, fill_value=0.0)
    neg_cost = float((staff_cost[staff_cost < 0].abs().sum() + demand_cost[demand_cost < 0].abs().sum()) or 0.0)
    neg_fte = float((staff_fte[staff_fte < 0].abs().sum() + demand_fte[demand_fte < 0].abs().sum()) or 0.0)
    return (
        x_labels,
        staff_cost.clip(lower=0.0).tolist(),
        demand_cost.clip(lower=0.0).tolist(),
        staff_fte.clip(lower=0.0).tolist(),
        demand_fte.clip(lower=0.0).tolist(),
        {"neg_cost_total": neg_cost, "neg_fte_total": neg_fte},
    )


def _build_capacity_demand_budget_series(
    year: int,
    programs: tuple[str, ...],
    teams: tuple[str, ...],
    df_baseline_scope: pd.DataFrame,
    df_expected_scope: pd.DataFrame,
    *,
    filters_sig: str = "",
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> tuple[list[str], list[float], list[float], list[float], list[float], dict]:
    debug: dict[str, Any] = {
        "status": "no_data",
        "rows": 0,
        "capacity_col": None,
        "demand_col": None,
        "total_capacity_fte": 0.0,
        "total_demand_fte": 0.0,
        "baseline_sod_cost_total": 0.0,
        "cost_source_mode": "no_cost_found",
        "baseline_rows_used": 0,
        "baseline_program_fallback_rows_used": 0,
        "expected_rows_used": 0,
        "overall_rate": 0.0,
        "neg_cost_total": 0.0,
        "neg_fte_total": 0.0,
        "rate_by_pi_head": [],
    }

    def _fallback_from_cost_lines() -> tuple[list[str], list[float], list[float], list[float], list[float], dict]:
        staff_labels, staff_cost, staff_fte, staff_dbg = _build_staffing_series(
            df_baseline_scope if isinstance(df_baseline_scope, pd.DataFrame) else pd.DataFrame()
        )
        dem_labels, dem_cost, dem_fte, dem_dbg = _build_demand_series(
            df_expected_scope if isinstance(df_expected_scope, pd.DataFrame) else pd.DataFrame()
        )
        x2, cap_cost2, dem_cost2, cap_fte2, dem_fte2, merge_dbg = _merge_to_axis(
            staff_labels,
            staff_cost,
            staff_fte,
            dem_labels,
            dem_cost,
            dem_fte,
        )
        dbg = dict(debug)
        dbg["status"] = "fallback_cost_lines" if x2 else "no_data"
        dbg["capacity_source_mode"] = "cost_lines_fallback"
        dbg["rows"] = int(staff_dbg.get("rows", 0) or 0)
        dbg["total_capacity_fte"] = float(sum(cap_fte2) if cap_fte2 else 0.0)
        dbg["total_demand_fte"] = float(sum(dem_fte2) if dem_fte2 else 0.0)
        dbg["neg_cost_total"] = float(merge_dbg.get("neg_cost_total", 0.0) or 0.0)
        dbg["neg_fte_total"] = float(merge_dbg.get("neg_fte_total", 0.0) or 0.0)
        dbg["staff_debug"] = staff_dbg
        dbg["demand_debug"] = dem_dbg
        return x2, cap_fte2, dem_fte2, cap_cost2, dem_cost2, dbg
    staffing_df = pd.DataFrame()
    demand_df = pd.DataFrame()

    # Snapshot-first path: keeps Dashboard responsive on narrow Team/Program scopes.
    try:
        cap_snap = fetch_capacity_demand_pi(
            int(year),
            list(programs or ()),
            list(teams or ()),
            data_version=int(get_data_version() or 0),
        )
    except Exception:
        cap_snap = pd.DataFrame()

    if isinstance(cap_snap, pd.DataFrame) and not cap_snap.empty:
        snap = cap_snap.copy()
        snap["YEAR"] = pd.to_numeric(snap.get("YEAR"), errors="coerce").astype("Int64")
        snap["PI"] = pd.to_numeric(snap.get("PI"), errors="coerce").astype("Int64")
        snap["PROGRAMNAME"] = snap.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
        snap["TEAMNAME"] = snap.get("TEAMNAME", "").fillna("").astype(str).str.strip()
        snap["CAPACITY_FTE"] = pd.to_numeric(snap.get("CAPACITY_FTE"), errors="coerce").fillna(0.0)
        snap["DEMAND_FTE"] = pd.to_numeric(snap.get("DEMAND_FTE"), errors="coerce").fillna(0.0)

        staffing_df = snap[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CAPACITY_FTE"]].copy()
        staffing_df = staffing_df.rename(columns={"CAPACITY_FTE": "FTE"})
        staffing_df["COMPONENT"] = "DELIVERY"

        demand_df = snap[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "DEMAND_FTE"]].copy()
        debug["capacity_source_mode"] = "snapshot_team_pi"
    else:
        # Avoid heavy fallback SQL here (it causes frequent HYT00 on local SQL).
        # Use already-loaded canonical cost lines as the resilient fallback source.
        return _fallback_from_cost_lines()
    debug["rows"] = int(staffing_df.shape[0]) if isinstance(staffing_df, pd.DataFrame) else 0
    if staffing_df is None or staffing_df.empty:
        return _fallback_from_cost_lines()

    cap = staffing_df.copy()
    cap["PI_LABEL"] = (cap["YEAR"].astype("Int64").astype(str) + " I" + cap["PI"].astype("Int64").astype(str)).where(
        cap["YEAR"].notna() & cap["PI"].notna(), pd.NA
    )
    cap = cap.loc[cap["PI_LABEL"].fillna("").astype(str).str.strip().ne("")].copy()
    if cap.empty:
        return _fallback_from_cost_lines()

    comp_norm = cap.get("COMPONENT", "").fillna("").astype(str).str.upper()
    allowed = comp_norm.isin(["DELIVERY", "CONTRACTOR C", "CONTRACTOR_C", "CONTRACTOR CS", "CONTRACTOR_CS"])
    excluded = comp_norm.str.contains("TEAM") | comp_norm.str.contains("OVERHEAD")
    cap["CAPACITY_FTE_CALC"] = cap["FTE"].where(allowed & ~excluded, 0.0)
    debug["capacity_col"] = "CAPACITY_FTE_CALC"
    debug["demand_col"] = "DEMAND_FTE"

    cap["PI_ORDER"] = (pd.to_numeric(cap.get("YEAR"), errors="coerce").fillna(0).astype(int) * 10) + pd.to_numeric(
        cap.get("PI"), errors="coerce"
    ).fillna(0).astype(int)
    order_df = cap[["PI_LABEL", "PI_ORDER"]].drop_duplicates().sort_values("PI_ORDER")
    x_labels = order_df["PI_LABEL"].tolist()

    cap_grouped = cap.groupby("PI_LABEL", as_index=False)["CAPACITY_FTE_CALC"].sum()
    cap_grouped = cap_grouped.set_index("PI_LABEL").reindex(x_labels, fill_value=0.0)
    cap_fte = cap_grouped["CAPACITY_FTE_CALC"].astype(float)
    demand_df["PI_LABEL"] = (
        demand_df["YEAR"].astype("Int64").astype(str) + " I" + demand_df["PI"].astype("Int64").astype(str)
    ).where(demand_df["YEAR"].notna() & demand_df["PI"].notna(), pd.NA)
    dem_grouped = demand_df.groupby("PI_LABEL", as_index=False)["DEMAND_FTE"].sum()
    dem_grouped = dem_grouped.set_index("PI_LABEL").reindex(x_labels, fill_value=0.0)
    dem_fte = dem_grouped["DEMAND_FTE"].astype(float)
    debug["total_capacity_fte"] = float(cap_fte.sum() or 0.0)
    debug["total_demand_fte"] = float(dem_fte.sum() or 0.0)
    debug["neg_fte_total"] = float((cap_fte[cap_fte < 0].abs().sum() + dem_fte[dem_fte < 0].abs().sum()) or 0.0)

    def _sod_cost_by_pi(df_cost: pd.DataFrame, labels: list[str]) -> tuple[pd.Series, float, int, list[str]]:
        if df_cost is None or not isinstance(df_cost, pd.DataFrame) or df_cost.empty:
            return pd.Series([0.0] * len(labels), index=labels, dtype=float), 0.0, 0, []
        work = df_cost.copy()
        amt = pd.to_numeric(work.get("AMOUNT"), errors="coerce").fillna(0.0)
        work = work.loc[amt.ne(0)].copy()
        if work.empty:
            return pd.Series([0.0] * len(labels), index=labels, dtype=float), 0.0, 0, []

        work["PI_LABEL"] = _pi_label_year_prefix(work)
        work = work.loc[work["PI_LABEL"].fillna("").astype(str).str.strip().ne("")].copy()
        if work.empty:
            return pd.Series([0.0] * len(labels), index=labels, dtype=float), 0.0, 0, []

        layer = work.get("WF_LAYER2", "").fillna("").astype(str).str.strip().str.lower()
        sod_mask = layer.eq("sod")
        if "WF_LAYER2" in work.columns:
            sod_mask = sod_mask & ~layer.eq("program")

        if "WF_SUBTYPE" in work.columns:
            sub = work["WF_SUBTYPE"].fillna("").astype(str).str.lower()
            sod_mask = sod_mask & ~sub.str.contains("overhead", na=False)
        if "SOD_SUBTYPE" in work.columns:
            sub = work["SOD_SUBTYPE"].fillna("").astype(str).str.lower()
            sod_mask = sod_mask & ~sub.str.contains("overhead", na=False)

        if "COST_BUCKET" in work.columns:
            bucket = work["COST_BUCKET"].fillna("").astype(str).str.lower()
            sod_mask = sod_mask & ~bucket.str.contains("overhead", na=False)

        sod_rows = int(sod_mask.sum())
        if sod_rows == 0:
            return pd.Series([0.0] * len(labels), index=labels, dtype=float), 0.0, 0, []
        sod_cost = work.loc[sod_mask].groupby("PI_LABEL", as_index=False)["AMOUNT"].sum()
        sod_cost = sod_cost.set_index("PI_LABEL").reindex(labels, fill_value=0.0)["AMOUNT"].astype(float)
        sample_labels = work["PI_LABEL"].dropna().astype(str).unique().tolist()[:10]
        return sod_cost, float(sod_cost.sum() or 0.0), sod_rows, sample_labels

    base_cost = pd.Series([0.0] * len(x_labels), index=x_labels, dtype=float)
    baseline = df_baseline_scope.copy() if isinstance(df_baseline_scope, pd.DataFrame) else pd.DataFrame()
    base_cost, base_total, base_rows, base_labels = _sod_cost_by_pi(baseline, x_labels)
    debug["baseline_sod_cost_total"] = float(base_total or 0.0)
    debug["baseline_rows_used"] = int(base_rows)
    debug["baseline_pi_labels_sample"] = base_labels
    if base_total > 0:
        debug["cost_source_mode"] = "baseline_team_scope"
    else:
        expected = df_expected_scope.copy() if isinstance(df_expected_scope, pd.DataFrame) else pd.DataFrame()
        base_cost, base_total, base_rows, base_labels = _sod_cost_by_pi(expected, x_labels)
        debug["baseline_sod_cost_total"] = float(base_total or 0.0)
        debug["expected_rows_used"] = int(base_rows)
        debug["expected_pi_labels_sample"] = base_labels
        if base_total > 0:
            debug["cost_source_mode"] = "expected_fallback"
        else:
            debug["cost_source_mode"] = "no_cost_found"

    overall_rate = 0.0
    if debug["total_capacity_fte"] > 0:
        overall_rate = debug["baseline_sod_cost_total"] / debug["total_capacity_fte"]
    elif debug["total_demand_fte"] > 0:
        overall_rate = debug["baseline_sod_cost_total"] / debug["total_demand_fte"]
    debug["overall_rate"] = float(overall_rate or 0.0)

    rate_by_pi = pd.Series(overall_rate, index=x_labels, dtype=float)
    nonzero_cap = cap_fte > 0
    rate_by_pi.loc[nonzero_cap] = (base_cost.loc[nonzero_cap] / cap_fte.loc[nonzero_cap]).replace([pd.NA, pd.NaT], overall_rate)

    cap_cost = (cap_fte * rate_by_pi).astype(float)
    dem_cost = (dem_fte * rate_by_pi).astype(float)
    neg_cost = float((cap_cost[cap_cost < 0].abs().sum() + dem_cost[dem_cost < 0].abs().sum()) or 0.0)
    debug["neg_cost_total"] = neg_cost
    debug["rate_by_pi_head"] = (
        rate_by_pi.head(6).reset_index().rename(columns={0: "rate"}).to_dict(orient="records")
        if isinstance(rate_by_pi, pd.Series)
        else []
    )

    return (
        x_labels,
        cap_fte.clip(lower=0.0).tolist(),
        dem_fte.clip(lower=0.0).tolist(),
        cap_cost.clip(lower=0.0).tolist(),
        dem_cost.clip(lower=0.0).tolist(),
        debug,
    )


def _shade_hex(color: str, factor: float) -> str:
    hex_color = str(color or "").lstrip("#")
    if len(hex_color) != 6:
        return color
    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    l = max(0.0, min(1.0, l * factor))
    nr, ng, nb = colorsys.hls_to_rgb(h, l, s)
    return f"#{int(nr*255):02x}{int(ng*255):02x}{int(nb*255):02x}"


def _build_cost_anatomy_nested_pie_option(
    inner_data: list[dict],
    outer_data: list[dict],
    *,
    total_amt: float,
    theme: dict,
) -> dict:
    text_col = theme.get("echarts_text", theme.get("text", "#f5f5f5"))
    tooltip_bg = theme.get("echarts_tooltip_bg", "#3B3B3B")
    tooltip_border = theme.get("echarts_tooltip_border", "#2a2a2a")

    tooltip_total = total_amt if total_amt > 0 else 1.0
    tooltip_fn = (
        "function(p){"
        f" var total={tooltip_total};"
        " var v=(typeof p.value==='number') ? p.value : (p.value||0);"
        " var parent=(p.data && p.data.parent) ? p.data.parent : '';"
        " var name=String(p.name||'');"
        " var path= parent ? (parent + ' > ' + name) : name;"
        " var pct= total>0 ? (v/total*100.0) : 0;"
        " return path"
        "  +'<br/>$'+Number(v||0).toLocaleString(undefined,{maximumFractionDigits:0})"
        "  +' ('+pct.toFixed(1)+'%)';"
        "}"
    )
    outer_label_fn = (
        "function(p){"
        f" var total={tooltip_total};"
        " var v=(typeof p.value==='number') ? p.value : (p.value||0);"
        " var pct= total>0 ? (v/total*100.0) : 0;"
        " if(pct < 2){ return ''; }"
        " return p.name;"
        "}"
    )
    def _grad_for(color: str, top_factor: float = 1.18, bottom_factor: float = 0.80) -> dict:
        base = str(color or "#5B7BE3")
        top = _shade_hex(base, top_factor)
        bottom = _shade_hex(base, bottom_factor)
        return {
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
        }

    def _styled_pie_items(items: list[dict]) -> list[dict]:
        out: list[dict] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            i = dict(item)
            style = dict(i.get("itemStyle") or {})
            base_color = str(style.get("color") or "#5B7BE3")
            style["color"] = _grad_for(base_color)
            style.setdefault("borderWidth", 1)
            style.setdefault("borderColor", "rgba(10,18,34,0.45)")
            style.setdefault("shadowBlur", 4)
            style.setdefault("shadowColor", "rgba(0,0,0,0.14)")
            i["itemStyle"] = style
            out.append(i)
        return out

    inner_styled = _styled_pie_items(inner_data)
    outer_styled = _styled_pie_items(outer_data)

    return {
        "tooltip": {
            "trigger": "item",
            "backgroundColor": tooltip_bg,
            "borderColor": tooltip_border,
            "textStyle": {"color": text_col},
            "formatter": JsCode(tooltip_fn) if JsCode else tooltip_fn,
        },
        "legend": {"show": False},
        "series": [
            {
                "type": "pie",
                "radius": ["0%", "43%"],
                "center": ["50%", "52%"],
                "padAngle": 0,
                "selectedMode": False,
                "label": {
                    "show": True,
                    "position": "inside",
                    "formatter": "{b}",
                    "fontSize": 13,
                    "fontWeight": 600,
                    "color": "#FFFFFF",
                    "textBorderColor": "rgba(0,0,0,0.6)",
                    "textBorderWidth": 2,
                    "textShadowBlur": 4,
                },
                "labelLayout": {"hideOverlap": True, "moveOverlap": "none"},
                "labelLine": {"show": False},
                "emphasis": {"scale": False, "focus": "self", "itemStyle": {"opacity": 0.92}},
                "itemStyle": {"borderWidth": 1, "borderColor": "rgba(10,18,34,0.45)", "borderRadius": 0},
                "data": inner_styled,
            },
            {
                "type": "pie",
                "radius": ["56%", "79%"],
                "center": ["50%", "52%"],
                "padAngle": 0,
                "selectedMode": False,
                "label": {
                    "show": True,
                    "position": "outside",
                    "formatter": JsCode(outer_label_fn) if JsCode else "{b}",
                    "color": "#FFFFFF",
                    "textBorderColor": "rgba(0,0,0,0.6)",
                    "textBorderWidth": 2,
                    "textShadowBlur": 4,
                },
                "labelLayout": {"hideOverlap": True},
                "labelLine": {"show": True, "length": 8, "length2": 6},
                "emphasis": {
                    "scale": False,
                    "focus": "self",
                    "itemStyle": {"opacity": 0.92},
                    "label": {"show": False},
                    "labelLine": {"show": False},
                },
                "itemStyle": {"borderWidth": 1, "borderColor": "rgba(10,18,34,0.45)", "borderRadius": 0},
                "data": outer_styled,
            },
        ],
    }


data_version = get_data_version()
theme = _merged_theme()

base_df = fetch_filter_options(data_version=data_version)
scope = read_scope_from_session(fetch_df)
role = getattr(scope, "inferred_role", None) or infer_role(scope)
view_label = scope_label(scope, role)

render_page_header(
    "Dashboard",
    "Executive one-page view",
    view_label,
    role,
    scope_status_right=touch_last_updated_status("dashboard"),
)

years = (
    sorted(pd.to_numeric(base_df.get("YEAR"), errors="coerce").dropna().astype(int).unique().tolist())
    if base_df is not None and not base_df.empty
    else []
)
current_year = int(pd.Timestamp.today().year)
default_year = current_year if current_year in years else (max(years) if years else current_year)

with st.expander("Filters", expanded=False):
    # TODO: connect filters consistently with other dashboards
    st.session_state.setdefault("dashboard_filter_programs", [])
    st.session_state.setdefault("dashboard_filter_teams", [])
    st.session_state.setdefault("dashboard_filter_groups", [])
    year_options = years or [default_year]
    default_year_idx = year_options.index(default_year) if default_year in year_options else 0
    selected_year = int(st.selectbox("Year", year_options, index=default_year_idx))
    programs = _drop_unassigned_if_mixed(sorted(_safe_list(base_df.get("PROGRAMNAME")) if base_df is not None else []))
    teams = _drop_unassigned_if_mixed(sorted(_safe_list(base_df.get("TEAMNAME")) if base_df is not None else []))
    groups = _drop_unassigned_if_mixed(sorted(_safe_list(base_df.get("GROUPNAME")) if base_df is not None else []))
    # For portfolio-wide/admin views, always merge master program list so registered programs
    # remain visible even when selected-year cost rows are sparse.
    show_portfolio_program_options = role in {"Portfolio Manager", "Unknown", "ADMIN"} or not bool(getattr(scope, "programs", []))
    if show_portfolio_program_options:
        try:
            p_df = list_programs()
            if isinstance(p_df, pd.DataFrame) and not p_df.empty and "PROGRAMNAME" in p_df.columns:
                p_master = {str(v).strip() for v in p_df["PROGRAMNAME"].dropna().astype(str).tolist() if str(v).strip()}
                programs = sorted(set(programs).union(p_master))
        except Exception:
            pass
    # Always include scoped options so contributors/viewers keep visibility
    # even when selected-year cost rows are sparse.
    scope_programs = {str(v).strip() for v in (getattr(scope, "programs", []) or []) if str(v).strip()}
    scope_teams = {str(v).strip() for v in (getattr(scope, "teams", []) or []) if str(v).strip()}
    scope_groups = {str(v).strip() for v in (getattr(scope, "groups", []) or []) if str(v).strip()}
    if scope_programs:
        programs = sorted(set(programs).union(scope_programs))
    if scope_teams:
        teams = sorted(set(teams).union(scope_teams))
    if scope_groups:
        groups = sorted(set(groups).union(scope_groups))
    if not teams:
        try:
            t_df = list_teams()
            if isinstance(t_df, pd.DataFrame) and not t_df.empty and "TEAMNAME" in t_df.columns:
                teams = sorted({str(v).strip() for v in t_df["TEAMNAME"].dropna().astype(str).tolist() if str(v).strip()})
        except Exception:
            pass
    if not groups:
        try:
            g_df = list_application_groups()
            if isinstance(g_df, pd.DataFrame) and not g_df.empty and "GROUPNAME" in g_df.columns:
                groups = sorted({str(v).strip() for v in g_df["GROUPNAME"].dropna().astype(str).tolist() if str(v).strip()})
        except Exception:
            pass
    program_label_map: dict[str, str] = {}
    team_label_map: dict[str, str] = {}
    try:
        p_map_df = list_programs()
        if isinstance(p_map_df, pd.DataFrame) and not p_map_df.empty and "PROGRAMNAME" in p_map_df.columns:
            for _, row in p_map_df.iterrows():
                disp = str(row.get("PROGRAMNAME") or "").strip()
                raw = str(row.get("PROGRAMNAME_RAW") or "").strip()
                if disp:
                    program_label_map.setdefault(disp, disp)
                if raw and disp:
                    program_label_map[raw] = disp
    except Exception:
        pass
    try:
        t_map_df = list_teams()
        if isinstance(t_map_df, pd.DataFrame) and not t_map_df.empty and "TEAMNAME" in t_map_df.columns:
            for _, row in t_map_df.iterrows():
                disp = str(row.get("TEAMNAME") or "").strip()
                raw = str(row.get("TEAMNAME_RAW") or "").strip()
                if disp:
                    team_label_map.setdefault(disp, disp)
                if raw and disp:
                    team_label_map[raw] = disp
    except Exception:
        pass

    def _dedupe_options_by_label(options: list[str], label_map: dict[str, str]) -> list[str]:
        chosen: dict[str, str] = {}
        order: list[str] = []
        for opt in options or []:
            raw_opt = str(opt).strip()
            if not raw_opt:
                continue
            disp = str(label_map.get(raw_opt, raw_opt)).strip() or raw_opt
            key = disp.upper()
            prev = chosen.get(key)
            if prev is None:
                chosen[key] = raw_opt
                order.append(key)
                continue
            if prev != disp and raw_opt == disp:
                chosen[key] = raw_opt
        return [chosen[k] for k in order]

    def _remap_selected_by_label(values: list[str], options: list[str], label_map: dict[str, str]) -> list[str]:
        if not values or not options:
            return []
        option_by_disp = {
            str(label_map.get(str(o).strip(), str(o).strip())).strip().upper(): str(o).strip()
            for o in options
            if str(o).strip()
        }
        out: list[str] = []
        seen: set[str] = set()
        for v in values:
            raw_v = str(v).strip()
            if not raw_v:
                continue
            disp_key = str(label_map.get(raw_v, raw_v)).strip().upper()
            opt = option_by_disp.get(disp_key)
            if opt and opt not in seen:
                seen.add(opt)
                out.append(opt)
        return out

    programs = _drop_unassigned_if_mixed(_dedupe_options_by_label(programs, program_label_map))
    teams = _drop_unassigned_if_mixed(_dedupe_options_by_label(teams, team_label_map))
    default_programs = _safe_list(getattr(scope, "programs", []))
    default_teams = _safe_list(getattr(scope, "teams", []))
    default_groups = _safe_list(getattr(scope, "groups", []))
    if not st.session_state["dashboard_filter_programs"]:
        st.session_state["dashboard_filter_programs"] = [p for p in default_programs if p in programs]
    if not st.session_state["dashboard_filter_teams"]:
        st.session_state["dashboard_filter_teams"] = [t for t in default_teams if t in teams]
    if not st.session_state["dashboard_filter_groups"]:
        st.session_state["dashboard_filter_groups"] = [g for g in default_groups if g in groups]
    st.session_state["dashboard_filter_programs"] = _remap_selected_by_label(
        list(st.session_state.get("dashboard_filter_programs", [])),
        programs,
        program_label_map,
    )
    st.session_state["dashboard_filter_teams"] = _remap_selected_by_label(
        list(st.session_state.get("dashboard_filter_teams", [])),
        teams,
        team_label_map,
    )
    st.session_state["dashboard_filter_groups"] = [
        g for g in st.session_state.get("dashboard_filter_groups", []) if g in groups
    ]

    rel_df = base_df.copy() if isinstance(base_df, pd.DataFrame) else pd.DataFrame()
    if not rel_df.empty and "YEAR" in rel_df.columns:
        rel_df["YEAR"] = pd.to_numeric(rel_df.get("YEAR"), errors="coerce").astype("Int64")
        rel_df = rel_df.loc[rel_df["YEAR"].eq(int(selected_year))].copy()
    if rel_df.empty and isinstance(base_df, pd.DataFrame):
        rel_df = base_df.copy()

    sel_programs = st.multiselect(
        "Programs",
        programs,
        key="dashboard_filter_programs",
        format_func=lambda x, _m=program_label_map: _m.get(str(x).strip(), str(x).strip()),
    )

    team_options: list[str] = []
    team_disabled = not bool(sel_programs)
    if not team_disabled:
        if not rel_df.empty and "TEAMNAME" in rel_df.columns:
            team_df = rel_df.copy()
            if "PROGRAMNAME" in team_df.columns:
                team_df = team_df.loc[team_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
            team_options = sorted({str(v).strip() for v in team_df.get("TEAMNAME", pd.Series(dtype=str)).dropna().astype(str).tolist() if str(v).strip()})
        if not team_options:
            try:
                t_df = list_teams()
                if isinstance(t_df, pd.DataFrame) and not t_df.empty and "TEAMNAME" in t_df.columns:
                    if sel_programs and "PROGRAMNAME" in t_df.columns:
                        t_df = t_df.loc[t_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
                    team_options = sorted({str(v).strip() for v in t_df["TEAMNAME"].dropna().astype(str).tolist() if str(v).strip()})
            except Exception:
                team_options = []
    team_options = _drop_unassigned_if_mixed(_dedupe_options_by_label(team_options, team_label_map))
    st.session_state["dashboard_filter_teams"] = _remap_selected_by_label(
        list(st.session_state.get("dashboard_filter_teams", [])),
        team_options,
        team_label_map,
    )
    sel_teams = st.multiselect(
        "Teams",
        team_options,
        key="dashboard_filter_teams",
        disabled=team_disabled,
        help="Select Program(s) first." if team_disabled else None,
        format_func=lambda x, _m=team_label_map: _m.get(str(x).strip(), str(x).strip()),
    )

    group_options: list[str] = []
    group_disabled = not bool(sel_teams)
    if not group_disabled:
        if not rel_df.empty and "GROUPNAME" in rel_df.columns:
            group_df = rel_df.copy()
            if "PROGRAMNAME" in group_df.columns and sel_programs:
                group_df = group_df.loc[group_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
            if "TEAMNAME" in group_df.columns:
                group_df = group_df.loc[group_df["TEAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_teams))].copy()
            group_options = sorted({str(v).strip() for v in group_df.get("GROUPNAME", pd.Series(dtype=str)).dropna().astype(str).tolist() if str(v).strip()})
        if not group_options:
            try:
                g_df = list_application_groups()
                if isinstance(g_df, pd.DataFrame) and not g_df.empty and "GROUPNAME" in g_df.columns:
                    if sel_teams and "TEAMNAME" in g_df.columns:
                        g_df = g_df.loc[g_df["TEAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_teams))].copy()
                    group_options = sorted({str(v).strip() for v in g_df["GROUPNAME"].dropna().astype(str).tolist() if str(v).strip()})
            except Exception:
                group_options = []
    group_options = _drop_unassigned_if_mixed(group_options)
    st.session_state["dashboard_filter_groups"] = [g for g in st.session_state.get("dashboard_filter_groups", []) if g in set(group_options)]
    sel_groups = st.multiselect(
        "Applications",
        group_options,
        key="dashboard_filter_groups",
        disabled=group_disabled,
        help="Select Team(s) first." if group_disabled else None,
    )

render_active_filters_summary(
    "Active filters",
    [
        ("Year", selected_year),
        ("Programs", sel_programs),
        ("Teams", sel_teams),
        ("Applications", sel_groups),
    ],
)

page_perf_key = "dashboard"
dashboard_filters_payload = {
    "year": int(selected_year),
    "programs": list(sel_programs or []),
    "teams": list(sel_teams or []),
    "groups": list(sel_groups or []),
}
dashboard_filters_sig = filters_signature(dashboard_filters_payload)
dashboard_user_scope_sig = user_scope_signature(scope, role)
dashboard_cache_buster = get_portfolio_cache_buster()

global_loading_ph = st.empty()


def _set_loading(message: str, step: int, total_steps: int = 5) -> None:
    with global_loading_ph.container():
        step_idx = max(1, min(int(step), int(total_steps)))
        st.markdown(
            f"<div style='font-size:0.8rem; color:color-mix(in srgb, var(--tco-text) 70%, transparent); margin-bottom:0.2rem;'>Step {step_idx} of {int(total_steps)}</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<div style='font-size:0.9rem; color:var(--tco-text); margin-bottom:0.32rem;'>{message}</div>",
            unsafe_allow_html=True,
        )
        st.progress(float(step_idx) / float(total_steps))


_set_loading("Loading filters and cost model...", 1)

if base_df is None or base_df.empty:
    st.info("No data available. Showing placeholders.")

scope_dict = {"programs": sel_programs, "teams": sel_teams, "groups": sel_groups}


def _share(part: Optional[float], whole: Optional[float]) -> Optional[float]:
    if part is None or whole is None or whole == 0:
        return None
    return part / whole * 100.0


with page_loader(
    messages=[
        "Loading scope and filters...",
        "Building the cost model...",
        "Aggregating...",
        "Preparing charts...",
        "Finalizing view...",
    ],
    show_status=False,
) as loader:
    loader.step("Loading scope and filters...")
    loader.step("Building the cost model...")
    _set_loading("Building KPI inputs...", 2)
    with perf_step("load_cost_model", page_key=page_perf_key, meta={"year": int(selected_year)}):
        cm = load_cost_model(int(selected_year), scope_dict)
    cost_model_warning = get_cost_model_warning(int(selected_year), scope_dict)
    if cost_model_warning:
        st.warning(cost_model_warning, icon="⚠️")
    df_baseline = cm.get("BASELINE", pd.DataFrame()).copy()
    df_expected = cm.get("EXPECTED", pd.DataFrame()).copy()
    df_actual = cm.get("ACTUAL", pd.DataFrame()).copy()

    loader.step("Aggregating...")
    primary_label = "—"
    primary_df = None
    if _has_data(df_expected):
        primary_label = "EXPECTED"
        primary_df = df_expected
    elif _has_data(df_baseline):
        primary_label = "BASELINE"
        primary_df = df_baseline
    elif _has_data(df_actual):
        primary_label = "ACTUAL"
        primary_df = df_actual

    baseline_available = _has_data(df_baseline)
    _set_loading("Computing KPI strip...", 3)
    with perf_step("compute_primary_metrics", page_key=page_perf_key):
        primary_metrics = _scenario_metrics(primary_df) if primary_df is not None else {}
        baseline_metrics = _scenario_metrics(df_baseline) if baseline_available else {}

    primary_total = primary_metrics.get("total") if primary_metrics else None
    baseline_total = baseline_metrics.get("total") if baseline_metrics else None
    loader.step("Preparing charts...")

    wf_share = _share(primary_metrics.get("wf_total"), primary_total) if primary_metrics else None
    nwf_share = _share(primary_metrics.get("nwf_total"), primary_total) if primary_metrics else None
    sod_denom = None
    if primary_metrics:
        sod_denom = (primary_metrics.get("sod_total") or 0.0) + (primary_metrics.get("contractor_cs_total") or 0.0)
    sod_share = _share(primary_metrics.get("sod_total"), sod_denom) if primary_metrics else None

    wf_share_base = _share(baseline_metrics.get("wf_total"), baseline_total) if baseline_metrics else None
    nwf_share_base = _share(baseline_metrics.get("nwf_total"), baseline_total) if baseline_metrics else None
    sod_base_denom = None
    if baseline_metrics:
        sod_base_denom = (baseline_metrics.get("sod_total") or 0.0) + (baseline_metrics.get("contractor_cs_total") or 0.0)
    sod_share_base = _share(baseline_metrics.get("sod_total"), sod_base_denom) if baseline_metrics else None

    delta_total = (primary_total - baseline_total) if (primary_total is not None and baseline_total is not None) else None
    delta_total_pct = (
        (delta_total / baseline_total * 100.0) if (delta_total is not None and baseline_total) else None
    )
    loader.step("Finalizing view...")


if primary_df is None:
    st.info("No data available for KPIs.")

# Row 1 — KPI Strip
_set_loading("Rendering KPI strip...", 3)
st.markdown("<div id='dashboard-kpi-strip-anchor'></div>", unsafe_allow_html=True)
st.markdown(
    """
    <style>
    @import url("https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20,400,0,0");
    /* FinOps KPI cards – Dashboard only */
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-card {
        height: 164px;
        display: flex;
        background: transparent;
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 11px;
        overflow: hidden;
        position: relative;
        isolation: isolate;
        box-shadow: 0 6px 14px rgba(2, 6, 23, 0.12), 0 1px 0 rgba(255,255,255,0.03) inset;
        transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-layer {
        position: absolute;
        inset: 0 0 0 4px;
        pointer-events: none;
        z-index: 0;
        background:
            radial-gradient(160px 90px at 95% 12%, color-mix(in srgb, var(--finops-kpi-accent) 30%, transparent), transparent 76%),
            linear-gradient(180deg, color-mix(in srgb, var(--finops-kpi-accent) 10%, transparent), transparent 64%);
        opacity: 0.9;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art {
        position: absolute;
        right: 0;
        bottom: 0;
        width: 50%;
        height: 56%;
        display: inline-flex;
        align-items: flex-end;
        gap: 4px;
        pointer-events: none;
        z-index: 0;
        opacity: 0.3;
        filter: saturate(1.05) blur(0.4px);
        -webkit-mask-image: linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 0.08) 28%, rgba(0, 0, 0, 0.55) 54%, #000 100%);
        mask-image: linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 0.08) 28%, rgba(0, 0, 0, 0.55) 54%, #000 100%);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-bar {
        flex: 1 1 auto;
        border-radius: 3px 3px 0 0;
        background: linear-gradient(180deg, rgba(148, 163, 184, 0.9), rgba(148, 163, 184, 0.2));
        min-height: 3px;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-bar {
        background: linear-gradient(180deg, rgba(74, 222, 128, 0.95), rgba(34, 197, 94, 0.22));
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-bar {
        background: linear-gradient(180deg, rgba(251, 191, 36, 0.95), rgba(245, 158, 11, 0.24));
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-bar {
        background: linear-gradient(180deg, rgba(248, 113, 113, 0.95), rgba(239, 68, 68, 0.24));
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--wave .finops-kpi-bg-bar,
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--dotline .finops-kpi-bg-bar,
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--curve .finops-kpi-bg-bar {
        display: none;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--wave,
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--dotline,
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--curve {
        width: 58%;
        height: 60%;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-svg {
        width: 100%;
        height: 100%;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-fill {
        display: none !important;
        fill: none !important;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-poly {
        fill: none;
        stroke: rgba(148, 163, 184, 0.72);
        stroke-width: 1.05px;
        stroke-linecap: round;
        stroke-linejoin: round;
        vector-effect: non-scaling-stroke;
        opacity: 0.42;
        filter: drop-shadow(0 0 1px color-mix(in srgb, var(--finops-kpi-accent) 20%, transparent));
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-curve {
        fill: none;
        stroke: rgba(148, 163, 184, 0.72);
        stroke-width: 1.05px;
        stroke-linecap: round;
        stroke-linejoin: round;
        vector-effect: non-scaling-stroke;
        opacity: 0.42;
        filter: drop-shadow(0 0 1px color-mix(in srgb, var(--finops-kpi-accent) 20%, transparent));
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-dots {
        position: absolute;
        inset: 0;
        pointer-events: none;
        opacity: 0.2;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-dot-el {
        position: absolute;
        width: 5px;
        height: 5px;
        border-radius: 50%;
        background: rgba(226, 232, 240, 0.9);
        transform: translate(-50%, -50%);
        box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.5), 0 0 3px color-mix(in srgb, var(--finops-kpi-accent) 24%, transparent);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-fill { fill: rgba(34, 197, 94, 0.16); }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-fill { fill: rgba(245, 158, 11, 0.16); }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-fill { fill: rgba(239, 68, 68, 0.16); }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-poly { stroke: rgba(74, 222, 128, 0.76); }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-poly { stroke: rgba(251, 191, 36, 0.76); }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-poly { stroke: rgba(248, 113, 113, 0.76); }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-curve { stroke: rgba(74, 222, 128, 0.78); }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-curve { stroke: rgba(251, 191, 36, 0.78); }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-curve { stroke: rgba(248, 113, 113, 0.78); }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-card::before {
        content: "";
        position: absolute;
        inset: 0 0 45% 4px;
        pointer-events: none;
        z-index: 0;
        background: linear-gradient(180deg, rgba(255,255,255,0.10) 0%, rgba(255,255,255,0.03) 55%, rgba(255,255,255,0.0) 100%);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-card::after {
        content: "";
        position: absolute;
        inset: 0 0 0 4px;
        pointer-events: none;
        z-index: 0;
        background-image: repeating-radial-gradient(circle at 12% 10%, rgba(255,255,255,0.06) 0 0.7px, transparent 0.7px 2.8px);
        opacity: 0.04;
        filter: blur(0.7px);
        mix-blend-mode: screen;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-watermark {
        position: absolute;
        right: 10px;
        top: 8px;
        z-index: 0;
        pointer-events: none;
        opacity: 0.05;
        filter: blur(0.4px);
        line-height: 1;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-watermark .material-symbols-outlined {
        font-size: 54px;
        color: rgba(226, 232, 240, 0.65);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-accent {
        width: 4px;
        flex-shrink: 0;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-body {
        padding: 11px 13px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        width: 100%;
        position: relative;
        z-index: 1;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 4px;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-title-wrap {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        min-width: 0;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-title-icon {
        font-size: 14px;
        color: rgba(148, 163, 184, 0.9);
        flex-shrink: 0;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-title {
        font-size: 0.78rem;
        color: rgba(255,255,255,0.74);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 90%;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-help {
        font-size: 0.75rem;
        color: rgba(255,255,255,0.6);
        border: 1px solid rgba(255,255,255,0.4);
        border-radius: 50%;
        width: 16px;
        height: 16px;
        line-height: 14px;
        text-align: center;
        cursor: default;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-meta {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        flex-shrink: 0;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-status {
        display: inline-flex;
        align-items: center;
        gap: 2px;
        font-size: 0.60rem;
        border-radius: 999px;
        padding: 2px 6px;
        border: 1px solid rgba(148, 163, 184, 0.35);
        background: rgba(148, 163, 184, 0.14);
        color: rgba(226, 232, 240, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-status .material-symbols-outlined {
        font-size: 11px;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-status--good {
        border-color: rgba(34, 197, 94, 0.55);
        background: rgba(34, 197, 94, 0.16);
        color: rgba(187, 247, 208, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-status--watch {
        border-color: rgba(245, 158, 11, 0.55);
        background: rgba(245, 158, 11, 0.16);
        color: rgba(253, 230, 138, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-status--risk {
        border-color: rgba(239, 68, 68, 0.55);
        background: rgba(239, 68, 68, 0.16);
        color: rgba(254, 202, 202, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-value-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-value {
        font-size: clamp(1.22rem, 1.7vw, 1.82rem);
        font-weight: 700;
        margin: 2px 0 5px 0;
        color: rgba(248, 250, 252, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-trend {
        display: inline-flex;
        align-items: center;
        gap: 2px;
        border-radius: 999px;
        font-size: 0.62rem;
        line-height: 1;
        padding: 3px 6px;
        border: 1px solid rgba(148, 163, 184, 0.35);
        background: rgba(148, 163, 184, 0.14);
        color: rgba(226, 232, 240, 0.95);
        white-space: nowrap;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-trend .material-symbols-outlined {
        font-size: 11px;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-trend--good {
        border-color: rgba(34, 197, 94, 0.55);
        background: rgba(34, 197, 94, 0.16);
        color: rgba(187, 247, 208, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-trend--watch {
        border-color: rgba(245, 158, 11, 0.55);
        background: rgba(245, 158, 11, 0.16);
        color: rgba(253, 230, 138, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-trend--risk {
        border-color: rgba(239, 68, 68, 0.55);
        background: rgba(239, 68, 68, 0.16);
        color: rgba(254, 202, 202, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-chips {
        margin-bottom: 6px;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-chip {
        display: inline-block;
        font-size: 0.68rem;
        padding: 2px 6px;
        border-radius: 10px;
        margin-right: 4px;
        background: rgba(255,255,255,0.12);
        color: rgba(248, 250, 252, 0.9);
        white-space: nowrap;
        max-width: 100%;
        overflow: hidden;
        text-overflow: ellipsis;
        vertical-align: top;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-lines {
        font-size: 0.72rem;
        color: rgba(255,255,255,0.5);
        line-height: 1.2;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    @media (hover: hover) and (pointer: fine) {
        [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-card:hover {
            transform: translateY(-1px);
            box-shadow: 0 10px 20px rgba(2, 6, 23, 0.18), 0 1px 0 rgba(255,255,255,0.05) inset;
            border-color: rgba(255,255,255,0.2);
        }
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .finops-kpi-line {
        height: 0.9rem;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
    }
    [data-testid="stAppViewContainer"]:has(#dashboard-kpi-strip-anchor) .material-symbols-outlined {
        font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 20;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
cols = st.columns(5)
kpi_labels = [
    "Total Cost of Ownership (Forecast)",
    "Workforce Share",
    "System of Delivery (WF)",
    "Non-Workforce Share",
    "Change vs Baseline",
]

values = [
    _fmt_currency(primary_total),
    _fmt_pct(wf_share),
    _fmt_pct(sod_share),
    _fmt_pct(nwf_share),
    _fmt_currency(delta_total),
]

deltas = [
    (
        f"{_fmt_currency(delta_total)} ({_fmt_pct(delta_total_pct)})"
        if delta_total is not None and delta_total_pct is not None
        else "—"
    ),
    _fmt_pp(wf_share - wf_share_base) if (wf_share is not None and wf_share_base is not None) else "—",
    _fmt_pp(sod_share - sod_share_base) if (sod_share is not None and sod_share_base is not None) else "—",
    _fmt_pp(nwf_share - nwf_share_base) if (nwf_share is not None and nwf_share_base is not None) else "—",
    _fmt_pct(delta_total_pct),
]

help_texts = {
    "Total Cost of Ownership (Forecast)": "Total cost for the selected scope in the Forecast scenario (Expected when available). Delta compares vs Baseline.",
    "Workforce Share": "Workforce portion of total cost (WF ÷ Total). Delta is change vs Baseline in percentage points.",
    "System of Delivery (WF)": "Within System of Delivery, the share that is WF SoD vs Contractor CS (SoD ÷ (SoD + Contractor CS)). Delta vs Baseline in percentage points.",
    "Non-Workforce Share": "Non-Workforce portion of total cost (NWF ÷ Total). Delta vs Baseline in percentage points.",
    "Change vs Baseline": "Forecast total minus Baseline total. Delta shows the percent change vs Baseline.",
}

accent_map = {
    "Total Cost of Ownership (Forecast)": "#7c3aed",
    "Workforce Share": "#2563eb",
    "System of Delivery (WF)": "#0ea5e9",
    "Non-Workforce Share": "#f59e0b",
    "Change vs Baseline": "#16a34a",
}
icon_map = {
    "Total Cost of Ownership (Forecast)": "account_balance",
    "Workforce Share": "groups",
    "System of Delivery (WF)": "alt_route",
    "Non-Workforce Share": "receipt_long",
    "Change vs Baseline": "monitoring",
}
DASHBOARD_KPI_MOTIF_MAP = {
    "Total Cost of Ownership (Forecast)": "bars",
    "Workforce Share": "curve",
    "System of Delivery (WF)": "dotline",
    "Non-Workforce Share": "curve",
    "Change vs Baseline": "wave",
}

for col, label, value, delta in zip(cols, kpi_labels, values, deltas):
    with col:
        if label == "Total Cost of Ownership (Forecast)":
            chips = ["Forecast"]
        elif label == "Workforce Share":
            chips = ["WF"]
        elif label == "System of Delivery (WF)":
            chips = ["SoD"]
        elif label == "Non-Workforce Share":
            chips = ["NWF"]
        else:
            chips = ["Baseline vs Forecast"]
        lines = [f"Δ vs baseline: {delta}"] if label != "Change vs Baseline" else [f"% vs baseline: {delta}"]
        status = "neutral"
        trend_tone = "neutral"
        if label == "Change vs Baseline" and delta_total_pct is not None:
            if abs(delta_total_pct) <= 3:
                status = "good"
                trend_tone = "good"
            elif abs(delta_total_pct) <= 10:
                status = "watch"
                trend_tone = "watch"
            else:
                status = "risk"
                trend_tone = "risk"
        elif label in {"Workforce Share", "System of Delivery (WF)", "Non-Workforce Share"} and delta not in {"—", "", None}:
            status = "watch" if str(delta).startswith("+") else "good"
        spark_values: Optional[list[float]] = None
        spark_tone = trend_tone
        spark_motif = DASHBOARD_KPI_MOTIF_MAP.get(label, "bars")
        if label == "Total Cost of Ownership (Forecast)" and primary_total is not None and baseline_total is not None:
            spark_values = [
                max(baseline_total * 0.82, 0.0),
                max(baseline_total * 0.9, 0.0),
                max(baseline_total * 0.97, 0.0),
                max(primary_total, 0.0),
            ]
            spark_tone = "risk" if (delta_total or 0.0) > 0 else "good"
            spark_motif = DASHBOARD_KPI_MOTIF_MAP.get(label, "bars")
        elif label == "Workforce Share" and wf_share is not None and wf_share_base is not None:
            spark_values = (
                [0.24, 0.41, 0.34, 0.56, 0.47, 0.66]
                if (wf_share - wf_share_base) >= 0
                else [0.66, 0.57, 0.61, 0.46, 0.4, 0.31]
            )
            spark_tone = "watch" if (wf_share - wf_share_base) > 0 else "good"
            spark_motif = DASHBOARD_KPI_MOTIF_MAP.get(label, "curve")
        elif label == "System of Delivery (WF)" and sod_share is not None and sod_share_base is not None:
            spark_values = (
                [0.52, 0.34, 0.58, 0.39, 0.66, 0.44, 0.73]
                if (sod_share - sod_share_base) >= 0
                else [0.73, 0.56, 0.66, 0.47, 0.58, 0.4, 0.48]
            )
            spark_tone = "watch" if (sod_share - sod_share_base) > 0 else "good"
            spark_motif = DASHBOARD_KPI_MOTIF_MAP.get(label, "dotline")
        elif label == "Non-Workforce Share" and nwf_share is not None and nwf_share_base is not None:
            spark_values = (
                [0.18, 0.27, 0.23, 0.35, 0.31, 0.46, 0.42, 0.55]
                if (nwf_share - nwf_share_base) >= 0
                else [0.55, 0.47, 0.5, 0.38, 0.42, 0.3, 0.34, 0.22]
            )
            spark_tone = "watch" if (nwf_share - nwf_share_base) > 0 else "good"
            spark_motif = DASHBOARD_KPI_MOTIF_MAP.get(label, "curve")
        elif label == "Change vs Baseline" and delta_total is not None:
            spark_values = (
                [0.16, 0.31, 0.25, 0.45, 0.37, 0.61, 0.53, 0.76]
                if (delta_total or 0.0) >= 0
                else [0.76, 0.63, 0.68, 0.52, 0.58, 0.39, 0.44, 0.26]
            )
            spark_tone = trend_tone
            spark_motif = DASHBOARD_KPI_MOTIF_MAP.get(label, "wave")
        render_finops_kpi_card(
            title=label,
            value=value if value is not None else "—",
            tip=help_texts.get(label, ""),
            chips=chips,
            lines=lines,
            accent_color=accent_map.get(label, "#64748b"),
            icon=icon_map.get(label, "insights"),
            status=status,
            status_label=("Variance" if label == "Change vs Baseline" else "Signal"),
            trend_text=(str(delta) if delta not in {"—", "", None} else ""),
            trend_tone=trend_tone,
            spark_values=spark_values,
            spark_tone=spark_tone,
            spark_motif=spark_motif,
        )
mark_first_kpi_render(page_perf_key)

_render_section_divider()

# Row 2 — Cost Structure
_set_loading("Rendering charts (1/3)...", 4)
left, right = st.columns([1, 1])
with left:
    render_chart_title_with_infotip(
        "Cost Anatomy",
        "Breakdown of Total Cost of Ownership into Workforce (WF) vs Non-Workforce (NWF), and their main components. "
        "Scenario: Forecast / Expected.",
    )
    anatomy = build_cost_anatomy_from_cost_lines(df_baseline)
    if sel_teams:
        wf_buckets = anatomy.get("wf_buckets") or []
        anatomy["wf_buckets"] = [b for b in wf_buckets if str(b.get("name")) != "Overhead"]
        anatomy["wf_overhead_total"] = 0.0
    if df_baseline is None or df_baseline.empty:
        st.info("No baseline data available for Cost Anatomy.")
    else:
        amt = pd.to_numeric(df_baseline.get("AMOUNT"), errors="coerce").fillna(0.0)
        df_cost = df_baseline.loc[amt.gt(0)].copy()
        amt = amt.loc[df_cost.index]

        wf_mask, nwf_mask = _wf_nwf_mask(df_cost)
        df_wf = df_cost.loc[wf_mask].copy()
        df_nwf = df_cost.loc[nwf_mask].copy()

        sub_u = df_wf.get("SUBCOMPONENT", pd.Series("", index=df_wf.index)).fillna("").astype(str).str.upper().str.strip()
        teamname_u = df_wf.get("TEAMNAME", pd.Series("", index=df_wf.index)).fillna("").astype(str).str.upper().str.strip()
        wf_type = df_wf.get("WORKFORCE_TYPE", pd.Series("", index=df_wf.index)).fillna("").astype(str).str.upper().str.strip()

        is_delivery = sub_u.isin({"DELIVERY TEAM", "DELIVERY"})
        is_contr_c = sub_u.eq("CONTRACTOR C")
        is_team_oh = sub_u.eq("TEAM")
        is_prog_oh = sub_u.eq("PROGRAM") | teamname_u.str.contains("PROGRAM OVERHEAD", na=False) | wf_type.eq("PROGRAM")
        if sel_teams:
            is_prog_oh = pd.Series(False, index=df_wf.index)

        wf_amt = amt.loc[df_wf.index] if not df_wf.empty else pd.Series([], dtype=float)
        wf_delivery = float(wf_amt.loc[is_delivery].sum() or 0.0)
        wf_contr_c = float(wf_amt.loc[is_contr_c].sum() or 0.0)
        wf_team_oh = float(wf_amt.loc[is_team_oh].sum() or 0.0)
        wf_prog_oh = float(wf_amt.loc[is_prog_oh].sum() or 0.0)
        wf_other = float(wf_amt.sum() or 0.0) - (wf_delivery + wf_contr_c + wf_team_oh + wf_prog_oh)
        if wf_other > 0:
            wf_team_oh += wf_other

        nwf_buckets = anatomy.get("nwf_buckets") or []
        nwf_order = ["Invoices", "Azure", "AWS", "MSP", "Contractor CS", "Travel", "Other"]
        nwf_map = {b.get("name"): float(b.get("value") or 0.0) for b in nwf_buckets}

        wf_children = [
            {"name": "Delivery", "value": wf_delivery, "parent": "WF"},
            {"name": "Contr C", "value": wf_contr_c, "parent": "WF"},
            {"name": "Team", "value": wf_team_oh, "parent": "WF"},
            {"name": "Overhead", "value": wf_prog_oh, "parent": "WF"},
        ]

        nwf_children = []
        for key in nwf_order:
            val = float(nwf_map.get(key, 0.0) or 0.0)
            if val <= 0:
                continue
            display = "Contr CS" if key == "Contractor CS" else key
            nwf_children.append({"name": display, "value": val, "parent": "NWF"})

        wf_base = "#4F76B6"
        nwf_base = "#B8943B"
        wf_shades = {
            "Delivery": 0.90,
            "Contr C": 0.95,
            "Team": 1.05,
            "Overhead": 1.10,
        }
        nwf_shades = {
            "Invoices": 0.90,
            "Azure": 0.96,
            "AWS": 1.05,
            "MSP": 0.88,
            "Contr CS": 0.94,
            "Travel": 1.08,
            "Other": 1.02,
        }

        inner_data = [
            {"name": "WF", "value": float(anatomy.get("wf_total") or 0.0), "parent": "WF", "itemStyle": {"color": wf_base}},
            {"name": "NWF", "value": float(anatomy.get("nwf_total") or 0.0), "parent": "NWF", "itemStyle": {"color": nwf_base}},
        ]

        MIN_OUTER_LABEL_SHARE = 0.02
        outer_total = float(sum(d["value"] for d in wf_children + nwf_children) or 0.0)
        outer_data = []
        for item in wf_children:
            if item["value"] <= 0:
                continue
            color = _shade_hex(wf_base, wf_shades.get(item["name"], 1.0))
            share = (float(item["value"]) / outer_total) if outer_total > 0 else 0.0
            label_enabled = share >= MIN_OUTER_LABEL_SHARE
            outer_data.append(
                {
                    **item,
                    "itemStyle": {"color": color},
                    "label": {"show": bool(label_enabled)},
                    "labelLine": {"show": bool(label_enabled)},
                    "emphasis": {"label": {"show": False}, "labelLine": {"show": False}} if not label_enabled else None,
                    "share": share,
                }
            )
        for item in nwf_children:
            color = _shade_hex(nwf_base, nwf_shades.get(item["name"], 1.0))
            share = (float(item["value"]) / outer_total) if outer_total > 0 else 0.0
            label_enabled = share >= MIN_OUTER_LABEL_SHARE
            outer_data.append(
                {
                    **item,
                    "itemStyle": {"color": color},
                    "label": {"show": bool(label_enabled)},
                    "labelLine": {"show": bool(label_enabled)},
                    "emphasis": {"label": {"show": False}, "labelLine": {"show": False}} if not label_enabled else None,
                    "share": share,
                }
            )

        total_amt = float(anatomy.get("total") or 0.0)
        option = _build_cost_anatomy_nested_pie_option(inner_data, outer_data, total_amt=total_amt, theme=theme)
        option = _strip_chart_title(option)
        chart_key = f"dash_cost_anatomy_{dashboard_filters_sig}"
        with perf_step("render_cost_anatomy_chart", page_key=page_perf_key):
            render_echart(option, height="420px", key=chart_key, page_theme=page_theme)
with right:
    filters_kpi = {
        "year": int(selected_year),
        "program": list(sel_programs or []),
        "team": list(sel_teams or []),
        "app_group": list(sel_groups or []),
    }
    filters_kpi_signature = filters_signature(filters_kpi)
    with perf_step("compute_cost_driver_kpis", page_key=page_perf_key):
        kpi_data = _compute_cost_driver_kpis(
            year=int(selected_year),
            scope_key=json.dumps(
                {"programs": list(sel_programs or []), "teams": list(sel_teams or []), "groups": list(sel_groups or [])},
                sort_keys=True,
            ),
            filters_kpi_signature=filters_kpi_signature,
            filters_kpi=filters_kpi,
            user_scope_sig=dashboard_user_scope_sig,
            cache_buster=dashboard_cache_buster,
        )
    fg_filters = {
        "year": int(selected_year),
        "program": list(sel_programs or []),
        "team": list(sel_teams or []),
        "app_group": list(sel_groups or []),
    }
    fg_scope_key = json.dumps(
        {"programs": list(sel_programs or []), "teams": list(sel_teams or []), "groups": list(sel_groups or [])},
        sort_keys=True,
    )
    fg_filters_sig = filters_signature(fg_filters)
    with perf_step("compute_financial_governance_kpis", page_key=page_perf_key):
        fg_data = _compute_financial_governance_kpis(
            year=int(selected_year),
            scope_key=fg_scope_key,
            filters_sig=fg_filters_sig,
            filters_scope=fg_filters,
            user_scope_sig=dashboard_user_scope_sig,
            cache_buster=dashboard_cache_buster,
        )

    st.markdown("<div id='dashboard-kpi-six-panel-anchor'></div>", unsafe_allow_html=True)
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"]:has(#dashboard-kpi-six-panel-anchor) .kpi6-card {
            border: 1px solid rgba(255,255,255,0.12);
            border-radius: 10px;
            padding: 10px 12px 9px 12px;
            background: linear-gradient(180deg, #242424, #242424);
            box-shadow: 0 8px 20px rgba(36, 36, 36, 0.55);
            min-height: 154px;
        }
        [data-testid="stAppViewContainer"]:has(#dashboard-kpi-six-panel-anchor) .kpi6-title {
            font-size: 0.66rem;
            color: rgba(148,163,184,0.95);
            margin: 0 0 3px 0;
            letter-spacing: 0.02em;
        }
        [data-testid="stAppViewContainer"]:has(#dashboard-kpi-six-panel-anchor) .kpi6-value {
            font-size: 1.60rem;
            line-height: 1.1;
            font-weight: 650;
            color: rgba(248,250,252,0.96);
            margin: 0 0 4px 0;
        }
        [data-testid="stAppViewContainer"]:has(#dashboard-kpi-six-panel-anchor) .kpi6-subtitle {
            font-size: 0.76rem;
            color: rgba(226,232,240,0.74);
            margin: 0;
        }
        [data-testid="stAppViewContainer"]:has(#dashboard-kpi-six-panel-anchor) .kpi6-subtitle-row {
            display: inline-flex;
            align-items: center;
            gap: 5px;
            margin: 0;
        }
        [data-testid="stAppViewContainer"]:has(#dashboard-kpi-six-panel-anchor) .kpi6-subicon {
            font-size: 12px;
            line-height: 1;
            color: rgba(226,232,240,0.65);
            opacity: 0.95;
        }
        [data-testid="stAppViewContainer"]:has(#dashboard-kpi-six-panel-anchor) .kpi6-secondary {
            font-size: 0.70rem;
            color: rgba(148,163,184,0.95);
            margin-top: 2px;
        }
        [data-testid="stAppViewContainer"]:has(#dashboard-kpi-six-panel-anchor) [data-testid="stVerticalBlockBorderWrapper"] {
            border-bottom-color: transparent !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    var_pct = fg_data.get("variance_pct")
    var_value = "—" if var_pct is None else f"{float(var_pct):+.1f}%"
    var_amt = fg_data.get("variance_amt")
    plan_total = float(fg_data.get("plan_total") or 0.0)
    if var_pct is None and plan_total <= 0:
        var_secondary = "Plan is zero or unavailable in scope."
    elif var_amt is None:
        var_secondary = "Variance unavailable in scope."
    else:
        var_secondary = f"{_fmt_currency_compact(var_amt)} vs {_fmt_currency_compact(plan_total)} plan"

    panel_cards = [
        {
            "title": "COST EFFICIENCY",
            "value": ("—" if kpi_data.get("eff_ratio") is None else f"{float(kpi_data.get('eff_ratio') or 0.0):.2f}x"),
            "subtitle": "Actual / Projected",
            "icon": "sync_alt",
            "secondary": "",
        },
        {
            "title": "COST AT RISK",
            "value": _fmt_pct(kpi_data.get("risk_pct")),
            "subtitle": "Projected cost unmapped",
            "icon": "warning_amber",
            "secondary": f"{_fmt_currency_compact(kpi_data.get('risk_unassigned'))} at risk",
        },
        {
            "title": "VOLATILITY",
            "value": _fmt_pct(kpi_data.get("vol_avg")),
            "subtitle": "Avg drift vs Baseline (last 6)",
            "icon": "show_chart",
            "secondary": "",
        },
        {
            "title": "RUN RATE",
            "value": _fmt_currency_compact(fg_data.get("run_rate")),
            "subtitle": "Annualized Actual NWF",
            "icon": "speed",
            "secondary": f"{_fmt_currency_compact(fg_data.get('ytd_actual'))} YTD across {int(fg_data.get('elapsed_periods') or 0)} periods",
        },
        {
            "title": "OUTLOOK",
            "value": _fmt_currency_compact(fg_data.get("outlook")),
            "subtitle": "Forecast EOY (run rate)",
            "icon": "insights",
            "secondary": f"Annualized at {int(fg_data.get('periods_per_year') or 12)} periods/year",
        },
        {
            "title": "OL vs PL",
            "value": var_value,
            "subtitle": "Forecast vs Plan",
            "icon": "monitoring",
            "secondary": var_secondary,
        },
    ]

    with st.container(border=True):
        render_chart_title_with_infotip(
            "Cost Driver + Financial Governance KPIs",
            "Six compact KPIs in current scope: Cost Efficiency, Cost at Risk, Volatility, Run Rate, Outlook, and Outlook vs Plan.",
        )
        for row_idx in range(2):
            row_cards = panel_cards[row_idx * 3: (row_idx + 1) * 3]
            row_cols = st.columns(3)
            for col, card in zip(row_cols, row_cards):
                with col:
                    st.markdown(
                        (
                            "<div class='kpi6-card'>"
                            + f"<div class='kpi6-title'>{card['title']}</div>"
                            + f"<div class='kpi6-value'>{card['value']}</div>"
                            + "<p class='kpi6-subtitle-row'>"
                            + f"<span class='material-symbols-outlined kpi6-subicon'>{card['icon']}</span>"
                            + f"<span class='kpi6-subtitle'>{card['subtitle']}</span>"
                            + "</p>"
                            + f"<div class='kpi6-secondary'>{card['secondary']}</div>"
                            + "</div>"
                        ),
                        unsafe_allow_html=True,
                    )
            if row_idx == 0:
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

        notes: list[str] = []
        if fg_data.get("warning_message"):
            notes.append(str(fg_data.get("warning_message") or ""))
        if bool(fg_data.get("plan_fallback")):
            notes.append("Plan (Baseline total, no additional-cost breakdown available).")
        if notes:
            st.caption(" | ".join(notes))

        if is_debug_enabled(label="Debug"):
            with st.expander("Debug — Financial Governance diagnostics", expanded=False):
                st.caption("Detected dimensions and fallback behavior used by the Financial Governance cards.")
                st.json(fg_data.get("debug", {}))

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
_render_section_divider()

# Row 4 — Non-Workforce Spend Dynamics
_set_loading("Rendering charts (2/3)...", 4)
render_chart_title_with_infotip(
    "Non-Workforce Spend Evolution",
    "Actual non-workforce spend over time by spend category (Invoices, Cloud, MSP, Contractor CS, Travel). "
    "Scenario: Actual (Apptio), program-level actuals.",
)
opt_nwf = _placeholder_area(theme, "Non-Workforce Spend Evolution")
opt_nwf = _strip_chart_title(opt_nwf)
month_labels, nwf_series, nwf_debug = _apptio_nwf_evolution_series(
    int(selected_year),
    sel_programs,
    sel_teams,
    base_df,
)
grid_nwf = opt_nwf.get("grid", {})
grid_nwf.update({"left": 60, "right": 20, "bottom": 78, "containLabel": True})
opt_nwf["grid"] = grid_nwf
opt_nwf["yAxis"] = {**opt_nwf.get("yAxis", {}), "min": 0}
base_axis = _option_base(theme).get("xAxis", {})
opt_nwf["xAxis"] = {
    **opt_nwf.get("xAxis", {}),
    "axisLabel": {
        **base_axis.get("axisLabel", {}),
        **(opt_nwf.get("xAxis", {}).get("axisLabel", {}) if isinstance(opt_nwf.get("xAxis", {}), dict) else {}),
        "show": True,
        "hideOverlap": False,
        "interval": 0,
        "rotate": 35,
        "fontSize": 10,
        "margin": 14,
    },
    "axisLine": {
        **base_axis.get("axisLine", {}),
        **(opt_nwf.get("xAxis", {}).get("axisLine", {}) if isinstance(opt_nwf.get("xAxis", {}), dict) else {}),
        "show": True,
    },
    "axisTick": {"show": True, "alignWithLabel": True},
}
if not month_labels:
    st.info("No Apptio ACTUAL NWF data for selected scope (program-level).")
else:
    opt_nwf["xAxis"]["data"] = month_labels
    for series in opt_nwf["series"]:
        name = series.get("name")
        if name in nwf_series:
            series["data"] = nwf_series.get(name, [0.0] * len(month_labels))
NWF_SECTION_CHART_HEIGHT = "392px"
with perf_step("render_nwf_evolution_chart", page_key=page_perf_key):
    render_echart(
        opt_nwf,
        height=NWF_SECTION_CHART_HEIGHT,
        key="dashboard_nwf_evolution",
        page_theme=page_theme,
    )
_render_section_divider()

# Row 5 — Capacity Reality Check
_set_loading("Rendering charts (3/3, capacity-demand)...", 4)
col_left, col_right = st.columns([1, 1])
effective_programs = sel_programs
effective_teams = sel_teams
sod_x, sod_cap_fte, sod_dem_fte, sod_cap_cost, sod_dem_cost, sod_debug = _build_capacity_demand_budget_series(
    int(selected_year),
    tuple(effective_programs or ()),
    tuple(effective_teams or ()),
    df_baseline,
    df_expected,
    filters_sig=dashboard_filters_sig,
    user_scope_sig=dashboard_user_scope_sig,
    cache_buster=dashboard_cache_buster,
)
with col_left:
    render_chart_title_with_infotip(
        "Demand Concentration (Apps)",
        "SoD demand concentration across application groups using Forecast / Expected derived FTE demand. "
        "Mapped app groups only; unassigned excluded.",
    )
    sod_demand_tidy = _get_sod_demand_tidy_df(df_expected)
    app_totals = (
        sod_demand_tidy.groupby("GROUPNAME", dropna=False)["DEMAND_FTE"]
        .sum()
        .reset_index()
        .rename(columns={"DEMAND_FTE": "FTE"})
        .sort_values("FTE", ascending=False)
    )
    app_totals["FTE"] = pd.to_numeric(app_totals["FTE"], errors="coerce").fillna(0.0).clip(lower=0.0)
    total_fte = float(app_totals["FTE"].sum() or 0.0)
    if app_totals.empty or total_fte <= 0:
        st.info("No SoD demand data in current scope.")
        render_echart(
            _placeholder_line(theme, "Staffing vs Demand (Cost)"),
            height="260px",
            key="dashboard_staffing_cost",
            page_theme=page_theme,
        )
    else:
        ranked_names = app_totals["GROUPNAME"].astype(str).tolist()
        n_apps_total = int(app_totals.shape[0])
        n_remaining = max(0, n_apps_total - 5)
        seg1 = ranked_names[0:1]
        seg2 = ranked_names[1:3]
        seg3 = ranked_names[3:5]
        seg4 = ranked_names[5:]

        def _pct(val: float, total: float) -> float:
            val = float(val or 0.0)
            total = float(total or 0.0)
            if total <= 0:
                return 0.0
            return float(max(0.0, min(100.0, val / total * 100.0)))

        def _fmt_names(names: list[str], max_show: int = 6) -> str:
            names = [str(n) for n in names if str(n)]
            if not names:
                return "(none)"
            if len(names) <= max_show:
                return ", ".join(names)
            shown = names[:max_show]
            remaining = len(names) - len(shown)
            return f"{', '.join(shown)} (+{remaining} more)"

        top1 = float(app_totals.head(1)["FTE"].sum() or 0.0)
        top3 = float(app_totals.iloc[1:3]["FTE"].sum() or 0.0)
        top5 = float(app_totals.iloc[3:5]["FTE"].sum() or 0.0)
        rest = max(total_fte - (top1 + top3 + top5), 0.0)
        segments = [
            ("Top 1", top1, seg1),
            ("Top 3", top3, seg2),
            ("Top 5", top5, seg3),
            ("Others", rest, seg4),
        ]
        seg_pct = [(name, _pct(val, total_fte), float(val or 0.0), raw) for name, val, raw in segments]
        import math
        seg_pct = [
            (
                name,
                0.0 if not math.isfinite(float(pct)) else float(pct),
                0.0 if not math.isfinite(float(val)) else float(val),
                raw,
            )
            for name, pct, val, raw in seg_pct
        ]
        base = _option_base(theme)
        option = {
            **base,
            "grid": {"left": "10%", "right": "10%", "top": 60, "bottom": 30, "containLabel": True},
            "xAxis": {"type": "value", "min": 0, "max": 100, "axisLabel": {"formatter": "{value}%"}},
            "yAxis": {"type": "category", "data": ["SoD Demand"], "axisTick": {"show": False}},
            "legend": {"top": 10, "left": "center"},
            "series": [],
            "tooltip": {"trigger": "item"},
        }
        series_list = []
        for name, pct, val, raw_names in seg_pct:
            pct_rounded = round(float(pct), 1)
            val_rounded = round(float(val), 1)
            app_list = _fmt_names(raw_names)
            series_list.append(
                {
                    "name": name,
                    "type": "bar",
                    "stack": "total",
                    "data": [{"name": app_list, "value": pct_rounded, "raw": val_rounded}],
                    "label": {"show": bool(pct_rounded >= 7), "position": "inside", "formatter": "{c}%"},
                }
            )
        option["series"] = series_list
        option["tooltip"]["formatter"] = "{a}<br/>{b}<br/>Share: {c}%"
        with perf_step("render_sod_demand_concentration_chart", page_key=page_perf_key):
            render_echart(option, height="392px", key="dashboard_sod_demand_concentration_main", page_theme=page_theme)
with col_right:
    render_chart_title_with_infotip(
        "SoD – Staffing vs Demand (FTE)",
        "SoD capacity vs demand in FTE over time. Capacity: Baseline SoD supply. "
        "Demand: Forecast / Expected demand.",
    )
    if not sod_x:
        st.info("No demand/capacity data available. Add headcount or ensure derived demand exists.")
        render_echart(
            _placeholder_line(theme, "Staffing vs Demand (FTE)"),
            height="392px",
            key="dashboard_staffing_fte",
            page_theme=page_theme,
        )
    else:
        opt_fte = _placeholder_area(theme, "Staffing vs Demand (FTE)")
        opt_fte = _strip_chart_title(opt_fte)
        opt_fte["grid"] = {
            "left": 60,
            "right": 20,
            "top": 60,
            "bottom": 30,
            "containLabel": True,
        }
        opt_fte["xAxis"]["data"] = sod_x
        opt_fte["series"] = opt_fte.get("series", [])[:2]
        opt_fte["series"][0]["name"] = "Capacity"
        opt_fte["series"][1]["name"] = "Demand"
        opt_fte["series"][0]["type"] = "line"
        opt_fte["series"][1]["type"] = "line"
        opt_fte["series"][0].pop("stack", None)
        opt_fte["series"][1].pop("stack", None)
        opt_fte["series"][0]["areaStyle"] = {"opacity": 0.16}
        opt_fte["series"][1]["areaStyle"] = {"opacity": 0.16}
        opt_fte.setdefault("legend", {})
        opt_fte["legend"].update({"top": 10, "left": "center", "data": ["Capacity", "Demand"]})
        opt_fte["series"][0].setdefault("lineStyle", {})["width"] = 3
        opt_fte["series"][1].setdefault("lineStyle", {})["width"] = 3
        opt_fte["series"][0]["lineStyle"]["color"] = "#22c55e"
        opt_fte["series"][1]["lineStyle"]["color"] = "#4C78A8"
        opt_fte["series"][0].setdefault("itemStyle", {})["color"] = "#22c55e"
        opt_fte["series"][1].setdefault("itemStyle", {})["color"] = "#4C78A8"
        opt_fte["series"][0]["areaStyle"] = {"opacity": 0.16, "color": "rgba(34,197,94,0.24)"}
        opt_fte["series"][1]["areaStyle"] = {"opacity": 0.16, "color": "rgba(76,120,168,0.22)"}
        opt_fte["series"][0]["data"] = [round(float(v), 2) for v in sod_cap_fte]
        opt_fte["series"][1]["data"] = [round(float(v), 2) for v in sod_dem_fte]
        opt_fte["tooltip"] = {
            "trigger": "axis",
            "formatter": "{b}<br/>Capacity: {c0}<br/>Demand: {c1}",
        }
        with perf_step("render_staffing_vs_demand_fte_chart", page_key=page_perf_key):
            render_echart(
                opt_fte,
                height="392px",
                key="dashboard_staffing_fte",
                page_theme=page_theme,
            )
    st.caption("Capacity excludes Team Overhead (TEAMFTE).")

if is_debug_enabled(label="Debug"):
    with st.expander("Debug: Staffing vs Demand", expanded=False):
        st.caption("Admin-only debug panel for staffing vs demand aggregation.")
        st.write(
            {
                "year": int(selected_year),
                "pi": "All",
                "programs": list(sel_programs or []),
                "teams": list(sel_teams or []),
                "app_groups": list(sel_groups or []),
                "scenarios": {"capacity": "BASELINE", "demand": "EXPECTED"},
                "sod_filters": "(none)",
            }
        )

        staffing_df = _build_staffing_capacity_df(
            int(selected_year),
            tuple(effective_programs or ()),
            tuple(effective_teams or ()),
            filters_sig=dashboard_filters_sig,
            user_scope_sig=dashboard_user_scope_sig,
            cache_buster=dashboard_cache_buster,
        )
        demand_df = _build_demand_fte_df(
            int(selected_year),
            tuple(effective_programs or ()),
            tuple(effective_teams or ()),
            filters_sig=dashboard_filters_sig,
            user_scope_sig=dashboard_user_scope_sig,
            cache_buster=dashboard_cache_buster,
        )
        st.markdown("**Staffing DF (capacity source)**")
        st.dataframe(staffing_df.head(200), use_container_width=True)
        st.markdown("**Demand DF (demand source)**")
        st.dataframe(demand_df.head(200), use_container_width=True)

    with st.expander("Debug — Active portfolio/db", expanded=False):
        active_key = get_active_portfolio_key() or "Default"
        display = st.session_state.get("active_portfolio_display_name") or active_key
        st.write(f"Active portfolio: {display} ({active_key})")
        try:
            df_db = fetch_df("SELECT DB_NAME() AS DB_NAME")
            db_name = df_db.iloc[0]["DB_NAME"] if df_db is not None and not df_db.empty else "Unknown"
            st.write(f"DB_NAME(): {db_name}")
        except Exception as e:
            st.write(f"DB_NAME() error: {e}")

        if staffing_df is not None and not staffing_df.empty:
            comp_norm = staffing_df["COMPONENT"].fillna("").astype(str).str.upper()
            allowed = comp_norm.isin(["DELIVERY", "CONTRACTOR C", "CONTRACTOR_C", "CONTRACTOR CS", "CONTRACTOR_CS"])
            excluded = comp_norm.str.contains("TEAM") | comp_norm.str.contains("OVERHEAD")
            staffing_df["capacity_fte_calc"] = staffing_df["FTE"].where(allowed & ~excluded, 0.0)
            cap_grouped = (
                staffing_df.groupby(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], dropna=False)["capacity_fte_calc"]
                .sum()
                .reset_index()
            )
        else:
            cap_grouped = pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "capacity_fte_calc"])

        if demand_df is not None and not demand_df.empty:
            dem_grouped = demand_df.copy()
        else:
            dem_grouped = pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "DEMAND_FTE"])

        merged = cap_grouped.merge(
            dem_grouped,
            on=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"],
            how="outer",
        )
        st.markdown("**Merged staffing vs demand (per team/PI)**")
        st.dataframe(merged.head(200), use_container_width=True)

        comp = staffing_df.copy() if isinstance(staffing_df, pd.DataFrame) else pd.DataFrame()
        if comp is not None and not comp.empty:
            comp["COMPONENT_NORM"] = comp["COMPONENT"].fillna("").astype(str).str.lower()
            comp["IN_CAPACITY"] = comp["COMPONENT_NORM"].isin(
                ["delivery", "contractor c", "contractor_c", "contractor cs", "contractor_cs"]
            ) & ~(
                comp["COMPONENT_NORM"].str.contains("team") | comp["COMPONENT_NORM"].str.contains("overhead")
            )
            comp_grouped = (
                comp.groupby(["TEAMNAME", "PI", "COMPONENT", "IN_CAPACITY"], dropna=False)["FTE"]
                .sum()
                .reset_index()
                .sort_values(["TEAMNAME", "PI", "FTE"], ascending=[True, True, False])
            )
            st.markdown("**Component FTE by team and PI (capacity source)**")
            st.dataframe(comp_grouped.head(200), use_container_width=True)

            included = sorted(set(comp.loc[comp["IN_CAPACITY"], "COMPONENT"].dropna().astype(str).tolist()))
            excluded = sorted(set(comp.loc[~comp["IN_CAPACITY"], "COMPONENT"].dropna().astype(str).tolist()))
            st.write(
                {
                    "capacity_components_included": included,
                    "capacity_components_excluded": excluded,
                }
            )

            team_choice = sel_teams[0] if sel_teams else None
            if not team_choice:
                if "ERNE WEST" in set(comp["TEAMNAME"].astype(str).tolist()):
                    team_choice = "ERNE WEST"
                else:
                    by_team = comp.groupby("TEAMNAME")["FTE"].sum().sort_values(ascending=False)
                    team_choice = by_team.index[0] if not by_team.empty else None
            if team_choice:
                comp_team = comp.loc[comp["TEAMNAME"].eq(team_choice)].copy()
                comp_team_bucket = pd.Series("OTHER", index=comp_team.index, dtype=str)
                comp_team_bucket.loc[comp_team["COMPONENT_NORM"].str.contains("delivery")] = "DELIVERY"
                comp_team_bucket.loc[
                    comp_team["COMPONENT_NORM"].str.contains("contractor") & comp_team["COMPONENT_NORM"].str.contains("cs")
                ] = "CONTRACTOR_CS"
                comp_team_bucket.loc[
                    comp_team["COMPONENT_NORM"].str.contains("contractor") & ~comp_team["COMPONENT_NORM"].str.contains("cs")
                ] = "CONTRACTOR_C"
                comp_team_bucket.loc[
                    comp_team["COMPONENT_NORM"].str.contains("team") | comp_team["COMPONENT_NORM"].str.contains("overhead")
                ] = "TEAM"
                comp_team = comp_team.assign(_BUCKET=comp_team_bucket)
                bucket_sum = comp_team.groupby("_BUCKET")["FTE"].sum()
                team_fte = float(bucket_sum.get("TEAM", 0.0))
                delivery_fte = float(bucket_sum.get("DELIVERY", 0.0))
                c_fte = float(bucket_sum.get("CONTRACTOR_C", 0.0))
                cs_fte = float(bucket_sum.get("CONTRACTOR_CS", 0.0))
                capacity_fte = delivery_fte + c_fte + cs_fte
                st.write(
                    f"{team_choice} -> TEAM={team_fte:.2f}, DELIVERY={delivery_fte:.2f}, C={c_fte:.2f}, CS={cs_fte:.2f} => capacity={capacity_fte:.2f}"
                )

        if demand_df is not None and not demand_df.empty:
            st.markdown("**Demand rows (derived FTE)**")
            st.dataframe(demand_df.head(200), use_container_width=True)

        debug_payload = {
            "filters": {
                "year": int(selected_year),
                "programs": list(sel_programs or []),
                "teams": list(sel_teams or []),
                "app_groups": list(sel_groups or []),
            },
            "rows": {
                "staffing_raw": int(staffing_df.shape[0]) if isinstance(staffing_df, pd.DataFrame) else 0,
                "demand_raw": int(demand_df.shape[0]) if isinstance(demand_df, pd.DataFrame) else 0,
                "merged_rows": int(merged.shape[0]) if isinstance(merged, pd.DataFrame) else 0,
            },
        }
        if st.button("Copy debug as JSON", key="debug_staffing_vs_demand_json"):
            st.json(debug_payload)

_set_loading("Finalizing view...", 5)
global_loading_ph.empty()
show_perf_panel(page_perf_key)
