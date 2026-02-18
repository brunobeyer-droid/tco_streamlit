# Summary: update source label colors to NEXT branding.
from __future__ import annotations

import datetime as dt
import html
import json
import math
from typing import Optional

import pandas as pd
import streamlit as st
from zoneinfo import ZoneInfo
from core.cache_utils import cache_data_portfolio

# Optional JS support for ECharts tooltips
try:
    from streamlit_echarts import JsCode  # type: ignore

except Exception:  # pragma: no cover
    JsCode = None  # type: ignore

# Cost data on this page is sourced via the canonical cost API (`core/canonical_costs.py`).
from core.canonical_costs import get_pi_costs as _get_pi_costs_raw
from core.ado_recon import load_explorer_feature_rows, load_velocity_baseline
from core.cost_model import get_cost_model_warning, load_cost_model
from core.name_resolution import apply_display_scope_names
from core.data import (
    attach_pi_period_columns,
    compute_ado_coverage,
    fetch_ado_features,
    fetch_apptio_actuals_monthly_breakdown,
    fetch_filter_options,
    fetch_pi_calendar_resolved_dates,
    _program_ids_for_names,
)
from core.capacity_data import fetch_capacity_demand_pi
from core.init import init_page, page_loader, render_echart
from core.debug import is_debug_enabled
from core.perf import (
    filters_signature,
    get_portfolio_cache_buster,
    mark_first_kpi_render,
    perf_step,
    show_perf_panel,
    user_scope_signature,
)
from core.insights_headlines import compute_headline
from core.program_maturity import (
    ProgramMaturityBundle,
    build_maturity_risk_signals,
    calculate_program_maturity,
    calculate_program_maturity_trend,
)
from core.program_strategic_maturity import (
    build_program_strategic_drivers,
    build_strategic_maturity_bundle,
    calculate_program_strategic_maturity,
)
from core.insights_cost_scope import (
    SHOW_SCOPE_OPTIONS,
    build_initiative_feature_detail_df,
    build_initiative_plot_df,
    resolve_show_default,
    rollup_scope_plot_df,
)
from core.echarts_quadrant_theme import build_quadrant_theme
from core.portfolio_signals import (
    build_delivery_signals,
    build_financial_signals,
    build_signal_summary,
    make_signal,
    rank_signals,
)
from core.scope import infer_role, read_scope_from_session, scope_label
from core.ui import render_headline_with_help, render_page_header, touch_last_updated_status
from core.nwf_program import get_program_nwf_actuals_monthly, get_program_nwf_actuals_monthly_breakdown
from core.velocity_baseline import select_velocity_snapshot
from welcome.layout import APP_COLORS
from utils.theme import _merged_theme, echarts_semantic_colors
from utils.ui_patterns import render_active_filters_summary
from utils.finops_kpi_cards import build_finops_kpi_card_html

from db import get_data_version, list_programs, list_application_groups, list_teams  # type: ignore
from db import fetch_df_active as _fetch_df_raw

from utils.app_shell import bootstrap_page
bootstrap_page()


def fetch_df(sql, params=None):  # type: ignore[override]
    df = _fetch_df_raw(sql, params)
    return apply_display_scope_names(df) if isinstance(df, pd.DataFrame) else df


_PI_COSTS_MEMO: dict = {}


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
try:
    from utils.auth import ensure_sso
except Exception:
    st.error("Authentication is not available. Contact an admin.")
    st.stop()

page_theme = init_page("Insights", page_path=__file__)
STRATEGIC_QUADRANT_DISPLAY = {
    "Invest": "Grow",
    "Optimize": "Scale",
    "Monitor": "Watch",
    "Reassess": "Stop",
}
STRATEGIC_QUADRANT_COLORS = {
    "Invest": "#22c55e",
    "Optimize": "#f59e0b",
    "Monitor": "#3b82f6",
    "Reassess": "#ef4444",
}


def _forecast_ytd_from_cost_base(df: pd.DataFrame, *, year: int) -> Optional[float]:
    """Forecast cost basis (YTD) from a unified cost dataframe."""
    if df is None or df.empty:
        return None
    work = df.copy()
    work["AMOUNT"] = pd.to_numeric(work.get("AMOUNT"), errors="coerce").fillna(0.0)
    if work.empty:
        return 0.0
    year = int(year)
    today = pd.Timestamp.today().normalize()
    if year == dt.date.today().year and "PERIOD_END_DATE" in work.columns:
        work["PERIOD_END_DATE"] = pd.to_datetime(work.get("PERIOD_END_DATE"), errors="coerce")
        if work["PERIOD_END_DATE"].notna().any():
            work = work[work["PERIOD_END_DATE"] <= today]
    return float(work["AMOUNT"].sum())


def _pi_to_num(val) -> Optional[int]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    digits = "".join(ch for ch in str(val) if ch.isdigit())
    return int(digits) if digits else None


def _benefit_for_bv(val: float):
    benefit_unit = 1000.0
    if val <= 10:
        return 0.0 * benefit_unit, 10.0 * benefit_unit, 5.0 * benefit_unit, 10
    if val <= 20:
        return 11.0 * benefit_unit, 20.0 * benefit_unit, 15.5 * benefit_unit, 20
    if val <= 30:
        return 21.0 * benefit_unit, 50.0 * benefit_unit, 35.5 * benefit_unit, 30
    if val <= 50:
        return 51.0 * benefit_unit, 100.0 * benefit_unit, 75.5 * benefit_unit, 50
    if val <= 80:
        return 101.0 * benefit_unit, 250.0 * benefit_unit, 175.5 * benefit_unit, 80
    if val <= 130:
        return 251.0 * benefit_unit, 500.0 * benefit_unit, 375.5 * benefit_unit, 130
    return 500.0 * benefit_unit, None, 500.0 * benefit_unit, 200


def _safe_float(val, default: float = 0.0) -> float:
    try:
        if pd.isna(val):
            return default
        out = float(val)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _safe_div(num: float, den: float) -> Optional[float]:
    if den in (None, 0):
        return None
    return num / den


def _safe_label(val, fallback: str = "(Needs mapping)") -> str:
    if val is None:
        return fallback
    try:
        if pd.isna(val):
            return fallback
    except Exception:
        pass
    s = str(val).strip()
    if s in {"", "nan", "None", "NaN"}:
        return fallback
    return s


def _assign_quadrant_label(demand_val: float, value_val: float, x_split: float, y_split: float) -> str:
    if demand_val < x_split and value_val >= y_split:
        return "Invest"
    if demand_val >= x_split and value_val >= y_split:
        return "Optimize"
    if demand_val < x_split and value_val < y_split:
        return "Monitor"
    return "Reassess"


def _build_app_group_positioning_df(plot_df: pd.DataFrame, x_split: float, y_split: float) -> pd.DataFrame:
    cols = ["PROGRAMNAME", "GROUPNAME", "DEMAND_AXIS", "Y_VALUE_PLOT", "ACTUAL_COST", "PROJECTED_SPEND", "quadrant_label"]
    if plot_df is None or plot_df.empty:
        return pd.DataFrame(columns=cols)
    need = {"PROGRAMNAME", "GROUPNAME", "DEMAND_AXIS", "Y_VALUE_PLOT", "ACTUAL_COST"}
    if not need.issubset(set(plot_df.columns)):
        return pd.DataFrame(columns=cols)
    w = plot_df.copy()
    w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    w["GROUPNAME"] = w.get("GROUPNAME", "").fillna("").astype(str).str.strip()
    w["DEMAND_AXIS"] = pd.to_numeric(w.get("DEMAND_AXIS"), errors="coerce")
    w["Y_VALUE_PLOT"] = pd.to_numeric(w.get("Y_VALUE_PLOT"), errors="coerce")
    w["ACTUAL_COST"] = pd.to_numeric(w.get("ACTUAL_COST"), errors="coerce")
    # Cost-vs-Value plotting cost is the projected planning basis for Insights.
    # Strategic maturity consumes it as PROJECTED_SPEND to make spend basis explicit.
    w["PROJECTED_SPEND"] = pd.to_numeric(w.get("ACTUAL_COST"), errors="coerce")
    w = w.dropna(subset=["DEMAND_AXIS", "Y_VALUE_PLOT", "ACTUAL_COST"]).copy()
    w = w[w["PROGRAMNAME"].ne("") & w["GROUPNAME"].ne("") & (w["ACTUAL_COST"] > 0)].copy()
    if w.empty:
        return pd.DataFrame(columns=cols)
    w["quadrant_label"] = w.apply(
        lambda r: _assign_quadrant_label(
            float(r["DEMAND_AXIS"]),
            float(r["Y_VALUE_PLOT"]),
            float(x_split),
            float(y_split),
        ),
        axis=1,
    )
    return w[cols].reset_index(drop=True)


def _build_feature_work_df(df_explorer: pd.DataFrame) -> pd.DataFrame:
    cols = ["PROGRAMNAME", "TEAMNAME", "IS_MSP_FEATURE", "INVESTMENT_DIMENSION", "DEMAND_FTE"]
    if df_explorer is None or df_explorer.empty:
        return pd.DataFrame(columns=cols)
    w = df_explorer.copy()
    if "PROGRAMNAME" not in w.columns:
        return pd.DataFrame(columns=cols)
    w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    w["TEAMNAME"] = w.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    if "DERIVED_FTE_EXPLORER" in w.columns:
        w["DEMAND_FTE"] = pd.to_numeric(w.get("DERIVED_FTE_EXPLORER"), errors="coerce").fillna(0.0)
    else:
        w["DEMAND_FTE"] = pd.to_numeric(w.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
    w["IS_MSP_FEATURE"] = pd.to_numeric(w.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int)
    w["INVESTMENT_DIMENSION"] = w.get("INVESTMENT_DIMENSION", "").fillna("").astype(str).str.strip()
    w = w[w["PROGRAMNAME"].ne("") & (w["DEMAND_FTE"] >= 0)].copy()
    if w.empty:
        return pd.DataFrame(columns=cols)
    return w[cols].reset_index(drop=True)


def _build_app_plot_df_for_quadrant(
    df_ado_src: pd.DataFrame,
    df_cost_src: pd.DataFrame,
    df_explorer_src: pd.DataFrame,
    *,
    benefit_mode: str,
) -> pd.DataFrame:
    cols = [
        "GROUPNAME",
        "TEAMNAME",
        "PROGRAMNAME",
        "IS_BASE",
        "FEATURE_COUNT",
        "BENEFITS_SELECTED",
        "ACTUAL_COST",
        "DERIVED_FTE",
        "DEMAND_AXIS",
        "Y_VALUE_PLOT",
    ]
    if not isinstance(df_ado_src, pd.DataFrame) or df_ado_src.empty:
        return pd.DataFrame(columns=cols)
    if not isinstance(df_cost_src, pd.DataFrame) or df_cost_src.empty:
        return pd.DataFrame(columns=cols)

    ado_raw = df_ado_src.copy()
    ado_raw["BUSINESS_VALUE"] = pd.to_numeric(ado_raw.get("BUSINESS_VALUE"), errors="coerce")
    if "IS_BASE" not in ado_raw.columns:
        ado_raw["IS_BASE"] = 0
    ado_raw["IS_BASE"] = pd.to_numeric(ado_raw.get("IS_BASE"), errors="coerce").fillna(0).astype(int)
    benefits = ado_raw["BUSINESS_VALUE"].fillna(0.0).apply(_benefit_for_bv)
    ado_raw["BENEFIT_LOW"] = benefits.apply(lambda x: x[0])
    ado_raw["BENEFIT_HIGH"] = benefits.apply(lambda x: x[1])
    ado_raw["BENEFIT_MID"] = benefits.apply(lambda x: x[2])
    ado_raw["BENEFIT_SELECTED"] = _select_benefit_series(ado_raw, benefit_mode)
    ado_raw["GROUPNAME"] = ado_raw.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    ado_raw["TEAMNAME"] = ado_raw.get("TEAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    ado_raw["PROGRAMNAME"] = ado_raw.get("PROGRAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    ado_raw.loc[ado_raw["GROUPNAME"].eq(""), "GROUPNAME"] = "(Unassigned)"

    cost_raw = df_cost_src.copy()
    cost_raw["SOURCE"] = cost_raw.get("SOURCE", pd.Series(dtype=str)).fillna("").astype(str).str.strip().str.upper()
    cost_raw = cost_raw[cost_raw["SOURCE"].ne("PROGRAM_ADDITIONAL")].copy()
    amount_col = "TOTAL_COST" if "TOTAL_COST" in cost_raw.columns else "AMOUNT"
    cost_raw["AMOUNT"] = pd.to_numeric(cost_raw.get(amount_col), errors="coerce").fillna(0.0)
    cost_raw["PROGRAMNAME"] = cost_raw.get("PROGRAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    cost_raw["TEAMNAME"] = cost_raw.get("TEAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    cost_raw["GROUPNAME"] = cost_raw.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()

    unassigned_tokens = {"", "unassigned", "(unassigned)", "(blank)", "blank", "none", "null", "nan", "n/a", "na"}
    ado_curr = ado_raw[~ado_raw["GROUPNAME"].str.lower().isin(unassigned_tokens)].copy()
    cost_curr = cost_raw[~cost_raw["GROUPNAME"].str.lower().isin(unassigned_tokens)].copy()
    if ado_curr.empty or cost_curr.empty:
        return pd.DataFrame(columns=cols)

    group_agg = ado_curr.groupby("GROUPNAME", dropna=False).agg(
        TEAMNAME=("TEAMNAME", lambda s: _safe_label(s.dropna().iloc[0]) if not s.dropna().empty else "Unassigned"),
        PROGRAMNAME=("PROGRAMNAME", lambda s: _safe_label(s.dropna().iloc[0]) if not s.dropna().empty else "Unassigned"),
        IS_BASE=("IS_BASE", "max"),
        FEATURE_COUNT=("FEATURE_ID", "count"),
        BENEFITS_SELECTED=("BENEFIT_SELECTED", "sum"),
    ).reset_index()
    cost_group = cost_curr.groupby("GROUPNAME", dropna=False)["AMOUNT"].sum().reset_index()
    plot_df = group_agg.merge(cost_group, on="GROUPNAME", how="left").rename(columns={"AMOUNT": "ACTUAL_COST"})

    plot_df["DERIVED_FTE"] = 0.0
    if isinstance(df_explorer_src, pd.DataFrame) and not df_explorer_src.empty and "GROUPNAME" in df_explorer_src.columns:
        tmp = df_explorer_src.copy()
        tmp["GROUPNAME"] = tmp.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        fte_col = "DERIVED_FTE" if "DERIVED_FTE" in tmp.columns else "DERIVED_FTE_EXPLORER"
        tmp["DERIVED_FTE"] = pd.to_numeric(tmp.get(fte_col), errors="coerce").fillna(0.0)
        fte_by_group = tmp.groupby("GROUPNAME", dropna=False)["DERIVED_FTE"].sum().reset_index()
        plot_df = plot_df.merge(fte_by_group, on="GROUPNAME", how="left", suffixes=("", "_EXPL"))
        plot_df["DERIVED_FTE"] = pd.to_numeric(
            plot_df.get("DERIVED_FTE_EXPL", pd.Series(dtype=float)), errors="coerce"
        ).fillna(0.0)
        plot_df = plot_df.drop(columns=["DERIVED_FTE_EXPL"], errors="ignore")

    plot_df["DEMAND_AXIS"] = pd.to_numeric(plot_df.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
    plot_df["Y_VALUE_PLOT"] = plot_df.apply(
        lambda r: _safe_div(_safe_float(r.get("BENEFITS_SELECTED")), _safe_float(r.get("ACTUAL_COST"))),
        axis=1,
    )
    plot_df = plot_df.dropna(subset=["DEMAND_AXIS", "Y_VALUE_PLOT", "ACTUAL_COST"])
    plot_df = plot_df[(plot_df["DEMAND_AXIS"] > 0) & (plot_df["Y_VALUE_PLOT"] > 0) & (plot_df["ACTUAL_COST"] > 0)].copy()
    for c in cols:
        if c not in plot_df.columns:
            plot_df[c] = pd.NA
    return plot_df[cols].copy()


def _fmt_money(val: Optional[float], digits: int = 0) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "—"
    return f"${val:,.{digits}f}"


def _fmt_pct(val: Optional[float], digits: int = 1) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "—"
    return f"{val * 100:.{digits}f}%"


def _benefit_mode_key(mode: str) -> str:
    low = mode.lower()
    if "low" in low:
        return "low"
    if "high" in low:
        return "high"
    return "mid"


def _select_benefit_series(df: pd.DataFrame, mode: str) -> pd.Series:
    key = _benefit_mode_key(mode)
    if key == "low":
        return df.get("BENEFIT_LOW", pd.Series(dtype=float))
    if key == "high":
        return df.get("BENEFIT_HIGH", pd.Series(dtype=float))
    return df.get("BENEFIT_MID", pd.Series(dtype=float))


def _shade_color(hex_color: str, factor: float) -> str:
    color = hex_color.lstrip("#")
    if len(color) != 6:
        return hex_color
    r = int(color[0:2], 16)
    g = int(color[2:4], 16)
    b = int(color[4:6], 16)
    r = max(0, min(255, int(r * factor)))
    g = max(0, min(255, int(g * factor)))
    b = max(0, min(255, int(b * factor)))
    return f"#{r:02x}{g:02x}{b:02x}"


def _nice_log_min(val: Optional[float]) -> Optional[float]:
    if val is None or val <= 0:
        return val
    return 10 ** math.floor(math.log10(val))


def _nice_log_max(val: Optional[float]) -> Optional[float]:
    if val is None or val <= 0:
        return val
    exp = math.floor(math.log10(val))
    base = 10 ** exp
    for m in (1, 2, 5, 10):
        if val <= m * base:
            return m * base
    return 10 * base


def _nice_linear_max(val: Optional[float], *, min_value: float = 1.0, padding: float = 0.05) -> float:
    v = _safe_float(val, 0.0)
    if v <= 0:
        return float(min_value)
    return max(float(min_value), float(v) * (1.0 + float(padding)))


def _strategic_display_shares(row: pd.Series) -> dict[str, float]:
    return {
        "Grow": _safe_float(row.get("invest_pct"), 0.0),
        "Scale": _safe_float(row.get("optimize_pct"), 0.0),
        "Watch": _safe_float(row.get("monitor_pct"), 0.0),
        "Stop": _safe_float(row.get("reassess_pct"), 0.0),
    }


def _adaptive_target_split(values: pd.Series, *, median_value: float, target_value: float) -> float:
    """Use target split only when it meaningfully separates points; else fallback to median."""
    try:
        arr = pd.to_numeric(values, errors="coerce").dropna()
    except Exception:
        return _safe_float(median_value, 0.0)
    if arr.empty:
        return _safe_float(median_value, 0.0)
    target = _safe_float(target_value, _safe_float(median_value, 0.0))
    below_share = float((arr < target).mean()) if len(arr.index) else 0.0
    if 0.15 <= below_share <= 0.85:
        return target
    return _safe_float(median_value, 0.0)


def _compute_quadrant_params(
    plot_df: pd.DataFrame,
    *,
    split_mode: str,
    split_percentile: int,
    roi_target_active: float,
    use_log: bool = True,
) -> dict[str, float]:
    out = {
        "axis_x_min": 1.0 if use_log else 0.0,
        "axis_x_max": 1.0,
        "axis_y_min": 1.0 if use_log else 0.0,
        "axis_y_max": 1.0,
        "x_split": 0.5,
        "y_split": 0.5,
        "x_ratio": 0.5,
        "y_ratio": 0.5,
    }
    if plot_df is None or plot_df.empty:
        return out
    split_df = plot_df.copy()
    if "IS_BASE" in split_df.columns:
        non_base = split_df[split_df["IS_BASE"].fillna(0).astype(int) == 0]
        split_df = non_base if not non_base.empty else split_df

    eff_vals = pd.to_numeric(split_df.get("DEMAND_AXIS"), errors="coerce").dropna()
    y_vals = pd.to_numeric(split_df.get("Y_VALUE_PLOT"), errors="coerce").dropna()
    if eff_vals.empty or y_vals.empty:
        return out

    x_med = _safe_float(eff_vals.median(), 0.0)
    y_med = _safe_float(y_vals.median(), 0.0)
    x_min = _safe_float(eff_vals.min(), 0.0)
    x_max = _safe_float(eff_vals.max(), 0.0)
    y_min = _safe_float(y_vals.min(), 0.0)
    y_max = _safe_float(y_vals.max(), 0.0)
    y_min = roi_target_active if y_min <= 0 else min(y_min, roi_target_active)
    y_max = max(y_max, roi_target_active)

    axis_x_min, axis_x_max = x_min, x_max
    axis_y_min, axis_y_max = y_min, y_max
    if use_log:
        axis_x_min = _nice_log_min(axis_x_min)
        axis_x_max = _nice_log_max(axis_x_max)
        axis_y_min = _nice_log_min(axis_y_min)
        axis_y_max = _nice_log_max(axis_y_max)

    equal_quadrants = split_mode == "Balanced (equal-size)"
    x_split = x_med
    y_split = y_med
    if equal_quadrants and axis_x_min is not None and axis_x_max is not None:
        x_split = (
            math.sqrt(axis_x_min * axis_x_max)
            if use_log and axis_x_min > 0 and axis_x_max > 0
            else (axis_x_min + axis_x_max) / 2.0
        )
    if equal_quadrants and axis_y_min is not None and axis_y_max is not None:
        y_split = (
            math.sqrt(axis_y_min * axis_y_max)
            if use_log and axis_y_min > 0 and axis_y_max > 0
            else (axis_y_min + axis_y_max) / 2.0
        )
    if not equal_quadrants:
        q = float(max(30, min(70, int(split_percentile)))) / 100.0
        x_split = _safe_float(eff_vals.quantile(q), x_med)
        y_split = _safe_float(y_vals.quantile(q), y_med)
        y_split = _adaptive_target_split(y_vals, median_value=y_med, target_value=roi_target_active)

    if use_log and _safe_float(axis_x_min, 0.0) > 0 and _safe_float(axis_x_max, 0.0) > _safe_float(axis_x_min, 0.0):
        x_ratio = (
            (math.log(max(_safe_float(x_split, 1e-9), 1e-9)) - math.log(max(_safe_float(axis_x_min, 1e-9), 1e-9)))
            / (math.log(max(_safe_float(axis_x_max, 1e-9), 1e-9)) - math.log(max(_safe_float(axis_x_min, 1e-9), 1e-9)) + 1e-9)
        )
    else:
        x_ratio = (_safe_float(x_split, 0.0) - _safe_float(axis_x_min, 0.0)) / (
            _safe_float(axis_x_max, 1.0) - _safe_float(axis_x_min, 0.0) + 1e-9
        )
    if use_log and _safe_float(axis_y_min, 0.0) > 0 and _safe_float(axis_y_max, 0.0) > _safe_float(axis_y_min, 0.0):
        y_ratio = (
            (math.log(max(_safe_float(y_split, 1e-9), 1e-9)) - math.log(max(_safe_float(axis_y_min, 1e-9), 1e-9)))
            / (math.log(max(_safe_float(axis_y_max, 1e-9), 1e-9)) - math.log(max(_safe_float(axis_y_min, 1e-9), 1e-9)) + 1e-9)
        )
    else:
        y_ratio = (_safe_float(y_split, 0.0) - _safe_float(axis_y_min, 0.0)) / (
            _safe_float(axis_y_max, 1.0) - _safe_float(axis_y_min, 0.0) + 1e-9
        )
    out.update(
        {
            "axis_x_min": _safe_float(axis_x_min, 0.0),
            "axis_x_max": _safe_float(axis_x_max, 1.0),
            "axis_y_min": _safe_float(axis_y_min, 0.0),
            "axis_y_max": _safe_float(axis_y_max, 1.0),
            "x_split": _safe_float(x_split, 0.5),
            "y_split": _safe_float(y_split, 0.5),
            "x_ratio": max(0.05, min(0.95, float(x_ratio))),
            "y_ratio": max(0.05, min(0.95, float(y_ratio))),
        }
    )
    return out


def _stable_key(obj: object) -> str:
    return json.dumps(obj, sort_keys=True, default=str)


@cache_data_portfolio(ttl=120, show_spinner=False)
def cached_get_pi_costs(
    *,
    scenario: str,
    filters_key: str,
    year: int,
    scope_key: str,
    filters_sig: str = "",
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> pd.DataFrame:
    _ = (filters_sig, user_scope_sig, cache_buster)
    filters = json.loads(filters_key)
    scope = json.loads(scope_key)
    cm = load_cost_model(int(year), scope)
    scen = str(scenario or "").strip().upper()
    if scen.startswith("BASELINE") or scen.startswith("BUDGET"):
        src = cm.get("BASELINE", pd.DataFrame())
    elif scen.startswith("ACTUAL"):
        src = cm.get("ACTUAL", pd.DataFrame())
    else:
        src = cm.get("EXPECTED", pd.DataFrame())
    return get_pi_costs(src, scenario=scenario, filters=filters)


@cache_data_portfolio(ttl=120, show_spinner=False)
def cached_load_cost_model(
    *,
    year: int,
    scope_key: str,
    filters_sig: str = "",
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> dict:
    _ = (filters_sig, user_scope_sig, cache_buster)
    scope = json.loads(scope_key)
    return load_cost_model(int(year), scope)


@cache_data_portfolio(ttl=120, show_spinner=False)
def cached_prepare_insights_inputs(
    *,
    year: int,
    programs: tuple[str, ...],
    teams: tuple[str, ...],
    groups: tuple[str, ...],
    data_version: int,
    preview_rev: int,
    filters_cost_key: str,
    scope_key: str,
    filters_sig: str = "",
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> dict:
    _ = (filters_sig, user_scope_sig, cache_buster)
    df_cost_src = cached_get_pi_costs(
        scenario="Projected",
        filters_key=filters_cost_key,
        year=int(year),
        scope_key=scope_key,
        filters_sig=filters_sig,
        user_scope_sig=user_scope_sig,
        cache_buster=cache_buster,
    )
    if df_cost_src is None or df_cost_src.empty:
        df_cost = pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE", "AMOUNT"])
    else:
        w = df_cost_src.copy()
        dims = [c for c in w.columns if c not in {"LABOR_BUCKET", "TOTAL_COST"}]
        w["AMOUNT"] = pd.to_numeric(w.get("TOTAL_COST"), errors="coerce").fillna(0.0)
        df_cost = w.groupby(dims, dropna=False)["AMOUNT"].sum().reset_index()
        if "TEAMNAME" not in df_cost.columns:
            df_cost["TEAMNAME"] = ""
        for c in ["PROGRAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"]:
            if c not in df_cost.columns:
                df_cost[c] = ""
            df_cost[c] = df_cost[c].fillna("").astype(str).str.strip()
        df_cost.loc[df_cost["GROUPNAME"].eq(""), "GROUPNAME"] = "(Unassigned)"
        df_cost = df_cost[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE", "AMOUNT"]]
    df_cost = attach_pi_period_columns(df_cost)

    df_ado = fetch_ado_features(int(year), list(programs), list(teams), list(groups), rev=preview_rev, data_version=data_version)
    if df_ado is None:
        df_ado = pd.DataFrame()
    elif not df_ado.empty:
        df_ado = df_ado.copy()
        df_ado["TEAMNAME"] = df_ado.get("TEAMNAME", "").fillna("").astype(str).str.strip()
        df_ado["GROUPNAME"] = df_ado.get("GROUPNAME", "").fillna("").astype(str).str.strip()
        df_ado.loc[df_ado["GROUPNAME"].eq(""), "GROUPNAME"] = "(Unassigned)"
        if groups:
            df_ado = df_ado[df_ado["GROUPNAME"].isin(set(groups))].copy()

    try:
        # Keep Insights responsive under constrained DB latency:
        # this dataset is used for demand-focused analytics and does not require
        # ADO enrichment joins or velocity-column materialization at this stage.
        df_explorer = load_explorer_feature_rows(
            years=[int(year)],
            programs=tuple(programs),
            teams=tuple(teams),
            groups=tuple(groups),
            pi_nums=tuple(),
            data_version=data_version,
            include_ado_enrichment=False,
            include_velocity_column=False,
        )
    except Exception:
        df_explorer = pd.DataFrame()
    if df_explorer is None:
        df_explorer = pd.DataFrame()

    return {
        "df_cost_src": df_cost_src if df_cost_src is not None else pd.DataFrame(),
        "df_cost": df_cost,
        "df_ado": df_ado,
        "df_explorer": df_explorer,
    }


@cache_data_portfolio(ttl=120, show_spinner=False)
def cached_program_nwf_actuals_monthly(
    *,
    year: int,
    programs: tuple[str, ...],
    lag_months: int,
    filters_sig: str = "",
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> pd.DataFrame:
    _ = (filters_sig, user_scope_sig, cache_buster)
    try:
        raw = get_program_nwf_actuals_monthly_breakdown(
            fetch_df,
            years=[int(year)],
            filters={"program": list(programs or ())},
        )
    except Exception:
        raw = get_program_nwf_actuals_monthly(
            fetch_df,
            years=[int(year)],
            filters={"program": list(programs or ())},
        )
    out = raw if isinstance(raw, pd.DataFrame) else pd.DataFrame()
    if not out.empty and "MONTH_KEY" in out.columns:
        out = out.copy()
        out["MONTH_KEY"] = out["MONTH_KEY"].apply(lambda v: _shift_month_key(v, int(lag_months)))
    return out


INSIGHTS_KPI_EXTRA_LINES = 3


def _metric_card(
    col,
    label: str,
    value: str,
    extra: str = "",
    source: str = "",
    source_color: str = "",
    *,
    chips: Optional[list[tuple[str, str]]] = None,
    extra_lines: Optional[list[str]] = None,
    min_extra_lines: int = 1,
    help_text: Optional[str] = None,
) -> None:
    with col:
        with st.container(border=True):
            try:
                st.metric(label, value, help=help_text)
            except TypeError:
                st.metric(label, value)
            if chips:
                parts = []
                for text, color in chips:
                    safe = html.escape(str(text or "").strip())
                    if not safe:
                        continue
                    c = str(color or "#64748b").strip() or "#64748b"
                    parts.append(
                        "<span style='display:inline-block; padding:2px 8px; border-radius:10px; "
                        f"font-size:11px; background:{c}; color:#fff; margin-right:6px;'>{safe}</span>"
                    )
                if parts:
                    st.markdown("".join(parts), unsafe_allow_html=True)
            elif source:
                safe_source = html.escape(source)
                color = source_color or "#64748b"
                st.markdown(
                    "<span style='display:inline-block; padding:2px 8px; border-radius:10px; "
                    f"font-size:11px; background:{color}; color:#fff;'>{safe_source}</span>",
                    unsafe_allow_html=True,
                )

            if extra_lines is None:
                lines = [extra] if extra else []
            else:
                lines = list(extra_lines or [])
            while len(lines) < INSIGHTS_KPI_EXTRA_LINES:
                lines.append("")
            lines = lines[:INSIGHTS_KPI_EXTRA_LINES]
            for line in lines:
                txt = "" if line is None else str(line)
                if txt.strip():
                    st.caption(txt)
                else:
                    st.markdown("<div style='height:0.9rem'></div>", unsafe_allow_html=True)


def _section_title(title: str, tip: str) -> None:
    st.subheader(title, help=tip or None)


SECTION_DIVIDER_MARGIN = "8px 0 10px 0"


def _render_section_divider() -> None:
    st.markdown(f"<hr style='margin:{SECTION_DIVIDER_MARGIN};'>", unsafe_allow_html=True)


def render_finops_kpi_card(
    title: str,
    value: str,
    *,
    tip: str,
    chips: Optional[list[str]] = None,
    lines: Optional[list[str]] = None,
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
            extra_lines=INSIGHTS_KPI_EXTRA_LINES,
        ),
        unsafe_allow_html=True,
    )


def _nwf_bucket_from_text(val: str) -> str:
    text = str(val or "").strip().lower()
    if not text:
        return "Other"
    if "invoice" in text:
        return "Invoices"
    if "azure" in text:
        return "Cloud Azure"
    if "aws" in text:
        return "Cloud AWS"
    if "msp" in text:
        return "MSP"
    if "contractor" in text and "cs" in text:
        return "Contractor CS"
    if "travel" in text:
        return "Travel"
    return "Other"


def _pi_label_from_base(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=str)
    year = pd.to_numeric(df.get("YEAR"), errors="coerce").fillna(0).astype(int)
    if "PI_NAME" in df.columns:
        pi_name = df.get("PI_NAME", "").fillna("").astype(str).str.strip()
        label = year.astype(str) + " " + pi_name
        return label.where(pi_name.ne(""), "")
    if "PI" in df.columns:
        pi = pd.to_numeric(df.get("PI"), errors="coerce").fillna(0).astype(int)
        label = year.astype(str) + " I" + pi.astype(str)
        return label.where(pi.gt(0), "")
    return pd.Series("", index=df.index, dtype=str)


def _infer_programs_for_selected_teams(base_df: pd.DataFrame, sel_teams: list[str]) -> list[str]:
    if base_df is None or base_df.empty or not sel_teams:
        return []
    if "TEAMNAME" not in base_df.columns or "PROGRAMNAME" not in base_df.columns:
        return []
    mask = base_df["TEAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_teams))
    programs = base_df.loc[mask, "PROGRAMNAME"].dropna().astype(str).str.strip()
    return sorted({p for p in programs.tolist() if p})


def _programs_for_scope(base_df: pd.DataFrame, year: int, sel_programs: list[str], sel_teams: list[str]) -> list[str]:
    if sel_programs:
        return sorted({p for p in sel_programs if str(p).strip()})
    if sel_teams:
        return _infer_programs_for_selected_teams(base_df, sel_teams)
    if base_df is None or base_df.empty or "PROGRAMNAME" not in base_df.columns:
        return []
    base_y = base_df.copy()
    if "YEAR" in base_y.columns:
        base_y["YEAR"] = pd.to_numeric(base_y.get("YEAR"), errors="coerce").astype("Int64")
        base_y = base_y.loc[base_y["YEAR"].eq(int(year))].copy()
    programs = base_y["PROGRAMNAME"].dropna().astype(str).str.strip()
    return sorted({p for p in programs.tolist() if p})


def _fetch_apptio_actuals_breakdown(year: int, program_names: list[str]) -> tuple[pd.DataFrame, dict]:
    debug = {"programs_used": program_names, "program_ids_used": [], "rows": 0}

    def _fallback() -> pd.DataFrame:
        try:
            fb = get_program_nwf_actuals_monthly_breakdown(
                fetch_df,
                years=[int(year)],
                filters={"program": list(program_names or [])},
            )
        except Exception as exc:  # pragma: no cover
            debug["fallback_status"] = f"error:{exc}"
            return pd.DataFrame()
        fb = fb if isinstance(fb, pd.DataFrame) else pd.DataFrame()
        debug["fallback_rows"] = int(fb.shape[0]) if not fb.empty else 0
        debug["fallback_status"] = "ok" if not fb.empty else "no_data"
        return fb

    if not program_names:
        debug["status"] = "no_programs"
        fb = _fallback()
        return fb, debug

    program_ids = _program_ids_for_names(program_names)
    program_ids = [p for p in (program_ids or []) if p]
    debug["program_ids_used"] = program_ids
    if not program_ids:
        debug["status"] = "no_program_ids"
        fb = _fallback()
        return fb, debug
    try:
        df = fetch_apptio_actuals_monthly_breakdown(int(year), program_ids, data_version=data_version)
    except Exception as exc:  # pragma: no cover
        debug["status"] = f"error:{exc}"
        fb = _fallback()
        return fb, debug
    df = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    debug["rows"] = int(df.shape[0]) if not df.empty else 0
    if df.empty:
        debug["status"] = "no_data"
        fb = _fallback()
        return fb, debug
    debug["status"] = "ok"
    return df, debug


def _baseline_weighted_avg(series: pd.Series, weights: pd.Series) -> Optional[float]:
    vals = pd.to_numeric(series, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0)
    mask = vals.notna()
    if not bool(mask.any()):
        return None
    vals = vals.loc[mask].astype(float)
    w = w.loc[mask].astype(float).clip(lower=0.0)
    if bool((w > 0).any()) and float(w.sum()) > 0:
        return float((vals * w).sum() / w.sum())
    return float(vals.mean())


def _baseline_dominant_source(series: pd.Series, weights: pd.Series) -> str:
    src = series.fillna("GLOBAL").astype(str).str.upper().str.strip()
    src = src.where(src.isin({"TEAM", "PROGRAM", "GLOBAL"}), "GLOBAL")
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    rank = {"TEAM": 3, "PROGRAM": 2, "GLOBAL": 1}
    score = (
        pd.DataFrame({"SRC": src, "W": w})
        .groupby("SRC", dropna=False)["W"]
        .sum()
        .reset_index()
    )
    if float(score.get("W", pd.Series(dtype=float)).sum() or 0.0) <= 0:
        score = src.value_counts(dropna=False).rename_axis("SRC").reset_index(name="W")
    if score.empty:
        return "GLOBAL"
    score["RANK"] = score["SRC"].map(rank).fillna(0).astype(int)
    score = score.sort_values(["W", "RANK"], ascending=[False, False])
    top = str(score.iloc[0]["SRC"]).strip().upper()
    return top if top in {"TEAM", "PROGRAM", "GLOBAL"} else "GLOBAL"


def _allow_baseline_signal(role: str, eff_programs: list[str], eff_teams: list[str]) -> bool:
    role_now = str(role or "").strip()
    if role_now == "Product Owner":
        return bool(eff_teams)
    if role_now == "Program Manager":
        return bool(eff_programs)
    selected_programs = [p for p in (eff_programs or []) if str(p).strip()]
    return len(selected_programs) == 1


def _build_baseline_change_signal(
    *,
    year: int,
    role: str,
    eff_programs: list[str],
    eff_teams: list[str],
    eff_groups: list[str],
    data_version: Optional[int],
) -> tuple[Optional[dict], dict]:
    debug: dict = {
        "eligible": False,
        "rows": 0,
        "changed": False,
        "reason": "",
    }
    if not _allow_baseline_signal(role, eff_programs, eff_teams):
        debug["reason"] = "scope_not_eligible"
        return None, debug
    debug["eligible"] = True
    try:
        df_baseline = load_velocity_baseline(
            year=int(year),
            programs=tuple(eff_programs or ()),
            teams=tuple(eff_teams or ()),
            groups=tuple(eff_groups or ()),
            data_version=data_version,
            include_prior_year=True,
            reference_year_only=False,
        )
    except Exception as exc:  # pragma: no cover
        debug["reason"] = f"load_error:{exc}"
        return None, debug
    if df_baseline is None or df_baseline.empty:
        debug["reason"] = "no_data"
        return None, debug
    debug["rows"] = int(df_baseline.shape[0])

    try:
        cal_df = fetch_pi_calendar_resolved_dates(
            years=(int(year),),
            data_version=int(data_version or 0),
        )
    except Exception:
        cal_df = pd.DataFrame()

    snapshot = select_velocity_snapshot(
        df_velocity=df_baseline,
        selected_year=int(year),
        as_of=dt.date.today(),
        programs=list(eff_programs or []),
        teams=list(eff_teams or []),
        calendar_df=cal_df,
    )
    debug["changed"] = bool(snapshot.get("changed"))
    debug["reference_pi_label"] = str(snapshot.get("reference_pi_label") or "")
    debug["selected_pi_label"] = str(snapshot.get("selected_pi_label") or "")
    debug["fallback_chain_used"] = str(snapshot.get("fallback_chain_used") or "")
    debug["selected_source"] = str(snapshot.get("source") or "")
    if not bool(snapshot.get("changed")):
        debug["reason"] = "not_changed"
        return None, debug

    prev_val = snapshot.get("prev")
    new_val = snapshot.get("value")
    if prev_val is None or new_val is None:
        debug["reason"] = "missing_prev_or_new"
        return None, debug
    source = str(snapshot.get("source") or "GLOBAL").upper().strip() or "GLOBAL"
    level = "Team" if bool(eff_teams) else "Program"
    sig = make_signal(
        id="delivery_baseline_updated",
        level=level,
        severity="risk",
        icon="⚠",
        title="Delivery baseline updated",
        text=f"Delivery baseline updated: {float(prev_val):.1f} → {float(new_val):.1f} pts/PI (Source: {source})",
        metric_value=float(new_val),
        metric_unit="pts/PI",
        impact=None,
        action="Watch",
        source="Delivery",
    )
    return sig, debug


def build_key_signals(
    cm: dict,
    cap_df: pd.DataFrame,
    apptio_df: pd.DataFrame,
    *,
    programs_used: list[str],
) -> tuple[list[dict], dict]:
    signals: list[dict] = []
    debug: dict = {"programs_used": programs_used}

    # Signal 1 — Invoices share of NWF (Actual)
    if isinstance(apptio_df, pd.DataFrame) and not apptio_df.empty and "AMOUNT" in apptio_df.columns:
        nwf_type_col = "EFFECTIVE_NWF_TYPE" if "EFFECTIVE_NWF_TYPE" in apptio_df.columns else None
        if nwf_type_col:
            types = apptio_df[nwf_type_col].fillna("").astype(str)
            amt = pd.to_numeric(apptio_df.get("AMOUNT"), errors="coerce").fillna(0.0)
            total = float(amt.sum() or 0.0)
            invoices = float(amt.loc[types.str.contains("invoice", case=False, na=False)].sum() or 0.0)
            debug["invoices_total"] = invoices
            debug["nwf_actual_total"] = total
            if total > 0:
                share = invoices / total * 100.0
                signals.append(
                    {"icon": "↑", "text": f"Invoices represent {share:.0f}% of Non-Workforce spend (Actual)"}
                )

    # Signal 2 — Azure QoQ change (Actual)
    if isinstance(apptio_df, pd.DataFrame) and not apptio_df.empty and "MONTH_KEY" in apptio_df.columns:
        nwf_type_col = "EFFECTIVE_NWF_TYPE" if "EFFECTIVE_NWF_TYPE" in apptio_df.columns else None
        if nwf_type_col:
            w = apptio_df.copy()
            w["MONTH_KEY"] = w["MONTH_KEY"].fillna("").astype(str).str.strip()
            w = w[w["MONTH_KEY"].str.match(r"^\\d{4}-\\d{2}$", na=False)]
            w["YEAR"] = w["MONTH_KEY"].str.slice(0, 4).astype(int)
            w["MONTH"] = w["MONTH_KEY"].str.slice(5, 7).astype(int)
            w["QUARTER"] = ((w["MONTH"] - 1) // 3 + 1).astype(int)
            w["Q_LABEL"] = w["YEAR"].astype(str) + " Q" + w["QUARTER"].astype(str)
            w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)
            w = w[w[nwf_type_col].fillna("").astype(str).str.contains("azure", case=False, na=False)]
            q = w.groupby(["YEAR", "QUARTER", "Q_LABEL"], as_index=False)["AMOUNT"].sum()
            q = q.sort_values(["YEAR", "QUARTER"])
            debug["azure_quarters"] = q.to_dict(orient="records")
            if len(q.index) >= 2:
                latest = q.iloc[-1]
                prev = q.iloc[-2]
                if float(prev["AMOUNT"] or 0.0) > 0:
                    qoq = (float(latest["AMOUNT"]) - float(prev["AMOUNT"])) / float(prev["AMOUNT"]) * 100.0
                    if qoq >= 5:
                        signals.append({"icon": "↑", "text": f"Azure spend increased {qoq:.0f}% QoQ (Actual)"})
                    elif qoq <= -5:
                        signals.append({"icon": "↓", "text": f"Azure spend decreased {abs(qoq):.0f}% QoQ (Actual)"})
                    else:
                        signals.append({"icon": "↔", "text": "Azure spend stable QoQ (Actual)"})

    # Signal 3 — Highest cost per delivered FTE (Forecast/Expected)
    df_expected = cm.get("EXPECTED", pd.DataFrame()) if isinstance(cm, dict) else pd.DataFrame()
    if isinstance(df_expected, pd.DataFrame) and not df_expected.empty and isinstance(cap_df, pd.DataFrame):
        program_col = "PROGRAMNAME" if "PROGRAMNAME" in df_expected.columns else None
        if program_col:
            cost_work = df_expected.copy()
            cost_work["AMOUNT"] = pd.to_numeric(cost_work.get("AMOUNT"), errors="coerce").fillna(0.0)
            if "WF_LAYER2" in cost_work.columns:
                cost_work = cost_work[
                    cost_work["WF_LAYER2"].fillna("").astype(str).str.upper().str.strip().eq("SOD")
                ]
            elif "SOD_LAYER2" in cost_work.columns:
                cost_work = cost_work[
                    cost_work["SOD_LAYER2"].fillna("").astype(str).str.upper().str.strip().eq("SOD")
                ]
            cost_by_prog = cost_work.groupby(program_col)["AMOUNT"].sum()

            delivered_col = None
            for cand in ["DELIVERED_FTE", "DEMAND_SOD_FTE", "DEMAND_FTE"]:
                if cand in cap_df.columns:
                    delivered_col = cand
                    break
            if delivered_col and "PROGRAMNAME" in cap_df.columns:
                fte_by_prog = cap_df.groupby("PROGRAMNAME")[delivered_col].sum()
                ratios = (cost_by_prog / fte_by_prog).dropna()
                ratios = ratios[ratios > 0]
                if not ratios.empty:
                    top_prog = ratios.sort_values(ascending=False).index[0]
                    top_ratio = float(ratios.loc[top_prog])
                    signals.append(
                        {
                            "icon": "⚠",
                            "text": f"{top_prog} has the highest cost per delivered FTE (${top_ratio:,.0f})",
                        }
                    )
                    debug["cost_per_fte"] = ratios.sort_values(ascending=False).head(5).to_dict()

    # Signal 4 — Capacity vs Demand pressure (Forecast/Expected)
    if isinstance(cap_df, pd.DataFrame) and not cap_df.empty:
        cap_col = "CAPACITY_SOD_FTE" if "CAPACITY_SOD_FTE" in cap_df.columns else "CAPACITY_FTE"
        dem_col = "DEMAND_SOD_FTE" if "DEMAND_SOD_FTE" in cap_df.columns else "DEMAND_FTE"
        label_col = "ITERATION_LEVEL3" if "ITERATION_LEVEL3" in cap_df.columns else "PI_LABEL"
        if cap_col in cap_df.columns and dem_col in cap_df.columns and label_col in cap_df.columns:
            agg = cap_df.groupby(label_col)[[cap_col, dem_col]].sum().reset_index()
            if "PI_ORDER" in cap_df.columns:
                pi_order = cap_df.groupby(label_col)["PI_ORDER"].min().reset_index()
                agg = agg.merge(pi_order, on=label_col, how="left").sort_values("PI_ORDER")
            else:
                agg = agg.sort_values(label_col)
            if not agg.empty:
                last = agg.iloc[-1]
                capacity = float(last.get(cap_col) or 0.0)
                demand = float(last.get(dem_col) or 0.0)
                pi_label = str(last.get(label_col))
                if capacity > 0:
                    pressure = (demand - capacity) / capacity * 100.0
                    if pressure >= 5:
                        signals.append({"icon": "⚠", "text": f"Demand exceeds capacity by {pressure:.0f}% in {pi_label}"})
                    elif pressure <= -5:
                        signals.append({"icon": "↔", "text": f"Capacity exceeds demand by {abs(pressure):.0f}% in {pi_label}"})
                    else:
                        signals.append({"icon": "↔", "text": f"Demand and capacity are balanced in {pi_label}"})
                    debug["pressure_last_pi"] = {"pi": pi_label, "capacity": capacity, "demand": demand}

    # Signal 5 — Contractor mix shift since last PI (Forecast/Expected)
    if isinstance(cap_df, pd.DataFrame) and not cap_df.empty:
        c_col = None
        d_col = None
        for col in cap_df.columns:
            low = col.lower()
            if "contractor" in low and "fte" in low and ("cap" in low or "capacity" in low):
                if "cs" not in low:
                    c_col = c_col or col
            if "delivery" in low and "fte" in low and ("cap" in low or "capacity" in low):
                d_col = d_col or col
        label_col = "ITERATION_LEVEL3" if "ITERATION_LEVEL3" in cap_df.columns else "PI_LABEL"
        if c_col and d_col and label_col in cap_df.columns:
            agg = cap_df.groupby(label_col)[[c_col, d_col]].sum().reset_index()
            if "PI_ORDER" in cap_df.columns:
                pi_order = cap_df.groupby(label_col)["PI_ORDER"].min().reset_index()
                agg = agg.merge(pi_order, on=label_col, how="left").sort_values("PI_ORDER")
            else:
                agg = agg.sort_values(label_col)
            if len(agg.index) >= 2:
                prev = agg.iloc[-2]
                last = agg.iloc[-1]
                prev_total = float(prev.get(c_col) or 0.0) + float(prev.get(d_col) or 0.0)
                last_total = float(last.get(c_col) or 0.0) + float(last.get(d_col) or 0.0)
                if prev_total > 0 and last_total > 0:
                    prev_share = float(prev.get(c_col) or 0.0) / prev_total
                    last_share = float(last.get(c_col) or 0.0) / last_total
                    delta_pp = (last_share - prev_share) * 100.0
                    if abs(delta_pp) >= 3:
                        if delta_pp > 0:
                            signals.append({"icon": "↑", "text": f"Contractor share of SoD increased by {delta_pp:.0f} pp since last PI"})
                        else:
                            signals.append({"icon": "↓", "text": f"Contractor share of SoD decreased by {abs(delta_pp):.0f} pp since last PI"})
                    else:
                        signals.append({"icon": "↔", "text": "Contractor share of SoD unchanged since last PI"})
                    debug["contractor_mix"] = {"prev_share": prev_share, "last_share": last_share}

    return signals[:6], debug


def build_fallback_scope_signals(
    *,
    projected_vs_baseline_pct: Optional[float],
    nwf_plan_vs_actual_pct: Optional[float],
    swag_ready_cov: Optional[float],
    bv_cov_required: Optional[float],
) -> list[dict]:
    """Fallback insights when detailed signal sources are sparse for the selected scope."""
    out: list[dict] = []
    if projected_vs_baseline_pct is not None:
        pct = float(projected_vs_baseline_pct) * 100.0
        if pct >= 5.0:
            out.append({"icon": "↑", "text": f"Projected cost is {pct:.0f}% above baseline for the selected scope"})
        elif pct <= -5.0:
            out.append({"icon": "↓", "text": f"Projected cost is {abs(pct):.0f}% below baseline for the selected scope"})
        else:
            out.append({"icon": "↔", "text": "Projected cost is broadly in line with baseline"})
    if nwf_plan_vs_actual_pct is not None:
        pct = float(nwf_plan_vs_actual_pct) * 100.0
        if pct >= 5.0:
            out.append({"icon": "⚠", "text": f"Program NWF actuals are {pct:.0f}% above plan"})
        elif pct <= -5.0:
            out.append({"icon": "↓", "text": f"Program NWF actuals are {abs(pct):.0f}% below plan"})
        else:
            out.append({"icon": "↔", "text": "Program NWF actuals are close to plan"})
    if swag_ready_cov is not None:
        pct = float(swag_ready_cov) * 100.0
        if pct < 70.0:
            out.append({"icon": "⚠", "text": f"Only {pct:.0f}% of features are SWAG-ready (demand confidence risk)"})
        else:
            out.append({"icon": "↑", "text": f"{pct:.0f}% of features are SWAG-ready"})
    if bv_cov_required is not None:
        pct = float(bv_cov_required) * 100.0
        if pct < 80.0:
            out.append({"icon": "⚠", "text": f"Business Value coverage is {pct:.0f}% for non-BASE scope"})
        else:
            out.append({"icon": "↑", "text": f"Business Value coverage is {pct:.0f}% for non-BASE scope"})
    return out[:6]


def _fallback_items_to_structured_signals(
    items: list[dict],
    *,
    eff_teams: Optional[list[str]] = None,
    eff_groups: Optional[list[str]] = None,
) -> list[dict]:
    out: list[dict] = []
    default_level = "Application" if bool(eff_groups) else ("Team" if bool(eff_teams) else "Program")
    for idx, item in enumerate(items or []):
        icon = str(item.get("icon") or "•").strip() or "•"
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        if icon in {"⚠"}:
            severity = "risk"
            action = "Investigate"
            title = "Attention needed"
        elif icon in {"↑", "↓"}:
            severity = "watch"
            action = "Watch"
            title = "Trend change"
        else:
            severity = "info"
            action = "No action"
            title = "Scope signal"
        is_nwf_fallback = "nwf actuals" in text.lower()
        level = "Program" if is_nwf_fallback else default_level
        source = "financial" if is_nwf_fallback else "delivery"
        out.append(
            make_signal(
                id=f"fallback_scope_{idx + 1}",
                level=level,
                severity=severity,
                icon=icon,
                title=title,
                text=text,
                metric_value=None,
                metric_unit=None,
                impact=None,
                action=action,
                source=source,
            )
        )
    return out[:6]


def render_signals_hub(
    signals: list[dict],
    *,
    scope,
    role: str,
    theme: dict,
    financial_available: bool,
    eff_programs: list[str],
    eff_teams: list[str],
    eff_groups: list[str],
    debug_meta: Optional[dict] = None,
) -> None:
    _ = scope, eff_programs
    is_team_scope = bool(eff_teams)
    is_group_scope = bool(eff_groups)
    scoped_delivery_view = bool(is_team_scope or is_group_scope)
    sem = echarts_semantic_colors(theme)
    ranked = rank_signals(signals)
    if scoped_delivery_view:
        def _delivery_first_key(sig: dict) -> tuple[int, int, float, str]:
            source = str(sig.get("source") or "").strip().lower()
            severity = str(sig.get("severity") or "info").strip().lower()
            if source == "delivery" and severity == "risk":
                bucket = 0
            elif source == "delivery" and severity == "watch":
                bucket = 1
            elif source == "financial" and severity == "risk":
                bucket = 2
            elif source == "financial" and severity == "watch":
                bucket = 3
            else:
                bucket = 4
            impact = float(sig.get("impact") or 0.0) if sig.get("impact") is not None else 0.0
            title = str(sig.get("title") or "").strip().lower()
            return (bucket, 0 if sig.get("impact") is not None else 1, -impact, title)

        ranked = sorted(ranked, key=_delivery_first_key)
    summary = build_signal_summary(ranked)
    top_rows: list[dict[str, str]] = []
    for sig in ranked[:10]:
        source = str(sig.get("source") or "").strip().lower()
        source_label = "Financial" if source == "financial" else "Delivery"
        level_label = "Program" if source_label == "Financial" else str(sig.get("level") or "Program")
        impact = sig.get("impact")
        impact_txt = f"${float(impact):,.0f}" if impact is not None else "-"
        title = str(sig.get("title") or "").strip()
        text = str(sig.get("text") or "").strip()
        top_rows.append(
            {
                "LEVEL": level_label,
                "SOURCE": source_label,
                "SIGNAL": f"{title}: {text}" if title else text,
                "IMPACT": impact_txt,
                "ACTION": str(sig.get("action") or "No action"),
            }
        )
    top_df = pd.DataFrame(top_rows, columns=["LEVEL", "SOURCE", "SIGNAL", "IMPACT", "ACTION"])

    st.markdown("<div id='insights-signals-hub-anchor'></div>", unsafe_allow_html=True)
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"]:has(#insights-signals-hub-anchor) .signals-hub-mini {
            height: 138px !important;
            min-height: 138px !important;
        }
        [data-testid="stAppViewContainer"]:has(#insights-signals-hub-anchor) .signals-hub-mini .finops-kpi-value {
            font-size: 1.25rem !important;
        }
        [data-testid="stAppViewContainer"]:has(#insights-signals-hub-anchor) .signals-hub-mini .finops-kpi-lines {
            font-size: 0.66rem !important;
        }
        [data-testid="stAppViewContainer"]:has(#insights-signals-hub-anchor) .signals-hub-chip-wrap {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            margin: 4px 0 8px 0;
        }
        [data-testid="stAppViewContainer"]:has(#insights-signals-hub-anchor) .signals-hub-chip {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            border-radius: 999px;
            border: 1px solid rgba(148, 163, 184, 0.35);
            padding: 3px 9px;
            font-size: 0.72rem;
            color: rgba(226, 232, 240, 0.95);
            background: rgba(15, 23, 42, 0.22);
            max-width: 100%;
        }
        [data-testid="stAppViewContainer"]:has(#insights-signals-hub-anchor) .signals-hub-chip-badge {
            display: inline-block;
            border-radius: 999px;
            padding: 1px 7px;
            font-size: 0.62rem;
            letter-spacing: 0.02em;
        }
        [data-testid="stAppViewContainer"]:has(#insights-signals-hub-anchor) .signals-hub-chip-level {
            color: rgba(148, 163, 184, 0.95);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    card1_title = str(summary.get("apps_title") or "Delivery risks")
    card1_value = int(summary.get("apps_value") or 0)
    card2_value = int(summary.get("program_financial_risk") or 0)
    card3_value = int(summary.get("delivery_pressure") or 0)
    top_driver = str(summary.get("top_driver") or "No active risk").strip() or "No active risk"
    top_sig = summary.get("top_signal") or {}

    row = st.columns(4, gap="small")
    cards = [
        {
            "title": card1_title,
            "value": str(card1_value),
            "tip": "Count of application risks; when unavailable, falls back to delivery risks.",
            "accent": sem.get("risk", "#f97316"),
            "tone": "risk" if card1_value > 0 else "neutral",
            "icon": "warning",
            "lines": ["Severity: risk", "Scope-aligned"],
            "spark": [0.2, 0.35, 0.42, 0.31, 0.55, 0.63, 0.52, 0.74],
        },
        {
            "title": "Program financial (Apptio)" if scoped_delivery_view else "Programs at financial risk",
            "value": str(card2_value),
            "tip": "Financial signals from Apptio Actuals, Program level only.",
            "accent": sem.get("actual", "#f59e0b"),
            "tone": "risk" if card2_value > 0 else "neutral",
            "icon": "receipt_long",
            "lines": [
                "Apptio Actuals",
                "Program total (not team/app allocated)" if scoped_delivery_view else "Program-level only",
            ],
            "spark": [0.26, 0.33, 0.48, 0.4, 0.52, 0.61, 0.57, 0.64],
        },
        {
            "title": "Delivery pressure",
            "value": str(card3_value),
            "tip": "Capacity vs demand pressure signals in the selected scope.",
            "accent": sem.get("forecast", "#84cc16"),
            "tone": "watch" if card3_value > 0 else "neutral",
            "icon": "monitoring",
            "lines": ["Capacity vs demand", "Latest PI pressure"],
            "spark": [0.22, 0.28, 0.34, 0.43, 0.46, 0.58, 0.62, 0.69],
        },
        {
            "title": "Top driver",
            "value": top_driver,
            "tip": "Highest-severity signal in current scope.",
            "accent": sem.get("baseline", "#3b82f6"),
            "tone": "risk" if str(top_sig.get("severity") or "").lower() == "risk" else "watch",
            "icon": "insights",
            "lines": [
                str(top_sig.get("text") or "No active risk signal"),
                f"Action: {str(top_sig.get('action') or 'No action')}",
            ],
            "spark": [0.19, 0.25, 0.31, 0.37, 0.45, 0.52, 0.6, 0.68],
        },
    ]
    for idx, spec in enumerate(cards):
        with row[idx]:
            st.markdown(
                build_finops_kpi_card_html(
                    title=str(spec["title"]),
                    value=str(spec["value"]),
                    tip=str(spec["tip"]),
                    chips=[],
                    lines=[str(spec["lines"][0]), str(spec["lines"][1])],
                    accent_color=str(spec["accent"]),
                    extra_class="signals-hub-mini",
                    icon=str(spec["icon"]),
                    status=str(spec["tone"]),
                    trend_text="",
                    trend_tone=str(spec["tone"]),
                    spark_values=list(spec["spark"]),
                    spark_tone=str(spec["tone"]),
                    spark_motif="curve",
                    extra_lines=2,
                ),
                unsafe_allow_html=True,
            )
    if scoped_delivery_view:
        card_notes = st.columns(4, gap="small")
        with card_notes[1]:
            st.caption("Program total (Apptio Actuals, not allocated to team/app)")

    chip_colors = {
        "risk": str(sem.get("risk", "#f97316")),
        "watch": str(sem.get("forecast", "#84cc16")),
        "info": str(sem.get("neutral", "#94a3b8")),
    }
    badge_labels = {"risk": "Risk", "watch": "Watch", "info": "Info"}
    chip_html: list[str] = []
    for sig in ranked[:6]:
        sev = str(sig.get("severity") or "info").lower()
        sev_color = chip_colors.get(sev, chip_colors["info"])
        badge = badge_labels.get(sev, "Info")
        level_label = html.escape(str(sig.get("level") or "Program"))
        icon = html.escape(str(sig.get("icon") or "•"))
        text = html.escape(str(sig.get("text") or ""))
        chip_html.append(
            "<span class='signals-hub-chip'>"
            f"<span class='signals-hub-chip-badge' style='border:1px solid {sev_color}; color:{sev_color};'>{badge}</span>"
            f"<span class='signals-hub-chip-level'>{level_label}</span>"
            f"<span>{icon} {text}</span>"
            "</span>"
        )
    if chip_html:
        st.markdown(
            "<div class='signals-hub-chip-wrap'>" + "".join(chip_html) + "</div>",
            unsafe_allow_html=True,
        )

    groups_without_programs = bool((debug_meta or {}).get("groups_without_programs"))
    if not financial_available:
        st.caption("Financial signals are available at Program level (Apptio Actuals).")
    elif str(role or "").strip() == "Product Owner" or groups_without_programs:
        st.caption("Financial signals are available at Program level (Apptio Actuals).")

    if top_df is None or top_df.empty:
        st.info("No signals available for the selected scope.")
    else:
        height = 120 + min(220, int(len(top_df.index)) * 34)
        st.dataframe(top_df, use_container_width=True, hide_index=True, height=height)

    if is_debug_enabled():
        with st.expander("Debug - Signals Hub diagnostics", expanded=False):
            sev_counts = (
                pd.Series([str(s.get("severity") or "info").lower() for s in ranked], dtype=str)
                .value_counts(dropna=False)
                .to_dict()
            )
            lvl_counts = (
                pd.Series([str(s.get("level") or "Program") for s in ranked], dtype=str)
                .value_counts(dropna=False)
                .to_dict()
            )
            st.write("Signal counts by severity:", sev_counts)
            st.write("Signal counts by level:", lvl_counts)
            if debug_meta:
                st.write(
                    "Effective scope:",
                    {
                        "eff_programs": debug_meta.get("eff_programs", []),
                        "eff_teams": debug_meta.get("eff_teams", []),
                        "eff_groups": debug_meta.get("eff_groups", []),
                    },
                )
                st.write(
                    "Filtered feature scope:",
                    {
                        "df_features_rows_filtered": debug_meta.get("df_features_rows_filtered", 0),
                        "df_features_teams_filtered": debug_meta.get("df_features_teams_filtered", []),
                    },
                )
                st.write("Source availability:", debug_meta)


data_version = get_data_version()
st.markdown(
    """
    <style>
    div[data-testid="stMetric"] { min-height: 64px; }
    div[data-testid="stCaption"] { min-height: 18px; }
    </style>
    """,
    unsafe_allow_html=True,
)

scope = read_scope_from_session(fetch_df)
role = getattr(scope, "inferred_role", None) or infer_role(scope)

source_colors = {
    "NEXT (WF+NWF)": "#2563eb",
    "NEXT Forecast": "#f59e0b",
    "Apptio": "#16a34a",
    "ADO": "#7c3aed",
    "NEXT + ADO": "#0ea5e9",
    "Apptio + NEXT Forecast": "#0891b2",
    "Apptio + ADO": "#0d9488",
}

base_df = fetch_filter_options(data_version=data_version)

years_opts = (
    sorted(pd.to_numeric(base_df.get("YEAR"), errors="coerce").dropna().astype(int).unique().tolist())
    if base_df is not None and not base_df.empty
    else []
)
as_of = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date()
as_of_year = int(as_of.year)
default_year = as_of_year if as_of_year in years_opts else (max(years_opts) if years_opts else as_of_year)
view_label = scope_label(scope, role)
render_page_header(
    "Insights",
    "Signals that highlight risk, imbalance, and data gaps",
    view_label,
    role,
    scope_status_right=touch_last_updated_status("insights"),
)

# Filters: Year + Programs + Teams + Applications.
default_programs = list(getattr(scope, "programs", []) or [])
default_teams = list(getattr(scope, "teams", []) or [])
default_groups = list(getattr(scope, "groups", []) or [])
insights_year_key = "insights_year"
insights_programs_key = "insights_programs"
insights_teams_key = "insights_teams"
insights_groups_key = "insights_groups"

with st.expander("Filters", expanded=False):
    raw_year = st.session_state.get(insights_year_key, default_year)
    try:
        safe_year = int(raw_year) if raw_year is not None and not pd.isna(raw_year) else int(default_year)
    except Exception:
        safe_year = int(default_year)
    year_index = years_opts.index(safe_year) if safe_year in years_opts else 0
    if not years_opts:
        st.caption("No years available for Insights yet.")
        year = int(default_year)
    else:
        year = int(st.selectbox("Year", years_opts, index=year_index, key=insights_year_key))

    base_y = base_df.copy() if base_df is not None else pd.DataFrame()
    if not base_y.empty:
        base_y["YEAR"] = pd.to_numeric(base_y.get("YEAR"), errors="coerce").astype("Int64")
        base_y = base_y.loc[base_y["YEAR"].eq(int(year))].copy()

    programs_opts: list[str] = []
    if not base_y.empty and "PROGRAMNAME" in base_y.columns:
        programs_opts = sorted({str(x).strip() for x in base_y["PROGRAMNAME"].dropna().astype(str).tolist() if str(x).strip()})
    show_portfolio_program_options = role in {"Portfolio Manager", "Unknown", "ADMIN"} or not bool(getattr(scope, "programs", []))
    if show_portfolio_program_options:
        try:
            p_df = list_programs()
            if isinstance(p_df, pd.DataFrame) and not p_df.empty and "PROGRAMNAME" in p_df.columns:
                p_master = {str(v).strip() for v in p_df["PROGRAMNAME"].dropna().astype(str).tolist() if str(v).strip()}
                programs_opts = sorted(set(programs_opts).union(p_master))
        except Exception:
            pass
    scope_programs = {
        str(v).strip()
        for v in (getattr(scope, "programs", []) or [])
        if str(v).strip()
    }
    if scope_programs:
        programs_opts = sorted(set(programs_opts).union(scope_programs))
    program_label_map: dict[str, str] = {}
    program_alias_map: dict[str, set[str]] = {}
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
                aliases = {v for v in [disp, raw] if str(v).strip()}
                for alias in aliases:
                    key = str(alias).strip().upper()
                    if not key:
                        continue
                    slot = program_alias_map.setdefault(key, set())
                    slot.update({str(v).strip() for v in aliases if str(v).strip()})
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
            if key not in chosen:
                chosen[key] = disp
                order.append(key)
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

    def _expand_program_filter_values(values: list[str], alias_map: dict[str, set[str]]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for v in values or []:
            raw_v = str(v).strip()
            if not raw_v:
                continue
            aliases = alias_map.get(raw_v.upper(), {raw_v})
            for alias in sorted({str(a).strip() for a in aliases if str(a).strip()}):
                if alias.upper() in seen:
                    continue
                seen.add(alias.upper())
                out.append(alias)
        return out

    programs_opts = _dedupe_options_by_label(programs_opts, program_label_map)
    default_programs = _remap_selected_by_label(default_programs, programs_opts, program_label_map)

    sel_programs = st.multiselect(
        "Programs",
        programs_opts,
        default=default_programs,
        key=insights_programs_key,
        format_func=lambda x, _m=program_label_map: _m.get(str(x).strip(), str(x).strip()),
    )

    teams_opts: list[str] = []
    team_disabled = not bool(sel_programs)
    if not team_disabled:
        teams_df = base_y.copy()
        if sel_programs and not teams_df.empty and "PROGRAMNAME" in teams_df.columns:
            teams_df = teams_df.loc[teams_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
        if not teams_df.empty and "TEAMNAME" in teams_df.columns:
            teams_opts = sorted({str(x).strip() for x in teams_df["TEAMNAME"].dropna().astype(str).tolist() if str(x).strip()})
        if not teams_opts:
            try:
                t_df = list_teams()
                if isinstance(t_df, pd.DataFrame) and not t_df.empty and "TEAMNAME" in t_df.columns:
                    if sel_programs and "PROGRAMNAME" in t_df.columns:
                        t_df = t_df.loc[t_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
                    teams_opts = sorted({str(v).strip() for v in t_df["TEAMNAME"].dropna().astype(str).tolist() if str(v).strip()})
            except Exception:
                teams_opts = []
    scope_teams = {str(v).strip() for v in (getattr(scope, "teams", []) or []) if str(v).strip()}
    if scope_teams:
        teams_opts = sorted(set(teams_opts).union(scope_teams))
    current_teams = [t for t in st.session_state.get(insights_teams_key, []) if t in set(teams_opts)]
    st.session_state[insights_teams_key] = current_teams
    sel_teams = st.multiselect(
        "Teams",
        teams_opts,
        default=current_teams or [t for t in default_teams if t in set(teams_opts)],
        key=insights_teams_key,
        disabled=team_disabled,
        help="Select Program(s) first." if team_disabled else None,
    )

    groups_opts: list[str] = []
    group_disabled = not bool(sel_programs)
    if not group_disabled:
        groups_df = base_y.copy()
        if sel_programs and not groups_df.empty and "PROGRAMNAME" in groups_df.columns:
            groups_df = groups_df.loc[groups_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
        if sel_teams and not groups_df.empty and "TEAMNAME" in groups_df.columns:
            groups_df = groups_df.loc[groups_df["TEAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_teams))].copy()
        if not groups_df.empty and "GROUPNAME" in groups_df.columns:
            groups_opts = sorted({str(x).strip() for x in groups_df["GROUPNAME"].dropna().astype(str).tolist() if str(x).strip()})
        if not groups_opts:
            try:
                g_df = list_application_groups()
                if isinstance(g_df, pd.DataFrame) and not g_df.empty and "GROUPNAME" in g_df.columns:
                    if sel_programs and "PROGRAMNAME" in g_df.columns:
                        g_df = g_df.loc[g_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
                    if sel_teams and "TEAMNAME" in g_df.columns:
                        g_df = g_df.loc[g_df["TEAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_teams))].copy()
                    groups_opts = sorted({str(v).strip() for v in g_df["GROUPNAME"].dropna().astype(str).tolist() if str(v).strip()})
            except Exception:
                groups_opts = []
    current_groups = [g for g in st.session_state.get(insights_groups_key, []) if g in set(groups_opts)]
    st.session_state[insights_groups_key] = current_groups
    sel_groups = st.multiselect(
        "Application groups",
        groups_opts,
        default=current_groups or [g for g in default_groups if g in set(groups_opts)],
        key=insights_groups_key,
        disabled=group_disabled,
        help="Select Program(s) first." if group_disabled else None,
    )
    render_active_filters_summary(
        "Active filters",
        [
            ("Year", year),
            ("Programs", sel_programs),
            ("Teams", sel_teams),
            ("Application groups", sel_groups),
        ],
    )
    insights_render_mode = st.radio(
        "Render mode",
        ["Summary", "Full"],
        index=0,
        horizontal=True,
        key="insights_render_mode",
        help="Summary renders core insights only. Full also renders detailed NWF and signal sections.",
    )

# Insights compares Baseline/Projected internally; users do not select scenarios here.
scenario = "Projected"
driver_dim = "APP_GROUP" if sel_groups else ("TEAM" if sel_teams else "PROGRAM")

include_unmapped = True

ins_nonce = int(st.session_state.get("insights_nonce", 0))
preview_rev = ins_nonce
page_perf_key = "insights"
insights_filters_sig = filters_signature(
    {
        "year": int(year),
        "programs": list(sel_programs or []),
        "teams": list(sel_teams or []),
        "groups": list(sel_groups or []),
        "render_mode": str(insights_render_mode or ""),
    }
)
insights_user_scope_sig = user_scope_signature(scope, role)
insights_cache_buster = get_portfolio_cache_buster()

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

with page_loader(messages=[
    "Loading scope and filters...",
    "Building the cost model...",
    "Aggregating...",
    "Preparing charts...",
    "Finalizing view...",
], show_status=False) as loader:
    loader.step("Loading scope and filters...")
    loader.step("Building the cost model...")
    _set_loading("Resolving scope and KPI inputs...", 2)
    with perf_step("resolve_effective_scope", page_key=page_perf_key):
        eff_programs_selected = [program_label_map.get(str(p).strip(), str(p).strip()) for p in (sel_programs or []) if str(p).strip()]
        eff_programs_selected = list(dict.fromkeys(eff_programs_selected))
        eff_programs = _expand_program_filter_values(eff_programs_selected, program_alias_map)
        eff_teams = list(sel_teams or [])
        eff_groups = list(sel_groups or [])
        view_mode = "Application view"

    viewing_parts = [f"Year: {int(year)}"]
    if eff_programs_selected:
        viewing_parts.append(f"Programs: {len(eff_programs_selected)}")
    if eff_teams:
        viewing_parts.append(f"Teams: {len(eff_teams)}")
    if eff_groups:
        viewing_parts.append(f"Applications: {len(eff_groups)}")
    st.caption("Viewing: " + " • ".join(viewing_parts))

    # Shared headline (same wording/logic as Welcome/Dashboard/Data Quality for the same filters).
    try:
        as_of = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date()
        scope_dict = {"programs": list(eff_programs or []), "teams": list(eff_teams or []), "groups": []}
        filters_headline = {
            "year": int(year) if year else None,
            "program": list(eff_programs or []),
            "team": list(eff_teams or []),
            "app_group": [],
            "group_by": ["PROGRAMNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"],
        }

        def _pi_to_lines(df_pi: pd.DataFrame) -> pd.DataFrame:
            if df_pi is None or df_pi.empty:
                return pd.DataFrame()
            w = df_pi.copy()
            dims = [c for c in w.columns if c not in {"LABOR_BUCKET", "TOTAL_COST"}]
            w["AMOUNT"] = pd.to_numeric(w.get("TOTAL_COST"), errors="coerce").fillna(0.0)
            out = w.groupby(dims, dropna=False)["AMOUNT"].sum().reset_index()
            return out

        filters_headline_key = _stable_key(filters_headline)
        scope_headline_key = _stable_key({"programs": list(eff_programs or []), "teams": list(eff_teams or []), "groups": []})
        base_pi = cached_get_pi_costs(
            scenario="Baseline",
            filters_key=filters_headline_key,
            year=int(year),
            scope_key=scope_headline_key,
            filters_sig=insights_filters_sig,
            user_scope_sig=insights_user_scope_sig,
            cache_buster=insights_cache_buster,
        )
        filters_headline_proj = {**filters_headline, "include_unmapped": True}
        proj_pi = cached_get_pi_costs(
            scenario="Projected",
            filters_key=_stable_key(filters_headline_proj),
            year=int(year),
            scope_key=scope_headline_key,
            filters_sig=insights_filters_sig,
            user_scope_sig=insights_user_scope_sig,
            cache_buster=insights_cache_buster,
        )
        df_lines = pd.concat([_pi_to_lines(base_pi), _pi_to_lines(proj_pi)], ignore_index=True, sort=False)

        scope_for_headline = type(scope)(
            user=scope.user,
            programs=list(eff_programs or []),
            teams=list(eff_teams or []),
            groups=[],
            program_ids=list(getattr(scope, "program_ids", []) or []),
            user_scope=getattr(scope, "user_scope", None),
            inferred_role=getattr(scope, "inferred_role", None),
        )
        headline = compute_headline(
            df_lines=df_lines,
            selected_year=int(year),
            as_of=as_of,
            role=str(role or ""),
            scope=scope_for_headline,
            scope_dict=scope_dict,
            driver_dim=driver_dim,
        )
        render_headline_with_help(headline)
    except Exception:
        pass

    def _pi_total(df_pi: pd.DataFrame) -> float:
        if df_pi is None or df_pi.empty:
            return 0.0
        return float(pd.to_numeric(df_pi.get("TOTAL_COST"), errors="coerce").fillna(0.0).sum() or 0.0)

    filters_totals = {
        "year": int(year) if year else None,
        "program": list(eff_programs or []),
        "team": list(eff_teams or []),
        "app_group": list(eff_groups or []),
        "group_by": [],
        "include_unmapped": True,
    }
    filters_totals_key = _stable_key(filters_totals)
    scope_costs_key = _stable_key({"programs": list(eff_programs or []), "teams": list(eff_teams or []), "groups": list(eff_groups or [])})
    _set_loading("Computing KPI strip...", 3)
    with perf_step("totals_cost_fetch", page_key=page_perf_key):
        base_tot = cached_get_pi_costs(
            scenario="Baseline",
            filters_key=filters_totals_key,
            year=int(year),
            scope_key=scope_costs_key,
            filters_sig=insights_filters_sig,
            user_scope_sig=insights_user_scope_sig,
            cache_buster=insights_cache_buster,
        )
        proj_tot = cached_get_pi_costs(
            scenario="Projected",
            filters_key=filters_totals_key,
            year=int(year),
            scope_key=scope_costs_key,
            filters_sig=insights_filters_sig,
            user_scope_sig=insights_user_scope_sig,
            cache_buster=insights_cache_buster,
        )

    baseline_cost_total = _pi_total(base_tot)
    expected_cost_total = _pi_total(proj_tot)
    loader.step("Aggregating...")
    actual_cost_total = None

    # YTD approximation: sum PIs that have ended (for current FY); full-year otherwise.
    forecast_ytd = None
    actual_ytd = None
    variance = None
    variance_pct = None
    try:
        today = pd.Timestamp.today().normalize()
        proj_pi2 = proj_tot.copy()
        proj_pi2["AMOUNT"] = pd.to_numeric(proj_pi2.get("TOTAL_COST"), errors="coerce").fillna(0.0)
        proj_pi2 = proj_pi2[["YEAR", "PI", "AMOUNT"]]
        proj_pi2 = attach_pi_period_columns(proj_pi2)
        if int(year) == int(dt.date.today().year) and "PERIOD_END_DATE" in proj_pi2.columns:
            proj_pi2["PERIOD_END_DATE"] = pd.to_datetime(proj_pi2.get("PERIOD_END_DATE"), errors="coerce")
            forecast_ytd = float(proj_pi2.loc[proj_pi2["PERIOD_END_DATE"].notna() & (proj_pi2["PERIOD_END_DATE"] <= today), "AMOUNT"].sum() or 0.0)
        else:
            forecast_ytd = float(proj_pi2["AMOUNT"].sum() or 0.0)
    except Exception:
        pass

    filters_cost: dict = {
        "year": int(year) if year else None,
        "program": list(eff_programs or []),
        "team": list(eff_teams or []),
        "app_group": list(eff_groups or []),
        "include_unmapped": True,
        "group_by": ["PROGRAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"],
    }

    _set_loading("Preparing insights datasets (scope)...", 4)
    with perf_step("prepare_insights_inputs", page_key=page_perf_key):
        _inputs = cached_prepare_insights_inputs(
            year=int(year),
            programs=tuple(eff_programs or ()),
            teams=tuple(eff_teams or ()),
            groups=tuple(eff_groups or ()),
            data_version=int(data_version or 0),
            preview_rev=int(preview_rev or 0),
            filters_cost_key=_stable_key(filters_cost),
            scope_key=scope_costs_key,
            filters_sig=insights_filters_sig,
            user_scope_sig=insights_user_scope_sig,
            cache_buster=insights_cache_buster,
        )
    df_cost_src = _inputs.get("df_cost_src", pd.DataFrame()).copy()
    df_cost = _inputs.get("df_cost", pd.DataFrame()).copy()
    df_ado = _inputs.get("df_ado", pd.DataFrame()).copy()

    loader.step("Preparing charts...")
    # Derived FTE is canonical across the app (Budget/Capacity/Insights/Welcome).
    df_explorer = _inputs.get("df_explorer", pd.DataFrame()).copy()
    if eff_teams:
        _team_set = {str(t).strip().upper() for t in eff_teams if str(t).strip()}
        if _team_set:
            if isinstance(df_explorer, pd.DataFrame) and not df_explorer.empty:
                _team_col_explorer = (
                    "TEAMNAME"
                    if "TEAMNAME" in df_explorer.columns
                    else ("team_name" if "team_name" in df_explorer.columns else ("team_key" if "team_key" in df_explorer.columns else None))
                )
                if _team_col_explorer:
                    df_explorer[_team_col_explorer] = df_explorer.get(_team_col_explorer, "").fillna("").astype(str).str.strip()
                    df_explorer = df_explorer[df_explorer[_team_col_explorer].str.upper().isin(_team_set)].copy()
            if isinstance(df_ado, pd.DataFrame) and not df_ado.empty:
                _team_col_ado = (
                    "TEAMNAME"
                    if "TEAMNAME" in df_ado.columns
                    else ("team_name" if "team_name" in df_ado.columns else ("team_key" if "team_key" in df_ado.columns else None))
                )
                if _team_col_ado:
                    df_ado[_team_col_ado] = df_ado.get(_team_col_ado, "").fillna("").astype(str).str.strip()
                    df_ado = df_ado[df_ado[_team_col_ado].str.upper().isin(_team_set)].copy()
    portfolio_programs_scope = _expand_program_filter_values(
        [program_label_map.get(str(p).strip(), str(p).strip()) for p in (programs_opts or []) if str(p).strip()],
        program_alias_map,
    )
    if not portfolio_programs_scope:
        portfolio_programs_scope = list(eff_programs or [])
    filters_cost_portfolio = {
        "year": int(year) if year else None,
        "program": list(portfolio_programs_scope or []),
        "team": [],
        "app_group": [],
        "include_unmapped": True,
        "group_by": ["PROGRAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"],
    }
    portfolio_scope_key = _stable_key(
        {"programs": list(portfolio_programs_scope or []), "teams": [], "groups": []}
    )
    _set_loading("Preparing insights datasets (portfolio benchmark)...", 4)
    with perf_step("prepare_portfolio_inputs", page_key=page_perf_key):
        _portfolio_inputs = cached_prepare_insights_inputs(
            year=int(year),
            programs=tuple(portfolio_programs_scope or ()),
            teams=tuple(),
            groups=tuple(),
            data_version=int(data_version or 0),
            preview_rev=int(preview_rev or 0),
            filters_cost_key=_stable_key(filters_cost_portfolio),
            scope_key=portfolio_scope_key,
            filters_sig=insights_filters_sig,
            user_scope_sig=insights_user_scope_sig,
            cache_buster=insights_cache_buster,
        )
    df_cost_src_portfolio = _portfolio_inputs.get("df_cost_src", pd.DataFrame()).copy()
    df_ado_portfolio = _portfolio_inputs.get("df_ado", pd.DataFrame()).copy()
    df_explorer_portfolio = _portfolio_inputs.get("df_explorer", pd.DataFrame()).copy()
    total_cost = float(pd.to_numeric(df_cost.get("AMOUNT"), errors="coerce").fillna(0.0).sum()) if df_cost is not None and not df_cost.empty else 0.0

    # Mapping completeness belongs to Data Quality; Insights does not gate or compute mapped-only values.
    total_bv = float(pd.to_numeric(df_ado.get("BUSINESS_VALUE"), errors="coerce").fillna(0.0).sum()) if df_ado is not None and not df_ado.empty else 0.0
    feature_count = int(len(df_ado.index)) if df_ado is not None and not df_ado.empty else 0
    bv_cov_all = None
    if df_ado is not None and not df_ado.empty:
        bv = pd.to_numeric(df_ado.get("BUSINESS_VALUE"), errors="coerce").fillna(0.0)
        bv_cov_all = float((bv > 0).mean()) if len(bv.index) else None

    derived_fte_sum = (
        float(pd.to_numeric(df_explorer.get("DERIVED_FTE_EXPLORER"), errors="coerce").fillna(0.0).sum())
        if df_explorer is not None and not df_explorer.empty
        else 0.0
    )
    swag_ready_cov = None
    if df_explorer is not None and not df_explorer.empty:
        ready = df_explorer.get("SWAG_READY", pd.Series([False] * len(df_explorer), index=df_explorer.index)).fillna(False).astype(bool)
        swag_ready_cov = float(ready.mean()) if len(ready.index) else None

    # Mapping coverage (ADO -> TCO) for transparency. Uses MAPPING_STATUS produced during the ADO→TCO join.
    mapping_cov_note = None
    mapping_details_note = None
    if df_ado is not None and not df_ado.empty and "MAPPING_STATUS" in df_ado.columns:
        ms = df_ado.get("MAPPING_STATUS", "").fillna("").astype(str).str.upper()
        mapped_n = int((ms == "MAPPED").sum())
        unmapped_app_n = int((ms == "UNMAPPED_APP_GROUP").sum())
        out_scope_n = int((ms == "OUT_OF_SCOPE_PROGRAM").sum())
        total_n = int(len(ms.index))
        mapping_cov_note = _fmt_pct((mapped_n / total_n) if total_n else None)
        mapping_details_note = f"{mapped_n} mapped • {unmapped_app_n} need app map • {out_scope_n} out of scope"

    benefits_mid = 0.0
    benefits_low = 0.0
    benefits_high = None
    ado_non_base = df_ado.copy() if df_ado is not None else pd.DataFrame()
    if not ado_non_base.empty:
        if "IS_BASE" not in ado_non_base.columns:
            ado_non_base["IS_BASE"] = 0
        ado_non_base["IS_BASE"] = pd.to_numeric(ado_non_base.get("IS_BASE"), errors="coerce").fillna(0).astype(int)
        ado_non_base = ado_non_base[ado_non_base["IS_BASE"] == 0].copy()
    base_groups: set[str] = set()
    if df_ado is not None and not df_ado.empty and "IS_BASE" in df_ado.columns:
        tmp_bg = df_ado.copy()
        tmp_bg["IS_BASE"] = pd.to_numeric(tmp_bg.get("IS_BASE"), errors="coerce").fillna(0).astype(int)
        tmp_bg["GROUPNAME"] = tmp_bg.get("GROUPNAME", "").fillna("").astype(str).str.strip()
        base_groups = set(tmp_bg.loc[(tmp_bg["IS_BASE"] == 1) & (tmp_bg["GROUPNAME"] != ""), "GROUPNAME"].tolist())

    # BV completeness should ignore BASE app groups (run-the-business).
    bv_cov_required: Optional[float] = None
    if df_ado is not None and not df_ado.empty:
        if ado_non_base is None or ado_non_base.empty:
            bv_cov_required = 1.0
        else:
            bv_cov_required, _ = compute_ado_coverage(ado_non_base, data_version=data_version)

    benefits_low_non_base = 0.0
    benefits_mid_non_base = 0.0
    benefits_high_non_base = None
    total_bv_non_base = 0.0
    if df_ado is not None and not df_ado.empty:
        bv_vals = pd.to_numeric(df_ado.get("BUSINESS_VALUE"), errors="coerce").fillna(0.0)
        benefits = bv_vals.apply(_benefit_for_bv)
        benefits_low = float(benefits.apply(lambda x: x[0]).sum())
        benefits_mid = float(benefits.apply(lambda x: x[2]).sum())
        high_vals = benefits.apply(lambda x: x[1])
        benefits_high = None if high_vals.isna().any() else float(high_vals.sum())
    if not ado_non_base.empty:
        bv_vals_nb = pd.to_numeric(ado_non_base.get("BUSINESS_VALUE"), errors="coerce").fillna(0.0)
        total_bv_non_base = float(bv_vals_nb.sum())
        benefits_nb = bv_vals_nb.apply(_benefit_for_bv)
        benefits_low_non_base = float(benefits_nb.apply(lambda x: x[0]).sum())
        benefits_mid_non_base = float(benefits_nb.apply(lambda x: x[2]).sum())
        high_vals_nb = benefits_nb.apply(lambda x: x[1])
        benefits_high_non_base = None if high_vals_nb.isna().any() else float(high_vals_nb.sum())

    forecast_cost_ytd = _forecast_ytd_from_cost_base(df_cost, year=int(year))
    df_cost_non_base = df_cost.copy() if df_cost is not None else pd.DataFrame()
    if df_cost_non_base is not None and not df_cost_non_base.empty and "GROUPNAME" in df_cost_non_base.columns:
        df_cost_non_base["GROUPNAME"] = df_cost_non_base["GROUPNAME"].fillna("").astype(str).str.strip()
        if base_groups:
            df_cost_non_base = df_cost_non_base[~df_cost_non_base["GROUPNAME"].isin(base_groups)].copy()
    forecast_cost_ytd_non_base = _forecast_ytd_from_cost_base(df_cost_non_base, year=int(year))
    forecast_cost_reason = None
    if forecast_cost_ytd is None:
        forecast_cost_reason = "Forecast is unavailable for selected scope."
    elif forecast_cost_ytd <= 0:
        forecast_cost_reason = "Forecast is 0 or unavailable for selected scope."

    # Cost per Demand should use the same TCO-cost basis and demand axis as Welcome.
    total_cost_reason = None
    if df_cost is None or df_cost.empty:
        total_cost_reason = "Cost is unavailable for selected scope."
    elif total_cost <= 0:
        total_cost_reason = "Cost is 0 or unavailable for selected scope."

    # Demand axis: SWAG-derived Derived FTE only.
    demand_axis_total: Optional[float] = derived_fte_sum if derived_fte_sum > 0 else None
    demand_axis_label = "Derived FTE (SWAG)"

    roi_proxy_reason = None
    cost_per_bv_reason = None
    if ado_non_base is None or ado_non_base.empty:
        roi_proxy_reason = "N/A - BASE work has no BV."
        cost_per_bv_reason = "N/A - BASE work has no BV."
    roi_proxy = _safe_div(benefits_mid_non_base, forecast_cost_ytd_non_base) if (forecast_cost_ytd_non_base and not roi_proxy_reason) else None
    cost_per_bv = _safe_div(forecast_cost_ytd_non_base, total_bv_non_base) if (forecast_cost_ytd_non_base and total_bv_non_base > 0 and not cost_per_bv_reason) else None
    cost_per_demand = _safe_div(total_cost, demand_axis_total) if (total_cost and demand_axis_total) else None
    loader.step("Finalizing view...")


_set_loading("Rendering KPI strip...", 3)
st.markdown("<div id='insights-kpi-strip-anchor'></div>", unsafe_allow_html=True)
st.markdown(
    """
    <style>
    @import url("https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20,400,0,0");
    /* FinOps KPI cards – Insights only */
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-card {
        height: 170px;
        display: flex;
        background: transparent;
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 11px;
        overflow: hidden;
        position: relative;
        isolation: isolate;
        box-shadow: 0 8px 18px rgba(2, 6, 23, 0.16), 0 1px 0 rgba(255,255,255,0.03) inset;
        transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-layer {
        position: absolute;
        inset: 0 0 0 4px;
        pointer-events: none;
        z-index: 0;
        background:
            radial-gradient(180px 96px at 95% 12%, color-mix(in srgb, var(--finops-kpi-accent) 42%, transparent), transparent 74%),
            linear-gradient(180deg, color-mix(in srgb, var(--finops-kpi-accent) 16%, transparent), transparent 62%);
        opacity: 1;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art {
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
        opacity: 0.42;
        filter: saturate(1.1) blur(0.8px);
        -webkit-mask-image: linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 0.08) 28%, rgba(0, 0, 0, 0.55) 54%, #000 100%);
        mask-image: linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 0.08) 28%, rgba(0, 0, 0, 0.55) 54%, #000 100%);
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-bar {
        flex: 1 1 auto;
        border-radius: 3px 3px 0 0;
        background: linear-gradient(180deg, rgba(148, 163, 184, 0.9), rgba(148, 163, 184, 0.2));
        min-height: 3px;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-bar {
        background: linear-gradient(180deg, rgba(74, 222, 128, 0.95), rgba(34, 197, 94, 0.22));
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-bar {
        background: linear-gradient(180deg, rgba(251, 191, 36, 0.95), rgba(245, 158, 11, 0.24));
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-bar {
        background: linear-gradient(180deg, rgba(248, 113, 113, 0.95), rgba(239, 68, 68, 0.24));
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--wave .finops-kpi-bg-bar,
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--dotline .finops-kpi-bg-bar,
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--curve .finops-kpi-bg-bar {
        display: none;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--wave,
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--dotline,
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--curve {
        width: 58%;
        height: 60%;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-svg {
        width: 100%;
        height: 100%;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-fill {
        display: none !important;
        fill: none !important;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-poly {
        fill: none;
        stroke: rgba(148, 163, 184, 0.72);
        stroke-width: 1.05px;
        stroke-linecap: round;
        stroke-linejoin: round;
        vector-effect: non-scaling-stroke;
        opacity: 0.42;
        filter: drop-shadow(0 0 1px color-mix(in srgb, var(--finops-kpi-accent) 20%, transparent));
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-curve {
        fill: none;
        stroke: rgba(148, 163, 184, 0.72);
        stroke-width: 1.05px;
        stroke-linecap: round;
        stroke-linejoin: round;
        vector-effect: non-scaling-stroke;
        opacity: 0.42;
        filter: drop-shadow(0 0 1px color-mix(in srgb, var(--finops-kpi-accent) 20%, transparent));
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-dots {
        position: absolute;
        inset: 0;
        pointer-events: none;
        opacity: 0.2;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-dot-el {
        position: absolute;
        width: 5px;
        height: 5px;
        border-radius: 50%;
        background: rgba(226, 232, 240, 0.9);
        transform: translate(-50%, -50%);
        box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.5), 0 0 3px color-mix(in srgb, var(--finops-kpi-accent) 24%, transparent);
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-fill { fill: rgba(34, 197, 94, 0.16); }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-fill { fill: rgba(245, 158, 11, 0.16); }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-fill { fill: rgba(239, 68, 68, 0.16); }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-poly { stroke: rgba(74, 222, 128, 0.76); }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-poly { stroke: rgba(251, 191, 36, 0.76); }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-poly { stroke: rgba(248, 113, 113, 0.76); }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-curve { stroke: rgba(74, 222, 128, 0.78); }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-curve { stroke: rgba(251, 191, 36, 0.78); }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-curve { stroke: rgba(248, 113, 113, 0.78); }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-card::before {
        content: "";
        position: absolute;
        inset: 0 0 45% 4px;
        pointer-events: none;
        z-index: 0;
        background: linear-gradient(180deg, rgba(255,255,255,0.10) 0%, rgba(255,255,255,0.03) 55%, rgba(255,255,255,0.0) 100%);
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-card::after {
        content: "";
        position: absolute;
        inset: 0 0 0 4px;
        pointer-events: none;
        z-index: 0;
        background-image: repeating-radial-gradient(circle at 12% 10%, rgba(255,255,255,0.06) 0 0.7px, transparent 0.7px 2.8px);
        opacity: 0.07;
        filter: blur(0.9px);
        mix-blend-mode: screen;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-watermark {
        position: absolute;
        right: 10px;
        top: 8px;
        z-index: 0;
        pointer-events: none;
        opacity: 0.08;
        filter: blur(0.4px);
        line-height: 1;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-watermark .material-symbols-outlined {
        font-size: 54px;
        color: rgba(226, 232, 240, 0.65);
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-accent {
        width: 4px;
        flex-shrink: 0;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-body {
        padding: 12px 14px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        width: 100%;
        position: relative;
        z-index: 1;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 4px;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-title-wrap {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        min-width: 0;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-title-icon {
        font-size: 14px;
        color: rgba(148, 163, 184, 0.9);
        flex-shrink: 0;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-title {
        font-size: 0.76rem;
        color: rgba(255,255,255,0.74);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 90%;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-meta {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        flex-shrink: 0;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-help {
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
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-value {
        font-size: clamp(1.22rem, 1.7vw, 1.82rem);
        font-weight: 700;
        margin: 2px 0 6px 0;
        color: rgba(248, 250, 252, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-value-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-trend {
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
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-trend .material-symbols-outlined {
        font-size: 11px;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-trend--good {
        border-color: rgba(34, 197, 94, 0.55);
        background: rgba(34, 197, 94, 0.16);
        color: rgba(187, 247, 208, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-trend--watch {
        border-color: rgba(245, 158, 11, 0.55);
        background: rgba(245, 158, 11, 0.16);
        color: rgba(253, 230, 138, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-trend--risk {
        border-color: rgba(239, 68, 68, 0.55);
        background: rgba(239, 68, 68, 0.16);
        color: rgba(254, 202, 202, 0.95);
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-chips {
        margin-bottom: 6px;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-chip {
        display: inline-block;
        font-size: 0.65rem;
        padding: 2px 6px;
        border-radius: 10px;
        margin-right: 4px;
        background: rgba(255,255,255,0.12);
        color: rgba(248, 250, 252, 0.9);
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-lines {
        font-size: 0.7rem;
        color: rgba(255,255,255,0.5);
        line-height: 1.2;
    }
    @media (hover: hover) and (pointer: fine) {
        [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 14px 24px rgba(2, 6, 23, 0.24), 0 1px 0 rgba(255,255,255,0.05) inset;
            border-color: rgba(255,255,255,0.2);
        }
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .finops-kpi-line {
        height: 0.9rem;
    }
    [data-testid="stAppViewContainer"]:has(#insights-kpi-strip-anchor) .material-symbols-outlined {
        font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 20;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
st.subheader("KPI strip")
kpi_help = {
    "Estimated Benefits (Midpoint)": "Estimated benefits midpoint derived from BV score mapping.",
    "Projected Cost (Total)": "Projected (forecast) total for selected scope, with baseline reference.",
    "Cost Variance vs Baseline": "Projected minus baseline (Δ$ and Δ%) for the selected scope.",
    "Program NWF Variance": "Program-level non-workforce spend variance (Plan vs Actual).",
}
KPI_MOTIF_MAP = {
    "Estimated Benefits (Midpoint)": "curve",         # total benefits signal
    "Projected Cost (Total)": "bars",                 # total cost signal
    "Cost Variance vs Baseline": "dotline",           # variance/change signal
    "Program NWF Variance": "curve",                  # variance/change signal
}

def _shift_month_key(month_key: str, lag_months: int) -> str:
    s = str(month_key or "").strip()
    if not s or "-" not in s:
        return s
    try:
        y_s, m_s = s.split("-", 1)
        y = int(y_s)
        m = int(m_s)
    except Exception:
        return s
    idx = (y * 12 + (m - 1)) + int(lag_months or 0)
    y2 = idx // 12
    m2 = (idx % 12) + 1
    return f"{y2:04d}-{m2:02d}"


def _nwf_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cat = df.get("COST_CATEGORY", pd.Series(dtype=str)).fillna("").astype(str).str.upper().str.strip()
    return df.loc[cat.eq("NON_WORK_FORCE")].copy()


def _expand_pi_to_monthly_simple(df: pd.DataFrame) -> pd.DataFrame:
    """Minimal PI→Month expansion (matches Dashboard fallback behavior when PI calendar weights are unavailable)."""
    if df is None or df.empty:
        return pd.DataFrame()
    w = df.copy()
    w["YEAR"] = pd.to_numeric(w.get("YEAR"), errors="coerce").fillna(int(year)).astype(int)
    w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce").fillna(0).astype(int)
    w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)
    rows = []
    for _, r in w.iterrows():
        pi_num = int(r.get("PI") or 0)
        yv = int(r.get("YEAR") or int(year))
        amt = float(r.get("AMOUNT") or 0.0)
        if pi_num in {1, 2, 3, 4}:
            months = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}.get(pi_num, [])
            for m in months:
                tmp = r.copy()
                tmp["MONTH_KEY"] = f"{yv:04d}-{m:02d}"
                tmp["AMOUNT"] = amt / 3.0
                rows.append(tmp)
        else:
            for m in range(1, 13):
                tmp = r.copy()
                tmp["MONTH_KEY"] = f"{yv:04d}-{m:02d}"
                tmp["PI"] = int(((m - 1) // 3) + 1)
                tmp["AMOUNT"] = amt / 12.0
                rows.append(tmp)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# Program-level NWF Actuals (Apptio) section inputs (same dataset as Dashboard NWF Spend — Plan vs Actual).
programs_for_actuals = list(eff_programs or [])
lag_months = 2
with perf_step("nwf_actuals_monthly_fetch", page_key=page_perf_key):
    actual_monthly_raw = cached_program_nwf_actuals_monthly(
        year=int(year),
        programs=tuple(programs_for_actuals or ()),
        lag_months=int(lag_months),
        filters_sig=insights_filters_sig,
        user_scope_sig=insights_user_scope_sig,
        cache_buster=insights_cache_buster,
    )
actual_nwf_total = float(pd.to_numeric(actual_monthly_raw.get("AMOUNT"), errors="coerce").fillna(0.0).sum() or 0.0) if not actual_monthly_raw.empty else 0.0

with perf_step("load_cost_model_program_nwf", page_key=page_perf_key):
    cm_prog = cached_load_cost_model(
        year=int(year),
        scope_key=_stable_key({"programs": list(programs_for_actuals or []), "teams": [], "groups": []}),
        filters_sig=insights_filters_sig,
        user_scope_sig=insights_user_scope_sig,
        cache_buster=insights_cache_buster,
    )
cost_model_warning = get_cost_model_warning(
    int(year),
    {"programs": list(programs_for_actuals or []), "teams": [], "groups": []},
)
baseline_nwf = _nwf_rows(cm_prog.get("BASELINE", pd.DataFrame()).assign(SCENARIO="BASELINE"))
baseline_nwf_total = float(pd.to_numeric(baseline_nwf.get("AMOUNT"), errors="coerce").fillna(0.0).sum() or 0.0) if not baseline_nwf.empty else 0.0
program_nwf_delta = actual_nwf_total - baseline_nwf_total
program_nwf_delta_pct = (program_nwf_delta / baseline_nwf_total) if baseline_nwf_total else None


def _fmt_signed_money(val: Optional[float]) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "—"
    sign = "+" if float(val) > 0 else ""
    return f"{sign}${float(val):,.0f}"


def _fmt_signed_pct(val: Optional[float]) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "—"
    sign = "+" if float(val) > 0 else ""
    return f"{sign}{float(val) * 100:.1f}%"


delta_expected = expected_cost_total - baseline_cost_total
delta_expected_pct = (delta_expected / baseline_cost_total) if baseline_cost_total else None

budget_color = source_colors["NEXT (WF+NWF)"]
ado_color = source_colors["ADO"]
apptio_color = source_colors["Apptio"]

row = st.columns(4)
with row[0]:
    kpi_title = "Estimated Benefits (Midpoint)"
    benefits_tone = "good" if (benefits_mid or 0.0) > 0 else "neutral"
    render_finops_kpi_card(
        title=kpi_title,
        value=f"{_fmt_money(benefits_mid, 0)}" if benefits_mid is not None else "—",
        tip="Estimated benefits based on mid-case assumptions.",
        chips=["ADO"],
        lines=[],
        accent_color="#7c3aed",
        icon="insights",
        status=benefits_tone,
        trend_text="BV-based",
        trend_tone=benefits_tone,
        spark_values=[0.2, 0.26, 0.33, 0.31, 0.4, 0.47, 0.52, 0.6],
        spark_tone=benefits_tone,
        spark_motif=KPI_MOTIF_MAP.get(kpi_title, "bars"),
    )
with row[1]:
    kpi_title = "Projected Cost (Total)"
    projected_tone = "risk" if (delta_expected or 0.0) > 0 else "good"
    render_finops_kpi_card(
        title=kpi_title,
        value=_fmt_money(expected_cost_total, 0),
        tip="Comparison between baseline budget and projected (forecast) cost.",
        chips=["Projected", "Baseline"],
        lines=[
            f"Baseline (Plan): {_fmt_money(baseline_cost_total, 0)}",
            f"Δ vs baseline: {_fmt_signed_money(delta_expected)} ({_fmt_signed_pct(delta_expected_pct)})",
        ],
        accent_color="#2563eb",
        icon="account_balance",
        status=projected_tone,
        trend_text=_fmt_signed_pct(delta_expected_pct),
        trend_tone=projected_tone,
        spark_values=(
            [0.24, 0.33, 0.29, 0.46, 0.42, 0.57, 0.53, 0.69]
            if (delta_expected or 0.0) >= 0
            else [0.69, 0.57, 0.62, 0.49, 0.53, 0.4, 0.43, 0.31]
        ),
        spark_tone=projected_tone,
        spark_motif=KPI_MOTIF_MAP.get(kpi_title, "bars"),
    )
with row[2]:
    kpi_title = "Cost Variance vs Baseline"
    variance_tone = "risk" if (delta_expected or 0.0) > 0 else ("good" if (delta_expected or 0.0) < 0 else "neutral")
    render_finops_kpi_card(
        title=kpi_title,
        value=_fmt_signed_money(delta_expected),
        tip="Difference between projected and baseline cost.",
        chips=["Variance"],
        lines=[
            f"{_fmt_signed_pct(delta_expected_pct)} vs baseline",
            f"Baseline (Plan): {_fmt_money(baseline_cost_total, 0)}",
            f"Projected: {_fmt_money(expected_cost_total, 0)}",
        ],
        accent_color="#f59e0b",
        icon="monitoring",
        status=variance_tone,
        trend_text=_fmt_signed_pct(delta_expected_pct),
        trend_tone=variance_tone,
        spark_values=(
            [0.2, 0.43, 0.3, 0.54, 0.38, 0.62, 0.49, 0.7]
            if (delta_expected or 0.0) >= 0
            else [0.72, 0.58, 0.65, 0.46, 0.53, 0.36, 0.41, 0.23]
        ),
        spark_tone=variance_tone,
        spark_motif=KPI_MOTIF_MAP.get(kpi_title, "dotline"),
    )
with row[3]:
    kpi_title = "Program NWF Variance"
    nwf_tone = "risk" if (program_nwf_delta or 0.0) > 0 else ("good" if (program_nwf_delta or 0.0) < 0 else "neutral")
    render_finops_kpi_card(
        title=kpi_title,
        value=_fmt_signed_money(program_nwf_delta),
        tip="Non-workforce planned versus actual spend (program-level).",
        chips=["Plan vs Actual"],
        lines=[
            f"{_fmt_signed_pct(program_nwf_delta_pct)} vs plan",
            f"Plan: {_fmt_money(baseline_nwf_total, 0)}",
            f"Actual: {_fmt_money(actual_nwf_total, 0)}",
        ],
        accent_color="#16a34a",
        icon="receipt_long",
        status=nwf_tone,
        trend_text=_fmt_signed_pct(program_nwf_delta_pct),
        trend_tone=nwf_tone,
        spark_values=(
            [0.18, 0.27, 0.35, 0.32, 0.43, 0.49, 0.57, 0.64]
            if (program_nwf_delta or 0.0) >= 0
            else [0.66, 0.59, 0.51, 0.46, 0.38, 0.34, 0.27, 0.21]
        ),
        spark_tone=nwf_tone,
        spark_motif=KPI_MOTIF_MAP.get(kpi_title, "curve"),
    )

mark_first_kpi_render(page_perf_key)
_render_section_divider()

scope_programs_for_financial = [str(v).strip() for v in (getattr(scope, "programs", []) or []) if str(v).strip()]
programs_for_apptio = _programs_for_scope(
    base_df,
    int(year),
    list(eff_programs or []) if eff_programs else scope_programs_for_financial,
    eff_teams,
)
can_show_program_financial = bool(programs_for_apptio) and (
    str(role or "").strip() in {"Portfolio Manager", "Program Manager", "ADMIN"}
    or bool(scope_programs_for_financial)
    or bool(eff_programs)
)
programs_for_apptio_fetch = programs_for_apptio if can_show_program_financial else []
apptio_df, apptio_debug = _fetch_apptio_actuals_breakdown(int(year), programs_for_apptio_fetch)

with perf_step("load_cost_model_signals", page_key=page_perf_key):
    cm_signals = cached_load_cost_model(
        year=int(year),
        scope_key=_stable_key({"programs": list(eff_programs or []), "teams": list(eff_teams or []), "groups": []}),
        filters_sig=insights_filters_sig,
        user_scope_sig=insights_user_scope_sig,
        cache_buster=insights_cache_buster,
    )
if not cost_model_warning:
    cost_model_warning = get_cost_model_warning(
        int(year),
        {"programs": list(eff_programs or []), "teams": list(eff_teams or []), "groups": []},
    )
if cost_model_warning:
    st.warning(cost_model_warning, icon="⚠️")
cap_df = fetch_capacity_demand_pi(int(year), list(eff_programs or []), list(eff_teams or []), data_version=data_version)

filters_maturity = {
    "year": int(year) if year else None,
    "program": list(eff_programs or []),
    "team": list(eff_teams or []),
    "app_group": list(eff_groups or []),
    "include_unmapped": True,
    "group_by": ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"],
}
filters_maturity_key = _stable_key(filters_maturity)
maturity_scope_key = _stable_key({"programs": list(eff_programs or []), "teams": list(eff_teams or []), "groups": list(eff_groups or [])})
with perf_step("fetch_maturity_cost_scenarios", page_key=page_perf_key):
    maturity_base = cached_get_pi_costs(
        scenario="Baseline",
        filters_key=filters_maturity_key,
        year=int(year),
        scope_key=maturity_scope_key,
        filters_sig=insights_filters_sig,
        user_scope_sig=insights_user_scope_sig,
        cache_buster=insights_cache_buster,
    )
    maturity_proj = cached_get_pi_costs(
        scenario="Projected",
        filters_key=filters_maturity_key,
        year=int(year),
        scope_key=maturity_scope_key,
        filters_sig=insights_filters_sig,
        user_scope_sig=insights_user_scope_sig,
        cache_buster=insights_cache_buster,
    )
    maturity_act = cached_get_pi_costs(
        scenario="Actual",
        filters_key=filters_maturity_key,
        year=int(year),
        scope_key=maturity_scope_key,
        filters_sig=insights_filters_sig,
        user_scope_sig=insights_user_scope_sig,
        cache_buster=insights_cache_buster,
    )

maturity_bundle = ProgramMaturityBundle(
    cost_baseline_df=maturity_base if isinstance(maturity_base, pd.DataFrame) else pd.DataFrame(),
    cost_projected_df=maturity_proj if isinstance(maturity_proj, pd.DataFrame) else pd.DataFrame(),
    cost_actual_df=maturity_act if isinstance(maturity_act, pd.DataFrame) else pd.DataFrame(),
    ado_df=df_ado if isinstance(df_ado, pd.DataFrame) else pd.DataFrame(),
    explorer_df=df_explorer if isinstance(df_explorer, pd.DataFrame) else pd.DataFrame(),
    capacity_df=cap_df if isinstance(cap_df, pd.DataFrame) else pd.DataFrame(),
    meta={
        "year": int(year),
        "programs": list(eff_programs or []),
        "groups": list(eff_groups or []),
        "scenarios": {"baseline": "Baseline", "expected": "Projected", "actual": "Actual"},
        "financial_mode": "forecast_only",
    },
)
maturity_df = calculate_program_maturity(maturity_bundle, financial_mode="forecast_only")
maturity_trend_df = calculate_program_maturity_trend(maturity_bundle)
maturity_risk_df = build_maturity_risk_signals(maturity_df, maturity_trend_df)

financial_signals, financial_debug = build_financial_signals(
    apptio_df if can_show_program_financial else pd.DataFrame(),
    programs_used=programs_for_apptio_fetch,
)
delivery_signals, delivery_debug = build_delivery_signals(
    cm_signals if isinstance(cm_signals, dict) else {},
    cap_df if isinstance(cap_df, pd.DataFrame) else pd.DataFrame(),
    df_explorer if isinstance(df_explorer, pd.DataFrame) else pd.DataFrame(),
    eff_programs=list(eff_programs or []),
    eff_teams=list(eff_teams or []),
    eff_groups=list(eff_groups or []),
)
baseline_signal, baseline_signal_debug = _build_baseline_change_signal(
    year=int(year),
    role=str(role or ""),
    eff_programs=list(eff_programs or []),
    eff_teams=list(eff_teams or []),
    eff_groups=list(eff_groups or []),
    data_version=data_version,
)
signals_pool = list(financial_signals or []) + list(delivery_signals or [])
if baseline_signal:
    signals_pool.insert(0, baseline_signal)
signals = rank_signals(signals_pool)
if not signals:
    fallback_items = build_fallback_scope_signals(
        projected_vs_baseline_pct=delta_expected_pct,
        nwf_plan_vs_actual_pct=program_nwf_delta_pct,
        swag_ready_cov=swag_ready_cov,
        bv_cov_required=bv_cov_required,
    )
    signals = rank_signals(
        _fallback_items_to_structured_signals(
            fallback_items,
            eff_teams=list(eff_teams or []),
            eff_groups=list(eff_groups or []),
        )
    )
signals_debug = {
    "eff_programs": list(eff_programs or []),
    "eff_teams": list(eff_teams or []),
    "eff_groups": list(eff_groups or []),
    "can_show_program_financial": bool(can_show_program_financial),
    "fallback_used": not bool(financial_signals) and not bool(delivery_signals) and not bool(baseline_signal),
    "baseline_signal_added": bool(baseline_signal),
    "financial_available": bool(financial_signals),
    "groups_without_programs": bool(eff_groups and not eff_programs),
    "rows": {
        "apptio_df": int(apptio_df.shape[0]) if isinstance(apptio_df, pd.DataFrame) else 0,
        "cap_df": int(cap_df.shape[0]) if isinstance(cap_df, pd.DataFrame) else 0,
        "cm_expected": int((cm_signals.get("EXPECTED", pd.DataFrame()) if isinstance(cm_signals, dict) else pd.DataFrame()).shape[0]),
    },
    "df_features_rows_filtered": int(df_explorer.shape[0]) if isinstance(df_explorer, pd.DataFrame) else 0,
    "df_features_teams_filtered": [],
    "financial": financial_debug,
    "delivery": delivery_debug,
    "baseline": baseline_signal_debug,
    "apptio_fetch": apptio_debug,
}
if isinstance(df_explorer, pd.DataFrame) and not df_explorer.empty:
    _feat_team_col = (
        "TEAMNAME"
        if "TEAMNAME" in df_explorer.columns
        else ("team_name" if "team_name" in df_explorer.columns else ("team_key" if "team_key" in df_explorer.columns else None))
    )
    if _feat_team_col:
        _teams = (
            df_explorer.get(_feat_team_col, "")
            .fillna("")
            .astype(str)
            .str.strip()
        )
        _teams = sorted([t for t in _teams.unique().tolist() if t])[:30]
        signals_debug["df_features_teams_filtered"] = _teams

_section_title(
    "Key Signals & Observations",
    "Auto-generated signals highlight cost structure, trends, and delivery pressure. "
    "Financial signals use Apptio Actuals at Program level only; they are not app-group attributable.",
)
render_signals_hub(
    signals,
    scope=scope,
    role=str(role or ""),
    theme=_merged_theme(),
    financial_available=bool(financial_signals),
    eff_programs=list(eff_programs or []),
    eff_teams=list(eff_teams or []),
    eff_groups=list(eff_groups or []),
    debug_meta=signals_debug,
)

app_group_position_df = pd.DataFrame(
    columns=["PROGRAMNAME", "GROUPNAME", "DEMAND_AXIS", "Y_VALUE_PLOT", "ACTUAL_COST", "PROJECTED_SPEND", "quadrant_label"]
)
feature_work_df = pd.DataFrame(columns=["PROGRAMNAME", "TEAMNAME", "IS_MSP_FEATURE", "INVESTMENT_DIMENSION", "DEMAND_FTE"])
program_strategic_df = pd.DataFrame()
strategic_drivers_df = pd.DataFrame()
strategic_ref_axis_bounds = {
    "x_min": 0.0,
    "x_max": 1.0,
    "y_min": 0.0,
    "y_max": 1.0,
    "x_split": 0.5,
    "y_split": 0.5,
    "x_split_ratio": 0.5,
    "y_split_ratio": 0.5,
}

_set_loading("Rendering insight tabs...", 4)
insights_cost_tab, insights_program_maturity_tab, insights_governance_tab, insights_risk_tab, insights_nwf_tab = st.tabs(
    [
        "Cost vs Value",
        "Program Maturity (Strategic Positioning)",
        "NEXT Governance Readiness (Operational Control)",
        "Risk Signals",
        "NWF Actuals",
    ]
)

with insights_cost_tab:
    cost_tab_slot = st.container()

with insights_program_maturity_tab:
    strategic_maturity_tab_slot = st.container()

with insights_nwf_tab:
    nwf_tab_slot = st.container()

with insights_governance_tab:
    _section_title(
        "NEXT Governance Readiness",
        "Operational control scorecard using canonical filtered inputs only (cost, demand, and capacity).",
    )
    with st.container(border=True):
        st.markdown(
            "This score measures how well the program is structured and controlled inside the NEXT cost governance model (baseline, mapping, capacity, transparency)."
        )
        st.caption(
            "This view measures structural governance inside NEXT (mapping, planning, transparency). It does not evaluate strategic investment mix."
        )
    with st.expander("How the score is calculated", expanded=False):
        st.markdown(
            "\n".join(
                [
                    "- Financial Predictability (30%): Forecast vs Baseline variance.",
                    "- Cost Transparency (20%): % spend mapped to real app groups; overhead penalty.",
                    "- Demand Alignment (20%): % derived FTE mapped; unassigned penalty.",
                    "- Delivery Efficiency (20%): Demand vs capacity deviation + cost per story point (normalized).",
                    "- Planning Discipline (10%): Baseline configured + capacity configured + rates present.",
                ]
            )
        )
        with st.expander("Detailed formulas", expanded=False):
            st.markdown(
                "\n".join(
                    [
                        "- Financial: 70% variance score + 30% stability score.",
                        "- Transparency: 70% mapped cost + 30% (100 - overhead).",
                        "- Demand: 70% mapped demand + 30% (100 - unmapped effort).",
                        "- Efficiency: 60% normalized cost/SP score + 40% demand-capacity deviation score.",
                        "- Planning: average of baseline configured, capacity configured, rates configured.",
                    ]
                )
            )
    st.caption("Delivery Efficiency uses a planning proxy: Demand vs Capacity deviation (not Actual labor deviation).")
    if maturity_df is None or maturity_df.empty:
        st.info("No governance readiness data available for the selected scope.")
    else:
        maturity_view = st.radio(
            "View",
            ["Quadrant", "Heatmap"],
            horizontal=True,
            key="insights_governance_view_mode",
        )

        mdf = maturity_df.copy()
        mdf["financial_control"] = 100.0 - pd.to_numeric(mdf.get("variance_pct"), errors="coerce").abs().clip(0.0, 100.0)
        mdf["financial_control"] = pd.to_numeric(mdf.get("financial_control"), errors="coerce").fillna(50.0)
        mdf["transparency_index"] = pd.to_numeric(mdf.get("transparency_pct"), errors="coerce").fillna(0.0)
        mdf["maturity_score"] = pd.to_numeric(mdf.get("maturity_score"), errors="coerce").fillna(0.0)
        mdf["total_spend"] = pd.to_numeric(mdf.get("total_spend"), errors="coerce").fillna(0.0)

        def _maturity_size(v: float, lo: float, hi: float) -> float:
            if hi <= 0:
                return 18.0
            norm = (math.sqrt(max(v, 0.0)) - math.sqrt(max(lo, 0.0))) / (math.sqrt(max(hi, 0.0)) - math.sqrt(max(lo, 0.0)) + 1e-9)
            return max(10.0, min(68.0, 10.0 + norm * 58.0))

        spend_lo = float(mdf["total_spend"].min()) if not mdf.empty else 0.0
        spend_hi = float(mdf["total_spend"].max()) if not mdf.empty else 0.0

        if maturity_view == "Quadrant":
            points = []
            for _, r in mdf.iterrows():
                program = str(r.get("program") or "").strip()
                if not program:
                    continue
                x = float(r.get("financial_control") or 0.0)
                y = float(r.get("transparency_index") or 0.0)
                spend = float(r.get("total_spend") or 0.0)
                maturity_score_val = float(r.get("maturity_score") or 0.0)
                points.append(
                    {
                        "name": program,
                        "value": [x, y, spend, maturity_score_val],
                        "symbolSize": _maturity_size(spend, spend_lo, spend_hi),
                    }
                )
            x_med = float(pd.to_numeric(mdf.get("financial_control"), errors="coerce").median()) if not mdf.empty else 50.0
            y_med = float(pd.to_numeric(mdf.get("transparency_index"), errors="coerce").median()) if not mdf.empty else 50.0
            option_maturity = {
                "tooltip": {
                    "trigger": "item",
                    "formatter": JsCode(
                        """
                        function(params){
                          const v = params.value || [0,0,0,0];
                          return `<b>${params.name}</b><br/>Financial control: ${Number(v[0]).toFixed(1)}<br/>Transparency: ${Number(v[1]).toFixed(1)}<br/>Spend: $${Number(v[2]).toLocaleString()}<br/>Maturity: ${Number(v[3]).toFixed(1)}`;
                        }
                        """
                    )
                    if JsCode
                    else None,
                },
                "grid": {"left": 55, "right": 35, "top": 50, "bottom": 40},
                "xAxis": {"type": "value", "name": "Financial Control", "min": 0, "max": 100},
                "yAxis": {"type": "value", "name": "Operational Transparency", "min": 0, "max": 100},
                "visualMap": {
                    "type": "continuous",
                    "min": 0,
                    "max": 100,
                    "dimension": 3,
                    "orient": "vertical",
                    "right": 0,
                    "top": "middle",
                    "inRange": {"color": ["#ef4444", "#f59e0b", "#22c55e"]},
                    "text": ["High", "Low"],
                },
                "series": [
                    {
                        "type": "scatter",
                        "data": points,
                        "markLine": {
                            "silent": True,
                            "lineStyle": {"type": "dashed", "color": "#64748b"},
                            "data": [{"xAxis": x_med}, {"yAxis": y_med}],
                        },
                    }
                ],
                "graphic": [
                    {"type": "text", "left": "13%", "top": "15%", "style": {"text": "Transparent but Unstable", "fill": "#9ca3af", "font": "12px sans-serif"}},
                    {"type": "text", "right": "20%", "top": "15%", "style": {"text": "Mature & Controlled", "fill": "#9ca3af", "font": "12px sans-serif"}},
                    {"type": "text", "left": "13%", "bottom": "10%", "style": {"text": "Cost Chaos", "fill": "#9ca3af", "font": "12px sans-serif"}},
                    {"type": "text", "right": "24%", "bottom": "10%", "style": {"text": "Hidden Risk", "fill": "#9ca3af", "font": "12px sans-serif"}},
                ],
            }
            chart_col, legend_col = st.columns([0.74, 0.26], gap="medium")
            with chart_col:
                maturity_click = render_echart(
                    option_maturity,
                    height="520px",
                    key=f"insights_program_maturity_quadrant_{int(year)}",
                    page_theme=page_theme,
                    events={"click": "function(params){ return { program: params.name }; }"},
                )
            with legend_col:
                st.markdown("**How to read this chart**")
                st.caption("X = Financial Control")
                st.caption("Y = Transparency")
                st.caption("Bubble size = Spend")
                st.caption("Color = Governance readiness score")
                st.markdown("**Quadrants**")
                st.caption("Mature & Controlled: high control, high transparency.")
                st.caption("Transparent but Unstable: high transparency, low control.")
                st.caption("Hidden Risk: high control, low transparency.")
                st.caption("Cost Chaos: low control, low transparency.")
            if isinstance(maturity_click, dict):
                picked = str(maturity_click.get("program") or maturity_click.get("name") or "").strip()
                if picked:
                    st.session_state["insights_governance_selected_program"] = picked
        else:
            st.caption("Governance Health Matrix")
            heat_cols = [
                ("Financial", "financial_score"),
                ("Transparency", "transparency_score"),
                ("Demand", "demand_score"),
                ("Efficiency", "efficiency_score"),
                ("Planning", "planning_score"),
                ("Overall", "maturity_score"),
            ]
            programs = [str(v) for v in mdf.get("program", pd.Series(dtype=str)).tolist()]
            heat_data = []
            for y_idx, program in enumerate(programs):
                row = mdf.iloc[y_idx]
                for x_idx, (_, col) in enumerate(heat_cols):
                    heat_data.append([x_idx, y_idx, float(pd.to_numeric(row.get(col), errors="coerce") or 0.0)])
            option_heat = {
                "tooltip": {"position": "top"},
                "grid": {"left": 120, "right": 30, "top": 25, "bottom": 35},
                "xAxis": {"type": "category", "data": [c[0] for c in heat_cols]},
                "yAxis": {"type": "category", "data": programs},
                "visualMap": {
                    "min": 0,
                    "max": 100,
                    "calculable": True,
                    "orient": "horizontal",
                    "left": "center",
                    "bottom": 0,
                    "inRange": {"color": ["#ef4444", "#f59e0b", "#22c55e"]},
                },
                "series": [
                    {
                        "type": "heatmap",
                        "data": heat_data,
                        "label": (
                            {
                                "show": True,
                                "formatter": JsCode(
                                    "function(params){ return Number((params.value||[])[2] || 0).toFixed(1); }"
                                ),
                            }
                            if JsCode
                            else {"show": False}
                        ),
                    }
                ],
            }
            render_echart(option_heat, height="520px", key=f"insights_governance_heatmap_{int(year)}", page_theme=page_theme)

        program_opts = sorted({str(v).strip() for v in mdf.get("program", pd.Series(dtype=str)).tolist() if str(v).strip()})
        selected_program = str(st.session_state.get("insights_governance_selected_program") or "").strip()
        if selected_program not in set(program_opts):
            selected_program = ""

        if not selected_program:
            top_cols = ["program", "maturity_score", "financial_score", "transparency_score", "total_spend"]
            needs_cols = ["program", "maturity_score", "variance_pct", "unmapped_effort_pct", "overhead_pct"]
            st.markdown("**Top Governance Ready**")
            st.dataframe(
                mdf.sort_values(["maturity_score", "total_spend"], ascending=[False, False]).head(5)[top_cols],
                use_container_width=True,
                hide_index=True,
            )
            st.markdown("**Needs Governance Attention**")
            st.dataframe(
                mdf.sort_values(["maturity_score", "total_spend"], ascending=[True, False]).head(5)[needs_cols],
                use_container_width=True,
                hide_index=True,
            )
            manual_pick = st.selectbox(
                "Select program for governance drilldown",
                options=[""] + program_opts,
                index=0,
                key="insights_governance_program_select_manual",
            ) if program_opts else ""
            if manual_pick:
                selected_program = str(manual_pick).strip()
                st.session_state["insights_governance_selected_program"] = selected_program

        if selected_program:
            st.session_state["insights_governance_selected_program"] = selected_program
            clear_cols = st.columns([0.8, 0.2], gap="small")
            with clear_cols[0]:
                st.caption(f"Selected program: {selected_program}")
            with clear_cols[1]:
                if st.button("Clear selection", key="insights_governance_clear_selection"):
                    st.session_state["insights_governance_selected_program"] = ""
                    selected_program = ""
            sel = mdf.loc[mdf["program"].astype(str).str.strip().eq(selected_program)].head(1)
            if selected_program and not sel.empty:
                r = sel.iloc[0]
                radar_height_px = 440
                c_left, c_right = st.columns([0.72, 0.28], gap="large")
                with c_left:
                    st.caption("Governance Dimensions")
                    radar_option = {
                        "tooltip": {"trigger": "item"},
                        "radar": {
                            "radius": "68%",
                            "splitNumber": 5,
                            "axisName": {"color": "#cbd5e1", "fontSize": 12},
                            "axisLine": {"lineStyle": {"color": "rgba(148,163,184,0.35)"}},
                            "splitLine": {"lineStyle": {"color": "rgba(148,163,184,0.25)"}},
                            "splitArea": {"areaStyle": {"color": ["rgba(15,23,42,0.0)", "rgba(30,41,59,0.10)"]}},
                            "indicator": [
                                {"name": "Financial Predictability", "max": 100},
                                {"name": "Cost Transparency", "max": 100},
                                {"name": "Demand Alignment", "max": 100},
                                {"name": "Delivery Efficiency", "max": 100},
                                {"name": "Planning Discipline", "max": 100},
                            ]
                        },
                        "series": [
                            {
                                "type": "radar",
                                "data": [
                                    {
                                        "name": selected_program,
                                        "value": [
                                            float(r.get("financial_score") or 0.0),
                                            float(r.get("transparency_score") or 0.0),
                                            float(r.get("demand_score") or 0.0),
                                            float(r.get("efficiency_score") or 0.0),
                                            float(r.get("planning_score") or 0.0),
                                        ],
                                    }
                                ],
                                "lineStyle": {"width": 2, "color": "#22c55e"},
                                "itemStyle": {"color": "#22c55e"},
                                "areaStyle": {"color": "rgba(34,197,94,0.22)"},
                            }
                        ],
                    }
                    render_echart(
                        radar_option,
                        height=f"{radar_height_px}px",
                        key=f"insights_governance_radar_{int(year)}_{selected_program}",
                        page_theme=page_theme,
                    )
                with c_right:
                    cpsp_val = pd.to_numeric(r.get("cost_per_story_point"), errors="coerce")
                    maturity_score = float(r.get("maturity_score") or 0.0)
                    variance_pct_val = float(pd.to_numeric(r.get("variance_pct"), errors="coerce") or 0.0)
                    unmapped_pct_val = float(r.get("unmapped_effort_pct") or 0.0)
                    overhead_pct_val = float(r.get("overhead_pct") or 0.0)
                    cpsp_disp = f"${float(cpsp_val):,.0f}" if cpsp_val == cpsp_val else "—"
                    dim_scores = [
                        ("Financial", float(r.get("financial_score") or 0.0)),
                        ("Transparency", float(r.get("transparency_score") or 0.0)),
                        ("Demand", float(r.get("demand_score") or 0.0)),
                        ("Efficiency", float(r.get("efficiency_score") or 0.0)),
                        ("Planning", float(r.get("planning_score") or 0.0)),
                    ]
                    bar_rows = []
                    for label, score in dim_scores:
                        safe_label = html.escape(label)
                        pct = max(0.0, min(100.0, score))
                        bar_rows.append(
                            "<div style='margin:8px 0 0 0;'>"
                            f"<div style='display:flex;justify-content:space-between;font-size:0.76rem;color:#cbd5e1;'>"
                            f"<span>{safe_label}</span><span>{pct:.1f}</span></div>"
                            "<div style='height:7px;background:rgba(148,163,184,0.25);border-radius:999px;overflow:hidden;'>"
                            f"<div style='height:7px;width:{pct:.1f}%;background:#22c55e;'></div>"
                            "</div>"
                            "</div>"
                        )
                    st.markdown(
                        f"""
                        <div style="
                            height:{int(radar_height_px)}px;
                            border:1px solid rgba(148,163,184,0.25);
                            border-radius:10px;
                            padding:12px 12px 10px 12px;
                            overflow:auto;
                            background:rgba(15,23,42,0.35);
                        ">
                            <div style="font-size:0.8rem;color:#94a3b8;margin-bottom:2px;">Governance Readiness Score</div>
                            <div style="font-size:2rem;line-height:1.1;font-weight:700;color:#f8fafc;margin-bottom:10px;">{maturity_score:.1f}</div>
                            <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px 8px;font-size:0.76rem;color:#cbd5e1;">
                                <div>Variance</div><div style="text-align:right;">{variance_pct_val:.1f}%</div>
                                <div>Unmapped</div><div style="text-align:right;">{unmapped_pct_val:.1f}%</div>
                                <div>Overhead</div><div style="text-align:right;">{overhead_pct_val:.1f}%</div>
                                <div>Cost / SP</div><div style="text-align:right;">{html.escape(cpsp_disp)}</div>
                            </div>
                            <div style="margin-top:10px;border-top:1px solid rgba(148,163,184,0.2);padding-top:6px;">
                                {''.join(bar_rows)}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        if is_debug_enabled():
            with st.expander("Debug – Governance Readiness diagnostics", expanded=False):
                st.caption("Per-program canonical maturity inputs and computed outputs.")
                diag_cols = [
                    "program",
                    "financial_mode_used",
                    "total_spend",
                    "variance_pct",
                    "mapped_cost_pct",
                    "mapped_demand_pct",
                    "transparency_pct",
                    "unmapped_effort_pct",
                    "cost_per_story_point",
                    "demand_capacity_deviation_pct",
                    "maturity_score",
                    "cpsp_min_portfolio",
                    "cpsp_max_portfolio",
                ]
                show_cols = [c for c in diag_cols if c in maturity_df.columns]
                st.dataframe(maturity_df[show_cols], use_container_width=True, hide_index=True)
                if maturity_risk_df is not None and not maturity_risk_df.empty:
                    st.caption("Triggered risk rules")
                    st.dataframe(maturity_risk_df, use_container_width=True, hide_index=True)

with insights_risk_tab:
    _section_title(
        "Governance Risk Signals",
        "Automatic governance-operational signals from readiness thresholds (Red, Yellow, Green).",
    )
    if maturity_risk_df is None or maturity_risk_df.empty:
        st.info("No maturity risk signals triggered for the selected scope.")
    else:
        r1, r2, r3 = st.columns(3)
        r1.metric("Red", int((maturity_risk_df["severity"] == "Red").sum()))
        r2.metric("Yellow", int((maturity_risk_df["severity"] == "Yellow").sum()))
        r3.metric("Green", int((maturity_risk_df["severity"] == "Green").sum()))
        st.dataframe(maturity_risk_df, use_container_width=True, hide_index=True)

with cost_tab_slot:
    _section_title(
        "Cost vs Value vs Demand",
        "Portfolio view of app-group cost, demand, and ROI proxy (benefits vs cost). Scenario: Forecast / Expected.",
    )
    benefit_mode = st.session_state.get("preview_benefit_mode", "Mid (default)")
    if benefit_mode not in ["Mid (default)", "Low", "High"]:
        benefit_mode = "Mid (default)"
    split_mode = str(st.session_state.get("insights_quadrant_split_mode", "Balanced (equal-size)"))
    if split_mode not in {"Balanced (equal-size)", "Data-driven"}:
        split_mode = "Balanced (equal-size)"
    split_percentile = int(st.session_state.get("insights_quadrant_split_percentile", 50) or 50)
    split_percentile = max(30, min(70, split_percentile))
    
    ROI_TARGET_BASELINE = 1.0
    COST_THRESHOLD_BASELINE = 500000
    BV_STRATEGIC_MIN = 80
    
    roi_target_active = float(st.session_state.get("roi_target_active", ROI_TARGET_BASELINE))
    scenario_active = abs(roi_target_active - ROI_TARGET_BASELINE) > 1e-9
    show_scope_default = resolve_show_default(eff_programs, eff_teams, eff_groups)
    show_scope_ctx = _stable_key(
        {
            "programs": [str(v).strip() for v in (eff_programs or []) if str(v).strip()],
            "teams": [str(v).strip() for v in (eff_teams or []) if str(v).strip()],
            "groups": [str(v).strip() for v in (eff_groups or []) if str(v).strip()],
        }
    )
    if st.session_state.get("insights_cost_show_scope_context") != show_scope_ctx:
        st.session_state["insights_cost_show_scope"] = show_scope_default
        st.session_state["insights_cost_show_scope_context"] = show_scope_ctx
    show_scope = str(st.session_state.get("insights_cost_show_scope", show_scope_default))
    if show_scope not in set(SHOW_SCOPE_OPTIONS):
        show_scope = show_scope_default
        st.session_state["insights_cost_show_scope"] = show_scope_default
    
    ado_curr = pd.DataFrame()
    cost_curr = pd.DataFrame()
    quadrant_empty_reason: Optional[str] = None
    
    # App-group view: keep unmapped rows (Data Quality owns mapping completeness).
    unassigned_tokens = {
        "",
        "unassigned",
        "(unassigned)",
        "(blank)",
        "blank",
        "none",
        "null",
        "nan",
        "n/a",
        "na",
    }
    
    team_display_map: dict[str, str] = {}
    try:
        teams_df_map = list_teams()
        if isinstance(teams_df_map, pd.DataFrame) and not teams_df_map.empty and "TEAMNAME" in teams_df_map.columns:
            for _, row in teams_df_map.iterrows():
                disp = str(row.get("TEAMNAME") or "").strip()
                raw = str(row.get("TEAMNAME_RAW") or "").strip()
                if disp:
                    team_display_map[disp.strip().lower()] = disp
                if raw and disp:
                    team_display_map[raw.strip().lower()] = disp
    except Exception:
        team_display_map = {}
    
    if df_ado is None or df_ado.empty:
        quadrant_empty_reason = "No ADO features found for selected scope."
        st.info(quadrant_empty_reason)
    else:
        ado_raw = df_ado.copy()
        ado_raw["BUSINESS_VALUE"] = pd.to_numeric(ado_raw.get("BUSINESS_VALUE"), errors="coerce")
        if "IS_BASE" not in ado_raw.columns:
            ado_raw["IS_BASE"] = 0
        ado_raw["IS_BASE"] = pd.to_numeric(ado_raw.get("IS_BASE"), errors="coerce").fillna(0).astype(int)
        benefits = ado_raw["BUSINESS_VALUE"].fillna(0.0).apply(_benefit_for_bv)
        ado_raw["BENEFIT_LOW"] = benefits.apply(lambda x: x[0])
        ado_raw["BENEFIT_HIGH"] = benefits.apply(lambda x: x[1])
        ado_raw["BENEFIT_MID"] = benefits.apply(lambda x: x[2])
        ado_raw["BENEFIT_SELECTED"] = _select_benefit_series(ado_raw, benefit_mode)
        ado_raw["GROUPNAME"] = ado_raw.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        ado_raw["TEAMNAME"] = (
            ado_raw.get("TEAMNAME", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.strip()
            .apply(lambda v, _m=team_display_map: _m.get(str(v).strip().lower(), str(v).strip()))
        )
        ado_raw.loc[ado_raw["GROUPNAME"].eq(""), "GROUPNAME"] = "(Unassigned)"
    
        cost_raw = df_cost_src.copy() if df_cost_src is not None else pd.DataFrame()
        if not cost_raw.empty:
            cost_raw["SOURCE"] = cost_raw.get("SOURCE", "").fillna("").astype(str).str.strip().str.upper()
            cost_raw = cost_raw[cost_raw["SOURCE"].ne("PROGRAM_ADDITIONAL")].copy()
            cost_raw["AMOUNT"] = pd.to_numeric(cost_raw.get("TOTAL_COST"), errors="coerce").fillna(0.0)
            cost_raw["PROGRAMNAME"] = cost_raw.get("PROGRAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
            cost_raw["TEAMNAME"] = (
                cost_raw.get("TEAMNAME", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.strip()
                .apply(lambda v, _m=team_display_map: _m.get(str(v).strip().lower(), str(v).strip()))
            )
            cost_raw["GROUPNAME"] = cost_raw.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
            cost_raw = cost_raw[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "SOURCE", "AMOUNT"]]
        else:
            cost_raw = pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "SOURCE", "AMOUNT"])
    
        ado_curr = ado_raw[~ado_raw["GROUPNAME"].str.lower().isin(unassigned_tokens)].copy()
        cost_curr = cost_raw[~cost_raw["GROUPNAME"].fillna("").astype(str).str.lower().isin(unassigned_tokens)].copy()
        if ado_curr.empty:
            quadrant_empty_reason = (
                "No mapped Applications for selected scope. Most features are Unassigned. "
                "Fix in Settings → ADO → App mapping."
            )
        elif cost_raw is None or cost_raw.empty:
            quadrant_empty_reason = (
                "No app-attributable costs found. Invoices and/or workforce costs may not be mapped to Applications."
            )
        elif cost_curr.empty:
            quadrant_empty_reason = (
                "No app-attributable costs mapped to Applications for the selected scope. "
                "Invoices and/or workforce costs may be missing Application/Application mapping."
            )
    
        if ado_curr.empty or cost_curr.empty:
            plot_df = pd.DataFrame(
                columns=[
                    "GROUPNAME",
                    "TEAMNAME",
                    "PROGRAMNAME",
                    "IS_BASE",
                    "BV_TOTAL",
                    "BV_MAX",
                    "FEATURE_COUNT",
                    "DERIVED_FTE",
                    "BENEFITS_SELECTED",
                    "BV_COVERAGE",
                    "SWAG_READY_PCT",
                    "ACTUAL_COST",
                ]
            )
        else:
            group_agg = ado_curr.groupby("GROUPNAME", dropna=False).agg(
                TEAMNAME=("TEAMNAME", lambda s: _safe_label(s.dropna().iloc[0]) if not s.dropna().empty else "Unassigned"),
                PROGRAMNAME=("PROGRAMNAME", lambda s: _safe_label(s.dropna().iloc[0]) if not s.dropna().empty else "Unassigned"),
                IS_BASE=("IS_BASE", "max"),
                BV_TOTAL=("BUSINESS_VALUE", "sum"),
                BV_MAX=("BUSINESS_VALUE", "max"),
                FEATURE_COUNT=("FEATURE_ID", "count"),
                BENEFITS_LOW=("BENEFIT_LOW", "sum"),
                BENEFITS_HIGH=("BENEFIT_HIGH", "sum"),
                BENEFITS_MID=("BENEFIT_MID", "sum"),
                BENEFITS_SELECTED=("BENEFIT_SELECTED", "sum"),
                BV_COUNT=("BUSINESS_VALUE", lambda s: s.notna().sum()),
            ).reset_index()
    
            group_agg["BV_COVERAGE"] = group_agg.apply(
                lambda r: _safe_div(_safe_float(r["BV_COUNT"]), _safe_float(r["FEATURE_COUNT"])) if r["FEATURE_COUNT"] else None,
                axis=1,
            )
            cost_group = cost_curr.groupby("GROUPNAME", dropna=False)["AMOUNT"].sum().reset_index()
            plot_df = group_agg.merge(cost_group, on="GROUPNAME", how="left")
            plot_df = plot_df.rename(columns={"AMOUNT": "ACTUAL_COST"})
            plot_df["GROUPNAME"] = plot_df.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
            plot_df["IS_BASE"] = pd.to_numeric(plot_df.get("IS_BASE"), errors="coerce").fillna(0).astype(int)
    
            # Attach Derived FTE per app group from Explorer v2 (canonical).
            plot_df["DERIVED_FTE"] = 0.0
            plot_df["SWAG_READY_PCT"] = pd.NA
            if df_explorer is not None and not df_explorer.empty and "GROUPNAME" in df_explorer.columns:
                tmp = df_explorer.copy()
                tmp["GROUPNAME"] = tmp.get("GROUPNAME", "").astype(str).str.strip()
                tmp["DERIVED_FTE"] = pd.to_numeric(tmp.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
                tmp["SWAG_READY"] = tmp.get("SWAG_READY", False).fillna(False).astype(bool)
                fte_by_group = (
                    tmp.groupby("GROUPNAME", dropna=False)
                    .agg(
                        DERIVED_FTE=("DERIVED_FTE", "sum"),
                        SWAG_READY_FEATURES=("SWAG_READY", "sum"),
                        FEATURE_COUNT=("FEATURE_ID", pd.Series.nunique),
                    )
                    .reset_index()
                )
                plot_df = plot_df.merge(fte_by_group, on="GROUPNAME", how="left", suffixes=("", "_EXPL"))
                plot_df["DERIVED_FTE"] = pd.to_numeric(
                    plot_df.get("DERIVED_FTE_EXPL", pd.Series(dtype=float)), errors="coerce"
                ).fillna(0.0)
                ready_n = pd.to_numeric(
                    plot_df.get("SWAG_READY_FEATURES_EXPL", pd.Series(dtype=float)), errors="coerce"
                ).fillna(0.0)
                feat_n = pd.to_numeric(
                    plot_df.get("FEATURE_COUNT_EXPL", pd.Series(dtype=float)), errors="coerce"
                ).fillna(0.0)
                plot_df["SWAG_READY_PCT"] = (ready_n / feat_n).where(feat_n > 0, pd.NA)
                plot_df = plot_df.drop(
                    columns=["DERIVED_FTE_EXPL", "SWAG_READY_FEATURES_EXPL", "FEATURE_COUNT_EXPL"], errors="ignore"
                )
    
        demand_axis_label = "Demand"
        plot_df["DEMAND_AXIS"] = pd.to_numeric(plot_df.get("DERIVED_FTE"), errors="coerce").fillna(0.0)

        # Insights governance view: ROI Proxy only (no Y-axis selector).
        y_axis_label = "ROI Proxy (x)"
        y_axis_col = "ROI_PROXY"
        plot_df["ROI_PROXY"] = plot_df.apply(
            lambda r: _safe_div(_safe_float(r["BENEFITS_SELECTED"]), _safe_float(r["ACTUAL_COST"])), axis=1
        )
        plot_df["Y_VALUE"] = pd.to_numeric(plot_df.get(y_axis_col), errors="coerce")
        plot_df["Y_VALUE_PLOT"] = plot_df["Y_VALUE"]
        non_base_min = pd.to_numeric(
            plot_df.loc[
                (plot_df.get("IS_BASE", 0).fillna(0).astype(int) == 0) & (plot_df["Y_VALUE"] > 0),
                "Y_VALUE",
            ],
            errors="coerce",
        ).dropna()
        y_floor = float(non_base_min.min()) * 0.5 if not non_base_min.empty else 1.0
        y_floor = max(y_floor, 1e-6)
        base_missing = (plot_df.get("IS_BASE", 0).fillna(0).astype(int) == 1) & (
            plot_df["Y_VALUE"].isna() | (plot_df["Y_VALUE"] <= 0)
        )
        plot_df.loc[base_missing, "Y_VALUE_PLOT"] = y_floor
        plot_df = plot_df.dropna(subset=["DEMAND_AXIS", "Y_VALUE_PLOT", "ACTUAL_COST"])

        app_plot_df = plot_df.copy()
        initiative_detail_df = pd.DataFrame()
        if show_scope == "Initiatives":
            initiative_detail_df = build_initiative_feature_detail_df(
                ado_curr=ado_curr,
                cost_curr=cost_curr,
                df_explorer=df_explorer if isinstance(df_explorer, pd.DataFrame) else pd.DataFrame(),
            )
            plot_df = build_initiative_plot_df(
                ado_curr=ado_curr,
                cost_curr=cost_curr,
                df_explorer=df_explorer if isinstance(df_explorer, pd.DataFrame) else pd.DataFrame(),
            )
        else:
            plot_df = rollup_scope_plot_df(app_plot_df, show_scope)

        show_zero = False
        use_log = True
        if not show_zero:
            plot_df = plot_df[(plot_df["DEMAND_AXIS"] > 0) & (plot_df["Y_VALUE_PLOT"] > 0) & (plot_df["ACTUAL_COST"] > 0)]
        if use_log:
            plot_df = plot_df[(plot_df["DEMAND_AXIS"] > 0) & (plot_df["Y_VALUE_PLOT"] > 0)]

        entity_values = (
            plot_df.get("ENTITY_LABEL", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.strip()
            .replace({"": "Unassigned"})
            .unique()
            .tolist()
        )
        entity_color_map = {name: APP_COLORS[idx % len(APP_COLORS)] for idx, name in enumerate(sorted(entity_values))}
        if entity_values:
            st.caption(show_scope)
            chips = []
            top_legend = plot_df.sort_values("ACTUAL_COST", ascending=False).head(12)
            for label in top_legend["ENTITY_LABEL"].astype(str).tolist():
                safe_label = html.escape(str(label).strip())
                color = entity_color_map.get(label, "#999999")
                chips.append(
                    "<span style='display:inline-flex; align-items:center; gap:6px; margin:2px 8px 2px 0;'>"
                    f"<span style='display:inline-block; width:10px; height:10px; background:{color}; border-radius:2px;'></span>"
                    f"{safe_label}"
                    "</span>"
                )
            remaining = max(0, len(entity_values) - min(len(top_legend.index), 12))
            if remaining > 0:
                chips.append(
                    "<span style='display:inline-flex; align-items:center; margin:2px 8px 2px 0; color:#94a3b8;'>"
                    f"+{remaining} more"
                    "</span>"
                )
            st.markdown(
                "<div style='display:flex; flex-wrap:wrap; margin:4px 0 2px 0;'>" + "".join(chips) + "</div>",
                unsafe_allow_html=True,
            )
        if eff_programs_selected:
            st.caption(f"Programs: {', '.join(eff_programs_selected[:6])}")

        min_cost = float(plot_df["ACTUAL_COST"].min()) if not plot_df.empty else 0.0
        max_cost = float(plot_df["ACTUAL_COST"].max()) if not plot_df.empty else 0.0

        def _bubble_size(cost: float) -> float:
            if max_cost <= 0:
                return 18.0
            norm = (math.sqrt(cost) - math.sqrt(min_cost)) / (math.sqrt(max_cost) - math.sqrt(min_cost) + 1e-9)
            return max(12.0, min(70.0, 12.0 + norm * 58.0))

        entity_term = {
            "Programs": "Program",
            "Teams": "Team",
            "Applications": "Application",
            "Initiatives": "Initiative",
        }.get(show_scope, "Entity")
        points = []
        for _, row in plot_df.iterrows():
            entity_label = _safe_label(row.get("ENTITY_LABEL"), fallback="(Unassigned)")
            demand_val = _safe_float(row.get("DEMAND_AXIS"), 0.0)
            cost_val = _safe_float(row.get("ACTUAL_COST"), 0.0)
            y_plot_val = _safe_float(row.get("Y_VALUE_PLOT"), 0.0)
            if use_log and (demand_val <= 0 or y_plot_val <= 0):
                continue
            benefit_selected = _safe_float(row.get("BENEFITS_SELECTED"), 0.0)
            roi_val = (_safe_div(benefit_selected, cost_val) or 0.0) if cost_val else None
            roi_disp = f"{roi_val:.2f}x" if roi_val is not None else "—"
            tooltip_html = "<br/>".join(
                [
                    f"<b>{html.escape(str(entity_label))}</b>",
                    f"{entity_term}: {html.escape(str(entity_label))}",
                    f"Cost: {_fmt_money(cost_val)}",
                    f"Demand: {demand_val:,.2f} FTE",
                    f"ROI Proxy: {roi_disp} | Benefits: {_fmt_money(benefit_selected)}",
                ]
            )
            points.append(
                {
                    "name": entity_label,
                    "value": [demand_val, y_plot_val, cost_val],
                    "symbolSize": _bubble_size(cost_val),
                    "itemStyle": {"color": entity_color_map.get(entity_label, APP_COLORS[0])},
                    "tooltip": {"formatter": tooltip_html},
                }
            )

        series = []
        if points:
            series.append(
                {
                    "name": show_scope,
                    "type": "scatter",
                    "data": points,
                    "emphasis": {"focus": "series"},
                    "z": 2,
                }
            )

        chart_params = _compute_quadrant_params(
            plot_df,
            split_mode=split_mode,
            split_percentile=split_percentile,
            roi_target_active=roi_target_active,
            use_log=use_log,
        )
        axis_x_min = chart_params["axis_x_min"]
        axis_x_max = chart_params["axis_x_max"]
        axis_y_min = chart_params["axis_y_min"]
        axis_y_max = chart_params["axis_y_max"]
        x_split = chart_params["x_split"]
        y_split = chart_params["y_split"]
        axis_use_log = use_log

        # Balanced visual mode: keep data-driven split placement but render equal-size quadrants.
        if split_mode == "Balanced (equal-size)" and plot_df is not None and not plot_df.empty:
            split_df = plot_df.copy()
            if "IS_BASE" in split_df.columns:
                non_base = split_df[split_df["IS_BASE"].fillna(0).astype(int) == 0]
                split_df = non_base if not non_base.empty else split_df
            x_vals_bal = pd.to_numeric(split_df.get("DEMAND_AXIS"), errors="coerce").dropna()
            y_vals_bal = pd.to_numeric(split_df.get("Y_VALUE_PLOT"), errors="coerce").dropna()
            if not x_vals_bal.empty and not y_vals_bal.empty:
                q_bal = float(max(30, min(70, int(split_percentile)))) / 100.0
                x_split = _safe_float(x_vals_bal.quantile(q_bal), _safe_float(x_vals_bal.median(), x_split))
                y_med_bal = _safe_float(y_vals_bal.median(), y_split)
                y_split = _adaptive_target_split(y_vals_bal, median_value=y_med_bal, target_value=roi_target_active)
                if use_log and x_split > 0 and y_split > 0:
                    x_pos = x_vals_bal[x_vals_bal > 0]
                    y_pos = y_vals_bal[y_vals_bal > 0]
                    if not x_pos.empty and not y_pos.empty:
                        x_obs_min = _safe_float(x_pos.min(), max(x_split * 0.1, 1e-6))
                        x_obs_max = _safe_float(x_pos.max(), x_split)
                        y_obs_min = _safe_float(y_pos.min(), max(y_split * 0.1, 1e-6))
                        y_obs_max = _safe_float(y_pos.max(), y_split)
                        x_ratio = max(x_obs_max / max(x_split, 1e-9), x_split / max(x_obs_min, 1e-9), 1.05)
                        y_ratio = max(y_obs_max / max(y_split, 1e-9), y_split / max(y_obs_min, 1e-9), 1.05)
                        axis_x_min = max(x_split / x_ratio, 1e-9)
                        axis_x_max = max(x_split * x_ratio, axis_x_min * 1.0001)
                        axis_y_min = max(y_split / y_ratio, 1e-9)
                        axis_y_max = max(y_split * y_ratio, axis_y_min * 1.0001)
                        axis_use_log = True
                else:
                    x_obs_min = _safe_float(x_vals_bal.min(), 0.0)
                    x_obs_max = _safe_float(x_vals_bal.max(), 1.0)
                    y_obs_min = _safe_float(y_vals_bal.min(), 0.0)
                    y_obs_max = _safe_float(y_vals_bal.max(), 1.0)
                    x_span = max(x_split - x_obs_min, x_obs_max - x_split, 1e-6)
                    y_span = max(y_split - y_obs_min, y_obs_max - y_split, 1e-6)
                    axis_x_min = x_split - x_span
                    axis_x_max = x_split + x_span
                    axis_y_min = y_split - y_span
                    axis_y_max = y_split + y_span
                    axis_use_log = False

        portfolio_app_plot_df = _build_app_plot_df_for_quadrant(
            df_ado_portfolio if isinstance(df_ado_portfolio, pd.DataFrame) else pd.DataFrame(),
            df_cost_src_portfolio if isinstance(df_cost_src_portfolio, pd.DataFrame) else pd.DataFrame(),
            df_explorer_portfolio if isinstance(df_explorer_portfolio, pd.DataFrame) else pd.DataFrame(),
            benefit_mode=benefit_mode,
        )
        strategic_source_df = (
            portfolio_app_plot_df
            if isinstance(portfolio_app_plot_df, pd.DataFrame) and not portfolio_app_plot_df.empty
            else app_plot_df
        )
        if isinstance(strategic_source_df, pd.DataFrame) and not strategic_source_df.empty and "PROGRAMNAME" in strategic_source_df.columns:
            strategic_source_df = strategic_source_df.copy()
            strategic_source_df["PROGRAMNAME"] = (
                strategic_source_df.get("PROGRAMNAME", "")
                .fillna("")
                .astype(str)
                .str.strip()
                .apply(lambda v, _m=program_label_map: _m.get(str(v).strip(), str(v).strip()))
            )
        strategic_params = _compute_quadrant_params(
            strategic_source_df,
            split_mode=split_mode,
            split_percentile=split_percentile,
            roi_target_active=roi_target_active,
            use_log=use_log,
        )

        strategic_ref_axis_bounds = {
            "x_min": _safe_float(strategic_params.get("axis_x_min"), 0.0),
            "x_max": _safe_float(strategic_params.get("axis_x_max"), 1.0),
            "y_min": _safe_float(strategic_params.get("axis_y_min"), 0.0),
            "y_max": _safe_float(strategic_params.get("axis_y_max"), 1.0),
            "x_split": _safe_float(strategic_params.get("x_split"), 0.5),
            "y_split": _safe_float(strategic_params.get("y_split"), 0.5),
            "x_split_ratio": _safe_float(strategic_params.get("x_ratio"), 0.5),
            "y_split_ratio": _safe_float(strategic_params.get("y_ratio"), 0.5),
        }
        app_group_position_df = _build_app_group_positioning_df(
            strategic_source_df,
            float(strategic_ref_axis_bounds["x_split"]),
            float(strategic_ref_axis_bounds["y_split"]),
        )
        if isinstance(app_group_position_df, pd.DataFrame) and not app_group_position_df.empty and "PROGRAMNAME" in app_group_position_df.columns:
            app_group_position_df["PROGRAMNAME"] = (
                app_group_position_df.get("PROGRAMNAME", "")
                .fillna("")
                .astype(str)
                .str.strip()
                .apply(lambda v, _m=program_label_map: _m.get(str(v).strip(), str(v).strip()))
            )
        feature_work_df = _build_feature_work_df(
            df_explorer_portfolio if isinstance(df_explorer_portfolio, pd.DataFrame) else pd.DataFrame()
        )
        if isinstance(feature_work_df, pd.DataFrame) and not feature_work_df.empty and "PROGRAMNAME" in feature_work_df.columns:
            feature_work_df["PROGRAMNAME"] = (
                feature_work_df.get("PROGRAMNAME", "")
                .fillna("")
                .astype(str)
                .str.strip()
                .apply(lambda v, _m=program_label_map: _m.get(str(v).strip(), str(v).strip()))
            )
        strategic_bundle = build_strategic_maturity_bundle(
            app_group_position_df=app_group_position_df,
            feature_work_df=feature_work_df,
            meta={
                "year": int(year),
                "spend_col": "PROJECTED_SPEND",
                "programs": list(portfolio_programs_scope or []),
                "groups": [],
                "portfolio_scope_for_strategic": True,
            },
        )
        program_strategic_df = calculate_program_strategic_maturity(strategic_bundle)
        strategic_drivers_df = build_program_strategic_drivers(program_strategic_df)

        if series:
            series[0]["markLine"] = {
                "silent": True,
                "lineStyle": {"type": "dashed", "width": 2, "color": "#64748b"},
                "data": [{"xAxis": x_split}, {"yAxis": y_split}],
                "label": {"show": False},
            }
            series[0]["markArea"] = {
                "silent": True,
                "data": [
                    [
                        {
                            "name": "Monitor",
                            "xAxis": axis_x_min,
                            "yAxis": axis_y_min,
                            "itemStyle": {"color": "#3b82f6", "opacity": 0.08},
                            "label": {
                                "show": True,
                                "color": "rgba(191,219,254,0.35)",
                                "fontSize": 20,
                                "fontWeight": 700,
                            },
                        },
                        {"xAxis": x_split, "yAxis": y_split},
                    ],
                    [
                        {
                            "name": "Reassess",
                            "xAxis": x_split,
                            "yAxis": axis_y_min,
                            "itemStyle": {"color": "#ef4444", "opacity": 0.08},
                            "label": {
                                "show": True,
                                "color": "rgba(254,202,202,0.35)",
                                "fontSize": 20,
                                "fontWeight": 700,
                            },
                        },
                        {"xAxis": axis_x_max, "yAxis": y_split},
                    ],
                    [
                        {
                            "name": "Invest",
                            "xAxis": axis_x_min,
                            "yAxis": y_split,
                            "itemStyle": {"color": "#22c55e", "opacity": 0.08},
                            "label": {
                                "show": True,
                                "color": "rgba(134,239,172,0.35)",
                                "fontSize": 20,
                                "fontWeight": 700,
                            },
                        },
                        {"xAxis": x_split, "yAxis": axis_y_max},
                    ],
                    [
                        {
                            "name": "Optimize",
                            "xAxis": x_split,
                            "yAxis": y_split,
                            "itemStyle": {"color": "#f59e0b", "opacity": 0.08},
                            "label": {
                                "show": True,
                                "color": "rgba(253,230,138,0.35)",
                                "fontSize": 20,
                                "fontWeight": 700,
                            },
                        },
                        {"xAxis": axis_x_max, "yAxis": axis_y_max},
                    ],
                ],
                "label": {"show": True, "position": "inside", "formatter": "{b}"},
                "z": 0,
            }
    
        option = {
            "legend": {"show": False},
            "tooltip": {"trigger": "item"},
            "grid": {"left": "7%", "right": "7%", "top": 50, "bottom": 30, "containLabel": True},
            "xAxis": {
                "type": "log" if axis_use_log else "value",
                "name": demand_axis_label,
                "axisLabel": {"formatter": "{value}", "margin": 24, "show": True, "showMinLabel": False, "showMaxLabel": True},
                "axisTick": {"show": True},
                "axisLine": {"show": True},
                "nameGap": 36,
                "min": axis_x_min,
                "max": axis_x_max,
            },
            "yAxis": {
                "type": "log" if axis_use_log else "value",
                "name": y_axis_label,
                "axisLabel": {"formatter": "{value}"},
                "min": axis_y_min,
                "max": axis_y_max,
            },
            "series": series,
            "dataZoom": [{"type": "inside"}],
            "animationDuration": 600,
        }
    
        chart_tab, drill_tab = st.tabs(["Chart", "Drilldown"])
        with chart_tab:
            chart_col, control_col = st.columns([0.8, 0.2], gap="medium")
            with chart_col:
                if not series:
                    st.info(quadrant_empty_reason or "No valid data points available for the selected filters.")
                else:
                    chart_key = (
                        "preview_insights_quadrants__"
                        f"benefit={_benefit_mode_key(benefit_mode)}__"
                        f"roi_target={float(roi_target_active):.1f}"
                        f"__split={str(st.session_state.get('insights_quadrant_split_mode', 'Balanced (equal-size)'))}"
                        f"__q={int(st.session_state.get('insights_quadrant_split_percentile', 50) or 50)}"
                        f"__show={str(st.session_state.get('insights_cost_show_scope', show_scope))}"
                    )
                    render_echart(option, height="740px", key=chart_key, page_theme=page_theme)
    
            with control_col:
                benefit_mode = st.radio(
                    "Benefit estimate",
                    ["Mid (default)", "Low", "High"],
                    horizontal=False,
                    key="preview_benefit_mode",
                )
                roi_target_active = st.slider(
                    "ROI Target (What-if, baseline = 1.0)",
                    min_value=0.5,
                    max_value=2.0,
                    step=0.1,
                    value=ROI_TARGET_BASELINE,
                    key="roi_target_active",
                )
                st.radio(
                    "Quadrant split",
                    ["Balanced (equal-size)", "Data-driven"],
                    key="insights_quadrant_split_mode",
                )
                if str(st.session_state.get("insights_quadrant_split_mode", "Balanced (equal-size)")) == "Data-driven":
                    st.slider(
                        "Split percentile",
                        min_value=30,
                        max_value=70,
                        step=5,
                        value=int(st.session_state.get("insights_quadrant_split_percentile", 50) or 50),
                        key="insights_quadrant_split_percentile",
                    )
                st.selectbox(
                    "Show",
                    options=list(SHOW_SCOPE_OPTIONS),
                    key="insights_cost_show_scope",
                    help="Controls bubble aggregation level for Cost vs Value vs Demand.",
                )
                roi_target_active = float(st.session_state.get("roi_target_active", ROI_TARGET_BASELINE))
                scenario_active = abs(roi_target_active - ROI_TARGET_BASELINE) > 1e-9
                with st.popover("What this chart shows"):
                    st.markdown("**What this chart shows**")
                    st.markdown(
                        "\n".join(
                            [
                                "- ROI proxy = **ValueUSD / CostUSD** (ValueUSD is derived from BV via your Low/Mid/High mapping).",
                                f"- Baseline thresholds (governance): ROI target = **{ROI_TARGET_BASELINE:.1f}x**, cost threshold = **${COST_THRESHOLD_BASELINE:,.0f}**.",
                                "- Balanced split: equal-size visual quadrants with data-driven split placement.",
                                "- Data-driven split: percentile split + ROI target adaptation on Y.",
                                "- What-if mode changes ROI target used for Data-driven classification (session-only; not saved).",
                            ]
                        )
                    )
                    st.markdown("**Quadrants (ROI view)**")
                    st.markdown(
                        "\n".join(
                            [
                                "- Invest = high ROI, low demand.",
                                "- Optimize = high ROI, high demand.",
                                "- Monitor = low ROI, low demand.",
                                "- Reassess = low ROI, high demand.",
                            ]
                        )
                    )
    
        with drill_tab:
            entity_list = sorted(
                {
                    str(v).strip()
                    for v in plot_df.get("ENTITY_LABEL", pd.Series(dtype=str)).dropna().astype(str).tolist()
                    if str(v).strip()
                }
            )
            if not entity_list:
                st.info(f"No {show_scope.lower()} available for drilldown.")
            else:
                entity_term = {
                    "Programs": "program",
                    "Teams": "team",
                    "Applications": "application",
                    "Initiatives": "initiative",
                }.get(show_scope, "entity")
                selected_entity = st.selectbox(f"Select a {entity_term} to view details", entity_list)

                if show_scope == "Applications":
                    selected_group = selected_entity
                    sel_features = ado_curr[ado_curr["GROUPNAME"] == selected_group] if not ado_curr.empty else pd.DataFrame()
                    if df_explorer is not None and not df_explorer.empty and not sel_features.empty:
                        expl = df_explorer.copy()
                        expl["FEATURE_ID"] = pd.to_numeric(expl.get("FEATURE_ID"), errors="coerce")
                        expl["ADO_YEAR"] = pd.to_numeric(expl.get("ADO_YEAR"), errors="coerce")
                        expl["PI_NUM"] = pd.to_numeric(expl.get("PI_NUM"), errors="coerce")
                        expl["DERIVED_FTE_EXPLORER"] = pd.to_numeric(expl.get("DERIVED_FTE_EXPLORER"), errors="coerce")
                        sf = sel_features.copy()
                        sf["FEATURE_ID"] = pd.to_numeric(sf.get("FEATURE_ID"), errors="coerce")
                        sf["ADO_YEAR"] = pd.to_numeric(sf.get("ADO_YEAR"), errors="coerce")
                        sf["PI_NUM"] = pd.to_numeric(sf.get("ITERATION_NUM"), errors="coerce")
                        sf = sf.merge(
                            expl[["FEATURE_ID", "ADO_YEAR", "PI_NUM", "DERIVED_FTE_EXPLORER"]].dropna(
                                subset=["FEATURE_ID", "ADO_YEAR", "PI_NUM"]
                            ),
                            on=["FEATURE_ID", "ADO_YEAR", "PI_NUM"],
                            how="left",
                        )
                        sel_features = sf
                    sel_cost = cost_curr[cost_curr["GROUPNAME"] == selected_group] if not cost_curr.empty else pd.DataFrame()

                    st.markdown("**ADO features**")
                    if sel_features.empty:
                        st.info("No ADO features mapped to this group.")
                    else:
                        show_cols = [
                            "FEATURE_ID",
                            "TITLE",
                            "STATE",
                            "TEAMNAME",
                            "DERIVED_FTE_EXPLORER",
                            "BUSINESS_VALUE",
                        ]
                        avail_cols = [c for c in show_cols if c in sel_features.columns]
                        st.dataframe(sel_features[avail_cols], use_container_width=True, hide_index=True, height=260)

                    st.markdown("**Cost summary**")
                    if sel_cost is None or sel_cost.empty:
                        st.info("No costs mapped to this group.")
                    else:
                        cost_summary = (
                            sel_cost.groupby("PI", dropna=False)["AMOUNT"]
                            .sum()
                            .reset_index()
                            .sort_values("AMOUNT", ascending=False)
                        )
                        st.dataframe(cost_summary, use_container_width=True, hide_index=True, height=220)

                elif show_scope == "Initiatives":
                    detail = initiative_detail_df.copy() if isinstance(initiative_detail_df, pd.DataFrame) else pd.DataFrame()
                    detail = detail[detail.get("ENTITY_LABEL", "").astype(str).str.strip().eq(selected_entity)].copy() if not detail.empty else detail
                    if detail.empty:
                        st.info("No epic/feature allocation detail available for this initiative.")
                    else:
                        show_cols = [
                            "EPIC_ID",
                            "EPIC_TITLE",
                            "FEATURE_ID",
                            "FEATURE_TITLE",
                            "PROGRAMNAME",
                            "TEAMNAME",
                            "GROUPNAME",
                            "DEMAND_FTE",
                            "BENEFIT_SELECTED",
                            "ALLOCATED_COST",
                            "ROI_PROXY",
                        ]
                        avail_cols = [c for c in show_cols if c in detail.columns]
                        st.markdown("**Initiative feature allocation detail**")
                        st.dataframe(detail[avail_cols], use_container_width=True, hide_index=True, height=320)

                elif show_scope == "Teams":
                    apps = app_plot_df[app_plot_df.get("TEAMNAME", "").astype(str).str.strip().eq(selected_entity)].copy()
                    st.markdown("**Applications in selected team**")
                    if apps.empty:
                        st.info("No applications available for this team.")
                    else:
                        show_cols = ["GROUPNAME", "PROGRAMNAME", "ACTUAL_COST", "DEMAND_AXIS", "Y_VALUE_PLOT", "BENEFITS_SELECTED"]
                        st.dataframe(apps[[c for c in show_cols if c in apps.columns]], use_container_width=True, hide_index=True, height=300)

                else:
                    apps = app_plot_df[app_plot_df.get("PROGRAMNAME", "").astype(str).str.strip().eq(selected_entity)].copy()
                    st.markdown("**Applications in selected program**")
                    if apps.empty:
                        st.info("No applications available for this program.")
                    else:
                        show_cols = ["GROUPNAME", "TEAMNAME", "ACTUAL_COST", "DEMAND_AXIS", "Y_VALUE_PLOT", "BENEFITS_SELECTED"]
                        st.dataframe(apps[[c for c in show_cols if c in apps.columns]], use_container_width=True, hide_index=True, height=300)

with strategic_maturity_tab_slot:
    _section_title(
        "Program Maturity",
        "Strategic positioning lens derived from Cost vs Value vs Demand app-group signals.",
    )
    st.caption(
        "Program Maturity reflects how strategically healthy the program's investment mix is, based on Cost vs Value vs Demand positioning of its applications."
    )
    st.caption("This tab always uses portfolio scope (it ignores Program/Application filters on this page).")

    spdf = program_strategic_df.copy() if isinstance(program_strategic_df, pd.DataFrame) else pd.DataFrame()
    if spdf.empty:
        st.info("No strategic positioning maturity data available for portfolio scope.")
    else:
        for col in [
            "weighted_demand_avg",
            "weighted_value_avg",
            "total_spend",
            "strategic_maturity_score",
            "positioning_score",
            "roi_per_fte",
            "roi_efficiency_score",
            "workforce_alignment_score",
            "invest_pct",
            "optimize_pct",
            "monitor_pct",
            "reassess_pct",
            "delivery_newopp_pct",
            "delivery_operating_pct",
            "delivery_prodhealth_pct",
            "msp_operating_pct",
            "msp_newopp_pct",
            "msp_prodhealth_pct",
        ]:
            spdf[col] = pd.to_numeric(spdf.get(col), errors="coerce").fillna(0.0)

        top_left, top_right = st.columns([0.75, 0.25], gap="large")

        with top_left:
            spend_lo = float(spdf["total_spend"].min()) if not spdf.empty else 0.0
            spend_hi = float(spdf["total_spend"].max()) if not spdf.empty else 0.0
            program_names = sorted({str(v).strip() for v in spdf.get("program", pd.Series(dtype=str)).tolist() if str(v).strip()})
            shade_steps = [1.0, 0.86, 1.14, 0.74, 1.26]
            program_color_map = {}
            for i, pname in enumerate(program_names):
                base = APP_COLORS[i % len(APP_COLORS)]
                shade = shade_steps[(i // max(len(APP_COLORS), 1)) % len(shade_steps)]
                program_color_map[pname] = _shade_color(base, shade)

            def _prog_bubble_size(v: float) -> float:
                if spend_hi <= 0:
                    return 18.0
                norm = (math.sqrt(max(v, 0.0)) - math.sqrt(max(spend_lo, 0.0))) / (
                    math.sqrt(max(spend_hi, 0.0)) - math.sqrt(max(spend_lo, 0.0)) + 1e-9
                )
                return max(12.0, min(70.0, 12.0 + norm * 58.0))

            prog_points = []
            for _, row in spdf.iterrows():
                program = str(row.get("program") or "").strip()
                if not program:
                    continue
                weighted_demand = float(row.get("weighted_demand_avg") or 0.0)
                weighted_value = float(row.get("weighted_value_avg") or 0.0)
                total_spend = float(row.get("total_spend") or 0.0)
                strategic_score = float(row.get("strategic_maturity_score") or 0.0)
                roi_per_fte = float(row.get("roi_per_fte") or 0.0)
                delivery_newopp = float(row.get("delivery_newopp_pct") or 0.0)
                msp_operating = float(row.get("msp_operating_pct") or 0.0)
                invest_pct = float(row.get("invest_pct") or 0.0)
                optimize_pct = float(row.get("optimize_pct") or 0.0)
                monitor_pct = float(row.get("monitor_pct") or 0.0)
                reassess_pct = float(row.get("reassess_pct") or 0.0)
                tooltip_html = "<br/>".join(
                    [
                        f"<b>{html.escape(program)}</b>",
                        f"Strategic maturity: {strategic_score:.1f}",
                        f"ROI per FTE: {roi_per_fte:.3f}",
                        f"Delivery New Opp: {delivery_newopp:.1f}%",
                        f"MSP Operating: {msp_operating:.1f}%",
                        (
                            "Grow/Scale/Watch/Stop: "
                            f"{invest_pct:.1f}% / {optimize_pct:.1f}% / {monitor_pct:.1f}% / {reassess_pct:.1f}%"
                        ),
                        f"Spend: {_fmt_money(total_spend, 0)}",
                    ]
                )
                prog_points.append(
                    {
                        "name": program,
                        "value": [
                            weighted_demand,
                            weighted_value,
                            total_spend,
                            strategic_score,
                            roi_per_fte,
                            delivery_newopp,
                            msp_operating,
                            invest_pct,
                            optimize_pct,
                            monitor_pct,
                            reassess_pct,
                        ],
                        "symbolSize": _prog_bubble_size(total_spend),
                        "itemStyle": {"color": program_color_map.get(program, APP_COLORS[0])},
                        "tooltip": {"formatter": tooltip_html},
                    }
                )

            x_min = 0.0
            y_min = 0.0
            x_obs_max = float(spdf["weighted_demand_avg"].max()) if not spdf.empty else 0.0
            y_obs_max = float(spdf["weighted_value_avg"].max()) if not spdf.empty else 0.0
            x_max = _nice_linear_max(x_obs_max, min_value=1.0, padding=0.05)
            # Use program-level ROI distribution for Y bounds to avoid compression caused by app-level outliers.
            y_max = _nice_linear_max(y_obs_max, min_value=max(1.0, float(roi_target_active) * 1.2), padding=0.08)
            x_vals_prog = pd.to_numeric(spdf.get("weighted_demand_avg"), errors="coerce").dropna()
            y_vals_prog = pd.to_numeric(spdf.get("weighted_value_avg"), errors="coerce").dropna()
            x_split_prog = _safe_float(x_vals_prog.median(), (x_min + x_max) / 2.0) if not x_vals_prog.empty else (x_min + x_max) / 2.0
            if not x_vals_prog.empty:
                q_prog = float(split_percentile) / 100.0
                x_split_prog = _safe_float(x_vals_prog.quantile(q_prog), x_split_prog)
            y_med_prog = _safe_float(y_vals_prog.median(), (y_min + y_max) / 2.0) if not y_vals_prog.empty else (y_min + y_max) / 2.0
            y_split_prog = _adaptive_target_split(y_vals_prog, median_value=y_med_prog, target_value=roi_target_active) if not y_vals_prog.empty else y_med_prog

            if split_mode == "Balanced (equal-size)":
                x_obs_min = _safe_float(x_vals_prog.min(), x_min) if not x_vals_prog.empty else x_min
                x_obs_max = _safe_float(x_vals_prog.max(), x_max) if not x_vals_prog.empty else x_max
                y_obs_min = _safe_float(y_vals_prog.min(), y_min) if not y_vals_prog.empty else y_min
                y_obs_max = _safe_float(y_vals_prog.max(), y_max) if not y_vals_prog.empty else y_max
                x_span = max(
                    x_split_prog - x_obs_min,
                    x_obs_max - x_split_prog,
                    max(abs(x_split_prog) * 0.1, 1e-6),
                )
                y_span = max(
                    y_split_prog - y_obs_min,
                    y_obs_max - y_split_prog,
                    max(abs(y_split_prog) * 0.1, 1e-6),
                )
                x_min = x_split_prog - x_span
                x_max = x_split_prog + x_span
                y_min = y_split_prog - y_span
                y_max = y_split_prog + y_span
            x_split_prog = min(max(x_split_prog, x_min), x_max)
            y_split_prog = min(max(y_split_prog, y_min), y_max)

            quad_theme = build_quadrant_theme(
                x_min=x_min,
                x_split=x_split_prog,
                x_max=x_max,
                y_min=y_min,
                y_split=y_split_prog,
                y_max=y_max,
                labels=STRATEGIC_QUADRANT_DISPLAY,
                colors=STRATEGIC_QUADRANT_COLORS,
                line_color="#64748b",
                line_width=2,
                area_opacity=0.08,
                label_position="inside",
                label_font_size=20,
                label_font_weight=700,
            )

            # BEFORE: Program chart used its own markArea/markLine config and drifted from Cost vs Value.
            # AFTER: Program chart reuses the same quadrant grammar (split lines, regions, palette, bounds).
            option_prog = {
                "tooltip": {"trigger": "item"},
                "grid": {"left": "7%", "right": 100, "top": 50, "bottom": 30, "containLabel": True},
                "xAxis": {"type": "value", "name": "Weighted Demand", "min": x_min, "max": x_max},
                "yAxis": {"type": "value", "name": "Weighted Value (ROI Proxy)", "min": y_min, "max": y_max},
                "legend": {"show": False},
                "series": [
                    {
                        "type": "scatter",
                        "data": prog_points,
                        "markLine": quad_theme.get("markLine"),
                        "markArea": quad_theme.get("markArea"),
                    }
                ],
            }
            strategic_click = render_echart(
                option_prog,
                height="540px",
                key=(
                    "insights_program_positioning_quadrant_"
                    f"{int(year)}__split={split_mode}__p={int(split_percentile)}__n={len(prog_points)}"
                ),
                page_theme=page_theme,
                events={"click": "function(params){ return { program: params.name }; }"},
            )
            st.caption("Bubble color represents program (not heatmap).")
            if isinstance(strategic_click, dict):
                picked = str(strategic_click.get("program") or strategic_click.get("name") or "").strip()
                if picked:
                    st.session_state["insights_positioning_selected_program"] = picked

        with top_right:
            with st.container(border=True):
                st.markdown("**What is Program Maturity?**")
                st.caption(
                    "A strategic lens over program portfolio mix, combining quadrant positioning, value efficiency, and team-work alignment."
                )
                with st.expander("How it's calculated", expanded=False):
                    st.markdown(
                        "\n".join(
                            [
                                "- Quadrant Positioning (40%): spend-weighted mix across Grow/Scale/Watch/Stop.",
                                "- ROI per FTE (30%): value proxy efficiency normalized against portfolio peers.",
                                "- Team-work Alignment (30%): Delivery/MSP demand split by investment dimension.",
                            ]
                        )
                    )
            if is_debug_enabled():
                with st.expander("Debug – Strategic spend basis", expanded=False):
                    st.caption("Strategic spend basis: PROJECTED_SPEND.")
                    st.caption("PROJECTED_SPEND is sourced from Cost vs Value projected plotting cost column.")

        prog_opts = sorted({str(v).strip() for v in spdf.get("program", pd.Series(dtype=str)).tolist() if str(v).strip()})
        selected_prog = str(st.session_state.get("insights_positioning_selected_program") or "").strip()
        if selected_prog not in set(prog_opts):
            selected_prog = ""

        if not selected_prog:
            st.caption("Click a bubble to drill down, or use manual selection below.")
            top_c, low_c = st.columns(2, gap="large")
            with top_c:
                st.markdown("**Top Mature**")
                top_df = spdf.sort_values(["strategic_maturity_score", "total_spend"], ascending=[False, False]).head(5).copy()
                top_df = top_df.rename(columns={"invest_pct": "grow_pct", "optimize_pct": "scale_pct"})
                st.dataframe(top_df[["program", "strategic_maturity_score", "grow_pct", "scale_pct", "total_spend"]], use_container_width=True, hide_index=True)
            with low_c:
                st.markdown("**Needs Attention**")
                low_df = spdf.sort_values(["strategic_maturity_score", "total_spend"], ascending=[True, False]).head(5).copy()
                low_df = low_df.rename(columns={"monitor_pct": "watch_pct", "reassess_pct": "stop_pct"})
                st.dataframe(low_df[["program", "strategic_maturity_score", "watch_pct", "stop_pct", "total_spend"]], use_container_width=True, hide_index=True)
            sel_manual = (
                st.selectbox(
                    "Select program manually",
                    options=[""] + prog_opts,
                    index=0,
                    key="insights_positioning_program_manual",
                )
                if prog_opts
                else ""
            )
            if sel_manual:
                selected_prog = str(sel_manual).strip()
                st.session_state["insights_positioning_selected_program"] = selected_prog

        if selected_prog:
            sel_row = spdf.loc[spdf["program"].astype(str).str.strip().eq(selected_prog)].head(1)
            if not sel_row.empty:
                s = sel_row.iloc[0]
                shares_disp = _strategic_display_shares(s)
                chip_cols = st.columns(4, gap="small")
                chip_cols[0].metric("Grow", f"{shares_disp['Grow']:.1f}%")
                chip_cols[1].metric("Scale", f"{shares_disp['Scale']:.1f}%")
                chip_cols[2].metric("Watch", f"{shares_disp['Watch']:.1f}%")
                chip_cols[3].metric("Stop", f"{shares_disp['Stop']:.1f}%")
                st.caption(
                    f"Delivery -> New Opp: {float(s.get('delivery_newopp_pct') or 0.0):.1f}% | "
                    f"MSP -> Operating: {float(s.get('msp_operating_pct') or 0.0):.1f}%"
                )

                left_col, right_col = st.columns([0.66, 0.34], gap="large")
                with left_col:
                    ag = app_group_position_df.copy() if isinstance(app_group_position_df, pd.DataFrame) else pd.DataFrame()
                    ag = ag[ag.get("PROGRAMNAME", "").astype(str).str.strip().eq(selected_prog)].copy() if not ag.empty else ag
                    if ag.empty:
                        st.info("No app-group composition data available for this program.")
                    else:
                        total_spend = float(pd.to_numeric(ag.get("PROJECTED_SPEND"), errors="coerce").fillna(0.0).sum())
                        by_q = ag.groupby("quadrant_label", dropna=False)["PROJECTED_SPEND"].sum()
                        shares = {
                            "Grow": (float(by_q.get("Invest", 0.0)) / total_spend * 100.0) if total_spend > 0 else 0.0,
                            "Scale": (float(by_q.get("Optimize", 0.0)) / total_spend * 100.0) if total_spend > 0 else 0.0,
                            "Watch": (float(by_q.get("Monitor", 0.0)) / total_spend * 100.0) if total_spend > 0 else 0.0,
                            "Stop": (float(by_q.get("Reassess", 0.0)) / total_spend * 100.0) if total_spend > 0 else 0.0,
                        }
                        option_stack_spend = {
                            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
                            "legend": {"bottom": 0},
                            "grid": {"left": 55, "right": 15, "top": 20, "bottom": 45, "containLabel": True},
                            "xAxis": {"type": "value", "max": 100, "name": "% spend"},
                            "yAxis": {"type": "category", "data": [selected_prog]},
                            "series": [
                                {"name": "Grow", "type": "bar", "stack": "total", "data": [shares["Grow"]], "itemStyle": {"color": STRATEGIC_QUADRANT_COLORS["Invest"]}},
                                {"name": "Scale", "type": "bar", "stack": "total", "data": [shares["Scale"]], "itemStyle": {"color": STRATEGIC_QUADRANT_COLORS["Optimize"]}},
                                {"name": "Watch", "type": "bar", "stack": "total", "data": [shares["Watch"]], "itemStyle": {"color": STRATEGIC_QUADRANT_COLORS["Monitor"]}},
                                {"name": "Stop", "type": "bar", "stack": "total", "data": [shares["Stop"]], "itemStyle": {"color": STRATEGIC_QUADRANT_COLORS["Reassess"]}},
                            ],
                        }
                        render_echart(
                            option_stack_spend,
                            height="250px",
                            key=f"insights_positioning_mix_{int(year)}_{selected_prog}",
                            page_theme=page_theme,
                        )

                    fw = feature_work_df.copy() if isinstance(feature_work_df, pd.DataFrame) else pd.DataFrame()
                    fw = fw[fw.get("PROGRAMNAME", "").astype(str).str.strip().eq(selected_prog)].copy() if not fw.empty else fw
                    if fw.empty:
                        st.info("No feature demand alignment data available for this program.")
                    else:
                        fw["TEAM_FAMILY"] = pd.to_numeric(fw.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int).map({1: "MSP", 0: "Delivery"})
                        fw["INVESTMENT_DIMENSION_NORM"] = fw.get("INVESTMENT_DIMENSION", "").fillna("").astype(str).str.strip()
                        fw["INVESTMENT_DIMENSION_NORM"] = fw["INVESTMENT_DIMENSION_NORM"].apply(
                            lambda v: (
                                "New Opportunities"
                                if ("new" in str(v).lower() and "opp" in str(v).lower())
                                else ("Operating" if "operat" in str(v).lower() else ("Product Health" if "health" in str(v).lower() else "Other"))
                            )
                        )
                        fw["DEMAND_FTE"] = pd.to_numeric(fw.get("DEMAND_FTE"), errors="coerce").fillna(0.0)
                        grp = (
                            fw.groupby(["TEAM_FAMILY", "INVESTMENT_DIMENSION_NORM"], dropna=False)["DEMAND_FTE"]
                            .sum()
                            .reset_index()
                        )
                        team_tot = grp.groupby("TEAM_FAMILY", dropna=False)["DEMAND_FTE"].sum().to_dict()
                        for dim in ["New Opportunities", "Product Health", "Operating", "Other"]:
                            mask = grp["INVESTMENT_DIMENSION_NORM"].eq(dim)
                            grp.loc[mask, "PCT"] = grp.loc[mask].apply(
                                lambda r: (float(r["DEMAND_FTE"]) / float(team_tot.get(str(r["TEAM_FAMILY"]), 0.0)) * 100.0)
                                if float(team_tot.get(str(r["TEAM_FAMILY"]), 0.0)) > 0
                                else 0.0,
                                axis=1,
                            )
                        rows_map = {
                            fam: {
                                dim: float(
                                    grp.loc[
                                        grp["TEAM_FAMILY"].eq(fam) & grp["INVESTMENT_DIMENSION_NORM"].eq(dim),
                                        "PCT",
                                    ].sum()
                                )
                                for dim in ["New Opportunities", "Product Health", "Operating", "Other"]
                            }
                            for fam in ["Delivery", "MSP"]
                        }
                        option_align = {
                            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
                            "legend": {"bottom": 0},
                            "grid": {"left": 60, "right": 20, "top": 20, "bottom": 50, "containLabel": True},
                            "xAxis": {"type": "value", "max": 100, "name": "% demand FTE"},
                            "yAxis": {"type": "category", "data": ["Delivery", "MSP"]},
                            "series": [
                                {"name": "New Opportunities", "type": "bar", "stack": "demand", "data": [rows_map["Delivery"]["New Opportunities"], rows_map["MSP"]["New Opportunities"]], "itemStyle": {"color": "#22c55e"}},
                                {"name": "Product Health", "type": "bar", "stack": "demand", "data": [rows_map["Delivery"]["Product Health"], rows_map["MSP"]["Product Health"]], "itemStyle": {"color": "#3b82f6"}},
                                {"name": "Operating", "type": "bar", "stack": "demand", "data": [rows_map["Delivery"]["Operating"], rows_map["MSP"]["Operating"]], "itemStyle": {"color": "#f59e0b"}},
                                {"name": "Other", "type": "bar", "stack": "demand", "data": [rows_map["Delivery"]["Other"], rows_map["MSP"]["Other"]], "itemStyle": {"color": "#94a3b8"}},
                            ],
                        }
                        render_echart(
                            option_align,
                            height="250px",
                            key=f"insights_positioning_alignment_{int(year)}_{selected_prog}",
                            page_theme=page_theme,
                        )

                with right_col:
                    driver_row = (
                        strategic_drivers_df.loc[strategic_drivers_df["program"].astype(str).str.strip().eq(selected_prog)].head(1)
                        if isinstance(strategic_drivers_df, pd.DataFrame) and not strategic_drivers_df.empty
                        else pd.DataFrame()
                    )
                    portfolio_avg = float(spdf["strategic_maturity_score"].mean()) if not spdf.empty else 0.0
                    st.metric("Strategic Maturity Score", f"{float(s.get('strategic_maturity_score') or 0.0):.1f}")
                    st.caption(f"Portfolio Average Maturity = {portfolio_avg:.1f}")
                    st.metric("Quadrant Positioning", f"{float(s.get('positioning_score') or 0.0):.1f}")
                    st.metric("ROI Efficiency", f"{float(s.get('roi_efficiency_score') or 0.0):.1f}")
                    st.metric("Workforce Alignment", f"{float(s.get('workforce_alignment_score') or 0.0):.1f}")
                    st.metric("ROI per FTE", f"{float(s.get('roi_per_fte') or 0.0):.3f}")

                    option_radar = {
                        "tooltip": {},
                        "radar": {
                            "indicator": [
                                {"name": "Strategic", "max": 100},
                                {"name": "Positioning", "max": 100},
                                {"name": "ROI Efficiency", "max": 100},
                                {"name": "Workforce Align.", "max": 100},
                            ]
                        },
                        "series": [
                            {
                                "type": "radar",
                                "data": [
                                    {
                                        "value": [
                                            float(s.get("strategic_maturity_score") or 0.0),
                                            float(s.get("positioning_score") or 0.0),
                                            float(s.get("roi_efficiency_score") or 0.0),
                                            float(s.get("workforce_alignment_score") or 0.0),
                                        ],
                                        "name": selected_prog,
                                    }
                                ],
                                "areaStyle": {"opacity": 0.2},
                            }
                        ],
                    }
                    render_echart(
                        option_radar,
                        height="240px",
                        key=f"insights_positioning_radar_{int(year)}_{selected_prog}",
                        page_theme=page_theme,
                    )

                    why_lines: list[str] = []
                    if float(s.get("delivery_operating_pct") or 0.0) > 25.0:
                        why_lines.append(
                            f"Delivery is spending {float(s.get('delivery_operating_pct') or 0.0):.1f}% of FTE on Operating (target < 25%)."
                        )
                    if float(s.get("msp_newopp_pct") or 0.0) > 10.0:
                        why_lines.append(
                            f"MSP is spending {float(s.get('msp_newopp_pct') or 0.0):.1f}% of FTE on New Opportunities (target < 10%)."
                        )
                    if float(s.get("roi_efficiency_score") or 0.0) < (float(spdf["roi_efficiency_score"].median()) - 10.0):
                        why_lines.append("ROI per FTE is below the portfolio median band.")
                    if why_lines:
                        st.markdown("**Why this score?**")
                        for line in why_lines:
                            st.caption(f"- {line}")
                    if not driver_row.empty:
                        st.caption(f"Driver: {driver_row.iloc[0].get('biggest_driver', '')}")
                        st.caption(f"Recommendation: {driver_row.iloc[0].get('quick_recommendation', '')}")

        st.markdown("**Top programs to improve**")
        improve = spdf.sort_values(["strategic_maturity_score", "total_spend"], ascending=[True, False]).copy()
        if isinstance(strategic_drivers_df, pd.DataFrame) and not strategic_drivers_df.empty:
            improve = improve.merge(strategic_drivers_df, on="program", how="left")
        show_cols = [c for c in ["program", "strategic_maturity_score", "biggest_driver", "quick_recommendation"] if c in improve.columns]
        st.dataframe(improve[show_cols].head(10), use_container_width=True, hide_index=True)

with nwf_tab_slot:
    if str(insights_render_mode) != "Full":
        st.caption("Summary mode: detailed NWF sections are skipped. Switch Render mode to Full to load them.")
        _set_loading("Finalizing view...", 5)
        global_loading_ph.empty()
        show_perf_panel(page_perf_key)
        st.stop()

    _section_title(
        "NWF Baseline vs Actual by Cost Type",
        "Program-level comparison of planned (Baseline) vs actual (Apptio) non-workforce spend. "
        "Actuals are program-level only (not attributable to app groups). "
        "Buckets are aggregated across the selected year.",
    )
    sem = echarts_semantic_colors(_merged_theme())
    bucket_order = ["Invoices", "Cloud Azure", "Cloud AWS", "MSP", "Contractor CS", "Travel", "Other"]
    bucket_colors = {
        "Baseline": sem["baseline"],
        "Actual": sem["actual"],
    }
    baseline_nwf_tab = baseline_nwf.copy() if isinstance(baseline_nwf, pd.DataFrame) else pd.DataFrame()
    if not baseline_nwf_tab.empty and "PROGRAMNAME" in baseline_nwf_tab.columns:
        baseline_nwf_tab["PROGRAMNAME"] = (
            baseline_nwf_tab.get("PROGRAMNAME", "")
            .fillna("")
            .astype(str)
            .str.strip()
            .apply(lambda v, _m=program_label_map: _m.get(str(v).strip(), str(v).strip()))
        )

    apptio_for_nwf = apptio_df.copy() if isinstance(apptio_df, pd.DataFrame) and not apptio_df.empty else pd.DataFrame()
    if apptio_for_nwf.empty and isinstance(actual_monthly_raw, pd.DataFrame) and not actual_monthly_raw.empty:
        apptio_for_nwf = actual_monthly_raw.copy()
    if not apptio_for_nwf.empty and "PROGRAMNAME" in apptio_for_nwf.columns:
        apptio_for_nwf["PROGRAMNAME"] = (
            apptio_for_nwf.get("PROGRAMNAME", "")
            .fillna("")
            .astype(str)
            .str.strip()
            .apply(lambda v, _m=program_label_map: _m.get(str(v).strip(), str(v).strip()))
        )

    baseline_rows = 0
    baseline_total = 0.0
    baseline_bucket = pd.DataFrame(columns=["BUCKET", "BASELINE"])
    if isinstance(baseline_nwf_tab, pd.DataFrame) and not baseline_nwf_tab.empty:
        baseline_rows = int(baseline_nwf_tab.shape[0])
        baseline_nwf_tab = baseline_nwf_tab.copy()
        baseline_nwf_tab["AMOUNT"] = pd.to_numeric(baseline_nwf_tab.get("AMOUNT"), errors="coerce").fillna(0.0)
        baseline_total = float(baseline_nwf_tab["AMOUNT"].sum() or 0.0)
        col_candidates = ["SUBCOMPONENT_ROLLUP", "SUBCOMPONENT", "COST_BUCKET", "SOD_LAYER2"]
        text_col = next((c for c in col_candidates if c in baseline_nwf_tab.columns), None)
        if text_col:
            baseline_nwf_tab["BUCKET"] = baseline_nwf_tab[text_col].apply(_nwf_bucket_from_text)
        else:
            baseline_nwf_tab["BUCKET"] = "Other"
        baseline_bucket = (
            baseline_nwf_tab.groupby("BUCKET", dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "BASELINE"})
        )
    
    actual_rows = 0
    actual_total = 0.0
    actual_bucket = pd.DataFrame(columns=["BUCKET", "ACTUAL"])
    if isinstance(apptio_for_nwf, pd.DataFrame) and not apptio_for_nwf.empty:
        actual_rows = int(apptio_for_nwf.shape[0])
        w = apptio_for_nwf.copy()
        w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)
        actual_total = float(w["AMOUNT"].sum() or 0.0)
        if "EFFECTIVE_NWF_TYPE" in w.columns:
            w["BUCKET"] = w["EFFECTIVE_NWF_TYPE"].apply(_nwf_bucket_from_text)
        elif "SUBTYPE" in w.columns:
            w["BUCKET"] = w["SUBTYPE"].apply(_nwf_bucket_from_text)
        else:
            w["BUCKET"] = w.get("COST_TYPE", "").apply(_nwf_bucket_from_text)
        actual_bucket = (
            w.groupby("BUCKET", dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "ACTUAL"})
        )
    
    merged = baseline_bucket.merge(actual_bucket, on="BUCKET", how="outer")
    merged["BASELINE"] = pd.to_numeric(merged.get("BASELINE"), errors="coerce").fillna(0.0)
    merged["ACTUAL"] = pd.to_numeric(merged.get("ACTUAL"), errors="coerce").fillna(0.0)
    merged["BUCKET"] = merged["BUCKET"].where(merged["BUCKET"].isin(bucket_order), "Other")
    merged = merged.groupby("BUCKET", dropna=False)[["BASELINE", "ACTUAL"]].sum().reset_index()
    merged["VARIANCE"] = merged["ACTUAL"] - merged["BASELINE"]
    merged["VAR_PCT"] = merged.apply(
        lambda r: (r["VARIANCE"] / r["BASELINE"] * 100.0) if r["BASELINE"] > 0 else None, axis=1
    )
    merged["ABS_VARIANCE"] = merged["VARIANCE"].abs()
    merged = merged[(merged["BASELINE"] != 0) | (merged["ACTUAL"] != 0)].copy()
    merged = merged.sort_values("ABS_VARIANCE", ascending=False)
    
    if merged.empty:
        st.info("No baseline or actual NWF data available for selected scope.")
    else:
        if actual_rows == 0:
            st.info("No Apptio actuals for selected scope.")
        x_labels = merged["BUCKET"].tolist()
        baseline_vals = merged["BASELINE"].astype(float).tolist()
        actual_vals = merged["ACTUAL"].astype(float).tolist()
        tooltip_map = {}
        for _, row in merged.iterrows():
            bucket = str(row["BUCKET"])
            base = float(row["BASELINE"] or 0.0)
            act = float(row["ACTUAL"] or 0.0)
            var = act - base
            pct = (var / base * 100.0) if base > 0 else None
            text = (
                f"{bucket}"
                f"<br/>Baseline: {_fmt_money(base, 0)}"
                f"<br/>Actual: {_fmt_money(act, 0)}"
                f"<br/>Variance: {_fmt_money(var, 0)}"
            )
            if pct is not None:
                text += f" ({pct:.1f}%)"
            tooltip_map[bucket] = text
        option = {
            "tooltip": {"trigger": "item"},
            "legend": {"data": ["Baseline", "Actual"], "bottom": 0},
            "grid": {"left": 60, "right": 20, "top": 30, "bottom": 60, "containLabel": True},
            "xAxis": {"type": "category", "data": x_labels},
            "yAxis": {"type": "value"},
            "series": [
                {
                    "name": "Baseline",
                    "type": "bar",
                    "data": [{"value": v, "tooltip": {"formatter": tooltip_map.get(label)}} for label, v in zip(x_labels, baseline_vals)],
                    "itemStyle": {"color": bucket_colors["Baseline"]},
                },
                {
                    "name": "Actual",
                    "type": "bar",
                    "data": [{"value": v, "tooltip": {"formatter": tooltip_map.get(label)}} for label, v in zip(x_labels, actual_vals)],
                    "itemStyle": {"color": bucket_colors["Actual"]},
                },
            ],
        }
        render_echart(option, height="320px", key=f"insights_nwf_cost_type_{int(year)}", page_theme=page_theme)
    
    _render_section_divider()
    
    _section_title(
        "NWF Baseline vs Actuals (Program-level)",
        "Compares planned (Baseline) vs actual (Apptio) non-workforce spend. "
        "Actuals are program-level only (not attributable to app groups). "
        "Use this to identify overspend/underspend by program.",
    )
    if baseline_nwf_tab is None or baseline_nwf_tab.empty:
        st.info("No baseline NWF data available for selected scope.")
    else:
        prog_col = "PROGRAMNAME" if "PROGRAMNAME" in baseline_nwf_tab.columns else "PROGRAM"
        baseline_nwf_tab[prog_col] = baseline_nwf_tab.get(prog_col, "").fillna("").astype(str).str.strip()
        base_by_prog = baseline_nwf_tab.groupby(prog_col, dropna=False)["AMOUNT"].sum().reset_index()
        base_by_prog = base_by_prog[base_by_prog[prog_col].ne("")]
        actual_by_prog = pd.DataFrame(columns=[prog_col, "ACTUAL_NWF"])
        if isinstance(apptio_for_nwf, pd.DataFrame) and not apptio_for_nwf.empty and "AMOUNT" in apptio_for_nwf.columns:
            act_prog_col = "PROGRAMNAME" if "PROGRAMNAME" in apptio_for_nwf.columns else "PROGRAM"
            tmp = apptio_for_nwf.copy()
            tmp[act_prog_col] = tmp.get(act_prog_col, "").fillna("").astype(str).str.strip()
            actual_by_prog = tmp.groupby(act_prog_col, dropna=False)["AMOUNT"].sum().reset_index()
            actual_by_prog = actual_by_prog.rename(columns={act_prog_col: prog_col, "AMOUNT": "ACTUAL_NWF"})
            actual_by_prog = actual_by_prog[actual_by_prog[prog_col].ne("")]
        merged = base_by_prog.rename(columns={"AMOUNT": "BASELINE_NWF"}).merge(actual_by_prog, on=prog_col, how="outer")
        merged["BASELINE_NWF"] = pd.to_numeric(merged.get("BASELINE_NWF"), errors="coerce").fillna(0.0)
        merged["ACTUAL_NWF"] = pd.to_numeric(merged.get("ACTUAL_NWF"), errors="coerce").fillna(0.0)
        merged["VARIANCE"] = merged["ACTUAL_NWF"] - merged["BASELINE_NWF"]
        merged["VAR_PCT"] = merged.apply(
            lambda r: (r["VARIANCE"] / r["BASELINE_NWF"]) if r["BASELINE_NWF"] > 0 else None, axis=1
        )
        merged = merged.sort_values("BASELINE_NWF", ascending=False)
        if apptio_for_nwf is None or apptio_for_nwf.empty:
            st.info("No Apptio actuals available for selected scope. Showing baseline only.")
    
        prog_labels = merged[prog_col].astype(str).tolist()
        baseline_vals = merged["BASELINE_NWF"].astype(float).tolist()
        actual_vals = merged["ACTUAL_NWF"].astype(float).tolist()
        tooltip_map = {}
        for _, row in merged.iterrows():
            label = str(row[prog_col])
            base = float(row["BASELINE_NWF"] or 0.0)
            act = float(row["ACTUAL_NWF"] or 0.0)
            var = act - base
            pct = (var / base * 100.0) if base > 0 else None
            text = (
                f"{label}"
                f"<br/>Baseline: {_fmt_money(base, 0)}"
                f"<br/>Actual: {_fmt_money(act, 0)}"
                f"<br/>Variance: {_fmt_money(var, 0)}"
            )
            if pct is not None:
                text += f" ({pct:.1f}%)"
            tooltip_map[label] = text
        option = {
            "tooltip": {"trigger": "item"},
            "legend": {"data": ["Baseline", "Actual"], "bottom": 0},
            "grid": {"left": 60, "right": 20, "top": 30, "bottom": 60, "containLabel": True},
            "xAxis": {"type": "category", "data": prog_labels, "axisLabel": {"interval": 0, "rotate": 30}},
            "yAxis": {"type": "value"},
            "series": [
                {
                    "name": "Baseline",
                    "type": "bar",
                    "data": [{"value": v, "tooltip": {"formatter": tooltip_map.get(label)}} for label, v in zip(prog_labels, baseline_vals)],
                    "itemStyle": {"color": sem["baseline"]},
                },
                {
                    "name": "Actual",
                    "type": "bar",
                    "data": [{"value": v, "tooltip": {"formatter": tooltip_map.get(label)}} for label, v in zip(prog_labels, actual_vals)],
                    "itemStyle": {"color": sem["actual"]},
                },
            ],
        }
        render_echart(option, height="360px", key=f"insights_nwf_baseline_actuals_{int(year)}", page_theme=page_theme)

_set_loading("Finalizing view...", 5)
global_loading_ph.empty()
show_perf_panel(page_perf_key)
