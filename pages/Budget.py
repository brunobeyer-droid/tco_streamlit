# Summary: update KPI source labels and messages to NEXT branding.
from __future__ import annotations
from core.cache_utils import cache_data_portfolio
from core.freshness import get_data_freshness_token

# Changelog: moved insights/data readiness up, removed variance drivers expander, deduped Budget Explorer filters, updated leadership description

import datetime as dt
import html
import math
from typing import Any, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from core.ado_recon import load_explorer_feature_rows, load_explorer_fte_by_group
from core.canonical_costs import (
    get_app_group_costs as _get_app_group_costs_raw,
    get_pi_costs as _get_pi_costs_raw,
    get_unassigned_breakdown as _get_unassigned_breakdown_raw,
)
from core.capacity_data import fetch_capacity_demand_pi
from core.cost_model import load_cost_model
from core.data import fetch_cost_events_by_pi, fetch_filter_options
from core.debug import is_debug_enabled
from core.init import init_page, render_echart
from core.name_resolution import apply_display_scope_names
from core.scope import infer_role, read_scope_from_session, scope_label
from utils.labels import display_class_label
from utils.ui_patterns import render_active_filters_summary
from utils.theme import use_theme
from utils.finops_kpi_cards import build_finops_kpi_card_html
from utils.app_shell import bootstrap_page
from welcome.layout import APP_COLORS
from core.init import ensure_analytics_views

from db import fetch_df as _fetch_df_raw, get_data_version_info  # type: ignore

bootstrap_page()

try:
    from db.tco_events import insert_cost_event
except Exception:
    insert_cost_event = None  # type: ignore[assignment]

# Events are currently disabled for normal planning edits.
# We keep the table/helpers for future explicit change logging, but Budget should not rely on them for now.
ENABLE_COST_EVENTS = False


def fetch_df(sql, params=None):  # type: ignore[override]
    df = _fetch_df_raw(sql, params)
    return apply_display_scope_names(df) if isinstance(df, pd.DataFrame) else df


def get_pi_costs(*args, **kwargs):  # type: ignore[override]
    return apply_display_scope_names(_get_pi_costs_raw(*args, **kwargs))


def get_app_group_costs(*args, **kwargs):  # type: ignore[override]
    return apply_display_scope_names(_get_app_group_costs_raw(*args, **kwargs))


def get_unassigned_breakdown(*args, **kwargs):  # type: ignore[override]
    return apply_display_scope_names(_get_unassigned_breakdown_raw(*args, **kwargs))

# This page shows Baseline (budget) costs using the canonical cost API in `core/canonical_costs.py`.
scenario = "Baseline"


page_theme = init_page("Budget", page_path=__file__)
user = st.session_state.get("auth_user") or {}

show_debug = is_debug_enabled(label="Debug")

st.markdown(
    """
    <style>
    div[data-testid="stMetric"] { min-height: 64px; }
    div[data-testid="stCaption"] { min-height: 18px; }
    </style>
    """,
    unsafe_allow_html=True,
)


LABEL_NWF = "Non-People Cost (Vendors / Licenses / Cloud)"
LABEL_MSP = "Outsourced Delivery (MSP)"

# KPI source badge colors (match `pages/0_Welcome.py` for consistency).
source_colors = {
    "NEXT (WF+NWF)": "#2563eb",
    "NEXT Forecast": "#f59e0b",
    "Apptio": "#16a34a",
    "ADO": "#7c3aed",
    "NEXT + ADO": "#0ea5e9",
    "Apptio + NEXT Forecast": "#0891b2",
}

# Map internal/raw sources to leader-friendly KPI badges.
source_tag_map = {
    "VW_TCO_WORKFORCE_SPLIT": "NEXT (WF+NWF)",
    "VW_TCO_WORKFORCE_SPLIT + VW_PROGRAM_*": "NEXT (WF+NWF)",
    "VW_PROGRAM_COMPOSITION_EFFECTIVE + VW_PROGRAM_RATE_EFFECTIVE": "NEXT (WF+NWF)",
    "VW_ADO_FEATURES_PI_MAPPED": "ADO",
    "VW_TEAM_ALLOCATED_HEADCOUNT_PI": "NEXT (WF+NWF)",
    ".streamlit/secrets.toml": "NEXT Forecast",
    "Derived from baseline + expected": "NEXT (WF+NWF)",
    "Derived from demand + capacity": "NEXT + ADO",
    "(FTE×rate) − (view)": "NEXT (WF+NWF)",
}
BUDGET_KPI_MOTIF_MAP = {
    "Baseline (Plan) – Total": "bars",
    "Expected (Projection) – Total": "bars",
    "Variance ($)": "wave",
    "Variance (%)": "wave",
    "Top Program driver": "wave",
    "Top Application driver": "wave",
    "Unassigned share": "dotline",
    "Coverage": "curve",
}


def _kpi_source_tag(raw_source: str) -> Tuple[str, str]:
    raw = str(raw_source or "").strip()
    tag = source_tag_map.get(raw, "")
    if not tag:
        # Fallback: keep badges friendly even if a new raw label sneaks in.
        up = raw.upper()
        if "APPTIO" in up:
            tag = "Apptio"
        elif "ADO" in up:
            tag = "ADO"
        elif "FORECAST" in up:
            tag = "NEXT Forecast"
        else:
            tag = "NEXT (WF+NWF)"
    return tag, source_colors.get(tag, "#64748b")


def _metric(
    col,
    label: str,
    value: str,
    extra: str = "",
    *,
    raw_source: str = "",
    help_text: Optional[str] = None,
) -> None:
    tag, color = _kpi_source_tag(raw_source)
    _metric_card(col, label, value, extra=extra, source=tag, source_color=color, help_text=help_text)


def _safe_str(val: Any) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    return str(val).strip()


def _fmt_money(val: Optional[float], digits: int = 0) -> str:
    if val is None:
        return "—"
    try:
        if math.isnan(float(val)):
            return "—"
    except Exception:
        return "—"
    return f"${float(val):,.{digits}f}"


def _fmt_num(val: Optional[float], digits: int = 2) -> str:
    if val is None:
        return "—"
    try:
        if math.isnan(float(val)):
            return "—"
    except Exception:
        return "—"
    return f"{float(val):,.{digits}f}"


def _fmt_pct(val: Optional[float], digits: int = 0) -> str:
    if val is None:
        return "—"
    try:
        if math.isnan(float(val)):
            return "—"
    except Exception:
        return "—"
    return f"{float(val) * 100:.{digits}f}%"


def _norm_category(cat: Any, subcomponent: Any = None) -> str:
    c = _safe_str(cat).upper().replace(" ", "_")
    if c in {"WORK_FORCE", "WORKFORCE", "WORK"}:
        return "WORK_FORCE"
    if c in {"NON_WORK_FORCE", "NONWORK_FORCE", "NONWORKFORCE", "NON_WORK"}:
        return "NON_WORK_FORCE"
    sub = _safe_str(subcomponent).upper()
    if any(k in sub for k in ("INVOICE", "MSP", "CLOUD", "TRAVEL", "INFRA", "CONTRACTOR CS", "CONTRACTOR_CS")):
        return "NON_WORK_FORCE"
    return c or "UNKNOWN"


def _build_budget_scope_frames(
    *,
    years_sel: Sequence[int],
    pi_sel: Sequence[int],
    programs_sel: Sequence[str],
    teams_sel: Sequence[str],
    app_groups_sel: Optional[Sequence[str]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return canonical (Baseline, Expected) cost lines for the selected scope filters.

    Notes
    -----
    - Uses `load_cost_model` as the single source of scenario cost lines.
    - Applies the same app-group filter semantics as the page UI:
      selecting an app group should not drop program-level rows (blank/unassigned buckets).
    """

    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy() if df is not None else pd.DataFrame()
        if out.empty:
            return pd.DataFrame()
        out = apply_display_scope_names(out)
        out["YEAR"] = pd.to_numeric(out.get("YEAR"), errors="coerce").astype("Int64")
        out["PI"] = pd.to_numeric(out.get("PI"), errors="coerce").astype("Int64")
        out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
        out["TEAMNAME"] = out.get("TEAMNAME", "").fillna("").astype(str).str.strip()
        out["GROUPNAME"] = out.get("GROUPNAME", "").fillna("").astype(str).str.strip()
        out["SOURCE"] = out.get("SOURCE", "").fillna("").astype(str).str.strip()
        out["SUBCOMPONENT"] = out.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
        out["COST_CATEGORY"] = out.get("COST_CATEGORY", "").fillna("").astype(str).str.strip()
        out["AMOUNT"] = pd.to_numeric(out.get("AMOUNT"), errors="coerce").fillna(0.0)
        out["CAT"] = [
            _norm_category(c, s)
            for c, s in zip(out.get("COST_CATEGORY", pd.Series(dtype=object)), out.get("SUBCOMPONENT", pd.Series(dtype=object)))
        ]
        return out

    years_i = sorted({int(y) for y in (years_sel or []) if y is not None and int(y) > 0})
    if not years_i:
        return pd.DataFrame(), pd.DataFrame()

    expected_parts: list[pd.DataFrame] = []
    baseline_parts: list[pd.DataFrame] = []
    for y in years_i:
        cm = load_cost_model(
            int(y),
            {
                "programs": list(programs_sel or []),
                "teams": list(teams_sel or []),
                # Cost scope ignores Application filter at load-time by design (program-level costs are not app-attributable).
                "groups": [],
            },
        )
        expected_parts.append(cm.get("EXPECTED", pd.DataFrame()))
        baseline_parts.append(cm.get("BASELINE", pd.DataFrame()))

    expected_nonempty = [d for d in expected_parts if isinstance(d, pd.DataFrame) and not d.empty]
    baseline_nonempty = [d for d in baseline_parts if isinstance(d, pd.DataFrame) and not d.empty]

    df_expected = _normalize(pd.concat(expected_nonempty, ignore_index=True, sort=False) if expected_nonempty else pd.DataFrame())
    df_baseline = _normalize(pd.concat(baseline_nonempty, ignore_index=True, sort=False) if baseline_nonempty else pd.DataFrame())

    pi_i = sorted({int(p) for p in (pi_sel or []) if p is not None and 1 <= int(p) <= 4})
    if pi_i:
        if not df_expected.empty:
            df_expected = df_expected[df_expected.get("PI").isin(pi_i)].copy()
        if not df_baseline.empty:
            pi_series = pd.to_numeric(df_baseline.get("PI"), errors="coerce").fillna(0).astype(int)
            keep_mask = pi_series.isin(pi_i) | pi_series.eq(0)
            df_baseline = df_baseline.loc[keep_mask].copy()

    groups_i = [str(g).strip() for g in (app_groups_sel or []) if g is not None and str(g).strip()]
    if groups_i:
        gsel = str(groups_i[0]).strip()
        keep_program_level = {"", "(UNASSIGNED)", "(PROGRAM NWF)", "(UNALLOCATED NWF)"}

        def _apply_group_semantics(frame: pd.DataFrame) -> pd.DataFrame:
            if frame is None or frame.empty:
                return pd.DataFrame()
            gcol = frame.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
            gcol_up = gcol.astype(str).str.upper().str.strip()
            keep = gcol.eq(gsel) | gcol_up.isin(keep_program_level)
            return frame.loc[keep].copy()

        df_expected = _apply_group_semantics(df_expected)
        df_baseline = _apply_group_semantics(df_baseline)

    return df_baseline, df_expected


def _metric_card(
    col,
    label: str,
    value: str,
    extra: str = "",
    *,
    source: str = "",
    source_color: str = "#64748b",
    help_text: Optional[str] = None,
) -> None:
    with col:
        with st.container(border=True):
            try:
                st.metric(label, value, help=help_text)
            except TypeError:
                st.metric(label, value)
            if source:
                safe_source = html.escape(source)
                st.markdown(
                    f"<span style='display:inline-block; padding:2px 8px; border-radius:10px; "
                    f"font-size:11px; background:{source_color}; color:#fff;'>{safe_source}</span>",
                    unsafe_allow_html=True,
                )
            st.caption(extra if extra else " ")


def _render_finops_kpi_card(
    col,
    *,
    title: str,
    value: str,
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
    with col:
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
                extra_lines=2,
            ),
            unsafe_allow_html=True,
        )


def _pick_color(label: str) -> str:
    colors = APP_COLORS or ["#60a5fa", "#22c55e", "#f59e0b", "#ef4444", "#a78bfa", "#14b8a6"]
    if not label:
        return colors[0]
    idx = abs(hash(label)) % len(colors)
    return colors[idx]

def _dedup_keep_order(values: Sequence[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for v in values or []:
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out

def _bullets(lines: Sequence[str]) -> None:
    items = [str(x).strip() for x in (lines or []) if str(x).strip()]
    if not items:
        st.info("No insights for this scope.")
        return
    st.markdown("\n".join([f"- {x}" for x in items]))


def _variance_driver_caption(*, by_program: pd.DataFrame, variance_pct: Optional[float]) -> str:
    """Short, leader-friendly driver hint for the variance KPI card."""
    if variance_pct is None:
        return "Variance vs baseline is unavailable for this scope."
    if by_program is None or by_program.empty or "PROGRAMNAME" not in by_program.columns:
        return "Variance drivers unavailable."
    work = by_program.copy()
    work["VARIANCE"] = pd.to_numeric(work.get("VARIANCE"), errors="coerce").fillna(0.0)
    work["PROGRAMNAME"] = work.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    top = work.sort_values("VARIANCE", ascending=False).head(2)
    drivers = ", ".join([str(x) for x in top["PROGRAMNAME"].tolist() if str(x).strip()][:2])
    pct = float(variance_pct) * 100.0
    if abs(pct) < 10:
        return f"Forecast is close to plan (Δ {pct:.0f}%)."
    direction = "above" if pct > 0 else "below"
    if drivers:
        return f"Forecast is {abs(pct):.0f}% {direction} plan, mainly driven by {drivers}."
    return f"Forecast is {abs(pct):.0f}% {direction} plan."


def _roadmap_capacity_fraction(default: float = 0.8) -> float:
    """Capacity-side only: clamp roadmap_capacity_fraction to [0.3, 1.0]."""
    raw: Any = None
    try:
        raw = st.secrets.get("model", {}).get("roadmap_capacity_fraction", None)  # type: ignore[attr-defined]
    except Exception:
        raw = None
    try:
        val = float(raw) if raw is not None and str(raw).strip() != "" else float(default)
    except Exception:
        val = float(default)
    if not (val == val) or val in (float("inf"), float("-inf")):
        val = float(default)
    return max(0.3, min(1.0, float(val)))

@cache_data_portfolio(ttl=600, show_spinner=False)
def _program_portfolio_map() -> pd.DataFrame:
    """Best-effort PROGRAMNAME -> PORTFOLIO mapping (optional; depends on PROGRAMS schema)."""
    try:
        cols = fetch_df(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'PROGRAMS'
            """,
            None,
        )
    except Exception:
        cols = None
    if cols is None or cols.empty or "COLUMN_NAME" not in cols.columns:
        return pd.DataFrame(columns=["PROGRAMNAME", "PORTFOLIO"])
    names = {str(c).strip().upper() for c in cols["COLUMN_NAME"].dropna().tolist()}
    portfolio_col = next((c for c in ("PORTFOLIO", "PORTFOLIONAME", "PORTFOLIO_NAME") if c in names), None)
    if not portfolio_col:
        return pd.DataFrame(columns=["PROGRAMNAME", "PORTFOLIO"])
    try:
        df = fetch_df(f"SELECT PROGRAMNAME, {portfolio_col} AS PORTFOLIO FROM PROGRAMS", None)
    except Exception:
        df = None
    if df is None or df.empty:
        return pd.DataFrame(columns=["PROGRAMNAME", "PORTFOLIO"])
    out = df.copy()
    out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").astype(str).str.strip()
    out["PORTFOLIO"] = out.get("PORTFOLIO", "").astype(str).str.strip()
    out = out[out["PROGRAMNAME"].ne("")].copy()
    return out


@cache_data_portfolio(ttl=600, show_spinner=False)
def _program_alias_map() -> pd.DataFrame:
    """Best-effort program alias map (ADO/raw/display) -> canonical program name."""
    try:
        prog = fetch_df(
            """
            SELECT
              LTRIM(RTRIM(PROGRAMNAME)) AS PROGRAM_CANONICAL,
              LTRIM(RTRIM(COALESCE(NULLIF(PROGRAM_DISPLAY_NAME, ''), PROGRAMNAME))) AS PROGRAM_DISPLAY
            FROM PROGRAMS
            """,
            None,
        )
    except Exception:
        prog = None
    if prog is None or prog.empty:
        return pd.DataFrame(columns=["ALIAS", "PROGRAM_CANONICAL", "PROGRAM_DISPLAY"])

    p = prog.copy()
    p["PROGRAM_CANONICAL"] = p.get("PROGRAM_CANONICAL", "").astype(str).str.strip()
    p["PROGRAM_DISPLAY"] = p.get("PROGRAM_DISPLAY", "").astype(str).str.strip()
    p = p[p["PROGRAM_CANONICAL"].ne("")].copy()
    if p.empty:
        return pd.DataFrame(columns=["ALIAS", "PROGRAM_CANONICAL", "PROGRAM_DISPLAY"])

    alias_rows = [
        p[["PROGRAM_CANONICAL", "PROGRAM_DISPLAY"]]
        .rename(columns={"PROGRAM_CANONICAL": "ALIAS"})
        .assign(PROGRAM_CANONICAL=p["PROGRAM_CANONICAL"].values),
        pd.DataFrame(
            {
                "ALIAS": p["PROGRAM_DISPLAY"].values,
                "PROGRAM_CANONICAL": p["PROGRAM_CANONICAL"].values,
                "PROGRAM_DISPLAY": p["PROGRAM_DISPLAY"].values,
            }
        ),
    ]

    try:
        ado_map = fetch_df(
            """
            SELECT
              LTRIM(RTRIM(mp.ADO_PROGRAM)) AS ALIAS,
              LTRIM(RTRIM(p.PROGRAMNAME)) AS PROGRAM_CANONICAL,
              LTRIM(RTRIM(COALESCE(NULLIF(p.PROGRAM_DISPLAY_NAME, ''), p.PROGRAMNAME))) AS PROGRAM_DISPLAY
            FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = mp.PROGRAMID
            """,
            None,
        )
    except Exception:
        ado_map = None
    if ado_map is not None and not ado_map.empty:
        a = ado_map.copy()
        a["ALIAS"] = a.get("ALIAS", "").astype(str).str.strip()
        a["PROGRAM_CANONICAL"] = a.get("PROGRAM_CANONICAL", "").astype(str).str.strip()
        a["PROGRAM_DISPLAY"] = a.get("PROGRAM_DISPLAY", "").astype(str).str.strip()
        a = a[a["ALIAS"].ne("") & a["PROGRAM_CANONICAL"].ne("")].copy()
        if not a.empty:
            alias_rows.append(a[["ALIAS", "PROGRAM_CANONICAL", "PROGRAM_DISPLAY"]])

    out = pd.concat(alias_rows, ignore_index=True, sort=False)
    out["ALIAS"] = out.get("ALIAS", "").astype(str).str.strip()
    out["PROGRAM_CANONICAL"] = out.get("PROGRAM_CANONICAL", "").astype(str).str.strip()
    out["PROGRAM_DISPLAY"] = out.get("PROGRAM_DISPLAY", "").astype(str).str.strip()
    out = out[out["ALIAS"].ne("") & out["PROGRAM_CANONICAL"].ne("")].copy()
    out = out.drop_duplicates(subset=["ALIAS", "PROGRAM_CANONICAL"], keep="first")
    return out


def _program_lookup_maps() -> tuple[dict[str, str], dict[str, list[str]]]:
    aliases = _program_alias_map()
    if aliases is None or aliases.empty:
        return {}, {}
    disp_map: dict[str, str] = {}
    canon_map: dict[str, list[str]] = {}
    for _, r in aliases.iterrows():
        alias = str(r.get("ALIAS") or "").strip()
        canonical = str(r.get("PROGRAM_CANONICAL") or "").strip()
        display = str(r.get("PROGRAM_DISPLAY") or "").strip() or canonical
        if not alias or not canonical:
            continue
        key = alias.upper()
        disp_map[key] = display
        canon_map.setdefault(key, [])
        if canonical not in canon_map[key]:
            canon_map[key].append(canonical)
    return disp_map, canon_map


def _program_display_name(name: Any, disp_map: dict[str, str]) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""
    return str(disp_map.get(raw.upper(), raw))


def _expand_program_filters(programs: Sequence[str], canon_map: dict[str, list[str]]) -> List[str]:
    out: List[str] = []
    for p in programs or []:
        raw = str(p or "").strip()
        if not raw:
            continue
        out.append(raw)
        for c in canon_map.get(raw.upper(), []):
            c0 = str(c or "").strip()
            if c0:
                out.append(c0)
    return _dedup_keep_order(out)


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_allocated_capacity_team_pi_years(
    *,
    years: Sequence[int],
    programs: Sequence[str],
    teams: Sequence[str],
) -> pd.DataFrame:
    years = sorted({int(y) for y in years if y is not None and int(y) > 0})
    if not years:
        return pd.DataFrame()
    where = [f"ADO_YEAR IN ({', '.join(['%s'] * len(years))})"]
    params: List[Any] = list(years)
    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(PROGRAMNAME) IN ({placeholders})")
        params.extend([str(p).upper() for p in programs])
    if teams:
        placeholders = ", ".join(["%s"] * len(teams))
        where.append(f"UPPER(TEAMNAME) IN ({placeholders})")
        params.extend([str(t).upper() for t in teams])
    sql = f"""
      SELECT
        TEAMID,
        TEAMNAME,
        PROGRAMID,
        PROGRAMNAME,
        ADO_YEAR,
        ITERATION_NUM,
        DELIVERY_HEADCOUNT,
        CONTRACTOR_C_HEADCOUNT,
        CONTRACTOR_CS_HEADCOUNT,
        ALLOCATED_HEADCOUNT
      FROM VW_TEAM_ALLOCATED_HEADCOUNT_PI
      WHERE {" AND ".join(where)}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=180, show_spinner=False)
def _fallback_capacity_from_headcount(
    *,
    years: Sequence[int],
    programs: Sequence[str],
    teams: Sequence[str],
    demand_multi: pd.DataFrame,
) -> pd.DataFrame:
    """Fallback capacity scaffold when `VW_TEAM_ALLOCATED_HEADCOUNT_PI` is empty/missing.

    Why this exists:
    - Budget Capacity + readiness checks are anchored on `VW_TEAM_ALLOCATED_HEADCOUNT_PI`, which
      depends on `VW_TEAM_COMPOSITION_EFFECTIVE`.
    - If the composition view isn't reflecting recent headcount edits yet, Budget can show
      demand with zero capacity.

    What this does:
    - Reads saved headcount directly from:
        - `TEAM_HEADCOUNT_HISTORY` (TEAM + DELIVERY classes)
        - `TEAM_CONTRACTOR_HEADCOUNT` (CONTRACTOR_C / CONTRACTOR_CS)
    - Produces per-PI capacity rows for PI 1..4 using this rule:
        - If PI-specific rows exist (PI=1..4), use them.
        - Otherwise fall back to PI=0 ("All PIs") values.

    Returned columns align with `VW_TEAM_ALLOCATED_HEADCOUNT_PI`:
      TEAMID, TEAMNAME, PROGRAMID, PROGRAMNAME, ADO_YEAR, ITERATION_NUM,
      DELIVERY_HEADCOUNT, CONTRACTOR_CS_HEADCOUNT, CONTRACTOR_C_HEADCOUNT, ALLOCATED_HEADCOUNT
    """

    years_norm = sorted({int(y) for y in (years or []) if y is not None and int(y) > 0})
    if not years_norm:
        return pd.DataFrame()

    # Normalize demand for deriving scope (when programs/teams filters aren't provided).
    dem = demand_multi.copy() if demand_multi is not None else pd.DataFrame()
    if dem is None:
        dem = pd.DataFrame()
    if not dem.empty:
        dem["_Y"] = pd.to_numeric(dem.get("ADO_YEAR"), errors="coerce").astype("Int64")
        dem["_P"] = dem.get("PROGRAMNAME", "").astype(str).str.strip()
        dem["_T"] = dem.get("TEAMNAME", "").astype(str).str.strip()

    programs_up = [str(p).strip().upper() for p in (programs or []) if str(p).strip()]
    teams_up = [str(t).strip().upper() for t in (teams or []) if str(t).strip()]

    out_frames: List[pd.DataFrame] = []
    for yr in years_norm:
        # If no explicit filters are provided, derive an effective scope from demand rows for this year.
        eff_programs = programs_up
        eff_teams = teams_up
        if not eff_programs and not eff_teams and not dem.empty:
            dem_y = dem.loc[dem["_Y"].fillna(-1).astype(int) == int(yr)]
            eff_programs = sorted({str(x).strip().upper() for x in dem_y["_P"].dropna().tolist() if str(x).strip()})
            eff_teams = sorted({str(x).strip().upper() for x in dem_y["_T"].dropna().tolist() if str(x).strip()})

        if not eff_programs and not eff_teams and dem.empty:
            # No scope hints; avoid scanning entire history tables.
            continue

        where_staff = [
            "TRY_CONVERT(INT, h.YEAR) = %s",
            "ISNULL(TRY_CONVERT(INT, h.PI), 0) BETWEEN 0 AND 4",
            "UPPER(h.CLASS) IN ('TEAM','DELIVERY')",
            "t.TEAMID IS NOT NULL",
        ]
        params_staff: List[Any] = [int(yr)]

        where_cons = [
            "TRY_CONVERT(INT, h.YEAR) = %s",
            "ISNULL(TRY_CONVERT(INT, h.PI), 0) BETWEEN 0 AND 4",
            "UPPER(h.CLASS) IN ('CONTRACTOR_CS','CONTRACTOR_C')",
            "t.TEAMID IS NOT NULL",
        ]
        params_cons: List[Any] = [int(yr)]

        if eff_programs:
            placeholders = ", ".join(["%s"] * len(eff_programs))
            clause = f"UPPER(p.PROGRAMNAME) IN ({placeholders})"
            where_staff.append(clause)
            where_cons.append(clause)
            params_staff.extend(eff_programs)
            params_cons.extend(eff_programs)

        if eff_teams:
            placeholders = ", ".join(["%s"] * len(eff_teams))
            clause = f"UPPER(t.TEAMNAME) IN ({placeholders})"
            where_staff.append(clause)
            where_cons.append(clause)
            params_staff.extend(eff_teams)
            params_cons.extend(eff_teams)

        sql_staff = f"""
          SELECT
            t.TEAMID,
            t.TEAMNAME,
            t.PROGRAMID,
            p.PROGRAMNAME,
            TRY_CONVERT(INT, h.YEAR) AS ADO_YEAR,
            TRY_CONVERT(INT, h.PI) AS PI,
            SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS STAFF_HEADCOUNT
          FROM TEAM_HEADCOUNT_HISTORY h
          LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
          WHERE {" AND ".join(where_staff)}
          GROUP BY
            t.TEAMID, t.TEAMNAME, t.PROGRAMID, p.PROGRAMNAME,
            TRY_CONVERT(INT, h.YEAR), TRY_CONVERT(INT, h.PI)
        """
        sql_cons = f"""
          SELECT
            t.TEAMID,
            t.TEAMNAME,
            t.PROGRAMID,
            p.PROGRAMNAME,
            TRY_CONVERT(INT, h.YEAR) AS ADO_YEAR,
            TRY_CONVERT(INT, h.PI) AS PI,
            SUM(
              CASE WHEN UPPER(h.CLASS) = 'CONTRACTOR_CS'
                   THEN COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0) ELSE 0 END
            ) AS CONTRACTOR_CS_HEADCOUNT,
            SUM(
              CASE WHEN UPPER(h.CLASS) = 'CONTRACTOR_C'
                   THEN COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0) ELSE 0 END
            ) AS CONTRACTOR_C_HEADCOUNT
          FROM TEAM_CONTRACTOR_HEADCOUNT h
          LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
          WHERE {" AND ".join(where_cons)}
          GROUP BY
            t.TEAMID, t.TEAMNAME, t.PROGRAMID, p.PROGRAMNAME,
            TRY_CONVERT(INT, h.YEAR), TRY_CONVERT(INT, h.PI)
        """
        try:
            df_staff = fetch_df(sql_staff, tuple(params_staff) if params_staff else None)
        except Exception:
            df_staff = None
        try:
            df_cons = fetch_df(sql_cons, tuple(params_cons) if params_cons else None)
        except Exception:
            df_cons = None

        staff = df_staff.copy() if df_staff is not None and not df_staff.empty else pd.DataFrame()
        cons = df_cons.copy() if df_cons is not None and not df_cons.empty else pd.DataFrame()

        keys = ["TEAMID", "TEAMNAME", "PROGRAMID", "PROGRAMNAME", "ADO_YEAR"]
        if staff.empty and cons.empty:
            continue

        def _prep_base(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame(columns=keys + ["PI"])
            out = df.copy()
            out["TEAMID"] = out.get("TEAMID", "").astype(str).str.strip()
            out["TEAMNAME"] = out.get("TEAMNAME", "").astype(str).str.strip()
            out["PROGRAMID"] = out.get("PROGRAMID", "").astype(str).str.strip()
            out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").astype(str).str.strip()
            out["ADO_YEAR"] = pd.to_numeric(out.get("ADO_YEAR"), errors="coerce").astype("Int64")
            out["PI"] = pd.to_numeric(out.get("PI"), errors="coerce").fillna(0).astype(int)
            out = out[out["TEAMNAME"].ne("") & out["ADO_YEAR"].notna()].copy()
            out["ADO_YEAR"] = out["ADO_YEAR"].astype(int)
            return out

        staff = _prep_base(staff)
        cons = _prep_base(cons)
        if staff.empty and cons.empty:
            continue

        # Aggregate to (team, year, pi).
        staff["STAFF_HEADCOUNT"] = pd.to_numeric(staff.get("STAFF_HEADCOUNT"), errors="coerce").fillna(0.0)
        staff = staff.groupby(keys + ["PI"], dropna=False)[["STAFF_HEADCOUNT"]].sum().reset_index()

        cons["CONTRACTOR_CS_HEADCOUNT"] = pd.to_numeric(cons.get("CONTRACTOR_CS_HEADCOUNT"), errors="coerce").fillna(0.0)
        cons["CONTRACTOR_C_HEADCOUNT"] = pd.to_numeric(cons.get("CONTRACTOR_C_HEADCOUNT"), errors="coerce").fillna(0.0)
        cons = (
            cons.groupby(keys + ["PI"], dropna=False)[["CONTRACTOR_CS_HEADCOUNT", "CONTRACTOR_C_HEADCOUNT"]]
            .sum()
            .reset_index()
        )

        base_keys = pd.concat(
            [staff[keys].drop_duplicates() if not staff.empty else pd.DataFrame(columns=keys),
             cons[keys].drop_duplicates() if not cons.empty else pd.DataFrame(columns=keys)],
            ignore_index=True,
        ).drop_duplicates()

        if base_keys.empty:
            continue

        # Build per-PI rows (1..4), applying PI fallback to PI=0 where needed.
        def _pivot(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame()
            w = df[keys + ["PI", value_col]].copy()
            w[value_col] = pd.to_numeric(w.get(value_col), errors="coerce").fillna(0.0)
            pvt = w.pivot_table(index=keys, columns="PI", values=value_col, aggfunc="sum", fill_value=0.0)
            return pvt

        p_staff = _pivot(staff, "STAFF_HEADCOUNT")
        p_cs = _pivot(cons, "CONTRACTOR_CS_HEADCOUNT")
        p_c = _pivot(cons, "CONTRACTOR_C_HEADCOUNT")

        frames_year: List[pd.DataFrame] = []
        for pi in (1, 2, 3, 4):
            row = base_keys.reset_index(drop=True).copy()
            row["ITERATION_NUM"] = int(pi)

            def _series(pvt: pd.DataFrame) -> pd.Series:
                if pvt is None or pvt.empty:
                    return pd.Series([0.0] * len(row.index), index=row.index)
                base = row.merge(pvt.reset_index(), on=keys, how="left")
                col0 = base[0] if 0 in base.columns else 0.0
                colp = base[pi] if pi in base.columns else col0
                colp = pd.to_numeric(colp, errors="coerce")
                col0 = pd.to_numeric(col0, errors="coerce")
                vals = colp.fillna(col0.fillna(0.0)).astype(float).to_numpy()
                return pd.Series(vals, index=row.index, dtype=float)

            row["DELIVERY_HEADCOUNT"] = _series(p_staff).astype(float)
            row["CONTRACTOR_CS_HEADCOUNT"] = _series(p_cs).astype(float)
            row["CONTRACTOR_C_HEADCOUNT"] = _series(p_c).astype(float)
            row["ALLOCATED_HEADCOUNT"] = (
                pd.to_numeric(row["DELIVERY_HEADCOUNT"], errors="coerce").fillna(0.0)
                + pd.to_numeric(row["CONTRACTOR_CS_HEADCOUNT"], errors="coerce").fillna(0.0)
                + pd.to_numeric(row["CONTRACTOR_C_HEADCOUNT"], errors="coerce").fillna(0.0)
            )

            row["ADO_YEAR"] = pd.to_numeric(row.get("ADO_YEAR"), errors="coerce").astype("Int64")
            row["ITERATION_NUM"] = pd.to_numeric(row.get("ITERATION_NUM"), errors="coerce").astype("Int64")
            frames_year.append(
                row[
                    [
                        "TEAMID",
                        "TEAMNAME",
                        "PROGRAMID",
                        "PROGRAMNAME",
                        "ADO_YEAR",
                        "ITERATION_NUM",
                        "DELIVERY_HEADCOUNT",
                        "CONTRACTOR_CS_HEADCOUNT",
                        "CONTRACTOR_C_HEADCOUNT",
                        "ALLOCATED_HEADCOUNT",
                    ]
                ]
            )

        out_y = pd.concat(frames_year, ignore_index=True) if frames_year else pd.DataFrame()
        out_y["ALLOCATED_HEADCOUNT"] = pd.to_numeric(out_y.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
        out_y = out_y[out_y["ALLOCATED_HEADCOUNT"] > 0].copy()
        out_frames.append(out_y)

    if not out_frames:
        return pd.DataFrame()
    out_all = pd.concat(out_frames, ignore_index=True)
    out_all["TEAMNAME"] = out_all.get("TEAMNAME", "").astype(str).str.strip()
    out_all["PROGRAMNAME"] = out_all.get("PROGRAMNAME", "").astype(str).str.strip()
    out_all["ADO_YEAR"] = pd.to_numeric(out_all.get("ADO_YEAR"), errors="coerce").astype("Int64")
    out_all["ITERATION_NUM"] = pd.to_numeric(out_all.get("ITERATION_NUM"), errors="coerce").astype("Int64")
    out_all["ALLOCATED_HEADCOUNT"] = pd.to_numeric(out_all.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
    out_all = out_all[out_all["TEAMNAME"].ne("") & out_all["ADO_YEAR"].notna() & out_all["ITERATION_NUM"].notna()].copy()
    return out_all


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_program_overhead_people_cost_inputs(
    *,
    year: int,
    programs: Sequence[str],
    location: str = "GBC",
) -> pd.DataFrame:
    where = ["h.YEAR = %s"]
    params: List[Any] = [int(year)]
    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(p.PROGRAMNAME) IN ({placeholders})")
        params.extend([str(p).upper() for p in programs])
    sql = f"""
          SELECT
            h.TEAMID AS PROGRAMID,
            p.PROGRAMNAME,
            TRY_CONVERT(INT, h.YEAR) AS YEAR,
            TRY_CONVERT(INT, h.PI) AS PI,
            UPPER(LTRIM(RTRIM(h.LOCATION))) AS LOCATION,
            TRY_CONVERT(FLOAT, h.HEADCOUNT) AS PROGRAMFTE,
            COALESCE(TRY_CONVERT(FLOAT, pr.PROGRAM_XOM_RATE), 0) AS PROGRAM_XOM_RATE
          FROM VW_TEAM_HEADCOUNT_EFFECTIVE h
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = h.TEAMID
          LEFT JOIN VW_PROGRAM_RATE_EFFECTIVE pr
            ON pr.PROGRAMID = h.TEAMID
           AND pr.YEAR = TRY_CONVERT(INT, h.YEAR)
           AND pr.PI = TRY_CONVERT(INT, h.PI)
           AND UPPER(LTRIM(RTRIM(pr.LOCATION))) = UPPER(LTRIM(RTRIM(h.LOCATION)))
      WHERE {" AND ".join(where)}
        AND UPPER(LTRIM(RTRIM(h.CLASS))) = 'PROGRAM'
        AND TRY_CONVERT(INT, h.PI) BETWEEN 1 AND 4
    """
    try:
        # `location` kept for backwards compatibility / cache-key stability, but program overhead costing is
        # location-weighted (based on Programs headcount splits), so we do not filter to a single location here.
        df = fetch_df(sql, tuple(params))
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_team_rate_history(
    *,
    years: Sequence[int],
    team_ids: Sequence[str],
    location_preference: str = "GBC",
) -> pd.DataFrame:
    # WF rates come from `VW_TEAM_RATE_EFFECTIVE` to stay aligned with the Rates page.
    team_ids = [str(t).strip() for t in (team_ids or []) if str(t).strip()]
    years_i = sorted({int(y) for y in (years or []) if y is not None and int(y) > 0})
    if not team_ids or not years_i:
        return pd.DataFrame()
    where = [
        f"TEAMID IN ({', '.join(['%s'] * len(team_ids))})",
        f"YEAR IN ({', '.join(['%s'] * len(years_i))})",
    ]
    params: List[Any] = list(team_ids) + list(years_i)
    sql = f"""
      SELECT
        TEAMID,
        TRY_CONVERT(INT, YEAR) AS YEAR,
        TRY_CONVERT(INT, PI) AS PI,
        LOCATION,
        TRY_CONVERT(FLOAT, XOM_RATE) AS XOM_RATE,
        UPDATED_AT
      FROM VW_TEAM_RATE_EFFECTIVE
      WHERE {" AND ".join(where)}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
    except Exception:
        df = None
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["TEAMID"] = out.get("TEAMID", "").astype(str).str.strip()
    out["YEAR"] = pd.to_numeric(out.get("YEAR"), errors="coerce").astype("Int64")
    out["PI"] = pd.to_numeric(out.get("PI"), errors="coerce").astype("Int64")
    out["LOCATION"] = out.get("LOCATION", "").astype(str).str.strip()
    out["XOM_RATE"] = pd.to_numeric(out.get("XOM_RATE"), errors="coerce")
    out["UPDATED_AT"] = pd.to_datetime(out.get("UPDATED_AT"), errors="coerce")
    pref = str(location_preference or "GBC").strip().upper()
    out["_LOC_RANK"] = out["LOCATION"].astype(str).str.upper().ne(pref).astype(int)
    return out


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_program_rate_history(
    *,
    years: Sequence[int],
    program_ids: Sequence[str],
    location_preference: str = "GBC",
) -> pd.DataFrame:
    # WF rates come from `VW_PROGRAM_RATE_EFFECTIVE` to stay aligned with the Rates page.
    program_ids = [str(p).strip() for p in (program_ids or []) if str(p).strip()]
    years_i = sorted({int(y) for y in (years or []) if y is not None and int(y) > 0})
    if not program_ids or not years_i:
        return pd.DataFrame()
    where = [
        f"PROGRAMID IN ({', '.join(['%s'] * len(program_ids))})",
        f"YEAR IN ({', '.join(['%s'] * len(years_i))})",
    ]
    params: List[Any] = list(program_ids) + list(years_i)
    sql = f"""
      SELECT
        PROGRAMID,
        TRY_CONVERT(INT, YEAR) AS YEAR,
        TRY_CONVERT(INT, PI) AS PI,
        LOCATION,
        TRY_CONVERT(FLOAT, PROGRAM_XOM_RATE) AS PROGRAM_XOM_RATE,
        UPDATED_AT
      FROM VW_PROGRAM_RATE_EFFECTIVE
      WHERE {" AND ".join(where)}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
    except Exception:
        df = None
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["PROGRAMID"] = out.get("PROGRAMID", "").astype(str).str.strip()
    out["YEAR"] = pd.to_numeric(out.get("YEAR"), errors="coerce").astype("Int64")
    out["PI"] = pd.to_numeric(out.get("PI"), errors="coerce").astype("Int64")
    out["LOCATION"] = out.get("LOCATION", "").astype(str).str.strip()
    out["PROGRAM_XOM_RATE"] = pd.to_numeric(out.get("PROGRAM_XOM_RATE"), errors="coerce")
    out["UPDATED_AT"] = pd.to_datetime(out.get("UPDATED_AT"), errors="coerce")
    pref = str(location_preference or "GBC").strip().upper()
    out["_LOC_RANK"] = out["LOCATION"].astype(str).str.upper().ne(pref).astype(int)
    return out


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_msp_cost_by_pi_group_years(
    *,
    years: Sequence[int],
    programs: Sequence[str],
    teams: Sequence[str],
    groups: Sequence[str],
) -> pd.DataFrame:
    years = sorted({int(y) for y in years if y is not None and int(y) > 0})
    if not years:
        return pd.DataFrame()
    where = [f"YEAR IN ({', '.join(['%s'] * len(years))})", "UPPER(SOURCE) = 'MSP'"]
    params: List[Any] = list(years)
    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(PROGRAMNAME) IN ({placeholders})")
        params.extend([str(p).upper() for p in programs])
    if teams:
        placeholders = ", ".join(["%s"] * len(teams))
        where.append(f"UPPER(TEAMNAME) IN ({placeholders})")
        params.extend([str(t).upper() for t in teams])
    if groups:
        placeholders = ", ".join(["%s"] * len(groups))
        where.append(f"UPPER(GROUPNAME) IN ({placeholders})")
        params.extend([str(g).upper() for g in groups])
    sql = f"""
      SELECT
        YEAR,
        PI,
        PROGRAMNAME,
        TEAMNAME,
        GROUPNAME,
        SUM(CAST(AMOUNT AS DECIMAL(18,2))) AS MSP_COST_TOTAL
      FROM VW_TCO_WORKFORCE_SPLIT
      WHERE {" AND ".join(where)}
      GROUP BY YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

@cache_data_portfolio(ttl=300, show_spinner=False)
def _missing_rate_teams_for_year(*, year: int, team_names: Sequence[str]) -> List[str]:
    """Best-effort: teams with demand but missing team/program rates for the year."""
    team_names = [str(t).strip() for t in (team_names or []) if str(t).strip()]
    if not team_names:
        return []
    placeholders = ", ".join(["%s"] * len(team_names))
    try:
        df_ids = fetch_df(
            f"""
            SELECT TEAMID,
                   COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME) AS TEAMNAME,
                   PROGRAMID
            FROM TEAMS
            WHERE UPPER(COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME)) IN ({placeholders})
            """,
            tuple([t.upper() for t in team_names]),
        )
    except Exception:
        df_ids = None
    if df_ids is None or df_ids.empty:
        return []
    df_ids = df_ids.copy()
    df_ids["TEAMID"] = df_ids.get("TEAMID", "").astype(str).str.strip()
    df_ids["PROGRAMID"] = df_ids.get("PROGRAMID", "").astype(str).str.strip()
    df_ids["TEAMNAME"] = df_ids.get("TEAMNAME", "").astype(str).str.strip()
    team_ids = [t for t in df_ids["TEAMID"].dropna().tolist() if t]
    prog_ids = [p for p in df_ids["PROGRAMID"].dropna().tolist() if p]

    has_team: set[str] = set()
    has_prog: set[str] = set()

    try:
        if team_ids:
            ph = ", ".join(["%s"] * len(team_ids))
            df_tr = fetch_df(
                f"""
                SELECT TEAMID, MAX(CASE WHEN COALESCE(XOM_RATE, 0) > 0 THEN 1 ELSE 0 END) AS HAS_RATE
                FROM TEAM_RATE_HISTORY
                WHERE YEAR = %s AND TEAMID IN ({ph})
                GROUP BY TEAMID
                """,
                tuple([int(year)] + team_ids),
            )
            if df_tr is not None and not df_tr.empty:
                df_tr = df_tr.copy()
                df_tr["HAS_RATE"] = pd.to_numeric(df_tr.get("HAS_RATE"), errors="coerce").fillna(0).astype(int)
                has_team = set(df_tr.loc[df_tr["HAS_RATE"] > 0, "TEAMID"].dropna().astype(str).tolist())
    except Exception:
        has_team = set()

    try:
        if prog_ids:
            ph = ", ".join(["%s"] * len(prog_ids))
            df_pr = fetch_df(
                f"""
                SELECT PROGRAMID, MAX(CASE WHEN COALESCE(PROGRAM_XOM_RATE, 0) > 0 THEN 1 ELSE 0 END) AS HAS_RATE
                FROM PROGRAM_RATE_HISTORY
                WHERE YEAR = %s AND PROGRAMID IN ({ph})
                GROUP BY PROGRAMID
                """,
                tuple([int(year)] + prog_ids),
            )
            if df_pr is not None and not df_pr.empty:
                df_pr = df_pr.copy()
                df_pr["HAS_RATE"] = pd.to_numeric(df_pr.get("HAS_RATE"), errors="coerce").fillna(0).astype(int)
                has_prog = set(df_pr.loc[df_pr["HAS_RATE"] > 0, "PROGRAMID"].dropna().astype(str).tolist())
    except Exception:
        has_prog = set()

    missing = []
    for _, r in df_ids.iterrows():
        name = str(r.get("TEAMNAME") or "").strip()
        if not name:
            continue
        if (str(r.get("TEAMID") or "") not in has_team) and (str(r.get("PROGRAMID") or "") not in has_prog):
            missing.append(name)
    return sorted(set(missing))


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_headcount_notes_team_pi_years(*, years: Sequence[int], team_names: Sequence[str]) -> pd.DataFrame:
    """Best-effort: load free-text notes stored on headcount rows for team/year/PI.

    Schema-dependent: if headcount tables do not have NOTES/NOTE columns, returns empty.
    """
    years_i = sorted({int(y) for y in (years or []) if y is not None and int(y) > 0})
    teams = [str(t).strip() for t in (team_names or []) if str(t).strip()]
    if not years_i or not teams:
        return pd.DataFrame(columns=["TEAMNAME", "YEAR", "PI_NUM", "NOTE"])

    years_ph = ", ".join(["%s"] * len(years_i))
    teams_ph = ", ".join(["%s"] * len(teams))

    params_year_team: List[Any] = list(years_i) + [t.upper() for t in teams]

    def _try(col: str) -> Optional[pd.DataFrame]:
        try:
            df = fetch_df(
                f"""
                SELECT
                  t.TEAMNAME,
                  h.YEAR,
                  TRY_CONVERT(INT, ISNULL(h.PI,0)) AS PI_NUM,
                  LTRIM(RTRIM(h.{col})) AS NOTE
                FROM TEAM_HEADCOUNT_HISTORY h
                LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
                WHERE h.YEAR IN ({years_ph})
                  AND UPPER(t.TEAMNAME) IN ({teams_ph})
                  AND UPPER(LTRIM(RTRIM(h.CLASS))) IN ('TEAM','DELIVERY')
                  AND h.{col} IS NOT NULL AND LTRIM(RTRIM(h.{col})) <> ''

                UNION ALL

                SELECT
                  t.TEAMNAME,
                  c.YEAR,
                  TRY_CONVERT(INT, ISNULL(c.PI,0)) AS PI_NUM,
                  LTRIM(RTRIM(c.{col})) AS NOTE
                FROM TEAM_CONTRACTOR_HEADCOUNT c
                LEFT JOIN TEAMS t ON t.TEAMID = c.TEAMID
                WHERE c.YEAR IN ({years_ph})
                  AND UPPER(t.TEAMNAME) IN ({teams_ph})
                  AND UPPER(LTRIM(RTRIM(c.CLASS))) IN ('CONTRACTOR_C','CONTRACTOR_CS')
                  AND c.{col} IS NOT NULL AND LTRIM(RTRIM(c.{col})) <> ''
                """,
                tuple(params_year_team + params_year_team),
            )
            return df if df is not None else pd.DataFrame()
        except Exception:
            return None

    df = _try("NOTES")
    if df is None:
        df = _try("NOTE")
    if df is None or df.empty:
        return pd.DataFrame(columns=["TEAMNAME", "YEAR", "PI_NUM", "NOTE"])

    out = df.copy()
    out["TEAMNAME"] = out.get("TEAMNAME", "").astype(str).str.strip()
    out["YEAR"] = pd.to_numeric(out.get("YEAR"), errors="coerce").astype("Int64")
    out["PI_NUM"] = pd.to_numeric(out.get("PI_NUM"), errors="coerce").astype("Int64")
    out["NOTE"] = out.get("NOTE", "").fillna("").astype(str).str.strip()
    out = out[(out["TEAMNAME"].ne("")) & (out["YEAR"].notna()) & (out["PI_NUM"].notna()) & (out["NOTE"].ne(""))].copy()
    return out


st.title("Budget")
st.caption(
    "Plan vs projection, and where variance comes from"
)

_budget_load_slot = st.empty()
_budget_load_status = _budget_load_slot.status("Loading Budget data...", expanded=False) if hasattr(st, "status") else None
_budget_load_fallback = st.empty()


def _budget_status_step(message: str) -> None:
    msg = str(message or "").strip()
    if _budget_load_status is not None:
        _budget_load_status.update(label=msg, state="running")
    else:
        _budget_load_fallback.info(msg)


def _budget_status_done(message: str = "Budget data loaded.") -> None:
    msg = str(message or "").strip()
    if _budget_load_status is not None:
        _budget_load_status.update(label=msg, state="complete")
    try:
        _budget_load_slot.empty()
    except Exception:
        pass
    try:
        _budget_load_fallback.empty()
    except Exception:
        pass


def _budget_status_error(message: str) -> None:
    msg = str(message or "").strip()
    if _budget_load_status is not None:
        _budget_load_status.update(label=msg, state="error")
    else:
        _budget_load_fallback.error(msg)


_budget_status_step("Preparing analytics views...")
ensure_analytics_views()

_budget_status_step("Resolving user scope and filters...")
scope = read_scope_from_session(fetch_df)
role_inferred = infer_role(scope)
role_raw = str(scope.user_scope.role if scope.user_scope else "").strip().upper()
role_map = {
    "PORTFOLIO_MANAGER": "Portfolio Manager",
    "PROGRAM_MANAGER": "Program Manager",
    "SDM": "SDM",
    "PRODUCT_OWNER": "Product Owner",
    "PO": "Product Owner",
    "FINANCE": "Finance",
    "TCO": "Finance",
    "ADMIN": "Finance",
    "VIEWER": "",
}
# Prefer inferred "functional" role for generic access roles (ADMIN/TCO/VIEWER),
# so SDMs/POs don't see themselves as "Finance" when their scope is narrow.
role_display = role_map.get(role_raw, "") or ""
if role_display in {"Finance", ""} and role_inferred and role_inferred != "Finance":
    role_display = role_inferred
elif not role_display:
    role_display = role_inferred
role = role_display

base_raw = fetch_filter_options()
base_df = base_raw if isinstance(base_raw, pd.DataFrame) else pd.DataFrame()

if base_df.empty:
    _budget_status_error("No unified cost data found for this selection.")
    st.info("No unified cost data found (workforce split view is empty).")
    st.stop()

program_disp_map, program_canon_map = _program_lookup_maps()

years = (
    sorted(pd.to_numeric(base_df.get("YEAR"), errors="coerce").dropna().astype(int).unique().tolist(), reverse=True)
    if "YEAR" in base_df.columns
    else []
)
if not years:
    _budget_status_error("No YEAR values available in the unified dataset.")
    st.info("No YEAR values available in the unified dataset.")
    st.stop()

current_year = dt.date.today().year
# Default to the current year when present; otherwise fall back to the latest year in data.
default_years = [current_year] if current_year in years else [years[0]]

# ------------------------------------------------------------
# Filter bar (always visible; applies to all tabs)
# ------------------------------------------------------------
portfolio_map = _program_portfolio_map()
has_portfolio = bool(
    not portfolio_map.empty and portfolio_map.get("PORTFOLIO", pd.Series(dtype=str)).replace("", pd.NA).notna().any()
)

base_program_opts_all = sorted(
    base_df.get("PROGRAMNAME", pd.Series(dtype=str)).dropna().astype(str).str.strip().unique().tolist()
)
base_program_opts_all = [p for p in base_program_opts_all if p]
scope_program_pool = sorted({str(p).strip() for p in (scope.programs or []) if str(p).strip()})
if scope_program_pool:
    base_program_opts_all = sorted(set(base_program_opts_all).union(scope_program_pool))
scope_program_defaults = _dedup_keep_order([p for p in (scope.programs or []) if p in base_program_opts_all])
scope_team_defaults = _dedup_keep_order([t for t in (scope.teams or []) if str(t).strip()])

if has_portfolio:
    f0, f1, f2, f3, f4, f5 = st.columns([1.1, 1.3, 1.8, 1.6, 1.6, 1.2])
else:
    f0, f2, f3, f4, f5 = st.columns([1.1, 1.8, 1.6, 1.6, 1.2])

with f0:
    years_sel = st.multiselect("Year(s)", options=years, default=default_years, key="budget_years")

years_sel = sorted({int(y) for y in (years_sel or []) if y is not None}, reverse=True)
if not years_sel:
    years_sel = [years[0]]
outlook_year = int(max(years_sel))

scope_df = base_df.copy()
try:
    scope_df["YEAR"] = pd.to_numeric(scope_df.get("YEAR"), errors="coerce").astype("Int64")
    scope_df = scope_df[scope_df["YEAR"].isin([int(y) for y in years_sel])]
except Exception:
    scope_df = base_df.copy()

program_opts_all = sorted(scope_df.get("PROGRAMNAME", pd.Series(dtype=str)).dropna().astype(str).str.strip().unique().tolist())
program_opts_all = [p for p in program_opts_all if p]
if scope_program_pool:
    program_opts_all = sorted(set(program_opts_all).union(scope_program_pool))

# ------------------------------------------------------------
# Cascading defaults (program/team/app group) from session state
# ------------------------------------------------------------
# Streamlit widgets only pick up updated `st.session_state[key]` values when those
# values are set BEFORE the widget is rendered. So we pre-align the state here
# (before rendering Program/Team/Application widgets).
try:
    _prog_state = list(st.session_state.get("budget_programs") or [])
    _team_state = [str(t).strip() for t in (st.session_state.get("budget_teams") or []) if str(t).strip()]
    _group_state_sel = None

    scope_names = scope_df.copy()
    scope_names["PROGRAMNAME"] = scope_names.get("PROGRAMNAME", "").astype(str).str.strip()
    scope_names["TEAMNAME"] = scope_names.get("TEAMNAME", "").astype(str).str.strip()
    scope_names["GROUPNAME"] = scope_names.get("GROUPNAME", "").astype(str).str.strip()

    inferred_programs_state: List[str] = []
    inferred_teams_state: List[str] = []

    if _team_state:
        inferred_programs_state = (
            scope_names.loc[scope_names["TEAMNAME"].isin(_team_state), "PROGRAMNAME"]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )

    inferred_programs_state = [p for p in sorted(set(inferred_programs_state)) if p]
    inferred_teams_state = [t for t in sorted(set(inferred_teams_state)) if t]

    if inferred_programs_state and (not _prog_state):
        st.session_state["budget_programs"] = [p for p in inferred_programs_state if p in program_opts_all]
    if inferred_teams_state and (not _team_state):
        st.session_state["budget_teams"] = inferred_teams_state
except Exception:
    pass

portfolio_sel: List[str] = []
if has_portfolio:
    scope_df2 = scope_df.merge(portfolio_map, how="left", on="PROGRAMNAME")
    portfolio_opts = sorted(scope_df2.get("PORTFOLIO", pd.Series(dtype=str)).replace("", pd.NA).dropna().astype(str).unique().tolist())
    # Derive user portfolio defaults from user programs (if any).
    user_portfolios = (
        sorted(
            set(
                portfolio_map.loc[portfolio_map["PROGRAMNAME"].isin(scope_program_defaults), "PORTFOLIO"]
                .replace("", pd.NA)
                .dropna()
                .astype(str)
                .tolist()
            )
        )
        if scope_program_defaults
        else []
    )
    with f1:
        portfolio_sel = st.multiselect(
            "Portfolio",
            options=portfolio_opts,
            default=user_portfolios if role == "Portfolio Manager" else [],
            key="budget_portfolio",
        )
    if portfolio_sel:
        allowed = set(
            portfolio_map.loc[portfolio_map["PORTFOLIO"].astype(str).isin([str(x) for x in portfolio_sel]), "PROGRAMNAME"]
            .dropna()
            .astype(str)
            .tolist()
        )
        program_opts_all = [p for p in program_opts_all if p in allowed]
        scope_df = scope_df[scope_df.get("PROGRAMNAME", pd.Series(dtype=str)).isin(list(allowed))].copy()

def _clean_multiselect_state(key: str, options: Sequence[str]) -> None:
    try:
        cur = st.session_state.get(key)
    except Exception:
        return
    if cur is None:
        return
    try:
        cur_list = list(cur)
    except Exception:
        return
    allowed = set(options)
    cleaned = [v for v in _dedup_keep_order(cur_list) if v in allowed]
    if cleaned != cur_list:
        st.session_state[key] = cleaned


def _clean_selectbox_state(key: str, options: Sequence[str], default_value: str) -> None:
    try:
        cur = st.session_state.get(key)
    except Exception:
        return
    if cur is None:
        return
    if str(cur) not in set(options):
        st.session_state[key] = default_value


@cache_data_portfolio(ttl=300, show_spinner=False)
def _load_contractor_baseline_detail(
    *,
    years: Sequence[int],
    programs: Sequence[str],
    teams: Sequence[str],
    pi_nums: Sequence[int],
) -> pd.DataFrame:
    """Contractor baseline detail (headcount × rate) by team/program/year/PI.

    This mirrors the same contractor baseline source used by Budget Explorer so KPIs can remain consistent.
    """
    years_i = sorted({int(y) for y in years if y is not None and int(y) > 0})
    pi_i = sorted({int(p) for p in pi_nums if p is not None and 0 <= int(p) <= 4})
    programs_i = [str(p).strip() for p in programs if str(p).strip()]
    teams_i = [str(t).strip() for t in teams if str(t).strip()]

    where: List[str] = []
    params: List[Any] = []
    if years_i:
        where.append(f"h.YEAR IN ({', '.join(['%s'] * len(years_i))})")
        params.extend(years_i)
    if pi_i:
        where.append(f"h.PI IN ({', '.join(['%s'] * len(pi_i))})")
        params.extend(pi_i)
    if programs_i:
        ph = ", ".join(["%s"] * len(programs_i))
        where.append(f"UPPER(p.PROGRAMNAME) IN ({ph})")
        params.extend([p.upper() for p in programs_i])
    if teams_i:
        ph = ", ".join(["%s"] * len(teams_i))
        where.append(f"UPPER(t.TEAMNAME) IN ({ph})")
        params.extend([t.upper() for t in teams_i])
    where_sql = " AND ".join(where) if where else "1=1"

    sql_view = f"""
      SELECT
        h.YEAR,
        h.PI,
        p.PROGRAMNAME,
        t.TEAMNAME,
        h.CLASS,
        COALESCE(cc.NAME, h.COMPANYID) AS PROVIDER,
        SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS HEADCOUNT,
        COALESCE(TRY_CONVERT(FLOAT, cr.RATE), 0) AS RATE
      FROM VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE h
      LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
      LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
      LEFT JOIN CONTRACTOR_COMPANY cc ON cc.COMPANYID = h.COMPANYID
      LEFT JOIN VW_CONTRACTOR_RATE_EFFECTIVE cr
        ON cr.COMPANYID = h.COMPANYID AND cr.YEAR = h.YEAR AND cr.PI = h.PI AND cr.CLASS = h.CLASS
      WHERE {where_sql}
      GROUP BY h.YEAR, h.PI, p.PROGRAMNAME, t.TEAMNAME, h.CLASS, COALESCE(cc.NAME, h.COMPANYID), COALESCE(TRY_CONVERT(FLOAT, cr.RATE), 0)
    """
    try:
        df = fetch_df(sql_view, tuple(params) if params else None)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    sql_fallback = sql_view.replace("VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE", "TEAM_CONTRACTOR_HEADCOUNT")
    try:
        df = fetch_df(sql_fallback, tuple(params) if params else None)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# Cascade behavior (priority: Program → Team):
# - Program options come from the current scope (do not depend on a previously selected Application).
# - Team options are narrowed by selected Program(s) (and previously selected Application, if any).
# - Application options are narrowed by selected Program(s) and Team(s).
#
# Rationale: Application is downstream of Team/Program. If Program options are narrowed by an old Application
# selection, selecting a Team may not be able to auto-select its Program due to missing options.
st.session_state.pop("budget_group", None)
group_state_sel = None

program_opts_all = sorted(scope_df.get("PROGRAMNAME", pd.Series(dtype=str)).dropna().astype(str).str.strip().unique().tolist())
program_opts_all = [p for p in program_opts_all if p]
_clean_multiselect_state("budget_programs", program_opts_all)

# If a Team is already selected (e.g. SDM default scope), align Program(s) *before* rendering the widget.
# This avoids the "Choose options" empty state on first load because the Program multiselect is rendered
# before the Team multiselect.
try:
    current_program_state = list(st.session_state.get("budget_programs") or [])
    team_hint = list(st.session_state.get("budget_teams") or [])
    if not team_hint and scope_team_defaults and role != "Portfolio Manager":
        team_hint = list(scope_team_defaults)
    team_hint = [str(t).strip() for t in team_hint if str(t).strip()]

    if (not current_program_state) and team_hint:
        scope_df_names = scope_df.copy()
        scope_df_names["PROGRAMNAME"] = scope_df_names.get("PROGRAMNAME", "").astype(str).str.strip()
        scope_df_names["TEAMNAME"] = scope_df_names.get("TEAMNAME", "").astype(str).str.strip()
        team_hint_norm = {str(t).strip().upper() for t in team_hint if str(t).strip()}
        inferred_programs = (
            scope_df_names.loc[scope_df_names["TEAMNAME"].astype(str).str.upper().isin(team_hint_norm), "PROGRAMNAME"]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )
        # Fallback: if the unified dataset is missing the mapping for a team in scope,
        # infer the program from master data (TEAMS → PROGRAMS). This keeps the Program
        # multiselect from showing as empty ("Choose options") when a Team default exists.
        if not inferred_programs:
            try:
                ph = ", ".join(["%s"] * len(team_hint_norm))
                df_tp = fetch_df(
                    f"""
                    SELECT
                      t.TEAMNAME,
                      p.PROGRAMNAME
                    FROM TEAMS t
                    LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                    WHERE UPPER(t.TEAMNAME) IN ({ph})
                    """,
                    tuple(sorted(team_hint_norm)),
                )
                if df_tp is not None and not df_tp.empty and "PROGRAMNAME" in df_tp.columns:
                    inferred_programs = (
                        df_tp["PROGRAMNAME"].dropna().astype(str).str.strip().replace({"": pd.NA}).dropna().tolist()
                    )
            except Exception:
                inferred_programs = []
        inferred_programs = [p for p in sorted(set(inferred_programs)) if p and p in set(program_opts_all)]
        if inferred_programs:
            st.session_state["budget_programs"] = inferred_programs
except Exception:
    pass

with f2:
    programs_sel = st.multiselect(
        "Program(s)",
        options=program_opts_all,
        default=[p for p in scope_program_defaults if p in program_opts_all] if role in {"Program Manager", "Portfolio Manager"} else [],
        key="budget_programs",
        help="Leave empty for all programs in scope.",
        format_func=lambda x: _program_display_name(x, program_disp_map),
    )
programs_sel = _dedup_keep_order(programs_sel)

df_for_teams = scope_df.copy()
if programs_sel:
    df_for_teams = df_for_teams[df_for_teams.get("PROGRAMNAME", pd.Series(dtype=str)).isin(programs_sel)].copy()

team_opts = sorted(df_for_teams.get("TEAMNAME", pd.Series(dtype=str)).dropna().astype(str).str.strip().unique().tolist())
team_opts = [t for t in team_opts if t]
_clean_multiselect_state("budget_teams", team_opts)
with f3:
    teams_sel = st.multiselect(
        "Team(s)",
        options=team_opts,
        default=[t for t in scope_team_defaults if t in team_opts] if scope_team_defaults and role != "Portfolio Manager" else [],
        key="budget_teams",
        help="Leave empty for all teams in scope.",
    )
teams_sel = _dedup_keep_order(teams_sel)

group_sel = None

# If the user selects a Team or Application, keep Program(s) aligned with that selection to avoid scope confusion.
try:
    scope_df_names = scope_df.copy()
    scope_df_names["PROGRAMNAME"] = scope_df_names.get("PROGRAMNAME", "").astype(str).str.strip()
    scope_df_names["TEAMNAME"] = scope_df_names.get("TEAMNAME", "").astype(str).str.strip()
    scope_df_names["GROUPNAME"] = scope_df_names.get("GROUPNAME", "").astype(str).str.strip()

    inferred_programs: List[str] = []
    inferred_teams: List[str] = []
    if teams_sel:
        teams_sel_norm = {str(t).strip().upper() for t in (teams_sel or []) if str(t).strip()}
        inferred_programs = (
            scope_df_names.loc[scope_df_names["TEAMNAME"].astype(str).str.upper().isin(teams_sel_norm), "PROGRAMNAME"]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )

    inferred_programs = [p for p in sorted(set(inferred_programs)) if p]
    inferred_teams = [t for t in sorted(set(inferred_teams)) if t]

    changed = False
    if inferred_programs:
        current_programs = list(st.session_state.get("budget_programs") or [])
        if sorted(current_programs) != sorted(inferred_programs):
            st.session_state["budget_programs"] = [p for p in inferred_programs if p in program_opts_all]
            changed = True
    if inferred_teams:
        current_teams = list(st.session_state.get("budget_teams") or [])
        if (not current_teams) and inferred_teams:
            st.session_state["budget_teams"] = [t for t in inferred_teams if t in team_opts]
            changed = True
    if changed:
        st.rerun()
except Exception:
    pass
st.session_state.pop("budget_pis", None)
pi_nums = [1, 2, 3, 4]

scope_key = (tuple(years_sel), tuple(sorted(portfolio_sel)), tuple(sorted(programs_sel)), str(group_sel or ""), tuple(sorted(teams_sel)), tuple(pi_nums))
prev_scope_key = st.session_state.get("_budget_scope_key")
rev = int(st.session_state.get("_budget_rev", 0))
if prev_scope_key and prev_scope_key != scope_key:
    rev += 1
st.session_state["_budget_scope_key"] = scope_key
st.session_state["_budget_rev"] = rev

programs_for_query: List[str] = _expand_program_filters(list(programs_sel or []), program_canon_map)
# If the user filtered by Team/Application but not by Program, infer the program scope from the filtered dataset
# so program-level queries (overhead, additional costs, etc.) still respect the user's selection.
if not programs_for_query:
    inferred: List[str] = []
    try:
        if teams_sel and "TEAMNAME" in scope_df.columns and "PROGRAMNAME" in scope_df.columns:
            inferred = (
                scope_df.loc[scope_df.get("TEAMNAME", pd.Series(dtype=str)).astype(str).isin([str(t) for t in teams_sel]), "PROGRAMNAME"]
                .dropna()
                .astype(str)
                .str.strip()
                .tolist()
            )
        if (not inferred) and group_sel and "GROUPNAME" in scope_df.columns and "PROGRAMNAME" in scope_df.columns:
            inferred = (
                scope_df.loc[scope_df.get("GROUPNAME", pd.Series(dtype=str)).astype(str).str.strip() == str(group_sel), "PROGRAMNAME"]
                .dropna()
                .astype(str)
                .str.strip()
                .tolist()
            )
    except Exception:
        inferred = []
    programs_for_query = _expand_program_filters([p for p in sorted(set(inferred)) if p], program_canon_map)

groups_for_delivery: List[str] = [group_sel] if group_sel else []
# IMPORTANT: pass empty lists to downstream helpers when the user didn't filter,
# so we don't generate massive "IN (...)" SQL clauses.
effective_programs = list(programs_for_query)
effective_teams = list(teams_sel or [])
effective_groups = list(groups_for_delivery)

custom_scope = (
    set(programs_sel or []) != set(scope.programs or [])
    or set(teams_sel or []) != set(scope.teams or [])
    or set([group_sel] if group_sel else []) != set(scope.groups or [])
    or set(years_sel or []) != set(default_years or [])
)
view_label = "Viewing: Custom" if custom_scope else scope_label(scope, role)
scope_note = []
if portfolio_sel:
    scope_note.append(f"Portfolio: {', '.join(portfolio_sel[:2])}{'…' if len(portfolio_sel) > 2 else ''}")
if programs_sel:
    _ps = _dedup_keep_order([_program_display_name(p, program_disp_map) for p in programs_sel])
    scope_note.append(f"Programs: {', '.join(_ps[:2])}{'…' if len(_ps) > 2 else ''}")
if teams_sel:
    _ts = _dedup_keep_order(teams_sel)
    if len(_ts) > 1:
        scope_note.append(f"Teams: {', '.join(_ts[:2])}{'…' if len(_ts) > 2 else ''}")
if group_sel:
    scope_note.append(f"Application: {group_sel}")
parts = [view_label]
if scope_note:
    parts.extend(scope_note)
st.caption(" | ".join(parts))

if scope.user_scope and scope.user_scope.errors:
    st.info("Scope note: " + " ".join([str(e) for e in scope.user_scope.errors if str(e).strip()][:2]))

# Use the session-cached canonical cost model (Baseline/Expected/Actual) as the single source of truth
# for scenario cost lines. Build one scoped frame per scenario and reuse it everywhere in the page.
_budget_status_step("Loading baseline and expected cost lines...")
df_baseline_scope, df_expected_scope = _build_budget_scope_frames(
    years_sel=years_sel,
    pi_sel=pi_nums,
    programs_sel=programs_for_query,
    teams_sel=teams_sel,
    app_groups_sel=[group_sel] if group_sel else [],
)
if (df_baseline_scope is None or df_baseline_scope.empty) and (df_expected_scope is None or df_expected_scope.empty):
    _budget_status_error("No cost rows found for this selection.")
    st.info("No cost rows found for this selection.")
    st.stop()
if df_expected_scope is None or df_expected_scope.empty:
    _budget_status_error("No EXPECTED cost rows found for this selection.")
    st.info("No EXPECTED cost rows found for this selection.")
    st.stop()

work_scope = df_expected_scope.copy()
baseline_scope = df_baseline_scope.copy() if df_baseline_scope is not None else pd.DataFrame()

src_up = work_scope.get("SOURCE", "").astype(str).str.upper()
sub_up = work_scope.get("SUBCOMPONENT", "").astype(str).str.upper()
is_ado = src_up.eq("ADO")
is_wf = work_scope["CAT"].eq("WORK_FORCE")

# Overhead people cost inputs across selected years (for PI filter too).
ov_inputs_all = []
for y in sorted(set(int(x) for x in years_sel)):
    df_ov_y = _load_program_overhead_people_cost_inputs(year=int(y), programs=programs_for_query)
    if df_ov_y is not None and not df_ov_y.empty:
        ov_inputs_all.append(df_ov_y)
_overhead_inputs_all = pd.concat(ov_inputs_all, ignore_index=True) if ov_inputs_all else pd.DataFrame()

if not _overhead_inputs_all.empty:
    ov = _overhead_inputs_all.copy()
    ov["YEAR"] = pd.to_numeric(ov.get("YEAR"), errors="coerce").astype("Int64")
    ov["PI"] = pd.to_numeric(ov.get("PI"), errors="coerce").fillna(0).astype(int)
    ov = ov[ov["PI"].isin([int(p) for p in pi_nums])].copy() if pi_nums else ov
    ov["PROGRAMFTE"] = pd.to_numeric(ov.get("PROGRAMFTE"), errors="coerce").fillna(0.0)
    ov["PROGRAM_XOM_RATE"] = pd.to_numeric(ov.get("PROGRAM_XOM_RATE"), errors="coerce").fillna(0.0)
    overhead_people_primary = float((ov["PROGRAMFTE"] * ov["PROGRAM_XOM_RATE"]).sum() or 0.0)
else:
    overhead_people_primary = 0.0

# Expected spend: canonical scenario totals (program overhead correction is centralized in `core/canonical_costs`).
overhead_people_cost_from_view = float(work_scope.loc[is_ado & is_wf & sub_up.eq("PROGRAM"), "AMOUNT"].sum() or 0.0)
expected_total_from_view = float(work_scope["AMOUNT"].sum() or 0.0)
expected_people_from_view = float(work_scope.loc[work_scope["CAT"].eq("WORK_FORCE"), "AMOUNT"].sum() or 0.0)

expected_total = float(expected_total_from_view)
expected_people_total = float(expected_people_from_view)
expected_other_total = float(expected_total - expected_people_total)

# --- Tabs (fixed order) ---
tab_names = ["Overview", "Budget Explorer", "Capacity"]
tabs = st.tabs(tab_names)
tabs_by_name = {name: tab for name, tab in zip(tab_names, tabs)}

# Preload demand/capacity datasets used across multiple tabs.
cap_fraction = _roadmap_capacity_fraction()

_budget_status_step("Loading ADO demand (explorer) data...")
demand_multi_all = load_explorer_fte_by_group(
    years=[int(y) for y in years_sel],
    programs=tuple(effective_programs),
    teams=tuple(effective_teams),
    groups=tuple(effective_groups),
    pi_nums=tuple(pi_nums),
)
demand_multi_all = demand_multi_all if demand_multi_all is not None else pd.DataFrame()

# Weighting dataset for app-group attribution:
# If the user filtered to a single Application, we still want weights computed against the full
# team scope (all app groups) so baseline/capacity attribution doesn't incorrectly become 100%.
demand_for_weights = demand_multi_all
if group_sel:
    _budget_status_step("Recomputing demand weights for selected application scope...")
    demand_for_weights = load_explorer_fte_by_group(
        years=[int(y) for y in years_sel],
        programs=tuple(effective_programs),
        teams=tuple(effective_teams),
        groups=tuple(),
        pi_nums=tuple(pi_nums),
    )
    demand_for_weights = demand_for_weights if demand_for_weights is not None else pd.DataFrame()

# Explorer demand FTE used for weighting across app groups (may include all groups even when filtered).
eff_by_group_weights = pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "DERIVED_FTE_EXPLORER"])
if demand_for_weights is not None and not demand_for_weights.empty:
    eff_by_group_weights = demand_for_weights.copy()
    eff_by_group_weights["YEAR"] = pd.to_numeric(eff_by_group_weights.get("ADO_YEAR"), errors="coerce").astype("Int64")
    eff_by_group_weights["PI"] = pd.to_numeric(eff_by_group_weights.get("PI_NUM"), errors="coerce").astype("Int64")
    eff_by_group_weights["PROGRAMNAME"] = eff_by_group_weights.get("PROGRAMNAME", "").astype(str).str.strip()
    eff_by_group_weights["TEAMNAME"] = eff_by_group_weights.get("TEAMNAME", "").astype(str).str.strip()
    eff_by_group_weights["GROUPNAME"] = eff_by_group_weights.get("GROUPNAME", "").astype(str).str.strip()
    wcols = ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"]
    eff_by_group_weights = (
        eff_by_group_weights.groupby(wcols, dropna=False)
        .agg(DERIVED_FTE_EXPLORER=("DERIVED_FTE_SUM", "sum"))
        .reset_index()
    )
    eff_by_group_weights["DERIVED_FTE_EXPLORER"] = pd.to_numeric(
        eff_by_group_weights.get("DERIVED_FTE_EXPLORER"), errors="coerce"
    ).fillna(0.0)

# Canonical demand FTE (Explorer v2) by (Year, PI, Program, Team, Application).
eff_by_group = pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "DERIVED_FTE_EXPLORER"])
if demand_multi_all is not None and not demand_multi_all.empty:
    eff_by_group = demand_multi_all.copy()
    eff_by_group["YEAR"] = pd.to_numeric(eff_by_group.get("ADO_YEAR"), errors="coerce").astype("Int64")
    eff_by_group["PI"] = pd.to_numeric(eff_by_group.get("PI_NUM"), errors="coerce").astype("Int64")
    eff_by_group["PROGRAMNAME"] = eff_by_group.get("PROGRAMNAME", "").astype(str).str.strip()
    eff_by_group["TEAMNAME"] = eff_by_group.get("TEAMNAME", "").astype(str).str.strip()
    eff_by_group["GROUPNAME"] = eff_by_group.get("GROUPNAME", "").astype(str).str.strip()
    gcols = ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"]
    eff_by_group = (
        eff_by_group.groupby(gcols, dropna=False)
        .agg(DERIVED_FTE_EXPLORER=("DERIVED_FTE_SUM", "sum"))
        .reset_index()
    )
    eff_by_group["DERIVED_FTE_EXPLORER"] = pd.to_numeric(eff_by_group.get("DERIVED_FTE_EXPLORER"), errors="coerce").fillna(0.0)

# Join Explorer FTE into the unified cost rows so Budget drilldowns can use a single canonical FTE.
work_scope = work_scope.copy()
work_scope["YEAR"] = pd.to_numeric(work_scope.get("YEAR"), errors="coerce").astype("Int64")
work_scope["PI"] = pd.to_numeric(work_scope.get("PI"), errors="coerce").astype("Int64")
work_scope["PROGRAMNAME"] = work_scope.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
work_scope["TEAMNAME"] = work_scope.get("TEAMNAME", "").fillna("").astype(str).str.strip()
work_scope["GROUPNAME"] = work_scope.get("GROUPNAME", "").fillna("").astype(str).str.strip()
if eff_by_group is not None and not eff_by_group.empty:
    work_scope = work_scope.merge(
        eff_by_group,
        on=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"],
        how="left",
    )
    work_scope["DERIVED_FTE_EXPLORER"] = pd.to_numeric(work_scope.get("DERIVED_FTE_EXPLORER"), errors="coerce").fillna(0.0)
else:
    work_scope["DERIVED_FTE_EXPLORER"] = 0.0

_budget_status_step("Loading allocated capacity and baseline contractor inputs...")
cap_multi_all = _load_allocated_capacity_team_pi_years(
    years=[int(y) for y in years_sel],
    programs=effective_programs,
    teams=effective_teams,
)
cap_multi_all = cap_multi_all if cap_multi_all is not None else pd.DataFrame()

# -------------------------------------------------------------------
# Planned baseline (WF people) driven by the SAME capacity model as the Capacity tab.
#
# - Demand FTE is derived from ADO (Explorer v2).
# - Capacity FTE comes from `VW_TEAM_ALLOCATED_HEADCOUNT_PI` and is scaled by `roadmap_capacity_fraction`.
# - Planned baseline people cost is computed as: Capacity_FTE × rate (per FTE per PI).
#
# This makes WF baseline dollars consistent with the same staffing reality used in Demand vs Capacity.
# NWF baseline remains view-driven (contracts/invoices + non-ADO rows).
# -------------------------------------------------------------------

# NOTE: `work_scope` is merged/enriched above (Explorer FTE join), so re-compute masks here to ensure index alignment.
src_up_scope = work_scope.get("SOURCE", "").astype(str).str.upper()
is_ado_scope = src_up_scope.eq("ADO")
is_wf_scope = work_scope.get("CAT", pd.Series("", index=work_scope.index, dtype="object")).astype(str).eq("WORK_FORCE")
is_nwf_scope = work_scope.get("CAT", pd.Series("", index=work_scope.index, dtype="object")).astype(str).eq("NON_WORK_FORCE")

# Baseline other costs (NWF): keep existing behavior (non-ADO NWF rows in the unified view).
baseline_other_total = float(work_scope.loc[is_nwf_scope & ~is_ado_scope, "AMOUNT"].sum() or 0.0)

# Non-ADO WF (rare): keep existing behavior.
baseline_wf_other = float(work_scope.loc[(~is_ado_scope) & is_wf_scope, "AMOUNT"].sum() or 0.0)

# Contractor baseline (KPI): use the same contractor baseline source as Budget Explorer.
# - Contractor C contributes to WF planned baseline.
# - Contractor CS contributes to NWF planned baseline.
contractor_c_wf_total = 0.0
contractor_cs_nwf_total = 0.0
contractor_kpi_long = pd.DataFrame(
    columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE", "PLANNED_BASELINE"]
)
try:
    contractor_df = _load_contractor_baseline_detail(
        years=[int(y) for y in years_sel],
        programs=programs_for_query,
        teams=teams_sel,
        pi_nums=pi_nums,
    )
    contractor_df = contractor_df if contractor_df is not None else pd.DataFrame()
    if not contractor_df.empty:
        cdf = contractor_df.copy()
        cdf["YEAR"] = pd.to_numeric(cdf.get("YEAR"), errors="coerce").astype("Int64")
        cdf["PI"] = pd.to_numeric(cdf.get("PI"), errors="coerce").astype("Int64")
        cdf["PROGRAMNAME"] = cdf.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
        cdf["TEAMNAME"] = cdf.get("TEAMNAME", "").fillna("").astype(str).str.strip()
        cdf["CLASS"] = cdf.get("CLASS", "").fillna("").astype(str).str.strip()
        cdf["HEADCOUNT"] = pd.to_numeric(cdf.get("HEADCOUNT"), errors="coerce").fillna(0.0)
        cdf["RATE"] = pd.to_numeric(cdf.get("RATE"), errors="coerce").fillna(0.0)
        cdf["PLANNED_BASELINE"] = cdf["HEADCOUNT"] * cdf["RATE"]

        class_up = cdf["CLASS"].astype(str).str.upper().str.replace("-", " ").str.replace("_", " ").str.strip()
        is_c = class_up.isin({"CONTRACTOR C", "CONTRACTORC"})
        is_cs = class_up.isin({"CONTRACTOR CS", "CONTRACTORCS"})

        contractor_c_wf_total = float(cdf.loc[is_c, "PLANNED_BASELINE"].sum() or 0.0)
        contractor_cs_nwf_total = float(cdf.loc[is_cs, "PLANNED_BASELINE"].sum() or 0.0)

        contractor_kpi_long = pd.concat(
            [
                cdf.loc[is_c, ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "PLANNED_BASELINE"]]
                .assign(CLASS="CONTRACTOR C", COST_TYPE="WF"),
                cdf.loc[is_cs, ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "PLANNED_BASELINE"]]
                .assign(CLASS="CONTRACTOR CS", COST_TYPE="NWF"),
            ],
            ignore_index=True,
        )
except Exception:
    contractor_c_wf_total = 0.0
    contractor_cs_nwf_total = 0.0
    contractor_kpi_long = pd.DataFrame(
        columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE", "PLANNED_BASELINE"]
    )

wf_baseline_cap_team_pi = pd.DataFrame(
    columns=[
        "YEAR",
        "PI",
        "PROGRAMID",
        "PROGRAMNAME",
        "TEAMID",
        "TEAMNAME",
        "CAPACITY_FTE",
        "CAPACITY_TEAM_FTE",
        "CAPACITY_DELIVERY_FTE",
        "RATE_PER_FTE_PI",
        "WF_BASELINE_TEAM_PI",
        "WF_BASELINE_DELIVERY_PI",
        "WF_BASELINE_TOTAL_PI",
    ]
)
wf_baseline_cap_by_group_sub = pd.DataFrame(
    columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "_SUB_UP", "PLANNED_BASELINE_WF"]
)

baseline_people_capacity_total = 0.0
wf_baseline_people_by_pi = pd.DataFrame(columns=["YEAR", "PI", "WF_BASELINE_CAPACITY"])
wf_baseline_people_by_prog = pd.DataFrame(columns=["PROGRAMNAME", "WF_BASELINE_CAPACITY"])

_budget_status_step("Computing baseline capacity and rates...")

try:
    if cap_multi_all is not None and not cap_multi_all.empty:
        capd = cap_multi_all.copy()
        capd["YEAR"] = pd.to_numeric(capd.get("ADO_YEAR"), errors="coerce").astype("Int64")
        capd["PI"] = pd.to_numeric(capd.get("ITERATION_NUM"), errors="coerce").astype("Int64")
        capd["TEAMID"] = capd.get("TEAMID", "").astype(str).str.strip()
        capd["PROGRAMID"] = capd.get("PROGRAMID", "").astype(str).str.strip()
        capd["TEAMNAME"] = capd.get("TEAMNAME", "").astype(str).str.strip()
        capd["PROGRAMNAME"] = capd.get("PROGRAMNAME", "").astype(str).str.strip()
        capd = capd[capd["PI"].isin([int(p) for p in pi_nums])].copy() if pi_nums else capd

        capd["ALLOCATED_HEADCOUNT"] = pd.to_numeric(capd.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
        capd["DELIVERY_HEADCOUNT"] = pd.to_numeric(capd.get("DELIVERY_HEADCOUNT"), errors="coerce").fillna(0.0)

        capd["CAPACITY_FTE"] = capd["ALLOCATED_HEADCOUNT"] * float(cap_fraction)
        capd["CAPACITY_DELIVERY_FTE"] = capd["DELIVERY_HEADCOUNT"] * float(cap_fraction)
        capd["CAPACITY_TEAM_FTE"] = (capd["ALLOCATED_HEADCOUNT"] - capd["DELIVERY_HEADCOUNT"]).clip(lower=0.0) * float(
            cap_fraction
        )

        # ---------------------------------------------------------------------
        # HOTFIX (temporary): ERNE WEST Team Overhead FTE override
        # Some environments have an incorrect TEAM (overhead) capacity inferred for
        # this team/year (e.g. 5 instead of 3). This override forces the Team
        # capacity used for WF baseline + Budget Explorer to 3 FTE per PI.
        #
        # TODO(bruno): remove once TEAMS headcount configuration is corrected.
        # ---------------------------------------------------------------------
        try:
            _mask_hotfix = (
                (capd["YEAR"].astype("Int64") == 2025)
                & (capd["PROGRAMNAME"].astype(str).str.upper().str.strip() == "R&M")
                & (capd["TEAMNAME"].astype(str).str.upper().str.strip() == "ERNE WEST")
            )
            if bool(_mask_hotfix.any()):
                capd.loc[_mask_hotfix, "CAPACITY_TEAM_FTE"] = 3.0
                # Keep the aggregate capacity column coherent for this override row.
                capd.loc[_mask_hotfix, "CAPACITY_FTE"] = (
                    pd.to_numeric(capd.loc[_mask_hotfix, "CAPACITY_TEAM_FTE"], errors="coerce").fillna(0.0)
                    + pd.to_numeric(capd.loc[_mask_hotfix, "CAPACITY_DELIVERY_FTE"], errors="coerce").fillna(0.0)
                )
        except Exception:
            pass

        gcols = ["YEAR", "PI", "PROGRAMID", "PROGRAMNAME", "TEAMID", "TEAMNAME"]
        wf_baseline_cap_team_pi = (
            capd.groupby(gcols, dropna=False)[["CAPACITY_FTE", "CAPACITY_TEAM_FTE", "CAPACITY_DELIVERY_FTE"]]
            .sum()
            .reset_index()
        )

        years_for_rates = [int(y) for y in years_sel]
        team_ids = [t for t in wf_baseline_cap_team_pi["TEAMID"].dropna().astype(str).tolist() if t]
        prog_ids = [p for p in wf_baseline_cap_team_pi["PROGRAMID"].dropna().astype(str).tolist() if p]
        pi_list = [int(p) for p in (pi_nums or [1, 2, 3, 4]) if 1 <= int(p) <= 4]

        # WF rates come from the effective-rate views so Budget Explorer matches the Rates page:
        # - `VW_TEAM_RATE_EFFECTIVE`
        # - `VW_PROGRAM_RATE_EFFECTIVE`
        team_rates = _load_team_rate_history(years=years_for_rates, team_ids=team_ids)
        prog_rates = _load_program_rate_history(years=years_for_rates, program_ids=prog_ids)

        def _best_pi_and_year(
            df: pd.DataFrame,
            *,
            id_col: str,
            rate_col: str,
            out_rate_col: str,
            valid_years: Sequence[int] = (),
        ) -> Tuple[pd.DataFrame, pd.DataFrame]:
            if df is None or df.empty:
                return pd.DataFrame(columns=[id_col, "YEAR", "PI", out_rate_col]), pd.DataFrame(columns=[id_col, "YEAR", out_rate_col])
            work_all = df.copy()
            work_all[id_col] = work_all.get(id_col, "").astype(str).str.strip()
            work_all["YEAR"] = pd.to_numeric(work_all.get("YEAR"), errors="coerce").astype("Int64")
            work_all["PI"] = pd.to_numeric(work_all.get("PI"), errors="coerce").astype("Int64")
            work_all[rate_col] = pd.to_numeric(work_all.get(rate_col), errors="coerce")
            work_all["_LOC_RANK"] = pd.to_numeric(work_all.get("_LOC_RANK"), errors="coerce").fillna(9).astype(int)
            work_all["UPDATED_AT"] = pd.to_datetime(work_all.get("UPDATED_AT"), errors="coerce")
            work_all = work_all[work_all[id_col].ne("") & work_all["YEAR"].notna()].copy()
            work_all = work_all[work_all[rate_col].notna() & (work_all[rate_col] > 0)].copy()
            if work_all.empty:
                return pd.DataFrame(columns=[id_col, "YEAR", "PI", out_rate_col]), pd.DataFrame(columns=[id_col, "YEAR", out_rate_col])

            years_pref = sorted({int(y) for y in (valid_years or []) if y is not None and int(y) > 0})
            available_years = set(pd.to_numeric(work_all["YEAR"], errors="coerce").dropna().astype(int).tolist())

            work_pref = work_all
            if years_pref and any(y in available_years for y in years_pref):
                work_pref = work_all.loc[work_all["YEAR"].isin(years_pref)].copy()

            # Preferred: use the requested year(s) when available.
            work_pref = work_pref.sort_values([id_col, "YEAR", "PI", "_LOC_RANK", "UPDATED_AT"], ascending=[True, True, True, True, False])
            pref_pi = (
                work_pref.loc[work_pref["PI"].isin(pi_list)]
                .drop_duplicates([id_col, "YEAR", "PI"], keep="first")[[id_col, "YEAR", "PI", rate_col]]
                .rename(columns={rate_col: out_rate_col})
            )
            pref_0 = (
                work_pref.loc[work_pref["PI"].fillna(0).astype(int) == 0]
                .drop_duplicates([id_col, "YEAR"], keep="first")[[id_col, "YEAR", rate_col]]
                .rename(columns={rate_col: out_rate_col})
            )

            # Fallback: if a requested year is missing, use the latest available year for that id (per PI or PI=0).
            latest_pi = (
                work_all.loc[work_all["PI"].isin(pi_list)]
                .sort_values([id_col, "PI", "YEAR", "_LOC_RANK", "UPDATED_AT"], ascending=[True, True, False, True, False])
                .drop_duplicates([id_col, "PI"], keep="first")[[id_col, "PI", rate_col]]
                .rename(columns={rate_col: f"{out_rate_col}__FALLBACK"})
            )
            latest_0 = (
                work_all.loc[work_all["PI"].fillna(0).astype(int) == 0]
                .sort_values([id_col, "YEAR", "_LOC_RANK", "UPDATED_AT"], ascending=[True, False, True, False])
                .drop_duplicates([id_col], keep="first")[[id_col, rate_col]]
                .rename(columns={rate_col: f"{out_rate_col}__FALLBACK"})
            )

            ids = sorted(set(work_all[id_col].dropna().astype(str).tolist()))
            years_target = years_pref if years_pref else sorted(available_years)
            if not years_target or not ids:
                return pd.DataFrame(columns=[id_col, "YEAR", "PI", out_rate_col]), pd.DataFrame(columns=[id_col, "YEAR", out_rate_col])

            # Build a small grid for the requested scope and fill from preferred, then fallback.
            idx_pi = pd.MultiIndex.from_product([ids, years_target, pi_list], names=[id_col, "YEAR", "PI"]).to_frame(index=False)
            best_pi = idx_pi.merge(pref_pi, on=[id_col, "YEAR", "PI"], how="left")
            best_pi = best_pi.merge(latest_pi, on=[id_col, "PI"], how="left")
            best_pi[out_rate_col] = pd.to_numeric(best_pi.get(out_rate_col), errors="coerce").fillna(
                pd.to_numeric(best_pi.get(f"{out_rate_col}__FALLBACK"), errors="coerce")
            )
            best_pi = best_pi.drop(columns=[f"{out_rate_col}__FALLBACK"], errors="ignore")
            best_pi[out_rate_col] = pd.to_numeric(best_pi.get(out_rate_col), errors="coerce")
            best_pi = best_pi[best_pi[out_rate_col].notna() & (best_pi[out_rate_col] > 0)].copy()
            best_pi["YEAR"] = pd.to_numeric(best_pi.get("YEAR"), errors="coerce").astype("Int64")
            best_pi["PI"] = pd.to_numeric(best_pi.get("PI"), errors="coerce").astype("Int64")

            idx_0 = pd.MultiIndex.from_product([ids, years_target], names=[id_col, "YEAR"]).to_frame(index=False)
            best_0 = idx_0.merge(pref_0, on=[id_col, "YEAR"], how="left")
            best_0 = best_0.merge(latest_0, on=[id_col], how="left")
            best_0[out_rate_col] = pd.to_numeric(best_0.get(out_rate_col), errors="coerce").fillna(
                pd.to_numeric(best_0.get(f"{out_rate_col}__FALLBACK"), errors="coerce")
            )
            best_0 = best_0.drop(columns=[f"{out_rate_col}__FALLBACK"], errors="ignore")
            best_0[out_rate_col] = pd.to_numeric(best_0.get(out_rate_col), errors="coerce")
            best_0 = best_0[best_0[out_rate_col].notna() & (best_0[out_rate_col] > 0)].copy()
            best_0["YEAR"] = pd.to_numeric(best_0.get("YEAR"), errors="coerce").astype("Int64")
            return best_pi, best_0

        team_best_pi, team_best_0 = _best_pi_and_year(
            team_rates,
            id_col="TEAMID",
            rate_col="XOM_RATE",
            out_rate_col="TEAM_XOM_RATE",
            valid_years=years_for_rates,
        )
        prog_best_pi, prog_best_0 = _best_pi_and_year(
            prog_rates,
            id_col="PROGRAMID",
            rate_col="PROGRAM_XOM_RATE",
            out_rate_col="PROGRAM_XOM_RATE",
            valid_years=years_for_rates,
        )

        wf_baseline_cap_team_pi["YEAR"] = pd.to_numeric(wf_baseline_cap_team_pi.get("YEAR"), errors="coerce").astype("Int64")
        wf_baseline_cap_team_pi["PI"] = pd.to_numeric(wf_baseline_cap_team_pi.get("PI"), errors="coerce").astype("Int64")
        wf_baseline_cap_team_pi["TEAMID"] = wf_baseline_cap_team_pi.get("TEAMID", "").astype(str).str.strip()
        wf_baseline_cap_team_pi["PROGRAMID"] = wf_baseline_cap_team_pi.get("PROGRAMID", "").astype(str).str.strip()

        wf_baseline_cap_team_pi = wf_baseline_cap_team_pi.merge(team_best_pi, on=["TEAMID", "YEAR", "PI"], how="left")
        wf_baseline_cap_team_pi = wf_baseline_cap_team_pi.merge(
            team_best_0.rename(columns={"TEAM_XOM_RATE": "TEAM_XOM_RATE_0"}), on=["TEAMID", "YEAR"], how="left"
        )
        wf_baseline_cap_team_pi = wf_baseline_cap_team_pi.merge(prog_best_pi, on=["PROGRAMID", "YEAR", "PI"], how="left")
        wf_baseline_cap_team_pi = wf_baseline_cap_team_pi.merge(
            prog_best_0.rename(columns={"PROGRAM_XOM_RATE": "PROGRAM_XOM_RATE_0"}), on=["PROGRAMID", "YEAR"], how="left"
        )

        wf_baseline_cap_team_pi["RATE_PER_FTE_PI"] = pd.to_numeric(
            wf_baseline_cap_team_pi.get("TEAM_XOM_RATE"), errors="coerce"
        )
        wf_baseline_cap_team_pi["RATE_PER_FTE_PI"] = wf_baseline_cap_team_pi["RATE_PER_FTE_PI"].fillna(
            pd.to_numeric(wf_baseline_cap_team_pi.get("TEAM_XOM_RATE_0"), errors="coerce")
        )
        wf_baseline_cap_team_pi["RATE_PER_FTE_PI"] = wf_baseline_cap_team_pi["RATE_PER_FTE_PI"].fillna(
            pd.to_numeric(wf_baseline_cap_team_pi.get("PROGRAM_XOM_RATE"), errors="coerce")
        )
        wf_baseline_cap_team_pi["RATE_PER_FTE_PI"] = wf_baseline_cap_team_pi["RATE_PER_FTE_PI"].fillna(
            pd.to_numeric(wf_baseline_cap_team_pi.get("PROGRAM_XOM_RATE_0"), errors="coerce")
        )
        wf_baseline_cap_team_pi["RATE_PER_FTE_PI"] = wf_baseline_cap_team_pi["RATE_PER_FTE_PI"].fillna(0.0)

        for c in ["CAPACITY_FTE", "CAPACITY_TEAM_FTE", "CAPACITY_DELIVERY_FTE"]:
            wf_baseline_cap_team_pi[c] = pd.to_numeric(wf_baseline_cap_team_pi.get(c), errors="coerce").fillna(0.0)

        wf_baseline_cap_team_pi["WF_BASELINE_TEAM_PI"] = wf_baseline_cap_team_pi["CAPACITY_TEAM_FTE"] * wf_baseline_cap_team_pi["RATE_PER_FTE_PI"]
        wf_baseline_cap_team_pi["WF_BASELINE_DELIVERY_PI"] = wf_baseline_cap_team_pi["CAPACITY_DELIVERY_FTE"] * wf_baseline_cap_team_pi["RATE_PER_FTE_PI"]
        wf_baseline_cap_team_pi["WF_BASELINE_TOTAL_PI"] = wf_baseline_cap_team_pi["WF_BASELINE_TEAM_PI"] + wf_baseline_cap_team_pi["WF_BASELINE_DELIVERY_PI"]

        # Default total WF baseline (when not app-group filtered): sum team/PI capacity baseline directly.
        baseline_people_capacity_total_unattributed = float(wf_baseline_cap_team_pi["WF_BASELINE_TOTAL_PI"].sum() or 0.0)

        # App-group attribution (used for drilldown + group-filtered baselines).
        if eff_by_group_weights is not None and not eff_by_group_weights.empty:
            w = eff_by_group_weights.copy()
            w["YEAR"] = pd.to_numeric(w.get("YEAR"), errors="coerce").astype("Int64")
            w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce").astype("Int64")
            w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").astype(str).str.strip()
            w["TEAMNAME"] = w.get("TEAMNAME", "").astype(str).str.strip()
            w["GROUPNAME"] = w.get("GROUPNAME", "").astype(str).str.strip()
            w["DERIVED_FTE_EXPLORER"] = pd.to_numeric(w.get("DERIVED_FTE_EXPLORER"), errors="coerce").fillna(0.0)
            tcols = ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"]
            totals = w.groupby(tcols, dropna=False)["DERIVED_FTE_EXPLORER"].sum().reset_index().rename(columns={"DERIVED_FTE_EXPLORER": "TOTAL_FTE"})
            w = w.merge(totals, on=tcols, how="left")
            w["TOTAL_FTE"] = pd.to_numeric(w.get("TOTAL_FTE"), errors="coerce").fillna(0.0)
            w["WEIGHT"] = w.apply(
                lambda r: (float(r["DERIVED_FTE_EXPLORER"]) / float(r["TOTAL_FTE"])) if float(r["TOTAL_FTE"]) > 0 else 0.0,
                axis=1,
            )
            base_src = wf_baseline_cap_team_pi.copy()
            base_src["PROGRAMNAME"] = base_src.get("PROGRAMNAME", "").astype(str).str.strip()
            base_src["TEAMNAME"] = base_src.get("TEAMNAME", "").astype(str).str.strip()
            base_src["YEAR"] = pd.to_numeric(base_src.get("YEAR"), errors="coerce").astype("Int64")
            base_src["PI"] = pd.to_numeric(base_src.get("PI"), errors="coerce").astype("Int64")
            w = w.merge(
                base_src[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "WF_BASELINE_TEAM_PI", "WF_BASELINE_DELIVERY_PI", "WF_BASELINE_TOTAL_PI"]],
                on=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"],
                how="left",
            )
            for c in ["WF_BASELINE_TEAM_PI", "WF_BASELINE_DELIVERY_PI", "WF_BASELINE_TOTAL_PI"]:
                w[c] = pd.to_numeric(w.get(c), errors="coerce").fillna(0.0)
            w["WF_BASELINE_TEAM_GROUP"] = w["WF_BASELINE_TEAM_PI"] * w["WEIGHT"]
            w["WF_BASELINE_DELIVERY_GROUP"] = w["WF_BASELINE_DELIVERY_PI"] * w["WEIGHT"]
            w["WF_BASELINE_GROUP"] = w["WF_BASELINE_TEAM_GROUP"] + w["WF_BASELINE_DELIVERY_GROUP"]

            # Build per-subcomponent baseline allocation used by the detailed breakdown.
            wf_baseline_cap_by_group_sub = pd.concat(
                [
                    w[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"]].assign(
                        _SUB_UP="TEAM", PLANNED_BASELINE_WF=w["WF_BASELINE_TEAM_GROUP"]
                    ),
                    w[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"]].assign(
                        _SUB_UP="DELIVERY TEAM", PLANNED_BASELINE_WF=w["WF_BASELINE_DELIVERY_GROUP"]
                    ),
                ],
                ignore_index=True,
            )

            if group_sel:
                gsel = str(group_sel).strip()
                w_scope = w.loc[w["GROUPNAME"].astype(str).str.strip().eq(gsel)].copy()
                baseline_people_capacity_total = float(pd.to_numeric(w_scope.get("WF_BASELINE_GROUP"), errors="coerce").fillna(0.0).sum() or 0.0)
                wf_baseline_people_by_pi = (
                    w_scope.groupby(["YEAR", "PI"], dropna=False)["WF_BASELINE_GROUP"]
                    .sum()
                    .reset_index()
                    .rename(columns={"WF_BASELINE_GROUP": "WF_BASELINE_CAPACITY"})
                )
                wf_baseline_people_by_prog = (
                    w_scope.groupby(["PROGRAMNAME"], dropna=False)["WF_BASELINE_GROUP"]
                    .sum()
                    .reset_index()
                    .rename(columns={"WF_BASELINE_GROUP": "WF_BASELINE_CAPACITY"})
                )
            else:
                baseline_people_capacity_total = baseline_people_capacity_total_unattributed
                wf_baseline_people_by_pi = (
                    wf_baseline_cap_team_pi.groupby(["YEAR", "PI"], dropna=False)["WF_BASELINE_TOTAL_PI"]
                    .sum()
                    .reset_index()
                    .rename(columns={"WF_BASELINE_TOTAL_PI": "WF_BASELINE_CAPACITY"})
                )
                wf_baseline_people_by_prog = (
                    wf_baseline_cap_team_pi.groupby(["PROGRAMNAME"], dropna=False)["WF_BASELINE_TOTAL_PI"]
                    .sum()
                    .reset_index()
                    .rename(columns={"WF_BASELINE_TOTAL_PI": "WF_BASELINE_CAPACITY"})
                )
        else:
            baseline_people_capacity_total = baseline_people_capacity_total_unattributed
            wf_baseline_people_by_pi = (
                wf_baseline_cap_team_pi.groupby(["YEAR", "PI"], dropna=False)["WF_BASELINE_TOTAL_PI"]
                .sum()
                .reset_index()
                .rename(columns={"WF_BASELINE_TOTAL_PI": "WF_BASELINE_CAPACITY"})
            )
            wf_baseline_people_by_prog = (
                wf_baseline_cap_team_pi.groupby(["PROGRAMNAME"], dropna=False)["WF_BASELINE_TOTAL_PI"]
                .sum()
                .reset_index()
                .rename(columns={"WF_BASELINE_TOTAL_PI": "WF_BASELINE_CAPACITY"})
            )
except Exception:
    baseline_people_capacity_total = 0.0

# Planned baseline totals used throughout the page.
baseline_wf_sod_only = float(baseline_people_capacity_total + baseline_wf_other)
baseline_wf_incl_overhead = float(baseline_wf_sod_only + overhead_people_primary)
# Add Contractor C (WF) and Contractor CS (NWF) using the same contractor baseline source as Budget Explorer.
baseline_people_total = float(baseline_wf_incl_overhead + contractor_c_wf_total)
baseline_total = float(baseline_people_total + baseline_other_total + contractor_cs_nwf_total)

# -------------------------------------------------------------------
# Financial baseline (Budget Explorer)
# -------------------------------------------------------------------
# The Budget Explorer "Baseline" section is the source of truth for planned baseline dollars.
# Build the same baseline rows once and use them for General-tab KPIs/tables so totals reconcile.
baseline_long = pd.DataFrame()
baseline_err: Optional[str] = None
try:
    parts: List[pd.DataFrame] = []

    # Optional enrichment: invoice baseline at application/vendor level.
    @cache_data_portfolio(ttl=300, show_spinner=False)
    def _invoice_baseline_detail(
        *,
        years: Tuple[int, ...],
        programs: Tuple[str, ...],
        teams: Tuple[str, ...],
        groups: Tuple[str, ...],
        pi_nums: Tuple[int, ...],
    ) -> pd.DataFrame:
        if not years:
            return pd.DataFrame()
        where = ["COALESCE(i.FISCAL_YEAR, YEAR(i.RENEWALDATE)) IN (" + ", ".join(["%s"] * len(years)) + ")"]
        params: List[Any] = [int(y) for y in years]
        if programs:
            where.append("UPPER(p.PROGRAMNAME) IN (" + ", ".join(["%s"] * len(programs)) + ")")
            params.extend([str(x).upper() for x in programs])
        if teams:
            where.append("UPPER(t.TEAMNAME) IN (" + ", ".join(["%s"] * len(teams)) + ")")
            params.extend([str(x).upper() for x in teams])
        if groups:
            # Match Budget's Application filter semantics: keep the selected group plus any unassigned rows.
            where.append(
                "(UPPER(ag.GROUPNAME) IN (" + ", ".join(["%s"] * len(groups)) + ") OR ag.GROUPNAME IS NULL OR LTRIM(RTRIM(ag.GROUPNAME)) = '')"
            )
            params.extend([str(x).upper() for x in groups])
        if pi_nums:
            where.append(
                """
                CASE
                  WHEN MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(COALESCE(i.FISCAL_YEAR, YEAR(i.RENEWALDATE)), 1, 1))) BETWEEN 1 AND 3 THEN 1
                  WHEN MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(COALESCE(i.FISCAL_YEAR, YEAR(i.RENEWALDATE)), 1, 1))) BETWEEN 4 AND 6 THEN 2
                  WHEN MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(COALESCE(i.FISCAL_YEAR, YEAR(i.RENEWALDATE)), 1, 1))) BETWEEN 7 AND 9 THEN 3
                  ELSE 4
                END IN ("""
                + ", ".join(["%s"] * len(pi_nums))
                + ")"
            )
            params.extend([int(p) for p in pi_nums])

        # IMPORTANT: Budget Explorer should show one row per invoice/forecast row (no aggregation),
        # so RESOURCE and PROVIDER expose invoice-level details.
        sql_view = f"""
          SELECT
            COALESCE(i.FISCAL_YEAR, YEAR(i.RENEWALDATE)) AS YEAR,
            CASE
              WHEN MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(COALESCE(i.FISCAL_YEAR, YEAR(i.RENEWALDATE)), 1, 1))) BETWEEN 1 AND 3 THEN 1
              WHEN MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(COALESCE(i.FISCAL_YEAR, YEAR(i.RENEWALDATE)), 1, 1))) BETWEEN 4 AND 6 THEN 2
              WHEN MONTH(COALESCE(i.RENEWALDATE, DATEFROMPARTS(COALESCE(i.FISCAL_YEAR, YEAR(i.RENEWALDATE)), 1, 1))) BETWEEN 7 AND 9 THEN 3
              ELSE 4
            END AS PI,
            p.PROGRAMNAME,
            t.TEAMNAME,
            COALESCE(i.INVOICEID, '') AS INVOICEID,
            COALESCE(i.AGREEMENT_NUMBER, '') AS AGREEMENT_NUMBER,
            COALESCE(i.WORK_ORDER, '') AS WORK_ORDER,
            COALESCE(i.INVOICE_TYPE, 'Recurring Invoice') AS INVOICE_TYPE,
            a.APPLICATIONNAME AS APPLICATIONNAME,
            v.VENDORNAME AS VENDORNAME,
            CAST(ISNULL(i.AMOUNT, 0) AS DECIMAL(18,2)) AS BASELINE_COST
          FROM VW_INVOICES_ACTUAL_AND_FORECAST i
          LEFT JOIN TEAMS t ON t.TEAMID = i.TEAMID
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = ISNULL(i.PROGRAMID_AT_BOOKING, t.PROGRAMID)
          LEFT JOIN APPLICATIONS a ON a.APPLICATIONID = i.APPLICATIONID
          LEFT JOIN APPLICATION_GROUPS ag ON ag.GROUPID = a.GROUPID
          LEFT JOIN VENDORS v
            ON v.VENDORID = COALESCE(NULLIF(i.VENDORID_AT_BOOKING, ''), a.VENDORID, ag.DEFAULT_VENDORID)
          WHERE {" AND ".join(where)}
        """
        try:
            df = fetch_df(sql_view, tuple(params) if params else None)
            df = df if df is not None else pd.DataFrame()
            if df.empty:
                return df
            out = df.copy()
            out["APPLICATIONNAME"] = out.get("APPLICATIONNAME", "").fillna("").astype(str).str.strip()
            out["VENDORNAME"] = out.get("VENDORNAME", "").fillna("").astype(str).str.strip()
            out["INVOICEID"] = out.get("INVOICEID", "").fillna("").astype(str).str.strip()
            out["AGREEMENT_NUMBER"] = out.get("AGREEMENT_NUMBER", "").fillna("").astype(str).str.strip()
            out["WORK_ORDER"] = out.get("WORK_ORDER", "").fillna("").astype(str).str.strip()
            out["INVOICE_TYPE"] = out.get("INVOICE_TYPE", "").fillna("").astype(str).str.strip()

            def _invoice_resource_label(r: pd.Series) -> str:
                app = str(r.get("APPLICATIONNAME") or "").strip() or "(Unassigned app)"
                inv_type = str(r.get("INVOICE_TYPE") or "").strip()
                agr = str(r.get("AGREEMENT_NUMBER") or "").strip()
                wo = str(r.get("WORK_ORDER") or "").strip()

                token = agr or wo
                if token:
                    label = f"{app} • {token}"
                else:
                    label = app
                if inv_type:
                    label = f"{label} • {inv_type}"
                return label

            out["RESOURCE"] = out.apply(_invoice_resource_label, axis=1)
            out["PROVIDER"] = out["VENDORNAME"].replace({"": "(Unassigned vendor)"})
            out["SUBCOMPONENT"] = out["INVOICE_TYPE"].replace({"": "Invoice"})

            keep_cols = ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "RESOURCE", "PROVIDER", "SUBCOMPONENT", "BASELINE_COST"]
            return out[keep_cols]
        except Exception:
            return pd.DataFrame()

    # Contractor baseline: headcount by contractor company × effective contractor rate.
    @cache_data_portfolio(ttl=300, show_spinner=False)
    def _contractor_baseline_detail(
        *,
        years: Tuple[int, ...],
        programs: Tuple[str, ...],
        teams: Tuple[str, ...],
        pi_nums: Tuple[int, ...],
    ) -> pd.DataFrame:
        if not years:
            return pd.DataFrame()
        where = ["h.YEAR IN (" + ", ".join(["%s"] * len(years)) + ")"]
        params: List[Any] = [int(y) for y in years]
        if programs:
            where.append("UPPER(p.PROGRAMNAME) IN (" + ", ".join(["%s"] * len(programs)) + ")")
            params.extend([str(x).upper() for x in programs])
        if teams:
            where.append("UPPER(t.TEAMNAME) IN (" + ", ".join(["%s"] * len(teams)) + ")")
            params.extend([str(x).upper() for x in teams])
        if pi_nums:
            where.append("h.PI IN (" + ", ".join(["%s"] * len(pi_nums)) + ")")
            params.extend([int(p) for p in pi_nums])

        sql_view = f"""
          SELECT
            h.YEAR,
            h.PI,
            p.PROGRAMNAME,
            t.TEAMNAME,
            h.CLASS,
            COALESCE(cc.NAME, h.COMPANYID) AS PROVIDER,
            SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS HEADCOUNT,
            COALESCE(TRY_CONVERT(FLOAT, cr.RATE), 0) AS RATE
          FROM VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE h
          LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
          LEFT JOIN CONTRACTOR_COMPANY cc ON cc.COMPANYID = h.COMPANYID
          LEFT JOIN VW_CONTRACTOR_RATE_EFFECTIVE cr
            ON cr.COMPANYID = h.COMPANYID AND cr.YEAR = h.YEAR AND cr.PI = h.PI AND cr.CLASS = h.CLASS
          WHERE {" AND ".join(where)}
          GROUP BY h.YEAR, h.PI, p.PROGRAMNAME, t.TEAMNAME, h.CLASS, COALESCE(cc.NAME, h.COMPANYID), COALESCE(TRY_CONVERT(FLOAT, cr.RATE), 0)
        """
        try:
            df = fetch_df(sql_view, tuple(params) if params else None)
            if df is not None and not df.empty:
                return df
        except Exception:
            pass

        sql_fallback = sql_view.replace("VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE", "TEAM_CONTRACTOR_HEADCOUNT").replace(
            "COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS HEADCOUNT",
            "SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS HEADCOUNT",
        )
        try:
            df = fetch_df(sql_fallback, tuple(params) if params else None)
            return df if df is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    # Build Budget Explorer baseline rows from the canonical BASELINE frame.
    # Fallback to work_scope only if baseline_scope is unexpectedly empty.
    baseline_source = baseline_scope if baseline_scope is not None and not baseline_scope.empty else work_scope
    if baseline_source is not None and not baseline_source.empty:
        base = baseline_source.copy()
        base["YEAR"] = pd.to_numeric(base.get("YEAR"), errors="coerce").astype("Int64")
        base["PI"] = pd.to_numeric(base.get("PI"), errors="coerce").astype("Int64")
        base["PROGRAMNAME"] = base.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        base["TEAMNAME"] = base.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        base["SOURCE"] = base.get("SOURCE", "").fillna("").astype(str).str.strip()
        base["SUBCOMPONENT"] = base.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
        base["AMOUNT"] = pd.to_numeric(base.get("AMOUNT"), errors="coerce").fillna(0.0)
        base["CAT"] = base.get("CAT", pd.Series(dtype=str)).astype(str)
        src_up = base["SOURCE"].astype(str).str.upper()
        is_ado = src_up.eq("ADO")

        years_t = tuple(int(y) for y in years_sel) if years_sel else (int(outlook_year),)
        programs_t = tuple(str(x) for x in effective_programs) if effective_programs else ()
        teams_t = tuple(str(x) for x in effective_teams) if effective_teams else ()
        groups_t = tuple(str(x) for x in effective_groups) if effective_groups else ()
        pi_t = tuple(int(p) for p in pi_nums) if pi_nums else ()
        invoice_detail = _invoice_baseline_detail(years=years_t, programs=programs_t, teams=teams_t, groups=groups_t, pi_nums=pi_t)
        if invoice_detail is not None and not invoice_detail.empty:
            inv_mask = src_up.str.contains("INVOICE") | base["SUBCOMPONENT"].astype(str).str.upper().str.contains("INVOICE")
            base = base.loc[~inv_mask].copy()

            inv = invoice_detail.copy()
            inv["YEAR"] = pd.to_numeric(inv.get("YEAR"), errors="coerce").astype("Int64")
            inv["PI"] = pd.to_numeric(inv.get("PI"), errors="coerce").astype("Int64")
            inv["PROGRAMNAME"] = inv.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
            inv["TEAMNAME"] = inv.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
            inv["RESOURCE"] = inv.get("RESOURCE", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned app)"})
            inv["PROVIDER"] = inv.get("PROVIDER", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned vendor)"})
            inv["SUBCOMPONENT"] = inv.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip().replace({"": "Invoice"})
            inv["BASELINE_COST"] = pd.to_numeric(inv.get("BASELINE_COST"), errors="coerce").fillna(0.0)
            inv = inv[inv["BASELINE_COST"] != 0].copy()
            if not inv.empty:
                inv["SOURCE"] = "INVOICE"
                inv["COST_TYPE"] = "NWF"
                inv["HEADCOUNT"] = 0.0
                parts.append(inv[["YEAR","PI","PROGRAMNAME","TEAMNAME","SOURCE","SUBCOMPONENT","COST_TYPE","RESOURCE","PROVIDER","BASELINE_COST","HEADCOUNT"]])

        # Baseline NWF = non-ADO NWF rows (contracts/program additional costs etc).
        nwf = base.loc[base["CAT"].eq("NON_WORK_FORCE") & ~is_ado].copy()
        if not nwf.empty:
            nwf["COST_TYPE"] = "NWF"
            nwf["RESOURCE"] = nwf["SUBCOMPONENT"].astype(str).replace({"": "—"})
            nwf["PROVIDER"] = nwf["SUBCOMPONENT"].astype(str).replace({"": "(Unassigned)"})
            sub_up = nwf["SUBCOMPONENT"].astype(str).str.upper().str.strip()
            is_cloud_travel = sub_up.isin(["CLOUD AWS", "CLOUD AZURE", "TRAVEL"])
            nwf.loc[is_cloud_travel, "TEAMNAME"] = "—"
            nwf.loc[is_cloud_travel, "PROVIDER"] = "ExxonMobil"
            nwf["BASELINE_COST"] = nwf["AMOUNT"]
            nwf["HEADCOUNT"] = 0.0
            parts.append(nwf[["YEAR","PI","PROGRAMNAME","TEAMNAME","SOURCE","SUBCOMPONENT","COST_TYPE","RESOURCE","PROVIDER","BASELINE_COST","HEADCOUNT"]])

        # Non-ADO WF (rare): treat as baseline.
        wf_other = base.loc[base["CAT"].eq("WORK_FORCE") & ~is_ado].copy()
        if not wf_other.empty:
            wf_other["COST_TYPE"] = "WF"
            wf_other["RESOURCE"] = wf_other["SUBCOMPONENT"].astype(str).replace({"": "—"})
            wf_other["PROVIDER"] = "ExxonMobil"
            wf_other["BASELINE_COST"] = wf_other["AMOUNT"]
            wf_other["HEADCOUNT"] = 0.0
            parts.append(wf_other[["YEAR","PI","PROGRAMNAME","TEAMNAME","SOURCE","SUBCOMPONENT","COST_TYPE","RESOURCE","PROVIDER","BASELINE_COST","HEADCOUNT"]])

    # WF baseline detail (Team Overhead + Delivery) from Teams/Programs headcount + effective rates.
    #
    # This keeps Budget Explorer aligned with:
    # - Headcount: `VW_TEAM_HEADCOUNT_EFFECTIVE` (Teams page source-of-truth; PI=0 expands to PI 1..4)
    # - Rates: `VW_TEAM_RATE_EFFECTIVE` / `VW_PROGRAM_RATE_EFFECTIVE` (Rates page source-of-truth)
    try:
        years_td = tuple(int(y) for y in years_sel) if years_sel else (int(outlook_year),)
        pi_td = tuple(int(p) for p in (pi_nums or [1, 2, 3, 4]) if 1 <= int(p) <= 4)
        where_td: List[str] = ["h.CLASS IN ('TEAM','DELIVERY')"]
        params_td: List[Any] = []
        if years_td:
            where_td.append("h.YEAR IN (" + ", ".join(["%s"] * len(years_td)) + ")")
            params_td.extend([int(y) for y in years_td])
        if pi_td:
            where_td.append("h.PI IN (" + ", ".join(["%s"] * len(pi_td)) + ")")
            params_td.extend([int(p) for p in pi_td])
        if effective_teams:
            where_td.append("UPPER(t.TEAMNAME) IN (" + ", ".join(["%s"] * len(effective_teams)) + ")")
            params_td.extend([str(t).upper() for t in effective_teams])
        if effective_programs:
            where_td.append("UPPER(p.PROGRAMNAME) IN (" + ", ".join(["%s"] * len(effective_programs)) + ")")
            params_td.extend([str(pn).upper() for pn in effective_programs])

        sql_td = f"""
          SELECT
            TRY_CONVERT(INT, h.YEAR) AS YEAR,
            TRY_CONVERT(INT, h.PI) AS PI,
            t.TEAMID,
            t.TEAMNAME,
            p.PROGRAMID,
            p.PROGRAMNAME,
            UPPER(h.CLASS) AS CLASS,
            SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS HEADCOUNT
          FROM VW_TEAM_HEADCOUNT_EFFECTIVE h
          LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
          WHERE {" AND ".join(where_td)}
          GROUP BY
            TRY_CONVERT(INT, h.YEAR),
            TRY_CONVERT(INT, h.PI),
            t.TEAMID,
            t.TEAMNAME,
            p.PROGRAMID,
            p.PROGRAMNAME,
            UPPER(h.CLASS)
        """
        # Prefer the effective view (if present); fall back to canonical headcount history if the view is missing/empty.
        try:
            df_td = fetch_df(sql_td, tuple(params_td) if params_td else None)
        except Exception:
            df_td = None

        td = pd.DataFrame()
        if df_td is not None and not df_td.empty:
            td = df_td.copy()
            td["YEAR"] = pd.to_numeric(td.get("YEAR"), errors="coerce").astype("Int64")
            td["PI"] = pd.to_numeric(td.get("PI"), errors="coerce").astype("Int64")
            td["TEAMID"] = td.get("TEAMID", "").fillna("").astype(str).str.strip()
            td["TEAMNAME"] = td.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
            td["PROGRAMID"] = td.get("PROGRAMID", "").fillna("").astype(str).str.strip()
            td["PROGRAMNAME"] = td.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
            td["CLASS"] = td.get("CLASS", "").fillna("").astype(str).str.strip().str.upper()
            td["HEADCOUNT"] = pd.to_numeric(td.get("HEADCOUNT"), errors="coerce").fillna(0.0)
            td = td[(td["YEAR"].notna()) & (td["PI"].notna()) & (td["HEADCOUNT"] > 0)].copy()

        if td is None or td.empty:
            # Fallback: compute PI-effective TEAM/DELIVERY headcount from canonical tables:
            # - PI=0 is the year default
            # - PI 1..4 override PI=0 when present
            where_h: List[str] = ["UPPER(h.CLASS) IN ('TEAM','DELIVERY')", "ISNULL(h.PI, 0) IN (0,1,2,3,4)"]
            params_h: List[Any] = []
            if years_td:
                where_h.append("h.YEAR IN (" + ", ".join(["%s"] * len(years_td)) + ")")
                params_h.extend([int(y) for y in years_td])
            if effective_teams:
                where_h.append("UPPER(t.TEAMNAME) IN (" + ", ".join(["%s"] * len(effective_teams)) + ")")
                params_h.extend([str(t).upper() for t in effective_teams])
            if effective_programs:
                where_h.append("UPPER(p.PROGRAMNAME) IN (" + ", ".join(["%s"] * len(effective_programs)) + ")")
                params_h.extend([str(pn).upper() for pn in effective_programs])

            sql_hist = f"""
              SELECT
                TRY_CONVERT(INT, h.YEAR) AS YEAR,
                TRY_CONVERT(INT, ISNULL(h.PI, 0)) AS PI,
                t.TEAMID,
                t.TEAMNAME,
                p.PROGRAMID,
                p.PROGRAMNAME,
                UPPER(h.CLASS) AS CLASS,
                UPPER(LTRIM(RTRIM(h.LOCATION))) AS LOCATION,
                SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS HEADCOUNT
              FROM TEAM_HEADCOUNT_HISTORY h
              LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
              LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
              WHERE {" AND ".join(where_h)}
              GROUP BY
                TRY_CONVERT(INT, h.YEAR),
                TRY_CONVERT(INT, ISNULL(h.PI, 0)),
                t.TEAMID,
                t.TEAMNAME,
                p.PROGRAMID,
                p.PROGRAMNAME,
                UPPER(h.CLASS),
                UPPER(LTRIM(RTRIM(h.LOCATION)))
            """
            df_hist = fetch_df(sql_hist, tuple(params_h) if params_h else None)
            if df_hist is not None and not df_hist.empty:
                raw = df_hist.copy()
                raw["YEAR"] = pd.to_numeric(raw.get("YEAR"), errors="coerce").astype("Int64")
                raw["PI"] = pd.to_numeric(raw.get("PI"), errors="coerce").astype("Int64")
                raw["TEAMID"] = raw.get("TEAMID", "").fillna("").astype(str).str.strip()
                raw["TEAMNAME"] = raw.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
                raw["PROGRAMID"] = raw.get("PROGRAMID", "").fillna("").astype(str).str.strip()
                raw["PROGRAMNAME"] = raw.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
                raw["CLASS"] = raw.get("CLASS", "").fillna("").astype(str).str.strip().str.upper()
                raw["LOCATION"] = raw.get("LOCATION", "").fillna("").astype(str).str.strip()
                raw["HEADCOUNT"] = pd.to_numeric(raw.get("HEADCOUNT"), errors="coerce").fillna(0.0)
                raw = raw[(raw["YEAR"].notna()) & (raw["PI"].notna()) & raw["CLASS"].isin(["TEAM", "DELIVERY"])].copy()

                base0 = (
                    raw.loc[raw["PI"] == 0]
                    .groupby(["YEAR", "TEAMID", "TEAMNAME", "PROGRAMID", "PROGRAMNAME", "CLASS", "LOCATION"], dropna=False)["HEADCOUNT"]
                    .sum()
                    .reset_index()
                    .rename(columns={"HEADCOUNT": "HC0"})
                )
                ov = (
                    raw.loc[raw["PI"].isin(list(pi_td))]
                    .groupby(["YEAR", "PI", "TEAMID", "TEAMNAME", "PROGRAMID", "PROGRAMNAME", "CLASS", "LOCATION"], dropna=False)["HEADCOUNT"]
                    .sum()
                    .reset_index()
                    .rename(columns={"HEADCOUNT": "HC_OV"})
                )

                key_cols = ["YEAR", "TEAMID", "TEAMNAME", "PROGRAMID", "PROGRAMNAME", "CLASS", "LOCATION"]
                keys = pd.concat([base0[key_cols], ov[key_cols].drop(columns=["PI"], errors="ignore")], ignore_index=True).drop_duplicates()
                if not keys.empty:
                    rows_eff: List[pd.DataFrame] = []
                    for pi in list(pi_td):
                        cur = keys.copy()
                        cur["PI"] = int(pi)
                        cur = cur.merge(base0, on=key_cols, how="left")
                        cur = cur.merge(ov.loc[ov["PI"] == int(pi)], on=key_cols + ["PI"], how="left")
                        cur["HC_EFF"] = pd.to_numeric(cur.get("HC_OV"), errors="coerce")
                        cur["HC_EFF"] = cur["HC_EFF"].fillna(pd.to_numeric(cur.get("HC0"), errors="coerce"))
                        cur["HC_EFF"] = cur["HC_EFF"].fillna(0.0)
                        rows_eff.append(cur[key_cols + ["PI", "HC_EFF"]])
                    eff = pd.concat(rows_eff, ignore_index=True) if rows_eff else pd.DataFrame(columns=key_cols + ["PI", "HC_EFF"])
                    # Sum across locations to align to Budget Explorer's Team/Delivery row grain (team/year/PI).
                    td = (
                        eff.groupby(["YEAR", "PI", "PROGRAMID", "PROGRAMNAME", "TEAMID", "TEAMNAME", "CLASS"], dropna=False)["HC_EFF"]
                        .sum()
                        .reset_index()
                        .rename(columns={"HC_EFF": "HEADCOUNT"})
                    )

        if td is not None and not td.empty:
            pivot = (
                td.pivot_table(
                    index=["YEAR", "PI", "PROGRAMID", "PROGRAMNAME", "TEAMID", "TEAMNAME"],
                    columns="CLASS",
                    values="HEADCOUNT",
                    aggfunc="sum",
                    fill_value=0.0,
                )
                .reset_index()
            )
            pivot.columns = [str(c) for c in pivot.columns]
            pivot["HC_TEAM"] = pd.to_numeric(pivot.get("TEAM"), errors="coerce").fillna(0.0)
            pivot["HC_DELIVERY"] = pd.to_numeric(pivot.get("DELIVERY"), errors="coerce").fillna(0.0)

            # Use weighted/effective per-PI rates (includes global fallback and multi-location weighting).
            # This avoids Team/Delivery rows disappearing when location-specific rate joins fail.
            team_ids_td = [str(x) for x in pivot.get("TEAMID", "").dropna().astype(str).unique().tolist() if str(x).strip()]
            years_wr = [int(y) for y in years_td if y is not None]
            pi_wr = [int(p) for p in pi_td if p is not None]
            wr = pd.DataFrame()
            if team_ids_td and years_wr and pi_wr:
                sql_wr = f"""
                  SELECT
                    TEAMID,
                    TRY_CONVERT(INT, YEAR) AS YEAR,
                    TRY_CONVERT(INT, PI) AS PI,
                    TRY_CONVERT(FLOAT, TEAM_RATE) AS TEAM_RATE,
                    TRY_CONVERT(FLOAT, DELIVERY_RATE) AS DELIVERY_RATE
                  FROM VW_TEAM_WEIGHTED_RATES
                  WHERE TEAMID IN ({", ".join(["%s"] * len(team_ids_td))})
                    AND YEAR IN ({", ".join(["%s"] * len(years_wr))})
                    AND PI IN ({", ".join(["%s"] * len(pi_wr))})
                """
                try:
                    wr = fetch_df(sql_wr, tuple(team_ids_td + years_wr + pi_wr))
                except Exception:
                    wr = pd.DataFrame()
            if wr is None:
                wr = pd.DataFrame()
            if not wr.empty:
                wr = wr.copy()
                wr["TEAMID"] = wr.get("TEAMID", "").fillna("").astype(str).str.strip()
                wr["YEAR"] = pd.to_numeric(wr.get("YEAR"), errors="coerce").astype("Int64")
                wr["PI"] = pd.to_numeric(wr.get("PI"), errors="coerce").astype("Int64")
                wr["TEAM_RATE"] = pd.to_numeric(wr.get("TEAM_RATE"), errors="coerce").fillna(0.0)
                wr["DELIVERY_RATE"] = pd.to_numeric(wr.get("DELIVERY_RATE"), errors="coerce").fillna(0.0)

            base_rates = pivot.merge(wr, on=["TEAMID", "YEAR", "PI"], how="left")
            base_rates["TEAM_RATE"] = pd.to_numeric(base_rates.get("TEAM_RATE"), errors="coerce").fillna(0.0)
            base_rates["DELIVERY_RATE"] = pd.to_numeric(base_rates.get("DELIVERY_RATE"), errors="coerce").fillna(0.0)

            wf_rows: List[pd.DataFrame] = []
            team_rows = base_rates.loc[base_rates["HC_TEAM"] > 0].copy()
            if not team_rows.empty:
                team_rows["SOURCE"] = "ADO"
                team_rows["SUBCOMPONENT"] = "TEAM"
                team_rows["COST_TYPE"] = "WF"
                team_rows["RESOURCE"] = ""
                team_rows["PROVIDER"] = "ExxonMobil"
                team_rows["HEADCOUNT"] = pd.to_numeric(team_rows.get("HC_TEAM"), errors="coerce").fillna(0.0)
                team_rows["BASELINE_COST"] = team_rows["HEADCOUNT"] * team_rows["TEAM_RATE"]
                wf_rows.append(team_rows[["YEAR","PI","PROGRAMNAME","TEAMNAME","SOURCE","SUBCOMPONENT","COST_TYPE","RESOURCE","PROVIDER","BASELINE_COST","HEADCOUNT"]])

            del_rows = base_rates.loc[base_rates["HC_DELIVERY"] > 0].copy()
            if not del_rows.empty:
                del_rows["SOURCE"] = "ADO"
                del_rows["SUBCOMPONENT"] = "DELIVERY TEAM"
                del_rows["COST_TYPE"] = "WF"
                del_rows["RESOURCE"] = ""
                del_rows["PROVIDER"] = "ExxonMobil"
                del_rows["HEADCOUNT"] = pd.to_numeric(del_rows.get("HC_DELIVERY"), errors="coerce").fillna(0.0)
                del_rows["BASELINE_COST"] = del_rows["HEADCOUNT"] * del_rows["DELIVERY_RATE"]
                wf_rows.append(del_rows[["YEAR","PI","PROGRAMNAME","TEAMNAME","SOURCE","SUBCOMPONENT","COST_TYPE","RESOURCE","PROVIDER","BASELINE_COST","HEADCOUNT"]])

            if wf_rows:
                wf_td = pd.concat(wf_rows, ignore_index=True)
                wf_td["BASELINE_COST"] = pd.to_numeric(wf_td.get("BASELINE_COST"), errors="coerce").fillna(0.0)
                wf_td["HEADCOUNT"] = pd.to_numeric(wf_td.get("HEADCOUNT"), errors="coerce").fillna(0.0)
                # Keep headcount rows even if rates are missing (BASELINE_COST == 0),
                # so Budget Explorer makes it obvious which teams need rate configuration.
                wf_td = wf_td[wf_td["HEADCOUNT"] > 0].copy()
                if not wf_td.empty:
                    parts.append(wf_td)
    except Exception:
        pass

    # Contractors (line-by-line resources; provider = contractor company).
    years_c = tuple(int(y) for y in years_sel) if years_sel else (int(outlook_year),)
    programs_c = tuple(str(x) for x in effective_programs) if effective_programs else ()
    teams_c = tuple(str(x) for x in effective_teams) if effective_teams else ()
    pi_c = tuple(int(p) for p in pi_nums) if pi_nums else ()
    con = _contractor_baseline_detail(years=years_c, programs=programs_c, teams=teams_c, pi_nums=pi_c)
    if con is not None and not con.empty:
        con = con.copy()
        con["YEAR"] = pd.to_numeric(con.get("YEAR"), errors="coerce").astype("Int64")
        con["PI"] = pd.to_numeric(con.get("PI"), errors="coerce").astype("Int64")
        con["PROGRAMNAME"] = con.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        con["TEAMNAME"] = con.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        con["PROVIDER"] = con.get("PROVIDER", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        con["HEADCOUNT"] = pd.to_numeric(con.get("HEADCOUNT"), errors="coerce").fillna(0.0)
        con["RATE"] = pd.to_numeric(con.get("RATE"), errors="coerce").fillna(0.0)
        con["BASELINE_COST"] = con["HEADCOUNT"] * con["RATE"]
        con = con[(con["HEADCOUNT"] > 0) & (con["BASELINE_COST"] != 0)].copy()
        if not con.empty:
            con["SOURCE"] = "CONTRACTOR"
            con["CLASS"] = con.get("CLASS", "").fillna("").astype(str).str.strip().str.upper()
            con["SUBCOMPONENT"] = con["CLASS"].map({"CONTRACTOR_C": "Contractor C", "CONTRACTOR_CS": "Contractor CS"}).fillna(con["CLASS"])
            con["COST_TYPE"] = con["CLASS"].map({"CONTRACTOR_C": "WF", "CONTRACTOR_CS": "NWF"}).fillna("WF")
            con["RESOURCE"] = ""
            parts.append(con[["YEAR","PI","PROGRAMNAME","TEAMNAME","SOURCE","SUBCOMPONENT","COST_TYPE","RESOURCE","PROVIDER","BASELINE_COST","HEADCOUNT"]])

    # Program overhead baseline (program FTE × program rate) — editable on Programs page.
    if _overhead_inputs_all is not None and not _overhead_inputs_all.empty:
        ov = _overhead_inputs_all.copy()
        ov["YEAR"] = pd.to_numeric(ov.get("YEAR"), errors="coerce").astype("Int64")
        ov["PI"] = pd.to_numeric(ov.get("PI"), errors="coerce").fillna(0).astype(int)
        ov = ov[ov["PI"].isin([int(p) for p in pi_nums])].copy() if pi_nums else ov
        ov["PROGRAMNAME"] = ov.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        ov["PROGRAMFTE"] = pd.to_numeric(ov.get("PROGRAMFTE"), errors="coerce").fillna(0.0)
        ov["PROGRAM_XOM_RATE"] = pd.to_numeric(ov.get("PROGRAM_XOM_RATE"), errors="coerce").fillna(0.0)
        ov["BASELINE_COST"] = ov["PROGRAMFTE"] * ov["PROGRAM_XOM_RATE"]
        ov = ov[ov["BASELINE_COST"] != 0].copy()
        if not ov.empty:
            ov["TEAMNAME"] = "—"
            ov["SOURCE"] = "ADO"
            ov["SUBCOMPONENT"] = "Program"
            ov["COST_TYPE"] = "WF"
            # Keep overhead resources distinguishable when program overhead is split across locations.
            ov_loc = ov.get("LOCATION", "").fillna("").astype(str).str.strip()
            ov["RESOURCE"] = ov_loc.map(lambda loc: f"Overhead ({loc})" if loc else "Overhead")
            # Program overhead is internal workforce; provider should be the internal provider.
            ov["PROVIDER"] = "ExxonMobil"
            ov["HEADCOUNT"] = ov["PROGRAMFTE"]
            parts.append(ov[["YEAR","PI","PROGRAMNAME","TEAMNAME","SOURCE","SUBCOMPONENT","COST_TYPE","RESOURCE","PROVIDER","BASELINE_COST","HEADCOUNT"]])

    baseline_long = pd.concat(parts, ignore_index=True, sort=False) if parts else pd.DataFrame()

    # Guardrail: Budget Explorer must reconcile to canonical baseline scope (same source as Welcome KPIs).
    # Use canonical baseline rows as the primary explorer source; keep synthesized parts as fallback only.
    if baseline_scope is not None and not baseline_scope.empty:
        c = baseline_scope.copy()
        c["YEAR"] = pd.to_numeric(c.get("YEAR"), errors="coerce").astype("Int64")
        c["PI"] = pd.to_numeric(c.get("PI"), errors="coerce").astype("Int64")
        c["PROGRAMNAME"] = c.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        c["TEAMNAME"] = c.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        c["SOURCE"] = c.get("SOURCE", "").fillna("").astype(str).str.strip()
        c["SUBCOMPONENT"] = c.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
        c["CAT"] = c.get("CAT", "").fillna("").astype(str).str.strip()
        c["AMOUNT"] = pd.to_numeric(c.get("AMOUNT"), errors="coerce").fillna(0.0)
        c["FTE"] = pd.to_numeric(c.get("FTE"), errors="coerce").fillna(0.0)
        c["COST_TYPE"] = c["CAT"].map({"WORK_FORCE": "WF", "NON_WORK_FORCE": "NWF"}).fillna("NWF")
        c["RESOURCE"] = c["SUBCOMPONENT"].replace({"": "—"})
        c["PROVIDER"] = c["SOURCE"].replace({"": "(Unassigned)"})
        c.loc[c["COST_TYPE"].eq("WF"), "PROVIDER"] = "ExxonMobil"
        src_up_c = c["SOURCE"].astype(str).str.upper().str.strip()
        sub_up_c = c["SUBCOMPONENT"].astype(str).str.upper().str.strip()
        is_invoice_c = src_up_c.str.contains("INVOICE") | sub_up_c.str.contains("INVOICE")

        # Prefer invoice-level vendor names for invoice rows when available.
        try:
            inv_src = invoice_detail if "invoice_detail" in locals() else pd.DataFrame()
            if isinstance(inv_src, pd.DataFrame) and not inv_src.empty and bool(is_invoice_c.any()):
                invp = inv_src.copy()
                invp["YEAR"] = pd.to_numeric(invp.get("YEAR"), errors="coerce").astype("Int64")
                invp["PI"] = pd.to_numeric(invp.get("PI"), errors="coerce").astype("Int64")
                invp["PROGRAMNAME"] = invp.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
                invp["TEAMNAME"] = invp.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
                invp["PROVIDER"] = invp.get("PROVIDER", "").fillna("").astype(str).str.strip()
                invp = invp[invp["PROVIDER"].ne("")].copy()
                if not invp.empty:
                    prov_by_key = (
                        invp.groupby(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], dropna=False)["PROVIDER"]
                        .agg(lambda s: ", ".join(sorted(set([str(v).strip() for v in s if str(v).strip()]))))
                        .reset_index()
                        .rename(columns={"PROVIDER": "_INVOICE_PROVIDER"})
                    )
                    c = c.merge(prov_by_key, on=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], how="left")
                    c.loc[is_invoice_c, "PROVIDER"] = (
                        c.loc[is_invoice_c, "_INVOICE_PROVIDER"]
                        .fillna(c.loc[is_invoice_c, "PROVIDER"])
                        .replace({"INVOICE": "(Unassigned vendor)"})
                    )
                    c = c.drop(columns=["_INVOICE_PROVIDER"], errors="ignore")
                else:
                    c.loc[is_invoice_c & c["PROVIDER"].astype(str).str.upper().eq("INVOICE"), "PROVIDER"] = "(Unassigned vendor)"
            else:
                c.loc[is_invoice_c & c["PROVIDER"].astype(str).str.upper().eq("INVOICE"), "PROVIDER"] = "(Unassigned vendor)"
        except Exception:
            c.loc[is_invoice_c & c["PROVIDER"].astype(str).str.upper().eq("INVOICE"), "PROVIDER"] = "(Unassigned vendor)"

        is_prog_add = src_up_c.eq("PROGRAM_ADDITIONAL")
        # Program additional rows are program-level NWF and should not look team-attributed.
        c.loc[is_prog_add, "TEAMNAME"] = "—"
        # Provider normalization for common cloud subcomponents.
        c.loc[is_prog_add & sub_up_c.eq("CLOUD AWS"), "PROVIDER"] = "AWS"
        c.loc[is_prog_add & sub_up_c.eq("CLOUD AZURE"), "PROVIDER"] = "Azure"
        # Fallback provider for other program-additional rows.
        c.loc[is_prog_add & c["PROVIDER"].astype(str).str.strip().isin(["", "PROGRAM_ADDITIONAL"]), "PROVIDER"] = "ExxonMobil"
        c["BASELINE_COST"] = c["AMOUNT"]
        c["HEADCOUNT"] = c["FTE"]
        baseline_long = c[
            [
                "YEAR",
                "PI",
                "PROGRAMNAME",
                "TEAMNAME",
                "SOURCE",
                "SUBCOMPONENT",
                "COST_TYPE",
                "RESOURCE",
                "PROVIDER",
                "BASELINE_COST",
                "HEADCOUNT",
            ]
        ].copy()
except Exception as e:
    baseline_err = str(e)
    baseline_long = pd.DataFrame()

# Canonical Baseline totals for KPIs/charts; scenario cost lines come from `core.cost_model`.
# NOTE: We intentionally do NOT apply the Application filter at query time so we can preserve Budget's
# "(selected group + unassigned)" semantics via a post-filter below.
filters_baseline_canon: dict[str, Any] = {
    "year": [int(y) for y in years_sel] if years_sel else [int(outlook_year)],
    "pi": [int(p) for p in pi_nums] if pi_nums else [],
    "program": [str(x) for x in effective_programs] if effective_programs else [],
    "team": [str(x) for x in effective_teams] if effective_teams else [],
    # Baseline program overhead rates are location-scoped; keep a deterministic default until a UI control is added.
    "location": "GBC",
    "group_by": ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"],
}

df_baseline_canon = pd.DataFrame()
df_groups_canon = pd.DataFrame()
baseline_financial_source = "core.cost_model (Baseline)"
baseline_financial_err = ""
baseline_cost_lines = pd.DataFrame()
try:
    # Reuse the already-built scoped baseline frame (single source of truth for all Budget baseline visuals).
    baseline_cost_lines = baseline_scope.copy() if baseline_scope is not None else pd.DataFrame()
    df_baseline_canon = get_pi_costs(baseline_cost_lines, scenario=scenario, filters=filters_baseline_canon)
    df_baseline_canon = df_baseline_canon if df_baseline_canon is not None else pd.DataFrame()
    df_groups_canon = get_app_group_costs(baseline_cost_lines, scenario=scenario, filters=filters_baseline_canon)
    df_groups_canon = df_groups_canon if df_groups_canon is not None else pd.DataFrame()
except Exception as e:
    baseline_financial_err = str(e)
    baseline_cost_lines = pd.DataFrame()
    df_baseline_canon = pd.DataFrame()
    df_groups_canon = pd.DataFrame()

baseline_total_financial = 0.0
baseline_people_financial = 0.0
baseline_other_financial = 0.0
try:
    if baseline_cost_lines is not None and not baseline_cost_lines.empty:
        bl = baseline_cost_lines.copy()
        bl["AMOUNT"] = pd.to_numeric(bl.get("AMOUNT"), errors="coerce").fillna(0.0)
        baseline_total_financial = float(bl["AMOUNT"].sum() or 0.0)
        cat = bl.get("CAT", pd.Series("", index=bl.index, dtype="object")).astype(str)
        baseline_people_financial = float(bl.loc[cat.eq("WORK_FORCE"), "AMOUNT"].sum() or 0.0)
        baseline_other_financial = float(bl.loc[cat.eq("NON_WORK_FORCE"), "AMOUNT"].sum() or 0.0)
except Exception:
    baseline_total_financial = 0.0
    baseline_people_financial = 0.0
    baseline_other_financial = 0.0

variance_total_financial = float(expected_total - baseline_total_financial)
variance_pct_financial = (variance_total_financial / baseline_total_financial) if baseline_total_financial > 0 else None

# Expected spend (WF, SoD-only): exclude program overhead rows, do not apply the overhead FTE×rate correction here.
# This is used only for SoD baseline vs expected comparisons (debug/WF tab). High-level "WF expected" includes overhead.
expected_wf_sod_only = float(expected_people_from_view - overhead_people_cost_from_view)

variance_total = float(expected_total - baseline_total)
variance_pct = (variance_total / baseline_total) if baseline_total > 0 else None

# Revised plan totals (events disabled):
# Treat "Revised plan" as the working baseline for now to avoid confusing PM workflows.
revised_plan_total = float(baseline_total_financial or 0.0)
change_vs_baseline = 0.0

_budget_status_done("Budget data loaded. Rendering views...")
with tabs_by_name["Overview"]:
    st.subheader("Overview")

    debug_budget = bool(show_debug)

    # Program-level variance table (used for variance drivers + drilldown).
    by_prog_view = work_scope.copy()
    by_prog_view["PROGRAMNAME"] = by_prog_view.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    by_prog_view["AMOUNT"] = pd.to_numeric(by_prog_view.get("AMOUNT"), errors="coerce").fillna(0.0)
    src_up_prog = by_prog_view.get("SOURCE", "").astype(str).str.upper()
    sub_up_prog = by_prog_view.get("SUBCOMPONENT", "").astype(str).str.upper()
    is_ado_prog = src_up_prog.eq("ADO")
    is_wf_prog = by_prog_view["CAT"].eq("WORK_FORCE")

    # Planned baseline by program (canonical Baseline scenario cost lines).
    base_by_prog = pd.DataFrame(columns=["PROGRAMNAME", "PLANNED_BASELINE"])
    try:
        if baseline_cost_lines is not None and not baseline_cost_lines.empty:
            b = baseline_cost_lines.copy()
            b["PROGRAMNAME"] = b.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
            b["AMOUNT"] = pd.to_numeric(b.get("AMOUNT"), errors="coerce").fillna(0.0)
            base_by_prog = (
                b.groupby("PROGRAMNAME", dropna=False)["AMOUNT"]
                .sum()
                .reset_index()
                .rename(columns={"AMOUNT": "PLANNED_BASELINE"})
            )
    except Exception:
        base_by_prog = pd.DataFrame(columns=["PROGRAMNAME", "PLANNED_BASELINE"])

    exp_by_prog = (
        by_prog_view.groupby("PROGRAMNAME", dropna=False)["AMOUNT"]
        .sum()
        .reset_index()
        .rename(columns={"AMOUNT": "EXPECTED_SPEND"})
    )

    risk = exp_by_prog.merge(base_by_prog, on="PROGRAMNAME", how="outer")
    for c in ["EXPECTED_SPEND", "PLANNED_BASELINE"]:
        risk[c] = pd.to_numeric(risk.get(c), errors="coerce").fillna(0.0)
    risk["VARIANCE"] = risk["EXPECTED_SPEND"] - risk["PLANNED_BASELINE"]
    risk["VARIANCE_%"] = risk.apply(
        lambda r: (float(r["VARIANCE"]) / float(r["PLANNED_BASELINE"])) if float(r["PLANNED_BASELINE"]) > 0 else 0.0,
        axis=1,
    )
    risk = risk.sort_values("VARIANCE", ascending=False)

    people_baseline = float(baseline_people_financial)
    other_baseline = float(baseline_other_financial)
    variance_caption = _variance_driver_caption(by_program=risk, variance_pct=variance_pct_financial)

    st.markdown("<div id='budget-kpi-strip-anchor'></div>", unsafe_allow_html=True)
    st.markdown(
        """
        <style>
        @import url("https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20,400,0,0");
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-card {
            height: 142px;
            display: flex;
            background: transparent;
            border: 1px solid rgba(255,255,255,0.12);
            border-radius: 8px;
            overflow: hidden;
            position: relative;
            isolation: isolate;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-layer {
            position: absolute;
            inset: 0 0 0 4px;
            pointer-events: none;
            z-index: 0;
            background:
                radial-gradient(180px 96px at 95% 12%, color-mix(in srgb, var(--finops-kpi-accent) 36%, transparent), transparent 75%),
                linear-gradient(180deg, color-mix(in srgb, var(--finops-kpi-accent) 14%, transparent), transparent 62%);
            opacity: 1;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art {
            position: absolute;
            right: 0;
            bottom: 0;
            width: 52%;
            height: 58%;
            display: inline-flex;
            align-items: flex-end;
            gap: 4px;
            pointer-events: none;
            z-index: 0;
            opacity: 0.4;
            filter: saturate(1.2);
            -webkit-mask-image: linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 0.08) 28%, rgba(0, 0, 0, 0.55) 54%, #000 100%);
            mask-image: linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 0.08) 28%, rgba(0, 0, 0, 0.55) 54%, #000 100%);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-bar {
            flex: 1 1 auto;
            border-radius: 3px 3px 0 0;
            background: linear-gradient(180deg, rgba(148, 163, 184, 0.9), rgba(148, 163, 184, 0.2));
            min-height: 3px;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-bar {
            background: linear-gradient(180deg, rgba(74, 222, 128, 0.95), rgba(34, 197, 94, 0.22));
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-bar {
            background: linear-gradient(180deg, rgba(251, 191, 36, 0.95), rgba(245, 158, 11, 0.24));
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-bar {
            background: linear-gradient(180deg, rgba(248, 113, 113, 0.95), rgba(239, 68, 68, 0.24));
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--wave .finops-kpi-bg-bar,
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--dotline .finops-kpi-bg-bar,
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--curve .finops-kpi-bg-bar {
            display: none;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--wave,
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--dotline,
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--curve {
            width: 58%;
            height: 60%;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-svg {
            width: 100%;
            height: 100%;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-fill {
            fill: rgba(148, 163, 184, 0.18);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-poly {
            fill: none;
            stroke: rgba(148, 163, 184, 0.78);
            stroke-width: 1.15;
            stroke-linecap: round;
            stroke-linejoin: round;
            filter: drop-shadow(0 0 1px color-mix(in srgb, var(--finops-kpi-accent) 22%, transparent));
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-curve {
            fill: none;
            stroke: rgba(148, 163, 184, 0.8);
            stroke-width: 1.35;
            stroke-linecap: round;
            stroke-linejoin: round;
            filter: drop-shadow(0 0 1px color-mix(in srgb, var(--finops-kpi-accent) 20%, transparent));
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-dots {
            position: absolute;
            inset: 0;
            pointer-events: none;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-dot-el {
            position: absolute;
            width: 5px;
            height: 5px;
            border-radius: 50%;
            background: rgba(226, 232, 240, 0.9);
            transform: translate(-50%, -50%);
            box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.5), 0 0 3px color-mix(in srgb, var(--finops-kpi-accent) 24%, transparent);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-fill { fill: rgba(34, 197, 94, 0.16); }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-fill { fill: rgba(245, 158, 11, 0.16); }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-fill { fill: rgba(239, 68, 68, 0.16); }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-poly { stroke: rgba(74, 222, 128, 0.76); }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-poly { stroke: rgba(251, 191, 36, 0.76); }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-poly { stroke: rgba(248, 113, 113, 0.76); }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--good .finops-kpi-bg-curve { stroke: rgba(74, 222, 128, 0.78); }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--watch .finops-kpi-bg-curve { stroke: rgba(251, 191, 36, 0.78); }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-bg-art--risk .finops-kpi-bg-curve { stroke: rgba(248, 113, 113, 0.78); }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-card::after {
            content: "";
            position: absolute;
            inset: 0 0 0 4px;
            pointer-events: none;
            z-index: 0;
            background-image: repeating-radial-gradient(circle at 12% 10%, rgba(255,255,255,0.06) 0 0.7px, transparent 0.7px 2.8px);
            opacity: 0.12;
            mix-blend-mode: screen;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-accent {
            width: 4px;
            flex-shrink: 0;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-body {
            padding: 10px 12px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            width: 100%;
            height: 100%;
            position: relative;
            z-index: 1;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 4px;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-title-wrap {
            display: inline-flex;
            align-items: center;
            gap: 5px;
            min-width: 0;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-title-icon {
            font-size: 14px;
            color: rgba(148, 163, 184, 0.9);
            flex-shrink: 0;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-title {
            font-size: 0.76rem;
            color: rgba(255,255,255,0.7);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 90%;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-meta {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            flex-shrink: 0;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-help {
            font-size: 0.70rem;
            color: rgba(255,255,255,0.6);
            border: 1px solid rgba(255,255,255,0.4);
            border-radius: 50%;
            width: 14px;
            height: 14px;
            line-height: 12px;
            text-align: center;
            cursor: default;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-status {
            display: inline-flex;
            align-items: center;
            gap: 2px;
            font-size: 0.58rem;
            border-radius: 999px;
            padding: 2px 6px;
            border: 1px solid rgba(148, 163, 184, 0.35);
            background: rgba(148, 163, 184, 0.14);
            color: rgba(226, 232, 240, 0.95);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-status .material-symbols-outlined {
            font-size: 10px;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-status--good {
            border-color: rgba(34, 197, 94, 0.55);
            background: rgba(34, 197, 94, 0.16);
            color: rgba(187, 247, 208, 0.95);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-status--watch {
            border-color: rgba(245, 158, 11, 0.55);
            background: rgba(245, 158, 11, 0.16);
            color: rgba(253, 230, 138, 0.95);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-status--risk {
            border-color: rgba(239, 68, 68, 0.55);
            background: rgba(239, 68, 68, 0.16);
            color: rgba(254, 202, 202, 0.95);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-value-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-value {
            font-size: 1.35rem;
            font-weight: 600;
            margin: 2px 0 4px 0;
            color: rgba(248, 250, 252, 0.95);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-trend {
            display: inline-flex;
            align-items: center;
            gap: 2px;
            border-radius: 999px;
            font-size: 0.58rem;
            line-height: 1;
            padding: 3px 6px;
            border: 1px solid rgba(148, 163, 184, 0.35);
            background: rgba(148, 163, 184, 0.14);
            color: rgba(226, 232, 240, 0.95);
            white-space: nowrap;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-trend .material-symbols-outlined {
            font-size: 10px;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-trend--good {
            border-color: rgba(34, 197, 94, 0.55);
            background: rgba(34, 197, 94, 0.16);
            color: rgba(187, 247, 208, 0.95);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-trend--watch {
            border-color: rgba(245, 158, 11, 0.55);
            background: rgba(245, 158, 11, 0.16);
            color: rgba(253, 230, 138, 0.95);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-trend--risk {
            border-color: rgba(239, 68, 68, 0.55);
            background: rgba(239, 68, 68, 0.16);
            color: rgba(254, 202, 202, 0.95);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-chips {
            margin-bottom: 5px;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-chip {
            display: inline-block;
            font-size: 0.62rem;
            padding: 2px 6px;
            border-radius: 10px;
            margin-right: 4px;
            background: rgba(255,255,255,0.12);
            color: rgba(248, 250, 252, 0.9);
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-lines {
            font-size: 0.66rem;
            color: rgba(255,255,255,0.6);
            line-height: 1.2;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .finops-kpi-line {
            height: 0.86rem;
        }
        [data-testid="stAppViewContainer"]:has(#budget-kpi-strip-anchor) .material-symbols-outlined {
            font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 20;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ------------------------------------------------------------
    # KPI Row A (Baseline / Expected / Variance)
    # ------------------------------------------------------------
    kpi_a = st.columns(4)
    var_status = "neutral"
    if variance_pct_financial is not None:
        if abs(variance_pct_financial) <= 0.03:
            var_status = "good"
        elif abs(variance_pct_financial) <= 0.10:
            var_status = "watch"
        else:
            var_status = "risk"
    _render_finops_kpi_card(
        kpi_a[0],
        title="Baseline (Plan) – Total",
        value=_fmt_money(baseline_total_financial, 0),
        tip="Canonical baseline total for selected scope.",
        chips=["NEXT (WF+NWF)"],
        lines=["Approved baseline plan"],
        accent_color="#2563eb",
        icon="account_balance",
        status="neutral",
        status_label="Plan",
        spark_values=[0.28, 0.34, 0.41, 0.47, 0.56, 0.64, 0.72, 0.8],
        spark_tone="neutral",
        spark_motif=BUDGET_KPI_MOTIF_MAP.get("Baseline (Plan) – Total", "bars"),
    )
    _render_finops_kpi_card(
        kpi_a[1],
        title="Expected (Projection) – Total",
        value=_fmt_money(expected_total, 0),
        tip="Canonical expected total for selected scope.",
        chips=["NEXT + ADO"],
        lines=[variance_caption],
        accent_color="#0ea5e9",
        icon="query_stats",
        status=var_status,
        status_label="Forecast",
        spark_values=(
            [0.24, 0.33, 0.29, 0.46, 0.42, 0.57, 0.53, 0.69]
            if (variance_pct_financial or 0.0) >= 0
            else [0.69, 0.57, 0.62, 0.49, 0.53, 0.4, 0.43, 0.31]
        ),
        spark_tone=("risk" if (variance_pct_financial or 0.0) > 0 else "good"),
        spark_motif=BUDGET_KPI_MOTIF_MAP.get("Expected (Projection) – Total", "bars"),
    )
    _render_finops_kpi_card(
        kpi_a[2],
        title="Variance ($)",
        value=_fmt_money(variance_total_financial, 0),
        tip="Expected minus baseline.",
        chips=[],
        lines=["Absolute variance"],
        accent_color="#f59e0b",
        icon="difference",
        status=var_status,
        status_label="Variance",
        trend_text=(_fmt_money(variance_total_financial, 0) if variance_total_financial is not None else ""),
        trend_tone=var_status,
        spark_values=(
            [0.22, 0.48, 0.31, 0.55, 0.37, 0.63, 0.44, 0.71]
            if (variance_total_financial or 0.0) >= 0
            else [0.71, 0.56, 0.62, 0.45, 0.51, 0.34, 0.4, 0.24]
        ),
        spark_tone=var_status,
        spark_motif=BUDGET_KPI_MOTIF_MAP.get("Variance ($)", "wave"),
    )
    _render_finops_kpi_card(
        kpi_a[3],
        title="Variance (%)",
        value=_fmt_pct(variance_pct_financial, 1) if variance_pct_financial is not None else "—",
        tip="Relative variance against baseline.",
        chips=[],
        lines=["Percentage variance"],
        accent_color="#16a34a",
        icon="percent",
        status=var_status,
        status_label="Variance",
        trend_text=(f"{float(variance_pct_financial) * 100.0:+.1f}%" if variance_pct_financial is not None else ""),
        trend_tone=var_status,
        spark_values=(
            [0.18, 0.43, 0.29, 0.52, 0.36, 0.61, 0.44, 0.69]
            if (variance_pct_financial or 0.0) >= 0
            else [0.69, 0.55, 0.6, 0.42, 0.5, 0.31, 0.39, 0.21]
        ),
        spark_tone=var_status,
        spark_motif=BUDGET_KPI_MOTIF_MAP.get("Variance (%)", "wave"),
    )

    # ------------------------------------------------------------
    # KPI Row B (Drivers / Coverage)
    # ------------------------------------------------------------
    top_prog_name = "—"
    top_prog_var = None
    try:
        if risk is not None and not risk.empty:
            r0 = risk.sort_values("VARIANCE", ascending=False).head(1)
            if not r0.empty:
                top_prog_name = str(r0.iloc[0].get("PROGRAMNAME") or "—")
                top_prog_var = float(pd.to_numeric(r0.iloc[0].get("VARIANCE"), errors="coerce") or 0.0)
    except Exception:
        top_prog_name = "—"
        top_prog_var = None

    top_group_name = "—"
    top_group_var = None
    try:
        exp_g = work_scope.copy()
        exp_g["GROUPNAME"] = exp_g.get("GROUPNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        exp_g["AMOUNT"] = pd.to_numeric(exp_g.get("AMOUNT"), errors="coerce").fillna(0.0)
        exp_by_g = exp_g.groupby("GROUPNAME", dropna=False)["AMOUNT"].sum().reset_index().rename(columns={"AMOUNT": "EXPECTED"})
        base_by_g = pd.DataFrame(columns=["GROUPNAME", "BASELINE"])
        if baseline_cost_lines is not None and not baseline_cost_lines.empty:
            blg = baseline_cost_lines.copy()
            blg["GROUPNAME"] = blg.get("GROUPNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
            blg["AMOUNT"] = pd.to_numeric(blg.get("AMOUNT"), errors="coerce").fillna(0.0)
            base_by_g = blg.groupby("GROUPNAME", dropna=False)["AMOUNT"].sum().reset_index().rename(columns={"AMOUNT": "BASELINE"})
        vg = exp_by_g.merge(base_by_g, on="GROUPNAME", how="left")
        vg["BASELINE"] = pd.to_numeric(vg.get("BASELINE"), errors="coerce").fillna(0.0)
        vg["VARIANCE"] = pd.to_numeric(vg.get("EXPECTED"), errors="coerce").fillna(0.0) - vg["BASELINE"]
        vg = vg.sort_values("VARIANCE", ascending=False)
        if not vg.empty:
            top_group_name = str(vg.iloc[0].get("GROUPNAME") or "—")
            top_group_var = float(pd.to_numeric(vg.iloc[0].get("VARIANCE"), errors="coerce") or 0.0)
    except Exception:
        top_group_name = "—"
        top_group_var = None

    unassigned_amt = 0.0
    unassigned_pct = None
    try:
        g = work_scope.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip().str.lower()
        is_unassigned = g.eq("") | g.isin({"(unassigned)", "(needs mapping)", "unassigned", "needs mapping"})
        amt = pd.to_numeric(work_scope.get("AMOUNT"), errors="coerce").fillna(0.0)
        unassigned_amt = float(amt.loc[is_unassigned].sum() or 0.0)
        unassigned_pct = (unassigned_amt / expected_total) if expected_total > 0 else None
    except Exception:
        unassigned_amt = 0.0
        unassigned_pct = None

    coverage_pct = (1.0 - float(unassigned_pct)) if unassigned_pct is not None else None
    kpi_b = st.columns(4)
    _render_finops_kpi_card(
        kpi_b[0],
        title="Top Program driver",
        value=top_prog_name,
        tip="Program with highest expected minus baseline variance.",
        chips=[],
        lines=[_fmt_money(top_prog_var, 0) if top_prog_var is not None else "—"],
        accent_color="#7c3aed",
        icon="domain",
        status="watch" if (top_prog_var or 0.0) > 0 else "good",
        status_label="Driver",
        trend_text=(_fmt_money(top_prog_var, 0) if top_prog_var is not None else ""),
        trend_tone="watch" if (top_prog_var or 0.0) > 0 else "good",
        spark_values=(
            [0.52, 0.34, 0.58, 0.39, 0.66, 0.44, 0.73]
            if (top_prog_var or 0.0) >= 0
            else [0.73, 0.56, 0.66, 0.47, 0.58, 0.4, 0.48]
        ),
        spark_tone="watch" if (top_prog_var or 0.0) > 0 else "good",
        spark_motif=BUDGET_KPI_MOTIF_MAP.get("Top Program driver", "wave"),
    )
    _render_finops_kpi_card(
        kpi_b[1],
        title="Top Application driver",
        value=top_group_name,
        tip="Application group with highest expected minus baseline variance.",
        chips=[],
        lines=[_fmt_money(top_group_var, 0) if top_group_var is not None else "—"],
        accent_color="#0891b2",
        icon="apps",
        status="watch" if (top_group_var or 0.0) > 0 else "good",
        status_label="Driver",
        trend_text=(_fmt_money(top_group_var, 0) if top_group_var is not None else ""),
        trend_tone="watch" if (top_group_var or 0.0) > 0 else "good",
        spark_values=(
            [0.24, 0.41, 0.34, 0.56, 0.47, 0.66]
            if (top_group_var or 0.0) >= 0
            else [0.66, 0.57, 0.61, 0.46, 0.4, 0.31]
        ),
        spark_tone="watch" if (top_group_var or 0.0) > 0 else "good",
        spark_motif=BUDGET_KPI_MOTIF_MAP.get("Top Application driver", "wave"),
    )
    _render_finops_kpi_card(
        kpi_b[2],
        title="Unassigned share",
        value=_fmt_pct(unassigned_pct, 1) if unassigned_pct is not None else "—",
        tip="Expected spend not yet mapped to app groups.",
        chips=[],
        lines=[_fmt_money(unassigned_amt, 0) if unassigned_amt else "—"],
        accent_color="#f97316",
        icon="link_off",
        status="risk" if (unassigned_pct or 0.0) >= 0.10 else ("watch" if (unassigned_pct or 0.0) >= 0.03 else "good"),
        status_label="Mapping",
        trend_text=(_fmt_money(unassigned_amt, 0) if unassigned_amt else ""),
        trend_tone="risk" if (unassigned_pct or 0.0) >= 0.10 else ("watch" if (unassigned_pct or 0.0) >= 0.03 else "good"),
        spark_values=(
            [0.16, 0.31, 0.25, 0.45, 0.37, 0.61, 0.53, 0.76]
            if (unassigned_pct or 0.0) >= 0.03
            else [0.08, 0.16, 0.12, 0.2, 0.17, 0.26, 0.23, 0.31]
        ),
        spark_tone="risk" if (unassigned_pct or 0.0) >= 0.10 else ("watch" if (unassigned_pct or 0.0) >= 0.03 else "good"),
        spark_motif=BUDGET_KPI_MOTIF_MAP.get("Unassigned share", "dotline"),
    )
    _render_finops_kpi_card(
        kpi_b[3],
        title="Coverage",
        value=_fmt_pct(coverage_pct, 0) if coverage_pct is not None else "—",
        tip="Share of expected spend mapped to app groups.",
        chips=[],
        lines=["1 - unassigned share"],
        accent_color="#22c55e",
        icon="verified",
        status="good" if (coverage_pct or 0.0) >= 0.90 else ("watch" if (coverage_pct or 0.0) >= 0.80 else "risk"),
        status_label="Coverage",
        trend_text=(f"{(coverage_pct or 0.0) * 100.0:.0f}%" if coverage_pct is not None else ""),
        trend_tone="good" if (coverage_pct or 0.0) >= 0.90 else ("watch" if (coverage_pct or 0.0) >= 0.80 else "risk"),
        spark_values=(
            [0.38, 0.62, 0.42, 0.58, 0.39, 0.61, 0.43, 0.57]
            if (coverage_pct or 0.0) >= 0.8
            else [0.22, 0.36, 0.29, 0.44, 0.33, 0.48, 0.39, 0.54]
        ),
        spark_tone="good" if (coverage_pct or 0.0) >= 0.90 else ("watch" if (coverage_pct or 0.0) >= 0.80 else "risk"),
        spark_motif=BUDGET_KPI_MOTIF_MAP.get("Coverage", "curve"),
    )

    # ------------------------------------------------------------
    # Hero visual: Variance by Program
    # ------------------------------------------------------------
    st.markdown("##### Variance by Program (Expected − Baseline)")
    top_n = 15
    var_prog = risk.copy() if risk is not None else pd.DataFrame(columns=["PROGRAMNAME", "VARIANCE"])
    if not var_prog.empty:
        var_prog["PROGRAMNAME"] = var_prog.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
        var_prog["VARIANCE"] = pd.to_numeric(var_prog.get("VARIANCE"), errors="coerce").fillna(0.0)
        var_prog = var_prog.sort_values("VARIANCE", ascending=False)
        head = var_prog.head(top_n).copy()
        tail = var_prog.iloc[top_n:].copy()
        if not tail.empty:
            head = pd.concat(
                [head, pd.DataFrame([{"PROGRAMNAME": "Other", "VARIANCE": float(tail["VARIANCE"].sum() or 0.0)}])],
                ignore_index=True,
            )
        head = head[head["PROGRAMNAME"].astype(str).str.strip().ne("")].copy()
        cats = head["PROGRAMNAME"].astype(str).tolist()[::-1]
        vals = [float(x) for x in head["VARIANCE"].tolist()][::-1]
        opt = {
            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
            "grid": {"left": "3%", "right": "3%", "bottom": "6%", "top": "10%", "containLabel": True},
            "xAxis": {"type": "value", "axisLabel": {"formatter": "${value}"}},
            "yAxis": {"type": "category", "data": cats},
            "series": [{"type": "bar", "data": vals, "itemStyle": {"color": "#f59e0b"}}],
        }
        render_echart(opt, height="420px", key=f"budget_variance_by_program_{scope_key}_{rev}", page_theme=page_theme)
    else:
        st.info("No program variance data for this scope.")

    # ------------------------------------------------------------
    # Insights + Data Readiness (moved up under the hero chart)
    # ------------------------------------------------------------
    c_left, c_right = st.columns(2)
    with c_left:
        with st.container(border=True):
            st.markdown("##### Insights")
            insights: List[str] = []
            total_expected = float(risk["EXPECTED_SPEND"].sum() or 0.0) if not risk.empty else 0.0
            if not risk.empty and total_expected > 0:
                top_prog = risk.sort_values("EXPECTED_SPEND", ascending=False).head(1)
                if not top_prog.empty:
                    p = str(top_prog.iloc[0].get("PROGRAMNAME") or "(Unassigned)")
                    share = float(top_prog.iloc[0].get("EXPECTED_SPEND") or 0.0) / total_expected
                    insights.append(f"Top cost driver: Program {p} (≈ {_fmt_pct(share, 0)} of expected spend).")
            if baseline_total_financial > 0:
                insights.append(
                    f"WF vs NWF mix: {_fmt_pct(people_baseline / baseline_total_financial, 0)} WF, {_fmt_pct(other_baseline / baseline_total_financial, 0)} NWF."
                )
            if variance_pct_financial is not None:
                insights.append(f"Variance vs plan: {_fmt_pct(variance_pct_financial, 0)} • {variance_caption}")
            _bullets(insights[:4])

    with c_right:
        with st.container(border=True):
            st.markdown("##### Data readiness")
            readiness: List[str] = []

        # Missing headcount where demand exists (capacity assumed zero).
        # Use the same filtered demand/capacity datasets the other tabs use (multi-year aware).
        demand_check = demand_multi_all.copy() if demand_multi_all is not None else pd.DataFrame()
        cap_check = cap_multi_all.copy() if cap_multi_all is not None else pd.DataFrame()

        # Fallback: synthesize missing capacity rows from headcount history tables.
        # This targets cases where demand exists but `VW_TEAM_ALLOCATED_HEADCOUNT_PI` is missing
        # because `VW_TEAM_COMPOSITION_EFFECTIVE` is stale.
        try:
            if demand_check is not None and not demand_check.empty:
                demc0 = demand_check.copy()
                demc0["ADO_YEAR"] = pd.to_numeric(demc0.get("ADO_YEAR"), errors="coerce").astype("Int64")
                demc0["PI_NUM"] = pd.to_numeric(demc0.get("PI_NUM"), errors="coerce").astype("Int64")
                demc0 = demc0[demc0["ADO_YEAR"].notna() & demc0["PI_NUM"].notna() & demc0["PI_NUM"].isin(pi_nums)].copy()
                demc0["PROGRAMNAME"] = demc0.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
                demc0["TEAMNAME"] = demc0.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
                demc0["DEMAND_FTE"] = pd.to_numeric(demc0.get("DERIVED_FTE_SUM"), errors="coerce").fillna(0.0)

                dem_need = demc0.loc[demc0["DEMAND_FTE"] > 0, ["ADO_YEAR", "PI_NUM", "PROGRAMNAME", "TEAMNAME"]].drop_duplicates()
                dem_need["_PROG_KEY"] = dem_need["PROGRAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})
                dem_need["_TEAM_KEY"] = dem_need["TEAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})

                cap_need = cap_check.copy() if cap_check is not None else pd.DataFrame()
                if cap_need is not None and not cap_need.empty:
                    cap_need["ADO_YEAR"] = pd.to_numeric(cap_need.get("ADO_YEAR"), errors="coerce").astype("Int64")
                    cap_need["ITERATION_NUM"] = pd.to_numeric(cap_need.get("ITERATION_NUM"), errors="coerce").astype("Int64")
                    cap_need = cap_need[cap_need["ADO_YEAR"].notna() & cap_need["ITERATION_NUM"].notna() & cap_need["ITERATION_NUM"].isin(pi_nums)].copy()
                    cap_need["PROGRAMNAME"] = cap_need.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
                    cap_need["TEAMNAME"] = cap_need.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
                    cap_need["_PROG_KEY"] = cap_need["PROGRAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})
                    cap_need["_TEAM_KEY"] = cap_need["TEAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})

                if cap_need is None or cap_need.empty or "ADO_YEAR" not in cap_need.columns or "ITERATION_NUM" not in cap_need.columns:
                    cap_keys = pd.DataFrame(columns=["ADO_YEAR", "PI_NUM", "_PROG_KEY", "_TEAM_KEY"])
                else:
                    cap_keys = (
                        cap_need[["ADO_YEAR", "ITERATION_NUM", "_PROG_KEY", "_TEAM_KEY"]]
                        .drop_duplicates()
                        .rename(columns={"ITERATION_NUM": "PI_NUM"})
                    )

                missing = dem_need.merge(cap_keys, on=["ADO_YEAR", "PI_NUM", "_PROG_KEY", "_TEAM_KEY"], how="left", indicator=True)
                missing = missing.loc[missing["_merge"].ne("both")].copy()
                if (cap_check is None or cap_check.empty) or not missing.empty:
                    missing_teams = sorted(set(missing["TEAMNAME"].astype(str).tolist())) if not missing.empty else list(effective_teams or ())
                    fb = _fallback_capacity_from_headcount(
                        years=[int(outlook_year)],
                        programs=effective_programs,
                        teams=missing_teams,
                        demand_multi=demand_check,
                    )
                    fb = fb if fb is not None else pd.DataFrame()
                    if fb is not None and not fb.empty:
                        fb2 = fb.copy()
                        fb2["ADO_YEAR"] = pd.to_numeric(fb2.get("ADO_YEAR"), errors="coerce").astype("Int64")
                        fb2["ITERATION_NUM"] = pd.to_numeric(fb2.get("ITERATION_NUM"), errors="coerce").astype("Int64")
                        fb2 = fb2[fb2["ITERATION_NUM"].notna() & fb2["ITERATION_NUM"].isin(pi_nums)].copy()
                        fb2["PROGRAMNAME"] = fb2.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
                        fb2["TEAMNAME"] = fb2.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
                        fb2["_PROG_KEY"] = fb2["PROGRAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})
                        fb2["_TEAM_KEY"] = fb2["TEAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})

                        if not missing.empty:
                            missing_keys = set(
                                (int(r["ADO_YEAR"]), int(r["PI_NUM"]), str(r["_PROG_KEY"]), str(r["_TEAM_KEY"]))
                                for _, r in missing.loc[missing["ADO_YEAR"].notna() & missing["PI_NUM"].notna()].iterrows()
                            )
                            fb2 = fb2.loc[
                                fb2.apply(
                                    lambda r: (int(r["ADO_YEAR"]), int(r["ITERATION_NUM"]), str(r["_PROG_KEY"]), str(r["_TEAM_KEY"])) in missing_keys,
                                    axis=1,
                                )
                            ].copy()

                        if cap_check is None or cap_check.empty:
                            cap_check = fb2.drop(columns=["_PROG_KEY", "_TEAM_KEY"], errors="ignore")
                        else:
                            cap_check = pd.concat(
                                [cap_check, fb2.drop(columns=["_PROG_KEY", "_TEAM_KEY"], errors="ignore")],
                                ignore_index=True,
                            )
        except Exception:
            pass

        missing_hc: List[str] = []
        if demand_check is not None and not demand_check.empty:
            demc = demand_check.copy()
            demc["ADO_YEAR"] = pd.to_numeric(demc.get("ADO_YEAR"), errors="coerce").astype("Int64")
            demc["PI_NUM"] = pd.to_numeric(demc.get("PI_NUM"), errors="coerce").astype("Int64")
            demc = demc[demc["PI_NUM"].notna() & demc["PI_NUM"].isin(pi_nums)].copy()
            demc["TEAMNAME"] = demc.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
            dem_fte_series = demc["DERIVED_FTE_SUM"] if "DERIVED_FTE_SUM" in demc.columns else pd.Series([0.0] * len(demc), index=demc.index)
            demc["DEMAND_FTE"] = pd.to_numeric(dem_fte_series, errors="coerce").fillna(0.0)
            dem_team_pi = demc.groupby(["ADO_YEAR", "TEAMNAME", "PI_NUM"], dropna=False)["DEMAND_FTE"].sum().reset_index()

            capc = cap_check.copy() if cap_check is not None else pd.DataFrame()
            capc["ADO_YEAR"] = pd.to_numeric(capc.get("ADO_YEAR", pd.Series(dtype="float")), errors="coerce").astype("Int64")
            capc["ITERATION_NUM"] = pd.to_numeric(capc.get("ITERATION_NUM", pd.Series(dtype="float")), errors="coerce").astype("Int64")
            capc = capc[capc["ITERATION_NUM"].notna() & capc["ITERATION_NUM"].isin(pi_nums)].copy()
            cap_team_series = capc["TEAMNAME"] if "TEAMNAME" in capc.columns else pd.Series([""] * len(capc), index=capc.index)
            capc["TEAMNAME"] = cap_team_series.astype(str).str.strip().replace({"": "(Unassigned)"})
            cap_hc_series = capc["ALLOCATED_HEADCOUNT"] if "ALLOCATED_HEADCOUNT" in capc.columns else pd.Series([0.0] * len(capc), index=capc.index)
            capc["ALLOCATED_HEADCOUNT"] = pd.to_numeric(cap_hc_series, errors="coerce").fillna(0.0)
            cap_team_pi = capc.groupby(["ADO_YEAR", "TEAMNAME", "ITERATION_NUM"], dropna=False)["ALLOCATED_HEADCOUNT"].sum().reset_index()
            cap_team_pi = cap_team_pi.rename(columns={"ITERATION_NUM": "PI_NUM"})

            joined = dem_team_pi.merge(cap_team_pi, on=["ADO_YEAR", "TEAMNAME", "PI_NUM"], how="left")
            joined["ALLOCATED_HEADCOUNT"] = pd.to_numeric(joined.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
            miss = joined[(joined["DEMAND_FTE"] > 0) & (joined["ALLOCATED_HEADCOUNT"] <= 0)].copy()
            note_lookup: dict[tuple[str, int, int], str] = {}
            try:
                if not miss.empty:
                    teams_miss = miss["TEAMNAME"].dropna().astype(str).str.strip().unique().tolist()
                    years_miss = (
                        pd.to_numeric(miss.get("ADO_YEAR"), errors="coerce").dropna().astype(int).unique().tolist()
                        if "ADO_YEAR" in miss.columns
                        else []
                    )
                    notes_raw = _load_headcount_notes_team_pi_years(years=years_miss, team_names=teams_miss)
                    if notes_raw is not None and not notes_raw.empty:
                        nr = notes_raw.copy()
                        nr["TEAM_KEY"] = nr.get("TEAMNAME", "").astype(str).str.upper().str.strip()
                        nr["YEAR"] = pd.to_numeric(nr.get("YEAR"), errors="coerce").astype("Int64")
                        nr["PI_NUM"] = pd.to_numeric(nr.get("PI_NUM"), errors="coerce").astype("Int64")
                        nr["NOTE"] = nr.get("NOTE", "").fillna("").astype(str).str.strip()
                        nr = nr[nr["TEAM_KEY"].ne("") & nr["YEAR"].notna() & nr["PI_NUM"].notna() & nr["NOTE"].ne("")].copy()

                        def _join_notes(s: pd.Series) -> str:
                            return "; ".join(sorted(set([str(x).strip() for x in s.tolist() if str(x).strip()])))

                        n0 = (
                            nr.loc[nr["PI_NUM"] == 0]
                            .groupby(["TEAM_KEY", "YEAR"], dropna=False)["NOTE"]
                            .apply(_join_notes)
                            .to_dict()
                        )
                        npi = (
                            nr.loc[nr["PI_NUM"].isin([1, 2, 3, 4])]
                            .groupby(["TEAM_KEY", "YEAR", "PI_NUM"], dropna=False)["NOTE"]
                            .apply(_join_notes)
                            .to_dict()
                        )
                        for key, val in npi.items():
                            team_key, y, pi = key
                            note_lookup[(str(team_key), int(y), int(pi))] = str(val)
                        for key, val in n0.items():
                            team_key, y = key
                            for pi in [1, 2, 3, 4]:
                                note_lookup.setdefault((str(team_key), int(y), int(pi)), str(val))
            except Exception:
                note_lookup = {}

            for _, r in miss.head(6).iterrows():
                y = int(r["ADO_YEAR"]) if pd.notna(r.get("ADO_YEAR")) else int(outlook_year)
                pi_num = int(r["PI_NUM"]) if pd.notna(r.get("PI_NUM")) else 0
                note = note_lookup.get((str(r["TEAMNAME"]).strip().upper(), int(y), int(pi_num)), "")
                suffix = f" Note: {note}" if note else ""
                missing_hc.append(f"⚠️ Team {r['TEAMNAME']} has demand but no headcount in {y} I{pi_num}.{suffix}")

        if missing_hc:
            readiness.extend(missing_hc)
        else:
            readiness.append("✅ Headcount coverage looks OK for demand in this scope.")

        # Missing rates where demand exists (cost underreported).
        team_names_with_demand = []
        if demand_check is not None and not demand_check.empty:
            demc2 = demand_check.copy()
            demc2["DERIVED_FTE_SUM"] = pd.to_numeric(demc2.get("DERIVED_FTE_SUM"), errors="coerce").fillna(0.0)
            team_names_with_demand = (
                demc2.loc[demc2["DERIVED_FTE_SUM"] > 0, "TEAMNAME"].dropna().astype(str).str.strip().unique().tolist()
                if "TEAMNAME" in demc2.columns
                else []
            )
        missing_rates = _missing_rate_teams_for_year(year=int(outlook_year), team_names=team_names_with_demand)
        if missing_rates:
            readiness.append(f"⚠️ {len(missing_rates)} team(s) have demand but no rate configured (costs may be underreported).")
        else:
            readiness.append("✅ Rates look OK for teams with demand (best-effort check).")

        _bullets(readiness[:6])

    # Detailed views moved into dedicated tabs/expanders to keep Overview leadership-friendly.

    if show_debug:
        with st.expander("Debug: compare to Welcome logic", expanded=False):
            st.caption(
                "Validation only: these KPI totals come from canonical BASELINE/EXPECTED cost lines "
                "(via `core.cost_model`, after the centralized program overhead fix in `core.canonical_costs`)."
            )
            d1, d2, d3 = st.columns(3)
            d1.metric("Planned baseline total", _fmt_money(baseline_total_financial, 0))
            d2.metric("Expected total", _fmt_money(expected_total, 0))
            d3.metric("Variance", _fmt_money(variance_total_financial, 0))

    if show_debug:
        with st.expander("WF Baseline vs Expected – debug", expanded=False):
            st.write(
                {
                    "baseline_people_capacity_total": float(baseline_people_capacity_total),
                    "overhead_people_primary": float(overhead_people_primary),
                    "baseline_wf_other": float(baseline_wf_other),
                    "baseline_people_total": float(baseline_people_total),
                    "expected_people_from_view": float(expected_people_from_view),
                    "overhead_people_cost_from_view": float(overhead_people_cost_from_view),
                    "expected_people_total": float(expected_people_total),
                }
            )

        wf_recon = pd.DataFrame()
        try:
            # 1) Expected WF cost per Team/PI from the unified view (exclude program overhead).
            exp_team_pi = pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "EXPECTED_WF_COST_FROM_VIEW_TEAM_PI"])
            if work_scope is not None and not work_scope.empty:
                w = work_scope.copy()
                w["YEAR"] = pd.to_numeric(w.get("YEAR"), errors="coerce").astype("Int64")
                w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce").astype("Int64")
                w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").astype(str).str.strip()
                w["TEAMNAME"] = w.get("TEAMNAME", "").astype(str).str.strip()
                w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)
                w_sub = w.get("SUBCOMPONENT", "").astype(str).str.upper()
                is_wf_row = w.get("CAT", pd.Series(dtype=str)).astype(str).eq("WORK_FORCE")
                w = w[is_wf_row & ~w_sub.eq("PROGRAM")].copy()
                src_up = w.get("SOURCE", pd.Series("", index=w.index, dtype="object")).fillna("").astype(str).str.upper()
                w = w.loc[src_up.ne("APPTIO")].copy()
                exp_team_pi = (
                    w.groupby(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], dropna=False)["AMOUNT"]
                    .sum()
                    .reset_index()
                    .rename(columns={"AMOUNT": "EXPECTED_WF_COST_FROM_VIEW_TEAM_PI"})
                )

            # 2) Demand (Derived FTE) per Team/PI from Explorer aggregation (use all groups for stable team totals).
            dem_team_pi = pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "DEMAND_DERIVED_FTE"])
            if eff_by_group_weights is not None and not eff_by_group_weights.empty:
                d = eff_by_group_weights.copy()
                d["YEAR"] = pd.to_numeric(d.get("YEAR"), errors="coerce").astype("Int64")
                d["PI"] = pd.to_numeric(d.get("PI"), errors="coerce").astype("Int64")
                d["PROGRAMNAME"] = d.get("PROGRAMNAME", "").astype(str).str.strip()
                d["TEAMNAME"] = d.get("TEAMNAME", "").astype(str).str.strip()
                d["DERIVED_FTE_EXPLORER"] = pd.to_numeric(d.get("DERIVED_FTE_EXPLORER"), errors="coerce").fillna(0.0)
                dem_team_pi = (
                    d.groupby(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], dropna=False)["DERIVED_FTE_EXPLORER"]
                    .sum()
                    .reset_index()
                    .rename(columns={"DERIVED_FTE_EXPLORER": "DEMAND_DERIVED_FTE"})
                )

            # 3) Capacity baseline per Team/PI.
            base = wf_baseline_cap_team_pi.copy() if wf_baseline_cap_team_pi is not None else pd.DataFrame()
            if base is None or base.empty:
                wf_recon = pd.DataFrame()
            else:
                base["YEAR"] = pd.to_numeric(base.get("YEAR"), errors="coerce").astype("Int64")
                base["PI"] = pd.to_numeric(base.get("PI"), errors="coerce").astype("Int64")
                base["PROGRAMNAME"] = base.get("PROGRAMNAME", "").astype(str).str.strip()
                base["TEAMNAME"] = base.get("TEAMNAME", "").astype(str).str.strip()
                base["CAPACITY_FTE"] = pd.to_numeric(base.get("CAPACITY_FTE"), errors="coerce").fillna(0.0)
                base["WF_BASELINE_CAPACITY"] = pd.to_numeric(base.get("WF_BASELINE_TOTAL_PI"), errors="coerce").fillna(0.0)
                base = base[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CAPACITY_FTE", "WF_BASELINE_CAPACITY"]].copy()

                wf_recon = base.merge(dem_team_pi, on=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], how="left")
                wf_recon = wf_recon.merge(exp_team_pi, on=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], how="left")
                for c in ["DEMAND_DERIVED_FTE", "EXPECTED_WF_COST_FROM_VIEW_TEAM_PI"]:
                    wf_recon[c] = pd.to_numeric(wf_recon.get(c), errors="coerce").fillna(0.0)
                wf_recon = wf_recon.sort_values(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"])

        except Exception:
            wf_recon = pd.DataFrame()

        if wf_recon is not None and not wf_recon.empty:
            st.markdown("Team/PI WF reconciliation")
            st.dataframe(wf_recon, use_container_width=True, hide_index=True, height=360)
        else:
            st.info("WF reconciliation is not available for this scope (missing capacity, demand, or cost rows).")

    with st.expander("Deep dive", expanded=False):
        with st.container(border=True):
            st.markdown("##### YTD Planned vs Expected")
            st.caption("Cumulative planned baseline vs expected spend across increments for the selected filters.")

        ytd_opts = sorted([int(y) for y in years_sel])
        ytd_default = current_year if current_year in ytd_opts else (max(ytd_opts) if ytd_opts else outlook_year)
        ytd_year = st.selectbox(
            "Year",
            options=ytd_opts,
            index=ytd_opts.index(int(ytd_default)) if ytd_opts and int(ytd_default) in ytd_opts else 0,
            key="budget_general_ytd_year",
            help="YTD chart is shown for one year at a time.",
        )
        pi_list = [int(p) for p in sorted(set(pi_nums or [1, 2, 3, 4])) if 1 <= int(p) <= 4]

        ys = work_scope.loc[work_scope["YEAR"] == int(ytd_year)].copy()
        ys["PI"] = pd.to_numeric(ys.get("PI"), errors="coerce").astype("Int64")
        ys = ys[ys["PI"].notna() & ys["PI"].isin(pi_list)].copy()
        exp_pi_total = ys.groupby("PI", dropna=False)["AMOUNT"].sum().reset_index().rename(columns={"AMOUNT": "EXPECTED_PI_TOTAL"})

        bl_y = baseline_cost_lines.copy() if baseline_cost_lines is not None else pd.DataFrame()
        if "YEAR" not in bl_y.columns:
            bl_y["YEAR"] = pd.NA
        if "PI" not in bl_y.columns:
            bl_y["PI"] = pd.NA
        if "AMOUNT" not in bl_y.columns:
            bl_y["AMOUNT"] = 0.0
        bl_y["YEAR"] = pd.to_numeric(bl_y.get("YEAR"), errors="coerce").astype("Int64")
        bl_y["PI"] = pd.to_numeric(bl_y.get("PI"), errors="coerce").astype("Int64")
        bl_y = bl_y[(bl_y["YEAR"] == int(ytd_year)) & bl_y["PI"].notna() & bl_y["PI"].isin(pi_list)].copy()
        base_pi_total = (
            bl_y.groupby("PI", dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "BASELINE_PI_TOTAL"})
        )

        df_pi = pd.DataFrame({"PI": pi_list})
        df_pi = df_pi.merge(exp_pi_total, on="PI", how="left")
        df_pi = df_pi.merge(base_pi_total, on="PI", how="left")
        for c in ["EXPECTED_PI_TOTAL", "BASELINE_PI_TOTAL"]:
            df_pi[c] = pd.to_numeric(df_pi.get(c), errors="coerce").fillna(0.0)
        df_pi = df_pi.sort_values("PI")
        df_pi["BASELINE_YTD"] = df_pi["BASELINE_PI_TOTAL"].cumsum()
        df_pi["EXPECTED_YTD"] = df_pi["EXPECTED_PI_TOTAL"].cumsum()
        df_pi["VARIANCE_YTD"] = df_pi["EXPECTED_YTD"] - df_pi["BASELINE_YTD"]

        x = [f"I{int(p)}" for p in df_pi["PI"].astype(int).tolist()]
        y_base = df_pi["BASELINE_YTD"].astype(float).round(0).tolist()
        y_exp = df_pi["EXPECTED_YTD"].astype(float).round(0).tolist()
        opt = {
            "title": {"text": f"{int(ytd_year)} YTD baseline vs expected", "left": "center"},
            "tooltip": {"trigger": "axis"},
            "legend": {"top": 26, "data": ["Planned baseline (YTD)", "Expected spend (YTD)"]},
            "grid": {"left": 70, "right": 30, "top": 70, "bottom": 60},
            "xAxis": {"type": "category", "data": x},
            "yAxis": {"type": "value", "axisLabel": {"formatter": "${value}"}},
            "series": [
                {"name": "Planned baseline (YTD)", "type": "line", "data": y_base, "itemStyle": {"color": "#22c55e"}},
                {"name": "Expected spend (YTD)", "type": "line", "data": y_exp, "itemStyle": {"color": "#ef4444"}},
            ],
        }
        render_echart(opt, height="320px", key=f"budget_ytd_{scope_key}_{ytd_year}", page_theme=page_theme)

        # ------------------------------------------------------------
        # Program Health (YTD)
        # ------------------------------------------------------------
        st.subheader("Program Health")
        st.caption("Planned baseline vs expected spend year-to-date by program (based on the same YTD scope above).")

        program_health_df = pd.DataFrame(columns=["PROGRAMNAME", "PLANNED_YTD", "EXPECTED_YTD", "VARIANCE_PCT"])
        try:
            # Use a YTD PI cutoff for the selected year (current year -> current PI; past years -> full year).
            today = dt.date.today()
            if int(ytd_year) < int(today.year):
                ytd_pi_limit = 4
            elif int(ytd_year) > int(today.year):
                ytd_pi_limit = 0
            else:
                ytd_pi_limit = int(max(1, min(4, math.ceil(int(today.month) / 3.0))))

            ys2 = work_scope.copy()
            ys2["YEAR"] = pd.to_numeric(ys2.get("YEAR"), errors="coerce").astype("Int64")
            ys2["PI"] = pd.to_numeric(ys2.get("PI"), errors="coerce").astype("Int64")
            ys2 = ys2[(ys2["YEAR"] == int(ytd_year)) & ys2["PI"].notna()].copy()
            if ytd_pi_limit > 0:
                ys2 = ys2[ys2["PI"].astype(int) <= int(ytd_pi_limit)].copy()

            ys2["PROGRAMNAME"] = ys2.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
            ys2["AMOUNT"] = pd.to_numeric(ys2.get("AMOUNT"), errors="coerce").fillna(0.0)
            exp_by_prog = (
                ys2.groupby("PROGRAMNAME", dropna=False)["AMOUNT"]
                .sum()
                .reset_index()
                .rename(columns={"AMOUNT": "EXPECTED_YTD"})
            )

            bl2 = baseline_cost_lines.copy() if baseline_cost_lines is not None else pd.DataFrame()
            if "YEAR" not in bl2.columns:
                bl2["YEAR"] = pd.NA
            if "PI" not in bl2.columns:
                bl2["PI"] = pd.NA
            if "AMOUNT" not in bl2.columns:
                bl2["AMOUNT"] = 0.0
            bl2["YEAR"] = pd.to_numeric(bl2.get("YEAR"), errors="coerce").astype("Int64")
            bl2["PI"] = pd.to_numeric(bl2.get("PI"), errors="coerce").astype("Int64")
            bl2 = bl2[(bl2["YEAR"] == int(ytd_year)) & bl2["PI"].notna()].copy()
            if ytd_pi_limit > 0:
                bl2 = bl2[bl2["PI"].astype(int) <= int(ytd_pi_limit)].copy()
            bl2["PROGRAMNAME"] = bl2.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
            bl2["AMOUNT"] = pd.to_numeric(bl2.get("AMOUNT"), errors="coerce").fillna(0.0)
            base_ytd_by_prog = (
                bl2.groupby("PROGRAMNAME", dropna=False)["AMOUNT"]
                .sum()
                .reset_index()
                .rename(columns={"AMOUNT": "PLANNED_YTD"})
            )

            program_health_df = exp_by_prog.merge(base_ytd_by_prog, on="PROGRAMNAME", how="outer")
            for c in ["EXPECTED_YTD", "PLANNED_YTD"]:
                program_health_df[c] = pd.to_numeric(program_health_df.get(c), errors="coerce").fillna(0.0)
            program_health_df["VARIANCE_PCT"] = program_health_df.apply(
                lambda r: (float(r["EXPECTED_YTD"]) - float(r["PLANNED_YTD"])) / float(r["PLANNED_YTD"])
                if float(r["PLANNED_YTD"]) > 0
                else None,
                axis=1,
            )
            program_health_df = program_health_df[["PROGRAMNAME", "PLANNED_YTD", "EXPECTED_YTD", "VARIANCE_PCT"]].copy()
            program_health_df["ABS_VAR"] = (program_health_df["EXPECTED_YTD"] - program_health_df["PLANNED_YTD"]).abs()
            program_health_df = program_health_df.sort_values("ABS_VAR", ascending=False).drop(columns=["ABS_VAR"], errors="ignore")
        except Exception:
            program_health_df = pd.DataFrame(columns=["PROGRAMNAME", "PLANNED_YTD", "EXPECTED_YTD", "VARIANCE_PCT"])

        if program_health_df.empty:
            st.info("No program health rows available for this scope/year.")
        else:
            show_ph = program_health_df.copy()
            show_ph["Planned YTD"] = show_ph["PLANNED_YTD"].map(lambda v: _fmt_money(float(v or 0.0), 0))
            show_ph["Expected YTD"] = show_ph["EXPECTED_YTD"].map(lambda v: _fmt_money(float(v or 0.0), 0))
            show_ph["Variance %"] = show_ph["VARIANCE_PCT"].map(
                lambda v: _fmt_pct(float(v), 0) if v is not None and not pd.isna(v) else "—"
            )
            show_ph = show_ph.rename(columns={"PROGRAMNAME": "Program"})
            st.dataframe(
                show_ph[["Program", "Planned YTD", "Expected YTD", "Variance %"]],
                use_container_width=True,
                hide_index=True,
                height=320,
            )

            st.subheader("Top 5 Over and Under Budget Programs")
            top_src = program_health_df.copy()
            top_src = top_src.dropna(subset=["VARIANCE_PCT"]).copy()
            top_over = top_src.loc[top_src["VARIANCE_PCT"] > 0].sort_values("VARIANCE_PCT", ascending=False).head(5).copy()
            top_under = top_src.loc[top_src["VARIANCE_PCT"] < 0].sort_values("VARIANCE_PCT", ascending=True).head(5).copy()
            c1, c2 = st.columns(2)
            with c1:
                st.caption("Top 5 Over Budget")
                if top_over.empty:
                    st.info("No over-budget programs in this scope.")
                else:
                    show_over = top_over.copy()
                    show_over["Planned YTD"] = show_over["PLANNED_YTD"].map(lambda v: _fmt_money(float(v or 0.0), 0))
                    show_over["Expected YTD"] = show_over["EXPECTED_YTD"].map(lambda v: _fmt_money(float(v or 0.0), 0))
                    show_over["Variance %"] = show_over["VARIANCE_PCT"].map(
                        lambda v: _fmt_pct(float(v), 0) if v is not None and not pd.isna(v) else "—"
                    )
                    st.dataframe(
                        show_over.rename(columns={"PROGRAMNAME": "Program"})[["Program", "Planned YTD", "Expected YTD", "Variance %"]],
                        use_container_width=True,
                        hide_index=True,
                        height=240,
                    )
            with c2:
                st.caption("Top 5 Under Budget")
                if top_under.empty:
                    st.info("No under-budget programs in this scope.")
                else:
                    show_under = top_under.copy()
                    show_under["Planned YTD"] = show_under["PLANNED_YTD"].map(lambda v: _fmt_money(float(v or 0.0), 0))
                    show_under["Expected YTD"] = show_under["EXPECTED_YTD"].map(lambda v: _fmt_money(float(v or 0.0), 0))
                    show_under["Variance %"] = show_under["VARIANCE_PCT"].map(
                        lambda v: _fmt_pct(float(v), 0) if v is not None and not pd.isna(v) else "—"
                    )
                    st.dataframe(
                        show_under.rename(columns={"PROGRAMNAME": "Program"})[["Program", "Planned YTD", "Expected YTD", "Variance %"]],
                        use_container_width=True,
                        hide_index=True,
                        height=240,
                    )

with tabs_by_name["Budget Explorer"]:
    st.subheader("Budget Explorer (Baseline)")
    st.caption("Explore budget, demand, and allocation across the portfolio")
    _debug_baseline_placeholder = st.empty()
    # `baseline_long` is prepared once above and reused here to keep all General-tab totals consistent.

    if baseline_long is None or baseline_long.empty:
        st.info("No baseline rows found for this scope.")
        if show_debug:
            with st.expander("Explorer debug", expanded=False):
                st.write(
                    {
                        "work_scope_rows": int(len(work_scope.index)) if work_scope is not None else 0,
                        "wf_baseline_cap_by_group_sub_rows": int(len(wf_baseline_cap_by_group_sub.index))
                        if wf_baseline_cap_by_group_sub is not None
                        else 0,
                        "overhead_inputs_rows": int(len(_overhead_inputs_all.index)) if _overhead_inputs_all is not None else 0,
                    }
                )
                if baseline_err:
                    st.code(baseline_err)
    else:
            base2 = baseline_long.copy()
            base2["YEAR"] = pd.to_numeric(base2.get("YEAR"), errors="coerce").astype("Int64")
            base2["PI"] = pd.to_numeric(base2.get("PI"), errors="coerce").astype("Int64")
            base2["PROGRAMNAME"] = base2.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
            base2["TEAMNAME"] = base2.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
            base2["SUBCOMPONENT"] = base2.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
            base2["COST_TYPE"] = base2.get("COST_TYPE", "").fillna("").astype(str).str.strip()
            base2["RESOURCE"] = base2.get("RESOURCE", "").fillna("").astype(str).str.strip()
            base2["PROVIDER"] = base2.get("PROVIDER", "").fillna("").astype(str).str.strip()
            base2["BASELINE_COST"] = pd.to_numeric(base2.get("BASELINE_COST"), errors="coerce").fillna(0.0)
            base2["HEADCOUNT"] = pd.to_numeric(base2.get("HEADCOUNT"), errors="coerce").fillna(0.0)

            base2["PI_LABEL"] = base2.apply(
                lambda r: f"{int(r['YEAR'])} I{int(r['PI'])}" if pd.notna(r.get("YEAR")) and pd.notna(r.get("PI")) else "",
                axis=1,
            )

            # Default resource/provider fallbacks for non-headcount rows.
            base2.loc[base2["RESOURCE"].eq(""), "RESOURCE"] = base2.loc[base2["RESOURCE"].eq(""), "SUBCOMPONENT"].astype(str)
            base2.loc[base2["PROVIDER"].eq(""), "PROVIDER"] = base2.loc[base2["PROVIDER"].eq(""), "SUBCOMPONENT"].astype(str)
            base2["RESOURCE"] = base2["RESOURCE"].replace({"": "—"})
            base2["PROVIDER"] = base2["PROVIDER"].replace({"": "(Unassigned)"})

            # -------------------------------------------------------------------
            # DEBUG: Compare Planned Baseline KPI source vs Budget Explorer source
            # -------------------------------------------------------------------
            if debug_budget:
                try:
                    # 1) KPI planned baseline source (class/cost-type breakdown)
                    kpi_parts: List[pd.DataFrame] = []

                    # WF baseline from capacity (split by Team vs Delivery)
                    if group_sel and wf_baseline_cap_by_group_sub is not None and not wf_baseline_cap_by_group_sub.empty:
                        kb = wf_baseline_cap_by_group_sub.copy()
                        kb["YEAR"] = pd.to_numeric(kb.get("YEAR"), errors="coerce").astype("Int64")
                        kb["PI"] = pd.to_numeric(kb.get("PI"), errors="coerce").astype("Int64")
                        kb["PROGRAMNAME"] = kb.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                        kb["TEAMNAME"] = kb.get("TEAMNAME", "").fillna("").astype(str).str.strip()
                        kb["GROUPNAME"] = kb.get("GROUPNAME", "").fillna("").astype(str).str.strip()
                        kb["_SUB_UP"] = kb.get("_SUB_UP", "").fillna("").astype(str).str.strip().str.upper()
                        kb["PLANNED_BASELINE_WF"] = pd.to_numeric(kb.get("PLANNED_BASELINE_WF"), errors="coerce").fillna(0.0)
                        kb = kb[kb["GROUPNAME"].astype(str).str.strip().eq(str(group_sel).strip())].copy()
                        if not kb.empty:
                            kb["CLASS"] = kb["_SUB_UP"].replace({"DELIVERY TEAM": "DELIVERY", "TEAM": "TEAM"})
                            kpi_parts.append(
                                kb[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CLASS"]]
                                .assign(COST_TYPE="WF", PLANNED_BASELINE=kb["PLANNED_BASELINE_WF"])
                            )
                    elif wf_baseline_cap_team_pi is not None and not wf_baseline_cap_team_pi.empty:
                        kb = wf_baseline_cap_team_pi.copy()
                        kb["YEAR"] = pd.to_numeric(kb.get("YEAR"), errors="coerce").astype("Int64")
                        kb["PI"] = pd.to_numeric(kb.get("PI"), errors="coerce").astype("Int64")
                        kb["PROGRAMNAME"] = kb.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                        kb["TEAMNAME"] = kb.get("TEAMNAME", "").fillna("").astype(str).str.strip()
                        kb["WF_BASELINE_TEAM_PI"] = pd.to_numeric(kb.get("WF_BASELINE_TEAM_PI"), errors="coerce").fillna(0.0)
                        kb["WF_BASELINE_DELIVERY_PI"] = pd.to_numeric(kb.get("WF_BASELINE_DELIVERY_PI"), errors="coerce").fillna(0.0)
                        if not kb.empty:
                            kpi_parts.append(
                                kb[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"]]
                                .assign(CLASS="TEAM", COST_TYPE="WF", PLANNED_BASELINE=kb["WF_BASELINE_TEAM_PI"])
                            )
                            kpi_parts.append(
                                kb[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"]]
                                .assign(CLASS="DELIVERY", COST_TYPE="WF", PLANNED_BASELINE=kb["WF_BASELINE_DELIVERY_PI"])
                            )

                    # Program overhead people cost (WF)
                    if _overhead_inputs_all is not None and not _overhead_inputs_all.empty:
                        ovd = _overhead_inputs_all.copy()
                        ovd["YEAR"] = pd.to_numeric(ovd.get("YEAR"), errors="coerce").astype("Int64")
                        ovd["PI"] = pd.to_numeric(ovd.get("PI"), errors="coerce").fillna(0).astype(int)
                        ovd = ovd[ovd["PI"].isin([int(p) for p in pi_nums])].copy() if pi_nums else ovd
                        ovd["PROGRAMNAME"] = ovd.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                        ovd["TEAMNAME"] = "—"
                        ovd["PROGRAMFTE"] = pd.to_numeric(ovd.get("PROGRAMFTE"), errors="coerce").fillna(0.0)
                        ovd["PROGRAM_XOM_RATE"] = pd.to_numeric(ovd.get("PROGRAM_XOM_RATE"), errors="coerce").fillna(0.0)
                        ovd["PLANNED_BASELINE"] = ovd["PROGRAMFTE"] * ovd["PROGRAM_XOM_RATE"]
                        if not ovd.empty:
                            kpi_parts.append(
                                ovd[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"]]
                                .assign(CLASS="PROGRAM", COST_TYPE="WF", PLANNED_BASELINE=ovd["PLANNED_BASELINE"])
                            )

                    # Contractors baseline (C=WF, CS=NWF) – reuse the same dataset used to compute KPI totals.
                    if "contractor_kpi_long" in globals():
                        ckl = contractor_kpi_long if isinstance(contractor_kpi_long, pd.DataFrame) else pd.DataFrame()
                        if ckl is not None and not ckl.empty:
                            ckl = ckl.copy()
                            ckl["PLANNED_BASELINE"] = pd.to_numeric(ckl.get("PLANNED_BASELINE"), errors="coerce").fillna(0.0)
                            kpi_parts.append(
                                ckl[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE", "PLANNED_BASELINE"]].copy()
                            )

                    # WF baseline "other" (non-ADO WF rows)
                    if work_scope is not None and not work_scope.empty:
                        w = work_scope.copy()
                        w["YEAR"] = pd.to_numeric(w.get("YEAR"), errors="coerce").astype("Int64")
                        w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce").astype("Int64")
                        w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                        w["TEAMNAME"] = w.get("TEAMNAME", "").fillna("").astype(str).str.strip()
                        w["SUBCOMPONENT"] = w.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
                        w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)
                        w_src = w.get("SOURCE", "").astype(str).str.upper()
                        w_is_ado = w_src.eq("ADO")
                        w_is_wf = w.get("CAT", pd.Series(dtype=str)).astype(str).eq("WORK_FORCE")
                        w_is_nwf = w.get("CAT", pd.Series(dtype=str)).astype(str).eq("NON_WORK_FORCE")

                        wf_other = (
                            w.loc[~w_is_ado & w_is_wf]
                            .groupby(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "SUBCOMPONENT"], dropna=False)["AMOUNT"]
                            .sum()
                            .reset_index()
                        )
                        if not wf_other.empty:
                            wf_other = wf_other.rename(columns={"SUBCOMPONENT": "CLASS", "AMOUNT": "PLANNED_BASELINE"})
                            wf_other["COST_TYPE"] = "WF"
                            kpi_parts.append(wf_other[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE", "PLANNED_BASELINE"]])

                        nwf = (
                            w.loc[~w_is_ado & w_is_nwf]
                            .groupby(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "SUBCOMPONENT"], dropna=False)["AMOUNT"]
                            .sum()
                            .reset_index()
                        )
                        if not nwf.empty:
                            nwf = nwf.rename(columns={"SUBCOMPONENT": "CLASS", "AMOUNT": "PLANNED_BASELINE"})
                            nwf["COST_TYPE"] = "NWF"
                            kpi_parts.append(nwf[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE", "PLANNED_BASELINE"]])

                    kpi_source_long = pd.concat(kpi_parts, ignore_index=True, sort=False) if kpi_parts else pd.DataFrame()
                    if kpi_source_long is None:
                        kpi_source_long = pd.DataFrame()
                    if not kpi_source_long.empty:
                        kpi_source_long["PLANNED_BASELINE"] = pd.to_numeric(
                            kpi_source_long.get("PLANNED_BASELINE"), errors="coerce"
                        ).fillna(0.0)
                        kpi_debug = (
                            kpi_source_long.groupby(
                                ["YEAR", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE"], dropna=False
                            )["PLANNED_BASELINE"]
                            .sum()
                            .reset_index()
                        )
                    else:
                        kpi_debug = pd.DataFrame(columns=["YEAR", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE", "PLANNED_BASELINE"])
                    kpi_debug_total = float(pd.to_numeric(kpi_debug.get("PLANNED_BASELINE"), errors="coerce").fillna(0.0).sum() or 0.0)

                    # 2) Budget Explorer baseline source (same scope, before pivot)
                    expl = base2.copy()
                    expl["BASELINE_COST"] = pd.to_numeric(expl.get("BASELINE_COST"), errors="coerce").fillna(0.0)
                    expl["COST_TYPE"] = expl.get("COST_TYPE", "").fillna("").astype(str).str.strip()
                    sub_u = expl.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
                    sub_up2 = sub_u.str.upper()
                    expl["CLASS"] = sub_u
                    expl.loc[sub_up2.eq("TEAM"), "CLASS"] = "TEAM"
                    expl.loc[sub_up2.eq("DELIVERY TEAM"), "CLASS"] = "DELIVERY"
                    expl.loc[sub_up2.eq("PROGRAM"), "CLASS"] = "PROGRAM"
                    expl_debug = (
                        expl.groupby(["YEAR", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE"], dropna=False)["BASELINE_COST"]
                        .sum()
                        .reset_index()
                        .rename(columns={"BASELINE_COST": "EXPLORER_BASELINE"})
                    )
                    expl_debug_total = float(pd.to_numeric(expl_debug.get("EXPLORER_BASELINE"), errors="coerce").fillna(0.0).sum() or 0.0)

                    # 3) Summary comparison (by CLASS/COST_TYPE)
                    k_sum = (
                        kpi_debug.groupby(["CLASS", "COST_TYPE"], dropna=False)["PLANNED_BASELINE"]
                        .sum()
                        .reset_index()
                        .rename(columns={"PLANNED_BASELINE": "KPI_PLANNED_BASELINE"})
                    )
                    e_sum = (
                        expl_debug.groupby(["CLASS", "COST_TYPE"], dropna=False)["EXPLORER_BASELINE"]
                        .sum()
                        .reset_index()
                    )
                    comp = k_sum.merge(e_sum, on=["CLASS", "COST_TYPE"], how="outer")
                    comp["KPI_PLANNED_BASELINE"] = pd.to_numeric(comp.get("KPI_PLANNED_BASELINE"), errors="coerce").fillna(0.0)
                    comp["EXPLORER_BASELINE"] = pd.to_numeric(comp.get("EXPLORER_BASELINE"), errors="coerce").fillna(0.0)
                    comp["DELTA (KPI − Explorer)"] = comp["KPI_PLANNED_BASELINE"] - comp["EXPLORER_BASELINE"]
                    comp = comp.sort_values("DELTA (KPI − Explorer)", ascending=False)

                    delta_total = float(kpi_debug_total - expl_debug_total)

                    with _debug_baseline_placeholder.container(border=True):
                        st.subheader("DEBUG – Planned Baseline KPI vs Budget Explorer")
                        st.caption(
                            f"KPI Planned Baseline: {_fmt_money(kpi_debug_total, 0)} • "
                            f"Budget Explorer sum: {_fmt_money(expl_debug_total, 0)} • "
                            f"Delta (KPI − Explorer): {_fmt_money(delta_total, 0)}"
                        )
                        st.markdown("**Summary by Class and Cost Type**")
                        show_comp = comp.copy()
                        if "CLASS" in show_comp.columns:
                            show_comp["CLASS"] = show_comp["CLASS"].map(display_class_label)
                        show_comp["KPI_PLANNED_BASELINE"] = show_comp["KPI_PLANNED_BASELINE"].map(lambda v: _fmt_money(float(v or 0.0), 0))
                        show_comp["EXPLORER_BASELINE"] = show_comp["EXPLORER_BASELINE"].map(lambda v: _fmt_money(float(v or 0.0), 0))
                        show_comp["DELTA (KPI − Explorer)"] = show_comp["DELTA (KPI − Explorer)"].map(lambda v: _fmt_money(float(v or 0.0), 0))
                        st.dataframe(show_comp, use_container_width=True, hide_index=True, height=220)

                        st.markdown("**KPI Planned Baseline source (by Year/Program/Team/Class/Cost Type)**")
                        show_k = kpi_debug.rename(columns={"YEAR": "ADO_YEAR"}).copy()
                        if "CLASS" in show_k.columns:
                            show_k["CLASS"] = show_k["CLASS"].map(display_class_label)
                        show_k["PLANNED_BASELINE"] = show_k["PLANNED_BASELINE"].map(lambda v: _fmt_money(float(v or 0.0), 0))
                        st.dataframe(
                            show_k.sort_values(["ADO_YEAR", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE"], na_position="last"),
                            use_container_width=True,
                            hide_index=True,
                            height=260,
                        )

                        st.markdown("**Budget Explorer baseline source (by Year/Program/Team/Class/Cost Type)**")
                        show_e = expl_debug.rename(columns={"YEAR": "ADO_YEAR"}).copy()
                        if "CLASS" in show_e.columns:
                            show_e["CLASS"] = show_e["CLASS"].map(display_class_label)
                        show_e["EXPLORER_BASELINE"] = show_e["EXPLORER_BASELINE"].map(lambda v: _fmt_money(float(v or 0.0), 0))
                        st.dataframe(
                            show_e.sort_values(["ADO_YEAR", "PROGRAMNAME", "TEAMNAME", "CLASS", "COST_TYPE"], na_position="last"),
                            use_container_width=True,
                            hide_index=True,
                            height=260,
                        )
                except Exception as e:
                    with _debug_baseline_placeholder.container(border=True):
                        st.subheader("DEBUG – Planned Baseline KPI vs Budget Explorer")
                        st.error(f"Debug block failed: {e}")

            # Add FTE visibility for baseline rows (used only for display as FTE per Resource).
            # This uses the same FTE that drives baseline people dollars:
            # - Team baseline rows use `CAPACITY_TEAM_FTE` from `wf_baseline_cap_team_pi`
            # - Delivery baseline rows use `CAPACITY_DELIVERY_FTE`
            # - Contractor / Program overhead rows treat headcount as FTE (1 resource ≈ 1 FTE)
            base2["TOTAL_FTE"] = pd.NA
            try:
                if wf_baseline_cap_team_pi is not None and not wf_baseline_cap_team_pi.empty:
                    ft_raw = wf_baseline_cap_team_pi.copy()
                    ft_raw["YEAR"] = pd.to_numeric(ft_raw.get("YEAR"), errors="coerce").astype("Int64")
                    ft_raw["PI"] = pd.to_numeric(ft_raw.get("PI"), errors="coerce").astype("Int64")
                    ft_raw["PROGRAMNAME"] = (
                        ft_raw.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
                    )
                    ft_raw["TEAMNAME"] = ft_raw.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
                    for c in [
                        "CAPACITY_FTE",
                        "CAPACITY_TEAM_FTE",
                        "CAPACITY_DELIVERY_FTE",
                        "RATE_PER_FTE_PI",
                        "WF_BASELINE_TEAM_PI",
                        "WF_BASELINE_DELIVERY_PI",
                        "WF_BASELINE_TOTAL_PI",
                    ]:
                        if c in ft_raw.columns:
                            ft_raw[c] = pd.to_numeric(ft_raw.get(c), errors="coerce").fillna(0.0)

                    # -----------------------------------------------------------
                    # DEBUG: where does TOTAL_FTE come from for a specific team?
                    # TOTAL_FTE for Budget Explorer rows is sourced from capacity:
                    # - Team rows use CAPACITY_TEAM_FTE
                    # - Delivery Team rows use CAPACITY_DELIVERY_FTE
                    # -----------------------------------------------------------
                    try:
                        wf_cap_df = pd.concat(
                            [
                                ft_raw.assign(
                                    ADO_YEAR=ft_raw["YEAR"],
                                    CLASS="Team",
                                    FTE_VALUE=ft_raw.get("CAPACITY_TEAM_FTE", 0.0),
                                    BASELINE_COST=ft_raw.get("WF_BASELINE_TEAM_PI", 0.0),
                                ),
                                ft_raw.assign(
                                    ADO_YEAR=ft_raw["YEAR"],
                                    CLASS="Delivery Team",
                                    FTE_VALUE=ft_raw.get("CAPACITY_DELIVERY_FTE", 0.0),
                                    BASELINE_COST=ft_raw.get("WF_BASELINE_DELIVERY_PI", 0.0),
                                ),
                            ],
                            ignore_index=True,
                        )
                        wf_cap_df["FTE_VALUE"] = pd.to_numeric(wf_cap_df.get("FTE_VALUE"), errors="coerce").fillna(0.0)
                        wf_cap_df["BASELINE_COST"] = pd.to_numeric(wf_cap_df.get("BASELINE_COST"), errors="coerce").fillna(0.0)
                        wf_cap_df["CLASS"] = wf_cap_df.get("CLASS", "").astype(str).str.strip()
                    except Exception:
                        wf_cap_df = pd.DataFrame()

                    if debug_budget and wf_cap_df is not None and not wf_cap_df.empty:
                        debug_team_cap = wf_cap_df[
                            (wf_cap_df["ADO_YEAR"] == 2025)
                            & (wf_cap_df["PROGRAMNAME"].astype(str) == "R&M")
                            & (wf_cap_df["TEAMNAME"].astype(str) == "ERNE WEST")
                            & (wf_cap_df["CLASS"].astype(str) == "Team")
                        ].copy()
                        st.subheader("DEBUG – WF capacity rows for ERNE WEST Team (2025)")
                        show_cols = [
                            c
                            for c in [
                                "ADO_YEAR",
                                "YEAR",
                                "PI",
                                "PROGRAMNAME",
                                "TEAMNAME",
                                "CLASS",
                                "CAPACITY_FTE",
                                "CAPACITY_TEAM_FTE",
                                "CAPACITY_DELIVERY_FTE",
                                "FTE_VALUE",
                                "RATE_PER_FTE_PI",
                                "WF_BASELINE_TEAM_PI",
                                "WF_BASELINE_DELIVERY_PI",
                                "WF_BASELINE_TOTAL_PI",
                                "BASELINE_COST",
                            ]
                            if c in debug_team_cap.columns
                        ]
                        st.dataframe(debug_team_cap[show_cols], use_container_width=True, hide_index=True)

                    # "fte_agg" equivalent: total FTE per class for the team/year (and per PI).
                    if wf_cap_df is not None and not wf_cap_df.empty:
                        fte_agg = (
                            wf_cap_df.groupby(["ADO_YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CLASS"], dropna=False)["FTE_VALUE"]
                            .sum()
                            .reset_index()
                            .rename(columns={"FTE_VALUE": "TOTAL_FTE"})
                        )
                    else:
                        fte_agg = pd.DataFrame()

                    if debug_budget and fte_agg is not None and not fte_agg.empty:
                        debug_team_fte_agg = fte_agg[
                            (fte_agg["ADO_YEAR"] == 2025)
                            & (fte_agg["PROGRAMNAME"].astype(str) == "R&M")
                            & (fte_agg["TEAMNAME"].astype(str) == "ERNE WEST")
                            & (fte_agg["CLASS"].astype(str) == "Team")
                        ].copy()
                        st.subheader("DEBUG – TOTAL_FTE for ERNE WEST Team (2025)")
                        st.dataframe(
                            debug_team_fte_agg.sort_values(["ADO_YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "CLASS"]),
                            use_container_width=True,
                            hide_index=True,
                        )

                    ft = ft_raw.copy()
                    ft["CAPACITY_TEAM_FTE"] = pd.to_numeric(ft.get("CAPACITY_TEAM_FTE"), errors="coerce").fillna(0.0)
                    ft["CAPACITY_DELIVERY_FTE"] = pd.to_numeric(ft.get("CAPACITY_DELIVERY_FTE"), errors="coerce").fillna(0.0)
                    ft = (
                        ft.groupby(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], dropna=False)[
                            ["CAPACITY_TEAM_FTE", "CAPACITY_DELIVERY_FTE"]
                        ]
                        .sum()
                        .reset_index()
                    )
                    base2 = base2.merge(ft, on=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], how="left")
                    sub_u0 = base2.get("SUBCOMPONENT", "").astype(str).str.upper().str.strip()
                    base2.loc[sub_u0.eq("TEAM"), "TOTAL_FTE"] = base2.loc[sub_u0.eq("TEAM"), "CAPACITY_TEAM_FTE"]
                    base2.loc[sub_u0.eq("DELIVERY TEAM"), "TOTAL_FTE"] = base2.loc[
                        sub_u0.eq("DELIVERY TEAM"), "CAPACITY_DELIVERY_FTE"
                    ]
                    # Prefer explicit headcount-as-FTE when HEADCOUNT is present (matches Teams/Programs editing model).
                    has_hc = pd.to_numeric(base2.get("HEADCOUNT"), errors="coerce").fillna(0.0) > 0
                    base2.loc[has_hc & sub_u0.isin(["TEAM", "DELIVERY TEAM"]), "TOTAL_FTE"] = pd.to_numeric(
                        base2.loc[has_hc & sub_u0.isin(["TEAM", "DELIVERY TEAM"]), "HEADCOUNT"], errors="coerce"
                    ).fillna(base2.loc[has_hc & sub_u0.isin(["TEAM", "DELIVERY TEAM"]), "TOTAL_FTE"])
                    base2 = base2.drop(columns=["CAPACITY_TEAM_FTE", "CAPACITY_DELIVERY_FTE"], errors="ignore")
            except Exception:
                base2["TOTAL_FTE"] = pd.NA

            sub_u0 = base2.get("SUBCOMPONENT", "").astype(str).str.upper().str.strip()
            is_contractor = sub_u0.str.contains("CONTRACTOR", na=False)
            is_overhead = sub_u0.eq("PROGRAM")
            base2.loc[is_contractor | is_overhead, "TOTAL_FTE"] = base2.loc[is_contractor | is_overhead, "HEADCOUNT"]
            base2["TOTAL_FTE"] = pd.to_numeric(base2.get("TOTAL_FTE"), errors="coerce")

            # Attach internal headcount for Team/Delivery rows if not already populated (contractor rows and overhead already carry HEADCOUNT).
            try:
                # Prefer the Teams configuration source-of-truth for headcount:
                # `VW_TEAM_HEADCOUNT_EFFECTIVE` expands PI=0 to all PIs (same as Teams page behavior).
                hc_team_pi_from_teams = pd.DataFrame()
                try:
                    years_i = sorted({int(y) for y in years_sel if y is not None and int(y) > 0})
                    pi_i = sorted({int(p) for p in (pi_nums or []) if p is not None and 1 <= int(p) <= 4})
                    where: List[str] = ["h.CLASS IN ('TEAM','DELIVERY')"]
                    params_h: List[Any] = []
                    if years_i:
                        where.append(f"h.YEAR IN ({', '.join(['%s'] * len(years_i))})")
                        params_h.extend(years_i)
                    if pi_i:
                        where.append(f"h.PI IN ({', '.join(['%s'] * len(pi_i))})")
                        params_h.extend(pi_i)
                    if effective_teams:
                        ph = ", ".join(["%s"] * len(effective_teams))
                        where.append(f"UPPER(t.TEAMNAME) IN ({ph})")
                        params_h.extend([str(t).upper() for t in effective_teams])
                    if effective_programs:
                        ph = ", ".join(["%s"] * len(effective_programs))
                        where.append(f"UPPER(p.PROGRAMNAME) IN ({ph})")
                        params_h.extend([str(pn).upper() for pn in effective_programs])

                    sql_hc = f"""
                      SELECT
                        TRY_CONVERT(INT, h.YEAR) AS YEAR,
                        TRY_CONVERT(INT, h.PI) AS PI,
                        p.PROGRAMNAME,
                        t.TEAMNAME,
                        h.CLASS,
                        SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS HEADCOUNT
                      FROM VW_TEAM_HEADCOUNT_EFFECTIVE h
                      LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
                      LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                      WHERE {" AND ".join(where)}
                      GROUP BY TRY_CONVERT(INT, h.YEAR), TRY_CONVERT(INT, h.PI), p.PROGRAMNAME, t.TEAMNAME, h.CLASS
                    """
                    df_hc = fetch_df(sql_hc, tuple(params_h) if params_h else None)
                    if df_hc is not None and not df_hc.empty:
                        df_hc = df_hc.copy()
                        df_hc["YEAR"] = pd.to_numeric(df_hc.get("YEAR"), errors="coerce").astype("Int64")
                        df_hc["PI"] = pd.to_numeric(df_hc.get("PI"), errors="coerce").astype("Int64")
                        df_hc["PROGRAMNAME"] = (
                            df_hc.get("PROGRAMNAME", "")
                            .fillna("")
                            .astype(str)
                            .str.strip()
                            .replace({"": "(Unassigned)"})
                        )
                        df_hc["TEAMNAME"] = (
                            df_hc.get("TEAMNAME", "")
                            .fillna("")
                            .astype(str)
                            .str.strip()
                            .replace({"": "(Unassigned)"})
                        )
                        df_hc["CLASS"] = df_hc.get("CLASS", "").fillna("").astype(str).str.strip().str.upper()
                        df_hc["HEADCOUNT"] = pd.to_numeric(df_hc.get("HEADCOUNT"), errors="coerce").fillna(0.0)

                        # HOTFIX (temporary): ERNE WEST Team Overhead headcount override to match baseline FTE.
                        # This forces Budget Explorer to expand 3 "Team Overhead #n" resources (not 5) for 2025.
                        try:
                            _mask_hc_hotfix = (
                                (df_hc["YEAR"].astype("Int64") == 2025)
                                & (df_hc.get("PROGRAMNAME", "").astype(str).str.upper().str.strip() == "R&M")
                                & (df_hc.get("TEAMNAME", "").astype(str).str.upper().str.strip() == "ERNE WEST")
                                & (df_hc.get("CLASS", "").astype(str).str.upper().str.strip() == "TEAM")
                                & (df_hc["PI"].notna())
                                & (df_hc["PI"].astype(int).between(1, 4))
                            )
                            if bool(_mask_hc_hotfix.any()):
                                df_hc.loc[_mask_hc_hotfix, "HEADCOUNT"] = 3.0
                        except Exception:
                            pass

                        pivot_hc = (
                            df_hc.pivot_table(
                                index=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"],
                                columns="CLASS",
                                values="HEADCOUNT",
                                aggfunc="sum",
                                fill_value=0.0,
                            )
                            .reset_index()
                        )
                        pivot_hc.columns = [str(c) for c in pivot_hc.columns]
                        pivot_hc["HC_TEAM"] = pd.to_numeric(pivot_hc.get("TEAM"), errors="coerce").fillna(0.0)
                        pivot_hc["HC_DELIVERY"] = pd.to_numeric(pivot_hc.get("DELIVERY"), errors="coerce").fillna(0.0)
                        hc_team_pi_from_teams = pivot_hc[
                            ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "HC_TEAM", "HC_DELIVERY"]
                        ].copy()
                except Exception:
                    hc_team_pi_from_teams = pd.DataFrame()

                hc_team_pi_from_capacity = pd.DataFrame()
                if cap_multi_all is not None and not cap_multi_all.empty:
                    hc = cap_multi_all.copy()
                    hc["YEAR"] = pd.to_numeric(hc.get("ADO_YEAR"), errors="coerce").astype("Int64")
                    hc["PI"] = pd.to_numeric(hc.get("ITERATION_NUM"), errors="coerce").astype("Int64")
                    hc["PROGRAMNAME"] = hc.get("PROGRAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
                    hc["TEAMNAME"] = hc.get("TEAMNAME", "").fillna("").astype(str).str.strip().replace({"": "(Unassigned)"})
                    hc["ALLOCATED_HEADCOUNT"] = pd.to_numeric(hc.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
                    hc["DELIVERY_HEADCOUNT"] = pd.to_numeric(hc.get("DELIVERY_HEADCOUNT"), errors="coerce").fillna(0.0)
                    # True "Team" headcount should exclude Delivery + Contractors (those are tracked separately).
                    hc["CONTRACTOR_C_HEADCOUNT"] = pd.to_numeric(hc.get("CONTRACTOR_C_HEADCOUNT"), errors="coerce").fillna(0.0)
                    hc["CONTRACTOR_CS_HEADCOUNT"] = pd.to_numeric(hc.get("CONTRACTOR_CS_HEADCOUNT"), errors="coerce").fillna(0.0)
                    hc["HC_TEAM"] = (
                        hc["ALLOCATED_HEADCOUNT"]
                        - hc["DELIVERY_HEADCOUNT"]
                        - hc["CONTRACTOR_C_HEADCOUNT"]
                        - hc["CONTRACTOR_CS_HEADCOUNT"]
                    ).clip(lower=0.0)
                    hc["HC_DELIVERY"] = hc["DELIVERY_HEADCOUNT"].clip(lower=0.0)
                    hc_team_pi_from_capacity = (
                        hc.groupby(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], dropna=False)[["HC_TEAM", "HC_DELIVERY"]]
                        .sum()
                        .reset_index()
                    )

                hc_source = "VW_TEAM_HEADCOUNT_EFFECTIVE" if not hc_team_pi_from_teams.empty else "VW_TEAM_ALLOCATED_HEADCOUNT_PI"
                hc_team_pi = hc_team_pi_from_teams if not hc_team_pi_from_teams.empty else hc_team_pi_from_capacity

                if show_debug:
                    with st.expander("Debug · Headcount used by Budget Explorer", expanded=False):
                        st.caption(
                            "Budget Explorer uses this headcount only to render Team/Delivery resource lines (e.g., 'Team Overhead #1'). "
                            f"Source: {hc_source if hc_team_pi is not None and not hc_team_pi.empty else '(none)'}."
                        )
                        show_cols = ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "HC_TEAM", "HC_DELIVERY"]
                        if hc_team_pi is None or hc_team_pi.empty:
                            st.dataframe(pd.DataFrame(columns=show_cols), use_container_width=True, hide_index=True, height=220)
                        else:
                            show_hc = hc_team_pi.copy()
                            for c in ["YEAR", "PI"]:
                                show_hc[c] = pd.to_numeric(show_hc.get(c), errors="coerce").astype("Int64")
                            show_hc["PROGRAMNAME"] = show_hc.get("PROGRAMNAME", "").astype(str).str.strip()
                            show_hc["TEAMNAME"] = show_hc.get("TEAMNAME", "").astype(str).str.strip()
                            for c in ["HC_TEAM", "HC_DELIVERY"]:
                                show_hc[c] = pd.to_numeric(show_hc.get(c), errors="coerce").fillna(0.0)
                            st.dataframe(
                                show_hc[show_cols].sort_values(["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], na_position="last"),
                                use_container_width=True,
                                hide_index=True,
                                height=220,
                            )
                if hc_team_pi is not None and not hc_team_pi.empty:
                    base2["_SUB_UP"] = base2["SUBCOMPONENT"].astype(str).str.upper()
                    base2 = base2.merge(hc_team_pi, on=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME"], how="left")
                    has_team = base2.get("HC_TEAM").notna()
                    has_delivery = base2.get("HC_DELIVERY").notna()
                    base2["HC_TEAM"] = pd.to_numeric(base2.get("HC_TEAM"), errors="coerce").fillna(0.0)
                    base2["HC_DELIVERY"] = pd.to_numeric(base2.get("HC_DELIVERY"), errors="coerce").fillna(0.0)

                    # Override Team/Delivery headcount only when the join produced a row.
                    # If the join has no data (both sources empty), leave existing HEADCOUNT/TOTAL_FTE untouched.
                    base2.loc[has_team & base2["_SUB_UP"].eq("TEAM"), "HEADCOUNT"] = base2.loc[
                        has_team & base2["_SUB_UP"].eq("TEAM"), "HC_TEAM"
                    ]
                    base2.loc[has_delivery & base2["_SUB_UP"].eq("DELIVERY TEAM"), "HEADCOUNT"] = base2.loc[
                        has_delivery & base2["_SUB_UP"].eq("DELIVERY TEAM"), "HC_DELIVERY"
                    ]
                else:
                    base2["_SUB_UP"] = base2["SUBCOMPONENT"].astype(str).str.upper()
            except Exception:
                base2["_SUB_UP"] = base2.get("SUBCOMPONENT", "").astype(str).str.upper()

            # Expand headcount rows into line-by-line "Resource" rows.
            def _expand_resources(total: float, *, prefix: str) -> List[dict]:
                t = float(total or 0.0)
                if t <= 0:
                    return [{"RESOURCE": "—", "SHARE": 1.0}]
                n = int(math.floor(t + 1e-9))
                rem = float(t - n)
                out: List[dict] = []
                if n <= 0:
                    return [{"RESOURCE": f"{prefix} #1 (fraction)", "SHARE": 1.0}]
                for i in range(1, n + 1):
                    out.append({"RESOURCE": f"{prefix} #{i}", "SHARE": 1.0 / t})
                if rem > 1e-6:
                    out.append({"RESOURCE": f"{prefix} #{n + 1} (fraction)", "SHARE": rem / t})
                return out

            def _resource_list_for_row(r) -> List[dict]:
                hc = float(pd.to_numeric(r.get("HEADCOUNT"), errors="coerce") or 0.0)
                cost = float(pd.to_numeric(r.get("BASELINE_COST"), errors="coerce") or 0.0)
                if hc <= 0 or cost == 0:
                    return [{"RESOURCE": str(r.get("RESOURCE") or "—"), "SHARE": 1.0}]

                sub_up = str(r.get("_SUB_UP") or "").strip().upper()
                sub = str(r.get("SUBCOMPONENT") or "").strip()
                provider = str(r.get("PROVIDER") or "").strip()
                if "CONTRACTOR" in sub_up:
                    prefix = f"{provider} {sub}".strip()
                elif sub_up == "DELIVERY TEAM":
                    prefix = "Delivery"
                elif sub_up == "TEAM":
                    prefix = display_class_label("TEAM")
                elif sub_up == "PROGRAM":
                    prefix = str(r.get("RESOURCE") or "").strip() or "Overhead"
                else:
                    prefix = sub or "Resource"
                return _expand_resources(hc, prefix=prefix)

            base2["_RES_LIST"] = base2.apply(_resource_list_for_row, axis=1)
            base2["_RES_LIST"] = base2["_RES_LIST"].map(lambda v: v if isinstance(v, list) else [{"RESOURCE": "—", "SHARE": 1.0}])

            expanded = base2.explode("_RES_LIST").copy()
            expanded["RESOURCE"] = expanded["_RES_LIST"].map(lambda d: d.get("RESOURCE", "—") if isinstance(d, dict) else "—")
            expanded["_SHARE"] = pd.to_numeric(
                expanded["_RES_LIST"].map(lambda d: d.get("SHARE", 1.0) if isinstance(d, dict) else 1.0),
                errors="coerce",
            ).fillna(1.0)
            expanded["FTE_PER_RESOURCE_PI"] = (
                pd.to_numeric(expanded.get("TOTAL_FTE"), errors="coerce").fillna(0.0) * expanded["_SHARE"]
            )
            expanded.loc[pd.to_numeric(expanded.get("TOTAL_FTE"), errors="coerce").fillna(0.0) <= 0, "FTE_PER_RESOURCE_PI"] = pd.NA
            expanded["BASELINE_COST"] = pd.to_numeric(expanded.get("BASELINE_COST"), errors="coerce").fillna(0.0) * expanded["_SHARE"]
            expanded = expanded.drop(columns=["_RES_LIST", "_SHARE"], errors="ignore")

            agg = (
                expanded.groupby(
                    ["YEAR", "PROGRAMNAME", "TEAMNAME", "RESOURCE", "COST_TYPE", "PROVIDER", "PI_LABEL"],
                    dropna=False,
                )["BASELINE_COST"]
                .sum()
                .reset_index()
            )

            idx_cols = ["YEAR", "PROGRAMNAME", "TEAMNAME", "RESOURCE", "COST_TYPE", "PROVIDER"]
            fte_meta = (
                expanded.groupby(idx_cols, dropna=False)["FTE_PER_RESOURCE_PI"]
                .max()
                .reset_index()
                .rename(columns={"FTE_PER_RESOURCE_PI": "FTE_PER_RESOURCE"})
            )
            pivot = (
                agg.pivot_table(
                    index=idx_cols,
                    columns="PI_LABEL",
                    values="BASELINE_COST",
                    aggfunc="sum",
                    fill_value=0.0,
                )
                .reset_index()
            )
            pivot = pivot.merge(fte_meta, on=idx_cols, how="left")

            pi_cols = [c for c in pivot.columns if c not in idx_cols and c != "FTE_PER_RESOURCE"]
            pivot["TOTAL_BASELINE"] = pivot[pi_cols].sum(axis=1) if pi_cols else 0.0
            pivot["WF_COST_TOTAL"] = pivot.apply(lambda r: float(r["TOTAL_BASELINE"]) if str(r.get("COST_TYPE")).upper() == "WF" else 0.0, axis=1)
            pivot["NWF_COST_TOTAL"] = pivot.apply(lambda r: float(r["TOTAL_BASELINE"]) if str(r.get("COST_TYPE")).upper() == "NWF" else 0.0, axis=1)

            # Budget Explorer uses the scope selected in the global filter bar (avoid duplicate selectors here).
            prog_filter = [str(x) for x in (effective_programs or []) if str(x).strip()]
            team_filter = [str(x) for x in (effective_teams or []) if str(x).strip()]

            with st.expander("Advanced filters", expanded=False):
                f3, f4, f5 = st.columns([1.6, 1.6, 1.0])
                with f3:
                    resource_filter = st.multiselect(
                        "Resource",
                        options=sorted(pivot["RESOURCE"].dropna().astype(str).unique().tolist()),
                        default=[],
                        key="budget_baseline_explorer_resource",
                    )
                with f4:
                    prov_filter = st.multiselect(
                        "Providers",
                        options=sorted(pivot["PROVIDER"].dropna().astype(str).unique().tolist()),
                        default=[],
                        key="budget_baseline_explorer_provider",
                    )
                with f5:
                    wf_only = st.checkbox("WF only", value=False, key="budget_baseline_explorer_wf_only")

            render_active_filters_summary(
                "Active explorer filters",
                [
                    ("Programs", prog_filter),
                    ("Teams", team_filter),
                    ("Resource", resource_filter),
                    ("Providers", prov_filter),
                    ("WF only", "Yes" if wf_only else None),
                ],
            )

            explorer_df = pivot.copy()
            def _norm_key(v: Any) -> str:
                return str(v or "").strip().upper()

            program_allow: set[str] = set()
            for p in (effective_programs or []):
                raw = str(p or "").strip()
                if not raw:
                    continue
                program_allow.add(_norm_key(raw))
                disp = _program_display_name(raw, program_disp_map)
                if disp:
                    program_allow.add(_norm_key(disp))
                for c in program_canon_map.get(_norm_key(raw), []):
                    program_allow.add(_norm_key(c))
                    c_disp = _program_display_name(c, program_disp_map)
                    if c_disp:
                        program_allow.add(_norm_key(c_disp))

            team_allow: set[str] = {_norm_key(t) for t in (effective_teams or []) if str(t or "").strip()}

            if prog_filter:
                explorer_df = explorer_df[
                    explorer_df["PROGRAMNAME"].astype(str).map(_norm_key).isin(program_allow)
                ].copy()
            if team_filter:
                explorer_df = explorer_df[
                    explorer_df["TEAMNAME"].astype(str).map(_norm_key).isin(team_allow)
                ].copy()
            if resource_filter:
                explorer_df = explorer_df[explorer_df["RESOURCE"].astype(str).isin([str(x) for x in resource_filter])].copy()
            if prov_filter:
                explorer_df = explorer_df[explorer_df["PROVIDER"].astype(str).isin([str(x) for x in prov_filter])].copy()
            if wf_only:
                explorer_df = explorer_df[explorer_df["COST_TYPE"].astype(str).str.upper().eq("WF")].copy()

            # Hide empty unassigned WF scaffolding rows (no headcount / no dollars).
            if not explorer_df.empty:
                _fte = pd.to_numeric(explorer_df.get("FTE_PER_RESOURCE"), errors="coerce").fillna(0.0)
                _tot = pd.to_numeric(explorer_df.get("TOTAL_BASELINE"), errors="coerce").fillna(0.0)
                _res = explorer_df.get("RESOURCE", "").astype(str).str.upper().str.strip()
                _prog = explorer_df.get("PROGRAMNAME", "").astype(str).str.strip()
                _team = explorer_df.get("TEAMNAME", "").astype(str).str.strip()
                _wf = explorer_df.get("COST_TYPE", "").astype(str).str.upper().str.strip().eq("WF")
                noise_mask = (
                    _wf
                    & _prog.eq("(Unassigned)")
                    & _team.eq("(Unassigned)")
                    & _res.isin({"TEAM", "DELIVERY TEAM"})
                    & _tot.abs().le(1e-9)
                    & _fte.abs().le(1e-9)
                )
                explorer_df = explorer_df.loc[~noise_mask].copy()

            total_baseline = float(pd.to_numeric(explorer_df.get("TOTAL_BASELINE"), errors="coerce").fillna(0.0).sum() or 0.0)
            st.caption(f"Showing {len(explorer_df):,} rows • Baseline: {_fmt_money(total_baseline, 0)}")
            if (not resource_filter) and (not prov_filter) and (not wf_only):
                delta_recon = float(total_baseline - baseline_total_financial)
                if abs(delta_recon) > 1.0:
                    st.warning(
                        "Reconciliation warning: Budget Explorer total does not match canonical Baseline KPI "
                        f"(Δ {_fmt_money(delta_recon, 0)})."
                    )

            show = explorer_df.copy()
            if "PROGRAMNAME" in show.columns:
                show["PROGRAMNAME"] = show["PROGRAMNAME"].map(lambda v: _program_display_name(v, program_disp_map))
            if "FTE_PER_RESOURCE" in show.columns:
                show["FTE_PER_RESOURCE"] = pd.to_numeric(show.get("FTE_PER_RESOURCE"), errors="coerce")
            for c in ["WF_COST_TOTAL", "NWF_COST_TOTAL", "TOTAL_BASELINE"]:
                if c in show.columns:
                    show[c] = show[c].map(lambda v: _fmt_money(float(v or 0.0), 0))
            if "FTE_PER_RESOURCE" in show.columns:
                show["FTE_PER_RESOURCE"] = show["FTE_PER_RESOURCE"].map(
                    lambda v: _fmt_num(float(v), 2) if v is not None and not pd.isna(v) else "—"
                )
            for c in pi_cols:
                if c in show.columns:
                    show[c] = show[c].map(lambda v: _fmt_money(float(v or 0.0), 0))

            show_cols = (
                ["YEAR", "PROGRAMNAME", "TEAMNAME", "RESOURCE", "COST_TYPE", "PROVIDER", "FTE_PER_RESOURCE", "WF_COST_TOTAL", "NWF_COST_TOTAL"]
                + [c for c in pi_cols if c in show.columns]
                + [c for c in ["TOTAL_BASELINE"] if c in show.columns]
            )
            show_cols = [c for c in show_cols if c in show.columns]
            show = show[show_cols].copy()
            show = show.sort_values(["YEAR", "PROGRAMNAME", "TEAMNAME", "RESOURCE", "COST_TYPE", "PROVIDER"], ascending=True)
            st.dataframe(show, use_container_width=True, hide_index=True, height=520)

            try:
                import io

                buf = io.BytesIO()
                explorer_df.to_excel(buf, index=False)
                buf.seek(0)
                st.download_button(
                    "Download baseline as Excel",
                    data=buf,
                    file_name="budget_explorer_baseline.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="budget_baseline_explorer_download",
                )
            except Exception:
                st.info("Download is not available in this environment.")
if False:  # Tab removed: Cost Explorer (kept as dead code to avoid risky refactors)
    st.subheader("Cost Explorer")
    st.caption("- Baseline = staffing plan (not planned by app group).")
    st.caption("- Expected = demand allocation from ADO.")
    st.caption("- Actual = Apptio NWF at program level (can be shown program-only).")

    auth_user = st.session_state.get("auth_user") or {}
    is_admin = str(auth_user.get("role", "") or "").strip().upper() == "ADMIN"

    baseline_scope = df_baseline_scope.copy() if df_baseline_scope is not None else pd.DataFrame()
    expected_scope = df_expected_scope.copy() if df_expected_scope is not None else pd.DataFrame()

    show_actual_override = st.checkbox(
        "Show Actuals even when Team/App filters are selected (program-level)",
        value=True,
        key="budget_cost_explorer_actual_override",
        help="Actuals are program-level (Apptio). Team/App filters do not apply.",
    )

    if (teams_sel or group_sel) and show_actual_override:
        st.info("Actuals are program-level (Apptio). Team/App filters do not apply.")

    def _safe_concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
        parts = [f for f in frames if f is not None and not f.empty]
        return pd.concat(parts, ignore_index=True, sort=False) if parts else pd.DataFrame()

    def _norm_cost_lines(df_in: pd.DataFrame, scenario_label: str) -> pd.DataFrame:
        base_cols = [
            "SCENARIO",
            "YEAR",
            "PI",
            "ITERATION_LEVEL3",
            "PROGRAMNAME",
            "TEAMNAME",
            "GROUPNAME",
            "CAT",
            "SUBCOMPONENT",
            "SOURCE",
            "COST_TYPE",
            "AMOUNT",
            "FEATURE_ID",
            "FEATURE_TITLE",
        ]
        if df_in is None or df_in.empty:
            return pd.DataFrame(columns=base_cols)
        w = df_in.copy()
        w["SCENARIO"] = str(scenario_label).upper()

        def _series(col: str, default) -> pd.Series:
            if col in w.columns:
                s = w[col]
                return s if isinstance(s, pd.Series) else pd.Series([s] * len(w.index), index=w.index)
            return pd.Series([default] * len(w.index), index=w.index)

        w["YEAR"] = pd.to_numeric(_series("YEAR", pd.NA), errors="coerce").astype("Int64")
        w["PI"] = pd.to_numeric(_series("PI", pd.NA), errors="coerce").astype("Int64")
        w["PI"] = w["PI"].fillna(0).astype("Int64")

        w["PROGRAMNAME"] = _series("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        w["TEAMNAME"] = _series("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        w["GROUPNAME"] = _series("GROUPNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        w["SOURCE"] = _series("SOURCE", "").astype(str).str.strip().replace({"": "(Unknown)"})
        w["SUBCOMPONENT"] = _series("SUBCOMPONENT", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        w["COST_TYPE"] = _series("COST_TYPE", "").astype(str).str.strip()

        if "CAT" not in w.columns:
            cc = _series("COST_CATEGORY", "").astype(str).str.upper().str.strip()
            w["CAT"] = cc.replace({"WORKFORCE": "WORK_FORCE", "WF": "WORK_FORCE", "NWF": "NON_WORK_FORCE"})
        w["CAT"] = _series("CAT", "").astype(str).str.upper().str.strip()
        w.loc[w["CAT"].eq(""), "CAT"] = "(Unassigned)"

        w["AMOUNT"] = pd.to_numeric(_series("AMOUNT", 0.0), errors="coerce").fillna(0.0).astype(float)

        # Feature fields (only real if present in cost lines).
        w["FEATURE_ID"] = w.get("FEATURE_ID", pd.Series([pd.NA] * len(w.index))).copy()
        ft_col = "FEATURE_TITLE" if "FEATURE_TITLE" in w.columns else ("TITLE" if "TITLE" in w.columns else None)
        w["FEATURE_TITLE"] = (
            w.get(ft_col, pd.Series([pd.NA] * len(w.index))).copy() if ft_col else pd.Series([pd.NA] * len(w.index))
        )

        y = w["YEAR"].astype("Int64")
        p = w["PI"].astype("Int64")
        w["ITERATION_LEVEL3"] = (y.astype(str) + " I" + p.astype(str)).where(
            (y.notna()) & (p.notna()) & (p > 0), y.astype(str) + " FY"
        )
        w.loc[w["ITERATION_LEVEL3"].astype(str).str.strip().eq(""), "ITERATION_LEVEL3"] = "(Unknown)"

        for c in base_cols:
            if c not in w.columns:
                w[c] = pd.NA
        return w[base_cols + [c for c in w.columns if c not in base_cols]].copy()

    baseline_norm = _norm_cost_lines(baseline_scope, "BASELINE")
    expected_norm = _norm_cost_lines(expected_scope, "EXPECTED")

    actual_norm = pd.DataFrame()
    if show_actual_override or ((not teams_sel) and (not group_sel)):
        scope_for_actual = {"programs": list(effective_programs or []), "teams": [], "groups": []}
        actual_frames = []
        for y in [int(x) for x in years_sel]:
            try:
                cm_y = load_cost_model(int(y), scope_for_actual)
                df_a = cm_y.get("ACTUAL", pd.DataFrame())
                if df_a is not None and not df_a.empty:
                    actual_frames.append(df_a)
            except Exception:
                continue
        actual_norm = _norm_cost_lines(_safe_concat(actual_frames), "ACTUAL")

    df_all = _safe_concat([baseline_norm, expected_norm, actual_norm])
    if df_all.empty:
        st.info("No cost lines available for the selected filters.")
    else:
        # Enforce Budget time filters (years + PI selection) for exploration consistency.
        if years_sel:
            df_all = df_all.loc[pd.to_numeric(df_all.get("YEAR"), errors="coerce").isin([int(y) for y in years_sel])].copy()
        if pi_nums:
            df_all = df_all.loc[pd.to_numeric(df_all.get("PI"), errors="coerce").isin([int(p) for p in pi_nums])].copy()

        # Controls
        ctrl = st.columns([1.3, 1.1, 1.1, 1.2], vertical_alignment="center")
        with ctrl[0]:
            scen_options = ["BASELINE", "EXPECTED"] + (["ACTUAL"] if (actual_norm is not None and not actual_norm.empty) else ["ACTUAL"])
            scen_default = ["BASELINE", "EXPECTED"] + (["ACTUAL"] if (actual_norm is not None and not actual_norm.empty) else [])
            scen_sel = st.multiselect("Scenarios", ["BASELINE", "EXPECTED", "ACTUAL"], default=scen_default, key="budget_cost_explorer_scenarios")
        with ctrl[1]:
            measure = st.selectbox("Measure", ["Amount ($)", "FTE"], index=0, key="budget_cost_explorer_measure")
        with ctrl[2]:
            sod_only = st.checkbox("SoD only", value=False, key="budget_cost_explorer_sod_only", help="Delivery + Contractors only (Team/Program overhead excluded).")
        with ctrl[3]:
            cat_sel = st.multiselect(
                "Cost category",
                ["WORK_FORCE", "NON_WORK_FORCE"],
                default=["WORK_FORCE", "NON_WORK_FORCE"],
                key="budget_cost_explorer_cat",
            )

        timegrain = st.radio("Timegrain", ["PI", "Year"], index=0, horizontal=True, key="budget_cost_explorer_timegrain")
        pivot_col = st.selectbox(
            "Columns (pivot)",
            ["(none)", "SCENARIO", "CAT", "SUBCOMPONENT"],
            index=1,
            key="budget_cost_explorer_pivot",
        )

        search = st.text_input("Search", value="", key="budget_cost_explorer_search", placeholder="Program / Team / Application / Subcomponent / Feature title")

        rows_default = ["ITERATION_LEVEL3", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "CAT", "SUBCOMPONENT"]
        if timegrain == "Year":
            rows_default = ["YEAR", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "CAT", "SUBCOMPONENT"]
        rows_key = "budget_cost_explorer_rows"
        if rows_key not in st.session_state:
            st.session_state[rows_key] = rows_default

        has_feature_cost = False
        try:
            has_feature_cost = (
                "FEATURE_ID" in df_all.columns
                and df_all.loc[df_all["SCENARIO"].astype(str).str.upper().eq("EXPECTED"), "FEATURE_ID"].notna().any()
            )
        except Exception:
            has_feature_cost = False

        row_options = [
            "YEAR",
            "PI",
            "ITERATION_LEVEL3",
            "PROGRAMNAME",
            "TEAMNAME",
            "GROUPNAME",
            "CAT",
            "SUBCOMPONENT",
            "SOURCE",
            "COST_TYPE",
        ] + (["FEATURE_ID", "FEATURE_TITLE"] if has_feature_cost else [])

        rows_sel = st.multiselect(
            "Rows (group by)",
            row_options,
            default=[c for c in st.session_state.get(rows_key, rows_default) if c in row_options],
            key=rows_key,
        )

        sort_mode = st.selectbox("Sort", ["Amount desc", "Amount asc", "Name"], index=0, key="budget_cost_explorer_sort")
        top_n = st.slider("Top N", min_value=50, max_value=5000, value=200, step=50, key="budget_cost_explorer_topn")

        # Filters
        work = df_all.copy()
        if scen_sel:
            work = work.loc[work["SCENARIO"].astype(str).str.upper().isin([str(s).upper() for s in scen_sel])].copy()
        if cat_sel:
            work = work.loc[work["CAT"].astype(str).str.upper().isin([str(c).upper() for c in cat_sel])].copy()
        if sod_only:
            sub = work.get("SUBCOMPONENT", "").astype(str).str.upper()
            work = work.loc[sub.str.contains("DELIVERY", na=False) | sub.str.contains("CONTRACTOR", na=False)].copy()
        if search and str(search).strip():
            q = str(search).strip()
            masks = []
            for c in ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "SUBCOMPONENT", "FEATURE_TITLE"]:
                if c in work.columns:
                    masks.append(work[c].fillna("").astype(str).str.contains(q, case=False, na=False))
            if masks:
                m = masks[0]
                for mm in masks[1:]:
                    m = m | mm
                work = work.loc[m].copy()

        # Guardrail: if SWAG demand is not ready for a feature (FTE=0), that feature must contribute $0 expected cost.
        if has_feature_cost and "FTE" in work.columns:
            try:
                scen = work.get("SCENARIO", "").astype(str).str.upper()
                src = work.get("SOURCE", "").astype(str).str.upper()
                cat = work.get("CAT", pd.Series("", index=work.index, dtype="object")).astype(str).str.upper()
                fte_used = pd.to_numeric(work.get("FTE"), errors="coerce").fillna(0.0)
                amt = pd.to_numeric(work.get("AMOUNT"), errors="coerce").fillna(0.0)
                viol = (
                    scen.eq("EXPECTED")
                    & src.eq("ADO")
                    & cat.eq("WORK_FORCE")
                    & work.get("FEATURE_ID").notna()
                    & (fte_used <= 0)
                    & (amt.abs() > 1e-9)
                )
                if bool(viol.any()):
                    st.error(
                        f"Guardrail violation: {int(viol.sum()):,} EXPECTED ADO feature-cost row(s) have zero demand FTE but non-zero amount. "
                        "This indicates SWAG is not ready or the DB views are out of sync."
                    )
                    if is_admin:
                        show_cols = [c for c in ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "FEATURE_ID", "FEATURE_TITLE", "FTE", "AMOUNT"] if c in work.columns]
                        st.dataframe(work.loc[viol, show_cols].head(200), use_container_width=True, hide_index=True, height=280)
            except Exception:
                pass

        # Optional: enrich feature titles (only when FEATURE_ID exists in cost lines).
        if has_feature_cost:
            title_map_key = "budget_cost_explorer_feature_title_map"
            if st.button("Enrich feature titles (ADO)", key="budget_cost_explorer_enrich_titles"):
                try:
                    feats = load_explorer_feature_rows(
                        years=[int(y) for y in years_sel],
                        programs=tuple(effective_programs),
                        teams=tuple(effective_teams),
                        groups=tuple([str(group_sel)] if group_sel else ()),
                        pi_nums=tuple(pi_nums),
                    )
                    feats = feats if feats is not None else pd.DataFrame()
                    if feats is not None and not feats.empty and "FEATURE_ID" in feats.columns:
                        feats["FEATURE_ID"] = feats["FEATURE_ID"].astype(str)
                        tcol = "FEATURE_TITLE" if "FEATURE_TITLE" in feats.columns else ("TITLE" if "TITLE" in feats.columns else None)
                        if tcol:
                            feats[tcol] = feats[tcol].fillna("").astype(str).str.strip()
                            m = feats.loc[feats[tcol].ne(""), ["FEATURE_ID", tcol]].drop_duplicates()
                            st.session_state[title_map_key] = dict(zip(m["FEATURE_ID"].tolist(), m[tcol].tolist()))
                except Exception:
                    st.warning("Unable to enrich feature titles.")

            title_map = st.session_state.get(title_map_key, None)
            if isinstance(title_map, dict) and title_map:
                fid = work.get("FEATURE_ID")
                if fid is not None:
                    fid_str = fid.astype(str)
                    ft = work.get("FEATURE_TITLE", pd.Series([pd.NA] * len(work.index)))
                    ft_str = ft.fillna("").astype(str).str.strip()
                    missing = ft_str.eq("") & fid.notna()
                if missing.any():
                    work.loc[missing, "FEATURE_TITLE"] = fid_str.loc[missing].map(title_map)

        if show_debug:
            with st.expander("Debug – Explorer v2 consistency (Admin)", expanded=False):
                st.caption(
                    "Compares Derived FTE (SWAG demand driver) and SWAG readiness between Explorer v2 feature rows "
                    "and Cost Explorer EXPECTED cost lines for the current scope."
                )
                fid_in = st.text_input("Feature ID", value="1713064", key="budget_cost_explorer_dbg_feature_id").strip()
                if fid_in:
                    fid_norm = str(fid_in).strip()
                    try:
                        fid_norm = str(int(float(fid_norm)))
                    except Exception:
                        pass

                    feats_v2 = load_explorer_feature_rows(
                        years=[int(y) for y in years_sel],
                        programs=tuple(effective_programs),
                        teams=tuple(effective_teams),
                        groups=tuple([str(group_sel)] if group_sel else ()),
                        pi_nums=tuple(pi_nums),
                    )
                    feats_v2 = feats_v2 if isinstance(feats_v2, pd.DataFrame) else pd.DataFrame()
                    if not feats_v2.empty and "FEATURE_ID" in feats_v2.columns:
                        feats_v2 = feats_v2.copy()
                        feats_v2["FEATURE_ID"] = feats_v2["FEATURE_ID"].astype(str).str.strip()
                        feats_v2 = feats_v2.loc[feats_v2["FEATURE_ID"] == fid_norm].copy()
                    else:
                        feats_v2 = pd.DataFrame()

                    cost_exp = expected_norm.copy() if isinstance(expected_norm, pd.DataFrame) else pd.DataFrame()
                    if not cost_exp.empty and "FEATURE_ID" in cost_exp.columns:
                        cost_exp = cost_exp.copy()
                        cost_exp["FEATURE_ID"] = cost_exp["FEATURE_ID"].astype(str).str.strip()
                        cost_exp = cost_exp.loc[cost_exp["FEATURE_ID"] == fid_norm].copy()
                    else:
                        cost_exp = pd.DataFrame()

                    if feats_v2.empty and cost_exp.empty:
                        st.info("No matching rows found in either dataset for this Feature ID in the current scope.")
                    else:
                        # Explorer v2 side
                        if not feats_v2.empty:
                            feats_v2["ADO_YEAR"] = pd.to_numeric(feats_v2.get("ADO_YEAR"), errors="coerce").astype("Int64")
                            feats_v2["PI_NUM"] = pd.to_numeric(feats_v2.get("PI_NUM"), errors="coerce").astype("Int64")
                            feats_v2["DERIVED_FTE_EXPLORER"] = pd.to_numeric(feats_v2.get("DERIVED_FTE_EXPLORER"), errors="coerce").fillna(0.0)
                            feats_v2["SWAG_READY"] = feats_v2.get("SWAG_READY", False).fillna(False).astype(bool)

                        # Cost Explorer side (EXPECTED cost lines)
                        if not cost_exp.empty:
                            cost_exp["YEAR"] = pd.to_numeric(cost_exp.get("YEAR"), errors="coerce").astype("Int64")
                            cost_exp["PI"] = pd.to_numeric(cost_exp.get("PI"), errors="coerce").astype("Int64")
                            for c in ["FTE", "AMOUNT"]:
                                if c not in cost_exp.columns:
                                    cost_exp[c] = 0.0
                                cost_exp[c] = pd.to_numeric(cost_exp.get(c), errors="coerce").fillna(0.0)
                            cost_agg = (
                                cost_exp.groupby(["YEAR", "PI"], dropna=False)
                                .agg(
                                    FTE_COST=("FTE", "sum"),
                                    AMOUNT_COST=("AMOUNT", "sum"),
                                )
                                .reset_index()
                            )
                        else:
                            cost_agg = pd.DataFrame(columns=["YEAR", "PI", "FTE_COST", "AMOUNT_COST"])

                        if feats_v2.empty:
                            st.warning("Explorer v2: no matching feature rows (check PI mapping or scope filters).")
                        else:
                            show_cols = [
                                c
                                for c in [
                                    "FEATURE_ID",
                                    "FEATURE_TITLE",
                                    "ADO_YEAR",
                                    "PI_NUM",
                                    "PI_LABEL",
                                    "DERIVED_FTE_EXPLORER",
                                    "SWAG_READY",
                                    "PROGRAMNAME",
                                    "TEAMNAME",
                                    "GROUPNAME",
                                    "MAPPING_STATUS",
                                ]
                                if c in feats_v2.columns
                            ]
                            st.markdown("**Explorer v2 rows**")
                            st.dataframe(feats_v2[show_cols].sort_values(["ADO_YEAR", "PI_NUM"]), use_container_width=True, hide_index=True, height=220)

                        if cost_agg.empty:
                            st.warning("Cost Explorer: no matching EXPECTED cost lines with this Feature ID.")
                        else:
                            st.markdown("**Cost Explorer EXPECTED totals (by Year×PI)**")
                            st.dataframe(cost_agg.sort_values(["YEAR", "PI"]), use_container_width=True, hide_index=True, height=220)

                        if (not feats_v2.empty) and (not cost_agg.empty):
                            left = feats_v2.copy()
                            left = left.rename(columns={"ADO_YEAR": "YEAR", "PI_NUM": "PI"})
                            left = left.groupby(["YEAR", "PI"], dropna=False).agg(
                                DERIVED_FTE_V2=("DERIVED_FTE_EXPLORER", "sum"),
                                SWAG_READY_V2=("SWAG_READY", "max"),
                            ).reset_index()
                            merged = left.merge(cost_agg, on=["YEAR", "PI"], how="outer")
                            for c in ["DERIVED_FTE_V2", "FTE_COST", "AMOUNT_COST"]:
                                if c in merged.columns:
                                    merged[c] = pd.to_numeric(merged.get(c), errors="coerce").fillna(0.0)
                            merged["SWAG_READY_V2"] = merged.get("SWAG_READY_V2", False).fillna(False).astype(bool)
                            merged["FTE_MATCH"] = (merged["DERIVED_FTE_V2"] - merged["FTE_COST"]).abs() < 1e-6
                            merged["GUARDRAIL_OK"] = ((~merged["SWAG_READY_V2"]) & (merged["AMOUNT_COST"].abs() < 1e-6)) | merged["SWAG_READY_V2"]
                            st.markdown("**Comparison**")
                            st.dataframe(
                                merged.sort_values(["YEAR", "PI"]),
                                use_container_width=True,
                                hide_index=True,
                                height=240,
                            )

        if "GROUPNAME" in rows_sel and ("BASELINE" in [str(x).upper() for x in scen_sel]):
            st.info("Baseline is not planned at app-group level; treat Baseline vs Expected deltas here as allocation contrast, not budget variance.")

        # Feature support (only when real)
        if not has_feature_cost:
            with st.expander("ADO Feature Demand (not cost lines)", expanded=False):
                if st.button("Load Feature Rows (ADO Explorer)", key="budget_cost_explorer_load_features"):
                    try:
                        feat = load_explorer_feature_rows(
                            years=[int(y) for y in years_sel],
                            programs=tuple(effective_programs),
                            teams=tuple(effective_teams),
                            groups=tuple([str(group_sel)] if group_sel else ()),
                            pi_nums=tuple(pi_nums),
                        )
                        feat = feat if feat is not None else pd.DataFrame()
                        if feat.empty:
                            st.info("No ADO feature demand rows found for this scope.")
                        else:
                            st.dataframe(feat.head(200), use_container_width=True, height=520)
                    except Exception:
                        st.warning("Unable to load ADO feature demand rows.")

        # Measure selection
        value_col = "AMOUNT"
        can_build = True
        if measure != "Amount ($)":
            cand = [c for c in ["FTE", "DERIVED_FTE_EXPLORER"] if c in work.columns]
            value_col = cand[0] if cand else ""
            if not value_col:
                st.info("FTE measure unavailable: no FTE column present in these cost lines.")
                can_build = False
            else:
                work[value_col] = pd.to_numeric(work.get(value_col), errors="coerce").fillna(0.0)

        if can_build:
            # Aggregate/pivot
            group_cols = []
            for c in (rows_sel or []):
                if c and c not in group_cols and c in work.columns:
                    group_cols.append(c)
            if not group_cols:
                group_cols = ["ITERATION_LEVEL3"]

            agg_cols = group_cols + ([pivot_col] if pivot_col and pivot_col != "(none)" else [])
            agg = (
                work.groupby(agg_cols, dropna=False)[value_col]
                .sum()
                .reset_index()
            )
            if pivot_col and pivot_col != "(none)":
                pivot = (
                    agg.pivot_table(index=group_cols, columns=pivot_col, values=value_col, aggfunc="sum", fill_value=0.0)
                    .reset_index()
                )
            else:
                pivot = agg.copy()

            # Delta helpers (scenario pivot)
            colset = {str(c).upper() for c in pivot.columns}
            if "BASELINE" in colset and "EXPECTED" in colset:
                pivot["DELTA"] = pd.to_numeric(pivot.get("EXPECTED"), errors="coerce").fillna(0.0) - pd.to_numeric(pivot.get("BASELINE"), errors="coerce").fillna(0.0)
                pivot["DELTA_PCT"] = pivot.apply(
                    lambda r: (float(r["DELTA"]) / float(r["BASELINE"])) if float(r["BASELINE"]) != 0 else pd.NA,
                    axis=1,
                )
            if "ACTUAL" in colset and "BASELINE" in colset:
                pivot["ACTUAL_DELTA"] = pd.to_numeric(pivot.get("ACTUAL"), errors="coerce").fillna(0.0) - pd.to_numeric(pivot.get("BASELINE"), errors="coerce").fillna(0.0)

            # Sorting + Top N
            num_cols = [c for c in ["EXPECTED", "BASELINE", "ACTUAL", value_col, "DELTA", "ACTUAL_DELTA"] if c in pivot.columns]
            for c in num_cols:
                pivot[c] = pd.to_numeric(pivot.get(c), errors="coerce").fillna(0.0)
            pivot["__TOTAL__"] = 0.0
            if pivot_col and pivot_col != "(none)":
                for c in ["BASELINE", "EXPECTED", "ACTUAL"]:
                    if c in pivot.columns:
                        pivot["__TOTAL__"] += pd.to_numeric(pivot.get(c), errors="coerce").fillna(0.0)
            else:
                pivot["__TOTAL__"] = pd.to_numeric(pivot.get(value_col), errors="coerce").fillna(0.0)

            if sort_mode == "Amount asc":
                pivot = pivot.sort_values("__TOTAL__", ascending=True)
            elif sort_mode == "Name" and group_cols:
                pivot = pivot.sort_values(group_cols)
            else:
                pivot = pivot.sort_values("__TOTAL__", ascending=False)

            pivot_show = pivot.drop(columns=["__TOTAL__"], errors="ignore").head(int(top_n)).copy()
            st.dataframe(pivot_show, use_container_width=True, height=520)
            st.download_button(
                "Download CSV",
                data=pivot_show.to_csv(index=False).encode("utf-8"),
                file_name="budget_cost_explorer.csv",
                mime="text/csv",
                key="budget_cost_explorer_download",
            )

            with st.expander("Sanity checks", expanded=False):
                # Totals by scenario
                scen_tot = (
                    work.groupby("SCENARIO", dropna=False)[value_col].sum().reset_index().rename(columns={value_col: "TOTAL"})
                    if "SCENARIO" in work.columns
                    else pd.DataFrame()
                )
                if not scen_tot.empty:
                    scen_tot["TOTAL"] = pd.to_numeric(scen_tot.get("TOTAL"), errors="coerce").fillna(0.0)
                    st.dataframe(scen_tot.sort_values("TOTAL", ascending=False), use_container_width=True, height=220)

                # Totals by CAT and SUBCOMPONENT
                if all(c in work.columns for c in ["SCENARIO", "CAT", "SUBCOMPONENT"]):
                    by_comp = (
                        work.groupby(["SCENARIO", "CAT", "SUBCOMPONENT"], dropna=False)[value_col]
                        .sum()
                        .reset_index()
                        .rename(columns={value_col: "TOTAL"})
                        .sort_values("TOTAL", ascending=False)
                        .head(50)
                    )
                    st.dataframe(by_comp, use_container_width=True, height=320)

                # Expected-only allocation checks
                if "SCENARIO" in work.columns and "EXPECTED" in {str(x).upper() for x in work["SCENARIO"].unique().tolist()}:
                    exp = work.loc[work["SCENARIO"].astype(str).str.upper().eq("EXPECTED")].copy()
                    if "SOURCE" in exp.columns:
                        by_src = exp.groupby("SOURCE", dropna=False)[value_col].sum().reset_index().rename(columns={value_col: "TOTAL"}).sort_values("TOTAL", ascending=False).head(20)
                        st.dataframe(by_src, use_container_width=True, height=260)
                    if "GROUPNAME" in exp.columns:
                        g = exp["GROUPNAME"].fillna("").astype(str).str.strip()
                        unassigned = g.eq("") | g.eq("(Unassigned)")
                        unassigned_amt = float(pd.to_numeric(exp.loc[unassigned, value_col], errors="coerce").fillna(0.0).sum() or 0.0)
                        total_amt = float(pd.to_numeric(exp.get(value_col), errors="coerce").fillna(0.0).sum() or 0.0)
                        st.caption(f"Expected unassigned app spend: {_fmt_money(unassigned_amt, 0)} of {_fmt_money(total_amt, 0)}")

                # SoD-only breakdown
                if sod_only:
                    sub = work.get("SUBCOMPONENT", "").astype(str).str.upper()
                    bucket = pd.Series("Other", index=work.index)
                    bucket = bucket.where(~sub.str.contains("DELIVERY", na=False), "Delivery")
                    bucket = bucket.where(~sub.str.contains("CONTRACTOR", na=False), "Contractors")
                    sod_tbl = work.assign(SOD_BUCKET=bucket).groupby(["SCENARIO", "SOD_BUCKET"], dropna=False)[value_col].sum().reset_index().rename(columns={value_col: "TOTAL"})
                    st.dataframe(sod_tbl.sort_values("TOTAL", ascending=False), use_container_width=True, height=220)

                # Red flags
                warnings = []
                base_total = float(pd.to_numeric(work.loc[work["SCENARIO"].eq("BASELINE"), value_col], errors="coerce").fillna(0.0).sum() or 0.0) if "SCENARIO" in work.columns else 0.0
                exp_total = float(pd.to_numeric(work.loc[work["SCENARIO"].eq("EXPECTED"), value_col], errors="coerce").fillna(0.0).sum() or 0.0) if "SCENARIO" in work.columns else 0.0
                if base_total <= 0:
                    warnings.append("BASELINE has 0 rows/total.")
                if exp_total <= 0:
                    warnings.append("EXPECTED has 0 rows/total.")
                if base_total > 0 and exp_total > (2.0 * base_total) and ("GROUPNAME" in group_cols):
                    warnings.append("EXPECTED is >2× BASELINE at app-group level (baseline is not planned by app group).")
                if "ACTUAL" in {str(x).upper() for x in scen_sel}:
                    act_total = float(pd.to_numeric(work.loc[work["SCENARIO"].eq("ACTUAL"), value_col], errors="coerce").fillna(0.0).sum() or 0.0)
                    if act_total <= 0 and show_actual_override:
                        warnings.append("ACTUAL is 0 — check Apptio tables/mappings or selected years/programs.")
                if warnings:
                    for wmsg in warnings[:6]:
                        st.warning(wmsg)

if False:  # Tab removed: WF
    st.subheader("WF")
    st.caption("Workforce view – people-related costs and effort by program, team and app group (SoD-only baseline/expected; overhead shown separately).")

    wf_view = work_scope.loc[work_scope["CAT"].eq("WORK_FORCE")].copy()
    wf_view["YEAR"] = pd.to_numeric(wf_view.get("YEAR"), errors="coerce").astype("Int64")
    wf_view["PI"] = pd.to_numeric(wf_view.get("PI"), errors="coerce").astype("Int64")
    wf_view["PROGRAMNAME"] = wf_view.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    wf_view["TEAMNAME"] = wf_view.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    wf_view["GROUPNAME"] = wf_view.get("GROUPNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    wf_view["SOURCE"] = wf_view.get("SOURCE", "").astype(str).str.strip()
    wf_view["SUBCOMPONENT"] = wf_view.get("SUBCOMPONENT", "").astype(str).str.strip()
    wf_view["AMOUNT"] = pd.to_numeric(wf_view.get("AMOUNT"), errors="coerce").fillna(0.0)

    # SoD-only comparison: exclude program overhead people cost from both baseline and expected.
    wf_baseline = 0.0
    wf_expected = 0.0
    try:
        bl_wf = baseline_cost_lines.copy() if baseline_cost_lines is not None else pd.DataFrame()
        if "CAT" not in bl_wf.columns:
            bl_wf["CAT"] = ""
        if "SUBCOMPONENT" not in bl_wf.columns:
            bl_wf["SUBCOMPONENT"] = ""
        if "AMOUNT" not in bl_wf.columns:
            bl_wf["AMOUNT"] = 0.0
        bl_wf = bl_wf.loc[bl_wf["CAT"].astype(str).eq("WORK_FORCE")].copy()
        bl_sub_up = bl_wf.get("SUBCOMPONENT", "").astype(str).str.upper()
        bl_wf = bl_wf.loc[~bl_sub_up.str.contains("PROGRAM", na=False)].copy()
        wf_baseline = float(pd.to_numeric(bl_wf.get("AMOUNT"), errors="coerce").fillna(0.0).sum() or 0.0)
    except Exception:
        wf_baseline = 0.0
    try:
        if "CAT" not in work_scope.columns:
            work_scope["CAT"] = ""
        exp_wf = work_scope.loc[work_scope["CAT"].astype(str).eq("WORK_FORCE")].copy()
        exp_sub_up = exp_wf.get("SUBCOMPONENT", "").astype(str).str.upper()
        exp_wf = exp_wf.loc[~exp_sub_up.str.contains("PROGRAM", na=False)].copy()
        wf_expected = float(pd.to_numeric(exp_wf.get("AMOUNT"), errors="coerce").fillna(0.0).sum() or 0.0)
    except Exception:
        wf_expected = 0.0
    wf_variance = float(wf_expected - wf_baseline)
    wf_var_pct = (wf_variance / wf_baseline) if wf_baseline > 0 else None
    demand_total = (
        float(pd.to_numeric(demand_multi_all.get("DERIVED_FTE_SUM"), errors="coerce").fillna(0.0).sum() or 0.0)
        if demand_multi_all is not None and not demand_multi_all.empty
        else 0.0
    )

    k = st.columns(4)
    _metric(
        k[0],
        "Planned Baseline – WF",
        _fmt_money(wf_baseline, 0),
        extra="Planned WF baseline for SoD teams (excludes program overhead).",
        raw_source="VW_TCO_WORKFORCE_SPLIT + VW_PROGRAM_*",
    )
    _metric(
        k[1],
        "Expected Spend – WF",
        _fmt_money(wf_expected, 0),
        extra="Expected WF spend for SoD teams (excludes program overhead).",
        raw_source="VW_TCO_WORKFORCE_SPLIT + VW_PROGRAM_*",
    )
    _metric(
        k[2],
        "WF Variance vs Planned",
        _fmt_money(wf_variance, 0),
        extra=_fmt_pct(wf_var_pct, 0) if wf_var_pct is not None else "—",
        raw_source="Derived from baseline + expected",
    )
    _metric(
        k[3],
        "Demand (Derived FTE)",
        _fmt_num(demand_total, 2),
        extra="Total demand from Explorer v2 (SWAG-derived).",
        raw_source="VW_TCO_FEATURE_DEMAND",
    )

    st.markdown("##### WF cost drilldown")
    if wf_view.empty:
        st.info("No WF rows found for this scope.")
    else:
        wf_view["INCREMENT"] = wf_view.apply(
            lambda r: f"{int(r['YEAR'])} I{int(r['PI'])}" if pd.notna(r.get("YEAR")) and pd.notna(r.get("PI")) else "",
            axis=1,
        )
        wf_view["COST_TYPE"] = wf_view["SUBCOMPONENT"].astype(str).str.upper().replace({"": "OTHER"})
        wf_view.loc[wf_view["TEAMNAME"].eq("—"), "COST_TYPE"] = "PROGRAM_OVERHEAD"
        wf_agg = (
            wf_view.groupby(["YEAR", "PI", "INCREMENT", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_TYPE"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "WF_COST"})
        )
        if demand_multi_all is not None and not demand_multi_all.empty:
            dem_for_join = demand_multi_all.copy()
            dem_for_join["ADO_YEAR"] = pd.to_numeric(dem_for_join.get("ADO_YEAR"), errors="coerce").astype("Int64")
            dem_for_join["PI_NUM"] = pd.to_numeric(dem_for_join.get("PI_NUM"), errors="coerce").astype("Int64")
            dem_for_join["INCREMENT"] = dem_for_join.apply(
                lambda r: f"{int(r['ADO_YEAR'])} I{int(r['PI_NUM'])}"
                if pd.notna(r.get("ADO_YEAR")) and pd.notna(r.get("PI_NUM"))
                else "",
                axis=1,
            )
            dem_for_join["DEMAND_FTE"] = pd.to_numeric(dem_for_join.get("DERIVED_FTE_SUM"), errors="coerce").fillna(0.0)
            dem_for_join = dem_for_join[["ADO_YEAR", "PI_NUM", "INCREMENT", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "DEMAND_FTE"]].rename(
                columns={"ADO_YEAR": "YEAR", "PI_NUM": "PI"}
            )
            wf_agg = wf_agg.merge(dem_for_join, on=["YEAR", "PI", "INCREMENT", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"], how="left")
        wf_agg["DEMAND_FTE"] = pd.to_numeric(wf_agg.get("DEMAND_FTE"), errors="coerce").fillna(0.0)

        with st.container(border=True):
            st.markdown("##### Insights")
            insights: List[str] = []
            total_wf = float(pd.to_numeric(wf_agg.get("WF_COST"), errors="coerce").fillna(0.0).sum() or 0.0)
            if total_wf > 0:
                top_prog = (
                    wf_agg.groupby("PROGRAMNAME", dropna=False)["WF_COST"]
                    .sum()
                    .reset_index()
                    .sort_values("WF_COST", ascending=False)
                    .head(1)
                )
                if not top_prog.empty:
                    p = str(top_prog.iloc[0].get("PROGRAMNAME") or "(Unassigned)")
                    share = float(top_prog.iloc[0].get("WF_COST") or 0.0) / total_wf
                    insights.append(f"Most expensive program (WF): {p} (≈ {_fmt_pct(share, 0)} of WF cost).")
            if demand_total > 0:
                insights.append(f"Planned effort in scope: {_fmt_num(demand_total, 2)} FTE (SWAG-only).")
            _bullets(insights[:4])

        group_by = st.selectbox("Group by", options=["Program", "Team", "Application", "Cost type"], index=0, key="budget_wf_group_by")
        key_map = {
            "Program": ["PROGRAMNAME"],
            "Team": ["PROGRAMNAME", "TEAMNAME"],
            "Application": ["PROGRAMNAME", "GROUPNAME"],
            "Cost type": ["COST_TYPE"],
        }
        gcols = ["YEAR", "INCREMENT"] + key_map.get(group_by, ["PROGRAMNAME"])
        wf_tbl = wf_agg.groupby(gcols, dropna=False).agg(DEMAND_FTE=("DEMAND_FTE", "sum"), WF_COST=("WF_COST", "sum")).reset_index()
        wf_tbl = wf_tbl.sort_values("WF_COST", ascending=False)
        wf_show = wf_tbl.copy()
        wf_show["DEMAND_FTE"] = wf_show["DEMAND_FTE"].map(lambda v: _fmt_num(float(v or 0.0), 2))
        wf_show["WF_COST"] = wf_show["WF_COST"].map(lambda v: _fmt_money(float(v or 0.0), 0))
        st.dataframe(wf_show, use_container_width=True, hide_index=True, height=360)

        prog_tot = wf_agg.groupby("PROGRAMNAME", dropna=False)["WF_COST"].sum().reset_index().sort_values("WF_COST", ascending=False)
        top_programs = prog_tot.head(10)["PROGRAMNAME"].tolist()
        chart_df = wf_agg[wf_agg["PROGRAMNAME"].isin(top_programs)].copy()
        pivot = (
            chart_df.groupby(["PROGRAMNAME", "COST_TYPE"], dropna=False)["WF_COST"]
            .sum()
            .reset_index()
            .pivot_table(index="PROGRAMNAME", columns="COST_TYPE", values="WF_COST", fill_value=0.0, aggfunc="sum")
        )
        x = pivot.index.astype(str).tolist()
        series = []
        for c in list(pivot.columns):
            vals = pd.to_numeric(pivot[c], errors="coerce").fillna(0.0).astype(float).round(0).tolist()
            series.append({"name": str(c), "type": "bar", "stack": "wf", "data": vals, "itemStyle": {"color": _pick_color(str(c))}})
        opt = {
            "title": {"text": "WF cost by program (Top 10)", "left": "center"},
            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
            "legend": {"top": 26},
            "grid": {"left": "6%", "right": "4%", "top": 70, "bottom": 90, "containLabel": True},
            "xAxis": {"type": "category", "data": x, "axisLabel": {"rotate": 25}},
            "yAxis": {"type": "value", "axisLabel": {"formatter": "${value}"}},
            "series": series,
        }
        render_echart(opt, height="380px", key=f"budget_wf_prog_{scope_key}", page_theme=page_theme)

if False:  # Tab removed: NWF
    st.subheader("NWF")
    st.caption("Non-workforce view – contracts, invoices and other non-staff costs for the selected scope.")

    include_msp = st.checkbox("Include MSP in NWF drilldown", value=False, key="budget_nwf_include_msp")
    nwf_view = work_scope.loc[work_scope["CAT"].eq("NON_WORK_FORCE")].copy()
    nwf_view["YEAR"] = pd.to_numeric(nwf_view.get("YEAR"), errors="coerce").astype("Int64")
    nwf_view["PI"] = pd.to_numeric(nwf_view.get("PI"), errors="coerce").astype("Int64")
    nwf_view["PROGRAMNAME"] = nwf_view.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    nwf_view["TEAMNAME"] = nwf_view.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    nwf_view["GROUPNAME"] = nwf_view.get("GROUPNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    nwf_view["SOURCE"] = nwf_view.get("SOURCE", "").astype(str).str.strip()
    nwf_view["SUBCOMPONENT"] = nwf_view.get("SUBCOMPONENT", "").astype(str).str.strip()
    nwf_view["AMOUNT"] = pd.to_numeric(nwf_view.get("AMOUNT"), errors="coerce").fillna(0.0)
    if not include_msp:
        nwf_view = nwf_view[nwf_view["SOURCE"].astype(str).str.upper().ne("MSP")].copy()

    nwf_baseline_view = baseline_cost_lines.copy() if baseline_cost_lines is not None else pd.DataFrame()
    if "CAT" not in nwf_baseline_view.columns:
        nwf_baseline_view["CAT"] = ""
    if "SOURCE" not in nwf_baseline_view.columns:
        nwf_baseline_view["SOURCE"] = ""
    if "AMOUNT" not in nwf_baseline_view.columns:
        nwf_baseline_view["AMOUNT"] = 0.0
    nwf_baseline_view = nwf_baseline_view.loc[nwf_baseline_view["CAT"].astype(str).eq("NON_WORK_FORCE")].copy()
    if not include_msp and not nwf_baseline_view.empty:
        nwf_baseline_view = nwf_baseline_view[nwf_baseline_view["SOURCE"].astype(str).str.upper().ne("MSP")].copy()
    nwf_baseline_view["AMOUNT"] = pd.to_numeric(nwf_baseline_view["AMOUNT"], errors="coerce").fillna(0.0)

    nwf_baseline = float(nwf_baseline_view["AMOUNT"].sum() or 0.0)
    nwf_expected = float(nwf_view["AMOUNT"].sum() or 0.0)
    nwf_variance = float(nwf_expected - nwf_baseline)
    nwf_var_pct = (nwf_variance / nwf_baseline) if nwf_baseline > 0 else None

    k = st.columns(3)
    _metric(
        k[0],
        "Planned Baseline – NWF",
        _fmt_money(nwf_baseline, 0),
        extra="Planned non-workforce (NWF) costs for this view.",
        raw_source=baseline_financial_source,
    )
    _metric(
        k[1],
        "Expected Spend – NWF",
        _fmt_money(nwf_expected, 0),
        extra="Expected non-workforce (NWF) spend from contracts and invoices.",
        raw_source="VW_TCO_WORKFORCE_SPLIT",
    )
    _metric(
        k[2],
        "NWF Variance vs Planned",
        _fmt_money(nwf_variance, 0),
        extra=_fmt_pct(nwf_var_pct, 0) if nwf_var_pct is not None else "—",
        raw_source="Derived from baseline + expected",
    )

    st.markdown("##### NWF cost drilldown")
    if nwf_view.empty:
        st.info("No NWF rows found for this scope.")
    else:
        nwf_view["INCREMENT"] = nwf_view.apply(
            lambda r: f"{int(r['YEAR'])} I{int(r['PI'])}" if pd.notna(r.get("YEAR")) and pd.notna(r.get("PI")) else "",
            axis=1,
        )
        nwf_view["COST_TYPE"] = (
            nwf_view["SOURCE"].astype(str).str.upper().replace({"": "UNKNOWN"})
            + " • "
            + nwf_view["SUBCOMPONENT"].astype(str).str.upper().replace({"": "OTHER"})
        )
        nwf_agg = (
            nwf_view.groupby(["YEAR", "PI", "INCREMENT", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_TYPE"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "NWF_COST"})
        )

        with st.container(border=True):
            st.markdown("##### Insights")
            insights: List[str] = []
            total_nwf = float(pd.to_numeric(nwf_agg.get("NWF_COST"), errors="coerce").fillna(0.0).sum() or 0.0)
            if total_nwf > 0:
                top_prog = (
                    nwf_agg.groupby("PROGRAMNAME", dropna=False)["NWF_COST"]
                    .sum()
                    .reset_index()
                    .sort_values("NWF_COST", ascending=False)
                    .head(1)
                )
                if not top_prog.empty:
                    p = str(top_prog.iloc[0].get("PROGRAMNAME") or "(Unassigned)")
                    share = float(top_prog.iloc[0].get("NWF_COST") or 0.0) / total_nwf
                    insights.append(f"Top NWF driver: Program {p} (≈ {_fmt_pct(share, 0)} of NWF cost).")
            _bullets(insights[:4])

        group_by = st.selectbox("Group by", options=["Program", "Application", "Cost type"], index=0, key="budget_nwf_group_by")
        key_map = {"Program": ["PROGRAMNAME"], "Application": ["PROGRAMNAME", "GROUPNAME"], "Cost type": ["COST_TYPE"]}
        gcols = ["YEAR", "INCREMENT"] + key_map.get(group_by, ["PROGRAMNAME"])
        nwf_tbl = nwf_agg.groupby(gcols, dropna=False)["NWF_COST"].sum().reset_index().sort_values("NWF_COST", ascending=False)
        nwf_show = nwf_tbl.copy()
        nwf_show["NWF_COST"] = nwf_show["NWF_COST"].map(lambda v: _fmt_money(float(v or 0.0), 0))
        st.dataframe(nwf_show, use_container_width=True, hide_index=True, height=360)

        prog_tot = nwf_agg.groupby("PROGRAMNAME", dropna=False)["NWF_COST"].sum().reset_index().sort_values("NWF_COST", ascending=False)
        top_programs = prog_tot.head(10)["PROGRAMNAME"].tolist()
        chart_df = nwf_agg[nwf_agg["PROGRAMNAME"].isin(top_programs)].copy()
        top_types = (
            chart_df.groupby("COST_TYPE", dropna=False)["NWF_COST"]
            .sum()
            .reset_index()
            .sort_values("NWF_COST", ascending=False)
            .head(8)["COST_TYPE"]
            .tolist()
        )
        chart_df["TYPE_SERIES"] = chart_df["COST_TYPE"].where(chart_df["COST_TYPE"].isin(top_types), other="Other")
        pivot = (
            chart_df.groupby(["PROGRAMNAME", "TYPE_SERIES"], dropna=False)["NWF_COST"]
            .sum()
            .reset_index()
            .pivot_table(index="PROGRAMNAME", columns="TYPE_SERIES", values="NWF_COST", fill_value=0.0, aggfunc="sum")
        )
        x = pivot.index.astype(str).tolist()
        series = []
        for t in list(pivot.columns):
            vals = pd.to_numeric(pivot[t], errors="coerce").fillna(0.0).astype(float).round(0).tolist()
            series.append({"name": str(t), "type": "bar", "stack": "nwf", "data": vals, "itemStyle": {"color": _pick_color(str(t))}})
        opt = {
            "title": {"text": "NWF cost by program (Top 10)", "left": "center"},
            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
            "legend": {"top": 26},
            "grid": {"left": "6%", "right": "4%", "top": 70, "bottom": 90, "containLabel": True},
            "xAxis": {"type": "category", "data": x, "axisLabel": {"rotate": 25}},
            "yAxis": {"type": "value", "axisLabel": {"formatter": "${value}"}},
            "series": series,
        }
        render_echart(opt, height="380px", key=f"budget_nwf_prog_{scope_key}", page_theme=page_theme)

with tabs_by_name["Capacity"]:
    st.subheader("Capacity")
    st.caption("Staffing and workload alignment overview")

    demand_multi = demand_multi_all.copy() if demand_multi_all is not None else pd.DataFrame()
    cap_multi = cap_multi_all.copy() if cap_multi_all is not None else pd.DataFrame()

    if demand_multi.empty:
        st.info("No Derived FTE demand rows found for this scope.")
    else:
        dem = demand_multi.copy()
        dem["ADO_YEAR"] = pd.to_numeric(dem.get("ADO_YEAR"), errors="coerce").astype("Int64")
        dem["PI_NUM"] = pd.to_numeric(dem.get("PI_NUM"), errors="coerce").astype("Int64")
        dem = dem[dem["PI_NUM"].notna() & dem["PI_NUM"].isin(pi_nums)].copy()
        dem["PROGRAMNAME"] = dem.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        dem["TEAMNAME"] = dem.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        dem["DEMAND_FTE"] = pd.to_numeric(dem.get("DERIVED_FTE_SUM"), errors="coerce").fillna(0.0)

        # Shared source of truth: extracted from Budget Capacity tab so Dashboard can match it exactly.
        merged_frames = []
        for yr in [int(y) for y in years_sel]:
            try:
                merged_frames.append(
                    fetch_capacity_demand_pi(
                        year=int(yr),
                        programs=list(effective_programs or []),
                        teams=list(effective_teams or []),
                    )
                )
            except Exception:
                merged_frames.append(pd.DataFrame())
        merged_non_empty = [m for m in merged_frames if isinstance(m, pd.DataFrame) and not m.empty]
        merged = pd.concat(merged_non_empty, ignore_index=True, sort=False) if merged_non_empty else pd.DataFrame()

        if merged is None or merged.empty:
            st.info("No headcount capacity rows found for this scope. Add headcount in Teams/Programs and refresh.")
        else:
            merged = merged.rename(
                columns={"YEAR": "ADO_YEAR", "PI": "PI_NUM", "ITERATION_LEVEL3": "PI_LABEL", "UTILIZATION_PCT": "UTILIZATION"}
            )
            merged["ADO_YEAR"] = pd.to_numeric(merged.get("ADO_YEAR"), errors="coerce").astype("Int64")
            merged["PI_NUM"] = pd.to_numeric(merged.get("PI_NUM"), errors="coerce").astype("Int64")
            merged = merged[merged["PI_NUM"].notna() & merged["PI_NUM"].isin(pi_nums)].copy()
            y = pd.to_numeric(merged.get("ADO_YEAR"), errors="coerce").astype("Int64")
            p = pd.to_numeric(merged.get("PI_NUM"), errors="coerce").astype("Int64")
            merged["PI_ORDER"] = (y * 10 + p).astype("Int64")

            tot_demand = float(pd.to_numeric(merged.get("DEMAND_FTE"), errors="coerce").fillna(0.0).sum() or 0.0)
            tot_capacity = float(pd.to_numeric(merged.get("CAPACITY_FTE"), errors="coerce").fillna(0.0).sum() or 0.0)
            tot_headcount = float(pd.to_numeric(merged.get("HEADCOUNT"), errors="coerce").fillna(0.0).sum() or 0.0)
            util_all = (tot_demand / tot_capacity) if tot_capacity > 0 else None
            peak_util = None
            peak_label = ""
            try:
                u = pd.to_numeric(merged.get("UTILIZATION"), errors="coerce")
                if u is not None and not u.dropna().empty:
                    peak_idx = int(u.astype(float).idxmax())
                    peak_util = float(u.loc[peak_idx])
                    peak_label = str(merged.loc[peak_idx].get("PI_LABEL") or "")
            except Exception:
                peak_util = None
                peak_label = ""

            # Quick KPIs: real headcount, effective FTE capacity, and utilization.
            c1, c2, c3 = st.columns(3)
            c1.metric("Total headcount (people)", _fmt_num(tot_headcount, 1))
            c2.metric("Effective capacity (FTE)", _fmt_num(tot_capacity, 1))
            c3.metric("Overall utilization", _fmt_pct(util_all, 0) if util_all is not None else "—")

            k = st.columns(5)
            _metric(
                k[0],
                "Demand (FTE)",
                _fmt_num(tot_demand, 2),
                extra="Derived FTE demand from Explorer v2 (SWAG-derived).",
                raw_source="VW_TCO_FEATURE_DEMAND",
            )
            _metric(
                k[1],
                "Capacity (FTE)",
                _fmt_num(tot_capacity, 2),
                extra=f"Headcount × capacity fraction ({cap_fraction:.0%}).",
                raw_source="VW_TEAM_ALLOCATED_HEADCOUNT_PI",
            )
            _metric(
                k[2],
                "Utilization",
                _fmt_pct(util_all, 0) if util_all is not None else "—",
                extra="Demand ÷ Capacity (selected scope).",
                raw_source="Derived from demand + capacity",
            )
            _metric(
                k[3],
                "Peak utilization",
                _fmt_pct(peak_util, 0) if peak_util is not None else "—",
                extra=f"{peak_label}" if peak_label else "Highest PI utilization in this scope.",
                raw_source="Derived from demand + capacity",
             )
            _metric(
                k[4],
                "Capacity fraction",
                _fmt_pct(cap_fraction, 0),
                extra="Available capacity reserved for planned features.",
                raw_source=".streamlit/secrets.toml",
            )

            over = merged.loc[merged["UTILIZATION"].notna() & (merged["UTILIZATION"] > 1.0)]
            near = merged.loc[merged["UTILIZATION"].notna() & (merged["UTILIZATION"] > 0.8) & (merged["UTILIZATION"] <= 1.0)]
            if not over.empty:
                st.warning(f"{len(over.index)} team×PI bucket(s) exceed capacity (> 100%). Consider re-scoping or adding capacity.")
            elif not near.empty:
                st.info(f"{len(near.index)} team×PI bucket(s) are above 80% utilization.")

            with st.container(border=True):
                st.markdown("##### Insights")
                insights: List[str] = []
                if not over.empty:
                    team_over = (
                        over.groupby("TEAMNAME", dropna=False)["DEMAND_FTE"].sum().reset_index().sort_values("DEMAND_FTE", ascending=False).head(1)
                    )
                    if not team_over.empty:
                        tname = str(team_over.iloc[0].get("TEAMNAME") or "(Unassigned)")
                        insights.append(f"Team {tname} exceeds capacity in the selected increments.")
                    most_over = over.copy()
                    most_over["GAP"] = most_over["DEMAND_FTE"] - most_over["CAPACITY_FTE"]
                    most_over = most_over.sort_values("GAP", ascending=False).head(1)
                    if not most_over.empty:
                        r = most_over.iloc[0]
                        insights.append(
                            f"Most overloaded increment: {r.get('PI_LABEL')} ({_fmt_num(float(r.get('DEMAND_FTE') or 0.0), 2)} FTE vs {_fmt_num(float(r.get('CAPACITY_FTE') or 0.0), 2)} capacity)."
                        )
                else:
                    avg_util = merged["UTILIZATION"].dropna()
                    if not avg_util.empty:
                        insights.append(f"Average utilization: {_fmt_pct(float(avg_util.mean()), 0)}.")
                _bullets(insights[:4])

            with st.container(border=True):
                st.markdown("##### Data readiness")
                readiness: List[str] = []
                missing_cap = merged[(merged["DEMAND_FTE"] > 0) & (merged["CAPACITY_FTE"] <= 0)].copy()
                if not missing_cap.empty:
                    note_lookup: dict[tuple[str, int, int], str] = {}
                    try:
                        teams_miss = missing_cap["TEAMNAME"].dropna().astype(str).str.strip().unique().tolist()
                        years_miss = (
                            pd.to_numeric(missing_cap.get("ADO_YEAR"), errors="coerce").dropna().astype(int).unique().tolist()
                            if "ADO_YEAR" in missing_cap.columns
                            else [int(outlook_year)]
                        )
                        notes_raw = _load_headcount_notes_team_pi_years(years=years_miss, team_names=teams_miss)
                        if notes_raw is not None and not notes_raw.empty:
                            nr = notes_raw.copy()
                            nr["TEAM_KEY"] = nr.get("TEAMNAME", "").astype(str).str.upper().str.strip()
                            nr["YEAR"] = pd.to_numeric(nr.get("YEAR"), errors="coerce").astype("Int64")
                            nr["PI_NUM"] = pd.to_numeric(nr.get("PI_NUM"), errors="coerce").astype("Int64")
                            nr["NOTE"] = nr.get("NOTE", "").fillna("").astype(str).str.strip()
                            nr = nr[nr["TEAM_KEY"].ne("") & nr["YEAR"].notna() & nr["PI_NUM"].notna() & nr["NOTE"].ne("")].copy()

                            def _join_notes(s: pd.Series) -> str:
                                return "; ".join(sorted(set([str(x).strip() for x in s.tolist() if str(x).strip()])))

                            n0 = (
                                nr.loc[nr["PI_NUM"] == 0]
                                .groupby(["TEAM_KEY", "YEAR"], dropna=False)["NOTE"]
                                .apply(_join_notes)
                                .to_dict()
                            )
                            npi = (
                                nr.loc[nr["PI_NUM"].isin([1, 2, 3, 4])]
                                .groupby(["TEAM_KEY", "YEAR", "PI_NUM"], dropna=False)["NOTE"]
                                .apply(_join_notes)
                                .to_dict()
                            )
                            for key, val in npi.items():
                                team_key, y, pi = key
                                note_lookup[(str(team_key), int(y), int(pi))] = str(val)
                            for key, val in n0.items():
                                team_key, y = key
                                for pi in [1, 2, 3, 4]:
                                    note_lookup.setdefault((str(team_key), int(y), int(pi)), str(val))
                    except Exception:
                        note_lookup = {}

                    r0 = missing_cap.head(4)
                    for _, r in r0.iterrows():
                        y = int(r.get("ADO_YEAR") or outlook_year)
                        pi_num = None
                        if "PI_NUM" in r and pd.notna(r.get("PI_NUM")):
                            try:
                                pi_num = int(r.get("PI_NUM"))
                            except Exception:
                                pi_num = None
                        if pi_num is None:
                            s = str(r.get("PI_LABEL") or "")
                            try:
                                pi_num = int("".join(ch for ch in s.split("I")[-1] if ch.isdigit())) if "I" in s else None
                            except Exception:
                                pi_num = None
                        note = (
                            note_lookup.get((str(r.get("TEAMNAME") or "").strip().upper(), int(y), int(pi_num)), "")
                            if pi_num in [1, 2, 3, 4]
                            else ""
                        )
                        suffix = f" Note: {note}" if note else ""
                        readiness.append(f"⚠️ Team {r['TEAMNAME']} has demand but no headcount in {r['PI_LABEL']}.{suffix}")
                else:
                    readiness.append("✅ Headcount is present for demand (in this scope).")

                teams_with_demand = (
                    merged.loc[pd.to_numeric(merged.get("DEMAND_FTE"), errors="coerce").fillna(0.0) > 0, "TEAMNAME"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                    .tolist()
                )
                missing_rates = _missing_rate_teams_for_year(year=int(outlook_year), team_names=teams_with_demand)
                if missing_rates:
                    readiness.append(f"⚠️ Missing rates for {len(missing_rates)} team(s) with demand (costs may be underreported).")
                else:
                    readiness.append("✅ Rates look OK for teams with demand (best-effort check).")
                _bullets(readiness[:6])

            show = merged.sort_values(["ADO_YEAR", "PI_ORDER", "PROGRAMNAME", "TEAMNAME"]).copy()
            show["DEMAND_FTE"] = show["DEMAND_FTE"].map(lambda v: f"{float(v or 0.0):,.2f}")
            show["HEADCOUNT"] = show["HEADCOUNT"].map(lambda v: f"{float(v or 0.0):,.1f}")
            show["CAPACITY_FTE"] = show["CAPACITY_FTE"].map(lambda v: f"{float(v or 0.0):,.2f}")
            show["UTILIZATION"] = show["UTILIZATION"].map(lambda v: f"{float(v) * 100:,.0f}%" if v is not None and not pd.isna(v) else "—")
            st.dataframe(
                show[["ADO_YEAR", "PI_LABEL", "PROGRAMNAME", "TEAMNAME", "HEADCOUNT", "CAPACITY_FTE", "DEMAND_FTE", "UTILIZATION"]],
                use_container_width=True,
                hide_index=True,
                height=420,
            )

            team_list = sorted(set(merged["TEAMNAME"].astype(str).tolist()))
            focus_team = st.selectbox("Team chart focus", options=["(All)"] + team_list, index=0, key="budget_team_focus")
            chart_df = merged.copy() if focus_team == "(All)" else merged.loc[merged["TEAMNAME"].astype(str) == str(focus_team)].copy()
            chart_df = chart_df.groupby(["PI_ORDER", "PI_LABEL"], dropna=False)[["DEMAND_FTE", "CAPACITY_FTE"]].sum().reset_index().sort_values("PI_ORDER")
            x = chart_df["PI_LABEL"].astype(str).tolist()
            demand_vals = pd.to_numeric(chart_df["DEMAND_FTE"], errors="coerce").fillna(0.0).astype(float).round(2).tolist()
            cap_vals = pd.to_numeric(chart_df["CAPACITY_FTE"], errors="coerce").fillna(0.0).astype(float).round(2).tolist()
            opt_combo = {
                "title": {"text": f"Demand vs Capacity by PI ({focus_team})", "left": "center"},
                "tooltip": {"trigger": "axis"},
                "legend": {"top": 26, "data": ["Demand (FTE)", "Capacity (FTE)"]},
                "grid": {"left": 70, "right": 50, "top": 70, "bottom": 70},
                "xAxis": {"type": "category", "data": x, "axisLabel": {"rotate": 25}},
                "yAxis": {"type": "value", "name": "FTE"},
                "series": [
                    {
                        "name": "Demand (FTE)",
                        "type": "bar",
                        "data": demand_vals,
                    },
                    {
                        "name": "Capacity (FTE)",
                        "type": "line",
                        "data": cap_vals,
                    },
                ],
            }
            render_echart(opt_combo, height="380px", key=f"budget_team_cap_combo_{scope_key}_{focus_team}", page_theme=page_theme)
            st.caption(
                "Demand FTE comes from ADO roadmap (Derived FTE). Capacity FTE comes from allocated headcount per increment. "
                "Utilization above 100% indicates teams are over-subscribed for the planned scope."
            )

            st.markdown("##### Trend")
            future_only = st.toggle("Show only future increments", value=False, key="budget_cap_future_only")
            trend = (
                merged.groupby(["PI_ORDER", "PI_LABEL"], dropna=False)[["DEMAND_FTE", "CAPACITY_FTE"]]
                .sum()
                .reset_index()
                .sort_values("PI_ORDER")
            )
            if future_only and not trend.empty:
                today = dt.date.today()
                current_pi = int(max(1, min(4, math.ceil(int(today.month) / 3.0))))
                current_order = int(today.year) * 10 + int(current_pi)
                trend = trend.loc[
                    pd.to_numeric(trend.get("PI_ORDER"), errors="coerce").fillna(0).astype(int) >= int(current_order)
                ].copy()
            x_all = trend["PI_LABEL"].astype(str).tolist()
            d_all = pd.to_numeric(trend["DEMAND_FTE"], errors="coerce").fillna(0.0).astype(float).round(2).tolist()
            c_all = pd.to_numeric(trend["CAPACITY_FTE"], errors="coerce").fillna(0.0).astype(float).round(2).tolist()
            opt_trend = {
                "title": {"text": "Demand and capacity over time", "left": "center"},
                "tooltip": {"trigger": "axis"},
                "legend": {"top": 26, "data": ["Demand (FTE)", "Capacity (FTE)"]},
                "grid": {"left": 70, "right": 30, "top": 70, "bottom": 70},
                "xAxis": {"type": "category", "data": x_all, "axisLabel": {"rotate": 25}},
                "yAxis": {"type": "value", "name": "FTE"},
                "series": [
                    {"name": "Demand (FTE)", "type": "line", "data": d_all, "itemStyle": {"color": "#4C78A8"}},
                    {"name": "Capacity (FTE)", "type": "line", "data": c_all, "itemStyle": {"color": "#22c55e"}},
                ],
            }
            render_echart(opt_trend, height="320px", key=f"budget_cap_trend_{scope_key}_{focus_team}", page_theme=page_theme)

if False:  # Tab removed: Overhead & MSP
    st.subheader("Overhead & MSP")
    st.caption("Overhead is program-level only (not tied to features/app groups). MSP is shown as cost (not internal delivery demand).")

    st.markdown("##### Program overhead people (FTE × rate)")
    overhead_primary = float(overhead_people_primary)
    overhead_view = float(overhead_people_cost_from_view)
    overhead_delta = float(overhead_primary - overhead_view)
    overhead_delta_pct = (overhead_delta / overhead_view) if overhead_view else None

    k = st.columns(3)
    _metric(
        k[0],
        "Overhead people cost (FTE×rate)",
        _fmt_money(overhead_primary, 0),
        extra="Primary calculation for program overhead people cost.",
        raw_source="VW_PROGRAM_COMPOSITION_EFFECTIVE + VW_PROGRAM_RATE_EFFECTIVE",
    )
    _metric(
        k[1],
        "Overhead people cost (unified view)",
        _fmt_money(overhead_view, 0),
        extra="Shown for consistency checking only.",
        raw_source="VW_TCO_WORKFORCE_SPLIT",
    )
    _metric(
        k[2],
        "Delta",
        _fmt_money(overhead_delta, 0),
        extra=_fmt_pct(overhead_delta_pct, 0) if overhead_delta_pct is not None else "—",
        raw_source="(FTE×rate) − (view)",
    )

    if _overhead_inputs_all is None or _overhead_inputs_all.empty:
        st.info("No program overhead FTE/rate rows found for the selected scope.")
    else:
        ov = _overhead_inputs_all.copy()
        ov["YEAR"] = pd.to_numeric(ov.get("YEAR"), errors="coerce").astype("Int64")
        ov["PI"] = pd.to_numeric(ov.get("PI"), errors="coerce").fillna(0).astype(int)
        ov = ov[ov["PI"].isin([int(p) for p in pi_nums])].copy() if pi_nums else ov
        ov["PROGRAMNAME"] = ov.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        ov["PROGRAMFTE"] = pd.to_numeric(ov.get("PROGRAMFTE"), errors="coerce").fillna(0.0)
        ov["PROGRAM_XOM_RATE"] = pd.to_numeric(ov.get("PROGRAM_XOM_RATE"), errors="coerce").fillna(0.0)
        ov["COST"] = ov["PROGRAMFTE"] * ov["PROGRAM_XOM_RATE"]
        ov_sum = (
            ov.groupby(["YEAR", "PI", "PROGRAMNAME"], dropna=False)[["PROGRAMFTE", "COST"]]
            .sum()
            .reset_index()
            .sort_values(["YEAR", "PI", "PROGRAMNAME"])
        )
        show = ov_sum.copy()
        show["PROGRAMFTE"] = show["PROGRAMFTE"].map(lambda v: f"{float(v or 0.0):,.2f}")
        show["COST"] = show["COST"].map(lambda v: _fmt_money(float(v or 0.0), 0))
        st.dataframe(show, use_container_width=True, hide_index=True, height=320)

        if overhead_view and abs(overhead_delta_pct or 0.0) >= 0.1:
            st.warning(
                f"Note: Overhead people cost from FTE×rate is {_fmt_money(overhead_primary, 0)}, "
                f"but the unified cost view shows {_fmt_money(overhead_view, 0)}. "
                "Review program rates or workforce split configuration if this is unexpected."
            )

    st.markdown("##### Program additional costs (NWF)")
    st.caption("These costs are not allocated to app groups and are shown at Program level only.")
    work_outlook = work_scope.copy()
    src_up_local = work_outlook.get("SOURCE", pd.Series(dtype=str)).astype(str).str.upper()
    pac = work_outlook.loc[src_up_local.eq("PROGRAM_ADDITIONAL")].copy()
    if pac.empty:
        st.info("No Program Additional Costs found in this scope/year.")
    else:
        pac["PROGRAMNAME"] = pac.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        pac["SUBCOMPONENT"] = pac.get("SUBCOMPONENT", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        pac["AMOUNT"] = pd.to_numeric(pac.get("AMOUNT"), errors="coerce").fillna(0.0)
        pac_show = (
            pac.groupby(["PROGRAMNAME", "SUBCOMPONENT"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .sort_values("AMOUNT", ascending=False)
        )
        pac_show["AMOUNT"] = pac_show["AMOUNT"].map(lambda v: _fmt_money(float(v or 0.0), 0))
        st.dataframe(pac_show, use_container_width=True, hide_index=True, height=280)

    st.markdown("##### MSP cost by increment / app group")
    msp = _load_msp_cost_by_pi_group_years(
        years=[int(y) for y in years_sel],
        programs=effective_programs,
        teams=effective_teams,
        groups=effective_groups,
    )
    if msp is None or msp.empty:
        st.info("No MSP cost rows found for this scope.")
    else:
        msp2 = msp.copy()
        msp2["YEAR"] = pd.to_numeric(msp2.get("YEAR"), errors="coerce").astype("Int64")
        msp2["PI"] = pd.to_numeric(msp2.get("PI"), errors="coerce").astype("Int64")
        msp2 = msp2[msp2["PI"].notna() & msp2["PI"].isin(pi_nums)].copy()
        msp2["GROUPNAME"] = msp2.get("GROUPNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
        y2 = pd.to_numeric(msp2.get("YEAR"), errors="coerce").astype("Int64")
        p2 = pd.to_numeric(msp2.get("PI"), errors="coerce").astype("Int64")
        msp2["PI_LABEL"] = (y2.astype(str) + " I" + p2.astype(str)).where(y2.notna() & p2.notna(), "")
        msp2["PI_ORDER"] = (y2 * 10 + p2).astype("Int64")

        agg = msp2.groupby(["PI_ORDER", "PI_LABEL", "GROUPNAME"], dropna=False)["MSP_COST_TOTAL"].sum().reset_index().sort_values("PI_ORDER")
        x = agg["PI_LABEL"].astype(str).tolist()
        top_groups = (
            agg.groupby("GROUPNAME", dropna=False)["MSP_COST_TOTAL"].sum().reset_index().sort_values("MSP_COST_TOTAL", ascending=False).head(10)["GROUPNAME"].tolist()
        )
        agg["GROUP_SERIES"] = agg["GROUPNAME"].where(agg["GROUPNAME"].isin(top_groups), other="Other")
        pivot = (
            agg.groupby(["PI_LABEL", "GROUP_SERIES"], dropna=False)["MSP_COST_TOTAL"]
            .sum()
            .reset_index()
            .pivot_table(index="PI_LABEL", columns="GROUP_SERIES", values="MSP_COST_TOTAL", fill_value=0.0, aggfunc="sum")
        )
        series = []
        series_order = [g for g in top_groups if g in pivot.columns] + (["Other"] if "Other" in pivot.columns else [])
        for gname in series_order:
            vals = pd.to_numeric(pivot[gname], errors="coerce").fillna(0.0).astype(float).round(0).tolist()
            series.append({"name": gname, "type": "bar", "stack": "msp", "data": vals, "itemStyle": {"color": _pick_color(gname)}})
        opt = {
            "title": {"text": "MSP cost by increment (Top app groups)", "left": "center"},
            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
            "legend": {"top": 26},
            "grid": {"left": 70, "right": 30, "top": 70, "bottom": 70},
            "xAxis": {"type": "category", "data": x, "axisLabel": {"rotate": 25}},
            "yAxis": {"type": "value", "axisLabel": {"formatter": "${value}"}},
            "series": series,
        }
        render_echart(opt, height="380px", key=f"budget_msp_{scope_key}", page_theme=page_theme)


if False:  # Tab removed: Changes by PI
    st.subheader("Changes by PI / Iteration")

    if not ENABLE_COST_EVENTS:
        st.caption("Events are disabled; this tab shows planned baseline vs expected spend by PI.")

        base_pi = pd.DataFrame(columns=["YEAR", "PI", "BASELINE_TOTAL", "BASELINE_WF", "BASELINE_NWF"])
        try:
            b = baseline_cost_lines.copy() if baseline_cost_lines is not None else pd.DataFrame()
            if not b.empty:
                b["YEAR"] = pd.to_numeric(b.get("YEAR"), errors="coerce").astype("Int64")
                b["PI"] = pd.to_numeric(b.get("PI"), errors="coerce").astype("Int64")
                b["AMOUNT"] = pd.to_numeric(b.get("AMOUNT"), errors="coerce").fillna(0.0)
                if "CAT" not in b.columns:
                    b["CAT"] = ""
                b["__CAT"] = b["CAT"].astype(str).fillna("")
                g = b.groupby(["YEAR", "PI"], dropna=False)
                base_pi = g["AMOUNT"].sum().reset_index().rename(columns={"AMOUNT": "BASELINE_TOTAL"})
                baseline_split = (
                    b.pivot_table(
                        index=["YEAR", "PI"],
                        columns="__CAT",
                        values="AMOUNT",
                        aggfunc="sum",
                        fill_value=0.0,
                    )
                    .reset_index()
                )
                baseline_split["BASELINE_WF"] = pd.to_numeric(baseline_split.get("WORK_FORCE", 0.0), errors="coerce").fillna(0.0)
                baseline_split["BASELINE_NWF"] = pd.to_numeric(baseline_split.get("NON_WORK_FORCE", 0.0), errors="coerce").fillna(0.0)
                base_pi = base_pi.merge(
                    baseline_split[["YEAR", "PI", "BASELINE_WF", "BASELINE_NWF"]],
                    on=["YEAR", "PI"],
                    how="left",
                )
        except Exception:
            base_pi = pd.DataFrame(columns=["YEAR", "PI", "BASELINE_TOTAL", "BASELINE_WF", "BASELINE_NWF"])

        exp_pi = pd.DataFrame(columns=["YEAR", "PI", "EXPECTED_TOTAL"])
        try:
            if work_scope is not None and not work_scope.empty:
                w = work_scope.copy()
                w["YEAR"] = pd.to_numeric(w.get("YEAR"), errors="coerce").astype("Int64")
                w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce").astype("Int64")
                w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)
                exp_pi = w.groupby(["YEAR", "PI"], dropna=False)["AMOUNT"].sum().reset_index().rename(columns={"AMOUNT": "EXPECTED_TOTAL"})
        except Exception:
            exp_pi = pd.DataFrame(columns=["YEAR", "PI", "EXPECTED_TOTAL"])

        merged = base_pi.merge(exp_pi, on=["YEAR", "PI"], how="outer")
        for c in ["BASELINE_TOTAL", "BASELINE_WF", "BASELINE_NWF", "EXPECTED_TOTAL"]:
            merged[c] = pd.to_numeric(merged.get(c), errors="coerce").fillna(0.0)
        merged["DELTA_EXPECTED_MINUS_BASELINE"] = merged["EXPECTED_TOTAL"] - merged["BASELINE_TOTAL"]
        merged = merged.sort_values(["YEAR", "PI"], na_position="last")
        show = merged.copy()
        for c in ["BASELINE_TOTAL", "BASELINE_WF", "BASELINE_NWF", "EXPECTED_TOTAL", "DELTA_EXPECTED_MINUS_BASELINE"]:
            show[c] = show[c].map(lambda v: _fmt_money(float(v or 0.0), 0))
        st.dataframe(show, use_container_width=True, hide_index=True, height=420)
    else:
        st.info("Events are enabled, but this view is currently not maintained.")


# Data quality diagnostics using canonical cost API for Baseline budget
with st.expander("Data quality – unassigned Baseline (budget) costs", expanded=False):
    filters_unassigned: dict[str, object] = {}
    try:
        filters_unassigned = {
            k: v
            for k, v in (filters_baseline_canon or {}).items()
            if k in {"year", "pi", "program", "team", "app_group", "location"}
        }
    except Exception:
        filters_unassigned = {}

    df_unassigned = get_unassigned_breakdown(baseline_cost_lines, scenario=scenario, filters=filters_unassigned or None)
    if df_unassigned is None or df_unassigned.empty:
        st.caption("No unassigned Baseline costs found for the current filters.")
    else:
        grouping_cols = [c for c in ["ISSUE", "SOURCE", "COST_CATEGORY", "SUBCOMPONENT", "YEAR", "PI"] if c in df_unassigned.columns]
        value_cols = [c for c in ["RECORDS", "TOTAL_COST"] if c in df_unassigned.columns]
        if grouping_cols and value_cols:
            summary = (
                df_unassigned.groupby(grouping_cols, dropna=False, as_index=False)[value_cols]
                .sum()
            )
            if "TOTAL_COST" in summary.columns:
                summary["TOTAL_COST"] = pd.to_numeric(summary.get("TOTAL_COST"), errors="coerce").fillna(0.0)
                summary = summary.sort_values("TOTAL_COST", ascending=False)
            st.dataframe(summary, use_container_width=True, hide_index=True)
        else:
            st.dataframe(df_unassigned, use_container_width=True, hide_index=True)

        st.caption(
            "These represent Baseline costs that are not fully mapped (e.g., missing program, app group, or PI). "
            "Fix these mappings in Master Data / Budget configuration to reduce “Unassigned” across the app."
        )

    st.divider()
    st.caption("Canonical Baseline totals by app group (for quick validation of attribution and unassigned buckets).")
    try:
        df_groups = get_app_group_costs(baseline_cost_lines, scenario=scenario, filters=filters_unassigned or None)
        if df_groups is None or df_groups.empty:
            st.caption("No app-group Baseline rows returned for the current filters.")
        else:
            g = df_groups.copy()
            g["GROUPNAME"] = g.get("GROUPNAME", "").fillna("").astype(str).str.strip()
            g["TOTAL_COST"] = pd.to_numeric(g.get("TOTAL_COST"), errors="coerce").fillna(0.0)
            top = (
                g.groupby("GROUPNAME", dropna=False)["TOTAL_COST"]
                .sum()
                .reset_index()
                .sort_values("TOTAL_COST", ascending=False)
                .head(25)
            )
            top["TOTAL_COST"] = top["TOTAL_COST"].map(lambda v: _fmt_money(float(v or 0.0), 0))
            st.dataframe(top.rename(columns={"GROUPNAME": "Application", "TOTAL_COST": "Baseline"}), use_container_width=True, hide_index=True)
    except Exception as e:
        st.caption(f"App-group Baseline preview unavailable: {e}")


if show_debug:
    with st.expander("Debug: KPI consistency", expanded=False):
        base_sum = float(pd.to_numeric(baseline_cost_lines.get("AMOUNT"), errors="coerce").fillna(0.0).sum() or 0.0) if baseline_cost_lines is not None and not baseline_cost_lines.empty else 0.0
        exp_sum = float(pd.to_numeric(work_scope.get("AMOUNT"), errors="coerce").fillna(0.0).sum() or 0.0) if work_scope is not None and not work_scope.empty else 0.0
        d_base = float(baseline_total_financial - base_sum)
        d_exp = float(expected_total - exp_sum)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Baseline KPI total", _fmt_money(baseline_total_financial, 0))
        c2.metric("Baseline (sum of baseline frame)", _fmt_money(base_sum, 0))
        c3.metric("Expected KPI total", _fmt_money(expected_total, 0))
        c4.metric("Expected (sum of expected frame)", _fmt_money(exp_sum, 0))

        tol = 0.01
        if abs(d_base) > tol or abs(d_exp) > tol:
            st.warning(
                f"Mismatch detected (Baseline Δ={d_base:,.2f}, Expected Δ={d_exp:,.2f}). "
                "KPI cards, charts, and tables should all read from the same scoped canonical frames."
            )
        else:
            st.caption("KPI cards reconcile with the scoped canonical frames used throughout this page (Δ < $0.01).")

    with st.expander("Debug: freshness token", expanded=False):
        dv = {}
        try:
            dv = get_data_version_info() or {}
        except Exception:
            dv = {}
        st.write(f"DATA_VERSION: {int(dv.get('version') or 0)}")
        st.write(f"Freshness token: {get_data_freshness_token()}")
        st.write(f"Session _cache_epoch: {int(st.session_state.get('_cache_epoch', 0) or 0)}")
        st.write(f"Freshness fetched at (UTC): {dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}")

    with st.expander("Debug: recurring invoice visibility", expanded=False):
        probe_team = st.text_input("Probe team contains", value="ERNE WEST", key="budget_dbg_probe_team")
        probe_app = st.text_input("Probe app/group contains", value="HUVR", key="budget_dbg_probe_app")
        st.write(
            {
                "years": [int(y) for y in (years_sel or [])],
                "pis": [int(p) for p in (pi_nums or [])],
                "effective_programs": list(effective_programs or []),
                "effective_teams": list(effective_teams or []),
                "effective_groups": list(effective_groups or []),
            }
        )

        try:
            b = baseline_cost_lines.copy() if isinstance(baseline_cost_lines, pd.DataFrame) else pd.DataFrame()
            if b.empty:
                st.caption("`baseline_cost_lines` is empty.")
            else:
                b["AMOUNT"] = pd.to_numeric(b.get("AMOUNT"), errors="coerce").fillna(0.0)
                src_u = b.get("SOURCE", "").astype(str).str.upper()
                sub_u = b.get("SUBCOMPONENT", "").astype(str).str.upper()
                team_u = b.get("TEAMNAME", "").astype(str).str.upper()
                grp_u = b.get("GROUPNAME", "").astype(str).str.upper()
                probe_team_u = str(probe_team or "").strip().upper()
                probe_app_u = str(probe_app or "").strip().upper()
                mask = src_u.str.contains("INVOICE") | sub_u.str.contains("INVOICE")
                if probe_team_u:
                    mask = mask & team_u.str.contains(probe_team_u, na=False)
                if probe_app_u:
                    mask = mask & grp_u.str.contains(probe_app_u, na=False)
                b_inv = b.loc[mask].copy()
                st.caption(f"Canonical baseline invoice rows matched: {len(b_inv):,}")
                if not b_inv.empty:
                    cols = [c for c in ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "SOURCE", "SUBCOMPONENT", "COST_CATEGORY", "AMOUNT"] if c in b_inv.columns]
                    st.dataframe(b_inv[cols], use_container_width=True, hide_index=True, height=220)
                    by_pi = b_inv.groupby(["YEAR", "PI"], dropna=False)["AMOUNT"].sum().reset_index()
                    st.dataframe(by_pi, use_container_width=True, hide_index=True, height=160)
        except Exception as e:
            st.caption(f"Canonical invoice trace failed: {e}")

        try:
            bl = baseline_long.copy() if isinstance(baseline_long, pd.DataFrame) else pd.DataFrame()
            if bl.empty:
                st.caption("`baseline_long` is empty.")
            else:
                bl["BASELINE_COST"] = pd.to_numeric(bl.get("BASELINE_COST"), errors="coerce").fillna(0.0)
                src_u2 = bl.get("SOURCE", "").astype(str).str.upper()
                sub_u2 = bl.get("SUBCOMPONENT", "").astype(str).str.upper()
                team_u2 = bl.get("TEAMNAME", "").astype(str).str.upper()
                res_u2 = bl.get("RESOURCE", "").astype(str).str.upper()
                probe_team_u = str(probe_team or "").strip().upper()
                probe_app_u = str(probe_app or "").strip().upper()
                mask2 = src_u2.str.contains("INVOICE") | sub_u2.str.contains("INVOICE")
                if probe_team_u:
                    mask2 = mask2 & team_u2.str.contains(probe_team_u, na=False)
                if probe_app_u:
                    mask2 = mask2 & (res_u2.str.contains(probe_app_u, na=False) | bl.get("TEAMNAME", "").astype(str).str.upper().str.contains(probe_app_u, na=False))
                bl_inv = bl.loc[mask2].copy()
                st.caption(f"Budget Explorer source invoice rows matched (`baseline_long`): {len(bl_inv):,}")
                if not bl_inv.empty:
                    cols2 = [c for c in ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "RESOURCE", "PROVIDER", "SOURCE", "SUBCOMPONENT", "COST_TYPE", "BASELINE_COST"] if c in bl_inv.columns]
                    st.dataframe(bl_inv[cols2], use_container_width=True, hide_index=True, height=220)
        except Exception as e:
            st.caption(f"Budget Explorer source trace failed: {e}")
