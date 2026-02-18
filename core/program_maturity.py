from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

try:
    from core.name_resolution import apply_display_scope_names
except Exception:  # pragma: no cover
    apply_display_scope_names = None  # type: ignore

EPSILON = 1e-9

UNMAPPED_GROUP_TOKENS = {
    "",
    "unassigned",
    "(unassigned)",
    "(needs mapping)",
    "(unmapped application)",
    "(unmapped app group)",
    "(blank)",
    "blank",
    "none",
    "null",
    "nan",
    "n/a",
    "na",
}


@dataclass(frozen=True)
class ProgramMaturityBundle:
    cost_baseline_df: pd.DataFrame
    cost_projected_df: pd.DataFrame
    cost_actual_df: pd.DataFrame
    ado_df: pd.DataFrame
    explorer_df: pd.DataFrame
    capacity_df: pd.DataFrame
    meta: dict[str, Any] = field(default_factory=dict)


def _safe_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _series_float(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df.get(col), errors="coerce").fillna(0.0)


def _series_text(df: pd.DataFrame, col: str) -> pd.Series:
    return df.get(col, "").fillna("").astype(str).str.strip()


def _apply_display_names(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    try:
        if apply_display_scope_names is None:
            return df
        return apply_display_scope_names(df)
    except Exception:
        return df


def _amount_col(df: pd.DataFrame) -> str:
    if "TOTAL_COST" in df.columns:
        return "TOTAL_COST"
    return "AMOUNT"


def _pi_col(df: pd.DataFrame) -> str:
    for col in ("PI", "PI_NUM", "ITERATION_NUM"):
        if col in df.columns:
            return col
    return "PI"


def _program_col(df: pd.DataFrame) -> str:
    for col in ("PROGRAMNAME", "PROGRAM", "program"):
        if col in df.columns:
            return col
    return "PROGRAMNAME"


def _is_unmapped_group(series: pd.Series) -> pd.Series:
    norm = series.fillna("").astype(str).str.strip().str.lower()
    return norm.isin(UNMAPPED_GROUP_TOKENS)


def _clamp(v: Any, lo: float = 0.0, hi: float = 100.0) -> float:
    try:
        x = float(v)
    except Exception:
        return lo
    if x != x:  # NaN
        return lo
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _score_from_abs_pct(abs_pct: Optional[float], default: float = 50.0) -> float:
    if abs_pct is None:
        return default
    try:
        x = float(abs_pct)
    except Exception:
        return default
    if x != x:
        return default
    return _clamp(100.0 - abs(x), 0.0, 100.0)


def _apply_program_filter(programs: list[str], filters: Optional[dict]) -> list[str]:
    if not filters:
        return programs
    requested = [str(v).strip() for v in (filters.get("program") or []) if str(v).strip()]
    if not requested:
        return programs
    req_u = {p.upper() for p in requested}
    return [p for p in programs if str(p).upper() in req_u]


def _resolve_financial_mode(bundle: ProgramMaturityBundle, financial_mode: Optional[str]) -> str:
    if financial_mode is not None and str(financial_mode).strip():
        mode = str(financial_mode).strip().lower()
    else:
        mode = str((bundle.meta or {}).get("financial_mode", "")).strip().lower()
    if mode not in {"forecast_only", "actual_based"}:
        return "forecast_only"
    return mode


def _build_program_metrics(bundle: ProgramMaturityBundle, *, include_pi: bool = False) -> pd.DataFrame:
    base = _apply_display_names(_safe_df(bundle.cost_baseline_df))
    proj = _apply_display_names(_safe_df(bundle.cost_projected_df))
    act = _apply_display_names(_safe_df(bundle.cost_actual_df))
    ado = _apply_display_names(_safe_df(bundle.ado_df))
    explorer = _apply_display_names(_safe_df(bundle.explorer_df))
    cap = _apply_display_names(_safe_df(bundle.capacity_df))

    for df in (base, proj, act):
        if df.empty:
            continue
        amt_col = _amount_col(df)
        df[amt_col] = _series_float(df, amt_col)
        df["PROGRAMNAME"] = _series_text(df, _program_col(df))
        df["COST_CATEGORY"] = _series_text(df, "COST_CATEGORY").str.upper()
        df["SUBCOMPONENT"] = _series_text(df, "SUBCOMPONENT")
        df["GROUPNAME"] = _series_text(df, "GROUPNAME")
        if include_pi:
            pi = _series_float(df, _pi_col(df)).astype(int)
            df["PI"] = pi

    if not explorer.empty:
        explorer["PROGRAMNAME"] = _series_text(explorer, _program_col(explorer))
        explorer["GROUPNAME"] = _series_text(explorer, "GROUPNAME")
        explorer["DERIVED_FTE_EXPLORER"] = _series_float(explorer, "DERIVED_FTE_EXPLORER")
        if "DERIVED_FTE_EXPLORER" not in explorer.columns or explorer["DERIVED_FTE_EXPLORER"].sum() == 0:
            explorer["DERIVED_FTE_EXPLORER"] = _series_float(explorer, "DERIVED_FTE")
        explorer["MAPPING_STATUS"] = _series_text(explorer, "MAPPING_STATUS").str.upper()
        if include_pi:
            explorer["PI"] = _series_float(explorer, _pi_col(explorer)).astype(int)

    if not ado.empty:
        ado["PROGRAMNAME"] = _series_text(ado, _program_col(ado))
        ado["STORY_POINTS"] = _series_float(ado, "STORY_POINTS")
        if include_pi:
            ado["PI"] = _series_float(ado, _pi_col(ado)).astype(int)

    if not cap.empty:
        cap["PROGRAMNAME"] = _series_text(cap, _program_col(cap))
        if "CAPACITY_SOD_FTE" in cap.columns:
            cap["CAPACITY_FTE"] = _series_float(cap, "CAPACITY_SOD_FTE")
        else:
            cap["CAPACITY_FTE"] = _series_float(cap, "CAPACITY_FTE")
        if include_pi:
            cap["PI"] = _series_float(cap, _pi_col(cap)).astype(int)

    key_cols = ["PROGRAMNAME"] + (["PI"] if include_pi else [])

    def _agg_cost(df: pd.DataFrame, scenario_name: str) -> pd.DataFrame:
        if df.empty:
            cols = key_cols + [f"{scenario_name}_total_cost", f"{scenario_name}_nwf_cost"]
            return pd.DataFrame(columns=cols)
        amt_col = _amount_col(df)
        by = df.groupby(key_cols, dropna=False)
        out = by[amt_col].sum().reset_index(name=f"{scenario_name}_total_cost")
        nwf = (
            df[df["COST_CATEGORY"].eq("NON_WORK_FORCE")]
            .groupby(key_cols, dropna=False)[amt_col]
            .sum()
            .reset_index(name=f"{scenario_name}_nwf_cost")
        )
        out = out.merge(nwf, on=key_cols, how="left")
        out[f"{scenario_name}_nwf_cost"] = _series_float(out, f"{scenario_name}_nwf_cost")
        return out

    base_agg = _agg_cost(base, "baseline")
    proj_agg = _agg_cost(proj, "projected")
    act_agg = _agg_cost(act, "actual")

    if proj.empty:
        proj_detail = pd.DataFrame(columns=key_cols + ["mapped_cost", "overhead_cost"])
    else:
        amt_col = _amount_col(proj)
        mapped_mask = ~_is_unmapped_group(proj["GROUPNAME"])
        overhead_mask = (
            proj["COST_CATEGORY"].eq("WORK_FORCE")
            & proj["SUBCOMPONENT"].str.upper().str.contains("PROGRAM", na=False)
        )
        proj_detail = pd.DataFrame({
            **{k: proj[k] for k in key_cols},
            "mapped_cost": proj[amt_col].where(mapped_mask, 0.0),
            "overhead_cost": proj[amt_col].where(overhead_mask, 0.0),
        })
        proj_detail = proj_detail.groupby(key_cols, dropna=False)[["mapped_cost", "overhead_cost"]].sum().reset_index()

    if explorer.empty:
        dem_agg = pd.DataFrame(columns=key_cols + ["derived_fte", "mapped_demand_fte", "unmapped_demand_fte"])
    else:
        mapped_status = explorer["MAPPING_STATUS"].eq("MAPPED")
        mapped_group = ~_is_unmapped_group(explorer["GROUPNAME"])
        mapped = mapped_status & mapped_group
        dem = pd.DataFrame({
            **{k: explorer[k] for k in key_cols},
            "derived_fte": explorer["DERIVED_FTE_EXPLORER"],
            "mapped_demand_fte": explorer["DERIVED_FTE_EXPLORER"].where(mapped, 0.0),
        })
        dem = dem.groupby(key_cols, dropna=False)[["derived_fte", "mapped_demand_fte"]].sum().reset_index()
        dem["unmapped_demand_fte"] = (dem["derived_fte"] - dem["mapped_demand_fte"]).clip(lower=0.0)
        dem_agg = dem

    if ado.empty:
        ado_agg = pd.DataFrame(columns=key_cols + ["story_points"])
    else:
        ado_agg = ado.groupby(key_cols, dropna=False)["STORY_POINTS"].sum().reset_index(name="story_points")

    if cap.empty:
        cap_agg = pd.DataFrame(columns=key_cols + ["planned_capacity_fte"])
    else:
        cap_agg = cap.groupby(key_cols, dropna=False)["CAPACITY_FTE"].sum().reset_index(name="planned_capacity_fte")

    all_frames = [base_agg, proj_agg, act_agg, proj_detail, dem_agg, ado_agg, cap_agg]
    out = None
    for fr in all_frames:
        if fr is None or fr.empty:
            continue
        out = fr if out is None else out.merge(fr, on=key_cols, how="outer")
    if out is None:
        return pd.DataFrame(columns=key_cols)

    for col in [
        "baseline_total_cost",
        "baseline_nwf_cost",
        "projected_total_cost",
        "projected_nwf_cost",
        "actual_total_cost",
        "actual_nwf_cost",
        "mapped_cost",
        "overhead_cost",
        "derived_fte",
        "mapped_demand_fte",
        "unmapped_demand_fte",
        "story_points",
        "planned_capacity_fte",
    ]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = _series_float(out, col)

    out["PROGRAMNAME"] = _series_text(out, "PROGRAMNAME")
    if include_pi:
        out["PI"] = _series_float(out, "PI").astype(int)
    out = _apply_display_names(out)
    out["PROGRAMNAME"] = _series_text(out, "PROGRAMNAME")
    return out


def calculate_program_maturity(
    bundle: ProgramMaturityBundle,
    filters: Optional[dict] = None,
    financial_mode: Optional[str] = None,
) -> pd.DataFrame:
    metrics = _build_program_metrics(bundle, include_pi=False)
    mode = _resolve_financial_mode(bundle, financial_mode)
    if metrics.empty:
        return pd.DataFrame(
            columns=[
                "program",
                "financial_score",
                "transparency_score",
                "demand_score",
                "efficiency_score",
                "planning_score",
                "maturity_score",
                "variance_pct",
                "transparency_pct",
                "total_spend",
                "unmapped_effort_pct",
                "overhead_pct",
                "cost_per_story_point",
                "demand_capacity_deviation_pct",
                "financial_mode_used",
            ]
        )

    programs = sorted({p for p in metrics["PROGRAMNAME"].tolist() if str(p).strip()})
    programs = _apply_program_filter(programs, filters)
    metrics = metrics[metrics["PROGRAMNAME"].isin(set(programs))].copy()

    if mode == "actual_based":
        metrics["variance_pct"] = (
            (metrics["actual_nwf_cost"] - metrics["baseline_nwf_cost"])
            / (metrics["baseline_nwf_cost"].clip(lower=EPSILON))
        ) * 100.0
        metrics.loc[metrics["baseline_nwf_cost"] <= 0, "variance_pct"] = pd.NA
        metrics["stability_pct"] = (
            (metrics["actual_nwf_cost"] - metrics["projected_nwf_cost"])
            / (metrics["projected_nwf_cost"].clip(lower=EPSILON))
        ) * 100.0
        metrics.loc[metrics["projected_nwf_cost"] <= 0, "stability_pct"] = pd.NA
    else:
        metrics["variance_pct"] = (
            (metrics["projected_nwf_cost"] - metrics["baseline_nwf_cost"])
            / (metrics["baseline_nwf_cost"].clip(lower=EPSILON))
        ) * 100.0
        metrics.loc[metrics["baseline_nwf_cost"] <= 0, "variance_pct"] = pd.NA
        metrics["stability_pct"] = metrics["variance_pct"]

    metrics["mapped_cost_pct"] = (metrics["mapped_cost"] / metrics["projected_total_cost"].clip(lower=EPSILON) * 100.0)
    metrics.loc[metrics["projected_total_cost"] <= 0, "mapped_cost_pct"] = 0.0

    metrics["overhead_pct"] = (metrics["overhead_cost"] / metrics["projected_total_cost"].clip(lower=EPSILON) * 100.0)
    metrics.loc[metrics["projected_total_cost"] <= 0, "overhead_pct"] = 0.0

    metrics["mapped_demand_pct"] = (metrics["mapped_demand_fte"] / metrics["derived_fte"].clip(lower=EPSILON) * 100.0)
    metrics.loc[metrics["derived_fte"] <= 0, "mapped_demand_pct"] = 0.0
    metrics["unmapped_effort_pct"] = (metrics["unmapped_demand_fte"] / metrics["derived_fte"].clip(lower=EPSILON) * 100.0)
    metrics.loc[metrics["derived_fte"] <= 0, "unmapped_effort_pct"] = 0.0

    metrics["cost_per_story_point"] = metrics["projected_total_cost"] / metrics["story_points"].clip(lower=EPSILON)
    metrics.loc[metrics["story_points"] <= 0, "cost_per_story_point"] = pd.NA

    metrics["demand_capacity_deviation_pct"] = (
        (metrics["derived_fte"] - metrics["planned_capacity_fte"]).abs() / metrics["planned_capacity_fte"].clip(lower=EPSILON)
    ) * 100.0
    metrics.loc[(metrics["planned_capacity_fte"] <= 0) & (metrics["derived_fte"] <= 0), "demand_capacity_deviation_pct"] = 0.0
    metrics.loc[(metrics["planned_capacity_fte"] <= 0) & (metrics["derived_fte"] > 0), "demand_capacity_deviation_pct"] = 100.0

    cpsp = pd.to_numeric(metrics["cost_per_story_point"], errors="coerce")
    cpsp_valid = cpsp[cpsp.notna()]
    if cpsp_valid.empty:
        metrics["cost_efficiency_score"] = 50.0
        cpsp_min = pd.NA
        cpsp_max = pd.NA
    else:
        cpsp_min = float(cpsp_valid.min())
        cpsp_max = float(cpsp_valid.max())
        if abs(cpsp_max - cpsp_min) <= EPSILON:
            metrics["cost_efficiency_score"] = 50.0
        else:
            norm = (cpsp - cpsp_min) / (cpsp_max - cpsp_min)
            metrics["cost_efficiency_score"] = (1.0 - norm) * 100.0
            metrics["cost_efficiency_score"] = metrics["cost_efficiency_score"].fillna(50.0)

    metrics["deviation_score"] = 100.0 - metrics["demand_capacity_deviation_pct"].clip(lower=0.0, upper=100.0)

    metrics["financial_score"] = (
        0.7 * metrics["variance_pct"].apply(lambda v: _score_from_abs_pct(v, default=50.0))
        + 0.3 * metrics["stability_pct"].apply(lambda v: _score_from_abs_pct(v, default=50.0))
    )
    metrics["transparency_score"] = 0.7 * metrics["mapped_cost_pct"].clip(0.0, 100.0) + 0.3 * (100.0 - metrics["overhead_pct"].clip(0.0, 100.0))
    metrics["demand_score"] = 0.7 * metrics["mapped_demand_pct"].clip(0.0, 100.0) + 0.3 * (100.0 - metrics["unmapped_effort_pct"].clip(0.0, 100.0))
    metrics["efficiency_score"] = 0.6 * metrics["cost_efficiency_score"].clip(0.0, 100.0) + 0.4 * metrics["deviation_score"].clip(0.0, 100.0)

    metrics["baseline_configured"] = ((metrics["baseline_nwf_cost"] > 0) | (metrics["baseline_total_cost"] > 0)).astype(float)
    metrics["headcount_configured"] = (metrics["planned_capacity_fte"] > 0).astype(float)
    metrics["rates_configured"] = (metrics["projected_total_cost"] > 0).astype(float)
    metrics["planning_score"] = (
        (metrics["baseline_configured"] + metrics["headcount_configured"] + metrics["rates_configured"]) / 3.0
    ) * 100.0

    metrics["maturity_score"] = (
        0.30 * metrics["financial_score"]
        + 0.20 * metrics["transparency_score"]
        + 0.20 * metrics["demand_score"]
        + 0.20 * metrics["efficiency_score"]
        + 0.10 * metrics["planning_score"]
    )

    for col in [
        "financial_score",
        "transparency_score",
        "demand_score",
        "efficiency_score",
        "planning_score",
        "maturity_score",
        "mapped_cost_pct",
        "mapped_demand_pct",
        "unmapped_effort_pct",
        "overhead_pct",
        "demand_capacity_deviation_pct",
    ]:
        metrics[col] = metrics[col].apply(_clamp)

    metrics["transparency_pct"] = (0.5 * metrics["mapped_demand_pct"] + 0.5 * metrics["mapped_cost_pct"]).clip(0.0, 100.0)
    metrics["total_spend"] = metrics["projected_total_cost"].clip(lower=0.0)

    metrics["cpsp_min_portfolio"] = cpsp_min
    metrics["cpsp_max_portfolio"] = cpsp_max
    metrics["financial_mode_used"] = mode

    out = metrics.rename(columns={"PROGRAMNAME": "program"})
    out = out.sort_values(["maturity_score", "total_spend"], ascending=[False, False])
    return out[
        [
            "program",
            "financial_score",
            "transparency_score",
            "demand_score",
            "efficiency_score",
            "planning_score",
            "maturity_score",
            "variance_pct",
            "transparency_pct",
            "total_spend",
            "unmapped_effort_pct",
            "overhead_pct",
            "cost_per_story_point",
            "demand_capacity_deviation_pct",
            "mapped_cost_pct",
            "mapped_demand_pct",
            "cost_efficiency_score",
            "deviation_score",
            "cpsp_min_portfolio",
            "cpsp_max_portfolio",
            "baseline_configured",
            "headcount_configured",
            "rates_configured",
            "financial_mode_used",
        ]
    ].reset_index(drop=True)


def calculate_program_positioning_maturity(
    app_group_position_df: pd.DataFrame,
    *,
    spend_col: str = "ACTUAL_COST",
) -> pd.DataFrame:
    empty = pd.DataFrame(
        columns=[
            "program",
            "weighted_value_avg",
            "weighted_demand_avg",
            "investment_pct",
            "maintain_pct",
            "reassess_pct",
            "chaos_pct",
            "maturity_score",
            "total_spend",
            "app_group_count",
        ]
    )
    if app_group_position_df is None or not isinstance(app_group_position_df, pd.DataFrame) or app_group_position_df.empty:
        return empty

    req_cols = {"PROGRAMNAME", "GROUPNAME", "DEMAND_AXIS", "Y_VALUE_PLOT", spend_col, "quadrant_label"}
    if not req_cols.issubset(set(app_group_position_df.columns)):
        return empty

    w = app_group_position_df.copy()
    w["PROGRAMNAME"] = _series_text(w, "PROGRAMNAME")
    w["GROUPNAME"] = _series_text(w, "GROUPNAME")
    w["DEMAND_AXIS"] = _series_float(w, "DEMAND_AXIS")
    w["Y_VALUE_PLOT"] = _series_float(w, "Y_VALUE_PLOT")
    w[spend_col] = _series_float(w, spend_col)
    w["quadrant_label"] = _series_text(w, "quadrant_label")

    w = w[w["PROGRAMNAME"].ne("") & w["GROUPNAME"].ne("") & (w[spend_col] > 0)].copy()
    if w.empty:
        return empty

    agg_rows: list[dict[str, Any]] = []
    for program, grp in w.groupby("PROGRAMNAME", dropna=False):
        g = grp.copy()
        total_spend = float(_series_float(g, spend_col).sum())
        if total_spend <= 0:
            continue

        demand_avg = float((g["DEMAND_AXIS"] * g[spend_col]).sum() / total_spend)
        value_avg = float((g["Y_VALUE_PLOT"] * g[spend_col]).sum() / total_spend)

        by_quadrant = g.groupby("quadrant_label", dropna=False)[spend_col].sum()
        invest_share = float(by_quadrant.get("Invest", 0.0)) / total_spend
        maintain_share = float(by_quadrant.get("Optimize", 0.0)) / total_spend
        reassess_share = float(by_quadrant.get("Monitor", 0.0)) / total_spend
        chaos_share = float(by_quadrant.get("Reassess", 0.0)) / total_spend

        raw_score = (
            1.0 * invest_share
            + 0.7 * maintain_share
            - 0.7 * reassess_share
            - 1.0 * chaos_share
        )
        maturity_score = _clamp(100.0 * raw_score, 0.0, 100.0)

        agg_rows.append(
            {
                "program": str(program),
                "weighted_value_avg": value_avg,
                "weighted_demand_avg": demand_avg,
                "investment_pct": _clamp(invest_share * 100.0, 0.0, 100.0),
                "maintain_pct": _clamp(maintain_share * 100.0, 0.0, 100.0),
                "reassess_pct": _clamp(reassess_share * 100.0, 0.0, 100.0),
                "chaos_pct": _clamp(chaos_share * 100.0, 0.0, 100.0),
                "maturity_score": maturity_score,
                "total_spend": max(0.0, total_spend),
                "app_group_count": int(g["GROUPNAME"].nunique()),
            }
        )

    if not agg_rows:
        return empty
    out = pd.DataFrame(agg_rows)
    out = out.sort_values(["maturity_score", "total_spend"], ascending=[False, False]).reset_index(drop=True)
    return out


def calculate_program_maturity_trend(bundle: ProgramMaturityBundle) -> pd.DataFrame:
    per_pi = _build_program_metrics(bundle, include_pi=True)
    if per_pi.empty:
        return pd.DataFrame(
            columns=[
                "program",
                "latest_pi",
                "previous_pi",
                "latest_efficiency_score",
                "previous_efficiency_score",
                "efficiency_delta",
            ]
        )

    per_pi["cost_per_story_point"] = per_pi["projected_total_cost"] / per_pi["story_points"].clip(lower=EPSILON)
    per_pi.loc[per_pi["story_points"] <= 0, "cost_per_story_point"] = pd.NA

    cpsp = pd.to_numeric(per_pi["cost_per_story_point"], errors="coerce")
    valid = cpsp[cpsp.notna()]
    if valid.empty:
        per_pi["cost_efficiency_score"] = 50.0
    else:
        vmin = float(valid.min())
        vmax = float(valid.max())
        if abs(vmax - vmin) <= EPSILON:
            per_pi["cost_efficiency_score"] = 50.0
        else:
            per_pi["cost_efficiency_score"] = (1.0 - ((cpsp - vmin) / (vmax - vmin))) * 100.0
            per_pi["cost_efficiency_score"] = per_pi["cost_efficiency_score"].fillna(50.0)

    per_pi["demand_capacity_deviation_pct"] = (
        (per_pi["derived_fte"] - per_pi["planned_capacity_fte"]).abs() / per_pi["planned_capacity_fte"].clip(lower=EPSILON)
    ) * 100.0
    per_pi.loc[(per_pi["planned_capacity_fte"] <= 0) & (per_pi["derived_fte"] <= 0), "demand_capacity_deviation_pct"] = 0.0
    per_pi.loc[(per_pi["planned_capacity_fte"] <= 0) & (per_pi["derived_fte"] > 0), "demand_capacity_deviation_pct"] = 100.0

    per_pi["efficiency_score"] = 0.6 * per_pi["cost_efficiency_score"].clip(0.0, 100.0) + 0.4 * (100.0 - per_pi["demand_capacity_deviation_pct"].clip(0.0, 100.0))

    rows: list[dict[str, Any]] = []
    for program, grp in per_pi.groupby("PROGRAMNAME", dropna=False):
        w = grp.copy()
        w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce").fillna(0).astype(int)
        w = w[w["PI"] > 0].sort_values("PI")
        if w.empty:
            continue
        latest = w.iloc[-1]
        prev = w.iloc[-2] if len(w.index) >= 2 else None
        rows.append(
            {
                "program": str(program),
                "latest_pi": int(latest["PI"]),
                "previous_pi": int(prev["PI"]) if prev is not None else pd.NA,
                "latest_efficiency_score": float(latest["efficiency_score"]),
                "previous_efficiency_score": float(prev["efficiency_score"]) if prev is not None else pd.NA,
                "efficiency_delta": (
                    float(latest["efficiency_score"]) - float(prev["efficiency_score"]) if prev is not None else pd.NA
                ),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["latest_efficiency_score"] = _series_float(out, "latest_efficiency_score")
    return out


def build_maturity_risk_signals(maturity_df: pd.DataFrame, trend_df: pd.DataFrame) -> pd.DataFrame:
    m = _safe_df(maturity_df)
    t = _safe_df(trend_df)
    if m.empty:
        return pd.DataFrame(columns=["program", "severity", "signal", "value", "threshold", "rule_key"])

    m["program"] = _series_text(m, "program")
    m["variance_pct_abs"] = _series_float(m, "variance_pct").abs()
    m["unmapped_effort_pct"] = _series_float(m, "unmapped_effort_pct")
    m["overhead_pct"] = _series_float(m, "overhead_pct")
    m["transparency_pct"] = _series_float(m, "transparency_pct")

    t = t.copy()
    if not t.empty:
        t["program"] = _series_text(t, "program")
        t["efficiency_delta"] = pd.to_numeric(t.get("efficiency_delta"), errors="coerce")
    trend_map = {
        str(r["program"]): r["efficiency_delta"]
        for _, r in t.iterrows()
        if str(r.get("program", "")).strip()
    }

    portfolio_transparency_avg = float(m["transparency_pct"].mean()) if not m.empty else 0.0

    rows: list[dict[str, Any]] = []
    for _, r in m.iterrows():
        program = str(r["program"] or "").strip()
        if not program:
            continue

        variance_abs = float(r["variance_pct_abs"])
        unmapped = float(r["unmapped_effort_pct"])
        overhead = float(r["overhead_pct"])
        transparency = float(r["transparency_pct"])
        eff_delta = trend_map.get(program)

        if variance_abs > 20:
            rows.append({"program": program, "severity": "Red", "signal": "Variance above threshold", "value": variance_abs, "threshold": 20.0, "rule_key": "variance_gt_20"})
        if unmapped > 15:
            rows.append({"program": program, "severity": "Red", "signal": "Unmapped effort above threshold", "value": unmapped, "threshold": 15.0, "rule_key": "unmapped_gt_15"})
        if overhead > 30:
            rows.append({"program": program, "severity": "Red", "signal": "Overhead above threshold", "value": overhead, "threshold": 30.0, "rule_key": "overhead_gt_30"})

        if eff_delta is not None and eff_delta == eff_delta and float(eff_delta) <= -5.0:
            rows.append({"program": program, "severity": "Yellow", "signal": "Efficiency declining", "value": float(eff_delta), "threshold": -5.0, "rule_key": "eff_delta_lte_-5"})
        if transparency < portfolio_transparency_avg:
            rows.append({"program": program, "severity": "Yellow", "signal": "Transparency below portfolio average", "value": transparency, "threshold": portfolio_transparency_avg, "rule_key": "transparency_lt_portfolio_avg"})

        if variance_abs < 5:
            rows.append({"program": program, "severity": "Green", "signal": "Variance controlled", "value": variance_abs, "threshold": 5.0, "rule_key": "variance_lt_5"})
        if transparency > 90:
            rows.append({"program": program, "severity": "Green", "signal": "High transparency", "value": transparency, "threshold": 90.0, "rule_key": "transparency_gt_90"})

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["program", "severity", "signal", "value", "threshold", "rule_key"])
    out["severity_rank"] = out["severity"].map({"Red": 0, "Yellow": 1, "Green": 2}).fillna(3)
    out = out.sort_values(["severity_rank", "program", "signal"], ascending=[True, True, True]).drop(columns=["severity_rank"])
    return out.reset_index(drop=True)
