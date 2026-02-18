# Summary: update headline source labels to NEXT branding.
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd

from core.data import fetch_pi_calendar_resolved_dates
from core.insights_driver import resolve_driver_dimension
from core.scope import PreviewScope


@dataclass(frozen=True)
class SnapshotMetrics:
    plan_ytd: float
    forecast_ytd: float
    actual_ytd: Optional[float]
    coverage_pct: float
    assigned_pct: float
    delta_pct: float
    top_driver_name: Optional[str]
    top_driver_share: float


@dataclass(frozen=True)
class Headline:
    text: str
    mode: str
    tone: str
    driver_dim: str
    driver_name: Optional[str]
    driver_share: float
    sources: list[str]


def _safe_ratio(num: float, den: float) -> float:
    try:
        n = float(num)
        d = float(den)
    except Exception:
        return 0.0
    if d == 0:
        return 0.0
    return n / d


def _safe_pct(num: float, den: float) -> float:
    r = _safe_ratio(num, den)
    if r < 0:
        return 0.0
    if r > 1:
        return 1.0
    return r


def _norm_series(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.upper().str.strip()


def _to_int_series_compat(x: Any, default: int = 0):
    """Convert to an integer dtype safely across pandas versions.

    Prefers pandas' nullable Int64 when available; falls back to numpy int64.
    """
    s = pd.to_numeric(x, errors="coerce")
    if not isinstance(s, pd.Series):
        try:
            return int(default) if pd.isna(s) else int(s)
        except Exception:
            return int(default)
    try:
        int64_nullable = pd.Int64Dtype()  # type: ignore[attr-defined]
        return s.astype(int64_nullable)
    except Exception:
        return s.fillna(default).astype("int64")


def _normalize_year_pi_amount(df_lines: pd.DataFrame, *, selected_year: int) -> pd.DataFrame:
    work = df_lines.copy() if df_lines is not None else pd.DataFrame()
    work["YEAR"] = pd.to_numeric(work.get("YEAR"), errors="coerce").fillna(int(selected_year)).astype(int)
    work = work.loc[work["YEAR"].eq(int(selected_year))].copy()
    if "PI" not in work.columns:
        work["PI"] = pd.NA
    work["PI"] = _to_int_series_compat(work.get("PI"))
    work["AMOUNT"] = pd.to_numeric(work.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    if "SCENARIO" not in work.columns:
        work["SCENARIO"] = ""
    work["SCENARIO"] = _norm_series(work["SCENARIO"])
    work["SCENARIO_BUCKET"] = work["SCENARIO"].map(_scenario_bucket)

    id_col = next((c for c in ("COST_LINE_ID", "LINE_ID", "UUID", "ID") if c in work.columns), None)
    if id_col:
        work = work.drop_duplicates(subset=["SCENARIO_BUCKET", id_col])
    else:
        subset_cols = [
            c
            for c in [
                "SCENARIO_BUCKET",
                "YEAR",
                "PI",
                "PROGRAMNAME",
                "TEAMNAME",
                "GROUPNAME",
                "COST_CATEGORY",
                "SUBCOMPONENT",
                "SOURCE",
                "AMOUNT",
            ]
            if c in work.columns
        ]
        if subset_cols:
            work = work.drop_duplicates(subset=subset_cols)
    return work


def _apply_scope_filters(df: pd.DataFrame, *, scope_dict: dict) -> pd.DataFrame:
    work = df.copy()
    programs = [str(p).strip() for p in (scope_dict or {}).get("programs", []) if str(p).strip()]
    teams = [str(t).strip() for t in (scope_dict or {}).get("teams", []) if str(t).strip()]
    groups = [str(g).strip() for g in (scope_dict or {}).get("groups", []) if str(g).strip()]

    if programs and "PROGRAMNAME" in work.columns:
        prog = work["PROGRAMNAME"].fillna("").astype(str).str.strip()
        work = work.loc[prog.isin(set(programs))].copy()
    if teams and "TEAMNAME" in work.columns:
        team = work["TEAMNAME"].fillna("").astype(str).str.strip()
        work = work.loc[team.isin(set(teams))].copy()
    if groups and "GROUPNAME" in work.columns:
        grp = work["GROUPNAME"].fillna("").astype(str).str.strip()
        keep_program_level = grp.eq("") | grp.str.upper().eq("(UNASSIGNED)")
        work = work.loc[keep_program_level | grp.isin(set(groups))].copy()
    return work


def _scenario_bucket(raw: Any) -> str:
    s = str(raw or "").strip().upper()
    if s.startswith("BASE"):
        return "BASELINE"
    if s.startswith("ACT"):
        return "ACTUAL"
    if s.startswith("EXP") or s.startswith("PROJ") or s.startswith("FORE"):
        return "EXPECTED"
    return "EXPECTED" if s else ""


def _attach_calendar_and_prorate(work: pd.DataFrame, *, selected_year: int, as_of: dt.date) -> pd.DataFrame:
    out = work.copy()
    out["AMOUNT_YTD"] = 0.0

    has_pi = out.get("PI")
    if has_pi is None:
        return out
    out = out.loc[out["PI"].notna()].copy()
    if out.empty:
        return out
    out["PI_INT"] = _to_int_series_compat(out.get("PI"))
    out = out.loc[out["PI_INT"].notna()].copy()
    if out.empty:
        return out
    out["PI_INT"] = out["PI_INT"].astype(int)

    is_current_year = int(selected_year) == int(as_of.year)
    if not is_current_year:
        out["AMOUNT_YTD"] = out["AMOUNT"]
        return out

    cal = fetch_pi_calendar_resolved_dates(years=(int(selected_year),))
    if cal is None or cal.empty:
        out["AMOUNT_YTD"] = 0.0
        return out

    calw = cal.copy()
    calw["CAL_YEAR"] = _to_int_series_compat(calw.get("CAL_YEAR"))
    calw = calw.loc[calw["CAL_YEAR"].eq(int(selected_year))].copy()
    calw["CAL_ITERATION_LEVEL3"] = calw.get("CAL_ITERATION_LEVEL3", "").fillna("").astype(str)
    calw["CAL_L3_NORM"] = (
        calw["CAL_ITERATION_LEVEL3"].astype(str).str.upper().str.strip().str.replace(r"\s+", " ", regex=True)
    )
    calw["CAL_START_DATE"] = pd.to_datetime(calw.get("CAL_START_DATE"), errors="coerce")
    calw["CAL_END_DATE"] = pd.to_datetime(calw.get("CAL_END_DATE"), errors="coerce")
    calw = calw.loc[calw["CAL_END_DATE"].notna() & calw["CAL_L3_NORM"].ne("")].copy()
    if calw.empty:
        out["AMOUNT_YTD"] = 0.0
        return out

    out["ITER_L3_NORM"] = (
        (out["YEAR"].astype(int).astype(str) + " I" + out["PI_INT"].astype(int).astype(str))
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    out = out.merge(
        calw[["CAL_L3_NORM", "CAL_START_DATE", "CAL_END_DATE"]],
        left_on="ITER_L3_NORM",
        right_on="CAL_L3_NORM",
        how="left",
    )

    start = pd.to_datetime(out.get("CAL_START_DATE"), errors="coerce")
    end = pd.to_datetime(out.get("CAL_END_DATE"), errors="coerce")
    pi_days = (end - start).dt.days + 1
    elapsed = (pd.to_datetime(as_of) - start).dt.days + 1
    elapsed_clamped = elapsed.clip(lower=0)
    elapsed_clamped = elapsed_clamped.where(pi_days.notna(), 0).clip(upper=pi_days.fillna(0))
    proration = (elapsed_clamped / pi_days.replace({0: pd.NA})).fillna(0.0)
    proration = proration.where(start.notna() & end.notna(), 0.0).clip(0.0, 1.0)
    out["AMOUNT_YTD"] = out["AMOUNT"] * proration

    return out


def _sum_scenario(df: pd.DataFrame, *, scenario: str, amount_col: str) -> float:
    if df is None or df.empty:
        return 0.0
    scen = _norm_series(df.get("SCENARIO", pd.Series(dtype=str))).map(_scenario_bucket)
    w = df.loc[scen.eq(str(scenario).upper())].copy()
    return float(pd.to_numeric(w.get(amount_col), errors="coerce").fillna(0.0).sum())


def _pick_driver_dimension(df: pd.DataFrame, *, scope_dict: dict) -> Optional[str]:
    # Backwards-compatible default if role is not provided: use scope to pick a reasonable dimension.
    is_team_scope = bool((scope_dict or {}).get("teams"))
    if is_team_scope:
        return "GROUPNAME" if "GROUPNAME" in df.columns else None
    if "TEAMNAME" in df.columns and (df["TEAMNAME"].fillna("").astype(str).str.strip().ne("")).any():
        return "TEAMNAME"
    if "GROUPNAME" in df.columns and (df["GROUPNAME"].fillna("").astype(str).str.strip().ne("")).any():
        return "GROUPNAME"
    if "PROGRAMNAME" in df.columns and (df["PROGRAMNAME"].fillna("").astype(str).str.strip().ne("")).any():
        return "PROGRAMNAME"
    return None


def _column_for_driver_dim(driver_dim: str, df: pd.DataFrame) -> Optional[str]:
    dd = str(driver_dim or "").strip().upper()
    if dd == "APP_GROUP":
        return "GROUPNAME" if "GROUPNAME" in df.columns else None
    if dd == "TEAM":
        return "TEAMNAME" if "TEAMNAME" in df.columns else None
    if dd == "PROGRAM":
        return "PROGRAMNAME" if "PROGRAMNAME" in df.columns else None
    return None


def _top_driver(
    df_ytd: pd.DataFrame,
    *,
    scope_dict: dict,
    driver_dim: str,
) -> tuple[Optional[str], float]:
    dim = _column_for_driver_dim(driver_dim, df_ytd) or _pick_driver_dimension(df_ytd, scope_dict=scope_dict)
    if not dim or df_ytd is None or df_ytd.empty:
        return (None, 0.0)

    w = df_ytd.copy()
    w[dim] = w.get(dim, "").fillna("").astype(str).str.strip()
    w.loc[w[dim].eq(""), dim] = "(Unassigned)"

    scen = _norm_series(w.get("SCENARIO", pd.Series(dtype=str))).map(_scenario_bucket)
    w = w.loc[scen.isin(["BASELINE", "EXPECTED"])].copy()
    if w.empty:
        return (None, 0.0)
    w["SCEN"] = scen.loc[w.index].astype(str)
    sums = (
        w.groupby([dim, "SCEN"], dropna=False)["AMOUNT_YTD"]
        .sum()
        .unstack("SCEN", fill_value=0.0)
        .reset_index()
    )
    if "BASELINE" not in sums.columns:
        sums["BASELINE"] = 0.0
    if "EXPECTED" not in sums.columns:
        sums["EXPECTED"] = 0.0
    sums["DELTA"] = pd.to_numeric(sums["EXPECTED"], errors="coerce").fillna(0.0) - pd.to_numeric(sums["BASELINE"], errors="coerce").fillna(0.0)
    sums["ABS_DELTA"] = sums["DELTA"].abs()
    total_abs = float(pd.to_numeric(sums["ABS_DELTA"], errors="coerce").fillna(0.0).sum())
    if total_abs <= 0:
        return (None, 0.0)

    top = sums.sort_values("ABS_DELTA", ascending=False).head(1)
    if top.empty:
        return (None, 0.0)
    name = str(top.iloc[0][dim] or "").strip() or None
    abs_val = pd.to_numeric(pd.Series([top.iloc[0]["ABS_DELTA"]]), errors="coerce").iloc[0]
    abs_val = 0.0 if pd.isna(abs_val) else float(abs_val)
    share = abs_val / total_abs
    return (name, max(0.0, min(1.0, share)))


def build_snapshot_metrics(
    df_lines: pd.DataFrame,
    selected_year: int,
    as_of: dt.date,
    scope_dict: dict,
    driver_dim: str = "PROGRAM",
) -> SnapshotMetrics:
    work = _normalize_year_pi_amount(df_lines, selected_year=int(selected_year))
    work = _apply_scope_filters(work, scope_dict=scope_dict)
    ytd = _attach_calendar_and_prorate(work, selected_year=int(selected_year), as_of=as_of)

    plan_ytd = _sum_scenario(ytd, scenario="BASELINE", amount_col="AMOUNT_YTD")
    forecast_ytd = _sum_scenario(ytd, scenario="EXPECTED", amount_col="AMOUNT_YTD")
    actual_ytd_val = _sum_scenario(ytd, scenario="ACTUAL", amount_col="AMOUNT_YTD")
    actual_ytd = actual_ytd_val if actual_ytd_val > 0 else None

    delta_pct = _safe_ratio((forecast_ytd - plan_ytd), plan_ytd) if plan_ytd > 0 else 0.0

    # Coverage & assignment are assessed on the full-year Forecast (not prorated).
    scen_bucket = work.get("SCENARIO_BUCKET", pd.Series(dtype=str))
    df_forecast = work.loc[scen_bucket.eq("EXPECTED")].copy()
    forecast_total = float(pd.to_numeric(df_forecast.get("AMOUNT"), errors="coerce").fillna(0.0).sum()) if not df_forecast.empty else 0.0

    is_wf = pd.Series(False, index=df_forecast.index)
    if not df_forecast.empty:
        if "COST_BUCKET" in df_forecast.columns:
            is_wf = _norm_series(df_forecast["COST_BUCKET"]).eq("WF")
        elif "COST_CATEGORY" in df_forecast.columns:
            is_wf = _norm_series(df_forecast["COST_CATEGORY"]).eq("WORK_FORCE")

    fte_col = "FTE" if "FTE" in df_forecast.columns else None
    fte_vals = pd.to_numeric(df_forecast.get(fte_col), errors="coerce").fillna(0.0) if fte_col else pd.Series(0.0, index=df_forecast.index)
    has_fte = fte_vals > 0
    covered_mask = (~is_wf) | has_fte
    covered_amt = float(pd.to_numeric(df_forecast.loc[covered_mask].get("AMOUNT"), errors="coerce").fillna(0.0).sum()) if not df_forecast.empty else 0.0
    coverage_pct = _safe_pct(covered_amt, forecast_total) if forecast_total > 0 else 0.0

    grp = df_forecast.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    assigned_mask = grp.ne("") & (~grp.str.upper().isin({"(UNASSIGNED)", "UNASSIGNED", "(PROGRAM NWF)", "(NEEDS MAPPING)"}))
    assigned_amt = float(pd.to_numeric(df_forecast.loc[assigned_mask].get("AMOUNT"), errors="coerce").fillna(0.0).sum()) if not df_forecast.empty else 0.0
    assigned_pct = _safe_pct(assigned_amt, forecast_total) if forecast_total > 0 else 0.0

    top_name, top_share = _top_driver(ytd, scope_dict=scope_dict, driver_dim=driver_dim)

    return SnapshotMetrics(
        plan_ytd=float(plan_ytd),
        forecast_ytd=float(forecast_ytd),
        actual_ytd=actual_ytd,
        coverage_pct=float(coverage_pct),
        assigned_pct=float(assigned_pct),
        delta_pct=float(delta_pct),
        top_driver_name=top_name,
        top_driver_share=float(top_share),
    )


def build_headline(metrics: SnapshotMetrics, *, driver_dim: str, sources: list[str]) -> Headline:
    coverage_pct = float(metrics.coverage_pct or 0.0)
    assigned_pct = float(metrics.assigned_pct or 0.0)
    delta_pct = float(metrics.delta_pct or 0.0)
    top_driver_name = metrics.top_driver_name
    top_driver_share = float(metrics.top_driver_share or 0.0)
    dd = str(driver_dim or "").strip().upper()
    driver_noun = "apps" if dd == "APP_GROUP" else ("teams" if dd == "TEAM" else "programs")

    if coverage_pct < 0.70 or assigned_pct < 0.70:
        return Headline(
            text="Forecast reliability is low; some work is not fully sized or mapped.",
            mode="confidence",
            tone="warn",
            driver_dim=driver_dim,
            driver_name=top_driver_name,
            driver_share=top_driver_share,
            sources=list(sources or []),
        )

    if abs(delta_pct) >= 0.05:
        direction = "above" if delta_pct > 0 else "below"
        if top_driver_share >= 0.40 and top_driver_name:
            text = f"Forecast is trending {direction} plan; changes are concentrated in {top_driver_name}."
        else:
            text = f"Forecast is trending {direction} plan; changes are distributed across {driver_noun}."
        tone = "neutral" if abs(delta_pct) < 0.10 else ("warn" if delta_pct > 0 else "good")
        return Headline(
            text=text,
            mode="trend",
            tone=tone,
            driver_dim=driver_dim,
            driver_name=top_driver_name,
            driver_share=top_driver_share,
            sources=list(sources or []),
        )

    if top_driver_share >= 0.40 and top_driver_name:
        text = f"Overall costs are stable; changes are concentrated in {top_driver_name}."
    else:
        text = f"Overall costs are stable; changes are distributed across {driver_noun}."
    return Headline(
        text=text,
        mode="stability",
        tone="good",
        driver_dim=driver_dim,
        driver_name=top_driver_name,
        driver_share=top_driver_share,
        sources=list(sources or []),
    )


def compute_headline(
    *,
    df_lines: pd.DataFrame,
    selected_year: int,
    as_of: dt.date,
    role: str,
    scope: PreviewScope,
    scope_dict: Optional[dict] = None,
    driver_dim: Optional[str] = None,
) -> Headline:
    sd = scope_dict or {"programs": list(scope.programs or []), "teams": list(scope.teams or []), "groups": list(scope.groups or [])}
    driver_dim = str(driver_dim or resolve_driver_dimension(role, scope)).strip().upper() or "PROGRAM"
    metrics = build_snapshot_metrics(
        df_lines=df_lines,
        selected_year=int(selected_year),
        as_of=as_of,
        scope_dict=sd,
        driver_dim=driver_dim,
    )
    sources: list[str] = ["NEXT (WF+NWF)", "NEXT + ADO"]
    if metrics.actual_ytd is not None and float(metrics.actual_ytd) > 0:
        sources.append("Apptio")
    return build_headline(metrics, driver_dim=driver_dim, sources=sources)
