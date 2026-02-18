from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

try:
    from core.name_resolution import apply_display_scope_names
except Exception:  # pragma: no cover
    apply_display_scope_names = None  # type: ignore

EPSILON = 1e-9
CANONICAL_QUADRANTS = ("Invest", "Optimize", "Monitor", "Reassess")


@dataclass(frozen=True)
class StrategicMaturityBundle:
    app_group_position_df: pd.DataFrame
    feature_work_df: pd.DataFrame
    meta: dict[str, Any] = field(default_factory=dict)


def _safe_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _series_float(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df.get(col), errors="coerce").fillna(0.0)


def _series_text(df: pd.DataFrame, col: str) -> pd.Series:
    return df.get(col, "").fillna("").astype(str).str.strip()


def _clamp(v: Any, lo: float = 0.0, hi: float = 100.0) -> float:
    try:
        x = float(v)
    except Exception:
        return lo
    if x != x:  # NaN
        return lo
    return max(lo, min(hi, x))


def _apply_display_names(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    try:
        if apply_display_scope_names is None:
            return df
        return apply_display_scope_names(df)
    except Exception:
        return df


def _normalize_dimension(val: Any) -> str:
    text = str(val or "").strip().lower()
    if "new" in text and "opp" in text:
        return "New Opportunities"
    if "operat" in text:
        return "Operating"
    if "health" in text:
        return "Product Health"
    return "Other"


def _empty_positioning_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "program",
            "weighted_demand_avg",
            "weighted_value_avg",
            "total_spend",
            "app_group_count",
            "invest_pct",
            "optimize_pct",
            "monitor_pct",
            "reassess_pct",
            "positioning_score",
        ]
    )


def _empty_strategic_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "program",
            "weighted_demand_avg",
            "weighted_value_avg",
            "total_spend",
            "app_group_count",
            "invest_pct",
            "optimize_pct",
            "monitor_pct",
            "reassess_pct",
            "positioning_score",
            "roi_per_fte",
            "roi_efficiency_score",
            "delivery_newopp_pct",
            "delivery_operating_pct",
            "delivery_prodhealth_pct",
            "msp_operating_pct",
            "msp_newopp_pct",
            "msp_prodhealth_pct",
            "workforce_alignment_score",
            "strategic_maturity_score",
        ]
    )


def build_strategic_maturity_bundle(
    app_group_position_df: pd.DataFrame,
    feature_work_df: pd.DataFrame,
    *,
    meta: Optional[dict[str, Any]] = None,
) -> StrategicMaturityBundle:
    pos = _apply_display_names(_safe_df(app_group_position_df))
    feat = _apply_display_names(_safe_df(feature_work_df))

    if not pos.empty:
        if "PROJECTED_SPEND" not in pos.columns and "ACTUAL_COST" in pos.columns:
            pos["PROJECTED_SPEND"] = _series_float(pos, "ACTUAL_COST")
        for col in ["PROGRAMNAME", "GROUPNAME", "quadrant_label"]:
            pos[col] = _series_text(pos, col)
        for col in ["DEMAND_AXIS", "Y_VALUE_PLOT", "PROJECTED_SPEND"]:
            pos[col] = _series_float(pos, col)
        pos = pos[
            ["PROGRAMNAME", "GROUPNAME", "DEMAND_AXIS", "Y_VALUE_PLOT", "PROJECTED_SPEND", "quadrant_label"]
        ].copy()

    if not feat.empty:
        feat["PROGRAMNAME"] = _series_text(feat, "PROGRAMNAME")
        feat["TEAMNAME"] = _series_text(feat, "TEAMNAME")
        feat["IS_MSP_FEATURE"] = _series_float(feat, "IS_MSP_FEATURE").astype(int) > 0
        feat["INVESTMENT_DIMENSION"] = _series_text(feat, "INVESTMENT_DIMENSION")
        if "DEMAND_FTE" not in feat.columns:
            if "DERIVED_FTE_EXPLORER" in feat.columns:
                feat["DEMAND_FTE"] = _series_float(feat, "DERIVED_FTE_EXPLORER")
            else:
                feat["DEMAND_FTE"] = _series_float(feat, "DERIVED_FTE")
        feat["DEMAND_FTE"] = _series_float(feat, "DEMAND_FTE")
        feat = feat[
            ["PROGRAMNAME", "TEAMNAME", "IS_MSP_FEATURE", "INVESTMENT_DIMENSION", "DEMAND_FTE"]
        ].copy()

    return StrategicMaturityBundle(
        app_group_position_df=pos,
        feature_work_df=feat,
        meta=dict(meta or {}),
    )


def calculate_program_positioning_maturity(
    app_group_position_df: pd.DataFrame,
    *,
    spend_col: str = "PROJECTED_SPEND",
) -> pd.DataFrame:
    empty = _empty_positioning_df()
    if app_group_position_df is None or not isinstance(app_group_position_df, pd.DataFrame) or app_group_position_df.empty:
        return empty

    w = _safe_df(app_group_position_df)
    if spend_col not in w.columns and spend_col == "PROJECTED_SPEND" and "ACTUAL_COST" in w.columns:
        w[spend_col] = _series_float(w, "ACTUAL_COST")

    req_cols = {"PROGRAMNAME", "GROUPNAME", "DEMAND_AXIS", "Y_VALUE_PLOT", spend_col, "quadrant_label"}
    if not req_cols.issubset(set(w.columns)):
        return empty

    w["PROGRAMNAME"] = _series_text(w, "PROGRAMNAME")
    w["GROUPNAME"] = _series_text(w, "GROUPNAME")
    w["quadrant_label"] = _series_text(w, "quadrant_label")
    w["DEMAND_AXIS"] = _series_float(w, "DEMAND_AXIS")
    w["Y_VALUE_PLOT"] = _series_float(w, "Y_VALUE_PLOT")
    w[spend_col] = _series_float(w, spend_col)
    w = w[w["PROGRAMNAME"].ne("") & w["GROUPNAME"].ne("") & (w[spend_col] > 0)].copy()
    w = w[w["quadrant_label"].isin(set(CANONICAL_QUADRANTS))].copy()
    if w.empty:
        return empty

    rows: list[dict[str, Any]] = []
    for program, grp in w.groupby("PROGRAMNAME", dropna=False):
        total_spend = float(_series_float(grp, spend_col).sum())
        if total_spend <= 0:
            continue
        demand_avg = float((grp["DEMAND_AXIS"] * grp[spend_col]).sum() / total_spend)
        value_avg = float((grp["Y_VALUE_PLOT"] * grp[spend_col]).sum() / total_spend)

        by_quadrant = grp.groupby("quadrant_label", dropna=False)[spend_col].sum()
        invest_share = float(by_quadrant.get("Invest", 0.0)) / total_spend
        optimize_share = float(by_quadrant.get("Optimize", 0.0)) / total_spend
        monitor_share = float(by_quadrant.get("Monitor", 0.0)) / total_spend
        reassess_share = float(by_quadrant.get("Reassess", 0.0)) / total_spend

        raw = (
            1.0 * invest_share
            + 0.7 * optimize_share
            - 0.7 * monitor_share
            - 1.0 * reassess_share
        )
        rows.append(
            {
                "program": str(program),
                "weighted_demand_avg": demand_avg,
                "weighted_value_avg": value_avg,
                "total_spend": max(0.0, total_spend),
                "app_group_count": int(grp["GROUPNAME"].nunique()),
                "invest_pct": _clamp(invest_share * 100.0),
                "optimize_pct": _clamp(optimize_share * 100.0),
                "monitor_pct": _clamp(monitor_share * 100.0),
                "reassess_pct": _clamp(reassess_share * 100.0),
                "positioning_score": _clamp(100.0 * raw),
            }
        )

    if not rows:
        return empty
    out = pd.DataFrame(rows)
    return out.sort_values(["positioning_score", "total_spend"], ascending=[False, False]).reset_index(drop=True)


def _calculate_roi_efficiency(
    app_group_position_df: pd.DataFrame,
    *,
    spend_col: str,
) -> pd.DataFrame:
    out_cols = ["program", "roi_per_fte", "roi_efficiency_score"]
    w = _safe_df(app_group_position_df)
    if w.empty:
        return pd.DataFrame(columns=out_cols)
    if spend_col not in w.columns and spend_col == "PROJECTED_SPEND" and "ACTUAL_COST" in w.columns:
        w[spend_col] = _series_float(w, "ACTUAL_COST")
    req = {"PROGRAMNAME", "DEMAND_AXIS", "Y_VALUE_PLOT", spend_col}
    if not req.issubset(set(w.columns)):
        return pd.DataFrame(columns=out_cols)

    w["PROGRAMNAME"] = _series_text(w, "PROGRAMNAME")
    w["DEMAND_AXIS"] = _series_float(w, "DEMAND_AXIS")
    w["Y_VALUE_PLOT"] = _series_float(w, "Y_VALUE_PLOT")
    w[spend_col] = _series_float(w, spend_col)
    w = w[w["PROGRAMNAME"].ne("") & (w[spend_col] > 0)].copy()
    if w.empty:
        return pd.DataFrame(columns=out_cols)

    agg = (
        w.assign(value_proxy_component=w["Y_VALUE_PLOT"] * w[spend_col])
        .groupby("PROGRAMNAME", dropna=False)
        .agg(value_proxy_total=("value_proxy_component", "sum"), demand_total=("DEMAND_AXIS", "sum"))
        .reset_index()
    )
    agg["roi_per_fte"] = agg["value_proxy_total"] / agg["demand_total"].clip(lower=EPSILON)
    agg.loc[agg["demand_total"] <= 0, "roi_per_fte"] = pd.NA

    valid = pd.to_numeric(agg["roi_per_fte"], errors="coerce")
    valid = valid[valid.notna()]
    if valid.empty:
        agg["roi_efficiency_score"] = 50.0
    else:
        vmin = float(valid.min())
        vmax = float(valid.max())
        if abs(vmax - vmin) <= EPSILON:
            agg["roi_efficiency_score"] = 50.0
        else:
            agg["roi_efficiency_score"] = (agg["roi_per_fte"] - vmin) / (vmax - vmin) * 100.0
            agg["roi_efficiency_score"] = pd.to_numeric(agg["roi_efficiency_score"], errors="coerce").fillna(50.0)
    agg["roi_efficiency_score"] = agg["roi_efficiency_score"].clip(0.0, 100.0)
    return agg.rename(columns={"PROGRAMNAME": "program"})[out_cols]


def _calculate_workforce_alignment(feature_work_df: pd.DataFrame) -> pd.DataFrame:
    out_cols = [
        "program",
        "delivery_newopp_pct",
        "delivery_operating_pct",
        "delivery_prodhealth_pct",
        "msp_operating_pct",
        "msp_newopp_pct",
        "msp_prodhealth_pct",
        "workforce_alignment_score",
    ]
    w = _safe_df(feature_work_df)
    if w.empty:
        return pd.DataFrame(columns=out_cols)
    req = {"PROGRAMNAME", "IS_MSP_FEATURE", "INVESTMENT_DIMENSION", "DEMAND_FTE"}
    if not req.issubset(set(w.columns)):
        return pd.DataFrame(columns=out_cols)

    w["PROGRAMNAME"] = _series_text(w, "PROGRAMNAME")
    w["DEMAND_FTE"] = _series_float(w, "DEMAND_FTE")
    w["IS_MSP_FEATURE"] = _series_float(w, "IS_MSP_FEATURE").astype(int) > 0
    w["TEAM_FAMILY"] = w["IS_MSP_FEATURE"].map({True: "MSP", False: "Delivery"})
    w["INVESTMENT_DIMENSION_NORM"] = w["INVESTMENT_DIMENSION"].map(_normalize_dimension)
    w = w[w["PROGRAMNAME"].ne("") & (w["DEMAND_FTE"] >= 0)].copy()
    if w.empty:
        return pd.DataFrame(columns=out_cols)

    grouped = (
        w.groupby(["PROGRAMNAME", "TEAM_FAMILY", "INVESTMENT_DIMENSION_NORM"], dropna=False)["DEMAND_FTE"]
        .sum()
        .reset_index()
    )

    rows: list[dict[str, Any]] = []
    for program, grp in grouped.groupby("PROGRAMNAME", dropna=False):
        delivery = grp[grp["TEAM_FAMILY"].eq("Delivery")]
        msp = grp[grp["TEAM_FAMILY"].eq("MSP")]
        delivery_total = float(delivery["DEMAND_FTE"].sum())
        msp_total = float(msp["DEMAND_FTE"].sum())
        total_demand = delivery_total + msp_total

        def _share(df: pd.DataFrame, label: str, total: float) -> float:
            if total <= 0:
                return 0.0
            return float(df.loc[df["INVESTMENT_DIMENSION_NORM"].eq(label), "DEMAND_FTE"].sum()) / total

        delivery_newopp = _share(delivery, "New Opportunities", delivery_total)
        delivery_operating = _share(delivery, "Operating", delivery_total)
        delivery_prodhealth = _share(delivery, "Product Health", delivery_total)

        msp_operating = _share(msp, "Operating", msp_total)
        msp_newopp = _share(msp, "New Opportunities", msp_total)
        msp_prodhealth = _share(msp, "Product Health", msp_total)

        delivery_score = _clamp(
            100.0
            * (
                0.65 * delivery_newopp
                + 0.25 * delivery_prodhealth
                + 0.10 * (1.0 - delivery_operating)
            )
        )
        msp_score = _clamp(
            100.0
            * (
                0.70 * msp_operating
                + 0.20 * msp_prodhealth
                + 0.10 * (1.0 - msp_newopp)
            )
        )

        if total_demand <= 0:
            alignment = 50.0
        elif delivery_total <= 0 and msp_total > 0:
            alignment = msp_score
        elif msp_total <= 0 and delivery_total > 0:
            alignment = delivery_score
        else:
            alignment = ((delivery_score * delivery_total) + (msp_score * msp_total)) / total_demand

        rows.append(
            {
                "program": str(program),
                "delivery_newopp_pct": _clamp(delivery_newopp * 100.0),
                "delivery_operating_pct": _clamp(delivery_operating * 100.0),
                "delivery_prodhealth_pct": _clamp(delivery_prodhealth * 100.0),
                "msp_operating_pct": _clamp(msp_operating * 100.0),
                "msp_newopp_pct": _clamp(msp_newopp * 100.0),
                "msp_prodhealth_pct": _clamp(msp_prodhealth * 100.0),
                "workforce_alignment_score": _clamp(alignment),
            }
        )

    if not rows:
        return pd.DataFrame(columns=out_cols)
    return pd.DataFrame(rows)[out_cols]


def calculate_program_strategic_maturity(bundle: StrategicMaturityBundle) -> pd.DataFrame:
    out = _empty_strategic_df()
    b = bundle if isinstance(bundle, StrategicMaturityBundle) else StrategicMaturityBundle(pd.DataFrame(), pd.DataFrame(), {})
    spend_col = str((b.meta or {}).get("spend_col") or "PROJECTED_SPEND").strip() or "PROJECTED_SPEND"

    positioning = calculate_program_positioning_maturity(b.app_group_position_df, spend_col=spend_col)
    roi = _calculate_roi_efficiency(b.app_group_position_df, spend_col=spend_col)
    workforce = _calculate_workforce_alignment(b.feature_work_df)

    if positioning.empty and roi.empty and workforce.empty:
        return out

    merged = positioning.merge(roi, on="program", how="outer").merge(workforce, on="program", how="outer")
    merged["program"] = _series_text(merged, "program")
    merged = merged[merged["program"].ne("")].copy()
    if merged.empty:
        return out

    for col in [
        "weighted_demand_avg",
        "weighted_value_avg",
        "total_spend",
        "app_group_count",
        "invest_pct",
        "optimize_pct",
        "monitor_pct",
        "reassess_pct",
        "positioning_score",
        "roi_per_fte",
        "roi_efficiency_score",
        "delivery_newopp_pct",
        "delivery_operating_pct",
        "delivery_prodhealth_pct",
        "msp_operating_pct",
        "msp_newopp_pct",
        "msp_prodhealth_pct",
        "workforce_alignment_score",
    ]:
        merged[col] = _series_float(merged, col)

    merged["positioning_score"] = merged["positioning_score"].clip(0.0, 100.0)
    merged["roi_efficiency_score"] = merged["roi_efficiency_score"].clip(0.0, 100.0)
    merged["workforce_alignment_score"] = merged["workforce_alignment_score"].clip(0.0, 100.0)
    merged["strategic_maturity_score"] = (
        0.40 * merged["positioning_score"]
        + 0.30 * merged["roi_efficiency_score"]
        + 0.30 * merged["workforce_alignment_score"]
    ).clip(0.0, 100.0)

    merged = merged.sort_values(["strategic_maturity_score", "total_spend"], ascending=[False, False]).reset_index(drop=True)
    return merged[out.columns]


def build_program_strategic_drivers(strategic_df: pd.DataFrame) -> pd.DataFrame:
    out_cols = ["program", "biggest_driver", "quick_recommendation"]
    w = _safe_df(strategic_df)
    if w.empty:
        return pd.DataFrame(columns=out_cols)

    w["program"] = _series_text(w, "program")
    w["delivery_operating_pct"] = _series_float(w, "delivery_operating_pct")
    w["msp_newopp_pct"] = _series_float(w, "msp_newopp_pct")
    w["roi_efficiency_score"] = _series_float(w, "roi_efficiency_score")
    w["positioning_score"] = _series_float(w, "positioning_score")
    w["workforce_alignment_score"] = _series_float(w, "workforce_alignment_score")
    roi_median = float(w["roi_efficiency_score"].median()) if not w.empty else 50.0

    rows: list[dict[str, str]] = []
    for _, row in w.iterrows():
        driver = ""
        reco = ""
        if float(row["delivery_operating_pct"]) > 25.0:
            driver = "Delivery over-indexed on Operating"
            reco = "Shift Delivery capacity toward New Opportunities."
        elif float(row["msp_newopp_pct"]) > 10.0:
            driver = "MSP doing too much New Opportunities"
            reco = "Move exploratory work to Delivery teams."
        elif float(row["roi_efficiency_score"]) < (roi_median - 10.0):
            driver = "Low ROI per FTE"
            reco = "Rebalance lower-value app-group spend."
        else:
            lows = {
                "Quadrant positioning weaker than peers": float(row["positioning_score"]),
                "ROI efficiency below potential": float(row["roi_efficiency_score"]),
                "Workforce alignment needs adjustment": float(row["workforce_alignment_score"]),
            }
            driver = min(lows, key=lows.get)
            if "positioning" in driver.lower():
                reco = "Increase allocation in Invest/Optimize quadrants."
            elif "roi" in driver.lower():
                reco = "Improve value output per unit of demand."
            else:
                reco = "Rebalance Delivery and MSP work by investment dimension."
        rows.append(
            {
                "program": str(row["program"]),
                "biggest_driver": driver,
                "quick_recommendation": reco,
            }
        )
    return pd.DataFrame(rows, columns=out_cols)
