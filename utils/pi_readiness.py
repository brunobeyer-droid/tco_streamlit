from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from utils.pi_readiness_config import load_pi_readiness_config


def _bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    vals = df.get(col, pd.Series(dtype=object))

    def _as_bool(v: Any) -> bool:
        if isinstance(v, (bool, int)):
            return bool(v)
        s = str(v or "").strip().lower()
        return s in {"1", "true", "yes", "y", "on"}

    return vals.apply(_as_bool).fillna(False)


def evaluate_pi_readiness(
    *,
    fetch_df,
    scope,
    filters: Dict[str, Optional[str]],
    df_completeness: pd.DataFrame,
    ado_summary: Optional[pd.DataFrame],
    ado_detail: Optional[pd.DataFrame],
) -> Dict[str, Any]:
    cfg = load_pi_readiness_config()
    thresholds = cfg.get("readiness_thresholds", {}) or {}
    required = cfg.get("required_checks", {}) or {}
    gating = cfg.get("gating_rules", {}) or {}
    ado_rules = cfg.get("ado_rules", {}) or {}

    ready_min = float(thresholds.get("ready_min_score", 90))
    at_risk_min = float(thresholds.get("at_risk_min_score", 70))

    score = float(df_completeness["COMPLETENESS_SCORE"].mean()) if not df_completeness.empty else 0.0

    ownership_ok = _bool_series(df_completeness, "OWNERSHIP_OK")
    wf_ok = _bool_series(df_completeness, "WORKFORCE_OK")
    rates_ok = _bool_series(df_completeness, "RATES_OK")
    nwf_ok = _bool_series(df_completeness, "NONWORKFORCE_OK")
    ado_ok = _bool_series(df_completeness, "ENHANCEMENTS_OK")

    blockers: List[Dict[str, Any]] = []
    check_results: Dict[str, Any] = {}

    def _add_blocker(bid: str, title: str, severity: str, impacted_count: int, explanation: str, links: List[tuple], table: Optional[pd.DataFrame] = None):
        blockers.append(
            {
                "id": bid,
                "title": title,
                "severity": severity,
                "impact_count": int(impacted_count),
                "explanation": explanation,
                "where_to_fix": links,
                "table": table,
            }
        )

    # Ownership
    missing_owner = df_completeness[~ownership_ok]
    check_results["ownership"] = {"pass": missing_owner.empty, "count_missing": len(missing_owner)}
    if required.get("require_ownership", True) and not missing_owner.empty:
        _add_blocker(
            "ownership",
            "Set ownership for all application groups",
            "BLOCKER",
            len(missing_owner),
            "Groups without ownership cannot be attributed in PI planning.",
            [("Applications", "pages/6_Applications.py"), ("Programs", "pages/3_Programs.py"), ("Teams", "pages/4_Teams.py")],
            missing_owner[["GROUPNAME", "PROGRAM", "TEAM", "MISSING_REASONS"]],
        )

    # Rates
    missing_rates = df_completeness[(wf_ok) & (~rates_ok)]
    check_results["rates"] = {"pass": missing_rates.empty, "count_missing": len(missing_rates)}
    if required.get("require_rates", True) and not missing_rates.empty:
        _add_blocker(
            "rates",
            "Add rates for teams with workforce demand",
            "BLOCKER" if gating.get("block_if_missing_rates_for_workforce", True) else "WARNING",
            len(missing_rates),
            "Workforce costs are zero where rates are missing.",
            [("Rates", "pages/7_Rates.py")],
            missing_rates[["GROUPNAME", "PROGRAM", "TEAM", "MISSING_REASONS"]],
        )

    # Non-workforce
    missing_nwf = df_completeness[~nwf_ok]
    check_results["nonworkforce"] = {"pass": missing_nwf.empty, "count_missing": len(missing_nwf)}
    if required.get("require_nonworkforce", True) and not missing_nwf.empty:
        _add_blocker(
            "nonworkforce",
            "Map invoices/contracts to applications",
            "WARNING",
            len(missing_nwf),
            "Non-workforce spend is missing or unmapped, understating total cost.",
            [("Invoices", "pages/2_Invoices.py"), ("Contracts", "pages/2_Contracts.py")],
            missing_nwf[["GROUPNAME", "PROGRAM", "TEAM", "MISSING_REASONS"]],
        )

    # Workforce inputs
    missing_wf_inputs = df_completeness[~wf_ok]
    check_results["workforce"] = {"pass": missing_wf_inputs.empty, "count_missing": len(missing_wf_inputs)}
    require_swag = required.get("require_swag_ready_demand", required.get("require_effort_points_or_feature_count", True))
    if require_swag and not missing_wf_inputs.empty:
        _add_blocker(
            "workforce_inputs",
            "Fix workforce demand inputs (SWAG)",
            "WARNING",
            len(missing_wf_inputs),
            "Projected (Expected) workforce costs are demand-driven by SWAG Derived FTE; when SWAG is not ready, demand/cost is 0.",
            [("Settings → Reconciliation", "pages/10_Settings.py")],
            missing_wf_inputs[["GROUPNAME", "PROGRAM", "TEAM", "MISSING_REASONS"]],
        )

    # ADO mapping from reconciliation
    total_features = 0
    missing_features = 0
    if ado_summary is not None and not ado_summary.empty:
        total_features = int(pd.to_numeric(ado_summary.get("FEATURES", []), errors="coerce").fillna(0).sum())
        missing_swag = int(pd.to_numeric(ado_summary.get("MISSING_SWAG_DEMAND", []), errors="coerce").fillna(0).sum())
        missing_features = int(
            pd.to_numeric(ado_summary.get("MISSING_TEAM", []), errors="coerce").fillna(0).sum()
            + pd.to_numeric(ado_summary.get("MISSING_APP_GROUP", []), errors="coerce").fillna(0).sum()
            + missing_swag
        )
    ado_percent_missing = (missing_features / total_features * 100) if total_features else 0.0
    check_results["ado"] = {
        "pass": missing_features == 0,
        "missing_features": missing_features,
        "total_features": total_features,
        "percent_missing": ado_percent_missing,
    }
    ado_required = required.get("require_ado_mapping", True)
    if ado_required:
        if ado_percent_missing > ado_rules.get("max_percent_features_missing_mapping_for_ready", 2.0):
            severity = "BLOCKER" if ado_percent_missing > ado_rules.get("max_percent_features_missing_mapping_for_at_risk", 10.0) else "WARNING"
            _add_blocker(
                "ado_mapping",
                "Fix ADO mappings and SWAG demand readiness",
                severity,
                missing_features,
                "Features are missing mapping fields or SWAG-derived demand (Derived FTE).",
                [("Settings → Reconciliation", "pages/10_Settings.py"), ("Data Quality → ADO Drilldown", "pages/11_Data_Quality.py")],
                (
                    ado_detail[[c for c in [
                        "PROGRAM",
                        "TEAM",
                        "APP_GROUP",
                        "FEATURE_ID",
                        "MISSING_TEAM",
                        "MISSING_APP_GROUP",
                        "MISSING_SWAG_DEMAND",
                    ] if c in ado_detail.columns]]
                    if ado_detail is not None and not ado_detail.empty
                    else None
                ),
            )
    # Unassigned costs
    unassigned = df_completeness[df_completeness["GROUPNAME"].str.contains("unassigned", case=False, na=False)]
    check_results["unassigned"] = {"pass": unassigned.empty, "count": len(unassigned)}
    if gating.get("block_if_unassigned_cost_present", True) and not unassigned.empty:
        _add_blocker(
            "unassigned",
            "Clear unassigned cost buckets",
            "BLOCKER",
            len(unassigned),
            "Unassigned cost indicates missing mappings and will block readiness.",
            [("Programs", "pages/3_Programs.py"), ("Applications", "pages/6_Applications.py")],
            unassigned[["GROUPNAME", "PROGRAM", "TEAM", "MISSING_REASONS"]],
        )

    # Status determination
    status = "READY"
    if score < at_risk_min or any(b["severity"] == "BLOCKER" for b in blockers):
        status = "NOT_READY"
    elif score < ready_min or any(b["severity"] in {"BLOCKER", "WARNING"} for b in blockers):
        status = "AT_RISK"

    return {
        "status": status,
        "score": score,
        "blockers": blockers,
        "check_results": check_results,
    }


__all__ = ["evaluate_pi_readiness"]
