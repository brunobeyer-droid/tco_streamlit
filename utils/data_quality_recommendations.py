# Summary: update data quality rationale text to NEXT branding.
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd


def generate_recommendations(
    *,
    df_completeness: pd.DataFrame,
    ado_summary: Optional[pd.DataFrame],
    ado_detail: Optional[pd.DataFrame],
    program: Optional[str],
    team: Optional[str],
    app_group: Optional[str],
) -> List[Dict[str, Any]]:
    recos: List[Dict[str, Any]] = []

    def _add_reco(rec: Dict[str, Any]):
        rec["impact_count"] = int(rec.get("impact_count", 0))
        recos.append(rec)

    # Filtered completeness issues
    issues = df_completeness.copy()
    issues = issues[issues["LEVEL"].isin(["LOW", "MED"])]

    # Ownership/master data
    own_missing = issues[~issues["OWNERSHIP_OK"]]
    if not own_missing.empty:
        _add_reco(
            {
                "id": "ownership",
                "title": "Assign application groups to teams/programs",
                "severity": "HIGH",
                "impact_count": len(own_missing),
                "rationale": "Groups not assigned to a team/program cannot be attributed correctly and often break rate/pricing logic, understating NEXT.",
                "steps": [
                    "In Applications, assign each Application to the owning Team.",
                    "In Teams/Programs, ensure the Team is mapped to the correct Program.",
                ],
                "links": [
                    ("Applications", "pages/6_Applications.py"),
                    ("Programs", "pages/3_Programs.py"),
                    ("Teams", "pages/4_Teams.py"),
                ],
                "table": own_missing[["GROUPNAME", "PROGRAM", "TEAM", "MISSING_REASONS"]],
            }
        )

    # Rates where workforce exists (prefer explicit workforce expectation flag if present)
    workforce_expected = issues.get("WORKFORCE_EXPECTED", issues["WORKFORCE_OK"])
    wf_missing_rates = issues[(workforce_expected.astype(bool)) & (~issues["RATES_OK"])]
    if not wf_missing_rates.empty:
        _add_reco(
            {
                "id": "rates",
                "title": "Add rates for teams with workforce costs",
                "severity": "HIGH",
                "impact_count": len(wf_missing_rates),
                "rationale": "Workforce costs are priced at zero when rates are missing, understating NEXT.",
                "steps": [
                    "Open Rates and set team rates for teams delivering work.",
                    "Rebuild analytics views after updating rates.",
                ],
                "links": [("Rates", "pages/7_Rates.py")],
                "table": wf_missing_rates[["GROUPNAME", "PROGRAM", "TEAM", "MISSING_REASONS"]],
            }
        )

    # Non-workforce mapping (invoices/contracts)
    nwf_missing = issues[~issues["NONWORKFORCE_OK"]]
    if not nwf_missing.empty:
        _add_reco(
            {
                "id": "nonworkforce",
                "title": "Map invoices/contracts to applications",
                "severity": "MED",
                "impact_count": len(nwf_missing),
                "rationale": "Non-workforce spend is missing or unmapped, understating total cost.",
                "steps": [
                    "Attach invoices and contracts to the correct application group and year.",
                    "Ensure contract status/dates cover the selected year.",
                ],
                "links": [
                    ("Invoices", "pages/2_Invoices.py"),
                    ("Contracts", "pages/2_Contracts.py"),
                ],
                "table": nwf_missing[["GROUPNAME", "PROGRAM", "TEAM", "MISSING_REASONS"]],
            }
        )

    # Workforce inputs (demand + mapping)
    wf_missing = issues[~issues["WORKFORCE_OK"]]
    if not wf_missing.empty:
        _add_reco(
            {
                "id": "workforce_inputs",
                "title": "Fix workforce inputs (demand + mapping)",
                "severity": "MED",
                "impact_count": len(wf_missing),
                "rationale": "Workforce costs are missing or unmapped for these groups, understating NEXT.",
                "steps": [
                    "Confirm features are mapped to the correct team and application group.",
                    "Check SWAG demand readiness for features in scope (Derived FTE > 0).",
                ],
                "links": [("Settings → Reconciliation", "pages/10_Settings.py")],
                "table": wf_missing[["GROUPNAME", "PROGRAM", "TEAM", "MISSING_REASONS"]],
            }
        )

    # ADO issues from reconciliation
    if ado_detail is not None and not ado_detail.empty:
        detail = ado_detail.copy()
        # Normalize column names across versions of the ADO quality query.
        if "PROGRAM" not in detail.columns:
            if "PROGRAMID" in detail.columns:
                detail["PROGRAM"] = detail["PROGRAMID"]
            elif "PROGRAM_FIRST" in detail.columns:
                detail["PROGRAM"] = detail["PROGRAM_FIRST"]
            elif "PROGRAMNAME" in detail.columns:
                detail["PROGRAM"] = detail["PROGRAMNAME"]
            else:
                detail["PROGRAM"] = ""
        if "MISSING_TEAM" not in detail.columns:
            detail["MISSING_TEAM"] = detail.get("MISSING_TEAM_MAP", 0)
        if "MISSING_APP_GROUP" not in detail.columns:
            detail["MISSING_APP_GROUP"] = detail.get("MISSING_APP_GROUP_MAP", 0)
        if "MISSING_BV" not in detail.columns:
            detail["MISSING_BV"] = detail.get("MISSING_BV", 0)

        for c in ["MISSING_TEAM", "MISSING_APP_GROUP", "MISSING_BV"]:
            detail[c] = pd.to_numeric(detail.get(c, 0), errors="coerce").fillna(0).astype(int)

        missing_rows = detail[(detail["MISSING_TEAM"] == 1) | (detail["MISSING_APP_GROUP"] == 1) | (detail["MISSING_BV"] == 1)]
        if not missing_rows.empty:
            table_cols = [c for c in ["PROGRAM", "TEAM", "APP_GROUP", "FEATURE_ID", "MISSING_TEAM", "MISSING_APP_GROUP", "MISSING_BV"] if c in missing_rows.columns]
            _add_reco(
                {
                    "id": "ado_mapping",
                    "title": "Fix ADO mappings and Business Value",
                    "severity": "HIGH",
                    "impact_count": len(missing_rows),
                    "rationale": "Features are missing team/app mapping or Business Value (non-BASE), reducing attribution and value insights.",
                    "steps": [
                        "Map features to the correct team and application group.",
                        "Fill Business Value for non-BASE features where required.",
                    ],
                    "links": [
                        ("Settings → Reconciliation", "pages/10_Settings.py"),
                        ("Data Quality → ADO Drilldown", "pages/11_Data_Quality.py"),
                    ],
                    "table": missing_rows[table_cols],
                }
            )

    # Rank by severity then impact
    severity_rank = {"HIGH": 0, "MED": 1, "LOW": 2}
    recos = sorted(recos, key=lambda r: (severity_rank.get(r.get("severity", "MED"), 1), -r.get("impact_count", 0)))
    for idx, rec in enumerate(recos[:3]):
        rec["badge"] = "Fix first"
    return recos


__all__ = ["generate_recommendations"]
