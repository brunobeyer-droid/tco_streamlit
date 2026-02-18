# Summary: update data quality issue text to NEXT branding.
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


class FixSystem:
    TCO = "TCO"
    ADO = "ADO"
    APPTIO = "APPTIO"
    EXTERNAL = "EXTERNAL"
    HOWTO = "HOWTO"


@dataclass(frozen=True)
class IssueSpec:
    issue_type: str
    title: str
    issue_area: str
    default_severity: str
    default_scenario: str
    guide_section: str
    fix_system: str
    fix_target: Optional[str]
    fix_action_template: str
    expects_evidence: bool
    evidence_columns: List[str]
    dedupe_group: Optional[str] = None


ISSUE_SPECS: Dict[str, IssueSpec] = {
    "ADO_FEATURES_MISSING_SWAG": IssueSpec(
        issue_type="ADO_FEATURES_MISSING_SWAG",
        title="ADO features missing SWAG",
        issue_area="ADO Demand & Forecast",
        default_severity="Blocking",
        default_scenario="Expected",
        guide_section="ADO Feature Requirements (SWAG, App Name)",
        fix_system=FixSystem.ADO,
        fix_target=None,
        fix_action_template="Populate SWAG in ADO for the affected features.",
        expects_evidence=True,
        evidence_columns=[
            "ADO_FEATURE_ID",
            "ADO_FEATURE_TITLE",
            "ADO_PROGRAM",
            "ADO_TEAM",
            "ADO_MISSING_FIELD",
        ],
    ),
    "ADO_FEATURES_MISSING_APP_NAME": IssueSpec(
        issue_type="ADO_FEATURES_MISSING_APP_NAME",
        title="ADO features missing App Name",
        issue_area="ADO Demand & Forecast",
        default_severity="Blocking",
        default_scenario="Expected",
        guide_section="ADO Feature Requirements (SWAG, App Name)",
        fix_system=FixSystem.ADO,
        fix_target=None,
        fix_action_template="Populate App Name in ADO for the affected features.",
        expects_evidence=True,
        evidence_columns=[
            "ADO_FEATURE_ID",
            "ADO_FEATURE_TITLE",
            "ADO_PROGRAM",
            "ADO_TEAM",
            "ADO_MISSING_FIELD",
        ],
    ),
    "CONFIG_NO_ADO_MAPPING_ROWS": IssueSpec(
        issue_type="CONFIG_NO_ADO_MAPPING_ROWS",
        title="ADO mapping not configured (no mapping rows yet)",
        issue_area="ADO Demand & Forecast",
        default_severity="Warning",
        default_scenario="Expected",
        guide_section="ADO App Name → NEXT Application Group Mapping",
        fix_system=FixSystem.TCO,
        fix_target="applications",
        fix_action_template="Create initial mappings for ADO app names to NEXT application groups.",
        expects_evidence=True,
        evidence_columns=["PROGRAM", "TEAM", "mapping_row_count"],
        dedupe_group="ADO_MAPPING",
    ),
    "ADO_UNMAPPED_APP": IssueSpec(
        issue_type="ADO_UNMAPPED_APP",
        title="Unmapped ADO app names found in features",
        issue_area="ADO Demand & Forecast",
        default_severity="Warning",
        default_scenario="Expected",
        guide_section="ADO App Name → NEXT Application Group Mapping",
        fix_system=FixSystem.TCO,
        fix_target="applications",
        fix_action_template="Map ADO app names to NEXT application groups.",
        expects_evidence=True,
        evidence_columns=[
            "ADO_FEATURE_ID",
            "ADO_FEATURE_TITLE",
            "ADO_PROGRAM",
            "ADO_TEAM",
            "ADO_MISSING_FIELD",
        ],
        dedupe_group="ADO_MAPPING",
    ),
    "CONFIG_MISSING_WF_RATES": IssueSpec(
        issue_type="CONFIG_MISSING_WF_RATES",
        title="Missing workforce rates for selected scope/year",
        issue_area="Rates",
        default_severity="Blocking",
        default_scenario="—",
        guide_section="Rates & Time Semantics (Rates are per PI / iteration)",
        fix_system=FixSystem.TCO,
        fix_target="rates",
        fix_action_template="Add workforce rates for the selected year.",
        expects_evidence=True,
        evidence_columns=["YEAR", "PROGRAM", "TEAM"],
    ),
    "CONFIG_APP_MISSING_OWNER": IssueSpec(
        issue_type="CONFIG_APP_MISSING_OWNER",
        title="Application groups missing owner",
        issue_area="Ownership & Structure",
        default_severity="Blocking",
        default_scenario="—",
        guide_section="Headcount & Cost Attribution Rules",
        fix_system=FixSystem.TCO,
        fix_target="applications",
        fix_action_template="Assign an owner team for each application group.",
        expects_evidence=True,
        evidence_columns=["APP_GROUP", "OWNER_TEAM", "PROGRAM"],
    ),
    "APPTIO_UNMAPPED_ACTUALS": IssueSpec(
        issue_type="APPTIO_UNMAPPED_ACTUALS",
        title="Apptio actuals not loaded or unmapped",
        issue_area="Actuals (Apptio/NWF)",
        default_severity="Warning",
        default_scenario="Actual",
        guide_section="Actuals Granularity (Program-level Apptio)",
        fix_system=FixSystem.APPTIO,
        fix_target=None,
        fix_action_template="Review Apptio mappings or upload process.",
        expects_evidence=True,
        evidence_columns=["PROGRAM", "MONTH_KEY", "AMOUNT"],
    ),
    "CONFIG_ADO_TEAMS_UNMAPPED": IssueSpec(
        issue_type="CONFIG_ADO_TEAMS_UNMAPPED",
        title="ADO teams not mapped to NEXT teams",
        issue_area="ADO Demand & Forecast",
        default_severity="Warning",
        default_scenario="Expected",
        guide_section="ADO Team Mapping",
        fix_system=FixSystem.TCO,
        fix_target="teams",
        fix_action_template="Map ADO teams to NEXT teams for the program.",
        expects_evidence=True,
        evidence_columns=[
            "ADO_TEAM_RAW",
            "PROGRAM",
            "IS_MAPPED",
            "LAST_SEEN_AT",
            "FEATURE_COUNT",
            "EFFORT_SUM",
        ],
    ),
    "ADO_UNMAPPED_TEAMS_IN_PROGRAM": IssueSpec(
        issue_type="ADO_UNMAPPED_TEAMS_IN_PROGRAM",
        title="ADO teams not mapped to NEXT teams (program onboarding incomplete)",
        issue_area="ADO Onboarding",
        default_severity="Warning",
        default_scenario="Expected",
        guide_section="ADO Team Mapping",
        fix_system=FixSystem.TCO,
        fix_target="settings",
        fix_action_template="Map ADO teams to NEXT teams for the program in Settings → ADO.",
        expects_evidence=True,
        evidence_columns=[
            "PROGRAM_RAW",
            "ADO_TEAM_RAW",
            "TEAM_DISPLAY",
            "FEATURE_COUNT",
            "EFFORT_SUM",
            "LAST_SEEN_AT",
            "IS_MAPPED",
        ],
    ),
}


def get_issue_spec(issue_type: str) -> Optional[IssueSpec]:
    return ISSUE_SPECS.get(str(issue_type or "").strip())
