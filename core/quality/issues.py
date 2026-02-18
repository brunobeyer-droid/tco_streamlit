from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


class Severity:
    BLOCKER = "BLOCKER"
    WARNING = "WARNING"
    INFO = "INFO"


SEVERITY_ORDER: dict[str, int] = {
    Severity.BLOCKER: 3,
    Severity.WARNING: 2,
    Severity.INFO: 1,
    "BLOCKING": 3,
    "WARNING": 2,
    "INFO": 1,
    "Blocking": 3,
    "Warning": 2,
    "Info": 1,
}


class IssueStatus:
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"


class OwnerRole:
    PORTFOLIO = "Portfolio Manager"
    PROGRAM = "Program Manager"
    PRODUCT_OWNER = "Product Owner"
    RATES = "Rates Owner"
    ADMIN = "Admin"


class ImpactBasis:
    USD = "USD"
    FTE = "FTE"
    FEATURES = "FEATURES"
    MIXED = "MIXED"


class IssueType:
    UNASSIGNED_COST = "UNASSIGNED_COST"
    MISSING_WF_RATES = "MISSING_WF_RATES"
    CONFIG_MISSING_WF_RATES = "CONFIG_MISSING_WF_RATES"
    TEAM_MISSING_PROGRAM = "TEAM_MISSING_PROGRAM"
    APP_GROUP_MISSING_OWNER_TEAM = "APP_GROUP_MISSING_OWNER_TEAM"
    CONFIG_APP_MISSING_OWNER = "CONFIG_APP_MISSING_OWNER"
    APP_GROUP_MISSING_MSP_ASSIGNMENT = "APP_GROUP_MISSING_MSP_ASSIGNMENT"
    APP_GROUP_MISSING_ADO_MAPPING = "APP_GROUP_MISSING_ADO_MAPPING"
    APP_GROUP_MULTI_INSTANCE = "APP_GROUP_MULTI_INSTANCE"
    ADO_UNMAPPED_APP = "ADO_UNMAPPED_APP"
    ADO_UNMAPPED_TEAM = "ADO_UNMAPPED_TEAM"
    ADO_MISSING_EFFORT = "ADO_MISSING_EFFORT"
    ADO_FEATURES_MISSING_SWAG = "ADO_FEATURES_MISSING_SWAG"
    ADO_FEATURES_MISSING_APP_NAME = "ADO_FEATURES_MISSING_APP_NAME"
    APPTIO_UNMAPPED_ACTUALS = "APPTIO_UNMAPPED_ACTUALS"
    CONFIG_NO_ADO_MAPPING_ROWS = "CONFIG_NO_ADO_MAPPING_ROWS"
    CONFIG_PROGRAM_HAS_NO_TEAMS = "CONFIG_PROGRAM_HAS_NO_TEAMS"
    CONFIG_ADO_TEAMS_UNMAPPED = "CONFIG_ADO_TEAMS_UNMAPPED"
    ADO_UNMAPPED_TEAMS_IN_PROGRAM = "ADO_UNMAPPED_TEAMS_IN_PROGRAM"


@dataclass(frozen=True)
class Issue:
    issue_id: str
    issue_type: str
    title: str
    severity: str
    status: str
    owner_role: Optional[str] = None
    scenario: str = ""
    source: str = ""
    scope: Dict[str, Any] = field(default_factory=dict)
    impact_usd_est: float = 0.0
    impact_fte: float = 0.0
    impact_features: int = 0
    impact_basis: str = ImpactBasis.MIXED
    impact_basis_detail: str = ""
    confidence: str = "MEDIUM"
    summary: str = ""
    evidence_ref: Optional[Dict[str, Any]] = None
    fix: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    dedupe_key: str = ""
    computed_at: str = field(default_factory=lambda: dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        # Ensure stable types for Streamlit tables / JSON.
        out["impact_usd_est"] = float(out.get("impact_usd_est") or 0.0)
        out["impact_fte"] = float(out.get("impact_fte") or 0.0)
        out["impact_features"] = int(out.get("impact_features") or 0)
        out["confidence"] = str(out.get("confidence") or "").strip() or "MEDIUM"
        out["impact_basis_detail"] = str(out.get("impact_basis_detail") or "").strip()
        out["tags"] = list(out.get("tags") or [])
        out["scope"] = dict(out.get("scope") or {})
        out["fix"] = dict(out.get("fix") or {})
        return out
