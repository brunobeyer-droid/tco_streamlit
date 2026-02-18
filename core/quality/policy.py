from __future__ import annotations

from typing import Any, Mapping, Optional

import pandas as pd


def _get(line: Any, key: str) -> Any:
    if line is None:
        return None
    if isinstance(line, Mapping):
        return line.get(key)
    if isinstance(line, pd.Series):
        return line.get(key)
    return getattr(line, key, None)


def _norm(x: Any) -> str:
    return str(x or "").strip().upper()


def is_app_group_mapping_required(line: Any) -> bool:
    """Return True if the line is expected to have an application-group mapping.

    Policy:
    - Program Additional Costs are planned at program level and are NOT in scope for app-group mapping.
    - We detect Program Additional defensively using any available hint columns.
    """
    hints = {
        "COST_BUCKET",
        "COST_SOURCE",
        "COST_SRC",
        "ALLOCATION_METHOD",
        "TCO_COST_CATEGORY",
        "COST_CATEGORY_L2",
        "COST_CATEGORY_L3",
        "SOURCE",
        "TEAM",
        "TEAMNAME",
        "COST_CATEGORY",
        "SUBCOMPONENT",
        "WORKFORCE_CLASS",
        "COST_SUBTYPE",
        "APP_GROUP",
        "GROUPNAME",
    }
    for col in hints:
        v = _norm(_get(line, col))
        if v == "PROGRAM_ADDITIONAL":
            return False

    # Baseline workforce capacity rows may be explicitly marked as not app-attributed.
    ms = _norm(_get(line, "MAPPING_STATUS") or _get(line, "MAPPING") or _get(line, "STATUS") or _get(line, "MAP_STATUS"))
    if ms == "BASELINE_NOT_APP_ATTRIBUTED":
        return False

    # Program overhead: program-level shared WF cost, not allocated to app groups.
    team = _norm(_get(line, "TEAMNAME") or _get(line, "TEAM"))
    if team in {"(PROGRAM OVERHEAD)", "PROGRAM OVERHEAD"}:
        return False
    if "PROGRAM OVERHEAD" in team:
        return False

    # Conservative extra hints for overhead buckets.
    group = _norm(_get(line, "GROUPNAME") or _get(line, "APP_GROUP"))
    cost_category = _norm(_get(line, "COST_CATEGORY"))
    subcomponent = _norm(_get(line, "SUBCOMPONENT"))
    workforce_class = _norm(_get(line, "WORKFORCE_CLASS"))
    cost_subtype = _norm(_get(line, "COST_SUBTYPE"))

    # If it is explicitly marked as program-level in any classification column.
    if "PROGRAM" in {subcomponent, workforce_class, cost_subtype}:
        return False

    # If it looks like an overhead bucket and has no app group.
    if group == "" and ("OVERHEAD" in team or team.startswith("(OVERHEAD")):
        return False

    return True


def normalize_mapping_status(line: Any) -> str:
    """Normalize mapping status for display/use in detectors.

    - If mapping is not required -> NOT_IN_SCOPE
    - Else return existing mapping status when present; otherwise UNKNOWN.
    """
    if not is_app_group_mapping_required(line):
        return "NOT_IN_SCOPE"
    for col in ["MAPPING_STATUS", "MAPPING", "STATUS", "MAP_STATUS"]:
        v = str(_get(line, col) or "").strip()
        if v:
            return v
    return "UNKNOWN"
