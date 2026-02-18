from __future__ import annotations

import re
from typing import Any, Set


PLANNED_NWF_SUBCOMPONENTS: list[str] = [
    "Invoices",
    "Cloud AWS",
    "Cloud Azure",
    "Contractor CS",
    "MSP",
    "Travel",
    "Infra",
    "NWF Other",
]


def get_planned_nwf_subcomponents(db: Any = None) -> Set[str]:
    """Closed set of allowed Planned NWF SUBCOMPONENT labels.

    `db` is accepted for future optional enrichment, but this is intentionally
    a closed set to prevent inventing new NWF types in Actuals.
    """
    return set(PLANNED_NWF_SUBCOMPONENTS)


def normalize_nwf_subcomponent(label: str) -> str:
    """Normalize an NWF subcomponent label into the allowed planned set."""
    raw = str(label or "").strip()
    raw = re.sub(r"\s+", " ", raw)
    allowed = get_planned_nwf_subcomponents()
    return raw if raw in allowed else "NWF Other"

