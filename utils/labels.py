from __future__ import annotations

from typing import Any

TERM_LABELS = {
    "APP_GROUP": "Application group",
    "APPLICATION": "Application",
    "TEAM_DELIVERY": "Team & Delivery",
    "RECURRING_INVOICE": "Recurring invoice",
}


def display_class_label(val: Any) -> str:
    """
    UI-only label normalization for workforce "CLASS" fields.

    Important: this must NOT change stored values used for joins or writes.
    """
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    if s.lower() == "nan":
        return ""
    if s.upper() == "TEAM":
        return "Team Overhead"
    return s


def term_label(key: str, default: str = "") -> str:
    k = str(key or "").strip().upper()
    if not k:
        return default
    return str(TERM_LABELS.get(k) or default or k.replace("_", " ").title())
