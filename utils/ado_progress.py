from __future__ import annotations

from typing import Any, Dict


def normalize_state_category(value: Any) -> str:
    """Map ADO StateCategory to canonical progress buckets."""
    s = str(value or "").strip().lower().replace(" ", "")
    if not s:
        return "proposed"
    if s in {"completed", "done"}:
        return "completed"
    if s in {"inprogress", "resolved", "active"}:
        return "inprogress"
    if s in {"proposed", "new"}:
        return "proposed"
    if s in {"removed", "cut"}:
        # Removed items are tracked as scope churn and should not affect progress %
        return "removed"
    return "inprogress"


def compute_progress_metrics(
    proposed_sp: float,
    inprogress_sp: float,
    completed_sp: float,
) -> Dict[str, float]:
    """Compute total and weighted completion percent.

    Formula:
      TOTAL_SP = PROPOSED + INPROGRESS + COMPLETED
      PCT_COMPLETE = (COMPLETED + 0.5 * INPROGRESS) / TOTAL_SP
    """
    proposed = float(proposed_sp or 0.0)
    inprogress = float(inprogress_sp or 0.0)
    completed = float(completed_sp or 0.0)
    total = proposed + inprogress + completed
    pct = ((completed + 0.5 * inprogress) / total) if total > 0 else 0.0
    return {
        "PROPOSED_SP": proposed,
        "INPROGRESS_SP": inprogress,
        "COMPLETED_SP": completed,
        "TOTAL_SP": total,
        "PCT_COMPLETE": pct,
    }
