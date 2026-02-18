from __future__ import annotations

"""
Canonical SoD demand helpers (Derived FTE from features).

This intentionally reuses the Explorer v2 feature-level universe:
- One row per (FEATURE_ID, ADO_YEAR, PI_NUM).
- Demand driver is Derived FTE (SWAG-derived) coming from DB view `VW_TCO_FEATURE_DEMAND`.

Legacy demand views like `VW_DERIVED_FTE_DEMAND` are deprecated and should not be used
for budgeting/costing.
"""

from core.ado_recon import load_explorer_feature_rows, load_derived_fte_by_bucket

# Canonical "ADO feature enriched" dataset (Explorer v2 semantics).
# This is the single source of truth for feature-level Derived FTE demand.
load_ado_features_v2_canon = load_explorer_feature_rows

__all__ = ["load_ado_features_v2_canon", "load_explorer_feature_rows", "load_derived_fte_by_bucket"]
