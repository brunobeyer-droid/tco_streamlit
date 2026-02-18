from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Union

import yaml


DEFAULT_PI_READINESS_CONFIG: Dict[str, Any] = {
    "readiness_thresholds": {
        "ready_min_score": 90,
        "at_risk_min_score": 70,
    },
    "required_checks": {
        "require_ownership": True,
        "require_rates": True,
        "require_nonworkforce": True,
        "require_ado_mapping": True,
        # Demand driver contract: Expected/Forecast uses SWAG-derived Derived FTE only.
        "require_swag_ready_demand": True,
    },
    "gating_rules": {
        "block_if_unassigned_cost_present": True,
        "block_if_missing_rates_for_workforce": True,
        "block_if_missing_ado_mapping_fields": True,
        "block_if_missing_program_team_group_mapping": True,
    },
    "ado_rules": {
        "required_feature_fields": ["TEAM", "APP_GROUP"],
        "max_percent_features_missing_mapping_for_ready": 2.0,
        "max_percent_features_missing_mapping_for_at_risk": 10.0,
    },
}


def _shallow_merge(base: Dict[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if value is None:
            continue
        if isinstance(merged.get(key), dict) and isinstance(value, Mapping):
            inner = merged.get(key, {}).copy()
            for inner_key, inner_val in value.items():
                if inner_val is not None:
                    inner[inner_key] = inner_val
            merged[key] = inner
        else:
            merged[key] = value
    return merged


def load_pi_readiness_config(path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    cfg = copy.deepcopy(DEFAULT_PI_READINESS_CONFIG)
    cfg_path = Path(path) if path else Path(__file__).resolve().parent.parent / "config" / "pi_readiness.yaml"
    try:
        raw_text = cfg_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return cfg
    except Exception:
        return cfg
    try:
        parsed = yaml.safe_load(raw_text)
    except Exception:
        return cfg
    if not isinstance(parsed, Mapping):
        return cfg
    return _shallow_merge(cfg, parsed)


__all__ = ["load_pi_readiness_config", "DEFAULT_PI_READINESS_CONFIG"]
