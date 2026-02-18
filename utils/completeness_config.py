from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Union

import yaml

DEFAULT_COMPLETENESS_CONFIG: Dict[str, Any] = {
    "version": 1,
    "thresholds": {"high": 90, "med": 70},
    "weights": {
        "ownership": 10,
        "workforce": 15,
        "rates": 10,
        "nonworkforce": 15,
        "adoinputs": 50,
    },
    "rules": {
        "ownership": {"exclude_groupnames": ["(Unassigned)", "Unassigned", ""]},
        "workforce": {"min_workforce_cost": 0.0},
        "rates": {"require_rates_only_if_workforce_present": True},
        "nonworkforce": {"min_nonworkforce_cost": 0.0},
        "adoinputs": {"accept_derived_fte": True, "accept_feature_count": True},
    },
}


def _shallow_merge(base: Dict[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    """Merge override into base (one level deep, preserving defaults)."""
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


def load_completeness_config(path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    """Load completeness config from YAML with safe defaults and shallow merge."""
    cfg = copy.deepcopy(DEFAULT_COMPLETENESS_CONFIG)
    cfg_path = Path(path) if path else Path(__file__).resolve().parent.parent / "config" / "completeness.yaml"
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

    # Backward compatibility: map legacy "enhancements" key to "adoinputs"
    merged = _shallow_merge(cfg, parsed)
    if "enhancements" in merged.get("weights", {}):
        merged["weights"]["adoinputs"] = merged["weights"].get("enhancements", merged["weights"].get("adoinputs", 0))
        merged["weights"].pop("enhancements", None)
    if "enhancements" in merged.get("rules", {}):
        merged["rules"]["adoinputs"] = merged["rules"].get("enhancements", merged["rules"].get("adoinputs", {}))
        merged["rules"].pop("enhancements", None)
    return merged


__all__ = ["load_completeness_config", "DEFAULT_COMPLETENESS_CONFIG"]
