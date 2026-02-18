from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pathlib import Path
from urllib.parse import urlparse

from db.mssql_backend import fetch_active_ado_profile_config
from core.portfolio_context import get_active_profile_key


_FALLBACK_PROFILE: Dict[str, Any] = {
    "mode": "legacy",
    "work_item_types": {"feature": "Feature", "epic": "Epic"},
    "fields": {"story_points": "Effort", "business_value": "BusinessValue"},
    "swag": {"points_per_fte": 65.0},
    "forecast": {"derived_fte_driver": "SWAG", "velocity_row_fallback": True},
}


def normalize_profile_config(profile: Dict[str, Any]) -> Dict[str, Any]:
    cfg = profile.copy() if isinstance(profile, dict) else {}
    fields = cfg.get("fields") if isinstance(cfg.get("fields"), dict) else {}
    points = fields.get("story_points") or fields.get("points")
    if points and str(points).strip() and str(points).strip() != "Custom_FTE":
        fields["story_points"] = str(points).strip()
    elif str(points).strip() == "Custom_FTE":
        fields.pop("story_points", None)
    if "fte" in fields:
        fields.pop("fte", None)
    if "points" in fields:
        fields.pop("points", None)
    if not str(fields.get("story_points") or "").strip():
        fields.pop("story_points", None)
    cfg["fields"] = fields
    swag = cfg.get("swag") if isinstance(cfg.get("swag"), dict) else {}
    if swag.get("points_per_fte") is None:
        swag["points_per_fte"] = 65.0
    cfg["swag"] = swag
    forecast = cfg.get("forecast") if isinstance(cfg.get("forecast"), dict) else {}
    raw_driver = str(forecast.get("derived_fte_driver") or "SWAG").strip().upper()
    if raw_driver in {"VELOCITY", "SNAPSHOT", "SNAPSHOT_VELOCITY"}:
        driver = "SNAPSHOT_VELOCITY"
    else:
        driver = "SWAG"
    raw_fb = forecast.get("velocity_row_fallback", True)
    if isinstance(raw_fb, bool):
        row_fallback = raw_fb
    else:
        row_fallback = str(raw_fb).strip().lower() not in {"0", "false", "no", "off"}
    cfg["forecast"] = {
        "derived_fte_driver": driver,
        "velocity_row_fallback": bool(row_fallback),
    }
    return cfg


def _load_profiles_config() -> Dict[str, Any]:
    cfg_path = Path(__file__).resolve().parent.parent / "config" / "ado_profiles.yml"
    if not cfg_path.exists():
        return {}
    try:
        import yaml  # type: ignore
    except Exception:
        return {}
    try:
        with cfg_path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _profile_from_key(profile_key: str) -> Optional[Dict[str, Any]]:
    if not profile_key:
        return None
    cfgs = _load_profiles_config()
    key = str(profile_key or "").strip()
    if key in cfgs and isinstance(cfgs[key], dict):
        return cfgs[key]
    return None


def get_active_ado_profile(fetch_df_func: Optional[Any] = None) -> Dict[str, Any]:
    """Return active ADO profile config, or a safe fallback on failure."""
    _ = fetch_df_func
    ctx_key = get_active_profile_key()
    if ctx_key:
        cfg = _profile_from_key(ctx_key)
        if cfg:
            return normalize_profile_config(cfg)
    try:
        cfg = fetch_active_ado_profile_config()
    except Exception:
        return normalize_profile_config(dict(_FALLBACK_PROFILE))
    if not isinstance(cfg, dict):
        return normalize_profile_config(dict(_FALLBACK_PROFILE))
    return normalize_profile_config(cfg)


def get_profile_value(profile: Dict[str, Any], path: str, default: Any = None) -> Any:
    """Fetch a nested value from a dict using dot-path syntax."""
    if not profile or not path:
        return default
    cur: Any = profile
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


def build_workitems_select_expand(profile: Dict[str, Any]) -> Tuple[List[str], str]:
    select_fields = [
        "WorkItemId",
        "WorkItemType",
        "Title",
        "State",
        "ChangedDate",
        "ParentWorkItemId",
    ]
    for key in (
        "fields.story_points",
        "fields.app_name",
        "fields.investment_dimension",
        "fields.business_value",
    ):
        val = get_profile_value(profile, key, None)
        if isinstance(val, str) and val.strip():
            select_fields.append(val.strip())

    expand_clause = (
        "Iteration($select=IterationLevel3,IterationPath,IterationSK),"
        "Area($select=AreaLevel1,AreaLevel2,AreaLevel3,AreaLevel4,AreaPath)"
    )
    return select_fields, expand_clause


def build_feature_query_from_profile(
    org: str,
    project: str,
    entity: str,
    profile: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Backward-compatible query builder used by sync scripts.
    Returns select/expand/filter fragments aligned to the active profile.
    """
    select_fields, expand_clause = build_workitems_select_expand(profile)

    # Ensure deterministic order and no duplicates.
    seen: set[str] = set()
    select_dedup: List[str] = []
    for f in select_fields:
        sf = str(f or "").strip()
        if not sf or sf in seen:
            continue
        seen.add(sf)
        select_dedup.append(sf)

    feature_type = str(get_profile_value(profile, "work_item_types.feature", "Feature") or "Feature").strip() or "Feature"
    pi_field = str(get_profile_value(profile, "iteration_mapping.pi_field", "IterationLevel3") or "IterationLevel3").strip() or "IterationLevel3"
    exclude_states = list(get_profile_value(profile, "filters.exclude_states", []) or [])
    years_prefix = list(get_profile_value(profile, "filters.years_prefix", []) or [])

    filter_parts: List[str] = [f"WorkItemType eq '{feature_type}'"]
    for state in exclude_states:
        sval = str(state or "").strip()
        if sval:
            filter_parts.append(f"State ne '{sval}'")
    year_tokens = [str(y).strip() for y in years_prefix if str(y).strip()]
    if year_tokens:
        ors = [f"startswith(Iteration/{pi_field},'{y}')" for y in year_tokens]
        if ors:
            filter_parts.append("(" + " or ".join(ors) + ")")
    filter_clause = " and ".join(filter_parts)

    base_url = f"https://analytics.dev.azure.com/{org}/{project}/_odata/v3.0-preview/{entity}"
    return {
        "base_url": base_url,
        "select_fields": select_dedup,
        "expand_clause": expand_clause,
        "filter_clause": filter_clause,
    }


def extract_program_team(profile: Dict[str, Any], area_obj: Dict[str, Any]) -> Tuple[str, str]:
    if not isinstance(area_obj, dict):
        return "", ""
    program_key = get_profile_value(profile, "area_mapping.program_level", "AreaLevel2")
    team_key = get_profile_value(profile, "area_mapping.team_level", "Leaf")
    program = str(area_obj.get(program_key) or "").strip() if program_key else ""
    team = ""
    team_key_str = str(team_key or "").strip()
    leaf_keys = {"LEAF", "AREAPATHLEAF", "LEAF_TEAM"}
    if team_key_str.upper() in leaf_keys:
        area_path = str(area_obj.get("AreaPath") or "").strip()
        if area_path:
            segs = [s.strip() for s in area_path.split("\\") if str(s).strip()]
            if segs:
                root = segs[0]
                root_norm = "".join(ch for ch in root.lower() if ch.isalnum())
                project_candidates: List[str] = []
                for k in ("project", "project_name", "ado_project"):
                    v = str(get_profile_value(profile, k, "") or "").strip()
                    if v:
                        project_candidates.append(v)
                base_url = str(get_profile_value(profile, "odata_base_url", "") or "").strip()
                if base_url:
                    try:
                        parts = [p for p in urlparse(base_url).path.split("/") if p]
                        if len(parts) >= 2:
                            project_candidates.append(str(parts[1]).strip())
                    except Exception:
                        pass
                project_candidates.extend(
                    [
                        str(area_obj.get("ProjectName") or "").strip(),
                        str(area_obj.get("Project") or "").strip(),
                    ]
                )
                project_norm = {
                    "".join(ch for ch in str(c).lower() if ch.isalnum())
                    for c in project_candidates
                    if str(c).strip()
                }
                if (
                    root_norm.startswith("portfolio")
                    or root_norm in project_norm
                    or root_norm.replace("portfolio", "", 1) in project_norm
                ) and len(segs) > 1:
                    segs = segs[1:]
            if segs:
                team = str(segs[-1] or "").strip()
    if not team:
        team = str(area_obj.get(team_key_str) or "").strip() if team_key_str else ""
    return program, team


def extract_pi_label(profile: Dict[str, Any], iteration_obj: Dict[str, Any]) -> str:
    if not isinstance(iteration_obj, dict):
        return ""
    pi_key = get_profile_value(profile, "iteration_mapping.pi_field", "IterationLevel3")
    return str(iteration_obj.get(pi_key) or "").strip() if pi_key else ""


def extract_app_name(profile: Dict[str, Any], row: Dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    key = get_profile_value(profile, "fields.app_name", None)
    if not key:
        return ""
    return str(row.get(key) or "").strip()


def extract_points(profile: Dict[str, Any], row: Dict[str, Any]) -> float:
    if not isinstance(row, dict):
        return 0.0
    key = get_profile_value(profile, "fields.story_points", None)
    if not key:
        return 0.0
    try:
        return float(row.get(key) or 0.0)
    except Exception:
        return 0.0


def compute_derived_fte_swag(profile: Dict[str, Any], points: float) -> float:
    pts = float(points or 0.0)
    ppf = get_profile_value(profile, "swag.points_per_fte", 65.0)
    try:
        denom = float(ppf or 0.0)
    except Exception:
        denom = 65.0
    if denom == 0:
        return 0.0
    return pts / denom
