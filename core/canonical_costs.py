from __future__ import annotations

"""
Canonical cost API (single source of truth).

This module is intentionally UI-free (no Streamlit imports) and is meant to be called
by Streamlit pages and helper layers later.

Business scenarios
------------------
The app supports two scenario concepts:

1) Baseline (Budget)
   - "What programs are allowed to ask for per year".
   - Workforce baseline comes from allocated headcount (capacity anchor) + effective rates.
   - Non-workforce baseline comes from non-ADO sources (invoices, program additional costs, MSP, etc.).

2) Projected (Expected)
   - Projected spend through the year.
   - Workforce projected costs MUST be based on Derived FTE demand from Explorer v2
     (`DERIVED_FTE_FEATURE_VELOCITY` with fallback to `DERIVED_FTE_FEATURE`).
   - MSP costs MUST follow MSP-specific allocation logic and MUST NOT be computed via Derived FTE.

3) Actual (Actuals) [optional]
   - Program-level actuals (typically Apptio) used for diagnostics and variance context.
   - This scenario is not app-attributed.

Canonical database views used (Azure SQL)
----------------------------------------
- `VW_TCO_WF_LABOR_SPLIT`: feature×labor-type costs for ADO workload, built from velocity-aware Derived FTE
  (`DERIVED_FTE_FEATURE_VELOCITY`, fallback to SWAG-derived `DERIVED_FTE_FEATURE`) and excluding MSP features.
- `VW_MSP_COSTS`: MSP costs per PI/app group using MSP assignment + rate tables.
- `VW_INVOICE_SPEND_PI` + `VW_PROGRAM_ADDITIONAL_COSTS_PI`: non-ADO/non-workforce PI costs.
- `VW_TEAM_HEADCOUNT_EFFECTIVE` / `VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE`: stable staffing plan headcount by team×PI.
- `VW_TEAM_WEIGHTED_RATES`: effective per-PI rates by team (includes contractor rates).
- `VW_PROGRAM_COMPOSITION_EFFECTIVE` + `VW_PROGRAM_RATE_EFFECTIVE`: program overhead headcount and rate for baseline overhead.

Legacy/obsolete inputs intentionally avoided
--------------------------------------------
- Any manual/custom effort overrides from ADO are NOT used here.
- Feature-level Projected costs are sourced from `VW_TCO_WF_LABOR_SPLIT` using velocity-aware Derived FTE.
"""

from dataclasses import dataclass
import html
import os
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

import pandas as pd
import warnings

from core.nwf_program import (
    get_program_nwf_actuals_by_pi,
    get_program_nwf_actuals_by_pi_breakdown,
    get_program_nwf_expected_by_pi,
)
from core.nwf_taxonomy import normalize_nwf_subcomponent


CANONICAL_COSTS_VERSION = "2026-02-17_projected_snapshot_velocity_p3"

_PROJECTED_VELOCITY_READY: bool = False
_PROJECTED_DEMAND_SNAPSHOT_READY: bool = False


def _fq(name: str) -> str:
    """Return a fully-qualified DB object name when the DB facade is available."""
    try:
        from db import _fq as _db_fq  # type: ignore

        return _db_fq(name)
    except Exception:
        return name


def _normalize_projected_driver_mode(raw: Any, default: str = "SWAG") -> str:
    s = str(raw or "").strip().upper()
    if s in {"SNAPSHOT", "SNAPSHOT_VELOCITY", "VELOCITY"}:
        return "SNAPSHOT_VELOCITY"
    if s in {"SWAG", "DERIVED_FTE", "DERIVED_FTE_FEATURE"}:
        return "SWAG"
    return default


def _boolish(raw: Any, default: bool = True) -> bool:
    if isinstance(raw, bool):
        return bool(raw)
    s = str(raw or "").strip().lower()
    if not s:
        return bool(default)
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _profile_projected_driver_settings(fetch: Optional[FetchFn]) -> tuple[str, bool]:
    """Read projected driver settings from active ADO profile (best-effort)."""
    default_mode = "SWAG"
    default_row_fallback = True
    if fetch is None:
        return default_mode, default_row_fallback
    q = """
      SELECT TOP 1
        UPPER(LTRIM(RTRIM(COALESCE(JSON_VALUE(CONFIG_JSON, '$.forecast.derived_fte_driver'), '')))) AS DRIVER_MODE,
        LOWER(LTRIM(RTRIM(COALESCE(JSON_VALUE(CONFIG_JSON, '$.forecast.velocity_row_fallback'), '')))) AS ROW_FALLBACK
      FROM ADO_PROFILES
      WHERE IS_ACTIVE = 1
      ORDER BY UPDATED_AT DESC
    """
    q_any = """
      SELECT TOP 1
        UPPER(LTRIM(RTRIM(COALESCE(JSON_VALUE(CONFIG_JSON, '$.forecast.derived_fte_driver'), '')))) AS DRIVER_MODE,
        LOWER(LTRIM(RTRIM(COALESCE(JSON_VALUE(CONFIG_JSON, '$.forecast.velocity_row_fallback'), '')))) AS ROW_FALLBACK
      FROM ADO_PROFILES
      ORDER BY UPDATED_AT DESC
    """
    try:
        df = fetch(q, None)
    except Exception:
        df = None
    if df is None or df.empty:
        try:
            df = fetch(q_any, None)
        except Exception:
            df = None
    if df is None or df.empty:
        return default_mode, default_row_fallback
    row = df.iloc[0]
    mode = _normalize_projected_driver_mode(row.get("DRIVER_MODE"), default=default_mode)
    row_fallback = _boolish(row.get("ROW_FALLBACK"), default=default_row_fallback)
    return mode, row_fallback


def _projected_fte_driver_mode(db: Any = None) -> str:
    """Projected FTE demand driver mode.

    Supported values:
    - SNAPSHOT_VELOCITY: compute demand from SWAG points using
      TEAM_VELOCITY_SNAPSHOT baseline points, fallback to global baseline.
    - SWAG (default when not configured): stable path based on DERIVED_FTE_FEATURE.
    """
    env_raw = str(os.getenv("TCO_PROJECTED_FTE_DRIVER", "") or "").strip()
    if env_raw:
        return _normalize_projected_driver_mode(env_raw, default="SWAG")
    fetch: Optional[FetchFn] = None
    try:
        fetch = _resolve_fetch(db) if db is not None else None
    except Exception:
        fetch = None
    mode, _ = _profile_projected_driver_settings(fetch)
    return mode


def _projected_velocity_row_fallback_enabled(db: Any = None) -> bool:
    """Whether snapshot mode can fallback row-by-row to global SWAG baseline points."""
    env_raw = str(os.getenv("TCO_PROJECTED_VELOCITY_ROW_FALLBACK", "") or "").strip()
    if env_raw:
        return _boolish(env_raw, default=True)
    fetch: Optional[FetchFn] = None
    try:
        fetch = _resolve_fetch(db) if db is not None else None
    except Exception:
        fetch = None
    _, row_fallback = _profile_projected_driver_settings(fetch)
    return bool(row_fallback)


def _ensure_projected_velocity_snapshot_ready() -> None:
    """Best-effort DDL guard so projected snapshot mode does not fail on missing table."""
    global _PROJECTED_VELOCITY_READY
    if _PROJECTED_VELOCITY_READY:
        return
    try:
        from db import ensure_tco_team_velocity_snapshot_table  # type: ignore

        if callable(ensure_tco_team_velocity_snapshot_table):
            ensure_tco_team_velocity_snapshot_table()
    except Exception:
        # Keep fail-open behavior; query path still has SWAG fallback controls.
        pass
    _PROJECTED_VELOCITY_READY = True


def _projected_demand_snapshot_enabled() -> bool:
    raw = str(os.getenv("TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED", "1") or "").strip()
    return _boolish(raw, default=True)


def _projected_demand_snapshot_auto_refresh_enabled() -> bool:
    raw = str(os.getenv("TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH", "0") or "").strip()
    return _boolish(raw, default=False)


def _projected_demand_snapshot_strict_scope_enabled() -> bool:
    raw = str(os.getenv("TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE", "1") or "").strip()
    return _boolish(raw, default=True)


def _projected_velocity_failopen_enabled() -> bool:
    # Reliability guardrail:
    # when velocity snapshot rows are unavailable for the requested window/scope,
    # fail-open to SWAG demand instead of returning zero projected WF.
    raw = str(os.getenv("TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG", "1") or "").strip()
    return _boolish(raw, default=True)


def _ensure_projected_demand_snapshot_ready() -> None:
    """Best-effort DDL guard for projected-demand snapshot reads."""
    global _PROJECTED_DEMAND_SNAPSHOT_READY
    if _PROJECTED_DEMAND_SNAPSHOT_READY:
        return
    try:
        from db import ensure_tco_projected_demand_snapshot_table  # type: ignore

        if callable(ensure_tco_projected_demand_snapshot_table):
            ensure_tco_projected_demand_snapshot_table()
    except Exception:
        pass
    _PROJECTED_DEMAND_SNAPSHOT_READY = True


def _projected_demand_snapshot_available(
    db: Any,
    *,
    years: Sequence[int],
    pis: Sequence[int],
    program_ids: Sequence[str],
    team_ids: Sequence[str],
) -> bool:
    """Whether projected-demand snapshot has rows for current scope."""
    if not _projected_demand_snapshot_enabled():
        return False
    try:
        fetch = _resolve_fetch(db) if db is not None else None
    except Exception:
        fetch = None
    if fetch is None:
        return False

    _ensure_projected_demand_snapshot_ready()

    where_parts: list[str] = ["1=1"]
    params: list[Any] = []
    _add_in(where_parts, params, "s.YEAR", [int(y) for y in years])
    _add_in(where_parts, params, "s.PI", [int(p) for p in pis])
    _add_in(where_parts, params, "s.PROGRAMID", [str(v).strip() for v in program_ids if str(v).strip()])
    _add_in(where_parts, params, "s.TEAMID", [str(v).strip() for v in team_ids if str(v).strip()])
    exists_sql = f"""
      SELECT TOP 1 1 AS OK
      FROM {_fq('TCO_PROJECTED_DEMAND_SNAPSHOT')} s
      WHERE {' AND '.join(where_parts)}
    """
    try:
        probe = fetch(exists_sql, tuple(params) if params else None)
    except Exception:
        probe = None
    if probe is not None and not probe.empty:
        return True

    # Optional one-shot refresh when scope is year-bounded.
    if not _projected_demand_snapshot_auto_refresh_enabled():
        return False
    refresh_year = max([int(y) for y in years], default=0)
    if refresh_year <= 0:
        return False
    try:
        from db import refresh_tco_projected_demand_snapshot  # type: ignore

        if callable(refresh_tco_projected_demand_snapshot):
            refresh_tco_projected_demand_snapshot(
                year=refresh_year,
                include_prior_year=True,
                reference_year_only=False,
            )
            probe = fetch(exists_sql, tuple(params) if params else None)
            return bool(probe is not None and not probe.empty)
    except Exception:
        return False
    return False


def _projected_velocity_snapshot_available(
    db: Any,
    *,
    years: Sequence[int],
    pis: Sequence[int],
    team_ids: Sequence[str],
) -> bool:
    """Whether team velocity snapshot has rows for the requested window/scope."""
    if db is None:
        # Keep deterministic SQL generation for unit tests and non-DB callers.
        return True
    try:
        fetch = _resolve_fetch(db)
    except Exception:
        return True

    where_parts: list[str] = ["1=1"]
    params: list[Any] = []
    _add_in(where_parts, params, "TRY_CONVERT(INT, s.YEAR)", [int(y) for y in years])
    _add_in(where_parts, params, "TRY_CONVERT(INT, s.PI)", [int(p) for p in pis])
    _add_in(where_parts, params, "s.TEAMID", [str(v).strip() for v in team_ids if str(v).strip()])
    sql = f"""
      SELECT TOP 1 1 AS OK
      FROM {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')} s
      WHERE {' AND '.join(where_parts)}
    """
    try:
        probe = fetch(sql, tuple(params) if params else None)
        return bool(probe is not None and not probe.empty)
    except Exception:
        return False


FetchFn = Callable[[str, Optional[Iterable[Any]]], pd.DataFrame]
Filters = Mapping[str, Any]


def _to_int_series(x, default: int = 0):
    """Convert a Series-like value to an integer dtype safely across pandas versions.

    - Prefer pandas' nullable `Int64` when available.
    - Fall back to numpy `int64` (fills missing with `default`) when `Int64` is unsupported.

    This helper also tolerates scalar inputs (e.g., when a missing column yields `None`).
    """
    s = pd.to_numeric(x, errors="coerce")
    if not isinstance(s, pd.Series):
        try:
            return int(default) if pd.isna(s) else int(s)
        except Exception:
            return int(default)
    try:
        # Prefer pandas' nullable integer dtype when available.
        int64_nullable = pd.Int64Dtype()  # type: ignore[attr-defined]
        return s.astype(int64_nullable)
    except Exception:
        return s.fillna(default).astype("int64")


@dataclass(frozen=True)
class ScenarioSpec:
    """Normalized scenario specification."""

    name: str  # "Baseline" or "Projected"


def _resolve_fetch(db: Any) -> FetchFn:
    """Normalize `db` to a `fetch_df(sql, params)` callable."""
    if callable(db):
        return db  # type: ignore[return-value]
    if hasattr(db, "fetch_df") and callable(getattr(db, "fetch_df")):
        return getattr(db, "fetch_df")
    raise TypeError("`db` must be a callable (sql, params)->DataFrame or expose a `fetch_df(sql, params)` method.")


def _normalize_scenario(scenario: str) -> ScenarioSpec:
    raw = str(scenario or "").strip().upper().replace(" ", "_")
    if raw in {"PROJECTED", "EXPECTED"}:
        return ScenarioSpec(name="Projected")
    if raw in {"BASELINE", "BUDGET"}:
        return ScenarioSpec(name="Baseline")
    if raw in {"ACTUAL", "ACTUALS"}:
        return ScenarioSpec(name="Actual")
    raise ValueError("scenario must be 'Baseline', 'Projected', or 'Actual' (aliases: Budget, Expected, Actuals).")


def normalize_cost_category_value(v: Any) -> str:
    s = str(v or "").strip().upper().replace(" ", "")
    if s in {"WF", "WORK", "WORKFORCE", "WORK_FORCE"}:
        return "WORK_FORCE"
    if s in {"NWF", "NONWORK", "NONWORKFORCE", "NON_WORK", "NON_WORK_FORCE", "NON-WORKFORCE"}:
        return "NON_WORK_FORCE"
    return s


def _apply_wf_taxonomy_tags(df: pd.DataFrame) -> pd.DataFrame:
    """Stamp WF taxonomy tags for UI consumers (no numeric impact).

    Adds/keeps:
    - WF_LAYER2: "SoD" | "Overhead" (WF rows only)
    - WF_SUBTYPE: Delivery | Contractor C | Contractor CS | Team Overhead | Program Overhead | Other SoD | Other Overhead (WF rows only)

    Rules:
    - ProgramFTE-derived program overhead rows are WF Overhead ("Program Overhead").
    - TeamFTE-derived team overhead rows are WF SoD ("Team Overhead").
    - Other WF labor rows are WF SoD with subtypes derived from existing SUBCOMPONENT labels.
    """
    if df is None or df.empty:
        return df

    out = df
    if "WF_LAYER2" not in out.columns:
        out["WF_LAYER2"] = ""
    if "WF_SUBTYPE" not in out.columns:
        out["WF_SUBTYPE"] = ""

    cat = out.get("COST_CATEGORY", pd.Series(dtype=str)).fillna("").astype(str).str.upper().str.strip()
    is_wf = cat.eq("WORK_FORCE")
    if not is_wf.any():
        return out

    # Do not overwrite pre-stamped values; only fill blanks.
    layer2 = out.get("WF_LAYER2", "").fillna("").astype(str).str.strip()
    subtype = out.get("WF_SUBTYPE", "").fillna("").astype(str).str.strip()

    sub = out.get("SUBCOMPONENT", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    sub_u = sub.str.upper()
    team = out.get("TEAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    team_u = team.str.upper()
    src = out.get("SOURCE", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    src_u = src.str.upper()

    # Program overhead: stable marker is TEAMNAME="(Program overhead)" (Baseline + Projected correction).
    is_program_overhead = (
        team_u.eq("(PROGRAM OVERHEAD)")
        | team_u.str.contains("PROGRAM OVERHEAD", na=False)
        | sub_u.eq("PROGRAM")
        | sub_u.str.contains("PROGRAM OVERHEAD", na=False)
    )
    # Team overhead: for baseline, SUBCOMPONENT="Team" represents TEAMFTE headcount (Teams page) and is part of SoD.
    is_team_overhead = (src_u.eq("TCO_BASELINE") & sub_u.eq("TEAM")) | sub_u.eq("TEAM OVERHEAD")

    # WF_LAYER2
    layer2 = layer2.where(~(is_wf & layer2.eq("") & ~is_program_overhead), "SoD")
    layer2 = layer2.where(~(is_wf & layer2.eq("") & is_program_overhead), "Overhead")

    # WF_SUBTYPE
    # Program Overhead rows
    subtype = subtype.where(~(is_wf & subtype.eq("") & is_program_overhead), "Program Overhead")

    # Team Overhead rows (SoD)
    subtype = subtype.where(~(is_wf & subtype.eq("") & is_team_overhead), "Team Overhead")

    # Other WF SoD labor buckets from SUBCOMPONENT
    is_other_wf = is_wf & subtype.eq("")
    if is_other_wf.any():
        s = sub.loc[is_other_wf].fillna("").astype(str).str.lower()
        mapped = pd.Series("Other SoD", index=s.index, dtype=str)
        mapped = mapped.where(~s.str.contains("delivery", na=False), "Delivery")
        mapped = mapped.where(~(s.str.contains("contractor cs", na=False) | s.str.contains("cs contractor", na=False) | s.str.contains("msp", na=False)), "Contractor CS")
        mapped = mapped.where(~s.str.contains("contractor", na=False), "Contractor C")
        subtype.loc[is_other_wf] = mapped

    # Fill remaining WF blanks safely.
    is_wf_overhead = is_wf & layer2.eq("Overhead")
    subtype = subtype.where(~(is_wf & subtype.eq("")), "Other SoD")
    subtype = subtype.where(~(is_wf_overhead & subtype.eq("Other SoD")), "Other Overhead")

    out["WF_LAYER2"] = layer2
    out["WF_SUBTYPE"] = subtype
    return out


def _apply_sod_taxonomy_tags(df: pd.DataFrame) -> pd.DataFrame:
    """Stamp SOD taxonomy tags for UI consumers (no numeric impact).

    Adds/keeps:
    - SOD_LAYER2: "SoD" | "Overhead" | "" (all rows)
    - SOD_SUBTYPE: Delivery | Contractor C | Contractor CS | Team Overhead | Program Overhead | "" (all rows)
    """
    if df is None or df.empty:
        return df

    out = df
    if "SOD_LAYER2" not in out.columns:
        out["SOD_LAYER2"] = ""
    if "SOD_SUBTYPE" not in out.columns:
        out["SOD_SUBTYPE"] = ""

    layer2 = out.get("SOD_LAYER2", "").fillna("").astype(str).str.strip()
    subtype = out.get("SOD_SUBTYPE", "").fillna("").astype(str).str.strip()
    sub = out.get("SUBCOMPONENT", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    sub_u = sub.str.upper()
    team = out.get("TEAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    team_u = team.str.upper()
    src = out.get("SOURCE", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    src_u = src.str.upper()

    is_program_overhead = team_u.eq("(PROGRAM OVERHEAD)") | sub_u.eq("PROGRAM") | sub_u.str.contains("PROGRAM OVERHEAD", na=False)
    is_team_overhead = (src_u.eq("TCO_BASELINE") & sub_u.eq("TEAM")) | sub_u.eq("TEAM OVERHEAD")
    is_delivery = sub_u.isin({"DELIVERY", "DELIVERY TEAM"})
    is_contractor_c = sub_u.eq("CONTRACTOR C")
    is_contractor_cs = sub_u.eq("CONTRACTOR CS")

    layer2 = layer2.where(~(layer2.eq("") & is_program_overhead), "Overhead")
    subtype = subtype.where(~(subtype.eq("") & is_program_overhead), "Program Overhead")

    layer2 = layer2.where(~(layer2.eq("") & is_team_overhead), "SoD")
    subtype = subtype.where(~(subtype.eq("") & is_team_overhead), "Team Overhead")

    layer2 = layer2.where(~(layer2.eq("") & is_delivery), "SoD")
    subtype = subtype.where(~(subtype.eq("") & is_delivery), "Delivery")

    layer2 = layer2.where(~(layer2.eq("") & is_contractor_c), "SoD")
    subtype = subtype.where(~(subtype.eq("") & is_contractor_c), "Contractor C")

    layer2 = layer2.where(~(layer2.eq("") & is_contractor_cs), "SoD")
    subtype = subtype.where(~(subtype.eq("") & is_contractor_cs), "Contractor CS")

    out["SOD_LAYER2"] = layer2
    out["SOD_SUBTYPE"] = subtype
    return out


def _mapping_status_filter(filters: Optional[Filters], *, scenario: str) -> Optional[list[str]]:
    """Return an explicit mapping-status filter list if requested by the caller.

    Supported filter keys:
    - mapping_status: list[str] (explicit allow-list)
    - include_unmapped: bool (when False, implies mapping_status=["MAPPED"] for Projected)
    """
    f = filters or {}
    statuses = [str(s).strip().upper() for s in _as_list(f.get("mapping_status")) if str(s).strip()]
    if statuses:
        return statuses
    include_unmapped = f.get("include_unmapped")
    if include_unmapped is False and str(scenario).strip().upper() in {"PROJECTED", "EXPECTED"}:
        # "Mapped-only" is meant to hide unmapped ADO app groups, not to hide explicit allocation buckets.
        return ["MAPPED", "UNALLOCATED", "PROGRAM_LEVEL"]
    return None


def get_cost_lines(
    db: Any,
    scenario: str = "Projected",
    filters: Optional[dict] = None,
    *,
    include_allocated_actuals: bool = False,
    include_native_actuals: bool = True,
    include_program_overhead: bool = True,
) -> pd.DataFrame:
    """Return detailed canonical cost lines for the selected scenario.

    Parameters
    ----------
    db:
      A `fetch_df(sql, params)` callable or an object exposing `fetch_df`.
    scenario:
      "Projected" (aliases: Expected), "Baseline" (aliases: Budget), or "Actual" (aliases: Actuals).
    filters:
      Standard filters: year, pi, program, team, app_group (plus optional `location` for baseline overhead rates).

      Mapping filters (no effect unless explicitly passed by the page):
      - include_unmapped: bool
          If False, Projected lines are filtered to mapped app-group rows only.
      - mapping_status: list[str]
          Explicit allow-list of mapping statuses (e.g., ["MAPPED"]).
    include_allocated_actuals:
      Reserved for future app-attribution of Actual NWF. Phase 3 returns program-level Actuals only.
    include_native_actuals:
      Only applies when `scenario` is "Actual". When True, returns the native program-level actuals (not app-attributed).

    Returns
    -------
    DataFrame with at least:
      YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, COST_CATEGORY, SUBCOMPONENT, SOURCE, AMOUNT, MAPPING_STATUS

    Allocation transparency (optional columns when applicable):
      - SCENARIO: stable identifier for the row layer (e.g., "Projected" vs "Actual")
      - ALLOCATION_DRIVER: reserved (Phase 3 does not allocate NWF to app groups)
      - SHARE: reserved (Phase 3 does not allocate NWF to app groups)
    """
    # Program overhead scope gating:
    # - Program overhead is WORK_FORCE with SUBCOMPONENT like "Program" (Baseline uses TEAMNAME="(Program overhead)").
    # - It must be included at program scope, but excluded whenever teams are explicitly selected.
    f0 = filters or {}
    team_filters0 = [str(t).strip() for t in _as_list(f0.get("team")) if str(t).strip()]
    include_program_overhead_effective = bool(include_program_overhead)
    if "include_program_overhead" in f0:
        include_program_overhead_effective = bool(f0.get("include_program_overhead"))
    if team_filters0:
        include_program_overhead_effective = False

    # DataFrame mode: allow callers (pages) to pass preloaded cost lines to avoid repeated DB calls.
    # This is intentionally conservative: it does NOT re-derive costs; it only filters/normalizes.
    if isinstance(db, pd.DataFrame):
        spec = _normalize_scenario(scenario)
        f = filters or {}
        out = db.copy()

        # Column aliases (for compatibility with other loaders).
        if "YEAR" not in out.columns and "FISCAL_YEAR" in out.columns:
            out["YEAR"] = out["FISCAL_YEAR"]
        if "PI" not in out.columns and "PI_NUM" in out.columns:
            out["PI"] = out["PI_NUM"]
        if "PROGRAMNAME" not in out.columns and "PROGRAM" in out.columns:
            out["PROGRAMNAME"] = out["PROGRAM"]
        if "TEAMNAME" not in out.columns and "TEAM" in out.columns:
            out["TEAMNAME"] = out["TEAM"]
        if "GROUPNAME" not in out.columns and "APP_GROUP" in out.columns:
            out["GROUPNAME"] = out["APP_GROUP"]
        if "AMOUNT" not in out.columns and "TOTAL_COST" in out.columns:
            out["AMOUNT"] = out["TOTAL_COST"]

        # If the incoming frame includes a scenario column, restrict to the requested scenario.
        if "SCENARIO" in out.columns:
            scen_norm = out["SCENARIO"].apply(_scenario_label_to_name)
            out = out[scen_norm.eq(spec.name) | scen_norm.isna()].copy()

        # Normalize dtypes and required text columns.
        out["YEAR"] = _to_int_series(out.get("YEAR"))
        out["PI"] = _to_int_series(out.get("PI"))
        for c in ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"]:
            if c not in out.columns:
                out[c] = ""
            out[c] = out[c].fillna("").astype(str).str.strip()
        if "COST_CATEGORY" in out.columns:
            out["COST_CATEGORY"] = out["COST_CATEGORY"].map(normalize_cost_category_value)
        if "AMOUNT" not in out.columns:
            out["AMOUNT"] = pd.NA
        out["AMOUNT"] = pd.to_numeric(out["AMOUNT"], errors="coerce").fillna(0.0).astype(float)

        # Apply standard filters (case-insensitive for names).
        years = [int(y) for y in _as_list(f.get("year")) if str(y).strip().isdigit()]
        pis = [int(p) for p in _as_list(f.get("pi")) if str(p).strip().isdigit()]
        programs = [str(p).strip() for p in _as_list(f.get("program")) if str(p).strip()]
        teams = [str(t).strip() for t in _as_list(f.get("team")) if str(t).strip()]
        groups = [str(g).strip() for g in _as_list(f.get("app_group")) if str(g).strip()]

        if years:
            out = out[out["YEAR"].isin([int(y) for y in years])].copy()
        # --- Baseline PI=0 expansion (annual -> quarterly) ---
        if spec.name == "Baseline" and pis:
            # Expand annual baseline rows (PI==0) into quarterly PIs so PI filters don’t wipe baseline out.
            pi_int = pd.to_numeric(out.get("PI"), errors="coerce").fillna(0).astype(int)
            base0 = out.loc[pi_int.eq(0)].copy()
            if base0 is not None and not base0.empty:
                target_pis = sorted({int(x) for x in pis if int(x) != 0})
                expanded: list[pd.DataFrame] = []
                for pi_num in target_pis:
                    tmp = base0.copy()
                    tmp["PI"] = int(pi_num)
                    # Split annual totals evenly across 4 PIs (even if the caller requests a subset).
                    tmp["AMOUNT"] = pd.to_numeric(tmp.get("AMOUNT"), errors="coerce").fillna(0.0) / 4.0
                    if "FTE" in tmp.columns:
                        tmp["FTE"] = pd.to_numeric(tmp.get("FTE"), errors="coerce").fillna(0.0) / 4.0
                    expanded.append(tmp)
                if expanded:
                    out_non0 = out.loc[~pi_int.eq(0)].copy()
                    out = pd.concat([out_non0, *expanded], ignore_index=True, sort=False)
                    out["PI"] = _to_int_series(out.get("PI"))
        if pis:
            out = out[out["PI"].isin([int(p) for p in pis])].copy()
        if programs:
            prog_set = {p.upper() for p in programs}
            out = out[out["PROGRAMNAME"].astype(str).str.upper().isin(prog_set)].copy()
        if teams:
            team_set = {t.upper() for t in teams}
            out = out[out["TEAMNAME"].astype(str).str.upper().isin(team_set)].copy()
        if groups and spec.name == "Projected":
            group_set = {g.upper() for g in groups}
            out = out[out["GROUPNAME"].astype(str).str.upper().isin(group_set)].copy()

        # Ensure mapping-status semantics match canonical outputs.
        statuses = _mapping_status_filter(filters, scenario=spec.name)
        if spec.name == "Projected":
            if "MAPPING_STATUS" not in out.columns:
                group = out.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
                status = pd.Series("UNMAPPED_APP_GROUP", index=out.index, dtype="object")
                status.loc[group.ne("")] = "MAPPED"
                status.loc[group.eq("(Unallocated NWF)")] = "UNALLOCATED"
                status.loc[group.eq("(Program NWF)")] = "PROGRAM_LEVEL"
                out["MAPPING_STATUS"] = status
            else:
                out["MAPPING_STATUS"] = out["MAPPING_STATUS"].fillna("").astype(str).str.strip().str.upper()
            if statuses:
                out = out[out["MAPPING_STATUS"].isin(statuses)].copy()
            if f.get("include_unmapped") is True or (statuses and "UNMAPPED_APP_GROUP" in statuses):
                out.loc[out["MAPPING_STATUS"] == "UNMAPPED_APP_GROUP", "GROUPNAME"] = "(Needs mapping)"

        elif spec.name == "Baseline":
            group = out.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
            status = pd.Series("BASELINE_NOT_APP_ATTRIBUTED", index=out.index, dtype="object")
            status.loc[group.ne("")] = "MAPPED"
            out["MAPPING_STATUS"] = status
            if statuses:
                out = out[out["MAPPING_STATUS"].isin(statuses)].copy()

        else:
            # Actuals are program-level by design in Phase 3.
            if "MAPPING_STATUS" not in out.columns:
                out["MAPPING_STATUS"] = "PROGRAM_LEVEL"
            if statuses:
                out = out[out["MAPPING_STATUS"].fillna("").astype(str).str.upper().isin(statuses)].copy()

        out["SCENARIO"] = spec.name
        if "ALLOCATION_DRIVER" not in out.columns:
            out["ALLOCATION_DRIVER"] = ""
        if "SHARE" not in out.columns:
            out["SHARE"] = pd.Series([pd.NA] * len(out.index), dtype="Float64")
        # Demand driver contract: pages should consume FTE only
        # (snapshot-velocity demand for Projected, with controlled fallback).
        if "FTE" not in out.columns:
            out["FTE"] = pd.Series([pd.NA] * len(out.index), dtype="Float64")
        out["FTE"] = pd.to_numeric(out["FTE"], errors="coerce")
        out["CANONICAL_VERSION"] = CANONICAL_COSTS_VERSION
        out = _apply_wf_taxonomy_tags(out)
        out = _apply_sod_taxonomy_tags(out)

        # Ensure taxonomy columns exist even when the filtered slice is empty.
        if "WF_LAYER2" not in out.columns:
            out["WF_LAYER2"] = ""
        if "WF_SUBTYPE" not in out.columns:
            out["WF_SUBTYPE"] = ""
        if "SOD_LAYER2" not in out.columns:
            out["SOD_LAYER2"] = ""
        if "SOD_SUBTYPE" not in out.columns:
            out["SOD_SUBTYPE"] = ""

        if not include_program_overhead_effective:
            sub = out.get("SUBCOMPONENT", pd.Series(dtype=str)).fillna("").astype(str)
            cat = out.get("COST_CATEGORY", pd.Series(dtype=str)).fillna("").astype(str)
            is_program = sub.str.upper().str.contains("PROGRAM", na=False)
            is_work_force = cat.str.upper().eq("WORK_FORCE")
            out = out.loc[~(is_program & is_work_force)].copy()

        # Data-contract guardrails: diagnostics only (never change financial math).
        try:
            from core.data_contracts import apply_cost_lines_contract

            out2, issues = apply_cost_lines_contract(
                out,
                context=f"canonical_costs.get_pi_costs[{spec.name}]",
                required_cols=["SCENARIO", "COST_CATEGORY", "SOURCE", "SUBCOMPONENT", "GROUPNAME"],
                amount_cols=["AMOUNT"],
                warn=False,
            )
            if issues:
                warnings.warn("; ".join(issues), RuntimeWarning)
            out = out2
        except Exception:
            pass

        return out[
            [
                "YEAR",
                "PI",
                "PROGRAMNAME",
                "TEAMNAME",
                "GROUPNAME",
                "COST_CATEGORY",
                "SUBCOMPONENT",
                "WF_LAYER2",
                "WF_SUBTYPE",
                "SOD_LAYER2",
                "SOD_SUBTYPE",
                "SOURCE",
                "AMOUNT",
                "MAPPING_STATUS",
                "SCENARIO",
                "ALLOCATION_DRIVER",
                "SHARE",
                "FTE",
                "CANONICAL_VERSION",
            ]
        ]

    fetch = _resolve_fetch(db)
    spec_for_query = _normalize_scenario(scenario)
    f = filters or {}
    pi_requested = [int(p) for p in _as_list(f.get("pi")) if str(p).strip().isdigit()]
    filters_for_sql = filters
    if spec_for_query.name == "Baseline" and pi_requested:
        # Baseline often stores annual totals as PI=0. If we push a PI filter into SQL we can wipe out
        # baseline rows entirely; fetch without PI filtering and expand PI=0 rows in Pandas below.
        filters_for_sql = dict(filters or {})
        filters_for_sql.pop("pi", None)

    sql, params, spec = _cost_lines_sql(db, scenario, filters_for_sql)

    select_sql = f"""
      {sql}
      SELECT
        YEAR,
        PI,
        PROGRAMNAME,
        TEAMNAME,
        GROUPNAME,
        COST_CATEGORY,
        SUBCOMPONENT,
        SOURCE,
        CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
        CAST(FTE AS FLOAT) AS FTE
      FROM base
    """
    out = fetch(select_sql, params)
    if out is None:
        out = pd.DataFrame(
            columns=[
                "YEAR",
                "PI",
                "PROGRAMNAME",
                "TEAMNAME",
                "GROUPNAME",
                "COST_CATEGORY",
                "SUBCOMPONENT",
                "SOURCE",
                "AMOUNT",
                "FTE",
            ]
        )
    out = out.copy()
    for c in ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"]:
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].fillna("").astype(str).str.strip()
    if "COST_CATEGORY" in out.columns:
        out["COST_CATEGORY"] = out["COST_CATEGORY"].map(normalize_cost_category_value)
    out["YEAR"] = _to_int_series(out.get("YEAR"))
    out["PI"] = _to_int_series(out.get("PI"))
    if "AMOUNT" not in out.columns:
        out["AMOUNT"] = pd.NA
    if "FTE" not in out.columns:
        out["FTE"] = pd.NA
    out["AMOUNT"] = pd.to_numeric(out["AMOUNT"], errors="coerce").fillna(0.0).astype(float)
    out["FTE"] = pd.to_numeric(out["FTE"], errors="coerce")
    out["SCENARIO"] = spec.name
    out["ALLOCATION_DRIVER"] = ""
    out["SHARE"] = pd.Series([pd.NA] * len(out.index), dtype="Float64")
    out["CANONICAL_VERSION"] = CANONICAL_COSTS_VERSION
    out = _apply_wf_taxonomy_tags(out)
    out = _apply_sod_taxonomy_tags(out)

    if not include_program_overhead_effective:
        sub = out.get("SUBCOMPONENT", pd.Series(dtype=str)).fillna("").astype(str)
        cat = out.get("COST_CATEGORY", pd.Series(dtype=str)).fillna("").astype(str)
        is_program = sub.str.upper().str.contains("PROGRAM", na=False)
        is_work_force = cat.str.upper().eq("WORK_FORCE")
        out = out.loc[~(is_program & is_work_force)].copy()

    statuses = _mapping_status_filter(filters, scenario=spec.name)
    team_filters = [str(t).strip() for t in _as_list(f.get("team")) if str(t).strip()]
    group_filters = [str(g).strip() for g in _as_list(f.get("app_group")) if str(g).strip()]
    include_program_level = not team_filters and not group_filters

    years0 = [int(y) for y in _as_list(f.get("year")) if str(y).strip().isdigit()]
    if not years0 and out is not None and not out.empty:
        years0 = sorted(set(int(x) for x in pd.to_numeric(out.get("YEAR"), errors="coerce").dropna().astype(int).tolist()))
    years0 = sorted({int(y) for y in years0 if str(y).strip()})

    pi_filter = pi_requested
    pi_filter_set = {int(p) for p in pi_filter if 1 <= int(p) <= 4}

    # --- Baseline PI=0 expansion (annual -> quarterly) ---
    if spec.name == "Baseline" and pi_filter_set:
        # Expand annual baseline rows (PI==0) into quarterly PIs so PI filters don’t wipe baseline out.
        pi_int = pd.to_numeric(out.get("PI"), errors="coerce").fillna(0).astype(int)
        base0 = out.loc[pi_int.eq(0)].copy()
        if base0 is not None and not base0.empty:
            expanded: list[pd.DataFrame] = []
            for pi_num in sorted(pi_filter_set):
                tmp = base0.copy()
                tmp["PI"] = int(pi_num)
                # Split annual totals evenly across 4 PIs.
                tmp["AMOUNT"] = pd.to_numeric(tmp.get("AMOUNT"), errors="coerce").fillna(0.0) / 4.0
                tmp["FTE"] = pd.to_numeric(tmp.get("FTE"), errors="coerce").fillna(0.0) / 4.0
                expanded.append(tmp)
            if expanded:
                out_non0 = out.loc[~pi_int.eq(0)].copy()
                out = pd.concat([out_non0, *expanded], ignore_index=True, sort=False)
                out["PI"] = _to_int_series(out.get("PI"))

    # Apply PI filtering after Baseline expansion (and as a safety net for DB-mode calls).
    if pi_filter_set:
        out = out[out["PI"].isin(sorted(pi_filter_set))].copy()

    if spec.name == "Projected":
        # Centralized program overhead correction (Expected/Projected):
        # Some environments include a "Program" workforce row in `VW_TCO_WORKFORCE_SPLIT` that does not match
        # the stable program composition plan shown on Programs page. Replace those rows with:
        #   PROGRAMFTE (VW_PROGRAM_COMPOSITION_EFFECTIVE) × PROGRAM_XOM_RATE (VW_PROGRAM_RATE_EFFECTIVE, location-scoped).
        try:
            sub_u = out.get("SUBCOMPONENT", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
            cat_u = out.get("COST_CATEGORY", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
            is_overhead_view = cat_u.eq("WORK_FORCE") & sub_u.str.contains("PROGRAM", na=False)
            out = out.loc[~is_overhead_view].copy()

            if include_program_overhead_effective:
                years_ov = [int(y) for y in _as_list(f.get("year")) if str(y).strip().isdigit()]
                if not years_ov and out is not None and not out.empty:
                    years_ov = sorted(
                        {
                            int(x)
                            for x in pd.to_numeric(out.get("YEAR"), errors="coerce").dropna().astype(int).tolist()
                        }
                    )
                pi_ov = [int(p) for p in _as_list(f.get("pi")) if str(p).strip().isdigit()]
                if not pi_ov and out is not None and not out.empty:
                    pi_ov = sorted(
                        {
                            int(x)
                            for x in pd.to_numeric(out.get("PI"), errors="coerce").dropna().astype(int).tolist()
                        }
                    )
                programs_ov = [str(p).strip() for p in _as_list(f.get("program")) if str(p).strip()]
                loc_ov = str((filters or {}).get("location") or "GBC").strip()

                if years_ov:
                    ov_where: list[str] = ["1=1"]
                    ov_params: list[Any] = []
                    _add_in(ov_where, ov_params, "TRY_CONVERT(INT, c.YEAR)", [int(y) for y in years_ov])
                    _add_in(ov_where, ov_params, "TRY_CONVERT(INT, c.PI)", [int(p) for p in pi_ov])
                    _add_in(ov_where, ov_params, "p.PROGRAMNAME", programs_ov, upper=True)
                    ov_where_sql = " AND ".join(ov_where)

                    ov_sql = f"""
                      SELECT
                        TRY_CONVERT(INT, c.YEAR) AS YEAR,
                        TRY_CONVERT(INT, c.PI) AS PI,
                        p.PROGRAMNAME,
                        CAST('(Program overhead)' AS NVARCHAR(255)) AS TEAMNAME,
                        CAST('' AS NVARCHAR(255)) AS GROUPNAME,
                        CAST('WORK_FORCE' AS NVARCHAR(50)) AS COST_CATEGORY,
                        CAST('Program' AS NVARCHAR(255)) AS SUBCOMPONENT,
                        CAST('ADO' AS NVARCHAR(50)) AS SOURCE,
                        CAST(
                          ROUND(
                            COALESCE(TRY_CONVERT(FLOAT, c.PROGRAMFTE), 0.0) * COALESCE(TRY_CONVERT(FLOAT, pr.PROGRAM_XOM_RATE), 0.0),
                            2
                          ) AS DECIMAL(18,2)
                        ) AS AMOUNT,
                        CAST(COALESCE(TRY_CONVERT(FLOAT, c.PROGRAMFTE), 0.0) AS FLOAT) AS FTE
                      FROM {_fq('VW_PROGRAM_COMPOSITION_EFFECTIVE')} c
                      LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = c.PROGRAMID
                      LEFT JOIN {_fq('VW_PROGRAM_RATE_EFFECTIVE')} pr
                        ON pr.PROGRAMID = c.PROGRAMID
                       AND TRY_CONVERT(INT, pr.YEAR) = TRY_CONVERT(INT, c.YEAR)
                       AND TRY_CONVERT(INT, pr.PI) = TRY_CONVERT(INT, c.PI)
                       AND UPPER(LTRIM(RTRIM(pr.LOCATION))) = UPPER(LTRIM(RTRIM(%s)))
                      WHERE {ov_where_sql}
                        AND p.PROGRAMNAME IS NOT NULL
                    """
                    ov_df = fetch(ov_sql, [loc_ov] + ov_params)
                    if ov_df is not None and not ov_df.empty:
                        ov_out = ov_df.copy()
                        ov_out["YEAR"] = _to_int_series(ov_out.get("YEAR"))
                        ov_out["PI"] = _to_int_series(ov_out.get("PI"))
                        ov_out["PROGRAMNAME"] = ov_out.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                        ov_out["TEAMNAME"] = ov_out.get("TEAMNAME", "(Program overhead)").fillna("").astype(str).str.strip()
                        ov_out["GROUPNAME"] = ""
                        ov_out["COST_CATEGORY"] = "WORK_FORCE"
                        ov_out["SUBCOMPONENT"] = "Program"
                        # Canonical WF tags: ProgramFTE-derived overhead must be WF->Overhead.
                        ov_out["WF_LAYER2"] = "Overhead"
                        ov_out["WF_SUBTYPE"] = "Program Overhead"
                        ov_out["SOURCE"] = ov_out.get("SOURCE", "ADO").fillna("").astype(str).str.strip()
                        ov_out["AMOUNT"] = pd.to_numeric(ov_out.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
                        ov_out["FTE"] = pd.to_numeric(ov_out.get("FTE"), errors="coerce")
                        ov_out = ov_out[ov_out["AMOUNT"] != 0].copy()

                        out = (
                            ov_out
                            if out is None or out.empty
                            else pd.concat([out, ov_out], ignore_index=True, sort=False)
                        )
        except Exception:
            pass

        # Append program-level NWF expected (monthly Apptio → PI via iteration calendar).
        if include_program_level and years0:
            try:
                nwf_pi = get_program_nwf_expected_by_pi(db, years=years0, filters=filters)
            except Exception:
                nwf_pi = pd.DataFrame()
            if nwf_pi is not None and not nwf_pi.empty:
                nwf = nwf_pi.copy()
                nwf["YEAR"] = _to_int_series(nwf.get("YEAR"))
                nwf["PROGRAMNAME"] = nwf.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                nwf["PI_NAME"] = nwf.get("PI_NAME", "").fillna("").astype(str).str.strip()
                nwf["PI"] = _to_int_series(nwf["PI_NAME"].str.extract(r"(?i)\\bI\\s*([1-4])\\b", expand=False))
                nwf["PI"] = nwf["PI"].fillna(0)
                nwf["AMOUNT"] = pd.to_numeric(nwf.get("AMOUNT_PI"), errors="coerce").fillna(0.0).astype(float)
                nwf = nwf[nwf["YEAR"].notna() & (nwf["PROGRAMNAME"] != "")].copy()
                if pi_filter_set:
                    nwf = nwf[nwf["PI"].isin(list(pi_filter_set))].copy()
                if not nwf.empty:
                    nwf_lines = pd.DataFrame(
                        {
                            "YEAR": nwf["YEAR"],
                            "PI": nwf["PI"],
                            "PROGRAMNAME": nwf["PROGRAMNAME"],
                            "TEAMNAME": "",
                            "GROUPNAME": "(Program NWF)",
                            "COST_CATEGORY": "NON_WORK_FORCE",
                            "SUBCOMPONENT": "(Additional Costs - Program)",
                            "SOURCE": "APPTIO",
                            "AMOUNT": nwf["AMOUNT"],
                            "FTE": pd.NA,
                            "SCENARIO": "Projected",
                            "ALLOCATION_DRIVER": "",
                        }
                    )
                    nwf_lines["SHARE"] = pd.Series([pd.NA] * len(nwf_lines.index), dtype="Float64")
                    out = nwf_lines if out is None or out.empty else pd.concat([out, nwf_lines], ignore_index=True, sort=False)

        group = out.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        status = pd.Series("UNMAPPED_APP_GROUP", index=out.index, dtype="object")
        status.loc[group.ne("")] = "MAPPED"
        status.loc[group.eq("(Unallocated NWF)")] = "UNALLOCATED"
        status.loc[group.eq("(Program NWF)")] = "PROGRAM_LEVEL"
        out["MAPPING_STATUS"] = status
        if statuses:
            out = out[out["MAPPING_STATUS"].isin(statuses)].copy()
        if (filters or {}).get("include_unmapped") is True or (statuses and "UNMAPPED_APP_GROUP" in statuses):
            out.loc[out["MAPPING_STATUS"] == "UNMAPPED_APP_GROUP", "GROUPNAME"] = "(Needs mapping)"

    elif spec.name == "Baseline":
        group = out.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        status = pd.Series("BASELINE_NOT_APP_ATTRIBUTED", index=out.index, dtype="object")
        status.loc[group.ne("")] = "MAPPED"
        out["MAPPING_STATUS"] = status
        if statuses:
            out = out[out["MAPPING_STATUS"].isin(statuses)].copy()

    else:
        # Actuals (program-level only): monthly Apptio NWF → PI via ADO iteration calendar.
        out = out.iloc[0:0].copy()
        out["MAPPING_STATUS"] = "PROGRAM_LEVEL"
        if include_program_level and include_native_actuals and years0:
            try:
                nwf_pi = get_program_nwf_actuals_by_pi_breakdown(db, years=years0, filters=filters)
            except Exception:
                nwf_pi = pd.DataFrame()
            breakdown_available = (
                nwf_pi is not None
                and not nwf_pi.empty
                and "COST_TYPE" in nwf_pi.columns
                and "SUBTYPE" in nwf_pi.columns
            )
            if not breakdown_available:
                # Backward-compatible fallback (legacy APPTIO_ACTUALS totals only).
                try:
                    nwf_pi = get_program_nwf_actuals_by_pi(db, years=years0, filters=filters)
                except Exception:
                    nwf_pi = pd.DataFrame()
            if nwf_pi is not None and not nwf_pi.empty:
                nwf = nwf_pi.copy()
                nwf["YEAR"] = _to_int_series(nwf.get("YEAR"))
                nwf["PROGRAMNAME"] = nwf.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                nwf["PI_NAME"] = nwf.get("PI_NAME", "").fillna("").astype(str).str.strip()
                nwf["PI"] = _to_int_series(nwf["PI_NAME"].str.extract(r"(?i)\\bI\\s*([1-4])\\b", expand=False))
                nwf["AMOUNT"] = pd.to_numeric(nwf.get("AMOUNT_PI"), errors="coerce").fillna(0.0).astype(float)
                nwf = nwf[nwf["YEAR"].notna() & nwf["PI"].notna() & (nwf["PROGRAMNAME"] != "")].copy()
                if pi_filter_set:
                    nwf = nwf[nwf["PI"].isin(list(pi_filter_set))].copy()
                if not nwf.empty:
                    if breakdown_available:
                        nwf["COST_TYPE"] = nwf.get("COST_TYPE", "").fillna("").astype(str).str.strip()
                        nwf.loc[nwf["COST_TYPE"].eq(""), "COST_TYPE"] = "NWF"
                        nwf["SUBTYPE"] = nwf.get("SUBTYPE", "").fillna("").astype(str).str.strip()
                        if "EFFECTIVE_NWF_TYPE" in nwf.columns:
                            nwf["EFFECTIVE_NWF_TYPE"] = nwf.get("EFFECTIVE_NWF_TYPE", "").fillna("").astype(str).str.strip()

                        apptio_map_status = nwf.get("MAPPING_STATUS", pd.Series([""] * len(nwf.index), index=nwf.index))
                        apptio_map_status = apptio_map_status.fillna("").astype(str).str.strip()
                        nwf["_STATUS_RANK"] = apptio_map_status.map(
                            {"MAPPED_PRODUCT": 4, "MAPPED_LEDGER": 3, "INVALID_TARGET": 2, "DEFAULT_OTHER": 1}
                        ).fillna(0).astype(int)

                        if "EFFECTIVE_NWF_TYPE" in nwf.columns:
                            grouped = (
                                nwf.groupby(["YEAR", "PI", "PROGRAMNAME", "EFFECTIVE_NWF_TYPE"], dropna=False)
                                .agg({"AMOUNT": "sum", "_STATUS_RANK": "max"})
                                .reset_index()
                            )
                            grouped["SUBCOMPONENT"] = grouped.get("EFFECTIVE_NWF_TYPE", "").map(normalize_nwf_subcomponent)
                        else:
                            grouped = (
                                nwf.groupby(["YEAR", "PI", "PROGRAMNAME", "COST_TYPE", "SUBTYPE"], dropna=False)
                                .agg({"AMOUNT": "sum", "_STATUS_RANK": "max"})
                                .reset_index()
                            )

                            def _subcomponent_from_pair(r: pd.Series) -> str:
                                ct = str(r.get("COST_TYPE") or "").strip()
                                stp = str(r.get("SUBTYPE") or "").strip()
                                if ct == "Cloud" and stp in {"AWS", "Azure"}:
                                    raw = f"Cloud {stp}".strip()
                                elif ct in {"Invoices", "Contractor CS", "MSP", "Travel", "Infra"} and stp == "":
                                    raw = ct
                                elif ct == "NWF" and stp == "Other":
                                    raw = "NWF Other"
                                else:
                                    raw = f"{ct} {stp}".strip()
                                return normalize_nwf_subcomponent(raw)

                            grouped["SUBCOMPONENT"] = grouped.apply(_subcomponent_from_pair, axis=1)
                        grouped["ALLOCATION_DRIVER"] = grouped["_STATUS_RANK"].map(
                            {4: "MAPPED_PRODUCT", 3: "MAPPED_LEDGER", 2: "INVALID_TARGET", 1: "DEFAULT_OTHER"}
                        ).fillna("")
                        grouped = grouped.drop(columns=["_STATUS_RANK"])

                        out = pd.DataFrame(
                            {
                                "YEAR": grouped["YEAR"],
                                "PI": grouped["PI"],
                                "PROGRAMNAME": grouped["PROGRAMNAME"],
                                "TEAMNAME": "",
                                "GROUPNAME": "(Program-level)",
                                "COST_CATEGORY": "NON_WORK_FORCE",
                                "SUBCOMPONENT": grouped["SUBCOMPONENT"],
                                "SOURCE": "APPTIO",
                                "AMOUNT": grouped["AMOUNT"],
                                "FTE": pd.NA,
                                "MAPPING_STATUS": "PROGRAM_LEVEL",
                                "SCENARIO": "Actual",
                                "ALLOCATION_DRIVER": grouped["ALLOCATION_DRIVER"],
                            }
                        )
                        out["SHARE"] = pd.Series([pd.NA] * len(out.index), dtype="Float64")
                    else:
                        grouped = (
                            nwf.groupby(["YEAR", "PI", "PROGRAMNAME"], dropna=False)["AMOUNT"]
                            .sum()
                            .reset_index()
                        )
                        out = pd.DataFrame(
                            {
                                "YEAR": grouped["YEAR"],
                                "PI": grouped["PI"],
                                "PROGRAMNAME": grouped["PROGRAMNAME"],
                                "TEAMNAME": "",
                                "GROUPNAME": "(Program-level)",
                                "COST_CATEGORY": "NON_WORK_FORCE",
                                "SUBCOMPONENT": "NWF Other",
                                "SOURCE": "APPTIO",
                                "AMOUNT": grouped["AMOUNT"],
                                "FTE": pd.NA,
                                "MAPPING_STATUS": "PROGRAM_LEVEL",
                                "SCENARIO": "Actual",
                                "ALLOCATION_DRIVER": "",
                            }
                        )
                        out["SHARE"] = pd.Series([pd.NA] * len(out.index), dtype="Float64")
        if statuses and "MAPPING_STATUS" in out.columns:
            out = out[out["MAPPING_STATUS"].isin(statuses)].copy()

    # Ensure demand driver field + version are always present (helps downstream diagnostics and busts Streamlit caches safely).
    if "FTE" not in out.columns:
        out["FTE"] = pd.NA
    out["FTE"] = pd.to_numeric(out["FTE"], errors="coerce")
    if "CANONICAL_VERSION" not in out.columns:
        out["CANONICAL_VERSION"] = CANONICAL_COSTS_VERSION
    else:
        out["CANONICAL_VERSION"] = CANONICAL_COSTS_VERSION

    # Rollup label used only for NWF comparisons (keep original SUBCOMPONENT intact).
    out["SUBCOMPONENT_ROLLUP"] = out.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
    cat = out.get("COST_CATEGORY", pd.Series(dtype=str)).fillna("").astype(str).str.upper().str.strip()
    sub = out.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
    src = out.get("SOURCE", "").fillna("").astype(str).str.upper().str.strip()
    nwf_mask = cat.eq("NON_WORK_FORCE")
    if nwf_mask.any():
        out.loc[nwf_mask & sub.astype(str).str.upper().str.startswith("MSP "), "SUBCOMPONENT_ROLLUP"] = "MSP"
        invoice_like = (
            src.eq("INVOICES")
            | sub.astype(str).str.contains("invoice", case=False, na=False)
            | sub.astype(str).str.upper().isin(["RECURRING", "AD HOC", "ADHOC"])
        )
        out.loc[nwf_mask & invoice_like, "SUBCOMPONENT_ROLLUP"] = "Invoices"

    # Ensure WF taxonomy tags always exist even when the slice has no WF rows
    # (e.g., Actual scenario with NWF-only rows, or NWF-only scoped filters).
    if "WF_LAYER2" not in out.columns:
        out["WF_LAYER2"] = ""
    if "WF_SUBTYPE" not in out.columns:
        out["WF_SUBTYPE"] = ""
    if "SOD_LAYER2" not in out.columns:
        out["SOD_LAYER2"] = ""
    if "SOD_SUBTYPE" not in out.columns:
        out["SOD_SUBTYPE"] = ""
    out = _apply_sod_taxonomy_tags(out)

    return out[
        [
            "YEAR",
            "PI",
            "PROGRAMNAME",
            "TEAMNAME",
            "GROUPNAME",
            "COST_CATEGORY",
            "SUBCOMPONENT",
            "SUBCOMPONENT_ROLLUP",
            "WF_LAYER2",
            "WF_SUBTYPE",
            "SOD_LAYER2",
            "SOD_SUBTYPE",
            "SOURCE",
            "AMOUNT",
            "MAPPING_STATUS",
            "SCENARIO",
            "ALLOCATION_DRIVER",
            "SHARE",
            "FTE",
            "CANONICAL_VERSION",
        ]
    ]


def _scenario_label_to_name(val: Any) -> Optional[str]:
    if val is None:
        return None
    raw = str(val).strip().upper().replace(" ", "_")
    if raw in {"PROJECTED", "EXPECTED"}:
        return "Projected"
    if raw in {"BASELINE", "BUDGET"}:
        return "Baseline"
    if raw in {"ACTUAL", "ACTUALS"}:
        return "Actual"
    return None


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [v for v in value if v is not None and str(v).strip() != ""]
    return [value]


def _clean_scope_text(value: Any) -> str:
    s = html.unescape(str(value or "")).replace("\u00a0", " ").strip()
    return " ".join(s.split())


def _normalize_scope_key(value: Any) -> str:
    return _clean_scope_text(value).upper()


def _normalize_scope_key_variants(value: Any) -> set[str]:
    base = _normalize_scope_key(value)
    if not base:
        return set()
    variants = {base}
    amp_to_and = " ".join(base.replace("&", " AND ").split())
    and_to_amp = " ".join(base.replace(" AND ", " & ").split())
    variants.add(amp_to_and)
    variants.add(and_to_amp)
    return {v for v in variants if v}


def _dedupe_str_list(values: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        v = _clean_scope_text(raw)
        if not v:
            continue
        k = v.upper()
        if k in seen:
            continue
        seen.add(k)
        out.append(v)
    return out


def _normalize_scope_filters(filters: Optional[Filters]) -> dict[str, Any]:
    f = dict(filters or {})
    for key in ("program", "team", "app_group", "program_id", "team_id"):
        vals: list[str] = []
        for v in _as_list(f.get(key)):
            clean = _clean_scope_text(v)
            if clean:
                vals.append(clean)
        if vals or key in f:
            f[key] = _dedupe_str_list(vals)
    return f


def _expand_named_scope_values(
    fetch: FetchFn,
    *,
    key: str,
    table: str,
    raw_col: str,
    display_col: str,
    filters: dict[str, Any],
) -> None:
    selected = _dedupe_str_list([str(v) for v in _as_list(filters.get(key)) if str(v).strip()])
    if not selected:
        return
    try:
        df = fetch(
            f"""
            SELECT
              LTRIM(RTRIM({raw_col})) AS RAW_NAME,
              LTRIM(RTRIM({display_col})) AS DISPLAY_NAME
            FROM {_fq(table)}
            """,
            None,
        )
    except Exception:
        # Backward compatibility: some schemas do not expose *_DISPLAY_NAME yet.
        try:
            df = fetch(
                f"""
                SELECT
                  LTRIM(RTRIM({raw_col})) AS RAW_NAME,
                  CAST(NULL AS NVARCHAR(512)) AS DISPLAY_NAME
                FROM {_fq(table)}
                """,
                None,
            )
        except Exception:
            return
    if df is None or df.empty:
        return

    rows: list[tuple[str, str, set[str]]] = []
    for _, r in df.iterrows():
        raw_name = _clean_scope_text(r.get("RAW_NAME"))
        display_name = _clean_scope_text(r.get("DISPLAY_NAME"))
        keys = set()
        keys.update(_normalize_scope_key_variants(raw_name))
        keys.update(_normalize_scope_key_variants(display_name))
        if keys:
            rows.append((raw_name, display_name, keys))

    selected_keys: set[str] = set()
    for s in selected:
        selected_keys.update(_normalize_scope_key_variants(s))

    expanded = list(selected)
    for raw_name, display_name, keys in rows:
        if not (keys & selected_keys):
            continue
        if raw_name:
            expanded.append(raw_name)
        if display_name:
            expanded.append(display_name)

    filters[key] = _dedupe_str_list(expanded)


def _resolve_scope_filters_with_aliases(db: Any, filters: Optional[Filters]) -> dict[str, Any]:
    f = _normalize_scope_filters(filters)
    if isinstance(db, pd.DataFrame):
        return f
    try:
        fetch = _resolve_fetch(db)
    except Exception:
        return f
    _expand_named_scope_values(
        fetch,
        key="program",
        table="PROGRAMS",
        raw_col="PROGRAMNAME",
        display_col="PROGRAM_DISPLAY_NAME",
        filters=f,
    )
    _expand_named_scope_values(
        fetch,
        key="team",
        table="TEAMS",
        raw_col="TEAMNAME",
        display_col="TEAM_DISPLAY_NAME",
        filters=f,
    )
    # Resolve stable IDs for projected-demand filtering. ID predicates are cheaper
    # than case-insensitive name predicates on large demand views.
    def _resolve_ids(
        *,
        table: str,
        id_col: str,
        raw_col: str,
        display_col: str,
        selected_names: list[str],
    ) -> list[str]:
        if not selected_names:
            return []
        try:
            df = fetch(
                f"""
                SELECT
                  LTRIM(RTRIM({id_col})) AS ENTITY_ID,
                  LTRIM(RTRIM({raw_col})) AS RAW_NAME,
                  LTRIM(RTRIM({display_col})) AS DISPLAY_NAME
                FROM {_fq(table)}
                """,
                None,
            )
        except Exception:
            # Backward compatibility: resolve IDs using raw names when display column is unavailable.
            try:
                df = fetch(
                    f"""
                    SELECT
                      LTRIM(RTRIM({id_col})) AS ENTITY_ID,
                      LTRIM(RTRIM({raw_col})) AS RAW_NAME,
                      CAST(NULL AS NVARCHAR(512)) AS DISPLAY_NAME
                    FROM {_fq(table)}
                    """,
                    None,
                )
            except Exception:
                return []
        if df is None or df.empty:
            return []
        key_to_ids: dict[str, list[str]] = {}
        for _, r in df.iterrows():
            entity_id = _clean_scope_text(r.get("ENTITY_ID"))
            if not entity_id:
                continue
            keys = set()
            keys.update(_normalize_scope_key_variants(r.get("RAW_NAME")))
            keys.update(_normalize_scope_key_variants(r.get("DISPLAY_NAME")))
            for key in keys:
                bucket = key_to_ids.setdefault(key, [])
                bucket.append(entity_id)
        out: list[str] = []
        seen: set[str] = set()
        for name in selected_names:
            for key in _normalize_scope_key_variants(name):
                for entity_id in key_to_ids.get(key, []):
                    if entity_id in seen:
                        continue
                    seen.add(entity_id)
                    out.append(entity_id)
        return out

    program_ids = _resolve_ids(
        table="PROGRAMS",
        id_col="PROGRAMID",
        raw_col="PROGRAMNAME",
        display_col="PROGRAM_DISPLAY_NAME",
        selected_names=[str(v) for v in _as_list(f.get("program")) if str(v).strip()],
    )
    if program_ids:
        f["program_id"] = _dedupe_str_list(program_ids)

    team_ids = _resolve_ids(
        table="TEAMS",
        id_col="TEAMID",
        raw_col="TEAMNAME",
        display_col="TEAM_DISPLAY_NAME",
        selected_names=[str(v) for v in _as_list(f.get("team")) if str(v).strip()],
    )
    if team_ids:
        f["team_id"] = _dedupe_str_list(team_ids)
    return f


def _add_in(where: list[str], params: list[Any], col_expr: str, values: Sequence[Any], *, upper: bool = False) -> None:
    vals = [v for v in values if v is not None and str(v).strip() != ""]
    if not vals:
        return
    if upper:
        vals = [str(v).strip().upper() for v in vals]
        col_expr = f"UPPER({col_expr})"
    placeholders = ", ".join(["%s"] * len(vals))
    where.append(f"{col_expr} IN ({placeholders})")
    params.extend(vals)


def _build_where(
    filters: Optional[Filters],
    *,
    year_col: str = "YEAR",
    pi_col: str = "PI",
    allow_group_filter: bool = True,
) -> tuple[str, list[Any]]:
    """Build a safe WHERE clause for the common filter keys."""
    where: list[str] = ["1=1"]
    params: list[Any] = []
    f = _normalize_scope_filters(filters)

    years = [int(y) for y in _as_list(f.get("year")) if str(y).strip().isdigit()]
    pis = [int(p) for p in _as_list(f.get("pi")) if str(p).strip().isdigit()]
    programs = [str(p).strip() for p in _as_list(f.get("program")) if str(p).strip()]
    teams = [str(t).strip() for t in _as_list(f.get("team")) if str(t).strip()]
    groups = [str(g).strip() for g in _as_list(f.get("app_group")) if str(g).strip()]

    _add_in(where, params, year_col, years)
    _add_in(where, params, pi_col, pis)
    _add_in(where, params, "PROGRAMNAME", programs, upper=True)
    _add_in(where, params, "TEAMNAME", teams, upper=True)
    if allow_group_filter:
        _add_in(where, params, "GROUPNAME", groups, upper=True)

    return " AND ".join(where), params


def _labor_bucket_sql(source_col: str = "SOURCE", sub_col: str = "SUBCOMPONENT") -> str:
    """SQL CASE expression to map raw rows to the requested labor buckets."""
    return f"""
    CASE
      WHEN UPPER(COALESCE({source_col}, '')) = 'MSP' OR UPPER(COALESCE({sub_col}, '')) LIKE 'MSP%'
        THEN 'MSP'
      WHEN UPPER(COALESCE({sub_col}, '')) IN ('TEAM', 'DELIVERY TEAM', 'DELIVERY')
        THEN 'Team/Delivery'
      WHEN UPPER(COALESCE({sub_col}, '')) = 'CONTRACTOR C'
        THEN 'Contractor C'
      WHEN UPPER(COALESCE({sub_col}, '')) = 'CONTRACTOR CS'
        THEN 'Contractor CS'
      ELSE 'Other'
    END
    """.strip()


def _labor_bucket_py(source: Any, subcomponent: Any) -> str:
    s = str(source or "").strip().upper()
    sub = str(subcomponent or "").strip().upper()
    if s == "MSP" or sub.startswith("MSP"):
        return "MSP"
    if sub in {"TEAM", "DELIVERY TEAM", "DELIVERY"}:
        return "Team/Delivery"
    if sub == "CONTRACTOR C":
        return "Contractor C"
    if sub == "CONTRACTOR CS":
        return "Contractor CS"
    return "Other"


def _labor_bucket_series(df: pd.DataFrame, source_col: str = "SOURCE", sub_col: str = "SUBCOMPONENT") -> pd.Series:
    """Vectorized labor bucket mapping for DataFrame aggregation paths."""
    if df is None or df.empty:
        return pd.Series(dtype=object)
    src_raw = df.get(source_col)
    sub_raw = df.get(sub_col)
    if not isinstance(src_raw, pd.Series):
        src_raw = pd.Series([""] * len(df.index), index=df.index, dtype=object)
    if not isinstance(sub_raw, pd.Series):
        sub_raw = pd.Series([""] * len(df.index), index=df.index, dtype=object)

    src = src_raw.fillna("").astype(str).str.strip().str.upper()
    sub = sub_raw.fillna("").astype(str).str.strip().str.upper()

    out = pd.Series(["Other"] * len(df.index), index=df.index, dtype=object)
    is_msp = src.eq("MSP") | sub.str.startswith("MSP")
    out.loc[is_msp] = "MSP"

    team_mask = (~is_msp) & sub.isin(["TEAM", "DELIVERY TEAM", "DELIVERY"])
    out.loc[team_mask] = "Team/Delivery"
    out.loc[(~is_msp) & sub.eq("CONTRACTOR C")] = "Contractor C"
    out.loc[(~is_msp) & sub.eq("CONTRACTOR CS")] = "Contractor CS"
    return out


def _projected_cost_lines_sql(filters: Optional[Filters], *, db: Any = None) -> tuple[str, list[Any]]:
    # Keep app-group filtering after base-group split. If we filter GROUPNAME before split,
    # rows from base groups can be incorrectly dropped before they are redistributed.
    f = filters or {}
    years = [int(y) for y in _as_list(f.get("year")) if str(y).strip().isdigit()]
    pis = [int(p) for p in _as_list(f.get("pi")) if str(p).strip().isdigit()]
    programs = [str(p).strip() for p in _as_list(f.get("program")) if str(p).strip()]
    teams = [str(t).strip() for t in _as_list(f.get("team")) if str(t).strip()]
    program_ids = [str(p).strip() for p in _as_list(f.get("program_id")) if str(p).strip()]
    team_ids = [str(t).strip() for t in _as_list(f.get("team_id")) if str(t).strip()]
    groups = [str(g).strip() for g in _as_list(f.get("app_group")) if str(g).strip()]
    snapshot_enabled = _projected_demand_snapshot_enabled()
    strict_scope_snapshot = (
        snapshot_enabled
        and _projected_demand_snapshot_strict_scope_enabled()
        and bool(programs or teams or program_ids or team_ids)
    )
    snapshot_available = False
    if snapshot_enabled and not strict_scope_snapshot:
        snapshot_available = _projected_demand_snapshot_available(
            db,
            years=years,
            pis=pis,
            program_ids=program_ids,
            team_ids=team_ids,
        )
    use_demand_snapshot = snapshot_enabled and (strict_scope_snapshot or snapshot_available)

    d_where_parts: list[str] = ["COALESCE(d.IN_SCOPE_FOR_ROADMAP, 0) = 1"]
    d_params: list[Any] = []
    _add_in(d_where_parts, d_params, "d.YEAR", years)
    _add_in(d_where_parts, d_params, "d.PI", pis)
    d_where_sql = " AND ".join(d_where_parts)
    # Push program/team filters into ADO demand extraction to avoid scanning full-year
    # feature volume when the user is already in a narrower scope.
    # Keep app-group filter out of this stage because base-group split happens later.
    d_scope_where_parts: list[str] = []
    d_scope_params: list[Any] = []
    if program_ids:
        _add_in(d_scope_where_parts, d_scope_params, "d.PROGRAMID", program_ids)
    elif programs:
        vals = [str(v).strip().upper() for v in programs if str(v).strip()]
        if vals:
            placeholders = ", ".join(["%s"] * len(vals))
            d_scope_where_parts.append(
                f"d.PROGRAMID IN (SELECT p.PROGRAMID FROM {_fq('PROGRAMS')} p WHERE UPPER(p.PROGRAMNAME) IN ({placeholders}))"
            )
            d_scope_params.extend(vals)
    if team_ids:
        _add_in(d_scope_where_parts, d_scope_params, "d.TEAMID", team_ids)
    elif teams:
        vals = [str(v).strip().upper() for v in teams if str(v).strip()]
        if vals:
            placeholders = ", ".join(["%s"] * len(vals))
            d_scope_where_parts.append(
                f"d.TEAMID IN (SELECT t.TEAMID FROM {_fq('TEAMS')} t WHERE UPPER(t.TEAMNAME) IN ({placeholders}))"
            )
            d_scope_params.extend(vals)
    d_scope_sql = (" AND " + " AND ".join(d_scope_where_parts)) if d_scope_where_parts else ""

    ds_where_parts: list[str] = ["1=1"]
    ds_params: list[Any] = []
    _add_in(ds_where_parts, ds_params, "ds.YEAR", years)
    _add_in(ds_where_parts, ds_params, "ds.PI", pis)
    if program_ids:
        _add_in(ds_where_parts, ds_params, "ds.PROGRAMID", program_ids)
    elif programs:
        _add_in(ds_where_parts, ds_params, "ds.PROGRAMNAME", programs, upper=True)
    if team_ids:
        _add_in(ds_where_parts, ds_params, "ds.TEAMID", team_ids)
    elif teams:
        _add_in(ds_where_parts, ds_params, "ds.TEAMNAME", teams, upper=True)
    ds_where_sql = " AND ".join(ds_where_parts)

    c_where_parts: list[str] = ["1=1"]
    c_params: list[Any] = []
    _add_in(c_where_parts, c_params, "c.YEAR", years)
    _add_in(c_where_parts, c_params, "c.PI", pis)
    c_where_sql = " AND ".join(c_where_parts)

    r_where_parts: list[str] = ["1=1"]
    r_params: list[Any] = []
    _add_in(r_where_parts, r_params, "r.YEAR", years)
    _add_in(r_where_parts, r_params, "r.PI", pis)
    r_where_sql = " AND ".join(r_where_parts)

    base_where_parts: list[str] = ["1=1"]
    base_where_params: list[Any] = []
    if program_ids:
        _add_in(base_where_parts, base_where_params, "PROGRAMID", program_ids)
    elif programs:
        vals = [str(v).strip().upper() for v in programs if str(v).strip()]
        if vals:
            placeholders = ", ".join(["%s"] * len(vals))
            base_where_parts.append(
                f"PROGRAMID IN (SELECT p.PROGRAMID FROM {_fq('PROGRAMS')} p WHERE UPPER(p.PROGRAMNAME) IN ({placeholders}))"
            )
            base_where_params.extend(vals)
    if team_ids:
        _add_in(base_where_parts, base_where_params, "TEAMID", team_ids)
    elif teams:
        vals = [str(v).strip().upper() for v in teams if str(v).strip()]
        if vals:
            placeholders = ", ".join(["%s"] * len(vals))
            base_where_parts.append(
                f"TEAMID IN (SELECT t.TEAMID FROM {_fq('TEAMS')} t WHERE UPPER(t.TEAMNAME) IN ({placeholders}))"
            )
            base_where_params.extend(vals)
    _add_in(base_where_parts, base_where_params, "GROUPNAME", groups, upper=True)
    base_where_sql = " AND ".join(base_where_parts)

    non_ado_where_parts: list[str] = ["1=1"]
    non_ado_params: list[Any] = []
    _add_in(non_ado_where_parts, non_ado_params, "YEAR", years)
    _add_in(non_ado_where_parts, non_ado_params, "PI", pis)
    if program_ids:
        _add_in(non_ado_where_parts, non_ado_params, "PROGRAMID", program_ids)
    elif programs:
        vals = [str(v).strip().upper() for v in programs if str(v).strip()]
        if vals:
            placeholders = ", ".join(["%s"] * len(vals))
            non_ado_where_parts.append(
                f"PROGRAMID IN (SELECT p.PROGRAMID FROM {_fq('PROGRAMS')} p WHERE UPPER(p.PROGRAMNAME) IN ({placeholders}))"
            )
            non_ado_params.extend(vals)
    if team_ids:
        _add_in(non_ado_where_parts, non_ado_params, "TEAMID", team_ids)
    elif teams:
        vals = [str(v).strip().upper() for v in teams if str(v).strip()]
        if vals:
            placeholders = ", ".join(["%s"] * len(vals))
            non_ado_where_parts.append(
                f"TEAMID IN (SELECT t.TEAMID FROM {_fq('TEAMS')} t WHERE UPPER(t.TEAMNAME) IN ({placeholders}))"
            )
            non_ado_params.extend(vals)
    _add_in(non_ado_where_parts, non_ado_params, "GROUPNAME", groups, upper=True)
    non_ado_where_sql = " AND ".join(non_ado_where_parts)

    fte_driver_mode = _projected_fte_driver_mode(db)
    requested_snapshot_velocity = fte_driver_mode == "SNAPSHOT_VELOCITY"
    velocity_snapshot_available = True
    if requested_snapshot_velocity and _projected_velocity_failopen_enabled():
        velocity_snapshot_available = _projected_velocity_snapshot_available(
            db,
            years=years,
            pis=pis,
            team_ids=team_ids,
        )
    use_snapshot_velocity = requested_snapshot_velocity and velocity_snapshot_available
    row_fallback_enabled = _projected_velocity_row_fallback_enabled(db)
    baseline_points_expr = (
        "COALESCE(sv.EFFECTIVE_BASELINE_POINTS, gb.GLOBAL_BASELINE_POINTS)"
        if row_fallback_enabled
        else "sv.EFFECTIVE_BASELINE_POINTS"
    )
    snapshot_row_guard = (
        ""
        if row_fallback_enabled
        else "AND COALESCE(TRY_CONVERT(FLOAT, sv.EFFECTIVE_BASELINE_POINTS), 0.0) > 0"
    )
    if use_snapshot_velocity:
        snapshot_where_parts: list[str] = ["1=1"]
        snapshot_params: list[Any] = []
        _add_in(snapshot_where_parts, snapshot_params, "TRY_CONVERT(INT, v.YEAR)", years)
        _add_in(snapshot_where_parts, snapshot_params, "TRY_CONVERT(INT, v.PI)", pis)
        snapshot_where_sql = " AND ".join(snapshot_where_parts)
        if use_demand_snapshot:
            src_sql = f"""
      src AS (
        -- Snapshot velocity mode with persisted demand source.
        SELECT
          ds.PROGRAMID,
          ds.PROGRAMNAME,
          ds.TEAMID,
          ds.TEAMNAME,
          ds.GROUPID,
          ds.GROUPNAME,
          ds.YEAR,
          ds.PI,
          CAST(COALESCE(TRY_CONVERT(FLOAT, ds.SWAG_POINTS_SUM), 0.0) AS FLOAT) AS SWAG_POINTS_SUM
        FROM {_fq('TCO_PROJECTED_DEMAND_SNAPSHOT')} ds
        WHERE {ds_where_sql}
          AND COALESCE(TRY_CONVERT(FLOAT, ds.SWAG_POINTS_SUM), 0.0) > 0
      ),
            """.rstrip()
            src_params = ds_params
        else:
            src_sql = f"""
      src AS (
        -- Snapshot velocity mode:
        -- Aggregate SWAG demand first (program/team/group/year/pi), then apply the
        -- velocity baseline once per aggregate row.
        SELECT
          d.PROGRAMID,
          d.PROGRAMNAME,
          d.TEAMID,
          d.TEAMNAME,
          d.GROUPID,
          d.GROUPNAME,
          TRY_CONVERT(INT, d.YEAR) AS YEAR,
          TRY_CONVERT(INT, d.PI) AS PI,
          CAST(SUM(COALESCE(TRY_CONVERT(FLOAT, d.SWAG_POINTS), 0.0)) AS FLOAT) AS SWAG_POINTS_SUM
        FROM {_fq('VW_TCO_FEATURE_DEMAND')} d
        WHERE {d_where_sql}
          AND COALESCE(d.IS_MSP_FEATURE, 0) = 0
          AND COALESCE(TRY_CONVERT(FLOAT, d.SWAG_POINTS), 0.0) > 0
          {d_scope_sql}
        GROUP BY
          d.PROGRAMID,
          d.PROGRAMNAME,
          d.TEAMID,
          d.TEAMNAME,
          d.GROUPID,
          d.GROUPNAME,
          TRY_CONVERT(INT, d.YEAR),
          TRY_CONVERT(INT, d.PI)
      ),
            """.rstrip()
            src_params = d_params + d_scope_params
        demand_with_prefix = f"""
      ;WITH global_baseline AS (
        SELECT
          CAST(
            COALESCE(
              (
                SELECT TOP 1 TRY_CONVERT(FLOAT, JSON_VALUE(ap.CONFIG_JSON, '$.swag.points_per_fte'))
                FROM {_fq('ADO_PROFILES')} ap
                WHERE COALESCE(ap.IS_ACTIVE, 0) = 1
                ORDER BY ap.UPDATED_AT DESC
              ),
              (
                SELECT TOP 1 TRY_CONVERT(FLOAT, JSON_VALUE(ap.CONFIG_JSON, '$.swag.points_per_fte'))
                FROM {_fq('ADO_PROFILES')} ap
                ORDER BY ap.UPDATED_AT DESC
              ),
              65.0
            ) AS FLOAT
          ) AS GLOBAL_BASELINE_POINTS
      ),
      snapshot AS (
        SELECT
          COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMID)), ''), 'UNKNOWN') AS TEAMID,
          TRY_CONVERT(INT, v.YEAR) AS YEAR,
          TRY_CONVERT(INT, v.PI) AS PI,
          TRY_CONVERT(FLOAT, v.EFFECTIVE_BASELINE_POINTS) AS EFFECTIVE_BASELINE_POINTS
        FROM {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')} v
        WHERE {snapshot_where_sql}
      ),
      {src_sql}
      d AS (
        -- Use team-level baseline points from velocity snapshot and compute demand from SWAG points.
        -- Fallback to global baseline only when snapshot rows are unavailable.
        SELECT
          src.PROGRAMID,
          src.PROGRAMNAME,
          src.TEAMID,
          src.TEAMNAME,
          src.GROUPID,
          src.GROUPNAME,
          src.YEAR,
          src.PI,
          CAST(
            COALESCE(
              src.SWAG_POINTS_SUM / NULLIF({baseline_points_expr}, 0.0),
              0.0
            ) AS FLOAT
          ) AS DEMAND_DERIVED_FTE
        FROM src
        CROSS JOIN global_baseline gb
        LEFT JOIN snapshot sv
          ON sv.TEAMID = COALESCE(NULLIF(LTRIM(RTRIM(src.TEAMID)), ''), 'UNKNOWN')
         AND sv.YEAR = src.YEAR
         AND sv.PI = src.PI
        WHERE 1=1
          {snapshot_row_guard}
      ),
        """.rstrip()
        demand_params: list[Any] = snapshot_params + src_params
    else:
        if use_demand_snapshot:
            demand_with_prefix = f"""
      ;WITH d AS (
        -- Legacy SWAG mode with persisted demand source.
        SELECT
          ds.PROGRAMID,
          ds.PROGRAMNAME,
          ds.TEAMID,
          ds.TEAMNAME,
          ds.GROUPID,
          ds.GROUPNAME,
          ds.YEAR,
          ds.PI,
          CAST(COALESCE(TRY_CONVERT(FLOAT, ds.DERIVED_FTE_SUM), 0.0) AS FLOAT) AS DEMAND_DERIVED_FTE
        FROM {_fq('TCO_PROJECTED_DEMAND_SNAPSHOT')} ds
        WHERE {ds_where_sql}
          AND COALESCE(TRY_CONVERT(FLOAT, ds.DERIVED_FTE_SUM), 0.0) > 0
      ),
        """.rstrip()
            demand_params = ds_params
        else:
            demand_with_prefix = f"""
      ;WITH d AS (
        -- Legacy SWAG mode:
        -- Keep projected path available by using stable SWAG-derived feature demand.
        SELECT
          d.PROGRAMID,
          d.PROGRAMNAME,
          d.TEAMID,
          d.TEAMNAME,
          d.GROUPID,
          d.GROUPNAME,
          TRY_CONVERT(INT, d.YEAR) AS YEAR,
          TRY_CONVERT(INT, d.PI) AS PI,
          CAST(SUM(COALESCE(TRY_CONVERT(FLOAT, d.DERIVED_FTE_FEATURE), 0.0)) AS FLOAT) AS DEMAND_DERIVED_FTE
        FROM {_fq('VW_TCO_FEATURE_DEMAND')} d
        WHERE {d_where_sql}
          AND COALESCE(d.IS_MSP_FEATURE, 0) = 0
          AND COALESCE(TRY_CONVERT(FLOAT, d.DERIVED_FTE_FEATURE), 0.0) > 0
          {d_scope_sql}
        GROUP BY
          d.PROGRAMID,
          d.PROGRAMNAME,
          d.TEAMID,
          d.TEAMNAME,
          d.GROUPID,
          d.GROUPNAME,
          TRY_CONVERT(INT, d.YEAR),
          TRY_CONVERT(INT, d.PI)
      ),
        """.rstrip()
            demand_params = d_params + d_scope_params

    sql = f"""
      {demand_with_prefix}
      c AS (
        SELECT
          c.YEAR,
          c.PI,
          c.TEAMID,
          c.HC_TEAM,
          c.HC_DELIVERY,
          c.HC_CONTRACTOR_C,
          c.HC_CONTRACTOR_CS,
          c.HC_TOTAL_LABOR
        FROM {_fq('VW_TCO_TEAM_LABOR_COMPOSITION')} c
        WHERE {c_where_sql}
      ),
      r AS (
        SELECT
          r.TEAMID,
          r.YEAR,
          r.PI,
          r.TEAM_RATE,
          r.DELIVERY_RATE,
          r.CONTRACTOR_C_RATE,
          r.CONTRACTOR_CS_RATE
        FROM {_fq('VW_TEAM_WEIGHTED_RATES')} r
        WHERE {r_where_sql}
      ),
      joined AS (
        SELECT
          d.PROGRAMID,
          d.PROGRAMNAME,
          d.TEAMID,
          d.TEAMNAME,
          d.GROUPID,
          d.GROUPNAME,
          d.YEAR,
          d.PI,
          d.DEMAND_DERIVED_FTE,
          COALESCE(c.HC_TEAM, 0) AS HC_TEAM,
          COALESCE(c.HC_DELIVERY, 0) AS HC_DELIVERY,
          COALESCE(c.HC_CONTRACTOR_C, 0) AS HC_CONTRACTOR_C,
          COALESCE(c.HC_CONTRACTOR_CS, 0) AS HC_CONTRACTOR_CS,
          COALESCE(c.HC_TOTAL_LABOR, 0) AS HC_TOTAL_LABOR,
          COALESCE(r.TEAM_RATE, 0) AS TEAM_RATE,
          COALESCE(r.DELIVERY_RATE, 0) AS DELIVERY_RATE,
          COALESCE(r.CONTRACTOR_C_RATE, 0) AS CONTRACTOR_C_RATE,
          COALESCE(r.CONTRACTOR_CS_RATE, 0) AS CONTRACTOR_CS_RATE
        FROM d
        LEFT JOIN c ON c.TEAMID = d.TEAMID AND c.YEAR = d.YEAR AND c.PI = d.PI
        LEFT JOIN r ON r.TEAMID = d.TEAMID AND r.YEAR = d.YEAR AND r.PI = d.PI
      ),
      weights AS (
        SELECT
          *,
          CASE WHEN HC_TOTAL_LABOR > 0 THEN HC_TEAM / HC_TOTAL_LABOR ELSE 0 END AS W_TEAM,
          CASE WHEN HC_TOTAL_LABOR > 0 THEN HC_DELIVERY / HC_TOTAL_LABOR ELSE 0 END AS W_DELIVERY,
          CASE WHEN HC_TOTAL_LABOR > 0 THEN HC_CONTRACTOR_C / HC_TOTAL_LABOR ELSE 0 END AS W_CONTRACTOR_C,
          CASE WHEN HC_TOTAL_LABOR > 0 THEN HC_CONTRACTOR_CS / HC_TOTAL_LABOR ELSE 0 END AS W_CONTRACTOR_CS
        FROM joined
      ),
      ado_labor_raw AS (
        SELECT
          'ADO' AS SOURCE,
          v.COST_CATEGORY,
          v.SUBCOMPONENT,
          w.PROGRAMID,
          w.PROGRAMNAME,
          w.TEAMID,
          w.TEAMNAME,
          w.GROUPID,
          w.GROUPNAME,
          w.YEAR,
          w.PI,
          CAST((w.DEMAND_DERIVED_FTE * v.WEIGHT * v.RATE) AS DECIMAL(18,2)) AS AMOUNT,
          CAST((w.DEMAND_DERIVED_FTE * v.WEIGHT) AS FLOAT) AS FTE
        FROM weights w
        CROSS APPLY (
          VALUES
            (CAST('WORK_FORCE' AS NVARCHAR(50)), CAST('Team' AS NVARCHAR(255)), w.W_TEAM, w.TEAM_RATE),
            (CAST('WORK_FORCE' AS NVARCHAR(50)), CAST('Delivery Team' AS NVARCHAR(255)), w.W_DELIVERY, w.DELIVERY_RATE),
            (CAST('WORK_FORCE' AS NVARCHAR(50)), CAST('Contractor C' AS NVARCHAR(255)), w.W_CONTRACTOR_C, w.CONTRACTOR_C_RATE),
            (CAST('NON_WORK_FORCE' AS NVARCHAR(50)), CAST('Contractor CS' AS NVARCHAR(255)), w.W_CONTRACTOR_CS, w.CONTRACTOR_CS_RATE)
        ) v(COST_CATEGORY, SUBCOMPONENT, WEIGHT, RATE)
        WHERE COALESCE(w.DEMAND_DERIVED_FTE, 0) > 0
      ),
      ado_labor AS (
        SELECT
          SOURCE,
          COST_CATEGORY,
          SUBCOMPONENT,
          PROGRAMID,
          PROGRAMNAME,
          TEAMID,
          TEAMNAME,
          GROUPID,
          GROUPNAME,
          YEAR,
          PI,
          CAST(SUM(COALESCE(AMOUNT, 0)) AS DECIMAL(18,2)) AS AMOUNT,
          CAST(SUM(COALESCE(FTE, 0)) AS FLOAT) AS FTE
        FROM ado_labor_raw
        GROUP BY
          SOURCE,
          COST_CATEGORY,
          SUBCOMPONENT,
          PROGRAMID,
          PROGRAMNAME,
          TEAMID,
          TEAMNAME,
          GROUPID,
          GROUPNAME,
          YEAR,
          PI
      ),
      base_rows AS (
        SELECT s.*
        FROM ado_labor s
        LEFT JOIN {_fq('APPLICATION_GROUPS')} ag ON ag.GROUPID = s.GROUPID
        WHERE ISNULL(ag.IS_BASE, 0) = 1
      ),
      non_base_src AS (
        SELECT s.*
        FROM ado_labor s
        LEFT JOIN {_fq('APPLICATION_GROUPS')} ag ON ag.GROUPID = s.GROUPID
        WHERE ISNULL(ag.IS_BASE, 0) = 0
      ),
      team_targets AS (
        SELECT
          g.GROUPID AS TARGET_GROUPID,
          g.GROUPNAME AS TARGET_GROUPNAME,
          g.TEAMID AS TARGET_TEAMID,
          COALESCE(g.PROGRAMID, t.PROGRAMID) AS TARGET_PROGRAMID,
          p.PROGRAMNAME AS TARGET_PROGRAMNAME,
          t.TEAMNAME AS TARGET_TEAMNAME
        FROM {_fq('APPLICATION_GROUPS')} g
        JOIN {_fq('TEAMS')} t ON t.TEAMID = g.TEAMID
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = COALESCE(g.PROGRAMID, t.PROGRAMID)
      ),
      target_counts AS (
        SELECT TARGET_TEAMID, COUNT(*) AS TARGET_CT
        FROM team_targets
        GROUP BY TARGET_TEAMID
      ),
      base_split AS (
        SELECT
          s.SOURCE,
          s.COST_CATEGORY,
          s.SUBCOMPONENT,
          nb.TARGET_PROGRAMID AS PROGRAMID,
          nb.TARGET_PROGRAMNAME AS PROGRAMNAME,
          nb.TARGET_TEAMID AS TEAMID,
          nb.TARGET_TEAMNAME AS TEAMNAME,
          nb.TARGET_GROUPID AS GROUPID,
          nb.TARGET_GROUPNAME AS GROUPNAME,
          s.YEAR,
          s.PI,
          CAST(s.AMOUNT / NULLIF(tc.TARGET_CT, 0) AS DECIMAL(18,2)) AS AMOUNT,
          TRY_CONVERT(FLOAT, s.FTE) / NULLIF(tc.TARGET_CT, 0) AS FTE
        FROM base_rows s
        JOIN team_targets nb ON nb.TARGET_TEAMID = s.TEAMID
        JOIN target_counts tc ON tc.TARGET_TEAMID = s.TEAMID
      ),
      ado_union AS (
        SELECT * FROM non_base_src
        UNION ALL
        SELECT
          SOURCE,
          COST_CATEGORY,
          SUBCOMPONENT,
          PROGRAMID,
          PROGRAMNAME,
          TEAMID,
          TEAMNAME,
          GROUPID,
          GROUPNAME,
          YEAR,
          PI,
          AMOUNT,
          FTE
        FROM base_split
      ),
      ado_filtered AS (
        SELECT
          SOURCE,
          COST_CATEGORY,
          SUBCOMPONENT,
          PROGRAMID,
          PROGRAMNAME,
          TEAMID,
          TEAMNAME,
          GROUPID,
          GROUPNAME,
          YEAR,
          PI,
          CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(FTE AS FLOAT) AS FTE
        FROM ado_union
        WHERE {base_where_sql}
      ),
      non_ado_src AS (
        SELECT
          'INVOICE' AS SOURCE,
          CAST('NON_WORK_FORCE' AS NVARCHAR(50)) AS COST_CATEGORY,
          isp.SUBCOMPONENT,
          isp.PROGRAMID,
          isp.PROGRAMNAME,
          isp.TEAMID,
          isp.TEAMNAME,
          isp.GROUPID,
          isp.GROUPNAME,
          TRY_CONVERT(INT, isp.FISCAL_YEAR) AS YEAR,
          TRY_CONVERT(INT, isp.PI) AS PI,
          CAST(isp.AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(NULL AS FLOAT) AS FTE
        FROM {_fq('VW_INVOICE_SPEND_PI')} isp

        UNION ALL

        SELECT
          m.SOURCE,
          m.COST_CATEGORY,
          m.SUBCOMPONENT,
          m.PROGRAMID,
          m.PROGRAMNAME,
          m.TEAMID,
          m.TEAMNAME,
          m.GROUPID,
          m.GROUPNAME,
          TRY_CONVERT(INT, m.YEAR) AS YEAR,
          TRY_CONVERT(INT, m.PI) AS PI,
          CAST(m.AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(NULL AS FLOAT) AS FTE
        FROM {_fq('VW_MSP_COSTS')} m

        UNION ALL

        SELECT
          'PROGRAM_ADDITIONAL' AS SOURCE,
          CAST('NON_WORK_FORCE' AS NVARCHAR(50)) AS COST_CATEGORY,
          CASE
            WHEN UPPER(pac.COST_TYPE) = 'CLOUD' AND ISNULL(pac.SUBTYPE,'') <> '' THEN CONCAT('Cloud ', pac.SUBTYPE)
            WHEN UPPER(pac.COST_TYPE) = 'CLOUD' THEN 'Cloud'
            WHEN UPPER(pac.COST_TYPE) = 'TRAVEL' THEN 'Travel'
            WHEN UPPER(pac.COST_TYPE) = 'INFRASTRUCTURE' THEN 'Infra'
            ELSE pac.COST_TYPE
          END AS SUBCOMPONENT,
          pac.PROGRAMID,
          pac.PROGRAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS TEAMID,
          CAST(NULL AS NVARCHAR(255)) AS TEAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS GROUPID,
          CAST(NULL AS NVARCHAR(255)) AS GROUPNAME,
          TRY_CONVERT(INT, pac.YEAR) AS YEAR,
          TRY_CONVERT(INT, pac.PI) AS PI,
          CAST(pac.AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(NULL AS FLOAT) AS FTE
        FROM {_fq('VW_PROGRAM_ADDITIONAL_COSTS_PI')} pac
      ),
      non_ado_base_rows AS (
        SELECT s.*
        FROM non_ado_src s
        LEFT JOIN {_fq('APPLICATION_GROUPS')} ag ON ag.GROUPID = s.GROUPID
        WHERE ISNULL(ag.IS_BASE, 0) = 1
      ),
      non_ado_non_base_src AS (
        SELECT s.*
        FROM non_ado_src s
        LEFT JOIN {_fq('APPLICATION_GROUPS')} ag ON ag.GROUPID = s.GROUPID
        WHERE ISNULL(ag.IS_BASE, 0) = 0
      ),
      non_ado_team_targets AS (
        SELECT
          g.GROUPID AS TARGET_GROUPID,
          g.GROUPNAME AS TARGET_GROUPNAME,
          g.TEAMID AS TARGET_TEAMID,
          COALESCE(g.PROGRAMID, t.PROGRAMID) AS TARGET_PROGRAMID,
          p.PROGRAMNAME AS TARGET_PROGRAMNAME,
          t.TEAMNAME AS TARGET_TEAMNAME
        FROM {_fq('APPLICATION_GROUPS')} g
        JOIN {_fq('TEAMS')} t ON t.TEAMID = g.TEAMID
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = COALESCE(g.PROGRAMID, t.PROGRAMID)
      ),
      non_ado_target_counts AS (
        SELECT TARGET_TEAMID, COUNT(*) AS TARGET_CT
        FROM non_ado_team_targets
        GROUP BY TARGET_TEAMID
      ),
      non_ado_base_split AS (
        SELECT
          s.SOURCE,
          s.COST_CATEGORY,
          s.SUBCOMPONENT,
          nb.TARGET_PROGRAMID AS PROGRAMID,
          nb.TARGET_PROGRAMNAME AS PROGRAMNAME,
          nb.TARGET_TEAMID AS TEAMID,
          nb.TARGET_TEAMNAME AS TEAMNAME,
          nb.TARGET_GROUPID AS GROUPID,
          nb.TARGET_GROUPNAME AS GROUPNAME,
          s.YEAR,
          s.PI,
          CAST(s.AMOUNT / NULLIF(tc.TARGET_CT, 0) AS DECIMAL(18,2)) AS AMOUNT,
          CAST(TRY_CONVERT(FLOAT, s.FTE) / NULLIF(tc.TARGET_CT, 0) AS FLOAT) AS FTE
        FROM non_ado_base_rows s
        JOIN non_ado_team_targets nb ON nb.TARGET_TEAMID = s.TEAMID
        JOIN non_ado_target_counts tc ON tc.TARGET_TEAMID = s.TEAMID
      ),
      non_ado_union AS (
        SELECT * FROM non_ado_non_base_src
        UNION ALL
        SELECT * FROM non_ado_base_split
      ),
      non_ado AS (
        SELECT
          SOURCE,
          COST_CATEGORY,
          SUBCOMPONENT,
          PROGRAMID,
          PROGRAMNAME,
          TEAMID,
          TEAMNAME,
          GROUPID,
          GROUPNAME,
          TRY_CONVERT(INT, YEAR) AS YEAR,
          TRY_CONVERT(INT, PI) AS PI,
          CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(FTE AS FLOAT) AS FTE
        FROM non_ado_union
        WHERE {non_ado_where_sql}
      ),
      base AS (
        SELECT * FROM ado_filtered
        UNION ALL
        SELECT * FROM non_ado
      )
    """
    return sql, demand_params + c_params + r_params + base_where_params + non_ado_params


def _baseline_cost_lines_sql(filters: Optional[Filters]) -> tuple[str, list[Any]]:
    """Baseline = (capacity headcount × effective rates) + (program overhead) + (non-ADO costs)."""
    # Baseline workforce headcount is team+delivery+contractor composition, and is not app-group scoped.
    filters_hc = dict(filters or {})
    filters_hc.pop("app_group", None)
    where_sql, params_hc = _build_where(filters_hc, year_col="YEAR", pi_col="PI", allow_group_filter=False)
    where_sql_non_ado, params_non_ado = _build_where(filters, year_col="YEAR", pi_col="PI", allow_group_filter=False)
    loc = str((filters or {}).get("location") or "GBC").strip()
    # NOTE: program overhead rate view is location-scoped.
    ov_where_parts: list[str] = ["1=1"]
    ov_params: list[Any] = []
    f = filters or {}
    years = [int(y) for y in _as_list(f.get("year")) if str(y).strip().isdigit()]
    pis = [int(p) for p in _as_list(f.get("pi")) if str(p).strip().isdigit()]
    programs = [str(p).strip() for p in _as_list(f.get("program")) if str(p).strip()]
    teams = [str(t).strip() for t in _as_list(f.get("team")) if str(t).strip()]

    # Workforce baseline must be composition-driven for stability and performance.
    # Use the effective team labor composition view (already PI-expanded and latest-wins)
    # instead of re-aggregating raw headcount/contractor history on every page load.
    comp0_where_parts: list[str] = ["c.YEAR IS NOT NULL", "c.PI IS NOT NULL"]
    comp0_params: list[Any] = []
    _add_in(comp0_where_parts, comp0_params, "c.YEAR", years)
    _add_in(comp0_where_parts, comp0_params, "c.PI", pis)
    _add_in(comp0_where_parts, comp0_params, "p.PROGRAMNAME", programs, upper=True)
    _add_in(comp0_where_parts, comp0_params, "t.TEAMNAME", teams, upper=True)
    comp0_where = " AND ".join(comp0_where_parts)

    _add_in(ov_where_parts, ov_params, "c.YEAR", years)
    _add_in(ov_where_parts, ov_params, "c.PI", pis)
    _add_in(ov_where_parts, ov_params, "p.PROGRAMNAME", programs, upper=True)
    ov_where = " AND ".join(ov_where_parts)

    r_where_parts: list[str] = ["1=1"]
    r_params: list[Any] = []
    _add_in(r_where_parts, r_params, "rw.YEAR", years)
    _add_in(r_where_parts, r_params, "rw.PI", pis)
    r_where = " AND ".join(r_where_parts)
    sql = f"""
      ;WITH comp0 AS (
        SELECT
          c.TEAMID,
          t.TEAMNAME,
          t.PROGRAMID,
          p.PROGRAMNAME,
          c.YEAR AS YEAR,
          c.PI AS PI,
          COALESCE(TRY_CONVERT(FLOAT, c.HC_TEAM), 0.0) AS HC_TEAM,
          COALESCE(TRY_CONVERT(FLOAT, c.HC_DELIVERY), 0.0) AS HC_DELIVERY,
          COALESCE(TRY_CONVERT(FLOAT, c.HC_CONTRACTOR_C), 0.0) AS HC_CONTRACTOR_C,
          COALESCE(TRY_CONVERT(FLOAT, c.HC_CONTRACTOR_CS), 0.0) AS HC_CONTRACTOR_CS
        FROM {_fq('VW_TCO_TEAM_LABOR_COMPOSITION')} c
        LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = c.TEAMID
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
        WHERE {comp0_where}
      ),
      comp AS (
        SELECT * FROM comp0
        WHERE {where_sql}
      ),
      comp_scope AS (
        SELECT DISTINCT TEAMID, YEAR, PI
        FROM comp
      ),
      r AS (
        SELECT
          rw.TEAMID,
          rw.YEAR AS YEAR,
          rw.PI AS PI,
          COALESCE(TRY_CONVERT(FLOAT, rw.TEAM_RATE), 0.0) AS TEAM_RATE,
          COALESCE(TRY_CONVERT(FLOAT, rw.DELIVERY_RATE), COALESCE(TRY_CONVERT(FLOAT, rw.TEAM_RATE), 0.0)) AS DELIVERY_RATE,
          COALESCE(TRY_CONVERT(FLOAT, rw.CONTRACTOR_C_RATE), 0.0) AS CONTRACTOR_C_RATE,
          COALESCE(TRY_CONVERT(FLOAT, rw.CONTRACTOR_CS_RATE), 0.0) AS CONTRACTOR_CS_RATE
        FROM {_fq('VW_TEAM_WEIGHTED_RATES')} rw
        JOIN comp_scope cs
          ON cs.TEAMID = rw.TEAMID
         AND cs.YEAR = rw.YEAR
         AND cs.PI = rw.PI
        WHERE {r_where}
      ),
      wf AS (
        SELECT
          'TCO_BASELINE' AS SOURCE,
          v.COST_CATEGORY,
          v.SUBCOMPONENT,
          comp.PROGRAMID, comp.PROGRAMNAME,
          comp.TEAMID, comp.TEAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS GROUPID,
          CAST('' AS NVARCHAR(255)) AS GROUPNAME,
          comp.YEAR, comp.PI,
          CAST(ROUND(v.HC * COALESCE(v.RATE, 0.0), 2) AS DECIMAL(18,2)) AS AMOUNT,
          CAST(v.HC AS FLOAT) AS FTE
        FROM comp
        LEFT JOIN r ON r.TEAMID = comp.TEAMID AND r.YEAR = comp.YEAR AND r.PI = comp.PI
        CROSS APPLY (
          VALUES
            (CAST('WORK_FORCE' AS NVARCHAR(50)), CAST('Team' AS NVARCHAR(255)), comp.HC_TEAM, r.TEAM_RATE),
            (CAST('WORK_FORCE' AS NVARCHAR(50)), CAST('Delivery Team' AS NVARCHAR(255)), comp.HC_DELIVERY, r.DELIVERY_RATE),
            (CAST('WORK_FORCE' AS NVARCHAR(50)), CAST('Contractor C' AS NVARCHAR(255)), comp.HC_CONTRACTOR_C, r.CONTRACTOR_C_RATE),
            (CAST('NON_WORK_FORCE' AS NVARCHAR(50)), CAST('Contractor CS' AS NVARCHAR(255)), comp.HC_CONTRACTOR_CS, r.CONTRACTOR_CS_RATE)
        ) v(COST_CATEGORY, SUBCOMPONENT, HC, RATE)
      ),
      ov AS (
        SELECT
          'TCO_BASELINE' AS SOURCE,
          'WORK_FORCE' AS COST_CATEGORY,
          'Program' AS SUBCOMPONENT,
          c.PROGRAMID,
          p.PROGRAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS TEAMID,
          CAST('(Program overhead)' AS NVARCHAR(255)) AS TEAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS GROUPID,
          CAST('' AS NVARCHAR(255)) AS GROUPNAME,
          c.YEAR AS YEAR,
          c.PI AS PI,
          CAST(
            ROUND(
              COALESCE(TRY_CONVERT(FLOAT, c.PROGRAMFTE), 0.0) * COALESCE(TRY_CONVERT(FLOAT, pr.PROGRAM_XOM_RATE), 0.0),
              2
            ) AS DECIMAL(18,2)
          ) AS AMOUNT,
          CAST(COALESCE(TRY_CONVERT(FLOAT, c.PROGRAMFTE), 0.0) AS FLOAT) AS FTE
        FROM {_fq('VW_PROGRAM_COMPOSITION_EFFECTIVE')} c
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = c.PROGRAMID
        LEFT JOIN {_fq('VW_PROGRAM_RATE_EFFECTIVE')} pr
         ON pr.PROGRAMID = c.PROGRAMID
         AND pr.YEAR = c.YEAR
         AND pr.PI = c.PI
         AND UPPER(LTRIM(RTRIM(pr.LOCATION))) = UPPER(LTRIM(RTRIM(%s)))
        WHERE {ov_where}
      ),
      non_ado_src AS (
        SELECT
          'INVOICE' AS SOURCE,
          CAST('NON_WORK_FORCE' AS NVARCHAR(50)) AS COST_CATEGORY,
          isp.SUBCOMPONENT,
          isp.PROGRAMID,
          isp.PROGRAMNAME,
          isp.TEAMID,
          isp.TEAMNAME,
          isp.GROUPID,
          isp.GROUPNAME,
          TRY_CONVERT(INT, isp.FISCAL_YEAR) AS YEAR,
          TRY_CONVERT(INT, isp.PI) AS PI,
          CAST(isp.AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(NULL AS FLOAT) AS FTE
        FROM {_fq('VW_INVOICE_SPEND_PI')} isp

        UNION ALL

        SELECT
          m.SOURCE,
          m.COST_CATEGORY,
          m.SUBCOMPONENT,
          m.PROGRAMID,
          m.PROGRAMNAME,
          m.TEAMID,
          m.TEAMNAME,
          m.GROUPID,
          m.GROUPNAME,
          TRY_CONVERT(INT, m.YEAR) AS YEAR,
          TRY_CONVERT(INT, m.PI) AS PI,
          CAST(m.AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(NULL AS FLOAT) AS FTE
        FROM {_fq('VW_MSP_COSTS')} m

        UNION ALL

        SELECT
          'PROGRAM_ADDITIONAL' AS SOURCE,
          CAST('NON_WORK_FORCE' AS NVARCHAR(50)) AS COST_CATEGORY,
          CASE
            WHEN UPPER(pac.COST_TYPE) = 'CLOUD' AND ISNULL(pac.SUBTYPE,'') <> '' THEN CONCAT('Cloud ', pac.SUBTYPE)
            WHEN UPPER(pac.COST_TYPE) = 'CLOUD' THEN 'Cloud'
            WHEN UPPER(pac.COST_TYPE) = 'TRAVEL' THEN 'Travel'
            WHEN UPPER(pac.COST_TYPE) = 'INFRASTRUCTURE' THEN 'Infra'
            ELSE pac.COST_TYPE
          END AS SUBCOMPONENT,
          pac.PROGRAMID,
          pac.PROGRAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS TEAMID,
          CAST(NULL AS NVARCHAR(255)) AS TEAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS GROUPID,
          CAST(NULL AS NVARCHAR(255)) AS GROUPNAME,
          TRY_CONVERT(INT, pac.YEAR) AS YEAR,
          TRY_CONVERT(INT, pac.PI) AS PI,
          CAST(pac.AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(NULL AS FLOAT) AS FTE
        FROM {_fq('VW_PROGRAM_ADDITIONAL_COSTS_PI')} pac
      ),
      non_ado_base_rows AS (
        SELECT s.*
        FROM non_ado_src s
        LEFT JOIN {_fq('APPLICATION_GROUPS')} ag ON ag.GROUPID = s.GROUPID
        WHERE ISNULL(ag.IS_BASE, 0) = 1
      ),
      non_ado_non_base_src AS (
        SELECT s.*
        FROM non_ado_src s
        LEFT JOIN {_fq('APPLICATION_GROUPS')} ag ON ag.GROUPID = s.GROUPID
        WHERE ISNULL(ag.IS_BASE, 0) = 0
      ),
      non_ado_team_targets AS (
        SELECT
          g.GROUPID AS TARGET_GROUPID,
          g.GROUPNAME AS TARGET_GROUPNAME,
          g.TEAMID AS TARGET_TEAMID,
          COALESCE(g.PROGRAMID, t.PROGRAMID) AS TARGET_PROGRAMID,
          p.PROGRAMNAME AS TARGET_PROGRAMNAME,
          t.TEAMNAME AS TARGET_TEAMNAME
        FROM {_fq('APPLICATION_GROUPS')} g
        JOIN {_fq('TEAMS')} t ON t.TEAMID = g.TEAMID
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = COALESCE(g.PROGRAMID, t.PROGRAMID)
      ),
      non_ado_target_counts AS (
        SELECT TARGET_TEAMID, COUNT(*) AS TARGET_CT
        FROM non_ado_team_targets
        GROUP BY TARGET_TEAMID
      ),
      non_ado_base_split AS (
        SELECT
          s.SOURCE,
          s.COST_CATEGORY,
          s.SUBCOMPONENT,
          nb.TARGET_PROGRAMID AS PROGRAMID,
          nb.TARGET_PROGRAMNAME AS PROGRAMNAME,
          nb.TARGET_TEAMID AS TEAMID,
          nb.TARGET_TEAMNAME AS TEAMNAME,
          nb.TARGET_GROUPID AS GROUPID,
          nb.TARGET_GROUPNAME AS GROUPNAME,
          s.YEAR,
          s.PI,
          CAST(s.AMOUNT / NULLIF(tc.TARGET_CT, 0) AS DECIMAL(18,2)) AS AMOUNT,
          CAST(TRY_CONVERT(FLOAT, s.FTE) / NULLIF(tc.TARGET_CT, 0) AS FLOAT) AS FTE
        FROM non_ado_base_rows s
        JOIN non_ado_team_targets nb ON nb.TARGET_TEAMID = s.TEAMID
        JOIN non_ado_target_counts tc ON tc.TARGET_TEAMID = s.TEAMID
      ),
      non_ado_union AS (
        SELECT * FROM non_ado_non_base_src
        UNION ALL
        SELECT * FROM non_ado_base_split
      ),
      non_ado AS (
        SELECT
          SOURCE,
          COST_CATEGORY,
          SUBCOMPONENT,
          PROGRAMID, PROGRAMNAME,
          TEAMID, TEAMNAME,
          GROUPID, GROUPNAME,
          YEAR,
          PI,
          CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(NULL AS FLOAT) AS FTE
        FROM non_ado_union
        WHERE {where_sql_non_ado}
      ),
      base AS (
        SELECT * FROM wf
        UNION ALL SELECT * FROM ov
        UNION ALL SELECT * FROM non_ado
      )
    """
    # Parameter order must match placeholder order in the SQL text:
    # 1) comp0 WHERE ({comp0_where}) -> comp0_params
    # 2) comp WHERE ({where_sql}) -> params_hc
    # 3) r WHERE ({r_where}) -> r_params
    # 4) pr.LOCATION = %s -> loc
    # 5) ov WHERE ({ov_where}) -> ov_params
    # 6) non_ado WHERE ({where_sql_non_ado}) -> params_non_ado
    return sql, comp0_params + params_hc + r_params + [loc] + ov_params + params_non_ado


def _cost_lines_sql(db: Any, scenario: str, filters: Optional[Filters]) -> tuple[str, list[Any], ScenarioSpec]:
    filters = _resolve_scope_filters_with_aliases(db, filters)
    spec = _normalize_scenario(scenario)
    if spec.name == "Projected":
        if _projected_demand_snapshot_enabled():
            _ensure_projected_demand_snapshot_ready()
        if _projected_fte_driver_mode(db) == "SNAPSHOT_VELOCITY":
            _ensure_projected_velocity_snapshot_ready()
        sql, params = _projected_cost_lines_sql(filters, db=db)
        return sql, params, spec
    if spec.name == "Actual":
        # Actuals are appended in Python (monthly Apptio NWF → PI allocation via ADO iteration calendar).
        # Keep the SQL base empty so that all authoritative actual math stays in canonical functions.
        sql = """
          ;WITH base AS (
            SELECT
              CAST(NULL AS NVARCHAR(255)) AS SOURCE,
              CAST(NULL AS NVARCHAR(255)) AS COST_CATEGORY,
              CAST(NULL AS NVARCHAR(255)) AS SUBCOMPONENT,
              CAST(NULL AS NVARCHAR(255)) AS PROGRAMID,
              CAST(NULL AS NVARCHAR(255)) AS PROGRAMNAME,
              CAST(NULL AS NVARCHAR(255)) AS TEAMID,
              CAST(NULL AS NVARCHAR(255)) AS TEAMNAME,
              CAST(NULL AS NVARCHAR(255)) AS GROUPID,
              CAST(NULL AS NVARCHAR(255)) AS GROUPNAME,
	              CAST(NULL AS INT) AS YEAR,
	              CAST(NULL AS INT) AS PI,
	              CAST(NULL AS DECIMAL(18,2)) AS AMOUNT,
	              CAST(NULL AS FLOAT) AS FTE
	            WHERE 1=0
	          )
	        """
        return sql, [], spec
    sql, params = _baseline_cost_lines_sql(filters)
    return sql, params, spec


def get_pi_costs(
    db: Any,
    scenario: str = "Projected",
    filters: Optional[dict] = None,
    *,
    include_allocated_actuals: bool = False,
    include_native_actuals: bool = True,
) -> pd.DataFrame:
    """Return costs aggregated by PI, broken down by labor bucket.

    Parameters
    ----------
    db:
      A `fetch_df(sql, params)` callable or an object exposing `fetch_df`.
    scenario:
      "Projected" (Expected), "Baseline" (Budget), or "Actual" (Actuals).
    filters:
      Optional filters: year, pi, program, team, app_group (plus optional `location` for baseline overhead rates).
    include_allocated_actuals / include_native_actuals:
      Only applies for `scenario="Actual"`. Phase 3 returns program-level Actual NWF; app allocation is not implemented yet.

    Returns
    -------
    DataFrame columns:
      YEAR, PI, SCENARIO, LABOR_BUCKET, TOTAL_COST
    """
    filters = _resolve_scope_filters_with_aliases(db, filters)
    spec = _normalize_scenario(scenario)

    if isinstance(db, pd.DataFrame):
        lines = get_cost_lines(
            db,
            scenario=scenario,
            filters=filters,
            include_allocated_actuals=include_allocated_actuals,
            include_native_actuals=include_native_actuals,
        )
        group_by = _as_list((filters or {}).get("group_by"))
        dims = ["YEAR", "PI"]
        for d in group_by:
            d0 = str(d or "").strip().upper()
            if d0 in {"PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"} and d0 not in dims:
                dims.append(d0)
        if lines is None or lines.empty:
            return pd.DataFrame(columns=dims + ["SCENARIO", "LABOR_BUCKET", "TOTAL_COST"])
        w = lines.copy()
        w["LABOR_BUCKET"] = _labor_bucket_series(w)
        out = (
            w.groupby(dims + ["LABOR_BUCKET"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "TOTAL_COST"})
        )
        out["SCENARIO"] = spec.name
        out["TOTAL_COST"] = pd.to_numeric(out.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)
        return out[dims + ["SCENARIO", "LABOR_BUCKET", "TOTAL_COST"]]

    if spec.name == "Actual":
        lines = get_cost_lines(
            db,
            scenario="Actual",
            filters=filters,
            include_allocated_actuals=include_allocated_actuals,
            include_native_actuals=include_native_actuals,
        )
        group_by = _as_list((filters or {}).get("group_by"))
        dims = ["YEAR", "PI"]
        for d in group_by:
            d0 = str(d or "").strip().upper()
            if d0 in {"PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"} and d0 not in dims:
                dims.append(d0)

        if lines is None or lines.empty:
            return pd.DataFrame(columns=dims + ["SCENARIO", "LABOR_BUCKET", "TOTAL_COST"])

        w = lines.copy()
        w["LABOR_BUCKET"] = _labor_bucket_series(w)
        out = (
            w.groupby(dims + ["LABOR_BUCKET"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "TOTAL_COST"})
        )
        out["SCENARIO"] = "Actual"
        out["TOTAL_COST"] = pd.to_numeric(out.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)
        return out[dims + ["SCENARIO", "LABOR_BUCKET", "TOTAL_COST"]]

    if spec.name == "Projected":
        lines = get_cost_lines(db, scenario="Projected", filters=filters)
        group_by = _as_list((filters or {}).get("group_by"))
        dims = ["YEAR", "PI"]
        for d in group_by:
            d0 = str(d or "").strip().upper()
            if d0 in {"PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"} and d0 not in dims:
                dims.append(d0)
        if lines is None or lines.empty:
            return pd.DataFrame(columns=dims + ["SCENARIO", "LABOR_BUCKET", "TOTAL_COST"])
        w = lines.copy()
        w["LABOR_BUCKET"] = _labor_bucket_series(w)
        out = (
            w.groupby(dims + ["LABOR_BUCKET"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "TOTAL_COST"})
        )
        out["SCENARIO"] = "Projected"
        out["TOTAL_COST"] = pd.to_numeric(out.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)
        return out[dims + ["SCENARIO", "LABOR_BUCKET", "TOTAL_COST"]]

    fetch = _resolve_fetch(db)
    sql, params, _ = _cost_lines_sql(db, scenario, filters)
    group_by = _as_list((filters or {}).get("group_by"))
    dims = ["YEAR", "PI"]
    for d in group_by:
        d0 = str(d or "").strip().upper()
        if d0 in {"PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE"} and d0 not in dims:
            dims.append(d0)

    labor_expr = _labor_bucket_sql()
    status_filter = _mapping_status_filter(filters, scenario=spec.name)
    mapped_predicate = (
        " AND COALESCE(NULLIF(LTRIM(RTRIM(GROUPNAME)), ''), '') <> '' "
        if (spec.name == "Projected" and status_filter == ["MAPPED"])
        else ""
    )
    sql2 = f"""
      {sql}
      SELECT
        {", ".join(dims)},
        '{spec.name}' AS SCENARIO,
        {labor_expr} AS LABOR_BUCKET,
        CAST(SUM(COALESCE(AMOUNT, 0)) AS DECIMAL(18,2)) AS TOTAL_COST
      FROM base
      WHERE 1=1 {mapped_predicate}
      GROUP BY {", ".join(dims)}, {labor_expr}
      ORDER BY YEAR, PI, LABOR_BUCKET
    """
    out = fetch(sql2, params)
    return out if out is not None else pd.DataFrame(columns=dims + ["SCENARIO", "LABOR_BUCKET", "TOTAL_COST"])


def get_app_group_costs(
    db: Any,
    scenario: str = "Projected",
    filters: Optional[dict] = None,
    *,
    include_allocated_actuals: bool = False,
    include_native_actuals: bool = True,
) -> pd.DataFrame:
    """Return costs aggregated by app group (GROUPNAME), optionally broken down by PI and labor bucket.

    Notes
    -----
    Baseline workforce costs are not inherently app-attributed; they will typically show up under
    GROUPNAME = '' (unassigned) unless future allocation logic is applied.
    """
    filters = _resolve_scope_filters_with_aliases(db, filters)
    spec = _normalize_scenario(scenario)

    if isinstance(db, pd.DataFrame):
        lines = get_cost_lines(
            db,
            scenario=scenario,
            filters=filters,
            include_allocated_actuals=include_allocated_actuals,
            include_native_actuals=include_native_actuals,
        )
        if lines is None or lines.empty:
            return pd.DataFrame(columns=["YEAR", "PI", "GROUPNAME", "SCENARIO", "LABOR_BUCKET", "TOTAL_COST"])
        w = lines.copy()
        w["GROUPNAME"] = w.get("GROUPNAME", "").fillna("").astype(str).str.strip()
        w["LABOR_BUCKET"] = _labor_bucket_series(w)
        out = (
            w.groupby(["YEAR", "PI", "GROUPNAME", "LABOR_BUCKET"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "TOTAL_COST"})
        )
        out["SCENARIO"] = spec.name
        out["TOTAL_COST"] = pd.to_numeric(out.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)
        return out[["YEAR", "PI", "GROUPNAME", "SCENARIO", "LABOR_BUCKET", "TOTAL_COST"]]

    if spec.name == "Actual":
        lines = get_cost_lines(
            db,
            scenario="Actual",
            filters=filters,
            include_allocated_actuals=include_allocated_actuals,
            include_native_actuals=include_native_actuals,
        )
        if lines is None or lines.empty:
            return pd.DataFrame(columns=["YEAR", "PI", "GROUPNAME", "SCENARIO", "LABOR_BUCKET", "TOTAL_COST"])
        w = lines.copy()
        w["GROUPNAME"] = w.get("GROUPNAME", "").fillna("").astype(str).str.strip()
        w["LABOR_BUCKET"] = _labor_bucket_series(w)
        out = (
            w.groupby(["YEAR", "PI", "GROUPNAME", "LABOR_BUCKET"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "TOTAL_COST"})
        )
        out["SCENARIO"] = "Actual"
        out["TOTAL_COST"] = pd.to_numeric(out.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)
        return out[["YEAR", "PI", "GROUPNAME", "SCENARIO", "LABOR_BUCKET", "TOTAL_COST"]]

    if spec.name == "Projected":
        lines = get_cost_lines(db, scenario="Projected", filters=filters)
        if lines is None or lines.empty:
            return pd.DataFrame(columns=["YEAR", "PI", "GROUPNAME", "SCENARIO", "LABOR_BUCKET", "TOTAL_COST"])
        w = lines.copy()
        w["GROUPNAME"] = w.get("GROUPNAME", "").fillna("").astype(str).str.strip()

        # Back-compat: when the caller doesn't explicitly ask for mapping-aware behavior, keep the historical "(Unassigned)" label.
        status_filter = _mapping_status_filter(filters, scenario=spec.name)
        if (filters or {}).get("include_unmapped") is True or (status_filter and "UNMAPPED_APP_GROUP" in status_filter):
            w.loc[w["MAPPING_STATUS"] == "UNMAPPED_APP_GROUP", "GROUPNAME"] = "(Needs mapping)"
        elif not status_filter:
            w.loc[w["GROUPNAME"] == "", "GROUPNAME"] = "(Unassigned)"

        w["LABOR_BUCKET"] = _labor_bucket_series(w)
        out = (
            w.groupby(["YEAR", "PI", "GROUPNAME", "LABOR_BUCKET"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "TOTAL_COST"})
        )
        out["SCENARIO"] = "Projected"
        out["TOTAL_COST"] = pd.to_numeric(out.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)
        return out[["YEAR", "PI", "GROUPNAME", "SCENARIO", "LABOR_BUCKET", "TOTAL_COST"]]

    fetch = _resolve_fetch(db)
    sql, params, _ = _cost_lines_sql(db, scenario, filters)
    dims = ["YEAR", "PI", "GROUPNAME"]
    labor_expr = _labor_bucket_sql()
    status_filter = _mapping_status_filter(filters, scenario=spec.name)
    if spec.name == "Baseline":
        group_expr = "'(Not app-attributed)'"
    elif spec.name == "Projected" and ((filters or {}).get("include_unmapped") is True or (status_filter and "UNMAPPED_APP_GROUP" in status_filter)):
        group_expr = "COALESCE(NULLIF(LTRIM(RTRIM(GROUPNAME)), ''), '(Needs mapping)')"
    else:
        # Back-compat: historical label for unmapped rows unless the page asks for mapping-aware behavior.
        group_expr = "COALESCE(NULLIF(LTRIM(RTRIM(GROUPNAME)), ''), '(Unassigned)')"
    mapped_predicate = (
        " AND COALESCE(NULLIF(LTRIM(RTRIM(GROUPNAME)), ''), '') <> '' "
        if (spec.name == "Projected" and status_filter == ["MAPPED"])
        else ""
    )
    sql2 = f"""
      {sql}
      SELECT
        YEAR,
        PI,
        {group_expr} AS GROUPNAME,
        '{spec.name}' AS SCENARIO,
        {labor_expr} AS LABOR_BUCKET,
        CAST(SUM(COALESCE(AMOUNT, 0)) AS DECIMAL(18,2)) AS TOTAL_COST
      FROM base
      WHERE 1=1 {mapped_predicate}
      GROUP BY YEAR, PI, {group_expr}, {labor_expr}
      ORDER BY YEAR, PI, TOTAL_COST DESC
    """
    out = fetch(sql2, params)
    return out if out is not None else pd.DataFrame(columns=["YEAR", "PI", "GROUPNAME", "SCENARIO", "LABOR_BUCKET", "TOTAL_COST"])


def get_feature_costs(db: Any, filters: Optional[dict] = None) -> pd.DataFrame:
    """Return feature-level Projected (Expected) costs based on velocity-aware Derived FTE demand.

    This function MUST NOT use manual/custom FTE overrides.
    It reads from the canonical DB view `VW_TCO_WF_LABOR_SPLIT`, which:
    - uses feature-level demand from `VW_TCO_FEATURE_DEMAND`
      (`DERIVED_FTE_FEATURE_VELOCITY` with fallback to `DERIVED_FTE_FEATURE`),
    - splits cost across labor types using team composition weights,
    - excludes MSP features (MSP is handled separately via `VW_MSP_COSTS`).
    """
    filters = _resolve_scope_filters_with_aliases(db, filters)
    fetch = _resolve_fetch(db)
    where_sql, params = _build_where(filters, year_col="YEAR", pi_col="PI")
    sql = f"""
      SELECT
        YEAR,
        PI,
        PROGRAMNAME,
        TEAMNAME,
        GROUPNAME,
        MAPPING_STATUS,
        ADO_FEATURE_ID AS FEATURE_ID,
        ADO_FEATURE_TITLE AS TITLE,
        ADO_FEATURE_STATE AS STATE,
        COST_CATEGORY,
        SUBCOMPONENT,
        CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
        CAST(FTE AS FLOAT) AS DERIVED_FTE,
        CAST(FTE AS FLOAT) AS DERIVED_FTE_SWAG
      FROM {_fq('VW_TCO_WF_LABOR_SPLIT')}
      WHERE {where_sql}
    """
    out = fetch(sql, params)
    return out if out is not None else pd.DataFrame(
        columns=[
            "YEAR","PI","PROGRAMNAME","TEAMNAME","GROUPNAME","MAPPING_STATUS","FEATURE_ID","TITLE","STATE",
            "COST_CATEGORY","SUBCOMPONENT","AMOUNT","DERIVED_FTE","DERIVED_FTE_SWAG",
        ]
    )


def get_unassigned_breakdown(db: Any, scenario: str = "Projected", filters: Optional[dict] = None) -> pd.DataFrame:
    """Return diagnostics for unassigned costs.

    This is a best-effort classification based on missing IDs/names and PI validity. It helps answer:
    - "Which costs are missing app-group mapping?"
    - "Which costs are missing team/program mapping?"
    - "Which costs are missing PI/year mapping?"
    """
    filters = _resolve_scope_filters_with_aliases(db, filters)
    spec0 = _normalize_scenario(scenario)
    # Actuals are program-level by design; "missing app-group mapping" would be misleading here.
    if spec0.name == "Actual":
        return pd.DataFrame(
            columns=["SCENARIO", "ISSUE", "SOURCE", "COST_CATEGORY", "SUBCOMPONENT", "YEAR", "PI", "RECORDS", "TOTAL_COST"]
        )

    if isinstance(db, pd.DataFrame):
        lines = get_cost_lines(db, scenario=scenario, filters=filters)
        if lines is None or lines.empty:
            return pd.DataFrame(
                columns=["SCENARIO", "ISSUE", "SOURCE", "COST_CATEGORY", "SUBCOMPONENT", "YEAR", "PI", "RECORDS", "TOTAL_COST"]
            )
        w = lines.copy()
        w["YEAR"] = _to_int_series(w.get("YEAR"))
        w["PI"] = _to_int_series(w.get("PI"))
        for c in ["SOURCE", "COST_CATEGORY", "SUBCOMPONENT", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"]:
            if c not in w.columns:
                w[c] = ""
            w[c] = w[c].fillna("").astype(str).str.strip()
        w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)

        issue = pd.Series("OK", index=w.index, dtype="object")
        issue.loc[w["YEAR"].isna()] = "Missing YEAR"
        issue.loc[issue.eq("OK") & (w["PI"].isna() | ~w["PI"].between(1, 4))] = "Missing/Invalid PI"
        issue.loc[issue.eq("OK") & w["SOURCE"].astype(str).str.upper().eq("PROGRAM_ADDITIONAL")] = "Program additional (program-level)"
        issue.loc[issue.eq("OK") & w["GROUPNAME"].eq("")] = "Missing APP_GROUP mapping"
        issue.loc[issue.eq("OK") & w["TEAMNAME"].eq("")] = "Missing TEAM mapping"
        issue.loc[issue.eq("OK") & w["PROGRAMNAME"].eq("")] = "Missing PROGRAM mapping"
        w["ISSUE"] = issue

        bad = w[w["ISSUE"].ne("OK")].copy()
        if bad.empty:
            return pd.DataFrame(
                columns=["SCENARIO", "ISSUE", "SOURCE", "COST_CATEGORY", "SUBCOMPONENT", "YEAR", "PI", "RECORDS", "TOTAL_COST"]
            )
        out = (
            bad.groupby(["ISSUE", "SOURCE", "COST_CATEGORY", "SUBCOMPONENT", "YEAR", "PI"], dropna=False)["AMOUNT"]
            .agg(RECORDS="count", TOTAL_COST="sum")
            .reset_index()
        )
        out.insert(0, "SCENARIO", spec0.name)
        out["TOTAL_COST"] = pd.to_numeric(out.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)
        out["RECORDS"] = pd.to_numeric(out.get("RECORDS"), errors="coerce").fillna(0).astype(int)
        return out[["SCENARIO", "ISSUE", "SOURCE", "COST_CATEGORY", "SUBCOMPONENT", "YEAR", "PI", "RECORDS", "TOTAL_COST"]]

    fetch = _resolve_fetch(db)
    sql, params, spec = _cost_lines_sql(db, scenario, filters)
    sql2 = f"""
      {sql},
      tagged AS (
        SELECT
          YEAR,
          PI,
          SOURCE,
          COST_CATEGORY,
          SUBCOMPONENT,
          PROGRAMID, PROGRAMNAME,
          TEAMID, TEAMNAME,
          GROUPID, GROUPNAME,
          CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CASE
            WHEN YEAR IS NULL THEN 'Missing YEAR'
            WHEN PI IS NULL OR PI NOT BETWEEN 1 AND 4 THEN 'Missing/Invalid PI'
            WHEN UPPER(COALESCE(SOURCE, '')) = 'PROGRAM_ADDITIONAL' THEN 'Program additional (program-level)'
            WHEN COALESCE(NULLIF(LTRIM(RTRIM(GROUPNAME)), ''), '') = '' THEN 'Missing APP_GROUP mapping'
            WHEN COALESCE(NULLIF(LTRIM(RTRIM(TEAMNAME)), ''), '') = '' THEN 'Missing TEAM mapping'
            WHEN COALESCE(NULLIF(LTRIM(RTRIM(PROGRAMNAME)), ''), '') = '' THEN 'Missing PROGRAM mapping'
            ELSE 'OK'
          END AS ISSUE
        FROM base
      )
      SELECT
        '{spec.name}' AS SCENARIO,
        ISSUE,
        SOURCE,
        COST_CATEGORY,
        SUBCOMPONENT,
        YEAR,
        PI,
        COUNT(*) AS RECORDS,
        CAST(SUM(COALESCE(AMOUNT, 0)) AS DECIMAL(18,2)) AS TOTAL_COST
      FROM tagged
      WHERE ISSUE <> 'OK'
      GROUP BY ISSUE, SOURCE, COST_CATEGORY, SUBCOMPONENT, YEAR, PI
      ORDER BY TOTAL_COST DESC
    """
    out = fetch(sql2, params)
    return out if out is not None else pd.DataFrame(
        columns=["SCENARIO","ISSUE","SOURCE","COST_CATEGORY","SUBCOMPONENT","YEAR","PI","RECORDS","TOTAL_COST"]
    )


def get_msp_costs(db: Any, scenario: str = "Projected", filters: Optional[dict] = None) -> pd.DataFrame:
    """Return MSP costs by app group and PI using MSP-specific allocation logic.

    MSP costs are NOT computed via Derived FTE. They come from `VW_MSP_COSTS`, which is based on:
    - `TEAM_MSP_ASSIGNMENTS` (app-group ownership/weights over PI ranges)
    - `TEAM_MSP_RATE` (rates per PI and size)
    """
    filters = _resolve_scope_filters_with_aliases(db, filters)
    _ = _normalize_scenario(scenario)  # accepted for symmetry; MSP is not scenario-derived
    fetch = _resolve_fetch(db)
    where_sql, params = _build_where(filters, year_col="YEAR", pi_col="PI")
    sql = f"""
      SELECT
        YEAR,
        PI,
        PROGRAMNAME,
        TEAMNAME,
        GROUPNAME,
        SUBCOMPONENT,
        CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT
      FROM {_fq('VW_MSP_COSTS')}
      WHERE {where_sql}
    """
    out = fetch(sql, params)
    return out if out is not None else pd.DataFrame(
        columns=["YEAR","PI","PROGRAMNAME","TEAMNAME","GROUPNAME","SUBCOMPONENT","AMOUNT"]
    )
