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
   - Workforce projected costs MUST be based on SWAG-derived Derived FTE only.
   - MSP costs MUST follow MSP-specific allocation logic and MUST NOT be computed via Derived FTE.

3) Actual (Actuals) [optional]
   - Program-level actuals (typically Apptio) used for diagnostics and variance context.
   - This scenario is not app-attributed.

Canonical database views used (Azure SQL)
----------------------------------------
- `VW_TCO_WORKFORCE_SPLIT`: unified costs (WF + NWF) including ADO-derived workforce, invoices, MSP, program additional.
  Backed by `VW_COSTS_AND_INVOICES` in the DB bootstrap (`db/mssql_backend.py`).
- `VW_TCO_WF_LABOR_SPLIT`: feature×labor-type costs for ADO workload, built from SWAG-derived Derived FTE only
  (excluding MSP features).
- `VW_MSP_COSTS`: MSP costs per PI/app group using MSP assignment + rate tables.
- `VW_TEAM_HEADCOUNT_EFFECTIVE` / `VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE`: stable staffing plan headcount by team×PI.
- `VW_TEAM_WEIGHTED_RATES`: effective per-PI rates by team (includes contractor rates).
- `VW_PROGRAM_COMPOSITION_EFFECTIVE` + `VW_PROGRAM_RATE_EFFECTIVE`: program overhead headcount and rate for baseline overhead.

Legacy/obsolete inputs intentionally avoided
--------------------------------------------
- Any manual/custom effort overrides from ADO are NOT used here.
- Feature-level Projected costs are sourced from `VW_TCO_WF_LABOR_SPLIT` with SWAG-derived FTE only.
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


CANONICAL_COSTS_VERSION = "2026-01-02_expected_program_overhead_fix_v1"


def _fq(name: str) -> str:
    """Return a fully-qualified DB object name when the DB facade is available."""
    try:
        from db import _fq as _db_fq  # type: ignore

        return _db_fq(name)
    except Exception:
        return name


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
        # Demand driver contract: pages should consume FTE only (SWAG-derived demand for Projected).
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
    try:
        out = fetch(select_sql, params)
    except Exception:
        allow_snapshot_fallback = (
            spec.name == "Projected"
            and str(os.getenv("TCO_PROJECTED_COST_FAILOPEN_SNAPSHOT", "1") or "1").strip().lower() in {"1", "true", "yes"}
        )
        if not allow_snapshot_fallback:
            raise
        snap_sql, snap_params = _projected_cost_lines_snapshot_sql(filters_for_sql)
        out = fetch(snap_sql, snap_params)
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
    for key in ("program", "team", "app_group"):
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


def _projected_cost_lines_sql(filters: Optional[Filters]) -> tuple[str, list[Any]]:
    where_sql, params = _build_where(filters, year_col="YEAR", pi_col="PI")
    sql = f"""
      ;WITH base AS (
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
          CAST(FTE AS FLOAT) AS FTE
        FROM {_fq('VW_TCO_WORKFORCE_SPLIT')}
        WHERE {where_sql}
          AND UPPER(COALESCE(SOURCE, '')) <> 'TCO_BASELINE'
      )
    """
    return sql, params


def _projected_cost_lines_snapshot_sql(filters: Optional[Filters]) -> tuple[str, list[Any]]:
    """Fail-open projected path using projected-demand snapshot + team rates."""
    where_sql, params = _build_where(filters, year_col="d.YEAR", pi_col="d.PI")
    sql = f"""
      SELECT
        TRY_CONVERT(INT, d.YEAR) AS YEAR,
        TRY_CONVERT(INT, d.PI) AS PI,
        d.PROGRAMNAME,
        d.TEAMNAME,
        COALESCE(d.GROUPNAME, '(Unmapped Application)') AS GROUPNAME,
        CAST('WORK_FORCE' AS NVARCHAR(40)) AS COST_CATEGORY,
        CAST('Team' AS NVARCHAR(40)) AS SUBCOMPONENT,
        CAST('ADO' AS NVARCHAR(40)) AS SOURCE,
        CAST(
          ROUND(
            COALESCE(TRY_CONVERT(FLOAT, d.DERIVED_FTE_SUM), 0.0)
            * COALESCE(TRY_CONVERT(FLOAT, r.TEAM_RATE), 0.0),
            2
          ) AS DECIMAL(18,2)
        ) AS AMOUNT,
        CAST(COALESCE(TRY_CONVERT(FLOAT, d.DERIVED_FTE_SUM), 0.0) AS FLOAT) AS FTE
      FROM {_fq('TCO_PROJECTED_DEMAND_SNAPSHOT')} d
      LEFT JOIN {_fq('VW_TEAM_WEIGHTED_RATES')} r
        ON r.TEAMID = d.TEAMID
       AND TRY_CONVERT(INT, r.YEAR) = TRY_CONVERT(INT, d.YEAR)
       AND TRY_CONVERT(INT, r.PI) = TRY_CONVERT(INT, d.PI)
      WHERE {where_sql}
        AND COALESCE(TRY_CONVERT(FLOAT, d.DERIVED_FTE_SUM), 0.0) > 0.0
    """
    return sql, params


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
    _add_in(ov_where_parts, ov_params, "TRY_CONVERT(INT, c.YEAR)", years)
    _add_in(ov_where_parts, ov_params, "TRY_CONVERT(INT, c.PI)", pis)
    _add_in(ov_where_parts, ov_params, "p.PROGRAMNAME", programs, upper=True)
    ov_where = " AND ".join(ov_where_parts)
    # Performance guardrail:
    # Apply scope prefilters in headcount CTEs before aggregation.
    hc_pref_parts: list[str] = []
    hc_pref_params: list[Any] = []
    teams = [str(t).strip() for t in _as_list(f.get("team")) if str(t).strip()]
    _add_in(hc_pref_parts, hc_pref_params, "TRY_CONVERT(INT, h.YEAR)", years)
    _add_in(hc_pref_parts, hc_pref_params, "TRY_CONVERT(INT, h.PI)", pis)
    _add_in(hc_pref_parts, hc_pref_params, "p.PROGRAMNAME", programs, upper=True)
    _add_in(hc_pref_parts, hc_pref_params, "t.TEAMNAME", teams, upper=True)
    hc_pref_where = " AND ".join(hc_pref_parts) if hc_pref_parts else "1=1"
    sql = f"""
      ;WITH hc0 AS (
        SELECT
          h.TEAMID,
          t.TEAMNAME,
          t.PROGRAMID,
          p.PROGRAMNAME,
          TRY_CONVERT(INT, h.YEAR) AS YEAR,
          TRY_CONVERT(INT, h.PI) AS PI,
          CAST(
            SUM(
              CASE
                WHEN UPPER(COALESCE(h.CLASS, '')) = 'TEAM'
                  THEN COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0.0)
                ELSE 0.0
              END
            )
            AS FLOAT
          ) AS HC_TEAM,
          CAST(
            SUM(
              CASE
                WHEN UPPER(COALESCE(h.CLASS, '')) = 'DELIVERY'
                  THEN COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0.0)
                ELSE 0.0
              END
            )
            AS FLOAT
          ) AS HC_DELIVERY,
          CAST(0.0 AS FLOAT) AS HC_CONTRACTOR_C,
          CAST(0.0 AS FLOAT) AS HC_CONTRACTOR_CS
        FROM {_fq('VW_TEAM_HEADCOUNT_EFFECTIVE')} h
        LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = h.TEAMID
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
        WHERE TRY_CONVERT(INT, h.YEAR) IS NOT NULL
          AND TRY_CONVERT(INT, h.PI) IS NOT NULL
          AND {hc_pref_where}
        GROUP BY
          h.TEAMID,
          t.TEAMNAME,
          t.PROGRAMID,
          p.PROGRAMNAME,
          TRY_CONVERT(INT, h.YEAR),
          TRY_CONVERT(INT, h.PI)
      ),
      hc AS (
        SELECT * FROM hc0
        WHERE {where_sql}
      ),
      con0 AS (
        SELECT
          h.TEAMID,
          t.TEAMNAME,
          t.PROGRAMID,
          p.PROGRAMNAME,
          TRY_CONVERT(INT, h.YEAR) AS YEAR,
          TRY_CONVERT(INT, h.PI) AS PI,
          UPPER(COALESCE(h.CLASS, '')) AS CLASS,
          COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0.0) AS HEADCOUNT,
          COALESCE(TRY_CONVERT(FLOAT, cr.RATE), 0.0) AS RATE
        FROM {_fq('VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE')} h
        LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = h.TEAMID
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
        LEFT JOIN {_fq('VW_CONTRACTOR_RATE_EFFECTIVE')} cr
          ON cr.COMPANYID = h.COMPANYID
         AND UPPER(COALESCE(cr.CLASS, '')) = UPPER(COALESCE(h.CLASS, ''))
         AND TRY_CONVERT(INT, cr.YEAR) = TRY_CONVERT(INT, h.YEAR)
         AND TRY_CONVERT(INT, cr.PI) = TRY_CONVERT(INT, h.PI)
        WHERE TRY_CONVERT(INT, h.YEAR) IS NOT NULL
          AND TRY_CONVERT(INT, h.PI) IS NOT NULL
          AND {hc_pref_where}
      ),
      con AS (
        SELECT * FROM con0
        WHERE {where_sql}
      ),
      r AS (
        SELECT
          TEAMID,
          TRY_CONVERT(INT, YEAR) AS YEAR,
          TRY_CONVERT(INT, PI) AS PI,
          COALESCE(TRY_CONVERT(FLOAT, TEAM_RATE), 0.0) AS TEAM_RATE,
          COALESCE(TRY_CONVERT(FLOAT, DELIVERY_RATE), COALESCE(TRY_CONVERT(FLOAT, TEAM_RATE), 0.0)) AS DELIVERY_RATE,
          COALESCE(TRY_CONVERT(FLOAT, CONTRACTOR_C_RATE), 0.0) AS CONTRACTOR_C_RATE,
          COALESCE(TRY_CONVERT(FLOAT, CONTRACTOR_CS_RATE), 0.0) AS CONTRACTOR_CS_RATE
        FROM {_fq('VW_TEAM_WEIGHTED_RATES')}
      ),
      wf AS (
        SELECT
          'TCO_BASELINE' AS SOURCE,
          'WORK_FORCE' AS COST_CATEGORY,
          'Team' AS SUBCOMPONENT,
          hc.PROGRAMID, hc.PROGRAMNAME,
          hc.TEAMID, hc.TEAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS GROUPID,
          CAST('' AS NVARCHAR(255)) AS GROUPNAME,
          hc.YEAR, hc.PI,
          CAST(ROUND(hc.HC_TEAM * COALESCE(r.TEAM_RATE, 0.0), 2) AS DECIMAL(18,2)) AS AMOUNT,
          CAST(hc.HC_TEAM AS FLOAT) AS FTE
        FROM hc
        LEFT JOIN r ON r.TEAMID = hc.TEAMID AND r.YEAR = hc.YEAR AND r.PI = hc.PI

        UNION ALL
        SELECT
          'TCO_BASELINE' AS SOURCE,
          'WORK_FORCE' AS COST_CATEGORY,
          'Delivery Team' AS SUBCOMPONENT,
          hc.PROGRAMID, hc.PROGRAMNAME,
          hc.TEAMID, hc.TEAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS GROUPID,
          CAST('' AS NVARCHAR(255)) AS GROUPNAME,
          hc.YEAR, hc.PI,
          CAST(ROUND(hc.HC_DELIVERY * COALESCE(r.DELIVERY_RATE, 0.0), 2) AS DECIMAL(18,2)) AS AMOUNT,
          CAST(hc.HC_DELIVERY AS FLOAT) AS FTE
        FROM hc
        LEFT JOIN r ON r.TEAMID = hc.TEAMID AND r.YEAR = hc.YEAR AND r.PI = hc.PI

	        UNION ALL
	        SELECT
	          'TCO_BASELINE' AS SOURCE,
	          CASE
	            WHEN UPPER(COALESCE(con.CLASS, '')) = 'CONTRACTOR_CS'
	            THEN 'NON_WORK_FORCE'
	            ELSE 'WORK_FORCE'
	          END AS COST_CATEGORY,
	          CASE
	            WHEN UPPER(COALESCE(con.CLASS, '')) = 'CONTRACTOR_CS'
	            THEN 'Contractor CS'
	            ELSE 'Contractor C'
	          END AS SUBCOMPONENT,
	          con.PROGRAMID, con.PROGRAMNAME,
	          con.TEAMID, con.TEAMNAME,
	          CAST(NULL AS NVARCHAR(255)) AS GROUPID,
	          CAST('' AS NVARCHAR(255)) AS GROUPNAME,
	          con.YEAR, con.PI,
	          CAST(
	            ROUND(COALESCE(con.HEADCOUNT, 0.0) * COALESCE(con.RATE, 0.0), 2)
	            AS DECIMAL(18,2)
	          ) AS AMOUNT,
	          CAST(COALESCE(con.HEADCOUNT, 0.0) AS FLOAT) AS FTE
	        FROM con
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
          TRY_CONVERT(INT, c.YEAR) AS YEAR,
          TRY_CONVERT(INT, c.PI) AS PI,
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
        WHERE {ov_where}
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
        FROM {_fq('VW_TCO_WORKFORCE_SPLIT')}
        WHERE {where_sql_non_ado}
          AND UPPER(COALESCE(SOURCE,'')) NOT IN ('ADO', 'TCO_BASELINE')
      ),
      base AS (
        SELECT * FROM wf
        UNION ALL SELECT * FROM ov
        UNION ALL SELECT * FROM non_ado
      )
    """
    # Parameter order must match placeholder order in the SQL text:
    # 1) hc0 prefilter ({hc_pref_where}) -> hc_pref_params
    # 2) hc WHERE ({where_sql}) -> params_hc
    # 3) con0 prefilter ({hc_pref_where}) -> hc_pref_params
    # 4) con WHERE ({where_sql}) -> params_hc
    # 5) pr.LOCATION = %s -> loc
    # 6) ov WHERE ({ov_where}) -> ov_params
    # 7) non_ado WHERE ({where_sql_non_ado}) -> params_non_ado
    return sql, hc_pref_params + params_hc + hc_pref_params + params_hc + [loc] + ov_params + params_non_ado


def _baseline_cost_lines_fast_sql(filters: Optional[Filters]) -> tuple[str, list[Any]]:
    """Fast baseline path from unified split view (runtime-safe under local SQL pressure)."""
    # Keep baseline semantics close to canonical:
    # - Include TCO_BASELINE workforce rows already materialized in split view
    # - Include non-ADO rows (invoices, program additional, MSP, etc.)
    where_sql, params = _build_where(filters, year_col="YEAR", pi_col="PI", allow_group_filter=False)
    sql = f"""
      ;WITH base AS (
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
          CAST(FTE AS FLOAT) AS FTE
        FROM {_fq('VW_TCO_WORKFORCE_SPLIT')}
        WHERE {where_sql}
          AND UPPER(COALESCE(SOURCE, '')) <> 'ADO'
      )
    """
    return sql, params


def _cost_lines_sql(db: Any, scenario: str, filters: Optional[Filters]) -> tuple[str, list[Any], ScenarioSpec]:
    filters = _resolve_scope_filters_with_aliases(db, filters)
    spec = _normalize_scenario(scenario)
    if spec.name == "Projected":
        sql, params = _projected_cost_lines_sql(filters)
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
    use_fast_baseline = str(os.getenv("TCO_BASELINE_FAST_FROM_SPLIT", "1") or "1").strip().lower() in {"1", "true", "yes"}
    if use_fast_baseline:
        sql, params = _baseline_cost_lines_fast_sql(filters)
    else:
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
    """Return feature-level Projected (Expected) costs based on SWAG-derived Derived FTE only.

    This function MUST NOT use manual/custom FTE overrides.
    It reads from the canonical DB view `VW_TCO_WF_LABOR_SPLIT`, which:
    - uses the feature-level SWAG-derived demand from `VW_TCO_FEATURE_DEMAND`,
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
