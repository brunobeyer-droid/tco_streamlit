from __future__ import annotations

"""
Program-level NWF actuals allocation (Phase 3).

This module provides a *transparent, reconcilable* way to allocate Apptio NWF actuals
that exist only at Program level down to App Groups for app-level analytics.

Design principles
-----------------
- Keep native program-level actuals intact (finance truth).
- Produce a separate allocated layer for app-level views (managerial).
- Never silently mix native + allocated in a way that double-counts.
- Allocation driver uses SWAG-derived Derived FTE only; when demand is zero/missing,
  allocation falls back to equal shares across active mapped app groups.

This module is intentionally UI-free (no Streamlit imports) and is intended to be used
by `core/canonical_costs.py` only.
"""

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

import pandas as pd

FetchFn = Callable[[str, Optional[Iterable[Any]]], pd.DataFrame]
Filters = Mapping[str, Any]


def _resolve_fetch(db: Any) -> FetchFn:
    if callable(db):
        return db  # type: ignore[return-value]
    if hasattr(db, "fetch_df") and callable(getattr(db, "fetch_df")):
        return getattr(db, "fetch_df")
    raise TypeError("`db` must be a callable (sql, params)->DataFrame or expose a `fetch_df(sql, params)` method.")


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [v for v in value if v is not None and str(v).strip() != ""]
    return [value]


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


def _month_to_pi_expr(month_expr: str = "a.MONTH") -> str:
    # Keep consistent with the existing canonical actuals stub (month->quarter->PI 1..4).
    return f"""
      CASE
        WHEN TRY_CONVERT(INT, {month_expr}) BETWEEN 1 AND 3 THEN 1
        WHEN TRY_CONVERT(INT, {month_expr}) BETWEEN 4 AND 6 THEN 2
        WHEN TRY_CONVERT(INT, {month_expr}) BETWEEN 7 AND 9 THEN 3
        WHEN TRY_CONVERT(INT, {month_expr}) BETWEEN 10 AND 12 THEN 4
        ELSE NULL
      END
    """.strip()


def get_program_nwf_actuals(
    db: Any,
    years: list[int],
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Return native (program-level) NWF actuals at YEAR×PI×PROGRAM grain.

    Source tables:
    - `APPTIO_ACTUALS`
    - `PROGRAM_APPTIO_WORKIDS`
    - `PROGRAMS`
    """
    fetch = _resolve_fetch(db)
    years0 = sorted({int(y) for y in years or [] if str(y).strip()})
    if not years0:
        return pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "AMOUNT"])

    where: list[str] = ["1=1"]
    params: list[Any] = []
    _add_in(where, params, "a.FISCAL_YEAR", years0)

    programs = [str(p).strip() for p in _as_list((filters or {}).get("program")) if str(p).strip()]
    _add_in(where, params, "p.PROGRAMNAME", programs, upper=True)

    pi_filter = [int(p) for p in _as_list((filters or {}).get("pi")) if str(p).strip().isdigit()]
    pi_predicate = ""
    pi_params: list[Any] = []
    if pi_filter:
        placeholders = ", ".join(["%s"] * len(pi_filter))
        pi_predicate = f" AND pi.PI IN ({placeholders})"
        pi_params = list(pi_filter)

    pi_expr = _month_to_pi_expr("a.MONTH")
    sql = f"""
      ;WITH pi AS (
        SELECT
          p.PROGRAMNAME,
          TRY_CONVERT(INT, a.FISCAL_YEAR) AS YEAR,
          {pi_expr} AS PI,
          CAST(SUM(COALESCE(TRY_CONVERT(FLOAT, a.AMOUNT), 0.0)) AS FLOAT) AS AMOUNT
        FROM APPTIO_ACTUALS a
        JOIN PROGRAM_APPTIO_WORKIDS w ON w.WORK_ID = a.WORK_ID
        JOIN PROGRAMS p ON p.PROGRAMID = w.PROGRAMID
        WHERE {" AND ".join(where)}
        GROUP BY
          p.PROGRAMNAME,
          TRY_CONVERT(INT, a.FISCAL_YEAR),
          {pi_expr}
      )
      SELECT YEAR, PI, PROGRAMNAME, CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT
      FROM pi
      WHERE 1=1 {pi_predicate}
      ORDER BY YEAR, PI, PROGRAMNAME
    """
    out = fetch(sql, params + pi_params)
    if out is None or out.empty:
        return pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "AMOUNT"])
    w = out.copy()
    w["YEAR"] = pd.to_numeric(w.get("YEAR"), errors="coerce").astype("Int64")
    w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce").astype("Int64")
    w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    return w[["YEAR", "PI", "PROGRAMNAME", "AMOUNT"]]


@dataclass(frozen=True)
class AllocationDriver:
    name: str  # e.g. "DERIVED_FTE" | "EQUAL"

def get_active_app_groups_for_program_pi(
    db: Any,
    years: list[int],
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Return distinct active *mapped* app groups per YEAR×PI×PROGRAM (Explorer v2 source).

    Active groups are defined as:
    - App groups that appear in ADO feature rows for the (PROGRAM, YEAR, PI) slice, and
    - Have a mapping to a known TCO app group (MAP_ADO_APP_TO_TCO_GROUP -> APPLICATION_GROUPS).

    This is used by the EQUAL allocation driver to distribute program-level costs evenly across active groups.
    """
    fetch = _resolve_fetch(db)
    years0 = sorted({int(y) for y in years or [] if str(y).strip()})
    if not years0:
        return pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "ACTIVE_FLAG"])

    where: list[str] = [f"TRY_CONVERT(INT, d.YEAR) IN ({', '.join(['%s'] * len(years0))})"]
    params: list[Any] = list(years0)

    programs = [str(p).strip() for p in _as_list((filters or {}).get("program")) if str(p).strip()]
    if programs:
        ph = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(d.PROGRAMNAME) IN ({ph})")
        params.extend([str(p).upper() for p in programs])

    pis = [int(p) for p in _as_list((filters or {}).get("pi")) if str(p).strip().isdigit()]
    if pis:
        pi_i = sorted({int(p) for p in pis if 1 <= int(p) <= 4})
        if pi_i:
            ph = ", ".join(["%s"] * len(pi_i))
            where.append(f"TRY_CONVERT(INT, d.PI) IN ({ph})")
            params.extend(pi_i)

    sql = f"""
      SELECT DISTINCT
        TRY_CONVERT(INT, d.YEAR) AS YEAR,
        TRY_CONVERT(INT, d.PI) AS PI,
        d.PROGRAMNAME,
        d.TEAMNAME,
        d.GROUPNAME
      FROM VW_TCO_FEATURE_DEMAND d
      WHERE COALESCE(TRY_CONVERT(INT, d.IN_SCOPE_FOR_ROADMAP), 0) = 1
        AND {" AND ".join(where)}
        AND COALESCE(NULLIF(LTRIM(RTRIM(d.GROUPNAME)), ''), '') <> ''
    """
    df = fetch(sql, params)
    if df is None or df.empty:
        return pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "ACTIVE_FLAG"])

    w = df.copy()
    w["YEAR"] = pd.to_numeric(w.get("YEAR"), errors="coerce").astype("Int64")
    w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce").astype("Int64")
    for c in ["PROGRAMNAME", "TEAMNAME", "GROUPNAME"]:
        w[c] = w.get(c, "").fillna("").astype(str).str.strip()
    w = w[(w["PROGRAMNAME"] != "") & w["YEAR"].notna() & w["PI"].notna() & (w["GROUPNAME"] != "")].copy()
    w["ACTIVE_FLAG"] = 1
    return w[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "ACTIVE_FLAG"]]


def build_equal_shares(active_groups_df: pd.DataFrame) -> pd.DataFrame:
    """Build equal allocation shares for active groups per YEAR×PI×PROGRAM."""
    if active_groups_df is None or active_groups_df.empty:
        return pd.DataFrame(
            columns=[
                "YEAR",
                "PI",
                "PROGRAMNAME",
                "TEAMNAME",
                "GROUPNAME",
                "DRIVER_VALUE",
                "SHARE",
                "ALLOCATION_DRIVER",
            ]
        )

    w = active_groups_df.copy()
    w["YEAR"] = pd.to_numeric(w.get("YEAR"), errors="coerce").astype("Int64")
    w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce").astype("Int64")
    w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    w["TEAMNAME"] = w.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    w["GROUPNAME"] = w.get("GROUPNAME", "").fillna("").astype(str).str.strip()
    w = w[(w["PROGRAMNAME"] != "") & w["YEAR"].notna() & w["PI"].notna() & (w["GROUPNAME"] != "")].copy()
    if w.empty:
        return build_equal_shares(pd.DataFrame())

    # One row per Program×PI×Group (choose a stable TEAMNAME if multiple are present).
    w = w.sort_values(["YEAR", "PI", "PROGRAMNAME", "GROUPNAME", "TEAMNAME"], ascending=[False, False, True, True, True])
    w = w.groupby(["YEAR", "PI", "PROGRAMNAME", "GROUPNAME"], dropna=False, as_index=False).agg(TEAMNAME=("TEAMNAME", "first"))

    group_counts = w.groupby(["YEAR", "PI", "PROGRAMNAME"], dropna=False)["GROUPNAME"].transform("nunique").astype(float)
    w["SHARE"] = 1.0 / group_counts.where(group_counts > 0)
    w["DRIVER_VALUE"] = 1.0
    w["ALLOCATION_DRIVER"] = "EQUAL"
    w = w[w["SHARE"].notna()].copy()
    return w[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "DRIVER_VALUE", "SHARE", "ALLOCATION_DRIVER"]]


def get_allocation_driver(
    db: Any,
    years: list[int],
    driver: str = "DERIVED_FTE",
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Return per-program per-PI app-group driver rows with allocation shares.

    Grain:
      YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, DRIVER_VALUE, SHARE, ALLOCATION_DRIVER

    Driver:
    - Derived FTE (SWAG-derived) from `VW_TCO_FEATURE_DEMAND` (Explorer v2 canonical demand)

    Special driver:
    - EQUAL: distribute equally across active mapped app groups per Program×PI slice (Explorer v2 source).
    """
    fetch = _resolve_fetch(db)
    years0 = sorted({int(y) for y in years or [] if str(y).strip()})
    if not years0:
        return pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "DRIVER_VALUE", "SHARE", "ALLOCATION_DRIVER"])

    desired = str(driver or "DERIVED_FTE").strip().upper()
    if desired in {"EQUAL", "EQUAL_SHARES"}:
        active = get_active_app_groups_for_program_pi(fetch, years=years0, filters=filters)
        return build_equal_shares(active)

    where: list[str] = [f"TRY_CONVERT(INT, d.YEAR) IN ({', '.join(['%s'] * len(years0))})"]
    params: list[Any] = list(years0)
    where.append("COALESCE(TRY_CONVERT(INT, d.IN_SCOPE_FOR_ROADMAP), 0) = 1")
    where.append("COALESCE(TRY_CONVERT(INT, d.IS_MSP_FEATURE), 0) = 0")

    programs = [str(p).strip() for p in _as_list((filters or {}).get("program")) if str(p).strip()]
    if programs:
        ph = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(d.PROGRAMNAME) IN ({ph})")
        params.extend([str(p).upper() for p in programs])

    pis = [int(p) for p in _as_list((filters or {}).get("pi")) if str(p).strip().isdigit()]
    if pis:
        pi_i = sorted({int(p) for p in pis if 1 <= int(p) <= 4})
        if pi_i:
            ph = ", ".join(["%s"] * len(pi_i))
            where.append(f"TRY_CONVERT(INT, d.PI) IN ({ph})")
            params.extend(pi_i)

    sql = f"""
      SELECT
        TRY_CONVERT(INT, d.YEAR) AS YEAR,
        TRY_CONVERT(INT, d.PI) AS PI,
        d.PROGRAMNAME,
        d.TEAMNAME,
        d.GROUPNAME,
        SUM(COALESCE(TRY_CONVERT(FLOAT, d.DERIVED_FTE_FEATURE), 0)) AS DERIVED_FTE_SWAG
      FROM VW_TCO_FEATURE_DEMAND d
      WHERE {" AND ".join(where)}
        AND COALESCE(NULLIF(LTRIM(RTRIM(d.GROUPNAME)), ''), '') <> ''
      GROUP BY
        TRY_CONVERT(INT, d.YEAR),
        TRY_CONVERT(INT, d.PI),
        d.PROGRAMNAME,
        d.TEAMNAME,
        d.GROUPNAME
    """
    feats = fetch(sql, tuple(params) if params else None)
    feats = feats.copy() if feats is not None else pd.DataFrame()

    for c in ["YEAR", "PI"]:
        if c in feats.columns:
            feats[c] = pd.to_numeric(feats[c], errors="coerce").astype("Int64")
    for c in ["PROGRAMNAME", "TEAMNAME", "GROUPNAME"]:
        if c not in feats.columns:
            feats[c] = ""
        feats[c] = feats[c].fillna("").astype(str).str.strip()
    for c in ["DERIVED_FTE_SWAG"]:
        if c not in feats.columns:
            feats[c] = 0.0
        feats[c] = pd.to_numeric(feats[c], errors="coerce").fillna(0.0).astype(float)

    # Collapse to one row per Program×PI×Group (choose a stable TEAMNAME if multiple are present).
    group_cols = ["YEAR", "PI", "PROGRAMNAME", "GROUPNAME"]
    fte_sum = (
        feats.sort_values(["YEAR", "PI", "PROGRAMNAME", "GROUPNAME", "TEAMNAME"], ascending=[False, False, True, True, True])
        .groupby(group_cols, dropna=False, as_index=False)
        .agg(TEAMNAME=("TEAMNAME", "first"), DERIVED_FTE_SWAG=("DERIVED_FTE_SWAG", "sum"))
        if not feats.empty
        else pd.DataFrame(columns=group_cols + ["TEAMNAME", "DERIVED_FTE_SWAG"])
    )

    # Use Derived FTE only. If a Program×PI slice has zero demand, it produces no app-attributable rows.
    merged = fte_sum.copy()
    merged["DERIVED_FTE_SWAG"] = pd.to_numeric(merged.get("DERIVED_FTE_SWAG"), errors="coerce").fillna(0.0).astype(float)
    merged["DRIVER_VALUE"] = merged["DERIVED_FTE_SWAG"]
    merged["ALLOCATION_DRIVER"] = "DERIVED_FTE"
    totals = merged.groupby(["YEAR", "PI", "PROGRAMNAME"], dropna=False)["DRIVER_VALUE"].transform("sum")
    merged["SHARE"] = merged["DRIVER_VALUE"] / totals.where(totals > 0)
    derived_out = merged[(merged["DRIVER_VALUE"] > 0) & merged["SHARE"].notna()].copy()

    out = derived_out

    if out is None or out.empty:
        return pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "DRIVER_VALUE", "SHARE", "ALLOCATION_DRIVER"])

    out["DRIVER_VALUE"] = pd.to_numeric(out.get("DRIVER_VALUE"), errors="coerce").fillna(0.0).astype(float)
    out["SHARE"] = pd.to_numeric(out.get("SHARE"), errors="coerce")
    out = out[out["SHARE"].notna() & (out["SHARE"] > 0)].copy()
    out = out.sort_values(["YEAR", "PI", "PROGRAMNAME", "DRIVER_VALUE"], ascending=[False, False, True, False])
    return out[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "DRIVER_VALUE", "SHARE", "ALLOCATION_DRIVER"]]


def allocate_program_nwf_to_app_groups(
    program_actuals_df: pd.DataFrame,
    driver_df: pd.DataFrame,
    *,
    cost_category: str = "NON_WORK_FORCE",
    source: str = "APPTIO",
    subcomponent: str = "Apptio Actuals (Allocated)",
    unallocated_group_label: str = "(Unallocated NWF)",
    default_allocation_driver: str = "",
) -> pd.DataFrame:
    """Allocate program-level NWF actuals down to app groups using provided shares.

    Output columns include:
      YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME,
      COST_CATEGORY, SOURCE, SUBCOMPONENT,
      AMOUNT, ALLOCATION_DRIVER, SHARE

    If a Program×PI has actuals but no driver rows, emits a single "(Unallocated NWF)" row so totals reconcile.
    """
    if program_actuals_df is None or program_actuals_df.empty:
        return pd.DataFrame(
            columns=[
                "YEAR",
                "PI",
                "PROGRAMNAME",
                "TEAMNAME",
                "GROUPNAME",
                "COST_CATEGORY",
                "SOURCE",
                "SUBCOMPONENT",
                "AMOUNT",
                "ALLOCATION_DRIVER",
                "SHARE",
            ]
        )

    prog = program_actuals_df.copy()
    prog["YEAR"] = pd.to_numeric(prog.get("YEAR"), errors="coerce").astype("Int64")
    prog["PI"] = pd.to_numeric(prog.get("PI"), errors="coerce").astype("Int64")
    prog["PROGRAMNAME"] = prog.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    prog["AMOUNT"] = pd.to_numeric(prog.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    prog = prog[(prog["PROGRAMNAME"] != "") & prog["YEAR"].notna() & prog["PI"].notna()].copy()
    if prog.empty:
        return pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SOURCE", "SUBCOMPONENT", "AMOUNT", "ALLOCATION_DRIVER", "SHARE"])

    drv = driver_df.copy() if driver_df is not None else pd.DataFrame()
    if drv.empty:
        out = prog.copy()
        out["TEAMNAME"] = ""
        out["GROUPNAME"] = unallocated_group_label
        out["COST_CATEGORY"] = str(cost_category or "NON_WORK_FORCE").strip()
        out["SOURCE"] = str(source or "").strip()
        out["SUBCOMPONENT"] = str(subcomponent or "").strip()
        out["ALLOCATION_DRIVER"] = str(default_allocation_driver or "").strip()
        out["SHARE"] = None
        return out[["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SOURCE", "SUBCOMPONENT", "AMOUNT", "ALLOCATION_DRIVER", "SHARE"]]

    for c in ["YEAR", "PI"]:
        drv[c] = pd.to_numeric(drv.get(c), errors="coerce").astype("Int64")
    for c in ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "ALLOCATION_DRIVER"]:
        drv[c] = drv.get(c, "").fillna("").astype(str).str.strip()
    drv["SHARE"] = pd.to_numeric(drv.get("SHARE"), errors="coerce")
    drv = drv[drv["SHARE"].notna() & (drv["SHARE"] > 0)].copy()
    if drv.empty:
        return allocate_program_nwf_to_app_groups(
            prog,
            pd.DataFrame(),
            cost_category=cost_category,
            source=source,
            subcomponent=subcomponent,
            unallocated_group_label=unallocated_group_label,
            default_allocation_driver=default_allocation_driver,
        )

    merged = prog.merge(drv, on=["YEAR", "PI", "PROGRAMNAME"], how="left", suffixes=("", "_DRV"))
    # Rows with no share become "unallocated" buckets (keeps reconciliation transparent).
    alloc = merged[merged["SHARE"].notna()].copy()
    alloc["AMOUNT"] = alloc["AMOUNT"] * alloc["SHARE"].astype(float)
    alloc["COST_CATEGORY"] = str(cost_category or "NON_WORK_FORCE").strip()
    alloc["SOURCE"] = str(source or "").strip()
    alloc["SUBCOMPONENT"] = str(subcomponent or "").strip()

    missing = merged[merged["SHARE"].isna()].copy()
    if not missing.empty:
        missing = (
            missing.groupby(["YEAR", "PI", "PROGRAMNAME"], dropna=False)[["AMOUNT"]]
            .sum()
            .reset_index()
        )
        missing["TEAMNAME"] = ""
        missing["GROUPNAME"] = unallocated_group_label
        missing["COST_CATEGORY"] = str(cost_category or "NON_WORK_FORCE").strip()
        missing["SOURCE"] = str(source or "").strip()
        missing["SUBCOMPONENT"] = str(subcomponent or "").strip()
        missing["ALLOCATION_DRIVER"] = str(default_allocation_driver or "").strip()
        missing["SHARE"] = None
        out = pd.concat([alloc, missing], ignore_index=True, sort=False)
    else:
        out = alloc

    out["AMOUNT"] = pd.to_numeric(out.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    return out[
        [
            "YEAR",
            "PI",
            "PROGRAMNAME",
            "TEAMNAME",
            "GROUPNAME",
            "COST_CATEGORY",
            "SOURCE",
            "SUBCOMPONENT",
            "AMOUNT",
            "ALLOCATION_DRIVER",
            "SHARE",
        ]
    ]
