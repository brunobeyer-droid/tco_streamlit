from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def allocate_monthly_to_pi(monthly_df: pd.DataFrame, pi_calendar_df: pd.DataFrame) -> pd.DataFrame:
    """
    Allocate program-level monthly costs into PIs using day-overlap.

    Parameters
    ----------
    monthly_df columns (required):
      PROGRAMID or PROGRAMNAME
      YEAR
      MONTH_KEY (YYYY-MM) OR MONTH_START_DATE (date/datetime)
      AMOUNT
    pi_calendar_df columns (required):
      YEAR
      PI_NAME (or PI)  (string)
      START_DATE (date/datetime)
      END_DATE (date/datetime)

    Returns
    -------
    DataFrame with columns:
      PROGRAMID/PROGRAMNAME, YEAR, PI_NAME, AMOUNT_PI

    Notes
    -----
    - Overlap uses inclusive day counts.
    - Rounding is applied at the end (2 decimals).
    - Reconciliation warnings are logged and also attached to `out.attrs["reconciliation_warnings"]`.
    """

    if monthly_df is None or monthly_df.empty:
        program_col = "PROGRAMID" if monthly_df is not None and "PROGRAMID" in monthly_df.columns else "PROGRAMNAME"
        return pd.DataFrame(columns=[program_col, "YEAR", "PI_NAME", "AMOUNT_PI"])

    if pi_calendar_df is None or pi_calendar_df.empty:
        program_col = "PROGRAMID" if "PROGRAMID" in monthly_df.columns else ("PROGRAMNAME" if "PROGRAMNAME" in monthly_df.columns else "PROGRAM")
        return pd.DataFrame(columns=[program_col, "YEAR", "PI_NAME", "AMOUNT_PI"])

    program_col = "PROGRAMID" if "PROGRAMID" in monthly_df.columns else ("PROGRAMNAME" if "PROGRAMNAME" in monthly_df.columns else "")
    if not program_col:
        raise ValueError("monthly_df must include PROGRAMID or PROGRAMNAME")

    if "YEAR" not in monthly_df.columns:
        raise ValueError("monthly_df must include YEAR")
    if "AMOUNT" not in monthly_df.columns:
        raise ValueError("monthly_df must include AMOUNT")
    if "MONTH_START_DATE" not in monthly_df.columns and "MONTH_KEY" not in monthly_df.columns:
        raise ValueError("monthly_df must include MONTH_KEY (YYYY-MM) or MONTH_START_DATE")

    pi_name_col = "PI_NAME" if "PI_NAME" in pi_calendar_df.columns else ("PI" if "PI" in pi_calendar_df.columns else "")
    if not pi_name_col:
        raise ValueError("pi_calendar_df must include PI_NAME (or PI)")
    for c in ["YEAR", "START_DATE", "END_DATE"]:
        if c not in pi_calendar_df.columns:
            raise ValueError(f"pi_calendar_df must include {c}")

    m = monthly_df.copy()
    m["YEAR"] = pd.to_numeric(m.get("YEAR"), errors="coerce").astype("Int64")
    m[program_col] = m.get(program_col, "").fillna("").astype(str).str.strip()
    m["AMOUNT"] = pd.to_numeric(m.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)

    if "MONTH_START_DATE" in m.columns and pd.to_datetime(m.get("MONTH_START_DATE"), errors="coerce").notna().any():
        month_start = pd.to_datetime(m.get("MONTH_START_DATE"), errors="coerce")
        m["MONTH_START_DATE"] = month_start.dt.to_period("M").dt.to_timestamp()
    else:
        mk = m.get("MONTH_KEY", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        mk2 = mk.str.replace(r"^(\d{4})(\d{2})$", r"\1-\2", regex=True)
        m["MONTH_START_DATE"] = pd.to_datetime(mk2.str.slice(0, 7) + "-01", errors="coerce")

    m["MONTH_END_DATE"] = m["MONTH_START_DATE"] + pd.offsets.MonthEnd(0)
    m["MONTH_DAYS"] = (m["MONTH_END_DATE"] - m["MONTH_START_DATE"]).dt.days + 1

    m_ok = m["YEAR"].notna() & (m[program_col] != "") & m["MONTH_START_DATE"].notna() & m["MONTH_END_DATE"].notna() & (m["MONTH_DAYS"] > 0)
    m = m[m_ok].copy()
    if m.empty:
        return pd.DataFrame(columns=[program_col, "YEAR", "PI_NAME", "AMOUNT_PI"])

    cal = pi_calendar_df.copy()
    cal["YEAR"] = pd.to_numeric(cal.get("YEAR"), errors="coerce").astype("Int64")
    cal["PI_NAME"] = cal.get(pi_name_col, "").fillna("").astype(str).str.strip()
    cal["START_DATE"] = pd.to_datetime(cal.get("START_DATE"), errors="coerce").dt.normalize()
    cal["END_DATE"] = pd.to_datetime(cal.get("END_DATE"), errors="coerce").dt.normalize()
    cal_ok = cal["YEAR"].notna() & (cal["PI_NAME"] != "") & cal["START_DATE"].notna() & cal["END_DATE"].notna()
    cal = cal[cal_ok].copy()
    if cal.empty:
        return pd.DataFrame(columns=[program_col, "YEAR", "PI_NAME", "AMOUNT_PI"])

    # Calendar sanity check: a PI calendar year should typically have ~4 PI rows.
    try:
        cal_counts = cal.groupby("YEAR", dropna=False)["PI_NAME"].nunique()
        bad_years = cal_counts[cal_counts > 6]
        if not bad_years.empty:
            logger.warning(
                "PI calendar has %s unique PI rows for YEAR(s): %s",
                bad_years.to_dict(),
                sorted([int(y) for y in bad_years.index.dropna().astype(int).tolist()]),
            )
    except Exception:
        pass

    work = m.merge(cal[["YEAR", "PI_NAME", "START_DATE", "END_DATE"]], on="YEAR", how="inner")
    work["OVERLAP_START"] = work[["MONTH_START_DATE", "START_DATE"]].max(axis=1)
    work["OVERLAP_END"] = work[["MONTH_END_DATE", "END_DATE"]].min(axis=1)
    work["OVERLAP_DAYS"] = (work["OVERLAP_END"] - work["OVERLAP_START"]).dt.days + 1
    work = work[work["OVERLAP_DAYS"] > 0].copy()
    if work.empty:
        return pd.DataFrame(columns=[program_col, "YEAR", "PI_NAME", "AMOUNT_PI"])

    work["SHARE"] = pd.to_numeric(work["OVERLAP_DAYS"], errors="coerce") / pd.to_numeric(work["MONTH_DAYS"], errors="coerce")
    work["ALLOCATED"] = work["AMOUNT"] * work["SHARE"]

    out = (
        work.groupby([program_col, "YEAR", "PI_NAME"], dropna=False)["ALLOCATED"]
        .sum()
        .reset_index()
        .rename(columns={"ALLOCATED": "AMOUNT_PI"})
    )
    out["AMOUNT_PI"] = pd.to_numeric(out.get("AMOUNT_PI"), errors="coerce").fillna(0.0).round(2).astype(float)

    # Reconciliation: per (PROGRAM, YEAR), monthly sum ~= PI-allocated sum.
    monthly_tot = m.groupby([program_col, "YEAR"], dropna=False)["AMOUNT"].sum().reset_index().rename(columns={"AMOUNT": "AMOUNT_MONTHLY"})
    pi_tot = out.groupby([program_col, "YEAR"], dropna=False)["AMOUNT_PI"].sum().reset_index().rename(columns={"AMOUNT_PI": "AMOUNT_PI_TOTAL"})
    recon = monthly_tot.merge(pi_tot, on=[program_col, "YEAR"], how="left").fillna({"AMOUNT_PI_TOTAL": 0.0})
    recon["DELTA"] = pd.to_numeric(recon["AMOUNT_PI_TOTAL"], errors="coerce").fillna(0.0) - pd.to_numeric(
        recon["AMOUNT_MONTHLY"], errors="coerce"
    ).fillna(0.0)

    warnings: list[dict[str, Any]] = []
    for _, r in recon.iterrows():
        total = float(r.get("AMOUNT_MONTHLY") or 0.0)
        delta = float(r.get("DELTA") or 0.0)
        tol = max(0.01 * abs(total), 0.05)
        if abs(delta) > tol:
            warn = {
                program_col: r.get(program_col),
                "YEAR": int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None,
                "AMOUNT_MONTHLY": total,
                "AMOUNT_PI_TOTAL": float(r.get("AMOUNT_PI_TOTAL") or 0.0),
                "DELTA": delta,
                "TOLERANCE": tol,
            }
            warnings.append(warn)
    if warnings:
        logger.warning("Monthly→PI allocation reconciliation warnings: %s", warnings[:10])
    out.attrs["reconciliation_warnings"] = warnings
    out.attrs["reconciliation"] = recon

    return out[[program_col, "YEAR", "PI_NAME", "AMOUNT_PI"]]
