from __future__ import annotations

import math
import warnings
from typing import Iterable, Optional, Sequence, Tuple

import pandas as pd


def _to_str_series(s: pd.Series) -> pd.Series:
    out = s.fillna("").astype(str)
    try:
        out = out.str.replace(r"\s+", " ", regex=True).str.strip()
    except Exception:
        out = out.astype(str)
    return out


def _normalize_cost_category(values: pd.Series) -> pd.Series:
    s = _to_str_series(values).str.upper()
    s = s.replace({"NWF": "NON_WORK_FORCE", "NON-WORKFORCE": "NON_WORK_FORCE"})
    return s


def _replace_non_finite(series: pd.Series, default: float = 0.0) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    if s is None:
        return pd.Series([default] * 0, dtype="float64")
    s = s.fillna(default)
    try:
        mask = ~s.apply(lambda v: math.isfinite(float(v)))
        s.loc[mask] = default
    except Exception:
        pass
    return s.astype(float)


def validate_cost_lines_contract(
    df: pd.DataFrame,
    *,
    context: str,
    required_cols: Optional[Sequence[str]] = None,
    amount_cols: Optional[Sequence[str]] = None,
) -> list[str]:
    if df is None or not isinstance(df, pd.DataFrame):
        return [f"{context}: not a DataFrame"]

    issues: list[str] = []
    required = list(required_cols or [])
    for col in required:
        if col not in df.columns:
            issues.append(f"{context}: missing column {col}")

    for col in (amount_cols or []):
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            if s.isna().any():
                issues.append(f"{context}: non-numeric values in {col}")
            try:
                if (~s.fillna(0.0).apply(lambda v: math.isfinite(float(v)))).any():
                    issues.append(f"{context}: non-finite values in {col}")
            except Exception:
                pass

    # NWF rows should never have an "Unassigned" type label; reserve it for app-group mapping gaps.
    if "COST_CATEGORY" in df.columns and "SUBCOMPONENT" in df.columns:
        cat = _normalize_cost_category(df["COST_CATEGORY"])
        sub = _to_str_series(df["SUBCOMPONENT"])
        bad = cat.eq("NON_WORK_FORCE") & sub.eq("(Unassigned)")
        if bool(bad.any()):
            issues.append(f"{context}: found '(Unassigned)' in SUBCOMPONENT for NON_WORK_FORCE rows")

    return issues


def apply_cost_lines_contract(
    df: pd.DataFrame,
    *,
    context: str,
    required_cols: Optional[Sequence[str]] = None,
    amount_cols: Optional[Sequence[str]] = None,
    warn: bool = True,
) -> Tuple[pd.DataFrame, list[str]]:
    """
    Apply minimal, safe "data contract" normalization to cost line frames.

    This intentionally avoids any business logic transformations beyond:
    - adding missing columns (as empty)
    - coercing numeric amount columns
    - preventing NWF type dimensions from becoming "(Unassigned)" or blank
    - filling SUBCOMPONENT_ROLLUP from SUBCOMPONENT when missing (NWF-only)
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return (df if isinstance(df, pd.DataFrame) else pd.DataFrame(), [])

    out = df.copy()

    for col in (required_cols or []):
        if col not in out.columns:
            out[col] = ""

    for col in (amount_cols or []):
        if col in out.columns:
            out[col] = _replace_non_finite(out[col], default=0.0)

    if "COST_CATEGORY" in out.columns:
        out["COST_CATEGORY"] = _normalize_cost_category(out["COST_CATEGORY"])

    if "COST_CATEGORY" in out.columns and "SUBCOMPONENT" in out.columns:
        cat = out["COST_CATEGORY"]
        sub = _to_str_series(out["SUBCOMPONENT"])
        is_nwf = cat.eq("NON_WORK_FORCE")
        sub = sub.where(~(is_nwf & sub.eq("(Unassigned)")), other="NWF Other")
        sub = sub.where(~(is_nwf & sub.eq("")), other="NWF Other")
        out["SUBCOMPONENT"] = sub

    if "SUBCOMPONENT_ROLLUP" not in out.columns:
        out["SUBCOMPONENT_ROLLUP"] = ""

    if "COST_CATEGORY" in out.columns and "SUBCOMPONENT" in out.columns and "SUBCOMPONENT_ROLLUP" in out.columns:
        cat = out["COST_CATEGORY"]
        sub = _to_str_series(out["SUBCOMPONENT"])
        roll = _to_str_series(out["SUBCOMPONENT_ROLLUP"])
        is_nwf = cat.eq("NON_WORK_FORCE")
        roll_missing = roll.eq("")
        out.loc[is_nwf & roll_missing & sub.ne(""), "SUBCOMPONENT_ROLLUP"] = sub.loc[is_nwf & roll_missing & sub.ne("")]

    issues = validate_cost_lines_contract(
        out,
        context=context,
        required_cols=required_cols,
        amount_cols=amount_cols,
    )
    if warn and issues:
        warnings.warn("; ".join(issues), RuntimeWarning)

    return (out, issues)

