from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd


# Bulk edit helper functions.
# To add a new entity editor, define key_cols/required_cols/numeric_cols/editable_cols and reuse the helpers below.


def normalize_editor_df(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    for col in df2.columns:
        if df2[col].dtype == "O":
            df2[col] = df2[col].fillna("").map(lambda x: str(x).strip())
        else:
            df2[col] = df2[col].where(pd.notna(df2[col]), None)
    return df2


def validate_bulk_df(
    entity: str,
    df: pd.DataFrame,
    config: Dict[str, Any],
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    df2 = df.copy()
    required_cols = config.get("required_cols") or []
    numeric_cols = config.get("numeric_cols") or []
    key_cols = config.get("key_cols") or []

    errors: List[str] = []
    valid_flags: List[bool] = []
    for _, row in df2.iterrows():
        row_errors: List[str] = []
        for col in required_cols:
            val = row.get(col)
            if val is None or (isinstance(val, str) and not val.strip()):
                row_errors.append(f"Missing {col}")
        for col in numeric_cols:
            val = row.get(col)
            try:
                if val is not None and float(val) < 0:
                    row_errors.append(f"{col} must be >= 0")
            except Exception:
                row_errors.append(f"{col} must be numeric")
        errors.append("; ".join(row_errors))
        valid_flags.append(len(row_errors) == 0)

    df2["_VALID"] = valid_flags
    df2["_ERRORS"] = errors
    df2["_ACTION"] = ["INVALID" if not ok else "PENDING" for ok in valid_flags]

    summary = {
        "invalid": sum(1 for ok in valid_flags if not ok),
        "total": len(df2),
    }

    if key_cols:
        def _has_key(row: pd.Series) -> bool:
            for col in key_cols:
                val = row.get(col)
                if val is None or (isinstance(val, str) and not val.strip()):
                    return False
            return True

        keyed = df2[df2.apply(_has_key, axis=1)]
        dupes = keyed[keyed.duplicated(subset=key_cols, keep=False)]
        if not dupes.empty:
            df2.loc[dupes.index, "_VALID"] = False
            df2.loc[dupes.index, "_ERRORS"] = df2.loc[dupes.index, "_ERRORS"].apply(
                lambda x: f"{x}; Duplicate key" if x else "Duplicate key"
            )
            df2.loc[dupes.index, "_ACTION"] = "INVALID"
            summary["invalid"] = int(df2["_VALID"].eq(False).sum())

    return df2, summary


def compute_change_summary(
    original_df: pd.DataFrame,
    edited_df: pd.DataFrame,
    key_cols: List[str],
    editable_cols: List[str],
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    orig = normalize_editor_df(original_df)
    edit = normalize_editor_df(edited_df)

    orig_keyed = {}
    for _, row in orig.iterrows():
        key = tuple(row.get(k) for k in key_cols)
        if any(k is None or (isinstance(k, str) and not k.strip()) for k in key):
            continue
        orig_keyed[key] = row

    actions: List[str] = []
    for _, row in edit.iterrows():
        key = tuple(row.get(k) for k in key_cols)
        if any(k is None or (isinstance(k, str) and not k.strip()) for k in key):
            actions.append("NEW")
            continue
        before = orig_keyed.get(key)
        if before is None:
            actions.append("NEW")
            continue
        changed = False
        for col in editable_cols:
            if str(before.get(col) or "") != str(row.get(col) or ""):
                changed = True
                break
        actions.append("UPDATE" if changed else "UNCHANGED")

    edit["_ACTION"] = actions
    summary = {
        "new": int((edit["_ACTION"] == "NEW").sum()),
        "updated": int((edit["_ACTION"] == "UPDATE").sum()),
        "unchanged": int((edit["_ACTION"] == "UNCHANGED").sum()),
    }
    return edit, summary


def build_preview_payload(
    entity: str,
    edited_df: pd.DataFrame,
    summary: Dict[str, int],
    key_cols: List[str],
    limit: int = 20,
) -> Dict[str, Any]:
    changed = edited_df[edited_df["_ACTION"].isin(["NEW", "UPDATE"])]
    sample = changed[key_cols + [c for c in edited_df.columns if c not in key_cols]].head(limit)
    return {
        "ok": summary.get("invalid", 0) == 0,
        "message": f"{entity} preview ready.",
        "counts": summary,
        "sample": sample,
    }
