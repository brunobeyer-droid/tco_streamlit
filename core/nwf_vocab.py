from __future__ import annotations

from typing import Any, Callable, Iterable, Optional

import pandas as pd

FetchFn = Callable[[str, Optional[Iterable[Any]]], pd.DataFrame]


REQUIRED_NWF_PAIRS: list[tuple[str, str]] = [
    ("Cloud", "AWS"),
    ("Cloud", "Azure"),
    ("Travel", ""),
    ("Invoices", ""),
    ("Contractor CS", ""),
    ("MSP", ""),
    ("Infra", ""),
    ("NWF", "Other"),
]


def _resolve_fetch(db: Any) -> FetchFn:
    if callable(db):
        return db  # type: ignore[return-value]
    if hasattr(db, "fetch_df") and callable(getattr(db, "fetch_df")):
        return getattr(db, "fetch_df")
    raise TypeError("`db` must be a callable (sql, params)->DataFrame or expose a `fetch_df(sql, params)` method.")


def get_nwf_vocab_pairs(db: Any) -> pd.DataFrame:
    """Return allowed NWF (COST_TYPE, SUBTYPE) pairs used across the app.

    Source of truth:
    - distinct pairs from PROGRAM_ADDITIONAL_COSTS (SUBTYPE normalized to '')
    - union with REQUIRED_NWF_PAIRS (always present)
    """
    fetch = _resolve_fetch(db)
    try:
        vocab = fetch(
            """
            SELECT DISTINCT
              LTRIM(RTRIM(COST_TYPE)) AS COST_TYPE,
              COALESCE(LTRIM(RTRIM(SUBTYPE)), '') AS SUBTYPE
            FROM PROGRAM_ADDITIONAL_COSTS
            WHERE COST_TYPE IS NOT NULL AND LTRIM(RTRIM(COST_TYPE)) <> ''
            """,
            None,
        )
        vocab = vocab if vocab is not None else pd.DataFrame()
    except Exception:
        vocab = pd.DataFrame()

    rows: list[dict[str, str]] = []
    if vocab is not None and not vocab.empty:
        for _, r in vocab.iterrows():
            ct = str(r.get("COST_TYPE") or "").strip()
            stp = str(r.get("SUBTYPE") or "").strip()
            if ct:
                rows.append({"COST_TYPE": ct, "SUBTYPE": stp})
    for ct, stp in REQUIRED_NWF_PAIRS:
        rows.append({"COST_TYPE": str(ct).strip(), "SUBTYPE": str(stp or "").strip()})

    out = pd.DataFrame(rows, columns=["COST_TYPE", "SUBTYPE"]).dropna()
    if out.empty:
        return pd.DataFrame(columns=["COST_TYPE", "SUBTYPE"])
    out["COST_TYPE"] = out["COST_TYPE"].fillna("").astype(str).str.strip()
    out["SUBTYPE"] = out["SUBTYPE"].fillna("").astype(str).str.strip()
    out = out[out["COST_TYPE"].ne("")].drop_duplicates(subset=["COST_TYPE", "SUBTYPE"]).copy()
    out = out.sort_values(["COST_TYPE", "SUBTYPE"]).reset_index(drop=True)
    return out


def get_nwf_cost_type_options(db: Any) -> list[str]:
    df = get_nwf_vocab_pairs(db)
    if df is None or df.empty:
        return []
    return sorted({str(v).strip() for v in df["COST_TYPE"].dropna().astype(str).tolist() if str(v).strip()})


def get_nwf_subtype_options(db: Any, cost_type: str) -> list[str]:
    ct = str(cost_type or "").strip()
    df = get_nwf_vocab_pairs(db)
    if df is None or df.empty or not ct:
        return [""]
    sub = df.loc[df["COST_TYPE"].astype(str).str.strip().eq(ct), "SUBTYPE"].fillna("").astype(str).tolist()
    return [""] + sorted({str(v).strip() for v in sub if str(v).strip()})

