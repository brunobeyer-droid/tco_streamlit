from __future__ import annotations
import pandas as pd

def to_kusd(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return s / 1_000.0

def nonempty(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    for c in cols:
        if c in work.columns:
            work[c] = (
                work[c]
                .astype(str)
                .str.strip()
                .replace({"": None, "None": None, "none": None, "NaN": None, "nan": None})
            )
    return work.dropna(subset=[c for c in cols if c in work.columns], how="any")

def topn(df: pd.DataFrame, n: int, by: str, ascending: bool = False) -> pd.DataFrame:
    if df.empty or by not in df.columns:
        return df
    return df.sort_values(by, ascending=ascending).head(n)
