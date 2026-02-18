from __future__ import annotations

from typing import Dict, Iterable

import pandas as pd

from core.capacity_data import fetch_capacity_demand_pi


def _pi_label(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(dtype=str)
    if "PI_NAME" in df.columns and df["PI_NAME"].fillna("").astype(str).str.strip().ne("").any():
        return df["PI_NAME"].fillna("").astype(str).str.strip()
    if "PI" in df.columns:
        pi = pd.to_numeric(df.get("PI"), errors="coerce").fillna(0).astype(int)
        return pi.astype(str)
    if "MONTH_KEY" in df.columns:
        return df["MONTH_KEY"].fillna("").astype(str).str.strip()
    if "MONTH" in df.columns:
        return df["MONTH"].fillna("").astype(str).str.strip()
    return pd.Series(dtype=str)


def _norm_pi_label(label: str) -> str:
    return " ".join(str(label or "").strip().upper().split())


def capacity_fte_by_pi(
    years: Iterable[int],
    programs: list[str],
    teams: list[str],
) -> Dict[str, float]:
    frames = []
    for year in years:
        df = fetch_capacity_demand_pi(int(year), programs, teams)
        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
            frames.append(df)
    if not frames:
        return {}

    cap = pd.concat(frames, ignore_index=True)
    pi_label = cap.get("ITERATION_LEVEL3", "").fillna("").astype(str).str.strip()
    if pi_label.eq("").all():
        pi_label = _pi_label(cap)
    cap = cap.copy()
    cap["PI_LABEL"] = pi_label
    cap = cap.loc[cap["PI_LABEL"].fillna("").astype(str).str.strip().ne("")].copy()
    if cap.empty:
        return {}

    cap_col = "CAPACITY_SOD_FTE" if "CAPACITY_SOD_FTE" in cap.columns and cap["CAPACITY_SOD_FTE"].notna().any() else "CAPACITY_FTE"
    cap[cap_col] = pd.to_numeric(cap.get(cap_col), errors="coerce").fillna(0.0)
    grouped = cap.groupby("PI_LABEL", dropna=False)[cap_col].sum()
    return {_norm_pi_label(str(k)): float(v or 0.0) for k, v in grouped.items()}
