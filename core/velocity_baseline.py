from __future__ import annotations

import datetime as dt
import re
from typing import Any, Optional

import pandas as pd

_SOURCE_ORDER = ("TEAM", "PROGRAM", "GLOBAL")


def _to_upper_set(values: list[str] | tuple[str, ...] | None) -> set[str]:
    if not values:
        return set()
    return {str(v).strip().upper() for v in values if str(v).strip()}


def _weighted_avg(series: pd.Series, weights: pd.Series) -> Optional[float]:
    vals = pd.to_numeric(series, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    mask = vals.notna()
    if not bool(mask.any()):
        return None
    vals = vals.loc[mask].astype(float)
    w = w.loc[mask].astype(float)
    if bool((w > 0).any()) and float(w.sum()) > 0:
        return float((vals * w).sum() / w.sum())
    return float(vals.mean())


def _parse_pi_parts(label: Any) -> tuple[Optional[int], Optional[int]]:
    s = str(label or "").strip().upper()
    if not s:
        return None, None
    m = re.search(r"^\s*(\d{4})\s*(?:PI|I)\s*([1-4])\s*$", s)
    if m:
        try:
            return int(m.group(1)), int(m.group(2))
        except Exception:
            return None, None
    m = re.search(r"(?:^|\s)(?:PI|I)\s*([1-4])(?:\s|$)", s)
    if not m:
        return None, None
    try:
        return None, int(m.group(1))
    except Exception:
        return None, None


def _format_pi_label(year: int, pi: int) -> str:
    return f"{int(year)} PI{int(pi)}"


def _source_counts(df: pd.DataFrame) -> dict[str, int]:
    if df is None or df.empty:
        return {}
    return (
        df.get("BASELINE_SOURCE", pd.Series(dtype=str))
        .fillna("")
        .astype(str)
        .str.upper()
        .str.strip()
        .value_counts(dropna=False)
        .to_dict()
    )


def resolve_reference_pi(
    *,
    calendar_df: Optional[pd.DataFrame],
    selected_year: int,
    as_of: dt.date,
) -> dict[str, Any]:
    out = {
        "reference_pi_order": None,
        "reference_pi_label": "—",
        "non_future_max_pi_order": None,
        "mode": "none",
    }
    if calendar_df is None or calendar_df.empty:
        return out

    c = calendar_df.copy()
    c["CAL_YEAR"] = pd.to_numeric(c.get("CAL_YEAR"), errors="coerce")
    c = c[c["CAL_YEAR"].eq(int(selected_year))].copy()
    if c.empty:
        return out

    c["CAL_START_DATE"] = pd.to_datetime(c.get("CAL_START_DATE"), errors="coerce")
    c["CAL_END_DATE"] = pd.to_datetime(c.get("CAL_END_DATE"), errors="coerce")
    c["CAL_ITERATION_LEVEL3"] = c.get("CAL_ITERATION_LEVEL3", "").fillna("").astype(str).str.strip()
    parts = c["CAL_ITERATION_LEVEL3"].map(_parse_pi_parts)
    c["LABEL_YEAR"] = parts.map(lambda x: x[0] if isinstance(x, tuple) else None)
    c["PI_NUM"] = parts.map(lambda x: x[1] if isinstance(x, tuple) else None)
    c["PI_NUM"] = pd.to_numeric(c.get("PI_NUM"), errors="coerce")
    c["PI_YEAR"] = pd.to_numeric(c.get("LABEL_YEAR"), errors="coerce")
    c["PI_YEAR"] = c["PI_YEAR"].fillna(c["CAL_YEAR"])
    c = c[c["PI_NUM"].notna()].copy()
    if c.empty:
        return out

    c["PI_ORDER"] = (c["PI_YEAR"].astype(int) * 100) + c["PI_NUM"].astype(int)
    asof_ts = pd.Timestamp(as_of)

    non_future = c[c["CAL_START_DATE"].notna() & (c["CAL_START_DATE"] <= asof_ts)].copy()
    if not non_future.empty:
        pick_nf = non_future.sort_values(["PI_ORDER", "CAL_START_DATE"]).tail(1).iloc[0]
        out["non_future_max_pi_order"] = int(pick_nf["PI_ORDER"])

    last_closed = c[c["CAL_END_DATE"].notna() & (c["CAL_END_DATE"] < asof_ts)].copy()
    if not last_closed.empty:
        pick = last_closed.sort_values(["CAL_END_DATE", "PI_ORDER"]).tail(1).iloc[0]
        out["reference_pi_order"] = int(pick["PI_ORDER"])
        label_year = int(pick.get("PI_YEAR") or selected_year)
        out["reference_pi_label"] = str(pick.get("CAL_ITERATION_LEVEL3") or "").strip() or _format_pi_label(
            label_year,
            int(pick["PI_NUM"]),
        )
        out["mode"] = "last_closed"
        return out

    if out.get("non_future_max_pi_order") is not None:
        nf_order = int(out["non_future_max_pi_order"])
        pick = c[c["PI_ORDER"].eq(nf_order)].sort_values(["CAL_START_DATE", "CAL_END_DATE"]).tail(1)
        if not pick.empty:
            row = pick.iloc[0]
            out["reference_pi_order"] = nf_order
            label_year = int(row.get("PI_YEAR") or selected_year)
            out["reference_pi_label"] = str(row.get("CAL_ITERATION_LEVEL3") or "").strip() or _format_pi_label(
                label_year,
                int(row.get("PI_NUM") or 0),
            )
            out["mode"] = "non_future"
    return out


def _pick_source_subset(
    *,
    frame: pd.DataFrame,
    source_chain: list[str],
    strict: bool = False,
) -> tuple[pd.DataFrame, str, str]:
    src = frame.get("BASELINE_SOURCE", pd.Series(dtype=str)).fillna("").astype(str).str.upper().str.strip()
    for idx, want in enumerate(source_chain):
        part = frame[src.eq(want)].copy()
        if not part.empty:
            return part, want, ">".join(s.lower() for s in source_chain[: idx + 1])
    # Final defensive fallback.
    if frame.empty:
        return frame, "GLOBAL", "none"
    if strict:
        return frame.iloc[0:0].copy(), "GLOBAL", "none"
    first_source = "GLOBAL"
    try:
        s = str(src.iloc[0]).upper().strip()
        if s in _SOURCE_ORDER:
            first_source = s
    except Exception:
        pass
    return frame.copy(), first_source, "fallback"


def select_velocity_snapshot(
    *,
    df_velocity: pd.DataFrame,
    selected_year: int,
    as_of: dt.date,
    programs: list[str] | tuple[str, ...] | None,
    teams: list[str] | tuple[str, ...] | None,
    calendar_df: Optional[pd.DataFrame],
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "value": None,
        "prev": None,
        "changed": False,
        "source": "GLOBAL",
        "pi_label": "—",
        "spark_values": [],
        "debug_scoped_rows": 0,
        "debug_scoped_source_counts": {},
        "debug_upto_now_source_counts": {},
        "debug_latest_pi_source_counts": {},
        "reference_pi_label": "—",
        "selected_pi_label": "—",
        "fallback_chain_used": "none",
    }
    if df_velocity is None or df_velocity.empty:
        return out

    w = df_velocity.copy()
    w["YEAR"] = pd.to_numeric(w.get("YEAR"), errors="coerce")
    w["PI"] = pd.to_numeric(w.get("PI"), errors="coerce")
    w["PI_ORDER"] = (w["YEAR"].fillna(0).astype(int) * 100) + w["PI"].fillna(0).astype(int)
    w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    w["TEAMNAME"] = w.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    w["PI_LABEL"] = w.get("PI_LABEL", "").fillna("").astype(str).str.strip()
    w["BASELINE_SOURCE"] = (
        w.get("BASELINE_SOURCE", "GLOBAL").fillna("GLOBAL").astype(str).str.upper().str.strip()
    )
    w["PI_POINTS_DONE"] = pd.to_numeric(w.get("PI_POINTS_DONE"), errors="coerce").fillna(0.0).astype(float)
    w["EFFECTIVE_BASELINE_POINTS"] = pd.to_numeric(w.get("EFFECTIVE_BASELINE_POINTS"), errors="coerce")
    w["BASELINE_PREV"] = pd.to_numeric(w.get("BASELINE_PREV"), errors="coerce")
    w["BASELINE_CHANGED_FLAG"] = pd.to_numeric(w.get("BASELINE_CHANGED_FLAG"), errors="coerce").fillna(0).astype(int)
    w = w[w["YEAR"].notna() & w["PI"].notna()].copy()
    if selected_year:
        selected_year_i = int(selected_year)
        # Keep the selected year plus prior year so PI1 can correctly reference
        # prior-year PI4 (e.g., 2026 PI1 -> 2025 PI4).
        year_floor = max(0, selected_year_i - 1)
        w = w[w["YEAR"].astype(int).between(year_floor, selected_year_i)].copy()
    if w.empty:
        return out

    prog_set = _to_upper_set(list(programs or []))
    team_set = _to_upper_set(list(teams or []))
    src = w["BASELINE_SOURCE"].fillna("GLOBAL").astype(str).str.upper().str.strip()
    prog_mask = w["PROGRAMNAME"].str.upper().isin(prog_set) if prog_set else pd.Series(True, index=w.index)
    team_mask = w["TEAMNAME"].str.upper().isin(team_set) if team_set else pd.Series(True, index=w.index)

    # Scope-aware filtering by source:
    # - TEAM rows honor team scope
    # - PROGRAM rows honor program scope
    # - GLOBAL rows remain eligible as broader fallback
    if team_set:
        # When only team filter is provided, restrict PROGRAM fallback to the
        # selected team's own program(s), not every program in the portfolio.
        if prog_set:
            program_fallback_mask = prog_mask
        else:
            team_programs = {
                str(v).strip().upper()
                for v in w.loc[team_mask, "PROGRAMNAME"].dropna().tolist()
                if str(v).strip()
            }
            if team_programs:
                program_fallback_mask = w["PROGRAMNAME"].str.upper().isin(team_programs)
            else:
                program_fallback_mask = pd.Series(False, index=w.index)
        keep = (src.eq("TEAM") & team_mask) | (src.eq("PROGRAM") & program_fallback_mask) | src.eq("GLOBAL")
        w = w[keep].copy()
    elif prog_set:
        keep = (src.eq("PROGRAM") & prog_mask) | src.eq("GLOBAL")
        w = w[keep].copy()
    if w.empty:
        return out

    out["debug_scoped_rows"] = int(len(w.index))
    out["debug_scoped_source_counts"] = _source_counts(w)

    ref = resolve_reference_pi(calendar_df=calendar_df, selected_year=int(selected_year), as_of=as_of)
    ref_order = ref.get("reference_pi_order")
    non_future_limit = ref.get("non_future_max_pi_order")
    out["reference_pi_label"] = str(ref.get("reference_pi_label") or "—")

    if non_future_limit is not None:
        w_upto = w[w["PI_ORDER"].astype(int) <= int(non_future_limit)].copy()
        if not w_upto.empty:
            w = w_upto
    out["debug_upto_now_source_counts"] = _source_counts(w)
    if w.empty:
        return out

    available_orders = sorted({int(v) for v in w["PI_ORDER"].dropna().astype(int).tolist()})
    if not available_orders:
        return out

    selected_order: Optional[int] = None
    if ref_order is not None and int(ref_order) in available_orders:
        selected_order = int(ref_order)
    else:
        selected_order = int(max(available_orders))

    if team_set:
        source_chain = ["TEAM", "PROGRAM", "GLOBAL"]
    elif prog_set:
        source_chain = ["PROGRAM", "GLOBAL"]
    else:
        source_chain = ["GLOBAL"]

    initial_order = int(selected_order)
    selected_rows = pd.DataFrame()
    latest_rows = pd.DataFrame()
    latest_source = "GLOBAL"
    chain_used = "none"
    candidate_orders = [initial_order] + [int(v) for v in sorted(available_orders, reverse=True) if int(v) < initial_order]
    for order in candidate_orders:
        cand = w[w["PI_ORDER"].astype(int).eq(int(order))].copy()
        if cand.empty:
            continue
        cand_rows, cand_source, cand_chain = _pick_source_subset(
            frame=cand,
            source_chain=source_chain,
            strict=True,
        )
        if cand_rows.empty:
            continue
        selected_order = int(order)
        selected_rows = cand
        latest_rows = cand_rows
        latest_source = cand_source
        chain_used = cand_chain
        break

    if selected_rows.empty or latest_rows.empty:
        return out

    if int(selected_order) != int(initial_order):
        chain_used = f"order_fallback>{chain_used}"

    value = _weighted_avg(
        latest_rows.get("EFFECTIVE_BASELINE_POINTS", pd.Series(dtype=float)),
        latest_rows.get("PI_POINTS_DONE", pd.Series(dtype=float)),
    )
    prev = _weighted_avg(
        latest_rows.get("BASELINE_PREV", pd.Series(dtype=float)),
        latest_rows.get("PI_POINTS_DONE", pd.Series(dtype=float)),
    )
    changed = bool((selected_rows.get("BASELINE_CHANGED_FLAG", pd.Series(dtype=int)) == 1).any())

    pi_label = next(
        (str(v).strip() for v in selected_rows.get("PI_LABEL", pd.Series(dtype=str)).tolist() if str(v).strip()),
        "",
    )
    if not pi_label:
        yr = int(float(selected_order) // 100)
        pi = int(float(selected_order) % 100)
        pi_label = _format_pi_label(yr, pi)

    spark_rows = []
    for order in sorted(available_orders):
        grp = w[w["PI_ORDER"].astype(int).eq(int(order))].copy()
        if grp.empty:
            continue
        grp_use, _src, _chain = _pick_source_subset(frame=grp, source_chain=source_chain, strict=True)
        if grp_use.empty:
            continue
        val = _weighted_avg(
            grp_use.get("EFFECTIVE_BASELINE_POINTS", pd.Series(dtype=float)),
            grp_use.get("PI_POINTS_DONE", pd.Series(dtype=float)),
        )
        if val is not None:
            spark_rows.append(float(val))

    out["value"] = value
    out["prev"] = prev
    out["changed"] = changed
    out["source"] = str(latest_source or "GLOBAL").upper().strip() or "GLOBAL"
    out["pi_label"] = pi_label
    out["selected_pi_label"] = pi_label
    out["spark_values"] = spark_rows[-6:]
    out["debug_latest_pi_source_counts"] = _source_counts(selected_rows)
    if ref_order is not None and int(ref_order) != int(selected_order):
        out["fallback_chain_used"] = f"reference_pi_missing>{chain_used}"
    else:
        out["fallback_chain_used"] = chain_used
    return out


def build_velocity_debug_meta(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "value": snapshot.get("value"),
        "prev": snapshot.get("prev"),
        "changed": bool(snapshot.get("changed")),
        "source": str(snapshot.get("source") or ""),
        "pi_label": str(snapshot.get("pi_label") or ""),
        "reference_pi_label": str(snapshot.get("reference_pi_label") or ""),
        "selected_pi_label": str(snapshot.get("selected_pi_label") or ""),
        "fallback_chain_used": str(snapshot.get("fallback_chain_used") or ""),
        "scoped_rows": int(snapshot.get("debug_scoped_rows") or 0),
        "scoped_source_counts": snapshot.get("debug_scoped_source_counts") or {},
        "upto_now_source_counts": snapshot.get("debug_upto_now_source_counts") or {},
        "latest_pi_source_counts": snapshot.get("debug_latest_pi_source_counts") or {},
    }
