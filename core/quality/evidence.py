from __future__ import annotations

from typing import Any, Callable, Dict, Optional

import pandas as pd

from core.quality.context import QualityContext
from core.quality.policy import normalize_mapping_status


def _safe_cols(df: pd.DataFrame, cols: list[str]) -> list[str]:
    return [c for c in cols if c in df.columns]

def _find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    by_lower = {str(c).lower(): str(c) for c in cols}
    for cand in candidates:
        if cand in cols:
            return cand
        hit = by_lower.get(str(cand).lower())
        if hit:
            return hit
    return None


def _pick_cols(df: pd.DataFrame, candidates: list[str]) -> list[str]:
    out: list[str] = []
    for cand in candidates:
        c = _find_col(df, [cand])
        if c and c not in out:
            out.append(c)
    return out


def _limit(df: pd.DataFrame, n: int = 200) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    return df.head(int(n)).copy()


def _resolve_unassigned_cost_lines(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    df = ctx.cost_lines
    if df is None or df.empty:
        return pd.DataFrame()
    scen = str((params or {}).get("scenario") or "").strip().upper()
    out = df.copy()
    if scen and "SCENARIO" in out.columns:
        out = out[out["SCENARIO"].fillna("").astype(str).str.upper().str.strip().eq(scen)].copy()
    if "GROUPNAME" in out.columns:
        g = out["GROUPNAME"].fillna("").astype(str).str.strip().str.upper()
        out = out[g.isin({"", "(UNASSIGNED)", "UNASSIGNED", "(UNMAPPED APP GROUP)", "UNMAPPED APP GROUP"})].copy()
    out["MAPPING_STATUS_DISPLAY"] = out.apply(normalize_mapping_status, axis=1)
    # Bruno requirement: NOT_IN_SCOPE lines must not appear in evidence tables at all.
    out = out[out["MAPPING_STATUS_DISPLAY"].astype(str).str.upper().ne("NOT_IN_SCOPE")].copy()
    cols = _safe_cols(out, ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "SOURCE", "AMOUNT", "MAPPING_STATUS"])
    if "MAPPING_STATUS_DISPLAY" in out.columns:
        cols.append("MAPPING_STATUS_DISPLAY")
    if "AMOUNT" in out.columns:
        out["AMOUNT"] = pd.to_numeric(out["AMOUNT"], errors="coerce").fillna(0.0)
        out = out.sort_values("AMOUNT", ascending=False)
    return _limit(out[cols], 200)


def _resolve_unassigned_cost_offenders(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    """Return top unassigned cost offenders (actionable), excluding NOT_IN_SCOPE rows."""
    df = ctx.cost_lines
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame([{"MESSAGE": "No cost lines available in this scope."}])

    out = df.copy()

    # Apply scope filters from params (best-effort; column-flexible).
    year_val = params.get("year")
    year_col = _find_col(out, ["YEAR", "FISCAL_YEAR", "ADO_YEAR"])
    if year_col and year_val is not None:
        out = out[pd.to_numeric(out[year_col], errors="coerce").fillna(-1).astype(int).eq(int(year_val))].copy()

    scen = str(params.get("scenario") or "ALL").strip().upper()
    scen_col = _find_col(out, ["SCENARIO"])
    if scen_col and scen != "ALL":
        out = out[out[scen_col].fillna("").astype(str).str.upper().str.strip().eq(scen)].copy()

    def _apply_in(col_candidates: list[str], values: list[str]) -> None:
        nonlocal out
        if not values:
            return
        col = _find_col(out, col_candidates)
        if not col:
            return
        want = {str(v).strip().upper() for v in values if str(v).strip()}
        if not want:
            return
        out = out[out[col].fillna("").astype(str).str.strip().str.upper().isin(want)].copy()

    _apply_in(["PROGRAMNAME", "PROGRAM", "PROGRAM_NAME"], list(params.get("programs") or []))
    _apply_in(["TEAMNAME", "TEAM", "TEAM_NAME"], list(params.get("teams") or []))
    _apply_in(["GROUPNAME", "APP_GROUP", "APP_GROUP_NAME"], list(params.get("app_groups") or []))

    amount_col = _find_col(out, ["AMOUNT", "COST_USD", "COST", "COST_AMOUNT", "AMOUNT_USD"])
    if amount_col:
        out[amount_col] = pd.to_numeric(out[amount_col], errors="coerce").fillna(0.0)

    group_col = _find_col(out, ["GROUPNAME", "APP_GROUP", "APP_GROUP_NAME"])
    status_col = _find_col(out, ["MAPPING_STATUS", "MAPPING", "STATUS", "MAP_STATUS"])

    # Robust unassigned/unmapped app group condition.
    unassigned_vals = {"", "(UNASSIGNED)", "UNASSIGNED", "(UNMAPPED APP GROUP)", "UNMAPPED APP GROUP"}
    mask = pd.Series([False] * len(out), index=out.index)
    if group_col:
        g = out[group_col].fillna("").astype(str).str.strip().str.upper()
        mask = mask | g.isin(unassigned_vals)
    if status_col:
        s = out[status_col].fillna("").astype(str).str.strip().str.upper()
        mask = mask | s.isin({"UNMAPPED_APP_GROUP", "UNMAPPED", "UNASSIGNED", "UNKNOWN"})

    out = out[mask].copy()
    if out.empty:
        return pd.DataFrame([{"MESSAGE": "No actionable unassigned rows after NOT_IN_SCOPE filtering."}])

    # Exclude NOT_IN_SCOPE rows (only evaluate policy on the candidate subset).
    out["MAPPING_STATUS_DISPLAY"] = out.apply(normalize_mapping_status, axis=1)
    out = out[out["MAPPING_STATUS_DISPLAY"].astype(str).str.upper().ne("NOT_IN_SCOPE")].copy()
    if out.empty:
        return pd.DataFrame([{"MESSAGE": "No actionable unassigned rows after NOT_IN_SCOPE filtering."}])

    # Build offenders group-by using best available columns.
    group_cols: list[str] = []
    group_cols += _pick_cols(out, ["YEAR", "FISCAL_YEAR", "ADO_YEAR"])
    group_cols += _pick_cols(out, ["PI", "PI_NAME", "MONTH_KEY"])
    group_cols += _pick_cols(out, ["PROGRAMNAME", "TEAMNAME", "SOURCE"])
    group_cols += _pick_cols(out, ["COST_BUCKET", "COST_CATEGORY", "SUBCOMPONENT"])
    # Always keep a status display to make it clear this is actionable.
    if "MAPPING_STATUS_DISPLAY" not in group_cols:
        group_cols.append("MAPPING_STATUS_DISPLAY")

    if not amount_col:
        cols = _safe_cols(out, group_cols + _pick_cols(out, ["AMOUNT", "SOURCE", "SCENARIO", "MAPPING_STATUS"]))
        return _limit(out[cols], int(params.get("top_n") or 200))

    g = (
        out.groupby(group_cols, dropna=False)[amount_col]
        .agg(amount_usd="sum", count_lines="count")
        .reset_index()
        .sort_values("amount_usd", ascending=False)
    )
    top_n = int(params.get("top_n") or 25)
    g = g.head(top_n).copy()
    if g.empty:
        cols = _safe_cols(out, group_cols + [amount_col])
        return _limit(out[cols], 200)
    g["amount_usd"] = pd.to_numeric(g.get("amount_usd"), errors="coerce").fillna(0.0)
    g["count_lines"] = pd.to_numeric(g.get("count_lines"), errors="coerce").fillna(0).astype(int)
    return g


def _resolve_missing_team_rates(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    # Show TEAMS metadata for missing rate teams (evidence is a small table).
    team_ids = (params or {}).get("team_ids") or []
    if not team_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(team_ids))
    sql = f"SELECT TEAMID, TEAMNAME, PROGRAMID FROM TEAMS WHERE TEAMID IN ({placeholders})"
    try:
        if callable(ctx.db):
            df = ctx.db(sql, tuple(team_ids))
        else:
            df = ctx.db.fetch_df(sql, tuple(team_ids))  # type: ignore[attr-defined]
    except Exception:
        df = None
    out = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if out.empty:
        return pd.DataFrame()
    out["TEAMNAME"] = out.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    return _limit(out, 200)


def _resolve_teams_missing_program(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    team_ids = (params or {}).get("team_ids") or []
    if not team_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(team_ids))
    sql = f"SELECT TEAMID, TEAMNAME, PROGRAMID FROM TEAMS WHERE TEAMID IN ({placeholders})"
    try:
        if callable(ctx.db):
            df = ctx.db(sql, tuple(team_ids))
        else:
            df = ctx.db.fetch_df(sql, tuple(team_ids))  # type: ignore[attr-defined]
    except Exception:
        df = None
    out = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if out.empty:
        return pd.DataFrame()
    out["TEAMNAME"] = out.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    out["PROGRAMID"] = out.get("PROGRAMID", "").fillna("").astype(str).str.strip()
    return _limit(out, 200)


def _resolve_app_groups_missing_owner(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    group_ids = (params or {}).get("group_ids") or []
    if not group_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(group_ids))
    sql = f"SELECT GROUPID, GROUPNAME, TEAMID FROM APPLICATION_GROUPS WHERE GROUPID IN ({placeholders})"
    try:
        if callable(ctx.db):
            df = ctx.db(sql, tuple(group_ids))
        else:
            df = ctx.db.fetch_df(sql, tuple(group_ids))  # type: ignore[attr-defined]
    except Exception:
        df = None
    out = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if out.empty:
        return pd.DataFrame()
    out["GROUPNAME"] = out.get("GROUPNAME", "").fillna("").astype(str).str.strip()
    out["TEAMID"] = out.get("TEAMID", "").fillna("").astype(str).str.strip()
    return _limit(out, 200)


def _resolve_app_groups_missing_msp(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    group_ids = (params or {}).get("group_ids") or []
    if not group_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(group_ids))
    sql = f"""
        SELECT g.GROUPID, g.GROUPNAME, p.PROGRAMNAME, t.TEAMNAME
        FROM APPLICATION_GROUPS g
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = g.PROGRAMID
        LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
        WHERE g.GROUPID IN ({placeholders})
    """
    try:
        if callable(ctx.db):
            df = ctx.db(sql, tuple(group_ids))
        else:
            df = ctx.db.fetch_df(sql, tuple(group_ids))  # type: ignore[attr-defined]
    except Exception:
        df = None
    out = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if out.empty:
        return pd.DataFrame()
    return _limit(out, 200)


def _resolve_app_groups_missing_ado_mapping(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    group_ids = (params or {}).get("group_ids") or []
    if not group_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(group_ids))
    sql = f"""
        SELECT g.GROUPID, g.GROUPNAME, p.PROGRAMNAME, t.TEAMNAME
        FROM APPLICATION_GROUPS g
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = g.PROGRAMID
        LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
        WHERE g.GROUPID IN ({placeholders})
    """
    try:
        if callable(ctx.db):
            df = ctx.db(sql, tuple(group_ids))
        else:
            df = ctx.db.fetch_df(sql, tuple(group_ids))  # type: ignore[attr-defined]
    except Exception:
        df = None
    out = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if out.empty:
        return pd.DataFrame()
    return _limit(out, 200)


def _resolve_app_groups_multi_instance(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    group_ids = (params or {}).get("group_ids") or []
    if not group_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(group_ids))
    sql = f"""
        SELECT a.GROUPID, g.GROUPNAME, p.PROGRAMNAME, COUNT(*) AS INSTANCE_COUNT
        FROM APPLICATIONS a
        JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = g.PROGRAMID
        WHERE a.GROUPID IN ({placeholders})
        GROUP BY a.GROUPID, g.GROUPNAME, p.PROGRAMNAME
    """
    try:
        if callable(ctx.db):
            df = ctx.db(sql, tuple(group_ids))
        else:
            df = ctx.db.fetch_df(sql, tuple(group_ids))  # type: ignore[attr-defined]
    except Exception:
        df = None
    out = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if out.empty:
        return pd.DataFrame()
    return _limit(out, 200)


def _resolve_ado_unmapped_app(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    df = ctx.ado_features
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    g = out.get("GROUPNAME", "").fillna("").astype(str)
    s = out.get("MAPPING_STATUS", "").fillna("").astype(str).str.upper()
    mask = g.str.upper().isin({"(UNMAPPED APP GROUP)", "(UNASSIGNED)", "UNASSIGNED"}) | s.str.contains("UNMAPPED", na=False)
    out = out[mask].copy()
    cols = _safe_cols(out, ["FEATURE_ID", "TITLE", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "PI_LABEL", "DERIVED_FTE", "MAPPING_STATUS"])
    if "DERIVED_FTE" in out.columns:
        out["DERIVED_FTE"] = pd.to_numeric(out["DERIVED_FTE"], errors="coerce").fillna(0.0)
        out = out.sort_values("DERIVED_FTE", ascending=False)
    return _limit(out[cols], int((params or {}).get("limit") or 200))


def _resolve_ado_unmapped_team(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    df = ctx.ado_features
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    t = out.get("TEAMNAME", "").fillna("").astype(str)
    s = out.get("MAPPING_STATUS", "").fillna("").astype(str).str.upper()
    mask = t.str.upper().isin({"(UNASSIGNED)", "UNASSIGNED"}) | (s.str.contains("UNMAPPED", na=False) & s.str.contains("TEAM", na=False))
    out = out[mask].copy()
    cols = _safe_cols(out, ["FEATURE_ID", "TITLE", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "PI_LABEL", "DERIVED_FTE", "MAPPING_STATUS"])
    if "DERIVED_FTE" in out.columns:
        out["DERIVED_FTE"] = pd.to_numeric(out["DERIVED_FTE"], errors="coerce").fillna(0.0)
        out = out.sort_values("DERIVED_FTE", ascending=False)
    return _limit(out[cols], int((params or {}).get("limit") or 200))


def _resolve_ado_missing_effort(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    df = ctx.ado_features
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    d = pd.to_numeric(out.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
    msp = pd.to_numeric(out.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int) > 0
    out = out[(~msp) & (d <= 0.0)].copy()
    cols = _safe_cols(out, ["FEATURE_ID", "TITLE", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "PI_LABEL", "DERIVED_FTE", "MAPPING_STATUS"])
    return _limit(out[cols], int((params or {}).get("limit") or 200))


def _resolve_ado_missing_swag(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    df = ctx.ado_features
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    swag_col = _find_col(out, ["STORY_POINTS", "EFFORT_POINTS", "SWAG", "SWAG_POINTS"])
    if not swag_col:
        return pd.DataFrame()
    swag = pd.to_numeric(out[swag_col], errors="coerce")
    msp = pd.to_numeric(out.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int) > 0
    out = out[(~msp) & (swag.isna() | (swag <= 0))].copy()
    cols = _safe_cols(out, ["FEATURE_ID", "TITLE", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "PI_LABEL", swag_col])
    out = out[cols].copy()
    if "FEATURE_ID" in out.columns and "ADO_FEATURE_ID" not in out.columns:
        out["ADO_FEATURE_ID"] = out["FEATURE_ID"]
    if "TITLE" in out.columns and "ADO_FEATURE_TITLE" not in out.columns:
        out["ADO_FEATURE_TITLE"] = out["TITLE"]
    if "PROGRAMNAME" in out.columns and "ADO_PROGRAM" not in out.columns:
        out["ADO_PROGRAM"] = out["PROGRAMNAME"]
    if "TEAMNAME" in out.columns and "ADO_TEAM" not in out.columns:
        out["ADO_TEAM"] = out["TEAMNAME"]
    return _limit(out, int((params or {}).get("limit") or 200))


def _resolve_ado_missing_app_name(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    df = ctx.ado_features
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    app_col = _find_col(out, ["APP_NAME_RAW", "APP_NAME", "ADO_APP_NAME"])
    if not app_col:
        return pd.DataFrame()
    app_vals = out[app_col].fillna("").astype(str).str.strip()
    msp = pd.to_numeric(out.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int) > 0
    out = out[(~msp) & app_vals.eq("")].copy()
    cols = _safe_cols(out, ["FEATURE_ID", "TITLE", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "PI_LABEL", app_col])
    out = out[cols].copy()
    if "FEATURE_ID" in out.columns and "ADO_FEATURE_ID" not in out.columns:
        out["ADO_FEATURE_ID"] = out["FEATURE_ID"]
    if "TITLE" in out.columns and "ADO_FEATURE_TITLE" not in out.columns:
        out["ADO_FEATURE_TITLE"] = out["TITLE"]
    if "PROGRAMNAME" in out.columns and "ADO_PROGRAM" not in out.columns:
        out["ADO_PROGRAM"] = out["PROGRAMNAME"]
    if "TEAMNAME" in out.columns and "ADO_TEAM" not in out.columns:
        out["ADO_TEAM"] = out["TEAMNAME"]
    return _limit(out, int((params or {}).get("limit") or 200))


def _resolve_apptio_unmapped_actuals(ctx: QualityContext, params: Dict[str, Any]) -> pd.DataFrame:
    df = ctx.apptio_actuals_breakdown
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    st = out.get("MAPPING_STATUS", "").fillna("").astype(str).str.upper().str.strip()
    bad = st.isin({"DEFAULT_OTHER", "INVALID_TARGET"}) | st.eq("")
    out = out[bad].copy()
    amt_col = "AMOUNT"
    if amt_col in out.columns:
        out[amt_col] = pd.to_numeric(out[amt_col], errors="coerce").fillna(0.0)
        out = out.sort_values(amt_col, ascending=False)
    cols = _safe_cols(out, ["PROGRAMNAME", "MONTH_KEY", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT", "LEDGER_ACCOUNT_L3_DESC", "PRODUCT_ID", "PRODUCT_NAME"])
    return _limit(out[cols], int((params or {}).get("limit") or 200))


_RESOLVERS: dict[str, Callable[[QualityContext, Dict[str, Any]], pd.DataFrame]] = {
    "unassigned_cost_lines": _resolve_unassigned_cost_lines,
    "unassigned_cost_offenders": _resolve_unassigned_cost_offenders,
    "missing_team_rates": _resolve_missing_team_rates,
    "teams_missing_program": _resolve_teams_missing_program,
    "app_groups_missing_owner": _resolve_app_groups_missing_owner,
    "app_groups_missing_msp": _resolve_app_groups_missing_msp,
    "app_groups_missing_ado_mapping": _resolve_app_groups_missing_ado_mapping,
    "app_groups_multi_instance": _resolve_app_groups_multi_instance,
    "ado_unmapped_app": _resolve_ado_unmapped_app,
    "ado_unmapped_team": _resolve_ado_unmapped_team,
    "ado_missing_effort": _resolve_ado_missing_effort,
    "ado_missing_swag": _resolve_ado_missing_swag,
    "ado_missing_app_name": _resolve_ado_missing_app_name,
    "apptio_unmapped_actuals": _resolve_apptio_unmapped_actuals,
    "config_missing_wf_rates": lambda ctx, params: _limit(pd.DataFrame(params.get("rows") or []), 200),
    "config_no_ado_mapping": lambda ctx, params: _limit(pd.DataFrame(params.get("rows") or []), 200),
    "config_app_missing_owner": lambda ctx, params: _limit(pd.DataFrame(params.get("rows") or []), 200),
    "config_program_has_no_teams": lambda ctx, params: _limit(pd.DataFrame(params.get("rows") or []), 200),
    "config_ado_unmapped_teams": lambda ctx, params: _limit(pd.DataFrame(params.get("rows") or []), 200),
    "ado_unmapped_teams_in_program": lambda ctx, params: _limit(pd.DataFrame(params.get("rows") or []), 200),
}


def resolve_evidence(evidence_ref: Optional[Dict[str, Any]], ctx: QualityContext) -> pd.DataFrame:
    if not evidence_ref or not isinstance(evidence_ref, dict):
        return pd.DataFrame()
    fn = str(evidence_ref.get("fn") or "").strip()
    params = evidence_ref.get("params") or {}
    if not fn or fn not in _RESOLVERS:
        return pd.DataFrame()
    try:
        return _RESOLVERS[fn](ctx, dict(params))
    except Exception:
        return pd.DataFrame()
