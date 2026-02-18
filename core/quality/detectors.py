from __future__ import annotations

import hashlib
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from core.quality.context import QualityContext
from core.quality.impact import estimate_usd_from_fte
from core.quality.issues import (
    ImpactBasis,
    Issue,
    IssueStatus,
    IssueType,
    SEVERITY_ORDER,
    Severity,
)
try:
    from core.quality.policy import is_app_group_mapping_required
except Exception:  # pragma: no cover
    def is_app_group_mapping_required(_: Any) -> bool:
        return True


USD_BLOCKER = 250_000.0
USD_WARNING = 50_000.0


def _fetch_df(db: Any, sql: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
    try:
        if callable(db):
            out = db(sql, params)
        else:
            out = db.fetch_df(sql, params)  # type: ignore[attr-defined]
        return out if isinstance(out, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _norm_str(x: Any) -> str:
    return str(x or "").strip()


def _upper(x: Any) -> str:
    return _norm_str(x).upper()


def _is_blank(x: Any) -> bool:
    return _norm_str(x) == ""


def _is_unassigned_group(groupname: Any) -> bool:
    g = _upper(groupname)
    return g in {"", "(UNASSIGNED)", "UNASSIGNED", "(UNMAPPED)", "(UNMAPPED APP GROUP)", "UNMAPPED APP GROUP"}


def _severity_for_usd(impact_usd_est: float) -> str:
    v = float(impact_usd_est or 0.0)
    if v >= USD_BLOCKER:
        return Severity.BLOCKER
    if v >= USD_WARNING:
        return Severity.WARNING
    return Severity.INFO


def _stable_id(prefix: str, dedupe_key: str) -> str:
    h = hashlib.sha1(str(dedupe_key or "").encode("utf-8")).hexdigest()[:10]
    return f"{prefix}_{h}"


def _make_issue(
    *,
    issue_type: str,
    title: str,
    severity: str,
    owner_role: Optional[str] = None,
    scenario: str = "",
    source: str = "",
    scope: Optional[Dict[str, Any]] = None,
    impact_usd_est: float = 0.0,
    impact_fte: float = 0.0,
    impact_features: int = 0,
    impact_basis: str = ImpactBasis.MIXED,
    impact_basis_detail: str = "",
    confidence: str = "MEDIUM",
    summary: str = "",
    evidence_ref: Optional[Dict[str, Any]] = None,
    fix: Optional[Dict[str, Any]] = None,
    tags: Optional[List[str]] = None,
    dedupe_key: Optional[str] = None,
) -> Issue:
    dk = dedupe_key or f"{issue_type}:{title}:{scenario}:{source}:{sorted((scope or {}).items())}"
    issue_id = _stable_id(issue_type, dk)
    return Issue(
        issue_id=issue_id,
        issue_type=issue_type,
        title=title,
        severity=severity,
        status=IssueStatus.OPEN,
        owner_role=owner_role,
        scenario=scenario,
        source=source,
        scope=dict(scope or {}),
        impact_usd_est=float(impact_usd_est or 0.0),
        impact_fte=float(impact_fte or 0.0),
        impact_features=int(impact_features or 0),
        impact_basis=impact_basis,
        impact_basis_detail=str(impact_basis_detail or "").strip(),
        confidence=str(confidence or "").strip() or "MEDIUM",
        summary=str(summary or "").strip(),
        evidence_ref=evidence_ref,
        fix=dict(fix or {}),
        tags=list(tags or []),
        dedupe_key=dk,
    )


def dedupe_issues(issues: Iterable[Issue]) -> List[Issue]:
    seen: set[str] = set()
    out: List[Issue] = []
    for issue in issues:
        key = str(issue.dedupe_key or issue.issue_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(issue)
    return out


def sort_issues(issues: Iterable[Issue]) -> List[Issue]:
    def _key(i: Issue) -> Tuple[int, float, float, int, str]:
        sev = SEVERITY_ORDER.get(str(i.severity), 0)
        return (sev, float(i.impact_usd_est or 0.0), float(i.impact_fte or 0.0), int(i.impact_features or 0), str(i.title))

    return sorted(list(issues), key=_key, reverse=True)


def detect_unassigned_cost(ctx: QualityContext) -> List[Issue]:
    df = ctx.cost_lines
    if df is None or df.empty:
        return []
    required = {"AMOUNT", "GROUPNAME"}
    if not required.issubset(set(df.columns)):
        return []

    scen = df.get("SCENARIO", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str).str.upper().str.strip()
    group = df["GROUPNAME"].fillna("").astype(str)
    amt = pd.to_numeric(df["AMOUNT"], errors="coerce").fillna(0.0)

    mask_unassigned = group.apply(_is_unassigned_group)
    # Exclude lines that are not in scope for app-group mapping (e.g., program additional costs).
    try:
        mapping_required = df.apply(is_app_group_mapping_required, axis=1)
    except Exception:
        mapping_required = pd.Series([True] * len(df), index=df.index)
    df_u = df.loc[mask_unassigned & mapping_required].copy()
    if df_u.empty:
        return []

    df_u["_AMOUNT"] = amt.loc[df_u.index]
    df_u["_SCEN"] = scen.loc[df_u.index]
    # Prefer Expected impact when available; otherwise fall back to all.
    impact_expected = float(df_u.loc[df_u["_SCEN"].eq("EXPECTED"), "_AMOUNT"].sum() or 0.0)
    impact_all = float(df_u["_AMOUNT"].sum() or 0.0)
    impact = impact_expected if impact_expected > 0 else impact_all
    if impact <= 0:
        return []

    top_sources: List[str] = []
    if "SOURCE" in df_u.columns:
        src = df_u["SOURCE"].fillna("").astype(str).str.strip()
        top_sources = (
            df_u.assign(_SRC=src)
            .groupby("_SRC", dropna=False)["_AMOUNT"]
            .sum()
            .sort_values(ascending=False)
            .head(3)
            .index.astype(str)
            .tolist()
        )

    severity = _severity_for_usd(impact)
    title = "Unassigned cost (not mapped to an application group)"
    summary = "Cost lines have no application group mapping; treat these as not-mapped/out-of-scope, not a real group."
    if top_sources:
        summary += f" Top sources: {', '.join([s for s in top_sources if s.strip()])}."

    return [
        _make_issue(
            issue_type=IssueType.UNASSIGNED_COST,
            title=title,
            severity=severity,
            scenario="ALL",
            source="TCO",
            scope={"year": ctx.year, "programs": list(ctx.programs), "teams": list(ctx.teams), "app_groups": list(ctx.app_groups)},
            impact_usd_est=impact,
            impact_basis=ImpactBasis.USD,
            impact_basis_detail="Est. using sum of unassigned cost lines (USD).",
            confidence="HIGH",
            summary=summary,
            evidence_ref={
                "fn": "unassigned_cost_offenders",
                "params": {
                    "year": int(ctx.year),
                    "scenario": "ALL",
                    "programs": list(ctx.programs),
                    "teams": list(ctx.teams),
                    "app_groups": list(ctx.app_groups),
                    "top_n": 25,
                },
            },
            fix={
                "page": "pages/6_Applications.py",
                "action_label": "Fix app group mappings",
                "help": "Assign the work/cost to an application group (or correct team/program mappings).",
            },
            tags=["mapping", "allocation"],
            dedupe_key=f"UNASSIGNED_COST|YEAR={int(ctx.year)}|SCEN=ALL|PROG={tuple(ctx.programs)}|TEAM={tuple(ctx.teams)}|GRP={tuple(ctx.app_groups)}",
        )
    ]


def detect_missing_rates_for_wf_cost(ctx: QualityContext) -> List[Issue]:
    # IMPORTANT: Do not reference PROGRAM_XOM_RATE here. Team rates are the intended source.
    df = ctx.cost_lines
    if df is None or df.empty:
        return []

    if "TEAMNAME" not in df.columns:
        return []
    scen = df.get("SCENARIO", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str).str.upper().str.strip()
    cat = df.get("COST_CATEGORY", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str).str.upper().str.strip()

    # Use Expected+Baseline workforce activity to determine relevant teams.
    wf_mask = cat.isin({"WORK_FORCE", "WF", "WORKFORCE"}) & scen.isin({"BASELINE", "EXPECTED"})
    teams_from_cost = (
        df.loc[wf_mask, "TEAMNAME"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    teams = list(ctx.teams) if ctx.teams else [t for t in teams_from_cost if t]
    if not teams:
        return []

    placeholders = ", ".join(["%s"] * len(teams))
    df_teams = _fetch_df(
        ctx.db,
        f"""
        SELECT
            TEAMID,
            COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME) AS TEAMNAME,
            PROGRAMID
        FROM TEAMS
        WHERE UPPER(LTRIM(RTRIM(COALESCE(NULLIF(TEAM_DISPLAY_NAME, ''), TEAMNAME)))) IN ({placeholders})
        """,
        tuple([t.upper() for t in teams]),
    )
    if df_teams.empty or "TEAMID" not in df_teams.columns:
        return []

    df_teams["TEAMID"] = df_teams["TEAMID"].fillna("").astype(str).str.strip()
    df_teams["TEAMNAME"] = df_teams.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    team_ids = [t for t in df_teams["TEAMID"].tolist() if str(t).strip()]
    if not team_ids:
        return []

    placeholders = ", ".join(["%s"] * len(team_ids))
    df_rates = _fetch_df(
        ctx.db,
        f"""
        SELECT
          TEAMID,
          MAX(CASE WHEN COALESCE(TRY_CONVERT(FLOAT, XOM_RATE), 0) > 0 THEN 1 ELSE 0 END) AS HAS_RATE
        FROM TEAM_RATE_HISTORY
        WHERE YEAR = %s AND TEAMID IN ({placeholders})
        GROUP BY TEAMID
        """,
        tuple([int(ctx.year)] + team_ids),
    )
    has_rate: set[str] = set()
    if not df_rates.empty and "TEAMID" in df_rates.columns:
        has_rate = set(
            df_rates.loc[pd.to_numeric(df_rates.get("HAS_RATE"), errors="coerce").fillna(0).astype(int) > 0, "TEAMID"]
            .fillna("")
            .astype(str)
            .tolist()
        )

    missing = df_teams.loc[~df_teams["TEAMID"].isin(has_rate), ["TEAMID", "TEAMNAME"]].copy()
    if missing.empty:
        return []

    # Impact: use Derived FTE from ADO where available, otherwise count teams.
    impact_fte = 0.0
    if ctx.ado_features is not None and not ctx.ado_features.empty and "TEAMNAME" in ctx.ado_features.columns:
        ado = ctx.ado_features.copy()
        ado_team = ado["TEAMNAME"].fillna("").astype(str).str.strip()
        missing_names_u = set(missing["TEAMNAME"].fillna("").astype(str).str.strip().str.upper().tolist())
        mask = ado_team.str.upper().isin(missing_names_u)
        if mask.any() and "DERIVED_FTE" in ado.columns:
            impact_fte = float(pd.to_numeric(ado.loc[mask, "DERIVED_FTE"], errors="coerce").fillna(0.0).sum() or 0.0)

    title = "Missing workforce rates (team rates not found)"
    summary = f"{len(missing):,d} team(s) have no team rate history for {int(ctx.year)}."
    if impact_fte > 0:
        summary += f" Derived FTE affected: {impact_fte:,.2f}."

    sev = Severity.WARNING
    usd_per_fte, basis, conf = estimate_usd_from_fte(ctx, year=ctx.year, program=(ctx.programs[0] if len(ctx.programs) == 1 else None), team=(ctx.teams[0] if len(ctx.teams) == 1 else None))
    impact_usd_est = float(impact_fte * usd_per_fte) if usd_per_fte > 0 and impact_fte > 0 else 0.0
    return [
        _make_issue(
            issue_type=IssueType.MISSING_WF_RATES,
            title=title,
            severity=sev,
            scenario="BASELINE/EXPECTED",
            source="RATES",
            scope={"year": ctx.year, "teams": missing["TEAMNAME"].astype(str).tolist()},
            impact_usd_est=impact_usd_est,
            impact_fte=impact_fte,
            impact_basis=ImpactBasis.FTE if impact_fte > 0 else ImpactBasis.MIXED,
            impact_basis_detail=basis if impact_usd_est > 0 else "No $/FTE rate available; USD impact omitted.",
            confidence=conf if impact_usd_est > 0 else "LOW",
            summary=summary,
            evidence_ref={"fn": "missing_team_rates", "params": {"team_ids": missing["TEAMID"].astype(str).tolist()}},
            fix={"page": "pages/7_Rates.py", "action_label": "Add/update team rates", "help": "Ensure each team has a valid rate for the selected year."},
            tags=["rates", "workforce"],
            dedupe_key=f"missing_team_rates:{ctx.year}:{sorted(missing['TEAMID'].astype(str).tolist())}",
        )
    ]


def detect_team_missing_program(ctx: QualityContext) -> List[Issue]:
    df_teams = _fetch_df(ctx.db, "SELECT TEAMID, TEAMNAME, PROGRAMID FROM TEAMS", None)
    if df_teams.empty:
        return []
    df_teams["TEAMNAME"] = df_teams.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    df_teams["PROGRAMID"] = df_teams.get("PROGRAMID", "").fillna("").astype(str).str.strip()
    missing = df_teams.loc[df_teams["PROGRAMID"].eq(""), ["TEAMID", "TEAMNAME"]].copy()
    if ctx.teams:
        sel_u = {t.upper() for t in ctx.teams if t.strip()}
        missing = missing[missing["TEAMNAME"].str.upper().isin(sel_u)].copy()
    if missing.empty:
        return []

    title = "Teams missing program assignment"
    summary = f"{len(missing):,d} team(s) have no program mapping."
    return [
        _make_issue(
            issue_type=IssueType.TEAM_MISSING_PROGRAM,
            title=title,
            severity=Severity.WARNING,
            scenario="",
            source="MASTER_DATA",
            scope={"year": ctx.year, "teams": missing["TEAMNAME"].astype(str).tolist()},
            impact_basis=ImpactBasis.MIXED,
            confidence="MEDIUM",
            summary=summary,
            evidence_ref={"fn": "teams_missing_program", "params": {"team_ids": missing["TEAMID"].astype(str).tolist()}},
            fix={"page": "pages/4_Teams.py", "action_label": "Assign programs to teams", "help": "Map each team to a program so costs and demand roll up correctly."},
            tags=["master-data", "mapping"],
            dedupe_key=f"teams_missing_program:{ctx.year}:{sorted(missing['TEAMID'].astype(str).tolist())}",
        )
    ]


def detect_app_group_missing_owner_team(ctx: QualityContext) -> List[Issue]:
    df_groups = _fetch_df(ctx.db, "SELECT GROUPID, GROUPNAME, TEAMID FROM APPLICATION_GROUPS", None)
    if df_groups.empty or "GROUPNAME" not in df_groups.columns:
        return []
    df_groups["GROUPNAME"] = df_groups["GROUPNAME"].fillna("").astype(str).str.strip()
    df_groups["TEAMID"] = df_groups.get("TEAMID", "").fillna("").astype(str).str.strip()
    missing = df_groups.loc[df_groups["TEAMID"].eq("") & df_groups["GROUPNAME"].ne(""), ["GROUPID", "GROUPNAME"]].copy()
    if ctx.app_groups:
        sel_u = {g.upper() for g in ctx.app_groups if g.strip()}
        missing = missing[missing["GROUPNAME"].str.upper().isin(sel_u)].copy()
    if missing.empty:
        return []

    title = "Application groups missing owning team"
    summary = f"{len(missing):,d} application group(s) have no owning team."
    return [
        _make_issue(
            issue_type=IssueType.APP_GROUP_MISSING_OWNER_TEAM,
            title=title,
            severity=Severity.WARNING,
            source="MASTER_DATA",
            scope={"year": ctx.year, "app_groups": missing["GROUPNAME"].astype(str).tolist()},
            impact_basis=ImpactBasis.MIXED,
            confidence="MEDIUM",
            summary=summary,
            evidence_ref={"fn": "app_groups_missing_owner", "params": {"group_ids": missing["GROUPID"].astype(str).tolist()}},
            fix={"page": "pages/6_Applications.py", "action_label": "Assign owning team", "help": "Set an owning team for each application group."},
            tags=["master-data", "ownership"],
            dedupe_key=f"app_groups_missing_owner:{ctx.year}:{sorted(missing['GROUPID'].astype(str).tolist())}",
        )
    ]


def _load_app_groups(ctx: QualityContext) -> pd.DataFrame:
    df = _fetch_df(
        ctx.db,
        """
        SELECT g.GROUPID, g.GROUPNAME, g.TEAMID, g.PROGRAMID, p.PROGRAMNAME, t.TEAMNAME
        FROM APPLICATION_GROUPS g
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = g.PROGRAMID
        LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
        """,
        None,
    )
    if df.empty:
        return pd.DataFrame()
    df["GROUPNAME"] = df.get("GROUPNAME", "").fillna("").astype(str).str.strip()
    df["PROGRAMNAME"] = df.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    df["TEAMNAME"] = df.get("TEAMNAME", "").fillna("").astype(str).str.strip()
    if ctx.programs:
        sel = {p.upper() for p in ctx.programs if p.strip()}
        df = df[df["PROGRAMNAME"].str.upper().isin(sel)].copy()
    if ctx.teams:
        sel = {t.upper() for t in ctx.teams if t.strip()}
        df = df[df["TEAMNAME"].str.upper().isin(sel)].copy()
    if ctx.app_groups:
        sel = {g.upper() for g in ctx.app_groups if g.strip()}
        df = df[df["GROUPNAME"].str.upper().isin(sel)].copy()
    return df


def _mapping_row_count(ctx: QualityContext) -> int:
    sql = """
        SELECT COUNT(*) AS CNT
        FROM MAP_ADO_APP_TO_TCO_GROUP m
        LEFT JOIN APPLICATION_GROUPS g
          ON UPPER(LTRIM(RTRIM(m.APP_GROUP))) = UPPER(LTRIM(RTRIM(g.GROUPNAME)))
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = g.PROGRAMID
        LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
    """
    clauses: List[str] = []
    params: List[Any] = []
    if ctx.programs:
        placeholders = ", ".join(["%s"] * len(ctx.programs))
        clauses.append(f"UPPER(LTRIM(RTRIM(p.PROGRAMNAME))) IN ({placeholders})")
        params.extend([str(p).strip().upper() for p in ctx.programs])
    if ctx.teams:
        placeholders = ", ".join(["%s"] * len(ctx.teams))
        clauses.append(f"UPPER(LTRIM(RTRIM(t.TEAMNAME))) IN ({placeholders})")
        params.extend([str(t).strip().upper() for t in ctx.teams])
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    df = _fetch_df(ctx.db, sql, tuple(params) if params else None)
    if df.empty:
        return 0
    return int(pd.to_numeric(df.get("CNT"), errors="coerce").fillna(0).iloc[0])


def detect_app_groups_missing_msp_assignment(ctx: QualityContext) -> List[Issue]:
    apps = _load_app_groups(ctx)
    if apps.empty:
        return []
    sample = _fetch_df(ctx.db, "SELECT TOP 1 * FROM TEAM_MSP_ASSIGNMENTS", None)
    if sample is None or not isinstance(sample, pd.DataFrame):
        return []
    year_col = "YEAR" if "YEAR" in sample.columns else ("EFFECTIVE_YEAR" if "EFFECTIVE_YEAR" in sample.columns else "")
    if not year_col:
        return []
    # Only require MSP assignment when there is a signal that MSP applies.
    required_group_ids: set[str] = set()
    group_map = (
        apps[["GROUPID", "GROUPNAME"]]
        .dropna()
        .astype(str)
        .applymap(lambda x: str(x).strip())
    )
    group_map = group_map[group_map["GROUPID"].ne("") & group_map["GROUPNAME"].ne("")].copy()
    group_lookup = dict(zip(group_map["GROUPNAME"], group_map["GROUPID"]))

    # MSP costs present in cost lines for an app group.
    cl = ctx.cost_lines if isinstance(ctx.cost_lines, pd.DataFrame) else pd.DataFrame()
    if not cl.empty and "GROUPNAME" in cl.columns:
        cl_group = cl["GROUPNAME"].fillna("").astype(str).str.strip()
        source_cols = [c for c in ["SOURCE", "COST_CATEGORY", "SUBCOMPONENT"] if c in cl.columns]
        if source_cols:
            src = cl[source_cols[0]].fillna("").astype(str).str.upper().str.strip()
            for col in source_cols[1:]:
                src = src + " " + cl[col].fillna("").astype(str).str.upper().str.strip()
            msp_mask = src.str.contains("MSP", na=False)
            if msp_mask.any():
                for name in cl_group[msp_mask].unique().tolist():
                    gid = group_lookup.get(str(name).strip())
                    if gid:
                        required_group_ids.add(str(gid))

    # ADO features marked as MSP for an app group.
    ado = ctx.ado_features if isinstance(ctx.ado_features, pd.DataFrame) else pd.DataFrame()
    if not ado.empty and "IS_MSP_FEATURE" in ado.columns and "GROUPNAME" in ado.columns:
        msp_flag = pd.to_numeric(ado.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int) > 0
        if msp_flag.any():
            for name in ado.loc[msp_flag, "GROUPNAME"].fillna("").astype(str).str.strip().unique().tolist():
                gid = group_lookup.get(str(name).strip())
                if gid:
                    required_group_ids.add(str(gid))

    if not required_group_ids:
        return []
    assigned = _fetch_df(
        ctx.db,
        f"SELECT DISTINCT GROUPID FROM TEAM_MSP_ASSIGNMENTS WHERE {year_col} = %s",
        (ctx.year,),
    )
    assigned_ids = set(assigned.get("GROUPID", pd.Series([], dtype=str)).dropna().astype(str).tolist())
    missing = apps.loc[
        apps["GROUPID"].astype(str).isin(required_group_ids)
        & ~apps["GROUPID"].astype(str).isin(assigned_ids),
        ["GROUPID", "GROUPNAME", "PROGRAMNAME", "TEAMNAME"],
    ].copy()
    if missing.empty:
        return []
    title = "Applications missing MSP assignment"
    summary = f"{len(missing):,d} application(s) have no MSP assignment for year {ctx.year}."
    return [
        _make_issue(
            issue_type=IssueType.APP_GROUP_MISSING_MSP_ASSIGNMENT,
            title=title,
            severity=Severity.WARNING,
            source="MSP",
            scope={
                "year": ctx.year,
                "programs": list(ctx.programs),
                "teams": list(ctx.teams),
                "app_groups": missing["GROUPNAME"].astype(str).tolist(),
            },
            impact_basis=ImpactBasis.MIXED,
            confidence="MEDIUM",
            summary=summary,
            evidence_ref={"fn": "app_groups_missing_msp", "params": {"group_ids": missing["GROUPID"].astype(str).tolist()}},
            fix={
                "page": "pages/7_Rates.py",
                "action_label": "Assign MSP (only when MSP applies)",
                "help": "Add MSP assignments for apps that carry MSP work or MSP-tagged features.",
            },
            tags=["msp", "ownership"],
            dedupe_key=f"app_groups_missing_msp:{ctx.year}:{sorted(missing['GROUPID'].astype(str).tolist())}",
        )
    ]


def detect_app_groups_missing_ado_mapping(ctx: QualityContext) -> List[Issue]:
    if _mapping_row_count(ctx) == 0:
        return []
    apps = _load_app_groups(ctx)
    if apps.empty:
        return []
    ado = _ado_scope_rows(ctx)
    if not ado.empty:
        group = ado.get("GROUPNAME", pd.Series([""] * len(ado), index=ado.index)).fillna("").astype(str).str.strip()
        status = ado.get("MAPPING_STATUS", pd.Series([""] * len(ado), index=ado.index)).fillna("").astype(str).str.upper().str.strip()
        is_unmapped = group.str.upper().isin({"(UNMAPPED APP GROUP)", "(UNASSIGNED)", "UNASSIGNED"}) | status.str.contains("UNMAPPED", na=False)
        if is_unmapped.any():
            return []
    mapped = _fetch_df(
        ctx.db,
        """
        SELECT DISTINCT APP_GROUP
        FROM MAP_ADO_APP_TO_TCO_GROUP
        WHERE APP_GROUP IS NOT NULL AND LTRIM(RTRIM(APP_GROUP)) <> ''
        """,
        None,
    )
    mapped_ids = set(mapped.get("APP_GROUP", pd.Series([], dtype=str)).dropna().astype(str).tolist())
    missing = apps.loc[~apps["GROUPID"].astype(str).isin(mapped_ids), ["GROUPID", "GROUPNAME", "PROGRAMNAME", "TEAMNAME"]].copy()
    if missing.empty:
        return []
    title = "Applications missing ADO mapping"
    summary = f"{len(missing):,d} application(s) have no linked ADO app names."
    return [
        _make_issue(
            issue_type=IssueType.APP_GROUP_MISSING_ADO_MAPPING,
            title=title,
            severity=Severity.WARNING,
            source="ADO",
            scope={
                "year": ctx.year,
                "programs": list(ctx.programs),
                "teams": list(ctx.teams),
                "app_groups": missing["GROUPNAME"].astype(str).tolist(),
            },
            impact_basis=ImpactBasis.MIXED,
            confidence="MEDIUM",
            summary=summary,
            evidence_ref={"fn": "app_groups_missing_ado_mapping", "params": {"group_ids": missing["GROUPID"].astype(str).tolist()}},
            fix={"page": "pages/6_Applications.py", "action_label": "Link ADO app names", "help": "Map ADO app names to each application."},
            tags=["ado", "mapping"],
            dedupe_key=f"app_groups_missing_ado_mapping:{ctx.year}:{sorted(missing['GROUPID'].astype(str).tolist())}",
        )
    ]


def detect_app_groups_multiple_instances(ctx: QualityContext) -> List[Issue]:
    apps = _load_app_groups(ctx)
    if apps.empty:
        return []
    inst = _fetch_df(
        ctx.db,
        "SELECT GROUPID, COUNT(*) AS INSTANCE_COUNT FROM APPLICATIONS GROUP BY GROUPID",
        None,
    )
    if inst is None or inst.empty or "GROUPID" not in inst.columns:
        return []
    inst["GROUPID"] = inst["GROUPID"].astype(str)
    inst["INSTANCE_COUNT"] = pd.to_numeric(inst.get("INSTANCE_COUNT"), errors="coerce").fillna(0).astype(int)
    merged = apps.merge(inst[["GROUPID", "INSTANCE_COUNT"]], on="GROUPID", how="left")
    multi = merged.loc[merged["INSTANCE_COUNT"] > 1, ["GROUPID", "GROUPNAME", "PROGRAMNAME", "TEAMNAME", "INSTANCE_COUNT"]].copy()
    if multi.empty:
        return []
    title = "Applications with multiple instances"
    summary = f"{len(multi):,d} application(s) have multiple instances."
    return [
        _make_issue(
            issue_type=IssueType.APP_GROUP_MULTI_INSTANCE,
            title=title,
            severity=Severity.INFO,
            source="APPLICATIONS",
            scope={
                "year": ctx.year,
                "programs": list(ctx.programs),
                "teams": list(ctx.teams),
                "app_groups": multi["GROUPNAME"].astype(str).tolist(),
            },
            impact_basis=ImpactBasis.MIXED,
            confidence="LOW",
            summary=summary,
            evidence_ref={"fn": "app_groups_multi_instance", "params": {"group_ids": multi["GROUPID"].astype(str).tolist()}},
            fix={"page": "pages/6_Applications.py", "action_label": "Review instances", "help": "Confirm instance structure and invoice/contract links."},
            tags=["applications", "structure"],
            dedupe_key=f"app_groups_multi_instance:{ctx.year}:{sorted(multi['GROUPID'].astype(str).tolist())}",
        )
    ]


def _ado_scope_rows(ctx: QualityContext) -> pd.DataFrame:
    df = ctx.ado_features
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "ADO_YEAR" in out.columns:
        out = out[pd.to_numeric(out["ADO_YEAR"], errors="coerce").fillna(0).astype(int).eq(int(ctx.year))].copy()
    return out


def _ado_find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
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


def detect_ado_unmapped_app(ctx: QualityContext) -> List[Issue]:
    ado = _ado_scope_rows(ctx)
    if ado.empty:
        return []
    group = ado.get("GROUPNAME", pd.Series([""] * len(ado), index=ado.index)).fillna("").astype(str).str.strip()
    status = ado.get("MAPPING_STATUS", pd.Series([""] * len(ado), index=ado.index)).fillna("").astype(str).str.upper().str.strip()
    derived = pd.to_numeric(ado.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
    is_unmapped = group.str.upper().isin({"(UNMAPPED APP GROUP)", "(UNASSIGNED)", "UNASSIGNED"}) | status.str.contains("UNMAPPED", na=False)
    if ctx.app_groups:
        # If the user filtered to app groups, unmapped app detection is less relevant.
        return []
    if not is_unmapped.any():
        return []
    n = int(is_unmapped.sum())
    fte = float(derived.loc[is_unmapped].sum() or 0.0)
    title = "ADO demand not mapped to an application group"
    summary = f"{n:,d} feature(s) are missing application group mapping."
    if fte > 0:
        summary += f" Derived FTE affected: {fte:,.2f}."
    usd_per_fte, basis, conf = estimate_usd_from_fte(
        ctx,
        year=ctx.year,
        program=(ctx.programs[0] if len(ctx.programs) == 1 else None),
        team=(ctx.teams[0] if len(ctx.teams) == 1 else None),
    )
    impact_usd_est = float(fte * usd_per_fte) if usd_per_fte > 0 and fte > 0 else 0.0
    impact_detail = basis if impact_usd_est > 0 else "Not estimated"
    return [
        _make_issue(
            issue_type=IssueType.ADO_UNMAPPED_APP,
            title=title,
            severity=Severity.WARNING,
            scenario="Expected",
            source="ADO",
            scope={"year": ctx.year, "programs": list(ctx.programs), "teams": list(ctx.teams)},
            impact_usd_est=impact_usd_est,
            impact_fte=fte,
            impact_features=n,
            impact_basis=ImpactBasis.FTE if fte > 0 else ImpactBasis.FEATURES,
            impact_basis_detail=impact_detail,
            confidence=conf if impact_usd_est > 0 else "LOW",
            summary=summary,
            evidence_ref={"fn": "ado_unmapped_app", "params": {"limit": 200}},
            fix={"page": "pages/6_Applications.py", "action_label": "Map apps to app groups", "help": "Ensure each feature has an application group mapping."},
            tags=["ado", "mapping"],
            dedupe_key=f"ado_unmapped_app:{ctx.year}:{tuple(ctx.programs)}:{tuple(ctx.teams)}",
        )
    ]


def detect_ado_unmapped_team(ctx: QualityContext) -> List[Issue]:
    ado = _ado_scope_rows(ctx)
    if ado.empty:
        return []
    team = ado.get("TEAMNAME", pd.Series([""] * len(ado), index=ado.index)).fillna("").astype(str).str.strip()
    status = ado.get("MAPPING_STATUS", pd.Series([""] * len(ado), index=ado.index)).fillna("").astype(str).str.upper().str.strip()
    derived = pd.to_numeric(ado.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
    is_unmapped = team.str.upper().isin({"(UNASSIGNED)", "UNASSIGNED"}) | status.str.contains("TEAM", na=False) & status.str.contains("UNMAPPED", na=False)
    if ctx.teams:
        return []
    if not is_unmapped.any():
        return []
    n = int(is_unmapped.sum())
    fte = float(derived.loc[is_unmapped].sum() or 0.0)
    title = "ADO demand not mapped to a team"
    summary = f"{n:,d} feature(s) are missing team mapping."
    if fte > 0:
        summary += f" Derived FTE affected: {fte:,.2f}."
    usd_per_fte, basis, conf = estimate_usd_from_fte(
        ctx,
        year=ctx.year,
        program=(ctx.programs[0] if len(ctx.programs) == 1 else None),
        team=(ctx.teams[0] if len(ctx.teams) == 1 else None),
    )
    impact_usd_est = float(fte * usd_per_fte) if usd_per_fte > 0 and fte > 0 else 0.0
    return [
        _make_issue(
            issue_type=IssueType.ADO_UNMAPPED_TEAM,
            title=title,
            severity=Severity.WARNING,
            source="ADO",
            scope={"year": ctx.year, "programs": list(ctx.programs), "app_groups": list(ctx.app_groups)},
            impact_usd_est=impact_usd_est,
            impact_fte=fte,
            impact_features=n,
            impact_basis=ImpactBasis.FTE if fte > 0 else ImpactBasis.FEATURES,
            impact_basis_detail=basis if impact_usd_est > 0 else "No $/FTE rate available; USD impact omitted.",
            confidence=conf if impact_usd_est > 0 else "LOW",
            summary=summary,
            evidence_ref={"fn": "ado_unmapped_team", "params": {"limit": 200}},
            fix={"page": "pages/4_Teams.py", "action_label": "Map ADO teams", "help": "Ensure each feature has a valid team assignment."},
            tags=["ado", "mapping"],
            dedupe_key=f"ado_unmapped_team:{ctx.year}:{tuple(ctx.programs)}:{tuple(ctx.app_groups)}",
        )
    ]


def detect_ado_missing_effort(ctx: QualityContext) -> List[Issue]:
    ado = _ado_scope_rows(ctx)
    if ado.empty:
        return []
    if "DERIVED_FTE" not in ado.columns:
        return []
    derived = pd.to_numeric(ado["DERIVED_FTE"], errors="coerce").fillna(0.0)
    is_msp = pd.to_numeric(ado.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int) > 0
    missing = (~is_msp) & (derived <= 0.0)
    if not missing.any():
        return []
    n = int(missing.sum())
    title = "ADO demand missing SWAG effort (Derived FTE is 0)"
    summary = f"{n:,d} feature(s) have no Derived FTE; projections may under-report."
    return [
        _make_issue(
            issue_type=IssueType.ADO_MISSING_EFFORT,
            title=title,
            severity=Severity.INFO,
            source="ADO",
            scope={"year": ctx.year, "programs": list(ctx.programs), "teams": list(ctx.teams), "app_groups": list(ctx.app_groups)},
            impact_features=n,
            impact_basis=ImpactBasis.FEATURES,
            confidence="MEDIUM",
            summary=summary,
            evidence_ref={"fn": "ado_missing_effort", "params": {"limit": 200}},
            fix={"page": "pages/10_Settings.py", "action_label": "Review SWAG readiness", "help": "Ensure SWAG/Derived FTE inputs are present for in-scope features."},
            tags=["ado", "demand"],
            dedupe_key=f"ado_missing_effort:{ctx.year}:{tuple(ctx.programs)}:{tuple(ctx.teams)}:{tuple(ctx.app_groups)}",
        )
    ]


def detect_ado_features_missing_swag(ctx: QualityContext) -> List[Issue]:
    ado = _ado_scope_rows(ctx)
    if ado.empty:
        return []
    swag_col = _ado_find_col(ado, ["STORY_POINTS", "EFFORT_POINTS", "SWAG", "SWAG_POINTS"])
    if not swag_col:
        return []
    swag = pd.to_numeric(ado[swag_col], errors="coerce")
    is_msp = pd.to_numeric(ado.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int) > 0
    missing = (~is_msp) & (swag.isna() | (swag <= 0))
    if not missing.any():
        return []
    n = int(missing.sum())
    title = ""
    summary = f"{n:,d} feature(s) are missing SWAG (story points)."
    return [
        _make_issue(
            issue_type=IssueType.ADO_FEATURES_MISSING_SWAG,
            title=title,
            severity=Severity.BLOCKER,
            scenario="Expected",
            source="ADO",
            scope={"year": ctx.year, "programs": list(ctx.programs), "teams": list(ctx.teams), "app_groups": list(ctx.app_groups)},
            impact_features=n,
            impact_basis=ImpactBasis.FEATURES,
            confidence="HIGH",
            summary=summary,
            evidence_ref={"fn": "ado_missing_swag", "params": {"limit": 200}},
            fix=None,
            tags=["ado", "demand"],
            dedupe_key=f"ado_missing_swag:{ctx.year}:{tuple(ctx.programs)}:{tuple(ctx.teams)}:{tuple(ctx.app_groups)}",
        )
    ]


def detect_ado_features_missing_app_name(ctx: QualityContext) -> List[Issue]:
    ado = _ado_scope_rows(ctx)
    if ado.empty:
        return []
    app_col = _ado_find_col(ado, ["APP_NAME_RAW", "APP_NAME", "ADO_APP_NAME"])
    if not app_col:
        return []
    app_vals = ado[app_col].fillna("").astype(str).str.strip()
    is_msp = pd.to_numeric(ado.get("IS_MSP_FEATURE"), errors="coerce").fillna(0).astype(int) > 0
    missing = (~is_msp) & app_vals.eq("")
    if not missing.any():
        return []
    n = int(missing.sum())
    title = ""
    summary = f"{n:,d} feature(s) are missing an App Name."
    return [
        _make_issue(
            issue_type=IssueType.ADO_FEATURES_MISSING_APP_NAME,
            title=title,
            severity=Severity.BLOCKER,
            scenario="Expected",
            source="ADO",
            scope={"year": ctx.year, "programs": list(ctx.programs), "teams": list(ctx.teams), "app_groups": list(ctx.app_groups)},
            impact_features=n,
            impact_basis=ImpactBasis.FEATURES,
            confidence="HIGH",
            summary=summary,
            evidence_ref={"fn": "ado_missing_app_name", "params": {"limit": 200}},
            fix=None,
            tags=["ado", "mapping"],
            dedupe_key=f"ado_missing_app_name:{ctx.year}:{tuple(ctx.programs)}:{tuple(ctx.teams)}:{tuple(ctx.app_groups)}",
        )
    ]


def detect_unmapped_apptio_actuals(ctx: QualityContext) -> List[Issue]:
    df = ctx.apptio_actuals_breakdown
    if df is None or df.empty:
        return []
    if "MAPPING_STATUS" not in df.columns or "AMOUNT" not in df.columns:
        return []
    status = df["MAPPING_STATUS"].fillna("").astype(str).str.upper().str.strip()
    amt = pd.to_numeric(df["AMOUNT"], errors="coerce").fillna(0.0)
    bad = status.isin({"DEFAULT_OTHER", "INVALID_TARGET"}) | status.eq("")
    impact = float(amt.loc[bad].sum() or 0.0)
    if impact <= 0:
        return []
    sev = _severity_for_usd(impact)
    title = "Apptio actuals not mapped to a planned NWF category"
    summary = "Some Apptio NWF lines defaulted to 'NWF Other' due to missing/invalid mapping."
    return [
        _make_issue(
            issue_type=IssueType.APPTIO_UNMAPPED_ACTUALS,
            title=title,
            severity=sev,
            scenario="ACTUAL",
            source="APPTIO",
            scope={"year": ctx.year, "programs": list(ctx.programs)},
            impact_usd_est=impact,
            impact_basis=ImpactBasis.USD,
            impact_basis_detail="Est. using sum of Apptio rows with DEFAULT_OTHER/INVALID_TARGET (USD).",
            confidence="HIGH",
            summary=summary,
            evidence_ref={"fn": "apptio_unmapped_actuals", "params": {"limit": 200}},
            fix={"page": "pages/10_Settings.py", "action_label": "Fix Apptio mappings", "help": "Add product mappings and ledger fallback mappings so actuals land in planned NWF types."},
            tags=["actuals", "apptio", "mapping"],
            dedupe_key=f"apptio_unmapped:{ctx.year}:{tuple(ctx.programs)}",
        )
    ]


def run_all_detectors(ctx: QualityContext) -> List[Issue]:
    from core.quality.detectors_config import detect_configuration_readiness

    detectors = [
        detect_configuration_readiness,
        detect_unassigned_cost,
        detect_missing_rates_for_wf_cost,
        detect_team_missing_program,
        detect_app_group_missing_owner_team,
        detect_app_groups_missing_msp_assignment,
        detect_app_groups_missing_ado_mapping,
        detect_app_groups_multiple_instances,
        detect_ado_unmapped_app,
        detect_ado_unmapped_team,
        detect_ado_features_missing_swag,
        detect_ado_features_missing_app_name,
        detect_ado_missing_effort,
        detect_unmapped_apptio_actuals,
    ]
    out: List[Issue] = []
    for fn in detectors:
        try:
            out.extend(fn(ctx))
        except Exception:
            continue
    out = dedupe_issues(out)
    return sort_issues(out)
