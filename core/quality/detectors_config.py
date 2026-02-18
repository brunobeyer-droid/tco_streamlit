# Summary: update data quality action labels to NEXT branding.
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from core.quality.context import QualityContext
from core.quality.issues import ImpactBasis, Issue, IssueStatus, IssueType, Severity


def _fetch_df(db: Any, sql: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
    try:
        if callable(db):
            out = db(sql, params)
        else:
            out = db.fetch_df(sql, params)  # type: ignore[attr-defined]
        return out if isinstance(out, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _make_issue(
    *,
    issue_type: str,
    title: str,
    severity: str,
    scope: Dict[str, Any],
    summary: str,
    evidence_ref: Optional[Dict[str, Any]],
    fix: Dict[str, Any],
    scenario: str = "",
    impact_usd_est: Optional[float] = None,
    impact_features: int = 0,
    impact_basis_detail: str = "",
    tags: Optional[List[str]] = None,
    dedupe_key: Optional[str] = None,
) -> Issue:
    dedupe = str(dedupe_key or f"{issue_type}:{title}:{sorted((scope or {}).items())}")
    issue_id = hashlib.sha1(dedupe.encode("utf-8")).hexdigest()[:10]
    return Issue(
        issue_id=f"{issue_type}_{issue_id}",
        issue_type=issue_type,
        title=title,
        severity=severity,
        status=IssueStatus.OPEN,
        scenario=str(scenario or "").strip(),
        scope=dict(scope or {}),
        impact_usd_est=float(impact_usd_est) if impact_usd_est is not None else 0.0,
        impact_features=int(impact_features or 0),
        impact_basis=ImpactBasis.MIXED,
        impact_basis_detail=str(impact_basis_detail or "").strip(),
        summary=summary,
        evidence_ref=evidence_ref,
        fix=dict(fix or {}),
        tags=list(tags or []),
        dedupe_key=dedupe,
    )


def _ado_enabled(ctx: QualityContext) -> bool:
    df = _fetch_df(ctx.db, "SELECT TOP 1 PORTFOLIO_NAME FROM ADO_PORTFOLIO_SETTINGS", None)
    return not df.empty


def _programs_in_scope(ctx: QualityContext) -> List[str]:
    if ctx.programs:
        return [str(p).strip() for p in ctx.programs if str(p).strip()]
    df = _fetch_df(ctx.db, "SELECT PROGRAMNAME FROM PROGRAMS", None)
    if df.empty or "PROGRAMNAME" not in df.columns:
        return []
    return df["PROGRAMNAME"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().tolist()


def _teams_in_scope(ctx: QualityContext, programs: List[str]) -> pd.DataFrame:
    if ctx.teams:
        placeholders = ", ".join(["%s"] * len(ctx.teams))
        return _fetch_df(
            ctx.db,
            f"""
            SELECT t.TEAMID, t.TEAMNAME, p.PROGRAMNAME
            FROM TEAMS t
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
            WHERE UPPER(LTRIM(RTRIM(t.TEAMNAME))) IN ({placeholders})
            """,
            tuple([str(t).strip().upper() for t in ctx.teams]),
        )
    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        return _fetch_df(
            ctx.db,
            f"""
            SELECT t.TEAMID, t.TEAMNAME, p.PROGRAMNAME
            FROM TEAMS t
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
            WHERE UPPER(LTRIM(RTRIM(p.PROGRAMNAME))) IN ({placeholders})
            """,
            tuple([str(p).strip().upper() for p in programs]),
        )
    return _fetch_df(
        ctx.db,
        """
        SELECT t.TEAMID, t.TEAMNAME, p.PROGRAMNAME
        FROM TEAMS t
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        """,
        None,
    )


def _teams_for_program(ctx: QualityContext, program_name: str) -> List[str]:
    if not program_name:
        return []
    df = _fetch_df(
        ctx.db,
        """
        SELECT t.TEAMNAME
        FROM TEAMS t
        JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
        WHERE UPPER(LTRIM(RTRIM(p.PROGRAMNAME))) = %s
        """,
        (str(program_name).strip().upper(),),
    )
    if df.empty or "TEAMNAME" not in df.columns:
        return []
    return df["TEAMNAME"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().tolist()


def detect_configuration_readiness(ctx: QualityContext) -> List[Issue]:
    issues: List[Issue] = []
    year = int(ctx.year)
    programs = _programs_in_scope(ctx)
    teams_df = _teams_in_scope(ctx, programs)

    # A) Missing WF rates (configuration-level)
    team_ids = teams_df.get("TEAMID", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    team_names = teams_df.get("TEAMNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    team_ids = team_ids[team_ids.ne("")]
    if not team_ids.empty:
        placeholders = ", ".join(["%s"] * len(team_ids))
        df_rates = _fetch_df(
            ctx.db,
            f"""
            SELECT DISTINCT TEAMID
            FROM TEAM_RATE_HISTORY
            WHERE YEAR = %s AND TEAMID IN ({placeholders})
            """,
            tuple([year] + team_ids.tolist()),
        )
        has_rate = set(df_rates.get("TEAMID", pd.Series(dtype=str)).fillna("").astype(str).tolist())
        missing_ids = [tid for tid in team_ids.tolist() if tid not in has_rate]
        if missing_ids and len(missing_ids) == len(team_ids):
            missing_names = (
                teams_df.loc[teams_df["TEAMID"].astype(str).isin(missing_ids), "TEAMNAME"]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .tolist()
            )
            evidence_rows = [
                {"YEAR": year, "SCOPE_LEVEL": "TEAM", "TEAM": name}
                for name in missing_names[:200]
            ]
            summary = f"No team rates configured for {year} in the current scope."
            issues.append(
                _make_issue(
                    issue_type=IssueType.CONFIG_MISSING_WF_RATES,
                    title="Missing workforce rates (configuration)",
                    severity=Severity.BLOCKER,
                    scope={"year": year, "teams": missing_names},
                    summary=summary,
                    evidence_ref={"fn": "config_missing_wf_rates", "params": {"rows": evidence_rows}},
                    fix={
                        "page": "pages/7_Rates.py",
                        "action_label": "Add WF rates for the selected year (rates are per PI/iteration)",
                        "help": "Create rate rows for each team/location in the selected year.",
                    },
                    tags=["rates", "configuration"],
                    dedupe_key=f"config_missing_wf_rates:{year}:{sorted(missing_ids)}",
                )
            )

    # B) ADO mapping rows not configured yet
    if _ado_enabled(ctx):
        map_sql = """
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
            map_sql += " WHERE " + " AND ".join(clauses)
        df_map = _fetch_df(ctx.db, map_sql, tuple(params) if params else None)
        count_val = int(pd.to_numeric(df_map.get("CNT"), errors="coerce").fillna(0).iloc[0] if not df_map.empty else 0)
        if count_val == 0:
            evidence_rows = [{"PROGRAM": p, "TEAM": "", "mapping_row_count": 0} for p in programs[:200]]
            if ctx.teams:
                evidence_rows = [{"PROGRAM": "", "TEAM": t, "mapping_row_count": 0} for t in ctx.teams[:200]]
            issues.append(
                _make_issue(
                    issue_type=IssueType.CONFIG_NO_ADO_MAPPING_ROWS,
                    title="ADO mapping not configured (no mapping rows yet)",
                    severity=Severity.WARNING,
                    scope={"year": year, "programs": list(ctx.programs), "teams": list(ctx.teams)},
                    scenario="Expected",
                    impact_usd_est=None,
                    impact_features=0,
                    impact_basis_detail="Not estimated",
                    summary="No ADO app name mappings exist for this scope.",
                    evidence_ref={"fn": "config_no_ado_mapping", "params": {"rows": evidence_rows}},
                    fix={
                        "page": "pages/6_Applications.py",
                        "action_label": "Create initial mappings for ADO app names to NEXT application groups",
                        "help": "Map ADO app names to Applications to enable allocation.",
                    },
                    tags=["ado", "mapping", "configuration"],
                    dedupe_key=f"config_no_ado_mapping:{year}:{tuple(ctx.programs)}:{tuple(ctx.teams)}",
                )
            )

    # B2) ADO teams unmapped for programs
    if _ado_enabled(ctx):
        programs = _programs_in_scope(ctx)
        if programs:
            placeholders = ", ".join(["%s"] * len(programs))
            df_prog = _fetch_df(
                ctx.db,
                f"""
                SELECT PROGRAMID, PROGRAMNAME
                FROM PROGRAMS
                WHERE UPPER(LTRIM(RTRIM(COALESCE(NULLIF(PROGRAM_DISPLAY_NAME, ''), PROGRAMNAME)))) IN ({placeholders})
                """,
                tuple([p.upper() for p in programs]),
            )
            df_prog = df_prog if isinstance(df_prog, pd.DataFrame) else pd.DataFrame()
            prog_ids = df_prog.get("PROGRAMID", pd.Series([], dtype=str)).fillna("").astype(str).tolist()

            ado_prog = pd.DataFrame()
            if prog_ids:
                placeholders = ", ".join(["%s"] * len(prog_ids))
                ado_prog = _fetch_df(
                    ctx.db,
                    f"SELECT ADO_PROGRAM, PROGRAMID FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM WHERE PROGRAMID IN ({placeholders})",
                    tuple(prog_ids),
                )
            ado_prog = ado_prog if isinstance(ado_prog, pd.DataFrame) else pd.DataFrame()

            for prog in programs:
                prog_name = str(prog).strip()
                if not prog_name:
                    continue
                ado_keys: list[str] = []
                if not ado_prog.empty and not df_prog.empty and "PROGRAMID" in df_prog.columns:
                    pid = (
                        df_prog.loc[
                            df_prog.get("PROGRAMNAME", pd.Series(dtype=str))
                            .fillna("")
                            .astype(str)
                            .str.upper()
                            .eq(prog_name.upper()),
                            "PROGRAMID",
                        ]
                        .head(1)
                        .tolist()
                    )
                    if pid:
                        ado_keys = (
                            ado_prog.loc[ado_prog["PROGRAMID"].astype(str).isin([str(pid[0])]), "ADO_PROGRAM"]
                            .fillna("")
                            .astype(str)
                            .str.strip()
                            .tolist()
                        )
                key_set = {k for k in ado_keys if k}
                key_set.add(prog_name)
                if not key_set:
                    continue
                placeholders = ", ".join(["%s"] * len(key_set))
                df_team = _fetch_df(
                    ctx.db,
                    f"""
                    SELECT ADO_TEAM_KEY, ADO_TEAM, PROGRAM_RAW, TEAMID
                    FROM MAP_ADO_TEAM_TO_TCO_TEAM
                    WHERE UPPER(LTRIM(RTRIM(PROGRAM_RAW))) IN ({placeholders})
                    """,
                    tuple([k.upper() for k in key_set]),
                )
                if df_team is None or df_team.empty:
                    continue
                df_team["ADO_TEAM_KEY"] = df_team.get("ADO_TEAM_KEY", "").fillna("").astype(str).str.strip()
                df_team = df_team[df_team["ADO_TEAM_KEY"].ne("")].copy()
                total = int(df_team["ADO_TEAM_KEY"].nunique())
                if total <= 0:
                    continue
                teamid = df_team.get("TEAMID", "").fillna("").astype(str).str.strip()
                unmapped_mask = teamid.eq("")
                unmapped = int(unmapped_mask.sum())
                if unmapped <= 0:
                    continue
                ratio = float(unmapped) / float(total) if total else 0.0
                sev = Severity.BLOCKER if ratio >= 0.30 else Severity.WARNING
                evidence_rows = (
                    df_team.loc[unmapped_mask, ["ADO_TEAM", "PROGRAM_RAW", "ADO_TEAM_KEY"]]
                    .rename(columns={"ADO_TEAM": "ADO_TEAM_RAW", "PROGRAM_RAW": "PROGRAM"})
                    .copy()
                )
                evidence_rows["IS_MAPPED"] = False
                issues.append(
                    _make_issue(
                        issue_type=IssueType.CONFIG_ADO_TEAMS_UNMAPPED,
                        title="ADO teams not mapped to NEXT teams",
                        severity=sev,
                        scope={"year": year, "programs": [prog_name], "teams": list(ctx.teams)},
                        summary=f"{unmapped:,d} of {total:,d} ADO teams are not mapped for this program.",
                        evidence_ref={"fn": "config_ado_unmapped_teams", "params": {"rows": evidence_rows.head(200).to_dict('records')}},
                        fix={
                            "page": "pages/4_Teams.py",
                            "action_label": "Map ADO teams to NEXT teams",
                            "help": "Map ADO teams for this program.",
                        },
                        scenario="Expected",
                        impact_features=unmapped,
                        impact_basis_detail="Not estimated",
                        tags=["ado", "mapping", "configuration"],
                        dedupe_key=f"config_ado_teams_unmapped:{year}:{prog_name}",
                    )
            )

    # B2) ADO teams unmapped at program level (onboarding readiness)
    if _ado_enabled(ctx):
        programs = _programs_in_scope(ctx)
        if programs:
            placeholders = ", ".join(["%s"] * len(programs))
            df_prog = _fetch_df(
                ctx.db,
                f"""
                SELECT PROGRAMID, PROGRAMNAME
                FROM PROGRAMS
                WHERE UPPER(LTRIM(RTRIM(COALESCE(NULLIF(PROGRAM_DISPLAY_NAME, ''), PROGRAMNAME)))) IN ({placeholders})
                """,
                tuple([p.upper() for p in programs]),
            )
            df_prog = df_prog if isinstance(df_prog, pd.DataFrame) else pd.DataFrame()
            prog_ids = df_prog.get("PROGRAMID", pd.Series([], dtype=str)).fillna("").astype(str).tolist()
            ado_prog = pd.DataFrame()
            if prog_ids:
                placeholders = ", ".join(["%s"] * len(prog_ids))
                ado_prog = _fetch_df(
                    ctx.db,
                    f"SELECT ADO_PROGRAM, PROGRAMID FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM WHERE PROGRAMID IN ({placeholders})",
                    tuple(prog_ids),
                )
            ado_prog = ado_prog if isinstance(ado_prog, pd.DataFrame) else pd.DataFrame()

            # Preload mapped team keys (no join required for unmapped detection).
            map_df = _fetch_df(
                ctx.db,
                "SELECT ADO_TEAM_KEY, TEAMID FROM MAP_ADO_TEAM_TO_TCO_TEAM",
                None,
            )
            map_df = map_df if isinstance(map_df, pd.DataFrame) else pd.DataFrame()
            mapped_keys = set(
                map_df.loc[
                    map_df.get("TEAMID", "").fillna("").astype(str).str.strip().ne(""),
                    "ADO_TEAM_KEY",
                ]
                .fillna("")
                .astype(str)
                .str.strip()
                .tolist()
            )

            for prog in programs:
                prog_name = str(prog).strip()
                if not prog_name:
                    continue
                ado_keys: list[str] = []
                if not ado_prog.empty and not df_prog.empty and "PROGRAMID" in df_prog.columns:
                    pid = (
                        df_prog.loc[
                            df_prog.get("PROGRAMNAME", pd.Series(dtype=str))
                            .fillna("")
                            .astype(str)
                            .str.upper()
                            .eq(prog_name.upper()),
                            "PROGRAMID",
                        ]
                        .head(1)
                        .tolist()
                    )
                    if pid:
                        ado_keys = (
                            ado_prog.loc[ado_prog["PROGRAMID"].astype(str).isin([str(pid[0])]), "ADO_PROGRAM"]
                            .fillna("")
                            .astype(str)
                            .str.strip()
                            .tolist()
                        )
                key_set = {k for k in ado_keys if k}
                key_set.add(prog_name)
                if not key_set:
                    continue
                placeholders = ", ".join(["%s"] * len(key_set))
                ado_team_rows = _fetch_df(
                    ctx.db,
                    f"""
                    SELECT TEAM_VARIANT_KEY, TEAM_RAW, PROGRAM_RAW,
                           MAX(COALESCE(CHANGED_AT, CREATED_AT)) AS LAST_SEEN_AT,
                           COUNT(DISTINCT FEATURE_ID) AS FEATURE_COUNT,
                           SUM(TRY_CONVERT(FLOAT, COALESCE(STORY_POINTS, EFFORT_POINTS))) AS EFFORT_SUM
                    FROM ADO_FEATURES
                    WHERE TEAM_VARIANT_KEY IS NOT NULL AND LTRIM(RTRIM(TEAM_VARIANT_KEY)) <> ''
                      AND PROGRAM_RAW IS NOT NULL AND LTRIM(RTRIM(PROGRAM_RAW)) <> ''
                      AND UPPER(LTRIM(RTRIM(PROGRAM_RAW))) IN ({placeholders})
                    GROUP BY TEAM_VARIANT_KEY, TEAM_RAW, PROGRAM_RAW
                    """,
                    tuple([k.upper() for k in key_set]),
                )
                ado_team_rows = ado_team_rows if isinstance(ado_team_rows, pd.DataFrame) else pd.DataFrame()
                if ado_team_rows.empty:
                    continue
                ado_team_rows["TEAM_VARIANT_KEY"] = (
                    ado_team_rows.get("TEAM_VARIANT_KEY", "").fillna("").astype(str).str.strip()
                )
                ado_team_rows["IS_MAPPED"] = ado_team_rows["TEAM_VARIANT_KEY"].isin(mapped_keys)
                unmapped = ado_team_rows.loc[~ado_team_rows["IS_MAPPED"]].copy()
                if unmapped.empty:
                    continue
                unmapped_count = int(len(unmapped))
                feat_sum = int(pd.to_numeric(unmapped.get("FEATURE_COUNT"), errors="coerce").fillna(0).sum())
                effort_sum = float(pd.to_numeric(unmapped.get("EFFORT_SUM"), errors="coerce").fillna(0.0).sum())
                sev = Severity.BLOCKER if (feat_sum >= 10 or effort_sum >= 200 or unmapped_count >= 3) else Severity.WARNING
                evidence = (
                    unmapped.sort_values(
                        by=[
                            "EFFORT_SUM" if "EFFORT_SUM" in unmapped.columns else "FEATURE_COUNT",
                        ],
                        ascending=False,
                    )
                    .head(200)
                    .copy()
                )
                evidence["PROGRAM_RAW"] = evidence.get("PROGRAM_RAW", "").fillna("").astype(str)
                evidence["ADO_TEAM_RAW"] = evidence.get("TEAM_RAW", "").fillna("").astype(str)
                evidence["TEAM_DISPLAY"] = evidence.get("TEAM_RAW", "").fillna("").astype(str)
                evidence_rows = evidence[
                    [c for c in ["PROGRAM_RAW", "ADO_TEAM_RAW", "TEAM_DISPLAY", "FEATURE_COUNT", "EFFORT_SUM", "LAST_SEEN_AT", "IS_MAPPED"] if c in evidence.columns]
                ].to_dict("records")
                issues.append(
                    _make_issue(
                        issue_type=IssueType.ADO_UNMAPPED_TEAMS_IN_PROGRAM,
                        title="ADO teams not mapped to NEXT teams (program onboarding incomplete)",
                        severity=sev,
                        scope={"year": year, "programs": [prog_name]},
                        summary=f"{unmapped_count:,d} ADO team(s) are not mapped for this program.",
                        evidence_ref={"fn": "ado_unmapped_teams_in_program", "params": {"rows": evidence_rows}},
                        fix={
                            "page": "pages/10_Settings.py",
                            "action_label": "Map ADO teams to NEXT teams for the program",
                            "help": "Use Settings → ADO to complete team mappings.",
                        },
                        scenario="Expected",
                        impact_features=feat_sum if feat_sum > 0 else unmapped_count,
                        impact_basis_detail="Not estimated",
                        tags=["ado", "mapping", "onboarding"],
                        dedupe_key=f"ado_unmapped_teams_in_program:{year}:{prog_name}",
                    )
                )

    # C) Application groups missing owner (configuration-level)
    df_groups = _fetch_df(
        ctx.db,
        """
        SELECT g.GROUPID, g.GROUPNAME, g.TEAMID, p.PROGRAMNAME, t.TEAMNAME
        FROM APPLICATION_GROUPS g
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = g.PROGRAMID
        LEFT JOIN TEAMS t ON t.TEAMID = g.TEAMID
        """,
        None,
    )
    if not df_groups.empty:
        if ctx.programs:
            sel = {str(p).strip().upper() for p in ctx.programs}
            df_groups = df_groups[df_groups["PROGRAMNAME"].fillna("").astype(str).str.upper().isin(sel)].copy()
        if ctx.teams:
            sel = {str(t).strip().upper() for t in ctx.teams}
            df_groups = df_groups[df_groups["TEAMNAME"].fillna("").astype(str).str.upper().isin(sel)].copy()
        df_groups["TEAMID"] = df_groups.get("TEAMID", "").fillna("").astype(str).str.strip()
        df_groups["GROUPNAME"] = df_groups.get("GROUPNAME", "").fillna("").astype(str).str.strip()
        missing_groups = df_groups.loc[df_groups["TEAMID"].eq("") & df_groups["GROUPNAME"].ne(""), ["GROUPID", "GROUPNAME", "PROGRAMNAME"]]
        if not missing_groups.empty:
            evidence_rows = [
                {"APP_GROUP": r.GROUPNAME, "PROGRAM": str(r.PROGRAMNAME or "")}
                for r in missing_groups.itertuples(index=False)
            ][:200]
            issues.append(
                _make_issue(
                    issue_type=IssueType.CONFIG_APP_MISSING_OWNER,
                    title="Applications missing owning team (configuration)",
                    severity=Severity.BLOCKER,
                    scope={"year": year, "app_groups": missing_groups["GROUPNAME"].astype(str).tolist()},
                    summary=f"{len(missing_groups):,d} application(s) have no owning team configured.",
                    evidence_ref={"fn": "config_app_missing_owner", "params": {"rows": evidence_rows}},
                    fix={
                        "page": "pages/6_Applications.py",
                        "action_label": "Assign an owner team for each application group",
                        "help": "Set the Accountable Team for each application group.",
                    },
                    tags=["ownership", "configuration"],
                    dedupe_key=f"app_groups_missing_owner:{year}:{sorted(missing_groups['GROUPID'].astype(str).tolist())}",
                )
            )

    # D) Program has no teams configured (optional)
    for program in programs:
        teams_for_prog = _teams_for_program(ctx, program)
        if not teams_for_prog:
            issues.append(
                _make_issue(
                    issue_type=IssueType.CONFIG_PROGRAM_HAS_NO_TEAMS,
                    title="Program has no teams configured",
                    severity=Severity.WARNING,
                    scope={"year": year, "programs": [program]},
                    summary=f"No teams exist for program {program}.",
                    evidence_ref={"fn": "config_program_has_no_teams", "params": {"rows": [{"PROGRAM": program}]}},
                    fix={
                        "page": "pages/4_Teams.py",
                        "action_label": "Create teams and assign them to the program",
                        "help": "Teams are required for ownership, rates, and ADO mapping.",
                    },
                    tags=["teams", "configuration"],
                    dedupe_key=f"config_program_has_no_teams:{year}:{program}",
                )
            )

    return issues
