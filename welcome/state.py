import logging
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence

import pandas as pd


logger = logging.getLogger(__name__)

FetchFn = Callable[[str, Optional[Sequence]], Optional[pd.DataFrame]]


@dataclass
class UserScope:
    email: str
    name: str
    role: str
    program_ids: List[str] = field(default_factory=list)
    programs: List[str] = field(default_factory=list)
    teams: List[str] = field(default_factory=list)
    groups: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


def normalize_display_name(raw: str) -> str:
    """Normalize 'Last, First' to 'First Last' for display."""
    s = str(raw or "").strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",", 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            return f"{parts[1]} {parts[0]}"
    return s


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    seen = set()
    unique = []
    for raw in items:
        name = str(raw or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        unique.append(name)
    return unique


def _identity_candidates(user: dict) -> List[str]:
    """Return normalized owner identifiers for matching PROGRAMOWNER/PRODUCTOWNER/OWNER."""
    vals: List[str] = []
    if not isinstance(user, dict):
        return vals
    for key in ("email", "name", "display_name", "auth_display_name"):
        raw = str(user.get(key) or "").strip()
        if raw:
            vals.append(raw)
            vals.append(normalize_display_name(raw))
    cleaned: List[str] = []
    seen = set()
    for v in vals:
        s = str(v or "").strip()
        if not s:
            continue
        k = s.upper()
        if k in seen:
            continue
        seen.add(k)
        cleaned.append(s)
    return cleaned


def _owner_match_clause(column: str, owner_keys: Sequence[str]) -> tuple[str, tuple[str, ...]]:
    """
    Build a tolerant owner-match predicate for columns that may store single owners
    or owner lists (comma/semicolon/pipe/slash-delimited, or wrapped in <...>).
    """
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in owner_keys or []:
        key = str(raw or "").strip().upper()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    if not normalized:
        return "1=0", tuple()

    exact_ph = ", ".join(["%s"] * len(normalized))
    owner_norm_sql = (
        "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
        f"UPPER(COALESCE({column}, ''))"
        ", ';', ','), '|', ','), '/', ','), '<', ','), '>', ','), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), ' ', '')"
    )
    like_parts = [f"(',' + {owner_norm_sql} + ',') LIKE %s" for _ in normalized]
    where_sql = (
        f"(UPPER(LTRIM(RTRIM(COALESCE({column}, '')))) IN ({exact_ph})"
        + (f" OR {' OR '.join(like_parts)}" if like_parts else "")
        + ")"
    )
    like_params = [f"%,{k.replace(' ', '')},%" for k in normalized]
    return where_sql, tuple(normalized + like_params)


def _split_msp(teams: Sequence[str]) -> tuple[list[str], list[str]]:
    primary = [t for t in teams if "MSP" not in t.upper()]
    msp = [t for t in teams if "MSP" in t.upper()]
    return primary, msp


def format_team_caption(team_names: Sequence[str]) -> str:
    primary, msp = _split_msp(team_names)
    if primary and msp:
        return f"{', '.join(primary)} : {', '.join(msp)}"
    if primary:
        return ", ".join(primary)
    if msp:
        return ", ".join(msp)
    return "None"


def derive_user_scope(user: dict, fetch_df: FetchFn) -> UserScope:
    """
    Build the user scope (programs/teams) for the Welcome page.
    Logs failures and returns best-effort data instead of silently swallowing errors.
    """
    email = str(user.get("email") or "").strip()
    name = str(user.get("name") or user.get("display_name") or "").strip()
    role = str(user.get("role") or "").upper() or "VIEWER"
    owner_keys = _identity_candidates(user)

    scope = UserScope(email=email, name=name, role=role)

    # Membership-first scope resolution:
    # contributors/viewers are often granted access via APP_USER_MEMBERSHIP
    # rather than owner fields on PROGRAMS/TEAMS/APPLICATION_GROUPS.
    membership_program_ids: list[str] = []
    membership_programs: list[str] = []
    membership_team_ids: list[str] = []
    membership_teams: list[str] = []
    if email:
        try:
            dfm = fetch_df(
                """
                SELECT
                    NULLIF(LTRIM(RTRIM(m.PROGRAMID)), '') AS MEMBERSHIP_PROGRAMID,
                    NULLIF(LTRIM(RTRIM(m.TEAMID)), '') AS MEMBERSHIP_TEAMID,
                    COALESCE(
                      NULLIF(LTRIM(RTRIM(m.PROGRAMID)), ''),
                      NULLIF(LTRIM(RTRIM(t.PROGRAMID)), '')
                    ) AS PROGRAMID,
                    COALESCE(
                      NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''),
                      NULLIF(LTRIM(RTRIM(p.PROGRAMNAME)), ''),
                      NULLIF(LTRIM(RTRIM(p2.PROGRAM_DISPLAY_NAME)), ''),
                      NULLIF(LTRIM(RTRIM(p2.PROGRAMNAME)), '')
                    ) AS PROGRAMNAME,
                    COALESCE(
                      NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''),
                      NULLIF(LTRIM(RTRIM(t.TEAMNAME)), '')
                    ) AS TEAMNAME
                FROM APP_USER_MEMBERSHIP m
                LEFT JOIN TEAMS t
                  ON t.TEAMID = NULLIF(LTRIM(RTRIM(m.TEAMID)), '')
                LEFT JOIN PROGRAMS p
                  ON p.PROGRAMID = NULLIF(LTRIM(RTRIM(m.PROGRAMID)), '')
                LEFT JOIN PROGRAMS p2
                  ON p2.PROGRAMID = t.PROGRAMID
                WHERE UPPER(LTRIM(RTRIM(m.USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
                ORDER BY PROGRAMNAME, TEAMNAME
                """,
                (email,),
            )
            if dfm is not None and not dfm.empty:
                for _, row in dfm.iterrows():
                    pid = str(row.get("PROGRAMID") or row.get("MEMBERSHIP_PROGRAMID") or "").strip()
                    pname = str(row.get("PROGRAMNAME") or "").strip() or pid
                    tid = str(row.get("MEMBERSHIP_TEAMID") or "").strip()
                    tname = str(row.get("TEAMNAME") or "").strip() or tid
                    if pid:
                        membership_program_ids.append(pid)
                    if pname:
                        membership_programs.append(pname)
                    if tid:
                        membership_team_ids.append(tid)
                    if tname:
                        membership_teams.append(tname)
        except Exception:
            # Membership scope is optional. Fall back to ownership-based scope below.
            logger.exception("Failed to fetch APP_USER_MEMBERSHIP scope", extra={"user_email": email, "user_name": name})

    if membership_program_ids or membership_programs or membership_team_ids or membership_teams:
        scope.program_ids = _dedupe_keep_order(membership_program_ids)
        scope.programs = sorted(_dedupe_keep_order(membership_programs))
        unique_ordered = _dedupe_keep_order(membership_teams)
        primary, msp = _split_msp(unique_ordered)
        scope.teams = primary + msp

        # Resolve application groups from membership-bounded teams/programs.
        try:
            where_parts: list[str] = []
            params: list[str] = []
            if membership_team_ids:
                placeholders = ", ".join(["%s"] * len(membership_team_ids))
                where_parts.append(f"TEAMID IN ({placeholders})")
                params.extend(membership_team_ids)
            if scope.program_ids:
                placeholders = ", ".join(["%s"] * len(scope.program_ids))
                where_parts.append(f"PROGRAMID IN ({placeholders})")
                params.extend(scope.program_ids)
            if where_parts:
                dfg = fetch_df(
                    f"""
                    SELECT DISTINCT GROUPNAME
                    FROM APPLICATION_GROUPS
                    WHERE {' OR '.join(where_parts)}
                    ORDER BY GROUPNAME
                    """,
                    tuple(params),
                )
                if dfg is not None and not dfg.empty and "GROUPNAME" in dfg.columns:
                    scope.groups = sorted(
                        _dedupe_keep_order(dfg["GROUPNAME"].dropna().astype(str).tolist())
                    )
        except Exception:
            logger.exception(
                "Failed to fetch app groups for membership scope",
                extra={"user_email": email, "user_name": name},
            )
            scope.errors.append("Could not load applications for your account.")
        return scope

    # Programs managed by this user
    try:
        if owner_keys:
            owner_where, owner_params = _owner_match_clause("PROGRAMOWNER", owner_keys)
            dfp = fetch_df(
                f"""
                SELECT
                    PROGRAMID,
                    COALESCE(
                        NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''),
                        NULLIF(LTRIM(RTRIM(PROGRAMNAME)), '')
                    ) AS PROGRAMNAME
                FROM PROGRAMS
                WHERE {owner_where}
                ORDER BY PROGRAMNAME
                """,
                owner_params,
            )
        else:
            dfp = None
        if dfp is not None and not dfp.empty:
            scope.program_ids = [str(x) for x in dfp["PROGRAMID"].dropna().astype(str).tolist()]
            scope.programs = sorted(dfp["PROGRAMNAME"].dropna().astype(str).tolist())
    except Exception:
        logger.exception("Failed to fetch programs for user", extra={"user_email": email, "user_name": name})
        scope.errors.append("Could not load programs for your account.")

    # Teams managed by this user (or tied to their programs)
    try:
        teams_combined: list[str] = []
        if owner_keys:
            owner_where, owner_params = _owner_match_clause("PRODUCTOWNER", owner_keys)
            dft1 = fetch_df(
                f"SELECT TEAMNAME FROM TEAMS WHERE {owner_where}",
                owner_params,
            )
        else:
            dft1 = None
        if dft1 is not None and not dft1.empty:
            teams_combined += dft1["TEAMNAME"].dropna().astype(str).tolist()

        if scope.program_ids:
            placeholders = ", ".join(["%s"] * len(scope.program_ids))
            params: Sequence[str] = tuple(scope.program_ids)
            dft2 = fetch_df(f"SELECT TEAMNAME FROM TEAMS WHERE PROGRAMID IN ({placeholders})", params)
            if dft2 is not None and not dft2.empty and "TEAMNAME" in dft2.columns:
                teams_combined += dft2["TEAMNAME"].dropna().astype(str).tolist()

        unique_ordered = _dedupe_keep_order(teams_combined)
        primary, msp = _split_msp(unique_ordered)
        scope.teams = primary + msp
    except Exception:
        logger.exception("Failed to fetch teams for user", extra={"user_email": email, "user_name": name})
        scope.errors.append("Could not load teams for your account.")

    # If we have teams but no programs, derive programs from those teams' PROGRAMID links
    if scope.teams and not scope.programs:
        try:
            placeholders = ", ".join(["%s"] * len(scope.teams))
            dpp = fetch_df(
                f"""
                SELECT DISTINCT
                    COALESCE(
                        NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''),
                        NULLIF(LTRIM(RTRIM(p.PROGRAMNAME)), '')
                    ) AS PROGRAMNAME
                FROM TEAMS t
                JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                WHERE UPPER(t.TEAMNAME) IN ({placeholders})
                """,
                tuple([t.upper() for t in scope.teams]),
            )
            if dpp is not None and not dpp.empty and "PROGRAMNAME" in dpp.columns:
                scope.programs = sorted(dpp["PROGRAMNAME"].dropna().astype(str).tolist())
        except Exception:
            logger.exception("Failed to derive programs from teams", extra={"teams": scope.teams})
            scope.errors.append("Could not derive programs from your teams.")

    # Application-group ownership visibility for Product Owners.
    try:
        groups_combined: list[str] = []
        if owner_keys:
            owner_where, owner_params = _owner_match_clause("OWNER", owner_keys)
            dfg1 = fetch_df(
                f"""
                SELECT GROUPNAME
                FROM APPLICATION_GROUPS
                WHERE {owner_where}
                """,
                owner_params,
            )
            if dfg1 is not None and not dfg1.empty and "GROUPNAME" in dfg1.columns:
                groups_combined += dfg1["GROUPNAME"].dropna().astype(str).tolist()

        if scope.teams:
            placeholders = ", ".join(["%s"] * len(scope.teams))
            dfg2 = fetch_df(
                f"""
                SELECT DISTINCT g.GROUPNAME
                FROM APPLICATION_GROUPS g
                JOIN TEAMS t ON t.TEAMID = g.TEAMID
                WHERE UPPER(t.TEAMNAME) IN ({placeholders})
                """,
                tuple([t.upper() for t in scope.teams]),
            )
            if dfg2 is not None and not dfg2.empty and "GROUPNAME" in dfg2.columns:
                groups_combined += dfg2["GROUPNAME"].dropna().astype(str).tolist()
        scope.groups = sorted(_dedupe_keep_order(groups_combined))
    except Exception:
        logger.exception("Failed to fetch app groups for user scope", extra={"user_email": email, "user_name": name})
        scope.errors.append("Could not load applications for your account.")

    return scope
