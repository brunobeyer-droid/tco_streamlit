from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Sequence, Set

import pandas as pd
import streamlit as st

FetchFn = Callable[[str, Optional[Sequence[Any]]], Optional[pd.DataFrame]]


def normalize_role(raw: Any, default: str = "VIEWER") -> str:
    role = str(raw or "").strip().upper()
    if role == "EDITOR":
        role = "CONTRIBUTOR"
    if role in {"ADMIN", "CONTRIBUTOR", "VIEWER"}:
        return role
    return str(default or "VIEWER").strip().upper() or "VIEWER"


@dataclass
class PrincipalContext:
    email: str
    display_name: str
    role: str
    user: dict = field(default_factory=dict)

    @property
    def is_admin(self) -> bool:
        return self.role == "ADMIN"

    @property
    def is_contributor(self) -> bool:
        return self.role == "CONTRIBUTOR"

    @property
    def is_viewer(self) -> bool:
        return self.role == "VIEWER"


@dataclass
class EffectiveScope:
    program_ids: Set[str] = field(default_factory=set)
    team_ids: Set[str] = field(default_factory=set)
    app_group_ids: Set[str] = field(default_factory=set)
    ownership_program_ids: Set[str] = field(default_factory=set)
    ownership_team_ids: Set[str] = field(default_factory=set)
    ownership_app_group_ids: Set[str] = field(default_factory=set)
    program_sources: Dict[str, Set[str]] = field(default_factory=dict)
    team_sources: Dict[str, Set[str]] = field(default_factory=dict)
    app_group_sources: Dict[str, Set[str]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def has_scope(self) -> bool:
        return bool(self.program_ids or self.team_ids or self.app_group_ids)


@dataclass
class AccessDecision:
    allowed: bool
    reason: str
    principal_role: str
    action: str
    resource_type: str
    resource_id: Optional[str] = None
    matched_scope_source: Optional[str] = None


SOURCE_AUTO_OWNER = "AUTO_OWNER"
SOURCE_MANUAL_OVERRIDE = "MANUAL_OVERRIDE"


_ACTION_MATRIX: Dict[str, Set[str]] = {
    "VIEWER": {"view"},
    "CONTRIBUTOR": {"view", "create", "edit", "map", "headcount_edit", "onboard"},
    "ADMIN": {"view", "create", "edit", "map", "headcount_edit", "onboard", "manage_access"},
}

_SCOPED_RESOURCE_TYPES = {"program", "team", "application", "app_group"}


def get_current_principal() -> PrincipalContext:
    auth_user = st.session_state.get("auth_user") or {}
    if not isinstance(auth_user, dict):
        auth_user = {}
    email = str(
        auth_user.get("email")
        or st.session_state.get("user_email")
        or st.session_state.get("auth_email")
        or ""
    ).strip().lower()
    display_name = str(
        auth_user.get("display_name")
        or auth_user.get("name")
        or st.session_state.get("auth_display_name")
        or email
    ).strip()
    role = normalize_role(
        st.session_state.get("role")
        or st.session_state.get("user_role")
        or auth_user.get("role")
        or auth_user.get("ROLE")
        or "VIEWER"
    )
    return PrincipalContext(email=email, display_name=display_name or email, role=role, user=auth_user)


def _owner_match_clause(column: str, owner_keys: Sequence[str]) -> tuple[str, tuple[Any, ...]]:
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


def _identity_candidates(principal: PrincipalContext) -> list[str]:
    vals = [
        str(principal.email or "").strip(),
        str(principal.display_name or "").strip(),
        str((principal.user or {}).get("name") or "").strip(),
        str((principal.user or {}).get("display_name") or "").strip(),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for raw in vals:
        if not raw:
            continue
        candidates = [raw]
        if "," in raw:
            parts = [p.strip() for p in raw.split(",", 1)]
            if len(parts) == 2 and parts[0] and parts[1]:
                candidates.append(f"{parts[1]} {parts[0]}")
        for c in candidates:
            key = str(c).strip().upper()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(str(c).strip())
    return out


def get_effective_scope(fetch_df: FetchFn, principal: PrincipalContext) -> EffectiveScope:
    if principal.is_admin:
        return EffectiveScope()
    scope = EffectiveScope()
    email = str(principal.email or "").strip()
    if not email:
        return scope

    try:
        df_mem = fetch_df(
            """
            SELECT
              NULLIF(LTRIM(RTRIM(PROGRAMID)), '') AS PROGRAMID,
              NULLIF(LTRIM(RTRIM(TEAMID)), '') AS TEAMID
            FROM APP_USER_MEMBERSHIP
            WHERE UPPER(LTRIM(RTRIM(USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
            """,
            (email,),
        )
        if isinstance(df_mem, pd.DataFrame) and not df_mem.empty:
            if "PROGRAMID" in df_mem.columns:
                mem_program_ids = {
                    str(v).strip()
                    for v in df_mem["PROGRAMID"].dropna().astype(str).tolist()
                    if str(v).strip()
                }
                scope.program_ids |= mem_program_ids
                _merge_sources_for_id(mem_program_ids, scope.program_sources, SOURCE_MANUAL_OVERRIDE)
            if "TEAMID" in df_mem.columns:
                mem_team_ids = {
                    str(v).strip()
                    for v in df_mem["TEAMID"].dropna().astype(str).tolist()
                    if str(v).strip()
                }
                scope.team_ids |= mem_team_ids
                _merge_sources_for_id(mem_team_ids, scope.team_sources, SOURCE_MANUAL_OVERRIDE)
    except Exception as ex:
        scope.errors.append(f"membership_lookup_failed:{ex}")

    try:
        if scope.program_ids:
            placeholders = ", ".join(["%s"] * len(scope.program_ids))
            df_prog_teams = fetch_df(
                f"SELECT TEAMID, PROGRAMID FROM TEAMS WHERE PROGRAMID IN ({placeholders})",
                tuple(sorted(scope.program_ids)),
            )
            if isinstance(df_prog_teams, pd.DataFrame) and not df_prog_teams.empty and "TEAMID" in df_prog_teams.columns:
                for _, row in df_prog_teams.iterrows():
                    tid = str(row.get("TEAMID") or "").strip()
                    pid = str(row.get("PROGRAMID") or "").strip()
                    if not tid:
                        continue
                    scope.team_ids.add(tid)
                    if pid:
                        srcs = set(scope.program_sources.get(pid) or set())
                        _add_sources(scope.team_sources, tid, *(srcs or {SOURCE_MANUAL_OVERRIDE}))
                    else:
                        _add_sources(scope.team_sources, tid, SOURCE_MANUAL_OVERRIDE)
    except Exception as ex:
        scope.errors.append(f"program_team_lookup_failed:{ex}")

    try:
        if scope.team_ids:
            placeholders = ", ".join(["%s"] * len(scope.team_ids))
            df_team_programs = fetch_df(
                f"SELECT TEAMID, PROGRAMID FROM TEAMS WHERE TEAMID IN ({placeholders})",
                tuple(sorted(scope.team_ids)),
            )
            if isinstance(df_team_programs, pd.DataFrame) and not df_team_programs.empty and "PROGRAMID" in df_team_programs.columns:
                for _, row in df_team_programs.iterrows():
                    pid = str(row.get("PROGRAMID") or "").strip()
                    tid = str(row.get("TEAMID") or "").strip()
                    if not pid:
                        continue
                    scope.program_ids.add(pid)
                    if tid:
                        srcs = set(scope.team_sources.get(tid) or set())
                        _add_sources(scope.program_sources, pid, *(srcs or {SOURCE_MANUAL_OVERRIDE}))
                    else:
                        _add_sources(scope.program_sources, pid, SOURCE_MANUAL_OVERRIDE)
    except Exception as ex:
        scope.errors.append(f"team_program_lookup_failed:{ex}")

    owner_keys = _identity_candidates(principal)
    if owner_keys:
        try:
            where_sql, params = _owner_match_clause("PROGRAMOWNER", owner_keys)
            df_prog_owner = fetch_df(
                f"SELECT PROGRAMID FROM PROGRAMS WHERE {where_sql}",
                params,
            )
            if isinstance(df_prog_owner, pd.DataFrame) and not df_prog_owner.empty and "PROGRAMID" in df_prog_owner.columns:
                owner_programs = {
                    str(v).strip()
                    for v in df_prog_owner["PROGRAMID"].dropna().astype(str).tolist()
                    if str(v).strip()
                }
                scope.ownership_program_ids |= owner_programs
                scope.program_ids |= owner_programs
                _merge_sources_for_id(owner_programs, scope.program_sources, SOURCE_AUTO_OWNER)
        except Exception as ex:
            scope.errors.append(f"owner_program_lookup_failed:{ex}")

        try:
            where_sql, params = _owner_match_clause("PRODUCTOWNER", owner_keys)
            df_team_owner = fetch_df(
                f"SELECT TEAMID, PROGRAMID FROM TEAMS WHERE {where_sql}",
                params,
            )
            if isinstance(df_team_owner, pd.DataFrame) and not df_team_owner.empty:
                if "TEAMID" in df_team_owner.columns:
                    owner_teams = {
                        str(v).strip()
                        for v in df_team_owner["TEAMID"].dropna().astype(str).tolist()
                        if str(v).strip()
                    }
                    scope.ownership_team_ids |= owner_teams
                    scope.team_ids |= owner_teams
                    _merge_sources_for_id(owner_teams, scope.team_sources, SOURCE_AUTO_OWNER)
                if "PROGRAMID" in df_team_owner.columns:
                    team_owner_programs = {
                        str(v).strip()
                        for v in df_team_owner["PROGRAMID"].dropna().astype(str).tolist()
                        if str(v).strip()
                    }
                    scope.program_ids |= team_owner_programs
                    _merge_sources_for_id(team_owner_programs, scope.program_sources, SOURCE_AUTO_OWNER)
        except Exception as ex:
            scope.errors.append(f"owner_team_lookup_failed:{ex}")

        try:
            where_sql, params = _owner_match_clause("OWNER", owner_keys)
            df_group_owner = fetch_df(
                f"SELECT GROUPID FROM APPLICATION_GROUPS WHERE {where_sql}",
                params,
            )
            if isinstance(df_group_owner, pd.DataFrame) and not df_group_owner.empty and "GROUPID" in df_group_owner.columns:
                owner_groups = {
                    str(v).strip()
                    for v in df_group_owner["GROUPID"].dropna().astype(str).tolist()
                    if str(v).strip()
                }
                scope.ownership_app_group_ids |= owner_groups
                scope.app_group_ids |= owner_groups
                _merge_sources_for_id(owner_groups, scope.app_group_sources, SOURCE_AUTO_OWNER)
        except Exception as ex:
            scope.errors.append(f"owner_group_lookup_failed:{ex}")

    try:
        where_parts: list[str] = []
        params: list[Any] = []
        if scope.team_ids:
            placeholders = ", ".join(["%s"] * len(scope.team_ids))
            where_parts.append(f"TEAMID IN ({placeholders})")
            params.extend(sorted(scope.team_ids))
        if scope.program_ids:
            placeholders = ", ".join(["%s"] * len(scope.program_ids))
            where_parts.append(f"PROGRAMID IN ({placeholders})")
            params.extend(sorted(scope.program_ids))
        if where_parts:
            df_groups = fetch_df(
                f"SELECT GROUPID, TEAMID, PROGRAMID FROM APPLICATION_GROUPS WHERE {' OR '.join(where_parts)}",
                tuple(params),
            )
            if isinstance(df_groups, pd.DataFrame) and not df_groups.empty and "GROUPID" in df_groups.columns:
                for _, row in df_groups.iterrows():
                    gid = str(row.get("GROUPID") or "").strip()
                    if not gid:
                        continue
                    scope.app_group_ids.add(gid)
                    tid = str(row.get("TEAMID") or "").strip()
                    pid = str(row.get("PROGRAMID") or "").strip()
                    combined = set()
                    if tid:
                        combined |= set(scope.team_sources.get(tid) or set())
                    if pid:
                        combined |= set(scope.program_sources.get(pid) or set())
                    _add_sources(
                        scope.app_group_sources,
                        gid,
                        *(combined or {SOURCE_MANUAL_OVERRIDE}),
                    )
    except Exception as ex:
        scope.errors.append(f"scope_group_lookup_failed:{ex}")

    return scope


def _scope_ids_for_resource(scope: EffectiveScope, resource_type: str) -> Set[str]:
    rt = str(resource_type or "").strip().lower()
    if rt == "program":
        return set(scope.program_ids)
    if rt == "team":
        return set(scope.team_ids)
    if rt in {"application", "app_group"}:
        return set(scope.app_group_ids)
    return set()


def _scope_sources_for_resource(scope: EffectiveScope, resource_type: str) -> Dict[str, Set[str]]:
    rt = str(resource_type or "").strip().lower()
    if rt == "program":
        return scope.program_sources
    if rt == "team":
        return scope.team_sources
    if rt in {"application", "app_group"}:
        return scope.app_group_sources
    return {}


def _add_sources(target: Dict[str, Set[str]], resource_id: str, *sources: str) -> None:
    rid = str(resource_id or "").strip()
    if not rid:
        return
    if rid not in target:
        target[rid] = set()
    for src in sources:
        s = str(src or "").strip().upper()
        if s:
            target[rid].add(s)


def _merge_sources_for_id(
    resource_ids: Set[str],
    source_map: Dict[str, Set[str]],
    default_source: str,
) -> None:
    for rid in resource_ids:
        _add_sources(source_map, rid, default_source)


def source_label_for_id(scope: Optional[EffectiveScope], resource_type: str, resource_id: Optional[str]) -> Optional[str]:
    if scope is None:
        return None
    rid = str(resource_id or "").strip()
    if not rid:
        return None
    src_map = _scope_sources_for_resource(scope, resource_type)
    srcs = set(src_map.get(rid) or set())
    if not srcs:
        return None
    ordered = sorted(srcs)
    return "+".join(ordered)


def build_name_to_id_map(
    df: Optional[pd.DataFrame],
    *,
    id_col: str,
    primary_name_col: str,
    fallback_name_col: Optional[str] = None,
) -> tuple[Dict[str, str], Dict[str, int]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return {}, {}
    out: Dict[str, str] = {}
    counts: Dict[str, int] = defaultdict(int)
    for _, row in df.iterrows():
        rid = str(row.get(id_col) or "").strip()
        if not rid:
            continue
        name = str(row.get(primary_name_col) or "").strip()
        if not name and fallback_name_col:
            name = str(row.get(fallback_name_col) or "").strip()
        if not name:
            name = rid
        counts[name] += 1
        if name not in out:
            out[name] = rid
    return out, dict(counts)


def role_action_matrix_df() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    ordered_actions = ["view", "onboard", "edit", "map", "headcount_edit", "manage_access"]
    for action in ordered_actions:
        row = {"Action": action}
        for role in ["VIEWER", "CONTRIBUTOR", "ADMIN"]:
            allowed = action in _ACTION_MATRIX.get(role, set())
            if not allowed:
                row[role] = "Denied"
            elif role == "CONTRIBUTOR" and action in {"onboard", "edit", "map", "headcount_edit"}:
                row[role] = "Scoped"
            else:
                row[role] = "Allowed"
        rows.append(row)
    return pd.DataFrame(rows)


def can(
    principal: PrincipalContext,
    action: str,
    resource_type: str,
    resource_id: Optional[str] = None,
    scope: Optional[EffectiveScope] = None,
) -> bool:
    role = normalize_role(principal.role)
    act = str(action or "").strip().lower()
    rt = str(resource_type or "").strip().lower()
    if principal.is_admin:
        return True

    allowed_actions = _ACTION_MATRIX.get(role, {"view"})
    if act not in allowed_actions:
        return False

    if role != "CONTRIBUTOR":
        return act == "view"

    if rt not in _SCOPED_RESOURCE_TYPES:
        return True
    if scope is None:
        return False
    scope_ids = _scope_ids_for_resource(scope, rt)
    rid = str(resource_id or "").strip()
    if rid:
        return rid in scope_ids
    if rt in {"application", "app_group"} and act in {"create", "edit", "map", "headcount_edit", "onboard"}:
        # App operations can be initiated from program/team scope even before an app group exists.
        return bool(scope.app_group_ids or scope.team_ids or scope.program_ids)
    return bool(scope_ids) if act in {"create", "edit", "map", "headcount_edit", "onboard"} else True


def can_view_page(principal: PrincipalContext, page_key: str) -> bool:
    role = normalize_role(principal.role)
    key = str(page_key or "").strip().lower()
    if role == "ADMIN":
        return True
    if key in {"admin", "99_admin.py", "pages/99_admin.py"}:
        return False
    return True


def filter_df_by_scope(
    df: Optional[pd.DataFrame],
    resource_type: str,
    id_col: str,
    principal: PrincipalContext,
    scope: Optional[EffectiveScope],
) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    if principal.is_admin:
        return df.copy()
    if not principal.is_contributor:
        return df.copy()
    if scope is None:
        return df.iloc[0:0].copy()
    ids = _scope_ids_for_resource(scope, resource_type)
    if not ids:
        return df.iloc[0:0].copy()
    if id_col not in df.columns:
        return df.iloc[0:0].copy()
    out = df.copy()
    keep = out[id_col].fillna("").astype(str).str.strip().isin(ids)
    return out.loc[keep].copy()


def filter_application_groups_by_scope(
    df: Optional[pd.DataFrame],
    *,
    principal: PrincipalContext,
    scope: Optional[EffectiveScope],
    group_id_col: str = "GROUPID",
    team_id_col: str = "TEAMID",
    program_id_col: str = "PROGRAMID",
) -> pd.DataFrame:
    """Filter application groups for contributors using hierarchical scope fallback."""
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    if principal.is_admin:
        return df.copy()
    if not principal.is_contributor:
        return df.copy()
    if scope is None:
        return df.iloc[0:0].copy()

    out = df.copy()
    if group_id_col in out.columns and scope.app_group_ids:
        keep = out[group_id_col].fillna("").astype(str).str.strip().isin(set(scope.app_group_ids))
        return out.loc[keep].copy()
    if team_id_col in out.columns and scope.team_ids:
        keep = out[team_id_col].fillna("").astype(str).str.strip().isin(set(scope.team_ids))
        return out.loc[keep].copy()
    if program_id_col in out.columns and scope.program_ids:
        keep = out[program_id_col].fillna("").astype(str).str.strip().isin(set(scope.program_ids))
        return out.loc[keep].copy()
    return out.iloc[0:0].copy()


def explain_access(
    principal: PrincipalContext,
    action: str,
    resource_type: str,
    resource_id: Optional[str] = None,
    scope: Optional[EffectiveScope] = None,
) -> AccessDecision:
    allowed = can(principal, action, resource_type, resource_id=resource_id, scope=scope)
    rid = str(resource_id or "").strip() or None
    matched_scope_source = source_label_for_id(scope, resource_type, rid)
    if matched_scope_source is None and allowed and scope is not None and rid is None:
        src_map = _scope_sources_for_resource(scope, resource_type)
        all_srcs: Set[str] = set()
        for srcs in src_map.values():
            all_srcs |= set(srcs or set())
        if all_srcs:
            matched_scope_source = "+".join(sorted(all_srcs))
    if allowed:
        reason = "allowed"
    elif principal.is_admin:
        reason = "unexpected_admin_deny"
    elif not can(principal, action, "program", scope=scope) and action != "view":
        reason = "role_action_denied"
    elif str(resource_type or "").strip().lower() in _SCOPED_RESOURCE_TYPES and rid:
        reason = "out_of_scope"
    elif str(resource_type or "").strip().lower() in _SCOPED_RESOURCE_TYPES:
        reason = "missing_scope"
    else:
        reason = "denied"
    return AccessDecision(
        allowed=allowed,
        reason=reason,
        principal_role=normalize_role(principal.role),
        action=str(action or "").strip().lower(),
        resource_type=str(resource_type or "").strip().lower(),
        resource_id=rid,
        matched_scope_source=matched_scope_source,
    )
