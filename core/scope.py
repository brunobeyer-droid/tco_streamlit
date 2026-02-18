from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from welcome.state import UserScope, derive_user_scope


@dataclass
class PreviewScope:
    user: dict
    programs: List[str]
    teams: List[str]
    groups: List[str]
    program_ids: List[str]
    user_scope: Optional[UserScope] = None
    inferred_role: Optional[str] = None


ROLE_PORTFOLIO = "Portfolio Manager"
ROLE_PROGRAM = "Program Manager"
ROLE_PO = "Product Owner"
ROLE_UNKNOWN = "Unknown"


def _session_list(*keys: str) -> List[str]:
    for key in keys:
        val = st.session_state.get(key)
        if isinstance(val, (list, tuple)) and val:
            return [str(v) for v in val if str(v).strip()]
    return []


def read_scope_from_session(fetch_df) -> PreviewScope:
    user = st.session_state.get("auth_user") or {}
    try:
        from utils.auth import get_scope_user_for_filters

        user = get_scope_user_for_filters() or user
    except Exception:
        pass
    scope = derive_user_scope(user, fetch_df) if user else None
    # Keep manual page-level selections if users explicitly set them.
    programs = _session_list("flt_programs", "ins_flt_programs", "ui_programs", "ins_ui_programs")
    teams = _session_list("flt_teams", "ins_flt_teams", "ui_teams", "ins_ui_teams")
    groups = _session_list("flt_groups", "ins_flt_groups", "ui_groups", "ins_ui_groups")
    program_ids = scope.program_ids if scope else []

    preview = PreviewScope(
        user=user,
        programs=programs,
        teams=teams,
        groups=groups,
        program_ids=program_ids,
        user_scope=scope,
    )
    explicit_role = infer_role_from_user(user)
    role_probe = PreviewScope(
        user=user,
        programs=(programs or (list(scope.programs) if scope else [])),
        teams=(teams or (list(scope.teams) if scope else [])),
        groups=(groups or (list(scope.groups) if scope else [])),
        program_ids=program_ids,
        user_scope=scope,
    )
    inferred_role = explicit_role or infer_role(role_probe)

    # Default scope policy for core pages:
    # - Never auto-select app groups.
    # - Product Owner: constrain to owned program(s) + team(s).
    # - Program Manager/Owner: constrain to owned program(s) only.
    # - Unassigned: no default filters (portfolio-wide).
    if scope:
        scope_programs = list(scope.programs or [])
        scope_teams = list(scope.teams or [])
        if not programs:
            if inferred_role == ROLE_PROGRAM:
                programs = scope_programs
            elif inferred_role == ROLE_PO:
                programs = scope_programs
        if not teams:
            if inferred_role == ROLE_PO:
                teams = scope_teams
            elif inferred_role == ROLE_PROGRAM:
                teams = []
        # App groups should never be auto-selected by default scope.
        if not groups:
            groups = []

    preview.programs = programs
    preview.teams = teams
    preview.groups = groups
    preview.inferred_role = inferred_role
    return preview


def infer_role_from_user(user: dict) -> Optional[str]:
    """Return an explicit role from auth user info when available; otherwise None."""
    if not isinstance(user, dict) or not user:
        return None
    raw = user.get("role") or user.get("ROLE") or user.get("app_role") or user.get("APP_ROLE")
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    key = s.upper().replace("-", "_").replace(" ", "_")

    # Preserve explicit admin role when present.
    if key == "ADMIN":
        return "ADMIN"

    # Map common variants to stable role labels.
    if key in {"PORTFOLIO_MANAGER", "PORTFOLIO", "EXEC", "EXECUTIVE"}:
        return ROLE_PORTFOLIO
    if key in {"PROGRAM_MANAGER", "PROGRAM"}:
        return ROLE_PROGRAM
    if key in {"PRODUCT_OWNER", "PO", "TEAM_OWNER"}:
        return ROLE_PO
    return None


def infer_role(scope: PreviewScope) -> str:
    programs = len(scope.programs)
    teams = len(scope.teams)
    groups = len(scope.groups)

    if programs == 0 and teams == 0 and groups == 0:
        # Unscoped users should default to portfolio-wide view.
        return ROLE_PORTFOLIO
    if programs > 1:
        return ROLE_PORTFOLIO
    # Product-owner-like scopes should win over single-program heuristics:
    # one owned team (with or without explicit groups) behaves as PO.
    if groups:
        return ROLE_PO
    if teams == 1:
        return ROLE_PO
    if programs == 1 and teams >= 2:
        return ROLE_PROGRAM
    if teams >= 2:
        return ROLE_PROGRAM
    if programs >= 1:
        return ROLE_PROGRAM
    return ROLE_UNKNOWN


def scope_label(scope: PreviewScope, role: str) -> str:
    if not scope.programs and not scope.teams and not scope.groups:
        return "Viewing: Portfolio"
    if role == ROLE_PORTFOLIO:
        return "Viewing: Portfolio"
    if role == ROLE_PROGRAM:
        if scope.programs:
            if len(scope.programs) == 1:
                return f"Viewing: Program {scope.programs[0]}"
            return f"Viewing: {len(scope.programs)} programs"
        if scope.teams:
            if len(scope.teams) == 1:
                return f"Viewing: Team {scope.teams[0]}"
            return f"Viewing: {len(scope.teams)} teams"
    if role == ROLE_PO:
        if scope.groups:
            if len(scope.groups) == 1:
                return f"Viewing: App group {scope.groups[0]}"
            return f"Viewing: {len(scope.groups)} app groups"
    if scope.groups:
        return f"Viewing: {len(scope.groups)} app groups"
    if scope.teams:
        if len(scope.teams) == 1:
            return f"Viewing: Team {scope.teams[0]}"
        return f"Viewing: {len(scope.teams)} teams"
    if scope.programs:
        if len(scope.programs) == 1:
            return f"Viewing: Program {scope.programs[0]}"
        return f"Viewing: {len(scope.programs)} programs"
    return f"Viewing: {role}"
