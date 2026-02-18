from __future__ import annotations

from core.scope import PreviewScope, ROLE_PO, ROLE_PORTFOLIO, ROLE_PROGRAM


def resolve_driver_dimension(role: str, scope: PreviewScope) -> str:
    """Resolve the driver dimension lens for insights.

    Returns one of: "PROGRAM", "TEAM", "APP_GROUP".
    """
    role_norm = str(role or "").strip()
    if role_norm.upper() == "ADMIN":
        return "PROGRAM"
    if role_norm == ROLE_PO:
        return "APP_GROUP"
    if role_norm == ROLE_PROGRAM:
        return "TEAM"
    if role_norm == ROLE_PORTFOLIO:
        return "PROGRAM"

    # Fallback rules when role is unknown/ambiguous.
    if scope.groups:
        return "APP_GROUP"
    if len(scope.teams or []) >= 3:
        return "TEAM"
    if len(scope.programs or []) >= 2:
        return "PROGRAM"
    if len(scope.programs or []) == 1 and len(scope.teams or []) in (1, 2):
        return "APP_GROUP"
    return "PROGRAM"

