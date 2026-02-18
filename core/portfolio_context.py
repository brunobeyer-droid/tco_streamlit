from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # type: ignore


@dataclass
class PortfolioContext:
    portfolio_key: str
    profile_key: str
    db_server: str
    db_database: str
    db_user: str = ""
    db_password: str = ""
    db_driver: str = ""
    db_encrypt: bool = True
    db_trust_server_certificate: bool = False
    db_schema: str = "dbo"
    role: str = ""
    pat_env_key: str = ""


def _ctx_raw() -> Dict[str, Any]:
    if st is None:
        return {}
    st.session_state.setdefault("portfolio_ctx", {})
    return st.session_state["portfolio_ctx"]


def get_portfolio_ctx() -> Optional[PortfolioContext]:
    raw = _ctx_raw()
    if not raw:
        return None
    try:
        return PortfolioContext(**raw)
    except Exception:
        return None


def set_portfolio_ctx(ctx: PortfolioContext) -> None:
    if st is None:
        return
    st.session_state["portfolio_ctx"] = asdict(ctx)


def clear_portfolio_ctx() -> None:
    if st is None:
        return
    st.session_state.pop("portfolio_ctx", None)


def set_active_portfolio_context(
    portfolio_key: str,
    profile_key: str,
    db_config: Dict[str, Any],
    role: str = "",
    pat_env_key: str = "",
) -> None:
    if st is None:
        return
    ctx = PortfolioContext(
        portfolio_key=str(portfolio_key or "").strip(),
        profile_key=str(profile_key or "").strip(),
        db_server=str((db_config or {}).get("server") or "").strip(),
        db_database=str((db_config or {}).get("database") or "").strip(),
        db_user=str((db_config or {}).get("user") or "").strip(),
        db_password=str((db_config or {}).get("password") or "").strip(),
        db_driver=str((db_config or {}).get("driver") or "").strip(),
        db_encrypt=bool((db_config or {}).get("encrypt", True)),
        db_trust_server_certificate=bool((db_config or {}).get("trust_server_certificate", False)),
        db_schema=str((db_config or {}).get("schema") or "dbo").strip() or "dbo",
        role=str(role or "").strip(),
        pat_env_key=str(pat_env_key or "").strip(),
    )
    set_portfolio_ctx(ctx)


def get_active_portfolio_key() -> str:
    ctx = get_portfolio_ctx()
    return str(ctx.portfolio_key).strip() if ctx else ""


def get_active_profile_key() -> str:
    ctx = get_portfolio_ctx()
    return str(ctx.profile_key).strip() if ctx else ""


def get_active_db_config() -> Optional[Dict[str, Any]]:
    ctx = get_portfolio_ctx()
    if ctx is None:
        return None
    return {
        "server": ctx.db_server,
        "database": ctx.db_database,
        "user": ctx.db_user,
        "password": ctx.db_password,
        "driver": ctx.db_driver,
        "encrypt": ctx.db_encrypt,
        "trust_server_certificate": ctx.db_trust_server_certificate,
        "schema": ctx.db_schema,
    }


def get_active_role() -> str:
    ctx = get_portfolio_ctx()
    return str(ctx.role).strip() if ctx else ""


def get_active_pat_env_key() -> str:
    ctx = get_portfolio_ctx()
    return str(ctx.pat_env_key).strip() if ctx else ""
