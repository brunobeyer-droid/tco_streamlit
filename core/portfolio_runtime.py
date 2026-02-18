from __future__ import annotations

from typing import Any, Dict, List, Optional

import time

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # type: ignore

from utils.query_params import get_qp, set_qp, clear_qp
from core.portfolio_context import set_active_portfolio_context
from core.db_passwords import resolve_portfolio_db_password
from db.mssql_backend import get_default_db_config_dict

def get_active_portfolio_db_cfg() -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    if st is None:
        return None, "streamlit_not_available"
    active_key = str(st.session_state.get("active_portfolio_key") or "").strip()
    if not active_key:
        return None, "no_active_portfolio"
    try:
        from db.control_db import control_db_available, control_fetch_df
    except Exception:
        return None, "control_db_unavailable"
    if not control_db_available():
        return None, "control_db_unavailable"
    try:
        df = control_fetch_df(
            "SELECT * FROM CONTROL_PORTFOLIOS WHERE PORTFOLIO_KEY = %s",
            (active_key,),
        )
    except Exception:
        return None, "control_db_query_failed"
    if df is None or df.empty:
        return None, "portfolio_not_found"
    row = df.iloc[0]
    password_key = str(row.get("DB_PASSWORD_KEY") or "").strip()
    db_password = resolve_portfolio_db_password(
        password_key,
        str(row.get("PORTFOLIO_KEY") or "").strip(),
        str(row.get("PROFILE_KEY") or "").strip(),
    ) or ""
    db_cfg = {
        "server": str(row.get("DB_SERVER") or "").strip(),
        "database": str(row.get("DB_DATABASE") or "").strip(),
        "user": str(row.get("DB_USER") or "").strip(),
        "password": db_password,
        "driver": str(row.get("DB_DRIVER") or "ODBC Driver 18 for SQL Server").strip(),
        "encrypt": bool(row.get("DB_ENCRYPT")) if row.get("DB_ENCRYPT") is not None else True,
        "trust_server_certificate": bool(row.get("DB_TRUST_SERVER_CERTIFICATE"))
        if row.get("DB_TRUST_SERVER_CERTIFICATE") is not None
        else False,
        "schema": str(row.get("DB_SCHEMA") or "dbo").strip() or "dbo",
    }
    missing = [k for k in ("server", "database", "user", "password", "driver") if not db_cfg.get(k)]
    if missing:
        err = f"portfolio_connection_incomplete:{','.join(missing)}"
        st.session_state["_portfolio_db_error"] = err
        st.session_state["_portfolio_db_error_details"] = {
            "portfolio_key": active_key,
            "server": db_cfg.get("server"),
            "database": db_cfg.get("database"),
            "user": db_cfg.get("user"),
            "missing": missing,
        }
        return None, err
    st.session_state.pop("_portfolio_db_error", None)
    st.session_state.pop("_portfolio_db_error_details", None)
    st.session_state["_active_portfolio_db_cfg"] = db_cfg
    return db_cfg, None

def _clear_portfolio_caches_once(key: str) -> None:
    if st is None:
        return
    k = str(key or "").strip()
    if not k:
        return
    if st.session_state.get("_portfolio_cache_key") == k:
        return
    try:
        st.cache_data.clear()
        st.cache_resource.clear()
    except Exception:
        pass
    try:
        from db.mssql_backend import _reset_connection_cache
        _reset_connection_cache()
    except Exception:
        pass
    st.session_state["_portfolio_cache_key"] = k


def _get_query_params() -> Dict[str, Any]:
    if st is None:
        return {}
    return get_qp()


def _update_query_params(**updates: Optional[str]) -> None:
    if st is None:
        return
    set_qp(**updates)


def get_active_portfolio_key() -> Optional[str]:
    if st is None:
        return None
    key = st.session_state.get("active_portfolio_key")
    if key:
        return key
    qp_key = str(_get_query_params().get("portfolio") or "").strip()
    if qp_key:
        is_admin = False
        try:
            role = (
                st.session_state.get("role")
                or st.session_state.get("user_role")
                or (st.session_state.get("auth_user") or {}).get("role")
                or ""
            )
            is_admin = str(role or "").strip().upper() == "ADMIN"
        except Exception:
            is_admin = False
        if not is_admin:
            try:
                user_email = (
                    (st.session_state.get("auth_user") or {}).get("email")
                    or st.session_state.get("user_email")
                    or ""
                )
                user_role = (
                    (st.session_state.get("auth_user") or {}).get("role")
                    or st.session_state.get("user_role")
                    or ""
                )
                accessible = user_accessible_portfolios(str(user_email or ""), str(user_role or ""))
                accessible_keys = {
                    str(p.get("portfolio_key") or "").strip()
                    for p in (accessible or [])
                    if isinstance(p, dict)
                }
            except Exception:
                accessible_keys = None
            if accessible_keys:
                if qp_key not in accessible_keys:
                    clear_qp("portfolio")
                    return None
            else:
                # Unknown access state (e.g., control DB unavailable); don't clear or set.
                st.session_state["_portfolio_access_unknown"] = True
                return None
        st.session_state.pop("_portfolio_access_unknown", None)
        _clear_portfolio_caches_once(qp_key)
        st.session_state["active_portfolio_key"] = qp_key
        return qp_key
    return None


def _set_active_display(name: str) -> None:
    if st is None:
        return
    st.session_state["active_portfolio_display_name"] = name


def set_active_portfolio(portfolio_key: str, email: str = "", role: str = "") -> None:
    if st is None:
        return
    key = str(portfolio_key or "").strip()
    prev_key = st.session_state.get("active_portfolio_key")
    st.session_state["active_portfolio_key"] = key
    st.session_state.pop("_app_block_reason", None)
    st.session_state.pop("_portfolio_access_unknown", None)
    if key and key != prev_key:
        _clear_portfolio_caches_once(key)
    # Persist selection across refresh/deep links.
    if key:
        try:
            set_qp(portfolio=key)
        except Exception as e:
            # Don't fail the app for URL persistence; surface the issue in debug mode.
            try:
                if st.session_state.get("debug_mode") is True:
                    st.error(f"Failed to persist portfolio in URL query params: {e}")
            except Exception:
                pass

    # Resolve portfolio details from control DB when available.
    try:
        from db.control_db import control_db_available, control_fetch_df
        if control_db_available() and key:
            user_email = email or (
                st.session_state.get("auth_user", {}).get("email")
                or st.session_state.get("user_email")
                or ""
            )
            df = control_fetch_df(
                """
                SELECT p.*, u.ROLE
                FROM CONTROL_PORTFOLIOS p
                LEFT JOIN CONTROL_PORTFOLIO_USERS u
                  ON u.PORTFOLIO_KEY = p.PORTFOLIO_KEY
                 AND UPPER(LTRIM(RTRIM(u.USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
                WHERE p.PORTFOLIO_KEY = %s
                """,
                (user_email, key),
            )
            if df is not None and not df.empty:
                row = df.iloc[0]
                display = str(row.get("DISPLAY_NAME") or key)
                _set_active_display(display)
                password_key = str(row.get("DB_PASSWORD_KEY") or "").strip()
                db_password = resolve_portfolio_db_password(
                    password_key,
                    str(row.get("PORTFOLIO_KEY") or key).strip(),
                    str(row.get("PROFILE_KEY") or "").strip(),
                ) or ""
                db_cfg = {
                    "server": str(row.get("DB_SERVER") or "").strip(),
                    "database": str(row.get("DB_DATABASE") or "").strip(),
                    "user": str(row.get("DB_USER") or "").strip(),
                    "password": db_password,
                    "driver": str(row.get("DB_DRIVER") or "ODBC Driver 18 for SQL Server").strip(),
                    "encrypt": bool(row.get("DB_ENCRYPT")) if row.get("DB_ENCRYPT") is not None else True,
                    "trust_server_certificate": bool(row.get("DB_TRUST_SERVER_CERTIFICATE")) if row.get("DB_TRUST_SERVER_CERTIFICATE") is not None else False,
                    "schema": str(row.get("DB_SCHEMA") or "dbo").strip() or "dbo",
                }
                missing = [k for k in ("server", "database", "user", "password", "driver") if not db_cfg.get(k)]
                if missing:
                    st.session_state["_app_block_reason"] = "PORTFOLIO_DB_INCOMPLETE"
                    st.session_state["_portfolio_db_error"] = "Portfolio DB connection is incomplete."
                    st.session_state["_portfolio_db_error_details"] = {
                        "portfolio_key": key,
                        "server": db_cfg.get("server"),
                        "database": db_cfg.get("database"),
                        "user": db_cfg.get("user"),
                        "missing": missing,
                    }
                    return
                profile_key = str(row.get("PROFILE_KEY") or "").strip()
                role = str(role or row.get("ROLE") or "").strip()
                pat_env_key = str(row.get("PAT_ENV_KEY") or "").strip()
                set_active_portfolio_context(key, profile_key, db_cfg, role=role, pat_env_key=pat_env_key)
                if st.session_state.get("_portfolio_conn_tested_key") != key:
                    try:
                        from db.mssql_backend import MssqlConfig, fetch_df
                        cfg = MssqlConfig(
                            server=db_cfg["server"],
                            database=db_cfg["database"],
                            user=db_cfg["user"],
                            password=db_cfg["password"],
                            driver=db_cfg["driver"],
                            encrypt="yes" if db_cfg.get("encrypt", True) else "no",
                            trust_server_certificate="yes" if db_cfg.get("trust_server_certificate", False) else "no",
                            schema=db_cfg.get("schema") or "dbo",
                        )
                        fetch_df("SELECT 1 AS OK", cfg=cfg)
                        st.session_state["_portfolio_conn_tested_key"] = key
                        st.session_state.pop("_portfolio_db_error", None)
                        st.session_state.pop("_portfolio_db_error_details", None)
                    except Exception as e:
                        st.session_state["_portfolio_db_error"] = "Portfolio DB connection failed."
                        st.session_state["_portfolio_db_error_details"] = {
                            "portfolio_key": key,
                            "server": db_cfg.get("server"),
                            "database": db_cfg.get("database"),
                            "user": db_cfg.get("user"),
                            "error": str(e),
                        }
                        st.session_state["_app_block_reason"] = "PORTFOLIO_DB_CONNECT_FAILED"
                        return
                return
    except Exception:
        pass

    # Fallback: default DB config only.
    try:
        from db.control_db import control_db_available
        if control_db_available() and key:
            st.session_state["_app_block_reason"] = "PORTFOLIO_DB_INCOMPLETE"
            st.session_state["_portfolio_db_error"] = "Portfolio DB connection details not found in Control DB."
            st.session_state["_portfolio_db_error_details"] = {"portfolio_key": key}
            return
    except Exception:
        pass
    default_cfg = get_default_db_config_dict()
    set_active_portfolio_context(key or "DEFAULT", "", default_cfg, role=role or "")
    _set_active_display(key or "Default")


def clear_portfolio_caches_and_rerun() -> None:
    if st is None:
        return
    key = str(st.session_state.get("active_portfolio_key") or "").strip()
    _clear_portfolio_caches_once(key)
    st.session_state["portfolio_changed_ts"] = time.time()
    st.rerun()


def get_user_portfolios_for_sidebar(user_email: str, user_role: str) -> List[Dict[str, Any]]:
    user_email = str(user_email or "").strip()
    role = str(user_role or "").strip().upper()
    include_onboarding = role == "ADMIN"
    # A) Control DB
    try:
        from db.control_db import control_db_available, control_fetch_df, ensure_control_schema
        if control_db_available():
            status = ensure_control_schema()
            if status.get("ok"):
                status_filter = "('ACTIVE','ONBOARDING')" if include_onboarding else "('ACTIVE')"

                def _load_control_portfolios(email_key: str, role_key: str, status_filter_key: str):
                    if role_key == "ADMIN":
                        return control_fetch_df(
                            """
                            SELECT p.PORTFOLIO_KEY, p.DISPLAY_NAME
                            FROM CONTROL_PORTFOLIOS p
                            WHERE COALESCE(p.STATUS,'ACTIVE') IN """ + status_filter_key + """
                            ORDER BY p.DISPLAY_NAME
                            """,
                            None,
                        )
                    if email_key:
                        return control_fetch_df(
                            """
                            SELECT p.PORTFOLIO_KEY, p.DISPLAY_NAME
                            FROM CONTROL_PORTFOLIOS p
                            JOIN CONTROL_PORTFOLIO_USERS u
                              ON LOWER(LTRIM(RTRIM(u.PORTFOLIO_KEY))) = LOWER(LTRIM(RTRIM(p.PORTFOLIO_KEY)))
                            WHERE LOWER(LTRIM(RTRIM(u.USER_EMAIL))) = LOWER(LTRIM(RTRIM(%s)))
                              AND COALESCE(p.STATUS,'ACTIVE') IN """ + status_filter_key + """
                            ORDER BY p.DISPLAY_NAME
                            """,
                            (email_key,),
                        )
                    return None

                if st is not None:
                    try:
                        _load_control_portfolios = st.cache_data(ttl=30, show_spinner=False)(_load_control_portfolios)
                    except Exception:
                        pass

                df = _load_control_portfolios(user_email, role, status_filter)
                if df is not None and not df.empty:
                    return [
                        {
                            "portfolio_key": str(r.get("PORTFOLIO_KEY") or ""),
                            "display_name": str(r.get("DISPLAY_NAME") or r.get("PORTFOLIO_KEY") or ""),
                        }
                        for _, r in df.iterrows()
                    ]
    except Exception:
        pass

    # B) Local DB access control (best-effort)
    try:
        from core.db_session import db_fetch_df
        df = db_fetch_df(
            """
            SELECT p.PORTFOLIO_KEY, p.DISPLAY_NAME
            FROM CONTROL_PORTFOLIOS p
            JOIN CONTROL_PORTFOLIO_USERS u
              ON LOWER(LTRIM(RTRIM(u.PORTFOLIO_KEY))) = LOWER(LTRIM(RTRIM(p.PORTFOLIO_KEY)))
            WHERE LOWER(LTRIM(RTRIM(u.USER_EMAIL))) = LOWER(LTRIM(RTRIM(%s)))
              AND COALESCE(p.STATUS,'ACTIVE') = 'ACTIVE'
            """,
            (user_email,),
        )
        if df is not None and not df.empty:
            return [
                {
                    "portfolio_key": str(r.get("PORTFOLIO_KEY") or ""),
                    "display_name": str(r.get("DISPLAY_NAME") or r.get("PORTFOLIO_KEY") or ""),
                }
                for _, r in df.iterrows()
            ]
    except Exception:
        pass

    # C) Secrets fallback
    if st is not None:
        try:
            portfolios = st.secrets.get("portfolios", [])  # type: ignore[attr-defined]
            if isinstance(portfolios, list):
                out = []
                for p in portfolios:
                    if isinstance(p, dict) and p.get("portfolio_key"):
                        out.append(
                            {
                                "portfolio_key": str(p.get("portfolio_key")),
                                "display_name": str(p.get("display_name") or p.get("portfolio_key")),
                            }
                        )
                return out
        except Exception:
            pass
    return []


def user_accessible_portfolios(user_email: str, user_role: str) -> List[Dict[str, Any]]:
    return get_user_portfolios_for_sidebar(user_email, user_role)


def get_portfolio_by_key(portfolio_key: str) -> Optional[Dict[str, Any]]:
    key = str(portfolio_key or "").strip()
    if not key:
        return None
    try:
        from db.control_db import control_db_available, control_fetch_df, ensure_control_schema
        if control_db_available():
            status = ensure_control_schema()
            if status.get("ok"):
                df = control_fetch_df(
                    """
                    SELECT *
                    FROM CONTROL_PORTFOLIOS
                    WHERE PORTFOLIO_KEY = %s
                    """,
                    (key,),
                )
                if df is not None and not df.empty:
                    row = df.iloc[0].to_dict()
                    return {
                        "portfolio_key": str(row.get("PORTFOLIO_KEY") or key),
                        "display_name": str(row.get("DISPLAY_NAME") or key),
                        "db_server": row.get("DB_SERVER"),
                        "db_database": row.get("DB_DATABASE"),
                        "db_schema": row.get("DB_SCHEMA"),
                        "db_user": row.get("DB_USER"),
                        "db_password_key": row.get("DB_PASSWORD_KEY"),
                        "db_driver": row.get("DB_DRIVER"),
                        "db_encrypt": row.get("DB_ENCRYPT"),
                        "db_trust_server_certificate": row.get("DB_TRUST_SERVER_CERTIFICATE"),
                        "profile_key": row.get("PROFILE_KEY"),
                        "pat_env_key": row.get("PAT_ENV_KEY"),
                    }
    except Exception:
        pass
    return {"portfolio_key": key, "display_name": key}
