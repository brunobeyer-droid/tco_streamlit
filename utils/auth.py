from __future__ import annotations
import time
import os
from pathlib import Path
import re
import uuid
from typing import Optional, Dict, Any, Tuple
import streamlit as st
try:
    from streamlit.runtime.scriptrunner import get_script_run_ctx
except Exception:  # pragma: no cover
    get_script_run_ctx = None  # type: ignore

try:
    import msal  # type: ignore
except Exception:
    msal = None  # Optional; SSO only if available

from utils.query_params import get_qp, set_qp, clear_qp
from db import ensure_access_control_tables, get_user_by_email, record_user_login


# -------------------------
# Role mapping
# -------------------------
ROLES = {"VIEWER": 1, "CONTRIBUTOR": 2, "ADMIN": 3}


class AuthIdentityError(RuntimeError):
    pass


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    sval = str(value or "").strip().lower()
    if sval in {"1", "true", "yes", "y", "on"}:
        return True
    if sval in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default if sval == "" else sval)


def _role_level(role: Optional[str]) -> int:
    if not role:
        return 0
    return ROLES.get(str(role).upper(), 0)


def _app_env() -> str:
    raw = str(os.getenv("APP_ENV", "prod") or "prod").strip().lower()
    return raw if raw in {"dev", "prod"} else "prod"


def _allow_local_dev_identity() -> bool:
    return (
        _app_env() == "dev"
        and str(os.getenv("ALLOW_LOCAL_ADMIN", "") or "").strip() == "1"
        and bool(str(os.getenv("LOCAL_ADMIN_EMAIL", "") or "").strip())
    )


def _sso_required() -> bool:
    # SSO is only required when AzureAD is enabled AND auth.require_sso is true.
    require_sso = _as_bool(_secret_get("auth", "require_sso", True), default=True)
    return bool(_azuread_enabled() and require_sso)


def _mask_email(email: str) -> str:
    em = str(email or "").strip()
    if not em:
        return ""
    if "@" not in em:
        return (em[:3] + "***") if len(em) > 3 else "***"
    local, domain = em.split("@", 1)
    left = (local[:3] if local else "") + "***"
    return f"{left}@{domain}"


def _resolve_portfolio_membership_role(email: str) -> str:
    em = str(email or "").strip()
    key = str(st.session_state.get("active_portfolio_key") or "").strip() if st is not None else ""
    if not em or not key:
        return "none"
    try:
        from db.control_db import control_db_available, control_fetch_df
        if not control_db_available():
            return "none"
        df = control_fetch_df(
            """
            SELECT TOP 1 ROLE
            FROM CONTROL_PORTFOLIO_USERS
            WHERE UPPER(LTRIM(RTRIM(USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
              AND UPPER(LTRIM(RTRIM(PORTFOLIO_KEY))) = UPPER(LTRIM(RTRIM(%s)))
            """,
            (em, key),
        )
        if df is None or df.empty:
            return "none"
        role = str(df.iloc[0].get("ROLE") or "").strip().upper()
        if role == "EDITOR":
            role = "CONTRIBUTOR"
        return role if role in {"ADMIN", "CONTRIBUTOR", "VIEWER"} else "none"
    except Exception:
        return "none"


def _resolve_admin_membership(email: str) -> Tuple[bool, str]:
    em = str(email or "").strip()
    if not em:
        return False, "none"
    try:
        cache = st.session_state.setdefault("_admin_membership_cache", {})
        if isinstance(cache, dict):
            hit = cache.get(em.lower())
            now = time.time()
            if isinstance(hit, dict):
                ts = float(hit.get("ts") or 0.0)
                if now - ts <= 30.0:
                    return bool(hit.get("global_admin", False)), str(hit.get("portfolio_role") or "none")
    except Exception:
        cache = None

    global_admin = False
    try:
        from db.control_db import is_control_global_admin
        global_admin = bool(is_control_global_admin(em))
    except Exception:
        global_admin = False
    portfolio_role = _resolve_portfolio_membership_role(em)
    try:
        cache = st.session_state.setdefault("_admin_membership_cache", {})
        if isinstance(cache, dict):
            cache[em.lower()] = {
                "ts": time.time(),
                "global_admin": bool(global_admin),
                "portfolio_role": str(portfolio_role or "none"),
            }
    except Exception:
        pass
    return global_admin, portfolio_role


def get_identity_email() -> str:
    sso_candidates: list[Any] = []
    auth_user = st.session_state.get("auth_user") if st is not None else None
    if isinstance(auth_user, dict):
        provider = str(auth_user.get("provider") or "").strip().upper()
        if provider in {"SSO", "AZUREAD", "MSAL"}:
            sso_candidates.extend([
                auth_user.get("email"),
                auth_user.get("EMAIL"),
            ])
    token = st.session_state.get("_aad_token") if st is not None else None
    if isinstance(token, dict):
        claims = token.get("id_token_claims") or {}
        if isinstance(claims, dict):
            sso_candidates.extend([
                claims.get("preferred_username"),
                claims.get("email"),
                claims.get("upn"),
            ])
    ms = (st.session_state.get("ms_user") if st is not None else None) or (st.session_state.get("azuread_user") if st is not None else None)
    if isinstance(ms, dict):
        sso_candidates.extend([
            ms.get("preferred_username"),
            ms.get("email"),
            ms.get("upn"),
        ])
    try:
        headers = getattr(getattr(st, "context", None), "headers", {}) if st is not None else {}
        if isinstance(headers, dict):
            sso_candidates.extend([
                headers.get("x-ms-client-principal-name"),
                headers.get("x-auth-request-email"),
            ])
    except Exception:
        pass
    for c in sso_candidates:
        s = str(c or "").strip().lower()
        if s and "@" in s:
            if st is not None:
                st.session_state["_auth_identity_source"] = "sso"
            return s

    if _allow_local_dev_identity():
        local_email = str(os.getenv("LOCAL_ADMIN_EMAIL", "") or "").strip().lower()
        if local_email:
            if st is not None:
                st.session_state["_auth_identity_source"] = "local-dev"
            return local_email

    if st is not None:
        st.session_state["_auth_identity_source"] = "missing"
    raise AuthIdentityError("SSO identity missing")


def get_identity_groups() -> list[str]:
    """Return Azure AD group IDs from token claims when available (no Graph lookup)."""
    groups: list[str] = []
    token = st.session_state.get("_aad_token") if st is not None else None
    if isinstance(token, dict):
        claims = token.get("id_token_claims") or {}
        if isinstance(claims, dict):
            raw = claims.get("groups")
            if isinstance(raw, (list, tuple)):
                groups = [str(x).strip() for x in raw if str(x).strip()]
    return groups


def _set_auth_debug_state(identity_source: str, email: str, global_admin: bool, portfolio_role: str, effective_role: str) -> None:
    if st is None:
        return
    st.session_state["_auth_debug_state"] = {
        "identity_source": identity_source or "missing",
        "resolved_email_masked": _mask_email(email),
        "global_admin": bool(global_admin),
        "portfolio_role": portfolio_role or "none",
        "effective_role": effective_role or "VIEWER",
    }


def _normalize_non_admin_role(role: Any, default: str = "VIEWER") -> str:
    r = str(role or "").strip().upper()
    if not r:
        return str(default or "VIEWER").strip().upper() or "VIEWER"
    if r == "ADMIN":
        return "VIEWER"
    return r


# -------------------------
# Global flow cache (survives session_state loss)
# -------------------------
_FLOW_CACHE: Dict[str, Dict[str, Any]] = {}
_FLOW_CACHE_TS: Dict[str, float] = {}
_FLOW_TTL_SECONDS = 300  # 5 minutes; adjust as desired


def _flowcache_put(flow: Dict[str, Any]) -> None:
    state = flow.get("state")
    if not state:
        return
    _FLOW_CACHE[state] = flow
    _FLOW_CACHE_TS[state] = time.time()
    _flowcache_gc()


def _flowcache_get(state: Optional[str]) -> Optional[Dict[str, Any]]:
    if not state:
        return None
    _flowcache_gc()
    return _FLOW_CACHE.get(state)


def _flowcache_pop(state: Optional[str]) -> None:
    if not state:
        return
    _FLOW_CACHE.pop(state, None)
    _FLOW_CACHE_TS.pop(state, None)


def _flowcache_gc() -> None:
    now = time.time()
    stale = [s for s, ts in _FLOW_CACHE_TS.items() if now - ts > _FLOW_TTL_SECONDS]
    for s in stale:
        _FLOW_CACHE.pop(s, None)
        _FLOW_CACHE_TS.pop(s, None)


# -------------------------
# Session helpers
# -------------------------
def current_user() -> Optional[dict]:
    # If local persistence is enabled and session_state was lost (refresh/deep link),
    # attempt to restore auth_user from the URL token.
    if not st.session_state.get("auth_user"):
        _maybe_restore_local_session_from_query_params()
    return st.session_state.get("auth_user")


def is_authenticated() -> bool:
    return bool(current_user())


def get_current_user_email() -> Optional[str]:
    """Return the current user's canonical email (strip + lower), if available."""
    candidates: list[Any] = []

    # Local login / legacy keys
    candidates.append(st.session_state.get("user_email"))
    candidates.append(st.session_state.get("email"))
    candidates.append(st.session_state.get("auth_email"))

    # Primary auth payload
    auth_user = st.session_state.get("auth_user")
    if isinstance(auth_user, dict):
        candidates.append(auth_user.get("email"))
        candidates.append(auth_user.get("EMAIL"))

    # AzureAD / MSAL paths (keep safe even if disabled)
    ms = (
        st.session_state.get("ms_user")
        or st.session_state.get("azuread_user")
        or st.session_state.get("user")
    )
    if isinstance(ms, dict):
        candidates.append(ms.get("preferred_username"))
        candidates.append(ms.get("upn"))
        candidates.append(ms.get("email"))

    for c in candidates:
        if c is None:
            continue
        s = str(c).strip()
        if s:
            return s.lower()
    return None


def is_admin_user() -> bool:
    role = (st.session_state.get("role") or st.session_state.get("user_role") or "").strip().upper()
    if role:
        return role == "ADMIN"
    return False


def _save_user(user: dict) -> None:
    saved = dict(user or {})
    email = str(saved.get("email") or saved.get("EMAIL") or "").strip().lower()
    role_raw = str(saved.get("role") or "").strip().upper()
    if role_raw == "EDITOR":
        role_raw = "CONTRIBUTOR"
    role = role_raw if role_raw in {"ADMIN", "CONTRIBUTOR", "VIEWER"} else "VIEWER"
    if email:
        st.session_state["user_email"] = email
    st.session_state["role"] = role
    saved["role"] = role
    st.session_state["auth_user"] = saved
    st.session_state["_auth_last_active"] = time.time()
    # Clear any app guard block once authentication is confirmed.
    st.session_state.pop("_app_block_reason", None)


def _consume_auth_just_logged_in_flag() -> bool:
    just = bool(st.session_state.get("__auth_just_logged_in__"))
    if just:
        st.session_state.pop("__auth_just_logged_in__", None)
    return just


def _set_auth_session_identity(email: str, role: str, provider: str, display_name: str = "") -> None:
    em = str(email or "").strip().lower()
    rl = str(role or "").strip().upper() or "VIEWER"
    pv = str(provider or "").strip().lower() or "local"
    dn = str(display_name or em or "User").strip()
    st.session_state["auth_email"] = em
    st.session_state["auth_display_name"] = dn
    st.session_state["auth_role"] = rl
    st.session_state["auth_provider"] = pv
    st.session_state["user_email"] = em
    st.session_state["role"] = rl


def _is_admin_base_role() -> bool:
    # Base role must be sourced from authenticated identity (auth_user/current_user),
    # not from the mutable previewed session role.
    auth_user = st.session_state.get("auth_user") or {}
    base = str((auth_user.get("role") if isinstance(auth_user, dict) else "") or "").strip().upper()
    if not base:
        try:
            user = current_user() or {}
            base = str((user.get("role") if isinstance(user, dict) else "") or "").strip().upper()
        except Exception:
            base = ""
    if not base:
        base = str(st.session_state.get("_role_base_effective") or "").strip().upper()
    return base == "ADMIN"


def _admin_preview_tools_enabled() -> bool:
    if not _is_admin_base_role():
        return False
    return bool(st.session_state.get("admin_preview_tools_enabled", False))


def get_scope_user_for_filters() -> dict:
    """Return auth user or admin-selected scope-preview user for read filtering."""
    user = st.session_state.get("auth_user") or {}
    if not _is_admin_base_role():
        st.session_state.pop("_admin_scope_preview_email", None)
        st.session_state.pop("_admin_scope_preview_name", None)
        st.session_state.pop("_admin_role_preview", None)
        return user if isinstance(user, dict) else {}

    if not _admin_preview_tools_enabled():
        st.session_state.pop("_admin_scope_preview_email", None)
        st.session_state.pop("_admin_scope_preview_name", None)
        st.session_state.pop("_admin_role_preview", None)
        return user if isinstance(user, dict) else {}

    imp_email = str(st.session_state.get("_admin_scope_preview_email") or "").strip().lower()
    if not imp_email:
        return user if isinstance(user, dict) else {}

    imp_name = str(st.session_state.get("_admin_scope_preview_name") or "").strip()
    return {
        "email": imp_email,
        "name": imp_name or imp_email,
        "display_name": imp_name or imp_email,
        # Keep scope preview read-only and non-privileged.
        "role": "VIEWER",
    }


def is_readonly_preview_active() -> bool:
    if not _is_admin_base_role():
        return False
    if not _admin_preview_tools_enabled():
        return False
    return bool(str(st.session_state.get("_admin_scope_preview_email") or "").strip())


def _scope_preview_choice_options() -> tuple[list[str], dict[str, tuple[str, str]]]:
    options: list[str] = ["Actual scope (me)"]
    mapping: dict[str, tuple[str, str]] = {"Actual scope (me)": ("", "")}
    try:
        from db.mssql_backend import fetch_df

        df = fetch_df(
            """
            SELECT TOP 500
              CAST(COALESCE(EMAIL, '') AS NVARCHAR(320)) AS EMAIL,
              CAST(COALESCE(DISPLAY_NAME, EMAIL, '') AS NVARCHAR(320)) AS DISPLAY_NAME
            FROM APP_USERS
            ORDER BY EMAIL
            """
        )
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                email = str(row.get("EMAIL") or "").strip().lower()
                if not email:
                    continue
                name = str(row.get("DISPLAY_NAME") or email).strip()
                label = f"{name} <{email}>"
                if label not in mapping:
                    mapping[label] = (email, name)
                    options.append(label)
    except Exception:
        pass
    return options, mapping


def _logout() -> None:
    # Clear all auth-related state to avoid half-open PKCE flows
    st.session_state.pop("auth_user", None)
    st.session_state.pop("_aad_token", None)
    st.session_state.pop("_aad_token_time", None)
    st.session_state.pop("_auth_last_active", None)
    st.session_state.pop("_auth_expired_notice", None)
    st.session_state.pop("_msal_flow", None)
    st.session_state.pop("_msal_auto_started", None)
    st.session_state.pop("_auth_started_ts", None)
    st.session_state.pop("_post_auth_page", None)
    st.session_state.pop("user_email", None)
    st.session_state.pop("role", None)
    st.session_state.pop("auth_email", None)
    st.session_state.pop("auth_role", None)
    st.session_state.pop("auth_display_name", None)
    st.session_state.pop("auth_provider", None)
    st.session_state.pop("_active_local_token", None)
    st.session_state.pop("_admin_scope_preview_email", None)
    st.session_state.pop("_admin_scope_preview_name", None)
    # Do NOT nuke the global cache; Azure front-channel logout does not know our 'state'
    # Clear local-token persistence from the URL (if present).
    try:
        params = _get_query_params()
        tok = str(params.get("local_token") or "").strip()
        if tok:
            _LOCAL_SESSION_STORE.pop(tok, None)
        _update_query_params(local_token=None)
    except Exception:
        pass


# -------------------------
# Secrets / config helpers
# -------------------------
def _secret_get(section: str, key: str, default: Any = None):
    """Safe access for st.secrets[section][key], with nested get fallbacks."""
    env_map = {
        ("auth", "admin_email"): "AUTH_ADMIN_EMAIL",
        ("auth", "admin_password"): "AUTH_ADMIN_PASSWORD",
        # Safety default: AUTH_AUTO_LOGIN is opt-in. If unset, auto-login remains disabled.
        ("auth", "auto_login"): "AUTH_AUTO_LOGIN",
        ("auth", "sso_debug"): "AUTH_SSO_DEBUG",
        ("auth", "require_sso"): "AUTH_REQUIRE_SSO",
        # When AzureAD SSO is enabled, local-admin UI defaults to disabled unless env explicitly sets it.
        ("auth", "show_admin_login_page"): "AUTH_SHOW_ADMIN_LOGIN_PAGE",
        # When AzureAD SSO is enabled, local-admin UI defaults to disabled unless env explicitly sets it.
        ("auth", "enable_local_admin_ui"): "AUTH_ENABLE_LOCAL_ADMIN_UI",
        ("azuread", "enabled"): "AZUREAD_ENABLED",
        ("azuread", "tenant_id"): "AZUREAD_TENANT_ID",
        ("azuread", "client_id"): "AZUREAD_CLIENT_ID",
        ("azuread", "client_secret"): "AZUREAD_CLIENT_SECRET",
        ("azuread", "redirect_uri"): "AZUREAD_REDIRECT_URI",
    }
    bool_keys = {
        ("auth", "auto_login"),
        ("auth", "sso_debug"),
        ("auth", "require_sso"),
        ("auth", "show_admin_login_page"),
        ("auth", "enable_local_admin_ui"),
        ("azuread", "enabled"),
    }

    def _parse_env_bool(val: Any) -> bool:
        if isinstance(val, bool):
            return val
        sval = str(val or "").strip().lower()
        if sval in {"1", "true", "yes", "y", "on"}:
            return True
        if sval in {"0", "false", "no", "n", "off"}:
            return False
        return bool(sval)

    try:
        sec = getattr(st, "secrets", {})
        s = sec.get(section, {})
        try:
            val = s.get(key, None)
        except Exception:
            try:
                val = s[key]
            except Exception:
                val = None
    except Exception:
        val = None

    if val is not None:
        return val

    env_key = env_map.get((section, key))
    if not env_key:
        return default
    env_val = os.getenv(env_key)
    if env_val is None or env_val == "":
        return default
    if (section, key) in bool_keys:
        return _parse_env_bool(env_val)
    return env_val


def _mask(val: Optional[str], left: int = 4, right: int = 4) -> str:
    try:
        s = str(val or "")
    except Exception:
        return ""
    if len(s) <= left + right:
        return s
    return s[:left] + "…" + s[-right:]


def _try_auto_login_admin() -> Optional[dict]:
    if not (_allow_local_dev_identity() or not _sso_required()):
        return None
    mode = auth_mode_summary()
    if not bool(mode.get("local_admin_ui_enabled")):
        return None
    if not bool(mode.get("auto_login_enabled")):
        return None
    admin_email = str(os.getenv("LOCAL_ADMIN_EMAIL", "") or "").strip() or _secret_get("auth", "admin_email")
    admin_pass = _secret_get("auth", "admin_password")
    if not (admin_email and admin_pass):
        return None
    user = {"email": str(admin_email).strip().lower(), "name": "Admin", "role": "ADMIN", "provider": "local"}
    _save_user(user)
    try:
        _set_auth_session_identity(str(admin_email), "ADMIN", "local", "Admin")
    except Exception:
        pass
    # Persist local login across refresh/deep links when AzureAD is disabled.
    if _local_persistence_enabled():
        try:
            tok = _local_store_put(user)
            st.session_state["_active_local_token"] = tok
            set_qp(local_token=tok)
        except Exception:
            pass
    st.session_state["__auth_just_logged_in__"] = True
    st.rerun()
    return None


def _get_query_params() -> Dict[str, Any]:
    """Works across Streamlit versions."""
    return get_qp()


def _set_query_params(**kwargs) -> None:
    """Clear or set query params (fallback safe)."""
    # Preserve unrelated params (e.g., portfolio) instead of overwriting everything.
    if not kwargs:
        # Clear only auth-related keys.
        clear_qp("code", "state", "local_token")
        return
    # Merge into existing params.
    set_qp(**kwargs)


# -------------------------
# Optional: local session persistence (dev mode)
# -------------------------
# NOTE: This is in-memory only (per Streamlit server process). It enables refresh/deep-link
# reliability for local admin logins when AzureAD SSO is disabled via secrets.
_LOCAL_SESSION_STORE: Dict[str, Dict[str, Any]] = {}


def _azuread_enabled() -> bool:
    raw = _secret_get("azuread", "enabled", True)
    if isinstance(raw, str):
        return raw.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(raw)


def auth_mode_summary() -> Dict[str, bool]:
    """
    Return effective auth-mode booleans used by UI and local auto-login safety guards.
    """
    azuread_enabled = _azuread_enabled()
    require_sso = _sso_required()
    local_admin_ui_env = os.getenv("AUTH_ENABLE_LOCAL_ADMIN_UI")
    show_admin_login_env = os.getenv("AUTH_SHOW_ADMIN_LOGIN_PAGE")

    admin_email = _secret_get("auth", "admin_email")
    admin_pass = _secret_get("auth", "admin_password")
    local_admin_cfg_default = bool(admin_email and admin_pass)

    if azuread_enabled:
        local_admin_ui_enabled = _as_bool(local_admin_ui_env, default=False) if local_admin_ui_env not in (None, "") else False
        show_admin_login_page = _as_bool(show_admin_login_env, default=False) if show_admin_login_env not in (None, "") else False
    else:
        local_admin_ui_enabled = _as_bool(_secret_get("auth", "enable_local_admin_ui", local_admin_cfg_default), default=local_admin_cfg_default)
        show_admin_login_page = _as_bool(_secret_get("auth", "show_admin_login_page", local_admin_ui_enabled), default=local_admin_ui_enabled)

    auto_login_enabled = _as_bool(_secret_get("auth", "auto_login", False), default=False) and local_admin_ui_enabled

    summary = {
        "azuread.enabled": bool(azuread_enabled),
        "require_sso": bool(require_sso),
        "local_admin_ui_enabled": bool(local_admin_ui_enabled),
        "show_admin_login_page": bool(show_admin_login_page),
        "auto_login_enabled": bool(auto_login_enabled),
    }
    try:
        if st is not None:
            st.session_state["_auth_mode_summary"] = summary
    except Exception:
        pass
    return summary


def _local_persistence_enabled() -> bool:
    # Only enable local-token persistence when SSO is explicitly disabled.
    return not _azuread_enabled()


def _local_session_ttl_seconds() -> int:
    raw = _secret_get("auth", "session_idle_minutes", None)
    if raw is None:
        return int(8 * 60 * 60)  # 8 hours default
    try:
        mins = float(raw)
        if mins <= 0:
            return int(8 * 60 * 60)
        return int(mins * 60)
    except Exception:
        return int(8 * 60 * 60)


def _local_store_gc() -> None:
    now = time.time()
    stale = [k for k, v in _LOCAL_SESSION_STORE.items() if float(v.get("expires_at") or 0) <= now]
    for k in stale:
        _LOCAL_SESSION_STORE.pop(k, None)


def _update_query_params(**updates: Optional[str]) -> None:
    """
    Update query params without blowing away unrelated params.
    Set a value to None to remove it.
    """
    set_qp(**updates)


def _local_store_put(user: Dict[str, Any]) -> str:
    token = uuid.uuid4().hex
    _LOCAL_SESSION_STORE[token] = {
        "user": dict(user),
        "expires_at": time.time() + float(_local_session_ttl_seconds()),
    }
    _local_store_gc()
    return token


def _maybe_restore_local_session_from_query_params() -> None:
    if st.session_state.get("auth_user"):
        return
    if not _local_persistence_enabled():
        return
    params = _get_query_params()
    token = str(params.get("local_token") or "").strip()
    if not token:
        return
    _local_store_gc()
    entry = _LOCAL_SESSION_STORE.get(token)
    if not entry:
        # Token not recognized (process restarted) -> remove it.
        st.session_state["_local_session_expired"] = True
        _update_query_params(local_token=None)
        return
    if float(entry.get("expires_at") or 0) <= time.time():
        _LOCAL_SESSION_STORE.pop(token, None)
        st.session_state["_local_session_expired"] = True
        _update_query_params(local_token=None)
        return
    user = entry.get("user") or {}
    if isinstance(user, dict) and user.get("email"):
        restored_user = dict(user)
        rp = str(restored_user.get("provider") or "").strip().lower()
        rr = str(restored_user.get("role") or "").strip().upper()
        if not _sso_required() and rp == "local" and rr == "ADMIN":
            restored_user["role"] = "ADMIN"
        else:
            restored_user["role"] = _normalize_non_admin_role(restored_user.get("role"), default="VIEWER")
        _save_user(restored_user)
        # Keep canonical keys aligned for downstream access checks.
        try:
            _set_auth_session_identity(
                str(restored_user.get("email") or ""),
                str(restored_user.get("role") or "VIEWER"),
                str(restored_user.get("provider") or "local"),
                str(restored_user.get("name") or "User"),
            )
        except Exception:
            pass
        # Rotate URL token once per restored session to reduce replay window while
        # preserving local-auth persistence behavior across refresh/deep links.
        try:
            active_tok = str(st.session_state.get("_active_local_token") or "").strip()
            if token and token != active_tok:
                _LOCAL_SESSION_STORE.pop(token, None)
                new_tok = _local_store_put(restored_user)
                st.session_state["_active_local_token"] = new_tok
                _update_query_params(local_token=new_tok)
            else:
                st.session_state["_active_local_token"] = token
        except Exception:
            pass


# -------------------------
# Post-auth redirect helpers
# -------------------------
def _normalize_page_path(page_path: Optional[str]) -> Optional[str]:
    if not page_path:
        return None
    try:
        p = Path(str(page_path))
    except Exception:
        return str(page_path)
    if p.is_absolute():
        try:
            root = Path(__file__).resolve().parents[1]
            return p.resolve().relative_to(root).as_posix()
        except Exception:
            return p.as_posix()
    return p.as_posix()


def _remember_post_auth_page(page_path: Optional[str]) -> None:
    target = _normalize_page_path(page_path)
    if not target:
        return
    params = _get_query_params()
    if "code" in params:
        return
    existing = st.session_state.get("_post_auth_page")
    if existing != target:
        st.session_state["_post_auth_page"] = target


def _pop_post_auth_page(flow: Optional[dict] = None) -> Optional[str]:
    target = None
    if isinstance(flow, dict):
        target = flow.get("post_auth_page")
    if not target:
        target = st.session_state.get("_post_auth_page")
    if target:
        st.session_state.pop("_post_auth_page", None)
    return _normalize_page_path(target)


def _redirect_post_auth(flow: Optional[dict] = None) -> None:
    # Avoid auto-routing; rely on navigation UI instead.
    _ = _pop_post_auth_page(flow)
    return


# -------------------------
# Session timeout helpers
# -------------------------
def _session_timeout_seconds() -> int:
    raw = _secret_get("auth", "session_idle_minutes", 120)
    try:
        mins = float(raw)
        if mins <= 0:
            return 0
        return int(mins * 60)
    except Exception:
        return 120 * 60


def _maybe_expire_session() -> bool:
    """
    Track last activity and expire idle auth sessions.
    Returns True if the session was expired.
    """
    user = current_user()
    if not user:
        return False

    timeout_s = _session_timeout_seconds()
    if timeout_s <= 0:
        return False

    now = time.time()
    last = float(st.session_state.get("_auth_last_active") or 0)
    if last and now - last > timeout_s:
        _logout()
        st.session_state["_auth_expired_notice"] = True
        return True

    st.session_state["_auth_last_active"] = now
    return False


# -------------------------
# MSAL (Auth Code flow) helpers
# -------------------------
def _msal_confidential_app():
    """Build a ConfidentialClientApplication if secrets allow."""
    if not msal:
        return None
    tenant_id = _secret_get("azuread", "tenant_id")
    client_id = _secret_get("azuread", "client_id")
    client_secret = _secret_get("azuread", "client_secret")
    if not (tenant_id and client_id and client_secret):
        return None
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    return msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret,
    )


def _graph_scopes() -> list[str]:
    scopes_cfg = _secret_get("azuread", "graph_scopes", None)
    if isinstance(scopes_cfg, (list, tuple)):
        return [str(s) for s in scopes_cfg if s]
    if isinstance(scopes_cfg, str) and scopes_cfg.strip():
        return [s.strip() for s in scopes_cfg.split()]
    # Include openid/profile/offline_access implicitly via MSAL; we ask minimal graph scopes here
    return ["User.Read", "User.ReadBasic.All"]


def _get_or_create_flow(app, post_auth_page: Optional[str] = None) -> dict:
    """
    Return existing pending flow if present; otherwise create one and persist.
    CRITICAL for PKCE: do NOT regenerate after starting.
    """
    target = _normalize_page_path(post_auth_page) or _normalize_page_path(
        st.session_state.get("_post_auth_page")
    )
    existing = st.session_state.get("_msal_flow")
    if isinstance(existing, dict) and "state" in existing and "auth_uri" in existing:
        if target and existing.get("post_auth_page") != target:
            existing["post_auth_page"] = target
        return existing

    redirect_uri = _secret_get("azuread", "redirect_uri")
    if not redirect_uri:
        st.error("Missing azuread.redirect_uri in secrets.")
        st.stop()

    flow = app.initiate_auth_code_flow(scopes=_graph_scopes(), redirect_uri=redirect_uri)
    if target:
        flow["post_auth_page"] = target
    # Persist both in session_state and in-process cache (keyed by 'state')
    st.session_state["_msal_flow"] = flow
    st.session_state["_auth_started_ts"] = int(time.time())
    _flowcache_put(flow)
    return flow


def _authcode_begin(auto: bool = False, post_auth_page: Optional[str] = None) -> None:
    """Initiate Authorization Code flow without regenerating the PKCE challenge."""
    app = _msal_confidential_app()
    if not app:
        st.error("Auth code flow is not configured (missing client secret or MSAL).")
        st.stop()

    flow = _get_or_create_flow(app, post_auth_page=post_auth_page)
    auth_uri = flow.get("auth_uri") or _secret_get("azuread", "redirect_uri")
    sso_debug = bool(_secret_get("auth", "sso_debug", False))
    if auto:
        # Minimal UX and robust redirect
        st.info("SSO in progress…")
        html = f"""
        <meta http-equiv=\"refresh\" content=\"0; url={auth_uri}\">\n
        <p>If you are not redirected automatically, <a href=\"{auth_uri}\">click here</a>.</p>
        """
        st.markdown(html, unsafe_allow_html=True)
        st.stop()
    else:
        st.link_button("Continue", auth_uri)
        st.stop()


def _resolve_control_effective_role(email: str) -> Tuple[Optional[str], bool, bool]:
    """
    Resolve role from Control DB only, so SSO callback works even without active portfolio context.
    Returns (effective_role, is_global_admin, has_any_membership).
    """
    em = str(email or "").strip().lower()
    if not em:
        return None, False, False
    try:
        from db.control_db import create_control_schema, is_control_global_admin, control_fetch_df

        # Ensure schema exists before membership queries.
        create_control_schema()

        global_admin = bool(is_control_global_admin(em))
        df_roles = control_fetch_df(
            """
            SELECT UPPER(LTRIM(RTRIM(ROLE))) AS ROLE
            FROM CONTROL_PORTFOLIO_USERS
            WHERE UPPER(LTRIM(RTRIM(USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
            """,
            (em,),
        )
        has_membership = bool(df_roles is not None and not df_roles.empty)
        has_admin_membership = False
        if has_membership:
            try:
                roles = set(str(r or "").strip().upper() for r in df_roles["ROLE"].tolist())
            except Exception:
                roles = set()
            has_admin_membership = "ADMIN" in roles

        if global_admin or has_admin_membership:
            return "ADMIN", global_admin, has_membership
        if has_membership:
            return "VIEWER", global_admin, has_membership
        return None, global_admin, has_membership
    except Exception as exc:
        raise RuntimeError(f"Control DB membership check failed: {exc}")


def _try_complete_authcode() -> None:
    """Complete Authorization Code flow if we were redirected back with ?code=..."""
    if current_user():
        return

    params = _get_query_params()
    sso_debug = bool(_secret_get("auth", "sso_debug", False))
    if "error" in params:
        err = str(params.get("error") or "").strip() or "Unknown SSO error"
        err_desc = str(params.get("error_description") or "").strip()
        st.error(f"SSO callback error: {err}")
        if err_desc:
            st.error(err_desc)
        if sso_debug:
            st.code(params)
        try:
            clear_qp("code", "state", "error", "error_description")
        except Exception:
            _set_query_params()
        st.stop()
    if "code" not in params:
        return

    app = _msal_confidential_app()
    if not app:
        st.warning("Auth code flow configuration missing; cannot complete sign-in.")
        return

    flow = st.session_state.get("_msal_flow")
    if not isinstance(flow, dict) or "state" not in flow:
        # Try to recover from global cache using ?state=<...>
        state = params.get("state")
        cached = _flowcache_get(state)
        if cached:
            flow = cached
            st.session_state["_msal_flow"] = flow  # restore into session for MSAL
        else:
            # Could not recover; restart cleanly
            _set_query_params()  # clear ?code&state
            st.session_state.pop("_msal_flow", None)
            st.session_state.pop("_msal_auto_started", None)
            _authcode_begin(auto=True)
            st.stop()

    # Normal happy path
    try:
        result = app.acquire_token_by_auth_code_flow(flow, params)
    except Exception as e:
        if sso_debug:
            st.error(f"SSO exchange failed: {e}")
        else:
            st.error("SSO failed while exchanging the authorization code.")
        # Consume/cleanup and retry once
        st.session_state.pop("_msal_flow", None)
        _flowcache_pop(flow.get("state"))
        _set_query_params()
        _authcode_begin(auto=True)
        st.stop()

    # Consume the flow (one-shot)
    st.session_state.pop("_msal_flow", None)
    _flowcache_pop(flow.get("state"))

    if "id_token_claims" not in result:
        if sso_debug:
            redacted = {
                k: ("<redacted>" if k in {"access_token","refresh_token","id_token"} else v)
                for k, v in result.items()
            }
            st.warning(result.get("error_description") or "SSO did not return expected tokens.")
            st.code(redacted)
        else:
            st.warning(result.get("error_description") or "SSO did not return expected tokens.")
        _set_query_params()
        # Retry cleanly so we don't land in a dead end
        _authcode_begin(auto=True)
        st.stop()

    claims = result.get("id_token_claims") or {}
    email = claims.get("preferred_username") or claims.get("email")
    name = claims.get("name") or email or "User"

    if email:
        try:
            effective_role, is_global_admin, has_membership = _resolve_control_effective_role(str(email))
        except Exception as exc:
            st.error(str(exc))
            _set_query_params()
            return

        if effective_role:
            try:
                if "access_token" in result:
                    st.session_state["_aad_token"] = result
                    st.session_state["_aad_token_time"] = time.time()
            except Exception:
                pass

            if sso_debug:
                st.caption("[SSO Debug] id_token_claims")
                st.code(claims)

            resolved_name = str(name or email or "User").strip() or str(email)
            _save_user(
                {
                    "email": str(email).strip().lower(),
                    "display_name": resolved_name,
                    "name": resolved_name,
                    "role": effective_role,
                    "auth_provider": "azuread",
                    "provider": "AZUREAD",
                    "is_global_admin": bool(is_global_admin),
                }
            )
            try:
                from db.control_db import resolve_allowed_portfolios
                st.session_state["_allowed_portfolios_after_sso"] = resolve_allowed_portfolios(
                    str(email).strip().lower(),
                    get_identity_groups(),
                )
            except Exception:
                st.session_state["_allowed_portfolios_after_sso"] = []
            _set_auth_session_identity(str(email), effective_role, "azuread", resolved_name)
            _set_auth_debug_state("sso", str(email), bool(is_global_admin), "ADMIN" if effective_role == "ADMIN" and has_membership else ("VIEWER" if has_membership else "none"), effective_role)
            try:
                record_user_login(str(email))
            except Exception as e:
                # Non-fatal: login already succeeded, this should not surface as a hard SSO failure.
                if bool(_secret_get("auth", "sso_debug", False)):
                    st.session_state["_sso_nonfatal_warning"] = f"Failed to record login timestamp: {e}"
            try:
                _set_query_params()  # clear ?code&state
            except Exception:
                pass
            try:
                _redirect_post_auth(flow)
            except Exception:
                pass
            st.rerun()
        else:
            st.error("SSO user is not authorized in Control DB (no global admin or portfolio membership).")
            _set_query_params()
            if _sso_required():
                st.stop()
            return
    else:
        st.warning("SSO finished, but email claim was not present.")
        _set_query_params()
        # Retry cleanly (some tenants omit email on first try)
        _authcode_begin(auto=True)
        st.stop()


# -------------------------
# Optional: Front-channel logout handler
# -------------------------
def _try_front_channel_logout() -> None:
    """
    If your App Registration sets a Front-channel logout URL like:
      https://yourapp.azurewebsites.net/?fc_logout=1
    Azure will load that URL on sign-out. We detect it here and clear the session.
    """
    params = _get_query_params()
    fc = str(params.get("fc_logout", "")).lower()
    if fc in {"1", "true", "yes"} or ("sid" in params and not current_user()):
        _logout()
        _set_query_params()


# -------------------------
# UI: Sidebar auth widget
# -------------------------
def _first_name_from_identity(name: str, email: str) -> str:
    # Avoid rendering email in the sidebar to prevent width expansion.
    base = (name or "").strip()
    if not base:
        base = (email or "").strip()
    if "@" in base:
        base = base.split("@", 1)[0]
    base = re.split(r"[\\._\\-\\s]+", base)[0] if base else ""
    base = base.strip()
    if not base:
        return "User"
    return base[:1].upper() + base[1:]


def _sidebar_display_name(name: str, email: str) -> str:
    # Prefer resolved display name from session/auth payload to avoid lossy parsing.
    base = str(st.session_state.get("auth_display_name") or name or email or "User").strip()
    if "@" in base:
        base = base.split("@", 1)[0].strip()
    base = re.sub(r"\s+", " ", base).strip()
    return base or "User"


def _local_login_allowed(mode: Dict[str, bool]) -> bool:
    if not bool(mode.get("require_sso", True)):
        return True
    if not bool(_azuread_enabled()) and (bool(mode.get("local_admin_ui_enabled")) or _allow_local_dev_identity()):
        return True
    return bool(mode.get("local_admin_ui_enabled")) and bool(mode.get("show_admin_login_page")) and _allow_local_dev_identity()


def _render_unauth_controls(*, key_prefix: str = "sidebar") -> None:
    mode = auth_mode_summary()
    azuread_enabled = bool(mode.get("azuread.enabled"))
    local_admin_ui_enabled = bool(mode.get("local_admin_ui_enabled"))
    local_login_enabled = _local_login_allowed(mode)
    rendered_any = False
    if not bool(mode.get("require_sso", True)):
        st.info("Local auth mode")

    admin_email = _secret_get("auth", "admin_email")
    admin_pass = _secret_get("auth", "admin_password")
    local_dev_email = str(os.getenv("LOCAL_ADMIN_EMAIL", "") or "").strip().lower()

    if local_login_enabled:
        rendered_any = True
        page_hint = (
            st.session_state.get("auth_page_path_hint")
            or st.session_state.get("page_id")
            or st.session_state.get("current_page")
            or st.session_state.get("nav_page")
            or "main"
        )
        page_key = str(page_hint or "main").strip() or "main"
        page_key = re.sub(r"[^A-Za-z0-9_-]+", "_", page_key)
        form_key = f"local_admin_login_form__{key_prefix}__{page_key}"
        email_key = f"{key_prefix}_login_email"
        pass_key = f"{key_prefix}_login_password"
        with st.form(form_key, clear_on_submit=False):
            email_in = st.text_input("Email", key=email_key, placeholder="admin@admin.com")
            pass_in = st.text_input("Password", type="password", key=pass_key)
            submitted = st.form_submit_button("Sign in", use_container_width=True)

        if submitted:
            expected_email = local_dev_email or (str(admin_email).strip().lower() if admin_email else "")
            email_ok = bool(expected_email) and str(email_in).strip().lower() == expected_email
            pass_ok = bool(admin_pass) and str(pass_in).strip() == str(admin_pass).strip()
            if email_ok and pass_ok:
                entered_email = str(email_in).strip().lower()
                configured_admin = str(admin_email or "").strip().lower()
                role = "ADMIN" if (configured_admin and entered_email == configured_admin) else "VIEWER"
                user = {"email": entered_email, "role": role, "mode": "local", "provider": "LOCAL"}
                _save_user(user)
                _set_auth_session_identity(entered_email, role, "LOCAL", "Local Admin")
                st.session_state["auth_user"] = {
                    "email": entered_email,
                    "role": role,
                    "mode": "local",
                    "provider": "LOCAL",
                }
                st.session_state.pop("_app_block_reason", None)
                if _local_persistence_enabled():
                    try:
                        tok = _local_store_put(user)
                        st.session_state["_active_local_token"] = tok
                        _update_query_params(local_token=tok)
                    except Exception:
                        pass
                st.session_state["__auth_just_logged_in__"] = True
                st.rerun()
                return
            else:
                st.error("Invalid admin credentials.")

    client_id = _secret_get("azuread", "client_id")
    tenant_id = _secret_get("azuread", "tenant_id")
    flow_pref = str(_secret_get("azuread", "flow", "authcode") or "authcode").strip().lower()
    sso_button_label = "Sign in with Microsoft" if (azuread_enabled and not local_admin_ui_enabled) else "SSO"

    if azuread_enabled and msal and client_id and tenant_id:
        rendered_any = True
        if flow_pref in {"authcode", "auth_code", "code"} and _secret_get("azuread", "client_secret"):
            params_now = _get_query_params()
            should_autostart = (
                _sso_required()
                and not current_user()
                and not st.session_state.get("_msal_auto_started")
                and ("code" not in params_now)
                and ("error" not in params_now)
            )
            if should_autostart:
                st.session_state["_msal_auto_started"] = True
                _authcode_begin(auto=True)
            pending_flow = bool(st.session_state.get("_msal_flow"))
            if not pending_flow:
                if st.button(sso_button_label, key=f"{key_prefix}_sb_btn_ms_login_authcode"):
                    _authcode_begin(auto=True)
            else:
                st.caption("Starting Microsoft sign-in…")
        else:
            authority = f"https://login.microsoftonline.com/{tenant_id}"
            app = msal.PublicClientApplication(client_id, authority=authority)  # type: ignore
            scopes = _graph_scopes()
            device_key = f"{key_prefix}_device_flow"
            if st.button(sso_button_label, key=f"{key_prefix}_sb_btn_ms_login_device"):
                flow = app.initiate_device_flow(scopes=scopes)
                st.session_state[device_key] = flow
                st.info(f"Open {flow['verification_uri']} and enter code: {flow['user_code']}")
                st.stop()

            if st.session_state.get(device_key):
                if st.button("Complete SSO", key=f"{key_prefix}_sb_btn_ms_login_device_complete"):
                    try:
                        token = app.acquire_token_by_device_flow(st.session_state[device_key])  # type: ignore
                        if "access_token" in token:
                            st.session_state.pop(device_key, None)
                            try:
                                st.session_state["_aad_token"] = token
                                st.session_state["_aad_token_time"] = time.time()
                            except Exception:
                                pass
                            claims = token.get("id_token_claims", {}) or {}
                            email = claims.get("preferred_username") or claims.get("email")
                            name = claims.get("name") or email
                            if email:
                                effective_role, is_global_admin, has_membership = _resolve_control_effective_role(str(email))
                                if effective_role:
                                    resolved_name = str(name or email or "User").strip() or str(email)
                                    _save_user(
                                        {
                                            "email": str(email).strip().lower(),
                                            "display_name": resolved_name,
                                            "name": resolved_name,
                                            "role": effective_role,
                                            "auth_provider": "azuread",
                                            "provider": "AZUREAD",
                                            "is_global_admin": bool(is_global_admin),
                                        }
                                    )
                                    try:
                                        from db.control_db import resolve_allowed_portfolios
                                        st.session_state["_allowed_portfolios_after_sso"] = resolve_allowed_portfolios(
                                            str(email).strip().lower(),
                                            get_identity_groups(),
                                        )
                                    except Exception:
                                        st.session_state["_allowed_portfolios_after_sso"] = []
                                    _set_auth_session_identity(str(email), effective_role, "azuread", resolved_name)
                                    _set_auth_debug_state("sso", str(email), bool(is_global_admin), "ADMIN" if effective_role == "ADMIN" and has_membership else ("VIEWER" if has_membership else "none"), effective_role)
                                    record_user_login(str(email))
                                    _redirect_post_auth()
                                    st.rerun()
                                else:
                                    st.error("SSO user is not authorized in Control DB (no global admin or portfolio membership).")
                            else:
                                st.warning("SSO finished, but email claim missing.")
                        else:
                            st.warning(str(token))
                    except Exception as e:
                        st.error(f"SSO failed: {e}")

    if not rendered_any:
        st.error(
            "No login method is currently available. Check `azuread.enabled`, `auth.require_sso`, "
            "`auth.enable_local_admin_ui`, and local admin credentials."
        )


def _render_auth_sidebar_inner() -> None:
    """Compact auth widget anchored in the sidebar bottom-ish."""
    # Render at most once per Streamlit script run to avoid duplicate widget keys.
    # IMPORTANT: must still render again on the next rerun (e.g. after clicking "SSO"),
    # so we key the guard off Streamlit's run_id, not a sticky boolean.
    run_id = None
    if get_script_run_ctx is not None:
        try:
            ctx = get_script_run_ctx()
            run_id = getattr(ctx, "run_id", None)
        except Exception:
            run_id = None
    last_run = st.session_state.get("_tco_auth_sidebar_rendered_run_id")
    if run_id is not None and last_run == run_id:
        return
    if run_id is not None:
        st.session_state["_tco_auth_sidebar_rendered_run_id"] = run_id
    _try_front_channel_logout()
    try:
        _try_complete_authcode()
    except Exception as e:
        st.session_state["_sso_callback_failed"] = str(e)
        if bool(_secret_get("auth", "sso_debug", False)):
            st.error(f"SSO callback failed: {e}")
    user = current_user()
    if user:
        if not bool(auth_mode_summary().get("require_sso", True)):
            st.info("Local auth mode")
        if bool(st.session_state.pop("_preview_write_blocked", False)):
            st.warning(
                "Write blocked: Scope preview is active. "
                "Set Scope preview to 'Actual scope (me)' to enable saves."
            )
        name = _sidebar_display_name(str(user.get("name", "") or ""), str(user.get("email", "") or ""))
        role = str(user.get("role", "") or "").strip().upper() or "VIEWER"
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown(
            f"<div style='font-weight:600; line-height:1.1; margin:0;'>{name}</div>"
            f"<div style='color:rgba(255,255,255,0.65); font-size:0.85rem; margin-top:2px;'>{role}</div>",
            unsafe_allow_html=True,
        )
        real_role = str(st.session_state.get("_role_base_effective") or role).strip().upper()
        preview_email = str(st.session_state.get("_admin_scope_preview_email") or "").strip().lower()
        preview_enabled = bool(st.session_state.get("admin_preview_tools_enabled", False))
        preview_strict = bool(st.session_state.get("_admin_scope_preview_strict_role", False))
        if real_role == "ADMIN" and preview_enabled and preview_email:
            nav_role = str(st.session_state.get("_admin_scope_preview_effective_role") or "VIEWER").strip().upper() or "VIEWER"
            mode_label = "strict nav" if preview_strict else "scope-only"
            st.markdown(
                f"<div style='color:rgba(255,255,255,0.62); font-size:0.80rem; margin-top:2px;'>"
                f"Preview: {nav_role} ({mode_label}) • {_mask_email(preview_email)}</div>",
                unsafe_allow_html=True,
            )
        try:
            from core.portfolio_runtime import get_active_portfolio_key
            active_key = get_active_portfolio_key()
            portfolio_name = st.session_state.get("active_portfolio_display_name")
            if not active_key:
                portfolio_name = None
            if not portfolio_name and active_key:
                portfolio_name = active_key
            label = portfolio_name if portfolio_name else "(none)"
            st.markdown(
                f"<div style='color:rgba(255,255,255,0.55); font-size:0.82rem; margin-top:2px;'>"
                f"Portfolio: {label}</div>",
                unsafe_allow_html=True,
            )
        except Exception:
            st.markdown(
                "<div style='color:rgba(255,255,255,0.55); font-size:0.82rem; margin-top:2px;'>"
                "Portfolio: (none)</div>",
                unsafe_allow_html=True,
            )
        if real_role == "ADMIN":
            st.session_state.setdefault("admin_preview_tools_enabled", False)
            toggle = getattr(st, "toggle", None)
            if callable(toggle):
                preview_enabled = bool(
                    toggle(
                        "Enable preview tools",
                        key="admin_preview_tools_enabled",
                        help="Admin-only role/scope simulation controls.",
                    )
                )
            else:
                checkbox = getattr(st, "checkbox", None)
                if callable(checkbox):
                    preview_enabled = bool(
                        checkbox(
                            "Enable preview tools",
                            key="admin_preview_tools_enabled",
                            help="Admin-only role/scope simulation controls.",
                        )
                    )
                else:
                    # Test/runtime shim fallback: keep disabled when widget is unavailable.
                    preview_enabled = False
            if not preview_enabled:
                st.session_state["_admin_scope_preview_email"] = ""
                st.session_state["_admin_scope_preview_name"] = ""
                st.session_state["_admin_scope_preview_strict_role"] = False
            else:
                def _render_scope_preview_controls() -> None:
                    options, mapping = _scope_preview_choice_options()
                    cur_email = str(st.session_state.get("_admin_scope_preview_email") or "").strip().lower()
                    current_label = "Actual scope (me)"
                    if cur_email:
                        for label, payload in mapping.items():
                            if str(payload[0] or "").strip().lower() == cur_email:
                                current_label = label
                                break
                    if current_label not in options:
                        current_label = "Actual scope (me)"
                    selected_scope = st.selectbox(
                        "Preview scope as user",
                        options=options,
                        index=max(0, options.index(current_label)),
                        key="admin_scope_preview_select",
                        help=(
                            "Admin-only read filter preview. Simulates Program/Product owner scope for dashboards. "
                            "Writes are blocked while this preview is active."
                        ),
                    )
                    selected_email, selected_name = mapping.get(selected_scope, ("", ""))
                    st.session_state["_admin_scope_preview_email"] = selected_email
                    st.session_state["_admin_scope_preview_name"] = selected_name
                    if selected_email:
                        st.caption(f"Read-only preview active for {_mask_email(selected_email)}")
                        toggle = getattr(st, "toggle", None)
                        if callable(toggle):
                            strict_role = bool(
                                toggle(
                                    "Strict impersonation (navigation role)",
                                    key="_admin_scope_preview_strict_role",
                                    help=(
                                        "When enabled, sidebar page visibility follows the selected user role. "
                                        "Authorization and writes still remain protected."
                                    ),
                                )
                            )
                        else:
                            strict_role = bool(
                                st.checkbox(
                                    "Strict impersonation (navigation role)",
                                    key="_admin_scope_preview_strict_role",
                                    help=(
                                        "When enabled, sidebar page visibility follows the selected user role. "
                                        "Authorization and writes still remain protected."
                                    ),
                                )
                            )
                        if strict_role:
                            st.caption("Strict mode: sidebar visibility follows impersonated role.")
                    else:
                        st.session_state["_admin_scope_preview_strict_role"] = False

                expander = getattr(st, "expander", None)
                if callable(expander):
                    with expander("Scope preview (admin)", expanded=False):
                        _render_scope_preview_controls()
                else:
                    _render_scope_preview_controls()
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if role == "ADMIN" and bool(st.session_state.get("debug_mode", False)):
            dbg = st.session_state.get("_auth_debug_state") or {}
            with st.expander("Auth Identity Debug", expanded=False):
                st.code(
                    {
                        "identity_source": dbg.get("identity_source", "missing"),
                        "resolved_email": dbg.get("resolved_email_masked", ""),
                        "global_admin": bool(dbg.get("global_admin", False)),
                        "portfolio_role": dbg.get("portfolio_role", "none"),
                    }
                )
        if st.button("Log out", key="btn_logout_sidebar", type="secondary", use_container_width=True):
            _logout()
            st.rerun()
    else:
        if st.session_state.pop("_local_session_expired", False):
            st.info("Local session expired (server restarted). Please sign in again.")
        st.caption("Sign in")
        _render_unauth_controls(key_prefix="sidebar")

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.caption("Not signed in")

    # Brand mark (bottom sidebar, centered)
    assets_dir = Path(__file__).resolve().parents[1] / "assets"
    icon_path = assets_dir / "next_icon.png"
    # Optional SSO debug panel (sidebar)
    sso_debug = bool(_secret_get("auth", "sso_debug", False))
    if sso_debug:
        with st.expander("SSO Debug", expanded=True):
            try:
                flow_pref = str(_secret_get("azuread", "flow", "")).strip().lower()
                have_conf = bool(
                    _secret_get("azuread", "client_id")
                    and _secret_get("azuread", "tenant_id")
                    and _secret_get("azuread", "client_secret")
                    and _secret_get("azuread", "redirect_uri")
                )
                info = {
                    "user_present": bool(current_user()),
                    "flow_pref": flow_pref,
                    "have_conf": have_conf,
                    "auth_mode": auth_mode_summary(),
                    "client_id": _mask(_secret_get("azuread", "client_id")),
                    "redirect_uri": _secret_get("azuread", "redirect_uri"),
                    "_msal_auto_started": bool(st.session_state.get("_msal_auto_started")),
                    "_msal_flow_set": bool(st.session_state.get("_msal_flow")),
                    "_aad_token_cached": bool(st.session_state.get("_aad_token")),
                    "_flow_cache_size": len(_FLOW_CACHE),
                }
                st.code(info)
                params = _get_query_params()
                if params:
                    st.caption("Query params (masked code):")
                    st.code({k: (_mask(v) if k == "code" else v) for k, v in params.items()})
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Clear SSO state", key="btn_clear_sso_state"):
                        _logout()
                        _set_query_params()
                        st.success("SSO state cleared. Reloading…")
                        st.rerun()
                with c2:
                    if st.button("Restart SSO (authcode)", key="btn_restart_sso"):
                        _authcode_begin(auto=True)
            except Exception as e:
                st.warning(f"Debug panel error: {e}")

    # Global debug toggle (admin only) placed below the icon
    try:
        from core.debug import is_debug_enabled
        st.session_state["debug_toggle_enable_render"] = True
        is_debug_enabled(label="Debug", key="debug_mode")
    except Exception:
        pass

    # Brand mark (bottom sidebar, centered) - keep below debug toggle
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    if icon_path.exists():
        try:
            import base64

            icon_b64 = base64.b64encode(icon_path.read_bytes()).decode("ascii")
        except Exception:
            icon_b64 = ""
        if icon_b64:
            st.markdown(
                f"<div class='tco-sidebar-logo' style='display:flex;justify-content:center;align-items:center;'>"
                f"<img src='data:image/png;base64,{icon_b64}' alt='Sidebar logo' "
                "style='width:60px;height:auto;display:block;'/></div>",
                unsafe_allow_html=True,
            )


def render_auth_sidebar() -> None:
    # Ensure the auth widget always renders in the sidebar, even if called from main/page contexts.
    run_nonce = st.session_state.get("_tco_run_nonce")
    if run_nonce is not None:
        if st.session_state.get("_auth_sidebar_rendered") == run_nonce:
            return
        st.session_state["_auth_sidebar_rendered"] = run_nonce
    else:
        if st.session_state.get("_auth_sidebar_rendered"):
            return
        st.session_state["_auth_sidebar_rendered"] = True
    with st.sidebar:
        _render_auth_sidebar_inner()


def render_login_ui(location: str = "sidebar") -> bool:
    """
    Render login controls in the requested location.
    Returns True when rendering was attempted successfully.
    """
    try:
        render_auth_sidebar()
        return True
    except Exception:
        return False


# -------------------------
# Public UI helpers
# -------------------------
def login_ui(page_path: Optional[str] = None) -> None:
    """Render sign-in prompt or auto-start SSO."""
    if page_path:
        st.session_state["auth_page_path_hint"] = str(page_path)
    just_logged_in = _consume_auth_just_logged_in_flag()
    try:
        ensure_access_control_tables()
    except Exception:
        pass

    expired = _maybe_expire_session()

    # Order matters: complete first (uses existing flow), then maybe start
    _try_front_channel_logout()
    try:
        _try_complete_authcode()
    except Exception as e:
        st.error(f"SSO callback failed: {e}")
        if st.button("Try again", key="btn_auth_retry_callback"):
            _set_query_params()
            st.rerun()

    if not current_user() and not just_logged_in:
        _try_auto_login_admin()
    if not current_user():
        _remember_post_auth_page(page_path)

    # Decide autostart before rendering sidebar (reduce flicker)
    try:
        flow_pref_raw = str(_secret_get("azuread", "flow", "") or "").strip().lower()
        flow_pref = flow_pref_raw or "authcode"  # default to authcode when config is present
        have_conf = bool(
            _secret_get("azuread", "client_id")
            and _secret_get("azuread", "tenant_id")
            and _secret_get("azuread", "client_secret")
            and _secret_get("azuread", "redirect_uri")
        )
    except Exception:
        flow_pref = ""
        have_conf = False

    params_now = _get_query_params()
    should_autostart = (
        not current_user()
        and _sso_required()
        and have_conf
        and flow_pref in {"authcode", "auth_code", "code"}
        and not st.session_state.get("_msal_auto_started")
        and ("code" not in params_now)
    )

    if should_autostart:
        st.session_state["_msal_auto_started"] = True
        _authcode_begin(auto=True, post_auth_page=page_path)

    # Now render sidebar
    render_auth_sidebar()

    # Small main-area debug box (only when sso_debug=true)
    if bool(_secret_get("auth", "sso_debug", False)):
        qp_now = _get_query_params()
        info = {
            "user_present": bool(current_user()),
            "auth_mode": auth_mode_summary(),
            "_msal_flow_set": bool(st.session_state.get("_msal_flow")),
            "_msal_auto_started": bool(st.session_state.get("_msal_auto_started")),
            "query_params": qp_now,
            "query_has_code": "code" in qp_now,
            "query_has_error": "error" in qp_now,
            "redirect_uri": _mask(_secret_get("azuread", "redirect_uri")),
            "_flow_cache_size": len(_FLOW_CACHE),
        }
        st.caption("SSO Debug (main view)")
        st.code(info)

    expired_flag = bool(expired or st.session_state.pop("_auth_expired_notice", False))
    if not current_user() and not should_autostart:
        if expired_flag:
            st.warning("Your session expired after inactivity. Please sign in again using the sidebar.")
        render_login_ui(location="sidebar")


def preflight_auth(page_path: Optional[str] = None, *, autostart: bool = True) -> None:
    """Run auth completion/auto-start without rendering UI."""
    just_logged_in = _consume_auth_just_logged_in_flag()
    try:
        ensure_access_control_tables()
    except Exception:
        pass

    _maybe_expire_session()
    _try_front_channel_logout()
    _try_complete_authcode()

    if not current_user() and not just_logged_in:
        _try_auto_login_admin()
    if not current_user():
        _remember_post_auth_page(page_path)

    # Auto-start auth code flow when configured (SSO only).
    try:
        flow_pref_raw = str(_secret_get("azuread", "flow", "") or "").strip().lower()
        flow_pref = flow_pref_raw or "authcode"  # default to authcode when config is present
        have_conf = bool(
            _secret_get("azuread", "client_id")
            and _secret_get("azuread", "tenant_id")
            and _secret_get("azuread", "client_secret")
            and _secret_get("azuread", "redirect_uri")
        )
    except Exception:
        flow_pref = ""
        have_conf = False

    params_now = get_qp()
    should_autostart = (
        not current_user()
        and _sso_required()
        and have_conf
        and flow_pref in {"authcode", "auth_code", "code"}
        and not st.session_state.get("_msal_auto_started")
        and ("code" not in params_now)
    )

    if autostart and should_autostart:
        st.session_state["_msal_auto_started"] = True
        _authcode_begin(auto=True, post_auth_page=page_path)


def ensure_sso(
    min_role: str = "VIEWER",
    page_name: str = "",
    page_path: Optional[str] = None,
    *,
    render_sidebar: bool = True,
) -> Optional[dict]:
    """
    Centralized SSO guard for pages.
    - Keeps the same authcode completion/auto-start logic.
    - Shows a banner + sidebar action when session is missing/expired.
    """
    st.session_state["_tco_auth_sidebar_call_counter"] = 0
    just_logged_in = _consume_auth_just_logged_in_flag()
    try:
        ensure_access_control_tables()
    except Exception:
        pass

    expired = _maybe_expire_session()
    _try_front_channel_logout()
    _try_complete_authcode()

    user = current_user()
    if not user:
        _remember_post_auth_page(page_path)
    else:
        st.session_state.pop("_app_block_reason", None)
    expired_flag = bool(expired or st.session_state.pop("_auth_expired_notice", False))

    # Auto-start auth code flow (same logic as login_ui) when configured
    try:
        flow_pref = str(_secret_get("azuread", "flow", "")).strip().lower()
        have_conf = bool(
            _secret_get("azuread", "client_id")
            and _secret_get("azuread", "tenant_id")
            and _secret_get("azuread", "client_secret")
            and _secret_get("azuread", "redirect_uri")
        )
    except Exception:
        flow_pref = ""
        have_conf = False
    params_now = _get_query_params()
    should_autostart = (
        not user
        and _sso_required()
        and have_conf
        and flow_pref in {"authcode", "auth_code", "code"}
        and not st.session_state.get("_msal_auto_started")
        and ("code" not in params_now)
    )
    if should_autostart:
        st.session_state["_msal_auto_started"] = True
        _authcode_begin(auto=True, post_auth_page=page_path)

    if bool(_secret_get("auth", "sso_debug", False)):
        qp_now = _get_query_params()
        st.caption("SSO Debug (ensure_sso)")
        st.code(
            {
                "query_params": qp_now,
                "query_has_code": "code" in qp_now,
                "query_has_error": "error" in qp_now,
                "_msal_flow_set": bool(st.session_state.get("_msal_flow")),
                "redirect_uri": _mask(_secret_get("azuread", "redirect_uri")),
            }
        )

    if not user:
        # Dev aid: impersonate a user (no SSO); resolve role from DB when possible
        imp_email = _secret_get("auth", "impersonate")
        imp_role = _secret_get("auth", "impersonate_role")
        if imp_email:
            rec = get_user_by_email(str(imp_email))
            role = (str(rec.get("ROLE")) if rec else None) or (str(imp_role) if imp_role else "VIEWER")
            role = _normalize_non_admin_role(role, default="VIEWER")
            user = {"email": str(imp_email), "name": str(imp_email), "role": role.upper(), "provider": "IMPERSONATE"}
            _save_user(user)

    if not user and not just_logged_in:
        # Auto-login admin from secrets (no UI) when configured
        user = _try_auto_login_admin()

    if not user:
        st.session_state["_app_block_reason"] = "NOT_AUTHENTICATED"
        st.stop()

    if _sso_required():
        try:
            identity_email = get_identity_email()
        except AuthIdentityError:
            st.session_state["_app_block_reason"] = "IDENTITY_MISSING"
            st.error("SSO identity missing")
            st.stop()
    else:
        identity_email = str(get_current_user_email() or user.get("email") or "").strip().lower()
        if not identity_email:
            st.session_state["_app_block_reason"] = "IDENTITY_MISSING"
            st.stop()
        st.session_state["_auth_identity_source"] = "local-auth"

    user["email"] = identity_email
    st.session_state["user_email"] = identity_email

    # Resolve routing eligibility before portfolio-local authorization when no active portfolio is selected.
    try:
        from core.portfolio_runtime import get_active_portfolio_key, set_active_portfolio
        from db.control_db import control_db_available, resolve_allowed_portfolios

        active_portfolio_key = str(get_active_portfolio_key() or "").strip()
        if not active_portfolio_key and control_db_available():
            allowed = resolve_allowed_portfolios(identity_email, get_identity_groups())
            if len(allowed) == 1:
                set_active_portfolio(str(allowed[0]).strip(), email=identity_email, role=str(st.session_state.get("role") or ""))
            elif len(allowed) == 0:
                st.session_state["_app_block_reason"] = "NO_PORTFOLIO_ACCESS"
                st.error("No portfolio access granted. Contact an admin.")
                st.stop()
    except Exception:
        pass

    rec = get_user_by_email(identity_email)
    global_admin, portfolio_role = _resolve_admin_membership(identity_email)
    effective_role = "ADMIN" if (global_admin or portfolio_role == "ADMIN") else ""
    local_provider = str(st.session_state.get("auth_provider") or user.get("provider") or "").strip().lower()
    local_role = str(st.session_state.get("auth_role") or user.get("role") or "").strip().upper()
    if not effective_role and not _sso_required() and local_provider == "local" and local_role == "ADMIN":
        effective_role = "ADMIN"

    if not effective_role:
        if not rec:
            st.session_state["_app_block_reason"] = "NOT_AUTHORIZED"
            st.stop()
        if not bool(rec.get("IS_ACTIVE", True)):
            st.session_state["_app_block_reason"] = "INACTIVE_USER"
            st.stop()
        db_role = str(rec.get("ROLE") or "VIEWER").strip().upper() or "VIEWER"
        effective_role = db_role

    user["role"] = effective_role
    st.session_state["role"] = effective_role
    _save_user(user)
    _set_auth_debug_state(
        str(st.session_state.get("_auth_identity_source") or "missing"),
        identity_email,
        global_admin,
        portfolio_role,
        effective_role,
    )

    have = _role_level(effective_role)
    need = _role_level(min_role)
    if have < need:
        st.session_state["_app_block_reason"] = "INSUFFICIENT_PERMISSIONS"
        st.stop()

    st.session_state.pop("_app_block_reason", None)
    return user


def require_role(
    min_role: str = "VIEWER", page_name: str = "", page_path: Optional[str] = None
) -> dict:
    """Enforce minimum role and SSO on protected pages."""
    just_logged_in = _consume_auth_just_logged_in_flag()
    _maybe_expire_session()
    _try_front_channel_logout()
    _try_complete_authcode()

    user = current_user()
    if not user:
        # Dev aid: impersonate a user (no SSO); resolve role from DB when possible
        imp_email = _secret_get("auth", "impersonate")
        imp_role = _secret_get("auth", "impersonate_role")
        if imp_email:
            rec = get_user_by_email(str(imp_email))
            role = (str(rec.get("ROLE")) if rec else None) or (str(imp_role) if imp_role else "VIEWER")
            role = _normalize_non_admin_role(role, default="VIEWER")
            user = {"email": str(imp_email), "name": str(imp_email), "role": role.upper(), "provider": "IMPERSONATE"}
            _save_user(user)
        else:
            # Auto-login admin from secrets (no UI) when configured
            user = None if just_logged_in else _try_auto_login_admin()
            if not user:
                login_ui(page_path=page_path)
                st.stop()

    if _sso_required():
        try:
            identity_email = get_identity_email()
        except AuthIdentityError:
            st.error("SSO identity missing")
            st.stop()
    else:
        identity_email = str(get_current_user_email() or user.get("email") or "").strip().lower()
        if not identity_email:
            st.stop()
        st.session_state["_auth_identity_source"] = "local-auth"

    user["email"] = identity_email
    st.session_state["user_email"] = identity_email

    rec = get_user_by_email(identity_email)
    global_admin, portfolio_role = _resolve_admin_membership(identity_email)
    effective_role = "ADMIN" if (global_admin or portfolio_role == "ADMIN") else ""
    local_provider = str(st.session_state.get("auth_provider") or user.get("provider") or "").strip().lower()
    local_role = str(st.session_state.get("auth_role") or user.get("role") or "").strip().upper()
    if not effective_role and not _sso_required() and local_provider == "local" and local_role == "ADMIN":
        effective_role = "ADMIN"

    if not effective_role:
        if not rec:
            st.error("You are not authorized to use this application.")
            st.stop()
        if not bool(rec.get("IS_ACTIVE", True)):
            st.error("Your account is inactive. Contact an admin.")
            st.stop()
        db_role = str(rec.get("ROLE") or "VIEWER").strip().upper() or "VIEWER"
        effective_role = db_role

    have = _role_level(effective_role)
    need = _role_level(min_role)
    if have < need:
        st.error(f"Insufficient permissions for this page. Required: {min_role}")
        st.stop()

    user["role"] = effective_role
    st.session_state["role"] = effective_role
    _save_user(user)
    _set_auth_debug_state(
        str(st.session_state.get("_auth_identity_source") or "missing"),
        identity_email,
        global_admin,
        portfolio_role,
        effective_role,
    )
    return user
