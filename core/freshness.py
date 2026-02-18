from __future__ import annotations

import os
import time
from typing import Any

import streamlit as st

from db import bump_data_version, get_data_version, get_data_version_info


def _active_portfolio_key() -> str:
    try:
        return str(st.session_state.get("active_portfolio_key") or "").strip()
    except Exception:
        return ""


def _freshness_v2_enabled() -> bool:
    try:
        raw = str(os.getenv("TCO_FRESHNESS_V2", "1") or "1").strip().lower()
        return raw in {"1", "true", "yes", "on"}
    except Exception:
        return True


def _freshness_token_ttl_sec(default: int = 3) -> int:
    try:
        raw = str(os.getenv("TCO_FRESHNESS_TOKEN_TTL_SEC", str(default)) or str(default)).strip()
        val = int(float(raw))
        return max(1, val)
    except Exception:
        return int(default)


def _session_cache_get(key: str) -> Any:
    try:
        payload = st.session_state.get(key)
        if not isinstance(payload, dict):
            return None
        exp_at = float(payload.get("exp_at") or 0.0)
        if exp_at <= time.time():
            return None
        return payload.get("value")
    except Exception:
        return None


def _session_cache_set(key: str, value: Any, ttl_sec: int) -> None:
    try:
        st.session_state[key] = {"value": value, "exp_at": time.time() + max(1, int(ttl_sec or 1))}
    except Exception:
        pass


def _ensure_portfolio_cache_sanity() -> None:
    """Drop per-portfolio token cache when active portfolio changes in-session."""
    try:
        current = _active_portfolio_key()
        prev = str(st.session_state.get("_freshness_last_portfolio_key") or "")
        if prev and current != prev:
            prefix = "_freshness_token:"
            for k in list(st.session_state.keys()):
                if str(k).startswith(prefix):
                    st.session_state.pop(k, None)
            st.session_state.pop("_freshness_diag_last_fetch_ts", None)
        st.session_state["_freshness_last_portfolio_key"] = current
    except Exception:
        pass


def _token_cache_entry(ttl_sec: int = 15) -> dict[str, Any]:
    _ensure_portfolio_cache_sanity()
    portfolio_key = _active_portfolio_key()
    cache_key = f"_freshness_token:{portfolio_key}"
    payload = _session_cache_get(cache_key)
    if isinstance(payload, dict):
        token = payload.get("token")
        version = payload.get("version")
        fetched_at = float(payload.get("fetched_at") or 0.0)
        if isinstance(token, str) and token:
            return {
                "token": token,
                "version": str(version or "unknown"),
                "fetched_at": fetched_at,
                "cache_hit": True,
            }

    version = "unknown"
    try:
        info = get_data_version_info()
        if isinstance(info, dict):
            v = int(info.get("version") or 0)
            version = str(v)
        else:
            version = str(int(get_data_version() or 0))
    except Exception:
        try:
            version = str(int(get_data_version() or 0))
        except Exception:
            version = "unknown"

    now = time.time()
    token = f"{portfolio_key}:{version}"
    _session_cache_set(
        cache_key,
        {"token": token, "version": version, "fetched_at": now},
        ttl_sec,
    )
    try:
        st.session_state["_freshness_diag_last_fetch_ts"] = now
    except Exception:
        pass
    return {"token": token, "version": version, "fetched_at": now, "cache_hit": False}


def get_data_freshness_token(ttl_sec: int | None = None) -> str:
    ttl = _freshness_token_ttl_sec() if ttl_sec is None else max(1, int(ttl_sec))
    entry = _token_cache_entry(ttl_sec=ttl)
    token = entry.get("token")
    return str(token) if isinstance(token, str) and token else f"{_active_portfolio_key()}:unknown"


def get_freshness_diagnostics(ttl_sec: int | None = None) -> dict[str, Any]:
    ttl = _freshness_token_ttl_sec() if ttl_sec is None else max(1, int(ttl_sec))
    entry = _token_cache_entry(ttl_sec=ttl)
    fetched_at = float(entry.get("fetched_at") or 0.0)
    age_sec = max(0.0, time.time() - fetched_at) if fetched_at > 0 else None
    return {
        "enabled": _freshness_v2_enabled(),
        "portfolio_key": _active_portfolio_key(),
        "data_version": entry.get("version"),
        "freshness_token": entry.get("token"),
        "cache_epoch": int(st.session_state.get("_cache_epoch", 0) or 0),
        "token_ttl_seconds": int(ttl),
        "token_age_seconds": round(age_sec, 3) if age_sec is not None else None,
        "last_fetch_ts": fetched_at or None,
    }


def render_freshness_debug_expander(title: str = "Debug - Freshness") -> None:
    try:
        with st.expander(title, expanded=False):
            st.json(get_freshness_diagnostics())
    except Exception:
        pass


def post_write_refresh(
    context: str,
    *,
    ensure_views: bool = False,
    rerun: bool = True,
    bump_version: bool | None = None,
) -> None:
    warnings: list[str] = []
    if ensure_views:
        try:
            from db import ensure_analytics_views_ok

            ensure_analytics_views_ok()
        except Exception as exc:
            warnings.append(f"analytics refresh warning: {exc}")
    should_bump = (not _freshness_v2_enabled()) if bump_version is None else bool(bump_version)
    try:
        if should_bump:
            bump_data_version(context)
    except Exception as exc:
        warnings.append(f"DATA_VERSION bump warning: {exc}")

    try:
        st.session_state["_cache_epoch"] = int(st.session_state.get("_cache_epoch", 0) or 0) + 1
    except Exception:
        pass
    try:
        st.session_state.pop(f"_freshness_token:{_active_portfolio_key()}", None)
    except Exception:
        pass

    if warnings:
        st.warning("Saved with refresh warnings: " + " | ".join(warnings))
    if rerun:
        st.rerun()
