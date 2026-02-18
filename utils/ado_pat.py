from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional, Tuple

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # type: ignore

try:
    from db import get_profile_pat, ALLOW_DB_PAT_FALLBACK  # type: ignore
except Exception:  # pragma: no cover
    get_profile_pat = None  # type: ignore
    ALLOW_DB_PAT_FALLBACK = False  # type: ignore


def _normalize_profile_key(profile_key: str) -> str:
    key = str(profile_key or "").strip()
    if not key:
        return ""
    key = re.sub(r"[^A-Za-z0-9]+", "_", key).strip("_")
    return key.upper()


def resolve_ado_pat(profile_key: str, pat_env_key: str = "") -> Optional[str]:
    pat, _source = resolve_ado_pat_source(profile_key, pat_env_key=pat_env_key)
    return pat


def resolve_ado_pat_source(profile_key: str, pat_env_key: str = "") -> Tuple[Optional[str], str]:
    key_raw = str(profile_key or "").strip()
    key_norm = _normalize_profile_key(key_raw)
    key_env = f"ADO_PAT_{key_raw}" if key_raw else ""
    key_env_norm = f"ADO_PAT_{key_norm}" if key_norm else ""
    pat_env_key = str(pat_env_key or "").strip()

    if key_env and os.getenv(key_env):
        return os.getenv(key_env), "env"
    if key_env_norm and os.getenv(key_env_norm):
        return os.getenv(key_env_norm), "env"
    if pat_env_key and os.getenv(pat_env_key):
        return os.getenv(pat_env_key), "env"

    if st is not None:
        try:
            secret_env = st.secrets.get("env", {})  # type: ignore[attr-defined]
            if key_env and key_env in secret_env:
                val = str(secret_env.get(key_env) or "").strip()
                return (val or None), "secrets"
            if key_env_norm and key_env_norm in secret_env:
                val = str(secret_env.get(key_env_norm) or "").strip()
                return (val or None), "secrets"
            if pat_env_key and pat_env_key in secret_env:
                val = str(secret_env.get(pat_env_key) or "").strip()
                return (val or None), "secrets"
        except Exception:
            pass

        try:
            overrides = st.session_state.get("ado_pat_overrides", {})
            if key_raw and key_raw in overrides:
                val = str(overrides.get(key_raw) or "").strip()
                return (val or None), "session"
            if key_norm and key_norm in overrides:
                val = str(overrides.get(key_norm) or "").strip()
                return (val or None), "session"
        except Exception:
            pass

    if ALLOW_DB_PAT_FALLBACK and get_profile_pat is not None and key_raw:
        try:
            db_pat = get_profile_pat(key_raw)
            if db_pat:
                return db_pat, "db"
        except Exception:
            pass

    return None, "missing"
