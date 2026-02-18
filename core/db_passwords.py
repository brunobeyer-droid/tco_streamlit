from __future__ import annotations

from typing import Optional, Iterable, List
from collections.abc import Mapping
import os

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # type: ignore


def _normalize_key(raw: str) -> str:
    cleaned = "".join([c if c.isalnum() else "_" for c in (raw or "").upper()])
    return cleaned.strip("_")


def resolve_portfolio_db_password(
    password_key: Optional[str],
    fallback_portfolio_key: Optional[str] = None,
    profile_key: Optional[str] = None,
) -> Optional[str]:
    candidates: List[str] = []
    for raw in (password_key, fallback_portfolio_key, profile_key):
        if raw and str(raw).strip():
            candidates.append(str(raw).strip())
    if not candidates:
        return None
    secrets_map = {}
    if st is not None:
        try:
            secrets_map = st.secrets.get("portfolio_db_passwords", {})  # type: ignore[attr-defined]
        except Exception:
            secrets_map = {}
    for key in candidates:
        k = str(key).strip()
        if secrets_map and isinstance(secrets_map, Mapping):
            if k in secrets_map:
                return str(secrets_map.get(k) or "") or None
            k_norm = k.lower()
            if k_norm in secrets_map:
                return str(secrets_map.get(k_norm) or "") or None
        env1 = f"PORTFOLIO_DB_PASSWORD_{_normalize_key(k)}"
        env2 = f"PORTFOLIO_DB_PASSWORD_{_normalize_key(k.lower())}"
        val = os.getenv(env1) or os.getenv(env2)
        if val:
            return val
    return None
