from __future__ import annotations

from typing import Any, Dict, Optional

import streamlit as st


def _normalize(qp: Dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (qp or {}).items():
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            if not v:
                continue
            v = v[0]
        out[str(k)] = str(v)
    return out


def get_qp() -> dict[str, str]:
    if hasattr(st, "query_params"):
        try:
            return _normalize(dict(st.query_params))
        except Exception:
            pass
    try:
        qp = st.experimental_get_query_params()
        return _normalize(qp)
    except Exception:
        return {}


def set_qp(**updates: Optional[str]) -> None:
    if not updates:
        return
    if hasattr(st, "query_params"):
        try:
            for k, v in updates.items():
                if v is None:
                    try:
                        del st.query_params[k]
                    except Exception:
                        pass
                else:
                    st.query_params[k] = str(v)
            return
        except Exception:
            pass
    params = get_qp()
    for k, v in updates.items():
        if v is None:
            params.pop(k, None)
        else:
            params[k] = str(v)
    try:
        st.experimental_set_query_params(**params)
    except Exception:
        pass


def clear_qp(*keys: str) -> None:
    if not keys:
        return
    if hasattr(st, "query_params"):
        try:
            for k in keys:
                try:
                    del st.query_params[k]
                except Exception:
                    pass
            return
        except Exception:
            pass
    params = get_qp()
    for k in keys:
        params.pop(k, None)
    try:
        st.experimental_set_query_params(**params)
    except Exception:
        pass
