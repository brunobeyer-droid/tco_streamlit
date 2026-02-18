from __future__ import annotations

"""
Compatibility shims for portfolio-scoped caching.

Some pages historically used `cache_data_portfolio` / `cache_resource_portfolio`.
They behave like `st.cache_data` / `st.cache_resource`, but key the cache by the
active portfolio so different portfolios don't share cached results.
"""

from functools import wraps
from typing import Any, Callable, Optional, TypeVar

import streamlit as st

from core.freshness import get_data_freshness_token

T = TypeVar("T")


def _active_portfolio_key() -> str:
    try:
        return str(st.session_state.get("active_portfolio_key") or "").strip()
    except Exception:
        return ""


def _cache_epoch() -> int:
    try:
        return int(st.session_state.get("_cache_epoch", 0) or 0)
    except Exception:
        return 0


def _cache_stats_enabled() -> bool:
    try:
        return bool(st.session_state.get("debug_mode"))
    except Exception:
        return False


def _cache_stats_inc(fn_id: str, *, miss: bool) -> None:
    if not _cache_stats_enabled():
        return
    try:
        stats = st.session_state.setdefault("_tco_cache_stats", {"calls": 0, "misses": 0, "funcs": {}})
        stats["calls"] = int(stats.get("calls", 0)) + 1
        stats["misses"] = int(stats.get("misses", 0)) + (1 if miss else 0)
        funcs = stats.setdefault("funcs", {})
        f = funcs.setdefault(fn_id, {"calls": 0, "misses": 0})
        f["calls"] = int(f.get("calls", 0)) + 1
        f["misses"] = int(f.get("misses", 0)) + (1 if miss else 0)
    except Exception:
        pass


def _fn_namespace(fn: Callable[..., Any]) -> str:
    """Build a stable, page-safe cache namespace for a function.

    Streamlit multipage scripts often run under `__main__`, so module+name alone can
    collide across pages. Include source file and first line to prevent collisions.
    """
    try:
        code = getattr(fn, "__code__", None)
        filename = str(getattr(code, "co_filename", "") or "")
        firstlineno = int(getattr(code, "co_firstlineno", 0) or 0)
        qualname = str(getattr(fn, "__qualname__", getattr(fn, "__name__", "fn")) or "fn")
        module = str(getattr(fn, "__module__", "") or "")
        return f"{module}:{filename}:{firstlineno}:{qualname}"
    except Exception:
        return f"{getattr(fn, '__module__', '')}.{getattr(fn, '__name__', 'fn')}"


def cache_data_portfolio(*, ttl: Optional[int] = None, show_spinner: bool = False, **kwargs: Any):
    """Like st.cache_data, but automatically scoped by active_portfolio_key."""

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        fn_id = _fn_namespace(fn)

        @st.cache_data(ttl=ttl, show_spinner=show_spinner, **kwargs)
        def _cached(
            _fn_namespace: str,
            _portfolio_key: str,
            _cache_epoch: int,
            _data_freshness_token: str,
            *args: Any,
            **kw: Any,
        ) -> T:
            _cache_stats_inc(fn_id, miss=True)
            return fn(*args, **kw)

        @wraps(fn)
        def wrapper(*args: Any, **kw: Any) -> T:
            _cache_stats_inc(fn_id, miss=False)
            # Include function namespace in cache key so different cached wrappers
            # never share entries even when args/signatures coincide.
            return _cached(
                fn_id,
                _active_portfolio_key(),
                _cache_epoch(),
                get_data_freshness_token(),
                *args,
                **kw,
            )

        return wrapper

    return decorator


def cache_resource_portfolio(*, show_spinner: bool = False, **kwargs: Any):
    """Like st.cache_resource, but automatically scoped by active_portfolio_key."""

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        fn_id = _fn_namespace(fn)

        @st.cache_resource(show_spinner=show_spinner, **kwargs)
        def _cached(
            _fn_namespace: str,
            _portfolio_key: str,
            *args: Any,
            **kw: Any,
        ) -> T:
            _cache_stats_inc(fn_id, miss=True)
            return fn(*args, **kw)

        @wraps(fn)
        def wrapper(*args: Any, **kw: Any) -> T:
            _cache_stats_inc(fn_id, miss=False)
            # Resource caches should remain stable for a portfolio and not churn on
            # data-epoch bumps triggered by normal CRUD saves.
            return _cached(fn_id, _active_portfolio_key(), *args, **kw)

        return wrapper

    return decorator
