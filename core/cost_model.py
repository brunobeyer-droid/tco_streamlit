from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
from pathlib import Path

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio
from core import canonical_costs as canonical_costs_module
from core.canonical_costs import CANONICAL_COSTS_VERSION
from core.data import fetch_cost_lines, get_cost_query_warning
from core.freshness import get_data_freshness_token


def _as_tuple(values: Any) -> Tuple[str, ...]:
    if values is None:
        return tuple()
    if isinstance(values, (list, tuple, set)):
        out = [str(v).strip() for v in values if v is not None and str(v).strip()]
        return tuple(sorted(out))
    s = str(values).strip()
    return (s,) if s else tuple()


def _mtime_ns(path: str | None) -> int:
    try:
        if not path:
            return 0
        return int(Path(path).stat().st_mtime_ns)
    except Exception:
        return 0


def _code_freshness_token() -> str:
    # Invalidate in-session model cache when core cost code changes during local dev.
    data_code_file = getattr(getattr(fetch_cost_lines, "__code__", None), "co_filename", None)
    canonical_file = getattr(canonical_costs_module, "__file__", None)
    return "|".join(
        [
            str(_mtime_ns(__file__)),
            str(_mtime_ns(canonical_file)),
            str(_mtime_ns(data_code_file)),
        ]
    )


def _scope_key(year: int, scope: Any, *, include_actual: bool = True, include_baseline: bool = True) -> tuple:
    portfolio_key = str(st.session_state.get("active_portfolio_key") or "").strip()
    freshness_token = str(get_data_freshness_token() or "").strip()
    canonical_version = str(CANONICAL_COSTS_VERSION or "").strip()
    code_freshness = _code_freshness_token()
    programs = _as_tuple(getattr(scope, "programs", None) if scope is not None else None)
    teams = _as_tuple(getattr(scope, "teams", None) if scope is not None else None)
    groups = _as_tuple(getattr(scope, "groups", None) if scope is not None else None)

    if isinstance(scope, dict):
        programs = _as_tuple(scope.get("programs") or scope.get("program") or programs)
        teams = _as_tuple(scope.get("teams") or scope.get("team") or teams)
        groups = _as_tuple(scope.get("groups") or scope.get("app_groups") or scope.get("app_group") or groups)

    return (
        portfolio_key,
        freshness_token,
        canonical_version,
        code_freshness,
        int(year),
        programs,
        teams,
        groups,
        bool(include_actual),
        bool(include_baseline),
    )


@cache_data_portfolio(ttl=300, show_spinner=False)
def _load_cost_model_cached(
    *,
    year: int,
    programs: Tuple[str, ...],
    teams: Tuple[str, ...],
    groups: Tuple[str, ...],
    include_actual: bool = True,
    include_baseline: bool = True,
) -> Dict[str, pd.DataFrame]:
    """Cached cost-model loader used by heavy pages (Welcome/Dashboard/Insights)."""
    baseline = pd.DataFrame()
    expected = pd.DataFrame()
    actual = pd.DataFrame()

    if include_actual:
        all_lines = fetch_cost_lines(
            fiscal_year=int(year),
            scenario=None,
            programs=programs,
            teams=teams,
            app_groups=groups,
        )
        if all_lines is None or all_lines.empty or "SCENARIO" not in all_lines.columns:
            return {"BASELINE": pd.DataFrame(), "EXPECTED": pd.DataFrame(), "ACTUAL": pd.DataFrame()}

        scen = all_lines["SCENARIO"].fillna("").astype(str).str.upper().str.strip()
        baseline_set = {"BASELINE", "PLANNED", "PLAN", "BUDGET", "PLANNED_BASELINE", "PLANNED_BASELINE_TOTAL"}
        expected_set = {"EXPECTED", "FORECAST", "PROJECTED"}
        actual_set = {"ACTUAL"}

        baseline = all_lines.loc[scen.isin(baseline_set)].copy()
        expected = all_lines.loc[scen.isin(expected_set)].copy()
        actual = all_lines.loc[scen.isin(actual_set)].copy()
    else:
        if include_baseline:
            baseline = fetch_cost_lines(
                fiscal_year=int(year),
                scenario="BASELINE",
                programs=programs,
                teams=teams,
                app_groups=groups,
            )
        else:
            baseline = pd.DataFrame()
        expected = fetch_cost_lines(
            fiscal_year=int(year),
            scenario="EXPECTED",
            programs=programs,
            teams=teams,
            app_groups=groups,
        )
        if baseline is None:
            baseline = pd.DataFrame()
        if expected is None:
            expected = pd.DataFrame()

    if not baseline.empty:
        baseline["SCENARIO"] = "BASELINE"
    if not expected.empty:
        expected["SCENARIO"] = "EXPECTED"
    if include_actual and not actual.empty:
        actual["SCENARIO"] = "ACTUAL"
    return {"BASELINE": baseline, "EXPECTED": expected, "ACTUAL": actual}


def load_cost_model(
    year: int,
    scope: Any,
    *,
    include_actual: bool = True,
    include_baseline: bool = True,
) -> Dict[str, pd.DataFrame]:
    """Load (and cache) scenario cost lines for a (year, scope) slice.

    Returns a dict:
      {"BASELINE": df, "EXPECTED": df, "ACTUAL": df}

    Notes
    -----
    - Calls `fetch_cost_lines` once (all scenarios), then splits into BASELINE/EXPECTED/ACTUAL.
    - Stores the result in `st.session_state["COST_MODEL"]`.
    - Reuses the existing model when `year` and `scope` match.
    """
    y = int(year)
    key = _scope_key(y, scope, include_actual=include_actual, include_baseline=include_baseline)

    cache = st.session_state.get("COST_MODEL_CACHE")
    if isinstance(cache, dict):
        hit = cache.get(key)
        if isinstance(hit, dict) and all(k in hit for k in ("BASELINE", "EXPECTED", "ACTUAL")):
            st.session_state["COST_MODEL"] = hit
            st.session_state["COST_MODEL_KEY"] = key
            return {"BASELINE": hit["BASELINE"], "EXPECTED": hit["EXPECTED"], "ACTUAL": hit["ACTUAL"]}
    else:
        cache = {}

    # key shape: (portfolio_key, freshness_token, canonical_version, code_freshness, year, programs, teams, groups, include_actual, include_baseline)
    programs = key[5]
    teams = key[6]
    groups = key[7]
    include_actual_cached = bool(key[8])
    include_baseline_cached = bool(key[9])

    model = _load_cost_model_cached(
        year=y,
        programs=programs,
        teams=teams,
        groups=groups,
        include_actual=include_actual_cached,
        include_baseline=include_baseline_cached,
    )
    base_dbg = model.get("BASELINE", pd.DataFrame())
    exp_dbg = model.get("EXPECTED", pd.DataFrame())
    act_dbg = model.get("ACTUAL", pd.DataFrame())
    debug_counts = {
        "BASELINE": int(base_dbg.shape[0]) if isinstance(base_dbg, pd.DataFrame) else 0,
        "EXPECTED": int(exp_dbg.shape[0]) if isinstance(exp_dbg, pd.DataFrame) else 0,
        "ACTUAL": int(act_dbg.shape[0]) if isinstance(act_dbg, pd.DataFrame) else 0,
    }
    st.session_state["COST_MODEL_DEBUG_SCENARIOS"] = [k for k, v in debug_counts.items() if int(v or 0) > 0]
    st.session_state["COST_MODEL_DEBUG_COUNTS"] = debug_counts
    st.session_state["COST_MODEL"] = model
    st.session_state["COST_MODEL_KEY"] = key
    try:
        cache[key] = model
        st.session_state["COST_MODEL_CACHE"] = cache
    except Exception:
        pass
    return model


def get_cost_model_warning(
    year: int,
    scope: Any,
    *,
    include_actual: bool = True,
    include_baseline: bool = True,
) -> Optional[str]:
    """Return the latest scoped cost-model warning for a (year, scope) slice."""
    key = _scope_key(int(year), scope, include_actual=include_actual, include_baseline=include_baseline)
    programs = key[5]
    teams = key[6]
    groups = key[7]
    if include_actual:
        return get_cost_query_warning(
            fiscal_year=int(year),
            scenario=None,
            programs=programs,
            teams=teams,
            app_groups=groups,
        )
    warn_baseline = None
    if include_baseline:
        warn_baseline = get_cost_query_warning(
            fiscal_year=int(year),
            scenario="BASELINE",
            programs=programs,
            teams=teams,
            app_groups=groups,
        )
    warn_expected = get_cost_query_warning(
        fiscal_year=int(year),
        scenario="EXPECTED",
        programs=programs,
        teams=teams,
        app_groups=groups,
    )
    parts = [str(w).strip() for w in (warn_baseline, warn_expected) if str(w or "").strip()]
    if not parts:
        return None
    return " | ".join(parts)
