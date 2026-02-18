from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

import pandas as pd

from core.data import fetch_cost_lines


@dataclass(frozen=True)
class BudgetScenarioContext:
    baseline: pd.DataFrame
    expected: pd.DataFrame
    work_scope: pd.DataFrame
    demand_multi: Optional[pd.DataFrame] = None
    cap_multi: Optional[pd.DataFrame] = None


def _first_year(years_sel: Optional[Sequence[int]], outlook_year: Optional[int]) -> Optional[int]:
    if outlook_year is not None:
        return int(outlook_year)
    if not years_sel:
        return None
    try:
        return int(list(years_sel)[0])
    except Exception:
        return None


def build_budget_context(
    *,
    years_sel: Optional[Sequence[int]] = None,
    programs_for_query: Sequence[str] = (),
    teams_sel: Sequence[str] = (),
    groups_sel: Sequence[str] = (),
    rev: Optional[int] = None,
    outlook_year: Optional[int] = None,
) -> BudgetScenarioContext:
    """Shared Budget scenario loader (Baseline + Expected) for a given scope.

    Notes:
    - `baseline` and `expected` are loaded with the provided scope filters.
    - `work_scope` loads EXPECTED for the same year/program scope, without team/group filters,
      so callers can allocate program-level costs down to apps when needed.
    """
    year = _first_year(years_sel, outlook_year)
    if year is None:
        empty = pd.DataFrame()
        return BudgetScenarioContext(baseline=empty, expected=empty, work_scope=empty, demand_multi=None, cap_multi=None)

    programs = tuple(str(p) for p in (programs_for_query or ()) if str(p).strip())
    teams = tuple(str(t) for t in (teams_sel or ()) if str(t).strip())
    groups = tuple(str(g) for g in (groups_sel or ()) if str(g).strip())

    baseline = fetch_cost_lines(
        fiscal_year=int(year),
        scenario="BASELINE",
        programs=programs,
        teams=teams,
        app_groups=groups,
        rev=rev,
    )
    expected = fetch_cost_lines(
        fiscal_year=int(year),
        scenario="EXPECTED",
        programs=programs,
        teams=teams,
        app_groups=groups,
        rev=rev,
    )
    work_scope = fetch_cost_lines(
        fiscal_year=int(year),
        scenario="EXPECTED",
        programs=programs,
        teams=tuple(),
        app_groups=tuple(),
        rev=rev,
    )
    return BudgetScenarioContext(baseline=baseline, expected=expected, work_scope=work_scope, demand_multi=None, cap_multi=None)

