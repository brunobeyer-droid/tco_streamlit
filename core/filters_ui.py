from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from core.scope import read_scope_from_session, scope_label
from db import fetch_df_active as fetch_df, list_programs, list_teams


def _as_clean_list(val: Any) -> List[str]:
    if not val:
        return []
    if isinstance(val, (list, tuple, set)):
        out = [str(x).strip() for x in val if str(x).strip()]
        return list(dict.fromkeys(out))
    s = str(val).strip()
    return [s] if s else []


def _norm_sig(items: List[str]) -> Tuple[str, ...]:
    return tuple(sorted({str(x).strip() for x in (items or []) if str(x).strip()}))


def _drop_unassigned_if_mixed(values: List[str]) -> List[str]:
    vals = [str(v).strip() for v in (values or []) if str(v).strip()]
    if not vals:
        return []
    placeholders = {"(UNASSIGNED)", "UNASSIGNED", "(UNMAPPED APPLICATION)", "(UNMAPPED APP GROUP)"}
    has_real = any(str(v).strip().upper() not in placeholders for v in vals)
    if not has_real:
        return vals
    return [v for v in vals if str(v).strip().upper() not in placeholders]


def _df_filter_options(base_df: pd.DataFrame, *, selected_year: int) -> Dict[str, List[str]]:
    if base_df is None or base_df.empty:
        return {"programs": [], "teams": [], "groups": []}
    df = base_df.copy()
    df["YEAR"] = pd.to_numeric(df.get("YEAR"), errors="coerce").astype("Int64")
    df = df.loc[df["YEAR"].eq(int(selected_year))].copy()

    def uniq(col: str) -> List[str]:
        if col not in df.columns:
            return []
        vals = df[col].dropna().astype(str).str.strip().tolist()
        vals = [v for v in vals if v]
        return sorted(set(vals))

    return {
        "programs": _drop_unassigned_if_mixed(uniq("PROGRAMNAME")),
        "teams": _drop_unassigned_if_mixed(uniq("TEAMNAME")),
        "groups": _drop_unassigned_if_mixed(uniq("GROUPNAME")),
    }


def _infer_driver_dim(*, programs: List[str], teams: List[str], groups: List[str]) -> str:
    if groups:
        return "APP_GROUP"
    if teams:
        return "APP_GROUP"
    if programs:
        return "TEAM"
    return "PROGRAM"


def render_filters_expander(
    *,
    page_key: str,
    years: List[int],
    default_year: int,
    fetch_options_fn: Callable[..., Any],
) -> Dict[str, Any]:
    """Render a unified Filters expander and return the effective filter selections.

    Returns:
      {
        "year": int,
        "programs": list[str],
        "teams": list[str],
        "groups": list[str],
        "mode": "Default" | "Custom",
        "driver_dim": "PROGRAM" | "TEAM" | "APP_GROUP",
        "nonce": int,
      }
    """

    years_clean = sorted({int(y) for y in (years or []) if str(y).strip().lstrip("-").isdigit()})
    if not years_clean:
        years_clean = [int(default_year)]

    enabled_key = f"{page_key}_filters_enabled"
    year_key = f"{page_key}_year"
    programs_key = f"{page_key}_programs"
    teams_key = f"{page_key}_teams"
    groups_key = f"{page_key}_groups"
    prev_sig_key = f"{page_key}_prev_sig"
    nonce_key = f"{page_key}_nonce"
    bumped_key = f"{page_key}_nonce_bumped"

    scope = read_scope_from_session(fetch_df)
    default_programs = list(scope.programs or [])
    default_teams = list(scope.teams or [])
    default_groups = list(scope.groups or [])
    has_default_scope = bool(default_programs or default_teams or default_groups)

    if enabled_key not in st.session_state:
        st.session_state[enabled_key] = not has_default_scope

    if year_key not in st.session_state or st.session_state.get(year_key) not in years_clean:
        safe_default = int(default_year) if int(default_year) in years_clean else int(max(years_clean))
        st.session_state[year_key] = safe_default

    # Ensure custom selections exist even when disabled (so toggling retains the last custom picks).
    st.session_state.setdefault(programs_key, list(default_programs))
    st.session_state.setdefault(teams_key, list(default_teams))
    st.session_state.setdefault(groups_key, list(default_groups))

    def _bump_nonce() -> None:
        st.session_state[nonce_key] = int(st.session_state.get(nonce_key, 0)) + 1
        st.session_state[bumped_key] = True

    with st.expander("Filters", expanded=False):
        year_index = years_clean.index(int(st.session_state[year_key])) if int(st.session_state[year_key]) in years_clean else 0
        selected_year = int(
            st.selectbox(
                "Year",
                years_clean,
                index=year_index,
                key=year_key,
                on_change=_bump_nonce,
            )
        )

        enabled = bool(
            st.checkbox(
                "Use custom filters on this page",
                key=enabled_key,
                on_change=_bump_nonce,
            )
        )

        # Pull base options once and filter locally (dynamic behavior).
        try:
            base_df = fetch_options_fn()
        except TypeError:
            base_df = fetch_options_fn(selected_year)  # type: ignore[misc]
        if base_df is None:
            base_df = pd.DataFrame()
        if not isinstance(base_df, pd.DataFrame):
            base_df = pd.DataFrame(base_df)

        base_df = base_df.copy()
        opts_all = _df_filter_options(base_df, selected_year=int(selected_year))

        # Stepwise validation so downstream options can be filtered by earlier picks.
        sel_programs = [p for p in _as_clean_list(st.session_state.get(programs_key)) if p in set(opts_all["programs"])]
        if st.session_state.get(programs_key) != sel_programs:
            st.session_state[programs_key] = sel_programs

        teams_df = base_df.copy()
        teams_df["YEAR"] = pd.to_numeric(teams_df.get("YEAR"), errors="coerce").astype("Int64")
        teams_df = teams_df.loc[teams_df["YEAR"].eq(int(selected_year))].copy()
        if sel_programs and "PROGRAMNAME" in teams_df.columns:
            teams_df = teams_df.loc[teams_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
        team_opts = sorted(set(teams_df.get("TEAMNAME", pd.Series(dtype=str)).dropna().astype(str).str.strip().tolist()))
        team_opts = _drop_unassigned_if_mixed([t for t in team_opts if t])

        sel_teams = [t for t in _as_clean_list(st.session_state.get(teams_key)) if t in set(team_opts)]
        if st.session_state.get(teams_key) != sel_teams:
            st.session_state[teams_key] = sel_teams

        groups_df = teams_df.copy()
        if sel_teams and "TEAMNAME" in groups_df.columns:
            groups_df = groups_df.loc[groups_df["TEAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_teams))].copy()
        group_opts = sorted(set(groups_df.get("GROUPNAME", pd.Series(dtype=str)).dropna().astype(str).str.strip().tolist()))
        group_opts = _drop_unassigned_if_mixed([g for g in group_opts if g])

        sel_groups = [g for g in _as_clean_list(st.session_state.get(groups_key)) if g in set(group_opts)]
        if st.session_state.get(groups_key) != sel_groups:
            st.session_state[groups_key] = sel_groups

        program_label_map: dict[str, str] = {}
        team_label_map: dict[str, str] = {}
        try:
            p_map_df = list_programs()
            if isinstance(p_map_df, pd.DataFrame) and not p_map_df.empty and "PROGRAMNAME" in p_map_df.columns:
                for _, row in p_map_df.iterrows():
                    disp = str(row.get("PROGRAMNAME") or "").strip()
                    raw = str(row.get("PROGRAMNAME_RAW") or "").strip()
                    if disp:
                        program_label_map.setdefault(disp, disp)
                    if raw and disp:
                        program_label_map[raw] = disp
        except Exception:
            pass
        try:
            t_map_df = list_teams()
            if isinstance(t_map_df, pd.DataFrame) and not t_map_df.empty and "TEAMNAME" in t_map_df.columns:
                for _, row in t_map_df.iterrows():
                    disp = str(row.get("TEAMNAME") or "").strip()
                    raw = str(row.get("TEAMNAME_RAW") or "").strip()
                    if disp:
                        team_label_map.setdefault(disp, disp)
                    if raw and disp:
                        team_label_map[raw] = disp
        except Exception:
            pass

        if enabled:
            st.multiselect(
                "Programs",
                opts_all["programs"],
                default=sel_programs,
                key=programs_key,
                on_change=_bump_nonce,
                format_func=lambda x, _m=program_label_map: _m.get(str(x).strip(), str(x).strip()),
            )
            st.multiselect(
                "Teams",
                team_opts,
                default=sel_teams,
                key=teams_key,
                on_change=_bump_nonce,
                format_func=lambda x, _m=team_label_map: _m.get(str(x).strip(), str(x).strip()),
            )
            st.multiselect(
                "Application groups",
                group_opts,
                default=sel_groups,
                key=groups_key,
                on_change=_bump_nonce,
            )
            st.caption("This override affects this page only. Your default scope is unchanged.")
        else:
            st.caption(scope_label(scope, getattr(scope, "inferred_role", "") or ""))

    # Re-read enabled from session (avoid relying on local widget variable).
    enabled = bool(st.session_state.get(enabled_key))

    # Effective selections used by the page.
    if enabled:
        eff_programs = _as_clean_list(st.session_state.get(programs_key))
        eff_teams = _as_clean_list(st.session_state.get(teams_key))
        eff_groups = _as_clean_list(st.session_state.get(groups_key))
        mode = "Custom"
    else:
        eff_programs = list(default_programs)
        eff_teams = list(default_teams)
        eff_groups = list(default_groups)
        mode = "Default"

    driver_dim = _infer_driver_dim(programs=eff_programs, teams=eff_teams, groups=eff_groups)

    sig = (
        int(st.session_state.get(year_key) or default_year),
        _norm_sig(eff_programs),
        _norm_sig(eff_teams),
        _norm_sig(eff_groups),
        bool(enabled),
    )
    prev_sig = st.session_state.get(prev_sig_key)
    bumped = bool(st.session_state.pop(bumped_key, False))
    if (prev_sig != sig) and (not bumped):
        st.session_state[prev_sig_key] = sig
        st.session_state[nonce_key] = int(st.session_state.get(nonce_key, 0)) + 1
    elif prev_sig != sig:
        st.session_state[prev_sig_key] = sig
    nonce = int(st.session_state.get(nonce_key, 0))

    return {
        "year": int(st.session_state.get(year_key) or default_year),
        "programs": eff_programs,
        "teams": eff_teams,
        "groups": eff_groups,
        "mode": mode,
        "driver_dim": driver_dim,
        "nonce": nonce,
    }
