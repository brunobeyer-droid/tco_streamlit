# Summary: update data quality UI labels to NEXT branding.
from __future__ import annotations

from datetime import date
from typing import Optional, Any

import pandas as pd
import streamlit as st

from db import fetch_df

from core.ado_recon import load_explorer_feature_rows
from core.cost_model import load_cost_model
from core.data import fetch_apptio_actuals_monthly_breakdown, fetch_filter_options

# UI consumes canonical issues backlog from core/quality
from core.quality.collector import collect_issues
from core.quality.context import QualityContext
from core.quality.evidence import resolve_evidence
from core.kpi_cards import (




    KPI_GREEN,
    KPI_GREY,
    KPI_RED,
    KPI_YELLOW,
    render_kpi_card,
)

from core.init import init_page
from utils.theme import THEME_PALETTES
from utils.query_params import set_qp

from utils.app_shell import bootstrap_page
bootstrap_page()

page_theme = init_page("Data Quality", page_path=__file__)
palette = THEME_PALETTES.get(page_theme, THEME_PALETTES["light"])
user = st.session_state.get("auth_user") or {}


def _money(x: float) -> str:
    try:
        return f"${float(x or 0.0):,.0f}"
    except Exception:
        return "$0"


def _pct(x: float) -> str:
    try:
        return f"{float(x or 0.0):,.0%}"
    except Exception:
        return "—"


def _safe_unique(df: pd.DataFrame, col: str) -> list[str]:
    if df is None or df.empty or col not in df.columns:
        return []
    vals = df[col].dropna().astype(str).str.strip()
    vals = vals[vals.ne("")]
    return sorted(set(vals.tolist()))



def _derive_scope_defaults() -> dict:
    scope = None
    try:
        from welcome.state import derive_user_scope

        scope = derive_user_scope(user, fetch_df)
    except Exception:
        scope = None

    return {
        "programs": list(getattr(scope, "programs", []) or []),
        "teams": list(getattr(scope, "teams", []) or []),
        "groups": list(getattr(scope, "groups", []) or []),
    }


def _default_year(years: list[int]) -> int:
    today_year = int(date.today().year)
    if years:
        if today_year in years:
            return today_year
        return int(max(years))
    return today_year


def _init_filter_state(*, years: list[int], scope_defaults: dict) -> None:
    # "Applied" state keys (used by the page computations).
    default_year = _default_year(years)
    current = st.session_state.get("dq_year")
    if current is None or not str(current).strip():
        st.session_state["dq_year"] = default_year
    else:
        try:
            cur_i = int(current)
        except Exception:
            cur_i = default_year
        if years and cur_i not in years:
            cur_i = default_year
        st.session_state["dq_year"] = cur_i
    st.session_state.setdefault("dq_pi", [])
    st.session_state.setdefault("flt_programs", list(scope_defaults.get("programs") or []))
    st.session_state.setdefault("flt_teams", list(scope_defaults.get("teams") or []))
    st.session_state.setdefault("flt_groups", list(scope_defaults.get("groups") or []))


def _prune_selections(selections: list[str], options: list[str]) -> list[str]:
    opt_u = {str(o).strip().upper() for o in (options or []) if str(o).strip()}
    out: list[str] = []
    for s in selections or []:
        ss = str(s).strip()
        if not ss:
            continue
        if ss.upper() in opt_u:
            out.append(ss)
    return out


def _render_filters_form(*, years: list[int], program_options: list[str], team_options: list[str], group_options: list[str], scope_defaults: dict) -> None:
    # Use separate form widget keys so selecting options does not affect page computations until "Apply".
    cur_year = int(st.session_state.get("dq_year") or (years[0] if years else pd.Timestamp.utcnow().year))
    cur_pi = list(st.session_state.get("dq_pi") or [])
    cur_programs = list(st.session_state.get("flt_programs") or [])
    cur_teams = list(st.session_state.get("flt_teams") or [])
    cur_groups = list(st.session_state.get("flt_groups") or [])

    # Keep applied selections valid even if option lists change.
    cur_programs = _prune_selections(cur_programs, program_options)
    cur_teams = _prune_selections(cur_teams, team_options)
    cur_groups = _prune_selections(cur_groups, group_options)

    with st.form(key="dq_filters_form", clear_on_submit=False):
        c1, c2 = st.columns([1, 2])
        with c1:
            sel_year = int(st.selectbox("Year", years, index=years.index(cur_year) if cur_year in years else 0, key="dq_form_year"))
        with c2:
            sel_pi = st.multiselect("PI (optional)", [1, 2, 3, 4], default=cur_pi, key="dq_form_pi")

        c3, c4, c5 = st.columns(3)
        with c3:
            sel_programs = st.multiselect("Programs", program_options, default=cur_programs, key="dq_form_programs")
        with c4:
            sel_teams = st.multiselect("Teams", team_options, default=cur_teams, key="dq_form_teams")
        with c5:
            sel_groups = st.multiselect("Applications", group_options, default=cur_groups, key="dq_form_groups")

        b1, b2 = st.columns([1, 1])
        with b1:
            do_reset = st.form_submit_button("Reset", type="secondary", use_container_width=True)
        with b2:
            do_apply = st.form_submit_button("Apply", type="primary", use_container_width=True)

    if do_reset:
        st.session_state["dq_year"] = _default_year(years)
        st.session_state["dq_pi"] = []
        st.session_state["flt_programs"] = list(scope_defaults.get("programs") or [])
        st.session_state["flt_teams"] = list(scope_defaults.get("teams") or [])
        st.session_state["flt_groups"] = list(scope_defaults.get("groups") or [])
        st.session_state.pop("dq_issue_select", None)
        st.rerun()

    if do_apply:
        st.session_state["dq_year"] = int(sel_year)
        st.session_state["dq_pi"] = list(sel_pi or [])
        st.session_state["flt_programs"] = _prune_selections(list(sel_programs or []), program_options)
        st.session_state["flt_teams"] = _prune_selections(list(sel_teams or []), team_options)
        st.session_state["flt_groups"] = _prune_selections(list(sel_groups or []), group_options)
        st.session_state.pop("dq_issue_select", None)
        st.rerun()


def _render_active_filters() -> None:
    parts: list[str] = []
    y = st.session_state.get("dq_year")
    if y:
        parts.append(f"Year: {int(y)}")
    pi = list(st.session_state.get("dq_pi") or [])
    if pi:
        parts.append("PI: " + ", ".join([str(int(p)) for p in pi if str(p).strip()]))
    progs = list(st.session_state.get("flt_programs") or [])
    if progs:
        parts.append("Programs: " + ", ".join(progs[:4]) + (f" (+{len(progs)-4})" if len(progs) > 4 else ""))
    teams = list(st.session_state.get("flt_teams") or [])
    if teams:
        parts.append("Teams: " + ", ".join(teams[:4]) + (f" (+{len(teams)-4})" if len(teams) > 4 else ""))
    groups = list(st.session_state.get("flt_groups") or [])
    if groups:
        parts.append("Applications: " + ", ".join(groups[:4]) + (f" (+{len(groups)-4})" if len(groups) > 4 else ""))
    if parts:
        st.caption(" • ".join(parts))


opts = fetch_filter_options()
opts = opts if isinstance(opts, pd.DataFrame) else pd.DataFrame()
years = sorted({int(y) for y in pd.to_numeric(opts.get("YEAR"), errors="coerce").dropna().astype(int).tolist()}, reverse=True)
if not years:
    years = [_default_year([])]

scope_defaults = _derive_scope_defaults()
_init_filter_state(years=years, scope_defaults=scope_defaults)

# Options are derived from the full options dataset (all years) to prevent selections from disappearing.
program_options = _safe_unique(opts, "PROGRAMNAME")
team_options = _safe_unique(opts, "TEAMNAME")
group_options = _safe_unique(opts, "GROUPNAME")

def _scope_meta_line() -> str:
    parts: list[str] = []
    y = st.session_state.get("dq_year")
    if y:
        parts.append(f"Year {int(y)}")
    programs = list(st.session_state.get("flt_programs") or [])
    teams = list(st.session_state.get("flt_teams") or [])
    groups = list(st.session_state.get("flt_groups") or [])
    parts.append("Programs: " + (", ".join(programs[:2]) + ("…" if len(programs) > 2 else "") if programs else "All"))
    if teams:
        parts.append("Teams: " + ", ".join(teams[:2]) + ("…" if len(teams) > 2 else ""))
    if groups:
        parts.append("Applications: " + ", ".join(groups[:2]) + ("…" if len(groups) > 2 else ""))
    return " • ".join(parts)


st.title("Data Quality & NEXT Readiness")
st.caption(
    "This page shows whether your NEXT data is complete, consistent, and ready for decision-making — "
    "and where to fix issues."
)
st.caption(f"Last evaluated: {date.today():%Y-%m-%d} • Scope: {_scope_meta_line()}")

PAGE_ROUTES = {
    "applications": "pages/6_Applications.py",
    "rates": "pages/7_Rates.py",
    "teams": "pages/4_Teams.py",
    "programs": "pages/3_Programs.py",
    "how_to": "pages/9_How_To.py",
    "settings": "pages/10_Settings.py",
}


def go_to_page(page_key: str, query_params: Optional[dict] = None) -> None:
    path = PAGE_ROUTES.get(page_key)
    if not path:
        return
    if query_params:
        set_qp(**{str(k): str(v) for k, v in query_params.items() if v is not None})
    if hasattr(st, "switch_page"):
        try:
            st.switch_page(path)
            return
        except Exception:
            pass
    if hasattr(st, "page_link"):
        st.page_link(path, label=f"Open {page_key.title()}")
    st.info("Open the page from the sidebar if navigation is blocked.")

st.markdown("#### Filters")
_render_filters_form(
    years=years,
    program_options=program_options,
    team_options=team_options,
    group_options=group_options,
    scope_defaults=scope_defaults,
)
_render_active_filters()


def _load_ctx_frames() -> QualityContext:
    # Canonical cost lines via cost model adapter (which uses canonical_costs internally).
    year = int(st.session_state.get("dq_year") or (years[0] if years else pd.Timestamp.utcnow().year))
    pi_nums = list(st.session_state.get("dq_pi") or [])
    programs = list(st.session_state.get("flt_programs") or [])
    teams = list(st.session_state.get("flt_teams") or [])
    groups = list(st.session_state.get("flt_groups") or [])

    derived_teams: list[str] = []
    if programs and not teams:
        try:
            if isinstance(opts, pd.DataFrame) and not opts.empty and {"PROGRAMNAME", "TEAMNAME"}.issubset(set(opts.columns)):
                w = opts.copy()
                if "YEAR" in w.columns:
                    w = w[pd.to_numeric(w["YEAR"], errors="coerce").fillna(-1).astype(int).eq(int(year))].copy()
                prog_u = {str(p).strip().upper() for p in programs if str(p).strip()}
                w["PROGRAMNAME"] = w["PROGRAMNAME"].fillna("").astype(str).str.strip()
                w["TEAMNAME"] = w["TEAMNAME"].fillna("").astype(str).str.strip()
                w = w[w["PROGRAMNAME"].str.upper().isin(prog_u)].copy()
                derived_teams = sorted(set(w["TEAMNAME"][w["TEAMNAME"].ne("")].tolist()))
        except Exception:
            derived_teams = []

    effective_teams = teams if teams else derived_teams

    model = load_cost_model(year, {"programs": programs, "teams": teams, "groups": groups})
    cost_lines = pd.concat([model.get("BASELINE", pd.DataFrame()), model.get("EXPECTED", pd.DataFrame()), model.get("ACTUAL", pd.DataFrame())], ignore_index=True)

    # Canonical ADO feature demand rows (Explorer v2).
    # IMPORTANT: When a Program is selected but Team is not, do NOT filter ADO by program.
    # Many ADO rows have program missing/unmapped, but team is known; we scope ADO by the teams
    # that belong to the selected program(s) so Team Health remains consistent.
    ado_programs = list(programs)
    ado_teams = list(effective_teams)
    if programs and not teams:
        if derived_teams:
            ado_teams = derived_teams
            ado_programs = []  # team-only ADO scoping (program often missing/unmapped)

    ado = load_explorer_feature_rows(
        years=[int(year)],
        programs=tuple(ado_programs) if ado_programs else (),
        teams=tuple(ado_teams) if ado_teams else (),
        groups=groups or (),
        pi_nums=pi_nums or (),
    )
    ado = ado if isinstance(ado, pd.DataFrame) else pd.DataFrame()

    # Apptio breakdown (program-level actuals). Needs program IDs.
    program_ids: list[str] = []
    try:
        if programs:
            placeholders = ", ".join(["%s"] * len(programs))
            df_p = fetch_df(
                f"SELECT PROGRAMID, PROGRAMNAME FROM PROGRAMS WHERE UPPER(LTRIM(RTRIM(PROGRAMNAME))) IN ({placeholders})",
                tuple([p.upper() for p in programs]),
            )
        else:
            df_p = fetch_df("SELECT PROGRAMID, PROGRAMNAME FROM PROGRAMS", None)
        if isinstance(df_p, pd.DataFrame) and not df_p.empty and "PROGRAMID" in df_p.columns:
            program_ids = df_p["PROGRAMID"].dropna().astype(str).str.strip().tolist()
    except Exception:
        program_ids = []

    apptio = pd.DataFrame()
    if program_ids:
        try:
            apptio = fetch_apptio_actuals_monthly_breakdown(int(year), program_ids)
        except Exception:
            apptio = pd.DataFrame()

    return QualityContext(
        db=fetch_df,
        year=int(year),
        pi=int(pi_nums[0]) if pi_nums else None,
        programs=tuple(programs),
        # Keep ctx team scope as the explicit team filter only.
        # Team Health attribution is handled separately (program selection derives a team universe).
        teams=tuple(teams),
        app_groups=tuple(groups),
        cost_lines=cost_lines,
        ado_features=ado,
        apptio_actuals_breakdown=apptio if isinstance(apptio, pd.DataFrame) else pd.DataFrame(),
    )


ctx = _load_ctx_frames()
issues = collect_issues(ctx)
issues_df = issues if isinstance(issues, pd.DataFrame) else pd.DataFrame()
if issues_df is not None and not issues_df.empty and "SEVERITY" in issues_df.columns:
    sev = (
        issues_df["SEVERITY"]
        .fillna("")
        .astype(str)
        .str.replace("🔴", "", regex=False)
        .str.replace("🟠", "", regex=False)
        .str.replace("🟡", "", regex=False)
        .str.replace("⚠", "", regex=False)
        .str.upper()
        .str.strip()
    )
    sev = sev.replace(
        {
            "BLOCKING": "Blocking",
            "BLOCKER": "Blocking",
            "WARN": "Warning",
            "WARNING": "Warning",
            "INFO": "Info",
            "INFORMATIONAL": "Info",
        }
    )
    issues_df["SEVERITY"] = sev

def _derive_root_cause(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series([], dtype=str)
    root = pd.Series([""] * len(df), index=df.index, dtype=str)
    if "ROOT_CAUSE_CODE" in df.columns:
        root = df["ROOT_CAUSE_CODE"].fillna("").astype(str).str.strip()
    if "ISSUE_TYPE" in df.columns:
        root = root.where(root.ne(""), df["ISSUE_TYPE"].fillna("").astype(str).str.strip())
    if "TITLE" in df.columns:
        fallback = (
            df["TITLE"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.slice(0, 80)
        )
        root = root.where(root.ne(""), fallback)
    return root.replace("", "—")

if issues_df is not None and not issues_df.empty:
    issues_df["ROOT_CAUSE"] = _derive_root_cause(issues_df)

def _count_issue(issue_type: str) -> int:
    if issues_df is None or issues_df.empty or "ISSUE_TYPE" not in issues_df.columns:
        return 0
    return int(issues_df["ISSUE_TYPE"].astype(str).eq(str(issue_type)).sum())


def _issue_df_for_types(types: set[str]) -> pd.DataFrame:
    if issues_df is None or issues_df.empty or "ISSUE_TYPE" not in issues_df.columns:
        return pd.DataFrame()
    return issues_df[issues_df["ISSUE_TYPE"].astype(str).isin(types)].copy()


def _status_color(df: pd.DataFrame) -> str:
    if issues_df is None or issues_df.empty:
        return KPI_GREY
    if df is None or df.empty:
        return KPI_GREEN
    sev = df.get("SEVERITY", pd.Series(dtype=str)).fillna("").astype(str).str.upper().str.strip()
    if (sev == "BLOCKING").any():
        return KPI_RED
    if (sev == "WARNING").any():
        return KPI_YELLOW
    return KPI_GREY


def _status_label(df: pd.DataFrame) -> str:
    if issues_df is None or issues_df.empty:
        return "No data"
    if df is None or df.empty:
        return "Healthy"
    return "Needs attention"


def _format_issues_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    view = df.copy()
    if "IMPACT_USD_EST" in view.columns:
        view["IMPACT_USD_EST"] = view["IMPACT_USD_EST"].apply(_money)
    return view


def _fix_location_display_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series([], dtype=str)
    fix_system = df.get("FIX_SYSTEM", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str).str.upper()
    fix_target = df.get("FIX_TARGET", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str)
    labels = []
    route_labels = {
        "applications": "Applications",
        "rates": "Rates",
        "teams": "Teams",
        "programs": "Programs",
        "how_to": "How-To",
        "settings": "Settings → ADO",
    }
    for sys, tgt in zip(fix_system.tolist(), fix_target.tolist()):
        if sys == "TCO":
            key = str(tgt or "").strip()
            if key.startswith("pages/"):
                name = next((k for k, v in PAGE_ROUTES.items() if v == key), "")
                key = name or key
            label = route_labels.get(key, key) if key else "Internal (NEXT)"
        elif sys == "ADO":
            label = "External (Azure DevOps)"
        elif sys == "APPTIO":
            label = "External (Apptio)"
        elif sys == "HOWTO":
            label = "How-To"
        elif sys == "EXTERNAL":
            label = "External"
        else:
            label = "—"
        labels.append(label if label else "—")
    return pd.Series(labels, index=df.index, dtype=str)


def render_issue_table(
    df: pd.DataFrame,
    title: str,
    default_cols: list[str],
    advanced_cols: list[str],
    sort_cols: list[str],
    *,
    show_advanced: bool,
) -> None:
    if df is None or df.empty:
        return
    w = df.copy()
    if "SEVERITY" in w.columns:
        sev_rank = w["SEVERITY"].fillna("").astype(str).str.upper().str.strip().map(
            {"BLOCKING": 3, "WARNING": 2, "INFO": 1, "BLOCKER": 3}
        )
        w["_SEV_RANK"] = pd.to_numeric(sev_rank, errors="coerce").fillna(0).astype(int)
    if "IMPACT_USD_EST" in w.columns:
        w["_IMPACT_USD_EST"] = pd.to_numeric(w["IMPACT_USD_EST"], errors="coerce").fillna(0.0)
    sort_by: list[str] = []
    ascending: list[bool] = []
    for col in sort_cols:
        if col in w.columns:
            sort_by.append(col)
            if col in {"_SEV_RANK", "_IMPACT_USD_EST"}:
                ascending.append(False)
            elif col == "TITLE":
                ascending.append(True)
            else:
                ascending.append(True)
    if sort_by:
        w = w.sort_values(by=sort_by, ascending=ascending, kind="mergesort")
    if "FIX_LOCATION" in default_cols and "FIX_LOCATION" not in w.columns:
        w["FIX_LOCATION"] = _fix_location_display_series(w)
    view_cols = [c for c in default_cols if c in w.columns]
    if show_advanced:
        view_cols.extend([c for c in advanced_cols if c in w.columns and c not in view_cols])
    if not view_cols:
        st.info("No issue details available for this scope.")
        return
    view = w[view_cols].copy()
    if "FIX_LOCATION" in view.columns:
        view["FIX_LOCATION"] = _fix_location_display_series(w)
    if "IMPACT_USD_EST" in view.columns:
        impact = w.get("_IMPACT_USD_EST", pd.Series([0.0] * len(view), index=view.index))
        detail = w.get("IMPACT_BASIS_DETAIL", pd.Series([""] * len(view), index=view.index)).fillna("").astype(str)
        impact_display = impact.apply(_money)
        mask = detail.str.contains("not estimated", case=False, na=False)
        impact_display = impact_display.where(~mask, "—")
        view["IMPACT_USD_EST"] = impact_display
    view = view.replace({None: "—"}).fillna("—")
    view = view.rename(
        columns={
            "GUIDE_SECTION": "Read more",
            "ROOT_CAUSE": "Root Cause",
            "FIX_LOCATION": "Fix location",
        }
    )
    if title:
        st.markdown(f"**{title}**")
    st.dataframe(view, use_container_width=True, hide_index=True)


def _render_issue_block(
    *,
    title: str,
    summary: str,
    fix_in: str,
    read_more: str,
    df: pd.DataFrame,
    empty_note: Optional[str] = None,
    show_advanced: bool,
    default_cols: list[str],
    advanced_cols: list[str],
    sort_cols: list[str],
    why_bullets: Optional[list[str]] = None,
) -> None:
    st.markdown(f"#### {title}")
    st.caption(summary)
    st.caption(f"Fix in: {fix_in}")
    if df is not None and not df.empty and "GUIDE_SECTION" in df.columns:
        guide_vals = (
            df["GUIDE_SECTION"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        if guide_vals:
            st.caption("Read more: How To → " + " • ".join(guide_vals[:3]))
        else:
            st.caption(f"Read more: How To → {read_more}")
    else:
        st.caption(f"Read more: How To → {read_more}")
    if why_bullets:
        with st.expander("Why does this happen?", expanded=False):
            st.markdown("\n".join([f"- {b}" for b in why_bullets]))
    if df is None or df.empty:
        st.info(empty_note or "No issues found for the selected scope.")
        return
    render_issue_table(
        df,
        "",
        default_cols,
        advanced_cols,
        sort_cols,
        show_advanced=show_advanced,
    )


def _render_actions_row(actions: list[tuple[str, str, Optional[dict]]], *, key_prefix: str) -> None:
    if not actions:
        return
    counter_key = f"{key_prefix}_counter"
    counter = int(st.session_state.get(counter_key, 0)) + 1
    st.session_state[counter_key] = counter
    cols = st.columns(len(actions))
    for idx, (col, (label, page_key, params)) in enumerate(zip(cols, actions), start=1):
        with col:
            if st.button(
                label,
                use_container_width=True,
                key=f"{key_prefix}_{page_key}_{idx}_{counter}",
            ):
                go_to_page(page_key, params)


def _render_unmapped_app_actions(df: pd.DataFrame, limit: int = 20) -> None:
    if df is None or df.empty:
        return
    col = "TITLE"
    if "TITLE" not in df.columns and "APP_NAME_RAW" in df.columns:
        col = "APP_NAME_RAW"
    rows = df.copy()
    if "IMPACT_FEATURES" in rows.columns:
        rows = rows.sort_values(by="IMPACT_FEATURES", ascending=False)
    rows = rows.head(limit)
    st.markdown("**Quick fixes (top items)**")
    for i, (_, r) in enumerate(rows.iterrows(), start=1):
        label = str(r.get(col) or "").strip()
        if not label:
            continue
        c1, c2 = st.columns([4, 1])
        with c1:
            st.markdown(f"{i}. {label}")
        with c2:
            if st.button("Fix", key=f"dq_fix_ado_{i}", use_container_width=True):
                go_to_page(
                    "applications",
                    {"focus": "ado_mapping", "ado_app": label},
                )


def _filter_by_root_code(df: pd.DataFrame, codes: set[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    for col in ["ROOT_CAUSE_CODE", "MAPPING_STATUS", "STATUS_CODE", "mapping_status"]:
        if col in df.columns:
            code_u = {c.upper() for c in codes}
            s = df[col].fillna("").astype(str).str.upper().str.strip()
            return df[s.isin(code_u)].copy()
    return pd.DataFrame()


def _ado_url_column(df: pd.DataFrame) -> Optional[str]:
    for cand in ["FEATURE_URL", "ADO_URL", "URL", "WORK_ITEM_URL", "WORKITEM_URL"]:
        if cand in df.columns:
            return cand
    return None


def _render_ado_evidence_table(
    df: pd.DataFrame,
    *,
    missing_field: str,
    show_advanced: bool,
    export_label: str,
    export_key: str,
) -> None:
    if df is None or df.empty:
        return
    w = df.copy()
    url_col = _ado_url_column(w)
    w["ADO_MISSING_FIELD"] = missing_field
    base_cols = [
        "FEATURE_ID",
        "TITLE",
        "PROGRAMNAME",
        "TEAMNAME",
        "PI_LABEL",
        "ADO_MISSING_FIELD",
    ]
    adv_cols = ["GROUPNAME", "DERIVED_FTE", "MAPPING_STATUS"]
    cols = [c for c in base_cols if c in w.columns]
    if show_advanced:
        cols.extend([c for c in adv_cols if c in w.columns and c not in cols])
    if url_col and url_col not in cols:
        cols.append(url_col)
    view = w[cols].copy()
    rename_map = {
        "FEATURE_ID": "Feature ID",
        "TITLE": "Feature Title",
        "PROGRAMNAME": "Program",
        "TEAMNAME": "Team",
        "PI_LABEL": "ADO Iteration",
        "ADO_MISSING_FIELD": "Missing field",
        "GROUPNAME": "App Group",
        "DERIVED_FTE": "Derived FTE",
        "MAPPING_STATUS": "Mapping status",
    }
    if url_col:
        rename_map[url_col] = "Open in ADO"
    view = view.rename(
        columns={
            **rename_map,
        }
    )
    view = view.replace({None: "—"}).fillna("—")
    data_csv = view.to_csv(index=False)
    st.download_button(
        export_label,
        data=data_csv,
        file_name="ado_offenders.csv",
        mime="text/csv",
        use_container_width=True,
        key=export_key,
    )
    if url_col:
        st.dataframe(
            view,
            use_container_width=True,
            hide_index=True,
            column_config={"Open in ADO": st.column_config.LinkColumn("Open in ADO")},
        )
    else:
        st.dataframe(view, use_container_width=True, hide_index=True)


def compute_readiness_score(df: pd.DataFrame) -> dict:
    if df is None or df.empty or "SEVERITY" not in df.columns:
        return {
            "score": 100,
            "status": "Ready",
            "blockers": 0,
            "warnings": 0,
            "top_causes": [],
        }
    sev = df["SEVERITY"].fillna("").astype(str).str.upper().str.strip()
    blockers = int((sev == "BLOCKING").sum())
    warnings = int((sev == "WARNING").sum())
    block_penalty = min(80, blockers * 20)
    warn_penalty = min(20, warnings * 5)
    score = max(0, min(100, 100 - block_penalty - warn_penalty))
    if blockers > 0:
        status = "Not ready"
    elif warnings > 0:
        status = "Needs attention"
    else:
        status = "Ready"
    top_causes: list[str] = []
    if "ROOT_CAUSE" in df.columns:
        subset = df.loc[sev.isin({"BLOCKING", "WARNING"})].copy()
        if not subset.empty:
            top_causes = (
                subset["ROOT_CAUSE"]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace({"": pd.NA, "—": pd.NA})
                .dropna()
                .value_counts()
                .head(2)
                .index.tolist()
            )
    return {
        "score": int(score),
        "status": status,
        "blockers": blockers,
        "warnings": warnings,
        "top_causes": top_causes,
    }


def _status_badge(status: str) -> str:
    s = str(status or "").strip().lower()
    if s == "ready":
        return "✅ Ready"
    if s == "needs attention":
        return "🟡 Needs attention"
    if s == "not ready":
        return "🔴 Not ready"
    return status


def _score_from_counts(blockers: int, warnings: int) -> dict:
    block_penalty = min(80, int(blockers) * 20)
    warn_penalty = min(20, int(warnings) * 5)
    score = max(0, min(100, 100 - block_penalty - warn_penalty))
    if blockers > 0:
        status = "Not ready"
    elif warnings > 0:
        status = "Needs attention"
    else:
        status = "Ready"
    return {"score": int(score), "status": status}


def _teams_for_program(program_name: str, year: int) -> list[str]:
    if not program_name or opts is None or not isinstance(opts, pd.DataFrame) or opts.empty:
        return []
    if {"PROGRAMNAME", "TEAMNAME"}.issubset(set(opts.columns)):
        w = opts.copy()
        if "YEAR" in w.columns:
            w = w[pd.to_numeric(w["YEAR"], errors="coerce").fillna(-1).astype(int).eq(int(year))].copy()
        w["PROGRAMNAME"] = w["PROGRAMNAME"].fillna("").astype(str).str.strip()
        w["TEAMNAME"] = w["TEAMNAME"].fillna("").astype(str).str.strip()
        w = w[w["PROGRAMNAME"].eq(str(program_name).strip())].copy()
        return sorted(set(w["TEAMNAME"][w["TEAMNAME"].ne("")].tolist()))
    return []


def _programs_for_team(team_name: str, year: int) -> list[str]:
    if not team_name or opts is None or not isinstance(opts, pd.DataFrame) or opts.empty:
        return []
    if {"PROGRAMNAME", "TEAMNAME"}.issubset(set(opts.columns)):
        w = opts.copy()
        if "YEAR" in w.columns:
            w = w[pd.to_numeric(w["YEAR"], errors="coerce").fillna(-1).astype(int).eq(int(year))].copy()
        w["PROGRAMNAME"] = w["PROGRAMNAME"].fillna("").astype(str).str.strip()
        w["TEAMNAME"] = w["TEAMNAME"].fillna("").astype(str).str.strip()
        w = w[w["TEAMNAME"].eq(str(team_name).strip())].copy()
        return sorted(set(w["PROGRAMNAME"][w["PROGRAMNAME"].ne("")].tolist()))
    return []


def _collect_issues_for_scope(
    scope_programs: tuple[str, ...],
    scope_teams: tuple[str, ...],
    scope_year: int,
    scope_pi: tuple[int, ...],
    ado_teams: tuple[str, ...],
    ado_programs: tuple[str, ...],
) -> pd.DataFrame:
    model = load_cost_model(int(scope_year), {"programs": list(scope_programs), "teams": list(scope_teams), "groups": []})
    cost_lines = pd.concat(
        [
            model.get("BASELINE", pd.DataFrame()),
            model.get("EXPECTED", pd.DataFrame()),
            model.get("ACTUAL", pd.DataFrame()),
        ],
        ignore_index=True,
    )

    ado = load_explorer_feature_rows(
        years=[int(scope_year)],
        programs=tuple(ado_programs) if ado_programs else (),
        teams=tuple(ado_teams) if ado_teams else (),
        groups=(),
        pi_nums=list(scope_pi) if scope_pi else (),
    )
    ado = ado if isinstance(ado, pd.DataFrame) else pd.DataFrame()

    program_ids: list[str] = []
    if scope_programs:
        try:
            placeholders = ", ".join(["%s"] * len(scope_programs))
            df_p = fetch_df(
                f"SELECT PROGRAMID, PROGRAMNAME FROM PROGRAMS WHERE UPPER(LTRIM(RTRIM(PROGRAMNAME))) IN ({placeholders})",
                tuple([p.upper() for p in scope_programs]),
            )
            if isinstance(df_p, pd.DataFrame) and not df_p.empty and "PROGRAMID" in df_p.columns:
                program_ids = df_p["PROGRAMID"].dropna().astype(str).str.strip().tolist()
        except Exception:
            program_ids = []

    apptio = pd.DataFrame()
    if program_ids:
        try:
            apptio = fetch_apptio_actuals_monthly_breakdown(int(scope_year), program_ids)
        except Exception:
            apptio = pd.DataFrame()

    ctx_local = QualityContext(
        db=fetch_df,
        year=int(scope_year),
        pi=int(scope_pi[0]) if scope_pi else None,
        programs=tuple(scope_programs),
        teams=tuple(scope_teams),
        app_groups=tuple(),
        cost_lines=cost_lines,
        ado_features=ado,
        apptio_actuals_breakdown=apptio if isinstance(apptio, pd.DataFrame) else pd.DataFrame(),
    )
    df = collect_issues(ctx_local)
    df = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if not df.empty:
        df["ROOT_CAUSE"] = _derive_root_cause(df)
    return df


def _compute_team_readiness(
    *,
    team_name: str,
    year: int,
    pi_nums: tuple[int, ...],
    program_hint: Optional[str] = None,
) -> tuple[pd.Series, dict[str, int], pd.DataFrame]:
    programs = tuple([program_hint]) if program_hint else tuple(_programs_for_team(team_name, year))
    issues_df_local = _collect_issues_for_scope(
        programs,
        (team_name,),
        year,
        pi_nums,
        (team_name,),
        tuple(),
    )
    readiness_row = compute_readiness(
        programs,
        (team_name,),
        year,
        pi_nums,
        (team_name,),
        tuple(),
    )
    score_meta = _score_from_counts(
        int(readiness_row.get("BLOCKERS") or 0),
        int(readiness_row.get("WARNINGS") or 0),
    )
    return readiness_row, score_meta, issues_df_local

@st.cache_data(ttl=180, show_spinner=False)  # type: ignore[misc]
def compute_readiness(
    scope_programs: tuple[str, ...],
    scope_teams: tuple[str, ...],
    scope_year: int,
    scope_pi: tuple[int, ...],
    ado_teams: tuple[str, ...],
    ado_programs: tuple[str, ...],
) -> pd.Series:
    model = load_cost_model(int(scope_year), {"programs": list(scope_programs), "teams": list(scope_teams), "groups": []})
    cost_lines = pd.concat(
        [
            model.get("BASELINE", pd.DataFrame()),
            model.get("EXPECTED", pd.DataFrame()),
            model.get("ACTUAL", pd.DataFrame()),
        ],
        ignore_index=True,
    )

    ado = load_explorer_feature_rows(
        years=[int(scope_year)],
        programs=tuple(ado_programs) if ado_programs else (),
        teams=tuple(ado_teams) if ado_teams else (),
        groups=(),
        pi_nums=list(scope_pi) if scope_pi else (),
    )
    ado = ado if isinstance(ado, pd.DataFrame) else pd.DataFrame()

    program_ids: list[str] = []
    if scope_programs:
        try:
            placeholders = ", ".join(["%s"] * len(scope_programs))
            df_p = fetch_df(
                f"SELECT PROGRAMID, PROGRAMNAME FROM PROGRAMS WHERE UPPER(LTRIM(RTRIM(PROGRAMNAME))) IN ({placeholders})",
                tuple([p.upper() for p in scope_programs]),
            )
            if isinstance(df_p, pd.DataFrame) and not df_p.empty and "PROGRAMID" in df_p.columns:
                program_ids = df_p["PROGRAMID"].dropna().astype(str).str.strip().tolist()
        except Exception:
            program_ids = []

    apptio = pd.DataFrame()
    if program_ids:
        try:
            apptio = fetch_apptio_actuals_monthly_breakdown(int(scope_year), program_ids)
        except Exception:
            apptio = pd.DataFrame()

    ctx_local = QualityContext(
        db=fetch_df,
        year=int(scope_year),
        pi=int(scope_pi[0]) if scope_pi else None,
        programs=tuple(scope_programs),
        teams=tuple(scope_teams),
        app_groups=tuple(),
        cost_lines=cost_lines,
        ado_features=ado,
        apptio_actuals_breakdown=apptio if isinstance(apptio, pd.DataFrame) else pd.DataFrame(),
    )
    df = collect_issues(ctx_local)
    df = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if not df.empty:
        df["ROOT_CAUSE"] = _derive_root_cause(df)
    if df.empty or "SEVERITY" not in df.columns:
        return pd.Series(
            {
                "STATUS": "None",
                "STATUS_RANK": 0,
                "BLOCKERS": 0,
                "WARNINGS": 0,
                "IMPACT_USD": 0.0,
                "TOP_ROOT_CAUSE": "—",
            }
        )
    sev = df["SEVERITY"].fillna("").astype(str).str.upper().str.strip()
    blockers = int((sev == "BLOCKING").sum())
    warnings = int((sev == "WARNING").sum())
    info = int((sev == "INFO").sum())
    if blockers > 0:
        status = "Blocking"
        rank = 3
    elif warnings > 0:
        status = "Warning"
        rank = 2
    elif info > 0:
        status = "Info"
        rank = 1
    else:
        status = "None"
        rank = 0
    impact = float(pd.to_numeric(df.get("IMPACT_USD_EST"), errors="coerce").fillna(0.0).sum() or 0.0)
    top_cause = "—"
    if "ROOT_CAUSE" in df.columns:
        w = df.loc[sev.isin({"BLOCKING", "WARNING"})].copy()
        if not w.empty:
            w_root = w["ROOT_CAUSE"].fillna("").astype(str).replace("—", "").str.strip()
            w_root = w_root[w_root.ne("")]
            if not w_root.empty:
                top_cause = str(w_root.value_counts().idxmax() or "—")
    return pd.Series(
        {
            "STATUS": status,
            "STATUS_RANK": rank,
            "BLOCKERS": blockers,
            "WARNINGS": warnings,
            "IMPACT_USD": impact,
            "TOP_ROOT_CAUSE": top_cause,
        }
    )

mapping_types = {
    "ADO_UNMAPPED_APP",
    "ADO_UNMAPPED_TEAM",
    "ADO_UNMAPPED_PROGRAM",
    "UNASSIGNED_COST",
    "APP_GROUP_MISSING_ADO_MAPPING",
    "CONFIG_NO_ADO_MAPPING_ROWS",
    "ADO_FEATURES_MISSING_SWAG",
    "ADO_FEATURES_MISSING_APP_NAME",
    "CONFIG_ADO_TEAMS_UNMAPPED",
    "ADO_UNMAPPED_TEAMS_IN_PROGRAM",
}
integrity_types = {
    "MISSING_WF_RATES",
    "CONFIG_MISSING_WF_RATES",
    "TEAM_MISSING_PROGRAM",
    "APP_GROUP_MISSING_OWNER_TEAM",
    "CONFIG_APP_MISSING_OWNER",
    "APP_GROUP_MISSING_MSP_ASSIGNMENT",
    "APP_GROUP_MULTI_INSTANCE",
    "CONFIG_PROGRAM_HAS_NO_TEAMS",
}
scenario_types = {"APPTIO_UNMAPPED_ACTUALS"}

selected_programs = list(st.session_state.get("flt_programs") or [])
selected_teams = list(st.session_state.get("flt_teams") or [])

if len(selected_teams) > 0:
    mode = "TEAM"
elif len(selected_programs) == 1:
    mode = "PROGRAM"
else:
    mode = "PORTFOLIO"

tab_summary, tab_breakdown, tab_backlog, tab_fix, tab_catalog = st.tabs(
    ["Summary", "Breakdown", "Issue Backlog", "How to Fix", "Issue Catalog"]
)

with tab_summary:
    if mode == "TEAM":
        title = f"Team Readiness — {selected_teams[0]}"
        caption = "This view summarizes readiness for the selected team."
    elif mode == "PROGRAM":
        title = f"Program Readiness — {selected_programs[0]}"
        caption = "This view summarizes readiness for the selected program."
    else:
        title = "Portfolio Readiness"
        caption = "This view summarizes readiness across the selected portfolio scope."

    st.markdown(f"## {title}")
    st.caption(caption)

    readiness_source_df = issues_df
    if mode == "TEAM" and selected_teams:
        year_i = int(st.session_state.get("dq_year") or (years[0] if years else date.today().year))
        pi_nums = tuple(int(p) for p in (st.session_state.get("dq_pi") or []) if str(p).strip())
        team_name = selected_teams[0]
        program_hint = selected_programs[0] if selected_programs else None
        _, _, readiness_source_df = _compute_team_readiness(
            team_name=team_name,
            year=year_i,
            pi_nums=pi_nums,
            program_hint=program_hint,
        )

    readiness = compute_readiness_score(readiness_source_df)
    score_label = f"{readiness['score']}"
    score_color = KPI_GREEN if readiness["score"] >= 90 else (KPI_YELLOW if readiness["score"] >= 70 else KPI_RED)
    card, meta = st.columns([1, 2], vertical_alignment="top")
    with card:
        render_kpi_card("Readiness Score", score_label, score_color)
    with meta:
        st.markdown(
            "\n".join(
                [
                    f"- Status: {_status_badge(readiness['status'])}",
                    f"- Blocking issues: {readiness['blockers']}",
                    f"- Warning issues: {readiness['warnings']}",
                    f"- Top causes: {', '.join(readiness['top_causes']) if readiness['top_causes'] else '—'}",
                ]
            )
        )

    year_i = int(st.session_state.get("dq_year") or (years[0] if years else date.today().year))
    pi_nums = tuple(int(p) for p in (st.session_state.get("dq_pi") or []) if str(p).strip())
    if mode == "PORTFOLIO":
        st.markdown("### Programs Readiness")
        programs = selected_programs if selected_programs else program_options
        rows: list[dict[str, object]] = []
        for prog in programs:
            prog_name = str(prog).strip()
            if not prog_name:
                continue
            derived_teams = _teams_for_program(prog_name, year_i)
            readiness_row = compute_readiness(
                (prog_name,),
                tuple(),
                year_i,
                pi_nums,
                tuple(derived_teams),
                tuple(),
            )
            score_meta = _score_from_counts(
                int(readiness_row.get("BLOCKERS") or 0),
                int(readiness_row.get("WARNINGS") or 0),
            )
            rows.append(
                {
                    "Program": prog_name,
                    "Status": _status_badge(score_meta["status"]),
                    "Score": score_meta["score"],
                    "Blockers": int(readiness_row.get("BLOCKERS") or 0),
                    "Warnings": int(readiness_row.get("WARNINGS") or 0),
                    "Top Root Cause": readiness_row.get("TOP_ROOT_CAUSE"),
                }
            )
        prog_df = pd.DataFrame(rows)
        if prog_df.empty:
            st.info("All programs are healthy for the selected scope.")
        else:
            prog_df = prog_df.sort_values(by=["Score", "Blockers"], ascending=[True, False], kind="mergesort")
            st.dataframe(
                prog_df[["Program", "Status", "Score", "Blockers", "Warnings", "Top Root Cause"]],
                use_container_width=True,
                hide_index=True,
            )
    elif mode == "PROGRAM":
        st.markdown("### Teams Readiness")
        program_name = selected_programs[0] if selected_programs else ""
        teams = _teams_for_program(program_name, year_i)
        rows = []
        for team in teams:
            team_name = str(team).strip()
            if not team_name:
                continue
            readiness_row, score_meta, _ = _compute_team_readiness(
                team_name=team_name,
                year=year_i,
                pi_nums=pi_nums,
                program_hint=program_name,
            )
            rows.append(
                {
                    "Team": team_name,
                    "Status": _status_badge(score_meta["status"]),
                    "Score": score_meta["score"],
                    "Blockers": int(readiness_row.get("BLOCKERS") or 0),
                    "Warnings": int(readiness_row.get("WARNINGS") or 0),
                    "Top Root Cause": readiness_row.get("TOP_ROOT_CAUSE"),
                }
            )
        team_df = pd.DataFrame(rows)
        if team_df.empty:
            st.info("All teams are healthy for the selected scope.")
        else:
            team_df = team_df.sort_values(by=["Score", "Blockers"], ascending=[True, False], kind="mergesort")
            st.dataframe(
                team_df[["Team", "Status", "Score", "Blockers", "Warnings", "Top Root Cause"]],
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.markdown("### Top blockers")
        blockers = readiness_source_df.copy() if isinstance(readiness_source_df, pd.DataFrame) else pd.DataFrame()
        if not blockers.empty and "SEVERITY" in blockers.columns:
            blockers = blockers[blockers["SEVERITY"].astype(str).str.upper().eq("BLOCKING")].copy()
        if blockers.empty:
            st.info("No blocking issues detected for this team.")
        else:
            blockers["IMPACT_USD_EST"] = pd.to_numeric(blockers.get("IMPACT_USD_EST"), errors="coerce").fillna(0.0)
            blockers = blockers.sort_values(by=["IMPACT_USD_EST", "TITLE"], ascending=[False, True], kind="mergesort")
            top = blockers.head(5)
            top["IMPACT_USD_EST"] = top["IMPACT_USD_EST"].apply(_money)
            st.dataframe(
                top[["TITLE", "IMPACT_USD_EST"]].rename(columns={"TITLE": "Issue", "IMPACT_USD_EST": "Impact ($)"}),
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("### ADO offenders (Top 25)")
        ado_frames: list[pd.DataFrame] = []
        for label, fn in [
            ("App Name", "ado_unmapped_app"),
            ("Team", "ado_unmapped_team"),
            ("SWAG", "ado_missing_swag"),
            ("App Name", "ado_missing_app_name"),
        ]:
            ev = resolve_evidence({"fn": fn, "params": {"limit": 200}}, ctx)
            if ev is None or ev.empty:
                continue
            ev = ev.copy()
            ev["ADO_MISSING_FIELD"] = label
            ado_frames.append(ev)
        if not ado_frames:
            st.info("No ADO offenders in this scope.")
        else:
            ado = pd.concat(ado_frames, ignore_index=True)
            cols = [c for c in ["FEATURE_ID", "TITLE", "ADO_MISSING_FIELD"] if c in ado.columns]
            ado = ado[cols].head(25).rename(
                columns={"FEATURE_ID": "Feature ID", "TITLE": "Feature Title", "ADO_MISSING_FIELD": "Missing field"}
            )
            st.dataframe(ado, use_container_width=True, hide_index=True)

with tab_breakdown:
    year_i = int(st.session_state.get("dq_year") or (years[0] if years else date.today().year))
    pi_nums = tuple(int(p) for p in (st.session_state.get("dq_pi") or []) if str(p).strip())
    if mode == "PORTFOLIO":
        st.markdown("## Program Readiness")
        st.caption("Program-level readiness across the current portfolio scope.")
        programs = selected_programs if selected_programs else program_options
        rows: list[dict[str, object]] = []
        for prog in programs:
            prog_name = str(prog).strip()
            if not prog_name:
                continue
            derived_teams = _teams_for_program(prog_name, year_i)
            readiness = compute_readiness(
                (prog_name,),
                tuple(),
                year_i,
                pi_nums,
                tuple(derived_teams),
                tuple(),
            )
            score_meta = _score_from_counts(
                int(readiness.get("BLOCKERS") or 0),
                int(readiness.get("WARNINGS") or 0),
            )
            rows.append(
                {
                    "Program": prog_name,
                    "Status": _status_badge(score_meta["status"]),
                    "Score": score_meta["score"],
                    "Blockers": int(readiness.get("BLOCKERS") or 0),
                    "Warnings": int(readiness.get("WARNINGS") or 0),
                    "Top Root Cause": readiness.get("TOP_ROOT_CAUSE"),
                }
            )
        prog_df = pd.DataFrame(rows)
        if prog_df.empty:
            st.info("All programs are healthy for the selected scope.")
        else:
            prog_df = prog_df.sort_values(
                by=["Score", "Blockers"],
                ascending=[True, False],
                kind="mergesort",
            )
            st.dataframe(
                prog_df[["Program", "Status", "Score", "Blockers", "Warnings", "Top Root Cause"]],
                use_container_width=True,
                hide_index=True,
            )
            top5 = prog_df.head(5)[["Program", "Score"]].copy()
            st.caption("Top 5 programs by lowest score")
            st.dataframe(top5, use_container_width=True, hide_index=True)
            st.markdown("**View backlog**")
            for i, r in enumerate(prog_df.head(25).itertuples(index=False), start=1):
                label = getattr(r, "Program", "")
                if not label:
                    continue
                if st.button(f"View backlog — {label}", key=f"dq_prog_backlog_{i}", use_container_width=True):
                    st.session_state["flt_programs"] = [label]
                    st.session_state["flt_teams"] = []
                    st.session_state["flt_groups"] = []
                    st.rerun()
    elif mode == "PROGRAM":
        st.markdown("## Team Readiness")
        st.caption("Team-level readiness breakdown for the selected program.")
        program_name = selected_programs[0] if selected_programs else ""
        teams = _teams_for_program(program_name, year_i)
        rows = []
        for team in teams:
            team_name = str(team).strip()
            if not team_name:
                continue
            readiness, score_meta, _ = _compute_team_readiness(
                team_name=team_name,
                year=year_i,
                pi_nums=pi_nums,
                program_hint=program_name,
            )
            rows.append(
                {
                    "Team": team_name,
                    "Status": _status_badge(score_meta["status"]),
                    "Score": score_meta["score"],
                    "Blockers": int(readiness.get("BLOCKERS") or 0),
                    "Warnings": int(readiness.get("WARNINGS") or 0),
                    "Top Root Cause": readiness.get("TOP_ROOT_CAUSE"),
                }
            )
        team_df = pd.DataFrame(rows)
        if team_df.empty:
            st.info("All teams are healthy for the selected scope.")
        else:
            team_df = team_df.sort_values(
                by=["Score", "Blockers"],
                ascending=[True, False],
                kind="mergesort",
            )
            st.dataframe(
                team_df[["Team", "Status", "Score", "Blockers", "Warnings", "Top Root Cause"]],
                use_container_width=True,
                hide_index=True,
            )
            top5 = team_df.head(5)[["Team", "Score"]].copy()
            st.caption("Top 5 teams by lowest score")
            st.dataframe(top5, use_container_width=True, hide_index=True)
            st.markdown("**View backlog**")
            for i, r in enumerate(team_df.head(25).itertuples(index=False), start=1):
                label = getattr(r, "Team", "")
                if not label:
                    continue
                if st.button(f"View backlog — {label}", key=f"dq_team_backlog_{i}", use_container_width=True):
                    st.session_state["flt_programs"] = [program_name] if program_name else []
                    st.session_state["flt_teams"] = [label]
                    st.session_state["flt_groups"] = []
                    st.rerun()
    else:
        st.markdown("## Top Issue Areas (Team)")
        st.caption("Top issue areas for the selected team.")
        if issues_df is None or issues_df.empty or "ISSUE_AREA" not in issues_df.columns:
            st.info("No issue breakdown available for this scope.")
        else:
            area_counts = (
                issues_df["ISSUE_AREA"]
                .fillna("Other")
                .astype(str)
                .value_counts()
                .reset_index()
                .rename(columns={"index": "Issue Area", "ISSUE_AREA": "Count"})
            )
            st.dataframe(area_counts.head(10), use_container_width=True, hide_index=True)

with tab_backlog:
    st.markdown("## Issue Backlog")
    backlog_df = issues_df.copy() if isinstance(issues_df, pd.DataFrame) else pd.DataFrame()
    if "SEVERITY" in backlog_df.columns:
        sev_norm = (
            backlog_df["SEVERITY"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.strip()
            .replace({"BLOCKER": "BLOCKING", "WARN": "WARNING"})
        )
        backlog_df["_SEVERITY_NORM"] = sev_norm
    else:
        backlog_df["_SEVERITY_NORM"] = ""

    sev_options = sorted(set(backlog_df["_SEVERITY_NORM"].dropna().astype(str).tolist()))
    f1, f2 = st.columns([1, 2])
    with f1:
        sel_sev = st.multiselect(
            "Severity",
            sev_options,
            default=[s for s in ["BLOCKING", "WARNING"] if s in sev_options],
            key="dq_backlog_severity",
        )
    with f2:
        search_term = st.text_input("Search", value="", key="dq_backlog_search")

    if search_term:
        q = str(search_term).strip().lower()
        cols = [
            c
            for c in [
                "TITLE",
                "ROOT_CAUSE",
                "ROOT_CAUSE_CODE",
                "ISSUE_TYPE",
                "SCOPE_PROGRAMS",
                "SCOPE_TEAMS",
                "SCOPE_APP_GROUPS",
            ]
            if c in backlog_df.columns
        ]
        if cols:
            mask = pd.Series([False] * len(backlog_df), index=backlog_df.index)
            for col in cols:
                mask = mask | backlog_df[col].fillna("").astype(str).str.lower().str.contains(q, na=False)
            backlog_df = backlog_df[mask].copy()

    scope_bw = backlog_df.copy()
    if "_SEVERITY_NORM" in scope_bw.columns:
        scope_bw = scope_bw[scope_bw["_SEVERITY_NORM"].isin(["BLOCKING", "WARNING"])].copy()

    if sel_sev:
        backlog_df = backlog_df[backlog_df["_SEVERITY_NORM"].isin([s.upper() for s in sel_sev])].copy()

    issues_filtered = backlog_df.copy()

    if scope_bw.empty:
        st.success("✅ No blocking or warning issues detected for the selected scope.\n\nYour NEXT data is ready for analysis.")
    else:

        def _issue_df_for_types_from(base_df: pd.DataFrame, types: set[str]) -> pd.DataFrame:
            if base_df is None or base_df.empty or "ISSUE_TYPE" not in base_df.columns:
                return pd.DataFrame()
            return base_df[base_df["ISSUE_TYPE"].astype(str).isin(types)].copy()

        default_cols = [
            "TITLE",
            "ROOT_CAUSE",
            "SEVERITY",
            "SCENARIO",
            "IMPACT_USD_EST",
            "FIX_LOCATION",
            "FIX_ACTION",
            "GUIDE_SECTION",
        ]
        advanced_cols = [
            "ISSUE_ID",
            "ISSUE_TYPE",
            "ROOT_CAUSE_CODE",
            "STATUS",
            "SOURCE",
            "IMPACT_FTE",
            "IMPACT_FEATURES",
            "IMPACT_BASIS",
            "IMPACT_BASIS_DETAIL",
            "CONFIDENCE",
            "TAGS",
            "SCOPE_YEAR",
            "SCOPE_PI",
            "SCOPE_PROGRAMS",
            "SCOPE_TEAMS",
            "SCOPE_APP_GROUPS",
            "GUIDE_KEY",
            "COMPUTED_AT",
        ]
        sort_cols = ["_SEV_RANK", "_IMPACT_USD_EST", "TITLE"]
        unmapped_apps_df = _issue_df_for_types_from(
            issues_filtered, {"ADO_UNMAPPED_APP", "APP_GROUP_MISSING_ADO_MAPPING"}
        )
        unmapped_team_df = _issue_df_for_types_from(issues_filtered, {"ADO_UNMAPPED_TEAM"})
        missing_swag_df = _issue_df_for_types_from(issues_filtered, {"ADO_FEATURES_MISSING_SWAG"})
        missing_app_name_df = _issue_df_for_types_from(issues_filtered, {"ADO_FEATURES_MISSING_APP_NAME"})
        unmapped_ado_teams_df = _issue_df_for_types_from(
            issues_filtered, {"CONFIG_ADO_TEAMS_UNMAPPED", "ADO_UNMAPPED_TEAMS_IN_PROGRAM"}
        )
        missing_effort_df = _issue_df_for_types_from(issues_filtered, {"ADO_MISSING_EFFORT"})
        unassigned_df = _issue_df_for_types_from(issues_filtered, {"UNASSIGNED_COST"})
        out_of_scope_df = _filter_by_root_code(unassigned_df, {"NOT_IN_SCOPE"})
        overhead_df = _filter_by_root_code(
            unassigned_df, {"PROGRAM_OVERHEAD", "PROGRAM_ADDITIONAL", "BASELINE_NOT_APP_ATTRIBUTED"}
        )
        mapping_any = any(
            not df.empty
            for df in [
                unmapped_apps_df,
                unmapped_team_df,
                missing_swag_df,
                missing_app_name_df,
                unmapped_ado_teams_df,
                missing_effort_df,
                out_of_scope_df,
                overhead_df,
            ]
        )
        if mapping_any:
            st.markdown("## Mapping & Coverage Issues")
            mapping_show_advanced = st.checkbox("Show advanced columns", value=False, key="dq_adv_mapping")
            _render_actions_row(
                [
                    ("Open Applications → ADO Mapping", "applications", {"focus": "ado_mapping"}),
                    ("Open Teams", "teams", None),
                    ("Open Programs", "programs", None),
                ],
                key_prefix="dq_actions_mapping",
            )
            if not unmapped_apps_df.empty:
                _render_issue_block(
                    title="🔴 Unmapped ADO App Names (Blocking)",
                    summary="ADO app names in scope are not linked to an onboarded Application.",
                    fix_in="Applications → ADO Mapping",
                    read_more="ADO App Name → NEXT Application Group Mapping",
                    df=unmapped_apps_df,
                    show_advanced=mapping_show_advanced,
                    default_cols=default_cols,
                    advanced_cols=advanced_cols,
                    sort_cols=sort_cols,
                )
                evidence_unmapped_app = resolve_evidence({"fn": "ado_unmapped_app", "params": {"limit": 200}}, ctx)
                if not evidence_unmapped_app.empty:
                    _render_ado_evidence_table(
                        evidence_unmapped_app,
                        missing_field="App Name",
                        show_advanced=mapping_show_advanced,
                        export_label="Export offenders (ADO app names)",
                        export_key="dq_export_ado_app",
                    )
                _render_unmapped_app_actions(unmapped_apps_df)

            if not unmapped_team_df.empty:
                _render_issue_block(
                    title="🟠 Unmapped ADO Teams (Warning)",
                    summary="ADO features are not mapped to a valid team.",
                    fix_in="Teams → Onboard / ADO Mapping",
                    read_more="ADO App Name → NEXT Application Group Mapping",
                    df=unmapped_team_df,
                    show_advanced=mapping_show_advanced,
                    default_cols=default_cols,
                    advanced_cols=advanced_cols,
                    sort_cols=sort_cols,
                )
                evidence_unmapped_team = resolve_evidence({"fn": "ado_unmapped_team", "params": {"limit": 200}}, ctx)
                if not evidence_unmapped_team.empty:
                    _render_ado_evidence_table(
                        evidence_unmapped_team,
                        missing_field="Team",
                        show_advanced=mapping_show_advanced,
                        export_label="Export offenders (ADO teams)",
                        export_key="dq_export_ado_team",
                    )

            if not missing_swag_df.empty:
                _render_issue_block(
                    title="🔴 ADO Features Missing SWAG (Blocking)",
                    summary="Story points are missing for in-scope ADO features.",
                    fix_in="External (ADO)",
                    read_more="ADO Feature Requirements (SWAG, App Name)",
                    df=missing_swag_df,
                    show_advanced=mapping_show_advanced,
                    default_cols=default_cols,
                    advanced_cols=advanced_cols,
                    sort_cols=sort_cols,
                )
                evidence_missing_swag = resolve_evidence({"fn": "ado_missing_swag", "params": {"limit": 200}}, ctx)
                if not evidence_missing_swag.empty:
                    _render_ado_evidence_table(
                        evidence_missing_swag,
                        missing_field="SWAG",
                        show_advanced=mapping_show_advanced,
                        export_label="Export offenders (missing SWAG)",
                        export_key="dq_export_ado_swag",
                    )

            if not missing_app_name_df.empty:
                _render_issue_block(
                    title="🔴 ADO Features Missing App Name (Blocking)",
                    summary="App Name is missing for in-scope ADO features.",
                    fix_in="External (ADO)",
                    read_more="ADO Feature Requirements (SWAG, App Name)",
                    df=missing_app_name_df,
                    show_advanced=mapping_show_advanced,
                    default_cols=default_cols,
                    advanced_cols=advanced_cols,
                    sort_cols=sort_cols,
                    )
                evidence_missing_app = resolve_evidence({"fn": "ado_missing_app_name", "params": {"limit": 200}}, ctx)
                if not evidence_missing_app.empty:
                    _render_ado_evidence_table(
                        evidence_missing_app,
                        missing_field="App Name",
                        show_advanced=mapping_show_advanced,
                        export_label="Export offenders (missing App Name)",
                        export_key="dq_export_ado_app_name",
                    )

            if not unmapped_ado_teams_df.empty:
                _render_issue_block(
                    title="🟠 ADO Teams Not Mapped (Warning)",
                    summary="ADO teams exist for this program but are not mapped to NEXT teams.",
                    fix_in="Settings → ADO",
                    read_more="ADO Team Mapping",
                    df=unmapped_ado_teams_df,
                    show_advanced=mapping_show_advanced,
                    default_cols=default_cols,
                    advanced_cols=advanced_cols,
                    sort_cols=sort_cols,
                )
                evidence_unmapped_teams = resolve_evidence(
                    {"fn": "ado_unmapped_teams_in_program", "params": {"limit": 200}}, ctx
                )
                if not evidence_unmapped_teams.empty:
                    view = evidence_unmapped_teams.copy()
                    rename = {
                        "ADO_TEAM_RAW": "ADO Team",
                        "PROGRAM_RAW": "Program",
                        "TEAM_DISPLAY": "Team Display",
                        "IS_MAPPED": "Mapped?",
                        "LAST_SEEN_AT": "Last Seen",
                        "FEATURE_COUNT": "Feature Count",
                        "EFFORT_SUM": "Effort Sum",
                    }
                    cols = [
                        c
                        for c in [
                            "PROGRAM_RAW",
                            "ADO_TEAM_RAW",
                            "TEAM_DISPLAY",
                            "FEATURE_COUNT",
                            "EFFORT_SUM",
                            "LAST_SEEN_AT",
                            "IS_MAPPED",
                        ]
                        if c in view.columns
                    ]
                    if cols:
                        view = view[cols].rename(columns=rename).replace({None: "—"}).fillna("—")
                        st.dataframe(view, use_container_width=True, hide_index=True)

            if not missing_effort_df.empty:
                _render_issue_block(
                    title="🟠 Derived FTE is 0 (Warning)",
                    summary="Derived FTE is 0 for in-scope features.",
                    fix_in="Settings → ADO / SWAG",
                    read_more="ADO Feature Requirements (SWAG, App Name)",
                    df=missing_effort_df,
                    show_advanced=mapping_show_advanced,
                    default_cols=default_cols,
                    advanced_cols=advanced_cols,
                    sort_cols=sort_cols,
                )
                evidence_missing_effort = resolve_evidence({"fn": "ado_missing_effort", "params": {"limit": 200}}, ctx)
                if not evidence_missing_effort.empty:
                    _render_ado_evidence_table(
                        evidence_missing_effort,
                        missing_field="SWAG",
                        show_advanced=mapping_show_advanced,
                        export_label="Export offenders (missing SWAG)",
                        export_key="dq_export_ado_swag",
                    )

            if not out_of_scope_df.empty:
                _render_issue_block(
                    title="🟠 Unassigned – Out of Scope (Warning)",
                    summary="Program-level items intentionally remain unallocated to Applications.",
                    fix_in="Data Quality → Mapping & Coverage",
                    read_more="Understanding Unassigned (Unmapped vs Out of Scope vs Program Overhead)",
                    df=out_of_scope_df,
                    empty_note="Not available yet for this scope.",
                    show_advanced=mapping_show_advanced,
                    default_cols=default_cols,
                    advanced_cols=advanced_cols,
                    sort_cols=sort_cols,
                    why_bullets=[
                        "Program-level costs can be in scope even without app attribution.",
                        "Some ADO rows lack app group mapping by design.",
                        "These items should be reviewed, not necessarily fixed.",
                    ],
                )

            if not overhead_df.empty:
                _render_issue_block(
                    title="🔵 Program Overhead (Expected)",
                    summary="Shared program costs are expected to stay at program level.",
                    fix_in="Programs → Overview",
                    read_more="Program Overhead Semantics",
                    df=overhead_df,
                    empty_note="Program overhead items are not listed for this scope yet.",
                    show_advanced=mapping_show_advanced,
                    default_cols=default_cols,
                    advanced_cols=advanced_cols,
                    sort_cols=sort_cols,
                    why_bullets=[
                        "Overhead is planned at program scope by design.",
                        "These costs are not intended to be assigned to Applications.",
                    ],
                )

        integrity_df = _issue_df_for_types_from(issues_filtered, integrity_types)
        if not integrity_df.empty:
            st.markdown("## Cost Model Integrity Issues")
            integrity_show_advanced = st.checkbox("Show advanced columns", value=False, key="dq_adv_integrity")
            _render_actions_row(
                [
                    ("Open Rates", "rates", None),
                    ("Open Teams", "teams", None),
                    ("Open Programs", "programs", None),
                    ("Open Applications", "applications", None),
                ],
                key_prefix="dq_actions_integrity",
            )
            _render_issue_block(
                title="Rates & Ownership Gaps",
                summary="Missing rates or ownership fields prevent complete cost calculation.",
                fix_in="Rates, Teams, Applications",
                read_more="Rates & Time Semantics (Rates are per PI / iteration)",
                df=integrity_df,
                show_advanced=integrity_show_advanced,
                default_cols=default_cols,
                advanced_cols=advanced_cols,
                sort_cols=sort_cols,
            )

        show_info = st.checkbox(
            "Show informational checks",
            value=(mode != "TEAM"),
            key="dq_show_info",
        )
        if show_info:
            info_df = issues_filtered.copy() if isinstance(issues_filtered, pd.DataFrame) else pd.DataFrame()
            if not info_df.empty and "SEVERITY" in info_df.columns:
                info_df = info_df[info_df["SEVERITY"].astype(str).str.upper().eq("INFO")].copy()
            scenario_all = (
                pd.concat([_issue_df_for_types_from(issues_filtered, scenario_types), info_df], ignore_index=True)
                if not info_df.empty
                else _issue_df_for_types_from(issues_filtered, scenario_types)
            )
            if not scenario_all.empty:
                st.markdown("## Scenario Consistency & Informational Checks")
                scenario_show_advanced = st.checkbox("Show advanced columns", value=False, key="dq_adv_scenario")
                _render_actions_row(
                    [
                        ("Open Rates", "rates", None),
                        ("Open Data Quality", "how_to", {"section": "GUIDE_SCENARIOS"}),
                    ],
                    key_prefix="dq_actions_scenario",
                )
                _render_issue_block(
                    title="Scenario Consistency & Informational Checks",
                    summary="Scenario inputs and actuals alignment are tracked for transparency.",
                    fix_in="Data Quality, Rates",
                    read_more="Actuals Granularity (Program-level Apptio)",
                    df=scenario_all,
                    show_advanced=scenario_show_advanced,
                    default_cols=default_cols,
                    advanced_cols=advanced_cols,
                    sort_cols=sort_cols,
                    why_bullets=[
                        "Actuals can be program-level only depending on source system.",
                        "Scenario alignment relies on consistent mappings and timing.",
                    ],
                )

with tab_fix:
    st.markdown("## How to Fix")
    st.markdown(
        "\n".join(
            [
                "- If you see **Unmapped ADO App Names** → go to **Applications → ADO Mapping**.",
                "- If you see **Missing rates** → go to **Rates**.",
                "- If you see **Ownership & Structure** issues → update ownership in **Programs**, **Teams**, or **Applications**.",
            ]
        )
    )
    _render_actions_row(
        [
            ("Open Applications → ADO Mapping", "applications", {"focus": "ado_mapping"}),
            ("Open Rates", "rates", None),
            ("Open Teams", "teams", None),
            ("Open Programs", "programs", None),
        ],
        key_prefix="dq_actions_fix",
    )
    st.markdown("## Severity Legend")
    st.markdown("- 🔴 Blocking\n- 🟠 Warning\n- 🔵 Info")
    with st.expander("Glossary & Concepts", expanded=False):
        st.markdown(
            "\n".join(
                [
                    "- **SWAG**: Story-point-derived effort used for Expected demand. See How-To → ADO Feature Requirements (SWAG, App Name).",
                    "- **ADO App Name**: Raw ADO application identifier mapped to NEXT Applications. See How-To → ADO App Name → NEXT Application Group Mapping.",
                    "- **Unassigned**: Items not attributed to an Application (unmapped or out of scope). See How-To → Understanding Unassigned (Unmapped vs Out of Scope vs Program Overhead).",
                    "- **Program Overhead**: Shared program-level costs that stay at program scope. See How-To → Program Overhead Semantics.",
                    "- **Blocking / Warning / Info**: Severity levels used in Data Quality. See How-To → Severity Model (Used in Data Quality).",
                ]
            )
        )

with tab_catalog:
    st.markdown("## Issue Catalog")
    if issues_df is None or issues_df.empty:
        st.info("No issues available for the selected scope.")
    else:
        catalog_src = issues_df.copy()
        catalog_src["SCENARIO"] = catalog_src.get("SCENARIO", "—").fillna("—").replace("", "—")
        catalog_src["FIX_LOCATION"] = _fix_location_display_series(catalog_src)
        expect_evidence_types = {
            "ADO_UNMAPPED_APP",
            "ADO_UNMAPPED_TEAM",
            "ADO_FEATURES_MISSING_SWAG",
            "ADO_FEATURES_MISSING_APP_NAME",
            "ADO_MISSING_EFFORT",
            "UNASSIGNED_COST",
            "APPTIO_UNMAPPED_ACTUALS",
        }
        rows: list[dict[str, object]] = []
        for issue_type, g in catalog_src.groupby("ISSUE_TYPE", dropna=False):
            g = g.copy()
            issue_type_str = str(issue_type or "—")
            issue_area = str(g.get("ISSUE_AREA", pd.Series(["—"])).fillna("—").iloc[0])
            root_code = str(g.get("ROOT_CAUSE_CODE", pd.Series(["—"])).fillna("—").iloc[0])
            title_example = (
                g.get("TITLE", pd.Series(["—"]))
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .head(1)
                .tolist()
            )
            title_example_val = title_example[0] if title_example else "—"
            sev_vals = sorted(set(g.get("SEVERITY", pd.Series(["—"])).fillna("—").astype(str).tolist()))
            scen_vals = sorted(set(g.get("SCENARIO", pd.Series(["—"])).fillna("—").astype(str).tolist()))
            fix_system_vals = sorted(set(g.get("FIX_SYSTEM", pd.Series(["—"])).fillna("—").astype(str).tolist()))
            fix_target_vals = sorted(set(g.get("FIX_TARGET", pd.Series(["—"])).fillna("—").astype(str).tolist()))
            fix_location_vals = sorted(set(g.get("FIX_LOCATION", pd.Series(["—"])).fillna("—").astype(str).tolist()))
            fix_action_example = (
                g.get("FIX_ACTION", pd.Series(["—"]))
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .head(1)
                .tolist()
            )
            fix_action_val = fix_action_example[0] if fix_action_example else "—"
            guide_section = (
                g.get("GUIDE_SECTION", pd.Series(["—"]))
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .head(1)
                .tolist()
            )
            guide_val = guide_section[0] if guide_section else "—"
            evidence_vals = sorted(set(g.get("EVIDENCE_FN", pd.Series(["—"])).fillna("—").astype(str).tolist()))
            missing_fix_page = ("—" in fix_system_vals) or (
                any(v == "TCO" for v in fix_system_vals) and all(v in {"", "—"} for v in fix_target_vals)
            )
            missing_scenario = any(v in {"", "—"} for v in scen_vals)
            missing_guide = guide_val in {"", "—"}
            expects_evidence_col = g.get("EXPECTS_EVIDENCE", pd.Series([], dtype=bool))
            expects_evidence = (
                bool(expects_evidence_col.dropna().iloc[0])
                if not expects_evidence_col.empty
                else (issue_type_str in expect_evidence_types)
            )
            missing_evidence = expects_evidence and all(v in {"", "—"} for v in evidence_vals)
            notes: list[str] = []
            if missing_fix_page:
                notes.append("Fix page missing or inconsistent")
            if missing_scenario:
                notes.append("Scenario missing")
            if missing_guide:
                notes.append("Guide section missing")
            if missing_evidence:
                notes.append("Evidence missing")
            ado_only = issue_type_str in {"ADO_FEATURES_MISSING_SWAG", "ADO_FEATURES_MISSING_APP_NAME"}
            if ado_only and "ADO" not in fix_system_vals:
                notes.append("Fix system should be ADO")
            rows.append(
                {
                    "ISSUE_TYPE": issue_type_str,
                    "ISSUE_AREA": issue_area,
                    "ROOT_CAUSE_CODE": root_code,
                    "TITLE_EXAMPLE": title_example_val,
                    "SEVERITY_VALUES": ", ".join(sev_vals),
                    "SCENARIO_VALUES": ", ".join(scen_vals),
                    "FIX_SYSTEM_VALUES": ", ".join(fix_system_vals),
                    "FIX_TARGET_VALUES": ", ".join(fix_target_vals),
                    "FIX_LOCATION_VALUES": ", ".join(fix_location_vals),
                    "FIX_ACTION_EXAMPLE": fix_action_val,
                    "GUIDE_SECTION": guide_val,
                    "EVIDENCE_FN_VALUES": ", ".join(evidence_vals),
                    "COUNT": int(len(g)),
                    "MISSING_FIX_PAGE": bool(missing_fix_page),
                    "MISSING_SCENARIO": bool(missing_scenario),
                    "MISSING_GUIDE": bool(missing_guide),
                    "MISSING_EVIDENCE": bool(missing_evidence),
                    "NOTES": "; ".join(notes) if notes else "—",
                }
            )

        catalog_df = pd.DataFrame(rows)
        if catalog_df.empty:
            st.info("No issue types found for this scope.")
        else:
            st.dataframe(catalog_df, use_container_width=True, hide_index=True)
            catalog_csv = catalog_df.to_csv(index=False)
            st.download_button(
                "Export catalog",
                data=catalog_csv,
                file_name="issue_catalog.csv",
                mime="text/csv",
                use_container_width=True,
                key="dq_export_issue_catalog",
            )

            st.markdown("### Issue details for selected type")
            issue_types = sorted(catalog_df["ISSUE_TYPE"].dropna().astype(str).tolist())
            selected_issue = st.selectbox("Issue type", issue_types, key="dq_issue_catalog_select")
            sample = issues_df[issues_df["ISSUE_TYPE"].astype(str).eq(str(selected_issue))].copy()
            if not sample.empty:
                sample["FIX_LOCATION"] = _fix_location_display_series(sample)
                cols = [
                    c
                    for c in [
                        "ISSUE_ID",
                        "TITLE",
                        "ROOT_CAUSE",
                        "SEVERITY",
                        "SCENARIO",
                        "FIX_SYSTEM",
                        "FIX_TARGET",
                        "FIX_LOCATION",
                        "FIX_ACTION",
                        "GUIDE_SECTION",
                        "EVIDENCE_FN",
                        "SCOPE_PROGRAMS",
                        "SCOPE_TEAMS",
                    ]
                    if c in sample.columns
                ]
                st.dataframe(sample[cols].head(25), use_container_width=True, hide_index=True)
            else:
                st.info("No rows found for this issue type in the current scope.")
