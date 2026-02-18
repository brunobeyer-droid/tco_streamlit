from __future__ import annotations

import json
import datetime as dt
import re
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

from core.init import init_page, page_loader
from core.data import (
    fetch_filter_options,
    fetch_ado_features_for_roadmap,
    fetch_ado_team_options,
    fetch_pi_calendar_resolved_dates,
)
from core.ado_profile import get_active_ado_profile, normalize_profile_config
from core.debug import is_debug_enabled
from core.sod_series import capacity_fte_by_pi
from core.scope import infer_role, read_scope_from_session, scope_label
from core.ui import render_page_header, touch_last_updated_status
from core.cache_utils import cache_data_portfolio
from core.freshness import post_write_refresh
from core.name_resolution import apply_display_scope_names
from core.roadmap_ui_events import (
    unpack_event,
    selection_from_payload,
    route_event,
)
from db import (  # type: ignore
    get_data_version,
    list_programs,
    list_teams,
    list_application_groups,
    list_roadmap_milestones,
    upsert_roadmap_milestone,
    delete_roadmap_milestone,
)
from db import fetch_df_active as _fetch_df_raw

from components.roadmap_ui import roadmap_ui, roadmap_ui_debug_info
from utils.ado import normalize_pi_key
from utils.ui_patterns import render_active_filters_summary

from utils.app_shell import bootstrap_page
bootstrap_page()


def fetch_df(sql, params=None):  # type: ignore[override]
    df = _fetch_df_raw(sql, params)
    return apply_display_scope_names(df) if isinstance(df, pd.DataFrame) else df


page_theme = init_page("Roadmap", page_path=__file__)


class _NoopLoader:
    def step(self, _label: Optional[str] = None) -> None:
        return None


@contextmanager
def _noop_page_loader():
    yield _NoopLoader()


def _link_button(label: str, url: str) -> None:
    if not url:
        return
    try:
        st.link_button(label, url)
    except Exception:
        st.markdown(f"[{label}]({url})")

def _ado_workitem_url(org: str, project: str, work_item_id: Any) -> str:
    try:
        wid = str(int(float(work_item_id)))
    except Exception:
        wid = str(work_item_id or "").strip()
    if not (org and project and wid):
        return ""
    return f"https://dev.azure.com/{org}/{project}/_workitems/edit/{wid}"

def _parse_org_project_from_odata_base(base_url: str) -> Tuple[str, str]:
    try:
        parsed = urlparse(str(base_url or "").strip())
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2:
            return parts[0], parts[1]
    except Exception:
        return "", ""
    return "", ""


def _current_user_email() -> str:
    for key in ("auth_email", "user_email", "email"):
        raw = str(st.session_state.get(key) or "").strip()
        if raw:
            return raw
    auth_user = st.session_state.get("auth_user")
    if isinstance(auth_user, dict):
        raw = str(auth_user.get("email") or "").strip()
        if raw:
            return raw
    return ""


def _pi_display_label(label: Any, fallback_key: Any = "") -> str:
    """Display-only PI label normalizer. Keeps data keys untouched."""
    raw = str(label or "").strip()
    if raw:
        short = normalize_pi_key(raw)
        if short:
            return short
        return " ".join(raw.split())
    fb = normalize_pi_key(fallback_key)
    if fb:
        return fb
    return " ".join(str(fallback_key or "").strip().split())


def _roadmap_event_signature(action: str, payload: Dict[str, Any]) -> str:
    try:
        raw = json.dumps(payload or {}, sort_keys=True, default=str, separators=(",", ":"))
    except Exception:
        raw = str(payload or {})
    return f"{str(action or '').strip().lower()}::{raw}"


def _build_filter_options(df: pd.DataFrame) -> Tuple[List[int], List[str], List[str], List[str]]:
    if df is None or df.empty:
        return [], [], [], []
    years = sorted({int(y) for y in df["YEAR"].dropna().tolist() if str(y).isdigit()})
    programs = sorted({str(x).strip() for x in df["PROGRAMNAME"].dropna().tolist() if str(x).strip()})
    teams = sorted({str(x).strip() for x in df["TEAMNAME"].dropna().tolist() if str(x).strip()})
    if "GROUPNAME" in df.columns:
        groups = sorted({str(x).strip() for x in df["GROUPNAME"].dropna().tolist() if str(x).strip()})
    else:
        groups = []
    return years, programs, teams, groups


def _resolve_rows_by_mode(raw_value: Any, *, default_mode: str = "Epics") -> str:
    group_options = {"Applications", "Epics"}
    legacy_labels = {
        "Applications (App Groups)": "Applications",
        "Application (App Group)": "Applications",
        "App Groups": "Applications",
        "Epic": "Epics",
    }
    current_group = str(raw_value or default_mode)
    current_group = legacy_labels.get(current_group, current_group)
    if current_group not in group_options:
        current_group = default_mode if default_mode in group_options else "Epics"
    return current_group


def _build_display_lookup(label_map: Dict[str, str]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for raw, disp in (label_map or {}).items():
        raw_s = str(raw or "").strip()
        disp_s = str(disp or "").strip()
        if not raw_s or not disp_s:
            continue
        lookup.setdefault(raw_s.casefold(), disp_s)
        lookup.setdefault(disp_s.casefold(), disp_s)
    return lookup


def _apply_display_name_maps(
    df: pd.DataFrame,
    *,
    program_label_map: Dict[str, str],
    team_label_map: Dict[str, str],
    group_label_map: Dict[str, str],
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    program_lookup = _build_display_lookup(program_label_map)
    team_lookup = _build_display_lookup(team_label_map)
    group_lookup = _build_display_lookup(group_label_map)

    def _remap_col(col: str, lookup: Dict[str, str]) -> None:
        if col not in out.columns or not lookup:
            return
        ser = out[col].fillna("").astype(str).str.strip()
        out[col] = ser.map(lambda v: lookup.get(v.casefold(), v))

    _remap_col("PROGRAMNAME", program_lookup)
    _remap_col("TEAMNAME", team_lookup)
    _remap_col("GROUPNAME", group_lookup)
    return out


def _mapping_filter(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    status = df.get("MAPPING_STATUS").fillna("").astype(str).str.upper()
    groupname = df.get("GROUPNAME", pd.Series("", index=df.index)).astype(str)
    unmapped = status.str.contains("UNMAPPED") | status.str.contains("OUT_OF_SCOPE") | (groupname == "(Unmapped Application)")
    mapped = ~unmapped
    if mode == "Mapped only":
        return df[mapped]
    if mode == "Unassigned only":
        return df[unmapped]
    return df


def _normalize_features(df: pd.DataFrame, rows_by: str) -> Tuple[pd.DataFrame, bool]:
    if df is None or df.empty:
        return pd.DataFrame(), False
    out = df.copy()
    out["FEATURE_ID"] = out.get("FEATURE_ID")
    out["TITLE"] = out.get("TITLE", out.get("FEATURE_TITLE", ""))
    out["PROGRAMNAME"] = out.get("PROGRAMNAME")
    out["TEAMNAME"] = out.get("TEAMNAME")
    if rows_by == "Applications":
        group_col = out.get("GROUPNAME")
        if group_col is None:
            group_col = pd.Series([None] * len(out), index=out.index)
        out["GROUPNAME"] = group_col.fillna("(Unmapped Application)")
    out["PI_LABEL"] = out.get("PI_LABEL")
    pi_raw = out.get("ITERATION_LEVEL3_RAW")
    if pi_raw is None:
        pi_raw = out.get("ITERATION_LEVEL3")
    if pi_raw is None:
        pi_raw = out.get("PI_LABEL")
    if pi_raw is None:
        pi_raw = pd.Series([None] * len(out), index=out.index)
    out["PI_LABEL_RAW"] = pi_raw
    out["PI_KEY"] = pi_raw.fillna("").astype(str).map(normalize_pi_key)
    out["PI_KEY"] = out["PI_KEY"].replace({"": None, "nan": None, "None": None})
    if "ITERATION_NUM" in out.columns:
        out["PI_SORT"] = pd.to_numeric(out["ITERATION_NUM"], errors="coerce")
    else:
        out["PI_SORT"] = pd.NA
    points = pd.to_numeric(out.get("STORY_POINTS"), errors="coerce")
    has_points = points.notna().any()
    if has_points:
        out["POINTS"] = points.fillna(0.0)
    else:
        out["POINTS"] = 1.0
    out["STATE"] = out.get("STATE")
    out["MAPPING_STATUS"] = out.get("MAPPING_STATUS")
    out["BUSINESS_VALUE"] = pd.to_numeric(out.get("BUSINESS_VALUE"), errors="coerce")
    out["EPIC_TITLE"] = out.get("EPIC_TITLE")
    out["EPIC_STATE"] = out.get("EPIC_STATE")
    out["FEATURE_URL"] = out.get("FEATURE_URL")
    out["EPIC_URL"] = out.get("EPIC_URL")
    if out["PI_KEY"].isna().any():
        out["PI_KEY"] = out["PI_KEY"].fillna(out.get("ITERATION_NUM").astype(str))
    out["PI_KEY"] = out["PI_KEY"].fillna("PI")
    out["PI_LABEL"] = out["PI_KEY"]
    if rows_by == "Epics":
        epic_title = out.get("EPIC_TITLE")
        out["ROW_KEY"] = epic_title.fillna("").astype(str).str.strip()
        out.loc[out["ROW_KEY"].eq(""), "ROW_KEY"] = "(No Epic)"
    else:
        out["ROW_KEY"] = out.get("GROUPNAME").fillna("(Unmapped Application)")
    return out, has_points


def _compute_derived_fte(df: pd.DataFrame, has_points: bool) -> pd.Series:
    # Match SWAG-derived FTE logic used in analytics views.
    fte_col = next(
        (c for c in ["DERIVED_FTE_SWAG", "DERIVED_FTE", "DERIVED_FTE_SUM"] if c in df.columns),
        None,
    )
    if fte_col:
        return pd.to_numeric(df.get(fte_col), errors="coerce").fillna(0.0)
    if has_points:
        swag_points_per_fte = 65.0
        return pd.to_numeric(df.get("POINTS"), errors="coerce").fillna(0.0) / swag_points_per_fte
    return pd.Series(0.0, index=df.index)


def _build_pi_order(df: pd.DataFrame) -> List[str]:
    if "PI_SORT" in df.columns and df["PI_SORT"].notna().any():
        order = (
            df.groupby("PI_KEY", dropna=False)["PI_SORT"]
            .min()
            .sort_values()
            .index.astype(str)
            .tolist()
        )
        return order
    return sorted({str(x) for x in df["PI_KEY"].dropna().tolist()})


def _attach_pi_dates(df: pd.DataFrame, year: int, pi_order: List[str], data_version: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["PI_KEY", "PI_START_DATE", "PI_END_DATE"])
    cal = fetch_pi_calendar_resolved_dates(years=(int(year),), data_version=data_version)
    if cal is not None and not cal.empty:
        cal = cal.copy()
        cal["CAL_PI_KEY"] = cal.get("CAL_PI_KEY", "").fillna("").astype(str).map(normalize_pi_key)
        cal = cal.rename(
            columns={
                "CAL_PI_KEY": "PI_KEY",
                "CAL_START_DATE": "PI_START_DATE",
                "CAL_END_DATE": "PI_END_DATE",
            }
        )
        out = df.merge(cal[["PI_KEY", "PI_START_DATE", "PI_END_DATE"]], on="PI_KEY", how="left")
        if out["PI_START_DATE"].notna().any() and out["PI_END_DATE"].notna().any():
            return out
    base = dt.date(int(year), 1, 1)
    fallback = {}
    for idx, pi in enumerate(pi_order):
        start = base + dt.timedelta(days=idx * 14)
        end = start + dt.timedelta(days=13)
        fallback[pi] = (pd.Timestamp(start), pd.Timestamp(end))
    out = df.copy()
    out["PI_START_DATE"] = out["PI_KEY"].map(lambda x: fallback.get(str(x), (pd.NaT, pd.NaT))[0])
    out["PI_END_DATE"] = out["PI_KEY"].map(lambda x: fallback.get(str(x), (pd.NaT, pd.NaT))[1])
    return out


def _build_pi_calendar_map(
    years: Tuple[int, ...],
    pi_order: List[str],
    data_version: int,
) -> Tuple[Dict[str, Dict[str, Optional[pd.Timestamp]]], List[str], Dict[str, str]]:
    cal = fetch_pi_calendar_resolved_dates(years=years, data_version=data_version)
    pi_dates: Dict[str, Dict[str, Optional[pd.Timestamp]]] = {}
    ordered_keys: List[str] = []
    label_by_key: Dict[str, str] = {}
    if cal is not None and not cal.empty:
        cal = cal.copy()
        cal["CAL_PI_KEY"] = cal.get("CAL_PI_KEY", "").fillna("").astype(str).map(normalize_pi_key)
        cal["CAL_ITERATION_LEVEL3"] = cal.get("CAL_ITERATION_LEVEL3", "").fillna("").astype(str).str.strip()
        cal["CAL_START_DATE"] = pd.to_datetime(cal.get("CAL_START_DATE"), errors="coerce")
        cal["CAL_END_DATE"] = pd.to_datetime(cal.get("CAL_END_DATE"), errors="coerce")
        cal = cal.sort_values(by=["CAL_START_DATE", "CAL_END_DATE"])
        for _, row in cal.drop_duplicates("CAL_PI_KEY").iterrows():
            pi_key = normalize_pi_key(row.get("CAL_PI_KEY") or row.get("CAL_ITERATION_LEVEL3") or "")
            pi_label = str(row.get("CAL_ITERATION_LEVEL3") or "")
            start = row.get("CAL_START_DATE")
            end = row.get("CAL_END_DATE")
            if pi_key:
                pi_dates[pi_key] = {
                    "start_ts": start if pd.notna(start) else None,
                    "end_ts": end if pd.notna(end) else None,
                }
                ordered_keys.append(pi_key)
                if pi_label:
                    label_by_key[pi_key] = pi_label
    base_year = min(years) if years else dt.datetime.now().year
    base = dt.date(int(base_year), 1, 1)
    for idx, pi in enumerate(pi_order):
        pi_key = normalize_pi_key(pi)
        if not pi_key or pi_key in pi_dates:
            continue
        start = base + dt.timedelta(days=idx * 14)
        end = start + dt.timedelta(days=13)
        pi_dates[pi_key] = {"start_ts": pd.Timestamp(start), "end_ts": pd.Timestamp(end)}
    return pi_dates, ordered_keys, label_by_key


def _bucket_feature(state: Any, pi_end: Optional[pd.Timestamp], risk_window_days: int) -> str:
    state_norm = re.sub(r"\s+", " ", str(state or "").strip().upper())
    state_compact = re.sub(r"[^A-Z0-9]", "", state_norm)

    completed_states = {
        "DONE",
        "CLOSED",
        "RESOLVED",
        "COMPLETE",
        "COMPLETED",
        "FINISHED",
    }
    planned_states = {
        "PROPOSED",
        "PLANNED",
        "PLANNING",
        "PLAN",
        "READY",
        "NEW",
        "TODO",
        "BACKLOG",
    }
    validation_states = {
        "VALIDATION",
        "VALIDATING",
        "UAT",
        "QA",
        "TEST",
        "TESTING",
        "READYFORUAT",
        "READYFORVALIDATION",
    }
    in_progress_states = {
        "INPROGRESS",
        "ACTIVE",
        "COMMITTED",
        "IMPLEMENTING",
        "IMPLEMENTATION",
        "DEVELOPMENT",
        "DOING",
        "EXECUTING",
        "WORKINPROGRESS",
        "WIP",
    }

    if state_compact in completed_states:
        return "done"
    if state_compact in planned_states:
        return "planned"
    if state_compact in validation_states:
        return "validation"
    if state_compact in in_progress_states:
        return "active"
    # Keep unknown states as active to avoid misleading "planned" color fallback.
    return "active"


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_features_cached(
    signature: str,
    cache_bust: str,
    years: Tuple[int, ...],
    programs: List[str],
    teams: List[str],
    groups: List[str],
    data_version: int,
) -> pd.DataFrame:
    frames = []
    for yr in years:
        frames.append(fetch_ado_features_for_roadmap(int(yr), programs, teams, groups, rev=None, data_version=data_version))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


@cache_data_portfolio(ttl=180, show_spinner=False)
def _build_chip_rows(signature: str, df: pd.DataFrame, max_per_cell: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    grouped = df.groupby(["PI_KEY", "ROW_KEY"], dropna=False)
    for (pi_key, row_key), g in grouped:
        g_sorted = g.sort_values(by=["POINTS", "TITLE"], ascending=[False, True])
        top = g_sorted.head(max_per_cell)
        for idx, (_, row) in enumerate(top.iterrows()):
            rows.append(
                {
                    "PI_KEY": pi_key,
                    "ROW_KEY": row_key,
                    "CHIP_INDEX": idx,
                    "CHIP_TYPE": "feature",
                    "FEATURE_ID": row.get("FEATURE_ID"),
                    "TITLE": row.get("TITLE"),
                    "TITLE_SHORT": str(row.get("TITLE") or "")[:80],
                    "POINTS": float(row.get("POINTS") or 0.0),
                    "DERIVED_FTE": float(row.get("DERIVED_FTE") or 0.0),
                    "STATE": row.get("STATE"),
                    "PROGRAMNAME": row.get("PROGRAMNAME"),
                    "TEAMNAME": row.get("TEAMNAME"),
                }
            )
        # "more" indicator handled at cell layer for the HTML chart.
    return pd.DataFrame(rows)


def _selection_kpis(df: pd.DataFrame, has_points: bool) -> Tuple[float, int]:
    if df is None or df.empty:
        return 0.0, 0
    if has_points:
        total = float(pd.to_numeric(df.get("POINTS"), errors="coerce").fillna(0.0).sum())
    else:
        total = float(df["FEATURE_ID"].nunique())
    return total, int(df["FEATURE_ID"].nunique())


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_timeline_overlay(feature_ids: Tuple[int, ...], data_version: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not feature_ids:
        return pd.DataFrame(), pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(feature_ids))
    params = tuple(int(x) for x in feature_ids)
    try:
        df_dates = fetch_df(
            f"""
            SELECT
              WORKITEM_ID,
              WORKITEM_TYPE,
              START_DATE,
              END_DATE,
              ITERATION_PATH,
              UPDATED_AT
            FROM ADO_WORKITEM_DATES
            WHERE WORKITEM_ID IN ({placeholders})
            """,
            params,
        )
    except Exception:
        df_dates = pd.DataFrame()
    try:
        df_links = fetch_df(
            f"""
            SELECT
              SOURCE_ID,
              TARGET_ID,
              LINK_CATEGORY,
              LINK_TYPE,
              UPDATED_AT
            FROM ADO_WORKITEM_LINKS
            WHERE SOURCE_ID IN ({placeholders})
              AND TARGET_ID IN ({placeholders})
            """,
            params + params,
        )
    except Exception:
        df_links = pd.DataFrame()
    return (
        df_dates if isinstance(df_dates, pd.DataFrame) else pd.DataFrame(),
        df_links if isinstance(df_links, pd.DataFrame) else pd.DataFrame(),
    )


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_iteration_calendar_slice(
    iteration_paths: Tuple[str, ...],
    pi_keys: Tuple[str, ...],
    data_version: int,
) -> pd.DataFrame:
    _ = data_version
    paths = [str(p).strip() for p in (iteration_paths or ()) if str(p).strip()]
    pis = [str(p).strip() for p in (pi_keys or ()) if str(p).strip()]
    if not paths and not pis:
        return pd.DataFrame()

    where_parts: List[str] = []
    params: List[str] = []
    if paths:
        where_parts.append("ITERATION_PATH IN (" + ", ".join(["%s"] * len(paths)) + ")")
        params.extend(paths)
    if pis:
        where_parts.append("ITERATION_LEVEL3 IN (" + ", ".join(["%s"] * len(pis)) + ")")
        params.extend(pis)
    if not where_parts:
        return pd.DataFrame()

    where_sql = " OR ".join([f"({w})" for w in where_parts])
    try:
        df = fetch_df(
            f"""
            SELECT
              ITERATION_PATH,
              ITERATION_LEVEL3,
              ITERATION_GRAIN,
              START_DATE,
              END_DATE
            FROM ADO_ITERATION_CALENDAR
            WHERE {where_sql}
            """,
            tuple(params),
        )
    except Exception:
        df = pd.DataFrame()
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _build_epic_timeline_payload(
    *,
    df: pd.DataFrame,
    selection: Dict[str, Any],
    group_col: str,
    group_mode: str,
    no_epic_label: str,
    data_version: int,
    pi_catalog: Optional[List[Dict[str, Any]]] = None,
    milestones_df: Optional[pd.DataFrame] = None,
) -> Optional[Dict[str, Any]]:
    if df is None or df.empty:
        return None

    sel_feature = str(selection.get("feature_id") or "").strip()
    sel_epic = str(selection.get("epic_id") or "").strip()
    selected_mode = str(selection.get("mode") or "").strip().lower()

    epic_rows = pd.DataFrame()
    multi_epic_mode = False
    focus_epic_id = ""
    if group_mode == "Epics":
        epic_rows = df.copy()
        if group_col in epic_rows.columns:
            epic_rows = epic_rows.loc[epic_rows[group_col].astype(str).str.strip().ne(no_epic_label)].copy()
        multi_epic_mode = True
        if selected_mode == "feature" and sel_feature:
            feat_row = df.loc[df["FEATURE_ID"].astype(str) == sel_feature].copy()
            if not feat_row.empty:
                epic_id = feat_row.iloc[0].get("EPIC_ID")
                if epic_id is not None and str(epic_id).strip() not in {"", "nan", "None"}:
                    focus_epic_id = str(epic_id).strip()
        elif selected_mode == "epic" and sel_epic:
            focus_epic_id = sel_epic
    elif selected_mode == "feature" and sel_feature:
        feat_row = df.loc[df["FEATURE_ID"].astype(str) == sel_feature].copy()
        if not feat_row.empty:
            epic_id = feat_row.iloc[0].get("EPIC_ID")
            if epic_id is not None and str(epic_id).strip() not in {"", "nan", "None"}:
                epic_rows = df.loc[df["EPIC_ID"] == epic_id].copy()
                focus_epic_id = str(epic_id).strip()
    else:
        # Applications mode with no feature selected: show timeline for current scope
        # instead of returning empty payload.
        epic_rows = df.copy()
        multi_epic_mode = True

    if epic_rows.empty:
        return None

    epic_rows["EPIC_ID_STR"] = epic_rows.get("EPIC_ID", "").astype(str).str.strip()
    epic_rows = epic_rows.loc[
        epic_rows["EPIC_ID_STR"].ne("")
        & epic_rows["EPIC_ID_STR"].str.lower().ne("none")
        & epic_rows["EPIC_ID_STR"].str.lower().ne("nan")
    ].copy()
    if epic_rows.empty:
        return None

    if not focus_epic_id:
        try:
            focus_epic_id = str(epic_rows["EPIC_ID_STR"].iloc[0]).strip()
        except Exception:
            focus_epic_id = ""
    if not focus_epic_id:
        return None

    def _epic_title_for(rows_df: pd.DataFrame, eid: str) -> str:
        part = rows_df.loc[rows_df["EPIC_ID_STR"].astype(str) == str(eid)].copy()
        if "EPIC_TITLE" in part.columns and part["EPIC_TITLE"].notna().any():
            try:
                return str(part["EPIC_TITLE"].dropna().astype(str).iloc[0]).strip() or "(Epic)"
            except Exception:
                pass
        return "(Epic)"

    epic_title_val = _epic_title_for(epic_rows, focus_epic_id)

    feat_df = epic_rows.copy()
    feat_df["FEATURE_ID"] = pd.to_numeric(feat_df.get("FEATURE_ID"), errors="coerce")
    feat_df = feat_df.loc[feat_df["FEATURE_ID"].notna()].copy()
    if feat_df.empty:
        return None
    feat_df["FEATURE_ID"] = feat_df["FEATURE_ID"].astype(int)
    feat_df = feat_df.sort_values(by=["FEATURE_ID"]).drop_duplicates(subset=["FEATURE_ID"], keep="first")

    feature_ids = tuple(sorted({int(v) for v in feat_df["FEATURE_ID"].tolist()}))
    dates_df, links_df = _load_timeline_overlay(feature_ids, data_version=data_version)
    if dates_df is None or dates_df.empty:
        dates_df = pd.DataFrame(columns=["WORKITEM_ID", "START_DATE", "END_DATE", "ITERATION_PATH"])
    if links_df is None or links_df.empty:
        links_df = pd.DataFrame(columns=["SOURCE_ID", "TARGET_ID", "LINK_CATEGORY", "LINK_TYPE"])

    merged = feat_df.merge(
        dates_df.rename(columns={"WORKITEM_ID": "FEATURE_ID", "ITERATION_PATH": "DATE_ITERATION_PATH"}),
        on="FEATURE_ID",
        how="left",
    )
    def _series_or_blank(col: str) -> pd.Series:
        if col in merged.columns:
            return merged[col]
        return pd.Series([""] * len(merged), index=merged.index)

    merged["START_DATE"] = pd.to_datetime(merged.get("START_DATE"), errors="coerce")
    merged["END_DATE"] = pd.to_datetime(merged.get("END_DATE"), errors="coerce")
    merged["PI_START_DATE"] = pd.to_datetime(merged.get("PI_START_DATE"), errors="coerce")
    merged["PI_END_DATE"] = pd.to_datetime(merged.get("PI_END_DATE"), errors="coerce")
    merged["START_EFFECTIVE"] = merged["START_DATE"].fillna(merged["PI_START_DATE"]).fillna(merged["PI_END_DATE"])
    merged["END_EFFECTIVE"] = merged["END_DATE"].fillna(merged["PI_END_DATE"]).fillna(merged["START_EFFECTIVE"])
    merged["TITLE"] = _series_or_blank("TITLE").fillna("").astype(str)
    merged["TEAMNAME"] = _series_or_blank("TEAMNAME").fillna("").astype(str)
    merged["PROGRAMNAME"] = _series_or_blank("PROGRAMNAME").fillna("").astype(str)
    merged["GROUPNAME"] = _series_or_blank("GROUPNAME").fillna("").astype(str)
    merged["EPIC_ID_STR"] = _series_or_blank("EPIC_ID_STR").fillna("").astype(str).str.strip()
    merged["EPIC_TITLE"] = _series_or_blank("EPIC_TITLE").fillna("").astype(str)
    merged["PI_KEY"] = _series_or_blank("PI_KEY").fillna("").astype(str)
    merged["PI_LABEL"] = _series_or_blank("PI_LABEL").fillna("").astype(str)
    merged["DATE_ITERATION_PATH"] = _series_or_blank("DATE_ITERATION_PATH").fillna("").astype(str)
    merged["FEATURE_ITERATION_PATH"] = _series_or_blank("ITERATION_PATH").fillna("").astype(str)
    merged["FEATURE_PROGRESS_PCT"] = pd.to_numeric(merged.get("FEATURE_PROGRESS_PCT"), errors="coerce").fillna(0.0)
    merged["FEATURE_PROGRESS_TOTAL_SP"] = pd.to_numeric(merged.get("FEATURE_PROGRESS_TOTAL_SP"), errors="coerce").fillna(0.0)
    merged["FEATURE_PROGRESS_INPROGRESS_SP"] = pd.to_numeric(merged.get("FEATURE_PROGRESS_INPROGRESS_SP"), errors="coerce").fillna(0.0)
    merged["FEATURE_PROGRESS_COMPLETED_SP"] = pd.to_numeric(merged.get("FEATURE_PROGRESS_COMPLETED_SP"), errors="coerce").fillna(0.0)
    merged["FEATURE_PROGRESS_PROPOSED_SP"] = pd.to_numeric(merged.get("FEATURE_PROGRESS_PROPOSED_SP"), errors="coerce").fillna(0.0)
    merged["BUSINESS_VALUE"] = pd.to_numeric(merged.get("BUSINESS_VALUE"), errors="coerce")
    iter_paths_scope = sorted(
        {
            str(v).strip()
            for v in pd.concat(
                [merged["DATE_ITERATION_PATH"], merged["FEATURE_ITERATION_PATH"]],
                ignore_index=True,
            ).tolist()
            if str(v).strip()
        }
    )
    pi_keys_scope = sorted({str(v).strip() for v in merged["PI_KEY"].tolist() if str(v).strip()})
    cal_df = _load_iteration_calendar_slice(tuple(iter_paths_scope), tuple(pi_keys_scope), data_version=data_version)
    if not isinstance(cal_df, pd.DataFrame):
        cal_df = pd.DataFrame()

    sprint_df_all = pd.DataFrame()
    path_meta: Dict[str, Dict[str, Any]] = {}
    sprints_by_pi: Dict[str, List[Dict[str, Any]]] = {}
    if not cal_df.empty:
        cal_df = cal_df.copy()
        cal_df["ITERATION_PATH"] = cal_df.get("ITERATION_PATH", "").fillna("").astype(str).str.strip()
        cal_df["ITERATION_LEVEL3"] = cal_df.get("ITERATION_LEVEL3", "").fillna("").astype(str).str.strip()
        cal_df["ITERATION_GRAIN"] = (
            cal_df.get("ITERATION_GRAIN", "PI")
            .fillna("PI")
            .astype(str)
            .str.strip()
            .str.upper()
        )
        cal_df["START_DATE"] = pd.to_datetime(cal_df.get("START_DATE"), errors="coerce")
        cal_df["END_DATE"] = pd.to_datetime(cal_df.get("END_DATE"), errors="coerce")
        cal_df = cal_df.loc[cal_df["ITERATION_PATH"].ne("")].copy()
        cal_df = cal_df.sort_values(by=["START_DATE", "END_DATE", "ITERATION_PATH"], na_position="last")
        for _, r in cal_df.iterrows():
            p = str(r.get("ITERATION_PATH") or "").strip()
            if not p:
                continue
            p_norm = p.lower()
            if p_norm not in path_meta:
                path_meta[p_norm] = {
                    "path": p,
                    "level3": str(r.get("ITERATION_LEVEL3") or "").strip(),
                    "grain": str(r.get("ITERATION_GRAIN") or "PI").strip().upper(),
                    "start": pd.to_datetime(r.get("START_DATE"), errors="coerce"),
                    "end": pd.to_datetime(r.get("END_DATE"), errors="coerce"),
                }
        sprint_df_all = cal_df.loc[cal_df["ITERATION_GRAIN"].eq("SPRINT")].copy()
        if not sprint_df_all.empty:
            sprint_df_all = sprint_df_all.sort_values(by=["START_DATE", "ITERATION_PATH"], na_position="last")
            for _, r in sprint_df_all.iterrows():
                lvl3 = str(r.get("ITERATION_LEVEL3") or "").strip().lower()
                if not lvl3:
                    continue
                sprints_by_pi.setdefault(lvl3, []).append(
                    {
                        "path": str(r.get("ITERATION_PATH") or "").strip(),
                        "start": pd.to_datetime(r.get("START_DATE"), errors="coerce"),
                        "end": pd.to_datetime(r.get("END_DATE"), errors="coerce"),
                    }
                )

    sprint_keys_resolved: List[str] = []
    start_filled: List[pd.Timestamp] = []
    end_filled: List[pd.Timestamp] = []
    explicit_start_flags: List[bool] = []
    explicit_end_flags: List[bool] = []
    sprint_fallback_flags: List[bool] = []
    for _, row in merged.iterrows():
        raw_start_ts = pd.to_datetime(row.get("START_DATE"), errors="coerce")
        raw_end_ts = pd.to_datetime(row.get("END_DATE"), errors="coerce")
        had_explicit_start = pd.notna(raw_start_ts)
        had_explicit_end = pd.notna(raw_end_ts)
        start_ts = raw_start_ts
        end_ts = raw_end_ts
        date_path = str(row.get("DATE_ITERATION_PATH") or row.get("FEATURE_ITERATION_PATH") or "").strip()
        date_path_norm = date_path.lower()
        pi_norm = str(row.get("PI_KEY") or "").strip().lower()
        sprint_key = ""
        used_sprint_fallback = False

        meta = path_meta.get(date_path_norm)
        if meta is not None:
            if pd.isna(start_ts) and pd.notna(meta.get("start")):
                start_ts = pd.to_datetime(meta.get("start"), errors="coerce")
                if str(meta.get("grain") or "").upper() == "SPRINT":
                    used_sprint_fallback = True
            if pd.isna(end_ts) and pd.notna(meta.get("end")):
                end_ts = pd.to_datetime(meta.get("end"), errors="coerce")
                if str(meta.get("grain") or "").upper() == "SPRINT":
                    used_sprint_fallback = True
            if str(meta.get("grain") or "").upper() == "SPRINT":
                sprint_key = str(meta.get("path") or "").strip()

        pi_sprints = sprints_by_pi.get(pi_norm, [])
        if pi_sprints:
            first_start = pi_sprints[0].get("start")
            last_end = pi_sprints[-1].get("end")
            if pd.isna(start_ts) and pd.notna(first_start):
                start_ts = pd.to_datetime(first_start, errors="coerce")
                used_sprint_fallback = True
            if pd.isna(end_ts) and pd.notna(last_end):
                end_ts = pd.to_datetime(last_end, errors="coerce")
                used_sprint_fallback = True
            if not sprint_key and pd.notna(start_ts):
                for s in pi_sprints:
                    s_start = pd.to_datetime(s.get("start"), errors="coerce")
                    s_end = pd.to_datetime(s.get("end"), errors="coerce")
                    if pd.notna(s_start) and pd.notna(s_end) and s_start <= start_ts <= s_end:
                        sprint_key = str(s.get("path") or "").strip()
                        break

        if pd.isna(end_ts) and pd.notna(start_ts):
            end_ts = start_ts
        if pd.isna(start_ts) and pd.notna(end_ts):
            start_ts = end_ts

        sprint_keys_resolved.append(sprint_key)
        start_filled.append(start_ts)
        end_filled.append(end_ts)
        explicit_start_flags.append(bool(had_explicit_start))
        explicit_end_flags.append(bool(had_explicit_end))
        sprint_fallback_flags.append(bool(used_sprint_fallback))

    merged["START_DATE_FILLED"] = start_filled
    merged["END_DATE_FILLED"] = end_filled
    merged["SPRINT_KEY_RESOLVED"] = sprint_keys_resolved
    merged["HAS_EXPLICIT_START"] = explicit_start_flags
    merged["HAS_EXPLICIT_END"] = explicit_end_flags
    merged["USED_SPRINT_FALLBACK"] = sprint_fallback_flags
    merged["START_EFFECTIVE"] = merged["START_DATE_FILLED"].fillna(merged["PI_START_DATE"]).fillna(merged["PI_END_DATE"])
    merged["END_EFFECTIVE"] = merged["END_DATE_FILLED"].fillna(merged["PI_END_DATE"]).fillna(merged["START_EFFECTIVE"])

    merged = merged.sort_values(by=["START_EFFECTIVE", "END_EFFECTIVE", "FEATURE_ID"], ascending=[True, True, True])

    def _iso_date(v: Any) -> Optional[str]:
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date().isoformat()

    def _iteration_leaf(path: Any) -> str:
        s = str(path or "").strip()
        if not s:
            return ""
        if "\\" in s:
            return s.split("\\")[-1].strip()
        if "/" in s:
            return s.split("/")[-1].strip()
        return s

    features_payload: List[Dict[str, Any]] = []
    feature_set: set[int] = set()
    for _, row in merged.iterrows():
        fid = int(row.get("FEATURE_ID"))
        feature_set.add(fid)
        epic_id_row = str(row.get("EPIC_ID_STR") or "").strip()
        epic_title_row = str(row.get("EPIC_TITLE") or "").strip()
        sprint_key = str(row.get("SPRINT_KEY_RESOLVED") or "").strip()
        has_explicit_start = bool(row.get("HAS_EXPLICIT_START"))
        has_explicit_end = bool(row.get("HAS_EXPLICIT_END"))
        used_sprint_fallback = bool(row.get("USED_SPRINT_FALLBACK"))
        has_any_explicit = has_explicit_start or has_explicit_end
        pi_start_ts = pd.to_datetime(row.get("PI_START_DATE"), errors="coerce")
        pi_end_ts = pd.to_datetime(row.get("PI_END_DATE"), errors="coerce")
        used_pi_fallback = bool(
            (not has_explicit_start or not has_explicit_end)
            and not used_sprint_fallback
            and (pd.notna(pi_start_ts) or pd.notna(pi_end_ts))
        )
        if has_explicit_start and has_explicit_end and not used_sprint_fallback:
            date_source = "Explicit dates"
        elif has_any_explicit and (used_sprint_fallback or used_pi_fallback):
            date_source = "Mixed (explicit + fallback)"
        elif used_sprint_fallback:
            date_source = "Sprint calendar fallback"
        elif used_pi_fallback:
            date_source = "PI calendar fallback"
        elif has_any_explicit:
            date_source = "Explicit dates (partial)"
        else:
            date_source = "Unknown"
        features_payload.append(
            {
                "id": str(fid),
                "title": str(row.get("TITLE") or ""),
                "epicId": epic_id_row,
                "epicTitle": epic_title_row or "(Epic)",
                "state": str(row.get("STATE") or ""),
                "start": _iso_date(row.get("START_EFFECTIVE")),
                "end": _iso_date(row.get("END_EFFECTIVE")),
                "progress": float(row.get("FEATURE_PROGRESS_PCT") or 0.0),
                "points": float(row.get("POINTS") or 0.0),
                "fte": float(row.get("DERIVED_FTE") or 0.0),
                "team": str(row.get("TEAMNAME") or ""),
                "program": str(row.get("PROGRAMNAME") or ""),
                "application": str(row.get("GROUPNAME") or ""),
                "piKey": str(row.get("PI_KEY") or ""),
                "sprintKey": sprint_key,
                "dateSource": date_source,
                "businessValue": float(row.get("BUSINESS_VALUE")) if pd.notna(row.get("BUSINESS_VALUE")) else None,
                "featureUrl": str(row.get("FEATURE_URL") or ""),
                "epicUrl": str(row.get("EPIC_URL") or ""),
            }
        )

    if not features_payload:
        return None

    pi_bands: List[Dict[str, Any]] = []
    if pi_catalog:
        for row in pi_catalog:
            pi_key = str((row or {}).get("piKey") or "").strip()
            if not pi_key:
                continue
            pi_bands.append(
                {
                    "piKey": pi_key,
                    "label": str((row or {}).get("label") or pi_key),
                    "start": _iso_date((row or {}).get("start")),
                    "end": _iso_date((row or {}).get("end")),
                }
            )
    if not pi_bands and "PI_KEY" in merged.columns:
        pi_df = (
            merged[["PI_KEY", "PI_LABEL", "PI_START_DATE", "PI_END_DATE"]]
            .drop_duplicates(subset=["PI_KEY"])
            .copy()
        )
        pi_df["PI_START_DATE"] = pd.to_datetime(pi_df.get("PI_START_DATE"), errors="coerce")
        pi_df["PI_END_DATE"] = pd.to_datetime(pi_df.get("PI_END_DATE"), errors="coerce")
        pi_df = pi_df.sort_values(by=["PI_START_DATE", "PI_KEY"], na_position="last")
        for _, row in pi_df.iterrows():
            pi_key = str(row.get("PI_KEY") or "").strip()
            if not pi_key:
                continue
            pi_bands.append(
                {
                    "piKey": pi_key,
                    "label": str(row.get("PI_LABEL") or pi_key),
                    "start": _iso_date(row.get("PI_START_DATE")),
                    "end": _iso_date(row.get("PI_END_DATE")),
                }
            )

    sprint_bands: List[Dict[str, Any]] = []
    timeline_start = pd.to_datetime(merged.get("START_EFFECTIVE"), errors="coerce").min()
    timeline_end = pd.to_datetime(merged.get("END_EFFECTIVE"), errors="coerce").max()
    sprint_df = sprint_df_all.copy() if isinstance(sprint_df_all, pd.DataFrame) else pd.DataFrame()
    if isinstance(sprint_df, pd.DataFrame) and not sprint_df.empty:
        sprint_df["ITERATION_PATH"] = sprint_df.get("ITERATION_PATH", "").fillna("").astype(str).str.strip()
        sprint_df["ITERATION_LEVEL3"] = sprint_df.get("ITERATION_LEVEL3", "").fillna("").astype(str).str.strip()
        sprint_df["START_DATE"] = pd.to_datetime(sprint_df.get("START_DATE"), errors="coerce")
        sprint_df["END_DATE"] = pd.to_datetime(sprint_df.get("END_DATE"), errors="coerce")
        sprint_df = sprint_df.loc[sprint_df["ITERATION_PATH"].ne("")].copy()
        if pd.notna(timeline_start) and pd.notna(timeline_end):
            sprint_df = sprint_df.loc[
                (sprint_df["END_DATE"].isna() | (sprint_df["END_DATE"] >= timeline_start))
                & (sprint_df["START_DATE"].isna() | (sprint_df["START_DATE"] <= timeline_end))
            ].copy()
        sprint_df = sprint_df.drop_duplicates(subset=["ITERATION_PATH"], keep="first")
        for _, row in sprint_df.iterrows():
            path = str(row.get("ITERATION_PATH") or "").strip()
            if not path:
                continue
            leaf = _iteration_leaf(path)
            lvl3 = str(row.get("ITERATION_LEVEL3") or "").strip()
            sprint_bands.append(
                {
                    "sprintKey": path,
                    "label": leaf or lvl3 or path,
                    "start": _iso_date(row.get("START_DATE")),
                    "end": _iso_date(row.get("END_DATE")),
                }
            )

    links_payload: List[Dict[str, Any]] = []
    if not links_df.empty and feature_set:
        deps = links_df.copy()
        deps["SOURCE_ID"] = pd.to_numeric(deps.get("SOURCE_ID"), errors="coerce").astype("Int64")
        deps["TARGET_ID"] = pd.to_numeric(deps.get("TARGET_ID"), errors="coerce").astype("Int64")
        deps["LINK_CATEGORY"] = deps.get("LINK_CATEGORY", "").fillna("").astype(str)
        deps["LINK_TYPE"] = deps.get("LINK_TYPE", "").fillna("").astype(str)
        deps = deps[
            deps["LINK_CATEGORY"].str.upper().str.contains("DEPENDENCY")
            | deps["LINK_TYPE"].str.upper().str.contains("PREDECESSOR|SUCCESSOR|DEPENDENCY")
        ].copy()
        for _, row in deps.iterrows():
            sid = int(row["SOURCE_ID"]) if pd.notna(row["SOURCE_ID"]) else None
            tid = int(row["TARGET_ID"]) if pd.notna(row["TARGET_ID"]) else None
            if sid is None or tid is None:
                continue
            if sid not in feature_set or tid not in feature_set:
                continue
            links_payload.append(
                {
                    "sourceId": str(sid),
                    "targetId": str(tid),
                    "type": str(row.get("LINK_TYPE") or row.get("LINK_CATEGORY") or "Dependency"),
                }
            )

    milestone_payload: List[Dict[str, Any]] = []
    ms_df = milestones_df.copy() if isinstance(milestones_df, pd.DataFrame) else pd.DataFrame()
    if not ms_df.empty:
        ms_df["MILESTONE_ID"] = ms_df.get("MILESTONE_ID", "").fillna("").astype(str).str.strip()
        ms_df["TITLE"] = ms_df.get("TITLE", "").fillna("").astype(str).str.strip()
        ms_df["TARGET_DATE"] = pd.to_datetime(ms_df.get("TARGET_DATE"), errors="coerce")
        ms_df["EPIC_ID"] = pd.to_numeric(ms_df.get("EPIC_ID"), errors="coerce")
        ms_df["FEATURE_ID"] = pd.to_numeric(ms_df.get("FEATURE_ID"), errors="coerce")
        ms_df["TAG"] = ms_df.get("TAG", "").fillna("").astype(str).str.strip()
        ms_df["SOURCE_TYPE"] = ms_df.get("SOURCE_TYPE", "").fillna("").astype(str).str.strip().str.upper()
        ms_df = ms_df.loc[
            ms_df["MILESTONE_ID"].ne("")
            & ms_df["TITLE"].ne("")
            & ms_df["TARGET_DATE"].notna()
        ].copy()
        feature_to_epic: Dict[str, str] = (
            merged[["FEATURE_ID", "EPIC_ID_STR"]]
            .dropna(subset=["FEATURE_ID"])
            .assign(FEATURE_ID=lambda d: d["FEATURE_ID"].astype("Int64").astype(str).str.strip())
            .set_index("FEATURE_ID")["EPIC_ID_STR"]
            .astype(str)
            .to_dict()
            if not merged.empty and "FEATURE_ID" in merged.columns and "EPIC_ID_STR" in merged.columns
            else {}
        )
        if not ms_df.empty and not multi_epic_mode:
            ms_df["EPIC_ID_STR"] = ms_df["EPIC_ID"].astype("Int64").astype(str).str.strip()
            ms_df["FEATURE_ID_STR"] = ms_df["FEATURE_ID"].astype("Int64").astype(str).str.strip()
            ms_df = ms_df.loc[
                ms_df["EPIC_ID_STR"].eq("")
                | ms_df["EPIC_ID_STR"].str.lower().isin({"<na>", "nan", "none"})
                | ms_df["EPIC_ID_STR"].eq(str(focus_epic_id))
                | ms_df["FEATURE_ID_STR"].map(lambda f: str(feature_to_epic.get(str(f).strip(), "")).strip()).eq(str(focus_epic_id))
            ].copy()
        for _, row in ms_df.sort_values(by=["TARGET_DATE", "TITLE"], ascending=[True, True]).iterrows():
            ms_epic_id = ""
            ms_feature_id = ""
            try:
                if pd.notna(row.get("EPIC_ID")):
                    ms_epic_id = str(int(float(row.get("EPIC_ID"))))
            except Exception:
                ms_epic_id = ""
            try:
                if pd.notna(row.get("FEATURE_ID")):
                    ms_feature_id = str(int(float(row.get("FEATURE_ID"))))
            except Exception:
                ms_feature_id = ""
            ms_tag = str(row.get("TAG") or "").strip()
            milestone_payload.append(
                {
                    "id": str(row.get("MILESTONE_ID") or "").strip(),
                    "label": str(row.get("TITLE") or "").strip(),
                    "date": _iso_date(row.get("TARGET_DATE")),
                    "type": "custom",
                    "epicId": ms_epic_id or None,
                    "featureId": ms_feature_id or None,
                    "tag": ms_tag or None,
                    "sourceType": str(row.get("SOURCE_TYPE") or "").strip().upper() or None,
                }
            )

    epics_payload: List[Dict[str, Any]] = []
    try:
        e_df = merged[
            ["EPIC_ID_STR", "EPIC_TITLE", "EPIC_PROGRESS_PCT", "BUSINESS_VALUE", "EPIC_URL", "FEATURE_ID"]
        ].copy()
        e_df = e_df.loc[e_df["EPIC_ID_STR"].astype(str).str.strip().ne("")].copy()
        e_df["BUSINESS_VALUE"] = pd.to_numeric(e_df.get("BUSINESS_VALUE"), errors="coerce").fillna(0.0)
        e_agg = (
            e_df.groupby(["EPIC_ID_STR", "EPIC_TITLE"], dropna=False)
            .agg(
                EPIC_PROGRESS_PCT=("EPIC_PROGRESS_PCT", "max"),
                BUSINESS_VALUE_SUM=("BUSINESS_VALUE", "sum"),
                FEATURE_COUNT=("FEATURE_ID", "nunique"),
                EPIC_URL=("EPIC_URL", lambda s: next((str(v).strip() for v in s.tolist() if str(v).strip()), "")),
            )
            .reset_index()
            .sort_values(by=["EPIC_TITLE", "EPIC_ID_STR"], na_position="last")
        )
        for _, r in e_agg.iterrows():
            epics_payload.append(
                {
                    "id": str(r.get("EPIC_ID_STR") or "").strip(),
                    "title": str(r.get("EPIC_TITLE") or "").strip() or "(Epic)",
                    "progress": float(r.get("EPIC_PROGRESS_PCT") or 0.0),
                    "businessValueSum": float(r.get("BUSINESS_VALUE_SUM") or 0.0),
                    "featureCount": int(r.get("FEATURE_COUNT") or 0),
                    "epicUrl": str(r.get("EPIC_URL") or "").strip(),
                }
            )
    except Exception:
        epics_payload = []

    epic_progress = 0.0
    try:
        epic_progress = float(
            merged.loc[merged["EPIC_ID_STR"].astype(str) == str(focus_epic_id), "EPIC_PROGRESS_PCT"]
            .dropna()
            .astype(float)
            .iloc[0]
        )
    except Exception:
        epic_progress = float(merged.iloc[0].get("EPIC_PROGRESS_PCT") or 0.0)
    return {
        "epic": {
            "id": focus_epic_id,
            "title": epic_title_val,
            "progress": epic_progress,
        },
        "epics": epics_payload,
        "focusEpicId": focus_epic_id,
        "multiEpic": bool(multi_epic_mode),
        "features": features_payload,
        "piBands": pi_bands,
        "sprintBands": sprint_bands,
        "defaultGranularity": "pi",
        "links": links_payload,
        "milestones": milestone_payload,
    }


def main() -> None:
    data_version = get_data_version()
    scope = read_scope_from_session(fetch_df)
    role = infer_role(scope)
    page_theme = st.session_state.get("page_theme", "light")

    render_page_header(
        "Roadmap",
        "Roadmap delivery view (PI timeline, Epics/Applications, demand vs capacity, and feature readiness)",
        scope_label(scope, role),
        role,
        scope_status_right=touch_last_updated_status("roadmap"),
    )
    is_unassigned_scope = not bool(scope.programs or scope.teams or scope.groups)
    require_manual_filter = role in {"Portfolio Manager", "Unknown"} or is_unassigned_scope
    timeline_milestones_df = pd.DataFrame()

    with st.expander("Filters", expanded=False):
        st.session_state.setdefault("roadmap_filter_programs", [])
        st.session_state.setdefault("roadmap_filter_teams", [])
        st.session_state.setdefault("roadmap_filter_groups", [])
        opts_df = fetch_filter_options(data_version=data_version)
        years, programs, teams, groups = _build_filter_options(opts_df)
        current_year = int(dt.date.today().year)
        years_selected = (current_year - 1, current_year, current_year + 1)
        ado_opts = fetch_ado_team_options(tuple(sorted(set(years_selected))), data_version=data_version)
        if ado_opts is not None and not ado_opts.empty:
            ado_programs = (
                ado_opts.get("PROGRAMNAME")
                .dropna()
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )
            ado_teams = (
                ado_opts.get("TEAMNAME")
                .dropna()
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )
            programs = sorted(set(programs).union(ado_programs))
            teams = sorted(set(teams).union(ado_teams))
        # For portfolio-wide/admin views, always merge master program list so registered programs
        # remain visible even when selected-year cost rows are sparse.
        show_portfolio_program_options = role in {"Portfolio Manager", "Unknown", "ADMIN"} or not bool(getattr(scope, "programs", []))
        if show_portfolio_program_options:
            try:
                p_df = list_programs()
                if isinstance(p_df, pd.DataFrame) and not p_df.empty and "PROGRAMNAME" in p_df.columns:
                    p_master = {str(v).strip() for v in p_df["PROGRAMNAME"].dropna().astype(str).tolist() if str(v).strip()}
                    programs = sorted(set(programs).union(p_master))
            except Exception:
                pass
        scope_programs = {
            str(v).strip()
            for v in (getattr(scope, "programs", []) or [])
            if str(v).strip()
        }
        if scope_programs:
            programs = sorted(set(programs).union(scope_programs))
        if not teams:
            try:
                t_df = list_teams()
                if isinstance(t_df, pd.DataFrame) and not t_df.empty and "TEAMNAME" in t_df.columns:
                    teams = sorted({str(v).strip() for v in t_df["TEAMNAME"].dropna().astype(str).tolist() if str(v).strip()})
            except Exception:
                pass
        if not groups:
            try:
                g_df = list_application_groups()
                if isinstance(g_df, pd.DataFrame) and not g_df.empty and "GROUPNAME" in g_df.columns:
                    groups = sorted({str(v).strip() for v in g_df["GROUPNAME"].dropna().astype(str).tolist() if str(v).strip()})
            except Exception:
                pass
        program_label_map: dict[str, str] = {}
        team_label_map: dict[str, str] = {}
        group_label_map: dict[str, str] = {}
        program_alias_map: Dict[str, set[str]] = {}
        team_alias_map: Dict[str, set[str]] = {}
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
                    vals = [v for v in [disp, raw] if v]
                    if vals:
                        union_vals = set(vals)
                        for v in vals:
                            k = v.strip().lower()
                            if not k:
                                continue
                            program_alias_map.setdefault(k, set()).update(union_vals)
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
                    vals = [v for v in [disp, raw] if v]
                    if vals:
                        union_vals = set(vals)
                        for v in vals:
                            k = v.strip().lower()
                            if not k:
                                continue
                            team_alias_map.setdefault(k, set()).update(union_vals)
        except Exception:
            pass
        try:
            g_map_df = list_application_groups()
            if isinstance(g_map_df, pd.DataFrame) and not g_map_df.empty and "GROUPNAME" in g_map_df.columns:
                for _, row in g_map_df.iterrows():
                    disp = str(row.get("GROUPNAME") or "").strip()
                    if disp:
                        group_label_map.setdefault(disp, disp)
        except Exception:
            pass

        def _dedupe_options_by_label(options: list[str], label_map: dict[str, str]) -> list[str]:
            chosen: dict[str, str] = {}
            order: list[str] = []
            for opt in options or []:
                raw_opt = str(opt).strip()
                if not raw_opt:
                    continue
                disp = str(label_map.get(raw_opt, raw_opt)).strip() or raw_opt
                key = disp.upper()
                prev = chosen.get(key)
                if prev is None:
                    chosen[key] = raw_opt
                    order.append(key)
                    continue
                if prev != disp and raw_opt == disp:
                    chosen[key] = raw_opt
            return [chosen[k] for k in order]

        def _remap_selected_by_label(values: list[str], options: list[str], label_map: dict[str, str]) -> list[str]:
            if not values or not options:
                return []
            option_by_disp = {
                str(label_map.get(str(o).strip(), str(o).strip())).strip().upper(): str(o).strip()
                for o in options
                if str(o).strip()
            }
            out: list[str] = []
            seen: set[str] = set()
            for v in values:
                raw_v = str(v).strip()
                if not raw_v:
                    continue
                disp_key = str(label_map.get(raw_v, raw_v)).strip().upper()
                opt = option_by_disp.get(disp_key)
                if opt and opt not in seen:
                    seen.add(opt)
                    out.append(opt)
            return out

        def _expand_selected_aliases(values: list[str], alias_map: Dict[str, set[str]]) -> list[str]:
            expanded: set[str] = set()
            for v in values or []:
                raw_v = str(v).strip()
                if not raw_v:
                    continue
                expanded.add(raw_v)
                expanded.update(alias_map.get(raw_v.lower(), set()))
            return sorted(expanded)

        programs = _dedupe_options_by_label(programs, program_label_map)
        teams = _dedupe_options_by_label(teams, team_label_map)
        if require_manual_filter:
            default_programs = []
            default_teams = []
            default_groups = []
        else:
            default_programs = [p for p in (scope.programs or []) if p in set(programs)]
            default_teams = [t for t in (scope.teams or []) if t in set(teams)]
            default_groups = [g for g in (scope.groups or []) if g in set(groups)]
        if not st.session_state["roadmap_filter_programs"]:
            st.session_state["roadmap_filter_programs"] = default_programs
        if not st.session_state["roadmap_filter_teams"]:
            st.session_state["roadmap_filter_teams"] = default_teams
        if not st.session_state["roadmap_filter_groups"]:
            st.session_state["roadmap_filter_groups"] = default_groups
        st.session_state["roadmap_filter_programs"] = _remap_selected_by_label(
            list(st.session_state.get("roadmap_filter_programs", [])),
            programs,
            program_label_map,
        )
        st.session_state["roadmap_filter_teams"] = _remap_selected_by_label(
            list(st.session_state.get("roadmap_filter_teams", [])),
            teams,
            team_label_map,
        )
        st.session_state["roadmap_filter_groups"] = [
            g for g in st.session_state.get("roadmap_filter_groups", []) if g in groups
        ]

        rel_df = opts_df.copy() if isinstance(opts_df, pd.DataFrame) else pd.DataFrame()
        if rel_df.empty:
            rel_df = fetch_filter_options(data_version=data_version)

        sel_programs = st.multiselect(
            "Programs",
            options=programs,
            key="roadmap_filter_programs",
            format_func=lambda x, _m=program_label_map: _m.get(str(x).strip(), str(x).strip()),
        )
        sel_programs_match = _expand_selected_aliases(sel_programs, program_alias_map)
        sel_programs_match_l = {str(v).strip().lower() for v in sel_programs_match if str(v).strip()}

        team_options: list[str] = []
        team_disabled = not bool(sel_programs)
        if not team_disabled:
            if not rel_df.empty and "TEAMNAME" in rel_df.columns:
                team_df = rel_df.copy()
                if "PROGRAMNAME" in team_df.columns:
                    team_df = team_df.loc[
                        team_df["PROGRAMNAME"].fillna("").astype(str).str.strip().str.lower().isin(sel_programs_match_l)
                    ].copy()
                team_options = sorted({str(v).strip() for v in team_df.get("TEAMNAME", pd.Series(dtype=str)).dropna().astype(str).tolist() if str(v).strip()})
            if not team_options:
                try:
                    t_df = list_teams()
                    if isinstance(t_df, pd.DataFrame) and not t_df.empty and "TEAMNAME" in t_df.columns:
                        if sel_programs and "PROGRAMNAME" in t_df.columns:
                            t_df = t_df.loc[
                                t_df["PROGRAMNAME"].fillna("").astype(str).str.strip().str.lower().isin(sel_programs_match_l)
                            ].copy()
                        team_options = sorted({str(v).strip() for v in t_df["TEAMNAME"].dropna().astype(str).tolist() if str(v).strip()})
                except Exception:
                    team_options = []
        team_options = _dedupe_options_by_label(team_options, team_label_map)
        st.session_state["roadmap_filter_teams"] = _remap_selected_by_label(
            list(st.session_state.get("roadmap_filter_teams", [])),
            team_options,
            team_label_map,
        )
        sel_teams = st.multiselect(
            "Teams",
            options=team_options,
            key="roadmap_filter_teams",
            disabled=team_disabled,
            help="Select Program(s) first." if team_disabled else None,
            format_func=lambda x, _m=team_label_map: _m.get(str(x).strip(), str(x).strip()),
        )
        sel_teams_match = _expand_selected_aliases(sel_teams, team_alias_map)
        sel_teams_match_l = {str(v).strip().lower() for v in sel_teams_match if str(v).strip()}

        group_options: list[str] = []
        group_disabled = not bool(sel_teams)
        if not group_disabled:
            if not rel_df.empty and "GROUPNAME" in rel_df.columns:
                group_df = rel_df.copy()
                if "PROGRAMNAME" in group_df.columns and sel_programs:
                    group_df = group_df.loc[
                        group_df["PROGRAMNAME"].fillna("").astype(str).str.strip().str.lower().isin(sel_programs_match_l)
                    ].copy()
                if "TEAMNAME" in group_df.columns:
                    group_df = group_df.loc[
                        group_df["TEAMNAME"].fillna("").astype(str).str.strip().str.lower().isin(sel_teams_match_l)
                    ].copy()
                group_options = sorted({str(v).strip() for v in group_df.get("GROUPNAME", pd.Series(dtype=str)).dropna().astype(str).tolist() if str(v).strip()})
            if not group_options:
                try:
                    g_df = list_application_groups()
                    if isinstance(g_df, pd.DataFrame) and not g_df.empty and "GROUPNAME" in g_df.columns:
                        if sel_teams and "TEAMNAME" in g_df.columns:
                            g_df = g_df.loc[
                                g_df["TEAMNAME"].fillna("").astype(str).str.strip().str.lower().isin(sel_teams_match_l)
                            ].copy()
                        group_options = sorted({str(v).strip() for v in g_df["GROUPNAME"].dropna().astype(str).tolist() if str(v).strip()})
                except Exception:
                    group_options = []
        st.session_state["roadmap_filter_groups"] = [g for g in st.session_state.get("roadmap_filter_groups", []) if g in set(group_options)]
        sel_groups = st.multiselect(
            "Applications (App Groups)",
            options=group_options,
            key="roadmap_filter_groups",
            disabled=group_disabled,
            help="Select Team(s) first." if group_disabled else None,
        )
        render_active_filters_summary(
            "Active filters",
            [
                ("Programs", sel_programs),
                ("Teams", sel_teams),
                ("Applications", sel_groups),
            ],
        )
        # Keep backend payload canonical in Epics mode.
        # Applications/Epics switching is handled client-side in React for smooth UX.
        group_mode = "Epics"
        st.session_state["roadmap_rows_by"] = group_mode
        show_min_filter = False
        min_features = 1
        max_rows = int(st.session_state.get("roadmap_max_rows", 50))
        st.caption("Teams include ADO-only teams (may have 0 capacity/FTE).")
    scope_programs = list(sel_programs_match or [])
    scope_teams = list(sel_teams_match or [])
    # Keep exact user-picked scope for writes. Alias-expanded lists are for reads/filter matching.
    selected_scope_programs = [str(v).strip() for v in (sel_programs or []) if str(v).strip()]
    selected_scope_teams = [str(v).strip() for v in (sel_teams or []) if str(v).strip()]
    try:
        timeline_milestones_df = list_roadmap_milestones(
            programs=scope_programs or None,
            teams=scope_teams or None,
            include_inactive=False,
        )
    except Exception as e:
        timeline_milestones_df = pd.DataFrame()
        st.warning(f"Timeline markers unavailable: {e}")
    mapping_status = "Mapped only" if group_mode == "Applications" else "All"
    window_start_state = st.session_state.get("roadmap_window_start")
    if not isinstance(window_start_state, int) or window_start_state < 0:
        window_start_state = None
    window_size = 4

    try:
        sidebar_bg = st.get_option("theme.sidebarBackgroundColor")
    except Exception:
        sidebar_bg = None

    try:
        sync_df = fetch_df("SELECT MAX(LAST_SYNC_AT) AS LAST_SYNC_AT FROM ADO_PROFILES WHERE IS_ACTIVE=1")
        cache_bust = str(sync_df.iloc[0].get("LAST_SYNC_AT") or "") if sync_df is not None and not sync_df.empty else ""
    except Exception:
        cache_bust = ""
    signature = json.dumps(
        {
            "years": list(years_selected),
            "programs": sel_programs_match,
            "teams": sel_teams_match,
            "groups": sel_groups,
        },
        sort_keys=True,
    )
    load_signature = json.dumps(
        {
            "signature": signature,
            "cache_bust": cache_bust,
            "rows_by": group_mode,
        },
        sort_keys=True,
    )
    show_loader = str(st.session_state.get("_roadmap_load_signature") or "") != load_signature
    global_loading_ph = st.empty()

    def _set_loading(message: str, step: int, total_steps: int = 5) -> None:
        if not show_loader:
            return
        with global_loading_ph.container():
            step_idx = max(1, min(int(step), int(total_steps)))
            st.markdown(
                f"<div style='font-size:0.8rem; color:color-mix(in srgb, var(--tco-text) 70%, transparent); margin-bottom:0.2rem;'>Step {step_idx} of {int(total_steps)}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:0.9rem; color:var(--tco-text); margin-bottom:0.32rem;'>{message}</div>",
                unsafe_allow_html=True,
            )
            st.progress(float(step_idx) / float(total_steps))
    if require_manual_filter and (not sel_programs and not sel_teams and not sel_groups):
        st.info("Select at least one filter (Program, Team, or Application) to load Roadmap features.")
        if is_debug_enabled(label="Debug"):
            with st.expander("Debug - diagnostics"):
                st.json({"years": years_selected, "programs": sel_programs, "teams": sel_teams, "groups": sel_groups})
        return
    _set_loading("Loading roadmap filters...", 1)
    loader_cm = page_loader(
        messages=[
            "Loading scope and filters...",
            "Loading roadmap features...",
            "Preparing roadmap matrix...",
            "Finalizing view...",
        ],
        show_status=False,
    ) if show_loader else _noop_page_loader()
    with loader_cm as loader:
        loader.step("Loading scope and filters...")
        _set_loading("Resolving roadmap scope...", 2)
        loader.step("Loading roadmap features...")
        _set_loading("Loading roadmap features...", 3)
        df = _load_features_cached(signature, cache_bust, years_selected, sel_programs_match, sel_teams_match, sel_groups, data_version)
        if df is None or df.empty:
            global_loading_ph.empty()
            st.info("No features found for the selected filters.")
            if is_debug_enabled(label="Debug"):
                with st.expander("Debug - diagnostics"):
                    st.json({"years": years_selected, "programs": sel_programs, "teams": sel_teams, "groups": sel_groups})
            return
        loader.step("Preparing roadmap matrix...")
        _set_loading("Preparing roadmap matrix...", 4)
        df = df.copy()
        profile_cfg = normalize_profile_config(get_active_ado_profile())
        org, project = _parse_org_project_from_odata_base(str(profile_cfg.get("odata_base_url") or ""))
        if not (org and project):
            try:
                _ado_cfg = getattr(st, "secrets", {}).get("ado", {})  # type: ignore
            except Exception:
                _ado_cfg = {}
            org = org or str(st.session_state.get("ado_org") or _ado_cfg.get("org") or "").strip()
            project = project or str(st.session_state.get("ado_proj") or _ado_cfg.get("project") or "").strip()
        if org and project:
            feat_series = df.get("FEATURE_URL")
            feat_missing = (
                feat_series is None
                or feat_series.fillna("").astype(str).str.strip().eq("").all()
            )
            if feat_missing:
                df["FEATURE_URL"] = df["FEATURE_ID"].map(lambda v: _ado_workitem_url(org, project, v))
            else:
                df["FEATURE_URL"] = feat_series.fillna("").astype(str)
            epic_series = df.get("EPIC_URL")
            epic_missing = (
                epic_series is None
                or epic_series.fillna("").astype(str).str.strip().eq("").all()
            )
            if epic_missing:
                if "EPIC_ID" in df.columns:
                    df["EPIC_URL"] = df["EPIC_ID"].map(lambda v: _ado_workitem_url(org, project, v))
                else:
                    df["EPIC_URL"] = ""
            else:
                df["EPIC_URL"] = epic_series.fillna("").astype(str)
        if st.session_state.get("ado_features_used_fallback"):
            if is_debug_enabled(label="Debug"):
                with st.expander("Debug", expanded=False):
                    st.info("Using fallback because VW_ADO_FEATURES_ENRICHED is unavailable.")
        df["BUSINESS_VALUE"] = pd.to_numeric(df.get("BUSINESS_VALUE"), errors="coerce")
        df = _apply_display_name_maps(
            df,
            program_label_map=program_label_map,
            team_label_map=team_label_map,
            group_label_map=group_label_map,
        )
        df_base = df.copy()
        group_by_epic = group_mode == "Epics"
        epic_ok = False
        if "EPIC_TITLE" in df_base.columns:
            epic_ok = df_base["EPIC_TITLE"].notna().any() and df_base["EPIC_TITLE"].astype(str).str.strip().ne("").any()
        if group_by_epic and not epic_ok:
            st.info("No EPIC_TITLE available for this scope. Falling back to Applications view.")
            group_mode = "Applications"
            group_by_epic = False
            st.session_state["roadmap_rows_by"] = group_mode
        if group_mode == "Applications":
            df_mapped = _mapping_filter(df, mapping_status)
            if (df_mapped is None or df_mapped.empty) and df is not None and not df.empty:
                # Keep the board usable if mapping tables are temporarily empty/stale.
                df = _mapping_filter(df, "All")
                st.info("No mapped applications found for current data. Showing all application rows (including unmapped).")
            else:
                df = df_mapped
        df, has_points = _normalize_features(df, rows_by=group_mode)
        group_col = "ROW_KEY"
        df["DERIVED_FTE"] = _compute_derived_fte(df, has_points)
        df["COUNT_METRIC"] = 1.0
        points_label = "Story Points" if has_points else "Feature Count"
        if df.empty:
            global_loading_ph.empty()
            st.info("No features match the selected mapping status.")
            return
        loader.step("Finalizing view...")
        _set_loading("Finalizing roadmap view...", 5)
    global_loading_ph.empty()
    st.session_state["_roadmap_load_signature"] = load_signature


    pi_order = _build_pi_order(df)
    x_in_data = sorted(set(df["PI_KEY"].dropna().astype(str).tolist()))

    pi_dates, ordered_pi_keys, pi_label_map = _build_pi_calendar_map(years_selected, pi_order, data_version)
    ordered_pis = [k for k in ordered_pi_keys if k]
    if not ordered_pis:
        ordered_pis = [k for k in pi_order if k]
    if not ordered_pis:
        st.info("No PI labels available for the selected filters.")
        return

    if "PI_LABEL_RAW" in df.columns:
        feature_label_map = (
            df[["PI_KEY", "PI_LABEL_RAW"]]
            .dropna()
            .drop_duplicates("PI_KEY")
            .set_index("PI_KEY")["PI_LABEL_RAW"]
            .to_dict()
        )
    else:
        feature_label_map = {}

    df = df.copy()
    df["PI_START_DATE"] = df["PI_KEY"].map(lambda n: pi_dates.get(n, {}).get("start_ts"))
    df["PI_END_DATE"] = df["PI_KEY"].map(lambda n: pi_dates.get(n, {}).get("end_ts"))

    today = pd.Timestamp(dt.date.today())
    current_pi = None
    for pi_key in ordered_pis:
        dates = pi_dates.get(pi_key, {})
        start_ts = dates.get("start_ts")
        end_ts = dates.get("end_ts")
        if start_ts is not None and end_ts is not None:
            if start_ts <= today <= end_ts:
                current_pi = pi_key
                break
    if not current_pi:
        future = [
            pi_key
            for pi_key in ordered_pis
            if pi_dates.get(pi_key, {}).get("start_ts")
            and pi_dates[pi_key]["start_ts"] >= today
        ]
        if future:
            current_pi = future[0]
        else:
            past = [
                pi_key
                for pi_key in ordered_pis
                if pi_dates.get(pi_key, {}).get("end_ts")
                and pi_dates[pi_key]["end_ts"] <= today
            ]
            if past:
                current_pi = past[-1]
            else:
                current_pi = ordered_pis[-1] if ordered_pis else None

    current_pi_index = ordered_pis.index(current_pi) if current_pi in ordered_pis else 0
    window_start_idx = window_start_state if window_start_state is not None else current_pi_index
    window_start_idx = max(0, min(window_start_idx, max(0, len(ordered_pis) - 1)))
    window_end_idx = min(window_start_idx + window_size, len(ordered_pis))
    if window_end_idx < window_start_idx + window_size and len(ordered_pis) > 0:
        if current_pi_index is None:
            window_start_idx = max(0, len(ordered_pis) - window_size)
        else:
            future_ok = (len(ordered_pis) - current_pi_index) >= window_size
            if not future_ok:
                window_start_idx = max(0, len(ordered_pis) - window_size)
        window_end_idx = min(window_start_idx + window_size, len(ordered_pis))
    window_start_pi = ordered_pis[window_start_idx] if ordered_pis else None

    fte_by_pi = (
        df.groupby("PI_KEY")["DERIVED_FTE"]
        .sum()
        .reindex(ordered_pis, fill_value=0.0)
    )
    capacity_by_pi: Dict[str, float] = {}
    cap_team_filters = list(sel_teams_match or sel_teams or [])
    if cap_team_filters:
        overhead_terms = re.compile(r"(teamftee?|team\\s*oh|team\\s*overhead|overhead)", re.IGNORECASE)
        excluded_overhead = [t for t in cap_team_filters if overhead_terms.search(str(t or ""))]
        if excluded_overhead:
            st.info("Team Overhead is not delivery capacity; Roadmap capacity excludes overhead.")
            cap_team_filters = [t for t in cap_team_filters if t not in set(excluded_overhead)]

    years_list = [int(y) for y in years_selected]
    staffing_df = pd.DataFrame()
    if years_list:
        where = ["h.YEAR IN ({})".format(", ".join(["%s"] * len(years_list))), "UPPER(h.CLASS) IN ('TEAM','DELIVERY')"]
        params: list[Any] = years_list[:]
        if sel_programs_match:
            where.append("UPPER(p.PROGRAMNAME) IN ({})".format(", ".join(["%s"] * len(sel_programs_match))))
            params.extend([str(p).strip().upper() for p in sel_programs_match])
        if cap_team_filters:
            where.append("UPPER(t.TEAMNAME) IN ({})".format(", ".join(["%s"] * len(cap_team_filters))))
            params.extend([str(t).strip().upper() for t in cap_team_filters])
        sql = f"""
          SELECT h.YEAR, h.PI, p.PROGRAMNAME, t.TEAMNAME, UPPER(h.CLASS) AS COMPONENT,
                 CAST(COALESCE(h.HEADCOUNT, 0) AS FLOAT) AS FTE
          FROM VW_TEAM_HEADCOUNT_EFFECTIVE h
          LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
          WHERE {" AND ".join(where)}
        """
        staff_df1 = fetch_df(sql, tuple(params))
        where2 = ["h.YEAR IN ({})".format(", ".join(["%s"] * len(years_list))), "UPPER(h.CLASS) IN ('CONTRACTOR_C','CONTRACTOR_CS')"]
        params2: list[Any] = years_list[:]
        if sel_programs_match:
            where2.append("UPPER(p.PROGRAMNAME) IN ({})".format(", ".join(["%s"] * len(sel_programs_match))))
            params2.extend([str(p).strip().upper() for p in sel_programs_match])
        if cap_team_filters:
            where2.append("UPPER(t.TEAMNAME) IN ({})".format(", ".join(["%s"] * len(cap_team_filters))))
            params2.extend([str(t).strip().upper() for t in cap_team_filters])
        sql2 = f"""
          SELECT h.YEAR, h.PI, p.PROGRAMNAME, t.TEAMNAME, UPPER(h.CLASS) AS COMPONENT,
                 CAST(COALESCE(h.HEADCOUNT, 0) AS FLOAT) AS FTE
          FROM VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE h
          LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
          WHERE {" AND ".join(where2)}
        """
        staff_df2 = fetch_df(sql2, tuple(params2))
        frames = [df for df in [staff_df1, staff_df2] if isinstance(df, pd.DataFrame) and not df.empty]
        staffing_df = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
        if not staffing_df.empty:
            staffing_df["YEAR"] = pd.to_numeric(staffing_df.get("YEAR"), errors="coerce").astype("Int64")
            staffing_df["PI"] = pd.to_numeric(staffing_df.get("PI"), errors="coerce").astype("Int64")
            staffing_df["COMPONENT"] = staffing_df.get("COMPONENT", "").fillna("").astype(str).str.strip()
            staffing_df["FTE"] = pd.to_numeric(staffing_df.get("FTE"), errors="coerce").fillna(0.0)
            comp_norm = staffing_df["COMPONENT"].str.upper()
            allowed = comp_norm.isin(["DELIVERY", "CONTRACTOR C", "CONTRACTOR_C", "CONTRACTOR CS", "CONTRACTOR_CS"])
            excluded = comp_norm.str.contains("TEAM") | comp_norm.str.contains("OVERHEAD")
            staffing_df["CAPACITY_FTE_CALC"] = staffing_df["FTE"].where(allowed & ~excluded, 0.0)
            staffing_df["PI_LABEL"] = (
                staffing_df["YEAR"].astype("Int64").astype(str) + " I" + staffing_df["PI"].astype("Int64").astype(str)
            )
            grouped = staffing_df.groupby("PI_LABEL", dropna=False)["CAPACITY_FTE_CALC"].sum()
            capacity_by_pi = {normalize_pi_key(str(k)): float(v or 0.0) for k, v in grouped.items()}
    risk_window_days = 14
    if has_points:
        app_fte_total = df.groupby(group_col)["DERIVED_FTE"].sum().sort_values(ascending=False)
    else:
        app_fte_total = df.groupby(group_col)["FEATURE_ID"].nunique().sort_values(ascending=False)
    group_counts = df.groupby(group_col)["FEATURE_ID"].nunique().to_dict()
    if capacity_by_pi:
        capacity_by_pi = {
            normalize_pi_key(k): v
            for k, v in capacity_by_pi.items()
            if k is not None and str(k).strip() != ""
        }
    epic_state_map: Dict[str, str] = {}
    epic_id_map: Dict[str, str] = {}
    if group_by_epic and "EPIC_STATE" in df.columns:
        state_series = df[["ROW_KEY", "EPIC_STATE"]].dropna()
        if not state_series.empty:
            epic_state_map = (
                state_series.groupby("ROW_KEY")["EPIC_STATE"]
                .first()
                .astype(str)
                .to_dict()
            )
    if group_by_epic and "EPIC_ID" in df.columns:
        id_series = df[["ROW_KEY", "EPIC_ID"]].copy()
        id_series["EPIC_ID"] = pd.to_numeric(id_series.get("EPIC_ID"), errors="coerce").astype("Int64")
        id_series = id_series.dropna(subset=["ROW_KEY", "EPIC_ID"])
        if not id_series.empty:
            epic_id_map = (
                id_series.groupby("ROW_KEY")["EPIC_ID"]
                .first()
                .astype("Int64")
                .astype(str)
                .to_dict()
            )
    y_labels_sorted = [g for g in app_fte_total.index.astype(str).tolist() if g.strip() != "(Unmapped Application)"]
    if show_min_filter:
        y_labels_sorted = [g for g in y_labels_sorted if int(group_counts.get(g, 0)) >= int(min_features)]
    no_epic_label = "(No Epic)"
    if group_by_epic:
        y_labels_sorted = [g for g in y_labels_sorted if g.strip() != no_epic_label]
    app_limit = int(max_rows) if group_by_epic else 50
    y_labels_page = y_labels_sorted[:app_limit]
    if group_by_epic and no_epic_label in app_fte_total.index.astype(str).tolist():
        y_labels_page.append(no_epic_label)
    app_truncated = len(y_labels_sorted) > app_limit
    if not y_labels_page:
        st.info("No application rows to display for the selected filters.")
        return

    df = df[df[group_col].isin(y_labels_page)].copy()

    cell_counts = (
        df.groupby(["PI_KEY", group_col], dropna=False)["FEATURE_ID"]
        .nunique()
        .to_dict()
    )
    all_items_by_cell: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for (pi_key, groupname), g in df.groupby(["PI_KEY", group_col], dropna=False):
        if has_points:
            g_sorted = g.sort_values(by=["DERIVED_FTE", "POINTS", "TITLE"], ascending=[False, False, True])
        else:
            g_sorted = g.sort_values(by=["COUNT_METRIC", "TITLE"], ascending=[False, True])
        all_items: List[Dict[str, Any]] = []
        for _, row in g_sorted.head(300).iterrows():
            bucket = _bucket_feature(row.get("STATE"), row.get("PI_END_DATE"), risk_window_days)
            all_items.append(
                {
                    "id": str(row.get("FEATURE_ID") or ""),
                    "title": str(row.get("TITLE") or ""),
                    "shortTitle": str(row.get("TITLE") or "")[:120],
                    "statusBucket": bucket,
                    "state": str(row.get("STATE") or ""),
                    "points": float(row.get("POINTS") or 0.0),
                    "fte": float(row.get("DERIVED_FTE") or 0.0),
                    "team": str(row.get("TEAMNAME") or ""),
                    "program": str(row.get("PROGRAMNAME") or ""),
                    "application": str(row.get("GROUPNAME") or ""),
                    "mapping_status": str(row.get("MAPPING_STATUS") or ""),
                    "feature_url": str(row.get("FEATURE_URL") or ""),
                    "epic_id": str(row.get("EPIC_ID") or ""),
                    "epic_title": str(row.get("EPIC_TITLE") or ""),
                    "epic_state": str(row.get("EPIC_STATE") or ""),
                    "epic_url": str(row.get("EPIC_URL") or ""),
                    "progress_pct": float(row.get("FEATURE_PROGRESS_PCT") or 0.0),
                    "progress_total_sp": float(row.get("FEATURE_PROGRESS_TOTAL_SP") or 0.0),
                    "progress_proposed_sp": float(row.get("FEATURE_PROGRESS_PROPOSED_SP") or 0.0),
                    "progress_inprogress_sp": float(row.get("FEATURE_PROGRESS_INPROGRESS_SP") or 0.0),
                    "progress_completed_sp": float(row.get("FEATURE_PROGRESS_COMPLETED_SP") or 0.0),
                    "business_value": float(row.get("BUSINESS_VALUE")) if pd.notna(row.get("BUSINESS_VALUE")) else None,
                    "epic_progress_pct": float(row.get("EPIC_PROGRESS_PCT") or 0.0),
                    "epic_feature_count_total": int(row.get("EPIC_FEATURE_COUNT_TOTAL") or 0),
                    "epic_feature_count_with_stories": int(row.get("EPIC_FEATURE_COUNT_WITH_STORIES") or 0),
                    "epic_feature_count_with_sp": int(row.get("EPIC_FEATURE_COUNT_WITH_SP") or 0),
                }
            )
        all_items_by_cell[(str(pi_key), str(groupname))] = all_items

    display_group_map = {}
    for g in y_labels_page:
        if group_by_epic and g.strip() == no_epic_label:
            display_group_map[g] = "⚠ No Epic (Governance)"
        elif (not group_by_epic) and g.strip() == "(Unmapped Application)":
            display_group_map[g] = "⚠ Unmapped Application"
        else:
            display_group_map[g] = g
    cells_payload: Dict[str, Dict[str, Any]] = {}
    for app_name in y_labels_page:
        for pi_key in ordered_pis:
            key = f"{app_name}|||{pi_key}"
            all_items = all_items_by_cell.get((pi_key, app_name), [])
            total_count = int(cell_counts.get((pi_key, app_name), 0))
            remainder = max(total_count - len(all_items), 0)
            cells_payload[key] = {
                "items": all_items,
                "remainder": int(remainder),
            }

    pis_payload = []
    for pi in ordered_pis:
        date_info = pi_dates.get(pi, {})
        cap_val = capacity_by_pi.get(pi)
        start_ts = date_info.get("start_ts")
        end_ts = date_info.get("end_ts")
        start_date = start_ts.date().isoformat() if pd.notna(start_ts) else None
        end_date = end_ts.date().isoformat() if pd.notna(end_ts) else None
        display_label = (
            pi_label_map.get(pi)
            or feature_label_map.get(pi)
            or pi
        )
        pis_payload.append(
            {
                "id": str(pi),
                "label": _pi_display_label(display_label, pi),
                "demand_fte": float(fte_by_pi.get(pi, 0.0)),
                "capacity_fte": float(cap_val) if capacity_by_pi and cap_val is not None else None,
                "start_date": start_date,
                "end_date": end_date,
            }
        )

    if is_debug_enabled(label="Debug"):
        with st.expander("Debug: Roadmap Capacity vs Demand", expanded=False):
            st.caption("Admin-only debug panel for roadmap capacity vs demand.")
            st.write(
                {
                    "years": list(years_selected),
                    "pi_window_size": int(window_size),
                    "programs": list(sel_programs or []),
                    "teams": list(sel_teams or []),
                    "app_groups": list(sel_groups or []),
                    "rows_by": group_mode,
                }
            )

            if staffing_df is None or staffing_df.empty:
                st.info("No staffing rows available for the selected filters.")
            else:
                st.markdown("**Capacity raw (staffing source)**")
                st.dataframe(staffing_df.head(200), use_container_width=True)

            demand_cols = [
                "YEAR",
                "PI_KEY",
                "PI_LABEL_RAW",
                "PROGRAMNAME",
                "TEAMNAME",
                "GROUPNAME",
                "DERIVED_FTE",
                "POINTS",
                "FEATURE_ID",
                "MAPPING_STATUS",
            ]
            demand_show_cols = [c for c in demand_cols if c in df.columns]
            st.markdown("**Demand raw (pre-aggregation)**")
            st.dataframe(df[demand_show_cols].head(200) if demand_show_cols else df.head(200), use_container_width=True)

            plot_df = pd.DataFrame(
                {
                    "PI_KEY": ordered_pis,
                    "DEMAND_FTE": [float(fte_by_pi.get(pi, 0.0)) for pi in ordered_pis],
                    "CAPACITY_FTE": [
                        float(capacity_by_pi.get(pi)) if capacity_by_pi and capacity_by_pi.get(pi) is not None else None
                        for pi in ordered_pis
                    ],
                }
            )
            st.markdown("**Roadmap capacity vs demand (per PI)**")
            st.dataframe(plot_df.head(200), use_container_width=True)

            if staffing_df is not None and not staffing_df.empty:
                comp_long = staffing_df.copy()
                comp_long["FTE"] = pd.to_numeric(comp_long.get("FTE"), errors="coerce").fillna(0.0)
                comp_long["COMPONENT_NORM"] = comp_long["COMPONENT"].astype(str).str.lower()
                comp_long["IN_CAPACITY"] = comp_long["COMPONENT_NORM"].isin(
                    ["delivery", "contractor c", "contractor_c", "contractor cs", "contractor_cs"]
                ) & ~(
                    comp_long["COMPONENT_NORM"].str.contains("team") | comp_long["COMPONENT_NORM"].str.contains("overhead")
                )
                comp_grouped = (
                    comp_long.groupby(["TEAMNAME", "COMPONENT", "IN_CAPACITY"], dropna=False)["FTE"]
                    .sum()
                    .reset_index()
                    .sort_values(["TEAMNAME", "FTE"], ascending=[True, False])
                )
                st.markdown("**Capacity components by team**")
                st.dataframe(comp_grouped.head(200), use_container_width=True)

                included = sorted(set(comp_long.loc[comp_long["IN_CAPACITY"], "COMPONENT"].astype(str).tolist()))
                excluded = sorted(set(comp_long.loc[~comp_long["IN_CAPACITY"], "COMPONENT"].astype(str).tolist()))
                st.write(
                    {
                        "capacity_components_included": included,
                        "capacity_components_excluded": excluded,
                    }
                )

                team_choice = sel_teams[0] if sel_teams else None
                if not team_choice:
                    if "ERNE WEST" in set(comp_long["TEAMNAME"].astype(str).tolist()):
                        team_choice = "ERNE WEST"
                    else:
                        by_team = comp_long.groupby("TEAMNAME")["FTE"].sum().sort_values(ascending=False)
                        team_choice = by_team.index[0] if not by_team.empty else None
                if team_choice:
                    tcomp = comp_long.loc[comp_long["TEAMNAME"].eq(team_choice)].copy()
                    tcomp_bucket = pd.Series("OTHER", index=tcomp.index, dtype=str)
                    tcomp_bucket.loc[tcomp["COMPONENT_NORM"].str.contains("delivery")] = "DELIVERY"
                    tcomp_bucket.loc[
                        tcomp["COMPONENT_NORM"].str.contains("contractor") & tcomp["COMPONENT_NORM"].str.contains("cs")
                    ] = "CONTRACTOR_CS"
                    tcomp_bucket.loc[
                        tcomp["COMPONENT_NORM"].str.contains("contractor") & ~tcomp["COMPONENT_NORM"].str.contains("cs")
                    ] = "CONTRACTOR_C"
                    tcomp_bucket.loc[
                        tcomp["COMPONENT_NORM"].str.contains("team") | tcomp["COMPONENT_NORM"].str.contains("overhead")
                    ] = "TEAM"
                    tcomp = tcomp.assign(_BUCKET=tcomp_bucket)
                    bucket_sum = tcomp.groupby("_BUCKET")["FTE"].sum()
                    team_fte = float(bucket_sum.get("TEAM", 0.0))
                    delivery_fte = float(bucket_sum.get("DELIVERY", 0.0))
                    c_fte = float(bucket_sum.get("CONTRACTOR_C", 0.0))
                    cs_fte = float(bucket_sum.get("CONTRACTOR_CS", 0.0))
                    capacity_fte = delivery_fte + c_fte + cs_fte
                    st.write(
                        f"{team_choice} -> TEAM={team_fte:.2f}, DELIVERY={delivery_fte:.2f}, C={c_fte:.2f}, CS={cs_fte:.2f} => capacity={capacity_fte:.2f}"
                    )
            else:
                st.info("Component breakdown not available in staffing dataset.")

            debug_payload = {
                "filters": {
                    "years": list(years_selected),
                    "programs": list(sel_programs or []),
                    "teams": list(sel_teams or []),
                    "app_groups": list(sel_groups or []),
                },
                "rows": {
                    "capacity_raw": int(staffing_df.shape[0]) if isinstance(staffing_df, pd.DataFrame) else 0,
                    "demand_raw": int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0,
                    "plot_rows": int(plot_df.shape[0]) if isinstance(plot_df, pd.DataFrame) else 0,
                },
            }
            if st.button("Copy debug as JSON", key="debug_roadmap_capacity_json"):
                st.json(debug_payload)
    window_pis = set(ordered_pis[window_start_idx:window_end_idx])
    df_window = df[df["PI_KEY"].isin(window_pis)]
    app_feature_counts = (
        df_window.groupby(group_col)["FEATURE_ID"]
        .nunique()
        .reindex(y_labels_page, fill_value=0)
    )
    app_bv_sum = (
        df_window.groupby(group_col)["BUSINESS_VALUE"]
        .sum(min_count=1)
        .reindex(y_labels_page, fill_value=0.0)
    )
    app_demand_fte_sum = (
        df_window.groupby(group_col)["DERIVED_FTE"]
        .sum(min_count=1)
        .reindex(y_labels_page, fill_value=0.0)
    )
    app_avg_progress = (
        pd.to_numeric(df_window.get("FEATURE_PROGRESS_PCT"), errors="coerce")
        .groupby(df_window[group_col])
        .mean()
        .reindex(y_labels_page, fill_value=0.0)
    )
    y_labels_page = [
        app
        for app in y_labels_page
        if int(app_feature_counts.get(app, 0)) > 0
        and (not show_min_filter or int(app_feature_counts.get(app, 0)) >= int(min_features))
    ]
    if not y_labels_page:
        st.info("No application rows to display for the selected PI window.")
        return
    df = df[df[group_col].isin(y_labels_page)].copy()

    if is_debug_enabled(label="Debug"):
        with st.expander("Roadmap debug", expanded=False):
            feature_pi_keys = (
                sorted(set(df["PI_KEY"].dropna().astype(str).tolist()))
                if df is not None and "PI_KEY" in df.columns
                else []
            )
            cal_pi_keys = sorted(set(pi_dates.keys())) if pi_dates else []
            pi_intersect = len(set(feature_pi_keys).intersection(cal_pi_keys))
            st.write(
                {
                    "rows_by": group_mode,
                    "rows_total": int(len(df_base.index)) if df_base is not None else 0,
                    "epic_ok": bool(epic_ok),
                    "epic_non_null": int(df_base["EPIC_TITLE"].notna().sum()) if df_base is not None and "EPIC_TITLE" in df_base.columns else 0,
                    "distinct_row_key": int(df[group_col].nunique()) if df is not None and group_col in df.columns else 0,
                    "epic_title_col": "EPIC_TITLE" in df_base.columns if df_base is not None else False,
                    "feature_pi_keys": len(feature_pi_keys),
                    "calendar_pi_keys": len(cal_pi_keys),
                    "pi_key_intersect": int(pi_intersect),
                    "window_start_pi": window_start_pi,
                    "window_end_pi": ordered_pis[window_end_idx - 1] if ordered_pis and window_end_idx > 0 else None,
                }
            )
            try:
                sample_cell_key = next(iter(cells_payload.keys())) if cells_payload else ""
                cell_pi_id = sample_cell_key.split("|||")[1] if "|||" in sample_cell_key else ""
                pi_ids = {str(p.get("id")) for p in pis_payload}
                if cell_pi_id and cell_pi_id not in pi_ids:
                    st.warning("PI id mismatch: cells use PI_KEYs not present in pis[].id.")
                    st.write({"sample_cell_pi": cell_pi_id, "sample_pis": list(pi_ids)[:5]})
            except Exception:
                pass
    if len(ordered_pis) <= window_size:
        st.info(
            f"Only one PI window available (total PIs = {len(ordered_pis)}). "
            "Select more years to expand the board timeline."
        )

    cell_counts = (
        df.groupby(["PI_KEY", group_col], dropna=False)["FEATURE_ID"]
        .nunique()
        .to_dict()
    )
    all_items_by_cell = {}
    for (pi_key, groupname), g in df.groupby(["PI_KEY", group_col], dropna=False):
        if has_points:
            g_sorted = g.sort_values(by=["DERIVED_FTE", "POINTS", "TITLE"], ascending=[False, False, True])
        else:
            g_sorted = g.sort_values(by=["COUNT_METRIC", "TITLE"], ascending=[False, True])
        all_items = []
        for _, row in g_sorted.head(300).iterrows():
            bucket = _bucket_feature(row.get("STATE"), row.get("PI_END_DATE"), risk_window_days)
            all_items.append(
                {
                    "id": str(row.get("FEATURE_ID") or ""),
                    "title": str(row.get("TITLE") or ""),
                    "shortTitle": str(row.get("TITLE") or "")[:120],
                    "statusBucket": bucket,
                    "state": str(row.get("STATE") or ""),
                    "points": float(row.get("POINTS") or 0.0),
                    "fte": float(row.get("DERIVED_FTE") or 0.0),
                    "team": str(row.get("TEAMNAME") or ""),
                    "program": str(row.get("PROGRAMNAME") or ""),
                    "application": str(row.get("GROUPNAME") or ""),
                    "mapping_status": str(row.get("MAPPING_STATUS") or ""),
                    "feature_url": str(row.get("FEATURE_URL") or ""),
                    "epic_id": str(row.get("EPIC_ID") or ""),
                    "epic_title": str(row.get("EPIC_TITLE") or ""),
                    "epic_state": str(row.get("EPIC_STATE") or ""),
                    "epic_url": str(row.get("EPIC_URL") or ""),
                    "progress_pct": float(row.get("FEATURE_PROGRESS_PCT") or 0.0),
                    "progress_total_sp": float(row.get("FEATURE_PROGRESS_TOTAL_SP") or 0.0),
                    "progress_proposed_sp": float(row.get("FEATURE_PROGRESS_PROPOSED_SP") or 0.0),
                    "progress_inprogress_sp": float(row.get("FEATURE_PROGRESS_INPROGRESS_SP") or 0.0),
                    "progress_completed_sp": float(row.get("FEATURE_PROGRESS_COMPLETED_SP") or 0.0),
                    "business_value": float(row.get("BUSINESS_VALUE")) if pd.notna(row.get("BUSINESS_VALUE")) else None,
                    "epic_progress_pct": float(row.get("EPIC_PROGRESS_PCT") or 0.0),
                    "epic_feature_count_total": int(row.get("EPIC_FEATURE_COUNT_TOTAL") or 0),
                    "epic_feature_count_with_stories": int(row.get("EPIC_FEATURE_COUNT_WITH_STORIES") or 0),
                    "epic_feature_count_with_sp": int(row.get("EPIC_FEATURE_COUNT_WITH_SP") or 0),
                }
            )
        all_items_by_cell[(str(pi_key), str(groupname))] = all_items

    display_group_map = {}
    for g in y_labels_page:
        if group_by_epic and g.strip() == no_epic_label:
            display_group_map[g] = "⚠ No Epic (Governance)"
        elif (not group_by_epic) and g.strip() == "(Unmapped Application)":
            display_group_map[g] = "⚠ Unmapped Application"
        else:
            display_group_map[g] = g
    cells_payload = {}
    for app_name in y_labels_page:
        for pi_key in ordered_pis:
            key = f"{app_name}|||{pi_key}"
            all_items = all_items_by_cell.get((pi_key, app_name), [])
            total_count = int(cell_counts.get((pi_key, app_name), 0))
            remainder = max(total_count - len(all_items), 0)
            cells_payload[key] = {
                "items": all_items,
                "remainder": int(remainder),
            }
    apps_payload = []
    for app in y_labels_page:
        bv_val = app_bv_sum.get(app, 0.0)
        demand_val = app_demand_fte_sum.get(app, 0.0)
        prog_val = app_avg_progress.get(app, 0.0)
        entry = {
            "id": str(app),
            "label": str(display_group_map.get(app, app)),
            "item_count": int(app_feature_counts.get(app, 0)),
            "bv_sum": float(bv_val) if pd.notna(bv_val) else 0.0,
            "demand_fte_sum": float(demand_val) if pd.notna(demand_val) else 0.0,
            "feature_count": int(app_feature_counts.get(app, 0)),
            "avg_progress": float(prog_val) if pd.notna(prog_val) else 0.0,
        }
        if group_by_epic:
            entry["epic_state"] = epic_state_map.get(str(app), "")
            entry["epic_id"] = epic_id_map.get(str(app), "")
        apps_payload.append(entry)

    unified_ui_enabled = bool(st.session_state.get("roadmap_unified_ui_enabled", True))
    unified_controls_v2 = bool(st.session_state.get("roadmap_unified_controls_v2", True))
    if not bool(st.session_state.get("_roadmap_show_dependencies_defaulted", False)):
        st.session_state["roadmap_ui_show_dependencies"] = False
        existing_ui_state = st.session_state.get("roadmap_ui_state", {})
        if isinstance(existing_ui_state, dict):
            existing_ui_state = {**existing_ui_state, "showDependencies": False}
            st.session_state["roadmap_ui_state"] = existing_ui_state
        st.session_state["_roadmap_show_dependencies_defaulted"] = True
    roadmap_ui_state = st.session_state.get("roadmap_ui_state", {})
    if not isinstance(roadmap_ui_state, dict):
        roadmap_ui_state = {}
    roadmap_ui_state_defaults: Dict[str, Any] = {
        "view": "board",
        "density": str(st.session_state.get("roadmap_ui_density") or "comfortable"),
        "timeframePreset": "fit",
        "granularity": "pi",
        "zoom": float(st.session_state.get("roadmap_ui_zoom") or 1.0),
        "timeWindowStart": "",
        "timeWindowEnd": "",
        "fitSelectionNonce": 0,
        "todayAnchorToken": 0,
        "hoveredFeatureId": "",
        "dependencyFocus": None,
        "showDependencies": bool(st.session_state.get("roadmap_ui_show_dependencies", False)),
        "showProgress": True,
        "dependencyLineMode": "auto",
        "searchQuery": "",
        "drawer": "closed",
        "showRoi": bool(st.session_state.get("roadmap_ui_show_roi", False)),
        "sortBy": str(st.session_state.get("roadmap_ui_sort_by") or "roi_bv"),
        "sortDir": str(st.session_state.get("roadmap_ui_sort_dir") or "desc"),
        "timelineLeftWidth": float(st.session_state.get("roadmap_ui_timeline_left_width") or 260.0),
        "groupBy": "epics" if group_by_epic else "applications",
        "selection": {"kind": "none"},
    }
    merged_ui_state = {**roadmap_ui_state_defaults, **roadmap_ui_state}
    st.session_state["roadmap_ui_state"] = merged_ui_state
    ui_selection = merged_ui_state.get("selection", {})
    if not isinstance(ui_selection, dict):
        ui_selection = {}
    selection_for_payload = st.session_state.get("roadmap_selection", {})
    if not isinstance(selection_for_payload, dict):
        selection_for_payload = {}
    if unified_ui_enabled:
        # Unified UI keeps selection client-side; avoid stale server-side rehydration on page revisit.
        selection_for_payload = {}

    timeline_pi_catalog = [
        {
            "piKey": str(pi_key),
            "label": _pi_display_label(pi_label_map.get(pi_key) or feature_label_map.get(pi_key) or pi_key, pi_key),
            "start": pi_dates.get(pi_key, {}).get("start_ts"),
            "end": pi_dates.get(pi_key, {}).get("end_ts"),
        }
        for pi_key in ordered_pis
    ]

    selection_for_timeline: Dict[str, Any] = selection_for_payload.copy() if isinstance(selection_for_payload, dict) else {}
    if isinstance(ui_selection, dict) and ui_selection:
        sel_kind = str(ui_selection.get("kind") or "").strip().lower()
        if sel_kind == "feature":
            selection_for_timeline = {
                "mode": "feature",
                "feature_id": ui_selection.get("featureId"),
                "epic_id": ui_selection.get("epicId"),
                "groupname": ui_selection.get("appId"),
                "pi_label": ui_selection.get("piId"),
            }
        elif sel_kind == "epic":
            selection_for_timeline = {
                "mode": "epic",
                "epic_id": ui_selection.get("epicId"),
            }

    timeline_payload = _build_epic_timeline_payload(
        df=df,
        selection=selection_for_timeline,
        group_col=group_col,
        group_mode=group_mode,
        no_epic_label=no_epic_label,
        data_version=data_version,
        pi_catalog=timeline_pi_catalog,
        milestones_df=timeline_milestones_df if isinstance(timeline_milestones_df, pd.DataFrame) else None,
    )
    payload = {
        "meta": {
            "window": {"startPiId": window_start_pi, "size": window_size, "totalPis": len(ordered_pis)},
            "row_mode": "epics" if group_by_epic else "apps",
            "rows_by": group_mode,
            "row_label": "Epic" if group_by_epic else "Application",
            "selection": {
                "mode": str(ui_selection.get("kind") or selection_for_payload.get("mode") or ""),
                "appId": str(ui_selection.get("appId") or selection_for_payload.get("groupname") or ""),
                "piId": str(ui_selection.get("piId") or selection_for_payload.get("pi_label") or ""),
                "featureId": str(ui_selection.get("featureId") or selection_for_payload.get("feature_id") or ""),
                "epicId": str(ui_selection.get("epicId") or selection_for_payload.get("epic_id") or ""),
            },
            "labels": {
                "completed": "Completed",
                "inProgress": "In Progress",
                "planned": "Planned",
                "validation": "Validation",
                "atRisk": "At Risk",
            },
            "theme": {"sidebar_bg": sidebar_bg},
            "feature_flags": {
                "unified_controls_v2": unified_controls_v2,
                "roadmap_flag_design_tokens_v2": bool(st.session_state.get("roadmap_flag_design_tokens_v2", True)),
                "roadmap_flag_epic_color_v2": bool(st.session_state.get("roadmap_flag_epic_color_v2", True)),
                "roadmap_flag_selection_sync_v2": bool(st.session_state.get("roadmap_flag_selection_sync_v2", True)),
                "roadmap_flag_dependency_interactive_v2": bool(st.session_state.get("roadmap_flag_dependency_interactive_v2", True)),
                "roadmap_flag_timeframe_controls_v2": bool(st.session_state.get("roadmap_flag_timeframe_controls_v2", True)),
                "roadmap_flag_ui_filters_react_v2": bool(st.session_state.get("roadmap_flag_ui_filters_react_v2", True)),
                "roadmap_flag_selection_autoscroll_v3": bool(st.session_state.get("roadmap_flag_selection_autoscroll_v3", False)),
                "roadmap_flag_dependency_focus_v3": bool(st.session_state.get("roadmap_flag_dependency_focus_v3", False)),
                "roadmap_flag_milestone_cluster_v3": bool(st.session_state.get("roadmap_flag_milestone_cluster_v3", False)),
                "roadmap_flag_epic_color_finalize_v3": bool(st.session_state.get("roadmap_flag_epic_color_finalize_v3", False)),
                "roadmap_flag_time_controls_polish_v3": bool(st.session_state.get("roadmap_flag_time_controls_polish_v3", False)),
                "roadmap_flag_state_reset_on_revisit_v3": bool(st.session_state.get("roadmap_flag_state_reset_on_revisit_v3", False)),
                "roadmap_flag_micro_polish_v3": bool(st.session_state.get("roadmap_flag_micro_polish_v3", False)),
                "roadmap_flag_milestone_feedback_v4": bool(st.session_state.get("roadmap_flag_milestone_feedback_v4", False)),
            },
            "ui_defaults": merged_ui_state,
            "capabilities": {
                "milestoneCreate": True,
                "milestoneDelete": True,
                "configureGroupBy": True,
                "configureSort": unified_controls_v2,
                "showRoi": unified_controls_v2,
            },
            "status": {
                "text": f"{len(df):,d} features loaded • dependencies {'on' if bool(merged_ui_state.get('showDependencies', False)) else 'off'}",
                "milestones": st.session_state.get("_roadmap_milestone_status", {}) if isinstance(st.session_state.get("_roadmap_milestone_status", {}), dict) else {},
            },
        },
        "pis": pis_payload,
        "apps": apps_payload,
        "cells": cells_payload,
        "timeline": timeline_payload,
    }

    st.caption("Capacity excludes Team Overhead (TEAMFTE).")
    try:
        event = roadmap_ui(payload, height=820, key="roadmap_ui")
    except Exception as e:
        st.error("Roadmap UI render error.")
        st.exception(e)
        st.json(
            {
                "payload_meta": payload.get("meta"),
                "pis": len(payload.get("pis", [])),
                "apps": len(payload.get("apps", [])),
                "cells": len(payload.get("cells", {})),
            }
        )
        st.stop()

    diag = roadmap_ui_debug_info()
    # In normal operation the roadmap frontend runs fully client-side and may emit no backend event.
    # Only treat missing dist artifacts as a real load issue.
    if not bool(diag.get("index_exists")):
        st.warning("Roadmap UI assets are missing (frontend dist not found).")
    event_type, event_payload = unpack_event(event)
    routed = route_event(event)
    routed_action = str(routed.get("action") or "")
    routed_payload = routed.get("payload") if isinstance(routed.get("payload"), dict) else {}
    if event and isinstance(event, dict):
        req_id = str(event.get("requestId") or "").strip()
        if req_id:
            if str(st.session_state.get("_roadmap_last_event_id") or "") == req_id:
                event_type = ""
                routed_action = ""
            else:
                st.session_state["_roadmap_last_event_id"] = req_id
    # One-shot replay guard: after a milestone write-triggered rerun, Streamlit can
    # replay the same component value once. Ignore an identical next write event.
    skip_once = st.session_state.get("_roadmap_skip_event_once")
    if isinstance(skip_once, dict):
        try:
            exp_at = float(skip_once.get("exp_at") or 0.0)
        except Exception:
            exp_at = 0.0
        if exp_at <= time.time():
            st.session_state.pop("_roadmap_skip_event_once", None)
        elif event and isinstance(event, dict) and routed_action:
            skip_action = str(skip_once.get("action") or "").strip().lower()
            skip_sig = str(skip_once.get("sig") or "")
            cur_sig = _roadmap_event_signature(routed_action, routed_payload)
            if skip_action == str(routed_action).strip().lower():
                # If sig is empty, skip the next action regardless of payload shape.
                # Some frontend replays mutate payload envelope fields between reruns.
                if (not skip_sig) or (skip_sig == cur_sig):
                    routed_action = ""
                    st.session_state.pop("_roadmap_skip_event_once", None)

    if event and isinstance(event, dict) and event_type == "frontend_error":
        st.error("Roadmap UI (frontend) crashed.")
        st.code(event.get("message", ""), language="text")
        st.code(event.get("stack", ""), language="text")
        st.stop()
    if event and isinstance(event, dict) and event_type == "error":
        st.error("Roadmap UI reported an error.")
        st.code(event.get("message", ""), language="text")
        if event.get("stack"):
            st.code(event.get("stack", ""), language="text")
    if event and isinstance(event, dict) and event_type == "ui_error":
        st.error("Roadmap UI reported an error.")
        st.code(event.get("message", ""), language="text")
        if event.get("stack"):
            st.code(event.get("stack", ""), language="text")
        st.stop()
    if event and isinstance(event, dict) and routed_action == "view_change":
        # Group-by is handled client-side to prevent full-page reruns while toggling Applications/Epics.
        # Keep server payload canonical in Epics mode.
        st.session_state["roadmap_rows_by"] = "Epics"
        patch = routed_payload.get("state_patch") if isinstance(routed_payload.get("state_patch"), dict) else {}
        selection_patch = routed_payload.get("selection_patch") if isinstance(routed_payload.get("selection_patch"), dict) else {}
        ui_state = st.session_state.get("roadmap_ui_state", {})
        if not isinstance(ui_state, dict):
            ui_state = {}
        if patch:
            ui_state = {**ui_state, **patch}
        if selection_patch:
            ui_state["selection"] = selection_patch
        st.session_state["roadmap_ui_state"] = ui_state
        if patch.get("showDependencies") is not None:
            st.session_state["roadmap_ui_show_dependencies"] = bool(patch.get("showDependencies"))
        if patch.get("density"):
            st.session_state["roadmap_ui_density"] = str(patch.get("density"))
        if patch.get("zoom") is not None:
            try:
                st.session_state["roadmap_ui_zoom"] = float(patch.get("zoom"))
            except Exception:
                pass
        if patch.get("showRoi") is not None:
            st.session_state["roadmap_ui_show_roi"] = bool(patch.get("showRoi"))
        if patch.get("sortBy"):
            st.session_state["roadmap_ui_sort_by"] = str(patch.get("sortBy"))
        if patch.get("sortDir"):
            st.session_state["roadmap_ui_sort_dir"] = str(patch.get("sortDir"))
        if patch.get("timelineLeftWidth") is not None:
            try:
                st.session_state["roadmap_ui_timeline_left_width"] = float(patch.get("timelineLeftWidth"))
            except Exception:
                pass
    if event and isinstance(event, dict) and routed_action == "drawer_open":
        pass
    if event and isinstance(event, dict) and routed_action == "window_change":
        start_idx = routed_payload.get("window_start")
        if isinstance(start_idx, int) and start_idx >= 0:
            if st.session_state.get("roadmap_window_start") != start_idx:
                st.session_state["roadmap_window_start"] = start_idx
        else:
            start_pi_id = str(routed_payload.get("startPiId") or "")
            if isinstance(start_pi_id, str) and "ordered_pis" in locals():
                try:
                    idx = ordered_pis.index(start_pi_id)
                    if st.session_state.get("roadmap_window_start") != idx:
                        st.session_state["roadmap_window_start"] = idx
                except ValueError:
                    pass
    if event and isinstance(event, dict) and routed_action == "selection_change":
        next_sel = routed_payload.get("selection")
        if isinstance(next_sel, dict) and st.session_state.get("roadmap_selection") != next_sel:
            st.session_state["roadmap_selection"] = next_sel
            st.session_state["roadmap_markers_open"] = True
    if event and isinstance(event, dict) and routed_action == "milestone_create":
        ms_payload = routed_payload.get("milestone")
        if ms_payload:
            try:
                save_program = selected_scope_programs[0] if len(selected_scope_programs) == 1 else None
                save_team = selected_scope_teams[0] if len(selected_scope_teams) == 1 else None
                raw_payload = routed_payload.get("raw") if isinstance(routed_payload.get("raw"), dict) else {}
                upsert_roadmap_milestone(
                    title=ms_payload["title"],
                    target_date=ms_payload["target_date"],
                    pi_key=str(raw_payload.get("pi_key") or "").strip() or None,
                    epic_id=int(float(raw_payload.get("epic_id"))) if str(raw_payload.get("epic_id") or "").strip() else None,
                    feature_id=int(float(raw_payload.get("feature_id"))) if str(raw_payload.get("feature_id") or "").strip() else None,
                    tag=ms_payload["tag"],
                    source_type="MANUAL",
                    program_name=save_program,
                    team_name=save_team,
                    is_active=True,
                    updated_by=_current_user_email() or None,
                )
                st.session_state["_roadmap_skip_event_once"] = {
                    "action": "milestone_create",
                    "sig": "",
                    "exp_at": time.time() + 3.0,
                }
                post_write_refresh("roadmap_milestone_upsert", rerun=True, bump_version=False)
            except Exception as e:
                st.error(f"Marker save failed: {e}")
    if event and isinstance(event, dict) and routed_action == "milestone_batch_apply":
        creates = routed_payload.get("creates") if isinstance(routed_payload.get("creates"), list) else []
        deletes = routed_payload.get("deletes") if isinstance(routed_payload.get("deletes"), list) else []
        changed = False
        errors: List[str] = []
        created_ok = 0
        deleted_ok = 0
        for item in creates:
            try:
                if not isinstance(item, dict):
                    continue
                ms_payload = item.get("milestone") if isinstance(item.get("milestone"), dict) else None
                if not ms_payload:
                    continue
                save_program = selected_scope_programs[0] if len(selected_scope_programs) == 1 else None
                save_team = selected_scope_teams[0] if len(selected_scope_teams) == 1 else None
                epic_raw = str(item.get("epic_id") or "").strip()
                feat_raw = str(item.get("feature_id") or "").strip()
                upsert_roadmap_milestone(
                    title=ms_payload["title"],
                    target_date=ms_payload["target_date"],
                    pi_key=str(item.get("pi_key") or "").strip() or None,
                    epic_id=int(float(epic_raw)) if epic_raw else None,
                    feature_id=int(float(feat_raw)) if feat_raw else None,
                    tag=ms_payload["tag"],
                    source_type=str(item.get("source_type") or "MANUAL").strip().upper() or "MANUAL",
                    program_name=save_program,
                    team_name=save_team,
                    is_active=True,
                    updated_by=_current_user_email() or None,
                    milestone_id=str(item.get("milestone_id") or "").strip() or None,
                )
                changed = True
                created_ok += 1
            except Exception as e:
                errors.append(f"create failed: {e}")
        for mid in deletes:
            try:
                mid_s = str(mid or "").strip()
                if not mid_s:
                    continue
                delete_roadmap_milestone(mid_s)
                changed = True
                deleted_ok += 1
            except Exception as e:
                errors.append(f"delete failed: {e}")
        st.session_state["_roadmap_milestone_status"] = {
            "created": int(created_ok),
            "deleted": int(deleted_ok),
            "failed": int(len(errors)),
            "message": (
                f"Failed: {len(errors)}"
                if errors
                else f"Saved: {created_ok} created, {deleted_ok} deleted"
            ),
        }
        if changed:
            # Rebuild payload immediately from DB rows after batch writes.
            # Keeping the same run leaves stale milestones in the frontend draft, and
            # subsequent saves can replay creates (new UUID each time) causing duplicates.
            st.session_state["_roadmap_skip_event_once"] = {
                "action": "milestone_batch_apply",
                "sig": "",
                "exp_at": time.time() + 3.0,
            }
            post_write_refresh("roadmap_milestone_batch_apply", rerun=False, bump_version=False)
        if errors:
            st.error("Milestone save had errors: " + "; ".join(errors[:3]))
    if event and isinstance(event, dict) and routed_action == "milestone_delete":
        mid = str(routed_payload.get("milestone_id") or "").strip()
        if mid:
            try:
                delete_roadmap_milestone(mid)
                st.session_state["_roadmap_skip_event_once"] = {
                    "action": "milestone_delete",
                    "sig": "",
                    "exp_at": time.time() + 3.0,
                }
                post_write_refresh("roadmap_milestone_delete", rerun=True, bump_version=False)
            except Exception as e:
                st.error(f"Marker delete failed: {e}")

    # Backward compatibility with old frontend event names.
    if event and isinstance(event, dict) and event_type in {"feature_click", "timeline_epic_click", "cell_click", "more_click"}:
        compat_mode = "feature" if event_type == "feature_click" else ("epic" if event_type == "timeline_epic_click" else "cell")
        compat_payload = {"mode": compat_mode, **event_payload}
        next_sel = selection_from_payload(compat_payload)
        if isinstance(next_sel, dict) and st.session_state.get("roadmap_selection") != next_sel:
            st.session_state["roadmap_selection"] = next_sel

    sel_preview = st.session_state.get("roadmap_selection", {})
    if (not unified_ui_enabled) and isinstance(sel_preview, dict):
        _mode = str(sel_preview.get("mode") or "").strip().lower()
        _fid = str(sel_preview.get("feature_id") or "").strip() if _mode == "feature" else ""
        _eid = str(sel_preview.get("epic_id") or "").strip()
        _ft = str(sel_preview.get("feature_title") or "").strip()
        _et = str(sel_preview.get("epic_title") or "").strip()
        if _fid:
            st.caption(f"Selected for timeline marker: Feature `{_fid}` {('· ' + _ft) if _ft else ''}")
        elif _eid:
            st.caption(f"Selected for timeline marker: Epic `{_eid}` {('· ' + _et) if _et else ''}")

    if not unified_ui_enabled:
        with st.expander("Timeline markers", expanded=bool(st.session_state.get("roadmap_markers_open", False))):
            st.caption("Add colored tags to selected Features or Epics (outside Filters).")
            sel_state = st.session_state.get("roadmap_selection", {})
            if not isinstance(sel_state, dict):
                sel_state = {}
            sel_mode = str(sel_state.get("mode") or "").strip().lower()
            sel_feature_id = str(sel_state.get("feature_id") or "").strip() if sel_mode == "feature" else ""
            sel_epic_id = str(sel_state.get("epic_id") or "").strip()
            sel_feature_title = str(sel_state.get("feature_title") or "").strip()
            sel_epic_title = str(sel_state.get("epic_title") or "").strip()
            if sel_feature_id:
                st.caption(f"Selected feature: `{sel_feature_id}` {('· ' + sel_feature_title) if sel_feature_title else ''}")
            elif sel_epic_id:
                st.caption(f"Selected epic: `{sel_epic_id}` {('· ' + sel_epic_title) if sel_epic_title else ''}")
            else:
                st.caption("No persisted selection. Pick a Feature/Epic below to add a marker.")

        feature_options: List[Tuple[str, str, str]] = []
        epic_options: List[Tuple[str, str]] = []
        if isinstance(df, pd.DataFrame) and not df.empty:
            if "FEATURE_ID" in df.columns:
                feat_df = df.copy()
                feat_df["FEATURE_ID"] = pd.to_numeric(feat_df.get("FEATURE_ID"), errors="coerce").astype("Int64")
                feat_df = feat_df.dropna(subset=["FEATURE_ID"])
                feat_df["FEATURE_ID_STR"] = feat_df["FEATURE_ID"].astype("Int64").astype(str)
                feat_df["TITLE_STR"] = feat_df.get("TITLE", "").fillna("").astype(str).str.strip()
                if "EPIC_ID" in feat_df.columns:
                    feat_df["EPIC_ID"] = pd.to_numeric(feat_df.get("EPIC_ID"), errors="coerce").astype("Int64")
                    feat_df["EPIC_ID_STR"] = feat_df["EPIC_ID"].astype(str).replace("<NA>", "")
                else:
                    feat_df["EPIC_ID_STR"] = ""
                feat_df = feat_df.sort_values(["TITLE_STR", "FEATURE_ID_STR"])
                seen_feat: set[str] = set()
                for _, r in feat_df.iterrows():
                    fid = str(r.get("FEATURE_ID_STR") or "").strip()
                    if not fid or fid in seen_feat:
                        continue
                    seen_feat.add(fid)
                    ftitle = str(r.get("TITLE_STR") or "").strip() or f"Feature {fid}"
                    epic_ref = str(r.get("EPIC_ID_STR") or "").strip()
                    feature_options.append((fid, ftitle, epic_ref))
            if "EPIC_ID" in df.columns:
                epic_df = df.copy()
                epic_df["EPIC_ID"] = pd.to_numeric(epic_df.get("EPIC_ID"), errors="coerce").astype("Int64")
                epic_df = epic_df.dropna(subset=["EPIC_ID"])
                epic_df["EPIC_ID_STR"] = epic_df["EPIC_ID"].astype("Int64").astype(str)
                epic_df["EPIC_TITLE_STR"] = epic_df.get("EPIC_TITLE", "").fillna("").astype(str).str.strip()
                epic_df = epic_df.sort_values(["EPIC_TITLE_STR", "EPIC_ID_STR"])
                seen_epic: set[str] = set()
                for _, r in epic_df.iterrows():
                    eid = str(r.get("EPIC_ID_STR") or "").strip()
                    if not eid or eid in seen_epic:
                        continue
                    seen_epic.add(eid)
                    etitle = str(r.get("EPIC_TITLE_STR") or "").strip() or f"Epic {eid}"
                    epic_options.append((eid, etitle))
        with st.form("roadmap_milestone_add_form_selection", clear_on_submit=True):
            mode_options = ["Selected", "Choose Feature", "Choose Epic", "Manual"]
            default_mode = "Selected" if (sel_feature_id or sel_epic_id) else ("Choose Feature" if feature_options else ("Choose Epic" if epic_options else "Manual"))
            mode_index = mode_options.index(default_mode)
            target_mode = st.selectbox(
                "Target",
                options=mode_options,
                index=mode_index,
                key="roadmap_marker_target_mode",
            )
            pick_feature_id = ""
            pick_feature_title = ""
            pick_feature_epic_id = ""
            pick_epic_id = ""
            pick_epic_title = ""
            if target_mode == "Choose Feature":
                feature_labels = [""] + [f"{title} [{fid}]" for fid, title, _ in feature_options]
                choice = st.selectbox("Feature", options=feature_labels, index=0, key="roadmap_marker_pick_feature")
                if choice:
                    idx = feature_labels.index(choice) - 1
                    pick_feature_id, pick_feature_title, pick_feature_epic_id = feature_options[idx]
            elif target_mode == "Choose Epic":
                epic_labels = [""] + [f"{title} [{eid}]" for eid, title in epic_options]
                choice = st.selectbox("Epic", options=epic_labels, index=0, key="roadmap_marker_pick_epic")
                if choice:
                    idx = epic_labels.index(choice) - 1
                    pick_epic_id, pick_epic_title = epic_options[idx]

            effective_feature_id = ""
            effective_epic_id = ""
            effective_feature_title = ""
            effective_epic_title = ""
            if target_mode == "Selected":
                effective_feature_id = sel_feature_id
                effective_epic_id = sel_epic_id
                effective_feature_title = sel_feature_title
                effective_epic_title = sel_epic_title
            elif target_mode == "Choose Feature":
                effective_feature_id = pick_feature_id
                effective_feature_title = pick_feature_title
                effective_epic_id = pick_feature_epic_id
            elif target_mode == "Choose Epic":
                effective_epic_id = pick_epic_id
                effective_epic_title = pick_epic_title

            c_ms1, c_ms2, c_ms3 = st.columns([1.3, 1.4, 1], gap="small")
            ms_tag = c_ms1.selectbox(
                "Tag",
                options=["Release", "Risk", "Decision", "Dependency", "Milestone", "Custom"],
                index=0,
                key="roadmap_marker_tag",
            )
            ms_custom_tag = c_ms1.text_input("Custom tag", value="", key="roadmap_marker_custom_tag") if ms_tag == "Custom" else ""
            default_title = effective_feature_title or effective_epic_title
            ms_title = c_ms2.text_input(
                "Label (optional)",
                value=default_title if default_title else "",
                max_chars=200,
                key="roadmap_marker_title",
            )
            ms_date = c_ms3.date_input("Target date", value=dt.date.today(), key="roadmap_marker_date")
            c_ms4, c_ms5 = st.columns([1, 1], gap="small")
            ms_pi_key = c_ms4.text_input("PI key (optional)", value="", placeholder="2026 I2", key="roadmap_marker_pi")
            use_scope = c_ms5.checkbox("Use current Program/Team scope", value=True, key="roadmap_marker_scope")
            can_submit = bool(effective_feature_id or effective_epic_id or target_mode == "Manual")
            submit_ms = st.form_submit_button(
                "Add marker",
                use_container_width=True,
                disabled=not can_submit,
            )
            if submit_ms:
                try:
                    epic_id_val: Optional[int] = int(float(effective_epic_id)) if str(effective_epic_id or "").strip() else None
                    feature_id_val: Optional[int] = int(float(effective_feature_id)) if str(effective_feature_id or "").strip() else None
                    tag_final = str(ms_custom_tag if ms_tag == "Custom" else ms_tag).strip() or "Milestone"
                    marker_title = str(ms_title or "").strip()
                    if not marker_title:
                        if feature_id_val:
                            marker_title = effective_feature_title or f"Feature {feature_id_val}"
                        elif epic_id_val:
                            marker_title = effective_epic_title or f"Epic {epic_id_val}"
                        else:
                            marker_title = "Roadmap marker"
                    save_program = selected_scope_programs[0] if use_scope and len(selected_scope_programs) == 1 else None
                    save_team = selected_scope_teams[0] if use_scope and len(selected_scope_teams) == 1 else None
                    source_type = "FEATURE" if feature_id_val else ("EPIC" if epic_id_val else "MANUAL")
                    upsert_roadmap_milestone(
                        title=marker_title,
                        target_date=ms_date,
                        pi_key=str(ms_pi_key or "").strip() or None,
                        epic_id=epic_id_val,
                        feature_id=feature_id_val,
                        tag=tag_final,
                        source_type=source_type,
                        program_name=save_program,
                        team_name=save_team,
                        is_active=True,
                        updated_by=_current_user_email() or None,
                    )
                    post_write_refresh("roadmap_milestone_upsert", rerun=False, bump_version=False)
                    st.success("Marker added.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Marker save failed: {e}")

        if isinstance(timeline_milestones_df, pd.DataFrame) and not timeline_milestones_df.empty:
            show_cols = [
                c
                for c in ["MILESTONE_ID", "TITLE", "TAG", "TARGET_DATE", "SOURCE_TYPE", "EPIC_ID", "FEATURE_ID"]
                if c in timeline_milestones_df.columns
            ]
            st.dataframe(timeline_milestones_df[show_cols], use_container_width=True, hide_index=True, height=180)
            delete_opts = {
                f"[{str(r.get('TAG') or '').strip() or 'Milestone'}] {str(r.get('TITLE') or '').strip()} · {str(pd.to_datetime(r.get('TARGET_DATE'), errors='coerce').date())}": str(r.get("MILESTONE_ID") or "").strip()
                for _, r in timeline_milestones_df.iterrows()
            }
            delete_opts = {k: v for k, v in delete_opts.items() if v}
            del_choice = st.selectbox(
                "Delete marker",
                options=[""] + list(delete_opts.keys()),
                index=0,
                key="roadmap_milestone_delete_choice",
            )
            if st.button(
                "Delete selected marker",
                type="secondary",
                use_container_width=True,
                disabled=not bool(del_choice),
                key="roadmap_milestone_delete_btn",
            ):
                mid = delete_opts.get(del_choice)
                if mid:
                    try:
                        delete_roadmap_milestone(mid)
                        post_write_refresh("roadmap_milestone_delete", rerun=False, bump_version=False)
                        st.success("Marker deleted.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Marker delete failed: {e}")
        else:
            st.caption("No markers in current scope.")

if __name__ == "__main__":
    main()
