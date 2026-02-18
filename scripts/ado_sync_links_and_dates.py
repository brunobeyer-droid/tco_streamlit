#!/usr/bin/env python3
"""
Sync ADO workitem dates + links for Roadmap timeline (Phase C).

Additive sync:
  - writes into ADO_WORKITEM_DATES and ADO_WORKITEM_LINKS
  - does not modify ADO_FEATURES or cost logic
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import pandas as pd

from core.ado_profile import (
    build_feature_query_from_profile,
    get_active_ado_profile,
    get_profile_value,
)
from db import (
    ensure_ado_timeline_tables,
    ensure_ado_iteration_calendar_table,
    fetch_df,
    upsert_ado_workitem_dates,
    upsert_ado_workitem_links,
)

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore


def _parse_org_project_from_odata_base(base_url: str) -> Tuple[str, str]:
    try:
        parsed = urlparse(str(base_url or "").strip())
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2:
            return str(parts[0]).strip(), str(parts[1]).strip()
    except Exception:
        pass
    return "", ""


def _split_select_fields(select_clause: Optional[str]) -> List[str]:
    if not select_clause or not isinstance(select_clause, str):
        return []
    return [p.strip() for p in select_clause.split(",") if p.strip()]


def build_ado_odata_url(
    org: str,
    project: str,
    entity: str,
    select: Optional[str],
    filter_: Optional[str],
    expand: Optional[str] = None,
    order_by: Optional[str] = None,
) -> str:
    base = f"https://analytics.dev.azure.com/{org}/{project}/_odata/v3.0-preview/{entity}"
    qs: List[str] = []
    if select:
        qs.append(f"$select={select}")
    if expand:
        qs.append(f"$expand={expand}")
    if filter_:
        qs.append(f"$filter={filter_}")
    if order_by:
        qs.append(f"$orderby={order_by}")
    return base + ("?" + "&".join(qs) if qs else "")


def fetch_ado_odata(url: str, pat: str, max_pages: int = 30, timeout: int = 90) -> pd.DataFrame:
    if not url:
        raise ValueError("OData URL is required.")
    parsed = urlparse(url)
    rows: List[Dict[str, Any]] = []
    if parsed.scheme in ("http", "https"):
        if requests is None:
            raise RuntimeError("requests package is not installed.")
        headers = {"Accept": "application/json;odata.metadata=none"}
        auth = ("", pat or "")
        next_url = url
        pages = 0
        while next_url and pages < max_pages:
            pages += 1
            resp = requests.get(next_url, headers=headers, auth=auth if pat else None, timeout=timeout)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "3"))
                time.sleep(max(1, retry_after))
                resp = requests.get(next_url, headers=headers, auth=auth if pat else None, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
            vals = payload.get("value") if isinstance(payload, dict) else None
            if isinstance(vals, list):
                rows.extend(vals)
            elif isinstance(payload, list):
                rows.extend(payload)
            else:
                raise RuntimeError("Unexpected OData response payload.")
            if isinstance(payload, dict):
                next_url = payload.get("@odata.nextLink") or payload.get("odata.nextLink")
            else:
                next_url = None
    else:
        raise RuntimeError("Only HTTP(S) OData URLs are supported for this sync.")
    return pd.json_normalize(rows) if rows else pd.DataFrame()


def _norm_colmap(df: pd.DataFrame) -> Dict[str, str]:
    return {str(c).strip().lower(): str(c) for c in df.columns}


def _pick(df: pd.DataFrame, *candidates: str) -> pd.Series:
    cmap = _norm_colmap(df)
    for cand in candidates:
        col = cmap.get(cand.lower())
        if col:
            return df[col]
    return pd.Series([None] * len(df), index=df.index)


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return int(v)
    except Exception:
        try:
            return int(float(str(v).strip()))
        except Exception:
            return None


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    if s.lower() in {"none", "nan", "null", "<na>"}:
        return ""
    return s


def _to_ts(v: Any) -> Optional[pd.Timestamp]:
    if v is None:
        return None
    try:
        ts = pd.to_datetime(v, errors="coerce")
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return ts


def _build_or_filter(field: str, ids: Sequence[int]) -> str:
    parts = [f"{field} eq {int(i)}" for i in ids if i is not None]
    if not parts:
        return ""
    return "(" + " or ".join(parts) + ")"


def _fetch_iteration_calendar_maps() -> Tuple[Dict[str, Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]], Dict[str, Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]]]:
    ensure_ado_iteration_calendar_table()
    try:
        cal = fetch_df(
            """
            SELECT
              ITERATION_SK,
              ITERATION_PATH,
              START_DATE,
              END_DATE
            FROM ADO_ITERATION_CALENDAR
            """
        )
    except Exception:
        cal = None
    by_sk: Dict[str, Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]] = {}
    by_path: Dict[str, Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]] = {}
    if cal is None or cal.empty:
        return by_sk, by_path
    for _, row in cal.iterrows():
        start_ts = _to_ts(row.get("START_DATE"))
        end_ts = _to_ts(row.get("END_DATE"))
        sk = _to_str(row.get("ITERATION_SK"))
        path = _to_str(row.get("ITERATION_PATH"))
        if sk:
            by_sk[sk] = (start_ts, end_ts)
        if path:
            by_path[path.lower()] = (start_ts, end_ts)
    return by_sk, by_path


def _normalize_link_category(link_type: str, link_category: str) -> str:
    raw = f"{link_type} {link_category}".lower()
    if any(k in raw for k in ("dependency", "predecessor", "successor")):
        return "Dependency"
    if any(k in raw for k in ("hierarchy", "parent", "child")):
        return "ParentChild"
    return "Other"


def main(
    argv: Optional[List[str]] = None,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> int:
    def _emit(message: str) -> None:
        msg = str(message or "").strip()
        if not msg:
            return
        try:
            print(msg)
        except Exception:
            pass
        if progress_cb is not None:
            try:
                progress_cb(msg)
            except Exception:
                pass

    parser = argparse.ArgumentParser(description="Sync ADO timeline dates + links into portfolio DB.")
    parser.add_argument("--org", default=os.getenv("ADO_ORG"))
    parser.add_argument("--project", default=os.getenv("ADO_PROJECT"))
    parser.add_argument("--pat", default=os.getenv("ADO_PAT"))
    parser.add_argument("--entity", default=os.getenv("ADO_ENTITY", "WorkItems"))
    parser.add_argument("--links-entity", default=os.getenv("ADO_LINKS_ENTITY", "WorkItemLinks"))
    parser.add_argument("--extra-filter", default=os.getenv("ADO_TIMELINE_EXTRA_FILTER", ""))
    parser.add_argument("--order-by", default=os.getenv("ADO_TIMELINE_ORDER_BY", "WorkItemId desc"))
    parser.add_argument("--max-pages", type=int, default=int(os.getenv("ADO_TIMELINE_MAX_PAGES", "30")))
    parser.add_argument("--link-chunk-size", type=int, default=int(os.getenv("ADO_TIMELINE_LINK_CHUNK_SIZE", "30")))
    args = parser.parse_args(argv)

    profile = get_active_ado_profile()
    org = str(args.org or "").strip()
    project = str(args.project or "").strip()
    if not (org and project):
        p_org, p_project = _parse_org_project_from_odata_base(str(profile.get("odata_base_url") or ""))
        org = org or p_org
        project = project or p_project
    if not (org and project):
        parser.error("Provide --org and --project (or configure odata_base_url in active profile).")
    if not args.pat:
        parser.error("Provide --pat or set ADO_PAT.")

    _emit("Timeline sync: resolving ADO profile + filters...")

    query_info = build_feature_query_from_profile(org, project, args.entity, profile)
    profile_expand = str(query_info.get("expand_clause") or "").strip() or None
    extra_filter = str(args.extra_filter or "").strip()
    pi_field = str(get_profile_value(profile, "iteration_mapping.pi_field", "IterationLevel3") or "IterationLevel3").strip() or "IterationLevel3"
    exclude_states_raw = list(get_profile_value(profile, "filters.exclude_states", []) or [])
    years_prefix_raw = list(get_profile_value(profile, "filters.years_prefix", []) or [])

    scope_parts: List[str] = []
    for state in exclude_states_raw:
        sval = str(state or "").strip()
        if sval:
            scope_parts.append(f"State ne '{sval}'")
    years_prefix = [str(y).strip() for y in years_prefix_raw if str(y).strip()]
    base_scope_filter = " and ".join(scope_parts)
    if extra_filter:
        extra_norm = " ".join(str(extra_filter or "").split())
        base_norm = " ".join(str(base_scope_filter or "").split())
        if extra_norm and extra_norm != base_norm:
            scope_parts.append(f"({extra_filter})")

    type_feature = str(get_profile_value(profile, "work_item_types.feature", "Feature") or "Feature").strip()
    type_epic = str(get_profile_value(profile, "work_item_types.epic", "Epic") or "Epic").strip()
    story_types_raw = str(
        get_profile_value(profile, "work_item_types.story_csv", "User Story,Product Backlog Item")
        or "User Story,Product Backlog Item"
    ).split(",")
    story_types = [str(x).strip() for x in story_types_raw if str(x).strip()]
    include_types = [type_feature, type_epic] + story_types
    type_clauses: List[str] = []
    for t in include_types:
        tv = str(t or "").strip()
        if not tv:
            continue
        safe_tv = tv.replace("'", "''")
        type_clauses.append(f"WorkItemType eq '{safe_tv}'")
    type_filter = " or ".join(type_clauses)

    year_filter_candidates: List[str] = []
    if years_prefix:
        for year_field in [pi_field, "IterationPath", "IterationName"]:
            ors = [f"startswith(Iteration/{year_field},'{y}')" for y in years_prefix]
            if ors:
                year_filter_candidates.append("(" + " or ".join(ors) + ")")
    # De-dupe while preserving order.
    _seen_year_filters: set[str] = set()
    year_filter_candidates = [
        f for f in year_filter_candidates if not (f in _seen_year_filters or _seen_year_filters.add(f))
    ]
    if not years_prefix:
        raise RuntimeError(
            "Timeline sync requires ADO profile filters.years_prefix (strict year scope)."
        )
    if not year_filter_candidates:
        raise RuntimeError(
            "Timeline sync could not build a valid year filter from filters.years_prefix."
        )

    filter_candidates: List[str] = []
    if year_filter_candidates:
        for year_filter in year_filter_candidates:
            scope_bits: List[str] = []
            if base_scope_filter:
                scope_bits.append(base_scope_filter)
            scope_bits.append(year_filter)
            scope_filter = " and ".join([b for b in scope_bits if b])
            if type_filter:
                scope_filter = f"({scope_filter}) and ({type_filter})" if scope_filter else f"({type_filter})"
            filter_candidates.append(scope_filter)

    _seen_filters: set[str] = set()
    filter_candidates = [f for f in filter_candidates if f and not (f in _seen_filters or _seen_filters.add(f))]

    select_candidates = [
        "WorkItemId,WorkItemType,Title,State,StartDate,TargetDate,ChangedDate",
        "WorkItemId,WorkItemType,Title,State,ChangedDate",
        "WorkItemId,WorkItemType,State,ChangedDate",
    ]
    work_df: Optional[pd.DataFrame] = None
    last_err: Optional[Exception] = None
    _emit("Timeline sync: querying WorkItems (strict year scope)...")
    for filter_clause in filter_candidates:
        for select_clause in select_candidates:
            try:
                work_url = build_ado_odata_url(
                    org,
                    project,
                    args.entity,
                    select_clause,
                    filter_clause,
                    profile_expand,
                    str(args.order_by or "").strip() or None,
                )
                work_df = fetch_ado_odata(work_url, str(args.pat), max_pages=int(args.max_pages or 30))
                last_err = None
                break
            except Exception as e:
                last_err = e
                continue
        if work_df is not None:
            break
    if work_df is None:
        raise RuntimeError(f"WorkItems sync failed: {last_err}")
    if work_df.empty:
        _emit("Timeline sync: no WorkItems returned for configured year scope.")
        return 0

    work = pd.DataFrame(
        {
            "WORKITEM_ID": _pick(work_df, "WorkItemId", "WORKITEM_ID"),
            "WORKITEM_TYPE": _pick(work_df, "WorkItemType", "WORKITEM_TYPE"),
            "STATE": _pick(work_df, "State", "STATE"),
            "ITERATION_PATH": _pick(work_df, "IterationPath", "Iteration.IterationPath", "ITERATION_PATH"),
            "ITERATION_SK": _pick(work_df, "IterationSK", "Iteration.IterationSK", "ITERATION_SK"),
            "START_DATE": _pick(work_df, "StartDate", "START_DATE", "ActualStartDate"),
            "END_DATE": _pick(work_df, "TargetDate", "END_DATE", "DueDate", "FinishDate", "EndDate"),
        }
    )
    work["WORKITEM_ID"] = work["WORKITEM_ID"].apply(_to_int)
    work = work[work["WORKITEM_ID"].notna()].copy()
    work["WORKITEM_ID"] = work["WORKITEM_ID"].astype(int)
    work["WORKITEM_TYPE"] = work["WORKITEM_TYPE"].map(_to_str)
    work["ITERATION_PATH"] = work["ITERATION_PATH"].map(_to_str)
    work["ITERATION_SK"] = work["ITERATION_SK"].map(_to_str)
    work["START_DATE"] = work["START_DATE"].map(_to_ts)
    work["END_DATE"] = work["END_DATE"].map(_to_ts)

    cal_by_sk, cal_by_path = _fetch_iteration_calendar_maps()
    for idx, row in work.iterrows():
        start_ts = row.get("START_DATE")
        end_ts = row.get("END_DATE")
        iter_sk = _to_str(row.get("ITERATION_SK"))
        iter_path = _to_str(row.get("ITERATION_PATH")).lower()
        if start_ts is None or end_ts is None:
            cal_pair = None
            if iter_sk and iter_sk in cal_by_sk:
                cal_pair = cal_by_sk.get(iter_sk)
            elif iter_path and iter_path in cal_by_path:
                cal_pair = cal_by_path.get(iter_path)
            if cal_pair:
                if start_ts is None:
                    start_ts = cal_pair[0]
                if end_ts is None:
                    end_ts = cal_pair[1]
        work.at[idx, "START_DATE"] = start_ts
        work.at[idx, "END_DATE"] = end_ts

    ensure_ado_timeline_tables()
    date_rows: List[Tuple[Any, ...]] = []
    for _, r in work.iterrows():
        wid = _to_int(r.get("WORKITEM_ID"))
        if wid is None:
            continue
        s_ts = _to_ts(r.get("START_DATE"))
        e_ts = _to_ts(r.get("END_DATE"))
        date_rows.append(
            (
                int(wid),
                _to_str(r.get("WORKITEM_TYPE")) or None,
                s_ts.date() if s_ts is not None else None,
                e_ts.date() if e_ts is not None else None,
                _to_str(r.get("ITERATION_PATH")) or None,
            )
        )
    _emit(f"Timeline sync: upserting workitem dates ({len(date_rows):,} rows)...")
    n_dates = upsert_ado_workitem_dates(date_rows)

    ids = sorted({_to_int(v) for v in work["WORKITEM_ID"].tolist() if _to_int(v) is not None})
    link_rows_all: List[Tuple[Any, ...]] = []
    if ids:
        _emit("Timeline sync: querying dependency + parent/child links...")
        link_select_candidates = [
            "SourceWorkItemId,TargetWorkItemId,LinkTypeName,LinkCategoryName,ChangedDate",
            "SourceWorkItemId,TargetWorkItemId,LinkTypeReferenceName,LinkTypeName,ChangedDate",
            "SourceWorkItemId,TargetWorkItemId,LinkTypeName,ChangedDate",
            "SourceWorkItemId,TargetWorkItemId,LinkTypeReferenceName,ChangedDate",
        ]
        for start in range(0, len(ids), max(1, int(args.link_chunk_size or 30))):
            chunk = ids[start : start + max(1, int(args.link_chunk_size or 30))]
            part_filter = _build_or_filter("SourceWorkItemId", chunk)
            part_filter2 = _build_or_filter("TargetWorkItemId", chunk)
            local_filter = ""
            if part_filter and part_filter2:
                local_filter = f"({part_filter} or {part_filter2})"
            elif part_filter:
                local_filter = part_filter
            elif part_filter2:
                local_filter = part_filter2
            if not local_filter:
                continue

            links_df: Optional[pd.DataFrame] = None
            for select_clause in link_select_candidates:
                try:
                    links_url = build_ado_odata_url(
                        org,
                        project,
                        args.links_entity,
                        select_clause,
                        local_filter,
                        None,
                        None,
                    )
                    links_df = fetch_ado_odata(links_url, str(args.pat), max_pages=5)
                    break
                except Exception:
                    links_df = None
                    continue
            if links_df is None or links_df.empty:
                continue
            ldf = pd.DataFrame(
                {
                    "SOURCE_ID": _pick(links_df, "SourceWorkItemId", "SOURCE_ID"),
                    "TARGET_ID": _pick(links_df, "TargetWorkItemId", "TARGET_ID"),
                    "LINK_TYPE": _pick(links_df, "LinkTypeName", "LinkTypeReferenceName", "LINK_TYPE"),
                    "LINK_CATEGORY": _pick(links_df, "LinkCategoryName", "LINK_CATEGORY"),
                }
            )
            ldf["SOURCE_ID"] = ldf["SOURCE_ID"].apply(_to_int)
            ldf["TARGET_ID"] = ldf["TARGET_ID"].apply(_to_int)
            ldf = ldf[ldf["SOURCE_ID"].notna() & ldf["TARGET_ID"].notna()].copy()
            if ldf.empty:
                continue
            for _, r in ldf.iterrows():
                sid = _to_int(r.get("SOURCE_ID"))
                tid = _to_int(r.get("TARGET_ID"))
                if sid is None or tid is None:
                    continue
                link_type = _to_str(r.get("LINK_TYPE"))
                link_category_raw = _to_str(r.get("LINK_CATEGORY"))
                link_category = _normalize_link_category(link_type, link_category_raw)
                if link_category not in {"ParentChild", "Dependency"}:
                    continue
                link_rows_all.append((int(sid), int(tid), link_category, link_type or link_category_raw or link_category))

    if link_rows_all:
        dedup = {(r[0], r[1], str(r[2]).strip(), str(r[3]).strip()): r for r in link_rows_all}
        _emit(f"Timeline sync: upserting links ({len(dedup):,} rows)...")
        n_links = upsert_ado_workitem_links(list(dedup.values()))
    else:
        n_links = 0

    _emit(
        f"Timeline sync complete. workitems={len(work):,}, "
        f"workitem_dates_upserted={n_dates:,}, workitem_links_upserted={n_links:,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
