#!/usr/bin/env python3
"""
Sync ADO Feature/Epic progress (StoryPoints rollup by StateCategory).

This is an additive sync:
  - does NOT modify ADO_FEATURES
  - writes into ADO_FEATURE_PROGRESS / ADO_EPIC_PROGRESS
  - optionally refreshes ADO_WORKITEM_LOOKUP cache
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import pandas as pd

from db import (
    ensure_ado_progress_tables,
    ensure_ado_workitem_lookup_table,
    fetch_ado_workitem_lookup,
    upsert_ado_workitem_lookup,
    upsert_ado_feature_progress,
    upsert_ado_epic_progress,
)
from utils.ado_progress import compute_progress_metrics, normalize_state_category
from core.ado_profile import get_active_ado_profile, get_profile_value

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore


DEFAULT_FEATURE_TYPES = {"feature"}
DEFAULT_EPIC_TYPES = {"epic"}
DEFAULT_STORY_TYPES = {"user story", "product backlog item"}


def build_ado_odata_url(
    org: str,
    project: str,
    entity: str,
    select: Optional[str],
    filter_: Optional[str],
    order_by: Optional[str] = None,
) -> str:
    base = f"https://analytics.dev.azure.com/{org}/{project}/_odata/v3.0-preview/{entity}"
    qs: List[str] = []
    if select:
        qs.append(f"$select={select}")
    if filter_:
        qs.append(f"$filter={filter_}")
    if order_by:
        qs.append(f"$orderby={order_by}")
    return base + ("?" + "&".join(qs) if qs else "")


def fetch_ado_odata(url: str, pat: str, max_pages: int = 20, timeout: int = 90) -> pd.DataFrame:
    if not url:
        raise ValueError("OData URL is required")
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
        file_path = url
        if parsed.scheme == "file":
            file_path = parsed.path
        with open(file_path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict) and isinstance(payload.get("value"), list):
            rows = payload["value"]
        else:
            raise RuntimeError("Mock JSON must be a list or {'value': [...]} format.")
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


def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        if pd.isna(v):
            return 0.0
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return 0.0


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _fetch_lookup_chain(seed_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    pending = {int(i) for i in seed_ids if i is not None}
    max_hops = 8
    hops = 0
    while pending and hops < max_hops:
        hops += 1
        ids = sorted(i for i in pending if i not in out)
        if not ids:
            break
        chunk = 500
        seen_any = False
        for i in range(0, len(ids), chunk):
            part = ids[i : i + chunk]
            df = fetch_ado_workitem_lookup(part)
            if df is None or df.empty:
                continue
            seen_any = True
            for _, r in df.iterrows():
                wid = _to_int(r.get("WORKITEM_ID"))
                if wid is None:
                    continue
                out[wid] = {
                    "WORKITEM_TYPE": _to_str(r.get("WORKITEM_TYPE")),
                    "PARENT_ID": _to_int(r.get("PARENT_ID")),
                }
        if not seen_any:
            break
        next_pending: set[int] = set()
        for rec in out.values():
            pid = _to_int(rec.get("PARENT_ID"))
            if pid is not None and pid not in out:
                next_pending.add(pid)
        pending = next_pending
    return out


def _resolve_feature_epic(
    parent_id: Optional[int],
    type_map: Dict[int, str],
    parent_map: Dict[int, Optional[int]],
    feature_types: set[str],
    epic_types: set[str],
) -> Tuple[Optional[int], Optional[int]]:
    feature_id: Optional[int] = None
    epic_id: Optional[int] = None
    cur = parent_id
    seen: set[int] = set()
    max_depth = 30
    depth = 0
    while cur is not None and depth < max_depth and cur not in seen:
        seen.add(cur)
        depth += 1
        wtype = _to_str(type_map.get(cur)).lower()
        if feature_id is None and wtype in feature_types:
            feature_id = cur
        if epic_id is None and wtype in epic_types:
            epic_id = cur
        if feature_id is not None and epic_id is not None:
            break
        cur = _to_int(parent_map.get(cur))
    return feature_id, epic_id


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

    parser = argparse.ArgumentParser(description="Sync ADO Feature/Epic progress into portfolio DB.")
    parser.add_argument("--org", default=os.getenv("ADO_ORG"))
    parser.add_argument("--project", default=os.getenv("ADO_PROJECT"))
    parser.add_argument("--pat", default=os.getenv("ADO_PAT"))
    parser.add_argument("--entity", default=os.getenv("ADO_ENTITY", "WorkItems"))
    parser.add_argument("--url", default=os.getenv("ADO_PROGRESS_URL", ""))
    parser.add_argument("--extra-filter", default=os.getenv("ADO_PROGRESS_EXTRA_FILTER", ""))
    parser.add_argument("--order-by", default=os.getenv("ADO_PROGRESS_ORDER_BY", "WorkItemId desc"))
    parser.add_argument("--max-pages", type=int, default=int(os.getenv("ADO_PROGRESS_MAX_PAGES", "30")))
    parser.add_argument("--feature-type", default=os.getenv("ADO_PROGRESS_FEATURE_TYPE", "Feature"))
    parser.add_argument("--epic-type", default=os.getenv("ADO_PROGRESS_EPIC_TYPE", "Epic"))
    parser.add_argument(
        "--story-types",
        default=os.getenv("ADO_PROGRESS_STORY_TYPES", "User Story,Product Backlog Item"),
        help="Comma-separated story work item types.",
    )
    parser.add_argument(
        "--exclude-states",
        default=os.getenv("ADO_PROGRESS_EXCLUDE_STATES", ""),
        help="Comma-separated exact state names to skip in progress rollup.",
    )
    parser.add_argument(
        "--story-points-field",
        default=os.getenv("ADO_PROGRESS_STORY_POINTS_FIELD", ""),
        help="ADO field used as points source for progress rollup (defaults to active profile fields.story_points).",
    )
    args = parser.parse_args(argv)

    if not args.url and not (args.org and args.project):
        parser.error("Provide --url or --org + --project.")
    if not args.pat:
        parser.error("Provide --pat or set ADO_PAT.")
    _emit("Progress sync: resolving profile + filters...")
    profile = get_active_ado_profile()

    feature_type_raw = str(args.feature_type or "").strip() or "Feature"
    epic_type_raw = str(args.epic_type or "").strip() or "Epic"
    story_types_raw = [str(v).strip() for v in str(args.story_types or "").split(",") if str(v).strip()]
    if not story_types_raw:
        story_types_raw = ["User Story", "Product Backlog Item"]
    feature_types = {feature_type_raw.lower()} or set(DEFAULT_FEATURE_TYPES)
    epic_types = {epic_type_raw.lower()} or set(DEFAULT_EPIC_TYPES)
    story_types = {v.lower() for v in story_types_raw} or set(DEFAULT_STORY_TYPES)
    exclude_states = {
        str(v).strip().lower()
        for v in str(args.exclude_states or "").split(",")
        if str(v).strip()
    }
    story_points_field = str(args.story_points_field or "").strip()
    if not story_points_field:
        story_points_field = str(get_profile_value(profile, "fields.story_points", "") or "").strip()
    if not story_points_field:
        story_points_field = "StoryPoints"

    select = (
        f"WorkItemId,WorkItemType,Title,State,StateCategory,{story_points_field},"
        "ParentWorkItemId,ChangedDate"
    )
    query_types: List[str] = []
    query_types.extend([epic_type_raw, feature_type_raw])
    query_types.extend(story_types_raw)
    seen_types: set[str] = set()
    dedup_types: List[str] = []
    for t in query_types:
        k = t.lower()
        if k in seen_types:
            continue
        seen_types.add(k)
        dedup_types.append(t)
    type_filter = " or ".join([f"WorkItemType eq '{t}'" for t in dedup_types])
    extra_filter = str(args.extra_filter or "").strip()
    combined_filter = f"({type_filter})"
    if extra_filter:
        combined_filter = f"({combined_filter}) and ({extra_filter})"

    url = args.url or build_ado_odata_url(
        args.org,
        args.project,
        args.entity,
        select,
        combined_filter,
        str(args.order_by or "").strip() or None,
    )

    _emit("Progress sync: querying WorkItems...")
    df = fetch_ado_odata(url, args.pat, max_pages=args.max_pages)
    if df is None or df.empty:
        _emit("Progress sync: no ADO rows returned.")
        return 0

    work = pd.DataFrame(
        {
            "WORKITEM_ID": _pick(df, "WorkItemId", "WORKITEM_ID"),
            "WORKITEM_TYPE": _pick(df, "WorkItemType", "WORKITEM_TYPE"),
            "TITLE": _pick(df, "Title", "TITLE"),
            "STATE": _pick(df, "State", "STATE"),
            "STATE_CATEGORY": _pick(df, "StateCategory", "STATE_CATEGORY"),
            "STORY_POINTS": _pick(
                df,
                story_points_field,
                f"Fields.{story_points_field}",
                "StoryPoints",
                "Fields.StoryPoints",
                "Effort",
                "Fields.Effort",
                "STORY_POINTS",
            ),
            "PARENT_ID": _pick(df, "ParentWorkItemId", "PARENT_ID"),
            "CHANGED_DATE": _pick(df, "ChangedDate", "CHANGED_DATE"),
        }
    )
    if work.empty:
        _emit("Progress sync: no normalized rows.")
        return 0

    work["WORKITEM_ID"] = work["WORKITEM_ID"].apply(_to_int)
    work = work[work["WORKITEM_ID"].notna()].copy()
    work["WORKITEM_ID"] = work["WORKITEM_ID"].astype(int)
    work["WORKITEM_TYPE"] = work["WORKITEM_TYPE"].map(_to_str)
    work["STATE"] = work["STATE"].map(_to_str)
    work["STATE_CATEGORY"] = work["STATE_CATEGORY"].map(_to_str)
    work["STORY_POINTS"] = work["STORY_POINTS"].map(_to_float)
    work["PARENT_ID"] = work["PARENT_ID"].apply(_to_int)

    _emit("Progress sync: preparing lookup/progress tables...")
    ensure_ado_workitem_lookup_table()
    ensure_ado_progress_tables()

    lookup_rows: List[Tuple[Any, ...]] = []
    for _, r in work.iterrows():
        lookup_rows.append(
            (
                int(r["WORKITEM_ID"]),
                _to_str(r.get("WORKITEM_TYPE")),
                _to_str(r.get("TITLE")) or None,
                _to_str(r.get("STATE")) or None,
                _to_int(r.get("PARENT_ID")),
                r.get("CHANGED_DATE"),
            )
        )
    if lookup_rows:
        _emit(f"Progress sync: upserting lookup rows ({len(lookup_rows):,})...")
        upsert_ado_workitem_lookup(lookup_rows)

    type_map: Dict[int, str] = {}
    parent_map: Dict[int, Optional[int]] = {}
    for _, r in work.iterrows():
        wid = int(r["WORKITEM_ID"])
        type_map[wid] = _to_str(r.get("WORKITEM_TYPE"))
        parent_map[wid] = _to_int(r.get("PARENT_ID"))

    seed_parent_ids = [pid for pid in parent_map.values() if pid is not None and pid not in type_map]
    if seed_parent_ids:
        _emit("Progress sync: resolving parent chain from cached lookup...")
        cached = _fetch_lookup_chain(seed_parent_ids)
        for wid, rec in cached.items():
            if wid not in type_map:
                type_map[wid] = _to_str(rec.get("WORKITEM_TYPE"))
            if wid not in parent_map:
                parent_map[wid] = _to_int(rec.get("PARENT_ID"))

    stories = work[work["WORKITEM_TYPE"].str.lower().isin(story_types)].copy()
    if stories.empty:
        _emit("Progress sync: no story rows found for rollup.")
        return 0

    _emit(f"Progress sync: rolling up story points ({len(stories):,} story rows)...")
    feature_acc: Dict[int, Dict[str, float]] = defaultdict(
        lambda: {"PROPOSED_SP": 0.0, "INPROGRESS_SP": 0.0, "COMPLETED_SP": 0.0}
    )
    epic_acc: Dict[int, Dict[str, float]] = defaultdict(
        lambda: {"PROPOSED_SP": 0.0, "INPROGRESS_SP": 0.0, "COMPLETED_SP": 0.0}
    )
    epic_to_features: Dict[int, set[int]] = defaultdict(set)
    feature_has_stories: set[int] = set()
    feature_story_points_all: Dict[int, float] = defaultdict(float)
    removed_story_points = 0.0

    # Build epic->feature structure from feature parents first.
    feature_rows = work[work["WORKITEM_TYPE"].str.lower().isin(feature_types)].copy()
    for _, row in feature_rows.iterrows():
        feature_id = _to_int(row.get("WORKITEM_ID"))
        parent_id = _to_int(row.get("PARENT_ID"))
        if feature_id is None:
            continue
        _, epic_id = _resolve_feature_epic(parent_id, type_map, parent_map, feature_types, epic_types)
        if epic_id is not None:
            epic_to_features[int(epic_id)].add(int(feature_id))

    for _, row in stories.iterrows():
        state_name = _to_str(row.get("STATE")).lower()
        if state_name and state_name in exclude_states:
            continue
        parent_id = _to_int(row.get("PARENT_ID"))
        if parent_id is None:
            continue
        feature_id, epic_id = _resolve_feature_epic(parent_id, type_map, parent_map, feature_types, epic_types)
        bucket = normalize_state_category(row.get("STATE_CATEGORY"))
        points = _to_float(row.get("STORY_POINTS"))
        if feature_id is not None:
            fid = int(feature_id)
            feature_has_stories.add(fid)
            feature_story_points_all[fid] += points
            if epic_id is not None:
                epic_to_features[int(epic_id)].add(fid)
        if bucket == "removed":
            removed_story_points += points
            # Removed/cut stories are intentionally excluded from denominator.
            continue
        if feature_id is not None:
            if bucket == "completed":
                feature_acc[feature_id]["COMPLETED_SP"] += points
            elif bucket == "inprogress":
                feature_acc[feature_id]["INPROGRESS_SP"] += points
            else:
                feature_acc[feature_id]["PROPOSED_SP"] += points
        if epic_id is not None:
            if bucket == "completed":
                epic_acc[epic_id]["COMPLETED_SP"] += points
            elif bucket == "inprogress":
                epic_acc[epic_id]["INPROGRESS_SP"] += points
            else:
                epic_acc[epic_id]["PROPOSED_SP"] += points

    feature_rows: List[Tuple[Any, ...]] = []
    for feature_id, agg in feature_acc.items():
        m = compute_progress_metrics(
            agg.get("PROPOSED_SP", 0.0),
            agg.get("INPROGRESS_SP", 0.0),
            agg.get("COMPLETED_SP", 0.0),
        )
        feature_rows.append(
            (
                int(feature_id),
                float(m["PROPOSED_SP"]),
                float(m["INPROGRESS_SP"]),
                float(m["COMPLETED_SP"]),
                float(m["TOTAL_SP"]),
                float(m["PCT_COMPLETE"]),
            )
        )

    epic_rows: List[Tuple[Any, ...]] = []
    all_epic_ids = set(epic_acc.keys()) | set(epic_to_features.keys())
    for epic_id in sorted(all_epic_ids):
        agg = epic_acc.get(epic_id, {"PROPOSED_SP": 0.0, "INPROGRESS_SP": 0.0, "COMPLETED_SP": 0.0})
        m = compute_progress_metrics(
            agg.get("PROPOSED_SP", 0.0),
            agg.get("INPROGRESS_SP", 0.0),
            agg.get("COMPLETED_SP", 0.0),
        )
        epic_feature_ids = epic_to_features.get(int(epic_id), set())
        feature_count_total = int(len(epic_feature_ids))
        feature_count_with_stories = int(len([fid for fid in epic_feature_ids if fid in feature_has_stories]))
        feature_count_with_sp = int(len([fid for fid in epic_feature_ids if float(feature_story_points_all.get(fid, 0.0)) > 0.0]))
        epic_rows.append(
            (
                int(epic_id),
                float(m["PROPOSED_SP"]),
                float(m["INPROGRESS_SP"]),
                float(m["COMPLETED_SP"]),
                float(m["TOTAL_SP"]),
                float(m["PCT_COMPLETE"]),
                feature_count_total,
                feature_count_with_stories,
                feature_count_with_sp,
            )
        )

    _emit(
        f"Progress sync: upserting progress rows (features={len(feature_rows):,}, epics={len(epic_rows):,})..."
    )
    n_feature = upsert_ado_feature_progress(feature_rows)
    n_epic = upsert_ado_epic_progress(epic_rows)
    _emit(
        f"Progress sync complete. Stories={len(stories)}, "
        f"feature_progress={n_feature}, epic_progress={n_epic}, "
        f"removed_story_points_excluded={removed_story_points:.2f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
