from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional

import pandas as pd

from utils.ado import fetch_ado_odata
from db.mssql_backend import fetch_ado_workitem_lookup, upsert_ado_workitem_lookup

LAST_EPIC_SUMMARY: Dict[str, Any] = {}


def _normalize_id(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        return int(float(value))
    except Exception:
        return None


def _build_filter_for_ids(ids: Iterable[int]) -> str:
    parts = [f"WorkItemId eq {int(i)}" for i in ids]
    return "(" + " or ".join(parts) + ")"


def fetch_workitems_by_filter(
    odata_base_url: str,
    filter_clause: str,
    auth_ctx: Dict[str, Any],
    select_fields: List[str],
) -> List[Dict[str, Any]]:
    base = (odata_base_url or "").rstrip("/")
    if not base.lower().endswith("/workitems"):
        base = base + "/WorkItems"
    select_str = ",".join(select_fields)
    url = base + "?" + "&".join([f"$select={select_str}", f"$filter={filter_clause}"])
    diag: Dict[str, Any] = {}
    df = fetch_ado_odata(url, auth_ctx.get("pat") or "", diag, max_pages=1)
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


def fetch_workitems_by_ids(
    odata_base_url: str,
    ids: List[int],
    auth_ctx: Dict[str, Any],
    select_fields: List[str],
    batch_size: int = 100,
) -> List[Dict[str, Any]]:
    if not ids:
        return []
    rows: List[Dict[str, Any]] = []
    cleaned = [i for i in ids if i is not None]
    for i in range(0, len(cleaned), batch_size):
        batch = cleaned[i : i + batch_size]
        filter_clause = _build_filter_for_ids(batch)
        rows.extend(fetch_workitems_by_filter(odata_base_url, filter_clause, auth_ctx, select_fields))
    return rows


def _rows_to_upsert(rows: List[Dict[str, Any]]) -> List[tuple[Any, ...]]:
    out: List[tuple[Any, ...]] = []
    for row in rows:
        wid = _normalize_id(row.get("WorkItemId") or row.get("WORKITEM_ID"))
        if wid is None:
            continue
        out.append(
            (
                wid,
                row.get("WorkItemType") or row.get("WORKITEM_TYPE"),
                row.get("Title") or row.get("TITLE"),
                row.get("State") or row.get("STATE"),
                _normalize_id(row.get("ParentWorkItemId") or row.get("PARENT_ID")),
                row.get("ChangedDate") or row.get("CHANGED_DATE"),
            )
        )
    return out


def resolve_epics_for_features(
    feature_rows: List[Dict[str, Any]],
    profile: Dict[str, Any],
    fetch_fn: Callable[[List[int]], List[Dict[str, Any]]],
    max_rounds: int = 3,
) -> Dict[str, Dict[str, Any]]:
    max_rounds = min(int(max_rounds or 3), 3)
    max_ids_cap = 10000
    remaining_cap = max_ids_cap
    batch_size = 100
    epic_type = str((profile.get("work_item_types") or {}).get("epic") or "Epic").strip() or "Epic"
    max_depth = int((profile.get("epic_resolution") or {}).get("max_depth") or 6)

    def _node_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "type": row.get("WorkItemType"),
            "title": row.get("Title"),
            "state": row.get("State"),
            "parent_id": _normalize_id(row.get("ParentWorkItemId")),
        }

    parent_ids = {_normalize_id(r.get("ParentWorkItemId")) for r in feature_rows}
    parent_ids.discard(None)

    lookup: Dict[int, Dict[str, Any]] = {}
    rounds_used = 0
    cache_hits_count = 0
    cache_misses_count = 0
    odata_fetch_batches = 0
    odata_ids_fetched_total = 0

    fetch_by_filter = getattr(fetch_fn, "fetch_by_filter", None)
    if callable(fetch_by_filter):
        try:
            epic_rows = fetch_by_filter(f"WorkItemType eq '{epic_type}'")
            for row in epic_rows:
                wid = _normalize_id(row.get("WorkItemId"))
                if wid is not None:
                    lookup[wid] = _node_from_row(row)
            if epic_rows:
                upsert_ado_workitem_lookup(_rows_to_upsert(epic_rows))
        except Exception:
            pass

    for _ in range(max_rounds):
        missing = [pid for pid in parent_ids if pid not in lookup]
        if not missing:
            break
        if remaining_cap <= 0:
            break
        if len(missing) > remaining_cap:
            missing = missing[:remaining_cap]
        rounds_used += 1
        try:
            cache_df = fetch_ado_workitem_lookup(missing)
        except Exception:
            cache_df = pd.DataFrame()
        if cache_df is not None and not cache_df.empty:
            cache_rows = cache_df.to_dict(orient="records")
            for row in cache_rows:
                wid = _normalize_id(row.get("WORKITEM_ID"))
                if wid is not None:
                    lookup[wid] = {
                        "type": row.get("WORKITEM_TYPE"),
                        "title": row.get("TITLE"),
                        "state": row.get("STATE"),
                        "parent_id": _normalize_id(row.get("PARENT_ID")),
                    }
            cache_hits = {r.get("WORKITEM_ID") for r in cache_rows}
            cache_hits_count += len([x for x in cache_hits if x is not None])
        cache_misses = [pid for pid in missing if pid not in lookup]
        cache_misses_count += len(cache_misses)
        if not cache_misses:
            continue
        if len(cache_misses) > remaining_cap:
            cache_misses = cache_misses[:remaining_cap]
        odata_fetch_batches += int((len(cache_misses) + batch_size - 1) / batch_size)
        odata_ids_fetched_total += len(cache_misses)
        remaining_cap -= len(cache_misses)
        fetched = fetch_fn(cache_misses)
        for row in fetched:
            wid = _normalize_id(row.get("WorkItemId"))
            if wid is None:
                continue
            lookup[wid] = _node_from_row(row)
            pid = _normalize_id(row.get("ParentWorkItemId"))
            if pid is not None:
                parent_ids.add(pid)
        if fetched:
            try:
                upsert_ado_workitem_lookup(_rows_to_upsert(fetched))
            except Exception:
                pass

    results: Dict[str, Dict[str, Any]] = {}
    resolved = 0
    features_with_parent = 0

    for row in feature_rows:
        fid = _normalize_id(row.get("WorkItemId"))
        if fid is None:
            continue
        parent = _normalize_id(row.get("ParentWorkItemId"))
        if parent is not None:
            features_with_parent += 1
        epic_id = None
        epic_title = None
        epic_state = None
        visited = set()
        current = parent
        for _depth in range(max_depth):
            if current is None:
                break
            if current in visited:
                break
            visited.add(current)
            node = lookup.get(current)
            if not node:
                break
            if str(node.get("type") or "").strip() == epic_type:
                epic_id = current
                epic_title = node.get("title")
                epic_state = node.get("state")
                break
            current = _normalize_id(node.get("parent_id"))

        if epic_id is not None:
            resolved += 1

        results[str(fid)] = {
            "parent_id": parent,
            "epic_id": epic_id,
            "epic_title": epic_title,
            "epic_state": epic_state,
        }

    LAST_EPIC_SUMMARY.clear()
    LAST_EPIC_SUMMARY.update(
        {
            "total_features": len(results),
            "features_with_parent": features_with_parent,
            "features_resolved_to_epic": resolved,
            "unresolved_features": max(features_with_parent - resolved, 0),
            "rounds_used": rounds_used,
            "lookup_size": len(lookup),
            "max_depth": max_depth,
            "cache_hits_count": cache_hits_count,
            "cache_misses_count": cache_misses_count,
            "odata_fetch_batches": odata_fetch_batches,
            "odata_ids_fetched_total": odata_ids_fetched_total,
            "odata_ids_cap": max_ids_cap,
            "odata_ids_cap_remaining": remaining_cap,
        }
    )

    return results
