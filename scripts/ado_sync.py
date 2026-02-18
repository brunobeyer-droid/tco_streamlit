#!/usr/bin/env python3
"""
CLI to sync Azure DevOps Features via Analytics OData into MSSQL ADO_FEATURES.

Usage (env vars or flags):
  export MSSQL_SERVER=... MSSQL_DATABASE=... MSSQL_USER=... MSSQL_PASSWORD=...
  export ADO_PAT=...  # Personal Access Token with Analytics read
  python scripts/ado_sync.py --org ORG --project PROJECT

Optional:
  --entity WorkItems
  --select "WorkItemId,Title,State,CreatedDate,ChangedDate,BusinessValue,Custom_ApplicationName,Custom_InvestmentDimension"
  --expand "Team($select=TeamName),Iteration($select=IterationLevel3,IterationPath,IterationSK),Area($select=AreaPath,AreaLevel2,AreaLevel3,AreaLevel4)"
  --filter "WorkItemType eq 'Feature'"
  --max-pages 10
  --url FULL_ODATA_URL  # overrides builder

Env var fallbacks:
  ADO_ORG, ADO_PROJECT, ADO_ENTITY, ADO_SELECT, ADO_EXPAND, ADO_FILTER, ADO_PAT, ADO_URL

Scheduling:
  Add a weekly cron, e.g. (Sundays 02:15):
    15 2 * * 0 /path/to/venv/bin/python /path/to/repo/scripts/ado_sync.py --org ORG --project PROJECT >> /var/log/ado_sync.log 2>&1
  Ensure MSSQL_* and ADO_PAT environment variables are available to the cron environment (use an envfile or systemd timer if needed).
"""

from __future__ import annotations
import os
import sys
import time
import argparse
import json
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

import pandas as pd

# Reuse DB facade (reads MSSQL_* from env)
from db import (
    ensure_ado_minimal_tables,
    upsert_ado_features,
    fetch_ado_workitem_lookup,
    fetch_ado_workitem_lookup_by_type,
    upsert_ado_workitem_lookup,
)
from core.ado_profile import (
    get_active_ado_profile,
    build_feature_query_from_profile,
    get_profile_value,
)
from utils.ado import transform_ado_odata_to_expected, normalize_to_canonical, repair_leaf_teams
from scripts.ado_epic_resolver import resolve_epics_for_features

try:
    import requests  # type: ignore
except Exception as e:
    print("ERROR: requests package not installed. pip install requests", file=sys.stderr)
    raise


def build_ado_odata_url(org: str, project: str, entity: str, select: Optional[str], expand: Optional[str], filter_: Optional[str]) -> str:
    base = f"https://analytics.dev.azure.com/{org}/{project}/_odata/v3.0-preview/{entity}"
    qs: List[str] = []
    if select:
        qs.append(f"$select={select}")
    if expand:
        qs.append(f"$expand={expand}")
    if filter_:
        qs.append(f"$filter={filter_}")
    return base + ("?" + "&".join(qs) if qs else "")


def _split_select_fields(select_clause: Optional[str]) -> List[str]:
    if not select_clause or not isinstance(select_clause, str):
        return []
    return [part.strip() for part in select_clause.split(",") if part.strip()]


def _build_workitems_id_filter(ids: List[int]) -> Optional[str]:
    if not ids:
        return None
    clauses = [f"WorkItemId eq {int(i)}" for i in ids]
    return "(" + " or ".join(clauses) + ")"


def _candidate_field_names(field_name: str) -> List[str]:
    raw = str(field_name or "").strip()
    if not raw:
        return []
    candidates = [raw]
    if raw.startswith("Fields."):
        candidates.append(raw[len("Fields."):])
    else:
        candidates.append(f"Fields.{raw}")
    if "." in raw:
        candidates.append(raw.replace(".", "_"))
    if "_" in raw:
        candidates.append(raw.replace("_", "."))
    out: List[str] = []
    seen = set()
    for cand in candidates:
        c = str(cand or "").strip()
        if not c:
            continue
        key = c.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out


def _resolve_df_column(df: pd.DataFrame, field_name: str) -> Optional[str]:
    if df is None or df.empty:
        return None
    by_lower = {str(c).lower(): str(c) for c in df.columns}
    for cand in _candidate_field_names(field_name):
        if cand in df.columns:
            return cand
        mapped = by_lower.get(cand.lower())
        if mapped:
            return mapped
    return None


def _apply_profile_field_mappings(df_expected: pd.DataFrame, profile: Dict[str, Any]) -> None:
    fields = profile.get("fields") if isinstance(profile.get("fields"), dict) else {}
    if not fields:
        return
    app_field = str(fields.get("app_name") or "").strip()
    inv_field = str(fields.get("investment_dimension") or "").strip()
    sp_field = str(fields.get("story_points") or "").strip()
    bv_field = str(fields.get("business_value") or "").strip()

    app_col = _resolve_df_column(df_expected, app_field) if app_field else None
    inv_col = _resolve_df_column(df_expected, inv_field) if inv_field else None
    sp_col = _resolve_df_column(df_expected, sp_field) if sp_field else None
    bv_col = _resolve_df_column(df_expected, bv_field) if bv_field else None

    if app_col:
        df_expected["Custom_ApplicationName"] = df_expected.get(app_col)
    if inv_col:
        df_expected["Custom_InvestmentDimension"] = df_expected.get(inv_col)
    if sp_col:
        df_expected["StoryPoints"] = pd.to_numeric(df_expected.get(sp_col), errors="coerce")
    if bv_col:
        df_expected["BusinessValue"] = pd.to_numeric(df_expected.get(bv_col), errors="coerce")


def _fetch_workitems_by_ids(
    org: str,
    project: str,
    entity: str,
    pat: str,
    ids: List[int],
    select_fields: List[str],
    max_pages: int,
) -> List[Dict[str, Any]]:
    if not ids:
        return []
    url = build_ado_odata_url(
        org,
        project,
        entity,
        ",".join(select_fields),
        None,
        _build_workitems_id_filter(ids),
    )
    df = fetch_ado_odata(url, pat, max_pages=max_pages)
    return df.to_dict(orient="records") if df is not None and not df.empty else []


def fetch_ado_odata(url: str, pat: str, max_pages: int = 10, timeout: int = 60) -> pd.DataFrame:
    if not url:
        raise ValueError("OData URL or mock file path is required")
    parsed = urlparse(url)
    rows: List[Dict[str, Any]] = []
    if parsed.scheme in ("http","https"):
        headers = {"Accept": "application/json;odata.metadata=none"}
        auth = ("", pat or "")
        next_url = url
        page = 0
        while next_url and page < max_pages:
            page += 1
            resp = requests.get(next_url, headers=headers, auth=auth if pat else None, timeout=timeout)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "3"))
                time.sleep(max(1, retry_after))
                resp = requests.get(next_url, headers=headers, auth=auth if pat else None, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                rows.extend(data)
                next_url = None
            else:
                vals = data.get("value") or []
                if not isinstance(vals, list):
                    raise RuntimeError("Unexpected OData response")
                rows.extend(vals)
                next_url = data.get("@odata.nextLink") or data.get("odata.nextLink")
    else:
        # file path
        fpath = url
        if parsed.scheme == "file":
            fpath = parsed.path
        if not os.path.isabs(fpath):
            fpath = os.path.abspath(fpath)
        with open(fpath, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict) and isinstance(data.get("value"), list):
            rows = data["value"]
        else:
            raise RuntimeError("Mock JSON must be a list or a dict with a 'value' list.")
    return pd.json_normalize(rows) if rows else pd.DataFrame()




def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Sync ADO Features via OData into MSSQL")
    p.add_argument("--org", default=os.getenv("ADO_ORG"))
    p.add_argument("--project", default=os.getenv("ADO_PROJECT"))
    p.add_argument("--pat", default=os.getenv("ADO_PAT"))
    p.add_argument("--entity", default=os.getenv("ADO_ENTITY", "WorkItems"))
    p.add_argument("--select", default=os.getenv("ADO_SELECT"))
    p.add_argument("--expand", default=os.getenv("ADO_EXPAND"))
    p.add_argument("--filter", dest="filter_", default=os.getenv("ADO_FILTER"))
    p.add_argument("--max-pages", type=int, default=int(os.getenv("ADO_MAX_PAGES", "10")))
    p.add_argument("--url", default=os.getenv("ADO_URL", ""))
    args = p.parse_args(argv)

    profile = get_active_ado_profile()
    if not args.url:
        if not (args.org and args.project):
            p.error("Provide --url or --org and --project")
        profile_debug = build_feature_query_from_profile(args.org, args.project, args.entity, profile)
        select_fields = args.select or ",".join(profile_debug["select_fields"])
        expand_clause = args.expand or profile_debug["expand_clause"]
        filter_clause = args.filter_ or profile_debug["filter_clause"]
        url = build_ado_odata_url(args.org, args.project, args.entity, select_fields, expand_clause, filter_clause)
        debug_info = {
            "base_url": profile_debug["base_url"],
            "select_fields": _split_select_fields(select_fields),
            "expand_clause": expand_clause,
            "filter_clause": filter_clause,
            "final_url": url,
        }
        if str(os.getenv("ADO_DEBUG", "")).lower() in {"1", "true", "yes"}:
            print("ADO query debug:")
            print(json.dumps(debug_info, indent=2))
    else:
        url = args.url
    if not args.pat:
        p.error("Provide --pat or set ADO_PAT env var")

    print(f"Fetching OData: {url}")
    df_raw = fetch_ado_odata(url, args.pat, max_pages=args.max_pages)
    if df_raw is None or df_raw.empty:
        print("No rows returned.")
        return 0
    # Enforce Features only if WorkItemType present
    for c in df_raw.columns:
        if c.lower().endswith("workitemtype") or c.lower() == "workitemtype":
            feature_type = str(get_profile_value(profile, "work_item_types.feature", "Feature") or "Feature").strip()
            df_raw = df_raw[df_raw[c].astype(str).str.strip().str.lower() == feature_type.lower()].copy()
            break

    df_expected = transform_ado_odata_to_expected(df_raw)
    _apply_profile_field_mappings(df_expected, profile)
    df_norm = normalize_to_canonical(df_expected)
    df_norm = repair_leaf_teams(df_norm)

    # Epic resolution (parent chain)
    org_for_epic = args.org
    proj_for_epic = args.project
    if (not org_for_epic or not proj_for_epic) and args.url:
        try:
            parsed = urlparse(args.url)
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) >= 2:
                org_for_epic = org_for_epic or parts[0]
                proj_for_epic = proj_for_epic or parts[1]
        except Exception:
            pass

    debug_summary: Dict[str, Any] = {}
    if org_for_epic and proj_for_epic:
        epic_select = ["WorkItemId", "WorkItemType", "Title", "State", "ParentWorkItemId", "ChangedDate"]

        def _fetch_workitems(ids: List[int]) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            if not ids:
                return rows
            cached = fetch_ado_workitem_lookup(ids)
            cached_map: Dict[int, Dict[str, Any]] = {}
            if cached is not None and not cached.empty:
                for _, row in cached.iterrows():
                    try:
                        wid = int(row.get("WORKITEM_ID"))
                    except Exception:
                        continue
                    cached_map[wid] = {
                        "WorkItemId": wid,
                        "WorkItemType": row.get("WORKITEM_TYPE"),
                        "Title": row.get("TITLE"),
                        "State": row.get("STATE"),
                        "ParentWorkItemId": row.get("PARENT_ID"),
                        "ChangedDate": row.get("CHANGED_DATE"),
                    }
            missing = [i for i in ids if i not in cached_map or cached_map[i].get("ChangedDate") is None]
            fetched_rows: List[Dict[str, Any]] = []
            for i in range(0, len(missing), 50):
                batch = missing[i : i + 50]
                fetched_rows.extend(
                    _fetch_workitems_by_ids(
                        org_for_epic,
                        proj_for_epic,
                        args.entity,
                        args.pat,
                        batch,
                        epic_select,
                        args.max_pages,
                    )
                )
            if fetched_rows:
                rows_to_upsert: List[Tuple[Any, ...]] = []
                for row in fetched_rows:
                    try:
                        wid = int(row.get("WorkItemId"))
                    except Exception:
                        continue
                    rows_to_upsert.append(
                        (
                            wid,
                            row.get("WorkItemType"),
                            row.get("Title"),
                            row.get("State"),
                            row.get("ParentWorkItemId"),
                            row.get("ChangedDate"),
                        )
                    )
                if rows_to_upsert:
                    upsert_ado_workitem_lookup(rows_to_upsert)
            rows.extend(list(cached_map.values()))
            rows.extend(fetched_rows)
            return rows

        def _fetch_epics(epic_type: str) -> List[Dict[str, Any]]:
            cached = fetch_ado_workitem_lookup_by_type(epic_type)
            cached_rows: List[Dict[str, Any]] = []
            if cached is not None and not cached.empty:
                for _, row in cached.iterrows():
                    try:
                        wid = int(row.get("WORKITEM_ID"))
                    except Exception:
                        continue
                    cached_rows.append(
                        {
                            "WorkItemId": wid,
                            "WorkItemType": row.get("WORKITEM_TYPE"),
                            "Title": row.get("TITLE"),
                            "State": row.get("STATE"),
                            "ParentWorkItemId": row.get("PARENT_ID"),
                            "ChangedDate": row.get("CHANGED_DATE"),
                        }
                    )
            if cached_rows:
                return cached_rows
            safe = epic_type.replace("'", "''")
            epic_filter = f"WorkItemType eq '{safe}'"
            url = build_ado_odata_url(
                org_for_epic,
                proj_for_epic,
                args.entity,
                ",".join(epic_select),
                None,
                epic_filter,
            )
            df = fetch_ado_odata(url, args.pat, max_pages=args.max_pages)
            rows = df.to_dict(orient="records") if df is not None and not df.empty else []
            if rows:
                rows_to_upsert: List[Tuple[Any, ...]] = []
                for row in rows:
                    try:
                        wid = int(row.get("WorkItemId"))
                    except Exception:
                        continue
                    rows_to_upsert.append(
                        (
                            wid,
                            row.get("WorkItemType"),
                            row.get("Title"),
                            row.get("State"),
                            row.get("ParentWorkItemId"),
                            row.get("ChangedDate"),
                        )
                    )
                if rows_to_upsert:
                    upsert_ado_workitem_lookup(rows_to_upsert)
            return rows

        _fetch_workitems.fetch_epics = _fetch_epics  # type: ignore[attr-defined]

        features_rows = df_raw.to_dict(orient="records")
        epic_map, debug_summary = resolve_epics_for_features(features_rows, profile, _fetch_workitems)

        if "FEATURE_ID" in df_norm.columns:
            def _to_int(val: Any) -> Optional[int]:
                if val is None or str(val).strip() == "":
                    return None
                try:
                    return int(val)
                except Exception:
                    return None

            df_norm["PARENT_ID"] = df_norm["FEATURE_ID"].apply(
                lambda v: epic_map.get(_to_int(v), {}).get("parent_id")
            )
            df_norm["EPIC_ID"] = df_norm["FEATURE_ID"].apply(
                lambda v: epic_map.get(_to_int(v), {}).get("epic_id")
            )
            df_norm["EPIC_TITLE"] = df_norm["FEATURE_ID"].apply(
                lambda v: epic_map.get(_to_int(v), {}).get("epic_title")
            )
            df_norm["EPIC_STATE"] = df_norm["FEATURE_ID"].apply(
                lambda v: epic_map.get(_to_int(v), {}).get("epic_state")
            )

    if debug_summary:
        print(f"Epic resolution summary: {debug_summary}")

    ensure_ado_minimal_tables()
    n = upsert_ado_features(df_norm)
    print(f"Upserted {n} row(s) into ADO_FEATURES.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
