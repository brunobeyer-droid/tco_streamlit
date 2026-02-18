from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


def list_secret_portfolios(secrets: Dict[str, Any]) -> List[str]:
    try:
        portfolios = secrets.get("ado_portfolios", {}) or {}
        if isinstance(portfolios, dict):
            return sorted([str(k) for k in portfolios.keys()])
    except Exception:
        pass
    return []


def get_secret_pat(secrets: Dict[str, Any], key: str) -> str:
    try:
        portfolios = secrets.get("ado_portfolios", {}) or {}
        if isinstance(portfolios, dict):
            return str(portfolios.get(key) or "")
    except Exception:
        pass
    return ""


def build_ado_base_url(org_url: str, org_override: str) -> Tuple[str, str]:
    org_url = (org_url or "").strip()
    org_override = (org_override or "").strip()
    if not org_url and not org_override:
        return "", ""

    if org_override:
        org = org_override
        if "dev.azure.com" in org_url:
            base = f"https://dev.azure.com/{org}"
        elif org_url:
            base = org_url.rstrip("/")
        else:
            base = f"https://dev.azure.com/{org}"
        return base, org

    try:
        from urllib.parse import urlparse
        parsed = urlparse(org_url)
        host = parsed.netloc or ""
        path = parsed.path.strip("/")
        if host.endswith(".visualstudio.com"):
            org = host.split(".")[0]
            return f"{parsed.scheme}://{host}", org
        if "dev.azure.com" in host:
            org = path.split("/")[0] if path else ""
            base = f"{parsed.scheme}://{host}"
            if org:
                return f"{base}/{org}", org
            return base, ""
    except Exception:
        pass

    return org_url.rstrip("/"), ""


def _chunked(values: Iterable[int], size: int = 200) -> Iterable[List[int]]:
    batch: List[int] = []
    for v in values:
        batch.append(int(v))
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _escape_wiql(value: str) -> str:
    return value.replace("'", "''")


def _normalize_area_path(area_path: Any) -> str:
    if area_path is None:
        return ""
    s = str(area_path).strip()
    if not s or s.lower() in {"none", "nan"}:
        return ""
    s = s.replace("\\", "|")
    parts = [re.sub(r"\s+", " ", p.strip()) for p in s.split("|")]
    parts = [p.upper() for p in parts if p]
    return "|".join(parts)


def _key_segments(key: str) -> List[str]:
    return [seg.strip() for seg in key.split("|") if seg.strip()]


def _compute_key_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["TEAM_VARIANT_KEY_CANDIDATE"] = out["AreaPath"].apply(_normalize_area_path)
    segs = out["TEAM_VARIANT_KEY_CANDIDATE"].apply(_key_segments)
    out["KEY_DEPTH"] = segs.apply(len)
    out["KEY_LEAF"] = segs.apply(lambda s: s[-1] if s else "")
    out["KEY_PREV"] = segs.apply(lambda s: s[-2] if len(s) > 1 else "")
    out["IS_CONTAINER_NODE"] = out.apply(
        lambda r: (int(r.get("KEY_DEPTH") or 0) < 3)
        or (str(r.get("KEY_LEAF") or "").upper() == str(r.get("KEY_PREV") or "").upper()),
        axis=1,
    )
    return out


def make_unique_columns(cols: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for col in cols:
        count = seen.get(col, 0) + 1
        seen[col] = count
        if count == 1:
            out.append(col)
        else:
            out.append(f"{col}__{count}")
    return out


def _requests_session(pat: str) -> "requests.Session":
    if requests is None:
        raise RuntimeError("requests is not installed.")
    sess = requests.Session()
    sess.auth = ("", pat or "")
    sess.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
    return sess


def get_valid_field_refs(base_url: str, pat: str, timeout: int = 60) -> set[str]:
    if not base_url:
        return set()
    sess = _requests_session(pat)
    url = f"{base_url}/_apis/wit/fields"
    resp = sess.get(url, params={"api-version": "7.1"}, timeout=timeout)
    if resp.status_code == 429:
        time.sleep(2)
        resp = sess.get(url, params={"api-version": "7.1"}, timeout=timeout)
    if not resp.ok:
        raise RuntimeError(f"Field refs request failed ({resp.status_code}): {resp.text[:400]}")
    data = resp.json() or {}
    refs = set()
    for item in data.get("value", []) or []:
        ref = item.get("referenceName")
        if ref:
            refs.add(str(ref))
    return refs


def build_wiql(
    project: str,
    area_paths: Optional[List[str]] = None,
    work_item_types: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    iteration_paths: Optional[List[str]] = None,
) -> str:
    where_clauses: List[str] = [f"[System.TeamProject] = '{_escape_wiql(project)}'"]

    area_paths = [a for a in (area_paths or []) if str(a).strip()]
    if area_paths:
        areas_sql = ", ".join([f"'{_escape_wiql(str(a))}'" for a in area_paths])
        where_clauses.append(f"[System.AreaPath] IN ({areas_sql})")

    work_item_types = [t for t in (work_item_types or []) if str(t).strip()]
    if work_item_types:
        types_sql = ", ".join([f"'{_escape_wiql(str(t))}'" for t in work_item_types])
        where_clauses.append(f"[System.WorkItemType] IN ({types_sql})")

    states = [s for s in (states or []) if str(s).strip()]
    if states:
        states_sql = ", ".join([f"'{_escape_wiql(str(s))}'" for s in states])
        where_clauses.append(f"[System.State] IN ({states_sql})")

    iteration_paths = [p for p in (iteration_paths or []) if str(p).strip()]
    if iteration_paths:
        iters_sql = ", ".join([f"'{_escape_wiql(str(p))}'" for p in iteration_paths])
        where_clauses.append(f"[System.IterationPath] IN ({iters_sql})")

    where_sql = " AND ".join(where_clauses)
    return f"SELECT [System.Id] FROM WorkItems WHERE {where_sql}"


def query_work_item_ids(
    base_url: str,
    project: str,
    pat: str,
    sample_size: int,
    work_item_types: List[str],
    states: Optional[List[str]] = None,
    timeout: int = 60,
) -> Tuple[List[int], str]:
    if not base_url or not project:
        return [], ""
    if not pat:
        raise RuntimeError("PAT is required to query Azure DevOps.")

    types = [t for t in (work_item_types or []) if str(t).strip()] or ["Feature"]
    wiql_core = build_wiql(
        project=project,
        area_paths=None,
        work_item_types=types,
        states=states,
        iteration_paths=None,
    )
    wiql = f"{wiql_core} ORDER BY [System.ChangedDate] DESC"
    if " FROM WORKITEMS " not in f" {wiql.upper()} ":
        raise RuntimeError(f"Invalid WIQL (missing FROM WorkItems): {wiql}")
    wiql_sys_fields = set(re.findall(r"System\.[A-Za-z0-9_]+", wiql))
    wiql_allowed = {
        "System.Id",
        "System.TeamProject",
        "System.WorkItemType",
        "System.ChangedDate",
        "System.AreaPath",
        "System.IterationPath",
        "System.State",
    }
    if "System.ChangeDate" in wiql_sys_fields:
        raise RuntimeError(f"Invalid WIQL (use System.ChangedDate): {wiql}")
    unknown_fields = [f for f in sorted(wiql_sys_fields) if f not in wiql_allowed]
    if unknown_fields:
        date_fields = [f for f in unknown_fields if f.endswith("Date")]
        if date_fields:
            raise RuntimeError(
                f"Invalid WIQL (unknown date field {', '.join(date_fields)}; use System.ChangedDate): {wiql}"
            )
        raise RuntimeError(f"Invalid WIQL (unknown fields {', '.join(unknown_fields)}): {wiql}")
    url = f"{base_url}/{project}/_apis/wit/wiql"
    params = {"api-version": "7.1"}
    if sample_size:
        params["$top"] = int(sample_size)
    sess = _requests_session(pat)
    try:
        import streamlit as st  # type: ignore
        st.session_state["ado_last_wiql"] = wiql
        st.session_state["ado_last_wiql_url"] = url
    except Exception:
        pass
    resp = sess.post(url, json={"query": wiql}, params=params, timeout=timeout)
    if resp.status_code == 429:
        time.sleep(2)
        resp = sess.post(url, json={"query": wiql}, params=params, timeout=timeout)
    if not resp.ok:
        raise RuntimeError(f"WIQL failed ({resp.status_code}): {resp.text[:400]}")
    payload = resp.json() or {}
    ids = [int(row.get("id")) for row in payload.get("workItems", []) if row.get("id") is not None]
    return ids, wiql


def fetch_work_items_batch(
    base_url: str,
    project: str,
    pat: str,
    ids: List[int],
    fields_to_request: Optional[List[str]] = None,
    application_field_ref: str = "",
    timeout: int = 60,
) -> pd.DataFrame:
    if not ids:
        return pd.DataFrame()
    fields = [
        "System.Id",
        "System.Title",
        "System.State",
        "System.WorkItemType",
        "System.AreaPath",
        "System.IterationPath",
        "System.ChangedDate",
        "Microsoft.VSTS.Scheduling.StoryPoints",
        "Microsoft.VSTS.Scheduling.Effort",
        "Custom.ApplicationName",
    ]
    if application_field_ref:
        fields.append(application_field_ref)
    if fields_to_request:
        fields = list(dict.fromkeys([f for f in fields_to_request if f]))
    url = f"{base_url}/{project}/_apis/wit/workitemsbatch?api-version=7.0"
    sess = _requests_session(pat)
    rows: List[Dict[str, Any]] = []
    for batch in _chunked(ids, size=200):
        payload = {"ids": batch, "fields": fields}
        resp = sess.post(url, json=payload, timeout=timeout)
        if resp.status_code == 429:
            time.sleep(2)
            resp = sess.post(url, json=payload, timeout=timeout)
        if not resp.ok:
            raise RuntimeError(f"Work items batch failed ({resp.status_code}): {resp.text[:400]}")
        data = resp.json() or {}
        for item in data.get("value", []) or []:
            f = item.get("fields", {}) or {}
            app_val = None
            app_candidates = ["Custom.ApplicationName"]
            if application_field_ref:
                app_candidates = [application_field_ref] + app_candidates
            for key in app_candidates:
                if key in f and f.get(key) not in (None, ""):
                    app_val = f.get(key)
                    break
            rows.append(
                {
                    "WorkItemId": item.get("id"),
                    "Title": f.get("System.Title"),
                    "State": f.get("System.State"),
                    "WorkItemType": f.get("System.WorkItemType"),
                    "AreaPath": f.get("System.AreaPath"),
                    "IterationPath": f.get("System.IterationPath"),
                    "StoryPoints": f.get("Microsoft.VSTS.Scheduling.StoryPoints"),
                    "Effort": f.get("Microsoft.VSTS.Scheduling.Effort"),
                    "Application": app_val,
                    "ChangedDate": f.get("System.ChangedDate"),
                }
            )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    dup_mask = pd.Index(df.columns).duplicated()
    if dup_mask.any():
        df.columns = make_unique_columns(list(df.columns))
    return _compute_key_fields(df)


def _infer_pi_parent(path: Any) -> str:
    if path is None:
        return ""
    s = str(path).strip()
    if not s or s.lower() in {"none", "nan"}:
        return ""
    if "\\" in s:
        return s.rsplit("\\", 1)[0]
    return s


def build_iterations_profile(df_features: pd.DataFrame) -> pd.DataFrame:
    if df_features is None or df_features.empty or "IterationPath" not in df_features.columns:
        return pd.DataFrame(columns=["IterationPath", "InferredPIParentPath", "PI_Label_Inferred", "Notes"])
    unique_paths = sorted([p for p in df_features["IterationPath"].dropna().astype(str).unique() if p.strip()])
    rows = []
    for p in unique_paths:
        parent = _infer_pi_parent(p)
        pi_label = parent.rsplit("\\", 1)[-1] if parent else ""
        rows.append(
            {
                "IterationPath": p,
                "InferredPIParentPath": parent,
                "PI_Label_Inferred": pi_label,
                "Notes": "",
            }
        )
    return pd.DataFrame(rows)


def build_summary(df_features: pd.DataFrame) -> Dict[str, Any]:
    if df_features is None or df_features.empty:
        return {
            "sample_size": 0,
            "distinct_area_paths": 0,
            "distinct_keys": 0,
            "depth_distribution": {},
            "top_keys": [],
            "container_nodes_count": 0,
            "container_nodes_top": [],
            "iteration_path_examples": [],
            "missing_storypoints_pct": 0.0,
            "missing_application_pct": 0.0,
        }

    sample_size = int(len(df_features))
    area_paths = df_features.get("AreaPath", pd.Series(dtype=str))
    key_col = df_features.get("TEAM_VARIANT_KEY_CANDIDATE", pd.Series(dtype=str))

    depth_counts = (
        df_features.get("KEY_DEPTH")
        .fillna(0)
        .astype(int)
        .value_counts()
        .sort_index()
        .to_dict()
    )
    top_keys = (
        key_col.fillna("")
        .loc[key_col.fillna("") != ""]
        .value_counts()
        .head(30)
        .reset_index()
        .rename(columns={"index": "TEAM_VARIANT_KEY_CANDIDATE", "TEAM_VARIANT_KEY_CANDIDATE": "count"})
        .to_dict(orient="records")
    )
    container_df = df_features[df_features.get("IS_CONTAINER_NODE") == True]  # noqa: E712
    container_top = (
        container_df.get("TEAM_VARIANT_KEY_CANDIDATE", pd.Series(dtype=str))
        .fillna("")
        .loc[lambda s: s != ""]
        .value_counts()
        .head(20)
        .reset_index()
        .rename(columns={"index": "TEAM_VARIANT_KEY_CANDIDATE", "TEAM_VARIANT_KEY_CANDIDATE": "count"})
        .to_dict(orient="records")
    )

    iteration_examples = (
        df_features.get("IterationPath", pd.Series(dtype=str))
        .fillna("")
        .loc[lambda s: s != ""]
        .value_counts()
        .head(20)
        .reset_index()
        .rename(columns={"index": "IterationPath", "IterationPath": "count"})
        .to_dict(orient="records")
    )

    story_series = df_features.get("StoryPoints", pd.Series(dtype=float))
    app_series = df_features.get("Application", pd.Series(dtype=str))
    missing_story = story_series.isna().mean() * 100.0 if not story_series.empty else 0.0
    missing_app = app_series.isna().mean() * 100.0 if not app_series.empty else 0.0

    return {
        "sample_size": sample_size,
        "distinct_area_paths": int(area_paths.nunique(dropna=True)) if area_paths is not None else 0,
        "distinct_keys": int(key_col.nunique(dropna=True)) if key_col is not None else 0,
        "depth_distribution": depth_counts,
        "top_keys": top_keys,
        "container_nodes_count": int(len(container_df)),
        "container_nodes_top": container_top,
        "iteration_path_examples": iteration_examples,
        "missing_storypoints_pct": round(float(missing_story), 2),
        "missing_application_pct": round(float(missing_app), 2),
    }


def export_profile_files(
    df_features: pd.DataFrame,
    df_iterations: Optional[pd.DataFrame],
    summary: Dict[str, Any],
    portfolio_label: str,
    export_dir: str = "exports/ado_profile_extractor",
) -> Dict[str, str]:
    os.makedirs(export_dir, exist_ok=True)
    label = re.sub(r"[^a-zA-Z0-9_-]+", "_", portfolio_label.strip()) or "ado_profile"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    feature_path = os.path.join(export_dir, f"{label}__profile_sample_features__{ts}.csv")
    summary_path = os.path.join(export_dir, f"{label}__profile_summary__{ts}.json")
    df_features.to_csv(feature_path, index=False)
    if df_iterations is not None and not df_iterations.empty:
        iter_path = os.path.join(export_dir, f"{label}__profile_sample_iterations__{ts}.csv")
        df_iterations.to_csv(iter_path, index=False)
    else:
        iter_path = ""
    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    return {
        "features_csv": feature_path,
        "iterations_csv": iter_path,
        "summary_json": summary_path,
    }


def run_profile_extractor(
    *,
    org_url: str,
    org_override: str,
    project: str,
    pat: str,
    portfolio_label: str,
    sample_size: int,
    work_item_types: List[str],
    states: Optional[List[str]] = None,
    include_iterations: bool = True,
) -> Dict[str, Any]:
    base_url, org = build_ado_base_url(org_url, org_override)
    if not base_url:
        raise RuntimeError("Organization URL is required.")
    if not project:
        raise RuntimeError("Project name is required.")

    ids, wiql = query_work_item_ids(
        base_url=base_url,
        project=project,
        pat=pat,
        sample_size=sample_size,
        work_item_types=work_item_types,
        states=states,
    )
    df_features = fetch_work_items_batch(
        base_url=base_url,
        project=project,
        pat=pat,
        ids=ids,
    )
    df_iterations = build_iterations_profile(df_features) if include_iterations else pd.DataFrame()
    summary = build_summary(df_features)
    summary.update(
        {
            "org": org or "",
            "project": project,
            "portfolio_label": portfolio_label or "",
            "requested_sample_size": int(sample_size),
            "wiql": wiql,
        }
    )
    paths = export_profile_files(df_features, df_iterations, summary, portfolio_label=portfolio_label or project)
    return {
        "features_df": df_features,
        "iterations_df": df_iterations,
        "summary": summary,
        "paths": paths,
    }
