from __future__ import annotations
import os
import json
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import pandas as pd
import re

def _norm_state(val: Any) -> str:
    try:
        s = str(val or "").strip()
    except Exception:
        return ""
    return s.upper()


def normalize_pi_key(label: Any) -> str:
    """Normalize PI label strings to a stable key (e.g., '2025 PI1')."""
    try:
        s = str(label or "").strip()
    except Exception:
        return ""
    if " (" in s:
        s = s.split(" (", 1)[0]
    # Collapse internal whitespace to avoid key drift.
    return " ".join(s.split())


# v1 state semantics (case-insensitive matching)
COMPLETED_STATES = {"CLOSED", "DONE", "RESOLVED", "COMPLETED"}
COMMITTED_STATES = {"PLANNED", "IN PROGRESS", "IN_PROGRESS", "VALIDATION"}
ROADMAP_STATES = {"NEW", "ANALYZE"}
EXCLUDED_STATES = {"REMOVED"}

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


EXPECTED_RAW_COLUMNS = [
    "WorkItemId",
    "Id",
    "Team",
    "Team.TeamName",
    "Iteration",
    "Iteration.IterationPath",
    "Iteration.IterationLevel3",
    "Iteration.IterationSK",
    "AreaPath",
    "Area.AreaPath",
    "Custom_ApplicationName",
    "Fields.Custom_ApplicationName",
    "Custom_InvestmentDimension",
    "Fields.Custom_InvestmentDimension",
    "BusinessValue",
    "Fields.BusinessValue",
    # SWAG input (repo convention): use the Feature "Effort" field as the SWAG points source.
    # (Some orgs use StoryPoints, but this repo treats Effort as the authoritative SWAG.)
    "Effort",
    "Fields.Effort",
    "Microsoft.VSTS.Scheduling.Effort",
    "Fields.Microsoft.VSTS.Scheduling.Effort",
    # Also accept StoryPoints if present.
    "StoryPoints",
    "Fields.StoryPoints",
    "Microsoft.VSTS.Scheduling.StoryPoints",
    "Fields.Microsoft.VSTS.Scheduling.StoryPoints",
]


def missing_expected_columns(df: pd.DataFrame, required: Optional[List[str]] = None) -> List[str]:
    """Return which expected columns are absent from the provided dataframe."""
    required = required or EXPECTED_RAW_COLUMNS
    return [col for col in required if col not in df.columns]


def fetch_ado_odata(url: str, pat: str, diag: Dict[str, Any], max_pages: int = 20, timeout: int = 60) -> pd.DataFrame:
    from urllib.parse import urlparse
    import requests, json, os, time

    if not url:
        raise ValueError("Provide an OData URL or a mock JSON path")

    parsed = urlparse(url)
    rows: List[Dict[str, Any]] = []
    diag.setdefault("pages", [])
    diag["max_pages_hit"] = False

    # Web URL
    if parsed.scheme in ("http", "https"):
        if requests is None:
            raise RuntimeError("'requests' not installed; cannot fetch URL. Install it or use a local JSON file.")
        headers = {"Accept": "application/json;odata.metadata=none"}
        auth = ("", pat or "")
        next_url = url
        page = 0
        while next_url and page < max_pages:
            page += 1
            resp = requests.get(next_url, headers=headers, auth=auth if pat else None, timeout=timeout, verify=False)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "3"))
                time.sleep(max(1, retry_after))
                resp = requests.get(next_url, headers=headers, auth=auth if pat else None, timeout=timeout, verify=False)
            if not resp.ok:
                raise RuntimeError(f"OData request failed ({resp.status_code}): {resp.text[:400]}")
            data = resp.json()
            if isinstance(data, list):
                page_rows = data
                next_link = None
            else:
                page_rows = data.get("value") or []
                next_link = data.get("@odata.nextLink") or data.get("odata.nextLink")
            if isinstance(page_rows, list):
                rows.extend(page_rows)
            else:
                raise RuntimeError("Unexpected JSON payload: expected list or { 'value': [...] }")
            diag["pages"].append({"url": next_url, "count": len(page_rows)})
            next_url = next_link
        diag["max_pages_hit"] = bool(next_url)
        diag["mode"] = "http"
    else:
        # Local file
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
        diag["mode"] = "file"
        diag["file"] = fpath
        diag["max_pages_hit"] = False

    if not rows:
        return pd.DataFrame()
    df = pd.json_normalize(rows)
    diag["missing_expected"] = missing_expected_columns(df)
    try:
        interesting = [
            # Business value
            "BusinessValue",
            "Fields.BusinessValue",
            "Microsoft.VSTS.Common.BusinessValue",
            "Fields.Microsoft.VSTS.Common.BusinessValue",
            "Microsoft_VSTS_Common_BusinessValue",
            "Fields.Microsoft_VSTS_Common_BusinessValue",
        ]
        diag["columns_present"] = {c: (c in df.columns) for c in interesting}
        diag["nonnull_counts"] = {c: int(df[c].notna().sum()) for c in interesting if c in df.columns}
    except Exception:
        pass
    return df


def build_ado_iterations_odata_url(org: str, project: str, year_prefixes: Optional[List[str]] = None) -> str:
    base = f"https://analytics.dev.azure.com/{org}/{project}/_odata/v4.0-preview/Iterations"
    select = "IterationSK,IterationPath,IterationLevel3,StartDate,EndDate"
    prefixes = [p for p in (year_prefixes or []) if str(p).strip()]
    filter_sql = ""
    if prefixes:
        ors = " OR ".join([f"startswith(IterationLevel3,'{str(p).strip()}')" for p in prefixes])
        filter_sql = f"$filter={ors}"
    qs = [f"$select={select}"]
    if filter_sql:
        qs.append(filter_sql)
    return base + "?" + "&".join(qs)


def fetch_ado_iterations_odata(
    org: str,
    project: str,
    pat: str,
    diag: Dict[str, Any],
    year_prefixes: Optional[List[str]] = None,
    include_sprints: bool = False,
    max_pages: int = 20,
    timeout: int = 60,
) -> pd.DataFrame:
    url = build_ado_iterations_odata_url(org, project, year_prefixes=year_prefixes or ["2025", "2026", "2027"])
    df_raw = fetch_ado_odata(url, pat, diag, max_pages=max_pages, timeout=timeout)
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=[
            "ITERATION_PATH", "ITERATION_SK", "ITERATION_LEVEL3",
            "START_DATE", "END_DATE", "YEAR", "MONTH_KEY", "QUARTER_KEY", "ITERATION_GRAIN",
        ])

    work = df_raw.copy()
    rename_map = {}
    for src, dst in (
        ("IterationPath", "ITERATION_PATH"),
        ("IterationSK", "ITERATION_SK"),
        ("IterationLevel3", "ITERATION_LEVEL3"),
        ("StartDate", "START_DATE"),
        ("EndDate", "END_DATE"),
    ):
        if src in work.columns:
            rename_map[src] = dst
    work = work.rename(columns=rename_map)
    for col in ("ITERATION_PATH", "ITERATION_LEVEL3"):
        if col in work.columns:
            work[col] = work[col].astype(str).str.strip().replace({"": None, "nan": None, "None": None})
    if "ITERATION_SK" in work.columns:
        def _sk_to_str(v: Any) -> Optional[str]:
            if v is None:
                return None
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            if isinstance(v, (int,)):
                return str(v)
            if isinstance(v, float):
                if v != v:  # NaN
                    return None
                if float(v).is_integer():
                    return str(int(v))
                return str(v)
            s = str(v).strip()
            return s if s and s.lower() not in {"nan", "none", "<na>"} else None

        work["ITERATION_SK"] = work["ITERATION_SK"].map(_sk_to_str)
    if "ITERATION_PATH" in work.columns:
        sprint_re = re.compile(r"(?:\\s|[\\\\/])S\\d+\\b", flags=re.IGNORECASE)
        work["ITERATION_GRAIN"] = work["ITERATION_PATH"].apply(
            lambda p: "SPRINT" if (isinstance(p, str) and sprint_re.search(p)) else "PI"
        )
    else:
        work["ITERATION_GRAIN"] = "PI"

    if not include_sprints and "ITERATION_GRAIN" in work.columns:
        work = work[work["ITERATION_GRAIN"] == "PI"].copy()

    for dcol in ("START_DATE", "END_DATE"):
        if dcol in work.columns:
            parsed = pd.to_datetime(work[dcol], errors="coerce", utc=True)
            if isinstance(parsed, pd.DatetimeIndex):
                parsed = pd.Series(parsed, index=work.index)
            work[dcol] = parsed.map(lambda x: None if pd.isna(x) else x.to_pydatetime().date())

    start_dt = pd.to_datetime(work.get("START_DATE"), errors="coerce")
    work["YEAR"] = start_dt.dt.year.astype("Int64")
    work["MONTH_KEY"] = start_dt.dt.strftime("%Y-%m").where(start_dt.notna(), None)
    q = ((start_dt.dt.month - 1) // 3 + 1).astype("Int64")
    work["QUARTER_KEY"] = (
        (start_dt.dt.year.astype("Int64").astype(str) + "-Q" + q.astype("Int64").astype(str))
        .where(start_dt.notna(), None)
    )

    cols = [
        "ITERATION_PATH", "ITERATION_SK", "ITERATION_LEVEL3",
        "START_DATE", "END_DATE", "YEAR", "MONTH_KEY", "QUARTER_KEY", "ITERATION_GRAIN",
    ]
    for c in cols:
        if c not in work.columns:
            work[c] = None
    work = work[cols].copy()
    work = work[work["ITERATION_PATH"].notna()].copy()
    diag["iterations_rows"] = int(len(work))
    return work.reset_index(drop=True)


def transform_ado_odata_to_expected(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map flattened OData fields to the expected ADO columns used by our normalizer.
    Tries multiple common shapes for Team/Iteration/Custom fields.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()

    # Some shapes include an unflattened Iteration dict; extract join keys if present.
    if "Iteration" in work.columns:
        try:
            has_iter_dict = work["Iteration"].apply(lambda v: isinstance(v, dict)).any()
        except Exception:
            has_iter_dict = False
        if has_iter_dict:
            for k, dst in (
                ("IterationPath", "IterationPath"),
                ("IterationLevel3", "IterationLevel3"),
                ("IterationSK", "IterationSK"),
            ):
                extracted = work["Iteration"].apply(lambda v: (v.get(k) if isinstance(v, dict) else None))
                if dst not in work.columns:
                    work[dst] = extracted
                else:
                    # Fill blanks without overwriting existing extracted/flattened values.
                    try:
                        work[dst] = work[dst].where(work[dst].notna(), extracted)
                    except Exception:
                        pass

    rename_candidates = [
        ("WorkItemId", "ID"),
        ("Id", "ID"),
        ("Team.TeamName", "Team"),
        ("Team.Name", "Team"),
        ("Team", "Team"),
        ("Iteration.IterationPath", "Iteration"),
        ("IterationPath", "Iteration"),
        ("Iteration", "Iteration"),
        ("Iteration.IterationLevel3", "IterationLevel3"),
        ("IterationLevel3", "IterationLevel3"),
        ("Iteration.IterationSK", "IterationSK"),
        ("IterationSK", "IterationSK"),
        ("Fields.Custom_ApplicationName", "Custom_ApplicationName"),
        ("Custom_ApplicationName", "Custom_ApplicationName"),
        ("Fields.Custom_InvestmentDimension", "Custom_InvestmentDimension"),
        ("Custom_InvestmentDimension", "Custom_InvestmentDimension"),
        ("BusinessValue", "BusinessValue"),
        ("Fields.BusinessValue", "BusinessValue"),
        ("Microsoft.VSTS.Common.BusinessValue", "BusinessValue"),
        ("Fields.Microsoft.VSTS.Common.BusinessValue", "BusinessValue"),
        ("Microsoft_VSTS_Common_BusinessValue", "BusinessValue"),
        ("Fields.Microsoft_VSTS_Common_BusinessValue", "BusinessValue"),
        # SWAG input (repo convention): use "Effort" as SWAG points source.
        ("Effort", "Effort"),
        ("Fields.Effort", "Effort"),
        ("Microsoft.VSTS.Scheduling.Effort", "Effort"),
        ("Fields.Microsoft.VSTS.Scheduling.Effort", "Effort"),
        # Also accept StoryPoints if present.
        ("StoryPoints", "StoryPoints"),
        ("Fields.StoryPoints", "StoryPoints"),
        ("Microsoft.VSTS.Scheduling.StoryPoints", "StoryPoints"),
        ("Fields.Microsoft.VSTS.Scheduling.StoryPoints", "StoryPoints"),
        ("System.Title", "Title"),
        ("Title", "Title"),
        ("System.State", "State"),
        ("State", "State"),
        ("CreatedDate", "CreatedDate"),
        ("ChangedDate", "ChangedDate"),
        ("Area.AreaPath", "AreaPath"),
        ("AreaPath", "AreaPath"),
        ("Fields.AreaPath", "AreaPath"),
        ("Area.AreaLevel2", "AreaLevel2"),
        ("AreaLevel2", "AreaLevel2"),
        ("Fields.AreaLevel2", "AreaLevel2"),
        ("Area.AreaLevel3", "AreaLevel3"),
        ("AreaLevel3", "AreaLevel3"),
        ("Fields.AreaLevel3", "AreaLevel3"),
        ("Area.AreaLevel4", "AreaLevel4"),
        ("AreaLevel4", "AreaLevel4"),
        ("Fields.AreaLevel4", "AreaLevel4"),
    ]
    rename_map: Dict[str, str] = {}
    for src, dst in rename_candidates:
        if src in work.columns:
            rename_map[src] = dst
    work = work.rename(columns=rename_map)

    # Types and cleanup
    for col in (
        "Team",
        "Custom_ApplicationName",
        "Custom_InvestmentDimension",
        "Iteration",
        "IterationLevel3",
        "IterationSK",
        "Title",
        "State",
        "AreaPath",
        "AreaLevel2",
        "AreaLevel3",
        "AreaLevel4",
    ):
        if col in work.columns:
            work[col] = work[col].astype(str).str.strip()
    for num_col in ("BusinessValue",):
        if num_col in work.columns:
            work[num_col] = pd.to_numeric(work[num_col], errors="coerce")
    for dcol in ("CreatedDate", "ChangedDate"):
        if dcol in work.columns:
            work[dcol] = pd.to_datetime(work[dcol], errors="coerce")
    if "ID" in work.columns:
        work["ID"] = work["ID"].astype(str).str.strip()
    if "Year" not in work.columns:
        for dcol in ("ChangedDate", "CreatedDate"):
            if dcol in work.columns:
                tmp = pd.to_datetime(work[dcol], errors="coerce")
                work["Year"] = tmp.dt.year.astype("Int64")
                break
    return work


def _pick_col(df: pd.DataFrame, *names: str) -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([], dtype="object")


def normalize_to_canonical(
    df: pd.DataFrame,
    effort_fields: Optional[List[str]] = None,
    diag: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    diag = diag if isinstance(diag, dict) else None

    out = pd.DataFrame()
    raw_id = _pick_col(df, "ID")
    out["FEATURE_ID"]     = raw_id.astype(str) if not raw_id.empty else raw_id
    out["TITLE"]          = _pick_col(df, "Title")
    out["STATE"]          = _pick_col(df, "State")

    team_direct = _pick_col(df, "Team")
    program_raw = _pick_col(df, "AreaLevel2")
    area_l3 = _pick_col(df, "AreaLevel3")
    area_l4 = _pick_col(df, "AreaLevel4")
    area_path = _pick_col(df, "AreaPath")
    project_name = _pick_col(df, "Project", "ProjectName", "TeamProject", "ADOProject")

    def _clean(series: pd.Series) -> pd.Series:
        if series is None or series.empty:
            return pd.Series([None] * len(df), index=df.index, dtype="object")
        cleaned = series.astype(str).str.strip()
        cleaned = cleaned.replace({"": None, "nan": None, "None": None})
        return cleaned.reindex(df.index, fill_value=None)

    team_direct = _clean(team_direct)
    program_raw = _clean(program_raw)
    area_l3 = _clean(area_l3)
    area_l4 = _clean(area_l4)
    area_path = _clean(area_path)
    project_name = _clean(project_name)

    def _parse_area_path(path_val: Any, project_val: Any) -> list[str]:
        if path_val is None:
            return []
        sval = str(path_val).strip()
        if not sval or sval.lower() in {"nan", "none"}:
            return []
        parts = [p.strip() for p in sval.split("\\") if p and str(p).strip()]
        if not parts:
            return []
        proj = str(project_val).strip() if project_val is not None else ""
        if proj and parts and parts[0].strip().lower() == proj.strip().lower():
            parts = parts[1:]
        return parts

    hier_list: list[list[str]] = []
    for pval, proj in zip(area_path.tolist(), project_name.tolist()):
        hier_list.append(_parse_area_path(pval, proj))

    def _hier_get(hier: list[str], idx: int) -> Optional[str]:
        if idx < 0 or idx >= len(hier):
            return None
        return hier[idx]

    def _team_from_hier(hier: list[str]) -> Optional[str]:
        if len(hier) >= 2:
            return hier[-1]
        if len(hier) == 1:
            return hier[0]
        return None

    def _variant_key_from_hier(hier: list[str]) -> Optional[str]:
        parts = [str(seg).strip() for seg in hier if seg is not None and str(seg).strip()]
        if not parts:
            return None
        if len(parts) >= 2 and parts[-1].strip().upper() == parts[-2].strip().upper():
            parts = parts[:-1]
        parts = [p.strip().upper() for p in parts if p and str(p).strip()]
        return "|".join(parts) if parts else None

    hier_program = [(_hier_get(h, 0)) for h in hier_list]
    hier_team = [(_team_from_hier(h)) for h in hier_list]
    hier_area2 = [(_hier_get(h, 0)) for h in hier_list]
    hier_area3 = [(_hier_get(h, 1)) for h in hier_list]
    hier_area4 = [(_hier_get(h, 2)) for h in hier_list]
    hier_key = [(_variant_key_from_hier(h)) for h in hier_list]
    hier_used = [len(h) > 0 for h in hier_list]

    # Fallback precedence: Area Level 4 (most specific), then Level 3 when distinct, then explicit Team column
    team_combined = area_l4.copy()
    if team_combined.empty:
        team_combined = pd.Series([None] * len(df), index=df.index, dtype="object")
    if not area_l3.empty:
        area_l3_adj = area_l3.where(area_l3 != program_raw)
        team_combined = team_combined.where(team_combined.notna(), area_l3_adj)
    if not team_direct.empty:
        team_combined = team_combined.where(team_combined.notna(), team_direct)
    if not program_raw.empty:
        team_combined = team_combined.where(team_combined.notna(), program_raw)
    if not area_path.empty:
        team_combined = team_combined.where(team_combined.notna(), area_path)

    team_from_hier = pd.Series(hier_team, index=df.index, dtype="object")
    program_from_hier = pd.Series(hier_program, index=df.index, dtype="object")
    area2_from_hier = pd.Series(hier_area2, index=df.index, dtype="object")
    area3_from_hier = pd.Series(hier_area3, index=df.index, dtype="object")
    area4_from_hier = pd.Series(hier_area4, index=df.index, dtype="object")
    hier_used_series = pd.Series(hier_used, index=df.index, dtype=bool)

    out["TEAM_RAW"]       = team_from_hier.where(hier_used_series, team_combined)
    out["PROGRAM_RAW"]    = program_from_hier.where(hier_used_series, program_raw)
    out["AREA_LEVEL2_RAW"] = area2_from_hier.where(hier_used_series, program_raw)
    out["AREA_LEVEL3_RAW"] = area3_from_hier.where(hier_used_series, area_l3)
    out["AREA_LEVEL4_RAW"] = area4_from_hier.where(hier_used_series, area_l4)
    out["AREA_PATH_RAW"]  = area_path

    out["APP_NAME_RAW"]   = _pick_col(df, "Custom_ApplicationName")
    out["INVESTMENT_DIMENSION"] = _pick_col(df, "Custom_InvestmentDimension")
    out["BUSINESS_VALUE"] = _pick_col(df, "BusinessValue")
    # SWAG input: use Effort as the SWAG points source; fall back to StoryPoints when needed.
    effort = _pick_col(df, "Effort")
    story = _pick_col(df, "StoryPoints")
    out["STORY_POINTS"] = effort if not effort.empty else story
    if (not effort.empty) and (not story.empty):
        try:
            out["STORY_POINTS"] = out["STORY_POINTS"].where(out["STORY_POINTS"].notna(), story)
        except Exception:
            pass
    out["ITERATION_PATH"] = _pick_col(df, "Iteration")
    out["ITERATION_LEVEL3"] = _pick_col(df, "IterationLevel3")
    out["ITERATION_LEVEL3_RAW"] = out["ITERATION_LEVEL3"]
    out["ITERATION_SK"] = _pick_col(df, "IterationSK")
    out["CREATED_AT"]     = _pick_col(df, "CreatedDate")
    out["CHANGED_AT"]     = _pick_col(df, "ChangedDate")
    out["ADO_YEAR"]       = _pick_col(df, "Year")

    # Final cleanup for textual columns
    for col in ["FEATURE_ID", "TITLE", "STATE", "TEAM_RAW", "APP_NAME_RAW", "INVESTMENT_DIMENSION",
               "ITERATION_LEVEL3", "ITERATION_LEVEL3_RAW", "ITERATION_PATH", "ITERATION_SK",
               "PROGRAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW", "AREA_PATH_RAW"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()
            out[col] = out[col].replace({"": None, "nan": None, "None": None})

    # Normalize PI label from IterationLevel3 (strip any trailing " (..)" suffix).
    if "ITERATION_LEVEL3_RAW" in out.columns:
        raw = out["ITERATION_LEVEL3_RAW"].fillna("").astype(str)
        pi = raw.map(normalize_pi_key)
        pi = pi.replace({"": None, "nan": None, "None": None})
        out["PI_KEY"] = pi
        out["PI_LABEL"] = pi

    # Keep ITERATION_SK joinable to ADO_ITERATION_CALENDAR.ITERATION_SK (BIGINT) when possible.
    if "ITERATION_SK" in out.columns and not out["ITERATION_SK"].empty:
        sk_num = pd.to_numeric(out["ITERATION_SK"], errors="coerce")
        mask = sk_num.notna()
        if mask.any():
            try:
                out.loc[mask, "ITERATION_SK"] = sk_num.loc[mask].astype("Int64").astype(str)
            except Exception:
                out.loc[mask, "ITERATION_SK"] = sk_num.loc[mask].astype(str)
        out["ITERATION_SK"] = out["ITERATION_SK"].replace({"": None, "nan": None, "None": None, "<NA>": None})

    def _compose_variant_key(row: pd.Series) -> Optional[str]:
        def _clean(val: Any) -> Optional[str]:
            if val is None:
                return None
            if isinstance(val, str):
                sval = val.strip()
                return sval if sval else None
            sval = str(val).strip()
            return sval if sval else None

        program = _clean(row.get("PROGRAM_RAW"))
        area3 = _clean(row.get("AREA_LEVEL3_RAW"))
        area4 = _clean(row.get("AREA_LEVEL4_RAW"))
        team = _clean(row.get("TEAM_RAW"))

        if team is None:
            leaf_team = area4
        elif area4 is None:
            leaf_team = team
        elif (team or "").upper() == (area3 or "").upper():
            leaf_team = area4
        else:
            leaf_team = team

        parts = [program, area3, area4, leaf_team]
        parts = [p.upper() for p in parts if p]
        return "|".join(parts) if parts else None

    hier_key_series = pd.Series(hier_key, index=df.index, dtype="object")
    fallback_key = out.apply(_compose_variant_key, axis=1)
    out["TEAM_VARIANT_KEY"] = hier_key_series.where(hier_used_series, fallback_key)

    # Demand driver is Derived FTE (SWAG) computed downstream; do not normalize points/manual overrides here.
    out["BUSINESS_VALUE"] = pd.to_numeric(out["BUSINESS_VALUE"], errors="coerce")
    out["STORY_POINTS"] = pd.to_numeric(out.get("STORY_POINTS"), errors="coerce")
    out["CREATED_AT"]     = pd.to_datetime(out["CREATED_AT"], errors="coerce")
    out["CHANGED_AT"]     = pd.to_datetime(out["CHANGED_AT"], errors="coerce")
    if "ADO_YEAR" not in out.columns:
        out["ADO_YEAR"] = pd.Series(pd.NA, index=df.index, dtype="Int64")
    try:
        out["ADO_YEAR"] = pd.to_numeric(out["ADO_YEAR"], errors="coerce").astype("Int64")
    except Exception:
        out["ADO_YEAR"] = pd.Series(pd.NA, index=df.index, dtype="Int64")

    # Derive missing year values from iteration strings when explicit year data is absent.
    if "ITERATION_PATH" in out.columns and not out["ITERATION_PATH"].empty:
        missing_year = out["ADO_YEAR"].isna()
        if missing_year.any():
            iter_series = out.loc[missing_year, "ITERATION_PATH"].fillna("").astype(str)
            extracted = iter_series.str.extract(r"(?<!\d)(\d{4})(?!\d)", expand=False)
            inferred = pd.to_numeric(extracted, errors="coerce")
            out.loc[missing_year, "ADO_YEAR"] = inferred.values

    try:
        out["ADO_YEAR"] = pd.to_numeric(out["ADO_YEAR"], errors="coerce").astype("Int64")
    except Exception:
        pass
    out = out[out["FEATURE_ID"].notna()].copy()
    return out.reset_index(drop=True)


def repair_leaf_teams(df: pd.DataFrame) -> pd.DataFrame:
    """Repair TEAM_RAW and TEAM_VARIANT_KEY from AREA_PATH_RAW leaf hierarchy."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df

    out = df.copy()
    if "AREA_PATH_RAW" not in out.columns:
        return out

    project_hint_col = None
    for col in ("PROJECT_NAME", "PROJECT", "ADO_PROJECT", "TEAMPROJECT"):
        if col in out.columns:
            project_hint_col = col
            break

    def _clean_segment(val: Any) -> Optional[str]:
        if val is None:
            return None
        s = str(val).strip()
        if not s or s.lower() in {"nan", "none", "<na>"}:
            return None
        return s

    def _parse_hier(path_val: Any, project_hint: Any) -> List[str]:
        raw = _clean_segment(path_val)
        if raw is None:
            return []
        parts = [_clean_segment(p) for p in raw.split("\\")]
        parts = [p for p in parts if p]
        if not parts:
            return []

        root = parts[0]
        hint = _clean_segment(project_hint)
        root_upper = root.upper()
        if root_upper.startswith("PORTFOLIO-"):
            parts = parts[1:]
        elif hint and root_upper == hint.upper():
            parts = parts[1:]
        return parts

    repaired_team: List[Optional[str]] = []
    repaired_key: List[Optional[str]] = []
    for idx, row in out.iterrows():
        area_path = row.get("AREA_PATH_RAW")
        project_hint = row.get(project_hint_col) if project_hint_col else None
        hier = _parse_hier(area_path, project_hint)
        if not hier:
            repaired_team.append(row.get("TEAM_RAW"))
            repaired_key.append(row.get("TEAM_VARIANT_KEY"))
            continue
        team_raw = hier[-1] if len(hier) >= 2 else hier[0]
        variant_parts = [str(seg).upper().strip() for seg in hier if str(seg).strip()]
        if len(variant_parts) >= 2 and variant_parts[-1] == variant_parts[-2]:
            variant_parts = variant_parts[:-1]
        repaired_team.append(team_raw)
        repaired_key.append("|".join(variant_parts) if variant_parts else None)

    out["TEAM_RAW"] = pd.Series(repaired_team, index=out.index, dtype="object")
    out["TEAM_VARIANT_KEY"] = pd.Series(repaired_key, index=out.index, dtype="object")
    return out


def _debug_team_key_stats(df: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {"dup_tail_pct": None, "top_team_raw": []}
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return out
    try:
        keys = df.get("TEAM_VARIANT_KEY")
        if keys is not None:
            parts = keys.fillna("").astype(str).str.split("|")
            tail_dup = parts.map(
                lambda arr: len(arr) >= 2 and arr[-1].strip().upper() == arr[-2].strip().upper()
            )
            pct = float(tail_dup.mean() * 100.0) if len(tail_dup) else 0.0
            out["dup_tail_pct"] = round(pct, 3)
    except Exception:
        out["dup_tail_pct"] = None
    try:
        team_raw = df.get("TEAM_RAW")
        if team_raw is not None:
            counts = team_raw.fillna("").astype(str).str.strip()
            counts = counts[counts != ""]
            top = counts.value_counts().head(20)
            out["top_team_raw"] = [{"team": k, "count": int(v)} for k, v in top.to_dict().items()]
    except Exception:
        out["top_team_raw"] = []
    return out


__all__ = [
    "EXPECTED_RAW_COLUMNS",
    "missing_expected_columns",
    "fetch_ado_odata",
    "build_ado_iterations_odata_url",
    "fetch_ado_iterations_odata",
    "transform_ado_odata_to_expected",
    "normalize_to_canonical",
    "repair_leaf_teams",
]
