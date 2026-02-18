from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Callable, Iterable, Optional
import uuid

import pandas as pd

from core.text import normalize_app_name


FetchFn = Callable[[str, Optional[Iterable[Any]]], pd.DataFrame]
ExecuteFn = Callable[[str, Optional[Iterable[Any]], bool], Any]
UpsertVendorFn = Callable[..., Any]
UpsertGroupFn = Callable[..., Any]
MapAdoAppFn = Callable[[Iterable[str], str], Any]
EnsureDefaultsFn = Callable[[ExecuteFn, FetchFn], Any]


@dataclass(frozen=True)
class NextAppCandidateRow:
    ado_app_raw: str
    next_app_display_name: str
    vendor: str
    is_base_pool: bool
    create_new_app: bool
    is_existing_app: bool
    existing_group_id: Optional[str]
    existing_group_name: Optional[str]
    needs_vendor: bool
    suggested_vendor_id: Optional[str]
    suggested_vendor_name: Optional[str]
    suggested_base_pool: bool
    feature_count: int
    effort_points_sum: float
    status: str
    suggested_team: Optional[str]
    suggested_team_id: Optional[str]


@dataclass(frozen=True)
class OnboardResult:
    mapped_rows: int
    created_groups: int
    reused_groups: int
    created_vendors: int
    skipped_rows: int
    errors: list[dict[str, Any]]


def _clean_str(val: Any) -> str:
    return str(val or "").strip()


def _norm_key(val: Any) -> str:
    return _clean_str(val).upper()


def _as_bool(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(int(val))
    s = _clean_str(val).lower()
    return s in {"1", "true", "yes", "y", "on"}


def _similarity_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return float(100.0 * SequenceMatcher(None, a, b).ratio())


def _list_values(vals: Optional[Iterable[Any]]) -> list[str]:
    if vals is None:
        return []
    out = []
    for v in vals:
        s = _clean_str(v)
        if s:
            out.append(s)
    return sorted(set(out))


def _series_or_default(df: pd.DataFrame, col: str, default: Any) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df.index), index=df.index)


def suggest_vendor_from_history(
    fetch_df: FetchFn,
    app_name: str,
    *,
    threshold: int = 90,
) -> tuple[Optional[str], Optional[str], float]:
    target = normalize_app_name(app_name)
    if not target:
        return None, None, 0.0
    try:
        hist = fetch_df(
            """
            SELECT
              g.GROUPNAME,
              g.DEFAULT_VENDORID AS VENDORID,
              v.VENDORNAME
            FROM APPLICATION_GROUPS g
            LEFT JOIN VENDORS v ON v.VENDORID = g.DEFAULT_VENDORID
            WHERE g.GROUPNAME IS NOT NULL
            """,
            None,
        )
    except Exception:
        hist = pd.DataFrame()
    hist = hist.copy() if isinstance(hist, pd.DataFrame) else pd.DataFrame()
    if hist.empty:
        return None, None, 0.0

    best_vendor_id = None
    best_vendor_name = None
    best_score = 0.0
    for _, r in hist.iterrows():
        group_name = _clean_str(r.get("GROUPNAME"))
        vendor_id = _clean_str(r.get("VENDORID"))
        vendor_name = _clean_str(r.get("VENDORNAME"))
        if not vendor_id:
            continue
        score = _similarity_score(target, normalize_app_name(group_name))
        if score > best_score:
            best_score = score
            best_vendor_id = vendor_id
            best_vendor_name = vendor_name or None
    if best_score < float(threshold):
        return None, None, float(best_score)
    return best_vendor_id, best_vendor_name, float(best_score)


def build_next_app_candidates_df(
    fetch_df: FetchFn,
    *,
    years: Optional[Iterable[Any]] = None,
    programs: Optional[Iterable[str]] = None,
    teams: Optional[Iterable[str]] = None,
    ado_program_keys: Optional[Iterable[str]] = None,
    ado_team_keys: Optional[Iterable[str]] = None,
    include_mapped: bool = True,
    fuzzy_threshold: int = 92,
) -> pd.DataFrame:
    years_i = []
    for y in _list_values(years):
        try:
            years_i.append(int(y))
        except Exception:
            continue
    programs_l = _list_values(programs)
    teams_l = _list_values(teams)
    ado_program_keys_l = [p.upper() for p in _list_values(ado_program_keys)]
    ado_team_keys_l = [t.upper() for t in _list_values(ado_team_keys)]

    try:
        cands = fetch_df("SELECT ADO_APP_RAW, FEATURE_COUNT, FIRST_SEEN, LAST_SEEN FROM ADO_APP_CANDIDATES", None)
    except Exception:
        cands = pd.DataFrame(columns=["ADO_APP_RAW", "FEATURE_COUNT", "FIRST_SEEN", "LAST_SEEN"])
    cands = cands.copy() if isinstance(cands, pd.DataFrame) else pd.DataFrame(columns=["ADO_APP_RAW", "FEATURE_COUNT", "FIRST_SEEN", "LAST_SEEN"])
    for col in ["ADO_APP_RAW", "FEATURE_COUNT", "FIRST_SEEN", "LAST_SEEN"]:
        if col not in cands.columns:
            cands[col] = None
    cands["ADO_APP_RAW"] = cands["ADO_APP_RAW"].astype(str).str.strip()
    cands = cands[cands["ADO_APP_RAW"] != ""].copy()
    cands["FEATURE_COUNT"] = pd.to_numeric(cands["FEATURE_COUNT"], errors="coerce").fillna(0).astype(int)
    cands["LAST_SEEN"] = pd.to_datetime(cands["LAST_SEEN"], errors="coerce")

    where = ["af.APP_NAME_RAW IS NOT NULL", "LTRIM(RTRIM(af.APP_NAME_RAW)) <> ''"]
    params: list[Any] = []
    if years_i:
        where.append("TRY_CONVERT(INT, af.ADO_YEAR) IN (" + ", ".join(["%s"] * len(years_i)) + ")")
        params.extend(years_i)
    if programs_l:
        where.append("UPPER(LTRIM(RTRIM(COALESCE(af.PROGRAM_RAW, '')))) IN (" + ", ".join(["%s"] * len(programs_l)) + ")")
        params.extend([p.upper() for p in programs_l])
    if teams_l:
        where.append("UPPER(LTRIM(RTRIM(COALESCE(af.TEAM_RAW, '')))) IN (" + ", ".join(["%s"] * len(teams_l)) + ")")
        params.extend([t.upper() for t in teams_l])
    if ado_program_keys_l:
        prog_ph = ", ".join(["%s"] * len(ado_program_keys_l))
        prog_prefix_expr = " OR ".join(
            ["UPPER(LTRIM(RTRIM(COALESCE(af.TEAM_VARIANT_KEY, '')))) LIKE %s" for _ in ado_program_keys_l]
        )
        where.append(
            "("
            f"UPPER(LTRIM(RTRIM(COALESCE(af.PROGRAM_RAW, af.AREA_LEVEL2_RAW, '')))) IN ({prog_ph}) "
            f"OR UPPER(LTRIM(RTRIM(COALESCE(af.TEAM_VARIANT_KEY, '')))) IN ({prog_ph}) "
            f"OR {prog_prefix_expr}"
            ")"
        )
        params.extend(ado_program_keys_l)
        params.extend(ado_program_keys_l)
        params.extend([f"{k}|%" for k in ado_program_keys_l])
    if ado_team_keys_l:
        team_ph = ", ".join(["%s"] * len(ado_team_keys_l))
        team_prefix_expr = " OR ".join(
            ["UPPER(LTRIM(RTRIM(COALESCE(af.TEAM_VARIANT_KEY, '')))) LIKE %s" for _ in ado_team_keys_l]
        )
        where.append(
            "("
            f"UPPER(LTRIM(RTRIM(COALESCE(af.TEAM_VARIANT_KEY, '')))) IN ({team_ph}) "
            f"OR {team_prefix_expr}"
            ")"
        )
        params.extend(ado_team_keys_l)
        params.extend([f"{k}|%" for k in ado_team_keys_l])
    where_sql = " WHERE " + " AND ".join(where)

    try:
        effort_df = fetch_df(
            f"""
            SELECT
              af.APP_NAME_RAW AS ADO_APP_RAW,
              COUNT(DISTINCT af.FEATURE_ID) AS FEATURE_COUNT_FEATURES,
              /* Profile-normalized points live in STORY_POINTS; keep fallback for legacy rows. */
              SUM(COALESCE(TRY_CONVERT(FLOAT, af.STORY_POINTS), TRY_CONVERT(FLOAT, af.EFFORT_POINTS), 0.0)) AS EFFORT_POINTS_SUM,
              MAX(COALESCE(TRY_CONVERT(DATETIME2, af.CHANGED_AT), TRY_CONVERT(DATETIME2, af.CREATED_AT))) AS LAST_SEEN_FEATURES
            FROM ADO_FEATURES af
            {where_sql}
            GROUP BY af.APP_NAME_RAW
            """,
            tuple(params) if params else None,
        )
    except Exception:
        effort_df = pd.DataFrame(columns=["ADO_APP_RAW", "FEATURE_COUNT_FEATURES", "EFFORT_POINTS_SUM", "LAST_SEEN_FEATURES"])
    effort_df = effort_df.copy() if isinstance(effort_df, pd.DataFrame) else pd.DataFrame(columns=["ADO_APP_RAW", "FEATURE_COUNT_FEATURES", "EFFORT_POINTS_SUM", "LAST_SEEN_FEATURES"])
    if "ADO_APP_RAW" in effort_df.columns:
        effort_df["ADO_APP_RAW"] = effort_df["ADO_APP_RAW"].astype(str).str.strip()
        effort_df = effort_df[effort_df["ADO_APP_RAW"] != ""].copy()
    effort_df["EFFORT_POINTS_SUM"] = pd.to_numeric(effort_df.get("EFFORT_POINTS_SUM"), errors="coerce").fillna(0.0)
    effort_df["FEATURE_COUNT_FEATURES"] = pd.to_numeric(effort_df.get("FEATURE_COUNT_FEATURES"), errors="coerce").fillna(0).astype(int)
    effort_df["LAST_SEEN_FEATURES"] = pd.to_datetime(effort_df.get("LAST_SEEN_FEATURES"), errors="coerce")

    try:
        team_sugg = fetch_df(
            f"""
            WITH team_effort AS (
              SELECT
                af.APP_NAME_RAW AS ADO_APP_RAW,
                LTRIM(RTRIM(COALESCE(af.TEAM_RAW, ''))) AS TEAM_RAW,
                /* Keep team ranking aligned with the same points source as aggregate demand. */
                SUM(COALESCE(TRY_CONVERT(FLOAT, af.STORY_POINTS), TRY_CONVERT(FLOAT, af.EFFORT_POINTS), 0.0)) AS EFFORT_POINTS_SUM
              FROM ADO_FEATURES af
              {where_sql}
              GROUP BY af.APP_NAME_RAW, LTRIM(RTRIM(COALESCE(af.TEAM_RAW, '')))
            ),
            ranked AS (
              SELECT
                ADO_APP_RAW,
                TEAM_RAW,
                EFFORT_POINTS_SUM,
                ROW_NUMBER() OVER (PARTITION BY ADO_APP_RAW ORDER BY EFFORT_POINTS_SUM DESC, TEAM_RAW ASC) AS RN
              FROM team_effort
            )
            SELECT ADO_APP_RAW, TEAM_RAW AS SUGGESTED_TEAM
            FROM ranked
            WHERE RN = 1
            """,
            tuple(params) if params else None,
        )
    except Exception:
        team_sugg = pd.DataFrame(columns=["ADO_APP_RAW", "SUGGESTED_TEAM"])
    team_sugg = team_sugg.copy() if isinstance(team_sugg, pd.DataFrame) else pd.DataFrame(columns=["ADO_APP_RAW", "SUGGESTED_TEAM"])
    team_sugg["ADO_APP_RAW"] = _series_or_default(team_sugg, "ADO_APP_RAW", "").astype(str).str.strip()
    team_sugg["SUGGESTED_TEAM"] = _series_or_default(team_sugg, "SUGGESTED_TEAM", "").astype(str).str.strip()

    try:
        maps = fetch_df(
            """
            SELECT
              ma.ADO_APP,
              ma.APP_GROUP,
              g.GROUPNAME,
              g.TEAMID,
              g.IS_BASE,
              g.DEFAULT_VENDORID,
              v.VENDORNAME
            FROM MAP_ADO_APP_TO_TCO_GROUP ma
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = ma.APP_GROUP
            LEFT JOIN VENDORS v ON v.VENDORID = g.DEFAULT_VENDORID
            """,
            None,
        )
    except Exception:
        maps = pd.DataFrame(columns=["ADO_APP", "APP_GROUP", "GROUPNAME", "TEAMID", "IS_BASE", "DEFAULT_VENDORID", "VENDORNAME"])
    maps = maps.copy() if isinstance(maps, pd.DataFrame) else pd.DataFrame(columns=["ADO_APP", "APP_GROUP", "GROUPNAME", "TEAMID", "IS_BASE", "DEFAULT_VENDORID", "VENDORNAME"])
    maps["ADO_APP"] = _series_or_default(maps, "ADO_APP", "").astype(str).str.strip()
    maps["ADO_APP_KEY"] = maps["ADO_APP"].astype(str).str.upper()
    maps["APP_GROUP"] = _series_or_default(maps, "APP_GROUP", "").astype(str).str.strip()
    maps["GROUPNAME"] = _series_or_default(maps, "GROUPNAME", "").astype(str).str.strip()
    maps["TEAMID"] = _series_or_default(maps, "TEAMID", "").astype(str).str.strip()
    maps["VENDORNAME"] = _series_or_default(maps, "VENDORNAME", "").astype(str).str.strip()
    maps["DEFAULT_VENDORID"] = _series_or_default(maps, "DEFAULT_VENDORID", "").astype(str).str.strip()

    scoped_by_filters = bool(years_i or programs_l or teams_l or ado_program_keys_l or ado_team_keys_l)
    if scoped_by_filters:
        scoped_apps = set(effort_df.get("ADO_APP_RAW", pd.Series(dtype=str)).dropna().astype(str).str.strip().str.upper().tolist())
        if scoped_apps:
            cands = cands[cands["ADO_APP_RAW"].astype(str).str.upper().isin(scoped_apps)].copy()
            maps = maps[maps["ADO_APP_KEY"].astype(str).str.upper().isin(scoped_apps)].copy()
        else:
            cands = cands.iloc[0:0].copy()
            maps = maps.iloc[0:0].copy()

    try:
        teams_df = fetch_df(
            """
            SELECT
              TEAMID,
              LTRIM(RTRIM(COALESCE(NULLIF(TEAM_DISPLAY_NAME, ''), TEAMNAME))) AS TEAMNAME
            FROM TEAMS
            """,
            None,
        )
    except Exception:
        teams_df = pd.DataFrame(columns=["TEAMID", "TEAMNAME"])
    teams_df = teams_df.copy() if isinstance(teams_df, pd.DataFrame) else pd.DataFrame(columns=["TEAMID", "TEAMNAME"])
    team_name_to_id = {
        _norm_key(r.get("TEAMNAME")): _clean_str(r.get("TEAMID"))
        for _, r in teams_df.iterrows()
        if _clean_str(r.get("TEAMID"))
    }

    try:
        base_df = fetch_df(
            "SELECT TEAMID FROM APPLICATION_GROUPS WHERE IS_BASE = 1 AND TEAMID IS NOT NULL",
            None,
        )
    except Exception:
        base_df = pd.DataFrame(columns=["TEAMID"])
    base_team_ids = set(base_df.get("TEAMID", pd.Series(dtype=str)).dropna().astype(str).str.strip().tolist())

    apps = set(cands["ADO_APP_RAW"].tolist()) | set(effort_df.get("ADO_APP_RAW", pd.Series(dtype=str)).dropna().astype(str).str.strip().tolist())
    if include_mapped:
        apps |= set(maps.get("ADO_APP", pd.Series(dtype=str)).dropna().astype(str).str.strip().tolist())
    app_values = sorted([a for a in apps if a])
    if not app_values:
        return pd.DataFrame(
            columns=[
                "ADO_APP_RAW", "next_app_display_name", "vendor", "is_base_pool", "create_new_app", "is_existing_app",
                "existing_group_id", "existing_group_name", "needs_vendor", "suggested_vendor_id", "suggested_vendor_name",
                "suggested_base_pool", "feature_count", "effort_points_sum", "status", "suggested_team", "suggested_team_id",
            ]
        )

    merged = pd.DataFrame({"ADO_APP_RAW": app_values})
    merged = merged.merge(cands[["ADO_APP_RAW", "FEATURE_COUNT", "LAST_SEEN"]], on="ADO_APP_RAW", how="left")
    merged = merged.merge(effort_df[["ADO_APP_RAW", "FEATURE_COUNT_FEATURES", "EFFORT_POINTS_SUM", "LAST_SEEN_FEATURES"]], on="ADO_APP_RAW", how="left")
    merged = merged.merge(team_sugg[["ADO_APP_RAW", "SUGGESTED_TEAM"]], on="ADO_APP_RAW", how="left")
    merged["feature_count"] = pd.to_numeric(merged["FEATURE_COUNT"], errors="coerce").fillna(
        pd.to_numeric(merged["FEATURE_COUNT_FEATURES"], errors="coerce").fillna(0)
    ).astype(int)
    merged["effort_points_sum"] = pd.to_numeric(merged["EFFORT_POINTS_SUM"], errors="coerce").fillna(0.0)
    merged["last_seen"] = pd.to_datetime(merged["LAST_SEEN"], errors="coerce").fillna(pd.to_datetime(merged["LAST_SEEN_FEATURES"], errors="coerce"))
    merged["ADO_APP_KEY"] = merged["ADO_APP_RAW"].astype(str).str.upper()

    map_lookup = {k: g for k, g in maps.groupby("ADO_APP_KEY", dropna=False)}
    rows: list[dict[str, Any]] = []
    for _, r in merged.iterrows():
        ado_raw = _clean_str(r.get("ADO_APP_RAW"))
        if not ado_raw:
            continue
        map_rows = map_lookup.get(_norm_key(ado_raw))
        existing_group_id = None
        existing_group_name = None
        suggested_vendor_id = None
        suggested_vendor_name = None
        mapped = False
        team_id = None
        if map_rows is not None and not map_rows.empty:
            top = map_rows.iloc[0]
            existing_group_id = _clean_str(top.get("APP_GROUP")) or None
            existing_group_name = _clean_str(top.get("GROUPNAME")) or None
            suggested_vendor_id = _clean_str(top.get("DEFAULT_VENDORID")) or None
            suggested_vendor_name = _clean_str(top.get("VENDORNAME")) or None
            team_id = _clean_str(top.get("TEAMID")) or None
            mapped = bool(existing_group_id)

        suggested_team = _clean_str(r.get("SUGGESTED_TEAM")) or None
        if not team_id and suggested_team:
            team_id = team_name_to_id.get(_norm_key(suggested_team)) or None

        display_name = existing_group_name or ado_raw
        if not suggested_vendor_id:
            vid, vname, _score = suggest_vendor_from_history(fetch_df, display_name or ado_raw, threshold=fuzzy_threshold)
            suggested_vendor_id = suggested_vendor_id or vid
            suggested_vendor_name = suggested_vendor_name or vname

        lower_norm = normalize_app_name(display_name or ado_raw)
        keyword_base = any(tok in lower_norm for tok in ("base", "unplanned", "shared", "pool"))
        suggested_base_pool = keyword_base or (bool(team_id) and str(team_id) not in base_team_ids)
        create_new = not mapped
        vendor_name = suggested_vendor_name or ""
        needs_vendor = bool(create_new and not vendor_name)
        rows.append(
            {
                "ADO_APP_RAW": ado_raw,
                "next_app_display_name": display_name,
                "vendor": vendor_name,
                # UX default: never pre-select BASE; users must opt in explicitly.
                "is_base_pool": False,
                "create_new_app": bool(create_new),
                "is_existing_app": bool(mapped),
                "existing_group_id": existing_group_id,
                "existing_group_name": existing_group_name,
                "needs_vendor": bool(needs_vendor),
                "suggested_vendor_id": suggested_vendor_id,
                "suggested_vendor_name": suggested_vendor_name,
                "suggested_base_pool": bool(suggested_base_pool),
                "feature_count": int(r.get("feature_count") or 0),
                "effort_points_sum": float(r.get("effort_points_sum") or 0.0),
                "status": "Mapped" if mapped else "Unmapped",
                "suggested_team": suggested_team,
                "suggested_team_id": team_id,
                "last_seen": r.get("last_seen"),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["__status_order"] = out["status"].map({"Unmapped": 0, "Mapped": 1}).fillna(1).astype(int)
    out = out.sort_values(["__status_order", "effort_points_sum"], ascending=[True, False], na_position="last").reset_index(drop=True)
    out.drop(columns=["__status_order"], inplace=True, errors="ignore")
    return out


def safe_onboard_next_apps(
    fetch_df: FetchFn,
    execute: ExecuteFn,
    upsert_vendor_fn: UpsertVendorFn,
    upsert_application_group_fn: UpsertGroupFn,
    map_ado_app_to_group_fn: MapAdoAppFn,
    ensure_default_instances_fn: EnsureDefaultsFn,
    classification_df: pd.DataFrame,
    *,
    updated_by: Optional[str] = None,
) -> OnboardResult:
    rows_df = classification_df.copy() if isinstance(classification_df, pd.DataFrame) else pd.DataFrame()
    if rows_df.empty:
        return OnboardResult(0, 0, 0, 0, 0, [])

    errors: list[dict[str, Any]] = []
    mapped_rows = 0
    created_groups = 0
    reused_groups = 0
    created_vendors = 0

    rows_df = rows_df.reset_index(drop=True)
    rows_df["ADO_APP_RAW"] = _series_or_default(rows_df, "ADO_APP_RAW", "").astype(str).str.strip()
    rows_df["next_app_display_name"] = _series_or_default(rows_df, "next_app_display_name", "").astype(str).str.strip()
    rows_df["vendor"] = _series_or_default(rows_df, "vendor", "").astype(str).str.strip()
    rows_df["create_new_app"] = _series_or_default(rows_df, "create_new_app", False).apply(_as_bool)
    rows_df["is_base_pool"] = _series_or_default(rows_df, "is_base_pool", False).apply(_as_bool)
    rows_df["existing_group_id"] = _series_or_default(rows_df, "existing_group_id", "").astype(str).str.strip()
    rows_df["suggested_team_id"] = _series_or_default(rows_df, "suggested_team_id", "").astype(str).str.strip()

    # Guardrail: duplicate target names in payload when creating.
    create_rows = rows_df[rows_df["create_new_app"]]
    name_counts = create_rows["next_app_display_name"].str.upper().value_counts()
    dup_names = set(name_counts[name_counts > 1].index.tolist())

    vendors_df = fetch_df("SELECT VENDORID, VENDORNAME FROM VENDORS", None)
    vendors_df = vendors_df.copy() if isinstance(vendors_df, pd.DataFrame) else pd.DataFrame(columns=["VENDORID", "VENDORNAME"])
    vendors_df["VENDORID"] = _series_or_default(vendors_df, "VENDORID", "").astype(str).str.strip()
    vendors_df["VENDORNAME"] = _series_or_default(vendors_df, "VENDORNAME", "").astype(str).str.strip()

    groups_df = fetch_df("SELECT GROUPID, GROUPNAME, TEAMID FROM APPLICATION_GROUPS", None)
    groups_df = groups_df.copy() if isinstance(groups_df, pd.DataFrame) else pd.DataFrame(columns=["GROUPID", "GROUPNAME", "TEAMID"])
    groups_df["GROUPID"] = _series_or_default(groups_df, "GROUPID", "").astype(str).str.strip()
    groups_df["GROUPNAME"] = _series_or_default(groups_df, "GROUPNAME", "").astype(str).str.strip()
    groups_df["TEAMID"] = _series_or_default(groups_df, "TEAMID", "").astype(str).str.strip()
    existing_group_names = set(groups_df["GROUPNAME"].astype(str).str.upper().tolist())

    for idx, row in rows_df.iterrows():
        ado_raw = _clean_str(row.get("ADO_APP_RAW"))
        display_name = _clean_str(row.get("next_app_display_name")) or ado_raw
        create_new = bool(row.get("create_new_app"))
        vendor_name = _clean_str(row.get("vendor"))
        is_base = bool(row.get("is_base_pool"))
        suggested_team_id = _clean_str(row.get("suggested_team_id"))
        existing_group_id = _clean_str(row.get("existing_group_id"))

        if not ado_raw:
            errors.append({"row_index": int(idx), "ado_app_raw": "", "error": "ADO_APP_RAW is required."})
            continue
        if create_new and not display_name:
            errors.append({"row_index": int(idx), "ado_app_raw": ado_raw, "error": "next_app_display_name is required for create_new_app."})
            continue
        if create_new and _norm_key(display_name) in dup_names:
            errors.append({"row_index": int(idx), "ado_app_raw": ado_raw, "error": "Duplicate target application name in payload."})
            continue
        if create_new and _norm_key(display_name) in existing_group_names:
            errors.append({"row_index": int(idx), "ado_app_raw": ado_raw, "error": "Target application name already exists."})
            continue
        if create_new and not vendor_name and not is_base:
            errors.append(
                {
                    "row_index": int(idx),
                    "ado_app_raw": ado_raw,
                    "error": "Vendor is required when create_new_app is true unless is_base_pool is selected.",
                }
            )
            continue

        target_group_id = None
        if create_new:
            vendor_id: Optional[str] = None
            if vendor_name:
                vendor_rows = vendors_df.loc[vendors_df["VENDORNAME"].str.upper() == vendor_name.upper()]
                if vendor_rows is None or vendor_rows.empty:
                    vendor_id = str(uuid.uuid4())
                    upsert_vendor_fn(vendor_id, vendor_name, updated_by=updated_by)
                    vendors_df = pd.concat(
                        [
                            vendors_df,
                            pd.DataFrame([{"VENDORID": vendor_id, "VENDORNAME": vendor_name}]),
                        ],
                        ignore_index=True,
                    )
                    created_vendors += 1
                else:
                    vendor_id = _clean_str(vendor_rows.iloc[0].get("VENDORID"))

            group_rows = groups_df.loc[groups_df["GROUPNAME"].str.upper() == display_name.upper()] if display_name else pd.DataFrame()
            if group_rows is not None and not group_rows.empty:
                target_group_id = _clean_str(group_rows.iloc[0].get("GROUPID"))
                reused_groups += 1
            else:
                target_group_id = str(uuid.uuid4())
                owner_email = None
                if suggested_team_id:
                    try:
                        owner_df = fetch_df("SELECT TOP 1 PRODUCTOWNER FROM TEAMS WHERE TEAMID=%s", (suggested_team_id,))
                        if isinstance(owner_df, pd.DataFrame) and not owner_df.empty:
                            owner_email = _clean_str(owner_df.iloc[0].get("PRODUCTOWNER")) or None
                    except Exception:
                        owner_email = None
                upsert_application_group_fn(
                    target_group_id,
                    display_name,
                    team_id=suggested_team_id or "",
                    default_vendor_id=vendor_id,
                    owner=owner_email,
                    is_base=is_base,
                    updated_by=updated_by,
                )
                groups_df = pd.concat(
                    [
                        groups_df,
                        pd.DataFrame([{"GROUPID": target_group_id, "GROUPNAME": display_name, "TEAMID": suggested_team_id or ""}]),
                    ],
                    ignore_index=True,
                )
                created_groups += 1
        else:
            if existing_group_id:
                target_group_id = existing_group_id
            elif display_name:
                group_rows = groups_df.loc[groups_df["GROUPNAME"].str.upper() == display_name.upper()]
                if group_rows is not None and not group_rows.empty:
                    target_group_id = _clean_str(group_rows.iloc[0].get("GROUPID"))
            if not target_group_id:
                errors.append({"row_index": int(idx), "ado_app_raw": ado_raw, "error": "No existing Application target could be resolved."})
                continue

        if not target_group_id:
            errors.append({"row_index": int(idx), "ado_app_raw": ado_raw, "error": "Canonical key APP_GROUP/GROUPID missing."})
            continue

        map_ado_app_to_group_fn([ado_raw], target_group_id)
        mapped_rows += 1

    if mapped_rows > 0:
        # TODO(next-migration): when invoices/contracts migrate to application-level IDs,
        # this onboarding path should stop relying on instance defaults for compatibility.
        ensure_default_instances_fn(execute, fetch_df)

    return OnboardResult(
        mapped_rows=mapped_rows,
        created_groups=created_groups,
        reused_groups=reused_groups,
        created_vendors=created_vendors,
        skipped_rows=len(errors),
        errors=errors,
    )


def get_application_entity(
    fetch_df: FetchFn,
    *,
    team_id: Optional[str] = None,
    program_id: Optional[str] = None,
) -> pd.DataFrame:
    where = []
    params: list[Any] = []
    if _clean_str(team_id):
        where.append("g.TEAMID = %s")
        params.append(_clean_str(team_id))
    if _clean_str(program_id):
        where.append("g.PROGRAMID = %s")
        params.append(_clean_str(program_id))
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    df = fetch_df(
        f"""
        SELECT
          g.GROUPID AS APPLICATION_ID,
          g.GROUPNAME AS APPLICATION_NAME,
          g.TEAMID,
          g.PROGRAMID,
          g.DEFAULT_VENDORID,
          g.IS_BASE,
          d.APPLICATIONID AS DEFAULT_APPLICATION_INSTANCE_ID
        FROM APPLICATION_GROUPS g
        LEFT JOIN APPLICATIONS d
          ON d.GROUPID = g.GROUPID
         AND (
           d.IS_DEFAULT = 1
           OR UPPER(d.APPLICATIONID) = UPPER(CONCAT(g.GROUPID, '__DEFAULT'))
         )
        {where_sql}
        """,
        tuple(params) if params else None,
    )
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


__all__ = [
    "NextAppCandidateRow",
    "OnboardResult",
    "build_next_app_candidates_df",
    "suggest_vendor_from_history",
    "safe_onboard_next_apps",
    "get_application_entity",
]
