from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any, Callable, Iterable, Optional

import pandas as pd

from core.text import normalize_app_name


FetchFn = Callable[[str, Optional[Iterable[Any]]], pd.DataFrame]
ExecFn = Callable[[str, Optional[Iterable[Any]], bool], Any]


@dataclass(frozen=True)
class AdoCandidatesRefreshResult:
    new: int
    updated: int
    need_review: int


def _similarity_score(a: str, b: str) -> int:
    if not a or not b:
        return 0
    return int(round(100 * SequenceMatcher(None, a, b).ratio()))


def _best_fuzzy_match(needle: str, haystack: list[str]) -> tuple[Optional[str], int]:
    if not needle or not haystack:
        return None, 0
    try:
        from rapidfuzz import fuzz, process  # type: ignore

        m = process.extractOne(needle, haystack, scorer=fuzz.QRatio)
        if not m:
            return None, 0
        match, score, _ = m
        return str(match), int(score)
    except Exception:
        best: Optional[str] = None
        best_score = 0
        for h in haystack:
            sc = _similarity_score(needle, h)
            if sc > best_score:
                best_score = sc
                best = h
        return best, best_score


def _coerce_years(years: Optional[Iterable[Any]]) -> Optional[list[int]]:
    if years is None:
        return None
    out: list[int] = []
    for y in years:
        try:
            out.append(int(y))
        except Exception:
            continue
    return sorted(set(out)) if out else None


def refresh_ado_app_candidates(
    fetch_df: FetchFn,
    exec_sql: ExecFn,
    *,
    years: Optional[Iterable[Any]] = None,
    lookback_days: int = 365,
    fuzzy_threshold: int = 92,
) -> AdoCandidatesRefreshResult:
    """Refresh ADO_APP_CANDIDATES from ADO_FEATURES and store mapping suggestions.

    - Reads distinct ADO_FEATURES.APP_NAME_RAW (non-empty).
    - Aggregates first/last seen timestamps, feature counts, and Derived FTE (SWAG demand).
    - Computes a normalized key for matching.
    - Suggests a TCO Application Group (APPLICATION_GROUPS) via exact normalized match or fuzzy match (>= fuzzy_threshold).
    - Does NOT write MAP_ADO_APP_TO_TCO_GROUP; suggestions are stored on the candidate row only.

    Status values (Phase 2):
    - NEW | AUTO_SUGGESTED | NEEDS_REVIEW | IGNORED | MAPPED
    """
    # Ensure table exists (DB facade provides this in Azure SQL deployments).
    try:
        from db import ensure_ado_app_candidates_table  # type: ignore

        ensure_ado_app_candidates_table()
    except Exception:
        pass

    years_i = _coerce_years(years)
    where_base: list[str] = [
        "af.APP_NAME_RAW IS NOT NULL AND LTRIM(RTRIM(af.APP_NAME_RAW)) <> ''",
    ]
    params_base: list[Any] = []
    if years_i:
        where_base.append("af.ADO_YEAR IN (" + ", ".join(["%s"] * len(years_i)) + ")")
        params_base.extend(years_i)
    where_sql = "WHERE " + " AND ".join(where_base)

    cutoff = None
    if lookback_days and int(lookback_days) > 0:
        cutoff = datetime.utcnow() - timedelta(days=int(lookback_days))

    sql_agg = f"""
    WITH dem AS (
      SELECT
        FEATURE_ID,
        TRY_CONVERT(INT, YEAR) AS ADO_YEAR,
        SUM(COALESCE(DERIVED_FTE_FEATURE, 0.0)) AS DERIVED_FTE_SUM
      FROM VW_TCO_FEATURE_DEMAND
      GROUP BY FEATURE_ID, TRY_CONVERT(INT, YEAR)
    )
    SELECT
      af.APP_NAME_RAW AS ADO_APP_RAW,
      MIN(af.CREATED_AT) AS FIRST_SEEN,
      MAX(COALESCE(af.CHANGED_AT, af.CREATED_AT)) AS LAST_SEEN,
      COUNT(DISTINCT af.FEATURE_ID) AS FEATURE_COUNT,
      SUM(COALESCE(dem.DERIVED_FTE_SUM, 0.0)) AS DERIVED_FTE_SUM
    FROM ADO_FEATURES af
    LEFT JOIN dem
      ON dem.FEATURE_ID = af.FEATURE_ID
     AND dem.ADO_YEAR = TRY_CONVERT(INT, af.ADO_YEAR)
    {where_sql}
    GROUP BY af.APP_NAME_RAW
    """
    params_agg: list[Any] = list(params_base)
    if cutoff is not None:
        sql_agg += "\nHAVING MAX(COALESCE(CHANGED_AT, CREATED_AT)) >= %s\n"
        params_agg.append(cutoff)
    df_agg = fetch_df(sql_agg, tuple(params_agg) if params_agg else None)
    if df_agg is None or df_agg.empty:
        return AdoCandidatesRefreshResult(new=0, updated=0, need_review=0)

    df_agg = df_agg.copy()
    df_agg["ADO_APP_RAW"] = df_agg["ADO_APP_RAW"].astype(str).str.strip()
    df_agg = df_agg[df_agg["ADO_APP_RAW"] != ""].copy()
    if df_agg.empty:
        return AdoCandidatesRefreshResult(new=0, updated=0, need_review=0)

    df_prog = fetch_df(
        f"""
        SELECT DISTINCT
          af.APP_NAME_RAW AS ADO_APP_RAW,
          af.PROGRAM_RAW
        FROM ADO_FEATURES af
        {where_sql}
          AND af.PROGRAM_RAW IS NOT NULL AND LTRIM(RTRIM(af.PROGRAM_RAW)) <> ''
        """,
        tuple(params_base) if params_base else None,
    )
    programs_seen: dict[str, list[str]] = {}
    if df_prog is not None and not df_prog.empty and "ADO_APP_RAW" in df_prog.columns:
        tmp = df_prog.copy()
        tmp["ADO_APP_RAW"] = tmp["ADO_APP_RAW"].astype(str).str.strip()
        if "PROGRAM_RAW" in tmp.columns:
            tmp["PROGRAM_RAW"] = tmp["PROGRAM_RAW"].astype(str).str.strip()
            tmp = tmp[(tmp["ADO_APP_RAW"] != "") & (tmp["PROGRAM_RAW"] != "")]
            for app_raw, sub in tmp.groupby("ADO_APP_RAW", dropna=False):
                programs_seen[str(app_raw)] = sorted(set(sub["PROGRAM_RAW"].dropna().astype(str).tolist()))

    df_groups = fetch_df(
        """
        SELECT GROUPID, GROUPNAME
        FROM APPLICATION_GROUPS
        WHERE GROUPID IS NOT NULL AND LTRIM(RTRIM(GROUPID)) <> ''
        """,
        None,
    )
    group_key_to_id: dict[str, str] = {}
    group_id_to_name: dict[str, str] = {}
    if df_groups is not None and not df_groups.empty:
        for _, r in df_groups.iterrows():
            gid = str(r.get("GROUPID") or "").strip()
            gname = str(r.get("GROUPNAME") or "").strip()
            if not gid:
                continue
            group_id_to_name[gid] = gname
            nk = normalize_app_name(gname)
            if nk and nk not in group_key_to_id:
                group_key_to_id[nk] = gid

    df_maps = fetch_df("SELECT ADO_APP, APP_GROUP FROM MAP_ADO_APP_TO_TCO_GROUP", None)
    ado_to_groupid: dict[str, str] = {}
    if df_maps is not None and not df_maps.empty and "ADO_APP" in df_maps.columns:
        for _, r in df_maps.iterrows():
            a = str(r.get("ADO_APP") or "").strip()
            gid = str(r.get("APP_GROUP") or "").strip()
            if a and gid:
                ado_to_groupid[a] = gid

    df_existing = fetch_df(
        """
        SELECT *
        FROM ADO_APP_CANDIDATES
        """,
        None,
    )
    existing_status: dict[str, str] = {}
    existing_suggested: dict[str, tuple[Optional[str], Optional[str]]] = {}
    if df_existing is not None and not df_existing.empty and "ADO_APP_RAW" in df_existing.columns:
        for _, r in df_existing.iterrows():
            a = str(r.get("ADO_APP_RAW") or "").strip()
            if not a:
                continue
            existing_status[a] = str(r.get("STATUS") or "").strip().upper()
            existing_suggested[a] = (
                (
                    str(r.get("SUGGESTED_GROUP_ID") or r.get("SUGGESTED_TCO_GROUP_ID") or "").strip() or None
                ),
                (
                    str(r.get("SUGGESTED_GROUP_NAME") or r.get("SUGGESTED_TCO_GROUP") or "").strip() or None
                ),
            )
    existing_set = set(existing_status.keys())

    group_keys = list(group_key_to_id.keys())

    rows: list[tuple[Any, ...]] = []
    now = datetime.utcnow()
    for _, r in df_agg.iterrows():
        ado_raw = str(r.get("ADO_APP_RAW") or "").strip()
        if not ado_raw:
            continue

        nk = normalize_app_name(ado_raw)
        first_seen = r.get("FIRST_SEEN")
        last_seen = r.get("LAST_SEEN")
        try:
            feature_count = int(pd.to_numeric(r.get("FEATURE_COUNT"), errors="coerce") or 0)
        except Exception:
            feature_count = 0
        try:
            derived_fte_sum = float(pd.to_numeric(r.get("DERIVED_FTE_SUM"), errors="coerce") or 0.0)
        except Exception:
            derived_fte_sum = 0.0
        programs_json = json.dumps(programs_seen.get(ado_raw, [])) if ado_raw in programs_seen else None

        prev_status = existing_status.get(ado_raw, "").upper()
        prev_status = "AUTO_SUGGESTED" if prev_status == "AUTO_MAPPED" else prev_status
        if prev_status == "IGNORED":
            sug_id, sug_name = existing_suggested.get(ado_raw, (None, None))
            status = "IGNORED"
        else:
            # If a real mapping exists already, mark as MAPPED.
            mapped_gid = ado_to_groupid.get(ado_raw)
            if mapped_gid:
                sug_id = mapped_gid
                sug_name = group_id_to_name.get(mapped_gid, "")
                status = "MAPPED"
            else:
                sug_id = None
                sug_name = None
                if nk and nk in group_key_to_id:
                    sug_id = group_key_to_id[nk]
                    sug_name = group_id_to_name.get(sug_id, "")
                    status = "AUTO_SUGGESTED"
                else:
                    match_key, score = _best_fuzzy_match(nk, group_keys)
                    if match_key and score >= int(fuzzy_threshold):
                        sug_id = group_key_to_id.get(match_key)
                        sug_name = group_id_to_name.get(sug_id or "", "")
                        status = "AUTO_SUGGESTED"
                    else:
                        if prev_status in {"NEW", "NEEDS_REVIEW"}:
                            status = prev_status
                        else:
                            status = "NEW" if ado_raw not in existing_set else "NEEDS_REVIEW"

        rows.append(
            (
                ado_raw,
                nk or None,
                first_seen,
                last_seen,
                feature_count,
                derived_fte_sum,
                programs_json,
                sug_id,
                sug_name,
                # Backward compatible aliases
                sug_id,
                sug_name,
                status,
                now,
            )
        )

    merge_sql = """
    MERGE INTO ADO_APP_CANDIDATES t
    USING (
      SELECT
        %s AS ADO_APP_RAW,
        %s AS NORMALIZED_KEY,
        %s AS FIRST_SEEN,
        %s AS LAST_SEEN,
        %s AS FEATURE_COUNT,
        %s AS DERIVED_FTE_SUM,
        %s AS PROGRAMS_SEEN,
        %s AS SUGGESTED_GROUP_ID,
        %s AS SUGGESTED_GROUP_NAME,
        %s AS SUGGESTED_TCO_GROUP_ID,
        %s AS SUGGESTED_TCO_GROUP,
        %s AS STATUS,
        %s AS UPDATED_AT
    ) s
    ON t.ADO_APP_RAW = s.ADO_APP_RAW
    WHEN MATCHED THEN UPDATE SET
      NORMALIZED_KEY = s.NORMALIZED_KEY,
      FIRST_SEEN = CASE WHEN t.FIRST_SEEN IS NULL OR (s.FIRST_SEEN IS NOT NULL AND s.FIRST_SEEN < t.FIRST_SEEN) THEN s.FIRST_SEEN ELSE t.FIRST_SEEN END,
      LAST_SEEN = CASE WHEN t.LAST_SEEN IS NULL OR (s.LAST_SEEN IS NOT NULL AND s.LAST_SEEN > t.LAST_SEEN) THEN s.LAST_SEEN ELSE t.LAST_SEEN END,
      FEATURE_COUNT = s.FEATURE_COUNT,
      DERIVED_FTE_SUM = s.DERIVED_FTE_SUM,
      PROGRAMS_SEEN = s.PROGRAMS_SEEN,
      SUGGESTED_GROUP_ID = CASE WHEN UPPER(COALESCE(t.STATUS, '')) = 'IGNORED' THEN t.SUGGESTED_GROUP_ID ELSE s.SUGGESTED_GROUP_ID END,
      SUGGESTED_GROUP_NAME = CASE WHEN UPPER(COALESCE(t.STATUS, '')) = 'IGNORED' THEN t.SUGGESTED_GROUP_NAME ELSE s.SUGGESTED_GROUP_NAME END,
      SUGGESTED_TCO_GROUP_ID = CASE WHEN UPPER(COALESCE(t.STATUS, '')) = 'IGNORED' THEN t.SUGGESTED_TCO_GROUP_ID ELSE s.SUGGESTED_TCO_GROUP_ID END,
      SUGGESTED_TCO_GROUP = CASE WHEN UPPER(COALESCE(t.STATUS, '')) = 'IGNORED' THEN t.SUGGESTED_TCO_GROUP ELSE s.SUGGESTED_TCO_GROUP END,
      STATUS = CASE WHEN UPPER(COALESCE(t.STATUS, '')) = 'IGNORED' THEN 'IGNORED' ELSE s.STATUS END,
      UPDATED_AT = COALESCE(s.UPDATED_AT, SYSDATETIME())
    WHEN NOT MATCHED THEN INSERT (
      ADO_APP_RAW,
      NORMALIZED_KEY,
      FIRST_SEEN,
      LAST_SEEN,
      FEATURE_COUNT,
      DERIVED_FTE_SUM,
      PROGRAMS_SEEN,
      SUGGESTED_GROUP_ID,
      SUGGESTED_GROUP_NAME,
      SUGGESTED_TCO_GROUP_ID,
      SUGGESTED_TCO_GROUP,
      STATUS,
      UPDATED_AT
    )
    VALUES (
      s.ADO_APP_RAW,
      s.NORMALIZED_KEY,
      s.FIRST_SEEN,
      s.LAST_SEEN,
      s.FEATURE_COUNT,
      s.DERIVED_FTE_SUM,
      s.PROGRAMS_SEEN,
      s.SUGGESTED_GROUP_ID,
      s.SUGGESTED_GROUP_NAME,
      s.SUGGESTED_TCO_GROUP_ID,
      s.SUGGESTED_TCO_GROUP,
      s.STATUS,
      COALESCE(s.UPDATED_AT, SYSDATETIME())
    );
    """
    exec_sql(merge_sql, rows, True)  # type: ignore[arg-type]

    new_n = sum(1 for a, *_ in rows if str(a) not in existing_set)
    upd_n = len(rows) - new_n
    need_review_n = sum(1 for *_, status, _updated_at in rows if str(status).upper() in {"NEW", "NEEDS_REVIEW"})
    return AdoCandidatesRefreshResult(new=new_n, updated=upd_n, need_review=need_review_n)

def get_unmapped_ado_apps_for_teams(
    fetch_df: FetchFn,
    team_keys_or_names: Iterable[str],
    *,
    years: Optional[Iterable[Any]] = None,
    lookback_days: int = 365,
) -> pd.DataFrame:
    """Return unmapped ADO app names for the given TCO Team(s).

    Filters ADO_FEATURES to the selected teams via MAP_ADO_TEAM_TO_TCO_TEAM (TEAM_VARIANT_KEY → TEAMID),
    then returns APP_NAME_RAW values that have no mapping in MAP_ADO_APP_TO_TCO_GROUP.

    Returned columns:
      ADO_APP_RAW, FEATURE_COUNT, DERIVED_FTE_SUM, LAST_SEEN, SUGGESTED_GROUP_NAME, STATUS
    """
    team_vals = [str(t or "").strip() for t in (team_keys_or_names or [])]
    team_vals = [t for t in team_vals if t]
    if not team_vals:
        return pd.DataFrame(
            columns=["ADO_APP_RAW", "FEATURE_COUNT", "DERIVED_FTE_SUM", "LAST_SEEN", "SUGGESTED_GROUP_NAME", "STATUS"]
        )

    # Resolve TEAMNAME → TEAMID (accept TEAMID directly too).
    team_ids: list[str] = []
    for t in team_vals:
        if t in team_ids:
            continue
        try:
            df = fetch_df(
                """
                SELECT TOP 1 TEAMID
                FROM TEAMS
                WHERE TEAMID=%s
                   OR UPPER(COALESCE(NULLIF(TEAM_DISPLAY_NAME, ''), TEAMNAME))=UPPER(%s)
                """,
                (t, t),
            )
            if df is not None and not df.empty:
                tid = str(df.iloc[0].get("TEAMID") or "").strip()
                if tid:
                    team_ids.append(tid)
        except Exception:
            continue
    if not team_ids:
        return pd.DataFrame(
            columns=["ADO_APP_RAW", "FEATURE_COUNT", "DERIVED_FTE_SUM", "LAST_SEEN", "SUGGESTED_GROUP_NAME", "STATUS"]
        )

    years_i = _coerce_years(years)
    where: list[str] = [
        "mt.TEAMID IN (" + ", ".join(["%s"] * len(team_ids)) + ")",
        "af.APP_NAME_RAW IS NOT NULL AND LTRIM(RTRIM(af.APP_NAME_RAW)) <> ''",
        "mag.APP_GROUP IS NULL",
    ]
    params: list[Any] = list(team_ids)
    if years_i:
        where.append("af.ADO_YEAR IN (" + ", ".join(["%s"] * len(years_i)) + ")")
        params.extend(years_i)
    if lookback_days and int(lookback_days) > 0:
        where.append("COALESCE(af.CHANGED_AT, af.CREATED_AT) >= %s")
        params.append(datetime.utcnow() - timedelta(days=int(lookback_days)))

    where_sql = "WHERE " + " AND ".join(where)

    # Bring suggestion/status from candidates if present (new columns preferred, legacy as fallback).
    sql = f"""
    WITH dem AS (
      SELECT
        FEATURE_ID,
        TRY_CONVERT(INT, YEAR) AS ADO_YEAR,
        SUM(COALESCE(DERIVED_FTE_FEATURE, 0.0)) AS DERIVED_FTE_SUM
      FROM VW_TCO_FEATURE_DEMAND
      GROUP BY FEATURE_ID, TRY_CONVERT(INT, YEAR)
    )
    SELECT
      af.APP_NAME_RAW AS ADO_APP_RAW,
      COUNT(DISTINCT af.FEATURE_ID) AS FEATURE_COUNT,
      SUM(COALESCE(dem.DERIVED_FTE_SUM, 0.0)) AS DERIVED_FTE_SUM,
      MAX(COALESCE(af.CHANGED_AT, af.CREATED_AT)) AS LAST_SEEN,
      COALESCE(c.SUGGESTED_GROUP_NAME, c.SUGGESTED_TCO_GROUP) AS SUGGESTED_GROUP_NAME,
      COALESCE(c.STATUS, '') AS STATUS
    FROM ADO_FEATURES af
    LEFT JOIN dem
      ON dem.FEATURE_ID = af.FEATURE_ID
     AND dem.ADO_YEAR = TRY_CONVERT(INT, af.ADO_YEAR)
    INNER JOIN MAP_ADO_TEAM_TO_TCO_TEAM mt
      ON mt.ADO_TEAM_KEY = af.TEAM_VARIANT_KEY
    LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag
      ON mag.ADO_APP = af.APP_NAME_RAW
    LEFT JOIN ADO_APP_CANDIDATES c
      ON c.ADO_APP_RAW = af.APP_NAME_RAW
    {where_sql}
    GROUP BY
      af.APP_NAME_RAW,
      COALESCE(c.SUGGESTED_GROUP_NAME, c.SUGGESTED_TCO_GROUP),
      COALESCE(c.STATUS, '')
    ORDER BY
      COALESCE(DERIVED_FTE_SUM, 0.0) DESC,
      COALESCE(FEATURE_COUNT, 0) DESC,
      af.APP_NAME_RAW
    """
    try:
        df_out = fetch_df(sql, tuple(params) if params else None)
    except Exception:
        df_out = pd.DataFrame(
            columns=["ADO_APP_RAW", "FEATURE_COUNT", "DERIVED_FTE_SUM", "LAST_SEEN", "SUGGESTED_GROUP_NAME", "STATUS"]
        )
    if df_out is None:
        df_out = pd.DataFrame(
            columns=["ADO_APP_RAW", "FEATURE_COUNT", "DERIVED_FTE_SUM", "LAST_SEEN", "SUGGESTED_GROUP_NAME", "STATUS"]
        )
    return df_out


def get_unmapped_ado_apps_for_team(
    fetch_df: FetchFn,
    *,
    team_id: str,
    year: Optional[int] = None,
    lookback_days: int = 365,
) -> pd.DataFrame:
    """Return unmapped ADO app names for a single TCO Team.

    Matching logic (robust, in this order):
    1) TEAM_RAW exact match to TEAMS.TEAMNAME (case-insensitive)
    2) MAP_ADO_TEAM_TO_TCO_TEAM join via TEAM_VARIANT_KEY / TEAM_RAW
    3) AREA_PATH_RAW contains team name (fallback)

    Unmapped is defined as missing MAP_ADO_APP_TO_TCO_GROUP row, or an empty/null APP_GROUP.

    Returns columns:
      ADO_APP_RAW, FEATURE_COUNT, DERIVED_FTE_SUM, LAST_SEEN, CURRENT_TCO_GROUP, SUGGESTED_GROUP_NAME
    """
    team_id_s = str(team_id or "").strip()
    if not team_id_s:
        return pd.DataFrame(
            columns=[
                "ADO_APP_RAW",
                "FEATURE_COUNT",
                "DERIVED_FTE_SUM",
                "LAST_SEEN",
                "CURRENT_TCO_GROUP",
                "SUGGESTED_GROUP_NAME",
            ]
        )

    try:
        df_team = fetch_df(
            """
            SELECT TOP 1
              TEAMNAME,
              TEAM_DISPLAY_NAME,
              COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME) AS TEAMNAME_EFFECTIVE,
              PROGRAMID
            FROM TEAMS
            WHERE TEAMID=%s
            """,
            (team_id_s,),
        )
    except Exception:
        df_team = None
    if df_team is None or df_team.empty:
        out = pd.DataFrame(
            columns=[
                "ADO_APP_RAW",
                "FEATURE_COUNT",
                "DERIVED_FTE_SUM",
                "LAST_SEEN",
                "CURRENT_TCO_GROUP",
                "SUGGESTED_GROUP_NAME",
            ]
        )
        out.attrs["match_method"] = "NO_TEAM"
        return out

    team_name_raw = str(df_team.iloc[0].get("TEAMNAME") or "").strip()
    team_name_display = str(df_team.iloc[0].get("TEAM_DISPLAY_NAME") or "").strip()
    team_name_effective = str(df_team.iloc[0].get("TEAMNAME_EFFECTIVE") or "").strip()
    team_name = team_name_effective or team_name_display or team_name_raw
    team_names_for_match = [n for n in dict.fromkeys([team_name_effective, team_name_display, team_name_raw]) if n]
    if not team_name:
        out = pd.DataFrame(
            columns=[
                "ADO_APP_RAW",
                "FEATURE_COUNT",
                "DERIVED_FTE_SUM",
                "LAST_SEEN",
                "CURRENT_TCO_GROUP",
                "SUGGESTED_GROUP_NAME",
            ]
        )
        out.attrs["match_method"] = "NO_TEAMNAME"
        return out

    cutoff = None
    if lookback_days and int(lookback_days) > 0:
        cutoff = datetime.utcnow() - timedelta(days=int(lookback_days))

    base_where = [
        "af.APP_NAME_RAW IS NOT NULL AND LTRIM(RTRIM(af.APP_NAME_RAW)) <> ''",
        "(mag.APP_GROUP IS NULL OR LTRIM(RTRIM(mag.APP_GROUP)) = '')",
    ]
    params_base: list[Any] = []
    if year is not None:
        base_where.append("af.ADO_YEAR = %s")
        params_base.append(int(year))
    # Only apply lookback when there is date data; include rows with NULL dates so we don't hide data.
    if cutoff is not None:
        base_where.append("(COALESCE(af.CHANGED_AT, af.CREATED_AT) >= %s OR COALESCE(af.CHANGED_AT, af.CREATED_AT) IS NULL)")
        params_base.append(cutoff)

    select_sql = """
    SELECT
      af.APP_NAME_RAW AS ADO_APP_RAW,
      COUNT(DISTINCT af.FEATURE_ID) AS FEATURE_COUNT,
      SUM(COALESCE(dem.DERIVED_FTE_SUM, 0.0)) AS DERIVED_FTE_SUM,
      MAX(COALESCE(af.CHANGED_AT, af.CREATED_AT)) AS LAST_SEEN,
      CAST(NULL AS NVARCHAR(255)) AS CURRENT_TCO_GROUP,
      COALESCE(c.SUGGESTED_GROUP_NAME, c.SUGGESTED_TCO_GROUP) AS SUGGESTED_GROUP_NAME
    FROM ADO_FEATURES af
    LEFT JOIN (
      SELECT
        FEATURE_ID,
        TRY_CONVERT(INT, YEAR) AS ADO_YEAR,
        SUM(COALESCE(DERIVED_FTE_FEATURE, 0.0)) AS DERIVED_FTE_SUM
      FROM VW_TCO_FEATURE_DEMAND
      GROUP BY FEATURE_ID, TRY_CONVERT(INT, YEAR)
    ) dem
      ON dem.FEATURE_ID = af.FEATURE_ID
     AND dem.ADO_YEAR = TRY_CONVERT(INT, af.ADO_YEAR)
    LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag
      ON UPPER(LTRIM(RTRIM(mag.ADO_APP))) = UPPER(LTRIM(RTRIM(af.APP_NAME_RAW)))
    LEFT JOIN ADO_APP_CANDIDATES c
      ON c.ADO_APP_RAW = af.APP_NAME_RAW
    """

    group_order_sql = """
    GROUP BY
      af.APP_NAME_RAW,
      COALESCE(c.SUGGESTED_GROUP_NAME, c.SUGGESTED_TCO_GROUP)
    ORDER BY
      COALESCE(DERIVED_FTE_SUM, 0.0) DESC,
      COALESCE(LAST_SEEN, '1900-01-01') DESC,
      af.APP_NAME_RAW
    """

    def _run(method: str, join_sql: str, where_extra: str, params_extra: list[Any]) -> pd.DataFrame:
        where_all = list(base_where)
        if where_extra:
            where_all.append(where_extra)
        sql = select_sql + "\n" + (join_sql or "") + "\nWHERE " + " AND ".join(where_all) + "\n" + group_order_sql
        params = tuple(params_extra + params_base) if (params_extra + params_base) else None
        try:
            df = fetch_df(sql, params)
        except Exception:
            df = pd.DataFrame()
        if df is None:
            df = pd.DataFrame()
        df = df.copy()
        df.attrs["match_method"] = method
        df.attrs["team_name"] = team_name
        df.attrs["team_names_for_match"] = team_names_for_match
        df.attrs["sql"] = sql
        return df

    # 1) TEAM_RAW exact match
    if team_names_for_match:
        team_name_placeholders = ", ".join(["UPPER(%s)"] * len(team_names_for_match))
        team_raw_where = f"UPPER(LTRIM(RTRIM(af.TEAM_RAW))) IN ({team_name_placeholders})"
    else:
        team_raw_where = "UPPER(LTRIM(RTRIM(af.TEAM_RAW))) = UPPER(%s)"
    df1 = _run("TEAM_RAW", "", team_raw_where, team_names_for_match or [team_name])
    if df1 is not None and not df1.empty:
        return df1

    # 2) Mapping join (TEAM_VARIANT_KEY or TEAM_RAW mapped to TEAMID)
    join2 = """
    LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM mt
      ON UPPER(LTRIM(RTRIM(mt.ADO_TEAM_KEY))) = UPPER(LTRIM(RTRIM(af.TEAM_VARIANT_KEY)))
      OR UPPER(LTRIM(RTRIM(mt.ADO_TEAM))) = UPPER(LTRIM(RTRIM(af.TEAM_RAW)))
    """
    df2 = _run("TEAM_MAP", join2, "mt.TEAMID = %s", [team_id_s])
    if df2 is not None and not df2.empty:
        return df2

    # 3) AREA_PATH fallback (only if needed)
    if team_names_for_match:
        area_where = " OR ".join(["UPPER(COALESCE(af.AREA_PATH_RAW, '')) LIKE UPPER(%s)" for _ in team_names_for_match])
        area_params = [f"%{n}%" for n in team_names_for_match]
        df3 = _run("AREA_PATH", "", f"({area_where})", area_params)
    else:
        df3 = _run("AREA_PATH", "", "UPPER(COALESCE(af.AREA_PATH_RAW, '')) LIKE UPPER(%s)", [f"%{team_name}%"])

    # Attach debug counts when we still have no results.
    if df3 is None or df3.empty:
        debug_counts: dict[str, int] = {}
        for method, join_sql, where_extra, params_extra in (
            ("TEAM_RAW", "", team_raw_where, team_names_for_match or [team_name]),
            ("TEAM_MAP", join2, "mt.TEAMID = %s", [team_id_s]),
            (
                "AREA_PATH",
                "",
                f"({area_where})" if team_names_for_match else "UPPER(COALESCE(af.AREA_PATH_RAW, '')) LIKE UPPER(%s)",
                area_params if team_names_for_match else [f"%{team_name}%"],
            ),
        ):
            where_all = list(base_where)
            if where_extra:
                where_all.append(where_extra)
            sql_cnt = (
                "SELECT COUNT(DISTINCT af.APP_NAME_RAW) AS N_APPS "
                "FROM ADO_FEATURES af "
                "LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag "
                "  ON UPPER(LTRIM(RTRIM(mag.ADO_APP))) = UPPER(LTRIM(RTRIM(af.APP_NAME_RAW))) "
                + (join_sql or "")
                + " WHERE "
                + " AND ".join(where_all)
            )
            params = tuple(params_extra + params_base) if (params_extra + params_base) else None
            try:
                d = fetch_df(sql_cnt, params)
                n = int(d.iloc[0].get("N_APPS") or 0) if d is not None and not d.empty else 0
            except Exception:
                n = 0
            debug_counts[method] = n
        df3 = df3.copy()
        df3.attrs["debug_counts"] = debug_counts

    return df3


__all__ = [
    "AdoCandidatesRefreshResult",
    "refresh_ado_app_candidates",
    "get_unmapped_ado_apps_for_teams",
    "get_unmapped_ado_apps_for_team",
]
