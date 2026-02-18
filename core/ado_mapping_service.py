from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional
import uuid

import pandas as pd

from core.app_instances import ensure_default_instances

try:
    from db import execute_active as execute, fetch_df_active as fetch_df, upsert_program, upsert_team, upsert_application_group
except Exception:  # pragma: no cover - db facade not available in tests
    execute = None  # type: ignore[assignment]
    fetch_df = None  # type: ignore[assignment]
    upsert_program = None  # type: ignore[assignment]
    upsert_team = None  # type: ignore[assignment]
    upsert_application_group = None  # type: ignore[assignment]


@dataclass(frozen=True)
class MappingConflict:
    ado_value: str
    mapped_id: Optional[str]
    mapped_name: Optional[str]


def _clean_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def _normalize_key(series: pd.Series) -> pd.Series:
    return _clean_series(series).str.upper()


def _safe_datetime_max(df: pd.DataFrame) -> pd.Series:
    if "LAST_SEEN" in df.columns:
        return pd.to_datetime(df["LAST_SEEN"], errors="coerce")
    changed = pd.to_datetime(df.get("CHANGED_AT"), errors="coerce") if "CHANGED_AT" in df.columns else pd.Series([pd.NaT] * len(df))
    created = pd.to_datetime(df.get("CREATED_AT"), errors="coerce") if "CREATED_AT" in df.columns else pd.Series([pd.NaT] * len(df))
    return changed.fillna(created)


def _metric_column(df: pd.DataFrame) -> Optional[str]:
    for col in ["DERIVED_FTE_SUM", "DERIVED_FTE_FEATURE", "DERIVED_FTE_EXPLORER"]:
        if col in df.columns:
            return col
    for col in ["STORY_POINTS", "EFFORT_POINTS"]:
        if col in df.columns:
            return col
    return None


def _aggregate_candidates(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=group_cols + ["FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "DERIVED_FTE_SUM", "LAST_SEEN"])

    work = df.copy()
    for col in group_cols:
        if col not in work.columns:
            work[col] = ""
        work[col] = _clean_series(work[col])

    agg_cols = {}
    if "FEATURE_COUNT" in work.columns:
        agg_cols["FEATURE_COUNT"] = "sum"
    elif "FEATURE_ID" in work.columns:
        agg_cols["FEATURE_ID"] = pd.Series.nunique

    if "STORY_POINTS_SUM" in work.columns:
        agg_cols["STORY_POINTS_SUM"] = "sum"
    elif "STORY_POINTS" in work.columns:
        agg_cols["STORY_POINTS"] = "sum"

    if "EFFORT_POINTS_SUM" in work.columns:
        agg_cols["EFFORT_POINTS_SUM"] = "sum"
    elif "EFFORT_POINTS" in work.columns:
        agg_cols["EFFORT_POINTS"] = "sum"

    metric_col = _metric_column(work)
    if metric_col and metric_col not in {"STORY_POINTS", "EFFORT_POINTS"}:
        agg_cols[metric_col] = "sum"

    if "LAST_SEEN" in work.columns or "CHANGED_AT" in work.columns or "CREATED_AT" in work.columns:
        work["_LAST_SEEN"] = _safe_datetime_max(work)
        agg_cols["_LAST_SEEN"] = "max"

    grouped = work.groupby(group_cols, dropna=False).agg(agg_cols).reset_index()
    grouped.rename(columns={"FEATURE_ID": "FEATURE_COUNT"}, inplace=True)
    if "STORY_POINTS" in grouped.columns:
        grouped.rename(columns={"STORY_POINTS": "STORY_POINTS_SUM"}, inplace=True)
    if "EFFORT_POINTS" in grouped.columns:
        grouped.rename(columns={"EFFORT_POINTS": "EFFORT_POINTS_SUM"}, inplace=True)
    if metric_col and metric_col in grouped.columns and metric_col not in {"STORY_POINTS", "EFFORT_POINTS"}:
        grouped.rename(columns={metric_col: "DERIVED_FTE_SUM"}, inplace=True)
    if "_LAST_SEEN" in grouped.columns:
        grouped.rename(columns={"_LAST_SEEN": "LAST_SEEN"}, inplace=True)

    for col in ["FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "DERIVED_FTE_SUM"]:
        if col in grouped.columns:
            grouped[col] = pd.to_numeric(grouped[col], errors="coerce").fillna(0.0)
            if col == "FEATURE_COUNT":
                grouped[col] = grouped[col].astype(int)
    return grouped


def get_unmapped_ado_programs(ado_features_df: pd.DataFrame, program_map_df: pd.DataFrame) -> pd.DataFrame:
    df = _aggregate_candidates(ado_features_df, ["PROGRAM_RAW"]).rename(columns={"PROGRAM_RAW": "ADO_PROGRAM"})
    if df.empty:
        return df

    df = df[df["ADO_PROGRAM"].astype(str).str.strip() != ""].copy()
    mapped = program_map_df.copy() if program_map_df is not None else pd.DataFrame(columns=["ADO_PROGRAM", "PROGRAMID"])
    if not mapped.empty and "ADO_PROGRAM" in mapped.columns:
        mapped["ADO_PROGRAM_KEY"] = _normalize_key(mapped["ADO_PROGRAM"])
        mapped = mapped[mapped.get("PROGRAMID").astype(str).str.strip() != ""]
    else:
        mapped["ADO_PROGRAM_KEY"] = pd.Series([], dtype=str)

    df["ADO_PROGRAM_KEY"] = _normalize_key(df["ADO_PROGRAM"])
    if not mapped.empty:
        df = df[~df["ADO_PROGRAM_KEY"].isin(set(mapped["ADO_PROGRAM_KEY"].tolist()))].copy()
    df.drop(columns=["ADO_PROGRAM_KEY"], inplace=True, errors="ignore")
    return df


def get_unmapped_ado_teams(ado_features_df: pd.DataFrame, team_map_df: pd.DataFrame) -> pd.DataFrame:
    base = _aggregate_candidates(
        ado_features_df,
        ["TEAM_VARIANT_KEY", "TEAM_RAW", "PROGRAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW"],
    )
    if base.empty:
        return base

    base = base.rename(columns={"TEAM_VARIANT_KEY": "ADO_TEAM_KEY"}).copy()
    base = base[base["ADO_TEAM_KEY"].astype(str).str.strip() != ""].copy()
    if "TEAM_RAW" in base.columns:
        base["TEAM_RAW"] = base["TEAM_RAW"].fillna("").astype(str).str.strip()
    mapped = team_map_df.copy() if team_map_df is not None else pd.DataFrame(columns=["ADO_TEAM_KEY", "TEAMID"])
    if not mapped.empty and "ADO_TEAM_KEY" in mapped.columns:
        mapped["ADO_TEAM_KEY_NORM"] = _normalize_key(mapped["ADO_TEAM_KEY"])
        mapped = mapped[mapped.get("TEAMID").astype(str).str.strip() != ""]
    else:
        mapped["ADO_TEAM_KEY_NORM"] = pd.Series([], dtype=str)

    base["ADO_TEAM_KEY_NORM"] = _normalize_key(base["ADO_TEAM_KEY"])
    if not mapped.empty:
        base = base[~base["ADO_TEAM_KEY_NORM"].isin(set(mapped["ADO_TEAM_KEY_NORM"].tolist()))].copy()
    base.drop(columns=["ADO_TEAM_KEY_NORM"], inplace=True, errors="ignore")

    if "TEAM_RAW" in ado_features_df.columns and not ado_features_df.empty:
        raw_df = ado_features_df.copy()
        if "TEAM_VARIANT_KEY" in raw_df.columns:
            raw_df = raw_df.rename(columns={"TEAM_VARIANT_KEY": "ADO_TEAM_KEY"})
        if "ADO_TEAM_KEY" in raw_df.columns and "TEAM_RAW" in raw_df.columns:
            raw_df["ADO_TEAM_KEY"] = _clean_series(raw_df["ADO_TEAM_KEY"])
            raw_df["TEAM_RAW"] = _clean_series(raw_df["TEAM_RAW"])
            raw_df = raw_df[raw_df["ADO_TEAM_KEY"] != ""]
            raw_df = raw_df[raw_df["TEAM_RAW"] != ""]
            if "LAST_SEEN" not in raw_df.columns:
                if "CHANGED_AT" in raw_df.columns:
                    raw_df["LAST_SEEN"] = raw_df["CHANGED_AT"]
                elif "CREATED_AT" in raw_df.columns:
                    raw_df["LAST_SEEN"] = raw_df["CREATED_AT"]

            def _mode_or_latest(s: pd.Series) -> str:
                counts = s.value_counts()
                if not counts.empty:
                    return str(counts.index[0])
                return ""

            mode_map = (
                raw_df.groupby("ADO_TEAM_KEY", dropna=False)["TEAM_RAW"]
                .agg(_mode_or_latest)
                .rename("TEAM_RAW_MODE")
                .reset_index()
            )
            base = base.merge(mode_map, on="ADO_TEAM_KEY", how="left")
            if "TEAM_RAW_MODE" in base.columns:
                base["TEAM_RAW"] = base.get("TEAM_RAW", "").fillna("")
                base.loc[base["TEAM_RAW"] == "", "TEAM_RAW"] = base["TEAM_RAW_MODE"].fillna("")
                base.drop(columns=["TEAM_RAW_MODE"], inplace=True, errors="ignore")
    return base


def get_unmapped_ado_apps(
    ado_features_df: pd.DataFrame,
    app_map_df: pd.DataFrame,
    optional_filters: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    df = ado_features_df.copy() if ado_features_df is not None else pd.DataFrame()
    if optional_filters:
        for key, val in optional_filters.items():
            if val is None or key not in df.columns:
                continue
            df[key] = _clean_series(df[key])
            df = df[df[key].isin([str(v).strip() for v in (val if isinstance(val, (list, tuple, set)) else [val])])]

    base = _aggregate_candidates(df, ["APP_NAME_RAW"]).rename(columns={"APP_NAME_RAW": "ADO_APP"})
    if base.empty:
        return base

    base = base[base["ADO_APP"].astype(str).str.strip() != ""].copy()
    mapped = app_map_df.copy() if app_map_df is not None else pd.DataFrame(columns=["ADO_APP", "APP_GROUP"])
    if not mapped.empty and "ADO_APP" in mapped.columns:
        mapped["ADO_APP_KEY"] = _normalize_key(mapped["ADO_APP"])
        mapped = mapped[mapped.get("APP_GROUP").astype(str).str.strip() != ""]
        # Treat orphan mapping rows (APP_GROUP no longer exists) as unmapped.
        if "GROUPNAME" in mapped.columns:
            mapped = mapped[mapped["GROUPNAME"].fillna("").astype(str).str.strip() != ""]
    else:
        mapped["ADO_APP_KEY"] = pd.Series([], dtype=str)

    base["ADO_APP_KEY"] = _normalize_key(base["ADO_APP"])
    if not mapped.empty:
        base = base[~base["ADO_APP_KEY"].isin(set(mapped["ADO_APP_KEY"].tolist()))].copy()
    base.drop(columns=["ADO_APP_KEY"], inplace=True, errors="ignore")
    return base


def list_ado_program_mappings() -> pd.DataFrame:
    if fetch_df is None:
        return pd.DataFrame(columns=["ADO_PROGRAM", "PROGRAMID", "PROGRAMNAME"])
    return fetch_df(
        """
        SELECT
            mp.ADO_PROGRAM,
            mp.PROGRAMID,
            COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME
        FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = mp.PROGRAMID
        """,
        None,
    )


def list_ado_team_mappings() -> pd.DataFrame:
    if fetch_df is None:
        return pd.DataFrame(columns=["ADO_TEAM_KEY", "TEAMID", "TEAMNAME"])
    return fetch_df(
        """
        SELECT
            mt.ADO_TEAM_KEY,
            mt.TEAMID,
            COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME
        FROM MAP_ADO_TEAM_TO_TCO_TEAM mt
        LEFT JOIN TEAMS t ON t.TEAMID = mt.TEAMID
        """,
        None,
    )


def list_ado_app_mappings() -> pd.DataFrame:
    if fetch_df is None:
        return pd.DataFrame(columns=["ADO_APP", "APP_GROUP", "GROUPNAME"])
    return fetch_df(
        """
        SELECT ma.ADO_APP, ma.APP_GROUP, g.GROUPNAME
        FROM MAP_ADO_APP_TO_TCO_GROUP ma
        LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = ma.APP_GROUP
        """,
        None,
    )


def get_linked_ado_for_program(program_id: str) -> pd.DataFrame:
    if fetch_df is None:
        return pd.DataFrame(columns=["ADO_PROGRAM", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])
    return fetch_df(
        """
        SELECT
          mp.ADO_PROGRAM,
          COUNT(DISTINCT af.FEATURE_ID) AS FEATURE_COUNT,
          SUM(COALESCE(af.STORY_POINTS, 0.0)) AS STORY_POINTS_SUM,
          SUM(COALESCE(af.EFFORT_POINTS, 0.0)) AS EFFORT_POINTS_SUM,
          MAX(COALESCE(af.CHANGED_AT, af.CREATED_AT)) AS LAST_SEEN
        FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp
        LEFT JOIN ADO_FEATURES af
          ON UPPER(LTRIM(RTRIM(af.PROGRAM_RAW))) = UPPER(LTRIM(RTRIM(mp.ADO_PROGRAM)))
        WHERE mp.PROGRAMID = %s
        GROUP BY mp.ADO_PROGRAM
        ORDER BY mp.ADO_PROGRAM
        """,
        (str(program_id),),
    )


def get_linked_ado_for_team(team_id: str) -> pd.DataFrame:
    if fetch_df is None:
        return pd.DataFrame(columns=["ADO_TEAM_KEY", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])
    return fetch_df(
        """
        SELECT
          mt.ADO_TEAM_KEY,
          MAX(mt.ADO_TEAM) AS ADO_TEAM,
          MAX(mt.PROGRAM_RAW) AS PROGRAM_RAW,
          MAX(mt.AREA_LEVEL3_RAW) AS AREA_LEVEL3_RAW,
          MAX(mt.AREA_LEVEL4_RAW) AS AREA_LEVEL4_RAW,
          COUNT(DISTINCT af.FEATURE_ID) AS FEATURE_COUNT,
          SUM(COALESCE(af.STORY_POINTS, 0.0)) AS STORY_POINTS_SUM,
          SUM(COALESCE(af.EFFORT_POINTS, 0.0)) AS EFFORT_POINTS_SUM,
          MAX(COALESCE(af.CHANGED_AT, af.CREATED_AT)) AS LAST_SEEN
        FROM MAP_ADO_TEAM_TO_TCO_TEAM mt
        LEFT JOIN ADO_FEATURES af
          ON af.TEAM_VARIANT_KEY = mt.ADO_TEAM_KEY
        WHERE mt.TEAMID = %s
        GROUP BY mt.ADO_TEAM_KEY
        ORDER BY mt.ADO_TEAM_KEY
        """,
        (str(team_id),),
    )


def get_linked_ado_for_app_group(app_group_id: str) -> pd.DataFrame:
    if fetch_df is None:
        return pd.DataFrame(columns=["ADO_APP", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])
    return fetch_df(
        """
        SELECT
          ma.ADO_APP,
          COUNT(DISTINCT af.FEATURE_ID) AS FEATURE_COUNT,
          SUM(COALESCE(af.STORY_POINTS, 0.0)) AS STORY_POINTS_SUM,
          SUM(COALESCE(af.EFFORT_POINTS, 0.0)) AS EFFORT_POINTS_SUM,
          MAX(COALESCE(af.CHANGED_AT, af.CREATED_AT)) AS LAST_SEEN
        FROM MAP_ADO_APP_TO_TCO_GROUP ma
        LEFT JOIN ADO_FEATURES af
          ON UPPER(LTRIM(RTRIM(af.APP_NAME_RAW))) = UPPER(LTRIM(RTRIM(ma.ADO_APP)))
        WHERE ma.APP_GROUP = %s
        GROUP BY ma.ADO_APP
        ORDER BY ma.ADO_APP
        """,
        (str(app_group_id),),
    )


def map_ado_program_to_program(program_raw_list: Iterable[str], program_id: str) -> None:
    if execute is None:
        raise RuntimeError("DB execute helper not available")
    rows = []
    for raw in program_raw_list:
        val = str(raw or "").strip()
        if val:
            rows.append((val, str(program_id)))
    for ado_program, pid in rows:
        execute(
            """
            MERGE INTO MAP_ADO_PROGRAM_TO_TCO_PROGRAM t
            USING (SELECT %s AS ADO_PROGRAM, %s AS PROGRAMID) s
            ON t.ADO_PROGRAM = s.ADO_PROGRAM
            WHEN MATCHED THEN UPDATE SET PROGRAMID = s.PROGRAMID
            WHEN NOT MATCHED THEN INSERT (ADO_PROGRAM, PROGRAMID) VALUES (s.ADO_PROGRAM, s.PROGRAMID);
            """,
            (ado_program, pid),
        )


def map_ado_team_to_team(team_variant_key_list: Iterable[str], team_id: str) -> None:
    if execute is None or fetch_df is None:
        raise RuntimeError("DB helpers not available")
    keys = [str(k or "").strip() for k in team_variant_key_list if str(k or "").strip()]
    if not keys:
        return

    params = ",".join(["%s"] * len(keys))
    meta = fetch_df(
        f"""
        SELECT TEAM_VARIANT_KEY, TEAM_RAW, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW
        FROM ADO_FEATURES
        WHERE TEAM_VARIANT_KEY IN ({params})
        """,
        tuple(keys),
    )
    meta_lookup: dict[str, dict[str, Optional[str]]] = {}
    if meta is not None and not meta.empty:
        for _, r in meta.iterrows():
            key = str(r.get("TEAM_VARIANT_KEY") or "").strip()
            if not key:
                continue
            meta_lookup[key] = {
                "TEAM_RAW": str(r.get("TEAM_RAW") or "").strip() or None,
                "PROGRAM_RAW": str(r.get("PROGRAM_RAW") or "").strip() or None,
                "AREA_LEVEL3_RAW": str(r.get("AREA_LEVEL3_RAW") or "").strip() or None,
                "AREA_LEVEL4_RAW": str(r.get("AREA_LEVEL4_RAW") or "").strip() or None,
            }

    for key in keys:
        meta_row = meta_lookup.get(key, {})
        execute(
            """
            MERGE INTO MAP_ADO_TEAM_TO_TCO_TEAM t
            USING (
              SELECT %s AS ADO_TEAM_KEY,
                     %s AS TEAMID,
                     %s AS PROGRAM_RAW,
                     %s AS AREA_LEVEL3_RAW,
                     %s AS AREA_LEVEL4_RAW,
                     %s AS ADO_TEAM
            ) s
            ON t.ADO_TEAM_KEY = s.ADO_TEAM_KEY
            WHEN MATCHED THEN UPDATE SET TEAMID = s.TEAMID, PROGRAM_RAW = s.PROGRAM_RAW,
                                         AREA_LEVEL3_RAW = s.AREA_LEVEL3_RAW, AREA_LEVEL4_RAW = s.AREA_LEVEL4_RAW,
                                         ADO_TEAM = s.ADO_TEAM
            WHEN NOT MATCHED THEN INSERT (ADO_TEAM_KEY, TEAMID, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, ADO_TEAM)
            VALUES (s.ADO_TEAM_KEY, s.TEAMID, s.PROGRAM_RAW, s.AREA_LEVEL3_RAW, s.AREA_LEVEL4_RAW, s.ADO_TEAM);
            """,
            (
                key,
                str(team_id),
                meta_row.get("PROGRAM_RAW"),
                meta_row.get("AREA_LEVEL3_RAW"),
                meta_row.get("AREA_LEVEL4_RAW"),
                meta_row.get("TEAM_RAW"),
            ),
        )


def map_ado_app_to_app_group(app_name_raw_list: Iterable[str], app_group_id: str) -> None:
    if execute is None:
        raise RuntimeError("DB execute helper not available")
    rows = []
    for raw in app_name_raw_list:
        val = str(raw or "").strip()
        if val:
            rows.append((val, str(app_group_id)))
    for ado_app, gid in rows:
        execute(
            """
            MERGE INTO MAP_ADO_APP_TO_TCO_GROUP t
            USING (SELECT %s AS ADO_APP, %s AS APP_GROUP) s
            ON t.ADO_APP = s.ADO_APP
            WHEN MATCHED THEN UPDATE SET APP_GROUP = s.APP_GROUP
            WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP);
            """,
            (ado_app, gid),
        )
        try:
            execute(
                """
                UPDATE ADO_APP_CANDIDATES
                SET STATUS='MAPPED',
                    UPDATED_AT=SYSDATETIME()
                WHERE ADO_APP_RAW=%s
                """,
                (ado_app,),
            )
        except Exception:
            pass


def unmap_ado_app_from_app_group(app_name_raw_list: Iterable[str], app_group_id: Optional[str] = None) -> None:
    if execute is None:
        raise RuntimeError("DB execute helper not available")
    rows = [str(raw or "").strip() for raw in app_name_raw_list if str(raw or "").strip()]
    if not rows:
        return
    for ado_app in rows:
        if app_group_id:
            execute(
                "DELETE FROM MAP_ADO_APP_TO_TCO_GROUP WHERE ADO_APP=%s AND APP_GROUP=%s",
                (ado_app, str(app_group_id)),
            )
        else:
            execute(
                "DELETE FROM MAP_ADO_APP_TO_TCO_GROUP WHERE ADO_APP=%s",
                (ado_app,),
            )


def create_program(
    name: str,
    *,
    program_display_name: Optional[str] = None,
    owner: Optional[str] = None,
    fte: Optional[float] = None,
    program_rate: Optional[float] = None,
    updated_by: Optional[str] = None,
) -> str:
    if fetch_df is None or upsert_program is None:
        raise RuntimeError("DB helpers not available")
    clean_name = str(name or "").strip()
    if not clean_name:
        raise ValueError("Program name is required")
    existing = fetch_df("SELECT TOP 1 PROGRAMID FROM PROGRAMS WHERE UPPER(PROGRAMNAME)=UPPER(%s)", (clean_name,))
    if existing is not None and not existing.empty:
        return str(existing.iloc[0].get("PROGRAMID") or "").strip()
    program_id = str(uuid.uuid4())
    upsert_program(
        program_id,
        clean_name,
        owner=owner,
        fte=fte,
        program_rate=program_rate,
        program_display_name=(str(program_display_name).strip() if program_display_name else None),
        updated_by=updated_by,
    )
    return program_id


def create_program_from_ado(
    program_raw: str,
    *,
    name: Optional[str] = None,
    program_display_name: Optional[str] = None,
    updated_by: Optional[str] = None,
) -> str:
    if fetch_df is None or upsert_program is None:
        raise RuntimeError("DB helpers not available")
    program_name = str(name or program_raw or "").strip()
    if not program_name:
        raise ValueError("Program name is required")
    return create_program(
        program_name,
        program_display_name=program_display_name,
        owner=None,
        fte=None,
        program_rate=None,
        updated_by=updated_by,
    )


def create_team(
    name: str,
    *,
    program_id: Optional[str],
    team_display_name: Optional[str] = None,
    product_owner: Optional[str] = None,
    updated_by: Optional[str] = None,
) -> str:
    if fetch_df is None or execute is None:
        raise RuntimeError("DB helpers not available")
    team_name = str(name or "").strip()
    if not team_name:
        raise ValueError("Team name is required")
    if not program_id:
        raise ValueError("Program is required")
    existing = fetch_df("SELECT TOP 1 TEAMID FROM TEAMS WHERE UPPER(TEAMNAME)=UPPER(%s)", (team_name,))
    if existing is not None and not existing.empty:
        return str(existing.iloc[0].get("TEAMID") or "").strip()
    team_id = str(uuid.uuid4())
    execute(
        """
        INSERT INTO TEAMS (TEAMID, TEAMNAME, PROGRAMID, TEAMFTE,
                           DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE,
                           TEAM_DISPLAY_NAME, PRODUCTOWNER, UPDATED_BY)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            team_id,
            team_name,
            program_id,
            0.0,
            0.0,
            0.0,
            0.0,
            (str(team_display_name).strip() if team_display_name else None),
            product_owner,
            updated_by,
        ),
    )
    return team_id


def create_team_from_ado(
    team_variant_key: str,
    program_id: Optional[str] = None,
    *,
    name: Optional[str] = None,
    team_display_name: Optional[str] = None,
    updated_by: Optional[str] = None,
) -> str:
    if fetch_df is None or upsert_team is None:
        raise RuntimeError("DB helpers not available")
    key = str(team_variant_key or "").strip()
    if not key:
        raise ValueError("ADO team key is required")
    meta = fetch_df(
        "SELECT TOP 1 TEAM_RAW, PROGRAM_RAW FROM ADO_FEATURES WHERE TEAM_VARIANT_KEY=%s",
        (key,),
    )
    team_name = str(name or (meta.iloc[0].get("TEAM_RAW") if meta is not None and not meta.empty else "") or key).strip()
    existing = fetch_df("SELECT TOP 1 TEAMID FROM TEAMS WHERE UPPER(TEAMNAME)=UPPER(%s)", (team_name,))
    if existing is not None and not existing.empty:
        return str(existing.iloc[0].get("TEAMID") or "").strip()
    team_id = str(uuid.uuid4())
    upsert_team(
        team_id,
        team_name,
        program_id,
        team_fte=None,
        team_display_name=(str(team_display_name).strip() if team_display_name else None),
        updated_by=updated_by,
    )
    return team_id


def create_app_group(
    name: str,
    *,
    team_id: Optional[str] = None,
    owner: Optional[str] = None,
    default_vendor_id: Optional[str] = None,
    is_base: bool = False,
    updated_by: Optional[str] = None,
) -> str:
    if fetch_df is None or upsert_application_group is None:
        raise RuntimeError("DB helpers not available")
    group_name = str(name or "").strip()
    if not group_name:
        raise ValueError("Application name is required")
    existing = fetch_df("SELECT TOP 1 GROUPID FROM APPLICATION_GROUPS WHERE UPPER(GROUPNAME)=UPPER(%s)", (group_name,))
    if existing is not None and not existing.empty:
        return str(existing.iloc[0].get("GROUPID") or "").strip()
    group_id = str(uuid.uuid4())
    upsert_application_group(
        group_id,
        group_name,
        team_id=team_id or "",
        default_vendor_id=default_vendor_id,
        owner=owner,
        is_base=is_base,
        updated_by=updated_by,
    )
    if execute is not None and fetch_df is not None:
        try:
            ensure_default_instances(execute, fetch_df)
        except Exception:
            pass
    return group_id


def create_app_group_from_ado(
    app_name_raw: str,
    *,
    name: Optional[str] = None,
    team_id: Optional[str] = None,
    owner: Optional[str] = None,
    default_vendor_id: Optional[str] = None,
    updated_by: Optional[str] = None,
) -> str:
    if fetch_df is None or upsert_application_group is None:
        raise RuntimeError("DB helpers not available")
    group_name = str(name or app_name_raw or "").strip()
    if not group_name:
        raise ValueError("Application name is required")
    return create_app_group(
        group_name,
        team_id=team_id,
        owner=owner,
        default_vendor_id=default_vendor_id,
        updated_by=updated_by,
    )


__all__ = [
    "MappingConflict",
    "get_unmapped_ado_programs",
    "get_unmapped_ado_teams",
    "get_unmapped_ado_apps",
    "list_ado_program_mappings",
    "list_ado_team_mappings",
    "list_ado_app_mappings",
    "get_linked_ado_for_program",
    "get_linked_ado_for_team",
    "get_linked_ado_for_app_group",
    "map_ado_program_to_program",
    "map_ado_team_to_team",
    "map_ado_app_to_app_group",
    "unmap_ado_app_from_app_group",
    "create_program_from_ado",
    "create_program",
    "create_team_from_ado",
    "create_team",
    "create_app_group_from_ado",
    "create_app_group",
]
