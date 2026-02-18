from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from core.cache_utils import cache_data_portfolio
from core.db_session import db_fetch_df as fetch_df
from db import _fq


def _norm(v: Any) -> str:
    return str(v or "").strip().upper()


def _pick_display(primary: Any, fallback: Any, id_fallback: Any = "") -> str:
    p = str(primary or "").strip()
    if p:
        return p
    f = str(fallback or "").strip()
    if f:
        return f
    return str(id_fallback or "").strip()


def _add_alias(alias: Dict[str, str], key: Any, val: str) -> None:
    k = _norm(key)
    if not k:
        return
    if k not in alias:
        alias[k] = str(val or "").strip()


@cache_data_portfolio(ttl=300, show_spinner=False)
def get_display_alias_maps() -> Dict[str, Dict[str, str]]:
    program_id_to_display: Dict[str, str] = {}
    team_id_to_display: Dict[str, str] = {}
    group_id_to_display: Dict[str, str] = {}

    program_alias_to_display: Dict[str, str] = {}
    team_alias_to_display: Dict[str, str] = {}
    group_alias_to_display: Dict[str, str] = {}

    try:
        p = fetch_df(
            f"""
            SELECT PROGRAMID, PROGRAMNAME, PROGRAM_DISPLAY_NAME
            FROM {_fq('PROGRAMS')}
            """,
            None,
        )
        if isinstance(p, pd.DataFrame) and not p.empty:
            for _, r in p.iterrows():
                pid = str(r.get("PROGRAMID") or "").strip()
                disp = _pick_display(r.get("PROGRAM_DISPLAY_NAME"), r.get("PROGRAMNAME"), pid)
                if pid:
                    program_id_to_display[_norm(pid)] = disp
                _add_alias(program_alias_to_display, r.get("PROGRAM_DISPLAY_NAME"), disp)
                _add_alias(program_alias_to_display, r.get("PROGRAMNAME"), disp)
                _add_alias(program_alias_to_display, pid, disp)
    except Exception:
        pass

    try:
        t = fetch_df(
            f"""
            SELECT TEAMID, TEAMNAME, TEAM_DISPLAY_NAME
            FROM {_fq('TEAMS')}
            """,
            None,
        )
        if isinstance(t, pd.DataFrame) and not t.empty:
            for _, r in t.iterrows():
                tid = str(r.get("TEAMID") or "").strip()
                disp = _pick_display(r.get("TEAM_DISPLAY_NAME"), r.get("TEAMNAME"), tid)
                if tid:
                    team_id_to_display[_norm(tid)] = disp
                _add_alias(team_alias_to_display, r.get("TEAM_DISPLAY_NAME"), disp)
                _add_alias(team_alias_to_display, r.get("TEAMNAME"), disp)
                _add_alias(team_alias_to_display, tid, disp)
    except Exception:
        pass

    try:
        g = fetch_df(
            f"""
            SELECT GROUPID, GROUPNAME
            FROM {_fq('APPLICATION_GROUPS')}
            """,
            None,
        )
        if isinstance(g, pd.DataFrame) and not g.empty:
            for _, r in g.iterrows():
                gid = str(r.get("GROUPID") or "").strip()
                disp = str(r.get("GROUPNAME") or "").strip() or gid
                if gid:
                    group_id_to_display[_norm(gid)] = disp
                _add_alias(group_alias_to_display, r.get("GROUPNAME"), disp)
                _add_alias(group_alias_to_display, gid, disp)
    except Exception:
        pass

    # Program aliases from ADO mappings
    try:
        pm = fetch_df(
            f"""
            SELECT m.ADO_PROGRAM, p.PROGRAMID, p.PROGRAMNAME, p.PROGRAM_DISPLAY_NAME
            FROM {_fq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM')} m
            LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = m.PROGRAMID
            """,
            None,
        )
        if isinstance(pm, pd.DataFrame) and not pm.empty:
            for _, r in pm.iterrows():
                disp = _pick_display(r.get("PROGRAM_DISPLAY_NAME"), r.get("PROGRAMNAME"), r.get("PROGRAMID"))
                _add_alias(program_alias_to_display, r.get("ADO_PROGRAM"), disp)
    except Exception:
        pass

    # Team aliases from ADO mappings
    try:
        tm = fetch_df(
            f"""
            SELECT m.ADO_TEAM_KEY, m.ADO_TEAM, t.TEAMID, t.TEAMNAME, t.TEAM_DISPLAY_NAME
            FROM {_fq('MAP_ADO_TEAM_TO_TCO_TEAM')} m
            LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = m.TEAMID
            """,
            None,
        )
        if isinstance(tm, pd.DataFrame) and not tm.empty:
            for _, r in tm.iterrows():
                disp = _pick_display(r.get("TEAM_DISPLAY_NAME"), r.get("TEAMNAME"), r.get("TEAMID"))
                _add_alias(team_alias_to_display, r.get("ADO_TEAM"), disp)
                _add_alias(team_alias_to_display, r.get("ADO_TEAM_KEY"), disp)
    except Exception:
        pass

    # Application aliases from ADO app mapping
    try:
        gm = fetch_df(
            f"""
            SELECT m.ADO_APP, g.GROUPID, g.GROUPNAME
            FROM {_fq('MAP_ADO_APP_TO_TCO_GROUP')} m
            LEFT JOIN {_fq('APPLICATION_GROUPS')} g ON g.GROUPID = m.APP_GROUP
            """,
            None,
        )
        if isinstance(gm, pd.DataFrame) and not gm.empty:
            for _, r in gm.iterrows():
                disp = str(r.get("GROUPNAME") or "").strip() or str(r.get("GROUPID") or "").strip()
                _add_alias(group_alias_to_display, r.get("ADO_APP"), disp)
    except Exception:
        pass

    return {
        "program_id": program_id_to_display,
        "team_id": team_id_to_display,
        "group_id": group_id_to_display,
        "program_alias": program_alias_to_display,
        "team_alias": team_alias_to_display,
        "group_alias": group_alias_to_display,
    }


def _apply_one(
    out: pd.DataFrame,
    *,
    name_col: str,
    id_col: str,
    id_map: Dict[str, str],
    alias_map: Dict[str, str],
) -> None:
    if name_col not in out.columns:
        return

    s = out[name_col].fillna("").astype(str).str.strip()

    if id_col in out.columns:
        ids = out[id_col].fillna("").astype(str).str.strip().map(lambda v: id_map.get(_norm(v), ""))
        s = ids.where(ids.ne(""), s)

    s = s.map(lambda v: alias_map.get(_norm(v), v if isinstance(v, str) else str(v or "")))
    out[name_col] = s.fillna("").astype(str).str.strip()


def apply_display_scope_names(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    maps = get_display_alias_maps()
    out = df.copy()

    _apply_one(
        out,
        name_col="PROGRAMNAME",
        id_col="PROGRAMID",
        id_map=maps.get("program_id", {}),
        alias_map=maps.get("program_alias", {}),
    )
    _apply_one(
        out,
        name_col="TEAMNAME",
        id_col="TEAMID",
        id_map=maps.get("team_id", {}),
        alias_map=maps.get("team_alias", {}),
    )
    _apply_one(
        out,
        name_col="GROUPNAME",
        id_col="GROUPID",
        id_map=maps.get("group_id", {}),
        alias_map=maps.get("group_alias", {}),
    )

    if "PROGRAM" in out.columns and "PROGRAMNAME" in out.columns:
        out["PROGRAM"] = out["PROGRAMNAME"]
    if "TEAM" in out.columns and "TEAMNAME" in out.columns:
        out["TEAM"] = out["TEAMNAME"]
    if "APP_GROUP" in out.columns and "GROUPNAME" in out.columns:
        out["APP_GROUP"] = out["GROUPNAME"]

    return out
