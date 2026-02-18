from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio
from db import execute, fetch_df, _fq
from utils.admin_audit import log_admin_action

# Central delete guard for core entities.
# Use get_*_dependencies + render_dependency_summary before any delete attempt.


def _table_exists(table: str) -> bool:
    try:
        df = fetch_df(
            "SELECT COUNT(*) CNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = %s",
            (table,),
        )
        return int(df.iloc[0]["CNT"]) > 0 if isinstance(df, pd.DataFrame) and not df.empty else False
    except Exception:
        return False


def _count_dep(table: str, where_sql: str, params: Tuple[Any, ...]) -> Optional[int]:
    if not _table_exists(table):
        return None
    df = fetch_df(f"SELECT COUNT(*) CNT FROM { _fq(table) } WHERE {where_sql}", params)
    if isinstance(df, pd.DataFrame) and not df.empty:
        try:
            return int(df.iloc[0]["CNT"])
        except Exception:
            return 0
    return 0


def _column_exists(table: str, column: str) -> bool:
    try:
        df = fetch_df(
            """
            SELECT COUNT(*) CNT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = %s AND COLUMN_NAME = %s
            """,
            (table, column),
        )
        return int(df.iloc[0]["CNT"]) > 0 if isinstance(df, pd.DataFrame) and not df.empty else False
    except Exception:
        return False


def _count_contracts_for_group(group_id: str) -> Optional[int]:
    if not _table_exists("CONTRACTS"):
        return None
    # Newer schema: CONTRACTS has GROUPID directly.
    if _column_exists("CONTRACTS", "GROUPID"):
        return _count_dep("CONTRACTS", "GROUPID=%s", (group_id,))
    # Canonical schema: CONTRACTS stores APPLICATIONID + TEAMID only.
    if not _table_exists("APPLICATIONS"):
        return 0
    df = fetch_df(
        f"""
        SELECT COUNT(*) CNT
        FROM {_fq('CONTRACTS')} c
        JOIN {_fq('APPLICATIONS')} a
          ON a.APPLICATIONID = c.APPLICATIONID
        WHERE a.GROUPID = %s
        """,
        (group_id,),
    )
    if isinstance(df, pd.DataFrame) and not df.empty:
        try:
            return int(df.iloc[0]["CNT"])
        except Exception:
            return 0
    return 0


def _build_dep(table: str, count: Optional[int], where: str, severity: str = "block") -> Optional[Dict[str, Any]]:
    if count is None:
        return None
    return {"table": table, "count": int(count), "where": where, "severity": severity}


def _summarize(entity: str, entity_id: str, deps: List[Dict[str, Any]]) -> Dict[str, Any]:
    block_count = sum(int(d["count"]) for d in deps if d.get("severity") == "block")
    warn_count = sum(int(d["count"]) for d in deps if d.get("severity") == "warn")
    return {
        "ok": True,
        "entity": entity,
        "id": entity_id,
        "deps": deps,
        "block_count": block_count,
        "warn_count": warn_count,
    }


def _empty_deps(entity: str, entity_id: str) -> Dict[str, Any]:
    return _summarize(entity, entity_id, [])


@cache_data_portfolio(ttl=60)
def get_program_dependencies(program_id: str) -> Dict[str, Any]:
    deps: List[Dict[str, Any]] = []
    for table, where in [
        ("TEAMS", "PROGRAMID=%s"),
        ("PROGRAM_ADDITIONAL_COSTS", "PROGRAMID=%s"),
        ("PROGRAM_APPTIO_WORKID", "PROGRAMID=%s"),
        ("APP_USER_MEMBERSHIP", "PROGRAMID=%s"),
    ]:
        cnt = _count_dep(table, where, (program_id,))
        dep = _build_dep(table, cnt, where)
        if dep and dep["count"] > 0:
            deps.append(dep)
    # Mapping rows are auto-cleaned on delete; keep visible as warning-level deps.
    cnt_map = _count_dep("MAP_ADO_PROGRAM_TO_TCO_PROGRAM", "PROGRAMID=%s", (program_id,))
    dep_map = _build_dep("MAP_ADO_PROGRAM_TO_TCO_PROGRAM", cnt_map, "PROGRAMID=%s", severity="warn")
    if dep_map and dep_map["count"] > 0:
        deps.append(dep_map)
    return _summarize("PROGRAM", program_id, deps)


@cache_data_portfolio(ttl=60)
def get_team_dependencies(team_id: str) -> Dict[str, Any]:
    deps: List[Dict[str, Any]] = []
    for table, where in [
        ("APPLICATION_GROUPS", "TEAMID=%s"),
        ("APP_GROUP_TEAM_LINKS", "TEAMID=%s"),
        ("INVOICES", "TEAMID=%s"),
        ("CONTRACTS", "TEAMID=%s"),
        ("TEAM_HEADCOUNT_HISTORY", "TEAMID=%s"),
        ("TEAM_CONTRACTOR_HEADCOUNT", "TEAMID=%s"),
        ("TEAM_COMPOSITION_HISTORY", "TEAMID=%s"),
        ("TEAM_MSP_ASSIGNMENTS", "TEAMID=%s"),
        ("TEAM_MSP_RATE", "TEAMID=%s"),
        ("APP_USER_MEMBERSHIP", "TEAMID=%s"),
    ]:
        cnt = _count_dep(table, where, (team_id,))
        dep = _build_dep(table, cnt, where)
        if dep and dep["count"] > 0:
            deps.append(dep)
    # Mapping rows are auto-cleaned on delete; keep visible as warning-level deps.
    cnt_map = _count_dep("MAP_ADO_TEAM_TO_TCO_TEAM", "TEAMID=%s", (team_id,))
    dep_map = _build_dep("MAP_ADO_TEAM_TO_TCO_TEAM", cnt_map, "TEAMID=%s", severity="warn")
    if dep_map and dep_map["count"] > 0:
        deps.append(dep_map)
    return _summarize("TEAM", team_id, deps)


@cache_data_portfolio(ttl=60)
def get_app_group_dependencies(group_id: str) -> Dict[str, Any]:
    deps: List[Dict[str, Any]] = []
    contracts_cnt = _count_contracts_for_group(group_id)
    contracts_dep = _build_dep("CONTRACTS", contracts_cnt, "GROUPID=%s (or via APPLICATIONID join)")
    if contracts_dep and contracts_dep["count"] > 0:
        deps.append(contracts_dep)

    for table, where in [
        ("APPLICATIONS", "GROUPID=%s"),
        ("INVOICES", "GROUPID=%s"),
        ("TEAM_MSP_ASSIGNMENTS", "GROUPID=%s"),
        ("APP_GROUP_TEAM_LINKS", "GROUPID=%s"),
    ]:
        cnt = _count_dep(table, where, (group_id,))
        dep = _build_dep(table, cnt, where)
        if dep and dep["count"] > 0:
            deps.append(dep)
    return _summarize("APP_GROUP", group_id, deps)


@cache_data_portfolio(ttl=60)
def get_app_instance_dependencies(application_id: str) -> Dict[str, Any]:
    deps: List[Dict[str, Any]] = []
    for table, where in [
        ("INVOICES", "APPLICATIONID=%s"),
        ("CONTRACTS", "APPLICATIONID=%s"),
    ]:
        cnt = _count_dep(table, where, (application_id,))
        dep = _build_dep(table, cnt, where)
        if dep and dep["count"] > 0:
            deps.append(dep)
    return _summarize("APP_INSTANCE", application_id, deps)


@cache_data_portfolio(ttl=60)
def get_contract_dependencies(contract_id: str) -> Dict[str, Any]:
    # No direct dependents tracked in current schema.
    return _empty_deps("CONTRACT", contract_id)


@cache_data_portfolio(ttl=60)
def get_rate_dependencies(rate_key: str) -> Dict[str, Any]:
    # Rates are leaf records.
    return _empty_deps("RATE", rate_key)


def render_dependency_summary(deps: Dict[str, Any]) -> None:
    rows = deps.get("deps") or []
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=180)
    if deps.get("block_count", 0) > 0:
        st.error("Delete blocked because dependent records exist.")
    else:
        st.success("Safe to delete. No dependent records found.")


def can_delete(deps: Dict[str, Any]) -> bool:
    return int(deps.get("block_count", 0)) == 0


def _log_delete(action_type: str, entity: str, entity_id: str, deps: Dict[str, Any], source: str, message: str) -> None:
    log_admin_action(
        action_type=action_type,
        area="delete",
        entity=entity,
        summary=message,
        extra_json={
            "id": entity_id,
            "deps": deps.get("deps"),
            "block_count": deps.get("block_count"),
            "warn_count": deps.get("warn_count"),
            "source": source,
        },
    )


def safe_delete_program(program_id: str, *, source: str = "unknown") -> Dict[str, Any]:
    deps = get_program_dependencies(program_id)
    if not can_delete(deps):
        _log_delete("delete_blocked", "PROGRAM", program_id, deps, source, "Program delete blocked.")
        return {"ok": False, "message": "Delete blocked: dependencies exist.", "deps": deps}
    try:
        if _table_exists("MAP_ADO_PROGRAM_TO_TCO_PROGRAM"):
            execute(f"DELETE FROM { _fq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM') } WHERE PROGRAMID=%s", (program_id,))
        execute(f"DELETE FROM { _fq('PROGRAMS') } WHERE PROGRAMID=%s", (program_id,))
        _log_delete("delete_success", "PROGRAM", program_id, deps, source, "Program deleted.")
        return {"ok": True, "message": "Program deleted.", "deps": deps}
    except Exception as exc:
        _log_delete("delete_failed", "PROGRAM", program_id, deps, source, f"Delete failed: {exc}")
        return {"ok": False, "message": f"Delete failed: {exc}", "deps": deps}


def safe_delete_team(team_id: str, *, source: str = "unknown") -> Dict[str, Any]:
    deps = get_team_dependencies(team_id)
    if not can_delete(deps):
        _log_delete("delete_blocked", "TEAM", team_id, deps, source, "Team delete blocked.")
        return {"ok": False, "message": "Delete blocked: dependencies exist.", "deps": deps}
    try:
        if _table_exists("MAP_ADO_TEAM_TO_TCO_TEAM"):
            execute(f"DELETE FROM { _fq('MAP_ADO_TEAM_TO_TCO_TEAM') } WHERE TEAMID=%s", (team_id,))
        execute(f"DELETE FROM { _fq('TEAMS') } WHERE TEAMID=%s", (team_id,))
        _log_delete("delete_success", "TEAM", team_id, deps, source, "Team deleted.")
        return {"ok": True, "message": "Team deleted.", "deps": deps}
    except Exception as exc:
        _log_delete("delete_failed", "TEAM", team_id, deps, source, f"Delete failed: {exc}")
        return {"ok": False, "message": f"Delete failed: {exc}", "deps": deps}


def safe_delete_app_group(group_id: str, *, source: str = "unknown") -> Dict[str, Any]:
    deps = get_app_group_dependencies(group_id)
    if not can_delete(deps):
        _log_delete("delete_blocked", "APP_GROUP", group_id, deps, source, "App group delete blocked.")
        return {"ok": False, "message": "Delete blocked: dependencies exist.", "deps": deps}
    try:
        # Clean stale ADO app mappings for this group to avoid hidden onboard candidates.
        if _table_exists("MAP_ADO_APP_TO_TCO_GROUP"):
            execute(f"DELETE FROM { _fq('MAP_ADO_APP_TO_TCO_GROUP') } WHERE APP_GROUP=%s", (group_id,))
        execute(f"DELETE FROM { _fq('APPLICATION_GROUPS') } WHERE GROUPID=%s", (group_id,))
        _log_delete("delete_success", "APP_GROUP", group_id, deps, source, "Application group deleted.")
        return {"ok": True, "message": "Application group deleted.", "deps": deps}
    except Exception as exc:
        _log_delete("delete_failed", "APP_GROUP", group_id, deps, source, f"Delete failed: {exc}")
        return {"ok": False, "message": f"Delete failed: {exc}", "deps": deps}


def safe_delete_app_instance(application_id: str, *, source: str = "unknown") -> Dict[str, Any]:
    deps = get_app_instance_dependencies(application_id)
    if not can_delete(deps):
        _log_delete("delete_blocked", "APP_INSTANCE", application_id, deps, source, "App instance delete blocked.")
        return {"ok": False, "message": "Delete blocked: dependencies exist.", "deps": deps}
    try:
        execute(f"DELETE FROM { _fq('APPLICATIONS') } WHERE APPLICATIONID=%s", (application_id,))
        _log_delete("delete_success", "APP_INSTANCE", application_id, deps, source, "Application instance deleted.")
        return {"ok": True, "message": "Application instance deleted.", "deps": deps}
    except Exception as exc:
        _log_delete("delete_failed", "APP_INSTANCE", application_id, deps, source, f"Delete failed: {exc}")
        return {"ok": False, "message": f"Delete failed: {exc}", "deps": deps}


def safe_delete_contract(contract_id: str, *, source: str = "unknown") -> Dict[str, Any]:
    deps = get_contract_dependencies(contract_id)
    if not can_delete(deps):
        _log_delete("delete_blocked", "CONTRACT", contract_id, deps, source, "Contract delete blocked.")
        return {"ok": False, "message": "Delete blocked: dependencies exist.", "deps": deps}
    try:
        execute(f"DELETE FROM { _fq('CONTRACTS') } WHERE CONTRACT_ID=%s", (contract_id,))
        _log_delete("delete_success", "CONTRACT", contract_id, deps, source, "Contract deleted.")
        return {"ok": True, "message": "Contract deleted.", "deps": deps}
    except Exception as exc:
        _log_delete("delete_failed", "CONTRACT", contract_id, deps, source, f"Delete failed: {exc}")
        return {"ok": False, "message": f"Delete failed: {exc}", "deps": deps}


def safe_delete_rate(rate_type: str, owner_id: str, year: int, pi: int, location: str, *,
                     source: str = "unknown") -> Dict[str, Any]:
    rate_key = f"{rate_type}:{owner_id}:{year}:{pi}:{location}"
    deps = get_rate_dependencies(rate_key)
    if not can_delete(deps):
        _log_delete("delete_blocked", "RATE", rate_key, deps, source, "Rate delete blocked.")
        return {"ok": False, "message": "Delete blocked: dependencies exist.", "deps": deps}
    try:
        if rate_type == "TEAM":
            execute(
                f"DELETE FROM { _fq('TEAM_RATE_HISTORY') } WHERE TEAMID=%s AND YEAR=%s AND PI=%s AND LOCATION=%s",
                (owner_id, int(year), int(pi), location),
            )
        else:
            execute(
                f"DELETE FROM { _fq('PROGRAM_RATE_HISTORY') } WHERE PROGRAMID=%s AND YEAR=%s AND PI=%s AND LOCATION=%s",
                (owner_id, int(year), int(pi), location),
            )
        _log_delete("delete_success", "RATE", rate_key, deps, source, "Rate deleted.")
        return {"ok": True, "message": "Rate deleted.", "deps": deps}
    except Exception as exc:
        _log_delete("delete_failed", "RATE", rate_key, deps, source, f"Delete failed: {exc}")
        return {"ok": False, "message": f"Delete failed: {exc}", "deps": deps}


def scan_for_orphans(limit: int = 50) -> Dict[str, pd.DataFrame]:
    samples: Dict[str, pd.DataFrame] = {}
    if _table_exists("TEAMS") and _table_exists("PROGRAMS"):
        df = fetch_df(
            f"""SELECT TOP {limit} TEAMID, TEAMNAME, PROGRAMID
FROM { _fq('TEAMS') }
WHERE PROGRAMID NOT IN (SELECT PROGRAMID FROM { _fq('PROGRAMS') })"""
        )
        if isinstance(df, pd.DataFrame) and not df.empty:
            samples["teams_missing_program"] = df
    if _table_exists("APPLICATION_GROUPS") and _table_exists("TEAMS"):
        df = fetch_df(
            f"""SELECT TOP {limit} GROUPID, GROUPNAME, TEAMID
FROM { _fq('APPLICATION_GROUPS') }
WHERE TEAMID NOT IN (SELECT TEAMID FROM { _fq('TEAMS') })"""
        )
        if isinstance(df, pd.DataFrame) and not df.empty:
            samples["groups_missing_team"] = df
    if _table_exists("APPLICATIONS") and _table_exists("APPLICATION_GROUPS"):
        df = fetch_df(
            f"""SELECT TOP {limit} APPLICATIONID, APPLICATIONNAME, GROUPID
FROM { _fq('APPLICATIONS') }
WHERE GROUPID NOT IN (SELECT GROUPID FROM { _fq('APPLICATION_GROUPS') })"""
        )
        if isinstance(df, pd.DataFrame) and not df.empty:
            samples["apps_missing_group"] = df
    if _table_exists("INVOICES"):
        df = fetch_df(
            f"""SELECT TOP {limit} INVOICEID, TEAMID, GROUPID, APPLICATIONID
FROM { _fq('INVOICES') }
WHERE (TEAMID IS NOT NULL AND TEAMID NOT IN (SELECT TEAMID FROM { _fq('TEAMS') }))
   OR (GROUPID IS NOT NULL AND GROUPID NOT IN (SELECT GROUPID FROM { _fq('APPLICATION_GROUPS') }))
   OR (APPLICATIONID IS NOT NULL AND APPLICATIONID NOT IN (SELECT APPLICATIONID FROM { _fq('APPLICATIONS') }))"""
        )
        if isinstance(df, pd.DataFrame) and not df.empty:
            samples["invoices_missing_refs"] = df
    return samples
