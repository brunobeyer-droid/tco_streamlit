from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio
from db import (
    execute,
    fetch_df,
    upsert_program,
    upsert_team,
    upsert_vendor,
    upsert_application_group,
    upsert_application_instance,
    upsert_invoice,
    upsert_team_rate_history,
    upsert_program_rate_history,
    ensure_access_control_tables,
    list_app_users,
    upsert_app_user,
    _fq,
)
from utils.admin_audit import log_admin_action
from utils.admin_context import get_admin_context, apply_admin_context_filters
from utils.admin_shell import render_empty_state, render_perf_info, render_section
from utils.admin_safety import render_preview_apply_audit_block
from utils.admin_delete_guard import (
    get_program_dependencies,
    get_team_dependencies,
    get_app_group_dependencies,
    get_app_instance_dependencies,
    get_rate_dependencies,
    get_contract_dependencies,
    render_dependency_summary,
    can_delete,
    safe_delete_program,
    safe_delete_team,
    safe_delete_app_group,
    safe_delete_app_instance,
    safe_delete_rate,
    safe_delete_contract,
)
from utils.admin_entity_registry import list_entities_for, get_entity_by_id, table_exists

# To add a new entity:
# - extend _ENTITY_CONFIG with id/display/search columns and loader
# - add details + save/delete logic in _render_details


def _safe_fetch(query: str, params: Optional[Tuple[Any, ...]] = None) -> pd.DataFrame:
    df = fetch_df(query, params)
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _filter_df(df: pd.DataFrame, search: str, cols: List[str]) -> pd.DataFrame:
    if df.empty or not search:
        return df
    s = search.strip().lower()
    mask = pd.Series(False, index=df.index)
    for col in cols:
        if col in df.columns:
            mask = mask | df[col].astype(str).str.lower().str.contains(s, na=False)
    return df[mask]


@cache_data_portfolio(ttl=90)
def _load_programs(cache_bust: int = 0) -> pd.DataFrame:
    return _safe_fetch(f"""
        SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE, PROGRAM_XOM_RATE, UPDATED_AT, UPDATED_BY
        FROM { _fq('PROGRAMS') }
        ORDER BY PROGRAMNAME
    """)


@cache_data_portfolio(ttl=90)
def _load_teams(cache_bust: int = 0) -> pd.DataFrame:
    return _safe_fetch(f"""
        SELECT t.TEAMID, t.TEAMNAME, t.PROGRAMID, p.PROGRAMNAME, t.PRODUCTOWNER,
               t.TEAMFTE, t.DELIVERY_TEAM_FTE, t.CONTRACTOR_C_FTE, t.CONTRACTOR_CS_FTE,
               t.UPDATED_AT, t.UPDATED_BY
        FROM { _fq('TEAMS') } t
        LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = t.PROGRAMID
        ORDER BY t.TEAMNAME
    """)


@cache_data_portfolio(ttl=90)
def _load_groups(cache_bust: int = 0) -> pd.DataFrame:
    return _safe_fetch(f"""
        SELECT g.GROUPID, g.GROUPNAME, g.TEAMID, t.TEAMNAME, g.DEFAULT_VENDORID, v.VENDORNAME,
               g.OWNER, g.IS_BASE, g.UPDATED_AT, g.UPDATED_BY
        FROM { _fq('APPLICATION_GROUPS') } g
        LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = g.TEAMID
        LEFT JOIN { _fq('VENDORS') } v ON v.VENDORID = g.DEFAULT_VENDORID
        ORDER BY g.GROUPNAME
    """)


@cache_data_portfolio(ttl=90)
def _load_apps(cache_bust: int = 0) -> pd.DataFrame:
    return _safe_fetch(f"""
        SELECT a.APPLICATIONID, a.APPLICATIONNAME, a.GROUPID, g.GROUPNAME,
               COALESCE(a.VENDORID, g.DEFAULT_VENDORID) AS VENDORID,
               v.VENDORNAME, a.ADD_INFO, a.UPDATED_AT, a.UPDATED_BY
        FROM { _fq('APPLICATIONS') } a
        LEFT JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = a.GROUPID
        LEFT JOIN { _fq('VENDORS') } v ON v.VENDORID = COALESCE(a.VENDORID, g.DEFAULT_VENDORID)
        ORDER BY a.APPLICATIONNAME
    """)


@cache_data_portfolio(ttl=90)
def _load_vendors(cache_bust: int = 0) -> pd.DataFrame:
    return _safe_fetch(f"""
        SELECT VENDORID, VENDORNAME, UPDATED_AT, UPDATED_BY
        FROM { _fq('VENDORS') }
        ORDER BY VENDORNAME
    """)


@cache_data_portfolio(ttl=90)
def _load_invoices(cache_bust: int = 0) -> pd.DataFrame:
    return _safe_fetch(f"""
        SELECT i.INVOICEID, i.FISCAL_YEAR, i.INVOICEDATE, i.RENEWALDATE, i.AMOUNT,
               i.STATUS, COALESCE(i.INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE,
               t.TEAMNAME, a.APPLICATIONNAME, i.UPDATED_AT, i.UPDATED_BY
        FROM { _fq('INVOICES') } i
        LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = i.TEAMID
        LEFT JOIN { _fq('APPLICATIONS') } a ON a.APPLICATIONID = i.APPLICATIONID
        ORDER BY COALESCE(i.RENEWALDATE, i.INVOICEDATE) DESC
    """)


@cache_data_portfolio(ttl=90)
def _load_users(cache_bust: int = 0) -> pd.DataFrame:
    ensure_access_control_tables()
    df = list_app_users()
    if df is None or df.empty:
        return pd.DataFrame(columns=["EMAIL", "DISPLAY_NAME", "ROLE", "IS_ACTIVE", "PROVIDER", "LAST_LOGIN_AT"])
    return df


def _load_rates(cache_bust: int = 0) -> pd.DataFrame:
    team = _safe_fetch(f"""
        SELECT 'TEAM' AS RATE_TYPE, r.TEAMID AS OWNER_ID, t.TEAMNAME AS OWNER_NAME,
               r.YEAR, r.PI, r.LOCATION, r.XOM_RATE AS RATE_VALUE, r.UPDATED_AT, r.UPDATED_BY
        FROM { _fq('TEAM_RATE_HISTORY') } r
        LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = r.TEAMID
    """)
    prog = _safe_fetch(f"""
        SELECT 'PROGRAM' AS RATE_TYPE, r.PROGRAMID AS OWNER_ID, p.PROGRAMNAME AS OWNER_NAME,
               r.YEAR, r.PI, r.LOCATION, r.PROGRAM_XOM_RATE AS RATE_VALUE, r.UPDATED_AT, r.UPDATED_BY
        FROM { _fq('PROGRAM_RATE_HISTORY') } r
        LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = r.PROGRAMID
    """)
    if team.empty and prog.empty:
        return pd.DataFrame(columns=["RATE_TYPE", "OWNER_ID", "OWNER_NAME", "YEAR", "PI", "LOCATION", "RATE_VALUE"])
    df = pd.concat([team, prog], ignore_index=True)
    df["RATE_KEY"] = (
        df["RATE_TYPE"].astype(str) + ":" +
        df["OWNER_ID"].astype(str) + ":" +
        df["YEAR"].astype(str) + ":" +
        df["PI"].astype(str) + ":" +
        df["LOCATION"].astype(str)
    )
    return df


def _load_contracts(cache_bust: int = 0) -> pd.DataFrame:
    return _safe_fetch(f"""
        SELECT
            c.CONTRACT_ID,
            c.APPLICATIONID,
            a.APPLICATIONNAME,
            c.TEAMID,
            t.TEAMNAME,
            g.GROUPNAME,
            c.START_FY,
            c.END_FY,
            c.RENEWAL_MONTH,
            c.ANNUAL_AMOUNT,
            c.STATUS,
            c.UPDATED_AT,
            c.UPDATED_BY
        FROM { _fq('CONTRACTS') } c
        LEFT JOIN { _fq('APPLICATIONS') } a ON a.APPLICATIONID = c.APPLICATIONID
        LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = c.TEAMID
        LEFT JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = a.GROUPID
        ORDER BY c.CONTRACT_ID
    """)


def _count_team_cascade(team_id: str) -> Tuple[int, int, int]:
    inv_count_df = _safe_fetch(f"""
        SELECT COUNT(*) CNT
        FROM { _fq('INVOICES') }
        WHERE TEAMID = %s
           OR APPLICATIONID IN (
                SELECT a.APPLICATIONID
                FROM { _fq('APPLICATIONS') } a
                JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = a.GROUPID
                WHERE g.TEAMID = %s
           )
    """, (team_id, team_id))
    inv_count = int(inv_count_df.iloc[0]["CNT"]) if not inv_count_df.empty else 0
    app_count_df = _safe_fetch(f"""
        SELECT COUNT(*) CNT
        FROM { _fq('APPLICATIONS') } a
        WHERE a.GROUPID IN (SELECT g.GROUPID FROM { _fq('APPLICATION_GROUPS') } g WHERE g.TEAMID = %s)
    """, (team_id,))
    app_count = int(app_count_df.iloc[0]["CNT"]) if not app_count_df.empty else 0
    grp_count_df = _safe_fetch(f"SELECT COUNT(*) CNT FROM { _fq('APPLICATION_GROUPS') } WHERE TEAMID = %s", (team_id,))
    grp_count = int(grp_count_df.iloc[0]["CNT"]) if not grp_count_df.empty else 0
    return inv_count, app_count, grp_count


def _delete_team_cascade(team_id: str) -> Tuple[int, int, int]:
    raise RuntimeError(
        "Legacy cascade delete is disabled. Use safe_delete_team(...) through delete guard flows."
    )


def _count_program_cascade(program_id: str) -> Tuple[int, int, int, int]:
    teams = _safe_fetch(f"SELECT TEAMID FROM { _fq('TEAMS') } WHERE PROGRAMID = %s", (program_id,))
    if teams.empty:
        return (0, 0, 0, 0)
    t_count = 0
    inv_total = 0
    app_total = 0
    grp_total = 0
    for _, tr in teams.iterrows():
        team_id = tr["TEAMID"]
        inv_c, app_c, grp_c = _count_team_cascade(team_id)
        inv_total += inv_c
        app_total += app_c
        grp_total += grp_c
        t_count += 1
    return t_count, inv_total, app_total, grp_total


def _delete_program_cascade(program_id: str) -> Tuple[int, int, int, int]:
    raise RuntimeError(
        "Legacy cascade delete is disabled. Use safe_delete_program(...) through delete guard flows."
    )


_ENTITY_CONFIG: Dict[str, Dict[str, Any]] = {
    "Programs": {
        "loader": _load_programs,
        "id_col": "PROGRAMID",
        "display_col": "PROGRAMNAME",
        "search_cols": ["PROGRAMNAME", "PROGRAMID"],
        "list_cols": ["PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE"],
    },
    "Teams": {
        "loader": _load_teams,
        "id_col": "TEAMID",
        "display_col": "TEAMNAME",
        "search_cols": ["TEAMNAME", "PROGRAMNAME", "TEAMID"],
        "list_cols": ["TEAMNAME", "PROGRAMNAME", "PRODUCTOWNER"],
    },
    "Application Groups": {
        "loader": _load_groups,
        "id_col": "GROUPID",
        "display_col": "GROUPNAME",
        "search_cols": ["GROUPNAME", "TEAMNAME", "VENDORNAME"],
        "list_cols": ["GROUPNAME", "TEAMNAME", "VENDORNAME"],
    },
    "Applications": {
        "loader": _load_apps,
        "id_col": "APPLICATIONID",
        "display_col": "APPLICATIONNAME",
        "search_cols": ["APPLICATIONNAME", "GROUPNAME", "VENDORNAME"],
        "list_cols": ["APPLICATIONNAME", "GROUPNAME", "VENDORNAME"],
    },
    "Vendors": {
        "loader": _load_vendors,
        "id_col": "VENDORID",
        "display_col": "VENDORNAME",
        "search_cols": ["VENDORNAME", "VENDORID"],
        "list_cols": ["VENDORNAME"],
    },
    "Invoices": {
        "loader": _load_invoices,
        "id_col": "INVOICEID",
        "display_col": "INVOICEID",
        "search_cols": ["INVOICEID", "APPLICATIONNAME", "TEAMNAME"],
        "list_cols": ["INVOICEID", "APPLICATIONNAME", "TEAMNAME", "AMOUNT"],
    },
    "User": {
        "loader": _load_users,
        "id_col": "EMAIL",
        "display_col": "EMAIL",
        "search_cols": ["EMAIL", "DISPLAY_NAME", "ROLE"],
        "list_cols": ["EMAIL", "DISPLAY_NAME", "ROLE"],
    },
    "Rates": {
        "loader": _load_rates,
        "id_col": "RATE_KEY",
        "display_col": "RATE_KEY",
        "search_cols": ["RATE_TYPE", "OWNER_ID", "OWNER_NAME", "LOCATION"],
        "list_cols": ["RATE_TYPE", "OWNER_NAME", "YEAR", "PI", "LOCATION", "RATE_VALUE"],
    },
    "Contracts": {
        "loader": _load_contracts,
        "id_col": "CONTRACT_ID",
        "display_col": "CONTRACT_ID",
        "search_cols": ["CONTRACT_ID", "APPLICATIONID", "APPLICATIONNAME", "TEAMID", "TEAMNAME", "STATUS"],
        "list_cols": ["CONTRACT_ID", "APPLICATIONNAME", "TEAMNAME", "STATUS", "ANNUAL_AMOUNT"],
    },
}


def render_browse_manage_tab() -> None:
    render_section("Browse & Manage", "Search → select → edit with safe deletes and audit logging.")

    left_col, right_col = st.columns([0.38, 0.62], gap="large")
    ctx = get_admin_context()

    with left_col:
        bm_entities = [e["id"] for e in list_entities_for("browse_manage")]
        entity = st.selectbox(
            "Entity",
            options=bm_entities,
            key="admin.browse_manage.entity",
        )
        prev_entity = st.session_state.get("bm_entity_prev")
        if prev_entity and prev_entity != entity:
            st.session_state["bm_selected_id"] = None
        st.session_state["bm_entity_prev"] = entity

        search_key = f"admin.browse_manage.{entity}.search"
        search_val = st.text_input("Search", key=search_key, placeholder="Type to filter…")

        load_all_key = f"bm_load_all_{entity}"
        loaded_key = f"bm_loaded_{entity}"
        cache_ver_key = f"bm_cache_ver_{entity}"
        if not st.session_state.get(loaded_key):
            if st.button("Load results", key=f"bm_load_btn_{entity}"):
                st.session_state[loaded_key] = True
        if not st.session_state.get(loaded_key):
            render_empty_state(
                "No results loaded yet.",
                "Choose an entity and click “Load results” to fetch rows.",
            )
            return
        cache_ver = int(st.session_state.get(cache_ver_key, 0))
        t0 = time.perf_counter()
        ent_meta = get_entity_by_id(entity)
        tables = ent_meta.get("tables") if ent_meta else []
        missing_tables = [t for t in (tables or []) if not table_exists(t)]
        if missing_tables:
            render_empty_state(
                "Unavailable: table not found",
                f"Missing tables: {', '.join(missing_tables)}",
                tips=["Run schema initialization in Admin Home.", "Check database connectivity."],
            )
            return
        df = _ENTITY_CONFIG[entity]["loader"](cache_ver)
        df = apply_admin_context_filters(df, ctx, entity)
        render_perf_info(f"{entity} list", time.perf_counter() - t0, len(df))
        df = _filter_df(df, search_val, _ENTITY_CONFIG[entity]["search_cols"])
        total_rows = len(df)
        limit = ctx.get("row_limit")
        if limit is not None and not st.session_state.get(load_all_key):
            df = df.head(int(limit))
        st.caption(f"Loaded {len(df)} of {total_rows} rows.")
        if limit is not None and total_rows > limit and not st.session_state.get(load_all_key):
            if st.button("Load all", key=f"bm_load_all_btn_{entity}"):
                st.session_state[load_all_key] = True
        if st.button("Refresh", key=f"bm_refresh_{entity}"):
            st.session_state[cache_ver_key] = cache_ver + 1

        list_cols = [c for c in _ENTITY_CONFIG[entity]["list_cols"] if c in df.columns]
        if not ctx.get("show_ids"):
            list_cols = [c for c in list_cols if not c.upper().endswith("ID") and c not in ("RATE_KEY", "OWNER_ID")]
        if not df.empty:
            st.dataframe(df[list_cols], use_container_width=True, height=240)
        options: List[str] = []
        label_to_id: Dict[str, str] = {}
        id_col = _ENTITY_CONFIG[entity]["id_col"]
        display_col = _ENTITY_CONFIG[entity]["display_col"]
        for _, row in df.iterrows():
            if entity == "Contracts":
                app_label = str(row.get("APPLICATIONNAME") or "").strip()
                contract_label = str(row.get("CONTRACT_ID") or "").strip()
                label = " • ".join([v for v in [app_label, contract_label] if v])
            else:
                label = str(row.get(display_col) or row.get(id_col) or "").strip()
                if not ctx.get("show_ids") and label and (display_col.upper().endswith("ID") or display_col in (id_col, "RATE_KEY")):
                    preview_cols = [c for c in list_cols if c != display_col]
                    label = " • ".join([str(row.get(c) or "").strip() for c in preview_cols[:2] if str(row.get(c) or "").strip()]) or label
            if label:
                options.append(label)
                label_to_id[label] = str(row.get(id_col) or "").strip()
        selected_label = st.selectbox(
            "Select item",
            options=[""] + options,
            key=f"admin.browse_manage.{entity}.select",
        )
        selected_id = None
        if selected_label:
            if entity == "Contracts":
                selected_id = label_to_id.get(selected_label)
            else:
                match = df[df[display_col].astype(str) == selected_label]
                if match.empty:
                    match = df[df[id_col].astype(str) == selected_label]
                if not match.empty:
                    selected_id = match.iloc[0][id_col]
        st.session_state["bm_selected_id"] = selected_id

    with right_col:
        selected_id = st.session_state.get("bm_selected_id")
        if not selected_id:
            render_empty_state(
                "No selection",
                "Select an item from the left to view and edit details.",
            )
            return
        _render_details(entity, selected_id)


def _render_details(entity: str, selected_id: Any) -> None:
    render_section(f"{entity} details")
    ent_meta = get_entity_by_id(entity)
    if ent_meta and not ent_meta.get("supports", {}).get("delete", False):
        st.caption("Delete is not available for this entity.")

    if entity == "Programs":
        rec = _safe_fetch(f"SELECT * FROM { _fq('PROGRAMS') } WHERE PROGRAMID = %s", (selected_id,))
        if rec.empty:
            st.warning("Program not found.")
            return
        row = rec.iloc[0]
        st.caption(f"ID: {row['PROGRAMID']} | Updated: {row.get('UPDATED_AT') or '—'}")
        with st.form(f"bm_program_form_{selected_id}"):
            name = st.text_input("Program Name", value=str(row.get("PROGRAMNAME") or ""), key=f"bm_program_name_{selected_id}")
            owner = st.text_input("Program Owner", value=str(row.get("PROGRAMOWNER") or ""), key=f"bm_program_owner_{selected_id}")
            fte = st.number_input("Program FTE", value=float(row.get("PROGRAMFTE") or 0.0), step=0.1, key=f"bm_program_fte_{selected_id}")
            rate = st.number_input("Program Rate (annual per FTE)", value=float(row.get("PROGRAM_XOM_RATE") or 0.0), step=100.0, key=f"bm_program_rate_{selected_id}")
            save = st.form_submit_button("Save Changes", use_container_width=True)
        if save:
            original = (
                str(row.get("PROGRAMNAME") or ""),
                str(row.get("PROGRAMOWNER") or ""),
                float(row.get("PROGRAMFTE") or 0.0),
                float(row.get("PROGRAM_XOM_RATE") or 0.0),
            )
            edited = (name, owner, fte, rate)
            if original == edited:
                st.info("No changes to save.")
            else:
                upsert_program(row["PROGRAMID"], name, owner or None, fte, rate)
                log_admin_action(
                    action_type="update",
                    area="browse_manage",
                    entity="PROGRAMS",
                    summary=f"Updated program {row['PROGRAMID']}",
                    extra_json={"program": row["PROGRAMID"]},
                )
                st.success("Program updated.")

        teams = _safe_fetch(f"SELECT TEAMID, TEAMNAME FROM { _fq('TEAMS') } WHERE PROGRAMID = %s", (row["PROGRAMID"],))
        st.markdown("#### Teams in Program")
        st.dataframe(teams, use_container_width=True, height=200)

        deps = get_program_dependencies(str(row["PROGRAMID"]))
        render_dependency_summary(deps)

        def _preview() -> Dict[str, Any]:
            return {
                "ok": can_delete(deps),
                "message": "Safe to delete." if can_delete(deps) else "Delete blocked: dependencies exist.",
                "counts": {"block_count": deps.get("block_count", 0), "warn_count": deps.get("warn_count", 0)},
            }

        def _apply() -> Dict[str, Any]:
            return safe_delete_program(str(row["PROGRAMID"]), source="utils/admin_browse_manage.py")

        if ent_meta and ent_meta.get("supports", {}).get("delete", False):
            render_preview_apply_audit_block(
                title="Delete Program",
                area="browse_manage",
                action_type="delete",
                entity="PROGRAMS",
                context={"program_id": row["PROGRAMID"]},
                preview_fn=_preview,
                apply_fn=_apply,
                require_typed_confirm=True,
                confirm_phrase="DELETE",
                danger_level="danger",
            )

    elif entity == "Teams":
        rec = _safe_fetch(f"SELECT * FROM { _fq('TEAMS') } WHERE TEAMID = %s", (selected_id,))
        if rec.empty:
            st.warning("Team not found.")
            return
        row = rec.iloc[0]
        st.caption(f"ID: {row['TEAMID']} | Updated: {row.get('UPDATED_AT') or '—'}")
        with st.form(f"bm_team_form_{selected_id}"):
            name = st.text_input("Team Name", value=str(row.get("TEAMNAME") or ""), key=f"bm_team_name_{selected_id}")
            program_id = st.text_input("ProgramID (advanced)", value=str(row.get("PROGRAMID") or ""), key=f"bm_team_program_{selected_id}")
            team_fte = st.number_input("Team FTE", value=float(row.get("TEAMFTE") or 0.0), step=0.1, key=f"bm_team_fte_{selected_id}")
            delivery = st.number_input("Delivery Team FTE", value=float(row.get("DELIVERY_TEAM_FTE") or 0.0), step=0.1, key=f"bm_team_delivery_{selected_id}")
            c_fte = st.number_input("Contractor /C FTE", value=float(row.get("CONTRACTOR_C_FTE") or 0.0), step=0.1, key=f"bm_team_contractor_c_{selected_id}")
            cs_fte = st.number_input("Contractor /CS FTE", value=float(row.get("CONTRACTOR_CS_FTE") or 0.0), step=0.1, key=f"bm_team_contractor_cs_{selected_id}")
            owner = st.text_input("Product Owner", value=str(row.get("PRODUCTOWNER") or ""), key=f"bm_team_owner_{selected_id}")
            save = st.form_submit_button("Save Changes", use_container_width=True)
        if save:
            original = (
                str(row.get("TEAMNAME") or ""),
                str(row.get("PROGRAMID") or ""),
                float(row.get("TEAMFTE") or 0.0),
                float(row.get("DELIVERY_TEAM_FTE") or 0.0),
                float(row.get("CONTRACTOR_C_FTE") or 0.0),
                float(row.get("CONTRACTOR_CS_FTE") or 0.0),
                str(row.get("PRODUCTOWNER") or ""),
            )
            edited = (name, program_id, team_fte, delivery, c_fte, cs_fte, owner)
            if original == edited:
                st.info("No changes to save.")
            else:
                upsert_team(row["TEAMID"], name, program_id or None, team_fte, delivery, c_fte, cs_fte)
                execute(f"UPDATE { _fq('TEAMS') } SET PRODUCTOWNER=%s WHERE TEAMID=%s", (owner or None, row["TEAMID"]))
                log_admin_action(
                    action_type="update",
                    area="browse_manage",
                    entity="TEAMS",
                    summary=f"Updated team {row['TEAMID']}",
                    extra_json={"team": row["TEAMID"]},
                )
                st.success("Team updated.")

        groups = _safe_fetch(f"SELECT GROUPID, GROUPNAME FROM { _fq('APPLICATION_GROUPS') } WHERE TEAMID = %s", (row["TEAMID"],))
        st.markdown("#### Application Groups")
        st.dataframe(groups, use_container_width=True, height=200)

        deps = get_team_dependencies(str(row["TEAMID"]))
        render_dependency_summary(deps)

        def _preview() -> Dict[str, Any]:
            return {
                "ok": can_delete(deps),
                "message": "Safe to delete." if can_delete(deps) else "Delete blocked: dependencies exist.",
                "counts": {"block_count": deps.get("block_count", 0), "warn_count": deps.get("warn_count", 0)},
            }

        def _apply() -> Dict[str, Any]:
            return safe_delete_team(str(row["TEAMID"]), source="utils/admin_browse_manage.py")

        if ent_meta and ent_meta.get("supports", {}).get("delete", False):
            render_preview_apply_audit_block(
                title="Delete Team",
                area="browse_manage",
                action_type="delete",
                entity="TEAMS",
                context={"team_id": row["TEAMID"]},
                preview_fn=_preview,
                apply_fn=_apply,
                require_typed_confirm=True,
                confirm_phrase="DELETE",
                danger_level="danger",
            )

    elif entity == "Application Groups":
        rec = _safe_fetch(f"SELECT * FROM { _fq('APPLICATION_GROUPS') } WHERE GROUPID = %s", (selected_id,))
        if rec.empty:
            st.warning("Group not found.")
            return
        row = rec.iloc[0]
        st.caption(f"ID: {row['GROUPID']} | Updated: {row.get('UPDATED_AT') or '—'}")
        with st.form(f"bm_group_form_{selected_id}"):
            name = st.text_input("Group Name", value=str(row.get("GROUPNAME") or ""), key=f"bm_group_name_{selected_id}")
            team_id = st.text_input("TeamID (advanced)", value=str(row.get("TEAMID") or ""), key=f"bm_group_team_{selected_id}")
            vendor_id = st.text_input("Default VendorID (optional)", value=str(row.get("DEFAULT_VENDORID") or ""), key=f"bm_group_vendor_{selected_id}")
            owner = st.text_input("Owner", value=str(row.get("OWNER") or ""), key=f"bm_group_owner_{selected_id}")
            save = st.form_submit_button("Save Changes", use_container_width=True)
        if save:
            original = (
                str(row.get("GROUPNAME") or ""),
                str(row.get("TEAMID") or ""),
                str(row.get("DEFAULT_VENDORID") or ""),
                str(row.get("OWNER") or ""),
            )
            edited = (name, team_id, vendor_id, owner)
            if original == edited:
                st.info("No changes to save.")
            else:
                upsert_application_group(
                    group_id=row["GROUPID"],
                    group_name=name,
                    team_id=team_id or None,
                    default_vendor_id=vendor_id or None,
                    owner=owner or None,
                    is_base=bool(row.get("IS_BASE") or 0),
                )
                log_admin_action(
                    action_type="update",
                    area="browse_manage",
                    entity="APPLICATION_GROUPS",
                    summary=f"Updated group {row['GROUPID']}",
                    extra_json={"group": row["GROUPID"]},
                )
                st.success("Group updated.")

        apps = _safe_fetch(f"SELECT APPLICATIONID, APPLICATIONNAME FROM { _fq('APPLICATIONS') } WHERE GROUPID = %s", (row["GROUPID"],))
        st.markdown("#### Applications in Group")
        st.dataframe(apps, use_container_width=True, height=200)

        deps = get_app_group_dependencies(str(row["GROUPID"]))
        render_dependency_summary(deps)

        def _preview() -> Dict[str, Any]:
            return {
                "ok": can_delete(deps),
                "message": "Safe to delete." if can_delete(deps) else "Delete blocked: dependencies exist.",
                "counts": {"block_count": deps.get("block_count", 0), "warn_count": deps.get("warn_count", 0)},
            }

        def _apply() -> Dict[str, Any]:
            return safe_delete_app_group(str(row["GROUPID"]), source="utils/admin_browse_manage.py")

        if ent_meta and ent_meta.get("supports", {}).get("delete", False):
            render_preview_apply_audit_block(
                title="Delete Group",
                area="browse_manage",
                action_type="delete",
                entity="APPLICATION_GROUPS",
                context={"group_id": row["GROUPID"]},
                preview_fn=_preview,
                apply_fn=_apply,
                require_typed_confirm=True,
                confirm_phrase="DELETE",
                danger_level="danger",
            )

    elif entity == "Applications":
        rec = _safe_fetch(f"SELECT * FROM { _fq('APPLICATIONS') } WHERE APPLICATIONID = %s", (selected_id,))
        if rec.empty:
            st.warning("Application not found.")
            return
        row = rec.iloc[0]
        st.caption(f"ID: {row['APPLICATIONID']} | Updated: {row.get('UPDATED_AT') or '—'}")
        with st.form(f"bm_app_form_{selected_id}"):
            name = st.text_input("Application Name", value=str(row.get("APPLICATIONNAME") or ""), key=f"bm_app_name_{selected_id}")
            group_id = st.text_input("GroupID (advanced)", value=str(row.get("GROUPID") or ""), key=f"bm_app_group_{selected_id}")
            add_info = st.text_input("Additional Info", value=str(row.get("ADD_INFO") or ""), key=f"bm_app_info_{selected_id}")
            vendor_id = st.text_input("VendorID (optional)", value=str(row.get("VENDORID") or ""), key=f"bm_app_vendor_{selected_id}")
            save = st.form_submit_button("Save Changes", use_container_width=True)
        if save:
            original = (
                str(row.get("APPLICATIONNAME") or ""),
                str(row.get("GROUPID") or ""),
                str(row.get("ADD_INFO") or ""),
                str(row.get("VENDORID") or ""),
            )
            edited = (name, group_id, add_info, vendor_id)
            if original == edited:
                st.info("No changes to save.")
            else:
                upsert_application_instance(row["APPLICATIONID"], group_id or row.get("GROUPID"), name, add_info or None, vendor_id or None)
                log_admin_action(
                    action_type="update",
                    area="browse_manage",
                    entity="APPLICATIONS",
                    summary=f"Updated application {row['APPLICATIONID']}",
                    extra_json={"application": row["APPLICATIONID"]},
                )
                st.success("Application updated.")

        invs = _safe_fetch(f"SELECT INVOICEID, AMOUNT, STATUS FROM { _fq('INVOICES') } WHERE APPLICATIONID = %s", (row["APPLICATIONID"],))
        st.markdown("#### Invoices")
        st.dataframe(invs, use_container_width=True, height=200)

        deps = get_app_instance_dependencies(str(row["APPLICATIONID"]))
        render_dependency_summary(deps)

        def _preview() -> Dict[str, Any]:
            return {
                "ok": can_delete(deps),
                "message": "Safe to delete." if can_delete(deps) else "Delete blocked: dependencies exist.",
                "counts": {"block_count": deps.get("block_count", 0), "warn_count": deps.get("warn_count", 0)},
            }

        def _apply() -> Dict[str, Any]:
            return safe_delete_app_instance(str(row["APPLICATIONID"]), source="utils/admin_browse_manage.py")

        if ent_meta and ent_meta.get("supports", {}).get("delete", False):
            render_preview_apply_audit_block(
                title="Delete Application",
                area="browse_manage",
                action_type="delete",
                entity="APPLICATIONS",
                context={"application_id": row["APPLICATIONID"]},
                preview_fn=_preview,
                apply_fn=_apply,
                require_typed_confirm=True,
                confirm_phrase="DELETE",
                danger_level="danger",
            )

    elif entity == "Vendors":
        rec = _safe_fetch(f"SELECT * FROM { _fq('VENDORS') } WHERE VENDORID = %s", (selected_id,))
        if rec.empty:
            st.warning("Vendor not found.")
            return
        row = rec.iloc[0]
        st.caption(f"ID: {row['VENDORID']} | Updated: {row.get('UPDATED_AT') or '—'}")
        with st.form(f"bm_vendor_form_{selected_id}"):
            name = st.text_input("Vendor Name", value=str(row.get("VENDORNAME") or ""), key=f"bm_vendor_name_{selected_id}")
            save = st.form_submit_button("Save Changes", use_container_width=True)
        if save:
            original = str(row.get("VENDORNAME") or "")
            if original == name:
                st.info("No changes to save.")
            else:
                upsert_vendor(row["VENDORID"], name)
                log_admin_action(
                    action_type="update",
                    area="browse_manage",
                    entity="VENDORS",
                    summary=f"Updated vendor {row['VENDORID']}",
                    extra_json={"vendor": row["VENDORID"]},
                )
                st.success("Vendor updated.")

        def _preview() -> Dict[str, Any]:
            dep = _safe_fetch("SELECT COUNT(*) CNT FROM APPLICATIONS WHERE VENDORID = %s", (row["VENDORID"],))
            return {"ok": True, "message": "Previewed vendor delete impact.", "counts": {"apps": int(dep.iloc[0]["CNT"]) if not dep.empty else 0}}

        def _apply() -> Dict[str, Any]:
            execute(f"UPDATE { _fq('APPLICATION_GROUPS') } SET DEFAULT_VENDORID=NULL WHERE DEFAULT_VENDORID=%s", (row["VENDORID"],))
            execute(f"UPDATE { _fq('APPLICATIONS') } SET VENDORID=NULL WHERE VENDORID=%s", (row["VENDORID"],))
            execute(f"DELETE FROM { _fq('VENDORS') } WHERE VENDORID=%s", (row["VENDORID"],))
            return {"ok": True, "message": "Vendor deleted."}

        render_preview_apply_audit_block(
            title="Delete Vendor",
            area="browse_manage",
            action_type="delete",
            entity="VENDORS",
            context={"vendor_id": row["VENDORID"]},
            preview_fn=_preview,
            apply_fn=_apply,
            require_typed_confirm=True,
            confirm_phrase="DELETE",
            danger_level="danger",
        )

    elif entity == "Invoices":
        rec = _safe_fetch(f"SELECT * FROM { _fq('INVOICES') } WHERE INVOICEID = %s", (selected_id,))
        if rec.empty:
            st.warning("Invoice not found.")
            return
        row = rec.iloc[0]
        st.caption(f"ID: {row['INVOICEID']} | Updated: {row.get('UPDATED_AT') or '—'}")
        with st.form(f"bm_invoice_form_{selected_id}"):
            amount = st.number_input("Amount", value=float(row.get("AMOUNT") or 0.0), step=100.0, key=f"bm_invoice_amount_{selected_id}")
            status = st.text_input("Status", value=str(row.get("STATUS") or ""), key=f"bm_invoice_status_{selected_id}")
            save = st.form_submit_button("Save Changes", use_container_width=True)
        if save:
            original = (float(row.get("AMOUNT") or 0.0), str(row.get("STATUS") or ""))
            edited = (amount, status)
            if original == edited:
                st.info("No changes to save.")
            else:
                execute(f"UPDATE { _fq('INVOICES') } SET AMOUNT=%s, STATUS=%s WHERE INVOICEID=%s", (amount, status, row["INVOICEID"]))
                log_admin_action(
                    action_type="update",
                    area="browse_manage",
                    entity="INVOICES",
                    summary=f"Updated invoice {row['INVOICEID']}",
                    extra_json={"invoice": row["INVOICEID"]},
                )
                st.success("Invoice updated.")

        def _preview() -> Dict[str, Any]:
            return {"ok": True, "message": "Previewed invoice delete impact.", "counts": {"invoice": 1}}

        def _apply() -> Dict[str, Any]:
            execute(f"DELETE FROM { _fq('INVOICES') } WHERE INVOICEID = %s", (row["INVOICEID"],))
            return {"ok": True, "message": "Invoice deleted."}

        render_preview_apply_audit_block(
            title="Delete Invoice",
            area="browse_manage",
            action_type="delete",
            entity="INVOICES",
            context={"invoice_id": row["INVOICEID"]},
            preview_fn=_preview,
            apply_fn=_apply,
            require_typed_confirm=True,
            confirm_phrase="DELETE",
            danger_level="danger",
        )

    elif entity == "Rates":
        rate_df = _load_rates()
        row = rate_df[rate_df["RATE_KEY"] == selected_id]
        if row.empty:
            st.warning("Rate not found.")
            return
        row = row.iloc[0]
        st.caption(
            f"Type: {row.get('RATE_TYPE')} | Owner: {row.get('OWNER_ID')} | "
            f"Year: {row.get('YEAR')} | PI: {row.get('PI')} | Location: {row.get('LOCATION')}"
        )
        with st.form(f"bm_rate_form_{selected_id}"):
            rate_val = st.number_input(
                "Rate per PI",
                value=float(row.get("RATE_VALUE") or 0.0),
                step=100.0,
                key=f"bm_rate_value_{selected_id}",
            )
            save = st.form_submit_button("Save Changes", use_container_width=True)
        if save:
            original = float(row.get("RATE_VALUE") or 0.0)
            if original == rate_val:
                st.info("No changes to save.")
            else:
                rtype = str(row.get("RATE_TYPE") or "").upper()
                owner_id = str(row.get("OWNER_ID") or "")
                year = int(row.get("YEAR") or 0)
                pi = int(row.get("PI") or 0)
                loc = str(row.get("LOCATION") or "")
                if rtype == "TEAM":
                    upsert_team_rate_history(owner_id, year, pi, loc, rate_val)
                else:
                    upsert_program_rate_history(owner_id, year, pi, loc, rate_val)
                log_admin_action(
                    action_type="update",
                    area="browse_manage",
                    entity="RATES",
                    summary=f"Updated {rtype} rate {owner_id} {year}/{pi} {loc}",
                    extra_json={"rate_type": rtype, "owner_id": owner_id, "year": year, "pi": pi, "location": loc},
                )
                st.success("Rate updated.")

        rate_key = str(row.get("RATE_KEY") or "")
        deps = get_rate_dependencies(rate_key)
        render_dependency_summary(deps)

        def _preview() -> Dict[str, Any]:
            return {
                "ok": can_delete(deps),
                "message": "Safe to delete." if can_delete(deps) else "Delete blocked: dependencies exist.",
                "counts": {"block_count": deps.get("block_count", 0)},
            }

        def _apply() -> Dict[str, Any]:
            return safe_delete_rate(
                str(row.get("RATE_TYPE") or ""),
                str(row.get("OWNER_ID") or ""),
                int(row.get("YEAR") or 0),
                int(row.get("PI") or 0),
                str(row.get("LOCATION") or ""),
                source="utils/admin_browse_manage.py",
            )

        if ent_meta and ent_meta.get("supports", {}).get("delete", False):
            render_preview_apply_audit_block(
                title="Delete Rate",
                area="browse_manage",
                action_type="delete",
                entity="RATES",
                context={"rate_key": rate_key},
                preview_fn=_preview,
                apply_fn=_apply,
                require_typed_confirm=True,
                confirm_phrase="DELETE",
                danger_level="danger",
            )

    elif entity == "Contracts":
        rec = _safe_fetch(f"SELECT * FROM { _fq('CONTRACTS') } WHERE CONTRACT_ID = %s", (selected_id,))
        if rec.empty:
            st.warning("Contract not found.")
            return
        row = rec.iloc[0]
        st.caption(f"ID: {row.get('CONTRACT_ID')} | Updated: {row.get('UPDATED_AT') or '—'}")
        with st.form(f"bm_contract_form_{selected_id}"):
            status = st.text_input("Status", value=str(row.get("STATUS") or ""), key=f"bm_contract_status_{selected_id}")
            amount = st.number_input(
                "Annual Amount",
                value=float(row.get("ANNUAL_AMOUNT") or 0.0),
                step=100.0,
                key=f"bm_contract_amount_{selected_id}",
            )
            save = st.form_submit_button("Save Changes", use_container_width=True)
        if save:
            original = (str(row.get("STATUS") or ""), float(row.get("ANNUAL_AMOUNT") or 0.0))
            edited = (status, amount)
            if original == edited:
                st.info("No changes to save.")
            else:
                execute(
                    f"UPDATE { _fq('CONTRACTS') } SET STATUS=%s, ANNUAL_AMOUNT=%s WHERE CONTRACT_ID=%s",
                    (status or None, amount, row.get("CONTRACT_ID")),
                )
                log_admin_action(
                    action_type="update",
                    area="browse_manage",
                    entity="CONTRACTS",
                    summary=f"Updated contract {row.get('CONTRACT_ID')}",
                    extra_json={"contract_id": row.get("CONTRACT_ID")},
                )
                st.success("Contract updated.")

        deps = get_contract_dependencies(str(row.get("CONTRACT_ID") or ""))
        render_dependency_summary(deps)

        def _preview() -> Dict[str, Any]:
            return {
                "ok": can_delete(deps),
                "message": "Safe to delete." if can_delete(deps) else "Delete blocked: dependencies exist.",
                "counts": {"block_count": deps.get("block_count", 0)},
            }

        def _apply() -> Dict[str, Any]:
            return safe_delete_contract(str(row.get("CONTRACT_ID") or ""), source="utils/admin_browse_manage.py")

        if ent_meta and ent_meta.get("supports", {}).get("delete", False):
            render_preview_apply_audit_block(
                title="Delete Contract",
                area="browse_manage",
                action_type="delete",
                entity="CONTRACTS",
                context={"contract_id": row.get("CONTRACT_ID")},
                preview_fn=_preview,
                apply_fn=_apply,
                require_typed_confirm=True,
                confirm_phrase="DELETE",
                danger_level="danger",
            )

    elif entity == "User":
        rec = _safe_fetch(f"SELECT * FROM { _fq('APP_USERS') } WHERE UPPER(EMAIL) = UPPER(%s)", (str(selected_id),))
        if rec.empty:
            st.warning("User not found.")
            return
        row = rec.iloc[0]
        st.caption(f"Email: {row.get('EMAIL') or '—'}")
        with st.form(f"bm_user_form_{selected_id}"):
            display = st.text_input("Display Name", value=str(row.get("DISPLAY_NAME") or ""), key=f"bm_user_display_{selected_id}")
            role = st.selectbox(
                "Role",
                ["VIEWER", "CONTRIBUTOR", "ADMIN"],
                index=["VIEWER", "CONTRIBUTOR", "ADMIN"].index(str(row.get("ROLE") or "VIEWER").upper()),
                key=f"bm_user_role_{selected_id}",
            )
            active = st.checkbox("Active", value=bool(row.get("IS_ACTIVE", True)), key=f"bm_user_active_{selected_id}")
            save = st.form_submit_button("Save Changes", use_container_width=True)
        if save:
            original = (
                str(row.get("DISPLAY_NAME") or ""),
                str(row.get("ROLE") or "VIEWER").upper(),
                bool(row.get("IS_ACTIVE", True)),
            )
            edited = (display, role, active)
            if original == edited:
                st.info("No changes to save.")
            else:
                upsert_app_user(
                    email=str(row.get("EMAIL") or "").strip(),
                    display_name=display or None,
                    role=role,
                    is_active=active,
                    provider=str(row.get("PROVIDER") or None) or None,
                )
                log_admin_action(
                    action_type="update",
                    area="browse_manage",
                    entity="APP_USERS",
                    summary=f"Updated user {row.get('EMAIL')}",
                    extra_json={"email": row.get("EMAIL")},
                )
                st.success("User updated.")
