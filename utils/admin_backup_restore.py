# Summary: update admin export labels to NEXT branding.
from __future__ import annotations

import datetime as dt
import io as _io
import json as _json
import os as _os
import uuid
import zipfile as _zip
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from core.freshness import post_write_refresh
from utils.ado import repair_leaf_teams
from db import (
    execute,
    fetch_df,
    ensure_ado_minimal_tables,
    ensure_ado_iteration_calendar_table,
    ensure_ado_portfolio_settings_table,
    ensure_team_msp_assignments_table,
    ensure_team_composition_history,
    ensure_program_composition_history,
    ensure_user_membership_tables,
    ensure_email_alert_config_table,
    ensure_contracts_table,
    ensure_all_views_ok,
    ensure_analytics_views_ok,
    upsert_app_user,
    upsert_program,
    upsert_program_additional_cost,
    upsert_program_apptio_workid,
    upsert_team,
    upsert_vendor,
    upsert_application_group,
    upsert_application_instance,
    upsert_invoice,
    upsert_team_rate_history,
    upsert_program_rate_history,
    upsert_team_headcount,
    upsert_contractor_company,
    upsert_contractor_rate,
    upsert_team_contractor_headcount,
    upsert_apptio_actuals,
    upsert_ado_features,
    upsert_ado_iteration_calendar,
    _fq,
    ensure_ado_profiles_table,
    ensure_ado_workitem_lookup_table,
    ensure_ado_progress_tables,
    ensure_ado_timeline_tables,
    ensure_roadmap_milestones_table,
    set_active_ado_profile,
    upsert_ado_profile,
    upsert_ado_workitem_lookup,
    upsert_ado_feature_progress,
    upsert_ado_epic_progress,
    upsert_ado_workitem_dates,
    upsert_ado_workitem_links,
    upsert_roadmap_milestones,
    bump_data_version,
)
from utils.admin_audit import get_recent_admin_actions, log_admin_action
from utils.admin_safety import render_preview_apply_audit_block
from utils.admin_shell import render_empty_state, render_section

try:
    from db import recompute_feature_iteration_mapping as _recompute_feature_iteration_mapping  # type: ignore
except Exception:
    _recompute_feature_iteration_mapping = None


# Backup & Restore helpers.
# Backup includes: access/settings, core master data, workforce/rates, finance, ADO, PI readiness.
# Extend validation in _run_post_restore_validation().


def _backup_restore_post_write_refresh(
    context: str,
    *,
    rerun: bool = False,
    bump_version: Optional[bool] = None,
) -> None:
    post_write_refresh(context, ensure_views=False, rerun=rerun, bump_version=bump_version)


def _env_label() -> str:
    # IMPORTANT: do not touch st.secrets in backup/manifest paths.
    # Some production deployments are env-var-only and raise when secrets.toml is absent.
    for key in ("ENV", "ENVIRONMENT", "APP_ENV"):
        val = str(_os.getenv(key) or "").strip()
        if val:
            return val
    return str(_os.getenv("ENV") or _os.getenv("ENVIRONMENT") or _os.getenv("APP_ENV") or "").strip()


def _git_ref() -> str:
    return str(_os.getenv("GIT_SHA") or _os.getenv("GIT_COMMIT") or "").strip()


def _build_backup_manifest(
    *,
    dataset_stats: Dict[str, int],
    dataset_sources: Dict[str, str],
    schema_tables: Sequence[str],
) -> str:
    schema_snapshot: Dict[str, List[str]] = {}
    for tbl in schema_tables:
        try:
            schema_snapshot[tbl] = _get_table_columns(tbl)
        except Exception:
            schema_snapshot[tbl] = []
    manifest = {
        "timestamp_utc": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": _env_label(),
        "git_ref": _git_ref(),
        "datasets": [
            {"name": name, "source": dataset_sources.get(name, ""), "rows": int(dataset_stats.get(name, 0))}
            for name in sorted(dataset_stats.keys())
        ],
        "schema_snapshot": schema_snapshot,
    }
    return _json.dumps(manifest, indent=2)


def _to_bool(val: Any) -> Optional[bool]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("true", "1", "yes", "y"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None


def _to_float(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except Exception:
        return None


def _to_int(val: Any) -> Optional[int]:
    if val is None or (isinstance(val, float) and pd.isna(val)) or (isinstance(val, str) and not val.strip()):
        return None
    try:
        return int(val)
    except Exception:
        try:
            return int(float(str(val).replace(",", ".")))
        except Exception:
            return None


def _to_date(val: Any) -> Optional[dt.date]:
    if val is None or (isinstance(val, float) and pd.isna(val)) or (isinstance(val, str) and not val.strip()):
        return None
    if isinstance(val, dt.date) and not isinstance(val, dt.datetime):
        return val
    if isinstance(val, (pd.Timestamp, dt.datetime)):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def _opt_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    s = str(val).strip()
    if not s:
        return None
    if s.lower() in {"none", "nan", "null", "<na>"}:
        return None
    return s


def _ensure_column(df: pd.DataFrame, canonical: str, aliases: Sequence[str]) -> None:
    if canonical in df.columns:
        return
    for a in aliases:
        if a in df.columns:
            df[canonical] = df[a]
            return
    df[canonical] = None


def _normalize_ado_feature_columns(df: pd.DataFrame) -> None:
    rename_map = {
        "WORK_ITEM_ID": "FEATURE_ID",
        "WORKITEMID": "FEATURE_ID",
        "TITLE_TEXT": "TITLE",
    }
    for src, tgt in rename_map.items():
        if src in df.columns and tgt not in df.columns:
            df[tgt] = df[src]
    _ensure_column(df, "PROGRAM_RAW", ["PROGRAMNAME", "PROGRAM_NAME"])
    _ensure_column(df, "TEAM_RAW", ["TEAMNAME", "TEAM_NAME"])
    _ensure_column(df, "AREA_LEVEL2_RAW", ["AREA_LEVEL2_RAW", "AREALEVEL2"])
    _ensure_column(df, "AREA_LEVEL3_RAW", ["AREA_LEVEL3_RAW", "AREALEVEL3"])
    _ensure_column(df, "AREA_LEVEL4_RAW", ["AREA_LEVEL4_RAW", "AREALEVEL4"])
    _ensure_column(df, "AREA_PATH_RAW", ["AREA_PATH_RAW", "AREAPATH"])
    _ensure_column(df, "APP_NAME_RAW", ["APP_NAME_RAW", "APPNAME", "APP_NAME", "GROUPNAME"])
    _ensure_column(df, "INVESTMENT_DIMENSION", ["INVESTMENT_DIMENSION_RAW", "INVESTMENTDIMENSION"])
    _ensure_column(df, "PARENT_ID", ["ParentWorkItemId", "PARENTWORKITEMID", "PARENT_WORKITEM_ID"])
    _ensure_column(df, "EPIC_ID", ["EPICID", "EPIC_ID"])
    _ensure_column(df, "EPIC_TITLE", ["EPIC_TITLE", "EPICNAME", "EPIC_NAME"])
    _ensure_column(df, "EPIC_STATE", ["EPIC_STATE", "EPICSTATE"])
    _ensure_column(df, "STORY_POINTS", ["STORYPOINTS", "STORY_POINTS", "EFFORT_POINTS", "EFFORT"])
    _ensure_column(df, "EFFORT_POINTS", ["EFFORT_POINTS", "EFFORT", "STORYPOINTS"])


def _table_exists(table: str) -> bool:
    name = str(table or "").strip()
    if not name:
        return False
    schema = "dbo"
    if "." in name:
        parts = name.split(".", 1)
        schema = parts[0] or schema
        name = parts[1] or name
    try:
        df = fetch_df(
            "SELECT 1 AS OK FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (schema, name),
        )
        return isinstance(df, pd.DataFrame) and not df.empty
    except Exception:
        return False


def _get_table_columns(table: str) -> List[str]:
    name = str(table or "").strip()
    if not name:
        return []
    schema = "dbo"
    if "." in name:
        parts = name.split(".", 1)
        schema = parts[0] or schema
        name = parts[1] or name
    try:
        df = fetch_df(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (schema, name),
        )
    except Exception:
        return []
    if df is None or df.empty:
        return []
    return [str(v) for v in df["COLUMN_NAME"].tolist() if v is not None]


def _build_ado_features_export_spec() -> Tuple[str, str, bool, List[str]]:
    view_exists = _table_exists("VW_ADO_FEATURES_ENRICHED")
    view_cols = _get_table_columns("VW_ADO_FEATURES_ENRICHED") if view_exists else []
    view_has_sp = any(str(c).upper() == "STORY_POINTS" for c in view_cols)
    table_cols = _get_table_columns("ADO_FEATURES")
    table_has_sp = any(str(c).upper() == "STORY_POINTS" for c in table_cols)
    required_raw = {"PROGRAM_RAW", "TEAM_RAW", "AREA_LEVEL2_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW", "AREA_PATH_RAW"}
    view_has_raw = required_raw.issubset({str(c).upper() for c in view_cols})
    if view_exists and view_has_sp and view_has_raw:
        source = "VW_ADO_FEATURES_ENRICHED"
        cols = view_cols
    elif table_cols:
        source = "ADO_FEATURES"
        cols = table_cols
    else:
        source = "ADO_FEATURES"
        cols = []
    col_map = {str(c).lower(): str(c) for c in cols}

    def _col(name: str) -> Optional[str]:
        return col_map.get(str(name).lower())

    preferred_cols = [
        "FEATURE_ID",
        "TITLE",
        "STATE",
        "ADO_YEAR",
        "ITERATION_NUM",
        "PI_LABEL",
        "ITERATION_PATH",
        "ITERATION_LEVEL3",
        "ITERATION_SK",
        "PROGRAMNAME",
        "TEAMNAME",
        "GROUPNAME",
        "APP_NAME_RAW",
        "PROGRAM_RAW",
        "TEAM_RAW",
        "AREA_LEVEL2_RAW",
        "AREA_LEVEL3_RAW",
        "AREA_LEVEL4_RAW",
        "AREA_PATH_RAW",
        "EFFORT_POINTS",
        "STORY_POINTS",
        "BUSINESS_VALUE",
        "INVESTMENT_DIMENSION",
        "INVESTMENT_DIMENSION_RAW",
        "CHANGED_DATE",
        "UPDATED_AT",
        "CHANGED_AT",
        "CREATED_AT",
        "TEAM_VARIANT_KEY",
    ]
    select_cols: List[str] = []
    for c in preferred_cols:
        actual = _col(c)
        if actual:
            select_cols.append(actual)
    for c in ["PARENT_ID", "EPIC_ID", "EPIC_TITLE", "EPIC_STATE"]:
        actual = _col(c)
        if actual:
            select_cols.append(actual)
    parent_workitem = (
        _col("ParentWorkItemId")
        or _col("PARENTWORKITEMID")
        or _col("PARENT_WORKITEM_ID")
    )
    if parent_workitem:
        select_cols.append(parent_workitem)
    order_candidates = ["CHANGED_AT", "CHANGED_DATE", "UPDATED_AT", "CREATED_AT", "FEATURE_ID"]
    order_col = None
    for cand in order_candidates:
        actual = _col(cand)
        if actual:
            order_col = actual
            break
    order_clause = f"ORDER BY {order_col} DESC" if order_col else "ORDER BY FEATURE_ID"
    select_list = ", ".join(select_cols)
    sql = f"SELECT {select_list} FROM {source} {order_clause}"
    has_story_points = _col("STORY_POINTS") is not None
    return sql, source, has_story_points, select_cols


def _build_ado_epic_progress_export_sql() -> str:
    cols = {str(c).upper() for c in _get_table_columns("ADO_EPIC_PROGRESS")}
    has_feature_counts = (
        "FEATURE_COUNT_TOTAL" in cols
        and "FEATURE_COUNT_WITH_STORIES" in cols
        and "FEATURE_COUNT_WITH_SP" in cols
    )
    if has_feature_counts:
        return (
            "SELECT EPIC_ID, PROPOSED_SP, INPROGRESS_SP, COMPLETED_SP, TOTAL_SP, PCT_COMPLETE, "
            "FEATURE_COUNT_TOTAL, FEATURE_COUNT_WITH_STORIES, FEATURE_COUNT_WITH_SP, UPDATED_AT "
            "FROM ADO_EPIC_PROGRESS ORDER BY UPDATED_AT DESC, EPIC_ID"
        )
    return (
        "SELECT EPIC_ID, PROPOSED_SP, INPROGRESS_SP, COMPLETED_SP, TOTAL_SP, PCT_COMPLETE, "
        "CAST(NULL AS INT) AS FEATURE_COUNT_TOTAL, CAST(NULL AS INT) AS FEATURE_COUNT_WITH_STORIES, "
        "CAST(NULL AS INT) AS FEATURE_COUNT_WITH_SP, UPDATED_AT "
        "FROM ADO_EPIC_PROGRESS ORDER BY UPDATED_AT DESC, EPIC_ID"
    )


def _ado_profiles_export_sql(*, include_secrets: bool = False) -> str:
    cols = _get_table_columns("ADO_PROFILES")
    colset = {str(c).upper() for c in cols}
    select_cols = ["PROFILE_ID", "PROFILE_NAME", "IS_ACTIVE", "CONFIG_JSON", "UPDATED_AT", "UPDATED_BY"]
    if include_secrets and "PAT_ENC" in colset:
        select_cols.append("PAT_ENC")
    return f"SELECT {', '.join(select_cols)} FROM ADO_PROFILES ORDER BY IS_ACTIVE DESC, PROFILE_NAME"


def _normalize_ado_iteration_calendar(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    rename_map = {
        "ITERATIONPATH": "ITERATION_PATH",
        "ITERATIONLEVEL3": "ITERATION_LEVEL3",
        "START": "START_DATE",
        "END": "END_DATE",
    }
    for src, tgt in rename_map.items():
        if src in work.columns and tgt not in work.columns:
            work[tgt] = work[src]
    return work


def _zip_clean(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val).strip() or None


def _dl_csv(label: str, sql: str, filename: str) -> None:
    try:
        df = fetch_df(sql)
    except Exception as e:
        st.warning(f"{label}: {e}")
        return
    if df is None or df.empty:
        st.caption(f"{label}: no rows")
        return
    buf = _io.StringIO()
    df.to_csv(buf, index=False)
    st.download_button(
        f"⬇️ {label}",
        data=buf.getvalue(),
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )


def _full_backup_dataset_specs(include_secrets: bool = False) -> List[Tuple[str, str]]:
    return [
        ("app_users.csv", "SELECT EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, PROVIDER FROM APP_USERS ORDER BY EMAIL"),
        ("app_user_membership.csv", "SELECT USER_EMAIL, PROGRAMID, TEAMID, CREATED_AT FROM APP_USER_MEMBERSHIP ORDER BY USER_EMAIL, PROGRAMID, TEAMID"),
        ("ado_portfolio_settings.csv", "SELECT PORTFOLIO_NAME, SETTINGS_JSON, UPDATED_AT, UPDATED_BY FROM ADO_PORTFOLIO_SETTINGS ORDER BY PORTFOLIO_NAME"),
        ("email_alert_config.csv", "SELECT CONFIG_KEY, CONFIG_JSON, UPDATED_AT, UPDATED_BY FROM EMAIL_ALERT_CONFIG ORDER BY CONFIG_KEY"),
        ("programs.csv", "SELECT PROGRAMID, PROGRAMNAME, PROGRAM_DISPLAY_NAME, PROGRAMOWNER, PROGRAMFTE, PROGRAM_XOM_RATE FROM PROGRAMS ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)),''), PROGRAMNAME)"),
        ("program_composition_history.csv", "SELECT PROGRAMID, YEAR, PI, PROGRAMFTE, UPDATED_AT, UPDATED_BY FROM PROGRAM_COMPOSITION_HISTORY ORDER BY PROGRAMID, YEAR, PI"),
        ("program_additional_costs.csv", "SELECT PROGRAMID, YEAR, MONTH, COST_TYPE, SUBTYPE, DESCRIPTION, CURRENCY, IS_RECURRING, AMOUNT, UPDATED_AT, UPDATED_BY FROM PROGRAM_ADDITIONAL_COSTS ORDER BY PROGRAMID, YEAR DESC, MONTH DESC"),
        ("program_apptio_workids.csv", "SELECT WORK_ID, PROGRAMID, UPDATED_AT, UPDATED_BY FROM PROGRAM_APPTIO_WORKIDS ORDER BY PROGRAMID, WORK_ID"),
        ("apptio_actuals.csv", "SELECT WORK_ID, FISCAL_YEAR, MONTH, AMOUNT, SOURCE, LOADED_AT, LOADED_BY FROM APPTIO_ACTUALS ORDER BY FISCAL_YEAR DESC, MONTH DESC, WORK_ID"),
        ("teams.csv", "SELECT TEAMID, TEAMNAME, TEAM_DISPLAY_NAME, PROGRAMID, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE, PRODUCTOWNER FROM TEAMS ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)),''), TEAMNAME)"),
        ("team_composition_history.csv", "SELECT TEAMID, YEAR, PI, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE, UPDATED_AT FROM TEAM_COMPOSITION_HISTORY ORDER BY TEAMID, YEAR, PI"),
        ("team_rate_history.csv", "SELECT TEAMID, YEAR, PI, LOCATION, XOM_RATE, UPDATED_AT, UPDATED_BY FROM TEAM_RATE_HISTORY ORDER BY TEAMID, YEAR, PI, LOCATION"),
        ("program_rate_history.csv", "SELECT PROGRAMID, YEAR, PI, LOCATION, PROGRAM_XOM_RATE, UPDATED_AT, UPDATED_BY FROM PROGRAM_RATE_HISTORY ORDER BY PROGRAMID, YEAR, PI, LOCATION"),
        ("team_headcount_history.csv", "SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, UPDATED_AT, UPDATED_BY FROM TEAM_HEADCOUNT_HISTORY ORDER BY TEAMID, YEAR, PI, CLASS, LOCATION"),
        ("contractor_company.csv", "SELECT COMPANYID, NAME, ACTIVE, NOTES, UPDATED_AT, UPDATED_BY FROM CONTRACTOR_COMPANY ORDER BY NAME"),
        ("contractor_rate_history.csv", "SELECT COMPANYID, YEAR, PI, CLASS, RATE, UPDATED_AT, UPDATED_BY FROM CONTRACTOR_RATE_HISTORY ORDER BY COMPANYID, YEAR, PI, CLASS"),
        ("team_contractor_headcount.csv", "SELECT TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT, UPDATED_AT, UPDATED_BY FROM TEAM_CONTRACTOR_HEADCOUNT ORDER BY TEAMID, YEAR, PI, CLASS, COMPANYID"),
        ("team_msp_rate.csv", "SELECT TEAMID, MSP_ENABLED, MSP_SIZE, MSP_RATE_PER_PI, MSP_RATE_SMALL, MSP_RATE_MEDIUM, MSP_RATE_LARGE FROM TEAM_MSP_RATE ORDER BY TEAMID"),
        ("team_msp_assignments.csv", "SELECT TEAMID, GROUPID, EFFECTIVE_YEAR, [YEAR], ITERATION_START, ITERATION_END, MSP_SIZE, WEIGHT_PCT, UPDATED_AT FROM TEAM_MSP_ASSIGNMENTS ORDER BY TEAMID, EFFECTIVE_YEAR, GROUPID"),
        ("vendors.csv", "SELECT VENDORID, VENDORNAME FROM VENDORS ORDER BY VENDORNAME"),
        ("application_groups.csv", "SELECT GROUPID, GROUPNAME, DEFAULT_VENDORID, OWNER, TEAMID, PROGRAMID, IS_BASE FROM APPLICATION_GROUPS ORDER BY GROUPNAME"),
        ("app_group_team_links.csv", "SELECT GROUPID, TEAMID, CREATED_AT FROM APP_GROUP_TEAM_LINKS ORDER BY TEAMID, GROUPID"),
        ("applications.csv", "SELECT APPLICATIONID, APPLICATIONNAME, VENDORID, GROUPID, ADD_INFO FROM APPLICATIONS ORDER BY APPLICATIONNAME"),
        ("ado_iteration_calendar.csv", "SELECT ITERATION_SK, ITERATION_PATH, ITERATION_LEVEL3, START_DATE, END_DATE, YEAR, MONTH_KEY, QUARTER_KEY, ITERATION_GRAIN FROM ADO_ITERATION_CALENDAR ORDER BY YEAR DESC, ITERATION_LEVEL3, ITERATION_PATH"),
        ("ado_features.csv", _build_ado_features_export_spec()[0]),
        ("ado_profiles.csv", _ado_profiles_export_sql(include_secrets=include_secrets)),
        ("ado_workitem_lookup.csv", "SELECT WORKITEM_ID, WORKITEM_TYPE, TITLE, STATE, PARENT_ID, CHANGED_DATE, UPDATED_AT FROM ADO_WORKITEM_LOOKUP ORDER BY UPDATED_AT DESC"),
        ("ado_workitem_dates.csv", "SELECT WORKITEM_ID, WORKITEM_TYPE, START_DATE, END_DATE, ITERATION_PATH, UPDATED_AT FROM ADO_WORKITEM_DATES ORDER BY UPDATED_AT DESC, WORKITEM_ID"),
        ("ado_workitem_links.csv", "SELECT SOURCE_ID, TARGET_ID, LINK_CATEGORY, LINK_TYPE, UPDATED_AT FROM ADO_WORKITEM_LINKS ORDER BY UPDATED_AT DESC, SOURCE_ID, TARGET_ID"),
        ("ado_feature_progress.csv", "SELECT FEATURE_ID, PROPOSED_SP, INPROGRESS_SP, COMPLETED_SP, TOTAL_SP, PCT_COMPLETE, UPDATED_AT FROM ADO_FEATURE_PROGRESS ORDER BY UPDATED_AT DESC, FEATURE_ID"),
        ("ado_epic_progress.csv", _build_ado_epic_progress_export_sql()),
        ("roadmap_milestones.csv", "SELECT MILESTONE_ID, TITLE, TARGET_DATE, PI_KEY, EPIC_ID, FEATURE_ID, TAG, SOURCE_TYPE, PROGRAM_NAME, TEAM_NAME, IS_ACTIVE, UPDATED_AT, UPDATED_BY FROM ROADMAP_MILESTONES ORDER BY TARGET_DATE, TITLE"),
        ("map_ado_program.csv", "SELECT ADO_PROGRAM, PROGRAMID FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM ORDER BY ADO_PROGRAM"),
        ("map_ado_team.csv", "SELECT ADO_TEAM_KEY, ADO_TEAM, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, TEAMID FROM MAP_ADO_TEAM_TO_TCO_TEAM ORDER BY ADO_TEAM_KEY"),
        ("map_ado_app.csv", "SELECT ADO_APP, APP_GROUP FROM MAP_ADO_APP_TO_TCO_GROUP ORDER BY ADO_APP"),
        ("tco_commitments.csv", "SELECT COMMITMENT_ID, YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, STATUS, OWNER_EMAIL, OWNER_NAME, TARGET_DATE, NOTES, UPDATED_AT, UPDATED_BY FROM TCO_COMMITMENTS ORDER BY YEAR DESC, PI, PROGRAMNAME, TEAMNAME, GROUPNAME"),
        ("contracts.csv", "SELECT CONTRACT_ID, APPLICATIONID, TEAMID, START_FY, END_FY, RENEWAL_MONTH, ANNUAL_AMOUNT, ESCALATION_PCT, STATUS, AGREEMENT_NUMBER, COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE, TOTAL_CONTRACT_COST FROM CONTRACTS ORDER BY CONTRACT_ID"),
        ("invoices.csv", "SELECT INVOICEID, APPLICATIONID, TEAMID, GROUPID, FISCAL_YEAR, RENEWALDATE, AMOUNT, STATUS, PRODUCT_OWNER, AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE, COMPANY_CODE, COST_CENTER, SERIAL_NUMBER, WORK_ORDER, AGREEMENT_NUMBER, CONTRACT_DUE, SERVICE_TYPE, NOTES, PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING, GROUPID_AT_BOOKING, ROLLOVER_BATCH_ID, ROLLED_OVER_FROM_YEAR, INVOICE_TYPE FROM INVOICES ORDER BY FISCAL_YEAR, TEAMID, APPLICATIONID"),
        ("invoice_notes.csv", "SELECT NOTE_ID, INVOICEID, NOTE_TEXT, CREATED_AT, CREATED_BY FROM INVOICE_NOTES ORDER BY CREATED_AT DESC"),
        ("rollover_log.csv", "SELECT BATCH_ID, FROM_YEAR, TO_YEAR, ROWS_INSERTED, CREATED_AT, CREATED_BY FROM ROLLOVER_LOG ORDER BY CREATED_AT DESC"),
    ]


def _full_backup_schema_tables() -> List[str]:
    return [
        "ADO_FEATURES",
        "ADO_ITERATION_CALENDAR",
        "ADO_PROFILES",
        "ADO_WORKITEM_LOOKUP",
        "ADO_WORKITEM_DATES",
        "ADO_WORKITEM_LINKS",
        "ADO_FEATURE_PROGRESS",
        "ADO_EPIC_PROGRESS",
        "ROADMAP_MILESTONES",
        "PROGRAMS",
        "TEAMS",
        "APPLICATION_GROUPS",
        "APPLICATIONS",
        "MAP_ADO_PROGRAM_TO_TCO_PROGRAM",
        "MAP_ADO_TEAM_TO_TCO_TEAM",
        "MAP_ADO_APP_TO_TCO_GROUP",
        "ADO_PORTFOLIO_SETTINGS",
    ]


def _build_full_backup_zip() -> Tuple[bytes, Dict[str, int]]:
    stats: Dict[str, int] = {}
    dataset_sources: Dict[str, str] = {}
    zbuf = _io.BytesIO()
    with _zip.ZipFile(zbuf, "w", _zip.ZIP_DEFLATED) as zf:
        def _write_df(name: str, df: pd.DataFrame) -> None:
            buf = _io.StringIO()
            df.to_csv(buf, index=False)
            zf.writestr(name, buf.getvalue())
            stats[name] = len(df)

        datasets = _full_backup_dataset_specs(include_secrets=False)
        for name, sql in datasets:
            try:
                dataset_sources[name] = sql
                df = fetch_df(sql)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    _write_df(name, df)
            except Exception:
                continue
        schema_tables = _full_backup_schema_tables()
        manifest_json = _build_backup_manifest(
            dataset_stats=stats,
            dataset_sources=dataset_sources,
            schema_tables=schema_tables,
        )
        zf.writestr("manifest.json", manifest_json)
    return zbuf.getvalue(), stats


def _find_in_zip(zf: _zip.ZipFile, target: str) -> Optional[str]:
    target_lower = target.lower()
    for name in zf.namelist():
        if name.lower().endswith(target_lower):
            return name
    return None


def _read_csv_flexible(fh) -> pd.DataFrame:
    try:
        df_read = pd.read_csv(fh)
        if len(df_read.columns) == 1 and "\t" in str(df_read.columns[0]):
            try:
                fh.seek(0)
            except Exception:
                pass
            df_read = pd.read_csv(fh, sep="\t")
        return df_read
    except UnicodeDecodeError:
        try:
            fh.seek(0)
        except Exception:
            pass
        df_read = pd.read_csv(fh, encoding="latin1")
        if len(df_read.columns) == 1 and "\t" in str(df_read.columns[0]):
            try:
                fh.seek(0)
            except Exception:
                pass
            df_read = pd.read_csv(fh, encoding="latin1", sep="\t")
        return df_read


def _run_post_restore_validation() -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    try:
        df = fetch_df("SELECT 1 AS OK")
        ok = isinstance(df, pd.DataFrame) and not df.empty
        results.append({"check": "DB connectivity", "status": "ok" if ok else "fail", "message": ""})
    except Exception as exc:
        results.append({"check": "DB connectivity", "status": "fail", "message": str(exc)})

    core_tables = ["PROGRAMS", "TEAMS", "APPLICATION_GROUPS"]
    for tbl in core_tables:
        try:
            df = fetch_df(f"SELECT COUNT(*) CNT FROM { _fq(tbl) }")
            cnt = int(df.iloc[0]["CNT"]) if isinstance(df, pd.DataFrame) and not df.empty else 0
            status = "ok" if cnt > 0 else "warn"
            results.append({"check": f"{tbl} rows", "status": status, "message": f"{cnt} rows"})
        except Exception as exc:
            results.append({"check": f"{tbl} rows", "status": "fail", "message": str(exc)})

    for tbl in ["ADO_FEATURES", "ADO_PROFILES", "ADO_WORKITEM_LOOKUP", "ADO_WORKITEM_DATES", "ADO_WORKITEM_LINKS"]:
        try:
            df = fetch_df(f"SELECT COUNT(*) CNT FROM { _fq(tbl) }")
            cnt = int(df.iloc[0]["CNT"]) if isinstance(df, pd.DataFrame) and not df.empty else 0
            status = "ok" if cnt > 0 else "warn"
            results.append({"check": f"{tbl} rows", "status": status, "message": f"{cnt} rows"})
        except Exception as exc:
            results.append({"check": f"{tbl} rows", "status": "warn", "message": str(exc)})

    try:
        df_raw = fetch_df(
            "SELECT COUNT(DISTINCT PROGRAM_RAW) AS N_PROG, COUNT(DISTINCT TEAM_RAW) AS N_TEAM FROM ADO_FEATURES"
        )
        if isinstance(df_raw, pd.DataFrame) and not df_raw.empty:
            n_prog = int(df_raw.iloc[0]["N_PROG"] or 0)
            n_team = int(df_raw.iloc[0]["N_TEAM"] or 0)
            results.append({"check": "ADO raw programs (distinct)", "status": "ok" if n_prog else "warn", "message": f"{n_prog} programs"})
            results.append({"check": "ADO raw teams (distinct)", "status": "ok" if n_team else "warn", "message": f"{n_team} teams"})
    except Exception:
        pass

    for tbl in ["MAP_ADO_PROGRAM_TO_TCO_PROGRAM", "MAP_ADO_TEAM_TO_TCO_TEAM", "MAP_ADO_APP_TO_TCO_GROUP"]:
        try:
            df = fetch_df(f"SELECT COUNT(*) CNT FROM { _fq(tbl) }")
            cnt = int(df.iloc[0]["CNT"]) if isinstance(df, pd.DataFrame) and not df.empty else 0
            results.append({"check": f"{tbl} rows", "status": "ok" if cnt > 0 else "warn", "message": f"{cnt} rows"})
        except Exception as exc:
            results.append({"check": f"{tbl} rows", "status": "warn", "message": str(exc)})

    try:
        df = fetch_df("SELECT TOP 1 PROFILE_NAME FROM ADO_PROFILES WHERE IS_ACTIVE = 1 ORDER BY UPDATED_AT DESC")
        if isinstance(df, pd.DataFrame) and not df.empty:
            results.append({"check": "Active ADO profile", "status": "ok", "message": str(df.iloc[0][0])})
    except Exception:
        pass

    try:
        cols = _get_table_columns("ADO_FEATURES")
        col_set = {c.upper() for c in cols}
        needed = ["PARENT_ID", "EPIC_ID", "EPIC_TITLE", "EPIC_STATE"]
        missing = [c for c in needed if c.upper() not in col_set]
        if missing:
            results.append({"check": "ADO_FEATURES epic columns", "status": "warn", "message": f"Missing: {', '.join(missing)}"})
        else:
            results.append({"check": "ADO_FEATURES epic columns", "status": "ok", "message": "All epic columns present"})
            try:
                df_epic = fetch_df(
                    "SELECT COUNT(*) AS N, SUM(CASE WHEN EPIC_ID IS NOT NULL THEN 1 ELSE 0 END) AS N_EPIC FROM ADO_FEATURES"
                )
                if df_epic is not None and not df_epic.empty:
                    n_total = int(df_epic.iloc[0]["N"] or 0)
                    n_epic = int(df_epic.iloc[0]["N_EPIC"] or 0)
                    if n_total > 0 and n_epic == 0:
                        results.append({
                            "check": "Epic data presence",
                            "status": "warn",
                            "message": "Epic fields present but all null. Epic data not present in this backup.",
                        })
            except Exception:
                pass
    except Exception as exc:
        results.append({"check": "ADO_FEATURES epic columns", "status": "warn", "message": str(exc)})

    try:
        cols = _get_table_columns("ADO_FEATURES")
        col_set = {c.upper() for c in cols}
        if "STORY_POINTS" not in col_set:
            results.append({"check": "ADO_FEATURES story points", "status": "warn", "message": "STORY_POINTS column missing"})
        else:
            df_sp = fetch_df(
                "SELECT COUNT(*) AS N, SUM(CASE WHEN TRY_CONVERT(FLOAT, STORY_POINTS) > 0 THEN 1 ELSE 0 END) AS N_SP FROM ADO_FEATURES"
            )
            if df_sp is not None and not df_sp.empty:
                n_total = int(df_sp.iloc[0]["N"] or 0)
                n_sp = int(df_sp.iloc[0]["N_SP"] or 0)
                pct = (float(n_sp) / float(n_total) * 100.0) if n_total else 0.0
                results.append(
                    {
                        "check": "ADO_FEATURES story points coverage",
                        "status": "ok" if n_total > 0 else "warn",
                        "message": f"{n_sp}/{n_total} ({pct:.1f}%) populated",
                    }
                )
    except Exception as exc:
        results.append({"check": "ADO_FEATURES story points coverage", "status": "warn", "message": str(exc)})

    try:
        ensure_analytics_views_ok()
        results.append({"check": "Analytics views", "status": "ok", "message": "Views ensured"})
    except Exception as exc:
        results.append({"check": "Analytics views", "status": "fail", "message": str(exc)})
    return results


def render_backup_restore_tab() -> None:
    render_section("Backup & Restore")
    render_section("Backups overview")
    actions = get_recent_admin_actions(area="backup_restore", limit=50)
    if actions.empty:
        render_empty_state(
            "Backup history not tracked yet.",
            "Create a backup to populate this list.",
        )
    else:
        hist = actions[actions["ACTION_TYPE"].isin(["backup_create"])].copy()
        if hist.empty:
            render_empty_state(
                "Backup history not tracked yet.",
                "Create a backup to populate this list.",
            )
        else:
            st.dataframe(
                hist[["CREATED_AT", "USER_EMAIL", "SUMMARY"]],
                use_container_width=True,
                height=220,
            )
    if st.session_state.get("admin_last_backup"):
        st.download_button(
            "⬇️ Download latest backup",
            data=st.session_state["admin_last_backup"],
            file_name=st.session_state.get("admin_last_backup_name") or "full_backup.zip",
            mime="application/zip",
            use_container_width=True,
        )

    render_section("Create backup")
    st.caption(
        "Backups include: access/settings, core data, workforce/rates, finance, ADO, and PI readiness tables."
    )
    st.caption("This backup includes ADO iteration calendar and join keys required for PI-based time alignment.")

    exp_col, imp_col = st.columns(2)

    with exp_col:
        st.markdown("#### Export")
        import io as _io

        def _dl_csv(label: str, sql: str, filename: str):
            try:
                df = fetch_df(sql)
            except Exception as e:
                st.warning(f"{label}: {e}")
                return
            if df is None or df.empty:
                st.caption(f"{label}: no rows")
                return
            buf = _io.StringIO(); df.to_csv(buf, index=False)
            st.download_button(f"⬇️ {label}", data=buf.getvalue(), file_name=filename, mime="text/csv", use_container_width=True)

        st.markdown("##### Access & Settings")
        _dl_csv("APP_USERS", "SELECT EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, PROVIDER FROM APP_USERS ORDER BY EMAIL", "app_users.csv")
        _dl_csv("APP_USER_MEMBERSHIP", "SELECT USER_EMAIL, PROGRAMID, TEAMID, CREATED_AT FROM APP_USER_MEMBERSHIP ORDER BY USER_EMAIL, PROGRAMID, TEAMID", "app_user_membership.csv")
        _dl_csv("ADO_PORTFOLIO_SETTINGS", "SELECT PORTFOLIO_NAME, SETTINGS_JSON, UPDATED_AT, UPDATED_BY FROM ADO_PORTFOLIO_SETTINGS ORDER BY PORTFOLIO_NAME", "ado_portfolio_settings.csv")
        _dl_csv("EMAIL_ALERT_CONFIG", "SELECT CONFIG_KEY, CONFIG_JSON, UPDATED_AT, UPDATED_BY FROM EMAIL_ALERT_CONFIG ORDER BY CONFIG_KEY", "email_alert_config.csv")

        st.markdown("##### Core Master Data")
        _dl_csv("PROGRAMS", "SELECT PROGRAMID, PROGRAMNAME, PROGRAM_DISPLAY_NAME, PROGRAMOWNER, PROGRAMFTE, PROGRAM_XOM_RATE FROM PROGRAMS ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)),''), PROGRAMNAME)", "programs.csv")
        _dl_csv("PROGRAM_COMPOSITION_HISTORY", "SELECT PROGRAMID, YEAR, PI, PROGRAMFTE, UPDATED_AT, UPDATED_BY FROM PROGRAM_COMPOSITION_HISTORY ORDER BY PROGRAMID, YEAR, PI", "program_composition_history.csv")
        _dl_csv("TEAMS", "SELECT TEAMID, TEAMNAME, TEAM_DISPLAY_NAME, PROGRAMID, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE, PRODUCTOWNER FROM TEAMS ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)),''), TEAMNAME)", "teams.csv")
        _dl_csv("TEAM_COMPOSITION_HISTORY", "SELECT TEAMID, YEAR, PI, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE, UPDATED_AT FROM TEAM_COMPOSITION_HISTORY ORDER BY TEAMID, YEAR, PI", "team_composition_history.csv")
        _dl_csv("VENDORS", "SELECT VENDORID, VENDORNAME FROM VENDORS ORDER BY VENDORNAME", "vendors.csv")
        _dl_csv("APPLICATION_GROUPS", "SELECT GROUPID, GROUPNAME, DEFAULT_VENDORID, OWNER, TEAMID, PROGRAMID, IS_BASE FROM APPLICATION_GROUPS ORDER BY GROUPNAME", "application_groups.csv")
        _dl_csv("APP_GROUP_TEAM_LINKS", "SELECT GROUPID, TEAMID, CREATED_AT FROM APP_GROUP_TEAM_LINKS ORDER BY TEAMID, GROUPID", "app_group_team_links.csv")
        _dl_csv("APPLICATIONS", "SELECT APPLICATIONID, APPLICATIONNAME, VENDORID, GROUPID, ADD_INFO FROM APPLICATIONS ORDER BY APPLICATIONNAME", "applications.csv")

        st.markdown("##### Workforce, Rates & Commitments")
        _dl_csv("TEAM_RATE_HISTORY", "SELECT TEAMID, YEAR, PI, LOCATION, XOM_RATE, UPDATED_AT, UPDATED_BY FROM TEAM_RATE_HISTORY ORDER BY TEAMID, YEAR, PI, LOCATION", "team_rate_history.csv")
        _dl_csv("PROGRAM_RATE_HISTORY", "SELECT PROGRAMID, YEAR, PI, LOCATION, PROGRAM_XOM_RATE, UPDATED_AT, UPDATED_BY FROM PROGRAM_RATE_HISTORY ORDER BY PROGRAMID, YEAR, PI, LOCATION", "program_rate_history.csv")
        _dl_csv("TEAM_HEADCOUNT_HISTORY", "SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, UPDATED_AT, UPDATED_BY FROM TEAM_HEADCOUNT_HISTORY ORDER BY TEAMID, YEAR, PI, CLASS, LOCATION", "team_headcount_history.csv")
        _dl_csv("CONTRACTOR_COMPANY", "SELECT COMPANYID, NAME, ACTIVE, NOTES, UPDATED_AT, UPDATED_BY FROM CONTRACTOR_COMPANY ORDER BY NAME", "contractor_company.csv")
        _dl_csv("CONTRACTOR_RATE_HISTORY", "SELECT COMPANYID, YEAR, PI, CLASS, RATE, UPDATED_AT, UPDATED_BY FROM CONTRACTOR_RATE_HISTORY ORDER BY COMPANYID, YEAR, PI, CLASS", "contractor_rate_history.csv")
        _dl_csv("TEAM_CONTRACTOR_HEADCOUNT", "SELECT TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT, UPDATED_AT, UPDATED_BY FROM TEAM_CONTRACTOR_HEADCOUNT ORDER BY TEAMID, YEAR, PI, CLASS, COMPANYID", "team_contractor_headcount.csv")
        _dl_csv("TEAM_MSP_RATE", "SELECT TEAMID, MSP_ENABLED, MSP_SIZE, MSP_RATE_PER_PI, MSP_RATE_SMALL, MSP_RATE_MEDIUM, MSP_RATE_LARGE FROM TEAM_MSP_RATE ORDER BY TEAMID", "team_msp_rate.csv")
        _dl_csv("TEAM_MSP_ASSIGNMENTS", "SELECT TEAMID, GROUPID, EFFECTIVE_YEAR, [YEAR], ITERATION_START, ITERATION_END, MSP_SIZE, WEIGHT_PCT, UPDATED_AT FROM TEAM_MSP_ASSIGNMENTS ORDER BY TEAMID, EFFECTIVE_YEAR, GROUPID", "team_msp_assignments.csv")
        _dl_csv("TCO_COMMITMENTS", "SELECT COMMITMENT_ID, YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, STATUS, OWNER_EMAIL, OWNER_NAME, TARGET_DATE, NOTES, UPDATED_AT, UPDATED_BY FROM TCO_COMMITMENTS ORDER BY YEAR DESC, PI, PROGRAMNAME, TEAMNAME, GROUPNAME", "tco_commitments.csv")

        st.markdown("##### Finance")
        _dl_csv(
            "PROGRAM_ADDITIONAL_COSTS",
            "SELECT PROGRAMID, YEAR, MONTH, COST_TYPE, SUBTYPE, DESCRIPTION, CURRENCY, IS_RECURRING, AMOUNT, UPDATED_AT, UPDATED_BY FROM PROGRAM_ADDITIONAL_COSTS ORDER BY PROGRAMID, YEAR DESC, MONTH DESC",
            "program_additional_costs.csv",
        )
        _dl_csv("PROGRAM_APPTIO_WORKIDS", "SELECT WORK_ID, PROGRAMID, UPDATED_AT, UPDATED_BY FROM PROGRAM_APPTIO_WORKIDS ORDER BY PROGRAMID, WORK_ID", "program_apptio_workids.csv")
        _dl_csv("APPTIO_ACTUALS", "SELECT WORK_ID, FISCAL_YEAR, MONTH, AMOUNT, SOURCE, LOADED_AT, LOADED_BY FROM APPTIO_ACTUALS ORDER BY FISCAL_YEAR DESC, MONTH DESC, WORK_ID", "apptio_actuals.csv")
        _dl_csv("CONTRACTS", "SELECT CONTRACT_ID, APPLICATIONID, TEAMID, START_FY, END_FY, RENEWAL_MONTH, ANNUAL_AMOUNT, ESCALATION_PCT, STATUS, AGREEMENT_NUMBER, COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE, TOTAL_CONTRACT_COST FROM CONTRACTS ORDER BY CONTRACT_ID", "contracts.csv")
        _dl_csv("INVOICES", "SELECT INVOICEID, APPLICATIONID, TEAMID, GROUPID, FISCAL_YEAR, RENEWALDATE, AMOUNT, STATUS, PRODUCT_OWNER, AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE, COMPANY_CODE, COST_CENTER, SERIAL_NUMBER, WORK_ORDER, AGREEMENT_NUMBER, CONTRACT_DUE, SERVICE_TYPE, NOTES, PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING, GROUPID_AT_BOOKING, ROLLOVER_BATCH_ID, ROLLED_OVER_FROM_YEAR, INVOICE_TYPE FROM INVOICES ORDER BY FISCAL_YEAR, TEAMID, APPLICATIONID", "invoices.csv")
        _dl_csv("INVOICE_NOTES", "SELECT NOTE_ID, INVOICEID, NOTE_TEXT, CREATED_AT, CREATED_BY FROM INVOICE_NOTES ORDER BY CREATED_AT DESC", "invoice_notes.csv")
        _dl_csv("ROLLOVER_LOG", "SELECT BATCH_ID, FROM_YEAR, TO_YEAR, ROWS_INSERTED, CREATED_AT, CREATED_BY FROM ROLLOVER_LOG ORDER BY CREATED_AT DESC", "rollover_log.csv")

        st.markdown("##### ADO")
        st.caption(
            "Offline Roadmap-by-Epic needs ADO_FEATURES (with EPIC_*), ADO_ITERATION_CALENDAR, ADO_PROFILES, "
            "ADO_WORKITEM_LOOKUP, and Phase C tables (ADO_WORKITEM_DATES / ADO_WORKITEM_LINKS)."
        )
        include_secrets = st.checkbox(
            "Include secrets (PAT) in ADO_PROFILES export",
            value=False,
            key="backup_include_secrets",
            help="Off by default. Only enable if you explicitly want PAT values included.",
        )
        ado_sql, ado_source, ado_has_sp, ado_cols = _build_ado_features_export_spec()
        if not ado_has_sp:
            st.error(f"{ado_source}.STORY_POINTS not found; sync schema is incomplete and coverage may not restore.")
        _dl_csv(
            "ADO Iteration Calendar",
            "SELECT ITERATION_SK, ITERATION_PATH, ITERATION_LEVEL3, START_DATE, END_DATE, YEAR, MONTH_KEY, QUARTER_KEY, ITERATION_GRAIN FROM ADO_ITERATION_CALENDAR ORDER BY YEAR DESC, ITERATION_LEVEL3, ITERATION_PATH",
            "ado_iteration_calendar.csv",
        )
        _dl_csv(
            "ADO_FEATURES",
            ado_sql,
            "ado_features.csv",
        )
        with st.expander("ADO_FEATURES export columns", expanded=False):
            st.code(", ".join(ado_cols) if ado_cols else "(no columns resolved)")
        if _table_exists("ADO_PROFILES"):
            _dl_csv(
                "ADO_PROFILES",
                _ado_profiles_export_sql(include_secrets=include_secrets),
                "ado_profiles.csv",
            )
        else:
            st.caption("ADO_PROFILES: not available")
        if _table_exists("ADO_WORKITEM_LOOKUP"):
            _dl_csv(
                "ADO_WORKITEM_LOOKUP",
                "SELECT WORKITEM_ID, WORKITEM_TYPE, TITLE, STATE, PARENT_ID, CHANGED_DATE, UPDATED_AT FROM ADO_WORKITEM_LOOKUP ORDER BY UPDATED_AT DESC",
                "ado_workitem_lookup.csv",
            )
        else:
            st.caption("ADO_WORKITEM_LOOKUP: not available")
        if _table_exists("ADO_WORKITEM_DATES"):
            _dl_csv(
                "ADO_WORKITEM_DATES",
                "SELECT WORKITEM_ID, WORKITEM_TYPE, START_DATE, END_DATE, ITERATION_PATH, UPDATED_AT FROM ADO_WORKITEM_DATES ORDER BY UPDATED_AT DESC, WORKITEM_ID",
                "ado_workitem_dates.csv",
            )
        else:
            st.caption("ADO_WORKITEM_DATES: not available")
        if _table_exists("ADO_WORKITEM_LINKS"):
            _dl_csv(
                "ADO_WORKITEM_LINKS",
                "SELECT SOURCE_ID, TARGET_ID, LINK_CATEGORY, LINK_TYPE, UPDATED_AT FROM ADO_WORKITEM_LINKS ORDER BY UPDATED_AT DESC, SOURCE_ID, TARGET_ID",
                "ado_workitem_links.csv",
            )
        else:
            st.caption("ADO_WORKITEM_LINKS: not available")
        if _table_exists("ADO_FEATURE_PROGRESS"):
            _dl_csv(
                "ADO_FEATURE_PROGRESS",
                "SELECT FEATURE_ID, PROPOSED_SP, INPROGRESS_SP, COMPLETED_SP, TOTAL_SP, PCT_COMPLETE, UPDATED_AT FROM ADO_FEATURE_PROGRESS ORDER BY UPDATED_AT DESC, FEATURE_ID",
                "ado_feature_progress.csv",
            )
        else:
            st.caption("ADO_FEATURE_PROGRESS: not available")
        if _table_exists("ADO_EPIC_PROGRESS"):
            _dl_csv(
                "ADO_EPIC_PROGRESS",
                _build_ado_epic_progress_export_sql(),
                "ado_epic_progress.csv",
            )
        else:
            st.caption("ADO_EPIC_PROGRESS: not available")
        if _table_exists("ROADMAP_MILESTONES"):
            _dl_csv(
                "ROADMAP_MILESTONES",
                "SELECT MILESTONE_ID, TITLE, TARGET_DATE, PI_KEY, EPIC_ID, FEATURE_ID, TAG, SOURCE_TYPE, PROGRAM_NAME, TEAM_NAME, IS_ACTIVE, UPDATED_AT, UPDATED_BY FROM ROADMAP_MILESTONES ORDER BY TARGET_DATE, TITLE",
                "roadmap_milestones.csv",
            )
        else:
            st.caption("ROADMAP_MILESTONES: not available")
        _dl_csv(
            "ADO → NEXT (Program mapping)",
            "SELECT ADO_PROGRAM, PROGRAMID FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM ORDER BY ADO_PROGRAM",
            "map_ado_program.csv",
        )
        _dl_csv(
            "ADO → NEXT (Team mapping)",
            "SELECT ADO_TEAM_KEY, ADO_TEAM, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, TEAMID FROM MAP_ADO_TEAM_TO_TCO_TEAM ORDER BY ADO_TEAM_KEY",
            "map_ado_team.csv",
        )
        _dl_csv("ADO → NEXT (App→Group mapping)", "SELECT ADO_APP, APP_GROUP FROM MAP_ADO_APP_TO_TCO_GROUP ORDER BY ADO_APP", "map_ado_app.csv")

        # Attachments export as ZIP (CSV manifest + binary files)
        def _attachments_zip_bytes():
            import zipfile as _zip
            df_att = fetch_df("SELECT ATTACHMENT_ID, INVOICEID, FILENAME, MIMETYPE FROM INVOICE_ATTACHMENTS ORDER BY UPLOADED_AT DESC")
            if df_att is None or df_att.empty:
                return None
            buf = _io.BytesIO()
            with _zip.ZipFile(buf, mode="w", compression=_zip.ZIP_DEFLATED) as zf:
                man = _io.StringIO(); df_att.to_csv(man, index=False)
                zf.writestr("attachments_manifest.csv", man.getvalue())
                for _, r in df_att.iterrows():
                    aid = str(r.get("ATTACHMENT_ID"))
                    fn = str(r.get("FILENAME") or f"{aid}")
                    row = fetch_df("SELECT CONTENT FROM INVOICE_ATTACHMENTS WHERE ATTACHMENT_ID=%s", (aid,))
                    if row is not None and not row.empty:
                        content = row.iloc[0].get("CONTENT")
                        if isinstance(content, (bytes, bytearray)):
                            zf.writestr(f"files/{aid}_{fn}", content)
            return buf.getvalue()

        try:
            _att_bytes = _attachments_zip_bytes()
            if _att_bytes:
                st.download_button(
                    "⬇️ INVOICE_ATTACHMENTS (zip)",
                    data=_att_bytes,
                    file_name="invoice_attachments.zip",
                    mime="application/zip",
                    use_container_width=True,
                )
            else:
                st.caption("INVOICE_ATTACHMENTS: no rows")
        except Exception as e:
            st.warning(f"INVOICE_ATTACHMENTS export unavailable: {e}")

        # Manifest-only export (independent of full zip generation)
        if st.button("Download manifest.json only", use_container_width=True):
            try:
                dataset_stats: Dict[str, int] = {}
                dataset_sources: Dict[str, str] = {}
                manifest_tables = [
                    "APP_USERS",
                    "APP_USER_MEMBERSHIP",
                    "PROGRAMS",
                    "TEAMS",
                    "VENDORS",
                    "APPLICATION_GROUPS",
                    "APPLICATIONS",
                    "INVOICES",
                    "CONTRACTS",
                    "ADO_FEATURES",
                    "ADO_ITERATION_CALENDAR",
                    "ADO_PROFILES",
                    "ADO_WORKITEM_LOOKUP",
                    "ADO_WORKITEM_DATES",
                    "ADO_WORKITEM_LINKS",
                    "ADO_FEATURE_PROGRESS",
                    "ADO_EPIC_PROGRESS",
                    "ROADMAP_MILESTONES",
                    "MAP_ADO_PROGRAM_TO_TCO_PROGRAM",
                    "MAP_ADO_TEAM_TO_TCO_TEAM",
                    "MAP_ADO_APP_TO_TCO_GROUP",
                ]
                for tbl in manifest_tables:
                    if not _table_exists(tbl):
                        continue
                    try:
                        df_cnt = fetch_df(f"SELECT COUNT(*) AS N FROM {_fq(tbl)}")
                        n_rows = int(df_cnt.iloc[0]["N"]) if df_cnt is not None and not df_cnt.empty else 0
                    except Exception:
                        n_rows = 0
                    fname = f"{tbl.lower()}.csv"
                    dataset_stats[fname] = n_rows
                    dataset_sources[fname] = f"SELECT * FROM {tbl}"

                schema_tables = [
                    "ADO_FEATURES",
                    "ADO_ITERATION_CALENDAR",
                    "ADO_PROFILES",
                    "ADO_WORKITEM_LOOKUP",
                    "ADO_WORKITEM_DATES",
                    "ADO_WORKITEM_LINKS",
                    "ADO_FEATURE_PROGRESS",
                    "ADO_EPIC_PROGRESS",
                    "ROADMAP_MILESTONES",
                    "PROGRAMS",
                    "TEAMS",
                    "APPLICATION_GROUPS",
                    "APPLICATIONS",
                    "MAP_ADO_PROGRAM_TO_TCO_PROGRAM",
                    "MAP_ADO_TEAM_TO_TCO_TEAM",
                    "MAP_ADO_APP_TO_TCO_GROUP",
                    "ADO_PORTFOLIO_SETTINGS",
                ]
                manifest_json = _build_backup_manifest(
                    dataset_stats=dataset_stats,
                    dataset_sources=dataset_sources,
                    schema_tables=schema_tables,
                )
                manifest_name = f"manifest_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
                st.session_state["admin_last_manifest"] = manifest_json
                st.session_state["admin_last_manifest_name"] = manifest_name
                st.success("Manifest generated.")
            except Exception as e:
                st.warning(f"Manifest build failed: {e}")

        if st.session_state.get("admin_last_manifest"):
            st.download_button(
                "⬇️ Download latest manifest.json",
                data=st.session_state["admin_last_manifest"],
                file_name=st.session_state.get("admin_last_manifest_name") or "manifest.json",
                mime="application/json",
                use_container_width=True,
            )

        # Full backup ZIP (CSV tables + attachments)
        if st.button("Create backup now", use_container_width=True):
            try:
                import zipfile as _zip
                zbuf = _io.BytesIO()
                with _zip.ZipFile(zbuf, mode="w", compression=_zip.ZIP_DEFLATED) as zf:
                    # reuse the above datasets
                    dataset_stats: Dict[str, int] = {}
                    dataset_sources: Dict[str, str] = {}
                    datasets = _full_backup_dataset_specs(include_secrets=include_secrets)
                    for fname, sql in datasets:
                        df = fetch_df(sql)
                        if df is None or df.empty:
                            continue
                        csv_buf = _io.StringIO(); df.to_csv(csv_buf, index=False)
                        zf.writestr(fname, csv_buf.getvalue())
                        dataset_stats[fname] = len(df)
                        dataset_sources[fname] = sql
                    schema_tables = _full_backup_schema_tables()
                    manifest_json = _build_backup_manifest(
                        dataset_stats=dataset_stats,
                        dataset_sources=dataset_sources,
                        schema_tables=schema_tables,
                    )
                    zf.writestr("manifest.json", manifest_json)
                    att_bytes = _attachments_zip_bytes()
                    if att_bytes:
                        # embed attachment files + manifest directly
                        with _zip.ZipFile(_io.BytesIO(att_bytes), 'r') as att_zip:
                            for n in att_zip.namelist():
                                zf.writestr(n, att_zip.read(n))
                filename = f"full_backup_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
                st.session_state["admin_last_backup"] = zbuf.getvalue()
                st.session_state["admin_last_backup_name"] = filename
                log_admin_action(
                    action_type="backup_create",
                    area="backup_restore",
                    entity="system",
                    summary=f"Created backup {filename}",
                    extra_json={"filename": filename},
                )
                st.download_button(
                    "⬇️ FULL_BACKUP (zip)",
                    data=zbuf.getvalue(),
                    file_name=filename,
                    mime="application/zip",
                    use_container_width=True,
                )
            except Exception as e:
                st.warning(f"Full backup build failed: {e}")

    with imp_col:
        render_section("Restore from backup")
        st.caption("Use FULL_BACKUP to recreate an environment from scratch. Single dataset restores are for targeted fixes.")
        restore_mode = st.radio(
            "Restore mode",
            ["FULL_BACKUP (zip)", "Single dataset"],
            horizontal=True,
            key="backup_restore_mode",
        )
        restore_write_mode = st.radio(
            "Restore write behavior",
            ["Merge (upsert)", "Replace (truncate + insert)"],
            horizontal=True,
            key="backup_restore_write_mode",
        )
        replace_confirm_ok = True
        if restore_write_mode == "Replace (truncate + insert)":
            st.warning("Replace mode will delete existing rows before import.")
            confirm = st.text_input("Type REPLACE to confirm", value="", key="backup_restore_replace_confirm")
            replace_confirm_ok = confirm.strip().upper() == "REPLACE"
        if restore_mode == "FULL_BACKUP (zip)":
            ds = "FULL_BACKUP (zip)"
            file = st.file_uploader("FULL_BACKUP zip", type=["zip"], key="imp_full_backup")
            st.caption(
                "Includes access/settings, core data, workforce/rates, finance, ADO tables, and PI readiness tables."
            )
        else:
            datasets = [
                "APP_USERS",
                "APP_USER_MEMBERSHIP",
                "ADO_PORTFOLIO_SETTINGS",
                "EMAIL_ALERT_CONFIG",
                "PROGRAMS",
                "PROGRAM_COMPOSITION_HISTORY",
                "PROGRAM_ADDITIONAL_COSTS",
                "PROGRAM_APPTIO_WORKIDS",
                "APPTIO_ACTUALS",
                "TEAMS",
                "TEAM_COMPOSITION_HISTORY",
                "VENDORS",
                "APPLICATION_GROUPS",
                "APP_GROUP_TEAM_LINKS",
                "APPLICATIONS",
                "TEAM_RATE_HISTORY",
                "PROGRAM_RATE_HISTORY",
                "TEAM_HEADCOUNT_HISTORY",
                "CONTRACTOR_COMPANY",
                "CONTRACTOR_RATE_HISTORY",
                "TEAM_CONTRACTOR_HEADCOUNT",
                "TEAM_MSP_RATE",
                "TEAM_MSP_ASSIGNMENTS",
                "ADO_ITERATION_CALENDAR",
                "ADO_FEATURES",
                "ADO_PROFILES",
                "ADO_WORKITEM_LOOKUP",
                "ADO_WORKITEM_DATES",
                "ADO_WORKITEM_LINKS",
                "ADO_FEATURE_PROGRESS",
                "ADO_EPIC_PROGRESS",
                "ROADMAP_MILESTONES",
                "MAP_ADO_PROGRAM_TO_TCO_PROGRAM",
                "MAP_ADO_TEAM_TO_TCO_TEAM",
                "MAP_ADO_APP_TO_TCO_GROUP",
                "TCO_COMMITMENTS",
                "CONTRACTS",
                "INVOICES",
                "INVOICE_NOTES",
                "ROLLOVER_LOG",
                "INVOICE_ATTACHMENTS (zip)",
            ]
            ds = st.selectbox("Dataset", datasets, index=0)
            ds_help = {
                "PROGRAMS": "Programs and owners; required before Teams.",
                "TEAMS": "Teams; required before Applications/Applications.",
                "APPLICATION_GROUPS": "Application groups (including BASE).",
                "APPLICATIONS": "Applications tied to groups/vendors.",
                "TEAM_RATE_HISTORY": "Team rates by location/year/PI.",
                "PROGRAM_RATE_HISTORY": "Program rates by location/year/PI.",
                "TEAM_HEADCOUNT_HISTORY": "Team headcount by location/year/PI.",
                "TEAM_MSP_ASSIGNMENTS": "MSP allocations per team/group.",
                "APPTIO_ACTUALS": "Apptio actuals (requires PROGRAM_APPTIO_WORKIDS).",
                "ADO_ITERATION_CALENDAR": "ADO iteration start/end dates (calendar) used to align feature timing to real dates.",
                "ADO_FEATURES": "ADO features for workforce allocation.",
                "ADO_PROFILES": "ADO Profiles configuration (active profile selection).",
                "ADO_WORKITEM_LOOKUP": "ADO parent/epic cache used by epic resolver.",
                "ADO_WORKITEM_DATES": "Phase C timeline dates (feature/epic/story start/end for timeline panel).",
                "ADO_WORKITEM_LINKS": "Phase C timeline links (Parent/Child + Dependency edges).",
                "ADO_FEATURE_PROGRESS": "Feature progress add-on (SP rollup by state category).",
                "ADO_EPIC_PROGRESS": "Epic progress add-on (SP rollup + feature structure counts).",
                "ROADMAP_MILESTONES": "Manual roadmap milestones overlay (Phase D).",
                "MAP_ADO_APP_TO_TCO_GROUP": "ADO app mappings; depends on Applications.",
            }
            hint = ds_help.get(ds)
            if hint:
                st.caption(hint)
            file = st.file_uploader("CSV or ZIP file", type=["csv", "zip"], key=f"imp_{ds}")

        def _run_restore() -> Dict[str, Any]:
            try:
                import pandas as _pd

                # ZIP-based paths handled separately below
                def _read_csv_flexible(fh):
                    try:
                        df_read = _pd.read_csv(fh)
                        # Some exports are TSV but renamed to .csv; detect and re-parse.
                        if len(df_read.columns) == 1 and "\t" in str(df_read.columns[0]):
                            try:
                                fh.seek(0)
                            except Exception:
                                pass
                            df_read = _pd.read_csv(fh, sep="\t")
                        return df_read
                    except UnicodeDecodeError:
                        try:
                            fh.seek(0)
                        except Exception:
                            pass
                        df_read = _pd.read_csv(fh, encoding="latin1")
                        if len(df_read.columns) == 1 and "\t" in str(df_read.columns[0]):
                            try:
                                fh.seek(0)
                            except Exception:
                                pass
                            df_read = _pd.read_csv(fh, encoding="latin1", sep="\t")
                        return df_read

                df = None
                if not (ds.endswith("(zip)") or ds == "FULL_BACKUP (zip)"):
                    uploaded_name = str(getattr(file, "name", "") or "")
                    if uploaded_name.lower().endswith(".zip"):
                        import zipfile as _zip, io as _io

                        zbytes = file.getvalue() if hasattr(file, "getvalue") else file.read()
                        ds_to_csv = {
                            "ADO_FEATURES": "ado_features.csv",
                            "ADO_PORTFOLIO_SETTINGS": "ado_portfolio_settings.csv",
                            "ADO_ITERATION_CALENDAR": "ado_iteration_calendar.csv",
                            "ADO_PROFILES": "ado_profiles.csv",
                            "ADO_WORKITEM_LOOKUP": "ado_workitem_lookup.csv",
                            "ADO_WORKITEM_DATES": "ado_workitem_dates.csv",
                            "ADO_WORKITEM_LINKS": "ado_workitem_links.csv",
                            "ADO_FEATURE_PROGRESS": "ado_feature_progress.csv",
                            "ADO_EPIC_PROGRESS": "ado_epic_progress.csv",
                            "ROADMAP_MILESTONES": "roadmap_milestones.csv",
                            "MAP_ADO_PROGRAM_TO_TCO_PROGRAM": "map_ado_program.csv",
                            "MAP_ADO_TEAM_TO_TCO_TEAM": "map_ado_team.csv",
                            "MAP_ADO_APP_TO_TCO_GROUP": "map_ado_app.csv",
                        }
                        target = (ds_to_csv.get(ds) or "").lower()
                        with _zip.ZipFile(_io.BytesIO(zbytes), "r") as zf:
                            members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                            chosen = None
                            if target:
                                for n in members:
                                    if n.replace("\\", "/").split("/")[-1].lower() == target:
                                        chosen = n
                                        break
                            if chosen is None and len(members) == 1:
                                chosen = members[0]
                            if chosen is None:
                                st.error("ZIP must contain a single CSV (or a matching dataset CSV name).")
                                st.stop()
                            with zf.open(chosen) as fh:
                                df = _read_csv_flexible(fh)
                    else:
                        df = _read_csv_flexible(file)
                if df is not None:
                    df = df.where(pd.notna(df), None)
                n = 0
                try:
                    ensure_ado_minimal_tables()
                    ensure_ado_portfolio_settings_table()
                    ensure_ado_iteration_calendar_table()
                    ensure_ado_profiles_table()
                    ensure_ado_workitem_lookup_table()
                    ensure_ado_progress_tables()
                    ensure_ado_timeline_tables()
                    ensure_roadmap_milestones_table()
                except Exception as exc:
                    st.error(f"Schema ensure failed: {exc}")

                replace_table_map = {
                    "APP_USERS": "APP_USERS",
                    "APP_USER_MEMBERSHIP": "APP_USER_MEMBERSHIP",
                    "ADO_PORTFOLIO_SETTINGS": "ADO_PORTFOLIO_SETTINGS",
                    "EMAIL_ALERT_CONFIG": "EMAIL_ALERT_CONFIG",
                    "PROGRAMS": "PROGRAMS",
                    "PROGRAM_COMPOSITION_HISTORY": "PROGRAM_COMPOSITION_HISTORY",
                    "PROGRAM_ADDITIONAL_COSTS": "PROGRAM_ADDITIONAL_COSTS",
                    "PROGRAM_APPTIO_WORKIDS": "PROGRAM_APPTIO_WORKIDS",
                    "APPTIO_ACTUALS": "APPTIO_ACTUALS",
                    "TEAMS": "TEAMS",
                    "TEAM_COMPOSITION_HISTORY": "TEAM_COMPOSITION_HISTORY",
                    "TEAM_RATE_HISTORY": "TEAM_RATE_HISTORY",
                    "PROGRAM_RATE_HISTORY": "PROGRAM_RATE_HISTORY",
                    "TEAM_HEADCOUNT_HISTORY": "TEAM_HEADCOUNT_HISTORY",
                    "CONTRACTOR_COMPANY": "CONTRACTOR_COMPANY",
                    "CONTRACTOR_RATE_HISTORY": "CONTRACTOR_RATE_HISTORY",
                    "TEAM_CONTRACTOR_HEADCOUNT": "TEAM_CONTRACTOR_HEADCOUNT",
                    "TEAM_MSP_RATE": "TEAM_MSP_RATE",
                    "TEAM_MSP_ASSIGNMENTS": "TEAM_MSP_ASSIGNMENTS",
                    "VENDORS": "VENDORS",
                    "APPLICATION_GROUPS": "APPLICATION_GROUPS",
                    "APP_GROUP_TEAM_LINKS": "APP_GROUP_TEAM_LINKS",
                    "APPLICATIONS": "APPLICATIONS",
                    "ADO_ITERATION_CALENDAR": "ADO_ITERATION_CALENDAR",
                    "ADO_FEATURES": "ADO_FEATURES",
                    "ADO_PROFILES": "ADO_PROFILES",
                    "ADO_WORKITEM_LOOKUP": "ADO_WORKITEM_LOOKUP",
                    "ADO_WORKITEM_DATES": "ADO_WORKITEM_DATES",
                    "ADO_WORKITEM_LINKS": "ADO_WORKITEM_LINKS",
                    "ADO_FEATURE_PROGRESS": "ADO_FEATURE_PROGRESS",
                    "ADO_EPIC_PROGRESS": "ADO_EPIC_PROGRESS",
                    "ROADMAP_MILESTONES": "ROADMAP_MILESTONES",
                    "MAP_ADO_PROGRAM_TO_TCO_PROGRAM": "MAP_ADO_PROGRAM_TO_TCO_PROGRAM",
                    "MAP_ADO_TEAM_TO_TCO_TEAM": "MAP_ADO_TEAM_TO_TCO_TEAM",
                    "MAP_ADO_APP_TO_TCO_GROUP": "MAP_ADO_APP_TO_TCO_GROUP",
                    "TCO_COMMITMENTS": "TCO_COMMITMENTS",
                    "CONTRACTS": "CONTRACTS",
                    "INVOICES": "INVOICES",
                    "INVOICE_NOTES": "INVOICE_NOTES",
                    "ROLLOVER_LOG": "ROLLOVER_LOG",
                    "INVOICE_ATTACHMENTS (zip)": "INVOICE_ATTACHMENTS",
                }

                def _maybe_replace_dataset(dsname: str) -> None:
                    if restore_write_mode != "Replace (truncate + insert)":
                        return
                    table = replace_table_map.get(dsname)
                    if not table:
                        return
                    try:
                        execute(f"DELETE FROM {_fq(table)}")
                    except Exception as exc:
                        st.warning(f"Replace mode: could not clear {table}: {exc}")
                def _norm_key(val: Any) -> str:
                    return str(val or "").strip().lower()

                def _load_group_name_map() -> Dict[str, str]:
                    try:
                        df_map = fetch_df(f"SELECT GROUPID, GROUPNAME FROM {_fq('APPLICATION_GROUPS')}")
                    except Exception:
                        return {}
                    if df_map is None or df_map.empty:
                        return {}
                    out: Dict[str, str] = {}
                    for _, row in df_map.iterrows():
                        name = str(row.get("GROUPNAME") or "").strip()
                        gid = str(row.get("GROUPID") or "").strip()
                        if name and gid:
                            out[_norm_key(name)] = gid
                    return out

                def _resolve_group_id(
                    group_name: str,
                    provided_id: Optional[str],
                    name_map: Dict[str, str],
                    id_map: Optional[Dict[str, str]] = None,
                ) -> str:
                    key = _norm_key(group_name)
                    existing = name_map.get(key)
                    chosen = existing or (provided_id or str(uuid.uuid4()))
                    name_map[key] = chosen
                    if id_map is not None and provided_id:
                        id_map[str(provided_id)] = chosen
                    return chosen

                def _remap_group_id(val: Any, id_map: Dict[str, str]) -> Optional[str]:
                    if val is None:
                        return None
                    sval = str(val).strip()
                    if not sval:
                        return None
                    return id_map.get(sval, sval)
                BUSINESS_VALUE_ALIASES = ["BUSINESS_VALUE", "BusinessValue", "Business Value", "Microsoft.VSTS.Common.BusinessValue", "Microsoft_VSTS_Common_BusinessValue"]
                TEAM_VARIANT_ALIASES = ["TEAM_VARIANT_KEY", "TEAM_VARIANTKEY", "TEAM_VARIANT"]

                def _find_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
                    col_map = {str(c).strip().lower(): c for c in df.columns}
                    for cand in candidates:
                        key = str(cand).strip().lower()
                        if key in col_map:
                            return col_map[key]
                    return None

                def _ensure_column(df: pd.DataFrame, canonical: str, aliases: Sequence[str]) -> None:
                    if canonical in df.columns:
                        return
                    match = _find_col(df, [canonical, *aliases])
                    if match:
                        df[canonical] = df[match]

                def _normalize_ado_feature_columns(df: pd.DataFrame) -> None:
                    _ensure_column(df, "BUSINESS_VALUE", BUSINESS_VALUE_ALIASES)
                    _ensure_column(df, "TEAM_VARIANT_KEY", TEAM_VARIANT_ALIASES)
                    _ensure_column(df, "PROGRAM_RAW", ["PROGRAMNAME", "PROGRAM_NAME"])
                    _ensure_column(df, "TEAM_RAW", ["TEAMNAME", "TEAM_NAME"])
                    _ensure_column(df, "AREA_LEVEL2_RAW", ["AREA_LEVEL2_RAW", "AREALEVEL2"])
                    _ensure_column(df, "AREA_LEVEL3_RAW", ["AREA_LEVEL3_RAW", "AREALEVEL3"])
                    _ensure_column(df, "AREA_LEVEL4_RAW", ["AREA_LEVEL4_RAW", "AREALEVEL4"])
                    _ensure_column(df, "AREA_PATH_RAW", ["AREA_PATH_RAW", "AREAPATH"])
                    _ensure_column(df, "APP_NAME_RAW", ["APP_NAME_RAW", "APPNAME", "APP_NAME", "GROUPNAME"])
                    _ensure_column(df, "INVESTMENT_DIMENSION", ["INVESTMENT_DIMENSION_RAW", "INVESTMENTDIMENSION"])
                    _ensure_column(df, "PARENT_ID", ["ParentWorkItemId", "PARENTWORKITEMID", "PARENT_WORKITEM_ID"])
                    _ensure_column(df, "EPIC_ID", ["EPICID", "EPIC_ID"])
                    _ensure_column(df, "EPIC_TITLE", ["EPIC_TITLE", "EPICNAME", "EPIC_NAME"])
                    _ensure_column(df, "EPIC_STATE", ["EPIC_STATE", "EPICSTATE"])
                    _ensure_column(df, "STORY_POINTS", ["STORYPOINTS", "STORY_POINTS", "EFFORT_POINTS", "EFFORT"])
                    _ensure_column(df, "EFFORT_POINTS", ["EFFORT_POINTS", "EFFORT", "STORYPOINTS"])

                def _normalize_ado_iteration_calendar(df: pd.DataFrame) -> pd.DataFrame:
                    work = df.copy()
                    required_cols = ["ITERATION_PATH", "ITERATION_LEVEL3", "START_DATE", "END_DATE"]
                    for c in required_cols:
                        if c not in work.columns:
                            work[c] = None
                    for c in ["ITERATION_PATH", "ITERATION_LEVEL3", "ITERATION_GRAIN"]:
                        if c in work.columns:
                            work[c] = work[c].astype(str).str.strip().replace({"": None, "nan": None, "None": None})
                    if "ITERATION_SK" in work.columns:
                        work["ITERATION_SK"] = work["ITERATION_SK"].astype(str).str.strip().replace({"": None, "nan": None, "None": None, "<NA>": None})
                    # Dates -> python date
                    for dcol in ["START_DATE", "END_DATE"]:
                        parsed = pd.to_datetime(work.get(dcol), errors="coerce")
                        work[dcol] = parsed.map(lambda x: None if pd.isna(x) else x.date())
                    # Derive missing year/month/quarter from START_DATE
                    start_dt = pd.to_datetime(work.get("START_DATE"), errors="coerce")
                    if "YEAR" not in work.columns or work["YEAR"].isna().all():
                        work["YEAR"] = start_dt.dt.year.astype("Int64")
                    if "MONTH_KEY" not in work.columns or work["MONTH_KEY"].isna().all():
                        work["MONTH_KEY"] = start_dt.dt.strftime("%Y-%m").where(start_dt.notna(), None)
                    if "QUARTER_KEY" not in work.columns or work["QUARTER_KEY"].isna().all():
                        q = ((start_dt.dt.month - 1) // 3 + 1).astype("Int64")
                        work["QUARTER_KEY"] = (start_dt.dt.year.astype("Int64").astype(str) + "-Q" + q.astype("Int64").astype(str)).where(start_dt.notna(), None)
                    if "ITERATION_GRAIN" not in work.columns or work["ITERATION_GRAIN"].isna().all():
                        def _grain(path: Any) -> Optional[str]:
                            s = str(path or "").strip()
                            if not s:
                                return None
                            s_u = s.upper().replace("\\", "/")
                            return "SPRINT" if ("/S" in s_u or " S" in s_u) else "PI"
                        work["ITERATION_GRAIN"] = work.get("ITERATION_PATH").apply(_grain)
                    return work
                # Users
                if ds not in ("FULL_BACKUP (zip)",):
                    _maybe_replace_dataset(ds)
                if ds == "APP_USERS":
                    for _, r in df.iterrows():
                        em = str(r.get("EMAIL") or "").strip()
                        if not em:
                            continue
                        upsert_app_user(
                            email=em,
                            display_name=str(r.get("DISPLAY_NAME") or None) or None,
                            role=str(r.get("ROLE") or "VIEWER").upper(),
                            is_active=bool(r.get("IS_ACTIVE", True)),
                            provider=str(r.get("PROVIDER") or None) or None,
                        )
                        n += 1
                elif ds == "APP_USER_MEMBERSHIP":
                    ensure_user_membership_tables()
                    rows = []
                    for _, r in df.iterrows():
                        email = str(r.get("USER_EMAIL") or "").strip()
                        if not email:
                            continue
                        pid = str(r.get("PROGRAMID") or "").strip()
                        tid = str(r.get("TEAMID") or "").strip()
                        rows.append((email, pid, tid))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('APP_USER_MEMBERSHIP')} t
                            USING (SELECT %s USER_EMAIL, %s PROGRAMID, %s TEAMID) s
                            ON t.USER_EMAIL = s.USER_EMAIL AND t.PROGRAMID = s.PROGRAMID AND t.TEAMID = s.TEAMID
                            WHEN NOT MATCHED THEN INSERT (USER_EMAIL, PROGRAMID, TEAMID) VALUES (s.USER_EMAIL, s.PROGRAMID, s.TEAMID);
                            """,
                            rows,
                            many=True,
                        )
                        n = len(rows)
                elif ds == "ADO_PORTFOLIO_SETTINGS":
                    ensure_ado_portfolio_settings_table()
                    rows = []
                    for _, r in df.iterrows():
                        name = str(r.get("PORTFOLIO_NAME") or "").strip()
                        if not name:
                            continue
                        settings = str(r.get("SETTINGS_JSON") or "")
                        updated_by = str(r.get("UPDATED_BY") or "").strip() or None
                        rows.append((name, settings, updated_by))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('ADO_PORTFOLIO_SETTINGS')} t
                            USING (SELECT %s PORTFOLIO_NAME, %s SETTINGS_JSON, %s UPDATED_BY) s
                            ON t.PORTFOLIO_NAME = s.PORTFOLIO_NAME
                            WHEN MATCHED THEN UPDATE SET SETTINGS_JSON = s.SETTINGS_JSON, UPDATED_AT = SYSDATETIME(), UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
                            WHEN NOT MATCHED THEN INSERT (PORTFOLIO_NAME, SETTINGS_JSON, UPDATED_AT, UPDATED_BY)
                            VALUES (s.PORTFOLIO_NAME, s.SETTINGS_JSON, SYSDATETIME(), s.UPDATED_BY);
                            """,
                            rows,
                            many=True,
                        )
                        n = len(rows)
                elif ds == "EMAIL_ALERT_CONFIG":
                    ensure_email_alert_config_table()
                    rows = []
                    for _, r in df.iterrows():
                        key = str(r.get("CONFIG_KEY") or "").strip()
                        if not key:
                            continue
                        cfg = str(r.get("CONFIG_JSON") or "")
                        updated_by = str(r.get("UPDATED_BY") or "").strip() or None
                        rows.append((key, cfg, updated_by))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('EMAIL_ALERT_CONFIG')} t
                            USING (SELECT %s CONFIG_KEY, %s CONFIG_JSON, %s UPDATED_BY) s
                            ON t.CONFIG_KEY = s.CONFIG_KEY
                            WHEN MATCHED THEN UPDATE SET CONFIG_JSON = s.CONFIG_JSON, UPDATED_AT = SYSDATETIME(), UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
                            WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_JSON, UPDATED_AT, UPDATED_BY)
                            VALUES (s.CONFIG_KEY, s.CONFIG_JSON, SYSDATETIME(), s.UPDATED_BY);
                            """,
                            rows,
                            many=True,
                        )
                        n = len(rows)
                # ADO Features
                elif ds == "ADO_ITERATION_CALENDAR":
                    ensure_ado_iteration_calendar_table()
                    required = ["ITERATION_PATH", "ITERATION_LEVEL3", "START_DATE", "END_DATE"]
                    missing = [c for c in required if c not in df.columns]
                    if missing:
                        st.error(f"ADO_ITERATION_CALENDAR missing required columns: {missing}")
                    else:
                        if "ITERATION_SK" not in df.columns:
                            st.warning("IterationSK missing in backup; path-based join will be used.")
                        work = _normalize_ado_iteration_calendar(df)
                        # Ensure optional columns exist
                        for c in ["ITERATION_SK", "YEAR", "MONTH_KEY", "QUARTER_KEY", "ITERATION_GRAIN"]:
                            if c not in work.columns:
                                work[c] = None
                        if "ITERATION_SK" in work.columns and work["ITERATION_SK"].isna().all():
                            st.warning("IterationSK missing in backup; path-based join will be used.")
                        cols = ["ITERATION_PATH", "ITERATION_SK", "ITERATION_LEVEL3", "ITERATION_GRAIN", "START_DATE", "END_DATE", "YEAR", "MONTH_KEY", "QUARTER_KEY"]
                        work = work[cols].copy()
                        work = work[work["ITERATION_PATH"].notna()].copy()
                        n = int(upsert_ado_iteration_calendar(work))
                        try:
                            df_null = fetch_df(
                                "SELECT COUNT(*) AS N_NULL FROM ADO_ITERATION_CALENDAR WHERE ITERATION_SK IS NULL OR LTRIM(RTRIM(CONVERT(NVARCHAR(100), ITERATION_SK))) = ''"
                            )
                            n_null = int(df_null.iloc[0]["N_NULL"]) if df_null is not None and not df_null.empty else 0
                            if n_null > 0:
                                st.warning(f"Iteration calendar restore incomplete: ITERATION_SK still NULL for {n_null:,} row(s).")
                        except Exception:
                            pass
                        st.session_state["_restore_ado_iteration_calendar_done"] = True
                elif ds == "ADO_FEATURES":
                    ensure_ado_minimal_tables()
                    required = ["FEATURE_ID"]
                    optional = [
                        "TITLE",
                        "STATE",
                        "ITERATION_LEVEL3",
                        "ITERATION_SK",
                        "PROGRAM_RAW","AREA_LEVEL2_RAW","AREA_LEVEL3_RAW","AREA_LEVEL4_RAW","AREA_PATH_RAW","TEAM_VARIANT_KEY",
                        "BUSINESS_VALUE",
                        "STORY_POINTS","EFFORT_POINTS",
                        "PARENT_ID","EPIC_ID","EPIC_TITLE","EPIC_STATE",
                        "ADO_YEAR",
                        "ITERATION_PATH",
                        "CREATED_AT",
                        "CHANGED_AT",
                        "INVESTMENT_DIMENSION",
                        "APP_NAME_RAW",
                        "TEAM_RAW",
                    ]
                    df.columns = [str(c).strip() for c in df.columns]
                    _normalize_ado_feature_columns(df)
                    df = repair_leaf_teams(df)
                    db_cols = _get_table_columns("ADO_FEATURES")
                    db_map = {str(c).strip().upper(): str(c).strip() for c in db_cols}
                    df_map = {str(c).strip().upper(): c for c in df.columns}
                    rename_map: Dict[str, str] = {}
                    for upper, src in df_map.items():
                        if upper in db_map and src != db_map[upper]:
                            rename_map[src] = db_map[upper]
                    if rename_map:
                        df.rename(columns=rename_map, inplace=True)

                    if "CHANGED_AT" not in df.columns and "CHANGED_DATE" in df.columns and "CHANGED_AT" in db_map.values():
                        df["CHANGED_AT"] = df["CHANGED_DATE"]
                    if "CHANGED_DATE" not in df.columns and "CHANGED_AT" in df.columns and "CHANGED_DATE" in db_map.values():
                        df["CHANGED_DATE"] = df["CHANGED_AT"]
                    if "CREATED_AT" not in df.columns and "CREATED_DATE" in df.columns and "CREATED_AT" in db_map.values():
                        df["CREATED_AT"] = df["CREATED_DATE"]
                    if "CREATED_DATE" not in df.columns and "CREATED_AT" in df.columns and "CREATED_DATE" in db_map.values():
                        df["CREATED_DATE"] = df["CREATED_AT"]
                    if "INVESTMENT_DIMENSION" not in df.columns and "INVESTMENT_DIMENSION_RAW" in df.columns and "INVESTMENT_DIMENSION" in db_map.values():
                        df["INVESTMENT_DIMENSION"] = df["INVESTMENT_DIMENSION_RAW"]

                    missing_required = [c for c in required if c not in df.columns]
                    if missing_required:
                        st.error(f"Missing required columns (cannot restore): {missing_required}")
                    else:
                        missing_optional = [c for c in optional if c not in df.columns]
                        if missing_optional:
                            st.info(f"Missing optional columns (OK): {', '.join(missing_optional)}")
                        if db_cols:
                            db_upper = {str(c).strip().upper() for c in db_cols}
                            extra_cols = [c for c in df.columns if str(c).strip().upper() not in db_upper]
                            if extra_cols:
                                st.info(f"Dropping {len(extra_cols)} column(s) not present in DB: {', '.join(extra_cols[:12])}")
                                df = df.drop(columns=extra_cols, errors="ignore")
                        for col in optional:
                            if col not in df.columns:
                                df[col] = None
                        if "STORY_POINTS" in df.columns:
                            df["STORY_POINTS"] = pd.to_numeric(df["STORY_POINTS"], errors="coerce")
                        if "EFFORT_POINTS" in df.columns:
                            df["EFFORT_POINTS"] = pd.to_numeric(df["EFFORT_POINTS"], errors="coerce")
                        total = len(df)
                        prog = st.progress(0, text="Starting import…")
                        status = st.empty()
                        done = 0
                        n = 0
                        chunk_size = 1000 if total > 5000 else (500 if total > 2000 else 250)
                        for start in range(0, total, chunk_size):
                            stop = min(start + chunk_size, total)
                            status.write(f"Upserting rows {start+1:,} to {stop:,} of {total:,}…")
                            n += upsert_ado_features(df.iloc[start:stop], allow_null_overwrite=True)
                            done = stop
                            prog.progress(done/total, text=f"Progress: {int((done/total)*100)}%")
                        prog.empty()
                        st.success(f"Imported {n} rows into ADO_FEATURES")
                        try:
                            df_cov = fetch_df(
                                "SELECT COUNT(*) AS N, SUM(CASE WHEN TRY_CONVERT(FLOAT, STORY_POINTS) > 0 THEN 1 ELSE 0 END) AS N_SP FROM ADO_FEATURES"
                            )
                            if df_cov is not None and not df_cov.empty:
                                n_total = int(df_cov.iloc[0]["N"] or 0)
                                n_sp = int(df_cov.iloc[0]["N_SP"] or 0)
                                pct = (float(n_sp) / float(n_total) * 100.0) if n_total else 0.0
                                st.caption(f"Story Points coverage: {n_sp}/{n_total} ({pct:.1f}%)")
                                if n_sp <= 0 and n_total > 0:
                                    st.error("Restore completed but STORY_POINTS not persisted to DB.")
                        except Exception:
                            pass
                        try:
                            _backup_restore_post_write_refresh("admin_backup_restore", bump_version=True)
                        except Exception:
                            pass
                        st.session_state["_restore_ado_features_done"] = True
                elif ds == "ADO_PROFILES":
                    ensure_ado_profiles_table()
                    _ensure_column(df, "PROFILE_ID", ["PROFILEID", "PROFILE_ID"])
                    _ensure_column(df, "PROFILE_NAME", ["PROFILENAME", "PROFILE_NAME"])
                    _ensure_column(df, "IS_ACTIVE", ["ISACTIVE", "IS_ACTIVE"])
                    _ensure_column(df, "CONFIG_JSON", ["CONFIGJSON", "CONFIG_JSON"])
                    _ensure_column(df, "UPDATED_AT", ["UPDATEDAT", "UPDATED_AT"])
                    _ensure_column(df, "UPDATED_BY", ["UPDATEDBY", "UPDATED_BY"])
                    active_candidates: List[Tuple[str, Optional[pd.Timestamp]]] = []
                    for _, r in df.iterrows():
                        pid = str(r.get("PROFILE_ID") or "").strip() or None
                        pname = str(r.get("PROFILE_NAME") or "").strip() or None
                        config_raw = r.get("CONFIG_JSON")
                        config_str = str(config_raw) if config_raw is not None else "{}"
                        is_active = bool(_to_bool(r.get("IS_ACTIVE")) or False)
                        updated_by = str(r.get("UPDATED_BY") or "").strip() or None
                        upsert_ado_profile(
                            profile_id=pid,
                            profile_name=pname,
                            config_json=config_str,
                            is_active=is_active,
                            updated_by=updated_by,
                        )
                        n += 1
                        if is_active and pid:
                            upd = pd.to_datetime(r.get("UPDATED_AT"), errors="coerce")
                            active_candidates.append((pid, upd if not pd.isna(upd) else None))
                    if active_candidates:
                        active_candidates.sort(key=lambda x: (x[1] is not None, x[1]), reverse=True)
                        set_active_ado_profile(active_candidates[0][0])
                    st.success(f"Imported {n} rows into ADO_PROFILES")
                elif ds == "ADO_WORKITEM_LOOKUP":
                    ensure_ado_workitem_lookup_table()
                    _ensure_column(df, "WORKITEM_ID", ["WORKITEMID", "WORK_ITEM_ID"])
                    _ensure_column(df, "WORKITEM_TYPE", ["WORKITEMTYPE", "WORK_ITEM_TYPE"])
                    _ensure_column(df, "TITLE", ["TITLE_TEXT", "WORKITEM_TITLE"])
                    _ensure_column(df, "STATE", ["WORKITEM_STATE"])
                    _ensure_column(df, "PARENT_ID", ["PARENTID", "PARENT_WORKITEM_ID", "PARENTWORKITEMID"])
                    _ensure_column(df, "CHANGED_DATE", ["CHANGEDDATE", "CHANGED_AT", "CHANGED_AT"])
                    rows: List[Tuple[Any, ...]] = []
                    for _, r in df.iterrows():
                        wid = _to_int(r.get("WORKITEM_ID"))
                        if wid is None:
                            continue
                        pval = _to_int(r.get("PARENT_ID"))
                        cdt_raw = pd.to_datetime(r.get("CHANGED_DATE"), errors="coerce")
                        cdt = None if pd.isna(cdt_raw) else cdt_raw.to_pydatetime()
                        rows.append(
                            (
                                wid,
                                str(r.get("WORKITEM_TYPE") or "").strip() or None,
                                str(r.get("TITLE") or "").strip() or None,
                                str(r.get("STATE") or "").strip() or None,
                                pval,
                                cdt,
                            )
                        )
                    if rows:
                        upsert_ado_workitem_lookup(rows)
                    n = len(rows)
                    st.success(f"Imported {n} rows into ADO_WORKITEM_LOOKUP")
                elif ds == "ADO_WORKITEM_DATES":
                    ensure_ado_timeline_tables()
                    _ensure_column(df, "WORKITEM_ID", ["WORKITEMID", "WORK_ITEM_ID"])
                    _ensure_column(df, "WORKITEM_TYPE", ["WORKITEMTYPE", "WORK_ITEM_TYPE"])
                    _ensure_column(df, "START_DATE", ["STARTDATE"])
                    _ensure_column(df, "END_DATE", ["ENDDATE", "TARGET_DATE"])
                    _ensure_column(df, "ITERATION_PATH", ["ITERATIONPATH"])
                    rows: List[Tuple[Any, ...]] = []
                    for _, r in df.iterrows():
                        wid = _to_int(r.get("WORKITEM_ID"))
                        if wid is None:
                            continue
                        sdt_raw = pd.to_datetime(r.get("START_DATE"), errors="coerce")
                        edt_raw = pd.to_datetime(r.get("END_DATE"), errors="coerce")
                        rows.append(
                            (
                                wid,
                                str(r.get("WORKITEM_TYPE") or "").strip() or None,
                                None if pd.isna(sdt_raw) else sdt_raw.date(),
                                None if pd.isna(edt_raw) else edt_raw.date(),
                                str(r.get("ITERATION_PATH") or "").strip() or None,
                            )
                        )
                    if rows:
                        upsert_ado_workitem_dates(rows)
                    n = len(rows)
                    st.success(f"Imported {n} rows into ADO_WORKITEM_DATES")
                elif ds == "ADO_WORKITEM_LINKS":
                    ensure_ado_timeline_tables()
                    _ensure_column(df, "SOURCE_ID", ["SOURCEID", "SOURCE_WORKITEM_ID", "SOURCEWORKITEMID"])
                    _ensure_column(df, "TARGET_ID", ["TARGETID", "TARGET_WORKITEM_ID", "TARGETWORKITEMID"])
                    _ensure_column(df, "LINK_CATEGORY", ["LINKCATEGORY", "LINK_CATEGORY_NAME"])
                    _ensure_column(df, "LINK_TYPE", ["LINKTYPE", "LINK_TYPE_NAME", "LINKTYPENAME"])
                    rows: List[Tuple[Any, ...]] = []
                    for _, r in df.iterrows():
                        sid = _to_int(r.get("SOURCE_ID"))
                        tid = _to_int(r.get("TARGET_ID"))
                        if sid is None or tid is None:
                            continue
                        rows.append(
                            (
                                sid,
                                tid,
                                str(r.get("LINK_CATEGORY") or "").strip() or "Other",
                                str(r.get("LINK_TYPE") or "").strip() or "Other",
                            )
                        )
                    if rows:
                        upsert_ado_workitem_links(rows)
                    n = len(rows)
                    st.success(f"Imported {n} rows into ADO_WORKITEM_LINKS")
                elif ds == "ADO_FEATURE_PROGRESS":
                    ensure_ado_progress_tables()
                    _ensure_column(df, "FEATURE_ID", ["WORKITEM_ID", "WORK_ITEM_ID"])
                    rows: List[Tuple[Any, ...]] = []
                    for _, r in df.iterrows():
                        fid = _to_int(r.get("FEATURE_ID"))
                        if fid is None:
                            continue
                        rows.append(
                            (
                                fid,
                                _to_float(r.get("PROPOSED_SP")) or 0.0,
                                _to_float(r.get("INPROGRESS_SP")) or 0.0,
                                _to_float(r.get("COMPLETED_SP")) or 0.0,
                                _to_float(r.get("TOTAL_SP")) or 0.0,
                                _to_float(r.get("PCT_COMPLETE")) or 0.0,
                            )
                        )
                    if rows:
                        upsert_ado_feature_progress(rows)
                    n = len(rows)
                    st.success(f"Imported {n} rows into ADO_FEATURE_PROGRESS")
                    try:
                        bump_data_version("restore_ado_feature_progress")
                    except Exception:
                        pass
                    st.session_state["ado_data_cache_bust"] = int(st.session_state.get("ado_data_cache_bust", 0) or 0) + 1
                    st.session_state["_cache_epoch"] = int(st.session_state.get("_cache_epoch", 0) or 0) + 1
                    try:
                        _backup_restore_post_write_refresh("admin_backup_restore", bump_version=True)
                    except Exception:
                        pass
                elif ds == "ADO_EPIC_PROGRESS":
                    ensure_ado_progress_tables()
                    _ensure_column(df, "EPIC_ID", ["WORKITEM_ID", "WORK_ITEM_ID"])
                    rows: List[Tuple[Any, ...]] = []
                    for _, r in df.iterrows():
                        eid = _to_int(r.get("EPIC_ID"))
                        if eid is None:
                            continue
                        rows.append(
                            (
                                eid,
                                _to_float(r.get("PROPOSED_SP")) or 0.0,
                                _to_float(r.get("INPROGRESS_SP")) or 0.0,
                                _to_float(r.get("COMPLETED_SP")) or 0.0,
                                _to_float(r.get("TOTAL_SP")) or 0.0,
                                _to_float(r.get("PCT_COMPLETE")) or 0.0,
                                _to_int(r.get("FEATURE_COUNT_TOTAL")),
                                _to_int(r.get("FEATURE_COUNT_WITH_STORIES")),
                                _to_int(r.get("FEATURE_COUNT_WITH_SP")),
                            )
                        )
                    if rows:
                        upsert_ado_epic_progress(rows)
                    n = len(rows)
                    st.success(f"Imported {n} rows into ADO_EPIC_PROGRESS")
                    try:
                        bump_data_version("restore_ado_epic_progress")
                    except Exception:
                        pass
                    st.session_state["ado_data_cache_bust"] = int(st.session_state.get("ado_data_cache_bust", 0) or 0) + 1
                    st.session_state["_cache_epoch"] = int(st.session_state.get("_cache_epoch", 0) or 0) + 1
                    try:
                        _backup_restore_post_write_refresh("admin_backup_restore", bump_version=True)
                    except Exception:
                        pass
                elif ds == "ROADMAP_MILESTONES":
                    ensure_roadmap_milestones_table()
                    _ensure_column(df, "MILESTONE_ID", ["MILESTONEID", "ID"])
                    _ensure_column(df, "TITLE", ["NAME", "MILESTONE_NAME"])
                    _ensure_column(df, "TARGET_DATE", ["DATE", "MILESTONE_DATE", "TARGETDATE"])
                    _ensure_column(df, "PI_KEY", ["PI", "PI_LABEL"])
                    _ensure_column(df, "EPIC_ID", ["EPICID"])
                    _ensure_column(df, "FEATURE_ID", ["FEATUREID"])
                    _ensure_column(df, "TAG", ["MILESTONE_TAG"])
                    _ensure_column(df, "SOURCE_TYPE", ["SOURCE", "TYPE"])
                    _ensure_column(df, "PROGRAM_NAME", ["PROGRAM", "PROGRAMNAME"])
                    _ensure_column(df, "TEAM_NAME", ["TEAM", "TEAMNAME"])
                    _ensure_column(df, "IS_ACTIVE", ["ACTIVE"])
                    _ensure_column(df, "UPDATED_BY", ["UPDATEDBY"])
                    rows: List[Tuple[Any, ...]] = []
                    for _, r in df.iterrows():
                        mid = str(r.get("MILESTONE_ID") or "").strip()
                        title = str(r.get("TITLE") or "").strip()
                        dt_raw = pd.to_datetime(r.get("TARGET_DATE"), errors="coerce")
                        if not mid or not title or pd.isna(dt_raw):
                            continue
                        rows.append(
                            (
                                mid,
                                title,
                                dt_raw.date(),
                                str(r.get("PI_KEY") or "").strip() or None,
                                _to_int(r.get("EPIC_ID")),
                                _to_int(r.get("FEATURE_ID")),
                                str(r.get("TAG") or "").strip() or None,
                                str(r.get("SOURCE_TYPE") or "").strip().upper() or None,
                                str(r.get("PROGRAM_NAME") or "").strip() or None,
                                str(r.get("TEAM_NAME") or "").strip() or None,
                                bool(_to_bool(r.get("IS_ACTIVE"))),
                                str(r.get("UPDATED_BY") or "").strip() or None,
                            )
                        )
                    if rows:
                        upsert_roadmap_milestones(rows)
                    n = len(rows)
                    st.success(f"Imported {n} rows into ROADMAP_MILESTONES")
                    try:
                        bump_data_version("restore_roadmap_milestones")
                    except Exception:
                        pass
                    st.session_state["ado_data_cache_bust"] = int(st.session_state.get("ado_data_cache_bust", 0) or 0) + 1
                    st.session_state["_cache_epoch"] = int(st.session_state.get("_cache_epoch", 0) or 0) + 1
                    try:
                        _backup_restore_post_write_refresh("admin_backup_restore", bump_version=True)
                    except Exception:
                        pass
                # Post-restore recompute for offline environments (no ADO access).
                if ds in ("ADO_ITERATION_CALENDAR", "ADO_FEATURES"):
                    if st.session_state.get("_restore_ado_iteration_calendar_done") and st.session_state.get("_restore_ado_features_done"):
                        st.info("Recomputing iteration calendar mappings…")
                        with st.spinner("Updating feature↔PI calendar mapping views…"):
                            if callable(_recompute_feature_iteration_mapping):
                                _recompute_feature_iteration_mapping()
                            else:
                                # Fallback: ensure the full view pipeline exists.
                                ensure_all_views_ok()
                        try:
                            _backup_restore_post_write_refresh("admin_backup_restore", bump_version=True)
                        except Exception:
                            pass
                        st.success("Iteration mapping completed.")
                elif ds == "MAP_ADO_PROGRAM_TO_TCO_PROGRAM":
                    ensure_ado_minimal_tables()
                    rows = []
                    for _, r in df.iterrows():
                        ado = str(r.get("ADO_PROGRAM") or "").strip()
                        pid = str(r.get("PROGRAMID") or "").strip() or None
                        if ado:
                            rows.append((ado, pid))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM')} t
                            USING (SELECT %s ADO_PROGRAM, %s PROGRAMID) s
                            ON t.ADO_PROGRAM = s.ADO_PROGRAM
                            WHEN MATCHED THEN UPDATE SET PROGRAMID = s.PROGRAMID
                            WHEN NOT MATCHED THEN INSERT (ADO_PROGRAM, PROGRAMID) VALUES (s.ADO_PROGRAM, s.PROGRAMID);
                            """,
                            rows, many=True
                        )
                        n = len(rows)
                # Mappings
                elif ds == "MAP_ADO_TEAM_TO_TCO_TEAM":
                    ensure_ado_minimal_tables()
                    def _clean(val):
                        if val is None:
                            return None
                        try:
                            if pd.isna(val):
                                return None
                        except Exception:
                            pass
                        sval = str(val).strip()
                        return sval if sval else None

                    rows = []
                    for _, r in df.iterrows():
                        key = _clean(r.get("ADO_TEAM_KEY"))
                        if not key:
                            continue
                        rows.append((
                            key,
                            _clean(r.get("TEAMID")),
                            _clean(r.get("PROGRAM_RAW")),
                            _clean(r.get("AREA_LEVEL3_RAW")),
                            _clean(r.get("AREA_LEVEL4_RAW")),
                            _clean(r.get("ADO_TEAM")),
                        ))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('MAP_ADO_TEAM_TO_TCO_TEAM')} t
                            USING (SELECT %s ADO_TEAM_KEY, %s TEAMID, %s PROGRAM_RAW, %s AREA_LEVEL3_RAW, %s AREA_LEVEL4_RAW, %s ADO_TEAM) s
                            ON t.ADO_TEAM_KEY = s.ADO_TEAM_KEY
                            WHEN MATCHED THEN UPDATE SET TEAMID = s.TEAMID, PROGRAM_RAW = s.PROGRAM_RAW, AREA_LEVEL3_RAW = s.AREA_LEVEL3_RAW, AREA_LEVEL4_RAW = s.AREA_LEVEL4_RAW, ADO_TEAM = s.ADO_TEAM
                            WHEN NOT MATCHED THEN INSERT (ADO_TEAM_KEY, TEAMID, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, ADO_TEAM) VALUES (s.ADO_TEAM_KEY, s.TEAMID, s.PROGRAM_RAW, s.AREA_LEVEL3_RAW, s.AREA_LEVEL4_RAW, s.ADO_TEAM);
                            """,
                            rows, many=True
                        )
                        n = len(rows)
                elif ds == "MAP_ADO_APP_TO_TCO_GROUP":
                    ensure_ado_minimal_tables()
                    rows = []
                    for _, r in df.iterrows():
                        ado = str(r.get("ADO_APP") or "").strip()
                        gid = str(r.get("APP_GROUP") or "").strip() or None
                        if ado:
                            rows.append((ado, gid))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('MAP_ADO_APP_TO_TCO_GROUP')} t
                            USING (SELECT %s ADO_APP, %s APP_GROUP) s
                            ON t.ADO_APP = s.ADO_APP
                            WHEN MATCHED THEN UPDATE SET APP_GROUP = s.APP_GROUP
                            WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP);
                            """,
                            rows, many=True
                        )
                        n = len(rows)
                # Master data
                elif ds == "PROGRAMS":
                    for _, r in df.iterrows():
                        pid = str(r.get("PROGRAMID") or "").strip()
                        if not pid:
                            st.error("PROGRAMS restore requires PROGRAMID in CSV; aborting to preserve IDs.")
                            st.stop()
                        program_name = (
                            _opt_str(r.get("PROGRAMNAME"))
                            or _opt_str(r.get("PROGRAMNAME_RAW"))
                            or _opt_str(r.get("PROGRAM_DISPLAY_NAME"))
                            or _opt_str(r.get("DISPLAY_NAME"))
                            or pid
                        )
                        program_display_name = (
                            _opt_str(r.get("PROGRAM_DISPLAY_NAME"))
                            or _opt_str(r.get("DISPLAY_NAME"))
                        )
                        upsert_program(
                            pid,
                            program_name,
                            _opt_str(r.get("PROGRAMOWNER")),
                            (float(r.get("PROGRAMFTE")) if r.get("PROGRAMFTE") is not None and pd.notna(r.get("PROGRAMFTE")) else None),
                            (float(r.get("PROGRAM_XOM_RATE")) if r.get("PROGRAM_XOM_RATE") is not None and pd.notna(r.get("PROGRAM_XOM_RATE")) else None),
                            program_display_name=program_display_name,
                        )
                        n += 1
                elif ds == "PROGRAM_COMPOSITION_HISTORY":
                    ensure_program_composition_history()
                    rows = []
                    for _, r in df.iterrows():
                        pid = str(r.get("PROGRAMID") or "").strip()
                        if not pid:
                            continue
                        year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                        if year is None:
                            continue
                        rows.append((
                            pid,
                            year,
                            pi_val,
                            float(r.get("PROGRAMFTE") or 0.0) if pd.notna(r.get("PROGRAMFTE")) else 0.0,
                            (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None),
                        ))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('PROGRAM_COMPOSITION_HISTORY')} t
                            USING (SELECT %s PROGRAMID, %s YEAR, %s PI, %s PROGRAMFTE, %s UPDATED_BY) s
                            ON t.PROGRAMID = s.PROGRAMID AND t.YEAR = s.YEAR AND ISNULL(t.PI,0) = ISNULL(s.PI,0)
                            WHEN MATCHED THEN UPDATE SET PROGRAMFTE = s.PROGRAMFTE, UPDATED_AT = SYSDATETIME(), UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
                            WHEN NOT MATCHED THEN INSERT (PROGRAMID, YEAR, PI, PROGRAMFTE, UPDATED_AT, UPDATED_BY)
                            VALUES (s.PROGRAMID, s.YEAR, s.PI, s.PROGRAMFTE, SYSDATETIME(), s.UPDATED_BY);
                            """,
                            rows,
                            many=True,
                        )
                        n = len(rows)
                elif ds == "PROGRAM_ADDITIONAL_COSTS":
                    for _, r in df.iterrows():
                        pid = str(r.get("PROGRAMID") or "").strip()
                        if not pid:
                            continue
                        year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        month = int(r.get("MONTH")) if pd.notna(r.get("MONTH")) else None
                        ctype = str(r.get("COST_TYPE") or "").strip()
                        subtype = str(r.get("SUBTYPE") or "").strip()
                        if year is None or month is None or not ctype:
                            continue
                        amt = float(r.get("AMOUNT") or 0.0) if pd.notna(r.get("AMOUNT")) else 0.0
                        upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                        upsert_program_additional_cost(pid, year, month, ctype, subtype, amt, updated_by=upd)
                        n += 1
                elif ds == "PROGRAM_APPTIO_WORKIDS":
                    for _, r in df.iterrows():
                        pid = str(r.get("PROGRAMID") or "").strip()
                        work_id = str(r.get("WORK_ID") or "").strip()
                        if not pid or not work_id:
                            continue
                        upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                        upsert_program_apptio_workid(pid, work_id, updated_by=upd)
                        n += 1
                elif ds == "APPTIO_ACTUALS":
                    rows = []
                    for _, r in df.iterrows():
                        work_id = str(r.get("WORK_ID") or "").strip()
                        if not work_id:
                            continue
                        if pd.isna(r.get("FISCAL_YEAR")) or pd.isna(r.get("MONTH")):
                            continue
                        rows.append({
                            "work_id": work_id,
                            "fiscal_year": int(r.get("FISCAL_YEAR")),
                            "month": int(r.get("MONTH")),
                            "amount": float(r.get("AMOUNT") or 0.0) if pd.notna(r.get("AMOUNT")) else 0.0,
                            "source": (str(r.get("SOURCE")) if pd.notna(r.get("SOURCE")) else None),
                            "loaded_by": (str(r.get("LOADED_BY")) if pd.notna(r.get("LOADED_BY")) else None),
                        })
                    n = upsert_apptio_actuals(rows)
                elif ds == "TEAMS":
                    for _, r in df.iterrows():
                        tid = str(r.get("TEAMID") or "").strip()
                        if not tid:
                            st.error("TEAMS restore requires TEAMID in CSV; aborting to preserve IDs.")
                            st.stop()
                        team_name = (
                            _opt_str(r.get("TEAMNAME"))
                            or _opt_str(r.get("TEAMNAME_RAW"))
                            or _opt_str(r.get("TEAM_DISPLAY_NAME"))
                            or _opt_str(r.get("DISPLAY_NAME"))
                            or tid
                        )
                        team_display_name = (
                            _opt_str(r.get("TEAM_DISPLAY_NAME"))
                            or _opt_str(r.get("DISPLAY_NAME"))
                        )
                        upsert_team(
                            tid,
                            team_name,
                            _opt_str(r.get("PROGRAMID")),
                            _to_float(r.get("TEAMFTE")),
                            _to_float(r.get("DELIVERY_TEAM_FTE")),
                            _to_float(r.get("CONTRACTOR_C_FTE")),
                            _to_float(r.get("CONTRACTOR_CS_FTE")),
                            team_display_name=team_display_name,
                        )
                        try:
                            execute(
                                f"UPDATE { _fq('TEAMS') } SET PRODUCTOWNER=%s WHERE TEAMID=%s",
                                (_opt_str(r.get("PRODUCTOWNER")), tid),
                            )
                        except Exception:
                            pass
                        n += 1
                elif ds == "TEAM_COMPOSITION_HISTORY":
                    ensure_team_composition_history()
                    rows = []
                    for _, r in df.iterrows():
                        tid = str(r.get("TEAMID") or "").strip()
                        if not tid:
                            continue
                        year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                        if year is None:
                            continue
                        rows.append((
                            tid,
                            year,
                            pi_val,
                            float(r.get("TEAMFTE") or 0.0) if pd.notna(r.get("TEAMFTE")) else 0.0,
                            float(r.get("DELIVERY_TEAM_FTE") or 0.0) if pd.notna(r.get("DELIVERY_TEAM_FTE")) else 0.0,
                            float(r.get("CONTRACTOR_CS_FTE") or 0.0) if pd.notna(r.get("CONTRACTOR_CS_FTE")) else 0.0,
                            float(r.get("CONTRACTOR_C_FTE") or 0.0) if pd.notna(r.get("CONTRACTOR_C_FTE")) else 0.0,
                        ))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('TEAM_COMPOSITION_HISTORY')} t
                            USING (SELECT %s TEAMID, %s YEAR, %s PI, %s TEAMFTE, %s DELIVERY_TEAM_FTE, %s CONTRACTOR_CS_FTE, %s CONTRACTOR_C_FTE) s
                            ON t.TEAMID = s.TEAMID AND t.YEAR = s.YEAR AND ISNULL(t.PI,0) = ISNULL(s.PI,0)
                            WHEN MATCHED THEN UPDATE SET TEAMFTE = s.TEAMFTE, DELIVERY_TEAM_FTE = s.DELIVERY_TEAM_FTE,
                              CONTRACTOR_CS_FTE = s.CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE = s.CONTRACTOR_C_FTE,
                              UPDATED_AT = SYSDATETIME()
                            WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE, UPDATED_AT)
                            VALUES (s.TEAMID, s.YEAR, s.PI, s.TEAMFTE, s.DELIVERY_TEAM_FTE, s.CONTRACTOR_CS_FTE, s.CONTRACTOR_C_FTE, SYSDATETIME());
                            """,
                            rows,
                            many=True,
                        )
                        n = len(rows)
                elif ds == "VENDORS":
                    for _, r in df.iterrows():
                        upsert_vendor(str(r.get("VENDORID") or uuid.uuid4()), str(r.get("VENDORNAME") or "")); n += 1
                elif ds == "APPLICATION_GROUPS":
                    group_name_map = _load_group_name_map()
                    remapped = 0
                    for _, r in df.iterrows():
                        group_name = str(r.get("GROUPNAME") or "").strip()
                        if not group_name:
                            continue
                        gid_in = str(r.get("GROUPID") or "").strip() or None
                        gid = _resolve_group_id(group_name, gid_in, group_name_map)
                        if gid_in and gid != gid_in:
                            remapped += 1
                        team_id = (str(r.get("TEAMID")) if r.get("TEAMID") is not None else None)
                        default_vendor = (str(r.get("DEFAULT_VENDORID")) if r.get("DEFAULT_VENDORID") is not None else None)
                        owner = (str(r.get("OWNER")) if r.get("OWNER") is not None else None)
                        is_base = bool(r.get("IS_BASE")) if "IS_BASE" in r else False
                        upsert_application_group(
                            group_id=gid,
                            group_name=group_name,
                            team_id=team_id,
                            default_vendor_id=default_vendor,
                            owner=owner,
                            is_base=is_base,
                        )
                        try:
                            prog_id = (str(r.get("PROGRAMID")) if r.get("PROGRAMID") is not None else None)
                            if prog_id:
                                execute(f"UPDATE { _fq('APPLICATION_GROUPS') } SET PROGRAMID=%s WHERE GROUPID=%s", (prog_id, gid))
                        except Exception:
                            pass
                        n += 1
                    if remapped:
                        st.info(f"{remapped} application group name(s) already exist; reused existing GROUPID to avoid duplicates.")
                elif ds == "APP_GROUP_TEAM_LINKS":
                    rows = []
                    for _, r in df.iterrows():
                        gid = str(r.get("GROUPID") or "").strip()
                        tid = str(r.get("TEAMID") or "").strip()
                        if gid and tid:
                            rows.append((gid, tid))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('APP_GROUP_TEAM_LINKS')} t
                            USING (SELECT %s GROUPID, %s TEAMID) s
                            ON t.GROUPID = s.GROUPID AND t.TEAMID = s.TEAMID
                            WHEN NOT MATCHED THEN INSERT (GROUPID, TEAMID) VALUES (s.GROUPID, s.TEAMID);
                            """,
                            rows,
                            many=True,
                        )
                        n = len(rows)
                elif ds == "APPLICATIONS":
                    _app_name_cache: Dict[str, str] = {}
                    def _resolve_app_id_by_name(name: str) -> Optional[str]:
                        key = str(name or "").strip()
                        if not key:
                            return None
                        if key in _app_name_cache:
                            return _app_name_cache[key]
                        df_lookup = fetch_df(f"SELECT APPLICATIONID FROM {_fq('APPLICATIONS')} WHERE UPPER(APPLICATIONNAME)=UPPER(%s)", (key,))
                        if df_lookup is not None and not df_lookup.empty:
                            val = str(df_lookup.iloc[0]["APPLICATIONID"])
                            _app_name_cache[key] = val
                            return val
                        return None

                    for _, r in df.iterrows():
                        name = str(r.get("APPLICATIONNAME") or "")
                        aid = str(r.get("APPLICATIONID") or "").strip() or (_resolve_app_id_by_name(name) or str(uuid.uuid4()))
                        group_id = (str(r.get("GROUPID")) if r.get("GROUPID") is not None else None)
                        add_info = (str(r.get("ADD_INFO")) if r.get("ADD_INFO") is not None else None)
                        vendor_id = (str(r.get("VENDORID")) if r.get("VENDORID") is not None else None)
                        try:
                            upsert_application_instance(
                                aid,
                                group_id,
                                name,
                                add_info,
                                vendor_id,
                            )
                        except Exception:
                            alt = _resolve_app_id_by_name(name)
                            if alt and alt != aid:
                                upsert_application_instance(
                                    alt,
                                    group_id,
                                    name,
                                    add_info,
                                    vendor_id,
                                )
                            else:
                                raise
                        n += 1
                # Rates
                elif ds == "TEAM_RATE_HISTORY":
                    for _, r in df.iterrows():
                        tid = str(r.get("TEAMID") or "").strip()
                        if not tid:
                            continue
                        year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        if year is None:
                            continue
                        pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                        loc = str(r.get("LOCATION") or "").strip()
                        rate = float(r.get("XOM_RATE") or 0.0) if pd.notna(r.get("XOM_RATE")) else None
                        upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                        upsert_team_rate_history(tid, year, pi_val, loc or "GBC", rate, updated_by=upd)
                        n += 1
                elif ds == "PROGRAM_RATE_HISTORY":
                    for _, r in df.iterrows():
                        pid = str(r.get("PROGRAMID") or "").strip()
                        if not pid:
                            continue
                        year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        if year is None:
                            continue
                        pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                        loc = str(r.get("LOCATION") or "").strip()
                        rate = float(r.get("PROGRAM_XOM_RATE") or 0.0) if pd.notna(r.get("PROGRAM_XOM_RATE")) else None
                        upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                        upsert_program_rate_history(pid, year, pi_val, loc or "GBC", rate, updated_by=upd)
                        n += 1
                elif ds == "TEAM_HEADCOUNT_HISTORY":
                    for _, r in df.iterrows():
                        tid = str(r.get("TEAMID") or "").strip()
                        if not tid:
                            continue
                        year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        if year is None:
                            continue
                        pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                        cls = str(r.get("CLASS") or "").strip()
                        loc = str(r.get("LOCATION") or "").strip()
                        headcount = float(r.get("HEADCOUNT") or 0.0) if pd.notna(r.get("HEADCOUNT")) else 0.0
                        upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                        if cls and loc:
                            upsert_team_headcount(tid, year, pi_val, cls, loc, headcount, updated_by=upd)
                            n += 1
                elif ds == "CONTRACTOR_COMPANY":
                    for _, r in df.iterrows():
                        cid = str(r.get("COMPANYID") or uuid.uuid4())
                        name = str(r.get("NAME") or "").strip()
                        if not name:
                            continue
                        active = _to_bool(r.get("ACTIVE"))
                        if active is None:
                            active = True
                        notes = (str(r.get("NOTES")) if pd.notna(r.get("NOTES")) else None)
                        upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                        upsert_contractor_company(cid, name, active=active, notes=notes, updated_by=upd)
                        n += 1
                elif ds == "CONTRACTOR_RATE_HISTORY":
                    for _, r in df.iterrows():
                        cid = str(r.get("COMPANYID") or "").strip()
                        if not cid:
                            continue
                        year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        if year is None:
                            continue
                        pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                        cls = str(r.get("CLASS") or "").strip()
                        rate = float(r.get("RATE") or 0.0) if pd.notna(r.get("RATE")) else None
                        upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                        if cls:
                            upsert_contractor_rate(cid, year, pi_val, cls, rate, updated_by=upd)
                            n += 1
                elif ds == "TEAM_CONTRACTOR_HEADCOUNT":
                    for _, r in df.iterrows():
                        tid = str(r.get("TEAMID") or "").strip()
                        cid = str(r.get("COMPANYID") or "").strip()
                        if not tid or not cid:
                            continue
                        year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        if year is None:
                            continue
                        pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                        cls = str(r.get("CLASS") or "").strip()
                        headcount = float(r.get("HEADCOUNT") or 0.0) if pd.notna(r.get("HEADCOUNT")) else 0.0
                        upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                        if cls:
                            upsert_team_contractor_headcount(tid, year, pi_val, cls, cid, headcount, updated_by=upd)
                            n += 1
                elif ds == "TEAM_MSP_RATE":
                    rows = []
                    for _, r in df.iterrows():
                        enabled = _to_bool(r.get("MSP_ENABLED"))
                        rows.append((
                            str(r.get("TEAMID")),
                            1 if enabled else 0,
                            (str(r.get("MSP_SIZE")) if pd.notna(r.get("MSP_SIZE")) else None),
                            _to_float(r.get("MSP_RATE_PER_PI")),
                            _to_float(r.get("MSP_RATE_SMALL")),
                            _to_float(r.get("MSP_RATE_MEDIUM")),
                            _to_float(r.get("MSP_RATE_LARGE")),
                        ))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('TEAM_MSP_RATE')} t
                            USING (SELECT %s TEAMID, %s MSP_ENABLED, %s MSP_SIZE, %s MSP_RATE_PER_PI, %s MSP_RATE_SMALL, %s MSP_RATE_MEDIUM, %s MSP_RATE_LARGE) s
                            ON t.TEAMID = s.TEAMID
                            WHEN MATCHED THEN UPDATE SET
                              MSP_ENABLED = s.MSP_ENABLED,
                              MSP_SIZE = s.MSP_SIZE,
                              MSP_RATE_PER_PI = s.MSP_RATE_PER_PI,
                              MSP_RATE_SMALL = s.MSP_RATE_SMALL,
                              MSP_RATE_MEDIUM = s.MSP_RATE_MEDIUM,
                              MSP_RATE_LARGE = s.MSP_RATE_LARGE,
                              UPDATED_AT = SYSDATETIME()
                            WHEN NOT MATCHED THEN INSERT (TEAMID, MSP_ENABLED, MSP_SIZE, MSP_RATE_PER_PI, MSP_RATE_SMALL, MSP_RATE_MEDIUM, MSP_RATE_LARGE)
                            VALUES (s.TEAMID, s.MSP_ENABLED, s.MSP_SIZE, s.MSP_RATE_PER_PI, s.MSP_RATE_SMALL, s.MSP_RATE_MEDIUM, s.MSP_RATE_LARGE);
                            """,
                            rows, many=True
                        ); n = len(rows)
                elif ds == "TEAM_MSP_ASSIGNMENTS":
                    ensure_team_msp_assignments_table()
                    rows = []
                    for _, r in df.iterrows():
                        tid = str(r.get("TEAMID") or "").strip()
                        if not tid:
                            continue
                        gid = (str(r.get("GROUPID")) if pd.notna(r.get("GROUPID")) else None)
                        eff_year = int(r.get("EFFECTIVE_YEAR")) if pd.notna(r.get("EFFECTIVE_YEAR")) else 0
                        year_val = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        it_start = int(r.get("ITERATION_START")) if pd.notna(r.get("ITERATION_START")) else None
                        it_end = int(r.get("ITERATION_END")) if pd.notna(r.get("ITERATION_END")) else None
                        size = (str(r.get("MSP_SIZE")) if pd.notna(r.get("MSP_SIZE")) else None)
                        weight = float(r.get("WEIGHT_PCT")) if pd.notna(r.get("WEIGHT_PCT")) else None
                        rows.append((tid, gid, eff_year, year_val, it_start, it_end, size, weight))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('TEAM_MSP_ASSIGNMENTS')} t
                            USING (SELECT %s TEAMID, %s GROUPID, %s EFFECTIVE_YEAR, %s YEAR, %s ITERATION_START, %s ITERATION_END, %s MSP_SIZE, %s WEIGHT_PCT) s
                            ON t.TEAMID = s.TEAMID AND ISNULL(t.GROUPID,'') = ISNULL(s.GROUPID,'') AND t.EFFECTIVE_YEAR = s.EFFECTIVE_YEAR
                            WHEN MATCHED THEN UPDATE SET YEAR=s.YEAR, ITERATION_START=s.ITERATION_START, ITERATION_END=s.ITERATION_END,
                              MSP_SIZE=s.MSP_SIZE, WEIGHT_PCT=s.WEIGHT_PCT, UPDATED_AT=SYSDATETIME()
                            WHEN NOT MATCHED THEN INSERT (TEAMID, GROUPID, EFFECTIVE_YEAR, [YEAR], ITERATION_START, ITERATION_END, MSP_SIZE, WEIGHT_PCT, UPDATED_AT)
                            VALUES (s.TEAMID, s.GROUPID, s.EFFECTIVE_YEAR, s.YEAR, s.ITERATION_START, s.ITERATION_END, s.MSP_SIZE, s.WEIGHT_PCT, SYSDATETIME());
                            """,
                            rows,
                            many=True,
                        )
                        n = len(rows)

                elif ds == "TCO_COMMITMENTS":
                    rows = []
                    for _, r in df.iterrows():
                        year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                        if year is None:
                            continue
                        pi_val = (str(r.get("PI")) if pd.notna(r.get("PI")) else "")
                        prog = (str(r.get("PROGRAMNAME")) if pd.notna(r.get("PROGRAMNAME")) else "")
                        team = (str(r.get("TEAMNAME")) if pd.notna(r.get("TEAMNAME")) else "")
                        group = (str(r.get("GROUPNAME")) if pd.notna(r.get("GROUPNAME")) else "")
                        status = (str(r.get("STATUS")) if pd.notna(r.get("STATUS")) else "Not started")
                        owner_email = (str(r.get("OWNER_EMAIL")) if pd.notna(r.get("OWNER_EMAIL")) else None)
                        owner_name = (str(r.get("OWNER_NAME")) if pd.notna(r.get("OWNER_NAME")) else None)
                        target_date = (str(r.get("TARGET_DATE")) if pd.notna(r.get("TARGET_DATE")) else None)
                        notes = (str(r.get("NOTES")) if pd.notna(r.get("NOTES")) else None)
                        updated_by = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                        rows.append((year, pi_val, prog, team, group, status, owner_email, owner_name, target_date, notes, updated_by))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('TCO_COMMITMENTS')} t
                            USING (SELECT %s YEAR, %s PI, %s PROGRAMNAME, %s TEAMNAME, %s GROUPNAME, %s STATUS, %s OWNER_EMAIL, %s OWNER_NAME, %s TARGET_DATE, %s NOTES, %s UPDATED_BY) s
                            ON t.YEAR = s.YEAR AND ISNULL(t.PI,'') = ISNULL(s.PI,'') AND ISNULL(t.PROGRAMNAME,'') = ISNULL(s.PROGRAMNAME,'')
                               AND ISNULL(t.TEAMNAME,'') = ISNULL(s.TEAMNAME,'') AND ISNULL(t.GROUPNAME,'') = ISNULL(s.GROUPNAME,'')
                            WHEN MATCHED THEN UPDATE SET STATUS=s.STATUS, OWNER_EMAIL=s.OWNER_EMAIL, OWNER_NAME=s.OWNER_NAME,
                              TARGET_DATE=s.TARGET_DATE, NOTES=s.NOTES, UPDATED_AT=SYSUTCDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY)
                            WHEN NOT MATCHED THEN INSERT (YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, STATUS, OWNER_EMAIL, OWNER_NAME, TARGET_DATE, NOTES, UPDATED_AT, UPDATED_BY)
                            VALUES (s.YEAR, s.PI, s.PROGRAMNAME, s.TEAMNAME, s.GROUPNAME, s.STATUS, s.OWNER_EMAIL, s.OWNER_NAME, s.TARGET_DATE, s.NOTES, SYSUTCDATETIME(), s.UPDATED_BY);
                            """,
                            rows,
                            many=True,
                        )
                        n = len(rows)

                elif ds == "CONTRACTS":
                    # Expect: CONTRACT_ID (optional), APPLICATIONID, TEAMID, START_FY, END_FY, RENEWAL_MONTH, ANNUAL_AMOUNT, ESCALATION_PCT, STATUS, AGREEMENT_NUMBER, COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE, TOTAL_CONTRACT_COST
                    try:
                        from db import upsert_contract, ensure_contracts_table
                    except Exception as _e:
                        st.error("Contract helpers unavailable in this build."); raise
                    ensure_contracts_table()
                    for _, r in df.iterrows():
                        upsert_contract(
                            contract_id=str(r.get("CONTRACT_ID") or uuid.uuid4()),
                            application_id=str(r.get("APPLICATIONID") or ""),
                            team_id=str(r.get("TEAMID") or ""),
                            start_fy=int(r.get("START_FY")) if pd.notna(r.get("START_FY")) else 0,
                            end_fy=int(r.get("END_FY")) if pd.notna(r.get("END_FY")) else 0,
                            renewal_month=int(r.get("RENEWAL_MONTH")) if pd.notna(r.get("RENEWAL_MONTH")) else 1,
                            annual_amount=float(r.get("ANNUAL_AMOUNT")) if pd.notna(r.get("ANNUAL_AMOUNT")) else 0.0,
                            escalation_pct=float(r.get("ESCALATION_PCT")) if pd.notna(r.get("ESCALATION_PCT")) else 0.0,
                            status=str(r.get("STATUS")) if pd.notna(r.get("STATUS")) else 'Active',
                            agreement_number=(str(r.get("AGREEMENT_NUMBER")) if pd.notna(r.get("AGREEMENT_NUMBER")) else None),
                            company_code=(str(r.get("COMPANY_CODE")) if pd.notna(r.get("COMPANY_CODE")) else None),
                            cost_center=(str(r.get("COST_CENTER")) if pd.notna(r.get("COST_CENTER")) else None),
                            service_type=(str(r.get("SERVICE_TYPE")) if pd.notna(r.get("SERVICE_TYPE")) else None),
                            contract_renewal_date=(str(r.get("CONTRACT_RENEWAL_DATE")) if pd.notna(r.get("CONTRACT_RENEWAL_DATE")) else None),
                            invoice_renewal_date=(str(r.get("INVOICE_RENEWAL_DATE")) if pd.notna(r.get("INVOICE_RENEWAL_DATE")) else None),
                            total_contract_cost=(float(r.get("TOTAL_CONTRACT_COST")) if pd.notna(r.get("TOTAL_CONTRACT_COST")) else None),
                        )
                        n += 1

                elif ds == "INVOICES":
                    # Expect: INVOICEID (optional), APPLICATIONID, TEAMID, GROUPID, FISCAL_YEAR, RENEWALDATE, AMOUNT, STATUS, PRODUCT_OWNER, AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE, COMPANY_CODE, COST_CENTER, SERIAL_NUMBER, WORK_ORDER, AGREEMENT_NUMBER, CONTRACT_DUE, SERVICE_TYPE, NOTES, PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING, GROUPID_AT_BOOKING, ROLLOVER_BATCH_ID, ROLLED_OVER_FROM_YEAR, INVOICE_TYPE
                    for _, r in df.iterrows():
                        inv_id = str(r.get("INVOICEID") or uuid.uuid4())
                        def _opt_float(v):
                            try:
                                return float(v) if pd.notna(v) else None
                            except Exception:
                                return None
                        def _opt_int(v):
                            try:
                                return int(float(v)) if pd.notna(v) else None
                            except Exception:
                                return None
                        upsert_invoice(
                            invoice_id=inv_id,
                            application_id=str(r.get("APPLICATIONID") or ""),
                            team_id=str(r.get("TEAMID") or ""),
                            renewal_date=_opt_str(r.get("RENEWALDATE")),
                            amount=_opt_float(r.get("AMOUNT")) or 0.0,
                            status=_opt_str(r.get("STATUS")),
                            fiscal_year=int(r.get("FISCAL_YEAR")) if pd.notna(r.get("FISCAL_YEAR")) else None,
                            product_owner=_opt_str(r.get("PRODUCT_OWNER")),
                            amount_next_year=_opt_float(r.get("AMOUNT_NEXT_YEAR")),
                            contract_active=bool(r.get("CONTRACT_ACTIVE")) if pd.notna(r.get("CONTRACT_ACTIVE")) else None,
                            company_code=_opt_str(r.get("COMPANY_CODE")),
                            cost_center=_opt_str(r.get("COST_CENTER")),
                            serial_number=_opt_str(r.get("SERIAL_NUMBER")),
                            work_order=_opt_str(r.get("WORK_ORDER")),
                            agreement_number=_opt_str(r.get("AGREEMENT_NUMBER")),
                            contract_due=_opt_int(r.get("CONTRACT_DUE")),
                            service_type=_opt_str(r.get("SERVICE_TYPE")),
                            notes=_opt_str(r.get("NOTES")),
                            group_id=_opt_str(r.get("GROUPID")),
                            programid_at_booking=_opt_str(r.get("PROGRAMID_AT_BOOKING")),
                            vendorid_at_booking=_opt_str(r.get("VENDORID_AT_BOOKING")),
                            groupid_at_booking=_opt_str(r.get("GROUPID_AT_BOOKING")),
                            rollover_batch_id=_opt_str(r.get("ROLLOVER_BATCH_ID")),
                            rolled_over_from_year=_opt_int(r.get("ROLLED_OVER_FROM_YEAR")),
                            invoice_type=_opt_str(r.get("INVOICE_TYPE")) or 'Recurring Invoice',
                        )
                        n += 1
                elif ds == "INVOICE_NOTES":
                    # NOTE_ID (optional), INVOICEID, NOTE_TEXT, CREATED_BY
                    for _, r in df.iterrows():
                        note_id = str(r.get("NOTE_ID") or uuid.uuid4())
                        inv = str(r.get("INVOICEID") or "")
                        txt = str(r.get("NOTE_TEXT") or "")
                        cby = (str(r.get("CREATED_BY")) if pd.notna(r.get("CREATED_BY")) else None)
                        execute(
                            f"""
                            MERGE INTO {_fq('INVOICE_NOTES')} t
                            USING (SELECT %s NOTE_ID, %s INVOICEID, %s NOTE_TEXT, %s CREATED_BY) s
                            ON t.NOTE_ID = s.NOTE_ID
                            WHEN MATCHED THEN UPDATE SET NOTE_TEXT = s.NOTE_TEXT, CREATED_BY = s.CREATED_BY
                            WHEN NOT MATCHED THEN INSERT (NOTE_ID, INVOICEID, NOTE_TEXT, CREATED_BY) VALUES (s.NOTE_ID, s.INVOICEID, s.NOTE_TEXT, s.CREATED_BY);
                            """,
                            (note_id, inv, txt, cby)
                        ); n += 1
                elif ds == "ROLLOVER_LOG":
                    rows = []
                    for _, r in df.iterrows():
                        batch_id = str(r.get("BATCH_ID") or "").strip()
                        if not batch_id:
                            continue
                        rows.append((
                            batch_id,
                            int(r.get("FROM_YEAR")) if pd.notna(r.get("FROM_YEAR")) else None,
                            int(r.get("TO_YEAR")) if pd.notna(r.get("TO_YEAR")) else None,
                            int(r.get("ROWS_INSERTED")) if pd.notna(r.get("ROWS_INSERTED")) else None,
                            (str(r.get("CREATED_BY")) if pd.notna(r.get("CREATED_BY")) else None),
                        ))
                    if rows:
                        execute(
                            f"""
                            MERGE INTO {_fq('ROLLOVER_LOG')} t
                            USING (SELECT %s BATCH_ID, %s FROM_YEAR, %s TO_YEAR, %s ROWS_INSERTED, %s CREATED_BY) s
                            ON t.BATCH_ID = s.BATCH_ID
                            WHEN MATCHED THEN UPDATE SET FROM_YEAR=s.FROM_YEAR, TO_YEAR=s.TO_YEAR, ROWS_INSERTED=s.ROWS_INSERTED, CREATED_BY=s.CREATED_BY
                            WHEN NOT MATCHED THEN INSERT (BATCH_ID, FROM_YEAR, TO_YEAR, ROWS_INSERTED, CREATED_BY)
                            VALUES (s.BATCH_ID, s.FROM_YEAR, s.TO_YEAR, s.ROWS_INSERTED, s.CREATED_BY);
                            """,
                            rows,
                            many=True,
                        )
                        n = len(rows)
                elif ds == "INVOICE_ATTACHMENTS (zip)":
                    import zipfile as _zip, io as _io
                    zbytes = file.read() if file is not None else None
                    if not zbytes:
                        st.error("No zip provided."); st.stop()
                    with _zip.ZipFile(_io.BytesIO(zbytes), 'r') as zf:
                        # Manifest
                        try:
                            with zf.open('attachments_manifest.csv') as fh:
                                man = _pd.read_csv(fh)
                        except Exception as e:
                            st.error(f"Manifest not found in zip: {e}"); st.stop()
                        n = 0
                        for _, r in man.iterrows():
                            aid = str(r.get('ATTACHMENT_ID'))
                            inv = str(r.get('INVOICEID') or "")
                            fn = str(r.get('FILENAME') or aid)
                            mt = str(r.get('MIMETYPE') or 'application/octet-stream')
                            # File path pattern used on export
                            # Attempt both with and without filename suffix for robustness
                            data = None
                            for name in (f"files/{aid}_{fn}", f"files/{aid}"):
                                try:
                                    data = zf.read(name)
                                    break
                                except Exception:
                                    continue
                            if data is None:
                                continue
                            # Upsert by ATTACHMENT_ID
                            execute(
                                f"""
                                MERGE INTO {_fq('INVOICE_ATTACHMENTS')} t
                                USING (SELECT %s ATTACHMENT_ID, %s INVOICEID, %s FILENAME, %s MIMETYPE, %s CONTENT) s
                                ON t.ATTACHMENT_ID = s.ATTACHMENT_ID
                                WHEN MATCHED THEN UPDATE SET INVOICEID=s.INVOICEID, FILENAME=s.FILENAME, MIMETYPE=s.MIMETYPE, CONTENT=s.CONTENT, UPLOADED_AT=SYSDATETIME()
                                WHEN NOT MATCHED THEN INSERT (ATTACHMENT_ID, INVOICEID, FILENAME, MIMETYPE, CONTENT) VALUES (s.ATTACHMENT_ID, s.INVOICEID, s.FILENAME, s.MIMETYPE, s.CONTENT);
                                """,
                                (aid, inv, fn, mt, data)
                            ); n += 1

                elif ds == "FULL_BACKUP (zip)":
                    import zipfile as _zip, io as _io
                    zbytes = file.getvalue() if file is not None else None
                    if not zbytes:
                        st.error("No zip provided."); st.stop()
                    stats = []
                    datasets_todo = [
                        ("app_users.csv", "APP_USERS"),
                        ("app_user_membership.csv", "APP_USER_MEMBERSHIP"),
                        ("ado_portfolio_settings.csv", "ADO_PORTFOLIO_SETTINGS"),
                        ("email_alert_config.csv", "EMAIL_ALERT_CONFIG"),
                        # Dimensions first (preserve IDs)
                        ("programs.csv", "PROGRAMS"),
                        ("teams.csv", "TEAMS"),
                        ("vendors.csv", "VENDORS"),
                        ("application_groups.csv", "APPLICATION_GROUPS"),
                        ("applications.csv", "APPLICATIONS"),
                        ("app_group_team_links.csv", "APP_GROUP_TEAM_LINKS"),
                        # History / rates after dimensions
                        ("team_headcount_history.csv", "TEAM_HEADCOUNT_HISTORY"),
                        ("team_rate_history.csv", "TEAM_RATE_HISTORY"),
                        ("team_composition_history.csv", "TEAM_COMPOSITION_HISTORY"),
                        ("team_contractor_headcount.csv", "TEAM_CONTRACTOR_HEADCOUNT"),
                        ("program_rate_history.csv", "PROGRAM_RATE_HISTORY"),
                        ("program_composition_history.csv", "PROGRAM_COMPOSITION_HISTORY"),
                        ("contractor_company.csv", "CONTRACTOR_COMPANY"),
                        ("contractor_rate_history.csv", "CONTRACTOR_RATE_HISTORY"),
                        ("team_msp_rate.csv", "TEAM_MSP_RATE"),
                        ("team_msp_assignments.csv", "TEAM_MSP_ASSIGNMENTS"),
                        # Other
                        ("program_additional_costs.csv", "PROGRAM_ADDITIONAL_COSTS"),
                        ("program_apptio_workids.csv", "PROGRAM_APPTIO_WORKIDS"),
                        ("apptio_actuals.csv", "APPTIO_ACTUALS"),
                        ("ado_iteration_calendar.csv", "ADO_ITERATION_CALENDAR"),
                        ("ado_features.csv", "ADO_FEATURES"),
                        ("ado_profiles.csv", "ADO_PROFILES"),
                        ("ado_workitem_lookup.csv", "ADO_WORKITEM_LOOKUP"),
                        ("ado_workitem_dates.csv", "ADO_WORKITEM_DATES"),
                        ("ado_workitem_links.csv", "ADO_WORKITEM_LINKS"),
                        ("ado_feature_progress.csv", "ADO_FEATURE_PROGRESS"),
                        ("ado_epic_progress.csv", "ADO_EPIC_PROGRESS"),
                        ("roadmap_milestones.csv", "ROADMAP_MILESTONES"),
                        ("map_ado_program.csv", "MAP_ADO_PROGRAM_TO_TCO_PROGRAM"),
                        ("map_ado_team.csv", "MAP_ADO_TEAM_TO_TCO_TEAM"),
                        ("map_ado_app.csv", "MAP_ADO_APP_TO_TCO_GROUP"),
                        ("tco_commitments.csv", "TCO_COMMITMENTS"),
                        ("contracts.csv", "CONTRACTS"),
                        ("invoices.csv", "INVOICES"),
                        ("invoice_notes.csv", "INVOICE_NOTES"),
                        ("rollover_log.csv", "ROLLOVER_LOG"),
                    ]
                    prog = st.progress(0.0, text="Starting full restore…")
                    steps_total = len(datasets_todo) + 1  # + attachments
                    steps_done = 0
                    from typing import Optional
                    group_name_map = _load_group_name_map()
                    group_id_map: Dict[str, str] = {}

                    def _find_in_zip(zf: _zip.ZipFile, target: str) -> Optional[str]:
                        """Find a member by basename (case-insensitive), ignoring folders."""
                        tgt = target.replace("\\", "/").split("/")[-1].lower()
                        for name in zf.namelist():
                            base = name.replace("\\", "/").split("/")[-1].lower()
                            if base == tgt:
                                return name
                        return None

                    with _zip.ZipFile(_io.BytesIO(zbytes), 'r') as zf:
                        def _rd_csv(name):
                            try:
                                member = _find_in_zip(zf, name) or name
                                with zf.open(member) as fh:
                                    try:
                                        df_read = _pd.read_csv(fh)
                                        if len(df_read.columns) == 1 and "\t" in str(df_read.columns[0]):
                                            fh.seek(0)
                                            df_read = _pd.read_csv(fh, sep="\t")
                                        return df_read
                                    except UnicodeDecodeError:
                                        fh.seek(0)
                                        df_read = _pd.read_csv(fh, encoding="latin1")
                                        if len(df_read.columns) == 1 and "\t" in str(df_read.columns[0]):
                                            fh.seek(0)
                                            df_read = _pd.read_csv(fh, encoding="latin1", sep="\t")
                                        return df_read
                            except Exception as e:
                                st.warning(f"{name}: could not read ({e})")
                                return None
                        # Restore in dependency order
                        for name, dsname in datasets_todo:
                            dfzip = _rd_csv(name)
                            if dfzip is None or dfzip.empty:
                                steps_done += 1
                                prog.progress(min(steps_done/steps_total, 1.0), text=f"{dsname}: no rows")
                                continue
                            dfzip = dfzip.where(pd.notna(dfzip), None)
                            _maybe_replace_dataset(dsname)
                            rows_before = n
                            def _zip_clean(val):
                                if val is None:
                                    return None
                                try:
                                    if pd.isna(val):
                                        return None
                                except Exception:
                                    pass
                                sval = str(val).strip()
                                return sval if sval else None
                            # Reuse same branch logic by setting ds and df locally
                            # (minimal duplication; calls the same upsert/merge code above)
                            # Users
                            if dsname == "APP_USERS":
                                for _, r in dfzip.iterrows():
                                    em = str(r.get("EMAIL") or "").strip()
                                    if not em:
                                        continue
                                    upsert_app_user(
                                        email=em,
                                        display_name=str(r.get("DISPLAY_NAME") or None) or None,
                                        role=str(r.get("ROLE") or "VIEWER").upper(),
                                        is_active=bool(r.get("IS_ACTIVE", True)),
                                        provider=str(r.get("PROVIDER") or None) or None,
                                    )
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "APP_USER_MEMBERSHIP":
                                ensure_user_membership_tables()
                                rows = []
                                for _, r in dfzip.iterrows():
                                    email = str(r.get("USER_EMAIL") or "").strip()
                                    if not email:
                                        continue
                                    pid = str(r.get("PROGRAMID") or "").strip()
                                    tid = str(r.get("TEAMID") or "").strip()
                                    rows.append((email, pid, tid))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('APP_USER_MEMBERSHIP')} t USING (SELECT %s USER_EMAIL, %s PROGRAMID, %s TEAMID) s ON t.USER_EMAIL=s.USER_EMAIL AND t.PROGRAMID=s.PROGRAMID AND t.TEAMID=s.TEAMID WHEN NOT MATCHED THEN INSERT (USER_EMAIL, PROGRAMID, TEAMID) VALUES (s.USER_EMAIL, s.PROGRAMID, s.TEAMID);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ADO_PORTFOLIO_SETTINGS":
                                ensure_ado_portfolio_settings_table()
                                rows = []
                                for _, r in dfzip.iterrows():
                                    name = str(r.get("PORTFOLIO_NAME") or "").strip()
                                    if not name:
                                        continue
                                    settings = str(r.get("SETTINGS_JSON") or "")
                                    updated_by = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    rows.append((name, settings, updated_by))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('ADO_PORTFOLIO_SETTINGS')} t USING (SELECT %s PORTFOLIO_NAME, %s SETTINGS_JSON, %s UPDATED_BY) s ON t.PORTFOLIO_NAME=s.PORTFOLIO_NAME WHEN MATCHED THEN UPDATE SET SETTINGS_JSON=s.SETTINGS_JSON, UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY) WHEN NOT MATCHED THEN INSERT (PORTFOLIO_NAME, SETTINGS_JSON, UPDATED_AT, UPDATED_BY) VALUES (s.PORTFOLIO_NAME, s.SETTINGS_JSON, SYSDATETIME(), s.UPDATED_BY);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "EMAIL_ALERT_CONFIG":
                                ensure_email_alert_config_table()
                                rows = []
                                for _, r in dfzip.iterrows():
                                    key = str(r.get("CONFIG_KEY") or "").strip()
                                    if not key:
                                        continue
                                    cfg = str(r.get("CONFIG_JSON") or "")
                                    updated_by = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    rows.append((key, cfg, updated_by))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('EMAIL_ALERT_CONFIG')} t USING (SELECT %s CONFIG_KEY, %s CONFIG_JSON, %s UPDATED_BY) s ON t.CONFIG_KEY=s.CONFIG_KEY WHEN MATCHED THEN UPDATE SET CONFIG_JSON=s.CONFIG_JSON, UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY) WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_JSON, UPDATED_AT, UPDATED_BY) VALUES (s.CONFIG_KEY, s.CONFIG_JSON, SYSDATETIME(), s.UPDATED_BY);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ADO_ITERATION_CALENDAR":
                                ensure_ado_iteration_calendar_table()
                                for col in ["ITERATION_PATH", "ITERATION_LEVEL3", "START_DATE", "END_DATE"]:
                                    _ensure_column(dfzip, col, [])
                                missing = [
                                    c
                                    for c in ["ITERATION_PATH", "ITERATION_LEVEL3", "START_DATE", "END_DATE"]
                                    if c not in dfzip.columns
                                ]
                                if missing:
                                    st.warning(f"ADO_ITERATION_CALENDAR missing required columns: {missing}")
                                else:
                                    if "ITERATION_SK" not in dfzip.columns:
                                        st.warning("IterationSK missing in backup; path-based join will be used.")
                                    for col in ["ITERATION_SK", "YEAR", "MONTH_KEY", "QUARTER_KEY", "ITERATION_GRAIN"]:
                                        if col not in dfzip.columns:
                                            dfzip[col] = None
                                    for col in ["ITERATION_PATH", "ITERATION_LEVEL3", "ITERATION_GRAIN"]:
                                        if col in dfzip.columns:
                                            dfzip[col] = dfzip[col].astype(str).str.strip().replace({"": None, "nan": None, "None": None})
                                    dfzip["ITERATION_SK"] = (
                                        dfzip.get("ITERATION_SK")
                                        .astype(str)
                                        .str.strip()
                                        .replace({"": None, "nan": None, "None": None, "<NA>": None})
                                    )
                                    if dfzip.get("ITERATION_SK") is None or dfzip.get("ITERATION_SK").isna().all():
                                        st.warning("IterationSK missing in backup; path-based join will be used.")
                                    for dcol in ["START_DATE", "END_DATE"]:
                                        parsed = pd.to_datetime(dfzip.get(dcol), errors="coerce")
                                        dfzip[dcol] = parsed.map(lambda x: None if pd.isna(x) else x.date())
                                    start_dt = pd.to_datetime(dfzip.get("START_DATE"), errors="coerce")
                                    if dfzip.get("YEAR") is None or pd.to_numeric(dfzip.get("YEAR"), errors="coerce").isna().all():
                                        dfzip["YEAR"] = start_dt.dt.year.astype("Int64")
                                    if dfzip.get("MONTH_KEY") is None or dfzip.get("MONTH_KEY").isna().all():
                                        dfzip["MONTH_KEY"] = start_dt.dt.strftime("%Y-%m").where(start_dt.notna(), None)
                                    if dfzip.get("QUARTER_KEY") is None or dfzip.get("QUARTER_KEY").isna().all():
                                        q = ((start_dt.dt.month - 1) // 3 + 1).astype("Int64")
                                        dfzip["QUARTER_KEY"] = (
                                            start_dt.dt.year.astype("Int64").astype(str)
                                            + "-Q"
                                            + q.astype("Int64").astype(str)
                                        ).where(start_dt.notna(), None)
                                    if dfzip.get("ITERATION_GRAIN") is None or dfzip.get("ITERATION_GRAIN").isna().all():
                                        def _grain(path: Any) -> Optional[str]:
                                            s = str(path or "").strip()
                                            if not s:
                                                return None
                                            s_u = s.upper().replace("\\", "/")
                                            return "SPRINT" if ("/S" in s_u or " S" in s_u) else "PI"

                                        dfzip["ITERATION_GRAIN"] = dfzip.get("ITERATION_PATH").apply(_grain)
                                    cols = [
                                        "ITERATION_PATH",
                                        "ITERATION_SK",
                                        "ITERATION_LEVEL3",
                                        "ITERATION_GRAIN",
                                        "START_DATE",
                                        "END_DATE",
                                        "YEAR",
                                        "MONTH_KEY",
                                        "QUARTER_KEY",
                                    ]
                                    n += int(upsert_ado_iteration_calendar(dfzip[cols].copy()))
                                    try:
                                        df_null = fetch_df(
                                            "SELECT COUNT(*) AS N_NULL FROM ADO_ITERATION_CALENDAR WHERE ITERATION_SK IS NULL OR LTRIM(RTRIM(CONVERT(NVARCHAR(100), ITERATION_SK))) = ''"
                                        )
                                        n_null = int(df_null.iloc[0]["N_NULL"]) if df_null is not None and not df_null.empty else 0
                                        if n_null > 0:
                                            st.warning(f"Iteration calendar restore incomplete: ITERATION_SK still NULL for {n_null:,} row(s).")
                                    except Exception:
                                        pass
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ADO_FEATURES":
                                ensure_ado_minimal_tables()
                                required = ["FEATURE_ID"]
                                optional = [
                                    "TITLE",
                                    "STATE",
                                    "ITERATION_LEVEL3",
                                    "ITERATION_SK",
                                    "PROGRAM_RAW",
                                    "AREA_LEVEL2_RAW",
                                    "AREA_LEVEL3_RAW",
                                    "AREA_LEVEL4_RAW",
                                    "AREA_PATH_RAW",
                                    "TEAM_VARIANT_KEY",
                                    "BUSINESS_VALUE",
                                    "STORY_POINTS",
                                    "EFFORT_POINTS",
                                    "PARENT_ID",
                                    "EPIC_ID",
                                    "EPIC_TITLE",
                                    "EPIC_STATE",
                                    "ADO_YEAR",
                                    "ITERATION_PATH",
                                    "CREATED_AT",
                                    "CHANGED_AT",
                                    "INVESTMENT_DIMENSION",
                                    "APP_NAME_RAW",
                                    "TEAM_RAW",
                                ]
                                dfzip.columns = [str(c).strip() for c in dfzip.columns]
                                _normalize_ado_feature_columns(dfzip)
                                dfzip = repair_leaf_teams(dfzip)
                                db_cols = _get_table_columns("ADO_FEATURES")
                                db_map = {str(c).strip().upper(): str(c).strip() for c in db_cols}
                                df_map = {str(c).strip().upper(): c for c in dfzip.columns}
                                rename_map: Dict[str, str] = {}
                                for upper, src in df_map.items():
                                    if upper in db_map and src != db_map[upper]:
                                        rename_map[src] = db_map[upper]
                                if rename_map:
                                    dfzip.rename(columns=rename_map, inplace=True)

                                if "CHANGED_AT" not in dfzip.columns and "CHANGED_DATE" in dfzip.columns and "CHANGED_AT" in db_map.values():
                                    dfzip["CHANGED_AT"] = dfzip["CHANGED_DATE"]
                                if "CHANGED_DATE" not in dfzip.columns and "CHANGED_AT" in dfzip.columns and "CHANGED_DATE" in db_map.values():
                                    dfzip["CHANGED_DATE"] = dfzip["CHANGED_AT"]
                                if "CREATED_AT" not in dfzip.columns and "CREATED_DATE" in dfzip.columns and "CREATED_AT" in db_map.values():
                                    dfzip["CREATED_AT"] = dfzip["CREATED_DATE"]
                                if "CREATED_DATE" not in dfzip.columns and "CREATED_AT" in dfzip.columns and "CREATED_DATE" in db_map.values():
                                    dfzip["CREATED_DATE"] = dfzip["CREATED_AT"]
                                if "INVESTMENT_DIMENSION" not in dfzip.columns and "INVESTMENT_DIMENSION_RAW" in dfzip.columns and "INVESTMENT_DIMENSION" in db_map.values():
                                    dfzip["INVESTMENT_DIMENSION"] = dfzip["INVESTMENT_DIMENSION_RAW"]

                                missing_required = [c for c in required if c not in dfzip.columns]
                                if missing_required:
                                    st.error(f"ADO_FEATURES missing required columns (cannot restore): {missing_required}")
                                else:
                                    missing_optional = [c for c in optional if c not in dfzip.columns]
                                    if missing_optional:
                                        st.info(f"Missing optional columns (OK): {', '.join(missing_optional)}")
                                    if db_cols:
                                        db_upper = {str(c).strip().upper() for c in db_cols}
                                        extra_cols = [c for c in dfzip.columns if str(c).strip().upper() not in db_upper]
                                        if extra_cols:
                                            st.info(f"Dropping {len(extra_cols)} column(s) not present in DB: {', '.join(extra_cols[:12])}")
                                            dfzip = dfzip.drop(columns=extra_cols, errors="ignore")
                                    for col in optional:
                                        if col not in dfzip.columns:
                                            dfzip[col] = None
                                    if "STORY_POINTS" in dfzip.columns:
                                        dfzip["STORY_POINTS"] = pd.to_numeric(dfzip["STORY_POINTS"], errors="coerce")
                                    if "EFFORT_POINTS" in dfzip.columns:
                                        dfzip["EFFORT_POINTS"] = pd.to_numeric(dfzip["EFFORT_POINTS"], errors="coerce")
                                    total = len(dfzip)
                                    chunk_size = 1000 if total > 5000 else (500 if total > 2000 else 250)
                                    for start in range(0, total, chunk_size):
                                        stop = min(start + chunk_size, total)
                                        n += upsert_ado_features(dfzip.iloc[start:stop], allow_null_overwrite=True)
                                    try:
                                        df_cov = fetch_df(
                                            "SELECT COUNT(*) AS N, SUM(CASE WHEN TRY_CONVERT(FLOAT, STORY_POINTS) > 0 THEN 1 ELSE 0 END) AS N_SP FROM ADO_FEATURES"
                                        )
                                        if df_cov is not None and not df_cov.empty:
                                            n_total = int(df_cov.iloc[0]["N"] or 0)
                                            n_sp = int(df_cov.iloc[0]["N_SP"] or 0)
                                            pct = (float(n_sp) / float(n_total) * 100.0) if n_total else 0.0
                                            st.caption(f"Story Points coverage: {n_sp}/{n_total} ({pct:.1f}%)")
                                            if n_sp <= 0 and n_total > 0:
                                                st.error("Restore completed but STORY_POINTS not persisted to DB.")
                                    except Exception:
                                        pass
                                    try:
                                        _backup_restore_post_write_refresh("admin_backup_restore", bump_version=True)
                                    except Exception:
                                        pass
                                    # After restoring both ADO tables, recompute mapping/enrichment offline.
                                    st.info("Recomputing iteration calendar mappings…")
                                    with st.spinner("Updating feature↔PI calendar mapping views…"):
                                        if callable(_recompute_feature_iteration_mapping):
                                            _recompute_feature_iteration_mapping()
                                        else:
                                            ensure_all_views_ok()
                                    try:
                                        _backup_restore_post_write_refresh("admin_backup_restore", bump_version=True)
                                    except Exception:
                                        pass
                                    st.success("Iteration mapping completed.")
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ADO_PROFILES":
                                ensure_ado_profiles_table()
                                _ensure_column(dfzip, "PROFILE_ID", ["PROFILEID", "PROFILE_ID"])
                                _ensure_column(dfzip, "PROFILE_NAME", ["PROFILENAME", "PROFILE_NAME"])
                                _ensure_column(dfzip, "IS_ACTIVE", ["ISACTIVE", "IS_ACTIVE"])
                                _ensure_column(dfzip, "CONFIG_JSON", ["CONFIGJSON", "CONFIG_JSON"])
                                _ensure_column(dfzip, "UPDATED_AT", ["UPDATEDAT", "UPDATED_AT"])
                                _ensure_column(dfzip, "UPDATED_BY", ["UPDATEDBY", "UPDATED_BY"])
                                active_candidates: List[Tuple[str, Optional[pd.Timestamp]]] = []
                                for _, r in dfzip.iterrows():
                                    pid = str(r.get("PROFILE_ID") or "").strip() or None
                                    pname = str(r.get("PROFILE_NAME") or "").strip() or None
                                    config_raw = r.get("CONFIG_JSON")
                                    config_str = str(config_raw) if config_raw is not None else "{}"
                                    is_active = bool(_to_bool(r.get("IS_ACTIVE")) or False)
                                    updated_by = str(r.get("UPDATED_BY") or "").strip() or None
                                    upsert_ado_profile(
                                        profile_id=pid,
                                        profile_name=pname,
                                        config_json=config_str,
                                        is_active=is_active,
                                        updated_by=updated_by,
                                    )
                                    n += 1
                                    if is_active and pid:
                                        upd = pd.to_datetime(r.get("UPDATED_AT"), errors="coerce")
                                        active_candidates.append((pid, upd if not pd.isna(upd) else None))
                                if active_candidates:
                                    active_candidates.sort(key=lambda x: (x[1] is not None, x[1]), reverse=True)
                                    set_active_ado_profile(active_candidates[0][0])
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ADO_WORKITEM_LOOKUP":
                                ensure_ado_workitem_lookup_table()
                                _ensure_column(dfzip, "WORKITEM_ID", ["WORKITEMID", "WORK_ITEM_ID"])
                                _ensure_column(dfzip, "WORKITEM_TYPE", ["WORKITEMTYPE", "WORK_ITEM_TYPE"])
                                _ensure_column(dfzip, "TITLE", ["TITLE_TEXT", "WORKITEM_TITLE"])
                                _ensure_column(dfzip, "STATE", ["WORKITEM_STATE"])
                                _ensure_column(dfzip, "PARENT_ID", ["PARENTID", "PARENT_WORKITEM_ID", "PARENTWORKITEMID"])
                                _ensure_column(dfzip, "CHANGED_DATE", ["CHANGEDDATE", "CHANGED_AT", "CHANGED_AT"])
                                rows: List[Tuple[Any, ...]] = []
                                for _, r in dfzip.iterrows():
                                    wid = _to_int(r.get("WORKITEM_ID"))
                                    if wid is None:
                                        continue
                                    pval = _to_int(r.get("PARENT_ID"))
                                    cdt_raw = pd.to_datetime(r.get("CHANGED_DATE"), errors="coerce")
                                    cdt = None if pd.isna(cdt_raw) else cdt_raw.to_pydatetime()
                                    rows.append(
                                        (
                                            wid,
                                            _zip_clean(r.get("WORKITEM_TYPE")),
                                            _zip_clean(r.get("TITLE")),
                                            _zip_clean(r.get("STATE")),
                                            pval,
                                            cdt,
                                        )
                                    )
                                if rows:
                                    upsert_ado_workitem_lookup(rows)
                                n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ADO_WORKITEM_DATES":
                                ensure_ado_timeline_tables()
                                _ensure_column(dfzip, "WORKITEM_ID", ["WORKITEMID", "WORK_ITEM_ID"])
                                _ensure_column(dfzip, "WORKITEM_TYPE", ["WORKITEMTYPE", "WORK_ITEM_TYPE"])
                                _ensure_column(dfzip, "START_DATE", ["STARTDATE"])
                                _ensure_column(dfzip, "END_DATE", ["ENDDATE"])
                                _ensure_column(dfzip, "ITERATION_PATH", ["ITERATIONPATH"])
                                rows: List[Tuple[Any, ...]] = []
                                for _, r in dfzip.iterrows():
                                    wid = _to_int(r.get("WORKITEM_ID"))
                                    if wid is None:
                                        continue
                                    s_raw = pd.to_datetime(r.get("START_DATE"), errors="coerce")
                                    e_raw = pd.to_datetime(r.get("END_DATE"), errors="coerce")
                                    s_date = None if pd.isna(s_raw) else s_raw.date()
                                    e_date = None if pd.isna(e_raw) else e_raw.date()
                                    rows.append(
                                        (
                                            wid,
                                            _zip_clean(r.get("WORKITEM_TYPE")),
                                            s_date,
                                            e_date,
                                            _zip_clean(r.get("ITERATION_PATH")),
                                        )
                                    )
                                if rows:
                                    upsert_ado_workitem_dates(rows)
                                n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ADO_WORKITEM_LINKS":
                                ensure_ado_timeline_tables()
                                _ensure_column(dfzip, "SOURCE_ID", ["SOURCEID", "SOURCE_WORKITEM_ID"])
                                _ensure_column(dfzip, "TARGET_ID", ["TARGETID", "TARGET_WORKITEM_ID"])
                                _ensure_column(dfzip, "LINK_CATEGORY", ["LINKCATEGORY"])
                                _ensure_column(dfzip, "LINK_TYPE", ["LINKTYPE"])
                                rows: List[Tuple[Any, ...]] = []
                                for _, r in dfzip.iterrows():
                                    source_id = _to_int(r.get("SOURCE_ID"))
                                    target_id = _to_int(r.get("TARGET_ID"))
                                    if source_id is None or target_id is None:
                                        continue
                                    rows.append(
                                        (
                                            source_id,
                                            target_id,
                                            _zip_clean(r.get("LINK_CATEGORY")) or "Related",
                                            _zip_clean(r.get("LINK_TYPE")) or "Related",
                                        )
                                    )
                                if rows:
                                    upsert_ado_workitem_links(rows)
                                n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ADO_FEATURE_PROGRESS":
                                ensure_ado_progress_tables()
                                _ensure_column(dfzip, "FEATURE_ID", ["WORKITEM_ID", "WORK_ITEM_ID"])
                                rows: List[Tuple[Any, ...]] = []
                                for _, r in dfzip.iterrows():
                                    fid = _to_int(r.get("FEATURE_ID"))
                                    if fid is None:
                                        continue
                                    rows.append(
                                        (
                                            fid,
                                            _to_float(r.get("PROPOSED_SP")) or 0.0,
                                            _to_float(r.get("INPROGRESS_SP")) or 0.0,
                                            _to_float(r.get("COMPLETED_SP")) or 0.0,
                                            _to_float(r.get("TOTAL_SP")) or 0.0,
                                            _to_float(r.get("PCT_COMPLETE")) or 0.0,
                                        )
                                    )
                                if rows:
                                    upsert_ado_feature_progress(rows)
                                n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ADO_EPIC_PROGRESS":
                                ensure_ado_progress_tables()
                                _ensure_column(dfzip, "EPIC_ID", ["WORKITEM_ID", "WORK_ITEM_ID"])
                                rows: List[Tuple[Any, ...]] = []
                                for _, r in dfzip.iterrows():
                                    eid = _to_int(r.get("EPIC_ID"))
                                    if eid is None:
                                        continue
                                    rows.append(
                                        (
                                            eid,
                                            _to_float(r.get("PROPOSED_SP")) or 0.0,
                                            _to_float(r.get("INPROGRESS_SP")) or 0.0,
                                            _to_float(r.get("COMPLETED_SP")) or 0.0,
                                            _to_float(r.get("TOTAL_SP")) or 0.0,
                                            _to_float(r.get("PCT_COMPLETE")) or 0.0,
                                            _to_int(r.get("FEATURE_COUNT_TOTAL")),
                                            _to_int(r.get("FEATURE_COUNT_WITH_STORIES")),
                                            _to_int(r.get("FEATURE_COUNT_WITH_SP")),
                                        )
                                    )
                                if rows:
                                    upsert_ado_epic_progress(rows)
                                n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ROADMAP_MILESTONES":
                                ensure_roadmap_milestones_table()
                                _ensure_column(dfzip, "MILESTONE_ID", ["MILESTONEID", "ID"])
                                _ensure_column(dfzip, "TITLE", ["NAME", "MILESTONE_NAME"])
                                _ensure_column(dfzip, "TARGET_DATE", ["DATE", "MILESTONE_DATE", "TARGETDATE"])
                                _ensure_column(dfzip, "PI_KEY", ["PI", "PI_LABEL"])
                                _ensure_column(dfzip, "EPIC_ID", ["EPICID"])
                                _ensure_column(dfzip, "FEATURE_ID", ["FEATUREID"])
                                _ensure_column(dfzip, "TAG", ["MILESTONE_TAG"])
                                _ensure_column(dfzip, "SOURCE_TYPE", ["SOURCE", "TYPE"])
                                _ensure_column(dfzip, "PROGRAM_NAME", ["PROGRAM", "PROGRAMNAME"])
                                _ensure_column(dfzip, "TEAM_NAME", ["TEAM", "TEAMNAME"])
                                _ensure_column(dfzip, "IS_ACTIVE", ["ACTIVE"])
                                _ensure_column(dfzip, "UPDATED_BY", ["UPDATEDBY"])
                                rows: List[Tuple[Any, ...]] = []
                                for _, r in dfzip.iterrows():
                                    mid = _zip_clean(r.get("MILESTONE_ID"))
                                    title = _zip_clean(r.get("TITLE"))
                                    dt_raw = pd.to_datetime(r.get("TARGET_DATE"), errors="coerce")
                                    if not mid or not title or pd.isna(dt_raw):
                                        continue
                                    rows.append(
                                        (
                                            mid,
                                            title,
                                            dt_raw.date(),
                                            _zip_clean(r.get("PI_KEY")),
                                            _to_int(r.get("EPIC_ID")),
                                            _to_int(r.get("FEATURE_ID")),
                                            _zip_clean(r.get("TAG")),
                                            (_zip_clean(r.get("SOURCE_TYPE")) or "").upper() or None,
                                            _zip_clean(r.get("PROGRAM_NAME")),
                                            _zip_clean(r.get("TEAM_NAME")),
                                            bool(_to_bool(r.get("IS_ACTIVE"))),
                                            _zip_clean(r.get("UPDATED_BY")),
                                        )
                                    )
                                if rows:
                                    upsert_roadmap_milestones(rows)
                                n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "MAP_ADO_PROGRAM_TO_TCO_PROGRAM":
                                ensure_ado_minimal_tables()
                                rows = []
                                for _, r in dfzip.iterrows():
                                    ado = _zip_clean(r.get("ADO_PROGRAM"))
                                    pid = _zip_clean(r.get("PROGRAMID"))
                                    if ado:
                                        rows.append((ado, pid))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM')} t USING (SELECT %s ADO_PROGRAM, %s PROGRAMID) s ON t.ADO_PROGRAM = s.ADO_PROGRAM WHEN MATCHED THEN UPDATE SET PROGRAMID = s.PROGRAMID WHEN NOT MATCHED THEN INSERT (ADO_PROGRAM, PROGRAMID) VALUES (s.ADO_PROGRAM, s.PROGRAMID);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "MAP_ADO_TEAM_TO_TCO_TEAM":
                                ensure_ado_minimal_tables()
                                def _team_key(row):
                                    key = _zip_clean(row.get("ADO_TEAM_KEY"))
                                    if key:
                                        return key
                                    alt = _zip_clean(row.get("ADO_TEAM")) or _zip_clean(row.get("ADO_TEAM_NAME"))
                                    if alt:
                                        return alt
                                    al3 = _zip_clean(row.get("AREA_LEVEL3_RAW")) or ""
                                    al4 = _zip_clean(row.get("AREA_LEVEL4_RAW")) or ""
                                    comp = f"{al3}|{al4}".strip("|")
                                    return comp or None
                                rows = [
                                    (
                                        _team_key(r),
                                        _zip_clean(r.get("TEAMID")),
                                        _zip_clean(r.get("PROGRAM_RAW")),
                                        _zip_clean(r.get("AREA_LEVEL3_RAW")),
                                        _zip_clean(r.get("AREA_LEVEL4_RAW")),
                                        _zip_clean(r.get("ADO_TEAM")) or _zip_clean(r.get("ADO_TEAM_NAME")),
                                    )
                                    for _, r in dfzip.iterrows()
                                    if _team_key(r)
                                ]
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('MAP_ADO_TEAM_TO_TCO_TEAM')} t USING (SELECT %s ADO_TEAM_KEY, %s TEAMID, %s PROGRAM_RAW, %s AREA_LEVEL3_RAW, %s AREA_LEVEL4_RAW, %s ADO_TEAM) s ON t.ADO_TEAM_KEY=s.ADO_TEAM_KEY WHEN MATCHED THEN UPDATE SET TEAMID=s.TEAMID, PROGRAM_RAW=s.PROGRAM_RAW, AREA_LEVEL3_RAW=s.AREA_LEVEL3_RAW, AREA_LEVEL4_RAW=s.AREA_LEVEL4_RAW, ADO_TEAM=s.ADO_TEAM WHEN NOT MATCHED THEN INSERT (ADO_TEAM_KEY, TEAMID, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, ADO_TEAM) VALUES (s.ADO_TEAM_KEY, s.TEAMID, s.PROGRAM_RAW, s.AREA_LEVEL3_RAW, s.AREA_LEVEL4_RAW, s.ADO_TEAM);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "MAP_ADO_APP_TO_TCO_GROUP":
                                ensure_ado_minimal_tables()
                                rows = []
                                for _, r in dfzip.iterrows():
                                    ado = str(r.get("ADO_APP") or "").strip()
                                    if not ado:
                                        continue
                                    app_group_raw = (str(r.get("APP_GROUP")) if pd.notna(r.get("APP_GROUP")) else None)
                                    app_group = _remap_group_id(app_group_raw, group_id_map) if app_group_raw else None
                                    rows.append((ado, app_group))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('MAP_ADO_APP_TO_TCO_GROUP')} t USING (SELECT %s ADO_APP, %s APP_GROUP) s ON t.ADO_APP=s.ADO_APP WHEN MATCHED THEN UPDATE SET APP_GROUP=s.APP_GROUP WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "PROGRAMS":
                                for _, r in dfzip.iterrows():
                                    pid = str(r.get("PROGRAMID") or "").strip()
                                    if not pid:
                                        st.error("PROGRAMS restore requires PROGRAMID in CSV; aborting to preserve IDs.")
                                        st.stop()
                                    program_name = (
                                        _opt_str(r.get("PROGRAMNAME"))
                                        or _opt_str(r.get("PROGRAMNAME_RAW"))
                                        or _opt_str(r.get("PROGRAM_DISPLAY_NAME"))
                                        or _opt_str(r.get("DISPLAY_NAME"))
                                        or pid
                                    )
                                    program_display_name = (
                                        _opt_str(r.get("PROGRAM_DISPLAY_NAME"))
                                        or _opt_str(r.get("DISPLAY_NAME"))
                                    )
                                    upsert_program(
                                        pid,
                                        program_name,
                                        _opt_str(r.get("PROGRAMOWNER")),
                                        (float(r.get("PROGRAMFTE")) if pd.notna(r.get("PROGRAMFTE")) else None),
                                        (float(r.get("PROGRAM_XOM_RATE")) if pd.notna(r.get("PROGRAM_XOM_RATE")) else None),
                                        program_display_name=program_display_name,
                                    ); n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "PROGRAM_COMPOSITION_HISTORY":
                                ensure_program_composition_history()
                                rows = []
                                for _, r in dfzip.iterrows():
                                    pid = str(r.get("PROGRAMID") or "").strip()
                                    if not pid:
                                        continue
                                    year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                                    if year is None:
                                        continue
                                    rows.append((
                                        pid,
                                        year,
                                        pi_val,
                                        float(r.get("PROGRAMFTE") or 0.0) if pd.notna(r.get("PROGRAMFTE")) else 0.0,
                                        (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None),
                                    ))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('PROGRAM_COMPOSITION_HISTORY')} t USING (SELECT %s PROGRAMID, %s YEAR, %s PI, %s PROGRAMFTE, %s UPDATED_BY) s ON t.PROGRAMID=s.PROGRAMID AND t.YEAR=s.YEAR AND ISNULL(t.PI,0)=ISNULL(s.PI,0) WHEN MATCHED THEN UPDATE SET PROGRAMFTE=s.PROGRAMFTE, UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY) WHEN NOT MATCHED THEN INSERT (PROGRAMID, YEAR, PI, PROGRAMFTE, UPDATED_AT, UPDATED_BY) VALUES (s.PROGRAMID, s.YEAR, s.PI, s.PROGRAMFTE, SYSDATETIME(), s.UPDATED_BY);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "PROGRAM_ADDITIONAL_COSTS":
                                for _, r in dfzip.iterrows():
                                    pid = str(r.get("PROGRAMID") or "").strip()
                                    if not pid:
                                        continue
                                    year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    month = int(r.get("MONTH")) if pd.notna(r.get("MONTH")) else None
                                    ctype = str(r.get("COST_TYPE") or "").strip()
                                    subtype = str(r.get("SUBTYPE") or "").strip()
                                    if year is None or month is None or not ctype:
                                        continue
                                    amt = float(r.get("AMOUNT") or 0.0) if pd.notna(r.get("AMOUNT")) else 0.0
                                    desc = (str(r.get("DESCRIPTION")).strip() if pd.notna(r.get("DESCRIPTION")) else None)
                                    cur = (str(r.get("CURRENCY")).strip() if pd.notna(r.get("CURRENCY")) else None)
                                    rec = None
                                    if pd.notna(r.get("IS_RECURRING")):
                                        try:
                                            rec = bool(int(r.get("IS_RECURRING")))
                                        except Exception:
                                            rec = bool(r.get("IS_RECURRING"))
                                    upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    upsert_program_additional_cost(
                                        pid,
                                        year,
                                        month,
                                        ctype,
                                        subtype,
                                        amt,
                                        updated_by=upd,
                                        description=desc,
                                        currency=cur,
                                        is_recurring=rec,
                                    )
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "PROGRAM_APPTIO_WORKIDS":
                                for _, r in dfzip.iterrows():
                                    pid = str(r.get("PROGRAMID") or "").strip()
                                    work_id = str(r.get("WORK_ID") or "").strip()
                                    if not pid or not work_id:
                                        continue
                                    upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    upsert_program_apptio_workid(pid, work_id, updated_by=upd)
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "APPTIO_ACTUALS":
                                rows = []
                                for _, r in dfzip.iterrows():
                                    work_id = str(r.get("WORK_ID") or "").strip()
                                    if not work_id:
                                        continue
                                    if pd.isna(r.get("FISCAL_YEAR")) or pd.isna(r.get("MONTH")):
                                        continue
                                    rows.append({
                                        "work_id": work_id,
                                        "fiscal_year": int(r.get("FISCAL_YEAR")),
                                        "month": int(r.get("MONTH")),
                                        "amount": float(r.get("AMOUNT") or 0.0) if pd.notna(r.get("AMOUNT")) else 0.0,
                                        "source": (str(r.get("SOURCE")) if pd.notna(r.get("SOURCE")) else None),
                                        "loaded_by": (str(r.get("LOADED_BY")) if pd.notna(r.get("LOADED_BY")) else None),
                                    })
                                n += upsert_apptio_actuals(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "TEAMS":
                                for _, r in dfzip.iterrows():
                                    tid = str(r.get("TEAMID") or "").strip()
                                    if not tid:
                                        st.error("TEAMS restore requires TEAMID in CSV; aborting to preserve IDs.")
                                        st.stop()
                                    team_name = (
                                        _opt_str(r.get("TEAMNAME"))
                                        or _opt_str(r.get("TEAMNAME_RAW"))
                                        or _opt_str(r.get("TEAM_DISPLAY_NAME"))
                                        or _opt_str(r.get("DISPLAY_NAME"))
                                        or tid
                                    )
                                    team_display_name = (
                                        _opt_str(r.get("TEAM_DISPLAY_NAME"))
                                        or _opt_str(r.get("DISPLAY_NAME"))
                                    )
                                    upsert_team(
                                        tid,
                                        team_name,
                                        _opt_str(r.get("PROGRAMID")),
                                        (float(r.get("TEAMFTE")) if pd.notna(r.get("TEAMFTE")) else None),
                                        (float(r.get("DELIVERY_TEAM_FTE")) if pd.notna(r.get("DELIVERY_TEAM_FTE")) else None),
                                        (float(r.get("CONTRACTOR_C_FTE")) if pd.notna(r.get("CONTRACTOR_C_FTE")) else None),
                                        (float(r.get("CONTRACTOR_CS_FTE")) if pd.notna(r.get("CONTRACTOR_CS_FTE")) else None),
                                        team_display_name=team_display_name,
                                    )
                                    try:
                                        execute(f"UPDATE { _fq('TEAMS') } SET PRODUCTOWNER=%s WHERE TEAMID=%s", (_opt_str(r.get("PRODUCTOWNER")), tid))
                                    except Exception:
                                        pass
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "TEAM_COMPOSITION_HISTORY":
                                ensure_team_composition_history()
                                rows = []
                                for _, r in dfzip.iterrows():
                                    tid = str(r.get("TEAMID") or "").strip()
                                    if not tid:
                                        continue
                                    year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                                    if year is None:
                                        continue
                                    rows.append((
                                        tid,
                                        year,
                                        pi_val,
                                        float(r.get("TEAMFTE") or 0.0) if pd.notna(r.get("TEAMFTE")) else 0.0,
                                        float(r.get("DELIVERY_TEAM_FTE") or 0.0) if pd.notna(r.get("DELIVERY_TEAM_FTE")) else 0.0,
                                        float(r.get("CONTRACTOR_CS_FTE") or 0.0) if pd.notna(r.get("CONTRACTOR_CS_FTE")) else 0.0,
                                        float(r.get("CONTRACTOR_C_FTE") or 0.0) if pd.notna(r.get("CONTRACTOR_C_FTE")) else 0.0,
                                    ))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('TEAM_COMPOSITION_HISTORY')} t USING (SELECT %s TEAMID, %s YEAR, %s PI, %s TEAMFTE, %s DELIVERY_TEAM_FTE, %s CONTRACTOR_CS_FTE, %s CONTRACTOR_C_FTE) s ON t.TEAMID=s.TEAMID AND t.YEAR=s.YEAR AND ISNULL(t.PI,0)=ISNULL(s.PI,0) WHEN MATCHED THEN UPDATE SET TEAMFTE=s.TEAMFTE, DELIVERY_TEAM_FTE=s.DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE=s.CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE=s.CONTRACTOR_C_FTE, UPDATED_AT=SYSDATETIME() WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE, UPDATED_AT) VALUES (s.TEAMID, s.YEAR, s.PI, s.TEAMFTE, s.DELIVERY_TEAM_FTE, s.CONTRACTOR_CS_FTE, s.CONTRACTOR_C_FTE, SYSDATETIME());",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "VENDORS":
                                for _, r in dfzip.iterrows(): upsert_vendor(str(r.get("VENDORID") or uuid.uuid4()), str(r.get("VENDORNAME") or "")); n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "APPLICATION_GROUPS":
                                for _, r in dfzip.iterrows():
                                    group_name = str(r.get("GROUPNAME") or "").strip()
                                    if not group_name:
                                        continue
                                    gid_in = str(r.get("GROUPID") or "").strip() or None
                                    gid = _resolve_group_id(group_name, gid_in, group_name_map, group_id_map)
                                    team_id = (str(r.get("TEAMID")) if pd.notna(r.get("TEAMID")) else None)
                                    default_vendor = (str(r.get("DEFAULT_VENDORID")) if pd.notna(r.get("DEFAULT_VENDORID")) else None)
                                    owner = (str(r.get("OWNER")) if pd.notna(r.get("OWNER")) else None)
                                    is_base = bool(r.get("IS_BASE")) if "IS_BASE" in r else False
                                    upsert_application_group(
                                        group_id=gid,
                                        group_name=group_name,
                                        team_id=team_id,
                                        default_vendor_id=default_vendor,
                                        owner=owner,
                                        is_base=is_base,
                                    )
                                    try:
                                        prog_id = (str(r.get("PROGRAMID")) if pd.notna(r.get("PROGRAMID")) else None)
                                        if prog_id:
                                            execute(f"UPDATE { _fq('APPLICATION_GROUPS') } SET PROGRAMID=%s WHERE GROUPID=%s", (prog_id, gid))
                                    except Exception:
                                        pass
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "APP_GROUP_TEAM_LINKS":
                                rows = []
                                for _, r in dfzip.iterrows():
                                    gid = _zip_clean(r.get("GROUPID"))
                                    tid = _zip_clean(r.get("TEAMID"))
                                    if gid:
                                        gid = _remap_group_id(gid, group_id_map)
                                    if gid and tid:
                                        rows.append((gid, tid))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('APP_GROUP_TEAM_LINKS')} t USING (SELECT %s GROUPID, %s TEAMID) s ON t.GROUPID=s.GROUPID AND t.TEAMID=s.TEAMID WHEN NOT MATCHED THEN INSERT (GROUPID, TEAMID) VALUES (s.GROUPID, s.TEAMID);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "APPLICATIONS":
                                _app_name_cache: Dict[str, str] = {}
                                def _resolve_app_id_by_name(name: str) -> Optional[str]:
                                    key = str(name or "").strip()
                                    if not key:
                                        return None
                                    if key in _app_name_cache:
                                        return _app_name_cache[key]
                                    df_lookup = fetch_df(f"SELECT APPLICATIONID FROM {_fq('APPLICATIONS')} WHERE UPPER(APPLICATIONNAME)=UPPER(%s)", (key,))
                                    if df_lookup is not None and not df_lookup.empty:
                                        val = str(df_lookup.iloc[0]["APPLICATIONID"])
                                        _app_name_cache[key] = val
                                        return val
                                    return None

                                for _, r in dfzip.iterrows():
                                    name = str(r.get("APPLICATIONNAME") or "")
                                    aid = str(r.get("APPLICATIONID") or "").strip() or (_resolve_app_id_by_name(name) or str(uuid.uuid4()))
                                    group_id = (str(r.get("GROUPID")) if pd.notna(r.get("GROUPID")) else None)
                                    if group_id:
                                        group_id = _remap_group_id(group_id, group_id_map)
                                    add_info = (str(r.get("ADD_INFO")) if pd.notna(r.get("ADD_INFO")) else None)
                                    vendor_id = (str(r.get("VENDORID")) if pd.notna(r.get("VENDORID")) else None)
                                    try:
                                        upsert_application_instance(
                                            aid,
                                            group_id,
                                            name,
                                            add_info,
                                            vendor_id,
                                        )
                                    except Exception:
                                        alt = _resolve_app_id_by_name(name)
                                        if alt and alt != aid:
                                            upsert_application_instance(
                                                alt,
                                                group_id,
                                                name,
                                                add_info,
                                                vendor_id,
                                            )
                                        else:
                                            raise
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "TEAM_RATE_HISTORY":
                                for _, r in dfzip.iterrows():
                                    tid = _zip_clean(r.get("TEAMID"))
                                    if not tid:
                                        continue
                                    year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    if year is None:
                                        continue
                                    pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                                    loc = _zip_clean(r.get("LOCATION")) or "GBC"
                                    rate = float(r.get("XOM_RATE") or 0.0) if pd.notna(r.get("XOM_RATE")) else None
                                    upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    upsert_team_rate_history(tid, year, pi_val, loc, rate, updated_by=upd)
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "PROGRAM_RATE_HISTORY":
                                for _, r in dfzip.iterrows():
                                    pid = _zip_clean(r.get("PROGRAMID"))
                                    if not pid:
                                        continue
                                    year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    if year is None:
                                        continue
                                    pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                                    loc = _zip_clean(r.get("LOCATION")) or "GBC"
                                    rate = float(r.get("PROGRAM_XOM_RATE") or 0.0) if pd.notna(r.get("PROGRAM_XOM_RATE")) else None
                                    upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    upsert_program_rate_history(pid, year, pi_val, loc, rate, updated_by=upd)
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "TEAM_HEADCOUNT_HISTORY":
                                for _, r in dfzip.iterrows():
                                    tid = _zip_clean(r.get("TEAMID"))
                                    if not tid:
                                        continue
                                    year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    if year is None:
                                        continue
                                    pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                                    cls = _zip_clean(r.get("CLASS"))
                                    loc = _zip_clean(r.get("LOCATION"))
                                    headcount = float(r.get("HEADCOUNT") or 0.0) if pd.notna(r.get("HEADCOUNT")) else 0.0
                                    upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    if cls and loc:
                                        upsert_team_headcount(tid, year, pi_val, cls, loc, headcount, updated_by=upd)
                                        n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "CONTRACTOR_COMPANY":
                                for _, r in dfzip.iterrows():
                                    cid = str(r.get("COMPANYID") or uuid.uuid4())
                                    name = _zip_clean(r.get("NAME"))
                                    if not name:
                                        continue
                                    active = _to_bool(r.get("ACTIVE"))
                                    if active is None:
                                        active = True
                                    notes = (str(r.get("NOTES")) if pd.notna(r.get("NOTES")) else None)
                                    upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    upsert_contractor_company(cid, name, active=active, notes=notes, updated_by=upd)
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "CONTRACTOR_RATE_HISTORY":
                                for _, r in dfzip.iterrows():
                                    cid = _zip_clean(r.get("COMPANYID"))
                                    if not cid:
                                        continue
                                    year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    if year is None:
                                        continue
                                    pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                                    cls = _zip_clean(r.get("CLASS"))
                                    rate = float(r.get("RATE") or 0.0) if pd.notna(r.get("RATE")) else None
                                    upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    if cls:
                                        upsert_contractor_rate(cid, year, pi_val, cls, rate, updated_by=upd)
                                        n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "TEAM_CONTRACTOR_HEADCOUNT":
                                for _, r in dfzip.iterrows():
                                    tid = _zip_clean(r.get("TEAMID"))
                                    cid = _zip_clean(r.get("COMPANYID"))
                                    if not tid or not cid:
                                        continue
                                    year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    if year is None:
                                        continue
                                    pi_val = int(r.get("PI")) if pd.notna(r.get("PI")) else 0
                                    cls = _zip_clean(r.get("CLASS"))
                                    headcount = float(r.get("HEADCOUNT") or 0.0) if pd.notna(r.get("HEADCOUNT")) else 0.0
                                    upd = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    if cls:
                                        upsert_team_contractor_headcount(tid, year, pi_val, cls, cid, headcount, updated_by=upd)
                                        n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "TEAM_MSP_RATE":
                                rows = []
                                for _, r in dfzip.iterrows():
                                    enabled = _to_bool(r.get("MSP_ENABLED"))
                                    rows.append((
                                        str(r.get("TEAMID")),
                                        1 if enabled else 0,
                                        (str(r.get("MSP_SIZE")) if pd.notna(r.get("MSP_SIZE")) else None),
                                        _to_float(r.get("MSP_RATE_PER_PI")),
                                        _to_float(r.get("MSP_RATE_SMALL")),
                                        _to_float(r.get("MSP_RATE_MEDIUM")),
                                        _to_float(r.get("MSP_RATE_LARGE")),
                                    ))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('TEAM_MSP_RATE')} t USING (SELECT %s TEAMID, %s MSP_ENABLED, %s MSP_SIZE, %s MSP_RATE_PER_PI, %s MSP_RATE_SMALL, %s MSP_RATE_MEDIUM, %s MSP_RATE_LARGE) s ON t.TEAMID=s.TEAMID WHEN MATCHED THEN UPDATE SET MSP_ENABLED=s.MSP_ENABLED, MSP_SIZE=s.MSP_SIZE, MSP_RATE_PER_PI=s.MSP_RATE_PER_PI, MSP_RATE_SMALL=s.MSP_RATE_SMALL, MSP_RATE_MEDIUM=s.MSP_RATE_MEDIUM, MSP_RATE_LARGE=s.MSP_RATE_LARGE, UPDATED_AT=SYSDATETIME() WHEN NOT MATCHED THEN INSERT (TEAMID, MSP_ENABLED, MSP_SIZE, MSP_RATE_PER_PI, MSP_RATE_SMALL, MSP_RATE_MEDIUM, MSP_RATE_LARGE) VALUES (s.TEAMID, s.MSP_ENABLED, s.MSP_SIZE, s.MSP_RATE_PER_PI, s.MSP_RATE_SMALL, s.MSP_RATE_MEDIUM, s.MSP_RATE_LARGE);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "TEAM_MSP_ASSIGNMENTS":
                                ensure_team_msp_assignments_table()
                                rows = []
                                for _, r in dfzip.iterrows():
                                    tid = _zip_clean(r.get("TEAMID"))
                                    if not tid:
                                        continue
                                    gid = _zip_clean(r.get("GROUPID"))
                                    if gid:
                                        gid = _remap_group_id(gid, group_id_map)
                                    eff_year = int(r.get("EFFECTIVE_YEAR")) if pd.notna(r.get("EFFECTIVE_YEAR")) else 0
                                    year_val = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    it_start = int(r.get("ITERATION_START")) if pd.notna(r.get("ITERATION_START")) else None
                                    it_end = int(r.get("ITERATION_END")) if pd.notna(r.get("ITERATION_END")) else None
                                    size = (str(r.get("MSP_SIZE")) if pd.notna(r.get("MSP_SIZE")) else None)
                                    weight = float(r.get("WEIGHT_PCT")) if pd.notna(r.get("WEIGHT_PCT")) else None
                                    rows.append((tid, gid, eff_year, year_val, it_start, it_end, size, weight))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('TEAM_MSP_ASSIGNMENTS')} t USING (SELECT %s TEAMID, %s GROUPID, %s EFFECTIVE_YEAR, %s YEAR, %s ITERATION_START, %s ITERATION_END, %s MSP_SIZE, %s WEIGHT_PCT) s ON t.TEAMID=s.TEAMID AND ISNULL(t.GROUPID,'')=ISNULL(s.GROUPID,'') AND t.EFFECTIVE_YEAR=s.EFFECTIVE_YEAR WHEN MATCHED THEN UPDATE SET YEAR=s.YEAR, ITERATION_START=s.ITERATION_START, ITERATION_END=s.ITERATION_END, MSP_SIZE=s.MSP_SIZE, WEIGHT_PCT=s.WEIGHT_PCT, UPDATED_AT=SYSDATETIME() WHEN NOT MATCHED THEN INSERT (TEAMID, GROUPID, EFFECTIVE_YEAR, [YEAR], ITERATION_START, ITERATION_END, MSP_SIZE, WEIGHT_PCT, UPDATED_AT) VALUES (s.TEAMID, s.GROUPID, s.EFFECTIVE_YEAR, s.YEAR, s.ITERATION_START, s.ITERATION_END, s.MSP_SIZE, s.WEIGHT_PCT, SYSDATETIME());",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "TCO_COMMITMENTS":
                                rows = []
                                for _, r in dfzip.iterrows():
                                    year = int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None
                                    if year is None:
                                        continue
                                    pi_val = (str(r.get("PI")) if pd.notna(r.get("PI")) else "")
                                    prog = (str(r.get("PROGRAMNAME")) if pd.notna(r.get("PROGRAMNAME")) else "")
                                    team = (str(r.get("TEAMNAME")) if pd.notna(r.get("TEAMNAME")) else "")
                                    group = (str(r.get("GROUPNAME")) if pd.notna(r.get("GROUPNAME")) else "")
                                    status = (str(r.get("STATUS")) if pd.notna(r.get("STATUS")) else "Not started")
                                    owner_email = (str(r.get("OWNER_EMAIL")) if pd.notna(r.get("OWNER_EMAIL")) else None)
                                    owner_name = (str(r.get("OWNER_NAME")) if pd.notna(r.get("OWNER_NAME")) else None)
                                    target_date = (str(r.get("TARGET_DATE")) if pd.notna(r.get("TARGET_DATE")) else None)
                                    notes = (str(r.get("NOTES")) if pd.notna(r.get("NOTES")) else None)
                                    updated_by = (str(r.get("UPDATED_BY")) if pd.notna(r.get("UPDATED_BY")) else None)
                                    rows.append((year, pi_val, prog, team, group, status, owner_email, owner_name, target_date, notes, updated_by))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('TCO_COMMITMENTS')} t USING (SELECT %s YEAR, %s PI, %s PROGRAMNAME, %s TEAMNAME, %s GROUPNAME, %s STATUS, %s OWNER_EMAIL, %s OWNER_NAME, %s TARGET_DATE, %s NOTES, %s UPDATED_BY) s ON t.YEAR=s.YEAR AND ISNULL(t.PI,'')=ISNULL(s.PI,'') AND ISNULL(t.PROGRAMNAME,'')=ISNULL(s.PROGRAMNAME,'') AND ISNULL(t.TEAMNAME,'')=ISNULL(s.TEAMNAME,'') AND ISNULL(t.GROUPNAME,'')=ISNULL(s.GROUPNAME,'') WHEN MATCHED THEN UPDATE SET STATUS=s.STATUS, OWNER_EMAIL=s.OWNER_EMAIL, OWNER_NAME=s.OWNER_NAME, TARGET_DATE=s.TARGET_DATE, NOTES=s.NOTES, UPDATED_AT=SYSUTCDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY) WHEN NOT MATCHED THEN INSERT (YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, STATUS, OWNER_EMAIL, OWNER_NAME, TARGET_DATE, NOTES, UPDATED_AT, UPDATED_BY) VALUES (s.YEAR, s.PI, s.PROGRAMNAME, s.TEAMNAME, s.GROUPNAME, s.STATUS, s.OWNER_EMAIL, s.OWNER_NAME, s.TARGET_DATE, s.NOTES, SYSUTCDATETIME(), s.UPDATED_BY);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            elif dsname == "CONTRACTS":
                                from db import upsert_contract, ensure_contracts_table
                                ensure_contracts_table()
                                for _, r in dfzip.iterrows():
                                    upsert_contract(
                                        str(r.get("CONTRACT_ID") or uuid.uuid4()),
                                        str(r.get("APPLICATIONID") or ""),
                                        str(r.get("TEAMID") or ""),
                                        _to_int(r.get("START_FY")) or 0,
                                        _to_int(r.get("END_FY")) or 0,
                                        _to_int(r.get("RENEWAL_MONTH")) or 1,
                                        _to_float(r.get("ANNUAL_AMOUNT")) or 0.0,
                                        _to_float(r.get("ESCALATION_PCT")) or 0.0,
                                        str(r.get("STATUS") or "Active"),
                                        (str(r.get("AGREEMENT_NUMBER")) if pd.notna(r.get("AGREEMENT_NUMBER")) else None),
                                        (str(r.get("COMPANY_CODE")) if pd.notna(r.get("COMPANY_CODE")) else None),
                                        (str(r.get("COST_CENTER")) if pd.notna(r.get("COST_CENTER")) else None),
                                        (str(r.get("SERVICE_TYPE")) if pd.notna(r.get("SERVICE_TYPE")) else None),
                                        (str(r.get("CONTRACT_RENEWAL_DATE")) if pd.notna(r.get("CONTRACT_RENEWAL_DATE")) else None),
                                        (str(r.get("INVOICE_RENEWAL_DATE")) if pd.notna(r.get("INVOICE_RENEWAL_DATE")) else None),
                                        _to_float(r.get("TOTAL_CONTRACT_COST")),
                                    )
                                    n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "INVOICES":
                                for _, r in dfzip.iterrows():
                                    inv_id = str(r.get("INVOICEID") or uuid.uuid4())
                                    def _opt_float(v):
                                        try: return float(v) if pd.notna(v) else None
                                        except Exception: return None
                                    def _opt_int(v):
                                        try: return int(float(v)) if pd.notna(v) else None
                                        except Exception: return None
                                    grp = _opt_str(r.get("GROUPID"))
                                    grp_book = _opt_str(r.get("GROUPID_AT_BOOKING"))
                                    if grp:
                                        grp = _remap_group_id(grp, group_id_map)
                                    if grp_book:
                                        grp_book = _remap_group_id(grp_book, group_id_map)
                                    upsert_invoice(
                                        invoice_id=inv_id, application_id=str(r.get("APPLICATIONID") or ""), team_id=str(r.get("TEAMID") or ""),
                                        renewal_date=_opt_str(r.get("RENEWALDATE")), amount=_opt_float(r.get("AMOUNT")) or 0.0,
                                        status=_opt_str(r.get("STATUS")), fiscal_year=int(r.get("FISCAL_YEAR")) if pd.notna(r.get("FISCAL_YEAR")) else None,
                                        product_owner=_opt_str(r.get("PRODUCT_OWNER")), amount_next_year=_opt_float(r.get("AMOUNT_NEXT_YEAR")),
                                        contract_active=bool(r.get("CONTRACT_ACTIVE")) if pd.notna(r.get("CONTRACT_ACTIVE")) else None,
                                        company_code=_opt_str(r.get("COMPANY_CODE")), cost_center=_opt_str(r.get("COST_CENTER")), serial_number=_opt_str(r.get("SERIAL_NUMBER")),
                                        work_order=_opt_str(r.get("WORK_ORDER")), agreement_number=_opt_str(r.get("AGREEMENT_NUMBER")), contract_due=_opt_int(r.get("CONTRACT_DUE")),
                                        service_type=_opt_str(r.get("SERVICE_TYPE")), notes=_opt_str(r.get("NOTES")), group_id=grp,
                                        programid_at_booking=_opt_str(r.get("PROGRAMID_AT_BOOKING")), vendorid_at_booking=_opt_str(r.get("VENDORID_AT_BOOKING")),
                                        groupid_at_booking=grp_book, rollover_batch_id=_opt_str(r.get("ROLLOVER_BATCH_ID")), rolled_over_from_year=_opt_int(r.get("ROLLED_OVER_FROM_YEAR")),
                                        invoice_type=_opt_str(r.get("INVOICE_TYPE")) or 'Recurring Invoice',
                                    ); n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "INVOICE_NOTES":
                                for _, r in dfzip.iterrows():
                                    note_id = str(r.get("NOTE_ID") or uuid.uuid4()); inv=str(r.get("INVOICEID") or ""); txt=str(r.get("NOTE_TEXT") or ""); cby=(str(r.get("CREATED_BY")) if pd.notna(r.get("CREATED_BY")) else None)
                                    execute(
                                        f"MERGE INTO {_fq('INVOICE_NOTES')} t USING (SELECT %s NOTE_ID, %s INVOICEID, %s NOTE_TEXT, %s CREATED_BY) s ON t.NOTE_ID=s.NOTE_ID WHEN MATCHED THEN UPDATE SET NOTE_TEXT=s.NOTE_TEXT, CREATED_BY=s.CREATED_BY WHEN NOT MATCHED THEN INSERT (NOTE_ID, INVOICEID, NOTE_TEXT, CREATED_BY) VALUES (s.NOTE_ID, s.INVOICEID, s.NOTE_TEXT, s.CREATED_BY);",
                                        (note_id, inv, txt, cby),
                                    ); n += 1
                                stats.append((dsname, n - rows_before))
                            elif dsname == "ROLLOVER_LOG":
                                rows = []
                                for _, r in dfzip.iterrows():
                                    batch_id = _zip_clean(r.get("BATCH_ID"))
                                    if not batch_id:
                                        continue
                                    rows.append((
                                        batch_id,
                                        int(r.get("FROM_YEAR")) if pd.notna(r.get("FROM_YEAR")) else None,
                                        int(r.get("TO_YEAR")) if pd.notna(r.get("TO_YEAR")) else None,
                                        int(r.get("ROWS_INSERTED")) if pd.notna(r.get("ROWS_INSERTED")) else None,
                                        (str(r.get("CREATED_BY")) if pd.notna(r.get("CREATED_BY")) else None),
                                    ))
                                if rows:
                                    execute(
                                        f"MERGE INTO {_fq('ROLLOVER_LOG')} t USING (SELECT %s BATCH_ID, %s FROM_YEAR, %s TO_YEAR, %s ROWS_INSERTED, %s CREATED_BY) s ON t.BATCH_ID=s.BATCH_ID WHEN MATCHED THEN UPDATE SET FROM_YEAR=s.FROM_YEAR, TO_YEAR=s.TO_YEAR, ROWS_INSERTED=s.ROWS_INSERTED, CREATED_BY=s.CREATED_BY WHEN NOT MATCHED THEN INSERT (BATCH_ID, FROM_YEAR, TO_YEAR, ROWS_INSERTED, CREATED_BY) VALUES (s.BATCH_ID, s.FROM_YEAR, s.TO_YEAR, s.ROWS_INSERTED, s.CREATED_BY);",
                                        rows,
                                        many=True,
                                    ); n += len(rows)
                                stats.append((dsname, n - rows_before))
                            steps_done += 1
                            prog.progress(min(steps_done/steps_total, 1.0), text=f"{dsname}: imported {n - rows_before} rows")
                            stats.append((dsname, n - rows_before))
                        # Attachments content in zip
                        try:
                            _maybe_replace_dataset("INVOICE_ATTACHMENTS (zip)")
                            man_member = _find_in_zip(zf, 'attachments_manifest.csv')
                            if not man_member:
                                raise FileNotFoundError("attachments_manifest.csv missing")
                            with zf.open(man_member) as fh:
                                man = _pd.read_csv(fh)
                            rows_before = n
                            for _, r in man.iterrows():
                                aid = str(r.get('ATTACHMENT_ID'))
                                inv = str(r.get('INVOICEID') or "")
                                fn = str(r.get('FILENAME') or aid)
                                mt = str(r.get('MIMETYPE') or 'application/octet-stream')
                                data = None
                                for name in (f"files/{aid}_{fn}", f"files/{aid}"):
                                    try: data = zf.read(name); break
                                    except Exception:
                                        # try matching on basename only
                                        member = _find_in_zip(zf, name)
                                        if member:
                                            try:
                                                data = zf.read(member)
                                                break
                                            except Exception:
                                                continue
                                        continue
                                if data is None: continue
                                execute(
                                    f"MERGE INTO {_fq('INVOICE_ATTACHMENTS')} t USING (SELECT %s ATTACHMENT_ID, %s INVOICEID, %s FILENAME, %s MIMETYPE, %s CONTENT) s ON t.ATTACHMENT_ID = s.ATTACHMENT_ID WHEN MATCHED THEN UPDATE SET INVOICEID=s.INVOICEID, FILENAME=s.FILENAME, MIMETYPE=s.MIMETYPE, CONTENT=s.CONTENT, UPLOADED_AT=SYSDATETIME() WHEN NOT MATCHED THEN INSERT (ATTACHMENT_ID, INVOICEID, FILENAME, MIMETYPE, CONTENT) VALUES (s.ATTACHMENT_ID, s.INVOICEID, s.FILENAME, s.MIMETYPE, s.CONTENT);",
                                    (aid, inv, fn, mt, data),
                                ); n += 1
                            stats.append(("INVOICE_ATTACHMENTS", n - rows_before))
                        except Exception:
                            pass
                        steps_done += 1
                        prog.progress(1.0, text="Finished full restore")

                def _run_teamid_integrity_check() -> None:
                    try:
                        missing = fetch_df(
                            """
                            SELECT DISTINCT TOP 20 h.TEAMID
                            FROM TEAM_HEADCOUNT_HISTORY h
                            LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
                            LEFT JOIN PROGRAMS p ON p.PROGRAMID = h.TEAMID
                            WHERE (
                                UPPER(LTRIM(RTRIM(ISNULL(h.CLASS, '')))) = 'PROGRAM'
                                AND p.PROGRAMID IS NULL
                            )
                            OR (
                                UPPER(LTRIM(RTRIM(ISNULL(h.CLASS, '')))) <> 'PROGRAM'
                                AND t.TEAMID IS NULL
                            )
                            """
                        )
                    except Exception as exc:
                        st.warning(f"Integrity check failed: {exc}")
                        return
                    if missing is not None and not missing.empty:
                        ids = [str(v) for v in missing["TEAMID"].tolist() if str(v).strip()]
                        st.error(
                            "Integrity check failed: TEAM_HEADCOUNT_HISTORY contains invalid IDs "
                            "(TEAMID for team rows, PROGRAMID for CLASS='PROGRAM' rows). "
                            f"Missing (first 20): {', '.join(ids[:20])}"
                        )
                        st.stop()

                did_bump = False
                if ds in (
                    "FULL_BACKUP (zip)",
                    "TEAM_HEADCOUNT_HISTORY",
                    "TEAM_RATE_HISTORY",
                    "TEAM_COMPOSITION_HISTORY",
                    "TEAM_CONTRACTOR_HEADCOUNT",
                    "PROGRAM_RATE_HISTORY",
                    "PROGRAM_COMPOSITION_HISTORY",
                    "PROGRAMS",
                    "TEAMS",
                ):
                    _run_teamid_integrity_check()
                    bump_data_version("restore_csv")
                    did_bump = True
                    try:
                        ensure_analytics_views_ok()
                    except Exception as exc:
                        st.error(f"Analytics view rebuild failed: {exc}")
                        st.stop()

                if not did_bump:
                    bump_data_version("restore_csv")
                if ds == "FULL_BACKUP (zip)" and stats:
                    summary = ", ".join([f"{name}: {cnt}" for name, cnt in stats if cnt])
                    st.success(f"Imported {n} row(s) into {ds}. Details → {summary}")
                else:
                    st.success(f"Imported {n} row(s) into {ds}.")
                return {"ok": True, "message": f"Restore completed for {ds}.", "counts": {"rows": n}}
            except Exception as e:
                st.error(f"Import failed: {e}")
                return {"ok": False, "message": f"Restore failed: {e}"}

        def _read_csv_flexible(fh):
            import pandas as _pd

            try:
                df_read = _pd.read_csv(fh)
                if len(df_read.columns) == 1 and "\t" in str(df_read.columns[0]):
                    try:
                        fh.seek(0)
                    except Exception:
                        pass
                    df_read = _pd.read_csv(fh, sep="\t")
                return df_read
            except UnicodeDecodeError:
                try:
                    fh.seek(0)
                except Exception:
                    pass
                df_read = _pd.read_csv(fh, encoding="latin1")
                if len(df_read.columns) == 1 and "\t" in str(df_read.columns[0]):
                    try:
                        fh.seek(0)
                    except Exception:
                        pass
                    df_read = _pd.read_csv(fh, encoding="latin1", sep="\t")
                return df_read

        def _restore_preview() -> Dict[str, Any]:
            if file is None:
                return {"ok": False, "message": "Upload a backup file to preview.", "counts": {}}
            try:
                if ds == "FULL_BACKUP (zip)" or ds.endswith("(zip)"):
                    import zipfile as _zip, io as _io

                    zbytes = file.getvalue() if hasattr(file, "getvalue") else file.read()
                    with _zip.ZipFile(_io.BytesIO(zbytes), "r") as zf:
                        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                        sample_df = None
                        if members:
                            with zf.open(members[0]) as fh:
                                sample_df = _read_csv_flexible(fh).head(20)
                        return {
                            "ok": True,
                            "message": "Preview loaded from backup zip.",
                            "counts": {"csv_files": len(members)},
                            "sample": sample_df,
                        }
                import io as _io

                raw = file.getvalue() if hasattr(file, "getvalue") else file.read()
                df_all = _read_csv_flexible(_io.BytesIO(raw))
                df_prev = df_all.head(20)
                total_rows = len(df_all)
                return {
                    "ok": True,
                    "message": "Preview loaded from dataset file.",
                    "counts": {"rows": total_rows},
                    "sample": df_prev,
                }
            except Exception as e:
                return {"ok": False, "message": f"Preview failed: {e}", "counts": {}}

        def _restore_apply() -> Dict[str, Any]:
            if file is None:
                return {"ok": False, "message": "No file uploaded."}
            if restore_write_mode == "Replace (truncate + insert)" and not replace_confirm_ok:
                return {"ok": False, "message": "Replace confirmation missing. Type REPLACE to continue."}
            return _run_restore()

        render_preview_apply_audit_block(
            title="Restore data",
            area="backup_restore",
            action_type="restore",
            entity=ds,
            context={
                "mode": restore_mode,
                "write_mode": restore_write_mode,
                "dataset": ds,
                "file": getattr(file, "name", None),
            },
            preview_fn=_restore_preview,
            apply_fn=_restore_apply,
            require_typed_confirm=True,
            confirm_phrase="RESTORE",
            danger_level="danger",
        )


    render_section("Post-restore validation & audit")
    if st.button("Run validation"):
        results = _run_post_restore_validation()
        st.session_state["admin_restore_validation"] = results
        log_admin_action(
            action_type="post_restore_validation",
            area="backup_restore",
            entity="system",
            summary="Ran post-restore validation.",
            extra_json={"results": results},
        )
    results = st.session_state.get("admin_restore_validation") or []
    if results:
        for r in results:
            status = r.get("status")
            if status == "ok":
                st.success(f"✅ {r.get('check')}: {r.get('message')}")
            elif status == "warn":
                st.warning(f"⚠️ {r.get('check')}: {r.get('message')}")
            else:
                st.error(f"❌ {r.get('check')}: {r.get('message')}")
