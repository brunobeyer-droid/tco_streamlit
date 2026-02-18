# Summary: update mapping UI labels and template filenames to NEXT branding.
import io
import uuid
import time
from datetime import date
import re
from typing import Optional, List, Dict, Tuple, Any

import pandas as pd
import streamlit as st
from core import bulkload_v2
from core.ado_recon import load_explorer_feature_rows
from core.ado_profile import (

        return None



    get_active_ado_profile,
    build_feature_query_from_profile,
    extract_program_team,
)
from core.data import dominant_iteration_root, list_iteration_roots
from core.nwf_taxonomy import PLANNED_NWF_SUBCOMPONENTS, normalize_nwf_subcomponent
from utils.theme import use_theme
from utils.reconciliation import render_reconciliation_tab
import base64
import json
import math
import sys
import os
from pathlib import Path
from urllib.parse import urlparse, quote
from utils.ado import (
    fetch_ado_odata,
    fetch_ado_iterations_odata,
    transform_ado_odata_to_expected,
    normalize_to_canonical,
)

try:
    import requests
except Exception:  # handled at runtime if missing
    requests = None  # type: ignore

from db import (
    execute,
    fetch_df,
    ensure_ado_minimal_tables,
    ensure_ado_iteration_calendar_table,
    ensure_tables,
    ensure_apptio_actuals_lines_table,
    ensure_map_apptio_to_cost_type_table,
    ensure_tco_feature_demand_view,
    fetch_ado_profiles,
    fetch_ado_profiles_full,
    set_active_ado_profile,
    upsert_ado_profile,
    delete_ado_profile,
    load_ado_portfolio_settings,
    save_ado_portfolio_settings,
    delete_ado_portfolio_settings,

    # Lookups + upserts (align with your schema)
    list_programs,
    list_teams,
    list_vendors,
    list_application_groups,
    list_applications,
    upsert_program,
    upsert_team,
    upsert_vendor,
    upsert_application_group,
    upsert_application_instance,
    upsert_invoice,
    upsert_ado_features,
    upsert_ado_iteration_calendar,
    ensure_contracts_table,
    upsert_contract,
    list_program_apptio_workids,
    upsert_apptio_actuals,
    upsert_apptio_actuals_lines,
    upsert_apptio_to_cost_type_mappings,
    fetch_apptio_actuals_by_program_breakdown,
)
from utils.toast import toast_error, toast_success

# Master-data utilities (no cost math; helps mapping and default instance behavior)
from core.ado_candidates import refresh_ado_app_candidates
from core.app_instances import ensure_default_instances

# Auth guard: contributors only
try:
    from utils.auth import require_role, ensure_sso
except Exception:
    st.error("Authentication is not available. Contact an admin.")
    st.stop()
# -------------------------
# Page setup
# -------------------------
st.set_page_config(page_title="Settings", layout="wide")
_page_theme = use_theme(render_toggle=False)

_ = ensure_sso("ADMIN", page_name="Settings", page_path=__file__, render_sidebar=False)
st.title("Settings")
st.caption(
    "Maintain mappings, rates, and reference data"
)

# Ensure minimal schema is ready (non-fatal if creation fails so UI still loads)
st.session_state.setdefault("_settings_schema_checked", False)
_SETTINGS_SCHEMA_VERSION = 2
if st.session_state.get("_settings_schema_version") != _SETTINGS_SCHEMA_VERSION:
    st.session_state["_settings_schema_checked"] = False
    st.session_state["_settings_schema_version"] = _SETTINGS_SCHEMA_VERSION
if st.button("Re-run schema checks", type="secondary", key="settings_schema_rerun_btn"):
    st.session_state["_settings_schema_checked"] = False
    st.rerun()

if not st.session_state.get("_settings_schema_checked"):
    with st.spinner("Ensuring minimal ADO schema..."):
        try:
            ensure_ado_minimal_tables()
        except Exception as e:
            st.warning(f"ADO base tables check failed: {e}")
        try:
            if callable(ensure_tco_feature_demand_view):
                ensure_tco_feature_demand_view()
        except Exception as e:
            st.warning(f"View build (feature demand) failed: {e}")
    st.session_state["_settings_schema_checked"] = True

# -------------------------
# Session state
# -------------------------
for state_key, default in [
    ("ado_parsed_raw", None),
    ("ado_parsed_norm", None),
    ("ado_query_mode", "Advanced OData Builder"),
    ("_ado_active_portfolio_key", None),
    ("ado_portfolio_settings_loaded", False),
    ("one_sheet_df", None),
    ("colmap", {}),
    ("previews", {}),
]:
    if state_key not in st.session_state:
        st.session_state[state_key] = default

if not st.session_state.get("ado_portfolio_settings_loaded"):
    try:
        st.session_state["ado_portfolio_settings"] = load_ado_portfolio_settings()
    except Exception:
        st.session_state["ado_portfolio_settings"] = {}
    st.session_state["ado_portfolio_settings_loaded"] = True

# -------------------------
# Column expectations (ADO)
# -------------------------
EXPECTED = {
    "Team": ["Team", "System.Team", "Area Team"],
    "Custom_ApplicationName": ["Custom_ApplicationName", "Application", "App Name"],
    "Custom_InvestmentDimension": ["Custom_InvestmentDimension", "Investment Dimension", "INVESTMENT_DIMENSION"],  # NEW
    "BusinessValue": ["BusinessValue", "Business Value", "Microsoft.VSTS.Common.BusinessValue", "Microsoft_VSTS_Common_BusinessValue"],
    "Iteration": ["Iteration", "Iteration Path", "System.IterationPath", "Iteration.IterationLevel3.2"],
    "Title": ["Title", "System.Title"],
    "State": ["State", "System.State"],
    "ID": ["ID", "Work Item ID", "System.Id", "WorkItemId", "Work Item Id"],
    "CreatedDate": ["Created Date", "System.CreatedDate", "CreatedDate"],
    "ChangedDate": ["Changed Date", "System.ChangedDate", "ChangedDate"],
    "Year": ["Year", "ADO Year", "ADO_YEAR"],
    "AreaPath": ["AreaPath", "Area.Path", "Area.AreaPath"],
    "AreaLevel2": ["AreaLevel2", "Area.Level2", "Area.AreaLevel2", "Team Area Level2"],
    "AreaLevel3": ["AreaLevel3", "Area.Level3", "Area.AreaLevel3"],
    "AreaLevel4": ["AreaLevel4", "Area.Level4", "Area.AreaLevel4"],
}

# -------------------------
# Utilities
# -------------------------
def _table_count(table: str) -> int:
    try:
        df = fetch_df(f"SELECT COUNT(*) AS N FROM {table}")
        return int(df.iloc[0]["N"]) if df is not None and not df.empty else 0
    except Exception:
        return 0

def _get_preview(name: str) -> pd.DataFrame:
    obj = st.session_state.get("previews", {}).get(name)
    return obj if isinstance(obj, pd.DataFrame) else pd.DataFrame()

def _blank_or_nan(s) -> bool:
    """True for None, '', 'nan' (string), and values that stringify to blank."""
    if s is None:
        return True
    try:
        txt = str(s).strip()
    except Exception:
        return True
    return (txt == "") or (txt.lower() == "nan")

@st.cache_data(ttl=180, show_spinner=False)
def _ado_feature_columns() -> set:
    df = fetch_df(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'ADO_FEATURES'
        """
    )
    if df is None or df.empty:
        return set()
    return {str(c).strip().upper() for c in df["COLUMN_NAME"].tolist()}

def _ado_org_proj_defaults() -> Tuple[str, str]:
    """Best-effort org/project resolution from session or secrets for ADO links."""
    try:
        _ado_cfg = getattr(st, "secrets", {}).get("ado", {})  # type: ignore
    except Exception:
        _ado_cfg = {}
    org = (st.session_state.get("ado_org") or _ado_cfg.get("org") or "").strip()
    proj = (st.session_state.get("ado_proj") or _ado_cfg.get("project") or "").strip()
    return org, proj

def _ado_query_url(org: str, proj: str, feature_ids: List[Any]) -> str:
    """Build a WIQL query URL to list the given feature IDs in ADO."""
    if not org or not proj or not feature_ids:
        return ""
    ids = [str(int(float(i))) for i in feature_ids if pd.notna(i)]
    if not ids:
        return ""
    ids = ids[:200]  # keep URL reasonable
    wiql = f"Select [System.Id], [System.Title] From WorkItems Where [System.Id] In ({','.join(ids)})"
    return f"https://dev.azure.com/{org}/{proj}/_workitems/query?wiql={quote(wiql)}"


def _load_ado_profiles() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    profiles_base = None
    profiles_full = None
    try:
        profiles_base = fetch_ado_profiles()
    except Exception:
        profiles_base = None
    try:
        profiles_full = fetch_ado_profiles_full()
    except Exception as e:
        profiles_full = None
        st.warning(f"Could not load ADO profiles: {e}")
    return profiles_base, profiles_full


def _ado_profile_context(
    profiles_base: Optional[pd.DataFrame],
    profiles_full: Optional[pd.DataFrame],
) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        "profiles_full": None,
        "profiles_base": None,
        "selected_row": None,
        "selected_id": "",
        "selected_name": "",
        "is_active": False,
        "active_name": "",
        "config_dict": {},
        "parse_error": None,
    }
    ctx["profiles_base"] = profiles_base
    ctx["profiles_full"] = profiles_full
    if profiles_full is None or profiles_full.empty:
        return ctx
    profiles_full = profiles_full.copy()
    profiles_full["PROFILE_NAME"] = profiles_full["PROFILE_NAME"].astype(str)
    ctx["profiles_full"] = profiles_full
    profile_names = profiles_full["PROFILE_NAME"].tolist()
    active_name = ""
    try:
        active_name = profiles_full.loc[profiles_full["IS_ACTIVE"].astype(int) == 1, "PROFILE_NAME"].iloc[0]
    except Exception:
        try:
            if profiles_base is not None and not profiles_base.empty:
                active_name = profiles_base.loc[
                    profiles_base["IS_ACTIVE"].astype(int) == 1, "PROFILE_NAME"
                ].iloc[0]
        except Exception:
            active_name = ""
    ctx["active_name"] = active_name
    sel_profile = st.selectbox(
        "Profile",
        profile_names,
        index=profile_names.index(active_name) if active_name in profile_names else 0,
        key="ado_profile_select",
    )
    selected_row = profiles_full.loc[profiles_full["PROFILE_NAME"] == sel_profile].iloc[0]
    ctx["selected_row"] = selected_row
    ctx["selected_id"] = str(selected_row.get("PROFILE_ID"))
    ctx["selected_name"] = str(selected_row.get("PROFILE_NAME"))
    ctx["is_active"] = bool(int(selected_row.get("IS_ACTIVE", 0)))

    overrides = st.session_state.setdefault("ado_profile_config_overrides", {})
    raw_json = selected_row.get("CONFIG_JSON") or ""
    parse_error = None
    try:
        parsed_config = json.loads(raw_json) if isinstance(raw_json, str) and raw_json.strip() else {}
        if not isinstance(parsed_config, dict):
            parsed_config = {}
    except Exception as e:
        parse_error = str(e)
        parsed_config = {}
    config_dict = overrides.get(ctx["selected_id"])
    if not isinstance(config_dict, dict):
        config_dict = parsed_config
        overrides[ctx["selected_id"]] = config_dict
    ctx["config_dict"] = config_dict
    ctx["parse_error"] = parse_error
    return ctx


def _render_ado_active_profile_header(profiles_full: Optional[pd.DataFrame]) -> None:
    if profiles_full is None or profiles_full.empty:
        return
    active_row = None
    try:
        active_row = profiles_full.loc[profiles_full["IS_ACTIVE"].astype(int) == 1].iloc[0]
    except Exception:
        active_row = None
    if active_row is None:
        return
    base_url = ""
    updated_at = active_row.get("UPDATED_AT")
    try:
        payload = active_row.get("CONFIG_JSON")
        payload_obj = json.loads(payload) if isinstance(payload, str) and payload.strip() else {}
        if isinstance(payload_obj, dict):
            base_url = str(payload_obj.get("odata_base_url") or "").strip()
    except Exception:
        base_url = ""
    with st.container(border=True):
        col_a, col_b, col_c = st.columns([2, 3, 2])
        col_a.markdown(f"**Active Profile**: {active_row.get('PROFILE_NAME')}")
        col_b.markdown(f"**Base URL**: {base_url[:80] + ('…' if len(base_url) > 80 else '')}")
        col_c.markdown(f"**Last Updated**: {updated_at if updated_at is not None else '—'}")


# === ADO: Profiles Section (moved from former “Sync & Import”) ===
def render_ado_profiles_section(ctx: Dict[str, Any]) -> None:
    profiles_full = ctx.get("profiles_full")
    if profiles_full is None or profiles_full.empty:
        st.info("No ADO profiles found yet. Run schema checks to seed the default profile.")
        return
    selected_row = ctx.get("selected_row")
    selected_id = ctx.get("selected_id", "")
    selected_name = ctx.get("selected_name", "")
    is_active = ctx.get("is_active", False)
    active_name = ctx.get("active_name", "")

    st.markdown("### ADO Profiles")
    st.caption(f"Active profile: {active_name or 'None'}")

    left, right = st.columns([3, 2])
    with left:
        with st.container(border=True):
            st.markdown("**Profile Actions**")
            col_act, col_dup, col_exp = st.columns(3)
            with col_act:
                if st.button("Set Active", disabled=is_active, key="ado_profile_set_active_btn"):
                    try:
                        set_active_ado_profile(selected_id)
                        st.success("Active ADO profile updated.")
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Could not set active profile: {e}")
            with col_dup:
                if st.button("Duplicate", key="ado_profile_duplicate_btn"):
                    try:
                        updated_by = st.session_state.get("auth_user", {}).get("email") or "ADMIN"
                        config_json = selected_row.get("CONFIG_JSON")
                        new_name = f"{selected_name} (copy)"
                        upsert_ado_profile(None, new_name, False, config_json, updated_by)
                        st.success("Profile duplicated.")
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Could not duplicate profile: {e}")
            with col_exp:
                try:
                    payload = selected_row.get("CONFIG_JSON")
                    payload_obj = json.loads(payload) if isinstance(payload, str) and payload.strip() else {}
                    payload_text = json.dumps(payload_obj, indent=2)
                except Exception:
                    payload_text = "{}"
                export_name = selected_name.strip().replace(" ", "_") or "ado_profile"
                st.download_button(
                    "Export JSON",
                    data=payload_text,
                    file_name=f"{export_name}.json",
                    mime="application/json",
                    key="ado_profile_export_btn",
                )

        with st.container(border=True):
            st.markdown("**Import Profile JSON**")
            import_file = st.file_uploader(
                "Import profile JSON",
                type=["json"],
                key="ado_profile_import_file",
            )
            import_payload: Optional[Dict[str, Any]] = None
            import_error = None
            if import_file is not None:
                try:
                    raw_text = import_file.read().decode("utf-8")
                    import_payload = json.loads(raw_text)
                    if not isinstance(import_payload, dict):
                        raise ValueError("JSON must be an object at the top level.")
                except Exception as e:
                    import_error = str(e)
                    import_payload = None

                if import_error:
                    st.error(f"Could not parse JSON: {import_error}")
                else:
                    required_base = bool(import_payload.get("odata_base_url"))
                    required_feature = bool(
                        isinstance(import_payload.get("work_item_types"), dict)
                        and import_payload.get("work_item_types", {}).get("feature")
                    )
                    if not (required_base and required_feature):
                        st.error("JSON must include odata_base_url and work_item_types.feature.")
                    else:
                        default_name = os.path.splitext(import_file.name or "")[0] or "Imported Profile"
                        import_name = st.text_input(
                            "Profile name",
                            value=default_name,
                            key="ado_profile_import_name",
                        ).strip()
                        if st.button("Create Profile", key="ado_profile_import_btn", disabled=not import_name):
                            try:
                                updated_by = st.session_state.get("auth_user", {}).get("email") or "ADMIN"
                                payload_text = json.dumps(import_payload, indent=2, sort_keys=True)
                                upsert_ado_profile(None, import_name, False, payload_text, updated_by)
                                st.success("Profile imported.")
                                st.experimental_rerun()
                            except Exception as e:
                                st.error(f"Could not import profile: {e}")

    with right:
        with st.container(border=True):
            st.markdown("**Profile Summary**")
            st.write(f"Name: {selected_name}")
            st.write(f"Active: {'Yes' if is_active else 'No'}")
            st.write(f"Updated: {selected_row.get('UPDATED_AT') or '—'}")
            try:
                payload = selected_row.get("CONFIG_JSON")
                payload_obj = json.loads(payload) if isinstance(payload, str) and payload.strip() else {}
                base_url = payload_obj.get("odata_base_url") if isinstance(payload_obj, dict) else ""
            except Exception:
                base_url = ""
            st.write(f"Base URL: {str(base_url)[:80] + ('…' if base_url and len(str(base_url)) > 80 else '')}")

        if st.button("Edit Profile", key="ado_profile_edit_toggle_btn"):
            st.session_state["ado_profile_editor_open"] = not st.session_state.get("ado_profile_editor_open", False)

    if st.session_state.get("ado_profile_editor_open"):
        render_ado_profile_editor(ctx)

    with st.expander("Danger zone", expanded=False):
        profile_names = profiles_full["PROFILE_NAME"].tolist()
        if (not is_active) and len(profile_names) > 1:
            confirm = st.checkbox("I understand this will delete the selected profile.", key="ado_profile_delete_confirm")
            if st.button("Delete profile", disabled=not confirm, key="ado_profile_delete_btn"):
                try:
                    delete_ado_profile(selected_id)
                    st.success("Profile deleted.")
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"Could not delete profile: {e}")
        else:
            st.caption("Active profile cannot be deleted.")


# === ADO: Profile Editor Section ===
def render_ado_profile_editor(ctx: Dict[str, Any]) -> None:
    selected_id = ctx.get("selected_id", "")
    selected_name = ctx.get("selected_name", "")
    is_active = ctx.get("is_active", False)
    config_dict = ctx.get("config_dict", {})
    parse_error = ctx.get("parse_error")
    raw_json = ctx.get("selected_row").get("CONFIG_JSON") if ctx.get("selected_row") is not None else ""
    overrides = st.session_state.setdefault("ado_profile_config_overrides", {})

    st.markdown("### Profile Editor")
    if parse_error:
        st.error(f"Profile JSON could not be parsed ({parse_error}). Fix it in Advanced JSON editor.")

    def _cfg(path: str, default: Any) -> Any:
        cur = config_dict
        for part in path.split("."):
            if not isinstance(cur, dict):
                return default
            cur = cur.get(part)
        return default if cur is None else cur

    with st.form(key=f"ado_profile_form_{selected_id}"):
        st.markdown("#### Connection")
        odata_base_url = st.text_input(
            "OData base URL",
            value=str(_cfg("odata_base_url", "")),
        ).strip()

        st.markdown("#### Work item types")
        feature_type = st.text_input(
            "Feature type",
            value=str(_cfg("work_item_types.feature", "Feature")),
        ).strip()
        epic_type = st.text_input(
            "Epic type",
            value=str(_cfg("work_item_types.epic", "Epic")),
        ).strip()

        st.markdown("#### Area mapping")
        area_options = ["AreaLevel2", "AreaLevel3", "AreaLevel4"]
        program_level = st.selectbox(
            "Program level",
            options=area_options,
            index=area_options.index(_cfg("area_mapping.program_level", "AreaLevel2"))
            if _cfg("area_mapping.program_level", "AreaLevel2") in area_options
            else 0,
        )
        team_level = st.selectbox(
            "Team level",
            options=area_options,
            index=area_options.index(_cfg("area_mapping.team_level", "AreaLevel3"))
            if _cfg("area_mapping.team_level", "AreaLevel3") in area_options
            else 1,
        )

        st.markdown("#### Iteration mapping")
        pi_options = ["IterationLevel1", "IterationLevel2", "IterationLevel3", "IterationPath"]
        pi_field = st.selectbox(
            "PI field",
            options=pi_options,
            index=pi_options.index(_cfg("iteration_mapping.pi_field", "IterationLevel3"))
            if _cfg("iteration_mapping.pi_field", "IterationLevel3") in pi_options
            else 2,
        )
        path_options = ["IterationPath", "IterationLevel3"]
        path_field = st.selectbox(
            "Path field",
            options=path_options,
            index=path_options.index(_cfg("iteration_mapping.path_field", "IterationPath"))
            if _cfg("iteration_mapping.path_field", "IterationPath") in path_options
            else 0,
        )

        st.markdown("#### Fields")
        app_name_field = st.text_input(
            "App name field",
            value=str(_cfg("fields.app_name", "Custom_ApplicationName")),
        ).strip()
        points_field = st.text_input(
            "Points field",
            value=str(_cfg("fields.points", "Effort")),
        ).strip()
        fte_field = st.text_input(
            "FTE field",
            value=str(_cfg("fields.fte", "Custom_FTE")),
        ).strip()
        investment_field = st.text_input(
            "Investment dimension field",
            value=str(_cfg("fields.investment_dimension", "Custom_InvestmentDimension")),
        ).strip()

        st.markdown("#### SWAG")
        points_per_fte = st.number_input(
            "Points per FTE",
            value=float(_cfg("swag.points_per_fte", 65.0)),
            step=1.0,
        )

        st.markdown("#### Filters")
        exclude_defaults = [
            "Removed",
            "Closed",
            "Done",
            "Resolved",
            "Completed",
            "In Progress",
            "New",
        ]
        existing_excludes = _cfg("filters.exclude_states", [])
        if not isinstance(existing_excludes, list):
            existing_excludes = []
        exclude_options = sorted(set(exclude_defaults + [str(x) for x in existing_excludes if x]))
        exclude_states = st.multiselect(
            "Exclude states",
            options=exclude_options,
            default=existing_excludes or ["Removed"],
        )
        years_prefix_raw = st.text_input(
            "Years prefix (comma-separated)",
            value=",".join([str(x) for x in _cfg("filters.years_prefix", []) if x]),
        ).strip()

        st.markdown("#### Epic resolution")
        max_depth = st.number_input(
            "Max depth",
            min_value=1,
            max_value=20,
            value=int(_cfg("epic_resolution.max_depth", 6)),
            step=1,
        )

        make_active = st.checkbox(
            "Set as active profile",
            value=is_active,
            help="Only updates active status if you change this value.",
        )
        save_clicked = st.form_submit_button("Save profile")

    if save_clicked:
        if not odata_base_url:
            st.error("OData base URL is required.")
        elif not feature_type:
            st.error("Feature type is required.")
        else:
            years_prefix = [x.strip() for x in years_prefix_raw.split(",") if x.strip()]
            updated_config = dict(config_dict) if isinstance(config_dict, dict) else {}
            updated_config["odata_base_url"] = odata_base_url
            updated_config["work_item_types"] = {"feature": feature_type, "epic": epic_type}
            updated_config["area_mapping"] = {"program_level": program_level, "team_level": team_level}
            updated_config["iteration_mapping"] = {"pi_field": pi_field, "path_field": path_field}
            updated_config["fields"] = {
                "app_name": app_name_field,
                "points": points_field,
                "fte": fte_field,
                "investment_dimension": investment_field,
            }
            updated_config["swag"] = {"points_per_fte": float(points_per_fte)}
            updated_config["filters"] = {
                "exclude_states": exclude_states,
                "years_prefix": years_prefix,
            }
            updated_config["epic_resolution"] = {"max_depth": int(max_depth)}
            updated_by = st.session_state.get("auth_user", {}).get("email") or "ADMIN"
            payload = json.dumps(updated_config, indent=2, sort_keys=True)
            try:
                upsert_ado_profile(selected_id, selected_name, bool(make_active), payload, updated_by)
                overrides[selected_id] = updated_config
                st.success("Profile saved.")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Could not save profile: {e}")

    with st.expander("Advanced JSON editor", expanded=False):
        json_key = f"ado_profile_json_text_{selected_id}"
        default_json_text = raw_json if parse_error else json.dumps(config_dict, indent=2, sort_keys=True)
        json_text = st.text_area(
            "Profile JSON",
            value=st.session_state.get(json_key, default_json_text),
            height=240,
            key=json_key,
        )
        if st.button("Apply JSON", key=f"ado_profile_apply_json_{selected_id}"):
            try:
                parsed = json.loads(json_text or "{}")
                if not isinstance(parsed, dict):
                    raise ValueError("JSON must be an object at the top level.")
                overrides[selected_id] = parsed
                st.success("JSON applied to form.")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Invalid JSON: {e}")


# === ADO: Validation Section ===
def render_ado_validate_section(ctx: Dict[str, Any]) -> None:
    selected_id = ctx.get("selected_id", "")
    config_dict = ctx.get("config_dict", {})
    st.markdown("### Validation / Preview")
    st.warning(
        "Preview uses ParentWorkItemId only; Parent expand is intentionally avoided due to OData warnings.",
    )
    org_preview, proj_preview = _ado_org_proj_defaults()
    entity_preview = st.session_state.get("ado_entity") or "WorkItems"
    profile_for_preview = config_dict if isinstance(config_dict, dict) else {}
    preview_debug = build_feature_query_from_profile(
        org_preview or "",
        proj_preview or "",
        entity_preview,
        profile_for_preview,
    )
    base_url = preview_debug.get("final_url") or ""
    preview_url = ""
    if base_url:
        sep = "&" if "?" in base_url else "?"
        preview_url = f"{base_url}{sep}$top=3"
    url_key = f"ado_profile_preview_url_{selected_id}"
    if preview_url:
        st.session_state[url_key] = preview_url
    if st.button("Build Preview URL", key=f"ado_profile_build_preview_{selected_id}", disabled=not preview_url):
        st.session_state[url_key] = preview_url
    st.code(st.session_state.get(url_key) or "(missing org/project)", language="text")

    if st.button("Run Preview", key=f"ado_profile_run_preview_{selected_id}"):
        pat_preview = st.session_state.get("ado_pat") or ""
        url_to_use = st.session_state.get(url_key) or preview_url
        if not url_to_use:
            st.error("Preview URL is not available (missing org/project).")
        elif not pat_preview:
            st.error("Personal Access Token (PAT) is required for preview.")
        else:
            diag_api: Dict[str, Any] = {}
            raw_rows: List[Dict[str, Any]] = []
            try:
                df_preview = fetch_ado_odata(url_to_use, pat_preview, diag_api, max_pages=1)
                if requests is not None:
                    headers = {"Accept": "application/json;odata.metadata=none"}
                    resp = requests.get(url_to_use, headers=headers, auth=("", pat_preview), timeout=30, verify=False)
                    if resp.ok:
                        payload = resp.json()
                        if isinstance(payload, dict) and isinstance(payload.get("value"), list):
                            raw_rows = payload.get("value") or []
                if not raw_rows and df_preview is not None and not df_preview.empty:
                    raw_rows = [df_preview.iloc[0].to_dict()]
                if raw_rows:
                    st.json(raw_rows[0])
                else:
                    st.warning("Preview returned no rows.")

                if raw_rows:
                    first_row = raw_rows[0]
                    points_field = str(profile_for_preview.get("fields", {}).get("points") or "Effort")
                    app_field = str(profile_for_preview.get("fields", {}).get("app_name") or "Custom_ApplicationName")
                    summary_rows = [
                        {"check": "Iteration present", "value": "Iteration" in first_row},
                        {"check": "Area present", "value": "Area" in first_row},
                        {"check": "ParentWorkItemId present", "value": "ParentWorkItemId" in first_row},
                        {"check": f"Points field present ({points_field})", "value": points_field in first_row},
                        {"check": f"App field present ({app_field})", "value": app_field in first_row},
                    ]
                    st.table(pd.DataFrame(summary_rows))

                    def _has_value(val: Any) -> bool:
                        if val is None:
                            return False
                        if isinstance(val, str) and not val.strip():
                            return False
                        return True

                    total = len(raw_rows)
                    parent_pct = sum(1 for r in raw_rows if _has_value(r.get("ParentWorkItemId"))) / total
                    points_pct = sum(1 for r in raw_rows if _has_value(r.get(points_field))) / total
                    app_pct = sum(1 for r in raw_rows if _has_value(r.get(app_field))) / total
                    example_area = raw_rows[0].get("Area") if raw_rows else None
                    example_program = ""
                    example_team = ""
                    if isinstance(example_area, dict):
                        example_program, example_team = extract_program_team(profile_for_preview, example_area)
                    k1, k2, k3 = st.columns(3)
                    k1.metric("% rows with ParentWorkItemId", f"{parent_pct * 100:.0f}%")
                    k2.metric("% rows with points", f"{points_pct * 100:.0f}%")
                    k3.metric("% rows with app name", f"{app_pct * 100:.0f}%")
                    st.caption(
                        f"Example program/team from Area: {example_program or '(none)'} / {example_team or '(none)'}"
                    )
            except Exception as e:
                st.error(f"Preview failed: {e}")
                if requests is not None:
                    try:
                        headers = {"Accept": "application/json;odata.metadata=none"}
                        resp = requests.get(url_to_use, headers=headers, auth=("", pat_preview), timeout=30, verify=False)
                        st.error(f"HTTP {resp.status_code}")
                        st.code(resp.text[:400], language="text")
                    except Exception:
                        pass
                st.info("Hints: check base URL, project name, and PAT permissions for Analytics.")

    if st.button("Discover Fields", key=f"ado_profile_discover_fields_{selected_id}"):
        pat_preview = st.session_state.get("ado_pat") or ""
        if not (org_preview and proj_preview):
            st.error("Organization and Project are required to discover fields.")
        elif not pat_preview:
            st.error("Personal Access Token (PAT) is required to discover fields.")
        else:
            try:
                safe_select = "WorkItemId,Title,WorkItemType,ChangedDate"
                expand_clause = "Iteration($select=IterationLevel3,IterationPath,IterationSK),Area($select=AreaLevel2,AreaLevel3,AreaLevel4)"
                discover_url = _build_ado_odata_url(
                    org_preview,
                    proj_preview,
                    entity_preview,
                    select=safe_select,
                    expand=expand_clause,
                    filter_=None,
                )
                sep = "&" if "?" in discover_url else "?"
                discover_url = f"{discover_url}{sep}$top=1"
                st.code(discover_url, language="text")
                diag_discover: Dict[str, Any] = {}
                df_discover = fetch_ado_odata(discover_url, pat_preview, diag_discover, max_pages=1)
                row = None
                if df_discover is not None and not df_discover.empty:
                    row = df_discover.iloc[0].to_dict()
                if not row:
                    st.warning("No rows returned for field discovery.")
                else:
                    top_keys = sorted(list(row.keys()))
                    iter_keys = sorted(list((row.get("Iteration") or {}).keys())) if isinstance(row.get("Iteration"), dict) else []
                    area_keys = sorted(list((row.get("Area") or {}).keys())) if isinstance(row.get("Area"), dict) else []
                    st.json(
                        {
                            "top_level_keys": top_keys,
                            "iteration_keys": iter_keys,
                            "area_keys": area_keys,
                        }
                    )
                    points_field = str(profile_for_preview.get("fields", {}).get("points") or "Effort")
                    app_field = str(profile_for_preview.get("fields", {}).get("app_name") or "Custom_ApplicationName")
                    points_present = points_field in row and row.get(points_field) not in (None, "")
                    app_present = app_field in row and row.get(app_field) not in (None, "")
                    st.caption(f"Profile points field `{points_field}` present: {'✅' if points_present else '❌'}")
                    st.caption(f"Profile app field `{app_field}` present: {'✅' if app_present else '❌'}")

                    custom_fields = [k for k in top_keys if "CUSTOM_" in k.upper()]
                    if custom_fields:
                        st.caption("Custom fields detected:")
                        st.write(", ".join(custom_fields))

                    effort_like = [k for k in top_keys if "EFFORT" in k.upper() or "STORYPOINT" in k.upper()]
                    if not points_present and effort_like:
                        st.info(
                            "Points field appears missing or empty. Consider checking these fields: "
                            + ", ".join(effort_like)
                        )
            except Exception as e:
                st.error(f"Field discovery failed: {e}")

# -------------------------
# ADO OData helpers (direct sync)
# -------------------------
def _build_ado_odata_url(
    org: str,
    project: str,
    entity: str = "WorkItems",
    select: Optional[str] = None,
    expand: Optional[str] = None,
    filter_: Optional[str] = None,
    orderby: Optional[str] = None,
) -> str:
    base = f"https://analytics.dev.azure.com/{org}/{project}/_odata/v3.0-preview/{entity}"
    qs: List[str] = []
    if select:
        qs.append(f"$select={select}")
    if expand:
        qs.append(f"$expand={expand}")
    if filter_:
        qs.append(f"$filter={filter_}")
    if orderby:
        qs.append(f"$orderby={orderby}")
    return base + ("?" + "&".join(qs) if qs else "")


## fetch_ado_odata provided by utils.ado


## transform_ado_odata_to_expected provided by utils.ado

# -------------------------
# File parsing helpers
# -------------------------
def _auto_header_index(df_no_header: pd.DataFrame, expected_samples: List[str], max_scan: int = 20) -> Optional[int]:
    for i in range(min(max_scan, len(df_no_header))):
        row_vals = df_no_header.iloc[i].astype(str).str.strip().str.lower().tolist()
        hits = sum(1 for e in expected_samples if e.lower() in row_vals)
        if hits >= 2:
            return i
    return None

def _list_excel_sheets(data: bytes) -> List[str]:
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        return list(wb.sheetnames)
    except Exception:
        pass
    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=data)
        return wb.sheet_names()
    except Exception:
        pass
    try:
        from pyxlsb import open_workbook
        with open_workbook(fileobj=io.BytesIO(data)) as wb:
            return [s.name for s in wb.sheets]
    except Exception:
        pass
    return []

def _read_excel_any(data: bytes, sheet_name: Optional[str], diag: Dict[str, Any]) -> Optional[pd.DataFrame]:
    errors: List[str] = []
    for eng in ("openpyxl", "xlrd", "pyxlsb"):
        try:
            __import__(eng)
            df = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0), engine=eng)
            diag.setdefault("excel_engines_used", []).append(eng)
            return df
        except ModuleNotFoundError as e:
            errors.append(f"{eng} not installed: {e}")
        except Exception as e:
            errors.append(f"{eng} failed: {e}")
    try:
        df = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0))
        diag.setdefault("excel_engines_used", []).append("auto")
        return df
    except Exception as e:
        errors.append(f"pandas auto engine failed: {e}")
    try:
        df_raw = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0), header=None)
        hi = _auto_header_index(df_raw, expected_samples=["Title", "ID", "Team", "Iteration", "State", "Business Value", "BusinessValue"])
        if hi is not None:
            df = pd.read_excel(io.BytesIO(data), sheet_name=(sheet_name or 0), header=hi)
            diag.setdefault("header_autodetected", True)
            return df
        errors.append("Header auto-detect failed.")
    except Exception as e:
        errors.append(f"header=None strategy failed: {e}")

    diag["excel_errors"] = errors
    return None

def _read_csv_any(data: bytes, diag: Dict[str, Any]) -> Optional[pd.DataFrame]:
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1", "utf-16", "utf-16le", "utf-16be"]
    seps: List[Optional[str]] = [",", ";", "\t", None]
    errors: List[str] = []
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(io.BytesIO(data), encoding=enc, sep=sep, engine="python")
                if df.shape[1] >= 2:
                    diag.setdefault("csv_attempts", []).append({"encoding": enc, "sep": sep or "auto"})
                    return df
            except Exception as e:
                errors.append(f"csv {enc}/{sep or 'auto'} failed: {e}")
    diag["csv_errors"] = errors
    return None

def _read_file_any(upl, sheet_name: Optional[str], diag: Dict[str, Any]) -> pd.DataFrame:
    """
    IMPORTANT: never use `A or B` with DataFrames. Use explicit None/empty checks.
    """
    if upl is None:
        raise ValueError("No file uploaded.")
    upl.seek(0)
    raw = upl.read()
    upl.seek(0)
    name_lower = (upl.name or "").lower()
    looks_like_excel = any(ext in name_lower for ext in (".xlsx", ".xlsm", ".xls", ".xlsb")) or (b"\x00" in raw)

    df: Optional[pd.DataFrame] = None
    if looks_like_excel:
        df = _read_excel_any(raw, sheet_name, diag)
        if df is None or df.empty:
            df = _read_csv_any(raw, diag)
    else:
        df = _read_csv_any(raw, diag)
        if df is None or df.empty:
            df = _read_excel_any(raw, sheet_name, diag)

    if df is None or df.empty:
        raise ValueError("Could not parse the file. Try a clean XLSX (preferred) or CSV UTF‑8.")
    # Normalize column header BOM/whitespace
    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]
    return df


_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _norm_work_id(val) -> str:
    s = str(val or "").strip()
    s = re.sub(r"[^0-9A-Za-z]", "", s)
    s = s.lstrip("0")
    return s if s else "0"


def _normalize_apptio_actuals(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Apptio actuals wide table to tidy rows.

    Returns at least: WORK_ID, FISCAL_YEAR, MONTH, AMOUNT.
    When available, also returns dimensions for taxonomy mapping:
      - LEDGER_ACCOUNT_L3_DESC
      - PRODUCT_ID
      - PRODUCT_NAME
    """
    if df is None or df.empty:
        raise ValueError("Apptio sheet is empty.")
    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    work_col = None
    for key in ("work id", "work_id", "workid"):
        if key in cols_lower:
            work_col = cols_lower[key]
            break
    if not work_col:
        raise ValueError("Work ID column not found. Add a 'Work ID' column.")

    month_cols: Dict[str, Tuple[int, int]] = {}
    for col in df.columns:
        m = re.match(r"\s*([A-Za-z]{3,5})\s*FY\s*(\d{4})", str(col), re.IGNORECASE)
        if not m:
            continue
        mon_abbr = m.group(1).strip().lower()
        fy = int(m.group(2))
        if mon_abbr in _MONTH_ABBR:
            month_cols[col] = (_MONTH_ABBR[mon_abbr], fy)

    if not month_cols:
        raise ValueError("No month columns found (expected headers like 'Jan FY 2025').")

    def _canon_header(s: Any) -> str:
        # Robust against Excel oddities: line breaks, multiple spaces, punctuation, etc.
        return re.sub(r"[^0-9a-z]+", " ", str(s or "").lower()).strip()

    cols_canon_in_order: list[tuple[str, str]] = [(_canon_header(c), str(c)) for c in df.columns]

    def _pick_col(candidates: list[str]) -> Optional[str]:
        candidates_canon = [_canon_header(c) for c in candidates if str(c or "").strip()]
        # Pass 1: exact canonical match (prefer candidate priority, then left-most column).
        for cand in candidates_canon:
            for ccanon, orig in cols_canon_in_order:
                if ccanon == cand:
                    return orig
        # Pass 2: substring canonical match (prefer candidate priority, then left-most column).
        for cand in candidates_canon:
            for ccanon, orig in cols_canon_in_order:
                if cand and cand in ccanon:
                    return orig
        return None

    ledger_col = _pick_col(
        [
            "ledger accounts : account l3 medium text description",
            "account l3 medium text description",
            "ledger account l3",
            "ledger l3",
        ]
    )
    product_id_col = _pick_col(
        [
            "nwf / products : nwf id",
            "nwf id",
            "product id",
            "nwf_product_id",
        ]
    )
    product_name_col = _pick_col(
        [
            # Primary: column G in the standard Apptio export
            "nwf / products : nwf id name",
            "nwf / products : nwf name",
            "nwf name",
            "product name",
            "nwf_product_name",
        ]
    )

    extra_cols: list[str] = []
    if ledger_col:
        extra_cols.append(ledger_col)
    if product_id_col:
        extra_cols.append(product_id_col)
    if product_name_col:
        extra_cols.append(product_name_col)

    df_work = df[[work_col] + extra_cols + list(month_cols.keys())].copy()
    df_work = df_work.rename(columns={work_col: "WORK_ID"})
    df_work["WORK_ID"] = df_work["WORK_ID"].apply(_norm_work_id)
    rename_map: dict[str, str] = {}
    if ledger_col:
        rename_map[ledger_col] = "LEDGER_ACCOUNT_L3_DESC"
    if product_id_col:
        rename_map[product_id_col] = "PRODUCT_ID"
    if product_name_col:
        rename_map[product_name_col] = "PRODUCT_NAME"
    if rename_map:
        df_work = df_work.rename(columns=rename_map)

    id_vars = ["WORK_ID"] + [c for c in ["LEDGER_ACCOUNT_L3_DESC", "PRODUCT_ID", "PRODUCT_NAME"] if c in df_work.columns]
    long_df = df_work.melt(id_vars=id_vars, value_vars=list(month_cols.keys()), var_name="PERIOD", value_name="AMOUNT")
    long_df["AMOUNT"] = pd.to_numeric(long_df["AMOUNT"], errors="coerce")
    long_df = long_df.dropna(subset=["AMOUNT"])
    long_df["AMOUNT"] = long_df["AMOUNT"].astype(float)
    long_df["MONTH"] = long_df["PERIOD"].map({k: v[0] for k, v in month_cols.items()})
    long_df["FISCAL_YEAR"] = long_df["PERIOD"].map({k: v[1] for k, v in month_cols.items()})
    long_df = long_df.dropna(subset=["MONTH", "FISCAL_YEAR"])
    long_df["MONTH"] = long_df["MONTH"].astype(int)
    long_df["FISCAL_YEAR"] = long_df["FISCAL_YEAR"].astype(int)
    if "LEDGER_ACCOUNT_L3_DESC" not in long_df.columns:
        long_df["LEDGER_ACCOUNT_L3_DESC"] = None
    if "PRODUCT_ID" not in long_df.columns:
        long_df["PRODUCT_ID"] = None
    if "PRODUCT_NAME" not in long_df.columns:
        long_df["PRODUCT_NAME"] = None

    long_df["LEDGER_ACCOUNT_L3_DESC"] = long_df.get("LEDGER_ACCOUNT_L3_DESC").fillna("").astype(str).str.strip()
    long_df.loc[long_df["LEDGER_ACCOUNT_L3_DESC"].eq(""), "LEDGER_ACCOUNT_L3_DESC"] = None
    long_df["PRODUCT_ID"] = long_df.get("PRODUCT_ID").fillna("").astype(str).str.strip()
    long_df.loc[long_df["PRODUCT_ID"].eq(""), "PRODUCT_ID"] = None
    long_df["PRODUCT_NAME"] = long_df.get("PRODUCT_NAME").fillna("").astype(str).str.strip()
    long_df.loc[long_df["PRODUCT_NAME"].eq(""), "PRODUCT_NAME"] = None

    keys = ["WORK_ID", "FISCAL_YEAR", "MONTH", "PRODUCT_ID", "LEDGER_ACCOUNT_L3_DESC"]

    def _first_nonempty(s: pd.Series) -> Optional[str]:
        for v in s.dropna().astype(str).tolist():
            vv = str(v).strip()
            if vv:
                return vv
        return None

    agg = (
        long_df[keys + ["AMOUNT", "PRODUCT_NAME"]]
        .groupby(keys, as_index=False)
        .agg({"AMOUNT": "sum", "PRODUCT_NAME": _first_nonempty})
    )
    return agg

# -------------------------
# Template generator (Bulk Load)
# -------------------------
def generate_bulk_template() -> bytes:
    """Build an Excel template for the Bulk Load tab with required columns.

    If tables already have values, include a Lists sheet and add Excel
    data‑validation dropdowns for entity names on the Data sheet.
    """
    buf = io.BytesIO()
    # Recommended column order (covers required + optional fields used by import)
    cols = [
        # Core entities
        "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "APPNAME", "VENDORNAME",
        # Contract (required for Recurring invoices)
        "CONTRACT_START_FY", "CONTRACT_END_FY",
        "CONTRACT_ANNUAL_AMOUNT", "CONTRACT_ESCALATION_PCT", "CONTRACT_STATUS", "CONTRACT_AGREEMENT_NUMBER",
        "CONTRACT_COMPANY_CODE", "CONTRACT_COST_CENTER", "CONTRACT_SERVICE_TYPE", "CONTRACT_RENEWAL_DATE",
        "INVOICE_RENEWAL_DATE", "CONTRACT_TOTAL_COST",
        # Invoice requireds
        "AMOUNT", "FISCAL_YEAR", "RENEWAL_MONTH",
        # Invoice optional
        "INVOICE_STATUS", "CONTRACT_ACTIVE", "SERIAL_NUMBER", "WORK_ORDER", "COMPANY_CODE",
        "COST_CENTER", "PRODUCT_OWNER", "NOTES",
    ]
    df = pd.DataFrame(columns=cols)

    # Read current values from DB (best effort)
    try:
        progs_df = list_programs()
    except Exception:
        progs_df = pd.DataFrame()
    try:
        teams_df = list_teams()
    except Exception:
        teams_df = pd.DataFrame()
    try:
        groups_df = list_application_groups()
    except Exception:
        groups_df = pd.DataFrame()
    try:
        apps_df = list_applications()
    except Exception:
        apps_df = pd.DataFrame()
    try:
        vendors_df = list_vendors()
    except Exception:
        vendors_df = pd.DataFrame()

    def uniq_col(df0: pd.DataFrame, name: str) -> List[str]:
        if df0 is None or df0.empty or name not in df0.columns:
            return []
        return sorted(df0[name].dropna().astype(str).str.strip().unique().tolist())

    # Force a controlled list for Programs to avoid unnecessary entities
    prog_names = ALLOWED_PROGRAMS
    team_names = uniq_col(teams_df, "TEAMNAME")
    group_names = uniq_col(groups_df, "GROUPNAME")
    app_names = uniq_col(apps_df, "APPLICATIONNAME")
    vendor_names = uniq_col(vendors_df, "VENDORNAME")

    # Instructions sheet content
    instr: List[Dict[str, Any]] = [
        {"Field": "PROGRAMNAME",      "Required": "Yes", "Description": "Program (pick from allowed list)", "Example": "R&M"},
        {"Field": "TEAMNAME",         "Required": "Yes", "Description": "Team name (linked to Program if provided)", "Example": "ERNE WEST"},
        {"Field": "GROUPNAME",        "Required": "Yes", "Description": "This is an application group; multiple application instances can be aggregated here. If you don't have multiple application instances, the group can be named after your unique application instance/site.", "Example": "Application - MHM"},
        {"Field": "APPNAME",          "Required": "Yes", "Description": "Application instance name", "Example": "MHM Baytown"},
        {"Field": "VENDORNAME",       "Required": "Yes", "Description": "Vendor name", "Example": "Emerson"},
        {"Field": "CONTRACT_START_FY","Required": "Yes", "Description": "Contract start fiscal year", "Example": "2024"},
        {"Field": "CONTRACT_END_FY",  "Required": "Yes", "Description": "Contract end fiscal year (Contract Due)", "Example": "2027"},
        # CONTRACT_RENEWAL_MONTH intentionally omitted from template (use CONTRACT_RENEWAL_DATE or INVOICE_RENEWAL_DATE)
        {"Field": "CONTRACT_ANNUAL_AMOUNT", "Required": "Yes", "Description": "Annual amount at START_FY (numeric)", "Example": "12500.00"},
        {"Field": "CONTRACT_ESCALATION_PCT", "Required": "No", "Description": "% escalation per year (0..100)", "Example": "3.0"},
        {"Field": "CONTRACT_STATUS", "Required": "No", "Description": "Active/Terminated", "Example": "Active"},
        {"Field": "CONTRACT_AGREEMENT_NUMBER", "Required": "No", "Description": "Agreement/reference number", "Example": "AGR-123"},
        {"Field": "CONTRACT_COMPANY_CODE", "Required": "No", "Description": "Default Company Code for invoices", "Example": "1000"},
        {"Field": "CONTRACT_COST_CENTER", "Required": "No", "Description": "Default Cost Center for invoices", "Example": "CC-789"},
        {"Field": "CONTRACT_SERVICE_TYPE", "Required": "No", "Description": "Default Service Type", "Example": "SaaS"},
        {"Field": "CONTRACT_RENEWAL_DATE", "Required": "No", "Description": "Contract renewal date (MM/DD/YYYY)", "Example": "01/15/2025"},
        {"Field": "AMOUNT",           "Required": "No",  "Description": "Invoice amount (numeric). Optional — if omitted and covered by a Contract, planned amount is seeded from Contract.", "Example": "12500.00"},
        {"Field": "FISCAL_YEAR",      "Required": "Yes", "Description": "Year (integer)", "Example": "2025"},
        {"Field": "RENEWAL_MONTH",    "Required": "Yes", "Description": "Renewal month 1–12", "Example": "4"},
        {"Field": "INVOICE_STATUS",   "Required": "No",  "Description": "Recurring invoice status (Planned/Completed)", "Example": "Planned"},
        {"Field": "CONTRACT_ACTIVE",  "Required": "No",  "Description": "Derived from Contract Status (Active → TRUE, Terminated → FALSE)", "Example": "(derived)"},
        {"Field": "SERIAL_NUMBER",    "Required": "No",  "Description": "Serial / reference", "Example": "SN-123"},
        {"Field": "WORK_ORDER",       "Required": "No",  "Description": "Work order", "Example": "WO-456"},
        {"Field": "COMPANY_CODE",     "Required": "No",  "Description": "Company code", "Example": "1000"},
        {"Field": "COST_CENTER",      "Required": "No",  "Description": "Cost center", "Example": "CC-789"},
        {"Field": "PRODUCT_OWNER",    "Required": "No",  "Description": "Owner", "Example": "Alice"},
        {"Field": "NOTES",            "Required": "No",  "Description": "Free text notes", "Example": "Renewal changed to Apr"},
    ]

    from openpyxl.worksheet.datavalidation import DataValidation
    from openpyxl.utils import get_column_letter
    from openpyxl.comments import Comment
    from openpyxl.styles import PatternFill, Font
    from openpyxl.formatting.rule import FormulaRule

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
        pd.DataFrame(instr).to_excel(writer, index=False, sheet_name="Instructions")

        wb = writer.book
        ws_data = writer.sheets["Data"]
        # Freeze header row for convenience in Excel (Mac/Windows)
        try:
            ws_data.freeze_panes = "A2"
        except Exception:
            pass
        # Map header name -> column index for styling and validation
        header_to_idx = {h: i+1 for i, h in enumerate(cols)}

        # Create Lists sheet with current values (if any)
        any_lists = any([prog_names, team_names, group_names, app_names, vendor_names])
        if any_lists:
            ws_lists = wb.create_sheet("Lists")
            ws_lists["A1"] = "PROGRAMNAME";    ws_lists["B1"] = "TEAMNAME"
            ws_lists["C1"] = "GROUPNAME";      ws_lists["D1"] = "APPLICATIONNAME"
            ws_lists["E1"] = "VENDORNAME";     ws_lists["F1"] = "CONTRACT_STATUS"
            ws_lists["G1"] = "SERVICE_TYPE";   ws_lists["H1"] = "INVOICE_STATUS"

            for i, v in enumerate(prog_names, start=2):
                ws_lists.cell(row=i, column=1, value=v)
            for i, v in enumerate(team_names, start=2):
                ws_lists.cell(row=i, column=2, value=v)
            for i, v in enumerate(group_names, start=2):
                ws_lists.cell(row=i, column=3, value=v)
            for i, v in enumerate(app_names, start=2):
                ws_lists.cell(row=i, column=4, value=v)
            for i, v in enumerate(vendor_names, start=2):
                ws_lists.cell(row=i, column=5, value=v)
            # Status + Service type choices
            status_opts = ["Active", "Terminated"]
            svc_opts = ["OnPrem", "SaaS", "IaaS", "Other"]
            inv_status_opts = ["Planned", "Completed"]
            for i, v in enumerate(status_opts, start=2):
                ws_lists.cell(row=i, column=6, value=v)
            for i, v in enumerate(svc_opts, start=2):
                ws_lists.cell(row=i, column=7, value=v)
            for i, v in enumerate(inv_status_opts, start=2):
                ws_lists.cell(row=i, column=8, value=v)

            # Helper to add DV to an entire column in Data sheet
            def add_list_validation(header: str, lists_col: int, last_row: int):
                if last_row < 2:
                    return
                try:
                    idx = cols.index(header) + 1
                except ValueError:
                    return
                col_letter = get_column_letter(idx)
                first = 2; dest_last = 2000
                src_range = f"Lists!${get_column_letter(lists_col)}$2:${get_column_letter(lists_col)}${last_row}"
                dv = DataValidation(type="list", formula1=f"={src_range}", allow_blank=True)
                ws_data.add_data_validation(dv)
                dv.add(f"{col_letter}{first}:{col_letter}{dest_last}")

            add_list_validation("PROGRAMNAME", 1, 1 + len(prog_names))
            add_list_validation("TEAMNAME",    2, 1 + len(team_names))
            add_list_validation("GROUPNAME",   3, 1 + len(group_names))
            add_list_validation("APPNAME",     4, 1 + len(app_names))
            add_list_validation("VENDORNAME",  5, 1 + len(vendor_names))
            add_list_validation("CONTRACT_STATUS", 6, 1 + len(status_opts))
            add_list_validation("CONTRACT_SERVICE_TYPE", 7, 1 + len(svc_opts))
            add_list_validation("INVOICE_STATUS", 8, 1 + len(inv_status_opts))

        # Numeric validations
        def add_numeric_validation(header: str, dv: DataValidation):
            try:
                idx = cols.index(header) + 1
            except ValueError:
                return
            col_letter = get_column_letter(idx)
            first = 2; dest_last = 2000
            ws_data.add_data_validation(dv)
            dv.add(f"{col_letter}{first}:{col_letter}{dest_last}")

        # Force text formatting for code fields that may contain leading zeros
        def enforce_text_column(header: str):
            try:
                idx = cols.index(header) + 1
            except ValueError:
                return
            for r in range(2, 2001):
                ws_data.cell(row=r, column=idx).number_format = '@'

        # Months 1-12
        for header in ("RENEWAL_MONTH",):
            dv = DataValidation(type="whole", operator="between", formula1="1", formula2="12", allow_blank=True)
            add_numeric_validation(header, dv)
        # Fiscal years
        for header in ("FISCAL_YEAR", "CONTRACT_START_FY", "CONTRACT_END_FY"):
            dv = DataValidation(type="whole", operator="between", formula1="2000", formula2="2100", allow_blank=False)
            add_numeric_validation(header, dv)
        # Non-negative amounts and escalation percent
        for header in ("AMOUNT", "CONTRACT_ANNUAL_AMOUNT", "CONTRACT_TOTAL_COST"):
            dv = DataValidation(type="decimal", operator="greaterThanOrEqual", formula1="0")
            add_numeric_validation(header, dv)
        dv = DataValidation(type="decimal", operator="between", formula1="0", formula2="100", allow_blank=True)
        add_numeric_validation("CONTRACT_ESCALATION_PCT", dv)

        # Ensure codes are treated as text (not numeric)
        enforce_text_column("CONTRACT_COMPANY_CODE")
        enforce_text_column("COMPANY_CODE")

        # Header comments (tooltips)
        try:
            field_help = {row["Field"]: f"{row['Description']}" + (f" (Example: {row['Example']})" if row.get('Example') else "") for row in instr}
            for h in cols:
                if h in field_help:
                    i = header_to_idx.get(h)
                    if i:
                        cell = ws_data.cell(row=1, column=i)
                        cell.comment = Comment(field_help[h], "NEXT")
        except Exception:
            pass

        # Header colors: mark required fields (obligated) for quick visual guidance
        try:
            # Use a bright Excel-friendly yellow for visibility (Mac/Windows)
            req_header_fill = PatternFill(fill_type="solid", fgColor="FFFFCC00")
            for h in req_fields:
                i = header_to_idx.get(h)
                if i:
                    c = ws_data.cell(row=1, column=i)
                    c.fill = req_header_fill
                    try:
                        c.font = Font(bold=True)
                    except Exception:
                        pass
        except Exception:
            pass

        # Also highlight required rows in Instructions sheet for clarity
        try:
            ws_instr = writer.sheets.get("Instructions")
            if ws_instr is not None:
                # Find header positions in Instructions
                hdr_map = {}
                for col in range(1, ws_instr.max_column + 1):
                    val = ws_instr.cell(row=1, column=col).value
                    if isinstance(val, str):
                        hdr_map[val.strip().upper()] = col
                col_required = hdr_map.get("REQUIRED")
                col_field = hdr_map.get("FIELD")
                if col_required and col_field:
                    for r in range(2, ws_instr.max_row + 1):
                        if str(ws_instr.cell(row=r, column=col_required).value).strip().lower() == "yes":
                            cell = ws_instr.cell(row=r, column=col_field)
                            cell.fill = PatternFill(fill_type="solid", fgColor="FFFFCC00")
                            try:
                                cell.font = Font(bold=True)
                            except Exception:
                                pass
        except Exception:
            pass

        # Conditional formatting: missing requireds
        req_fields = [
            "PROGRAMNAME","TEAMNAME","GROUPNAME","APPNAME","VENDORNAME",
            "CONTRACT_START_FY","CONTRACT_END_FY","CONTRACT_ANNUAL_AMOUNT",
            "FISCAL_YEAR","RENEWAL_MONTH",
        ]
        try:
            red_fill = PatternFill(start_color="FFFFC7CE", end_color="FFFFC7CE", fill_type="solid")
            for h in req_fields:
                if h not in header_to_idx:
                    continue
                col_letter = get_column_letter(header_to_idx[h])
                rng = f"{col_letter}2:{col_letter}2000"
                ws_data.conditional_formatting.add(rng, FormulaRule(formula=[f"LEN(${col_letter}2)=0"], fill=red_fill))
        except Exception:
            pass

        # Conditional formatting: duplicate detection (Contracts: TEAMNAME+APPNAME; Invoices: TEAMNAME+APPNAME+FISCAL_YEAR)
        try:
            yellow_fill = PatternFill(start_color="FFFFEB9C", end_color="FFFFEB9C", fill_type="solid")
            # Contracts dupes on TEAMNAME + APPNAME
            if all(k in header_to_idx for k in ("TEAMNAME","APPNAME")):
                t_col = get_column_letter(header_to_idx["TEAMNAME"]) ; a_col = get_column_letter(header_to_idx["APPNAME"]) 
                rng = f"{t_col}2:{t_col}2000"
                formula = f"COUNTIFS($${t_col}$2:$${t_col}$2000,$${t_col}2,$${a_col}$2:$${a_col}$2000,$${a_col}2)>1".replace('$$', '$')
                ws_data.conditional_formatting.add(rng, FormulaRule(formula=[formula], fill=yellow_fill))
                rng2 = f"{a_col}2:{a_col}2000"
                ws_data.conditional_formatting.add(rng2, FormulaRule(formula=[formula], fill=yellow_fill))
            # Invoice dupes on TEAMNAME + APPNAME + FISCAL_YEAR
            if all(k in header_to_idx for k in ("TEAMNAME","APPNAME","FISCAL_YEAR")):
                t_col = get_column_letter(header_to_idx["TEAMNAME"]) ; a_col = get_column_letter(header_to_idx["APPNAME"]) ; y_col = get_column_letter(header_to_idx["FISCAL_YEAR"])
                rng = f"{y_col}2:{y_col}2000"
                formula = (
                    f"COUNTIFS($${t_col}$2:$${t_col}$2000,$${t_col}2,$${a_col}$2:$${a_col}$2000,$${a_col}2,$${y_col}$2:$${y_col}$2000,$${y_col}2)>1"
                ).replace('$$', '$')
                ws_data.conditional_formatting.add(rng, FormulaRule(formula=[formula], fill=yellow_fill))
        except Exception:
            pass

        # Pre-fill first data row with examples from actual values when available
        def set_cell(name: str, value):
            i = header_to_idx.get(name)
            if i:
                ws_data.cell(row=2, column=i, value=value)

        try:
            import datetime as _dt
            # Names: best-effort pick first available
            if prog_names:
                set_cell("PROGRAMNAME", "R&M")
            if team_names:
                set_cell("TEAMNAME", team_names[0])
            if group_names:
                set_cell("GROUPNAME", group_names[0])
            if app_names:
                set_cell("APPNAME", app_names[0])
            if vendor_names:
                set_cell("VENDORNAME", vendor_names[0])
            # Contracts
            year_now = _dt.date.today().year
            set_cell("CONTRACT_START_FY", year_now)
            set_cell("CONTRACT_END_FY", year_now + 2)
            set_cell("CONTRACT_ANNUAL_AMOUNT", 10000.00)
            set_cell("CONTRACT_ESCALATION_PCT", 3.0)
            set_cell("CONTRACT_STATUS", "Active")
            set_cell("CONTRACT_AGREEMENT_NUMBER", "AGR-001")
            set_cell("CONTRACT_COMPANY_CODE", "1000")
            set_cell("CONTRACT_COST_CENTER", "CC-001")
            set_cell("CONTRACT_SERVICE_TYPE", "SaaS")
            set_cell("CONTRACT_TOTAL_COST", 30000.00)
            set_cell("CONTRACT_RENEWAL_DATE", "01/15/2025")
            # Invoices
            set_cell("AMOUNT", 10000.00)
            set_cell("FISCAL_YEAR", year_now)
            set_cell("RENEWAL_MONTH", 1)
            set_cell("INVOICE_STATUS", "Planned")
            set_cell("CONTRACT_ACTIVE", True)
            set_cell("PRODUCT_OWNER", "Alice")
            set_cell("NOTES", "Example row; replace with your data")
        except Exception:
            pass

        # Auto-fit simple: based on header/examples
        try:
            from openpyxl.utils import get_column_letter as _gcl
            for h in cols:
                i = header_to_idx.get(h)
                if not i:
                    continue
                values = [str(h), str(ws_data.cell(row=2, column=i).value or "")]
                width = max(len(v) for v in values) + 4
                ws_data.column_dimensions[_gcl(i)].width = max(12, min(width, 60))
        except Exception:
            pass

        # Add end-of-row red warning text on example row
        try:
            from openpyxl.styles import Font as _Font
            max_idx = max(header_to_idx.values()) if header_to_idx else 1
            note_col = get_column_letter(max_idx + 1)
            ws_data.cell(row=2, column=max_idx + 1, value="<- THIS IS JUST AN EXAMPLE, DELETE THIS ROW BEFORE THE IMPORT").font = _Font(color="FF0000", bold=True)
        except Exception:
            pass

    buf.seek(0)
    return buf.getvalue()

ALLOWED_PROGRAMS = ["R&M", "Process Ops", "QM", "WFE", "Central Services"]

def generate_contracts_template() -> bytes:
    """Build an Excel template focused on Contracts (and core entities), without invoice columns.

    Includes dropdowns, numeric validations, header tooltips/colors, and a first example row.
    """
    buf = io.BytesIO()
    cols = [
        # Core entities
        "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "APPNAME", "VENDORNAME",
        # Contracts only
        "CONTRACT_START_FY", "CONTRACT_END_FY",
        "CONTRACT_ANNUAL_AMOUNT", "CONTRACT_ESCALATION_PCT", "CONTRACT_STATUS", "CONTRACT_AGREEMENT_NUMBER",
        "CONTRACT_COMPANY_CODE", "CONTRACT_COST_CENTER", "CONTRACT_SERVICE_TYPE", "CONTRACT_RENEWAL_DATE",
        "INVOICE_RENEWAL_DATE",
        "CONTRACT_TOTAL_COST",
    ]
    df = pd.DataFrame(columns=cols)

    # Current values from DB for dropdowns
    try:
        progs_df = list_programs()
    except Exception:
        progs_df = pd.DataFrame()
    try:
        teams_df = list_teams()
    except Exception:
        teams_df = pd.DataFrame()
    try:
        groups_df = list_application_groups()
    except Exception:
        groups_df = pd.DataFrame()
    try:
        apps_df = list_applications()
    except Exception:
        apps_df = pd.DataFrame()
    try:
        vendors_df = list_vendors()
    except Exception:
        vendors_df = pd.DataFrame()

    def uniq_col(df0: pd.DataFrame, name: str) -> List[str]:
        if df0 is None or df0.empty or name not in df0.columns:
            return []
        return sorted(df0[name].dropna().astype(str).str.strip().unique().tolist())

    # Use controlled list for Programs to avoid unnecessary entities
    prog_names = ALLOWED_PROGRAMS
    team_names = uniq_col(teams_df, "TEAMNAME")
    group_names = uniq_col(groups_df, "GROUPNAME")
    app_names = uniq_col(apps_df, "APPLICATIONNAME")
    vendor_names = uniq_col(vendors_df, "VENDORNAME")

    instr: List[Dict[str, Any]] = [
        {"Field": "PROGRAMNAME",      "Required": "Yes", "Description": "Program (pick from allowed list)", "Example": "R&M"},
        {"Field": "TEAMNAME",         "Required": "Yes", "Description": "Team name (linked to Program if provided)", "Example": "ERNE WEST"},
        {"Field": "GROUPNAME",        "Required": "Yes", "Description": "Application. Multiple application instances can be aggregated here. If you only have a single instance/site, you can name the group after the instance/site.", "Example": "Application - MHM"},
        {"Field": "APPNAME",          "Required": "Yes", "Description": "Application instance name", "Example": "MHM Baytown"},
        {"Field": "VENDORNAME",       "Required": "Yes", "Description": "Vendor name", "Example": "Emerson"},
        {"Field": "CONTRACT_START_FY","Required": "Yes", "Description": "Contract start fiscal year", "Example": "2024"},
        {"Field": "CONTRACT_END_FY",  "Required": "Yes", "Description": "Contract end fiscal year (Contract Due)", "Example": "2027"},
        # CONTRACT_RENEWAL_MONTH intentionally omitted from template (use CONTRACT_RENEWAL_DATE or INVOICE_RENEWAL_DATE)
        {"Field": "CONTRACT_ANNUAL_AMOUNT", "Required": "Yes", "Description": "Annual amount at START_FY (numeric)", "Example": "12500.00"},
        {"Field": "CONTRACT_ESCALATION_PCT", "Required": "No", "Description": "% escalation per year (0..100)", "Example": "3.0"},
        {"Field": "CONTRACT_STATUS", "Required": "No", "Description": "Active/Terminated", "Example": "Active"},
        {"Field": "CONTRACT_AGREEMENT_NUMBER", "Required": "No", "Description": "Agreement/reference number", "Example": "AGR-123"},
        {"Field": "CONTRACT_COMPANY_CODE", "Required": "No", "Description": "Default Company Code for invoices", "Example": "1000"},
        {"Field": "CONTRACT_COST_CENTER", "Required": "No", "Description": "Default Cost Center for invoices", "Example": "CC-789"},
        {"Field": "CONTRACT_SERVICE_TYPE", "Required": "No", "Description": "Default Service Type", "Example": "SaaS"},
        {"Field": "CONTRACT_RENEWAL_DATE", "Required": "No", "Description": "Contract renewal date (MM/DD/YYYY)", "Example": "01/15/2025"},
        {"Field": "INVOICE_RENEWAL_DATE", "Required": "No", "Description": "Default invoice renewal date (MM/DD)", "Example": "01/10"},
        {"Field": "CONTRACT_TOTAL_COST", "Required": "No", "Description": "Total contract cost for entire period (sum of annuals)", "Example": "30000.00"},
    ]

    from openpyxl.worksheet.datavalidation import DataValidation
    from openpyxl.utils import get_column_letter
    from openpyxl.comments import Comment
    from openpyxl.styles import PatternFill, Font
    from openpyxl.formatting.rule import FormulaRule

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
        pd.DataFrame(instr).to_excel(writer, index=False, sheet_name="Instructions")
        wb = writer.book
        ws_data = writer.sheets["Data"]
        try:
            ws_data.freeze_panes = "A2"
        except Exception:
            pass
        header_to_idx = {h: i+1 for i, h in enumerate(cols)}

        # Lists sheet
        any_lists = any([prog_names, team_names, group_names, app_names, vendor_names])
        if any_lists:
            ws_lists = wb.create_sheet("Lists")
            ws_lists["A1"] = "PROGRAMNAME";    ws_lists["B1"] = "TEAMNAME"
            ws_lists["C1"] = "GROUPNAME";      ws_lists["D1"] = "APPLICATIONNAME"
            ws_lists["E1"] = "VENDORNAME";     ws_lists["F1"] = "CONTRACT_STATUS"
            ws_lists["G1"] = "SERVICE_TYPE"
            for i, v in enumerate(prog_names, start=2): ws_lists.cell(row=i, column=1, value=v)
            for i, v in enumerate(team_names, start=2): ws_lists.cell(row=i, column=2, value=v)
            for i, v in enumerate(group_names, start=2): ws_lists.cell(row=i, column=3, value=v)
            for i, v in enumerate(app_names, start=2): ws_lists.cell(row=i, column=4, value=v)
            for i, v in enumerate(vendor_names, start=2): ws_lists.cell(row=i, column=5, value=v)
            status_opts = ["Active", "Terminated"]
            svc_opts = ["OnPrem", "SaaS", "IaaS", "Other"]
            for i, v in enumerate(status_opts, start=2): ws_lists.cell(row=i, column=6, value=v)
            for i, v in enumerate(svc_opts, start=2): ws_lists.cell(row=i, column=7, value=v)

            def add_list_validation(header: str, lists_col: int, last_row: int):
                if last_row < 2:
                    return
                try:
                    idx = cols.index(header) + 1
                except ValueError:
                    return
                col_letter = get_column_letter(idx)
                first = 2; dest_last = 2000
                src_range = f"Lists!${get_column_letter(lists_col)}$2:${get_column_letter(lists_col)}${last_row}"
                dv = DataValidation(type="list", formula1=f"={src_range}", allow_blank=True)
                ws_data.add_data_validation(dv)
                dv.add(f"{col_letter}{first}:{col_letter}{dest_last}")

            add_list_validation("PROGRAMNAME", 1, 1 + len(prog_names))
            add_list_validation("TEAMNAME",    2, 1 + len(team_names))
            add_list_validation("GROUPNAME",   3, 1 + len(group_names))
            add_list_validation("APPNAME",     4, 1 + len(app_names))
            add_list_validation("VENDORNAME",  5, 1 + len(vendor_names))
            add_list_validation("CONTRACT_STATUS", 6, 1 + len(status_opts))
            add_list_validation("CONTRACT_SERVICE_TYPE", 7, 1 + len(svc_opts))

        # Numeric validations
        def add_numeric_validation(header: str, dv: DataValidation):
            try:
                idx = cols.index(header) + 1
            except ValueError:
                return
            col_letter = get_column_letter(idx)
            first = 2; dest_last = 2000
            ws_data.add_data_validation(dv)
            dv.add(f"{col_letter}{first}:{col_letter}{dest_last}")

        # Force text formatting for code fields that may contain leading zeros
        def enforce_text_column(header: str):
            try:
                idx = cols.index(header) + 1
            except ValueError:
                return
            for r in range(2, 2001):
                ws_data.cell(row=r, column=idx).number_format = '@'

        # CONTRACT_RENEWAL_MONTH intentionally omitted from template
        for header in ("CONTRACT_START_FY", "CONTRACT_END_FY"):
            dv = DataValidation(type="whole", operator="between", formula1="2000", formula2="2100", allow_blank=False)
            add_numeric_validation(header, dv)
        for header in ("CONTRACT_ANNUAL_AMOUNT", "CONTRACT_TOTAL_COST"):
            dv = DataValidation(type="decimal", operator="greaterThanOrEqual", formula1="0")
            add_numeric_validation(header, dv)
        dv = DataValidation(type="decimal", operator="between", formula1="0", formula2="100", allow_blank=True)
        add_numeric_validation("CONTRACT_ESCALATION_PCT", dv)

        # Ensure CONTRACT_COMPANY_CODE is treated as text (not numeric)
        enforce_text_column("CONTRACT_COMPANY_CODE")

        # Header tooltips and colors + banner note
        try:
            field_help = {row["Field"]: f"{row['Description']}" + (f" (Example: {row['Example']})" if row.get('Example') else "") for row in instr}
            for h in cols:
                i = header_to_idx.get(h)
                if i and h in field_help:
                    cell = ws_data.cell(row=1, column=i)
                    cell.comment = Comment(field_help[h], "NEXT")
        except Exception:
            pass
        # Add a banner row in Instructions
        try:
            ws_instr = writer.sheets.get("Instructions")
            if ws_instr is not None:
                ws_instr.insert_rows(1)
                ws_instr.merge_cells(start_row=1, start_column=1, end_row=1, end_column=4)
                cell = ws_instr.cell(row=1, column=1)
                cell.value = (
                    "Importing Contracts will create planned Recurring Invoices automatically for each fiscal year between Start and End FY. "
                    "INVOICE_RENEWAL_DATE (optional) sets the default invoice renewal date."
                )
            
        except Exception:
            pass
        try:
            req_fields = [
                "PROGRAMNAME","TEAMNAME","GROUPNAME","APPNAME","VENDORNAME",
                "CONTRACT_START_FY","CONTRACT_END_FY","CONTRACT_ANNUAL_AMOUNT",
            ]
            req_header_fill = PatternFill(fill_type="solid", fgColor="FFFFCC00")
            for h in req_fields:
                i = header_to_idx.get(h)
                if i:
                    c = ws_data.cell(row=1, column=i)
                    c.fill = req_header_fill
                    try:
                        c.font = Font(bold=True)
                    except Exception:
                        pass
        except Exception:
            pass

        # Prefill example row
        def set_cell(name: str, value):
            i = header_to_idx.get(name)
            if i:
                ws_data.cell(row=2, column=i, value=value)
        try:
            import datetime as _dt
            if prog_names: set_cell("PROGRAMNAME", "R&M")
            if team_names: set_cell("TEAMNAME", team_names[0])
            if group_names: set_cell("GROUPNAME", group_names[0])
            if app_names:  set_cell("APPNAME", "MHM Baytown")
            if vendor_names: set_cell("VENDORNAME", "Emerson")
            year_now = _dt.date.today().year
            set_cell("CONTRACT_START_FY", year_now)
            set_cell("CONTRACT_END_FY", year_now + 2)
            set_cell("CONTRACT_ANNUAL_AMOUNT", 10000.00)
            set_cell("CONTRACT_ESCALATION_PCT", 3.0)
            set_cell("CONTRACT_STATUS", "Active")
            set_cell("CONTRACT_AGREEMENT_NUMBER", "AGR-001")
            set_cell("CONTRACT_COMPANY_CODE", "1000")
            set_cell("CONTRACT_COST_CENTER", "CC-001")
            set_cell("CONTRACT_SERVICE_TYPE", "SaaS")
            set_cell("CONTRACT_TOTAL_COST", 30000.00)
            set_cell("INVOICE_RENEWAL_DATE", "01/10")
        except Exception:
            pass

        # Auto-fit simple: based on header/examples
        try:
            from openpyxl.utils import get_column_letter as _gcl
            for h in cols:
                i = header_to_idx.get(h)
                if not i:
                    continue
                values = [str(h), str(ws_data.cell(row=2, column=i).value or "")]
                width = max(len(v) for v in values) + 4
                ws_data.column_dimensions[_gcl(i)].width = max(12, min(width, 60))
        except Exception:
            pass

        # Add end-of-row red warning text on example row
        try:
            from openpyxl.styles import Font as _Font
            max_idx = max(header_to_idx.values()) if header_to_idx else 1
            note_col = get_column_letter(max_idx + 1)
            ws_data.cell(row=2, column=max_idx + 1, value="<- THIS IS JUST AN EXAMPLE, DELETE THIS ROW BEFORE THE IMPORT").font = _Font(color="FF0000", bold=True)
        except Exception:
            pass

    buf.seek(0)
    return buf.getvalue()

def generate_core_entities_template() -> bytes:
    buf = io.BytesIO()
    cols = [
        "PROGRAMNAME", "TEAMNAME", "PRODUCT_OWNER", "GROUPNAME", "APPNAME", "VENDORNAME",
    ]
    df = pd.DataFrame(columns=cols)
    try:
        progs_df = list_programs()
    except Exception:
        progs_df = pd.DataFrame()
    try:
        teams_df = list_teams()
    except Exception:
        teams_df = pd.DataFrame()
    try:
        groups_df = list_application_groups()
    except Exception:
        groups_df = pd.DataFrame()
    try:
        apps_df = list_applications()
    except Exception:
        apps_df = pd.DataFrame()
    try:
        vendors_df = list_vendors()
    except Exception:
        vendors_df = pd.DataFrame()

    team_names = sorted(teams_df["TEAMNAME"].dropna().astype(str).str.strip().unique().tolist()) if not teams_df.empty else []
    group_names = sorted(groups_df["GROUPNAME"].dropna().astype(str).str.strip().unique().tolist()) if not groups_df.empty else []
    app_names  = sorted(apps_df["APPLICATIONNAME"].dropna().astype(str).str.strip().unique().tolist()) if not apps_df.empty else []
    vendor_names = sorted(vendors_df["VENDORNAME"].dropna().astype(str).str.strip().unique().tolist()) if not vendors_df.empty else []

    instr = [
        {"Field": "PROGRAMNAME", "Required": "Yes", "Description": "Program (pick from allowed list)", "Example": "R&M"},
        {"Field": "TEAMNAME", "Required": "Yes", "Description": "Team name", "Example": "ERNE WEST"},
        {"Field": "PRODUCT_OWNER", "Required": "No", "Description": "Default Product Owner for Team", "Example": "Alice"},
        {"Field": "GROUPNAME", "Required": "Yes", "Description": "Application. Multiple application instances can be aggregated here.", "Example": "Application - MHM"},
        {"Field": "APPNAME", "Required": "Yes", "Description": "Application instance name", "Example": "MHM Baytown"},
        {"Field": "VENDORNAME", "Required": "Yes", "Description": "Vendor name", "Example": "Emerson"},
    ]

    from openpyxl.worksheet.datavalidation import DataValidation
    from openpyxl.utils import get_column_letter
    from openpyxl.comments import Comment
    from openpyxl.styles import PatternFill, Font

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
        pd.DataFrame(instr).to_excel(writer, index=False, sheet_name="Instructions")
        wb = writer.book
        ws_data = writer.sheets["Data"]
        ws_data.freeze_panes = "A2"
        header_to_idx = {h: i+1 for i, h in enumerate(cols)}

        ws_lists = wb.create_sheet("Lists")
        ws_lists["A1"] = "PROGRAMNAME"; ws_lists["B1"] = "TEAMNAME"; ws_lists["C1"] = "GROUPNAME"; ws_lists["D1"] = "APPLICATIONNAME"; ws_lists["E1"] = "VENDORNAME"
        for i, v in enumerate(ALLOWED_PROGRAMS, start=2): ws_lists.cell(row=i, column=1, value=v)
        for i, v in enumerate(team_names, start=2): ws_lists.cell(row=i, column=2, value=v)
        for i, v in enumerate(group_names, start=2): ws_lists.cell(row=i, column=3, value=v)
        for i, v in enumerate(app_names, start=2): ws_lists.cell(row=i, column=4, value=v)
        for i, v in enumerate(vendor_names, start=2): ws_lists.cell(row=i, column=5, value=v)

        def add_list_validation(header: str, lists_col: int, last_row: int):
            if last_row < 2:
                return
            try:
                idx = cols.index(header) + 1
            except ValueError:
                return
            col_letter = get_column_letter(idx)
            first = 2; dest_last = 2000
            src_range = f"Lists!${get_column_letter(lists_col)}$2:${get_column_letter(lists_col)}${last_row}"
            dv = DataValidation(type="list", formula1=f"={src_range}", allow_blank=True)
            ws_data.add_data_validation(dv)
            dv.add(f"{col_letter}{first}:{col_letter}{dest_last}")

        add_list_validation("PROGRAMNAME", 1, 1 + len(ALLOWED_PROGRAMS))
        add_list_validation("TEAMNAME", 2, 1 + len(team_names))
        add_list_validation("GROUPNAME", 3, 1 + len(group_names))
        add_list_validation("APPNAME", 4, 1 + len(app_names))
        add_list_validation("VENDORNAME", 5, 1 + len(vendor_names))

        # Header tooltips/colors
        field_help = {row["Field"]: f"{row['Description']}" + (f" (Example: {row['Example']})" if row.get('Example') else "") for row in instr}
        for h in cols:
            i = header_to_idx.get(h)
            if i and h in field_help:
                cell = ws_data.cell(row=1, column=i)
                cell.comment = Comment(field_help[h], "NEXT")
        req_header_fill = PatternFill(fill_type="solid", fgColor="FFFFCC00")
        for h in ["PROGRAMNAME","TEAMNAME","GROUPNAME","APPNAME","VENDORNAME"]:
            i = header_to_idx.get(h)
            if i:
                c = ws_data.cell(row=1, column=i)
                c.fill = req_header_fill
                c.font = Font(bold=True)

        # Prefill row 2 example
        def set_cell(name: str, value):
            i = header_to_idx.get(name)
            if i:
                ws_data.cell(row=2, column=i, value=value)
        set_cell("PROGRAMNAME", "R&M")
        set_cell("TEAMNAME", "ERNE WEST")
        set_cell("GROUPNAME", "Application - MHM")
        set_cell("APPNAME", "MHM Baytown")
        set_cell("VENDORNAME", "Emerson")
        set_cell("PRODUCT_OWNER", "Alice")

        # Auto-fit simple: based on header/examples
        try:
            from openpyxl.utils import get_column_letter as _gcl
            for h in cols:
                i = header_to_idx.get(h)
                if not i:
                    continue
                values = [str(h), str(ws_data.cell(row=2, column=i).value or "")]
                width = max(len(v) for v in values) + 4
                ws_data.column_dimensions[_gcl(i)].width = max(12, min(width, 60))
        except Exception:
            pass

    buf.seek(0)
    return buf.getvalue()

# -------------------------
# ADO parsing (core)
# -------------------------
def _auto_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]
    rename: Dict[str, str] = {}
    for canonical, candidates in EXPECTED.items():
        for cand in candidates:
            match = [col for col in df.columns if isinstance(col, str) and col.lower() == cand.lower()]
            if match:
                rename[match[0]] = canonical
                break
        else:
            match2 = [col for col in df.columns if isinstance(col, str) and canonical.lower() in col.lower()]
            if match2:
                rename[match2[0]] = canonical
    return df.rename(columns=rename)

def read_ado_upload_any(upl, sheet_name: Optional[str], diag: Dict[str, Any]) -> pd.DataFrame:
    if upl is None:
        raise ValueError("No file uploaded.")
    df = _read_file_any(upl, sheet_name, diag)
    df = _auto_rename_columns(df)

    for col in (
        "Team",
        "Custom_ApplicationName",
        "Custom_InvestmentDimension",
        "Iteration",
        "Title",
        "State",
        "AreaPath",
        "AreaLevel2",
        "AreaLevel3",
        "AreaLevel4",
    ):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    for num_col in ("BusinessValue",):
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors="coerce")

    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str).str.strip()

    for dcol in ("CreatedDate", "ChangedDate"):
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce")

    if "Year" in df.columns:
        df["Year"] = df["Year"].astype(str).str.replace(",", ".", regex=False)
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")

    return df

# --- helper: pick first existing column by name(s) ---
## Normalization implemented in utils.ado.normalize_to_canonical

def _to_py(v: Any):
    import pandas as _pd, numpy as _np
    if v is None:
        return None
    if isinstance(v, _pd._libs.tslibs.nattype.NaTType):
        return None
    if isinstance(v, _pd.Timestamp):
        return v.to_pydatetime()
    if _pd.isna(v):
        return None
    if isinstance(v, _np.floating):
        return None if _np.isnan(v) else float(v)
    if isinstance(v, _np.integer):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    return v

# Note: upsert_ado_features now comes from db facade (centralized merge)

def load_ado_distincts() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    teams = fetch_df("""
        WITH base AS (
          SELECT COALESCE(t.TEAMNAME, af.TEAM_RAW) AS TEAMNAME
          FROM ADO_FEATURES af
          LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM m
            ON UPPER(m.ADO_TEAM_KEY) = COALESCE(
                 af.TEAM_VARIANT_KEY,
                 UPPER(NULLIF(LTRIM(RTRIM(af.TEAM_RAW)), ''))
               )
          LEFT JOIN TEAMS t ON t.TEAMID = m.TEAMID
          WHERE af.TEAM_RAW IS NOT NULL AND LTRIM(RTRIM(af.TEAM_RAW)) <> ''
        )
        SELECT DISTINCT TEAMNAME
        FROM base
        WHERE TEAMNAME IS NOT NULL AND LTRIM(RTRIM(TEAMNAME)) <> ''
        ORDER BY TEAMNAME
    """)
    apps  = fetch_df("""
        SELECT DISTINCT APP_NAME_RAW
        FROM ADO_FEATURES
        WHERE APP_NAME_RAW IS NOT NULL AND TRIM(APP_NAME_RAW) <> ''
        ORDER BY APP_NAME_RAW
    """)
    iters = fetch_df("""
        SELECT DISTINCT ITERATION_PATH
        FROM ADO_FEATURES
        WHERE ITERATION_PATH IS NOT NULL AND TRIM(ITERATION_PATH) <> ''
        ORDER BY ITERATION_PATH
    """)
    return teams, apps, iters

def ado_features_base_query(
    where_sql: str = "",
    params: Optional[tuple] = None,
    *,
    post_where_sql: str = "",
    post_params: Optional[tuple] = None,
) -> pd.DataFrame:
    ado_cols = _ado_feature_columns()
    business_value_expr = "f.BUSINESS_VALUE" if "BUSINESS_VALUE" in ado_cols else "CAST(NULL AS FLOAT)"
    ado_year_expr = "TRY_CONVERT(INT, f.ADO_YEAR)" if "ADO_YEAR" in ado_cols else "CAST(NULL AS INT)"
    business_value_sql = f"{business_value_expr} AS BUSINESS_VALUE"
    iteration_level3_sql = (
        "f.ITERATION_LEVEL3 AS ITERATION_LEVEL3"
        if "ITERATION_LEVEL3" in ado_cols
        else "CAST(NULL AS NVARCHAR(400)) AS ITERATION_LEVEL3"
    )
    iteration_sk_sel_sql = (
        "NULLIF(LTRIM(RTRIM(f.ITERATION_SK)), '') AS ITERATION_SK"
        if "ITERATION_SK" in ado_cols
        else "CAST(NULL AS NVARCHAR(100)) AS ITERATION_SK"
    )
    iteration_sk_join_sql = "NULLIF(LTRIM(RTRIM(f.ITERATION_SK)), '')" if "ITERATION_SK" in ado_cols else "CAST(NULL AS NVARCHAR(100))"
    # Avoid implicit nvarchar→int conversions (some environments have bad/mixed ADO_YEAR values like "2025 I1").
    cal_year_expr = """
        COALESCE(
          TRY_CONVERT(INT, LEFT(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3), 4)),
          TRY_CONVERT(INT, ic_sk.YEAR),
          TRY_CONVERT(INT, ic_path.YEAR),
          TRY_CONVERT(INT, f.ADO_YEAR)
        )
    """.strip()
    sql_with_demand = f"""
      WITH filtered AS (
        SELECT *
        FROM ADO_FEATURES
        {where_sql}
      )
      SELECT
        f.FEATURE_ID,
        f.TITLE,
        f.STATE,
        f.TEAM_RAW,
        COALESCE(t.TEAMNAME, f.TEAM_RAW) AS TEAMNAME,
        COALESCE(p.PROGRAMNAME, CAST(NULL AS NVARCHAR(255))) AS PROGRAMNAME,
        f.APP_NAME_RAW,
        COALESCE(ag.GROUPNAME, CAST(NULL AS NVARCHAR(255))) AS GROUPNAME,
        f.INVESTMENT_DIMENSION,
        {ado_year_expr} AS ADO_YEAR,
        {business_value_sql},
        f.ITERATION_PATH,
        {iteration_level3_sql},
        {iteration_sk_sel_sql},
        /* CAL_* prefix = fields from ADO_ITERATION_CALENDAR (debug visibility for iteration mapping) */
        COALESCE(ic_sk.ITERATION_SK, ic_path.ITERATION_SK) AS CAL_ITERATION_SK,
        COALESCE(ic_sk.ITERATION_PATH, ic_path.ITERATION_PATH) AS CAL_ITERATION_PATH,
        COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3) AS CAL_ITERATION_LEVEL3,
        COALESCE(ic_sk.ITERATION_GRAIN, ic_path.ITERATION_GRAIN) AS CAL_ITERATION_GRAIN,
        COALESCE(ic_sk.START_DATE, ic_path.START_DATE) AS CAL_START_DATE,
        COALESCE(ic_sk.END_DATE, ic_path.END_DATE) AS CAL_END_DATE,
        {cal_year_expr} AS CAL_YEAR,
        CASE
          WHEN COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3) IS NULL
               OR CHARINDEX('I', UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3))) = 0
          THEN NULL
          ELSE TRY_CONVERT(
                 INT,
                 SUBSTRING(
                   SUBSTRING(UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3)), CHARINDEX('I', UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3))), 10),
                   NULLIF(PATINDEX('%[1-4]%', SUBSTRING(UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3)), CHARINDEX('I', UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3))), 10)), 0),
                   1
                 )
               )
        END AS CAL_PI_NUM,
        fp.FEATURE_IS_SPRINT_LEVEL,
        CASE
          WHEN fp.FEATURE_IS_SPRINT_LEVEL = 1
               AND COALESCE(ic_sk.ITERATION_PATH, ic_path.ITERATION_PATH) IS NOT NULL
          THEN 1
          ELSE 0
        END AS SPRINT_TO_PI_RESOLVED,
        CASE
          WHEN COALESCE(ic_sk.ITERATION_PATH, ic_path.ITERATION_PATH) IS NULL THEN 1 ELSE 0
        END AS MISSING_PI_CALENDAR,
        TRY_CONVERT(FLOAT, dem.DERIVED_FTE_FEATURE) AS DERIVED_FTE,
        CAST(NULL AS FLOAT) AS FTE_DIFF,
        f.CHANGED_AT
      FROM filtered f
      OUTER APPLY (
        /* Keep this mapping logic aligned with the canonical feature→PI mapping (sprint-level Features map to PI parent path). */
        SELECT
          CASE
            WHEN f.ITERATION_PATH IS NULL OR LTRIM(RTRIM(f.ITERATION_PATH)) = '' THEN 0
            WHEN PATINDEX('%\\I[0-9] S[0-9]%', UPPER(f.ITERATION_PATH)) > 0 THEN 1
            WHEN PATINDEX('%\\S[0-9]%', UPPER(f.ITERATION_PATH)) > 0 THEN 1
            ELSE 0
          END AS FEATURE_IS_SPRINT_LEVEL,
          CASE
            WHEN f.ITERATION_PATH IS NULL OR LTRIM(RTRIM(f.ITERATION_PATH)) = '' THEN NULL
            WHEN (
              PATINDEX('%\\I[0-9] S[0-9]%', UPPER(f.ITERATION_PATH)) > 0
              OR PATINDEX('%\\S[0-9]%', UPPER(f.ITERATION_PATH)) > 0
            )
            AND CHARINDEX('\\', f.ITERATION_PATH) > 0
            THEN LEFT(f.ITERATION_PATH, LEN(f.ITERATION_PATH) - CHARINDEX('\\', REVERSE(f.ITERATION_PATH)))
            ELSE f.ITERATION_PATH
          END AS FEATURE_PI_PARENT_ITERATION_PATH
      ) fp
      LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM m
        ON UPPER(m.ADO_TEAM_KEY) = COALESCE(
             f.TEAM_VARIANT_KEY,
             UPPER(NULLIF(LTRIM(RTRIM(f.TEAM_RAW)), ''))
           )
      LEFT JOIN TEAMS t ON t.TEAMID = m.TEAMID
      LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
      LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag
        ON UPPER(mag.ADO_APP) = UPPER(LTRIM(RTRIM(f.APP_NAME_RAW)))
      LEFT JOIN APPLICATION_GROUPS ag ON ag.GROUPID = mag.APP_GROUP
      LEFT JOIN ADO_ITERATION_CALENDAR ic_sk
        ON UPPER(LTRIM(RTRIM(ic_sk.ITERATION_SK))) = UPPER(LTRIM(RTRIM({iteration_sk_join_sql})))
       AND UPPER(LTRIM(RTRIM(ic_sk.ITERATION_GRAIN))) = 'PI'
      LEFT JOIN ADO_ITERATION_CALENDAR ic_path
        ON UPPER(LTRIM(RTRIM(ic_path.ITERATION_PATH))) = UPPER(LTRIM(RTRIM(fp.FEATURE_PI_PARENT_ITERATION_PATH)))
       AND UPPER(LTRIM(RTRIM(ic_path.ITERATION_GRAIN))) = 'PI'
      LEFT JOIN VW_TCO_FEATURE_DEMAND dem
        ON dem.FEATURE_ID = f.FEATURE_ID
       AND TRY_CONVERT(INT, dem.YEAR) = {cal_year_expr}
       AND UPPER(LTRIM(RTRIM(COALESCE(dem.PI_LABEL, '')))) = UPPER(LTRIM(RTRIM(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3, ''))))
      {post_where_sql}
      ORDER BY COALESCE(f.CHANGED_AT, TRY_CONVERT(DATETIME2,'1900-01-01')) DESC, f.FEATURE_ID
    """
    sql_fallback = f"""
      WITH filtered AS (
        SELECT *
        FROM ADO_FEATURES
        {where_sql}
      )
      SELECT
        f.FEATURE_ID,
        f.TITLE,
        f.STATE,
        f.TEAM_RAW,
        COALESCE(t.TEAMNAME, f.TEAM_RAW) AS TEAMNAME,
        COALESCE(p.PROGRAMNAME, CAST(NULL AS NVARCHAR(255))) AS PROGRAMNAME,
        f.APP_NAME_RAW,
        COALESCE(ag.GROUPNAME, CAST(NULL AS NVARCHAR(255))) AS GROUPNAME,
        f.INVESTMENT_DIMENSION,
        {ado_year_expr} AS ADO_YEAR,
        {business_value_sql},
        f.ITERATION_PATH,
        {iteration_level3_sql},
        {iteration_sk_sel_sql},
        /* CAL_* prefix = fields from ADO_ITERATION_CALENDAR (debug visibility for iteration mapping) */
        COALESCE(ic_sk.ITERATION_SK, ic_path.ITERATION_SK) AS CAL_ITERATION_SK,
        COALESCE(ic_sk.ITERATION_PATH, ic_path.ITERATION_PATH) AS CAL_ITERATION_PATH,
        COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3) AS CAL_ITERATION_LEVEL3,
        COALESCE(ic_sk.ITERATION_GRAIN, ic_path.ITERATION_GRAIN) AS CAL_ITERATION_GRAIN,
        COALESCE(ic_sk.START_DATE, ic_path.START_DATE) AS CAL_START_DATE,
        COALESCE(ic_sk.END_DATE, ic_path.END_DATE) AS CAL_END_DATE,
        {cal_year_expr} AS CAL_YEAR,
        CASE
          WHEN COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3) IS NULL
               OR CHARINDEX('I', UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3))) = 0
          THEN NULL
          ELSE TRY_CONVERT(
                 INT,
                 SUBSTRING(
                   SUBSTRING(UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3)), CHARINDEX('I', UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3))), 10),
                   NULLIF(PATINDEX('%[1-4]%', SUBSTRING(UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3)), CHARINDEX('I', UPPER(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3))), 10)), 0),
                   1
                 )
               )
        END AS CAL_PI_NUM,
        fp.FEATURE_IS_SPRINT_LEVEL,
        CASE
          WHEN fp.FEATURE_IS_SPRINT_LEVEL = 1
               AND COALESCE(ic_sk.ITERATION_PATH, ic_path.ITERATION_PATH) IS NOT NULL
          THEN 1
          ELSE 0
        END AS SPRINT_TO_PI_RESOLVED,
        CASE
          WHEN COALESCE(ic_sk.ITERATION_PATH, ic_path.ITERATION_PATH) IS NULL THEN 1 ELSE 0
        END AS MISSING_PI_CALENDAR,
        TRY_CONVERT(FLOAT, dem.DERIVED_FTE_FEATURE) AS DERIVED_FTE,
        CAST(NULL AS FLOAT) AS FTE_DIFF,
        f.CHANGED_AT
      FROM filtered f
      OUTER APPLY (
        SELECT
          CASE
            WHEN f.ITERATION_PATH IS NULL OR LTRIM(RTRIM(f.ITERATION_PATH)) = '' THEN 0
            WHEN PATINDEX('%\\I[0-9] S[0-9]%', UPPER(f.ITERATION_PATH)) > 0 THEN 1
            WHEN PATINDEX('%\\S[0-9]%', UPPER(f.ITERATION_PATH)) > 0 THEN 1
            ELSE 0
          END AS FEATURE_IS_SPRINT_LEVEL,
          CASE
            WHEN f.ITERATION_PATH IS NULL OR LTRIM(RTRIM(f.ITERATION_PATH)) = '' THEN NULL
            WHEN (
              PATINDEX('%\\I[0-9] S[0-9]%', UPPER(f.ITERATION_PATH)) > 0
              OR PATINDEX('%\\S[0-9]%', UPPER(f.ITERATION_PATH)) > 0
            )
            AND CHARINDEX('\\', f.ITERATION_PATH) > 0
            THEN LEFT(f.ITERATION_PATH, LEN(f.ITERATION_PATH) - CHARINDEX('\\', REVERSE(f.ITERATION_PATH)))
            ELSE f.ITERATION_PATH
          END AS FEATURE_PI_PARENT_ITERATION_PATH
      ) fp
      LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM m
        ON UPPER(m.ADO_TEAM_KEY) = COALESCE(
             f.TEAM_VARIANT_KEY,
             UPPER(NULLIF(LTRIM(RTRIM(f.TEAM_RAW)), ''))
           )
      LEFT JOIN TEAMS t ON t.TEAMID = m.TEAMID
      LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
      LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag
        ON UPPER(mag.ADO_APP) = UPPER(LTRIM(RTRIM(f.APP_NAME_RAW)))
      LEFT JOIN APPLICATION_GROUPS ag ON ag.GROUPID = mag.APP_GROUP
      LEFT JOIN ADO_ITERATION_CALENDAR ic_sk
        ON UPPER(LTRIM(RTRIM(ic_sk.ITERATION_SK))) = UPPER(LTRIM(RTRIM({iteration_sk_join_sql})))
       AND UPPER(LTRIM(RTRIM(ic_sk.ITERATION_GRAIN))) = 'PI'
      LEFT JOIN ADO_ITERATION_CALENDAR ic_path
        ON UPPER(LTRIM(RTRIM(ic_path.ITERATION_PATH))) = UPPER(LTRIM(RTRIM(fp.FEATURE_PI_PARENT_ITERATION_PATH)))
       AND UPPER(LTRIM(RTRIM(ic_path.ITERATION_GRAIN))) = 'PI'
      LEFT JOIN VW_TCO_FEATURE_DEMAND dem
        ON dem.FEATURE_ID = f.FEATURE_ID
       AND TRY_CONVERT(INT, dem.YEAR) = {cal_year_expr}
       AND UPPER(LTRIM(RTRIM(COALESCE(dem.PI_LABEL, '')))) = UPPER(LTRIM(RTRIM(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3, ''))))
      {post_where_sql}
      ORDER BY COALESCE(f.CHANGED_AT, TRY_CONVERT(DATETIME2,'1900-01-01')) DESC, f.FEATURE_ID
    """

    combined_params: Optional[tuple]
    if params and post_params:
        combined_params = tuple(params) + tuple(post_params)
    elif params:
        combined_params = params
    elif post_params:
        combined_params = post_params
    else:
        combined_params = None

    try:
        df = fetch_df(sql_with_demand, combined_params)
    except Exception:
        df = fetch_df(sql_fallback, combined_params)
    if df is not None and not df.empty:
        for num_col in ("BUSINESS_VALUE", "CAL_PI_NUM", "CAL_YEAR", "ADO_YEAR"):
            if num_col in df.columns:
                df[num_col] = pd.to_numeric(df[num_col], errors="coerce")
    return df if df is not None else pd.DataFrame()

# === ADO: Sync Section (moved from former “Sync & Import”) ===
def render_ado_sync_section():
        try:
            info = fetch_df("SELECT COUNT(*) AS N FROM ADO_FEATURES")
            n_rows = int(info.iloc[0]["N"]) if info is not None and not info.empty else 0
            st.info(f"Current rows in ADO_FEATURES: **{n_rows}**")
        except
if top_nav == "ADO":
    profiles_base, profiles_full = _load_ado_profiles()
    _render_ado_active_profile_header(profiles_full)

    _settings_ado_nav_options = ["Profiles", "Validate", "Sync", "Mapping", "Advanced"]
    legacy_nav_map = {
        "Sync & Import": "Sync",
        "Mapping": "Mapping",
        "Explorer": "Advanced",
    }
    prev_nav = st.session_state.get("settings_ado_nav")
    if prev_nav in legacy_nav_map:
        st.session_state["settings_ado_nav"] = legacy_nav_map[prev_nav]
    ado_nav = st.radio(
        "ADO",
        _settings_ado_nav_options,
        horizontal=True,
        key="settings_ado_nav",
        label_visibility="collapsed",
    )

    if ado_nav == "Profiles":
        ctx = _ado_profile_context(profiles_base, profiles_full)
        render_ado_profiles_section(ctx)
    elif ado_nav == "Validate":
        ctx = _ado_profile_context(profiles_base, profiles_full)
        render_ado_validate_section(ctx)
    elif ado_nav == "Sync":
        render_ado_sync_section()
    elif ado_nav == "Mapping":
        render_ado_mapping_section()
    else:
        render_ado_advanced_section()

g") if isinstance(_ado_cfg, dict) else None) or "",
            "project": (_ado_cfg.get("project") if isinstance(_ado_cfg, dict) else None) or "",
            "entity": (_ado_cfg.get("entity") if isinstance(_ado_cfg, dict) else None) or "WorkItems",
            "select": (_ado_cfg.get("select") if isinstance(_ado_cfg, dict) else None)
            or "WorkItemId,Title,State,CreatedDate,ChangedDate,BusinessValue,Custom_ApplicationName,Custom_InvestmentDimension",
            "expand": (_ado_cfg.get("expand") if isinstance(_ado_cfg, dict) else None)
            or "Team($select=TeamName),Iteration($select=IterationLevel3,IterationPath,IterationSK),Area($select=AreaPath,AreaLevel2,AreaLevel3,AreaLevel4)",
            "filter": (_ado_cfg.get("filter") if isinstance(_ado_cfg, dict) else None) or "WorkItemType eq 'Feature'",
            "url": (_ado_cfg.get("url") if isinstance(_ado_cfg, dict) else None) or "",
        }

        st.markdown("### Connection & portfolio")
        col_conn, col_port = st.columns([3, 2])
        with col_port:
            # Portfolio-level settings persisted in session
            st.session_state.setdefault("ado_portfolio_settings", {})
            portfolio_options = ["(unsaved)"] + sorted(st.session_state["ado_portfolio_settings"].keys())
            sel_portfolio = st.selectbox("Portfolio", portfolio_options, index=0, key="ado_portfolio_select")
            portfolio_name = st.text_input("Portfolio name", value=("" if sel_portfolio == "(unsaved)" else sel_portfolio), key="ado_portfolio_name").strip()
        current_settings = st.session_state["ado_portfolio_settings"].get(sel_portfolio, {}) if sel_portfolio != "(unsaved)" else {}
        stored_pat = current_settings.get("pat") if current_settings.get("store_pat") else ""

        # Apply stored defaults only when switching portfolios (avoid overwriting user input on reruns).
        active_key = sel_portfolio if sel_portfolio != "(unsaved)" else "(unsaved)"
        if st.session_state.get("_ado_active_portfolio_key") != active_key:
            st.session_state["_ado_active_portfolio_key"] = active_key
            st.session_state["ado_query_mode"] = current_settings.get("query_mode", st.session_state.get("ado_query_mode", "Advanced OData Builder"))
            for key, fallback in [
                ("ado_full_url_input", current_settings.get("url", _ado_defaults["url"])),
                ("ado_pat", stored_pat or _ado_defaults["pat"]),
                ("ado_store_pat", bool(current_settings.get("store_pat"))),
                ("ado_max_pages", int(current_settings.get("max_pages", 5))),
                ("ado_org", current_settings.get("org", _ado_defaults["org"])),
                ("ado_proj", current_settings.get("project", _ado_defaults["project"])),
                ("ado_entity", current_settings.get("entity", _ado_defaults["entity"])),
                ("ado_select", current_settings.get("select", _ado_defaults["select"])),
                ("ado_expand", current_settings.get("expand", _ado_defaults["expand"])),
                ("ado_filter", current_settings.get("filter", _ado_defaults["filter"])),
                ("ado_features_only", current_settings.get("features_only", True)),
            ]:
                st.session_state[key] = fallback

        with col_conn:
            query_mode = st.radio(
                "OData query mode",
                options=["Advanced OData Builder", "Full OData URL (Expert)"],
                horizontal=True,
                key="ado_query_mode",
            )
            full_url = st.text_input(
                "Full OData URL",
                value=current_settings.get("url") or st.session_state.get("ado_prefill_full_url") or _ado_defaults["url"],
                key="ado_full_url_input",
                disabled=(query_mode == "Advanced OData Builder"),
            ).strip()
            require_pat = True
            pat = st.text_input("Personal Access Token (PAT)", value=stored_pat or _ado_defaults["pat"], type="password", key="ado_pat")
            store_pat = st.checkbox("Store PAT with portfolio", value=bool(current_settings.get("store_pat")), key="ado_store_pat")
            max_pages = st.number_input("Max pages to fetch", value=int(current_settings.get("max_pages") or 5), min_value=1, max_value=100, step=1, key="ado_max_pages")
            st.info("Source mode: Cloud (PAT required)")

        def _ensure_iteration_expand(expand_str: str) -> str:
            exp = (expand_str or "").strip()
            if not exp:
                return "Iteration($select=IterationLevel3,IterationPath,IterationSK)"
            # Ensure Iteration($select=...) contains join keys needed for iteration calendar mapping.
            m = re.search(r"Iteration\\(\\s*\\$select\\s*=\\s*([^\\)]*)\\)", exp, flags=re.IGNORECASE)
            if not m:
                # No Iteration expand at all: append it.
                return exp + ("," if exp and not exp.endswith(",") else "") + "Iteration($select=IterationLevel3,IterationPath,IterationSK)"
            raw_fields = m.group(1)
            fields = [f.strip() for f in raw_fields.split(",") if f.strip()]
            fields_l = {f.lower() for f in fields}
            required = ["IterationLevel3", "IterationPath", "IterationSK"]
            for req in required:
                if req.lower() not in fields_l:
                    fields.append(req)
            fixed = ",".join(fields)
            return exp[: m.start(1)] + fixed + exp[m.end(1) :]

        def _validate_full_url(url: str) -> Optional[str]:
            u = (url or "").strip()
            if not u:
                return "Missing OData URL."
            bad_tokens = ["$metadata", "@odata.context", "odata.context"]
            if any(tok.lower() in u.lower() for tok in bad_tokens):
                return "This looks like metadata/context, not an OData query URL. Paste the WorkItems query URL (with $select/$expand/$filter)."
            # Require Iteration join keys for iteration calendar mapping.
            decoded = u.replace("%28", "(").replace("%29", ")").replace("%2C", ",").replace("%24", "$")
            if "Iteration(" not in decoded and "iteration(" not in decoded:
                return "Missing Iteration expand. Add: $expand=Iteration($select=IterationLevel3,IterationPath,IterationSK)"
            for req in ("IterationLevel3", "IterationPath", "IterationSK"):
                if req.lower() not in decoded.lower():
                    return "Iteration expand must include IterationLevel3, IterationPath, and IterationSK for calendar mapping."
            return None

        with st.expander("Advanced OData builder (optional)", expanded=False):
            disabled_builder = (query_mode == "Full OData URL (Expert)")
            if disabled_builder:
                st.caption("Disabled in Full URL mode. Switch query mode to use the builder.")
            else:
                st.caption("Set org/project/select/expand/filter, then apply or run directly in Advanced Builder mode.")
            colb1, colb2 = st.columns(2)
            with colb1:
                org = st.text_input("Organization", value=current_settings.get("org", _ado_defaults["org"]), placeholder="your-org", key="ado_org", disabled=disabled_builder).strip()
                proj = st.text_input("Project", value=current_settings.get("project", _ado_defaults["project"]), placeholder="your-project", key="ado_proj", disabled=disabled_builder).strip()
                entity = (st.text_input("Entity", value=current_settings.get("entity", _ado_defaults["entity"]), key="ado_entity", disabled=disabled_builder).strip() or "WorkItems")
                features_only = st.checkbox("Only WorkItemType == 'Feature'", value=current_settings.get("features_only", True), key="ado_features_only", disabled=disabled_builder)
            with colb2:
                select = st.text_area("$select", value=current_settings.get("select", _ado_defaults["select"]), height=70, key="ado_select", disabled=disabled_builder).strip()
                expand = st.text_input("$expand", value=current_settings.get("expand", _ado_defaults["expand"]), key="ado_expand", disabled=disabled_builder).strip()
                filter_ = st.text_input("$filter (optional)", value=current_settings.get("filter", _ado_defaults["filter"]), placeholder="WorkItemType eq 'Feature'", key="ado_filter", disabled=disabled_builder).strip()

            built_url = ""
            if not disabled_builder:
                expand_fixed = _ensure_iteration_expand(expand)
                if expand_fixed != expand:
                    st.warning("Builder $expand is missing required Iteration join fields; URL generation will add them for execution.")
                    if st.button("Fix $expand to include Iteration join keys", key="ado_fix_expand_keys_btn"):
                        st.session_state["ado_expand"] = expand_fixed
                        st.rerun()
                filter_for_build = filter_ or ("WorkItemType eq 'Feature'" if features_only else None)
                built_url = _build_ado_odata_url(org, proj, entity, select or None, expand_fixed or None, filter_for_build, None) if (org and proj) else ""
                st.code(built_url or "(set org/project to build URL)")
                if st.button("Apply built URL", key="ado_apply_built", disabled=not built_url):
                    st.session_state["ado_full_url_input"] = built_url
                    st.success("Copied builder URL into Full OData URL field.")

        # Final URL used for fetch (strict mode precedence).
        final_url = built_url if query_mode == "Advanced OData Builder" else full_url
        with st.expander("Final OData URL sent to Azure DevOps", expanded=False):
            st.code(final_url or "(no URL)")

        st.markdown("### Demand driver (Expected/Forecast)")
        st.info(
            "The app always uses Derived FTE (SWAG) as the single demand driver for Expected/Forecast. "
            "If SWAG input is missing for a feature, its demand is 0."
        )

        diag_api: Dict[str, Any] = {}
        can_fetch = bool(final_url) and bool(pat)

        # Helper to execute the fetch + normalize and render diagnostics
        def _do_odata_fetch():
            try:
                url_to_use = final_url
                if query_mode == "Full OData URL (Expert)":
                    err = _validate_full_url(url_to_use)
                    if err:
                        st.error(err)
                        return
                raw_df = fetch_ado_odata(url_to_use, pat if require_pat else "", diag_api, max_pages=int(max_pages))
                if raw_df is None or raw_df.empty:
                    st.warning("OData call returned no rows.")
                    return
                if features_only:
                    try:
                        cols = [c for c in raw_df.columns if c.lower().endswith("workitemtype") or c.lower() == "workitemtype"]
                        if cols:
                            c0 = cols[0]
                            raw_df = raw_df[raw_df[c0].astype(str).str.strip().str.lower() == "feature"].copy()
                    except Exception:
                        pass

                df_expected = transform_ado_odata_to_expected(raw_df)
                norm_diag: Dict[str, Any] = {}
                df_norm = normalize_to_canonical(_auto_rename_columns(df_expected), diag=norm_diag)

                # Regression guard: prevent silently dropping key ADO fields during normalization.
                def _guard_field_not_dropped(
                    *,
                    label: str,
                    raw_aliases: List[str],
                    expected_col: str,
                    norm_col: str,
                ) -> None:
                    def _raw_nonnull_count() -> int:
                        for a in raw_aliases:
                            if a in raw_df.columns:
                                try:
                                    n = int(pd.to_numeric(raw_df[a], errors="coerce").notna().sum())
                                except Exception:
                                    n = int(raw_df[a].notna().sum())
                                if n > 0:
                                    return n
                        return 0

                    raw_n = _raw_nonnull_count()
                    if raw_n <= 0:
                        return
                    exp_n = int(pd.to_numeric(df_expected.get(expected_col), errors="coerce").notna().sum()) if expected_col in df_expected.columns else 0
                    norm_n = int(pd.to_numeric(df_norm.get(norm_col), errors="coerce").notna().sum()) if norm_col in df_norm.columns else 0
                    if exp_n <= 0:
                        raise RuntimeError(
                            f"Normalization bug: {label} exists in raw OData but was not mapped into expected columns. "
                            f"Check your $select includes the field and that the field name matches your Analytics schema."
                        )
                    if norm_n <= 0:
                        raise RuntimeError(
                            f"Normalization bug: {label} was dropped during normalization. Aborting sync to avoid data loss."
                        )

                _guard_field_not_dropped(
                    label="BusinessValue",
                    raw_aliases=[
                        "BusinessValue",
                        "Fields.BusinessValue",
                        "Microsoft.VSTS.Common.BusinessValue",
                        "Fields.Microsoft.VSTS.Common.BusinessValue",
                        "Microsoft_VSTS_Common_BusinessValue",
                        "Fields.Microsoft_VSTS_Common_BusinessValue",
                    ],
                    expected_col="BusinessValue",
                    norm_col="BUSINESS_VALUE",
                )
                st.session_state["ado_parsed_raw"] = df_expected
                st.session_state["ado_parsed_norm"] = df_norm
                st.success(f"Fetched {len(df_expected)} rows via OData; normalized {len(df_norm)} rows.")
                def _pct_present(series: pd.Series) -> float:
                    if series is None or series.empty:
                        return 0.0
                    return float(pd.to_numeric(series, errors="coerce").notna().mean() * 100.0)

                d_total = int(len(df_norm))
                bv_pct = _pct_present(df_norm.get("BUSINESS_VALUE", pd.Series(dtype=float)))
                c1, c2 = st.columns(2)
                c1.metric("Rows", f"{d_total:,d}")
                c2.metric("% with Business Value", f"{bv_pct:.1f}%")
                mode = diag_api.get("mode", "?")
                pages = diag_api.get("pages", [])
                st.caption(f"Diagnostics: mode={mode}; pages={len(pages)}")
                with st.expander("Preview (first 200 rows)", expanded=False):
                    st.dataframe(df_expected.head(200), use_container_width=True, height=320)
                with st.expander("Diagnostics", expanded=False):
                    st.json({**diag_api, **norm_diag})
                st.session_state["ado_auto_fetched"] = True
            except Exception as e:
                with st.expander("Diagnostics", expanded=True):
                    st.exception(e)
                    st.json(diag_api)
                st.error("OData fetch failed. Verify URL/PAT and that Analytics is enabled.")

        # Auto-fetch once per session when URL and PAT are present
        if can_fetch and not st.session_state.get("ado_auto_fetched"):
            _do_odata_fetch()

        action_col1, action_col2 = st.columns([1, 1])
        with action_col1:
            if st.button("Fetch from ADO (OData)", icon=":material/refresh:", disabled=not can_fetch, key="btn_fetch_ado_odata"):
                _do_odata_fetch()
        with action_col2:
            if st.button("Save portfolio settings", icon=":material/save:", disabled=not portfolio_name, key="btn_save_portfolio"):
                save_name = (portfolio_name or sel_portfolio or "(unsaved)").strip()
                settings_payload = {
                    "query_mode": query_mode,
                    "url": full_url,
                    "max_pages": max_pages,
                    "org": org,
                    "project": proj,
                    "entity": entity,
                    "select": select,
                    "expand": _ensure_iteration_expand(expand),
                    "filter": filter_,
                    "features_only": features_only,
                    "store_pat": bool(store_pat),
                }
                if store_pat:
                    settings_payload["pat"] = pat
                user_email = st.session_state.get("auth_user", {}).get("email")
                try:
                    save_ado_portfolio_settings(save_name, settings_payload, updated_by=user_email)
                except Exception as e:
                    st.error(f"Failed to save portfolio settings: {e}")
                else:
                    st.session_state["ado_portfolio_settings"][save_name] = settings_payload
                    toast_success("Portfolio settings saved.")
                    if sel_portfolio == "(unsaved)" and save_name:
                        st.session_state["ado_portfolio_select"] = save_name
                        st.rerun()

        with st.expander("Danger zone", expanded=False):
            if sel_portfolio == "(unsaved)":
                st.caption("Select a saved portfolio to delete or reset its stored settings.")
            else:
                confirm = st.checkbox(
                    "I understand this will remove saved ADO settings for this portfolio",
                    value=False,
                    key="ado_delete_confirm",
                )
                if st.button(
                    "Delete portfolio ADO settings",
                    type="secondary",
                    disabled=not confirm,
                    key="ado_delete_portfolio_btn",
                ):
                    try:
                        delete_ado_portfolio_settings(sel_portfolio)
                    except Exception as e:
                        st.error(f"Failed to delete portfolio settings: {e}")
                        st.stop()
                    st.session_state["ado_portfolio_settings"].pop(sel_portfolio, None)
                    for k in [
                        "ado_full_url_input",
                        "ado_pat",
                        "ado_store_pat",
                        "ado_max_pages",
                        "ado_org",
                        "ado_proj",
                        "ado_entity",
                        "ado_select",
                        "ado_expand",
                        "ado_filter",
                        "ado_features_only",
                        "ado_query_mode",
                        "ado_auto_fetched",
                        "ado_parsed_raw",
                        "ado_parsed_norm",
                    ]:
                        st.session_state.pop(k, None)
                    st.session_state["_ado_active_portfolio_key"] = None
                    st.session_state["ado_portfolio_select"] = "(unsaved)"
                    st.success("Portfolio ADO settings deleted. Defaults will be used.")
                    st.rerun()

                if st.button(
                    "Reset ADO settings to defaults (keep record)",
                    type="secondary",
                    key="ado_reset_portfolio_btn",
                ):
                    reset_payload = {
                        "query_mode": "Advanced OData Builder",
                        "url": _ado_defaults["url"],
                        "max_pages": 5,
                        "org": _ado_defaults["org"],
                        "project": _ado_defaults["project"],
                        "entity": _ado_defaults["entity"],
                        "select": _ado_defaults["select"],
                        "expand": _ado_defaults["expand"],
                        "filter": _ado_defaults["filter"],
                        "features_only": True,
                        "store_pat": False,
                    }
                    user_email = st.session_state.get("auth_user", {}).get("email")
                    try:
                        save_ado_portfolio_settings(sel_portfolio, reset_payload, updated_by=user_email)
                    except Exception as e:
                        st.error(f"Failed to reset portfolio settings: {e}")
                        st.stop()
                    st.session_state["ado_portfolio_settings"][sel_portfolio] = reset_payload
                    st.session_state["_ado_active_portfolio_key"] = None
                    st.success("Portfolio ADO settings reset to defaults.")
                    st.rerun()

        st.markdown("### Iteration Calendar")
        st.checkbox(
            "Sync Iteration Calendar (start/end dates)",
            value=True,
            help="Fetches Iterations (start/end dates) from Analytics OData and stores them in ADO_ITERATION_CALENDAR. Required for accurate feature time bucketing.",
            key="ado_sync_iteration_calendar",
        )
        try:
            ensure_ado_iteration_calendar_table()
            df_ic_meta = fetch_df("SELECT COUNT(*) AS N, MAX(UPDATED_AT) AS LAST_UPDATED FROM ADO_ITERATION_CALENDAR")
            ic_n = int(df_ic_meta.iloc[0]["N"]) if df_ic_meta is not None and not df_ic_meta.empty else 0
            ic_last = df_ic_meta.iloc[0].get("LAST_UPDATED") if df_ic_meta is not None and not df_ic_meta.empty else None
        except Exception:
            ic_n, ic_last = 0, None
        c_it1, c_it2 = st.columns([1, 2])
        c_it1.metric("Iterations synced", ic_n)
        c_it2.metric("Iteration calendar last updated", (str(ic_last) if ic_last is not None else "—"))

        with st.expander("Iteration mapping diagnostics", expanded=False):
            try:
                ensure_ado_iteration_calendar_table()
                df_breakdown = fetch_df(
                    """
                    WITH x AS (
                      SELECT
                        af.FEATURE_ID,
                        af.ITERATION_PATH AS FEATURE_ITERATION_PATH_RAW,
                        CASE
                          WHEN af.ITERATION_PATH IS NULL OR LTRIM(RTRIM(af.ITERATION_PATH)) = '' THEN 0
                          WHEN PATINDEX('%\\I[0-9] S[0-9]%', UPPER(af.ITERATION_PATH)) > 0 THEN 1
                          WHEN PATINDEX('%\\S[0-9]%', UPPER(af.ITERATION_PATH)) > 0 THEN 1
                          ELSE 0
                        END AS FEATURE_IS_SPRINT_LEVEL,
                        CASE
                          WHEN af.ITERATION_PATH IS NULL OR LTRIM(RTRIM(af.ITERATION_PATH)) = '' THEN NULL
                          WHEN (
                            PATINDEX('%\\I[0-9] S[0-9]%', UPPER(af.ITERATION_PATH)) > 0
                            OR PATINDEX('%\\S[0-9]%', UPPER(af.ITERATION_PATH)) > 0
                          )
                          AND CHARINDEX('\\', af.ITERATION_PATH) > 0
                          THEN LEFT(af.ITERATION_PATH, LEN(af.ITERATION_PATH) - CHARINDEX('\\', REVERSE(af.ITERATION_PATH)))
                          ELSE af.ITERATION_PATH
                        END AS FEATURE_PI_PARENT_ITERATION_PATH
                      FROM ADO_FEATURES af
                      WHERE af.ITERATION_PATH IS NOT NULL
                        AND LTRIM(RTRIM(af.ITERATION_PATH)) <> ''
                    ),
                    j AS (
                      SELECT
                        x.*,
                        ic.ITERATION_PATH AS PI_MATCH_PATH
                      FROM x
                      LEFT JOIN ADO_ITERATION_CALENDAR ic
                        ON UPPER(LTRIM(RTRIM(ic.ITERATION_PATH))) = UPPER(LTRIM(RTRIM(x.FEATURE_PI_PARENT_ITERATION_PATH)))
                       AND UPPER(LTRIM(RTRIM(ic.ITERATION_GRAIN))) = 'PI'
                    )
                    SELECT
                      COUNT(*) AS TOTAL_FEATURES,
                      SUM(CASE WHEN FEATURE_IS_SPRINT_LEVEL = 0 AND PI_MATCH_PATH IS NOT NULL THEN 1 ELSE 0 END) AS MAPPED_DIRECT,
                      SUM(CASE WHEN FEATURE_IS_SPRINT_LEVEL = 1 AND PI_MATCH_PATH IS NOT NULL THEN 1 ELSE 0 END) AS SPRINT_MAPPED,
                      SUM(CASE WHEN PI_MATCH_PATH IS NULL THEN 1 ELSE 0 END) AS MISSING_CALENDAR
                    FROM j
                    """
                )
                missing_count = (
                    int(df_breakdown.iloc[0]["MISSING_CALENDAR"])
                    if df_breakdown is not None and not df_breakdown.empty
                    else 0
                )
                st.metric("Features missing iteration mapping", missing_count)
                if df_breakdown is not None and not df_breakdown.empty:
                    st.dataframe(df_breakdown, use_container_width=True, height=80)

                df_top_missing = fetch_df(
                    """
                    WITH x AS (
                      SELECT
                        af.ITERATION_PATH AS FEATURE_ITERATION_PATH_RAW,
                        CASE
                          WHEN af.ITERATION_PATH IS NULL OR LTRIM(RTRIM(af.ITERATION_PATH)) = '' THEN 0
                          WHEN PATINDEX('%\\I[0-9] S[0-9]%', UPPER(af.ITERATION_PATH)) > 0 THEN 1
                          WHEN PATINDEX('%\\S[0-9]%', UPPER(af.ITERATION_PATH)) > 0 THEN 1
                          ELSE 0
                        END AS FEATURE_IS_SPRINT_LEVEL,
                        CASE
                          WHEN af.ITERATION_PATH IS NULL OR LTRIM(RTRIM(af.ITERATION_PATH)) = '' THEN NULL
                          WHEN (
                            PATINDEX('%\\I[0-9] S[0-9]%', UPPER(af.ITERATION_PATH)) > 0
                            OR PATINDEX('%\\S[0-9]%', UPPER(af.ITERATION_PATH)) > 0
                          )
                          AND CHARINDEX('\\', af.ITERATION_PATH) > 0
                          THEN LEFT(af.ITERATION_PATH, LEN(af.ITERATION_PATH) - CHARINDEX('\\', REVERSE(af.ITERATION_PATH)))
                          ELSE af.ITERATION_PATH
                        END AS FEATURE_PI_PARENT_ITERATION_PATH
                      FROM ADO_FEATURES af
                      WHERE af.ITERATION_PATH IS NOT NULL
                        AND LTRIM(RTRIM(af.ITERATION_PATH)) <> ''
                    )
                    SELECT TOP 20
                      x.FEATURE_PI_PARENT_ITERATION_PATH AS PI_PARENT_ITERATION_PATH,
                      COUNT(*) AS FEATURE_COUNT
                    FROM x
                    LEFT JOIN ADO_ITERATION_CALENDAR ic
                      ON UPPER(LTRIM(RTRIM(ic.ITERATION_PATH))) = UPPER(LTRIM(RTRIM(x.FEATURE_PI_PARENT_ITERATION_PATH)))
                     AND UPPER(LTRIM(RTRIM(ic.ITERATION_GRAIN))) = 'PI'
                    WHERE x.FEATURE_PI_PARENT_ITERATION_PATH IS NOT NULL
                      AND LTRIM(RTRIM(x.FEATURE_PI_PARENT_ITERATION_PATH)) <> ''
                      AND ic.ITERATION_PATH IS NULL
                    GROUP BY x.FEATURE_PI_PARENT_ITERATION_PATH
                    ORDER BY FEATURE_COUNT DESC, x.FEATURE_PI_PARENT_ITERATION_PATH
                    """
                )
                if df_top_missing is None or df_top_missing.empty:
                    st.caption("No missing PI parent iteration paths detected.")
                else:
                    st.caption("Top missing PI parent iteration paths")
                    st.dataframe(df_top_missing, use_container_width=True, height=360)

                df_top_sprints = fetch_df(
                    """
                    WITH x AS (
                      SELECT
                        af.ITERATION_PATH AS FEATURE_ITERATION_PATH_RAW,
                        CASE
                          WHEN af.ITERATION_PATH IS NULL OR LTRIM(RTRIM(af.ITERATION_PATH)) = '' THEN 0
                          WHEN PATINDEX('%\\I[0-9] S[0-9]%', UPPER(af.ITERATION_PATH)) > 0 THEN 1
                          WHEN PATINDEX('%\\S[0-9]%', UPPER(af.ITERATION_PATH)) > 0 THEN 1
                          ELSE 0
                        END AS FEATURE_IS_SPRINT_LEVEL,
                        CASE
                          WHEN af.ITERATION_PATH IS NULL OR LTRIM(RTRIM(af.ITERATION_PATH)) = '' THEN NULL
                          WHEN (
                            PATINDEX('%\\I[0-9] S[0-9]%', UPPER(af.ITERATION_PATH)) > 0
                            OR PATINDEX('%\\S[0-9]%', UPPER(af.ITERATION_PATH)) > 0
                          )
                          AND CHARINDEX('\\', af.ITERATION_PATH) > 0
                          THEN LEFT(af.ITERATION_PATH, LEN(af.ITERATION_PATH) - CHARINDEX('\\', REVERSE(af.ITERATION_PATH)))
                          ELSE af.ITERATION_PATH
                        END AS FEATURE_PI_PARENT_ITERATION_PATH
                      FROM ADO_FEATURES af
                      WHERE af.ITERATION_PATH IS NOT NULL
                        AND LTRIM(RTRIM(af.ITERATION_PATH)) <> ''
                    )
                    SELECT TOP 20
                      x.FEATURE_ITERATION_PATH_RAW AS SPRINT_ITERATION_PATH,
                      x.FEATURE_PI_PARENT_ITERATION_PATH AS PI_PARENT_ITERATION_PATH,
                      COUNT(*) AS FEATURE_COUNT
                    FROM x
                    WHERE x.FEATURE_IS_SPRINT_LEVEL = 1
                    GROUP BY x.FEATURE_ITERATION_PATH_RAW, x.FEATURE_PI_PARENT_ITERATION_PATH
                    ORDER BY FEATURE_COUNT DESC, x.FEATURE_ITERATION_PATH_RAW
                    """
                )
                if df_top_sprints is not None and not df_top_sprints.empty:
                    st.caption("Top sprint-level feature iteration paths (mapped to PI parent path)")
                    st.dataframe(df_top_sprints, use_container_width=True, height=360)
            except Exception as e:
                st.exception(e)

        # Schedule Automation (cron helper)
        with st.expander("Schedule automation (use saved portfolio settings)", expanded=False):
            st.caption("Generate a cron entry to run the CLI weekly. Set MSSQL_* and ADO_PAT in your cron environment.")
            dow_names = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
            c1, c2, c3 = st.columns([1,1,3])
            with c1:
                hour = st.number_input("Hour (24h)", min_value=0, max_value=23, value=2, step=1, key="cron_hour")
            with c2:
                minute = st.number_input("Minute", min_value=0, max_value=59, value=15, step=1, key="cron_minute")
            with c3:
                dow_label = st.selectbox("Day of week", options=dow_names, index=0, key="cron_dow")
            dow = ["0","1","2","3","4","5","6"][dow_names.index(dow_label)]

            py = sys.executable
            try:
                script_path = str((Path(__file__).resolve().parents[1] / "scripts" / "ado_sync.py").as_posix())
            except Exception:
                script_path = "scripts/ado_sync.py"
            selected_settings = st.session_state["ado_portfolio_settings"].get(portfolio_name or sel_portfolio or "(unsaved)", {})
            url_for_cmd = selected_settings.get("url") or full_url or "<paste-full-url>"
            max_pages_for_cmd = int(selected_settings.get("max_pages", max_pages))
            org_for_cmd = selected_settings.get("org", org or "YOUR_ORG")
            proj_for_cmd = selected_settings.get("project", proj or "YOUR_PROJECT")
            cron_line = f"{int(minute)} {int(hour)} * * {dow} ADO_PAT='<pat>' MSSQL_SERVER='<server>' MSSQL_DATABASE='<db>' MSSQL_USER='<user>' MSSQL_PASSWORD='<pass>' python {script_path} --url \"{url_for_cmd}\" --max-pages {max_pages_for_cmd} --org \"{org_for_cmd}\" --project \"{proj_for_cmd}\""
            st.code(cron_line)
            st.caption("Save your portfolio first, then use this cron snippet (or Logic App/Scheduler) to keep ADO_FEATURES fresh.")

        st.markdown("### Upload ADO export")
        st.caption("Upload **XLSX/XLSM/XLSB/XLS** or **CSV**. Only raw ADO fields are stored (including Year & Investment Dimension if present).")
        upl = st.file_uploader("Upload ADO export", type=["xlsx", "xlsm", "xlsb", "xls", "csv"], key="upl_ado")

        sheet_name: Optional[str] = None

        if upl:
            upl.seek(0); file_bytes = upl.read(); upl.seek(0)
            sheets = _list_excel_sheets(file_bytes)
            if sheets:
                sheet_name = st.selectbox("Worksheet", sheets, index=0, help="Choose the tab to import", key="sheet_select")
            else:
                st.info("Could not list sheets. I will try to read the first sheet automatically (or parse as CSV).")

            diag: Dict[str, Any] = {}
            if st.button("Parse file", icon=":material/description:", key="btn_parse"):
                upl.seek(0)
                try:
                    df_raw = read_ado_upload_any(upl, sheet_name, diag)
                except Exception as e:
                    with st.expander("Diagnostics", expanded=True):
                        st.write("**Why it failed**")
                        st.exception(e)
                        st.write("**Parse attempts**")
                        st.json(diag)
                    st.error("Could not parse the ADO file. Please install Excel engines (openpyxl/xlrd/pyxlsb) or re‑export as clean XLSX/CSV UTF‑8.")
                    st.stop()

                df_norm = normalize_to_canonical(df_raw)
                st.session_state["ado_parsed_raw"] = df_raw
                st.session_state["ado_parsed_norm"] = df_norm

                st.success(f"Parsed {len(df_raw)} rows from {upl.name} and normalized {len(df_norm)} rows.")

                # -------- Pre-merge diff preview (insert vs update) ----------
                try:
                    existing = fetch_df("SELECT FEATURE_ID FROM ADO_FEATURES")
                    existing_ids = set(existing["FEATURE_ID"].astype(str)) if not existing.empty else set()
                except Exception:
                    existing_ids = set()

                upload_ids = set(df_norm["FEATURE_ID"].astype(str))
                to_insert_ids = sorted(list(upload_ids - existing_ids))
                to_update_ids = sorted(list(upload_ids & existing_ids))
                invalid_rows = df_raw[df_raw.get("ID").isna()] if "ID" in df_raw.columns else pd.DataFrame()

                st.markdown("### What will happen")
                cA, cB, cC = st.columns(3)
                cA.metric("New (insert)", len(to_insert_ids))
                cB.metric("Existing (update)", len(to_update_ids))
                cC.metric("Missing IDs (skipped)", 0 if invalid_rows.empty else len(invalid_rows))

                with st.expander("Preview: New rows (by FEATURE_ID)", expanded=False):
                    st.dataframe(st.session_state["ado_parsed_norm"][st.session_state["ado_parsed_norm"]["FEATURE_ID"].isin(to_insert_ids)].head(300), use_container_width=True, height=300)
                with st.expander("Preview: Existing rows (by FEATURE_ID)", expanded=False):
                    st.dataframe(st.session_state["ado_parsed_norm"][st.session_state["ado_parsed_norm"]["FEATURE_ID"].isin(to_update_ids)].head(300), use_container_width=True, height=300)
                if not invalid_rows.empty:
                    with st.expander("Rows missing ID (will be skipped)", expanded=False):
                        st.dataframe(invalid_rows.head(300), use_container_width=True, height=240)

                with st.expander("Preview (raw + normalized)"):
                    show_all = st.checkbox("Show ALL rows (may be large)", value=False, key="show_all_rows")
                    preview = pd.concat(
                        [df_raw.reset_index(drop=True), df_norm.reset_index(drop=True)],
                        axis=1
                    )
                    st.dataframe((preview if show_all else preview.head(300)), use_container_width=True, height=600)

        if st.session_state["ado_parsed_norm"] is not None:
            if st.button("⬆️ Upsert into ADO_FEATURES", type="primary", key="btn_upsert"):
                def _resolve_ado_org_project() -> Tuple[str, str]:
                    o = (org or "").strip()
                    p = (proj or "").strip()
                    if o and p:
                        return o, p
                    try:
                        parsed = urlparse(full_url or "")
                        parts = [x for x in (parsed.path or "").strip("/").split("/") if x]
                        if len(parts) >= 2:
                            return parts[0], parts[1]
                    except Exception:
                        pass
                    return "", ""

                sync_iter_cal = bool(st.session_state.get("ado_sync_iteration_calendar", True))
                if sync_iter_cal:
                    org_res, proj_res = _resolve_ado_org_project()
                    if not (org_res and proj_res and pat):
                        st.error("Iteration calendar sync requires Organization, Project, and PAT.")
                        st.stop()
                    diag_iter: Dict[str, Any] = {}
                    with st.spinner("Syncing Iteration Calendar (ADO_ITERATION_CALENDAR)…"):
                        try:
                            df_iters = fetch_ado_iterations_odata(
                                org_res,
                                proj_res,
                                pat,
                                diag_iter,
                                year_prefixes=["2025", "2026", "2027"],
                                include_sprints=False,  # TODO: enable for PBIs/bugs if we ever need sprint-level timing
                                max_pages=int(max_pages or 20),
                            )
                            n_iters = upsert_ado_iteration_calendar(df_iters)
                        except Exception as e:
                            with st.expander("Iteration sync diagnostics", expanded=True):
                                st.exception(e)
                                st.json(diag_iter)
                            st.error("Iteration calendar sync failed; feature upsert stopped.")
                            st.stop()
                    try:
                        df_ic_meta2 = fetch_df("SELECT COUNT(*) AS N, MAX(UPDATED_AT) AS LAST_UPDATED FROM ADO_ITERATION_CALENDAR")
                        ic_n2 = int(df_ic_meta2.iloc[0]["N"]) if df_ic_meta2 is not None and not df_ic_meta2.empty else 0
                        ic_last2 = df_ic_meta2.iloc[0].get("LAST_UPDATED") if df_ic_meta2 is not None and not df_ic_meta2.empty else None
                    except Exception:
                        ic_n2, ic_last2 = None, None
                    st.success(f"Iterations synced: **{int(n_iters)}**")
                    st.caption(f"Iteration calendar last updated: {ic_last2 if ic_last2 is not None else '—'} (rows: {ic_n2 if ic_n2 is not None else '—'})")

                df_norm = st.session_state["ado_parsed_norm"]
                # capture pre-insert diff sets for reporting
                try:
                    existing = fetch_df("SELECT FEATURE_ID FROM ADO_FEATURES")
                    existing_ids = set(existing["FEATURE_ID"].astype(str)) if not existing.empty else set()
                except Exception:
                    existing_ids = set()
                upload_ids = set(df_norm["FEATURE_ID"].astype(str))
                to_insert_ids = sorted(list(upload_ids - existing_ids))
                to_update_ids = sorted(list(upload_ids & existing_ids))

                # Chunked upsert with progress bar
                total = len(df_norm)
                chunk_size = 1000 if total > 5000 else (500 if total > 2000 else 250)
                prog = st.progress(0, text="Starting upsert…")
                status = st.empty()
                n = 0
                for start in range(0, total, chunk_size):
                    stop = min(start + chunk_size, total)
                    chunk = df_norm.iloc[start:stop]
                    status.write(f"Upserting rows {start+1:,} to {stop:,} of {total:,}…")
                    try:
                        n += upsert_ado_features(chunk)
                    except Exception as e:
                        status.error(f"Chunk {start+1:,}-{stop:,} failed: {e}")
                        raise
                    prog.progress(stop / total, text=f"Upsert progress: {int((stop/total)*100)}%")
                prog.empty()
                status.write("Upsert complete.")
                info2 = fetch_df("SELECT COUNT(*) AS N FROM ADO_FEATURES")
                n_rows2 = int(info2.iloc[0]["N"]) if info2 is not None and not info2.empty else 0
                st.success(
                    f"Upserted **{n}** rows into ADO_FEATURES. "
                    f"Inserted: **{len(to_insert_ids)}**, Updated: **{len(to_update_ids)}**. "
                    f"New total rows: **{n_rows2}**. "
                    f"Iteration join keys (IterationPath/IterationSK) updated."
                )
                with st.expander("Show inserted IDs", expanded=False):
                    st.write(to_insert_ids[:500])
                with st.expander("Show updated IDs", expanded=False):
                    st.write(to_update_ids[:500])
        else:
            st.caption("Parse a file first to enable upsert.")

    # =========================
    # Tab: Map Values
    # =========================
    if ado_nav == "Mapping":
        st.subheader("ADO → NEXT Mapping")
        st.caption("These mappings connect raw ADO fields to NEXT Programs, Teams, and Applications.")
        st.info(
            "Why this matters: ADO features drive workforce cost allocation. If mappings are missing, costs land in "
            "unknown buckets or are under-reported, which reduces forecast accuracy."
        )

        cnt = fetch_df("SELECT COUNT(*) AS N FROM ADO_FEATURES")
        current_n = int(cnt.iloc[0]["N"]) if cnt is not None and not cnt.empty else 0
        if current_n == 0:
            st.warning("ADO_FEATURES is empty. Load and upsert data in the first tab.")
        else:
            st.info(f"ADO_FEATURES currently has **{current_n}** rows.")

        ado_programs = fetch_df("""
            SELECT DISTINCT PROGRAM_RAW
            FROM ADO_FEATURES
            WHERE PROGRAM_RAW IS NOT NULL AND LTRIM(RTRIM(PROGRAM_RAW)) <> ''
            ORDER BY PROGRAM_RAW
        """)
        ado_teams = fetch_df("""
            SELECT DISTINCT
                   TEAM_VARIANT_KEY,
                   TEAM_RAW,
                   PROGRAM_RAW,
                   AREA_LEVEL3_RAW,
                   AREA_LEVEL4_RAW
            FROM ADO_FEATURES
            WHERE TEAM_VARIANT_KEY IS NOT NULL AND LTRIM(RTRIM(TEAM_VARIANT_KEY)) <> ''
            ORDER BY TEAM_VARIANT_KEY
        """)
        ado_apps = fetch_df("""
            SELECT DISTINCT APP_NAME_RAW
            FROM ADO_FEATURES
            WHERE APP_NAME_RAW IS NOT NULL AND APP_NAME_RAW <> ''
            ORDER BY APP_NAME_RAW
        """)

        program_maps = fetch_df("SELECT ADO_PROGRAM, PROGRAMID FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM ORDER BY ADO_PROGRAM")
        team_maps    = fetch_df("""
            SELECT
              mt.ADO_TEAM_KEY,
              mt.ADO_TEAM,
              mt.PROGRAM_RAW,
              mt.AREA_LEVEL3_RAW,
              mt.AREA_LEVEL4_RAW,
              mt.TEAMID,
              t.PROGRAMID
            FROM MAP_ADO_TEAM_TO_TCO_TEAM mt
            LEFT JOIN TEAMS t ON t.TEAMID = mt.TEAMID
            ORDER BY mt.ADO_TEAM_KEY
        """)
        app_maps     = fetch_df("SELECT ADO_APP, APP_GROUP FROM MAP_ADO_APP_TO_TCO_GROUP ORDER BY ADO_APP")

        programs_df = fetch_df("SELECT PROGRAMID, PROGRAMNAME FROM PROGRAMS ORDER BY PROGRAMNAME")
        teams_df    = fetch_df("SELECT TEAMID, TEAMNAME FROM TEAMS ORDER BY TEAMNAME")
        groups_df   = fetch_df("SELECT GROUPID, GROUPNAME FROM APPLICATION_GROUPS ORDER BY GROUPNAME")

        def _mapped_stats(values_df: Optional[pd.DataFrame], key_col: str, maps_df: Optional[pd.DataFrame], map_key: str, map_val: str) -> Tuple[int, int]:
            if values_df is None or values_df.empty or key_col not in values_df.columns:
                return 0, 0
            vals = values_df[key_col].dropna().astype(str).str.strip()
            vals = vals[vals != ""]
            total = vals.nunique()
            if maps_df is None or maps_df.empty or map_key not in maps_df.columns or map_val not in maps_df.columns:
                return total, 0
            mapped_keys = maps_df.loc[maps_df[map_val].notna(), map_key].dropna().astype(str).str.strip()
            mapped_keys = mapped_keys[mapped_keys != ""]
            mapped = len(set(vals.str.upper()) & set(mapped_keys.str.upper()))
            return total, mapped

        st.markdown("### Coverage snapshot")
        prog_total, prog_mapped = _mapped_stats(ado_programs, "PROGRAM_RAW", program_maps, "ADO_PROGRAM", "PROGRAMID")
        team_total, team_mapped = _mapped_stats(ado_teams, "TEAM_VARIANT_KEY", team_maps, "ADO_TEAM_KEY", "TEAMID")
        app_total, app_mapped = _mapped_stats(ado_apps, "APP_NAME_RAW", app_maps, "ADO_APP", "APP_GROUP")
        c1, c2, c3 = st.columns(3)
        c1.metric("Programs mapped", f"{prog_mapped}/{prog_total}")
        c2.metric("Teams mapped", f"{team_mapped}/{team_total}")
        c3.metric("Applications mapped", f"{app_mapped}/{app_total}")

        with st.expander("ADO app candidates (discovery)", expanded=False):
            st.caption(
                "Stages distinct ADO app names from ADO_FEATURES into ADO_APP_CANDIDATES with matching suggestions. "
                "This does not write MAP_ADO_APP_TO_TCO_GROUP until you explicitly save mappings in the editor below."
            )
            updated_by_email = str(st.session_state.get("auth_user", {}).get("email") or "").strip() or None

            b1, b2 = st.columns([1, 1])
            with b1:
                if st.button("Refresh candidates", icon=":material/sync:", key="ado_candidates_refresh"):
                    try:
                        ensure_ado_minimal_tables()
                        res = refresh_ado_app_candidates(fetch_df, execute)
                        st.success(
                            f"ADO candidates refreshed: {res.new} new, {res.updated} updated, {res.need_review} need review."
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Refresh failed: {e}")
            with b2:
                if st.button(
                    "Repair default application instances",
                    icon=":material/auto_fix_high:",
                    key="btn_repair_default_instances",
                ):
                    try:
                        r = ensure_default_instances(execute, fetch_df)
                        st.success(
                            "Default instances ensured. "
                            f"Created: {r.groups_created}, Updated: {r.groups_updated}, Repaired multi-default: {r.groups_repaired_multi_default}."
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Repair failed: {e}")

            show_all_candidates = st.checkbox("Show all candidate statuses", value=False, key="ado_candidates_show_all")
            try:
                try:
                    ensure_ado_minimal_tables()
                except Exception:
                    pass
                df_candidates = fetch_df(
                    """
                    SELECT
                      ADO_APP_RAW,
                      STATUS,
                      FEATURE_COUNT,
                      DERIVED_FTE_SUM,
                      FIRST_SEEN,
                      LAST_SEEN,
                      COALESCE(SUGGESTED_GROUP_NAME, SUGGESTED_TCO_GROUP) AS SUGGESTED_GROUP_NAME,
                      COALESCE(SUGGESTED_GROUP_ID, SUGGESTED_TCO_GROUP_ID) AS SUGGESTED_GROUP_ID,
                      PROGRAMS_SEEN,
                      UPDATED_AT
                    FROM ADO_APP_CANDIDATES
                    ORDER BY
                      CASE STATUS
                        WHEN 'NEEDS_REVIEW' THEN 0
                        WHEN 'NEW' THEN 1
                        WHEN 'AUTO_SUGGESTED' THEN 2
                        WHEN 'MAPPED' THEN 3
                        WHEN 'IGNORED' THEN 4
                        ELSE 9
                      END,
                      COALESCE(FEATURE_COUNT, 0) DESC,
                      COALESCE(DERIVED_FTE_SUM, 0) DESC
                    """,
                    None,
                )
            except Exception:
                df_candidates = None

            if df_candidates is None or df_candidates.empty:
                st.caption("No candidate rows found yet. Click “Refresh candidates” to populate ADO_APP_CANDIDATES.")
            else:
                df_candidates = df_candidates.copy()
                if "STATUS" in df_candidates.columns:
                    df_candidates["STATUS"] = (
                        df_candidates["STATUS"].astype(str).str.upper().replace({"AUTO_MAPPED": "AUTO_SUGGESTED"})
                    )

                try:
                    status_counts = df_candidates["STATUS"].fillna("").astype(str).str.upper().value_counts().to_dict()
                except Exception:
                    status_counts = {}
                k1, k2, k3, k4, k5 = st.columns(5)
                k1.metric("Total candidates", int(len(df_candidates)))
                k2.metric("Needs review", int(status_counts.get("NEEDS_REVIEW", 0)))
                k3.metric("Auto-suggested", int(status_counts.get("AUTO_SUGGESTED", 0)))
                k4.metric("Mapped", int(status_counts.get("MAPPED", 0)))
                k5.metric("Ignored", int(status_counts.get("IGNORED", 0)))

                tab_queue, tab_manage = st.tabs(["Review queue", "Manage statuses"])

                with tab_queue:
                    queue_df = df_candidates.copy()
                    if "STATUS" in queue_df.columns:
                        queue_df = queue_df[queue_df["STATUS"].isin(["NEW", "AUTO_SUGGESTED", "NEEDS_REVIEW"])].copy()
                    if queue_df.empty:
                        st.caption("No NEW/AUTO_SUGGESTED/NEEDS_REVIEW candidates for the current data set.")
                    else:
                        st.dataframe(
                                queue_df[
                                    [
                                        c
                                        for c in [
                                            "ADO_APP_RAW",
                                            "STATUS",
                                            "FEATURE_COUNT",
                                            "DERIVED_FTE_SUM",
                                            "LAST_SEEN",
                                            "SUGGESTED_GROUP_NAME",
                                            "PROGRAMS_SEEN",
                                        ]
                                    if c in queue_df.columns
                                ]
                            ],
                            use_container_width=True,
                            height=280,
                            hide_index=True,
                        )

                        ado_pick = st.selectbox(
                            "Select candidate ADO app",
                            options=["(Select)"] + queue_df["ADO_APP_RAW"].astype(str).tolist(),
                            index=0,
                            key="ado_candidate_pick",
                        )
                        if ado_pick and ado_pick != "(Select)":
                            row0 = queue_df.loc[queue_df["ADO_APP_RAW"].astype(str) == str(ado_pick)].iloc[0]
                            suggested_name = str(row0.get("SUGGESTED_GROUP_NAME") or "").strip()
                            st.caption(f"Suggested group: {suggested_name or '—'}")

                            groups_df2 = fetch_df("SELECT GROUPID, GROUPNAME FROM APPLICATION_GROUPS ORDER BY GROUPNAME")
                            name_to_id_group2 = {}
                            group_opts = []
                            if groups_df2 is not None and not groups_df2.empty:
                                group_opts = groups_df2["GROUPNAME"].astype(str).tolist()
                                name_to_id_group2 = {str(r.GROUPNAME): str(r.GROUPID) for _, r in groups_df2.iterrows()}

                            col_map, col_create, col_ignore = st.columns([2, 2, 1])
                            with col_map:
                                sel_gname = st.selectbox(
                                    "Map to existing app group",
                                    options=["(Select)"] + group_opts,
                                    index=(1 + group_opts.index(suggested_name) if suggested_name in group_opts else 0),
                                    key="ado_candidate_map_group",
                                )
                                if st.button(
                                    "Apply mapping",
                                    icon=":material/link:",
                                    key="ado_candidate_apply_map",
                                    disabled=(sel_gname == "(Select)"),
                                ):
                                    try:
                                        gid = name_to_id_group2.get(sel_gname)
                                        execute(
                                            """
                                            MERGE INTO MAP_ADO_APP_TO_TCO_GROUP t
                                            USING (SELECT %s AS ADO_APP, %s AS APP_GROUP) s
                                            ON t.ADO_APP = s.ADO_APP
                                            WHEN MATCHED THEN UPDATE SET APP_GROUP = s.APP_GROUP
                                            WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP);
                                            """,
                                            (str(ado_pick), str(gid)),
                                        )
                                        execute(
                                            """
                                            UPDATE ADO_APP_CANDIDATES
                                            SET STATUS='MAPPED',
                                                SUGGESTED_GROUP_ID=%s,
                                                SUGGESTED_GROUP_NAME=%s,
                                                SUGGESTED_TCO_GROUP_ID=%s,
                                                SUGGESTED_TCO_GROUP=%s,
                                                UPDATED_AT=SYSDATETIME()
                                            WHERE ADO_APP_RAW=%s
                                            """,
                                            (gid, sel_gname, gid, sel_gname, str(ado_pick)),
                                        )
                                        toast_success("Mapping saved and candidate marked MAPPED.")
                                        st.cache_data.clear()
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Mapping failed: {e}")

                            with col_create:
                                with st.expander("Create new app group", expanded=False):
                                    vendors_df = list_vendors()
                                    vendor_opts = (
                                        vendors_df["VENDORNAME"].astype(str).tolist()
                                        if vendors_df is not None and not vendors_df.empty
                                        else []
                                    )
                                    vname = st.selectbox(
                                        "Vendor (required)",
                                        options=["(Select)"] + vendor_opts,
                                        index=0,
                                        key="ado_candidate_vendor",
                                    )
                                    new_group_name = st.text_input(
                                        "New Application name",
                                        value=str(ado_pick),
                                        key="ado_candidate_new_group_name",
                                    )
                                    if st.button(
                                        "Create group + map",
                                        icon=":material/add_circle:",
                                        key="ado_candidate_create_group_map",
                                        disabled=(vname == "(Select)") or (not str(new_group_name or "").strip()),
                                    ):
                                        try:
                                            vendor_id = None
                                            if vendors_df is not None and not vendors_df.empty and vname != "(Select)":
                                                vendor_id = str(
                                                    vendors_df.loc[
                                                        vendors_df["VENDORNAME"].astype(str) == str(vname),
                                                        "VENDORID",
                                                    ].iloc[0]
                                                )
                                            group_id = str(uuid.uuid4())
                                            upsert_application_group(
                                                group_id,
                                                str(new_group_name).strip(),
                                                team_id="",
                                                default_vendor_id=vendor_id,
                                                owner=None,
                                                updated_by=updated_by_email,
                                            )
                                            try:
                                                ensure_default_instances(execute, fetch_df)
                                            except Exception:
                                                pass
                                            execute(
                                                """
                                                MERGE INTO MAP_ADO_APP_TO_TCO_GROUP t
                                                USING (SELECT %s AS ADO_APP, %s AS APP_GROUP) s
                                                ON t.ADO_APP = s.ADO_APP
                                                WHEN MATCHED THEN UPDATE SET APP_GROUP = s.APP_GROUP
                                                WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP);
                                                """,
                                                (str(ado_pick), group_id),
                                            )
                                            execute(
                                                """
                                                UPDATE ADO_APP_CANDIDATES
                                                SET STATUS='MAPPED',
                                                    SUGGESTED_GROUP_ID=%s,
                                                    SUGGESTED_GROUP_NAME=%s,
                                                    SUGGESTED_TCO_GROUP_ID=%s,
                                                    SUGGESTED_TCO_GROUP=%s,
                                                    UPDATED_AT=SYSDATETIME()
                                                WHERE ADO_APP_RAW=%s
                                                """,
                                                (
                                                    group_id,
                                                    str(new_group_name).strip(),
                                                    group_id,
                                                    str(new_group_name).strip(),
                                                    str(ado_pick),
                                                ),
                                            )
                                            toast_success("Application created and mapping saved.")
                                            st.cache_data.clear()
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Create+map failed: {e}")

                            with col_ignore:
                                if st.button("Ignore", icon=":material/block:", key="ado_candidate_ignore"):
                                    try:
                                        execute(
                                            "UPDATE ADO_APP_CANDIDATES SET STATUS='IGNORED', UPDATED_AT=SYSDATETIME() WHERE ADO_APP_RAW=%s",
                                            (str(ado_pick),),
                                        )
                                        toast_success("Candidate marked IGNORED.")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Ignore failed: {e}")

                        st.caption(
                            "These candidates are ADO app names that appear in features but are not yet mapped to a NEXT Application. "
                            "Mapping them reduces '(Needs mapping)' / Unassigned buckets in analytics."
                        )

                with tab_manage:
                    df_manage = df_candidates.copy()
                    if (not show_all_candidates) and "STATUS" in df_manage.columns:
                        df_manage = df_manage[df_manage["STATUS"].isin(["NEW", "AUTO_SUGGESTED", "NEEDS_REVIEW"])].copy()

                    if df_manage.empty:
                        st.caption("No candidates to show for the current filter.")
                    else:
                        edited_candidates = st.data_editor(
                            df_manage,
                            use_container_width=True,
                            height=320,
                            num_rows="fixed",
                            column_config={
                                "ADO_APP_RAW": st.column_config.TextColumn("ADO App (raw)", disabled=True),
                                "STATUS": st.column_config.SelectboxColumn(
                                    "Status",
                                    options=["NEW", "AUTO_SUGGESTED", "NEEDS_REVIEW", "IGNORED", "MAPPED"],
                                    required=True,
                                ),
                                "SUGGESTED_GROUP_NAME": st.column_config.TextColumn("Suggested Group", disabled=True),
                                "SUGGESTED_GROUP_ID": st.column_config.TextColumn("Suggested Group ID", disabled=True),
                            },
                            key="ado_candidates_editor",
                        )
                        if st.button("Save candidate statuses", icon=":material/save:", key="ado_candidates_save_status"):
                            try:
                                rows = []
                                for _, r in edited_candidates.iterrows():
                                    ado_app = str(r.get("ADO_APP_RAW") or "").strip()
                                    status = str(r.get("STATUS") or "").strip().upper()
                                    if ado_app and status:
                                        rows.append((status, ado_app))
                                if rows:
                                    execute(
                                        "UPDATE ADO_APP_CANDIDATES SET STATUS = %s, UPDATED_AT = SYSDATETIME() WHERE ADO_APP_RAW = %s",
                                        rows,
                                        many=True,
                                    )
                                toast_success("Candidate statuses saved.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Save failed: {e}")

        with st.expander("Mapping impact (features missing mapping)", expanded=False):
            try:
                impact_df = fetch_df("""
                    SELECT
                      COUNT(*) AS FEATURES_TOTAL,
                      SUM(CASE WHEN mp.PROGRAMID IS NULL THEN 1 ELSE 0 END) AS MISSING_PROGRAM,
                      SUM(CASE WHEN mt.TEAMID IS NULL THEN 1 ELSE 0 END) AS MISSING_TEAM,
                      SUM(CASE WHEN mag.APP_GROUP IS NULL THEN 1 ELSE 0 END) AS MISSING_APP_GROUP
                    FROM ADO_FEATURES af
                    LEFT JOIN MAP_ADO_PROGRAM_TO_TCO_PROGRAM mp ON UPPER(mp.ADO_PROGRAM) = UPPER(af.PROGRAM_RAW)
                    LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM mt ON mt.ADO_TEAM_KEY = af.TEAM_VARIANT_KEY
                    LEFT JOIN MAP_ADO_APP_TO_TCO_GROUP mag ON mag.ADO_APP = af.APP_NAME_RAW
                """)
                if impact_df is not None and not impact_df.empty:
                    row = impact_df.iloc[0]
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Total features", int(row.get("FEATURES_TOTAL") or 0))
                    c2.metric("Missing program map", int(row.get("MISSING_PROGRAM") or 0))
                    c3.metric("Missing team map", int(row.get("MISSING_TEAM") or 0))
                    c4.metric("Missing app map", int(row.get("MISSING_APP_GROUP") or 0))
            except Exception:
                st.caption("Impact metrics unavailable.")

        guided_mode = st.toggle("Guided mode", value=True, key="ado_map_guided")

        st.markdown("### ADO → NEXT Program")
        st.caption("Maps ADO Area Level 2 (Program) to a NEXT Program. Impacts program rollups and cost distribution.")
        if ado_programs is None or ado_programs.empty or programs_df is None or programs_df.empty:
            st.info("Need ADO program values and Programs in NEXT before mapping.")
        else:
            if guided_mode:
                prog_status = "Complete" if prog_total and (prog_mapped >= prog_total) else f"{prog_mapped}/{prog_total} mapped"
                st.caption(f"Step 1 of 3 • Program mapping status: {prog_status}")
                show_unmapped_prog = st.checkbox("Show unmapped programs only", value=True, key="pm_show_unmapped")
            else:
                show_unmapped_prog = st.checkbox("Show unmapped programs only", value=False, key="pm_show_unmapped")
            base_pm = ado_programs.rename(columns={"PROGRAM_RAW": "ADO_PROGRAM"}).copy()
            if program_maps is not None and not program_maps.empty:
                base_pm = base_pm.merge(program_maps, how="left", on="ADO_PROGRAM")
            else:
                base_pm["PROGRAMID"] = None
            if show_unmapped_prog:
                base_pm = base_pm[base_pm["PROGRAMID"].isna()].copy()

            id_to_name_program = {r.PROGRAMID: r.PROGRAMNAME for _, r in programs_df.iterrows()}
            name_to_id_program = {r.PROGRAMNAME: r.PROGRAMID for _, r in programs_df.iterrows()}
            base_pm["TCO_PROGRAMNAME"] = base_pm["PROGRAMID"].map(id_to_name_program)

            edited_prog = st.data_editor(
                base_pm[["ADO_PROGRAM", "TCO_PROGRAMNAME"]],
                use_container_width=True,
                height=240,
                num_rows="fixed",
                column_config={
                    "ADO_PROGRAM": st.column_config.TextColumn("ADO Program (Area Level 2)", disabled=True),
                    "TCO_PROGRAMNAME": st.column_config.SelectboxColumn(
                        "NEXT Program",
                        options=programs_df["PROGRAMNAME"].tolist(),
                        required=False,
                    ),
                },
                key="pm_editor",
            )
            if st.button("Save Program Mappings", icon=":material/save:", key="btn_save_program_mappings"):
                rows_to_upsert_pm: List[Tuple[str, str]] = []
                for _, row in edited_prog.iterrows():
                    ado_val = str(row["ADO_PROGRAM"]).strip()
                    pname = row.get("TCO_PROGRAMNAME")
                    if _blank_or_nan(ado_val):
                        continue
                    if pname and pname in name_to_id_program:
                        rows_to_upsert_pm.append((ado_val, name_to_id_program[pname]))
                    else:
                        execute("DELETE FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM WHERE ADO_PROGRAM = %s", (ado_val,))
                if rows_to_upsert_pm:
                    merge_sql = """
                    MERGE INTO MAP_ADO_PROGRAM_TO_TCO_PROGRAM t
                    USING (SELECT %s AS ADO_PROGRAM, %s AS PROGRAMID) s
                    ON t.ADO_PROGRAM = s.ADO_PROGRAM
                    WHEN MATCHED THEN UPDATE SET PROGRAMID = s.PROGRAMID
                    WHEN NOT MATCHED THEN INSERT (ADO_PROGRAM, PROGRAMID) VALUES (s.ADO_PROGRAM, s.PROGRAMID);
                    """
                    execute(merge_sql, rows_to_upsert_pm, many=True)
                toast_success("Program mappings saved.")

        st.markdown("---")

        st.markdown("### ADO → NEXT Team")
        st.caption("Links raw ADO team variants to a NEXT Team. Missing team mapping causes workforce costs to land in unknown teams.")
        if ado_teams is None or ado_teams.empty or teams_df is None or teams_df.empty:
            st.info("Load features (with Area levels) and ensure Teams exist in NEXT before mapping.")
        else:
            if guided_mode:
                team_status = "Complete" if team_total and (team_mapped >= team_total) else f"{team_mapped}/{team_total} mapped"
                st.caption(f"Step 2 of 3 • Team mapping status: {team_status}")
                show_unmapped_team = st.checkbox("Show unmapped teams only", value=True, key="tm_show_unmapped")
            else:
                show_unmapped_team = st.checkbox("Show unmapped teams only", value=False, key="tm_show_unmapped")
            base_tm = ado_teams.rename(columns={"TEAM_VARIANT_KEY": "ADO_TEAM_KEY"}).copy()
            if not base_tm.empty:
                base_tm = base_tm.sort_values(
                    ["ADO_TEAM_KEY", "PROGRAM_RAW", "TEAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW"],
                    na_position="last"
                ).drop_duplicates(
                    subset=["ADO_TEAM_KEY", "PROGRAM_RAW", "TEAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW"],
                    keep="first"
                ).reset_index(drop=True)
            if team_maps is not None and not team_maps.empty:
                base_tm = base_tm.merge(team_maps, how="left", on="ADO_TEAM_KEY", suffixes=("", "_MAP"))
                for col in ["ADO_TEAM", "PROGRAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW", "TEAMID", "PROGRAMID"]:
                    map_col = f"{col}_MAP"
                    if map_col in base_tm.columns:
                        base_tm[col] = base_tm[col].fillna(base_tm[map_col])
                        base_tm.drop(columns=[map_col], inplace=True)
            else:
                base_tm["TEAMID"] = None
                base_tm["PROGRAMID"] = None

            for col in ["PROGRAM_RAW", "TEAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW", "ADO_TEAM", "TEAMID", "PROGRAMID"]:
                if col not in base_tm.columns:
                    base_tm[col] = None

            # Fill PROGRAMID from ADO Program mapping when Team is not mapped
            prog_map_lookup = {}
            if program_maps is not None and not program_maps.empty:
                prog_map_lookup = {str(r.ADO_PROGRAM).upper(): r.PROGRAMID for _, r in program_maps.iterrows() if pd.notna(r.ADO_PROGRAM)}
            if prog_map_lookup:
                base_tm["PROGRAMID"] = base_tm["PROGRAMID"].fillna(
                    base_tm["PROGRAM_RAW"].apply(lambda x: prog_map_lookup.get(str(x).upper()) if pd.notna(x) else None)
                )

            base_tm["TEAM_DISPLAY"] = (
                base_tm["AREA_LEVEL4_RAW"].fillna(base_tm["TEAM_RAW"])
                .fillna(base_tm["AREA_LEVEL3_RAW"])
                .fillna(base_tm["PROGRAM_RAW"])
            )
            base_tm["TEAM_DISPLAY"] = base_tm["TEAM_DISPLAY"].fillna("")

            meta_lookup = base_tm.set_index("ADO_TEAM_KEY")[
                ["PROGRAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW", "TEAM_RAW", "ADO_TEAM"]
            ].to_dict("index")

            id_to_name_team = {r.TEAMID: r.TEAMNAME for _, r in teams_df.iterrows()}
            name_to_id_team = {r.TEAMNAME: r.TEAMID for _, r in teams_df.iterrows()}
            base_tm["TCO_TEAMNAME"] = base_tm["TEAMID"].map(id_to_name_team)
            base_tm["TCO_PROGRAMNAME"] = base_tm["PROGRAMID"].map(id_to_name_program) if "PROGRAMID" in base_tm.columns else None
            if show_unmapped_team:
                base_tm = base_tm[base_tm["TEAMID"].isna()].copy()

            edited = st.data_editor(
                base_tm[["ADO_TEAM_KEY", "PROGRAM_RAW", "TEAM_DISPLAY", "TCO_TEAMNAME", "TCO_PROGRAMNAME"]],
                use_container_width=True,
                height=360,
                num_rows="fixed",
                column_config={
                    "ADO_TEAM_KEY": st.column_config.TextColumn("ADO Team Variant Key", disabled=True),
                    "PROGRAM_RAW": st.column_config.TextColumn("Area Level 2", disabled=True),
                    "TEAM_DISPLAY": st.column_config.TextColumn("ADO Area (Team)", disabled=True),
                    "TCO_PROGRAMNAME": st.column_config.TextColumn("NEXT Program (from mapping)", disabled=True),
                    "TCO_TEAMNAME": st.column_config.SelectboxColumn(
                        "NEXT Team",
                        options=teams_df["TEAMNAME"].tolist(),
                        required=False,
                    ),
                },
                key="tm_editor",
            )
            if st.button("Save Team Mappings", icon=":material/save:", key="btn_save_team_mappings"):
                def _clean(val: Any) -> Optional[str]:
                    if val is None:
                        return None
                    sval = str(val).strip()
                    return sval if sval else None

                rows_to_upsert: List[Tuple[str, str, Optional[str], Optional[str], Optional[str], Optional[str]]] = []
                for _, row in edited.iterrows():
                    ado_val = str(row["ADO_TEAM_KEY"]).strip()
                    tname = row.get("TCO_TEAMNAME")
                    if _blank_or_nan(ado_val):
                        continue
                    meta = meta_lookup.get(ado_val, {})
                    if tname and tname in name_to_id_team:
                        rows_to_upsert.append((
                            ado_val,
                            name_to_id_team[tname],
                            _clean(row.get("PROGRAM_RAW")) or _clean(meta.get("PROGRAM_RAW")),
                            _clean(meta.get("AREA_LEVEL3_RAW")),
                            _clean(meta.get("AREA_LEVEL4_RAW")),
                            _clean(meta.get("TEAM_RAW")) or _clean(meta.get("ADO_TEAM")),
                        ))
                    else:
                        execute("DELETE FROM MAP_ADO_TEAM_TO_TCO_TEAM WHERE ADO_TEAM_KEY = %s", (ado_val,))
                if rows_to_upsert:
                    merge_sql = """
                    MERGE INTO MAP_ADO_TEAM_TO_TCO_TEAM t
                    USING (SELECT %s AS ADO_TEAM_KEY, %s AS TEAMID, %s AS PROGRAM_RAW, %s AS AREA_LEVEL3_RAW, %s AS AREA_LEVEL4_RAW, %s AS ADO_TEAM) s
                    ON t.ADO_TEAM_KEY = s.ADO_TEAM_KEY
                    WHEN MATCHED THEN UPDATE SET TEAMID = s.TEAMID, PROGRAM_RAW = s.PROGRAM_RAW, AREA_LEVEL3_RAW = s.AREA_LEVEL3_RAW, AREA_LEVEL4_RAW = s.AREA_LEVEL4_RAW, ADO_TEAM = s.ADO_TEAM
                    WHEN NOT MATCHED THEN INSERT (ADO_TEAM_KEY, TEAMID, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, ADO_TEAM) VALUES (s.ADO_TEAM_KEY, s.TEAMID, s.PROGRAM_RAW, s.AREA_LEVEL3_RAW, s.AREA_LEVEL4_RAW, s.ADO_TEAM);
                    """
                    execute(merge_sql, rows_to_upsert, many=True)
                toast_success("Team mappings saved.")

        st.markdown("---")

        st.markdown("### ADO → NEXT Application")
        st.caption("Maps ADO Application Name to an Application. Required for application-level cost distribution.")
        if ado_apps is None or ado_apps.empty or groups_df is None or groups_df.empty:
            st.info("Load features and create Applications first.")
        else:
            if guided_mode:
                app_status = "Complete" if app_total and (app_mapped >= app_total) else f"{app_mapped}/{app_total} mapped"
                st.caption(f"Step 3 of 3 • Application mapping status: {app_status}")
                show_unmapped_app = st.checkbox("Show unmapped app groups only", value=True, key="am_show_unmapped")
            else:
                show_unmapped_app = st.checkbox("Show unmapped app groups only", value=False, key="am_show_unmapped")
            base_am = ado_apps.rename(columns={"APP_NAME_RAW": "ADO_APP"}).copy()
            if app_maps is not None and not app_maps.empty:
                base_am = base_am.merge(app_maps, how="left", on="ADO_APP")
            else:
                base_am["APP_GROUP"] = None
            if show_unmapped_app:
                base_am = base_am[base_am["APP_GROUP"].isna()].copy()

            id_to_name_group = {r.GROUPID: r.GROUPNAME for _, r in groups_df.iterrows()}
            name_to_id_group = {r.GROUPNAME: r.GROUPID for _, r in groups_df.iterrows()}
            base_am["TCO_GROUPNAME"] = base_am["APP_GROUP"].map(id_to_name_group)
            # Pre-fill unmapped apps with high-confidence suggestions from ADO_APP_CANDIDATES (still requires explicit save).
            try:
                cand = fetch_df(
                    """
                    SELECT
                      ADO_APP_RAW,
                      STATUS,
                      COALESCE(SUGGESTED_GROUP_ID, SUGGESTED_TCO_GROUP_ID) AS SUGGESTED_GROUP_ID
                    FROM ADO_APP_CANDIDATES
                    """,
                    None,
                )
            except Exception:
                cand = None
            if cand is not None and not cand.empty:
                cand = cand.copy()
                cand["ADO_APP_RAW"] = cand["ADO_APP_RAW"].astype(str).str.strip()
                cand["STATUS"] = cand["STATUS"].astype(str).str.upper().replace({"AUTO_MAPPED": "AUTO_SUGGESTED"})
                cand["SUGGESTED_GROUP_ID"] = cand["SUGGESTED_GROUP_ID"].astype(str).str.strip()
                base_am = base_am.merge(cand, how="left", left_on="ADO_APP", right_on="ADO_APP_RAW")
                base_am.drop(columns=["ADO_APP_RAW"], inplace=True, errors="ignore")
                auto_mask = (
                    base_am.get("STATUS", "").astype(str).str.upper().eq("AUTO_SUGGESTED")
                    & base_am.get("TCO_GROUPNAME").isna()
                    & base_am.get("SUGGESTED_GROUP_ID", "").astype(str).str.strip().ne("")
                )
                if auto_mask.any():
                    base_am.loc[auto_mask, "TCO_GROUPNAME"] = base_am.loc[auto_mask, "SUGGESTED_GROUP_ID"].map(id_to_name_group)

            edited2 = st.data_editor(
                base_am[["ADO_APP","TCO_GROUPNAME"]],
                use_container_width=True,
                height=360,
                num_rows="fixed",
                column_config={
                    "ADO_APP": st.column_config.TextColumn("ADO App Name", disabled=True),
                    "TCO_GROUPNAME": st.column_config.SelectboxColumn(
                        "NEXT Application",
                        options=groups_df["GROUPNAME"].tolist() if groups_df is not None and not groups_df.empty else [],
                        required=False,
                        help="This picker shows group names. The mapping will store the group ID."
                    ),
                },
                key="am_editor",
            )
            if st.button("Save Application Mappings", icon=":material/save:", key="btn_save_app_group_mappings"):
                rows_to_upsert: List[Tuple[str, str]] = []
                for _, row in edited2.iterrows():
                    ado_val = str(row["ADO_APP"]).strip()
                    gname = row.get("TCO_GROUPNAME")
                    if _blank_or_nan(ado_val):
                        continue
                    if gname and gname in name_to_id_group:
                        gid = name_to_id_group[gname]
                        rows_to_upsert.append((ado_val, gid))
                    else:
                        execute("DELETE FROM MAP_ADO_APP_TO_TCO_GROUP WHERE ADO_APP = %s", (ado_val,))
                if rows_to_upsert:
                    merge_sql = """
                    MERGE INTO MAP_ADO_APP_TO_TCO_GROUP t
                    USING (SELECT %s AS ADO_APP, %s AS APP_GROUP) s
                    ON t.ADO_APP = s.ADO_APP
                    WHEN MATCHED THEN UPDATE SET APP_GROUP = s.APP_GROUP
                    WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP);
                    """
                    execute(merge_sql, rows_to_upsert, many=True)
                toast_success("Application mappings saved.")

    # =========================
    # Tab: 🔎 ADO Explorer
    # =========================
    if ado_nav == "Explorer":
        st.subheader("ADO Explorer")
        st.caption("Browse raw ADO imports and demand diagnostics. Filters apply to all sections.")

        tab_explore_po, = st.tabs(["Explorer v2 (PO view)"])

        if False:
            with st.expander("Maintenance", expanded=False):
                st.caption("No maintenance actions are exposed for ADO Explorer.")

            df_teams, df_apps, df_iters = load_ado_distincts()
            c1, c2, c3 = st.columns(3)
            c1.metric("Distinct ADO Teams", len(df_teams))
            c2.metric("Distinct ADO Apps", len(df_apps))
            c3.metric("Distinct Iterations", len(df_iters))

            with st.expander("Filters", expanded=True):
                f1, f2, f3 = st.columns([2, 2, 2])
                team_like = f1.text_input("Team contains (raw)", "", key="exp_team_contains")
                app_like = f2.text_input("App contains", "", key="exp_app_contains")
                iter_like = f3.text_input("Iteration contains", "", key="exp_iter_contains")
                st.caption("Filters match raw ADO fields; mapped names may display differently.")

            where: List[str] = []
            params: List[str] = []
            if team_like.strip():
                where.append("UPPER(TEAM_RAW) LIKE UPPER(%s)")
                params.append(f"%{team_like.strip()}%")
            if app_like.strip():
                where.append("UPPER(APP_NAME_RAW) LIKE UPPER(%s)")
                params.append(f"%{app_like.strip()}%")
            if iter_like.strip():
                where.append("UPPER(ITERATION_PATH) LIKE UPPER(%s)")
                params.append(f"%{iter_like.strip()}%")
            where_sql = " WHERE " + " AND ".join(where) if where else ""

            st.markdown("#### Latest features")
            st.caption(
                "Derived FTE (SWAG) is the only demand driver used by Expected costing. Features with Derived FTE = 0 contribute no demand/cost."
            )
            df_raw = ado_features_base_query(where_sql, tuple(params) if params else None)

            if df_raw is not None and not df_raw.empty:
                fte = pd.to_numeric(df_raw.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
                ready_pct = float((fte > 0).mean()) if len(fte.index) else 0.0
                st.caption(f"SWAG-ready (Derived FTE > 0): {ready_pct*100:.0f}%")

            df_show = df_raw.copy() if isinstance(df_raw, pd.DataFrame) else pd.DataFrame()
            if not df_show.empty:
                def _fmt_fte(v: Any) -> str:
                    try:
                        x = float(v)
                        if pd.isna(x) or not math.isfinite(x):
                            return "N/A"
                        return f"{x:,.2f}"
                    except Exception:
                        return "N/A"

                def _fmt_diff(v: Any) -> str:
                    try:
                        x = float(v)
                        if pd.isna(x) or not math.isfinite(x):
                            return ""
                        return f"{x:,.2f}"
                    except Exception:
                        return ""

                df_show["DERIVED_FTE_DISPLAY"] = df_show.get("DERIVED_FTE", pd.Series([pd.NA] * len(df_show))).apply(_fmt_fte)
                df_show["FTE_DIFF_DISPLAY"] = df_show.get("FTE_DIFF", pd.Series([pd.NA] * len(df_show))).apply(_fmt_diff)

            show_cols = [
                "FEATURE_ID",
                "TITLE",
                "STATE",
                "TEAM_RAW",
                "PROGRAMNAME",
                "ITERATION_LEVEL3",
                "ITERATION_PATH",
                "ITERATION_SK",
                "CAL_ITERATION_LEVEL3",
                "CAL_ITERATION_PATH",
                "CAL_ITERATION_SK",
                "CAL_ITERATION_GRAIN",
                "CAL_START_DATE",
                "CAL_END_DATE",
                "FEATURE_IS_SPRINT_LEVEL",
                "SPRINT_TO_PI_RESOLVED",
                "MISSING_PI_CALENDAR",
                "TEAMNAME",
                "APP_NAME_RAW",
                "GROUPNAME",
                "INVESTMENT_DIMENSION",
                "ADO_YEAR",
                "DERIVED_FTE",
                "BUSINESS_VALUE",
                "CHANGED_AT",
            ]
            show_cols = [c for c in show_cols if c in df_show.columns]
            st.dataframe(
                df_show[show_cols] if show_cols else df_show,
                use_container_width=True,
                height=340,
                column_config={"DERIVED_FTE": st.column_config.NumberColumn("Derived FTE (SWAG)", format="%.2f")},
            )

            st.markdown("#### Summary")
            summary_view = st.selectbox(
                "Summary view",
                ["By iteration", "By year & iteration", "By team & iteration"],
                index=0,
                key="ado_explore_summary_view",
            )

            if summary_view == "By iteration":
                df_iter_sum = fetch_df(f"""
                  SELECT ITERATION_PATH,
                         COUNT(*) AS FEATURES
                  FROM ADO_FEATURES
                  {where_sql}
                  GROUP BY ITERATION_PATH
                  ORDER BY ITERATION_PATH
                """, tuple(params) if params else None)
                st.dataframe(df_iter_sum, use_container_width=True, height=300)
            elif summary_view == "By year & iteration":
                df_year_iter = fetch_df(f"""
                      WITH base AS (
                        SELECT ADO_YEAR, ITERATION_PATH
                        FROM ADO_FEATURES
                        {where_sql}
                      ),
                      labeled AS (
                        SELECT
                          ADO_YEAR,
                      /* Extract the first digit 1-4 after the letter 'I' (case-insensitive) */
                      CASE
                        WHEN ITERATION_PATH IS NULL OR CHARINDEX('I', UPPER(ITERATION_PATH)) = 0 THEN NULL
                        ELSE SUBSTRING(
                               SUBSTRING(UPPER(ITERATION_PATH), CHARINDEX('I', UPPER(ITERATION_PATH)), 10),
                               NULLIF(PATINDEX('%[1-4]%', SUBSTRING(UPPER(ITERATION_PATH), CHARINDEX('I', UPPER(ITERATION_PATH)), 10)), 0),
                               1
                             )
                          END AS ITER_NUM
                        FROM base
                      )
                      SELECT
                        ADO_YEAR AS YEAR,
                        CASE WHEN ITER_NUM IS NOT NULL THEN CONCAT('I', ITER_NUM) ELSE NULL END AS ITERATION,
                        COUNT(*) AS FEATURES
                      FROM labeled
                      GROUP BY ADO_YEAR, ITER_NUM
                      ORDER BY YEAR, TRY_CONVERT(INT, ITER_NUM)
                    """, tuple(params) if params else None)
                st.dataframe(df_year_iter, use_container_width=True, height=300)
            else:
                df_team_iter = fetch_df(f"""
                      WITH base AS (
                        SELECT
                          COALESCE(t.TEAMNAME, af.TEAM_RAW) AS TEAMNAME,
                          af.ITERATION_PATH
                        FROM ADO_FEATURES af
                        LEFT JOIN MAP_ADO_TEAM_TO_TCO_TEAM m
                          ON UPPER(m.ADO_TEAM_KEY) = COALESCE(
                               af.TEAM_VARIANT_KEY,
                           UPPER(NULLIF(LTRIM(RTRIM(af.TEAM_RAW)), ''))
                         )
                    LEFT JOIN TEAMS t ON t.TEAMID = m.TEAMID
                    {where_sql}
                  )
                      SELECT
                        TEAMNAME,
                        ITERATION_PATH,
                        COUNT(*) AS FEATURES
                      FROM base
                      GROUP BY TEAMNAME, ITERATION_PATH
                      ORDER BY TEAMNAME, ITERATION_PATH
                    """, tuple(params) if params else None)
                st.dataframe(df_team_iter, use_container_width=True, height=300)
        with tab_explore_po:
            st.caption(
                "Explorer v2 is the canonical feature-level demand view. "
                "Demand driver is Derived FTE (SWAG). Features with Derived FTE = 0 contribute no Expected demand/cost."
            )

            years_df = None
            try:
                years_df = fetch_df(
                    """
                    SELECT DISTINCT TRY_CONVERT(INT, YEAR) AS Y
                    FROM VW_TCO_FEATURE_DEMAND
                    WHERE TRY_CONVERT(INT, YEAR) IS NOT NULL
                    ORDER BY Y DESC
                    """,
                    None,
                )
            except Exception as e:
                st.warning(f"Could not read VW_TCO_FEATURE_DEMAND years: {e}")

            years = (
                years_df["Y"].dropna().astype(int).tolist()
                if years_df is not None and not years_df.empty and "Y" in years_df.columns
                else []
            )

            if not years:
                st.info("No rows found in VW_TCO_FEATURE_DEMAND yet. Build/refresh ADO demand first.")
            else:
                year = st.selectbox("Year", options=years, index=0, key="ado_po_year")
                feats = load_explorer_feature_rows(years=[int(year)])

                if feats is None or feats.empty:
                    st.info("No in-scope features found for this year in Explorer v2.")
                else:
                    # SWAG input (points) is now surfaced directly from the canonical Explorer v2 view as SWAG_POINTS.
                    # Keep a safe fallback enrichment from ADO_FEATURES for older deployments.
                    if "SWAG_POINTS" not in feats.columns:
                        try:
                            pts_df = fetch_df(
                                """
                                SELECT
                                  FEATURE_ID,
                                  TRY_CONVERT(FLOAT, STORY_POINTS) AS STORY_POINTS
                                FROM ADO_FEATURES
                                WHERE FEATURE_ID IS NOT NULL
                                """,
                                None,
                            )
                        except Exception:
                            pts_df = None
                        if isinstance(pts_df, pd.DataFrame) and not pts_df.empty and "FEATURE_ID" in pts_df.columns:
                            pts = pts_df.copy()
                            pts["FEATURE_ID"] = pts["FEATURE_ID"].astype(str).str.strip()
                            pts["STORY_POINTS"] = pd.to_numeric(pts.get("STORY_POINTS"), errors="coerce")
                            pts = pts.drop_duplicates(subset=["FEATURE_ID"], keep="first")
                            feats = feats.copy()
                            feats["FEATURE_ID"] = feats["FEATURE_ID"].astype(str).str.strip()
                            feats = feats.merge(pts[["FEATURE_ID", "STORY_POINTS"]], on="FEATURE_ID", how="left")
                            feats["SWAG_POINTS"] = feats["STORY_POINTS"]

                    f1, f2, f3, f4 = st.columns([1.0, 1.6, 1.6, 1.6])
                    with f1:
                        _ = st.selectbox("Year", options=[int(year)], index=0, disabled=True, key="ado_po_year_locked")
                    with f2:
                        prog_opts = ["(All)"] + sorted(feats["PROGRAMNAME"].dropna().astype(str).unique().tolist())
                        prog_sel = st.selectbox("Program", options=prog_opts, index=0, key="ado_po_prog")
                    with f3:
                        team_opts = ["(All)"] + sorted(feats["TEAMNAME"].dropna().astype(str).unique().tolist())
                        team_sel = st.selectbox("Team", options=team_opts, index=0, key="ado_po_team")
                    with f4:
                        pi_opts = ["(All)"] + sorted(feats["PI_LABEL"].dropna().astype(str).unique().tolist())
                        pi_sel = st.selectbox("PI", options=pi_opts, index=0, key="ado_po_pi")

                    filt = feats.copy()
                    if prog_sel != "(All)":
                        filt = filt[filt["PROGRAMNAME"] == prog_sel].copy()
                    if team_sel != "(All)":
                        filt = filt[filt["TEAMNAME"] == team_sel].copy()
                    if pi_sel != "(All)":
                        filt = filt[filt["PI_LABEL"] == pi_sel].copy()

                    filt["DERIVED_FTE"] = pd.to_numeric(filt.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
                    if "SWAG_POINTS" in filt.columns:
                        filt["SWAG_POINTS"] = pd.to_numeric(filt.get("SWAG_POINTS"), errors="coerce")
                        filt["SWAG_READY"] = filt["SWAG_POINTS"].fillna(0.0) > 0
                    else:
                        filt["SWAG_READY"] = filt["DERIVED_FTE"] > 0

                    bad_cols = [c for c in filt.columns if c.upper().startswith(("EFFORT_", "MANUAL_"))]
                    if bad_cols:
                        st.error(f"Explorer v2 dataset unexpectedly contains legacy columns: {', '.join(bad_cols)}")

                    total_features = int(filt["FEATURE_ID"].nunique())
                    swag_ready_ct = int(filt["SWAG_READY"].fillna(False).astype(bool).sum()) if total_features else 0
                    derived_fte_total = float(filt["DERIVED_FTE"].sum()) if total_features else 0.0

                    m1, m2, m3 = st.columns(3)
                    m1.metric("Features (in scope)", f"{total_features:,d}")
                    m2.metric("SWAG-ready features", f"{swag_ready_ct:,d}")
                    m3.metric("Derived FTE total (SWAG)", f"{derived_fte_total:,.2f}" if derived_fte_total else "0.00")

                    not_ready = filt[~filt["SWAG_READY"].fillna(False).astype(bool)].copy()
                    if not not_ready.empty:
                        st.warning(
                            f"SWAG not ready for {len(not_ready):,d} feature(s) in this selection (Derived FTE = 0 \u2192 no Expected demand/cost)."
                        )

                    show_cols = [
                        "FEATURE_ID",
                        "FEATURE_TITLE",
                        "STATE",
                        "PROGRAMNAME",
                        "TEAMNAME",
                        "GROUPNAME",
                        "ADO_YEAR",
                        "PI_LABEL",
                        "SWAG_POINTS",
                        "DERIVED_FTE",
                        "SWAG_READY",
                        "IS_MSP_FEATURE",
                        "MAPPING_STATUS",
                        "INVESTMENT_DIMENSION",
                    ]
                    show_cols = [c for c in show_cols if c in filt.columns]
                    st.dataframe(
                        filt[show_cols].sort_values(["ADO_YEAR", "PI_LABEL", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "FEATURE_ID"]),
                        use_container_width=True,
                        height=520,
                        hide_index=True,
                        column_config={
                            "SWAG_POINTS": st.column_config.NumberColumn("SWAG (Story Points)", format="%.0f"),
                            "DERIVED_FTE": st.column_config.NumberColumn("Derived FTE (SWAG)", format="%.2f"),
                            "SWAG_READY": st.column_config.CheckboxColumn("SWAG ready"),
                        },
                    )
if top_nav == "Reconciliation":
    render_reconciliation_tab(fetch_df=fetch_df, scope=None)

# =========================
# Tab: 📈 APPTIO
# =========================
if top_nav == "APPTIO":
    st.subheader("APPTIO Actuals (Work ID → Program)")

    with st.expander("PI Calendar (monthly → PI allocation)", expanded=False):
        st.caption("Controls which PI calendar root is used when mapping monthly Apptio program NWF into PIs.")

        # Year selector (used only to populate root options).
        try:
            df_years = fetch_df(
                """
                SELECT DISTINCT TRY_CONVERT(INT, ADO_YEAR) AS YEAR
                FROM dbo.ADO_FEATURES
                WHERE ADO_YEAR IS NOT NULL
                ORDER BY TRY_CONVERT(INT, ADO_YEAR)
                """
            )
            years_opts = (
                sorted(pd.to_numeric(df_years.get("YEAR"), errors="coerce").dropna().astype(int).unique().tolist())
                if df_years is not None and not df_years.empty
                else []
            )
        except Exception:
            years_opts = []

        current_year = date.today().year
        default_year = current_year if current_year in years_opts else (years_opts[-1] if years_opts else current_year)
        pick_year = st.selectbox("Year", years_opts or [default_year], index=0, key="settings_pi_calendar_year")

        roots = list_iteration_roots(int(pick_year)) if pick_year is not None else []
        auto_root = dominant_iteration_root(int(pick_year)) if pick_year is not None else None
        if auto_root:
            st.caption(f"Auto (dominant from ADO) would select: `{auto_root}`")

        options = ["AUTO (dominant from ADO)"] + roots
        override = st.session_state.get("pi_calendar_root_override")
        default_choice = override if override and str(override).strip() in options else "AUTO (dominant from ADO)"
        try:
            default_idx = options.index(default_choice)
        except Exception:
            default_idx = 0
        picked_root = st.selectbox("PI Calendar Root", options, index=default_idx, key="settings_pi_calendar_root")

        if str(picked_root).strip().upper().startswith("AUTO"):
            st.session_state["pi_calendar_root_override"] = None
        else:
            st.session_state["pi_calendar_root_override"] = str(picked_root).strip()

    try:
        ensure_tables()
        ensure_apptio_actuals_lines_table()
        ensure_map_apptio_to_cost_type_table()
    except Exception as e:
        st.warning(f"Could not ensure base tables: {e}")

    mapping_df = list_program_apptio_workids()
    prog_opts = []
    if mapping_df is not None and not mapping_df.empty:
        prog_opts = sorted(mapping_df["PROGRAMNAME"].dropna().astype(str).unique().tolist())
        with st.expander("Current Work ID mappings (edit in Programs page)", expanded=False):
            st.dataframe(mapping_df, use_container_width=True, height=240)
    else:
        st.warning("No Work ID mappings found. Add mappings on the Programs page before importing actuals.")

    with st.expander("Apptio mapping (COST_TYPE / SUBTYPE)", expanded=False):
        st.caption("Map Apptio dimensions to the same COST_TYPE/SUBTYPE taxonomy used in Programs → Additional Costs.")
        try:
            yrs_df = fetch_df("SELECT DISTINCT TRY_CONVERT(INT, FISCAL_YEAR) AS YEAR FROM APPTIO_ACTUALS_LINES WHERE FISCAL_YEAR IS NOT NULL ORDER BY YEAR DESC", None)
            years_map = sorted(pd.to_numeric(yrs_df.get("YEAR"), errors="coerce").dropna().astype(int).unique().tolist()) if yrs_df is not None and not yrs_df.empty else []
        except Exception:
            years_map = []
        if not years_map:
            try:
                yrs_df = fetch_df("SELECT DISTINCT TRY_CONVERT(INT, FISCAL_YEAR) AS YEAR FROM APPTIO_ACTUALS WHERE FISCAL_YEAR IS NOT NULL ORDER BY YEAR DESC", None)
                years_map = sorted(pd.to_numeric(yrs_df.get("YEAR"), errors="coerce").dropna().astype(int).unique().tolist()) if yrs_df is not None and not yrs_df.empty else []
            except Exception:
                years_map = []
        map_year = st.selectbox("Fiscal year (for mapping preview)", years_map or [date.today().year], index=0, key="apptio_map_year")

        planned_opts = [""] + list(PLANNED_NWF_SUBCOMPONENTS)

        def _pair_to_planned(ct: Any, stp: Any) -> str:
            cost_type = str(ct or "").strip()
            subtype = str(stp or "").strip()
            if not cost_type:
                return ""
            if cost_type.lower() == "cloud" and subtype in {"AWS", "Azure"}:
                raw = f"Cloud {subtype}".strip()
            elif cost_type == "NWF" and subtype == "Other":
                raw = "NWF Other"
            elif cost_type in {"Invoices", "Contractor CS", "MSP", "Travel", "Infra"} and subtype == "":
                raw = cost_type
            else:
                raw = f"{cost_type} {subtype}".strip()
            return normalize_nwf_subcomponent(raw)

        def _planned_to_pair(label: Any) -> tuple[str, str]:
            lab = normalize_nwf_subcomponent(str(label or ""))
            if lab == "Cloud AWS":
                return ("Cloud", "AWS")
            if lab == "Cloud Azure":
                return ("Cloud", "Azure")
            if lab == "NWF Other":
                return ("NWF", "Other")
            if lab in {"Invoices", "Contractor CS", "MSP", "Travel", "Infra"}:
                return (lab, "")
            return ("NWF", "Other")

        tab_prod, tab_ledger, tab_rec = st.tabs(["Products", "Ledger fallback", "Reconciliation"])

        with tab_prod:
            st.caption("Tip: map MSP via Product ID (ledger fallback can mix multiple spend types under the same ledger).")
            try:
                prod_df = fetch_df(
                    """
                    SELECT
                      LEDGER_ACCOUNT_L3_DESC,
                      PRODUCT_ID,
                      MAX(PRODUCT_NAME) AS PRODUCT_NAME,
                      COUNT(*) AS ROWS_N,
                      SUM(COALESCE(TRY_CONVERT(FLOAT, AMOUNT), 0.0)) AS AMOUNT
                    FROM APPTIO_ACTUALS_LINES
                    WHERE TRY_CONVERT(INT, FISCAL_YEAR) = ?
                      AND PRODUCT_ID IS NOT NULL
                      AND LTRIM(RTRIM(PRODUCT_ID)) <> ''
                    GROUP BY PRODUCT_ID
                    , LEDGER_ACCOUNT_L3_DESC
                    ORDER BY AMOUNT DESC
                    """,
                    (int(map_year),),
                )
            except Exception:
                prod_df = pd.DataFrame()
            try:
                map_df = fetch_df(
                    """
                    SELECT
                      MAP_ID, FISCAL_YEAR, PRODUCT_ID, LEDGER_ACCOUNT_CONTAINS,
                      COST_TYPE, SUBTYPE, IS_ACTIVE, NOTES
                    FROM MAP_APPTIO_TO_COST_TYPE
                    WHERE PRODUCT_ID IS NOT NULL
                    """,
                    None,
                )
            except Exception:
                map_df = pd.DataFrame()

            try:
                ledger_map_df = fetch_df(
                    """
                    SELECT
                      MAP_ID, FISCAL_YEAR, PRODUCT_ID, LEDGER_ACCOUNT_CONTAINS,
                      COST_TYPE, SUBTYPE, IS_ACTIVE, NOTES
                    FROM MAP_APPTIO_TO_COST_TYPE
                    WHERE PRODUCT_ID IS NULL AND LEDGER_ACCOUNT_CONTAINS IS NOT NULL
                    """,
                    None,
                )
            except Exception:
                ledger_map_df = pd.DataFrame()

            map_id_by_pid: dict[str, Any] = {}
            if prod_df is None or prod_df.empty:
                st.info("No product IDs found in APPTIO_ACTUALS_LINES for this year yet. Upload parses the file; you must click 'Save Apptio actuals' to write APPTIO_ACTUALS_LINES. Then return here.")
                prod_editor_src = pd.DataFrame(
                    columns=[
                        "LEDGER_ACCOUNT_L3_DESC",
                        "PRODUCT_ID",
                        "PRODUCT_NAME",
                        "AMOUNT",
                        "ROWS_N",
                        "PLANNED_NWF_TYPE",
                        "EFFECTIVE_NWF_TYPE",
                        "MAPPED_BY",
                        "IS_ACTIVE",
                        "NOTES",
                    ]
                )
            else:
                p = prod_df.copy()
                p["LEDGER_ACCOUNT_L3_DESC"] = p.get("LEDGER_ACCOUNT_L3_DESC", "").fillna("").astype(str).str.strip()
                p["PRODUCT_ID"] = p.get("PRODUCT_ID", "").fillna("").astype(str).str.strip()
                p["PRODUCT_NAME"] = p.get("PRODUCT_NAME", "").fillna("").astype(str).str.strip()
                p["AMOUNT"] = pd.to_numeric(p.get("AMOUNT"), errors="coerce").fillna(0.0)
                p["ROWS_N"] = pd.to_numeric(p.get("ROWS_N"), errors="coerce").fillna(0).astype(int)
                p = p[p["PRODUCT_ID"].ne("")].copy()

                m = map_df.copy() if map_df is not None and not map_df.empty else pd.DataFrame(columns=["MAP_ID", "FISCAL_YEAR", "PRODUCT_ID", "COST_TYPE", "SUBTYPE", "IS_ACTIVE", "NOTES"])
                if not m.empty:
                    m["PRODUCT_ID"] = m.get("PRODUCT_ID", "").fillna("").astype(str).str.strip()
                    m["FISCAL_YEAR"] = pd.to_numeric(m.get("FISCAL_YEAR"), errors="coerce")
                    m["_pri"] = (m["FISCAL_YEAR"].fillna(int(map_year)) != int(map_year)).astype(int)
                    m["_active_pri"] = (pd.to_numeric(m.get("IS_ACTIVE"), errors="coerce").fillna(1).astype(int) != 1).astype(int)
                    m = m.sort_values(["PRODUCT_ID", "_active_pri", "_pri"]).drop(columns=["_pri", "_active_pri"])
                    m_best = m.groupby("PRODUCT_ID", dropna=False).head(1).copy()
                else:
                    m_best = pd.DataFrame(columns=["MAP_ID", "FISCAL_YEAR", "PRODUCT_ID", "COST_TYPE", "SUBTYPE", "IS_ACTIVE", "NOTES"])

                map_id_by_pid = (
                    m_best.set_index("PRODUCT_ID")["MAP_ID"].to_dict()
                    if m_best is not None and not m_best.empty and "MAP_ID" in m_best.columns and "PRODUCT_ID" in m_best.columns
                    else {}
                )

                prod_editor_src = p.merge(m_best.drop(columns=["MAP_ID"], errors="ignore"), on="PRODUCT_ID", how="left")
                prod_editor_src["IS_ACTIVE"] = prod_editor_src.get("IS_ACTIVE").fillna(1).astype(int)
                prod_editor_src["NOTES"] = prod_editor_src.get("NOTES", "").fillna("").astype(str).str.strip()
                prod_editor_src["PLANNED_NWF_TYPE"] = prod_editor_src.apply(
                    lambda r: _pair_to_planned(r.get("COST_TYPE"), r.get("SUBTYPE")), axis=1
                )

                # Compute effective mapping (product override unless it is blank or NWF Other; otherwise ledger fallback; else default).
                lm = ledger_map_df.copy() if ledger_map_df is not None else pd.DataFrame()
                if lm is not None and not lm.empty:
                    lm["LEDGER_ACCOUNT_CONTAINS"] = lm.get("LEDGER_ACCOUNT_CONTAINS", "").fillna("").astype(str).str.strip()
                    lm = lm[lm["LEDGER_ACCOUNT_CONTAINS"].ne("")].copy()
                    lm["FISCAL_YEAR"] = pd.to_numeric(lm.get("FISCAL_YEAR"), errors="coerce")
                    lm["IS_ACTIVE"] = pd.to_numeric(lm.get("IS_ACTIVE"), errors="coerce").fillna(1).astype(int)
                    lm["PLANNED_NWF_TYPE"] = lm.apply(lambda r: _pair_to_planned(r.get("COST_TYPE"), r.get("SUBTYPE")), axis=1)

                def _best_ledger_planned(desc: str) -> tuple[str, str]:
                    if lm is None or lm.empty:
                        return ("", "")
                    d_up = str(desc or "").strip().upper()
                    if not d_up:
                        return ("", "")
                    cand = lm[lm["LEDGER_ACCOUNT_CONTAINS"].astype(str).str.upper().apply(lambda s: s and s in d_up)].copy()
                    if cand.empty:
                        return ("", "")
                    # Prefer active, year-specific, and the longest match.
                    cand["_apri"] = (cand["IS_ACTIVE"] != 1).astype(int)
                    cand["_ypri"] = (cand["FISCAL_YEAR"].fillna(int(map_year)) != int(map_year)).astype(int)
                    cand["_lpri"] = cand["LEDGER_ACCOUNT_CONTAINS"].astype(str).str.len().mul(-1)
                    cand = cand.sort_values(["_apri", "_ypri", "_lpri"])
                    lab = str(cand.iloc[0].get("PLANNED_NWF_TYPE") or "").strip()
                    return (lab, "Ledger")

                eff_type: list[str] = []
                eff_by: list[str] = []
                for _, rr in prod_editor_src.iterrows():
                    prod_planned = str(rr.get("PLANNED_NWF_TYPE") or "").strip()
                    prod_override = prod_planned if prod_planned and prod_planned != "NWF Other" else ""
                    if prod_override:
                        eff_type.append(prod_override)
                        eff_by.append("Product")
                        continue
                    ledger_planned, by = _best_ledger_planned(str(rr.get("LEDGER_ACCOUNT_L3_DESC") or "").strip())
                    if ledger_planned:
                        eff_type.append(ledger_planned)
                        eff_by.append(by)
                    else:
                        eff_type.append("NWF Other")
                        eff_by.append("Default")
                prod_editor_src["EFFECTIVE_NWF_TYPE"] = eff_type
                prod_editor_src["MAPPED_BY"] = eff_by

                # UX: small filters help quickly map MSP (by Product ID/Name).
                f1, f2 = st.columns([2, 1])
                prod_filter = f1.text_input("Filter (optional)", value="", key="apptio_products_filter")
                show_unmapped_only = f2.checkbox("Show unmapped only", value=False, key="apptio_products_unmapped_only")
                if prod_filter.strip():
                    q = prod_filter.strip().lower()
                    mask = (
                        prod_editor_src["LEDGER_ACCOUNT_L3_DESC"].fillna("").astype(str).str.lower().str.contains(q)
                        | prod_editor_src["PRODUCT_ID"].fillna("").astype(str).str.lower().str.contains(q)
                        | prod_editor_src["PRODUCT_NAME"].fillna("").astype(str).str.lower().str.contains(q)
                    )
                    prod_editor_src = prod_editor_src[mask].copy()
                if show_unmapped_only:
                    prod_editor_src = prod_editor_src[prod_editor_src["PLANNED_NWF_TYPE"].fillna("").astype(str).str.strip().eq("")].copy()

                # Required display order (then editable mapping fields).
                view_cols = [
                    "LEDGER_ACCOUNT_L3_DESC",
                    "PRODUCT_ID",
                    "PRODUCT_NAME",
                    "AMOUNT",
                    "ROWS_N",
                    "PLANNED_NWF_TYPE",
                    "EFFECTIVE_NWF_TYPE",
                    "MAPPED_BY",
                    "IS_ACTIVE",
                    "NOTES",
                ]
                view_cols = [c for c in view_cols if c in prod_editor_src.columns]
                prod_editor_src = prod_editor_src[view_cols].copy()

            edited_prod = st.data_editor(
                prod_editor_src,
                key="apptio_map_products_editor",
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "LEDGER_ACCOUNT_L3_DESC": st.column_config.TextColumn("LEDGER_ACCOUNT_L3_DESC", disabled=True),
                    "PRODUCT_ID": st.column_config.TextColumn("PRODUCT_ID", disabled=True),
                    "PRODUCT_NAME": st.column_config.TextColumn("PRODUCT_NAME", disabled=True),
                    "AMOUNT": st.column_config.NumberColumn("AMOUNT", format="%.2f", disabled=True),
                    "ROWS_N": st.column_config.NumberColumn("ROWS_N", disabled=True),
                    "PLANNED_NWF_TYPE": st.column_config.SelectboxColumn("Planned NWF Type", options=planned_opts),
                    "EFFECTIVE_NWF_TYPE": st.column_config.TextColumn("Effective NWF Type", disabled=True),
                    "MAPPED_BY": st.column_config.TextColumn("Mapped By", disabled=True),
                    "IS_ACTIVE": st.column_config.CheckboxColumn("IS_ACTIVE"),
                    "NOTES": st.column_config.TextColumn("NOTES"),
                },
                hide_index=True,
            )

            with st.expander(f"Cleanup (FY{int(map_year)})", expanded=False):
                st.warning("This deletes legacy product-level mapping rows that were defaulted to NWF Other and can block ledger fallback.")
                confirm = st.checkbox(f"I understand — delete PRODUCT_ID mappings that are NWF Other for FY{int(map_year)}", value=False, key="apptio_del_prod_nwf_other_confirm")
                if st.button(f"Delete ALL product mappings = NWF Other (FY{int(map_year)})", disabled=not confirm, key="apptio_del_prod_nwf_other_btn"):
                    try:
                        execute(
                            """
                            DELETE FROM MAP_APPTIO_TO_COST_TYPE
                            WHERE PRODUCT_ID IS NOT NULL
                              AND TRY_CONVERT(INT, FISCAL_YEAR) = ?
                              AND UPPER(LTRIM(RTRIM(COST_TYPE))) = 'NWF'
                              AND UPPER(COALESCE(NULLIF(LTRIM(RTRIM(SUBTYPE)), ''), 'Other')) = 'OTHER'
                            """,
                            (int(map_year),),
                        )
                        st.session_state["ver_apptio_mapping"] = int(st.session_state.get("ver_apptio_mapping", 0)) + 1
                        toast_success("Deleted legacy product NWF Other mappings.")
                        try:
                            st.cache_data.clear()
                        except Exception:
                            pass
                    except Exception as e:
                        toast_error(f"Could not delete rows: {e}")

            if st.button("Save product mappings", key="btn_save_apptio_prod_map", type="primary"):
                try:
                    rows_map: list[dict[str, Any]] = []
                    if edited_prod is not None and not edited_prod.empty:
                        # Deduplicate by PRODUCT_ID (mapping is keyed by PRODUCT_ID in SQL; ledger is display-only).
                        ed = edited_prod.copy()
                        ed["PRODUCT_ID"] = ed.get("PRODUCT_ID", "").fillna("").astype(str).str.strip()
                        ed["PLANNED_NWF_TYPE"] = ed.get("PLANNED_NWF_TYPE", "").fillna("").astype(str).str.strip()
                        ed["NOTES"] = ed.get("NOTES", "").fillna("").astype(str).str.strip()
                        ed["IS_ACTIVE"] = ed.get("IS_ACTIVE").fillna(1).astype(int)
                        for pid, grp in ed.groupby("PRODUCT_ID", dropna=False):
                            if not str(pid).strip():
                                continue
                            first = grp.iloc[0]
                            planned = str(first.get("PLANNED_NWF_TYPE") or "").strip()
                            # Do not persist "NWF Other" as a product override; it's treated as default and should not block ledger fallback.
                            if not planned or planned == "NWF Other":
                                continue
                            ct, stp = _planned_to_pair(planned)
                            rows_map.append(
                                {
                                    "MAP_ID": map_id_by_pid.get(str(pid).strip()),
                                    "FISCAL_YEAR": int(map_year),
                                    "PRODUCT_ID": str(pid).strip(),
                                    "LEDGER_ACCOUNT_CONTAINS": None,
                                    "COST_TYPE": ct,
                                    "SUBTYPE": stp,
                                    "IS_ACTIVE": 1 if int(first.get("IS_ACTIVE") or 0) == 1 else 0,
                                    "NOTES": str(first.get("NOTES") or "").strip(),
                                }
                            )
                    if not rows_map:
                        st.info("No mapping rows to save.")
                    else:
                        n = upsert_apptio_to_cost_type_mappings(
                            rows_map, updated_by=st.session_state.get("auth_user", {}).get("email")
                        )
                        st.session_state["ver_apptio_mapping"] = int(st.session_state.get("ver_apptio_mapping", 0)) + 1
                        toast_success(f"Saved {n} mapping rows.")
                except Exception as e:
                    toast_error(f"Could not save mappings: {e}")

        with tab_ledger:
            st.caption("Ledger fallback mappings are only used when no Product mapping matches (or product_id is missing).")
            st.caption("Tip: map MSP via Product ID; ledger fallback is not sufficient for MSP because the same ledger can include Contractor CS.")
            try:
                ledgers_df = fetch_df(
                    """
                    SELECT
                      LEDGER_ACCOUNT_L3_DESC,
                      SUM(COALESCE(TRY_CONVERT(FLOAT, AMOUNT),0)) AS AMOUNT
                    FROM APPTIO_ACTUALS_LINES
                    WHERE TRY_CONVERT(INT,FISCAL_YEAR)=?
                      AND LEDGER_ACCOUNT_L3_DESC IS NOT NULL
                      AND LTRIM(RTRIM(LEDGER_ACCOUNT_L3_DESC))<>''
                    GROUP BY LEDGER_ACCOUNT_L3_DESC
                    ORDER BY AMOUNT DESC
                    """,
                    (int(map_year),),
                )
            except Exception:
                ledgers_df = pd.DataFrame()

            try:
                ledger_map = fetch_df(
                    """
                    SELECT
                      MAP_ID, FISCAL_YEAR, PRODUCT_ID, LEDGER_ACCOUNT_CONTAINS,
                      COST_TYPE, SUBTYPE, IS_ACTIVE, NOTES
                    FROM MAP_APPTIO_TO_COST_TYPE
                    WHERE PRODUCT_ID IS NULL
                    """,
                    None,
                )
            except Exception:
                ledger_map = pd.DataFrame()

            if ledgers_df is None or ledgers_df.empty:
                st.info("No ledger descriptions found in APPTIO_ACTUALS_LINES for this year yet. Upload parses the file; you must click 'Save Apptio actuals' to write APPTIO_ACTUALS_LINES. Then return here.")
                ledger_editor_src = pd.DataFrame(
                    columns=[
                        "MAP_ID",
                        "FISCAL_YEAR",
                        "LEDGER_ACCOUNT_L3_DESC",
                        "AMOUNT",
                        "LEDGER_ACCOUNT_CONTAINS",
                        "PLANNED_NWF_TYPE",
                        "IS_ACTIVE",
                        "NOTES",
                    ]
                )
            else:
                ldf = ledgers_df.copy()
                ldf["LEDGER_ACCOUNT_L3_DESC"] = ldf.get("LEDGER_ACCOUNT_L3_DESC", "").fillna("").astype(str).str.strip()
                ldf["AMOUNT"] = pd.to_numeric(ldf.get("AMOUNT"), errors="coerce").fillna(0.0)
                ldf = ldf[ldf["LEDGER_ACCOUNT_L3_DESC"].ne("")].copy()
                ldf = ldf.head(250).copy()

                m = ledger_map.copy() if ledger_map is not None and not ledger_map.empty else pd.DataFrame(
                    columns=["MAP_ID", "FISCAL_YEAR", "LEDGER_ACCOUNT_CONTAINS", "COST_TYPE", "SUBTYPE", "IS_ACTIVE", "NOTES"]
                )
                if not m.empty:
                    m["LEDGER_ACCOUNT_CONTAINS"] = m.get("LEDGER_ACCOUNT_CONTAINS", "").fillna("").astype(str).str.strip()
                    m = m[m["LEDGER_ACCOUNT_CONTAINS"].ne("")].copy()
                    m["FISCAL_YEAR"] = pd.to_numeric(m.get("FISCAL_YEAR"), errors="coerce")
                    m["IS_ACTIVE"] = pd.to_numeric(m.get("IS_ACTIVE"), errors="coerce").fillna(1).astype(int)

                def _best_match(desc: str) -> pd.Series:
                    if m is None or m.empty:
                        return pd.Series(dtype="object")
                    d_up = str(desc or "").strip().upper()
                    if not d_up:
                        return pd.Series(dtype="object")
                    cand = m[m["LEDGER_ACCOUNT_CONTAINS"].astype(str).str.upper().apply(lambda s: s in d_up)].copy()
                    if cand.empty:
                        return pd.Series(dtype="object")
                    cand["_ypri"] = (cand["FISCAL_YEAR"].fillna(int(map_year)) != int(map_year)).astype(int)
                    cand["_apri"] = (cand["IS_ACTIVE"] != 1).astype(int)
                    cand["_lpri"] = cand["LEDGER_ACCOUNT_CONTAINS"].astype(str).str.len().mul(-1)
                    cand = cand.sort_values(["_apri", "_ypri", "_lpri"])
                    return cand.iloc[0]

                best_rows: list[dict[str, Any]] = []
                for _, rr in ldf.iterrows():
                    desc = str(rr.get("LEDGER_ACCOUNT_L3_DESC") or "").strip()
                    match = _best_match(desc)
                    best_rows.append(
                        {
                            "LEDGER_ACCOUNT_L3_DESC": desc,
                            "AMOUNT": float(rr.get("AMOUNT") or 0.0),
                            "MAP_ID": match.get("MAP_ID") if isinstance(match, pd.Series) and not match.empty else None,
                            "FISCAL_YEAR": (
                                int(match.get("FISCAL_YEAR"))
                                if isinstance(match, pd.Series) and not match.empty and str(match.get("FISCAL_YEAR") or "").strip().isdigit()
                                else int(map_year)
                            ),
                            "LEDGER_ACCOUNT_CONTAINS": (
                                str(match.get("LEDGER_ACCOUNT_CONTAINS") or "").strip()
                                if isinstance(match, pd.Series) and not match.empty and str(match.get("LEDGER_ACCOUNT_CONTAINS") or "").strip()
                                else desc
                            ),
                            "PLANNED_NWF_TYPE": _pair_to_planned(
                                match.get("COST_TYPE") if isinstance(match, pd.Series) and not match.empty else "",
                                match.get("SUBTYPE") if isinstance(match, pd.Series) and not match.empty else "",
                            ),
                            "IS_ACTIVE": (int(match.get("IS_ACTIVE")) if isinstance(match, pd.Series) and not match.empty else 1),
                            "NOTES": str(match.get("NOTES") or "").strip() if isinstance(match, pd.Series) and not match.empty else "",
                        }
                    )
                ledger_editor_src = pd.DataFrame(best_rows)

            edited_ledger = st.data_editor(
                ledger_editor_src,
                key="apptio_map_ledger_editor",
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "MAP_ID": st.column_config.TextColumn("MAP_ID", disabled=True),
                    "FISCAL_YEAR": st.column_config.NumberColumn("FISCAL_YEAR", help="Optional year filter (blank = all years)."),
                    "LEDGER_ACCOUNT_L3_DESC": st.column_config.TextColumn("LEDGER_ACCOUNT_L3_DESC", disabled=True),
                    "AMOUNT": st.column_config.NumberColumn("AMOUNT", format="%.2f", disabled=True),
                    "LEDGER_ACCOUNT_CONTAINS": st.column_config.TextColumn("LEDGER_ACCOUNT_CONTAINS", help="Substring match; defaults to the full ledger description so you can shorten it."),
                    "PLANNED_NWF_TYPE": st.column_config.SelectboxColumn("Planned NWF Type", options=planned_opts),
                    "IS_ACTIVE": st.column_config.CheckboxColumn("IS_ACTIVE"),
                },
                hide_index=True,
            )
            if st.button("Save ledger mappings", key="btn_save_apptio_ledger_map", type="primary"):
                try:
                    rows_map: list[dict[str, Any]] = []
                    if edited_ledger is not None and not edited_ledger.empty:
                        for _, r in edited_ledger.iterrows():
                            if not str(r.get("LEDGER_ACCOUNT_CONTAINS") or "").strip():
                                continue
                            planned = str(r.get("PLANNED_NWF_TYPE") or "").strip()
                            if not planned:
                                continue
                            ct, stp = _planned_to_pair(planned)
                            rows_map.append(
                                {
                                    "MAP_ID": r.get("MAP_ID"),
                                    "FISCAL_YEAR": int(r.get("FISCAL_YEAR")) if str(r.get("FISCAL_YEAR") or "").strip().isdigit() else None,
                                    "PRODUCT_ID": None,
                                    "LEDGER_ACCOUNT_CONTAINS": str(r.get("LEDGER_ACCOUNT_CONTAINS")).strip(),
                                    "COST_TYPE": ct,
                                    "SUBTYPE": stp,
                                    "IS_ACTIVE": 1 if bool(r.get("IS_ACTIVE")) else 0,
                                    "NOTES": str(r.get("NOTES") or "").strip(),
                                }
                            )
                    if not rows_map:
                        st.info("No mapping rows to save.")
                    else:
                        n = upsert_apptio_to_cost_type_mappings(
                            rows_map, updated_by=st.session_state.get("auth_user", {}).get("email")
                        )
                        st.session_state["ver_apptio_mapping"] = int(st.session_state.get("ver_apptio_mapping", 0)) + 1
                        toast_success(f"Saved {n} mapping rows.")
                except Exception as e:
                    toast_error(f"Could not save mappings: {e}")

        with tab_rec:
            st.caption("Program-level Apptio actuals breakdown (from enriched APPTIO_ACTUALS_LINES + mapping).")
            try:
                df_rec = fetch_apptio_actuals_by_program_breakdown(int(map_year), None)
            except Exception as e:
                st.error(f"Could not load Apptio reconciliation: {e}")
                df_rec = pd.DataFrame()
            if df_rec is None or df_rec.empty:
                st.info("No enriched Apptio rows found for this year. Upload and click 'Save Apptio actuals' to populate APPTIO_ACTUALS_LINES.")
            else:
                wrec = df_rec.copy()
                wrec["PROGRAMNAME"] = wrec.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                program_opts = sorted({p for p in wrec["PROGRAMNAME"].dropna().astype(str).tolist() if str(p).strip()})
                sel_prog = st.multiselect("Filter programs (optional)", program_opts, default=program_opts, key="apptio_rec_prog_filter")
                if sel_prog:
                    wrec = wrec[wrec["PROGRAMNAME"].isin(sel_prog)].copy()
                show_cols = ["PROGRAMNAME", "MONTH_KEY", "COST_TYPE", "SUBTYPE", "MAPPING_STATUS", "AMOUNT"]
                show_cols = [c for c in show_cols if c in wrec.columns]
                if "AMOUNT" in wrec.columns:
                    wrec["AMOUNT"] = pd.to_numeric(wrec.get("AMOUNT"), errors="coerce").fillna(0.0)
                st.dataframe(wrec[show_cols].sort_values(["PROGRAMNAME", "MONTH_KEY", "COST_TYPE", "SUBTYPE"]), use_container_width=True, height=520)

apptio_file = None
if top_nav == "APPTIO":
    apptio_file = st.file_uploader(
        "Upload Apptio actuals (XLSX/XLSM/XLSB/XLS/CSV) with columns: Work ID, Jan FY 2025, Feb FY 2025, ...",
        type=["xlsx", "xlsm", "xlsb", "xls", "csv"],
        key="apptio_actuals_file",
    )
apptio_sheet: Optional[str] = None
diag_apptio: Dict[str, Any] = {}
parsed_df: Optional[pd.DataFrame] = None

if apptio_file:
    apptio_file.seek(0)
    file_bytes = apptio_file.read()
    apptio_file.seek(0)
    sheets = _list_excel_sheets(file_bytes)
    if sheets:
        apptio_sheet = st.selectbox("Worksheet", sheets, index=0, key="apptio_sheet_select")
    try:
        raw_df = _read_file_any(apptio_file, apptio_sheet, diag_apptio)
        parsed_df = _normalize_apptio_actuals(raw_df)
        st.success(f"Parsed {len(parsed_df)} Work ID-month rows from the file.")
    except Exception as e:
        st.error(f"Could not parse Apptio file: {e}")
        if diag_apptio:
            with st.expander("Diagnostics", expanded=False):
                st.json(diag_apptio)

if parsed_df is not None:
    if mapping_df is None or mapping_df.empty:
        st.info("Upload succeeded but no mappings are available to associate Work IDs to programs.")
    mapped = parsed_df.copy()
    if mapping_df is not None and not mapping_df.empty:
        mapped = mapped.merge(mapping_df[["WORK_ID", "PROGRAMID", "PROGRAMNAME"]], how="left", on="WORK_ID")

    if prog_opts:
        sel_prog = st.multiselect(
            "Programs to include",
            prog_opts,
            default=prog_opts,
            key="apptio_prog_filter",
        )
        if sel_prog:
            mapped = mapped[mapped["PROGRAMNAME"].isin(sel_prog)]

    unmapped = mapped[mapped.get("PROGRAMID").isna()]
    if not unmapped.empty:
        missing_ids = unmapped["WORK_ID"].dropna().unique().tolist()
        st.warning(f"{len(missing_ids)} Work IDs are not mapped to a program and will be skipped.")

    to_upsert = mapped.dropna(subset=["PROGRAMID"]).copy()
    if to_upsert.empty:
        st.info("No rows matched a mapped program.")
    else:
        to_upsert["AMOUNT"] = pd.to_numeric(to_upsert["AMOUNT"], errors="coerce").fillna(0.0)
        summary = (
            to_upsert.groupby(["PROGRAMNAME", "FISCAL_YEAR", "MONTH"], as_index=False)["AMOUNT"]
            .sum()
            .rename(columns={"FISCAL_YEAR": "Year", "MONTH": "Month", "AMOUNT": "Amount"})
        )
        month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        summary["Month"] = summary["Month"].astype(int).map(lambda m: month_names[m-1] if 1 <= m <= 12 else m)
        st.dataframe(summary, use_container_width=True, height=280)

        # Portfolio/program total across all months shown before save
        total_df = (
            to_upsert.groupby(["PROGRAMNAME"], as_index=False)["AMOUNT"]
            .sum()
            .rename(columns={"AMOUNT": "Total Amount"})
            .sort_values("Total Amount", ascending=False)
        )
        total_df["Total Amount"] = total_df["Total Amount"].map(lambda v: f"${v:,.2f}")
        st.dataframe(total_df, use_container_width=True, height=180)

        grouped = (
            to_upsert.groupby(["WORK_ID", "FISCAL_YEAR", "MONTH"], as_index=False)["AMOUNT"]
            .sum()
        )
        user_email = st.session_state.get("auth_user", {}).get("email")
        rows = [
            {
                "work_id": r["WORK_ID"],
                "fiscal_year": int(r["FISCAL_YEAR"]),
                "month": int(r["MONTH"]),
                "amount": float(r["AMOUNT"] or 0),
                "source": apptio_file.name if apptio_file else None,
                "loaded_by": user_email,
            }
            for _, r in grouped.iterrows()
        ]
        # Enriched lines for taxonomy mapping (best-effort; still keep legacy totals upsert above).
        line_cols = ["WORK_ID", "FISCAL_YEAR", "MONTH", "AMOUNT", "LEDGER_ACCOUNT_L3_DESC", "PRODUCT_ID", "PRODUCT_NAME"]
        line_src = to_upsert.copy()
        for c in ["LEDGER_ACCOUNT_L3_DESC", "PRODUCT_ID", "PRODUCT_NAME"]:
            if c not in line_src.columns:
                line_src[c] = None
        line_src = line_src[line_cols].copy()
        line_src["AMOUNT"] = pd.to_numeric(line_src.get("AMOUNT"), errors="coerce").fillna(0.0)
        line_grouped = (
            line_src.groupby(["WORK_ID", "FISCAL_YEAR", "MONTH", "PRODUCT_ID", "LEDGER_ACCOUNT_L3_DESC"], as_index=False)
            .agg({"AMOUNT": "sum", "PRODUCT_NAME": "first"})
        )
        rows_lines = [
            {
                "work_id": r.get("WORK_ID"),
                "fiscal_year": int(r.get("FISCAL_YEAR")),
                "month": int(r.get("MONTH")),
                "amount": float(r.get("AMOUNT") or 0.0),
                "ledger_account_l3_desc": r.get("LEDGER_ACCOUNT_L3_DESC"),
                "product_id": r.get("PRODUCT_ID"),
                "product_name": r.get("PRODUCT_NAME"),
            }
            for _, r in line_grouped.iterrows()
        ]
        if st.button("Save Apptio actuals", icon=":material/save:", key="btn_save_apptio_actuals", type="primary"):
            try:
                legacy_saved = upsert_apptio_actuals(rows)
                try:
                    lines_saved = upsert_apptio_actuals_lines(
                        rows_lines,
                        updated_by=user_email,
                        source=apptio_file.name if apptio_file else None,
                    )
                except Exception:
                    # Legacy totals must remain saveable even if enriched table/mapping is not ready.
                    lines_saved = 0
                toast_success(f"Saved Apptio actuals: legacy_saved={legacy_saved}, lines_saved={lines_saved}.")
                try:
                    yrs_saved = sorted({int(y) for y in grouped.get('FISCAL_YEAR').dropna().astype(int).tolist()}) if grouped is not None and not grouped.empty else []
                except Exception:
                    yrs_saved = []
                for fy in (yrs_saved or []):
                    try:
                        stats = fetch_df(
                            """
                            SELECT
                              COUNT(*) AS ROWS_N,
                              COUNT(DISTINCT NULLIF(LTRIM(RTRIM(PRODUCT_ID)), '')) AS PRODUCT_IDS_N
                            FROM APPTIO_ACTUALS_LINES
                            WHERE TRY_CONVERT(INT, FISCAL_YEAR) = ?
                            """,
                            (int(fy),),
                        )
                        if stats is not None and not stats.empty:
                            st.caption(
                                f"APPTIO_ACTUALS_LINES now contains {int(stats.iloc[0]['ROWS_N'])} rows and "
                                f"{int(stats.iloc[0]['PRODUCT_IDS_N'])} distinct PRODUCT_IDs for FY{int(fy)}."
                            )
                    except Exception:
                        pass
                try:
                    st.cache_data.clear()
                except Exception:
                    pass
            except Exception as e:
                toast_error(f"Could not save Apptio actuals: {e}")

# =========================# =========================================================
# 📦 Bulk Load (ONE sheet): Programs, Vendors, Applications, Applications, Teams & Invoices
# =========================================================
def _render_legacy_bulk_load_one_sheet() -> None:
    st.subheader("Bulk Load (one sheet): Programs, Vendors, Applications, Applications, Teams & Invoices")
    st.caption("Upload a single sheet that contains the columns for all sections. Map once, preview auto‑generates, then MERGE in a safe order with UUIDs.")
    # Downloadable Excel templates: Contracts-only (recommended) or Full (contracts + invoices)
    tmpl_mode = st.radio(
        "Template type",
        ["Contracts only (recommended)", "Core entities only (Programs/Vendors/Teams/Groups/Apps)"],
        index=0,
        horizontal=True,
        key="bulk_tmpl_mode"
    )
    if tmpl_mode.startswith("Contracts only"):
        tmpl = generate_contracts_template()
        fname = "next_contracts_template.xlsx"
        help_text = "Use this template to load Programs, Vendors, Groups, Applications, Teams, and Contracts. Planned invoices will be created per FY from contracts."
    else:
        tmpl = generate_core_entities_template()
        fname = "next_core_entities_template.xlsx"
        help_text = "Use this template to load Programs, Vendors, Teams (with Product Owner), Applications, and Application Instances."
    st.download_button(
        label="⬇️ Download Excel Template",
        data=tmpl,
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        help=help_text,
    )

    upl_one = st.file_uploader("Upload workbook (XLSX preferred; CSV allowed)", type=["xlsx","xlsm","xlsb","xls","csv"], key="one_workbook")
    sheet = None
    if upl_one:
        upl_one.seek(0); raw = upl_one.read(); upl_one.seek(0)
        sheets = _list_excel_sheets(raw)
        if sheets:
            sheet = st.selectbox("Worksheet (one sheet for everything)", sheets, index=0, key="one_sheet_select")
        else:
            st.info("No sheet list available (CSV or detection failed). I will parse the first/only sheet.")

        diag: Dict[str, Any] = {}
        try:
            df_src = _read_file_any(upl_one, sheet, diag)
            # De-duplicate column labels for display logic
            seen: Dict[str, int] = {}
            newcols: List[str] = []
            for col_lbl in df_src.columns:
                key_lbl = col_lbl
                if key_lbl in seen:
                    seen[key_lbl] += 1
                    key_lbl = f"{col_lbl}__{seen[col_lbl]}"
                else:
                    seen[key_lbl] = 0
                newcols.append(key_lbl)
            df_src.columns = newcols

            st.session_state["one_sheet_df"] = df_src
            st.success(f"Parsed {len(df_src)} rows from {upl_one.name}.")
        except Exception as e:
            with st.expander("Diagnostics", expanded=True):
                st.write("**Why it failed**")
                st.exception(e)
                st.write("**Parse attempts**")
                st.json(diag)
            st.error("Could not parse the workbook. Try a clean XLSX/CSV with headers.")
            st.stop()

    # -------------------------
    # Column mapping (single source)
    # -------------------------
    if st.session_state["one_sheet_df"] is not None:
        df_src = st.session_state["one_sheet_df"]
        cols = df_src.columns.tolist()
        # Determine current template mode to tailor mapping/previews
        is_contracts_only = str(st.session_state.get("bulk_tmpl_mode", "")).startswith("Contracts only")

        def _default_pick(name: str) -> Optional[str]:
            lower = {c.lower(): c for c in cols if isinstance(c, str)}
            if name.lower() in lower:
                return lower[name.lower()]
            matches = [c for c in cols if isinstance(c, str) and name.lower() in c.lower()]
            return matches[0] if matches else None

        # Initialize mapping once
        if not st.session_state["colmap"]:
            st.session_state["colmap"] = {
                # Programs
                "PROGRAMNAME": _default_pick("PROGRAMNAME") or _default_pick("PROGRAM"),
                # Vendors
                "VENDORNAME": _default_pick("VENDORNAME") or _default_pick("VENDOR"),
                # Applications
                "GROUPNAME": _default_pick("APP GROUP NAME") or _default_pick("GROUPNAME") or _default_pick("GROUP"),
                # Applications
                "APPNAME": _default_pick("APPNAME") or _default_pick("APPLICATION"),
                # Teams
                "TEAMNAME": _default_pick("TEAMNAME") or _default_pick("TEAM"),
                # Contracts (required)
                "CONTRACT_START_FY": _default_pick("CONTRACT_START_FY") or _default_pick("START_FY"),
                "CONTRACT_END_FY": _default_pick("CONTRACT_END_FY") or _default_pick("END_FY"),
                "CONTRACT_RENEWAL_MONTH": _default_pick("CONTRACT_RENEWAL_MONTH") or _default_pick("CONTRACT_RENEWAL") or _default_pick("RENEWAL_MONTH"),
                "CONTRACT_ANNUAL_AMOUNT": _default_pick("CONTRACT_ANNUAL_AMOUNT") or _default_pick("ANNUAL_AMOUNT") or _default_pick("AMOUNT"),
                "CONTRACT_ESCALATION_PCT": _default_pick("CONTRACT_ESCALATION_PCT") or _default_pick("ESCALATION_PCT"),
                "CONTRACT_STATUS": _default_pick("CONTRACT_STATUS") or _default_pick("STATUS"),
                "CONTRACT_AGREEMENT_NUMBER": _default_pick("CONTRACT_AGREEMENT_NUMBER") or _default_pick("AGREEMENT_NUMBER"),
                # Invoices (required)
                "AMOUNT": _default_pick("AMOUNT"),
                "FISCAL_YEAR": _default_pick("FISCAL_YEAR") or _default_pick("YEAR"),
                "RENEWAL_MONTH": _default_pick("RENEWAL_MONTH") or _default_pick("RENEWAL MONTH"),
                # Invoices (optional)
                "INVOICE_STATUS": _default_pick("INVOICE_STATUS") or _default_pick("STATUS"),
                "CONTRACT_ACTIVE": _default_pick("CONTRACT_ACTIVE"),
                "SERIAL_NUMBER": _default_pick("SERIAL_NUMBER"),
                "WORK_ORDER": _default_pick("WORK_ORDER"),
                "COMPANY_CODE": _default_pick("COMPANY_CODE"),
                "COST_CENTER": _default_pick("COST_CENTER"),
                "PRODUCT_OWNER": _default_pick("PRODUCT_OWNER"),
                "NOTES": _default_pick("NOTES"),
            }

        cm: Dict[str, Optional[str]] = st.session_state["colmap"]

        st.markdown("### Column Mapping")
        # Group pickers into collapsible sections
        with st.expander("Core Entities (Programs, Vendors, Groups, Apps, Teams)", expanded=True):
            ce1, ce2 = st.columns(2)
            with ce1:
                st.markdown("**Program**")
                cm["PROGRAMNAME"] = st.selectbox("PROGRAMNAME (required)", options=["(none)"] + cols,
                                                 index=(cols.index(cm["PROGRAMNAME"]) + 1) if cm.get("PROGRAMNAME") in cols else 0,
                                                 key="map_programname")
                st.markdown("**Vendor**")
                cm["VENDORNAME"] = st.selectbox("VENDORNAME (required)", options=["(none)"] + cols,
                                                index=(cols.index(cm["VENDORNAME"]) + 1) if cm.get("VENDORNAME") in cols else 0,
                                                key="map_vendorname")
                st.markdown("**Application**")
                cm["GROUPNAME"] = st.selectbox("APP GROUP NAME (required)", options=["(none)"] + cols,
                                               index=(cols.index(cm["GROUPNAME"]) + 1) if cm.get("GROUPNAME") in cols else 0,
                                               key="map_groupname")
            with ce2:
                st.markdown("**Applications**")
                cm["APPNAME"] = st.selectbox("APPNAME (required)", options=["(none)"] + cols,
                                             index=(cols.index(cm["APPNAME"]) + 1) if cm.get("APPNAME") in cols else 0,
                                             key="map_appname")
                st.markdown("**Teams**")
                cm["TEAMNAME"] = st.selectbox("TEAMNAME (required)", options=["(none)"] + cols,
                                              index=(cols.index(cm["TEAMNAME"]) + 1) if cm.get("TEAMNAME") in cols else 0,
                                              key="map_teamname")

        with st.expander("Contracts Mapping", expanded=True):
            st.markdown("**Contracts (required)**")
            for key, label in [
                ("CONTRACT_START_FY", "CONTRACT_START_FY"),
                ("CONTRACT_END_FY", "CONTRACT_END_FY"),
                ("CONTRACT_ANNUAL_AMOUNT", "CONTRACT_ANNUAL_AMOUNT"),
                ("CONTRACT_RENEWAL_DATE", "CONTRACT_RENEWAL_DATE"),
            ]:
                cm[key] = st.selectbox(label, options=["(none)"] + cols,
                                       index=(cols.index(cm[key]) + 1) if cm.get(key) in cols else 0,
                                       key=f"map_contract_{key}")
            st.markdown("**Contracts (optional)**")
            for key, label in [
                ("CONTRACT_ESCALATION_PCT", "CONTRACT_ESCALATION_PCT"),
                ("CONTRACT_STATUS", "CONTRACT_STATUS"),
                ("CONTRACT_AGREEMENT_NUMBER", "CONTRACT_AGREEMENT_NUMBER"),
                ("CONTRACT_COMPANY_CODE", "CONTRACT_COMPANY_CODE"),
                ("CONTRACT_COST_CENTER", "CONTRACT_COST_CENTER"),
                ("CONTRACT_SERVICE_TYPE", "CONTRACT_SERVICE_TYPE"),
                ("INVOICE_RENEWAL_DATE", "INVOICE_RENEWAL_DATE"),
                ("CONTRACT_TOTAL_COST", "CONTRACT_TOTAL_COST"),
            ]:
                cm[key] = st.selectbox(label, options=["(none)"] + cols,
                                       index=(cols.index(cm[key]) + 1) if cm.get(key) in cols else 0,
                                       key=f"map_contract_opt_{key}")

        if not is_contracts_only:
            with st.expander("Invoices Mapping", expanded=False):
                st.markdown("**Invoices (required)**")
                cm["AMOUNT"] = st.selectbox("AMOUNT", options=["(none)"] + cols,
                                            index=(cols.index(cm["AMOUNT"]) + 1) if cm.get("AMOUNT") in cols else 0,
                                            key="map_inv_amt")
                cm["FISCAL_YEAR"] = st.selectbox("FISCAL_YEAR (Year)", options=["(none)"] + cols,
                                                 index=(cols.index(cm["FISCAL_YEAR"]) + 1) if cm.get("FISCAL_YEAR") in cols else 0,
                                                 key="map_inv_fy")
                cm["RENEWAL_MONTH"] = st.selectbox("RENEWAL_MONTH (1-12)", options=["(none)"] + cols,
                                                   index=(cols.index(cm["RENEWAL_MONTH"]) + 1) if cm.get("RENEWAL_MONTH") in cols else 0,
                                                   key="map_inv_rmonth")

                st.markdown("**Invoices (optional)**")
                for opt_key, label in [("INVOICE_STATUS", "INVOICE_STATUS"),("CONTRACT_ACTIVE", "CONTRACT_ACTIVE"),("SERIAL_NUMBER", "SERIAL_NUMBER"),("WORK_ORDER", "WORK_ORDER"),("COMPANY_CODE", "COMPANY_CODE"),("COST_CENTER", "COST_CENTER"),("PRODUCT_OWNER", "PRODUCT_OWNER"),("NOTES", "NOTES")]:
                    current = cm.get(opt_key)
                    cm[opt_key] = st.selectbox(label, options=["(none)"] + cols,
                                               index=(cols.index(current) + 1) if current in cols else 0,
                                               key=f"map_opt_{opt_key}")
        else:
            st.caption("Invoices are auto-generated from Contracts (no invoice fields to map).")

        st.markdown("---")

        # -------------------------
        # Build auto-previews from one sheet using the mapping
        # -------------------------
        def _col(name: str) -> Optional[str]:
            val = cm.get(name)
            return val if val and val != "(none)" and val in df_src.columns else None

        def _safe_num(series: pd.Series) -> pd.Series:
            return pd.to_numeric(series.astype(str).str.replace(",", ".", regex=False), errors="coerce")

        # Programs preview
        dfP = pd.DataFrame()
        if _col("PROGRAMNAME"):
            tmp = df_src[[_col("PROGRAMNAME")]].rename(columns={_col("PROGRAMNAME"): "PROGRAMNAME"})
            tmp["PROGRAMNAME"] = tmp["PROGRAMNAME"].astype(str).str.strip()
            tmp = tmp[~tmp["PROGRAMNAME"].apply(_blank_or_nan)]
            dfP = tmp.drop_duplicates(subset=["PROGRAMNAME"]).reset_index(drop=True)

        # Vendors preview
        dfV = pd.DataFrame()
        if _col("VENDORNAME"):
            tmp = df_src[[_col("VENDORNAME")]].rename(columns={_col("VENDORNAME"): "VENDORNAME"})
            tmp["VENDORNAME"] = tmp["VENDORNAME"].astype(str).str.strip()
            tmp = tmp[~tmp["VENDORNAME"].apply(_blank_or_nan)]
            dfV = tmp.drop_duplicates(subset=["VENDORNAME"]).reset_index(drop=True)

        # Groups preview  (TEAM × GROUP scoped + majority vendor)
        def _mode_nonblank(series: pd.Series) -> Optional[str]:
            s = series.dropna().astype(str).str.strip()
            s = s[s != ""]
            if s.empty:
                return None
            return s.value_counts().idxmax()

        dfG = pd.DataFrame()
        if _col("GROUPNAME"):
            tmp = df_src[[_col("GROUPNAME")]].rename(columns={_col("GROUPNAME"): "GROUPNAME"})
            tmp["GROUPNAME"] = tmp["GROUPNAME"].astype(str).str.strip()

            if _col("TEAMNAME"):
                tmp["TEAMNAME"] = df_src[_col("TEAMNAME")].astype(str).str.strip()
            else:
                tmp["TEAMNAME"] = None

            if _col("PROGRAMNAME"):
                tmp["PROGRAMNAME"] = df_src[_col("PROGRAMNAME")].astype(str).str.strip()

            if _col("VENDORNAME"):
                tmp["DEFAULT_VENDORNAME"] = df_src[_col("VENDORNAME")].astype(str).str.strip()

            # Drop blank group names; then aggregate by TEAMNAME × GROUPNAME
            tmp = tmp[~tmp["GROUPNAME"].apply(_blank_or_nan)].copy()

            # Choose vendor by majority occurrence within each TEAM×GROUP
            grp_keys = ["TEAMNAME", "GROUPNAME"]
            agg = {
                "PROGRAMNAME": "first",
                "DEFAULT_VENDORNAME": _mode_nonblank,
            }
            # Only aggregate columns that exist
            agg = {k: v for k, v in agg.items() if k in tmp.columns}
            dfG = (
                tmp.groupby([k for k in grp_keys if k in tmp.columns], dropna=False)
                .agg(agg)
                .reset_index()
            )


        # Applications preview
        dfA = pd.DataFrame()
        if _col("APPNAME"):
            tmp = df_src[[_col("APPNAME")]].rename(columns={_col("APPNAME"): "APPNAME"})
            tmp["APPNAME"] = tmp["APPNAME"].astype(str).str.strip()
            tmp = tmp[~tmp["APPNAME"].apply(_blank_or_nan)]
            if _col("GROUPNAME"):
                tmp["GROUPNAME"] = df_src[_col("GROUPNAME")].astype(str).str.strip()
            dfA = tmp.drop_duplicates(subset=["APPNAME","GROUPNAME"] if "GROUPNAME" in tmp.columns else ["APPNAME"]).reset_index(drop=True)

        # Teams preview
        dfT = pd.DataFrame()
        if _col("TEAMNAME"):
            tmp = df_src[[_col("TEAMNAME")]].rename(columns={_col("TEAMNAME"): "TEAMNAME"})
            tmp["TEAMNAME"] = tmp["TEAMNAME"].astype(str).str.strip()
            tmp = tmp[~tmp["TEAMNAME"].apply(_blank_or_nan)]
            if _col("PROGRAMNAME"):
                tmp["PROGRAMNAME"] = df_src[_col("PROGRAMNAME")].astype(str).str.strip()
            dfT = tmp.drop_duplicates(subset=["TEAMNAME"]).reset_index(drop=True)

        # -------------------------
        # Invoices preview (row-level requireds + validity)
        # -------------------------
        # Separate requireds for invoices and contracts
        INVOICE_REQUIRED_KEYS = [
            "PROGRAMNAME",      # Program Name
            "TEAMNAME",         # Team Name
            "GROUPNAME",        # Application
            "APPNAME",          # Application Instance
            "VENDORNAME",       # Vendor Name
            "FISCAL_YEAR",      # Year
            "RENEWAL_MONTH",    # Renewal Month
        ]
        CONTRACT_REQUIRED_KEYS = [
            "CONTRACT_START_FY","CONTRACT_END_FY","CONTRACT_ANNUAL_AMOUNT"
        ]

        def _has_required_mapping() -> Dict[str, bool]:
            return {key: bool(_col(key)) for key in REQUIRED_KEYS}

        def _row_is_missing(series_val: Any) -> bool:
            if series_val is None:
                return True
            s = str(series_val).strip()
            return s == "" or s.lower() == "nan"

        dfI = pd.DataFrame()
        def _has_invoice_mapping() -> Dict[str, bool]:
            return {key: bool(_col(key)) for key in INVOICE_REQUIRED_KEYS}
        invoice_diag = {"column_mapping": _has_invoice_mapping(), "row_counts": {}}
        missing_invoice_reqs = [key for key, ok in invoice_diag["column_mapping"].items() if not ok]

        if not missing_invoice_reqs:
            out = pd.DataFrame()
            out["AMOUNT"] = _safe_num(df_src[_col("AMOUNT")]) if _col("AMOUNT") else None
            out["FISCAL_YEAR"] = pd.to_numeric(df_src[_col("FISCAL_YEAR")], errors="coerce").astype("Int64")
            out["RENEWAL_MONTH"] = pd.to_numeric(df_src[_col("RENEWAL_MONTH")], errors="coerce").astype("Int64")

            # required name fields
            out["PROGRAMNAME"] = df_src[_col("PROGRAMNAME")].astype(str).str.strip()
            out["TEAMNAME"] = df_src[_col("TEAMNAME")].astype(str).str.strip()
            out["GROUPNAME"] = df_src[_col("GROUPNAME")].astype(str).str.strip()
            out["APPNAME"] = df_src[_col("APPNAME")].astype(str).str.strip()
            out["VENDORNAME"] = df_src[_col("VENDORNAME")].astype(str).str.strip()

            # optional extras
            for opt_name in ["INVOICE_STATUS","CONTRACT_ACTIVE","SERIAL_NUMBER","WORK_ORDER","COMPANY_CODE","COST_CENTER","PRODUCT_OWNER","NOTES"]:
                out[opt_name] = df_src[_col(opt_name)] if _col(opt_name) else None
            # contracts columns
            out["CONTRACT_START_FY"] = pd.to_numeric(df_src[_col("CONTRACT_START_FY")], errors="coerce").astype("Int64")
            out["CONTRACT_END_FY"] = pd.to_numeric(df_src[_col("CONTRACT_END_FY")], errors="coerce").astype("Int64")
            out["CONTRACT_ANNUAL_AMOUNT"] = _safe_num(df_src[_col("CONTRACT_ANNUAL_AMOUNT")])
            out["CONTRACT_ESCALATION_PCT"] = _safe_num(df_src[_col("CONTRACT_ESCALATION_PCT")]) if _col("CONTRACT_ESCALATION_PCT") else 0.0
            out["CONTRACT_STATUS"] = df_src[_col("CONTRACT_STATUS")].astype(str).str.strip() if _col("CONTRACT_STATUS") else "Active"
            out["CONTRACT_AGREEMENT_NUMBER"] = df_src[_col("CONTRACT_AGREEMENT_NUMBER")].astype(str).str.strip() if _col("CONTRACT_AGREEMENT_NUMBER") else None
            out["CONTRACT_COMPANY_CODE"] = df_src[_col("CONTRACT_COMPANY_CODE")].astype(str).str.strip() if _col("CONTRACT_COMPANY_CODE") else None
            out["CONTRACT_COST_CENTER"] = df_src[_col("CONTRACT_COST_CENTER")].astype(str).str.strip() if _col("CONTRACT_COST_CENTER") else None
            out["CONTRACT_SERVICE_TYPE"] = df_src[_col("CONTRACT_SERVICE_TYPE")].astype(str).str.strip() if _col("CONTRACT_SERVICE_TYPE") else None
            # Parse contract renewal date (assume month/day; attach fiscal year later per invoice)
            if _col("CONTRACT_RENEWAL_DATE"):
                try:
                    out["CONTRACT_RENEWAL_DATE"] = pd.to_datetime(df_src[_col("CONTRACT_RENEWAL_DATE")], errors="coerce").dt.date
                except Exception:
                    out["CONTRACT_RENEWAL_DATE"] = None
            else:
                out["CONTRACT_RENEWAL_DATE"] = None
            # Invoice renewal date (REQUIRED) — accept MM/DD/YYYY or MM/DD
            def _parse_md_or_mdy(val):
                try:
                    s = str(val).strip()
                except Exception:
                    return None
                if not s:
                    return None
                import re
                m = re.match(r"^\s*(\d{1,2})[\/-](\d{1,2})(?:[\/-](\d{2,4}))?\s*$", s)
                if m:
                    mm, dd, yy = m.group(1), m.group(2), m.group(3)
                    try:
                        mm_i, dd_i = int(mm), int(dd)
                        yy_i = int(yy) if yy else 2000  # placeholder year; only month/day are used downstream
                        return date(yy_i, mm_i, dd_i)
                    except Exception:
                        return None
                try:
                    return pd.to_datetime(s, errors="coerce").date()
                except Exception:
                    return None

            if _col("INVOICE_RENEWAL_DATE"):
                out["INVOICE_RENEWAL_DATE"] = df_src[_col("INVOICE_RENEWAL_DATE")].apply(_parse_md_or_mdy)
            else:
                out["INVOICE_RENEWAL_DATE"] = None
            # Optional total contract cost
            out["CONTRACT_TOTAL_COST"] = _safe_num(df_src[_col("CONTRACT_TOTAL_COST")]) if _col("CONTRACT_TOTAL_COST") else None

            # Derived date for preview — use INVOICE_RENEWAL_DATE day when available
            def _mk_date(row: pd.Series) -> Optional[date]:
                fy = row.get("FISCAL_YEAR")
                m  = row.get("RENEWAL_MONTH")
                try:
                    if pd.isna(fy) or pd.isna(m):
                        return None
                    mm = int(m); yy = int(fy)
                    dd = 1
                    try:
                        d_src = row.get("INVOICE_RENEWAL_DATE") or row.get("CONTRACT_RENEWAL_DATE")
                        if pd.notna(d_src):
                            d_parsed = pd.to_datetime(d_src, errors="coerce")
                            if pd.notna(d_parsed):
                                dd = int(d_parsed.day)
                    except Exception:
                        pass
                    if 1 <= mm <= 12:
                        return date(yy, mm, dd)
                    return None
                except Exception:
                    return None
            out["RENEWALDATE"] = out.apply(_mk_date, axis=1)

            # Row-level reasons
            def _row_reasons(r: pd.Series) -> List[str]:
                reasons: List[str] = []
                if _row_is_missing(r["PROGRAMNAME"]): reasons.append("missing PROGRAMNAME")
                if _row_is_missing(r["TEAMNAME"]): reasons.append("missing TEAMNAME")
                if _row_is_missing(r["GROUPNAME"]): reasons.append("missing GROUPNAME")
                if _row_is_missing(r["APPNAME"]): reasons.append("missing APPNAME")
                if _row_is_missing(r["VENDORNAME"]): reasons.append("missing VENDORNAME")
                if pd.isna(r["FISCAL_YEAR"]): reasons.append("missing FISCAL_YEAR")
                if pd.isna(r["RENEWAL_MONTH"]): reasons.append("missing RENEWAL_MONTH")
                # contract requireds
                if pd.isna(r["CONTRACT_START_FY"]): reasons.append("missing CONTRACT_START_FY")
                if pd.isna(r["CONTRACT_END_FY"]): reasons.append("missing CONTRACT_END_FY")
                if pd.isna(r["CONTRACT_ANNUAL_AMOUNT"]): reasons.append("missing CONTRACT_ANNUAL_AMOUNT")
                if r.get("INVOICE_RENEWAL_DATE") is None: reasons.append("missing INVOICE_RENEWAL_DATE")
                try:
                    mm = int(r["RENEWAL_MONTH"]) if pd.notna(r["RENEWAL_MONTH"]) else None
                    if mm is not None and (mm < 1 or mm > 12):
                        reasons.append("RENEWAL_MONTH out of range")
                except Exception:
                    reasons.append("RENEWAL_MONTH invalid")
                return reasons

            out["_REASONS"] = out.apply(_row_reasons, axis=1)
            out["_IS_VALID"] = out["_REASONS"].apply(lambda lst: len(lst) == 0)

            dfI = out

        # Store previews
        # Contracts preview (unique App×Team)
        dfK = pd.DataFrame()
        try:
            # Ensure contract required mapping exists
            contract_map_ok = all(_col(k) for k in CONTRACT_REQUIRED_KEYS)
            if contract_map_ok and _col("TEAMNAME") and _col("APPNAME"):
                dfK = pd.DataFrame({
                    "TEAMNAME": df_src[_col("TEAMNAME")].astype(str).str.strip(),
                    "APPNAME": df_src[_col("APPNAME")].astype(str).str.strip(),
                    "CONTRACT_START_FY": pd.to_numeric(df_src[_col("CONTRACT_START_FY")], errors="coerce").astype("Int64"),
                    "CONTRACT_END_FY": pd.to_numeric(df_src[_col("CONTRACT_END_FY")], errors="coerce").astype("Int64"),
                    "CONTRACT_ANNUAL_AMOUNT": _safe_num(df_src[_col("CONTRACT_ANNUAL_AMOUNT")]),
                    "CONTRACT_ESCALATION_PCT": _safe_num(df_src[_col("CONTRACT_ESCALATION_PCT")]) if _col("CONTRACT_ESCALATION_PCT") else 0.0,
                    "CONTRACT_STATUS": df_src[_col("CONTRACT_STATUS")].astype(str).str.strip() if _col("CONTRACT_STATUS") else "Active",
                    "CONTRACT_AGREEMENT_NUMBER": df_src[_col("CONTRACT_AGREEMENT_NUMBER")].astype(str).str.strip() if _col("CONTRACT_AGREEMENT_NUMBER") else None,
                    "CONTRACT_COMPANY_CODE": df_src[_col("CONTRACT_COMPANY_CODE")].astype(str).str.strip() if _col("CONTRACT_COMPANY_CODE") else None,
                    "CONTRACT_COST_CENTER": df_src[_col("CONTRACT_COST_CENTER")].astype(str).str.strip() if _col("CONTRACT_COST_CENTER") else None,
                    "CONTRACT_SERVICE_TYPE": df_src[_col("CONTRACT_SERVICE_TYPE")].astype(str).str.strip() if _col("CONTRACT_SERVICE_TYPE") else None,
                    "CONTRACT_TOTAL_COST": _safe_num(df_src[_col("CONTRACT_TOTAL_COST")]) if _col("CONTRACT_TOTAL_COST") else None,
                })
                if _col("GROUPNAME"):
                    dfK["GROUPNAME"] = df_src[_col("GROUPNAME")].astype(str).str.strip()
                # Derive CONTRACT_RENEWAL_MONTH from dates if mapping not provided
                try:
                    if _col("CONTRACT_RENEWAL_MONTH"):
                        dfK["CONTRACT_RENEWAL_MONTH"] = pd.to_numeric(df_src[_col("CONTRACT_RENEWAL_MONTH")], errors="coerce").astype("Int64")
                    else:
                        inv_m = pd.Series(dtype="Int64")
                        con_m = pd.Series(dtype="Int64")
                        if _col("INVOICE_RENEWAL_DATE"):
                            inv_m = df_src[_col("INVOICE_RENEWAL_DATE")].apply(_parse_md_or_mdy)
                            inv_m = pd.to_datetime(inv_m, errors="coerce").dt.month.astype("Int64")
                        if _col("CONTRACT_RENEWAL_DATE"):
                            con_m = pd.to_datetime(df_src[_col("CONTRACT_RENEWAL_DATE")], errors="coerce").dt.month.astype("Int64")
                        # Reindex to match dfK length if needed
                        if inv_m.shape[0] != dfK.shape[0]:
                            inv_m = inv_m.reindex(dfK.index)
                        if con_m.shape[0] != dfK.shape[0]:
                            con_m = con_m.reindex(dfK.index)
                        dfK["CONTRACT_RENEWAL_MONTH"] = inv_m.combine_first(con_m)
                except Exception:
                    pass
                # Renewal date
                if _col("CONTRACT_RENEWAL_DATE"):
                    try:
                        dfK["CONTRACT_RENEWAL_DATE"] = pd.to_datetime(df_src[_col("CONTRACT_RENEWAL_DATE")], errors="coerce").dt.date
                    except Exception:
                        dfK["CONTRACT_RENEWAL_DATE"] = None
                # Also include invoice renewal date for preview and later upsert_contract use
                if _col("INVOICE_RENEWAL_DATE"):
                    try:
                        dfK["INVOICE_RENEWAL_DATE"] = pd.to_datetime(df_src[_col("INVOICE_RENEWAL_DATE")], errors="coerce").dt.date
                    except Exception:
                        dfK["INVOICE_RENEWAL_DATE"] = None
                dfK = dfK[~dfK["TEAMNAME"].apply(_blank_or_nan) & ~dfK["APPNAME"].apply(_blank_or_nan)]
                # Prefer later rows on duplicates so user-entered rows override template examples
                dfK = dfK.drop_duplicates(subset=["TEAMNAME","APPNAME"], keep="last").reset_index(drop=True)
        except Exception:
            dfK = pd.DataFrame()

        st.session_state["previews"] = {
            "Programs": dfP, "Vendors": dfV, "Groups": dfG, "Apps": dfA, "Teams": dfT, "Contracts": dfK, "Invoices": dfI
        }

        # ----- Show previews + preflight -----
        st.markdown("#### Previews")
        p1, p2 = st.columns(2)
        with p1:
            st.markdown("**Programs (unique)**")
            st.dataframe(dfP.head(400), use_container_width=True, height=220)
            st.markdown("**Vendors (unique)**")
            st.dataframe(dfV.head(400), use_container_width=True, height=220)
            st.markdown("**Applications (unique)**")
            st.dataframe(dfG.head(400), use_container_width=True, height=240)
        with p2:
            st.markdown("**Applications (unique)**")
            st.dataframe(dfA.head(400), use_container_width=True, height=240)
            st.markdown("**Teams (unique)**")
            st.dataframe(dfT.head(400), use_container_width=True, height=220)
        st.markdown("**Contracts (App × Team, unique)**")
        st.dataframe(dfK.head(400), use_container_width=True, height=220)

        if not is_contracts_only:
            st.markdown("**Invoices — Row‑level Preflight**")
            if missing_invoice_reqs:
                st.error(
                    "Missing column mappings for required field(s): "
                    + ", ".join(missing_invoice_reqs)
                    + ". Map these columns to proceed."
                )
                df_valid_preview = pd.DataFrame()
                df_invalid_preview = pd.DataFrame()
            else:
                c_valid, c_invalid = st.columns(2)
                with c_valid:
                    st.caption("✅ Will be imported (valid rows)")
                    df_valid_preview = dfI[dfI["_IS_VALID"]].copy() if ("_IS_VALID" in dfI.columns) else pd.DataFrame()
                    show_cols_valid = ["PROGRAMNAME","TEAMNAME","GROUPNAME","APPNAME","VENDORNAME","FISCAL_YEAR","RENEWAL_MONTH","INVOICE_RENEWAL_DATE","RENEWALDATE","AMOUNT"]
                    show_cols_valid = [c for c in show_cols_valid if c in df_valid_preview.columns]
                    st.dataframe(df_valid_preview[show_cols_valid].head(400), use_container_width=True, height=260)
                with c_invalid:
                    st.caption("❌ Will be skipped (invalid rows) — with reasons")
                    df_invalid_preview = dfI[~dfI["_IS_VALID"]].copy() if ("_IS_VALID" in dfI.columns) else pd.DataFrame()
                    if not df_invalid_preview.empty and "_REASONS" in df_invalid_preview.columns:
                        df_invalid_preview["_REASONS_STR"] = df_invalid_preview["_REASONS"].apply(lambda xs: "; ".join(xs))
                    show_cols_invalid = ["PROGRAMNAME","TEAMNAME","GROUPNAME","APPNAME","VENDORNAME","FISCAL_YEAR","RENEWAL_MONTH","INVOICE_RENEWAL_DATE","AMOUNT","AMOUNT_NEXT_YEAR","_REASONS_STR"]
                    show_cols_invalid = [c for c in show_cols_invalid if c in df_invalid_preview.columns]
                    st.dataframe(df_invalid_preview[show_cols_invalid].head(400), use_container_width=True, height=260)
        else:
            st.markdown("**Contracts — Preflight**")
            # Check required mappings for contracts
            contract_required_map = ["TEAMNAME","APPNAME","CONTRACT_START_FY","CONTRACT_END_FY","CONTRACT_ANNUAL_AMOUNT"]
            contract_missing_map = [k for k in contract_required_map if not _col(k)]
            if contract_missing_map:
                st.error("Missing column mappings for required contract field(s): " + ", ".join(contract_missing_map))
            # Validate dfK rows
            def _contract_reasons(r: pd.Series) -> list:
                reasons = []
                if _blank_or_nan(r.get("TEAMNAME")): reasons.append("missing TEAMNAME")
                if _blank_or_nan(r.get("APPNAME")): reasons.append("missing APPNAME")
                if pd.isna(r.get("CONTRACT_START_FY")): reasons.append("missing CONTRACT_START_FY")
                if pd.isna(r.get("CONTRACT_END_FY")): reasons.append("missing CONTRACT_END_FY")
                if pd.isna(r.get("CONTRACT_ANNUAL_AMOUNT")): reasons.append("missing CONTRACT_ANNUAL_AMOUNT")
                if _blank_or_nan(r.get("CONTRACT_RENEWAL_DATE")): reasons.append("missing CONTRACT_RENEWAL_DATE")
                return reasons
            dfK_preview = dfK.copy() if isinstance(dfK, pd.DataFrame) else pd.DataFrame()
            if not dfK_preview.empty:
                dfK_preview["_REASONS"] = dfK_preview.apply(_contract_reasons, axis=1)
                dfK_valid = dfK_preview[dfK_preview["_REASONS"].apply(len) == 0]
                dfK_invalid = dfK_preview[dfK_preview["_REASONS"].apply(len) > 0]
            else:
                dfK_valid = pd.DataFrame(); dfK_invalid = pd.DataFrame()
            cc1, cc2 = st.columns(2)
            with cc1:
                st.caption("✅ Contracts to import (valid, unique Team×App)")
                show_cols = [
                    "TEAMNAME","APPNAME",
                    "CONTRACT_RENEWAL_DATE","INVOICE_RENEWAL_DATE","CONTRACT_RENEWAL_MONTH",
                    "CONTRACT_START_FY","CONTRACT_END_FY","CONTRACT_ANNUAL_AMOUNT"
                ]
                show_cols = [c for c in show_cols if c in dfK_valid.columns]
                st.dataframe(dfK_valid[show_cols].head(400), use_container_width=True, height=260)
            with cc2:
                st.caption("❌ Skipped contracts (invalid) — with reasons")
                if not dfK_invalid.empty:
                    dfK_invalid["_REASONS_STR"] = dfK_invalid["_REASONS"].apply(lambda xs: "; ".join(xs))
                show_cols2 = [
                    "TEAMNAME","APPNAME",
                    "CONTRACT_RENEWAL_DATE","INVOICE_RENEWAL_DATE","CONTRACT_RENEWAL_MONTH",
                    "CONTRACT_START_FY","CONTRACT_END_FY","CONTRACT_ANNUAL_AMOUNT",
                    "_REASONS_STR"
                ]
                show_cols2 = [c for c in show_cols2 if c in dfK_invalid.columns]
                st.dataframe(dfK_invalid[show_cols2].head(400), use_container_width=True, height=260)

        with st.expander("Preflight Summary", expanded=True):
            if not is_contracts_only:
                if missing_invoice_reqs:
                    st.warning("Map all required invoice columns to enable import.")
                    total_rows = int(dfI.shape[0])
                    st.write(f"- Total invoice rows detected: **{total_rows}**")
                else:
                    total_rows = int(dfI.shape[0])
                    valid_rows_n = int(df_valid_preview.shape[0])
                    invalid_rows_n = int(df_invalid_preview.shape[0])
                    st.write(
                        f"- Total invoice rows detected: **{total_rows}**  \n"
                        f"- Valid rows (will import): **{valid_rows_n}**  \n"
                        f"- Invalid rows (skipped): **{invalid_rows_n}**"
                    )
                    if invalid_rows_n > 0 and "_REASONS" in df_invalid_preview.columns:
                        agg: Dict[str, int] = {}
                        for reasons in df_invalid_preview["_REASONS"].tolist():
                            for reason in reasons:
                                agg[reason] = agg.get(reason, 0) + 1
                        if agg:
                            st.write("**Top issues:**")
                            for reason, n in sorted(agg.items(), key=lambda x: -x[1]):
                                st.write(f"- {reason}: **{n}**")
            else:
                total_contracts = int(dfK.shape[0]) if isinstance(dfK, pd.DataFrame) else 0
                valid_n = int(dfK_valid.shape[0]) if 'dfK_valid' in locals() else 0
                invalid_n = int(dfK_invalid.shape[0]) if 'dfK_invalid' in locals() else 0
                st.write(
                    f"- Total contracts detected (unique Team×App): **{total_contracts}**  \n"
                    f"- Valid contracts (will import): **{valid_n}**  \n"
                    f"- Invalid contracts (skipped): **{invalid_n}**"
                )
                if invalid_n > 0 and not dfK_invalid.empty and "_REASONS" in dfK_invalid.columns:
                    agg2: Dict[str, int] = {}
                    for reasons in dfK_invalid["_REASONS"].tolist():
                        for reason in reasons:
                            agg2[reason] = agg2.get(reason, 0) + 1
                    if agg2:
                        st.write("**Top issues:**")
                        for reason, n in sorted(agg2.items(), key=lambda x: -x[1]):
                            st.write(f"- {reason}: **{n}**")

            # Duplicate checks (block import if duplicates)
            has_dupes_contracts = False
            has_dupes_invoices = False
            try:
                dfK = st.session_state["previews"].get("Contracts")
                if isinstance(dfK, pd.DataFrame) and not dfK.empty:
                    dupk = dfK[dfK.duplicated(subset=["TEAMNAME","APPNAME"], keep=False)].copy()
                    if not dupk.empty:
                        has_dupes_contracts = True
                        st.error(f"Duplicate Contracts detected (same TEAMNAME+APPNAME): {len(dupk)} row(s).")
                        st.dataframe(dupk, use_container_width=True, height=180)
            except Exception:
                pass

        # -------------------------
        # Contracts → Planned invoices (preview)
        # -------------------------
        try:
            dfK_preview_src = st.session_state.get("previews", {}).get("Contracts")
            if isinstance(dfK_preview_src, pd.DataFrame) and not dfK_preview_src.empty:
                with st.expander("Contracts → Planned invoices (preview)", expanded=False):
                    # Use only rows that have required fields
                    dfKp = dfK_preview_src.copy()
                    for col in ("TEAMNAME","APPNAME","CONTRACT_START_FY","CONTRACT_END_FY"):
                        if col not in dfKp.columns:
                            dfKp[col] = None
                    dfKp = dfKp[
                        (~dfKp["TEAMNAME"].apply(_blank_or_nan)) &
                        (~dfKp["APPNAME"].apply(_blank_or_nan)) &
                        (pd.notna(dfKp["CONTRACT_START_FY"])) &
                        (pd.notna(dfKp["CONTRACT_END_FY"]))
                    ].reset_index(drop=True)
                    if dfKp.empty:
                        st.info("No valid contract rows to preview.")
                    else:
                        # Build maps from DB
                        teams_now = list_teams(); tmap_now = {}
                        if teams_now is not None and not teams_now.empty:
                            for _, r0 in teams_now.iterrows():
                                tmap_now[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID
                        apps_now = list_applications(); amap_now = {}
                        if apps_now is not None and not apps_now.empty:
                            for _, r0 in apps_now.iterrows():
                                amap_now[str(r0.APPLICATIONNAME).strip().upper()] = r0.APPLICATIONID

                        preview_rows: List[Dict[str, Any]] = []
                        candidate_pairs: List[Tuple[str,str]] = []
                        # First resolve pairs and compute FY spans
                        for _, r in dfKp.iterrows():
                            tname = str(r.get("TEAMNAME") or "").strip()
                            aname = str(r.get("APPNAME") or "").strip()
                            gname = str(r.get("GROUPNAME") or "").strip() if "GROUPNAME" in dfKp.columns else ""
                            sfy = int(pd.to_numeric(r.get("CONTRACT_START_FY"), errors="coerce")) if pd.notna(r.get("CONTRACT_START_FY")) else None
                            efy = int(pd.to_numeric(r.get("CONTRACT_END_FY"), errors="coerce")) if pd.notna(r.get("CONTRACT_END_FY")) else None
                            fy_span = max(0, (efy - sfy + 1)) if (sfy is not None and efy is not None) else 0
                            # Resolve canonical app name like in import
                            final_appname = aname
                            if gname:
                                prefix = f"{gname} - "
                                if aname.upper().startswith(prefix.upper()):
                                    final_appname = aname
                                elif aname.upper() == gname.upper():
                                    final_appname = f"{gname} - Instance"
                                else:
                                    final_appname = f"{gname} - {aname}"
                            tid = tmap_now.get(tname.upper())
                            aid = amap_now.get(final_appname.upper()) or amap_now.get(aname.upper())
                            row: Dict[str, Any] = {
                                "TEAMNAME": tname,
                                "APPNAME": aname,
                                "CANONICAL_APPNAME": final_appname,
                                "START_FY": sfy,
                                "END_FY": efy,
                                "FY_SPAN": fy_span,
                            }
                            if tid and aid and fy_span > 0:
                                row["_TID"] = tid; row["_AID"] = aid
                                candidate_pairs.append((tid, aid))
                            else:
                                reason = []
                                if not tid: reason.append("Team not found")
                                if not aid: reason.append("Application not found")
                                if fy_span <= 0: reason.append("Invalid FY span")
                                row["UNMATCHED_REASON"] = "; ".join(reason)
                            preview_rows.append(row)

                        # Fetch existing recurring invoice FYs for these pairs
                        existing_map: set[Tuple[str,str,int]] = set()
                        a_ids = sorted(set(a for _, a in candidate_pairs))
                        t_ids = sorted(set(t for t, _ in candidate_pairs))
                        if a_ids and t_ids:
                            try:
                                placeholders_a = ",".join(["%s"] * len(a_ids))
                                placeholders_t = ",".join(["%s"] * len(t_ids))
                                sql_exist = f"""
                                    SELECT APPLICATIONID, TEAMID, FISCAL_YEAR
                                    FROM INVOICES
                                    WHERE COALESCE(INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'
                                      AND APPLICATIONID IN ({placeholders_a})
                                      AND TEAMID IN ({placeholders_t})
                                """
                                df_exist = fetch_df(sql_exist, tuple(list(a_ids) + list(t_ids)))
                                if df_exist is not None and not df_exist.empty:
                                    for _, re in df_exist.iterrows():
                                        try:
                                            existing_map.add((str(re["TEAMID"]), str(re["APPLICATIONID"]), int(re["FISCAL_YEAR"])) )
                                        except Exception:
                                            pass
                            except Exception:
                                pass

                        # Finalize counts
                        for row in preview_rows:
                            tid = row.get("_TID"); aid = row.get("_AID")
                            sfy = row.get("START_FY"); efy = row.get("END_FY")
                            if tid and aid and isinstance(sfy, int) and isinstance(efy, int) and efy >= sfy:
                                yrs = list(range(int(sfy), int(efy) + 1))
                                exists_n = sum(1 for y in yrs if (str(tid), str(aid), int(y)) in existing_map)
                                row["EXISTING_PLANNED"] = exists_n
                                row["WILL_CREATE"] = max(0, len(yrs) - exists_n)
                            else:
                                row.setdefault("EXISTING_PLANNED", None)
                                row.setdefault("WILL_CREATE", None)

                        out_df = pd.DataFrame(preview_rows)
                        # Tidy output
                        show_cols = [
                            "TEAMNAME","CANONICAL_APPNAME","START_FY","END_FY","FY_SPAN","EXISTING_PLANNED","WILL_CREATE","UNMATCHED_REASON"
                        ]
                        show_cols = [c for c in show_cols if c in out_df.columns]
                        st.dataframe(out_df[show_cols], use_container_width=True, height=260)
                        try:
                            total_create = int(out_df["WILL_CREATE"].fillna(0).sum())
                            st.caption(f"Estimated new planned invoices to create: {total_create}")
                        except Exception:
                            pass
        except Exception:
            pass
            try:
                if isinstance(df_valid_preview, pd.DataFrame) and not df_valid_preview.empty:
                    dupi = df_valid_preview[df_valid_preview.duplicated(subset=["TEAMNAME","APPNAME","FISCAL_YEAR"], keep=False)].copy()
                    if not dupi.empty:
                        has_dupes_invoices = True
                        st.error(f"Duplicate Invoices detected (same TEAMNAME+APPNAME+FISCAL_YEAR): {len(dupi)} row(s).")
                        st.dataframe(dupi, use_container_width=True, height=180)
            except Exception:
                pass

        # -------------------------
        # Import ALL in safe order
        # -------------------------
        prog_widget = st.progress(0, text="Ready.")
        status_txt = st.empty()

        def _tick(pct: int, msg: str, start_time: float) -> None:
            elapsed = time.time() - start_time
            prog_widget.progress(pct, text=f"{msg}  ⏱ {elapsed:,.1f}s")
            status_txt.caption(f"⏱ Elapsed: **{elapsed:,.1f} s**")

        # Enable import if either valid invoices exist OR contracts are present, and no duplicates
        # Ensure df_valid_preview is defined in all modes
        if 'df_valid_preview' not in locals():
            df_valid_preview = pd.DataFrame()
        disabled_btn = (df_valid_preview.empty and (dfK is None or dfK.empty))
        try:
            if isinstance(dfK, pd.DataFrame) and not dfK.empty and dfK.duplicated(subset=["TEAMNAME","APPNAME"]).any():
                disabled_btn = True
        except Exception:
            pass
        try:
            if isinstance(df_valid_preview, pd.DataFrame) and not df_valid_preview.empty and df_valid_preview.duplicated(subset=["TEAMNAME","APPNAME","FISCAL_YEAR"]).any():
                disabled_btn = True
        except Exception:
            pass

        if st.button("Import ALL", icon=":material/download:", type="primary", use_container_width=True, disabled=disabled_btn):
            start_time = time.time()
            import_batch_id = str(uuid.uuid4())
            try:
                pre = {
                    "vendors": _table_count("VENDORS"),
                    "programs": _table_count("PROGRAMS"),
                    "teams": _table_count("TEAMS"),
                    "groups": _table_count("APPLICATION_GROUPS"),
                    "apps": _table_count("APPLICATIONS"),
                    "invoices": _table_count("INVOICES"),
                }

                # Refresh previews (safe)
                dfP = _get_preview("Programs")
                dfV = _get_preview("Vendors")
                dfG = _get_preview("Groups")
                dfA = _get_preview("Apps")
                dfT = _get_preview("Teams")
                dfI = _get_preview("Invoices")
                dfK = _get_preview("Contracts")

                # ---------- Vendors ----------
                _tick(5, "Importing Vendors...", start_time)
                existing_vendors = list_vendors()
                vmap: Dict[str, str] = {}
                if existing_vendors is not None and not existing_vendors.empty:
                    for _, r0 in existing_vendors.iterrows():
                        vmap[str(r0.VENDORNAME).strip().upper()] = r0.VENDORID
                inserted_v = updated_v = 0
                if dfV is not None and not dfV.empty:
                    for _, row in dfV.iterrows():
                        vname = str(row["VENDORNAME"]).strip()
                        if _blank_or_nan(vname):
                            continue
                        existed = vname.upper() in vmap
                        vid = vmap.get(vname.upper()) or str(uuid.uuid4())
                        upsert_vendor(vid, vname)
                        vmap[vname.upper()] = vid
                        inserted_v += 0 if existed else 1
                        updated_v  += 1 if existed else 0

                # ---------- Programs ----------
                _tick(12, "Importing Programs...", start_time)
                existing_programs = list_programs()
                pmap: Dict[str, str] = {}
                if existing_programs is not None and not existing_programs.empty:
                    for _, r0 in existing_programs.iterrows():
                        pmap[str(r0.PROGRAMNAME).strip().upper()] = r0.PROGRAMID
                inserted_p = updated_p = 0
                if dfP is not None and not dfP.empty:
                    for _, row in dfP.iterrows():
                        pname = str(row["PROGRAMNAME"]).strip()
                        if _blank_or_nan(pname):
                            continue
                        existed = pname.upper() in pmap
                        pid = pmap.get(pname.upper()) or str(uuid.uuid4())
                        upsert_program(pid, pname, owner=None, fte=None)
                        pmap[pname.upper()] = pid
                        inserted_p += 0 if existed else 1
                        updated_p  += 1 if existed else 0

                # ---------- Teams ----------
                _tick(22, "Importing Teams...", start_time)
                existing_teams = list_teams()
                tmap: Dict[str, str] = {}
                if existing_teams is not None and not existing_teams.empty:
                    for _, r0 in existing_teams.iterrows():
                        tmap[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID
                inserted_t = updated_t = 0
                if dfT is not None and not dfT.empty:
                    for _, row in dfT.iterrows():
                        tname = str(row["TEAMNAME"]).strip()
                        if _blank_or_nan(tname):
                            continue
                        pid = None
                        if "PROGRAMNAME" in row and not _blank_or_nan(row["PROGRAMNAME"]):
                            pid = pmap.get(str(row["PROGRAMNAME"]).strip().upper())
                        existed = tname.upper() in tmap
                        tid = tmap.get(tname.upper()) or str(uuid.uuid4())
                        upsert_team(
                            team_id=tid,
                            name=tname,
                            program_id=pid,
                            team_fte=None, delivery_team_fte=None,
                            contractor_c_fte=None, contractor_cs_fte=None
                        )
                        tmap[tname.upper()] = tid
                        inserted_t += 0 if existed else 1
                        updated_t  += 1 if existed else 0

                # ---------- Groups ----------
                _tick(35, "Importing Applications...", start_time)

                # Build TEAM map (already built above, but refresh safely)
                existing_teams2 = list_teams()
                tmap: Dict[str, str] = {}
                if existing_teams2 is not None and not existing_teams2.empty:
                    for _, r0 in existing_teams2.iterrows():
                        tmap[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID

                # Build VENDOR map
                existing_vendors2 = list_vendors()
                vmap: Dict[str, str] = {}
                if existing_vendors2 is not None and not existing_vendors2.empty:
                    for _, r0 in existing_vendors2.iterrows():
                        vmap[str(r0.VENDORNAME).strip().upper()] = r0.VENDORID

                # Build a composite GROUP map from DB: (TEAMID, UPPER(GROUPNAME)) -> GROUPID
                gpair_map: Dict[Tuple[str, str], str] = {}
                cur_groups_all = fetch_df("""
                    SELECT g.GROUPID, g.GROUPNAME, g.TEAMID
                    FROM APPLICATION_GROUPS g
                """)
                if cur_groups_all is not None and not cur_groups_all.empty:
                    for _, r in cur_groups_all.iterrows():
                        gid = str(r["GROUPID"]).strip()
                        gname = str(r["GROUPNAME"]).strip().upper()
                        tid = str(r["TEAMID"]).strip() if pd.notna(r["TEAMID"]) else ""
                        if gid and gname and tid:
                            gpair_map[(tid, gname)] = gid

                inserted_g = updated_g = 0
                if dfG is not None and not dfG.empty:
                    for _, row in dfG.iterrows():
                        gname = str(row["GROUPNAME"]).strip()
                        tname = str(row["TEAMNAME"]).strip() if "TEAMNAME" in row else ""
                        if _blank_or_nan(gname) or _blank_or_nan(tname):
                            continue

                        tid = tmap.get(tname.upper())
                        if not tid:
                            # cannot place group without a team
                            continue

                        default_vendor_id = None
                        if "DEFAULT_VENDORNAME" in row and not _blank_or_nan(row["DEFAULT_VENDORNAME"]):
                            default_vendor_id = vmap.get(str(row["DEFAULT_VENDORNAME"]).strip().upper())

                        key = (tid, gname.upper())
                        existed = key in gpair_map
                        gid = gpair_map.get(key) or str(uuid.uuid4())

                        # Upsert by (TEAMID, GROUPNAME). Your upsert helper likely merges on GROUPID.
                        upsert_application_group(
                            gid,
                            gname,
                            team_id=tid,
                            default_vendor_id=default_vendor_id,
                            owner=None
                        )

                        gpair_map[key] = gid
                        inserted_g += 0 if existed else 1
                        updated_g  += 1 if existed else 0


                    # ---------- Applications ----------
                    _tick(48, "Importing Application Instances...", start_time)

                    # Build APPLICATION map by name (still global, since app names look canonical)
                    existing_apps = list_applications()
                    amap: Dict[str, str] = {}
                    if existing_apps is not None and not existing_apps.empty:
                        for _, r0 in existing_apps.iterrows():
                            amap[str(r0.APPLICATIONNAME).strip().upper()] = r0.APPLICATIONID

                    inserted_a = updated_a = 0
                    if dfA is not None and not dfA.empty:
                        # refresh composite group map (TEAMID × GROUPNAME)
                        cur_groups = fetch_df("SELECT GROUPID, GROUPNAME, TEAMID FROM APPLICATION_GROUPS")
                        gpair_map: Dict[Tuple[str,str], str] = {}
                        gname_map: Dict[str, str] = {}
                        if cur_groups is not None and not cur_groups.empty:
                            for _, r in cur_groups.iterrows():
                                gid = str(r["GROUPID"]).strip()
                                gname = str(r["GROUPNAME"]).strip().upper()
                                tid = str(r["TEAMID"]).strip()
                                if gid and gname and tid:
                                    gpair_map[(tid, gname)] = gid
                                    # Also map by name only (GROUPNAME is globally unique in schema)
                                    if gname not in gname_map:
                                        gname_map[gname] = gid

                        # Need team map to resolve TEAMNAME → TEAMID
                        teams_now = list_teams()
                        tmap_now: Dict[str, str] = {}
                        if teams_now is not None and not teams_now.empty:
                            for _, r0 in teams_now.iterrows():
                                tmap_now[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID

                        for _, row in dfA.iterrows():
                            aname = str(row["APPNAME"]).strip()
                            gname = str(row["GROUPNAME"]).strip() if "GROUPNAME" in row else ""
                            tname = str(row["TEAMNAME"]).strip() if "TEAMNAME" in row else ""
                            if _blank_or_nan(aname):
                                continue

                            # If we have both Team & Group, resolve to the correct group's ID
                            gid = None
                            if not _blank_or_nan(gname) and not _blank_or_nan(tname):
                                tid = tmap_now.get(tname.upper())
                                if tid:
                                    gid = gpair_map.get((tid, gname.upper()))
                            # Fallback: resolve by group name only (unique globally)
                            if gid is None and not _blank_or_nan(gname):
                                gid = gname_map.get(gname.upper())

                            # Canonicalize application name to "Group - Suffix" when group provided
                            final_name = aname
                            if not _blank_or_nan(gname):
                                prefix = f"{gname} - "
                                if aname.upper().startswith(prefix.upper()):
                                    final_name = aname
                                elif aname.upper() == gname.upper():
                                    final_name = f"{gname} - Instance"
                                else:
                                    final_name = f"{gname} - {aname}"

                            existing_id_by_raw = amap.get(aname.upper())
                            existed = final_name.upper() in amap or (existing_id_by_raw is not None)

                            # If an app exists named exactly as the Group (bad data), rename it to canonical instead of creating a duplicate
                            if (not _blank_or_nan(gname)) and (aname.upper() == gname.upper()) and existing_id_by_raw and gid:
                                try:
                                    execute(
                                        "UPDATE APPLICATIONS SET APPLICATIONNAME=%s, GROUPID = COALESCE(GROUPID, %s) WHERE APPLICATIONID=%s",
                                        (final_name, gid, existing_id_by_raw),
                                    )
                                    # update local map
                                    amap.pop(aname.upper(), None)
                                    amap[final_name.upper()] = existing_id_by_raw
                                    updated_a += 1
                                    continue
                                except Exception:
                                    # fall through to normal upsert if rename fails
                                    pass

                            aid = amap.get(final_name.upper()) or existing_id_by_raw or str(uuid.uuid4())

                            upsert_application_instance(
                                application_id=aid,
                                group_id=(gid or None),
                                application_name=final_name,
                                add_info=None,
                                vendor_id=None
                            )
                            amap[final_name.upper()] = aid
                            inserted_a += 0 if existed else 1
                            updated_a  += 1 if existed else 0


                        # ---------- Contracts ----------
                        _tick(58, "Importing Contracts...", start_time)

                        ensure_contracts_table()
                        # Refresh maps for IDs
                        cur_teams3 = list_teams(); tmap3 = {}
                        if cur_teams3 is not None and not cur_teams3.empty:
                            for _, r0 in cur_teams3.iterrows():
                                tmap3[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID

                        cur_apps3 = list_applications(); amap3 = {}
                        if cur_apps3 is not None and not cur_apps3.empty:
                            for _, r0 in cur_apps3.iterrows():
                                amap3[str(r0.APPLICATIONNAME).strip().upper()] = r0.APPLICATIONID

                        created_c = updated_c = 0
                        created_invoices_total = 0
                        created_contracts_detail: List[Dict[str, Any]] = []
                        if dfK is not None and not dfK.empty:
                            from db import sync_contract_invoices
                            unmatched_contracts: List[Dict[str, Any]] = []
                            failed_contracts: List[Dict[str, Any]] = []
                            for _, row in dfK.iterrows():
                                tname = str(row.get("TEAMNAME") or "").strip()
                                aname = str(row.get("APPNAME") or "").strip()
                                gname = str(row.get("GROUPNAME") or "").strip() if "GROUPNAME" in row else ""
                                if _blank_or_nan(tname) or _blank_or_nan(aname):
                                    continue
                                tid = tmap3.get(tname.upper())
                                # Resolve ApplicationID by canonical name "Group - Suffix" where possible
                                final_appname = aname
                                if gname:
                                    prefix = f"{gname} - "
                                    if aname.upper().startswith(prefix.upper()):
                                        final_appname = aname
                                    elif aname.upper() == gname.upper():
                                        final_appname = f"{gname} - Instance"
                                    else:
                                        final_appname = f"{gname} - {aname}"
                                aid = amap3.get(final_appname.upper()) or amap3.get(aname.upper())
                                if not tid or not aid:
                                    reason = ", ".join([
                                        r for r in [
                                            ("Team not found" if not tid else None),
                                            ("Application not found" if not aid else None)
                                        ] if r
                                    ]) or "Unresolved"
                                    unmatched_contracts.append({
                                        "TEAMNAME": tname,
                                        "APPNAME": aname,
                                        "CANONICAL_APPNAME": final_appname,
                                        "GROUPNAME": gname or None,
                                        "REASON": reason,
                                    })
                                    continue
                                # Safe parse contract fields
                                def _to_int_default(val, default):
                                    try:
                                        if val is None or (hasattr(pd, 'isna') and pd.isna(val)):
                                            return default
                                        s = str(val).strip()
                                        if s == "":
                                            return default
                                        v = int(float(s))
                                        return v
                                    except Exception:
                                        return default
                                def _to_float_default(val, default):
                                    try:
                                        if val is None or (hasattr(pd, 'isna') and pd.isna(val)):
                                            return default
                                        s = str(val).strip()
                                        if s == "":
                                            return default
                                        return float(s)
                                    except Exception:
                                        return default

                                start_fy_val = _to_int_default(row.get("CONTRACT_START_FY"), None)
                                end_fy_val   = _to_int_default(row.get("CONTRACT_END_FY"), None)
                                annual_val   = _to_float_default(row.get("CONTRACT_ANNUAL_AMOUNT"), None)
                                esc_val      = _to_float_default(row.get("CONTRACT_ESCALATION_PCT"), 0.0)
                                rmon_val     = _to_int_default(row.get("CONTRACT_RENEWAL_MONTH"), 1)
                                if rmon_val is None or rmon_val < 1 or rmon_val > 12:
                                    rmon_val = 1

                                if start_fy_val is None or end_fy_val is None or annual_val is None:
                                    failed_contracts.append({
                                        "TEAMNAME": tname,
                                        "APPNAME": final_appname,
                                        "REASON": "Missing/invalid START_FY, END_FY, or ANNUAL_AMOUNT",
                                        "START_FY": start_fy_val,
                                        "END_FY": end_fy_val,
                                        "ANNUAL_AMOUNT": annual_val,
                                    })
                                    continue
                                if end_fy_val < start_fy_val:
                                    failed_contracts.append({
                                        "TEAMNAME": tname,
                                        "APPNAME": final_appname,
                                        "REASON": "END_FY < START_FY",
                                        "START_FY": start_fy_val,
                                        "END_FY": end_fy_val,
                                    })
                                    continue

                                try:
                                    import uuid as _uuid
                                    cid = str(_uuid.uuid5(_uuid.NAMESPACE_URL, f"contract:{aid}:{tid}"))
                                    # Guard: prevent overlapping contracts for the same Application Instance across any Team
                                    try:
                                        overlap_b = fetch_df(
                                            """
                                            SELECT TOP 1 c.CONTRACT_ID, t.TEAMNAME, c.START_FY, c.END_FY
                                            FROM CONTRACTS c
                                            LEFT JOIN TEAMS t ON t.TEAMID = c.TEAMID
                                            WHERE c.APPLICATIONID = %s
                                              AND NOT (c.END_FY < %s OR c.START_FY > %s)
                                              AND NOT (c.TEAMID = %s)
                                            ORDER BY c.CREATED_AT DESC
                                            """,
                                            (aid, int(start_fy_val), int(end_fy_val), tid)
                                        )
                                    except Exception:
                                        overlap_b = None
                                    if overlap_b is not None and not overlap_b.empty:
                                        failed_contracts.append({
                                            "TEAMNAME": tname,
                                            "APPNAME": final_appname,
                                            "REASON": f"Overlapping contract exists for this Application Instance (Team: {str(overlap_b.iloc[0].get('TEAMNAME') or overlap_b.iloc[0].get('TEAMID') or '(unknown team)')}). Skipped.",
                                        })
                                        continue
                                    upsert_contract(
                                        contract_id=cid,
                                        application_id=aid,
                                        team_id=tid,
                                        start_fy=int(start_fy_val),
                                        end_fy=int(end_fy_val),
                                        renewal_month=int(rmon_val),
                                        annual_amount=float(annual_val),
                                        escalation_pct=float(esc_val),
                                        status=(str(row.get("CONTRACT_STATUS") or "Active")),
                                        agreement_number=(str(row.get("CONTRACT_AGREEMENT_NUMBER")).strip() or None),
                                        company_code=(str(row.get("CONTRACT_COMPANY_CODE")).strip() or None),
                                        cost_center=(str(row.get("CONTRACT_COST_CENTER")).strip() or None),
                                        service_type=(str(row.get("CONTRACT_SERVICE_TYPE")).strip() or None),
                                        contract_renewal_date=(str(row.get("CONTRACT_RENEWAL_DATE")) if pd.notna(row.get("CONTRACT_RENEWAL_DATE")) else None),
                                        invoice_renewal_date=(str(row.get("INVOICE_RENEWAL_DATE")) if pd.notna(row.get("INVOICE_RENEWAL_DATE")) else None),
                                        total_contract_cost=(float(row.get("CONTRACT_TOTAL_COST")) if pd.notna(row.get("CONTRACT_TOTAL_COST")) else None),
                                    )
                                    updated_c += 1
                                    try:
                                        n_created = int(sync_contract_invoices(cid) or 0)
                                        created_invoices_total += n_created
                                        created_contracts_detail.append({
                                            "TEAMNAME": tname,
                                            "APPNAME": final_appname,
                                            "CONTRACT_ID": cid,
                                            "START_FY": int(start_fy_val),
                                            "END_FY": int(end_fy_val),
                                            "PLANNED_INVOICES": n_created,
                                        })
                                    except Exception:
                                        pass
                                except Exception as e:
                                    failed_contracts.append({
                                        "TEAMNAME": tname,
                                        "APPNAME": final_appname,
                                        "REASON": f"Upsert failed: {str(e)[:120]}",
                                    })

                        # Optional: show a compact summary of contracts processed and planned invoices created
                        if created_contracts_detail:
                            try:
                                sum_df = pd.DataFrame(created_contracts_detail)
                                st.success(f"Contracts processed: {len(created_contracts_detail)}  ·  Planned invoices created/updated: {created_invoices_total}")
                                with st.expander("Contracts results (per Team × App)", expanded=False):
                                    # Show key columns only to keep it tidy
                                    show_cols = [
                                        "TEAMNAME", "APPNAME", "START_FY", "END_FY", "PLANNED_INVOICES", "CONTRACT_ID"
                                    ]
                                    show_cols = [c for c in show_cols if c in sum_df.columns]
                                    st.dataframe(sum_df[show_cols], use_container_width=True, height=220)
                                try:
                                    # Clear caches so other tabs reflect newly imported contracts/invoices immediately
                                    st.cache_data.clear()
                                except Exception:
                                    pass
                            except Exception:
                                pass
                        if 'unmatched_contracts' in locals() and unmatched_contracts:
                            try:
                                um_df = pd.DataFrame(unmatched_contracts)
                                with st.expander("Contracts not matched (missing Team or Application)", expanded=True):
                                    st.warning("Some contract rows could not be resolved to an existing Team and/or Application. Fix names and reimport.")
                                    st.dataframe(um_df, use_container_width=True, height=220)
                            except Exception:
                                pass
                        if 'failed_contracts' in locals() and failed_contracts:
                            try:
                                fc_df = pd.DataFrame(failed_contracts)
                                with st.expander("Contracts failed to import (invalid values)", expanded=True):
                                    st.warning("Some contract rows had invalid/missing values (START_FY/END_FY/ANNUAL_AMOUNT or END_FY < START_FY). Fix and reimport.")
                                    st.dataframe(fc_df, use_container_width=True, height=220)
                            except Exception:
                                pass
                        # ---------- Invoices ----------
                        _tick(62, "Importing Invoices...", start_time)
                        created_i = 0
                        created_vendors = 0
                        created_apps = 0

                        # refresh all maps just before invoicing
                        cur_programs = list_programs();  pmap = {}
                        if cur_programs is not None and not cur_programs.empty:
                            for _, r0 in cur_programs.iterrows():
                                pmap[str(r0.PROGRAMNAME).strip().upper()] = r0.PROGRAMID

                        cur_teams = list_teams(); tmap = {}
                        if cur_teams is not None and not cur_teams.empty:
                            for _, r0 in cur_teams.iterrows():
                                tmap[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID

                        cur_groups2 = list_application_groups(); gmap = {}
                        if cur_groups2 is not None and not cur_groups2.empty:
                            for _, r0 in cur_groups2.iterrows():
                                gmap[str(r0.GROUPNAME).strip().upper()] = r0.GROUPID

                        cur_vendors = list_vendors(); vmap = {}
                        if cur_vendors is not None and not cur_vendors.empty:
                            for _, r0 in cur_vendors.iterrows():
                                vmap[str(r0.VENDORNAME).strip().upper()] = r0.VENDORID

                        cur_apps2 = list_applications(); amap = {}
                        if cur_apps2 is not None and not cur_apps2.empty:
                            for _, r0 in cur_apps2.iterrows():
                                amap[str(r0.APPLICATIONNAME).strip().upper()] = r0.APPLICATIONID

                        required_present = (
                            dfI is not None and not dfI.empty and
                            all(c in dfI.columns for c in
                                ["PROGRAMNAME","TEAMNAME","GROUPNAME","APPNAME","VENDORNAME","FISCAL_YEAR","RENEWAL_MONTH","AMOUNT"])
                        )

                        if required_present and (df_valid_preview is not None) and (not df_valid_preview.empty):
                            for i, row in enumerate(df_valid_preview.itertuples(index=False), start=1):
                                pname = str(getattr(row, "PROGRAMNAME", "") or "").strip()
                                tname = str(getattr(row, "TEAMNAME", "") or "").strip()
                                gname = str(getattr(row, "GROUPNAME", "") or "").strip()
                                aname = str(getattr(row, "APPNAME", "") or "").strip()
                                vname = str(getattr(row, "VENDORNAME", "") or "").strip()
                                fy    = getattr(row, "FISCAL_YEAR", None)
                                rmonth= getattr(row, "RENEWAL_MONTH", None)
                                amt   = getattr(row, "AMOUNT", None)

                                if any(_blank_or_nan(x) for x in [pname, tname, gname, aname, vname]):
                                    continue
                                if any(pd.isna(x) for x in [fy, rmonth]):
                                    continue

                                vendorid_at_booking = vmap.get(vname.upper())
                                if not vendorid_at_booking and vname:
                                    # Auto-create missing vendor on the fly
                                    try:
                                        new_vid = str(uuid.uuid4())
                                        upsert_vendor(new_vid, vname)
                                        vmap[vname.upper()] = new_vid
                                        vendorid_at_booking = new_vid
                                        created_vendors += 1
                                    except Exception:
                                        continue

                                teamid = tmap.get(tname.upper())
                                if not teamid:
                                    continue

                                groupid = gmap.get(gname.upper()) if gname else None

                                # Resolve application by canonical name "Group - Suffix" when group is provided
                                final_appname = aname
                                if gname:
                                    prefix = f"{gname} - "
                                    if aname.upper().startswith(prefix.upper()):
                                        final_appname = aname
                                    elif aname.upper() == gname.upper():
                                        final_appname = f"{gname} - Instance"
                                    else:
                                        final_appname = f"{gname} - {aname}"

                                appid = amap.get(final_appname.upper()) if final_appname else None
                                if (appid is None) and groupid and final_appname:
                                    # Create missing application in target group
                                    try:
                                        new_aid = str(uuid.uuid4())
                                        upsert_application_instance(
                                            application_id=new_aid,
                                            group_id=groupid,
                                            application_name=final_appname,
                                            add_info=None,
                                            vendor_id=vendorid_at_booking,
                                        )
                                        amap[final_appname.upper()] = new_aid
                                        appid = new_aid
                                        created_apps += 1
                                    except Exception:
                                        pass

                                try:
                                    d_renew = date(int(fy), int(rmonth), 1)
                                except Exception:
                                    continue

                                inv_id = str(uuid.uuid4())
                                # Derive contract-active and default codes from CONTRACTS (App×Team)
                                contract_active_val = None
                                company_code_val = (str(getattr(row, "COMPANY_CODE", None)) if pd.notna(getattr(row, "COMPANY_CODE", None)) else None)
                                cost_center_val = (str(getattr(row, "COST_CENTER", None)) if pd.notna(getattr(row, "COST_CENTER", None)) else None)
                                service_type_val = None
                                try:
                                    if appid and teamid:
                                        cdfx = fetch_df(
                                            "SELECT TOP 1 UPPER(ISNULL(STATUS,'Active')) AS ST, COMPANY_CODE, COST_CENTER, SERVICE_TYPE FROM CONTRACTS WHERE APPLICATIONID=%s AND TEAMID=%s",
                                            (str(appid), str(teamid))
                                        )
                                        if cdfx is not None and not cdfx.empty:
                                            contract_active_val = (str(cdfx.iloc[0]["ST"]) == "ACTIVE")
                                            if not company_code_val and pd.notna(cdfx.iloc[0].get("COMPANY_CODE")):
                                                company_code_val = str(cdfx.iloc[0].get("COMPANY_CODE"))
                                            if not cost_center_val and pd.notna(cdfx.iloc[0].get("COST_CENTER")):
                                                cost_center_val = str(cdfx.iloc[0].get("COST_CENTER"))
                                            if pd.notna(cdfx.iloc[0].get("SERVICE_TYPE")):
                                                service_type_val = str(cdfx.iloc[0].get("SERVICE_TYPE"))
                                except Exception:
                                    pass

                                # derive invoice status from sheet (optional)
                                inv_status = None
                                try:
                                    inv_status = str(getattr(row, "INVOICE_STATUS", "") or "").strip()
                                except Exception:
                                    inv_status = None
                                upsert_invoice(
                                    invoice_id=inv_id,
                                    application_id=str(appid or ""),
                                    team_id=str(teamid or ""),
                                    renewal_date=d_renew,
                                    amount=float(amt) if not pd.isna(amt) else None,
                                    status=(inv_status if inv_status in ("Planned","Completed") else "Planned"),
                                    fiscal_year=int(fy) if not pd.isna(fy) else None,
                                    product_owner=(str(getattr(row, "PRODUCT_OWNER", None)) if pd.notna(getattr(row, "PRODUCT_OWNER", None)) else None),
                                    amount_next_year=None,
                                    contract_active=contract_active_val,
                                    company_code=company_code_val,
                                    cost_center=cost_center_val,
                                    serial_number=(str(getattr(row, "SERIAL_NUMBER", None)) if pd.notna(getattr(row, "SERIAL_NUMBER", None)) else None),
                                    work_order=(str(getattr(row, "WORK_ORDER", None)) if pd.notna(getattr(row, "WORK_ORDER", None)) else None),
                                    agreement_number=None,
                                    contract_due=None,   # now comes from CONTRACTS
                                    service_type=service_type_val,
                                    notes=(str(getattr(row, "NOTES", None)) if pd.notna(getattr(row, "NOTES", None)) else None),
                                    group_id=str(groupid or ""),
                                    programid_at_booking=None,
                                    vendorid_at_booking=str(vendorid_at_booking or ""),
                                    groupid_at_booking=str(groupid or ""),
                                    rollover_batch_id=import_batch_id,
                                    rolled_over_from_year=None,
                                    invoice_type="Recurring Invoice",
                                )

                                if i % 50 == 0:
                                    _tick(62 + min(30, int(i / max(len(df_valid_preview), 1) * 30)), f"Importing Invoices... {i} processed", start_time)
                                created_i += 1

                        _tick(96, "Finalizing...", start_time)
                        post = {
                            "vendors": _table_count("VENDORS"),
                            "programs": _table_count("PROGRAMS"),
                            "teams": _table_count("TEAMS"),
                            "groups": _table_count("APPLICATION_GROUPS"),
                            "apps": _table_count("APPLICATIONS"),
                            "invoices": _table_count("INVOICES"),
                        }

                        _tick(100, "Done.", start_time)
                        st.success(
                            "Import complete ✅\n\n"
                            f"- Vendors: **{post['vendors'] - pre['vendors']} new** (total: {post['vendors']})\n"
                            f"- Programs: **{post['programs'] - pre['programs']} new** (total: {post['programs']})\n"
                            f"- Teams: **{post['teams'] - pre['teams']} new** (total: {post['teams']})\n"
                            f"- Applications: **{post['groups'] - pre['groups']} new** (total: {post['groups']})\n"
                            f"- Applications: **{post['apps'] - pre['apps']} new** (total: {post['apps']})\n"
                            f"- Invoices: **{post['invoices'] - pre['invoices']} new** (total: {post['invoices']})\n"
                            f"  (Created vendors: {created_vendors}, created apps: {created_apps})\n\n"
                            f"**Batch ID:** `{import_batch_id}`"
                        )
                        if created_invoices_total:
                            st.caption(f"Planned recurring invoices created from contracts: {created_invoices_total}")
                        # Fallback: if contracts were uploaded but no planned invoices were created yet,
                        # try syncing again outside of the group/apps gating (covers contracts-only imports)
                        try:
                            need_fallback = (('dfK' in locals()) and isinstance(dfK, pd.DataFrame) and not dfK.empty and int(created_invoices_total or 0) == 0)
                        except Exception:
                            need_fallback = False
                        if need_fallback:
                            try:
                                ensure_contracts_table()
                                # Refresh maps for IDs
                                cur_teams_fb = list_teams(); tmap_fb = {}
                                if cur_teams_fb is not None and not cur_teams_fb.empty:
                                    for _, r0 in cur_teams_fb.iterrows():
                                        tmap_fb[str(r0.TEAMNAME).strip().upper()] = r0.TEAMID

                                cur_apps_fb = list_applications(); amap_fb = {}
                                if cur_apps_fb is not None and not cur_apps_fb.empty:
                                    for _, r0 in cur_apps_fb.iterrows():
                                        amap_fb[str(r0.APPLICATIONNAME).strip().upper()] = r0.APPLICATIONID

                                from db import sync_contract_invoices as _sync
                                fb_created = 0
                                for _, row in dfK.iterrows():
                                    tname = str(row.get("TEAMNAME") or "").strip()
                                    aname = str(row.get("APPNAME") or "").strip()
                                    gname = str(row.get("GROUPNAME") or "").strip() if "GROUPNAME" in row else ""
                                    if _blank_or_nan(tname) or _blank_or_nan(aname):
                                        continue
                                    tid = tmap_fb.get(tname.upper())
                                    final_appname = aname
                                    if gname:
                                        prefix = f"{gname} - "
                                        if aname.upper().startswith(prefix.upper()):
                                            final_appname = aname
                                        elif aname.upper() == gname.upper():
                                            final_appname = f"{gname} - Instance"
                                        else:
                                            final_appname = f"{gname} - {aname}"
                                    aid = amap_fb.get(final_appname.upper()) or amap_fb.get(aname.upper())
                                    if not tid or not aid:
                                        continue
                                    import uuid as _uuid
                                    cid = str(_uuid.uuid5(_uuid.NAMESPACE_URL, f"contract:{aid}:{tid}"))
                                    # Upsert contract (idempotent) then sync planned invoices
                                    try:
                                        # Safe parse fallback
                                        def _to_int_default(val, default):
                                            try:
                                                if val is None or (hasattr(pd, 'isna') and pd.isna(val)):
                                                    return default
                                                s = str(val).strip()
                                                if s == "":
                                                    return default
                                                return int(float(s))
                                            except Exception:
                                                return default
                                        def _to_float_default(val, default):
                                            try:
                                                if val is None or (hasattr(pd, 'isna') and pd.isna(val)):
                                                    return default
                                                s = str(val).strip()
                                                if s == "":
                                                    return default
                                                return float(s)
                                            except Exception:
                                                return default

                                        sfy = _to_int_default(row.get("CONTRACT_START_FY"), None)
                                        efy = _to_int_default(row.get("CONTRACT_END_FY"), None)
                                        amt = _to_float_default(row.get("CONTRACT_ANNUAL_AMOUNT"), None)
                                        esc = _to_float_default(row.get("CONTRACT_ESCALATION_PCT"), 0.0)
                                        rmon = _to_int_default(row.get("CONTRACT_RENEWAL_MONTH"), 1)
                                        if rmon is None or rmon < 1 or rmon > 12:
                                            rmon = 1
                                        if sfy is None or efy is None or amt is None or (efy < sfy):
                                            raise ValueError("Invalid START_FY/END_FY/ANNUAL_AMOUNT")
                                        upsert_contract(
                                            contract_id=cid,
                                            application_id=aid,
                                            team_id=tid,
                                            start_fy=int(sfy),
                                            end_fy=int(efy),
                                            renewal_month=int(rmon),
                                            annual_amount=float(amt),
                                            escalation_pct=float(esc),
                                            status=(str(row.get("CONTRACT_STATUS") or "Active")),
                                            agreement_number=(str(row.get("CONTRACT_AGREEMENT_NUMBER")).strip() or None),
                                            company_code=(str(row.get("CONTRACT_COMPANY_CODE")).strip() or None),
                                            cost_center=(str(row.get("CONTRACT_COST_CENTER")).strip() or None),
                                            service_type=(str(row.get("CONTRACT_SERVICE_TYPE")).strip() or None),
                                            contract_renewal_date=(str(row.get("CONTRACT_RENEWAL_DATE")) if pd.notna(row.get("CONTRACT_RENEWAL_DATE")) else None),
                                            invoice_renewal_date=(str(row.get("INVOICE_RENEWAL_DATE")) if pd.notna(row.get("INVOICE_RENEWAL_DATE")) else None),
                                            total_contract_cost=(float(row.get("CONTRACT_TOTAL_COST")) if pd.notna(row.get("CONTRACT_TOTAL_COST")) else None),
                                        )
                                    except Exception:
                                        pass
                                    try:
                                        fb_created += int(_sync(cid) or 0)
                                    except Exception:
                                        pass
                                if fb_created > 0:
                                    st.success(f"Created planned recurring invoices from contracts (fallback): {fb_created}")
                                    try:
                                        st.cache_data.clear()
                                    except Exception:
                                        pass
                                else:
                                    st.info("No planned invoices created from contracts. Check Start/End FY and Team/App names.")
                            except Exception as e:
                                st.warning(f"Fallback planned invoice sync failed: {e}")

            except Exception as e:
                        st.error(f"Import failed: {e}")
                        st.exception(e)

        with st.expander("Sanity: Groups with ambiguous vendors per TEAM×GROUP", expanded=False):
            check = fetch_df("""
                WITH base AS (
                  SELECT t.TEAMNAME, g.GROUPNAME, g.DEFAULT_VENDORID, v.VENDORNAME
                  FROM APPLICATION_GROUPS g
                  JOIN TEAMS t ON t.TEAMID = g.TEAMID
                  LEFT JOIN VENDORS v ON v.VENDORID = g.DEFAULT_VENDORID
                ), counts AS (
                  SELECT TEAMNAME, GROUPNAME, COUNT(DISTINCT DEFAULT_VENDORID) AS VENDOR_IDS
                  FROM base
                  GROUP BY TEAMNAME, GROUPNAME
                ), names AS (
                  SELECT TEAMNAME, GROUPNAME, STRING_AGG(VENDORNAME, ', ') WITHIN GROUP (ORDER BY VENDORNAME) AS VENDORS
                  FROM (
                    SELECT DISTINCT TEAMNAME, GROUPNAME, VENDORNAME FROM base
                  ) d
                  GROUP BY TEAMNAME, GROUPNAME
                )
                SELECT c.TEAMNAME, c.GROUPNAME, c.VENDOR_IDS, n.VENDORS
                FROM counts c
                JOIN names n ON n.TEAMNAME = c.TEAMNAME AND n.GROUPNAME = c.GROUPNAME
                WHERE c.VENDOR_IDS > 1
                ORDER BY c.TEAMNAME, c.GROUPNAME
            """)
            if check is None or check.empty:
                st.success("No ambiguous vendors detected across TEAM×GROUP.")
            else:
                st.warning("Found TEAM×GROUP rows with multiple vendor IDs (shouldn’t happen):")
                st.dataframe(check, use_container_width=True, height=260)

        # --------------------------------------------
        # 🛠 One-click fix: set default vendor per TEAM×GROUP
        # --------------------------------------------
        with st.expander("One-click fix: default vendor (TEAM × GROUP) from majority invoices", expanded=False):
            st.caption(
                "For each Team × Group, pick the vendor that appears most often in INVOICES.VENDORID_AT_BOOKING "
                "(ties → most recent invoice wins; final tie → lowest VendorID). "
                "Only updates groups where the proposed vendor differs from the current default."
            )

            # Preview the proposed changes
            try:
                preview_sql = """
                WITH counts AS (
                  SELECT
                    ISNULL(i.TEAMID, g.TEAMID) AS TID,
                    ISNULL(i.GROUPID_AT_BOOKING, i.GROUPID) AS GID,
                    i.VENDORID_AT_BOOKING AS VID,
                    COUNT(*) AS CNT,
                    MAX(ISNULL(i.RENEWALDATE, CAST('1900-01-01' AS DATE))) AS LATEST_DATE
                  FROM INVOICES i
                  LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = i.GROUPID
                  WHERE i.VENDORID_AT_BOOKING IS NOT NULL AND LTRIM(RTRIM(i.VENDORID_AT_BOOKING)) <> ''
                  GROUP BY ISNULL(i.TEAMID, g.TEAMID), ISNULL(i.GROUPID_AT_BOOKING, i.GROUPID), i.VENDORID_AT_BOOKING
                ), ranked AS (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY TID, GID ORDER BY CNT DESC, LATEST_DATE DESC, VID) AS rn
                  FROM counts
                ), best AS (
                  SELECT * FROM ranked WHERE rn = 1
                )
                SELECT
                  t.TEAMNAME,
                  g.GROUPID,
                  g.GROUPNAME,
                  v_cur.VENDORNAME AS CURRENT_VENDOR,
                  v_new.VENDORNAME AS PROPOSED_VENDOR,
                  b.CNT            AS EVIDENCE_INVOICES
                FROM best b
                JOIN APPLICATION_GROUPS g ON g.GROUPID = b.GID AND g.TEAMID = b.TID
                JOIN TEAMS t ON t.TEAMID = g.TEAMID
                LEFT JOIN VENDORS v_cur ON v_cur.VENDORID = g.DEFAULT_VENDORID
                LEFT JOIN VENDORS v_new ON v_new.VENDORID = b.VID
                WHERE ISNULL(g.DEFAULT_VENDORID, '_') <> ISNULL(b.VID, '_')
                ORDER BY t.TEAMNAME, g.GROUPNAME
                """
                fix_preview = fetch_df(preview_sql)
            except Exception as e:
                fix_preview = pd.DataFrame()
                st.error(f"Could not build preview: {e}")

            if fix_preview is None or fix_preview.empty:
                st.success("No changes needed — current defaults already match the majority vendors per TEAM×GROUP.")
            else:
                st.info(f"Proposed updates: **{len(fix_preview)}** group(s) will change their default vendor.")
                st.dataframe(
                    fix_preview[["TEAMNAME","GROUPID","GROUPNAME","CURRENT_VENDOR","PROPOSED_VENDOR","EVIDENCE_INVOICES"]],
                    use_container_width=True,
                    height=320,
                )

                if st.button("✅ Apply fix (update default vendors)", type="primary", key="btn_apply_vendor_fix"):
                    try:
                        update_sql = """
                        WITH counts AS (
                          SELECT
                            ISNULL(i.TEAMID, g.TEAMID) AS TID,
                            ISNULL(i.GROUPID_AT_BOOKING, i.GROUPID) AS GID,
                            i.VENDORID_AT_BOOKING AS VID,
                            COUNT(*) AS CNT,
                            MAX(ISNULL(i.RENEWALDATE, CAST('1900-01-01' AS DATE))) AS LATEST_DATE
                          FROM INVOICES i
                          LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = i.GROUPID
                          WHERE i.VENDORID_AT_BOOKING IS NOT NULL AND LTRIM(RTRIM(i.VENDORID_AT_BOOKING)) <> ''
                          GROUP BY ISNULL(i.TEAMID, g.TEAMID), ISNULL(i.GROUPID_AT_BOOKING, i.GROUPID), i.VENDORID_AT_BOOKING
                        ), ranked AS (
                          SELECT *, ROW_NUMBER() OVER (PARTITION BY TID, GID ORDER BY CNT DESC, LATEST_DATE DESC, VID) AS rn
                          FROM counts
                        ), best AS (
                          SELECT * FROM ranked WHERE rn = 1
                        )
                        UPDATE g
                        SET DEFAULT_VENDORID = b.VID
                        FROM APPLICATION_GROUPS g
                        JOIN best b ON g.GROUPID = b.GID AND g.TEAMID = b.TID
                        WHERE ISNULL(g.DEFAULT_VENDORID, '_') <> ISNULL(b.VID, '_')
                        """
                        execute(update_sql)
                        st.success("Default vendors updated from invoice majorities. ✅")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Vendor fix failed: {e}")

        # --------------------------------------------
        # 🩹 Backfill app → group links from name prefix
        # --------------------------------------------
        with st.expander("Backfill Application → Group links from name prefix ('Group - Suffix')", expanded=False):
            st.caption("For application instances with empty GROUPID, infer the group by matching the name prefix before ' - ' against existing Applications.")
            if st.button("Backfill GROUPID from Application Name", key="btn_backfill_groupid"):
                try:
                    # Preview how many would be updated
                    preview = fetch_df(
                        """
                        WITH parsed AS (
                          SELECT a.APPLICATIONID,
                                 CASE WHEN CHARINDEX(' - ', a.APPLICATIONNAME) > 0
                                      THEN LEFT(a.APPLICATIONNAME, CHARINDEX(' - ', a.APPLICATIONNAME) - 1)
                                      ELSE NULL END AS GNAME
                          FROM APPLICATIONS a
                          WHERE a.GROUPID IS NULL OR LTRIM(RTRIM(a.GROUPID)) = ''
                        )
                        SELECT COUNT(*) AS N
                        FROM parsed p
                        JOIN APPLICATION_GROUPS g ON UPPER(g.GROUPNAME) = UPPER(p.GNAME)
                        """
                    )
                    will_update = int(preview.iloc[0]["N"]) if preview is not None and not preview.empty else 0

                    execute(
                        """
                        WITH parsed AS (
                          SELECT a.APPLICATIONID,
                                 CASE WHEN CHARINDEX(' - ', a.APPLICATIONNAME) > 0
                                      THEN LEFT(a.APPLICATIONNAME, CHARINDEX(' - ', a.APPLICATIONNAME) - 1)
                                      ELSE NULL END AS GNAME
                          FROM APPLICATIONS a
                          WHERE a.GROUPID IS NULL OR LTRIM(RTRIM(a.GROUPID)) = ''
                        ), matched AS (
                          SELECT p.APPLICATIONID, g.GROUPID
                          FROM parsed p
                          JOIN APPLICATION_GROUPS g ON UPPER(g.GROUPNAME) = UPPER(p.GNAME)
                        )
                        UPDATE a
                          SET a.GROUPID = m.GROUPID
                        FROM APPLICATIONS a
                        JOIN matched m ON m.APPLICATIONID = a.APPLICATIONID;
                        """
                    )
                    st.success(f"Linked {will_update} application(s) to groups by name prefix.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Backfill failed: {e}")

        # --------------------------------------------
        # 🔁 Rollback imported invoices by Batch ID
        # --------------------------------------------
        with st.expander("Rollback imported invoices by Batch ID", expanded=False):
            st.caption("This will **DELETE** all invoices with the selected `ROLLOVER_BATCH_ID`. It does not touch vendors/programs/teams/apps/groups.")

            def list_invoice_batches(limit: int = 50) -> pd.DataFrame:
                try:
                    return fetch_df(f"""
                        SELECT TOP {int(limit)}
                          ROLLOVER_BATCH_ID AS BATCH_ID,
                          COUNT(*) AS N
                        FROM INVOICES
                        WHERE ROLLOVER_BATCH_ID IS NOT NULL
                        GROUP BY ROLLOVER_BATCH_ID
                        ORDER BY MAX(ISNULL(RENEWALDATE, CAST('1900-01-01' AS DATE))) DESC, BATCH_ID DESC
                    """)
                except Exception:
                    return pd.DataFrame(columns=["BATCH_ID","N"])

            batches_df = list_invoice_batches(limit=100)
            if batches_df is None or batches_df.empty:
                st.info("No batches found yet.")
            else:
                st.dataframe(batches_df, use_container_width=True, height=220)

                batch_choices = ["(type a batch id)"] + batches_df["BATCH_ID"].astype(str).tolist()
                picked = st.selectbox("Pick a recent Batch ID", options=batch_choices, index=0, key="rollback_pick")
                typed = st.text_input("...or paste a Batch ID exactly", value="" if picked == "(type a batch id)" else picked, key="rollback_typed").strip()

                col_prev, col_del = st.columns([1,1])

                with col_prev:
                    if st.button("Preview rows in this batch", key="btn_preview_batch", use_container_width=True, disabled=(typed == "")):
                        try:
                            prev = fetch_df("""
                                SELECT TOP 500
                                  INVOICEID, TEAMID, APPLICATIONID, GROUPID, VENDORID_AT_BOOKING,
                                  FISCAL_YEAR, RENEWALDATE, AMOUNT, AMOUNT_NEXT_YEAR, STATUS, INVOICE_TYPE,
                                  ROLLOVER_BATCH_ID
                                FROM INVOICES
                                WHERE ROLLOVER_BATCH_ID = %s
                                ORDER BY FISCAL_YEAR DESC,
                                         CASE WHEN RENEWALDATE IS NULL THEN 1 ELSE 0 END ASC,
                                         RENEWALDATE DESC,
                                         INVOICEID
                            """, (typed,))
                            if prev is None or prev.empty:
                                st.warning("No invoices found for that Batch ID.")
                            else:
                                st.success(f"Found {len(prev)} row(s) (showing up to 500).")
                                st.dataframe(prev, use_container_width=True, height=320)
                        except Exception as e:
                            st.error(f"Preview failed: {e}")

                with col_del:
                    danger = st.checkbox("I understand this **permanently deletes** invoices in this batch.", value=False, key="confirm_delete_batch")
                    if st.button("Rollback (Delete Invoices in Batch)", icon=":material/delete:", type="secondary", use_container_width=True,
                                 disabled=(typed == "" or not danger), key="btn_delete_batch"):
                        try:
                            cnt = fetch_df("SELECT COUNT(*) AS N FROM INVOICES WHERE ROLLOVER_BATCH_ID = %s", (typed,))
                            n = int(cnt.iloc[0]["N"]) if cnt is not None and not cnt.empty else 0

                            execute("DELETE FROM INVOICES WHERE ROLLOVER_BATCH_ID = %s", (typed,))
                            st.success(f"Deleted **{n}** invoice(s) for batch `{typed}`.")
                        except Exception as e:
                            st.error(f"Rollback failed: {e}")

    else:
        st.info("Upload your workbook above to proceed.")


# =========================# =========================================================
# 📦 Bulk Load (v2 – MSSQL): Multi-sheet importer
# =========================================================
if top_nav == "Data Loader":
    st.subheader("Data Loader (v2 – MSSQL)")
    st.caption("Templates + importer for Master Data and Finance. MSSQL only. Idempotent upserts. Transactional (rollback on error). Rates are managed in-app on the Rates page.")

    with st.expander("Checklist (must-have vs optional)", expanded=False):
        st.markdown(bulkload_v2.get_checklist_markdown())

    col_dl, col_scope = st.columns([1, 1])
    with col_dl:
        st.download_button(
            "⬇️ Download Master Data template",
            data=bulkload_v2.build_master_template_bytes(),
            file_name="next_data_loader_master_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Programs/Teams/Vendors/Applications/Applications (+ optional Group-Team Links).",
            key="bulk_v2_download_master",
        )
        st.download_button(
            "⬇️ Download Finance template",
            data=bulkload_v2.build_finance_template_bytes(),
            file_name="next_data_loader_finance_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Contracts/Invoices/Program Addl Costs (assumes Master Data already exists).",
            key="bulk_v2_download_finance",
        )
        st.caption("Templates include an `Examples` sheet and Excel dropdowns (data validation). For dropdowns, open in Microsoft Excel Desktop.")
    with col_scope:
        scope_label = st.radio(
            "Loader type",
            ["Master Data", "Finance"],
            index=0,
            horizontal=True,
            key="bulk_v2_scope",
        )
        bulk_mode = {"Master Data": "master", "Finance": "finance"}[scope_label]

    upl = st.file_uploader(
        "Upload completed workbook",
        type=["xlsx", "xlsm", "xlsb", "xls", "csv"],
        key="bulk_v2_uploader",
    )

    if upl:
        dfs = bulkload_v2.read_workbook(upl)
        if st.session_state.get("bulk_v2_last_mode") != bulk_mode:
            st.session_state["bulk_v2_last_mode"] = bulk_mode
            st.session_state.pop("bulk_v2_validation_done", None)
            st.session_state.pop("bulk_v2_plan", None)
            st.session_state.pop("bulk_v2_blocking_df", None)
            st.session_state.pop("bulk_v2_warnings_df", None)
        detected = pd.DataFrame(
            [{"sheet": k, "rows": int(len(v.index)) if isinstance(v, pd.DataFrame) else 0, "cols": int(len(v.columns)) if isinstance(v, pd.DataFrame) else 0} for k, v in dfs.items()]
        ).sort_values(["sheet"])
        st.markdown("**Detected sheets**")
        st.dataframe(detected, use_container_width=True, height=220)

        c1, c2, c3 = st.columns([1, 1, 1])
        with c1:
            do_validate = st.button("Run validation", type="primary", key="bulk_v2_btn_validate", use_container_width=True)
        with c2:
            do_plan = st.button("Plan changes", key="bulk_v2_btn_plan", use_container_width=True)
        with c3:
            do_import = st.button("Import", key="bulk_v2_btn_import", use_container_width=True)

        if do_validate or ("bulk_v2_validation_done" not in st.session_state):
            blocking_df, warnings_df = bulkload_v2.validate_workbook(dfs, bulk_mode)
            st.session_state["bulk_v2_blocking_df"] = blocking_df
            st.session_state["bulk_v2_warnings_df"] = warnings_df
            st.session_state["bulk_v2_validation_done"] = True

        blocking_df = st.session_state.get("bulk_v2_blocking_df", pd.DataFrame())
        warnings_df = st.session_state.get("bulk_v2_warnings_df", pd.DataFrame())

        if blocking_df is not None and not blocking_df.empty:
            st.error(f"Blocking errors: {len(blocking_df)}")
            st.dataframe(blocking_df, use_container_width=True, height=260)
        else:
            st.success("No blocking validation errors.")

        if warnings_df is not None and not warnings_df.empty:
            st.warning(f"Warnings: {len(warnings_df)}")
            st.dataframe(warnings_df, use_container_width=True, height=220)

        rebuild_all_views = st.checkbox(
            "Rebuild ALL views after import (slow)",
            value=False,
            key="bulk_v2_rebuild_all_views",
            help="Defaults to rebuilding only the analytics views after import. Enable this to run a full rebuild of all view materializations.",
        )

        report_df = pd.concat(
            [
                (blocking_df.assign(severity="BLOCKING") if blocking_df is not None and not blocking_df.empty else pd.DataFrame(columns=["sheet", "row", "column", "error", "suggested_fix", "severity"])),
                (warnings_df.assign(severity="WARNING") if warnings_df is not None and not warnings_df.empty else pd.DataFrame(columns=["sheet", "row", "column", "error", "suggested_fix", "severity"])),
            ],
            ignore_index=True,
        )
        if report_df is not None and not report_df.empty:
            st.download_button(
                "⬇️ Download validation report (CSV)",
                data=report_df.to_csv(index=False).encode("utf-8"),
                file_name="bulkload_v2_validation_report.csv",
                mime="text/csv",
                key="bulk_v2_dl_report_csv",
            )
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as w:
                report_df.to_excel(w, index=False, sheet_name="Validation")
            st.download_button(
                "⬇️ Download validation report (XLSX)",
                data=bio.getvalue(),
                file_name="bulkload_v2_validation_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="bulk_v2_dl_report_xlsx",
            )

        if do_plan and (blocking_df is None or blocking_df.empty):
            st.session_state["bulk_v2_plan"] = bulkload_v2.plan_changes(dfs, None, mode=bulk_mode)

        plan = st.session_state.get("bulk_v2_plan")
        if plan and isinstance(plan, dict):
            st.markdown("**Preview (rows to insert / update / skipped)**")
            sheets_plan = plan.get("sheets") or {}
            summary_rows = []
            for sname, splan in sheets_plan.items():
                summary_rows.append(
                    {"sheet": sname, "rows": splan.get("rows"), "insert": splan.get("insert"), "update": splan.get("update"), "skip": splan.get("skip")}
                )
            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, height=320)

            with st.expander("Sample previews", expanded=False):
                for sname, splan in sheets_plan.items():
                    for kind in ("sample_insert", "sample_update", "sample_skip"):
                        samp = splan.get(kind)
                        if isinstance(samp, pd.DataFrame) and not samp.empty:
                            st.markdown(f"**{sname} – {kind.replace('sample_', '').replace('_', ' ')}**")
                            st.dataframe(samp, use_container_width=True, height=180)

        if do_import and plan and (blocking_df is None or blocking_df.empty):
            user_email = st.session_state.get("auth_user", {}).get("email")
            pb = st.progress(0.0)
            status = st.empty()

            def _cb(step: str, i: int, total: int) -> None:
                pct = 0.0 if total <= 0 else float(i) / float(total)
                pb.progress(min(1.0, max(0.0, pct)))
                status.info(f"{step} ({i}/{total})")

            result = bulkload_v2.apply_import(
                plan,
                None,
                progress_cb=_cb,
                user_email=user_email,
                source_filename=getattr(upl, "name", None),
                rebuild_all_views=rebuild_all_views,
            )
            pb.progress(1.0)

            if result.get("success"):
                toast_success("Bulk Load v2 import completed.")
                st.dataframe(pd.DataFrame(result.get("steps") or []), use_container_width=True, height=260)
                with st.expander("Import log", expanded=False):
                    st.code("\n".join(result.get("log") or []))
                try:
                    st.session_state.setdefault("db_rev", 0)
                    st.session_state["db_rev"] += 1
                    st.cache_data.clear()
                except Exception:
                    pass
                st.session_state["bulk_v2_validation_done"] = False
                st.session_state["bulk_v2_plan"] = None
            else:
                toast_error(f"Bulk Load v2 import failed: {result.get('error')}")
                with st.expander("Import log", expanded=True):
                    st.code("\n".join(result.get("log") or []))
    else:
        st.info("Upload your workbook above to proceed.")

    st.markdown("---")
    with st.expander("Legacy bulk load (deprecated)", expanded=False):
        st.warning("Deprecated: use Bulk Load (v2 – MSSQL) above for new imports.")
        enable_legacy = st.toggle("Enable legacy loader", value=False, key="bulk_v2_show_legacy")
        if enable_legacy:
            _render_legacy_bulk_load_one_sheet()
