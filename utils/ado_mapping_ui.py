from __future__ import annotations

from typing import Any, Iterable, Optional

import pandas as pd
import streamlit as st

from core.freshness import post_write_refresh
from core.ado_mapping_service import (
    create_app_group,
    create_app_group_from_ado,
    create_program,
    create_program_from_ado,
    create_team,
    create_team_from_ado,
    get_linked_ado_for_app_group,
    get_linked_ado_for_program,
    get_linked_ado_for_team,
    list_ado_app_mappings,
    list_ado_program_mappings,
    list_ado_team_mappings,
    map_ado_app_to_app_group,
    map_ado_program_to_program,
    map_ado_team_to_team,
    unmap_ado_app_from_app_group,
)
from db import fetch_df
from utils.toast import toast_error, toast_success, toast_warning


def _clean_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def _normalize(series: pd.Series) -> pd.Series:
    return _clean_series(series).str.upper()


def _normalize_vendors_df(df: Any) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    if out.empty:
        return out
    col_upper = {str(c).strip().upper(): c for c in out.columns}
    if "VENDORID" not in out.columns and "VENDORID" in col_upper:
        out = out.rename(columns={col_upper["VENDORID"]: "VENDORID"})
    if "VENDORNAME" not in out.columns and "VENDORNAME" in col_upper:
        out = out.rename(columns={col_upper["VENDORNAME"]: "VENDORNAME"})
    for src, dst in [
        ("VENDOR_ID", "VENDORID"),
        ("vendor_id", "VENDORID"),
        ("ID", "VENDORID"),
        ("id", "VENDORID"),
        ("VENDOR_NAME", "VENDORNAME"),
        ("vendor_name", "VENDORNAME"),
        ("NAME", "VENDORNAME"),
        ("name", "VENDORNAME"),
    ]:
        if dst not in out.columns and src in out.columns:
            out = out.rename(columns={src: dst})
    if "VENDORID" in out.columns:
        out["VENDORID"] = out["VENDORID"].astype(str).str.strip()
    if "VENDORNAME" in out.columns:
        out["VENDORNAME"] = out["VENDORNAME"].astype(str).str.strip()
    return out


def _build_where_in(values: Optional[Iterable[Any]]) -> tuple[str, list[Any]]:
    vals = [str(v).strip() for v in (values or []) if str(v).strip()]
    if not vals:
        return "", []
    return " IN (" + ", ".join(["%s"] * len(vals)) + ")", vals


def _fetch_program_candidates(years: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    where = ["PROGRAM_RAW IS NOT NULL AND LTRIM(RTRIM(PROGRAM_RAW)) <> ''"]
    params: list[Any] = []
    if years:
        year_clause, year_vals = _build_where_in(years)
        if year_clause:
            where.append("ADO_YEAR" + year_clause)
            params.extend(year_vals)
    sql = f"""
    SELECT
      PROGRAM_RAW,
      COUNT(DISTINCT FEATURE_ID) AS FEATURE_COUNT,
      SUM(COALESCE(STORY_POINTS, 0.0)) AS STORY_POINTS_SUM,
      SUM(COALESCE(EFFORT_POINTS, 0.0)) AS EFFORT_POINTS_SUM,
      MAX(COALESCE(CHANGED_AT, CREATED_AT)) AS LAST_SEEN
    FROM ADO_FEATURES
    WHERE {" AND ".join(where)}
    GROUP BY PROGRAM_RAW
    ORDER BY PROGRAM_RAW
    """
    try:
        return fetch_df(sql, tuple(params) if params else None)
    except Exception:
        return pd.DataFrame(columns=["PROGRAM_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])


def _fetch_team_candidates(program_raws: Optional[Iterable[Any]] = None, years: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    where = ["TEAM_VARIANT_KEY IS NOT NULL AND LTRIM(RTRIM(TEAM_VARIANT_KEY)) <> ''"]
    params: list[Any] = []
    if program_raws:
        clause, vals = _build_where_in(program_raws)
        if clause:
            where.append("PROGRAM_RAW" + clause)
            params.extend(vals)
    if years:
        clause, vals = _build_where_in(years)
        if clause:
            where.append("ADO_YEAR" + clause)
            params.extend(vals)
    sql = f"""
    SELECT
      TEAM_VARIANT_KEY,
      MAX(TEAM_RAW) AS TEAM_RAW,
      MAX(PROGRAM_RAW) AS PROGRAM_RAW,
      MAX(AREA_LEVEL3_RAW) AS AREA_LEVEL3_RAW,
      MAX(AREA_LEVEL4_RAW) AS AREA_LEVEL4_RAW,
      COUNT(DISTINCT FEATURE_ID) AS FEATURE_COUNT,
      SUM(COALESCE(STORY_POINTS, 0.0)) AS STORY_POINTS_SUM,
      SUM(COALESCE(EFFORT_POINTS, 0.0)) AS EFFORT_POINTS_SUM,
      MAX(COALESCE(CHANGED_AT, CREATED_AT)) AS LAST_SEEN
    FROM ADO_FEATURES
    WHERE {" AND ".join(where)}
    GROUP BY TEAM_VARIANT_KEY
    ORDER BY TEAM_VARIANT_KEY
    """
    try:
        return fetch_df(sql, tuple(params) if params else None)
    except Exception:
        return pd.DataFrame(
            columns=[
                "TEAM_VARIANT_KEY",
                "TEAM_RAW",
                "PROGRAM_RAW",
                "AREA_LEVEL3_RAW",
                "AREA_LEVEL4_RAW",
                "FEATURE_COUNT",
                "STORY_POINTS_SUM",
                "EFFORT_POINTS_SUM",
                "LAST_SEEN",
            ]
        )


def _fetch_app_candidates(
    program_raws: Optional[Iterable[Any]] = None,
    team_variant_keys: Optional[Iterable[Any]] = None,
    years: Optional[Iterable[Any]] = None,
) -> pd.DataFrame:
    where = ["APP_NAME_RAW IS NOT NULL AND LTRIM(RTRIM(APP_NAME_RAW)) <> ''"]
    params: list[Any] = []
    if team_variant_keys:
        clause, vals = _build_where_in(team_variant_keys)
        if clause:
            where.append("TEAM_VARIANT_KEY" + clause)
            params.extend(vals)
    if program_raws and not team_variant_keys:
        clause, vals = _build_where_in(program_raws)
        if clause:
            where.append("PROGRAM_RAW" + clause)
            params.extend(vals)
    if years:
        clause, vals = _build_where_in(years)
        if clause:
            where.append("ADO_YEAR" + clause)
            params.extend(vals)
    sql = f"""
    SELECT
      APP_NAME_RAW,
      COUNT(DISTINCT FEATURE_ID) AS FEATURE_COUNT,
      SUM(COALESCE(STORY_POINTS, 0.0)) AS STORY_POINTS_SUM,
      SUM(COALESCE(EFFORT_POINTS, 0.0)) AS EFFORT_POINTS_SUM,
      MAX(COALESCE(CHANGED_AT, CREATED_AT)) AS LAST_SEEN
    FROM ADO_FEATURES
    WHERE {" AND ".join(where)}
    GROUP BY APP_NAME_RAW
    ORDER BY APP_NAME_RAW
    """
    try:
        return fetch_df(sql, tuple(params) if params else None)
    except Exception:
        return pd.DataFrame(columns=["APP_NAME_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])


def _fetch_app_candidates_from_features_df(
    ado_features_df: Optional[pd.DataFrame],
    *,
    team_variant_keys: Optional[Iterable[Any]] = None,
) -> pd.DataFrame:
    if ado_features_df is None or not isinstance(ado_features_df, pd.DataFrame) or ado_features_df.empty:
        return pd.DataFrame(columns=["APP_NAME_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])

    work = ado_features_df.copy()
    team_keys = {str(v).strip().upper() for v in (team_variant_keys or []) if str(v).strip()}
    if team_keys:
        team_col = None
        for candidate in ["ADO_TEAM_KEY", "TEAM_VARIANT_KEY"]:
            if candidate in work.columns:
                team_col = candidate
                break
        if not team_col:
            return pd.DataFrame(columns=["APP_NAME_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])
        work[team_col] = work[team_col].astype(str).str.strip().str.upper()
        work = work[work[team_col].isin(team_keys)].copy()
    if "APP_NAME_RAW" not in work.columns and "ADO_APP" in work.columns:
        work.rename(columns={"ADO_APP": "APP_NAME_RAW"}, inplace=True)
    if "APP_NAME_RAW" not in work.columns:
        return pd.DataFrame(columns=["APP_NAME_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])

    work["APP_NAME_RAW"] = _clean_series(work["APP_NAME_RAW"])
    work = work[work["APP_NAME_RAW"] != ""].copy()
    if work.empty:
        return pd.DataFrame(columns=["APP_NAME_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])

    story_source = None
    for col in ["STORY_POINTS", "STORY_POINTS_SUM", "SWAG_POINTS"]:
        if col in work.columns:
            story_source = col
            break
    effort_source = "EFFORT_POINTS" if "EFFORT_POINTS" in work.columns else None
    last_seen_source = "CHANGED_AT" if "CHANGED_AT" in work.columns else ("CREATED_AT" if "CREATED_AT" in work.columns else None)

    agg: dict[str, Any] = {}
    if "FEATURE_ID" in work.columns:
        agg["FEATURE_ID"] = pd.Series.nunique
    if story_source:
        agg[story_source] = "sum"
    if effort_source:
        agg[effort_source] = "sum"
    if last_seen_source:
        agg[last_seen_source] = "max"

    if not agg:
        return pd.DataFrame(columns=["APP_NAME_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])

    grouped = work.groupby("APP_NAME_RAW", dropna=False).agg(agg).reset_index()
    rename = {"FEATURE_ID": "FEATURE_COUNT"}
    if story_source:
        rename[story_source] = "STORY_POINTS_SUM"
    if effort_source:
        rename[effort_source] = "EFFORT_POINTS_SUM"
    if last_seen_source:
        rename[last_seen_source] = "LAST_SEEN"
    grouped.rename(columns=rename, inplace=True)
    return grouped


def _safe_text(val: Any) -> str:
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    return str(val or "").strip()


def _display_mapping_target(name: Optional[str], _id: Optional[str]) -> str:
    name_s = _safe_text(name)
    id_s = _safe_text(_id)
    if name_s and id_s:
        return f"{name_s} ({id_s})"
    if name_s:
        return name_s
    return id_s


def render_ado_mapping_panel(
    entity_type: str,
    entity_id: str,
    *,
    entity_label: Optional[str] = None,
    context_filters: Optional[dict[str, Any]] = None,
    user_is_admin: bool = False,
    wizard_mode: bool = False,
    allow_create_toggle: bool = False,
    show_unmapped_candidates: bool = True,
    ado_features_df: Optional[pd.DataFrame] = None,
    allow_unmap: bool = False,
) -> None:
    entity_id = str(entity_id or "").strip()
    if not entity_id:
        st.info("Select an item to manage ADO mappings.")
        return
    summary_key = f"ado_mapping_summary_{entity_type}_{entity_id}"
    summary_payload = st.session_state.pop(summary_key, None)
    if isinstance(summary_payload, dict):
        action = str(summary_payload.get("action") or "").strip()
        count = int(summary_payload.get("count") or 0)
        sample_vals = [str(x).strip() for x in (summary_payload.get("values") or []) if str(x).strip()]
        sample_txt = ", ".join(sample_vals[:5])
        if len(sample_vals) > 5:
            sample_txt += ", ..."
        if action == "map":
            st.success(f"Mappings saved for {count} ADO value(s)." + (f" Values: {sample_txt}" if sample_txt else ""))
        elif action == "unmap":
            st.success(f"Mappings removed for {count} ADO value(s)." + (f" Values: {sample_txt}" if sample_txt else ""))

    entity_label = entity_label or entity_type.replace("_", " ").title()
    if entity_type == "app_group":
        entity_label = "Application"

    ctx = context_filters or {}
    years = ctx.get("years")
    program_raws = ctx.get("program_raws")
    team_variant_keys = ctx.get("team_variant_keys")
    require_team_scope = bool(ctx.get("require_team_scope"))

    entity_name = ""
    if entity_type == "program":
        linked = get_linked_ado_for_program(entity_id)
        candidates = _fetch_program_candidates(years=years)
        mappings = list_ado_program_mappings()
        ado_key = "ADO_PROGRAM"
        mapping_id_col = "PROGRAMID"
        mapping_name_col = "PROGRAMNAME"
        map_fn = map_ado_program_to_program
        create_fn = lambda ado_val: create_program_from_ado(ado_val, updated_by=ctx.get("updated_by"))
    elif entity_type == "team":
        linked = get_linked_ado_for_team(entity_id)
        candidates = _fetch_team_candidates(program_raws=program_raws, years=years)
        mappings = list_ado_team_mappings()
        ado_key = "ADO_TEAM_KEY"
        mapping_id_col = "TEAMID"
        mapping_name_col = "TEAMNAME"
        map_fn = map_ado_team_to_team
        create_fn = lambda ado_val: create_team_from_ado(ado_val, program_id=ctx.get("program_id"), updated_by=ctx.get("updated_by"))
    elif entity_type == "app_group":
        linked = get_linked_ado_for_app_group(entity_id)
        if require_team_scope and not team_variant_keys:
            candidates = pd.DataFrame(columns=["APP_NAME_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "LAST_SEEN"])
        else:
            if ado_features_df is not None and not ado_features_df.empty:
                candidates = _fetch_app_candidates_from_features_df(
                    ado_features_df,
                    team_variant_keys=team_variant_keys,
                )
            else:
                candidates = _fetch_app_candidates(program_raws=program_raws, team_variant_keys=team_variant_keys, years=years)
        mappings = list_ado_app_mappings()
        ado_key = "ADO_APP"
        mapping_id_col = "APP_GROUP"
        mapping_name_col = "GROUPNAME"
        map_fn = map_ado_app_to_app_group
        create_fn = lambda ado_val: create_app_group_from_ado(
            ado_val,
            team_id=ctx.get("team_id"),
            owner=ctx.get("owner"),
            default_vendor_id=ctx.get("default_vendor_id"),
            updated_by=ctx.get("updated_by"),
        )
    else:
        st.error(f"Unknown mapping entity type: {entity_type}")
        return

    linked = linked.copy() if linked is not None else pd.DataFrame()
    candidates = candidates.copy() if isinstance(candidates, pd.DataFrame) else pd.DataFrame()
    mappings = mappings.copy() if mappings is not None else pd.DataFrame()
    candidates_before_link_filter = int(len(candidates)) if isinstance(candidates, pd.DataFrame) else 0

    if not candidates.empty:
        # Normalize candidate keys
        if ado_key not in candidates.columns:
            if entity_type == "program":
                candidates.rename(columns={"PROGRAM_RAW": ado_key}, inplace=True)
            elif entity_type == "team":
                candidates.rename(columns={"TEAM_VARIANT_KEY": ado_key}, inplace=True)
            elif entity_type == "app_group":
                candidates.rename(columns={"APP_NAME_RAW": ado_key}, inplace=True)
        if ado_key in candidates.columns:
            candidates[ado_key] = _clean_series(candidates[ado_key])
            candidates = candidates[candidates[ado_key] != ""].copy()

            if not mappings.empty and ado_key in mappings.columns:
                mappings["_ADO_KEY"] = _normalize(mappings[ado_key])
            else:
                mappings["_ADO_KEY"] = pd.Series([], dtype=str)

            candidates["_ADO_KEY"] = _normalize(candidates[ado_key])
            candidates = candidates.merge(
                mappings[["_ADO_KEY", mapping_id_col, mapping_name_col]],
                how="left",
                on="_ADO_KEY",
            )
            candidates["MAPPED_TO"] = candidates.apply(
                lambda r: _display_mapping_target(r.get(mapping_name_col), r.get(mapping_id_col)),
                axis=1,
            )
            candidates["IS_CONFLICT"] = candidates[mapping_id_col].astype(str).str.strip().ne("") & (
                candidates[mapping_id_col].astype(str).str.strip() != entity_id
            )

            linked_keys = set()
            if not linked.empty and ado_key in linked.columns:
                linked_keys = set(_normalize(linked[ado_key]).tolist())
            candidates = candidates[~candidates["_ADO_KEY"].isin(linked_keys)].copy()
            if "IS_CONFLICT" in candidates.columns:
                candidates["MAPPING_STATUS"] = candidates["IS_CONFLICT"].map(
                    lambda v: "Mapped elsewhere (conflict)" if bool(v) else "Unmapped"
                )
        else:
            candidates = pd.DataFrame()

    st.markdown("#### Currently Linked ADO Values")
    st.caption(
        "Status model: values here are `Mapped to this app`; candidate table shows `Mapped elsewhere (conflict)` or `Unmapped`."
    )
    if linked is None or linked.empty:
        st.caption(f"No ADO {entity_label} values linked yet.")
    else:
        st.dataframe(linked, use_container_width=True, hide_index=True, height=220)
        if allow_unmap and entity_type == "app_group" and ado_key in linked.columns:
            linked_sel_df = linked.copy()
            linked_sel_df["SELECT"] = False
            linked_editor = st.data_editor(
                linked_sel_df[["SELECT", ado_key]],
                use_container_width=True,
                hide_index=True,
                height=220,
                column_config={"SELECT": st.column_config.CheckboxColumn("Unmap")},
                disabled=[ado_key],
                key=f"ado_unmap_editor_{entity_type}_{entity_id}",
            )
            selected_unmap = (
                linked_editor.loc[linked_editor["SELECT"] == True, ado_key].astype(str).str.strip().tolist()
                if isinstance(linked_editor, pd.DataFrame) and "SELECT" in linked_editor.columns
                else []
            )
            if st.button(
                f"Unmap selected from this {entity_label}",
                icon=":material/link_off:",
                key=f"ado_unmap_apply_{entity_type}_{entity_id}",
                disabled=(not selected_unmap),
            ):
                try:
                    unmap_ado_app_from_app_group(selected_unmap, app_group_id=entity_id)
                    st.session_state[summary_key] = {
                        "action": "unmap",
                        "count": len(selected_unmap),
                        "values": selected_unmap,
                    }
                    toast_success("Mappings removed.")
                    post_write_refresh("ado_mapping_unmap", rerun=True, bump_version=True)
                except Exception as e:
                    toast_error(f"Unmap failed: {e}")

    if not show_unmapped_candidates:
        return

    st.markdown("#### Candidate ADO Values")
    st.caption("Candidate statuses: `Mapped elsewhere (conflict)` or `Unmapped`.")
    if candidates.empty:
        if ado_features_df is not None and isinstance(ado_features_df, pd.DataFrame):
            if ado_features_df.empty:
                st.caption("No ADO rows were loaded for the current scope.")
            elif candidates_before_link_filter > 0:
                st.caption("All ADO candidates in this scope are already linked to this application.")
            else:
                st.caption("No ADO candidates were detected in this scope.")
        else:
            if entity_type == "app_group" and not program_raws and not team_variant_keys:
                st.caption("Missing ADO team/program mapping prerequisites for this scope.")
            elif entity_type == "app_group" and require_team_scope and not team_variant_keys:
                st.caption("Missing ADO team mapping for the selected team. Map Team -> ADO Team Key first.")
            else:
                st.caption("No ADO data available yet. Load ADO features to discover candidates.")
        with st.expander("Debug – ADO mapping diagnostics", expanded=False):
            st.write(
                {
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "ado_features_rows": int(len(ado_features_df))
                    if isinstance(ado_features_df, pd.DataFrame)
                    else 0,
                    "candidates_before_filter": candidates_before_link_filter,
                    "candidates_after_filter": int(len(candidates)),
                    "linked_rows": int(len(linked)),
                    "program_raws": program_raws or [],
                    "team_variant_keys": team_variant_keys or [],
                }
            )
        return

    display_cols = [ado_key]
    for col in ["TEAM_RAW", "PROGRAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW"]:
        if col in candidates.columns:
            display_cols.append(col)
    for col in ["FEATURE_COUNT", "STORY_POINTS_SUM", "EFFORT_POINTS_SUM", "DERIVED_FTE_SUM", "LAST_SEEN"]:
        if col in candidates.columns:
            display_cols.append(col)
    display_cols.extend(["MAPPED_TO", "MAPPING_STATUS"])

    display = candidates[display_cols].copy()
    display["SELECT"] = False
    display_cols = ["SELECT"] + display_cols
    display = display[display_cols]

    editor = st.data_editor(
        display,
        use_container_width=True,
        hide_index=True,
        height=320,
        column_config={
            "SELECT": st.column_config.CheckboxColumn("Select"),
        },
        key=f"ado_map_panel_editor_{entity_type}_{entity_id}",
    )

    selected = editor[editor["SELECT"] == True].copy() if editor is not None else pd.DataFrame()
    if selected.empty:
        st.caption("Select one or more candidates to map.")
        return

    selected_ado = selected[ado_key].astype(str).tolist()
    selected_conflicts = candidates.loc[candidates[ado_key].isin(selected_ado) & candidates["IS_CONFLICT"]]
    with st.expander("Debug – ADO mapping diagnostics", expanded=False):
        st.write(
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "ado_features_rows": int(len(ado_features_df))
                if isinstance(ado_features_df, pd.DataFrame)
                else 0,
                "candidates_before_filter": candidates_before_link_filter,
                "candidates_after_filter": int(len(candidates)),
                "linked_rows": int(len(linked)),
                "selected_rows": int(len(selected_ado)),
                "conflict_rows": int(len(selected_conflicts)),
                "program_raws": program_raws or [],
                "team_variant_keys": team_variant_keys or [],
            }
        )

    allow_reassign = False
    if not selected_conflicts.empty:
        msg = f"{len(selected_conflicts)} selected value(s) are already mapped elsewhere."
        if user_is_admin:
            st.warning(msg)
            allow_reassign = st.checkbox(
                "Confirm reassignment of existing mappings",
                value=False,
                key=f"ado_map_reassign_{entity_type}_{entity_id}",
            )
        else:
            st.error(msg + " Your current role cannot reassign existing mappings.")

    col_map, col_create = st.columns([1, 1])
    with col_map:
        if st.button(
            f"Map selected to this {entity_label}",
            icon=":material/link:",
            key=f"ado_map_apply_{entity_type}_{entity_id}",
            disabled=(not user_is_admin and not selected_conflicts.empty),
        ):
            if not selected_conflicts.empty and user_is_admin and not allow_reassign:
                toast_warning("Confirm reassignment to proceed.")
            else:
                try:
                    map_fn(selected_ado, entity_id)
                    st.session_state[summary_key] = {
                        "action": "map",
                        "count": len(selected_ado),
                        "values": selected_ado,
                    }
                    toast_success("Mappings saved.")
                    post_write_refresh("ado_mapping_save", rerun=True, bump_version=True)
                except Exception as e:
                    toast_error(f"Mapping failed: {e}")

    allow_create = wizard_mode
    if allow_create_toggle and not wizard_mode:
        allow_create = st.checkbox(
            "Enable create new from ADO (advanced)",
            value=False,
            key=f"ado_map_create_toggle_{entity_type}_{entity_id}",
        )

    with col_create:
        if allow_create and st.button(
            f"Create new {entity_label} + map",
            icon=":material/add_circle:",
            key=f"ado_map_create_{entity_type}_{entity_id}",
        ):
            try:
                created = 0
                for ado_val in selected_ado:
                    new_id = create_fn(ado_val)
                    map_fn([ado_val], new_id)
                    created += 1
                toast_success(f"Created {created} {entity_label}(s) and saved mappings.")
                post_write_refresh("ado_mapping_create_and_map", rerun=True, bump_version=True)
            except Exception as e:
                toast_error(f"Create + map failed: {e}")


def render_embedded_onboard_flow(
    entity_type: str,
    defaults_context: dict[str, Any],
    *,
    user_is_admin: bool,
    ado_features_df: Optional[pd.DataFrame] = None,
) -> None:
    ctx = defaults_context or {}
    updated_by = str(ctx.get("updated_by") or "").strip() or None

    if entity_type == "program":
        st.markdown("#### Onboard new Program")
        name_key = str(ctx.get("name_key") or "onboard_program_name")
        owner_key = str(ctx.get("owner_key") or "onboard_program_owner")
        program_name = st.text_input("Program Name (required)", value="", key=name_key)
        entity_name = program_name

        labels = ctx.get("labels") or []
        label_to_email = ctx.get("label_to_email") or {}
        sel_lbl = st.selectbox(
            "Program Manager (required)",
            options=[""] + list(labels),
            index=0,
            key=owner_key,
        )
        program_owner = label_to_email.get(sel_lbl, "") if sel_lbl else ""

        create_fn = ctx.get("create_fn")
        name_exists_fn = ctx.get("name_exists_fn")
        on_created = ctx.get("on_created")

        map_entity_label = "Program keys"
        candidates = _fetch_program_candidates()
        mappings = list_ado_program_mappings()
        ado_key = "ADO_PROGRAM"
        mapping_id_col = "PROGRAMID"
        mapping_name_col = "PROGRAMNAME"
        map_fn = map_ado_program_to_program

        def _create_entity() -> Optional[str]:
            name = (program_name or "").strip()
            owner = (program_owner or "").strip()
            if not name:
                st.error("Program Name is required.")
                return None
            if not owner:
                st.error("Program Manager is required.")
                return None
            if callable(name_exists_fn):
                existing_id = name_exists_fn(name)
                if existing_id:
                    toast_warning(f"A Program named '{name}' already exists. Selecting the existing program.")
                    if callable(on_created):
                        on_created(existing_id, name, 0)
                    st.rerun()
            if callable(create_fn):
                return create_fn(name=name, owner=owner)
            return create_program(name, owner=owner, fte=None, program_rate=None, updated_by=updated_by)

    elif entity_type == "team":
        st.markdown("#### Onboard new Team")
        name_key = str(ctx.get("name_key") or "onboard_team_name")
        program_key = str(ctx.get("program_key") or "onboard_team_program")
        owner_key = str(ctx.get("owner_key") or "onboard_team_owner")
        msp_key = str(ctx.get("msp_key") or "onboard_team_msp")

        team_name = st.text_input("Team Name (required)", value="", key=name_key)
        entity_name = team_name
        programs_df = ctx.get("programs_df")
        program_name_by_id = ctx.get("program_name_by_id") or {}
        program_id = st.selectbox(
            "Program (required)",
            options=(programs_df["PROGRAMID"].tolist() if isinstance(programs_df, pd.DataFrame) and not programs_df.empty else []),
            format_func=lambda x: program_name_by_id.get(str(x), str(x)),
            key=program_key,
        )

        labels = ctx.get("labels") or []
        label_to_email = ctx.get("label_to_email") or {}
        sel_lbl = st.selectbox(
            "Product Owner (required)",
            options=[""] + list(labels),
            index=0,
            key=owner_key,
        )
        product_owner = label_to_email.get(sel_lbl, "") if sel_lbl else ""
        msp_enabled = bool(
            st.checkbox(
                "Is this team MSP?",
                value=False,
                key=msp_key,
                help=(
                    "MSP teams use MSP sizing/rate cards from Rates -> MSP. "
                    "Standard labor headcount rates are not applied to features mapped to this team."
                ),
            )
        )
        if msp_enabled:
            st.info(
                "MSP mode enabled: labor headcount/rates are ignored for this team's feature costs. "
                "Costs come from MSP assignments and MSP size-based rates."
            )

        create_fn = ctx.get("create_fn")
        name_exists_fn = ctx.get("name_exists_fn")
        on_created = ctx.get("on_created")

        map_entity_label = "Team keys"
        ado_programs = []
        if program_id:
            prog_map = list_ado_program_mappings()
            if prog_map is not None and not prog_map.empty:
                ado_programs = (
                    prog_map.loc[prog_map["PROGRAMID"].astype(str) == str(program_id), "ADO_PROGRAM"]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
        candidates = _fetch_team_candidates(program_raws=ado_programs)
        mappings = list_ado_team_mappings()
        ado_key = "ADO_TEAM_KEY"
        mapping_id_col = "TEAMID"
        mapping_name_col = "TEAMNAME"
        map_fn = map_ado_team_to_team

        def _create_entity() -> Optional[str]:
            name = (team_name or "").strip()
            owner = (product_owner or "").strip()
            if not name:
                st.error("Team Name is required.")
                return None
            if not program_id:
                st.error("Program is required.")
                return None
            if not owner:
                st.error("Product Owner is required.")
                return None
            if callable(name_exists_fn):
                existing_id = name_exists_fn(name)
                if existing_id:
                    st.error(f"A Team named '{name}' already exists. Team names must be unique.")
                    return None
            if callable(create_fn):
                return create_fn(name=name, program_id=program_id, product_owner=owner, msp_enabled=msp_enabled)
            return create_team(name, program_id=program_id, product_owner=owner, updated_by=updated_by)

    elif entity_type == "app_group":
        st.markdown("#### Onboard new Application")
        name_key = str(ctx.get("name_key") or "onboard_group_name")
        program_key = str(ctx.get("program_key") or "onboard_group_program")
        team_key = str(ctx.get("team_key") or "onboard_group_team")
        vendor_key = str(ctx.get("vendor_key") or "onboard_group_vendor")
        base_key = str(ctx.get("base_key") or "onboard_group_base")

        programs_df = ctx.get("programs_df")
        teams_fn = ctx.get("teams_for_program_fn")
        groups_for_team_fn = ctx.get("groups_for_team_fn")
        vendors_df = _normalize_vendors_df(ctx.get("vendors_df"))

        selected_program_label = None
        selected_program_id = None
        if isinstance(programs_df, pd.DataFrame) and not programs_df.empty:
            programs_df = programs_df.copy()
            programs_df["LABEL"] = programs_df["PROGRAMNAME"].astype(str)
            selected_program_label = st.selectbox(
                "Program (required)",
                options=[""] + programs_df["LABEL"].tolist(),
                index=0,
                key=program_key,
            )
            if selected_program_label:
                selected_program_id = programs_df.loc[programs_df["LABEL"] == selected_program_label, "PROGRAMID"].iloc[0]

        teams_df = teams_fn(selected_program_id) if callable(teams_fn) and selected_program_id else pd.DataFrame()
        if teams_df is not None and not teams_df.empty:
            teams_df = teams_df.copy()
            teams_df["LABEL"] = teams_df["TEAMNAME"].astype(str)
        selected_team_label = st.selectbox(
            "Team (required)",
            options=[""] + (teams_df["LABEL"].tolist() if teams_df is not None and not teams_df.empty else []),
            index=0,
            key=team_key,
            disabled=selected_program_id is None,
        )
        selected_team_id = None
        if selected_team_label and teams_df is not None and not teams_df.empty:
            selected_team_id = teams_df.loc[teams_df["LABEL"] == selected_team_label, "TEAMID"].iloc[0]

        team_groups_current = groups_for_team_fn(selected_team_id) if callable(groups_for_team_fn) and selected_team_id else pd.DataFrame()
        has_base_already = bool(
            team_groups_current is not None
            and not team_groups_current.empty
            and "IS_BASE" in team_groups_current.columns
            and not team_groups_current[team_groups_current["IS_BASE"] == 1].empty
        )

        group_name = st.text_input(
            "Application Group Name (required)",
            value="",
            key=name_key,
            disabled=selected_team_id is None,
            help=(
                "This name is what users will see across NEXT interfaces "
                "(Dashboards, Insights, Invoices, Contracts, and other pages)."
            ),
        )
        st.caption(
            "Use a business-facing name. Renaming this group changes how it appears across core NEXT pages."
        )
        entity_name = group_name
        make_base = st.checkbox(
            "Mark as Shared Unplanned Pool (BASE) for this team",
            value=False,
            disabled=selected_team_id is None,
            key=base_key,
        )
        if make_base and has_base_already:
            st.info("This team already has BASE-marked apps; multiple Shared Unplanned Pool (BASE) apps are allowed.")
        if make_base:
            st.caption(
                "Use for bugs, break-ins, and unplanned work. Demand/cost is shared across this team's mapped apps in the PI."
            )
            st.session_state[vendor_key] = "(None)"

        vendor_label = ""
        vendor_id = None
        if isinstance(vendors_df, pd.DataFrame):
            vendor_options = ["(None)"] + (
                sorted(vendors_df["VENDORNAME"].dropna().astype(str).unique().tolist())
                if "VENDORNAME" in vendors_df.columns
                else []
            )
            vendor_label = st.selectbox(
                "Default Vendor (required)",
                options=vendor_options,
                index=(vendor_options.index(st.session_state.get(vendor_key)) if st.session_state.get(vendor_key) in vendor_options else 0),
                key=vendor_key,
                disabled=selected_team_id is None or make_base,
            )
            if vendor_label and "VENDORNAME" in vendors_df.columns and "VENDORID" in vendors_df.columns:
                vrow = vendors_df.loc[
                    vendors_df["VENDORNAME"].astype(str).str.strip().str.upper() == str(vendor_label).strip().upper()
                ]
                if not vrow.empty:
                    vendor_id = vrow.iloc[0]["VENDORID"]

        effective_vendor = "(None)" if make_base else (vendor_label or "(Select)")
        st.caption(
            f"Effective config: Type = **{'Shared Unplanned Pool (BASE)' if make_base else 'Standard'}** | "
            f"Vendor = **{effective_vendor}** | "
            f"Allocation = **{'Team shared' if make_base else 'Direct app/vendor'}**"
        )
        st.caption(
            f"Context: **{selected_program_label or '—'}** → **{selected_team_label or '—'}** → **{vendor_label or '—'}**"
        )

        create_fn = ctx.get("create_fn")
        name_exists_fn = ctx.get("name_exists_fn")
        on_created = ctx.get("on_created")

        map_entity_label = "App names"
        ado_team_keys = []
        if selected_team_id:
            team_map = list_ado_team_mappings()
            if team_map is not None and not team_map.empty:
                ado_team_keys = (
                    team_map.loc[team_map["TEAMID"].astype(str) == str(selected_team_id), "ADO_TEAM_KEY"]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
        if ado_features_df is not None and not ado_features_df.empty:
            candidates = _fetch_app_candidates_from_features_df(
                ado_features_df,
                team_variant_keys=ado_team_keys,
            )
        else:
            candidates = _fetch_app_candidates(team_variant_keys=ado_team_keys)
        mappings = list_ado_app_mappings()
        ado_key = "ADO_APP"
        mapping_id_col = "APP_GROUP"
        mapping_name_col = "GROUPNAME"
        map_fn = map_ado_app_to_app_group

        def _create_entity() -> Optional[str]:
            name = (group_name or "").strip()
            if not name:
                st.error("Application Name is required.")
                return None
            if not selected_program_id:
                st.error("Program is required.")
                return None
            if not selected_team_id:
                st.error("Team is required.")
                return None
            if make_base:
                _vendor_id = None
            else:
                _vendor_id = vendor_id
            if (not _vendor_id) and not make_base:
                st.error("Vendor is required unless this is marked as Shared Unplanned Pool (BASE).")
                return None
            if callable(name_exists_fn):
                exists_msg = name_exists_fn(name)
                if exists_msg:
                    st.error(exists_msg)
                    return None
            if callable(create_fn):
                return create_fn(
                    name=name,
                    team_id=selected_team_id,
                    vendor_id=_vendor_id,
                    is_base=bool(make_base),
                )
            return create_app_group(
                name,
                team_id=selected_team_id,
                default_vendor_id=_vendor_id,
                owner=None,
                is_base=bool(make_base),
                updated_by=updated_by,
            )

    else:
        st.error(f"Unknown entity type: {entity_type}")
        return

    allow_reassign = True
    st.markdown(f"#### Link ADO {map_entity_label} (optional)")
    if candidates is None or candidates.empty:
        if ado_features_df is not None and isinstance(ado_features_df, pd.DataFrame):
            if ado_features_df.empty:
                st.caption("No ADO rows were loaded for the current scope.")
            else:
                st.caption("No unmapped ADO candidates detected in this scope (items may already be mapped).")
        elif entity_type == "app_group" and not ado_team_keys:
            st.caption("Missing ADO team mapping for the selected team. Map the team first, then retry.")
        else:
            st.caption("No ADO data available to suggest mappings.")
        selected_ado = []
    else:
        cand = candidates.copy()
        if ado_key not in cand.columns:
            if entity_type == "program" and "PROGRAM_RAW" in cand.columns:
                cand.rename(columns={"PROGRAM_RAW": ado_key}, inplace=True)
            elif entity_type == "team" and "TEAM_VARIANT_KEY" in cand.columns:
                cand.rename(columns={"TEAM_VARIANT_KEY": ado_key}, inplace=True)
            elif entity_type == "app_group" and "APP_NAME_RAW" in cand.columns:
                cand.rename(columns={"APP_NAME_RAW": ado_key}, inplace=True)
        if ado_key not in cand.columns:
            st.caption("ADO candidate data is missing required columns.")
            return
        cand[ado_key] = _clean_series(cand[ado_key])
        cand = cand[cand[ado_key] != ""].copy()

        mappings = mappings.copy() if mappings is not None else pd.DataFrame()
        if not mappings.empty and ado_key in mappings.columns:
            mappings["_ADO_KEY"] = _normalize(mappings[ado_key])
        else:
            mappings["_ADO_KEY"] = pd.Series([], dtype=str)
        cand["_ADO_KEY"] = _normalize(cand[ado_key])
        cand = cand.merge(
            mappings[["_ADO_KEY", mapping_id_col, mapping_name_col]],
            how="left",
            on="_ADO_KEY",
        )
        cand["MAPPED_TO"] = cand[mapping_name_col].astype(str).str.strip()
        cand.loc[cand["MAPPED_TO"] == "", "MAPPED_TO"] = "—"
        cand["IS_CONFLICT"] = cand[mapping_id_col].astype(str).str.strip().ne("")
        cand["MAPPING_STATUS"] = cand["IS_CONFLICT"].map(
            lambda v: "Mapped elsewhere (conflict)" if bool(v) else "Unmapped"
        )

        story_col = "STORY_POINTS_SUM" if "STORY_POINTS_SUM" in cand.columns else ("STORY_POINTS" if "STORY_POINTS" in cand.columns else None)

        show_diag = st.checkbox(
            "Show diagnostic columns",
            value=False,
            key=f"ado_onboard_diag_{entity_type}",
        )

        if show_diag:
            display = cand.copy()
            display["SELECT"] = False
            display_cols = ["SELECT"] + [c for c in display.columns if c != "SELECT"]
            display = display[display_cols]
            key_col = ado_key
        else:
            if entity_type == "program":
                display_cols = [ado_key, "FEATURE_COUNT", story_col, "LAST_SEEN", "MAPPED_TO", "MAPPING_STATUS"]
                display_cols = [c for c in display_cols if c]
                display = cand[display_cols].copy()
                display.rename(
                    columns={
                        ado_key: "ADO Program",
                        "FEATURE_COUNT": "Features",
                        story_col or "STORY_POINTS_SUM": "Story Points",
                        "LAST_SEEN": "Last Seen",
                        "MAPPED_TO": "Mapped To",
                        "MAPPING_STATUS": "Status",
                    },
                    inplace=True,
                )
                key_col = "ADO Program"
            elif entity_type == "team":
                display_cols = [ado_key, "TEAM_RAW", "FEATURE_COUNT", story_col, "LAST_SEEN", "MAPPED_TO", "MAPPING_STATUS"]
                display_cols = [c for c in display_cols if c]
                display = cand[display_cols].copy()
                display.rename(
                    columns={
                        ado_key: "ADO Team Key",
                        "TEAM_RAW": "Team Name (ADO)",
                        "FEATURE_COUNT": "Features",
                        story_col or "STORY_POINTS_SUM": "Story Points",
                        "LAST_SEEN": "Last Seen",
                        "MAPPED_TO": "Mapped To",
                        "MAPPING_STATUS": "Status",
                    },
                    inplace=True,
                )
                key_col = "ADO Team Key"
            else:
                display_cols = [ado_key, "FEATURE_COUNT", story_col, "LAST_SEEN", "MAPPED_TO", "MAPPING_STATUS"]
                display_cols = [c for c in display_cols if c]
                display = cand[display_cols].copy()
                display.rename(
                    columns={
                        ado_key: "ADO App",
                        "FEATURE_COUNT": "Features",
                        story_col or "STORY_POINTS_SUM": "Story Points",
                        "LAST_SEEN": "Last Seen",
                        "MAPPED_TO": "Mapped To",
                        "MAPPING_STATUS": "Status",
                    },
                    inplace=True,
                )
                key_col = "ADO App"
            display["SELECT"] = False
            display_cols = ["SELECT"] + [c for c in display.columns if c != "SELECT"]
            display = display[display_cols]

        st.markdown(f"##### Unmapped ADO {map_entity_label} detected")
        st.caption("Status shows whether each candidate is unmapped or already mapped elsewhere.")
        editor = st.data_editor(
            display,
            use_container_width=True,
            hide_index=True,
            height=280,
            column_config={"SELECT": st.column_config.CheckboxColumn("Select")},
            key=f"ado_onboard_editor_{entity_type}",
        )
        selected = editor[editor["SELECT"] == True].copy() if editor is not None else pd.DataFrame()
        selected_ado = selected[key_col].astype(str).tolist() if not selected.empty else []
        # De-duplicate while preserving order to keep create/link deterministic.
        selected_ado = list(dict.fromkeys([str(x).strip() for x in selected_ado if str(x).strip()]))
        if selected_ado:
            st.caption(f"Selected ADO values: {len(selected_ado)}")
            st.code("\\n".join(selected_ado[:20]), language="text")
            if len(selected_ado) > 20:
                st.caption(f"... and {len(selected_ado) - 20} more")
        if entity_type == "app_group":
            st.markdown("##### What Will Happen")
            st.caption(
                "1) Create Application Group with the name above. "
                "2) Link all selected ADO apps to this group. "
                "3) First selected ADO app is the primary/default instance name; additional selected ADO apps become additional instances."
            )
        conflicts = cand.loc[cand[ado_key].isin(selected_ado) & cand["IS_CONFLICT"]]

        if not conflicts.empty:
            st.dataframe(
                conflicts[[ado_key, "MAPPED_TO"]],
                use_container_width=True,
                hide_index=True,
                height=160,
            )
            msg = f"{len(conflicts)} selected value(s) are already mapped elsewhere."
            if user_is_admin:
                st.warning(msg)
                allow_reassign = st.checkbox(
                    "I understand this will reassign existing mappings",
                    value=False,
                    key=f"ado_onboard_reassign_{entity_type}",
                )
            else:
                st.error(msg + " Your current role cannot reassign existing mappings.")
                allow_reassign = False
        else:
            allow_reassign = True

    col_primary, col_secondary = st.columns([1, 1])
    with col_primary:
        if st.button(
            "Create Group + Link Selected ADO Apps",
            icon=":material/add_circle:",
            key=f"ado_onboard_create_link_{entity_type}",
            disabled=bool(not selected_ado) or (bool(selected_ado) and not allow_reassign),
        ):
            if selected_ado:
                if "allow_reassign" in locals() and not allow_reassign:
                    toast_warning("Confirm reassignment to proceed.")
                else:
                    try:
                        new_id = _create_entity()
                        if not new_id:
                            return
                        map_fn(selected_ado, new_id)
                        # Verify mapping persistence and retry missing values once (app-group flow only).
                        if entity_type == "app_group":
                            try:
                                mapped_now = list_ado_app_mappings()
                                if mapped_now is None:
                                    mapped_now = pd.DataFrame()
                                if not mapped_now.empty and "ADO_APP" in mapped_now.columns and "APP_GROUP" in mapped_now.columns:
                                    mapped_now = mapped_now.copy()
                                    mapped_now["_K"] = mapped_now["ADO_APP"].astype(str).str.strip().str.upper()
                                    mapped_now["_G"] = mapped_now["APP_GROUP"].astype(str).str.strip()
                                    mapped_keys = set(
                                        mapped_now.loc[mapped_now["_G"] == str(new_id), "_K"].tolist()
                                    )
                                    missing = [x for x in selected_ado if str(x).strip().upper() not in mapped_keys]
                                    if missing:
                                        map_fn(missing, new_id)
                                        mapped_now2 = list_ado_app_mappings()
                                        if mapped_now2 is not None and not mapped_now2.empty and "ADO_APP" in mapped_now2.columns and "APP_GROUP" in mapped_now2.columns:
                                            mapped_now2 = mapped_now2.copy()
                                            mapped_now2["_K"] = mapped_now2["ADO_APP"].astype(str).str.strip().str.upper()
                                            mapped_now2["_G"] = mapped_now2["APP_GROUP"].astype(str).str.strip()
                                            mapped_keys2 = set(
                                                mapped_now2.loc[mapped_now2["_G"] == str(new_id), "_K"].tolist()
                                            )
                                            still_missing = [x for x in selected_ado if str(x).strip().upper() not in mapped_keys2]
                                            if still_missing:
                                                toast_warning(
                                                    "Some selected ADO apps could not be linked: "
                                                    + ", ".join(still_missing[:5])
                                                    + (" ..." if len(still_missing) > 5 else "")
                                                )
                            except Exception:
                                pass
                        if callable(on_created):
                            try:
                                on_created(new_id, entity_name, len(selected_ado), selected_ado)
                            except TypeError:
                                on_created(new_id, entity_name, len(selected_ado))
                        toast_success(f"Created and linked {len(selected_ado)} ADO value(s).")
                        post_write_refresh("ado_embedded_create_and_link", rerun=True, bump_version=True)
                    except Exception as e:
                        toast_error(f"Create & link failed: {e}")

    with col_secondary:
        if st.button(
            "Create Group Only (No ADO Links)",
            key=f"ado_onboard_create_only_{entity_type}",
        ):
            try:
                new_id = _create_entity()
                if not new_id:
                    return
                if callable(on_created):
                    try:
                        on_created(new_id, entity_name, 0, [])
                    except TypeError:
                        on_created(new_id, entity_name, 0)
                toast_success("Created.")
                post_write_refresh("ado_embedded_create", rerun=True, bump_version=True)
            except Exception as e:
                toast_error(f"Create failed: {e}")


__all__ = ["render_ado_mapping_panel", "render_embedded_onboard_flow"]
