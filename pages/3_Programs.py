# Summary: update mapping UI labels to NEXT branding.
import re
from typing import Any
import uuid
from datetime import date
from typing import Dict, Optional

import pandas as pd
import streamlit as st

from core.init import init_page

from core.data import fetch_program_allocated_headcount_by_pi
from core.ado_recon import load_derived_fte_by_bucket
from core.ado_mapping_service import (
    create_program_from_ado,
    get_unmapped_ado_apps,
    get_unmapped_ado_programs,
    get_unmapped_ado_teams,
    list_ado_app_mappings,
    list_ado_program_mappings,
    list_ado_team_mappings,
    map_ado_program_to_program,
)
from core.debug import _is_admin
from db import (
    ensure_tables,
    ensure_location_and_contractor_tables,
    ensure_composition_changelog_tables,
    execute,
    fetch_df,
    list_app_users,
    list_program_additional_costs,
    list_program_apptio_workids,
    list_team_headcount,
    recompute_program_composition_from_headcount_year,
    upsert_program,
    upsert_program_additional_cost,
    upsert_program_apptio_workid,
    delete_program_apptio_workid,
    upsert_team_headcount,
    ensure_analytics_views_ok,
    bump_data_version,
)
from utils.admin_delete_guard import (
    get_program_dependencies,
    render_dependency_summary,
    can_delete,
    safe_delete_program,
)
from utils.ado_mapping_ui import render_ado_mapping_panel
from utils.toast import toast_error, toast_success, toast_warning

from utils.app_shell import bootstrap_page
bootstrap_page()

try:
    from db.tco_events import record_cost_events_from_headcount_delta
except Exception:
    record_cost_events_from_headcount_delta = None  # type: ignore[assignment]

try:
    from db.tco_events import insert_cost_event
except Exception:
    insert_cost_event = None  # type: ignore[assignment]

try:
    from db.pi_calendar import map_date_to_pi
except Exception:
    map_date_to_pi = None  # type: ignore[assignment]

# Events are currently disabled for normal planning edits.
# We keep the table/helpers for future explicit change logging, but do not write automatically.
ENABLE_COST_EVENTS = False

page_theme = init_page("Programs", page_path=__file__)
_page_theme = page_theme
user = st.session_state.get("auth_user") or {}
is_admin = _is_admin()

ensure_tables()
ensure_location_and_contractor_tables()
ensure_composition_changelog_tables()


def _run_post_save_refresh(context: str) -> None:
    """Lightweight analytics refresh after write actions."""
    try:
        ensure_analytics_views_ok()
        bump_data_version(context)
    except Exception as e:
        toast_warning(f"Saved ({context}), but analytics refresh encountered an issue: {e}")
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()


def _display_name_map() -> Dict[str, str]:
    try:
        df = fetch_df("SELECT EMAIL, DISPLAY_NAME FROM APP_USERS")
        if df is None or df.empty:
            return {}
        return {
            str(r["EMAIL"]).strip().lower(): (str(r.get("DISPLAY_NAME") or "").strip() or str(r["EMAIL"]).strip())
            for _, r in df.iterrows()
        }
    except Exception:
        return {}


def _display_name_for(email: str) -> str:
    em = (email or "").strip()
    if not em:
        return "Unknown"
    m = _display_name_map()
    return m.get(em.lower(), em)


def _ensure_program_composition_history() -> None:
    execute(
        """
      CREATE TABLE IF NOT EXISTS PROGRAM_COMPOSITION_HISTORY (
        PROGRAMID STRING NOT NULL,
        YEAR NUMBER(4,0) NOT NULL,
        PI NUMBER(1,0) DEFAULT 0,
        PROGRAMFTE FLOAT DEFAULT 0,
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        UPDATED_BY STRING,
        CONSTRAINT PK_PROGRAM_COMPOSITION_HISTORY PRIMARY KEY (PROGRAMID, YEAR, PI)
      )
    """
    )
    try:
        execute("UPDATE PROGRAM_COMPOSITION_HISTORY SET PI = 0 WHERE PI IS NULL")
        execute("ALTER TABLE PROGRAM_COMPOSITION_HISTORY ALTER COLUMN PI NUMBER(1,0) NOT NULL")
    except Exception:
        pass
    try:
        execute("ALTER TABLE PROGRAM_COMPOSITION_HISTORY ADD COLUMN IF NOT EXISTS UPDATED_BY STRING")
    except Exception:
        pass


def _effective_program_composition(program_id: str) -> Dict:
    try:
        df = fetch_df(
            """
            SELECT PROGRAMID, YEAR, PI, PROGRAMFTE, UPDATED_AT, UPDATED_BY
            FROM PROGRAM_COMPOSITION_HISTORY
            WHERE PROGRAMID=%s
            ORDER BY YEAR DESC, PI DESC, UPDATED_AT DESC
            """,
            (program_id,),
        )
        if df is None or df.empty:
            return {}
        df["PI"] = pd.to_numeric(df["PI"], errors="coerce").fillna(0).astype(int)
        current_year = pd.Timestamp.today().year
        prefer = df[(df["YEAR"] == current_year) & (df["PI"] == 0)]
        if not prefer.empty:
            return prefer.iloc[0].to_dict()
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def _sync_program_baseline_from_history(program_id: str) -> None:
    try:
        eff = _effective_program_composition(program_id)
        if not eff:
            return
        execute(
            """
            UPDATE PROGRAMS
            SET PROGRAMFTE=%s, UPDATED_AT=SYSDATETIME()
            WHERE PROGRAMID=%s
            """,
            (eff.get("PROGRAMFTE", 0.0), program_id),
        )
    except Exception:
        pass


_ensure_program_composition_history()
user_display = _display_name_for(user.get("email"))


@st.cache_data(ttl=180, show_spinner=False)
def _programs_df() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE, UPDATED_AT, UPDATED_BY
        FROM PROGRAMS
        ORDER BY PROGRAMNAME
        """
    )


@st.cache_data(ttl=180, show_spinner=False)
def _program_required_fte_by_pi(program_id: str, year: int) -> pd.DataFrame:
    """Kept for compatibility; not shown in UI."""
    try:
        dfp = fetch_df("SELECT PROGRAMNAME FROM PROGRAMS WHERE PROGRAMID = %s", (program_id,))
        program_name = str(dfp.iloc[0].get("PROGRAMNAME") or "").strip() if dfp is not None and not dfp.empty else ""
        if not program_name:
            return pd.DataFrame(columns=["ITERATION_NUM", "DERIVED_FTE"])

        buckets = load_derived_fte_by_bucket(years=[int(year)], programs=[program_name])
        if buckets is None or buckets.empty:
            return pd.DataFrame(columns=["ITERATION_NUM", "DERIVED_FTE"])

        b = buckets.copy()
        b["PI_NUM"] = pd.to_numeric(b.get("PI_NUM"), errors="coerce").astype("Int64")
        b = b[b["PI_NUM"].notna() & b["PI_NUM"].between(1, 4)].copy()
        by_pi = (
            b.groupby("PI_NUM", dropna=False)[["DERIVED_FTE_SUM"]]
            .sum()
            .reset_index()
            .rename(
                columns={
                    "PI_NUM": "ITERATION_NUM",
                    "DERIVED_FTE_SUM": "DERIVED_FTE",
                }
            )
        )
        by_pi["DERIVED_FTE"] = pd.to_numeric(by_pi.get("DERIVED_FTE"), errors="coerce").fillna(0.0)
        return by_pi.sort_values("ITERATION_NUM")
    except Exception:
        return pd.DataFrame(columns=["ITERATION_NUM", "DERIVED_FTE"])


@st.cache_data(ttl=180, show_spinner=False)
def _program_allocated_headcount_by_pi(program_id: str, year: int) -> pd.DataFrame:
    return fetch_program_allocated_headcount_by_pi(program_id, int(year))


def _program_id_for_name_ci(name: str) -> Optional[str]:
    if not name:
        return None
    df = fetch_df(
        "SELECT PROGRAMID FROM PROGRAMS WHERE UPPER(PROGRAMNAME) = UPPER(%s) LIMIT 1",
        (name.strip(),),
    )
    if df is not None and not df.empty:
        return str(df.iloc[0]["PROGRAMID"])
    return None


@st.cache_data(ttl=180, show_spinner=False)
def _ado_years_cached() -> list[int]:
    try:
        df = fetch_df(
            "SELECT DISTINCT TRY_CONVERT(INT, ADO_YEAR) AS ADO_YEAR FROM ADO_FEATURES WHERE ADO_YEAR IS NOT NULL",
            None,
        )
        if df is None or df.empty:
            return []
        years = pd.to_numeric(df["ADO_YEAR"], errors="coerce").dropna().astype(int).tolist()
        return sorted(set(years))
    except Exception:
        return []


@st.cache_data(ttl=180, show_spinner=False)
def _load_ado_features_for_mapping_candidates_cached(years: tuple[int, ...]) -> pd.DataFrame:
    if not years:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(years))
    sql = f"""
        SELECT
          FEATURE_ID,
          COALESCE(PROGRAM_RAW, AREA_LEVEL2_RAW) AS PROGRAM_RAW,
          AREA_LEVEL2_RAW,
          TEAM_VARIANT_KEY,
          TEAM_RAW,
          AREA_LEVEL3_RAW,
          AREA_LEVEL4_RAW,
          AREA_PATH_RAW,
          APP_NAME_RAW,
          TRY_CONVERT(FLOAT, STORY_POINTS) AS STORY_POINTS,
          TRY_CONVERT(FLOAT, EFFORT_POINTS) AS EFFORT_POINTS,
          TRY_CONVERT(DATETIME2, COALESCE(CHANGED_AT, CREATED_AT)) AS LAST_SEEN
        FROM ADO_FEATURES
        WHERE TRY_CONVERT(INT, ADO_YEAR) IN ({placeholders})
          AND COALESCE(PROGRAM_RAW, AREA_LEVEL2_RAW) IS NOT NULL
          AND LTRIM(RTRIM(COALESCE(PROGRAM_RAW, AREA_LEVEL2_RAW))) <> ''
    """
    df = fetch_df(sql, tuple(years))
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


st.title("Programs")
st.caption(
    "Maintain mappings, rates, and reference data"
)

section_options = ["Overview", "Onboard"] + (["Headcount & Rates", "Audit"] if is_admin else [])
if st.session_state.get("programs_section") not in section_options:
    st.session_state["programs_section"] = section_options[0]
if hasattr(st, "segmented_control"):
    section = st.segmented_control("Section", section_options, key="programs_section")
else:
    section = st.radio("Section", section_options, horizontal=True, key="programs_section")
if not section:
    section = section_options[0]
if not is_admin:
    st.info("🔒 Headcount & Rates and Audit are available to Admin users.")

progs = _programs_df()
progs = progs.copy() if progs is not None else pd.DataFrame()

program_options = ["(Select a program)"] + (
    progs["PROGRAMNAME"].astype(str).tolist() if not progs.empty and "PROGRAMNAME" in progs.columns else []
)
if st.session_state.get("programs_pick_pending"):
    st.session_state["programs_pick"] = st.session_state.get("programs_pick_pending")
    st.session_state.pop("programs_pick_pending", None)
program_pick = st.selectbox("Program", options=program_options, index=0, key="programs_pick")

program_row = None
if program_pick and program_pick != "(Select a program)" and not progs.empty:
    try:
        program_row = progs.loc[progs["PROGRAMNAME"].astype(str) == str(program_pick)].iloc[0]
    except Exception:
        program_row = None

def _users_for_selectbox() -> tuple[list[str], dict[str, str]]:
    try:
        # `list_app_users()` should return a DataFrame, but it may raise (e.g. if the DB
        # is temporarily unavailable). Never let Programs crash because a selectbox
        # options query failed.
        tmp = _list_app_users_cached()
        users_df = tmp if isinstance(tmp, pd.DataFrame) else pd.DataFrame()

        if not users_df.empty and "IS_ACTIVE" in users_df.columns:
            is_active = users_df["IS_ACTIVE"]
            if isinstance(is_active, pd.DataFrame):
                is_active = is_active.iloc[:, 0]
            users_df = users_df.loc[pd.to_numeric(is_active, errors="coerce").fillna(1).astype(int) > 0].copy()
    except Exception:
        return [], {}

    def _label_row(r):
        email = str(r.get("EMAIL") or "").strip()
        dn = str(r.get("DISPLAY_NAME") or "").strip()
        return f"{dn} ({email})" if dn else email

    opts = [(_label_row(r), str(r["EMAIL"]).strip()) for _, r in users_df.iterrows()] if (not users_df.empty and "EMAIL" in users_df.columns) else []
    labels = [lbl for (lbl, _) in opts]
    label_to_email = {lbl: em for (lbl, em) in opts}
    return labels, label_to_email


@st.cache_data(ttl=120, show_spinner=False)
def _list_app_users_cached() -> pd.DataFrame:
    try:
        tmp = list_app_users()
        return tmp if isinstance(tmp, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def _list_team_headcount_cached(program_id: str) -> pd.DataFrame:
    try:
        df = list_team_headcount(program_id)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _program_fragment(fn):
    frag = getattr(st, "fragment", None)
    return frag(fn) if callable(frag) else fn


@_program_fragment
def render_program_headcount_editor(*, edit_program_id: str, program_pick: str, user_display: str) -> None:
    loc_cols = ["GBC", "US-0970+HC", "US-0910", "OTHER"]

    st.divider()
    st.markdown("##### Headcount editor")
    st.caption(
        "Edit values in the table, then click Save headcount. Use All PIs to set PI=0 (year default), or Per PI to edit PI overrides (I1–I4)."
    )

    def _dominant_value(df: pd.DataFrame, key_col: str) -> str:
        if df is None or df.empty:
            return ""
        try:
            w = df.copy()
            w[key_col] = w.get(key_col, "").astype(str).str.strip()
            w["HEADCOUNT"] = pd.to_numeric(w.get("HEADCOUNT"), errors="coerce").fillna(0.0)
            w = w[w[key_col].ne("")]
            if w.empty:
                return ""
            by = w.groupby(key_col, dropna=False)["HEADCOUNT"].sum().sort_values(ascending=False)
            return str(by.index[0])
        except Exception:
            return ""

    hc_year_sel = st.number_input(
        "Year",
        min_value=2020,
        max_value=2100,
        step=1,
        value=date.today().year,
        key=f"prog_hc_year_{edit_program_id}",
    )
    year_i = int(hc_year_sel)

    selected_pis = [1, 2, 3, 4]
    fixed_cols = [
        "LOCATION_KEY",
        "__ORIG_LOCATION_KEY",
        "HC_ALL",
        "HC_I1",
        "HC_I2",
        "HC_I3",
        "HC_I4",
        "NOTE_ALL",
        "NOTE_I1",
        "NOTE_I2",
        "NOTE_I3",
        "NOTE_I4",
    ]
    editor_key = f"hc_editor_program_overhead_{edit_program_id}_{year_i}_v2"
    # widget_key stores Streamlit widget state dict; df_key stores the dataframe.
    widget_key = editor_key
    df_key = f"{editor_key}__df"
    ctx_key = f"{editor_key}__ctx"
    editor_meta_key = f"programs_hc_editor_meta_{edit_program_id}"
    editor_ctx = (edit_program_id, int(year_i), "overhead")

    def _init_prog_df(hc_year_in: pd.DataFrame, prog_map_in: dict, prog_note_map_in: dict) -> pd.DataFrame:
            keys: set[str] = set()
            if hc_year_in is not None and not hc_year_in.empty and "LOCATION" in hc_year_in.columns:
                for loc in hc_year_in["LOCATION"].dropna().astype(str).str.strip().tolist():
                    if loc:
                        keys.add(loc)
            if not keys:
                return pd.DataFrame(columns=fixed_cols)

            def _baseline_for(loc: str) -> float:
                v0 = prog_map_in.get((loc, 0), None)
                if v0 is not None:
                    return float(v0)
                for pi in [1, 2, 3, 4]:
                    vv = prog_map_in.get((loc, pi), None)
                    if vv is not None:
                        return float(vv)
                return 0.0

            rows = []
            for loc in sorted(keys):
                base = float(_baseline_for(loc))
                rows.append(
                    {
                        "LOCATION_KEY": loc,
                        "__ORIG_LOCATION_KEY": loc,
                        "HC_ALL": base,
                        "HC_I1": float(prog_map_in.get((loc, 1), base)),
                        "HC_I2": float(prog_map_in.get((loc, 2), base)),
                        "HC_I3": float(prog_map_in.get((loc, 3), base)),
                        "HC_I4": float(prog_map_in.get((loc, 4), base)),
                        "NOTE_ALL": str(prog_note_map_in.get((loc, 0), "")),
                        "NOTE_I1": str(prog_note_map_in.get((loc, 1), "")),
                        "NOTE_I2": str(prog_note_map_in.get((loc, 2), "")),
                        "NOTE_I3": str(prog_note_map_in.get((loc, 3), "")),
                        "NOTE_I4": str(prog_note_map_in.get((loc, 4), "")),
                    }
                )
            return pd.DataFrame(rows, columns=fixed_cols)

    editor_meta = st.session_state.get(editor_meta_key)
    if not isinstance(editor_meta, dict) or editor_meta.get("ctx") != editor_ctx:
        # ------------------------------------------------------------------
        # Auto-sync: keep PROGRAM_COMPOSITION_HISTORY (used by Budget) aligned
        # with the canonical program headcount rows (stored in TEAM_HEADCOUNT_HISTORY
        # with CLASS='PROGRAM' and TEAMID=PROGRAMID).
        #
        # Runs only when the editor context changes (program/year), not on every rerun.
        # ------------------------------------------------------------------
        try:
            need_sync = False
            df_comp = fetch_df(
                """
                SELECT PI, UPDATED_AT
                FROM PROGRAM_COMPOSITION_HISTORY
                WHERE PROGRAMID=%s AND YEAR=%s
                """,
                (edit_program_id, int(year_i)),
            )
            if df_comp is None or df_comp.empty:
                need_sync = True
                comp_pis: set[int] = set()
                comp_max = None
            else:
                comp_pi_col = pd.to_numeric(df_comp.get("PI"), errors="coerce").fillna(0).astype(int)
                comp_pis = set(int(p) for p in comp_pi_col.tolist() if 1 <= int(p) <= 4)
                comp_max = pd.to_datetime(df_comp.get("UPDATED_AT"), errors="coerce").max()

            df_hp = fetch_df(
                """
                SELECT DISTINCT PI
                FROM TEAM_HEADCOUNT_HISTORY
                WHERE TEAMID=%s AND YEAR=%s
                  AND UPPER(LTRIM(RTRIM(CLASS)))='PROGRAM'
                  AND TRY_CONVERT(INT, PI) BETWEEN 1 AND 4
                """,
                (edit_program_id, int(year_i)),
            )
            override_pis = set()
            if df_hp is not None and not df_hp.empty and "PI" in df_hp.columns:
                override_pis = set(
                    int(p)
                    for p in pd.to_numeric(df_hp["PI"], errors="coerce").dropna().astype(int).tolist()
                    if 1 <= int(p) <= 4
                )

            if comp_pis and (comp_pis - override_pis):
                need_sync = True

            if not need_sync:
                df_src = fetch_df(
                    """
                    SELECT MAX(UPDATED_AT) AS SRC_MAX
                    FROM TEAM_HEADCOUNT_HISTORY
                    WHERE TEAMID=%s AND YEAR=%s AND UPPER(LTRIM(RTRIM(CLASS)))='PROGRAM'
                    """,
                    (edit_program_id, int(year_i)),
                )
                src_max = None
                if df_src is not None and not df_src.empty:
                    src_max = pd.to_datetime(df_src.iloc[0].get("SRC_MAX"), errors="coerce")
                if src_max is not None and pd.notna(src_max) and comp_max is not None and pd.notna(comp_max):
                    if src_max > comp_max:
                        need_sync = True
                elif src_max is not None and pd.notna(src_max) and comp_max is None:
                    need_sync = True

            if need_sync:
                recompute_program_composition_from_headcount_year(
                    program_id=edit_program_id,
                    year=int(year_i),
                    changed_by=str(user.get("email") or ""),
                    source="HEADCOUNT_AUTO_SYNC",
                )
                _sync_program_baseline_from_history(edit_program_id)
        except Exception:
            pass

        hc_all = _list_team_headcount_cached(edit_program_id)
        hc_all = hc_all.copy() if hc_all is not None and not hc_all.empty else pd.DataFrame()
        if not hc_all.empty:
            hc_all["YEAR_NUM"] = pd.to_numeric(hc_all.get("YEAR"), errors="coerce").fillna(0).astype(int)
            hc_all["PI_NUM"] = pd.to_numeric(hc_all.get("PI"), errors="coerce").fillna(0).astype(int)
            hc_all["CLASS"] = hc_all.get("CLASS", "").astype(str).str.strip().str.upper()
            hc_all["LOCATION"] = hc_all.get("LOCATION", "").astype(str).str.strip()
            hc_all["HEADCOUNT"] = pd.to_numeric(hc_all.get("HEADCOUNT"), errors="coerce").fillna(0.0)
            if "NOTES" in hc_all.columns:
                hc_all["NOTES"] = hc_all.get("NOTES", "").fillna("").astype(str).str.strip()

        hc_year = (
            hc_all.loc[(hc_all["YEAR_NUM"] == int(year_i)) & (hc_all["CLASS"] == "PROGRAM")].copy()
            if not hc_all.empty
            else pd.DataFrame()
        )
        prog_notes_enabled = bool(hc_year is not None and not hc_year.empty and "NOTES" in hc_year.columns)

        loc_opts = set(loc_cols)
        if hc_year is not None and not hc_year.empty:
            loc_opts.update([str(x).strip() for x in hc_year.get("LOCATION", "").astype(str).tolist() if str(x).strip()])
        try:
            global_locs = fetch_df("SELECT DISTINCT LOCATION FROM VW_GLOBAL_RATE_EFFECTIVE WHERE YEAR=%s", (int(year_i),))
            if global_locs is not None and not global_locs.empty and "LOCATION" in global_locs.columns:
                loc_opts.update([str(x).strip() for x in global_locs["LOCATION"].astype(str).tolist() if str(x).strip()])
        except Exception:
            pass
        location_options = [""] + [l for l in loc_cols if l in loc_opts] + sorted([l for l in loc_opts if l and l not in loc_cols])

        prog_map: dict[tuple[str, int], float] = {}
        prog_note_map: dict[tuple[str, int], str] = {}
        if hc_year is not None and not hc_year.empty:
            for _, rr in hc_year.loc[hc_year["PI_NUM"].isin([0, 1, 2, 3, 4])].iterrows():
                loc = str(rr.get("LOCATION") or "").strip()
                pi = int(rr.get("PI_NUM") or 0)
                if not loc:
                    continue
                prog_map[(loc, pi)] = prog_map.get((loc, pi), 0.0) + float(rr.get("HEADCOUNT") or 0.0)
                if prog_notes_enabled:
                    note = str(rr.get("NOTES") or "").strip()
                    if note:
                        prog_note_map[(loc, pi)] = note

        # Reset widget state only on context changes to avoid desync.
        if st.session_state.get(ctx_key) != editor_ctx:
            st.session_state[df_key] = _init_prog_df(hc_year, prog_map, prog_note_map)
            st.session_state.pop(widget_key, None)
            st.session_state[ctx_key] = editor_ctx
            st.session_state[f"{editor_key}__init"] = st.session_state[df_key].copy()
        st.session_state[editor_meta_key] = {
            "ctx": editor_ctx,
            "hc_year": hc_year,
            "location_options": location_options,
            "prog_map": prog_map,
            "prog_note_map": prog_note_map,
            "prog_notes_enabled": prog_notes_enabled,
        }

    editor_meta = st.session_state.get(editor_meta_key, {}) if isinstance(st.session_state.get(editor_meta_key), dict) else {}
    hc_year = editor_meta.get("hc_year", pd.DataFrame())
    location_options = editor_meta.get("location_options", [""] + list(loc_cols))
    prog_map = editor_meta.get("prog_map", {}) or {}
    prog_note_map = editor_meta.get("prog_note_map", {}) or {}
    prog_notes_enabled = bool(editor_meta.get("prog_notes_enabled"))
    prog_has_overrides = any(int(k[1]) in [1, 2, 3, 4] for k in prog_map.keys()) if isinstance(prog_map, dict) else False

    # Keep options stable while editing; include current table values so selectboxes don't blank out.
    prog_full0 = st.session_state.get(df_key)
    if not isinstance(prog_full0, pd.DataFrame):
        prog_full0 = st.session_state.get(f"{editor_key}__init")
    if not isinstance(prog_full0, pd.DataFrame):
        prog_full0 = _init_prog_df(hc_year, prog_map, prog_note_map)
    if not isinstance(st.session_state.get(df_key), pd.DataFrame):
        st.session_state[df_key] = prog_full0.copy()
    if prog_full0 is not None and not prog_full0.empty and "LOCATION_KEY" in prog_full0.columns:
        location_options = list(
            dict.fromkeys(location_options + [str(x).strip() for x in prog_full0["LOCATION_KEY"].dropna().astype(str).tolist() if str(x).strip()])
        )
    st.session_state[editor_meta_key] = {**editor_meta, "location_options": location_options}

    st.markdown("###### Program Overhead (by Location)")
    prog_mode = st.radio(
        "Program overhead input mode",
        options=["All PIs same", "Per PI"],
        index=1 if prog_has_overrides else 0,
        horizontal=True,
        key=f"programs_overhead_mode_{edit_program_id}_{year_i}",
    )
    visible_pi_cols = [f"HC_I{pi}" for pi in selected_pis]

    def _coerce_editor_df(val: object, cols: list[str], fallback: pd.DataFrame) -> pd.DataFrame:
        if isinstance(val, pd.DataFrame):
            df = val.copy()
        elif isinstance(val, list) or isinstance(val, dict):
            try:
                df = pd.DataFrame(val)
            except Exception:
                df = fallback.copy() if isinstance(fallback, pd.DataFrame) else pd.DataFrame()
        else:
            df = fallback.copy() if isinstance(fallback, pd.DataFrame) else pd.DataFrame()
        for col in cols:
            if col not in df.columns:
                df[col] = "" if col in ["LOCATION_KEY", "__ORIG_LOCATION_KEY"] or col.startswith("NOTE_") else 0.0
        return df.reindex(columns=cols)

    current_df = st.session_state.get(df_key)
    if not isinstance(current_df, pd.DataFrame):
        current_df = prog_full0.copy()
        st.session_state[df_key] = current_df.copy()
    prog_full = _coerce_editor_df(current_df, fixed_cols, prog_full0)
    # Normalize before rendering so selectbox values stay stable without mutating widget state.
    note_cols = [c for c in fixed_cols if c.startswith("NOTE_")]
    str_cols = ["LOCATION_KEY", "__ORIG_LOCATION_KEY"] + note_cols
    num_cols = [c for c in fixed_cols if c not in str_cols]
    for col in str_cols:
        if col in prog_full.columns:
            s = prog_full[col].fillna("").astype(str).str.strip()
            s = s.replace({"nan": "", "<NA>": "", "None": ""})
            prog_full[col] = s
    for col in num_cols:
        if col in prog_full.columns:
            prog_full[col] = pd.to_numeric(prog_full[col], errors="coerce").fillna(0.0)

    prog_col_cfg: dict[str, object] = {
        "__ORIG_LOCATION_KEY": None,
        "LOCATION_KEY": st.column_config.SelectboxColumn("Location", options=location_options, required=False),
        "HC_ALL": st.column_config.NumberColumn("All PIs", step=0.1, min_value=0.0) if prog_mode == "All PIs same" else None,
        "NOTE_ALL": st.column_config.TextColumn("Note (All PIs)") if (prog_notes_enabled and prog_mode == "All PIs same") else None,
    }
    for pi in [1, 2, 3, 4]:
        prog_col_cfg[f"HC_I{pi}"] = (
            st.column_config.NumberColumn(f"I{pi}", step=0.1, min_value=0.0) if (prog_mode == "Per PI" and pi in selected_pis) else None
        )
        prog_col_cfg[f"NOTE_I{pi}"] = (
            st.column_config.TextColumn(f"Note I{pi}") if (prog_notes_enabled and prog_mode == "Per PI" and pi in selected_pis) else None
        )

    with st.form(key=f"{widget_key}__form", clear_on_submit=False):
        prog_out = st.data_editor(
            prog_full[fixed_cols],
            hide_index=True,
            num_rows="dynamic",
            column_config=prog_col_cfg,
            key=widget_key,
        )
        apply_btn = st.form_submit_button("Apply changes")
    prog_edited = prog_out.copy() if isinstance(prog_out, pd.DataFrame) else pd.DataFrame(columns=fixed_cols)
    if apply_btn:
        # Normalize editor output to keep schema stable and avoid extra numeric columns.
        prog_edited = prog_edited.reindex(columns=fixed_cols)
        note_cols = [c for c in fixed_cols if c.startswith("NOTE_")]
        str_cols = ["LOCATION_KEY", "__ORIG_LOCATION_KEY"] + note_cols
        num_cols = [c for c in fixed_cols if c not in str_cols]
        for col in str_cols:
            if col in prog_edited.columns:
                s = prog_edited[col].fillna("").astype(str).str.strip()
                s = s.replace({"nan": "", "<NA>": "", "None": ""})
                prog_edited[col] = s
        for col in num_cols:
            if col in prog_edited.columns:
                prog_edited[col] = pd.to_numeric(prog_edited[col], errors="coerce").fillna(0.0)

        # Streamlit may emit NaN/NA for selectbox cells during edits; normalize to stable strings
        # so selections don't appear to "disappear" on rerun.
        for _col in ["LOCATION_KEY", "__ORIG_LOCATION_KEY"]:
            if _col in prog_edited.columns:
                s = prog_edited[_col].fillna("").astype(str).str.strip()
                s = s.replace({"nan": "", "<NA>": "", "None": ""})
                prog_edited[_col] = s

        if isinstance(prog_full, pd.DataFrame) and not prog_full.empty:
            prog_visible_cols_set = {"LOCATION_KEY"}
            if prog_mode == "All PIs same":
                prog_visible_cols_set.add("HC_ALL")
            else:
                prog_visible_cols_set |= {f"HC_I{pi}" for pi in selected_pis}
            prog_backfill_cols = [c for c in fixed_cols if c not in prog_visible_cols_set]

            prev_lookup = {str(r.get("LOCATION_KEY") or "").strip(): r for _, r in prog_full.iterrows()}
            for idx, r in prog_edited.iterrows():
                key = str(r.get("LOCATION_KEY") or "").strip()
                prev = prev_lookup.get(key)
                if prev is None:
                    continue
                # Only backfill hidden columns so we don't clobber in-progress edits.
                for col in prog_backfill_cols:
                    if pd.isna(r.get(col)):
                        prog_edited.at[idx, col] = prev.get(col)
        prog_edited["__ORIG_LOCATION_KEY"] = prog_edited.get("__ORIG_LOCATION_KEY", "").replace({None: ""})
        prog_edited.loc[
            prog_edited["__ORIG_LOCATION_KEY"].astype(str).str.strip().eq(""), "__ORIG_LOCATION_KEY"
        ] = prog_edited.get("LOCATION_KEY", "")
        st.session_state[df_key] = prog_edited.reindex(columns=fixed_cols).copy()
        st.session_state.pop(widget_key, None)
        st.rerun()
    prog_full = prog_edited.copy()

    btn1, btn2 = st.columns([1, 1])
    save_hc = btn1.button("Save headcount", type="primary", key=f"programs_hc_save_{edit_program_id}_{year_i}")
    reset_hc = btn2.button("Reset", key=f"programs_hc_reset_{edit_program_id}_{year_i}")
    if reset_hc:
        for k in [editor_meta_key, editor_key, df_key, ctx_key, f"{editor_key}__init"]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

    if save_hc:
        try:
            prog_full = st.session_state.get(df_key, prog_edited).copy() if isinstance(prog_edited, pd.DataFrame) else pd.DataFrame(columns=fixed_cols)
            existing_locs = {loc for (loc, _pi) in prog_map.keys()}

            def _update_program_headcount_note(*, pi: int, loc: str, note: str) -> None:
                if not prog_notes_enabled:
                    return
                note_val = (note or "").strip()
                for col in ["NOTES", "NOTE"]:
                    try:
                        execute(
                            f"""
                            UPDATE TEAM_HEADCOUNT_HISTORY
                            SET {col}=%s, UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(%s, UPDATED_BY)
                            WHERE TEAMID=%s AND YEAR=%s AND ISNULL(PI,0)=%s
                              AND UPPER(LTRIM(RTRIM(CLASS)))='PROGRAM' AND LOCATION=%s
                            """,
                            (note_val, user_display, edit_program_id, int(year_i), int(pi), str(loc)),
                        )
                        return
                    except Exception:
                        try:
                            execute(
                                f"""
                                UPDATE TEAM_HEADCOUNT_HISTORY
                                SET {col}=%s, UPDATED_AT=SYSDATETIME()
                                WHERE TEAMID=%s AND YEAR=%s AND ISNULL(PI,0)=%s
                                  AND UPPER(LTRIM(RTRIM(CLASS)))='PROGRAM' AND LOCATION=%s
                                """,
                                (note_val, edit_program_id, int(year_i), int(pi), str(loc)),
                            )
                            return
                        except Exception:
                            continue

            def _num(v: object) -> float:
                x = pd.to_numeric(v, errors="coerce")
                if x is None or pd.isna(x):
                    return 0.0
                return float(x)

            def _s(v: object) -> str:
                if v is None or pd.isna(v):
                    return ""
                return str(v).strip()

            rows = []
            for _, rr in (prog_full if prog_full is not None else pd.DataFrame()).iterrows():
                loc = _s(rr.get("LOCATION_KEY"))
                orig_loc = _s(rr.get("__ORIG_LOCATION_KEY"))
                hc_all = _num(rr.get("HC_ALL"))
                pi_vals_all = {pi: _num(rr.get(f"HC_I{pi}")) for pi in [1, 2, 3, 4]}
                note_all = _s(rr.get("NOTE_ALL")) if prog_notes_enabled else ""
                pi_notes_all = {pi: _s(rr.get(f"NOTE_I{pi}")) for pi in [1, 2, 3, 4]} if prog_notes_enabled else {}
                if prog_mode == "All PIs same":
                    any_nonzero = hc_all != 0.0
                    any_note = bool(note_all.strip()) if prog_notes_enabled else False
                else:
                    any_nonzero = any(pi_vals_all[pi] != 0.0 for pi in selected_pis)
                    any_note = any(bool((pi_notes_all.get(pi) or "").strip()) for pi in selected_pis) if prog_notes_enabled else False
                is_blank_row = (not loc) and (not orig_loc) and (not any_nonzero) and (not any_note)
                if is_blank_row:
                    continue
                if orig_loc and not loc:
                    raise ValueError("To remove an existing Program location row, delete the row in the table (don’t clear Location).")
                if not loc:
                    if any_nonzero or any_note:
                        raise ValueError("Location is required for Program Overhead rows with headcount.")
                    continue
                rows.append(
                    {
                        "loc": loc,
                        "orig_loc": orig_loc,
                        "hc_all": hc_all,
                        "pi_vals": pi_vals_all,
                        "note_all": note_all,
                        "pi_notes": pi_notes_all,
                    }
                )

            new_locs = {r["loc"] for r in rows}
            if len(new_locs) != len(rows):
                raise ValueError("Duplicate Program Overhead location.")

            renamed_orig: set[str] = set()
            for r in rows:
                if not r["orig_loc"]:
                    continue
                if r["orig_loc"] in existing_locs and r["orig_loc"] != r["loc"]:
                    # When in Per PI mode, preserve PI values the user didn't edit (hidden PI columns).
                    # In All PIs mode, we intentionally clear overrides and do not carry them over.
                    if prog_mode == "Per PI":
                        for pi in [1, 2, 3, 4]:
                            if pi in selected_pis:
                                continue
                            old_v = prog_map.get((r["orig_loc"], int(pi)), None)
                            if old_v is None:
                                continue
                            upsert_team_headcount(
                                edit_program_id,
                                int(year_i),
                                int(pi),
                                "PROGRAM",
                                r["loc"],
                                float(old_v),
                                updated_by=user_display,
                            )
                            old_note = str(prog_note_map.get((r["orig_loc"], int(pi)), "") or "").strip()
                            if old_note:
                                _update_program_headcount_note(pi=int(pi), loc=r["loc"], note=old_note)
                    execute(
                        "DELETE FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID=%s AND YEAR=%s AND CLASS=%s AND LOCATION=%s",
                        (edit_program_id, int(year_i), "PROGRAM", r["orig_loc"]),
                    )
                    renamed_orig.add(r["orig_loc"])

            deleted_locs = (existing_locs - new_locs) - renamed_orig
            for loc in deleted_locs:
                execute(
                    "DELETE FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID=%s AND YEAR=%s AND CLASS=%s AND LOCATION=%s",
                    (edit_program_id, int(year_i), "PROGRAM", str(loc)),
                )

            for r in rows:
                upsert_team_headcount(
                    edit_program_id,
                    int(year_i),
                    0,
                    "PROGRAM",
                    r["loc"],
                    float(r["hc_all"]),
                    updated_by=user_display,
                )
                if prog_notes_enabled:
                    _update_program_headcount_note(pi=0, loc=r["loc"], note=str(r.get("note_all") or ""))
                if prog_mode == "All PIs same":
                    # Clear PI-specific overrides so PI=0 applies to all PIs again.
                    for pi in [1, 2, 3, 4]:
                        execute(
                            "DELETE FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID=%s AND YEAR=%s AND PI=%s AND CLASS=%s AND LOCATION=%s",
                            (edit_program_id, int(year_i), int(pi), "PROGRAM", r["loc"]),
                        )
                else:
                    for pi in selected_pis:
                        hc_pi = float(r["pi_vals"].get(int(pi), r["hc_all"]))
                        note_pi = str((r.get("pi_notes") or {}).get(int(pi), "") or "").strip() if prog_notes_enabled else ""
                        if hc_pi == float(r["hc_all"]) and not note_pi:
                            execute(
                                "DELETE FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID=%s AND YEAR=%s AND PI=%s AND CLASS=%s AND LOCATION=%s",
                                (edit_program_id, int(year_i), int(pi), "PROGRAM", r["loc"]),
                            )
                        else:
                            upsert_team_headcount(
                                edit_program_id,
                                int(year_i),
                                int(pi),
                                "PROGRAM",
                                r["loc"],
                                float(hc_pi),
                                updated_by=user_display,
                            )
                            if prog_notes_enabled:
                                _update_program_headcount_note(pi=int(pi), loc=r["loc"], note=note_pi)

            # Best-effort: record cost events from Program overhead headcount deltas (per PI effective).
            try:
                if ENABLE_COST_EVENTS and record_cost_events_from_headcount_delta:
                    locs = sorted(
                        {str(r.get("loc") or "").strip() for r in rows if str(r.get("loc") or "").strip()}
                        | {str(x or "").strip() for x in deleted_locs if str(x or "").strip()}
                    )
                    rate_map: dict[tuple[str, int], float] = {}
                    if locs:
                        placeholders = ", ".join(["%s"] * len(locs))
                        df_rates = fetch_df(
                            f"""
                            SELECT LOCATION, PI, PROGRAM_XOM_RATE
                            FROM VW_PROGRAM_RATE_EFFECTIVE
                            WHERE PROGRAMID=%s AND YEAR=%s AND PI IN (1,2,3,4) AND LOCATION IN ({placeholders})
                            """,
                            tuple([edit_program_id, int(year_i)] + locs),
                        )
                        if df_rates is not None and not df_rates.empty:
                            for _, rr in df_rates.iterrows():
                                loc = str(rr.get("LOCATION") or "").strip()
                                pi = int(pd.to_numeric(rr.get("PI"), errors="coerce") or 0)
                                rate = float(pd.to_numeric(rr.get("PROGRAM_XOM_RATE"), errors="coerce") or 0.0)
                                if loc and pi in [1, 2, 3, 4]:
                                    rate_map[(loc, pi)] = rate

                    def _old_eff(loc: str, pi: int) -> float:
                        base = float(prog_map.get((loc, 0), 0.0) or 0.0)
                        ov = prog_map.get((loc, int(pi)), None)
                        return float(ov) if ov is not None else base

                    program_name_ev = str(program_pick or "")
                    for r in rows:
                        loc_new = str(r.get("loc") or "").strip()
                        loc_old = str(r.get("orig_loc") or "").strip() or loc_new
                        base_new = float(r.get("hc_all") or 0.0)
                        pi_vals = r.get("pi_vals", {}) or {}
                        for pi in [1, 2, 3, 4]:
                            old_eff = _old_eff(loc_old, pi)
                            if pi in selected_pis:
                                new_eff = float(pi_vals.get(pi, base_new))
                            else:
                                ov = prog_map.get((loc_old, int(pi)), None)
                                new_eff = float(ov) if ov is not None else base_new
                            if new_eff == old_eff:
                                continue
                            rate = float(rate_map.get((loc_new, pi), 0.0))
                            record_cost_events_from_headcount_delta(
                                None,
                                fiscal_year=int(year_i),
                                pi_name=f"I{pi}",
                                program=program_name_ev,
                                team=None,
                                app_group=None,
                                bucket_name="Program",
                                old_headcount=float(old_eff),
                                new_headcount=float(new_eff),
                                annual_rate=float(rate),
                                notes=f"Programs page • {loc_new}",
                                cost_bucket="WF",
                                related_object_id=f"PROGRAM:{edit_program_id}|{loc_new}|PI{pi}",
                            )

                    for loc in deleted_locs:
                        loc_s = str(loc or "").strip()
                        for pi in [1, 2, 3, 4]:
                            old_eff = _old_eff(loc_s, pi)
                            if old_eff == 0.0:
                                continue
                            rate = float(rate_map.get((loc_s, pi), 0.0))
                            record_cost_events_from_headcount_delta(
                                None,
                                fiscal_year=int(year_i),
                                pi_name=f"I{pi}",
                                program=program_name_ev,
                                team=None,
                                app_group=None,
                                bucket_name="Program",
                                old_headcount=float(old_eff),
                                new_headcount=0.0,
                                annual_rate=float(rate),
                                notes=f"Programs page • {loc_s} • row removed",
                                cost_bucket="WF",
                                related_object_id=f"PROGRAM:{edit_program_id}|{loc_s}|PI{pi}",
                            )
            except Exception:
                pass

            recompute_program_composition_from_headcount_year(
                program_id=edit_program_id, year=int(year_i), changed_by=user_display
            )
            _sync_program_baseline_from_history(edit_program_id)
            toast_success("Headcount saved. Program composition updated automatically.")
            for k in [editor_meta_key, editor_key, df_key, ctx_key, f"{editor_key}__init"]:
                if k in st.session_state:
                    del st.session_state[k]
            kpi_cache_key = f"program_overview_kpis_{edit_program_id}_{int(year_i)}"
            if kpi_cache_key in st.session_state:
                del st.session_state[kpi_cache_key]
            st.session_state["programs_pick_pending"] = program_pick
            _run_post_save_refresh("program_headcount_save")
        except Exception as e:
            toast_error(f"Save failed: {e}")


if section == "Overview":
    st.markdown("### Overview")
    st.write("Headcount drives workforce costs in Budget. Composition is calculated automatically.")
    with st.container(border=True):
        st.markdown("**Onboarding checklist**")
        ado_years = _ado_years_cached()
        years_to_check = tuple(sorted(ado_years)[-3:]) if ado_years else (date.today().year,)
        ado_features_df = _load_ado_features_for_mapping_candidates_cached(years_to_check)
        if ado_features_df is None or not isinstance(ado_features_df, pd.DataFrame) or ado_features_df.empty:
            st.caption("No ADO data available yet. Load ADO features to populate this checklist.")
        else:
            prog_map_df = list_ado_program_mappings()
            unmapped_prog = get_unmapped_ado_programs(ado_features_df, prog_map_df)
            lines = [f"- Programs not onboarded (ADO): {int(len(unmapped_prog)) if unmapped_prog is not None else 0}"]
            if program_row is not None:
                program_id = str(program_row.get("PROGRAMID") or "")
                ado_prog_keys = []
                if (
                    program_id
                    and prog_map_df is not None
                    and not prog_map_df.empty
                    and "PROGRAMID" in prog_map_df.columns
                    and "ADO_PROGRAM" in prog_map_df.columns
                ):
                    ado_prog_keys = (
                        prog_map_df.loc[prog_map_df["PROGRAMID"].astype(str) == program_id, "ADO_PROGRAM"]
                        .dropna()
                        .astype(str)
                        .tolist()
                    )
                ado_scope_df = (
                    ado_features_df[ado_features_df["PROGRAM_RAW"].isin(ado_prog_keys)]
                    if ado_prog_keys
                    else pd.DataFrame()
                )
                team_map_df = list_ado_team_mappings()
                app_map_df = list_ado_app_mappings()
                team_unmapped = (
                    get_unmapped_ado_teams(ado_scope_df, team_map_df)
                    if ado_scope_df is not None and not ado_scope_df.empty
                    else pd.DataFrame()
                )
                app_unmapped = (
                    get_unmapped_ado_apps(ado_scope_df, app_map_df)
                    if ado_scope_df is not None and not ado_scope_df.empty
                    else pd.DataFrame()
                )
                lines.append(
                    f"- Teams not onboarded (ADO) for this Program: {int(len(team_unmapped)) if team_unmapped is not None else 0}"
                )
                lines.append(
                    f"- Applications not onboarded (ADO) for this Program: {int(len(app_unmapped)) if app_unmapped is not None else 0}"
                )
            st.markdown("\n".join(lines))
    if program_row is None:
        if progs.empty:
            st.info("No programs yet.")
        else:
            st.info("Select a program to view details.")
            display_df = progs.drop(columns=[c for c in ["PROGRAMID"] if c in progs.columns], errors="ignore")
            st.dataframe(display_df, use_container_width=True, hide_index=True, height=360)
    else:
        program_id = str(program_row.get("PROGRAMID") or "")
        kpi_year = int(st.session_state.get(f"prog_hc_year_{program_id}", date.today().year))
        kpi_cache_key = f"program_overview_kpis_{program_id}_{kpi_year}"
        if kpi_cache_key not in st.session_state:
            totals = {
                "TEAM": 0.0,
                "DELIVERY": 0.0,
                "CONTRACTOR_C": 0.0,
                "CONTRACTOR_CS": 0.0,
                "PROGRAM": 0.0,
            }
            try:
                hc_prog = _list_team_headcount_cached(program_id)
                hc_prog = hc_prog.copy() if hc_prog is not None and not hc_prog.empty else pd.DataFrame()
                if not hc_prog.empty:
                    hc_prog["YEAR_NUM"] = pd.to_numeric(hc_prog.get("YEAR"), errors="coerce").fillna(0).astype(int)
                    hc_prog["PI_NUM"] = pd.to_numeric(hc_prog.get("PI"), errors="coerce").fillna(0).astype(int)
                    hc_prog["CLASS"] = hc_prog.get("CLASS", "").astype(str).str.strip().str.upper()
                    hc_prog["HEADCOUNT"] = pd.to_numeric(hc_prog.get("HEADCOUNT"), errors="coerce").fillna(0.0)
                    totals["PROGRAM"] = float(
                        hc_prog.loc[
                            (hc_prog["YEAR_NUM"] == int(kpi_year))
                            & (hc_prog["PI_NUM"] == 0)
                            & (hc_prog["CLASS"] == "PROGRAM"),
                            "HEADCOUNT",
                        ].sum()
                    )
            except Exception:
                pass
            try:
                df_staff = fetch_df(
                    """
                    SELECT h.CLASS, SUM(h.HEADCOUNT) AS HC
                    FROM TEAM_HEADCOUNT_HISTORY h
                    JOIN TEAMS t ON t.TEAMID = h.TEAMID
                    WHERE t.PROGRAMID=%s AND h.YEAR=%s AND (h.PI = 0 OR h.PI IS NULL) AND h.CLASS IN ('TEAM','DELIVERY')
                    GROUP BY h.CLASS
                    """,
                    (program_id, int(kpi_year)),
                )
                if df_staff is not None and not df_staff.empty:
                    for _, r in df_staff.iterrows():
                        cls = str(r.get("CLASS") or "").strip().upper()
                        totals[cls] = float(pd.to_numeric(r.get("HC"), errors="coerce") or 0.0)
            except Exception:
                pass
            try:
                df_cons = fetch_df(
                    """
                    SELECT c.CLASS, SUM(c.HEADCOUNT) AS HC
                    FROM TEAM_CONTRACTOR_HEADCOUNT c
                    JOIN TEAMS t ON t.TEAMID = c.TEAMID
                    WHERE t.PROGRAMID=%s AND c.YEAR=%s AND (c.PI = 0 OR c.PI IS NULL) AND c.CLASS IN ('CONTRACTOR_C','CONTRACTOR_CS')
                    GROUP BY c.CLASS
                    """,
                    (program_id, int(kpi_year)),
                )
                if df_cons is not None and not df_cons.empty:
                    for _, r in df_cons.iterrows():
                        cls = str(r.get("CLASS") or "").strip().upper()
                        totals[cls] = float(pd.to_numeric(r.get("HC"), errors="coerce") or 0.0)
            except Exception:
                pass
            st.session_state[kpi_cache_key] = totals

        kpi = st.session_state.get(kpi_cache_key, {}) or {}
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Team Overhead FTE", f"{float(kpi.get('TEAM', 0.0)):,.2f}")
        c2.metric("Delivery FTE", f"{float(kpi.get('DELIVERY', 0.0)):,.2f}")
        c3.metric("Contractor C FTE", f"{float(kpi.get('CONTRACTOR_C', 0.0)):,.2f}")
        c4.metric("Contractor CS FTE", f"{float(kpi.get('CONTRACTOR_CS', 0.0)):,.2f}")
        c5.metric("Program Overhead FTE", f"{float(kpi.get('PROGRAM', 0.0)):,.2f}")


if section == "Onboard":
    st.markdown("### Onboard")
    st.caption("Select an ADO Program that is not onboarded yet to create it in NEXT and link it automatically.")

    ado_years = _ado_years_cached()
    years_to_check = tuple(sorted(ado_years)[-3:]) if ado_years else (date.today().year,)
    ado_features_df = _load_ado_features_for_mapping_candidates_cached(years_to_check)
    if ado_features_df is None or not isinstance(ado_features_df, pd.DataFrame) or ado_features_df.empty:
        st.caption("No ADO data available yet. Load ADO features to discover program candidates.")
    else:
        prog_map_df = list_ado_program_mappings()
        unmapped = get_unmapped_ado_programs(ado_features_df, prog_map_df)
        if unmapped is None or unmapped.empty:
            st.caption("All ADO programs detected in the selected years are already onboarded.")
        else:
            view_df = unmapped.copy()
            view_df["__pick"] = False
            view_df["MAPPED_TO"] = ""
            rename = {
                "__pick": "Select",
                "ADO_PROGRAM": "ADO Program",
                "FEATURE_COUNT": "Feature Count",
                "STORY_POINTS_SUM": "Story Points",
                "LAST_SEEN": "Last Seen",
                "MAPPED_TO": "Mapped To",
            }
            show_cols = ["__pick", "ADO_PROGRAM", "FEATURE_COUNT", "STORY_POINTS_SUM", "LAST_SEEN", "MAPPED_TO"]
            editor_view = view_df[show_cols].rename(columns=rename)
            pick = st.data_editor(
                editor_view,
                use_container_width=True,
                hide_index=True,
                height=260,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select"),
                },
                disabled=[c for c in editor_view.columns if c != "Select"],
                key="prog_onboard_pick",
            )
            selected_rows = pick[pick["Select"]].index.tolist() if "Select" in pick.columns else []
            selected_ado = str(pick.loc[selected_rows[-1], "ADO Program"]).strip() if selected_rows else ""

            if selected_ado:
                st.markdown("### Create Program from selected ADO program")
                name_val = st.text_input("Program Name", value=selected_ado, key="prog_onboard_name")
                display_val = st.text_input("Program Display Name (optional)", value="", key="prog_onboard_display")
                if st.button("Create & map", icon=":material/add:", key="prog_onboard_create_map"):
                    final_name = (display_val or name_val or "").strip()
                    if not final_name:
                        st.error("Program Name is required.")
                        st.stop()
                    if _program_id_for_name_ci(final_name):
                        st.error(f"A Program named '{final_name}' already exists.")
                        st.stop()
                    try:
                        program_id = create_program_from_ado(selected_ado, name=final_name, updated_by=user_display)
                        map_ado_program_to_program([selected_ado], program_id)
                        st.session_state["programs_pick_pending"] = final_name
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        toast_error(f"Create failed: {e}")
            else:
                st.markdown("### Create program manually")
                manual_name = st.text_input("Program Name", value="", key="prog_manual_name")
                if st.button("Create program", icon=":material/add:", key="prog_manual_create"):
                    final_name = (manual_name or "").strip()
                    if not final_name:
                        st.error("Program Name is required.")
                        st.stop()
                    if _program_id_for_name_ci(final_name):
                        st.error(f"A Program named '{final_name}' already exists.")
                        st.stop()
                    try:
                        program_id = create_program_from_ado(final_name, name=final_name, updated_by=user_display)
                        st.session_state["programs_pick_pending"] = final_name
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        toast_error(f"Create failed: {e}")

        with st.expander("Debug – ADO Program candidates", expanded=False):
            st.write(
                {
                    "columns": list(ado_features_df.columns),
                    "unmapped_rows": int(len(unmapped)) if unmapped is not None else 0,
                }
            )

if section == "Headcount & Rates":
    st.markdown("### Headcount & Rates")
    st.caption("Edit headcount below. Click Save to recompute composition automatically.")

    if program_row is None:
        st.info("Select a program above to manage headcount, composition, and mappings.")
    else:
        st.markdown(f"### Editing program: {program_row['PROGRAMNAME']}")
        edit_program_id = str(program_row.get("PROGRAMID") or "")

        with st.container(border=True):
            st.markdown("##### Program details")
            prefill_name = str(program_row.get("PROGRAMNAME") or "")
            prefill_owner = str(program_row.get("PROGRAMOWNER") or "")

            program_name = st.text_input("Program Name (required)", value=prefill_name, key="prog_name")
            labels, label_to_email = _users_for_selectbox()
            def_label = None
            if prefill_owner:
                for lbl, em in label_to_email.items():
                    if em.lower() == str(prefill_owner).lower():
                        def_label = lbl
                        break
                if def_label is None:
                    def_label = str(prefill_owner)
                    labels = [def_label] + labels
                    label_to_email[def_label] = str(prefill_owner)
            sel_lbl = st.selectbox(
                "Program Manager (required)",
                options=[""] + labels,
                index=(labels.index(def_label) + 1 if def_label in labels else 0),
                key="prog_mgr",
            )
            program_manager = label_to_email.get(sel_lbl, "") if sel_lbl else ""

            if st.button("Save Program", icon=":material/save:", type="primary", key="prog_save_btn"):
                name = (program_name or "").strip()
                mgr = (program_manager or "").strip()
                if not name:
                    st.error("Program Name is required.")
                elif not mgr:
                    st.error("Program Manager is required.")
                else:
                    existing_id = _program_id_for_name_ci(name)
                    if existing_id and existing_id != edit_program_id:
                        st.error(f"A Program named '{name}' already exists. Program names must be unique.")
                    else:
                        try:
                            upsert_program(
                                program_id=edit_program_id,
                                name=name,
                                owner=mgr,
                                fte=None,
                                program_rate=None,
                                updated_by=user_display,
                            )
                            st.session_state["programs_pick_pending"] = name
                            toast_success(f"Program saved: {name}.")
                            _run_post_save_refresh("program_save")
                        except Exception as e:
                            toast_error(f"Save failed: {e}")

        with st.expander("Link ADO Programs", expanded=False):
            st.caption("Connect raw ADO program values to this NEXT Program.")
            render_ado_mapping_panel(
                "program",
                edit_program_id,
                context_filters={"updated_by": user_display},
                user_is_admin=is_admin,
                allow_create_toggle=True,
            )

        render_program_headcount_editor(
            edit_program_id=edit_program_id,
            program_pick=str(program_pick or ""),
            user_display=user_display,
        )

        st.divider()
        st.markdown("##### Additional Costs (program-level NWF)")
        st.caption("Program additional costs are treated as non-people program overhead and are not allocated to app groups.")

        ALLOWED_ADDL_NWF_LABELS = ["Cloud AWS", "Cloud Azure", "Infra", "Travel", "NWF Other"]

        def _pair_to_addl_nwf_label(cost_type: Any, subtype: Any) -> str:
            ct = str(cost_type or "").strip()
            stp = str(subtype or "").strip()
            if ct == "Cloud" and stp == "AWS":
                return "Cloud AWS"
            if ct == "Cloud" and stp == "Azure":
                return "Cloud Azure"
            if ct == "Infra":
                return "Infra"
            if ct == "Travel":
                return "Travel"
            if ct == "NWF" and stp == "Other":
                return "NWF Other"
            return ""

        def _addl_nwf_label_to_pair(label: Any) -> tuple[str, str]:
            lab = str(label or "").strip()
            if lab == "Cloud AWS":
                return ("Cloud", "AWS")
            if lab == "Cloud Azure":
                return ("Cloud", "Azure")
            if lab == "Infra":
                return ("Infra", "")
            if lab == "Travel":
                return ("Travel", "")
            # Default/fallback
            return ("NWF", "Other")

        add_year = st.number_input("Year (required)", min_value=2020, max_value=2100, step=1, value=date.today().year, key="pac_year")
        try:
            add_hist_raw = list_program_additional_costs(edit_program_id)
        except Exception as e:
            add_hist_raw = pd.DataFrame()
            st.warning(f"Could not load additional costs: {e}")

        add_hist = add_hist_raw.copy() if add_hist_raw is not None and not add_hist_raw.empty else pd.DataFrame()
        if not add_hist.empty:
            add_hist["YEAR"] = pd.to_numeric(add_hist.get("YEAR"), errors="coerce").astype("Int64")
            add_hist["MONTH"] = pd.to_numeric(add_hist.get("MONTH"), errors="coerce").astype("Int64")
            add_hist["AMOUNT"] = pd.to_numeric(add_hist.get("AMOUNT"), errors="coerce").fillna(0.0)
            add_hist["COST_TYPE"] = add_hist.get("COST_TYPE", "").astype(str).str.strip()
            add_hist["SUBTYPE"] = add_hist.get("SUBTYPE", "").astype(str).str.strip()
            add_hist["DESCRIPTION"] = add_hist.get("DESCRIPTION", "").astype(str).str.strip()
            add_hist["CURRENCY"] = add_hist.get("CURRENCY", "").astype(str).str.strip()
            add_hist["IS_RECURRING"] = add_hist.get("IS_RECURRING", True)

        year_rows = add_hist[add_hist["YEAR"] == int(add_year)].copy() if (not add_hist.empty and "YEAR" in add_hist.columns) else pd.DataFrame()
        if not year_rows.empty:
            year_rows["ADDL_NWF_TYPE_LABEL"] = year_rows.apply(
                lambda r: _pair_to_addl_nwf_label(r.get("COST_TYPE"), r.get("SUBTYPE")), axis=1
            )
            year_rows["LEGACY_INVALID"] = year_rows["ADDL_NWF_TYPE_LABEL"].eq("").astype(int)
            if int(year_rows["LEGACY_INVALID"].sum() or 0) > 0:
                year_rows.loc[year_rows["ADDL_NWF_TYPE_LABEL"].eq(""), "ADDL_NWF_TYPE_LABEL"] = "NWF Other"
                st.warning(
                    f"{int(year_rows['LEGACY_INVALID'].sum())} row(s) had invalid categories and were defaulted to 'NWF Other'; please review before saving."
                )
            annual = (
                year_rows.groupby(["ADDL_NWF_TYPE_LABEL", "DESCRIPTION", "CURRENCY", "IS_RECURRING", "LEGACY_INVALID"], dropna=False)["AMOUNT"]
                .sum()
                .reset_index()
                .rename(columns={"AMOUNT": "ANNUAL_AMOUNT"})
            )
        else:
            annual = pd.DataFrame(columns=["ADDL_NWF_TYPE_LABEL", "DESCRIPTION", "CURRENCY", "IS_RECURRING", "LEGACY_INVALID", "ANNUAL_AMOUNT"])

        with st.form("pac_editor_form", clear_on_submit=False):
            editor = st.data_editor(
                annual,
                hide_index=True,
                num_rows="dynamic",
                column_config={
                    "ADDL_NWF_TYPE_LABEL": st.column_config.SelectboxColumn(
                        "Additional Cost Type (Program NWF)",
                        options=ALLOWED_ADDL_NWF_LABELS,
                        required=True,
                        help="Allowed planned program-level NWF types only (MSP/Invoices/Contractor CS are planned elsewhere).",
                    ),
                    "DESCRIPTION": st.column_config.TextColumn("Description (optional)", required=False),
                    "CURRENCY": st.column_config.TextColumn("Currency", required=False),
                    "IS_RECURRING": st.column_config.CheckboxColumn("Recurring", help="True = recurring monthly cost."),
                    "LEGACY_INVALID": st.column_config.NumberColumn("Legacy invalid", disabled=True, help="1 = legacy row was not in the allowed set and was defaulted."),
                    "ANNUAL_AMOUNT": st.column_config.NumberColumn("Annual Amount", min_value=0.0, step=100.0, required=True),
                },
                key="pac_editor",
            )
            save_pac = st.form_submit_button("Save changes", icon=":material/save:", type="primary")
        if save_pac:
            try:
                execute("DELETE FROM PROGRAM_ADDITIONAL_COSTS WHERE PROGRAMID=%s AND YEAR=%s", (edit_program_id, int(add_year)))
                updated = 0
                if editor is not None and not editor.empty:
                    bad_labels = sorted({str(v).strip() for v in editor.get("ADDL_NWF_TYPE_LABEL", []).tolist() if str(v).strip() and str(v).strip() not in ALLOWED_ADDL_NWF_LABELS})
                    if bad_labels:
                        st.error(
                            "Invalid Additional Cost Type. Choose one of: "
                            + ", ".join(ALLOWED_ADDL_NWF_LABELS)
                            + f". Invalid: {', '.join(bad_labels)}"
                        )
                        raise ValueError("Invalid Additional Cost Type label(s).")
                    for _, rr in editor.iterrows():
                        label = str(rr.get("ADDL_NWF_TYPE_LABEL") or "").strip()
                        if not label:
                            continue
                        if label not in ALLOWED_ADDL_NWF_LABELS:
                            raise ValueError(f"Invalid Additional Cost Type label: {label}")
                        cost_type, subtype = _addl_nwf_label_to_pair(label)
                        desc = str(rr.get("DESCRIPTION") or "").strip()
                        currency = str(rr.get("CURRENCY") or "").strip() or "USD"
                        is_rec = bool(rr.get("IS_RECURRING", True))
                        amt = float(rr.get("ANNUAL_AMOUNT") or 0.0)
                        if amt <= 0:
                            continue
                        monthly = float(amt) / 12.0
                        for month in range(1, 13):
                            upsert_program_additional_cost(edit_program_id, int(add_year), int(month), cost_type, subtype, desc, currency, is_rec, float(monthly), updated_by=user_display)
                            updated += 1
                toast_success(f"Saved {updated} monthly row(s).")
                _run_post_save_refresh("program_additional_costs_save")
            except Exception as e:
                toast_error(f"Save failed: {e}")

        try:
            add_hist2 = list_program_additional_costs(edit_program_id)
        except Exception:
            add_hist2 = pd.DataFrame()
        add_hist2 = add_hist2.copy() if add_hist2 is not None and not add_hist2.empty else pd.DataFrame()
        if not add_hist2.empty:
            add_hist2["YEAR"] = pd.to_numeric(add_hist2.get("YEAR"), errors="coerce").astype("Int64")
            add_hist2["AMOUNT"] = pd.to_numeric(add_hist2.get("AMOUNT"), errors="coerce").fillna(0.0)
            tot = float(add_hist2.loc[add_hist2["YEAR"] == int(add_year), "AMOUNT"].sum() or 0.0)
            st.metric("Total annual additional costs", f"${tot:,.0f}")
            show_monthly = st.checkbox("Show monthly rows (debug)", value=False, key="pac_show_monthly_debug")
            if show_monthly:
                show_cols = [c for c in ["YEAR", "MONTH", "COST_TYPE", "SUBTYPE", "DESCRIPTION", "CURRENCY", "IS_RECURRING", "AMOUNT"] if c in add_hist2.columns]
                st.dataframe(add_hist2.sort_values(by=["YEAR", "MONTH", "COST_TYPE", "SUBTYPE"], ascending=[False, True, True, True])[show_cols], use_container_width=True, height=280)
        else:
            st.info("No additional costs yet for this program.")

        st.divider()
        st.markdown("##### Program events (manual)")
        st.caption("Cost events are disabled for now (moving to a single working plan model with per-PI notes on headcount rows).")

        st.markdown("##### Apptio Work IDs (map to Program)")
        st.caption("These mappings drive the Apptio actuals import.")
        map_df = list_program_apptio_workids()
        prog_map = map_df[map_df["PROGRAMID"] == edit_program_id] if map_df is not None else pd.DataFrame()

        def _norm_work_id(val) -> str:
            s = str(val or "").strip()
            s = re.sub(r"[^0-9A-Za-z]", "", s)
            s = s.lstrip("0")
            return s if s else "0"

        new_work_id_raw = st.text_input("Work ID", key="apptio_work_id_input")
        new_work_id = _norm_work_id(new_work_id_raw)
        if st.button("Add Work ID", key="apptio_work_id_add", type="primary"):
            try:
                upsert_program_apptio_workid(edit_program_id, new_work_id, updated_by=user_display)
                toast_success("Work ID saved.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                toast_error(f"Could not save Work ID: {e}")
        if prog_map is not None and not prog_map.empty:
            st.dataframe(prog_map[["WORK_ID", "UPDATED_AT", "UPDATED_BY"]].sort_values("WORK_ID"), use_container_width=True, height=220)
            del_choice = st.selectbox("Remove Work ID", [""] + prog_map["WORK_ID"].tolist(), key="apptio_work_id_del")
            if del_choice and st.button("Delete mapping", icon=":material/delete:", key="apptio_work_id_del_btn"):
                try:
                    delete_program_apptio_workid(edit_program_id, _norm_work_id(del_choice))
                    toast_success("Work ID removed.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    toast_error(f"Delete failed: {e}")
        else:
            st.info("No Work IDs mapped to this program yet.")

        with st.container(border=True):
            st.markdown("##### Danger zone")
            deps = get_program_dependencies(edit_program_id)
            render_dependency_summary(deps)
            confirm = st.text_input("Type DELETE to confirm", key="prog_delete_confirm", disabled=not can_delete(deps))
            if st.button(
                "Delete Program",
                icon=":material/delete:",
                key="prog_delete_btn",
                type="secondary",
                disabled=not can_delete(deps),
            ):
                if confirm.strip().upper() != "DELETE":
                    st.error("Type DELETE to confirm.")
                else:
                    result = safe_delete_program(edit_program_id, source="pages/3_Programs.py")
                    if result.get("ok"):
                        st.success("Program deleted.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(result.get("message") or "Delete failed.")


if section == "Audit":
    st.markdown("### History / Audit")
    st.caption("Audit trail for composition changes derived from headcount saves.")
    if program_row is None:
        st.info("Select a program to view audit history.")
    else:
        program_id = str(program_row.get("PROGRAMID") or "")
        try:
            log_df = fetch_df(
                """
                SELECT TOP 200
                  CHANGED_AT, CHANGED_BY, YEAR, PI,
                  OLD_PROGRAMFTE, NEW_PROGRAMFTE,
                  SOURCE
                FROM TCO_PROGRAM_COMPOSITION_CHANGELOG
                WHERE PROGRAMID = %s
                ORDER BY CHANGED_AT DESC
                """,
                (program_id,),
            )
            if log_df is None or log_df.empty:
                st.info("No changes yet.")
            else:
                st.dataframe(log_df, use_container_width=True, hide_index=True, height=420)
        except Exception as e:
            st.warning(f"Could not load audit log: {e}")
