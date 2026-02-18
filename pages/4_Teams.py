# Summary: update mapping UI labels to NEXT branding.
# pages/Teams.py
import streamlit as st
import pandas as pd
import uuid
from datetime import date
from typing import Optional
from core.init import init_page

from core.data import fetch_team_allocated_headcount_by_pi
from core.ado_recon import load_derived_fte_by_bucket
from core.ado_candidates import get_unmapped_ado_apps_for_team
from core.app_instances import ensure_default_instances
from core.debug import is_debug_enabled, _is_admin
from core.ado_mapping_service import (
    create_team,
    create_team_from_ado,
    get_unmapped_ado_apps,
    get_unmapped_ado_teams,
    list_ado_app_mappings,
    list_ado_program_mappings,
    list_ado_team_mappings,
    map_ado_app_to_app_group,
    map_ado_team_to_team,
    create_app_group_from_ado,
)
from core.next_app_classification import (
    build_next_app_candidates_df,
    safe_onboard_next_apps,
)
from core.cache_utils import cache_data_portfolio, cache_resource_portfolio
from core.freshness import post_write_refresh
from core.authorization import (
    can,
    get_current_principal,
    get_effective_scope,
)
from db import (
    ensure_tables,
    fetch_df,
    execute,
    list_app_users,
    upsert_vendor,
    upsert_application_group,
    ensure_location_and_contractor_tables,
    upsert_team_headcount,
    list_team_headcount,
    list_team_contractor_headcount,
    upsert_team_contractor_headcount,
    list_contractor_companies,
    ensure_composition_changelog_tables,
    recompute_team_composition_from_headcount_year,
)
from utils.admin_delete_guard import (
    get_team_dependencies,
    render_dependency_summary,
    can_delete,
    safe_delete_team,
)
from utils.ado_mapping_ui import render_ado_mapping_panel
from utils.ui_patterns import render_scope_banner, render_section_picker, render_page_frame

from utils.app_shell import bootstrap_page
bootstrap_page()
try:
    from db.tco_events import record_cost_events_from_headcount_delta
except Exception:
    record_cost_events_from_headcount_delta = None  # type: ignore[assignment]

# Events are currently disabled for normal planning edits.
# We keep the table/helpers for future explicit change logging, but do not write automatically.
ENABLE_COST_EVENTS = False

page_theme = init_page("Teams", page_path=__file__)
_page_theme = page_theme
user = st.session_state.get("auth_user") or {}
is_admin = _is_admin()
principal = get_current_principal()
effective_scope = get_effective_scope(fetch_df, principal)
IS_CONTRIBUTOR = bool(principal.is_contributor)
IS_SCOPED_CONTRIBUTOR = bool(principal.is_contributor and effective_scope.has_scope)
ALLOWED_PROGRAM_IDS = set(effective_scope.program_ids or set())
ALLOWED_TEAM_IDS = set(effective_scope.team_ids or set())


def _can_edit_program(program_id: str) -> bool:
    pid = str(program_id or "").strip()
    return can(principal, "edit", "program", resource_id=pid, scope=effective_scope)


def _can_edit_team(team_id: str) -> bool:
    tid = str(team_id or "").strip()
    return can(principal, "edit", "team", resource_id=tid, scope=effective_scope)


def _deny_scope(action: str, subject: str) -> None:
    st.error(f"You do not have permission to {action} for this {subject}.")


@cache_resource_portfolio(show_spinner=False)
def _ensure_teams_page_schema() -> None:
    ensure_tables()
    ensure_location_and_contractor_tables()
    try:
        ensure_composition_changelog_tables()
    except Exception:
        pass


_ensure_teams_page_schema()

def _display_name_for(email: str) -> str:
    em = (email or "").strip()
    if not em:
        return "Unknown"
    try:
        df = fetch_df("SELECT TOP 1 DISPLAY_NAME FROM APP_USERS WHERE UPPER(EMAIL)=UPPER(%s)", (em,))
        if df is not None and not df.empty:
            dn = str(df.iloc[0].get("DISPLAY_NAME") or "").strip()
            if dn:
                return dn
    except Exception:
        pass
    return em

_user_display = _display_name_for(user.get("email"))

show_debug = is_debug_enabled(label="Debug")


def _run_post_save_refresh(context: str, *, bump_version: Optional[bool] = True) -> None:
    post_write_refresh(context, ensure_views=False, rerun=True, bump_version=bump_version)


@cache_data_portfolio(ttl=120, show_spinner=False)
def _list_app_users_cached() -> pd.DataFrame:
    try:
        tmp = list_app_users()
        return tmp if isinstance(tmp, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=60, show_spinner=False)
def _list_team_headcount_cached(team_id: str) -> pd.DataFrame:
    try:
        df = list_team_headcount(team_id)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=60, show_spinner=False)
def _list_team_contractor_headcount_cached(team_id: str) -> pd.DataFrame:
    try:
        df = list_team_contractor_headcount(team_id)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=300, show_spinner=False)
def _list_contractor_companies_cached() -> pd.DataFrame:
    try:
        df = list_contractor_companies()
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=180, show_spinner=False)
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

@cache_data_portfolio(ttl=180, show_spinner=False)
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

def _team_id_for_name_ci(name: str) -> Optional[str]:
    """Return TEAMID for a given name (case-insensitive), else None."""
    try:
        df = fetch_df("SELECT TOP 1 TEAMID FROM TEAMS WHERE UPPER(TEAMNAME)=UPPER(%s)", (name,))
        if df is not None and not df.empty:
            return str(df.iloc[0].get("TEAMID") or "") or None
    except Exception:
        pass
    return None

def _composition_for_year(teamid: str, year: int) -> dict:
    """Return composition row for given team/year (PI 0 preferred), else latest effective."""
    try:
        df = fetch_df(
            """
            SELECT TOP 1 TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE
            FROM TEAM_COMPOSITION_HISTORY
            WHERE TEAMID=%s AND YEAR=%s AND (PI = 0 OR PI IS NULL)
            ORDER BY UPDATED_AT DESC
            """,
            (teamid, year),
        )
        if df is not None and not df.empty:
            return {k: float(df.iloc[0].get(k) or 0.0) for k in df.columns}
    except Exception:
        pass
    eff = _effective_composition(teamid)
    return {
        "TEAMFTE": float(eff.get("TEAMFTE", 0.0)),
        "DELIVERY_TEAM_FTE": float(eff.get("DELIVERY_TEAM_FTE", 0.0)),
        "CONTRACTOR_CS_FTE": float(eff.get("CONTRACTOR_CS_FTE", 0.0)),
        "CONTRACTOR_C_FTE": float(eff.get("CONTRACTOR_C_FTE", 0.0)),
    } if eff else {"TEAMFTE":0.0,"DELIVERY_TEAM_FTE":0.0,"CONTRACTOR_CS_FTE":0.0,"CONTRACTOR_C_FTE":0.0}

# ---- Local helper: ensure TEAM_MSP_RATE exists (no import needed) ----
def _ensure_team_msp_rate_table():
    execute("""
        CREATE TABLE IF NOT EXISTS TEAM_MSP_RATE (
          TEAMID STRING NOT NULL,
          MSP_ENABLED BOOLEAN DEFAULT FALSE,
          MSP_SIZE STRING,
          MSP_RATE_PER_PI NUMBER(18,2),
          UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
          CONSTRAINT PK_TEAM_MSP_RATE PRIMARY KEY (TEAMID)
        )
    """)
    # Backfill columns in case the table was created earlier with a minimal schema
    execute("ALTER TABLE TEAM_MSP_RATE ADD COLUMN IF NOT EXISTS MSP_ENABLED BOOLEAN DEFAULT FALSE")
    execute("ALTER TABLE TEAM_MSP_RATE ADD COLUMN IF NOT EXISTS MSP_SIZE STRING")
    execute("ALTER TABLE TEAM_MSP_RATE ADD COLUMN IF NOT EXISTS MSP_RATE_PER_PI NUMBER(18,2)")
    # Best-effort FK – ignore if it already exists / mismatched types
    try:
        execute("""
            ALTER TABLE TEAM_MSP_RATE
            ADD CONSTRAINT FK_TEAM_MSP_RATE_TEAM
            FOREIGN KEY (TEAMID) REFERENCES TEAMS(TEAMID)
        """)
    except Exception:
        pass

# ---- Local helper: ensure TEAM_COMPOSITION_HISTORY exists (non-MSP FTE history) ----
def _ensure_team_composition_history():
    execute("""
      CREATE TABLE IF NOT EXISTS TEAM_COMPOSITION_HISTORY (
        TEAMID STRING NOT NULL,
        YEAR   NUMBER(4,0) NOT NULL,
        PI     NUMBER(1,0) DEFAULT 0,
        TEAMFTE FLOAT DEFAULT 0,
        DELIVERY_TEAM_FTE FLOAT DEFAULT 0,
        CONTRACTOR_CS_FTE FLOAT DEFAULT 0,
        CONTRACTOR_C_FTE  FLOAT DEFAULT 0,
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        UPDATED_BY STRING,
        CONSTRAINT PK_TEAM_COMPOSITION_HISTORY PRIMARY KEY (TEAMID, YEAR, PI)
      )
    """)
    # Align with MSSQL backend: All PIs is stored as 0 (not NULL)
    try:
        execute("UPDATE TEAM_COMPOSITION_HISTORY SET PI = 0 WHERE PI IS NULL")
        execute("ALTER TABLE TEAM_COMPOSITION_HISTORY ALTER COLUMN PI NUMBER(1,0) NOT NULL")
    except Exception:
        pass
    # Best effort: add UPDATED_BY if missing
    try:
        execute("ALTER TABLE TEAM_COMPOSITION_HISTORY ADD COLUMN IF NOT EXISTS UPDATED_BY STRING")
    except Exception:
        pass

# ---- Local helper: upsert MSP flag/rate/size (preserve existing when None) ----
def _upsert_team_msp_rate(team_id: str, msp_enabled: bool,
                          msp_size: Optional[str] = None,
                          msp_rate_per_pi: Optional[float] = None):
    _ensure_team_msp_rate_table()

    # Preserve existing if new values are None
    cur = fetch_df("SELECT MSP_SIZE, MSP_RATE_PER_PI FROM TEAM_MSP_RATE WHERE TEAMID=%s", (team_id,))
    if cur is not None and not cur.empty:
        if msp_size is None:
            msp_size = cur.iloc[0].get("MSP_SIZE")
        if msp_rate_per_pi is None:
            msp_rate_per_pi = cur.iloc[0].get("MSP_RATE_PER_PI")

    execute(
        """
        MERGE INTO TEAM_MSP_RATE t
        USING (SELECT %s TEAMID, %s MSP_ENABLED, %s MSP_SIZE, %s MSP_RATE_PER_PI, %s UPDATED_BY) s
        ON t.TEAMID = s.TEAMID
        WHEN MATCHED THEN UPDATE SET
          MSP_ENABLED = s.MSP_ENABLED,
          MSP_SIZE = s.MSP_SIZE,
          MSP_RATE_PER_PI = s.MSP_RATE_PER_PI,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (TEAMID, MSP_ENABLED, MSP_SIZE, MSP_RATE_PER_PI, UPDATED_BY)
        VALUES (s.TEAMID, s.MSP_ENABLED, s.MSP_SIZE, s.MSP_RATE_PER_PI, s.UPDATED_BY);
        """,
        (team_id, bool(msp_enabled), msp_size, msp_rate_per_pi, str(user.get("email") or "")),
    )

_ensure_team_msp_rate_table()  # make sure MSP settings table exists
_ensure_team_composition_history()  # ensure non-MSP composition history table exists


def _effective_composition(team_id: str):
    """Return the latest effective composition for a team.

    Selection logic:
    - Prefer the most recently updated row for the current calendar year (any PI).
    - If no rows exist for the current year, fall back to the most recent row overall.
    """
    today_year = date.today().year
    try:
        df = fetch_df(
            """
            SELECT TEAMID, YEAR, PI, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE, UPDATED_AT
            FROM TEAM_COMPOSITION_HISTORY
            WHERE TEAMID=%s
            ORDER BY YEAR DESC, COALESCE(PI, -1) DESC, UPDATED_AT DESC
            """,
            (team_id,),
        )
        if df is None or df.empty:
            return {}

        df = df.copy()
        df["YEAR"] = pd.to_numeric(df.get("YEAR"), errors="coerce").astype("Int64")
        df["PI"] = pd.to_numeric(df.get("PI"), errors="coerce").fillna(0).astype(int)
        for col in ["TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0.0)

        if "UPDATED_AT" in df.columns:
            df["UPDATED_AT"] = pd.to_datetime(df.get("UPDATED_AT"), errors="coerce")

        # Prefer the most recent row for the current year (any PI).
        current_year_rows = df[df["YEAR"] == int(today_year)].copy()
        if not current_year_rows.empty:
            sort_cols = ["PI"]
            sort_asc = [False]
            if "UPDATED_AT" in current_year_rows.columns:
                sort_cols = ["UPDATED_AT", "PI"]
                sort_asc = [False, False]
            current_year_rows = current_year_rows.sort_values(sort_cols, ascending=sort_asc, na_position="last")
            r = current_year_rows.iloc[0]
        else:
            # Fallback: latest row overall, ordered by YEAR and UPDATED_AT.
            tmp = df.copy()
            tmp["YEAR_SORT"] = pd.to_numeric(tmp.get("YEAR"), errors="coerce").fillna(-1).astype(int)
            sort_cols = ["YEAR_SORT", "PI"]
            sort_asc = [False, False]
            if "UPDATED_AT" in tmp.columns:
                sort_cols = ["YEAR_SORT", "UPDATED_AT", "PI"]
                sort_asc = [False, False, False]
            tmp = tmp.sort_values(sort_cols, ascending=sort_asc, na_position="last")
            r = tmp.iloc[0]
        return {
            "TEAMFTE": float(r.get("TEAMFTE") or 0.0),
            "DELIVERY_TEAM_FTE": float(r.get("DELIVERY_TEAM_FTE") or 0.0),
            "CONTRACTOR_CS_FTE": float(r.get("CONTRACTOR_CS_FTE") or 0.0),
            "CONTRACTOR_C_FTE": float(r.get("CONTRACTOR_C_FTE") or 0.0),
            "YEAR": r.get("YEAR"),
            "PI": r.get("PI"),
            "UPDATED_AT": r.get("UPDATED_AT"),
        }
    except Exception:
        return {}


def _sync_team_baseline_from_history(team_id: str):
    """Keep TEAMS baseline in sync with latest effective composition so existing dashboards stay consistent."""
    latest = _effective_composition(team_id)
    if not latest:
        return
    try:
        execute(
            """
            UPDATE TEAMS
            SET TEAMFTE=%s,
                DELIVERY_TEAM_FTE=%s,
                CONTRACTOR_CS_FTE=%s,
                CONTRACTOR_C_FTE=%s,
                UPDATED_AT=SYSDATETIME()
            WHERE TEAMID=%s
            """,
            (
                latest.get("TEAMFTE", 0.0),
                latest.get("DELIVERY_TEAM_FTE", 0.0),
                latest.get("CONTRACTOR_CS_FTE", 0.0),
                latest.get("CONTRACTOR_C_FTE", 0.0),
                team_id,
            ),
        )
    except Exception:
        pass

# --- Ensure columns exist (idempotent) ---
execute("ALTER TABLE TEAMS ADD COLUMN IF NOT EXISTS DELIVERY_TEAM_FTE FLOAT")
execute("ALTER TABLE TEAMS ADD COLUMN IF NOT EXISTS CONTRACTOR_C_FTE FLOAT")
execute("ALTER TABLE TEAMS ADD COLUMN IF NOT EXISTS CONTRACTOR_CS_FTE FLOAT")
execute("ALTER TABLE TEAMS ADD COLUMN IF NOT EXISTS PRODUCTOWNER STRING")

@cache_data_portfolio(ttl=180, show_spinner=False)
def _programs_df():
    return fetch_df(
        """
        SELECT
            PROGRAMID,
            PROGRAMNAME AS PROGRAMNAME_RAW,
            COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME) AS PROGRAMNAME
        FROM PROGRAMS
        ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME)
        """
    )

@cache_data_portfolio(ttl=60, show_spinner=False)
def _teams_df():
    return fetch_df(
        """
        SELECT TEAMID,
               TEAMNAME AS TEAMNAME_RAW,
               COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME) AS TEAMNAME,
               PROGRAMID,
               TEAMFTE,
               DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE,
               PRODUCTOWNER, UPDATED_AT, UPDATED_BY
        FROM TEAMS
        ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME)
        """
    )

@cache_data_portfolio(ttl=180, show_spinner=False)
def _team_required_fte_by_pi(team_id: str, year: int) -> pd.DataFrame:
    """Return Derived FTE (SWAG) demand by PI for this team."""
    try:
        dft = fetch_df("SELECT TEAMNAME FROM TEAMS WHERE TEAMID = %s", (team_id,))
        team_name = str(dft.iloc[0].get("TEAMNAME") or "").strip() if dft is not None and not dft.empty else ""
        if not team_name:
            return pd.DataFrame(columns=["ITERATION_NUM", "DERIVED_FTE"])

        buckets = load_derived_fte_by_bucket(years=[int(year)], teams=[team_name])
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

@cache_data_portfolio(ttl=180, show_spinner=False)
def _team_allocated_headcount_by_pi(team_id: str, year: int) -> pd.DataFrame:
    return fetch_team_allocated_headcount_by_pi(team_id, int(year))

programs = _programs_df()
if not isinstance(programs, pd.DataFrame):
    programs = pd.DataFrame()
st.session_state.setdefault("teams_op_feedback", None)


def _set_op_feedback(level: str, message: str, target: str = "team_edit") -> None:
    st.session_state["teams_op_feedback"] = {
        "level": str(level or "info"),
        "message": str(message or ""),
        "target": str(target or "team_edit"),
    }


def _render_op_feedback(target: str) -> None:
    payload = st.session_state.pop("teams_op_feedback", None)
    if not isinstance(payload, dict):
        return
    payload_target = str(payload.get("target") or "team_edit")
    if payload_target != str(target):
        st.session_state["teams_op_feedback"] = payload
        return
    lvl = str(payload.get("level") or "info").lower()
    msg = str(payload.get("message") or "").strip()
    if not msg:
        return
    if lvl == "success":
        st.success(msg)
    elif lvl == "warning":
        st.warning(msg)
    elif lvl == "error":
        st.error(msg)
    else:
        st.info(msg)

render_page_frame(
    title="Teams",
    purpose="Maintain mappings, rates, and reference data",
    status_line="Headcount edits write to baseline history tables and recompute composition.",
)

section_options = ["Overview"]
section_options.append("App Classification")
if is_admin or IS_SCOPED_CONTRIBUTOR:
    section_options.append("Onboard")
if is_admin or IS_SCOPED_CONTRIBUTOR:
    section_options.append("Headcount & Rates")
if is_admin:
    section_options.append("Audit")
section = render_section_picker(
    key="teams_section",
    options=section_options,
    label="Section",
    default=section_options[0],
)
render_scope_banner(
    principal=principal,
    scope=effective_scope,
    entity_label="Programs and Teams",
)


def _msp_row_for_team(team_id: str):
    df = fetch_df("SELECT MSP_ENABLED, MSP_RATE_PER_PI, MSP_SIZE FROM TEAM_MSP_RATE WHERE TEAMID=%s", (team_id,))
    if df is None or df.empty:
        return False, None, None
    r = df.iloc[0]
    return bool(r.get("MSP_ENABLED") or False), r.get("MSP_RATE_PER_PI"), r.get("MSP_SIZE")


def _team_fragment(fn):
    frag = getattr(st, "fragment", None)
    return frag(fn) if callable(frag) else fn


@_team_fragment
def render_team_headcount_editor(
    *,
    team_id: str,
    year_i: int,
    staff_fixed_cols: list[str],
    initial_df: pd.DataFrame,
    staff_has_overrides: bool,
    selected_pis: list[int],
    location_options: list[str],
    staff_notes_enabled: bool,
) -> tuple[str, pd.DataFrame]:
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
                df[col] = "" if col in ["CLASS", "LOCATION_KEY", "__ORIG_CLASS", "__ORIG_LOCATION_KEY"] or col.startswith("NOTE_") else 0.0
        return df.reindex(columns=cols)
    # -------------------------
    # Section 1: Team + Delivery
    # -------------------------
    st.markdown("###### Team & Delivery (by Location)")
    staff_mode = st.radio(
        "Team & Delivery input mode",
        options=["All PIs same", "Per PI"],
        index=1 if staff_has_overrides else 0,
        horizontal=True,
        key=f"teams_staff_mode_{team_id}_{year_i}",
    )
    visible_pi_cols = [f"HC_I{pi}" for pi in selected_pis]

    editor_key = f"hc_editor_team_delivery_{team_id}_{year_i}_v2"
    widget_key = editor_key
    df_key = f"{editor_key}__df"
    ctx_key = f"{editor_key}__ctx"
    ctx_val = (team_id, int(year_i))
    if st.session_state.get(ctx_key) != ctx_val:
        # Widget key stores Streamlit's dict state; df_key stores the dataframe.
        st.session_state[df_key] = initial_df.copy()
        st.session_state.pop(widget_key, None)
        st.session_state[ctx_key] = ctx_val

    current_df = st.session_state.get(df_key)
    if not isinstance(current_df, pd.DataFrame):
        current_df = initial_df.copy()
        st.session_state[df_key] = current_df.copy()
    staff_full = _coerce_editor_df(current_df, staff_fixed_cols, initial_df)
    # Normalize before rendering so selectbox values stay stable without mutating widget state.
    note_cols = [c for c in staff_fixed_cols if c.startswith("NOTE_")]
    str_cols = ["CLASS", "LOCATION_KEY", "__ORIG_CLASS", "__ORIG_LOCATION_KEY"] + note_cols
    num_cols = [c for c in staff_fixed_cols if c not in str_cols]
    for col in str_cols:
        if col in staff_full.columns:
            s = staff_full[col].fillna("").astype(str).str.strip()
            s = s.replace({"nan": "", "<NA>": "", "None": ""})
            staff_full[col] = s
    for col in num_cols:
        if col in staff_full.columns:
            staff_full[col] = pd.to_numeric(staff_full[col], errors="coerce").fillna(0.0)

    staff_col_cfg: dict[str, object] = {
        "__ORIG_CLASS": None,
        "__ORIG_LOCATION_KEY": None,
        "CLASS": st.column_config.SelectboxColumn("Class", options=["", "Team Overhead", "Delivery"], required=False),
        "LOCATION_KEY": st.column_config.SelectboxColumn("Location", options=location_options, required=False),
        "HC_ALL": st.column_config.NumberColumn("All PIs", step=0.1, min_value=0.0) if staff_mode == "All PIs same" else None,
        "NOTE_ALL": st.column_config.TextColumn("Note (All PIs)") if (staff_notes_enabled and staff_mode == "All PIs same") else None,
    }
    for pi in [1, 2, 3, 4]:
        staff_col_cfg[f"HC_I{pi}"] = (
            st.column_config.NumberColumn(f"I{pi}", step=0.1, min_value=0.0) if (staff_mode == "Per PI" and pi in selected_pis) else None
        )
        staff_col_cfg[f"NOTE_I{pi}"] = (
            st.column_config.TextColumn(f"Note I{pi}") if (staff_notes_enabled and staff_mode == "Per PI" and pi in selected_pis) else None
        )

    staff_out = st.data_editor(
        staff_full[staff_fixed_cols],
        hide_index=True,
        num_rows="dynamic",
        column_config=staff_col_cfg,
        key=widget_key,
    )
    staff_edited = staff_out.copy() if isinstance(staff_out, pd.DataFrame) else pd.DataFrame(columns=staff_fixed_cols)
    # Normalize editor output to keep schema stable and avoid extra numeric columns.
    staff_edited = staff_edited.reindex(columns=staff_fixed_cols)
    note_cols = [c for c in staff_fixed_cols if c.startswith("NOTE_")]
    str_cols = ["CLASS", "LOCATION_KEY", "__ORIG_CLASS", "__ORIG_LOCATION_KEY"] + note_cols
    num_cols = [c for c in staff_fixed_cols if c not in str_cols]
    for col in str_cols:
        if col in staff_edited.columns:
            s = staff_edited[col].fillna("").astype(str).str.strip()
            s = s.replace({"nan": "", "<NA>": "", "None": ""})
            staff_edited[col] = s
    for col in num_cols:
        if col in staff_edited.columns:
            staff_edited[col] = pd.to_numeric(staff_edited[col], errors="coerce").fillna(0.0)

    # Streamlit may emit NaN/NA for selectbox cells during edits; normalize to stable strings
    # so selections don't appear to "disappear" on rerun.
    for _col in ["CLASS", "LOCATION_KEY", "__ORIG_CLASS", "__ORIG_LOCATION_KEY"]:
        if _col in staff_edited.columns:
            s = staff_edited[_col].fillna("").astype(str).str.strip()
            s = s.replace({"nan": "", "<NA>": "", "None": ""})
            staff_edited[_col] = s

    if isinstance(staff_full, pd.DataFrame) and not staff_full.empty:
        staff_visible_cols_set = {"CLASS", "LOCATION_KEY"}
        if staff_mode == "All PIs same":
            staff_visible_cols_set.add("HC_ALL")
            if staff_notes_enabled:
                staff_visible_cols_set.add("NOTE_ALL")
        else:
            staff_visible_cols_set |= {f"HC_I{pi}" for pi in selected_pis}
            if staff_notes_enabled:
                staff_visible_cols_set |= {f"NOTE_I{pi}" for pi in selected_pis}
        staff_backfill_cols = [c for c in staff_fixed_cols if c not in staff_visible_cols_set]

        prev_lookup = {
            (str(r.get("CLASS") or "").strip(), str(r.get("LOCATION_KEY") or "").strip()): r
            for _, r in staff_full.iterrows()
        }
        for idx, r in staff_edited.iterrows():
            key = (str(r.get("CLASS") or "").strip(), str(r.get("LOCATION_KEY") or "").strip())
            prev = prev_lookup.get(key)
            if prev is None:
                continue
            # Only backfill hidden columns so we don't clobber in-progress edits (Streamlit reruns on cell commit).
            for col in staff_backfill_cols:
                if pd.isna(r.get(col)):
                    staff_edited.at[idx, col] = prev.get(col)
    staff_edited["__ORIG_CLASS"] = staff_edited.get("__ORIG_CLASS", "").replace({None: ""})
    staff_edited["__ORIG_LOCATION_KEY"] = staff_edited.get("__ORIG_LOCATION_KEY", "").replace({None: ""})
    staff_edited.loc[staff_edited["__ORIG_CLASS"].astype(str).str.strip().eq(""), "__ORIG_CLASS"] = staff_edited.get(
        "CLASS", ""
    )
    staff_edited.loc[
        staff_edited["__ORIG_LOCATION_KEY"].astype(str).str.strip().eq(""), "__ORIG_LOCATION_KEY"
    ] = staff_edited.get("LOCATION_KEY", "")
    st.session_state[df_key] = staff_edited.reindex(columns=staff_fixed_cols).copy()
    return staff_mode, staff_edited


@_team_fragment
def render_contractor_headcount_editor(
    *,
    team_id: str,
    year_i: int,
    cons_fixed_cols: list[str],
    initial_df: pd.DataFrame,
    cons_has_overrides: bool,
    selected_pis: list[int],
    provider_options: list[str],
    cons_notes_enabled: bool,
) -> tuple[str, pd.DataFrame]:
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
                df[col] = "" if col in ["CLASS", "PROVIDER", "__ORIG_CLASS", "__ORIG_COMPANYID"] or col.startswith("NOTE_") else 0.0
        return df.reindex(columns=cols)
    # -------------------------
    # Section 2: Contractors
    # -------------------------
    st.markdown("###### Contractors (by Provider)")
    cons_mode = st.radio(
        "Contractors input mode",
        options=["All PIs same", "Per PI"],
        index=1 if cons_has_overrides else 0,
        horizontal=True,
        key=f"teams_contractors_mode_{team_id}_{year_i}",
    )
    if not provider_options or len(provider_options) <= 1:
        st.info("No contractor providers configured yet. Add companies in Rates → Contractors.")

    editor_key = f"hc_editor_team_contractor_{team_id}_{year_i}_v2"
    widget_key = editor_key
    df_key = f"{editor_key}__df"
    ctx_key = f"{editor_key}__ctx"
    ctx_val = (team_id, int(year_i))
    if st.session_state.get(ctx_key) != ctx_val:
        # Widget key stores Streamlit's dict state; df_key stores the dataframe.
        st.session_state[df_key] = initial_df.copy()
        st.session_state.pop(widget_key, None)
        st.session_state[ctx_key] = ctx_val

    current_df = st.session_state.get(df_key)
    if not isinstance(current_df, pd.DataFrame):
        current_df = initial_df.copy()
        st.session_state[df_key] = current_df.copy()
    cons_full = _coerce_editor_df(current_df, cons_fixed_cols, initial_df)
    # Normalize before rendering so selectbox values stay stable without mutating widget state.
    note_cols = [c for c in cons_fixed_cols if c.startswith("NOTE_")]
    str_cols = ["CLASS", "PROVIDER", "__ORIG_CLASS", "__ORIG_COMPANYID"] + note_cols
    num_cols = [c for c in cons_fixed_cols if c not in str_cols]
    for col in str_cols:
        if col in cons_full.columns:
            s = cons_full[col].fillna("").astype(str).str.strip()
            s = s.replace({"nan": "", "<NA>": "", "None": ""})
            cons_full[col] = s
    for col in num_cols:
        if col in cons_full.columns:
            cons_full[col] = pd.to_numeric(cons_full[col], errors="coerce").fillna(0.0)

    cons_col_cfg: dict[str, object] = {
        "__ORIG_CLASS": None,
        "__ORIG_COMPANYID": None,
        "CLASS": st.column_config.SelectboxColumn("Class", options=["", "Contractor C", "Contractor CS"], required=False),
        "PROVIDER": st.column_config.SelectboxColumn("Provider", options=provider_options, required=False),
        "HC_ALL": st.column_config.NumberColumn("All PIs", step=0.1, min_value=0.0) if cons_mode == "All PIs same" else None,
        "NOTE_ALL": st.column_config.TextColumn("Note (All PIs)") if (cons_notes_enabled and cons_mode == "All PIs same") else None,
    }
    for pi in [1, 2, 3, 4]:
        cons_col_cfg[f"HC_I{pi}"] = (
            st.column_config.NumberColumn(f"I{pi}", step=0.1, min_value=0.0) if (cons_mode == "Per PI" and pi in selected_pis) else None
        )
        cons_col_cfg[f"NOTE_I{pi}"] = (
            st.column_config.TextColumn(f"Note I{pi}") if (cons_notes_enabled and cons_mode == "Per PI" and pi in selected_pis) else None
        )

    cons_out = st.data_editor(
        cons_full[cons_fixed_cols],
        hide_index=True,
        num_rows="dynamic",
        column_config=cons_col_cfg,
        key=widget_key,
    )
    cons_edited = cons_out.copy() if isinstance(cons_out, pd.DataFrame) else pd.DataFrame(columns=cons_fixed_cols)
    # Normalize editor output to keep schema stable and avoid extra numeric columns.
    cons_edited = cons_edited.reindex(columns=cons_fixed_cols)
    note_cols = [c for c in cons_fixed_cols if c.startswith("NOTE_")]
    str_cols = ["CLASS", "PROVIDER", "__ORIG_CLASS", "__ORIG_COMPANYID"] + note_cols
    num_cols = [c for c in cons_fixed_cols if c not in str_cols]
    for col in str_cols:
        if col in cons_edited.columns:
            s = cons_edited[col].fillna("").astype(str).str.strip()
            s = s.replace({"nan": "", "<NA>": "", "None": ""})
            cons_edited[col] = s
    for col in num_cols:
        if col in cons_edited.columns:
            cons_edited[col] = pd.to_numeric(cons_edited[col], errors="coerce").fillna(0.0)

    # Streamlit may emit NaN/NA for selectbox cells during edits; normalize to stable strings
    # so selections don't appear to "disappear" on rerun.
    for _col in ["CLASS", "PROVIDER", "__ORIG_CLASS", "__ORIG_COMPANYID"]:
        if _col in cons_edited.columns:
            s = cons_edited[_col].fillna("").astype(str).str.strip()
            s = s.replace({"nan": "", "<NA>": "", "None": ""})
            cons_edited[_col] = s

    if isinstance(cons_full, pd.DataFrame) and not cons_full.empty:
        cons_visible_cols_set = {"CLASS", "PROVIDER"}
        if cons_mode == "All PIs same":
            cons_visible_cols_set.add("HC_ALL")
            if cons_notes_enabled:
                cons_visible_cols_set.add("NOTE_ALL")
        else:
            cons_visible_cols_set |= {f"HC_I{pi}" for pi in selected_pis}
            if cons_notes_enabled:
                cons_visible_cols_set |= {f"NOTE_I{pi}" for pi in selected_pis}
        cons_backfill_cols = [c for c in cons_fixed_cols if c not in cons_visible_cols_set]

        prev_lookup = {
            (str(r.get("CLASS") or "").strip(), str(r.get("PROVIDER") or "").strip()): r
            for _, r in cons_full.iterrows()
        }
        for idx, r in cons_edited.iterrows():
            key = (str(r.get("CLASS") or "").strip(), str(r.get("PROVIDER") or "").strip())
            prev = prev_lookup.get(key)
            if prev is None:
                continue
            # Only backfill hidden columns so we don't clobber in-progress edits.
            for col in cons_backfill_cols:
                if pd.isna(r.get(col)):
                    cons_edited.at[idx, col] = prev.get(col)
    cons_edited["__ORIG_CLASS"] = cons_edited.get("__ORIG_CLASS", "").replace({None: ""})
    cons_edited["__ORIG_COMPANYID"] = cons_edited.get("__ORIG_COMPANYID", "").replace({None: ""})
    cons_edited.loc[cons_edited["__ORIG_CLASS"].astype(str).str.strip().eq(""), "__ORIG_CLASS"] = cons_edited.get(
        "CLASS", ""
    )
    st.session_state[df_key] = cons_edited.reindex(columns=cons_fixed_cols).copy()
    return cons_mode, cons_edited


# ---------------------------
# Unified, tabbed UI
# ---------------------------
teams_df = _teams_df()
teams_df = teams_df.copy() if isinstance(teams_df, pd.DataFrame) else pd.DataFrame()
programs_df = programs.copy() if isinstance(programs, pd.DataFrame) else pd.DataFrame()
if programs_df.empty:
    try:
        _p = fetch_df(
            """
            SELECT
                PROGRAMID,
                PROGRAMNAME AS PROGRAMNAME_RAW,
                COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME) AS PROGRAMNAME
            FROM PROGRAMS
            ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME)
            """
        )
        programs_df = _p.copy() if isinstance(_p, pd.DataFrame) else pd.DataFrame()
    except Exception:
        programs_df = pd.DataFrame()
if teams_df.empty:
    try:
        _t = fetch_df(
            """
            SELECT TEAMID,
                   TEAMNAME AS TEAMNAME_RAW,
                   COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME) AS TEAMNAME,
                   PROGRAMID, TEAMFTE,
                   DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE,
                   PRODUCTOWNER, UPDATED_AT, UPDATED_BY
            FROM TEAMS
            ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME)
            """
        )
        teams_df = _t.copy() if isinstance(_t, pd.DataFrame) else pd.DataFrame()
    except Exception:
        teams_df = pd.DataFrame()

# Normalize column aliases to stable uppercase names expected by this page.
if isinstance(programs_df, pd.DataFrame) and not programs_df.empty:
    p_cols = {str(c).strip().upper(): c for c in programs_df.columns}
    if "PROGRAMID" not in programs_df.columns and p_cols.get("PROGRAMID") in programs_df.columns:
        programs_df["PROGRAMID"] = programs_df[p_cols["PROGRAMID"]]
    if "PROGRAMNAME" not in programs_df.columns and p_cols.get("PROGRAMNAME") in programs_df.columns:
        programs_df["PROGRAMNAME"] = programs_df[p_cols["PROGRAMNAME"]]
    if "PROGRAMID" not in programs_df.columns and len(programs_df.columns) >= 1:
        programs_df["PROGRAMID"] = programs_df.iloc[:, 0]
    if "PROGRAMNAME" not in programs_df.columns:
        programs_df["PROGRAMNAME"] = programs_df.get("PROGRAMID", "")
    programs_df["PROGRAMID"] = programs_df["PROGRAMID"].fillna("").astype(str).str.strip()
    programs_df["PROGRAMNAME"] = programs_df["PROGRAMNAME"].fillna("").astype(str).str.strip()
    if "PROGRAMNAME_RAW" in programs_df.columns:
        blank_prog = programs_df["PROGRAMNAME"] == ""
        programs_df.loc[blank_prog, "PROGRAMNAME"] = (
            programs_df.loc[blank_prog, "PROGRAMNAME_RAW"].fillna("").astype(str).str.strip()
        )
    programs_df = programs_df[(programs_df["PROGRAMID"] != "") & (programs_df["PROGRAMNAME"] != "")]


def _resolve_program_row(df: pd.DataFrame, selected_label: str) -> pd.DataFrame:
    if df is None or df.empty or not selected_label:
        return pd.DataFrame()
    sel_u = str(selected_label).strip().upper()
    out = pd.DataFrame()
    if "PROGRAMNAME" in df.columns:
        out = df.loc[df["PROGRAMNAME"].fillna("").astype(str).str.strip().str.upper() == sel_u]
    if out.empty and "PROGRAMNAME_RAW" in df.columns:
        out = df.loc[df["PROGRAMNAME_RAW"].fillna("").astype(str).str.strip().str.upper() == sel_u]
    return out.copy() if out is not None else pd.DataFrame()

if isinstance(teams_df, pd.DataFrame) and not teams_df.empty:
    t_cols = {str(c).strip().upper(): c for c in teams_df.columns}
    if "TEAMID" not in teams_df.columns and t_cols.get("TEAMID") in teams_df.columns:
        teams_df["TEAMID"] = teams_df[t_cols["TEAMID"]]
    if "TEAMNAME" not in teams_df.columns and t_cols.get("TEAMNAME") in teams_df.columns:
        teams_df["TEAMNAME"] = teams_df[t_cols["TEAMNAME"]]
    if "PROGRAMID" not in teams_df.columns and t_cols.get("PROGRAMID") in teams_df.columns:
        teams_df["PROGRAMID"] = teams_df[t_cols["PROGRAMID"]]
    if "TEAMID" not in teams_df.columns and len(teams_df.columns) >= 1:
        teams_df["TEAMID"] = teams_df.iloc[:, 0]
    if "TEAMNAME" not in teams_df.columns:
        teams_df["TEAMNAME"] = teams_df.get("TEAMID", "")
    if "PROGRAMID" not in teams_df.columns:
        teams_df["PROGRAMID"] = ""
    teams_df["TEAMID"] = teams_df["TEAMID"].fillna("").astype(str).str.strip()
    teams_df["TEAMNAME"] = teams_df["TEAMNAME"].fillna("").astype(str).str.strip()
    if "TEAMNAME_RAW" in teams_df.columns:
        blank_team = teams_df["TEAMNAME"] == ""
        teams_df.loc[blank_team, "TEAMNAME"] = (
            teams_df.loc[blank_team, "TEAMNAME_RAW"].fillna("").astype(str).str.strip()
        )
    teams_df["PROGRAMID"] = teams_df["PROGRAMID"].fillna("").astype(str).str.strip()

# Scoped contributor filtering: hide out-of-scope programs/teams from selectors and views.
if IS_SCOPED_CONTRIBUTOR:
    if isinstance(programs_df, pd.DataFrame) and not programs_df.empty and "PROGRAMID" in programs_df.columns:
        programs_df = programs_df.loc[
            programs_df["PROGRAMID"].fillna("").astype(str).str.strip().isin(ALLOWED_PROGRAM_IDS)
        ].copy()
    if isinstance(teams_df, pd.DataFrame) and not teams_df.empty and "TEAMID" in teams_df.columns:
        teams_df = teams_df.loc[
            teams_df["TEAMID"].fillna("").astype(str).str.strip().isin(ALLOWED_TEAM_IDS)
        ].copy()
elif IS_CONTRIBUTOR:
    programs_df = programs_df.iloc[0:0].copy() if isinstance(programs_df, pd.DataFrame) else pd.DataFrame()
    teams_df = teams_df.iloc[0:0].copy() if isinstance(teams_df, pd.DataFrame) else pd.DataFrame()

program_name_by_id = {}
if not programs_df.empty and "PROGRAMID" in programs_df.columns and "PROGRAMNAME" in programs_df.columns:
    program_name_by_id = {
        str(r["PROGRAMID"]): str(r["PROGRAMNAME"])
        for _, r in programs_df.iterrows()
        if str(r.get("PROGRAMID") or "").strip()
    }

df_teams = teams_df.copy() if teams_df is not None else pd.DataFrame()
if (
    df_teams is not None
    and not df_teams.empty
    and programs_df is not None
    and not programs_df.empty
    and "PROGRAMID" in df_teams.columns
    and "PROGRAMID" in programs_df.columns
):
    df_teams = df_teams.merge(programs_df[["PROGRAMID", "PROGRAMNAME"]], on="PROGRAMID", how="left")
if df_teams is not None and not df_teams.empty and "PROGRAMNAME" in df_teams.columns:
    df_teams["PROGRAMNAME"] = df_teams["PROGRAMNAME"].fillna("").astype(str).str.strip()
if df_teams is not None and not df_teams.empty and "TEAMNAME" in df_teams.columns:
    df_teams["TEAMNAME"] = df_teams["TEAMNAME"].fillna("").astype(str).str.strip()

program_options = ["(Select a program)"] + (
    sorted([p for p in programs_df["PROGRAMNAME"].dropna().astype(str).unique().tolist() if p])
    if (programs_df is not None and not programs_df.empty and "PROGRAMNAME" in programs_df.columns)
    else []
)
if st.session_state.get("teams_program_pick_pending"):
    st.session_state["teams_program_pick"] = st.session_state.get("teams_program_pick_pending")
    st.session_state.pop("teams_program_pick_pending", None)
if st.session_state.get("teams_team_pick_pending"):
    st.session_state["teams_team_pick"] = st.session_state.get("teams_team_pick_pending")
    st.session_state.pop("teams_team_pick_pending", None)
if st.session_state.get("teams_program_pick") not in program_options:
    st.session_state["teams_program_pick"] = "(Select a program)"
def _on_program_scope_change() -> None:
    st.session_state["teams_team_pick"] = "(Select a team)"


st.selectbox(
    "Program",
    options=program_options,
    index=0,
    key="teams_program_pick",
    on_change=_on_program_scope_change,
)
program_pick = str(st.session_state.get("teams_program_pick") or "(Select a program)")
selected_program_id = ""
if (
    program_pick
    and program_pick != "(Select a program)"
    and programs_df is not None
    and not programs_df.empty
    and "PROGRAMID" in programs_df.columns
    and "PROGRAMNAME" in programs_df.columns
):
    try:
        rows = programs_df.loc[
            programs_df["PROGRAMNAME"].fillna("").astype(str).str.strip().str.upper()
            == str(program_pick).strip().upper()
        ]
        if not rows.empty:
            selected_program_id = str(rows.iloc[0].get("PROGRAMID") or "").strip()
    except Exception:
        selected_program_id = ""

team_options = ["(Select a team)"]
if (
    program_pick
    and program_pick != "(Select a program)"
    and df_teams is not None
    and not df_teams.empty
    and "TEAMNAME" in df_teams.columns
):
    team_scope_df = df_teams.copy()
    if selected_program_id and "PROGRAMID" in team_scope_df.columns:
        team_scope_df = team_scope_df.loc[
            team_scope_df["PROGRAMID"].fillna("").astype(str).str.strip().str.upper()
            == str(selected_program_id).strip().upper()
        ].copy()
    elif "PROGRAMNAME" in team_scope_df.columns:
        team_scope_df = team_scope_df.loc[
            team_scope_df["PROGRAMNAME"].fillna("").astype(str).str.strip().str.upper()
            == str(program_pick).strip().upper()
        ].copy()
    team_options = ["(Select a team)"] + sorted(
        [
            t
            for t in team_scope_df["TEAMNAME"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
            if t
        ]
    )
if st.session_state.get("teams_team_pick") not in team_options:
    st.session_state["teams_team_pick"] = "(Select a team)"

team_disabled = (program_pick == "(Select a program)")
st.selectbox(
    "Team",
    options=team_options,
    index=0,
    key="teams_team_pick",
    disabled=team_disabled,
)
team_pick = str(st.session_state.get("teams_team_pick") or "(Select a team)")
if team_disabled:
    st.caption("Next: select a Program to load Teams.")
st.caption(
    f"Applied scope: Program = **{program_pick if program_pick != '(Select a program)' else 'All'}** · "
    f"Team = **{team_pick if team_pick != '(Select a team)' else 'All'}**"
)

team_row = None
if (
    program_pick
    and program_pick != "(Select a program)"
    and team_pick
    and team_pick != "(Select a team)"
    and df_teams is not None
    and not df_teams.empty
):
    try:
        match = df_teams.loc[
            (df_teams["PROGRAMNAME"].astype(str) == str(program_pick))
            & (df_teams["TEAMNAME"].astype(str) == str(team_pick))
        ]
        team_row = match.iloc[0] if match is not None and not match.empty else None
    except Exception:
        team_row = None

if section == "Overview":
        st.markdown("### Overview")
        st.write("Headcount drives workforce costs in Budget. Composition is calculated automatically.")
        with st.container(border=True):
            st.markdown("**Onboarding checklist**")
            if program_pick and program_pick != "(Select a program)":
                ado_years = _ado_years_cached()
                years_to_check = tuple(sorted(ado_years)[-3:]) if ado_years else (date.today().year,)
                ado_features_df = _load_ado_features_for_mapping_candidates_cached(years_to_check)
                if ado_features_df is None or not isinstance(ado_features_df, pd.DataFrame) or ado_features_df.empty:
                    st.caption("No ADO data yet. Run sync in Settings to populate this checklist.")
                else:
                    program_id = ""
                    if (
                        programs_df is not None
                        and not programs_df.empty
                        and "PROGRAMID" in programs_df.columns
                    ):
                        match_rows = _resolve_program_row(programs_df, str(program_pick))
                        if not match_rows.empty:
                            program_id = str(match_rows.iloc[0].get("PROGRAMID") or "").strip()
                    prog_map_df = list_ado_program_mappings()
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
                    lines = [
                        f"- Teams not onboarded (ADO) for this Program: {int(len(team_unmapped)) if team_unmapped is not None else 0}",
                        f"- Applications not onboarded (ADO) for this Program: {int(len(app_unmapped)) if app_unmapped is not None else 0}",
                    ]
                    st.markdown("\n".join(lines))
            else:
                st.caption("Next: select a Program to see onboarding status.")
        if team_row is None:
            if teams_df is None or teams_df.empty:
                st.info("No teams yet.")
            else:
                st.info("Select a program and team to view details.")
                display_df = teams_df.copy()
                if program_pick and program_pick != "(Select a program)" and "PROGRAMNAME" in display_df.columns:
                    pick_upper = str(program_pick).strip().upper()
                    display_df = display_df[
                        display_df["PROGRAMNAME"].astype(str).str.strip().str.upper() == pick_upper
                    ]
                display_df = display_df.drop(
                    columns=[
                        c
                        for c in ["TEAMID", "PROGRAMID", "TEAMNAME_RAW", "PROGRAMNAME_RAW"]
                        if c in display_df.columns
                    ],
                    errors="ignore",
                )
                st.dataframe(display_df, use_container_width=True, hide_index=True, height=360)
        else:
            team_id = str(team_row.get("TEAMID") or "")
            program_id = str(team_row.get("PROGRAMID") or "")
            program_name = program_name_by_id.get(program_id, "—")
            st.caption(f"Program: {program_name}")

            kpi_year = int(st.session_state.get(f"hc_year_table_{team_id}", date.today().year))
            kpi_cache_key = f"teams_overview_kpis_{team_id}_{kpi_year}"
            if kpi_cache_key not in st.session_state:
                team_overhead = delivery = contractor_c = contractor_cs = 0.0
                meta = st.session_state.get(f"teams_hc_editor_meta_{team_id}")
                if isinstance(meta, dict) and meta.get("ctx") == (team_id, int(kpi_year)):
                    staff_map = meta.get("staff_map", {}) or {}
                    cons_map = meta.get("cons_map", {}) or {}
                    team_overhead = float(sum(v for (cls, _loc, pi), v in staff_map.items() if cls == "TEAM" and pi == 0))
                    delivery = float(sum(v for (cls, _loc, pi), v in staff_map.items() if cls == "DELIVERY" and pi == 0))
                    contractor_c = float(sum(v for (cls, _cid, pi), v in cons_map.items() if cls == "CONTRACTOR_C" and pi == 0))
                    contractor_cs = float(sum(v for (cls, _cid, pi), v in cons_map.items() if cls == "CONTRACTOR_CS" and pi == 0))
                else:
                    try:
                        hc_all_kpi = _list_team_headcount_cached(team_id)
                        hc_all_kpi = hc_all_kpi.copy() if hc_all_kpi is not None and not hc_all_kpi.empty else pd.DataFrame()
                        if not hc_all_kpi.empty:
                            hc_all_kpi["YEAR_NUM"] = pd.to_numeric(hc_all_kpi.get("YEAR"), errors="coerce").fillna(0).astype(int)
                            hc_all_kpi["PI_NUM"] = pd.to_numeric(hc_all_kpi.get("PI"), errors="coerce").fillna(0).astype(int)
                            hc_class = hc_all_kpi["CLASS"] if "CLASS" in hc_all_kpi.columns else pd.Series([""] * len(hc_all_kpi), index=hc_all_kpi.index)
                            hc_all_kpi["CLASS"] = hc_class.astype(str).str.strip().str.upper()
                            hc_all_kpi["HEADCOUNT"] = pd.to_numeric(hc_all_kpi.get("HEADCOUNT"), errors="coerce").fillna(0.0)
                            sub = hc_all_kpi.loc[
                                (hc_all_kpi["YEAR_NUM"] == int(kpi_year))
                                & (hc_all_kpi["PI_NUM"] == 0)
                                & (hc_all_kpi["CLASS"].isin(["TEAM", "DELIVERY"]))
                            ]
                            team_overhead = float(sub.loc[sub["CLASS"] == "TEAM", "HEADCOUNT"].sum())
                            delivery = float(sub.loc[sub["CLASS"] == "DELIVERY", "HEADCOUNT"].sum())
                    except Exception:
                        pass
                    try:
                        cons_all_kpi = _list_team_contractor_headcount_cached(team_id)
                        cons_all_kpi = cons_all_kpi.copy() if cons_all_kpi is not None and not cons_all_kpi.empty else pd.DataFrame()
                        if not cons_all_kpi.empty:
                            cons_all_kpi["YEAR_NUM"] = pd.to_numeric(cons_all_kpi.get("YEAR"), errors="coerce").fillna(0).astype(int)
                            cons_all_kpi["PI_NUM"] = pd.to_numeric(cons_all_kpi.get("PI"), errors="coerce").fillna(0).astype(int)
                            cons_class = cons_all_kpi["CLASS"] if "CLASS" in cons_all_kpi.columns else pd.Series([""] * len(cons_all_kpi), index=cons_all_kpi.index)
                            cons_all_kpi["CLASS"] = cons_class.astype(str).str.strip().str.upper()
                            cons_all_kpi["HEADCOUNT"] = pd.to_numeric(cons_all_kpi.get("HEADCOUNT"), errors="coerce").fillna(0.0)
                            subc = cons_all_kpi.loc[
                                (cons_all_kpi["YEAR_NUM"] == int(kpi_year))
                                & (cons_all_kpi["PI_NUM"] == 0)
                                & (cons_all_kpi["CLASS"].isin(["CONTRACTOR_C", "CONTRACTOR_CS"]))
                            ]
                            contractor_c = float(subc.loc[subc["CLASS"] == "CONTRACTOR_C", "HEADCOUNT"].sum())
                            contractor_cs = float(subc.loc[subc["CLASS"] == "CONTRACTOR_CS", "HEADCOUNT"].sum())
                    except Exception:
                        pass

                st.session_state[kpi_cache_key] = {
                    "TEAM": float(team_overhead),
                    "DELIVERY": float(delivery),
                    "CONTRACTOR_C": float(contractor_c),
                    "CONTRACTOR_CS": float(contractor_cs),
                }

            kpi = st.session_state.get(kpi_cache_key, {}) or {}
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Team Overhead FTE", f"{float(kpi.get('TEAM', 0.0)):,.2f}")
            c2.metric("Delivery FTE", f"{float(kpi.get('DELIVERY', 0.0)):,.2f}")
            c3.metric("Contractor C FTE", f"{float(kpi.get('CONTRACTOR_C', 0.0)):,.2f}")
            c4.metric("Contractor CS FTE", f"{float(kpi.get('CONTRACTOR_CS', 0.0)):,.2f}")

            st.markdown("---")
            with st.expander("Link ADO Teams", expanded=False):
                st.caption("Connect raw ADO team variants to this NEXT Team.")
                if not _can_edit_team(team_id):
                    st.info("Read-only for this team scope.")
                else:
                    render_ado_mapping_panel(
                        "team",
                        team_id,
                        context_filters={"program_id": program_id, "updated_by": str(user.get("email") or "")},
                        user_is_admin=is_admin,
                        allow_create_toggle=True,
                    )

if section == "App Classification":
    _render_op_feedback("next_app_classification")
    st.markdown("### App Classification")
    st.caption("Classify ADO app candidates before onboarding, then onboard selected rows safely.")

    ado_years = _ado_years_cached()
    years_to_check = tuple(sorted(ado_years)[-3:]) if ado_years else (date.today().year,)
    program_filters: tuple[str, ...] = tuple()
    team_filters: tuple[str, ...] = tuple()
    ado_program_scope_keys: tuple[str, ...] = tuple()
    ado_team_scope_keys: tuple[str, ...] = tuple()
    scope_program_id = ""
    scope_team_id = ""

    if program_pick and program_pick != "(Select a program)" and isinstance(programs_df, pd.DataFrame) and not programs_df.empty:
        prog_rows = _resolve_program_row(programs_df, str(program_pick))
        if not prog_rows.empty:
            scope_program_id = str(prog_rows.iloc[0].get("PROGRAMID") or "").strip()

    if team_pick and team_pick != "(Select a team)":
        if team_row is not None:
            scope_team_id = str(team_row.get("TEAMID") or "").strip()
        elif isinstance(teams_df, pd.DataFrame) and not teams_df.empty:
            team_match = teams_df.loc[teams_df["TEAMNAME"].astype(str) == str(team_pick)]
            if scope_program_id and "PROGRAMID" in team_match.columns:
                team_match = team_match.loc[team_match["PROGRAMID"].astype(str) == scope_program_id]
            if not team_match.empty:
                scope_team_id = str(team_match.iloc[0].get("TEAMID") or "").strip()

    try:
        prog_map_df = list_ado_program_mappings()
        if (
            scope_program_id
            and isinstance(prog_map_df, pd.DataFrame)
            and not prog_map_df.empty
            and "PROGRAMID" in prog_map_df.columns
            and "ADO_PROGRAM" in prog_map_df.columns
        ):
            ado_program_scope_keys = tuple(
                sorted(
                    {
                        str(v).strip().upper()
                        for v in prog_map_df.loc[prog_map_df["PROGRAMID"].astype(str) == scope_program_id, "ADO_PROGRAM"].dropna().tolist()
                        if str(v).strip()
                    }
                )
            )
    except Exception:
        ado_program_scope_keys = tuple()

    try:
        team_map_df = list_ado_team_mappings()
        if (
            scope_team_id
            and isinstance(team_map_df, pd.DataFrame)
            and not team_map_df.empty
            and "TEAMID" in team_map_df.columns
            and "ADO_TEAM_KEY" in team_map_df.columns
        ):
            ado_team_scope_keys = tuple(
                sorted(
                    {
                        str(v).strip().upper()
                        for v in team_map_df.loc[team_map_df["TEAMID"].astype(str) == scope_team_id, "ADO_TEAM_KEY"].dropna().tolist()
                        if str(v).strip()
                    }
                )
            )
    except Exception:
        ado_team_scope_keys = tuple()

    # Fallback to raw label filters only when no canonical ADO mappings exist for the selected scope.
    if program_pick and program_pick != "(Select a program)" and not ado_program_scope_keys:
        program_filters = tuple([str(program_pick)])
    if team_pick and team_pick != "(Select a team)" and not ado_team_scope_keys:
        team_filters = tuple([str(team_pick)])

    @cache_data_portfolio(ttl=180, show_spinner=False)
    def _build_next_app_candidates_cached(
        years: tuple[int, ...],
        programs: tuple[str, ...],
        teams: tuple[str, ...],
        ado_programs: tuple[str, ...],
        ado_teams: tuple[str, ...],
    ) -> pd.DataFrame:
        df = build_next_app_candidates_df(
            fetch_df,
            years=years,
            programs=programs,
            teams=teams,
            ado_program_keys=ado_programs,
            ado_team_keys=ado_teams,
            include_mapped=True,
            fuzzy_threshold=92,
        )
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    class_df = _build_next_app_candidates_cached(
        years_to_check,
        program_filters,
        team_filters,
        ado_program_scope_keys,
        ado_team_scope_keys,
    )
    if class_df is None or class_df.empty:
        st.caption("No app candidates found for the current filters.")
    else:
        mapped_count = int((class_df["status"].astype(str) == "Mapped").sum()) if "status" in class_df.columns else 0
        unmapped_count = int((class_df["status"].astype(str) == "Unmapped").sum()) if "status" in class_df.columns else 0
        total_effort = float(pd.to_numeric(class_df.get("effort_points_sum"), errors="coerce").fillna(0.0).sum())
        mapped_effort = float(
            pd.to_numeric(
                class_df.loc[class_df["status"].astype(str) == "Mapped", "effort_points_sum"]
                if "status" in class_df.columns and "effort_points_sum" in class_df.columns
                else pd.Series(dtype=float),
                errors="coerce",
            ).fillna(0.0).sum()
        )
        coverage = (100.0 * mapped_effort / total_effort) if total_effort > 0 else 0.0
        c_kpi1, c_kpi2, c_kpi3 = st.columns(3)
        c_kpi1.metric("Mapped apps", f"{mapped_count}")
        c_kpi2.metric("Unmapped apps", f"{unmapped_count}")
        c_kpi3.metric("Demand coverage mapped", f"{coverage:,.1f}%")

        edit_df = class_df.copy()
        edit_df["selected"] = False
        if "next_app_display_name" not in edit_df.columns:
            edit_df["next_app_display_name"] = edit_df.get("ADO_APP_RAW", "").astype(str)
        if "vendor" not in edit_df.columns:
            edit_df["vendor"] = ""
        if "is_base_pool" not in edit_df.columns:
            edit_df["is_base_pool"] = False
        if "create_new_app" not in edit_df.columns:
            edit_df["create_new_app"] = False

        show_cols = [
            "selected",
            "ADO_APP_RAW",
            "next_app_display_name",
            "vendor",
            "is_base_pool",
            "create_new_app",
            "status",
            "feature_count",
            "effort_points_sum",
            "suggested_team",
            "needs_vendor",
        ]
        show_cols = [c for c in show_cols if c in edit_df.columns]
        pick = st.data_editor(
            edit_df[show_cols],
            use_container_width=True,
            hide_index=True,
            height=360,
            column_config={
                "selected": st.column_config.CheckboxColumn("Select"),
                "next_app_display_name": st.column_config.TextColumn("NEXT App Display Name"),
                "vendor": st.column_config.TextColumn(
                    "Vendor",
                    help="Enter vendor per app row. Existing names are reused automatically (case-insensitive).",
                ),
                "is_base_pool": st.column_config.CheckboxColumn("Is BASE Pool"),
                "create_new_app": st.column_config.CheckboxColumn("Create New App"),
                "feature_count": st.column_config.NumberColumn("Feature Count", disabled=True),
                "effort_points_sum": st.column_config.NumberColumn("Effort Points", disabled=True),
                "needs_vendor": st.column_config.CheckboxColumn("Needs Vendor", disabled=True),
            },
            disabled=[c for c in show_cols if c not in {"selected", "next_app_display_name", "vendor", "is_base_pool", "create_new_app"}],
            key="teams_next_app_classification_editor",
        )
        selected = pick[pick["selected"] == True].copy() if isinstance(pick, pd.DataFrame) and "selected" in pick.columns else pd.DataFrame()

        st.markdown("#### Onboard selected")
        apply_btn = st.button(
            "Run safe onboarding",
            icon=":material/check_circle:",
            disabled=selected.empty,
            key="teams_next_app_onboard_btn",
        )
        if apply_btn:
            if selected.empty:
                st.error("Select at least one candidate row.")
            else:
                selected = selected.merge(
                    class_df.drop(columns=[c for c in ["next_app_display_name", "vendor", "is_base_pool", "create_new_app"] if c in class_df.columns]),
                    on=[c for c in ["ADO_APP_RAW"] if c in selected.columns],
                    how="left",
                    suffixes=("", "_orig"),
                )
                if "vendor" in selected.columns:
                    selected["vendor"] = selected["vendor"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

                if "create_new_app" in selected.columns and "suggested_team_id" in selected.columns:
                    for _, r in selected.loc[selected["create_new_app"] == True].iterrows():
                        tid = str(r.get("suggested_team_id") or "").strip()
                        if tid and not _can_edit_team(tid):
                            _deny_scope("onboard applications", "team")
                            st.stop()
                if "create_new_app" in selected.columns and "existing_group_id" in selected.columns:
                    for _, r in selected.loc[selected["create_new_app"] != True].iterrows():
                        gid = str(r.get("existing_group_id") or "").strip()
                        if not gid:
                            continue
                        target = fetch_df("SELECT TOP 1 TEAMID FROM APPLICATION_GROUPS WHERE GROUPID=%s", (gid,))
                        tteam = str(target.iloc[0].get("TEAMID") or "").strip() if isinstance(target, pd.DataFrame) and not target.empty else ""
                        if tteam and not _can_edit_team(tteam):
                            _deny_scope("map ADO apps", "application")
                            st.stop()

                result = safe_onboard_next_apps(
                    fetch_df,
                    execute,
                    upsert_vendor,
                    upsert_application_group,
                    map_ado_app_to_app_group,
                    ensure_default_instances,
                    selected,
                    updated_by=str(user.get("email") or ""),
                )
                summary = (
                    f"Mapped: {result.mapped_rows} • Created apps: {result.created_groups} • "
                    f"Reused apps: {result.reused_groups} • Created vendors: {result.created_vendors}"
                )
                if result.errors:
                    _set_op_feedback(
                        "warning",
                        f"{summary} • Completed with {len(result.errors)} error(s).",
                        target="next_app_classification",
                    )
                else:
                    _set_op_feedback("success", summary, target="next_app_classification")
                _run_post_save_refresh("teams_next_app_classification_onboard")

        with st.expander("Debug – App Classification", expanded=False):
            st.write(
                {
                    "rows": int(len(class_df)),
                    "mapped_count": mapped_count,
                    "unmapped_count": unmapped_count,
                    "total_effort_points": float(total_effort),
                    "selected_rows": int(len(selected)) if isinstance(selected, pd.DataFrame) else 0,
                    "filters": {
                        "years": list(years_to_check),
                        "programs": list(program_filters),
                        "teams": list(team_filters),
                        "ado_program_keys": list(ado_program_scope_keys),
                        "ado_team_keys": list(ado_team_scope_keys),
                        "program_id": scope_program_id,
                        "team_id": scope_team_id,
                    },
                }
            )

if section == "Onboard":
    st.markdown("### Onboard")
    st.caption("Select an unmapped ADO candidate to create it in NEXT and link it automatically.")

    ado_years = _ado_years_cached()
    years_to_check = tuple(sorted(ado_years)[-3:]) if ado_years else (date.today().year,)
    program_pick_state = st.session_state.get("teams_program_pick")
    base_df = _load_ado_features_for_mapping_candidates_cached(years_to_check)
    ado_scope_df = base_df.copy()
    ado_all_df = base_df
    # Use the same normalized source as the page-level Program selector/checklist.
    programs_df_onboard = programs_df.copy() if isinstance(programs_df, pd.DataFrame) else pd.DataFrame()
    if ado_all_df is None or not isinstance(ado_all_df, pd.DataFrame) or ado_all_df.empty:
        st.caption("No ADO data yet. Run sync in Settings to discover candidates.")
    else:
        include_global_default = bool(st.session_state.get("teams_unmapped_include_global", False))
        if IS_SCOPED_CONTRIBUTOR and not is_admin:
            include_global = False
            st.session_state["teams_unmapped_include_global"] = False
            st.caption("Global candidates are hidden in scoped contributor mode.")
        else:
            include_global = st.checkbox(
                "Include global (outside scope)",
                value=include_global_default,
                key="teams_unmapped_include_global",
            )
        team_map_df = list_ado_team_mappings()
        if IS_SCOPED_CONTRIBUTOR and not is_admin:
            if not program_pick_state or program_pick_state == "(Select a program)":
                ado_scope_df = ado_scope_df.iloc[0:0].copy()
                st.info("Select an assigned Program to onboard teams.")
        if (
            program_pick_state
            and program_pick_state != "(Select a program)"
            and programs_df_onboard is not None
            and not programs_df_onboard.empty
        ):
            program_rows = _resolve_program_row(programs_df_onboard, str(program_pick_state))
            program_id = str(program_rows.iloc[0].get("PROGRAMID") or "") if not program_rows.empty else ""
            program_raw_name = str(program_rows.iloc[0].get("PROGRAMNAME_RAW") or "") if not program_rows.empty else ""
            prog_map = list_ado_program_mappings()
            scoped = False
            if prog_map is not None and not prog_map.empty and program_id:
                ado_programs = (
                    prog_map.loc[prog_map["PROGRAMID"].astype(str) == program_id, "ADO_PROGRAM"]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
                if ado_programs:
                    prog_keys = {p.strip().upper() for p in ado_programs if str(p).strip()}
                    prog_raw_u = ado_scope_df.get("PROGRAM_RAW", pd.Series(dtype=str)).astype(str).str.strip().str.upper()
                    team_key_u = ado_scope_df.get("ADO_TEAM_KEY", pd.Series(dtype=str)).astype(str).str.strip().str.upper()
                    mask = prog_raw_u.isin(prog_keys)
                    # Extra guard: allow hierarchical ADO team keys that begin with mapped program key.
                    for k in prog_keys:
                        mask = mask | team_key_u.str.startswith(f"{k}|")
                    ado_scope_df = ado_scope_df.loc[mask].copy()
                    scoped = True
            if not scoped and isinstance(ado_scope_df, pd.DataFrame) and not ado_scope_df.empty:
                # Fallback: canonical/raw NEXT program name match against ADO program fields.
                pick_u = str(program_raw_name or program_pick_state).strip().upper()
                if pick_u:
                    prog_raw_u = (
                        ado_scope_df.get("PROGRAM_RAW", pd.Series(dtype=str))
                        .astype(str)
                        .str.strip()
                        .str.upper()
                    )
                    area2_u = (
                        ado_scope_df.get("AREA_LEVEL2_RAW", pd.Series(dtype=str))
                        .astype(str)
                        .str.strip()
                        .str.upper()
                    )
                    mask = prog_raw_u.eq(pick_u) | area2_u.eq(pick_u)
                    if bool(mask.any()):
                        ado_scope_df = ado_scope_df.loc[mask].copy()
                        scoped = True
            if not scoped:
                # Fail closed: do not show all teams when a program is selected but unresolved.
                ado_scope_df = ado_scope_df.iloc[0:0].copy()
                st.info(
                    "No ADO Program mapping found for the selected Program. "
                    "Map the Program first in Settings → ADO → Mapping to scope Team onboarding."
                )
        scope_candidates = get_unmapped_ado_teams(ado_scope_df, team_map_df)
        global_candidates = get_unmapped_ado_teams(ado_all_df, team_map_df)
        if scope_candidates is None:
            scope_candidates = pd.DataFrame()
        if global_candidates is None:
            global_candidates = pd.DataFrame()
        scope_keys = set(scope_candidates.get("ADO_TEAM_KEY", pd.Series(dtype=str)).astype(str).tolist())
        global_candidates = global_candidates.copy()
        if not global_candidates.empty:
            global_candidates["IN_SCOPE"] = global_candidates["ADO_TEAM_KEY"].astype(str).isin(scope_keys)
            global_candidates = global_candidates.sort_values(
                ["IN_SCOPE", "STORY_POINTS_SUM"], ascending=[False, False], na_position="last"
            )
        display_df = global_candidates if include_global else scope_candidates

        if display_df is None or display_df.empty:
            st.caption("No unmapped ADO teams detected in this scope.")
        else:
            view_df = display_df.copy()
            view_df["__pick"] = False
            view_df["MAPPED_TO"] = ""
            rename = {
                "__pick": "Select",
                "ADO_TEAM_KEY": "ADO Team Key",
                "TEAM_RAW": "Team Raw",
                "FEATURE_COUNT": "Feature Count",
                "STORY_POINTS_SUM": "Story Points",
                "LAST_SEEN": "Last Seen",
                "MAPPED_TO": "Mapped To",
                "IN_SCOPE": "In scope?",
            }
            show_cols = ["__pick", "ADO_TEAM_KEY", "TEAM_RAW", "FEATURE_COUNT", "STORY_POINTS_SUM", "LAST_SEEN", "MAPPED_TO"]
            if include_global and "IN_SCOPE" in view_df.columns:
                show_cols.append("IN_SCOPE")
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
                key="team_onboard_pick",
            )
            selected_rows = pick[pick["Select"]].index.tolist() if "Select" in pick.columns else []
            selected_ado_key = str(pick.loc[selected_rows[-1], "ADO Team Key"]).strip() if selected_rows else ""
            selected_team_raw = str(pick.loc[selected_rows[-1], "Team Raw"]).strip() if selected_rows else ""

            if selected_ado_key:
                st.markdown("### Create Team from selected ADO team")
                canonical_team_name = (selected_team_raw or selected_ado_key)
                st.text_input(
                    "Selected ADO Team (raw)",
                    value=canonical_team_name,
                    key="team_onboard_name_raw",
                    disabled=True,
                )
                team_display_name_val = st.text_input(
                    "Team Display Name (required)",
                    value="",
                    key="team_onboard_display",
                )
                program_opts = ["(Select)"] + (
                    programs_df_onboard["PROGRAMNAME"].astype(str).tolist()
                    if programs_df_onboard is not None and not programs_df_onboard.empty
                    else []
                )
                program_default = program_pick_state
                program_index = program_opts.index(program_default) if program_default in program_opts else 0
                program_sel = st.selectbox(
                    "Program",
                    options=program_opts,
                    index=program_index,
                    key="team_onboard_program",
                )
                onboard_msp_enabled = st.checkbox(
                    "Is this an MSP team?",
                    value=False,
                    key="team_onboard_msp_enabled",
                    help=(
                        "MSP teams use MSP sizing/rate cards from Rates -> MSP. "
                        "Standard labor headcount rates are not applied to features mapped to this team."
                    ),
                )
                if onboard_msp_enabled:
                    st.info(
                        "MSP mode enabled: labor headcount/rates are ignored for this team's feature costs. "
                        "Costs come from MSP assignments and MSP size-based rates."
                    )
                if st.button("Create & map", icon=":material/add:", key="team_onboard_create_map"):
                    save_status = st.status("Creating team...", expanded=False) if hasattr(st, "status") else None
                    canonical_name = str(canonical_team_name or "").strip()
                    team_display_name = (team_display_name_val or "").strip()
                    if not canonical_name:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Team Name is required.")
                        st.stop()
                    if not team_display_name:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Team Display Name is required.")
                        st.stop()
                    if not program_sel or program_sel == "(Select)":
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Program is required.")
                        st.stop()
                    program_id = None
                    if programs_df_onboard is not None and not programs_df_onboard.empty:
                        rows = _resolve_program_row(programs_df_onboard, str(program_sel))
                        if not rows.empty:
                            program_id = str(rows.iloc[0].get("PROGRAMID") or "")
                    if not program_id:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Program is required.")
                        st.stop()
                    if not _can_edit_program(program_id):
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        _deny_scope("create and map teams", "program")
                        st.stop()
                    try:
                        team_id = create_team_from_ado(
                            selected_ado_key,
                            program_id=program_id,
                            name=canonical_name,
                            team_display_name=team_display_name,
                            updated_by=_user_display,
                        )
                        _upsert_team_msp_rate(str(team_id), bool(onboard_msp_enabled), None, None)
                        if save_status is not None:
                            save_status.update(label="Mapping ADO team key...", state="running")
                        map_ado_team_to_team([selected_ado_key], team_id)
                        st.session_state["teams_program_pick_pending"] = program_sel
                        st.session_state["teams_team_pick_pending"] = canonical_name
                        if save_status is not None:
                            save_status.update(label=f"Team created: {team_display_name}.", state="complete")
                        _run_post_save_refresh("teams_onboard_create_and_map")
                    except Exception as e:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error(f"Create failed: {e}")
            else:
                st.markdown("### Create team manually")
                manual_team_name = st.text_input("Team Name", value="", key="team_manual_name")
                manual_team_display_name = st.text_input(
                    "Team Display Name (required)",
                    value="",
                    key="team_manual_display",
                )
                program_opts = ["(Select)"] + (
                    programs_df_onboard["PROGRAMNAME"].astype(str).tolist()
                    if programs_df_onboard is not None and not programs_df_onboard.empty
                    else []
                )
                program_default = program_pick_state
                program_index = program_opts.index(program_default) if program_default in program_opts else 0
                manual_program = st.selectbox(
                    "Program",
                    options=program_opts,
                    index=program_index,
                    key="team_manual_program",
                )
                manual_msp_enabled = st.checkbox(
                    "Is this an MSP team?",
                    value=False,
                    key="team_manual_msp_enabled",
                    help=(
                        "MSP teams use MSP sizing/rate cards from Rates -> MSP. "
                        "Standard labor headcount rates are not applied to features mapped to this team."
                    ),
                )
                if manual_msp_enabled:
                    st.info(
                        "MSP mode enabled: labor headcount/rates are ignored for this team's feature costs. "
                        "Costs come from MSP assignments and MSP size-based rates."
                    )
                if st.button("Create team", icon=":material/add:", key="team_manual_create"):
                    save_status = st.status("Creating team...", expanded=False) if hasattr(st, "status") else None
                    canonical_name = (manual_team_name or "").strip()
                    team_display_name = (manual_team_display_name or "").strip()
                    if not canonical_name:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Team Name is required.")
                        st.stop()
                    if not team_display_name:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Team Display Name is required.")
                        st.stop()
                    if not manual_program or manual_program == "(Select)":
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Program is required.")
                        st.stop()
                    program_id = None
                    if programs_df_onboard is not None and not programs_df_onboard.empty:
                        rows = _resolve_program_row(programs_df_onboard, str(manual_program))
                        if not rows.empty:
                            program_id = str(rows.iloc[0].get("PROGRAMID") or "")
                    if not program_id:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error("Program is required.")
                        st.stop()
                    if not _can_edit_program(program_id):
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        _deny_scope("create teams", "program")
                        st.stop()
                    try:
                        team_id = create_team(
                            canonical_name,
                            program_id=program_id,
                            team_display_name=team_display_name,
                            product_owner=None,
                            updated_by=_user_display,
                        )
                        _upsert_team_msp_rate(str(team_id), bool(manual_msp_enabled), None, None)
                        st.session_state["teams_program_pick_pending"] = manual_program
                        st.session_state["teams_team_pick_pending"] = canonical_name
                        if save_status is not None:
                            save_status.update(label=f"Team created: {team_display_name}.", state="complete")
                        _run_post_save_refresh("teams_onboard_manual_create")
                    except Exception as e:
                        if save_status is not None:
                            save_status.update(label="Create failed.", state="error")
                        st.error(f"Create failed: {e}")

            if show_debug:
                with st.expander("Debug – ADO Team candidates", expanded=False):
                    st.write(
                        {
                            "columns": list(ado_all_df.columns) if isinstance(ado_all_df, pd.DataFrame) else [],
                            "program_pick": str(program_pick_state or ""),
                            "resolved_program_rows": int(len(program_rows)) if 'program_rows' in locals() and isinstance(program_rows, pd.DataFrame) else 0,
                            "scope_rows": int(len(scope_candidates)) if isinstance(scope_candidates, pd.DataFrame) else 0,
                            "global_rows": int(len(global_candidates)) if isinstance(global_candidates, pd.DataFrame) else 0,
                            "include_global": bool(include_global),
                            "unmapped_rows": int(len(display_df)) if display_df is not None else 0,
                        }
                    )

if section == "Headcount & Rates":
    st.markdown("### Headcount & Rates")
    st.caption("Edit headcount below. Click Save to recompute composition automatically.")

    st.divider()

    if team_row is None:
        st.info("Select a program and team above to edit headcount and view composition.")
    else:
        row = team_row
        team_id = str(row.get("TEAMID") or "")

        with st.container(border=True):
            st.markdown("##### Team details")
            last_by = row.get("UPDATED_BY") or "Unknown"
            st.caption(f"Last updated: {row.get('UPDATED_AT')} by {last_by}")

            cur_enabled, cur_rate, cur_size = _msp_row_for_team(team_id)
            edit_msp_enabled = st.checkbox("Is this team MSP?", value=cur_enabled, key="edit_msp_enabled")
            new_name = st.text_input("Team Name", value=str(row.get("TEAMNAME") or ""), key="edit_teamname")

            # Product Owner from Access Control (APP_USERS)
            users_df2 = _list_app_users_cached()
            if not users_df2.empty and "IS_ACTIVE" in users_df2.columns:
                is_active2 = users_df2["IS_ACTIVE"]
                if isinstance(is_active2, pd.DataFrame):
                    is_active2 = is_active2.iloc[:, 0]
                users_df2 = users_df2.loc[pd.to_numeric(is_active2, errors="coerce").fillna(1).astype(int) > 0].copy()

            def _label_row2(r):
                email = str(r.get("EMAIL") or "").strip()
                dn = str(r.get("DISPLAY_NAME") or "").strip()
                return f"{dn} ({email})" if dn else email

            opts2 = []
            if not users_df2.empty and "EMAIL" in users_df2.columns:
                opts2 = [(_label_row2(r), str(r["EMAIL"]).strip()) for _, r in users_df2.iterrows()]
            default_po = str(row.get("PRODUCTOWNER") or "").strip()
            labels2 = [lbl for (lbl, _) in opts2]
            label_to_email2 = {lbl: em for (lbl, em) in opts2}
            def_label = None
            if default_po:
                for lbl, em in opts2:
                    if em.lower() == default_po.lower():
                        def_label = lbl
                        break
                if def_label is None:
                    def_label = default_po
                    labels2 = [def_label] + labels2
                    label_to_email2[def_label] = default_po
            sel_lbl2 = st.selectbox(
                "Product Owner (required)",
                options=[""] + labels2,
                index=(labels2.index(def_label) + 1 if def_label in labels2 else 0),
                key="edit_product_owner",
            )
            new_product_owner = label_to_email2.get(sel_lbl2, "") if sel_lbl2 else ""
            st.caption("Owner assignment note: Product Owner ownership grants contributor edit scope automatically.")

            _render_op_feedback("team_edit")
            if st.button("Update Team", key="edit_update_btn"):
                save_status = st.status("Saving team...", expanded=False) if hasattr(st, "status") else None
                name = (new_name or "").strip()
                po = (new_product_owner or "").strip()
                if not _can_edit_team(team_id):
                    if save_status is not None:
                        save_status.update(label="Team save failed.", state="error")
                    _deny_scope("update team details", "team")
                elif not name:
                    if save_status is not None:
                        save_status.update(label="Team save failed.", state="error")
                    st.error("Team Name is required.")
                elif not po:
                    if save_status is not None:
                        save_status.update(label="Team save failed.", state="error")
                    st.error("Product Owner is required.")
                else:
                    existing_id = _team_id_for_name_ci(name)
                    if existing_id and existing_id != team_id:
                        if save_status is not None:
                            save_status.update(label="Team save failed.", state="error")
                        st.error(f"A Team named '{name}' already exists. Team names must be unique.")
                    else:
                        try:
                            # FTE fields on TEAMS are maintained for backward compatibility; composition comes from headcount/history.
                            new_fte = float(row.get("TEAMFTE") or 0.0)
                            new_delivery = float(row.get("DELIVERY_TEAM_FTE") or 0.0)
                            new_cc = float(row.get("CONTRACTOR_C_FTE") or 0.0)
                            new_cs = float(row.get("CONTRACTOR_CS_FTE") or 0.0)
                            if edit_msp_enabled:
                                new_fte = new_delivery = new_cc = new_cs = 0.0

                            execute(
                                """
                                UPDATE TEAMS
                                SET TEAMNAME=%s, TEAMFTE=%s, DELIVERY_TEAM_FTE=%s, CONTRACTOR_C_FTE=%s, CONTRACTOR_CS_FTE=%s,
                                    PRODUCTOWNER=%s,
                                    UPDATED_AT=SYSDATETIME(),
                                    UPDATED_BY=%s
                                WHERE TEAMID=%s
                                """,
                                (name, new_fte, new_delivery, new_cc, new_cs, po, _user_display, team_id),
                            )
                            _upsert_team_msp_rate(team_id, edit_msp_enabled, cur_size, cur_rate)
                            st.session_state["teams_team_pick_pending"] = name
                            if save_status is not None:
                                save_status.update(label=f"Team saved: {name}.", state="complete")
                            _set_op_feedback("success", f"Team updated: {name}.", target="team_edit")
                            st.success(f"Team updated: {name}.")
                            _run_post_save_refresh("teams_update_team_details")
                        except Exception as e:
                            if save_status is not None:
                                save_status.update(label="Team save failed.", state="error")
                            _set_op_feedback("error", f"Update failed: {e}", target="team_edit")
                            st.error(f"Update failed: {e}")

        if edit_msp_enabled:
            st.info("This is an MSP team. It does not use internal headcount/composition.")
        else:
            ensure_location_and_contractor_tables()
            loc_cols = ["GBC", "US-0970+HC", "US-0910", "OTHER"]

            st.markdown("##### Headcount editor")
            st.caption(
                "Edit values in the tables, then click Save headcount. Use All PIs to set PI=0 (year default), or Per PI to edit PI overrides (I1–I4)."
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
                key=f"hc_year_table_{team_id}",
            )
            year_i = int(hc_year_sel)

            selected_pis = [1, 2, 3, 4]

            # Canonical in-memory shape (keep all PI columns; PI multiselect only affects visibility + save behavior).
            staff_fixed_cols = [
                "CLASS",
                "LOCATION_KEY",
                "__ORIG_CLASS",
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

            cons_fixed_cols = [
                "CLASS",
                "PROVIDER",
                "__ORIG_CLASS",
                "__ORIG_COMPANYID",
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

            staff_editor_key = f"hc_editor_team_delivery_{team_id}_{year_i}_v2"
            staff_df_key = f"{staff_editor_key}__df"
            staff_ctx_key = f"{staff_editor_key}__ctx"
            cons_editor_key = f"hc_editor_team_contractor_{team_id}_{year_i}_v2"
            cons_df_key = f"{cons_editor_key}__df"
            cons_ctx_key = f"{cons_editor_key}__ctx"
            editor_meta_key = f"teams_hc_editor_meta_{team_id}"
            editor_ctx = (team_id, int(year_i))
            staff_init_df: Optional[pd.DataFrame] = None
            cons_init_df: Optional[pd.DataFrame] = None

            def _build_staff_df(
                *,
                hc_year_in: pd.DataFrame,
                staff_map_in: dict,
                staff_note_map_in: dict,
                default_loc_in: str,
                default_team_in: float,
                default_delivery_in: float,
                staff_fixed_cols_in: list[str],
            ) -> pd.DataFrame:
                keys: set[tuple[str, str]] = set()
                if hc_year_in is not None and not hc_year_in.empty:
                    for _, rr in hc_year_in.loc[hc_year_in["CLASS"].isin(["TEAM", "DELIVERY"])].iterrows():
                        cls_key = str(rr.get("CLASS") or "").strip().upper()
                        loc = str(rr.get("LOCATION") or "").strip()
                        if cls_key in ["TEAM", "DELIVERY"] and loc:
                            keys.add((cls_key, loc))
                for cls_key in ["TEAM", "DELIVERY"]:
                    if not any(k[0] == cls_key for k in keys):
                        keys.add((cls_key, default_loc_in))

                def _baseline_for(cls_key: str, loc: str) -> float:
                    v0 = staff_map_in.get((cls_key, loc, 0), None)
                    if v0 is not None:
                        return float(v0)
                    for pi in [1, 2, 3, 4]:
                        vv = staff_map_in.get((cls_key, loc, pi), None)
                        if vv is not None:
                            return float(vv)
                    class_has_any = any(k[0] == cls_key for k in staff_map_in.keys())
                    if not class_has_any:
                        return float(default_team_in if cls_key == "TEAM" else default_delivery_in)
                    return 0.0

                rows = []
                for cls_key, loc in sorted(keys, key=lambda x: (["TEAM", "DELIVERY"].index(x[0]), x[1])):
                    base = float(_baseline_for(cls_key, loc))
                    cls_label = "Team Overhead" if cls_key == "TEAM" else "Delivery"
                    rows.append(
                        {
                            "CLASS": cls_label,
                            "LOCATION_KEY": loc,
                            "__ORIG_CLASS": cls_label,
                            "__ORIG_LOCATION_KEY": loc,
                            "HC_ALL": base,
                            "HC_I1": float(staff_map_in.get((cls_key, loc, 1), base)),
                            "HC_I2": float(staff_map_in.get((cls_key, loc, 2), base)),
                            "HC_I3": float(staff_map_in.get((cls_key, loc, 3), base)),
                            "HC_I4": float(staff_map_in.get((cls_key, loc, 4), base)),
                            "NOTE_ALL": str(staff_note_map_in.get((cls_key, loc, 0), "")),
                            "NOTE_I1": str(staff_note_map_in.get((cls_key, loc, 1), "")),
                            "NOTE_I2": str(staff_note_map_in.get((cls_key, loc, 2), "")),
                            "NOTE_I3": str(staff_note_map_in.get((cls_key, loc, 3), "")),
                            "NOTE_I4": str(staff_note_map_in.get((cls_key, loc, 4), "")),
                        }
                    )
                return pd.DataFrame(rows, columns=staff_fixed_cols_in)

            def _build_cons_df(
                *,
                cons_year_in: pd.DataFrame,
                cons_map_in: dict,
                cons_note_map_in: dict,
                comp_id_to_name_in: dict,
                cons_fixed_cols_in: list[str],
            ) -> pd.DataFrame:
                keys: set[tuple[str, str]] = set()
                if cons_year_in is not None and not cons_year_in.empty:
                    for _, rr in cons_year_in.loc[cons_year_in["CLASS"].isin(["CONTRACTOR_C", "CONTRACTOR_CS"])].iterrows():
                        cls_key = str(rr.get("CLASS") or "").strip().upper()
                        cid = str(rr.get("COMPANYID") or "").strip()
                        if cls_key in ["CONTRACTOR_C", "CONTRACTOR_CS"] and cid:
                            keys.add((cls_key, cid))

                def _baseline_for(cls_key: str, cid: str) -> float:
                    v0 = cons_map_in.get((cls_key, cid, 0), None)
                    if v0 is not None:
                        return float(v0)
                    for pi in [1, 2, 3, 4]:
                        vv = cons_map_in.get((cls_key, cid, pi), None)
                        if vv is not None:
                            return float(vv)
                    return 0.0

                rows = []
                for cls_key, cid in sorted(keys, key=lambda x: (["CONTRACTOR_C", "CONTRACTOR_CS"].index(x[0]), x[1])):
                    provider = comp_id_to_name_in.get(str(cid), str(cid)) if cid else ""
                    base = float(_baseline_for(cls_key, cid))
                    cls_label = "Contractor C" if cls_key == "CONTRACTOR_C" else "Contractor CS"
                    rows.append(
                        {
                            "CLASS": cls_label,
                            "PROVIDER": provider,
                            "__ORIG_CLASS": cls_label,
                            "__ORIG_COMPANYID": str(cid),
                            "HC_ALL": base,
                            "HC_I1": float(cons_map_in.get((cls_key, cid, 1), base)),
                            "HC_I2": float(cons_map_in.get((cls_key, cid, 2), base)),
                            "HC_I3": float(cons_map_in.get((cls_key, cid, 3), base)),
                            "HC_I4": float(cons_map_in.get((cls_key, cid, 4), base)),
                            "NOTE_ALL": str(cons_note_map_in.get((cls_key, cid, 0), "")),
                            "NOTE_I1": str(cons_note_map_in.get((cls_key, cid, 1), "")),
                            "NOTE_I2": str(cons_note_map_in.get((cls_key, cid, 2), "")),
                            "NOTE_I3": str(cons_note_map_in.get((cls_key, cid, 3), "")),
                            "NOTE_I4": str(cons_note_map_in.get((cls_key, cid, 4), "")),
                        }
                    )
                return pd.DataFrame(rows, columns=cons_fixed_cols_in)

            editor_meta = st.session_state.get(editor_meta_key)
            if not isinstance(editor_meta, dict) or editor_meta.get("ctx") != editor_ctx:
                # ------------------------------------------------------------------
                # Auto-sync: keep derived composition (used by Budget/Capacity views)
                # consistent with the canonical headcount tables.
                #
                # Why: Budget capacity is driven by TEAM_COMPOSITION_HISTORY via
                # VW_TEAM_COMPOSITION_EFFECTIVE/VW_TEAM_ALLOCATED_HEADCOUNT_PI.
                # If headcount rows were loaded/imported externally or older logic
                # left stale PI-specific composition rows behind, Budget can differ
                # until the user hits "Save headcount". This lightweight sync runs
                # only when the editor context changes (team/year), not on every rerun.
                # ------------------------------------------------------------------
                try:
                    need_sync = False
                    df_comp = fetch_df(
                        """
                        SELECT PI, UPDATED_AT
                        FROM TEAM_COMPOSITION_HISTORY
                        WHERE TEAMID=%s AND YEAR=%s
                        """,
                        (team_id, int(year_i)),
                    )
                    if df_comp is None or df_comp.empty:
                        need_sync = True
                        comp_pis: set[int] = set()
                        comp_max = None
                    else:
                        comp_pi_col = pd.to_numeric(df_comp.get("PI"), errors="coerce").fillna(0).astype(int)
                        comp_pis = set(int(p) for p in comp_pi_col.tolist() if 1 <= int(p) <= 4)
                        comp_max = pd.to_datetime(df_comp.get("UPDATED_AT"), errors="coerce").max()

                    # Determine which PIs have explicit overrides in the canonical tables (PI 1..4).
                    df_hp = fetch_df(
                        """
                        SELECT DISTINCT PI
                        FROM TEAM_HEADCOUNT_HISTORY
                        WHERE TEAMID=%s AND YEAR=%s AND TRY_CONVERT(INT, PI) BETWEEN 1 AND 4
                        """,
                        (team_id, int(year_i)),
                    )
                    headcount_override_pis = set()
                    if df_hp is not None and not df_hp.empty and "PI" in df_hp.columns:
                        headcount_override_pis = set(
                            int(p)
                            for p in pd.to_numeric(df_hp["PI"], errors="coerce").dropna().astype(int).tolist()
                            if 1 <= int(p) <= 4
                        )
                    df_cp = fetch_df(
                        """
                        SELECT DISTINCT PI
                        FROM TEAM_CONTRACTOR_HEADCOUNT
                        WHERE TEAMID=%s AND YEAR=%s AND TRY_CONVERT(INT, PI) BETWEEN 1 AND 4
                        """,
                        (team_id, int(year_i)),
                    )
                    contractor_override_pis = set()
                    if df_cp is not None and not df_cp.empty and "PI" in df_cp.columns:
                        contractor_override_pis = set(
                            int(p)
                            for p in pd.to_numeric(df_cp["PI"], errors="coerce").dropna().astype(int).tolist()
                            if 1 <= int(p) <= 4
                        )
                    desired_override_pis = headcount_override_pis | contractor_override_pis

                    # If composition has PI rows that no longer have overrides, it can override PI=0 expansion.
                    if comp_pis and (comp_pis - desired_override_pis):
                        need_sync = True

                    # If headcount is newer than composition, refresh composition.
                    if not need_sync:
                        df_src = fetch_df(
                            """
                            SELECT MAX(UPDATED_AT) AS SRC_MAX
                            FROM (
                              SELECT UPDATED_AT FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID=%s AND YEAR=%s
                              UNION ALL
                              SELECT UPDATED_AT FROM TEAM_CONTRACTOR_HEADCOUNT WHERE TEAMID=%s AND YEAR=%s
                            ) s
                            """,
                            (team_id, int(year_i), team_id, int(year_i)),
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
                        recompute_team_composition_from_headcount_year(
                            team_id=team_id,
                            year=int(year_i),
                            changed_by=str(user.get("email") or ""),
                            source="HEADCOUNT_AUTO_SYNC",
                        )
                        _sync_team_baseline_from_history(team_id)
                except Exception:
                    pass

                hc_all = _list_team_headcount_cached(team_id)
                hc_all = hc_all.copy() if hc_all is not None and not hc_all.empty else pd.DataFrame()
                if not hc_all.empty:
                    hc_all["YEAR_NUM"] = pd.to_numeric(hc_all.get("YEAR"), errors="coerce").fillna(0).astype(int)
                    hc_all["PI_NUM"] = pd.to_numeric(hc_all.get("PI"), errors="coerce").fillna(0).astype(int)
                    hc_class = hc_all["CLASS"] if "CLASS" in hc_all.columns else pd.Series([""] * len(hc_all), index=hc_all.index)
                    hc_all["CLASS"] = hc_class.astype(str).str.strip().str.upper()
                    hc_loc = hc_all["LOCATION"] if "LOCATION" in hc_all.columns else pd.Series([""] * len(hc_all), index=hc_all.index)
                    hc_all["LOCATION"] = hc_loc.astype(str).str.strip()
                    hc_all["HEADCOUNT"] = pd.to_numeric(hc_all.get("HEADCOUNT"), errors="coerce").fillna(0.0)
                    if "NOTES" in hc_all.columns:
                        hc_all["NOTES"] = hc_all.get("NOTES", "").fillna("").astype(str).str.strip()

                cons_all = _list_team_contractor_headcount_cached(team_id)
                cons_all = cons_all.copy() if cons_all is not None and not cons_all.empty else pd.DataFrame()
                if not cons_all.empty:
                    cons_all["YEAR_NUM"] = pd.to_numeric(cons_all.get("YEAR"), errors="coerce").fillna(0).astype(int)
                    cons_all["PI_NUM"] = pd.to_numeric(cons_all.get("PI"), errors="coerce").fillna(0).astype(int)
                    class_col = cons_all["CLASS"] if "CLASS" in cons_all.columns else pd.Series([""] * len(cons_all), index=cons_all.index)
                    cons_all["CLASS"] = class_col.astype(str).str.strip().str.upper()
                    company_col = cons_all["COMPANYID"] if "COMPANYID" in cons_all.columns else pd.Series([""] * len(cons_all), index=cons_all.index)
                    cons_all["COMPANYID"] = company_col.astype(str).str.strip()
                    cons_all["HEADCOUNT"] = pd.to_numeric(cons_all.get("HEADCOUNT"), errors="coerce").fillna(0.0)
                    if "NOTES" in cons_all.columns:
                        cons_all["NOTES"] = cons_all.get("NOTES", "").fillna("").astype(str).str.strip()

                hc_year = hc_all.loc[hc_all["YEAR_NUM"] == int(year_i)].copy() if not hc_all.empty else pd.DataFrame()
                cons_year = cons_all.loc[cons_all["YEAR_NUM"] == int(year_i)].copy() if not cons_all.empty else pd.DataFrame()

                # Always define location/provider options (even if no headcount exists yet) so editors can render.
                loc_opts = set(loc_cols)
                if not hc_year.empty:
                    loc_series = hc_year["LOCATION"] if "LOCATION" in hc_year.columns else pd.Series([""] * len(hc_year), index=hc_year.index)
                    loc_opts.update([str(x).strip() for x in loc_series.astype(str).tolist() if str(x).strip()])
                try:
                    global_locs = fetch_df("SELECT DISTINCT LOCATION FROM VW_GLOBAL_RATE_EFFECTIVE WHERE YEAR=%s", (int(year_i),))
                    if global_locs is not None and not global_locs.empty and "LOCATION" in global_locs.columns:
                        loc_opts.update([str(x).strip() for x in global_locs["LOCATION"].astype(str).tolist() if str(x).strip()])
                except Exception:
                    pass
                location_options = [""] + [l for l in loc_cols if l in loc_opts] + sorted([l for l in loc_opts if l and l not in loc_cols])

                comp_df = _list_contractor_companies_cached()
                comp_df = comp_df.copy() if comp_df is not None and not comp_df.empty else pd.DataFrame()
                if comp_df is not None and not comp_df.empty and "ACTIVE" in comp_df.columns:
                    comp_df = comp_df.loc[pd.to_numeric(comp_df["ACTIVE"], errors="coerce").fillna(1).astype(int) > 0].copy()
                comp_id_to_name: dict[str, str] = {}
                if comp_df is not None and not comp_df.empty and "COMPANYID" in comp_df.columns and "NAME" in comp_df.columns:
                    comp_id_to_name = {
                        str(r["COMPANYID"]).strip(): str(r.get("NAME") or "").strip()
                        for _, r in comp_df.iterrows()
                        if str(r.get("COMPANYID") or "").strip()
                    }

                provider_label_to_id: dict[str, str] = {name: cid for cid, name in comp_id_to_name.items() if name}
                if cons_year is not None and not cons_year.empty and "COMPANYID" in cons_year.columns:
                    for cid in cons_year["COMPANYID"].dropna().astype(str).tolist():
                        cid = str(cid or "").strip()
                        if cid and cid not in provider_label_to_id.values():
                            provider_label_to_id[cid] = cid
                provider_options = [""] + sorted(provider_label_to_id.keys(), key=lambda s: str(s).lower())

                comp_defaults = _composition_for_year(team_id, year_i) if team_id else {}
                default_team = float(comp_defaults.get("TEAMFTE") or 0.0) if comp_defaults else 0.0
                default_delivery = float(comp_defaults.get("DELIVERY_TEAM_FTE") or 0.0) if comp_defaults else 0.0

                staff_map: dict[tuple[str, str, int], float] = {}
                staff_note_map: dict[tuple[str, str, int], str] = {}
                if not hc_year.empty:
                    staff_src = hc_year.loc[hc_year["CLASS"].isin(["TEAM", "DELIVERY"]) & hc_year["PI_NUM"].isin([0, 1, 2, 3, 4])].copy()
                    for _, rr in staff_src.iterrows():
                        cls_key = str(rr.get("CLASS") or "").strip().upper()
                        loc = str(rr.get("LOCATION") or "").strip()
                        pi = int(rr.get("PI_NUM") or 0)
                        if not cls_key or not loc:
                            continue
                        staff_map[(cls_key, loc, pi)] = staff_map.get((cls_key, loc, pi), 0.0) + float(rr.get("HEADCOUNT") or 0.0)
                        if "NOTES" in staff_src.columns:
                            note = str(rr.get("NOTES") or "").strip()
                            if note:
                                staff_note_map[(cls_key, loc, pi)] = note

                cons_map: dict[tuple[str, str, int], float] = {}
                cons_note_map: dict[tuple[str, str, int], str] = {}
                if not cons_year.empty:
                    cons_src = cons_year.loc[
                        cons_year["CLASS"].isin(["CONTRACTOR_C", "CONTRACTOR_CS"]) & cons_year["PI_NUM"].isin([0, 1, 2, 3, 4])
                    ].copy()
                    for _, rr in cons_src.iterrows():
                        cls_key = str(rr.get("CLASS") or "").strip().upper()
                        cid = str(rr.get("COMPANYID") or "").strip()
                        pi = int(rr.get("PI_NUM") or 0)
                        if not cls_key or not cid:
                            continue
                        cons_map[(cls_key, cid, pi)] = cons_map.get((cls_key, cid, pi), 0.0) + float(rr.get("HEADCOUNT") or 0.0)
                        if "NOTES" in cons_src.columns:
                            note = str(rr.get("NOTES") or "").strip()
                            if note:
                                cons_note_map[(cls_key, cid, pi)] = note

                staff_location_options = [l for l in location_options if str(l).strip()]
                default_loc = staff_location_options[0] if staff_location_options else "GBC"

                staff_init_df = _build_staff_df(
                    hc_year_in=hc_year,
                    staff_map_in=staff_map,
                    staff_note_map_in=staff_note_map,
                    default_loc_in=default_loc,
                    default_team_in=default_team,
                    default_delivery_in=default_delivery,
                    staff_fixed_cols_in=staff_fixed_cols,
                )
                cons_init_df = _build_cons_df(
                    cons_year_in=cons_year,
                    cons_map_in=cons_map,
                    cons_note_map_in=cons_note_map,
                    comp_id_to_name_in=comp_id_to_name,
                    cons_fixed_cols_in=cons_fixed_cols,
                )

            if not isinstance(editor_meta, dict) or editor_meta.get("ctx") != editor_ctx:
                st.session_state[editor_meta_key] = {
                    "ctx": editor_ctx,
                    "hc_year": hc_year,
                    "cons_year": cons_year,
                    "location_options": location_options,
                    "provider_options": provider_options,
                    "provider_label_to_id": provider_label_to_id,
                    "comp_id_to_name": comp_id_to_name,
                    "default_team": default_team,
                    "default_delivery": default_delivery,
                    "staff_map": staff_map,
                    "cons_map": cons_map,
                    "staff_note_map": staff_note_map,
                    "cons_note_map": cons_note_map,
                    "staff_notes_enabled": bool("NOTES" in hc_all.columns) if hc_all is not None else False,
                    "cons_notes_enabled": bool("NOTES" in cons_all.columns) if cons_all is not None else False,
                }

            editor_meta = st.session_state.get(editor_meta_key, {}) if isinstance(st.session_state.get(editor_meta_key), dict) else {}
            hc_year = editor_meta.get("hc_year", pd.DataFrame())
            cons_year = editor_meta.get("cons_year", pd.DataFrame())
            location_options = editor_meta.get("location_options", [""] + list(loc_cols))
            provider_options = editor_meta.get("provider_options", [""])
            provider_label_to_id = editor_meta.get("provider_label_to_id", {}) or {}
            comp_id_to_name = editor_meta.get("comp_id_to_name", {}) or {}
            default_team = float(editor_meta.get("default_team", 0.0) or 0.0)
            default_delivery = float(editor_meta.get("default_delivery", 0.0) or 0.0)
            staff_map = editor_meta.get("staff_map", {}) or {}
            cons_map = editor_meta.get("cons_map", {}) or {}
            staff_note_map = editor_meta.get("staff_note_map", {}) or {}
            cons_note_map = editor_meta.get("cons_note_map", {}) or {}
            staff_notes_enabled = bool(editor_meta.get("staff_notes_enabled"))
            cons_notes_enabled = bool(editor_meta.get("cons_notes_enabled"))
            staff_has_overrides = any(int(k[2]) in [1, 2, 3, 4] for k in staff_map.keys()) if isinstance(staff_map, dict) else False
            cons_has_overrides = any(int(k[2]) in [1, 2, 3, 4] for k in cons_map.keys()) if isinstance(cons_map, dict) else False

            if staff_init_df is None:
                staff_location_options = [l for l in location_options if str(l).strip()]
                default_loc = staff_location_options[0] if staff_location_options else "GBC"
                staff_init_df = _build_staff_df(
                    hc_year_in=hc_year,
                    staff_map_in=staff_map,
                    staff_note_map_in=staff_note_map,
                    default_loc_in=default_loc,
                    default_team_in=default_team,
                    default_delivery_in=default_delivery,
                    staff_fixed_cols_in=staff_fixed_cols,
                )
            if cons_init_df is None:
                cons_init_df = _build_cons_df(
                    cons_year_in=cons_year,
                    cons_map_in=cons_map,
                    cons_note_map_in=cons_note_map,
                    comp_id_to_name_in=comp_id_to_name,
                    cons_fixed_cols_in=cons_fixed_cols,
                )

            staff_full0 = st.session_state.get(staff_df_key)
            if st.session_state.get(staff_ctx_key) != editor_ctx:
                st.session_state.pop(staff_editor_key, None)
                st.session_state[staff_ctx_key] = editor_ctx
                st.session_state[f"{staff_editor_key}__init"] = staff_init_df
                st.session_state[staff_df_key] = staff_init_df.copy()
                staff_full0 = staff_init_df
            if not isinstance(staff_full0, pd.DataFrame):
                staff_full0 = st.session_state.get(f"{staff_editor_key}__init")
            if not isinstance(staff_full0, pd.DataFrame):
                staff_full0 = pd.DataFrame(columns=staff_fixed_cols)
            if not isinstance(st.session_state.get(staff_df_key), pd.DataFrame):
                st.session_state[staff_df_key] = staff_full0.copy()

            cons_full0 = st.session_state.get(cons_df_key)
            if st.session_state.get(cons_ctx_key) != editor_ctx:
                st.session_state.pop(cons_editor_key, None)
                st.session_state[cons_ctx_key] = editor_ctx
                st.session_state[f"{cons_editor_key}__init"] = cons_init_df
                st.session_state[cons_df_key] = cons_init_df.copy()
                cons_full0 = cons_init_df
            if not isinstance(cons_full0, pd.DataFrame):
                cons_full0 = st.session_state.get(f"{cons_editor_key}__init")
            if not isinstance(cons_full0, pd.DataFrame):
                cons_full0 = pd.DataFrame(columns=cons_fixed_cols)
            if not isinstance(st.session_state.get(cons_df_key), pd.DataFrame):
                st.session_state[cons_df_key] = cons_full0.copy()

            # Keep options stable while editing; include current table values so selectboxes don't blank out.
            if staff_full0 is not None and not staff_full0.empty and "LOCATION_KEY" in staff_full0.columns:
                location_options = list(
                    dict.fromkeys(location_options + [str(x).strip() for x in staff_full0["LOCATION_KEY"].dropna().astype(str).tolist() if str(x).strip()])
                )
            if cons_full0 is not None and not cons_full0.empty and "PROVIDER" in cons_full0.columns:
                provider_options = list(
                    dict.fromkeys(provider_options + [str(x).strip() for x in cons_full0["PROVIDER"].dropna().astype(str).tolist() if str(x).strip()])
                )
            st.session_state[editor_meta_key] = {**editor_meta, "location_options": location_options, "provider_options": provider_options}

            with st.form(key=f"teams_hc_form_{team_id}_{year_i}", clear_on_submit=False):
                staff_mode, staff_edited = render_team_headcount_editor(
                    team_id=team_id,
                    year_i=year_i,
                    staff_fixed_cols=staff_fixed_cols,
                    initial_df=staff_full0 if isinstance(staff_full0, pd.DataFrame) else pd.DataFrame(columns=staff_fixed_cols),
                    staff_has_overrides=staff_has_overrides,
                    selected_pis=selected_pis,
                    location_options=location_options,
                    staff_notes_enabled=staff_notes_enabled,
                )

                cons_mode, cons_edited = render_contractor_headcount_editor(
                    team_id=team_id,
                    year_i=year_i,
                    cons_fixed_cols=cons_fixed_cols,
                    initial_df=cons_full0 if isinstance(cons_full0, pd.DataFrame) else pd.DataFrame(columns=cons_fixed_cols),
                    cons_has_overrides=cons_has_overrides,
                    selected_pis=selected_pis,
                    provider_options=provider_options,
                    cons_notes_enabled=cons_notes_enabled,
                )

                btn1, btn2 = st.columns([1, 1])
                save_btn = btn1.form_submit_button("Save headcount", type="primary")
                reset_btn = btn2.form_submit_button("Reset")
                st.caption("Click Save headcount to apply current table edits and persist to DB.")
            _render_op_feedback("headcount")
            if reset_btn:
                for k in [
                    editor_meta_key,
                    staff_editor_key,
                    staff_df_key,
                    staff_ctx_key,
                    f"{staff_editor_key}__init",
                    cons_editor_key,
                    cons_df_key,
                    cons_ctx_key,
                    f"{cons_editor_key}__init",
                ]:
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()

            if save_btn:
                save_status = st.status("Saving headcount...", expanded=False) if hasattr(st, "status") else None
                try:
                    if not _can_edit_team(team_id):
                        if save_status is not None:
                            save_status.update(label="Headcount save failed.", state="error")
                        _deny_scope("save headcount", "team")
                        st.stop()
                    staff_df = st.session_state.get(staff_df_key, staff_edited).copy() if isinstance(staff_edited, pd.DataFrame) else pd.DataFrame(columns=staff_fixed_cols)
                    cons_df = st.session_state.get(cons_df_key, cons_edited).copy() if isinstance(cons_edited, pd.DataFrame) else pd.DataFrame(columns=cons_fixed_cols)

                    # Optional NOTES support (schema-dependent): if the underlying headcount tables
                    # have NOTES/NOTE columns, we expose NOTE_* columns in the editors and persist
                    # them alongside headcount. If the DB doesn't have the column, notes remain hidden.
                    editor_meta_live = st.session_state.get(editor_meta_key, {}) if editor_meta_key in st.session_state else {}
                    staff_note_map = editor_meta_live.get("staff_note_map", {}) or {}
                    cons_note_map = editor_meta_live.get("cons_note_map", {}) or {}

                    def _update_team_headcount_note(*, pi: int, cls: str, loc: str, note: str) -> None:
                        if not staff_notes_enabled:
                            return
                        note_val = (note or "").strip()
                        # Try NOTES, then NOTE; tolerate missing UPDATED_BY.
                        for col in ["NOTES", "NOTE"]:
                            try:
                                execute(
                                    f"""
                                    UPDATE TEAM_HEADCOUNT_HISTORY
                                    SET {col}=%s, UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(%s, UPDATED_BY)
                                    WHERE TEAMID=%s AND YEAR=%s AND ISNULL(PI,0)=%s AND CLASS=%s AND LOCATION=%s
                                    """,
                                    (note_val, _user_display, team_id, int(year_i), int(pi), str(cls), str(loc)),
                                )
                                return
                            except Exception:
                                try:
                                    execute(
                                        f"""
                                        UPDATE TEAM_HEADCOUNT_HISTORY
                                        SET {col}=%s, UPDATED_AT=SYSDATETIME()
                                        WHERE TEAMID=%s AND YEAR=%s AND ISNULL(PI,0)=%s AND CLASS=%s AND LOCATION=%s
                                        """,
                                        (note_val, team_id, int(year_i), int(pi), str(cls), str(loc)),
                                    )
                                    return
                                except Exception:
                                    continue

                    def _update_team_contractor_note(*, pi: int, cls: str, company_id: str, note: str) -> None:
                        if not cons_notes_enabled:
                            return
                        note_val = (note or "").strip()
                        for col in ["NOTES", "NOTE"]:
                            try:
                                execute(
                                    f"""
                                    UPDATE TEAM_CONTRACTOR_HEADCOUNT
                                    SET {col}=%s, UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(%s, UPDATED_BY)
                                    WHERE TEAMID=%s AND YEAR=%s AND ISNULL(PI,0)=%s AND CLASS=%s AND COMPANYID=%s
                                    """,
                                    (note_val, _user_display, team_id, int(year_i), int(pi), str(cls), str(company_id)),
                                )
                                return
                            except Exception:
                                try:
                                    execute(
                                        f"""
                                        UPDATE TEAM_CONTRACTOR_HEADCOUNT
                                        SET {col}=%s, UPDATED_AT=SYSDATETIME()
                                        WHERE TEAMID=%s AND YEAR=%s AND ISNULL(PI,0)=%s AND CLASS=%s AND COMPANYID=%s
                                        """,
                                        (note_val, team_id, int(year_i), int(pi), str(cls), str(company_id)),
                                    )
                                    return
                                except Exception:
                                    continue

                    existing_staff_keys = {(cls, loc) for (cls, loc, _pi) in staff_map.keys()}
                    existing_cons_keys = {(cls, cid) for (cls, cid, _pi) in cons_map.keys()}

                    def _staff_cls_key(lbl: str) -> str:
                        s = (lbl or "").strip()
                        if s == "Team Overhead":
                            return "TEAM"
                        if s == "Delivery":
                            return "DELIVERY"
                        return ""

                    def _cons_cls_key(lbl: str) -> str:
                        s = (lbl or "").strip()
                        if s == "Contractor C":
                            return "CONTRACTOR_C"
                        if s == "Contractor CS":
                            return "CONTRACTOR_CS"
                        return ""

                    def _s(v: object) -> str:
                        if v is None or pd.isna(v):
                            return ""
                        return str(v).strip()

                    def _num(v: object) -> float:
                        x = pd.to_numeric(v, errors="coerce")
                        if x is None or pd.isna(x):
                            return 0.0
                        return float(x)

                    # -------------------------
                    # TEAM + DELIVERY (diff by (CLASS, LOCATION_KEY))
                    # -------------------------
                    staff_rows = []
                    for _, rr in (staff_df if staff_df is not None else pd.DataFrame()).iterrows():
                        cls_lbl = _s(rr.get("CLASS"))
                        loc = _s(rr.get("LOCATION_KEY"))
                        orig_cls_lbl = _s(rr.get("__ORIG_CLASS"))
                        orig_loc = _s(rr.get("__ORIG_LOCATION_KEY"))

                        hc_all = _num(rr.get("HC_ALL"))
                        pi_vals_all = {pi: _num(rr.get(f"HC_I{pi}")) for pi in [1, 2, 3, 4]}
                        note_all = _s(rr.get("NOTE_ALL")) if staff_notes_enabled else ""
                        pi_notes_all = {pi: _s(rr.get(f"NOTE_I{pi}")) for pi in [1, 2, 3, 4]} if staff_notes_enabled else {}

                        if staff_mode == "All PIs same":
                            any_nonzero = hc_all != 0.0
                            any_note = bool(note_all.strip()) if staff_notes_enabled else False
                        else:
                            any_nonzero = any(pi_vals_all[pi] != 0.0 for pi in selected_pis)
                            any_note = (
                                any(bool((pi_notes_all.get(pi) or "").strip()) for pi in selected_pis) if staff_notes_enabled else False
                            )
                        is_blank_row = (
                            (not cls_lbl)
                            and (not loc)
                            and (not orig_cls_lbl)
                            and (not orig_loc)
                            and (not any_nonzero)
                            and (not any_note)
                        )
                        if is_blank_row:
                            continue

                        if orig_cls_lbl and (not cls_lbl or not loc):
                            raise ValueError("To remove an existing Team/Delivery row, delete the row in the table (don’t clear Class/Location).")
                        if not cls_lbl or not loc:
                            if any_nonzero or any_note:
                                raise ValueError("Class and Location are required for Team/Delivery rows with headcount.")
                            continue

                        cls_key = _staff_cls_key(cls_lbl)
                        if not cls_key:
                            raise ValueError(f"Invalid class '{cls_lbl}' for Team/Delivery.")

                        staff_rows.append(
                            {
                                "cls_key": cls_key,
                                "loc": loc,
                                "hc_all": hc_all,
                                "pi_vals": pi_vals_all,
                                "note_all": note_all,
                                "pi_notes": pi_notes_all,
                                "orig_cls_key": _staff_cls_key(orig_cls_lbl) if orig_cls_lbl else "",
                                "orig_loc": orig_loc,
                            }
                        )

                    new_staff_keys = {(r["cls_key"], r["loc"]) for r in staff_rows}
                    if len(new_staff_keys) != len(staff_rows):
                        raise ValueError("Duplicate Team/Delivery row (same Class + Location).")

                    renamed_staff_orig: set[tuple[str, str]] = set()
                    for r in staff_rows:
                        if not r["orig_cls_key"] or not r["orig_loc"]:
                            continue
                        orig_key = (r["orig_cls_key"], r["orig_loc"])
                        new_key = (r["cls_key"], r["loc"])
                        if orig_key in existing_staff_keys and orig_key != new_key:
                            # When in Per PI mode, preserve PI values the user didn't edit (hidden PI columns).
                            # In All PIs mode, we intentionally clear overrides and do not carry them over.
                            if staff_mode == "Per PI":
                                for pi in [1, 2, 3, 4]:
                                    if pi in selected_pis:
                                        continue
                                    old_v = staff_map.get((r["orig_cls_key"], r["orig_loc"], int(pi)), None)
                                    if old_v is None:
                                        continue
                                    upsert_team_headcount(
                                        team_id,
                                        int(year_i),
                                        int(pi),
                                        r["cls_key"],
                                        r["loc"],
                                        float(old_v),
                                        updated_by=_user_display,
                                    )
                                    old_note = str(staff_note_map.get((r["orig_cls_key"], r["orig_loc"], int(pi)), "") or "").strip()
                                    if old_note:
                                        _update_team_headcount_note(pi=int(pi), cls=r["cls_key"], loc=r["loc"], note=old_note)
                            execute(
                                "DELETE FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID=%s AND YEAR=%s AND CLASS=%s AND LOCATION=%s",
                                (team_id, int(year_i), r["orig_cls_key"], r["orig_loc"]),
                            )
                            renamed_staff_orig.add(orig_key)

                    deleted_staff_keys = (existing_staff_keys - new_staff_keys) - renamed_staff_orig
                    for cls_key, loc in deleted_staff_keys:
                        execute(
                            "DELETE FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID=%s AND YEAR=%s AND CLASS=%s AND LOCATION=%s",
                            (team_id, int(year_i), cls_key, loc),
                        )

                    for r in staff_rows:
                        upsert_team_headcount(
                            team_id,
                            int(year_i),
                            0,
                            r["cls_key"],
                            r["loc"],
                            float(r["hc_all"]),
                            updated_by=_user_display,
                        )
                        if staff_notes_enabled:
                            _update_team_headcount_note(pi=0, cls=r["cls_key"], loc=r["loc"], note=str(r.get("note_all") or ""))
                        if staff_mode == "All PIs same":
                            # Clear PI-specific overrides so PI=0 applies to all PIs again.
                            for pi in [1, 2, 3, 4]:
                                execute(
                                    "DELETE FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID=%s AND YEAR=%s AND PI=%s AND CLASS=%s AND LOCATION=%s",
                                    (team_id, int(year_i), int(pi), r["cls_key"], r["loc"]),
                                )
                        else:
                            for pi in selected_pis:
                                hc_pi = float(r["pi_vals"].get(int(pi), r["hc_all"]))
                                note_pi = str((r.get("pi_notes") or {}).get(int(pi), "") or "").strip() if staff_notes_enabled else ""
                                if hc_pi == float(r["hc_all"]) and not note_pi:
                                    execute(
                                        "DELETE FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID=%s AND YEAR=%s AND PI=%s AND CLASS=%s AND LOCATION=%s",
                                        (team_id, int(year_i), int(pi), r["cls_key"], r["loc"]),
                                    )
                                else:
                                    upsert_team_headcount(
                                        team_id,
                                        int(year_i),
                                        int(pi),
                                        r["cls_key"],
                                        r["loc"],
                                        float(hc_pi),
                                        updated_by=_user_display,
                                    )
                                    if staff_notes_enabled:
                                        _update_team_headcount_note(pi=int(pi), cls=r["cls_key"], loc=r["loc"], note=note_pi)

                    # -------------------------
                    # CONTRACTORS (diff by (CLASS, PROVIDER))
                    # -------------------------
                    cons_rows = []
                    for _, rr in (cons_df if cons_df is not None else pd.DataFrame()).iterrows():
                        cls_lbl = _s(rr.get("CLASS"))
                        provider_lbl = _s(rr.get("PROVIDER"))
                        orig_cls_lbl = _s(rr.get("__ORIG_CLASS"))
                        orig_cid = _s(rr.get("__ORIG_COMPANYID"))

                        hc_all = _num(rr.get("HC_ALL"))
                        pi_vals_all = {pi: _num(rr.get(f"HC_I{pi}")) for pi in [1, 2, 3, 4]}
                        note_all = _s(rr.get("NOTE_ALL")) if cons_notes_enabled else ""
                        pi_notes_all = {pi: _s(rr.get(f"NOTE_I{pi}")) for pi in [1, 2, 3, 4]} if cons_notes_enabled else {}
                        if cons_mode == "All PIs same":
                            any_nonzero = hc_all != 0.0
                            any_note = bool(note_all.strip()) if cons_notes_enabled else False
                        else:
                            any_nonzero = any(pi_vals_all[pi] != 0.0 for pi in selected_pis)
                            any_note = any(bool((pi_notes_all.get(pi) or "").strip()) for pi in selected_pis) if cons_notes_enabled else False
                        is_blank_row = (
                            (not cls_lbl)
                            and (not provider_lbl)
                            and (not orig_cls_lbl)
                            and (not orig_cid)
                            and (not any_nonzero)
                            and (not any_note)
                        )
                        if is_blank_row:
                            continue

                        if orig_cid and (not cls_lbl or not provider_lbl):
                            raise ValueError("To remove an existing Contractor row, delete the row in the table (don’t clear Class/Provider).")

                        if not cls_lbl or not provider_lbl:
                            if any_nonzero or any_note:
                                raise ValueError("Class and Provider are required for Contractor rows with headcount.")
                            continue

                        cls_key = _cons_cls_key(cls_lbl)
                        if not cls_key:
                            raise ValueError(f"Invalid contractor class '{cls_lbl}'.")

                        cid = provider_label_to_id.get(provider_lbl, "") if provider_lbl else ""
                        if not cid:
                            if any_nonzero:
                                raise ValueError(f"Provider is required for {cls_lbl} when headcount is non-zero.")
                            continue

                        cons_rows.append(
                            {
                                "cls_key": cls_key,
                                "cid": str(cid),
                                "provider": provider_lbl,
                                "hc_all": hc_all,
                                "pi_vals": pi_vals_all,
                                "note_all": note_all,
                                "pi_notes": pi_notes_all,
                                "orig_cls_key": _cons_cls_key(orig_cls_lbl) if orig_cls_lbl else "",
                                "orig_cid": orig_cid,
                            }
                        )

                    new_cons_keys = {(r["cls_key"], r["cid"]) for r in cons_rows}
                    if len(new_cons_keys) != len(cons_rows):
                        raise ValueError("Duplicate Contractor row (same Class + Provider).")

                    renamed_cons_orig: set[tuple[str, str]] = set()
                    for r in cons_rows:
                        if not r["orig_cls_key"] or not r["orig_cid"]:
                            continue
                        orig_key = (r["orig_cls_key"], r["orig_cid"])
                        new_key = (r["cls_key"], r["cid"])
                        if orig_key in existing_cons_keys and orig_key != new_key:
                            # When in Per PI mode, preserve PI values the user didn't edit (hidden PI columns).
                            # In All PIs mode, we intentionally clear overrides and do not carry them over.
                            if cons_mode == "Per PI":
                                for pi in [1, 2, 3, 4]:
                                    if pi in selected_pis:
                                        continue
                                    old_v = cons_map.get((r["orig_cls_key"], r["orig_cid"], int(pi)), None)
                                    if old_v is None:
                                        continue
                                    upsert_team_contractor_headcount(
                                        team_id,
                                        int(year_i),
                                        int(pi),
                                        r["cls_key"],
                                        r["cid"],
                                        float(old_v),
                                        updated_by=_user_display,
                                    )
                                    old_note = str(cons_note_map.get((r["orig_cls_key"], r["orig_cid"], int(pi)), "") or "").strip()
                                    if old_note:
                                        _update_team_contractor_note(pi=int(pi), cls=r["cls_key"], company_id=r["cid"], note=old_note)
                            execute(
                                "DELETE FROM TEAM_CONTRACTOR_HEADCOUNT WHERE TEAMID=%s AND YEAR=%s AND CLASS=%s AND COMPANYID=%s",
                                (team_id, int(year_i), r["orig_cls_key"], r["orig_cid"]),
                            )
                            renamed_cons_orig.add(orig_key)

                    deleted_cons_keys = (existing_cons_keys - new_cons_keys) - renamed_cons_orig
                    for cls_key, cid in deleted_cons_keys:
                        execute(
                            "DELETE FROM TEAM_CONTRACTOR_HEADCOUNT WHERE TEAMID=%s AND YEAR=%s AND CLASS=%s AND COMPANYID=%s",
                            (team_id, int(year_i), cls_key, str(cid)),
                        )

                    for r in cons_rows:
                        upsert_team_contractor_headcount(
                            team_id,
                            int(year_i),
                            0,
                            r["cls_key"],
                            r["cid"],
                            float(r["hc_all"]),
                            updated_by=_user_display,
                        )
                        if cons_notes_enabled:
                            _update_team_contractor_note(pi=0, cls=r["cls_key"], company_id=r["cid"], note=str(r.get("note_all") or ""))
                        if cons_mode == "All PIs same":
                            # Clear PI-specific overrides so PI=0 applies to all PIs again.
                            for pi in [1, 2, 3, 4]:
                                execute(
                                    "DELETE FROM TEAM_CONTRACTOR_HEADCOUNT WHERE TEAMID=%s AND YEAR=%s AND PI=%s AND CLASS=%s AND COMPANYID=%s",
                                    (team_id, int(year_i), int(pi), r["cls_key"], str(r["cid"])),
                                )
                        else:
                            for pi in selected_pis:
                                hc_pi = float(r["pi_vals"].get(int(pi), r["hc_all"]))
                                note_pi = str((r.get("pi_notes") or {}).get(int(pi), "") or "").strip() if cons_notes_enabled else ""
                                if hc_pi == float(r["hc_all"]) and not note_pi:
                                    execute(
                                        "DELETE FROM TEAM_CONTRACTOR_HEADCOUNT WHERE TEAMID=%s AND YEAR=%s AND PI=%s AND CLASS=%s AND COMPANYID=%s",
                                        (team_id, int(year_i), int(pi), r["cls_key"], str(r["cid"])),
                                    )
                                else:
                                    upsert_team_contractor_headcount(
                                        team_id,
                                        int(year_i),
                                        int(pi),
                                        r["cls_key"],
                                        str(r["cid"]),
                                        float(hc_pi),
                                        updated_by=_user_display,
                                    )
                                    if cons_notes_enabled:
                                        _update_team_contractor_note(pi=int(pi), cls=r["cls_key"], company_id=str(r["cid"]), note=note_pi)

                    # Best-effort: record cost events from headcount deltas (per PI effective, WF/NWF split).
                    try:
                        if ENABLE_COST_EVENTS and record_cost_events_from_headcount_delta:
                            # Staff rate map: (LOCATION, PI) -> XOM_RATE
                            staff_locs = sorted(
                                {
                                    str(r.get("loc") or "").strip()
                                    for r in staff_rows
                                    if str(r.get("loc") or "").strip()
                                }
                                | {str(loc or "").strip() for (_cls, loc) in deleted_staff_keys if str(loc or "").strip()}
                            )
                            staff_rate_map: dict[tuple[str, int], float] = {}
                            if staff_locs:
                                placeholders = ", ".join(["%s"] * len(staff_locs))
                                df_rates = fetch_df(
                                    f"""
                                    SELECT LOCATION, PI, XOM_RATE
                                    FROM VW_TEAM_RATE_EFFECTIVE
                                    WHERE TEAMID=%s AND YEAR=%s AND PI IN (1,2,3,4) AND LOCATION IN ({placeholders})
                                    """,
                                    tuple([team_id, int(year_i)] + staff_locs),
                                )
                                if df_rates is not None and not df_rates.empty:
                                    for _, rr in df_rates.iterrows():
                                        loc = str(rr.get("LOCATION") or "").strip()
                                        pi = int(pd.to_numeric(rr.get("PI"), errors="coerce") or 0)
                                        rate = float(pd.to_numeric(rr.get("XOM_RATE"), errors="coerce") or 0.0)
                                        if loc and pi in [1, 2, 3, 4]:
                                            staff_rate_map[(loc, pi)] = rate

                            # Contractor rate map: (CLASS, COMPANYID, PI) -> RATE
                            cons_ids = sorted(
                                {
                                    str(r.get("cid") or "").strip()
                                    for r in cons_rows
                                    if str(r.get("cid") or "").strip()
                                }
                                | {str(cid or "").strip() for (_cls, cid) in deleted_cons_keys if str(cid or "").strip()}
                            )
                            cons_rate_map: dict[tuple[str, str, int], float] = {}
                            if cons_ids:
                                placeholders = ", ".join(["%s"] * len(cons_ids))
                                df_cr = fetch_df(
                                    f"""
                                    SELECT COMPANYID, PI, CLASS, RATE
                                    FROM VW_CONTRACTOR_RATE_EFFECTIVE
                                    WHERE YEAR=%s AND PI IN (1,2,3,4) AND COMPANYID IN ({placeholders})
                                    """,
                                    tuple([int(year_i)] + cons_ids),
                                )
                                if df_cr is not None and not df_cr.empty:
                                    for _, rr in df_cr.iterrows():
                                        cid = str(rr.get("COMPANYID") or "").strip()
                                        pi = int(pd.to_numeric(rr.get("PI"), errors="coerce") or 0)
                                        cls = str(rr.get("CLASS") or "").strip().upper()
                                        rate = float(pd.to_numeric(rr.get("RATE"), errors="coerce") or 0.0)
                                        if cid and pi in [1, 2, 3, 4] and cls:
                                            cons_rate_map[(cls, cid, pi)] = rate

                            def _old_staff_effective(cls_key: str, loc: str, pi: int) -> float:
                                base = float(staff_map.get((cls_key, loc, 0), 0.0) or 0.0)
                                ov = staff_map.get((cls_key, loc, int(pi)), None)
                                return float(ov) if ov is not None else base

                            def _old_cons_effective(cls_key: str, cid: str, pi: int) -> float:
                                base = float(cons_map.get((cls_key, cid, 0), 0.0) or 0.0)
                                ov = cons_map.get((cls_key, cid, int(pi)), None)
                                return float(ov) if ov is not None else base

                            def _staff_bucket_label(cls_key: str) -> str:
                                return "Team Overhead" if cls_key == "TEAM" else ("Delivery" if cls_key == "DELIVERY" else cls_key)

                            def _cons_bucket_label(cls_key: str) -> str:
                                return "Contractor C" if cls_key == "CONTRACTOR_C" else ("Contractor CS" if cls_key == "CONTRACTOR_CS" else cls_key)

                            program_name_ev = program_name or program_pick
                            team_name_ev = str(team_pick or "")

                            # Record staff row deltas (including deletes and renames) for PIs 1..4.
                            for r in staff_rows:
                                cls_new = str(r.get("cls_key") or "").strip().upper()
                                loc_new = str(r.get("loc") or "").strip()
                                cls_old = str(r.get("orig_cls_key") or "").strip().upper() or cls_new
                                loc_old = str(r.get("orig_loc") or "").strip() or loc_new
                                base_new = float(r.get("hc_all") or 0.0)
                                pi_vals = r.get("pi_vals", {}) or {}
                                for pi in [1, 2, 3, 4]:
                                    old_eff = _old_staff_effective(cls_old, loc_old, pi)
                                    if pi in selected_pis:
                                        new_eff = float(pi_vals.get(pi, base_new))
                                    else:
                                        ov = staff_map.get((cls_old, loc_old, int(pi)), None)
                                        new_eff = float(ov) if ov is not None else base_new
                                    if new_eff == old_eff:
                                        continue
                                    rate = float(staff_rate_map.get((loc_new, pi), 0.0))
                                    record_cost_events_from_headcount_delta(
                                        None,
                                        fiscal_year=int(year_i),
                                        pi_name=f"I{pi}",
                                        program=str(program_name_ev or ""),
                                        team=str(team_name_ev or ""),
                                        app_group=None,
                                        bucket_name=_staff_bucket_label(cls_new),
                                        old_headcount=float(old_eff),
                                        new_headcount=float(new_eff),
                                        annual_rate=float(rate),
                                        notes=f"Teams page • {loc_new}",
                                        cost_bucket="WF",
                                        related_object_id=f"TEAM:{team_id}|{cls_new}|{loc_new}|PI{pi}",
                                    )

                            # Record deleted staff keys (not renamed) -> new=0 for all PIs 1..4.
                            for cls_key, loc in deleted_staff_keys:
                                for pi in [1, 2, 3, 4]:
                                    old_eff = _old_staff_effective(cls_key, loc, pi)
                                    if old_eff == 0.0:
                                        continue
                                    rate = float(staff_rate_map.get((loc, pi), 0.0))
                                    record_cost_events_from_headcount_delta(
                                        None,
                                        fiscal_year=int(year_i),
                                        pi_name=f"I{pi}",
                                        program=str(program_name_ev or ""),
                                        team=str(team_name_ev or ""),
                                        app_group=None,
                                        bucket_name=_staff_bucket_label(cls_key),
                                        old_headcount=float(old_eff),
                                        new_headcount=0.0,
                                        annual_rate=float(rate),
                                        notes=f"Teams page • {loc} • row removed",
                                        cost_bucket="WF",
                                        related_object_id=f"TEAM:{team_id}|{cls_key}|{loc}|PI{pi}",
                                    )

                            # Record contractor row deltas.
                            for r in cons_rows:
                                cls_new = str(r.get("cls_key") or "").strip().upper()
                                cid_new = str(r.get("cid") or "").strip()
                                cls_old = str(r.get("orig_cls_key") or "").strip().upper() or cls_new
                                cid_old = str(r.get("orig_cid") or "").strip() or cid_new
                                base_new = float(r.get("hc_all") or 0.0)
                                pi_vals = r.get("pi_vals", {}) or {}
                                bucket = "NWF" if cls_new == "CONTRACTOR_CS" else "WF"
                                for pi in [1, 2, 3, 4]:
                                    old_eff = _old_cons_effective(cls_old, cid_old, pi)
                                    if pi in selected_pis:
                                        new_eff = float(pi_vals.get(pi, base_new))
                                    else:
                                        ov = cons_map.get((cls_old, cid_old, int(pi)), None)
                                        new_eff = float(ov) if ov is not None else base_new
                                    if new_eff == old_eff:
                                        continue
                                    rate = float(cons_rate_map.get((cls_new, cid_new, pi), 0.0))
                                    provider_lbl = str(r.get("provider") or "").strip()
                                    record_cost_events_from_headcount_delta(
                                        None,
                                        fiscal_year=int(year_i),
                                        pi_name=f"I{pi}",
                                        program=str(program_name_ev or ""),
                                        team=str(team_name_ev or ""),
                                        app_group=None,
                                        bucket_name=_cons_bucket_label(cls_new),
                                        old_headcount=float(old_eff),
                                        new_headcount=float(new_eff),
                                        annual_rate=float(rate),
                                        notes=f"Teams page • {provider_lbl}",
                                        cost_bucket=bucket,
                                        related_object_id=f"TEAM:{team_id}|{cls_new}|{cid_new}|PI{pi}",
                                    )

                            for cls_key, cid in deleted_cons_keys:
                                bucket = "NWF" if cls_key == "CONTRACTOR_CS" else "WF"
                                for pi in [1, 2, 3, 4]:
                                    old_eff = _old_cons_effective(cls_key, cid, pi)
                                    if old_eff == 0.0:
                                        continue
                                    rate = float(cons_rate_map.get((cls_key, cid, pi), 0.0))
                                    record_cost_events_from_headcount_delta(
                                        None,
                                        fiscal_year=int(year_i),
                                        pi_name=f"I{pi}",
                                        program=str(program_name_ev or ""),
                                        team=str(team_name_ev or ""),
                                        app_group=None,
                                        bucket_name=_cons_bucket_label(cls_key),
                                        old_headcount=float(old_eff),
                                        new_headcount=0.0,
                                        annual_rate=float(rate),
                                        notes="Teams page • row removed",
                                        cost_bucket=bucket,
                                        related_object_id=f"TEAM:{team_id}|{cls_key}|{cid}|PI{pi}",
                                    )
                    except Exception:
                        pass

                    recompute_team_composition_from_headcount_year(
                        team_id=team_id, year=int(year_i), changed_by=_user_display
                    )
                    _sync_team_baseline_from_history(team_id)
                    st.success("Headcount saved. Composition updated automatically.")
                    # Reset editors to fresh DB state.
                    for k in [
                        editor_meta_key,
                        staff_editor_key,
                        staff_df_key,
                        staff_ctx_key,
                        f"{staff_editor_key}__init",
                        cons_editor_key,
                        cons_df_key,
                        cons_ctx_key,
                        f"{cons_editor_key}__init",
                    ]:
                        if k in st.session_state:
                            del st.session_state[k]
                    kpi_cache_key = f"teams_overview_kpis_{team_id}_{int(year_i)}"
                    if kpi_cache_key in st.session_state:
                        del st.session_state[kpi_cache_key]
                    st.session_state["teams_program_pick_pending"] = str(program_pick or "")
                    st.session_state["teams_team_pick_pending"] = str(row.get("TEAMNAME") or team_pick or "")
                    if save_status is not None:
                        save_status.update(label="Headcount saved.", state="complete")
                    _set_op_feedback("success", "Headcount saved. Composition updated automatically.", target="headcount")
                    _run_post_save_refresh("teams_save_headcount")
                except Exception as e:
                    if save_status is not None:
                        save_status.update(label="Headcount save failed.", state="error")
                    _set_op_feedback("error", f"Save failed: {e}", target="headcount")
                    st.error(f"Save failed: {e}")

            st.divider()

            if show_debug:
                with st.expander("Debug – headcount pipeline (for this team/year)", expanded=False):
                    st.caption("Shows raw rows that drive TEAMFTE & workforce costs. Read-only.")

                    # Guard: if we don't have a team or year, bail out.
                    if not team_id or not year_i:
                        st.info("Select a team and year above to see debug information.")
                    else:
                        try:
                            debug_team_id = str(team_id)
                            debug_year = int(year_i)
                        except Exception:
                            st.info("Invalid team or year in context.")
                            debug_team_id = None
                            debug_year = None

                        def _fq(name: str) -> str:
                            return name

                        if debug_team_id and debug_year:
                            # 1) Raw TEAM_HEADCOUNT_HISTORY
                            df_hc_raw = None
                            try:
                                df_hc_raw = fetch_df(
                                    """
                                    SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, NOTES, UPDATED_AT, UPDATED_BY
                                    FROM TEAM_HEADCOUNT_HISTORY
                                    WHERE TEAMID=%s AND YEAR=%s
                                    ORDER BY YEAR, PI, CLASS, LOCATION
                                    """,
                                    (debug_team_id, debug_year),
                                )
                            except Exception:
                                df_hc_raw = fetch_df(
                                    """
                                    SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, UPDATED_AT, UPDATED_BY
                                    FROM TEAM_HEADCOUNT_HISTORY
                                    WHERE TEAMID=%s AND YEAR=%s
                                    ORDER BY YEAR, PI, CLASS, LOCATION
                                    """,
                                    (debug_team_id, debug_year),
                                )
                        st.markdown("**TEAM_HEADCOUNT_HISTORY (internal staff)**")
                        st.dataframe(
                            df_hc_raw if isinstance(df_hc_raw, pd.DataFrame) else pd.DataFrame(),
                            use_container_width=True,
                            hide_index=True,
                        )

                        # 2) Raw TEAM_CONTRACTOR_HEADCOUNT
                        df_cons_raw = None
                        try:
                            df_cons_raw = fetch_df(
                                """
                                SELECT TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT, NOTES, UPDATED_AT, UPDATED_BY
                                FROM TEAM_CONTRACTOR_HEADCOUNT
                                WHERE TEAMID=%s AND YEAR=%s
                                ORDER BY YEAR, PI, CLASS, COMPANYID
                                """,
                                (debug_team_id, debug_year),
                            )
                        except Exception:
                            df_cons_raw = fetch_df(
                                """
                                SELECT TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT, UPDATED_AT, UPDATED_BY
                                FROM TEAM_CONTRACTOR_HEADCOUNT
                                WHERE TEAMID=%s AND YEAR=%s
                                ORDER BY YEAR, PI, CLASS, COMPANYID
                                """,
                                (debug_team_id, debug_year),
                            )
                        st.markdown("**TEAM_CONTRACTOR_HEADCOUNT (contractors)**")
                        st.dataframe(
                            df_cons_raw if isinstance(df_cons_raw, pd.DataFrame) else pd.DataFrame(),
                            use_container_width=True,
                            hide_index=True,
                        )

                        # 3) Effective headcount views (what composition uses)
                        df_hc_eff = None
                        try:
                            df_hc_eff = fetch_df(
                                f"""
                                SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT
                                FROM {_fq('VW_TEAM_HEADCOUNT_EFFECTIVE')}
                                WHERE TEAMID=%s AND YEAR=%s
                                ORDER BY YEAR, PI, CLASS, LOCATION
                                """,
                                (debug_team_id, debug_year),
                            )
                        except Exception as e:
                            st.info(f"Could not load VW_TEAM_HEADCOUNT_EFFECTIVE: {e}")
                            df_hc_eff = pd.DataFrame()
                        st.markdown("**VW_TEAM_HEADCOUNT_EFFECTIVE**")
                        st.dataframe(
                            df_hc_eff if isinstance(df_hc_eff, pd.DataFrame) else pd.DataFrame(),
                            use_container_width=True,
                            hide_index=True,
                        )

                        df_cons_eff = None
                        try:
                            df_cons_eff = fetch_df(
                                f"""
                                SELECT TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT
                                FROM {_fq('VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE')}
                                WHERE TEAMID=%s AND YEAR=%s
                                ORDER BY YEAR, PI, CLASS, COMPANYID
                                """,
                                (debug_team_id, debug_year),
                            )
                        except Exception as e:
                            st.info(f"Could not load VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE: {e}")
                            df_cons_eff = pd.DataFrame()
                        st.markdown("**VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE**")
                        st.dataframe(
                            df_cons_eff if isinstance(df_cons_eff, pd.DataFrame) else pd.DataFrame(),
                            use_container_width=True,
                            hide_index=True,
                        )

                        # 4) Composition rows (what _effective_composition uses)
                        df_comp = fetch_df(
                            """
                            SELECT TEAMID, YEAR, PI, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE, UPDATED_AT
                            FROM TEAM_COMPOSITION_HISTORY
                            WHERE TEAMID=%s AND YEAR=%s
                            ORDER BY YEAR, PI, UPDATED_AT
                            """,
                            (debug_team_id, debug_year),
                        )
                        st.markdown("**TEAM_COMPOSITION_HISTORY**")
                        st.dataframe(
                            df_comp if isinstance(df_comp, pd.DataFrame) else pd.DataFrame(),
                            use_container_width=True,
                            hide_index=True,
                        )

                        # 5) TEAMS baseline row (what Teams overview displays)
                        df_team_row = fetch_df(
                            """
                            SELECT TEAMID, TEAMNAME, PROGRAMID, TEAMFTE,
                                   DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE,
                                   PRODUCTOWNER, UPDATED_AT, UPDATED_BY
                            FROM TEAMS
                            WHERE TEAMID=%s
                            """,
                            (debug_team_id,),
                        )
                        st.markdown("**TEAMS baseline row**")
                        st.dataframe(
                            df_team_row if isinstance(df_team_row, pd.DataFrame) else pd.DataFrame(),
                            use_container_width=True,
                            hide_index=True,
                        )

        if show_debug:
            with st.container(border=True):
                st.markdown("##### Danger zone")
                deps = get_team_dependencies(team_id)
                render_dependency_summary(deps)
                confirm = st.text_input("Type DELETE to confirm", key="team_delete_confirm", disabled=not can_delete(deps))
                if st.button(
                    "Delete team",
                    type="secondary",
                    key="danger_delete_team_btn",
                    disabled=not can_delete(deps),
                ):
                    if confirm.strip().upper() != "DELETE":
                        st.error("Type DELETE to confirm.")
                    else:
                        result = safe_delete_team(team_id, source="pages/4_Teams.py")
                        if result.get("ok"):
                            st.warning("Team deleted.")
                            _run_post_save_refresh("teams_delete_team", bump_version=True)
                        else:
                            st.error(result.get("message") or "Delete failed.")

if section == "Audit":
    st.markdown("### History / Audit")
    st.caption("Audit trail for composition changes derived from headcount saves.")
    if team_row is None:
        st.info("Select a team to view audit history.")
    else:
        team_id = str(team_row.get("TEAMID") or "")
        try:
            log_df = fetch_df(
                """
                SELECT TOP 200
                  CHANGED_AT, CHANGED_BY, YEAR, PI,
                  OLD_TEAMFTE, OLD_DELIVERY_TEAM_FTE, OLD_CONTRACTOR_C_FTE, OLD_CONTRACTOR_CS_FTE,
                  NEW_TEAMFTE, NEW_DELIVERY_TEAM_FTE, NEW_CONTRACTOR_C_FTE, NEW_CONTRACTOR_CS_FTE,
                  SOURCE
                FROM TCO_TEAM_COMPOSITION_CHANGELOG
                WHERE TEAMID = %s
                ORDER BY CHANGED_AT DESC
                """,
                (team_id,),
            )
            if log_df is None or log_df.empty:
                st.info("No changes yet.")
            else:
                st.dataframe(log_df, use_container_width=True, hide_index=True, height=420)
        except Exception as e:
            st.warning(f"Could not load audit log: {e}")
