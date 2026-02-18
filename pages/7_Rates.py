"""Rates page.

This page manages rate master data used by `core/canonical_costs.py` (and the underlying VW_* views).
"""

import streamlit as st

from core.init import init_page
from core.cache_utils import cache_data_portfolio, cache_resource_portfolio

import pandas as pd
from datetime import date
import uuid
from typing import Any, Optional

from utils.labels import display_class_label

from db import (
    # core
    list_programs,          # Program selector before Team
    list_teams,
    list_application_groups,
    upsert_team_rate_history,
    upsert_program_rate_history,
    upsert_contractor_company,
    upsert_contractor_rate,
    fetch_df,
    execute,
    ensure_workforce_split_view,
    ensure_msp_costs_view,
    ensure_analytics_views_ok,
    ensure_location_and_contractor_tables,
    list_contractor_companies,
    list_contractor_rates,
    bump_data_version,
)
from utils.toast import toast_error, toast_success

from utils.app_shell import bootstrap_page
bootstrap_page()

page_theme = init_page("Rates", page_path=__file__)

st.title("Rates")
st.caption("Rates drive workforce and MSP labor costs across Baseline and Projected scenarios.")
# SECTION 1 — Header + edit mode gate (UI-only; no logic changes).
with st.container(border=True):
    st.markdown("**Scope / Notes**")
    st.caption(
        "Use this page to manage rate inputs only. Changes affect cost calculations everywhere in the app."
    )
    edit_mode = st.toggle("Edit mode", value=False, key="rates_edit_mode")
    st.caption("Changing rates affects all downstream scenario cost calculations.")
# END SECTION 1
st.session_state.setdefault("rates_bootstrapped", False)


def _run_post_save_refresh(context: str) -> None:
    """Lightweight analytics refresh after rate/master-data writes."""
    try:
        ensure_analytics_views_ok()
        bump_data_version(context)
    except Exception as e:
        toast_error(f"Saved ({context}), but analytics refresh failed: {e}")
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()


def _render_rate_table(df: pd.DataFrame, *, editable: bool, editor_kwargs: dict, height: Optional[int] = None) -> pd.DataFrame:
    if editable:
        return st.data_editor(df, **editor_kwargs)
    st.dataframe(df, use_container_width=True, hide_index=True, height=height)
    return df


def render_audit_section() -> None:
    st.markdown("### History / Audit")
    st.caption("Recent rate changes for this page.")
    try:
        team_hist = fetch_df(
            "SELECT TEAMID, YEAR, LOCATION, XOM_RATE, UPDATED_AT, UPDATED_BY FROM TEAM_RATE_HISTORY",
            None,
        )
        prog_hist = fetch_df(
            "SELECT PROGRAMID, YEAR, LOCATION, PROGRAM_XOM_RATE, UPDATED_AT, UPDATED_BY FROM PROGRAM_RATE_HISTORY",
            None,
        )
    except Exception:
        team_hist = pd.DataFrame()
        prog_hist = pd.DataFrame()

    rows = []
    if team_hist is not None and not team_hist.empty:
        for _, r in team_hist.iterrows():
            rows.append(
                {
                    "Timestamp": r.get("UPDATED_AT"),
                    "User": r.get("UPDATED_BY") or "",
                    "Action": "Rate update",
                    "Entity": "Team rate",
                    "Scope": f"Year {r.get('YEAR')} • {r.get('LOCATION')}",
                    "Description": f"XOM rate = {r.get('XOM_RATE')}",
                }
            )
    if prog_hist is not None and not prog_hist.empty:
        for _, r in prog_hist.iterrows():
            rows.append(
                {
                    "Timestamp": r.get("UPDATED_AT"),
                    "User": r.get("UPDATED_BY") or "",
                    "Action": "Rate update",
                    "Entity": "Program rate",
                    "Scope": f"Year {r.get('YEAR')} • {r.get('LOCATION')}",
                    "Description": f"Program XOM rate = {r.get('PROGRAM_XOM_RATE')}",
                }
            )
    audit_df = pd.DataFrame(rows)
    if audit_df.empty or "Timestamp" not in audit_df.columns:
        st.info("No audit history available for rates.")
        return
    audit_df["Timestamp"] = pd.to_datetime(audit_df["Timestamp"], errors="coerce")
    audit_df = audit_df.sort_values("Timestamp", ascending=False)
    st.dataframe(
        audit_df[["Timestamp", "User", "Action", "Entity", "Scope", "Description"]].head(200),
        use_container_width=True,
        hide_index=True,
        height=280,
    )

@cache_resource_portfolio(show_spinner=False)
def _bootstrap_rates():
    try:
        ensure_msp_costs_view()
        ensure_workforce_split_view()
    except Exception:
        pass
    try:
        ensure_location_and_contractor_tables()
    except Exception:
        pass
    try:
        _ensure_msp_schema()
    except Exception:
        pass
    try:
        _ensure_team_composition_history()
    except Exception:
        pass

# ------------------------------------------------------------------
# MSP helpers (self-contained: create tables/columns ONLY — no views here)
# ------------------------------------------------------------------
def _ensure_msp_schema():
    # TEAM_MSP_RATE with per-size columns
    # Canonical inputs used by the MSP allocation pipeline:
    # - `MSP_RATE_SMALL|MEDIUM|LARGE` (preferred)
    # Legacy/compat columns kept for older deployments (not used by the current pipeline):
    # - `MSP_SIZE`, `MSP_RATE_PER_PI`
    execute("""
    CREATE TABLE IF NOT EXISTS TEAM_MSP_RATE (
        TEAMID STRING PRIMARY KEY,
        MSP_ENABLED BOOLEAN,
        MSP_SIZE STRING,
        MSP_RATE_PER_PI NUMBER(18,2),
        MSP_RATE_SMALL  NUMBER(18,2),
        MSP_RATE_MEDIUM NUMBER(18,2),
        MSP_RATE_LARGE  NUMBER(18,2),
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    execute("ALTER TABLE TEAM_MSP_RATE ADD COLUMN IF NOT EXISTS MSP_RATE_SMALL  NUMBER(18,2)")
    execute("ALTER TABLE TEAM_MSP_RATE ADD COLUMN IF NOT EXISTS MSP_RATE_MEDIUM NUMBER(18,2)")
    execute("ALTER TABLE TEAM_MSP_RATE ADD COLUMN IF NOT EXISTS MSP_RATE_LARGE  NUMBER(18,2)")
    execute("ALTER TABLE TEAM_MSP_RATE ADD COLUMN IF NOT EXISTS MSP_SIZE STRING")
    execute("ALTER TABLE TEAM_MSP_RATE ADD COLUMN IF NOT EXISTS MSP_RATE_PER_PI NUMBER(18,2)")

    # MSP Assignments (add WEIGHT_PCT if missing)
    execute("""
    CREATE TABLE IF NOT EXISTS TEAM_MSP_ASSIGNMENTS (
        TEAMID STRING NOT NULL,
        GROUPID STRING NOT NULL,
        YEAR NUMBER(4,0) NOT NULL,
        ITERATION_START NUMBER(1,0) NOT NULL,
        ITERATION_END   NUMBER(1,0) DEFAULT 4,
        MSP_SIZE STRING,
        WEIGHT_PCT NUMBER(6,2) DEFAULT 100,
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        CONSTRAINT PK_TEAM_MSP_ASSIGN PRIMARY KEY (TEAMID, GROUPID, YEAR)
    )
    """)
    try:
        execute("ALTER TABLE TEAM_MSP_ASSIGNMENTS ALTER COLUMN WEIGHT_PCT SET DEFAULT 100")
    except Exception:
        pass
    _ensure_msp_pk()

def _ensure_msp_pk():
    """Ensure PK covers (TEAMID, GROUPID, YEAR) so multiple groups per team/year work."""
    try:
        try:
            execute(
                """
                IF EXISTS (
                  SELECT 1 FROM sys.key_constraints
                  WHERE name IN ('PK_TEAM_MSP_ASSIGNMENTS','PK_TEAM_MSP_ASSIGN')
                    AND parent_object_id = OBJECT_ID('dbo.TEAM_MSP_ASSIGNMENTS')
                )
                ALTER TABLE dbo.TEAM_MSP_ASSIGNMENTS DROP CONSTRAINT
                  (SELECT TOP 1 name FROM sys.key_constraints
                   WHERE name IN ('PK_TEAM_MSP_ASSIGNMENTS','PK_TEAM_MSP_ASSIGN')
                     AND parent_object_id = OBJECT_ID('dbo.TEAM_MSP_ASSIGNMENTS'));
                """
            )
        except Exception:
            pass
        execute("ALTER TABLE dbo.TEAM_MSP_ASSIGNMENTS ADD CONSTRAINT PK_TEAM_MSP_ASSIGNMENTS PRIMARY KEY (TEAMID, GROUPID, YEAR);")
    except Exception:
        # Best effort
        pass


def _table_has_column(table: str, column: str) -> bool:
    """Return True when the given table exposes the requested column."""
    try:
        df = fetch_df(f"SELECT * FROM {table} WHERE 1=0")
        return df is not None and column in df.columns
    except Exception:
        return False

# ------------------------------------------------------------------
# NEW: Composition history helpers (non-MSP)
# ------------------------------------------------------------------
def _ensure_team_composition_history():
    # Allow PI=NULL for "All PIs" year-level default
    execute("""
      CREATE TABLE IF NOT EXISTS TEAM_COMPOSITION_HISTORY (
        TEAMID STRING NOT NULL,
        YEAR   NUMBER(4,0) NOT NULL,
        PI     NUMBER(1,0),
        TEAMFTE FLOAT DEFAULT 0,
        DELIVERY_TEAM_FTE FLOAT DEFAULT 0,
        CONTRACTOR_CS_FTE FLOAT DEFAULT 0,
        CONTRACTOR_C_FTE  FLOAT DEFAULT 0,
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        CONSTRAINT PK_TEAM_COMPOSITION_HISTORY PRIMARY KEY (TEAMID, YEAR, PI)
      )
    """)
    # Make sure PI stays nullable (older runs may have set NOT NULL implicitly)
    try:
        execute("ALTER TABLE TEAM_COMPOSITION_HISTORY ALTER COLUMN PI DROP NOT NULL")
    except Exception:
        pass

if not st.session_state.get("rates_bootstrapped", False):
    with st.spinner("Preparing rate tables & views (first load only)..."):
        _bootstrap_rates()
    st.session_state["rates_bootstrapped"] = True

# Cached wrappers to reduce reload lag
@cache_data_portfolio(ttl=300, show_spinner=False)
def cached_list_teams():
    return list_teams()

@cache_data_portfolio(ttl=300, show_spinner=False)
def cached_list_programs():
    return list_programs()

@cache_data_portfolio(ttl=120, show_spinner=False)
def cached_list_application_groups(team_id: str):
    return list_application_groups(team_id)

section_options = ["Employees", "Contractors", "MSP", "Audit"]
if st.session_state.get("rates_section") not in section_options:
    st.session_state["rates_section"] = section_options[0]
if hasattr(st, "segmented_control"):
    section = st.segmented_control("Section", section_options, key="rates_section")
else:
    section = st.radio("Section", section_options, horizontal=True, key="rates_section")
if not section:
    section = section_options[0]

def _safe_float(val, default=0.0) -> float:
    try:
        if pd.isna(val):
            return float(default)
        return float(val)
    except Exception:
        return float(default)

# ------------------------
# Section: Employees (XOM) Rates
# ------------------------
if section == "Employees":
    st.markdown("### Employees")

    teams = cached_list_teams()
    programs_df = cached_list_programs()
    upd_by = str(st.session_state.get("auth_user", {}).get("email", ""))

    st.markdown("### Rates Catalog")
    st.markdown("#### Location-Specific Rates (per PI)")
    st.caption("Rates are defined per PI (Iteration).")
    # Canonical input:
    # - Persisted to `TEAM_RATE_HISTORY.XOM_RATE` and `PROGRAM_RATE_HISTORY.PROGRAM_XOM_RATE`
    # - Materialized in `VW_GLOBAL_RATE_EFFECTIVE` and consumed by the cost engine for workforce costs.
    # Important: Location keys must match the location values used in headcount tables/views.
    loc_options = ["GBC", "US-0970+HC", "US-0910", "OTHER"]
    loc_year = st.number_input("Year (applies to all programs & child teams)", min_value=2020, max_value=2100, step=1, value=date.today().year, key="loc_year_global")

    # Build editable table with latest known rates per location (from program history first, fallback team history)
    def _latest_rate_from_df(df, rate_col):
        if df is None or df.empty or rate_col not in df.columns:
            return {}
        df = df.copy()
        if "UPDATED_AT" in df.columns:
            df = df.sort_values(by="UPDATED_AT", ascending=False)
        return {
            str(r["LOCATION"]): _safe_float(r.get(rate_col), 0.0)
            for _, r in df.iterrows()
        }

    # Pull most recent rows for the selected year (if any), else latest overall
    team_hist_all = fetch_df("SELECT * FROM TEAM_RATE_HISTORY WHERE YEAR=%s", (int(loc_year),)) if teams is not None and not teams.empty else pd.DataFrame()
    prog_hist_all = fetch_df("SELECT * FROM PROGRAM_RATE_HISTORY WHERE YEAR=%s", (int(loc_year),)) if programs_df is not None and not programs_df.empty else pd.DataFrame()

    def _slice_for_year(df, year):
        if df is None or df.empty:
            return pd.DataFrame()
        if "YEAR" in df.columns:
            df_year = df[df["YEAR"] == year]
            if df_year is not None and not df_year.empty:
                return df_year
        return df

    team_defaults = _latest_rate_from_df(_slice_for_year(team_hist_all, loc_year), "XOM_RATE")
    prog_defaults = _latest_rate_from_df(_slice_for_year(prog_hist_all, loc_year), "PROGRAM_XOM_RATE")

    def _ordered_locations(all_locations: list[str], preferred: list[str]) -> list[str]:
        pref = [p for p in preferred if p in all_locations]
        rest = sorted([l for l in all_locations if l not in pref])
        return pref + rest

    def _has_unassigned_location(df: pd.DataFrame) -> bool:
        if df is None or df.empty or "LOCATION" not in df.columns:
            return False
        s = df["LOCATION"].fillna("").astype(str).str.strip()
        return bool(s.eq("").any())

    history_locations = []
    for _df in [team_hist_all, prog_hist_all]:
        if _df is not None and not _df.empty and "LOCATION" in _df.columns:
            history_locations.extend([str(x).strip() for x in _df["LOCATION"].tolist() if str(x).strip()])
    all_locations = _ordered_locations(sorted(set(history_locations + loc_options)), loc_options)
    has_unassigned = _has_unassigned_location(team_hist_all) or _has_unassigned_location(prog_hist_all)

    rows = []
    for loc in all_locations:
        val = prog_defaults.get(loc, team_defaults.get(loc, 0.0))
        rows.append({"Location": loc, "Rate (per PI)": val})
    if has_unassigned:
        val = prog_defaults.get("", team_defaults.get("", 0.0))
        rows.append({"Location": "", "Rate (per PI)": val})
    loc_df = pd.DataFrame(rows)

    # SECTION 2 — Location-first tabs (UI-only; no logic changes).
    loc_state_key = f"rates_loc_df_{int(loc_year)}"
    loc_state_ctx = f"{loc_state_key}__ctx"
    if st.session_state.get(loc_state_ctx) != int(loc_year) or loc_state_key not in st.session_state:
        st.session_state[loc_state_key] = loc_df
        st.session_state[loc_state_ctx] = int(loc_year)
    loc_full = st.session_state.get(loc_state_key, loc_df).copy()
    loc_series = loc_full["Location"].fillna("").astype(str).str.strip() if "Location" in loc_full.columns else pd.Series([], dtype=str)
    assigned_locations = _ordered_locations(sorted(set(loc_series[loc_series.ne("")].tolist())), loc_options)
    unassigned_count = int(loc_series.eq("").sum()) if not loc_series.empty else 0
    summary = f"Locations: {len(assigned_locations)} | Rate rows: {len(loc_full)}"
    if unassigned_count:
        summary += f" | Unassigned: {unassigned_count}"
    st.caption(summary)

    # SECTION 3 — Location tab readability (grouping + column hierarchy).
    def _rate_class_col(df: pd.DataFrame) -> Optional[str]:
        if df is None or df.empty:
            return None
        candidates = ["CLASS", "ROLE", "RATE_CLASS", "WORKFORCE_CLASS"]
        by_lower = {str(c).lower(): str(c) for c in df.columns}
        for cand in candidates:
            if cand in df.columns:
                return cand
            hit = by_lower.get(cand.lower())
            if hit:
                return hit
        return None

    def _group_order(label: str) -> int:
        key = str(label or "").strip().upper()
        order = {
            "DELIVERY": 1,
            "CONTRACTOR_C": 2,
            "CONTRACTOR CS": 3,
            "CONTRACTOR_CS": 3,
            "PROGRAM OVERHEAD": 4,
            "PROGRAM_OVERHEAD": 4,
        }
        return order.get(key, 99)

    def split_visible_vs_advanced_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
        if df is None or df.empty:
            return [], []
        preferred = ["CLASS", "ROLE", "RATE_CLASS", "Location", "Rate (per PI)"]
        visible = [c for c in preferred if c in df.columns]
        if "Location" in df.columns and "Location" not in visible:
            visible.insert(0, "Location")
        if "Rate (per PI)" in df.columns and "Rate (per PI)" not in visible:
            visible.insert(1, "Rate (per PI)")
        advanced = [c for c in df.columns if c not in visible]
        return visible, advanced

    def order_rate_columns(df: pd.DataFrame, primary_cols: list[str]) -> list[str]:
        cols = list(df.columns)
        ordered = [c for c in primary_cols if c in cols]
        ordered.extend([c for c in cols if c not in ordered])
        return ordered

    def render_rate_group(
        *,
        title: str,
        group_df: pd.DataFrame,
        edit_mode: bool,
        visible_cols: list[str],
        advanced_cols: list[str],
        key_suffix: str,
    ) -> pd.DataFrame:
        if group_df is None or group_df.empty:
            return pd.DataFrame()
        st.markdown(f"#### {title}")
        view_cols = order_rate_columns(group_df, visible_cols)
        view_df = group_df[view_cols].copy()
        column_config = {}
        if "Rate (per PI)" in view_df.columns:
            column_config["Rate (per PI)"] = st.column_config.NumberColumn(
                "Rate per PI",
                min_value=0.0,
                step=50.0,
                format="$%0.2f",
            )
        if "Location" in view_df.columns:
            column_config["Location"] = st.column_config.TextColumn("Location", disabled=True)
        editor_kwargs = {
            "use_container_width": True,
            "hide_index": True,
            "num_rows": "fixed",
            "column_config": column_config,
            "key": f"loc_group_{key_suffix}",
        }
        edited = _render_rate_table(
            view_df,
            editable=bool(edit_mode),
            editor_kwargs=editor_kwargs,
            height=140,
        )
        return edited

    # END SECTION 3

    # SECTION 4 — Guardrails + trust signals (UI-only; no blocking).
    def compute_rate_warnings(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty or "Rate (per PI)" not in df.columns:
            return pd.DataFrame(columns=["Location", "Class", "Rate", "Reason"])
        class_col = _rate_class_col(df)
        work = df.copy()
        work["__RATE"] = pd.to_numeric(work.get("Rate (per PI)"), errors="coerce")
        work["__CLASS"] = work[class_col].fillna("").astype(str).str.strip() if class_col else "Rates"
        loc_col = "Location" if "Location" in work.columns else None
        work["__LOC"] = work[loc_col].fillna("").astype(str).str.strip() if loc_col else ""

        medians = (
            work.loc[work["__RATE"] > 0]
            .groupby("__CLASS")["__RATE"]
            .median()
            .to_dict()
        )
        warnings = []
        for _, r in work.iterrows():
            rate = r["__RATE"]
            cls = r["__CLASS"] or "Rates"
            loc = r["__LOC"]
            if pd.isna(rate):
                warnings.append((loc, cls, "", "Missing rate"))
                continue
            if float(rate) < 0:
                warnings.append((loc, cls, float(rate), "Negative rate"))
                continue
            if float(rate) == 0:
                warnings.append((loc, cls, float(rate), "Zero rate"))
                continue
            med = float(medians.get(cls) or 0.0)
            if med > 0 and (float(rate) > 3.0 * med or float(rate) < 0.3 * med):
                warnings.append((loc, cls, float(rate), "Outlier vs median"))
        return pd.DataFrame(warnings, columns=["Location", "Class", "Rate", "Reason"])

    def summarize_last_updated(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
        if df is None or df.empty:
            return None, None
        cols = list(df.columns)
        time_candidates = ["UPDATED_AT", "UPDATED_ON", "UPDATED", "UPDATED_TS", "UPDATED_TIMESTAMP"]
        user_candidates = ["UPDATED_BY", "UPDATEDBY", "MODIFIED_BY", "CREATED_BY"]
        time_col = next((c for c in time_candidates if c in cols), None)
        user_col = next((c for c in user_candidates if c in cols), None)
        if not time_col:
            return None, None
        ts = pd.to_datetime(df[time_col], errors="coerce")
        if ts.isna().all():
            return None, None
        idx = ts.idxmax()
        last_ts = ts.loc[idx]
        last_user = str(df.loc[idx, user_col]) if user_col and idx in df.index else ""
        return str(last_ts), (last_user.strip() or None)

    def render_validation_summary(warnings_df: pd.DataFrame) -> None:
        if warnings_df is None or warnings_df.empty:
            st.caption("Validation: ✅ 0 warnings")
            return
        reasons = warnings_df["Reason"].astype(str)
        missing = int(reasons.isin(["Missing rate", "Zero rate"]).sum())
        outliers = int(reasons.eq("Outlier vs median").sum())
        negatives = int(reasons.eq("Negative rate").sum())
        parts = [f"Warnings: {len(warnings_df)}"]
        if missing:
            parts.append(f"{missing} missing/zero")
        if outliers:
            parts.append(f"{outliers} outlier")
        if negatives:
            parts.append(f"{negatives} negative")
        st.caption("Validation — " + ", ".join(parts))

    warnings_all = compute_rate_warnings(loc_full)
    last_ts, last_user = summarize_last_updated(loc_full)
    if last_ts:
        st.caption(f"Last updated: {last_ts}" + (f" by {last_user}" if last_user else ""))
    # END SECTION 4

    # END SECTION 4B (removed)

    tab_labels = list(assigned_locations)
    if unassigned_count:
        tab_labels.append("Unassigned")
    for tab_label, tab in zip(tab_labels, st.tabs(tab_labels)):
        with tab:
            if tab_label == "Unassigned":
                view_mask = loc_series.eq("")
            else:
                view_mask = loc_series.eq(str(tab_label))
            view_df = loc_full.loc[view_mask].copy() if not loc_full.empty else pd.DataFrame()
            if view_df.empty:
                st.info("No rate rows for this location.")
                continue
            view_df["Location"] = tab_label if tab_label == "Unassigned" else view_df["Location"]
            class_col = _rate_class_col(view_df)
            visible_cols, advanced_cols = split_visible_vs_advanced_columns(view_df)
            if class_col:
                group_items = []
                for grp, sub in view_df.groupby(class_col, dropna=False):
                    label = str(grp or "Other").strip() or "Other"
                    group_items.append((label, sub))
                group_items.sort(key=lambda x: (_group_order(x[0]), x[0]))
            else:
                group_items = [("Rates", view_df)]

            warnings_loc = (
                warnings_all.loc[warnings_all["Location"].astype(str).str.strip() == str(tab_label)]
                if tab_label != "Unassigned"
                else warnings_all.loc[warnings_all["Location"].astype(str).str.strip() == ""]
            )
            render_validation_summary(warnings_loc)
            if edit_mode:
                st.caption("Edits affect downstream scenario costs.")
            if warnings_loc is not None and not warnings_loc.empty:
                with st.expander("View warnings", expanded=False):
                    st.dataframe(warnings_loc, use_container_width=True, hide_index=True, height=200)

            updated = False
            for grp_label, grp_df in group_items:
                edited = render_rate_group(
                    title=grp_label,
                    group_df=grp_df,
                    edit_mode=bool(edit_mode),
                    visible_cols=visible_cols,
                    advanced_cols=advanced_cols,
                    key_suffix=f"{int(loc_year)}_{tab_label}_{grp_label}",
                )
                if edit_mode and not edited.empty and "Rate (per PI)" in edited.columns:
                    loc_full.loc[grp_df.index, "Rate (per PI)"] = edited["Rate (per PI)"].values
                    updated = True
            if updated:
                st.session_state[loc_state_key] = loc_full

    if edit_mode:
        if st.button("Save location rates for all programs & teams", type="primary", icon=":material/save:", key="save_global_loc_rates"):
            try:
                loc_full = st.session_state.get(loc_state_key, loc_df).copy()
                rates_map = {
                    str(r["Location"]).strip(): _safe_float(r.get("Rate (per PI)"), 0.0)
                    for _, r in loc_full.iterrows()
                    if str(r.get("Location") or "").strip()
                }
                if teams is not None and not teams.empty:
                    for team_id in teams["TEAMID"].tolist():
                        for loc, rate in rates_map.items():
                            upsert_team_rate_history(team_id, int(loc_year), 0, loc, rate, updated_by=upd_by)
                if programs_df is not None and not programs_df.empty:
                    for prog_id in programs_df["PROGRAMID"].tolist():
                        for loc, rate in rates_map.items():
                            upsert_program_rate_history(prog_id, int(loc_year), 0, loc, rate, updated_by=upd_by)
                toast_success("Saved location rates for all programs and teams.")
                st.session_state.pop(loc_state_key, None)
                st.session_state.pop(loc_state_ctx, None)
                _run_post_save_refresh("rates_location_global_save")
            except Exception as e:
                toast_error(f"Could not save location rates: {e}")
    else:
        st.caption("Enable Edit mode to update rates.")
    # END SECTION 2

    # Current effective rates from unified global view (one XOM rate per location/year/PI)
    st.markdown("#### Current Location Rates")
    try:
        global_rates = fetch_df("SELECT YEAR, LOCATION, RATE, UPDATED_AT FROM VW_GLOBAL_RATE_EFFECTIVE")
    except Exception as e:
        global_rates = pd.DataFrame()
        st.warning(f"Could not load global rates: {e}")
    if global_rates is not None and not global_rates.empty:
        df_cur = global_rates.copy()
        df_cur = df_cur[df_cur["YEAR"] == loc_year] if "YEAR" in df_cur.columns else df_cur
        if "UPDATED_AT" in df_cur.columns:
            df_cur = df_cur.sort_values(by=["LOCATION","UPDATED_AT"], ascending=[True, False]).drop_duplicates(subset=["LOCATION"])
        else:
            df_cur = df_cur.sort_values(by=["LOCATION"], ascending=[True]).drop_duplicates(subset=["LOCATION"])
        st.dataframe(df_cur[["LOCATION","YEAR","RATE","UPDATED_AT"]], use_container_width=True, height=220)
    else:
        st.info("No location rates captured yet.")

    st.caption("Location rates apply per year and location to all programs and their teams. MSP and contractor settings remain on their respective tabs.")

# ------------------------
# Section: Contractors (companies, rates, headcount)
# ------------------------
if section == "Contractors":
    st.markdown("### Contractors")
    st.caption("Contractor rates are defined per PI (Iteration).")
    # Canonical input:
    # - `CONTRACTOR_RATE_HISTORY` provides per-year rates for `CLASS in ('CONTRACTOR_C','CONTRACTOR_CS')`
    # - These classes are used by the cost engine and should not be renamed/repurposed here.
    upd_by = str(st.session_state.get("auth_user", {}).get("email", ""))
    if not edit_mode:
        st.caption("Enable Edit mode to manage contractor companies and rates.")
        rate_table_cs = fetch_df(
            """
            SELECT c.NAME AS COMPANY, r.CLASS, r.YEAR, r.RATE, r.UPDATED_AT, r.UPDATED_BY
            FROM CONTRACTOR_RATE_HISTORY r
            LEFT JOIN CONTRACTOR_COMPANY c ON c.COMPANYID = r.COMPANYID
            WHERE r.CLASS = 'CONTRACTOR_CS'
            ORDER BY r.YEAR DESC, c.NAME
            """
        )
        rate_table_c = fetch_df(
            """
            SELECT c.NAME AS COMPANY, r.CLASS, r.YEAR, r.RATE, r.UPDATED_AT, r.UPDATED_BY
            FROM CONTRACTOR_RATE_HISTORY r
            LEFT JOIN CONTRACTOR_COMPANY c ON c.COMPANYID = r.COMPANYID
            WHERE r.CLASS = 'CONTRACTOR_C'
            ORDER BY r.YEAR DESC, c.NAME
            """
        )
        st.markdown("#### Current Contractor Rates (all companies)")
        d1, d2 = st.columns(2)
        with d1:
            st.caption("/CS rates")
            if rate_table_cs is not None and not rate_table_cs.empty:
                st.dataframe(rate_table_cs[["COMPANY","YEAR","RATE","UPDATED_AT","UPDATED_BY"]].head(200), use_container_width=True, height=240)
            else:
                st.info("No contractor /CS rates recorded yet.")
        with d2:
            st.caption("/C rates")
            if rate_table_c is not None and not rate_table_c.empty:
                st.dataframe(rate_table_c[["COMPANY","YEAR","RATE","UPDATED_AT","UPDATED_BY"]].head(200), use_container_width=True, height=240)
            else:
                st.info("No contractor /C rates recorded yet.")
        st.stop()

    comp_df = list_contractor_companies()
    comp_names = comp_df["NAME"].tolist() if comp_df is not None and not comp_df.empty else []
    comp_ids = comp_df["COMPANYID"].tolist() if comp_df is not None and not comp_df.empty else []
    comp_map = dict(zip(comp_names, comp_ids)) if comp_names else {}

    action_mode = st.radio("Contractor company mode", ["Add new", "Edit existing"], horizontal=True, key="contractor_mode")

    if action_mode == "Add new":
        with st.form("company_form_add"):
            c1, c2 = st.columns([2, 1])
            with c1:
                comp_name = st.text_input("Company name", "")
            with c2:
                comp_active = st.checkbox("Active", value=True)
            comp_notes = st.text_area("Notes", height=80)
            save_comp = st.form_submit_button("Save contractor", type="primary")
            if save_comp:
                try:
                    comp_id = str(uuid.uuid4())
                    upsert_contractor_company(comp_id, comp_name, active=comp_active, notes=comp_notes, updated_by=upd_by)
                    toast_success(f"Saved contractor company: {comp_name.strip() or 'Unnamed'}.")
                    _run_post_save_refresh("rates_contractor_create")
                except Exception as e:
                    toast_error(f"Could not save company: {e}")

        # Current CS/C rates across all companies (show company name, no PI)
        rate_table_cs = fetch_df(
            """
            SELECT c.NAME AS COMPANY, r.CLASS, r.YEAR, r.RATE, r.UPDATED_AT, r.UPDATED_BY
            FROM CONTRACTOR_RATE_HISTORY r
            LEFT JOIN CONTRACTOR_COMPANY c ON c.COMPANYID = r.COMPANYID
            WHERE r.CLASS = 'CONTRACTOR_CS'
            ORDER BY r.YEAR DESC, c.NAME
            """
        )
        rate_table_c = fetch_df(
            """
            SELECT c.NAME AS COMPANY, r.CLASS, r.YEAR, r.RATE, r.UPDATED_AT, r.UPDATED_BY
            FROM CONTRACTOR_RATE_HISTORY r
            LEFT JOIN CONTRACTOR_COMPANY c ON c.COMPANYID = r.COMPANYID
            WHERE r.CLASS = 'CONTRACTOR_C'
            ORDER BY r.YEAR DESC, c.NAME
            """
        )
        st.markdown("#### Current Contractor Rates (all companies)")
        d1, d2 = st.columns(2)
        with d1:
            st.caption("/CS rates")
            if rate_table_cs is not None and not rate_table_cs.empty:
                st.dataframe(rate_table_cs[["COMPANY","YEAR","RATE","UPDATED_AT","UPDATED_BY"]].head(200), use_container_width=True, height=240)
            else:
                st.info("No contractor /CS rates recorded yet.")
        with d2:
            st.caption("/C rates")
            if rate_table_c is not None and not rate_table_c.empty:
                st.dataframe(rate_table_c[["COMPANY","YEAR","RATE","UPDATED_AT","UPDATED_BY"]].head(200), use_container_width=True, height=240)
            else:
                st.info("No contractor /C rates recorded yet.")

    else:
        if comp_df is None or comp_df.empty:
            st.info("No contractor companies found.")
        else:
            sel_comp_name = st.selectbox("Select contractor", ["(select)"] + comp_names, index=0, key="edit_contractor_select")
            sel_comp_id = comp_map.get(sel_comp_name) if sel_comp_name in comp_map else None
            if sel_comp_id:
                sel_row = comp_df[comp_df["NAME"] == sel_comp_name].iloc[0]
                with st.form("company_form_edit"):
                    c1, c2 = st.columns([2, 1])
                    with c1:
                        comp_name_edit = st.text_input("Company name", value=sel_row.get("NAME", ""))
                    with c2:
                        comp_active_edit = st.checkbox("Active", value=bool(sel_row.get("ACTIVE", True)))
                    comp_notes_edit = st.text_area("Notes", value=str(sel_row.get("NOTES") or ""), height=80)
                    save_comp_edit = st.form_submit_button("Update contractor", type="primary")
                    if save_comp_edit:
                        try:
                            upsert_contractor_company(sel_comp_id, comp_name_edit, active=comp_active_edit, notes=comp_notes_edit, updated_by=upd_by)
                            toast_success(f"Updated contractor company: {comp_name_edit.strip() or 'Unnamed'}.")
                            _run_post_save_refresh("rates_contractor_update")
                        except Exception as e:
                            toast_error(f"Could not update company: {e}")

    # Show rates editor only when editing an existing company and a selection is made
    if action_mode == "Edit existing" and comp_map and sel_comp_id:
        sel_row = comp_df[comp_df["NAME"] == sel_comp_name].iloc[0]

        def _current_rate(hist_df, cls: str, year: int) -> float:
            if hist_df is None or hist_df.empty:
                return 0.0
            dfc = hist_df.copy()
            dfc.columns = [c.upper() for c in dfc.columns]
            dfc["PI"] = pd.to_numeric(dfc.get("PI"), errors="coerce").fillna(0).astype(int) if "PI" in dfc.columns else 0
            try:
                match = dfc[(dfc["CLASS"] == cls) & (dfc["YEAR"] == int(year))].sort_values(
                    by=["PI", "UPDATED_AT"] if "UPDATED_AT" in dfc.columns else ["PI"],
                    ascending=[False, False] if "UPDATED_AT" in dfc.columns else [False],
                )
                if not match.empty:
                    return float(match.iloc[0].get("RATE") or 0.0)
            except Exception:
                pass
            return 0.0

        try:
            cr_hist_full = list_contractor_rates(sel_comp_id)
        except Exception:
            cr_hist_full = pd.DataFrame()

        def _latest_year(hist_df) -> int:
            try:
                if hist_df is not None and not hist_df.empty and "YEAR" in hist_df.columns:
                    y = pd.to_numeric(hist_df["YEAR"], errors="coerce").dropna()
                    if not y.empty:
                        return int(y.max())
            except Exception:
                pass
            return int(date.today().year)

        # Show current effective rates (latest per class) for quick reference
        latest_year = _latest_year(cr_hist_full)
        latest_cs = _current_rate(cr_hist_full, "CONTRACTOR_CS", latest_year)
        latest_c = _current_rate(cr_hist_full, "CONTRACTOR_C", latest_year)
        st.info(f"Current effective rates — /CS: {latest_cs:.2f} • /C: {latest_c:.2f}")

        st.markdown("### Rates per Company / PI (split by class)")
        c_cs, c_c = st.columns(2)
        with c_cs:
            st.caption("Contractor /CS rate")
            with st.form("company_rate_form_cs"):
                cs_year = st.number_input("Year (/CS)", min_value=2020, max_value=2100, step=1, value=date.today().year, key="cr_year_cs")
                cs_rate_default = _current_rate(cr_hist_full, "CONTRACTOR_CS", int(cs_year))
                cs_rate = st.number_input(
                    "Rate (per PI)",
                    min_value=0.0,
                    step=50.0,
                    value=cs_rate_default,
                    key=f"cr_rate_cs_{sel_comp_id}_{int(cs_year)}",
                )
                save_cs = st.form_submit_button("Save /CS rate", type="primary")
            if save_cs:
                try:
                    upsert_contractor_rate(sel_comp_id, int(cs_year), None, "CONTRACTOR_CS", cs_rate, updated_by=upd_by)
                    label = str(sel_comp_name or "").strip() or "Selected company"
                    toast_success(f"Saved contractor /CS rate: {label} ({int(cs_year)}).")
                    _run_post_save_refresh("rates_contractor_cs_save")
                except Exception as e:
                    toast_error(f"Could not save contractor /CS rate: {e}")
        with c_c:
            st.caption("Contractor /C rate (usually a different company)")
            with st.form("company_rate_form_c"):
                c_year = st.number_input("Year (/C)", min_value=2020, max_value=2100, step=1, value=date.today().year, key="cr_year_c")
                c_rate_default = _current_rate(cr_hist_full, "CONTRACTOR_C", int(c_year))
                c_rate = st.number_input(
                    "Rate (per PI)",
                    min_value=0.0,
                    step=50.0,
                    value=c_rate_default,
                    key=f"cr_rate_c_{sel_comp_id}_{int(c_year)}",
                )
                save_c = st.form_submit_button("Save /C rate", type="primary")
            if save_c:
                try:
                    upsert_contractor_rate(sel_comp_id, int(c_year), None, "CONTRACTOR_C", c_rate, updated_by=upd_by)
                    label = str(sel_comp_name or "").strip() or "Selected company"
                    toast_success(f"Saved contractor /C rate: {label} ({int(c_year)}).")
                    _run_post_save_refresh("rates_contractor_c_save")
                except Exception as e:
                    toast_error(f"Could not save contractor /C rate: {e}")

        st.markdown("#### Current rates")
        try:
            cr_hist = list_contractor_rates(sel_comp_id)
            if cr_hist is not None and not cr_hist.empty:
                cr_hist_display = cr_hist[["COMPANYID","CLASS","YEAR", "RATE", "UPDATED_AT", "UPDATED_BY"]]
                # Replace CompanyID with Company Name for display
                comp_name_lookup = {comp_id: name for name, comp_id in comp_map.items()}
                cr_hist_display["COMPANY"] = cr_hist_display["COMPANYID"].apply(lambda cid: comp_name_lookup.get(cid, cid))
                cr_hist_display = cr_hist_display[["COMPANY","CLASS","YEAR","RATE","UPDATED_AT","UPDATED_BY"]]
                st.dataframe(cr_hist_display.sort_values(by="UPDATED_AT", ascending=False).head(200), use_container_width=True, height=220)
            else:
                st.info("No rates yet for this company.")
        except Exception as e:
            st.warning(f"Could not load contractor rates: {e}")

        # Contractor change log (all entries, most recent first) shown in main tab below

# ------------------------
# Section: MSP (Assignments & Sizing)
# ------------------------
if section == "MSP":
    st.markdown("### MSP")
    st.caption("Only MSP-enabled teams are listed. Toggle MSP flag on the Teams page.")
    st.caption("MSP rates are defined per PI (Iteration).")
    # Canonical input:
    # - `TEAM_MSP_RATE` stores per-team MSP rate cards (small/medium/large).
    # - `TEAM_MSP_ASSIGNMENTS` links application groups to MSP teams with an MSP size + optional weight.
    # These tables feed `VW_MSP_COSTS` and must remain MSP-only (not Derived-FTE-based).
    upd_by = str(st.session_state.get("auth_user", {}).get("email", ""))

    programs_df = cached_list_programs()
    prog_names = programs_df["PROGRAMNAME"].astype(str).tolist() if programs_df is not None and not programs_df.empty else []
    prog_ids = programs_df["PROGRAMID"].astype(str).tolist() if programs_df is not None and not programs_df.empty else []
    prog_map = dict(zip(prog_names, prog_ids))

    teams_df = cached_list_teams()
    c_prog, c_team_scope, c_msp, c_year = st.columns([1, 1, 1, 1])
    with c_prog:
        prog_sel_label = st.selectbox("Program", ["(any program)"] + prog_names, index=0, key="msp_prog_select")
    prog_sel_id = prog_map.get(prog_sel_label)

    team_scope_df = teams_df if teams_df is not None else pd.DataFrame()
    if prog_sel_id and team_scope_df is not None and not team_scope_df.empty:
        team_scope_df = team_scope_df[team_scope_df["PROGRAMID"].astype(str) == str(prog_sel_id)]
    team_scope_names = team_scope_df["TEAMNAME"].astype(str).tolist() if team_scope_df is not None and not team_scope_df.empty else []
    team_scope_ids = team_scope_df["TEAMID"].astype(str).tolist() if team_scope_df is not None and not team_scope_df.empty else []
    team_scope_map = dict(zip(team_scope_names, team_scope_ids))
    with c_team_scope:
        team_scope_label = st.selectbox("Team (owner scope)", ["(any team)"] + team_scope_names, index=0, key="msp_team_scope")
    team_scope_id = team_scope_map.get(team_scope_label)

    # Only MSP-enabled teams, optionally filtered by program
    if prog_sel_id:
        msp_teams = fetch_df(
            """
            SELECT t.TEAMID, t.TEAMNAME
            FROM TEAMS t
            JOIN TEAM_MSP_RATE m ON m.TEAMID = t.TEAMID
            WHERE COALESCE(m.MSP_ENABLED, FALSE) = TRUE AND t.PROGRAMID = %s
            ORDER BY t.TEAMNAME
            """,
            (prog_sel_id,),
        )
    else:
        msp_teams = fetch_df(
            """
            SELECT t.TEAMID, t.TEAMNAME
            FROM TEAMS t
            JOIN TEAM_MSP_RATE m ON m.TEAMID = t.TEAMID
            WHERE COALESCE(m.MSP_ENABLED, FALSE) = TRUE
            ORDER BY t.TEAMNAME
            """
        )

    if st.session_state.get("msp_rate_flash"):
        st.success(str(st.session_state.get("msp_rate_flash")))
        st.session_state.pop("msp_rate_flash", None)
    if st.session_state.get("msp_assign_flash"):
        st.success(str(st.session_state.get("msp_assign_flash")))
        st.session_state.pop("msp_assign_flash", None)
    if st.session_state.get("msp_assign_skipped"):
        skipped_df = pd.DataFrame(st.session_state.get("msp_assign_skipped"), columns=["Application", "Reason"])
        st.caption("Skipped rows")
        st.dataframe(skipped_df, use_container_width=True, hide_index=True, height=200)
        st.session_state.pop("msp_assign_skipped", None)

    if msp_teams is None or msp_teams.empty:
        st.info("No MSP teams yet. Mark a team as MSP on the Teams page.")
    else:
        team_names = msp_teams["TEAMNAME"].astype(str).tolist()
        team_ids   = msp_teams["TEAMID"].astype(str).tolist()
        name_to_id = dict(zip(team_names, team_ids))

        with c_msp:
            team_name_sel_msp = st.selectbox("MSP Team", ["(select team)"] + team_names, index=0, key="msp_team_select")
        team_id_sel_msp = name_to_id.get(team_name_sel_msp) if team_name_sel_msp in name_to_id else None
        with c_year:
            year_sel = st.number_input(
                "Year",
                min_value=2000,
                max_value=2100,
                value=date.today().year,
                step=1,
                key="msp_assign_year",
            )

        if not team_id_sel_msp:
            st.info("Select an MSP team to edit rates and assignments.")
            st.stop()

        # Per-size MSP rates for this team (only used when assignments specify a size)
        rate_row = fetch_df(
            "SELECT MSP_RATE_SMALL, MSP_RATE_MEDIUM, MSP_RATE_LARGE FROM TEAM_MSP_RATE WHERE TEAMID=%s",
            (team_id_sel_msp,),
        )
        small_rate = float(rate_row.iloc[0].get("MSP_RATE_SMALL") or 0.0) if rate_row is not None and not rate_row.empty else 0.0
        med_rate = float(rate_row.iloc[0].get("MSP_RATE_MEDIUM") or 0.0) if rate_row is not None and not rate_row.empty else 0.0
        large_rate = float(rate_row.iloc[0].get("MSP_RATE_LARGE") or 0.0) if rate_row is not None and not rate_row.empty else 0.0

        st.markdown("#### MSP Rate Editor")
        with st.container(border=True):
            rate_df = pd.DataFrame(
                [
                    {
                        "MSP Team": str(team_name_sel_msp or "").strip(),
                        "Small Rate (per PI)": float(small_rate),
                        "Medium Rate (per PI)": float(med_rate),
                        "Large Rate (per PI)": float(large_rate),
                    }
                ]
            )
            rate_edit = _render_rate_table(
                rate_df,
                editable=bool(edit_mode),
                editor_kwargs={
                    "use_container_width": True,
                    "hide_index": True,
                    "column_config": {
                        "Small Rate (per PI)": st.column_config.NumberColumn("Small Rate (per PI)", min_value=0.0, step=50.0),
                        "Medium Rate (per PI)": st.column_config.NumberColumn("Medium Rate (per PI)", min_value=0.0, step=50.0),
                        "Large Rate (per PI)": st.column_config.NumberColumn("Large Rate (per PI)", min_value=0.0, step=50.0),
                    },
                    "disabled": ["MSP Team"],
                    "key": "msp_rate_editor",
                },
                height=140,
            )
            if edit_mode:
                if st.button("Save rate card", type="primary", key="msp_rate_save_btn"):
                    try:
                        row = rate_edit.iloc[0]
                        small_rate_in = float(row.get("Small Rate (per PI)") or 0.0)
                        med_rate_in = float(row.get("Medium Rate (per PI)") or 0.0)
                        large_rate_in = float(row.get("Large Rate (per PI)") or 0.0)
                        if min(small_rate_in, med_rate_in, large_rate_in) < 0:
                            st.error("Rates must be non-negative.")
                            st.stop()
                        execute(
                            """
                            MERGE INTO TEAM_MSP_RATE t
                            USING (SELECT %s AS TEAMID, %s AS MSP_ENABLED, %s AS MSP_RATE_SMALL, %s AS MSP_RATE_MEDIUM, %s AS MSP_RATE_LARGE, %s AS UPDATED_BY) s
                              ON t.TEAMID = s.TEAMID
                            WHEN MATCHED THEN UPDATE SET
                              MSP_ENABLED = COALESCE(t.MSP_ENABLED, s.MSP_ENABLED),
                              MSP_RATE_PER_PI = NULL,
                              MSP_SIZE = NULL,
                              MSP_RATE_SMALL = s.MSP_RATE_SMALL,
                              MSP_RATE_MEDIUM = s.MSP_RATE_MEDIUM,
                              MSP_RATE_LARGE = s.MSP_RATE_LARGE,
                              UPDATED_AT = SYSDATETIME(),
                              UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
                            WHEN NOT MATCHED THEN INSERT (TEAMID, MSP_ENABLED, MSP_RATE_PER_PI, MSP_RATE_SMALL, MSP_RATE_MEDIUM, MSP_RATE_LARGE, UPDATED_AT, UPDATED_BY)
                              VALUES (s.TEAMID, s.MSP_ENABLED, NULL, s.MSP_RATE_SMALL, s.MSP_RATE_MEDIUM, s.MSP_RATE_LARGE, SYSDATETIME(), s.UPDATED_BY);
                            """,
                            (
                                team_id_sel_msp,
                                1,
                                small_rate_in,
                                med_rate_in,
                                large_rate_in,
                                upd_by,
                            ),
                        )
                        team_label = str(team_name_sel_msp or "").strip() or "Selected team"
                        st.session_state["msp_rate_flash"] = f"Rate card saved: {team_label}."
                        st.session_state.pop("msp_rate_editor", None)
                        _run_post_save_refresh("rates_msp_rate_card_save")
                    except Exception as e:
                        toast_error(f"Could not save MSP rates: {e}")
            else:
                st.caption("Enable Edit mode to update MSP rates.")

        # Show current effective rates for the selected team
        rate_summary = fetch_df(
            """
            SELECT MSP_RATE_SMALL, MSP_RATE_MEDIUM, MSP_RATE_LARGE, UPDATED_AT, UPDATED_BY
            FROM TEAM_MSP_RATE WHERE TEAMID=%s
            """,
            (team_id_sel_msp,),
        )
        if rate_summary is not None and not rate_summary.empty:
            r = rate_summary.iloc[0]
            st.info(
                f"Effective rates (per PI) — Small: {float(r.get('MSP_RATE_SMALL') or 0):,.2f} | "
                f"Medium: {float(r.get('MSP_RATE_MEDIUM') or 0):,.2f} | "
                f"Large: {float(r.get('MSP_RATE_LARGE') or 0):,.2f} "
                f"(updated {r.get('UPDATED_AT')} by {r.get('UPDATED_BY') or 'unknown'})"
            )

        st.divider()
        st.markdown("### MSP Assignments Editor")
        st.caption(
            "Edit assignments for the selected MSP team and year. "
            "Only existing assignments are shown by default; add rows to create new ones."
        )

        apps_df = cached_list_application_groups(None)
        if apps_df is None or apps_df.empty:
            st.info("No Applications found.")
            st.stop()

        apps_scope = apps_df.copy()
        if prog_sel_id:
            apps_scope = apps_scope[apps_scope["PROGRAMID"].astype(str) == str(prog_sel_id)]
        if team_scope_id:
            apps_scope = apps_scope[apps_scope["TEAMID"].astype(str) == str(team_scope_id)]
        if apps_scope.empty:
            st.info("No Applications found in the selected scope.")
            st.stop()

        def _app_label(row: pd.Series) -> str:
            prog = str(row.get("PROGRAMNAME") or "").strip() or "Unknown program"
            team = str(row.get("TEAMNAME") or "").strip() or "Unknown team"
            name = str(row.get("GROUPNAME") or "").strip() or "Unnamed application"
            return f"{prog} • {team} • {name}"

        apps_scope = apps_scope.copy()
        apps_scope["APP_LABEL"] = apps_scope.apply(_app_label, axis=1)
        dup_labels = apps_scope["APP_LABEL"].duplicated(keep=False)
        if dup_labels.any():
            apps_scope.loc[dup_labels, "APP_LABEL"] = apps_scope.loc[dup_labels].apply(
                lambda r: f"{r['APP_LABEL']} [{r['GROUPID']}]",
                axis=1,
            )

        app_labels = sorted(apps_scope["APP_LABEL"].astype(str).tolist())
        app_label_to_gid = dict(
            zip(apps_scope["APP_LABEL"].astype(str), apps_scope["GROUPID"].astype(str))
        )

        has_year_col = _table_has_column("TEAM_MSP_ASSIGNMENTS", "YEAR")
        has_effective_year_col = _table_has_column("TEAM_MSP_ASSIGNMENTS", "EFFECTIVE_YEAR")
        year_filters = []
        year_params: list[Any] = []
        if has_year_col:
            year_filters.append("a.YEAR = %s")
            year_params.append(int(year_sel))
        if has_effective_year_col:
            year_filters.append("a.EFFECTIVE_YEAR = %s")
            year_params.append(int(year_sel))
        year_clause = f"AND ({' OR '.join(year_filters)})" if year_filters else ""

        existing = fetch_df(
            f"""
            SELECT a.TEAMID, a.GROUPID, a.YEAR, a.ITERATION_START, a.ITERATION_END, a.MSP_SIZE, a.WEIGHT_PCT
            FROM TEAM_MSP_ASSIGNMENTS a
            WHERE a.TEAMID = %s {year_clause}
            """,
            tuple([team_id_sel_msp] + year_params),
        )
        existing = existing if existing is not None else pd.DataFrame()
        existing_map = {str(r.GROUPID): r for r in existing.itertuples()} if not existing.empty else {}

        existing_scoped = pd.DataFrame()
        if not existing.empty:
            existing_scoped = existing.merge(
                apps_scope[["GROUPID", "APP_LABEL"]],
                on="GROUPID",
                how="inner",
            )

        editor_rows = []
        if existing_scoped is not None and not existing_scoped.empty:
            for r in existing_scoped.itertuples():
                editor_rows.append(
                    {
                        "Application": str(getattr(r, "APP_LABEL", "") or ""),
                        "MSP Size": str(getattr(r, "MSP_SIZE", "") or ""),
                        "Iter Start": getattr(r, "ITERATION_START", ""),
                        "Iter End": getattr(r, "ITERATION_END", ""),
                        "Weight %": getattr(r, "WEIGHT_PCT", ""),
                        "Delete": False,
                    }
                )

        editor_df = pd.DataFrame(
            editor_rows,
            columns=["Application", "MSP Size", "Iter Start", "Iter End", "Weight %", "Delete"],
        )

        editor_key = f"msp_assign_editor_{team_id_sel_msp}_{int(year_sel)}_{str(prog_sel_id or '')}_{str(team_scope_id or '')}"
        edit = _render_rate_table(
            editor_df,
            editable=bool(edit_mode),
            editor_kwargs={
                "use_container_width": True,
                "hide_index": True,
                "num_rows": "dynamic",
                "height": 420,
                "column_config": {
                    "Application": st.column_config.SelectboxColumn(
                        "Application",
                        options=[""] + app_labels,
                    ),
                    "MSP Size": st.column_config.SelectboxColumn(
                        "MSP Size",
                        options=["", "Small", "Medium", "Large"],
                    ),
                    "Iter Start": st.column_config.NumberColumn("Iter Start", min_value=1, max_value=4, step=1),
                    "Iter End": st.column_config.NumberColumn("Iter End", min_value=1, max_value=4, step=1),
                    "Weight %": st.column_config.NumberColumn("Weight %", min_value=0.0, max_value=100.0, step=5.0),
                    "Delete": st.column_config.CheckboxColumn("Delete"),
                },
                "key": editor_key,
            },
            height=420,
        )

        if edit_mode and st.button("Save assignments", type="primary", key="msp_assign_apply"):
            created = updated = deleted = skipped = 0
            skipped_rows = []
            _ensure_msp_pk()

            edit_df = edit.copy()
            if "Application" not in edit_df.columns:
                st.error("Assignments table is missing the Application column.")
                st.stop()

            edit_df["Application"] = edit_df["Application"].astype(str).fillna("")
            if "Delete" in edit_df.columns:
                edit_df["Delete"] = edit_df["Delete"].fillna(False).astype(bool)

            working = edit_df[edit_df["Application"].str.strip() != ""].copy()
            if not working.empty:
                dup_apps = working.loc[~working["Delete"], "Application"]
                if dup_apps.duplicated().any():
                    st.error("Duplicate applications found. Keep one row per application.")
                    st.stop()

            for _, row in edit_df.iterrows():
                app_label = str(row.get("Application") or "").strip()
                delete_row = bool(row.get("Delete"))

                if not app_label:
                    if delete_row:
                        skipped_rows.append(("", "Missing Application"))
                        skipped += 1
                    continue

                group_id = app_label_to_gid.get(app_label)
                if not group_id:
                    skipped_rows.append((app_label, "Application not in scope"))
                    skipped += 1
                    continue

                existing_row = existing_map.get(str(group_id))
                if delete_row:
                    if existing_row:
                        delete_filters = ["TEAMID=%s", "GROUPID=%s"]
                        delete_params = [team_id_sel_msp, group_id]
                        if has_year_col or has_effective_year_col:
                            delete_filters.append("(YEAR=%s OR COALESCE(EFFECTIVE_YEAR, YEAR)=%s)")
                            delete_params.extend([int(year_sel), int(year_sel)])
                        execute(
                            f"DELETE FROM TEAM_MSP_ASSIGNMENTS WHERE {' AND '.join(delete_filters)}",
                            tuple(delete_params),
                        )
                        deleted += 1
                    else:
                        skipped_rows.append((app_label, "No existing assignment to delete"))
                        skipped += 1
                    continue

                size = str(row.get("MSP Size") or "").strip()
                if size not in ("Small", "Medium", "Large"):
                    skipped_rows.append((app_label, "MSP Size must be Small/Medium/Large"))
                    skipped += 1
                    continue

                it_start = row.get("Iter Start")
                it_end = row.get("Iter End")
                weight = row.get("Weight %")
                if (
                    pd.isna(it_start)
                    or pd.isna(it_end)
                    or pd.isna(weight)
                    or str(it_start).strip() == ""
                    or str(it_end).strip() == ""
                    or str(weight).strip() == ""
                ):
                    skipped_rows.append((app_label, "Iter Start/Iter End/Weight are required"))
                    skipped += 1
                    continue
                try:
                    it_start_i = int(it_start)
                    it_end_i = int(it_end)
                except Exception:
                    skipped_rows.append((app_label, "Iter Start/Iter End must be integers"))
                    skipped += 1
                    continue
                if it_start_i > it_end_i:
                    skipped_rows.append((app_label, "Iter Start must be <= Iter End"))
                    skipped += 1
                    continue
                try:
                    weight_f = float(weight)
                except Exception:
                    skipped_rows.append((app_label, "Weight % must be a number"))
                    skipped += 1
                    continue
                if weight_f < 0 or weight_f > 100:
                    skipped_rows.append((app_label, "Weight % must be 0–100"))
                    skipped += 1
                    continue

                if existing_row and (
                    str(existing_row.MSP_SIZE) == size
                    and int(existing_row.ITERATION_START or 0) == it_start_i
                    and int(existing_row.ITERATION_END or 0) == it_end_i
                    and float(existing_row.WEIGHT_PCT or 0.0) == weight_f
                ):
                    skipped_rows.append((app_label, "No changes detected"))
                    skipped += 1
                    continue

                delete_filters = ["TEAMID=%s", "GROUPID=%s"]
                delete_params = [team_id_sel_msp, group_id]
                if has_year_col or has_effective_year_col:
                    delete_filters.append("(YEAR=%s OR COALESCE(EFFECTIVE_YEAR, YEAR)=%s)")
                    delete_params.extend([int(year_sel), int(year_sel)])
                execute(
                    f"DELETE FROM TEAM_MSP_ASSIGNMENTS WHERE {' AND '.join(delete_filters)}",
                    tuple(delete_params),
                )

                insert_cols = ["TEAMID", "GROUPID"]
                insert_params = [team_id_sel_msp, group_id]
                if has_year_col:
                    insert_cols.append("YEAR")
                    insert_params.append(int(year_sel))
                if has_effective_year_col:
                    insert_cols.append("EFFECTIVE_YEAR")
                    insert_params.append(int(year_sel))
                insert_cols.extend(["ITERATION_START", "ITERATION_END", "MSP_SIZE", "WEIGHT_PCT"])
                insert_params.extend([it_start_i, it_end_i, size, weight_f])
                insert_sql = f"""
                    INSERT INTO TEAM_MSP_ASSIGNMENTS ({', '.join(insert_cols)})
                    VALUES ({', '.join(['%s'] * len(insert_cols))})
                """
                execute(insert_sql, tuple(insert_params))
                if existing_row:
                    updated += 1
                else:
                    created += 1

            st.session_state["msp_assign_flash"] = f"Created: {created}, Updated: {updated}, Deleted: {deleted}, Skipped: {skipped}"
            if skipped_rows:
                st.session_state["msp_assign_skipped"] = skipped_rows
            else:
                st.session_state.pop("msp_assign_skipped", None)
            st.session_state.pop(editor_key, None)
            _run_post_save_refresh("rates_msp_assignments_save")
        if not edit_mode:
            st.caption("Enable Edit mode to update assignments.")

if section == "Audit":
    render_audit_section()
