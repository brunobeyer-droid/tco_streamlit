# pages/2_Contracts.py
import uuid
from contextlib import nullcontext
from datetime import date
from typing import Optional, Any, List

import pandas as pd
import streamlit as st
from core.init import init_page, page_loader

from db import (
    list_programs,
    list_teams,
    list_application_groups,
    list_applications,
    ensure_contracts_table,
    upsert_contract,
    sync_contract_invoices,
    fetch_df,
    execute,
)
from core.cache_utils import cache_data_portfolio
from core.freshness import post_write_refresh

from core.canonical_costs import get_unassigned_breakdown
from core.scope import infer_role, read_scope_from_session, scope_label, ROLE_PROGRAM, ROLE_PO
from core.ui import render_page_header, touch_last_updated_status
from utils.ui_patterns import render_empty_state

from utils.app_shell import bootstrap_page
bootstrap_page()

page_theme = init_page("Contracts", page_path=__file__)
_page_theme = page_theme
user = st.session_state.get("auth_user") or {}
if "contracts_op_feedback" not in st.session_state:
    st.session_state["contracts_op_feedback"] = None


def _scope_default_program_team() -> tuple[Optional[str], Optional[str]]:
    """Role-based defaults aligned with core pages scope behavior."""
    try:
        scope = read_scope_from_session(fetch_df)
        role = infer_role(scope)
    except Exception:
        scope = None
        role = None
    if scope is None:
        return None, None
    if role == ROLE_PO:
        prog_name = str((scope.programs or [None])[0] or "").strip() or None
        team_name = str((scope.teams or [None])[0] or "").strip() or None
        return prog_name, team_name
    if role == ROLE_PROGRAM:
        prog_name = str((scope.programs or [None])[0] or "").strip() or None
        return prog_name, None
    return None, None

# Caches and helpers
def cached_list_programs():
    return list_programs()

def cached_list_teams():
    return list_teams()

def cached_list_groups_for_team(team_id: str):
    return fetch_df("SELECT GROUPID, GROUPNAME FROM APPLICATION_GROUPS WHERE TEAMID=%s ORDER BY GROUPNAME", (team_id,))

def cached_list_application_groups():
    return list_application_groups()

def cached_list_applications(team_id: Optional[str] = None, group_id: Optional[str] = None):
    df = list_applications(team_id=team_id)
    if group_id and not df.empty and "GROUPID" in df.columns:
        df = df[df["GROUPID"] == group_id]
    return df

def _to_int_opt(x) -> Optional[int]:
    try:
        if x is None:
            return None
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None

def _display_name_map() -> dict:
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

def _select_team_group_app(prefix: str):
    def _canon_id(v) -> str:
        return str(v or "").strip().upper()

    def _with_upper_aliases(df: pd.DataFrame, cols_needed: list[str]) -> pd.DataFrame:
        out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        if out.empty:
            return out
        cols = {str(c).strip().upper(): c for c in out.columns}
        for c in cols_needed:
            if c not in out.columns and cols.get(c) in out.columns:
                out[c] = out[cols[c]]
        return out

    def _normalize_programs_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        if not out.empty:
            cols = {str(c).strip().upper(): c for c in out.columns}
            if "PROGRAMID" not in out.columns and cols.get("PROGRAMID") in out.columns:
                out["PROGRAMID"] = out[cols["PROGRAMID"]]
            if "PROGRAMNAME" not in out.columns and cols.get("PROGRAMNAME") in out.columns:
                out["PROGRAMNAME"] = out[cols["PROGRAMNAME"]]
            if "PROGRAMNAME_RAW" not in out.columns and cols.get("PROGRAMNAME_RAW") in out.columns:
                out["PROGRAMNAME_RAW"] = out[cols["PROGRAMNAME_RAW"]]
        if "PROGRAMID" not in out.columns:
            out["PROGRAMID"] = ""
        if "PROGRAMNAME" not in out.columns:
            out["PROGRAMNAME"] = out.get("PROGRAMNAME_RAW", "")
        out["PROGRAMID"] = out["PROGRAMID"].astype(str).str.strip()
        out["PROGRAMNAME"] = out["PROGRAMNAME"].astype(str).str.strip()
        if "PROGRAMNAME_RAW" in out.columns:
            blank = out["PROGRAMNAME"] == ""
            out.loc[blank, "PROGRAMNAME"] = out.loc[blank, "PROGRAMNAME_RAW"].astype(str).str.strip()
        out = out[(out["PROGRAMID"] != "") & (out["PROGRAMNAME"] != "")]
        if not out.empty:
            out = out.drop_duplicates(subset=["PROGRAMID", "PROGRAMNAME"]).sort_values("PROGRAMNAME")
        return out

    def _normalize_teams_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        if not out.empty:
            cols = {str(c).strip().upper(): c for c in out.columns}
            if "TEAMID" not in out.columns and cols.get("TEAMID") in out.columns:
                out["TEAMID"] = out[cols["TEAMID"]]
            if "TEAMNAME" not in out.columns and cols.get("TEAMNAME") in out.columns:
                out["TEAMNAME"] = out[cols["TEAMNAME"]]
            if "TEAMNAME_RAW" not in out.columns and cols.get("TEAMNAME_RAW") in out.columns:
                out["TEAMNAME_RAW"] = out[cols["TEAMNAME_RAW"]]
            if "PROGRAMID" not in out.columns and cols.get("PROGRAMID") in out.columns:
                out["PROGRAMID"] = out[cols["PROGRAMID"]]
        if "TEAMID" not in out.columns:
            out["TEAMID"] = ""
        if "TEAMNAME" not in out.columns:
            out["TEAMNAME"] = out.get("TEAMNAME_RAW", "")
        if "PROGRAMID" not in out.columns:
            out["PROGRAMID"] = ""
        out["TEAMID"] = out["TEAMID"].astype(str).str.strip()
        out["TEAMNAME"] = out["TEAMNAME"].astype(str).str.strip()
        if "TEAMNAME_RAW" in out.columns:
            blank = out["TEAMNAME"] == ""
            out.loc[blank, "TEAMNAME"] = out.loc[blank, "TEAMNAME_RAW"].astype(str).str.strip()
        out["PROGRAMID"] = out["PROGRAMID"].astype(str).str.strip()
        out = out[(out["TEAMID"] != "") & (out["TEAMNAME"] != "")]
        return out

    programs_df = cached_list_programs()
    if programs_df is None:
        programs_df = pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME"])
    else:
        programs_df = _with_upper_aliases(programs_df, ["PROGRAMID", "PROGRAMNAME", "PROGRAMNAME_RAW"])
    if programs_df.empty or "PROGRAMNAME" not in programs_df.columns:
        try:
            p_from_teams = fetch_df(
                """
                SELECT DISTINCT
                    p.PROGRAMID,
                    COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME,
                    p.PROGRAMNAME AS PROGRAMNAME_RAW
                FROM TEAMS t
                JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                WHERE LTRIM(RTRIM(COALESCE(NULLIF(p.PROGRAM_DISPLAY_NAME, ''), p.PROGRAMNAME, ''))) <> ''
                ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME)
                """,
                None,
            )
            p_from_teams = _with_upper_aliases(p_from_teams, ["PROGRAMID", "PROGRAMNAME", "PROGRAMNAME_RAW"])
            if isinstance(p_from_teams, pd.DataFrame) and not p_from_teams.empty:
                programs_df = p_from_teams
        except Exception:
            pass
    programs_df = _normalize_programs_df(programs_df)

    prog_names = programs_df["PROGRAMNAME"].tolist() if not programs_df.empty else []
    defaults_key = f"{prefix}_defaults_applied"
    default_prog_name, default_team_name = _scope_default_program_team()
    if not bool(st.session_state.get(defaults_key, False)):
        cur_prog = st.session_state.get(f"{prefix}_prog_sel")
        if (cur_prog is None or cur_prog == "Select Program") and default_prog_name in prog_names:
            st.session_state[f"{prefix}_prog_sel"] = default_prog_name
    sel_prog_name = st.selectbox("Program", options=["Select Program"] + prog_names, index=0, key=f"{prefix}_prog_sel")
    sel_program_id = None
    if sel_prog_name != "Select Program" and not programs_df.empty:
        sel_program_id = programs_df.loc[programs_df["PROGRAMNAME"] == sel_prog_name, "PROGRAMID"].iloc[0]

    teams_df = cached_list_teams()
    if teams_df is None:
        teams_df = pd.DataFrame(columns=["TEAMID", "TEAMNAME", "PROGRAMID"])
    else:
        teams_df = _with_upper_aliases(teams_df, ["TEAMID", "TEAMNAME", "TEAMNAME_RAW", "PROGRAMID"])
    teams_df = _normalize_teams_df(teams_df)

    if sel_program_id:
        teams_df = teams_df.loc[teams_df["PROGRAMID"].map(_canon_id) == _canon_id(sel_program_id)]
        # Fallback by program name join for schema/data variants where PROGRAMID matching fails.
        if teams_df.empty and sel_prog_name != "Select Program":
            try:
                teams_df = fetch_df(
                    """
                    SELECT
                        t.TEAMID,
                        COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
                        t.TEAMNAME AS TEAMNAME_RAW,
                        t.PROGRAMID
                    FROM TEAMS t
                    JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
                    WHERE UPPER(LTRIM(RTRIM(COALESCE(NULLIF(p.PROGRAM_DISPLAY_NAME, ''), p.PROGRAMNAME)))) = UPPER(LTRIM(RTRIM(%s)))
                    ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME)
                    """,
                    (sel_prog_name,),
                )
                teams_df = _with_upper_aliases(teams_df, ["TEAMID", "TEAMNAME", "TEAMNAME_RAW", "PROGRAMID"])
                teams_df = _normalize_teams_df(teams_df)
            except Exception:
                pass
    teams_df = teams_df[(teams_df["TEAMID"] != "") & (teams_df["TEAMNAME"] != "")]
    team_enabled = bool(sel_program_id)
    if team_enabled and teams_df.empty:
        render_empty_state(
            kind="NO_RESULTS_FILTER",
            action_target="Adjust Program/Team filters or add teams in the Teams page",
        )
        return sel_program_id, None, None, None, None

    team_names = teams_df["TEAMNAME"].tolist() if team_enabled else []
    if not bool(st.session_state.get(defaults_key, False)):
        cur_team = st.session_state.get(f"{prefix}_team_sel")
        if (cur_team is None or cur_team == "Select Team") and default_team_name in team_names:
            st.session_state[f"{prefix}_team_sel"] = default_team_name
    sel_team_name = st.selectbox(
        "Team",
        options=(["Select Team"] + team_names) if team_enabled else ["Select Program first"],
        index=0,
        key=f"{prefix}_team_sel",
        disabled=not team_enabled,
        help="Select Program first to load Teams.",
    )
    if not team_enabled:
        st.session_state[defaults_key] = True
        return sel_program_id, None, None, None, None
    if sel_team_name == "Select Team":
        st.session_state[defaults_key] = True
        return sel_program_id, None, None, None, None
    sel_team_row = teams_df.loc[teams_df["TEAMNAME"] == sel_team_name].iloc[0]
    sel_team_id = sel_team_row["TEAMID"]

    groups_df = cached_list_groups_for_team(sel_team_id)
    if groups_df.empty:
        st.session_state[defaults_key] = True
        st.info("This team has no Applications yet.")
        return sel_program_id, sel_team_id, sel_team_name, None, None
    group_names = groups_df["GROUPNAME"].tolist()
    sel_group_name = st.selectbox("Application", options=["Select Group"] + group_names, index=0, key=f"{prefix}_group_sel")
    if sel_group_name == "Select Group":
        st.session_state[defaults_key] = True
        return sel_program_id, sel_team_id, sel_team_name, None, None
    sel_group_row = groups_df.loc[groups_df["GROUPNAME"] == sel_group_name].iloc[0]
    sel_group_id = sel_group_row["GROUPID"]

    apps_df = cached_list_applications(group_id=sel_group_id)
    if apps_df.empty:
        st.session_state[defaults_key] = True
        st.info("No instances in this Application. Create one in the Applications page.")
        return sel_program_id, sel_team_id, sel_team_name, sel_group_id, None

    apps_df = apps_df.copy()
    apps_df["APPLICATIONID"] = apps_df["APPLICATIONID"].fillna("").astype(str).str.strip()
    apps_df["LABEL"] = apps_df.apply(
        lambda r: f"{r['APPLICATIONNAME']}" + (f" — {r['ADD_INFO']}" if r.get("ADD_INFO") else ""),
        axis=1
    )

    def _resolve_default_instance(df: pd.DataFrame, group_id: str) -> Optional[pd.Series]:
        gid = str(group_id or "").strip()
        if gid:
            exact = df.loc[df["APPLICATIONID"].astype(str).str.upper() == f"{gid}__DEFAULT".upper()]
            if not exact.empty:
                return exact.iloc[0]
        if "IS_DEFAULT" in df.columns:
            try:
                is_def = pd.to_numeric(df["IS_DEFAULT"], errors="coerce").fillna(0).astype(int) == 1
                if is_def.any():
                    return df.loc[is_def].iloc[0]
            except Exception:
                pass
        if len(df) == 1:
            return df.iloc[0]
        return None

    default_row = _resolve_default_instance(apps_df, str(sel_group_id))
    multi_instance = len(apps_df) > 1
    show_instance_advanced = st.checkbox(
        "Advanced: choose instance (site/location)",
        value=False,
        key=f"{prefix}_show_instance_advanced",
    )
    show_instance_picker = multi_instance or show_instance_advanced

    if not show_instance_picker and default_row is not None:
        sel_app_id = str(default_row.get("APPLICATIONID") or "").strip() or None
        st.caption("Using the default instance for this Application.")
        st.session_state[defaults_key] = True
        return sel_program_id, sel_team_id, sel_team_name, sel_group_id, sel_app_id

    app_labels = apps_df["LABEL"].tolist()
    default_idx = 0
    if default_row is not None:
        default_label = str(default_row.get("LABEL") or "").strip()
        if default_label in app_labels:
            default_idx = 1 + app_labels.index(default_label)
    sel_app_label = st.selectbox(
        "Instance (site/location)",
        options=["Select Instance"] + app_labels,
        index=default_idx,
        key=f"{prefix}_app_sel",
        help="Only needed for Applications with multiple instances.",
    )
    if sel_app_label == "Select Instance":
        st.session_state[defaults_key] = True
        return sel_program_id, sel_team_id, sel_team_name, sel_group_id, None
    sel_app_row = apps_df.iloc[app_labels.index(sel_app_label)]
    sel_app_id = sel_app_row["APPLICATIONID"]
    st.session_state[defaults_key] = True

    return sel_program_id, sel_team_id, sel_team_name, sel_group_id, sel_app_id


def _fmt_company_code(val: Any) -> str:
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    s = str(val or "").strip()
    if not s:
        return ""
    # Normalize numeric-like strings/floats (e.g., 970.0 -> 970)
    try:
        f = float(s)
        if f.is_integer():
            s = str(int(f))
    except Exception:
        pass
    s = s.replace(".0", "", 1) if s.endswith(".0") else s
    return s.zfill(4) if s.isdigit() else s


def _app_label_for_id(app_id: Optional[str]) -> str:
    app_id = str(app_id or "").strip()
    if not app_id:
        return ""
    try:
        df = fetch_df(
            """
            SELECT
                g.GROUPNAME,
                a.APPLICATIONNAME,
                a.ADD_INFO
            FROM APPLICATIONS a
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
            WHERE a.APPLICATIONID=%s
            """,
            (app_id,),
        )
        if df is None or df.empty:
            return ""
        r = df.iloc[0]
        group_name = str(r.get("GROUPNAME") or "").strip()
        if group_name:
            return group_name
        name = str(r.get("APPLICATIONNAME") or "").strip()
        add = str(r.get("ADD_INFO") or "").strip()
        if name and add:
            return f"{name} — {add}"
        return name
    except Exception:
        return ""


def _set_op_feedback(level: str, message: str) -> None:
    st.session_state["contracts_op_feedback"] = {"level": str(level or "info"), "message": str(message or "")}


def _render_op_feedback() -> None:
    payload = st.session_state.pop("contracts_op_feedback", None)
    if not isinstance(payload, dict):
        return
    level = str(payload.get("level") or "info").lower()
    message = str(payload.get("message") or "").strip()
    if not message:
        return
    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)

# UI
_scope = read_scope_from_session(fetch_df)
_role = infer_role(_scope)
render_page_header(
    "Contracts",
    "Manage contract windows, renewals, and linked recurring invoices.",
    scope_label(_scope, _role),
    _role,
    scope_status_right=touch_last_updated_status("contracts"),
)
_render_op_feedback()
try:
    ensure_contracts_table()
except Exception as e:
    st.error(f"Contracts table unavailable: {e}")

with page_loader(
    messages=[
        "Loading scope and filters...",
        "Building the cost model...",
        "Aggregating...",
        "Preparing charts...",
        "Finalizing view...",
    ]
) as loader:
    loader.step("Loading scope and filters...")
    _ = cached_list_programs()
    _ = cached_list_teams()
    _ = cached_list_application_groups()
    loader.step("Finalizing view...")


# NOTE: `CONTRACTS` is the canonical contract master used by forecasting/cost attribution (and by canonical diagnostics).
current_user_email = str(st.session_state.get("auth_user", {}).get("email", "")).strip() or None
create_tab, edit_tab, search_tab = st.tabs(["Create / Update", "Edit Existing", "Search"])

# Create / Update
with create_tab:
    st.subheader("Create Contract")
    prog_id_k, team_id_k, team_name_k, group_id_k, app_id_k = _select_team_group_app("contract_create")
    existing_contract = None
    existing_start_fy = None
    existing_end_fy = None
    existing_span = 1
    if team_id_k and app_id_k:
        try:
            existing_contract = fetch_df(
                """
                SELECT TOP 1 *
                FROM CONTRACTS
                WHERE APPLICATIONID = %s AND TEAMID = %s
                ORDER BY UPDATED_AT DESC, CREATED_AT DESC
                """,
                (app_id_k, team_id_k)
            )
            existing_contract = existing_contract.iloc[0] if (existing_contract is not None and not existing_contract.empty) else None
            if existing_contract is not None:
                existing_start_fy = _to_int_opt(existing_contract.get("START_FY"))
                existing_end_fy = _to_int_opt(existing_contract.get("END_FY"))
                if existing_start_fy is not None and existing_end_fy is not None and existing_end_fy >= existing_start_fy:
                    existing_span = max(1, existing_end_fy - existing_start_fy + 1)
        except Exception:
            existing_contract = None
    if not (team_id_k and app_id_k):
        st.info("Pick Program → Team → Application to create a contract.")
    allow_create = bool(team_id_k and app_id_k)
    renewal_mode = bool(st.session_state.get("contract_create_show_renewal"))
    if team_id_k and app_id_k and existing_contract is not None and not renewal_mode:
        st.warning(
            "A contract already exists for this Application × Team. "
            "Use Edit Existing to change the current contract. "
            "If you need to draft a future renewal (non-overlapping), click the button below."
        )
        if st.button("Plan a future renewal", key="btn_show_renewal_form"):
            st.session_state["contract_create_show_renewal"] = True
            renewal_mode = True
    if team_id_k and app_id_k and existing_contract is not None and renewal_mode:
        st.info(f"Planning renewal after current END_FY={existing_end_fy or '?'}")
        if st.button("Prefill renewal window from existing contract", key="btn_prefill_renew_contract"):
            span = existing_span
            start_hint = (existing_end_fy + 1) if existing_end_fy is not None else date.today().year
            end_hint = start_hint + span - 1
            esc_hint = float(existing_contract.get("ESCALATION_PCT") or 0.0)
            base_hint = float(existing_contract.get("ANNUAL_AMOUNT") or 0.0)
            base_hint = round(base_hint * (1 + esc_hint/100.0), 2) if base_hint else base_hint
            st.session_state["contract_start_fy2"] = start_hint
            st.session_state["contract_end_fy2"] = end_hint
            st.session_state["contract_renewal_month2"] = int(existing_contract.get("RENEWAL_MONTH") or 1)
            st.session_state["contract_escalation2"] = esc_hint
            st.session_state["contract_amount2"] = base_hint
            st.session_state["contract_company_code2"] = _fmt_company_code(existing_contract.get("COMPANY_CODE"))
            st.session_state["contract_cost_center2"] = str(existing_contract.get("COST_CENTER") or "")
            st.session_state["contract_service_type2"] = str(existing_contract.get("SERVICE_TYPE") or "")
            st.session_state["contract_agr2"] = str(existing_contract.get("AGREEMENT_NUMBER") or "")

    show_form = allow_create and (existing_contract is None or renewal_mode)
    if show_form:
        with st.form("contract_create_form"):
            cc1, cc2, cc3, cc4 = st.columns(4)
            with cc1:
                start_fy_k = st.number_input("Start FY", value=int(date.today().year), step=1, min_value=2000, max_value=2100, key="contract_start_fy2")
            with cc2:
                end_fy_k = st.number_input("End FY", value=int(date.today().year), step=1, min_value=2000, max_value=2100, key="contract_end_fy2")
            with cc3:
                renewal_month_k = st.number_input("Renewal Month", value=1, min_value=1, max_value=12, step=1, key="contract_renewal_month2")
            with cc4:
                escalation_k = st.number_input("Escalation %", value=0.0, min_value=0.0, max_value=100.0, step=0.1, key="contract_escalation2")

            cc5, cc6, cc7 = st.columns(3)
            with cc5:
                base_amount_k = st.number_input("Base Annual Amount (USD)", value=0.0, min_value=0.0, step=0.1, key="contract_amount2")
            with cc6:
                status_k = st.selectbox("Status", ["Active","Terminated"], index=0, key="contract_status2")
            with cc7:
                contract_renewal_dt_k = st.date_input("Contract Renewal Date (optional)", value=None, key="contract_renewal_date2")

            cc8, cc9, cc10 = st.columns(3)
            with cc8:
                comp_code_k = st.text_input("Company Code (default)", value=_fmt_company_code(st.session_state.get("contract_company_code2")), key="contract_company_code2")
            with cc9:
                cost_center_k = st.text_input("Cost Center (default)", value="", key="contract_cost_center2")
            with cc10:
                service_type_k = st.text_input("Service Type (default)", value="", key="contract_service_type2")

            ccl, ccr = st.columns([2,1])
            with ccl:
                agr_k = st.text_input("Agreement # (optional)", value="", key="contract_agr2")
            with ccr:
                total_cost_k = st.number_input("Total Contract Cost (optional)", value=0.0, min_value=0.0, step=0.1, key="contract_total_cost2")
            di1, di2 = st.columns([1,1])
            with di1:
                invoice_renewal_dt_k = st.date_input("Default Invoice Renewal Date (optional)", value=None, key="contract_invoice_renewal_date2")

            save_c = st.form_submit_button("Save Contract", type="primary")

        invalid_window = start_fy_k > end_fy_k
        invalid_overlap_current = bool(
            renewal_mode and existing_contract is not None and existing_end_fy is not None and start_fy_k <= existing_end_fy
        )
        if save_c and invalid_overlap_current:
            st.error(f"Renewal must start after the current contract ends (current END_FY={existing_end_fy}). Use Edit Existing to change the current window.")
        if save_c and invalid_window:
            st.error("Start FY must be less than or equal to End FY.")
        if save_c:
            if invalid_window or invalid_overlap_current:
                st.stop()
            try:
                contract_id_k = str(uuid.uuid5(uuid.NAMESPACE_URL, f"contract:{app_id_k}:{team_id_k}"))
                try:
                    overlap = fetch_df(
                        """
                        SELECT TOP 1 c.CONTRACT_ID, t.TEAMNAME, c.START_FY, c.END_FY
                        FROM CONTRACTS c
                        LEFT JOIN TEAMS t ON t.TEAMID = c.TEAMID
                        WHERE c.APPLICATIONID = %s
                          AND NOT (c.END_FY < %s OR c.START_FY > %s)
                          AND NOT (c.TEAMID = %s)
                        ORDER BY c.CREATED_AT DESC
                        """,
                        (app_id_k, int(start_fy_k), int(end_fy_k), team_id_k)
                    )
                except Exception:
                    overlap = None
                if overlap is not None and not overlap.empty:
                    other_team = str(overlap.iloc[0].get("TEAMNAME") or overlap.iloc[0].get("TEAMID") or "(unknown team)")
                    st.error(f"Another contract exists for this Application during the selected period (Team: {other_team}). Overlapping contracts per Application are not allowed.")
                else:
                    sync_warn: Optional[str] = None
                    status_ctx = st.status("Saving contract...", expanded=False) if hasattr(st, "status") else nullcontext()
                    with status_ctx as op:
                        upsert_contract(
                            contract_id=contract_id_k,
                            application_id=app_id_k,
                            team_id=team_id_k,
                            start_fy=int(start_fy_k),
                            end_fy=int(end_fy_k),
                            renewal_month=int(renewal_month_k),
                            annual_amount=float(base_amount_k),
                            escalation_pct=float(escalation_k or 0.0),
                            status=status_k,
                            agreement_number=(agr_k.strip() or None),
                            company_code=_fmt_company_code(comp_code_k),
                            cost_center=(cost_center_k.strip() or None),
                            service_type=(service_type_k.strip() or None),
                            contract_renewal_date=(contract_renewal_dt_k.isoformat() if contract_renewal_dt_k else None),
                            invoice_renewal_date=(invoice_renewal_dt_k.isoformat() if invoice_renewal_dt_k else None),
                            total_contract_cost=(float(total_cost_k) if total_cost_k else None),
                            updated_by=current_user_email,
                        )
                        if op is not None:
                            op.update(label="Contract saved.", state="running")
                    app_label = _app_label_for_id(app_id_k)
                    contract_label = " — ".join([x for x in [team_name_k, app_label] if x])
                    try:
                        n = sync_contract_invoices(contract_id_k)
                        if op is not None:
                            op.update(label=f"Contract saved. Planned invoices synced for {n} fiscal year(s).", state="complete")
                        _set_op_feedback(
                            "success",
                            f"Contract saved{': ' + contract_label if contract_label else ''}. Planned invoices synced for {n} fiscal year(s).",
                        )
                        st.success(
                            f"Contract saved{': ' + contract_label if contract_label else ''}. "
                            f"Planned invoices synced for {n} fiscal year(s)."
                        )
                    except Exception as e:
                        sync_warn = str(e)
                        if op is not None:
                            op.update(label="Contract saved. Planned invoice sync failed.", state="complete")
                        _set_op_feedback(
                            "warning",
                            f"Contract saved{': ' + contract_label if contract_label else ''}. Planned invoice sync failed: {sync_warn}",
                        )
                        st.success(f"Contract saved{': ' + contract_label if contract_label else ''}.")
                        st.warning(f"Planned invoice sync failed: {e}")
                    post_write_refresh("contract_create_or_update", rerun=True, bump_version=True)
            except Exception as e:
                _set_op_feedback("error", f"Save failed: {e}")
                st.error(f"Save failed: {e}")
    # Quick preview
    with st.expander("Auto-created planned invoices for this contract (Recurring)", expanded=False):
        st.caption("These are recurring planned invoices created from the contract. Save already syncs them; use Edit to resync later if needed.")
        if team_id_k and app_id_k and st.button("Show planned invoices", key="btn_show_planned_invoices"):
            try:
                q = """
                    SELECT INVOICEID, FISCAL_YEAR, RENEWALDATE, AMOUNT, STATUS, COALESCE(INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE
                    FROM INVOICES
                    WHERE APPLICATIONID = %s AND TEAMID = %s AND COALESCE(INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'
                    ORDER BY FISCAL_YEAR, RENEWALDATE
                """
                res = fetch_df(q, (app_id_k, team_id_k))
                if res is None or res.empty:
                    render_empty_state(
                        kind="NO_RESULTS_FILTER",
                        action_target="Create/extend contract years to generate planned recurring invoices",
                    )
                else:
                    st.dataframe(res, use_container_width=True, height=300)
            except Exception as e:
                st.error(f"Preview failed: {e}")

# Edit
with edit_tab:
    st.subheader("Edit Contract")
    try:
        df_edit = fetch_df(
            """
            SELECT c.CONTRACT_ID,
                   c.APPLICATIONID, a.APPLICATIONNAME,
                   c.TEAMID, COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
                   g.GROUPNAME AS APPLICATION_GROUP,
                   c.START_FY, c.END_FY, c.RENEWAL_MONTH,
                   c.ANNUAL_AMOUNT, c.ESCALATION_PCT, c.STATUS, c.AGREEMENT_NUMBER,
                   c.COMPANY_CODE, c.COST_CENTER, c.SERVICE_TYPE,
                   c.CONTRACT_RENEWAL_DATE, c.INVOICE_RENEWAL_DATE,
                   c.TOTAL_CONTRACT_COST
            FROM CONTRACTS c
            LEFT JOIN APPLICATIONS a ON a.APPLICATIONID = c.APPLICATIONID
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
            LEFT JOIN TEAMS t ON t.TEAMID = c.TEAMID
            ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME), g.GROUPNAME, a.APPLICATIONNAME
            """
        )
    except Exception as e:
        df_edit = pd.DataFrame()
        st.error(f"Load contracts failed: {e}")

    if df_edit is None or df_edit.empty:
        render_empty_state(
            kind="NO_DATA_SCOPE",
            action_target="Create a contract in the Create / Update tab",
        )
    else:
        df_edit = df_edit.copy()
        if "APPLICATION_GROUP" not in df_edit.columns:
            df_edit["APPLICATION_GROUP"] = df_edit.get("APPLICATIONNAME", "")
        df_edit["LABEL"] = df_edit.apply(lambda r: f"{r['TEAMNAME']} — {r['APPLICATION_GROUP']} ({r['START_FY']}-{r['END_FY']})", axis=1)
        labels = df_edit["LABEL"].tolist()
        sel_label = st.selectbox("Select a contract", options=["Select…"] + labels, index=0, key="contract_edit_picker")
        if sel_label != "Select…":
            r = df_edit.iloc[labels.index(sel_label)]
            with st.form("contract_edit_form"):
                ec1, ec2, ec3, ec4 = st.columns(4)
                with ec1:
                    e_start = st.number_input("Start FY", value=int(r.get("START_FY") or date.today().year), step=1, min_value=2000, max_value=2100, key="contract_edit_start_fy")
                with ec2:
                    e_end = st.number_input("End FY", value=int(r.get("END_FY") or date.today().year), step=1, min_value=2000, max_value=2100, key="contract_edit_end_fy")
                with ec3:
                    e_renew_m = st.number_input("Renewal Month", value=int(r.get("RENEWAL_MONTH") or 1), min_value=1, max_value=12, step=1, key="contract_edit_renewal_month")
                with ec4:
                    e_escal = st.number_input("Escalation %", value=float(r.get("ESCALATION_PCT") or 0.0), min_value=0.0, max_value=100.0, step=0.1, key="contract_edit_escalation")

                ec5, ec6, ec7 = st.columns(3)
                with ec5:
                    e_amount = st.number_input("Base Annual Amount (USD)", value=float(r.get("ANNUAL_AMOUNT") or 0.0), min_value=0.0, step=0.1, key="contract_edit_amount")
                with ec6:
                    e_status = st.selectbox("Status", ["Active","Terminated"], index=0 if (str(r.get("STATUS") or "Active").strip().capitalize() == "Active") else 1, key="contract_edit_status")
                with ec7:
                    e_c_renew = st.date_input("Contract Renewal Date (optional)", value=(pd.to_datetime(r.get("CONTRACT_RENEWAL_DATE")).date() if pd.notna(r.get("CONTRACT_RENEWAL_DATE")) else None), key="contract_edit_contract_renewal_date")

                ec8, ec9, ec10 = st.columns(3)
                with ec8:
                    e_ccode = st.text_input("Company Code (default)", value=_fmt_company_code(r.get("COMPANY_CODE")), key="contract_edit_company_code")
                with ec9:
                    e_ccenter = st.text_input("Cost Center (default)", value=str(r.get("COST_CENTER") or ""), key="contract_edit_cost_center")
                with ec10:
                    e_stype = st.text_input("Service Type (default)", value=str(r.get("SERVICE_TYPE") or ""), key="contract_edit_service_type")

                el, er = st.columns([2,1])
                with el:
                    e_agr = st.text_input("Agreement # (optional)", value=str(r.get("AGREEMENT_NUMBER") or ""), key="contract_edit_agr")
                with er:
                    e_tcost = st.number_input("Total Contract Cost (optional)", value=float(r.get("TOTAL_CONTRACT_COST") or 0.0), min_value=0.0, step=0.1, key="contract_edit_total_cost")
                ed1, ed2 = st.columns([1,1])
                with ed1:
                    e_inv_renew = st.date_input("Default Invoice Renewal Date (optional)", value=(pd.to_datetime(r.get("INVOICE_RENEWAL_DATE")).date() if pd.notna(r.get("INVOICE_RENEWAL_DATE")) else None), key="contract_edit_invoice_renewal_date")

                save_e = st.form_submit_button("Save Changes", type="primary")

            invalid_window_edit = e_start > e_end
            if save_e and invalid_window_edit:
                st.error("Start FY must be less than or equal to End FY.")
                st.stop()

            try:
                app_for_edit = str(r.get("APPLICATIONID"))
                team_for_edit = str(r.get("TEAMID"))
                prev_del = fetch_df(
                    """
                    SELECT INVOICEID, FISCAL_YEAR, RENEWALDATE, AMOUNT, STATUS, COALESCE(INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE
                    FROM INVOICES
                    WHERE APPLICATIONID = %s AND TEAMID = %s
                      AND COALESCE(INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'
                      AND STATUS = 'Planned' AND FISCAL_YEAR > %s
                    ORDER BY FISCAL_YEAR, RENEWALDATE
                    """,
                    (app_for_edit, team_for_edit, int(e_end) if e_end else None)
                )
                if prev_del is not None and not prev_del.empty:
                    st.warning(f"Saving with END_FY={int(e_end)} will delete {len(prev_del)} planned recurring invoice(s) in future fiscal years.")
                    with st.expander("Show planned invoices that will be removed", expanded=False):
                        st.dataframe(prev_del, use_container_width=True, height=220)
            except Exception:
                pass

            if save_e:
                try:
                    try:
                        overlap2 = fetch_df(
                            """
                            SELECT TOP 1 c.CONTRACT_ID, t.TEAMNAME, c.START_FY, c.END_FY
                            FROM CONTRACTS c
                            LEFT JOIN TEAMS t ON t.TEAMID = c.TEAMID
                            WHERE c.APPLICATIONID = %s
                              AND NOT (c.END_FY < %s OR c.START_FY > %s)
                              AND NOT (c.TEAMID = %s AND c.CONTRACT_ID = %s)
                            ORDER BY c.CREATED_AT DESC
                            """,
                            (str(r.get("APPLICATIONID")), int(e_start), int(e_end), str(r.get("TEAMID")), str(r.get("CONTRACT_ID")))
                        )
                    except Exception:
                        overlap2 = None
                    if overlap2 is not None and not overlap2.empty:
                        other_team2 = str(overlap2.iloc[0].get("TEAMNAME") or overlap2.iloc[0].get("TEAMID") or "(unknown team)")
                        st.error(f"Another contract exists for this Application during the selected period (Team: {other_team2}). Overlapping contracts per Application are not allowed.")
                        raise RuntimeError("Overlapping contract detected.")
                    status_ctx = st.status("Saving contract changes...", expanded=False) if hasattr(st, "status") else nullcontext()
                    with status_ctx as op:
                        upsert_contract(
                            contract_id=str(r.get("CONTRACT_ID")),
                            application_id=str(r.get("APPLICATIONID")),
                            team_id=str(r.get("TEAMID")),
                            start_fy=int(e_start),
                            end_fy=int(e_end),
                            renewal_month=int(e_renew_m),
                            annual_amount=float(e_amount),
                            escalation_pct=float(e_escal or 0.0),
                            status=str(e_status),
                            agreement_number=(e_agr.strip() or None),
                            company_code=_fmt_company_code(e_ccode),
                            cost_center=(e_ccenter.strip() or None),
                            service_type=(e_stype.strip() or None),
                            contract_renewal_date=(e_c_renew.isoformat() if e_c_renew else None),
                            invoice_renewal_date=(e_inv_renew.isoformat() if e_inv_renew else None),
                            total_contract_cost=(float(e_tcost) if e_tcost else None),
                            updated_by=current_user_email,
                        )
                        if op is not None:
                            op.update(label="Contract updated.", state="running")
                    team_label = str(r.get("TEAMNAME") or "").strip()
                    app_label = str(r.get("APPLICATION_GROUP") or "").strip()
                    if not app_label:
                        app_label = _app_label_for_id(str(r.get("APPLICATIONID") or ""))
                    contract_label = " — ".join([x for x in [team_label, app_label] if x])
                    try:
                        n3 = sync_contract_invoices(str(r.get("CONTRACT_ID")))
                        if op is not None:
                            op.update(label=f"Contract updated. Planned invoices synced for {n3} fiscal year(s).", state="complete")
                        _set_op_feedback(
                            "success",
                            f"Contract updated{': ' + contract_label if contract_label else ''}. Planned invoices synced for {n3} fiscal year(s).",
                        )
                        st.success(
                            f"Contract updated{': ' + contract_label if contract_label else ''}. "
                            f"Planned invoices synced for {n3} fiscal year(s)."
                        )
                    except Exception as e:
                        if op is not None:
                            op.update(label="Contract updated. Planned invoice sync failed.", state="complete")
                        _set_op_feedback(
                            "warning",
                            f"Contract updated{': ' + contract_label if contract_label else ''}. Planned invoice sync failed: {e}",
                        )
                        st.success(f"Contract updated{': ' + contract_label if contract_label else ''}.")
                        st.warning(f"Planned invoice sync failed: {e}")
                    post_write_refresh("contract_edit_update", rerun=True, bump_version=True)
                except Exception as e:
                    _set_op_feedback("error", f"Update failed: {e}")
                    st.error(f"Update failed: {e}")

            # Change log for this Application × Team
            with st.expander("Change log for this Application × Team", expanded=False):
                try:
                    log_df = fetch_df(
                        """
                        SELECT TOP 50
                          c.START_FY, c.END_FY, c.RENEWAL_MONTH, c.ANNUAL_AMOUNT, c.ESCALATION_PCT, c.STATUS,
                          c.COMPANY_CODE, c.COST_CENTER, c.SERVICE_TYPE, c.AGREEMENT_NUMBER,
                          c.UPDATED_AT,
                          COALESCE(
                            NULLIF(LTRIM(RTRIM(au.DISPLAY_NAME)), ''),
                            NULLIF(LTRIM(RTRIM(c.UPDATED_BY)), ''),
                            'Unknown'
                          ) AS UPDATED_BY
                        FROM CONTRACTS c
                        OUTER APPLY (
                          SELECT TOP 1 DISPLAY_NAME
                          FROM APP_USERS au
                          WHERE UPPER(LTRIM(RTRIM(au.EMAIL))) = UPPER(LTRIM(RTRIM(c.UPDATED_BY)))
                             OR UPPER(LTRIM(RTRIM(au.DISPLAY_NAME))) = UPPER(LTRIM(RTRIM(c.UPDATED_BY)))
                        ) au
                        WHERE c.APPLICATIONID = %s AND c.TEAMID = %s
                        ORDER BY c.UPDATED_AT DESC
                        """,
                        (str(r.get("APPLICATIONID")), str(r.get("TEAMID"))),
                    )
                    if log_df is None or log_df.empty:
                        st.info("No contract changes found for this selection.")
                    else:
                        st.dataframe(log_df, use_container_width=True, height=320)
                except Exception as e:
                    st.warning(f"Could not load change log: {e}")
            ecols = st.columns([1,1])
            with ecols[0]:
                with st.expander("Delete Contract and its recurring invoices", expanded=False):
                    try:
                        appid = str(r.get("APPLICATIONID"))
                        teamid = str(r.get("TEAMID"))
                        invs_preview = fetch_df(
                            """
                            SELECT INVOICEID, FISCAL_YEAR, RENEWALDATE, AMOUNT, STATUS,
                                   COALESCE(INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE
                            FROM INVOICES
                            WHERE APPLICATIONID = %s AND TEAMID = %s
                              AND COALESCE(INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'
                            ORDER BY FISCAL_YEAR, RENEWALDATE
                            """,
                            (appid, teamid)
                        )
                        total_n = 0 if invs_preview is None or invs_preview.empty else len(invs_preview)
                        planned_n = 0 if invs_preview is None or invs_preview.empty else int((invs_preview["STATUS"].astype(str).str.strip() == "Planned").sum())
                        completed_n = total_n - planned_n
                        keep_completed = st.checkbox("Keep Completed invoices (delete only Planned)", value=False, key="keep_completed_del_edit")
                        if keep_completed:
                            del_preview = invs_preview[invs_preview["STATUS"].astype(str).str.strip() == "Planned"] if invs_preview is not None and not invs_preview.empty else invs_preview
                            st.caption(f"This will delete the contract and {planned_n} Planned recurring invoice(s). Completed invoices will be preserved ({completed_n}).")
                        else:
                            del_preview = invs_preview
                            st.caption(f"This will delete the contract and {total_n} recurring invoice(s): {planned_n} Planned, {completed_n} Completed.")
                        if del_preview is not None and not del_preview.empty:
                            with st.expander("Show invoices to be deleted", expanded=False):
                                st.dataframe(del_preview, use_container_width=True, height=220)
                    except Exception as _e:
                        st.info("Preview unavailable; proceed with caution.")
                    danger1 = st.checkbox("I understand this permanently deletes the contract and its recurring invoices.", value=False, key="confirm_del_contract_edit")
                    if st.button("Delete Contract + Recurring Invoices", type="secondary", disabled=not danger1, key="btn_del_contract_edit_confirm"):
                        delete_status = st.status("Deleting contract and recurring invoices...", expanded=False) if hasattr(st, "status") else None
                        try:
                            if keep_completed:
                                execute(
                                    "DELETE FROM INVOICES WHERE APPLICATIONID = %s AND TEAMID = %s AND COALESCE(INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice' AND STATUS = 'Planned'",
                                    (appid, teamid)
                                )
                            else:
                                execute(
                                    "DELETE FROM INVOICES WHERE APPLICATIONID = %s AND TEAMID = %s AND COALESCE(INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'",
                                    (appid, teamid)
                                )
                            execute("DELETE FROM CONTRACTS WHERE CONTRACT_ID = %s", (str(r.get("CONTRACT_ID")),))
                            if delete_status is not None:
                                delete_status.update(label="Contract and recurring invoices deleted.", state="complete")
                            st.success("Contract and its recurring invoices deleted.")
                            post_write_refresh("contract_delete", rerun=False, bump_version=True)
                        except Exception as e:
                            if delete_status is not None:
                                delete_status.update(label="Delete failed.", state="error")
                            st.error(f"Delete failed: {e}")
            with ecols[1]:
                if st.button("Show planned invoices", key="btn_show_planned_invoices_edit"):
                    try:
                        q2 = """
                            SELECT INVOICEID, FISCAL_YEAR, RENEWALDATE, AMOUNT, STATUS, COALESCE(INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE
                            FROM INVOICES
                            WHERE APPLICATIONID = %s AND TEAMID = %s AND COALESCE(INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'
                            ORDER BY FISCAL_YEAR, RENEWALDATE
                        """
                        res2 = fetch_df(q2, (str(r.get("APPLICATIONID")), str(r.get("TEAMID"))))
                        if res2 is None or res2.empty:
                            render_empty_state(
                                kind="NO_RESULTS_FILTER",
                                action_target="Sync planned invoices from this contract after saving",
                            )
                        else:
                            st.dataframe(res2, use_container_width=True, height=260)
                    except Exception as e:
                        st.error(f"Preview failed: {e}")

# Search
with search_tab:
    st.subheader("Search Contracts")
    try:
        dfc = fetch_df(
            """
            SELECT c.CONTRACT_ID,
                   c.APPLICATIONID,
                   a.APPLICATIONNAME,
                   g.GROUPNAME AS APPLICATION_GROUP,
                   c.TEAMID,
                   COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
                   p.PROGRAMID,
                   COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME,
                   c.START_FY, c.END_FY, c.RENEWAL_MONTH,
                   c.ANNUAL_AMOUNT, c.ESCALATION_PCT, c.STATUS, c.AGREEMENT_NUMBER,
                   c.COMPANY_CODE, c.COST_CENTER, c.SERVICE_TYPE,
                   c.CONTRACT_RENEWAL_DATE, c.INVOICE_RENEWAL_DATE,
                   c.TOTAL_CONTRACT_COST,
                   c.CREATED_AT
            FROM CONTRACTS c
            LEFT JOIN APPLICATIONS a ON a.APPLICATIONID = c.APPLICATIONID
            LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
            LEFT JOIN TEAMS t ON t.TEAMID = c.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
            """
        )
    except Exception as e:
        st.error(f"Read contracts failed: {e}")
        dfc = pd.DataFrame()

    if dfc is None or dfc.empty:
        render_empty_state(
            kind="NO_DATA_SCOPE",
            action_target="Create a contract in Create / Update",
        )
    else:
        dfc = dfc.copy()
        if "APPLICATION_GROUP" not in dfc.columns:
            dfc["APPLICATION_GROUP"] = dfc.get("APPLICATIONNAME", "")
        search_defaults_applied = bool(st.session_state.get("contract_search_defaults_applied", False))
        default_prog_name, default_team_name = _scope_default_program_team()
        fcols = st.columns(6)
        with fcols[0]:
            prog_names_f = ["All"] + sorted(dfc["PROGRAMNAME"].dropna().unique().tolist())
            if not search_defaults_applied:
                cur_prog = st.session_state.get("contract_search_prog")
                if (cur_prog is None) or (cur_prog not in prog_names_f) or (cur_prog == "All"):
                    desired_prog = default_prog_name if default_prog_name in prog_names_f else "All"
                    st.session_state["contract_search_prog"] = desired_prog
            else:
                cur_prog = st.session_state.get("contract_search_prog")
                if cur_prog is not None and cur_prog not in prog_names_f:
                    st.session_state["contract_search_prog"] = "All"
            sel_prog_f = st.selectbox("Program", options=prog_names_f, index=0, key="contract_search_prog")
        with fcols[1]:
            team_search_enabled = sel_prog_f != "All"
            team_names_f = (
                ["All"] + sorted(
                    dfc.loc[dfc["PROGRAMNAME"].astype(str).str.strip().str.upper() == str(sel_prog_f).strip().upper(), "TEAMNAME"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                    .tolist()
                )
                if team_search_enabled
                else ["Select Program first"]
            )
            if not search_defaults_applied:
                cur_team = st.session_state.get("contract_search_team")
                if (cur_team is None) or (cur_team not in team_names_f) or (cur_team == "All"):
                    desired_team = default_team_name if default_team_name in team_names_f else "All"
                    st.session_state["contract_search_team"] = desired_team
            else:
                cur_team = st.session_state.get("contract_search_team")
                if cur_team is not None and cur_team not in team_names_f:
                    st.session_state["contract_search_team"] = "All"
            sel_team_f = st.selectbox(
                "Team",
                options=team_names_f,
                index=0,
                key="contract_search_team",
                disabled=not team_search_enabled,
                help="Select Program first to load Teams.",
            )
        if not search_defaults_applied:
            st.session_state["contract_search_defaults_applied"] = True
        with fcols[2]:
            team_filter_enabled = bool(team_search_enabled and sel_team_f != "All" and sel_team_f != "Select Program first")
            app_names_f = (
                ["All"] + sorted(
                    dfc.loc[dfc["TEAMNAME"].astype(str).str.strip().str.upper() == str(sel_team_f).strip().upper(), "APPLICATION_GROUP"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                    .tolist()
                )
                if team_filter_enabled
                else ["Select Team first"]
            )
            sel_app_f = st.selectbox(
                "Application",
                options=app_names_f,
                index=0,
                key="contract_search_app",
                disabled=not team_filter_enabled,
                help="Select Program and Team first to load Applications.",
            )
        with fcols[3]:
            app_instance_names_f = (
                ["All"] + sorted(
                    dfc.loc[dfc["TEAMNAME"].astype(str).str.strip().str.upper() == str(sel_team_f).strip().upper(), "APPLICATIONNAME"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                    .tolist()
                )
                if team_filter_enabled
                else ["Select Team first"]
            )
            sel_app_instance_f = st.selectbox(
                "Instance (site/location)",
                options=app_instance_names_f,
                index=0,
                key="contract_search_app_instance",
                disabled=not team_filter_enabled,
                help="Select Program and Team first to load instances.",
            )
        with fcols[4]:
            status_f = st.selectbox("Status", options=["All", "Active", "Terminated"], index=0, key="contract_search_status")
        with fcols[5]:
            agr_like = st.text_input("Agreement # contains", value="", key="contract_search_agr_like").strip()

        fy_cols = st.columns(2)
        with fy_cols[0]:
            start_from = st.number_input("Start FY from", value=2000, min_value=1990, max_value=2100, step=1, key="contract_search_start_from")
        with fy_cols[1]:
            start_to = st.number_input("Start FY to", value=2100, min_value=1990, max_value=2100, step=1, key="contract_search_start_to")

        filt = dfc
        if sel_prog_f != "All":
            filt = filt[
                filt["PROGRAMNAME"].astype(str).str.strip().str.upper()
                == str(sel_prog_f).strip().upper()
            ]
        if sel_team_f != "All":
            filt = filt[
                filt["TEAMNAME"].astype(str).str.strip().str.upper()
                == str(sel_team_f).strip().upper()
            ]
        if sel_app_f != "All":
            filt = filt[
                filt["APPLICATION_GROUP"].astype(str).str.strip().str.upper()
                == str(sel_app_f).strip().upper()
            ]
        if sel_app_instance_f != "All":
            filt = filt[
                filt["APPLICATIONNAME"].astype(str).str.strip().str.upper()
                == str(sel_app_instance_f).strip().upper()
            ]
        if status_f != "All":
            filt = filt[filt["STATUS"] == status_f]
        if agr_like:
            filt = filt[filt["AGREEMENT_NUMBER"].fillna("").str.contains(agr_like, case=False, na=False)]
        filt = filt[(filt["START_FY"] >= start_from) & (filt["START_FY"] <= start_to)]

        if filt.empty:
            render_empty_state(
                kind="NO_RESULTS_FILTER",
                action_target="Broaden filters or clear one filter at a time",
            )
        else:
            filt = filt.copy()
            filt["COMPANY_CODE"] = filt["COMPANY_CODE"].apply(_fmt_company_code)
            display_cols = [
                "PROGRAMNAME", "TEAMNAME", "APPLICATION_GROUP",
                "START_FY", "END_FY", "ANNUAL_AMOUNT", "ESCALATION_PCT",
                "STATUS", "AGREEMENT_NUMBER", "COMPANY_CODE", "COST_CENTER",
                "SERVICE_TYPE", "CONTRACT_RENEWAL_DATE", "INVOICE_RENEWAL_DATE", "TOTAL_CONTRACT_COST"
            ]
            st.dataframe(filt[display_cols], use_container_width=True, height=280)
            labels = filt.apply(lambda r: f"{r['PROGRAMNAME']} / {r['TEAMNAME']} / {r['APPLICATION_GROUP']} ({r['START_FY']}-{r['END_FY']})", axis=1).tolist()
            sel_contract_label = st.selectbox("Select a contract to view invoices", options=["Select…"] + labels, index=0, key="contract_search_picker")
            if sel_contract_label != "Select…":
                sel_row = filt.iloc[labels.index(sel_contract_label)]
                try:
                    invs = fetch_df(
                        """
                        SELECT FISCAL_YEAR, RENEWALDATE, AMOUNT, STATUS, COALESCE(INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE,
                               COMPANY_CODE, COST_CENTER, SERVICE_TYPE, AGREEMENT_NUMBER, CONTRACT_DUE
                        FROM INVOICES
                        WHERE APPLICATIONID = %s AND TEAMID = %s
                          AND COALESCE(INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'
                        ORDER BY FISCAL_YEAR, RENEWALDATE
                        """,
                        (str(sel_row["APPLICATIONID"]), str(sel_row["TEAMID"]))
                    )
                    if invs is None or invs.empty:
                        st.info("No invoices found for this contract.")
                    else:
                        invs_disp = invs.rename(columns={
                            "FISCAL_YEAR": "Fiscal Year",
                            "RENEWALDATE": "Renewal Date",
                            "AMOUNT": "Amount (USD)",
                            "STATUS": "Status",
                            "INVOICE_TYPE": "Invoice Type",
                            "COMPANY_CODE": "Company Code",
                            "COST_CENTER": "Cost Center",
                            "SERVICE_TYPE": "Service Type",
                            "AGREEMENT_NUMBER": "Agreement #",
                            "CONTRACT_DUE": "Contract Due (FY)"
                        })[[
                            "Fiscal Year", "Renewal Date", "Amount (USD)", "Status", "Invoice Type",
                            "Company Code", "Cost Center", "Service Type", "Agreement #", "Contract Due (FY)"
                        ]]
                        invs_disp["Company Code"] = invs_disp["Company Code"].apply(_fmt_company_code)
                        st.markdown("**Recurring invoices for this contract**")
                        st.dataframe(invs_disp, use_container_width=True, height=280)
                except Exception as e:
                    st.error(f"Failed to load invoices: {e}")

# Data quality diagnostics using canonical cost API (unassigned contract costs)
with st.expander("Data quality – unassigned contract costs", expanded=False):
    filters: dict[str, object] = {}
    try:
        if start_from == start_to:
            filters["year"] = int(start_from)
    except Exception:
        pass
    try:
        if sel_prog_f != "All":
            filters["program"] = sel_prog_f
    except Exception:
        pass
    try:
        if sel_team_f != "All":
            filters["team"] = sel_team_f
    except Exception:
        pass

    df_unassigned = get_unassigned_breakdown(fetch_df, scenario="Projected", filters=filters or None)
    if df_unassigned is None or df_unassigned.empty:
        st.caption("No unassigned contract-related costs found for the current filters.")
    else:
        df_contract = df_unassigned.copy()
        if "SOURCE" in df_contract.columns:
            df_contract = df_contract[df_contract["SOURCE"].fillna("").astype(str).str.upper() == "CONTRACT"].copy()
        else:
            df_contract = pd.DataFrame()

        if df_contract.empty:
            st.caption("No unassigned costs from contracts for the current filters.")
        else:
            if "TOTAL_COST" in df_contract.columns:
                df_contract["TOTAL_COST"] = pd.to_numeric(df_contract.get("TOTAL_COST"), errors="coerce").fillna(0.0)
                total_unassigned = float(df_contract["TOTAL_COST"].sum())
                st.markdown(f"**Total unassigned contract cost:** ${total_unassigned:,.2f}")

            numeric_cols = [c for c in ["RECORDS", "TOTAL_COST"] if c in df_contract.columns]
            if "RECORDS" in df_contract.columns:
                df_contract["RECORDS"] = pd.to_numeric(df_contract.get("RECORDS"), errors="coerce").fillna(0).astype(int)

            grouping_cols = [c for c in ["ISSUE", "COST_CATEGORY", "SUBCOMPONENT", "YEAR", "PI"] if c in df_contract.columns]
            if grouping_cols and numeric_cols:
                summary = (
                    df_contract.groupby(grouping_cols, dropna=False, as_index=False)[numeric_cols]
                    .sum()
                )
                if "TOTAL_COST" in summary.columns:
                    summary = summary.sort_values("TOTAL_COST", ascending=False)
                st.dataframe(summary, use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_contract, use_container_width=True, hide_index=True)

            st.caption(
                "These rows represent contract-related costs that are missing some mapping (for example: program/team, app group, PI/year, or linkage). "
                "Fix the underlying master data/mappings so these costs stop appearing as “Unassigned” in analytical pages."
            )
