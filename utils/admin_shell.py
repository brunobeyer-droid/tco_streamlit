from __future__ import annotations

import json
from typing import Optional, Iterable

import pandas as pd
import streamlit as st

from db import fetch_df
from utils.admin_audit import get_recent_admin_actions, ensure_admin_audit_log_table
from utils.admin_context import (
    get_admin_context,
    set_admin_context,
    get_default_admin_context,
    load_program_options,
    load_team_options,
    load_year_options,
    load_pi_options,
)

# Admin UI shell helpers.
# To extend: add new audit "area" strings in pages/99_Admin.py and log via utils.admin_audit.log_admin_action.


def _db_status_label() -> str:
    try:
        df = fetch_df("SELECT DB_NAME() AS DB_NAME")
        if isinstance(df, pd.DataFrame) and not df.empty:
            name = str(df.iloc[0].get("DB_NAME") or "").strip()
            if name:
                return f"DB: {name}"
    except Exception:
        pass
    return "DB: Connected"


def render_admin_header() -> None:
    """Render the shared admin header with environment banner."""
    try:
        ensure_admin_audit_log_table()
    except Exception:
        pass
    st.title("Admin Console")
    st.caption("Administrator workspace for high-impact actions. Use with care.")
    st.info(_db_status_label())


def render_context_bar() -> None:
    """Render the shared admin context bar with global scope filters."""
    def _normalize_option_pairs(raw_options: Iterable[object]) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        if raw_options is None:
            return pairs
        if isinstance(raw_options, pd.DataFrame):
            if raw_options.empty:
                return pairs
            cols_upper = {str(c).strip().upper(): c for c in raw_options.columns}
            id_col = cols_upper.get("PROGRAMID") or cols_upper.get("TEAMID") or cols_upper.get("ID")
            name_col = cols_upper.get("PROGRAMNAME") or cols_upper.get("TEAMNAME") or cols_upper.get("NAME")
            if id_col and name_col:
                for _, row in raw_options.iterrows():
                    oid = str(row.get(id_col) or "").strip()
                    oname = str(row.get(name_col) or "").strip()
                    if oid and oname:
                        pairs.append((oid, oname))
                return pairs
            raw_iter = raw_options.to_dict(orient="records")
        else:
            try:
                raw_iter = list(raw_options)
            except Exception:
                raw_iter = []

        for item in raw_iter:
            try:
                if isinstance(item, dict):
                    oid = str(item.get("id") or item.get("PROGRAMID") or item.get("TEAMID") or "").strip()
                    oname = str(item.get("name") or item.get("PROGRAMNAME") or item.get("TEAMNAME") or "").strip()
                    if oid and oname:
                        pairs.append((oid, oname))
                    continue
                if isinstance(item, (list, tuple)):
                    if len(item) >= 2:
                        oid = str(item[0] or "").strip()
                        oname = str(item[1] or "").strip()
                        if oid and oname:
                            pairs.append((oid, oname))
                    continue
            except Exception:
                continue
        return pairs

    with st.expander("Context Filters", expanded=False):
        ctx = get_admin_context()
        prog_options = _normalize_option_pairs(load_program_options())
        prog_labels = ["All programs"] + [name for _, name in prog_options]
        if "admin.ctx.program" not in st.session_state:
            st.session_state["admin.ctx.program"] = ctx.get("program_name") or "All programs"
        selected_prog = st.selectbox(
            "Program",
            options=prog_labels,
            key="admin.ctx.program",
        )
        program_id = None
        program_name = None
        if selected_prog and selected_prog != "All programs":
            for pid, name in prog_options:
                if name == selected_prog:
                    program_id = pid
                    program_name = name
                    break
        team_options = _normalize_option_pairs(load_team_options(program_id))
        team_labels = ["All teams"] + [name for _, name in team_options]
        if "admin.ctx.team" not in st.session_state:
            st.session_state["admin.ctx.team"] = ctx.get("team_name") or "All teams"
        selected_team = st.selectbox(
            "Team",
            options=team_labels,
            key="admin.ctx.team",
        )
        team_id = None
        team_name = None
        if selected_team and selected_team != "All teams":
            for tid, name in team_options:
                if name == selected_team:
                    team_id = tid
                    team_name = name
                    break

        years_raw = load_year_options()
        if isinstance(years_raw, pd.DataFrame):
            year_col = "Y" if "Y" in years_raw.columns else (years_raw.columns[0] if len(years_raw.columns) else None)
            years_vals = years_raw[year_col].tolist() if year_col is not None else []
        elif isinstance(years_raw, (list, tuple, set)):
            years_vals = list(years_raw)
        else:
            years_vals = []
        years: list[int] = []
        for v in years_vals:
            try:
                y = int(str(v).strip())
            except Exception:
                continue
            # Keep only plausible fiscal years for this app UI.
            if 1990 <= y <= 2100:
                years.append(y)
        years = sorted(set(years))
        year_labels = ["All years"] + [str(y) for y in years]
        if "admin.ctx.year" not in st.session_state:
            st.session_state["admin.ctx.year"] = str(ctx.get("year")) if ctx.get("year") is not None else "All years"
        selected_year = st.selectbox(
            "Year",
            options=year_labels,
            key="admin.ctx.year",
        )
        year_val = int(selected_year) if selected_year and selected_year != "All years" else None

        pis_raw = load_pi_options()

        def _to_int_pis(values: list[object]) -> list[int]:
            out: list[int] = []
            for val in values:
                try:
                    out.append(int(str(val).strip()))
                except Exception:
                    continue
            return out

        if isinstance(pis_raw, pd.DataFrame):
            if pis_raw.empty:
                pis = []
            else:
                cols_upper = {str(c).strip().upper(): c for c in pis_raw.columns}
                pi_col = (
                    cols_upper.get("PI")
                    or cols_upper.get("PI_NUM")
                    or cols_upper.get("ITERATION_NUM")
                    or (pis_raw.columns[0] if len(pis_raw.columns) else None)
                )
                pis = _to_int_pis(pis_raw[pi_col].tolist()) if pi_col is not None else []
        elif isinstance(pis_raw, (list, tuple, set)):
            flat_vals: list[object] = []
            for item in list(pis_raw):
                if isinstance(item, (tuple, list)):
                    flat_vals.extend(list(item))
                elif isinstance(item, dict):
                    flat_vals.extend(list(item.values()))
                else:
                    flat_vals.append(item)
            pis = _to_int_pis(flat_vals)
        else:
            pis = []

        pis = sorted(set(pis))
        if pis:
            pi_labels = ["All PIs"] + [str(p) for p in pis]
            if "admin.ctx.pi" not in st.session_state:
                st.session_state["admin.ctx.pi"] = str(ctx.get("pi")) if ctx.get("pi") is not None else "All PIs"
            selected_pi = st.selectbox(
                "PI",
                options=pi_labels,
                key="admin.ctx.pi",
            )
            pi_val = int(selected_pi) if selected_pi and selected_pi != "All PIs" else None
        else:
            pi_val = None
            st.caption("PI filter unavailable (no PI data detected).")

        if "admin.ctx.row_limit" not in st.session_state:
            st.session_state["admin.ctx.row_limit"] = "All" if ctx.get("row_limit") is None else int(ctx.get("row_limit") or 200)
        row_limit = st.selectbox(
            "Row limit",
            options=[200, 1000, "All"],
            key="admin.ctx.row_limit",
        )
        show_ids = st.checkbox("Show IDs", value=bool(ctx.get("show_ids")), key="admin.ctx.show_ids")
        show_advanced = st.checkbox("Show advanced columns", value=bool(ctx.get("show_advanced")), key="admin.ctx.show_advanced")
        show_perf = st.checkbox(
            "Show performance info",
            value=bool(ctx.get("show_perf")),
            key="admin.ctx.show_perf",
            help="Reveal load timing and row counts for Admin diagnostics.",
        )
        if st.button("Reset filters", key="admin.ctx.reset"):
            set_admin_context(get_default_admin_context())
            st.session_state["admin.ctx.program"] = "All programs"
            st.session_state["admin.ctx.team"] = "All teams"
            st.session_state["admin.ctx.year"] = "All years"
            st.session_state["admin.ctx.pi"] = "All PIs"
            st.session_state["admin.ctx.row_limit"] = 200
            st.session_state["admin.ctx.show_ids"] = False
            st.session_state["admin.ctx.show_advanced"] = False
            st.session_state["admin.ctx.show_perf"] = False
            st.rerun()

        set_admin_context(
            {
                "program": program_id,
                "program_name": program_name,
                "team": team_id,
                "team_name": team_name,
                "year": year_val,
                "pi": pi_val,
                "row_limit": None if row_limit == "All" else int(row_limit),
                "show_ids": show_ids,
                "show_advanced": show_advanced,
                "show_perf": show_perf,
            }
        )


def _perf_enabled() -> bool:
    ctx = get_admin_context()
    return bool(ctx.get("show_perf", False))


def render_section(title: str, caption: Optional[str] = None) -> None:
    st.markdown(f"### {title}")
    if caption:
        st.caption(caption)


def render_action_row(primary_label: str, secondary_label: Optional[str] = None, *,
                      primary_key: Optional[str] = None,
                      secondary_key: Optional[str] = None,
                      primary_disabled: bool = False,
                      secondary_disabled: bool = False) -> bool:
    cols = st.columns([1, 1])
    clicked = False
    with cols[0]:
        clicked = st.button(primary_label, key=primary_key, disabled=primary_disabled, use_container_width=True)
    if secondary_label:
        with cols[1]:
            st.button(secondary_label, key=secondary_key, disabled=secondary_disabled, use_container_width=True)
    return clicked


def render_feedback(ok: bool, message: str, details: Optional[dict] = None) -> None:
    if ok:
        st.success(message)
    else:
        st.error(message)
    if details:
        with st.expander("Details"):
            st.json(details)


def render_empty_state(title: str, body: str, tips: Optional[Iterable[str]] = None) -> None:
    st.info(title)
    st.write(body)
    if tips:
        st.caption("Tips:")
        for tip in tips:
            st.caption(f"• {tip}")


def render_perf_info(label: str, duration_s: float, rows: Optional[int] = None) -> None:
    if not _perf_enabled():
        return
    msg = f"{label} loaded in {duration_s:.2f}s"
    if rows is not None:
        msg += f" ({rows} rows)"
    st.caption(msg)


def render_audit_panel(area: Optional[str] = None, limit: int = 20) -> None:
    st.markdown("### Audit")
    st.caption("Recent admin actions for this area.")
    df = get_recent_admin_actions(area=area, limit=limit)
    if df is None or df.empty:
        st.info("No audit data available.")
        return
    show_cols = [c for c in ["CREATED_AT", "USER_EMAIL", "ACTION_TYPE", "ENTITY", "SUMMARY"] if c in df.columns]
    if show_cols:
        st.dataframe(df[show_cols], use_container_width=True, height=240)
    else:
        st.dataframe(df, use_container_width=True, height=240)
    with st.expander("Details"):
        for _, row in df.iterrows():
            with st.container(border=True):
                st.write(f"**{row.get('ACTION_TYPE') or 'Action'}** — {row.get('SUMMARY') or ''}")
                st.write(
                    f"Time: {row.get('CREATED_AT') or '—'} | "
                    f"User: {row.get('USER_EMAIL') or '—'} | "
                    f"Area: {row.get('AREA') or '—'} | "
                    f"Entity: {row.get('ENTITY') or '—'}"
                )
                extra = row.get("EXTRA_JSON")
                if extra:
                    try:
                        st.json(json.loads(extra))
                    except Exception:
                        st.write(extra)
