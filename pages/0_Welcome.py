# Summary: update user-facing labels and disclaimer copy to NEXT.
from __future__ import annotations

import datetime as dt
import hashlib
import html
import os
import uuid
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from zoneinfo import ZoneInfo
from core.cache_utils import cache_data_portfolio
from core.freshness import get_data_freshness_token
try:
    import numpy as np  # type: ignore

except Exception:  # pragma: no cover
    np = None  # type: ignore

try:
    from streamlit_echarts import JsCode  # type: ignore
except Exception:  # pragma: no cover
    JsCode = None  # type: ignore

from core.ado_recon import load_explorer_feature_rows, load_velocity_baseline
from core.ado_profile import get_active_ado_profile, get_profile_value
from core.data import (
    compute_ado_coverage,
    fetch_ado_features,
    fetch_filter_options,
    fetch_pi_calendar_resolved_dates,
)
from core.canonical_costs import (
    get_cost_lines as _get_cost_lines_raw,
    get_pi_costs as _get_pi_costs_raw,
    get_unassigned_breakdown as _get_unassigned_breakdown_raw,
)
from core.cost_model import get_cost_model_warning, load_cost_model
from core.name_resolution import apply_display_scope_names
from core.init import init_page, page_loader, render_echart
from core.insights_headlines import compute_headline
from core.scope import infer_role, read_scope_from_session, scope_label
from core.debug import is_debug_enabled
from core.perf import (
    filters_signature,
    get_portfolio_cache_buster,
    mark_first_kpi_render,
    perf_step,
    show_perf_panel,
    user_scope_signature,
)
from core.portfolio_runtime import get_active_portfolio_key
from core.ui import render_headline_with_help, render_page_header, touch_last_updated_status
from core.velocity_baseline import build_velocity_debug_meta, select_velocity_snapshot
from utils.theme import THEME
from utils.ui_patterns import render_active_filters_summary
from utils.finops_kpi_cards import build_finops_kpi_card_html
from welcome.layout import (
    APP_COLORS,
    PaletteHelper,
    build_demand_vs_cost_option,
    build_pie_option,
    build_treemap_data,
)
try:
    from utils.auth import ensure_sso
except Exception:
    st.error("Authentication is not available. Contact an admin.")
    st.stop()

page_theme = init_page("Welcome", page_path=__file__)
from db import (
    list_programs,
    list_teams,
    list_application_groups,
    list_applications,
    get_email_alert_config,
    get_data_version,
    get_data_version_info,
)
from db import fetch_df_active as _fetch_df_raw
from core.invoices_alerts import get_filtered_invoice_alerts, summarize_alerts
from core.kpi_cards import KPI_GREEN, KPI_GREY, KPI_RED, KPI_YELLOW

from utils.app_shell import bootstrap_page
bootstrap_page()


def fetch_df(sql, params=None):  # type: ignore[override]
    df = _fetch_df_raw(sql, params)
    return apply_display_scope_names(df) if isinstance(df, pd.DataFrame) else df


_PI_COSTS_MEMO: dict = {}
_COST_LINES_MEMO: dict = {}


def _freeze_cache_key(obj):
    if isinstance(obj, dict):
        return tuple((str(k), _freeze_cache_key(v)) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0])))
    if isinstance(obj, (list, tuple)):
        return tuple(_freeze_cache_key(v) for v in obj)
    if isinstance(obj, set):
        frozen = [_freeze_cache_key(v) for v in obj]
        return tuple(sorted(frozen, key=lambda x: str(x)))
    if isinstance(obj, pd.Series):
        return ("series", len(obj.index), str(obj.dtype))
    if isinstance(obj, pd.DataFrame):
        return ("df", id(obj), tuple(obj.shape))
    try:
        hash(obj)
        return obj
    except Exception:
        return str(obj)


def get_pi_costs(*args, **kwargs):  # type: ignore[override]
    base = args[0] if args else None
    key = None
    if isinstance(base, pd.DataFrame):
        key = (
            "pi",
            id(base),
            tuple(base.shape),
            _freeze_cache_key(args[1:]),
            _freeze_cache_key(kwargs),
        )
        hit = _PI_COSTS_MEMO.get(key)
        if isinstance(hit, pd.DataFrame):
            return hit.copy()
    out = apply_display_scope_names(_get_pi_costs_raw(*args, **kwargs))
    if key is not None and isinstance(out, pd.DataFrame):
        if len(_PI_COSTS_MEMO) > 256:
            _PI_COSTS_MEMO.clear()
        _PI_COSTS_MEMO[key] = out.copy()
    return out


def get_cost_lines(*args, **kwargs):  # type: ignore[override]
    base = args[0] if args else None
    key = None
    if isinstance(base, pd.DataFrame):
        key = (
            "lines",
            id(base),
            tuple(base.shape),
            _freeze_cache_key(args[1:]),
            _freeze_cache_key(kwargs),
        )
        hit = _COST_LINES_MEMO.get(key)
        if isinstance(hit, pd.DataFrame):
            return hit.copy()
    out = apply_display_scope_names(_get_cost_lines_raw(*args, **kwargs))
    if key is not None and isinstance(out, pd.DataFrame):
        if len(_COST_LINES_MEMO) > 256:
            _COST_LINES_MEMO.clear()
        _COST_LINES_MEMO[key] = out.copy()
    return out


def get_unassigned_breakdown(*args, **kwargs):  # type: ignore[override]
    return apply_display_scope_names(_get_unassigned_breakdown_raw(*args, **kwargs))


def _drop_all_na_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    cols = [c for c in df.columns if df[c].isna().all()]
    return df.drop(columns=cols) if cols else df


def _to_int_series_compat(x, default: int = 0):
    """Pandas-version-safe int conversion (nullable Int64 when supported)."""
    s = pd.to_numeric(x, errors="coerce")
    if not isinstance(s, pd.Series):
        try:
            return int(default) if pd.isna(s) else int(s)
        except Exception:
            return int(default)
    try:
        return s.astype("Int64")
    except Exception:
        return s.fillna(default).astype(int)


def _metric_card(
    col,
    label: str,
    value: str,
    extra: str = "",
    source: str = "",
    source_color: str = "",
    help_text: Optional[str] = None,
) -> None:
    with col:
        with st.container(border=True):
            try:
                st.metric(label, value, help=help_text)
            except TypeError:
                st.metric(label, value)
            if source:
                safe_source = html.escape(source)
                color = source_color or "#64748b"
                st.markdown(
                    f"<span style='display:inline-block; padding:2px 8px; border-radius:10px; "
                    f"font-size:11px; background:{color}; color:#fff;'>{safe_source}</span>",
                    unsafe_allow_html=True,
                )
            st.caption(extra if extra else " ")
        

def _section_title(title: str, tip: str) -> None:
    st.subheader(title, help=tip or None)


SECTION_DIVIDER_MARGIN = "8px 0 10px 0"


def _render_section_divider() -> None:
    st.markdown(f"<hr style='margin:{SECTION_DIVIDER_MARGIN};'>", unsafe_allow_html=True)


WELCOME_KPI_EXTRA_LINES = 3


def _build_finops_kpi_card_html(
    title: str,
    value: str,
    *,
    tip: str,
    chips: Optional[list[str]],
    lines: Optional[list[str]],
    accent_color: str,
    extra_class: str = "",
    icon: str = "insights",
    status: str = "neutral",
    status_label: str = "",
    trend_text: str = "",
    trend_tone: str = "neutral",
    spark_values: Optional[list[float]] = None,
    spark_tone: str = "neutral",
    spark_motif: str = "bars",
) -> str:
    return build_finops_kpi_card_html(
        title=title,
        value=value,
        tip=tip,
        chips=chips,
        lines=lines,
        accent_color=accent_color,
        extra_class=extra_class,
        icon=icon,
        status=status,
        status_label=status_label,
        trend_text=trend_text,
        trend_tone=trend_tone,
        spark_values=spark_values,
        spark_tone=spark_tone,
        spark_motif=spark_motif,
        extra_lines=WELCOME_KPI_EXTRA_LINES,
    )


def render_finops_kpi_card(
    title: str,
    value: str,
    *,
    tip: str,
    chips: Optional[list[str]],
    lines: Optional[list[str]],
    accent_color: str,
    icon: str = "insights",
    status: str = "neutral",
    status_label: str = "",
    trend_text: str = "",
    trend_tone: str = "neutral",
    spark_values: Optional[list[float]] = None,
    spark_tone: str = "neutral",
    spark_motif: str = "bars",
) -> None:
    st.markdown(
        _build_finops_kpi_card_html(
            title=title,
            value=value,
            tip=tip,
            chips=chips,
            lines=lines,
            accent_color=accent_color,
            icon=icon,
            status=status,
            status_label=status_label,
            trend_text=trend_text,
            trend_tone=trend_tone,
            spark_values=spark_values,
            spark_tone=spark_tone,
            spark_motif=spark_motif,
        ),
        unsafe_allow_html=True,
    )


@cache_data_portfolio(ttl=120, show_spinner=False)
def _cached_invoice_lookup_tables(*, user_scope_sig: str = "", cache_buster: str = "0"):
    _ = (user_scope_sig, cache_buster)
    return {
        "programs": list_programs(),
        "teams": list_teams(),
        "groups": list_application_groups(),
        "apps": list_applications(),
    }


def _ids_for_names(df: pd.DataFrame, name_col: str, id_col: str, names: list[str]) -> list[str]:
    if not names or df is None or df.empty or id_col not in df.columns:
        return []
    norm = {str(n).strip().lower() for n in names if str(n).strip()}
    if not norm:
        return []

    matches = pd.Series(False, index=df.index)
    if name_col in df.columns:
        matches = matches | df[name_col].astype(str).str.strip().str.lower().isin(norm)

    raw_col = f"{name_col}_RAW"
    if raw_col in df.columns:
        matches = matches | df[raw_col].astype(str).str.strip().str.lower().isin(norm)

    # Defensive: if caller already passed ids, keep scope instead of falling back to unfiltered.
    matches = matches | df[id_col].astype(str).str.strip().str.lower().isin(norm)

    m = df.loc[matches, [id_col]].copy()
    out: list[str] = []
    for v in m[id_col].dropna().astype(str).tolist():
        if v not in out:
            out.append(v)
    return out


def _expand_with_raw_aliases(df: pd.DataFrame, *, selected: Tuple[str, ...], name_col: str, raw_col: str) -> tuple[str, ...]:
    if not selected:
        return tuple()
    selected_norm = {str(v).strip().upper() for v in selected if str(v).strip()}
    if not selected_norm:
        return tuple()
    out: set[str] = set(selected_norm)
    if isinstance(df, pd.DataFrame) and not df.empty and name_col in df.columns:
        w = df.copy()
        disp = w[name_col].fillna("").astype(str).str.strip()
        disp_norm = disp.str.upper()
        mask = disp_norm.isin(selected_norm)
        if raw_col in w.columns:
            raw = w[raw_col].fillna("").astype(str).str.strip()
            raw_norm = raw.str.upper()
            mask = mask | raw_norm.isin(selected_norm)
            out.update(raw_norm.loc[mask].tolist())
        out.update(disp_norm.loc[mask].tolist())
    return tuple(sorted({v for v in out if v}))


@cache_data_portfolio(ttl=120, show_spinner=False)
def _cached_invoice_alert_rows(
    fiscal_year: int,
    program_ids: tuple[str, ...],
    team_ids: tuple[str, ...],
    group_ids: tuple[str, ...],
    app_ids: tuple[str, ...],
    *,
    filters_sig: str = "",
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> pd.DataFrame:
    _ = (filters_sig, user_scope_sig, cache_buster)
    where = []
    params: list[object] = []
    if fiscal_year:
        where.append("i.FISCAL_YEAR = %s")
        params.append(int(fiscal_year))
    if team_ids:
        where.append(f"COALESCE(g.TEAMID, t.TEAMID, i.TEAMID) IN ({','.join(['%s'] * len(team_ids))})")
        params.extend(list(team_ids))
    if app_ids:
        where.append(f"i.APPLICATIONID IN ({','.join(['%s'] * len(app_ids))})")
        params.extend(list(app_ids))
    if group_ids:
        where.append(f"g.GROUPID IN ({','.join(['%s'] * len(group_ids))})")
        params.extend(list(group_ids))
    if program_ids:
        where.append(f"p.PROGRAMID IN ({','.join(['%s'] * len(program_ids))})")
        params.extend(list(program_ids))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT
            i.INVOICEID,
            i.FISCAL_YEAR,
            i.RENEWALDATE,
            i.AMOUNT,
            i.STATUS,
            i.CONTRACT_DUE,
            COALESCE(i.INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE,
            i.AGREEMENT_NUMBER,
            t.TEAMNAME,
            g.GROUPNAME,
            a.APPLICATIONNAME,
            p.PROGRAMNAME
        FROM INVOICES i
        LEFT JOIN TEAMS t              ON t.TEAMID  = i.TEAMID
        LEFT JOIN APPLICATIONS a       ON a.APPLICATIONID = i.APPLICATIONID
        LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = a.GROUPID
        LEFT JOIN PROGRAMS p           ON p.PROGRAMID = ISNULL(g.PROGRAMID, t.PROGRAMID)
        {where_sql}
        ORDER BY t.TEAMNAME, g.GROUPNAME, a.APPLICATIONNAME
    """
    df = fetch_df(sql, tuple(params) if params else None)
    if df is None or df.empty:
        return pd.DataFrame()

    def _mk_label(row: pd.Series) -> str:
        amt = row.get("AMOUNT") or 0
        dte = row.get("RENEWALDATE")
        try:
            dte_str = pd.to_datetime(dte).date().isoformat() if pd.notna(dte) else "n/a"
        except Exception:
            dte_str = str(dte)
        typ = row.get("INVOICE_TYPE") or "Recurring Invoice"
        status = row.get("STATUS") or ""
        return f"{typ} - {status} - {float(amt):.2f} USD - {dte_str}"

    df["Invoice Name"] = df.apply(_mk_label, axis=1)
    df = df.rename(
        columns={
            "FISCAL_YEAR": "Year",
            "RENEWALDATE": "Renewal Date",
            "STATUS": "Status",
            "AMOUNT": "Amount (USD)",
            "TEAMNAME": "Team",
            "GROUPNAME": "Application",
            "APPLICATIONNAME": "Application",
            "INVOICE_TYPE": "Invoice Type",
            "AGREEMENT_NUMBER": "Agreement Number",
            "CONTRACT_DUE": "Contract Due",
            "PROGRAMNAME": "Program",
        }
    )
    return df


@cache_data_portfolio(ttl=120, show_spinner=False)
def _cached_welcome_demand_inputs(
    *,
    year: int,
    programs: Tuple[str, ...],
    teams: Tuple[str, ...],
    groups: Tuple[str, ...],
    data_version: int,
    include_cost_proj: bool = False,
    include_ado: bool = False,
    include_explorer: bool = False,
) -> dict:
    cost_proj = pd.DataFrame()
    df_ado = pd.DataFrame()
    df_explorer = pd.DataFrame()
    if include_cost_proj:
        scope = {"programs": list(programs), "teams": list(teams), "groups": list(groups)}
        cm = load_cost_model(
            int(year),
            scope,
            include_actual=False,
            include_baseline=not bool(int(str(os.getenv("TCO_WELCOME_EXPECTED_ONLY_FASTLOAD", "0") or "0"))),
        )
        filters_cost_local = {
            "year": int(year),
            "program": list(programs or []),
            "team": list(teams or []),
            "app_group": list(groups or []),
            "location": "GBC",
            "include_program_overhead": False if (teams and len(teams) > 0) else True,
            "group_by": ["PROGRAMNAME", "TEAMNAME", "GROUPNAME"],
        }
        cost_proj = get_pi_costs(
            cm.get("EXPECTED", pd.DataFrame()),
            scenario="Projected",
            filters=filters_cost_local,
        )
    if include_ado:
        df_ado = fetch_ado_features(int(year), list(programs), list(teams), list(groups), rev=0, data_version=data_version)
    if include_explorer:
        df_explorer = load_explorer_feature_rows(
            years=[int(year)],
            programs=tuple(programs),
            teams=tuple(teams),
            groups=tuple(groups),
            pi_nums=tuple(),
            data_version=data_version,
        )
    p_df = list_programs() if programs else pd.DataFrame()
    t_df = list_teams() if teams else pd.DataFrame()
    velocity_program_ids = tuple(_ids_for_names(p_df, "PROGRAMNAME", "PROGRAMID", list(programs or [])))
    velocity_team_ids = tuple(_ids_for_names(t_df, "TEAMNAME", "TEAMID", list(teams or [])))
    velocity_programs = _expand_with_raw_aliases(
        p_df if isinstance(p_df, pd.DataFrame) else pd.DataFrame(),
        selected=programs,
        name_col="PROGRAMNAME",
        raw_col="PROGRAMNAME_RAW",
    )
    velocity_teams = _expand_with_raw_aliases(
        t_df if isinstance(t_df, pd.DataFrame) else pd.DataFrame(),
        selected=teams,
        name_col="TEAMNAME",
        raw_col="TEAMNAME_RAW",
    )

    df_velocity = load_velocity_baseline(
        year=int(year),
        programs=velocity_programs,
        teams=velocity_teams,
        # Delivery baseline is team/program scoped; group-level filtering can suppress
        # valid baseline rows and hide the widget value.
        groups=tuple(),
        data_version=data_version,
        include_prior_year=True,
        reference_year_only=False,
        program_ids=velocity_program_ids,
        team_ids=velocity_team_ids,
    )
    # Fallback for portfolios where DB-side program/team label filters don't line up
    # with display aliases; preserve baseline visibility while keeping scoped fast-path.
    velocity_fallback_used = False
    if (df_velocity is None or df_velocity.empty) and (velocity_programs or velocity_teams):
        velocity_fallback_used = True
        df_velocity = load_velocity_baseline(
            year=int(year),
            programs=tuple(),
            teams=tuple(),
            groups=tuple(),
            data_version=data_version,
            include_prior_year=True,
            reference_year_only=False,
        )
        if isinstance(df_velocity, pd.DataFrame) and not df_velocity.empty:
            vw = df_velocity.copy()
            if velocity_program_ids and "PROGRAMID" in vw.columns:
                pid_set = {str(v).strip().upper() for v in velocity_program_ids if str(v).strip()}
                vw = vw[vw["PROGRAMID"].fillna("").astype(str).str.strip().str.upper().isin(pid_set)].copy()
            if velocity_team_ids and "TEAMID" in vw.columns:
                tid_set = {str(v).strip().upper() for v in velocity_team_ids if str(v).strip()}
                vw = vw[vw["TEAMID"].fillna("").astype(str).str.strip().str.upper().isin(tid_set)].copy()
            if velocity_programs and "PROGRAMNAME" in vw.columns:
                prog_set = {str(v).strip().upper() for v in velocity_programs if str(v).strip()}
                vw = vw[vw["PROGRAMNAME"].fillna("").astype(str).str.strip().str.upper().isin(prog_set)].copy()
            if velocity_teams and "TEAMNAME" in vw.columns:
                team_set = {str(v).strip().upper() for v in velocity_teams if str(v).strip()}
                vw = vw[vw["TEAMNAME"].fillna("").astype(str).str.strip().str.upper().isin(team_set)].copy()
            df_velocity = vw
    return {
        "cost_proj": cost_proj if cost_proj is not None else pd.DataFrame(),
        "df_ado": df_ado if df_ado is not None else pd.DataFrame(),
        "df_explorer": df_explorer if df_explorer is not None else pd.DataFrame(),
        "df_velocity": df_velocity if df_velocity is not None else pd.DataFrame(),
        "velocity_fallback_used": bool(velocity_fallback_used),
    }


def _fmt_invoice_date(val) -> str:
    try:
        if pd.isna(val):
            return "n/a"
        return pd.to_datetime(val).date().isoformat()
    except Exception:
        return str(val) if val is not None else "n/a"


def _build_invoices_contracts_card_html(
    *,
    fiscal_year: int,
    programs: list[str],
    teams: list[str],
    groups: list[str],
    apps: list[str],
    filters_sig: str = "",
    user_scope_sig: str = "",
    cache_buster: str = "0",
) -> tuple[str, dict]:
    lookup = _cached_invoice_lookup_tables(user_scope_sig=user_scope_sig, cache_buster=cache_buster)
    program_ids = tuple(_ids_for_names(lookup["programs"], "PROGRAMNAME", "PROGRAMID", programs))
    team_ids = tuple(_ids_for_names(lookup["teams"], "TEAMNAME", "TEAMID", teams))
    group_ids = tuple(_ids_for_names(lookup["groups"], "GROUPNAME", "GROUPID", groups))
    app_ids = tuple(_ids_for_names(lookup["apps"], "APPLICATIONNAME", "APPLICATIONID", apps))

    cfg = get_email_alert_config() or {}
    due_soon_days = int(max(7, min(120, int(cfg.get("pending_window_days") or 30))))
    contract_months = int(max(1, min(24, int(cfg.get("contract_window_months") or 9))))

    track_df = get_filtered_invoice_alerts(
        fiscal_year=int(fiscal_year),
        program_ids=program_ids,
        team_ids=team_ids,
        group_ids=group_ids,
        application_ids=app_ids,
        due_soon_days=due_soon_days,
        contract_months=contract_months,
        include_forecast=True,
        only_active=True,
        invoice_type_filter="Recurring Invoice",
    )
    summary = summarize_alerts(track_df)
    alert_only = (
        track_df[track_df["Tracking Status"].isin(["Critical", "Pending", "Contract"])].copy()
        if track_df is not None and not track_df.empty
        else pd.DataFrame()
    )

    critical = summary["critical"]
    pending = summary["pending"]
    contract = summary["contract"]
    alerts_n = summary["alerts_n"]
    missing_dates_n = summary["missing_dates_n"]
    top_alerts = summary["top_alerts"].head(5) if not summary["top_alerts"].empty else pd.DataFrame()

    if track_df is None or track_df.empty:
        accent = KPI_GREY
    elif critical > 0:
        accent = KPI_RED
    elif pending + contract > 0:
        accent = KPI_YELLOW
    else:
        accent = KPI_GREEN

    def _icon_html(name: str, color: str) -> str:
        return (
            f"<span class='material-symbols-outlined' "
            f"style='color:{color}; font-size:14px; vertical-align:-2px;'>"
            f"{html.escape(name)}</span>"
        )

    def _short_name(row: pd.Series) -> str:
        def _cell_value(series: pd.Series, col: str) -> str:
            val = series.get(col)
            if isinstance(val, pd.Series):
                val = val.iloc[0] if not val.empty else ""
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return ""
            return str(val).strip()

        for col in ["Application", "APP_GROUP", "APPGROUP", "APP_GROUP_NAME"]:
            val = _cell_value(row, col)
            if val:
                name = val
                break
        else:
            app = _cell_value(row, "Application")
            name = app if app else _cell_value(row, "Invoice Type") or "Invoice"
            for sep in [" - ", " — ", ":"]:
                if sep in name:
                    name = name.split(sep, 1)[0].strip()
                    break
        if len(name) > 22:
            name = name[:21].rstrip() + "…"
        return html.escape(name)

    def _status_label(status: str) -> str:
        status = (status or "").strip()
        if status == "Critical":
            return "Overdue"
        if status == "Pending":
            return "Soon"
        if status == "Contract":
            return "Contract"
        return "Info"

    def _status_icon(status: str) -> str:
        if status == "Critical":
            return _icon_html("error", KPI_RED)
        if status == "Pending":
            return _icon_html("schedule", KPI_YELLOW)
        if status == "Contract":
            return _icon_html("event_busy", KPI_YELLOW)
        if status == "OK":
            return _icon_html("check_circle", KPI_GREEN)
        return _icon_html("info", KPI_GREY)

    alerts_html = ""
    total_html = ""
    more_html = ""

    if track_df is None or track_df.empty:
        value_text = "No invoices"
        chips_html = "<span class='finops-kpi-chip'>No data</span>"
        alerts_html = (
            f"<div class='finops-kpi-row finops-kpi-line inv-row'>"
            f"<div class='finops-kpi-label inv-row-left'>{_icon_html('info', KPI_GREY)} No invoices/contracts found for this scope.</div>"
            f"<div class='finops-kpi-amount inv-row-right'>&nbsp;</div></div>"
        )
    elif alerts_n == 0:
        value_text = "All good"
        chips_html = "<span class='finops-kpi-chip'>No alerts</span>"
        alerts_html = (
            f"<div class='finops-kpi-row finops-kpi-line inv-row'>"
            f"<div class='finops-kpi-label inv-row-left'>{_icon_html('check_circle', KPI_GREEN)} No renewals at risk.</div>"
            f"<div class='finops-kpi-amount inv-row-right'>&nbsp;</div></div>"
        )
    else:
        value_text = f"{alerts_n} alerts" if alerts_n != 1 else "1 alert"
        chips_html = "".join(
            f"<span class='finops-kpi-chip'>{label}: {cnt}</span>"
            for label, cnt in [("Crit", critical), ("Pend", pending), ("Ctr", contract)]
            if cnt > 0
        )
        alert_rows = []
        for _, row in top_alerts.head(5).iterrows():
            status = str(row.get("Tracking Status") or "Info")
            label = _status_label(status)
            icon = _status_icon(status)
            name = _short_name(row)
            amount = _fmt_money(row.get("Amount (USD)") or 0)
            alert_rows.append(
                f"<div class='finops-kpi-row finops-kpi-line inv-row'>"
                f"<div class='finops-kpi-label inv-row-left'>{icon} {label} — {name}</div>"
                f"<div class='finops-kpi-amount inv-row-right'>{amount}</div>"
                f"</div>"
            )
        alerts_html = "".join(alert_rows)

        total_alert_cost = float(
            pd.to_numeric(
                alert_only.get("Amount (USD)", pd.Series(dtype=float)),
                errors="coerce",
            )
            .fillna(0.0)
            .sum()
        )
        more_n = max(alerts_n - len(alert_rows), 0)
        total_label = "Impact"
        total_amount = _fmt_money(total_alert_cost)
        total_suffix = ""
        if more_n > 0:
            total_suffix += f" (+{more_n} more)"
        if missing_dates_n > 0:
            total_suffix += f" • ⚠ {missing_dates_n} missing renewal dates"
        total_suffix_html = (
            f" <span class='finops-kpi-muted'>{html.escape(total_suffix)}</span>"
            if total_suffix
            else ""
        )
        total_html = (
            f"<div class='inv-total'>"
            f"<div class='inv-total-label'>{total_label}</div>"
            f"<div class='inv-total-value'>{total_amount}{total_suffix_html}</div>"
            f"</div>"
        )

    card_html = "\n".join(
        [
            "<div class=\"finops-kpi-card finops-kpi-card--tall\">",
            f"<div class=\"finops-kpi-accent\" style=\"background:{accent};\"></div>",
            "<div class=\"finops-kpi-body\">",
            "<div>",
            "<div class=\"finops-kpi-header\">",
            "<div class=\"finops-kpi-title\">Inv/Contracts</div>",
            "<div class=\"finops-kpi-help\" title=\"Invoice and contract renewals for the current scope.\">?</div>",
            "</div>",
            f"<div class=\"finops-kpi-value\">{html.escape(value_text)}</div>",
            f"<div class=\"finops-kpi-chips\">{chips_html}</div>",
            "</div>",
            "<div class=\"finops-kpi-lines inv-card-body\">",
            f"<div class=\"inv-alerts\">{alerts_html}{more_html}</div>",
            f"{total_html}",
            "</div>",
            "</div>",
            "</div>",
        ]
    )

    state = {
        "alerts_n": alerts_n,
        "missing_dates_n": missing_dates_n,
        "critical": critical,
        "pending": pending,
        "contract": contract,
        "has_data": track_df is not None and not track_df.empty,
        "debug_invoice_names": alert_only.get("Invoice Name", pd.Series(dtype=str)).head(10).tolist()
        if alert_only is not None and not alert_only.empty
        else [],
        "debug_top_names": top_alerts.get("Invoice Name", pd.Series(dtype=str)).head(10).tolist()
        if top_alerts is not None and not top_alerts.empty
        else [],
    }
    return card_html, state


def render_invoices_contracts_card(
    *,
    fiscal_year: int,
    programs: list[str],
    teams: list[str],
    groups: list[str],
    apps: list[str],
) -> dict:
    card_html, state = _build_invoices_contracts_card_html(
        fiscal_year=fiscal_year,
        programs=programs,
        teams=teams,
        groups=groups,
        apps=apps,
    )
    st.markdown(card_html, unsafe_allow_html=True)
    return state

def _forecast_status_option(fill_ratio: float, page_theme: str) -> dict:
    pct_value = max(0.0, min(1.0, fill_ratio)) * 100.0
    return {
        "series": [
            {
                "type": "gauge",
                "startAngle": 90,
                "endAngle": -270,
                "radius": "88%",
                "pointer": {"show": False},
                "axisLine": {
                    "lineStyle": {
                        "width": 10,
                        "color": [
                            [1.0, "#2a2a2a"],
                        ],
                    }
                },
                "progress": {"show": True, "width": 10, "roundCap": True},
                "axisTick": {"show": False},
                "splitLine": {"show": False},
                "axisLabel": {"show": False},
                "detail": {
                    "valueAnimation": True,
                    "formatter": "{value}%",
                    "fontSize": 18,
                    "color": "#f5f5f5" if page_theme == "dark" else "#111827",
                    "offsetCenter": [0, "0%"],
                },
                "data": [{"value": round(pct_value, 0)}],
            }
        ]
    }


def _render_forecast_status_card(
    *,
    container,
    ratio: Optional[float],
    page_theme: str,
    label: str,
    help_text: str,
    subtitle: str = "",
    lines: Optional[list[str]] = None,
    show_label: bool = True,
) -> None:
    with container:
        with st.container(border=False):
            if show_label:
                try:
                    st.metric(label, " ", help=help_text)
                except TypeError:
                    st.metric(label, " ")
            if ratio is None:
                if show_label:
                    st.caption(f"{subtitle} • Not available" if subtitle else "Not available")
                return
            ratio = float(ratio)
            capped_ratio = min(ratio, 1.0)
            fill_pct = max(0, min(100, int(round(capped_ratio * 100, 0))))
            display_pct = max(0, int(round(ratio * 100, 0)))
            # Widget style/colors are configurable via THEME (Visual Lab exportable).
            style_now = str(THEME.get("forecast_liquid_style") or "Ocean")
            base_hex = str(THEME.get("forecast_liquid_primary") or THEME.get("primary") or "#3177cb")
            wave1_hex = str(THEME.get("forecast_liquid_wave_1") or base_hex)
            wave2_hex = str(THEME.get("forecast_liquid_wave_2") or wave1_hex)
            wave3_hex = str(THEME.get("forecast_liquid_wave_3") or wave1_hex)
            wave4_hex = str(THEME.get("forecast_liquid_wave_4") or "#ffffff")
            border_hex = str(THEME.get("forecast_liquid_border") or THEME.get("forecast_liquid_border") or base_hex)
            bg_top_hex = str(THEME.get("forecast_liquid_bg_top") or "#eaf5ff")
            bg_bottom_hex = str(THEME.get("forecast_liquid_bg_bottom") or "#cfe6ff")
            label_out_hex = str(THEME.get("forecast_liquid_label_outside") or THEME.get("forecast_liquid_label") or "#f5f5f5")
            label_in_hex = str(THEME.get("forecast_liquid_label_inside") or THEME.get("forecast_liquid_label") or "#ffffff")
            label_size_px_raw = int(float(THEME.get("forecast_liquid_label_size_px") or 28))
            # Keep the % label visually consistent across high-DPI/large monitors.
            label_size_px = max(26, min(30, label_size_px_raw))
            # Keep the widget a fixed visual size even when the page is resized.
            box_width_px = int(float(THEME.get("forecast_liquid_box_width_px") or 216))
            box_height_px = int(float(THEME.get("forecast_liquid_box_height_px") or 225))
            wave_opacity = float(THEME.get("forecast_liquid_wave_opacity") or 0.85)
            bg_top_opacity = float(THEME.get("forecast_liquid_bg_top_opacity") or 0.22)
            bg_bottom_opacity = float(THEME.get("forecast_liquid_bg_bottom_opacity") or 0.08)
            highlight_opacity = float(THEME.get("forecast_liquid_wave_highlight_opacity") or 0.14)
            highlight_over_hex = str(THEME.get("forecast_liquid_wave_highlight_over") or "#ffffff")
            highlight_over_opacity = float(THEME.get("forecast_liquid_wave_highlight_over_opacity") or 0.0)
            over_wave_hex = str(THEME.get("forecast_liquid_over_primary") or "#e74c3c")
            over_border_hex = str(THEME.get("forecast_liquid_over_border") or over_wave_hex)

            def _rgb(h: str, default: tuple[int, int, int]) -> tuple[int, int, int]:
                try:
                    hh = str(h).strip().lstrip("#")
                    return (int(hh[0:2], 16), int(hh[2:4], 16), int(hh[4:6], 16))
                except Exception:
                    return default

            w1r, w1g, w1b = _rgb(wave1_hex, (49, 119, 203))
            w2r, w2g, w2b = _rgb(wave2_hex, (38, 95, 166))
            w3r, w3g, w3b = _rgb(wave3_hex, (91, 180, 255))
            w4r, w4g, w4b = _rgb(wave4_hex, (255, 255, 255))
            hor, hog, hob = _rgb(highlight_over_hex, (255, 255, 255))
            br, bg, bb = _rgb(border_hex, (w1r, w1g, w1b))
            tr, tg, tb = _rgb(bg_top_hex, (234, 245, 255))
            rr, rg_, rb_ = _rgb(bg_bottom_hex, (207, 230, 255))
            lor, log, lob = _rgb(label_out_hex, (245, 245, 245))
            lir, lig, lib = _rgb(label_in_hex, (255, 255, 255))
            orr, org, orb = _rgb(over_wave_hex, (231, 76, 60))
            obr, obg, obb = _rgb(over_border_hex, (231, 76, 60))

            border_rgba = f"rgba({br},{bg},{bb},0.9)"
            over_border_rgba = f"rgba({obr},{obg},{obb},0.95)"
            bg_top_rgba = f"rgba({tr},{tg},{tb},{max(0.0, min(1.0, bg_top_opacity))})"
            bg_bottom_rgba = f"rgba({rr},{rg_},{rb_},{max(0.0, min(1.0, bg_bottom_opacity))})"
            label_out_rgb = f"rgb({lor},{log},{lob})"
            label_in_rgb = f"rgb({lir},{lig},{lib})"
            highlight_over_rgba = f"rgba({hor},{hog},{hob},{max(0.0, min(1.0, highlight_over_opacity))})"

            style_params = {
                "Ocean": {"amplitude": 10, "period": 2000, "waveLength": "80%"},
                "Calm": {"amplitude": 7, "period": 2600, "waveLength": "95%"},
                "Choppy": {"amplitude": 13, "period": 1500, "waveLength": "70%"},
                "Flat": {"amplitude": 0, "period": 2200, "waveLength": "90%"},
            }.get(style_now, {"amplitude": 10, "period": 2000, "waveLength": "80%"})
            wave_vals = [
                round(max(capped_ratio - 0.00, 0.0), 4),
                round(max(capped_ratio - 0.03, 0.0), 4),
                round(max(capped_ratio - 0.06, 0.0), 4),
                round(max(capped_ratio - 0.09, 0.0), 4),
            ]
            liquid_chart_id = f"tco-liquid-{uuid.uuid4().hex}"
            liquid_chart_html = f"""
            <div id="{liquid_chart_id}" style="width: {box_width_px}px; height: {box_height_px}px;"></div>
            <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/echarts-liquidfill@3/dist/echarts-liquidfill.min.js"></script>
            <script>
              (function() {{
                const el = document.getElementById("{liquid_chart_id}");
                if (!el || !window.echarts) return;
                const chart = echarts.init(el);
                const ratio = {round(capped_ratio, 6)};
                const displayPct = {int(display_pct)};
                const isOver = {str(ratio > 1.0).lower()};
                const isNearFull = displayPct > 99;
                const waveData = [
                  ratio,
                  {{ value: Math.max(ratio - 0.03, 0), direction: 'left' }},
                  Math.max(ratio - 0.06, 0),
                  {{ value: Math.max(ratio - 0.09, 0), direction: 'left' }}
                ];
                const option = {{
                  backgroundColor: 'transparent',
                  series: [{{
                    type: 'liquidFill',
                    radius: '70%',
                    center: ['50%', '50%'],
                    data: waveData,
                    waveAnimation: true,
                    animationDuration: 0,
                    animationDurationUpdate: {style_params["period"]},
                    period: {style_params["period"]},
                    amplitude: {style_params["amplitude"]},
                    waveLength: '{style_params["waveLength"]}',
                    direction: 'right',
                    color: [
                      isOver ? 'rgba({orr},{org},{orb},0.65)' : 'rgba({w1r},{w1g},{w1b},0.65)',
                      isOver ? 'rgba({orr},{org},{orb},0.45)' : 'rgba({w2r},{w2g},{w2b},0.45)',
                      isOver ? 'rgba({orr},{org},{orb},0.30)' : 'rgba({w3r},{w3g},{w3b},0.30)',
                      isNearFull ? '{highlight_over_rgba}' : 'rgba({w4r},{w4g},{w4b},{max(0.0, min(1.0, highlight_opacity))})'
                    ],
                    itemStyle: {{
                      opacity: {max(0.0, min(1.0, wave_opacity))}
                    }},
                    outline: {{
                      show: true,
                      borderDistance: 6,
                      itemStyle: {{
                        borderWidth: 3,
                        borderColor: isOver ? '{over_border_rgba}' : '{border_rgba}'
                      }}
                    }},
                    backgroundStyle: {{
                      color: {{
                        type: 'linear',
                        x: 0, y: 0, x2: 0, y2: 1,
                        colorStops: [
                          {{ offset: 0, color: '{bg_top_rgba}' }},
                          {{ offset: 1, color: '{bg_bottom_rgba}' }}
                        ]
                      }}
                    }},
                    label: {{
                      show: true,
                      formatter: function () {{ return displayPct + '%'; }},
                      fontSize: {label_size_px},
                      lineHeight: {int(round(label_size_px * 1.15))},
                      fontWeight: '700',
                      fontStyle: 'normal',
                      color: '{label_out_rgb}',
                      insideColor: '{label_in_rgb}',
                      align: 'center',
                      verticalAlign: 'middle'
                    }}
                  }}]
                }};
                chart.setOption(option);
                setTimeout(function() {{ try {{ chart.resize(); }} catch (e) {{}} }}, 250);
                window.addEventListener('resize', function() {{ chart.resize(); }});
              }})();
            </script>
            """
            components.html(liquid_chart_html, width=box_width_px, height=box_height_px + 20)
            if subtitle and str(subtitle).strip():
                st.caption(str(subtitle))
            for line in (lines or []):
                if str(line or "").strip():
                    st.caption(str(line))


def _build_snapshot_liquid_card_html(
    *,
    ratio: Optional[float],
    page_theme: str,
    title: str,
    help_text: str,
    tall_h: int,
    value_text: str = "—",
    chips: Optional[list[str]] = None,
    insight_line: str = "",
    trend_text: str = "",
    trend_tone: str = "neutral",
    accent_color: str = "#f59e0b",
) -> tuple[str, int]:
    # Mirror the existing liquidfill configuration and embed it in a tall KPI card.
    html_escape = html.escape
    title_rgba = "rgba(255,255,255,0.7)" if page_theme == "dark" else "rgba(17,24,39,0.7)"
    help_color = "rgba(255,255,255,0.6)" if page_theme == "dark" else "rgba(17,24,39,0.6)"
    card_border = "rgba(255,255,255,0.12)" if page_theme == "dark" else "rgba(15,23,42,0.15)"
    card_bg = str(THEME.get("sidebar") or "#181818")
    chips = [str(c).strip() for c in (chips or []) if str(c).strip()]
    chips_html = "".join(f"<span class='finops-kpi-chip'>{html_escape(c)}</span>" for c in chips)
    pct_label = "—"
    fixed_liquid_w = 136
    fixed_liquid_h = max(int(tall_h), 1)

    liquid_chart_html = ""
    liquid_embed_css = "left:0; right:0; top:0; bottom:0;"
    if ratio is not None:
        ratio = float(ratio)
        capped_ratio = min(ratio, 1.0)
        display_pct = max(0, int(round(ratio * 100, 0)))
        pct_label = f"{int(display_pct)}%"

        style_now = str(THEME.get("forecast_liquid_style") or "Ocean")
        base_hex = str(THEME.get("forecast_liquid_primary") or THEME.get("primary") or "#3177cb")
        wave1_hex = str(THEME.get("forecast_liquid_wave_1") or base_hex)
        wave2_hex = str(THEME.get("forecast_liquid_wave_2") or wave1_hex)
        wave3_hex = str(THEME.get("forecast_liquid_wave_3") or wave1_hex)
        wave4_hex = str(THEME.get("forecast_liquid_wave_4") or "#ffffff")
        border_hex = str(THEME.get("forecast_liquid_border") or THEME.get("forecast_liquid_border") or base_hex)
        bg_top_hex = str(THEME.get("forecast_liquid_bg_top") or "#eaf5ff")
        bg_bottom_hex = str(THEME.get("forecast_liquid_bg_bottom") or "#cfe6ff")
        label_out_hex = str(THEME.get("forecast_liquid_label_outside") or THEME.get("forecast_liquid_label") or "#f5f5f5")
        label_in_hex = str(THEME.get("forecast_liquid_label_inside") or THEME.get("forecast_liquid_label") or "#ffffff")
        label_size_px = 18
        wave_opacity = float(THEME.get("forecast_liquid_wave_opacity") or 0.85)
        bg_top_opacity = float(THEME.get("forecast_liquid_bg_top_opacity") or 0.22)
        bg_bottom_opacity = float(THEME.get("forecast_liquid_bg_bottom_opacity") or 0.08)
        highlight_opacity = float(THEME.get("forecast_liquid_wave_highlight_opacity") or 0.14)
        highlight_over_hex = str(THEME.get("forecast_liquid_wave_highlight_over") or "#ffffff")
        highlight_over_opacity = float(THEME.get("forecast_liquid_wave_highlight_over_opacity") or 0.0)
        over_wave_hex = str(THEME.get("forecast_liquid_over_primary") or "#e74c3c")
        over_border_hex = str(THEME.get("forecast_liquid_over_border") or over_wave_hex)

        def _rgb(h: str, default: tuple[int, int, int]) -> tuple[int, int, int]:
            try:
                hh = str(h).strip().lstrip("#")
                return (int(hh[0:2], 16), int(hh[2:4], 16), int(hh[4:6], 16))
            except Exception:
                return default

        w1r, w1g, w1b = _rgb(wave1_hex, (49, 119, 203))
        w2r, w2g, w2b = _rgb(wave2_hex, (38, 95, 166))
        w3r, w3g, w3b = _rgb(wave3_hex, (91, 180, 255))
        w4r, w4g, w4b = _rgb(wave4_hex, (255, 255, 255))
        hor, hog, hob = _rgb(highlight_over_hex, (255, 255, 255))
        br, bg, bb = _rgb(border_hex, (w1r, w1g, w1b))
        tr, tg, tb = _rgb(bg_top_hex, (234, 245, 255))
        rr, rg_, rb_ = _rgb(bg_bottom_hex, (207, 230, 255))
        lor, log, lob = _rgb(label_out_hex, (245, 245, 245))
        lir, lig, lib = _rgb(label_in_hex, (255, 255, 255))
        orr, org, orb = _rgb(over_wave_hex, (231, 76, 60))
        obr, obg, obb = _rgb(over_border_hex, (231, 76, 60))

        border_rgba = f"rgba({br},{bg},{bb},0.9)"
        over_border_rgba = f"rgba({obr},{obg},{obb},0.95)"
        bg_top_rgba = f"rgba({tr},{tg},{tb},{max(0.0, min(1.0, bg_top_opacity))})"
        bg_bottom_rgba = f"rgba({rr},{rg_},{rb_},{max(0.0, min(1.0, bg_bottom_opacity))})"
        label_out_rgb = f"rgb({lor},{log},{lob})"
        label_in_rgb = f"rgb({lir},{lig},{lib})"
        highlight_over_rgba = f"rgba({hor},{hog},{hob},{max(0.0, min(1.0, highlight_over_opacity))})"

        style_params = {
            "Ocean": {"amplitude": 9, "period": 3200, "waveLength": "88%"},
            "Calm": {"amplitude": 7, "period": 2600, "waveLength": "95%"},
            "Choppy": {"amplitude": 13, "period": 1500, "waveLength": "70%"},
            "Flat": {"amplitude": 0, "period": 2200, "waveLength": "90%"},
        }.get(style_now, {"amplitude": 9, "period": 3200, "waveLength": "88%"})

        liquid_chart_id = f"tco-liquid-{uuid.uuid4().hex}"
        liquid_chart_html = f"""
        <div class="liquid-fixed-frame">
          <div class="liquid-canvas">
            <div id="{liquid_chart_id}" class="liquid-root"></div>
          </div>
        </div>
        <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/echarts-liquidfill@3/dist/echarts-liquidfill.min.js"></script>
        <script>
          (function() {{
            const el = document.getElementById("{liquid_chart_id}");
            if (!el || !window.echarts) return;
            const W = {fixed_liquid_w};
            const H = {fixed_liquid_h};
            const chart = echarts.init(el, null, {{ renderer: 'canvas', width: W, height: H }});
            const rawRatio = {round(ratio, 6)};
            const ratio = Math.min(rawRatio, 1.0);
            const displayPct = Math.max(0, Math.round(rawRatio * 100));
            const isOver = rawRatio > 1.0;
            const isNearFull = displayPct > 99;
            const waveData = isOver
              ? [1.0, {{ value: 0.998, direction: 'left' }}, 0.996]
              : [
                  ratio,
                  {{ value: Math.max(ratio - 0.04, 0), direction: 'left' }},
                  Math.max(ratio - 0.08, 0)
                ];
            const effectiveAmplitude = isOver ? Math.max(2, Math.round({style_params["amplitude"]} * 0.35)) : {style_params["amplitude"]};
            const option = {{
              backgroundColor: 'transparent',
              series: [{{
              type: 'liquidFill',
              shape: 'rect',
              radius: '108%',
              center: ['50%', '60%'],
              data: waveData,
              waveAnimation: true,
              animationDuration: 0,
              animationDurationUpdate: {style_params["period"]},
              period: {style_params["period"]},
              amplitude: effectiveAmplitude,
              waveLength: '{style_params["waveLength"]}',
              direction: 'right',
              color: [
                isOver ? 'rgba({orr},{org},{orb},0.92)' : 'rgba({w1r},{w1g},{w1b},0.92)',
                isOver ? 'rgba({orr},{org},{orb},0.72)' : 'rgba({w2r},{w2g},{w2b},0.72)',
                isOver ? 'rgba({orr},{org},{orb},0.52)' : 'rgba({w3r},{w3g},{w3b},0.52)',
                isNearFull ? '{highlight_over_rgba}' : 'rgba({w4r},{w4g},{w4b},0.35)'
              ],
              itemStyle: {{
                opacity: 1.0
              }},
              outline: {{
                show: false
              }},
              backgroundStyle: {{
                color: 'rgba(0,0,0,0)'
              }},
              label: {{
                show: false,
                formatter: function () {{ return displayPct + '%'; }},
                fontSize: {label_size_px},
                lineHeight: {int(round(label_size_px * 1.15))},
                fontWeight: '700',
                color: '{label_out_rgb}',
                insideColor: '{label_in_rgb}',
                align: 'center',
                verticalAlign: 'middle'
              }}
              }}]
            }};
            chart.setOption(option);
          }})();
        </script>
        """

    card_html = f"""
    <html><head>
    <style>
    @import url("https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20,400,0,0");
    body {{ margin:0; background:transparent; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }}
    .liquid-hero-fixed {{ width:{fixed_liquid_w}px; min-width:{fixed_liquid_w}px; flex:0 0 auto; justify-self:start; align-self:stretch; margin-left:auto; margin-right:0; margin-top:-2px; }}
    .finops-kpi-card {{ height:{tall_h}px; width:{fixed_liquid_w}px; min-width:{fixed_liquid_w}px; max-width:{fixed_liquid_w}px; border:1px solid {card_border}; border-radius:11px;
        background:{card_bg}; display:block; box-sizing:border-box; overflow:hidden; position:relative; isolation:isolate;
        box-shadow: 0 8px 18px rgba(2, 6, 23, 0.16), 0 1px 0 rgba(255,255,255,0.03) inset;
        transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease; }}
    .finops-kpi-accent {{ position:absolute; left:1px; top:1px; bottom:1px; width:3px; z-index:2;
        border-top-left-radius:10px; border-bottom-left-radius:10px;
        background: color-mix(in srgb, {accent_color} 78%, rgba(255,255,255,0.22));
        display:none; }}
    .finops-kpi-card::before {{ content:""; position:absolute; inset:0 0 45% 0; pointer-events:none; z-index:0;
        background: linear-gradient(180deg, rgba(255,255,255,0.10) 0%, rgba(255,255,255,0.03) 55%, rgba(255,255,255,0.0) 100%); }}
    .finops-kpi-card::after {{ content:""; position:absolute; inset:0 0 0 0; pointer-events:none; z-index:0;
        background-image: repeating-radial-gradient(circle at 12% 10%, rgba(255,255,255,0.06) 0 0.7px, transparent 0.7px 2.8px);
        opacity:0.07; filter: blur(0.9px); mix-blend-mode:screen; }}
    .finops-kpi-body {{ flex:1; padding:10px 12px; display:flex; flex-direction:column; justify-content:space-between;
        width:100%; height:100%; position:relative; z-index:2; min-width:0; }}
    .finops-kpi-header {{ display:flex; justify-content:flex-start; align-items:center; margin-top:4px; margin-bottom:6px; }}
    .finops-kpi-title-wrap {{ display:inline-flex; align-items:center; gap:6px; min-width:0; }}
    .finops-kpi-title-icon {{ font-size:15px; color:rgba(148,163,184,0.9); flex-shrink:0; }}
    .material-symbols-outlined {{ font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 20; }}
    .finops-kpi-title {{ font-size:0.78rem; color:{title_rgba}; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:100%; }}
    .finops-kpi-help {{ font-size:0.75rem; color:{help_color}; border:1px solid rgba(255,255,255,0.4); border-radius:50%;
        width:16px; height:16px; line-height:14px; text-align:center; }}
    .finops-kpi-center-value {{
        position: absolute;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
        /* Keep percent label stable regardless of parent iframe width. */
        font-size: 2.8rem;
        font-weight: 700;
        color: rgba(255,255,255,0.97);
        letter-spacing: 0.01em;
        z-index: 3;
        pointer-events: none;
        text-shadow: 0 1px 1px rgba(0,0,0,0.32);
    }}
    .liquid-embed {{ position:absolute; {liquid_embed_css} pointer-events:none; z-index:1; opacity:1;
        overflow:hidden; border-radius:inherit; display:flex; align-items:stretch; justify-content:center;
        container-type: inline-size; container-name: liquidkpi;
        background: transparent; }}
    .liquid-fixed-frame {{ width:{fixed_liquid_w}px; height:{fixed_liquid_h}px; margin:auto; position:relative; flex:0 0 auto; }}
    .liquid-canvas {{ position:absolute; inset:0; transform:none !important; }}
    .liquid-root {{ position:absolute; inset:0; width:100%; height:100%; background:transparent; }}
    @media (max-width: 980px) {{
        .liquid-hero-fixed {{ margin: 0 auto; }}
    }}
    @media (hover: hover) and (pointer: fine) {{
        .finops-kpi-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 14px 24px rgba(2, 6, 23, 0.24), 0 1px 0 rgba(255,255,255,0.05) inset;
            border-color: rgba(255,255,255,0.2);
        }}
    }}
    </style></head>
    <body>
      <div class="liquid-hero-fixed">
        <div class="finops-kpi-card">
          <div class="finops-kpi-accent"></div>
          <div class="finops-kpi-body">
            <div>
              <div class="finops-kpi-header">
                <div class="finops-kpi-title">{html_escape(title)}</div>
              </div>
            </div>
          </div>
          <div class="finops-kpi-center-value">{html_escape(pct_label)}</div>
          <div class="liquid-embed">{liquid_chart_html}</div>
        </div>
      </div>
    </body></html>
    """
    return card_html, tall_h


def _compute_mover_summary(df_cost: Optional[pd.DataFrame], role: str, data_version: int) -> dict:
    if df_cost is None or df_cost.empty:
        return {"ready": False}
    work = df_cost.copy()
    if "SUBCOMPONENT" in work.columns:
        sub = work["SUBCOMPONENT"].fillna("").astype(str)
        work = work[~sub.str.contains("INVOICE", case=False, na=False)]
    work["YEAR"] = pd.to_numeric(work.get("YEAR"), errors="coerce").fillna(0).astype(int)
    work["PI_NUM"] = pd.to_numeric(work.get("PI"), errors="coerce")
    if work["PI_NUM"].isna().any():
        pi_str = work.get("PI", pd.Series(dtype=object)).astype(str).fillna("")
        work["PI_NUM"] = work["PI_NUM"].fillna(pd.to_numeric(pi_str.str.extract(r"(\\d+)", expand=False), errors="coerce"))
    work["PI_NUM"] = _to_int_series_compat(work.get("PI_NUM"))

    years = tuple(sorted({int(y) for y in work["YEAR"].tolist() if int(y) > 0})) if not work.empty else tuple()
    cal = fetch_pi_calendar_resolved_dates(years=years or None, data_version=data_version)
    if cal is not None and not cal.empty:
        cal_w = cal.copy()
        cal_w["CAL_ITERATION_LEVEL3"] = cal_w.get("CAL_ITERATION_LEVEL3", "").fillna("").astype(str).str.strip().str.upper()
        cal_w["CAL_END_DATE"] = pd.to_datetime(cal_w.get("CAL_END_DATE"), errors="coerce")
        work["ITER_L3"] = (
            _to_int_series_compat(work.get("YEAR")).astype(str)
            + " I"
            + _to_int_series_compat(work.get("PI_NUM")).astype(str)
        ).str.strip().str.upper()
        work = work.merge(
            cal_w[["CAL_ITERATION_LEVEL3", "CAL_END_DATE"]],
            left_on="ITER_L3",
            right_on="CAL_ITERATION_LEVEL3",
            how="left",
        )
        work["PERIOD_END_DATE"] = pd.to_datetime(work.get("CAL_END_DATE"), errors="coerce")
        work["PERIOD_LABEL"] = work.get("CAL_ITERATION_LEVEL3")
    else:
        work["PERIOD_END_DATE"] = pd.NaT
        work["PERIOD_LABEL"] = None

    use_dates = work["PERIOD_END_DATE"].notna().any() and work["PERIOD_END_DATE"].dropna().nunique() >= 2
    pi_mode = work["PI_NUM"].notna().any()
    if not use_dates:
        if pi_mode:
            work = work[work["PI_NUM"].notna()].copy()
            work["PI_NUM"] = work["PI_NUM"].astype(int)
            work["PERIOD_KEY"] = work["YEAR"] * 100 + work["PI_NUM"]
        else:
            work["PERIOD_KEY"] = work["YEAR"]
    dim = "PROGRAMNAME" if role == "Portfolio Manager" else "GROUPNAME"
    work[dim] = work.get(dim, pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    work = work[work[dim] != ""]
    if use_dates:
        uniq = sorted(work["PERIOD_END_DATE"].dropna().unique())
        today = pd.Timestamp.today().normalize()
        cutoff = today - pd.Timedelta(days=60)
        uniq_past = [d for d in uniq if pd.to_datetime(d) <= today]
        base = uniq_past if len(uniq_past) >= 2 else uniq
        uniq_recent = [d for d in base if pd.to_datetime(d) >= cutoff]
        chosen = uniq_recent if len(uniq_recent) >= 2 else base
        latest_end = chosen[-1] if len(chosen) >= 1 else None
        prev_end = chosen[-2] if len(chosen) >= 2 else None
        if latest_end is None or prev_end is None:
            return {"ready": False}
        curr = work[work["PERIOD_END_DATE"] == latest_end].groupby(dim)["TOTAL_COST"].sum()
        prev = work[work["PERIOD_END_DATE"] == prev_end].groupby(dim)["TOTAL_COST"].sum()
    else:
        latest_key = int(work["PERIOD_KEY"].max()) if not work.empty else None
        prev_key = work.loc[work["PERIOD_KEY"] < latest_key, "PERIOD_KEY"].max() if latest_key else None
        if latest_key is None or pd.isna(prev_key):
            return {"ready": False}
        curr = work[work["PERIOD_KEY"] == latest_key].groupby(dim)["TOTAL_COST"].sum()
        prev = work[work["PERIOD_KEY"] == prev_key].groupby(dim)["TOTAL_COST"].sum()
    if curr is None or prev is None or curr.empty and prev.empty:
        return {"ready": False}
    delta = (curr - prev).fillna(0.0)
    delta_abs = delta.abs().sort_values(ascending=False)
    top_names = [str(n).strip() for n in delta_abs.head(3).index.tolist() if str(n).strip()]
    total_delta = float(delta.sum())
    total_abs = float(delta_abs.sum())
    top_abs = float(delta_abs.head(2).sum())
    share = (top_abs / total_abs) if total_abs else 0.0
    return {
        "ready": True,
        "top_names": top_names,
        "top_count": len(top_names),
        "total_delta": total_delta,
        "total_abs": total_abs,
        "top_share": share,
    }


def _render_welcome_header(view_lbl: str, role_lbl: str) -> None:
    render_page_header(
        "Welcome",
        "Portfolio cost and demand at a glance",
        view_lbl,
        role_lbl,
        scope_status_right=touch_last_updated_status("welcome"),
        right_logo_path=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "assets", "next_logo.png")),
        # Keep enough horizontal space so the 256px logo stays on the same row as the title.
        right_logo_ratio=0.30,
        right_logo_size_px=256,
        right_logo_vertical_alignment="top",
    )


_header_placeholder = st.empty()
_role_hint = str(st.session_state.get("role") or st.session_state.get("user_role") or "Portfolio Manager")
with _header_placeholder.container():
    _render_welcome_header("Loading scope...", _role_hint)

data_version = get_data_version()
base_df = fetch_filter_options(data_version=data_version)
scope = read_scope_from_session(fetch_df)
role = getattr(scope, "inferred_role", None) or infer_role(scope)

as_of = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date()
as_of_year = int(as_of.year)

years = (
    sorted(pd.to_numeric(base_df.get("YEAR"), errors="coerce").dropna().astype(int).unique().tolist())
    if base_df is not None and not base_df.empty
    else []
)
default_year = as_of_year if as_of_year in years else (max(years) if years else as_of_year)

def _safe_list(values) -> list:
    if values is None:
        return []
    if isinstance(values, (list, tuple, set)):
        items = list(values)
    else:
        items = list(getattr(values, "dropna", lambda: values)())
    cleaned = [str(v).strip() for v in items if str(v).strip()]
    return sorted(set(cleaned))


_UNASSIGNED_FILTER_TOKENS = {"UNASSIGNED", "(UNASSIGNED)"}


def _drop_unassigned_if_mixed(values: list[str]) -> list[str]:
    cleaned = [str(v).strip() for v in (values or []) if str(v).strip()]
    if not cleaned:
        return []
    has_real = any(v.upper() not in _UNASSIGNED_FILTER_TOKENS for v in cleaned)
    if not has_real:
        return cleaned
    return [v for v in cleaned if v.upper() not in _UNASSIGNED_FILTER_TOKENS]

view_label = scope_label(scope, role)
with _header_placeholder.container():
    _render_welcome_header(view_label, role)
st.markdown(
"""<style>
div[data-testid="stMetric"] { min-height: 64px; }
div[data-testid="stCaption"] { min-height: 18px; }
div[data-testid="stCaption"] p { margin-top: 0.15rem !important; margin-bottom: 0.15rem !important; }

/* Welcome liquidfill layout helpers (do not affect other KPI cards). */
.welcome-liquidfill-card {
  border: none !important;
  box-shadow: none !important;
  background: transparent !important;
}
</style>
""",
    unsafe_allow_html=True,
)

with st.expander("Filters", expanded=False):
    st.session_state.setdefault("welcome_filter_programs", [])
    st.session_state.setdefault("welcome_filter_teams", [])
    st.session_state.setdefault("welcome_filter_groups", [])
    year_options = years or [default_year]
    default_year_idx = year_options.index(default_year) if default_year in year_options else 0
    selected_year = int(st.selectbox("Year", year_options, index=default_year_idx))
    programs = _drop_unassigned_if_mixed(sorted(_safe_list(base_df.get("PROGRAMNAME")) if base_df is not None else []))
    teams = _drop_unassigned_if_mixed(sorted(_safe_list(base_df.get("TEAMNAME")) if base_df is not None else []))
    groups = _drop_unassigned_if_mixed(sorted(_safe_list(base_df.get("GROUPNAME")) if base_df is not None else []))
    # For portfolio-wide/admin views, always merge master program list so registered programs
    # remain visible even when selected-year cost rows are sparse.
    show_portfolio_program_options = role in {"Portfolio Manager", "Unknown", "ADMIN"} or not bool(getattr(scope, "programs", []))
    if show_portfolio_program_options:
        try:
            p_df = list_programs()
            if isinstance(p_df, pd.DataFrame) and not p_df.empty and "PROGRAMNAME" in p_df.columns:
                p_master = {str(v).strip() for v in p_df["PROGRAMNAME"].dropna().astype(str).tolist() if str(v).strip()}
                programs = sorted(set(programs).union(p_master))
        except Exception:
            pass
    # Always include scoped options so contributors/viewers keep visibility
    # even when selected-year cost rows are sparse.
    scope_programs = {str(v).strip() for v in (getattr(scope, "programs", []) or []) if str(v).strip()}
    scope_teams = {str(v).strip() for v in (getattr(scope, "teams", []) or []) if str(v).strip()}
    scope_groups = {str(v).strip() for v in (getattr(scope, "groups", []) or []) if str(v).strip()}
    if scope_programs:
        programs = sorted(set(programs).union(scope_programs))
    if scope_teams:
        teams = sorted(set(teams).union(scope_teams))
    if scope_groups:
        groups = sorted(set(groups).union(scope_groups))
    if not teams:
        try:
            t_df = list_teams()
            if isinstance(t_df, pd.DataFrame) and not t_df.empty and "TEAMNAME" in t_df.columns:
                teams = sorted({str(v).strip() for v in t_df["TEAMNAME"].dropna().astype(str).tolist() if str(v).strip()})
        except Exception:
            pass
    if not groups:
        try:
            g_df = list_application_groups()
            if isinstance(g_df, pd.DataFrame) and not g_df.empty and "GROUPNAME" in g_df.columns:
                groups = sorted({str(v).strip() for v in g_df["GROUPNAME"].dropna().astype(str).tolist() if str(v).strip()})
        except Exception:
            pass
    program_label_map: dict[str, str] = {}
    team_label_map: dict[str, str] = {}
    try:
        p_map_df = list_programs()
        if isinstance(p_map_df, pd.DataFrame) and not p_map_df.empty and "PROGRAMNAME" in p_map_df.columns:
            for _, row in p_map_df.iterrows():
                disp = str(row.get("PROGRAMNAME") or "").strip()
                raw = str(row.get("PROGRAMNAME_RAW") or "").strip()
                if disp:
                    program_label_map.setdefault(disp, disp)
                if raw and disp:
                    program_label_map[raw] = disp
    except Exception:
        pass
    try:
        t_map_df = list_teams()
        if isinstance(t_map_df, pd.DataFrame) and not t_map_df.empty and "TEAMNAME" in t_map_df.columns:
            for _, row in t_map_df.iterrows():
                disp = str(row.get("TEAMNAME") or "").strip()
                raw = str(row.get("TEAMNAME_RAW") or "").strip()
                if disp:
                    team_label_map.setdefault(disp, disp)
                if raw and disp:
                    team_label_map[raw] = disp
    except Exception:
        pass

    def _dedupe_options_by_label(options: list[str], label_map: dict[str, str]) -> list[str]:
        chosen: dict[str, str] = {}
        order: list[str] = []
        for opt in options or []:
            raw_opt = str(opt).strip()
            if not raw_opt:
                continue
            disp = str(label_map.get(raw_opt, raw_opt)).strip() or raw_opt
            key = disp.upper()
            prev = chosen.get(key)
            if prev is None:
                chosen[key] = raw_opt
                order.append(key)
                continue
            # Prefer canonical display token over raw alias when both map to same label.
            if prev != disp and raw_opt == disp:
                chosen[key] = raw_opt
        return [chosen[k] for k in order]

    def _remap_selected_by_label(values: list[str], options: list[str], label_map: dict[str, str]) -> list[str]:
        if not values or not options:
            return []
        option_by_disp = {
            str(label_map.get(str(o).strip(), str(o).strip())).strip().upper(): str(o).strip()
            for o in options
            if str(o).strip()
        }
        out: list[str] = []
        seen: set[str] = set()
        for v in values:
            raw_v = str(v).strip()
            if not raw_v:
                continue
            disp_key = str(label_map.get(raw_v, raw_v)).strip().upper()
            opt = option_by_disp.get(disp_key)
            if opt and opt not in seen:
                seen.add(opt)
                out.append(opt)
        return out

    programs = _drop_unassigned_if_mixed(_dedupe_options_by_label(programs, program_label_map))
    teams = _drop_unassigned_if_mixed(_dedupe_options_by_label(teams, team_label_map))
    default_programs = _safe_list(getattr(scope, "programs", []))
    default_teams = _safe_list(getattr(scope, "teams", []))
    default_groups = _safe_list(getattr(scope, "groups", []))

    def _filter_defaults(options: list[str], defaults: list[str]) -> list[str]:
        """Streamlit requires defaults to be present in options; normalize case/whitespace."""
        if not options or not defaults:
            return []
        opt_map = {str(o).strip().lower(): o for o in options if str(o).strip()}
        out: list[str] = []
        for d in defaults:
            key = str(d).strip().lower()
            if not key:
                continue
            o = opt_map.get(key)
            if o is not None and o not in out:
                out.append(o)
        return out

    if not st.session_state["welcome_filter_programs"]:
        st.session_state["welcome_filter_programs"] = _filter_defaults(programs, default_programs)
    if not st.session_state["welcome_filter_teams"]:
        st.session_state["welcome_filter_teams"] = _filter_defaults(teams, default_teams)
    if not st.session_state["welcome_filter_groups"]:
        st.session_state["welcome_filter_groups"] = _filter_defaults(groups, default_groups)

    st.session_state["welcome_filter_programs"] = _remap_selected_by_label(
        list(st.session_state.get("welcome_filter_programs", [])),
        programs,
        program_label_map,
    )
    st.session_state["welcome_filter_teams"] = _remap_selected_by_label(
        list(st.session_state.get("welcome_filter_teams", [])),
        teams,
        team_label_map,
    )
    st.session_state["welcome_filter_groups"] = [
        g for g in st.session_state.get("welcome_filter_groups", []) if g in groups
    ]

    rel_df = base_df.copy() if isinstance(base_df, pd.DataFrame) else pd.DataFrame()
    if not rel_df.empty and "YEAR" in rel_df.columns:
        rel_df["YEAR"] = pd.to_numeric(rel_df.get("YEAR"), errors="coerce").astype("Int64")
        rel_df = rel_df.loc[rel_df["YEAR"].eq(int(selected_year))].copy()
    if rel_df.empty and isinstance(base_df, pd.DataFrame):
        rel_df = base_df.copy()

    sel_programs = st.multiselect(
        "Programs",
        programs,
        key="welcome_filter_programs",
        format_func=lambda x, _m=program_label_map: _m.get(str(x).strip(), str(x).strip()),
    )

    team_options: list[str] = []
    team_disabled = not bool(sel_programs)
    if not team_disabled:
        if not rel_df.empty and "TEAMNAME" in rel_df.columns:
            team_df = rel_df.copy()
            if "PROGRAMNAME" in team_df.columns:
                team_df = team_df.loc[team_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
            team_options = sorted({str(v).strip() for v in team_df.get("TEAMNAME", pd.Series(dtype=str)).dropna().astype(str).tolist() if str(v).strip()})
        if not team_options:
            try:
                t_df = list_teams()
                if isinstance(t_df, pd.DataFrame) and not t_df.empty and "TEAMNAME" in t_df.columns:
                    if sel_programs and "PROGRAMNAME" in t_df.columns:
                        t_df = t_df.loc[t_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
                    team_options = sorted({str(v).strip() for v in t_df["TEAMNAME"].dropna().astype(str).tolist() if str(v).strip()})
            except Exception:
                team_options = []
    team_options = _drop_unassigned_if_mixed(_dedupe_options_by_label(team_options, team_label_map))
    st.session_state["welcome_filter_teams"] = _remap_selected_by_label(
        list(st.session_state.get("welcome_filter_teams", [])),
        team_options,
        team_label_map,
    )
    sel_teams = st.multiselect(
        "Teams",
        team_options,
        key="welcome_filter_teams",
        disabled=team_disabled,
        help="Select Program(s) first." if team_disabled else None,
        format_func=lambda x, _m=team_label_map: _m.get(str(x).strip(), str(x).strip()),
    )

    group_options: list[str] = []
    group_disabled = not bool(sel_teams)
    if not group_disabled:
        if not rel_df.empty and "GROUPNAME" in rel_df.columns:
            group_df = rel_df.copy()
            if "PROGRAMNAME" in group_df.columns and sel_programs:
                group_df = group_df.loc[group_df["PROGRAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_programs))].copy()
            if "TEAMNAME" in group_df.columns:
                group_df = group_df.loc[group_df["TEAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_teams))].copy()
            group_options = sorted({str(v).strip() for v in group_df.get("GROUPNAME", pd.Series(dtype=str)).dropna().astype(str).tolist() if str(v).strip()})
        if not group_options:
            try:
                g_df = list_application_groups()
                if isinstance(g_df, pd.DataFrame) and not g_df.empty and "GROUPNAME" in g_df.columns:
                    if sel_teams and "TEAMNAME" in g_df.columns:
                        g_df = g_df.loc[g_df["TEAMNAME"].fillna("").astype(str).str.strip().isin(set(sel_teams))].copy()
                    group_options = sorted({str(v).strip() for v in g_df["GROUPNAME"].dropna().astype(str).tolist() if str(v).strip()})
            except Exception:
                group_options = []
    group_options = _drop_unassigned_if_mixed(group_options)
    st.session_state["welcome_filter_groups"] = [g for g in st.session_state.get("welcome_filter_groups", []) if g in set(group_options)]
    sel_groups = st.multiselect(
        "Applications",
        group_options,
        key="welcome_filter_groups",
        disabled=group_disabled,
        help="Select Team(s) first." if group_disabled else None,
    )
    render_active_filters_summary(
        "Active filters",
        [
            ("Year", selected_year),
            ("Programs", sel_programs),
            ("Teams", sel_teams),
            ("Applications", sel_groups),
        ],
    )
    if is_debug_enabled(label="Debug"):
        st.checkbox(
            "Debug Welcome payload",
            key="debug_welcome",
            help="Shows velocity/debug JSON block in the Snapshot section.",
        )
    render_mode = "Summary"
    st.caption("Welcome runs in Summary mode for faster performance.")

filters_nonce = abs(hash((selected_year, tuple(sel_programs), tuple(sel_teams), tuple(sel_groups)))) % 1_000_000
filters = {
    "year": selected_year,
    "programs": sel_programs,
    "teams": sel_teams,
    "groups": sel_groups,
    "apps": [],
    "applications": [],
    "mode": "Default",
    "driver_dim": "PROGRAM",
    "nonce": filters_nonce,
}

sel_apps = []
driver_dim = "PROGRAM"
year_nonce = filters_nonce  # refresh nonce used to bust cached KPI helpers
viewing_caption = "Viewing: " + " • ".join(
    [
        ("Custom filters" if str(filters.get("mode", "Default")).strip().lower() == "custom" else "Default scope"),
        *([f"Programs: {len(sel_programs)}"] if sel_programs else []),
        *([f"Teams: {len(sel_teams)}"] if sel_teams else []),
        *([f"Apps: {len(sel_groups)}"] if sel_groups else []),
    ]
)
st.markdown(
    f"<div style='margin:0 0 4px 0; font-size:0.85rem; color:rgba(255,255,255,0.6);'>{viewing_caption}</div>",
    unsafe_allow_html=True,
)
 

include_unmapped = False
diagnostics_mode = False
summary_mode = str(render_mode) != "Full"

# Welcome never lets users pick a scenario; keep a single fixed scenario only for visuals
# that are inherently forecast-scoped (e.g., app treemap uses Expected allocations).
scenario_for_app_visuals = "Projected"

def _scope_key_for_charts(*, programs: list[str], teams: list[str], groups: list[str]) -> str:
    raw = "|".join(
        [
            ",".join(sorted(set([str(x).strip() for x in programs if str(x).strip()]))),
            ",".join(sorted(set([str(x).strip() for x in teams if str(x).strip()]))),
            ",".join(sorted(set([str(x).strip() for x in groups if str(x).strip()]))),
        ]
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]


chart_scope_sig = _scope_key_for_charts(programs=sel_programs, teams=sel_teams, groups=sel_groups)
chart_key_suffix = f"{int(selected_year)}_{int(filters_nonce)}_{chart_scope_sig}"
page_perf_key = "welcome"
cache_user_scope_sig = user_scope_signature(scope, role)
cache_filters_sig = filters_signature(
    {
        "year": int(selected_year),
        "programs": list(sel_programs or []),
        "teams": list(sel_teams or []),
        "groups": list(sel_groups or []),
        "render_mode": str(render_mode or ""),
        "scenario_for_app_visuals": str(scenario_for_app_visuals or ""),
    }
)
cache_buster = get_portfolio_cache_buster()

global_loading_ph = st.empty()


def _set_loading(message: str, step: int, total_steps: int = 5) -> None:
    with global_loading_ph.container():
        step_idx = max(1, min(int(step), int(total_steps)))
        st.markdown(
            f"<div style='font-size:0.8rem; color:color-mix(in srgb, var(--tco-text) 70%, transparent); margin-bottom:0.2rem;'>Step {step_idx} of {int(total_steps)}</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<div style='font-size:0.9rem; color:var(--tco-text); margin-bottom:0.32rem;'>{message}</div>",
            unsafe_allow_html=True,
        )
        st.progress(float(step_idx) / float(total_steps))


_set_loading("Loading filters and cost model...", 1)

# Fast-start guardrail for local/dev SQL pressure:
# allow loading Expected first and skipping Baseline model lines during initial render.
WELCOME_EXPECTED_ONLY = bool(int(str(os.getenv("TCO_WELCOME_EXPECTED_ONLY_FASTLOAD", "0") or "0")))
WELCOME_LIGHT_MODE = bool(int(str(os.getenv("TCO_WELCOME_LIGHT_MODE", "1") or "1")))

filters_cost: dict = {
    "year": int(selected_year),
    "program": list(sel_programs or []),
    "team": list(sel_teams or []),
    "app_group": list(sel_groups or []),
    # Baseline program overhead rates are location-scoped; keep a deterministic default until a UI control is added.
    "location": "GBC",
}
filters_cost["include_program_overhead"] = False if (sel_teams and len(sel_teams) > 0) else True
filters_cost_mapped = {**filters_cost, "mapping_status": ["MAPPED"]}
filters_cost_all = {**filters_cost, "include_unmapped": True}

# Scenario cost lines (cached) for this page (single scope source of truth).
scope_dict = {"programs": list(sel_programs or []), "teams": list(sel_teams or []), "groups": list(sel_groups or [])}
with page_loader(messages=[
    "Loading scope and filters...",
    "Building the cost model...",
    "Aggregating...",
    "Preparing charts...",
    "Finalizing view...",
], show_status=False) as loader:
    loader.step("Loading scope and filters...")
    loader.step("Building the cost model...")
    _set_loading("Building KPI inputs...", 2)
    with perf_step("load_cost_model", page_key=page_perf_key, meta={"year": int(selected_year)}):
        cm = load_cost_model(
            int(selected_year),
            scope_dict,
            include_actual=False,
            include_baseline=not WELCOME_EXPECTED_ONLY,
        )
    cost_model_scope = cm
    cost_model_warning = get_cost_model_warning(
        int(selected_year),
        scope_dict,
        include_actual=False,
        include_baseline=not WELCOME_EXPECTED_ONLY,
    )
    if cost_model_warning:
        st.warning(cost_model_warning, icon="⚠️")

    def _cost_df_for(scen: str, *, program_level: bool = False) -> pd.DataFrame:
        up = str(scen or "").strip().upper()
        if up.startswith("BASELINE") or up.startswith("BUDGET"):
            return cm["BASELINE"]
        if up.startswith("ACTUAL"):
            return cm["ACTUAL"]
        return cm["EXPECTED"]

    loader.step("Aggregating...")
    # Canonical totals (used for KPIs + baseline vs expected chart).
    _set_loading("Computing KPI snapshot...", 3)
    with perf_step("kpi_cost_aggregates", page_key=page_perf_key):
        df_pi_baseline = get_pi_costs(_cost_df_for("Baseline"), scenario="Baseline", filters=filters_cost)
        df_pi_projected_all = get_pi_costs(
            _cost_df_for("Projected"),
            scenario="Projected",
            filters=filters_cost_all,
        )
        df_pi_projected_mapped = get_pi_costs(
            _cost_df_for("Projected"),
            scenario="Projected",
            filters=filters_cost_mapped,
        )

    loader.step("Preparing charts...")
    # Canonical app-level breakdown for treemap + Top 5 (single shared dataset).
    # - Uses canonical scenario handling.
    # - Reuses MSP rows already present in canonical PI breakdown (no extra DB round-trip).
    # - Actual NWF is program-level only in Phase 3 (no app allocation yet).
    _set_loading("Preparing chart datasets (including MSP)...", 4)
    with perf_step("chart_app_breakdown", page_key=page_perf_key):
        df_pi_app_breakdown = get_pi_costs(
            _cost_df_for(scenario_for_app_visuals),
            scenario=scenario_for_app_visuals,
            filters={
                **(filters_cost_all if include_unmapped else filters_cost_mapped),
                # Keep one canonical aggregate and derive both app visuals and mover summary from it.
                "group_by": ["PROGRAMNAME", "GROUPNAME", "SUBCOMPONENT", "COST_CATEGORY"],
            },
        )
    loader.step("Finalizing view...")


baseline_total = float(pd.to_numeric(df_pi_baseline.get("TOTAL_COST"), errors="coerce").fillna(0.0).sum()) if not df_pi_baseline.empty else 0.0
expected_total = float(pd.to_numeric(df_pi_projected_all.get("TOTAL_COST"), errors="coerce").fillna(0.0).sum()) if df_pi_projected_all is not None and not df_pi_projected_all.empty else 0.0
expected_total_all = expected_total
expected_total_mapped = float(pd.to_numeric(df_pi_projected_mapped.get("TOTAL_COST"), errors="coerce").fillna(0.0).sum()) if df_pi_projected_mapped is not None and not df_pi_projected_mapped.empty else 0.0
expected_cost_mapped_pct = (expected_total_mapped / expected_total_all) if expected_total_all else None
expected_cost_unmapped = (expected_total_all - expected_total_mapped) if expected_total_all else None
if df_pi_app_breakdown is None or df_pi_app_breakdown.empty:
    df_pi_app_breakdown = pd.DataFrame(
        columns=["YEAR", "PI", "PROGRAMNAME", "GROUPNAME", "SUBCOMPONENT", "COST_CATEGORY", "TOTAL_COST", "LABOR_BUCKET"]
    )

base_non_msp = df_pi_app_breakdown.copy()
if "LABOR_BUCKET" in base_non_msp.columns:
    base_non_msp = base_non_msp[base_non_msp["LABOR_BUCKET"].astype(str).str.upper().ne("MSP")].copy()
base_non_msp["YEAR"] = _to_int_series_compat(base_non_msp.get("YEAR"))
base_non_msp["GROUPNAME"] = base_non_msp.get("GROUPNAME", "").fillna("").astype(str).str.strip()
base_non_msp = base_non_msp[base_non_msp["GROUPNAME"].ne("(Program NWF)")].copy()
if include_unmapped:
    base_non_msp.loc[base_non_msp["GROUPNAME"].eq(""), "GROUPNAME"] = "(Needs mapping)"
base_non_msp["SUBCOMPONENT"] = base_non_msp.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
base_non_msp["COST_CATEGORY"] = base_non_msp.get("COST_CATEGORY", "").fillna("").astype(str).str.strip().str.upper()
base_non_msp["TOTAL_COST"] = pd.to_numeric(base_non_msp.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)

df_app_costs = (
    base_non_msp.groupby(["YEAR", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT"], dropna=False)["TOTAL_COST"]
    .sum()
    .reset_index()
    if not base_non_msp.empty
    else pd.DataFrame(columns=["YEAR", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "TOTAL_COST"])
)

mspw = pd.DataFrame()
if df_pi_app_breakdown is not None and not df_pi_app_breakdown.empty and "LABOR_BUCKET" in df_pi_app_breakdown.columns:
    mspw = df_pi_app_breakdown[
        df_pi_app_breakdown["LABOR_BUCKET"].astype(str).str.upper().eq("MSP")
    ].copy()

if mspw is None or mspw.empty:
    # Optional emergency fallback for unusual portfolios where MSP rows are not
    # emitted in the canonical PI breakdown shape.
    use_direct_msp_fallback = str(os.getenv("TCO_WELCOME_MSP_DIRECT_QUERY_FALLBACK", "0") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if use_direct_msp_fallback:
        from core.canonical_costs import get_msp_costs as _get_msp_costs_raw  # local import to avoid normal-path DB call

        mspw = apply_display_scope_names(_get_msp_costs_raw(fetch_df, scenario=scenario_for_app_visuals, filters=filters_cost))
        if mspw is not None and not mspw.empty:
            mspw = mspw.rename(columns={"AMOUNT": "TOTAL_COST"})

if mspw is not None and not mspw.empty:
    mspw["YEAR"] = _to_int_series_compat(mspw.get("YEAR"))
    mspw["GROUPNAME"] = mspw.get("GROUPNAME", "").fillna("").astype(str).str.strip()
    mspw["SUBCOMPONENT"] = mspw.get("SUBCOMPONENT", "").fillna("").astype(str).str.strip()
    mspw["TOTAL_COST"] = pd.to_numeric(mspw.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)
    df_msp_app = (
        mspw.groupby(["YEAR", "GROUPNAME", "SUBCOMPONENT"], dropna=False)["TOTAL_COST"]
        .sum()
        .reset_index()
    )
    df_msp_app["COST_CATEGORY"] = "NON_WORK_FORCE"
    extra_msp = df_msp_app[["YEAR", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "TOTAL_COST"]].copy()
    extra_msp = _drop_all_na_columns(extra_msp)
    if extra_msp is not None and not extra_msp.empty:
        app_parts = [_drop_all_na_columns(df_app_costs), extra_msp]
        app_parts = [p for p in app_parts if p is not None and not p.empty]
        df_app_costs = pd.concat(app_parts, ignore_index=True, sort=False) if len(app_parts) > 1 else (app_parts[0].copy() if app_parts else pd.DataFrame())

df_top_apps_all = (
    df_app_costs.groupby("GROUPNAME", as_index=False)["TOTAL_COST"].sum().sort_values("TOTAL_COST", ascending=False)
    if df_app_costs is not None and not df_app_costs.empty
    else pd.DataFrame(columns=["GROUPNAME", "TOTAL_COST"])
)
df_top_apps_all["GROUPNAME"] = df_top_apps_all.get("GROUPNAME", "").fillna("").astype(str).str.strip()
apps_total = float(pd.to_numeric(df_top_apps_all.get("TOTAL_COST"), errors="coerce").fillna(0.0).sum()) if not df_top_apps_all.empty else 0.0

# Mapped-only view for app visuals (prevents a fake mega "(Unassigned)" app group).
df_top_apps = df_top_apps_all[df_top_apps_all["GROUPNAME"].ne("")].copy()
apps_total_mapped = float(pd.to_numeric(df_top_apps.get("TOTAL_COST"), errors="coerce").fillna(0.0).sum()) if not df_top_apps.empty else 0.0
expected_apps_total = apps_total_mapped

# Allocation coverage for the app visuals (mapped share of app-attributed costs).
expected_alloc_cov = (apps_total_mapped / apps_total) if apps_total else None

# Insights-style source chip colors (keep consistent across pages).
source_colors = {
    "NEXT (WF+NWF)": "#2563eb",
    "NEXT Forecast": "#f59e0b",
    "Apptio": "#16a34a",
    "ADO": "#7c3aed",
    "NEXT + ADO": "#0ea5e9",
    "Apptio + NEXT Forecast": "#0891b2",
    "Apptio + ADO": "#0d9488",
}

welcome_kpi_accents = {
    "Planned cost": "#7c3aed",
    "Forecast cost": "#2563eb",
    "Forecast vs Plan": "#f59e0b",
    "Cost Mix": "#16a34a",
}

app_color_map: dict[str, str] = {}
if df_top_apps is not None and not df_top_apps.empty and "GROUPNAME" in df_top_apps.columns:
    for idx, name in enumerate(df_top_apps["GROUPNAME"].tolist()):
        key = str(name).strip() if pd.notna(name) and str(name).strip() else "(Needs mapping)"
        app_color_map.setdefault(key, APP_COLORS[idx % len(APP_COLORS)])

# Canonical per-PI costs for mover summary + allocation coverage (scenario-scoped).
# Build directly from the already scoped cost-model lines to avoid a second expensive canonical grouping.
df_cost_src = _cost_df_for(scenario_for_app_visuals)
if df_cost_src is None or df_cost_src.empty:
    df_cost = pd.DataFrame(columns=["YEAR", "PI", "PROGRAMNAME", "GROUPNAME", "TOTAL_COST"])
else:
    df_cost = df_cost_src.copy()
    df_cost["YEAR"] = _to_int_series_compat(df_cost.get("YEAR"))
    df_cost["PI"] = _to_int_series_compat(df_cost.get("PI"))
    df_cost["PROGRAMNAME"] = df_cost.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    df_cost["GROUPNAME"] = df_cost.get("GROUPNAME", "").fillna("").astype(str).str.strip()
    df_cost["TOTAL_COST"] = pd.to_numeric(df_cost.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    df_cost = (
        df_cost.groupby(["YEAR", "PI", "PROGRAMNAME", "GROUPNAME"], dropna=False)["TOTAL_COST"]
        .sum()
        .reset_index()
    )
    df_cost = df_cost[df_cost["GROUPNAME"].ne("(Program NWF)")].copy()

total_cost = float(expected_total or 0.0)

alloc_cov = None
if df_cost is not None and not df_cost.empty:
    denom = float(pd.to_numeric(df_cost.get("TOTAL_COST"), errors="coerce").fillna(0.0).sum() or 0.0)
    assigned = df_cost.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    numer = float(pd.to_numeric(df_cost.loc[assigned.ne(""), "TOTAL_COST"], errors="coerce").fillna(0.0).sum() or 0.0)
    alloc_cov = (numer / denom) if denom else None

def _sum_amount(df: pd.DataFrame, *, cost_bucket: Optional[str] = None) -> float:
    if df is None or df.empty or "AMOUNT" not in df.columns:
        return 0.0
    work = df
    if cost_bucket and "COST_BUCKET" in work.columns:
        want = str(cost_bucket).strip().upper()
        work = work.loc[work["COST_BUCKET"].fillna("").astype(str).str.upper().eq(want)]
    return float(pd.to_numeric(work.get("AMOUNT"), errors="coerce").fillna(0.0).sum())


def _compute_plan_forecast_ytd(
    df_scoped_cost: pd.DataFrame,
    *,
    selected_year: int,
    as_of: dt.date,
    cost_bucket: Optional[str] = None,
    year_nonce: int = 0,
) -> dict:
    """
    Single source of truth for FY + calendar-YTD totals for Welcome KPIs.

    Returns:
      period_label,
      plan_fy, plan_ytd,
      forecast_fy, forecast_ytd,
      actual_fy, actual_ytd,
      has_calendar (bool),
      ytd_warning (Optional[str]),
      df_year (joined cost rows, selected_year only),
      df_calendar (calendar rows used)
    """
    _ = int(year_nonce or 0)  # make refresh nonce part of the call signature
    work = df_scoped_cost.copy() if df_scoped_cost is not None else pd.DataFrame()
    selected_year = int(selected_year)
    as_of_date = as_of
    is_current_year = selected_year == int(as_of.year)
    period_label = f"YTD through {as_of:%b %d, %Y}" if is_current_year else f"Full year {selected_year}"

    work["YEAR"] = _to_int_series_compat(work.get("YEAR"))
    work["PI"] = _to_int_series_compat(work.get("PI"))
    work = work.loc[work["YEAR"].eq(selected_year)].copy()
    if "AMOUNT" not in work.columns:
        work["AMOUNT"] = pd.Series([0.0] * len(work.index), index=work.index, dtype=float)
    work["AMOUNT"] = pd.to_numeric(work["AMOUNT"], errors="coerce").fillna(0.0)

    if cost_bucket and "COST_BUCKET" in work.columns:
        want = str(cost_bucket).strip().upper()
        b = work["COST_BUCKET"].fillna("").astype(str).str.upper()
        work = work.loc[b.eq(want)].copy()

    scen_col = "SCENARIO" if "SCENARIO" in work.columns else "__SCENARIO_KEY"
    scen = work.get(scen_col, pd.Series(dtype=str)).fillna("").astype(str).str.upper()
    work[scen_col] = scen

    cal = pd.DataFrame(columns=["YEAR", "CAL_ITERATION_LEVEL3", "CAL_L3_NORM", "CAL_START_DATE", "CAL_END_DATE"])
    has_calendar = False
    ytd_warning: Optional[str] = None

    if not is_current_year:
        work["AMOUNT_YTD"] = work["AMOUNT"]
        work["YTD_PRORATION"] = 1.0
    else:
        cal_df = fetch_pi_calendar_resolved_dates(years=(selected_year,), data_version=data_version)
        if cal_df is None or cal_df.empty:
            work["YTD_PRORATION"] = 0.0
            work["AMOUNT_YTD"] = 0.0
            ytd_warning = "YTD unavailable: missing PI calendar dates."
        else:
            cal = cal_df.copy()
            cal["YEAR"] = _to_int_series_compat(cal.get("CAL_YEAR"))
            cal["CAL_ITERATION_LEVEL3"] = cal.get("CAL_ITERATION_LEVEL3", "").fillna("").astype(str).str.strip()
            cal["CAL_L3_NORM"] = cal["CAL_ITERATION_LEVEL3"].str.upper().str.strip().str.replace(r"\s+", " ", regex=True)
            cal["CAL_START_DATE"] = pd.to_datetime(cal.get("CAL_START_DATE"), errors="coerce")
            cal["CAL_END_DATE"] = pd.to_datetime(cal.get("CAL_END_DATE"), errors="coerce")
            cal = cal.loc[cal["YEAR"].eq(selected_year) & cal["CAL_L3_NORM"].ne("")].copy()
            has_calendar = bool(not cal.empty and cal["CAL_END_DATE"].notna().any())

            work["YEAR_INT"] = _to_int_series_compat(work.get("YEAR"))
            work["PI_INT"] = _to_int_series_compat(work.get("PI"))
            work["ITER_L3"] = ""
            work["ITER_L3_NORM"] = ""
            key_mask = work["YEAR_INT"].notna() & work["PI_INT"].notna()
            if bool(key_mask.any()):
                raw_keys = (
                    work.loc[key_mask, "YEAR_INT"].astype(int).astype(str)
                    + " I"
                    + work.loc[key_mask, "PI_INT"].astype(int).astype(str)
                )
                work.loc[key_mask, "ITER_L3"] = raw_keys
                work.loc[key_mask, "ITER_L3_NORM"] = (
                    raw_keys.astype(str).str.upper().str.strip().str.replace(r"\s+", " ", regex=True)
                )

            work = work.merge(
                cal[["CAL_L3_NORM", "CAL_START_DATE", "CAL_END_DATE"]],
                left_on="ITER_L3_NORM",
                right_on="CAL_L3_NORM",
                how="left",
            )
            work["CAL_START_DATE"] = pd.to_datetime(work.get("CAL_START_DATE"), errors="coerce")
            work["CAL_END_DATE"] = pd.to_datetime(work.get("CAL_END_DATE"), errors="coerce")

            start_date = work.get("CAL_START_DATE").dt.date if "CAL_START_DATE" in work.columns else pd.Series([pd.NaT] * len(work))
            end_date = work.get("CAL_END_DATE").dt.date if "CAL_END_DATE" in work.columns else pd.Series([pd.NaT] * len(work))

            pi_days = (end_date - start_date).apply(lambda x: int(x.days + 1) if pd.notna(x) else 0)
            elapsed = (as_of_date - start_date).apply(lambda x: int(x.days + 1) if pd.notna(x) else -9999)
            elapsed_clamped = elapsed.clip(lower=0)

            if np is not None:
                pi_days_arr = pi_days.to_numpy(dtype=float)
                elapsed_arr = elapsed_clamped.to_numpy(dtype=float)
                elapsed_arr = np.minimum(elapsed_arr, pi_days_arr)
                proration_arr = np.where(pi_days_arr > 0, elapsed_arr / pi_days_arr, 0.0)
                proration = pd.Series(proration_arr, index=work.index)
            else:
                proration = (elapsed_clamped / pi_days.replace({0: pd.NA})).fillna(0.0)

            work["YTD_PRORATION"] = pd.to_numeric(proration, errors="coerce").fillna(0.0).clip(0.0, 1.0)
            work["AMOUNT_YTD"] = work["AMOUNT"] * work["YTD_PRORATION"]

    def _sum_for(scenario_key: str, amount_col: str) -> float:
        if work is None or work.empty or amount_col not in work.columns:
            return 0.0
        scen_now = work.get(scen_col, pd.Series(dtype=str)).fillna("").astype(str).str.upper()
        mask = scen_now.eq(str(scenario_key).upper())
        return float(pd.to_numeric(work.loc[mask, amount_col], errors="coerce").fillna(0.0).sum())

    plan_fy = _sum_for("BASELINE", "AMOUNT")
    plan_ytd = _sum_for("BASELINE", "AMOUNT_YTD")
    forecast_fy = _sum_for("EXPECTED", "AMOUNT")
    forecast_ytd = _sum_for("EXPECTED", "AMOUNT_YTD")
    actual_fy = _sum_for("ACTUAL", "AMOUNT")
    actual_ytd = _sum_for("ACTUAL", "AMOUNT_YTD")

    if is_current_year and plan_fy > 0 and plan_ytd <= 0:
        joined = int(pd.to_datetime(work.get("CAL_END_DATE"), errors="coerce").notna().sum()) if "CAL_END_DATE" in work.columns else 0
        if not has_calendar:
            ytd_warning = ytd_warning or "YTD unavailable: missing PI calendar dates."
        elif joined == 0:
            ytd_warning = ytd_warning or "YTD unavailable: PI calendar did not join (check iteration labels)."
        else:
            start_ok = pd.to_datetime(work.get("CAL_START_DATE"), errors="coerce") if "CAL_START_DATE" in work.columns else pd.Series(dtype="datetime64[ns]")
            started = int((start_ok.notna() & (start_ok.dt.date <= as_of_date)).sum()) if not start_ok.empty else 0
            if started == 0:
                ytd_warning = ytd_warning or "YTD begins when the first PI starts."

    out = {
        "period_label": period_label,
        "plan_fy": plan_fy,
        "plan_ytd": plan_ytd,
        "forecast_fy": forecast_fy,
        "forecast_ytd": forecast_ytd,
        "actual_fy": actual_fy,
        "actual_ytd": actual_ytd,
        "has_calendar": has_calendar,
        "ytd_warning": ytd_warning,
        "df_year": work,
        "df_calendar": cal,
        "scenario_col": scen_col,
    }
    return out


is_team_scope = bool(sel_teams)
is_group_scope = (not is_team_scope) and bool(sel_groups)
is_program_scope = (not is_team_scope) and (not is_group_scope)
is_current_year = int(selected_year) == int(as_of.year)

# KPI pipeline: build canonical cost lines only (no exploded/split frames), dedupe, then compute FY/YTD.
def _normalize_cost_lines(df: pd.DataFrame, *, scenario_key: str) -> pd.DataFrame:
    work = df.copy() if df is not None else pd.DataFrame()
    work["SCENARIO"] = str(scenario_key).upper()
    if "YEAR" in work.columns:
        work["YEAR"] = pd.to_numeric(work.get("YEAR"), errors="coerce").fillna(int(selected_year)).astype(int)
    else:
        work["YEAR"] = int(selected_year)
    if "AMOUNT" not in work.columns:
        work["AMOUNT"] = pd.Series([0.0] * len(work.index), index=work.index, dtype=float)
    work["AMOUNT"] = pd.to_numeric(work["AMOUNT"], errors="coerce").fillna(0.0)
    if "PI" in work.columns:
        work["PI"] = pd.to_numeric(work.get("PI"), errors="coerce")
    return work


df_lines_key = next((k for k in ("LINES", "COST_LINES", "CANONICAL_LINES") if k in (cm or {})), None)
if df_lines_key:
    df_lines = cm.get(df_lines_key, pd.DataFrame()).copy()
    if "SCENARIO" not in df_lines.columns:
        df_lines["SCENARIO"] = ""
    df_lines["SCENARIO"] = df_lines["SCENARIO"].fillna("").astype(str).str.upper()
    if "YEAR" in df_lines.columns:
        df_lines["YEAR"] = pd.to_numeric(df_lines.get("YEAR"), errors="coerce").fillna(int(selected_year)).astype(int)
    else:
        df_lines["YEAR"] = int(selected_year)
    if "AMOUNT" not in df_lines.columns:
        df_lines["AMOUNT"] = pd.Series([0.0] * len(df_lines.index), index=df_lines.index, dtype=float)
    df_lines["AMOUNT"] = pd.to_numeric(df_lines["AMOUNT"], errors="coerce").fillna(0.0)
else:
    df_baseline = _normalize_cost_lines(cm.get("BASELINE", pd.DataFrame()), scenario_key="BASELINE")
    df_expected = _normalize_cost_lines(cm.get("EXPECTED", pd.DataFrame()), scenario_key="EXPECTED")
    df_actual = _normalize_cost_lines(cm.get("ACTUAL", pd.DataFrame()), scenario_key="ACTUAL")
    parts = [_drop_all_na_columns(df_baseline), _drop_all_na_columns(df_expected), _drop_all_na_columns(df_actual)]
    parts = [p for p in parts if p is not None and not p.empty]
    df_lines = pd.concat(parts, ignore_index=True, sort=False) if parts else pd.DataFrame()

if "YEAR" not in df_lines.columns:
    df_lines["YEAR"] = int(selected_year)
else:
    df_lines["YEAR"] = pd.to_numeric(df_lines["YEAR"], errors="coerce").fillna(int(selected_year)).astype(int)
df_lines = df_lines.loc[df_lines["YEAR"].eq(int(selected_year))].copy()
if "AMOUNT" not in df_lines.columns:
    df_lines["AMOUNT"] = pd.Series([0.0] * len(df_lines.index), index=df_lines.index, dtype=float)
df_lines["AMOUNT"] = pd.to_numeric(df_lines["AMOUNT"], errors="coerce").fillna(0.0)

df_lines_pre_dedupe = df_lines.copy()
subset_cols_used = None
id_col = next((c for c in ("COST_LINE_ID", "LINE_ID", "UUID", "ID") if c in df_lines.columns), None)
if id_col:
    df_lines = df_lines.drop_duplicates(subset=["SCENARIO", id_col])
else:
    subset_cols_used = None

# Last-defense scope filtering (prevents KPI inflation if upstream scope is ignored).
if sel_programs and "PROGRAMNAME" in df_lines.columns:
    prog = df_lines["PROGRAMNAME"].fillna("").astype(str).str.strip()
    df_lines = df_lines.loc[prog.isin(set(sel_programs))].copy()
if sel_teams and "TEAMNAME" in df_lines.columns:
    team = df_lines["TEAMNAME"].fillna("").astype(str).str.strip()
    df_lines = df_lines.loc[team.isin(set(sel_teams))].copy()
if sel_groups and "GROUPNAME" in df_lines.columns:
    grp = df_lines["GROUPNAME"].fillna("").astype(str).str.strip()
    keep_program_level = grp.eq("") | grp.eq("(Unassigned)")
    df_lines = df_lines.loc[keep_program_level | grp.isin(set(sel_groups))].copy()

ytd_meta_all = {"cost_bucket": "ALL"}
with perf_step("compute_ytd_totals_all", page_key=page_perf_key, meta=ytd_meta_all):
    ytd_totals_all = _compute_plan_forecast_ytd(
        df_lines,
        selected_year=int(selected_year),
        as_of=as_of,
        cost_bucket=None,
        year_nonce=year_nonce,
    )
    _df_year_all = ytd_totals_all.get("df_year", pd.DataFrame())
    _df_cal_all = ytd_totals_all.get("df_calendar", pd.DataFrame())
    ytd_meta_all["rows_df_year"] = int(_df_year_all.shape[0]) if isinstance(_df_year_all, pd.DataFrame) else 0
    ytd_meta_all["rows_calendar"] = int(_df_cal_all.shape[0]) if isinstance(_df_cal_all, pd.DataFrame) else 0

ytd_totals_widget = ytd_totals_all
if is_program_scope:
    ytd_meta_widget = {"cost_bucket": "NWF"}
    with perf_step("compute_ytd_totals_widget", page_key=page_perf_key, meta=ytd_meta_widget):
        ytd_totals_widget = _compute_plan_forecast_ytd(
            df_lines,
            selected_year=int(selected_year),
            as_of=as_of,
            cost_bucket="NWF",
            year_nonce=year_nonce,
        )
        _df_year_widget = ytd_totals_widget.get("df_year", pd.DataFrame())
        _df_cal_widget = ytd_totals_widget.get("df_calendar", pd.DataFrame())
        ytd_meta_widget["rows_df_year"] = int(_df_year_widget.shape[0]) if isinstance(_df_year_widget, pd.DataFrame) else 0
        ytd_meta_widget["rows_calendar"] = int(_df_cal_widget.shape[0]) if isinstance(_df_cal_widget, pd.DataFrame) else 0

df_year = ytd_totals_widget.get("df_year")
period_label = str(ytd_totals_widget.get("period_label") or "")
ytd_warning = ytd_totals_widget.get("ytd_warning")

plan_fy = float(ytd_totals_all.get("plan_fy") or 0.0)
plan_ytd = float(ytd_totals_all.get("plan_ytd") or 0.0)
forecast_fy = float(ytd_totals_all.get("forecast_fy") or 0.0)
forecast_ytd = float(ytd_totals_all.get("forecast_ytd") or 0.0)
actual_fy = float(ytd_totals_all.get("actual_fy") or 0.0)
actual_ytd = float(ytd_totals_all.get("actual_ytd") or 0.0)

plan_ytd_widget = float(ytd_totals_widget.get("plan_ytd") or 0.0)
forecast_ytd_widget = float(ytd_totals_widget.get("forecast_ytd") or 0.0)
actual_ytd_widget = float(ytd_totals_widget.get("actual_ytd") or 0.0)

if is_debug_enabled(label="Debug"):
    with st.expander("Admin debug — PI calendar join & YTD proration", expanded=False):
        st.caption("DEBUG: dedupe diagnostics (copy/paste this section)")
        st.write("DEDUPE id column:", id_col or "(none)")
        if df_lines_pre_dedupe is not None and isinstance(df_lines_pre_dedupe, pd.DataFrame):
            dropped = int(len(df_lines_pre_dedupe.index) - len(df_lines.index))
            st.write("DEDUPE dropped rows:", dropped)
            if not id_col and subset_cols_used:
                dupes = int(df_lines_pre_dedupe.duplicated(subset=subset_cols_used).sum())
                st.write("DEDUPE subset duplicate rows:", dupes)
                st.write("DEDUPE subset columns:", subset_cols_used)
                dup_rows = df_lines_pre_dedupe.loc[
                    df_lines_pre_dedupe.duplicated(subset=subset_cols_used, keep=False)
                ].copy()
                st.write("DEDUPE duplicate rows (pre-dedupe):", dup_rows.shape)
                if not dup_rows.empty:
                    dup_rows["AMOUNT"] = pd.to_numeric(dup_rows.get("AMOUNT"), errors="coerce").fillna(0.0)
                    st.write("DEDUPE duplicate AMOUNT total:", float(dup_rows["AMOUNT"].sum() or 0.0))
                    summary_cols = [
                        c
                        for c in [
                            "SCENARIO",
                            "SOURCE",
                            "SUBCOMPONENT",
                            "COST_CATEGORY",
                            "PROGRAMNAME",
                            "TEAMNAME",
                            "GROUPNAME",
                        ]
                        if c in dup_rows.columns
                    ]
                    if summary_cols:
                        dup_summary = (
                            dup_rows.groupby(summary_cols, dropna=False)["AMOUNT"]
                            .sum()
                            .reset_index()
                            .sort_values("AMOUNT", ascending=False)
                        )
                        st.write("DEDUPE duplicate rows by scenario/source/subcomponent:")
                        st.dataframe(dup_summary.head(30), use_container_width=True, hide_index=True)
                    dup_keys = (
                        dup_rows.groupby(subset_cols_used, dropna=False)
                        .size()
                        .reset_index(name="DUP_COUNT")
                        .sort_values("DUP_COUNT", ascending=False)
                    )
                    st.write("DEDUPE duplicate key samples:")
                    st.dataframe(dup_keys.head(20), use_container_width=True, hide_index=True)
    with st.expander("Debug — Active portfolio/db", expanded=False):
        active_key = get_active_portfolio_key() or "Default"
        display = st.session_state.get("active_portfolio_display_name") or active_key
        st.write(f"Active portfolio: {display} ({active_key})")
        try:
            df_db = fetch_df("SELECT DB_NAME() AS DB_NAME")
            db_name = df_db.iloc[0]["DB_NAME"] if df_db is not None and not df_db.empty else "Unknown"
            st.write(f"DB_NAME(): {db_name}")
        except Exception as e:
            st.write(f"DB_NAME() error: {e}")
        try:
            dv = get_data_version_info()
        except Exception:
            dv = {"version": get_data_version(), "updated_at": None}
        st.write(f"DATA_VERSION: {int((dv or {}).get('version') or 0)}")
        st.write(f"Freshness token: {get_data_freshness_token()}")
        st.write(f"Session _cache_epoch: {int(st.session_state.get('_cache_epoch', 0) or 0)}")
        st.write(f"Freshness fetched at (UTC): {dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}")
    with st.expander("Debug — Recurring invoice visibility", expanded=False):
        st.write(
            {
                "year": int(selected_year),
                "program_filters": list(sel_programs or []),
                "team_filters": list(sel_teams or []),
                "app_filters": list(sel_groups or []),
            }
        )
        try:
            lk = _cached_invoice_lookup_tables(
                user_scope_sig=cache_user_scope_sig,
                cache_buster=cache_buster,
            )
            program_ids = tuple(_ids_for_names(lk.get("programs"), "PROGRAMNAME", "PROGRAMID", list(sel_programs or [])))
            team_ids = tuple(_ids_for_names(lk.get("teams"), "TEAMNAME", "TEAMID", list(sel_teams or [])))
            group_ids = tuple(_ids_for_names(lk.get("groups"), "GROUPNAME", "GROUPID", list(sel_groups or [])))
            app_ids = tuple(_ids_for_names(lk.get("apps"), "APPLICATIONNAME", "APPLICATIONID", []))
            st.write(
                {
                    "program_ids": list(program_ids),
                    "team_ids": list(team_ids),
                    "group_ids": list(group_ids),
                    "app_ids": list(app_ids),
                }
            )
            inv_df = _cached_invoice_alert_rows(
                fiscal_year=int(selected_year),
                program_ids=program_ids,
                team_ids=team_ids,
                group_ids=group_ids,
                app_ids=app_ids,
                filters_sig=cache_filters_sig,
                user_scope_sig=cache_user_scope_sig,
                cache_buster=cache_buster,
            )
            if inv_df is None or inv_df.empty:
                st.caption("No invoice rows matched in `INVOICES` for current Welcome filters.")
            else:
                show_cols = [
                    c
                    for c in [
                        "INVOICEID",
                        "FISCAL_YEAR",
                        "INVOICE_TYPE",
                        "STATUS",
                        "AMOUNT",
                        "TEAMNAME",
                        "GROUPNAME",
                        "APPLICATIONNAME",
                        "PROGRAMNAME",
                    ]
                    if c in inv_df.columns
                ]
                st.caption(f"Matched invoice rows: {len(inv_df):,}")
                st.dataframe(inv_df[show_cols], use_container_width=True, hide_index=True, height=240)
        except Exception as e:
            st.caption(f"Invoice visibility trace failed: {e}")

        try:
            if df_lines is None or df_lines.empty:
                st.caption("Canonical `df_lines` is empty for this scope/year.")
            else:
                tmp = df_lines.copy()
                src = tmp.get("SOURCE", "").astype(str).str.upper()
                sub = tmp.get("SUBCOMPONENT", "").astype(str).str.upper()
                inv = tmp.loc[src.str.contains("INVOICE") | sub.str.contains("INVOICE")].copy()
                inv["AMOUNT"] = pd.to_numeric(inv.get("AMOUNT"), errors="coerce").fillna(0.0)
                if inv.empty:
                    st.caption("No invoice rows found in canonical `df_lines` after current scope filters.")
                else:
                    by_scen = (
                        inv.groupby(inv.get("SCENARIO", pd.Series([""] * len(inv.index))).astype(str), dropna=False)["AMOUNT"]
                        .sum()
                        .reset_index(name="TOTAL_AMOUNT")
                    )
                    st.caption("Canonical invoice totals by scenario (current Welcome scope):")
                    st.dataframe(by_scen, use_container_width=True, hide_index=True)
        except Exception as e:
            st.caption(f"Canonical visibility trace failed: {e}")


def _fmt_money(val: Optional[float]) -> str:
    return f"${float(val):,.0f}" if val is not None else "—"


def _compute_velocity_snapshot(
    df_velocity: pd.DataFrame,
    *,
    role: str,
    programs: list[str],
    teams: list[str],
    selected_year: int,
    as_of: dt.date,
    data_version: int,
) -> dict:
    _ = role
    try:
        cal_df = fetch_pi_calendar_resolved_dates(
            years=(int(selected_year),),
            data_version=int(data_version or 0),
        )
    except Exception:
        cal_df = pd.DataFrame()

    return select_velocity_snapshot(
        df_velocity=df_velocity if isinstance(df_velocity, pd.DataFrame) else pd.DataFrame(),
        selected_year=int(selected_year),
        as_of=as_of,
        programs=list(programs or []),
        teams=list(teams or []),
        calendar_df=cal_df,
    )

def _resolve_global_baseline_points(default: float = 65.0) -> float:
    try:
        profile = get_active_ado_profile()
        raw = get_profile_value(profile, "swag.points_per_fte", default)
        pts = float(raw)
        if pd.isna(pts) or pts <= 0:
            return float(default)
        return float(pts)
    except Exception:
        return float(default)


def _build_velocity_baseline_card_html(snapshot: dict, *, global_baseline_points: float = 65.0) -> str:
    value = snapshot.get("value")
    prev = snapshot.get("prev")
    changed = bool(snapshot.get("changed"))
    if (not changed) and value is not None and prev is not None:
        try:
            changed = abs(float(value) - float(prev)) >= 0.01
        except Exception:
            changed = bool(snapshot.get("changed"))
    source = str(snapshot.get("source") or "GLOBAL").upper().strip() or "GLOBAL"
    source_label = "Global" if source == "GLOBAL" else source
    pi_label = str(snapshot.get("pi_label") or "—").strip() or "—"
    values = list(snapshot.get("spark_values") or [])

    value_txt = "—" if value is None else f"{float(value):.1f}"
    global_pts = float(global_baseline_points) if float(global_baseline_points) > 0 else 65.0
    delta_vs_global = None if value is None else ((float(value) / global_pts) - 1.0) * 100.0
    delta_txt = "—" if delta_vs_global is None else f"{delta_vs_global:+.1f}%"

    spark_svg = "—"
    if values:
        w = 118
        h = 28
        pad = 2
        vals = [float(v) for v in values]
        vmin = min(vals)
        vmax = max(vals)
        span = (vmax - vmin) if (vmax - vmin) > 1e-6 else 1.0
        points = []
        for i, v in enumerate(vals):
            x = pad + (i * ((w - (2 * pad)) / max(1, len(vals) - 1)))
            y = h - pad - (((v - vmin) / span) * (h - (2 * pad)))
            points.append(f"{x:.2f},{y:.2f}")
        spark_svg = (
            f"<svg viewBox='0 0 {w} {h}' class='baseline-spark' aria-hidden='true'>"
            f"<polyline points='{' '.join(points)}' fill='none' stroke='rgba(96,165,250,0.85)' stroke-width='1.6' stroke-linecap='round' stroke-linejoin='round'></polyline>"
            "</svg>"
        )

    badge = "<span class='baseline-badge'>Changed</span>" if changed else ""
    pulse_cls = " baseline-value--pulse" if changed else ""
    delta_chip_html = ""
    if value is not None and prev is not None:
        delta = float(value) - float(prev)
        if abs(delta) >= 0.01:
            delta_icon = "▲" if delta > 0 else "▼"
            delta_sign = "+" if delta > 0 else ""
            delta_chip_html = (
                f"<span class='baseline-delta-chip baseline-delta-chip--{'up' if delta > 0 else 'down'}'>"
                f"{delta_icon} {delta_sign}{delta:.1f}</span>"
            )
    prev_txt = "" if prev is None else f"{float(prev):.1f}"
    prev_hint = "" if not prev_txt else f" · Prev {prev_txt}"
    return (
        "<div class='welcome-baseline-card baseline-card-enter'>"
        "<div class='baseline-watermark material-symbols-outlined' aria-hidden='true'>speed</div>"
        "<div class='baseline-title-row'>"
        "<div class='baseline-title'>Delivery Baseline (Derived)</div>"
        f"{badge}"
        "</div>"
        f"<div class='baseline-value-row'><div class='baseline-value{pulse_cls}'>{html.escape(value_txt)} pts/PI</div>{delta_chip_html}</div>"
        f"<div class='baseline-subline'>vs Global {html.escape(f'{global_pts:.1f}'.rstrip('0').rstrip('.'))}: {html.escape(delta_txt)}{html.escape(prev_hint)}</div>"
        f"<div class='baseline-spark-wrap'>{spark_svg}</div>"
        f"<div class='baseline-footer'>Source: {html.escape(source_label)} · Updated: PI {html.escape(pi_label)}</div>"
        "</div>"
    )


def _build_velocity_baseline_compact_card_html(
    snapshot: dict,
    *,
    height_px: int,
) -> str:
    value = snapshot.get("value")
    prev = snapshot.get("prev")
    changed = bool(snapshot.get("changed"))
    if (not changed) and value is not None and prev is not None:
        try:
            changed = abs(float(value) - float(prev)) >= 0.01
        except Exception:
            changed = bool(snapshot.get("changed"))
    source = str(snapshot.get("source") or "GLOBAL").upper().strip() or "GLOBAL"
    source_label = "Global" if source == "GLOBAL" else source
    pi_label = str(snapshot.get("pi_label") or "—").strip() or "—"
    values = list(snapshot.get("spark_values") or [])
    value_txt = "—" if value is None else f"{float(value):.1f}"
    changed_badge = "<span class='baseline-compact-badge'>Changed</span>" if changed else ""
    delta_chip_html = ""
    if value is not None and prev is not None:
        delta = float(value) - float(prev)
        if abs(delta) >= 0.01:
            delta_icon = "▲" if delta > 0 else "▼"
            delta_sign = "+" if delta > 0 else ""
            delta_chip_html = (
                f"<span class='baseline-compact-delta baseline-compact-delta--{'up' if delta > 0 else 'down'}'>"
                f"{delta_icon} {delta_sign}{delta:.1f}</span>"
            )
    spark_svg = ""
    if values:
        w = 126
        h = 30
        pad = 2
        vals = [float(v) for v in values]
        vmin = min(vals)
        vmax = max(vals)
        span = (vmax - vmin) if (vmax - vmin) > 1e-6 else 1.0
        points = []
        for i, v in enumerate(vals):
            x = pad + (i * ((w - (2 * pad)) / max(1, len(vals) - 1)))
            y = h - pad - (((v - vmin) / span) * (h - (2 * pad)))
            points.append(f"{x:.2f},{y:.2f}")
        spark_svg = (
            "<div class='baseline-compact-spark'>"
            f"<svg viewBox='0 0 {w} {h}' aria-hidden='true'>"
            f"<polyline points='{' '.join(points)}' fill='none' stroke='rgba(96,165,250,0.85)' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round'></polyline>"
            "</svg>"
            "</div>"
        )

    return (
        f"<div class='welcome-baseline-compact' style='height:{int(height_px)}px; width:100%; max-width:100%; margin:0;'>"
        "<div class='finops-kpi-bg-layer'></div>"
        "<div class='baseline-compact-bg-art' aria-hidden='true'></div>"
        "<div class='baseline-watermark material-symbols-outlined' aria-hidden='true'>speed</div>"
        "<div class='baseline-compact-accent'></div>"
        "<div class='baseline-compact-body'>"
        "<div class='finops-kpi-header'>"
        "<div class='finops-kpi-title-wrap'>"
        "<span class='material-symbols-outlined finops-kpi-title-icon'>speed</span>"
        "<div class='finops-kpi-title'>Delivery Baseline</div>"
        "</div>"
        "<div class='baseline-compact-header-meta'>"
        f"{changed_badge}"
        "<div class='finops-kpi-help' title='Derived delivery baseline (velocity) for the selected scope.'>?</div>"
        "</div>"
        "</div>"
        f"<div class='baseline-compact-value-row'><div class='baseline-compact-value'>{html.escape(value_txt)} <span class='baseline-compact-unit'>pts/PI</span></div>{delta_chip_html}</div>"
        f"{spark_svg}"
        f"<div class='baseline-compact-meta'><span class='finops-kpi-chip'>{html.escape(source_label)}</span><span class='baseline-compact-pi'>{html.escape(pi_label)}</span></div>"
        "</div>"
        "</div>"
    )


def _should_show_baseline_widget(*, role: str, programs: list[str], teams: list[str]) -> bool:
    role_now = str(role or "").strip()
    if role_now == "Product Owner" and bool(teams):
        return True
    return len([p for p in (programs or []) if str(p).strip()]) == 1


def _should_show_baseline_ripple(*, role: str, programs: list[str], teams: list[str], baseline_changed: bool) -> bool:
    if not baseline_changed:
        return False
    return _should_show_baseline_widget(role=role, programs=programs, teams=teams)


# Liquid fill card uses full-year Plan vs Forecast to avoid YTD calendar edge cases.
liquid_line2 = (
    "Not available"
    if plan_fy <= 0
    else f"Forecast {_fmt_money(forecast_fy)} / Plan {_fmt_money(plan_fy)}"
)

forecast_vs_plan_ratio = (float(forecast_fy) / float(plan_fy)) if plan_fy > 0 else None
if forecast_vs_plan_ratio is not None:
    forecast_vs_plan_ratio = max(0.0, min(2.0, float(forecast_vs_plan_ratio)))

plan_ytd_for_cards: float = plan_ytd if is_current_year else plan_fy
forecast_ytd_for_cards: float = forecast_ytd if is_current_year else forecast_fy

_welcome_inputs_meta = {"summary_mode": bool(summary_mode), "diagnostics_mode": bool(diagnostics_mode)}
with perf_step("welcome_demand_inputs", page_key=page_perf_key, meta=_welcome_inputs_meta):
    _welcome_inputs = _cached_welcome_demand_inputs(
        year=int(selected_year),
        programs=tuple(sel_programs or ()),
        teams=tuple(sel_teams or ()),
        groups=tuple(sel_groups or ()),
        data_version=int(data_version or 0),
        include_cost_proj=(not summary_mode) and (not WELCOME_LIGHT_MODE),
        include_ado=bool(diagnostics_mode) and (not WELCOME_LIGHT_MODE),
        include_explorer=bool(diagnostics_mode) and (not WELCOME_LIGHT_MODE),
    )
    _cost_proj_df = _welcome_inputs.get("cost_proj", pd.DataFrame())
    _ado_df = _welcome_inputs.get("df_ado", pd.DataFrame())
    _explorer_df = _welcome_inputs.get("df_explorer", pd.DataFrame())
    _velocity_df = _welcome_inputs.get("df_velocity", pd.DataFrame())
    _welcome_inputs_meta["rows_cost_proj"] = int(_cost_proj_df.shape[0]) if isinstance(_cost_proj_df, pd.DataFrame) else 0
    _welcome_inputs_meta["rows_ado"] = int(_ado_df.shape[0]) if isinstance(_ado_df, pd.DataFrame) else 0
    _welcome_inputs_meta["rows_explorer"] = int(_explorer_df.shape[0]) if isinstance(_explorer_df, pd.DataFrame) else 0
    _welcome_inputs_meta["rows_velocity"] = int(_velocity_df.shape[0]) if isinstance(_velocity_df, pd.DataFrame) else 0
    _welcome_inputs_meta["velocity_fallback_used"] = bool(_welcome_inputs.get("velocity_fallback_used", False))
df_ado = _welcome_inputs.get("df_ado", pd.DataFrame()).copy()
total_bv = float(pd.to_numeric(df_ado.get("BUSINESS_VALUE"), errors="coerce").fillna(0.0).sum()) if df_ado is not None and not df_ado.empty else 0.0
cost_per_bv = (total_cost / total_bv) if total_bv else None
bv_cov, _ = compute_ado_coverage(df_ado, data_version=data_version)

feature_count = int(len(df_ado.index)) if df_ado is not None and not df_ado.empty else 0

# Derived FTE (Explorer v2) is the canonical FTE driver across the app.
df_explorer = _welcome_inputs.get("df_explorer", pd.DataFrame()).copy()
derived_fte_sum = (
    float(pd.to_numeric(df_explorer.get("DERIVED_FTE_EXPLORER"), errors="coerce").fillna(0.0).sum())
    if df_explorer is not None and not df_explorer.empty
    else 0.0
)
df_velocity = _welcome_inputs.get("df_velocity", pd.DataFrame()).copy()
velocity_meta = {"rows_velocity": int(df_velocity.shape[0]) if isinstance(df_velocity, pd.DataFrame) else 0}
with perf_step("compute_velocity_snapshot", page_key=page_perf_key, meta=velocity_meta):
    velocity_snapshot = _compute_velocity_snapshot(
        df_velocity,
        role=str(role or ""),
        programs=list(sel_programs or []),
        teams=list(sel_teams or []),
        selected_year=int(selected_year),
        as_of=as_of,
        data_version=int(data_version or 0),
    )
velocity_baseline_changed = bool(velocity_snapshot.get("changed"))
if (not velocity_baseline_changed) and velocity_snapshot.get("value") is not None and velocity_snapshot.get("prev") is not None:
    try:
        velocity_baseline_changed = abs(float(velocity_snapshot.get("value")) - float(velocity_snapshot.get("prev"))) >= 0.01
    except Exception:
        velocity_baseline_changed = bool(velocity_snapshot.get("changed"))
show_velocity_baseline_widget = _should_show_baseline_widget(
    role=str(role or ""),
    programs=list(sel_programs or []),
    teams=list(sel_teams or []),
)
# Keep the derived baseline card visible whenever scoped velocity data is available,
# even if strict role/scope gating would otherwise hide it.
has_velocity_snapshot_data = (
    velocity_snapshot.get("value") is not None
    or bool(velocity_snapshot.get("spark_values"))
)
show_velocity_baseline_widget = bool(show_velocity_baseline_widget or has_velocity_snapshot_data)
liquid_ripple_on = _should_show_baseline_ripple(
    role=str(role or ""),
    programs=list(sel_programs or []),
    teams=list(sel_teams or []),
    baseline_changed=velocity_baseline_changed,
)
if not show_velocity_baseline_widget:
    liquid_ripple_on = False

headline_meta = {"rows_df_lines": int(df_lines.shape[0]) if isinstance(df_lines, pd.DataFrame) else 0}
with perf_step("compute_headline", page_key=page_perf_key, meta=headline_meta):
    headline = compute_headline(
        df_lines=df_lines,
        selected_year=int(selected_year),
        as_of=as_of,
        role=str(role or ""),
        scope=scope,
        scope_dict=scope_dict,
        driver_dim=driver_dim,
    )

# Cost vs Demand input (Derived demand FTE + Projected costs; Baseline isn't feature/demand-based).
df_demand = pd.DataFrame()
cost_per_demand_fte = None
mover_summary = {"ready": False}
top5_share = None
label_top_movers = "Stable"
label_top_apps = "Stable"
label_treemap = "Stable"
label_demand_ready = "Watch closely"

if not summary_mode:
    demand_meta = {"rows_explorer": int(df_explorer.shape[0]) if isinstance(df_explorer, pd.DataFrame) else 0}
    with perf_step("prepare_demand_df", page_key=page_perf_key, meta=demand_meta):
        if df_explorer is not None and not df_explorer.empty:
            dem = df_explorer.copy()
            dem["YEAR"] = _to_int_series_compat(dem.get("ADO_YEAR"))
            dem["PROGRAMNAME"] = dem.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
            dem["TEAMNAME"] = dem.get("TEAMNAME", "").fillna("").astype(str).str.strip()
            dem["GROUPNAME"] = dem.get("GROUPNAME", "").fillna("").astype(str).str.strip()
            dem["DERIVED_FTE"] = pd.to_numeric(dem.get("DERIVED_FTE"), errors="coerce").fillna(0.0)

            dem_agg = (
                dem.groupby(["YEAR", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"], dropna=False)
                .agg(
                    FEATURE_COUNT=("FEATURE_ID", pd.Series.nunique),
                    DEMAND_FTE=("DERIVED_FTE", "sum"),
                )
                .reset_index()
            )

            cost_proj = _welcome_inputs.get("cost_proj", pd.DataFrame()).copy()
            if cost_proj is None or cost_proj.empty:
                cost_agg = pd.DataFrame(columns=["YEAR", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "TOTAL_COST"])
            else:
                cw = cost_proj.copy()
                cw["YEAR"] = _to_int_series_compat(cw.get("YEAR"))
                cw["PROGRAMNAME"] = cw.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                cw["TEAMNAME"] = cw.get("TEAMNAME", "").fillna("").astype(str).str.strip()
                cw["GROUPNAME"] = cw.get("GROUPNAME", "").fillna("").astype(str).str.strip()
                cw = cw[cw["GROUPNAME"].ne("(Program NWF)")].copy()
                cw["TOTAL_COST"] = pd.to_numeric(cw.get("TOTAL_COST"), errors="coerce").fillna(0.0).astype(float)
                cost_agg = (
                    cw.groupby(["YEAR", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"], dropna=False)["TOTAL_COST"]
                    .sum()
                    .reset_index()
                )
            df_demand = dem_agg.merge(cost_agg, on=["YEAR", "PROGRAMNAME", "TEAMNAME", "GROUPNAME"], how="left").fillna({"TOTAL_COST": 0.0})
        demand_meta["rows_df_demand"] = int(df_demand.shape[0]) if isinstance(df_demand, pd.DataFrame) else 0

    movers_meta = {
        "rows_df_cost": int(df_cost.shape[0]) if isinstance(df_cost, pd.DataFrame) else 0,
        "rows_df_top_apps": int(df_top_apps.shape[0]) if isinstance(df_top_apps, pd.DataFrame) else 0,
    }
    with perf_step("prepare_movers_and_signals", page_key=page_perf_key, meta=movers_meta):
        cost_per_demand_fte = (total_cost / derived_fte_sum) if derived_fte_sum > 0 else None
        mover_summary = _compute_mover_summary(df_cost, role, data_version)

        if expected_apps_total > 0 and df_top_apps is not None and not df_top_apps.empty:
            top5_total = (
                pd.to_numeric(df_top_apps.get("TOTAL_COST"), errors="coerce")
                .fillna(0.0)
                .sort_values(ascending=False)
                .head(5)
                .sum()
            )
            top5_share = float(top5_total) / float(expected_apps_total)

    if mover_summary.get("ready") and total_cost > 0:
        delta_ratio = abs(mover_summary.get("total_delta", 0.0)) / total_cost
        if delta_ratio >= 0.1:
            label_top_movers = "Needs attention"
        elif delta_ratio >= 0.04:
            label_top_movers = "Watch closely"

    if top5_share is not None:
        if top5_share >= 0.7:
            label_top_apps = "Needs attention"
        elif top5_share >= 0.4:
            label_top_apps = "Watch closely"

    label_treemap = "Stable"
    if expected_alloc_cov is None:
        label_treemap = "Watch closely"
    elif expected_alloc_cov < 0.8:
        label_treemap = "Needs attention"
    elif expected_alloc_cov < 0.95:
        label_treemap = "Watch closely"

    swag_ready_cov = None
    if df_explorer is not None and not df_explorer.empty:
        ready = df_explorer.get("SWAG_READY", pd.Series([False] * len(df_explorer), index=df_explorer.index)).fillna(False).astype(bool)
        swag_ready_cov = float(ready.mean()) if len(ready.index) else None
    if swag_ready_cov is None:
        label_demand_ready = "Watch closely"
    elif swag_ready_cov < 0.6:
        label_demand_ready = "Needs attention"
    elif swag_ready_cov < 0.85:
        label_demand_ready = "Watch closely"

def _safe_pct(num: float, den: float) -> Optional[float]:
    try:
        n = float(num)
        d = float(den)
    except Exception:
        return None
    if d <= 0:
        return None
    return n / d


def _sum_amt(df: pd.DataFrame, mask: Optional[pd.Series] = None) -> float:
    if df is None or df.empty or "AMOUNT" not in df.columns:
        return 0.0
    w = df if mask is None else df.loc[mask]
    return float(pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0).sum())


def _kpi_insight(kind: str, *, selected_year: int, as_of_year: int, values: dict) -> str:
    k = str(kind or "").strip().lower()
    plan_fy_now = float(values.get("plan_fy") or 0.0)
    forecast_fy_now = float(values.get("forecast_fy") or 0.0)
    variance_pct = values.get("variance_pct")
    wf_share = values.get("wf_share")
    coverage_pct = values.get("coverage_pct")
    assigned_pct = values.get("assigned_pct")

    if k == "plan":
        return "Baseline set for this year." if int(selected_year) == int(as_of_year) else f"Baseline for {int(selected_year)}."

    if k == "forecast":
        if plan_fy_now <= 0:
            return "Forecast loaded."
        if forecast_fy_now > plan_fy_now * 1.01:
            return "Above baseline — watch demand."
        if forecast_fy_now < plan_fy_now * 0.99:
            return "Below baseline — capacity available."
        return "On baseline."

    if k == "variance":
        try:
            vp = float(variance_pct) if variance_pct is not None else None
        except Exception:
            vp = None
        if vp is None:
            return "Variance not available."
        if abs(vp) < 0.03:
            return "Stable vs baseline."
        return "Forecast trending higher." if vp >= 0.03 else "Forecast trending lower."

    if k == "mix":
        try:
            s = float(wf_share) if wf_share is not None else None
        except Exception:
            s = None
        if s is None:
            return "Mix not available."
        if s >= 0.70:
            return "Mostly workforce-driven."
        if s <= 0.40:
            return "Mostly non-workforce-driven."
        return "Balanced mix."

    if k == "coverage":
        try:
            c = float(coverage_pct) if coverage_pct is not None else None
        except Exception:
            c = None
        if c is None:
            return "Coverage not available."
        if c >= 0.90:
            return "Strong ADO coverage."
        if c >= 0.70:
            return "Some work not fully sized."
        return "Low coverage — check inputs."

    if k == "allocation":
        try:
            a = float(assigned_pct) if assigned_pct is not None else None
        except Exception:
            a = None
        if a is None:
            return "Allocation not available."
        if a >= 0.90:
            return "Most spend assigned to apps."
        if a >= 0.70:
            return "Some spend not mapped to apps."
        return "High unmapped spend — review."

    return ""


variance_full = float(forecast_fy - plan_fy)
variance_pct_full = (forecast_fy / plan_fy - 1.0) if plan_fy > 0 else None

# Forecast FY lines (for mix/coverage/allocation KPIs).
df_forecast_fy_lines = df_lines.copy()
df_forecast_fy_lines["SCENARIO"] = (
    df_forecast_fy_lines.get("SCENARIO", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
)
df_forecast_fy_lines = df_forecast_fy_lines.loc[df_forecast_fy_lines["SCENARIO"].eq("EXPECTED")].copy()

cat = df_forecast_fy_lines.get("COST_CATEGORY", pd.Series(dtype=str)).fillna("").astype(str).str.upper().str.strip()
bucket = df_forecast_fy_lines.get("COST_BUCKET", pd.Series(dtype=str)).fillna("").astype(str).str.upper().str.strip()
is_wf = cat.eq("WORK_FORCE") | bucket.eq("WF")
is_nwf = cat.eq("NON_WORK_FORCE") | bucket.eq("NWF")

wf_fy = _sum_amt(df_forecast_fy_lines, is_wf)
nwf_fy = _sum_amt(df_forecast_fy_lines, is_nwf)
mix_total = wf_fy + nwf_fy
wf_pct = _safe_pct(wf_fy, mix_total)
nwf_pct = _safe_pct(nwf_fy, mix_total)
mix_main = "—" if not mix_total else f"{(wf_pct or 0.0)*100:.0f}% WF / {(nwf_pct or 0.0)*100:.0f}% NWF"

fte_col = next((c for c in ["FTE", "DERIVED_FTE_SWAG"] if c in df_forecast_fy_lines.columns), None)
fte_vals = pd.to_numeric(df_forecast_fy_lines.get(fte_col), errors="coerce").fillna(0.0) if fte_col else pd.Series(0.0, index=df_forecast_fy_lines.index)
has_fte = fte_vals > 0
covered_mask = (~is_wf) | has_fte
forecast_total_fy = _sum_amt(df_forecast_fy_lines)
covered_amt = _sum_amt(df_forecast_fy_lines, covered_mask)
coverage_pct = _safe_pct(covered_amt, forecast_total_fy)
coverage_main = f"{(coverage_pct or 0.0)*100:.0f}%" if forecast_total_fy else "—"

grp = df_forecast_fy_lines.get("GROUPNAME", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
assigned_mask = grp.ne("") & (~grp.str.upper().isin({"(UNASSIGNED)", "UNASSIGNED", "(PROGRAM NWF)", "(NEEDS MAPPING)"}))
assigned_amt = _sum_amt(df_forecast_fy_lines, assigned_mask)
assigned_pct = _safe_pct(assigned_amt, forecast_total_fy)
alloc_main = f"{(assigned_pct or 0.0)*100:.0f}% Assigned" if forecast_total_fy else "—"

insight_values = {
    "plan_fy": plan_fy,
    "forecast_fy": forecast_fy,
    "variance_pct": variance_pct_full,
    "wf_share": wf_pct,
    "coverage_pct": coverage_pct,
    "assigned_pct": assigned_pct,
}
plan_insight = _kpi_insight("plan", selected_year=int(selected_year), as_of_year=int(as_of.year), values=insight_values)
forecast_insight = _kpi_insight("forecast", selected_year=int(selected_year), as_of_year=int(as_of.year), values=insight_values)
variance_insight = _kpi_insight("variance", selected_year=int(selected_year), as_of_year=int(as_of.year), values=insight_values)
mix_insight = _kpi_insight("mix", selected_year=int(selected_year), as_of_year=int(as_of.year), values=insight_values)
coverage_insight = _kpi_insight("coverage", selected_year=int(selected_year), as_of_year=int(as_of.year), values=insight_values)
alloc_insight = _kpi_insight("allocation", selected_year=int(selected_year), as_of_year=int(as_of.year), values=insight_values)
WELCOME_KPI_MOTIF_MAP = {
    "Planned cost": "bars",           # total
    "Forecast cost": "bars",          # total
    "Forecast vs Plan": "wave",       # variance
    "Cost Mix": "curve",              # share
}

st.markdown('<div id="welcome-kpi-grid-anchor" style="display:none;"></div>', unsafe_allow_html=True)
st.markdown('<div id="welcome-snapshot-anchor" style="display:none;"></div>', unsafe_allow_html=True)
st.markdown(
    """
    <style>
    @import url("https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20,400,0,0");
    /* FinOps KPI cards – Welcome only */
    :root {
        --kpi-card-h: 156px;
        --kpi-row-gap: 12px;
        --kpi-tall-h: calc((2 * var(--kpi-card-h)) + var(--kpi-row-gap));
    }
    .finops-kpi-card {
        display: flex;
        background: transparent;
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 11px;
        overflow: hidden;
        height: var(--kpi-card-h);
        width: 100%;
        max-width: 100%;
        box-sizing: border-box;
        position: relative;
        isolation: isolate;
        min-width: 0;
        box-shadow: 0 6px 14px rgba(2, 6, 23, 0.12), 0 1px 0 rgba(255,255,255,0.03) inset;
        transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
    }
    .finops-kpi-bg-layer {
        position: absolute;
        inset: 0 0 0 4px;
        pointer-events: none;
        z-index: 0;
        background:
            radial-gradient(160px 90px at 95% 12%, color-mix(in srgb, var(--finops-kpi-accent) 30%, transparent), transparent 76%),
            linear-gradient(180deg, color-mix(in srgb, var(--finops-kpi-accent) 10%, transparent), transparent 64%);
        opacity: 0.9;
    }
    .finops-kpi-bg-art {
        position: absolute;
        right: 0;
        bottom: 0;
        width: 52%;
        height: 58%;
        display: inline-flex;
        align-items: flex-end;
        gap: 8px;
        pointer-events: none;
        z-index: 0;
        opacity: 0.3;
        filter: saturate(1.05) blur(0.4px);
        -webkit-mask-image: linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 0.08) 28%, rgba(0, 0, 0, 0.55) 54%, #000 100%);
        mask-image: linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 0.08) 28%, rgba(0, 0, 0, 0.55) 54%, #000 100%);
    }
    .finops-kpi-bg-bar {
        flex: 1 1 auto;
        border-radius: 3px 3px 0 0;
        background: linear-gradient(180deg, rgba(148, 163, 184, 0.9), rgba(148, 163, 184, 0.2));
        min-height: 3px;
    }
    .finops-kpi-bg-art--good .finops-kpi-bg-bar {
        background: linear-gradient(180deg, rgba(74, 222, 128, 0.95), rgba(34, 197, 94, 0.22));
    }
    .finops-kpi-bg-art--watch .finops-kpi-bg-bar {
        background: linear-gradient(180deg, rgba(251, 191, 36, 0.95), rgba(245, 158, 11, 0.24));
    }
    .finops-kpi-bg-art--risk .finops-kpi-bg-bar {
        background: linear-gradient(180deg, rgba(248, 113, 113, 0.95), rgba(239, 68, 68, 0.24));
    }
    .finops-kpi-bg-art--wave .finops-kpi-bg-bar,
    .finops-kpi-bg-art--dotline .finops-kpi-bg-bar,
    .finops-kpi-bg-art--curve .finops-kpi-bg-bar {
        display: none;
    }
    .finops-kpi-bg-art--wave,
    .finops-kpi-bg-art--dotline,
    .finops-kpi-bg-art--curve {
        width: 58%;
        height: 62%;
    }
    .finops-kpi-bg-svg {
        width: 100%;
        height: 100%;
    }
    .finops-kpi-bg-fill {
        display: none !important;
        fill: none !important;
    }
    .finops-kpi-bg-poly {
        fill: none;
        stroke: rgba(148, 163, 184, 0.72);
        stroke-width: 1.05px;
        stroke-linecap: round;
        stroke-linejoin: round;
        vector-effect: non-scaling-stroke;
        opacity: 0.42;
        filter: drop-shadow(0 0 1px color-mix(in srgb, var(--finops-kpi-accent) 20%, transparent));
    }
    .finops-kpi-bg-curve {
        fill: none;
        stroke: rgba(148, 163, 184, 0.72);
        stroke-width: 1.05px;
        stroke-linecap: round;
        stroke-linejoin: round;
        vector-effect: non-scaling-stroke;
        opacity: 0.42;
        filter: drop-shadow(0 0 1px color-mix(in srgb, var(--finops-kpi-accent) 20%, transparent));
    }
    .finops-kpi-bg-dots {
        position: absolute;
        inset: 0;
        pointer-events: none;
        opacity: 0.2;
    }
    .finops-kpi-bg-dot-el {
        position: absolute;
        width: 5px;
        height: 5px;
        border-radius: 50%;
        background: rgba(226, 232, 240, 0.9);
        transform: translate(-50%, -50%);
        box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.5), 0 0 3px color-mix(in srgb, var(--finops-kpi-accent) 24%, transparent);
    }
    .finops-kpi-bg-art--good .finops-kpi-bg-fill { fill: rgba(34, 197, 94, 0.16); }
    .finops-kpi-bg-art--watch .finops-kpi-bg-fill { fill: rgba(245, 158, 11, 0.16); }
    .finops-kpi-bg-art--risk .finops-kpi-bg-fill { fill: rgba(239, 68, 68, 0.16); }
    .finops-kpi-bg-art--good .finops-kpi-bg-poly { stroke: rgba(74, 222, 128, 0.76); }
    .finops-kpi-bg-art--watch .finops-kpi-bg-poly { stroke: rgba(251, 191, 36, 0.76); }
    .finops-kpi-bg-art--risk .finops-kpi-bg-poly { stroke: rgba(248, 113, 113, 0.76); }
    .finops-kpi-bg-art--good .finops-kpi-bg-curve { stroke: rgba(74, 222, 128, 0.78); }
    .finops-kpi-bg-art--watch .finops-kpi-bg-curve { stroke: rgba(251, 191, 36, 0.78); }
    .finops-kpi-bg-art--risk .finops-kpi-bg-curve { stroke: rgba(248, 113, 113, 0.78); }
    .finops-kpi-card::before {
        content: "";
        position: absolute;
        inset: 0 0 45% 4px;
        pointer-events: none;
        z-index: 0;
        background: linear-gradient(180deg, rgba(255,255,255,0.10) 0%, rgba(255,255,255,0.03) 55%, rgba(255,255,255,0.0) 100%);
    }
    .finops-kpi-card::after {
        content: "";
        position: absolute;
        inset: 0 0 0 4px;
        pointer-events: none;
        z-index: 0;
        background-image: repeating-radial-gradient(circle at 12% 10%, rgba(255,255,255,0.06) 0 0.7px, transparent 0.7px 2.8px);
        opacity: 0.04;
        filter: blur(0.7px);
        mix-blend-mode: screen;
    }
    .finops-kpi-watermark {
        position: absolute;
        right: 10px;
        top: 8px;
        z-index: 0;
        pointer-events: none;
        opacity: 0.05;
        filter: blur(0.4px);
        line-height: 1;
    }
    .finops-kpi-watermark .material-symbols-outlined {
        font-size: 54px;
        color: rgba(226, 232, 240, 0.65);
    }
    .finops-kpi-card--tall {
        height: var(--kpi-tall-h);
    }
    .finops-kpi-accent {
        width: 4px;
        flex-shrink: 0;
        border-top-left-radius: 8px;
        border-bottom-left-radius: 8px;
    }
    .finops-kpi-body {
        padding: 11px 13px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        width: 100%;
        height: 100%;
        position: relative;
        z-index: 1;
    }
    .finops-kpi-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 6px;
    }
    .finops-kpi-title-wrap {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        min-width: 0;
    }
    .finops-kpi-title-icon {
        font-size: 16px;
        color: rgba(148, 163, 184, 0.9);
        flex-shrink: 0;
    }
    .finops-kpi-title {
        font-size: 0.78rem;
        color: rgba(255,255,255,0.74);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 100%;
    }
    .finops-kpi-title--sm {
        font-size: 0.5rem;
    }
    .finops-kpi-help {
        font-size: 0.75rem;
        color: rgba(255,255,255,0.6);
        border: 1px solid rgba(255,255,255,0.4);
        border-radius: 50%;
        width: 16px;
        height: 16px;
        line-height: 14px;
        text-align: center;
        cursor: default;
    }
    .finops-kpi-meta {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        flex-shrink: 0;
    }
    .finops-kpi-status {
        display: inline-flex;
        align-items: center;
        gap: 3px;
        font-size: 0.65rem;
        border-radius: 999px;
        padding: 2px 7px;
        border: 1px solid rgba(148, 163, 184, 0.35);
        background: rgba(148, 163, 184, 0.14);
        color: rgba(226, 232, 240, 0.95);
    }
    .finops-kpi-status .material-symbols-outlined {
        font-size: 13px;
    }
    .finops-kpi-status--good {
        border-color: rgba(34, 197, 94, 0.55);
        background: rgba(34, 197, 94, 0.16);
        color: rgba(187, 247, 208, 0.95);
    }
    .finops-kpi-status--watch {
        border-color: rgba(245, 158, 11, 0.55);
        background: rgba(245, 158, 11, 0.16);
        color: rgba(253, 230, 138, 0.95);
    }
    .finops-kpi-status--risk {
        border-color: rgba(239, 68, 68, 0.55);
        background: rgba(239, 68, 68, 0.16);
        color: rgba(254, 202, 202, 0.95);
    }
    .finops-kpi-value {
        font-size: clamp(1.22rem, 1.7vw, 1.82rem);
        font-weight: 700;
        margin: 2px 0 6px 0;
        color: rgba(248, 250, 252, 0.95);
        min-width: 0;
    }
    .finops-kpi-value-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
    }
    .finops-kpi-value-meta {
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }
    .finops-kpi-trend {
        display: inline-flex;
        align-items: center;
        gap: 2px;
        border-radius: 999px;
        font-size: 0.67rem;
        line-height: 1;
        padding: 3px 7px;
        border: 1px solid rgba(148, 163, 184, 0.35);
        background: rgba(148, 163, 184, 0.14);
        color: rgba(226, 232, 240, 0.95);
        white-space: nowrap;
    }
    .finops-kpi-trend .material-symbols-outlined {
        font-size: 12px;
    }
    .finops-kpi-trend--good {
        border-color: rgba(34, 197, 94, 0.55);
        background: rgba(34, 197, 94, 0.16);
        color: rgba(187, 247, 208, 0.95);
    }
    .finops-kpi-trend--watch {
        border-color: rgba(245, 158, 11, 0.55);
        background: rgba(245, 158, 11, 0.16);
        color: rgba(253, 230, 138, 0.95);
    }
    .finops-kpi-trend--risk {
        border-color: rgba(239, 68, 68, 0.55);
        background: rgba(239, 68, 68, 0.16);
        color: rgba(254, 202, 202, 0.95);
    }
    .finops-kpi-value--sm {
        font-size: 1.4rem;
    }
    .finops-kpi-chips {
        margin-bottom: 6px;
    }
    .finops-kpi-chip {
        display: inline-block;
        font-size: 0.68rem;
        padding: 2px 6px;
        border-radius: 10px;
        margin-right: 4px;
        background: rgba(255,255,255,0.12);
        color: rgba(248, 250, 252, 0.9);
        white-space: nowrap;
        max-width: 100%;
        overflow: hidden;
        text-overflow: ellipsis;
        vertical-align: top;
    }
    .finops-kpi-lines {
        font-size: 0.72rem;
        color: rgba(255,255,255,0.5);
        line-height: 1.2;
        min-width: 0;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    @media (hover: hover) and (pointer: fine) {
        .finops-kpi-card:hover {
            transform: translateY(-1px);
            box-shadow: 0 10px 20px rgba(2, 6, 23, 0.18), 0 1px 0 rgba(255,255,255,0.05) inset;
            border-color: rgba(255,255,255,0.2);
        }
    }
    .finops-kpi-line {
        height: 1.2rem;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
    }
    .finops-kpi-row {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        gap: 8px;
        width: 100%;
        align-items: center;
        overflow: hidden;
    }
    .finops-kpi-row .finops-kpi-label {
        display: block;
        gap: 6px;
        align-items: center;
        min-width: 0;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
        max-width: 100%;
    }
    .finops-kpi-row .finops-kpi-amount {
        text-align: right;
        color: rgba(248, 250, 252, 0.95);
        white-space: nowrap;
    }
    .finops-kpi-total {
        font-weight: 600;
        color: rgba(248, 250, 252, 0.95);
    }
    .finops-kpi-muted {
        color: rgba(255,255,255,0.6);
    }
    .inv-card-body {
        display: flex;
        flex-direction: column;
        height: 100%;
    }
    .inv-alerts {
        display: flex;
        flex-direction: column;
        gap: 6px;
    }
    .inv-row-left {
        min-width: 0;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
        max-width: 100%;
    }
    .inv-row-right {
        white-space: nowrap;
        max-width: 42%;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .kpi-stack {
        display: flex;
        flex-direction: column;
        gap: var(--kpi-row-gap);
        min-width: 0;
    }
    .welcome-baseline-card {
        height: var(--kpi-tall-h);
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 11px;
        background: rgba(15, 23, 42, 0.28);
        padding: 12px 12px 10px;
        display: flex;
        flex-direction: column;
        box-sizing: border-box;
        overflow: hidden;
        position: relative;
        isolation: isolate;
        box-shadow: 0 8px 18px rgba(2, 6, 23, 0.16), 0 1px 0 rgba(255,255,255,0.03) inset;
        transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
    }
    .welcome-baseline-compact {
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 11px;
        background: rgba(15, 23, 42, 0.28);
        display: flex;
        box-sizing: border-box;
        overflow: hidden;
        width: 100%;
        max-width: 100%;
        min-width: 0;
        position: relative;
        isolation: isolate;
        box-shadow: 0 8px 18px rgba(2, 6, 23, 0.16), 0 1px 0 rgba(255,255,255,0.03) inset;
        transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
    }
    .welcome-baseline-card::before,
    .welcome-baseline-compact::before {
        content: "";
        position: absolute;
        inset: 0 0 45% 4px;
        pointer-events: none;
        z-index: 0;
        background: linear-gradient(180deg, rgba(255,255,255,0.10) 0%, rgba(255,255,255,0.03) 55%, rgba(255,255,255,0.0) 100%);
    }
    .welcome-baseline-compact .finops-kpi-bg-layer {
        position: absolute;
        inset: 0 0 0 4px;
        pointer-events: none;
        z-index: 0;
        background:
            radial-gradient(180px 100px at 95% 12%, color-mix(in srgb, #3b82f6 42%, transparent), transparent 74%),
            linear-gradient(180deg, color-mix(in srgb, #3b82f6 16%, transparent), transparent 62%);
        opacity: 1;
    }
    .baseline-compact-bg-art {
        position: absolute;
        right: 0;
        bottom: 0;
        width: 58%;
        height: 62%;
        pointer-events: none;
        z-index: 0;
        opacity: 0.38;
        background:
            radial-gradient(circle at 20% 65%, rgba(148,163,184,0.7) 0 2px, transparent 2px),
            radial-gradient(circle at 36% 46%, rgba(148,163,184,0.6) 0 2px, transparent 2px),
            radial-gradient(circle at 52% 58%, rgba(148,163,184,0.52) 0 2px, transparent 2px),
            radial-gradient(circle at 68% 38%, rgba(148,163,184,0.46) 0 2px, transparent 2px),
            radial-gradient(circle at 84% 54%, rgba(148,163,184,0.4) 0 2px, transparent 2px);
        filter: saturate(1.15);
    }
    .welcome-baseline-compact::after {
        content: "";
        position: absolute;
        inset: 0 0 0 4px;
        pointer-events: none;
        z-index: 0;
        background-image: repeating-radial-gradient(circle at 12% 10%, rgba(255,255,255,0.06) 0 0.7px, transparent 0.7px 2.8px);
        opacity: 0.12;
        mix-blend-mode: screen;
    }
    .baseline-watermark {
        position: absolute;
        right: 10px;
        top: 8px;
        z-index: 0;
        pointer-events: none;
        opacity: 0.08;
        filter: blur(0.3px);
        line-height: 1;
        font-size: 54px;
        color: rgba(226, 232, 240, 0.65);
    }
    .baseline-compact-accent {
        width: 4px;
        flex-shrink: 0;
        border-top-left-radius: 8px;
        border-bottom-left-radius: 8px;
        background: linear-gradient(180deg, rgba(96,165,250,0.95), rgba(59,130,246,0.7));
    }
    .baseline-compact-body {
        flex: 1;
        padding: 10px 12px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        min-width: 0;
        position: relative;
        z-index: 1;
    }
    .baseline-compact-header-meta {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        flex-shrink: 0;
    }
    .baseline-compact-value-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
    }
    .baseline-compact-title {
        font-size: 0.68rem;
        color: rgba(226, 232, 240, 0.78);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .baseline-compact-value {
        font-size: clamp(1.12rem, 1.55vw, 1.6rem);
        font-weight: 600;
        color: rgba(248, 250, 252, 0.95);
        min-width: 0;
    }
    .baseline-compact-unit {
        font-size: 0.72rem;
        font-weight: 500;
        color: rgba(226, 232, 240, 0.72);
    }
    .baseline-compact-meta {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
        min-height: 1rem;
        min-width: 0;
    }
    .baseline-compact-chip {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        font-size: 0.62rem;
        padding: 2px 7px;
        border: 1px solid rgba(148, 163, 184, 0.45);
        color: rgba(226, 232, 240, 0.95);
        background: rgba(148, 163, 184, 0.14);
        white-space: nowrap;
        line-height: 1;
    }
    .baseline-compact-badge {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        font-size: 0.62rem;
        padding: 2px 7px;
        border: 1px solid rgba(251, 191, 36, 0.55);
        background: rgba(245, 158, 11, 0.18);
        color: rgba(253, 230, 138, 0.95);
        white-space: nowrap;
        line-height: 1;
    }
    .baseline-compact-delta {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        font-size: 0.61rem;
        line-height: 1;
        padding: 3px 8px;
        border: 1px solid rgba(148, 163, 184, 0.45);
        background: rgba(148, 163, 184, 0.16);
        color: rgba(226, 232, 240, 0.95);
        white-space: nowrap;
        animation: baseline-delta-enter 620ms ease-out 1 both;
    }
    .baseline-compact-delta--up {
        border-color: rgba(34, 197, 94, 0.5);
        background: rgba(34, 197, 94, 0.16);
        color: rgba(187, 247, 208, 0.95);
    }
    .baseline-compact-delta--down {
        border-color: rgba(239, 68, 68, 0.5);
        background: rgba(239, 68, 68, 0.15);
        color: rgba(254, 202, 202, 0.95);
    }
    .baseline-compact-pi {
        font-size: 0.68rem;
        color: rgba(148, 163, 184, 0.95);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 48%;
        text-align: right;
    }
    .baseline-compact-spark {
        width: 100%;
        height: 30px;
        display: flex;
        align-items: center;
        margin: 2px 0 2px 0;
        opacity: 0.95;
    }
    .baseline-compact-spark svg {
        width: 100%;
        height: 30px;
    }
    .baseline-card-enter {
        animation: baseline-card-enter 360ms ease-out 1 both;
    }
    .baseline-title-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 6px;
    }
    .baseline-title {
        font-size: 0.83rem;
        color: rgba(255,255,255,0.72);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .baseline-badge {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        font-size: 0.62rem;
        padding: 2px 7px;
        border: 1px solid rgba(251, 191, 36, 0.55);
        background: rgba(245, 158, 11, 0.18);
        color: rgba(253, 230, 138, 0.95);
        white-space: nowrap;
        line-height: 1;
    }
    .baseline-value {
        margin-top: 6px;
        font-size: 1.45rem;
        font-weight: 600;
        color: rgba(248, 250, 252, 0.96);
        letter-spacing: 0.01em;
    }
    .baseline-value-row {
        margin-top: 6px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
    }
    .baseline-delta-chip {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        font-size: 0.62rem;
        line-height: 1;
        padding: 3px 8px;
        border: 1px solid rgba(148, 163, 184, 0.45);
        background: rgba(148, 163, 184, 0.16);
        color: rgba(226, 232, 240, 0.95);
        animation: baseline-delta-enter 620ms ease-out 1 both;
        white-space: nowrap;
    }
    .baseline-delta-chip--up {
        border-color: rgba(34, 197, 94, 0.5);
        background: rgba(34, 197, 94, 0.16);
        color: rgba(187, 247, 208, 0.95);
    }
    .baseline-delta-chip--down {
        border-color: rgba(239, 68, 68, 0.5);
        background: rgba(239, 68, 68, 0.15);
        color: rgba(254, 202, 202, 0.95);
    }
    .baseline-value--pulse {
        animation: baseline-value-pulse 650ms ease-out 1;
    }
    .baseline-subline {
        margin-top: 2px;
        font-size: 0.73rem;
        color: rgba(226, 232, 240, 0.78);
        min-height: 1.1rem;
    }
    .baseline-spark-wrap {
        margin-top: 8px;
        min-height: 30px;
        display: flex;
        align-items: center;
    }
    .baseline-spark {
        width: 100%;
        height: 28px;
    }
    .baseline-footer {
        margin-top: auto;
        font-size: 0.68rem;
        color: rgba(148, 163, 184, 0.95);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    @keyframes baseline-card-enter {
        0% {
            opacity: 0;
            transform: translateY(6px);
        }
        100% {
            opacity: 1;
            transform: translateY(0);
        }
    }
    @keyframes baseline-value-pulse {
        0% {
            text-shadow: 0 0 0 rgba(96, 165, 250, 0);
            filter: drop-shadow(0 0 0 rgba(96, 165, 250, 0));
        }
        45% {
            text-shadow: 0 0 10px rgba(96, 165, 250, 0.26);
            filter: drop-shadow(0 0 6px rgba(96, 165, 250, 0.2));
        }
        100% {
            text-shadow: 0 0 0 rgba(96, 165, 250, 0);
            filter: drop-shadow(0 0 0 rgba(96, 165, 250, 0));
        }
    }
    @keyframes baseline-delta-enter {
        0% {
            opacity: 0;
            transform: translateY(6px);
        }
        100% {
            opacity: 1;
            transform: translateY(0);
        }
    }
    @media (prefers-reduced-motion: reduce) {
        .baseline-card-enter,
        .baseline-value--pulse,
        .baseline-delta-chip,
        .baseline-compact-delta,
        .finops-kpi-card {
            animation: none !important;
            transition: none !important;
        }
    }
    @media (hover: hover) and (pointer: fine) {
        .welcome-baseline-card:hover,
        .welcome-baseline-compact:hover {
            transform: translateY(-2px);
            box-shadow: 0 14px 24px rgba(2, 6, 23, 0.24), 0 1px 0 rgba(255,255,255,0.05) inset;
            border-color: rgba(255,255,255,0.2);
        }
    }
    .inv-total {
        margin-top: 12px;
        padding-top: 10px;
        border-top: 1px solid rgba(255,255,255,0.08);
        display: flex;
        justify-content: space-between;
        align-items: baseline;
        font-weight: 600;
        color: rgba(248, 250, 252, 0.95);
    }
    .inv-total-label {
        font-size: 1.1em;
    }
    .inv-total-value {
        text-align: right;
        font-size: 1.1em;
    }
    .inv-more {
        opacity: 0.75;
        font-size: 0.9em;
        margin-top: 2px;
    }
    .material-symbols-outlined {
        font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 20;
        font-size: 18px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

planned_html = _build_finops_kpi_card_html(
    title="Planned cost",
    value=_fmt_money(plan_fy),
    tip="Baseline planned cost for the selected scope.",
    chips=["NEXT (WF+NWF)"],
    lines=[plan_insight],
    accent_color=welcome_kpi_accents["Planned cost"],
    icon="account_balance",
    status="neutral",
    status_label="Plan",
    spark_values=[0.28, 0.34, 0.41, 0.47, 0.56, 0.64, 0.72, 0.8],
    spark_tone="neutral",
    spark_motif=WELCOME_KPI_MOTIF_MAP.get("Planned cost", "bars"),
)
forecast_status = "neutral"
if variance_pct_full is not None:
    if variance_pct_full <= 0.02:
        forecast_status = "good"
    elif variance_pct_full <= 0.10:
        forecast_status = "watch"
    else:
        forecast_status = "risk"

forecast_html = _build_finops_kpi_card_html(
    title="Forecast cost",
    value=_fmt_money(forecast_fy),
    tip="Forecast cost for the selected scope (NEXT + ADO).",
    chips=["NEXT + ADO"],
    lines=[forecast_insight],
    accent_color=welcome_kpi_accents["Forecast cost"],
    icon="query_stats",
    status=forecast_status,
    status_label="Forecast",
    trend_text=(f"{variance_pct_full * 100.0:+.1f}% vs plan" if variance_pct_full is not None else ""),
    trend_tone="risk" if (variance_pct_full or 0.0) > 0.0 else "good",
    spark_values=(
        [0.24, 0.33, 0.29, 0.46, 0.42, 0.57, 0.53, 0.69]
        if (variance_pct_full or 0.0) >= 0
        else [0.69, 0.57, 0.62, 0.49, 0.53, 0.4, 0.43, 0.31]
    ),
    spark_tone=("risk" if (variance_pct_full or 0.0) > 0.0 else "good"),
    spark_motif=WELCOME_KPI_MOTIF_MAP.get("Forecast cost", "bars"),
)

variance_status = "neutral"
variance_tone = "neutral"
if variance_pct_full is not None:
    if abs(variance_pct_full) <= 0.03:
        variance_status = "good"
        variance_tone = "good"
    elif abs(variance_pct_full) <= 0.10:
        variance_status = "watch"
        variance_tone = "watch"
    else:
        variance_status = "risk"
        variance_tone = "risk"

variance_html = _build_finops_kpi_card_html(
    title="Forecast vs Plan",
    value=_fmt_money(variance_full),
    tip="Forecast minus Plan for the selected scope.",
    chips=["NEXT + ADO"],
    lines=[variance_insight],
    accent_color=welcome_kpi_accents["Forecast vs Plan"],
    icon="monitoring",
    status=variance_status,
    status_label="Variance",
    trend_text=(f"{variance_pct_full * 100.0:+.1f}%" if variance_pct_full is not None else ""),
    trend_tone=variance_tone,
    spark_values=(
        [0.22, 0.48, 0.31, 0.55, 0.37, 0.63, 0.44, 0.71]
        if (variance_full or 0.0) >= 0
        else [0.71, 0.56, 0.62, 0.45, 0.51, 0.34, 0.4, 0.24]
    ),
    spark_tone=variance_tone,
    spark_motif=WELCOME_KPI_MOTIF_MAP.get("Forecast vs Plan", "wave"),
)

mix_html = _build_finops_kpi_card_html(
    title="Cost Mix",
    value=mix_main,
    tip="Forecast full-year split between workforce and non-workforce.",
    chips=["NEXT + ADO"],
    lines=[mix_insight],
    accent_color=welcome_kpi_accents["Cost Mix"],
    icon="pie_chart",
    status="good" if wf_pct is not None and nwf_pct is not None else "neutral",
    status_label="Mix",
    trend_text=(f"WF {(wf_pct or 0.0) * 100.0:.0f}% / NWF {(nwf_pct or 0.0) * 100.0:.0f}%" if wf_pct is not None and nwf_pct is not None else ""),
    trend_tone="neutral",
    spark_values=(
        [0.62, 0.38, 0.58, 0.42, 0.61, 0.39, 0.57, 0.43]
        if (wf_pct or 0.0) >= (nwf_pct or 0.0)
        else [0.38, 0.62, 0.42, 0.58, 0.39, 0.61, 0.43, 0.57]
    ),
    spark_tone="neutral",
    spark_motif=WELCOME_KPI_MOTIF_MAP.get("Cost Mix", "curve"),
)
invoices_html, alert_state = _build_invoices_contracts_card_html(
    fiscal_year=int(selected_year),
    programs=sel_programs,
    teams=sel_teams,
    groups=sel_groups,
    apps=sel_apps,
    filters_sig=cache_filters_sig,
    user_scope_sig=cache_user_scope_sig,
    cache_buster=cache_buster,
)


def render_snapshot_section() -> None:
    st.markdown(
        """
        <style>
        .other-kpi-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(220px, 1fr));
            gap: 12px;
            align-items: stretch;
            width: 100%;
        }
        .snapshot-kpi-grid {
            margin-top: -20px;
        }
        .other-kpi-stack {
            display: flex;
            flex-direction: column;
            gap: var(--kpi-row-gap);
            min-width: 0;
        }
        @media (max-width: 1280px) {
            .other-kpi-grid { grid-template-columns: repeat(2, minmax(220px, 1fr)); }
        }
        @media (max-width: 640px) {
            .other-kpi-grid { grid-template-columns: 1fr; }
            .snapshot-kpi-grid { margin-top: 0; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    kpi_card_h = 150
    kpi_row_gap = 12
    kpi_tall_h = (2 * kpi_card_h) + kpi_row_gap
    baseline_single_row_html = _build_velocity_baseline_compact_card_html(
        velocity_snapshot,
        height_px=kpi_card_h,
    )
    st.subheader("Snapshot")
    st.markdown(f"**{headline.text}**")

    st.markdown(
        (
            "<div class='other-kpi-grid snapshot-kpi-grid'>"
            f"<div class='other-kpi-stack'>{planned_html}{baseline_single_row_html}</div>"
            f"<div class='other-kpi-stack'>{forecast_html}{mix_html}</div>"
            f"<div class='other-kpi-stack'>{invoices_html}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    if st.session_state.get("debug_welcome"):
        vel_src_counts = {}
        try:
            vel_src = df_velocity.get("BASELINE_SOURCE", pd.Series(dtype=str)).fillna("").astype(str).str.upper().str.strip()
            vel_src_counts = vel_src.value_counts(dropna=False).to_dict()
        except Exception:
            vel_src_counts = {}
        st.json(
            {
                "alerts_n": alert_state.get("alerts_n"),
                "critical": alert_state.get("critical"),
                "pending": alert_state.get("pending"),
                "contract": alert_state.get("contract"),
                "missing_dates_n": alert_state.get("missing_dates_n"),
                "invoice_names_sample": alert_state.get("debug_invoice_names"),
                "top_rows_names": alert_state.get("debug_top_names"),
                "programs": sel_programs,
                "teams": sel_teams,
                "groups": sel_groups,
                "velocity_rows_loaded": int(len(df_velocity.index)) if isinstance(df_velocity, pd.DataFrame) else 0,
                "velocity_source_counts": vel_src_counts,
                "velocity_snapshot": build_velocity_debug_meta(velocity_snapshot),
            }
        )

    _render_section_divider()


def render_disclaimer() -> None:
    st.subheader("Disclaimer")
    st.caption(
        "The NEXT app is an analytical and planning tool designed to estimate baseline, projected, and actual costs across programs, teams, and application groups. Cost views are generated from a combination of Azure DevOps effort allocation, Apptio actuals (where available), and manual inputs provided by users.\n\n"
        "Variances and gaps may occur due to incomplete mappings, program-level actuals, timing differences, or missing updates. NEXT outputs should be used for directional insights and scenario evaluation, and not as an authoritative financial statement."
    )
    st.caption("💡 Tip: Use the Data Quality and Mapping Coverage indicators to understand how reliable each view is.")


_set_loading("Rendering KPI snapshot...", 3)
with perf_step("render_kpi_snapshot", page_key=page_perf_key):
    render_snapshot_section()
mark_first_kpi_render(page_perf_key)

_set_loading("Rendering charts (1/3)...", 4)
row1_left, row1_right = st.columns(2, gap="large")
with row1_left:
    _section_title(
        "Plan vs Forecast",
        "Compares the approved plan to the current forecast for the selected scope.",
    )
    # Canonical sources (single source of truth).
    # Baseline: approved plan. Expected: expected run-rate for the year.
    _plan_vs_forecast_meta = {"dimension": "PROGRAMNAME"}
    with perf_step("plan_vs_forecast_cost_fetch", page_key=page_perf_key, meta=_plan_vs_forecast_meta):
        df_base_pi = get_pi_costs(
            _cost_df_for("Baseline"),
            scenario="Baseline",
            filters={**filters_cost, "group_by": ["PROGRAMNAME"]},
        )
        df_exp_pi = get_pi_costs(
            _cost_df_for("Projected"),
            scenario="Projected",
            filters={**filters_cost, "group_by": ["PROGRAMNAME"]},
        )
        _plan_vs_forecast_meta["rows_baseline"] = int(df_base_pi.shape[0]) if isinstance(df_base_pi, pd.DataFrame) else 0
        _plan_vs_forecast_meta["rows_projected"] = int(df_exp_pi.shape[0]) if isinstance(df_exp_pi, pd.DataFrame) else 0
    baseline_lines = (
        df_base_pi.rename(columns={"TOTAL_COST": "AMOUNT"})
        if df_base_pi is not None and not df_base_pi.empty
        else pd.DataFrame(columns=["PROGRAMNAME", "AMOUNT"])
    )
    expected_lines = (
        df_exp_pi.rename(columns={"TOTAL_COST": "AMOUNT"})
        if df_exp_pi is not None and not df_exp_pi.empty
        else pd.DataFrame(columns=["PROGRAMNAME", "AMOUNT"])
    )

    dim = "PROGRAMNAME"
    if role != "Portfolio Manager":
        pass
    st.caption("Plan is the approved view; Forecast is the current outlook for the year.")

    def _by_dim(df: pd.DataFrame, out_col: str) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame(columns=[dim, out_col])
            w = df.copy()
            w[dim] = w.get(dim, "").fillna("").astype(str).str.strip()
            if dim == "GROUPNAME":
                if include_unmapped:
                    w.loc[w[dim].eq(""), dim] = "(Needs mapping)"
                else:
                    w = w[w[dim].ne("")].copy()
            elif dim == "PROGRAMNAME":
                if include_unmapped:
                    w.loc[w[dim].eq(""), dim] = "(Not onboarded)"
                else:
                    w = w[w[dim].ne("")].copy()
            w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)
            return w.groupby(dim, as_index=False)["AMOUNT"].sum().rename(columns={"AMOUNT": out_col})

    if dim == "GROUPNAME":
            # Hybrid app allocation:
            # - Take Baseline/Expected totals by PROGRAM (including program-level overhead / additional costs).
            # - Allocate down to app groups using EXPECTED app shares per program.
            # This avoids a large '(Unassigned)' bucket and ensures all EXPECTED app groups appear.
            base_prog = pd.DataFrame(columns=["PROGRAMNAME", "BASELINE_TOTAL"])
            if baseline_lines is not None and not baseline_lines.empty:
                b = baseline_lines.copy()
                b["PROGRAMNAME"] = b.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                if include_unmapped:
                    b.loc[b["PROGRAMNAME"].eq(""), "PROGRAMNAME"] = "(Not onboarded)"
                else:
                    b = b[b["PROGRAMNAME"].ne("")].copy()
                b["AMOUNT"] = pd.to_numeric(b.get("AMOUNT"), errors="coerce").fillna(0.0)
                base_prog = b.groupby("PROGRAMNAME", as_index=False)["AMOUNT"].sum().rename(columns={"AMOUNT": "BASELINE_TOTAL"})

            exp_prog = pd.DataFrame(columns=["PROGRAMNAME", "EXPECTED_TOTAL"])
            if expected_lines is not None and not expected_lines.empty:
                r = expected_lines.copy()
                r["PROGRAMNAME"] = r.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                if include_unmapped:
                    r.loc[r["PROGRAMNAME"].eq(""), "PROGRAMNAME"] = "(Not onboarded)"
                else:
                    r = r[r["PROGRAMNAME"].ne("")].copy()
                r["AMOUNT"] = pd.to_numeric(r.get("AMOUNT"), errors="coerce").fillna(0.0)
                exp_prog = r.groupby("PROGRAMNAME", as_index=False)["AMOUNT"].sum().rename(columns={"AMOUNT": "EXPECTED_TOTAL"})

            totals_prog = base_prog.merge(exp_prog, on="PROGRAMNAME", how="outer").fillna(0.0)

            exp_src = expected_lines
            if exp_src is None or exp_src.empty:
                df_compare = pd.DataFrame(columns=["GROUPNAME", "BASELINE_COST", "EXPECTED_COST"])
            else:
                exp = exp_src.copy()
                exp["PROGRAMNAME"] = exp.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
                if include_unmapped:
                    exp.loc[exp["PROGRAMNAME"].eq(""), "PROGRAMNAME"] = "(Not onboarded)"
                else:
                    exp = exp[exp["PROGRAMNAME"].ne("")].copy()
                exp["TEAMNAME"] = exp.get("TEAMNAME", "").fillna("").astype(str).str.strip()
                exp["GROUPNAME"] = exp.get("GROUPNAME", "").fillna("").astype(str).str.strip()
                if include_unmapped:
                    exp.loc[exp["GROUPNAME"].eq(""), "GROUPNAME"] = "(Needs mapping)"
                exp["AMOUNT"] = pd.to_numeric(exp.get("AMOUNT"), errors="coerce").fillna(0.0)
                exp_groups_all = exp[exp["GROUPNAME"].ne("")].copy()
                exp = exp_groups_all[exp_groups_all["AMOUNT"] != 0].copy()

                if exp_groups_all.empty or totals_prog.empty:
                    df_compare = pd.DataFrame(columns=["GROUPNAME", "BASELINE_COST", "EXPECTED_COST"])
                else:
                    exp_pg = exp.groupby(["PROGRAMNAME", "GROUPNAME"], as_index=False)["AMOUNT"].sum().rename(columns={"AMOUNT": "EXPECTED_COST"})
                    # Determine which app groups to display based on current scope (team/app-group filters).
                    allowed_groups: Optional[set[str]] = None
                    if sel_teams:
                        team_set = {str(t).strip().upper() for t in (sel_teams or []) if str(t).strip()}
                        if team_set:
                            m = exp_groups_all.copy()
                            m["TEAMNAME"] = m["TEAMNAME"].astype(str).str.strip()
                            m = m[m["TEAMNAME"].astype(str).str.upper().isin(team_set)]
                            allowed_groups = set(m["GROUPNAME"].dropna().astype(str).str.strip().tolist())
                    if sel_groups:
                        group_set = {str(g).strip() for g in (sel_groups or []) if str(g).strip()}
                        allowed_groups = (allowed_groups & group_set) if allowed_groups is not None else set(group_set)

                    # Allocate per program using per-program EXPECTED shares.
                    alloc_rows: list[dict] = []
                    for _, pr in totals_prog.iterrows():
                        prog = str(pr.get("PROGRAMNAME") or "").strip()
                        base_total = float(pr.get("BASELINE_TOTAL") or 0.0)
                        exp_total = float(pr.get("EXPECTED_TOTAL") or 0.0)

                        groups_p = sorted(
                            exp_groups_all.loc[exp_groups_all["PROGRAMNAME"] == prog, "GROUPNAME"]
                            .dropna()
                            .astype(str)
                            .str.strip()
                            .unique()
                            .tolist()
                        )
                        if not groups_p:
                            continue

                        pg = exp_pg[exp_pg["PROGRAMNAME"] == prog].copy()
                        pg["EXPECTED_COST"] = pd.to_numeric(pg.get("EXPECTED_COST"), errors="coerce").fillna(0.0)
                        pg = pg.merge(pd.DataFrame({"GROUPNAME": groups_p}), on="GROUPNAME", how="right").fillna({"EXPECTED_COST": 0.0})
                        exp_total_p = float(pg["EXPECTED_COST"].sum() or 0.0)

                        if exp_total_p > 0:
                            pg["SHARE"] = pg["EXPECTED_COST"] / exp_total_p
                        else:
                            pg["SHARE"] = 1.0 / float(len(groups_p))

                        for _, gr in pg.iterrows():
                            gname = str(gr.get("GROUPNAME") or "").strip()
                            if not gname:
                                continue
                            if allowed_groups is not None and gname not in allowed_groups:
                                continue
                            share = float(gr.get("SHARE") or 0.0)
                            alloc_rows.append(
                                {
                                    "GROUPNAME": gname,
                                    "BASELINE_COST": base_total * share,
                                    "EXPECTED_COST": exp_total * share,
                                }
                            )

                    if not alloc_rows:
                        df_compare = pd.DataFrame(columns=["GROUPNAME", "BASELINE_COST", "EXPECTED_COST"])
                    else:
                        df_compare = (
                            pd.DataFrame(alloc_rows)
                            .groupby("GROUPNAME", as_index=False)[["BASELINE_COST", "EXPECTED_COST"]]
                            .sum()
                        )
    else:
        df_base = _by_dim(baseline_lines, "BASELINE_COST")
        df_exp = _by_dim(expected_lines, "EXPECTED_COST")
        df_compare = df_base.merge(df_exp, on=dim, how="outer").fillna(0.0)

    if df_compare is None or df_compare.empty:
        if dim == "GROUPNAME":
            st.info("No EXPECTED app-level data available to allocate baseline vs expected for this scope.")
        else:
            st.info("No baseline/expected data available for this scope.")
    else:
        df_compare["BASELINE_COST"] = pd.to_numeric(df_compare.get("BASELINE_COST"), errors="coerce").fillna(0.0)
        df_compare["EXPECTED_COST"] = pd.to_numeric(df_compare.get("EXPECTED_COST"), errors="coerce").fillna(0.0)
        df_compare["DELTA"] = df_compare["EXPECTED_COST"] - df_compare["BASELINE_COST"]
        df_compare["DELTA_ABS"] = df_compare["DELTA"].abs()
        df_compare = df_compare.sort_values(["DELTA_ABS", "DELTA"], ascending=[False, False])

        labels = df_compare[dim].astype(str).tolist()
        baseline_vals = df_compare["BASELINE_COST"].round(0).astype(float).tolist()
        expected_vals = df_compare["EXPECTED_COST"].round(0).astype(float).tolist()
        data_zoom = None
        if len(labels) > 18:
            end_pct = max(10, int(round(100.0 * 18.0 / float(len(labels)))))
            data_zoom = [
                {"type": "slider", "xAxisIndex": 0, "start": 0, "end": end_pct},
                {"type": "inside", "xAxisIndex": 0},
            ]

        option = {
            "tooltip": {
                "trigger": "axis",
                "axisPointer": {"type": "shadow"},
                # streamlit-echarts only unwraps JsCode when the `function...` is on one line (no newlines).
                "formatter": (
                    JsCode(
                        "function(params){var b=(params[0]&&params[0].data)?params[0].data:0;"
                        "var r=(params[1]&&params[1].data)?params[1].data:0;var d=r-b;"
                        "return params[0].axisValue+'<br/>'+'Plan: $'+b.toLocaleString()+'<br/>'"
                        "+'Forecast: $'+r.toLocaleString()+'<br/>'+'Change: $'+d.toLocaleString();}"
                    )
                    if JsCode
                    else "function(params){var b=(params[0]&&params[0].data)?params[0].data:0;"
                    "var r=(params[1]&&params[1].data)?params[1].data:0;var d=r-b;"
                    "return params[0].axisValue+'<br/>'+'Plan: $'+b.toLocaleString()+'<br/>'"
                    "+'Forecast: $'+r.toLocaleString()+'<br/>'+'Change: $'+d.toLocaleString();}"
                ),
            },
            "legend": {"top": 26, "data": ["Plan", "Forecast"]},
            "grid": {"left": 80, "right": 30, "top": 70, "bottom": 110 if data_zoom else 80},
            "xAxis": {"type": "category", "data": labels, "axisLabel": {"rotate": 30, "interval": 0}},
            "yAxis": {"type": "value", "axisLabel": {"formatter": "${value}"}},
            "series": [
                {"name": "Plan", "type": "bar", "data": baseline_vals, "itemStyle": {"color": "#22c55e"}},
                {"name": "Forecast", "type": "bar", "data": expected_vals, "itemStyle": {"color": "#2563eb"}},
            ],
        }
        if data_zoom:
            option["dataZoom"] = data_zoom
        with perf_step("render_plan_vs_forecast_chart", page_key=page_perf_key):
            render_echart(option, height="500px", key=f"welcome_baseline_vs_expected_{chart_key_suffix}", page_theme=page_theme)

_set_loading("Rendering charts (2/3)...", 4)
with row1_right:
    _section_title("Top 5 costly apps", "Top 5 application groups by forecast cost for the selected scope.")
    st.caption(label_top_apps)
    if df_top_apps is None or df_top_apps.empty:
        st.info("No app-level cost data available for Top 5 apps in the selected scope.")
    else:
        items1: list[dict] = []
        top5 = df_top_apps.copy()
        top5["TOTAL_COST"] = pd.to_numeric(top5.get("TOTAL_COST"), errors="coerce").fillna(0.0)
        top5 = top5.sort_values("TOTAL_COST", ascending=False).head(5)
        for _, r in top5.iterrows():
            raw_name = r.get("GROUPNAME")
            display = str(raw_name).strip() if pd.notna(raw_name) and str(raw_name).strip() else "(Needs mapping)"
            value = float(r.get("TOTAL_COST") or 0.0)
            items1.append(
                {
                    "name": display,
                    "value": int(round(value, 0)),
                    "itemStyle": {"color": app_color_map.get(display, APP_COLORS[0])},
                }
            )
        if not items1:
            st.info("No app groups found in scope.")
        else:
            pie_colors = [item.get("itemStyle", {}).get("color", APP_COLORS[0]) for item in items1]
            option = build_pie_option("By Application", items1, pie_colors, page_theme)
            with perf_step("render_top5_apps_chart", page_key=page_perf_key):
                render_echart(option, height="432px", key=f"preview_welcome_top_apps_pie_{chart_key_suffix}", page_theme=page_theme)

_render_section_divider()
_set_loading("Rendering charts (3/3)...", 4)
_section_title("Application cost treemap", "Treemap shows cost allocation by app group; rectangle size = total cost.")
st.caption(label_treemap)
st.caption(
    "Treemap uses forecast costs for the selected scope (labor + non-labor + partners). "
    "Rectangle size = total cost for the app group. "
    "Click an app to see the breakdown by cost type and cost category."
)
if df_app_costs is None or df_app_costs.empty:
    chart_cols = st.columns([0.06, 0.88, 0.06])
    with chart_cols[1]:
        st.info("No app-level cost data available.")
else:
    _treemap_prep_meta = {}
    with perf_step("prepare_treemap_data", page_key=page_perf_key, meta=_treemap_prep_meta):
        df_treemap = df_app_costs.copy()
        df_treemap["GROUPNAME"] = df_treemap.get("GROUPNAME", "").fillna("").astype(str).str.strip()
        df_treemap["TOTAL_COST"] = pd.to_numeric(df_treemap.get("TOTAL_COST"), errors="coerce").fillna(0.0)
        df_treemap = df_treemap[df_treemap["TOTAL_COST"] != 0].copy()
        # Default behavior: mapped-only for Projected when include_unmapped is off.
        if not include_unmapped:
            df_treemap = df_treemap[df_treemap["GROUPNAME"].ne("")].copy()
        # When include_unmapped is on, show unmapped as a separate "(Needs mapping)" bucket (not "(Unassigned)").
        if include_unmapped:
            df_treemap.loc[df_treemap["GROUPNAME"].eq(""), "GROUPNAME"] = "(Needs mapping)"
        treemap_data = build_treemap_data(df_treemap, PaletteHelper(), app_color_map)
        _treemap_prep_meta["rows_treemap_df"] = int(df_treemap.shape[0]) if isinstance(df_treemap, pd.DataFrame) else 0
        _treemap_prep_meta["nodes_treemap"] = int(len(treemap_data or []))
    if treemap_data:
        treemap_option = {
            "title": {"text": "App Treemap", "left": "center", "top": 6, "textStyle": {"fontSize": 12}},
            "tooltip": {"formatter": "{b}<br/>Cost: ${c}"},
            "series": [
                {
                    "type": "treemap",
                    "data": treemap_data,
                    "leafDepth": 2,
                    "visibleMin": 200,
                    "colorMappingBy": "id",
                    "label": {
                        "show": True,
                        "position": "inside",
                        "formatter": "{b}",
                        "fontSize": 11,
                        "overflow": "break",
                    },
                    "upperLabel": {
                        "show": True,
                        "height": 22,
                        "fontSize": 12,
                        "color": "#fff",
                        "backgroundColor": "rgba(0,0,0,0.35)",
                    },
                    "breadcrumb": {"show": True, "left": "center", "bottom": 4, "height": 24},
                    "nodeClick": "zoomToNode",
                }
            ],
        }
        chart_cols = st.columns([0.06, 0.88, 0.06])
        with chart_cols[1]:
            with perf_step("render_treemap_chart", page_key=page_perf_key):
                render_echart(treemap_option, height="440px", key=f"preview_welcome_treemap_{chart_key_suffix}", page_theme=page_theme)
    else:
        chart_cols = st.columns([0.06, 0.88, 0.06])
        with chart_cols[1]:
            st.info("Not enough data to render the treemap.")

if is_debug_enabled(label="Debug"):
    with st.expander("Debug - treemap WF diagnostics", expanded=False):
        st.caption("Treemap inputs and WF visibility checks (debug only).")
        st.write("Scenario (app visuals):", scenario_for_app_visuals)
        st.write("Filters (cost):", filters_cost)

        if df_pi_app_breakdown is None or df_pi_app_breakdown.empty:
            st.info("No app breakdown rows from canonical costs.")
        else:
            diag = df_pi_app_breakdown.copy()
            diag["GROUPNAME"] = diag.get("GROUPNAME", "").fillna("").astype(str).str.strip()
            diag["COST_CATEGORY"] = diag.get("COST_CATEGORY", "").fillna("").astype(str).str.upper()
            diag["TOTAL_COST"] = pd.to_numeric(diag.get("TOTAL_COST"), errors="coerce").fillna(0.0)

            by_cat = (
                diag.groupby("COST_CATEGORY", dropna=False)["TOTAL_COST"]
                .sum()
                .reset_index()
                .sort_values("TOTAL_COST", ascending=False)
            )
            st.markdown("**App breakdown totals by cost category**")
            st.dataframe(by_cat, use_container_width=True, hide_index=True, height=180)

            wf_diag = diag.loc[diag["COST_CATEGORY"].eq("WORK_FORCE")].copy()
            st.write("WF rows in app breakdown:", int(len(wf_diag.index)))
            if not wf_diag.empty:
                wf_groups = (
                    wf_diag.groupby("GROUPNAME", dropna=False)["TOTAL_COST"]
                    .sum()
                    .reset_index()
                    .sort_values("TOTAL_COST", ascending=False)
                )
                st.dataframe(wf_groups.head(20), use_container_width=True, hide_index=True, height=240)

        if df_app_costs is not None and not df_app_costs.empty:
            tm = df_app_costs.copy()
            tm["COST_CATEGORY"] = tm.get("COST_CATEGORY", "").fillna("").astype(str).str.upper()
            tm["TOTAL_COST"] = pd.to_numeric(tm.get("TOTAL_COST"), errors="coerce").fillna(0.0)
            tm_sum = (
                tm.groupby("COST_CATEGORY", dropna=False)["TOTAL_COST"]
                .sum()
                .reset_index()
                .sort_values("TOTAL_COST", ascending=False)
            )
            st.markdown("**Treemap input totals by cost category**")
            st.dataframe(tm_sum, use_container_width=True, hide_index=True, height=160)

            tm_wf = tm.loc[tm["COST_CATEGORY"].eq("WORK_FORCE")].copy()
            st.write("WF rows in treemap input:", int(len(tm_wf.index)))
            if not tm_wf.empty:
                tm_groups = (
                    tm_wf.groupby("GROUPNAME", dropna=False)["TOTAL_COST"]
                    .sum()
                    .reset_index()
                    .sort_values("TOTAL_COST", ascending=False)
                )
                st.dataframe(tm_groups.head(20), use_container_width=True, hide_index=True, height=240)


_render_section_divider()


def _safe_ratio(num: float, den: float) -> Optional[float]:
    try:
        n = float(num)
        d = float(den)
    except Exception:
        return None
    if d <= 0:
        return None
    return n / d


def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df.get(col), errors="coerce").fillna(0.0).sum())


def _pick_first_numeric_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    for c in candidates:
        if c not in df.columns:
            continue
        s = pd.to_numeric(df.get(c), errors="coerce")
        if s.notna().any():
            return c
    return None


def _by_bucket_summary(df_baseline: pd.DataFrame, df_expected: pd.DataFrame) -> pd.DataFrame:
    buckets = ["WF", "NWF"]
    rows = []
    for b in buckets:
        base_amt = _sum_amount(df_baseline, cost_bucket=b)
        exp_amt = _sum_amount(df_expected, cost_bucket=b)
        delta = exp_amt - base_amt
        ratio = _safe_ratio(exp_amt, base_amt)
        rows.append(
            {
                "BUCKET": b,
                "BASELINE_AMOUNT": round(base_amt, 2),
                "EXPECTED_AMOUNT": round(exp_amt, 2),
                "DELTA": round(delta, 2),
                "RATIO": round(ratio, 4) if ratio is not None else pd.NA,
            }
        )
    return pd.DataFrame(rows, columns=["BUCKET", "BASELINE_AMOUNT", "EXPECTED_AMOUNT", "DELTA", "RATIO"])


if diagnostics_mode:
    _render_section_divider()
    st.subheader("Debug - diagnostics")
    st.caption("Visible only in debug mode. Helps explain Baseline vs Expected deviations.")

    df_base_scope = cost_model_scope.get("BASELINE", pd.DataFrame())
    df_exp_scope = cost_model_scope.get("EXPECTED", pd.DataFrame())

    baseline_total_diag = _sum_amount(df_base_scope)
    expected_total_diag = _sum_amount(df_exp_scope)
    delta_total_diag = expected_total_diag - baseline_total_diag
    ratio_total_diag = _safe_ratio(expected_total_diag, baseline_total_diag)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Baseline total", f"${baseline_total_diag:,.0f}")
    k2.metric("Expected total", f"${expected_total_diag:,.0f}")
    k3.metric("Delta", f"${delta_total_diag:,.0f}")
    k4.metric("Ratio", f"{(ratio_total_diag * 100):,.1f}%" if ratio_total_diag is not None else "—")

    st.markdown("**Baseline vs Expected by bucket**")
    st.dataframe(_by_bucket_summary(df_base_scope, df_exp_scope), use_container_width=True, hide_index=True, height=140)

    st.markdown("**Mapped vs Unassigned**")
    diag_base = get_unassigned_breakdown(df_base_scope, scenario="Baseline", filters=filters_cost)
    diag_exp = get_unassigned_breakdown(df_exp_scope, scenario="Projected", filters=filters_cost)
    diag_base = diag_base if diag_base is not None else pd.DataFrame()
    diag_exp = diag_exp if diag_exp is not None else pd.DataFrame()

    def _unassigned_amount(diag_df: pd.DataFrame) -> float:
        if diag_df is None or diag_df.empty:
            return 0.0
        issue = diag_df.get("ISSUE", pd.Series(dtype=str)).fillna("").astype(str)
        amt = pd.to_numeric(diag_df.get("TOTAL_COST"), errors="coerce").fillna(0.0)
        return float(amt.loc[issue.eq("Missing APP_GROUP mapping")].sum())

    exp_unassigned = _unassigned_amount(diag_exp)
    base_unassigned = _unassigned_amount(diag_base)
    exp_unassigned_pct = _safe_ratio(exp_unassigned, expected_total_diag)
    base_unassigned_pct = _safe_ratio(base_unassigned, baseline_total_diag)

    u1, u2 = st.columns(2)
    u1.metric(
        "Expected unassigned",
        f"${exp_unassigned:,.0f}",
        delta=f"{(exp_unassigned_pct*100):,.1f}%" if exp_unassigned_pct is not None else None,
    )
    u2.metric(
        "Baseline unassigned",
        f"${base_unassigned:,.0f}",
        delta=f"{(base_unassigned_pct*100):,.1f}%" if base_unassigned_pct is not None else None,
    )

    left, right = st.columns(2, gap="large")
    with left:
        st.caption("Top 10 issues (Expected)")
        if diag_exp is None or diag_exp.empty:
            st.info("No issues detected.")
        else:
            top = diag_exp.sort_values("TOTAL_COST", ascending=False).head(10).copy()
            st.dataframe(top, use_container_width=True, hide_index=True, height=240)
    with right:
        st.caption("Top 10 issues (Baseline)")
        if diag_base is None or diag_base.empty:
            st.info("No issues detected.")
        else:
            top = diag_base.sort_values("TOTAL_COST", ascending=False).head(10).copy()
            st.dataframe(top, use_container_width=True, hide_index=True, height=240)

    st.markdown("**SWAG readiness (quick)**")
    expl = df_explorer.copy() if df_explorer is not None and not df_explorer.empty else pd.DataFrame()
    total_feat = int(len(expl.index)) if expl is not None and not expl.empty else 0
    ready_feat = int(expl.get("SWAG_READY", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if total_feat else 0
    not_ready_feat = int(total_feat - ready_feat)
    r1, r2, r3 = st.columns(3)
    r1.metric("Features in scope", f"{total_feat:,d}")
    r2.metric("SWAG-ready", f"{ready_feat:,d}")
    r3.metric("SWAG not ready", f"{not_ready_feat:,d}")

    st.markdown("**Workforce diagnostics (Expected)**")
    exp_wf = df_exp_scope.copy() if df_exp_scope is not None and not df_exp_scope.empty else pd.DataFrame()
    if not exp_wf.empty and "COST_BUCKET" in exp_wf.columns:
        exp_wf = exp_wf.loc[exp_wf["COST_BUCKET"].fillna("").astype(str).str.upper().eq("WF")].copy()

    exp_fte_col = _pick_first_numeric_col(exp_wf, ["FTE", "DERIVED_FTE_SWAG"])
    exp_wf_amount = _safe_sum(exp_wf, "AMOUNT")
    exp_wf_fte = _safe_sum(exp_wf, exp_fte_col) if exp_fte_col else 0.0
    pi_count = 1
    if exp_wf is not None and not exp_wf.empty and "PI" in exp_wf.columns:
        pi_count = int(pd.to_numeric(exp_wf.get("PI"), errors="coerce").dropna().nunique())
    cost_per_fte_per_pi_expected = _safe_ratio(exp_wf_amount, exp_wf_fte)
    annualized_cost_per_fte_expected = (
        (float(cost_per_fte_per_pi_expected) * float(pi_count))
        if (cost_per_fte_per_pi_expected is not None and pi_count > 0)
        else None
    )

    count_wf_rows = int(len(exp_wf.index)) if exp_wf is not None and not exp_wf.empty else 0
    if exp_fte_col and count_wf_rows:
        fte_s = pd.to_numeric(exp_wf.get(exp_fte_col), errors="coerce")
        missing_fte_rows = int((fte_s.isna() | (fte_s <= 0)).sum())
    else:
        missing_fte_rows = count_wf_rows
    pct_missing_fte = _safe_ratio(float(missing_fte_rows), float(count_wf_rows))

    w_cols1 = st.columns(5)
    w_cols1[0].metric("Expected WF amount", f"${exp_wf_amount:,.0f}")
    w_cols1[1].metric(f"Expected WF FTE ({exp_fte_col or '—'})", f"{exp_wf_fte:,.2f}" if exp_wf_fte else "—")
    w_cols1[2].metric("PI count in scope", f"{pi_count:,d}" if isinstance(pi_count, int) else "—")
    w_cols1[3].metric(
        "Implied cost per FTE (per PI)",
        f"${cost_per_fte_per_pi_expected:,.0f}" if cost_per_fte_per_pi_expected is not None else "—",
    )
    w_cols1[4].metric(
        "Annualized cost per FTE",
        f"${annualized_cost_per_fte_expected:,.0f}" if annualized_cost_per_fte_expected is not None else "—",
    )

    w_cols2 = st.columns(3)
    w_cols2[0].metric("WF rows", f"{count_wf_rows:,d}")
    w_cols2[1].metric("Missing FTE rows", f"{missing_fte_rows:,d}")
    w_cols2[2].metric("Missing FTE %", f"{(pct_missing_fte*100):,.1f}%" if pct_missing_fte is not None else "—")

    st.markdown("**Internal WF composition — Baseline vs Expected (per PI)**")
    base_lines = get_cost_lines(df_base_scope, scenario="Baseline", filters=filters_cost)
    exp_lines = get_cost_lines(df_exp_scope, scenario="Projected", filters=filters_cost)

    bucket_order = ["Team Overhead", "Delivery", "Contractor C", "Contractor CS", "Program"]
    scen_order = ["BASELINE", "EXPECTED"]

    comp_frames: list[pd.DataFrame] = []
    if base_lines is not None and not base_lines.empty:
        w = base_lines.copy()
        w["SCENARIO"] = "BASELINE"
        comp_frames.append(w)
    if exp_lines is not None and not exp_lines.empty:
        w = exp_lines.copy()
        w["SCENARIO"] = "EXPECTED"
        comp_frames.append(w)

    pi_count_scope = 1
    if comp_frames:
        pi_parts = []
        for d in comp_frames:
            if d is None or d.empty or "PI" not in d.columns:
                continue
            pi_col = d[["PI"]].copy()
            if pd.to_numeric(pi_col["PI"], errors="coerce").dropna().empty:
                continue
            pi_parts.append(pi_col)
        pi_all = pd.concat(pi_parts, ignore_index=True, sort=False) if pi_parts else pd.DataFrame(columns=["PI"])
        if pi_all is not None and not pi_all.empty:
            pi_count_scope = int(pd.to_numeric(pi_all.get("PI"), errors="coerce").dropna().nunique()) or 1

    # Build the full scenario×bucket grid so missing buckets still render as zeros.
    grid_rows = [{"SCENARIO": s, "COMPOSITION_BUCKET": b, "PI_COUNT": pi_count_scope} for s in scen_order for b in bucket_order]
    grid = pd.DataFrame(grid_rows, columns=["SCENARIO", "COMPOSITION_BUCKET", "PI_COUNT"])

    if not comp_frames:
        out = grid.copy()
        out["AMOUNT"] = 0.0
        out["HC_OR_FTE"] = 0.0
        out["COST_PER_UNIT_PER_PI"] = pd.NA
        st.dataframe(out, use_container_width=True, hide_index=True, height=240)
    else:
        comp_parts = [_drop_all_na_columns(d) for d in comp_frames if d is not None and not d.empty]
        comp = pd.concat(comp_parts, ignore_index=True, sort=False) if comp_parts else pd.DataFrame()
        # WF-only
        if "COST_BUCKET" in comp.columns:
            comp = comp.loc[comp["COST_BUCKET"].fillna("").astype(str).str.upper().eq("WF")].copy()
        elif "COST_CATEGORY" in comp.columns:
            comp = comp.loc[comp["COST_CATEGORY"].fillna("").astype(str).str.upper().eq("WORK_FORCE")].copy()

        sub = comp.get("SUBCOMPONENT", "").fillna("").astype(str)
        sub_u = sub.str.upper()
        comp["COMPOSITION_BUCKET"] = pd.NA
        comp.loc[sub_u.str.contains("CONTRACTOR CS", na=False), "COMPOSITION_BUCKET"] = "Contractor CS"
        comp.loc[(comp["COMPOSITION_BUCKET"].isna()) & sub_u.str.contains("CONTRACTOR C", na=False), "COMPOSITION_BUCKET"] = "Contractor C"
        comp.loc[(comp["COMPOSITION_BUCKET"].isna()) & sub_u.str.contains("PROGRAM", na=False), "COMPOSITION_BUCKET"] = "Program"
        comp.loc[(comp["COMPOSITION_BUCKET"].isna()) & sub_u.str.contains("DELIVERY", na=False), "COMPOSITION_BUCKET"] = "Delivery"
        comp.loc[(comp["COMPOSITION_BUCKET"].isna()) & sub_u.str.contains("TEAM", na=False), "COMPOSITION_BUCKET"] = "Team Overhead"
        comp = comp[comp["COMPOSITION_BUCKET"].isin(bucket_order)].copy()

        comp["AMOUNT"] = pd.to_numeric(comp.get("AMOUNT"), errors="coerce").fillna(0.0)
        unit_col = _pick_first_numeric_col(comp, ["FTE", "HEADCOUNT"])
        comp["_UNIT"] = pd.to_numeric(comp.get(unit_col), errors="coerce") if unit_col else pd.Series([0.0] * len(comp.index))
        comp["_UNIT"] = pd.to_numeric(comp.get("_UNIT"), errors="coerce").fillna(0.0)

        g = (
            comp.groupby(["SCENARIO", "COMPOSITION_BUCKET"], dropna=False)
            .agg(AMOUNT=("AMOUNT", "sum"), HC_OR_FTE=("_UNIT", "sum"), ROWS=("AMOUNT", "size"))
            .reset_index()
        )
        g["PI_COUNT"] = pi_count_scope
        g["COST_PER_UNIT_PER_PI"] = g.apply(lambda r: _safe_ratio(r.get("AMOUNT"), r.get("HC_OR_FTE")), axis=1)

        out = grid.merge(g, on=["SCENARIO", "COMPOSITION_BUCKET", "PI_COUNT"], how="left")
        out["AMOUNT"] = pd.to_numeric(out.get("AMOUNT"), errors="coerce").fillna(0.0)
        out["HC_OR_FTE"] = pd.to_numeric(out.get("HC_OR_FTE"), errors="coerce").fillna(0.0)
        out["ROWS"] = pd.to_numeric(out.get("ROWS"), errors="coerce").fillna(0).astype(int)
        out["COST_PER_UNIT_PER_PI"] = pd.to_numeric(out.get("COST_PER_UNIT_PER_PI"), errors="coerce")
        out.loc[out["ROWS"].le(0) | out["HC_OR_FTE"].le(0), "COST_PER_UNIT_PER_PI"] = pd.NA

        scen_dtype = pd.CategoricalDtype(categories=scen_order, ordered=True)
        bucket_dtype = pd.CategoricalDtype(categories=bucket_order, ordered=True)
        out["SCENARIO"] = out["SCENARIO"].astype(scen_dtype)
        out["COMPOSITION_BUCKET"] = out["COMPOSITION_BUCKET"].astype(bucket_dtype)
        out = out.sort_values(["SCENARIO", "COMPOSITION_BUCKET"]).copy()

        st.dataframe(
            out[["SCENARIO", "COMPOSITION_BUCKET", "AMOUNT", "HC_OR_FTE", "PI_COUNT", "COST_PER_UNIT_PER_PI"]],
            use_container_width=True,
            hide_index=True,
            height=240,
        )

    st.markdown("**Expected WF drivers by team (top 20)**")
    if exp_wf is None or exp_wf.empty:
        st.info("No Expected WF rows in scope.")
    else:
        team_col = "TEAMNAME" if "TEAMNAME" in exp_wf.columns else None
        w = exp_wf.copy()
        if team_col:
            w["TEAMNAME"] = w.get("TEAMNAME", "").fillna("").astype(str).str.strip()
            w.loc[w["TEAMNAME"].eq(""), "TEAMNAME"] = "(Missing team)"
        else:
            w["TEAMNAME"] = "(Team not available)"

        w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)
        if exp_fte_col:
            w["_FTE_USED"] = pd.to_numeric(w.get(exp_fte_col), errors="coerce")
        else:
            w["_FTE_USED"] = pd.NA

        agg_cols = {"AMOUNT": "sum", "_FTE_USED": "sum"}
        g = w.groupby("TEAMNAME", dropna=False).agg(agg_cols).reset_index()
        g = g.rename(columns={"AMOUNT": "AMOUNT_SUM", "_FTE_USED": "FTE_SUM"})
        g["IMPLIED_RATE"] = g.apply(lambda r: _safe_ratio(r.get("AMOUNT_SUM") or 0.0, r.get("FTE_SUM") or 0.0), axis=1)
        g["AMOUNT_SUM"] = pd.to_numeric(g.get("AMOUNT_SUM"), errors="coerce").fillna(0.0)
        g = g.sort_values("AMOUNT_SUM", ascending=False).head(20).copy()
        show_cols = ["TEAMNAME"]
        show_cols += ["FTE_SUM", "AMOUNT_SUM", "IMPLIED_RATE"]
        st.dataframe(g[show_cols], use_container_width=True, hide_index=True, height=320)

    st.markdown("**Expected WF cost per FTE flags (top 50)**")
    if exp_wf is None or exp_wf.empty:
        st.info("No Expected WF rows in scope.")
    else:
        rw = exp_wf.copy()
        for c in ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT"]:
            if c not in rw.columns:
                rw[c] = ""
            rw[c] = rw[c].fillna("").astype(str).str.strip()
        rw["AMOUNT"] = pd.to_numeric(rw.get("AMOUNT"), errors="coerce").fillna(0.0)
        if exp_fte_col:
            rw["FTE"] = pd.to_numeric(rw.get(exp_fte_col), errors="coerce")
        elif "FTE" in rw.columns:
            rw["FTE"] = pd.to_numeric(rw.get("FTE"), errors="coerce")
        else:
            rw["FTE"] = pd.NA
        rw["FTE"] = pd.to_numeric(rw.get("FTE"), errors="coerce")

        rw["COST_PER_FTE_PER_PI"] = rw.apply(lambda r: _safe_ratio(r.get("AMOUNT"), r.get("FTE")), axis=1)
        rw["COST_PER_FTE_PER_PI"] = pd.to_numeric(rw.get("COST_PER_FTE_PER_PI"), errors="coerce")
        rw["PI_COUNT_IN_SCOPE"] = int(pi_count) if isinstance(pi_count, int) else 1
        rw["ANNUALIZED_COST_PER_FTE"] = pd.to_numeric(rw.get("COST_PER_FTE_PER_PI"), errors="coerce") * float(
            rw["PI_COUNT_IN_SCOPE"].iloc[0] if len(rw.index) else 1
        )

        def _flag_row(r) -> str:
            fte = r.get("FTE")
            amt = r.get("AMOUNT")
            annual_cost = r.get("ANNUALIZED_COST_PER_FTE")
            if fte is None or (isinstance(fte, float) and pd.isna(fte)) or float(fte) <= 0:
                return "MISSING_FTE"
            if amt is None or (isinstance(amt, float) and pd.isna(amt)) or float(amt) == 0:
                return "ZERO_AMOUNT"
            if annual_cost is None or (isinstance(annual_cost, float) and pd.isna(annual_cost)):
                return ""
            if float(annual_cost) < 50000:
                return "LOW_ANNUAL_COST"
            if float(annual_cost) > 400000:
                return "HIGH_ANNUAL_COST"
            return ""

        rw["FLAG"] = rw.apply(_flag_row, axis=1)
        flagged = rw[rw["FLAG"].ne("")].copy()
        if flagged.empty:
            st.info("No flagged rows.")
        else:
            cols = ["PROGRAMNAME", "TEAMNAME", "GROUPNAME", "COST_CATEGORY", "SUBCOMPONENT", "AMOUNT", "FTE"]
            cols += ["COST_PER_FTE_PER_PI", "PI_COUNT_IN_SCOPE", "ANNUALIZED_COST_PER_FTE", "FLAG"]
            st.dataframe(flagged[cols].head(50), use_container_width=True, hide_index=True, height=360)

    st.markdown("**Workforce diagnostics (Baseline)**")
    base_wf = df_base_scope.copy() if df_base_scope is not None and not df_base_scope.empty else pd.DataFrame()
    if not base_wf.empty and "COST_BUCKET" in base_wf.columns:
        base_wf = base_wf.loc[base_wf["COST_BUCKET"].fillna("").astype(str).str.upper().eq("WF")].copy()
    base_fte_col = _pick_first_numeric_col(base_wf, ["FTE", "HEADCOUNT"])
    baseline_wf_amount = _safe_sum(base_wf, "AMOUNT")
    baseline_wf_fte = _safe_sum(base_wf, base_fte_col) if base_fte_col else 0.0
    baseline_cost_per_fte = _safe_ratio(baseline_wf_amount, baseline_wf_fte)

    b1, b2, b3 = st.columns(3)
    b1.metric("Baseline WF amount", f"${baseline_wf_amount:,.0f}")
    b2.metric(f"Baseline WF FTE ({base_fte_col or '—'})", f"{baseline_wf_fte:,.2f}" if baseline_wf_fte else "—")
    b3.metric("Baseline cost per FTE", f"${baseline_cost_per_fte:,.0f}" if baseline_cost_per_fte is not None else "—")

_set_loading("Finalizing view...", 5)
global_loading_ph.empty()
with perf_step("render_tail_sections", page_key=page_perf_key):
    render_disclaimer()
show_perf_panel(page_perf_key)
