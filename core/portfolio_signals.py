from __future__ import annotations

from typing import Any, Optional

import pandas as pd


SEVERITY_ORDER = {"risk": 0, "watch": 1, "info": 2}


def _to_float(val: Any) -> Optional[float]:
    try:
        out = float(val)
        if pd.isna(out):
            return None
        return out
    except Exception:
        return None


def _clean_text(val: Any) -> str:
    return str(val or "").strip()


def _normalize_source(source: Any) -> str:
    s = str(source or "").strip().lower()
    if s == "financial":
        return "Financial"
    return "Delivery"


def _first_team_col(df: pd.DataFrame) -> Optional[str]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    for cand in ("TEAMNAME", "team_name", "team_key"):
        if cand in df.columns:
            return cand
    return None


def _filter_by_teams(df: pd.DataFrame, teams: list[str]) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty or not teams:
        return df
    team_col = _first_team_col(df)
    if not team_col:
        return df
    teams_set = {str(t).strip().upper() for t in teams if str(t).strip()}
    if not teams_set:
        return df
    work = df.copy()
    work[team_col] = work.get(team_col, "").fillna("").astype(str).str.strip()
    return work[work[team_col].str.upper().isin(teams_set)].copy()


def make_signal(
    *,
    id: str,
    level: str,
    severity: str,
    icon: str,
    title: str,
    text: str,
    metric_value: Optional[float] = None,
    metric_unit: Optional[str] = None,
    impact: Optional[float] = None,
    action: Optional[str] = None,
    source: str = "Delivery",
) -> dict[str, Any]:
    sev = str(severity or "info").strip().lower()
    if sev not in {"info", "watch", "risk"}:
        sev = "info"
    lvl = str(level or "Program").strip().title()
    if lvl not in {"Portfolio", "Program", "Team", "Application"}:
        lvl = "Program"
    return {
        "id": _clean_text(id),
        "level": lvl,
        "severity": sev,
        "icon": _clean_text(icon) or "•",
        "title": _clean_text(title),
        "text": _clean_text(text),
        "metric_value": _to_float(metric_value),
        "metric_unit": _clean_text(metric_unit) or None,
        "impact": _to_float(impact),
        "action": _clean_text(action) or None,
        "source": _normalize_source(source),
    }


def _delivery_level(
    cm_expected: pd.DataFrame,
    cap_df: pd.DataFrame,
    df_explorer: pd.DataFrame,
    *,
    eff_groups: list[str],
    eff_teams: list[str],
) -> tuple[str, str]:
    has_groups = bool(eff_groups)
    has_group_cost = isinstance(cm_expected, pd.DataFrame) and not cm_expected.empty and "GROUPNAME" in cm_expected.columns
    has_group_demand = isinstance(df_explorer, pd.DataFrame) and not df_explorer.empty and "GROUPNAME" in df_explorer.columns
    if has_groups and has_group_cost and has_group_demand:
        return "Application", "GROUPNAME"

    has_team_filter = bool(eff_teams)
    team_col_cost = _first_team_col(cm_expected)
    team_col_cap = _first_team_col(cap_df)
    team_col_explorer = _first_team_col(df_explorer)
    if has_team_filter and (team_col_cost or team_col_cap or team_col_explorer):
        return "Team", str(team_col_cost or team_col_cap or team_col_explorer)

    return "Program", "PROGRAMNAME"


def build_financial_signals(
    apptio_df: pd.DataFrame,
    *,
    programs_used: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    meta: dict[str, Any] = {
        "programs_used": list(programs_used or []),
        "financial_available": False,
        "rows": int(apptio_df.shape[0]) if isinstance(apptio_df, pd.DataFrame) else 0,
        "reason": "",
    }
    if not programs_used:
        meta["reason"] = "no_programs"
        return [], meta
    if not isinstance(apptio_df, pd.DataFrame) or apptio_df.empty:
        meta["reason"] = "no_data"
        return [], meta
    if "AMOUNT" not in apptio_df.columns:
        meta["reason"] = "missing_amount"
        return [], meta

    nwf_type_col = "EFFECTIVE_NWF_TYPE" if "EFFECTIVE_NWF_TYPE" in apptio_df.columns else None
    if nwf_type_col is None:
        meta["reason"] = "missing_nwf_type"
        return [], meta

    out: list[dict[str, Any]] = []
    amt = pd.to_numeric(apptio_df.get("AMOUNT"), errors="coerce").fillna(0.0)
    types = apptio_df[nwf_type_col].fillna("").astype(str)

    total = float(amt.sum() or 0.0)
    invoices = float(amt.loc[types.str.contains("invoice", case=False, na=False)].sum() or 0.0)
    if total > 0:
        share = invoices / total * 100.0
        severity = "risk" if share >= 65.0 else ("watch" if share >= 45.0 else "info")
        icon = "⚠" if severity == "risk" else ("↑" if severity == "watch" else "↔")
        action = "Investigate" if severity == "risk" else ("Watch" if severity == "watch" else "No action")
        out.append(
            make_signal(
                id="nwf_invoices_share",
                level="Program",
                severity=severity,
                icon=icon,
                title="Invoice share",
                text=f"Invoices represent {share:.0f}% of Non-Workforce spend (Actual)",
                metric_value=share,
                metric_unit="%",
                impact=invoices,
                action=action,
                source="Financial",
            )
        )

    if "MONTH_KEY" in apptio_df.columns:
        w = apptio_df.copy()
        w["MONTH_KEY"] = w["MONTH_KEY"].fillna("").astype(str).str.strip()
        w = w[w["MONTH_KEY"].str.match(r"^\d{4}-\d{2}$", na=False)]
        if not w.empty:
            w["YEAR"] = w["MONTH_KEY"].str.slice(0, 4).astype(int)
            w["MONTH"] = w["MONTH_KEY"].str.slice(5, 7).astype(int)
            w["QUARTER"] = ((w["MONTH"] - 1) // 3 + 1).astype(int)
            w["Q_LABEL"] = w["YEAR"].astype(str) + " Q" + w["QUARTER"].astype(str)
            w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0)
            w = w[w[nwf_type_col].fillna("").astype(str).str.contains("azure", case=False, na=False)]
            q = w.groupby(["YEAR", "QUARTER", "Q_LABEL"], as_index=False)["AMOUNT"].sum().sort_values(["YEAR", "QUARTER"])
            if len(q.index) >= 2:
                latest = q.iloc[-1]
                prev = q.iloc[-2]
                prev_amt = float(prev.get("AMOUNT") or 0.0)
                latest_amt = float(latest.get("AMOUNT") or 0.0)
                if prev_amt > 0:
                    qoq = (latest_amt - prev_amt) / prev_amt * 100.0
                    if qoq >= 10:
                        severity = "risk"
                        icon = "⚠"
                        action = "Investigate"
                        text = f"Azure spend increased {qoq:.0f}% QoQ (Actual)"
                    elif qoq >= 5:
                        severity = "watch"
                        icon = "↑"
                        action = "Watch"
                        text = f"Azure spend increased {qoq:.0f}% QoQ (Actual)"
                    elif qoq <= -10:
                        severity = "watch"
                        icon = "↓"
                        action = "Reassess"
                        text = f"Azure spend decreased {abs(qoq):.0f}% QoQ (Actual)"
                    else:
                        severity = "info"
                        icon = "↔"
                        action = "No action"
                        text = "Azure spend stable QoQ (Actual)"
                    out.append(
                        make_signal(
                            id="azure_qoq_actual",
                            level="Program",
                            severity=severity,
                            icon=icon,
                            title="Azure QoQ",
                            text=text,
                            metric_value=qoq,
                            metric_unit="%",
                            impact=latest_amt - prev_amt,
                            action=action,
                            source="Financial",
                        )
                    )

    meta["financial_available"] = bool(out)
    meta["reason"] = "ok" if out else "no_financial_metrics"
    return out, meta


def build_delivery_signals(
    cm: dict[str, pd.DataFrame],
    cap_df: pd.DataFrame,
    df_explorer: pd.DataFrame,
    *,
    eff_programs: list[str],
    eff_teams: list[str],
    eff_groups: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _ = eff_programs
    df_expected = cm.get("EXPECTED", pd.DataFrame()) if isinstance(cm, dict) else pd.DataFrame()
    df_expected = _filter_by_teams(df_expected, eff_teams)
    cap_df = _filter_by_teams(cap_df, eff_teams)
    df_explorer = _filter_by_teams(df_explorer, eff_teams)
    out: list[dict[str, Any]] = []
    debug: dict[str, Any] = {
        "cm_expected_rows": 0,
        "cap_rows": int(cap_df.shape[0]) if isinstance(cap_df, pd.DataFrame) else 0,
        "explorer_rows": int(df_explorer.shape[0]) if isinstance(df_explorer, pd.DataFrame) else 0,
    }

    if isinstance(df_expected, pd.DataFrame):
        debug["cm_expected_rows"] = int(df_expected.shape[0])

    level, entity_col = _delivery_level(
        df_expected if isinstance(df_expected, pd.DataFrame) else pd.DataFrame(),
        cap_df if isinstance(cap_df, pd.DataFrame) else pd.DataFrame(),
        df_explorer if isinstance(df_explorer, pd.DataFrame) else pd.DataFrame(),
        eff_groups=list(eff_groups or []),
        eff_teams=list(eff_teams or []),
    )
    debug["delivery_level"] = level
    debug["entity_col"] = entity_col
    team_scope_requested = bool(eff_teams)
    team_breakdown_unavailable = bool(team_scope_requested and level != "Team")
    if team_breakdown_unavailable:
        debug["team_breakdown_unavailable"] = True

    def _with_team_suffix(text: str) -> str:
        if not team_breakdown_unavailable:
            return text
        suffix = "(Program-level; team breakdown unavailable)"
        return f"{text} {suffix}"

    if isinstance(df_expected, pd.DataFrame) and not df_expected.empty:
        cost_work = df_expected.copy()
        cost_entity_col = entity_col
        cost_level = level
        if cost_entity_col not in cost_work.columns and "PROGRAMNAME" in cost_work.columns:
            cost_entity_col = "PROGRAMNAME"
            cost_level = "Program"
        if cost_entity_col in cost_work.columns:
            cost_work[cost_entity_col] = cost_work[cost_entity_col].fillna("").astype(str).str.strip()
            cost_work = cost_work[cost_work[cost_entity_col] != ""].copy()
            cost_work["AMOUNT"] = pd.to_numeric(cost_work.get("AMOUNT"), errors="coerce").fillna(0.0)
            if "WF_LAYER2" in cost_work.columns:
                cost_work = cost_work[cost_work["WF_LAYER2"].fillna("").astype(str).str.upper().str.strip().eq("SOD")]
            elif "SOD_LAYER2" in cost_work.columns:
                cost_work = cost_work[cost_work["SOD_LAYER2"].fillna("").astype(str).str.upper().str.strip().eq("SOD")]
            cost_by_entity = cost_work.groupby(cost_entity_col, dropna=False)["AMOUNT"].sum()

            demand_by_entity = pd.Series(dtype=float)
            if cost_level == "Application" and isinstance(df_explorer, pd.DataFrame) and not df_explorer.empty and "GROUPNAME" in df_explorer.columns:
                exp = df_explorer.copy()
                exp["GROUPNAME"] = exp["GROUPNAME"].fillna("").astype(str).str.strip()
                demand_col = "DERIVED_FTE_EXPLORER" if "DERIVED_FTE_EXPLORER" in exp.columns else "DERIVED_FTE"
                if demand_col in exp.columns:
                    exp[demand_col] = pd.to_numeric(exp.get(demand_col), errors="coerce").fillna(0.0)
                    demand_by_entity = exp.groupby("GROUPNAME", dropna=False)[demand_col].sum()
            elif isinstance(cap_df, pd.DataFrame) and not cap_df.empty and cost_entity_col in cap_df.columns:
                delivered_col = None
                for cand in ["DELIVERED_FTE", "DEMAND_SOD_FTE", "DEMAND_FTE"]:
                    if cand in cap_df.columns:
                        delivered_col = cand
                        break
                if delivered_col:
                    capw = cap_df.copy()
                    capw[cost_entity_col] = capw[cost_entity_col].fillna("").astype(str).str.strip()
                    capw[delivered_col] = pd.to_numeric(capw.get(delivered_col), errors="coerce").fillna(0.0)
                    demand_by_entity = capw.groupby(cost_entity_col, dropna=False)[delivered_col].sum()

            if not cost_by_entity.empty and not demand_by_entity.empty:
                ratios = (cost_by_entity / demand_by_entity).dropna()
                ratios = ratios[ratios > 0]
                if not ratios.empty:
                    ranked = ratios.sort_values(ascending=False)
                    top_entity = str(ranked.index[0]).strip()
                    top_ratio = float(ranked.iloc[0])
                    median_ratio = float(ranked.median()) if len(ranked.index) > 1 else top_ratio
                    severity = "risk" if top_ratio >= (median_ratio * 1.25) else "watch"
                    icon = "⚠" if severity == "risk" else "↑"
                    action = "Investigate" if severity == "risk" else "Watch"
                    top_cost = float(cost_by_entity.get(top_entity, 0.0) or 0.0)
                    out.append(
                        make_signal(
                            id="highest_cost_per_delivered_fte",
                            level=cost_level,
                            severity=severity,
                            icon=icon,
                            title="Cost per delivered FTE",
                            text=_with_team_suffix(f"{top_entity} has the highest cost per delivered FTE (${top_ratio:,.0f})"),
                            metric_value=top_ratio,
                            metric_unit="$/FTE",
                            impact=top_cost if top_cost > 0 else None,
                            action=action,
                            source="Delivery",
                        )
                    )

    if isinstance(cap_df, pd.DataFrame) and not cap_df.empty:
        cap_col = "CAPACITY_SOD_FTE" if "CAPACITY_SOD_FTE" in cap_df.columns else "CAPACITY_FTE"
        dem_col = "DEMAND_SOD_FTE" if "DEMAND_SOD_FTE" in cap_df.columns else "DEMAND_FTE"
        label_col = "ITERATION_LEVEL3" if "ITERATION_LEVEL3" in cap_df.columns else "PI_LABEL"
        if cap_col in cap_df.columns and dem_col in cap_df.columns and label_col in cap_df.columns:
            agg = cap_df.groupby(label_col, dropna=False)[[cap_col, dem_col]].sum().reset_index()
            if "PI_ORDER" in cap_df.columns:
                pi_order = cap_df.groupby(label_col, dropna=False)["PI_ORDER"].min().reset_index()
                agg = agg.merge(pi_order, on=label_col, how="left").sort_values("PI_ORDER")
            else:
                agg = agg.sort_values(label_col)
            if not agg.empty:
                last = agg.iloc[-1]
                capacity = float(last.get(cap_col) or 0.0)
                demand = float(last.get(dem_col) or 0.0)
                pi_label = _clean_text(last.get(label_col))
                if capacity > 0:
                    pressure = (demand - capacity) / capacity * 100.0
                    if pressure >= 10:
                        severity = "risk"
                        icon = "⚠"
                        action = "Investigate"
                        text = f"Demand exceeds capacity by {pressure:.0f}% in {pi_label}"
                    elif pressure >= 5:
                        severity = "watch"
                        icon = "↑"
                        action = "Watch"
                        text = f"Demand exceeds capacity by {pressure:.0f}% in {pi_label}"
                    elif pressure <= -10:
                        severity = "info"
                        icon = "↔"
                        action = "No action"
                        text = f"Capacity exceeds demand by {abs(pressure):.0f}% in {pi_label}"
                    else:
                        severity = "info"
                        icon = "↔"
                        action = "No action"
                        text = f"Demand and capacity are balanced in {pi_label}"
                    out.append(
                        make_signal(
                            id="capacity_pressure_last_pi",
                            level="Team" if level == "Team" else "Program",
                            severity=severity,
                            icon=icon,
                            title="Capacity pressure",
                            text=_with_team_suffix(text),
                            metric_value=pressure,
                            metric_unit="%",
                            impact=None,
                            action=action,
                            source="Delivery",
                        )
                    )

    if isinstance(cap_df, pd.DataFrame) and not cap_df.empty:
        c_col = None
        d_col = None
        for col in cap_df.columns:
            low = str(col).lower()
            if "contractor" in low and "fte" in low and ("cap" in low or "capacity" in low):
                if "cs" not in low:
                    c_col = c_col or col
            if "delivery" in low and "fte" in low and ("cap" in low or "capacity" in low):
                d_col = d_col or col
        label_col = "ITERATION_LEVEL3" if "ITERATION_LEVEL3" in cap_df.columns else "PI_LABEL"
        if c_col and d_col and label_col in cap_df.columns:
            agg = cap_df.groupby(label_col, dropna=False)[[c_col, d_col]].sum().reset_index()
            if "PI_ORDER" in cap_df.columns:
                pi_order = cap_df.groupby(label_col, dropna=False)["PI_ORDER"].min().reset_index()
                agg = agg.merge(pi_order, on=label_col, how="left").sort_values("PI_ORDER")
            else:
                agg = agg.sort_values(label_col)
            if len(agg.index) >= 2:
                prev = agg.iloc[-2]
                last = agg.iloc[-1]
                prev_total = float(prev.get(c_col) or 0.0) + float(prev.get(d_col) or 0.0)
                last_total = float(last.get(c_col) or 0.0) + float(last.get(d_col) or 0.0)
                if prev_total > 0 and last_total > 0:
                    prev_share = float(prev.get(c_col) or 0.0) / prev_total
                    last_share = float(last.get(c_col) or 0.0) / last_total
                    delta_pp = (last_share - prev_share) * 100.0
                    if delta_pp >= 6:
                        severity = "risk"
                        icon = "⚠"
                        action = "Reassess"
                        text = f"Contractor share of SoD increased by {delta_pp:.0f} pp since last PI"
                    elif delta_pp >= 3:
                        severity = "watch"
                        icon = "↑"
                        action = "Watch"
                        text = f"Contractor share of SoD increased by {delta_pp:.0f} pp since last PI"
                    elif delta_pp <= -3:
                        severity = "info"
                        icon = "↓"
                        action = "No action"
                        text = f"Contractor share of SoD decreased by {abs(delta_pp):.0f} pp since last PI"
                    else:
                        severity = "info"
                        icon = "↔"
                        action = "No action"
                        text = "Contractor share of SoD unchanged since last PI"
                    out.append(
                        make_signal(
                            id="contractor_mix_shift_last_pi",
                            level="Team" if level == "Team" else "Program",
                            severity=severity,
                            icon=icon,
                            title="Contractor mix",
                            text=_with_team_suffix(text),
                            metric_value=delta_pp,
                            metric_unit="%",
                            impact=None,
                            action=action,
                            source="Delivery",
                        )
                    )

    return out[:6], debug


def rank_signals(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _sort_key(sig: dict[str, Any]) -> tuple[int, int, float, str]:
        sev = str(sig.get("severity") or "info").strip().lower()
        sev_rank = SEVERITY_ORDER.get(sev, 2)
        impact = _to_float(sig.get("impact"))
        has_impact_rank = 0 if impact is not None else 1
        impact_rank = -(impact or 0.0)
        title = _clean_text(sig.get("title")).lower()
        return (sev_rank, has_impact_rank, impact_rank, title)

    return sorted(list(signals or []), key=_sort_key)


def build_top_signals_table(signals: list[dict[str, Any]]) -> pd.DataFrame:
    ranked = rank_signals(signals)
    rows: list[dict[str, Any]] = []
    for sig in ranked:
        impact = _to_float(sig.get("impact"))
        impact_txt = f"${impact:,.0f}" if impact is not None else "-"
        title = _clean_text(sig.get("title"))
        text = _clean_text(sig.get("text"))
        rows.append(
            {
                "LEVEL": _clean_text(sig.get("level")) or "Program",
                "SIGNAL": f"{title}: {text}" if title else text,
                "IMPACT": impact_txt,
                "ACTION": _clean_text(sig.get("action")) or "No action",
            }
        )
    return pd.DataFrame(rows, columns=["LEVEL", "SIGNAL", "IMPACT", "ACTION"])


def build_signal_summary(signals: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = rank_signals(signals)
    app_risk = sum(1 for s in signals if s.get("severity") == "risk" and s.get("level") == "Application")
    delivery_risk = sum(
        1 for s in signals if s.get("severity") == "risk" and str(s.get("source") or "").strip().lower() == "delivery"
    )
    financial_risk = sum(
        1
        for s in signals
        if s.get("severity") == "risk"
        and str(s.get("source") or "").strip().lower() == "financial"
        and s.get("level") == "Program"
    )
    delivery_pressure = sum(
        1
        for s in signals
        if s.get("id") == "capacity_pressure_last_pi" and str(s.get("severity") or "").lower() in {"risk", "watch"}
    )
    top = ranked[0] if ranked else None
    apps_title = "Apps/Groups at risk" if app_risk > 0 else "Delivery risks"
    apps_value = int(app_risk if app_risk > 0 else delivery_risk)
    return {
        "apps_title": apps_title,
        "apps_value": apps_value,
        "program_financial_risk": int(financial_risk),
        "delivery_pressure": int(delivery_pressure),
        "top_driver": _clean_text((top or {}).get("title")) or "No active risk",
        "top_signal": top,
    }
