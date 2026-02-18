# Summary: update data quality guide section labels to NEXT branding.
from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Tuple

import pandas as pd

from core.cache_utils import cache_data_portfolio

from core.quality.context import QualityContext
from core.quality.detectors import run_all_detectors
from core.quality.issue_registry import FixSystem, get_issue_spec
from core.quality.issues import Issue, SEVERITY_ORDER

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None  # type: ignore

_ISSUES_CACHE_VERSION = 4


def _norm_severity(val: Any) -> str:
    raw = str(val or "").strip().upper()
    if raw in {"BLOCKING", "BLOCKER"}:
        return "Blocking"
    if raw in {"WARNING", "WARN"}:
        return "Warning"
    if raw in {"INFO", "INFORMATIONAL"}:
        return "Info"
    return str(val or "").strip() or "Info"


def _norm_scenario(val: Any, default_val: str) -> str:
    scen = str(val or "").strip()
    if scen:
        return scen
    fallback = str(default_val or "").strip()
    return fallback if fallback else "—"


def _scope_cols(scope: Dict[str, Any]) -> Dict[str, Any]:
    s = dict(scope or {})
    out: Dict[str, Any] = {}
    out["SCOPE_YEAR"] = s.get("year")
    out["SCOPE_PI"] = s.get("pi")
    out["SCOPE_PROGRAMS"] = ", ".join([str(x) for x in (s.get("programs") or []) if str(x).strip()])
    out["SCOPE_TEAMS"] = ", ".join([str(x) for x in (s.get("teams") or []) if str(x).strip()])
    out["SCOPE_APP_GROUPS"] = ", ".join([str(x) for x in (s.get("app_groups") or []) if str(x).strip()])
    return out


def _issue_area(issue_type: str) -> str:
    t = str(issue_type or "").strip().upper()
    if t in {
        "TEAM_MISSING_PROGRAM",
        "APP_GROUP_MISSING_OWNER_TEAM",
        "CONFIG_APP_MISSING_OWNER",
        "APP_GROUP_MISSING_MSP_ASSIGNMENT",
        "APP_GROUP_MISSING_ADO_MAPPING",
        "APP_GROUP_MULTI_INSTANCE",
        "CONFIG_PROGRAM_HAS_NO_TEAMS",
    }:
        return "Ownership & Structure"
    if t in {"MISSING_WF_RATES", "CONFIG_MISSING_WF_RATES"}:
        return "Rates & Workforce"
    if t in {
        "ADO_UNMAPPED_APP",
        "ADO_UNMAPPED_TEAM",
        "ADO_MISSING_EFFORT",
        "ADO_FEATURES_MISSING_SWAG",
        "ADO_FEATURES_MISSING_APP_NAME",
        "CONFIG_NO_ADO_MAPPING_ROWS",
    }:
        return "ADO Demand & Forecast"
    if t in {"APPTIO_UNMAPPED_ACTUALS"}:
        return "Actuals (Apptio/NWF)"
    if t in {"UNASSIGNED_COST"}:
        return "Ownership & Structure"
    return "Other"


def _root_cause_code(issue_type: str) -> str:
    """Compact, user-facing root-cause code for mapping guide."""
    t = str(issue_type or "").strip().upper()
    mapping = {
        "UNASSIGNED_COST": "UNMAPPED_APP_GROUP",
        "MISSING_WF_RATES": "RATES_MISSING",
        "CONFIG_MISSING_WF_RATES": "CONFIG_MISSING_WF_RATES",
        "TEAM_MISSING_PROGRAM": "TEAM_MISSING_PROGRAM",
        "APP_GROUP_MISSING_OWNER_TEAM": "APP_GROUP_MISSING_OWNER_TEAM",
        "CONFIG_APP_MISSING_OWNER": "CONFIG_APP_MISSING_OWNER",
        "APP_GROUP_MISSING_MSP_ASSIGNMENT": "APP_GROUP_MISSING_MSP_ASSIGNMENT",
        "APP_GROUP_MISSING_ADO_MAPPING": "APP_GROUP_MISSING_ADO_MAPPING",
        "APP_GROUP_MULTI_INSTANCE": "APP_GROUP_MULTI_INSTANCE",
        "ADO_UNMAPPED_APP": "UNMAPPED_APP_GROUP",
        "ADO_UNMAPPED_TEAM": "UNMAPPED_TEAM",
        "ADO_MISSING_EFFORT": "ADO_MISSING_EFFORT",
        "ADO_FEATURES_MISSING_SWAG": "ADO_MISSING_SWAG",
        "ADO_FEATURES_MISSING_APP_NAME": "ADO_MISSING_APP_NAME",
        "APPTIO_UNMAPPED_ACTUALS": "APPTIO_UNMAPPED_NWF",
        "CONFIG_NO_ADO_MAPPING_ROWS": "CONFIG_NO_ADO_MAPPING_ROWS",
        "CONFIG_PROGRAM_HAS_NO_TEAMS": "CONFIG_PROGRAM_HAS_NO_TEAMS",
        "CONFIG_ADO_TEAMS_UNMAPPED": "CONFIG_ADO_TEAMS_UNMAPPED",
        "ADO_UNMAPPED_TEAMS_IN_PROGRAM": "ADO_UNMAPPED_TEAMS_IN_PROGRAM",
    }
    return mapping.get(t, "OTHER")


def _guide_section(root_cause_code: str) -> tuple[str, str]:
    code = str(root_cause_code or "").strip().upper()
    mapping: Dict[str, tuple[str, str]] = {
        "UNMAPPED_APP_GROUP": ("ADO App Name → NEXT Application Group Mapping", "GUIDE_ADO_MAPPING"),
        "UNMAPPED_TEAM": ("ADO App Name → NEXT Application Group Mapping", "GUIDE_ADO_MAPPING"),
        "RATES_MISSING": ("Rates & Time Semantics (Rates are per PI / iteration)", "GUIDE_RATES_TIME"),
        "APPTIO_UNMAPPED_NWF": ("Scenario Semantics (Baseline vs Expected vs Actual)", "GUIDE_SCENARIOS"),
        "TEAM_MISSING_PROGRAM": ("How to Fix Common Data Quality Issues", "GUIDE_OWNERSHIP"),
        "APP_GROUP_MISSING_OWNER_TEAM": ("How to Fix Common Data Quality Issues", "GUIDE_OWNERSHIP"),
        "CONFIG_APP_MISSING_OWNER": ("Headcount & Cost Attribution Rules", "GUIDE_OWNERSHIP"),
        "APP_GROUP_MISSING_MSP_ASSIGNMENT": ("How to Fix Common Data Quality Issues", "GUIDE_OWNERSHIP"),
        "APP_GROUP_MISSING_ADO_MAPPING": ("ADO App Name → NEXT Application Group Mapping", "GUIDE_ADO_MAPPING"),
        "APP_GROUP_MULTI_INSTANCE": ("How to Fix Common Data Quality Issues", "GUIDE_OWNERSHIP"),
        "CONFIG_MISSING_WF_RATES": ("Rates & Time Semantics (Rates are per PI / iteration)", "GUIDE_RATES_TIME"),
        "CONFIG_NO_ADO_MAPPING_ROWS": ("ADO App Name → NEXT Application Group Mapping", "GUIDE_ADO_MAPPING"),
        "CONFIG_PROGRAM_HAS_NO_TEAMS": ("How NEXT Data Is Built", "GUIDE_OWNERSHIP"),
        "ADO_MISSING_EFFORT": ("Scenario Semantics (Baseline vs Expected vs Actual)", "GUIDE_SCENARIOS"),
        "ADO_MISSING_SWAG": ("ADO Feature Requirements (SWAG, App Name)", "GUIDE_ADO_FEATURES"),
        "ADO_MISSING_APP_NAME": ("ADO Feature Requirements (SWAG, App Name)", "GUIDE_ADO_FEATURES"),
    }
    section, key = mapping.get(code, ("How to Fix Common Data Quality Issues", "GUIDE_GENERAL"))
    return section, key


def _flatten_issue(issue: Issue) -> Dict[str, Any]:
    d = issue.to_dict()
    scope = d.pop("scope", {}) or {}
    fix = d.pop("fix", {}) or {}
    evidence_ref = d.get("evidence_ref")
    spec = get_issue_spec(d.get("issue_type") or "")
    out: Dict[str, Any] = {}
    out["ISSUE_ID"] = d.get("issue_id")
    issue_type = str(d.get("issue_type") or "")
    out["ISSUE_TYPE"] = issue_type
    out["ISSUE_AREA"] = spec.issue_area if spec else _issue_area(issue_type)
    root_code = _root_cause_code(issue_type)
    guide_section, guide_key = _guide_section(root_code)
    if spec and spec.guide_section:
        guide_section = spec.guide_section
    out["ROOT_CAUSE_CODE"] = root_code
    out["GUIDE_SECTION"] = guide_section
    out["GUIDE_KEY"] = guide_key
    title = spec.title if spec and spec.title else d.get("title")
    out["TITLE"] = title
    severity_raw = d.get("severity") or (spec.default_severity if spec else "")
    out["SEVERITY"] = _norm_severity(severity_raw)
    out["STATUS"] = d.get("status")
    out["SCENARIO"] = _norm_scenario(d.get("scenario"), spec.default_scenario if spec else "—")
    out["SOURCE"] = d.get("source")
    out["IMPACT_USD_EST"] = float(d.get("impact_usd_est") or 0.0)
    out["IMPACT_FTE"] = float(d.get("impact_fte") or 0.0)
    out["IMPACT_FEATURES"] = int(d.get("impact_features") or 0)
    out["IMPACT_BASIS"] = d.get("impact_basis")
    out["IMPACT_BASIS_DETAIL"] = d.get("impact_basis_detail")
    out["CONFIDENCE"] = d.get("confidence")
    out["SUMMARY"] = d.get("summary")
    out["TAGS"] = ", ".join([str(x) for x in (d.get("tags") or []) if str(x).strip()])
    out["COMPUTED_AT"] = d.get("computed_at")
    out.update(_scope_cols(scope))
    out["FIX_SYSTEM"] = spec.fix_system if spec else FixSystem.TCO
    out["FIX_TARGET"] = spec.fix_target if spec else str(fix.get("page") or "")
    out["FIX_PAGE"] = out["FIX_TARGET"] if out["FIX_SYSTEM"] == FixSystem.TCO else ""
    fix_action = str(fix.get("action_label") or "").strip()
    if not fix_action and spec:
        fix_action = spec.fix_action_template
    out["FIX_ACTION"] = fix_action
    out["FIX_HELP"] = str(fix.get("help") or "")
    out["EXPECTS_EVIDENCE"] = bool(spec.expects_evidence) if spec else False
    out["EVIDENCE_COLUMNS"] = ", ".join(spec.evidence_columns) if spec else ""
    out["DEDUPE_GROUP"] = spec.dedupe_group if spec else ""
    out["EVIDENCE_FN"] = str((evidence_ref or {}).get("fn") or "")
    out["EVIDENCE_PARAMS"] = (evidence_ref or {}).get("params")
    return out


def _sort_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    sev_rank = df["SEVERITY"].map(lambda s: SEVERITY_ORDER.get(str(s), 0)).fillna(0).astype(int)
    out = df.copy()
    out["_SEV_RANK"] = sev_rank
    out = out.sort_values(
        by=["_SEV_RANK", "IMPACT_USD_EST", "IMPACT_FTE", "IMPACT_FEATURES", "TITLE"],
        ascending=[False, False, False, False, True],
    ).drop(columns=["_SEV_RANK"])
    return out


def _cache_key(ctx: QualityContext) -> Tuple:
    # Bump `_ISSUES_CACHE_VERSION` when the flattened schema changes.
    return (_ISSUES_CACHE_VERSION, int(ctx.year), int(ctx.pi or 0), tuple(ctx.programs), tuple(ctx.teams), tuple(ctx.app_groups))


def _collect_issues_uncached(ctx: QualityContext) -> pd.DataFrame:
    issues = run_all_detectors(ctx)
    rows = [_flatten_issue(i) for i in issues]
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "ISSUE_ID",
                "ISSUE_TYPE",
                "ISSUE_AREA",
                "ROOT_CAUSE_CODE",
                "GUIDE_SECTION",
                "GUIDE_KEY",
                "TITLE",
                "SEVERITY",
                "STATUS",
                "SCENARIO",
                "SOURCE",
                "IMPACT_USD_EST",
                "IMPACT_FTE",
                "IMPACT_FEATURES",
                "IMPACT_BASIS",
                "IMPACT_BASIS_DETAIL",
                "CONFIDENCE",
                "SUMMARY",
                "TAGS",
                "SCOPE_YEAR",
                "SCOPE_PI",
                "SCOPE_PROGRAMS",
                "SCOPE_TEAMS",
                "SCOPE_APP_GROUPS",
                "FIX_PAGE",
                "FIX_ACTION",
                "FIX_HELP",
                "FIX_SYSTEM",
                "FIX_TARGET",
                "EXPECTS_EVIDENCE",
                "EVIDENCE_COLUMNS",
                "DEDUPE_GROUP",
                "EVIDENCE_FN",
                "EVIDENCE_PARAMS",
                "COMPUTED_AT",
            ]
        )
    for c in ["IMPACT_USD_EST", "IMPACT_FTE"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df.get(c), errors="coerce")
    if "IMPACT_FEATURES" in df.columns:
        df["IMPACT_FEATURES"] = pd.to_numeric(df.get("IMPACT_FEATURES"), errors="coerce").fillna(0).astype(int)

    df = _apply_dedupe_groups(df)
    return _sort_df(df)


def _mapping_row_count(params: Any) -> Optional[int]:
    if not params:
        return None
    if isinstance(params, dict):
        rows = params.get("rows")
        if isinstance(rows, list):
            counts = []
            for r in rows:
                if isinstance(r, dict) and "mapping_row_count" in r:
                    try:
                        counts.append(int(r.get("mapping_row_count") or 0))
                    except Exception:
                        continue
            if counts:
                return min(counts)
    return None


def _apply_dedupe_groups(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "DEDUPE_GROUP" not in df.columns:
        return df
    scoped = ["SCOPE_YEAR", "SCOPE_PROGRAMS", "SCOPE_TEAMS", "DEDUPE_GROUP"]
    missing_cols = [c for c in scoped if c not in df.columns]
    if missing_cols:
        return df
    df = df.copy()
    drop_idx: set[int] = set()
    for _, grp in df.groupby(scoped, dropna=False):
        group_name = str(grp["DEDUPE_GROUP"].iloc[0] or "").strip().upper()
        if group_name == "ADO_MAPPING":
            types = set(grp["ISSUE_TYPE"].astype(str).tolist())
            if "CONFIG_NO_ADO_MAPPING_ROWS" in types and "ADO_UNMAPPED_APP" in types:
                cfg = grp[grp["ISSUE_TYPE"].astype(str).eq("CONFIG_NO_ADO_MAPPING_ROWS")]
                count_val = None
                if not cfg.empty:
                    count_val = _mapping_row_count(cfg["EVIDENCE_PARAMS"].iloc[0])
                if count_val == 0 or count_val is None:
                    drop_idx.update(grp[grp["ISSUE_TYPE"].astype(str).eq("ADO_UNMAPPED_APP")].index.tolist())
                else:
                    drop_idx.update(grp[grp["ISSUE_TYPE"].astype(str).eq("CONFIG_NO_ADO_MAPPING_ROWS")].index.tolist())
    if drop_idx:
        df = df.loc[[i for i in df.index if i not in drop_idx]].copy()
    return df


def collect_issues(ctx: QualityContext) -> pd.DataFrame:
    """Collect, dedupe, and sort issues for the current scope.

    Caching is applied when Streamlit is available, keyed by (year, pi, programs, teams, app_groups).
    """
    if st is None:
        return _collect_issues_uncached(ctx)

    @cache_data_portfolio(ttl=180, show_spinner=False)  # type: ignore[misc]
    def _cached(key: Tuple, cost_lines: pd.DataFrame, ado: pd.DataFrame, apptio: pd.DataFrame) -> pd.DataFrame:
        # Reconstruct a lightweight ctx for detectors (db + scope + frames).
        # key schema: (cache_version, year, pi, programs, teams, app_groups)
        _ctx = QualityContext(
            db=ctx.db,
            year=int(key[1]),
            pi=int(key[2]) if int(key[2]) else None,
            programs=tuple(key[3]),
            teams=tuple(key[4]),
            app_groups=tuple(key[5]),
            cost_lines=cost_lines,
            ado_features=ado,
            apptio_actuals_breakdown=apptio,
        )
        return _collect_issues_uncached(_ctx)

    return _cached(_cache_key(ctx), ctx.cost_lines, ctx.ado_features, ctx.apptio_actuals_breakdown)
