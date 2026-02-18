from __future__ import annotations

import io
import json
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from difflib import SequenceMatcher
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation

from db import ensure_all_views_ok
from db import ensure_analytics_views_ok
from db import ensure_contracts_table
from db import ensure_location_and_contractor_tables
from db import ensure_msp_support
from db import ensure_tables
from db import ensure_team_composition_history
from db import ensure_program_composition_history
from db import _fq
from db import execute as _execute_autocommit
from db.mssql_backend import _new_connection


# ======================================================================================
# Public API (called by Streamlit UI only)
# ======================================================================================

ValidationMode = str  # "master" | "finance"


def get_checklist_markdown() -> str:
    """Human-facing checklist for what’s required vs optional in Bulk Load v2."""
    return """\
### Master Data (must-have for dashboards)

Required sheets:
- **Programs**: `PROGRAMNAME` (unique)
- **Teams**: `TEAMNAME` (unique), `PROGRAMNAME` (must exist in Programs)
- **Vendors**: `VENDORNAME` (unique)
- **App Groups**: `GROUPNAME` (unique), `TEAMNAME`, `IS_BASE` (each Team must have exactly one row with `IS_BASE=1`)
- **Applications**: `APPLICATIONNAME` (unique)

Key rules:
- `PROGRAMS.PROGRAMNAME` must be unique (use the exact existing name if it already exists).
- `TEAMS.TEAMNAME` must be unique and must reference an existing Program.
- `APPLICATION_GROUPS`: exactly one base group per Team (`IS_BASE=1`).
- `APPLICATIONS.APPLICATIONNAME` must be unique.

### Finance (invoices/forecast views)

Required sheets:
- **Contracts**: unique per `(TEAMNAME, APPLICATIONNAME)`; used to generate planned recurring invoices
- **Invoices**: unique per `(TEAMNAME, APPLICATIONNAME, FISCAL_YEAR, INVOICE_TYPE)`
- **Program Addl Costs**: unique per `(PROGRAMNAME, YEAR, MONTH, COST_TYPE, SUBTYPE)`

Important rule:
- Invoices with `STATUS='Completed'` are never overwritten by contract forecast sync.
"""


def read_workbook(uploaded_file: Any) -> Dict[str, pd.DataFrame]:
    """Read an uploaded workbook into a {canonical_sheet_name: DataFrame} mapping.

    Supports: .xlsx, .xlsm, .xlsb, .xls, .csv
    """
    filename = getattr(uploaded_file, "name", "") or ""
    raw = _read_uploaded_bytes(uploaded_file)

    # CSV fallback (treated as a single sheet)
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(io.BytesIO(raw))
        return {"CSV": df}

    # Excel
    engine: Optional[str] = None
    fn = filename.lower()
    if fn.endswith((".xlsx", ".xlsm")):
        engine = "openpyxl"
    elif fn.endswith(".xlsb"):
        engine = "pyxlsb"
    elif fn.endswith(".xls"):
        engine = "xlrd"
    xls = pd.ExcelFile(io.BytesIO(raw), engine=engine)
    out: Dict[str, pd.DataFrame] = {}
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
        except Exception:
            # Never fail whole import on a single unreadable sheet; UI will surface empty/missing.
            df = pd.DataFrame()
        out[_canonical_sheet_name(sheet)] = df
    return out


def validate_workbook(
    dfs: Dict[str, pd.DataFrame],
    mode: ValidationMode,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Validate workbook content and cross-sheet referential integrity.

    Returns: (blocking_errors_df, warnings_df) with columns:
      sheet, row, column, error, suggested_fix
    """
    mode = _normalize_mode(mode)
    required = _required_sheets_for_mode(mode)
    present = set(dfs.keys())

    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    for sheet in required:
        if sheet not in present:
            errors.append(
                _err(
                    sheet=sheet,
                    row=0,
                    column="(sheet)",
                    error=f"Missing required sheet: {sheet}",
                    fix="Download the Bulk Load v2 template and add this sheet.",
                )
            )

    # If core sheets are missing, bail early to avoid noisy follow-on errors.
    if any(e["column"] == "(sheet)" for e in errors):
        return pd.DataFrame(errors), pd.DataFrame(warnings)

    # Normalize sheets to canonical dataframes (columns + trims), but keep original indices for row numbers.
    norm: Dict[str, pd.DataFrame] = {}
    for sheet_name, df in dfs.items():
        if df is None:
            continue
        norm[sheet_name] = df.copy()

    # Validate each sheet's required columns.
    for sheet, spec in _SHEET_SPECS.items():
        if sheet not in norm:
            continue
        df = norm[sheet]
        if df.empty:
            continue
        colmap, missing = _map_columns(df.columns.tolist(), spec.required_columns)
        for c in missing:
            target = errors if sheet in required else warnings
            target.append(
                _err(
                    sheet=sheet,
                    row=1,
                    column=c,
                    error=f"Missing required column: {c}",
                    fix=f"Add column '{c}' (exact header) to sheet '{sheet}'.",
                )
            )
        norm[sheet] = df.rename(columns=colmap)

    if errors:
        return pd.DataFrame(errors), pd.DataFrame(warnings)

    # Canonicalize
    df_vendors = _normalize_sheet(norm.get("Vendors", pd.DataFrame()), "Vendors")
    df_programs = _normalize_sheet(norm.get("Programs", pd.DataFrame()), "Programs")
    df_teams = _normalize_sheet(norm.get("Teams", pd.DataFrame()), "Teams")
    df_groups = _normalize_sheet(norm.get("App Groups", pd.DataFrame()), "App Groups")
    df_apps = _normalize_sheet(norm.get("Applications", pd.DataFrame()), "Applications")

    if mode == "master":
        # Uniqueness rules (case-insensitive, whitespace-normalized)
        errors += _validate_unique(df_programs, "Programs", "PROGRAMNAME", "PROGRAMS.PROGRAMNAME must be unique")
        errors += _validate_unique(df_teams, "Teams", "TEAMNAME", "TEAMS.TEAMNAME must be unique")
        errors += _validate_unique(df_vendors, "Vendors", "VENDORNAME", "VENDORS.VENDORNAME must be unique")
        errors += _validate_unique(df_groups, "App Groups", "GROUPNAME", "APPLICATION_GROUPS.GROUPNAME must be unique")
        errors += _validate_unique(df_apps, "Applications", "APPLICATIONNAME", "APPLICATIONS.APPLICATIONNAME must be unique")

        # Required references / fields
        errors += _validate_required(df_programs, "Programs", ["PROGRAMNAME"])
        errors += _validate_required(df_vendors, "Vendors", ["VENDORNAME"])
        errors += _validate_required(df_teams, "Teams", ["TEAMNAME", "PROGRAMNAME"])
        errors += _validate_required(df_groups, "App Groups", ["GROUPNAME", "TEAMNAME", "IS_BASE"])
        errors += _validate_required(df_apps, "Applications", ["APPLICATIONNAME"])

        # Team -> Program reference (must exist in Programs sheet)
        if not df_teams.empty and not df_programs.empty:
            program_names = set(df_programs["PROGRAMNAME_NK"].dropna().tolist())
            missing_refs = df_teams[~df_teams["PROGRAMNAME_NK"].isin(program_names)]
            for idx, r in missing_refs.iterrows():
                errors.append(
                    _err(
                        sheet="Teams",
                        row=_excel_row(idx),
                        column="PROGRAMNAME",
                        error=f"Team references unknown PROGRAMNAME: {r.get('PROGRAMNAME')}",
                        fix="Ensure the program exists in the Programs sheet (exact name match).",
                    )
                )

        # Exactly one base group per team
        if not df_groups.empty:
            base = df_groups.copy()
            base["IS_BASE_BOOL"] = base["IS_BASE"].apply(_to_bool)
            per_team = base.groupby("TEAMNAME_NK", dropna=False)["IS_BASE_BOOL"].sum().reset_index()
            bad = per_team[(per_team["TEAMNAME_NK"].notna()) & (per_team["IS_BASE_BOOL"] != 1)]
            if not bad.empty:
                bad_teams = bad["TEAMNAME_NK"].tolist()
                offenders = df_groups[df_groups["TEAMNAME_NK"].isin(bad_teams)]
                for idx, r in offenders.iterrows():
                    errors.append(
                        _err(
                            sheet="App Groups",
                            row=_excel_row(idx),
                            column="IS_BASE",
                            error=f"Each TEAM must have exactly one App Group with IS_BASE=1 (team '{r.get('TEAMNAME')}')",
                            fix="Set IS_BASE=1 on exactly one group per team; set others to 0.",
                        )
                    )

    # Soft duplicate warnings vs existing DB (e.g., "R&M" vs "Reliability and Maintenance")
    try:
        db_lists = _fetch_existing_natural_keys_for_warnings()
        warnings += _warn_similar_to_existing(df_programs, "Programs", "PROGRAMNAME", db_lists.get("PROGRAMNAME", []))
        warnings += _warn_similar_to_existing(df_teams, "Teams", "TEAMNAME", db_lists.get("TEAMNAME", []))
        warnings += _warn_similar_to_existing(df_vendors, "Vendors", "VENDORNAME", db_lists.get("VENDORNAME", []))
        warnings += _warn_similar_to_existing(df_groups, "App Groups", "GROUPNAME", db_lists.get("GROUPNAME", []))
        warnings += _warn_similar_to_existing(df_apps, "Applications", "APPLICATIONNAME", db_lists.get("APPLICATIONNAME", []))
    except Exception:
        # Never block validation if DB connection/secrets aren’t available.
        pass

    # Finance sheets (finance-only loader validates against existing DB)
    if mode == "finance":
        df_contracts = _normalize_sheet(norm.get("Contracts", pd.DataFrame()), "Contracts")
        df_invoices = _normalize_sheet(norm.get("Invoices", pd.DataFrame()), "Invoices")
        df_pac = _normalize_sheet(norm.get("Program Addl Costs", pd.DataFrame()), "Program Addl Costs")

        errors += _validate_required(df_contracts, "Contracts", ["TEAMNAME", "APPLICATIONNAME", "START_FY", "END_FY", "ANNUAL_AMOUNT"])
        errors += _validate_required(df_invoices, "Invoices", ["TEAMNAME", "APPLICATIONNAME", "FISCAL_YEAR", "INVOICE_TYPE", "AMOUNT"])
        errors += _validate_required(df_pac, "Program Addl Costs", ["PROGRAMNAME", "YEAR", "MONTH", "COST_TYPE", "AMOUNT"])

        # Contract uniqueness per (TEAMNAME, APPLICATIONNAME)
        errors += _validate_unique_multi(
            df_contracts,
            "Contracts",
            ["TEAMNAME_NK", "APPLICATIONNAME_NK"],
            "CONTRACTS must be unique per (TEAMID, APPLICATIONID)",
            display_cols=["TEAMNAME", "APPLICATIONNAME"],
        )
        # Invoice uniqueness per key
        errors += _validate_unique_multi(
            df_invoices,
            "Invoices",
            ["TEAMNAME_NK", "APPLICATIONNAME_NK", "FISCAL_YEAR", "INVOICE_TYPE_NK"],
            "INVOICES must be unique per (TEAMID, APPLICATIONID, FISCAL_YEAR, INVOICE_TYPE)",
            display_cols=["TEAMNAME", "APPLICATIONNAME", "FISCAL_YEAR", "INVOICE_TYPE"],
        )

        # Coerce fiscal / month fields
        errors += _validate_int_range(df_contracts, "Contracts", "START_FY", 1900, 3000, required=True)
        errors += _validate_int_range(df_contracts, "Contracts", "END_FY", 1900, 3000, required=True)
        errors += _validate_int_range(df_contracts, "Contracts", "RENEWAL_MONTH", 1, 12, required=False)
        errors += _validate_num(df_contracts, "Contracts", "ANNUAL_AMOUNT", required=True)
        warnings += _validate_num(df_contracts, "Contracts", "ESCALATION_PCT", required=False, warn_only=True)

        # Contract window
        if not df_contracts.empty:
            for idx, r in df_contracts.iterrows():
                try:
                    s_fy = int(_to_int(r.get("START_FY")))
                    e_fy = int(_to_int(r.get("END_FY")))
                    if e_fy < s_fy:
                        errors.append(
                            _err(
                                sheet="Contracts",
                                row=_excel_row(idx),
                                column="END_FY",
                                error="END_FY must be >= START_FY",
                                fix="Fix the fiscal year window for the contract.",
                            )
                        )
                except Exception:
                    continue

        # Invoices basic coercions
        errors += _validate_int_range(df_invoices, "Invoices", "FISCAL_YEAR", 1900, 3000, required=True)
        errors += _validate_num(df_invoices, "Invoices", "AMOUNT", required=True)

        # Program addl costs coercions
        errors += _validate_int_range(df_pac, "Program Addl Costs", "YEAR", 1900, 3000, required=True)
        errors += _validate_int_range(df_pac, "Program Addl Costs", "MONTH", 1, 12, required=True)
        errors += _validate_num(df_pac, "Program Addl Costs", "AMOUNT", required=True)

        # Finance reference checks (against DB; workbook may not include master sheets)
        try:
            db_lists = _fetch_existing_natural_keys_for_warnings()
            teams_db = set((_nk(x) for x in db_lists.get("TEAMNAME", [])) if db_lists else [])
            apps_db = set((_nk(x) for x in db_lists.get("APPLICATIONNAME", [])) if db_lists else [])
            progs_db = set((_nk(x) for x in db_lists.get("PROGRAMNAME", [])) if db_lists else [])
            teams_db.discard(None)  # type: ignore[arg-type]
            apps_db.discard(None)   # type: ignore[arg-type]
            progs_db.discard(None)  # type: ignore[arg-type]
            if df_contracts is not None and not df_contracts.empty:
                for idx, r in df_contracts.iterrows():
                    if r.get("TEAMNAME_NK") and r.get("TEAMNAME_NK") not in teams_db:
                        errors.append(_err("Contracts", _excel_row(idx), "TEAMNAME", f"Unknown TEAMNAME: {r.get('TEAMNAME')}", "Create the team in Master Data first (Teams page or Master Data template)."))
                    if r.get("APPLICATIONNAME_NK") and r.get("APPLICATIONNAME_NK") not in apps_db:
                        errors.append(_err("Contracts", _excel_row(idx), "APPLICATIONNAME", f"Unknown APPLICATIONNAME: {r.get('APPLICATIONNAME')}", "Create the application in Master Data first (Applications page or Master Data template)."))
            if df_invoices is not None and not df_invoices.empty:
                for idx, r in df_invoices.iterrows():
                    if r.get("TEAMNAME_NK") and r.get("TEAMNAME_NK") not in teams_db:
                        errors.append(_err("Invoices", _excel_row(idx), "TEAMNAME", f"Unknown TEAMNAME: {r.get('TEAMNAME')}", "Create the team in Master Data first (Teams page or Master Data template)."))
                    if r.get("APPLICATIONNAME_NK") and r.get("APPLICATIONNAME_NK") not in apps_db:
                        errors.append(_err("Invoices", _excel_row(idx), "APPLICATIONNAME", f"Unknown APPLICATIONNAME: {r.get('APPLICATIONNAME')}", "Create the application in Master Data first (Applications page or Master Data template)."))
            if df_pac is not None and not df_pac.empty:
                for idx, r in df_pac.iterrows():
                    if r.get("PROGRAMNAME_NK") and r.get("PROGRAMNAME_NK") not in progs_db:
                        errors.append(_err("Program Addl Costs", _excel_row(idx), "PROGRAMNAME", f"Unknown PROGRAMNAME: {r.get('PROGRAMNAME')}", "Create the program in Master Data first (Programs page or Master Data template)."))
        except Exception:
            pass

    return pd.DataFrame(errors), pd.DataFrame(warnings)


def plan_changes(dfs: Dict[str, pd.DataFrame], mssql_connection: Any, mode: ValidationMode = "master") -> Dict[str, Any]:
    """Build an import plan with insert/update/skip counts and sample previews."""
    mode = _normalize_mode(mode)
    detected = {k: {"rows": int(len(v.index)) if isinstance(v, pd.DataFrame) else 0, "cols": list(v.columns) if isinstance(v, pd.DataFrame) else []}
                for k, v in dfs.items()}

    # Normalize to canonical sheets and coerce types for comparisons
    clean = _clean_workbook(dfs, mode=mode)

    cn = mssql_connection or _new_connection()
    close_cn = mssql_connection is None
    try:
        plan = _plan_against_db(clean, cn, mode=mode)
    finally:
        if close_cn:
            try:
                cn.close()
            except Exception:
                pass

    plan["mode"] = mode
    plan["detected_sheets"] = detected
    plan["run_id"] = str(uuid.uuid4())
    plan["created_at"] = datetime.utcnow().isoformat()
    plan["data"] = {k: v for k, v in clean.items() if isinstance(v, pd.DataFrame)}
    return plan


def apply_import(
    plan: Dict[str, Any],
    mssql_connection: Any,
    progress_cb: Optional[Callable[[str, int, int], None]] = None,
    user_email: Optional[str] = None,
    source_filename: Optional[str] = None,
    rebuild_all_views: bool = False,
) -> Dict[str, Any]:
    """Apply an import plan in a single MSSQL transaction (rollback on error)."""
    run_id = str(plan.get("run_id") or uuid.uuid4())
    mode: ValidationMode = _normalize_mode(str(plan.get("mode") or "master"))

    # Ensure schema & required views exist (idempotent). This uses autocommit connections.
    ensure_tables()
    ensure_contracts_table()
    ensure_location_and_contractor_tables()
    ensure_team_composition_history()
    ensure_program_composition_history()
    ensure_msp_support()

    started = datetime.utcnow().isoformat()
    summary: Dict[str, Any] = {
        "run_id": run_id,
        "mode": mode,
        "started_at": started,
        "ended_at": None,
        "success": False,
        "steps": [],
        "log": [],
    }

    cn = mssql_connection or _new_connection()
    close_cn = mssql_connection is None
    try:
        with cn.cursor() as cur:
            cur.execute("SET NOCOUNT ON;")
            steps = _build_steps_from_plan(plan, mode=mode)
            total = len(steps)
            for i, step in enumerate(steps, start=1):
                if progress_cb:
                    progress_cb(step.name, i - 1, total)
                res = step.run(cur, user_email=user_email)
                summary["steps"].append(res)
                summary["log"].append(f"{step.name}: {res.get('inserted', 0)} inserted, {res.get('updated', 0)} updated, {res.get('skipped', 0)} skipped")
            if progress_cb:
                progress_cb("Commit", total, total)
        cn.commit()
        summary["success"] = True
    except Exception as e:
        try:
            cn.rollback()
        except Exception:
            pass
        summary["log"].append(f"ERROR: {e}")
        summary["error"] = str(e)
        # Return failure summary to the UI instead of raising; the UI can display it.
    finally:
        summary["ended_at"] = datetime.utcnow().isoformat()
        try:
            # Best-effort view refresh/verification after data commit.
            if summary.get("success"):
                if bool(rebuild_all_views):
                    ensure_all_views_ok()
                else:
                    ensure_analytics_views_ok()
        except Exception as e:
            summary["log"].append(f"WARNING: view check failed: {e}")
            summary["views_ok"] = False
        else:
            summary["views_ok"] = True

        try:
            _persist_bulkload_run_log(summary, user_email=user_email, source_filename=source_filename)
        except Exception:
            # Never fail UI due to logging persistence.
            pass

        if close_cn:
            try:
                cn.close()
            except Exception:
                pass

    return summary


def build_excel_template_bytes() -> bytes:
    """Back-compat alias: returns the Master Data template."""
    return build_master_template_bytes()


def build_master_template_bytes() -> bytes:
    """Create the Master Data template (no Rates sheets; rates are managed in-app)."""
    wb = Workbook()
    wb.remove(wb.active)

    ws_instr = wb.create_sheet("Instructions")
    ws_instr.append(["Data Loader – Master Data (MSSQL)", "Programs/Teams/Vendors/Groups/Applications."])
    ws_instr.append(["Rates", "Rates are set by location in the Rates page (not imported here)."])
    ws_instr.append(["Notes", "Do not rename sheet tabs; do not change headers; leave unused sheets blank."])
    ws_instr.append(["Tip", "Use dropdowns to select existing names and avoid duplicates (e.g. 'R&M' vs 'Reliability and Maintenance')."])
    ws_instr.append(["Dropdowns", "Open in Microsoft Excel Desktop to see data validation dropdowns."])

    ws_chk = wb.create_sheet("Checklist")
    for line in get_checklist_markdown().splitlines():
        ws_chk.append([line])
    ws_chk.column_dimensions["A"].width = 120

    ws_ex = wb.create_sheet("Examples")
    ws_ex.append(["Examples only (NOT imported)."])
    ws_ex.append(["Copy/paste rows into the real sheets and adjust."])
    ws_ex.append([])
    ws_ex.append(["Programs"])
    ws_ex.append(_SHEET_SPECS["Programs"].template_columns)
    ws_ex.append(["R&M", "jane.doe@company.com", 12, 150000])
    ws_ex.append([])
    ws_ex.append(["Teams"])
    ws_ex.append(_SHEET_SPECS["Teams"].template_columns)
    ws_ex.append(["R&M - Platform", "R&M", "po@company.com", 8, 2, 0, 0, 0])
    ws_ex.append([])
    ws_ex.append(["Vendors"])
    ws_ex.append(_SHEET_SPECS["Vendors"].template_columns)
    ws_ex.append(["ACME Software"])
    ws_ex.append([])
    ws_ex.append(["App Groups"])
    ws_ex.append(_SHEET_SPECS["App Groups"].template_columns)
    ws_ex.append(["R&M Base", "R&M - Platform", "ACME Software", "po@company.com", 1])
    ws_ex.append([])
    ws_ex.append(["Applications"])
    ws_ex.append(_SHEET_SPECS["Applications"].template_columns)
    ws_ex.append(["ACME Platform", "ACME Software", "R&M Base", "Hosted SaaS"])
    ws_ex.append([])
    ws_ex.append(["Group-Team Links"])
    ws_ex.append(_SHEET_SPECS["Group-Team Links"].template_columns)
    ws_ex.append(["R&M Base", "R&M - Platform"])
    ws_ex.column_dimensions["A"].width = 26
    ws_ex.freeze_panes = "A1"

    for sheet_name in ["Programs", "Teams", "Vendors", "App Groups", "Applications", "Group-Team Links"]:
        ws = wb.create_sheet(sheet_name)
        cols = _SHEET_SPECS[sheet_name].template_columns
        ws.append(cols)
        ws.freeze_panes = "A2"
        for i, c in enumerate(cols, start=1):
            ws.column_dimensions[get_column_letter(i)].width = max(14, min(42, len(str(c)) + 2))

    ws_lu = wb.create_sheet("Lookups")
    ws_lu.sheet_state = "hidden"
    lookups = _build_lookup_lists_for_template()
    _write_lookups(ws_lu, lookups)
    _apply_dropdowns(wb, ws_lu, lookups)

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


def build_finance_template_bytes() -> bytes:
    """Create the Finance template (Contracts, Invoices, Program Addl Costs)."""
    wb = Workbook()
    wb.remove(wb.active)

    ws_instr = wb.create_sheet("Instructions")
    ws_instr.append(["Data Loader – Finance (MSSQL)", "Contracts/Invoices/Program Additional Costs."])
    ws_instr.append(["Pre-req", "Teams/Applications/Programs must already exist (load Master Data first)."])
    ws_instr.append(["Rates", "Rates are set by location in the Rates page (not imported here)."])
    ws_instr.append(["Notes", "Do not rename sheet tabs; do not change headers; leave unused sheets blank."])
    ws_instr.append(["Tip", "Use dropdowns to pick existing TEAMNAME/APPLICATIONNAME/PROGRAMNAME to avoid duplicates."])
    ws_instr.append(["Dropdowns", "Open in Microsoft Excel Desktop to see data validation dropdowns."])

    ws_chk = wb.create_sheet("Checklist")
    for line in get_checklist_markdown().splitlines():
        ws_chk.append([line])
    ws_chk.column_dimensions["A"].width = 120

    ws_ex = wb.create_sheet("Examples")
    ws_ex.append(["Examples only (NOT imported)."])
    ws_ex.append(["Copy/paste rows into the real sheets and adjust."])
    ws_ex.append([])
    ws_ex.append(["Contracts"])
    ws_ex.append(_SHEET_SPECS["Contracts"].template_columns)
    ws_ex.append(["R&M - Platform", "ACME Platform", 2025, 2027, 1, 120000, 3.0, "Active", "AGR-123", "1000", "CC-200", "SaaS", None, None, None])
    ws_ex.append([])
    ws_ex.append(["Invoices"])
    ws_ex.append(_SHEET_SPECS["Invoices"].template_columns)
    ws_ex.append(["R&M - Platform", "ACME Platform", 2025, "Recurring Invoice", 120000, "Planned", None, None, None, None, 1, "1000", "CC-200", None, None, "AGR-123", 2027, "SaaS", None])
    ws_ex.append([])
    ws_ex.append(["Program Addl Costs"])
    ws_ex.append(_SHEET_SPECS["Program Addl Costs"].template_columns)
    ws_ex.append(["R&M", 2025, 1, "Other", "", 5000])
    ws_ex.column_dimensions["A"].width = 26
    ws_ex.freeze_panes = "A1"

    for sheet_name in ["Contracts", "Invoices", "Program Addl Costs"]:
        ws = wb.create_sheet(sheet_name)
        cols = _SHEET_SPECS[sheet_name].template_columns
        ws.append(cols)
        ws.freeze_panes = "A2"
        for i, c in enumerate(cols, start=1):
            ws.column_dimensions[get_column_letter(i)].width = max(14, min(42, len(str(c)) + 2))

    ws_lu = wb.create_sheet("Lookups")
    ws_lu.sheet_state = "hidden"
    lookups = _build_lookup_lists_for_template()
    _write_lookups(ws_lu, lookups)
    _apply_dropdowns(wb, ws_lu, lookups)

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


def _build_lookup_lists_for_template() -> Dict[str, List[str]]:
    """Best-effort: query current natural keys from MSSQL to power Excel dropdowns."""
    lookups: Dict[str, List[str]] = {
        "PROGRAMNAME": [],
        "TEAMNAME": [],
        "VENDORNAME": [],
        "GROUPNAME": [],
        "APPLICATIONNAME": [],
        "INVOICE_TYPE": ["Recurring Invoice", "One-Time Invoice", "Other"],
        "STATUS": ["Planned", "Completed", "Cancelled"],
        "IS_BASE": ["0", "1"],
    }
    try:
        cn = _new_connection()
    except Exception:
        return lookups
    try:
        for key, sql in (
            ("PROGRAMNAME", f"SELECT PROGRAMNAME FROM {_fq('PROGRAMS')} WHERE PROGRAMNAME IS NOT NULL ORDER BY PROGRAMNAME"),
            ("TEAMNAME", f"SELECT TEAMNAME FROM {_fq('TEAMS')} WHERE TEAMNAME IS NOT NULL ORDER BY TEAMNAME"),
            ("VENDORNAME", f"SELECT VENDORNAME FROM {_fq('VENDORS')} WHERE VENDORNAME IS NOT NULL ORDER BY VENDORNAME"),
            ("GROUPNAME", f"SELECT GROUPNAME FROM {_fq('APPLICATION_GROUPS')} WHERE GROUPNAME IS NOT NULL ORDER BY GROUPNAME"),
            ("APPLICATIONNAME", f"SELECT APPLICATIONNAME FROM {_fq('APPLICATIONS')} WHERE APPLICATIONNAME IS NOT NULL ORDER BY APPLICATIONNAME"),
        ):
            df = pd.read_sql(sql, cn)
            vals = [str(v).strip() for v in (df[key].tolist() if key in df.columns else df.iloc[:, 0].tolist()) if str(v).strip()]
            lookups[key] = vals
    finally:
        try:
            cn.close()
        except Exception:
            pass
    return lookups


def _write_lookups(ws_lu: Any, lookups: Dict[str, List[str]]) -> None:
    headers = list(lookups.keys())
    ws_lu.append(headers)
    max_len = max((len(v) for v in lookups.values()), default=0)
    for i in range(max_len):
        row = []
        for h in headers:
            vals = lookups.get(h, [])
            row.append(vals[i] if i < len(vals) else None)
        ws_lu.append(row)
    for i, h in enumerate(headers, start=1):
        ws_lu.column_dimensions[get_column_letter(i)].width = max(18, min(48, len(h) + 2))


def _apply_dropdowns(wb: Workbook, ws_lu: Any, lookups: Dict[str, List[str]]) -> None:
    headers = [c.value for c in ws_lu[1]]
    col_by_header = {str(h): idx + 1 for idx, h in enumerate(headers) if h}

    def _range_for(header: str, max_rows: int = 1000) -> Optional[str]:
        col = col_by_header.get(header)
        if not col:
            return None
        end = max(2, min(max_rows + 1, len(lookups.get(header, [])) + 1))
        letter = get_column_letter(col)
        return f"Lookups!${letter}$2:${letter}${end}"

    def _add_list_validation(ws_name: str, header: str, target_col_name: str, allow_blank: bool = True) -> None:
        if ws_name not in wb.sheetnames:
            return
        ws = wb[ws_name]
        # Find target column index by header row
        header_row = [c.value for c in ws[1]]
        try:
            idx = header_row.index(target_col_name) + 1
        except ValueError:
            return
        rng = _range_for(header)
        if not rng:
            return
        # NOTE: In OOXML, showDropDown=True suppresses the arrow in Excel.
        dv = DataValidation(type="list", formula1=f"={rng}", allow_blank=allow_blank, showDropDown=False)
        dv.promptTitle = "Pick an existing value"
        dv.prompt = "Prefer selecting existing names to avoid duplicates."
        dv.errorTitle = "Invalid value"
        dv.error = "Pick a value from the dropdown (or match an existing name exactly)."
        ws.add_data_validation(dv)
        dv.add(f"{get_column_letter(idx)}2:{get_column_letter(idx)}5000")

    # Dropdowns (limited to the Data Loader templates)
    _add_list_validation("Teams", "PROGRAMNAME", "PROGRAMNAME")
    _add_list_validation("App Groups", "TEAMNAME", "TEAMNAME")
    _add_list_validation("App Groups", "VENDORNAME", "DEFAULT_VENDORNAME")
    _add_list_validation("Applications", "VENDORNAME", "VENDORNAME")
    _add_list_validation("Applications", "GROUPNAME", "GROUPNAME")
    _add_list_validation("Contracts", "TEAMNAME", "TEAMNAME")
    _add_list_validation("Contracts", "APPLICATIONNAME", "APPLICATIONNAME")
    _add_list_validation("Invoices", "TEAMNAME", "TEAMNAME")
    _add_list_validation("Invoices", "APPLICATIONNAME", "APPLICATIONNAME")
    _add_list_validation("Program Addl Costs", "PROGRAMNAME", "PROGRAMNAME")
    _add_list_validation("Group-Team Links", "GROUPNAME", "GROUPNAME")
    _add_list_validation("Group-Team Links", "TEAMNAME", "TEAMNAME")

    # Enums
    _add_list_validation("App Groups", "IS_BASE", "IS_BASE")
    _add_list_validation("Invoices", "INVOICE_TYPE", "INVOICE_TYPE")
    _add_list_validation("Invoices", "STATUS", "STATUS")
    _add_list_validation("Contracts", "STATUS", "STATUS")

    # (Rates are managed in-app on the Rates page; intentionally not part of templates.)



# ======================================================================================
# Internal helpers
# ======================================================================================


@dataclass(frozen=True)
class _SheetSpec:
    required_columns: List[str]
    template_columns: List[str]


@dataclass
class _ImportStep:
    name: str
    run: Callable[..., Dict[str, Any]]


def _read_uploaded_bytes(uploaded_file: Any) -> bytes:
    if uploaded_file is None:
        return b""
    try:
        # Streamlit UploadedFile
        if hasattr(uploaded_file, "getvalue"):
            return uploaded_file.getvalue()
        if hasattr(uploaded_file, "read"):
            uploaded_file.seek(0)
            raw = uploaded_file.read()
            try:
                uploaded_file.seek(0)
            except Exception:
                pass
            return raw
    except Exception:
        pass
    if isinstance(uploaded_file, (bytes, bytearray)):
        return bytes(uploaded_file)
    raise TypeError("Unsupported uploaded_file type")


def _canonical_sheet_name(name: str) -> str:
    base = str(name or "").strip()
    key = re.sub(r"\s+", " ", base).strip().casefold()
    return _SHEET_NAME_ALIASES.get(key, base)


def _normalize_mode(mode: str) -> ValidationMode:
    m = str(mode or "").strip().casefold()
    if m in {"master", "master data", "core", "core only"}:
        return "master"
    if m in {"finance", "financial", "finance only"}:
        return "finance"
    return "master"


def _required_sheets_for_mode(mode: ValidationMode) -> List[str]:
    if mode == "master":
        return ["Programs", "Teams", "Vendors", "App Groups", "Applications"]
    return ["Contracts", "Invoices", "Program Addl Costs"]


def _err(sheet: str, row: int, column: str, error: str, fix: str) -> Dict[str, Any]:
    return {"sheet": sheet, "row": int(row), "column": column, "error": error, "suggested_fix": fix}


def _excel_row(df_index: int) -> int:
    # Header is row 1 in Excel; pandas index 0 corresponds to Excel row 2.
    try:
        return int(df_index) + 2
    except Exception:
        return 0


def _normalize_col_label(col: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(col or "").upper().strip())


def _map_columns(actual_cols: List[str], required: List[str]) -> Tuple[Dict[str, str], List[str]]:
    """Return (rename_map, missing_required)."""
    by_norm: Dict[str, str] = {}
    for c in actual_cols:
        n = _normalize_col_label(c)
        if n and n not in by_norm:
            by_norm[n] = c

    rename: Dict[str, str] = {}
    missing: List[str] = []
    for req in required:
        key = _normalize_col_label(req)
        if key in by_norm:
            rename[by_norm[key]] = req
        else:
            missing.append(req)
    return rename, missing


def _normalize_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v)
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def _nk(v: Any) -> Optional[str]:
    s = _normalize_str(v)
    return s.casefold() if s else None


def _to_bool(v: Any) -> int:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0
    if isinstance(v, (int, bool)):
        return 1 if bool(v) else 0
    s = str(v).strip().casefold()
    if s in {"1", "true", "yes", "y", "t"}:
        return 1
    if s in {"0", "false", "no", "n", "f"}:
        return 0
    # Excel can store TRUE/FALSE
    if s == "true":
        return 1
    if s == "false":
        return 0
    return 0


def _to_int(v: Any) -> Optional[int]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int,)):
            return int(v)
        if isinstance(v, float):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _to_float(v: Any) -> Optional[float]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        if isinstance(v, bool):
            return float(int(v))
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return None
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return None


def _parse_excel_date(v: Any) -> Optional[date]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    # Excel serial date (1900 date system)
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            # pandas uses 1899-12-30 as day 0 for Excel serials
            return (pd.Timestamp("1899-12-30") + pd.to_timedelta(float(v), unit="D")).date()
        except Exception:
            return None
    s = _normalize_str(v)
    if not s:
        return None
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


def _normalize_sheet(df: pd.DataFrame, sheet: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    spec = _SHEET_SPECS.get(sheet)
    if not spec:
        return df.copy()
    # Keep both required + optional template columns (so v2 can load all supported fields).
    wanted = list(dict.fromkeys((spec.template_columns or []) + (spec.required_columns or [])))
    colmap, _ = _map_columns(df.columns.tolist(), wanted)
    out = df.rename(columns=colmap).copy()
    # normalize known string natural key fields
    for col in out.columns:
        if col.endswith("NAME") or col.endswith("TYPE") or col.endswith("LOCATION") or col.endswith("CLASS") or col.endswith("OWNER_EMAIL") or col.endswith("OWNER_NAME"):
            out[col] = out[col].apply(_normalize_str)
    for c in ["PROGRAMNAME", "TEAMNAME", "VENDORNAME", "GROUPNAME", "APPLICATIONNAME", "INVOICE_TYPE"]:
        if c in out.columns:
            out[f"{c}_NK"] = out[c].apply(_nk)
    return out


def _validate_required(df: pd.DataFrame, sheet: str, cols: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if df is None or df.empty:
        return out
    for col in cols:
        if col not in df.columns:
            continue
        for idx, v in df[col].items():
            if _normalize_str(v) is None:
                out.append(
                    _err(
                        sheet=sheet,
                        row=_excel_row(idx),
                        column=col,
                        error="Required value is missing",
                        fix=f"Fill in '{col}'.",
                    )
                )
    return out


def _validate_unique(df: pd.DataFrame, sheet: str, col: str, message: str) -> List[Dict[str, Any]]:
    if df is None or df.empty or col not in df.columns:
        return []
    nk_col = f"{col}_NK" if f"{col}_NK" in df.columns else None
    key = nk_col or col
    s = df[key].dropna()
    dup_keys = s[s.duplicated(keep=False)].unique().tolist()
    out: List[Dict[str, Any]] = []
    if not dup_keys:
        return out
    offenders = df[df[key].isin(dup_keys)]
    for idx, r in offenders.iterrows():
        out.append(
            _err(
                sheet=sheet,
                row=_excel_row(idx),
                column=col,
                error=message,
                fix="Remove duplicates or rename values so the key is unique.",
            )
        )
    return out


def _validate_unique_multi(
    df: pd.DataFrame,
    sheet: str,
    key_cols: List[str],
    message: str,
    display_cols: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    for c in key_cols:
        if c not in df.columns:
            return []
    key_df = df[key_cols].copy()
    key_df["_k"] = key_df.astype(str).agg("|".join, axis=1)
    dup_keys = key_df["_k"][key_df["_k"].duplicated(keep=False)].unique().tolist()
    if not dup_keys:
        return []
    out: List[Dict[str, Any]] = []
    offenders = df[key_df["_k"].isin(dup_keys)]
    for idx, _r in offenders.iterrows():
        out.append(
            _err(
                sheet=sheet,
                row=_excel_row(idx),
                column=" + ".join(display_cols or key_cols),
                error=message,
                fix="Remove duplicates so the composite key is unique.",
            )
        )
    return out


def _validate_int_range(
    df: pd.DataFrame,
    sheet: str,
    col: str,
    lo: int,
    hi: int,
    required: bool,
) -> List[Dict[str, Any]]:
    if df is None or df.empty or col not in df.columns:
        return []
    out: List[Dict[str, Any]] = []
    for idx, v in df[col].items():
        iv = _to_int(v)
        if iv is None:
            if required and _normalize_str(v) is not None:
                out.append(_err(sheet, _excel_row(idx), col, "Value must be an integer", f"Enter an integer between {lo} and {hi}."))
            elif required and _normalize_str(v) is None:
                out.append(_err(sheet, _excel_row(idx), col, "Required value is missing", f"Enter an integer between {lo} and {hi}."))
            continue
        if iv < lo or iv > hi:
            out.append(_err(sheet, _excel_row(idx), col, f"Value {iv} is out of range", f"Enter an integer between {lo} and {hi}."))
    return out


def _validate_num(
    df: pd.DataFrame,
    sheet: str,
    col: str,
    required: bool,
    warn_only: bool = False,
) -> List[Dict[str, Any]]:
    if df is None or df.empty or col not in df.columns:
        return []
    out: List[Dict[str, Any]] = []
    for idx, v in df[col].items():
        fv = _to_float(v)
        if fv is None:
            if required and _normalize_str(v) is None:
                out.append(_err(sheet, _excel_row(idx), col, "Required value is missing", f"Enter a numeric value for {col}."))
            elif _normalize_str(v) is not None:
                (out if not warn_only else out).append(_err(sheet, _excel_row(idx), col, "Value must be numeric", "Fix the value so it can be parsed as a number."))
    return out


def _fetch_existing_natural_keys_for_warnings() -> Dict[str, List[str]]:
    cn = _new_connection()
    try:
        out: Dict[str, List[str]] = {}
        for key, sql in (
            ("PROGRAMNAME", f"SELECT PROGRAMNAME FROM {_fq('PROGRAMS')} WHERE PROGRAMNAME IS NOT NULL"),
            ("TEAMNAME", f"SELECT TEAMNAME FROM {_fq('TEAMS')} WHERE TEAMNAME IS NOT NULL"),
            ("VENDORNAME", f"SELECT VENDORNAME FROM {_fq('VENDORS')} WHERE VENDORNAME IS NOT NULL"),
            ("GROUPNAME", f"SELECT GROUPNAME FROM {_fq('APPLICATION_GROUPS')} WHERE GROUPNAME IS NOT NULL"),
            ("APPLICATIONNAME", f"SELECT APPLICATIONNAME FROM {_fq('APPLICATIONS')} WHERE APPLICATIONNAME IS NOT NULL"),
        ):
            try:
                df = pd.read_sql(sql, cn)
                vals = [str(v).strip() for v in df.iloc[:, 0].tolist() if str(v).strip()]
                out[key] = vals
            except Exception:
                out[key] = []
        return out
    finally:
        try:
            cn.close()
        except Exception:
            pass


def _warn_similar_to_existing(
    df: pd.DataFrame,
    sheet: str,
    col: str,
    existing_values: List[str],
    threshold: float = 0.86,
) -> List[Dict[str, Any]]:
    if df is None or df.empty or col not in df.columns or not existing_values:
        return []
    existing_nk = {(_nk(v) or ""): str(v) for v in existing_values if _nk(v)}
    candidates = list(existing_nk.items())
    out: List[Dict[str, Any]] = []
    for idx, v in df[col].items():
        s = _normalize_str(v)
        if not s:
            continue
        s_nk = _nk(s)
        if not s_nk:
            continue
        # Exact match exists: no warning (this is preferred).
        if s_nk in existing_nk:
            continue
        # Fuzzy best match
        best = ("", 0.0)
        for ex_nk, ex_raw in candidates:
            score = SequenceMatcher(a=s_nk, b=ex_nk).ratio()
            if score > best[1]:
                best = (ex_raw, score)
        if best[1] >= threshold:
            out.append(
                _err(
                    sheet=sheet,
                    row=_excel_row(idx),
                    column=col,
                    error=f"Value looks similar to an existing entry: '{best[0]}' ({best[1]*100:.0f}% match)",
                    fix=f"Use the exact existing name '{best[0]}' if it represents the same entity; otherwise confirm this is truly a new entry.",
                )
            )
    return out


def _clean_workbook(dfs: Dict[str, pd.DataFrame], mode: ValidationMode) -> Dict[str, pd.DataFrame]:
    clean: Dict[str, pd.DataFrame] = {}
    for sheet in set(dfs.keys()).union(_SHEET_SPECS.keys()):
        if sheet not in dfs:
            continue
        if not isinstance(dfs[sheet], pd.DataFrame):
            continue
        clean[sheet] = _normalize_sheet(dfs[sheet], sheet)
    # Apply coercions per sheet
    def _coerce_int(df: pd.DataFrame, col: str) -> None:
        if df is not None and not df.empty and col in df.columns:
            df[col] = df[col].apply(_to_int)

    def _coerce_float(df: pd.DataFrame, col: str) -> None:
        if df is not None and not df.empty and col in df.columns:
            df[col] = df[col].apply(_to_float)

    def _coerce_date(df: pd.DataFrame, col: str) -> None:
        if df is not None and not df.empty and col in df.columns:
            df[col] = df[col].apply(_parse_excel_date)

    for col in ["PROGRAMFTE", "PROGRAM_XOM_RATE"]:
        _coerce_float(clean.get("Programs"), col)
    for col in ["TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE", "COSTPERFTE"]:
        _coerce_float(clean.get("Teams"), col)
    for col in ["IS_BASE"]:
        if "App Groups" in clean and col in clean["App Groups"].columns:
            clean["App Groups"][col] = clean["App Groups"][col].apply(_to_bool)
    if mode == "finance":
        for col in ["START_FY", "END_FY", "RENEWAL_MONTH"]:
            _coerce_int(clean.get("Contracts"), col)
        for col in ["ANNUAL_AMOUNT", "ESCALATION_PCT", "TOTAL_CONTRACT_COST"]:
            _coerce_float(clean.get("Contracts"), col)
        for col in ["CONTRACT_RENEWAL_DATE", "INVOICE_RENEWAL_DATE"]:
            _coerce_date(clean.get("Contracts"), col)

        for col in ["FISCAL_YEAR"]:
            _coerce_int(clean.get("Invoices"), col)
        for col in ["AMOUNT", "AMOUNT_NEXT_YEAR", "CONTRACT_DUE"]:
            _coerce_float(clean.get("Invoices"), col)
        for col in ["INVOICEDATE", "RENEWALDATE"]:
            _coerce_date(clean.get("Invoices"), col)
        if "Invoices" in clean and "CONTRACT_ACTIVE" in clean["Invoices"].columns:
            clean["Invoices"]["CONTRACT_ACTIVE"] = clean["Invoices"]["CONTRACT_ACTIVE"].apply(_to_bool)

        for col in ["YEAR", "MONTH"]:
            _coerce_int(clean.get("Program Addl Costs"), col)
        _coerce_float(clean.get("Program Addl Costs"), "AMOUNT")

    return clean


def _plan_against_db(clean: Dict[str, pd.DataFrame], cn: Any, mode: ValidationMode) -> Dict[str, Any]:
    plan: Dict[str, Any] = {"sheets": {}}
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON;")

    # Lookups
    vendors_db = _fetch_keyed(cur, "VENDORS", "VENDORNAME", ["VENDORID", "VENDORNAME"])
    programs_db = _fetch_keyed(cur, "PROGRAMS", "PROGRAMNAME", ["PROGRAMID", "PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE", "PROGRAM_XOM_RATE"])
    teams_db = _fetch_keyed(cur, "TEAMS", "TEAMNAME", ["TEAMID", "TEAMNAME", "PROGRAMID", "TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE", "COSTPERFTE", "PRODUCTOWNER"])
    groups_db = _fetch_keyed(cur, "APPLICATION_GROUPS", "GROUPNAME", ["GROUPID", "GROUPNAME", "TEAMID", "PROGRAMID", "DEFAULT_VENDORID", "OWNER", "IS_BASE"])
    apps_db = _fetch_keyed(cur, "APPLICATIONS", "APPLICATIONNAME", ["APPLICATIONID", "APPLICATIONNAME", "VENDORID", "GROUPID", "ADD_INFO"])

    # Helpful reverse maps
    teamid_by_name = {k: v["TEAMID"] for k, v in teams_db.items()}
    programid_by_name = {k: v["PROGRAMID"] for k, v in programs_db.items()}
    vendorid_by_name = {k: v["VENDORID"] for k, v in vendors_db.items()}
    groupid_by_name = {k: v["GROUPID"] for k, v in groups_db.items()}
    appid_by_name = {k: v["APPLICATIONID"] for k, v in apps_db.items()}

    def _plan_simple(sheet: str, key_col_nk: str, db_map: Dict[str, Dict[str, Any]], compare_cols: List[str]) -> Dict[str, Any]:
        df = clean.get(sheet, pd.DataFrame())
        if df is None or df.empty or key_col_nk not in df.columns:
            return {"rows": 0, "insert": 0, "update": 0, "skip": 0, "sample_insert": pd.DataFrame(), "sample_update": pd.DataFrame(), "sample_skip": pd.DataFrame()}
        ins: List[int] = []
        upd: List[int] = []
        skp: List[int] = []
        for idx, r in df.iterrows():
            k = r.get(key_col_nk)
            if not k:
                continue
            existing = db_map.get(k)
            if not existing:
                ins.append(idx)
                continue
            changed = False
            for c in compare_cols:
                if c not in df.columns:
                    continue
                val = r.get(c)
                if isinstance(val, float) and pd.isna(val):
                    val = None
                if isinstance(existing.get(c), float) and pd.isna(existing.get(c)):
                    existing[c] = None
                if _normalize_str(val) != _normalize_str(existing.get(c)) and val != existing.get(c):
                    changed = True
                    break
            (upd if changed else skp).append(idx)
        return {
            "rows": int(len(df.index)),
            "insert": len(ins),
            "update": len(upd),
            "skip": len(skp),
            "sample_insert": df.loc[ins].head(8) if ins else df.head(0),
            "sample_update": df.loc[upd].head(8) if upd else df.head(0),
            "sample_skip": df.loc[skp].head(8) if skp else df.head(0),
        }

    if mode == "master":
        plan["sheets"]["Vendors"] = _plan_simple("Vendors", "VENDORNAME_NK", vendors_db, ["VENDORNAME"])
        plan["sheets"]["Programs"] = _plan_simple("Programs", "PROGRAMNAME_NK", programs_db, ["PROGRAMOWNER", "PROGRAMFTE", "PROGRAM_XOM_RATE"])
        plan["sheets"]["Teams"] = _plan_simple("Teams", "TEAMNAME_NK", teams_db, ["PROGRAMNAME", "TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE", "COSTPERFTE", "PRODUCTOWNER"])
        plan["sheets"]["App Groups"] = _plan_simple("App Groups", "GROUPNAME_NK", groups_db, ["TEAMNAME", "DEFAULT_VENDORNAME", "OWNER", "IS_BASE"])
        plan["sheets"]["Applications"] = _plan_simple("Applications", "APPLICATIONNAME_NK", apps_db, ["VENDORNAME", "GROUPNAME", "ADD_INFO"])

        # Composite-key plan
        if "Group-Team Links" in clean and not clean["Group-Team Links"].empty:
            df = clean["Group-Team Links"].copy()
            df["TEAMNAME_NK"] = df.get("TEAMNAME").apply(_nk) if "TEAMNAME" in df.columns else None
            df["GROUPNAME_NK"] = df.get("GROUPNAME").apply(_nk) if "GROUPNAME" in df.columns else None
            df["_k"] = df[["GROUPNAME_NK", "TEAMNAME_NK"]].astype(str).agg("|".join, axis=1)
            links_db = _fetch_links(cur)
            ins = df[~df["_k"].isin(links_db)].index.tolist()
            skp = df[df["_k"].isin(links_db)].index.tolist()
            plan["sheets"]["Group-Team Links"] = {
                "rows": int(len(df.index)),
                "insert": len(ins),
                "update": 0,
                "skip": len(skp),
                "sample_insert": df.loc[ins].head(8) if ins else df.head(0),
                "sample_update": df.head(0),
                "sample_skip": df.loc[skp].head(8) if skp else df.head(0),
            }

    if mode == "finance":
        # Contracts plan
        dfc = clean.get("Contracts", pd.DataFrame())
        if dfc is not None and not dfc.empty:
            contracts_db = _fetch_contracts_by_app_team(cur)
            ins: List[int] = []
            upd: List[int] = []
            skp: List[int] = []
            for idx, r in dfc.iterrows():
                app_nk = r.get("APPLICATIONNAME_NK")
                team_nk = r.get("TEAMNAME_NK")
                if not app_nk or not team_nk:
                    continue
                app_id = appid_by_name.get(app_nk)
                team_id = teamid_by_name.get(team_nk)
                if not app_id or not team_id:
                    ins.append(idx)
                    continue
                k = f"{team_id}|{app_id}"
                existing = contracts_db.get(k)
                if not existing:
                    ins.append(idx)
                    continue
                changed = False
                for c in ["START_FY", "END_FY", "RENEWAL_MONTH", "ANNUAL_AMOUNT", "ESCALATION_PCT", "STATUS", "AGREEMENT_NUMBER", "COMPANY_CODE", "COST_CENTER", "SERVICE_TYPE", "CONTRACT_RENEWAL_DATE", "INVOICE_RENEWAL_DATE", "TOTAL_CONTRACT_COST"]:
                    if c not in dfc.columns:
                        continue
                    if _normalize_str(r.get(c)) != _normalize_str(existing.get(c)) and r.get(c) != existing.get(c):
                        changed = True
                        break
                (upd if changed else skp).append(idx)
            plan["sheets"]["Contracts"] = {
                "rows": int(len(dfc.index)),
                "insert": len(ins),
                "update": len(upd),
                "skip": len(skp),
                "sample_insert": dfc.loc[ins].head(8) if ins else dfc.head(0),
                "sample_update": dfc.loc[upd].head(8) if upd else dfc.head(0),
                "sample_skip": dfc.loc[skp].head(8) if skp else dfc.head(0),
            }

        # Invoices plan
        dfi = clean.get("Invoices", pd.DataFrame())
        if dfi is not None and not dfi.empty:
            inv_db = _fetch_invoices_by_key(cur)
            ins: List[int] = []
            upd: List[int] = []
            skp: List[int] = []
            for idx, r in dfi.iterrows():
                app_nk = r.get("APPLICATIONNAME_NK")
                team_nk = r.get("TEAMNAME_NK")
                fy = r.get("FISCAL_YEAR")
                it = _nk(r.get("INVOICE_TYPE")) or "recurring invoice"
                if not app_nk or not team_nk or fy is None:
                    continue
                app_id = appid_by_name.get(app_nk)
                team_id = teamid_by_name.get(team_nk)
                if not app_id or not team_id:
                    ins.append(idx)
                    continue
                k = f"{team_id}|{app_id}|{int(fy)}|{it}"
                existing = inv_db.get(k)
                if not existing:
                    ins.append(idx)
                    continue
                if str(existing.get("STATUS") or "").strip().casefold() == "completed":
                    skp.append(idx)
                    continue
                changed = False
                for c in ["AMOUNT", "STATUS", "RENEWALDATE", "INVOICEDATE", "PRODUCT_OWNER", "AMOUNT_NEXT_YEAR", "CONTRACT_ACTIVE", "COMPANY_CODE", "COST_CENTER", "SERIAL_NUMBER", "WORK_ORDER", "AGREEMENT_NUMBER", "CONTRACT_DUE", "SERVICE_TYPE", "NOTES"]:
                    if c in dfi.columns and (_normalize_str(r.get(c)) != _normalize_str(existing.get(c)) and r.get(c) != existing.get(c)):
                        changed = True
                        break
                (upd if changed else skp).append(idx)
            plan["sheets"]["Invoices"] = {
                "rows": int(len(dfi.index)),
                "insert": len(ins),
                "update": len(upd),
                "skip": len(skp),
                "sample_insert": dfi.loc[ins].head(8) if ins else dfi.head(0),
                "sample_update": dfi.loc[upd].head(8) if upd else dfi.head(0),
                "sample_skip": dfi.loc[skp].head(8) if skp else dfi.head(0),
            }

        # Program Addl Costs plan
        dfp = clean.get("Program Addl Costs", pd.DataFrame())
        if dfp is not None and not dfp.empty:
            pac_db = _fetch_program_addl_costs(cur)
            ins: List[int] = []
            upd: List[int] = []
            skp: List[int] = []
            for idx, r in dfp.iterrows():
                prog_nk = r.get("PROGRAMNAME_NK")
                y = r.get("YEAR")
                m = r.get("MONTH")
                ct = _nk(r.get("COST_TYPE"))
                st = _nk(r.get("SUBTYPE") or "")
                if not prog_nk or y is None or m is None or not ct:
                    continue
                prog_id = programid_by_name.get(prog_nk)
                if not prog_id:
                    ins.append(idx)
                    continue
                k = f"{prog_id}|{int(y)}|{int(m)}|{ct}|{st}"
                existing = pac_db.get(k)
                if not existing:
                    ins.append(idx)
                    continue
                amt = _to_float(r.get("AMOUNT"))
                if amt != _to_float(existing.get("AMOUNT")):
                    upd.append(idx)
                else:
                    skp.append(idx)
            plan["sheets"]["Program Addl Costs"] = {
                "rows": int(len(dfp.index)),
                "insert": len(ins),
                "update": len(upd),
                "skip": len(skp),
                "sample_insert": dfp.loc[ins].head(8) if ins else dfp.head(0),
                "sample_update": dfp.loc[upd].head(8) if upd else dfp.head(0),
                "sample_skip": dfp.loc[skp].head(8) if skp else dfp.head(0),
            }

    return plan


def _fetch_keyed(cur: Any, table: str, key_col: str, cols: List[str]) -> Dict[str, Dict[str, Any]]:
    df = pd.read_sql(f"SELECT {', '.join(cols)} FROM {_fq(table)}", cur.connection)
    out: Dict[str, Dict[str, Any]] = {}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        k = _nk(r.get(key_col))
        if not k:
            continue
        out[k] = {c: r.get(c) for c in cols}
    return out


def _fetch_links(cur: Any) -> set[str]:
    df = pd.read_sql(f"SELECT GROUPID, TEAMID FROM {_fq('APP_GROUP_TEAM_LINKS')}", cur.connection)
    if df is None or df.empty:
        return set()
    return {f"{str(r['GROUPID'])}|{str(r['TEAMID'])}" for _, r in df.iterrows()}


def _fetch_contracts_by_app_team(cur: Any) -> Dict[str, Dict[str, Any]]:
    df = pd.read_sql(
        f"SELECT CONTRACT_ID, TEAMID, APPLICATIONID, START_FY, END_FY, RENEWAL_MONTH, ANNUAL_AMOUNT, ESCALATION_PCT, STATUS, AGREEMENT_NUMBER, COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE, TOTAL_CONTRACT_COST FROM {_fq('CONTRACTS')}",
        cur.connection,
    )
    out: Dict[str, Dict[str, Any]] = {}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        k = f"{str(r['TEAMID'])}|{str(r['APPLICATIONID'])}"
        out[k] = r.to_dict()
    return out


def _fetch_invoices_by_key(cur: Any) -> Dict[str, Dict[str, Any]]:
    df = pd.read_sql(
        f"SELECT APPLICATIONID, TEAMID, FISCAL_YEAR, INVOICE_TYPE, STATUS, AMOUNT, RENEWALDATE, INVOICEDATE, PRODUCT_OWNER, AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE, COMPANY_CODE, COST_CENTER, SERIAL_NUMBER, WORK_ORDER, AGREEMENT_NUMBER, CONTRACT_DUE, SERVICE_TYPE, NOTES FROM {_fq('INVOICES')}",
        cur.connection,
    )
    out: Dict[str, Dict[str, Any]] = {}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        it = _nk(r.get("INVOICE_TYPE")) or "recurring invoice"
        k = f"{str(r['TEAMID'])}|{str(r['APPLICATIONID'])}|{int(r['FISCAL_YEAR'])}|{it}"
        out[k] = r.to_dict()
    return out


def _fetch_program_addl_costs(cur: Any) -> Dict[str, Dict[str, Any]]:
    df = pd.read_sql(
        f"""
        SELECT
          PROGRAMID,
          YEAR,
          MONTH,
          COST_TYPE,
          SUBTYPE,
          DESCRIPTION,
          CURRENCY,
          IS_RECURRING,
          AMOUNT,
          UPDATED_AT,
          UPDATED_BY
        FROM {_fq('PROGRAM_ADDITIONAL_COSTS')}
        """,
        cur.connection,
    )
    out: Dict[str, Dict[str, Any]] = {}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        k = f"{str(r['PROGRAMID'])}|{int(r['YEAR'])}|{int(r['MONTH'])}|{_nk(r.get('COST_TYPE'))}|{_nk(r.get('SUBTYPE') or '')}"
        out[k] = r.to_dict()
    return out


def _build_steps_from_plan(plan: Dict[str, Any], mode: ValidationMode) -> List[_ImportStep]:
    data: Dict[str, pd.DataFrame] = plan.get("data") or {}

    # Keep strict dependency order
    if mode == "finance":
        return [
            _ImportStep("Contracts", lambda cur, user_email=None: _upsert_contracts(cur, data.get("Contracts", pd.DataFrame()), user_email)),
            _ImportStep("Sync Planned Invoices", lambda cur, user_email=None: _sync_planned_invoices_for_workbook_contracts(cur, data.get("Contracts", pd.DataFrame()), user_email)),
            _ImportStep("Invoices", lambda cur, user_email=None: _upsert_invoices(cur, data.get("Invoices", pd.DataFrame()), user_email)),
            _ImportStep("Program Additional Costs", lambda cur, user_email=None: _upsert_program_addl_costs(cur, data.get("Program Addl Costs", pd.DataFrame()), user_email)),
        ]

    # master
    return [
        _ImportStep("Vendors", lambda cur, user_email=None: _upsert_vendors(cur, data.get("Vendors", pd.DataFrame()), user_email)),
        _ImportStep("Programs", lambda cur, user_email=None: _upsert_programs(cur, data.get("Programs", pd.DataFrame()), user_email)),
        _ImportStep("Teams", lambda cur, user_email=None: _upsert_teams(cur, data.get("Teams", pd.DataFrame()), user_email)),
        _ImportStep("App Groups", lambda cur, user_email=None: _upsert_app_groups(cur, data.get("App Groups", pd.DataFrame()), user_email)),
        _ImportStep("Applications", lambda cur, user_email=None: _upsert_applications(cur, data.get("Applications", pd.DataFrame()), user_email)),
        _ImportStep("Group-Team Links", lambda cur, user_email=None: _upsert_group_team_links(cur, data.get("Group-Team Links", pd.DataFrame()), user_email)),
    ]


def _create_temp(cur: Any, name: str, cols: List[Tuple[str, str]]) -> None:
    ddl_cols = ", ".join([f"[{c}] {t}" for c, t in cols])
    cur.execute(f"IF OBJECT_ID('tempdb..{name}') IS NOT NULL DROP TABLE {name}; CREATE TABLE {name} ({ddl_cols});")


def _insert_temp(cur: Any, name: str, cols: List[str], rows: List[Tuple[Any, ...]]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join([f"[{c}]" for c in cols])
    sql = f"INSERT INTO {name} ({col_list}) VALUES ({placeholders})"
    cur.fast_executemany = True  # type: ignore[attr-defined]
    cur.executemany(sql, rows)


def _merge_counts(cur: Any, merge_sql: str) -> Dict[str, int]:
    cur.execute("IF OBJECT_ID('tempdb..#merge_actions') IS NOT NULL DROP TABLE #merge_actions; CREATE TABLE #merge_actions(action NVARCHAR(10));")
    cur.execute(f"{merge_sql} OUTPUT $action INTO #merge_actions;")
    cur.execute("SELECT action, COUNT(*) AS CNT FROM #merge_actions GROUP BY action;")
    rows = cur.fetchall()
    counts = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
    for action, cnt in rows:
        counts[str(action).upper()] = int(cnt)
    return counts


def _upsert_vendors(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Vendors", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        vn = _normalize_str(r.get("VENDORNAME"))
        if not vn:
            continue
        rows.append((vn, user_email))
    _create_temp(cur, "#stg_vendors", [("VENDORNAME", "NVARCHAR(255)"), ("UPDATED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_vendors", ["VENDORNAME", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('VENDORS')} AS t
      USING (SELECT DISTINCT VENDORNAME, UPDATED_BY FROM #stg_vendors) AS s
      ON t.VENDORNAME = s.VENDORNAME
      WHEN MATCHED THEN UPDATE SET
        UPDATED_AT = SYSDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (VENDORID, VENDORNAME, CREATED_AT, UPDATED_AT, UPDATED_BY)
      VALUES (CONVERT(NVARCHAR(36), NEWID()), s.VENDORNAME, SYSDATETIME(), SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Vendors", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_programs(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Programs", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        pn = _normalize_str(r.get("PROGRAMNAME"))
        if not pn:
            continue
        rows.append(
            (
                pn,
                _normalize_str(r.get("PROGRAMOWNER")),
                _to_float(r.get("PROGRAMFTE")),
                _to_float(r.get("PROGRAM_XOM_RATE")),
                user_email,
            )
        )
    _create_temp(
        cur,
        "#stg_programs",
        [
            ("PROGRAMNAME", "NVARCHAR(255)"),
            ("PROGRAMOWNER", "NVARCHAR(255)"),
            ("PROGRAMFTE", "FLOAT"),
            ("PROGRAM_XOM_RATE", "FLOAT"),
            ("UPDATED_BY", "NVARCHAR(255)"),
        ],
    )
    _insert_temp(cur, "#stg_programs", ["PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE", "PROGRAM_XOM_RATE", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('PROGRAMS')} AS t
      USING (SELECT DISTINCT PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE, PROGRAM_XOM_RATE, UPDATED_BY FROM #stg_programs) AS s
      ON t.PROGRAMNAME = s.PROGRAMNAME
      WHEN MATCHED THEN UPDATE SET
        PROGRAMOWNER = s.PROGRAMOWNER,
        PROGRAMFTE = s.PROGRAMFTE,
        PROGRAM_XOM_RATE = s.PROGRAM_XOM_RATE,
        UPDATED_AT = SYSDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE, PROGRAM_XOM_RATE, CREATED_AT, UPDATED_AT, UPDATED_BY)
      VALUES (CONVERT(NVARCHAR(36), NEWID()), s.PROGRAMNAME, s.PROGRAMOWNER, s.PROGRAMFTE, s.PROGRAM_XOM_RATE, SYSDATETIME(), SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Programs", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_teams(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Teams", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        tn = _normalize_str(r.get("TEAMNAME"))
        pn = _normalize_str(r.get("PROGRAMNAME"))
        if not tn or not pn:
            continue
        rows.append(
            (
                tn,
                pn,
                _to_float(r.get("TEAMFTE")),
                _to_float(r.get("DELIVERY_TEAM_FTE")),
                _to_float(r.get("CONTRACTOR_C_FTE")),
                _to_float(r.get("CONTRACTOR_CS_FTE")),
                _to_float(r.get("COSTPERFTE")),
                _normalize_str(r.get("PRODUCTOWNER")),
                user_email,
            )
        )
    _create_temp(
        cur,
        "#stg_teams",
        [
            ("TEAMNAME", "NVARCHAR(255)"),
            ("PROGRAMNAME", "NVARCHAR(255)"),
            ("TEAMFTE", "DECIMAL(18,2)"),
            ("DELIVERY_TEAM_FTE", "DECIMAL(18,2)"),
            ("CONTRACTOR_C_FTE", "DECIMAL(18,2)"),
            ("CONTRACTOR_CS_FTE", "DECIMAL(18,2)"),
            ("COSTPERFTE", "FLOAT"),
            ("PRODUCTOWNER", "NVARCHAR(255)"),
            ("UPDATED_BY", "NVARCHAR(255)"),
        ],
    )
    _insert_temp(
        cur,
        "#stg_teams",
        ["TEAMNAME", "PROGRAMNAME", "TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE", "COSTPERFTE", "PRODUCTOWNER", "UPDATED_BY"],
        rows,
    )
    merge_sql = f"""
      MERGE INTO {_fq('TEAMS')} AS t
      USING (
        SELECT s.TEAMNAME, p.PROGRAMID, s.TEAMFTE, s.DELIVERY_TEAM_FTE, s.CONTRACTOR_C_FTE, s.CONTRACTOR_CS_FTE, s.COSTPERFTE, s.PRODUCTOWNER, s.UPDATED_BY
        FROM #stg_teams s
        JOIN {_fq('PROGRAMS')} p ON p.PROGRAMNAME = s.PROGRAMNAME
      ) AS s
      ON t.TEAMNAME = s.TEAMNAME
      WHEN MATCHED THEN UPDATE SET
        PROGRAMID = s.PROGRAMID,
        TEAMFTE = s.TEAMFTE,
        DELIVERY_TEAM_FTE = s.DELIVERY_TEAM_FTE,
        CONTRACTOR_C_FTE = s.CONTRACTOR_C_FTE,
        CONTRACTOR_CS_FTE = s.CONTRACTOR_CS_FTE,
        COSTPERFTE = s.COSTPERFTE,
        PRODUCTOWNER = s.PRODUCTOWNER,
        UPDATED_AT = SYSDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (TEAMID, TEAMNAME, PROGRAMID, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE, COSTPERFTE, PRODUCTOWNER, CREATED_AT, UPDATED_AT, UPDATED_BY)
      VALUES (CONVERT(NVARCHAR(36), NEWID()), s.TEAMNAME, s.PROGRAMID, s.TEAMFTE, s.DELIVERY_TEAM_FTE, s.CONTRACTOR_C_FTE, s.CONTRACTOR_CS_FTE, s.COSTPERFTE, s.PRODUCTOWNER, SYSDATETIME(), SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Teams", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_app_groups(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "App Groups", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        gn = _normalize_str(r.get("GROUPNAME"))
        tn = _normalize_str(r.get("TEAMNAME"))
        if not gn or not tn:
            continue
        rows.append(
            (
                gn,
                tn,
                _normalize_str(r.get("DEFAULT_VENDORNAME")),
                _normalize_str(r.get("OWNER")),
                int(_to_bool(r.get("IS_BASE"))),
                user_email,
            )
        )
    _create_temp(
        cur,
        "#stg_groups",
        [
            ("GROUPNAME", "NVARCHAR(255)"),
            ("TEAMNAME", "NVARCHAR(255)"),
            ("DEFAULT_VENDORNAME", "NVARCHAR(255)"),
            ("OWNER", "NVARCHAR(255)"),
            ("IS_BASE", "BIT"),
            ("UPDATED_BY", "NVARCHAR(255)"),
        ],
    )
    _insert_temp(cur, "#stg_groups", ["GROUPNAME", "TEAMNAME", "DEFAULT_VENDORNAME", "OWNER", "IS_BASE", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('APPLICATION_GROUPS')} AS t
      USING (
        SELECT
          s.GROUPNAME,
          tm.TEAMID,
          tm.PROGRAMID,
          v.VENDORID AS DEFAULT_VENDORID,
          s.OWNER,
          s.IS_BASE,
          s.UPDATED_BY
        FROM #stg_groups s
        JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
        LEFT JOIN {_fq('VENDORS')} v ON v.VENDORNAME = s.DEFAULT_VENDORNAME
      ) AS s
      ON t.GROUPNAME = s.GROUPNAME
      WHEN MATCHED THEN UPDATE SET
        TEAMID = s.TEAMID,
        PROGRAMID = s.PROGRAMID,
        DEFAULT_VENDORID = s.DEFAULT_VENDORID,
        OWNER = s.OWNER,
        IS_BASE = s.IS_BASE,
        UPDATED_AT = SYSDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (GROUPID, GROUPNAME, DEFAULT_VENDORID, OWNER, TEAMID, PROGRAMID, IS_BASE, CREATED_AT, UPDATED_AT, UPDATED_BY)
      VALUES (CONVERT(NVARCHAR(36), NEWID()), s.GROUPNAME, s.DEFAULT_VENDORID, s.OWNER, s.TEAMID, s.PROGRAMID, s.IS_BASE, SYSDATETIME(), SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "App Groups", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_applications(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Applications", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        an = _normalize_str(r.get("APPLICATIONNAME"))
        if not an:
            continue
        rows.append(
            (
                an,
                _normalize_str(r.get("VENDORNAME")),
                _normalize_str(r.get("GROUPNAME")),
                _normalize_str(r.get("ADD_INFO")),
                user_email,
            )
        )
    _create_temp(
        cur,
        "#stg_apps",
        [
            ("APPLICATIONNAME", "NVARCHAR(255)"),
            ("VENDORNAME", "NVARCHAR(255)"),
            ("GROUPNAME", "NVARCHAR(255)"),
            ("ADD_INFO", "NVARCHAR(MAX)"),
            ("UPDATED_BY", "NVARCHAR(255)"),
        ],
    )
    _insert_temp(cur, "#stg_apps", ["APPLICATIONNAME", "VENDORNAME", "GROUPNAME", "ADD_INFO", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('APPLICATIONS')} AS t
      USING (
        SELECT
          s.APPLICATIONNAME,
          v.VENDORID,
          g.GROUPID,
          s.ADD_INFO,
          s.UPDATED_BY
        FROM #stg_apps s
        LEFT JOIN {_fq('VENDORS')} v ON v.VENDORNAME = s.VENDORNAME
        LEFT JOIN {_fq('APPLICATION_GROUPS')} g ON g.GROUPNAME = s.GROUPNAME
      ) AS s
      ON t.APPLICATIONNAME = s.APPLICATIONNAME
      WHEN MATCHED THEN UPDATE SET
        VENDORID = s.VENDORID,
        GROUPID = s.GROUPID,
        ADD_INFO = s.ADD_INFO,
        UPDATED_AT = SYSDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (APPLICATIONID, APPLICATIONNAME, VENDORID, GROUPID, ADD_INFO, CREATED_AT, UPDATED_AT, UPDATED_BY)
      VALUES (CONVERT(NVARCHAR(36), NEWID()), s.APPLICATIONNAME, s.VENDORID, s.GROUPID, s.ADD_INFO, SYSDATETIME(), SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Applications", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_group_team_links(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Group-Team Links", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        gn = _normalize_str(r.get("GROUPNAME"))
        tn = _normalize_str(r.get("TEAMNAME"))
        if not gn or not tn:
            continue
        rows.append((gn, tn))
    _create_temp(cur, "#stg_gtl", [("GROUPNAME", "NVARCHAR(255)"), ("TEAMNAME", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_gtl", ["GROUPNAME", "TEAMNAME"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('APP_GROUP_TEAM_LINKS')} AS t
      USING (
        SELECT DISTINCT g.GROUPID, tm.TEAMID
        FROM #stg_gtl s
        JOIN {_fq('APPLICATION_GROUPS')} g ON g.GROUPNAME = s.GROUPNAME
        JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
      ) AS s
      ON t.GROUPID = s.GROUPID AND t.TEAMID = s.TEAMID
      WHEN NOT MATCHED THEN INSERT (GROUPID, TEAMID, CREATED_AT)
      VALUES (s.GROUPID, s.TEAMID, SYSDATETIME())
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Group-Team Links", "inserted": c["INSERT"], "updated": 0, "skipped": max(0, len(rows) - c["INSERT"])}


def _upsert_contracts(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Contracts", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        tn = _normalize_str(r.get("TEAMNAME"))
        an = _normalize_str(r.get("APPLICATIONNAME"))
        if not tn or not an:
            continue
        rows.append(
            (
                tn,
                an,
                int(r.get("START_FY") or 0),
                int(r.get("END_FY") or 0),
                int(r.get("RENEWAL_MONTH") or 1),
                _to_float(r.get("ANNUAL_AMOUNT")) or 0.0,
                _to_float(r.get("ESCALATION_PCT")) or 0.0,
                _normalize_str(r.get("STATUS")) or "Active",
                _normalize_str(r.get("AGREEMENT_NUMBER")),
                _normalize_str(r.get("COMPANY_CODE")),
                _normalize_str(r.get("COST_CENTER")),
                _normalize_str(r.get("SERVICE_TYPE")),
                r.get("CONTRACT_RENEWAL_DATE").isoformat() if isinstance(r.get("CONTRACT_RENEWAL_DATE"), date) else None,
                r.get("INVOICE_RENEWAL_DATE").isoformat() if isinstance(r.get("INVOICE_RENEWAL_DATE"), date) else None,
                _to_float(r.get("TOTAL_CONTRACT_COST")),
                user_email,
            )
        )
    _create_temp(
        cur,
        "#stg_contracts",
        [
            ("TEAMNAME", "NVARCHAR(255)"),
            ("APPLICATIONNAME", "NVARCHAR(255)"),
            ("START_FY", "INT"),
            ("END_FY", "INT"),
            ("RENEWAL_MONTH", "TINYINT"),
            ("ANNUAL_AMOUNT", "DECIMAL(18,2)"),
            ("ESCALATION_PCT", "DECIMAL(9,4)"),
            ("STATUS", "NVARCHAR(32)"),
            ("AGREEMENT_NUMBER", "NVARCHAR(255)"),
            ("COMPANY_CODE", "NVARCHAR(255)"),
            ("COST_CENTER", "NVARCHAR(255)"),
            ("SERVICE_TYPE", "NVARCHAR(255)"),
            ("CONTRACT_RENEWAL_DATE", "DATE"),
            ("INVOICE_RENEWAL_DATE", "DATE"),
            ("TOTAL_CONTRACT_COST", "DECIMAL(18,2)"),
            ("UPDATED_BY", "NVARCHAR(255)"),
        ],
    )
    _insert_temp(
        cur,
        "#stg_contracts",
        [
            "TEAMNAME",
            "APPLICATIONNAME",
            "START_FY",
            "END_FY",
            "RENEWAL_MONTH",
            "ANNUAL_AMOUNT",
            "ESCALATION_PCT",
            "STATUS",
            "AGREEMENT_NUMBER",
            "COMPANY_CODE",
            "COST_CENTER",
            "SERVICE_TYPE",
            "CONTRACT_RENEWAL_DATE",
            "INVOICE_RENEWAL_DATE",
            "TOTAL_CONTRACT_COST",
            "UPDATED_BY",
        ],
        rows,
    )
    merge_sql = f"""
      MERGE INTO {_fq('CONTRACTS')} AS t
      USING (
        SELECT
          a.APPLICATIONID,
          tm.TEAMID,
          s.START_FY,
          s.END_FY,
          NULLIF(s.RENEWAL_MONTH,0) AS RENEWAL_MONTH,
          s.ANNUAL_AMOUNT,
          s.ESCALATION_PCT,
          s.STATUS,
          s.AGREEMENT_NUMBER,
          s.COMPANY_CODE,
          s.COST_CENTER,
          s.SERVICE_TYPE,
          s.CONTRACT_RENEWAL_DATE,
          s.INVOICE_RENEWAL_DATE,
          s.TOTAL_CONTRACT_COST,
          s.UPDATED_BY
        FROM #stg_contracts s
        JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
        JOIN {_fq('APPLICATIONS')} a ON a.APPLICATIONNAME = s.APPLICATIONNAME
      ) AS s
      ON t.APPLICATIONID = s.APPLICATIONID AND t.TEAMID = s.TEAMID
      WHEN MATCHED THEN UPDATE SET
        START_FY = s.START_FY,
        END_FY = s.END_FY,
        RENEWAL_MONTH = s.RENEWAL_MONTH,
        ANNUAL_AMOUNT = s.ANNUAL_AMOUNT,
        ESCALATION_PCT = s.ESCALATION_PCT,
        STATUS = s.STATUS,
        AGREEMENT_NUMBER = s.AGREEMENT_NUMBER,
        COMPANY_CODE = s.COMPANY_CODE,
        COST_CENTER = s.COST_CENTER,
        SERVICE_TYPE = s.SERVICE_TYPE,
        CONTRACT_RENEWAL_DATE = s.CONTRACT_RENEWAL_DATE,
        INVOICE_RENEWAL_DATE = s.INVOICE_RENEWAL_DATE,
        TOTAL_CONTRACT_COST = s.TOTAL_CONTRACT_COST,
        UPDATED_AT = SYSDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (
        CONTRACT_ID, APPLICATIONID, TEAMID, START_FY, END_FY, RENEWAL_MONTH,
        ANNUAL_AMOUNT, ESCALATION_PCT, STATUS, AGREEMENT_NUMBER,
        COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE, TOTAL_CONTRACT_COST,
        CREATED_AT, UPDATED_AT, UPDATED_BY
      ) VALUES (
        CONVERT(NVARCHAR(36), NEWID()), s.APPLICATIONID, s.TEAMID, s.START_FY, s.END_FY, s.RENEWAL_MONTH,
        s.ANNUAL_AMOUNT, s.ESCALATION_PCT, s.STATUS, s.AGREEMENT_NUMBER,
        s.COMPANY_CODE, s.COST_CENTER, s.SERVICE_TYPE, s.CONTRACT_RENEWAL_DATE, s.INVOICE_RENEWAL_DATE, s.TOTAL_CONTRACT_COST,
        SYSDATETIME(), SYSDATETIME(), s.UPDATED_BY
      )
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Contracts", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _sync_planned_invoices_for_workbook_contracts(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Sync Planned Invoices", "inserted": 0, "updated": 0, "skipped": 0}
    # Fetch CONTRACT_IDs for the workbook contracts by (TEAMNAME, APPLICATIONNAME)
    pairs = []
    for _, r in df.iterrows():
        tn = _normalize_str(r.get("TEAMNAME"))
        an = _normalize_str(r.get("APPLICATIONNAME"))
        if tn and an:
            pairs.append((tn, an))
    if not pairs:
        return {"step": "Sync Planned Invoices", "inserted": 0, "updated": 0, "skipped": 0}

    _create_temp(cur, "#stg_contract_pairs", [("TEAMNAME", "NVARCHAR(255)"), ("APPLICATIONNAME", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_contract_pairs", ["TEAMNAME", "APPLICATIONNAME"], pairs)
    cur.execute(
        f"""
        SELECT DISTINCT c.CONTRACT_ID
        FROM #stg_contract_pairs p
        JOIN {_fq('TEAMS')} t ON t.TEAMNAME = p.TEAMNAME
        JOIN {_fq('APPLICATIONS')} a ON a.APPLICATIONNAME = p.APPLICATIONNAME
        JOIN {_fq('CONTRACTS')} c ON c.TEAMID = t.TEAMID AND c.APPLICATIONID = a.APPLICATIONID
        """
    )
    contract_ids = [str(r[0]) for r in cur.fetchall() if r and r[0]]
    inserted = 0
    updated = 0
    for cid in contract_ids:
        ins, upd = _sync_contract_invoices_tx(cur, cid)
        inserted += ins
        updated += upd
    return {"step": "Sync Planned Invoices", "inserted": inserted, "updated": updated, "skipped": 0}


def _sync_contract_invoices_tx(cur: Any, contract_id: str) -> Tuple[int, int]:
    """Transaction-safe version of db.sync_contract_invoices for a single contract_id."""
    # Insert/update planned recurring invoices
    merge_sql = f"""
    WITH c AS (
      SELECT CONTRACT_ID, APPLICATIONID, TEAMID, START_FY, END_FY,
             ISNULL(NULLIF(RENEWAL_MONTH,0), 1) AS RENEWAL_MONTH,
             ANNUAL_AMOUNT, ISNULL(ESCALATION_PCT,0) AS ESCALATION_PCT,
             UPPER(ISNULL(STATUS,'Active')) AS STATUS,
             AGREEMENT_NUMBER, COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE
      FROM {_fq('CONTRACTS')}
      WHERE CONTRACT_ID = ?
    ), yrs (CONTRACT_ID, FY) AS (
      SELECT c.CONTRACT_ID, c.START_FY FROM c
      UNION ALL
      SELECT y.CONTRACT_ID, y.FY + 1 FROM yrs y
      JOIN c ON c.CONTRACT_ID = y.CONTRACT_ID
      WHERE y.FY < c.END_FY
    ), rows AS (
      SELECT c.CONTRACT_ID, c.APPLICATIONID, c.TEAMID, y.FY,
             c.END_FY AS CONTRACT_DUE_FY,
             CAST(ROUND(c.ANNUAL_AMOUNT * POWER(1 + (c.ESCALATION_PCT/100.0), y.FY - c.START_FY), 2) AS DECIMAL(18,2)) AS AMT,
             DATEFROMPARTS(y.FY,
               COALESCE(MONTH(c.INVOICE_RENEWAL_DATE), MONTH(c.CONTRACT_RENEWAL_DATE), c.RENEWAL_MONTH, 1),
               COALESCE(DAY(c.INVOICE_RENEWAL_DATE), DAY(c.CONTRACT_RENEWAL_DATE), 1)
             ) AS RENEWALDATE,
             CASE WHEN c.STATUS = 'ACTIVE' THEN 1 ELSE 0 END AS CONTRACT_ACTIVE,
             c.AGREEMENT_NUMBER, c.COMPANY_CODE, c.COST_CENTER, c.SERVICE_TYPE
      FROM yrs y JOIN c ON c.CONTRACT_ID = y.CONTRACT_ID
    )
    MERGE INTO {_fq('INVOICES')} AS t
    USING (
      SELECT
        APPLICATIONID, TEAMID, FY AS FISCAL_YEAR,
        RENEWALDATE, AMT, CONTRACT_ACTIVE,
        CONTRACT_DUE_FY AS CONTRACT_DUE,
        AGREEMENT_NUMBER, COMPANY_CODE, COST_CENTER, SERVICE_TYPE
      FROM rows
    ) AS s
    ON t.APPLICATIONID = s.APPLICATIONID AND t.TEAMID = s.TEAMID AND t.FISCAL_YEAR = s.FISCAL_YEAR
       AND ISNULL(t.INVOICE_TYPE, 'Recurring Invoice') = 'Recurring Invoice'
    WHEN MATCHED AND t.STATUS = 'Planned' THEN UPDATE SET
      t.RENEWALDATE = s.RENEWALDATE,
      t.AMOUNT = s.AMT,
      t.AMOUNT_NEXT_YEAR = NULL,
      t.CONTRACT_ACTIVE = s.CONTRACT_ACTIVE,
      t.COMPANY_CODE = s.COMPANY_CODE,
      t.COST_CENTER = s.COST_CENTER,
      t.SERVICE_TYPE = s.SERVICE_TYPE,
      t.CONTRACT_DUE = s.CONTRACT_DUE,
      t.AGREEMENT_NUMBER = s.AGREEMENT_NUMBER,
      t.INVOICE_TYPE = 'Recurring Invoice'
    WHEN NOT MATCHED THEN INSERT (
      INVOICEID, APPLICATIONID, TEAMID, RENEWALDATE, AMOUNT, STATUS, FISCAL_YEAR,
      PRODUCT_OWNER, AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE, COMPANY_CODE, COST_CENTER,
      SERIAL_NUMBER, WORK_ORDER, AGREEMENT_NUMBER, CONTRACT_DUE, SERVICE_TYPE, NOTES,
      GROUPID, PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING, GROUPID_AT_BOOKING,
      ROLLOVER_BATCH_ID, ROLLED_OVER_FROM_YEAR, INVOICE_TYPE
    ) VALUES (
      CONVERT(NVARCHAR(36), NEWID()), s.APPLICATIONID, s.TEAMID, s.RENEWALDATE, s.AMT, 'Planned', s.FISCAL_YEAR,
      NULL, NULL, s.CONTRACT_ACTIVE, s.COMPANY_CODE, s.COST_CENTER,
      NULL, NULL, s.AGREEMENT_NUMBER, s.CONTRACT_DUE, s.SERVICE_TYPE, NULL,
      NULL, NULL, NULL, NULL,
      NULL, NULL, 'Recurring Invoice'
    )
    """
    # Count inserted/updated via temp actions
    cur.execute("IF OBJECT_ID('tempdb..#merge_actions') IS NOT NULL DROP TABLE #merge_actions; CREATE TABLE #merge_actions(action NVARCHAR(10));")
    cur.execute(merge_sql + " OUTPUT $action INTO #merge_actions;", (contract_id,))
    cur.execute("SELECT action, COUNT(*) CNT FROM #merge_actions GROUP BY action;")
    rows = cur.fetchall()
    c = {str(a).upper(): int(n) for a, n in rows} if rows else {}
    ins = c.get("INSERT", 0)
    upd = c.get("UPDATE", 0)

    # Cleanup stale planned invoices outside contract window
    cur.execute(f"SELECT APPLICATIONID, TEAMID, START_FY, END_FY FROM {_fq('CONTRACTS')} WHERE CONTRACT_ID = ?", (contract_id,))
    r = cur.fetchone()
    if r:
        app_id, team_id, start_fy, end_fy = r[0], r[1], int(r[2]), int(r[3])
        cur.execute(
            f"""
            DELETE FROM {_fq('INVOICES')}
            WHERE APPLICATIONID = ? AND TEAMID = ?
              AND ISNULL(INVOICE_TYPE, 'Recurring Invoice') = 'Recurring Invoice'
              AND STATUS = 'Planned'
              AND (FISCAL_YEAR < ? OR FISCAL_YEAR > ?)
            """,
            (app_id, team_id, start_fy, end_fy),
        )
    return ins, upd


def _upsert_invoices(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Invoices", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        tn = _normalize_str(r.get("TEAMNAME"))
        an = _normalize_str(r.get("APPLICATIONNAME"))
        fy = _to_int(r.get("FISCAL_YEAR"))
        it = _normalize_str(r.get("INVOICE_TYPE")) or "Recurring Invoice"
        if not tn or not an or fy is None:
            continue
        rows.append(
            (
                tn,
                an,
                int(fy),
                it,
                _to_float(r.get("AMOUNT")) or 0.0,
                _normalize_str(r.get("STATUS")) or "Planned",
                r.get("RENEWALDATE").isoformat() if isinstance(r.get("RENEWALDATE"), date) else None,
                r.get("INVOICEDATE").isoformat() if isinstance(r.get("INVOICEDATE"), date) else None,
                _normalize_str(r.get("PRODUCT_OWNER")),
                _to_float(r.get("AMOUNT_NEXT_YEAR")),
                int(_to_bool(r.get("CONTRACT_ACTIVE"))),
                _normalize_str(r.get("COMPANY_CODE")),
                _normalize_str(r.get("COST_CENTER")),
                _normalize_str(r.get("SERIAL_NUMBER")),
                _normalize_str(r.get("WORK_ORDER")),
                _normalize_str(r.get("AGREEMENT_NUMBER")),
                _to_int(r.get("CONTRACT_DUE")),
                _normalize_str(r.get("SERVICE_TYPE")),
                _normalize_str(r.get("NOTES")),
                user_email,
            )
        )
    _create_temp(
        cur,
        "#stg_invoices",
        [
            ("TEAMNAME", "NVARCHAR(255)"),
            ("APPLICATIONNAME", "NVARCHAR(255)"),
            ("FISCAL_YEAR", "INT"),
            ("INVOICE_TYPE", "NVARCHAR(255)"),
            ("AMOUNT", "DECIMAL(18,2)"),
            ("STATUS", "NVARCHAR(255)"),
            ("RENEWALDATE", "DATE"),
            ("INVOICEDATE", "DATE"),
            ("PRODUCT_OWNER", "NVARCHAR(255)"),
            ("AMOUNT_NEXT_YEAR", "DECIMAL(18,2)"),
            ("CONTRACT_ACTIVE", "BIT"),
            ("COMPANY_CODE", "NVARCHAR(255)"),
            ("COST_CENTER", "NVARCHAR(255)"),
            ("SERIAL_NUMBER", "NVARCHAR(255)"),
            ("WORK_ORDER", "NVARCHAR(255)"),
            ("AGREEMENT_NUMBER", "NVARCHAR(255)"),
            ("CONTRACT_DUE", "INT"),
            ("SERVICE_TYPE", "NVARCHAR(255)"),
            ("NOTES", "NVARCHAR(MAX)"),
            ("UPDATED_BY", "NVARCHAR(255)"),
        ],
    )
    _insert_temp(
        cur,
        "#stg_invoices",
        [
            "TEAMNAME",
            "APPLICATIONNAME",
            "FISCAL_YEAR",
            "INVOICE_TYPE",
            "AMOUNT",
            "STATUS",
            "RENEWALDATE",
            "INVOICEDATE",
            "PRODUCT_OWNER",
            "AMOUNT_NEXT_YEAR",
            "CONTRACT_ACTIVE",
            "COMPANY_CODE",
            "COST_CENTER",
            "SERIAL_NUMBER",
            "WORK_ORDER",
            "AGREEMENT_NUMBER",
            "CONTRACT_DUE",
            "SERVICE_TYPE",
            "NOTES",
            "UPDATED_BY",
        ],
        rows,
    )
    merge_sql = f"""
      MERGE INTO {_fq('INVOICES')} AS t
      USING (
        SELECT
          a.APPLICATIONID,
          tm.TEAMID,
          s.FISCAL_YEAR,
          ISNULL(NULLIF(s.INVOICE_TYPE,''),'Recurring Invoice') AS INVOICE_TYPE,
          s.AMOUNT,
          s.STATUS,
          s.RENEWALDATE,
          s.INVOICEDATE,
          s.PRODUCT_OWNER,
          s.AMOUNT_NEXT_YEAR,
          s.CONTRACT_ACTIVE,
          s.COMPANY_CODE,
          s.COST_CENTER,
          s.SERIAL_NUMBER,
          s.WORK_ORDER,
          s.AGREEMENT_NUMBER,
          s.CONTRACT_DUE,
          s.SERVICE_TYPE,
          s.NOTES,
          tm.PROGRAMID AS PROGRAMID_AT_BOOKING,
          a.VENDORID AS VENDORID_AT_BOOKING,
          a.GROUPID AS GROUPID_AT_BOOKING,
          a.GROUPID AS GROUPID,
          s.UPDATED_BY
        FROM #stg_invoices s
        JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
        JOIN {_fq('APPLICATIONS')} a ON a.APPLICATIONNAME = s.APPLICATIONNAME
      ) AS s
      ON t.APPLICATIONID = s.APPLICATIONID AND t.TEAMID = s.TEAMID AND t.FISCAL_YEAR = s.FISCAL_YEAR AND ISNULL(t.INVOICE_TYPE,'Recurring Invoice') = s.INVOICE_TYPE
      WHEN MATCHED AND ISNULL(t.STATUS,'') <> 'Completed' THEN UPDATE SET
        INVOICEDATE = s.INVOICEDATE,
        RENEWALDATE = s.RENEWALDATE,
        AMOUNT = s.AMOUNT,
        STATUS = s.STATUS,
        PRODUCT_OWNER = s.PRODUCT_OWNER,
        AMOUNT_NEXT_YEAR = s.AMOUNT_NEXT_YEAR,
        CONTRACT_ACTIVE = s.CONTRACT_ACTIVE,
        COMPANY_CODE = s.COMPANY_CODE,
        COST_CENTER = s.COST_CENTER,
        SERIAL_NUMBER = s.SERIAL_NUMBER,
        WORK_ORDER = s.WORK_ORDER,
        AGREEMENT_NUMBER = s.AGREEMENT_NUMBER,
        CONTRACT_DUE = s.CONTRACT_DUE,
        SERVICE_TYPE = s.SERVICE_TYPE,
        NOTES = s.NOTES,
        GROUPID = COALESCE(s.GROUPID, t.GROUPID),
        PROGRAMID_AT_BOOKING = COALESCE(s.PROGRAMID_AT_BOOKING, t.PROGRAMID_AT_BOOKING),
        VENDORID_AT_BOOKING = COALESCE(s.VENDORID_AT_BOOKING, t.VENDORID_AT_BOOKING),
        GROUPID_AT_BOOKING = COALESCE(s.GROUPID_AT_BOOKING, t.GROUPID_AT_BOOKING),
        UPDATED_AT = SYSDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (
        INVOICEID, APPLICATIONID, TEAMID,
        INVOICEDATE, RENEWALDATE, AMOUNT, STATUS, FISCAL_YEAR,
        PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING, GROUPID, GROUPID_AT_BOOKING,
        PRODUCT_OWNER, AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE,
        COMPANY_CODE, COST_CENTER, SERIAL_NUMBER, WORK_ORDER, AGREEMENT_NUMBER,
        CONTRACT_DUE, SERVICE_TYPE, NOTES,
        INVOICE_TYPE, UPDATED_AT, UPDATED_BY
      ) VALUES (
        CONVERT(NVARCHAR(36), NEWID()), s.APPLICATIONID, s.TEAMID,
        s.INVOICEDATE, s.RENEWALDATE, s.AMOUNT, s.STATUS, s.FISCAL_YEAR,
        s.PROGRAMID_AT_BOOKING, s.VENDORID_AT_BOOKING, s.GROUPID, s.GROUPID_AT_BOOKING,
        s.PRODUCT_OWNER, s.AMOUNT_NEXT_YEAR, s.CONTRACT_ACTIVE,
        s.COMPANY_CODE, s.COST_CENTER, s.SERIAL_NUMBER, s.WORK_ORDER, s.AGREEMENT_NUMBER,
        s.CONTRACT_DUE, s.SERVICE_TYPE, s.NOTES,
        s.INVOICE_TYPE, SYSDATETIME(), s.UPDATED_BY
      )
    """
    c = _merge_counts(cur, merge_sql)
    # Skips include existing Completed invoices, plus no-op matches
    return {"step": "Invoices", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_program_addl_costs(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Program Additional Costs", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        pn = _normalize_str(r.get("PROGRAMNAME"))
        y = _to_int(r.get("YEAR"))
        m = _to_int(r.get("MONTH"))
        ct = _normalize_str(r.get("COST_TYPE"))
        st = _normalize_str(r.get("SUBTYPE") or "") or ""
        desc = _normalize_str(r.get("DESCRIPTION") or "") or None
        curcy = _normalize_str(r.get("CURRENCY") or "") or None
        rec_raw = r.get("IS_RECURRING")
        try:
            rec = 1 if (rec_raw is None or bool(rec_raw)) else 0
        except Exception:
            rec = 1
        amt = _to_float(r.get("AMOUNT")) or 0.0
        if not pn or y is None or m is None or not ct:
            continue
        rows.append((pn, int(y), int(m), ct, st, desc, curcy, rec, amt, user_email))
    _create_temp(
        cur,
        "#stg_pac",
        [
            ("PROGRAMNAME", "NVARCHAR(255)"),
            ("YEAR", "INT"),
            ("MONTH", "INT"),
            ("COST_TYPE", "NVARCHAR(50)"),
            ("SUBTYPE", "NVARCHAR(50)"),
            ("DESCRIPTION", "NVARCHAR(255)"),
            ("CURRENCY", "NVARCHAR(10)"),
            ("IS_RECURRING", "BIT"),
            ("AMOUNT", "DECIMAL(18,2)"),
            ("UPDATED_BY", "NVARCHAR(255)"),
        ],
    )
    _insert_temp(
        cur,
        "#stg_pac",
        ["PROGRAMNAME", "YEAR", "MONTH", "COST_TYPE", "SUBTYPE", "DESCRIPTION", "CURRENCY", "IS_RECURRING", "AMOUNT", "UPDATED_BY"],
        rows,
    )
    merge_sql = f"""
      MERGE INTO {_fq('PROGRAM_ADDITIONAL_COSTS')} AS t
      USING (
        SELECT
          p.PROGRAMID,
          s.YEAR,
          s.MONTH,
          s.COST_TYPE,
          ISNULL(s.SUBTYPE,'') AS SUBTYPE,
          s.DESCRIPTION,
          s.CURRENCY,
          s.IS_RECURRING,
          s.AMOUNT,
          s.UPDATED_BY
        FROM #stg_pac s
        JOIN {_fq('PROGRAMS')} p ON p.PROGRAMNAME = s.PROGRAMNAME
      ) AS s
      ON t.PROGRAMID = s.PROGRAMID AND t.YEAR = s.YEAR AND t.MONTH = s.MONTH AND t.COST_TYPE = s.COST_TYPE AND t.SUBTYPE = s.SUBTYPE
      WHEN MATCHED THEN UPDATE SET
        AMOUNT = s.AMOUNT,
        DESCRIPTION = s.DESCRIPTION,
        CURRENCY = s.CURRENCY,
        IS_RECURRING = s.IS_RECURRING,
        UPDATED_AT = SYSDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (PROGRAMID, YEAR, MONTH, COST_TYPE, SUBTYPE, DESCRIPTION, CURRENCY, IS_RECURRING, AMOUNT, UPDATED_AT, UPDATED_BY)
      VALUES (s.PROGRAMID, s.YEAR, s.MONTH, s.COST_TYPE, s.SUBTYPE, s.DESCRIPTION, s.CURRENCY, s.IS_RECURRING, s.AMOUNT, SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Program Additional Costs", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


# Note: Rates are set by location in the Rates page and are not imported via Data Loader templates.


def _upsert_team_headcount(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Team Headcount", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        tn = _normalize_str(r.get("TEAMNAME"))
        y = _to_int(r.get("YEAR"))
        pi = _to_int(r.get("PI")) or 0
        cls = _normalize_str(r.get("CLASS"))
        loc = _normalize_str(r.get("LOCATION"))
        hc = _to_float(r.get("HEADCOUNT")) or 0.0
        if not tn or y is None or not cls or not loc:
            continue
        rows.append((tn, int(y), int(pi), cls, loc, float(hc), user_email))
    _create_temp(cur, "#stg_hc", [("TEAMNAME", "NVARCHAR(255)"), ("YEAR", "INT"), ("PI", "INT"), ("CLASS", "NVARCHAR(30)"), ("LOCATION", "NVARCHAR(50)"), ("HEADCOUNT", "FLOAT"), ("UPDATED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_hc", ["TEAMNAME", "YEAR", "PI", "CLASS", "LOCATION", "HEADCOUNT", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('TEAM_HEADCOUNT_HISTORY')} t
      USING (
        SELECT tm.TEAMID, s.YEAR, s.PI, s.CLASS, s.LOCATION, s.HEADCOUNT, s.UPDATED_BY
        FROM #stg_hc s
        JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
      ) s
      ON t.TEAMID = s.TEAMID AND t.YEAR = s.YEAR AND t.PI = s.PI AND t.CLASS = s.CLASS AND t.LOCATION = s.LOCATION
      WHEN MATCHED THEN UPDATE SET HEADCOUNT = s.HEADCOUNT, UPDATED_AT = SYSDATETIME(), UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, UPDATED_AT, UPDATED_BY)
      VALUES (s.TEAMID, s.YEAR, s.PI, s.CLASS, s.LOCATION, s.HEADCOUNT, SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Team Headcount", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_team_composition(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Team Composition", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        tn = _normalize_str(r.get("TEAMNAME"))
        y = _to_int(r.get("YEAR"))
        pi = _to_int(r.get("PI")) or 0
        if not tn or y is None:
            continue
        rows.append(
            (
                tn,
                int(y),
                int(pi),
                _to_float(r.get("TEAMFTE")) or 0.0,
                _to_float(r.get("DELIVERY_TEAM_FTE")) or 0.0,
                _to_float(r.get("CONTRACTOR_CS_FTE")) or 0.0,
                _to_float(r.get("CONTRACTOR_C_FTE")) or 0.0,
            )
        )
    _create_temp(cur, "#stg_team_comp", [("TEAMNAME", "NVARCHAR(255)"), ("YEAR", "INT"), ("PI", "INT"), ("TEAMFTE", "FLOAT"), ("DELIVERY_TEAM_FTE", "FLOAT"), ("CONTRACTOR_CS_FTE", "FLOAT"), ("CONTRACTOR_C_FTE", "FLOAT")])
    _insert_temp(cur, "#stg_team_comp", ["TEAMNAME", "YEAR", "PI", "TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_CS_FTE", "CONTRACTOR_C_FTE"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('TEAM_COMPOSITION_HISTORY')} t
      USING (
        SELECT tm.TEAMID, s.YEAR, s.PI, s.TEAMFTE, s.DELIVERY_TEAM_FTE, s.CONTRACTOR_CS_FTE, s.CONTRACTOR_C_FTE
        FROM #stg_team_comp s
        JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
      ) s
      ON t.TEAMID = s.TEAMID AND t.YEAR = s.YEAR AND t.PI = s.PI
      WHEN MATCHED THEN UPDATE SET TEAMFTE = s.TEAMFTE, DELIVERY_TEAM_FTE = s.DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE = s.CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE = s.CONTRACTOR_C_FTE, UPDATED_AT = SYSDATETIME()
      WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE, UPDATED_AT)
      VALUES (s.TEAMID, s.YEAR, s.PI, s.TEAMFTE, s.DELIVERY_TEAM_FTE, s.CONTRACTOR_CS_FTE, s.CONTRACTOR_C_FTE, SYSDATETIME())
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Team Composition", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_program_composition(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Program Composition", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        pn = _normalize_str(r.get("PROGRAMNAME"))
        y = _to_int(r.get("YEAR"))
        pi = _to_int(r.get("PI")) or 0
        fte = _to_float(r.get("PROGRAMFTE")) or 0.0
        if not pn or y is None:
            continue
        rows.append((pn, int(y), int(pi), float(fte), user_email))
    _create_temp(cur, "#stg_prog_comp", [("PROGRAMNAME", "NVARCHAR(255)"), ("YEAR", "INT"), ("PI", "INT"), ("PROGRAMFTE", "FLOAT"), ("UPDATED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_prog_comp", ["PROGRAMNAME", "YEAR", "PI", "PROGRAMFTE", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('PROGRAM_COMPOSITION_HISTORY')} t
      USING (
        SELECT p.PROGRAMID, s.YEAR, s.PI, s.PROGRAMFTE, s.UPDATED_BY
        FROM #stg_prog_comp s
        JOIN {_fq('PROGRAMS')} p ON p.PROGRAMNAME = s.PROGRAMNAME
      ) s
      ON t.PROGRAMID = s.PROGRAMID AND t.YEAR = s.YEAR AND t.PI = s.PI
      WHEN MATCHED THEN UPDATE SET PROGRAMFTE = s.PROGRAMFTE, UPDATED_AT = SYSDATETIME(), UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (PROGRAMID, YEAR, PI, PROGRAMFTE, UPDATED_AT, UPDATED_BY)
      VALUES (s.PROGRAMID, s.YEAR, s.PI, s.PROGRAMFTE, SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Program Composition", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_contractor_companies(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Contractor Companies", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        name = _normalize_str(r.get("NAME"))
        if not name:
            continue
        rows.append((name, int(_to_bool(r.get("ACTIVE"))), _normalize_str(r.get("NOTES")), user_email))
    _create_temp(cur, "#stg_cc", [("NAME", "NVARCHAR(255)"), ("ACTIVE", "BIT"), ("NOTES", "NVARCHAR(MAX)"), ("UPDATED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_cc", ["NAME", "ACTIVE", "NOTES", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('CONTRACTOR_COMPANY')} t
      USING (SELECT DISTINCT NAME, ACTIVE, NOTES, UPDATED_BY FROM #stg_cc) s
      ON t.NAME = s.NAME
      WHEN MATCHED THEN UPDATE SET ACTIVE = s.ACTIVE, NOTES = s.NOTES, UPDATED_AT = SYSDATETIME(), UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (COMPANYID, NAME, ACTIVE, NOTES, UPDATED_AT, UPDATED_BY)
      VALUES (CONVERT(NVARCHAR(36), NEWID()), s.NAME, s.ACTIVE, s.NOTES, SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Contractor Companies", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_contractor_rates(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Contractor Rates", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        name = _normalize_str(r.get("COMPANYNAME"))
        cls = _normalize_str(r.get("CLASS"))
        y = _to_int(r.get("YEAR"))
        pi = _to_int(r.get("PI")) or 0
        rate = _to_float(r.get("RATE"))
        if not name or not cls or y is None:
            continue
        rows.append((name, cls, int(y), int(pi), rate, user_email))
    _create_temp(cur, "#stg_cr", [("COMPANYNAME", "NVARCHAR(255)"), ("CLASS", "NVARCHAR(30)"), ("YEAR", "INT"), ("PI", "INT"), ("RATE", "DECIMAL(18,2)"), ("UPDATED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_cr", ["COMPANYNAME", "CLASS", "YEAR", "PI", "RATE", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('CONTRACTOR_RATE_HISTORY')} t
      USING (
        SELECT c.COMPANYID, s.CLASS, s.YEAR, s.PI, s.RATE, s.UPDATED_BY
        FROM #stg_cr s
        JOIN {_fq('CONTRACTOR_COMPANY')} c ON c.NAME = s.COMPANYNAME
      ) s
      ON t.COMPANYID = s.COMPANYID AND t.CLASS = s.CLASS AND t.YEAR = s.YEAR AND t.PI = s.PI
      WHEN MATCHED THEN UPDATE SET RATE = s.RATE, UPDATED_AT = SYSDATETIME(), UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (COMPANYID, CLASS, YEAR, PI, RATE, UPDATED_AT, UPDATED_BY)
      VALUES (s.COMPANYID, s.CLASS, s.YEAR, s.PI, s.RATE, SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Contractor Rates", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_contractor_headcount(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Contractor Headcount", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        tn = _normalize_str(r.get("TEAMNAME"))
        name = _normalize_str(r.get("COMPANYNAME"))
        cls = _normalize_str(r.get("CLASS"))
        y = _to_int(r.get("YEAR"))
        pi = _to_int(r.get("PI")) or 0
        hc = _to_float(r.get("HEADCOUNT")) or 0.0
        if not tn or not name or not cls or y is None:
            continue
        rows.append((tn, name, cls, int(y), int(pi), float(hc), user_email))
    _create_temp(cur, "#stg_chc", [("TEAMNAME", "NVARCHAR(255)"), ("COMPANYNAME", "NVARCHAR(255)"), ("CLASS", "NVARCHAR(30)"), ("YEAR", "INT"), ("PI", "INT"), ("HEADCOUNT", "FLOAT"), ("UPDATED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_chc", ["TEAMNAME", "COMPANYNAME", "CLASS", "YEAR", "PI", "HEADCOUNT", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('TEAM_CONTRACTOR_HEADCOUNT')} t
      USING (
        SELECT tm.TEAMID, c.COMPANYID, s.CLASS, s.YEAR, s.PI, s.HEADCOUNT, s.UPDATED_BY
        FROM #stg_chc s
        JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
        JOIN {_fq('CONTRACTOR_COMPANY')} c ON c.NAME = s.COMPANYNAME
      ) s
      ON t.TEAMID = s.TEAMID AND t.COMPANYID = s.COMPANYID AND t.CLASS = s.CLASS AND t.YEAR = s.YEAR AND t.PI = s.PI
      WHEN MATCHED THEN UPDATE SET HEADCOUNT = s.HEADCOUNT, UPDATED_AT = SYSDATETIME(), UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT, UPDATED_AT, UPDATED_BY)
      VALUES (s.TEAMID, s.YEAR, s.PI, s.CLASS, s.COMPANYID, s.HEADCOUNT, SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Contractor Headcount", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_msp_rates(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "MSP Rates", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        tn = _normalize_str(r.get("TEAMNAME"))
        if not tn:
            continue
        rows.append(
            (
                tn,
                int(_to_bool(r.get("MSP_ENABLED"))),
                _normalize_str(r.get("MSP_SIZE")),
                _to_float(r.get("MSP_RATE_PER_PI")),
                _to_float(r.get("MSP_RATE_SMALL")),
                _to_float(r.get("MSP_RATE_MEDIUM")),
                _to_float(r.get("MSP_RATE_LARGE")),
                _to_float(r.get("MSP_RATE")),
                user_email,
            )
        )
    _create_temp(
        cur,
        "#stg_msp_rate",
        [
            ("TEAMNAME", "NVARCHAR(255)"),
            ("MSP_ENABLED", "BIT"),
            ("MSP_SIZE", "NVARCHAR(50)"),
            ("MSP_RATE_PER_PI", "DECIMAL(18,2)"),
            ("MSP_RATE_SMALL", "DECIMAL(18,2)"),
            ("MSP_RATE_MEDIUM", "DECIMAL(18,2)"),
            ("MSP_RATE_LARGE", "DECIMAL(18,2)"),
            ("MSP_RATE", "DECIMAL(18,2)"),
            ("UPDATED_BY", "NVARCHAR(255)"),
        ],
    )
    _insert_temp(cur, "#stg_msp_rate", ["TEAMNAME", "MSP_ENABLED", "MSP_SIZE", "MSP_RATE_PER_PI", "MSP_RATE_SMALL", "MSP_RATE_MEDIUM", "MSP_RATE_LARGE", "MSP_RATE", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('TEAM_MSP_RATE')} t
      USING (
        SELECT tm.TEAMID, s.MSP_ENABLED, s.MSP_SIZE, s.MSP_RATE_PER_PI, s.MSP_RATE_SMALL, s.MSP_RATE_MEDIUM, s.MSP_RATE_LARGE, s.MSP_RATE, s.UPDATED_BY
        FROM #stg_msp_rate s
        JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
      ) s
      ON t.TEAMID = s.TEAMID
      WHEN MATCHED THEN UPDATE SET
        MSP_ENABLED = s.MSP_ENABLED,
        MSP_SIZE = s.MSP_SIZE,
        MSP_RATE_PER_PI = s.MSP_RATE_PER_PI,
        MSP_RATE_SMALL = s.MSP_RATE_SMALL,
        MSP_RATE_MEDIUM = s.MSP_RATE_MEDIUM,
        MSP_RATE_LARGE = s.MSP_RATE_LARGE,
        MSP_RATE = s.MSP_RATE,
        UPDATED_AT = SYSDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (TEAMID, MSP_ENABLED, MSP_SIZE, MSP_RATE_PER_PI, MSP_RATE_SMALL, MSP_RATE_MEDIUM, MSP_RATE_LARGE, MSP_RATE, UPDATED_AT, UPDATED_BY)
      VALUES (s.TEAMID, s.MSP_ENABLED, s.MSP_SIZE, s.MSP_RATE_PER_PI, s.MSP_RATE_SMALL, s.MSP_RATE_MEDIUM, s.MSP_RATE_LARGE, s.MSP_RATE, SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "MSP Rates", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_msp_assignments(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "MSP Assignments", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        tn = _normalize_str(r.get("TEAMNAME"))
        if not tn:
            continue
        gn = _normalize_str(r.get("GROUPNAME")) or "__UNASSIGNED__"
        eff = _to_int(r.get("EFFECTIVE_YEAR")) or 0
        yr = _to_int(r.get("YEAR")) or eff
        it_s = _to_int(r.get("ITERATION_START")) or 1
        it_e = _to_int(r.get("ITERATION_END")) or it_s
        rows.append((tn, gn, int(eff), int(yr), int(it_s), int(it_e), _normalize_str(r.get("MSP_SIZE")), _to_float(r.get("WEIGHT_PCT")) or 100.0, user_email))
    _create_temp(cur, "#stg_msp_asg", [("TEAMNAME", "NVARCHAR(255)"), ("GROUPNAME", "NVARCHAR(255)"), ("EFFECTIVE_YEAR", "INT"), ("YEAR", "INT"), ("ITERATION_START", "TINYINT"), ("ITERATION_END", "TINYINT"), ("MSP_SIZE", "NVARCHAR(50)"), ("WEIGHT_PCT", "DECIMAL(9,2)"), ("UPDATED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_msp_asg", ["TEAMNAME", "GROUPNAME", "EFFECTIVE_YEAR", "YEAR", "ITERATION_START", "ITERATION_END", "MSP_SIZE", "WEIGHT_PCT", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('TEAM_MSP_ASSIGNMENTS')} t
      USING (
        SELECT tm.TEAMID, g.GROUPID, s.EFFECTIVE_YEAR, s.YEAR, s.ITERATION_START, s.ITERATION_END, s.MSP_SIZE, s.WEIGHT_PCT, s.UPDATED_BY
        FROM #stg_msp_asg s
        JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
        LEFT JOIN {_fq('APPLICATION_GROUPS')} g ON g.GROUPNAME = s.GROUPNAME
      ) s
      ON t.TEAMID = s.TEAMID AND t.GROUPID = COALESCE(s.GROUPID, '__UNASSIGNED__') AND t.EFFECTIVE_YEAR = s.EFFECTIVE_YEAR
      WHEN MATCHED THEN UPDATE SET
        [YEAR] = s.YEAR,
        ITERATION_START = s.ITERATION_START,
        ITERATION_END = s.ITERATION_END,
        MSP_SIZE = s.MSP_SIZE,
        WEIGHT_PCT = s.WEIGHT_PCT,
        UPDATED_AT = SYSDATETIME()
      WHEN NOT MATCHED THEN INSERT (TEAMID, GROUPID, EFFECTIVE_YEAR, [YEAR], ITERATION_START, ITERATION_END, MSP_SIZE, WEIGHT_PCT, UPDATED_AT)
      VALUES (s.TEAMID, COALESCE(s.GROUPID, '__UNASSIGNED__'), s.EFFECTIVE_YEAR, s.YEAR, s.ITERATION_START, s.ITERATION_END, s.MSP_SIZE, s.WEIGHT_PCT, SYSDATETIME())
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "MSP Assignments", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _ensure_ado_map_tables() -> None:
    # Non-destructive ensure for mapping tables used by dashboards.
    _execute_autocommit(
        f"""
        IF OBJECT_ID('{_svq('MAP_ADO_TEAM_TO_TCO_TEAM')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('MAP_ADO_TEAM_TO_TCO_TEAM')} (
            ADO_TEAM_KEY NVARCHAR(900) NOT NULL PRIMARY KEY,
            TEAMID NVARCHAR(255) NULL,
            PROGRAM_RAW NVARCHAR(400) NULL,
            AREA_LEVEL3_RAW NVARCHAR(400) NULL,
            AREA_LEVEL4_RAW NVARCHAR(400) NULL,
            ADO_TEAM NVARCHAR(400) NULL
          );
        END
        """
    )
    _execute_autocommit(
        f"""
        IF OBJECT_ID('{_svq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM')} (
            ADO_PROGRAM NVARCHAR(400) PRIMARY KEY,
            PROGRAMID NVARCHAR(255) NULL
          );
        END
        """
    )
    _execute_autocommit(
        f"""
        IF OBJECT_ID('{_svq('MAP_ADO_APP_TO_TCO_GROUP')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('MAP_ADO_APP_TO_TCO_GROUP')} (
            ADO_APP NVARCHAR(400) PRIMARY KEY,
            GROUPID NVARCHAR(255) NULL
          );
        END
        """
    )


def _upsert_ado_team_map(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    _ensure_ado_map_tables()
    if df is None or df.empty:
        return {"step": "ADO Team Map", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        key = _normalize_str(r.get("ADO_TEAM_KEY"))
        if not key:
            continue
        rows.append((key, _normalize_str(r.get("TEAMNAME")), _normalize_str(r.get("PROGRAM_RAW")), _normalize_str(r.get("AREA_LEVEL3_RAW")), _normalize_str(r.get("AREA_LEVEL4_RAW")), _normalize_str(r.get("ADO_TEAM"))))
    _create_temp(cur, "#stg_ado_team", [("ADO_TEAM_KEY", "NVARCHAR(900)"), ("TEAMNAME", "NVARCHAR(255)"), ("PROGRAM_RAW", "NVARCHAR(400)"), ("AREA_LEVEL3_RAW", "NVARCHAR(400)"), ("AREA_LEVEL4_RAW", "NVARCHAR(400)"), ("ADO_TEAM", "NVARCHAR(400)")])
    _insert_temp(cur, "#stg_ado_team", ["ADO_TEAM_KEY", "TEAMNAME", "PROGRAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW", "ADO_TEAM"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('MAP_ADO_TEAM_TO_TCO_TEAM')} t
      USING (
        SELECT s.ADO_TEAM_KEY, tm.TEAMID, s.PROGRAM_RAW, s.AREA_LEVEL3_RAW, s.AREA_LEVEL4_RAW, s.ADO_TEAM
        FROM #stg_ado_team s
        LEFT JOIN {_fq('TEAMS')} tm ON tm.TEAMNAME = s.TEAMNAME
      ) s
      ON t.ADO_TEAM_KEY = s.ADO_TEAM_KEY
      WHEN MATCHED THEN UPDATE SET TEAMID = s.TEAMID, PROGRAM_RAW = s.PROGRAM_RAW, AREA_LEVEL3_RAW = s.AREA_LEVEL3_RAW, AREA_LEVEL4_RAW = s.AREA_LEVEL4_RAW, ADO_TEAM = s.ADO_TEAM
      WHEN NOT MATCHED THEN INSERT (ADO_TEAM_KEY, TEAMID, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, ADO_TEAM)
      VALUES (s.ADO_TEAM_KEY, s.TEAMID, s.PROGRAM_RAW, s.AREA_LEVEL3_RAW, s.AREA_LEVEL4_RAW, s.ADO_TEAM)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "ADO Team Map", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_ado_program_map(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    _ensure_ado_map_tables()
    if df is None or df.empty:
        return {"step": "ADO Program Map", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        ado = _normalize_str(r.get("ADO_PROGRAM"))
        pn = _normalize_str(r.get("PROGRAMNAME"))
        if not ado:
            continue
        rows.append((ado, pn))
    _create_temp(cur, "#stg_ado_prog", [("ADO_PROGRAM", "NVARCHAR(400)"), ("PROGRAMNAME", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_ado_prog", ["ADO_PROGRAM", "PROGRAMNAME"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM')} t
      USING (
        SELECT s.ADO_PROGRAM, p.PROGRAMID
        FROM #stg_ado_prog s
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMNAME = s.PROGRAMNAME
      ) s
      ON t.ADO_PROGRAM = s.ADO_PROGRAM
      WHEN MATCHED THEN UPDATE SET PROGRAMID = s.PROGRAMID
      WHEN NOT MATCHED THEN INSERT (ADO_PROGRAM, PROGRAMID) VALUES (s.ADO_PROGRAM, s.PROGRAMID)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "ADO Program Map", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_ado_app_map(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    _ensure_ado_map_tables()
    if df is None or df.empty:
        return {"step": "ADO App Map", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        ado = _normalize_str(r.get("ADO_APP"))
        gn = _normalize_str(r.get("GROUPNAME"))
        if not ado:
            continue
        rows.append((ado, gn))
    _create_temp(cur, "#stg_ado_app", [("ADO_APP", "NVARCHAR(400)"), ("GROUPNAME", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_ado_app", ["ADO_APP", "GROUPNAME"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('MAP_ADO_APP_TO_TCO_GROUP')} t
      USING (
        SELECT s.ADO_APP, g.GROUPID
        FROM #stg_ado_app s
        LEFT JOIN {_fq('APPLICATION_GROUPS')} g ON g.GROUPNAME = s.GROUPNAME
      ) s
      ON t.ADO_APP = s.ADO_APP
      WHEN MATCHED THEN UPDATE SET GROUPID = s.GROUPID
      WHEN NOT MATCHED THEN INSERT (ADO_APP, GROUPID) VALUES (s.ADO_APP, s.GROUPID)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "ADO App Map", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _normalize_work_id(work_id: Any) -> Optional[str]:
    s = _normalize_str(work_id)
    if not s:
        return None
    s = re.sub(r"[^0-9A-Za-z]", "", s)
    s = s.lstrip("0")
    return s or "0"


def _upsert_apptio_workids(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Apptio WorkIDs", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        wid = _normalize_work_id(r.get("WORK_ID"))
        pn = _normalize_str(r.get("PROGRAMNAME"))
        if not wid or not pn:
            continue
        rows.append((wid, pn, user_email))
    _create_temp(cur, "#stg_wid", [("WORK_ID", "NVARCHAR(255)"), ("PROGRAMNAME", "NVARCHAR(255)"), ("UPDATED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_wid", ["WORK_ID", "PROGRAMNAME", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('PROGRAM_APPTIO_WORKIDS')} t
      USING (
        SELECT s.WORK_ID, p.PROGRAMID, s.UPDATED_BY
        FROM #stg_wid s
        JOIN {_fq('PROGRAMS')} p ON p.PROGRAMNAME = s.PROGRAMNAME
      ) s
      ON t.WORK_ID = s.WORK_ID
      WHEN MATCHED THEN UPDATE SET PROGRAMID = s.PROGRAMID, UPDATED_AT = SYSDATETIME(), UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (WORK_ID, PROGRAMID, UPDATED_AT, UPDATED_BY)
      VALUES (s.WORK_ID, s.PROGRAMID, SYSDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Apptio WorkIDs", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_apptio_actuals(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "Apptio Actuals", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        wid = _normalize_work_id(r.get("WORK_ID"))
        fy = _to_int(r.get("FISCAL_YEAR"))
        m = _to_int(r.get("MONTH"))
        amt = _to_float(r.get("AMOUNT")) or 0.0
        if not wid or fy is None or m is None:
            continue
        rows.append((wid, int(fy), int(m), float(amt), _normalize_str(r.get("SOURCE")), user_email))
    _create_temp(cur, "#stg_apptio", [("WORK_ID", "NVARCHAR(255)"), ("FISCAL_YEAR", "INT"), ("MONTH", "INT"), ("AMOUNT", "DECIMAL(18,2)"), ("SOURCE", "NVARCHAR(255)"), ("LOADED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_apptio", ["WORK_ID", "FISCAL_YEAR", "MONTH", "AMOUNT", "SOURCE", "LOADED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('APPTIO_ACTUALS')} t
      USING (SELECT WORK_ID, FISCAL_YEAR, MONTH, AMOUNT, SOURCE, LOADED_BY FROM #stg_apptio) s
      ON t.WORK_ID = s.WORK_ID AND t.FISCAL_YEAR = s.FISCAL_YEAR AND t.MONTH = s.MONTH
      WHEN MATCHED THEN UPDATE SET AMOUNT = s.AMOUNT, SOURCE = s.SOURCE, LOADED_AT = SYSDATETIME(), LOADED_BY = s.LOADED_BY
      WHEN NOT MATCHED THEN INSERT (WORK_ID, FISCAL_YEAR, MONTH, AMOUNT, SOURCE, LOADED_AT, LOADED_BY)
      VALUES (s.WORK_ID, s.FISCAL_YEAR, s.MONTH, s.AMOUNT, s.SOURCE, SYSDATETIME(), s.LOADED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "Apptio Actuals", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _upsert_pi_commitments(cur: Any, df: pd.DataFrame, user_email: Optional[str]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"step": "PI Commitments", "inserted": 0, "updated": 0, "skipped": 0}
    rows = []
    for _, r in df.iterrows():
        y = _to_int(r.get("YEAR"))
        pi = _normalize_str(r.get("PI"))
        prog = _normalize_str(r.get("PROGRAMNAME"))
        team = _normalize_str(r.get("TEAMNAME"))
        grp = _normalize_str(r.get("GROUPNAME"))
        if y is None or not pi or not prog or not team or not grp:
            continue
        td = r.get("TARGET_DATE")
        td_s = td.isoformat() if isinstance(td, date) else None
        rows.append((int(y), pi, prog, team, grp, _normalize_str(r.get("STATUS")) or "Not started", _normalize_str(r.get("OWNER_EMAIL")), _normalize_str(r.get("OWNER_NAME")), td_s, _normalize_str(r.get("NOTES")), user_email))
    _create_temp(cur, "#stg_pi", [("YEAR", "INT"), ("PI", "NVARCHAR(50)"), ("PROGRAMNAME", "NVARCHAR(255)"), ("TEAMNAME", "NVARCHAR(255)"), ("GROUPNAME", "NVARCHAR(255)"), ("STATUS", "NVARCHAR(50)"), ("OWNER_EMAIL", "NVARCHAR(255)"), ("OWNER_NAME", "NVARCHAR(255)"), ("TARGET_DATE", "DATE"), ("NOTES", "NVARCHAR(MAX)"), ("UPDATED_BY", "NVARCHAR(255)")])
    _insert_temp(cur, "#stg_pi", ["YEAR", "PI", "PROGRAMNAME", "TEAMNAME", "GROUPNAME", "STATUS", "OWNER_EMAIL", "OWNER_NAME", "TARGET_DATE", "NOTES", "UPDATED_BY"], rows)
    merge_sql = f"""
      MERGE INTO {_fq('TCO_COMMITMENTS')} t
      USING (SELECT YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, STATUS, OWNER_EMAIL, OWNER_NAME, TARGET_DATE, NOTES, UPDATED_BY FROM #stg_pi) s
      ON t.YEAR = s.YEAR AND t.PI = s.PI AND t.PROGRAMNAME = s.PROGRAMNAME AND t.TEAMNAME = s.TEAMNAME AND t.GROUPNAME = s.GROUPNAME
      WHEN MATCHED THEN UPDATE SET
        STATUS = s.STATUS,
        OWNER_EMAIL = s.OWNER_EMAIL,
        OWNER_NAME = s.OWNER_NAME,
        TARGET_DATE = s.TARGET_DATE,
        NOTES = s.NOTES,
        UPDATED_AT = SYSUTCDATETIME(),
        UPDATED_BY = s.UPDATED_BY
      WHEN NOT MATCHED THEN INSERT (YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME, STATUS, OWNER_EMAIL, OWNER_NAME, TARGET_DATE, NOTES, UPDATED_AT, UPDATED_BY)
      VALUES (s.YEAR, s.PI, s.PROGRAMNAME, s.TEAMNAME, s.GROUPNAME, s.STATUS, s.OWNER_EMAIL, s.OWNER_NAME, s.TARGET_DATE, s.NOTES, SYSUTCDATETIME(), s.UPDATED_BY)
    """
    c = _merge_counts(cur, merge_sql)
    return {"step": "PI Commitments", "inserted": c["INSERT"], "updated": c["UPDATE"], "skipped": max(0, len(rows) - c["INSERT"] - c["UPDATE"])}


def _persist_bulkload_run_log(summary: Dict[str, Any], user_email: Optional[str], source_filename: Optional[str]) -> None:
    # Idempotent creation of a simple audit table for Bulk Load v2 runs.
    _execute_autocommit(
        f"""
        IF OBJECT_ID('{_svq('BULKLOAD_V2_RUNS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('BULKLOAD_V2_RUNS')} (
            RUN_ID NVARCHAR(64) PRIMARY KEY,
            STARTED_AT DATETIME2 NOT NULL,
            ENDED_AT DATETIME2 NULL,
            SUCCESS BIT NOT NULL DEFAULT 0,
            MODE NVARCHAR(32) NULL,
            SOURCE_FILENAME NVARCHAR(400) NULL,
            USER_EMAIL NVARCHAR(255) NULL,
            SUMMARY_JSON NVARCHAR(MAX) NULL,
            ERROR_TEXT NVARCHAR(MAX) NULL
          );
        END
        """
    )
    _execute_autocommit(
        f"""
        MERGE INTO {_fq('BULKLOAD_V2_RUNS')} t
        USING (SELECT %s AS RUN_ID) s
        ON t.RUN_ID = s.RUN_ID
        WHEN MATCHED THEN UPDATE SET
          STARTED_AT = %s,
          ENDED_AT = %s,
          SUCCESS = %s,
          MODE = %s,
          SOURCE_FILENAME = %s,
          USER_EMAIL = %s,
          SUMMARY_JSON = %s,
          ERROR_TEXT = %s
        WHEN NOT MATCHED THEN INSERT (RUN_ID, STARTED_AT, ENDED_AT, SUCCESS, MODE, SOURCE_FILENAME, USER_EMAIL, SUMMARY_JSON, ERROR_TEXT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            summary.get("run_id"),
            summary.get("started_at"),
            summary.get("ended_at"),
            1 if summary.get("success") else 0,
            summary.get("mode"),
            source_filename,
            user_email,
            json.dumps(summary, default=str),
            summary.get("error"),
            summary.get("run_id"),
            summary.get("started_at"),
            summary.get("ended_at"),
            1 if summary.get("success") else 0,
            summary.get("mode"),
            source_filename,
            user_email,
            json.dumps(summary, default=str),
            summary.get("error"),
        ),
    )


# ======================================================================================
# Specs: expected sheets/columns
# ======================================================================================

_SHEET_NAME_ALIASES: Dict[str, str] = {
    "programs": "Programs",
    "teams": "Teams",
    "vendors": "Vendors",
    "app groups": "App Groups",
    "appgroups": "App Groups",
    "application groups": "App Groups",
    "applications": "Applications",
    "contracts": "Contracts",
    "invoices": "Invoices",
    "program addl costs": "Program Addl Costs",
    "program additional costs": "Program Addl Costs",
    # Optional (master template only)
    "group-team links": "Group-Team Links",
    "group team links": "Group-Team Links",
}

_SHEET_SPECS: Dict[str, _SheetSpec] = {
    "Programs": _SheetSpec(
        required_columns=["PROGRAMNAME"],
        template_columns=["PROGRAMNAME", "PROGRAMOWNER", "PROGRAMFTE", "PROGRAM_XOM_RATE"],
    ),
    "Teams": _SheetSpec(
        required_columns=["TEAMNAME", "PROGRAMNAME"],
        template_columns=["TEAMNAME", "PROGRAMNAME", "PRODUCTOWNER", "TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE", "COSTPERFTE"],
    ),
    "Vendors": _SheetSpec(
        required_columns=["VENDORNAME"],
        template_columns=["VENDORNAME"],
    ),
    "App Groups": _SheetSpec(
        required_columns=["GROUPNAME", "TEAMNAME", "IS_BASE"],
        template_columns=["GROUPNAME", "TEAMNAME", "DEFAULT_VENDORNAME", "OWNER", "IS_BASE"],
    ),
    "Applications": _SheetSpec(
        required_columns=["APPLICATIONNAME"],
        template_columns=["APPLICATIONNAME", "VENDORNAME", "GROUPNAME", "ADD_INFO"],
    ),
    "Contracts": _SheetSpec(
        required_columns=["TEAMNAME", "APPLICATIONNAME", "START_FY", "END_FY", "ANNUAL_AMOUNT"],
        template_columns=[
            "TEAMNAME",
            "APPLICATIONNAME",
            "START_FY",
            "END_FY",
            "RENEWAL_MONTH",
            "ANNUAL_AMOUNT",
            "ESCALATION_PCT",
            "STATUS",
            "AGREEMENT_NUMBER",
            "COMPANY_CODE",
            "COST_CENTER",
            "SERVICE_TYPE",
            "CONTRACT_RENEWAL_DATE",
            "INVOICE_RENEWAL_DATE",
            "TOTAL_CONTRACT_COST",
        ],
    ),
    "Invoices": _SheetSpec(
        required_columns=["TEAMNAME", "APPLICATIONNAME", "FISCAL_YEAR", "INVOICE_TYPE", "AMOUNT"],
        template_columns=[
            "TEAMNAME",
            "APPLICATIONNAME",
            "FISCAL_YEAR",
            "INVOICE_TYPE",
            "AMOUNT",
            "STATUS",
            "RENEWALDATE",
            "INVOICEDATE",
            "PRODUCT_OWNER",
            "AMOUNT_NEXT_YEAR",
            "CONTRACT_ACTIVE",
            "COMPANY_CODE",
            "COST_CENTER",
            "SERIAL_NUMBER",
            "WORK_ORDER",
            "AGREEMENT_NUMBER",
            "CONTRACT_DUE",
            "SERVICE_TYPE",
            "NOTES",
        ],
    ),
    "Program Addl Costs": _SheetSpec(
        required_columns=["PROGRAMNAME", "YEAR", "MONTH", "COST_TYPE", "AMOUNT"],
        template_columns=["PROGRAMNAME", "YEAR", "MONTH", "COST_TYPE", "SUBTYPE", "AMOUNT"],
    ),
    "Group-Team Links": _SheetSpec(required_columns=["GROUPNAME", "TEAMNAME"], template_columns=["GROUPNAME", "TEAMNAME"]),
}
