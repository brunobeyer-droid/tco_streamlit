#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import random
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

for _logger_name in (
    "streamlit",
    "streamlit.runtime.scriptrunner_utils.script_run_context",
):
    logging.getLogger(_logger_name).setLevel(logging.ERROR)


def _resolve_portfolio_row(portfolio_key: str) -> Dict[str, Any]:
    from db.control_db import control_db_available, control_fetch_df
    from core.db_passwords import resolve_portfolio_db_password

    key = str(portfolio_key or "").strip()
    if not key:
        raise ValueError("portfolio_key is required")
    if not control_db_available():
        raise RuntimeError("Control DB is not available. Configure CONTROL_MSSQL_* first.")

    df = control_fetch_df(
        """
        SELECT
          PORTFOLIO_KEY,
          DB_SERVER,
          DB_DATABASE,
          DB_USER,
          DB_SCHEMA,
          DB_DRIVER,
          DB_ENCRYPT,
          DB_TRUST_SERVER_CERTIFICATE,
          PROFILE_KEY,
          DB_PASSWORD_KEY
        FROM CONTROL_PORTFOLIOS
        WHERE UPPER(PORTFOLIO_KEY) = UPPER(%s)
        """,
        (key,),
    )
    if df is None or df.empty:
        raise RuntimeError(f"Portfolio '{key}' was not found in CONTROL_PORTFOLIOS.")
    row = df.iloc[0].to_dict()
    pwd = resolve_portfolio_db_password(
        str(row.get("DB_PASSWORD_KEY") or ""),
        str(row.get("PORTFOLIO_KEY") or ""),
        str(row.get("PROFILE_KEY") or ""),
    ) or ""
    if not pwd:
        raise RuntimeError(f"Could not resolve DB password for portfolio '{key}'.")
    row["DB_PASSWORD"] = pwd
    return row


def _apply_portfolio_env(row: Dict[str, Any]) -> None:
    os.environ["MSSQL_SERVER"] = str(row.get("DB_SERVER") or "").strip()
    os.environ["MSSQL_DATABASE"] = str(row.get("DB_DATABASE") or "").strip()
    os.environ["MSSQL_USER"] = str(row.get("DB_USER") or "").strip()
    os.environ["MSSQL_PASSWORD"] = str(row.get("DB_PASSWORD") or "")
    os.environ["MSSQL_SCHEMA"] = str(row.get("DB_SCHEMA") or "dbo").strip() or "dbo"
    os.environ["MSSQL_DRIVER"] = str(row.get("DB_DRIVER") or "ODBC Driver 18 for SQL Server").strip()
    os.environ["MSSQL_ENCRYPT"] = "yes" if bool(row.get("DB_ENCRYPT")) else "no"
    os.environ["MSSQL_TRUST_SERVER_CERTIFICATE"] = "yes" if bool(row.get("DB_TRUST_SERVER_CERTIFICATE")) else "no"


def _ensure_schema_ready() -> None:
    from db import (
        ensure_tables,
        ensure_location_and_contractor_tables,
        ensure_ado_minimal_tables,
        ensure_contracts_table,
        ensure_all_views_ok,
        ensure_analytics_views_ok,
    )

    ensure_tables()
    ensure_location_and_contractor_tables()
    ensure_ado_minimal_tables()
    ensure_contracts_table()
    ensure_all_views_ok()
    ensure_analytics_views_ok()


def _seed_master_data(seed_tag: str) -> Dict[str, Any]:
    from db import (
        execute,
        upsert_program,
        upsert_team,
        upsert_vendor,
        upsert_application_group,
        upsert_application_instance,
    )

    vendors = [
        ("DUMMY_VENDOR_AWS", "Dummy AWS Marketplace"),
        ("DUMMY_VENDOR_MSFT", "Dummy Microsoft Enterprise"),
        ("DUMMY_VENDOR_SAAS", "Dummy SaaS Provider"),
    ]
    programs = [
        ("DUMMY_PROG_DIGITAL", "Dummy Digital Services", "dummy.owner.digital@next.local", 4.0, 20500.0),
        ("DUMMY_PROG_DATA", "Dummy Data and AI", "dummy.owner.data@next.local", 3.5, 21800.0),
        ("DUMMY_PROG_CYBER", "Dummy Cyber Resilience", "dummy.owner.cyber@next.local", 3.0, 22500.0),
    ]
    teams = [
        ("DUMMY_TEAM_IDAM", "Dummy IDAM Team", "DUMMY_PROG_DIGITAL", 8.0, 14.0, 1.5, 1.0),
        ("DUMMY_TEAM_PORTAL", "Dummy Citizen Portal Team", "DUMMY_PROG_DIGITAL", 7.0, 12.0, 1.0, 1.5),
        ("DUMMY_TEAM_PLATFORM", "Dummy Data Platform Team", "DUMMY_PROG_DATA", 7.5, 13.0, 1.0, 1.0),
        ("DUMMY_TEAM_ANALYTICS", "Dummy Analytics Team", "DUMMY_PROG_DATA", 6.5, 11.0, 0.5, 1.0),
        ("DUMMY_TEAM_SECOPS", "Dummy SecOps Team", "DUMMY_PROG_CYBER", 8.0, 12.0, 1.5, 1.0),
        ("DUMMY_TEAM_GRC", "Dummy GRC Team", "DUMMY_PROG_CYBER", 6.0, 10.0, 0.5, 1.0),
    ]
    groups = [
        ("DUMMY_GRP_IDAM", "Dummy IDAM", "DUMMY_TEAM_IDAM", "DUMMY_VENDOR_MSFT"),
        ("DUMMY_GRP_PORTAL", "Dummy Citizen Portal", "DUMMY_TEAM_PORTAL", "DUMMY_VENDOR_AWS"),
        ("DUMMY_GRP_ETL", "Dummy Enterprise ETL", "DUMMY_TEAM_PLATFORM", "DUMMY_VENDOR_AWS"),
        ("DUMMY_GRP_BI", "Dummy BI Platform", "DUMMY_TEAM_ANALYTICS", "DUMMY_VENDOR_SAAS"),
        ("DUMMY_GRP_SIEM", "Dummy SIEM", "DUMMY_TEAM_SECOPS", "DUMMY_VENDOR_SAAS"),
        ("DUMMY_GRP_VULN", "Dummy Vulnerability Mgmt", "DUMMY_TEAM_SECOPS", "DUMMY_VENDOR_MSFT"),
        ("DUMMY_GRP_RISK", "Dummy Risk Registry", "DUMMY_TEAM_GRC", "DUMMY_VENDOR_SAAS"),
        ("DUMMY_GRP_AUDIT", "Dummy Audit Automation", "DUMMY_TEAM_GRC", "DUMMY_VENDOR_MSFT"),
    ]

    apps: List[Tuple[str, str, str, str]] = []

    for vid, vname in vendors:
        upsert_vendor(vid, vname, updated_by=seed_tag)

    for pid, pname, owner, fte, prate in programs:
        upsert_program(
            program_id=pid,
            name=pname,
            owner=owner,
            fte=fte,
            program_rate=prate,
            program_display_name=pname,
            updated_by=seed_tag,
        )

    for tid, tname, pid, tfte, dfte, cfte, csfte in teams:
        upsert_team(
            team_id=tid,
            name=tname,
            program_id=pid,
            team_fte=tfte,
            delivery_team_fte=dfte,
            contractor_c_fte=cfte,
            contractor_cs_fte=csfte,
            team_display_name=tname,
            updated_by=seed_tag,
        )

    for gid, gname, tid, vid in groups:
        upsert_application_group(
            group_id=gid,
            group_name=gname,
            team_id=tid,
            default_vendor_id=vid,
            owner=seed_tag,
            is_base=False,
            updated_by=seed_tag,
        )
        app_id = f"{gid}_APP_MAIN"
        app_name = f"{gname} Main"
        upsert_application_instance(
            application_id=app_id,
            group_id=gid,
            application_name=app_name,
            add_info="Dummy seeded app for onboarding validation",
            vendor_id=vid,
            updated_by=seed_tag,
        )
        apps.append((app_id, app_name, gid, tid))

    team_by_id = {r[0]: r for r in teams}
    group_to_program: Dict[str, str] = {}
    for gid, _gname, tid, _vid in groups:
        group_to_program[gid] = str(team_by_id[tid][2])

    # ADO mappings (program/team/app) used by projected SWAG views.
    for pid, pname, *_rest in programs:
        execute(
            """
            MERGE INTO MAP_ADO_PROGRAM_TO_TCO_PROGRAM t
            USING (SELECT %s AS ADO_PROGRAM, %s AS PROGRAMID) s
            ON UPPER(LTRIM(RTRIM(t.ADO_PROGRAM))) = UPPER(LTRIM(RTRIM(s.ADO_PROGRAM)))
            WHEN MATCHED THEN UPDATE SET PROGRAMID = s.PROGRAMID
            WHEN NOT MATCHED THEN INSERT (ADO_PROGRAM, PROGRAMID) VALUES (s.ADO_PROGRAM, s.PROGRAMID);
            """,
            (pname, pid),
        )

    for tid, tname, pid, *_rest in teams:
        program_name = next((pname for pkey, pname, *_ in programs if pkey == pid), pid)
        area_l3 = f"{program_name} Delivery"
        area_l4 = tname
        team_variant_key = f"Dummy Portfolio|{program_name}|{area_l3}|{tname}".upper()
        execute(
            """
            MERGE INTO MAP_ADO_TEAM_TO_TCO_TEAM t
            USING (
              SELECT
                %s AS ADO_TEAM_KEY,
                %s AS TEAMID,
                %s AS PROGRAM_RAW,
                %s AS AREA_LEVEL3_RAW,
                %s AS AREA_LEVEL4_RAW,
                %s AS ADO_TEAM
            ) s
            ON UPPER(LTRIM(RTRIM(t.ADO_TEAM_KEY))) = UPPER(LTRIM(RTRIM(s.ADO_TEAM_KEY)))
            WHEN MATCHED THEN UPDATE SET
              TEAMID = s.TEAMID,
              PROGRAM_RAW = s.PROGRAM_RAW,
              AREA_LEVEL3_RAW = s.AREA_LEVEL3_RAW,
              AREA_LEVEL4_RAW = s.AREA_LEVEL4_RAW,
              ADO_TEAM = s.ADO_TEAM
            WHEN NOT MATCHED THEN INSERT (ADO_TEAM_KEY, TEAMID, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, ADO_TEAM)
            VALUES (s.ADO_TEAM_KEY, s.TEAMID, s.PROGRAM_RAW, s.AREA_LEVEL3_RAW, s.AREA_LEVEL4_RAW, s.ADO_TEAM);
            """,
            (team_variant_key, tid, program_name, area_l3, area_l4, tname),
        )

    for gid, gname, _tid, _vid in groups:
        execute(
            """
            MERGE INTO MAP_ADO_APP_TO_TCO_GROUP t
            USING (SELECT %s AS ADO_APP, %s AS APP_GROUP) s
            ON UPPER(LTRIM(RTRIM(t.ADO_APP))) = UPPER(LTRIM(RTRIM(s.ADO_APP)))
            WHEN MATCHED THEN UPDATE SET APP_GROUP = s.APP_GROUP
            WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP);
            """,
            (gname, gid),
        )

    return {
        "programs": programs,
        "teams": teams,
        "groups": groups,
        "apps": apps,
        "group_to_program": group_to_program,
    }


def _seed_capacity_and_rates(
    seed_tag: str,
    year: int,
    teams: List[Tuple[str, str, str, float, float, float, float]],
    programs: List[Tuple[str, str, str, float, float]],
) -> None:
    from db import (
        upsert_team_rate_history,
        upsert_program_rate_history,
        upsert_team_headcount,
        upsert_contractor_company,
        upsert_contractor_rate,
        upsert_team_contractor_headcount,
    )

    contractor_companies = [
        ("DUMMY_CO_ALPHA", "Dummy Contractor Alpha"),
        ("DUMMY_CO_BRAVO", "Dummy Contractor Bravo"),
    ]
    for cid, cname in contractor_companies:
        upsert_contractor_company(cid, cname, active=True, notes="Dummy seed contractor", updated_by=seed_tag)

    for idx, (cid, _cname) in enumerate(contractor_companies):
        upsert_contractor_rate(cid, year, 0, "CONTRACTOR_C", 24500 + (idx * 1200), updated_by=seed_tag)
        upsert_contractor_rate(cid, year, 0, "CONTRACTOR_CS", 28500 + (idx * 1500), updated_by=seed_tag)

    for idx, (tid, _tname, _pid, team_fte, delivery_fte, c_fte, cs_fte) in enumerate(teams):
        base_rate = 16500.0 + (idx * 850.0)
        for pi in [0, 1, 2, 3, 4]:
            rate = base_rate * (1.0 + (0.0125 * max(0, pi - 1)))
            upsert_team_rate_history(tid, year, pi, "GBC", round(rate, 2), updated_by=seed_tag)

        upsert_team_headcount(tid, year, 0, "TEAM", "GBC", float(team_fte), updated_by=seed_tag)
        upsert_team_headcount(tid, year, 0, "DELIVERY", "GBC", float(delivery_fte), updated_by=seed_tag)
        upsert_team_contractor_headcount(
            tid,
            year,
            0,
            "CONTRACTOR_C",
            contractor_companies[idx % len(contractor_companies)][0],
            float(c_fte),
            updated_by=seed_tag,
        )
        upsert_team_contractor_headcount(
            tid,
            year,
            0,
            "CONTRACTOR_CS",
            contractor_companies[(idx + 1) % len(contractor_companies)][0],
            float(cs_fte),
            updated_by=seed_tag,
        )

    for idx, (pid, _pname, _owner, program_fte, program_rate) in enumerate(programs):
        upsert_program_rate_history(pid, year, 0, "GBC", float(program_rate), updated_by=seed_tag)
        upsert_team_headcount(pid, year, 0, "PROGRAM", "GBC", float(program_fte + (0.3 * idx)), updated_by=seed_tag)


def _seed_invoices_and_contracts(
    seed_tag: str,
    year: int,
    apps: List[Tuple[str, str, str, str]],
    groups: List[Tuple[str, str, str, str]],
    group_to_program: Dict[str, str],
) -> None:
    from db import upsert_invoice, upsert_contract, sync_contract_invoices

    group_to_vendor = {gid: vid for gid, _gname, _tid, vid in groups}
    month_cursor = [1, 2, 3, 4, 5, 6, 7, 8]

    for idx, (app_id, app_name, gid, tid) in enumerate(apps):
        month = month_cursor[idx % len(month_cursor)]
        amount = round(95000 + (idx * 11750), 2)
        renewal_date = dt.date(year, month, 15).isoformat()
        upsert_invoice(
            invoice_id=f"DUMMY_INV_{year}_{idx + 1:03d}",
            application_id=app_id,
            team_id=tid,
            renewal_date=renewal_date,
            amount=amount,
            status="Planned",
            fiscal_year=year,
            product_owner="dummy.product.owner@next.local",
            amount_next_year=round(amount * 1.045, 2),
            contract_active=1,
            company_code=f"DCO{idx + 1:03d}",
            cost_center=f"DCOST{idx + 1:03d}",
            serial_number=f"DSN{idx + 1:06d}",
            work_order=f"DWO{idx + 1:06d}",
            agreement_number=f"DAG{idx + 1:06d}",
            contract_due=year + 1,
            service_type="Software Subscription",
            notes=f"Dummy seeded invoice for {app_name}",
            group_id=gid,
            programid_at_booking=group_to_program.get(gid),
            vendorid_at_booking=group_to_vendor.get(gid),
            groupid_at_booking=gid,
            rollover_batch_id=None,
            rolled_over_from_year=None,
            invoice_type="Recurring Invoice",
            updated_by=seed_tag,
        )

    # Create a few contracts and sync their planned recurring invoices.
    for idx, (app_id, app_name, gid, tid) in enumerate(apps[:4]):
        annual = round(120000 + (idx * 15000), 2)
        contract_id = f"DUMMY_CONTRACT_{idx + 1:03d}"
        upsert_contract(
            contract_id=contract_id,
            application_id=app_id,
            team_id=tid,
            start_fy=year,
            end_fy=year + 2,
            renewal_month=((idx % 12) + 1),
            annual_amount=annual,
            escalation_pct=3.0,
            status="Active",
            agreement_number=f"D-CON-{idx + 1:03d}",
            company_code=f"DCC{idx + 1:03d}",
            cost_center=f"DCNTR{idx + 1:03d}",
            service_type=f"Managed Service ({app_name})",
            contract_renewal_date=dt.date(year, ((idx % 12) + 1), 1).isoformat(),
            invoice_renewal_date=dt.date(year, ((idx % 12) + 1), 1).isoformat(),
            total_contract_cost=round(annual * 3.0, 2),
            updated_by=seed_tag,
        )
        sync_contract_invoices(contract_id)


def _seed_program_additional_costs(
    seed_tag: str,
    year: int,
    programs: List[Tuple[str, str, str, float, float]],
) -> None:
    from db import upsert_program_additional_cost

    monthly_profile = [
        (1, "Travel", "", 38000.0),
        (4, "Infra", "", 52000.0),
        (7, "Cloud", "AWS", 68000.0),
        (10, "Cloud", "Azure", 61000.0),
    ]
    for idx, (pid, _pname, _owner, _fte, _rate) in enumerate(programs):
        for month, cost_type, subtype, amount in monthly_profile:
            adj = amount + (idx * 8500.0)
            upsert_program_additional_cost(
                program_id=pid,
                year=year,
                month=month,
                cost_type=cost_type,
                subtype=subtype,
                amount=round(adj, 2),
                updated_by=seed_tag,
                description="Dummy seeded program-level cost",
                currency="USD",
                is_recurring=True,
            )


def _seed_ado_features(
    year: int,
    features_per_team_pi: int,
    programs: List[Tuple[str, str, str, float, float]],
    teams: List[Tuple[str, str, str, float, float, float, float]],
    groups: List[Tuple[str, str, str, str]],
) -> int:
    from db import upsert_ado_features, recompute_feature_iteration_mapping

    program_name_by_id = {pid: pname for pid, pname, *_ in programs}
    group_for_team: Dict[str, Tuple[str, str]] = {}
    for gid, gname, tid, _vid in groups:
        group_for_team[tid] = (gid, gname)

    rng = random.Random(42)
    rows: List[Dict[str, Any]] = []
    feature_seq = 1
    for pi in [1, 2, 3, 4]:
        for tid, tname, pid, _team_fte, _delivery_fte, _c_fte, _cs_fte in teams:
            _gid, gname = group_for_team[tid]
            pname = program_name_by_id.get(pid, pid)
            area_l3 = f"{pname} Delivery"
            area_l4 = tname
            team_variant_key = f"Dummy Portfolio|{pname}|{area_l3}|{tname}".upper()
            for n in range(features_per_team_pi):
                story = float(rng.choice([8, 13, 21, 34, 55]))
                bizv = float(rng.choice([5, 8, 13, 20]))
                state = rng.choice(["New", "Active", "Resolved"])
                created_at = dt.datetime(year - 1, 10, 1) + dt.timedelta(days=(feature_seq % 90))
                changed_at = created_at + dt.timedelta(days=(feature_seq % 21))
                rows.append(
                    {
                        "FEATURE_ID": f"DUMMY_FEAT_{year}_{feature_seq:05d}",
                        "TITLE": f"Dummy Feature {feature_seq} for {gname}",
                        "STATE": state,
                        "TEAM_RAW": tname,
                        "APP_NAME_RAW": gname,
                        "EFFORT_POINTS": story,
                        "STORY_POINTS": story,
                        "BUSINESS_VALUE": bizv,
                        "ITERATION_LEVEL3": f"{year} PI{pi}",
                        "ITERATION_LEVEL3_RAW": f"{year} PI{pi}",
                        "PI_LABEL": f"{year} PI{pi}",
                        "ITERATION_PATH": f"Dummy\\{year}\\PI{pi}\\Sprint {(n % 4) + 1}",
                        "ITERATION_SK": f"DUMMY-{year}-PI{pi}",
                        "CREATED_AT": created_at.isoformat(sep=" "),
                        "CHANGED_AT": changed_at.isoformat(sep=" "),
                        "ADO_YEAR": year,
                        "INVESTMENT_DIMENSION": rng.choice(["Run", "Grow", "Transform"]),
                        "PROGRAM_RAW": pname,
                        "AREA_LEVEL1_RAW": "Dummy Portfolio",
                        "AREA_LEVEL2_RAW": pname,
                        "AREA_LEVEL3_RAW": area_l3,
                        "AREA_LEVEL4_RAW": area_l4,
                        "AREA_PATH_RAW": f"Dummy Portfolio\\{pname}\\{area_l3}\\{area_l4}",
                        "TEAM_VARIANT_KEY": team_variant_key,
                    }
                )
                feature_seq += 1

    df = pd.DataFrame(rows)
    inserted = upsert_ado_features(df)
    try:
        recompute_feature_iteration_mapping()
    except Exception:
        pass
    return int(inserted)


def _run_refresh() -> Dict[str, Any]:
    from scripts.refresh_pipeline import run_refresh_pipeline
    from db import ensure_analytics_views_ok

    ensure_analytics_views_ok()
    return run_refresh_pipeline(changed_by="dummy_seed")


def _collect_summary(seed_year: int) -> Dict[str, int]:
    from db import fetch_df

    checks = {
        "PROGRAMS": "SELECT COUNT(*) AS N FROM PROGRAMS WHERE PROGRAMID LIKE 'DUMMY_%'",
        "TEAMS": "SELECT COUNT(*) AS N FROM TEAMS WHERE TEAMID LIKE 'DUMMY_%'",
        "GROUPS": "SELECT COUNT(*) AS N FROM APPLICATION_GROUPS WHERE GROUPID LIKE 'DUMMY_%'",
        "APPLICATIONS": "SELECT COUNT(*) AS N FROM APPLICATIONS WHERE APPLICATIONID LIKE 'DUMMY_%'",
        "INVOICES": "SELECT COUNT(*) AS N FROM INVOICES WHERE INVOICEID LIKE 'DUMMY_%' OR NOTES LIKE 'Dummy seeded invoice%'",
        "CONTRACTS": "SELECT COUNT(*) AS N FROM CONTRACTS WHERE CONTRACT_ID LIKE 'DUMMY_%'",
        "ADO_FEATURES": "SELECT COUNT(*) AS N FROM ADO_FEATURES WHERE FEATURE_ID LIKE 'DUMMY_FEAT_%'",
        "HEADCOUNT": "SELECT COUNT(*) AS N FROM TEAM_HEADCOUNT_HISTORY WHERE TEAMID LIKE 'DUMMY_%' AND YEAR = %s",
        "TEAM_RATES": "SELECT COUNT(*) AS N FROM TEAM_RATE_HISTORY WHERE TEAMID LIKE 'DUMMY_%' AND YEAR = %s",
        "PROGRAM_ADDITIONAL": "SELECT COUNT(*) AS N FROM PROGRAM_ADDITIONAL_COSTS WHERE PROGRAMID LIKE 'DUMMY_%' AND YEAR = %s",
    }

    out: Dict[str, int] = {}
    for label, sql in checks.items():
        try:
            params = (seed_year,) if "%s" in sql else None
            df = fetch_df(sql, params)
            n = int(df.iloc[0]["N"]) if df is not None and not df.empty else 0
            out[label] = n
        except Exception:
            out[label] = -1
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed dummy onboarding data into a portfolio DB.")
    parser.add_argument("--portfolio-key", required=True, help="Portfolio key from CONTROL_PORTFOLIOS (example: sled_it)")
    parser.add_argument("--year", type=int, default=dt.date.today().year, help="Fiscal year for seeded capacity/cost rows")
    parser.add_argument("--features-per-team-pi", type=int, default=6, help="How many dummy ADO features per team per PI")
    args = parser.parse_args()

    portfolio_row = _resolve_portfolio_row(args.portfolio_key)
    _apply_portfolio_env(portfolio_row)
    _ensure_schema_ready()

    seed_tag = "dummy.seed@next.local"
    seeded = _seed_master_data(seed_tag=seed_tag)
    _seed_capacity_and_rates(
        seed_tag=seed_tag,
        year=int(args.year),
        teams=seeded["teams"],
        programs=seeded["programs"],
    )
    _seed_program_additional_costs(
        seed_tag=seed_tag,
        year=int(args.year),
        programs=seeded["programs"],
    )
    _seed_invoices_and_contracts(
        seed_tag=seed_tag,
        year=int(args.year),
        apps=seeded["apps"],
        groups=seeded["groups"],
        group_to_program=seeded["group_to_program"],
    )
    feature_rows = _seed_ado_features(
        year=int(args.year),
        features_per_team_pi=max(1, int(args.features_per_team_pi)),
        programs=seeded["programs"],
        teams=seeded["teams"],
        groups=seeded["groups"],
    )
    refresh = _run_refresh()
    summary = _collect_summary(seed_year=int(args.year))

    print(f"Seed complete for portfolio: {portfolio_row.get('PORTFOLIO_KEY')}")
    print(f"Database: {portfolio_row.get('DB_DATABASE')} on {portfolio_row.get('DB_SERVER')}")
    print(f"Feature rows upserted this run: {feature_rows}")
    print("Refresh pipeline:", refresh)
    print("Summary counts:", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
