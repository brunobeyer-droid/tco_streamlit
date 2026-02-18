#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.db_passwords import resolve_portfolio_db_password
from db.control_db import (
    control_db_available,
    ensure_control_schema,
    ensure_control_automation_tables,
    control_fetch_df,
    control_execute,
    list_automation_settings,
    queue_automation_run,
)


def _run_sql_count(sql: str, params: Tuple[Any, ...]) -> int:
    df = control_fetch_df(sql, params)
    if df is None or df.empty:
        return 0
    try:
        return int(df.iloc[0]["N"])
    except Exception:
        return 0


def _parse_org(org_url: str) -> str:
    raw = str(org_url or "").strip()
    if not raw:
        return ""
    if "://" not in raw and "/" not in raw:
        return raw
    try:
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        host = (parsed.netloc or "").lower()
        parts = [p for p in (parsed.path or "").split("/") if p]
        if "dev.azure.com" in host and parts:
            return parts[0]
        if host.endswith(".visualstudio.com"):
            return host.split(".")[0]
    except Exception:
        pass
    return raw.strip("/").split("/")[-1]


def _portfolio_row(portfolio_key: str):
    df = control_fetch_df(
        "SELECT TOP 1 * FROM CONTROL_PORTFOLIOS WHERE PORTFOLIO_KEY = %s",
        (portfolio_key,),
    )
    if df is None or df.empty:
        return None
    return df.iloc[0]


def _portfolio_env(row) -> Dict[str, str]:
    pkey = str(row.get("PORTFOLIO_KEY") or "").strip()
    profile_key = str(row.get("PROFILE_KEY") or "").strip()
    password_key = str(row.get("DB_PASSWORD_KEY") or "").strip()
    password = resolve_portfolio_db_password(password_key, pkey, profile_key) or ""
    env = os.environ.copy()
    env["MSSQL_SERVER"] = str(row.get("DB_SERVER") or "").strip()
    env["MSSQL_DATABASE"] = str(row.get("DB_DATABASE") or "").strip()
    env["MSSQL_USER"] = str(row.get("DB_USER") or "").strip()
    env["MSSQL_PASSWORD"] = str(password or "")
    env["MSSQL_DRIVER"] = str(row.get("DB_DRIVER") or "ODBC Driver 18 for SQL Server").strip()
    env["MSSQL_SCHEMA"] = str(row.get("DB_SCHEMA") or "dbo").strip() or "dbo"
    env["MSSQL_ENCRYPT"] = "yes" if bool(row.get("DB_ENCRYPT") if row.get("DB_ENCRYPT") is not None else True) else "no"
    env["MSSQL_TRUST_SERVER_CERTIFICATE"] = (
        "yes"
        if bool(row.get("DB_TRUST_SERVER_CERTIFICATE") if row.get("DB_TRUST_SERVER_CERTIFICATE") is not None else False)
        else "no"
    )
    return env


def _ado_env(row, env: Dict[str, str]) -> Dict[str, str]:
    out = dict(env)
    pat_env_key = str(row.get("PAT_ENV_KEY") or "").strip()
    if pat_env_key:
        out["ADO_PAT"] = str(os.getenv(pat_env_key) or "")
    else:
        out["ADO_PAT"] = str(os.getenv("ADO_PAT") or "")
    out["ADO_ORG"] = _parse_org(str(row.get("ADO_ORG_URL") or ""))
    out["ADO_PROJECT"] = str(row.get("ADO_PROJECT") or "").strip()
    return out


def _set_run_state(
    run_id: int,
    status: str,
    *,
    rows_upserted: Optional[int] = None,
    rows_changed: Optional[int] = None,
    message: str = "",
    started: bool = False,
    finished: bool = False,
) -> None:
    parts = ["STATUS = %s"]
    params: list[Any] = [status]
    if started:
        parts.append("STARTED_AT = SYSUTCDATETIME()")
    if finished:
        parts.append("FINISHED_AT = SYSUTCDATETIME()")
    if rows_upserted is not None:
        parts.append("ROWS_UPSERTED = %s")
        params.append(int(rows_upserted))
    if rows_changed is not None:
        parts.append("ROWS_CHANGED = %s")
        params.append(int(rows_changed))
    if message:
        parts.append("MESSAGE = %s")
        params.append(message[:4000])
    params.append(int(run_id))
    control_execute(
        f"""
        UPDATE CONTROL_AUTOMATION_RUNS
           SET {", ".join(parts)}
         WHERE RUN_ID = %s
        """,
        tuple(params),
    )


def _run_subprocess(cmd: list[str], env: Dict[str, str]) -> tuple[int, str]:
    proc = subprocess.run(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    out = proc.stdout or ""
    return int(proc.returncode), out


def _run_sync_job(portfolio_key: str, full: bool) -> tuple[bool, int, str]:
    row = _portfolio_row(portfolio_key)
    if row is None:
        return False, 0, "Portfolio not found in CONTROL_PORTFOLIOS."
    env = _portfolio_env(row)
    env = _ado_env(row, env)
    org = str(env.get("ADO_ORG") or "").strip()
    project = str(env.get("ADO_PROJECT") or "").strip()
    pat = str(env.get("ADO_PAT") or "").strip()
    if not org or not project:
        return False, 0, "Missing ADO org/project in portfolio settings."
    if not pat:
        return False, 0, "Missing PAT (PAT_ENV_KEY env variable value is empty)."
    max_pages = "200" if full else "60"
    sync_mode = "full" if full else "delta"
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "ado_sync.py"),
        "--org",
        org,
        "--project",
        project,
        "--sync-mode",
        sync_mode,
        "--max-pages",
        max_pages,
    ]
    rc, out = _run_subprocess(cmd, env)
    upserted = 0
    m = re.search(r"Upserted\\s+(\\d+)\\s+row", out, re.IGNORECASE)
    if m:
        try:
            upserted = int(m.group(1))
        except Exception:
            upserted = 0
    if rc != 0:
        return False, upserted, out[-3000:]
    return True, upserted, out[-3000:]


def _run_refresh_job(portfolio_key: str) -> tuple[bool, str]:
    row = _portfolio_row(portfolio_key)
    if row is None:
        return False, "Portfolio not found in CONTROL_PORTFOLIOS."
    env = _portfolio_env(row)
    cmd = [sys.executable, str(REPO_ROOT / "scripts" / "refresh_pipeline.py")]
    rc, out = _run_subprocess(cmd, env)
    if rc != 0:
        return False, out[-3000:]
    return True, out[-3000:]


def _is_schedule_due(now_utc: datetime, minute_target: int, tolerance: int = 2) -> bool:
    minute_now = int(now_utc.minute)
    return abs(minute_now - int(minute_target)) <= int(tolerance)


def _queue_due_schedule_runs(now_utc: datetime) -> int:
    queued = 0
    settings = list_automation_settings()
    if settings is None or settings.empty:
        return queued
    for _, row in settings.iterrows():
        pkey = str(row.get("PORTFOLIO_KEY") or "").strip()
        status = str(row.get("STATUS") or "").strip().upper()
        if not pkey or status != "ACTIVE":
            continue
        enabled = bool(int(row.get("ENABLE_AUTOMATION") or 0))
        if not enabled:
            continue

        # Hourly incremental
        if bool(int(row.get("HOURLY_SYNC_ENABLED") or 0)):
            minute_target = int(row.get("HOURLY_SYNC_MINUTE") or 10)
            if _is_schedule_due(now_utc, minute_target):
                n_recent = _run_sql_count(
                    """
                    SELECT COUNT(*) AS N
                    FROM CONTROL_AUTOMATION_RUNS
                    WHERE PORTFOLIO_KEY = %s
                      AND JOB_KIND = 'SYNC_INCREMENTAL'
                      AND TRIGGER_TYPE = 'schedule'
                      AND REQUESTED_AT >= DATEADD(MINUTE, -55, SYSUTCDATETIME())
                    """,
                    (pkey,),
                )
                if n_recent == 0:
                    queue_automation_run(pkey, "SYNC_INCREMENTAL", trigger_type="schedule")
                    queued += 1

        # Nightly full
        if bool(int(row.get("NIGHTLY_SYNC_ENABLED") or 0)):
            hour_target = int(row.get("NIGHTLY_SYNC_HOUR_UTC") or 2)
            minute_target = int(row.get("NIGHTLY_SYNC_MINUTE") or 15)
            if int(now_utc.hour) == hour_target and _is_schedule_due(now_utc, minute_target):
                n_recent = _run_sql_count(
                    """
                    SELECT COUNT(*) AS N
                    FROM CONTROL_AUTOMATION_RUNS
                    WHERE PORTFOLIO_KEY = %s
                      AND JOB_KIND = 'SYNC_FULL'
                      AND TRIGGER_TYPE = 'schedule'
                      AND REQUESTED_AT >= DATEADD(HOUR, -20, SYSUTCDATETIME())
                    """,
                    (pkey,),
                )
                if n_recent == 0:
                    queue_automation_run(pkey, "SYNC_FULL", trigger_type="schedule")
                    queued += 1
    return queued


def _process_queued_runs(limit: int = 10) -> int:
    runs = control_fetch_df(
        f"""
        SELECT TOP {max(1, min(200, int(limit)))}
          RUN_ID, PORTFOLIO_KEY, JOB_KIND
        FROM CONTROL_AUTOMATION_RUNS
        WHERE STATUS = 'QUEUED'
        ORDER BY RUN_ID ASC
        """
    )
    if runs is None or runs.empty:
        return 0

    processed = 0
    for _, run in runs.iterrows():
        run_id = int(run.get("RUN_ID"))
        pkey = str(run.get("PORTFOLIO_KEY") or "").strip()
        kind = str(run.get("JOB_KIND") or "").strip().upper()
        if not pkey or not kind:
            continue

        # Claim run atomically-ish.
        control_execute(
            """
            UPDATE CONTROL_AUTOMATION_RUNS
               SET STATUS = 'RUNNING',
                   STARTED_AT = SYSUTCDATETIME()
             WHERE RUN_ID = %s
               AND STATUS = 'QUEUED'
            """,
            (run_id,),
        )
        state = control_fetch_df("SELECT STATUS FROM CONTROL_AUTOMATION_RUNS WHERE RUN_ID = %s", (run_id,))
        if state is None or state.empty or str(state.iloc[0].get("STATUS") or "").upper() != "RUNNING":
            continue

        try:
            if kind in {"SYNC_INCREMENTAL", "SYNC_FULL"}:
                ok, upserted, msg = _run_sync_job(pkey, full=(kind == "SYNC_FULL"))
                if not ok:
                    _set_run_state(run_id, "FAILED", rows_upserted=upserted, message=msg, finished=True)
                    processed += 1
                    continue

                # Optionally queue/execute refresh after change.
                refresh_on_change = True
                try:
                    stg = control_fetch_df(
                        "SELECT TOP 1 REFRESH_ON_CHANGE FROM CONTROL_AUTOMATION_SETTINGS WHERE PORTFOLIO_KEY = %s",
                        (pkey,),
                    )
                    if stg is not None and not stg.empty:
                        refresh_on_change = bool(int(stg.iloc[0].get("REFRESH_ON_CHANGE") or 1))
                except Exception:
                    refresh_on_change = True

                refresh_msg = ""
                rows_changed = int(upserted or 0)
                if rows_changed > 0 and refresh_on_change:
                    ok_ref, ref_msg = _run_refresh_job(pkey)
                    refresh_msg = f" | refresh: {'ok' if ok_ref else 'failed'}"
                    if not ok_ref:
                        msg = (msg or "") + "\n" + (ref_msg or "")
                _set_run_state(
                    run_id,
                    "SUCCESS",
                    rows_upserted=upserted,
                    rows_changed=rows_changed,
                    message=(msg + refresh_msg)[:4000],
                    finished=True,
                )
            elif kind == "REFRESH_PIPELINE":
                ok, msg = _run_refresh_job(pkey)
                _set_run_state(
                    run_id,
                    "SUCCESS" if ok else "FAILED",
                    rows_changed=0,
                    message=msg[:4000],
                    finished=True,
                )
            else:
                _set_run_state(run_id, "FAILED", message=f"Unknown job kind: {kind}", finished=True)
        except Exception as e:
            _set_run_state(run_id, "FAILED", message=str(e), finished=True)
        processed += 1
    return processed


def run_once(max_runs: int = 10) -> Dict[str, int]:
    ensure_control_schema()
    ensure_control_automation_tables()
    now_utc = datetime.now(timezone.utc)
    queued = _queue_due_schedule_runs(now_utc)
    processed = _process_queued_runs(limit=max_runs)
    return {"queued_schedule_runs": int(queued), "processed_runs": int(processed)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Control DB automation queue worker.")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    parser.add_argument("--loop", action="store_true", help="Run continuously.")
    parser.add_argument("--interval-sec", type=int, default=300, help="Loop interval in seconds.")
    parser.add_argument("--max-runs", type=int, default=10, help="Max queued runs processed per cycle.")
    args = parser.parse_args()

    if not control_db_available():
        print("Control DB is not configured (CONTROL_MSSQL_*).")
        return 2

    if not args.loop:
        out = run_once(max_runs=args.max_runs)
        print(out)
        return 0

    while True:
        out = run_once(max_runs=args.max_runs)
        print(out)
        time.sleep(max(30, int(args.interval_sec)))


if __name__ == "__main__":
    raise SystemExit(main())
