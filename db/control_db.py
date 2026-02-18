from __future__ import annotations

import os
from typing import Any, Dict, Optional, Sequence, List

import pandas as pd

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # type: ignore

from .mssql_backend import MssqlConfig, execute, fetch_df


def _is_cursor_spec_error(exc: Exception) -> bool:
    msg = str(exc or "")
    low = msg.lower()
    return (
        ("07005" in msg)
        or ("hy010" in low)
        or ("hy007" in low)
        or ("not a cursor-specification" in low)
        or ("function sequence error" in low)
        or ("sqlfetch" in low)
        or ("3971" in low)
        or ("failed to resume the transaction" in low)
    )


def _resolve_control_cfg() -> Optional[Dict[str, Any]]:
    cfg: Dict[str, Any] = {}
    if st is not None:
        try:
            secrets_cfg = st.secrets.get("control_db", {})  # type: ignore[attr-defined]
            if secrets_cfg:
                try:
                    cfg = dict(secrets_cfg)
                except Exception:
                    cfg = {}
        except Exception:
            cfg = {}

    source = "secrets" if cfg else "env"
    server = str(cfg.get("server") or "").strip() or os.getenv("CONTROL_MSSQL_SERVER", "")
    database = str(cfg.get("database") or "").strip() or os.getenv("CONTROL_MSSQL_DATABASE", "")
    user = str(cfg.get("user") or "").strip() or os.getenv("CONTROL_MSSQL_USER", "")
    password = str(cfg.get("password") or "").strip() or os.getenv("CONTROL_MSSQL_PASSWORD", "")
    driver = (
        str(cfg.get("driver") or "").strip()
        or os.getenv("CONTROL_MSSQL_DRIVER", "")
        or "ODBC Driver 17 for SQL Server"
    )
    encrypt_val = cfg.get("encrypt", None)
    trust_val = cfg.get("trust_server_certificate", None)

    def _parse_boolish(val: Any, *, env_key: str, env_default: str) -> bool:
        # Preserve explicit False from secrets.toml (avoid `or ""` dropping it).
        if val is None:
            raw = str(os.getenv(env_key, env_default) or "").strip()
        elif isinstance(val, bool):
            return bool(val)
        else:
            raw = str(val).strip()
        return str(raw).strip().lower() in {"1", "true", "yes", "y"}

    encrypt = _parse_boolish(encrypt_val, env_key="CONTROL_MSSQL_ENCRYPT", env_default="yes")
    trust = _parse_boolish(trust_val, env_key="CONTROL_MSSQL_TRUST_SERVER_CERTIFICATE", env_default="no")
    schema = str(cfg.get("schema") or "").strip() or os.getenv("CONTROL_MSSQL_SCHEMA", "dbo")
    if not (server and database and user and password and driver):
        return None
    return {
        "server": server,
        "database": database,
        "user": user,
        "password": password,
        "driver": driver,
        "encrypt": encrypt,
        "trust_server_certificate": trust,
        "schema": schema or "dbo",
        "source": source,
    }


def normalize_control_role(role: Any, default: str = "VIEWER") -> str:
    r = str(role or "").strip().upper()
    if r == "EDITOR":
        r = "CONTRIBUTOR"
    if r not in {"ADMIN", "CONTRIBUTOR", "VIEWER"}:
        r = str(default or "VIEWER").strip().upper() or "VIEWER"
        if r == "EDITOR":
            r = "CONTRIBUTOR"
        if r not in {"ADMIN", "CONTRIBUTOR", "VIEWER"}:
            r = "VIEWER"
    return r


def _control_cfg() -> Optional[MssqlConfig]:
    cfg = _resolve_control_cfg()
    if not cfg:
        return None
    return MssqlConfig(
        server=cfg["server"],
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        driver=cfg["driver"],
        encrypt=str(cfg["encrypt"]).lower(),
        trust_server_certificate=str(cfg["trust_server_certificate"]).lower(),
        schema=cfg.get("schema") or "dbo",
    )


def control_db_available() -> bool:
    cfg = _resolve_control_cfg()
    return bool(
        cfg
        and cfg.get("server")
        and cfg.get("database")
        and cfg.get("user")
        and cfg.get("password")
        and cfg.get("driver")
    )


def control_execute(sql: str, params: Optional[Any] = None) -> None:
    cfg = _control_cfg()
    if cfg is None:
        raise RuntimeError("Control DB config missing (CONTROL_MSSQL_* env vars).")
    try:
        execute(sql, params, cfg=cfg)
    except Exception as exc:
        _handle_control_db_error(exc, cfg)
        raise


def control_fetch_df(sql: str, params: Optional[Any] = None) -> pd.DataFrame:
    cfg = _control_cfg()
    if cfg is None:
        raise RuntimeError("Control DB config missing (CONTROL_MSSQL_* env vars).")
    try:
        return fetch_df(sql, params, cfg=cfg)
    except Exception as exc:
        # Intermittent pyodbc/ODBC18 issue observed in App Service:
        # "07005 ... Prepared statement is not a cursor-specification".
        # Treat as transient cursor-state issue and retry once after resetting pooled connections.
        if _is_cursor_spec_error(exc):
            try:
                from .mssql_backend import _reset_connection_cache, _is_connection_error  # type: ignore

                _reset_connection_cache()
                return fetch_df(sql, params, cfg=cfg)
            except Exception as exc2:
                if _is_connection_error(str(exc2)):
                    try:
                        _reset_connection_cache()
                        return fetch_df(sql, params, cfg=cfg)
                    except Exception as exc3:
                        _handle_control_db_error(exc3, cfg)
                        raise
                _handle_control_db_error(exc2, cfg)
                raise
        _handle_control_db_error(exc, cfg)
        raise


def _handle_control_db_error(exc: Exception, cfg: MssqlConfig) -> None:
    msg = str(exc)
    if "4060" in msg or "Cannot open database" in msg:
        try:
            if st is not None:
                db_name = str(cfg.database or "").strip()
                st.error(
                    f"Control DB {db_name or '(unknown)'} does not exist. "
                    f"Create it in SQL Server (CREATE DATABASE {db_name}) "
                    "or point control_db.database to an existing DB."
                )
                with st.expander("Control DB error details", expanded=False):
                    st.code(msg, language="text")
        except Exception:
            pass


def control_schema_status() -> Dict[str, Any]:
    cfg = _control_cfg()
    if cfg is None:
        return {
            "ok": False,
            "missing": [
                "CONTROL_PORTFOLIOS",
                "CONTROL_PORTFOLIO_USERS",
                "CONTROL_AUDIT_LOG",
                "CONTROL_GLOBAL_ADMINS",
                "CONTROL_PORTFOLIO_ACCESS_RULES",
                "CONTROL_AUTOMATION_SETTINGS",
                "CONTROL_AUTOMATION_RUNS",
                "CONTROL_AUTOMATION_LOCKS",
            ],
        }
    required = [
        "CONTROL_PORTFOLIOS",
        "CONTROL_PORTFOLIO_USERS",
        "CONTROL_AUDIT_LOG",
        "CONTROL_GLOBAL_ADMINS",
        "CONTROL_PORTFOLIO_ACCESS_RULES",
        "CONTROL_AUTOMATION_SETTINGS",
        "CONTROL_AUTOMATION_RUNS",
        "CONTROL_AUTOMATION_LOCKS",
    ]
    missing = []
    schema = cfg.schema or "dbo"
    try:
        placeholders = ", ".join(["%s"] * len(required))
        params = tuple([schema] + required)
        df = control_fetch_df(
            f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME IN ({placeholders})
            """,
            params,
        )
        existing = set(
            df["TABLE_NAME"].dropna().astype(str).tolist()
            if df is not None and not df.empty and "TABLE_NAME" in df.columns
            else []
        )
        missing = [name for name in required if name not in existing]
    except Exception as exc:
        return {
            "ok": False,
            "missing": required,
            "error": str(exc),
        }
    return {"ok": not missing, "missing": missing}


def ensure_control_schema() -> Dict[str, Any]:
    try:
        create_control_schema()
    except Exception:
        pass
    return control_schema_status()


def create_control_schema() -> None:
    cfg = _control_cfg()
    if cfg is None:
        raise RuntimeError("Control DB config missing (CONTROL_MSSQL_* env vars).")
    schema = cfg.schema or "dbo"

    control_execute(
        f"""
        IF OBJECT_ID('{schema}.CONTROL_PORTFOLIOS', 'U') IS NULL
        BEGIN
          CREATE TABLE {schema}.CONTROL_PORTFOLIOS (
            PORTFOLIO_KEY NVARCHAR(64) PRIMARY KEY,
            DISPLAY_NAME NVARCHAR(128),
            DESCRIPTION NVARCHAR(512) NULL,
            DB_SERVER NVARCHAR(256),
            DB_DATABASE NVARCHAR(256),
            DB_USER NVARCHAR(256) NULL,
            DB_PASSWORD_KEY NVARCHAR(128) NULL,
            DB_SCHEMA NVARCHAR(64) DEFAULT 'dbo',
            DB_DRIVER NVARCHAR(128) NULL,
            DB_ENCRYPT BIT NULL,
            DB_TRUST_SERVER_CERTIFICATE BIT NULL,
            PROFILE_KEY NVARCHAR(64),
            ADO_ORG_URL NVARCHAR(512) NULL,
            ADO_PROJECT NVARCHAR(256) NULL,
            PAT_ENV_KEY NVARCHAR(128) NULL,
            STATUS NVARCHAR(32) DEFAULT 'ACTIVE',
            CREATED_AT DATETIME2 DEFAULT SYSUTCDATETIME(),
            UPDATED_AT DATETIME2 DEFAULT SYSUTCDATETIME()
          );
        END
        """
    )
    control_execute(
        f"""
        IF OBJECT_ID('{schema}.CONTROL_PORTFOLIO_USERS', 'U') IS NULL
        BEGIN
          CREATE TABLE {schema}.CONTROL_PORTFOLIO_USERS (
            USER_EMAIL NVARCHAR(256),
            PORTFOLIO_KEY NVARCHAR(64),
            ROLE NVARCHAR(32),
            IS_ACTIVE BIT NOT NULL DEFAULT 1,
            CREATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            UPDATED_AT DATETIME2 NULL,
            PRIMARY KEY (USER_EMAIL, PORTFOLIO_KEY)
          );
        END
        """
    )
    control_execute(
        f"""
        IF OBJECT_ID('{schema}.CONTROL_AUDIT_LOG', 'U') IS NULL
        BEGIN
          CREATE TABLE {schema}.CONTROL_AUDIT_LOG (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            TS DATETIME2 DEFAULT SYSUTCDATETIME(),
            USER_EMAIL NVARCHAR(256),
            ACTION NVARCHAR(256),
            PORTFOLIO_KEY NVARCHAR(64) NULL,
            DETAILS NVARCHAR(MAX) NULL
          );
        END
        """
    )
    control_execute(
        f"""
        IF OBJECT_ID('{schema}.CONTROL_GLOBAL_ADMINS', 'U') IS NULL
        BEGIN
          CREATE TABLE {schema}.CONTROL_GLOBAL_ADMINS (
            USER_EMAIL NVARCHAR(320) NOT NULL PRIMARY KEY,
            IS_ACTIVE BIT NOT NULL DEFAULT 1,
            CREATED_AT DATETIME2 DEFAULT SYSUTCDATETIME(),
            UPDATED_AT DATETIME2 NULL
          );
        END
        """
    )
    control_execute(
        f"""
        IF OBJECT_ID('{schema}.CONTROL_PORTFOLIO_ACCESS_RULES', 'U') IS NULL
        BEGIN
          CREATE TABLE {schema}.CONTROL_PORTFOLIO_ACCESS_RULES (
            PORTFOLIO_KEY NVARCHAR(64) NOT NULL,
            RULE_TYPE NVARCHAR(64) NOT NULL,
            RULE_VALUE NVARCHAR(512) NOT NULL,
            IS_ACTIVE BIT NOT NULL DEFAULT 1,
            CREATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            UPDATED_AT DATETIME2 NULL
          );
        END
        """
    )
    ensure_control_automation_tables()
    # Backward-compatible column upgrades for existing installations.
    control_execute(
        f"""
        IF COL_LENGTH('{schema}.CONTROL_GLOBAL_ADMINS', 'IS_ACTIVE') IS NULL
          ALTER TABLE {schema}.CONTROL_GLOBAL_ADMINS ADD IS_ACTIVE BIT NOT NULL CONSTRAINT DF_CONTROL_GLOBAL_ADMINS_IS_ACTIVE DEFAULT 1;
        IF COL_LENGTH('{schema}.CONTROL_PORTFOLIO_USERS', 'IS_ACTIVE') IS NULL
          ALTER TABLE {schema}.CONTROL_PORTFOLIO_USERS ADD IS_ACTIVE BIT NOT NULL CONSTRAINT DF_CONTROL_PORTFOLIO_USERS_IS_ACTIVE DEFAULT 1;
        IF COL_LENGTH('{schema}.CONTROL_PORTFOLIO_USERS', 'CREATED_AT') IS NULL
          ALTER TABLE {schema}.CONTROL_PORTFOLIO_USERS ADD CREATED_AT DATETIME2 NOT NULL CONSTRAINT DF_CONTROL_PORTFOLIO_USERS_CREATED_AT DEFAULT SYSUTCDATETIME();
        IF COL_LENGTH('{schema}.CONTROL_PORTFOLIO_USERS', 'UPDATED_AT') IS NULL
          ALTER TABLE {schema}.CONTROL_PORTFOLIO_USERS ADD UPDATED_AT DATETIME2 NULL;

        -- Canonical role vocabulary migration: EDITOR -> CONTRIBUTOR.
        UPDATE {schema}.CONTROL_PORTFOLIO_USERS
           SET ROLE = 'CONTRIBUTOR'
         WHERE UPPER(LTRIM(RTRIM(COALESCE(ROLE, '')))) = 'EDITOR';
        UPDATE {schema}.CONTROL_PORTFOLIO_USERS
           SET ROLE = 'VIEWER'
         WHERE NULLIF(LTRIM(RTRIM(COALESCE(ROLE, ''))), '') IS NULL;

        IF NOT EXISTS (
          SELECT 1
          FROM sys.check_constraints
          WHERE name = 'CK_CONTROL_PORTFOLIO_USERS_ROLE_CANONICAL'
            AND parent_object_id = OBJECT_ID('{schema}.CONTROL_PORTFOLIO_USERS')
        )
        BEGIN
          ALTER TABLE {schema}.CONTROL_PORTFOLIO_USERS
          ADD CONSTRAINT CK_CONTROL_PORTFOLIO_USERS_ROLE_CANONICAL
          CHECK (UPPER(LTRIM(RTRIM(COALESCE(ROLE, '')))) IN ('ADMIN','CONTRIBUTOR','VIEWER'));
        END
        """
    )
    control_execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_CONTROL_PORTFOLIO_USERS_PORTFOLIOS'
        )
        BEGIN
          ALTER TABLE {schema}.CONTROL_PORTFOLIO_USERS
          ADD CONSTRAINT FK_CONTROL_PORTFOLIO_USERS_PORTFOLIOS
          FOREIGN KEY (PORTFOLIO_KEY)
          REFERENCES {schema}.CONTROL_PORTFOLIOS (PORTFOLIO_KEY);
        END
        """
    )


def ensure_control_automation_tables() -> None:
    cfg = _control_cfg()
    if cfg is None:
        raise RuntimeError("Control DB config missing (CONTROL_MSSQL_* env vars).")
    schema = cfg.schema or "dbo"

    control_execute(
        f"""
        IF OBJECT_ID('{schema}.CONTROL_AUTOMATION_SETTINGS', 'U') IS NULL
        BEGIN
          CREATE TABLE {schema}.CONTROL_AUTOMATION_SETTINGS (
            PORTFOLIO_KEY NVARCHAR(64) NOT NULL PRIMARY KEY,
            ENABLE_AUTOMATION BIT NOT NULL DEFAULT 0,
            HOURLY_SYNC_ENABLED BIT NOT NULL DEFAULT 0,
            HOURLY_SYNC_MINUTE INT NOT NULL DEFAULT 10,
            NIGHTLY_SYNC_ENABLED BIT NOT NULL DEFAULT 1,
            NIGHTLY_SYNC_HOUR_UTC INT NOT NULL DEFAULT 2,
            NIGHTLY_SYNC_MINUTE INT NOT NULL DEFAULT 15,
            REFRESH_ON_CHANGE BIT NOT NULL DEFAULT 1,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            UPDATED_BY NVARCHAR(256) NULL
          );
        END
        """
    )
    control_execute(
        f"""
        IF OBJECT_ID('{schema}.CONTROL_AUTOMATION_RUNS', 'U') IS NULL
        BEGIN
          CREATE TABLE {schema}.CONTROL_AUTOMATION_RUNS (
            RUN_ID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            PORTFOLIO_KEY NVARCHAR(64) NOT NULL,
            JOB_KIND NVARCHAR(64) NOT NULL,
            TRIGGER_TYPE NVARCHAR(32) NOT NULL DEFAULT 'manual',
            STATUS NVARCHAR(32) NOT NULL DEFAULT 'QUEUED',
            REQUESTED_BY NVARCHAR(256) NULL,
            REQUESTED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            STARTED_AT DATETIME2 NULL,
            FINISHED_AT DATETIME2 NULL,
            ROWS_UPSERTED INT NULL,
            ROWS_CHANGED INT NULL,
            MESSAGE NVARCHAR(MAX) NULL
          );
        END
        """
    )
    control_execute(
        f"""
        IF OBJECT_ID('{schema}.CONTROL_AUTOMATION_LOCKS', 'U') IS NULL
        BEGIN
          CREATE TABLE {schema}.CONTROL_AUTOMATION_LOCKS (
            LOCK_NAME NVARCHAR(128) NOT NULL PRIMARY KEY,
            OWNER_ID NVARCHAR(128) NULL,
            LOCKED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            EXPIRES_AT DATETIME2 NULL
          );
        END
        """
    )
    control_execute(
        f"""
        IF NOT EXISTS (
          SELECT 1
          FROM sys.indexes
          WHERE name = 'IX_CONTROL_AUTOMATION_RUNS_STATUS_REQUESTED_AT'
            AND object_id = OBJECT_ID('{schema}.CONTROL_AUTOMATION_RUNS')
        )
        BEGIN
          CREATE INDEX IX_CONTROL_AUTOMATION_RUNS_STATUS_REQUESTED_AT
          ON {schema}.CONTROL_AUTOMATION_RUNS (STATUS, REQUESTED_AT DESC);
        END
        """
    )
    control_execute(
        f"""
        IF NOT EXISTS (
          SELECT 1
          FROM sys.foreign_keys
          WHERE name = 'FK_CONTROL_AUTOMATION_SETTINGS_PORTFOLIOS'
        )
        BEGIN
          ALTER TABLE {schema}.CONTROL_AUTOMATION_SETTINGS
          ADD CONSTRAINT FK_CONTROL_AUTOMATION_SETTINGS_PORTFOLIOS
          FOREIGN KEY (PORTFOLIO_KEY)
          REFERENCES {schema}.CONTROL_PORTFOLIOS (PORTFOLIO_KEY);
        END
        IF NOT EXISTS (
          SELECT 1
          FROM sys.foreign_keys
          WHERE name = 'FK_CONTROL_AUTOMATION_RUNS_PORTFOLIOS'
        )
        BEGIN
          ALTER TABLE {schema}.CONTROL_AUTOMATION_RUNS
          ADD CONSTRAINT FK_CONTROL_AUTOMATION_RUNS_PORTFOLIOS
          FOREIGN KEY (PORTFOLIO_KEY)
          REFERENCES {schema}.CONTROL_PORTFOLIOS (PORTFOLIO_KEY);
        END
        """
    )


def list_automation_settings() -> pd.DataFrame:
    ensure_control_automation_tables()
    return control_fetch_df(
        """
        SELECT
          p.PORTFOLIO_KEY,
          p.DISPLAY_NAME,
          p.STATUS,
          COALESCE(s.ENABLE_AUTOMATION, 0) AS ENABLE_AUTOMATION,
          COALESCE(s.HOURLY_SYNC_ENABLED, 0) AS HOURLY_SYNC_ENABLED,
          COALESCE(s.HOURLY_SYNC_MINUTE, 10) AS HOURLY_SYNC_MINUTE,
          COALESCE(s.NIGHTLY_SYNC_ENABLED, 1) AS NIGHTLY_SYNC_ENABLED,
          COALESCE(s.NIGHTLY_SYNC_HOUR_UTC, 2) AS NIGHTLY_SYNC_HOUR_UTC,
          COALESCE(s.NIGHTLY_SYNC_MINUTE, 15) AS NIGHTLY_SYNC_MINUTE,
          COALESCE(s.REFRESH_ON_CHANGE, 1) AS REFRESH_ON_CHANGE,
          s.UPDATED_AT,
          s.UPDATED_BY
        FROM CONTROL_PORTFOLIOS p
        LEFT JOIN CONTROL_AUTOMATION_SETTINGS s
          ON s.PORTFOLIO_KEY = p.PORTFOLIO_KEY
        ORDER BY p.PORTFOLIO_KEY
        """
    )


def upsert_automation_setting(
    portfolio_key: str,
    *,
    enable_automation: bool,
    hourly_sync_enabled: bool,
    hourly_sync_minute: int,
    nightly_sync_enabled: bool,
    nightly_sync_hour_utc: int,
    nightly_sync_minute: int,
    refresh_on_change: bool,
    updated_by: str = "",
) -> None:
    ensure_control_automation_tables()
    pkey = str(portfolio_key or "").strip()
    if not pkey:
        raise RuntimeError("portfolio_key is required")
    h_min = max(0, min(59, int(hourly_sync_minute)))
    n_hour = max(0, min(23, int(nightly_sync_hour_utc)))
    n_min = max(0, min(59, int(nightly_sync_minute)))
    control_execute(
        """
        MERGE CONTROL_AUTOMATION_SETTINGS AS tgt
        USING (
          SELECT
            %s AS PORTFOLIO_KEY,
            %s AS ENABLE_AUTOMATION,
            %s AS HOURLY_SYNC_ENABLED,
            %s AS HOURLY_SYNC_MINUTE,
            %s AS NIGHTLY_SYNC_ENABLED,
            %s AS NIGHTLY_SYNC_HOUR_UTC,
            %s AS NIGHTLY_SYNC_MINUTE,
            %s AS REFRESH_ON_CHANGE,
            %s AS UPDATED_BY
        ) AS src
          ON tgt.PORTFOLIO_KEY = src.PORTFOLIO_KEY
        WHEN MATCHED THEN UPDATE SET
          ENABLE_AUTOMATION = src.ENABLE_AUTOMATION,
          HOURLY_SYNC_ENABLED = src.HOURLY_SYNC_ENABLED,
          HOURLY_SYNC_MINUTE = src.HOURLY_SYNC_MINUTE,
          NIGHTLY_SYNC_ENABLED = src.NIGHTLY_SYNC_ENABLED,
          NIGHTLY_SYNC_HOUR_UTC = src.NIGHTLY_SYNC_HOUR_UTC,
          NIGHTLY_SYNC_MINUTE = src.NIGHTLY_SYNC_MINUTE,
          REFRESH_ON_CHANGE = src.REFRESH_ON_CHANGE,
          UPDATED_AT = SYSUTCDATETIME(),
          UPDATED_BY = src.UPDATED_BY
        WHEN NOT MATCHED THEN INSERT (
          PORTFOLIO_KEY, ENABLE_AUTOMATION, HOURLY_SYNC_ENABLED, HOURLY_SYNC_MINUTE,
          NIGHTLY_SYNC_ENABLED, NIGHTLY_SYNC_HOUR_UTC, NIGHTLY_SYNC_MINUTE,
          REFRESH_ON_CHANGE, UPDATED_AT, UPDATED_BY
        )
        VALUES (
          src.PORTFOLIO_KEY, src.ENABLE_AUTOMATION, src.HOURLY_SYNC_ENABLED, src.HOURLY_SYNC_MINUTE,
          src.NIGHTLY_SYNC_ENABLED, src.NIGHTLY_SYNC_HOUR_UTC, src.NIGHTLY_SYNC_MINUTE,
          src.REFRESH_ON_CHANGE, SYSUTCDATETIME(), src.UPDATED_BY
        );
        """,
        (
            pkey,
            1 if bool(enable_automation) else 0,
            1 if bool(hourly_sync_enabled) else 0,
            h_min,
            1 if bool(nightly_sync_enabled) else 0,
            n_hour,
            n_min,
            1 if bool(refresh_on_change) else 0,
            str(updated_by or "").strip() or None,
        ),
    )


def queue_automation_run(
    portfolio_key: str,
    job_kind: str,
    *,
    requested_by: str = "",
    trigger_type: str = "manual",
    message: str = "",
) -> Optional[int]:
    ensure_control_automation_tables()
    pkey = str(portfolio_key or "").strip()
    jkind = str(job_kind or "").strip().upper()
    trig = str(trigger_type or "manual").strip().lower()
    if not pkey or not jkind:
        return None
    if trig not in {"manual", "schedule", "worker"}:
        trig = "manual"
    df = control_fetch_df(
        """
        INSERT INTO CONTROL_AUTOMATION_RUNS (
          PORTFOLIO_KEY, JOB_KIND, TRIGGER_TYPE, STATUS, REQUESTED_BY, MESSAGE
        )
        OUTPUT INSERTED.RUN_ID
        VALUES (%s, %s, %s, 'QUEUED', %s, %s)
        """,
        (
            pkey,
            jkind,
            trig,
            str(requested_by or "").strip() or None,
            str(message or "").strip() or None,
        ),
    )
    if df is None or df.empty:
        return None
    try:
        return int(df.iloc[0]["RUN_ID"])
    except Exception:
        return None


def list_automation_runs(limit: int = 100, portfolio_key: str = "") -> pd.DataFrame:
    ensure_control_automation_tables()
    top_n = max(1, min(500, int(limit or 100)))
    pkey = str(portfolio_key or "").strip()
    if pkey:
        return control_fetch_df(
            f"""
            SELECT TOP {top_n}
              RUN_ID, PORTFOLIO_KEY, JOB_KIND, TRIGGER_TYPE, STATUS, REQUESTED_BY,
              REQUESTED_AT, STARTED_AT, FINISHED_AT, ROWS_UPSERTED, ROWS_CHANGED, MESSAGE
            FROM CONTROL_AUTOMATION_RUNS
            WHERE PORTFOLIO_KEY = %s
            ORDER BY RUN_ID DESC
            """,
            (pkey,),
        )
    return control_fetch_df(
        f"""
        SELECT TOP {top_n}
          RUN_ID, PORTFOLIO_KEY, JOB_KIND, TRIGGER_TYPE, STATUS, REQUESTED_BY,
          REQUESTED_AT, STARTED_AT, FINISHED_AT, ROWS_UPSERTED, ROWS_CHANGED, MESSAGE
        FROM CONTROL_AUTOMATION_RUNS
        ORDER BY RUN_ID DESC
        """
    )


def bootstrap_control_db() -> None:
    create_control_schema()


def is_control_global_admin(email: str) -> bool:
    if not email:
        return False
    try:
        df = control_fetch_df(
            """
            SELECT COUNT(*) AS N
            FROM CONTROL_GLOBAL_ADMINS
            WHERE UPPER(LTRIM(RTRIM(USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
              AND COALESCE(IS_ACTIVE, 1) = 1
            """,
            (email,),
        )
        return bool(int(df.iloc[0]["N"])) if df is not None and not df.empty else False
    except Exception:
        return False


def seed_first_global_admin(email: str) -> None:
    if not email:
        return
    try:
        control_execute(
            """
            IF OBJECT_ID('CONTROL_GLOBAL_ADMINS', 'U') IS NULL
              BEGIN
                RETURN;
              END
            """,
            None,
        )
        df = control_fetch_df(
            """
            SELECT COUNT(*) AS N
            FROM CONTROL_GLOBAL_ADMINS
            WHERE UPPER(LTRIM(RTRIM(USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
            """,
            (email,),
        )
        n = int(df.iloc[0]["N"]) if df is not None and not df.empty else 0
        if n == 0:
            control_execute(
                """
                INSERT INTO CONTROL_GLOBAL_ADMINS (USER_EMAIL, UPDATED_AT)
                VALUES (%s, SYSUTCDATETIME())
                """,
                (email,),
            )
    except Exception:
        pass


def _normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def _normalize_groups(groups: Optional[Sequence[str]]) -> List[str]:
    vals: List[str] = []
    for g in list(groups or []):
        s = str(g or "").strip()
        if s:
            vals.append(s)
    return vals


def resolve_allowed_portfolios_debug(email: str, groups: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    """
    Resolve Control DB routing eligibility using direct memberships and optional AAD group rules.
    Returns keys only + split details for diagnostics.
    """
    em = _normalize_email(email)
    grp_vals = _normalize_groups(groups)
    if not em:
        return {
            "email": "",
            "groups_count": len(grp_vals),
            "manual_portfolios": [],
            "rule_portfolios": [],
            "allowed_portfolios": [],
            "rules_enabled": False,
        }

    manual_portfolios: List[str] = []
    rule_portfolios: List[str] = []
    rules_enabled = False
    try:
        df_manual = control_fetch_df(
            """
            SELECT DISTINCT p.PORTFOLIO_KEY
            FROM CONTROL_PORTFOLIOS p
            JOIN CONTROL_PORTFOLIO_USERS u
              ON LOWER(LTRIM(RTRIM(u.PORTFOLIO_KEY))) = LOWER(LTRIM(RTRIM(p.PORTFOLIO_KEY)))
            WHERE LOWER(LTRIM(RTRIM(u.USER_EMAIL))) = LOWER(LTRIM(RTRIM(%s)))
              AND COALESCE(u.IS_ACTIVE, 1) = 1
              AND COALESCE(p.STATUS,'ACTIVE') IN ('ACTIVE','ONBOARDING')
            """,
            (em,),
        )
        if df_manual is not None and not df_manual.empty:
            manual_portfolios = [str(x).strip() for x in df_manual["PORTFOLIO_KEY"].astype(str).tolist() if str(x).strip()]
    except Exception:
        manual_portfolios = []

    # Rules are optional. If table is missing/empty/inactive, rule_portfolios stays empty.
    try:
        df_rules_count = control_fetch_df(
            """
            SELECT COUNT(*) AS N
            FROM CONTROL_PORTFOLIO_ACCESS_RULES
            WHERE COALESCE(IS_ACTIVE, 1) = 1
            """
        )
        rules_enabled = bool(
            df_rules_count is not None
            and not df_rules_count.empty
            and int(df_rules_count.iloc[0].get("N", 0) or 0) > 0
        )
    except Exception:
        rules_enabled = False

    if rules_enabled and grp_vals:
        try:
            placeholders = ",".join(["%s"] * len(grp_vals))
            df_rules = control_fetch_df(
                f"""
                SELECT DISTINCT p.PORTFOLIO_KEY
                FROM CONTROL_PORTFOLIOS p
                JOIN CONTROL_PORTFOLIO_ACCESS_RULES r
                  ON LOWER(LTRIM(RTRIM(r.PORTFOLIO_KEY))) = LOWER(LTRIM(RTRIM(p.PORTFOLIO_KEY)))
                WHERE COALESCE(r.IS_ACTIVE, 1) = 1
                  AND COALESCE(p.STATUS,'ACTIVE') IN ('ACTIVE','ONBOARDING')
                  AND UPPER(LTRIM(RTRIM(r.RULE_TYPE))) = 'AAD_GROUP'
                  AND UPPER(LTRIM(RTRIM(r.RULE_VALUE))) IN ({placeholders})
                """,
                tuple([str(g).strip().upper() for g in grp_vals]),
            )
            if df_rules is not None and not df_rules.empty:
                rule_portfolios = [str(x).strip() for x in df_rules["PORTFOLIO_KEY"].astype(str).tolist() if str(x).strip()]
        except Exception:
            rule_portfolios = []

    allowed = sorted(set(manual_portfolios) | set(rule_portfolios))
    return {
        "email": em,
        "groups_count": len(grp_vals),
        "manual_portfolios": manual_portfolios,
        "rule_portfolios": rule_portfolios,
        "allowed_portfolios": allowed,
        "rules_enabled": rules_enabled,
    }


def resolve_allowed_portfolios(email: str, groups: Optional[Sequence[str]] = None) -> List[str]:
    debug = resolve_allowed_portfolios_debug(email, groups)
    return list(debug.get("allowed_portfolios") or [])
