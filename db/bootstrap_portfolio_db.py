from __future__ import annotations

from typing import Any, Dict, List

from db.mssql_backend import (
    MssqlConfig,
    execute,
    fetch_df,
    ensure_tables,
    ensure_ado_minimal_tables,
    ensure_apptio_actuals_lines_table,
    ensure_map_apptio_to_cost_type_table,
    ensure_all_views_ok,
    ensure_analytics_views_ok,
    ensure_user_membership_tables,
    _fq,
)
from db.control_db import control_db_available, create_control_schema, control_execute
from core.portfolio_context import get_active_db_config, get_active_portfolio_key, set_active_portfolio_context


def _table_exists(name: str, cfg: MssqlConfig) -> bool:
    df = fetch_df(
        """
        SELECT COUNT(*) AS N
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """,
        (cfg.schema or "dbo", name),
        cfg=cfg,
    )
    return bool(int(df.iloc[0]["N"])) if df is not None and not df.empty else False


def _sql_app_users_prepare(cfg: MssqlConfig) -> str:
    return f"""
    IF COL_LENGTH('{_fq('APP_USERS', cfg)}', 'UPDATED_AT') IS NULL
    BEGIN
      ALTER TABLE {_fq('APP_USERS', cfg)} ADD UPDATED_AT DATETIME2 NULL;
    END

    DECLARE @app_users_obj_id INT = OBJECT_ID('{_fq('APP_USERS', cfg)}');
    IF @app_users_obj_id IS NOT NULL
    BEGIN
      DECLARE @userid_type SYSNAME = NULL;
      DECLARE @userid_is_identity BIT = 0;
      DECLARE @userid_has_default BIT = 0;

      SELECT TOP 1
        @userid_type = t.name,
        @userid_is_identity = c.is_identity
      FROM sys.columns c
      JOIN sys.types t ON c.user_type_id = t.user_type_id
      WHERE c.object_id = @app_users_obj_id
        AND c.name = 'USERID';

      SELECT
        @userid_has_default = CASE WHEN EXISTS (
          SELECT 1
          FROM sys.default_constraints dc
          JOIN sys.columns c
            ON c.object_id = dc.parent_object_id
           AND c.column_id = dc.parent_column_id
          WHERE dc.parent_object_id = @app_users_obj_id
            AND c.name = 'USERID'
        ) THEN 1 ELSE 0 END;

      IF @userid_has_default = 0
         AND @userid_is_identity = 0
         AND @userid_type = 'uniqueidentifier'
      BEGIN
        ALTER TABLE {_fq('APP_USERS', cfg)} ADD DEFAULT NEWID() FOR USERID;
      END
    END
    """


def _sql_seed_admin_app_users(cfg: MssqlConfig) -> str:
    return f"""
    DECLARE @email NVARCHAR(320) = %s;
    DECLARE @disp NVARCHAR(200) = LEFT(REPLACE(REPLACE(@email, '.', ' '), '@', ' @'), 200);
    IF NOT EXISTS (
      SELECT 1 FROM {_fq('APP_USERS', cfg)}
      WHERE UPPER(LTRIM(RTRIM(EMAIL))) = UPPER(LTRIM(RTRIM(@email)))
    )
    BEGIN
      DECLARE @type_name NVARCHAR(128);
      DECLARE @is_identity BIT = 0;
      DECLARE @has_created BIT = IIF(COL_LENGTH('{_fq('APP_USERS', cfg)}','CREATED_AT') IS NULL, 0, 1);
      DECLARE @has_updated BIT = IIF(COL_LENGTH('{_fq('APP_USERS', cfg)}','UPDATED_AT') IS NULL, 0, 1);
      SELECT @type_name = t.name,
             @is_identity = c.is_identity
      FROM sys.columns c
      JOIN sys.types t ON c.user_type_id = t.user_type_id
      WHERE c.object_id = OBJECT_ID('{_fq('APP_USERS', cfg)}') AND c.name = 'USERID';

      IF COL_LENGTH('{_fq('APP_USERS', cfg)}','IS_ACTIVE') IS NULL
      BEGIN
        IF @is_identity = 1
        BEGIN
          IF @has_created = 1 AND @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (EMAIL, DISPLAY_NAME, ROLE, CREATED_AT, UPDATED_AT)
            VALUES (@email, @disp, 'ADMIN', SYSUTCDATETIME(), SYSUTCDATETIME());
          ELSE IF @has_created = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (EMAIL, DISPLAY_NAME, ROLE, CREATED_AT)
            VALUES (@email, @disp, 'ADMIN', SYSUTCDATETIME());
          ELSE IF @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (EMAIL, DISPLAY_NAME, ROLE, UPDATED_AT)
            VALUES (@email, @disp, 'ADMIN', SYSUTCDATETIME());
          ELSE
            INSERT INTO {_fq('APP_USERS', cfg)} (EMAIL, DISPLAY_NAME, ROLE)
            VALUES (@email, @disp, 'ADMIN');
        END
        ELSE IF @type_name = 'uniqueidentifier'
        BEGIN
          IF @has_created = 1 AND @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, CREATED_AT, UPDATED_AT)
            VALUES (NEWID(), @email, @disp, 'ADMIN', SYSUTCDATETIME(), SYSUTCDATETIME());
          ELSE IF @has_created = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, CREATED_AT)
            VALUES (NEWID(), @email, @disp, 'ADMIN', SYSUTCDATETIME());
          ELSE IF @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, UPDATED_AT)
            VALUES (NEWID(), @email, @disp, 'ADMIN', SYSUTCDATETIME());
          ELSE
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE)
            VALUES (NEWID(), @email, @disp, 'ADMIN');
        END
        ELSE IF @type_name IN ('int','bigint','smallint','tinyint')
        BEGIN
          DECLARE @next_id BIGINT = (
            SELECT ISNULL(MAX(TRY_CONVERT(BIGINT, CONVERT(NVARCHAR(100), USERID))), 0) + 1
            FROM {_fq('APP_USERS', cfg)}
          );
          IF @has_created = 1 AND @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, CREATED_AT, UPDATED_AT)
            VALUES (@next_id, @email, @disp, 'ADMIN', SYSUTCDATETIME(), SYSUTCDATETIME());
          ELSE IF @has_created = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, CREATED_AT)
            VALUES (@next_id, @email, @disp, 'ADMIN', SYSUTCDATETIME());
          ELSE IF @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, UPDATED_AT)
            VALUES (@next_id, @email, @disp, 'ADMIN', SYSUTCDATETIME());
          ELSE
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE)
            VALUES (@next_id, @email, @disp, 'ADMIN');
        END
        ELSE
        BEGIN
          IF @has_created = 1 AND @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, CREATED_AT, UPDATED_AT)
            VALUES (CONVERT(VARCHAR(36), NEWID()), @email, @disp, 'ADMIN', SYSUTCDATETIME(), SYSUTCDATETIME());
          ELSE IF @has_created = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, CREATED_AT)
            VALUES (CONVERT(VARCHAR(36), NEWID()), @email, @disp, 'ADMIN', SYSUTCDATETIME());
          ELSE IF @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, UPDATED_AT)
            VALUES (CONVERT(VARCHAR(36), NEWID()), @email, @disp, 'ADMIN', SYSUTCDATETIME());
          ELSE
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE)
            VALUES (CONVERT(VARCHAR(36), NEWID()), @email, @disp, 'ADMIN');
        END
      END
      ELSE
      BEGIN
        IF @is_identity = 1
        BEGIN
          IF @has_created = 1 AND @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, CREATED_AT, UPDATED_AT)
            VALUES (@email, @disp, 'ADMIN', 1, SYSUTCDATETIME(), SYSUTCDATETIME());
          ELSE IF @has_created = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, CREATED_AT)
            VALUES (@email, @disp, 'ADMIN', 1, SYSUTCDATETIME());
          ELSE IF @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, UPDATED_AT)
            VALUES (@email, @disp, 'ADMIN', 1, SYSUTCDATETIME());
          ELSE
            INSERT INTO {_fq('APP_USERS', cfg)} (EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE)
            VALUES (@email, @disp, 'ADMIN', 1);
        END
        ELSE IF @type_name = 'uniqueidentifier'
        BEGIN
          IF @has_created = 1 AND @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, CREATED_AT, UPDATED_AT)
            VALUES (NEWID(), @email, @disp, 'ADMIN', 1, SYSUTCDATETIME(), SYSUTCDATETIME());
          ELSE IF @has_created = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, CREATED_AT)
            VALUES (NEWID(), @email, @disp, 'ADMIN', 1, SYSUTCDATETIME());
          ELSE IF @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, UPDATED_AT)
            VALUES (NEWID(), @email, @disp, 'ADMIN', 1, SYSUTCDATETIME());
          ELSE
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE)
            VALUES (NEWID(), @email, @disp, 'ADMIN', 1);
        END
        ELSE IF @type_name IN ('int','bigint','smallint','tinyint')
        BEGIN
          DECLARE @next_id2 BIGINT = (
            SELECT ISNULL(MAX(TRY_CONVERT(BIGINT, CONVERT(NVARCHAR(100), USERID))), 0) + 1
            FROM {_fq('APP_USERS', cfg)}
          );
          IF @has_created = 1 AND @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, CREATED_AT, UPDATED_AT)
            VALUES (@next_id2, @email, @disp, 'ADMIN', 1, SYSUTCDATETIME(), SYSUTCDATETIME());
          ELSE IF @has_created = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, CREATED_AT)
            VALUES (@next_id2, @email, @disp, 'ADMIN', 1, SYSUTCDATETIME());
          ELSE IF @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, UPDATED_AT)
            VALUES (@next_id2, @email, @disp, 'ADMIN', 1, SYSUTCDATETIME());
          ELSE
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE)
            VALUES (@next_id2, @email, @disp, 'ADMIN', 1);
        END
        ELSE
        BEGIN
          IF @has_created = 1 AND @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, CREATED_AT, UPDATED_AT)
            VALUES (CONVERT(VARCHAR(36), NEWID()), @email, @disp, 'ADMIN', 1, SYSUTCDATETIME(), SYSUTCDATETIME());
          ELSE IF @has_created = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, CREATED_AT)
            VALUES (CONVERT(VARCHAR(36), NEWID()), @email, @disp, 'ADMIN', 1, SYSUTCDATETIME());
          ELSE IF @has_updated = 1
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, UPDATED_AT)
            VALUES (CONVERT(VARCHAR(36), NEWID()), @email, @disp, 'ADMIN', 1, SYSUTCDATETIME());
          ELSE
            INSERT INTO {_fq('APP_USERS', cfg)} (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE)
            VALUES (CONVERT(VARCHAR(36), NEWID()), @email, @disp, 'ADMIN', 1);
        END
      END
    END
    """


def _sql_create_tco_admins(cfg: MssqlConfig) -> str:
    return f"""
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables WHERE name = 'TCO_APP_ADMINS' AND schema_id = SCHEMA_ID('{cfg.schema}')
    )
    BEGIN
      CREATE TABLE {cfg.database}.{cfg.schema}.TCO_APP_ADMINS (
        USER_EMAIL NVARCHAR(256) PRIMARY KEY,
        ROLE NVARCHAR(32),
        CREATED_AT DATETIME2 DEFAULT SYSUTCDATETIME()
      );
    END
    """


def _sql_seed_tco_admins(cfg: MssqlConfig) -> str:
    return f"""
    IF NOT EXISTS (
      SELECT 1 FROM {cfg.database}.{cfg.schema}.TCO_APP_ADMINS
      WHERE UPPER(LTRIM(RTRIM(USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
    )
    BEGIN
      INSERT INTO {cfg.database}.{cfg.schema}.TCO_APP_ADMINS (USER_EMAIL, ROLE)
      VALUES (%s, 'ADMIN')
    END
    """


def _sql_settings_table(cfg: MssqlConfig) -> str:
    return f"""
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables WHERE name = 'TCO_PORTFOLIO_SETTINGS' AND schema_id = SCHEMA_ID('{cfg.schema}')
    )
    BEGIN
      CREATE TABLE {cfg.database}.{cfg.schema}.TCO_PORTFOLIO_SETTINGS (
        [KEY] NVARCHAR(255) PRIMARY KEY,
        [VALUE] NVARCHAR(255) NULL,
        UPDATED_AT DATETIME2 DEFAULT SYSUTCDATETIME()
      );
    END
    """


def _sql_settings_merge(cfg: MssqlConfig) -> str:
    return f"""
    MERGE {cfg.database}.{cfg.schema}.TCO_PORTFOLIO_SETTINGS AS tgt
    USING (SELECT 'PROFILE_KEY' AS [KEY], %s AS [VALUE]) AS src
    ON tgt.[KEY] = src.[KEY]
    WHEN MATCHED THEN UPDATE SET [VALUE] = src.[VALUE], UPDATED_AT = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT ([KEY], [VALUE]) VALUES (src.[KEY], src.[VALUE]);
    """


def plan_portfolio_bootstrap(cfg: MssqlConfig, profile_key: str, admin_email: str) -> Dict[str, Any]:
    actions: List[Dict[str, Any]] = []
    warnings: List[str] = []

    # Keep helper-driven bootstrap explicit in plan; these are only executed on Initialize schema.
    actions.extend([
        {"kind": "ensure_helper", "name": "ensure_tables", "sql": "ensure_tables()", "helper": "ensure_tables"},
        {"kind": "ensure_helper", "name": "ensure_ado_minimal_tables", "sql": "ensure_ado_minimal_tables()", "helper": "ensure_ado_minimal_tables"},
        {"kind": "ensure_helper", "name": "ensure_apptio_actuals_lines_table", "sql": "ensure_apptio_actuals_lines_table()", "helper": "ensure_apptio_actuals_lines_table"},
        {"kind": "ensure_helper", "name": "ensure_map_apptio_to_cost_type_table", "sql": "ensure_map_apptio_to_cost_type_table()", "helper": "ensure_map_apptio_to_cost_type_table"},
        {"kind": "ensure_helper", "name": "ensure_all_views_ok", "sql": "ensure_all_views_ok()", "helper": "ensure_all_views_ok"},
        {"kind": "ensure_helper", "name": "ensure_analytics_views_ok", "sql": "ensure_analytics_views_ok()", "helper": "ensure_analytics_views_ok"},
        {"kind": "ensure_helper", "name": "ensure_user_membership_tables", "sql": "ensure_user_membership_tables()", "helper": "ensure_user_membership_tables"},
    ])

    if _table_exists("APP_USERS", cfg):
        actions.append({"kind": "alter_table_add_column", "name": _fq("APP_USERS", cfg), "sql": _sql_app_users_prepare(cfg), "params": None})
        if admin_email:
            actions.append({"kind": "seed_admin", "name": _fq("APP_USERS", cfg), "sql": _sql_seed_admin_app_users(cfg), "params": (admin_email,)})
    else:
        actions.append({"kind": "create_table", "name": f"{cfg.database}.{cfg.schema}.TCO_APP_ADMINS", "sql": _sql_create_tco_admins(cfg), "params": None})
        if admin_email:
            actions.append({"kind": "seed_admin", "name": f"{cfg.database}.{cfg.schema}.TCO_APP_ADMINS", "sql": _sql_seed_tco_admins(cfg), "params": (admin_email, admin_email)})

    actions.append({"kind": "create_table", "name": f"{cfg.database}.{cfg.schema}.TCO_PORTFOLIO_SETTINGS", "sql": _sql_settings_table(cfg), "params": None})
    actions.append({"kind": "upsert_setting", "name": f"{cfg.database}.{cfg.schema}.TCO_PORTFOLIO_SETTINGS", "sql": _sql_settings_merge(cfg), "params": (profile_key,)})

    summary: Dict[str, int] = {}
    for a in actions:
        k = str(a.get("kind") or "other")
        summary[k] = int(summary.get(k, 0)) + 1

    if not admin_email:
        warnings.append("Admin email is empty; admin seed action is skipped.")

    return {
        "actions": actions,
        "warnings": warnings,
        "summary": summary,
    }


def bootstrap_portfolio_db(
    cfg: MssqlConfig,
    profile_key: str,
    admin_email: str,
    dry_run: bool = False,
    portfolio_key: str | None = None,
) -> Dict[str, Any]:
    prev_ctx = get_active_db_config()
    prev_key = get_active_portfolio_key()
    try:
        set_active_portfolio_context("bootstrap", profile_key, {
            "server": cfg.server,
            "database": cfg.database,
            "user": cfg.user,
            "password": cfg.password,
            "driver": cfg.driver,
            "encrypt": cfg.encrypt,
            "trust_server_certificate": cfg.trust_server_certificate,
            "schema": cfg.schema,
        })

        plan = plan_portfolio_bootstrap(cfg, profile_key, admin_email)
        if dry_run:
            return {
                "ok": True,
                "dry_run": True,
                **plan,
            }

        helper_map = {
            "ensure_tables": ensure_tables,
            "ensure_ado_minimal_tables": ensure_ado_minimal_tables,
            "ensure_apptio_actuals_lines_table": ensure_apptio_actuals_lines_table,
            "ensure_map_apptio_to_cost_type_table": ensure_map_apptio_to_cost_type_table,
            "ensure_all_views_ok": ensure_all_views_ok,
            "ensure_analytics_views_ok": ensure_analytics_views_ok,
            "ensure_user_membership_tables": ensure_user_membership_tables,
        }

        executed: List[str] = []
        for action in plan["actions"]:
            kind = str(action.get("kind") or "")
            if kind == "ensure_helper":
                helper = helper_map.get(str(action.get("helper") or ""))
                if helper is None:
                    raise RuntimeError(f"Unknown bootstrap helper: {action.get('helper')}")
                helper()
                executed.append(str(action.get("name") or action.get("helper") or "helper"))
                continue

            sql = str(action.get("sql") or "").strip()
            params = action.get("params")
            try:
                execute(sql, params, cfg=cfg)
            except Exception as e:
                first_line = (sql.splitlines()[0].strip() if sql else "<empty>")
                raise RuntimeError(f"Execute failed. First line: {first_line}. Error: {e}")
            executed.append(str(action.get("name") or kind or "sql"))

        counts = {}
        for t in ["PROGRAMS", "TEAMS", "ADO_FEATURES", "ADO_ITERATION_CALENDAR"]:
            try:
                df = fetch_df(f"SELECT COUNT(*) AS N FROM {_fq(t, cfg)}", cfg=cfg)
                counts[t] = int(df.iloc[0]["N"]) if df is not None and not df.empty else 0
            except Exception:
                counts[t] = None

        extra_warnings: List[str] = []
        membership_key = str(portfolio_key or profile_key or "").strip()
        membership_email = str(admin_email or "").strip()
        if membership_key and membership_email:
            try:
                if control_db_available():
                    create_control_schema()
                    control_execute(
                        """
                        MERGE CONTROL_PORTFOLIO_USERS AS tgt
                        USING (SELECT %s AS USER_EMAIL, %s AS PORTFOLIO_KEY) AS src
                        ON UPPER(LTRIM(RTRIM(tgt.USER_EMAIL))) = UPPER(LTRIM(RTRIM(src.USER_EMAIL)))
                           AND UPPER(LTRIM(RTRIM(tgt.PORTFOLIO_KEY))) = UPPER(LTRIM(RTRIM(src.PORTFOLIO_KEY)))
                        WHEN MATCHED THEN UPDATE SET ROLE = 'ADMIN', IS_ACTIVE = 1, UPDATED_AT = SYSUTCDATETIME()
                        WHEN NOT MATCHED THEN
                          INSERT (USER_EMAIL, PORTFOLIO_KEY, ROLE, IS_ACTIVE) VALUES (%s, %s, 'ADMIN', 1);
                        """,
                        (membership_email, membership_key, membership_email, membership_key),
                    )
                else:
                    extra_warnings.append("Control DB unavailable; skipped CONTROL_PORTFOLIO_USERS admin upsert.")
            except Exception as e:
                extra_warnings.append(f"Control DB admin upsert failed: {e}")

        return {
            "ok": True,
            "dry_run": False,
            "executed": len(executed),
            "executed_objects": executed,
            "actions": plan.get("actions") or [],
            "summary": plan.get("summary") or {},
            "warnings": (plan.get("warnings") or []) + extra_warnings,
            "counts": counts,
        }
    finally:
        if prev_ctx:
            set_active_portfolio_context(prev_key, profile_key, prev_ctx)
