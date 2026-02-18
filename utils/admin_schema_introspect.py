from __future__ import annotations

from typing import Optional

import pandas as pd

from db import fetch_df, execute, _fq


def list_db_tables() -> pd.DataFrame:
    try:
        return fetch_df(
            """
            SELECT s.name AS SCHEMA_NAME, t.name AS OBJECT_NAME, t.create_date AS CREATED_AT, t.modify_date AS MODIFIED_AT
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            ORDER BY s.name, t.name
            """
        )
    except Exception:
        return pd.DataFrame(columns=["SCHEMA_NAME", "OBJECT_NAME", "CREATED_AT", "MODIFIED_AT"])


def list_db_views() -> pd.DataFrame:
    try:
        return fetch_df(
            """
            SELECT s.name AS SCHEMA_NAME, v.name AS OBJECT_NAME, v.create_date AS CREATED_AT, v.modify_date AS MODIFIED_AT
            FROM sys.views v
            JOIN sys.schemas s ON s.schema_id = v.schema_id
            ORDER BY s.name, v.name
            """
        )
    except Exception:
        return pd.DataFrame(columns=["SCHEMA_NAME", "OBJECT_NAME", "CREATED_AT", "MODIFIED_AT"])


def list_db_columns(object_name: str) -> pd.DataFrame:
    try:
        return fetch_df(
            """
            SELECT c.name AS COLUMN_NAME, t.name AS DATA_TYPE, c.max_length AS MAX_LENGTH,
                   c.is_nullable AS IS_NULLABLE
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(%s)
            ORDER BY c.column_id
            """,
            (f"dbo.{object_name}",),
        )
    except Exception:
        return pd.DataFrame(columns=["COLUMN_NAME", "DATA_TYPE", "MAX_LENGTH", "IS_NULLABLE"])


def list_view_dependencies() -> pd.DataFrame:
    try:
        return fetch_df(
            """
            SELECT
              OBJECT_NAME(d.referencing_id) AS VIEW_NAME,
              COALESCE(OBJECT_NAME(d.referenced_id), d.referenced_entity_name) AS REFERENCED_OBJECT
            FROM sys.sql_expression_dependencies d
            WHERE OBJECT_NAME(d.referencing_id) IS NOT NULL
            ORDER BY VIEW_NAME, REFERENCED_OBJECT
            """
        )
    except Exception:
        return pd.DataFrame(columns=["VIEW_NAME", "REFERENCED_OBJECT"])


def object_exists(name: str, kind: str = "table") -> bool:
    try:
        if kind == "view":
            df = fetch_df("SELECT 1 AS OK FROM sys.views WHERE name = %s", (name,))
        else:
            df = fetch_df("SELECT 1 AS OK FROM sys.tables WHERE name = %s", (name,))
        return df is not None and not df.empty
    except Exception:
        return False


def get_row_count(table_name: str) -> Optional[int]:
    try:
        df = fetch_df(f"SELECT COUNT(*) AS CNT FROM { _fq(table_name) }")
        if df is None or df.empty:
            return None
        return int(df.iloc[0]["CNT"])
    except Exception:
        return None


def ensure_deprecated_objects_table() -> None:
    try:
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1 FROM INFORMATION_SCHEMA.TABLES
              WHERE TABLE_NAME = 'ADMIN_DEPRECATED_OBJECTS'
            )
            BEGIN
              CREATE TABLE { _fq('ADMIN_DEPRECATED_OBJECTS') } (
                OBJECT_NAME NVARCHAR(255) NOT NULL,
                OBJECT_TYPE NVARCHAR(50) NOT NULL,
                DEPRECATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                DEPRECATED_BY NVARCHAR(255) NULL,
                NOTES NVARCHAR(1000) NULL
              )
            END
            """
        )
    except Exception:
        pass


def list_deprecated_objects() -> pd.DataFrame:
    try:
        return fetch_df(f"SELECT OBJECT_NAME, OBJECT_TYPE, DEPRECATED_AT, DEPRECATED_BY, NOTES FROM { _fq('ADMIN_DEPRECATED_OBJECTS') }")
    except Exception:
        return pd.DataFrame(columns=["OBJECT_NAME", "OBJECT_TYPE", "DEPRECATED_AT", "DEPRECATED_BY", "NOTES"])


def mark_deprecated(object_name: str, object_type: str, user_email: Optional[str], notes: Optional[str]) -> None:
    ensure_deprecated_objects_table()
    execute(
        f"""
        INSERT INTO { _fq('ADMIN_DEPRECATED_OBJECTS') } (OBJECT_NAME, OBJECT_TYPE, DEPRECATED_BY, NOTES)
        VALUES (%s, %s, %s, %s)
        """,
        (object_name, object_type, user_email, notes),
    )
