from __future__ import annotations

import json
import uuid
from typing import Any, Dict, Optional

import pandas as pd

from db import execute, fetch_df, _fq

try:
    from utils.auth import current_user
except Exception:  # pragma: no cover
    def current_user() -> Optional[dict]:
        return None


def ensure_admin_audit_log_table() -> None:
    """Ensure ADMIN_AUDIT_LOG exists for lightweight admin action tracking."""
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADMIN_AUDIT_LOG')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADMIN_AUDIT_LOG')} (
            AUDIT_ID NVARCHAR(64) PRIMARY KEY,
            CREATED_AT DATETIME2 NOT NULL CONSTRAINT DF_ADMIN_AUDIT_LOG_CREATED_AT DEFAULT SYSUTCDATETIME(),
            USER_EMAIL NVARCHAR(255) NULL,
            ACTION_TYPE NVARCHAR(120) NULL,
            AREA NVARCHAR(120) NULL,
            ENTITY NVARCHAR(255) NULL,
            SUMMARY NVARCHAR(1000) NULL,
            EXTRA_JSON NVARCHAR(MAX) NULL
          );
        END
        """
    )


def log_admin_action(
    *,
    action_type: str,
    area: str,
    entity: str,
    summary: str,
    extra_json: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        ensure_admin_audit_log_table()
        user = current_user() or {}
        email = str(user.get("email") or user.get("EMAIL") or "").strip()
        payload = json.dumps(extra_json or {}, default=str) if extra_json else None
        audit_id = str(uuid.uuid4())
        execute(
            f"""
            INSERT INTO {_fq('ADMIN_AUDIT_LOG')}
              (AUDIT_ID, USER_EMAIL, ACTION_TYPE, AREA, ENTITY, SUMMARY, EXTRA_JSON)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (audit_id, email or None, action_type, area, entity, summary, payload),
        )
    except Exception:
        return


def get_recent_admin_actions(area: Optional[str] = None, limit: int = 20) -> pd.DataFrame:
    try:
        ensure_admin_audit_log_table()
        params: list[Any] = []
        where = ""
        if area:
            where = "WHERE AREA = %s"
            params.append(area)
        sql = f"""
            SELECT TOP {int(limit)}
              CREATED_AT, USER_EMAIL, ACTION_TYPE, AREA, ENTITY, SUMMARY, EXTRA_JSON
            FROM {_fq('ADMIN_AUDIT_LOG')}
            {where}
            ORDER BY CREATED_AT DESC
        """
        df = fetch_df(sql, tuple(params) if params else None)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()
