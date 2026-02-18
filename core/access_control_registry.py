from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from db.control_db import control_fetch_df, ensure_control_schema, control_db_available


def list_user_portfolios(user_email: str) -> List[Dict[str, Any]]:
    if not user_email:
        return []
    if not control_db_available():
        return []
    status = ensure_control_schema()
    if not status.get("ok"):
        return []
    df = control_fetch_df(
        """
        SELECT
          p.PORTFOLIO_KEY,
          p.DISPLAY_NAME,
          p.DB_SERVER,
          p.DB_DATABASE,
          p.DB_USER,
          p.DB_SCHEMA,
          p.PROFILE_KEY,
          p.ADO_ORG_URL,
          p.ADO_PROJECT,
          p.PAT_ENV_KEY,
          p.STATUS,
          u.ROLE
        FROM CONTROL_PORTFOLIOS p
        INNER JOIN CONTROL_PORTFOLIO_USERS u
          ON u.PORTFOLIO_KEY = p.PORTFOLIO_KEY
        WHERE UPPER(LTRIM(RTRIM(u.USER_EMAIL))) = UPPER(LTRIM(RTRIM(%s)))
          AND (p.STATUS IS NULL OR UPPER(p.STATUS) <> 'DISABLED')
        ORDER BY p.PORTFOLIO_KEY
        """,
        (user_email,),
    )
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


def get_control_db_status() -> Dict[str, Any]:
    try:
        status = ensure_control_schema()
        return status
    except Exception as e:
        return {"ok": False, "error": str(e)}
