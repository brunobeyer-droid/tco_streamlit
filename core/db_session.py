from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from db.mssql_backend import MssqlConfig, _default_cfg, fetch_df, execute
from core.portfolio_context import get_portfolio_ctx
from core.portfolio_runtime import get_active_portfolio_db_cfg


def get_active_cfg() -> MssqlConfig:
    ctx = get_portfolio_ctx()
    if ctx is None:
        cfg_dict, _err = get_active_portfolio_db_cfg()
        if cfg_dict:
            return MssqlConfig(
                server=cfg_dict["server"],
                database=cfg_dict["database"],
                user=cfg_dict["user"],
                password=cfg_dict["password"],
                driver=cfg_dict["driver"],
                encrypt=str(cfg_dict.get("encrypt", True)).lower(),
                trust_server_certificate=str(cfg_dict.get("trust_server_certificate", False)).lower(),
                schema=cfg_dict.get("schema") or "dbo",
            )
        return _default_cfg()
    if ctx.db_server and ctx.db_database and ctx.db_user and ctx.db_password and ctx.db_driver:
        return MssqlConfig(
            server=ctx.db_server,
            database=ctx.db_database,
            user=ctx.db_user,
            password=ctx.db_password,
            driver=ctx.db_driver,
            encrypt=str(ctx.db_encrypt).lower(),
            trust_server_certificate=str(ctx.db_trust_server_certificate).lower(),
            schema=ctx.db_schema or "dbo",
        )
    cfg_dict, _err = get_active_portfolio_db_cfg()
    if cfg_dict:
        return MssqlConfig(
            server=cfg_dict["server"],
            database=cfg_dict["database"],
            user=cfg_dict["user"],
            password=cfg_dict["password"],
            driver=cfg_dict["driver"],
            encrypt=str(cfg_dict.get("encrypt", True)).lower(),
            trust_server_certificate=str(cfg_dict.get("trust_server_certificate", False)).lower(),
            schema=cfg_dict.get("schema") or "dbo",
        )
    base = _default_cfg()
    return MssqlConfig(
        server=ctx.db_server or base.server,
        database=ctx.db_database or base.database,
        user=ctx.db_user or base.user,
        password=ctx.db_password or base.password,
        driver=ctx.db_driver or base.driver,
        encrypt=str(ctx.db_encrypt).lower() if ctx.db_driver else base.encrypt,
        trust_server_certificate=str(ctx.db_trust_server_certificate).lower() if ctx.db_driver else base.trust_server_certificate,
        schema=ctx.db_schema or base.schema,
    )


def db_fetch_df(sql: str, params: Optional[Any] = None) -> pd.DataFrame:
    return fetch_df(sql, params, cfg=get_active_cfg())


def db_execute(sql: str, params: Optional[Any] = None) -> None:
    execute(sql, params, cfg=get_active_cfg())


def db_scalar(sql: str, params: Optional[Any] = None) -> Optional[Any]:
    df = db_fetch_df(sql, params)
    if df is None or df.empty:
        return None
    return df.iloc[0, 0]
