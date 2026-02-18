from __future__ import annotations

# DB facade: Azure SQL only (Snowflake removed)
from .mssql_backend import *  # noqa: F401,F403
from .mssql_backend import execute as _execute_raw  # noqa: F401

# Explicitly re-export internal helpers some pages import directly
from .mssql_backend import _fq as _fq  # noqa: F401
try:
    from .mssql_backend import _svq as _svq  # noqa: F401
except Exception:
    pass

# Active-portfolio DB wrappers (minimal shim)
def fetch_df_active(sql, params=None):  # type: ignore
    try:
        from core.db_session import db_fetch_df
        return db_fetch_df(sql, params)
    except Exception:
        # Fallback to default single-DB behavior
        return fetch_df(sql, params)  # type: ignore[name-defined]


def execute_active(sql, params=None, *, many=False):  # type: ignore
    try:
        from core.db_session import db_execute
        return db_execute(sql, params, many=many)
    except Exception:
        return execute(sql, params, many=many)  # type: ignore[name-defined]

def execute_control(sql, params=None):  # type: ignore
    from db.control_db import control_execute, control_db_available
    if not control_db_available():
        raise RuntimeError("Control DB config missing; use CONTROL_MSSQL_* env vars.")
    return control_execute(sql, params)

def execute_portfolio(sql, params=None, *, many=False):  # type: ignore
    from core.db_session import db_execute
    return db_execute(sql, params, many=many)

def execute(sql, params=None, *args, **kwargs):  # type: ignore
    if isinstance(sql, str) and "CONTROL_" in sql.upper():
        raise RuntimeError("Control DB operation detected. Use execute_control().")
    many = bool(kwargs.pop("many", False))
    return execute_portfolio(sql, params, many=many)

# Apptio actuals/mapping helpers
try:
    from .mssql_backend import (
        upsert_program_apptio_workid as upsert_program_apptio_workid,  # noqa: F401
        delete_program_apptio_workid as delete_program_apptio_workid,  # noqa: F401
        list_program_apptio_workids as list_program_apptio_workids,    # noqa: F401
        upsert_apptio_actuals as upsert_apptio_actuals,                # noqa: F401
        upsert_apptio_actuals_lines as upsert_apptio_actuals_lines,    # noqa: F401
        fetch_apptio_actuals_by_program as fetch_apptio_actuals_by_program,  # noqa: F401
        fetch_apptio_actuals_by_program_breakdown as fetch_apptio_actuals_by_program_breakdown,  # noqa: F401
        upsert_apptio_to_cost_type_mappings as upsert_apptio_to_cost_type_mappings,  # noqa: F401
        ensure_apptio_actuals_lines_table as ensure_apptio_actuals_lines_table,  # noqa: F401
        ensure_map_apptio_to_cost_type_table as ensure_map_apptio_to_cost_type_table,  # noqa: F401
    )
except Exception:
    pass

# New helpers for contracts/forecast
try:
    from .mssql_backend import ensure_contracts_table as ensure_contracts_table  # noqa: F401
    from .mssql_backend import upsert_contract as upsert_contract  # noqa: F401
    from .mssql_backend import ensure_invoice_forecast_views as ensure_invoice_forecast_views  # noqa: F401
    from .mssql_backend import sync_contract_invoices as sync_contract_invoices  # noqa: F401
    from .mssql_backend import ensure_email_alert_config_table as ensure_email_alert_config_table  # noqa: F401
    from .mssql_backend import get_email_alert_config as get_email_alert_config  # noqa: F401
    from .mssql_backend import save_email_alert_config as save_email_alert_config  # noqa: F401
except Exception:
    # Safe when referenced in pages; no-op if backend not available
    pass
