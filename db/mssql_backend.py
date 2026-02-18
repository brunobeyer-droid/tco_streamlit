from __future__ import annotations

# Azure SQL (MSSQL) backend for the app. This provides minimal-compatible
# functions so app code can `from db import ...` regardless of backend.

import json
import logging
import os
import re
import time
import warnings
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from functools import wraps
from threading import Lock, local
from typing import Any, Iterable, Optional, Sequence, Tuple, List, Dict, Callable

import pandas as pd

# Feature flag: allow DB-stored PAT fallback (default False).
ALLOW_DB_PAT_FALLBACK = str(os.getenv("ALLOW_DB_PAT_FALLBACK", "0")).lower() in {"1", "true", "yes"}
TCO_FRESHNESS_V2 = str(os.getenv("TCO_FRESHNESS_V2", "1")).strip().lower() in {"1", "true", "yes", "on"}

_WRITE_TRACKER = local()


@dataclass(frozen=True)
class MssqlConfig:
    server: str
    database: str
    user: str
    password: str
    driver: str
    encrypt: str
    trust_server_certificate: str
    schema: str = "dbo"

# Make Streamlit cache decorators no-op when running outside Streamlit runtime
try:  # pragma: no cover - runtime dependent
    import streamlit as _st  # type: ignore
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx  # type: ignore

        if get_script_run_ctx() is None:
            def _noop_deco(*args, **kwargs):  # type: ignore
                def deco(fn):
                    return fn
                return deco

            _st.cache_resource = _noop_deco  # type: ignore[attr-defined]
    except Exception:
        pass
except Exception:
    # If Streamlit isn't present in this context, provide a minimal shim
    class _Shim:
        def cache_resource(self, *args, **kwargs):
            def deco(fn):
                return fn
            return deco

        secrets: dict = {}

    _st = _Shim()  # type: ignore


# =========================================================
# Config / connection
# =========================================================

def _read_mssql_secrets() -> dict:
    """Read MSSQL config from st.secrets or environment variables.

    Streamlit's `st.secrets` is a mapping-like object (not a plain dict), so avoid
    `isinstance(..., dict)` checks and use duck typing instead.
    """
    secrets_obj = getattr(_st, "secrets", None)
    try:
        if secrets_obj and ("mssql" in secrets_obj):  # type: ignore[operator]
            cfg = secrets_obj["mssql"]  # type: ignore[index]
            # Use .get if available; otherwise fall back to indexing
            def _g(k: str, default: Optional[str] = None):
                try:
                    return cfg.get(k, default)  # type: ignore[attr-defined]
                except Exception:
                    try:
                        return cfg[k]  # type: ignore[index]
                    except Exception:
                        return default

            return {
                "server": _g("server"),
                "database": _g("database"),
                "user": _g("user"),
                "password": _g("password"),
                "driver": _g("driver", "ODBC Driver 17 for SQL Server"),
                "encrypt": _g("encrypt", "yes"),
                "trust_server_certificate": _g("trust_server_certificate", "no"),
            }
    except Exception:
        # fall through to env vars
        pass
    return {
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "user": os.getenv("MSSQL_USER"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        "encrypt": os.getenv("MSSQL_ENCRYPT", "yes"),
        "trust_server_certificate": os.getenv("MSSQL_TRUST_SERVER_CERTIFICATE", "no"),
    }


def _cfg_from_mapping(raw: Dict[str, Any]) -> Optional[MssqlConfig]:
    if not isinstance(raw, dict):
        return None
    server = str(raw.get("server") or "").strip()
    database = str(raw.get("database") or "").strip()
    user = str(raw.get("user") or "").strip()
    password = str(raw.get("password") or "").strip()
    driver = str(raw.get("driver") or "ODBC Driver 17 for SQL Server").strip()
    encrypt = str(raw.get("encrypt") or "yes").strip()
    trust = str(raw.get("trust_server_certificate") or "no").strip()
    schema = str(raw.get("schema") or "dbo").strip() or "dbo"
    if not (server and database and user and password):
        return None
    return MssqlConfig(
        server=server,
        database=database,
        user=user,
        password=password,
        driver=driver,
        encrypt=encrypt,
        trust_server_certificate=trust,
        schema=schema,
    )


def _default_cfg() -> MssqlConfig:
    secrets = _read_mssql_secrets()
    schema = os.getenv("MSSQL_SCHEMA")
    if not schema:
        try:
            secrets_obj = getattr(_st, "secrets", None)
            if secrets_obj and ("mssql" in secrets_obj):  # type: ignore[operator]
                cfg = secrets_obj["mssql"]  # type: ignore[index]
                try:
                    schema = cfg.get("schema")  # type: ignore[attr-defined]
                except Exception:
                    try:
                        schema = cfg["schema"]  # type: ignore[index]
                    except Exception:
                        schema = None
        except Exception:
            schema = None
    schema = schema or "dbo"
    cfg = _cfg_from_mapping({**secrets, "schema": schema})
    if cfg is None:
        raise RuntimeError("MSSQL config missing; check MSSQL_* env vars or secrets.")
    return cfg


def _resolve_cfg(cfg: Optional[MssqlConfig]) -> MssqlConfig:
    if cfg is not None:
        return cfg
    active_key = None
    ctx = None
    # Read selected portfolio key from session (best-effort).
    try:
        active_key = str(_st.session_state.get("active_portfolio_key") or "").strip()  # type: ignore[attr-defined]
    except Exception:
        active_key = None

    # Prefer runtime-selected portfolio config first. This prevents stale ctx reads
    # when users switch portfolios and a previous ctx survives a rerun.
    try:
        from core.portfolio_runtime import get_active_portfolio_db_cfg
        cfg_dict, err = get_active_portfolio_db_cfg()
        if cfg_dict:
            return _cfg_from_mapping(
                {
                    "server": cfg_dict.get("server"),
                    "database": cfg_dict.get("database"),
                    "user": cfg_dict.get("user"),
                    "password": cfg_dict.get("password"),
                    "driver": cfg_dict.get("driver"),
                    "encrypt": str(cfg_dict.get("encrypt", True)).lower(),
                    "trust_server_certificate": str(cfg_dict.get("trust_server_certificate", False)).lower(),
                    "schema": cfg_dict.get("schema"),
                }
            )
        if err:
            try:
                _st.session_state["_portfolio_db_error"] = (
                    "Active portfolio configuration is unavailable. "
                    "Please reselect the portfolio."
                )
            except Exception:
                pass
        # Do not fail here; fall back to session portfolio context first.
    except Exception:
        # continue with ctx/default fallback paths below
        pass

    try:
        from core.portfolio_context import get_portfolio_ctx
        ctx = get_portfolio_ctx()
        if ctx:
            ctx_key = str(ctx.portfolio_key or "").strip()
            # Fail closed on mismatch: selected portfolio key should always win.
            if active_key and ctx_key and active_key != ctx_key:
                ctx = None
            else:
                active_key = active_key or ctx_key
        if ctx:
            if ctx.db_server and ctx.db_database and ctx.db_user and ctx.db_password and ctx.db_driver:
                cfg_active = _cfg_from_mapping(
                    {
                        "server": ctx.db_server,
                        "database": ctx.db_database,
                        "user": ctx.db_user,
                        "password": ctx.db_password,
                        "driver": ctx.db_driver,
                        "encrypt": str(ctx.db_encrypt).lower(),
                        "trust_server_certificate": str(ctx.db_trust_server_certificate).lower(),
                        "schema": ctx.db_schema,
                    }
                )
                if cfg_active is not None:
                    return cfg_active
    except Exception:
        ctx = None
    # If a portfolio is selected but no active cfg could be resolved, do not silently
    # fall back to default DB (prevents cross-portfolio leakage).
    if active_key:
        _db_runtime_error(
            "resolve_cfg",
            msg=f"failed_to_resolve_active_portfolio:{active_key}",
            cfg_error=True,
        )
        raise RuntimeError("Failed to resolve selected portfolio DB configuration.")
    return _default_cfg()


def get_default_db_config_dict() -> Dict[str, Any]:
    cfg = _default_cfg()
    return {
        "server": cfg.server,
        "database": cfg.database,
        "user": cfg.user,
        "password": cfg.password,
        "driver": cfg.driver,
        "encrypt": cfg.encrypt,
        "trust_server_certificate": cfg.trust_server_certificate,
        "schema": cfg.schema,
    }


def _db_and_schema(cfg: Optional[MssqlConfig] = None) -> Tuple[str, str]:
    resolved = _resolve_cfg(cfg)
    return resolved.database, (resolved.schema or "dbo")


def _fq(name: str, cfg: Optional[MssqlConfig] = None) -> str:
    """Fully-qualified name in the configured DB/Schema."""
    db, sch = _db_and_schema(cfg)
    return f"{db}.{sch}.{name}"

def _svq(name: str, cfg: Optional[MssqlConfig] = None) -> str:
    """Schema-qualified (no database) name — required for CREATE/ALTER VIEW headers."""
    _, sch = _db_and_schema(cfg)
    return f"{sch}.{name}"

def ensure_app_state_table() -> None:
    execute(f"""
        IF OBJECT_ID('{_svq('TCO_APP_STATE')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('TCO_APP_STATE')} (
            [KEY] NVARCHAR(255) PRIMARY KEY,
            [VALUE] NVARCHAR(255) NULL,
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME()
          );
        END
    """)
    try:
        execute(
            f"""
            IF NOT EXISTS (SELECT 1 FROM {_fq('TCO_APP_STATE')} WHERE [KEY] = 'DATA_VERSION')
            BEGIN
              INSERT INTO {_fq('TCO_APP_STATE')} ([KEY], [VALUE], UPDATED_AT)
              VALUES ('DATA_VERSION', '1', SYSDATETIME());
            END
            """
        )
    except Exception:
        pass

def get_data_version() -> int:
    auto_ensure = str(os.getenv("TCO_APP_STATE_AUTO_ENSURE", "0") or "0").strip().lower() in {"1", "true", "yes"}
    if auto_ensure:
        try:
            ensure_app_state_table()
        except Exception:
            pass
    try:
        df = fetch_df(f"SELECT [VALUE] FROM {_fq('TCO_APP_STATE')} WHERE [KEY] = 'DATA_VERSION'")
        if df is None or df.empty:
            return 0
        val = str(df.iloc[0]["VALUE"] or "").strip()
        return int(val) if val.isdigit() else 0
    except Exception:
        return 0

def get_data_version_info() -> Dict[str, Any]:
    auto_ensure = str(os.getenv("TCO_APP_STATE_AUTO_ENSURE", "0") or "0").strip().lower() in {"1", "true", "yes"}
    if auto_ensure:
        try:
            ensure_app_state_table()
        except Exception:
            pass
    try:
        df = fetch_df(
            f"SELECT [VALUE], UPDATED_AT FROM {_fq('TCO_APP_STATE')} WHERE [KEY] = 'DATA_VERSION'"
        )
        if df is None or df.empty:
            return {"version": 0, "updated_at": None}
        val = str(df.iloc[0]["VALUE"] or "").strip()
        version = int(val) if val.isdigit() else 0
        return {"version": version, "updated_at": df.iloc[0].get("UPDATED_AT")}
    except Exception:
        return {"version": 0, "updated_at": None}

def bump_data_version(reason: str = "") -> None:
    try:
        ensure_app_state_table()
    except Exception:
        return
    try:
        current = get_data_version()
        next_val = max(0, int(current)) + 1
        execute(
            f"""
            MERGE INTO {_fq('TCO_APP_STATE')} t
            USING (SELECT 'DATA_VERSION' AS [KEY], %s AS [VALUE]) s
            ON t.[KEY] = s.[KEY]
            WHEN MATCHED THEN UPDATE SET [VALUE] = s.[VALUE], UPDATED_AT = SYSDATETIME()
            WHEN NOT MATCHED THEN INSERT ([KEY], [VALUE], UPDATED_AT) VALUES (s.[KEY], s.[VALUE], SYSDATETIME());
            """,
            (str(next_val),),
        )
    except Exception:
        pass


def _freshness_v2_enabled() -> bool:
    try:
        secrets_obj = getattr(_st, "secrets", None)
        if secrets_obj and ("flags" in secrets_obj):  # type: ignore[operator]
            flags = secrets_obj["flags"]  # type: ignore[index]
            raw = None
            try:
                raw = flags.get("TCO_FRESHNESS_V2")  # type: ignore[attr-defined]
            except Exception:
                with suppress(Exception):
                    raw = flags["TCO_FRESHNESS_V2"]  # type: ignore[index]
            if raw is not None:
                return str(raw).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        pass
    return bool(TCO_FRESHNESS_V2)


def _tracker_get(key: str, default: int = 0) -> int:
    try:
        return int(getattr(_WRITE_TRACKER, key, default) or default)
    except Exception:
        return int(default)


def _tracker_set(key: str, value: int) -> None:
    try:
        setattr(_WRITE_TRACKER, key, int(value))
    except Exception:
        pass


def _track_write_mutation(rowcount: Optional[int], *, mutating: bool) -> None:
    if not mutating:
        return
    # SQL Server may report -1 for statements where exact rows are unknown.
    try:
        rc = int(rowcount) if rowcount is not None else -1
    except Exception:
        rc = -1
    if rc == 0:
        return
    cur = _tracker_get("mutation_count", 0)
    _tracker_set("mutation_count", cur + max(1, rc if rc > 0 else 1))


def _normalize_write_context(context: str) -> str:
    raw = str(context or "").strip()
    if not raw:
        return "DB.WRITE"
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", raw).strip("_").upper()
    if not cleaned:
        return "DB.WRITE"
    return cleaned


def _execute_write_with_version_bump(context: str, write_fn: Callable[[], Any]) -> Any:
    if not _freshness_v2_enabled():
        return write_fn()
    if hasattr(_st, "session_state") and not _active_portfolio_selected():
        # In app sessions, avoid bumping global freshness when no portfolio is selected.
        return write_fn()
    depth = _tracker_get("scope_depth", 0)
    baseline = _tracker_get("mutation_count", 0)
    _tracker_set("scope_depth", depth + 1)
    try:
        result = write_fn()
    finally:
        _tracker_set("scope_depth", max(0, _tracker_get("scope_depth", 0) - 1))
    if _tracker_get("scope_depth", 0) != 0:
        return result
    after = _tracker_get("mutation_count", 0)
    changed = (after - baseline) > 0
    if changed:
        bump_data_version(_normalize_write_context(context))
    return result


def _versioned_write(context: str):
    """Decorator for mutating DB API functions."""

    def deco(fn: Callable[..., Any]):
        @wraps(fn)
        def wrapped(*args: Any, **kwargs: Any):
            return _execute_write_with_version_bump(context, lambda: fn(*args, **kwargs))

        return wrapped

    return deco


def _create_or_alter_view(name: str, body_sql: str) -> None:
    """Create or alter a view using separate batches.

    SQL Server requires CREATE VIEW to be the first statement in a batch. We
    set required options in one batch and then issue the CREATE/ALTER as the
    first statement in the next batch.
    """
    # Ensure recommended SET options for persisted metadata
    try:
        execute("SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
    except Exception:
        # Non-fatal if SET fails on some drivers/contexts
        pass
    execute(f"CREATE OR ALTER VIEW {_svq(name)} AS\n{body_sql}")

def _new_connection_from_cfg(cfg: MssqlConfig):
    try:
        import pyodbc  # lazy import
    except ImportError as e:  # provide a clearer error with installation hints
        raise RuntimeError(
            "pyodbc is not installed. Activate your venv and run 'pip install pyodbc'. "
            "On macOS, also install unixODBC and Microsoft ODBC Driver 18:\n"
            "  brew install unixodbc\n"
            "  brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release\n"
            "  brew update && brew install --no-sandbox msodbcsql18 mssql-tools18\n"
            "Ensure the driver name matches your secrets.toml (e.g. 'ODBC Driver 18 for SQL Server')."
        ) from e

    # Resolve ODBC driver: honor configured value if installed; otherwise pick a sensible default.
    def _pick_odbc_driver(preferred: Optional[str]) -> str:
        try:
            drivers = pyodbc.drivers()  # type: ignore[attr-defined]
        except Exception:
            drivers = []
        if preferred and preferred in drivers:
            return preferred
        # Try common SQL Server drivers in order of preference
        for name in (
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server",
        ):
            if name in drivers:
                return name
        # Fallback: try any driver that mentions SQL Server
        for d in drivers:
            if "SQL Server" in d:
                return d
        raise RuntimeError(
            "No suitable ODBC driver for SQL Server found. Install msodbcsql18 or set a valid driver name. "
            f"Available drivers: {drivers}"
        )
    driver_name = _pick_odbc_driver(cfg.driver)
    timeout_raw = str(os.getenv("MSSQL_CONNECT_TIMEOUT", "8") or "").strip()
    timeout_val = None
    try:
        timeout_val = int(timeout_raw) if timeout_raw else None
    except Exception:
        timeout_val = None
    conn_str = (
        f"DRIVER={{{driver_name}}};"
        f"SERVER={cfg.server};"
        f"DATABASE={cfg.database};"
        f"UID={cfg.user};PWD={cfg.password};"
        f"Encrypt={'yes' if str(cfg.encrypt).lower() in {'1','true','yes'} else 'no'};"
        f"TrustServerCertificate={'yes' if str(cfg.trust_server_certificate).lower() in {'1','true','yes'} else 'no'};"
        f"MARS_Connection=Yes;"
    )
    if timeout_val is not None:
        conn_str += f"Connection Timeout={timeout_val};"
    # Prevent cross-session stale transaction/cursor state when the app is
    # running with many concurrent users in Azure App Service.
    with suppress(Exception):
        pyodbc.pooling = False  # type: ignore[attr-defined]
    cn = pyodbc.connect(conn_str, autocommit=True)
    query_timeout_raw = str(os.getenv("MSSQL_QUERY_TIMEOUT", "30") or "").strip()
    try:
        query_timeout_val = int(query_timeout_raw) if query_timeout_raw else 30
    except Exception:
        query_timeout_val = 30
    if query_timeout_val > 0:
        with suppress(Exception):
            # Prevent long-running statements from hanging page loads indefinitely.
            cn.timeout = query_timeout_val  # type: ignore[attr-defined]
    try:
        # Faster executemany
        cn.fast_executemany = True  # type: ignore[attr-defined]
    except Exception:
        pass
    return cn


def _new_connection():
    cfg = _default_cfg()
    return _new_connection_from_cfg(cfg)


def _normalize_work_id(work_id: str) -> str:
    """Normalize Work ID by stripping whitespace, non-alphanumerics, and leading zeros."""
    s = str(work_id or "").strip()
    s = re.sub(r"[^0-9A-Za-z]", "", s)
    s = s.lstrip("0")
    return s if s else "0"


def _get_connection():
    # Always return a fresh connection handle (no shared cross-session cursor state).
    return _new_connection()


def _get_connection_cached(
    server: str,
    database: str,
    user: str,
    password: str,
    driver: str,
    encrypt: str,
    trust_server_certificate: str,
) -> Any:
    cfg = MssqlConfig(
        server=server,
        database=database,
        user=user,
        password=password,
        driver=driver,
        encrypt=encrypt,
        trust_server_certificate=trust_server_certificate,
        schema="dbo",
    )
    return _new_connection_from_cfg(cfg)


def _get_connection_for_cfg(cfg: MssqlConfig):
    return _get_connection_cached(
        cfg.server,
        cfg.database,
        cfg.user,
        cfg.password,
        cfg.driver,
        cfg.encrypt,
        cfg.trust_server_certificate,
    )


def _reset_connection_cache() -> None:
    try:
        clear_fn = getattr(_get_connection, "clear", None)
        if callable(clear_fn):
            clear_fn()
    except Exception:
        pass
    try:
        clear_fn = getattr(_get_connection_cached, "clear", None)
        if callable(clear_fn):
            clear_fn()
    except Exception:
        pass


def _is_connection_error(msg: str) -> bool:
    m = msg.lower()
    return any(
        s in m
        for s in (
            "08001",
            "sqlstate 08001",
            "08s01",
            "sqldriverconnect",
            "handshakes before login",
            "server too busy to accept new connections",
            "communication link failure",
            "connection is closed",
            "closed connection",
            "invalid connection",
            "connection is busy with results for another command",
            # Intermittent ODBC cursor/statement state issues in App Service.
            "07005",
            "hy007",
            "hy010",
            "not a cursor-specification",
            "associated statement is not prepared",
            "function sequence error",
            "sqlfetch",
            "sqldescribecol",
            "sqlcolattribute",
            "3971",
            "failed to resume the transaction",
            "server failed to resume the transaction",
        )
    )


def _is_transient_timeout_error(msg: str) -> bool:
    m = str(msg or "").lower()
    return any(
        s in m
        for s in (
            "hyt00",
            "timeout expired",
            "query timeout expired",
            "statement terminated due to timeout",
        )
    )


# =========================================================
# Runtime DB health (process-level diagnostics)
# =========================================================

_db_runtime_logger = logging.getLogger("tco.db")
_db_runtime_lock = Lock()
_db_runtime_recent_errors: deque[Dict[str, Any]] = deque(maxlen=200)
_db_runtime_counters: Dict[str, Any] = {
    "execute_calls": 0,
    "execute_success": 0,
    "execute_errors": 0,
    "fetch_calls": 0,
    "fetch_success": 0,
    "fetch_errors": 0,
    "cfg_errors": 0,
    "conn_errors": 0,
    "retries": 0,
    "deadlock_retries": 0,
    "total_latency_ms": 0.0,
    "latency_samples": 0,
    "last_error_ts": None,
    "last_success_ts": None,
}


def _db_runtime_begin(scope: str) -> None:
    key = f"{scope}_calls"
    with _db_runtime_lock:
        _db_runtime_counters[key] = int(_db_runtime_counters.get(key, 0)) + 1


def _db_runtime_success(scope: str, duration_ms: float) -> None:
    key = f"{scope}_success"
    now = time.time()
    with _db_runtime_lock:
        _db_runtime_counters[key] = int(_db_runtime_counters.get(key, 0)) + 1
        _db_runtime_counters["total_latency_ms"] = float(_db_runtime_counters.get("total_latency_ms", 0.0)) + float(duration_ms)
        _db_runtime_counters["latency_samples"] = int(_db_runtime_counters.get("latency_samples", 0)) + 1
        _db_runtime_counters["last_success_ts"] = now


def _db_runtime_retry(scope: str, *, conn_error: bool, deadlock: bool, msg: str, sql_snippet: str) -> None:
    now = time.time()
    with _db_runtime_lock:
        _db_runtime_counters["retries"] = int(_db_runtime_counters.get("retries", 0)) + 1
        if deadlock:
            _db_runtime_counters["deadlock_retries"] = int(_db_runtime_counters.get("deadlock_retries", 0)) + 1
        if conn_error:
            _db_runtime_counters["conn_errors"] = int(_db_runtime_counters.get("conn_errors", 0)) + 1
        _db_runtime_recent_errors.append(
            {
                "ts": now,
                "scope": scope,
                "type": "retry",
                "conn_error": bool(conn_error),
                "deadlock": bool(deadlock),
                "message": str(msg or "")[:500],
                "sql": str(sql_snippet or "")[:160],
            }
        )


def _db_runtime_error(
    scope: str,
    *,
    msg: str,
    sql_snippet: str = "",
    conn_error: bool = False,
    cfg_error: bool = False,
) -> None:
    key = f"{scope}_errors"
    now = time.time()
    with _db_runtime_lock:
        _db_runtime_counters[key] = int(_db_runtime_counters.get(key, 0)) + 1
        if conn_error:
            _db_runtime_counters["conn_errors"] = int(_db_runtime_counters.get("conn_errors", 0)) + 1
        if cfg_error:
            _db_runtime_counters["cfg_errors"] = int(_db_runtime_counters.get("cfg_errors", 0)) + 1
        _db_runtime_counters["last_error_ts"] = now
        _db_runtime_recent_errors.append(
            {
                "ts": now,
                "scope": scope,
                "type": "error",
                "conn_error": bool(conn_error),
                "cfg_error": bool(cfg_error),
                "message": str(msg or "")[:500],
                "sql": str(sql_snippet or "")[:160],
            }
        )
    try:
        _db_runtime_logger.warning(
            "db_runtime_error scope=%s conn_error=%s cfg_error=%s sql=%s msg=%s",
            scope,
            bool(conn_error),
            bool(cfg_error),
            str(sql_snippet or "")[:160],
            str(msg or "")[:500],
        )
    except Exception:
        pass


def get_db_runtime_health() -> Dict[str, Any]:
    with _db_runtime_lock:
        out = dict(_db_runtime_counters)
        out["recent_errors"] = list(_db_runtime_recent_errors)
    samples = int(out.get("latency_samples", 0) or 0)
    total_ms = float(out.get("total_latency_ms", 0.0) or 0.0)
    out["avg_latency_ms"] = (total_ms / samples) if samples > 0 else 0.0
    return out


def reset_db_runtime_health() -> None:
    with _db_runtime_lock:
        for k in list(_db_runtime_counters.keys()):
            if k in {"total_latency_ms", "avg_latency_ms"}:
                _db_runtime_counters[k] = 0.0
            elif k in {"last_error_ts", "last_success_ts"}:
                _db_runtime_counters[k] = None
            else:
                _db_runtime_counters[k] = 0
        _db_runtime_recent_errors.clear()


# =========================================================
# Low-level helpers
# =========================================================

def _norm_params_and_sql(
    sql: str,
    params: Optional[Iterable[Any]],
    cfg: Optional[MssqlConfig] = None,
) -> tuple[str, Optional[Sequence[Any]]]:
    """Convert Snowflake-style placeholders to pyodbc qmark style.

    Supports positional `%s` and mapping `%(name)s` styles.
    """
    if params is None:
        # strip any Snowflake-only syntax that SQL Server won't accept in simple cases
        return _rewrite_sql(sql, cfg=cfg), None
    if isinstance(params, (list, tuple)) and len(params) == 0:
        return _rewrite_sql(sql, cfg=cfg), None
    import re

    # Mapping style: replace %(name)s with ? in left-to-right order
    if isinstance(params, dict):
        keys_in_order = re.findall(r"%\(([^)]+)\)s", sql)
        ordered: list[Any] = []
        for k in keys_in_order:
            if k not in params:
                raise KeyError(f"Missing SQL parameter: {k}")
            ordered.append(params[k])
        sql_q = re.sub(r"%\([^)]+\)s", "?", sql)
        return _rewrite_sql(sql_q, cfg=cfg), ordered

    # Positional style: replace %s with ?
    sql_q = sql.replace("%s", "?")
    return _rewrite_sql(sql_q, cfg=cfg), list(params) if not isinstance(params, (list, tuple)) else params  # type: ignore[return-value]


def _rewrite_sql(sql: str, cfg: Optional[MssqlConfig] = None) -> str:
    """Best-effort rewrite of common Snowflake SQL to T-SQL equivalents.

    NOTE: This is conservative and aimed to support simple DDL/DML used by the
    core tables. Complex view logic will likely still need manual translation.
    """
    import re
    s = sql
    # Types
    s = re.sub(r"\bNUMBER\s*\((\d+)\s*,\s*(\d+)\)", r"DECIMAL(\1,\2)", s, flags=re.IGNORECASE)
    s = re.sub(r"\bNUMBER\s*\((\d+)\)", r"DECIMAL(\1,0)", s, flags=re.IGNORECASE)
    s = re.sub(r"\bTIMESTAMP_NTZ\b", "DATETIME2", s, flags=re.IGNORECASE)
    s = re.sub(r"\bTIMESTAMP_TZ\b", "DATETIMEOFFSET", s, flags=re.IGNORECASE)
    s = re.sub(r"\bBOOLEAN\b", "BIT", s, flags=re.IGNORECASE)
    s = re.sub(r"\bBINARY\b", "VARBINARY(MAX)", s, flags=re.IGNORECASE)
    s = re.sub(r"\bSTRING\b", "NVARCHAR(255)", s, flags=re.IGNORECASE)

    # Functions / keywords
    s = re.sub(r"CURRENT_TIMESTAMP\s*\(\s*\)", "SYSDATETIME()", s, flags=re.IGNORECASE)
    s = re.sub(r"CREATE\s+OR\s+REPLACE\s+VIEW", "CREATE OR ALTER VIEW", s, flags=re.IGNORECASE)
    s = re.sub(r"::[A-Z_]+", "", s)  # remove Snowflake casts like ::STRING
    s = re.sub(r"NULLS\s+LAST", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\bTO_VARCHAR\s*\(([^)]+)\)", r"CONVERT(VARCHAR(255), \1)", s, flags=re.IGNORECASE)
    # Snowflake timestamp helpers -> SQL Server TRY_CONVERT equivalents
    s = re.sub(r"\bTO_TIMESTAMP_NTZ\s*\(([^)]+)\)", r"TRY_CONVERT(DATETIME2, \1)", s, flags=re.IGNORECASE)
    s = re.sub(r"\bTO_TIMESTAMP_TZ\s*\(([^)]+)\)", r"TRY_CONVERT(DATETIMEOFFSET, \1)", s, flags=re.IGNORECASE)
    s = re.sub(r"\bTO_TIMESTAMP\s*\(([^)]+)\)", r"TRY_CONVERT(DATETIME2, \1)", s, flags=re.IGNORECASE)
    s = re.sub(r"\bNVL\s*\(", "ISNULL(", s, flags=re.IGNORECASE)
    s = re.sub(r"\bTRUE\b", "1", s, flags=re.IGNORECASE)
    s = re.sub(r"\bFALSE\b", "0", s, flags=re.IGNORECASE)
    s = re.sub(r"\bUUID_STRING\s*\(\s*\)\b", "CONVERT(VARCHAR(36), NEWID())", s, flags=re.IGNORECASE)
    s = re.sub(r"\bCURRENT_USER\s*\(\s*\)\b", "SUSER_SNAME()", s, flags=re.IGNORECASE)
    s = re.sub(r"\bCURRENT_DATABASE\s*\(\s*\)\b", "DB_NAME()", s, flags=re.IGNORECASE)
    s = re.sub(r"\bCURRENT_SCHEMA\s*\(\s*\)\b", "SCHEMA_NAME()", s, flags=re.IGNORECASE)
    s = re.sub(r"\bCURRENT_ROLE\s*\(\s*\)\b", "NULL", s, flags=re.IGNORECASE)
    # TO_DATE('YYYY-MM-DD') -> CAST('YYYY-MM-DD' AS DATE)
    s = re.sub(r"TO_DATE\(\s*'([^']+)'\s*\)", r"CAST('\1' AS DATE)", s, flags=re.IGNORECASE)

    # LIMIT/FETCH to TOP 1 (general heuristic: add TOP 1 to first SELECT and strip LIMIT/FETCH)
    if re.search(r"\bLIMIT\s+1\b", s, flags=re.IGNORECASE) or re.search(r"FETCH\s+FIRST\s+1\s+ROWS\s+ONLY", s, flags=re.IGNORECASE):
        s = re.sub(r"\bSELECT\s+", "SELECT TOP 1 ", s, count=1, flags=re.IGNORECASE)
        s = re.sub(r"\bLIMIT\s+1\b", "", s, flags=re.IGNORECASE)
        s = re.sub(r"FETCH\s+FIRST\s+1\s+ROWS\s+ONLY", "", s, flags=re.IGNORECASE)

    # TO_DECIMAL(x, p, s) -> CAST(x AS DECIMAL(p,s))
    s = re.sub(r"TO_DECIMAL\(([^,]+),\s*(\d+)\s*,\s*(\d+)\)", r"CAST(\1 AS DECIMAL(\2,\3))", s, flags=re.IGNORECASE)
    # TRY_TO_NUMBER(expr) -> TRY_CONVERT(NUMERIC(38,10), expr)
    s = re.sub(r"TRY_TO_NUMBER\(([^\)]+)\)", r"TRY_CONVERT(NUMERIC(38,10), \1)", s, flags=re.IGNORECASE)

    # CREATE TABLE IF NOT EXISTS <tbl> (...)
    def _qualify_name(name: str) -> Tuple[str, str, str]:
        db, sch = _db_and_schema(cfg)
        parts = [p.strip('"') for p in name.strip().split('.')]
        if len(parts) == 1:
            return db, sch, parts[0]
        if len(parts) == 2:
            return db, parts[0], parts[1]
        return parts[0], parts[1], parts[2]

    def repl_create_if_not_exists(m: re.Match) -> str:
        tbl = m.group(1).strip()
        body = m.group(2)
        db, sch, table = _qualify_name(tbl)
        fq = f"{db}.{sch}.{table}"
        return (
            f"IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '{table}' AND schema_id = SCHEMA_ID('{sch}'))\n"
            f"BEGIN\nCREATE TABLE {fq} ({body})\nEND"
        )

    s = re.sub(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([\w\.\"]+)\s*\((.*)\)\s*;?",
               repl_create_if_not_exists, s, flags=re.IGNORECASE | re.DOTALL)

    # ALTER TABLE <t> ADD COLUMN IF NOT EXISTS <col> <type>
    def repl_add_column_if_not_exists(m: re.Match) -> str:
        tbl = m.group(1).strip()
        col = m.group(2).strip()
        typ = m.group(3).strip()
        db, sch, table = _qualify_name(tbl)
        fq = f"{db}.{sch}.{table}"
        return (
            f"IF COL_LENGTH('{fq}','{col}') IS NULL ALTER TABLE {fq} ADD {col} {typ}"
        )

    s = re.sub(r"ALTER\s+TABLE\s+([\w\.\"]+)\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+([\w\"]+)\s+([\w\(\), ]+)",
               repl_add_column_if_not_exists, s, flags=re.IGNORECASE)

    return s


def execute(
    sql: str,
    params: Optional[Iterable[Any]] = None,
    many: bool = False,
    cfg: Optional[MssqlConfig] = None,
) -> None:
    _db_runtime_begin("execute")
    if cfg is None:
        try:
            from core.portfolio_context import get_portfolio_ctx
            if get_portfolio_ctx() is not None:
                pass
        except Exception:
            pass
    resolved_cfg = _resolve_cfg(cfg)
    sql_q, prm = _norm_params_and_sql(sql, params, cfg=resolved_cfg)

    # Admin scope-preview mode is read-only by design.
    if _is_mutating_sql(sql_q) and _is_admin_scope_preview_readonly():
        if _is_preview_safe_schema_ensure_sql(sql_q):
            return
        # Block writes safely during scope preview without crashing page load.
        try:
            _st.session_state["_preview_write_blocked"] = True  # type: ignore[attr-defined]
            _st.session_state["_preview_write_blocked_sql"] = (sql_q.strip().splitlines()[0][:160] if sql_q else "")  # type: ignore[attr-defined]
        except Exception:
            pass
        return

    is_mutating_stmt = _is_mutating_sql(sql_q) and not _is_data_version_state_sql(sql_q)
    is_portfolio_dml_stmt = _is_portfolio_dml_sql(sql_q)
    allow_execute_safety_bump = bool(
        is_portfolio_dml_stmt
        and not _is_control_db_cfg(resolved_cfg)
        and (_active_portfolio_selected() or not hasattr(_st, "session_state"))
    )

    def _run_once() -> Optional[int]:
        start = time.perf_counter()
        cn = _get_connection_for_cfg(resolved_cfg) if resolved_cfg else _get_connection()
        rowcount: Optional[int] = None
        try:
            with cn.cursor() as cur:
                if many and prm is not None:
                    cur.executemany(sql_q, prm)  # type: ignore[arg-type]
                else:
                    if prm is None:
                        cur.execute(sql_q)
                    else:
                        cur.execute(sql_q, prm)  # type: ignore[arg-type]
                try:
                    rowcount = cur.rowcount
                except Exception:
                    pass
            with suppress(Exception):
                cn.commit()
        except Exception:
            with suppress(Exception):
                cn.rollback()
            raise
        finally:
            with suppress(Exception):
                cn.close()
        duration_ms = (time.perf_counter() - start) * 1000.0
        _record_sql_trace(duration_ms, rowcount, sql_q)
        _db_runtime_success("execute", duration_ms)
        _track_write_mutation(rowcount, mutating=is_mutating_stmt)
        return rowcount
    snippet = sql_q.strip().splitlines()[0][:160] if sql_q else ""

    def _run_with_retries() -> None:
        for attempt in range(3):
            try:
                _run_once()
                return
            except Exception as e:
                msg = str(e)
                is_deadlock = "deadlock victim" in msg.lower() or "1205" in msg or "40001" in msg
                is_comm = _is_connection_error(msg)
                if (is_deadlock or is_comm) and attempt < 2:
                    _db_runtime_retry("execute", conn_error=is_comm, deadlock=is_deadlock, msg=msg, sql_snippet=snippet)
                    if is_deadlock:
                        time.sleep(0.5 * (2 ** attempt))
                    if is_comm:
                        _reset_connection_cache()
                    continue
                try:
                    marker_count = sql_q.count("?") if sql_q else 0
                    param_len = len(prm) if prm is not None else 0
                    print(
                        f"SQL EXEC ERROR: markers={marker_count} params={param_len} "
                        f"snippet={snippet[:500]}"
                    )
                except Exception:
                    pass
                _db_runtime_error("execute", msg=msg, sql_snippet=snippet, conn_error=is_comm)
                raise RuntimeError(f"Execute failed. First line: {snippet}. Error: {e}") from e

    if allow_execute_safety_bump and _freshness_v2_enabled():
        _execute_write_with_version_bump("DB.EXECUTE_DML", _run_with_retries)
        return
    _run_with_retries()


def _is_mutating_sql(sql: str) -> bool:
    s = str(sql or "")
    if not s.strip():
        return False
    # Best-effort SQL classifier for write prevention in admin preview mode.
    s = re.sub(r"/\*.*?\*/", " ", s, flags=re.S)
    s = re.sub(r"--.*?$", " ", s, flags=re.M)
    up = s.upper()
    return bool(
        re.search(
            r"\b(INSERT|UPDATE|DELETE|MERGE|ALTER|CREATE|DROP|TRUNCATE|GRANT|REVOKE|DENY|EXEC|EXECUTE)\b",
            up,
        )
    )


def _is_data_version_state_sql(sql: str) -> bool:
    up = str(sql or "").upper()
    return "TCO_APP_STATE" in up and "DATA_VERSION" in up


def _is_derived_snapshot_maintenance_sql(sql: str) -> bool:
    up = str(sql or "").upper()
    # Derived snapshot tables are performance artifacts; maintaining them should
    # not advance portfolio DATA_VERSION or they will invalidate themselves.
    return "TCO_TEAM_VELOCITY_SNAPSHOT" in up


def _sql_contains_schema_ddl(sql: str) -> bool:
    up = str(sql or "").upper()
    return bool(re.search(r"\b(CREATE|ALTER|DROP)\b", up))


def _is_portfolio_dml_sql(sql: str) -> bool:
    """Best-effort classifier for data mutations that should advance DATA_VERSION."""
    up = str(sql or "").upper()
    if not up.strip():
        return False
    if _is_data_version_state_sql(up):
        return False
    if _is_derived_snapshot_maintenance_sql(up):
        return False
    if _sql_contains_schema_ddl(up):
        # Skip schema/bootstrap statements to avoid false freshness churn.
        return False
    return bool(re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE)\b", up))


def _active_portfolio_selected() -> bool:
    try:
        key = str(getattr(_st, "session_state", {}).get("active_portfolio_key") or "").strip()  # type: ignore[arg-type]
        return bool(key)
    except Exception:
        return False


def _is_control_db_cfg(cfg: Optional[MssqlConfig]) -> bool:
    if cfg is None:
        return False
    try:
        # Prefer explicit env settings when present.
        c_server = str(os.getenv("CONTROL_MSSQL_SERVER", "") or "").strip().upper()
        c_db = str(os.getenv("CONTROL_MSSQL_DATABASE", "") or "").strip().upper()
        c_user = str(os.getenv("CONTROL_MSSQL_USER", "") or "").strip().upper()
        if not c_server and not c_db and not c_user:
            # Fallback to Streamlit control_db secrets if available.
            try:
                c = getattr(_st, "secrets", {}).get("control_db", {})  # type: ignore[attr-defined]
                c_server = str(c.get("server") or "").strip().upper()
                c_db = str(c.get("database") or "").strip().upper()
                c_user = str(c.get("user") or "").strip().upper()
            except Exception:
                pass

        server = str(cfg.server or "").strip().upper()
        db = str(cfg.database or "").strip().upper()
        user = str(cfg.user or "").strip().upper()
        if c_db and db != c_db:
            return False
        if c_server and server != c_server:
            return False
        if c_user and user != c_user:
            return False
        return bool(c_db or c_server or c_user)
    except Exception:
        return False


def _is_admin_scope_preview_readonly() -> bool:
    try:
        from utils.auth import is_readonly_preview_active

        return bool(is_readonly_preview_active())
    except Exception:
        return False


def _is_preview_safe_schema_ensure_sql(sql: str) -> bool:
    """
    Allow idempotent schema/bootstrap guards during scope preview mode.
    These are page-load housekeeping statements and should not crash the UI.
    """
    s = str(sql or "")
    if not s.strip():
        return False
    up = s.upper()
    has_guard = ("OBJECT_ID(" in up) or ("COL_LENGTH(" in up) or ("INFORMATION_SCHEMA." in up)
    has_ddl = any(tok in up for tok in ("CREATE TABLE", "ALTER TABLE", "CREATE INDEX", "ADD CONSTRAINT"))
    has_if = "IF " in up
    # Never allow explicit data DML through this path.
    has_dml = bool(re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE)\b", up))
    return bool(has_if and has_guard and has_ddl and not has_dml)


def fetch_df(
    sql: str,
    params: Optional[Iterable[Any]] = None,
    cfg: Optional[MssqlConfig] = None,
) -> pd.DataFrame:
    _db_runtime_begin("fetch")
    if cfg is None:
        try:
            from core.portfolio_context import get_portfolio_ctx
            if get_portfolio_ctx() is not None:
                pass
        except Exception:
            pass
    resolved_cfg = _resolve_cfg(cfg)
    sql_q, prm = _norm_params_and_sql(sql, params, cfg=resolved_cfg)
    def _run_once() -> pd.DataFrame:
        start = time.perf_counter()
        cn = _get_connection_for_cfg(resolved_cfg) if resolved_cfg else _get_connection()
        try:
            with cn.cursor() as cur:
                if prm is None:
                    cur.execute(sql_q)
                else:
                    cur.execute(sql_q, prm)  # type: ignore[arg-type]
                # Capture column names even when there are zero rows; advance to the first result set if needed.
                def _fetch_with_cols() -> tuple[list[Any], list[str]]:
                    cols = [c[0] for c in cur.description] if cur.description else []
                    if cols:
                        return cur.fetchall(), cols
                    while True:
                        if not cur.nextset():
                            return [], []
                        cols = [c[0] for c in cur.description] if cur.description else []
                        if cols:
                            return cur.fetchall(), cols
                    return [], []

                try:
                    rows, cols = _fetch_with_cols()
                except Exception as e:
                    # Some drivers raise "Invalid cursor state" on the first fetch; try advancing once.
                    if "24000" in str(e) or "invalid cursor state" in str(e).lower():
                        if cur.nextset():
                            rows, cols = _fetch_with_cols()
                        else:
                            rows, cols = [], []
                    else:
                        raise
        finally:
            with suppress(Exception):
                cn.close()
        duration_ms = (time.perf_counter() - start) * 1000.0
        _record_sql_trace(duration_ms, len(rows), sql_q)
        _db_runtime_success("fetch", duration_ms)
        return pd.DataFrame.from_records(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    try:
        return _run_once()
    except Exception as e:
        msg = str(e)
        snippet = sql_q.strip().splitlines()[0][:160] if sql_q else ""
        is_conn_error = _is_connection_error(msg)
        is_timeout_error = _is_transient_timeout_error(msg)
        retry_timeout = str(os.getenv("TCO_DB_RETRY_TIMEOUT", "0") or "0").strip().lower() in {"1", "true", "yes"}
        should_retry = is_conn_error or (is_timeout_error and retry_timeout)
        if should_retry:
            _db_runtime_retry("fetch", conn_error=is_conn_error, deadlock=False, msg=msg, sql_snippet=snippet)
            if is_conn_error:
                _reset_connection_cache()
                # Small backoff helps local SQL Server recover under transient login/handshake pressure.
                try:
                    sleep_ms = int(str(os.getenv("MSSQL_CONN_RETRY_SLEEP_MS", "250") or "250").strip())
                except Exception:
                    sleep_ms = 250
                if sleep_ms > 0:
                    time.sleep(min(max(sleep_ms, 0), 5000) / 1000.0)
            try:
                return _run_once()
            except Exception as e2:
                _db_runtime_error("fetch", msg=str(e2), sql_snippet=snippet, conn_error=_is_connection_error(str(e2)))
                raise RuntimeError(f"Query retry failed. First line: {snippet}. Error: {e2}") from e2
        _db_runtime_error("fetch", msg=msg, sql_snippet=snippet, conn_error=False)
        raise RuntimeError(f"Query failed. First line: {snippet}. Error: {e}") from e


# =========================================================
# Minimal schema bootstrap (core tables)
# =========================================================

def ensure_tables() -> None:
    # Runtime guardrail:
    # In restored/local environments with large datasets, schema-ensure DDL can block
    # page loads (timeouts on ALTER/OBJECT_ID checks). Allow opt-out for UI runtime.
    skip_runtime_ensure = str(os.getenv("TCO_SKIP_RUNTIME_SCHEMA_ENSURE", "0") or "0").strip().lower() in {"1", "true", "yes"}
    if skip_runtime_ensure:
        return
    ensure_app_state_table()
    # PROGRAMS
    execute(f"""
        IF OBJECT_ID('{_svq('PROGRAMS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('PROGRAMS')} (
            PROGRAMID        NVARCHAR(255) PRIMARY KEY,
            PROGRAMNAME      NVARCHAR(255),
            PROGRAM_DISPLAY_NAME NVARCHAR(255) NULL,
            PROGRAMOWNER     NVARCHAR(255),
            PROGRAMFTE       FLOAT,
            PROGRAM_XOM_RATE FLOAT,
            CREATED_AT       DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_AT       DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY       NVARCHAR(255) NULL
          );
        END
    """)
    # Unique constraint on PROGRAMNAME
    execute(f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'UQ_PROGRAMS_PROGRAMNAME' AND object_id = OBJECT_ID('{_fq('PROGRAMS')}'))
        BEGIN
          CREATE UNIQUE INDEX UQ_PROGRAMS_PROGRAMNAME ON {_fq('PROGRAMS')}(PROGRAMNAME);
        END
    """)
    # ADMIN_AUDIT_LOG (lightweight admin action tracking)
    execute(f"""
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
    """)
    execute(f"ALTER TABLE {_fq('PROGRAMS')} ADD COLUMN IF NOT EXISTS PROGRAM_XOM_RATE FLOAT")
    execute(f"ALTER TABLE {_fq('PROGRAMS')} ADD COLUMN IF NOT EXISTS PROGRAM_DISPLAY_NAME NVARCHAR(255)")
    execute(f"ALTER TABLE {_fq('PROGRAMS')} ADD COLUMN IF NOT EXISTS CREATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('PROGRAMS')} ADD COLUMN IF NOT EXISTS UPDATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('PROGRAMS')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")

    # Program additional costs (per month)
    execute(f"""
        IF OBJECT_ID('{_svq('PROGRAM_ADDITIONAL_COSTS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('PROGRAM_ADDITIONAL_COSTS')} (
            PROGRAMID NVARCHAR(255) NOT NULL,
            YEAR INT NOT NULL,
            MONTH INT NOT NULL,
            COST_TYPE NVARCHAR(50) NOT NULL,
            SUBTYPE NVARCHAR(50) NOT NULL DEFAULT '',
            DESCRIPTION NVARCHAR(255) NULL,
            CURRENCY NVARCHAR(10) NULL,
            IS_RECURRING BIT NOT NULL DEFAULT 1,
            AMOUNT DECIMAL(18,2) DEFAULT 0,
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL,
            CONSTRAINT PK_PROGRAM_ADDITIONAL_COSTS PRIMARY KEY (PROGRAMID, YEAR, MONTH, COST_TYPE, SUBTYPE)
          );
        END
    """)
    # Safeguard existing deployments: normalize nulls and enforce NOT NULL
    try:
        execute(f"UPDATE {_fq('PROGRAM_ADDITIONAL_COSTS')} SET SUBTYPE='' WHERE SUBTYPE IS NULL")
        execute(f"ALTER TABLE {_fq('PROGRAM_ADDITIONAL_COSTS')} ALTER COLUMN SUBTYPE NVARCHAR(50) NOT NULL")
        execute(f"ALTER TABLE {_fq('PROGRAM_ADDITIONAL_COSTS')} ALTER COLUMN PROGRAMID NVARCHAR(255) NOT NULL")
        execute(f"ALTER TABLE {_fq('PROGRAM_ADDITIONAL_COSTS')} ALTER COLUMN YEAR INT NOT NULL")
        execute(f"ALTER TABLE {_fq('PROGRAM_ADDITIONAL_COSTS')} ALTER COLUMN MONTH INT NOT NULL")
        execute(f"ALTER TABLE {_fq('PROGRAM_ADDITIONAL_COSTS')} ALTER COLUMN COST_TYPE NVARCHAR(50) NOT NULL")
    except Exception:
        pass
    # Backfill columns for older DBs
    try:
        execute(f"ALTER TABLE {_fq('PROGRAM_ADDITIONAL_COSTS')} ADD COLUMN IF NOT EXISTS DESCRIPTION NVARCHAR(255)")
        execute(f"ALTER TABLE {_fq('PROGRAM_ADDITIONAL_COSTS')} ADD COLUMN IF NOT EXISTS CURRENCY NVARCHAR(10)")
        execute(f"ALTER TABLE {_fq('PROGRAM_ADDITIONAL_COSTS')} ADD COLUMN IF NOT EXISTS IS_RECURRING BIT")
        execute(f"UPDATE {_fq('PROGRAM_ADDITIONAL_COSTS')} SET IS_RECURRING = 1 WHERE IS_RECURRING IS NULL")
    except Exception:
        pass
    try:
        execute(f"UPDATE {_fq('PROGRAM_ADDITIONAL_COSTS')} SET SUBTYPE='' WHERE SUBTYPE IS NULL")
    except Exception:
        pass

    # TEAMS
    execute(f"""
        IF OBJECT_ID('{_svq('TEAMS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('TEAMS')} (
            TEAMID             NVARCHAR(255) PRIMARY KEY,
            TEAMNAME           NVARCHAR(255),
            TEAM_DISPLAY_NAME  NVARCHAR(255) NULL,
            PROGRAMID          NVARCHAR(255),
            TEAMFTE            DECIMAL(18,2),
            DELIVERY_TEAM_FTE  DECIMAL(18,2),
            CONTRACTOR_C_FTE   DECIMAL(18,2),
            CONTRACTOR_CS_FTE  DECIMAL(18,2),
            COSTPERFTE         FLOAT,
            PRODUCTOWNER       NVARCHAR(255),
            CREATED_AT         DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_AT         DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY         NVARCHAR(255) NULL
          );
        END
    """)
    execute(f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'UQ_TEAMS_TEAMNAME' AND object_id = OBJECT_ID('{_fq('TEAMS')}'))
        BEGIN
          CREATE UNIQUE INDEX UQ_TEAMS_TEAMNAME ON {_fq('TEAMS')}(TEAMNAME);
        END
    """)
    execute(f"ALTER TABLE {_fq('TEAMS')} ADD COLUMN IF NOT EXISTS CREATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('TEAMS')} ADD COLUMN IF NOT EXISTS UPDATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('TEAMS')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")
    execute(f"ALTER TABLE {_fq('TEAMS')} ADD COLUMN IF NOT EXISTS TEAM_DISPLAY_NAME NVARCHAR(255)")
    # Commitments (PI readiness commitments)
    execute(
        f"""
        IF OBJECT_ID('{_svq('TCO_COMMITMENTS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('TCO_COMMITMENTS')} (
            COMMITMENT_ID INT IDENTITY(1,1) PRIMARY KEY,
            YEAR INT NULL,
            PI NVARCHAR(50) NULL,
            PROGRAMNAME NVARCHAR(255) NULL,
            TEAMNAME NVARCHAR(255) NULL,
            GROUPNAME NVARCHAR(255) NULL,
            STATUS NVARCHAR(50) NOT NULL DEFAULT 'Not started',
            OWNER_EMAIL NVARCHAR(255) NULL,
            OWNER_NAME NVARCHAR(255) NULL,
            TARGET_DATE DATE NULL,
            NOTES NVARCHAR(MAX) NULL,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL
          );
        END
        """
    )
    try:
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1 FROM sys.indexes WHERE name = 'UQ_TCO_COMMITMENTS_SCOPE' AND object_id = OBJECT_ID('{_fq('TCO_COMMITMENTS')}')
            )
            BEGIN
              CREATE UNIQUE INDEX UQ_TCO_COMMITMENTS_SCOPE
              ON {_fq('TCO_COMMITMENTS')}(YEAR, PI, PROGRAMNAME, TEAMNAME, GROUPNAME);
            END
            """
        )
    except Exception:
        pass

    # VENDORS
    execute(f"""
        IF OBJECT_ID('{_svq('VENDORS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('VENDORS')} (
            VENDORID   NVARCHAR(255) PRIMARY KEY,
            VENDORNAME NVARCHAR(255)
          );
        END
    """)
    execute(f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'UQ_VENDORS_VENDORNAME' AND object_id = OBJECT_ID('{_fq('VENDORS')}'))
        BEGIN
          CREATE UNIQUE INDEX UQ_VENDORS_VENDORNAME ON {_fq('VENDORS')}(VENDORNAME);
        END
    """)
    execute(f"ALTER TABLE {_fq('VENDORS')} ADD COLUMN IF NOT EXISTS CREATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('VENDORS')} ADD COLUMN IF NOT EXISTS UPDATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('VENDORS')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")

    # APPLICATION_GROUPS
    execute(f"""
        IF OBJECT_ID('{_svq('APPLICATION_GROUPS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('APPLICATION_GROUPS')} (
            GROUPID NVARCHAR(255) PRIMARY KEY,
            GROUPNAME NVARCHAR(255) UNIQUE,
            DEFAULT_VENDORID NVARCHAR(255),
            OWNER NVARCHAR(255),
            TEAMID NVARCHAR(255),
            PROGRAMID NVARCHAR(255),
            IS_BASE BIT NOT NULL CONSTRAINT DF_APPLICATION_GROUPS_IS_BASE DEFAULT 0,
            CREATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL
          );
        END
    """)
    execute(f"""
        IF COL_LENGTH('{_fq('APPLICATION_GROUPS')}', 'IS_BASE') IS NULL
        BEGIN
          ALTER TABLE {_fq('APPLICATION_GROUPS')} ADD IS_BASE BIT NOT NULL CONSTRAINT DF_APPLICATION_GROUPS_IS_BASE DEFAULT 0;
        END
    """)
    execute(f"ALTER TABLE {_fq('APPLICATION_GROUPS')} ADD COLUMN IF NOT EXISTS UPDATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('APPLICATION_GROUPS')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")
    # Legacy compatibility: previous versions enforced a single BASE app per team.
    # Multi-BASE is now supported, so drop the filtered unique index when present.
    execute(f"""
        IF EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_APPLICATION_GROUPS_BASE_PER_TEAM' AND object_id = OBJECT_ID('{_fq('APPLICATION_GROUPS')}')
        )
        BEGIN
          DROP INDEX IX_APPLICATION_GROUPS_BASE_PER_TEAM ON {_fq('APPLICATION_GROUPS')};
        END
    """)

    # APPLICATIONS
    execute(f"""
        IF OBJECT_ID('{_svq('APPLICATIONS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('APPLICATIONS')} (
            APPLICATIONID   NVARCHAR(255) PRIMARY KEY,
            APPLICATIONNAME NVARCHAR(255),
            VENDORID        NVARCHAR(255),
            GROUPID         NVARCHAR(255),
            ADD_INFO        NVARCHAR(MAX),
            IS_DEFAULT      BIT NOT NULL CONSTRAINT DF_APPLICATIONS_IS_DEFAULT DEFAULT 0,
            CREATED_AT      DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_AT      DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY      NVARCHAR(255) NULL
          );
        END
    """)
    execute(f"""
        IF COL_LENGTH('{_fq('APPLICATIONS')}', 'IS_DEFAULT') IS NULL
        BEGIN
          ALTER TABLE {_fq('APPLICATIONS')} ADD IS_DEFAULT BIT NOT NULL CONSTRAINT DF_APPLICATIONS_IS_DEFAULT DEFAULT 0;
        END
    """)
    execute(f"ALTER TABLE {_fq('APPLICATIONS')} ADD COLUMN IF NOT EXISTS CREATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('APPLICATIONS')} ADD COLUMN IF NOT EXISTS UPDATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('APPLICATIONS')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")
    # Unique on APPLICATIONNAME (approximation of Snowflake UQ)
    execute(f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'UQ_APPLICATIONS_NAME' AND object_id = OBJECT_ID('{_fq('APPLICATIONS')}'))
        BEGIN
          CREATE UNIQUE INDEX UQ_APPLICATIONS_NAME ON {_fq('APPLICATIONS')}(APPLICATIONNAME);
        END
    """)
    execute(f"ALTER TABLE {_fq('APPLICATIONS')} ADD COLUMN IF NOT EXISTS CREATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('APPLICATIONS')} ADD COLUMN IF NOT EXISTS UPDATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('APPLICATIONS')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")
    # Default Application Instance per group (reduces PO burden on invoices/contracts).
    # - Create a default instance only when a group has 0 instances.
    # - Mark the single instance as default when a group has exactly 1 instance.
    try:
        execute(
            f"""
            INSERT INTO {_fq('APPLICATIONS')} (APPLICATIONID, APPLICATIONNAME, GROUPID, IS_DEFAULT, CREATED_AT, UPDATED_AT)
            SELECT
              CONCAT(g.GROUPID, '__DEFAULT') AS APPLICATIONID,
              LEFT(CONCAT(g.GROUPNAME, ' (Default)'), 255) AS APPLICATIONNAME,
              g.GROUPID,
              1 AS IS_DEFAULT,
              SYSDATETIME(),
              SYSDATETIME()
            FROM {_fq('APPLICATION_GROUPS')} g
            LEFT JOIN {_fq('APPLICATIONS')} a ON a.GROUPID = g.GROUPID
            WHERE a.APPLICATIONID IS NULL;
            """
        )
    except Exception:
        pass
    try:
        execute(
            f"""
            WITH one_app AS (
              SELECT GROUPID, MIN(APPLICATIONID) AS APPLICATIONID
              FROM {_fq('APPLICATIONS')}
              GROUP BY GROUPID
              HAVING COUNT(1) = 1
            )
            UPDATE a
            SET IS_DEFAULT = 1
            FROM {_fq('APPLICATIONS')} a
            JOIN one_app o ON o.GROUPID = a.GROUPID AND o.APPLICATIONID = a.APPLICATIONID;
            """
        )
    except Exception:
        pass

    # INVOICES (extended fields)
    execute(f"""
        IF OBJECT_ID('{_svq('INVOICES')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('INVOICES')} (
            INVOICEID     NVARCHAR(255) PRIMARY KEY,
            APPLICATIONID NVARCHAR(255),
            TEAMID        NVARCHAR(255),
            INVOICEDATE   DATE,
            RENEWALDATE   DATE,
            AMOUNT        DECIMAL(18,2),
            STATUS        NVARCHAR(255),
            FISCAL_YEAR   INT,
            PROGRAMID_AT_BOOKING NVARCHAR(255),
            VENDORID_AT_BOOKING  NVARCHAR(255),
            GROUPID       NVARCHAR(255),
            GROUPID_AT_BOOKING   NVARCHAR(255),
            PRODUCT_OWNER NVARCHAR(255),
            AMOUNT_NEXT_YEAR DECIMAL(18,2),
            CONTRACT_ACTIVE BIT,
            COMPANY_CODE  NVARCHAR(255),
            COST_CENTER   NVARCHAR(255),
            SERIAL_NUMBER NVARCHAR(255),
            WORK_ORDER    NVARCHAR(255),
            AGREEMENT_NUMBER NVARCHAR(255),
            CONTRACT_DUE  INT,
            SERVICE_TYPE  NVARCHAR(255),
            NOTES         NVARCHAR(MAX),
            ROLLOVER_BATCH_ID NVARCHAR(255),
            ROLLED_OVER_FROM_YEAR INT,
            INVOICE_TYPE  NVARCHAR(255),
            UPDATED_AT    DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY    NVARCHAR(255) NULL
          );
        END
    """)
    # Default invoice type if null – emulate via UPDATE
    execute(f"UPDATE {_fq('INVOICES')} SET INVOICE_TYPE = 'Recurring Invoice' WHERE INVOICE_TYPE IS NULL")
    # Unique annual type per (app, team, year, type)
    execute(f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'UQ_INVOICE_ANNUAL_TYPE' AND object_id = OBJECT_ID('{_fq('INVOICES')}'))
        BEGIN
          CREATE UNIQUE INDEX UQ_INVOICE_ANNUAL_TYPE ON {_fq('INVOICES')}(APPLICATIONID, TEAMID, FISCAL_YEAR, INVOICE_TYPE);
        END
    """)
    execute(f"ALTER TABLE {_fq('INVOICES')} ADD COLUMN IF NOT EXISTS UPDATED_AT DATETIME2 DEFAULT SYSDATETIME()")
    execute(f"ALTER TABLE {_fq('INVOICES')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")

    # Links between Teams and Application Groups (many-to-many)
    execute(f"""
        IF OBJECT_ID('{_svq('APP_GROUP_TEAM_LINKS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('APP_GROUP_TEAM_LINKS')} (
            GROUPID    NVARCHAR(255) NOT NULL,
            TEAMID     NVARCHAR(255) NOT NULL,
            CREATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            CONSTRAINT PK_APP_GROUP_TEAM_LINKS PRIMARY KEY (GROUPID, TEAMID)
          );
        END
    """)

    # Admin/support tables
    execute(f"""
        IF OBJECT_ID('{_svq('ROLLOVER_LOG')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ROLLOVER_LOG')} (
            BATCH_ID NVARCHAR(255) PRIMARY KEY,
            FROM_YEAR INT,
            TO_YEAR   INT,
            ROWS_INSERTED INT,
            CREATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            CREATED_BY NVARCHAR(255)
          );
        END
    """)

    execute(f"""
        IF OBJECT_ID('{_svq('INVOICE_NOTES')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('INVOICE_NOTES')} (
            NOTE_ID NVARCHAR(255) PRIMARY KEY,
            INVOICEID NVARCHAR(255),
            NOTE_TEXT NVARCHAR(MAX),
            CREATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            CREATED_BY NVARCHAR(255)
          );
        END
    """)

    execute(f"""
        IF OBJECT_ID('{_svq('INVOICE_ATTACHMENTS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('INVOICE_ATTACHMENTS')} (
            ATTACHMENT_ID NVARCHAR(255) PRIMARY KEY,
            INVOICEID NVARCHAR(255),
            FILENAME NVARCHAR(400),
            MIMETYPE NVARCHAR(255),
            CONTENT VARBINARY(MAX),
            UPLOADED_AT DATETIME2 DEFAULT SYSDATETIME()
          );
        END
    """)

    # Apptio work ID mapping + monthly actuals
    execute(f"""
        IF OBJECT_ID('{_svq('PROGRAM_APPTIO_WORKIDS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('PROGRAM_APPTIO_WORKIDS')} (
            WORK_ID NVARCHAR(255) PRIMARY KEY,
            PROGRAMID NVARCHAR(255) NOT NULL,
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL
          );
        END
    """)
    execute(f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_PROGRAM_APPTIO_WORKIDS_PROGRAM' AND object_id = OBJECT_ID('{_fq('PROGRAM_APPTIO_WORKIDS')}'))
        BEGIN
          CREATE INDEX IX_PROGRAM_APPTIO_WORKIDS_PROGRAM ON {_fq('PROGRAM_APPTIO_WORKIDS')}(PROGRAMID);
        END
    """)
    execute(f"""
        IF OBJECT_ID('{_svq('APPTIO_ACTUALS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('APPTIO_ACTUALS')} (
            WORK_ID NVARCHAR(255) NOT NULL,
            FISCAL_YEAR INT NOT NULL,
            MONTH INT NOT NULL,
            AMOUNT DECIMAL(18,2) DEFAULT 0,
            SOURCE NVARCHAR(255),
            LOADED_AT DATETIME2 DEFAULT SYSDATETIME(),
            LOADED_BY NVARCHAR(255) NULL,
            CONSTRAINT PK_APPTIO_ACTUALS PRIMARY KEY (WORK_ID, FISCAL_YEAR, MONTH)
          );
        END
    """)
    execute(f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_APPTIO_ACTUALS_FY' AND object_id = OBJECT_ID('{_fq('APPTIO_ACTUALS')}'))
        BEGIN
          CREATE INDEX IX_APPTIO_ACTUALS_FY ON {_fq('APPTIO_ACTUALS')}(FISCAL_YEAR, MONTH);
        END
    """)

    ensure_performance_indexes()

    # Optional enriched Apptio dimensions + mapping (kept separate from legacy APPTIO_ACTUALS).
    try:
        ensure_apptio_actuals_lines_table()
        ensure_map_apptio_to_cost_type_table()
    except Exception:
        pass


def ensure_apptio_actuals_lines_table() -> None:
    """Ensure `APPTIO_ACTUALS_LINES` exists (raw dimensions for NWF taxonomy mapping)."""
    execute(
        f"""
        IF OBJECT_ID('{_svq('APPTIO_ACTUALS_LINES')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('APPTIO_ACTUALS_LINES')} (
            WORK_ID NVARCHAR(200) NOT NULL,
            FISCAL_YEAR INT NOT NULL,
            MONTH INT NOT NULL,
            AMOUNT FLOAT NOT NULL,
            LEDGER_ACCOUNT_L3_DESC NVARCHAR(4000) NULL,
            PRODUCT_ID NVARCHAR(200) NULL,
            PRODUCT_NAME NVARCHAR(4000) NULL,
            SOURCE NVARCHAR(200) NULL,
            LOADED_AT DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            LOADED_BY NVARCHAR(200) NULL
          );
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_APPTIO_ACTUALS_LINES_FY' AND object_id = OBJECT_ID('{_fq('APPTIO_ACTUALS_LINES')}'))
        BEGIN
          CREATE INDEX IX_APPTIO_ACTUALS_LINES_FY ON {_fq('APPTIO_ACTUALS_LINES')}(FISCAL_YEAR, MONTH);
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_APPTIO_ACTUALS_LINES_WORK' AND object_id = OBJECT_ID('{_fq('APPTIO_ACTUALS_LINES')}'))
        BEGIN
          CREATE INDEX IX_APPTIO_ACTUALS_LINES_WORK ON {_fq('APPTIO_ACTUALS_LINES')}(WORK_ID);
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_APPTIO_ACTUALS_LINES_PRODUCT' AND object_id = OBJECT_ID('{_fq('APPTIO_ACTUALS_LINES')}'))
        BEGIN
          CREATE INDEX IX_APPTIO_ACTUALS_LINES_PRODUCT ON {_fq('APPTIO_ACTUALS_LINES')}(PRODUCT_ID);
        END
        """
    )


def ensure_map_apptio_to_cost_type_table() -> None:
    """Ensure `MAP_APPTIO_TO_COST_TYPE` exists (Apptio→Additional Costs taxonomy mapping)."""
    execute(
        f"""
        IF OBJECT_ID('{_svq('MAP_APPTIO_TO_COST_TYPE')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('MAP_APPTIO_TO_COST_TYPE')} (
            MAP_ID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
            FISCAL_YEAR INT NULL,
            PRODUCT_ID NVARCHAR(200) NULL,
            LEDGER_ACCOUNT_CONTAINS NVARCHAR(4000) NULL,
            COST_TYPE NVARCHAR(200) NOT NULL,
            SUBTYPE NVARCHAR(200) NULL,
            IS_ACTIVE BIT NOT NULL DEFAULT 1,
            NOTES NVARCHAR(4000) NULL,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            UPDATED_BY NVARCHAR(200) NULL
          );
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_MAP_APPTIO_TO_COST_TYPE_FY_PRODUCT' AND object_id = OBJECT_ID('{_fq('MAP_APPTIO_TO_COST_TYPE')}'))
        BEGIN
          CREATE INDEX IX_MAP_APPTIO_TO_COST_TYPE_FY_PRODUCT ON {_fq('MAP_APPTIO_TO_COST_TYPE')}(FISCAL_YEAR, PRODUCT_ID);
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_MAP_APPTIO_TO_COST_TYPE_FY_ACTIVE' AND object_id = OBJECT_ID('{_fq('MAP_APPTIO_TO_COST_TYPE')}'))
        BEGIN
          CREATE INDEX IX_MAP_APPTIO_TO_COST_TYPE_FY_ACTIVE ON {_fq('MAP_APPTIO_TO_COST_TYPE')}(FISCAL_YEAR, IS_ACTIVE);
        END
        """
    )

    # Cost events (audit trail for what changed by PI/iteration)
    try:
        ensure_cost_events_table()
    except Exception:
        pass


def ensure_cost_events_table() -> None:
    """Ensure `TCO_COST_EVENTS` exists (lightweight change-log for Budget 'Changes by PI')."""
    execute(
        f"""
        IF OBJECT_ID('{_svq('TCO_COST_EVENTS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('TCO_COST_EVENTS')} (
            EVENT_ID            BIGINT IDENTITY(1,1) PRIMARY KEY,
            FISCAL_YEAR         INT NOT NULL,
            EVENT_DATE          DATE NULL,
            PI_NAME             NVARCHAR(50) NULL,
            ITERATION_NAME      NVARCHAR(100) NULL,
            PROGRAM             NVARCHAR(255) NULL,
            TEAM                NVARCHAR(255) NULL,
            APP_GROUP           NVARCHAR(255) NULL,
            COST_BUCKET         NVARCHAR(10) NOT NULL,
            EVENT_TYPE          NVARCHAR(50) NOT NULL,
            AMOUNT_DELTA        DECIMAL(18,2) NOT NULL DEFAULT 0,
            HEADCOUNT_DELTA     DECIMAL(10,2) NOT NULL DEFAULT 0,
            SOURCE_SYSTEM       NVARCHAR(20) NOT NULL,
            RELATED_OBJECT_TYPE NVARCHAR(20) NULL,
            RELATED_OBJECT_ID   NVARCHAR(100) NULL,
            NOTES               NVARCHAR(MAX) NULL,
            CREATED_AT          DATETIME2 NOT NULL DEFAULT SYSDATETIME()
          );
        END
        """
    )
    # Backfill / migrate older DBs (best effort).
    try:
        execute(
            f"""
            IF COL_LENGTH('{_fq('TCO_COST_EVENTS')}', 'EVENT_DATE') IS NULL
            BEGIN
              ALTER TABLE {_fq('TCO_COST_EVENTS')} ADD EVENT_DATE DATE NULL;
            END
            """
        )
    except Exception:
        pass
    try:
        execute(f"ALTER TABLE {_fq('TCO_COST_EVENTS')} ALTER COLUMN PI_NAME NVARCHAR(50) NULL")
    except Exception:
        pass
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_TCO_COST_EVENTS_FY_PI' AND object_id = OBJECT_ID('{_fq('TCO_COST_EVENTS')}'))
        BEGIN
          CREATE INDEX IX_TCO_COST_EVENTS_FY_PI ON {_fq('TCO_COST_EVENTS')}(FISCAL_YEAR, PI_NAME);
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'IX_TCO_COST_EVENTS_SCOPE' AND object_id = OBJECT_ID('{_fq('TCO_COST_EVENTS')}'))
        BEGIN
          CREATE INDEX IX_TCO_COST_EVENTS_SCOPE ON {_fq('TCO_COST_EVENTS')}(PROGRAM, TEAM, APP_GROUP);
        END
        """
    )


# =========================================================
# Location- and contractor-aware rates & headcount
# =========================================================

def ensure_location_and_contractor_tables() -> None:
    """Create history tables for location/program rates and headcount splits."""
    db, sch = _db_and_schema()
    # Runtime DDL guard:
    # ALTER COLUMN on large history tables can block app startup in local/dev.
    # Keep strict NOT NULL migration opt-in.
    enforce_pi_not_null = str(os.getenv("TCO_SCHEMA_ENFORCE_PI_NOT_NULL", "0") or "0").strip().lower() in {"1", "true", "yes"}
    def _exec_retry(sql: str):
        try:
            execute(sql)
        except Exception as e:
            if "1205" in str(e):
                # deadlock victim, retry once
                execute(sql)
            else:
                raise
    # XOM rates per team/location/year/PI
    execute(f"""
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'TEAM_RATE_HISTORY' AND schema_id = SCHEMA_ID('{sch}'))
      BEGIN
        CREATE TABLE {_fq('TEAM_RATE_HISTORY')} (
          TEAMID NVARCHAR(255) NOT NULL,
          YEAR   INT NOT NULL,
          PI     INT NOT NULL CONSTRAINT DF_TEAM_RATE_HISTORY_PI DEFAULT 0,
          LOCATION NVARCHAR(50) NOT NULL,
          XOM_RATE DECIMAL(18,2),
          UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
          UPDATED_BY NVARCHAR(255) NULL,
          CONSTRAINT PK_TEAM_RATE_HISTORY PRIMARY KEY (TEAMID, LOCATION, YEAR, PI)
        );
      END
    """)
    try:
        execute(f"UPDATE {_fq('TEAM_RATE_HISTORY')} SET PI = 0 WHERE PI IS NULL")
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1
              FROM sys.default_constraints dc
              JOIN sys.columns c
                ON c.object_id = dc.parent_object_id
               AND c.column_id = dc.parent_column_id
              WHERE dc.parent_object_id = OBJECT_ID('{_fq('TEAM_RATE_HISTORY')}')
                AND c.name = 'PI'
            )
            BEGIN
              ALTER TABLE {_fq('TEAM_RATE_HISTORY')} ADD CONSTRAINT DF_TEAM_RATE_HISTORY_PI DEFAULT 0 FOR PI;
            END
            """
        )
        if enforce_pi_not_null:
            execute(f"ALTER TABLE {_fq('TEAM_RATE_HISTORY')} ALTER COLUMN PI INT NOT NULL")
    except Exception:
        pass
    execute(f"ALTER TABLE {_fq('TEAM_RATE_HISTORY')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")

    # Program rates per location/year/PI
    execute(f"""
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PROGRAM_RATE_HISTORY' AND schema_id = SCHEMA_ID('{sch}'))
      BEGIN
        CREATE TABLE {_fq('PROGRAM_RATE_HISTORY')} (
          PROGRAMID NVARCHAR(255) NOT NULL,
          YEAR   INT NOT NULL,
          PI     INT NOT NULL CONSTRAINT DF_PROGRAM_RATE_HISTORY_PI DEFAULT 0,
          LOCATION NVARCHAR(50) NOT NULL,
          PROGRAM_XOM_RATE DECIMAL(18,2),
          UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
          UPDATED_BY NVARCHAR(255) NULL,
          CONSTRAINT PK_PROGRAM_RATE_HISTORY PRIMARY KEY (PROGRAMID, LOCATION, YEAR, PI)
        );
      END
    """)
    try:
        execute(f"UPDATE {_fq('PROGRAM_RATE_HISTORY')} SET PI = 0 WHERE PI IS NULL")
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1
              FROM sys.default_constraints dc
              JOIN sys.columns c
                ON c.object_id = dc.parent_object_id
               AND c.column_id = dc.parent_column_id
              WHERE dc.parent_object_id = OBJECT_ID('{_fq('PROGRAM_RATE_HISTORY')}')
                AND c.name = 'PI'
            )
            BEGIN
              ALTER TABLE {_fq('PROGRAM_RATE_HISTORY')} ADD CONSTRAINT DF_PROGRAM_RATE_HISTORY_PI DEFAULT 0 FOR PI;
            END
            """
        )
        if enforce_pi_not_null:
            execute(f"ALTER TABLE {_fq('PROGRAM_RATE_HISTORY')} ALTER COLUMN PI INT NOT NULL")
    except Exception:
        pass
    execute(f"ALTER TABLE {_fq('PROGRAM_RATE_HISTORY')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")

    # Headcount splits for XOM employees (team core, delivery, program staff)
    execute(f"""
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'TEAM_HEADCOUNT_HISTORY' AND schema_id = SCHEMA_ID('{sch}'))
      BEGIN
        CREATE TABLE {_fq('TEAM_HEADCOUNT_HISTORY')} (
          TEAMID NVARCHAR(255) NOT NULL,
          YEAR   INT NOT NULL,
          PI     INT NOT NULL CONSTRAINT DF_TEAM_HEADCOUNT_HISTORY_PI DEFAULT 0,
          CLASS  NVARCHAR(30) NOT NULL,
          LOCATION NVARCHAR(50) NOT NULL,
          HEADCOUNT FLOAT DEFAULT 0,
          UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
          UPDATED_BY NVARCHAR(255) NULL,
          CONSTRAINT PK_TEAM_HEADCOUNT_HISTORY PRIMARY KEY (TEAMID, YEAR, PI, CLASS, LOCATION)
        );
      END
    """)
    try:
        execute(f"UPDATE {_fq('TEAM_HEADCOUNT_HISTORY')} SET PI = 0 WHERE PI IS NULL")
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1
              FROM sys.default_constraints dc
              JOIN sys.columns c
                ON c.object_id = dc.parent_object_id
               AND c.column_id = dc.parent_column_id
              WHERE dc.parent_object_id = OBJECT_ID('{_fq('TEAM_HEADCOUNT_HISTORY')}')
                AND c.name = 'PI'
            )
            BEGIN
              ALTER TABLE {_fq('TEAM_HEADCOUNT_HISTORY')} ADD CONSTRAINT DF_TEAM_HEADCOUNT_HISTORY_PI DEFAULT 0 FOR PI;
            END
            """
        )
        if enforce_pi_not_null:
            execute(f"ALTER TABLE {_fq('TEAM_HEADCOUNT_HISTORY')} ALTER COLUMN PI INT NOT NULL")
    except Exception:
        pass
    execute(f"ALTER TABLE {_fq('TEAM_HEADCOUNT_HISTORY')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")

    # Contractor companies and rates
    execute(f"""
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'CONTRACTOR_COMPANY' AND schema_id = SCHEMA_ID('{sch}'))
      BEGIN
        CREATE TABLE {_fq('CONTRACTOR_COMPANY')} (
          COMPANYID NVARCHAR(255) PRIMARY KEY,
          NAME NVARCHAR(255) UNIQUE,
          ACTIVE BIT DEFAULT 1,
          NOTES NVARCHAR(MAX),
          UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
          UPDATED_BY NVARCHAR(255) NULL
        );
      END
    """)
    execute(f"ALTER TABLE {_fq('CONTRACTOR_COMPANY')} ADD COLUMN IF NOT EXISTS ACTIVE BIT DEFAULT 1")
    execute(f"ALTER TABLE {_fq('CONTRACTOR_COMPANY')} ADD COLUMN IF NOT EXISTS NOTES NVARCHAR(MAX)")
    execute(f"ALTER TABLE {_fq('CONTRACTOR_COMPANY')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")

    execute(f"""
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'CONTRACTOR_RATE_HISTORY' AND schema_id = SCHEMA_ID('{sch}'))
      BEGIN
        CREATE TABLE {_fq('CONTRACTOR_RATE_HISTORY')} (
          COMPANYID NVARCHAR(255) NOT NULL,
          YEAR   INT NOT NULL,
          PI     INT NOT NULL CONSTRAINT DF_CONTRACTOR_RATE_HISTORY_PI DEFAULT 0,
          CLASS  NVARCHAR(30) NOT NULL, /* CONTRACTOR_CS or CONTRACTOR_C */
          RATE   DECIMAL(18,2),
          UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
          UPDATED_BY NVARCHAR(255) NULL,
          CONSTRAINT PK_CONTRACTOR_RATE_HISTORY PRIMARY KEY (COMPANYID, CLASS, YEAR, PI)
        );
      END
    """)
    try:
        execute(f"UPDATE {_fq('CONTRACTOR_RATE_HISTORY')} SET PI = 0 WHERE PI IS NULL")
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1
              FROM sys.default_constraints dc
              JOIN sys.columns c
                ON c.object_id = dc.parent_object_id
               AND c.column_id = dc.parent_column_id
              WHERE dc.parent_object_id = OBJECT_ID('{_fq('CONTRACTOR_RATE_HISTORY')}')
                AND c.name = 'PI'
            )
            BEGIN
              ALTER TABLE {_fq('CONTRACTOR_RATE_HISTORY')} ADD CONSTRAINT DF_CONTRACTOR_RATE_HISTORY_PI DEFAULT 0 FOR PI;
            END
            """
        )
        if enforce_pi_not_null:
            execute(f"ALTER TABLE {_fq('CONTRACTOR_RATE_HISTORY')} ALTER COLUMN PI INT NOT NULL")
    except Exception:
        pass
    execute(f"ALTER TABLE {_fq('CONTRACTOR_RATE_HISTORY')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")

    # Headcount per contractor company
    execute(f"""
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'TEAM_CONTRACTOR_HEADCOUNT' AND schema_id = SCHEMA_ID('{sch}'))
      BEGIN
        CREATE TABLE {_fq('TEAM_CONTRACTOR_HEADCOUNT')} (
          TEAMID NVARCHAR(255) NOT NULL,
          YEAR   INT NOT NULL,
          PI     INT NOT NULL CONSTRAINT DF_TEAM_CONTRACTOR_HEADCOUNT_PI DEFAULT 0,
          CLASS  NVARCHAR(30) NOT NULL,
          COMPANYID NVARCHAR(255) NOT NULL,
          HEADCOUNT FLOAT DEFAULT 0,
          UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
          UPDATED_BY NVARCHAR(255) NULL,
          CONSTRAINT PK_TEAM_CONTRACTOR_HEADCOUNT PRIMARY KEY (TEAMID, YEAR, PI, CLASS, COMPANYID)
        );
      END
    """)
    try:
        execute(f"UPDATE {_fq('TEAM_CONTRACTOR_HEADCOUNT')} SET PI = 0 WHERE PI IS NULL")
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1
              FROM sys.default_constraints dc
              JOIN sys.columns c
                ON c.object_id = dc.parent_object_id
               AND c.column_id = dc.parent_column_id
              WHERE dc.parent_object_id = OBJECT_ID('{_fq('TEAM_CONTRACTOR_HEADCOUNT')}')
                AND c.name = 'PI'
            )
            BEGIN
              ALTER TABLE {_fq('TEAM_CONTRACTOR_HEADCOUNT')} ADD CONSTRAINT DF_TEAM_CONTRACTOR_HEADCOUNT_PI DEFAULT 0 FOR PI;
            END
            """
        )
        if enforce_pi_not_null:
            execute(f"ALTER TABLE {_fq('TEAM_CONTRACTOR_HEADCOUNT')} ALTER COLUMN PI INT NOT NULL")
    except Exception:
        pass
    execute(f"ALTER TABLE {_fq('TEAM_CONTRACTOR_HEADCOUNT')} ADD COLUMN IF NOT EXISTS UPDATED_BY NVARCHAR(255)")

    # Contractor rates can be stored as annual (PI=0). When expanding to PI, prefer the
    # ADO iteration calendar for the year (if present) and fall back to PI 1..4.
    try:
        ensure_ado_iteration_calendar_table()
    except Exception:
        pass

    # Effective views (PI expansion + specificity ordering)
    try:
        execute("SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
    except Exception:
        pass
    # Helper numbers table inline
    nums_union = "SELECT 1 AS PI UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4"
    _exec_retry(f"""
        CREATE OR ALTER VIEW {_svq('VW_TEAM_RATE_EFFECTIVE')} AS
        WITH base AS (
            SELECT TEAMID, YEAR, PI, LOCATION, XOM_RATE, UPDATED_AT,
                   CASE WHEN PI IS NULL OR PI = 0 THEN 0 ELSE 1 END AS SPECIFICITY
            FROM {_svq('TEAM_RATE_HISTORY')}
        ), nums AS ({nums_union}), expanded AS (
            SELECT b.TEAMID, b.YEAR,
                   CASE WHEN b.PI IS NULL OR b.PI = 0 THEN n.PI ELSE b.PI END AS PI,
                   b.LOCATION, b.XOM_RATE, b.UPDATED_AT, b.SPECIFICITY
            FROM base b
            JOIN nums n ON (b.PI IS NULL OR b.PI = 0) OR b.PI = n.PI
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY TEAMID, YEAR, PI, LOCATION
                ORDER BY SPECIFICITY DESC, UPDATED_AT DESC
            ) AS rn
            FROM expanded
        )
        SELECT TEAMID, YEAR, PI, LOCATION, XOM_RATE
        FROM ranked WHERE rn = 1
    """)
    _exec_retry(f"""
        CREATE OR ALTER VIEW {_svq('VW_PROGRAM_RATE_EFFECTIVE')} AS
        WITH base AS (
            SELECT PROGRAMID, YEAR, PI, LOCATION, PROGRAM_XOM_RATE, UPDATED_AT,
                   CASE WHEN PI IS NULL OR PI = 0 THEN 0 ELSE 1 END AS SPECIFICITY
            FROM {_svq('PROGRAM_RATE_HISTORY')}
        ), nums AS ({nums_union}), expanded AS (
            SELECT b.PROGRAMID, b.YEAR,
                   CASE WHEN b.PI IS NULL OR b.PI = 0 THEN n.PI ELSE b.PI END AS PI,
                   b.LOCATION, b.PROGRAM_XOM_RATE, b.UPDATED_AT, b.SPECIFICITY
            FROM base b
            JOIN nums n ON (b.PI IS NULL OR b.PI = 0) OR b.PI = n.PI
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY PROGRAMID, YEAR, PI, LOCATION
                ORDER BY SPECIFICITY DESC, UPDATED_AT DESC
            ) AS rn
            FROM expanded
        )
        SELECT PROGRAMID, YEAR, PI, LOCATION, PROGRAM_XOM_RATE
        FROM ranked WHERE rn = 1
    """)
    _exec_retry(f"""
        CREATE OR ALTER VIEW {_svq('VW_GLOBAL_RATE_EFFECTIVE')} AS
        WITH base AS (
            SELECT YEAR, PI, LOCATION, XOM_RATE AS RATE, UPDATED_AT,
                   CASE WHEN PI IS NULL OR PI = 0 THEN 0 ELSE 1 END AS SPECIFICITY
            FROM {_svq('TEAM_RATE_HISTORY')}
            UNION ALL
            SELECT YEAR, PI, LOCATION, PROGRAM_XOM_RATE AS RATE, UPDATED_AT,
                   CASE WHEN PI IS NULL OR PI = 0 THEN 0 ELSE 1 END AS SPECIFICITY
            FROM {_svq('PROGRAM_RATE_HISTORY')}
        ), nums AS ({nums_union}), expanded AS (
            SELECT b.YEAR,
                   CASE WHEN b.PI IS NULL OR b.PI = 0 THEN n.PI ELSE b.PI END AS PI,
                   b.LOCATION, b.RATE, b.UPDATED_AT, b.SPECIFICITY
            FROM base b
            JOIN nums n ON (b.PI IS NULL OR b.PI = 0) OR b.PI = n.PI
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY YEAR, PI, LOCATION
                ORDER BY SPECIFICITY DESC, UPDATED_AT DESC
            ) AS rn
            FROM expanded
        )
        SELECT YEAR, PI, LOCATION, RATE, UPDATED_AT
        FROM ranked WHERE rn = 1
    """)
    _exec_retry(f"""
        CREATE OR ALTER VIEW {_svq('VW_TEAM_HEADCOUNT_EFFECTIVE')} AS
        WITH base AS (
            SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, UPDATED_AT,
                   CASE WHEN PI IS NULL OR PI = 0 THEN 0 ELSE 1 END AS SPECIFICITY
            FROM {_svq('TEAM_HEADCOUNT_HISTORY')}
        ), nums AS ({nums_union}), expanded AS (
            SELECT b.TEAMID, b.YEAR,
                   CASE WHEN b.PI IS NULL OR b.PI = 0 THEN n.PI ELSE b.PI END AS PI,
                   b.CLASS, b.LOCATION, b.HEADCOUNT, b.UPDATED_AT, b.SPECIFICITY
            FROM base b
            JOIN nums n ON (b.PI IS NULL OR b.PI = 0) OR b.PI = n.PI
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY TEAMID, YEAR, PI, CLASS, LOCATION
                ORDER BY SPECIFICITY DESC, UPDATED_AT DESC
            ) AS rn
            FROM expanded
        )
        SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT
        FROM ranked WHERE rn = 1
    """)
    _exec_retry(f"""
        CREATE OR ALTER VIEW {_svq('VW_CONTRACTOR_RATE_EFFECTIVE')} AS
        WITH base AS (
            SELECT
              UPPER(LTRIM(RTRIM(TRY_CONVERT(NVARCHAR(255), COMPANYID)))) AS COMPANY_KEY,
              TRY_CONVERT(INT, YEAR) AS YEAR,
              TRY_CONVERT(INT, PI) AS PI,
              UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(TRY_CONVERT(NVARCHAR(30), CLASS),' ','_'),'-','_'),'/','_')))) AS CLASS,
              TRY_CONVERT(DECIMAL(18,2), RATE) AS RATE,
              UPDATED_AT,
                   CASE WHEN PI IS NULL OR PI = 0 THEN 0 ELSE 1 END AS SPECIFICITY
            FROM {_svq('CONTRACTOR_RATE_HISTORY')}
        ), base_years AS (
            SELECT DISTINCT YEAR
            FROM base
            WHERE YEAR IS NOT NULL
        ), nums AS ({nums_union}), pi_cal AS (
            SELECT
              TRY_CONVERT(INT, YEAR) AS YEAR,
              CASE
                WHEN PI_NAME IS NOT NULL AND PATINDEX('%I[0-9]%', UPPER(PI_NAME)) > 0
                  THEN TRY_CONVERT(INT, SUBSTRING(UPPER(PI_NAME), PATINDEX('%I[0-9]%', UPPER(PI_NAME)) + 1, 10))
                WHEN ITERATION_LEVEL3 IS NOT NULL AND PATINDEX('% I[0-9]%', UPPER(ITERATION_LEVEL3)) > 0
                  THEN TRY_CONVERT(INT, SUBSTRING(UPPER(ITERATION_LEVEL3), PATINDEX('% I[0-9]%', UPPER(ITERATION_LEVEL3)) + 2, 10))
                ELSE NULL
              END AS PI
            FROM {_svq('ADO_ITERATION_CALENDAR')}
            WHERE UPPER(LTRIM(RTRIM(COALESCE(ITERATION_GRAIN,'')))) = 'PI'
              AND TRY_CONVERT(INT, YEAR) IS NOT NULL
        ), pi_nums AS (
            SELECT bys.YEAR, pc.PI
            FROM base_years bys
            JOIN (SELECT DISTINCT YEAR, PI FROM pi_cal WHERE PI IS NOT NULL) pc
              ON pc.YEAR = bys.YEAR
            UNION ALL
            SELECT bys.YEAR, n.PI
            FROM base_years bys
            CROSS JOIN nums n
            WHERE NOT EXISTS (SELECT 1 FROM pi_cal pc WHERE pc.YEAR = bys.YEAR AND pc.PI IS NOT NULL)
        ), expanded AS (
            SELECT
              b.COMPANY_KEY,
              b.YEAR,
              CASE WHEN b.PI IS NULL OR b.PI = 0 THEN pn.PI ELSE b.PI END AS PI,
              b.CLASS,
              b.RATE,
              b.UPDATED_AT,
              b.SPECIFICITY
            FROM base b
            LEFT JOIN pi_nums pn
              ON pn.YEAR = b.YEAR
             AND (b.PI IS NULL OR b.PI = 0)
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY COMPANY_KEY, YEAR, PI, CLASS
                ORDER BY SPECIFICITY DESC, UPDATED_AT DESC
            ) AS rn
            FROM expanded
        )
        SELECT
          COALESCE(cm.COMPANYID, r.COMPANY_KEY) AS COMPANYID,
          r.YEAR,
          r.PI,
          r.CLASS,
          r.RATE,
          CAST(CASE WHEN cm.COMPANYID IS NULL THEN 1 ELSE 0 END AS BIT) AS COMPANYID_MISSING
        FROM ranked r
        OUTER APPLY (
          SELECT TOP 1 c.COMPANYID
          FROM {_svq('CONTRACTOR_COMPANY')} c
          WHERE UPPER(LTRIM(RTRIM(c.COMPANYID))) = r.COMPANY_KEY
             OR UPPER(LTRIM(RTRIM(c.NAME))) = r.COMPANY_KEY
          ORDER BY CASE WHEN UPPER(LTRIM(RTRIM(c.COMPANYID))) = r.COMPANY_KEY THEN 0 ELSE 1 END, c.COMPANYID
        ) cm
        WHERE r.rn = 1
    """)
    _exec_retry(f"""
        CREATE OR ALTER VIEW {_svq('VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE')} AS
        WITH base AS (
            SELECT
              TEAMID,
              TRY_CONVERT(INT, YEAR) AS YEAR,
              TRY_CONVERT(INT, PI) AS PI,
              UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(TRY_CONVERT(NVARCHAR(30), CLASS),' ','_'),'-','_'),'/','_')))) AS CLASS,
              UPPER(LTRIM(RTRIM(TRY_CONVERT(NVARCHAR(255), COMPANYID)))) AS COMPANY_KEY,
              TRY_CONVERT(FLOAT, HEADCOUNT) AS HEADCOUNT,
              UPDATED_AT,
                   CASE WHEN PI IS NULL OR PI = 0 THEN 0 ELSE 1 END AS SPECIFICITY
            FROM {_svq('TEAM_CONTRACTOR_HEADCOUNT')}
        ), nums AS ({nums_union}), expanded AS (
            SELECT b.TEAMID, b.YEAR,
                   CASE WHEN b.PI IS NULL OR b.PI = 0 THEN n.PI ELSE b.PI END AS PI,
                   b.CLASS, b.COMPANY_KEY, b.HEADCOUNT, b.UPDATED_AT, b.SPECIFICITY
            FROM base b
            JOIN nums n ON (b.PI IS NULL OR b.PI = 0) OR b.PI = n.PI
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY TEAMID, YEAR, PI, CLASS, COMPANY_KEY
                ORDER BY SPECIFICITY DESC, UPDATED_AT DESC
            ) AS rn
            FROM expanded
        )
        SELECT
          r.TEAMID,
          r.YEAR,
          r.PI,
          r.CLASS,
          COALESCE(cm.COMPANYID, r.COMPANY_KEY) AS COMPANYID,
          r.HEADCOUNT,
          CAST(CASE WHEN cm.COMPANYID IS NULL THEN 1 ELSE 0 END AS BIT) AS COMPANYID_MISSING
        FROM ranked r
        OUTER APPLY (
          SELECT TOP 1 c.COMPANYID
          FROM {_svq('CONTRACTOR_COMPANY')} c
          WHERE UPPER(LTRIM(RTRIM(c.COMPANYID))) = r.COMPANY_KEY
             OR UPPER(LTRIM(RTRIM(c.NAME))) = r.COMPANY_KEY
          ORDER BY CASE WHEN UPPER(LTRIM(RTRIM(c.COMPANYID))) = r.COMPANY_KEY THEN 0 ELSE 1 END, c.COMPANYID
        ) cm
        WHERE r.rn = 1
    """)

    execute(f"""
        CREATE OR ALTER VIEW {_svq('VW_TEAM_WEIGHTED_RATES')} AS
        WITH loc_hc AS (
            SELECT h.TEAMID, h.YEAR, h.PI, h.CLASS, h.LOCATION, h.HEADCOUNT,
                   COALESCE(tr.XOM_RATE, gr.RATE) AS RATE,
                   t.PROGRAMID
            FROM {_svq('VW_TEAM_HEADCOUNT_EFFECTIVE')} h
            LEFT JOIN {_svq('VW_TEAM_RATE_EFFECTIVE')} tr
              ON tr.TEAMID = h.TEAMID AND tr.YEAR = h.YEAR AND tr.PI = h.PI AND tr.LOCATION = h.LOCATION
            LEFT JOIN {_svq('VW_GLOBAL_RATE_EFFECTIVE')} gr
              ON gr.YEAR = h.YEAR AND gr.PI = h.PI AND gr.LOCATION = h.LOCATION
            LEFT JOIN {_svq('TEAMS')} t ON t.TEAMID = h.TEAMID
        ), loc_hc_program AS (
            SELECT lh.TEAMID, lh.YEAR, lh.PI, lh.CLASS, lh.LOCATION, lh.HEADCOUNT,
                   /* Do not use legacy PROGRAMS.PROGRAM_XOM_RATE; rely on rate history + global defaults only. */
                   COALESCE(pr.PROGRAM_XOM_RATE, gr.RATE, lh.RATE) AS RATE
            FROM loc_hc lh
            LEFT JOIN {_svq('VW_PROGRAM_RATE_EFFECTIVE')} pr
              ON pr.PROGRAMID = lh.PROGRAMID AND pr.YEAR = lh.YEAR AND pr.PI = lh.PI AND pr.LOCATION = lh.LOCATION
            LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = lh.PROGRAMID
            LEFT JOIN {_svq('VW_GLOBAL_RATE_EFFECTIVE')} gr
              ON gr.YEAR = lh.YEAR AND gr.PI = lh.PI AND gr.LOCATION = lh.LOCATION
        ), loc_totals AS (
            SELECT TEAMID, YEAR, PI,
                   SUM(CASE WHEN CLASS = 'TEAM' THEN HEADCOUNT ELSE 0 END) AS TEAM_TOTAL,
                   SUM(CASE WHEN CLASS = 'DELIVERY' THEN HEADCOUNT ELSE 0 END) AS DELIVERY_TOTAL,
                   SUM(CASE WHEN CLASS = 'PROGRAM' THEN HEADCOUNT ELSE 0 END) AS PROGRAM_TOTAL,
                   SUM(CASE WHEN CLASS = 'TEAM' THEN HEADCOUNT * RATE ELSE 0 END) AS TEAM_NUM,
                   SUM(CASE WHEN CLASS = 'DELIVERY' THEN HEADCOUNT * RATE ELSE 0 END) AS DELIVERY_NUM,
                   SUM(CASE WHEN CLASS = 'PROGRAM' THEN HEADCOUNT * RATE ELSE 0 END) AS PROGRAM_NUM
            FROM loc_hc_program
            GROUP BY TEAMID, YEAR, PI
        ), cons AS (
            SELECT h.TEAMID, h.YEAR, h.PI, h.CLASS, h.COMPANYID, h.HEADCOUNT,
                   COALESCE(cr.RATE, 0) AS RATE
            FROM {_svq('VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE')} h
            LEFT JOIN {_svq('VW_CONTRACTOR_RATE_EFFECTIVE')} cr
              ON cr.COMPANYID = h.COMPANYID AND cr.YEAR = h.YEAR AND cr.PI = h.PI AND cr.CLASS = h.CLASS
        ), cons_totals AS (
            SELECT TEAMID, YEAR, PI,
                   SUM(CASE WHEN CLASS = 'CONTRACTOR_CS' THEN HEADCOUNT ELSE 0 END) AS CS_TOTAL,
                   SUM(CASE WHEN CLASS = 'CONTRACTOR_C' THEN HEADCOUNT ELSE 0 END) AS C_TOTAL,
                   SUM(CASE WHEN CLASS = 'CONTRACTOR_CS' THEN HEADCOUNT * RATE ELSE 0 END) AS CS_NUM,
                   SUM(CASE WHEN CLASS = 'CONTRACTOR_C' THEN HEADCOUNT * RATE ELSE 0 END) AS C_NUM
            FROM cons
            GROUP BY TEAMID, YEAR, PI
        ), defaults AS (
            SELECT YEAR, PI, MAX(RATE) AS DEFAULT_RATE
            FROM {_svq('VW_GLOBAL_RATE_EFFECTIVE')}
            WHERE LOCATION = 'GBC'
            GROUP BY YEAR, PI
        ), prog_defaults AS (
            SELECT p.PROGRAMID, gr.YEAR, gr.PI, MAX(gr.RATE) AS DEFAULT_RATE
            FROM {_svq('VW_GLOBAL_RATE_EFFECTIVE')} gr
            JOIN {_svq('TEAMS')} tt ON tt.PROGRAMID IS NOT NULL
            JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = tt.PROGRAMID
            WHERE gr.LOCATION = 'GBC'
            GROUP BY p.PROGRAMID, gr.YEAR, gr.PI
        )
        SELECT
            COALESCE(lt.TEAMID, ct.TEAMID) AS TEAMID,
            COALESCE(lt.YEAR, ct.YEAR) AS YEAR,
            COALESCE(lt.PI, ct.PI) AS PI,
            CASE WHEN COALESCE(lt.TEAM_TOTAL, 0) > 0 THEN lt.TEAM_NUM / NULLIF(lt.TEAM_TOTAL,0) ELSE COALESCE(d.DEFAULT_RATE, pr.PROGRAM_XOM_RATE, 0) END AS TEAM_RATE,
            CASE WHEN COALESCE(lt.DELIVERY_TOTAL, 0) > 0 THEN lt.DELIVERY_NUM / NULLIF(lt.DELIVERY_TOTAL,0) ELSE COALESCE(d.DEFAULT_RATE, pr.PROGRAM_XOM_RATE, 0) END AS DELIVERY_RATE,
            CASE WHEN COALESCE(lt.PROGRAM_TOTAL, 0) > 0 THEN lt.PROGRAM_NUM / NULLIF(lt.PROGRAM_TOTAL,0) ELSE COALESCE(pd.DEFAULT_RATE, d.DEFAULT_RATE, pr.PROGRAM_XOM_RATE, 0) END AS PROGRAM_RATE,
            CASE WHEN COALESCE(ct.CS_TOTAL, 0) > 0 THEN ct.CS_NUM / NULLIF(ct.CS_TOTAL,0) ELSE 0 END AS CONTRACTOR_CS_RATE,
            CASE WHEN COALESCE(ct.C_TOTAL, 0) > 0 THEN ct.C_NUM / NULLIF(ct.C_TOTAL,0) ELSE 0 END AS CONTRACTOR_C_RATE
        FROM loc_totals lt
        FULL OUTER JOIN cons_totals ct
          ON lt.TEAMID = ct.TEAMID AND lt.YEAR = ct.YEAR AND lt.PI = ct.PI
        LEFT JOIN {_svq('TEAMS')} t ON t.TEAMID = COALESCE(lt.TEAMID, ct.TEAMID)
        LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
        LEFT JOIN {_svq('VW_PROGRAM_RATE_EFFECTIVE')} pr
          ON pr.PROGRAMID = t.PROGRAMID AND pr.YEAR = COALESCE(lt.YEAR, ct.YEAR) AND pr.PI = COALESCE(lt.PI, ct.PI) AND pr.LOCATION = 'GBC'
        LEFT JOIN defaults d
          ON d.YEAR = COALESCE(lt.YEAR, ct.YEAR) AND d.PI = COALESCE(lt.PI, ct.PI)
        LEFT JOIN prog_defaults pd
          ON pd.PROGRAMID = t.PROGRAMID AND pd.YEAR = COALESCE(lt.YEAR, ct.YEAR) AND pd.PI = COALESCE(lt.PI, ct.PI)
    """)

    # Debug helper: identify contractor headcount rows where the rate join is missing.
    _exec_retry(f"""
      CREATE OR ALTER VIEW {_svq('VW_TEAM_CONTRACTOR_RATE_JOIN_GAPS')} AS
      SELECT
        h.TEAMID,
        t.TEAMNAME,
        p.PROGRAMNAME,
        h.YEAR,
        h.PI,
        h.CLASS,
        h.COMPANYID,
        h.HEADCOUNT,
        cr.RATE
      FROM {_svq('VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE')} h
      LEFT JOIN {_svq('VW_CONTRACTOR_RATE_EFFECTIVE')} cr
        ON cr.COMPANYID = h.COMPANYID
       AND cr.YEAR = h.YEAR
       AND cr.PI = h.PI
       AND cr.CLASS = h.CLASS
      LEFT JOIN {_svq('TEAMS')} t ON t.TEAMID = h.TEAMID
      LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
      WHERE COALESCE(h.HEADCOUNT, 0) > 0
        AND cr.RATE IS NULL
    """)


# Location/program rate + headcount helpers
@_versioned_write("TEAM_RATE_HISTORY.UPSERT")
def upsert_team_rate_history(team_id: str, year: int, pi: Optional[int], location: str, xom_rate: Optional[float], updated_by: Optional[str] = None) -> None:
    ensure_location_and_contractor_tables()
    execute(
        f"""
        MERGE INTO {_fq('TEAM_RATE_HISTORY')} AS t
        USING (SELECT ? AS TEAMID, ? AS YEAR, ? AS PI, ? AS LOCATION, ? AS XOM_RATE, ? AS UPDATED_BY) AS s
        ON CONVERT(NVARCHAR(255), t.TEAMID) = CONVERT(NVARCHAR(255), s.TEAMID)
           AND t.YEAR = s.YEAR
           AND ISNULL(t.PI,0) = ISNULL(s.PI,0)
           AND ISNULL(CONVERT(NVARCHAR(255), t.LOCATION), '') = ISNULL(CONVERT(NVARCHAR(255), s.LOCATION), '')
        WHEN MATCHED THEN UPDATE SET
          XOM_RATE = s.XOM_RATE,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, LOCATION, XOM_RATE, UPDATED_BY)
        VALUES (s.TEAMID, s.YEAR, s.PI, s.LOCATION, s.XOM_RATE, s.UPDATED_BY);
        """,
        (team_id, year, pi, location, xom_rate, updated_by),
    )
@_versioned_write("PROGRAM_RATE_HISTORY.UPSERT")
def upsert_program_rate_history(program_id: str, year: int, pi: Optional[int], location: str, program_rate: Optional[float], updated_by: Optional[str] = None) -> None:
    ensure_location_and_contractor_tables()
    execute(
        f"""
        MERGE INTO {_fq('PROGRAM_RATE_HISTORY')} AS t
        USING (SELECT ? AS PROGRAMID, ? AS YEAR, ? AS PI, ? AS LOCATION, ? AS PROGRAM_XOM_RATE, ? AS UPDATED_BY) AS s
        ON CONVERT(NVARCHAR(255), t.PROGRAMID) = CONVERT(NVARCHAR(255), s.PROGRAMID)
           AND t.YEAR = s.YEAR
           AND ISNULL(t.PI,0) = ISNULL(s.PI,0)
           AND ISNULL(CONVERT(NVARCHAR(255), t.LOCATION), '') = ISNULL(CONVERT(NVARCHAR(255), s.LOCATION), '')
        WHEN MATCHED THEN UPDATE SET
          PROGRAM_XOM_RATE = s.PROGRAM_XOM_RATE,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (PROGRAMID, YEAR, PI, LOCATION, PROGRAM_XOM_RATE, UPDATED_BY)
        VALUES (s.PROGRAMID, s.YEAR, s.PI, s.LOCATION, s.PROGRAM_XOM_RATE, s.UPDATED_BY);
        """,
        (program_id, year, pi, location, program_rate, updated_by),
    )
@_versioned_write("TEAM_HEADCOUNT_HISTORY.UPSERT")
def upsert_team_headcount(team_id: str, year: int, pi: Optional[int], cls: str, location: str, headcount: float, updated_by: Optional[str] = None) -> None:
    ensure_location_and_contractor_tables()
    execute(
        f"""
        MERGE INTO {_fq('TEAM_HEADCOUNT_HISTORY')} AS t
        USING (SELECT ? AS TEAMID, ? AS YEAR, ? AS PI, ? AS CLASS, ? AS LOCATION, ? AS HEADCOUNT, ? AS UPDATED_BY) AS s
        ON CONVERT(NVARCHAR(255), t.TEAMID) = CONVERT(NVARCHAR(255), s.TEAMID)
           AND t.YEAR = s.YEAR
           AND ISNULL(t.PI,0) = ISNULL(s.PI,0)
           AND ISNULL(CONVERT(NVARCHAR(255), t.CLASS), '') = ISNULL(CONVERT(NVARCHAR(255), s.CLASS), '')
           AND ISNULL(CONVERT(NVARCHAR(255), t.LOCATION), '') = ISNULL(CONVERT(NVARCHAR(255), s.LOCATION), '')
        WHEN MATCHED THEN UPDATE SET
          HEADCOUNT = s.HEADCOUNT,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, UPDATED_BY)
        VALUES (s.TEAMID, s.YEAR, s.PI, s.CLASS, s.LOCATION, s.HEADCOUNT, s.UPDATED_BY);
        """,
        (team_id, year, pi, cls, location, headcount, updated_by),
    )
@_versioned_write("PROGRAM_ADDITIONAL_COSTS.UPSERT")
def upsert_program_additional_cost(
    program_id: str,
    year: int,
    month: int,
    cost_type: str,
    subtype: Optional[str],
    amount: float,
    updated_by: Optional[str] = None,
    *,
    description: Optional[str] = None,
    currency: Optional[str] = None,
    is_recurring: Optional[bool] = None,
) -> None:
    ensure_tables()
    try:
        amount_val = float(amount) if amount is not None else 0.0
    except Exception:
        amount_val = 0.0
    desc_val = (str(description).strip() if description is not None else None) or None
    cur_val = (str(currency).strip() if currency is not None else None) or None
    rec_val = 1 if (is_recurring is None or bool(is_recurring)) else 0
    execute(
        f"""
        MERGE INTO {_fq('PROGRAM_ADDITIONAL_COSTS')} AS t
        USING (
          SELECT
            ? AS PROGRAMID,
            ? AS YEAR,
            ? AS MONTH,
            ? AS COST_TYPE,
            ? AS SUBTYPE,
            ? AS DESCRIPTION,
            ? AS CURRENCY,
            ? AS IS_RECURRING,
            ? AS AMOUNT,
            ? AS UPDATED_BY
        ) AS s
        ON t.PROGRAMID = s.PROGRAMID AND t.YEAR = s.YEAR AND t.MONTH = s.MONTH AND t.COST_TYPE = s.COST_TYPE AND ISNULL(t.SUBTYPE,'') = ISNULL(s.SUBTYPE,'')
        WHEN MATCHED THEN UPDATE SET
          AMOUNT = s.AMOUNT,
          DESCRIPTION = s.DESCRIPTION,
          CURRENCY = s.CURRENCY,
          IS_RECURRING = s.IS_RECURRING,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (PROGRAMID, YEAR, MONTH, COST_TYPE, SUBTYPE, DESCRIPTION, CURRENCY, IS_RECURRING, AMOUNT, UPDATED_AT, UPDATED_BY)
        VALUES (s.PROGRAMID, s.YEAR, s.MONTH, s.COST_TYPE, s.SUBTYPE, s.DESCRIPTION, s.CURRENCY, s.IS_RECURRING, s.AMOUNT, SYSDATETIME(), s.UPDATED_BY);
        """,
        (program_id, year, month, cost_type, subtype or "", desc_val, cur_val, rec_val, amount_val, updated_by),
    )
def list_program_additional_costs(program_id: str) -> pd.DataFrame:
    ensure_tables()
    return fetch_df(
        f"""
        SELECT PROGRAMID, YEAR, MONTH, COST_TYPE, SUBTYPE, DESCRIPTION, CURRENCY, IS_RECURRING, AMOUNT, UPDATED_AT, UPDATED_BY
        FROM {_fq('PROGRAM_ADDITIONAL_COSTS')}
        WHERE PROGRAMID = %s
        ORDER BY YEAR DESC, MONTH DESC, COST_TYPE, SUBTYPE
        """,
        (program_id,),
    )


@_versioned_write("PROGRAM_ADDITIONAL_COSTS.DELETE")
def delete_program_additional_cost(program_id: str, year: int, month: int, cost_type: str, subtype: Optional[str]) -> None:
    ensure_tables()
    execute(
        f"""
        DELETE FROM {_fq('PROGRAM_ADDITIONAL_COSTS')}
        WHERE PROGRAMID = %s
          AND YEAR = %s
          AND MONTH = %s
          AND COST_TYPE = %s
          AND ISNULL(SUBTYPE,'') = %s
        """,
        (program_id, int(year), int(month), str(cost_type), str(subtype or "")),
    )

@_versioned_write("CONTRACTOR_COMPANY.UPSERT")
def upsert_contractor_company(company_id: str, name: str, active: bool = True, notes: Optional[str] = None, updated_by: Optional[str] = None) -> None:
    ensure_location_and_contractor_tables()
    execute(
        f"""
        MERGE INTO {_fq('CONTRACTOR_COMPANY')} AS t
        USING (SELECT ? AS COMPANYID, ? AS NAME, ? AS ACTIVE, ? AS NOTES, ? AS UPDATED_BY) AS s
        ON t.COMPANYID = s.COMPANYID
        WHEN MATCHED THEN UPDATE SET
          NAME = s.NAME,
          ACTIVE = s.ACTIVE,
          NOTES = s.NOTES,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (COMPANYID, NAME, ACTIVE, NOTES, UPDATED_BY)
        VALUES (s.COMPANYID, s.NAME, s.ACTIVE, s.NOTES, s.UPDATED_BY);
        """,
        (company_id, name, active, notes, updated_by),
    )

@_versioned_write("CONTRACTOR_RATE_HISTORY.UPSERT")
def upsert_contractor_rate(company_id: str, year: int, pi: Optional[int], cls: str, rate: Optional[float], updated_by: Optional[str] = None) -> None:
    ensure_location_and_contractor_tables()
    pi_val = pi if pi is not None else 0  # default to year-level when PI not provided
    execute(
        f"""
        MERGE INTO {_fq('CONTRACTOR_RATE_HISTORY')} AS t
        USING (SELECT ? AS COMPANYID, ? AS YEAR, ? AS PI, ? AS CLASS, ? AS RATE, ? AS UPDATED_BY) AS s
        ON t.COMPANYID = s.COMPANYID AND t.YEAR = s.YEAR AND ISNULL(t.PI,0) = ISNULL(s.PI,0) AND t.CLASS = s.CLASS
        WHEN MATCHED THEN UPDATE SET
          RATE = s.RATE,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (COMPANYID, YEAR, PI, CLASS, RATE, UPDATED_BY)
        VALUES (s.COMPANYID, s.YEAR, s.PI, s.CLASS, s.RATE, s.UPDATED_BY);
        """,
        (company_id, year, pi_val, cls, rate, updated_by),
    )
@_versioned_write("TEAM_CONTRACTOR_HEADCOUNT.UPSERT")
def upsert_team_contractor_headcount(team_id: str, year: int, pi: Optional[int], cls: str, company_id: str, headcount: float, updated_by: Optional[str] = None) -> None:
    ensure_location_and_contractor_tables()
    execute(
        f"""
        MERGE INTO {_fq('TEAM_CONTRACTOR_HEADCOUNT')} AS t
        USING (SELECT ? AS TEAMID, ? AS YEAR, ? AS PI, ? AS CLASS, ? AS COMPANYID, ? AS HEADCOUNT, ? AS UPDATED_BY) AS s
        ON t.TEAMID = s.TEAMID AND t.YEAR = s.YEAR AND ISNULL(t.PI,0) = ISNULL(s.PI,0) AND t.CLASS = s.CLASS AND t.COMPANYID = s.COMPANYID
        WHEN MATCHED THEN UPDATE SET
          HEADCOUNT = s.HEADCOUNT,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT, UPDATED_BY)
        VALUES (s.TEAMID, s.YEAR, s.PI, s.CLASS, s.COMPANYID, s.HEADCOUNT, s.UPDATED_BY);
        """,
        (team_id, year, pi, cls, company_id, headcount, updated_by),
    )
def list_contractor_companies() -> pd.DataFrame:
    ensure_location_and_contractor_tables()
    return fetch_df(f"SELECT COMPANYID, NAME, ACTIVE, NOTES, UPDATED_AT, UPDATED_BY FROM {_fq('CONTRACTOR_COMPANY')} ORDER BY NAME")


def list_team_rate_history(team_id: str) -> pd.DataFrame:
    ensure_location_and_contractor_tables()
    return fetch_df(
        f"""
        SELECT TEAMID, YEAR, PI, LOCATION, XOM_RATE, UPDATED_AT, UPDATED_BY
        FROM {_fq('TEAM_RATE_HISTORY')}
        WHERE TEAMID = ?
        ORDER BY YEAR DESC, ISNULL(PI,0) DESC, LOCATION
        """,
        (team_id,),
    )


def list_program_rate_history(program_id: str) -> pd.DataFrame:
    ensure_location_and_contractor_tables()
    return fetch_df(
        f"""
        SELECT PROGRAMID, YEAR, PI, LOCATION, PROGRAM_XOM_RATE, UPDATED_AT, UPDATED_BY
        FROM {_fq('PROGRAM_RATE_HISTORY')}
        WHERE PROGRAMID = ?
        ORDER BY YEAR DESC, ISNULL(PI,0) DESC, LOCATION
        """,
        (program_id,),
    )


def list_team_headcount(team_id: str) -> pd.DataFrame:
    ensure_location_and_contractor_tables()
    # NOTES/COMMENT columns are optional (some deployments add them manually).
    try:
        return fetch_df(
            f"""
            SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, NOTES, UPDATED_AT, UPDATED_BY
            FROM {_fq('TEAM_HEADCOUNT_HISTORY')}
            WHERE TEAMID = ?
            ORDER BY YEAR DESC, ISNULL(PI,0) DESC, CLASS, LOCATION
            """,
            (team_id,),
        )
    except Exception:
        try:
            return fetch_df(
                f"""
                SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, NOTE AS NOTES, UPDATED_AT, UPDATED_BY
                FROM {_fq('TEAM_HEADCOUNT_HISTORY')}
                WHERE TEAMID = ?
                ORDER BY YEAR DESC, ISNULL(PI,0) DESC, CLASS, LOCATION
                """,
                (team_id,),
            )
        except Exception:
            return fetch_df(
                f"""
                SELECT TEAMID, YEAR, PI, CLASS, LOCATION, HEADCOUNT, UPDATED_AT, UPDATED_BY
                FROM {_fq('TEAM_HEADCOUNT_HISTORY')}
                WHERE TEAMID = ?
                ORDER BY YEAR DESC, ISNULL(PI,0) DESC, CLASS, LOCATION
                """,
                (team_id,),
            )


def list_team_contractor_headcount(team_id: str) -> pd.DataFrame:
    ensure_location_and_contractor_tables()
    # NOTES/COMMENT columns are optional (some deployments add them manually).
    try:
        return fetch_df(
            f"""
            SELECT TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT, NOTES, UPDATED_AT, UPDATED_BY
            FROM {_fq('TEAM_CONTRACTOR_HEADCOUNT')}
            WHERE TEAMID = ?
            ORDER BY YEAR DESC, ISNULL(PI,0) DESC, CLASS, COMPANYID
            """,
            (team_id,),
        )
    except Exception:
        try:
            return fetch_df(
                f"""
                SELECT TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT, NOTE AS NOTES, UPDATED_AT, UPDATED_BY
                FROM {_fq('TEAM_CONTRACTOR_HEADCOUNT')}
                WHERE TEAMID = ?
                ORDER BY YEAR DESC, ISNULL(PI,0) DESC, CLASS, COMPANYID
                """,
                (team_id,),
            )
        except Exception:
            return fetch_df(
                f"""
                SELECT TEAMID, YEAR, PI, CLASS, COMPANYID, HEADCOUNT, UPDATED_AT, UPDATED_BY
                FROM {_fq('TEAM_CONTRACTOR_HEADCOUNT')}
                WHERE TEAMID = ?
                ORDER BY YEAR DESC, ISNULL(PI,0) DESC, CLASS, COMPANYID
                """,
                (team_id,),
            )


def list_contractor_rates(company_id: str) -> pd.DataFrame:
    ensure_location_and_contractor_tables()
    return fetch_df(
        f"""
        SELECT COMPANYID, CLASS, YEAR, PI, RATE, UPDATED_AT, UPDATED_BY
        FROM {_fq('CONTRACTOR_RATE_HISTORY')}
        WHERE COMPANYID = ?
        ORDER BY YEAR DESC, ISNULL(PI,0) DESC, CLASS
        """,
        (company_id,),
    )


# =========================================================
# Minimal list helpers (expand as needed during migration)
# =========================================================

def list_programs() -> pd.DataFrame:
    return fetch_df(
        f"""
        SELECT
            PROGRAMID,
            COALESCE(
              CASE
                WHEN UPPER(LTRIM(RTRIM(PROGRAMNAME))) IN ('', 'NONE', 'NULL', 'NAN', '<NA>') THEN NULL
                ELSE LTRIM(RTRIM(PROGRAMNAME))
              END,
              PROGRAMID
            ) AS PROGRAMNAME_RAW,
            COALESCE(
              CASE
                WHEN UPPER(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME))) IN ('', 'NONE', 'NULL', 'NAN', '<NA>') THEN NULL
                ELSE LTRIM(RTRIM(PROGRAM_DISPLAY_NAME))
              END,
              CASE
                WHEN UPPER(LTRIM(RTRIM(PROGRAMNAME))) IN ('', 'NONE', 'NULL', 'NAN', '<NA>') THEN NULL
                ELSE LTRIM(RTRIM(PROGRAMNAME))
              END,
              PROGRAMID
            ) AS PROGRAMNAME,
            PROGRAM_DISPLAY_NAME,
            PROGRAMOWNER,
            PROGRAMFTE,
            PROGRAM_XOM_RATE
        FROM {_fq('PROGRAMS')}
        ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(PROGRAM_DISPLAY_NAME)), ''), PROGRAMNAME)
        """
    )


def list_teams() -> pd.DataFrame:
    return fetch_df(f"""
        SELECT TEAMID,
               COALESCE(
                 CASE
                   WHEN UPPER(LTRIM(RTRIM(TEAMNAME))) IN ('', 'NONE', 'NULL', 'NAN', '<NA>') THEN NULL
                   ELSE LTRIM(RTRIM(TEAMNAME))
                 END,
                 TEAMID
               ) AS TEAMNAME_RAW,
               COALESCE(
                 CASE
                   WHEN UPPER(LTRIM(RTRIM(TEAM_DISPLAY_NAME))) IN ('', 'NONE', 'NULL', 'NAN', '<NA>') THEN NULL
                   ELSE LTRIM(RTRIM(TEAM_DISPLAY_NAME))
                 END,
                 CASE
                   WHEN UPPER(LTRIM(RTRIM(TEAMNAME))) IN ('', 'NONE', 'NULL', 'NAN', '<NA>') THEN NULL
                   ELSE LTRIM(RTRIM(TEAMNAME))
                 END,
                 TEAMID
               ) AS TEAMNAME,
               TEAM_DISPLAY_NAME,
               PROGRAMID,
               PRODUCTOWNER,
               CAST(TEAMFTE AS DECIMAL(18,2)) AS TEAMFTE,
               CAST(DELIVERY_TEAM_FTE AS DECIMAL(18,2)) AS DELIVERY_TEAM_FTE,
               CAST(CONTRACTOR_C_FTE AS DECIMAL(18,2)) AS CONTRACTOR_C_FTE,
               CAST(CONTRACTOR_CS_FTE AS DECIMAL(18,2)) AS CONTRACTOR_CS_FTE
        FROM {_fq('TEAMS')} ORDER BY COALESCE(NULLIF(LTRIM(RTRIM(TEAM_DISPLAY_NAME)), ''), TEAMNAME), TEAMID
    """)


def list_application_groups(team_id: Optional[str] = None) -> pd.DataFrame:
    where = "WHERE g.TEAMID = ?" if team_id else ""
    params: Optional[Tuple[str]] = (team_id,) if team_id else None
    return fetch_df(f"""
        SELECT g.GROUPID, g.GROUPNAME, g.TEAMID,
               COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
               ISNULL(g.PROGRAMID, t.PROGRAMID) AS PROGRAMID,
               COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME,
               g.DEFAULT_VENDORID AS VENDORID, v.VENDORNAME,
               g.OWNER, g.CREATED_AT
        FROM {_fq('APPLICATION_GROUPS')} g
        LEFT JOIN {_fq('TEAMS')}    t ON t.TEAMID = g.TEAMID
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = ISNULL(g.PROGRAMID, t.PROGRAMID)
        LEFT JOIN {_fq('VENDORS')}  v ON v.VENDORID  = g.DEFAULT_VENDORID
        {where}
        ORDER BY g.GROUPNAME
    """, params)


def list_applications(team_id: Optional[str] = None) -> pd.DataFrame:
    where = "WHERE t.TEAMID = ?" if team_id else ""
    params: Optional[Tuple[str]] = (team_id,) if team_id else None
    return fetch_df(f"""
        SELECT a.APPLICATIONID, a.APPLICATIONNAME, a.ADD_INFO,
               a.IS_DEFAULT,
               a.GROUPID, g.GROUPNAME,
               t.TEAMID,
               COALESCE(NULLIF(LTRIM(RTRIM(t.TEAM_DISPLAY_NAME)), ''), t.TEAMNAME) AS TEAMNAME,
               p.PROGRAMID,
               COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME) AS PROGRAMNAME,
               ISNULL(a.VENDORID, g.DEFAULT_VENDORID) AS VENDORID,
               v.VENDORNAME,
               a.UPDATED_AT, a.UPDATED_BY
        FROM {_fq('APPLICATIONS')} a
        LEFT JOIN {_fq('APPLICATION_GROUPS')} g ON g.GROUPID = a.GROUPID
        LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = g.TEAMID
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = ISNULL(g.PROGRAMID, t.PROGRAMID)
        LEFT JOIN {_fq('VENDORS')} v ON v.VENDORID = ISNULL(a.VENDORID, g.DEFAULT_VENDORID)
        {where}
        ORDER BY a.APPLICATIONNAME
    """, params)


def list_program_apptio_workids() -> pd.DataFrame:
    return fetch_df(f"""
        SELECT w.WORK_ID, w.PROGRAMID, p.PROGRAMNAME, w.UPDATED_AT, w.UPDATED_BY
        FROM {_fq('PROGRAM_APPTIO_WORKIDS')} w
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = w.PROGRAMID
        ORDER BY p.PROGRAMNAME, w.WORK_ID
    """)


# Stubs for functions not yet migrated. These will be ported iteratively.
@_versioned_write("PROGRAMS.UPSERT")
def upsert_program(
    program_id: str,
    name: str,
    owner: Optional[str],
    fte: Optional[float],
    program_rate: Optional[float] = None,
    program_display_name: Optional[str] = None,
    updated_by: Optional[str] = None,
) -> None:
    if name and str(name).strip():
        dup = fetch_df(
            f"SELECT PROGRAMID FROM {_fq('PROGRAMS')} WHERE UPPER(PROGRAMNAME)=UPPER(?) AND PROGRAMID <> ?",
            (name.strip(), program_id),
        )
        if not dup.empty:
            raise ValueError(f"Program name '{name.strip()}' already exists.")
    execute(
        f"""
        MERGE INTO {_fq('PROGRAMS')} t
        USING (
            SELECT
                ? AS PROGRAMID,
                ? AS PROGRAMNAME,
                ? AS PROGRAMOWNER,
                ? AS PROGRAMFTE,
                ? AS PROGRAM_XOM_RATE,
                ? AS PROGRAM_DISPLAY_NAME,
                ? AS UPDATED_BY
        ) s
        ON t.PROGRAMID = s.PROGRAMID
        WHEN MATCHED THEN UPDATE SET
            PROGRAMNAME      = s.PROGRAMNAME,
            PROGRAMOWNER     = s.PROGRAMOWNER,
            PROGRAMFTE       = s.PROGRAMFTE,
            /* Legacy PROGRAM_XOM_RATE is preserved when not provided. */
            PROGRAM_XOM_RATE = COALESCE(s.PROGRAM_XOM_RATE, t.PROGRAM_XOM_RATE),
            PROGRAM_DISPLAY_NAME = COALESCE(s.PROGRAM_DISPLAY_NAME, t.PROGRAM_DISPLAY_NAME),
            UPDATED_AT       = SYSDATETIME(),
            UPDATED_BY       = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE, PROGRAM_XOM_RATE, PROGRAM_DISPLAY_NAME, UPDATED_BY)
        VALUES (s.PROGRAMID, s.PROGRAMNAME, s.PROGRAMOWNER, s.PROGRAMFTE, s.PROGRAM_XOM_RATE, s.PROGRAM_DISPLAY_NAME, s.UPDATED_BY);
        """,
        (program_id, name, owner, fte, program_rate, program_display_name, updated_by),
    )


@_versioned_write("TEAMS.UPSERT")
def upsert_team(team_id: str, name: str, program_id: Optional[str],
                team_fte: Optional[float],
                delivery_team_fte: Optional[float] = None,
                contractor_c_fte: Optional[float] = None,
                contractor_cs_fte: Optional[float] = None,
                team_display_name: Optional[str] = None,
                updated_by: Optional[str] = None) -> None:
    if name and str(name).strip():
        dup = fetch_df(
            f"SELECT TEAMID FROM {_fq('TEAMS')} WHERE UPPER(TEAMNAME)=UPPER(?) AND TEAMID <> ?",
            (name.strip(), team_id),
        )
        if not dup.empty:
            raise ValueError(f"Team name '{name.strip()}' already exists.")
    # ensure columns exist
    execute(f"ALTER TABLE {_fq('TEAMS')} ADD COLUMN IF NOT EXISTS DELIVERY_TEAM_FTE DECIMAL(18,2)")
    execute(f"ALTER TABLE {_fq('TEAMS')} ADD COLUMN IF NOT EXISTS CONTRACTOR_C_FTE DECIMAL(18,2)")
    execute(f"ALTER TABLE {_fq('TEAMS')} ADD COLUMN IF NOT EXISTS CONTRACTOR_CS_FTE DECIMAL(18,2)")
    execute(
        f"""
        MERGE INTO {_fq('TEAMS')} t
        USING (
          SELECT ? AS TEAMID, ? AS TEAMNAME, ? AS PROGRAMID,
                 ? AS TEAMFTE, ? AS DELIVERY_TEAM_FTE, ? AS CONTRACTOR_C_FTE, ? AS CONTRACTOR_CS_FTE, ? AS TEAM_DISPLAY_NAME,
                 ? AS UPDATED_BY
        ) s
        ON t.TEAMID = s.TEAMID
        WHEN MATCHED THEN UPDATE SET
          TEAMNAME=s.TEAMNAME, PROGRAMID=s.PROGRAMID, TEAMFTE=s.TEAMFTE,
          DELIVERY_TEAM_FTE=s.DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE=s.CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE=s.CONTRACTOR_CS_FTE,
          TEAM_DISPLAY_NAME=COALESCE(s.TEAM_DISPLAY_NAME, t.TEAM_DISPLAY_NAME),
          UPDATED_AT = SYSDATETIME(), UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (TEAMID, TEAMNAME, PROGRAMID, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE, TEAM_DISPLAY_NAME, UPDATED_BY)
        VALUES (s.TEAMID, s.TEAMNAME, s.PROGRAMID, s.TEAMFTE, s.DELIVERY_TEAM_FTE, s.CONTRACTOR_C_FTE, s.CONTRACTOR_CS_FTE, s.TEAM_DISPLAY_NAME, s.UPDATED_BY);
        """,
        (team_id, name, program_id, team_fte, delivery_team_fte, contractor_c_fte, contractor_cs_fte, team_display_name, updated_by),
    )


@_versioned_write("PROGRAM_APPTIO_WORKIDS.UPSERT")
def upsert_program_apptio_workid(program_id: str, work_id: str, updated_by: Optional[str] = None) -> None:
    work_id = _normalize_work_id(work_id)
    if not work_id:
        raise ValueError("Work ID is required")
    execute(f"""
        MERGE INTO {_fq('PROGRAM_APPTIO_WORKIDS')} t
        USING (SELECT ? AS WORK_ID, ? AS PROGRAMID, ? AS UPDATED_BY) s
          ON t.WORK_ID = s.WORK_ID
        WHEN MATCHED THEN UPDATE SET
          PROGRAMID = s.PROGRAMID,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (WORK_ID, PROGRAMID, UPDATED_AT, UPDATED_BY)
          VALUES (s.WORK_ID, s.PROGRAMID, SYSDATETIME(), s.UPDATED_BY);
    """, (work_id, program_id, updated_by))


@_versioned_write("PROGRAM_APPTIO_WORKIDS.DELETE")
def delete_program_apptio_workid(program_id: str, work_id: str) -> None:
    execute(
        f"DELETE FROM {_fq('PROGRAM_APPTIO_WORKIDS')} WHERE WORK_ID = ? AND PROGRAMID = ?",
        (work_id, program_id),
    )


@_versioned_write("APPTIO_ACTUALS.UPSERT")
def upsert_apptio_actuals(rows: Sequence[Dict[str, Any]]) -> int:
    """Insert or update Apptio monthly actuals keyed by Work ID + FY + month."""
    if not rows:
        return 0
    processed = 0
    for r in rows:
        work_id = _normalize_work_id(r.get("work_id") or "")
        if not work_id:
            continue
        fy = int(r.get("fiscal_year"))
        month = int(r.get("month"))
        amt = float(r.get("amount") or 0)
        source = r.get("source")
        loaded_by = r.get("loaded_by")
        execute(f"""
            MERGE INTO {_fq('APPTIO_ACTUALS')} t
            USING (SELECT ? AS WORK_ID, ? AS FISCAL_YEAR, ? AS MONTH, ? AS AMOUNT, ? AS SOURCE, ? AS LOADED_BY) s
              ON t.WORK_ID = s.WORK_ID AND t.FISCAL_YEAR = s.FISCAL_YEAR AND t.MONTH = s.MONTH
            WHEN MATCHED THEN UPDATE SET
              AMOUNT = s.AMOUNT,
              SOURCE = s.SOURCE,
              LOADED_AT = SYSDATETIME(),
              LOADED_BY = COALESCE(s.LOADED_BY, t.LOADED_BY)
            WHEN NOT MATCHED THEN INSERT (WORK_ID, FISCAL_YEAR, MONTH, AMOUNT, SOURCE, LOADED_BY, LOADED_AT)
              VALUES (s.WORK_ID, s.FISCAL_YEAR, s.MONTH, s.AMOUNT, s.SOURCE, s.LOADED_BY, SYSDATETIME());
        """, (work_id, fy, month, amt, source, loaded_by))
        processed += 1
    return processed


@_versioned_write("APPTIO_ACTUALS_LINES.UPSERT")
def upsert_apptio_actuals_lines(rows: Sequence[Dict[str, Any]], updated_by: Optional[str] = None, source: Optional[str] = None) -> int:
    """Upsert enriched Apptio line items keyed by (WORK_ID,FISCAL_YEAR,MONTH,PRODUCT_ID,LEDGER_ACCOUNT_L3_DESC)."""
    if not rows:
        return 0
    processed = 0
    for r in rows:
        work_id = _normalize_work_id(r.get("work_id") or "")
        if not work_id:
            continue
        fy = int(r.get("fiscal_year"))
        month = int(r.get("month"))
        amt = float(r.get("amount") or 0.0)
        ledger_desc = r.get("ledger_account_l3_desc")
        product_id = r.get("product_id")
        product_name = r.get("product_name")
        execute(
            f"""
            MERGE INTO {_fq('APPTIO_ACTUALS_LINES')} t
            USING (
              SELECT
                ? AS WORK_ID,
                ? AS FISCAL_YEAR,
                ? AS MONTH,
                ? AS AMOUNT,
                ? AS LEDGER_ACCOUNT_L3_DESC,
                ? AS PRODUCT_ID,
                ? AS PRODUCT_NAME,
                ? AS SOURCE,
                ? AS LOADED_BY
            ) s
              ON t.WORK_ID = s.WORK_ID
             AND t.FISCAL_YEAR = s.FISCAL_YEAR
             AND t.MONTH = s.MONTH
             AND ISNULL(t.PRODUCT_ID, '') = ISNULL(s.PRODUCT_ID, '')
             AND ISNULL(t.LEDGER_ACCOUNT_L3_DESC, '') = ISNULL(s.LEDGER_ACCOUNT_L3_DESC, '')
            WHEN MATCHED THEN UPDATE SET
              AMOUNT = s.AMOUNT,
              PRODUCT_NAME = s.PRODUCT_NAME,
              SOURCE = s.SOURCE,
              LOADED_AT = SYSDATETIME(),
              LOADED_BY = COALESCE(s.LOADED_BY, t.LOADED_BY)
            WHEN NOT MATCHED THEN INSERT (
              WORK_ID, FISCAL_YEAR, MONTH, AMOUNT,
              LEDGER_ACCOUNT_L3_DESC, PRODUCT_ID, PRODUCT_NAME,
              SOURCE, LOADED_AT, LOADED_BY
            ) VALUES (
              s.WORK_ID, s.FISCAL_YEAR, s.MONTH, s.AMOUNT,
              s.LEDGER_ACCOUNT_L3_DESC, s.PRODUCT_ID, s.PRODUCT_NAME,
              s.SOURCE, SYSDATETIME(), s.LOADED_BY
            );
            """,
            (
                work_id,
                fy,
                month,
                amt,
                (str(ledger_desc).strip() if ledger_desc is not None and str(ledger_desc).strip() else None),
                (str(product_id).strip() if product_id is not None and str(product_id).strip() else None),
                (str(product_name).strip() if product_name is not None and str(product_name).strip() else None),
                (str(source).strip() if source is not None and str(source).strip() else None),
                (str(updated_by).strip() if updated_by is not None and str(updated_by).strip() else None),
            ),
        )
        processed += 1
    return processed


@_versioned_write("MAP_APPTIO_TO_COST_TYPE.UPSERT")
def upsert_apptio_to_cost_type_mappings(rows: Sequence[Dict[str, Any]], updated_by: Optional[str] = None) -> int:
    """Upsert rows into MAP_APPTIO_TO_COST_TYPE (identified by MAP_ID)."""
    if not rows:
        return 0
    processed = 0
    import uuid

    for r in rows:
        map_id = r.get("map_id") or r.get("MAP_ID")
        if not map_id:
            map_id = str(uuid.uuid4())
        fiscal_year = r.get("fiscal_year") if r.get("fiscal_year") is not None else r.get("FISCAL_YEAR")
        fiscal_year_val = int(fiscal_year) if str(fiscal_year).strip().isdigit() else None
        product_id = r.get("product_id") if r.get("product_id") is not None else r.get("PRODUCT_ID")
        ledger_contains = r.get("ledger_account_contains") if r.get("ledger_account_contains") is not None else r.get("LEDGER_ACCOUNT_CONTAINS")
        cost_type = r.get("cost_type") if r.get("cost_type") is not None else r.get("COST_TYPE")
        subtype = r.get("subtype") if r.get("subtype") is not None else r.get("SUBTYPE")
        is_active = r.get("is_active") if r.get("is_active") is not None else r.get("IS_ACTIVE")
        notes = r.get("notes") if r.get("notes") is not None else r.get("NOTES")
        if not str(cost_type or "").strip():
            continue
        execute(
            f"""
            MERGE INTO {_fq('MAP_APPTIO_TO_COST_TYPE')} t
            USING (
              SELECT
                TRY_CONVERT(UNIQUEIDENTIFIER, ?) AS MAP_ID,
                ? AS FISCAL_YEAR,
                ? AS PRODUCT_ID,
                ? AS LEDGER_ACCOUNT_CONTAINS,
                ? AS COST_TYPE,
                ? AS SUBTYPE,
                ? AS IS_ACTIVE,
                ? AS NOTES,
                ? AS UPDATED_BY
            ) s
              ON t.MAP_ID = s.MAP_ID
            WHEN MATCHED THEN UPDATE SET
              FISCAL_YEAR = s.FISCAL_YEAR,
              PRODUCT_ID = s.PRODUCT_ID,
              LEDGER_ACCOUNT_CONTAINS = s.LEDGER_ACCOUNT_CONTAINS,
              COST_TYPE = s.COST_TYPE,
              SUBTYPE = s.SUBTYPE,
              IS_ACTIVE = COALESCE(s.IS_ACTIVE, t.IS_ACTIVE),
              NOTES = s.NOTES,
              UPDATED_AT = SYSDATETIME(),
              UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
            WHEN NOT MATCHED THEN INSERT (
              MAP_ID, FISCAL_YEAR, PRODUCT_ID, LEDGER_ACCOUNT_CONTAINS, COST_TYPE, SUBTYPE,
              IS_ACTIVE, NOTES, UPDATED_AT, UPDATED_BY
            ) VALUES (
              TRY_CONVERT(UNIQUEIDENTIFIER, ?), ?, ?, ?, ?, ?, COALESCE(?, 1), ?, SYSDATETIME(), ?
            );
            """,
            (
                str(map_id),
                fiscal_year_val,
                (str(product_id).strip() if product_id is not None and str(product_id).strip() else None),
                (str(ledger_contains).strip() if ledger_contains is not None and str(ledger_contains).strip() else None),
                str(cost_type).strip(),
                (str(subtype).strip() if subtype is not None else None),
                (1 if bool(is_active) else 0) if is_active is not None else None,
                (str(notes).strip() if notes is not None and str(notes).strip() else None),
                (str(updated_by).strip() if updated_by is not None and str(updated_by).strip() else None),
                str(map_id),
                fiscal_year_val,
                (str(product_id).strip() if product_id is not None and str(product_id).strip() else None),
                (str(ledger_contains).strip() if ledger_contains is not None and str(ledger_contains).strip() else None),
                str(cost_type).strip(),
                (str(subtype).strip() if subtype is not None else None),
                (1 if bool(is_active) else 0) if is_active is not None else None,
                (str(notes).strip() if notes is not None and str(notes).strip() else None),
                (str(updated_by).strip() if updated_by is not None and str(updated_by).strip() else None),
            ),
        )
        processed += 1
    return processed


def fetch_apptio_actuals_by_program(fiscal_year: Optional[int] = None, program_ids: Optional[Sequence[str]] = None) -> pd.DataFrame:
    where = []
    params: list[Any] = []
    if fiscal_year:
        where.append("a.FISCAL_YEAR = ?")
        params.append(int(fiscal_year))
    if program_ids:
        placeholders = ", ".join(["?"] * len(program_ids))
        where.append(f"w.PROGRAMID IN ({placeholders})")
        params.extend(program_ids)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    return fetch_df(f"""
        SELECT
          w.PROGRAMID,
          p.PROGRAMNAME,
          a.FISCAL_YEAR,
          a.MONTH,
          SUM(a.AMOUNT) AS AMOUNT
        FROM {_fq('APPTIO_ACTUALS')} a
        JOIN {_fq('PROGRAM_APPTIO_WORKIDS')} w ON w.WORK_ID = a.WORK_ID
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = w.PROGRAMID
        {where_sql}
        GROUP BY w.PROGRAMID, p.PROGRAMNAME, a.FISCAL_YEAR, a.MONTH
        ORDER BY a.FISCAL_YEAR, a.MONTH, p.PROGRAMNAME
    """, tuple(params) if params else None)


def fetch_apptio_actuals_by_program_breakdown(
    fiscal_year: Optional[int] = None,
    program_ids: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Return Apptio actuals enriched with COST_TYPE/SUBTYPE mapping (monthly)."""
    where: list[str] = ["1=1"]
    params: list[Any] = []
    if fiscal_year:
        where.append("a.FISCAL_YEAR = ?")
        params.append(int(fiscal_year))
    if program_ids:
        placeholders = ", ".join(["?"] * len(program_ids))
        where.append(f"w.PROGRAMID IN ({placeholders})")
        params.extend(program_ids)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    return fetch_df(
        f"""
        WITH AllowedPairs AS (
          SELECT 'Invoices' AS COST_TYPE, '' AS SUBTYPE
          UNION ALL SELECT 'Cloud', 'AWS'
          UNION ALL SELECT 'Cloud', 'Azure'
          UNION ALL SELECT 'Contractor CS', ''
          UNION ALL SELECT 'MSP', ''
          UNION ALL SELECT 'Travel', ''
          UNION ALL SELECT 'Infra', ''
          UNION ALL SELECT 'NWF', 'Other'
        ),
        base AS (
          SELECT
            w.PROGRAMID,
            p.PROGRAMNAME,
            TRY_CONVERT(INT, a.FISCAL_YEAR) AS YEAR,
            CONCAT(
              TRY_CONVERT(INT, a.FISCAL_YEAR),
              '-',
              RIGHT('0' + CONVERT(VARCHAR(2), TRY_CONVERT(INT, a.MONTH)), 2)
            ) AS MONTH_KEY,
            map2.COST_TYPE AS MAP_COST_TYPE,
            COALESCE(map2.SUBTYPE, '') AS MAP_SUBTYPE,
            COALESCE(map2.STATUS, '') AS MAP_STATUS,
            CAST(SUM(COALESCE(TRY_CONVERT(FLOAT, a.AMOUNT), 0.0)) AS DECIMAL(18,2)) AS AMOUNT
          FROM {_fq('APPTIO_ACTUALS_LINES')} a
          JOIN {_fq('PROGRAM_APPTIO_WORKIDS')} w ON w.WORK_ID = a.WORK_ID
          LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = w.PROGRAMID
          OUTER APPLY (
            SELECT TOP 1
              LTRIM(RTRIM(m.COST_TYPE)) AS COST_TYPE,
              COALESCE(LTRIM(RTRIM(m.SUBTYPE)), '') AS SUBTYPE
            FROM {_fq('MAP_APPTIO_TO_COST_TYPE')} m
            WHERE m.IS_ACTIVE = 1
              AND (m.FISCAL_YEAR IS NULL OR m.FISCAL_YEAR = a.FISCAL_YEAR)
              AND m.PRODUCT_ID IS NOT NULL
              AND LTRIM(RTRIM(m.PRODUCT_ID)) <> ''
              AND LTRIM(RTRIM(m.PRODUCT_ID)) = LTRIM(RTRIM(COALESCE(a.PRODUCT_ID, '')))
              AND m.COST_TYPE IS NOT NULL
              AND LTRIM(RTRIM(m.COST_TYPE)) <> ''
              AND EXISTS (
                SELECT 1
                FROM AllowedPairs ap2
                WHERE ap2.COST_TYPE = LTRIM(RTRIM(m.COST_TYPE))
                  AND ap2.SUBTYPE = COALESCE(LTRIM(RTRIM(m.SUBTYPE)), '')
              )
              AND NOT (
                UPPER(LTRIM(RTRIM(m.COST_TYPE))) = 'NWF'
                AND UPPER(COALESCE(NULLIF(LTRIM(RTRIM(m.SUBTYPE)), ''), 'Other')) = 'OTHER'
              )
            ORDER BY
              CASE WHEN m.FISCAL_YEAR IS NULL THEN 1 ELSE 0 END ASC,
              LEN(COALESCE(m.LEDGER_ACCOUNT_CONTAINS, '')) DESC
          ) product_map
          OUTER APPLY (
            SELECT TOP 1
              LTRIM(RTRIM(m.COST_TYPE)) AS COST_TYPE,
              COALESCE(LTRIM(RTRIM(m.SUBTYPE)), '') AS SUBTYPE
            FROM {_fq('MAP_APPTIO_TO_COST_TYPE')} m
            WHERE m.IS_ACTIVE = 1
              AND (m.FISCAL_YEAR IS NULL OR m.FISCAL_YEAR = a.FISCAL_YEAR)
              AND m.PRODUCT_ID IS NULL
              AND m.LEDGER_ACCOUNT_CONTAINS IS NOT NULL
              AND LTRIM(RTRIM(m.LEDGER_ACCOUNT_CONTAINS)) <> ''
              AND UPPER(COALESCE(a.LEDGER_ACCOUNT_L3_DESC, '')) LIKE '%' + UPPER(m.LEDGER_ACCOUNT_CONTAINS) + '%'
            ORDER BY
              CASE WHEN m.FISCAL_YEAR IS NULL THEN 1 ELSE 0 END ASC,
              LEN(COALESCE(m.LEDGER_ACCOUNT_CONTAINS, '')) DESC
          ) ledger_map
          OUTER APPLY (
            SELECT
              COALESCE(product_map.COST_TYPE, ledger_map.COST_TYPE) AS COST_TYPE,
              COALESCE(product_map.SUBTYPE, ledger_map.SUBTYPE, '') AS SUBTYPE,
              CASE
                WHEN product_map.COST_TYPE IS NOT NULL THEN 'MAPPED_PRODUCT'
                WHEN ledger_map.COST_TYPE IS NOT NULL THEN 'MAPPED_LEDGER'
                ELSE ''
              END AS STATUS
          ) map2
          {where_sql}
          GROUP BY
            w.PROGRAMID,
            p.PROGRAMNAME,
            TRY_CONVERT(INT, a.FISCAL_YEAR),
            TRY_CONVERT(INT, a.MONTH),
            map2.COST_TYPE,
            COALESCE(map2.SUBTYPE, ''),
            COALESCE(map2.STATUS, '')
        )
        SELECT
          b.PROGRAMID,
          b.PROGRAMNAME,
          b.YEAR,
          b.MONTH_KEY,
          CASE
            WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
            WHEN ap.COST_TYPE IS NULL THEN 'NWF'
            ELSE b.MAP_COST_TYPE
          END AS COST_TYPE,
          CASE
            WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'Other'
            WHEN ap.COST_TYPE IS NULL THEN 'Other'
            ELSE b.MAP_SUBTYPE
          END AS SUBTYPE,
          CASE
            WHEN (
              CASE
                WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
                WHEN ap.COST_TYPE IS NULL THEN 'NWF'
                ELSE b.MAP_COST_TYPE
              END
            ) = 'Cloud'
             AND (
              CASE
                WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'Other'
                WHEN ap.COST_TYPE IS NULL THEN 'Other'
                ELSE b.MAP_SUBTYPE
              END
            ) = 'AWS'
              THEN 'Cloud AWS'
            WHEN (
              CASE
                WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
                WHEN ap.COST_TYPE IS NULL THEN 'NWF'
                ELSE b.MAP_COST_TYPE
              END
            ) = 'Cloud'
             AND (
              CASE
                WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'Other'
                WHEN ap.COST_TYPE IS NULL THEN 'Other'
                ELSE b.MAP_SUBTYPE
              END
            ) = 'Azure'
              THEN 'Cloud Azure'
            WHEN (
              CASE
                WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
                WHEN ap.COST_TYPE IS NULL THEN 'NWF'
                ELSE b.MAP_COST_TYPE
              END
            ) IN ('Invoices','Contractor CS','MSP','Travel','Infra')
              THEN (
                CASE
                  WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
                  WHEN ap.COST_TYPE IS NULL THEN 'NWF'
                  ELSE b.MAP_COST_TYPE
                END
              )
            WHEN (
              CASE
                WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
                WHEN ap.COST_TYPE IS NULL THEN 'NWF'
                ELSE b.MAP_COST_TYPE
              END
            ) = 'NWF'
             AND (
              CASE
                WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'Other'
                WHEN ap.COST_TYPE IS NULL THEN 'Other'
                ELSE b.MAP_SUBTYPE
              END
            ) IN ('Other','')
              THEN 'NWF Other'
            ELSE 'NWF Other'
          END AS EFFECTIVE_NWF_TYPE,
          CASE
            WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'DEFAULT_OTHER'
            WHEN ap.COST_TYPE IS NULL THEN 'INVALID_TARGET'
            ELSE b.MAP_STATUS
          END AS MAPPING_STATUS,
          b.AMOUNT
        FROM base b
        LEFT JOIN AllowedPairs ap
          ON ap.COST_TYPE = b.MAP_COST_TYPE
         AND ap.SUBTYPE = b.MAP_SUBTYPE
        ORDER BY b.YEAR, b.MONTH_KEY, b.PROGRAMNAME, COST_TYPE, SUBTYPE
        """,
        tuple(params) if params else None,
    )


@_versioned_write("VENDORS.UPSERT")
def upsert_vendor(vendor_id: str, vendor_name: str, updated_by: Optional[str] = None) -> None:
    if vendor_name and str(vendor_name).strip():
        dup = fetch_df(
            f"SELECT VENDORID FROM {_fq('VENDORS')} WHERE UPPER(VENDORNAME)=UPPER(?) AND VENDORID <> ?",
            (vendor_name.strip(), vendor_id),
        )
        if not dup.empty:
            raise ValueError(f"Vendor name '{vendor_name.strip()}' already exists.")
    execute(
        f"""
        MERGE INTO {_fq('VENDORS')} t
        USING (SELECT ? AS VENDORID, ? AS VENDORNAME, ? AS UPDATED_BY) s
        ON t.VENDORID = s.VENDORID
        WHEN MATCHED THEN UPDATE SET VENDORNAME=s.VENDORNAME, UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (VENDORID, VENDORNAME, UPDATED_BY) VALUES (s.VENDORID, s.VENDORNAME, s.UPDATED_BY);
        """,
        (vendor_id, vendor_name, updated_by),
    )


@_versioned_write("APPLICATION_GROUPS.UPSERT")
def upsert_application_group(group_id: str, group_name: str, team_id: str,
                             default_vendor_id: Optional[str], owner: Optional[str],
                             is_base: bool = False,
                             updated_by: Optional[str] = None) -> None:
    # Allow callers (e.g., Settings / ADO discovery flows) to create a group without assigning a Team yet.
    team_id_norm: Optional[str] = (str(team_id).strip() if team_id is not None else None)  # type: ignore[redundant-expr]
    if team_id_norm == "":
        team_id_norm = None
    execute(
        f"""
        MERGE INTO {_fq('APPLICATION_GROUPS')} t
        USING (SELECT ? AS GROUPID, ? AS GROUPNAME, ? AS TEAMID, ? AS DEFAULT_VENDORID, ? AS OWNER, ? AS IS_BASE, ? AS UPDATED_BY) s
        ON t.GROUPID = s.GROUPID
        WHEN MATCHED THEN UPDATE SET GROUPNAME=s.GROUPNAME, TEAMID=s.TEAMID, DEFAULT_VENDORID=s.DEFAULT_VENDORID, OWNER=s.OWNER,
                                    IS_BASE=s.IS_BASE,
                                    UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (GROUPID, GROUPNAME, TEAMID, DEFAULT_VENDORID, OWNER, IS_BASE, UPDATED_BY)
        VALUES (s.GROUPID, s.GROUPNAME, s.TEAMID, s.DEFAULT_VENDORID, s.OWNER, s.IS_BASE, s.UPDATED_BY);
        """,
        (group_id, group_name, team_id_norm, default_vendor_id, owner, 1 if is_base else 0, updated_by),
    )
    # Keep PROGRAMID in sync with team
    if team_id_norm:
        execute(
            f"""
            UPDATE g SET PROGRAMID = t.PROGRAMID
            FROM {_fq('APPLICATION_GROUPS')} g
            INNER JOIN {_fq('TEAMS')} t ON t.TEAMID = g.TEAMID
            WHERE g.GROUPID = ? AND (g.PROGRAMID IS NULL OR g.PROGRAMID <> t.PROGRAMID)
            """,
            (group_id,),
        )

    # Phase 2: default instance rule (PO burden removal)
    # If a group has no Application Instances yet, create a deterministic default instance:
    #   APPLICATIONID = "<GROUPID>__DEFAULT", IS_DEFAULT = 1
    try:
        any_app = fetch_df(f"SELECT TOP 1 APPLICATIONID FROM {_fq('APPLICATIONS')} WHERE GROUPID = ?", (group_id,))
        if any_app is None or any_app.empty:
            default_app_id = f"{group_id}__DEFAULT"
            base_name = (group_name or group_id).strip()
            default_app_name = (f"{base_name} (Default)")[:255]
            execute(
                f"""
                MERGE INTO {_fq('APPLICATIONS')} t
                USING (SELECT ? AS APPLICATIONID, ? AS APPLICATIONNAME, ? AS GROUPID, ? AS VENDORID, ? AS UPDATED_BY) s
                ON t.APPLICATIONID = s.APPLICATIONID
                WHEN MATCHED THEN UPDATE SET
                  GROUPID = s.GROUPID,
                  APPLICATIONNAME = COALESCE(NULLIF(s.APPLICATIONNAME, ''), t.APPLICATIONNAME),
                  VENDORID = COALESCE(NULLIF(s.VENDORID, ''), t.VENDORID),
                  IS_DEFAULT = 1,
                  UPDATED_AT = SYSDATETIME(),
                  UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
                WHEN NOT MATCHED THEN INSERT (APPLICATIONID, APPLICATIONNAME, GROUPID, VENDORID, IS_DEFAULT, UPDATED_BY)
                  VALUES (s.APPLICATIONID, s.APPLICATIONNAME, s.GROUPID, s.VENDORID, 1, s.UPDATED_BY);
                """,
                (default_app_id, default_app_name, group_id, default_vendor_id, updated_by),
            )
    except Exception:
        pass


@_versioned_write("APPLICATIONS.UPSERT")
def upsert_application_instance(application_id: str, group_id: str,
                                application_name: str, add_info: Optional[str],
                                vendor_id: Optional[str], updated_by: Optional[str] = None):
    # Resolve vendor via default if not provided
    vend_df = fetch_df(f"SELECT DEFAULT_VENDORID FROM {_fq('APPLICATION_GROUPS')} WHERE GROUPID = ?", (group_id,))
    resolved_vendor = vendor_id if vendor_id else (vend_df.iloc[0]["DEFAULT_VENDORID"] if vend_df is not None and not vend_df.empty else None)
    execute(
        f"""
        MERGE INTO {_fq('APPLICATIONS')} t
        USING (SELECT ? AS APPLICATIONID, ? AS GROUPID, ? AS APPLICATIONNAME, ? AS ADD_INFO, ? AS VENDORID, ? AS UPDATED_BY) s
        ON t.APPLICATIONID = s.APPLICATIONID
        WHEN MATCHED THEN UPDATE SET GROUPID=s.GROUPID, APPLICATIONNAME=s.APPLICATIONNAME, ADD_INFO=s.ADD_INFO, VENDORID=s.VENDORID
                                      , UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY)
        WHEN NOT MATCHED THEN INSERT (APPLICATIONID, GROUPID, APPLICATIONNAME, ADD_INFO, VENDORID, UPDATED_BY)
        VALUES (s.APPLICATIONID, s.GROUPID, s.APPLICATIONNAME, s.ADD_INFO, s.VENDORID, s.UPDATED_BY);
        """,
        (application_id, group_id, application_name, add_info, resolved_vendor, updated_by),
    )


@_versioned_write("INVOICES.UPSERT")
def upsert_invoice(
    invoice_id,
    application_id,
    team_id,
    renewal_date,
    amount,
    status,
    fiscal_year,
    product_owner,
    amount_next_year,
    contract_active,
    company_code,
    cost_center,
    serial_number,
    work_order,
    agreement_number,
    contract_due,
    service_type,
    notes,
    group_id,
    programid_at_booking,
    vendorid_at_booking,
    groupid_at_booking,
    rollover_batch_id,
    rolled_over_from_year,
    invoice_type,
    updated_by=None,
):
    sql = f"""
    MERGE INTO { _fq('INVOICES') } t
    USING (
      SELECT
        %(invoice_id)s INVOICEID,
        %(application_id)s APPLICATIONID,
        %(team_id)s TEAMID,
        %(renewal_date)s RENEWALDATE,
        %(amount)s AMOUNT,
        %(status)s STATUS,
        %(fiscal_year)s FISCAL_YEAR,
        %(product_owner)s PRODUCT_OWNER,
        %(amount_next_year)s AMOUNT_NEXT_YEAR,
        %(contract_active)s CONTRACT_ACTIVE,
        %(company_code)s COMPANY_CODE,
        %(cost_center)s COST_CENTER,
        %(serial_number)s SERIAL_NUMBER,
        %(work_order)s WORK_ORDER,
        %(agreement_number)s AGREEMENT_NUMBER,
        %(contract_due)s CONTRACT_DUE,
        %(service_type)s SERVICE_TYPE,
        %(notes)s NOTES,
        %(group_id)s GROUPID,
        %(programid_at_booking)s PROGRAMID_AT_BOOKING,
        %(vendorid_at_booking)s  VENDORID_AT_BOOKING,
        %(groupid_at_booking)s   GROUPID_AT_BOOKING,
        %(rollover_batch_id)s    ROLLOVER_BATCH_ID,
        %(rolled_over_from_year)s ROLLED_OVER_FROM_YEAR,
        %(invoice_type)s INVOICE_TYPE,
        %(updated_by)s UPDATED_BY
    ) s
    ON (
      t.APPLICATIONID = s.APPLICATIONID AND
      t.TEAMID = s.TEAMID AND
      t.FISCAL_YEAR = s.FISCAL_YEAR AND
      ISNULL(t.INVOICE_TYPE, 'Recurring Invoice') = ISNULL(s.INVOICE_TYPE, 'Recurring Invoice')
    )
    WHEN MATCHED THEN UPDATE SET
      APPLICATIONID=s.APPLICATIONID, TEAMID=s.TEAMID, RENEWALDATE=s.RENEWALDATE, AMOUNT=s.AMOUNT, STATUS=s.STATUS,
      FISCAL_YEAR=s.FISCAL_YEAR, PRODUCT_OWNER=s.PRODUCT_OWNER, AMOUNT_NEXT_YEAR=s.AMOUNT_NEXT_YEAR,
      CONTRACT_ACTIVE=s.CONTRACT_ACTIVE, COMPANY_CODE=s.COMPANY_CODE, COST_CENTER=s.COST_CENTER,
      SERIAL_NUMBER=s.SERIAL_NUMBER, WORK_ORDER=s.WORK_ORDER, AGREEMENT_NUMBER=s.AGREEMENT_NUMBER,
      CONTRACT_DUE=s.CONTRACT_DUE, SERVICE_TYPE=s.SERVICE_TYPE, NOTES=s.NOTES,
      GROUPID=s.GROUPID, PROGRAMID_AT_BOOKING=s.PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING=s.VENDORID_AT_BOOKING,
      GROUPID_AT_BOOKING=s.GROUPID_AT_BOOKING, ROLLOVER_BATCH_ID=s.ROLLOVER_BATCH_ID,
      ROLLED_OVER_FROM_YEAR=s.ROLLED_OVER_FROM_YEAR, INVOICE_TYPE=s.INVOICE_TYPE,
      UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY)
    WHEN NOT MATCHED THEN INSERT (
      INVOICEID, APPLICATIONID, TEAMID, RENEWALDATE, AMOUNT, STATUS, FISCAL_YEAR, PRODUCT_OWNER,
      AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE, COMPANY_CODE, COST_CENTER, SERIAL_NUMBER, WORK_ORDER,
      AGREEMENT_NUMBER, CONTRACT_DUE, SERVICE_TYPE, NOTES, GROUPID, PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING,
      GROUPID_AT_BOOKING, ROLLOVER_BATCH_ID, ROLLED_OVER_FROM_YEAR, INVOICE_TYPE, UPDATED_BY
    )
    VALUES (
      s.INVOICEID, s.APPLICATIONID, s.TEAMID, s.RENEWALDATE, s.AMOUNT, s.STATUS, s.FISCAL_YEAR, s.PRODUCT_OWNER,
      s.AMOUNT_NEXT_YEAR, s.CONTRACT_ACTIVE, s.COMPANY_CODE, s.COST_CENTER, s.SERIAL_NUMBER, s.WORK_ORDER,
      s.AGREEMENT_NUMBER, s.CONTRACT_DUE, s.SERVICE_TYPE, s.NOTES, s.GROUPID, s.PROGRAMID_AT_BOOKING, s.VENDORID_AT_BOOKING,
      s.GROUPID_AT_BOOKING, s.ROLLOVER_BATCH_ID, s.ROLLED_OVER_FROM_YEAR, s.INVOICE_TYPE, s.UPDATED_BY
    );
    """
    params = {
        "invoice_id": invoice_id,
        "application_id": application_id,
        "team_id": team_id,
        "renewal_date": renewal_date,
        "amount": amount,
        "status": status,
        "fiscal_year": fiscal_year,
        "product_owner": product_owner,
        "amount_next_year": amount_next_year,
        "contract_active": contract_active,
        "company_code": company_code,
        "cost_center": cost_center,
        "serial_number": serial_number,
        "work_order": work_order,
        "agreement_number": agreement_number,
        "contract_due": contract_due,
        "service_type": service_type,
        "notes": notes,
        "group_id": group_id,
        "programid_at_booking": programid_at_booking,
        "vendorid_at_booking": vendorid_at_booking,
        "groupid_at_booking": groupid_at_booking,
        "rollover_batch_id": rollover_batch_id,
        "rolled_over_from_year": rolled_over_from_year,
        "invoice_type": invoice_type,
        "updated_by": updated_by,
    }
    execute(sql, params)


def _roadmap_capacity_fraction() -> float:
    """Fraction of PI working days available for planned roadmap work (Derived mode only).

    Reads `model.roadmap_capacity_fraction` from Streamlit secrets (preferred) or
    `TCO_ROADMAP_CAPACITY_FRACTION` env var. Clamped to [0.3, 1.0].
    """
    raw: Any = None
    try:
        secrets_obj = getattr(_st, "secrets", None)
        if secrets_obj:
            try:
                cfg = secrets_obj.get("model", {})  # type: ignore[attr-defined]
            except Exception:
                cfg = secrets_obj["model"] if "model" in secrets_obj else {}  # type: ignore[index,operator]
            try:
                raw = cfg.get("roadmap_capacity_fraction", None)  # type: ignore[attr-defined]
            except Exception:
                raw = None
    except Exception:
        raw = None
    if raw is None or str(raw).strip() == "":
        raw = os.getenv("TCO_ROADMAP_CAPACITY_FRACTION")
    try:
        val = float(raw)
    except Exception:
        val = 0.8
    if not (val == val) or val in (float("inf"), float("-inf")):
        val = 0.8
    return max(0.3, min(1.0, float(val)))


def ensure_tco_team_velocity_baseline_view(*args, **kwargs) -> bool:
    """Ensure the Team/PI velocity baseline view used by Derived FTE conversion."""
    ensure_ado_minimal_tables()
    ensure_ado_iteration_calendar_table()
    ensure_team_msp_rate_table()
    _create_or_alter_view(
        "VW_TCO_TEAM_VELOCITY_BASELINE",
        f"""
        WITH done_seed AS (
          SELECT
            af.FEATURE_ID,
            TRY_CONVERT(FLOAT, af.STORY_POINTS) AS SWAG_POINTS,
            TRY_CONVERT(DATETIME2, af.CHANGED_AT) AS CHANGED_AT,
            af.TEAM_RAW,
            af.TEAM_VARIANT_KEY,
            af.ITERATION_PATH,
            af.ITERATION_SK,
            af.ADO_YEAR,
            af.PI_LABEL,
            af.ITERATION_LEVEL3
          FROM {_svq('ADO_FEATURES')} af
          WHERE UPPER(LTRIM(RTRIM(COALESCE(af.STATE, '')))) IN ('CLOSED', 'RESOLVED')
            AND UPPER(LTRIM(RTRIM(COALESCE(af.STATE, '')))) <> 'REMOVED'
            AND TRY_CONVERT(DATETIME2, af.CHANGED_AT) <= DATEADD(DAY, -7, SYSUTCDATETIME())
            AND TRY_CONVERT(FLOAT, af.STORY_POINTS) BETWEEN 5 AND 120
        ),
        f AS (
          SELECT
            ds.FEATURE_ID,
            ds.SWAG_POINTS,
            ds.TEAM_RAW,
            ds.TEAM_VARIANT_KEY,
            COALESCE(
              TRY_CONVERT(INT, LEFT(ls.LABEL_SOURCE, 4)),
              TRY_CONVERT(INT, ic_sk.YEAR),
              TRY_CONVERT(INT, ic_path.YEAR),
              TRY_CONVERT(INT, ds.ADO_YEAR)
            ) AS YEAR,
            CASE
              WHEN ls.LABEL_SOURCE_NORM IS NULL THEN NULL
              WHEN CHARINDEX('PI', ls.LABEL_SOURCE_NORM) > 0
                THEN TRY_CONVERT(INT, SUBSTRING(ls.LABEL_SOURCE_NORM, CHARINDEX('PI', ls.LABEL_SOURCE_NORM) + 2, 10))
              WHEN PATINDEX('% I[0-9]%', ls.LABEL_SOURCE_NORM) > 0
                THEN TRY_CONVERT(INT, SUBSTRING(ls.LABEL_SOURCE_NORM, PATINDEX('% I[0-9]%', ls.LABEL_SOURCE_NORM) + 2, 10))
              WHEN PATINDEX('I[0-9]%', ls.LABEL_SOURCE_NORM) > 0
                THEN TRY_CONVERT(INT, SUBSTRING(ls.LABEL_SOURCE_NORM, PATINDEX('I[0-9]%', ls.LABEL_SOURCE_NORM) + 1, 10))
              ELSE NULL
            END AS PI,
            ls.LABEL_SOURCE AS PI_LABEL
          FROM done_seed ds
          OUTER APPLY (
            SELECT
              CASE
                WHEN ds.ITERATION_PATH IS NULL OR LTRIM(RTRIM(ds.ITERATION_PATH)) = '' THEN NULL
                WHEN (
                  PATINDEX('%\\I[0-9] S[0-9]%', UPPER(ds.ITERATION_PATH)) > 0
                  OR PATINDEX('%\\S[0-9]%', UPPER(ds.ITERATION_PATH)) > 0
                )
                AND CHARINDEX('\\', ds.ITERATION_PATH) > 0
                THEN LEFT(ds.ITERATION_PATH, LEN(ds.ITERATION_PATH) - CHARINDEX('\\', REVERSE(ds.ITERATION_PATH)))
                ELSE ds.ITERATION_PATH
              END AS FEATURE_PI_PARENT_ITERATION_PATH
          ) fp
          LEFT JOIN {_svq('ADO_ITERATION_CALENDAR')} ic_sk
            ON UPPER(LTRIM(RTRIM(ic_sk.ITERATION_SK))) = UPPER(LTRIM(RTRIM(NULLIF(LTRIM(RTRIM(ds.ITERATION_SK)), ''))))
           AND UPPER(LTRIM(RTRIM(ic_sk.ITERATION_GRAIN))) = 'PI'
          LEFT JOIN {_svq('ADO_ITERATION_CALENDAR')} ic_path
            ON UPPER(LTRIM(RTRIM(ic_path.ITERATION_PATH))) = UPPER(LTRIM(RTRIM(fp.FEATURE_PI_PARENT_ITERATION_PATH)))
           AND UPPER(LTRIM(RTRIM(ic_path.ITERATION_GRAIN))) = 'PI'
          OUTER APPLY (
            SELECT
              COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3, ds.PI_LABEL, ds.ITERATION_LEVEL3) AS LABEL_SOURCE,
              UPPER(LTRIM(RTRIM(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3, ds.PI_LABEL, ds.ITERATION_LEVEL3)))) AS LABEL_SOURCE_NORM
          ) ls
        ),
        mapped AS (
          SELECT
            f.FEATURE_ID,
            f.YEAR,
            f.PI,
            f.PI_LABEL,
            f.SWAG_POINTS,
            COALESCE(mt_key.TEAMID, mt_raw.TEAMID) AS TEAMID,
            t.TEAMNAME,
            t.PROGRAMID,
            p.PROGRAMNAME,
            CAST(CASE WHEN COALESCE(mr.MSP_ENABLED, 0) = 1 THEN 1 ELSE 0 END AS INT) AS IS_MSP_FEATURE
          FROM f
          LEFT JOIN {_svq('MAP_ADO_TEAM_TO_TCO_TEAM')} mt_key
            ON UPPER(LTRIM(RTRIM(mt_key.ADO_TEAM_KEY))) = UPPER(LTRIM(RTRIM(f.TEAM_VARIANT_KEY)))
          LEFT JOIN {_svq('MAP_ADO_TEAM_TO_TCO_TEAM')} mt_raw
            ON (f.TEAM_VARIANT_KEY IS NULL OR LTRIM(RTRIM(f.TEAM_VARIANT_KEY)) = '')
           AND UPPER(LTRIM(RTRIM(mt_raw.ADO_TEAM))) = UPPER(NULLIF(LTRIM(RTRIM(f.TEAM_RAW)), ''))
          LEFT JOIN {_svq('TEAMS')} t ON t.TEAMID = COALESCE(mt_key.TEAMID, mt_raw.TEAMID)
          LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
          LEFT JOIN {_svq('TEAM_MSP_RATE')} mr ON mr.TEAMID = t.TEAMID
        ),
        done_features AS (
          SELECT
            FEATURE_ID,
            TEAMID,
            TEAMNAME,
            PROGRAMID,
            PROGRAMNAME,
            YEAR,
            PI,
            PI_LABEL,
            SWAG_POINTS
          FROM mapped
          WHERE TEAMID IS NOT NULL
            AND PROGRAMID IS NOT NULL
            AND YEAR IS NOT NULL
            AND PI IS NOT NULL
            AND COALESCE(IS_MSP_FEATURE, 0) = 0
        ),
        team_pi AS (
          SELECT
            TEAMID,
            MAX(TEAMNAME) AS TEAMNAME,
            PROGRAMID,
            MAX(PROGRAMNAME) AS PROGRAMNAME,
            YEAR,
            PI,
            MIN(PI_LABEL) AS PI_LABEL,
            CAST(SUM(COALESCE(SWAG_POINTS, 0)) AS FLOAT) AS PI_POINTS_DONE,
            COUNT(DISTINCT FEATURE_ID) AS PI_FEATURES_DONE
          FROM done_features
          GROUP BY TEAMID, PROGRAMID, YEAR, PI
        ),
        team_roll AS (
          SELECT
            t.*,
            (TRY_CONVERT(INT, t.YEAR) * 100 + TRY_CONVERT(INT, t.PI)) AS PI_ORDER,
            CAST(cap.ELIGIBLE_DELIVERY_FTE AS FLOAT) AS ELIGIBLE_DELIVERY_FTE,
            CAST(
              CASE
                WHEN COALESCE(cap.ELIGIBLE_DELIVERY_FTE, 0) <= 0 THEN NULL
                ELSE COALESCE(t.PI_POINTS_DONE, 0) / NULLIF(cap.ELIGIBLE_DELIVERY_FTE, 0)
              END AS FLOAT
            ) AS PI_POINTS_PER_FTE,
            AVG(
              CASE
                WHEN COALESCE(cap.ELIGIBLE_DELIVERY_FTE, 0) <= 0 THEN NULL
                ELSE COALESCE(t.PI_POINTS_DONE, 0) / NULLIF(cap.ELIGIBLE_DELIVERY_FTE, 0)
              END
            ) OVER (
              PARTITION BY t.TEAMID
              ORDER BY (TRY_CONVERT(INT, t.YEAR) * 100 + TRY_CONVERT(INT, t.PI))
              ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ) AS TEAM_ROLLING_POINTS_PER_FTE,
            COUNT(
              CASE WHEN COALESCE(cap.ELIGIBLE_DELIVERY_FTE, 0) > 0 THEN 1 ELSE NULL END
            ) OVER (
              PARTITION BY t.TEAMID
              ORDER BY (TRY_CONVERT(INT, t.YEAR) * 100 + TRY_CONVERT(INT, t.PI))
              ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ) AS SAMPLE_PIS
          FROM team_pi t
          LEFT JOIN {_svq('VW_TEAM_COMPOSITION_EFFECTIVE')} tc
            ON tc.TEAMID = t.TEAMID
           AND TRY_CONVERT(INT, tc.YEAR) = TRY_CONVERT(INT, t.YEAR)
           AND TRY_CONVERT(INT, tc.PI) = TRY_CONVERT(INT, t.PI)
          OUTER APPLY (
            SELECT
              CAST(
                COALESCE(TRY_CONVERT(FLOAT, tc.DELIVERY_TEAM_FTE), 0)
                + COALESCE(TRY_CONVERT(FLOAT, tc.CONTRACTOR_C_FTE), 0)
                + COALESCE(TRY_CONVERT(FLOAT, tc.CONTRACTOR_CS_FTE), 0)
                AS FLOAT
              ) AS ELIGIBLE_DELIVERY_FTE
          ) cap
        ),
        team_based AS (
          SELECT
            TEAMID,
            TEAMNAME,
            PROGRAMID,
            PROGRAMNAME,
            YEAR,
            PI,
            PI_LABEL,
            PI_ORDER,
            PI_POINTS_DONE,
            PI_FEATURES_DONE,
            ELIGIBLE_DELIVERY_FTE,
            PI_POINTS_PER_FTE,
            SAMPLE_PIS,
            CAST(
              CASE
                WHEN TEAM_ROLLING_POINTS_PER_FTE IS NULL THEN NULL
                WHEN TEAM_ROLLING_POINTS_PER_FTE < 35.0 THEN 35.0
                WHEN TEAM_ROLLING_POINTS_PER_FTE > 95.0 THEN 95.0
                ELSE TEAM_ROLLING_POINTS_PER_FTE
              END AS FLOAT
            ) AS TEAM_BASELINE_POINTS
          FROM team_roll
        ),
        program_pi AS (
          SELECT
            PROGRAMID,
            MAX(PROGRAMNAME) AS PROGRAMNAME,
            YEAR,
            PI,
            MIN(PI_LABEL) AS PI_LABEL,
            CAST(
              SUM(COALESCE(PI_POINTS_DONE, 0))
              / NULLIF(SUM(COALESCE(ELIGIBLE_DELIVERY_FTE, 0)), 0)
              AS FLOAT
            ) AS PROGRAM_PI_POINTS_PER_FTE
          FROM team_based
          GROUP BY PROGRAMID, YEAR, PI
        ),
        program_roll AS (
          SELECT
            p.*,
            (TRY_CONVERT(INT, p.YEAR) * 100 + TRY_CONVERT(INT, p.PI)) AS PI_ORDER,
            AVG(CAST(p.PROGRAM_PI_POINTS_PER_FTE AS FLOAT)) OVER (
              PARTITION BY p.PROGRAMID
              ORDER BY (TRY_CONVERT(INT, p.YEAR) * 100 + TRY_CONVERT(INT, p.PI))
              ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ) AS PROGRAM_ROLLING_POINTS,
            COUNT(p.PROGRAM_PI_POINTS_PER_FTE) OVER (
              PARTITION BY p.PROGRAMID
              ORDER BY (TRY_CONVERT(INT, p.YEAR) * 100 + TRY_CONVERT(INT, p.PI))
              ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ) AS PROGRAM_SAMPLE_PIS
          FROM program_pi p
        ),
        program_based AS (
          SELECT
            PROGRAMID,
            PROGRAMNAME,
            YEAR,
            PI,
            PI_LABEL,
            PROGRAM_SAMPLE_PIS,
            CAST(
              CASE
                WHEN PROGRAM_ROLLING_POINTS IS NULL THEN NULL
                WHEN PROGRAM_ROLLING_POINTS < 35.0 THEN 35.0
                WHEN PROGRAM_ROLLING_POINTS > 95.0 THEN 95.0
                ELSE PROGRAM_ROLLING_POINTS
              END AS FLOAT
            ) AS PROGRAM_BASELINE_POINTS
          FROM program_roll
        ),
        global_baseline AS (
          SELECT
            CAST(
              COALESCE(
                (
                  SELECT TOP 1 TRY_CONVERT(FLOAT, JSON_VALUE(ap.CONFIG_JSON, '$.swag.points_per_fte'))
                  FROM {_svq('ADO_PROFILES')} ap
                  WHERE COALESCE(ap.IS_ACTIVE, 0) = 1
                  ORDER BY ap.UPDATED_AT DESC
                ),
                (
                  SELECT TOP 1 TRY_CONVERT(FLOAT, JSON_VALUE(ap.CONFIG_JSON, '$.swag.points_per_fte'))
                  FROM {_svq('ADO_PROFILES')} ap
                  ORDER BY ap.UPDATED_AT DESC
                ),
                65.0
              ) AS FLOAT
            ) AS GLOBAL_BASELINE_POINTS
        ),
        effective AS (
          SELECT
            t.TEAMID,
            t.TEAMNAME,
            t.PROGRAMID,
            t.PROGRAMNAME,
            t.YEAR,
            t.PI,
            t.PI_LABEL,
            t.PI_POINTS_DONE,
            t.PI_FEATURES_DONE,
            t.ELIGIBLE_DELIVERY_FTE,
            t.PI_POINTS_PER_FTE,
            t.SAMPLE_PIS,
            t.TEAM_BASELINE_POINTS,
            p.PROGRAM_BASELINE_POINTS,
            p.PROGRAM_SAMPLE_PIS,
            CAST(
              CASE
                WHEN COALESCE(t.SAMPLE_PIS, 0) >= 3 AND t.TEAM_BASELINE_POINTS IS NOT NULL
                  THEN t.TEAM_BASELINE_POINTS
                WHEN COALESCE(p.PROGRAM_SAMPLE_PIS, 0) >= 3 AND p.PROGRAM_BASELINE_POINTS IS NOT NULL
                  THEN p.PROGRAM_BASELINE_POINTS
                ELSE gb.GLOBAL_BASELINE_POINTS
              END AS FLOAT
            ) AS EFFECTIVE_BASELINE_POINTS,
            CAST(
              CASE
                WHEN COALESCE(t.SAMPLE_PIS, 0) >= 3 AND t.TEAM_BASELINE_POINTS IS NOT NULL
                  THEN 'TEAM'
                WHEN COALESCE(p.PROGRAM_SAMPLE_PIS, 0) >= 3 AND p.PROGRAM_BASELINE_POINTS IS NOT NULL
                  THEN 'PROGRAM'
                ELSE 'GLOBAL'
              END AS NVARCHAR(20)
            ) AS BASELINE_SOURCE
          FROM team_based t
          LEFT JOIN program_based p
            ON p.PROGRAMID = t.PROGRAMID
           AND p.YEAR = t.YEAR
           AND p.PI = t.PI
          CROSS JOIN global_baseline gb
        ),
        with_prev AS (
          SELECT
            e.*,
            LAG(e.EFFECTIVE_BASELINE_POINTS) OVER (
              PARTITION BY e.TEAMID
              ORDER BY (TRY_CONVERT(INT, e.YEAR) * 100 + TRY_CONVERT(INT, e.PI))
            ) AS BASELINE_PREV
          FROM effective e
        )
        SELECT
          TEAMID,
          TEAMNAME,
          PROGRAMID,
          PROGRAMNAME,
          YEAR,
          PI,
          PI_LABEL,
          PI_POINTS_DONE,
          PI_FEATURES_DONE,
          ELIGIBLE_DELIVERY_FTE,
          PI_POINTS_PER_FTE,
          SAMPLE_PIS,
          TEAM_BASELINE_POINTS,
          EFFECTIVE_BASELINE_POINTS,
          BASELINE_SOURCE,
          CAST(
            CASE
              WHEN BASELINE_PREV IS NULL OR BASELINE_PREV = 0 THEN 0
              WHEN ABS(EFFECTIVE_BASELINE_POINTS - BASELINE_PREV) / NULLIF(ABS(BASELINE_PREV), 0) >= 0.15 THEN 1
              ELSE 0
            END AS INT
          ) AS BASELINE_CHANGED_FLAG,
          CAST(BASELINE_PREV AS FLOAT) AS BASELINE_PREV
        FROM with_prev
        """,
    )
    return True


def ensure_tco_team_velocity_snapshot_table() -> bool:
    """Persisted velocity snapshot for fast, stable reads on Welcome/Insights."""
    execute(
        f"""
        IF OBJECT_ID('{_svq('TCO_TEAM_VELOCITY_SNAPSHOT')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')} (
            TEAMID NVARCHAR(255) NOT NULL,
            TEAMNAME NVARCHAR(255) NULL,
            PROGRAMID NVARCHAR(255) NULL,
            PROGRAMNAME NVARCHAR(255) NULL,
            YEAR INT NOT NULL,
            PI INT NOT NULL,
            PI_LABEL NVARCHAR(64) NULL,
            PI_POINTS_DONE FLOAT NULL,
            PI_FEATURES_DONE INT NULL,
            SAMPLE_PIS INT NULL,
            TEAM_BASELINE_POINTS FLOAT NULL,
            EFFECTIVE_BASELINE_POINTS FLOAT NULL,
            BASELINE_SOURCE NVARCHAR(20) NULL,
            BASELINE_CHANGED_FLAG INT NULL,
            BASELINE_PREV FLOAT NULL,
            SNAPSHOT_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            DATA_VERSION INT NULL,
            CONSTRAINT PK_TCO_TEAM_VELOCITY_SNAPSHOT PRIMARY KEY (TEAMID, YEAR, PI)
          );
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1
          FROM sys.indexes
          WHERE object_id = OBJECT_ID('{_svq('TCO_TEAM_VELOCITY_SNAPSHOT')}')
            AND name = 'IX_TCO_TEAM_VELOCITY_SNAPSHOT_YEAR_PI'
        )
        BEGIN
          CREATE INDEX IX_TCO_TEAM_VELOCITY_SNAPSHOT_YEAR_PI
            ON {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')} (YEAR, PI);
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1
          FROM sys.indexes
          WHERE object_id = OBJECT_ID('{_svq('TCO_TEAM_VELOCITY_SNAPSHOT')}')
            AND name = 'IX_TCO_TEAM_VELOCITY_SNAPSHOT_PROGRAM_TEAM'
        )
        BEGIN
          CREATE INDEX IX_TCO_TEAM_VELOCITY_SNAPSHOT_PROGRAM_TEAM
            ON {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')} (PROGRAMID, TEAMID);
        END
        """
    )
    return True


def refresh_tco_team_velocity_snapshot(
    *,
    data_version: Optional[int] = None,
    year: Optional[int] = None,
    include_prior_year: bool = True,
    reference_year_only: bool = False,
) -> bool:
    """Refresh persisted team velocity snapshot from canonical velocity baseline view."""
    ensure_tco_team_velocity_baseline_view()
    ensure_tco_team_velocity_snapshot_table()
    version_raw = data_version if data_version is not None else (get_data_version() or 0)
    try:
        version = int(version_raw)
    except Exception:
        version = 0
    if version < 0:
        version = 0
    if version > 2_147_483_647:
        version = 2_147_483_647

    year_upper: Optional[int] = None
    year_lower: Optional[int] = None
    if year is not None:
        try:
            year_upper = int(year)
            if year_upper > 0:
                if bool(reference_year_only):
                    year_lower = year_upper
                else:
                    year_lower = max(0, year_upper - 1) if bool(include_prior_year) else year_upper
        except Exception:
            year_upper = None
            year_lower = None

    where_parts = [
        "TRY_CONVERT(INT, v.YEAR) IS NOT NULL",
        "TRY_CONVERT(INT, v.PI) IS NOT NULL",
        "COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMID)), ''), 'UNKNOWN') <> 'UNKNOWN'",
    ]
    params: List[Any] = []
    if year_upper is not None and year_lower is not None:
        where_parts.append("TRY_CONVERT(INT, v.YEAR) <= %s")
        where_parts.append("TRY_CONVERT(INT, v.YEAR) >= %s")
        params.extend([int(year_upper), int(year_lower)])

    src = fetch_df(
        f"""
        SELECT
          COALESCE(NULLIF(LTRIM(RTRIM(v.TEAMID)), ''), 'UNKNOWN') AS TEAMID,
          NULLIF(LTRIM(RTRIM(v.TEAMNAME)), '') AS TEAMNAME,
          NULLIF(LTRIM(RTRIM(v.PROGRAMID)), '') AS PROGRAMID,
          NULLIF(LTRIM(RTRIM(v.PROGRAMNAME)), '') AS PROGRAMNAME,
          TRY_CONVERT(INT, v.YEAR) AS YEAR,
          TRY_CONVERT(INT, v.PI) AS PI,
          NULLIF(LTRIM(RTRIM(v.PI_LABEL)), '') AS PI_LABEL,
          TRY_CONVERT(FLOAT, v.PI_POINTS_DONE) AS PI_POINTS_DONE,
          TRY_CONVERT(INT, v.PI_FEATURES_DONE) AS PI_FEATURES_DONE,
          TRY_CONVERT(INT, v.SAMPLE_PIS) AS SAMPLE_PIS,
          TRY_CONVERT(FLOAT, v.TEAM_BASELINE_POINTS) AS TEAM_BASELINE_POINTS,
          TRY_CONVERT(FLOAT, v.EFFECTIVE_BASELINE_POINTS) AS EFFECTIVE_BASELINE_POINTS,
          UPPER(LTRIM(RTRIM(COALESCE(v.BASELINE_SOURCE, 'GLOBAL')))) AS BASELINE_SOURCE,
          TRY_CONVERT(INT, COALESCE(v.BASELINE_CHANGED_FLAG, 0)) AS BASELINE_CHANGED_FLAG,
          TRY_CONVERT(FLOAT, v.BASELINE_PREV) AS BASELINE_PREV
        FROM {_fq('VW_TCO_TEAM_VELOCITY_BASELINE')} v
        WHERE {' AND '.join(where_parts)}
        ORDER BY TRY_CONVERT(INT, v.YEAR), TRY_CONVERT(INT, v.PI), v.PROGRAMNAME, v.TEAMNAME
        """,
        tuple(params) if params else None,
    )
    if src is None:
        src = pd.DataFrame()

    if year_upper is not None and year_lower is not None:
        execute(
            f"""
            DELETE FROM {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')}
            WHERE TRY_CONVERT(INT, YEAR) <= %s
              AND TRY_CONVERT(INT, YEAR) >= %s
            """,
            (int(year_upper), int(year_lower)),
        )
    else:
        execute(f"DELETE FROM {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')}")

    if src.empty:
        return True

    def _none(v: Any) -> Any:
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        return v

    rows: List[Tuple[Any, ...]] = []
    for _, r in src.iterrows():
        rows.append(
            (
                str(r.get("TEAMID") or "UNKNOWN"),
                _none(r.get("TEAMNAME")),
                _none(r.get("PROGRAMID")),
                _none(r.get("PROGRAMNAME")),
                int(r.get("YEAR")) if _none(r.get("YEAR")) is not None else None,
                int(r.get("PI")) if _none(r.get("PI")) is not None else None,
                _none(r.get("PI_LABEL")),
                _none(r.get("PI_POINTS_DONE")),
                _none(r.get("PI_FEATURES_DONE")),
                _none(r.get("SAMPLE_PIS")),
                _none(r.get("TEAM_BASELINE_POINTS")),
                _none(r.get("EFFECTIVE_BASELINE_POINTS")),
                str(_none(r.get("BASELINE_SOURCE")) or "GLOBAL").strip().upper(),
                _none(r.get("BASELINE_CHANGED_FLAG")),
                _none(r.get("BASELINE_PREV")),
                int(version),
            )
        )

    insert_sql = f"""
        INSERT INTO {_fq('TCO_TEAM_VELOCITY_SNAPSHOT')} (
          TEAMID,
          TEAMNAME,
          PROGRAMID,
          PROGRAMNAME,
          YEAR,
          PI,
          PI_LABEL,
          PI_POINTS_DONE,
          PI_FEATURES_DONE,
          SAMPLE_PIS,
          TEAM_BASELINE_POINTS,
          EFFECTIVE_BASELINE_POINTS,
          BASELINE_SOURCE,
          BASELINE_CHANGED_FLAG,
          BASELINE_PREV,
          SNAPSHOT_AT,
          DATA_VERSION
        )
        VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          SYSUTCDATETIME(), %s
        )
    """
    chunk_size = 1000
    for i in range(0, len(rows), chunk_size):
        execute(insert_sql, rows[i : i + chunk_size], many=True)
    return True


def ensure_tco_feature_demand_view(*args, force: bool = False, **kwargs) -> bool:
    """Ensure the canonical feature-level demand view exists."""
    try:
        existing = fetch_df(
            "SELECT 1 AS OK FROM sys.views WHERE schema_id = SCHEMA_ID('dbo') AND name = %s",
            ("VW_TCO_FEATURE_DEMAND",),
        )
        if existing is not None and not existing.empty:
            auto_migrate = str(os.getenv("TCO_AUTO_MIGRATE_FEATURE_DEMAND", "0") or "").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            if not force and not auto_migrate:
                # Fail-open startup behavior: keep serving the currently deployed
                # view shape unless explicit migration is requested. This avoids
                # blocking page loads on heavy auto-migrations for large portfolios.
                return True
            mod = fetch_df(
                """
                SELECT m.definition AS DEFN
                FROM sys.sql_modules m
                WHERE m.object_id = OBJECT_ID('dbo.VW_TCO_FEATURE_DEMAND')
                """,
                None,
            )
            defn = str(mod.iloc[0]["DEFN"]) if mod is not None and not mod.empty and "DEFN" in mod.columns else ""
            if not force and "TCO_FEATURE_DEMAND_VIEW_V6" in defn:
                return True
    except Exception:
        pass

    ensure_ado_minimal_tables()
    ensure_ado_iteration_calendar_table()
    ensure_team_msp_rate_table()

    ensure_tco_team_velocity_baseline_view()
    velocity_join_sql = f"""
    global_baseline AS (
      SELECT
        CAST(
          COALESCE(
            (
              SELECT TOP 1 TRY_CONVERT(FLOAT, JSON_VALUE(ap.CONFIG_JSON, '$.swag.points_per_fte'))
              FROM {_svq('ADO_PROFILES')} ap
              WHERE COALESCE(ap.IS_ACTIVE, 0) = 1
              ORDER BY ap.UPDATED_AT DESC
            ),
            (
              SELECT TOP 1 TRY_CONVERT(FLOAT, JSON_VALUE(ap.CONFIG_JSON, '$.swag.points_per_fte'))
              FROM {_svq('ADO_PROFILES')} ap
              ORDER BY ap.UPDATED_AT DESC
            ),
            65.0
          ) AS FLOAT
        ) AS GLOBAL_BASELINE_POINTS
    ),
    velocity_joined AS (
      SELECT
        m.*,
        CAST(COALESCE(v.EFFECTIVE_BASELINE_POINTS, gb.GLOBAL_BASELINE_POINTS) AS FLOAT) AS EFFECTIVE_BASELINE_POINTS,
        CAST(COALESCE(v.BASELINE_SOURCE, 'GLOBAL') AS NVARCHAR(20)) AS BASELINE_SOURCE,
        CAST(COALESCE(v.BASELINE_CHANGED_FLAG, 0) AS INT) AS BASELINE_CHANGED_FLAG,
        CAST(v.BASELINE_PREV AS FLOAT) AS BASELINE_PREV,
        CAST(gb.GLOBAL_BASELINE_POINTS AS FLOAT) AS GLOBAL_BASELINE_POINTS
      FROM mapped m
      CROSS JOIN global_baseline gb
      LEFT JOIN {_svq('VW_TCO_TEAM_VELOCITY_BASELINE')} v
        ON v.TEAMID = m.TEAMID
       AND v.YEAR = m.YEAR
       AND v.PI = m.PI
    )
    """

    _create_or_alter_view(
        "VW_TCO_FEATURE_DEMAND",
        f"""
        /* TCO_FEATURE_DEMAND_VIEW_V6 */
        WITH f AS (
          SELECT
            af.FEATURE_ID,
            af.TITLE AS FEATURE_TITLE,
            af.STATE AS FEATURE_STATE,
            af.APP_NAME_RAW,
            af.TEAM_RAW,
            af.TEAM_VARIANT_KEY,
            TRY_CONVERT(FLOAT, af.STORY_POINTS) AS SWAG_POINTS,
            af.ITERATION_PATH,
            ls.LABEL_SOURCE AS ITERATION_LEVEL3,
            af.ITERATION_SK,
            COALESCE(
              TRY_CONVERT(INT, LEFT(ls.LABEL_SOURCE, 4)),
              TRY_CONVERT(INT, ic_sk.YEAR),
              TRY_CONVERT(INT, ic_path.YEAR),
              TRY_CONVERT(INT, af.ADO_YEAR)
            ) AS YEAR,
            CASE
              WHEN ls.LABEL_SOURCE_NORM IS NULL THEN NULL
              WHEN CHARINDEX('PI', ls.LABEL_SOURCE_NORM) > 0
                THEN TRY_CONVERT(INT, SUBSTRING(ls.LABEL_SOURCE_NORM, CHARINDEX('PI', ls.LABEL_SOURCE_NORM) + 2, 10))
              WHEN PATINDEX('% I[0-9]%', ls.LABEL_SOURCE_NORM) > 0
                THEN TRY_CONVERT(INT, SUBSTRING(ls.LABEL_SOURCE_NORM, PATINDEX('% I[0-9]%', ls.LABEL_SOURCE_NORM) + 2, 10))
              WHEN PATINDEX('I[0-9]%', ls.LABEL_SOURCE_NORM) > 0
                THEN TRY_CONVERT(INT, SUBSTRING(ls.LABEL_SOURCE_NORM, PATINDEX('I[0-9]%', ls.LABEL_SOURCE_NORM) + 1, 10))
              ELSE NULL
            END AS PI,
            ls.LABEL_SOURCE AS PI_LABEL,
            af.INVESTMENT_DIMENSION
          FROM {_svq('ADO_FEATURES')} af
          OUTER APPLY (
            SELECT
              CASE
                WHEN af.ITERATION_PATH IS NULL OR LTRIM(RTRIM(af.ITERATION_PATH)) = '' THEN NULL
                WHEN (
                  PATINDEX('%\\I[0-9] S[0-9]%', UPPER(af.ITERATION_PATH)) > 0
                  OR PATINDEX('%\\S[0-9]%', UPPER(af.ITERATION_PATH)) > 0
                )
                AND CHARINDEX('\\', af.ITERATION_PATH) > 0
                THEN LEFT(af.ITERATION_PATH, LEN(af.ITERATION_PATH) - CHARINDEX('\\', REVERSE(af.ITERATION_PATH)))
                ELSE af.ITERATION_PATH
              END AS FEATURE_PI_PARENT_ITERATION_PATH
          ) fp
          LEFT JOIN {_svq('ADO_ITERATION_CALENDAR')} ic_sk
            ON UPPER(LTRIM(RTRIM(ic_sk.ITERATION_SK))) = UPPER(LTRIM(RTRIM(NULLIF(LTRIM(RTRIM(af.ITERATION_SK)), ''))))
           AND UPPER(LTRIM(RTRIM(ic_sk.ITERATION_GRAIN))) = 'PI'
          LEFT JOIN {_svq('ADO_ITERATION_CALENDAR')} ic_path
            ON UPPER(LTRIM(RTRIM(ic_path.ITERATION_PATH))) = UPPER(LTRIM(RTRIM(fp.FEATURE_PI_PARENT_ITERATION_PATH)))
           AND UPPER(LTRIM(RTRIM(ic_path.ITERATION_GRAIN))) = 'PI'
          OUTER APPLY (
            SELECT
              COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3, af.PI_LABEL, af.ITERATION_LEVEL3) AS LABEL_SOURCE,
              UPPER(LTRIM(RTRIM(COALESCE(ic_sk.ITERATION_LEVEL3, ic_path.ITERATION_LEVEL3, af.PI_LABEL, af.ITERATION_LEVEL3)))) AS LABEL_SOURCE_NORM
          ) ls
        ),
        mapped AS (
          SELECT
            f.*,
            COALESCE(mt_key.TEAMID, mt_raw.TEAMID) AS TEAMID,
            t.TEAMNAME,
            t.PROGRAMID,
            p.PROGRAMNAME,
            mag.APP_GROUP AS GROUPID,
            ag.GROUPNAME,
            CAST(CASE WHEN COALESCE(mr.MSP_ENABLED, 0) = 1 THEN 1 ELSE 0 END AS INT) AS IS_MSP_FEATURE
          FROM f
          OUTER APPLY (
            SELECT TOP 1 mtk.TEAMID
            FROM {_svq('MAP_ADO_TEAM_TO_TCO_TEAM')} mtk
            WHERE mtk.ADO_TEAM_KEY = UPPER(LTRIM(RTRIM(f.TEAM_VARIANT_KEY)))
          ) mt_key
          OUTER APPLY (
            SELECT TOP 1 mtr.TEAMID
            FROM {_svq('MAP_ADO_TEAM_TO_TCO_TEAM')} mtr
            WHERE (f.TEAM_VARIANT_KEY IS NULL OR LTRIM(RTRIM(f.TEAM_VARIANT_KEY)) = '')
              AND mtr.ADO_TEAM = UPPER(NULLIF(LTRIM(RTRIM(f.TEAM_RAW)), ''))
            ORDER BY CASE WHEN mtr.TEAMID IS NULL THEN 1 ELSE 0 END, mtr.TEAMID
          ) mt_raw
          LEFT JOIN {_svq('TEAMS')} t ON t.TEAMID = COALESCE(mt_key.TEAMID, mt_raw.TEAMID)
          LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
          LEFT JOIN {_svq('MAP_ADO_APP_TO_TCO_GROUP')} mag ON UPPER(LTRIM(RTRIM(mag.ADO_APP))) = UPPER(LTRIM(RTRIM(f.APP_NAME_RAW)))
          LEFT JOIN {_svq('APPLICATION_GROUPS')} ag ON ag.GROUPID = mag.APP_GROUP
          LEFT JOIN {_svq('TEAM_MSP_RATE')} mr ON mr.TEAMID = t.TEAMID
        ),
        {velocity_join_sql}
        SELECT
          FEATURE_ID,
          FEATURE_TITLE,
          FEATURE_STATE,
          APP_NAME_RAW,
          TEAMID,
          TEAMNAME,
          PROGRAMID,
          PROGRAMNAME,
          GROUPID,
          GROUPNAME,
          CAST(
            CASE
              WHEN TEAMID IS NULL OR PROGRAMID IS NULL THEN 'OUT_OF_SCOPE_PROGRAM'
              WHEN GROUPID IS NULL THEN 'UNMAPPED_APP_GROUP'
              ELSE 'MAPPED'
            END
            AS NVARCHAR(50)
          ) AS MAPPING_STATUS,
          YEAR,
          PI,
          PI_LABEL,
          SWAG_POINTS,
          EFFECTIVE_BASELINE_POINTS,
          BASELINE_SOURCE,
          BASELINE_CHANGED_FLAG,
          BASELINE_PREV,
          CAST(
            CASE
              WHEN UPPER(COALESCE(FEATURE_STATE, '')) = 'REMOVED' THEN 0
              WHEN YEAR IS NULL OR PI IS NULL THEN 0
              ELSE 1
            END
            AS INT
          ) AS IN_SCOPE_FOR_ROADMAP,
          CAST(
            CASE
              WHEN UPPER(COALESCE(FEATURE_STATE, '')) = 'REMOVED' THEN 0
              WHEN COALESCE(IS_MSP_FEATURE, 0) = 1 THEN 0
              WHEN YEAR IS NULL OR PI IS NULL THEN 0
              WHEN COALESCE(SWAG_POINTS, 0) > 0 THEN 1
              ELSE 0
            END
            AS INT
          ) AS IS_SWAG_READY,
          IS_MSP_FEATURE,
          CAST(
            CASE
              WHEN COALESCE(IS_MSP_FEATURE, 0) = 1 THEN 0.0
              WHEN YEAR IS NULL OR PI IS NULL THEN 0.0
              WHEN COALESCE(SWAG_POINTS, 0) <= 0 THEN 0.0
              ELSE COALESCE((COALESCE(SWAG_POINTS, 0) / NULLIF(COALESCE(GLOBAL_BASELINE_POINTS, 0), 0)), 0.0)
            END
            AS FLOAT
          ) AS DERIVED_FTE_FEATURE,
          CAST(
            CASE
              WHEN COALESCE(IS_MSP_FEATURE, 0) = 1 THEN 0.0
              WHEN YEAR IS NULL OR PI IS NULL THEN 0.0
              WHEN COALESCE(SWAG_POINTS, 0) <= 0 THEN 0.0
              ELSE COALESCE((COALESCE(SWAG_POINTS, 0) / NULLIF(COALESCE(EFFECTIVE_BASELINE_POINTS, 0), 0)), 0.0)
            END
            AS FLOAT
          ) AS DERIVED_FTE_FEATURE_VELOCITY,
          INVESTMENT_DIMENSION
        FROM velocity_joined
        WHERE FEATURE_ID IS NOT NULL
          AND YEAR IS NOT NULL
          AND PI IS NOT NULL
        """,
    )
    return True


def ensure_ado_features_enriched_view(*args, **kwargs) -> bool:
    """Stable, UI-facing ADO features view with consistent columns."""
    ensure_ado_minimal_tables()
    ensure_tco_feature_demand_view()
    _create_or_alter_view(
        "VW_ADO_FEATURES_ENRICHED",
        f"""
        SELECT
          af.FEATURE_ID,
          af.TITLE AS TITLE,
          af.STATE AS STATE,
          TRY_CONVERT(DATETIME2, af.CHANGED_AT) AS CHANGED_DATE,
          TRY_CONVERT(INT, COALESCE(d.YEAR, af.ADO_YEAR)) AS ADO_YEAR,
          COALESCE(af.PI_LABEL, d.PI_LABEL, af.ITERATION_LEVEL3) AS PI_LABEL,
          COALESCE(af.ITERATION_LEVEL3_RAW, af.ITERATION_LEVEL3) AS PI_LABEL_RAW,
          af.ITERATION_PATH,
          TRY_CONVERT(INT, d.PI) AS ITERATION_NUM,
          COALESCE(d.PROGRAMNAME, p.PROGRAMNAME) AS PROGRAMNAME,
          COALESCE(d.TEAMNAME, t.TEAMNAME) AS TEAMNAME,
          COALESCE(d.GROUPNAME, ag.GROUPNAME, '(Unmapped Application)') AS GROUPNAME,
          af.APP_NAME_RAW,
          TRY_CONVERT(FLOAT, af.BUSINESS_VALUE) AS BUSINESS_VALUE,
          COALESCE(TRY_CONVERT(FLOAT, af.STORY_POINTS), TRY_CONVERT(FLOAT, af.EFFORT_POINTS)) AS STORY_POINTS,
          TRY_CONVERT(FLOAT, d.DERIVED_FTE_FEATURE) AS DERIVED_FTE_SWAG,
          COALESCE(
            d.MAPPING_STATUS,
            CASE
              WHEN t.TEAMID IS NULL OR p.PROGRAMID IS NULL THEN 'OUT_OF_SCOPE_PROGRAM'
              WHEN mag.APP_GROUP IS NULL THEN 'UNMAPPED_APP_GROUP'
              ELSE 'MAPPED'
            END
          ) AS MAPPING_STATUS,
          CAST(
            CASE
              WHEN d.FEATURE_ID IS NULL THEN 1
              ELSE COALESCE(d.IN_SCOPE_FOR_ROADMAP, 0)
            END
            AS INT
          ) AS IN_SCOPE_FOR_ROADMAP,
          COALESCE(TRY_CONVERT(INT, ag.IS_BASE), 0) AS IS_BASE,
          af.PARENT_ID,
          af.EPIC_ID,
          af.EPIC_TITLE,
          af.EPIC_STATE
        FROM {_svq('ADO_FEATURES')} af
        LEFT JOIN {_svq('VW_TCO_FEATURE_DEMAND')} d ON d.FEATURE_ID = af.FEATURE_ID
        LEFT JOIN {_svq('MAP_ADO_TEAM_TO_TCO_TEAM')} mt
          ON UPPER(mt.ADO_TEAM_KEY) = COALESCE(
               af.TEAM_VARIANT_KEY,
               UPPER(NULLIF(LTRIM(RTRIM(af.TEAM_RAW)), ''))
             )
          OR UPPER(mt.ADO_TEAM) = UPPER(NULLIF(LTRIM(RTRIM(af.TEAM_RAW)), ''))
        LEFT JOIN {_svq('TEAMS')} t ON t.TEAMID = mt.TEAMID
        LEFT JOIN {_svq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM')} mp
          ON UPPER(mp.ADO_PROGRAM) = UPPER(LTRIM(RTRIM(af.PROGRAM_RAW)))
        LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = COALESCE(t.PROGRAMID, mp.PROGRAMID)
        LEFT JOIN {_svq('MAP_ADO_APP_TO_TCO_GROUP')} mag
          ON UPPER(mag.ADO_APP) = UPPER(LTRIM(RTRIM(af.APP_NAME_RAW)))
        LEFT JOIN {_svq('APPLICATION_GROUPS')} ag ON ag.GROUPID = mag.APP_GROUP
        WHERE af.FEATURE_ID IS NOT NULL
        """,
    )
    return True


def ensure_tco_team_labor_composition_view(*args, **kwargs) -> bool:
    """Team labor composition per (YEAR, PI, TEAM) used to split feature demand across labor types."""
    ensure_team_composition_history()
    _create_or_alter_view(
        "VW_TCO_TEAM_LABOR_COMPOSITION",
        f"""
        SELECT
          TRY_CONVERT(INT, c.YEAR) AS YEAR,
          TRY_CONVERT(INT, c.PI) AS PI,
          c.TEAMID,
          t.TEAMNAME,
          t.PROGRAMID,
          p.PROGRAMNAME,
          CAST(COALESCE(TRY_CONVERT(FLOAT, c.TEAMFTE), 0) AS FLOAT) AS HC_TEAM,
          CAST(COALESCE(TRY_CONVERT(FLOAT, c.DELIVERY_TEAM_FTE), 0) AS FLOAT) AS HC_DELIVERY,
          CAST(COALESCE(TRY_CONVERT(FLOAT, c.CONTRACTOR_C_FTE), 0) AS FLOAT) AS HC_CONTRACTOR_C,
          CAST(COALESCE(TRY_CONVERT(FLOAT, c.CONTRACTOR_CS_FTE), 0) AS FLOAT) AS HC_CONTRACTOR_CS,
          CAST(
            COALESCE(TRY_CONVERT(FLOAT, c.TEAMFTE), 0)
            + COALESCE(TRY_CONVERT(FLOAT, c.DELIVERY_TEAM_FTE), 0)
            + COALESCE(TRY_CONVERT(FLOAT, c.CONTRACTOR_C_FTE), 0)
            + COALESCE(TRY_CONVERT(FLOAT, c.CONTRACTOR_CS_FTE), 0)
            AS FLOAT
          ) AS HC_TOTAL_LABOR
        FROM {_svq('VW_TEAM_COMPOSITION_EFFECTIVE')} c
        LEFT JOIN {_svq('TEAMS')} t ON t.TEAMID = c.TEAMID
        LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
        WHERE TRY_CONVERT(INT, c.YEAR) IS NOT NULL
          AND TRY_CONVERT(INT, c.PI) IS NOT NULL
        """,
    )
    return True


def ensure_tco_wf_labor_split_view(*args, **kwargs) -> bool:
    """Feature×labor-type split for WF expected spend (Derived FTE split by team composition weights)."""
    ensure_tco_feature_demand_view()
    ensure_tco_team_labor_composition_view()
    ensure_location_and_contractor_tables()

    _create_or_alter_view(
        "VW_TCO_WF_LABOR_SPLIT",
        f"""
        WITH d AS (
          SELECT *
          FROM {_svq('VW_TCO_FEATURE_DEMAND')}
          WHERE COALESCE(IN_SCOPE_FOR_ROADMAP, 0) = 1
        ),
        c AS (
          SELECT
            YEAR, PI, TEAMID,
            HC_TEAM, HC_DELIVERY, HC_CONTRACTOR_C, HC_CONTRACTOR_CS,
            HC_TOTAL_LABOR
          FROM {_svq('VW_TCO_TEAM_LABOR_COMPOSITION')}
        ),
        r AS (
          SELECT TEAMID, YEAR, PI, TEAM_RATE, DELIVERY_RATE, CONTRACTOR_C_RATE, CONTRACTOR_CS_RATE
          FROM {_svq('VW_TEAM_WEIGHTED_RATES')}
        ),
        joined AS (
          SELECT
            d.FEATURE_ID,
            d.FEATURE_TITLE,
            d.FEATURE_STATE,
            d.PROGRAMID, d.PROGRAMNAME,
            d.TEAMID, d.TEAMNAME,
            d.GROUPID, d.GROUPNAME,
            d.MAPPING_STATUS,
            d.YEAR,
            d.PI,
            d.PI_LABEL,
            d.DERIVED_FTE_FEATURE,
            COALESCE(d.IS_MSP_FEATURE, 0) AS IS_MSP_FEATURE,
            COALESCE(c.HC_TEAM, 0) AS HC_TEAM,
            COALESCE(c.HC_DELIVERY, 0) AS HC_DELIVERY,
            COALESCE(c.HC_CONTRACTOR_C, 0) AS HC_CONTRACTOR_C,
            COALESCE(c.HC_CONTRACTOR_CS, 0) AS HC_CONTRACTOR_CS,
            COALESCE(c.HC_TOTAL_LABOR, 0) AS HC_TOTAL_LABOR,
            COALESCE(r.TEAM_RATE, 0) AS TEAM_RATE,
            COALESCE(r.DELIVERY_RATE, r.TEAM_RATE, 0) AS DELIVERY_RATE,
            COALESCE(r.CONTRACTOR_C_RATE, 0) AS CONTRACTOR_C_RATE,
            COALESCE(r.CONTRACTOR_CS_RATE, 0) AS CONTRACTOR_CS_RATE,
            d.INVESTMENT_DIMENSION
          FROM d
          LEFT JOIN c
            ON c.YEAR = d.YEAR AND c.PI = d.PI AND c.TEAMID = d.TEAMID
          LEFT JOIN r
            ON r.YEAR = d.YEAR AND r.PI = d.PI AND r.TEAMID = d.TEAMID
        ),
        weights AS (
          SELECT
            j.*,
            CASE WHEN j.HC_TOTAL_LABOR > 0 THEN j.HC_TEAM / NULLIF(j.HC_TOTAL_LABOR, 0) ELSE 0 END AS W_TEAM,
            CASE WHEN j.HC_TOTAL_LABOR > 0 THEN j.HC_DELIVERY / NULLIF(j.HC_TOTAL_LABOR, 0) ELSE 0 END AS W_DELIVERY,
            CASE WHEN j.HC_TOTAL_LABOR > 0 THEN j.HC_CONTRACTOR_C / NULLIF(j.HC_TOTAL_LABOR, 0) ELSE 0 END AS W_CONTRACTOR_C,
            CASE WHEN j.HC_TOTAL_LABOR > 0 THEN j.HC_CONTRACTOR_CS / NULLIF(j.HC_TOTAL_LABOR, 0) ELSE 0 END AS W_CONTRACTOR_CS
          FROM joined j
        )
        SELECT
          'ADO' AS SOURCE,
          'WORK_FORCE' AS COST_CATEGORY,
          'Team' AS SUBCOMPONENT,
          PROGRAMID, PROGRAMNAME,
          TEAMID, TEAMNAME,
          GROUPID, GROUPNAME,
          MAPPING_STATUS,
          YEAR, PI,
          CAST((DERIVED_FTE_FEATURE * W_TEAM * TEAM_RATE) AS DECIMAL(18,2)) AS AMOUNT,
          FEATURE_ID AS ADO_FEATURE_ID,
          FEATURE_TITLE AS ADO_FEATURE_TITLE,
          FEATURE_STATE AS ADO_FEATURE_STATE,
          CAST((DERIVED_FTE_FEATURE * W_TEAM) AS FLOAT) AS FTE,
          INVESTMENT_DIMENSION
        FROM weights
        WHERE COALESCE(IS_MSP_FEATURE, 0) = 0
          AND COALESCE(DERIVED_FTE_FEATURE, 0) > 0

        UNION ALL
        SELECT
          'ADO' AS SOURCE,
          'WORK_FORCE' AS COST_CATEGORY,
          'Delivery Team' AS SUBCOMPONENT,
          PROGRAMID, PROGRAMNAME,
          TEAMID, TEAMNAME,
          GROUPID, GROUPNAME,
          MAPPING_STATUS,
          YEAR, PI,
          CAST((DERIVED_FTE_FEATURE * W_DELIVERY * DELIVERY_RATE) AS DECIMAL(18,2)) AS AMOUNT,
          FEATURE_ID AS ADO_FEATURE_ID,
          FEATURE_TITLE AS ADO_FEATURE_TITLE,
          FEATURE_STATE AS ADO_FEATURE_STATE,
          CAST((DERIVED_FTE_FEATURE * W_DELIVERY) AS FLOAT) AS FTE,
          INVESTMENT_DIMENSION
        FROM weights
        WHERE COALESCE(IS_MSP_FEATURE, 0) = 0
          AND COALESCE(DERIVED_FTE_FEATURE, 0) > 0

        UNION ALL
        SELECT
          'ADO' AS SOURCE,
          'WORK_FORCE' AS COST_CATEGORY,
          'Contractor C' AS SUBCOMPONENT,
          PROGRAMID, PROGRAMNAME,
          TEAMID, TEAMNAME,
          GROUPID, GROUPNAME,
          MAPPING_STATUS,
          YEAR, PI,
          CAST((DERIVED_FTE_FEATURE * W_CONTRACTOR_C * CONTRACTOR_C_RATE) AS DECIMAL(18,2)) AS AMOUNT,
          FEATURE_ID AS ADO_FEATURE_ID,
          FEATURE_TITLE AS ADO_FEATURE_TITLE,
          FEATURE_STATE AS ADO_FEATURE_STATE,
          CAST((DERIVED_FTE_FEATURE * W_CONTRACTOR_C) AS FLOAT) AS FTE,
          INVESTMENT_DIMENSION
        FROM weights
        WHERE COALESCE(IS_MSP_FEATURE, 0) = 0
          AND COALESCE(DERIVED_FTE_FEATURE, 0) > 0

        UNION ALL
        SELECT
          'ADO' AS SOURCE,
          'NON_WORK_FORCE' AS COST_CATEGORY,
          'Contractor CS' AS SUBCOMPONENT,
          PROGRAMID, PROGRAMNAME,
          TEAMID, TEAMNAME,
          GROUPID, GROUPNAME,
          MAPPING_STATUS,
          YEAR, PI,
          CAST((DERIVED_FTE_FEATURE * W_CONTRACTOR_CS * CONTRACTOR_CS_RATE) AS DECIMAL(18,2)) AS AMOUNT,
          FEATURE_ID AS ADO_FEATURE_ID,
          FEATURE_TITLE AS ADO_FEATURE_TITLE,
          FEATURE_STATE AS ADO_FEATURE_STATE,
          CAST((DERIVED_FTE_FEATURE * W_CONTRACTOR_CS) AS FLOAT) AS FTE,
          INVESTMENT_DIMENSION
        FROM weights
        WHERE COALESCE(IS_MSP_FEATURE, 0) = 0
          AND COALESCE(DERIVED_FTE_FEATURE, 0) > 0
        """,
    )
    return True


def ensure_all_views_ok(*args, **kwargs):
    # Build full pipeline (invoices + MSP; ADO workforce when available)
    ensure_tables()
    ensure_contracts_table()
    ensure_ado_minimal_tables()
    ensure_location_and_contractor_tables()
    ensure_team_composition_history()
    ensure_program_composition_history()
    ensure_program_additional_costs_view()
    ensure_tco_feature_demand_view()
    ensure_ado_features_enriched_view()
    ensure_tco_team_labor_composition_view()
    ensure_tco_wf_labor_split_view()
    ensure_invoice_forecast_views()
    ensure_invoice_spend_view()
    ensure_invoice_spend_pi_view()
    ensure_msp_support()
    ensure_costs_and_invoices_view()
    ensure_workforce_split_view()
    ensure_tco_completeness_view()
    return True


def ensure_infra_ok(*args, **kwargs):
    """Ensure base tables + lightweight setup required by normal pages.

    Intended to be safe to call repeatedly. This should not create heavy fanout views.
    """
    ensure_tables()
    ensure_contracts_table()
    ensure_cost_events_table()
    ensure_location_and_contractor_tables()
    ensure_ado_minimal_tables()
    ensure_ado_app_candidates_table()
    ensure_ado_iteration_calendar_table()
    ensure_ado_portfolio_settings_table()
    ensure_team_composition_history()
    ensure_program_composition_history()
    ensure_composition_changelog_tables()
    ensure_team_msp_rate_table()
    ensure_team_msp_assignments_table()
    ensure_access_control_tables()
    ensure_user_membership_tables()
    ensure_email_alert_config_table()
    return True


def ensure_analytics_views_ok(*args, **kwargs):
    """Ensure the analytics view set required by normal pages (no full rebuild)."""
    # Depends on infra, but do not call the full ensure_all_views_ok() chain.
    ensure_infra_ok()
    ensure_program_additional_costs_view()
    ensure_tco_feature_demand_view(force=False)
    ensure_ado_features_enriched_view()
    ensure_tco_team_labor_composition_view()
    ensure_tco_wf_labor_split_view()
    ensure_invoice_forecast_views()
    ensure_invoice_spend_view()
    ensure_invoice_spend_pi_view()
    ensure_msp_support()
    ensure_costs_and_invoices_view()
    ensure_workforce_split_view()
    ensure_tco_completeness_view()
    return True


@_versioned_write("FEATURE_ITERATION_MAPPING.RECOMPUTE")
def recompute_feature_iteration_mapping() -> bool:
    """Rebuild DB-side iteration mapping for Features (no ADO/network calls).

    This recreates the views that implement sprint→PI resolution and PI calendar joins.
    Safe to run after Admin restore on offline machines.
    """
    try:
        ensure_ado_minimal_tables()
    except Exception:
        pass
    try:
        ensure_ado_iteration_calendar_table()
    except Exception:
        pass
    try:
        ensure_tco_feature_demand_view(force=True)
        ensure_tco_wf_labor_split_view()
    except Exception:
        pass
    return True


def ensure_invoice_spend_view(*args, **kwargs):
    # Simple invoice aggregation by team/program/group/year
    _create_or_alter_view(
        'VW_INVOICE_SPEND',
        f"""
        SELECT
          p.PROGRAMID, p.PROGRAMNAME,
          t.TEAMID, t.TEAMNAME,
          g.GROUPID, g.GROUPNAME,
          i.FISCAL_YEAR,
          ISNULL(i.INVOICE_TYPE, 'Invoice') AS SUBCOMPONENT,
          CAST(ISNULL(i.AMOUNT, 0) AS DECIMAL(18,2)) AS AMOUNT,
          i.RENEWALDATE
        FROM {_svq('INVOICES')} i
        LEFT JOIN {_svq('TEAMS')} t ON t.TEAMID = i.TEAMID
        LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = ISNULL(i.PROGRAMID_AT_BOOKING, t.PROGRAMID)
        LEFT JOIN {_svq('APPLICATIONS')} a ON a.APPLICATIONID = i.APPLICATIONID
        LEFT JOIN {_svq('APPLICATION_GROUPS')} g ON g.GROUPID = ISNULL(i.GROUPID, a.GROUPID)
        """
    )
    return True


def ensure_invoice_spend_pi_view(*args, **kwargs):
    # Derive PI from renewal month
    _create_or_alter_view(
        'VW_INVOICE_SPEND_PI',
        f"""
        SELECT
          PROGRAMID, PROGRAMNAME,
          TEAMID, TEAMNAME,
          GROUPID, GROUPNAME,
          FISCAL_YEAR,
          CASE
            WHEN RENEWALDATE IS NULL THEN NULL
            WHEN DATEPART(MONTH, RENEWALDATE) BETWEEN 1  AND 3  THEN 1
            WHEN DATEPART(MONTH, RENEWALDATE) BETWEEN 4  AND 6  THEN 2
            WHEN DATEPART(MONTH, RENEWALDATE) BETWEEN 7  AND 9  THEN 3
            WHEN DATEPART(MONTH, RENEWALDATE) BETWEEN 10 AND 12 THEN 4
          END AS PI,
          SUBCOMPONENT,
          AMOUNT
        FROM {_svq('VW_INVOICE_SPEND')}
        """
    )
    return True


def ensure_program_additional_costs_view(*args, **kwargs):
    # Map monthly additional costs to PI (quarters) — program-level NWF only (not allocated to teams/app groups).
    _create_or_alter_view(
        'VW_PROGRAM_ADDITIONAL_COSTS_PI',
        f"""
        WITH base AS (
          SELECT
            pac.PROGRAMID,
            CASE
              WHEN pac.MONTH BETWEEN 1 AND 3 THEN 1
              WHEN pac.MONTH BETWEEN 4 AND 6 THEN 2
              WHEN pac.MONTH BETWEEN 7 AND 9 THEN 3
              WHEN pac.MONTH BETWEEN 10 AND 12 THEN 4
            END AS PI,
            pac.YEAR,
            pac.COST_TYPE,
            pac.SUBTYPE,
            SUM(pac.AMOUNT) AS AMOUNT
          FROM {_svq('PROGRAM_ADDITIONAL_COSTS')} pac
          GROUP BY
            pac.PROGRAMID,
            pac.YEAR,
            CASE
              WHEN pac.MONTH BETWEEN 1 AND 3 THEN 1
              WHEN pac.MONTH BETWEEN 4 AND 6 THEN 2
              WHEN pac.MONTH BETWEEN 7 AND 9 THEN 3
              WHEN pac.MONTH BETWEEN 10 AND 12 THEN 4
            END,
            pac.COST_TYPE,
            pac.SUBTYPE
        )
        SELECT
          b.PROGRAMID,
          p.PROGRAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS TEAMID,
          CAST(NULL AS NVARCHAR(255)) AS TEAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS GROUPID,
          CAST(NULL AS NVARCHAR(255)) AS GROUPNAME,
          b.PI,
          b.YEAR,
          b.COST_TYPE,
          b.SUBTYPE,
          b.AMOUNT
        FROM base b
        LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = b.PROGRAMID
        """
    )
    return True


def ensure_costs_and_invoices_view(*args, **kwargs):
    # Combine ADO workforce + Invoices + MSP
    _create_or_alter_view(
        'VW_COSTS_AND_INVOICES',
        f"""
        /* ADO workforce costs (feature-level), split by labor type using team composition weights.
           This prevents double-counting Derived FTE across Team/Delivery/Contractor rows. */
        SELECT
          s.SOURCE,
          s.COST_CATEGORY,
          s.SUBCOMPONENT,
          s.PROGRAMID, s.PROGRAMNAME,
          s.TEAMID, s.TEAMNAME,
          s.GROUPID, s.GROUPNAME,
          TRY_CONVERT(INT, s.YEAR) AS YEAR,
          TRY_CONVERT(INT, s.PI)   AS PI,
          CAST(s.AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          s.ADO_FEATURE_ID,
          s.ADO_FEATURE_TITLE,
          s.ADO_FEATURE_STATE,
          TRY_CONVERT(FLOAT, s.FTE) AS FTE,
          s.INVESTMENT_DIMENSION
        FROM {_svq('VW_TCO_WF_LABOR_SPLIT')} s

        UNION ALL

        /* Invoices (NON_WORK_FORCE) */
        SELECT
          'INVOICE' AS SOURCE,
          'NON_WORK_FORCE' AS COST_CATEGORY,
          isp.SUBCOMPONENT,
          isp.PROGRAMID, isp.PROGRAMNAME,
          isp.TEAMID, isp.TEAMNAME,
          isp.GROUPID, isp.GROUPNAME,
          isp.FISCAL_YEAR AS YEAR,
          isp.PI,
          CAST(isp.AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(NULL AS NVARCHAR(255)) AS ADO_FEATURE_ID,
          CAST(NULL AS NVARCHAR(255)) AS ADO_FEATURE_TITLE,
          CAST(NULL AS NVARCHAR(255)) AS ADO_FEATURE_STATE,
          CAST(NULL AS FLOAT) AS FTE,
          CAST(NULL AS NVARCHAR(255)) AS INVESTMENT_DIMENSION
        FROM {_svq('VW_INVOICE_SPEND_PI')} isp
        UNION ALL
        /* MSP fixed (NON_WORK_FORCE) */
        SELECT
          SOURCE, COST_CATEGORY, SUBCOMPONENT,
          PROGRAMID, PROGRAMNAME, TEAMID, TEAMNAME, GROUPID, GROUPNAME,
          YEAR, PI, AMOUNT,
          ADO_FEATURE_ID, ADO_FEATURE_TITLE, ADO_FEATURE_STATE,
          CAST(NULL AS FLOAT) AS FTE,
          INVESTMENT_DIMENSION
        FROM {_svq('VW_MSP_COSTS')}
        UNION ALL
        /* Program additional monthly costs (NON_WORK_FORCE, aggregated per PI) */
        SELECT
          'PROGRAM_ADDITIONAL' AS SOURCE,
          'NON_WORK_FORCE' AS COST_CATEGORY,
          /* Program-level NWF (not allocated to app groups) */
          CASE
            WHEN UPPER(pac.COST_TYPE) = 'CLOUD' AND ISNULL(pac.SUBTYPE,'') <> ''
              THEN CONCAT('Cloud ', pac.SUBTYPE)
            WHEN UPPER(pac.COST_TYPE) = 'CLOUD'
              THEN 'Cloud'
            WHEN UPPER(pac.COST_TYPE) = 'TRAVEL'
              THEN 'Travel'
            WHEN UPPER(pac.COST_TYPE) = 'INFRASTRUCTURE'
              THEN 'Infra'
            ELSE pac.COST_TYPE
          END AS SUBCOMPONENT,
          pac.PROGRAMID, pac.PROGRAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS TEAMID,
          CAST(NULL AS NVARCHAR(255)) AS TEAMNAME,
          CAST(NULL AS NVARCHAR(255)) AS GROUPID,
          CAST(NULL AS NVARCHAR(255)) AS GROUPNAME,
          pac.YEAR,
          pac.PI,
          CAST(pac.AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
          CAST(NULL AS NVARCHAR(255)) AS ADO_FEATURE_ID,
          CAST(NULL AS NVARCHAR(255)) AS ADO_FEATURE_TITLE,
          CAST(NULL AS NVARCHAR(255)) AS ADO_FEATURE_STATE,
          CAST(NULL AS FLOAT) AS FTE,
          CAST(NULL AS NVARCHAR(255)) AS INVESTMENT_DIMENSION
        FROM {_svq('VW_PROGRAM_ADDITIONAL_COSTS_PI')} pac
        """
    )
    return True


def ensure_workforce_split_view(*args, **kwargs):
    _create_or_alter_view(
        'VW_TCO_WORKFORCE_SPLIT',
        f"""
        WITH src AS (
          SELECT
            SOURCE,
            COST_CATEGORY,
            SUBCOMPONENT,
            PROGRAMID, PROGRAMNAME,
            TEAMID, TEAMNAME,
            GROUPID, GROUPNAME,
            TRY_CONVERT(INT, YEAR) AS YEAR,
            TRY_CONVERT(INT, PI)   AS PI,
            CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
            ADO_FEATURE_ID,
            ADO_FEATURE_TITLE   AS FEATURE_TITLE,
            ADO_FEATURE_STATE   AS FEATURE_STATE,
            TRY_CONVERT(FLOAT, FTE) AS FTE,
            INVESTMENT_DIMENSION AS FEATURE_INVESTMENT_DIMENSION
          FROM {_svq('VW_COSTS_AND_INVOICES')}
        ),
        base_rows AS (
          SELECT s.*
          FROM src s
          LEFT JOIN {_svq('APPLICATION_GROUPS')} ag ON ag.GROUPID = s.GROUPID
          WHERE ISNULL(ag.IS_BASE, 0) = 1
        ),
        non_base_src AS (
          SELECT s.*
          FROM src s
          LEFT JOIN {_svq('APPLICATION_GROUPS')} ag ON ag.GROUPID = s.GROUPID
          WHERE ISNULL(ag.IS_BASE, 0) = 0
        ),
        team_targets AS (
          SELECT
            g.GROUPID   AS TARGET_GROUPID,
            g.GROUPNAME AS TARGET_GROUPNAME,
            g.TEAMID    AS TARGET_TEAMID,
            COALESCE(g.PROGRAMID, t.PROGRAMID) AS TARGET_PROGRAMID,
            p.PROGRAMNAME AS TARGET_PROGRAMNAME,
            t.TEAMNAME AS TARGET_TEAMNAME
          FROM {_svq('APPLICATION_GROUPS')} g
          JOIN {_svq('TEAMS')} t ON t.TEAMID = g.TEAMID
          LEFT JOIN {_svq('PROGRAMS')} p ON p.PROGRAMID = COALESCE(g.PROGRAMID, t.PROGRAMID)
        ),
        target_counts AS (
          SELECT TARGET_TEAMID, COUNT(*) AS TARGET_CT
          FROM team_targets
          GROUP BY TARGET_TEAMID
        ),
        base_split AS (
          SELECT
            s.SOURCE,
            s.COST_CATEGORY,
            s.SUBCOMPONENT,
            nb.TARGET_PROGRAMID AS PROGRAMID,
            nb.TARGET_PROGRAMNAME AS PROGRAMNAME,
            nb.TARGET_TEAMID AS TEAMID,
            nb.TARGET_TEAMNAME AS TEAMNAME,
            nb.TARGET_GROUPID AS GROUPID,
            nb.TARGET_GROUPNAME AS GROUPNAME,
            s.YEAR,
            s.PI,
            CAST(s.AMOUNT / NULLIF(tc.TARGET_CT, 0) AS DECIMAL(18,2)) AS AMOUNT,
            s.ADO_FEATURE_ID,
            s.FEATURE_TITLE,
            s.FEATURE_STATE,
            TRY_CONVERT(FLOAT, s.FTE) / NULLIF(tc.TARGET_CT, 0) AS FTE,
            s.FEATURE_INVESTMENT_DIMENSION
          FROM base_rows s
          JOIN team_targets nb ON nb.TARGET_TEAMID = s.TEAMID
          JOIN target_counts tc ON tc.TARGET_TEAMID = s.TEAMID
        )
        SELECT * FROM non_base_src
        UNION ALL
        SELECT
          SOURCE,
          COST_CATEGORY,
          SUBCOMPONENT,
          PROGRAMID, PROGRAMNAME,
          TEAMID, TEAMNAME,
          GROUPID, GROUPNAME,
          YEAR,
          PI,
          AMOUNT,
          ADO_FEATURE_ID,
          FEATURE_TITLE,
          FEATURE_STATE,
          FTE,
          FEATURE_INVESTMENT_DIMENSION
        FROM base_split
        """
    )
    return True


def ensure_tco_completeness_view(*args, **kwargs) -> bool:
    _create_or_alter_view(
        "VW_TCO_COMPLETENESS",
        f"""
	        WITH base AS (
	          SELECT
	            TRY_CONVERT(INT, YEAR) AS YEAR,
	            PROGRAMNAME,
	            TEAMNAME,
		            GROUPNAME,
		            COST_CATEGORY,
		            CAST(AMOUNT AS DECIMAL(18,2)) AS AMOUNT,
		            TRY_CONVERT(FLOAT, FTE) AS DEMAND_FTE,
		            ADO_FEATURE_ID
		          FROM {_svq('VW_TCO_WORKFORCE_SPLIT')}
		        ),
	        agg AS (
	          SELECT
	            YEAR,
	            PROGRAMNAME,
	            TEAMNAME,
	            GROUPNAME,
	            SUM(CASE WHEN UPPER(COST_CATEGORY) = 'WORK_FORCE' THEN AMOUNT ELSE 0 END) AS WORKFORCE_COST,
	            SUM(CASE WHEN UPPER(COST_CATEGORY) = 'NON_WORK_FORCE' THEN AMOUNT ELSE 0 END) AS NONWORKFORCE_COST,
	            SUM(COALESCE(DEMAND_FTE, 0)) AS TOTAL_DEMAND_FTE,
	            COUNT(DISTINCT CASE WHEN ADO_FEATURE_ID IS NOT NULL THEN ADO_FEATURE_ID END) AS FEATURE_COUNT
	          FROM base
	          GROUP BY YEAR, PROGRAMNAME, TEAMNAME, GROUPNAME
	        )
	        SELECT
	          a.YEAR,
	          a.PROGRAMNAME,
	          a.TEAMNAME,
	          a.GROUPNAME,
	          CAST(
	            CASE
	              WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.TEAMID,''))), '') IS NULL THEN 0
	              WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.PROGRAMID, t_md.PROGRAMID, ''))), '') IS NULL THEN 0
	              ELSE 1
	            END
	            AS BIT
	          ) AS ownership_ok,
	          CAST(CASE WHEN COALESCE(a.WORKFORCE_COST, 0) > 0 THEN 1 ELSE 0 END AS BIT) AS workforce_ok,
	          CAST(
	            CASE
              WHEN t.TEAMID IS NULL THEN 1
              WHEN EXISTS (
                SELECT 1
                FROM {_fq('TEAM_RATE_HISTORY')} rh
                WHERE rh.TEAMID = t.TEAMID AND TRY_CONVERT(INT, rh.YEAR) = a.YEAR AND COALESCE(rh.XOM_RATE, 0) > 0
              ) THEN 1
              ELSE 0
            END
            AS BIT
          ) AS rates_ok,
          CAST(CASE WHEN COALESCE(a.NONWORKFORCE_COST, 0) > 0 THEN 1 ELSE 0 END AS BIT) AS nonworkforce_ok,
	          CAST(CASE WHEN COALESCE(a.TOTAL_DEMAND_FTE, 0) > 0 OR COALESCE(a.FEATURE_COUNT, 0) > 0 THEN 1 ELSE 0 END AS BIT) AS enhancements_ok,
	          CAST(
	            (
		              (
		                CASE
		                  WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.TEAMID,''))), '') IS NULL THEN 0
		                  WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.PROGRAMID, t_md.PROGRAMID, ''))), '') IS NULL THEN 0
		                  ELSE 1
		                END
		              ) +
	              (CASE WHEN COALESCE(a.WORKFORCE_COST, 0) > 0 THEN 1 ELSE 0 END) +
	              (CASE
	                WHEN t.TEAMID IS NULL THEN 1
                WHEN EXISTS (
                  SELECT 1
                  FROM {_fq('TEAM_RATE_HISTORY')} rh
                  WHERE rh.TEAMID = t.TEAMID AND TRY_CONVERT(INT, rh.YEAR) = a.YEAR AND COALESCE(rh.XOM_RATE, 0) > 0
                ) THEN 1
                ELSE 0
              END) +
              (CASE WHEN COALESCE(a.NONWORKFORCE_COST, 0) > 0 THEN 1 ELSE 0 END) +
	              (CASE WHEN COALESCE(a.TOTAL_DEMAND_FTE, 0) > 0 OR COALESCE(a.FEATURE_COUNT, 0) > 0 THEN 1 ELSE 0 END)
            ) * 100.0 / 5.0
            AS FLOAT
          ) AS completeness_score,
	          CASE
	            WHEN (
	              (
		                (
		                  CASE
		                    WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.TEAMID,''))), '') IS NULL THEN 0
		                    WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.PROGRAMID, t_md.PROGRAMID, ''))), '') IS NULL THEN 0
		                    ELSE 1
		                  END
		                ) +
	                (CASE WHEN COALESCE(a.WORKFORCE_COST, 0) > 0 THEN 1 ELSE 0 END) +
	                (CASE
	                  WHEN t.TEAMID IS NULL THEN 1
                  WHEN EXISTS (
                    SELECT 1
                    FROM {_fq('TEAM_RATE_HISTORY')} rh
                    WHERE rh.TEAMID = t.TEAMID AND TRY_CONVERT(INT, rh.YEAR) = a.YEAR AND COALESCE(rh.XOM_RATE, 0) > 0
                  ) THEN 1
                  ELSE 0
                END) +
                (CASE WHEN COALESCE(a.NONWORKFORCE_COST, 0) > 0 THEN 1 ELSE 0 END) +
	                (CASE WHEN COALESCE(a.TOTAL_DEMAND_FTE, 0) > 0 OR COALESCE(a.FEATURE_COUNT, 0) > 0 THEN 1 ELSE 0 END)
              ) * 100.0 / 5.0
            ) >= 90 THEN 'HIGH'
	            WHEN (
	              (
		                (
		                  CASE
		                    WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.TEAMID,''))), '') IS NULL THEN 0
		                    WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.PROGRAMID, t_md.PROGRAMID, ''))), '') IS NULL THEN 0
		                    ELSE 1
		                  END
		                ) +
	                (CASE WHEN COALESCE(a.WORKFORCE_COST, 0) > 0 THEN 1 ELSE 0 END) +
	                (CASE
	                  WHEN t.TEAMID IS NULL THEN 1
                  WHEN EXISTS (
                    SELECT 1
                    FROM {_fq('TEAM_RATE_HISTORY')} rh
                    WHERE rh.TEAMID = t.TEAMID AND TRY_CONVERT(INT, rh.YEAR) = a.YEAR AND COALESCE(rh.XOM_RATE, 0) > 0
                  ) THEN 1
                  ELSE 0
                END) +
                (CASE WHEN COALESCE(a.NONWORKFORCE_COST, 0) > 0 THEN 1 ELSE 0 END) +
	                (CASE WHEN COALESCE(a.TOTAL_DEMAND_FTE, 0) > 0 OR COALESCE(a.FEATURE_COUNT, 0) > 0 THEN 1 ELSE 0 END)
              ) * 100.0 / 5.0
            ) >= 70 THEN 'MED'
            ELSE 'LOW'
          END AS level,
          LTRIM(RTRIM(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(
	                    CONCAT(
	                      CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.TEAMID,''))), '') IS NULL THEN '; Missing owning team assignment' ELSE '' END,
	                      CASE
	                        WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.TEAMID,''))), '') IS NULL THEN ''
	                        WHEN NULLIF(LTRIM(RTRIM(COALESCE(ag.PROGRAMID, t_md.PROGRAMID, ''))), '') IS NULL THEN '; Missing program mapping for team'
	                        ELSE ''
	                      END,
	                      CASE WHEN COALESCE(a.WORKFORCE_COST, 0) <= 0 THEN '; Missing workforce cost inputs' ELSE '' END,
	                      CASE
	                        WHEN t.TEAMID IS NULL THEN ''
                        WHEN EXISTS (
                          SELECT 1
                          FROM {_fq('TEAM_RATE_HISTORY')} rh
                          WHERE rh.TEAMID = t.TEAMID AND TRY_CONVERT(INT, rh.YEAR) = a.YEAR AND COALESCE(rh.XOM_RATE, 0) > 0
                        ) THEN ''
                        ELSE '; Missing rates'
                      END,
                      CASE WHEN COALESCE(a.NONWORKFORCE_COST, 0) <= 0 THEN '; Missing invoices/contracts' ELSE '' END,
	                      CASE WHEN COALESCE(a.TOTAL_DEMAND_FTE, 0) <= 0 AND COALESCE(a.FEATURE_COUNT, 0) <= 0 THEN '; Missing enhancements (demand/features)' ELSE '' END
                    ),
                    '; ', ''
                  ),
                  ';;', ';'
                ),
                '  ', ' '
              ),
              ';', ';'
            )
          )) AS missing_reasons
	        FROM agg a
	        LEFT JOIN {_fq('APPLICATION_GROUPS')} ag
	          ON UPPER(LTRIM(RTRIM(ag.GROUPNAME))) = UPPER(LTRIM(RTRIM(a.GROUPNAME)))
	        LEFT JOIN {_fq('TEAMS')} t_md
	          ON t_md.TEAMID = ag.TEAMID
	        LEFT JOIN {_fq('TEAMS')} t
	          ON UPPER(LTRIM(RTRIM(t.TEAMNAME))) = UPPER(LTRIM(RTRIM(a.TEAMNAME)))
	        """
    )
    return True


def list_vendors() -> pd.DataFrame:
    return fetch_df(f"SELECT VENDORID, VENDORNAME FROM {_fq('VENDORS')} ORDER BY VENDORNAME")


# MSP and ADO helpers (stubs/minimal)
def ensure_msp_support() -> bool:
    ensure_team_msp_rate_table()
    ensure_team_msp_assignments_table()
    ensure_msp_costs_view()
    return True


def ensure_team_msp_rate_table() -> None:
    # Ensure TEAM_MSP_RATE exists with expected columns used by pages (backward compatible)
    execute(
        f"""
        IF OBJECT_ID('{_svq('TEAM_MSP_RATE')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('TEAM_MSP_RATE')} (
            TEAMID NVARCHAR(255) PRIMARY KEY,
            MSP_ENABLED BIT DEFAULT 0,
            MSP_SIZE NVARCHAR(50) NULL,
            MSP_RATE_PER_PI DECIMAL(18,2) NULL,
            MSP_RATE_SMALL DECIMAL(18,2) NULL,
            MSP_RATE_MEDIUM DECIMAL(18,2) NULL,
            MSP_RATE_LARGE DECIMAL(18,2) NULL,
            MSP_RATE DECIMAL(18,2) NULL, -- legacy/compat
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL
          );
        END
        """
    )
    # Add columns if they are missing (idempotent schema evolution)
    for col_sql in [
        "IF COL_LENGTH('" + _fq('TEAM_MSP_RATE') + "','MSP_ENABLED') IS NULL ALTER TABLE " + _fq('TEAM_MSP_RATE') + " ADD MSP_ENABLED BIT DEFAULT 0",
        "IF COL_LENGTH('" + _fq('TEAM_MSP_RATE') + "','MSP_SIZE') IS NULL ALTER TABLE " + _fq('TEAM_MSP_RATE') + " ADD MSP_SIZE NVARCHAR(50) NULL",
        "IF COL_LENGTH('" + _fq('TEAM_MSP_RATE') + "','MSP_RATE_PER_PI') IS NULL ALTER TABLE " + _fq('TEAM_MSP_RATE') + " ADD MSP_RATE_PER_PI DECIMAL(18,2) NULL",
        "IF COL_LENGTH('" + _fq('TEAM_MSP_RATE') + "','MSP_RATE_SMALL') IS NULL ALTER TABLE " + _fq('TEAM_MSP_RATE') + " ADD MSP_RATE_SMALL DECIMAL(18,2) NULL",
        "IF COL_LENGTH('" + _fq('TEAM_MSP_RATE') + "','MSP_RATE_MEDIUM') IS NULL ALTER TABLE " + _fq('TEAM_MSP_RATE') + " ADD MSP_RATE_MEDIUM DECIMAL(18,2) NULL",
        "IF COL_LENGTH('" + _fq('TEAM_MSP_RATE') + "','MSP_RATE_LARGE') IS NULL ALTER TABLE " + _fq('TEAM_MSP_RATE') + " ADD MSP_RATE_LARGE DECIMAL(18,2) NULL",
        "IF COL_LENGTH('" + _fq('TEAM_MSP_RATE') + "','MSP_RATE') IS NULL ALTER TABLE " + _fq('TEAM_MSP_RATE') + " ADD MSP_RATE DECIMAL(18,2) NULL",
        "IF COL_LENGTH('" + _fq('TEAM_MSP_RATE') + "','UPDATED_BY') IS NULL ALTER TABLE " + _fq('TEAM_MSP_RATE') + " ADD UPDATED_BY NVARCHAR(255) NULL",
    ]:
        execute(col_sql)


def ensure_team_msp_assignments_table() -> None:
    # Fast path: if PK already matches (TEAMID, GROUPID, EFFECTIVE_YEAR) and GROUPID/EFFECTIVE_YEAR are NOT NULL, skip heavy work
    try:
        pk_info = fetch_df(
            f"""
            SELECT ic.key_ordinal, c.name AS colname, c.is_nullable
            FROM sys.key_constraints kc
            JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE kc.parent_object_id = OBJECT_ID('{_fq('TEAM_MSP_ASSIGNMENTS')}')
              AND kc.type = 'PK'
              AND kc.name = 'PK_TEAM_MSP_ASSIGNMENTS'
            ORDER BY ic.key_ordinal
            """
        )
        if pk_info is not None and not pk_info.empty:
            cols = [c.upper() for c in pk_info["colname"].tolist()]
            nullable_flags = list(pk_info["is_nullable"].tolist())
            if cols == ["TEAMID", "GROUPID", "EFFECTIVE_YEAR"] and all(n == 0 for n in nullable_flags):
                return
    except Exception:
        pass
    execute(
        f"""
        IF OBJECT_ID('{_svq('TEAM_MSP_ASSIGNMENTS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} (
            TEAMID NVARCHAR(255) NOT NULL,
            GROUPID NVARCHAR(255) NULL,
            EFFECTIVE_YEAR INT NOT NULL CONSTRAINT DF_TEAM_MSP_ASSIGNMENTS_YEAR DEFAULT 0,
            [YEAR] INT NULL,
            ITERATION_START TINYINT NULL,
            ITERATION_END TINYINT NULL,
            MSP_SIZE NVARCHAR(50) NULL,
            WEIGHT_PCT DECIMAL(9,2) NULL,
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            CONSTRAINT PK_TEAM_MSP_ASSIGNMENTS PRIMARY KEY (TEAMID, EFFECTIVE_YEAR)
          );
        END
        """
    )
    # If an older table exists without EFFECTIVE_YEAR, add it in its own batch
    execute(
        f"""
        IF COL_LENGTH('{_fq('TEAM_MSP_ASSIGNMENTS')}', 'EFFECTIVE_YEAR') IS NULL
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} ADD EFFECTIVE_YEAR INT NULL;
        END
        """
    )
    for col_sql in (
        f"""
        IF COL_LENGTH('{_fq('TEAM_MSP_ASSIGNMENTS')}', 'YEAR') IS NULL
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} ADD [YEAR] INT NULL;
        END
        """,
        f"""
        IF COL_LENGTH('{_fq('TEAM_MSP_ASSIGNMENTS')}', 'ITERATION_START') IS NULL
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} ADD ITERATION_START TINYINT NULL;
        END
        """,
        f"""
        IF COL_LENGTH('{_fq('TEAM_MSP_ASSIGNMENTS')}', 'ITERATION_END') IS NULL
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} ADD ITERATION_END TINYINT NULL;
        END
        """,
        f"""
        IF COL_LENGTH('{_fq('TEAM_MSP_ASSIGNMENTS')}', 'MSP_SIZE') IS NULL
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} ADD MSP_SIZE NVARCHAR(50) NULL;
        END
        """,
        f"""
        IF COL_LENGTH('{_fq('TEAM_MSP_ASSIGNMENTS')}', 'WEIGHT_PCT') IS NULL
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} ADD WEIGHT_PCT DECIMAL(9,2) NULL;
        END
        """,
        f"""
        IF COL_LENGTH('{_fq('TEAM_MSP_ASSIGNMENTS')}', 'UPDATED_AT') IS NULL
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} ADD UPDATED_AT DATETIME2 DEFAULT SYSDATETIME();
        END
        """,
    ):
        execute(col_sql)
    execute(
        f"""
        UPDATE {_fq('TEAM_MSP_ASSIGNMENTS')}
        SET WEIGHT_PCT = ISNULL(WEIGHT_PCT, 100.0)
        WHERE WEIGHT_PCT IS NULL;
        """
    )
    execute(
        f"""
        UPDATE {_fq('TEAM_MSP_ASSIGNMENTS')}
        SET ITERATION_START = COALESCE(NULLIF(ITERATION_START, 0), 1),
            ITERATION_END   = COALESCE(
                                 NULLIF(ITERATION_END, 0),
                                 CASE
                                   WHEN COALESCE(NULLIF(ITERATION_START, 0), 1) > 4 THEN COALESCE(NULLIF(ITERATION_START, 0), 1)
                                   ELSE 4
                                 END
                               ),
            [YEAR]          = COALESCE(NULLIF([YEAR], 0), EFFECTIVE_YEAR)
        WHERE ITERATION_START IS NULL OR ITERATION_END IS NULL OR [YEAR] IS NULL OR [YEAR] = 0;
        """
    )
    # Now, if column exists and is nullable, populate and set NOT NULL (separate batch to avoid compile errors)
    execute(
        f"""
        IF COL_LENGTH('{_fq('TEAM_MSP_ASSIGNMENTS')}', 'EFFECTIVE_YEAR') IS NOT NULL
        BEGIN
          DECLARE @is_nullable bit;
          SELECT @is_nullable = c.is_nullable FROM sys.columns c WHERE c.object_id = OBJECT_ID('{_fq('TEAM_MSP_ASSIGNMENTS')}') AND c.name = 'EFFECTIVE_YEAR';
          IF @is_nullable = 1
          BEGIN
            UPDATE {_fq('TEAM_MSP_ASSIGNMENTS')} SET EFFECTIVE_YEAR = 0 WHERE EFFECTIVE_YEAR IS NULL;
            ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} ALTER COLUMN EFFECTIVE_YEAR INT NOT NULL;
          END
        END
        """
    )
    # Normalize GROUPID and deduplicate before enforcing PK that allows multiple groups per team/year
    execute(
        f"""
        UPDATE {_fq('TEAM_MSP_ASSIGNMENTS')}
        SET GROUPID = '__UNASSIGNED__'
        WHERE GROUPID IS NULL OR LTRIM(RTRIM(GROUPID)) = '';
        """
    )
    # Enforce non-null GROUPID for PK compatibility
    execute(
        f"""
        IF COL_LENGTH('{_fq('TEAM_MSP_ASSIGNMENTS')}', 'GROUPID') IS NOT NULL
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')}
            ALTER COLUMN GROUPID NVARCHAR(255) NOT NULL;
        END
        """
    )
    execute(
        f"""
        ;WITH cte AS (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY TEAMID, GROUPID, COALESCE(NULLIF([YEAR],0), EFFECTIVE_YEAR)
                   ORDER BY UPDATED_AT DESC
                 ) AS rn
          FROM {_fq('TEAM_MSP_ASSIGNMENTS')}
        )
        DELETE FROM cte WHERE rn > 1;
        """
    )
    # Drop existing PK if present so we can redefine it to include GROUPID + YEAR
    execute(
        f"""
        IF EXISTS (
          SELECT 1 FROM sys.key_constraints
          WHERE parent_object_id = OBJECT_ID('{_fq('TEAM_MSP_ASSIGNMENTS')}')
            AND name = 'PK_TEAM_MSP_ASSIGNMENTS'
        )
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')} DROP CONSTRAINT PK_TEAM_MSP_ASSIGNMENTS;
        END
        """
    )
    # Ensure PK exists (separate batch; by now EFFECTIVE_YEAR exists) allowing multiple groups
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.key_constraints
          WHERE parent_object_id = OBJECT_ID('{_fq('TEAM_MSP_ASSIGNMENTS')}')
            AND type = 'PK'
        )
        BEGIN
          ALTER TABLE {_fq('TEAM_MSP_ASSIGNMENTS')}
            ADD CONSTRAINT PK_TEAM_MSP_ASSIGNMENTS PRIMARY KEY (TEAMID, GROUPID, EFFECTIVE_YEAR);
        END
        """
    )


def ensure_msp_costs_view() -> None:
    """(Re)build the VW_MSP_COSTS view with current assignment/rate data."""
    # Materialize MSP costs per iteration using team assignments and rate tables
    body = f"""
    WITH base AS (
      SELECT
        a.TEAMID,
        t.TEAMNAME AS MSP_TEAMNAME,
        g.TEAMID AS OWNER_TEAMID,
        towner.TEAMNAME AS OWNER_TEAMNAME,
        towner.PROGRAMID AS OWNER_PROGRAMID,
        p_owner.PROGRAMNAME AS OWNER_PROGRAMNAME,
        COALESCE(NULLIF(a.[YEAR], 0), NULLIF(a.EFFECTIVE_YEAR, 0), a.[YEAR], a.EFFECTIVE_YEAR) AS YEAR,
        COALESCE(NULLIF(a.ITERATION_START, 0), 1) AS ITERATION_START,
        COALESCE(NULLIF(a.ITERATION_END, 0), COALESCE(NULLIF(a.ITERATION_START, 0), 1)) AS ITERATION_END,
        LTRIM(RTRIM(a.MSP_SIZE)) AS MSP_SIZE,
        COALESCE(a.WEIGHT_PCT, 100.0) AS WEIGHT_PCT,
        a.GROUPID,
        t.TEAMNAME,
        t.PROGRAMID,
        p.PROGRAMNAME,
        g.GROUPNAME,
        COALESCE(r.MSP_RATE_SMALL, 0.0) AS MSP_RATE_SMALL,
        COALESCE(r.MSP_RATE_MEDIUM, 0.0) AS MSP_RATE_MEDIUM,
        COALESCE(r.MSP_RATE_LARGE, 0.0) AS MSP_RATE_LARGE,
        COALESCE(r.MSP_RATE_PER_PI, 0.0) AS MSP_RATE_PER_PI
      FROM {_fq('TEAM_MSP_ASSIGNMENTS')} a
      LEFT JOIN {_fq('TEAM_MSP_RATE')} r ON r.TEAMID = a.TEAMID
      LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = a.TEAMID
      LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = t.PROGRAMID
      LEFT JOIN {_fq('APPLICATION_GROUPS')} g ON g.GROUPID = a.GROUPID
      LEFT JOIN {_fq('TEAMS')} towner ON towner.TEAMID = g.TEAMID
      LEFT JOIN {_fq('PROGRAMS')} p_owner ON p_owner.PROGRAMID = COALESCE(g.PROGRAMID, towner.PROGRAMID)
    )
    SELECT
      'MSP' AS SOURCE,
      'NON_WORK_FORCE' AS COST_CATEGORY,
      CASE WHEN base.MSP_SIZE IS NULL OR LTRIM(RTRIM(base.MSP_SIZE)) = '' THEN 'MSP'
           ELSE CONCAT('MSP ', base.MSP_SIZE)
      END AS SUBCOMPONENT,
      COALESCE(base.OWNER_PROGRAMID, base.PROGRAMID) AS PROGRAMID,
      COALESCE(base.OWNER_PROGRAMNAME, base.PROGRAMNAME) AS PROGRAMNAME,
      COALESCE(base.OWNER_TEAMID, base.TEAMID) AS TEAMID,
      COALESCE(base.OWNER_TEAMNAME, base.TEAMNAME) AS TEAMNAME,
      base.GROUPID,
      base.GROUPNAME,
      TRY_CONVERT(INT, base.YEAR) AS YEAR,
      seq.PI AS PI,
      CAST(ROUND(
        COALESCE(
          CASE
            WHEN base.MSP_SIZE IS NULL OR LTRIM(RTRIM(base.MSP_SIZE)) = ''
              THEN COALESCE(base.MSP_RATE_PER_PI, base.MSP_RATE_MEDIUM, base.MSP_RATE_SMALL, base.MSP_RATE_LARGE)
            ELSE
              CASE UPPER(COALESCE(base.MSP_SIZE, ''))
                WHEN 'SMALL' THEN base.MSP_RATE_SMALL
                WHEN 'MEDIUM' THEN base.MSP_RATE_MEDIUM
                WHEN 'LARGE' THEN base.MSP_RATE_LARGE
                ELSE base.MSP_RATE_PER_PI
              END
          END,
          0.0
        ) * (COALESCE(base.WEIGHT_PCT, 100.0) / 100.0),
        2
      ) AS DECIMAL(18,2)) AS AMOUNT,
      CAST(NULL AS NVARCHAR(255)) AS ADO_FEATURE_ID,
      CAST(NULL AS NVARCHAR(255)) AS ADO_FEATURE_TITLE,
      CAST(NULL AS NVARCHAR(255)) AS ADO_FEATURE_STATE,
      CAST(NULL AS NVARCHAR(255)) AS INVESTMENT_DIMENSION,
      COALESCE(
        CASE
          WHEN base.MSP_SIZE IS NULL OR LTRIM(RTRIM(base.MSP_SIZE)) = ''
            THEN COALESCE(base.MSP_RATE_PER_PI, base.MSP_RATE_MEDIUM, base.MSP_RATE_SMALL, base.MSP_RATE_LARGE)
          ELSE
            CASE UPPER(COALESCE(base.MSP_SIZE, ''))
              WHEN 'SMALL' THEN base.MSP_RATE_SMALL
              WHEN 'MEDIUM' THEN base.MSP_RATE_MEDIUM
              WHEN 'LARGE' THEN base.MSP_RATE_LARGE
              ELSE base.MSP_RATE_PER_PI
            END
        END,
        0.0
      ) AS MSP_RATE_USED,
      base.MSP_SIZE,
      base.MSP_TEAMNAME,
      base.OWNER_TEAMID,
      base.OWNER_TEAMNAME
    FROM base
    CROSS APPLY (
      SELECT base.ITERATION_START + v.n AS PI
      FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS v(n)
      WHERE base.ITERATION_START + v.n <= base.ITERATION_END
    ) seq
    WHERE COALESCE(
          CASE
            WHEN base.MSP_SIZE IS NULL OR LTRIM(RTRIM(base.MSP_SIZE)) = ''
              THEN COALESCE(base.MSP_RATE_PER_PI, base.MSP_RATE_MEDIUM, base.MSP_RATE_SMALL, base.MSP_RATE_LARGE)
            ELSE
              CASE UPPER(COALESCE(base.MSP_SIZE, ''))
                WHEN 'SMALL' THEN base.MSP_RATE_SMALL
                WHEN 'MEDIUM' THEN base.MSP_RATE_MEDIUM
                WHEN 'LARGE' THEN base.MSP_RATE_LARGE
                ELSE base.MSP_RATE_PER_PI
              END
          END,
          0.0
        ) <> 0
    """
    try:
        _create_or_alter_view('VW_MSP_COSTS', body)
    except Exception:
        # Fallback if CREATE OR ALTER VIEW is unavailable
        try:
            execute("SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
        except Exception:
            pass
        try:
            execute(
                f"IF OBJECT_ID('{_svq('VW_MSP_COSTS')}', 'V') IS NOT NULL "
                f"DROP VIEW {_svq('VW_MSP_COSTS')};"
            )
        except Exception:
            pass
        execute(f"CREATE VIEW {_svq('VW_MSP_COSTS')} AS\n{body}")


def ensure_msp_view_placeholder() -> None:
    """Backward-compatible alias for legacy callers."""
    ensure_msp_costs_view()

def ensure_contracts_table() -> None:
    """Minimal contract metadata to enable dynamic forecasts (one per App×Team).

    Columns:
      - CONTRACT_ID (PK)
      - APPLICATIONID, TEAMID (unique pair)
      - START_FY, END_FY (inclusive fiscal years)
      - RENEWAL_MONTH (1-12; default 1 for Jan)
      - ANNUAL_AMOUNT (base amount for START_FY)
      - ESCALATION_PCT (annual escalation %, applied from START_FY forward)
      - STATUS (Active/Terminated)
      - AGREEMENT_NUMBER (optional reference)
    """
    execute(
        f"""
        IF OBJECT_ID('{_svq('CONTRACTS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('CONTRACTS')} (
            CONTRACT_ID NVARCHAR(255) PRIMARY KEY,
            APPLICATIONID NVARCHAR(255) NOT NULL,
            TEAMID NVARCHAR(255) NOT NULL,
            START_FY INT NOT NULL,
            END_FY INT NOT NULL,
            RENEWAL_MONTH TINYINT NULL,
            ANNUAL_AMOUNT DECIMAL(18,2) NOT NULL,
            ESCALATION_PCT DECIMAL(9,4) NULL,
            STATUS NVARCHAR(32) NULL,
            AGREEMENT_NUMBER NVARCHAR(255) NULL,
            COMPANY_CODE NVARCHAR(255) NULL,
            COST_CENTER NVARCHAR(255) NULL,
            SERVICE_TYPE NVARCHAR(255) NULL,
            CONTRACT_RENEWAL_DATE DATE NULL,
            INVOICE_RENEWAL_DATE  DATE NULL,
          TOTAL_CONTRACT_COST DECIMAL(18,2) NULL,
            CREATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL
          );
        END
        """
    )
    # Unique logical contract per App×Team (latest overwrite via MERGE helper)
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes WHERE name = 'UQ_CONTRACTS_APP_TEAM' AND object_id = OBJECT_ID('{_fq('CONTRACTS')}'))
        BEGIN
          CREATE UNIQUE INDEX UQ_CONTRACTS_APP_TEAM ON {_fq('CONTRACTS')}(APPLICATIONID, TEAMID);
        END
        """
    )
    # Add new columns if missing (migrations)
    for col, typ in (
        ("COMPANY_CODE", "NVARCHAR(255)"),
        ("COST_CENTER", "NVARCHAR(255)"),
        ("SERVICE_TYPE", "NVARCHAR(255)"),
        ("CONTRACT_RENEWAL_DATE", "DATE"),
        ("INVOICE_RENEWAL_DATE", "DATE"),
        ("TOTAL_CONTRACT_COST", "DECIMAL(18,2)"),
        ("UPDATED_AT", "DATETIME2"),
        ("UPDATED_BY", "NVARCHAR(255)"),
    ):
        execute(
            f"""
            IF COL_LENGTH('{_fq('CONTRACTS')}', '{col}') IS NULL
            BEGIN
              ALTER TABLE {_fq('CONTRACTS')} ADD {col} {typ} NULL;
            END
            """
        )

@_versioned_write("CONTRACTS.UPSERT")
def upsert_contract(
    contract_id: str,
    application_id: str,
    team_id: str,
    start_fy: int,
    end_fy: int,
    renewal_month: int = 1,
    annual_amount: float = 0.0,
    escalation_pct: float = 0.0,
    status: str = 'Active',
    agreement_number: Optional[str] = None,
    company_code: Optional[str] = None,
    cost_center: Optional[str] = None,
    service_type: Optional[str] = None,
    contract_renewal_date: Optional[str] = None,
    invoice_renewal_date: Optional[str] = None,
    total_contract_cost: Optional[float] = None,
    updated_by: Optional[str] = None,
) -> None:
    ensure_contracts_table()
    execute(
        f"""
        MERGE INTO {_fq('CONTRACTS')} AS t
        USING (
          SELECT ? AS CONTRACT_ID, ? AS APPLICATIONID, ? AS TEAMID,
                 ? AS START_FY, ? AS END_FY, ? AS RENEWAL_MONTH,
                 ? AS ANNUAL_AMOUNT, ? AS ESCALATION_PCT, ? AS STATUS,
                 ? AS AGREEMENT_NUMBER,
                 ? AS COMPANY_CODE, ? AS COST_CENTER, ? AS SERVICE_TYPE, ? AS CONTRACT_RENEWAL_DATE,
                 ? AS INVOICE_RENEWAL_DATE,
                 ? AS TOTAL_CONTRACT_COST,
                 SYSDATETIME() AS UPDATED_AT,
                 ? AS UPDATED_BY
        ) AS s
        ON t.APPLICATIONID = s.APPLICATIONID AND t.TEAMID = s.TEAMID
        WHEN MATCHED THEN UPDATE SET
          CONTRACT_ID = s.CONTRACT_ID,
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
          UPDATED_AT = s.UPDATED_AT,
          UPDATED_BY = s.UPDATED_BY
        WHEN NOT MATCHED THEN INSERT (
          CONTRACT_ID, APPLICATIONID, TEAMID, START_FY, END_FY, RENEWAL_MONTH,
          ANNUAL_AMOUNT, ESCALATION_PCT, STATUS, AGREEMENT_NUMBER,
          COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE, TOTAL_CONTRACT_COST,
          UPDATED_AT, UPDATED_BY
        ) VALUES (
          s.CONTRACT_ID, s.APPLICATIONID, s.TEAMID, s.START_FY, s.END_FY, s.RENEWAL_MONTH,
          s.ANNUAL_AMOUNT, s.ESCALATION_PCT, s.STATUS, s.AGREEMENT_NUMBER,
          s.COMPANY_CODE, s.COST_CENTER, s.SERVICE_TYPE, s.CONTRACT_RENEWAL_DATE, s.INVOICE_RENEWAL_DATE, s.TOTAL_CONTRACT_COST,
          s.UPDATED_AT, s.UPDATED_BY
        );
        """,
        (
            contract_id,
            application_id,
            team_id,
            int(start_fy),
            int(end_fy),
            int(renewal_month or 1),
            annual_amount,
            escalation_pct,
            status,
            agreement_number,
            company_code,
            cost_center,
            service_type,
            contract_renewal_date,
            invoice_renewal_date,
            total_contract_cost,
            updated_by,
        ),
    )

    # After upsert, propagate contract-level Company Code and Cost Center
    # to planned recurring invoices for the same (Application, Team) when
    # those invoice fields are currently NULL. Do not overwrite non-NULL
    # invoice values; Completed invoices are never modified.
    try:
        execute(
            f"""
            UPDATE i
            SET i.COMPANY_CODE = COALESCE(i.COMPANY_CODE, s.COMPANY_CODE),
                i.COST_CENTER  = COALESCE(i.COST_CENTER,  s.COST_CENTER)
            FROM {_fq('INVOICES')} i
            JOIN {_fq('CONTRACTS')} s
              ON s.APPLICATIONID = i.APPLICATIONID AND s.TEAMID = i.TEAMID
            WHERE s.CONTRACT_ID = %s
              AND ISNULL(i.INVOICE_TYPE, 'Recurring Invoice') = 'Recurring Invoice'
              AND i.STATUS = 'Planned'
            """,
            (contract_id,),
        )
    except Exception:
        pass
@_versioned_write("INVOICES.SYNC_CONTRACT")
def sync_contract_invoices(contract_id: str) -> int:
    """Create/Update planned recurring invoices for each FY of a contract and clean up stale ones.

    - Updates only rows with STATUS='Planned' to avoid overwriting Completed actuals.
    - Inserts planned rows for fiscal years within the contract window.
    - Deletes planned recurring invoices outside the contract START_FY..END_FY window for the same App×Team.

    Returns number of rows affected (best-effort approximation: number of FY rows processed).
    """
    ensure_contracts_table()
    # Build a derived set of rows (one per FY) with seeded fields
    sql = f"""
    WITH c AS (
      SELECT CONTRACT_ID, APPLICATIONID, TEAMID, START_FY, END_FY,
             ISNULL(NULLIF(RENEWAL_MONTH,0), 1) AS RENEWAL_MONTH,
             ANNUAL_AMOUNT, ISNULL(ESCALATION_PCT,0) AS ESCALATION_PCT,
             UPPER(ISNULL(STATUS,'Active')) AS STATUS,
             AGREEMENT_NUMBER, COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE
      FROM {_fq('CONTRACTS')}
      WHERE CONTRACT_ID = %s
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
             -- Prefer explicit CONTRACT_RENEWAL_DATE month/day if present; fallback to month=RENEWAL_MONTH, day=1
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
      -- keep Planned as Planned; do not overwrite Completed
      t.STATUS = t.STATUS,
      t.PRODUCT_OWNER = t.PRODUCT_OWNER, -- preserve if present
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
    );
    """
    # Execute; return count of FY rows processed
    # Best effort: compute count via Python by reading contract then computing span
    df = fetch_df(f"SELECT APPLICATIONID, TEAMID, START_FY, END_FY FROM {_fq('CONTRACTS')} WHERE CONTRACT_ID=%s", (contract_id,))
    span = 0
    invalid_window = False
    if df is not None and not df.empty:
        try:
            app_id = df.iloc[0]["APPLICATIONID"]
            team_id = df.iloc[0]["TEAMID"]
            start_fy = int(df.iloc[0]["START_FY"])
            end_fy = int(df.iloc[0]["END_FY"])
            if end_fy < start_fy:
                invalid_window = True
            else:
                span = end_fy - start_fy + 1
        except Exception:
            span = 0
    if invalid_window:
        # Avoid running destructive sync if contract window is invalid
        return 0
    execute(sql, (contract_id,))
    # Delete any stale planned recurring invoices for this App×Team outside the contract window
    try:
        if df is not None and not df.empty:
            execute(
                f"""
                DELETE FROM {_fq('INVOICES')}
                WHERE APPLICATIONID = %s AND TEAMID = %s
                  AND ISNULL(INVOICE_TYPE, 'Recurring Invoice') = 'Recurring Invoice'
                  AND STATUS = 'Planned'
                  AND (FISCAL_YEAR < %s OR FISCAL_YEAR > %s)
                """,
                (app_id, team_id, start_fy, end_fy),
            )
    except Exception:
        # Best-effort cleanup; ignore failures
        pass
    # Propagate contract-level Company Code and Cost Center to planned recurring invoices when missing
    try:
        execute(
            f"""
            UPDATE i
            SET i.COMPANY_CODE = COALESCE(i.COMPANY_CODE, c.COMPANY_CODE),
                i.COST_CENTER  = COALESCE(i.COST_CENTER,  c.COST_CENTER)
            FROM {_fq('INVOICES')} i
            JOIN {_fq('CONTRACTS')} c
              ON c.APPLICATIONID = i.APPLICATIONID AND c.TEAMID = i.TEAMID
            WHERE c.CONTRACT_ID = %s
              AND ISNULL(i.INVOICE_TYPE, 'Recurring Invoice') = 'Recurring Invoice'
              AND i.STATUS = 'Planned'
            """,
            (contract_id,),
        )
    except Exception:
        pass
    return span

def ensure_invoice_forecast_views() -> None:
    """Create forecast and union views based on CONTRACTS.

    - VW_INVOICE_FORECAST synthesizes one row per FY between START_FY..END_FY
      for each contract, excluding years where an actual Recurring Invoice
      already exists in INVOICES.
    - VW_INVOICES_ACTUAL_AND_FORECAST unions actuals with forecasts, matching
      the shape of INVOICES for drop-in use in read-only queries.
    """
    ensure_contracts_table()
    ensure_tables()  # ensures INVOICES exists

    # Forecast view using recursive CTE to generate fiscal years
    body_forecast = f"""
      WITH c AS (
        SELECT CONTRACT_ID, APPLICATIONID, TEAMID,
               START_FY, END_FY,
               ISNULL(NULLIF(RENEWAL_MONTH,0), 1) AS RENEWAL_MONTH,
               ISNULL(ANNUAL_AMOUNT, 0) AS ANNUAL_AMOUNT,
               ISNULL(ESCALATION_PCT, 0) AS ESCALATION_PCT,
               UPPER(ISNULL(STATUS,'Active')) AS STATUS,
               AGREEMENT_NUMBER,
               COMPANY_CODE, COST_CENTER, SERVICE_TYPE, CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE
        FROM {_fq('CONTRACTS')}
      ),
      yrs (CONTRACT_ID, FY) AS (
        SELECT c.CONTRACT_ID, c.START_FY AS FY FROM c
        UNION ALL
        SELECT y.CONTRACT_ID, y.FY + 1 FROM yrs y
        JOIN c ON c.CONTRACT_ID = y.CONTRACT_ID
        WHERE y.FY < c.END_FY
      ),
      j AS (
        SELECT y.FY, c.*
        FROM yrs y
        JOIN c   ON c.CONTRACT_ID = y.CONTRACT_ID
      ),
      excl AS (
        SELECT DISTINCT APPLICATIONID, TEAMID, FISCAL_YEAR
        FROM {_fq('INVOICES')}
        WHERE ISNULL(INVOICE_TYPE, 'Recurring Invoice') = 'Recurring Invoice'
      )
      SELECT
        CAST(NULL AS NVARCHAR(255)) AS INVOICEID,
        j.APPLICATIONID,
        j.TEAMID,
        CAST(NULL AS DATE) AS INVOICEDATE,
        DATEFROMPARTS(j.FY,
          COALESCE(MONTH(j.INVOICE_RENEWAL_DATE), MONTH(j.CONTRACT_RENEWAL_DATE), j.RENEWAL_MONTH, 1),
          COALESCE(DAY(j.INVOICE_RENEWAL_DATE), DAY(j.CONTRACT_RENEWAL_DATE), 1)
        ) AS RENEWALDATE,
        CAST(ROUND(j.ANNUAL_AMOUNT * POWER(1 + (j.ESCALATION_PCT/100.0), j.FY - j.START_FY), 2) AS DECIMAL(18,2)) AS AMOUNT,
        CAST('Planned' AS NVARCHAR(255)) AS STATUS,
        j.FY AS FISCAL_YEAR,
        CAST(NULL AS NVARCHAR(255)) AS PROGRAMID_AT_BOOKING,
        CAST(NULL AS NVARCHAR(255)) AS VENDORID_AT_BOOKING,
        CAST(NULL AS NVARCHAR(255)) AS GROUPID,
        CAST(NULL AS NVARCHAR(255)) AS GROUPID_AT_BOOKING,
        CAST(NULL AS NVARCHAR(255)) AS PRODUCT_OWNER,
        CAST(NULL AS DECIMAL(18,2)) AS AMOUNT_NEXT_YEAR,
        CAST(CASE WHEN j.STATUS = 'ACTIVE' THEN 1 ELSE 0 END AS BIT) AS CONTRACT_ACTIVE,
        j.COMPANY_CODE AS COMPANY_CODE,
        j.COST_CENTER AS COST_CENTER,
        CAST(NULL AS NVARCHAR(255)) AS SERIAL_NUMBER,
        CAST(NULL AS NVARCHAR(255)) AS WORK_ORDER,
        CAST(j.AGREEMENT_NUMBER AS NVARCHAR(255)) AS AGREEMENT_NUMBER,
        j.END_FY AS CONTRACT_DUE,
        j.SERVICE_TYPE AS SERVICE_TYPE,
        CAST(NULL AS NVARCHAR(MAX)) AS NOTES,
        CAST(NULL AS NVARCHAR(255)) AS ROLLOVER_BATCH_ID,
        CAST(NULL AS INT) AS ROLLED_OVER_FROM_YEAR,
        CAST('Recurring Invoice' AS NVARCHAR(255)) AS INVOICE_TYPE
      FROM j
      LEFT JOIN excl e
        ON e.APPLICATIONID = j.APPLICATIONID AND e.TEAMID = j.TEAMID AND e.FISCAL_YEAR = j.FY
      WHERE e.FISCAL_YEAR IS NULL
    """
    _create_or_alter_view('VW_INVOICE_FORECAST', body_forecast)

    # Union view aligning to INVOICES shape (column order matters for SELECT *)
    body_union = f"""
      SELECT
        i.INVOICEID, i.APPLICATIONID, i.TEAMID, i.INVOICEDATE, i.RENEWALDATE, i.AMOUNT, i.STATUS,
        i.FISCAL_YEAR, i.PROGRAMID_AT_BOOKING, i.VENDORID_AT_BOOKING, i.GROUPID, i.GROUPID_AT_BOOKING,
        i.PRODUCT_OWNER, i.AMOUNT_NEXT_YEAR, i.CONTRACT_ACTIVE, i.COMPANY_CODE, i.COST_CENTER, i.SERIAL_NUMBER,
        i.WORK_ORDER, i.AGREEMENT_NUMBER, i.CONTRACT_DUE, i.SERVICE_TYPE, i.NOTES,
        i.ROLLOVER_BATCH_ID, i.ROLLED_OVER_FROM_YEAR, COALESCE(i.INVOICE_TYPE, 'Recurring Invoice') AS INVOICE_TYPE
      FROM {_svq('INVOICES')} i
      UNION ALL
      SELECT
        f.INVOICEID, f.APPLICATIONID, f.TEAMID, f.INVOICEDATE, f.RENEWALDATE, f.AMOUNT, f.STATUS,
        f.FISCAL_YEAR, f.PROGRAMID_AT_BOOKING, f.VENDORID_AT_BOOKING, f.GROUPID, f.GROUPID_AT_BOOKING,
        f.PRODUCT_OWNER, f.AMOUNT_NEXT_YEAR, f.CONTRACT_ACTIVE, f.COMPANY_CODE, f.COST_CENTER, f.SERIAL_NUMBER,
        f.WORK_ORDER, f.AGREEMENT_NUMBER, f.CONTRACT_DUE, f.SERVICE_TYPE, f.NOTES,
        f.ROLLOVER_BATCH_ID, f.ROLLED_OVER_FROM_YEAR, f.INVOICE_TYPE
      FROM {_svq('VW_INVOICE_FORECAST')} f
    """
    _create_or_alter_view('VW_INVOICES_ACTUAL_AND_FORECAST', body_union)


def ensure_performance_indexes() -> None:
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_FEATURES')}', 'ADO_YEAR') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'FEATURE_ID') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ITERATION_LEVEL3') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ITERATION_SK') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'PROGRAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_VARIANT_KEY') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'APP_NAME_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'STORY_POINTS') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'EFFORT_POINTS') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'STATE') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'BUSINESS_VALUE') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_ADO_FEATURES_ADO_YEAR'
              AND object_id = OBJECT_ID('{_fq('ADO_FEATURES')}')
          )
        BEGIN
          CREATE INDEX IX_ADO_FEATURES_ADO_YEAR ON {_fq('ADO_FEATURES')}(ADO_YEAR)
          INCLUDE (FEATURE_ID, ITERATION_LEVEL3, ITERATION_SK, PROGRAM_RAW, TEAM_RAW, TEAM_VARIANT_KEY,
                   APP_NAME_RAW, STORY_POINTS, EFFORT_POINTS, STATE, BUSINESS_VALUE);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_FEATURES')}', 'ADO_YEAR') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ITERATION_LEVEL3') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'FEATURE_ID') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'PROGRAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_VARIANT_KEY') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'APP_NAME_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'STORY_POINTS') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'EFFORT_POINTS') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'STATE') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_ADO_FEATURES_YEAR_PI'
              AND object_id = OBJECT_ID('{_fq('ADO_FEATURES')}')
          )
        BEGIN
          CREATE INDEX IX_ADO_FEATURES_YEAR_PI ON {_fq('ADO_FEATURES')}(ADO_YEAR, ITERATION_LEVEL3)
          INCLUDE (FEATURE_ID, PROGRAM_RAW, TEAM_RAW, TEAM_VARIANT_KEY, APP_NAME_RAW, STORY_POINTS,
                   EFFORT_POINTS, STATE);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_VARIANT_KEY') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'FEATURE_ID') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ADO_YEAR') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ITERATION_LEVEL3') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ITERATION_SK') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'PROGRAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'APP_NAME_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'STORY_POINTS') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'EFFORT_POINTS') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_ADO_FEATURES_TEAMVAR'
              AND object_id = OBJECT_ID('{_fq('ADO_FEATURES')}')
          )
        BEGIN
          CREATE INDEX IX_ADO_FEATURES_TEAMVAR ON {_fq('ADO_FEATURES')}(TEAM_VARIANT_KEY)
          INCLUDE (FEATURE_ID, ADO_YEAR, ITERATION_LEVEL3, ITERATION_SK, PROGRAM_RAW, TEAM_RAW,
                   APP_NAME_RAW, STORY_POINTS, EFFORT_POINTS);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_FEATURES')}', 'APP_NAME_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'FEATURE_ID') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ADO_YEAR') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ITERATION_LEVEL3') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'PROGRAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_VARIANT_KEY') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'STORY_POINTS') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'EFFORT_POINTS') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_ADO_FEATURES_APP_RAW'
              AND object_id = OBJECT_ID('{_fq('ADO_FEATURES')}')
          )
        BEGIN
          CREATE INDEX IX_ADO_FEATURES_APP_RAW ON {_fq('ADO_FEATURES')}(APP_NAME_RAW)
          INCLUDE (FEATURE_ID, ADO_YEAR, ITERATION_LEVEL3, PROGRAM_RAW, TEAM_RAW, TEAM_VARIANT_KEY,
                   STORY_POINTS, EFFORT_POINTS);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}', 'ADO_TEAM') IS NOT NULL
          AND COL_LENGTH('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}', 'ADO_TEAM_KEY') IS NOT NULL
          AND COL_LENGTH('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}', 'TEAMID') IS NOT NULL
          AND COL_LENGTH('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}', 'PROGRAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}', 'AREA_LEVEL3_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}', 'AREA_LEVEL4_RAW') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_MAP_ADO_TEAM_ADO_TEAM'
              AND object_id = OBJECT_ID('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}')
          )
        BEGIN
          CREATE INDEX IX_MAP_ADO_TEAM_ADO_TEAM ON {_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}(ADO_TEAM)
          INCLUDE (ADO_TEAM_KEY, TEAMID, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}', 'ADO_TEAM_KEY') IS NOT NULL
          AND COL_LENGTH('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}', 'TEAMID') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_MAP_ADO_TEAM_ADO_TEAM_KEY'
              AND object_id = OBJECT_ID('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}')
          )
        BEGIN
          CREATE INDEX IX_MAP_ADO_TEAM_ADO_TEAM_KEY ON {_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}(ADO_TEAM_KEY)
          INCLUDE (TEAMID, ADO_TEAM);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_FEATURES')}', 'STATE') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'CHANGED_AT') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'STORY_POINTS') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_VARIANT_KEY') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'TEAM_RAW') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ITERATION_PATH') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ITERATION_SK') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ADO_YEAR') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'PI_LABEL') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_FEATURES')}', 'ITERATION_LEVEL3') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_ADO_FEATURES_STATE_CHANGED'
              AND object_id = OBJECT_ID('{_fq('ADO_FEATURES')}')
          )
        BEGIN
          CREATE INDEX IX_ADO_FEATURES_STATE_CHANGED ON {_fq('ADO_FEATURES')}(STATE, CHANGED_AT)
          INCLUDE (FEATURE_ID, STORY_POINTS, TEAM_VARIANT_KEY, TEAM_RAW, ITERATION_PATH, ITERATION_SK, ADO_YEAR, PI_LABEL, ITERATION_LEVEL3);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'ITERATION_GRAIN') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'ITERATION_SK') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'ITERATION_LEVEL3') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'YEAR') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'ITERATION_PATH') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_ADO_ITER_CAL_GRAIN_SK'
              AND object_id = OBJECT_ID('{_fq('ADO_ITERATION_CALENDAR')}')
          )
        BEGIN
          CREATE INDEX IX_ADO_ITER_CAL_GRAIN_SK ON {_fq('ADO_ITERATION_CALENDAR')}(ITERATION_GRAIN, ITERATION_SK)
          INCLUDE (ITERATION_LEVEL3, YEAR, ITERATION_PATH);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'ITERATION_GRAIN') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'ITERATION_PATH') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'ITERATION_LEVEL3') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'YEAR') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', 'ITERATION_SK') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_ADO_ITER_CAL_GRAIN_PATH'
              AND object_id = OBJECT_ID('{_fq('ADO_ITERATION_CALENDAR')}')
          )
        BEGIN
          CREATE INDEX IX_ADO_ITER_CAL_GRAIN_PATH ON {_fq('ADO_ITERATION_CALENDAR')}(ITERATION_GRAIN, ITERATION_PATH)
          INCLUDE (ITERATION_LEVEL3, YEAR, ITERATION_SK);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_APP_CANDIDATES')}', 'STATUS') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_APP_CANDIDATES')}', 'UPDATED_AT') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_APP_CANDIDATES')}', 'FEATURE_COUNT') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_APP_CANDIDATES')}', 'DERIVED_FTE_SUM') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_ADO_APP_CANDIDATES_STATUS'
              AND object_id = OBJECT_ID('{_fq('ADO_APP_CANDIDATES')}')
          )
        BEGIN
          CREATE INDEX IX_ADO_APP_CANDIDATES_STATUS ON {_fq('ADO_APP_CANDIDATES')}(STATUS)
          INCLUDE (UPDATED_AT, FEATURE_COUNT, DERIVED_FTE_SUM);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_APP_CANDIDATES')}', 'UPDATED_AT') IS NOT NULL
          AND COL_LENGTH('{_fq('ADO_APP_CANDIDATES')}', 'STATUS') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_ADO_APP_CANDIDATES_UPDATED_AT'
              AND object_id = OBJECT_ID('{_fq('ADO_APP_CANDIDATES')}')
          )
        BEGIN
          CREATE INDEX IX_ADO_APP_CANDIDATES_UPDATED_AT ON {_fq('ADO_APP_CANDIDATES')}(UPDATED_AT)
          INCLUDE (STATUS);
        END
        """
    )
    _trace("Indexes ensured")


def ensure_ado_minimal_tables() -> None:
    # Minimal structures used by analytics views when ADO sync is not wired
    # ADO_FEATURES base table
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_FEATURES')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_FEATURES')} (
            FEATURE_ID NVARCHAR(255) PRIMARY KEY,
            TITLE NVARCHAR(400),
            STATE NVARCHAR(100),
            TEAM_RAW NVARCHAR(400),
            APP_NAME_RAW NVARCHAR(400),
            EFFORT_POINTS FLOAT NULL,
            STORY_POINTS FLOAT NULL,
            BUSINESS_VALUE FLOAT NULL,
            ITERATION_LEVEL3 NVARCHAR(400) NULL,
            ITERATION_LEVEL3_RAW NVARCHAR(255) NULL,
            PI_LABEL NVARCHAR(64) NULL,
            ITERATION_PATH NVARCHAR(400),
            ITERATION_SK NVARCHAR(100) NULL,
            CREATED_AT DATETIME2 NULL,
            CHANGED_AT DATETIME2 NULL,
            PARENT_ID BIGINT NULL,
            EPIC_ID BIGINT NULL,
            EPIC_TITLE NVARCHAR(500) NULL,
            EPIC_STATE NVARCHAR(100) NULL,
            ADO_YEAR INT NULL,
            INVESTMENT_DIMENSION NVARCHAR(255) NULL,
            PROGRAM_RAW NVARCHAR(400) NULL,
            AREA_LEVEL1_RAW NVARCHAR(255) NULL,
            AREA_LEVEL2_RAW NVARCHAR(400) NULL,
            AREA_LEVEL3_RAW NVARCHAR(400) NULL,
            AREA_LEVEL4_RAW NVARCHAR(400) NULL,
            AREA_PATH_RAW NVARCHAR(600) NULL,
            TEAM_VARIANT_KEY NVARCHAR(900) NULL
          );
        END
        """
    )
    # Backfill new columns for existing deployments
    for col, col_type in (
        ("STORY_POINTS", "FLOAT"),
        ("BUSINESS_VALUE", "FLOAT"),
        ("ITERATION_LEVEL3", "NVARCHAR(400)"),
        ("ITERATION_LEVEL3_RAW", "NVARCHAR(255)"),
        ("PI_LABEL", "NVARCHAR(64)"),
        ("ITERATION_PATH", "NVARCHAR(400)"),
        ("ITERATION_SK", "NVARCHAR(100)"),
        ("PARENT_ID", "BIGINT"),
        ("EPIC_ID", "BIGINT"),
        ("EPIC_TITLE", "NVARCHAR(500)"),
        ("EPIC_STATE", "NVARCHAR(100)"),
        ("PROGRAM_RAW", "NVARCHAR(400)"),
        ("AREA_LEVEL1_RAW", "NVARCHAR(255)"),
        ("AREA_LEVEL2_RAW", "NVARCHAR(400)"),
        ("AREA_LEVEL3_RAW", "NVARCHAR(400)"),
        ("AREA_LEVEL4_RAW", "NVARCHAR(400)"),
        ("AREA_PATH_RAW", "NVARCHAR(600)"),
        ("TEAM_VARIANT_KEY", "NVARCHAR(900)"),
    ):
        execute(
            f"""
            IF COL_LENGTH('{_fq('ADO_FEATURES')}', '{col}') IS NULL
            BEGIN
              ALTER TABLE {_fq('ADO_FEATURES')} ADD {col} {col_type} NULL;
            END
            """
        )
    # Best-effort backfill variant key
    execute(
        f"""
        UPDATE {_fq('ADO_FEATURES')}
        SET TEAM_VARIANT_KEY = NULLIF(
            UPPER(CONCAT_WS('|',
                NULLIF(LTRIM(RTRIM(PROGRAM_RAW)), ''),
                NULLIF(LTRIM(RTRIM(AREA_LEVEL3_RAW)), ''),
                NULLIF(LTRIM(RTRIM(AREA_LEVEL4_RAW)), ''),
                CASE
                  WHEN NULLIF(LTRIM(RTRIM(TEAM_RAW)), '') IS NULL THEN NULLIF(LTRIM(RTRIM(AREA_LEVEL4_RAW)), '')
                  WHEN NULLIF(LTRIM(RTRIM(AREA_LEVEL4_RAW)), '') IS NULL THEN NULLIF(LTRIM(RTRIM(TEAM_RAW)), '')
                  WHEN UPPER(LTRIM(RTRIM(TEAM_RAW))) = UPPER(LTRIM(RTRIM(AREA_LEVEL4_RAW))) THEN NULL
                  WHEN UPPER(LTRIM(RTRIM(TEAM_RAW))) = UPPER(LTRIM(RTRIM(AREA_LEVEL3_RAW))) THEN NULLIF(LTRIM(RTRIM(AREA_LEVEL4_RAW)), '')
                  ELSE NULLIF(LTRIM(RTRIM(TEAM_RAW)), '')
                END
            )), ''
        )
        WHERE TEAM_VARIANT_KEY IS NULL
           OR LTRIM(RTRIM(TEAM_VARIANT_KEY)) = ''
           OR TEAM_VARIANT_KEY LIKE '%\\%'
           OR TEAM_VARIANT_KEY LIKE '%||%';
        """
    )
    # Best-effort backfill for raw/normalized iteration labels.
    execute(
        f"""
        UPDATE {_fq('ADO_FEATURES')}
        SET ITERATION_LEVEL3_RAW = COALESCE(ITERATION_LEVEL3_RAW, ITERATION_LEVEL3),
            PI_LABEL = COALESCE(
              PI_LABEL,
              CASE
                WHEN ITERATION_LEVEL3 IS NULL THEN NULL
                WHEN CHARINDEX(' (', ITERATION_LEVEL3) > 0
                  THEN LEFT(ITERATION_LEVEL3, CHARINDEX(' (', ITERATION_LEVEL3) - 1)
                ELSE ITERATION_LEVEL3
              END
            )
        WHERE ITERATION_LEVEL3 IS NOT NULL;
        """
    )
    # Mapping: ADO team name -> TCO TEAMID
    try:
        cols_df = fetch_df(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            ("dbo", "MAP_ADO_TEAM_TO_TCO_TEAM"),
        )
    except Exception:
        cols_df = None
    modern_cols = {"ADO_TEAM_KEY", "TEAMID", "PROGRAM_RAW", "AREA_LEVEL3_RAW", "AREA_LEVEL4_RAW", "ADO_TEAM"}
    colset = {str(c).upper() for c in cols_df["COLUMN_NAME"]} if cols_df is not None and not cols_df.empty else set()
    needs_rebuild = cols_df is None or cols_df.empty or not modern_cols.issubset(colset) or "TEAM_RAW" in colset

    if needs_rebuild:
        execute(
            f"""
            IF OBJECT_ID('{_fq('MAP_ADO_TEAM_TO_TCO_TEAM')}') IS NOT NULL
                DROP TABLE {_fq('MAP_ADO_TEAM_TO_TCO_TEAM')};

            CREATE TABLE {_fq('MAP_ADO_TEAM_TO_TCO_TEAM')} (
                ADO_TEAM_KEY NVARCHAR(900) NOT NULL PRIMARY KEY,
                TEAMID NVARCHAR(255) NULL,
                PROGRAM_RAW NVARCHAR(400) NULL,
                AREA_LEVEL3_RAW NVARCHAR(400) NULL,
                AREA_LEVEL4_RAW NVARCHAR(400) NULL,
                ADO_TEAM NVARCHAR(400) NULL
            );
            """
        )
    execute(
        f"""
        IF OBJECT_ID('{_svq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('MAP_ADO_PROGRAM_TO_TCO_PROGRAM')} (
            ADO_PROGRAM NVARCHAR(400) PRIMARY KEY,
            PROGRAMID NVARCHAR(255)
          );
        END
        """
    )
    # Mapping: ADO app name -> Application Group id
    execute(
        f"""
        IF OBJECT_ID('{_svq('MAP_ADO_APP_TO_TCO_GROUP')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('MAP_ADO_APP_TO_TCO_GROUP')} (
            ADO_APP NVARCHAR(400) PRIMARY KEY,
            APP_GROUP NVARCHAR(255)
          );
        END
        """
    )
    ensure_ado_app_candidates_table()
    ensure_ado_iteration_calendar_table()
    ensure_ado_workitem_lookup_table()
    ensure_ado_progress_tables()
    ensure_ado_timeline_tables()
    ensure_ado_profiles_table()
    ensure_performance_indexes()
    return None


def ensure_ado_app_candidates_table() -> None:
    """Ensure ADO_APP_CANDIDATES exists (staging for discovered ADO app names).

    This table is populated from ADO_FEATURES.APP_NAME_RAW and is used to surface unmapped apps and
    store mapping suggestions without writing the final MAP_ADO_APP_TO_TCO_GROUP automatically.
    """
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_APP_CANDIDATES')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_APP_CANDIDATES')} (
            ADO_APP_RAW NVARCHAR(400) NOT NULL PRIMARY KEY,
            NORMALIZED_KEY NVARCHAR(400) NULL,
            FIRST_SEEN DATETIME2 NULL,
            LAST_SEEN DATETIME2 NULL,
            FEATURE_COUNT INT NULL,
            DERIVED_FTE_SUM FLOAT NULL,
            PROGRAMS_SEEN NVARCHAR(MAX) NULL,
            -- Phase 2 canonical suggestion fields (preferred)
            SUGGESTED_GROUP_ID NVARCHAR(255) NULL,
            SUGGESTED_GROUP_NAME NVARCHAR(255) NULL,
            -- Backward compatible aliases (legacy UI/code may still read these)
            SUGGESTED_TCO_GROUP_ID NVARCHAR(255) NULL,
            SUGGESTED_TCO_GROUP NVARCHAR(255) NULL,
            STATUS NVARCHAR(50) NOT NULL CONSTRAINT DF_ADO_APP_CANDIDATES_STATUS DEFAULT 'NEW',
            UPDATED_AT DATETIME2 NOT NULL CONSTRAINT DF_ADO_APP_CANDIDATES_UPDATED_AT DEFAULT SYSDATETIME()
          );
        END
        """
    )
    # Backfill columns for existing deployments (best-effort).
    for col, col_type in (
        ("NORMALIZED_KEY", "NVARCHAR(400)"),
        ("FIRST_SEEN", "DATETIME2"),
        ("LAST_SEEN", "DATETIME2"),
        ("FEATURE_COUNT", "INT"),
        ("DERIVED_FTE_SUM", "FLOAT"),
        ("PROGRAMS_SEEN", "NVARCHAR(MAX)"),
        ("SUGGESTED_GROUP_ID", "NVARCHAR(255)"),
        ("SUGGESTED_GROUP_NAME", "NVARCHAR(255)"),
        ("SUGGESTED_TCO_GROUP_ID", "NVARCHAR(255)"),
        ("SUGGESTED_TCO_GROUP", "NVARCHAR(255)"),
        ("STATUS", "NVARCHAR(50)"),
        ("UPDATED_AT", "DATETIME2"),
    ):
        try:
            execute(
                f"""
                IF COL_LENGTH('{_fq('ADO_APP_CANDIDATES')}', '{col}') IS NULL
                BEGIN
                  ALTER TABLE {_fq('ADO_APP_CANDIDATES')} ADD {col} {col_type} NULL;
                END
                """
            )
        except Exception:
            pass

def ensure_ado_iteration_calendar_table() -> None:
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_ITERATION_CALENDAR')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_ITERATION_CALENDAR')} (
            ITERATION_PATH NVARCHAR(600) NOT NULL PRIMARY KEY,
            ITERATION_SK NVARCHAR(100) NULL,
            ITERATION_LEVEL3 NVARCHAR(400) NULL,
            ITERATION_GRAIN NVARCHAR(20) NULL,
            START_DATE DATE NULL,
            END_DATE DATE NULL,
            YEAR INT NULL,
            MONTH_KEY NVARCHAR(7) NULL,
            QUARTER_KEY NVARCHAR(7) NULL,
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME()
          );
        END
        """
    )
    # Ensure IterationSK is stored as a string (ADO Analytics often returns a GUID-like key).
    execute(
        f"""
        DECLARE @sk_type SYSNAME;
        SELECT @sk_type = t.name
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID('{_fq('ADO_ITERATION_CALENDAR')}')
          AND c.name = 'ITERATION_SK';

        IF @sk_type IS NOT NULL AND @sk_type NOT IN ('nvarchar','uniqueidentifier')
        BEGIN
          ALTER TABLE {_fq('ADO_ITERATION_CALENDAR')} ALTER COLUMN ITERATION_SK NVARCHAR(100) NULL;
        END
        """
    )
    for col, col_type in (
        ("ITERATION_SK", "NVARCHAR(100)"),
        ("ITERATION_LEVEL3", "NVARCHAR(400)"),
        ("PI_NAME", "NVARCHAR(50)"),
        ("ITERATION_NAME", "NVARCHAR(200)"),
        ("ITERATION_GRAIN", "NVARCHAR(20)"),
        ("START_DATE", "DATE"),
        ("END_DATE", "DATE"),
        ("YEAR", "INT"),
        ("MONTH_KEY", "NVARCHAR(7)"),
        ("QUARTER_KEY", "NVARCHAR(7)"),
        ("UPDATED_AT", "DATETIME2"),
    ):
        execute(
            f"""
            IF COL_LENGTH('{_fq('ADO_ITERATION_CALENDAR')}', '{col}') IS NULL
            BEGIN
              ALTER TABLE {_fq('ADO_ITERATION_CALENDAR')} ADD {col} {col_type} NULL;
            END
            """
        )
    # Best-effort backfill for older rows
    execute(
        f"""
        UPDATE {_fq('ADO_ITERATION_CALENDAR')}
        SET ITERATION_GRAIN =
          CASE
            WHEN PATINDEX('%[\\\\/]S[0-9]%', REPLACE(ITERATION_PATH,'\\','/')) > 0 OR PATINDEX('% S[0-9]%', ITERATION_PATH) > 0
              THEN 'SPRINT'
            ELSE 'PI'
          END
        WHERE ITERATION_GRAIN IS NULL;
        """
    )

    # Best-effort backfill for PI_NAME/ITERATION_NAME from the existing metadata.
    # PI_NAME: prefer IterationLevel3 only (do not parse IterationPath).
    execute(
        f"""
        UPDATE {_fq('ADO_ITERATION_CALENDAR')}
        SET PI_NAME =
          CASE
            WHEN PI_NAME IS NOT NULL AND LTRIM(RTRIM(PI_NAME)) <> '' THEN PI_NAME
            WHEN PATINDEX('%I[1-4]%', UPPER(COALESCE(ITERATION_LEVEL3, ''))) > 0
              THEN SUBSTRING(UPPER(ITERATION_LEVEL3), PATINDEX('%I[1-4]%', UPPER(ITERATION_LEVEL3)), 2)
            ELSE NULL
          END
        WHERE PI_NAME IS NULL OR LTRIM(RTRIM(PI_NAME)) = '';
        """
    )
    execute(
        f"""
        UPDATE {_fq('ADO_ITERATION_CALENDAR')}
        SET ITERATION_NAME =
          CASE
            WHEN ITERATION_NAME IS NOT NULL AND LTRIM(RTRIM(ITERATION_NAME)) <> '' THEN ITERATION_NAME
            WHEN ITERATION_LEVEL3 IS NOT NULL AND LTRIM(RTRIM(ITERATION_LEVEL3)) <> '' THEN ITERATION_LEVEL3
            ELSE NULL
          END
        WHERE ITERATION_NAME IS NULL OR LTRIM(RTRIM(ITERATION_NAME)) = '';
        """
    )


def ensure_ado_workitem_lookup_table() -> None:
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_WORKITEM_LOOKUP')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_WORKITEM_LOOKUP')} (
            WORKITEM_ID BIGINT NOT NULL PRIMARY KEY,
            WORKITEM_TYPE NVARCHAR(100) NULL,
            TITLE NVARCHAR(500) NULL,
            STATE NVARCHAR(100) NULL,
            PARENT_ID BIGINT NULL,
            CHANGED_DATE DATETIME2 NULL,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
          );
        END
        """
    )
    for col, col_type in (
        ("WORKITEM_TYPE", "NVARCHAR(100)"),
        ("TITLE", "NVARCHAR(500)"),
        ("STATE", "NVARCHAR(100)"),
        ("PARENT_ID", "BIGINT"),
        ("CHANGED_DATE", "DATETIME2"),
    ):
        execute(
            f"""
            IF COL_LENGTH('{_fq('ADO_WORKITEM_LOOKUP')}', '{col}') IS NULL
            BEGIN
              ALTER TABLE {_fq('ADO_WORKITEM_LOOKUP')} ADD {col} {col_type} NULL;
            END
            """
        )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_WORKITEM_LOOKUP')}', 'UPDATED_AT') IS NULL
        BEGIN
          ALTER TABLE {_fq('ADO_WORKITEM_LOOKUP')}
          ADD UPDATED_AT DATETIME2 NOT NULL CONSTRAINT DF_ADO_WORKITEM_LOOKUP_UPDATED_AT DEFAULT SYSUTCDATETIME();
        END
        """
    )


def ensure_ado_progress_tables() -> None:
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_FEATURE_PROGRESS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_FEATURE_PROGRESS')} (
            FEATURE_ID BIGINT NOT NULL PRIMARY KEY,
            PROPOSED_SP FLOAT NULL,
            INPROGRESS_SP FLOAT NULL,
            COMPLETED_SP FLOAT NULL,
            TOTAL_SP FLOAT NULL,
            PCT_COMPLETE FLOAT NULL,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
          );
        END
        """
    )
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_EPIC_PROGRESS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_EPIC_PROGRESS')} (
            EPIC_ID BIGINT NOT NULL PRIMARY KEY,
            PROPOSED_SP FLOAT NULL,
            INPROGRESS_SP FLOAT NULL,
            COMPLETED_SP FLOAT NULL,
            TOTAL_SP FLOAT NULL,
            PCT_COMPLETE FLOAT NULL,
            FEATURE_COUNT_TOTAL INT NULL,
            FEATURE_COUNT_WITH_STORIES INT NULL,
            FEATURE_COUNT_WITH_SP INT NULL,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
          );
        END
        """
    )
    for tbl, key_col in (
        ("ADO_FEATURE_PROGRESS", "FEATURE_ID"),
        ("ADO_EPIC_PROGRESS", "EPIC_ID"),
    ):
        for col, col_type in (
            (key_col, "BIGINT"),
            ("PROPOSED_SP", "FLOAT"),
            ("INPROGRESS_SP", "FLOAT"),
            ("COMPLETED_SP", "FLOAT"),
            ("TOTAL_SP", "FLOAT"),
            ("PCT_COMPLETE", "FLOAT"),
            ("FEATURE_COUNT_TOTAL", "INT"),
            ("FEATURE_COUNT_WITH_STORIES", "INT"),
            ("FEATURE_COUNT_WITH_SP", "INT"),
            ("UPDATED_AT", "DATETIME2"),
        ):
            execute(
                f"""
                IF COL_LENGTH('{_fq(tbl)}', '{col}') IS NULL
                BEGIN
                  ALTER TABLE {_fq(tbl)} ADD {col} {col_type} NULL;
                END
                """
            )
        execute(
            f"""
            IF COL_LENGTH('{_fq(tbl)}', 'UPDATED_AT') IS NULL
            BEGIN
              ALTER TABLE {_fq(tbl)}
              ADD UPDATED_AT DATETIME2 NOT NULL CONSTRAINT DF_{tbl}_UPDATED_AT DEFAULT SYSUTCDATETIME();
            END
            """
        )
        try:
            execute(
                f"""
                IF NOT EXISTS (
                  SELECT 1
                  FROM sys.indexes
                  WHERE name = 'IX_{tbl}_PCT_COMPLETE'
                    AND object_id = OBJECT_ID('{_fq(tbl)}')
                )
                BEGIN
                  CREATE INDEX IX_{tbl}_PCT_COMPLETE ON {_fq(tbl)}(PCT_COMPLETE);
                END
                """
            )
        except Exception:
            pass


def ensure_ado_timeline_tables() -> None:
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_WORKITEM_DATES')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_WORKITEM_DATES')} (
            WORKITEM_ID BIGINT NOT NULL PRIMARY KEY,
            WORKITEM_TYPE NVARCHAR(100) NULL,
            START_DATE DATE NULL,
            END_DATE DATE NULL,
            ITERATION_PATH NVARCHAR(600) NULL,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
          );
        END
        """
    )
    for col, col_type in (
        ("WORKITEM_TYPE", "NVARCHAR(100)"),
        ("START_DATE", "DATE"),
        ("END_DATE", "DATE"),
        ("ITERATION_PATH", "NVARCHAR(600)"),
        ("UPDATED_AT", "DATETIME2"),
    ):
        execute(
            f"""
            IF COL_LENGTH('{_fq('ADO_WORKITEM_DATES')}', '{col}') IS NULL
            BEGIN
              ALTER TABLE {_fq('ADO_WORKITEM_DATES')} ADD {col} {col_type} NULL;
            END
            """
        )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_WORKITEM_DATES')}', 'UPDATED_AT') IS NULL
        BEGIN
          ALTER TABLE {_fq('ADO_WORKITEM_DATES')}
          ADD UPDATED_AT DATETIME2 NOT NULL CONSTRAINT DF_ADO_WORKITEM_DATES_UPDATED_AT DEFAULT SYSUTCDATETIME();
        END
        """
    )

    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_WORKITEM_LINKS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_WORKITEM_LINKS')} (
            SOURCE_ID BIGINT NOT NULL,
            TARGET_ID BIGINT NOT NULL,
            LINK_CATEGORY NVARCHAR(100) NOT NULL,
            LINK_TYPE NVARCHAR(200) NOT NULL,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_ADO_WORKITEM_LINKS PRIMARY KEY (SOURCE_ID, TARGET_ID, LINK_CATEGORY, LINK_TYPE)
          );
        END
        """
    )
    for col, col_type in (
        ("SOURCE_ID", "BIGINT"),
        ("TARGET_ID", "BIGINT"),
        ("LINK_CATEGORY", "NVARCHAR(100)"),
        ("LINK_TYPE", "NVARCHAR(200)"),
        ("UPDATED_AT", "DATETIME2"),
    ):
        execute(
            f"""
            IF COL_LENGTH('{_fq('ADO_WORKITEM_LINKS')}', '{col}') IS NULL
            BEGIN
              ALTER TABLE {_fq('ADO_WORKITEM_LINKS')} ADD {col} {col_type} NULL;
            END
            """
        )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_WORKITEM_LINKS')}', 'UPDATED_AT') IS NULL
        BEGIN
          ALTER TABLE {_fq('ADO_WORKITEM_LINKS')}
          ADD UPDATED_AT DATETIME2 NOT NULL CONSTRAINT DF_ADO_WORKITEM_LINKS_UPDATED_AT DEFAULT SYSUTCDATETIME();
        END
        """
    )
    try:
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1
              FROM sys.indexes
              WHERE name = 'IX_ADO_WORKITEM_LINKS_SOURCE'
                AND object_id = OBJECT_ID('{_fq('ADO_WORKITEM_LINKS')}')
            )
            BEGIN
              CREATE INDEX IX_ADO_WORKITEM_LINKS_SOURCE ON {_fq('ADO_WORKITEM_LINKS')}(SOURCE_ID, LINK_CATEGORY);
            END
            """
        )
    except Exception:
        pass
    try:
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1
              FROM sys.indexes
              WHERE name = 'IX_ADO_WORKITEM_LINKS_TARGET'
                AND object_id = OBJECT_ID('{_fq('ADO_WORKITEM_LINKS')}')
            )
            BEGIN
              CREATE INDEX IX_ADO_WORKITEM_LINKS_TARGET ON {_fq('ADO_WORKITEM_LINKS')}(TARGET_ID, LINK_CATEGORY);
            END
            """
        )
    except Exception:
        pass


def ensure_roadmap_milestones_table() -> None:
    execute(
        f"""
        IF OBJECT_ID('{_svq('ROADMAP_MILESTONES')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ROADMAP_MILESTONES')} (
            MILESTONE_ID NVARCHAR(64) NOT NULL PRIMARY KEY,
            TITLE NVARCHAR(255) NOT NULL,
            TARGET_DATE DATE NOT NULL,
            PI_KEY NVARCHAR(64) NULL,
            EPIC_ID BIGINT NULL,
            FEATURE_ID BIGINT NULL,
            TAG NVARCHAR(64) NULL,
            SOURCE_TYPE NVARCHAR(16) NULL,
            PROGRAM_NAME NVARCHAR(255) NULL,
            TEAM_NAME NVARCHAR(255) NULL,
            IS_ACTIVE BIT NOT NULL DEFAULT 1,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL
          );
        END
        """
    )
    for col, col_type in (
        ("TITLE", "NVARCHAR(255)"),
        ("TARGET_DATE", "DATE"),
        ("PI_KEY", "NVARCHAR(64)"),
        ("EPIC_ID", "BIGINT"),
        ("FEATURE_ID", "BIGINT"),
        ("TAG", "NVARCHAR(64)"),
        ("SOURCE_TYPE", "NVARCHAR(16)"),
        ("PROGRAM_NAME", "NVARCHAR(255)"),
        ("TEAM_NAME", "NVARCHAR(255)"),
        ("IS_ACTIVE", "BIT"),
        ("UPDATED_AT", "DATETIME2"),
        ("UPDATED_BY", "NVARCHAR(255)"),
    ):
        execute(
            f"""
            IF COL_LENGTH('{_fq('ROADMAP_MILESTONES')}', '{col}') IS NULL
            BEGIN
              ALTER TABLE {_fq('ROADMAP_MILESTONES')} ADD {col} {col_type} NULL;
            END
            """
        )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ROADMAP_MILESTONES')}', 'UPDATED_AT') IS NULL
        BEGIN
          ALTER TABLE {_fq('ROADMAP_MILESTONES')}
          ADD UPDATED_AT DATETIME2 NOT NULL CONSTRAINT DF_ROADMAP_MILESTONES_UPDATED_AT DEFAULT SYSUTCDATETIME();
        END
        """
    )
    try:
        execute(
            f"""
            IF NOT EXISTS (
              SELECT 1
              FROM sys.indexes
              WHERE name = 'IX_ROADMAP_MILESTONES_TARGET_DATE'
                AND object_id = OBJECT_ID('{_fq('ROADMAP_MILESTONES')}')
            )
            BEGIN
              CREATE INDEX IX_ROADMAP_MILESTONES_TARGET_DATE ON {_fq('ROADMAP_MILESTONES')}(TARGET_DATE);
            END
            """
        )
    except Exception:
        pass


def list_roadmap_milestones(
    programs: Optional[Sequence[str]] = None,
    teams: Optional[Sequence[str]] = None,
    include_inactive: bool = False,
) -> pd.DataFrame:
    ensure_roadmap_milestones_table()
    where: List[str] = []
    params: List[Any] = []
    if not include_inactive:
        where.append("ISNULL(IS_ACTIVE, 1) = 1")
    if programs:
        vals = [str(v).strip() for v in programs if str(v).strip()]
        if vals:
            # Global milestones (NULL/blank scope) should remain visible in filtered views.
            where.append(
                "(NULLIF(LTRIM(RTRIM(ISNULL(PROGRAM_NAME, ''))), '') IS NULL "
                "OR UPPER(ISNULL(PROGRAM_NAME, '')) IN (" + ", ".join(["%s"] * len(vals)) + "))"
            )
            params.extend([v.upper() for v in vals])
    if teams:
        vals = [str(v).strip() for v in teams if str(v).strip()]
        if vals:
            # Global milestones (NULL/blank scope) should remain visible in filtered views.
            where.append(
                "(NULLIF(LTRIM(RTRIM(ISNULL(TEAM_NAME, ''))), '') IS NULL "
                "OR UPPER(ISNULL(TEAM_NAME, '')) IN (" + ", ".join(["%s"] * len(vals)) + "))"
            )
            params.extend([v.upper() for v in vals])
    sql = f"""
        SELECT
          MILESTONE_ID, TITLE, TARGET_DATE, PI_KEY, EPIC_ID, FEATURE_ID, TAG, SOURCE_TYPE,
          PROGRAM_NAME, TEAM_NAME, IS_ACTIVE, UPDATED_AT, UPDATED_BY
        FROM {_fq('ROADMAP_MILESTONES')}
    """
    if where:
        sql += "\nWHERE " + " AND ".join(where)
    sql += "\nORDER BY TARGET_DATE ASC, TITLE ASC"
    try:
        df = fetch_df(sql, tuple(params) if params else None)
    except Exception:
        return pd.DataFrame()
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


@_versioned_write("ROADMAP_MILESTONES.UPSERT")
def upsert_roadmap_milestone(
    *,
    title: str,
    target_date: Any,
    pi_key: Optional[str] = None,
    epic_id: Optional[int] = None,
    feature_id: Optional[int] = None,
    tag: Optional[str] = None,
    source_type: Optional[str] = None,
    program_name: Optional[str] = None,
    team_name: Optional[str] = None,
    is_active: bool = True,
    updated_by: Optional[str] = None,
    milestone_id: Optional[str] = None,
) -> str:
    ensure_roadmap_milestones_table()
    clean_title = str(title or "").strip()
    if not clean_title:
        raise ValueError("Milestone title is required.")
    dt_val = pd.to_datetime(target_date, errors="coerce")
    if pd.isna(dt_val):
        raise ValueError("Milestone target date is required.")
    if not milestone_id:
        import uuid
        milestone_id = str(uuid.uuid4())
    milestone_id = str(milestone_id).strip()
    if not milestone_id:
        raise ValueError("Milestone ID is invalid.")
    execute(
        f"""
        MERGE INTO {_fq('ROADMAP_MILESTONES')} t
        USING (
          SELECT
            %s AS MILESTONE_ID,
            %s AS TITLE,
            %s AS TARGET_DATE,
            %s AS PI_KEY,
            %s AS EPIC_ID,
            %s AS FEATURE_ID,
            %s AS TAG,
            %s AS SOURCE_TYPE,
            %s AS PROGRAM_NAME,
            %s AS TEAM_NAME,
            %s AS IS_ACTIVE,
            %s AS UPDATED_BY
        ) s
        ON t.MILESTONE_ID = s.MILESTONE_ID
        WHEN MATCHED THEN UPDATE SET
          TITLE = s.TITLE,
          TARGET_DATE = TRY_CONVERT(DATE, s.TARGET_DATE),
          PI_KEY = s.PI_KEY,
          EPIC_ID = s.EPIC_ID,
          FEATURE_ID = s.FEATURE_ID,
          TAG = s.TAG,
          SOURCE_TYPE = s.SOURCE_TYPE,
          PROGRAM_NAME = s.PROGRAM_NAME,
          TEAM_NAME = s.TEAM_NAME,
          IS_ACTIVE = COALESCE(s.IS_ACTIVE, 1),
          UPDATED_AT = SYSUTCDATETIME(),
          UPDATED_BY = s.UPDATED_BY
        WHEN NOT MATCHED THEN
          INSERT (MILESTONE_ID, TITLE, TARGET_DATE, PI_KEY, EPIC_ID, FEATURE_ID, TAG, SOURCE_TYPE, PROGRAM_NAME, TEAM_NAME, IS_ACTIVE, UPDATED_AT, UPDATED_BY)
          VALUES (s.MILESTONE_ID, s.TITLE, TRY_CONVERT(DATE, s.TARGET_DATE), s.PI_KEY, s.EPIC_ID, s.FEATURE_ID, s.TAG, s.SOURCE_TYPE, s.PROGRAM_NAME, s.TEAM_NAME, COALESCE(s.IS_ACTIVE, 1), SYSUTCDATETIME(), s.UPDATED_BY);
        """,
        (
            milestone_id,
            clean_title,
            dt_val.date(),
            (str(pi_key).strip() if str(pi_key or "").strip() else None),
            (int(epic_id) if epic_id is not None and str(epic_id).strip() else None),
            (int(feature_id) if feature_id is not None and str(feature_id).strip() else None),
            (str(tag).strip() if str(tag or "").strip() else None),
            (str(source_type).strip().upper() if str(source_type or "").strip() else None),
            (str(program_name).strip() if str(program_name or "").strip() else None),
            (str(team_name).strip() if str(team_name or "").strip() else None),
            1 if bool(is_active) else 0,
            (str(updated_by).strip() if str(updated_by or "").strip() else None),
        ),
    )
    return milestone_id


@_versioned_write("ROADMAP_MILESTONES.BULK_UPSERT")
def upsert_roadmap_milestones(rows: List[Tuple[Any, ...]]) -> int:
    ensure_roadmap_milestones_table()
    if not rows:
        return 0
    normalized: List[Tuple[Any, ...]] = []
    for row in rows:
        if not row or len(row) < 9:
            continue
        vals = list(row)
        mid = str(vals[0] or "").strip()
        title = str(vals[1] or "").strip()
        dt_val = pd.to_datetime(vals[2], errors="coerce")
        if not mid or not title or pd.isna(dt_val):
            continue
        pi_key = str(vals[3] or "").strip() or None
        epic_id = (int(float(vals[4])) if len(vals) > 4 and vals[4] not in (None, "", "None", "nan") else None)
        # Backward compatibility:
        # old shape (9): mid,title,date,pi,epic,program,team,is_active,updated_by
        # new shape (12): mid,title,date,pi,epic,feature,tag,source_type,program,team,is_active,updated_by
        if len(vals) >= 12:
            feature_id = (int(float(vals[5])) if vals[5] not in (None, "", "None", "nan") else None)
            tag = str(vals[6] or "").strip() or None
            source_type = str(vals[7] or "").strip().upper() or None
            program_name = str(vals[8] or "").strip() or None
            team_name = str(vals[9] or "").strip() or None
            is_active = 1 if bool(vals[10]) else 0
            updated_by = str(vals[11] or "").strip() or None
        else:
            feature_id = None
            tag = None
            source_type = None
            program_name = str(vals[5] or "").strip() or None
            team_name = str(vals[6] or "").strip() or None
            is_active = 1 if bool(vals[7]) else 0
            updated_by = str(vals[8] or "").strip() or None
        normalized.append(
            (
                mid,
                title,
                dt_val.date(),
                pi_key,
                epic_id,
                feature_id,
                tag,
                source_type,
                program_name,
                team_name,
                is_active,
                updated_by,
            )
        )
    if not normalized:
        return 0
    execute(
        f"""
        MERGE INTO {_fq('ROADMAP_MILESTONES')} t
        USING (
          SELECT
            %s AS MILESTONE_ID,
            %s AS TITLE,
            %s AS TARGET_DATE,
            %s AS PI_KEY,
            %s AS EPIC_ID,
            %s AS FEATURE_ID,
            %s AS TAG,
            %s AS SOURCE_TYPE,
            %s AS PROGRAM_NAME,
            %s AS TEAM_NAME,
            %s AS IS_ACTIVE,
            %s AS UPDATED_BY
        ) s
        ON t.MILESTONE_ID = s.MILESTONE_ID
        WHEN MATCHED THEN UPDATE SET
          TITLE = s.TITLE,
          TARGET_DATE = TRY_CONVERT(DATE, s.TARGET_DATE),
          PI_KEY = s.PI_KEY,
          EPIC_ID = s.EPIC_ID,
          FEATURE_ID = s.FEATURE_ID,
          TAG = s.TAG,
          SOURCE_TYPE = s.SOURCE_TYPE,
          PROGRAM_NAME = s.PROGRAM_NAME,
          TEAM_NAME = s.TEAM_NAME,
          IS_ACTIVE = COALESCE(s.IS_ACTIVE, 1),
          UPDATED_AT = SYSUTCDATETIME(),
          UPDATED_BY = s.UPDATED_BY
        WHEN NOT MATCHED THEN
          INSERT (MILESTONE_ID, TITLE, TARGET_DATE, PI_KEY, EPIC_ID, FEATURE_ID, TAG, SOURCE_TYPE, PROGRAM_NAME, TEAM_NAME, IS_ACTIVE, UPDATED_AT, UPDATED_BY)
          VALUES (s.MILESTONE_ID, s.TITLE, TRY_CONVERT(DATE, s.TARGET_DATE), s.PI_KEY, s.EPIC_ID, s.FEATURE_ID, s.TAG, s.SOURCE_TYPE, s.PROGRAM_NAME, s.TEAM_NAME, COALESCE(s.IS_ACTIVE, 1), SYSUTCDATETIME(), s.UPDATED_BY);
        """,
        normalized,
        many=True,
    )
    return len(normalized)


@_versioned_write("ROADMAP_MILESTONES.DELETE")
def delete_roadmap_milestone(milestone_id: str) -> None:
    ensure_roadmap_milestones_table()
    mid = str(milestone_id or "").strip()
    if not mid:
        return
    execute(
        f"DELETE FROM {_fq('ROADMAP_MILESTONES')} WHERE MILESTONE_ID = %s",
        (mid,),
    )


@_versioned_write("ADO_ITERATION_CALENDAR.UPSERT")
def upsert_ado_iteration_calendar(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    ensure_ado_iteration_calendar_table()

    work = df.copy()
    if "YEAR" in work.columns:
        try:
            work["YEAR"] = pd.to_numeric(work["YEAR"], errors="coerce").astype("Int64")
        except Exception:
            pass

    def _to_py(v: Any):
        try:
            import pandas as _pd, numpy as _np  # type: ignore
        except Exception:
            _pd = pd  # fallback
            class _np:  # type: ignore
                floating = float
                integer = int
        if v is None:
            return None
        try:
            from pandas._libs.tslibs.nattype import NaTType as _NaTType  # type: ignore
        except Exception:
            _NaTType = type(None)  # type: ignore
        if isinstance(v, _NaTType):
            return None
        try:
            from pandas import Timestamp as _Ts  # type: ignore
            if isinstance(v, _Ts):
                return v.to_pydatetime()
        except Exception:
            pass
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        try:
            import numpy as _np2  # type: ignore
            if isinstance(v, _np2.floating):
                return None if _np2.isnan(v) else float(v)
            if isinstance(v, _np2.integer):
                return int(v)
        except Exception:
            pass
        if isinstance(v, str):
            s = v.strip()
            return s if s != "" else None
        return v

    cols = [
        "ITERATION_PATH",
        "ITERATION_SK",
        "ITERATION_LEVEL3",
        "ITERATION_GRAIN",
        "START_DATE",
        "END_DATE",
        "YEAR",
        "MONTH_KEY",
        "QUARTER_KEY",
    ]
    rows: List[Tuple[Any, ...]] = []
    for _, r in work.iterrows():
        tup = tuple(_to_py(r.get(c)) for c in cols)
        if tup[0] is None:
            continue
        rows.append(tup)
    if not rows:
        return 0

    sql = f"""
    MERGE INTO {_fq('ADO_ITERATION_CALENDAR')} t
    USING (
      SELECT %s AS ITERATION_PATH,
             %s AS ITERATION_SK,
             %s AS ITERATION_LEVEL3,
             %s AS ITERATION_GRAIN,
             %s AS START_DATE,
             %s AS END_DATE,
             %s AS YEAR,
             %s AS MONTH_KEY,
             %s AS QUARTER_KEY
    ) s
    ON UPPER(LTRIM(RTRIM(t.ITERATION_PATH))) = UPPER(LTRIM(RTRIM(s.ITERATION_PATH)))
    WHEN MATCHED THEN UPDATE SET
      t.ITERATION_SK = s.ITERATION_SK,
      t.ITERATION_LEVEL3 = s.ITERATION_LEVEL3,
      t.ITERATION_GRAIN = s.ITERATION_GRAIN,
      t.START_DATE = TRY_CONVERT(DATE, s.START_DATE),
      t.END_DATE = TRY_CONVERT(DATE, s.END_DATE),
      t.YEAR = s.YEAR,
      t.MONTH_KEY = s.MONTH_KEY,
      t.QUARTER_KEY = s.QUARTER_KEY,
      t.UPDATED_AT = SYSDATETIME()
    WHEN NOT MATCHED THEN INSERT
      (ITERATION_PATH, ITERATION_SK, ITERATION_LEVEL3, ITERATION_GRAIN, START_DATE, END_DATE, YEAR, MONTH_KEY, QUARTER_KEY, UPDATED_AT)
    VALUES
      (s.ITERATION_PATH, s.ITERATION_SK, s.ITERATION_LEVEL3, s.ITERATION_GRAIN, TRY_CONVERT(DATE, s.START_DATE), TRY_CONVERT(DATE, s.END_DATE), s.YEAR, s.MONTH_KEY, s.QUARTER_KEY, SYSDATETIME());
    """
    execute(sql, rows, many=True)
    return len(rows)


@_versioned_write("ADO_FEATURES.UPSERT")
def upsert_ado_features(df: pd.DataFrame, *, allow_null_overwrite: bool = False) -> int:
    """Upsert ADO features into ADO_FEATURES table by FEATURE_ID (idempotent).

    Columns expected in df (strings are trimmed, dates converted):
      FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, BUSINESS_VALUE,
      ITERATION_LEVEL3, ITERATION_LEVEL3_RAW, PI_LABEL, ITERATION_PATH, ITERATION_SK, CREATED_AT, CHANGED_AT, ADO_YEAR, INVESTMENT_DIMENSION,
      PROGRAM_RAW, AREA_LEVEL1_RAW, AREA_LEVEL2_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, AREA_PATH_RAW, TEAM_VARIANT_KEY
    Extra columns are ignored.
    """
    if df is None or df.empty:
        return 0
    ensure_ado_minimal_tables()
    # Defensively normalize team leaf keys for every upsert path (UI, scripts, restore).
    try:
        from utils.ado import repair_leaf_teams as _repair_leaf_teams
        df = _repair_leaf_teams(df.copy())
    except Exception:
        pass

    def _to_py(v: Any):
        try:
            import pandas as _pd, numpy as _np  # type: ignore
        except Exception:
            _pd = pd  # fallback
            class _np:  # type: ignore
                floating = float
                integer = int
        if v is None:
            return None
        try:
            from pandas._libs.tslibs.nattype import NaTType as _NaTType  # type: ignore
        except Exception:
            _NaTType = type(None)  # type: ignore
        if isinstance(v, _NaTType):
            return None
        try:
            from pandas import Timestamp as _Ts  # type: ignore
            if isinstance(v, _Ts):
                return v.to_pydatetime()
        except Exception:
            pass
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        try:
            import numpy as _np2  # type: ignore
            if isinstance(v, _np2.floating):
                return None if _np2.isnan(v) else float(v)
            if isinstance(v, _np2.integer):
                return int(v)
        except Exception:
            pass
        if isinstance(v, str):
            s = v.strip()
            return s if s != "" else None
        return v

    cols = [
        "FEATURE_ID","TITLE","STATE","TEAM_RAW","APP_NAME_RAW","EFFORT_POINTS","STORY_POINTS","BUSINESS_VALUE",
        "ITERATION_LEVEL3","ITERATION_LEVEL3_RAW","PI_LABEL","ITERATION_PATH","ITERATION_SK","CREATED_AT","CHANGED_AT","PARENT_ID","EPIC_ID","EPIC_TITLE","EPIC_STATE",
        "ADO_YEAR","INVESTMENT_DIMENSION","PROGRAM_RAW","AREA_LEVEL1_RAW","AREA_LEVEL2_RAW","AREA_LEVEL3_RAW","AREA_LEVEL4_RAW","AREA_PATH_RAW","TEAM_VARIANT_KEY",
    ]
    rows: List[Tuple[Any, ...]] = []
    for _, r in df.iterrows():
        tup = tuple(_to_py(r.get(c)) for c in cols)
        if tup[0] is None:
            continue
        rows.append(tup)
    if not rows:
        return 0

    def _upd_expr(col: str) -> str:
        # During live ADO sync we should not clobber existing DB values with NULLs when the OData
        # query omitted a field or normalization produced blanks. Restore flows can opt in to NULL
        # overwrites for lossless recovery by setting allow_null_overwrite=True.
        return f"s.{col}" if allow_null_overwrite else f"COALESCE(s.{col}, t.{col})"

    sql = f"""
    MERGE INTO {_fq('ADO_FEATURES')} t
    USING (
      SELECT %s AS FEATURE_ID, %s AS TITLE, %s AS STATE,
             %s AS TEAM_RAW, %s AS APP_NAME_RAW, %s AS EFFORT_POINTS, %s AS STORY_POINTS, %s AS BUSINESS_VALUE,
             %s AS ITERATION_LEVEL3, %s AS ITERATION_LEVEL3_RAW, %s AS PI_LABEL, %s AS ITERATION_PATH, %s AS ITERATION_SK,
             %s AS CREATED_AT, %s AS CHANGED_AT, %s AS PARENT_ID, %s AS EPIC_ID, %s AS EPIC_TITLE, %s AS EPIC_STATE,
             %s AS ADO_YEAR, %s AS INVESTMENT_DIMENSION,
             %s AS PROGRAM_RAW, %s AS AREA_LEVEL1_RAW, %s AS AREA_LEVEL2_RAW, %s AS AREA_LEVEL3_RAW, %s AS AREA_LEVEL4_RAW, %s AS AREA_PATH_RAW,
             %s AS TEAM_VARIANT_KEY
    ) s
    ON t.FEATURE_ID = s.FEATURE_ID
    WHEN MATCHED THEN UPDATE SET
      TITLE = s.TITLE,
      STATE = s.STATE,
      TEAM_RAW = s.TEAM_RAW,
      APP_NAME_RAW = {_upd_expr("APP_NAME_RAW")},
      EFFORT_POINTS = {_upd_expr("EFFORT_POINTS")},
      STORY_POINTS = {_upd_expr("STORY_POINTS")},
      BUSINESS_VALUE = {_upd_expr("BUSINESS_VALUE")},
      ITERATION_LEVEL3 = {_upd_expr("ITERATION_LEVEL3")},
      ITERATION_LEVEL3_RAW = {_upd_expr("ITERATION_LEVEL3_RAW")},
      PI_LABEL = {_upd_expr("PI_LABEL")},
      ITERATION_PATH = {_upd_expr("ITERATION_PATH")},
      ITERATION_SK = {_upd_expr("ITERATION_SK")},
      CREATED_AT = s.CREATED_AT,
      CHANGED_AT = s.CHANGED_AT,
      PARENT_ID = {_upd_expr("PARENT_ID")},
      EPIC_ID = {_upd_expr("EPIC_ID")},
      EPIC_TITLE = {_upd_expr("EPIC_TITLE")},
      EPIC_STATE = {_upd_expr("EPIC_STATE")},
      ADO_YEAR = s.ADO_YEAR,
      INVESTMENT_DIMENSION = {_upd_expr("INVESTMENT_DIMENSION")},
      PROGRAM_RAW = s.PROGRAM_RAW,
      AREA_LEVEL1_RAW = s.AREA_LEVEL1_RAW,
      AREA_LEVEL2_RAW = s.AREA_LEVEL2_RAW,
      AREA_LEVEL3_RAW = s.AREA_LEVEL3_RAW,
      AREA_LEVEL4_RAW = s.AREA_LEVEL4_RAW,
      AREA_PATH_RAW = s.AREA_PATH_RAW,
      TEAM_VARIANT_KEY = s.TEAM_VARIANT_KEY
    WHEN NOT MATCHED THEN INSERT
      (FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, STORY_POINTS, BUSINESS_VALUE, ITERATION_LEVEL3, ITERATION_LEVEL3_RAW, PI_LABEL, ITERATION_PATH, ITERATION_SK, CREATED_AT, CHANGED_AT, PARENT_ID, EPIC_ID, EPIC_TITLE, EPIC_STATE, ADO_YEAR, INVESTMENT_DIMENSION, PROGRAM_RAW, AREA_LEVEL1_RAW, AREA_LEVEL2_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, AREA_PATH_RAW, TEAM_VARIANT_KEY)
    VALUES
      (s.FEATURE_ID, s.TITLE, s.STATE, s.TEAM_RAW, s.APP_NAME_RAW, s.EFFORT_POINTS, s.STORY_POINTS, s.BUSINESS_VALUE, s.ITERATION_LEVEL3, s.ITERATION_LEVEL3_RAW, s.PI_LABEL, s.ITERATION_PATH, s.ITERATION_SK, s.CREATED_AT, s.CHANGED_AT, s.PARENT_ID, s.EPIC_ID, s.EPIC_TITLE, s.EPIC_STATE, s.ADO_YEAR, s.INVESTMENT_DIMENSION, s.PROGRAM_RAW, s.AREA_LEVEL1_RAW, s.AREA_LEVEL2_RAW, s.AREA_LEVEL3_RAW, s.AREA_LEVEL4_RAW, s.AREA_PATH_RAW, s.TEAM_VARIANT_KEY);
    """
    execute(sql, rows, many=True)
    return len(rows)


# =========================================================
# Email alert configuration storage
# =========================================================

EMAIL_ALERT_CONFIG_KEY = "GLOBAL"
EMAIL_ALERT_CONFIG_DEFAULT: Dict[str, Any] = {
    "pending_window_days": 30,
    "contract_window_months": 9,
    "subject_prefix": "[TCO]",
    "subject_template": "{prefix} Action needed: {count} invoice(s) {status} for {program}",
    "body_intro": "The following invoices are currently flagged as {status_title}. Please review and take action.",
    "body_footer": "Update the invoice status, add notes, or attach approvals directly in the Invoice Tracking page. This email was generated automatically from the TCO Invoice Tracking dashboard.",
    "footnote_template": "Pending = due within {pending_window_days} day(s). Critical = past due and still planned.",
    "save_to_sent_items": False,
    "additional_recipients": "",
    "additional_cc": "",
    "schedule_enabled": False,
    "schedule_day": 1,
    "schedule_hour_utc": 13,
    "mail_sender": "",
}


def ensure_email_alert_config_table() -> None:
    # In admin scope-preview mode we enforce read-only behavior.
    # Skip schema-ensure writes to avoid page-load failures on read paths.
    if _is_admin_scope_preview_readonly():
        return
    execute(
        f"""
        IF OBJECT_ID('{_svq('EMAIL_ALERT_CONFIG')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('EMAIL_ALERT_CONFIG')} (
            CONFIG_KEY NVARCHAR(100) PRIMARY KEY,
            CONFIG_JSON NVARCHAR(MAX) NULL,
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL
          );
        END
        """
    )


def _email_alert_config_meta_row() -> Optional[pd.Series]:
    try:
        ensure_email_alert_config_table()
    except Exception:
        pass
    try:
        df = fetch_df(
            f"SELECT TOP 1 CONFIG_KEY, CONFIG_JSON, UPDATED_AT, UPDATED_BY FROM {_fq('EMAIL_ALERT_CONFIG')} WHERE CONFIG_KEY = %s",
            (EMAIL_ALERT_CONFIG_KEY,),
        )
    except Exception:
        return None
    if df is None or df.empty:
        return None
    return df.iloc[0]


def get_email_alert_config() -> Dict[str, Any]:
    row = _email_alert_config_meta_row()
    payload: Dict[str, Any] = {}
    updated_at = None
    updated_by = None
    if row is not None:
        try:
            payload = json.loads(row.get("CONFIG_JSON") or "{}") or {}
        except Exception:
            payload = {}
        updated_at = row.get("UPDATED_AT")
        updated_by = row.get("UPDATED_BY")
    merged = EMAIL_ALERT_CONFIG_DEFAULT.copy()
    if isinstance(payload, dict):
        merged.update({k: v for k, v in payload.items() if k in EMAIL_ALERT_CONFIG_DEFAULT})
    merged.setdefault("pending_window_days", EMAIL_ALERT_CONFIG_DEFAULT["pending_window_days"])
    merged.setdefault("contract_window_months", EMAIL_ALERT_CONFIG_DEFAULT["contract_window_months"])
    merged.setdefault("subject_prefix", EMAIL_ALERT_CONFIG_DEFAULT["subject_prefix"])
    merged.setdefault("subject_template", EMAIL_ALERT_CONFIG_DEFAULT["subject_template"])
    merged.setdefault("body_intro", EMAIL_ALERT_CONFIG_DEFAULT["body_intro"])
    merged.setdefault("body_footer", EMAIL_ALERT_CONFIG_DEFAULT["body_footer"])
    merged.setdefault("footnote_template", EMAIL_ALERT_CONFIG_DEFAULT["footnote_template"])
    merged.setdefault("save_to_sent_items", EMAIL_ALERT_CONFIG_DEFAULT["save_to_sent_items"])
    merged.setdefault("additional_recipients", EMAIL_ALERT_CONFIG_DEFAULT["additional_recipients"])
    merged.setdefault("additional_cc", EMAIL_ALERT_CONFIG_DEFAULT["additional_cc"])
    merged.setdefault("schedule_enabled", EMAIL_ALERT_CONFIG_DEFAULT["schedule_enabled"])
    merged.setdefault("schedule_day", EMAIL_ALERT_CONFIG_DEFAULT["schedule_day"])
    merged.setdefault("schedule_hour_utc", EMAIL_ALERT_CONFIG_DEFAULT["schedule_hour_utc"])
    merged.setdefault("mail_sender", EMAIL_ALERT_CONFIG_DEFAULT["mail_sender"])
    merged["_meta"] = {"updated_at": updated_at, "updated_by": updated_by}
    return merged


@_versioned_write("EMAIL_ALERT_CONFIG.SAVE")
def save_email_alert_config(config: Dict[str, Any], updated_by: Optional[str] = None) -> None:
    ensure_email_alert_config_table()
    clean = {k: v for k, v in config.items() if not str(k).startswith("_")}
    trimmed: Dict[str, Any] = EMAIL_ALERT_CONFIG_DEFAULT.copy()
    trimmed.update({k: v for k, v in clean.items() if k in EMAIL_ALERT_CONFIG_DEFAULT})
    payload = json.dumps(trimmed, default=str)
    execute(
        f"""
        MERGE INTO {_fq('EMAIL_ALERT_CONFIG')} AS t
        USING (SELECT ? AS CONFIG_KEY, ? AS CONFIG_JSON, ? AS UPDATED_BY) AS s
        ON t.CONFIG_KEY = s.CONFIG_KEY
        WHEN MATCHED THEN UPDATE SET
          CONFIG_JSON = s.CONFIG_JSON,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = s.UPDATED_BY
        WHEN NOT MATCHED THEN
          INSERT (CONFIG_KEY, CONFIG_JSON, UPDATED_AT, UPDATED_BY)
          VALUES (s.CONFIG_KEY, s.CONFIG_JSON, SYSDATETIME(), s.UPDATED_BY);
        """,
        (EMAIL_ALERT_CONFIG_KEY, payload, updated_by),
    )


# =========================================================
# Access control tables and helpers
# =========================================================

def ensure_access_control_tables(cfg: Optional[MssqlConfig] = None) -> None:
    """Create APP_USERS table if missing for role-based access control.

    Roles: ADMIN, CONTRIBUTOR, VIEWER. Email is unique identifier.
    """
    # In admin scope-preview mode we enforce read-only behavior.
    # Skip schema-ensure writes to avoid blocking auth/page load.
    if _is_admin_scope_preview_readonly():
        return
    execute(
        f"""
        IF OBJECT_ID('{_svq('APP_USERS', cfg)}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('APP_USERS', cfg)} (
            USERID UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_APP_USERS_USERID DEFAULT NEWID(),
            EMAIL NVARCHAR(320) NOT NULL UNIQUE,
            DISPLAY_NAME NVARCHAR(255),
            ROLE NVARCHAR(32) NOT NULL,
            IS_ACTIVE BIT NOT NULL DEFAULT 1,
            PROVIDER NVARCHAR(32) NULL,
            CREATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            LAST_LOGIN_AT DATETIME2 NULL
          );
        END
        IF OBJECT_ID('{_fq('APP_USERS', cfg)}','U') IS NOT NULL
           AND OBJECT_ID('DF_APP_USERS_USERID','D') IS NULL
        BEGIN
          ALTER TABLE {_fq('APP_USERS', cfg)} ADD CONSTRAINT DF_APP_USERS_USERID DEFAULT NEWID() FOR USERID;
        END
        """,
        cfg=cfg,
    )


def list_app_users(cfg: Optional[MssqlConfig] = None) -> pd.DataFrame:
    ensure_access_control_tables(cfg=cfg)
    return fetch_df(
        f"SELECT USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, PROVIDER, CREATED_AT, LAST_LOGIN_AT FROM {_fq('APP_USERS', cfg)} ORDER BY EMAIL",
        cfg=cfg,
    )


def get_user_by_email(email: str, cfg: Optional[MssqlConfig] = None) -> Optional[dict]:
    try:
        ensure_access_control_tables(cfg=cfg)
    except Exception:
        # Best-effort: if schema ensure cannot run in read-only preview mode,
        # still attempt a direct read; caller handles None.
        pass
    try:
        df = fetch_df(
            f"SELECT TOP 1 * FROM {_fq('APP_USERS', cfg)} WHERE UPPER(EMAIL) = UPPER(%s)",
            (email,),
            cfg=cfg,
        )
    except Exception:
        return None
    if df is None or df.empty:
        return None
    return df.iloc[0].to_dict()


@_versioned_write("APP_USERS.UPSERT")
def upsert_app_user(
    email: str,
    display_name: Optional[str],
    role: str,
    is_active: bool = True,
    provider: Optional[str] = None,
    cfg: Optional[MssqlConfig] = None,
) -> None:
    ensure_access_control_tables(cfg=cfg)
    execute(
        f"""
        MERGE INTO {_fq('APP_USERS', cfg)} AS t
        USING (SELECT ? AS EMAIL, ? AS DISPLAY_NAME, ? AS ROLE, ? AS IS_ACTIVE, ? AS PROVIDER) AS s
        ON UPPER(t.EMAIL) = UPPER(s.EMAIL)
        WHEN MATCHED THEN UPDATE SET
          DISPLAY_NAME = s.DISPLAY_NAME,
          ROLE = s.ROLE,
          IS_ACTIVE = s.IS_ACTIVE,
          PROVIDER = s.PROVIDER
        WHEN NOT MATCHED THEN
          INSERT (USERID, EMAIL, DISPLAY_NAME, ROLE, IS_ACTIVE, PROVIDER)
          VALUES (CONVERT(VARCHAR(36), NEWID()), s.EMAIL, s.DISPLAY_NAME, s.ROLE, s.IS_ACTIVE, s.PROVIDER);
        """,
        (email, display_name, role, 1 if is_active else 0, provider),
        cfg=cfg,
    )


@_versioned_write("APP_USERS.SET_ROLE")
def set_user_role(email: str, role: str) -> None:
    ensure_access_control_tables()
    execute(f"UPDATE {_fq('APP_USERS')} SET ROLE = %s WHERE UPPER(EMAIL) = UPPER(%s)", (role, email))


@_versioned_write("APP_USERS.SET_ACTIVE")
def set_user_active(email: str, is_active: bool) -> None:
    ensure_access_control_tables()
    execute(f"UPDATE {_fq('APP_USERS')} SET IS_ACTIVE = %s WHERE UPPER(EMAIL) = UPPER(%s)", (1 if is_active else 0, email))


def record_user_login(email: str) -> None:
    ensure_access_control_tables()
    execute(f"UPDATE {_fq('APP_USERS')} SET LAST_LOGIN_AT = SYSDATETIME() WHERE UPPER(EMAIL) = UPPER(%s)", (email,))


# =========================================================
# Optional: User → Program/Team membership mapping
# =========================================================

def ensure_user_membership_tables() -> None:
    """Create a simple mapping for user memberships to Program/Team.

    This is optional and only used if you want to restrict views per user.
    """
    execute(
        f"""
        IF OBJECT_ID('{_svq('APP_USER_MEMBERSHIP')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('APP_USER_MEMBERSHIP')} (
            USER_EMAIL NVARCHAR(320) NOT NULL,
            PROGRAMID  NVARCHAR(255) NOT NULL DEFAULT '',
            TEAMID     NVARCHAR(255) NOT NULL DEFAULT '',
            CREATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            CONSTRAINT PK_APP_USER_MEMBERSHIP PRIMARY KEY (USER_EMAIL, PROGRAMID, TEAMID)
          );
        END
        """
    )
    # If table exists with nullable columns, normalize to NOT NULL with default '' and ensure PK exists
    execute(
        f"""
        IF EXISTS (
          SELECT 1 FROM sys.columns c
          WHERE c.object_id = OBJECT_ID('{_fq('APP_USER_MEMBERSHIP')}') AND c.name IN ('PROGRAMID','TEAMID') AND c.is_nullable = 1
        )
        BEGIN
          UPDATE {_fq('APP_USER_MEMBERSHIP')} SET PROGRAMID = ISNULL(PROGRAMID, ''), TEAMID = ISNULL(TEAMID, '');
          ALTER TABLE {_fq('APP_USER_MEMBERSHIP')} ALTER COLUMN PROGRAMID NVARCHAR(255) NOT NULL;
          ALTER TABLE {_fq('APP_USER_MEMBERSHIP')} ALTER COLUMN TEAMID NVARCHAR(255) NOT NULL;
        END

        IF NOT EXISTS (
          SELECT 1 FROM sys.key_constraints WHERE name = 'PK_APP_USER_MEMBERSHIP' AND parent_object_id = OBJECT_ID('{_fq('APP_USER_MEMBERSHIP')}')
        )
        BEGIN
          ALTER TABLE {_fq('APP_USER_MEMBERSHIP')} ADD CONSTRAINT PK_APP_USER_MEMBERSHIP PRIMARY KEY (USER_EMAIL, PROGRAMID, TEAMID);
        END
        """
    )


def list_user_memberships(user_email: str) -> pd.DataFrame:
    """Return Program/Team memberships for a user (by email).

    Columns: USER_EMAIL, PROGRAMID, PROGRAMNAME, TEAMID, TEAMNAME
    """
    ensure_user_membership_tables()
    return fetch_df(
        f"""
        SELECT m.USER_EMAIL, m.PROGRAMID, p.PROGRAMNAME, m.TEAMID, t.TEAMNAME
        FROM {_fq('APP_USER_MEMBERSHIP')} m
        LEFT JOIN {_fq('PROGRAMS')} p ON p.PROGRAMID = m.PROGRAMID
        LEFT JOIN {_fq('TEAMS')} t ON t.TEAMID = m.TEAMID
        WHERE UPPER(m.USER_EMAIL) = UPPER(%s)
        ORDER BY p.PROGRAMNAME, t.TEAMNAME
        """,
        (user_email,),
    )


def repair_ado_effort_points_precision() -> None:
    # Implemented above
    return None


# =========================================================
# ADO profiles (persisted)
# =========================================================

ADO_DEFAULT_PROFILE_NAME = "Default (Legacy Portfolio Settings)"
ADO_DEFAULT_PROFILE_CONFIG_JSON = (
    "{\n"
    '  "mode": "legacy",\n'
    '  "description": "Uses existing portfolio settings stored in Settings > ADO",\n'
    '  "work_item_types": { "feature": "Feature", "epic": "Epic" },\n'
    '  "fields": { "story_points": "Effort", "app_name": "Custom_ApplicationName", "business_value": "BusinessValue" },\n'
    '  "swag": { "points_per_fte": 65.0 }\n'
    "}\n"
)


def ensure_ado_profiles_table() -> None:
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_PROFILES')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_PROFILES')} (
            PROFILE_ID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
            PROFILE_NAME NVARCHAR(200) NOT NULL,
            IS_ACTIVE BIT NOT NULL DEFAULT 0,
            CONFIG_JSON NVARCHAR(MAX) NOT NULL,
            PAT_ENC NVARCHAR(MAX) NULL,
            SYNC_MODE NVARCHAR(50) NULL,
            LAST_SYNC_AT DATETIME2 NULL,
            UPDATED_AT DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            UPDATED_BY NVARCHAR(100) NULL
          );
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes
          WHERE name = 'IX_ADO_PROFILES_IS_ACTIVE' AND object_id = OBJECT_ID('{_fq('ADO_PROFILES')}')
        )
        BEGIN
          CREATE INDEX IX_ADO_PROFILES_IS_ACTIVE ON {_fq('ADO_PROFILES')} (IS_ACTIVE);
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (
          SELECT 1 FROM sys.indexes
          WHERE name = 'IX_ADO_PROFILES_PROFILE_NAME' AND object_id = OBJECT_ID('{_fq('ADO_PROFILES')}')
        )
        BEGIN
          CREATE INDEX IX_ADO_PROFILES_PROFILE_NAME ON {_fq('ADO_PROFILES')} (PROFILE_NAME);
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_PROFILES')}', 'PAT_ENC') IS NULL
        BEGIN
          ALTER TABLE {_fq('ADO_PROFILES')} ADD PAT_ENC NVARCHAR(MAX) NULL;
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_PROFILES')}', 'SYNC_MODE') IS NULL
        BEGIN
          ALTER TABLE {_fq('ADO_PROFILES')} ADD SYNC_MODE NVARCHAR(50) NULL;
        END
        """
    )
    execute(
        f"""
        IF COL_LENGTH('{_fq('ADO_PROFILES')}', 'LAST_SYNC_AT') IS NULL
        BEGIN
          ALTER TABLE {_fq('ADO_PROFILES')} ADD LAST_SYNC_AT DATETIME2 NULL;
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (SELECT 1 FROM {_fq('ADO_PROFILES')})
        BEGIN
          INSERT INTO {_fq('ADO_PROFILES')}
            (PROFILE_NAME, IS_ACTIVE, CONFIG_JSON, UPDATED_AT, UPDATED_BY)
          VALUES (?, 1, ?, SYSUTCDATETIME(), ?);
        END
        """,
        (ADO_DEFAULT_PROFILE_NAME, ADO_DEFAULT_PROFILE_CONFIG_JSON, "SYSTEM"),
    )


def fetch_ado_profiles() -> pd.DataFrame:
    ensure_ado_profiles_table()
    return fetch_df(
        f"""
        SELECT PROFILE_ID, PROFILE_NAME, IS_ACTIVE, UPDATED_AT, UPDATED_BY
        FROM {_fq('ADO_PROFILES')}
        ORDER BY IS_ACTIVE DESC, PROFILE_NAME
        """
    )


def fetch_ado_profiles_full() -> pd.DataFrame:
    ensure_ado_profiles_table()
    return fetch_df(
        f"""
        SELECT
          PROFILE_ID,
          PROFILE_NAME,
          IS_ACTIVE,
          CONFIG_JSON,
          UPDATED_AT,
          UPDATED_BY,
          CASE WHEN PAT_ENC IS NOT NULL AND LTRIM(RTRIM(PAT_ENC)) <> '' THEN 1 ELSE 0 END AS HAS_PAT,
          SYNC_MODE,
          LAST_SYNC_AT
        FROM {_fq('ADO_PROFILES')}
        ORDER BY IS_ACTIVE DESC, PROFILE_NAME
        """
    )


def fetch_ado_workitem_lookup(ids: Sequence[int]) -> pd.DataFrame:
    ensure_ado_workitem_lookup_table()
    if not ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(ids))
    return fetch_df(
        f"""
        SELECT WORKITEM_ID, WORKITEM_TYPE, TITLE, STATE, PARENT_ID, CHANGED_DATE, UPDATED_AT
        FROM {_fq('ADO_WORKITEM_LOOKUP')}
        WHERE WORKITEM_ID IN ({placeholders})
        """,
        tuple(ids),
    )


def fetch_ado_workitem_lookup_by_type(workitem_type: str) -> pd.DataFrame:
    ensure_ado_workitem_lookup_table()
    return fetch_df(
        f"""
        SELECT WORKITEM_ID, WORKITEM_TYPE, TITLE, STATE, PARENT_ID, CHANGED_DATE
        FROM {_fq('ADO_WORKITEM_LOOKUP')}
        WHERE UPPER(WORKITEM_TYPE) = UPPER(%s)
        """,
        (workitem_type,),
    )


def fetch_ado_workitem_dates(ids: Sequence[int]) -> pd.DataFrame:
    ensure_ado_timeline_tables()
    if not ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(ids))
    return fetch_df(
        f"""
        SELECT WORKITEM_ID, WORKITEM_TYPE, START_DATE, END_DATE, ITERATION_PATH, UPDATED_AT
        FROM {_fq('ADO_WORKITEM_DATES')}
        WHERE WORKITEM_ID IN ({placeholders})
        """,
        tuple(ids),
    )


def fetch_ado_workitem_links(ids: Sequence[int]) -> pd.DataFrame:
    ensure_ado_timeline_tables()
    if not ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(ids))
    params = tuple(ids) + tuple(ids)
    return fetch_df(
        f"""
        SELECT SOURCE_ID, TARGET_ID, LINK_CATEGORY, LINK_TYPE, UPDATED_AT
        FROM {_fq('ADO_WORKITEM_LINKS')}
        WHERE SOURCE_ID IN ({placeholders})
           OR TARGET_ID IN ({placeholders})
        """,
        params,
    )


def fetch_active_ado_profile_config() -> Dict[str, Any]:
    ensure_ado_profiles_table()
    df = fetch_df(
        f"""
        SELECT TOP 1 CONFIG_JSON
        FROM {_fq('ADO_PROFILES')}
        WHERE IS_ACTIVE = 1
        ORDER BY UPDATED_AT DESC
        """
    )
    if df is None or df.empty:
        df = fetch_df(
            f"""
            SELECT TOP 1 CONFIG_JSON
            FROM {_fq('ADO_PROFILES')}
            ORDER BY UPDATED_AT DESC
            """
        )
        if df is None or df.empty:
            return {}
        warnings.warn("No active ADO profile found; using first available profile.")
    raw = df.iloc[0].get("CONFIG_JSON") if df is not None and not df.empty else None
    if isinstance(raw, str) and raw.strip():
        try:
            payload = json.loads(raw)
        except Exception:
            payload = {}
    else:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def get_profile_pat(profile_id: str) -> Optional[str]:
    ensure_ado_profiles_table()
    if not profile_id:
        return None
    if not ALLOW_DB_PAT_FALLBACK:
        return None
    df = fetch_df(
        f"SELECT PAT_ENC FROM {_fq('ADO_PROFILES')} WHERE PROFILE_ID = %s",
        (profile_id,),
    )
    if df is None or df.empty:
        return None
    raw = df.iloc[0].get("PAT_ENC")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return None


@_versioned_write("ADO_PROFILES.SET_PAT")
def set_profile_pat(profile_id: str, pat_plaintext: str, updated_by: Optional[str] = None) -> None:
    ensure_ado_profiles_table()
    execute(
        f"""
        UPDATE {_fq('ADO_PROFILES')}
        SET PAT_ENC = %s,
            UPDATED_AT = SYSUTCDATETIME(),
            UPDATED_BY = %s
        WHERE PROFILE_ID = %s
        """,
        (pat_plaintext, updated_by, profile_id),
    )


def get_profile_sync_mode(profile_id: str) -> Optional[str]:
    ensure_ado_profiles_table()
    if not profile_id:
        return None
    df = fetch_df(
        f"SELECT SYNC_MODE FROM {_fq('ADO_PROFILES')} WHERE PROFILE_ID = %s",
        (profile_id,),
    )
    if df is None or df.empty:
        return None
    val = df.iloc[0].get("SYNC_MODE")
    return str(val).strip() if isinstance(val, str) and str(val).strip() else None


@_versioned_write("ADO_PROFILES.SET_SYNC_MODE")
def set_profile_sync_mode(profile_id: str, mode: str, updated_by: Optional[str] = None) -> None:
    ensure_ado_profiles_table()
    execute(
        f"""
        UPDATE {_fq('ADO_PROFILES')}
        SET SYNC_MODE = %s,
            UPDATED_AT = SYSUTCDATETIME(),
            UPDATED_BY = %s
        WHERE PROFILE_ID = %s
        """,
        (mode, updated_by, profile_id),
    )


@_versioned_write("ADO_PROFILES.SET_LAST_SYNC")
def set_profile_last_sync_at(profile_id: str) -> None:
    ensure_ado_profiles_table()
    execute(
        f"""
        UPDATE {_fq('ADO_PROFILES')}
        SET LAST_SYNC_AT = SYSUTCDATETIME()
        WHERE PROFILE_ID = %s
        """,
        (profile_id,),
    )


@_versioned_write("ADO_PROFILES.SET_ACTIVE")
def set_active_ado_profile(profile_id: str) -> None:
    ensure_ado_profiles_table()
    execute(
        f"""
        DECLARE @profile_id UNIQUEIDENTIFIER = ?;
        BEGIN TRY
          BEGIN TRAN;
          UPDATE {_fq('ADO_PROFILES')} SET IS_ACTIVE = 0;
          UPDATE {_fq('ADO_PROFILES')} SET IS_ACTIVE = 1 WHERE PROFILE_ID = @profile_id;
          COMMIT TRAN;
        END TRY
        BEGIN CATCH
          IF @@TRANCOUNT > 0 ROLLBACK TRAN;
          THROW;
        END CATCH
        """,
        (profile_id,),
    )


@_versioned_write("ADO_PROFILES.UPSERT")
def upsert_ado_profile(
    profile_id: Optional[str] = None,
    profile_name: Optional[str] = None,
    config_json: Any = None,
    is_active: bool = False,
    updated_by: Optional[str] = None,
) -> None:
    ensure_ado_profiles_table()
    if isinstance(config_json, bool) and not isinstance(is_active, bool):
        config_json, is_active = is_active, config_json
    name = str(profile_name or "").strip()
    payload = config_json
    if isinstance(config_json, dict):
        payload = json.dumps(config_json)
    elif config_json is None:
        payload = "{}"
    is_active_bit = 1 if is_active else 0
    execute(
        f"""
        DECLARE @profile_id UNIQUEIDENTIFIER = ?;
        DECLARE @is_active BIT = ?;
        DECLARE @profile_name NVARCHAR(200) = ?;
        DECLARE @config_json NVARCHAR(MAX) = ?;
        DECLARE @updated_by NVARCHAR(100) = ?;

        IF @profile_id IS NULL
          SET @profile_id = NEWID();

        BEGIN TRY
          BEGIN TRAN;
          IF @is_active = 1
            UPDATE {_fq('ADO_PROFILES')} SET IS_ACTIVE = 0;
          MERGE INTO {_fq('ADO_PROFILES')} AS t
          USING (SELECT @profile_id AS PROFILE_ID) AS s
          ON t.PROFILE_ID = s.PROFILE_ID
          WHEN MATCHED THEN UPDATE SET
            PROFILE_NAME = @profile_name,
            IS_ACTIVE = @is_active,
            CONFIG_JSON = @config_json,
            UPDATED_AT = SYSUTCDATETIME(),
            UPDATED_BY = @updated_by
          WHEN NOT MATCHED THEN INSERT
            (PROFILE_ID, PROFILE_NAME, IS_ACTIVE, CONFIG_JSON, UPDATED_AT, UPDATED_BY)
          VALUES
            (@profile_id, @profile_name, @is_active, @config_json, SYSUTCDATETIME(), @updated_by);
          COMMIT TRAN;
        END TRY
        BEGIN CATCH
          IF @@TRANCOUNT > 0 ROLLBACK TRAN;
          THROW;
        END CATCH
        """,
        (profile_id, is_active_bit, name, payload, updated_by),
    )


@_versioned_write("ADO_PROFILES.DELETE")
def delete_ado_profile(profile_id: str) -> None:
    ensure_ado_profiles_table()
    execute(
        f"DELETE FROM {_fq('ADO_PROFILES')} WHERE PROFILE_ID = %s",
        (profile_id,),
    )


@_versioned_write("ADO_WORKITEM_LOOKUP.UPSERT")
def upsert_ado_workitem_lookup(rows: List[Tuple[Any, ...]]) -> None:
    ensure_ado_workitem_lookup_table()
    if not rows:
        return
    execute(
        f"""
        MERGE INTO {_fq('ADO_WORKITEM_LOOKUP')} AS t
        USING (
          SELECT %s AS WORKITEM_ID,
                 %s AS WORKITEM_TYPE,
                 %s AS TITLE,
                 %s AS STATE,
                 %s AS PARENT_ID,
                 %s AS CHANGED_DATE
        ) AS s
        ON t.WORKITEM_ID = s.WORKITEM_ID
        WHEN MATCHED AND (
          t.CHANGED_DATE IS NULL OR s.CHANGED_DATE IS NULL OR s.CHANGED_DATE >= t.CHANGED_DATE
        ) THEN UPDATE SET
          WORKITEM_TYPE = s.WORKITEM_TYPE,
          TITLE = s.TITLE,
          STATE = s.STATE,
          PARENT_ID = s.PARENT_ID,
          CHANGED_DATE = COALESCE(s.CHANGED_DATE, t.CHANGED_DATE),
          UPDATED_AT = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT
          (WORKITEM_ID, WORKITEM_TYPE, TITLE, STATE, PARENT_ID, CHANGED_DATE, UPDATED_AT)
        VALUES
          (s.WORKITEM_ID, s.WORKITEM_TYPE, s.TITLE, s.STATE, s.PARENT_ID, s.CHANGED_DATE, SYSUTCDATETIME());
        """,
        rows,
        many=True,
    )


@_versioned_write("ADO_FEATURE_PROGRESS.UPSERT")
def upsert_ado_feature_progress(rows: List[Tuple[Any, ...]]) -> int:
    ensure_ado_progress_tables()
    if not rows:
        return 0
    execute(
        f"""
        MERGE INTO {_fq('ADO_FEATURE_PROGRESS')} AS t
        USING (
          SELECT
            %s AS FEATURE_ID,
            %s AS PROPOSED_SP,
            %s AS INPROGRESS_SP,
            %s AS COMPLETED_SP,
            %s AS TOTAL_SP,
            %s AS PCT_COMPLETE
        ) AS s
        ON t.FEATURE_ID = s.FEATURE_ID
        WHEN MATCHED THEN UPDATE SET
          PROPOSED_SP = s.PROPOSED_SP,
          INPROGRESS_SP = s.INPROGRESS_SP,
          COMPLETED_SP = s.COMPLETED_SP,
          TOTAL_SP = s.TOTAL_SP,
          PCT_COMPLETE = s.PCT_COMPLETE,
          UPDATED_AT = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT
          (FEATURE_ID, PROPOSED_SP, INPROGRESS_SP, COMPLETED_SP, TOTAL_SP, PCT_COMPLETE, UPDATED_AT)
        VALUES
          (s.FEATURE_ID, s.PROPOSED_SP, s.INPROGRESS_SP, s.COMPLETED_SP, s.TOTAL_SP, s.PCT_COMPLETE, SYSUTCDATETIME());
        """,
        rows,
        many=True,
    )
    return len(rows)


@_versioned_write("ADO_EPIC_PROGRESS.UPSERT")
def upsert_ado_epic_progress(rows: List[Tuple[Any, ...]]) -> int:
    ensure_ado_progress_tables()
    if not rows:
        return 0
    normalized_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        if isinstance(r, (list, tuple)):
            vals = list(r)
        else:
            continue
        if len(vals) >= 9:
            normalized_rows.append(
                (
                    vals[0], vals[1], vals[2], vals[3], vals[4], vals[5],
                    vals[6], vals[7], vals[8],
                )
            )
        elif len(vals) >= 6:
            normalized_rows.append(
                (
                    vals[0], vals[1], vals[2], vals[3], vals[4], vals[5],
                    None, None, None,
                )
            )
        else:
            continue
    if not normalized_rows:
        return 0
    execute(
        f"""
        MERGE INTO {_fq('ADO_EPIC_PROGRESS')} AS t
        USING (
          SELECT
            %s AS EPIC_ID,
            %s AS PROPOSED_SP,
            %s AS INPROGRESS_SP,
            %s AS COMPLETED_SP,
            %s AS TOTAL_SP,
            %s AS PCT_COMPLETE,
            %s AS FEATURE_COUNT_TOTAL,
            %s AS FEATURE_COUNT_WITH_STORIES,
            %s AS FEATURE_COUNT_WITH_SP
        ) AS s
        ON t.EPIC_ID = s.EPIC_ID
        WHEN MATCHED THEN UPDATE SET
          PROPOSED_SP = s.PROPOSED_SP,
          INPROGRESS_SP = s.INPROGRESS_SP,
          COMPLETED_SP = s.COMPLETED_SP,
          TOTAL_SP = s.TOTAL_SP,
          PCT_COMPLETE = s.PCT_COMPLETE,
          FEATURE_COUNT_TOTAL = s.FEATURE_COUNT_TOTAL,
          FEATURE_COUNT_WITH_STORIES = s.FEATURE_COUNT_WITH_STORIES,
          FEATURE_COUNT_WITH_SP = s.FEATURE_COUNT_WITH_SP,
          UPDATED_AT = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT
          (EPIC_ID, PROPOSED_SP, INPROGRESS_SP, COMPLETED_SP, TOTAL_SP, PCT_COMPLETE, FEATURE_COUNT_TOTAL, FEATURE_COUNT_WITH_STORIES, FEATURE_COUNT_WITH_SP, UPDATED_AT)
        VALUES
          (s.EPIC_ID, s.PROPOSED_SP, s.INPROGRESS_SP, s.COMPLETED_SP, s.TOTAL_SP, s.PCT_COMPLETE, s.FEATURE_COUNT_TOTAL, s.FEATURE_COUNT_WITH_STORIES, s.FEATURE_COUNT_WITH_SP, SYSUTCDATETIME());
        """,
        normalized_rows,
        many=True,
    )
    return len(normalized_rows)


@_versioned_write("ADO_WORKITEM_DATES.UPSERT")
def upsert_ado_workitem_dates(rows: List[Tuple[Any, ...]]) -> int:
    ensure_ado_timeline_tables()
    if not rows:
        return 0
    execute(
        f"""
        MERGE INTO {_fq('ADO_WORKITEM_DATES')} AS t
        USING (
          SELECT
            %s AS WORKITEM_ID,
            %s AS WORKITEM_TYPE,
            %s AS START_DATE,
            %s AS END_DATE,
            %s AS ITERATION_PATH
        ) AS s
        ON t.WORKITEM_ID = s.WORKITEM_ID
        WHEN MATCHED THEN UPDATE SET
          WORKITEM_TYPE = s.WORKITEM_TYPE,
          START_DATE = TRY_CONVERT(DATE, s.START_DATE),
          END_DATE = TRY_CONVERT(DATE, s.END_DATE),
          ITERATION_PATH = s.ITERATION_PATH,
          UPDATED_AT = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT
          (WORKITEM_ID, WORKITEM_TYPE, START_DATE, END_DATE, ITERATION_PATH, UPDATED_AT)
        VALUES
          (s.WORKITEM_ID, s.WORKITEM_TYPE, TRY_CONVERT(DATE, s.START_DATE), TRY_CONVERT(DATE, s.END_DATE), s.ITERATION_PATH, SYSUTCDATETIME());
        """,
        rows,
        many=True,
    )
    return len(rows)


@_versioned_write("ADO_WORKITEM_LINKS.UPSERT")
def upsert_ado_workitem_links(rows: List[Tuple[Any, ...]]) -> int:
    ensure_ado_timeline_tables()
    if not rows:
        return 0
    execute(
        f"""
        MERGE INTO {_fq('ADO_WORKITEM_LINKS')} AS t
        USING (
          SELECT
            %s AS SOURCE_ID,
            %s AS TARGET_ID,
            %s AS LINK_CATEGORY,
            %s AS LINK_TYPE
        ) AS s
        ON t.SOURCE_ID = s.SOURCE_ID
           AND t.TARGET_ID = s.TARGET_ID
           AND UPPER(t.LINK_CATEGORY) = UPPER(s.LINK_CATEGORY)
           AND UPPER(t.LINK_TYPE) = UPPER(s.LINK_TYPE)
        WHEN MATCHED THEN UPDATE SET
          LINK_CATEGORY = s.LINK_CATEGORY,
          LINK_TYPE = s.LINK_TYPE,
          UPDATED_AT = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT
          (SOURCE_ID, TARGET_ID, LINK_CATEGORY, LINK_TYPE, UPDATED_AT)
        VALUES
          (s.SOURCE_ID, s.TARGET_ID, s.LINK_CATEGORY, s.LINK_TYPE, SYSUTCDATETIME());
        """,
        rows,
        many=True,
    )
    return len(rows)


# =========================================================
# ADO portfolio settings (persisted)
# =========================================================

def ensure_ado_portfolio_settings_table() -> None:
    execute(
        f"""
        IF OBJECT_ID('{_svq('ADO_PORTFOLIO_SETTINGS')}', 'U') IS NULL
        BEGIN
          CREATE TABLE {_fq('ADO_PORTFOLIO_SETTINGS')} (
            PORTFOLIO_NAME NVARCHAR(200) PRIMARY KEY,
            SETTINGS_JSON NVARCHAR(MAX) NULL,
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL
          );
        END
        """
    )


def load_ado_portfolio_settings() -> Dict[str, Dict[str, Any]]:
    ensure_ado_portfolio_settings_table()
    try:
        df = fetch_df(
            f"SELECT PORTFOLIO_NAME, SETTINGS_JSON FROM {_fq('ADO_PORTFOLIO_SETTINGS')}"
        )
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        name = str(row.get("PORTFOLIO_NAME") or "").strip()
        if not name:
            continue
        raw = row.get("SETTINGS_JSON")
        try:
            payload = json.loads(raw) if isinstance(raw, str) and raw.strip() else {}
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            out[name] = payload
    return out


@_versioned_write("ADO_PORTFOLIO_SETTINGS.SAVE")
def save_ado_portfolio_settings(portfolio_name: str, settings: Dict[str, Any], updated_by: Optional[str] = None) -> None:
    ensure_ado_portfolio_settings_table()
    name = (portfolio_name or "").strip()
    if not name:
        return
    payload = json.dumps(settings or {})
    execute(
        f"""
        MERGE INTO {_fq('ADO_PORTFOLIO_SETTINGS')} AS t
        USING (SELECT ? AS PORTFOLIO_NAME, ? AS SETTINGS_JSON, ? AS UPDATED_BY) AS s
        ON t.PORTFOLIO_NAME = s.PORTFOLIO_NAME
        WHEN MATCHED THEN UPDATE SET
          SETTINGS_JSON = s.SETTINGS_JSON,
          UPDATED_AT = SYSDATETIME(),
          UPDATED_BY = s.UPDATED_BY
        WHEN NOT MATCHED THEN INSERT (PORTFOLIO_NAME, SETTINGS_JSON, UPDATED_AT, UPDATED_BY)
          VALUES (s.PORTFOLIO_NAME, s.SETTINGS_JSON, SYSDATETIME(), s.UPDATED_BY);
        """,
        (name, payload, updated_by),
    )


@_versioned_write("ADO_PORTFOLIO_SETTINGS.DELETE")
def delete_ado_portfolio_settings(portfolio_name: str) -> None:
    ensure_ado_portfolio_settings_table()
    name = (portfolio_name or "").strip()
    if not name:
        return
    execute(
        f"DELETE FROM {_fq('ADO_PORTFOLIO_SETTINGS')} WHERE PORTFOLIO_NAME = %s",
        (name,),
    )


def normalize_team_numeric_types() -> None:
    # Not required on SQL Server; created with DECIMAL already
    return None


def repair_ado_features_leaf_teams() -> Dict[str, Any]:
    ensure_ado_minimal_tables()
    df = fetch_df(
        f"""
        SELECT FEATURE_ID, AREA_PATH_RAW, TEAM_RAW, TEAM_VARIANT_KEY, PROGRAM_RAW
        FROM {_fq('ADO_FEATURES')}
        """
    )
    if df is None or df.empty:
        return {
            "rows_total": 0,
            "rows_changed": 0,
            "dup_tail_pct_before": 0.0,
            "dup_tail_pct_after": 0.0,
        }

    def _dup_tail_pct(series: pd.Series) -> float:
        vals = series.fillna("").astype(str)
        if vals.empty:
            return 0.0
        flags = vals.apply(
            lambda k: (
                len([p for p in str(k).split("|") if p]) >= 2
                and [p for p in str(k).split("|") if p][-1].strip().upper()
                == [p for p in str(k).split("|") if p][-2].strip().upper()
            )
        )
        return float(flags.mean() * 100.0) if len(flags) else 0.0

    from utils.ado import repair_leaf_teams

    before = df.copy()
    after = repair_leaf_teams(df.copy())

    before_team = before.get("TEAM_RAW", pd.Series([None] * len(before))).fillna("").astype(str).str.strip()
    after_team = after.get("TEAM_RAW", pd.Series([None] * len(after))).fillna("").astype(str).str.strip()
    before_key = before.get("TEAM_VARIANT_KEY", pd.Series([None] * len(before))).fillna("").astype(str).str.strip()
    after_key = after.get("TEAM_VARIANT_KEY", pd.Series([None] * len(after))).fillna("").astype(str).str.strip()
    changed_mask = (before_team != after_team) | (before_key != after_key)

    changed = after.loc[changed_mask, ["FEATURE_ID", "TEAM_RAW", "TEAM_VARIANT_KEY"]].copy()
    rows = []
    for _, row in changed.iterrows():
        fid = row.get("FEATURE_ID")
        if fid is None or (isinstance(fid, str) and not fid.strip()):
            continue
        rows.append((row.get("TEAM_RAW"), row.get("TEAM_VARIANT_KEY"), str(fid).strip()))

    if rows:
        execute(
            f"""
            UPDATE {_fq('ADO_FEATURES')}
            SET TEAM_RAW = %s,
                TEAM_VARIANT_KEY = %s
            WHERE FEATURE_ID = %s
            """,
            rows,
            many=True,
        )

    return {
        "rows_total": int(len(df)),
        "rows_changed": int(len(rows)),
        "dup_tail_pct_before": round(_dup_tail_pct(before_key), 3),
        "dup_tail_pct_after": round(_dup_tail_pct(after_key), 3),
    }


def repair_team_fte_values() -> None:
    # Optional data fixups can be implemented here if needed
    return None


# =========================================================
# Team composition history (table + effective view)
# =========================================================

def ensure_team_composition_history() -> None:
    db, sch = _db_and_schema()
    execute(
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'TEAM_COMPOSITION_HISTORY' AND schema_id = SCHEMA_ID('{sch}'))
        BEGIN
          CREATE TABLE {db}.{sch}.TEAM_COMPOSITION_HISTORY (
            TEAMID NVARCHAR(255) NOT NULL,
            YEAR   INT NOT NULL,
            PI     INT NOT NULL CONSTRAINT DF_TEAM_COMPOSITION_HISTORY_PI DEFAULT 0,
            TEAMFTE FLOAT DEFAULT 0,
            DELIVERY_TEAM_FTE FLOAT DEFAULT 0,
            CONTRACTOR_CS_FTE FLOAT DEFAULT 0,
            CONTRACTOR_C_FTE  FLOAT DEFAULT 0,
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            CONSTRAINT PK_TEAM_COMPOSITION_HISTORY PRIMARY KEY (TEAMID, YEAR, PI)
          );
        END
        """
    )
    # If table exists but PI is nullable from an older version, correct it and ensure PK exists
    execute(
        f"""
        IF EXISTS (
          SELECT 1 FROM sys.columns c
          WHERE c.object_id = OBJECT_ID('{db}.{sch}.TEAM_COMPOSITION_HISTORY')
            AND c.name = 'PI' AND c.is_nullable = 1
        )
        BEGIN
          UPDATE {db}.{sch}.TEAM_COMPOSITION_HISTORY SET PI = 0 WHERE PI IS NULL;
          ALTER TABLE {db}.{sch}.TEAM_COMPOSITION_HISTORY ALTER COLUMN PI INT NOT NULL;
        END

        IF NOT EXISTS (
          SELECT 1 FROM sys.key_constraints
          WHERE name = 'PK_TEAM_COMPOSITION_HISTORY'
            AND parent_object_id = OBJECT_ID('{db}.{sch}.TEAM_COMPOSITION_HISTORY')
        )
        BEGIN
          ALTER TABLE {db}.{sch}.TEAM_COMPOSITION_HISTORY
            ADD CONSTRAINT PK_TEAM_COMPOSITION_HISTORY PRIMARY KEY (TEAMID, YEAR, PI);
        END
        """
    )
    # Effective view with PI expansion 1..4 and most-specific/latest wins
    try:
        execute("SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
    except Exception:
        pass
    execute(
        f"""
        CREATE OR ALTER VIEW {sch}.VW_TEAM_COMPOSITION_EFFECTIVE AS
        WITH base AS (
          SELECT TEAMID, YEAR, PI,
                 TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE,
                 UPDATED_AT,
                 CASE WHEN PI IS NULL OR PI = 0 THEN 0 ELSE 1 END AS SPECIFICITY
          FROM {db}.{sch}.TEAM_COMPOSITION_HISTORY
        ), nums AS (
          SELECT 1 AS PI UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        ), expanded AS (
          SELECT b.TEAMID, b.YEAR,
                 CASE WHEN b.PI IS NULL OR b.PI = 0 THEN n.PI ELSE b.PI END AS PI,
                 b.TEAMFTE, b.DELIVERY_TEAM_FTE, b.CONTRACTOR_CS_FTE, b.CONTRACTOR_C_FTE,
                 b.UPDATED_AT, b.SPECIFICITY
          FROM base b
          JOIN nums n ON (b.PI IS NULL OR b.PI = 0) OR b.PI = n.PI
        ), ranked AS (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY TEAMID, YEAR, PI
            ORDER BY SPECIFICITY DESC, UPDATED_AT DESC
          ) AS rn
          FROM expanded
        )
        SELECT TEAMID, YEAR, PI,
               TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE,
               UPDATED_AT
       FROM ranked
       WHERE rn = 1
       """
    )
    try:
        ensure_composition_changelog_tables()
    except Exception:
        pass
    # =========================================================
    # Program composition history (table + effective view)
    # =========================================================
def ensure_program_composition_history() -> None:
    db, sch = _db_and_schema()
    execute(
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PROGRAM_COMPOSITION_HISTORY' AND schema_id = SCHEMA_ID('{sch}'))
        BEGIN
          CREATE TABLE {db}.{sch}.PROGRAM_COMPOSITION_HISTORY (
            PROGRAMID NVARCHAR(255) NOT NULL,
            YEAR      INT NOT NULL,
            PI        INT NOT NULL CONSTRAINT DF_PROGRAM_COMPOSITION_HISTORY_PI DEFAULT 0,
            PROGRAMFTE FLOAT DEFAULT 0,
            UPDATED_AT DATETIME2 DEFAULT SYSDATETIME(),
            UPDATED_BY NVARCHAR(255) NULL,
            CONSTRAINT PK_PROGRAM_COMPOSITION_HISTORY PRIMARY KEY (PROGRAMID, YEAR, PI)
          );
        END
        """
    )
    execute(
        f"""
        UPDATE {db}.{sch}.PROGRAM_COMPOSITION_HISTORY SET PI = 0 WHERE PI IS NULL;

        IF NOT EXISTS (
          SELECT 1 FROM sys.key_constraints
          WHERE name = 'PK_PROGRAM_COMPOSITION_HISTORY'
            AND parent_object_id = OBJECT_ID('{db}.{sch}.PROGRAM_COMPOSITION_HISTORY')
        )
        BEGIN
          ALTER TABLE {db}.{sch}.PROGRAM_COMPOSITION_HISTORY
            ADD CONSTRAINT PK_PROGRAM_COMPOSITION_HISTORY PRIMARY KEY (PROGRAMID, YEAR, PI);
        END
        """
    )
    try:
        execute("SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
    except Exception:
        pass
    execute(
        f"""
        CREATE OR ALTER VIEW {sch}.VW_PROGRAM_COMPOSITION_EFFECTIVE AS
        WITH base AS (
          SELECT PROGRAMID, YEAR, PI,
                 PROGRAMFTE,
                 UPDATED_AT,
                 CASE WHEN PI IS NULL OR PI = 0 THEN 0 ELSE 1 END AS SPECIFICITY
          FROM {db}.{sch}.PROGRAM_COMPOSITION_HISTORY
        ), nums AS (
          SELECT 1 AS PI UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        ), expanded AS (
          SELECT b.PROGRAMID, b.YEAR,
                 CASE WHEN b.PI IS NULL OR b.PI = 0 THEN n.PI ELSE b.PI END AS PI,
                 b.PROGRAMFTE, b.UPDATED_AT, b.SPECIFICITY
          FROM base b
          JOIN nums n ON (b.PI IS NULL OR b.PI = 0) OR b.PI = n.PI
        ), ranked AS (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY PROGRAMID, YEAR, PI
            ORDER BY SPECIFICITY DESC, UPDATED_AT DESC
          ) AS rn
          FROM expanded
        )
        SELECT PROGRAMID, YEAR, PI,
               PROGRAMFTE, UPDATED_AT
        FROM ranked
       WHERE rn = 1
       """
    )
    try:
        ensure_composition_changelog_tables()
    except Exception:
        pass


def ensure_composition_changelog_tables() -> None:
    """Create durable changelog tables for composition updates (derived from headcount saves)."""
    db, sch = _db_and_schema()
    execute(
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'TCO_TEAM_COMPOSITION_CHANGELOG' AND schema_id = SCHEMA_ID('{sch}'))
        BEGIN
          CREATE TABLE {db}.{sch}.TCO_TEAM_COMPOSITION_CHANGELOG (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            TEAMID NVARCHAR(255) NOT NULL,
            YEAR INT NOT NULL,
            PI INT NOT NULL,
            OLD_TEAMFTE FLOAT NULL,
            OLD_DELIVERY_TEAM_FTE FLOAT NULL,
            OLD_CONTRACTOR_C_FTE FLOAT NULL,
            OLD_CONTRACTOR_CS_FTE FLOAT NULL,
            NEW_TEAMFTE FLOAT NULL,
            NEW_DELIVERY_TEAM_FTE FLOAT NULL,
            NEW_CONTRACTOR_C_FTE FLOAT NULL,
            NEW_CONTRACTOR_CS_FTE FLOAT NULL,
            CHANGED_BY NVARCHAR(255) NULL,
            CHANGED_AT DATETIME2 NOT NULL CONSTRAINT DF_TCO_TEAM_COMPOSITION_CHANGELOG_CHANGED_AT DEFAULT SYSUTCDATETIME(),
            SOURCE NVARCHAR(40) NOT NULL CONSTRAINT DF_TCO_TEAM_COMPOSITION_CHANGELOG_SOURCE DEFAULT 'HEADCOUNT_SAVE'
          );
        END
        """
    )
    execute(
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'TCO_PROGRAM_COMPOSITION_CHANGELOG' AND schema_id = SCHEMA_ID('{sch}'))
        BEGIN
          CREATE TABLE {db}.{sch}.TCO_PROGRAM_COMPOSITION_CHANGELOG (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            PROGRAMID NVARCHAR(255) NOT NULL,
            YEAR INT NOT NULL,
            PI INT NOT NULL,
            OLD_PROGRAMFTE FLOAT NULL,
            NEW_PROGRAMFTE FLOAT NULL,
            CHANGED_BY NVARCHAR(255) NULL,
            CHANGED_AT DATETIME2 NOT NULL CONSTRAINT DF_TCO_PROGRAM_COMPOSITION_CHANGELOG_CHANGED_AT DEFAULT SYSUTCDATETIME(),
            SOURCE NVARCHAR(40) NOT NULL CONSTRAINT DF_TCO_PROGRAM_COMPOSITION_CHANGELOG_SOURCE DEFAULT 'HEADCOUNT_SAVE'
          );
        END
        """
    )


@_versioned_write("TEAM_COMPOSITION.RECOMPUTE")
def recompute_team_composition_from_headcount(
    *,
    team_id: str,
    year: int,
    pi: int,
    changed_by: str = "",
    source: str = "HEADCOUNT_SAVE",
) -> bool:
    """Recompute TEAM_COMPOSITION_HISTORY row from headcount sources and log old→new."""
    ensure_location_and_contractor_tables()
    ensure_team_composition_history()
    ensure_composition_changelog_tables()

    team_id = str(team_id or "").strip()
    if not team_id:
        return False
    year_i = int(year)
    pi_i = int(pi)

    # Read current composition (old values).
    old = {"TEAMFTE": 0.0, "DELIVERY_TEAM_FTE": 0.0, "CONTRACTOR_C_FTE": 0.0, "CONTRACTOR_CS_FTE": 0.0}
    try:
        df_old = fetch_df(
            f"""
            SELECT TOP 1 TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE
            FROM {_fq('TEAM_COMPOSITION_HISTORY')}
            WHERE TEAMID = %s AND YEAR = %s AND PI = %s
            """,
            (team_id, year_i, pi_i),
        )
        if df_old is not None and not df_old.empty:
            r = df_old.iloc[0]
            old = {
                "TEAMFTE": float(r.get("TEAMFTE") or 0.0),
                "DELIVERY_TEAM_FTE": float(r.get("DELIVERY_TEAM_FTE") or 0.0),
                "CONTRACTOR_C_FTE": float(r.get("CONTRACTOR_C_FTE") or 0.0),
                "CONTRACTOR_CS_FTE": float(r.get("CONTRACTOR_CS_FTE") or 0.0),
            }
    except Exception:
        pass

    # Compute new values from headcount.
    new_vals = {"TEAMFTE": 0.0, "DELIVERY_TEAM_FTE": 0.0, "CONTRACTOR_C_FTE": 0.0, "CONTRACTOR_CS_FTE": 0.0}
    try:
        df_hc = fetch_df(
            f"""
            SELECT UPPER(LTRIM(RTRIM(CLASS))) AS CLASS, SUM(COALESCE(TRY_CONVERT(FLOAT, HEADCOUNT), 0)) AS HC
            FROM {_fq('VW_TEAM_HEADCOUNT_EFFECTIVE')}
            WHERE TEAMID = %s AND YEAR = %s AND PI = %s
            GROUP BY UPPER(LTRIM(RTRIM(CLASS)))
            """,
            (team_id, year_i, pi_i),
        )
        if df_hc is not None and not df_hc.empty:
            m = {str(r["CLASS"]).upper(): float(r["HC"] or 0.0) for _, r in df_hc.iterrows()}
            new_vals["TEAMFTE"] = float(m.get("TEAM", 0.0))
            new_vals["DELIVERY_TEAM_FTE"] = float(m.get("DELIVERY", 0.0))
    except Exception:
        pass
    try:
        df_cons = fetch_df(
            f"""
            SELECT UPPER(LTRIM(RTRIM(CLASS))) AS CLASS, SUM(COALESCE(TRY_CONVERT(FLOAT, HEADCOUNT), 0)) AS HC
            FROM {_fq('VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE')}
            WHERE TEAMID = %s AND YEAR = %s AND PI = %s
            GROUP BY UPPER(LTRIM(RTRIM(CLASS)))
            """,
            (team_id, year_i, pi_i),
        )
        if df_cons is not None and not df_cons.empty:
            m = {str(r["CLASS"]).upper(): float(r["HC"] or 0.0) for _, r in df_cons.iterrows()}
            new_vals["CONTRACTOR_C_FTE"] = float(m.get("CONTRACTOR_C", 0.0))
            new_vals["CONTRACTOR_CS_FTE"] = float(m.get("CONTRACTOR_CS", 0.0))
    except Exception:
        pass

    def _eq(a: float, b: float) -> bool:
        return abs(float(a) - float(b)) <= 1e-9

    changed = not (
        _eq(old["TEAMFTE"], new_vals["TEAMFTE"])
        and _eq(old["DELIVERY_TEAM_FTE"], new_vals["DELIVERY_TEAM_FTE"])
        and _eq(old["CONTRACTOR_C_FTE"], new_vals["CONTRACTOR_C_FTE"])
        and _eq(old["CONTRACTOR_CS_FTE"], new_vals["CONTRACTOR_CS_FTE"])
    )
    if not changed:
        return True

    # Changelog insert (best-effort; should not block composition update).
    try:
        execute(
            f"""
            INSERT INTO {_fq('TCO_TEAM_COMPOSITION_CHANGELOG')}
              (TEAMID, YEAR, PI,
               OLD_TEAMFTE, OLD_DELIVERY_TEAM_FTE, OLD_CONTRACTOR_C_FTE, OLD_CONTRACTOR_CS_FTE,
               NEW_TEAMFTE, NEW_DELIVERY_TEAM_FTE, NEW_CONTRACTOR_C_FTE, NEW_CONTRACTOR_CS_FTE,
               CHANGED_BY, SOURCE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                team_id,
                year_i,
                pi_i,
                old["TEAMFTE"],
                old["DELIVERY_TEAM_FTE"],
                old["CONTRACTOR_C_FTE"],
                old["CONTRACTOR_CS_FTE"],
                new_vals["TEAMFTE"],
                new_vals["DELIVERY_TEAM_FTE"],
                new_vals["CONTRACTOR_C_FTE"],
                new_vals["CONTRACTOR_CS_FTE"],
                str(changed_by or "").strip(),
                str(source or "HEADCOUNT_SAVE").strip() or "HEADCOUNT_SAVE",
            ),
        )
    except Exception:
        pass

    # Upsert composition (update updated_at; update updated_by if the column exists).
    try:
        execute(
            f"""
            MERGE INTO {_fq('TEAM_COMPOSITION_HISTORY')} t
            USING (SELECT %s AS TEAMID, %s AS YEAR, %s AS PI, %s AS UPDATED_BY) s
            ON t.TEAMID = s.TEAMID AND t.YEAR = s.YEAR AND t.PI = s.PI
            WHEN MATCHED THEN UPDATE SET
              TEAMFTE = %s,
              DELIVERY_TEAM_FTE = %s,
              CONTRACTOR_C_FTE = %s,
              CONTRACTOR_CS_FTE = %s,
              UPDATED_AT = SYSDATETIME(),
              UPDATED_BY = COALESCE(s.UPDATED_BY, t.UPDATED_BY)
            WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE, UPDATED_AT, UPDATED_BY)
            VALUES (s.TEAMID, s.YEAR, s.PI, %s, %s, %s, %s, SYSDATETIME(), s.UPDATED_BY);
            """,
            (
                team_id,
                year_i,
                pi_i,
                str(changed_by or "").strip(),
                new_vals["TEAMFTE"],
                new_vals["DELIVERY_TEAM_FTE"],
                new_vals["CONTRACTOR_C_FTE"],
                new_vals["CONTRACTOR_CS_FTE"],
                new_vals["TEAMFTE"],
                new_vals["DELIVERY_TEAM_FTE"],
                new_vals["CONTRACTOR_C_FTE"],
                new_vals["CONTRACTOR_CS_FTE"],
            ),
        )
    except Exception:
        # Fallback when UPDATED_BY column isn't present.
        execute(
            f"""
            MERGE INTO {_fq('TEAM_COMPOSITION_HISTORY')} t
            USING (SELECT %s AS TEAMID, %s AS YEAR, %s AS PI) s
            ON t.TEAMID = s.TEAMID AND t.YEAR = s.YEAR AND t.PI = s.PI
            WHEN MATCHED THEN UPDATE SET
              TEAMFTE = %s,
              DELIVERY_TEAM_FTE = %s,
              CONTRACTOR_C_FTE = %s,
              CONTRACTOR_CS_FTE = %s,
              UPDATED_AT = SYSDATETIME()
            WHEN NOT MATCHED THEN INSERT (TEAMID, YEAR, PI, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE, UPDATED_AT)
            VALUES (s.TEAMID, s.YEAR, s.PI, %s, %s, %s, %s, SYSDATETIME());
            """,
            (
                team_id,
                year_i,
                pi_i,
                new_vals["TEAMFTE"],
                new_vals["DELIVERY_TEAM_FTE"],
                new_vals["CONTRACTOR_C_FTE"],
                new_vals["CONTRACTOR_CS_FTE"],
                new_vals["TEAMFTE"],
                new_vals["DELIVERY_TEAM_FTE"],
                new_vals["CONTRACTOR_C_FTE"],
                new_vals["CONTRACTOR_CS_FTE"],
            ),
        )
    return True


def _trace(msg: str) -> None:
    if os.getenv("TCO_SQL_TRACE") == "1":
        print(msg)


def _sql_trace_enabled() -> bool:
    if os.getenv("TCO_SQL_TRACE") == "1":
        return True
    try:
        secrets_obj = getattr(_st, "secrets", None)
        if secrets_obj and ("TCO_SQL_TRACE" in secrets_obj):  # type: ignore[operator]
            val = secrets_obj["TCO_SQL_TRACE"]  # type: ignore[index]
            return str(val).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    try:
        ctx = getattr(_st, "session_state", None)
        if ctx is not None:
            return bool(ctx.get("_tco_sql_trace"))
    except Exception:
        pass
    return False


def _record_sql_trace(duration_ms: float, rowcount: Optional[int], sql: str) -> None:
    if not _sql_trace_enabled():
        return
    snippet = sql.strip().replace("\n", " ")[:120] if sql else ""
    print(f"sql_trace duration_ms={duration_ms:.1f} rowcount={rowcount} sql={snippet}")
    try:
        ctx = _st.session_state  # type: ignore[attr-defined]
        ctx["total_queries"] = int(ctx.get("total_queries", 0)) + 1
        ctx["total_sql_time_ms"] = float(ctx.get("total_sql_time_ms", 0.0)) + float(duration_ms)
        log = list(ctx.get("_tco_sql_trace_log", []))
        log.append(
            {
                "duration_ms": round(float(duration_ms), 2),
                "rowcount": rowcount,
                "sql": snippet,
                "ts": time.time(),
            }
        )
        if len(log) > 50:
            log = log[-50:]
        ctx["_tco_sql_trace_log"] = log
    except Exception:
        pass


def get_sql_trace_debug() -> Optional[dict]:
    if not _sql_trace_enabled():
        return None
    try:
        ctx = _st.session_state  # type: ignore[attr-defined]
        return {
            "total_queries": int(ctx.get("total_queries", 0)),
            "total_sql_time_ms": float(ctx.get("total_sql_time_ms", 0.0)),
        }
    except Exception:
        return {"total_queries": None, "total_sql_time_ms": None}


@_versioned_write("TEAM_COMPOSITION.RECOMPUTE_YEAR")
def recompute_team_composition_from_headcount_year(
    *,
    team_id: str,
    year: int,
    changed_by: str = "",
    source: str = "HEADCOUNT_SAVE",
) -> bool:
    """Recompute TEAM_COMPOSITION_HISTORY for a team+year for PI=0..4.

    Notes
    -----
    The canonical inputs are `TEAM_HEADCOUNT_HISTORY` / `TEAM_CONTRACTOR_HEADCOUNT`, where PI=0
    represents the "All PIs" default and PI=1..4 are overrides.

    Many downstream views and pages (Budget/Capacity) expect explicit PI 1..4 rows to exist in
    `TEAM_COMPOSITION_HISTORY` (and derived views). To keep the app consistent, we always
    materialize PI 1..4 using the effective headcount fallback logic in
    `VW_TEAM_HEADCOUNT_EFFECTIVE` / `VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE`.
    """
    team_id = str(team_id or "").strip()
    if not team_id:
        return False
    year_i = int(year)
    ok = True
    for pi in [0, 1, 2, 3, 4]:
        ok = recompute_team_composition_from_headcount(
            team_id=team_id,
            year=year_i,
            pi=int(pi),
            changed_by=changed_by,
            source=source,
        ) and ok
    return ok


@_versioned_write("PROGRAM_COMPOSITION.RECOMPUTE")
def recompute_program_composition_from_headcount(
    *,
    program_id: str,
    year: int,
    pi: int,
    changed_by: str = "",
    source: str = "HEADCOUNT_SAVE",
) -> bool:
    """Recompute PROGRAM_COMPOSITION_HISTORY from program headcount (stored in TEAM_HEADCOUNT_HISTORY with CLASS='PROGRAM')."""
    ensure_location_and_contractor_tables()
    ensure_program_composition_history()
    ensure_composition_changelog_tables()

    program_id = str(program_id or "").strip()
    if not program_id:
        return False
    year_i = int(year)
    pi_i = int(pi)

    old_progfte = 0.0
    try:
        df_old = fetch_df(
            f"""
            SELECT TOP 1 PROGRAMFTE
            FROM {_fq('PROGRAM_COMPOSITION_HISTORY')}
            WHERE PROGRAMID = %s AND YEAR = %s AND PI = %s
            """,
            (program_id, year_i, pi_i),
        )
        if df_old is not None and not df_old.empty:
            old_progfte = float(df_old.iloc[0].get("PROGRAMFTE") or 0.0)
    except Exception:
        pass

    new_progfte = 0.0
    try:
        df_hc = fetch_df(
            f"""
            SELECT SUM(COALESCE(TRY_CONVERT(FLOAT, HEADCOUNT), 0)) AS HC
            FROM {_fq('VW_TEAM_HEADCOUNT_EFFECTIVE')}
            WHERE TEAMID = %s AND YEAR = %s AND PI = %s AND UPPER(LTRIM(RTRIM(CLASS))) = 'PROGRAM'
            """,
            (program_id, year_i, pi_i),
        )
        if df_hc is not None and not df_hc.empty:
            new_progfte = float(df_hc.iloc[0].get("HC") or 0.0)
    except Exception:
        pass

    if abs(float(old_progfte) - float(new_progfte)) <= 1e-9:
        return True

    try:
        execute(
            f"""
            INSERT INTO {_fq('TCO_PROGRAM_COMPOSITION_CHANGELOG')}
              (PROGRAMID, YEAR, PI, OLD_PROGRAMFTE, NEW_PROGRAMFTE, CHANGED_BY, SOURCE)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                program_id,
                year_i,
                pi_i,
                float(old_progfte),
                float(new_progfte),
                str(changed_by or "").strip(),
                str(source or "HEADCOUNT_SAVE").strip() or "HEADCOUNT_SAVE",
            ),
        )
    except Exception:
        pass

    try:
        execute(
            f"""
            MERGE INTO {_fq('PROGRAM_COMPOSITION_HISTORY')} t
            USING (SELECT %s AS PROGRAMID, %s AS YEAR, %s AS PI, %s AS UPDATED_BY) s
            ON t.PROGRAMID = s.PROGRAMID AND t.YEAR = s.YEAR AND t.PI = s.PI
            WHEN MATCHED THEN UPDATE SET PROGRAMFTE=%s, UPDATED_AT=SYSDATETIME(), UPDATED_BY=COALESCE(s.UPDATED_BY, t.UPDATED_BY)
            WHEN NOT MATCHED THEN INSERT (PROGRAMID, YEAR, PI, PROGRAMFTE, UPDATED_AT, UPDATED_BY)
            VALUES (s.PROGRAMID, s.YEAR, s.PI, %s, SYSDATETIME(), s.UPDATED_BY);
            """,
            (
                program_id,
                year_i,
                pi_i,
                str(changed_by or "").strip(),
                float(new_progfte),
                float(new_progfte),
            ),
        )
    except Exception:
        execute(
            f"""
            MERGE INTO {_fq('PROGRAM_COMPOSITION_HISTORY')} t
            USING (SELECT %s AS PROGRAMID, %s AS YEAR, %s AS PI) s
            ON t.PROGRAMID = s.PROGRAMID AND t.YEAR = s.YEAR AND t.PI = s.PI
            WHEN MATCHED THEN UPDATE SET PROGRAMFTE=%s, UPDATED_AT=SYSDATETIME()
            WHEN NOT MATCHED THEN INSERT (PROGRAMID, YEAR, PI, PROGRAMFTE, UPDATED_AT)
            VALUES (s.PROGRAMID, s.YEAR, s.PI, %s, SYSDATETIME());
            """,
            (
                program_id,
                year_i,
                pi_i,
                float(new_progfte),
                float(new_progfte),
            ),
        )
    return True


@_versioned_write("PROGRAM_COMPOSITION.RECOMPUTE_YEAR")
def recompute_program_composition_from_headcount_year(
    *,
    program_id: str,
    year: int,
    changed_by: str = "",
    source: str = "HEADCOUNT_SAVE",
) -> bool:
    """Recompute PROGRAM_COMPOSITION_HISTORY for PI=0..4 for a program+year.

    This mirrors the team composition behavior: PI=0 is the "All PIs" default and PI=1..4 are
    materialized so that downstream views can reliably show program overhead per PI even when
    users only save PI=0.
    """
    program_id = str(program_id or "").strip()
    if not program_id:
        return False
    year_i = int(year)
    ok = True
    for pi in [0, 1, 2, 3, 4]:
        ok = recompute_program_composition_from_headcount(
            program_id=program_id,
            year=year_i,
            pi=int(pi),
            changed_by=changed_by,
            source=source,
        ) and ok
    return ok
