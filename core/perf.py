from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

import streamlit as st

logger = logging.getLogger(__name__)


@dataclass
class PerfStep:
    name: str
    elapsed_ms: float
    ts_utc: str
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class PerfCollector:
    page_key: str
    run_nonce: int
    started_at: float
    first_kpi_ms: Optional[float] = None
    steps: list[PerfStep] = field(default_factory=list)

    def add_step(self, *, name: str, elapsed_ms: float, meta: Optional[dict[str, Any]] = None) -> None:
        self.steps.append(
            PerfStep(
                name=str(name),
                elapsed_ms=round(float(elapsed_ms), 3),
                ts_utc=dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
                meta=dict(meta or {}),
            )
        )


def _collector_state_key(page_key: str) -> str:
    run_nonce = int(st.session_state.get("_tco_run_nonce", 0) or 0)
    return f"_perf_collector::{str(page_key or 'page').strip()}::{run_nonce}"


def get_perf_collector(page_key: str) -> PerfCollector:
    key = _collector_state_key(page_key)
    existing = st.session_state.get(key)
    if isinstance(existing, PerfCollector):
        return existing
    collector = PerfCollector(
        page_key=str(page_key or "page").strip() or "page",
        run_nonce=int(st.session_state.get("_tco_run_nonce", 0) or 0),
        started_at=time.perf_counter(),
    )
    st.session_state[key] = collector
    return collector


def mark_first_kpi_render(page_key: str) -> None:
    collector = get_perf_collector(page_key)
    if collector.first_kpi_ms is not None:
        return
    collector.first_kpi_ms = round((time.perf_counter() - collector.started_at) * 1000.0, 3)


@contextmanager
def perf_step(name: str, *, page_key: str, meta: Optional[dict[str, Any]] = None) -> Iterator[None]:
    collector = get_perf_collector(page_key)
    t0 = time.perf_counter()
    sql_q0: Optional[int] = None
    sql_ms0: Optional[float] = None
    db_fetch0: Optional[int] = None
    db_latency0: Optional[float] = None
    try:
        sql_q0 = int(st.session_state.get("total_queries", 0) or 0)
    except Exception:
        sql_q0 = None
    try:
        sql_ms0 = float(st.session_state.get("total_sql_time_ms", 0.0) or 0.0)
    except Exception:
        sql_ms0 = None
    try:
        from db import get_db_runtime_health

        health0 = get_db_runtime_health() or {}
        db_fetch0 = int(health0.get("fetch_calls", 0) or 0)
        db_latency0 = float(health0.get("total_latency_ms", 0.0) or 0.0)
    except Exception:
        db_fetch0 = None
        db_latency0 = None
    exc_name: Optional[str] = None
    try:
        yield
    except Exception as exc:  # pragma: no cover
        exc_name = type(exc).__name__
        raise
    finally:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        full_meta = dict(meta or {})
        if sql_q0 is not None:
            try:
                q1 = int(st.session_state.get("total_queries", 0) or 0)
                full_meta["sql_queries_delta"] = max(0, q1 - sql_q0)
            except Exception:
                pass
        if sql_ms0 is not None:
            try:
                ms1 = float(st.session_state.get("total_sql_time_ms", 0.0) or 0.0)
                full_meta["sql_time_ms_delta"] = round(max(0.0, ms1 - sql_ms0), 3)
            except Exception:
                pass
        if db_fetch0 is not None or db_latency0 is not None:
            try:
                from db import get_db_runtime_health

                health1 = get_db_runtime_health() or {}
                if db_fetch0 is not None:
                    fetch1 = int(health1.get("fetch_calls", 0) or 0)
                    full_meta["db_fetch_calls_delta"] = max(0, fetch1 - db_fetch0)
                if db_latency0 is not None:
                    lat1 = float(health1.get("total_latency_ms", 0.0) or 0.0)
                    full_meta["db_latency_ms_delta"] = round(max(0.0, lat1 - db_latency0), 3)
            except Exception:
                pass
        try:
            sql_ms = float(full_meta.get("sql_time_ms_delta", 0.0) or 0.0)
            full_meta["non_sql_ms_est"] = round(max(0.0, float(elapsed_ms) - sql_ms), 3)
        except Exception:
            pass
        if exc_name:
            full_meta["error"] = exc_name
        collector.add_step(name=str(name), elapsed_ms=elapsed_ms, meta=full_meta)
        logger.info(
            "PERF page=%s run=%s step=%s elapsed_ms=%.3f meta=%s",
            collector.page_key,
            collector.run_nonce,
            str(name),
            float(elapsed_ms),
            json.dumps(full_meta, sort_keys=True, default=str),
        )


def _perf_debug_enabled() -> bool:
    env_on = str(os.getenv("TCO_DEBUG_PERF", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
    if env_on:
        return True
    try:
        from core.debug import is_debug_enabled

        admin_debug_on = bool(is_debug_enabled(label="Debug", key="debug_mode"))
        perf_flag = st.session_state.get("debug_perf", None)
        if perf_flag is None:
            return admin_debug_on
        return admin_debug_on and bool(perf_flag)
    except Exception:
        return False


def _steps_to_rows(collector: PerfCollector) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, step in enumerate(collector.steps, start=1):
        row: dict[str, Any] = {
            "#": idx,
            "step": step.name,
            "elapsed_ms": round(float(step.elapsed_ms), 3),
            "ts_utc": step.ts_utc,
        }
        if "sql_queries_delta" in step.meta:
            row["sql_queries_delta"] = step.meta.get("sql_queries_delta")
        if "sql_time_ms_delta" in step.meta:
            row["sql_time_ms_delta"] = step.meta.get("sql_time_ms_delta")
        if "non_sql_ms_est" in step.meta:
            row["non_sql_ms_est"] = step.meta.get("non_sql_ms_est")
        if "db_fetch_calls_delta" in step.meta:
            row["db_fetch_calls_delta"] = step.meta.get("db_fetch_calls_delta")
        if "db_latency_ms_delta" in step.meta:
            row["db_latency_ms_delta"] = step.meta.get("db_latency_ms_delta")
        if step.meta:
            row["meta"] = json.dumps(step.meta, sort_keys=True, default=str)
        rows.append(row)
    return rows


def show_perf_panel(page_key: str) -> None:
    if not _perf_debug_enabled():
        return

    collector = get_perf_collector(page_key)
    total_page_ms = round((time.perf_counter() - collector.started_at) * 1000.0, 3)
    rows = _steps_to_rows(collector)

    with st.expander("Perf", expanded=False):
        st.caption(
            f"time_to_first_kpi_ms: {collector.first_kpi_ms if collector.first_kpi_ms is not None else '—'} | "
            f"total_page_ms: {total_page_ms}"
        )
        if rows:
            st.dataframe(rows, use_container_width=True, hide_index=True, height=min(360, 44 + len(rows) * 28))
            slowest = sorted(rows, key=lambda r: float(r.get("elapsed_ms") or 0.0), reverse=True)[:5]
            st.caption("Slowest 5 steps")
            st.dataframe(slowest, use_container_width=True, hide_index=True)
        else:
            st.info("No perf steps recorded in this run.")


def filters_signature(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload or {}, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _clean_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        out = [str(v).strip() for v in value if str(v).strip()]
        return sorted(set(out))
    if value is None:
        return []
    s = str(value).strip()
    return [s] if s else []


def user_scope_signature(scope: Any, role: Any) -> str:
    payload: dict[str, Any] = {
        "role": str(role or "").strip().upper(),
        "programs": [],
        "teams": [],
        "groups": [],
        "program_ids": [],
    }

    try:
        payload["programs"] = _clean_list(getattr(scope, "programs", []))
        payload["teams"] = _clean_list(getattr(scope, "teams", []))
        payload["groups"] = _clean_list(getattr(scope, "groups", []))
        payload["program_ids"] = _clean_list(getattr(scope, "program_ids", []))
    except Exception:
        pass

    if isinstance(scope, dict):
        payload["programs"] = _clean_list(scope.get("programs") or payload["programs"])
        payload["teams"] = _clean_list(scope.get("teams") or payload["teams"])
        payload["groups"] = _clean_list(scope.get("groups") or payload["groups"])
        payload["program_ids"] = _clean_list(scope.get("program_ids") or payload["program_ids"])

    raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def get_portfolio_cache_buster() -> str:
    try:
        from db import get_data_version_info

        info = get_data_version_info() or {}
        ver = str(int(info.get("version") or 0))
        updated_at = info.get("updated_at")
        updated = ""
        if updated_at is not None:
            updated = str(updated_at)
        return f"{ver}:{updated}"
    except Exception:
        return "0"
