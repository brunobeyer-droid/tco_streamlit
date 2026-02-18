from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import core.freshness as freshness
import db.mssql_backend as mssql


class _DummyExpander:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@dataclass
class _DummyStreamlit:
    session_state: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    rerun_called: bool = False
    last_json: Any = None

    def warning(self, msg: str) -> None:
        self.warnings.append(str(msg))

    def rerun(self) -> None:
        self.rerun_called = True

    def expander(self, *_args: Any, **_kwargs: Any) -> _DummyExpander:
        return _DummyExpander()

    def json(self, payload: Any) -> None:
        self.last_json = payload


def test_freshness_token_cache_and_portfolio_switch(monkeypatch):
    st = _DummyStreamlit(session_state={"active_portfolio_key": "MOIT"})
    monkeypatch.setattr(freshness, "st", st)

    calls = {"n": 0}

    def _fake_info():
        calls["n"] += 1
        return {"version": 7, "updated_at": None}

    monkeypatch.setattr(freshness, "get_data_version_info", _fake_info)

    t1 = freshness.get_data_freshness_token(ttl_sec=60)
    t2 = freshness.get_data_freshness_token(ttl_sec=60)

    assert t1 == "MOIT:7"
    assert t2 == "MOIT:7"
    assert calls["n"] == 1

    st.session_state["active_portfolio_key"] = "sled_it"
    t3 = freshness.get_data_freshness_token(ttl_sec=60)

    assert t3 == "sled_it:7"
    assert calls["n"] == 2
    assert "_freshness_token:MOIT" not in st.session_state


def test_post_write_refresh_respects_v2_default_and_explicit_bump(monkeypatch):
    st = _DummyStreamlit(
        session_state={
            "active_portfolio_key": "MOIT",
            "_freshness_token:MOIT": {"value": {"token": "MOIT:1", "version": "1", "fetched_at": 1.0}, "exp_at": 9999999999.0},
        }
    )
    monkeypatch.setattr(freshness, "st", st)
    monkeypatch.setattr(freshness, "_freshness_v2_enabled", lambda: True)

    bumped: list[str] = []
    monkeypatch.setattr(freshness, "bump_data_version", lambda ctx: bumped.append(str(ctx)))

    freshness.post_write_refresh("ctx_default", rerun=False)
    assert bumped == []
    assert st.session_state.get("_cache_epoch") == 1
    assert "_freshness_token:MOIT" not in st.session_state

    freshness.post_write_refresh("ctx_force", rerun=False, bump_version=True)
    assert bumped == ["ctx_force"]
    assert st.session_state.get("_cache_epoch") == 2


def test_freshness_diagnostics_shape(monkeypatch):
    st = _DummyStreamlit(session_state={"active_portfolio_key": "MOIT"})
    monkeypatch.setattr(freshness, "st", st)
    monkeypatch.setattr(freshness, "get_data_version_info", lambda: {"version": 11, "updated_at": None})

    diag = freshness.get_freshness_diagnostics(ttl_sec=60)

    assert diag["portfolio_key"] == "MOIT"
    assert diag["data_version"] == "11"
    assert diag["freshness_token"] == "MOIT:11"
    assert "token_age_seconds" in diag


def test_freshness_default_ttl_env(monkeypatch):
    st = _DummyStreamlit(session_state={"active_portfolio_key": "MOIT"})
    monkeypatch.setattr(freshness, "st", st)
    monkeypatch.setenv("TCO_FRESHNESS_TOKEN_TTL_SEC", "3")
    monkeypatch.setattr(freshness, "get_data_version_info", lambda: {"version": 3, "updated_at": None})

    diag = freshness.get_freshness_diagnostics()
    assert int(diag.get("token_ttl_seconds") or 0) == 3


def test_cross_session_token_refresh_after_version_change(monkeypatch):
    shared = {"version": 1}
    monkeypatch.setattr(
        freshness,
        "get_data_version_info",
        lambda: {"version": int(shared["version"]), "updated_at": None},
    )

    st_a = _DummyStreamlit(session_state={"active_portfolio_key": "MOIT"})
    st_b = _DummyStreamlit(session_state={"active_portfolio_key": "MOIT"})

    monkeypatch.setattr(freshness, "st", st_a)
    tok_a_1 = freshness.get_data_freshness_token(ttl_sec=3)
    assert tok_a_1 == "MOIT:1"

    monkeypatch.setattr(freshness, "st", st_b)
    tok_b_1 = freshness.get_data_freshness_token(ttl_sec=3)
    assert tok_b_1 == "MOIT:1"

    # Simulate a write by session A that advances data version globally.
    shared["version"] = 2
    monkeypatch.setattr(freshness, "st", st_a)
    freshness.post_write_refresh("writer_save", rerun=False, bump_version=False)
    assert int(st_a.session_state.get("_cache_epoch") or 0) == 1

    # Next interaction in session B after token expiry should pick the new version.
    token_key_b = "_freshness_token:MOIT"
    if token_key_b in st_b.session_state:
        st_b.session_state[token_key_b]["exp_at"] = 0.0
    monkeypatch.setattr(freshness, "st", st_b)
    tok_b_2 = freshness.get_data_freshness_token(ttl_sec=1)
    assert tok_b_2 == "MOIT:2"


class _DummyCursor:
    def __init__(self, rowcount: int = 1):
        self.rowcount = rowcount

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql, _params=None):
        return None

    def executemany(self, _sql, _params=None):
        return None


class _DummyConn:
    def cursor(self):
        return _DummyCursor()

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        return None


def _patch_mssql_execute_runtime(monkeypatch, *, control_cfg: bool) -> list[str]:
    bumped: list[str] = []
    fake_cfg = mssql.MssqlConfig(
        server="s",
        database="d",
        user="u",
        password="p",
        driver="drv",
        encrypt="yes",
        trust_server_certificate="no",
        schema="dbo",
    )
    monkeypatch.setattr(mssql, "_resolve_cfg", lambda cfg=None: cfg or fake_cfg)
    monkeypatch.setattr(mssql, "_norm_params_and_sql", lambda sql, params=None, cfg=None: (str(sql), params))
    monkeypatch.setattr(mssql, "_get_connection_for_cfg", lambda _cfg: _DummyConn())
    monkeypatch.setattr(mssql, "_get_connection", lambda: _DummyConn())
    monkeypatch.setattr(mssql, "_record_sql_trace", lambda *args, **kwargs: None)
    monkeypatch.setattr(mssql, "_db_runtime_begin", lambda *args, **kwargs: None)
    monkeypatch.setattr(mssql, "_db_runtime_success", lambda *args, **kwargs: None)
    monkeypatch.setattr(mssql, "_db_runtime_retry", lambda *args, **kwargs: None)
    monkeypatch.setattr(mssql, "_db_runtime_error", lambda *args, **kwargs: None)
    monkeypatch.setattr(mssql, "_is_connection_error", lambda _msg: False)
    monkeypatch.setattr(mssql, "_reset_connection_cache", lambda: None)
    monkeypatch.setattr(mssql, "_freshness_v2_enabled", lambda: True)
    monkeypatch.setattr(mssql, "_active_portfolio_selected", lambda: True)
    monkeypatch.setattr(mssql, "_is_control_db_cfg", lambda _cfg: bool(control_cfg))
    monkeypatch.setattr(mssql, "bump_data_version", lambda reason="": bumped.append(str(reason)))
    mssql._tracker_set("scope_depth", 0)
    mssql._tracker_set("mutation_count", 0)
    return bumped


def test_execute_direct_dml_bumps_version_for_portfolio(monkeypatch):
    bumped = _patch_mssql_execute_runtime(monkeypatch, control_cfg=False)
    cfg = mssql.MssqlConfig("s", "d", "u", "p", "drv", "yes", "no", "dbo")
    mssql.execute("UPDATE APP_USERS SET ROLE='VIEWER' WHERE 1=0", cfg=cfg)
    assert bumped == ["DB_EXECUTE_DML"]


def test_execute_direct_dml_does_not_bump_for_control_db(monkeypatch):
    bumped = _patch_mssql_execute_runtime(monkeypatch, control_cfg=True)
    cfg = mssql.MssqlConfig("s", "d", "u", "p", "drv", "yes", "no", "dbo")
    mssql.execute("UPDATE CONTROL_AUTOMATION_RUNS SET STATUS='X' WHERE 1=0", cfg=cfg)
    assert bumped == []
