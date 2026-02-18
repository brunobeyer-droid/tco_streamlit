from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import core.cost_model as cost_model


@dataclass
class _DummyStreamlit:
    session_state: dict[str, Any] = field(default_factory=dict)


def test_cost_model_scope_key_is_portfolio_isolated(monkeypatch):
    st = _DummyStreamlit(session_state={"active_portfolio_key": "PORT_A"})
    monkeypatch.setattr(cost_model, "st", st)
    monkeypatch.setattr(cost_model, "get_data_freshness_token", lambda: "fixed")

    scope = {"programs": ["R&M"], "teams": ["Core Team"], "groups": []}
    key_a = cost_model._scope_key(2026, scope)

    st.session_state["active_portfolio_key"] = "PORT_B"
    key_b = cost_model._scope_key(2026, scope)

    assert key_a != key_b
    assert key_a[0] == "PORT_A"
    assert key_b[0] == "PORT_B"
