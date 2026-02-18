import sys
import time
import types

import pytest

# Provide a minimal Streamlit shim when the real package is not installed.
if "streamlit" not in sys.modules:
    fake_st_module = types.ModuleType("streamlit")

    def _noop_deco(*args, **kwargs):
        def deco(fn):
            return fn

        return deco

    fake_st_module.session_state = {}
    fake_st_module.secrets = {}
    fake_st_module.cache_data = _noop_deco
    fake_st_module.cache_resource = _noop_deco
    fake_st_module.experimental_get_query_params = lambda: {}
    fake_st_module.experimental_set_query_params = lambda **kwargs: None
    fake_st_module.stop = lambda: None
    fake_st_module.warning = lambda *args, **kwargs: None
    fake_st_module.error = lambda *args, **kwargs: None
    fake_st_module.info = lambda *args, **kwargs: None
    fake_st_module.caption = lambda *args, **kwargs: None
    sys.modules["streamlit"] = fake_st_module

from utils import auth


@pytest.fixture
def fake_st(monkeypatch):
    """
    Minimal Streamlit stub to exercise auth helpers without a live Streamlit runtime.
    Records messages for assertions while keeping session_state mutable.
    """
    calls = {}

    class Sidebar:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def caption(self, msg):
            calls.setdefault("sidebar_captions", []).append(msg)

        def button(self, *args, **kwargs):
            return False

    st = types.SimpleNamespace(
        session_state={},
        secrets={},
        experimental_get_query_params=lambda: {},
        experimental_set_query_params=lambda **kwargs: calls.setdefault("set_qs", []).append(kwargs),
        stop=lambda: (_ for _ in ()).throw(RuntimeError("st.stop called")),
        warning=lambda msg: calls.setdefault("warnings", []).append(msg),
        error=lambda msg: calls.setdefault("errors", []).append(msg),
        info=lambda msg: calls.setdefault("infos", []).append(msg),
        caption=lambda msg: calls.setdefault("captions", []).append(msg),
        divider=lambda: None,
        button=lambda *args, **kwargs: False,
        selectbox=lambda label, options, index=0, key=None, **kwargs: options[index] if options else None,
        link_button=lambda *args, **kwargs: None,
        text_input=lambda *args, **kwargs: "",
        markdown=lambda *args, **kwargs: None,
        rerun=lambda: None,
        sidebar=Sidebar(),
    )

    # Swap the auth module's Streamlit reference for our stub
    monkeypatch.setattr(auth, "st", st)
    return st, calls


def test_session_timeout_resets_idle_user(fake_st):
    st, _ = fake_st
    # Set a tiny timeout so we can simulate expiry
    st.secrets = {"auth": {"session_idle_minutes": 0.02}}  # ~1.2 seconds
    st.session_state["auth_user"] = {"email": "user@example.com"}
    st.session_state["_auth_last_active"] = time.time() - 5

    expired = auth._maybe_expire_session()

    assert expired is True
    assert "auth_user" not in st.session_state
    assert st.session_state.get("_auth_expired_notice") is True


def test_require_role_uses_db_record_for_role(fake_st, monkeypatch):
    st, calls = fake_st
    st.secrets = {
        "azuread": {"enabled": False},
        "auth": {"require_sso": False},
    }
    st.session_state["auth_user"] = {"email": "user@example.com", "role": "VIEWER"}
    st.session_state["_auth_last_active"] = time.time()
    # Pretend DB says this user is a contributor
    monkeypatch.setattr(
        auth, "get_user_by_email", lambda email: {"ROLE": "CONTRIBUTOR", "IS_ACTIVE": True}
    )
    # Skip unrelated side effects
    monkeypatch.setattr(auth, "_try_front_channel_logout", lambda: None)
    monkeypatch.setattr(auth, "_try_complete_authcode", lambda: None)

    user = auth.require_role("VIEWER", page_name="Dashboard")

    assert user["role"] == "CONTRIBUTOR"
    assert st.session_state["auth_user"]["role"] == "CONTRIBUTOR"
    assert calls.get("errors") is None  # no errors shown


def test_save_user_preserves_contributor_role(fake_st):
    st, _ = fake_st
    auth._save_user({"email": "contrib@example.com", "role": "CONTRIBUTOR"})
    assert st.session_state.get("role") == "CONTRIBUTOR"
    assert (st.session_state.get("auth_user") or {}).get("role") == "CONTRIBUTOR"


def test_require_role_blocks_inactive_user(fake_st, monkeypatch):
    st, calls = fake_st
    st.session_state["auth_user"] = {"email": "user@example.com", "role": "VIEWER"}
    st.session_state["_auth_last_active"] = time.time()
    monkeypatch.setattr(auth, "get_user_by_email", lambda email: {"ROLE": "VIEWER", "IS_ACTIVE": False})
    monkeypatch.setattr(auth, "_try_front_channel_logout", lambda: None)
    monkeypatch.setattr(auth, "_try_complete_authcode", lambda: None)

    with pytest.raises(RuntimeError, match="st.stop called"):
        auth.require_role("VIEWER", page_name="Dashboard")

    assert calls.get("errors"), "Expected an error message when blocking inactive user"


def test_flowcache_evicts_stale_entries(monkeypatch):
    # Start with a clean cache
    auth._FLOW_CACHE.clear()
    auth._FLOW_CACHE_TS.clear()
    flow = {"state": "abc", "auth_uri": "https://login.example.com"}
    auth._flowcache_put(flow)
    assert auth._flowcache_get("abc") == flow

    # Force the entry to look stale and trigger GC
    auth._FLOW_CACHE_TS["abc"] = time.time() - (auth._FLOW_TTL_SECONDS + 10)
    auth._flowcache_gc()

    assert auth._flowcache_get("abc") is None


def test_auto_login_admin_sets_session(fake_st):
    st, _ = fake_st
    st.secrets = {
        "azuread": {"enabled": False},
        "auth": {
            "enable_local_admin_ui": True,
            "auto_login": True,
            "require_sso": False,
            "admin_email": "admin@example.com",
            "admin_password": "secret",
        }
    }

    auth._try_auto_login_admin()

    assert st.session_state.get("auth_user", {}).get("email") == "admin@example.com"
    assert st.session_state.get("user_email") == "admin@example.com"
    assert st.session_state.get("role") == "ADMIN"


def test_login_ui_auto_login_admin(fake_st, monkeypatch):
    st, _ = fake_st
    st.secrets = {
        "auth": {
            "enable_local_admin_ui": True,
            "auto_login": True,
            "require_sso": False,
            "admin_email": "admin@example.com",
            "admin_password": "secret",
        },
        "azuread": {"enabled": False},
    }
    monkeypatch.setattr(auth, "ensure_access_control_tables", lambda: None)
    monkeypatch.setattr(auth, "_try_front_channel_logout", lambda: None)
    monkeypatch.setattr(auth, "_try_complete_authcode", lambda: None)

    auth.login_ui(page_path="pages/0_Welcome.py")

    assert st.session_state.get("auth_user", {}).get("email") == "admin@example.com"
