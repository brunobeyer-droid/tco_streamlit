from utils.ux_copy import EMPTY_STATE_MESSAGES, PERMISSION_MESSAGES, empty_state_message, permission_message


def test_permission_message_keys_exist():
    required = {"READ_ONLY_NO_SCOPE", "SCOPED_EDIT_MODE", "ADMIN_ONLY_ACTION", "OUT_OF_SCOPE_RESOURCE"}
    assert required.issubset(set(PERMISSION_MESSAGES.keys()))


def test_empty_state_keys_exist():
    required = {"NO_DATA_SCOPE", "NO_SELECTION", "NO_RESULTS_FILTER", "NO_MAPPINGS"}
    assert required.issubset(set(EMPTY_STATE_MESSAGES.keys()))


def test_permission_messages_use_canonical_terms():
    joined = " ".join(PERMISSION_MESSAGES.values()).lower()
    assert "scope" in joined
    assert "program" in (permission_message("SCOPED_EDIT_MODE", entity="Program/Team/Application").lower())


def test_empty_state_message_formatting():
    msg = empty_state_message("NO_SELECTION", resource="Program")
    assert "Program" in msg
