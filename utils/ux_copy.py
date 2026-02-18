from __future__ import annotations

from typing import Any, Dict

PERMISSION_MESSAGES: Dict[str, str] = {
    "READ_ONLY_NO_SCOPE": "Read-only mode: no assigned {entity} scope found for this contributor.",
    "SCOPED_EDIT_MODE": "Scoped contributor mode: you can edit only assigned {entity} scope.",
    "ADMIN_ONLY_ACTION": "This action is available to Admin users.",
    "OUT_OF_SCOPE_RESOURCE": "You do not have permission to edit this {resource}.",
}

EMPTY_STATE_MESSAGES: Dict[str, str] = {
    "NO_DATA_SCOPE": "No data found for the current scope.",
    "NO_SELECTION": "Select a {resource} to continue.",
    "NO_RESULTS_FILTER": "No results for the selected filters.",
    "NO_MAPPINGS": "No mapped records were found for this scope.",
}

ERROR_STATE_MESSAGES: Dict[str, str] = {
    "LOAD_FAILED": "We could not load the requested data.",
    "SAVE_FAILED": "We could not save your changes.",
    "DELETE_FAILED": "We could not complete the delete action.",
    "VALIDATION_FAILED": "Some values need attention before continuing.",
}

SUCCESS_STATE_MESSAGES: Dict[str, str] = {
    "SAVED": "Changes saved successfully.",
    "UPDATED": "Update completed successfully.",
    "DELETED": "Delete completed successfully.",
    "IMPORTED": "Import completed successfully.",
}


def permission_message(key: str, **ctx: Any) -> str:
    template = PERMISSION_MESSAGES.get(str(key or "").strip().upper(), "")
    if not template:
        return ""
    try:
        return template.format(**ctx)
    except Exception:
        return template


def empty_state_message(key: str, **ctx: Any) -> str:
    template = EMPTY_STATE_MESSAGES.get(str(key or "").strip().upper(), "")
    if not template:
        return ""
    try:
        return template.format(**ctx)
    except Exception:
        return template


def error_state_message(key: str, **ctx: Any) -> str:
    template = ERROR_STATE_MESSAGES.get(str(key or "").strip().upper(), "")
    if not template:
        return ""
    try:
        return template.format(**ctx)
    except Exception:
        return template


def success_state_message(key: str, **ctx: Any) -> str:
    template = SUCCESS_STATE_MESSAGES.get(str(key or "").strip().upper(), "")
    if not template:
        return ""
    try:
        return template.format(**ctx)
    except Exception:
        return template
