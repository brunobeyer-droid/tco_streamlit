from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional, Tuple


def unpack_event(event: Any) -> Tuple[str, Dict[str, Any]]:
    if not isinstance(event, dict):
        return "", {}
    et = str(event.get("type") or "").strip()
    payload = event.get("payload")
    if isinstance(payload, dict):
        return et, payload
    return et, event


def selection_from_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    mode = str(payload.get("mode") or "").strip().lower()
    if mode == "feature":
        return {
            "mode": "feature",
            "feature_id": payload.get("feature_id"),
            "feature_title": payload.get("feature_title"),
            "epic_id": payload.get("epic_id"),
            "epic_title": payload.get("epic_title"),
            "groupname": payload.get("app_id"),
            "pi_label": payload.get("pi_id"),
        }
    if mode == "epic":
        return {
            "mode": "epic",
            "epic_id": payload.get("epic_id"),
            "epic_title": payload.get("epic_title"),
            "groupname": payload.get("app_id"),
            "pi_label": payload.get("pi_id"),
        }
    if mode == "cell":
        return {
            "mode": "cell",
            "epic_id": payload.get("epic_id"),
            "epic_title": payload.get("epic_title"),
            "groupname": payload.get("app_id"),
            "pi_label": payload.get("pi_id"),
        }
    if mode == "milestone":
        return {
            "mode": "milestone",
            "milestone_id": payload.get("milestone_id"),
            "groupname": payload.get("app_id"),
            "pi_label": payload.get("pi_id"),
        }
    if mode == "dependency":
        return {
            "mode": "dependency",
            "source_id": payload.get("source_id"),
            "target_id": payload.get("target_id"),
        }
    return None


def window_start_from_payload(payload: Dict[str, Any]) -> Optional[int]:
    raw = payload.get("window_start")
    if isinstance(raw, int) and raw >= 0:
        return raw
    return None


def group_by_from_payload(payload: Dict[str, Any]) -> Optional[str]:
    raw = str(payload.get("group_by") or "").strip().lower()
    if raw in {"applications", "apps", "application"}:
        return "Applications"
    if raw in {"epics", "epic"}:
        return "Epics"
    return None


def milestone_create_from_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = str(payload.get("title") or "").strip()
    date_raw = str(payload.get("date") or "").strip()
    tag = str(payload.get("tag") or "Milestone").strip() or "Milestone"
    if not title or not date_raw:
        return None
    try:
        parsed = dt.date.fromisoformat(date_raw)
    except Exception:
        return None
    return {"title": title, "target_date": parsed, "tag": tag}


def milestone_delete_id_from_payload(payload: Dict[str, Any]) -> str:
    return str(payload.get("milestone_id") or "").strip()


def milestone_batch_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    creates_raw = payload.get("creates")
    deletes_raw = payload.get("deletes")
    creates: list[Dict[str, Any]] = []
    deletes: list[str] = []
    if isinstance(creates_raw, list):
        for item in creates_raw:
            if not isinstance(item, dict):
                continue
            parsed = milestone_create_from_payload(item)
            if not parsed:
                continue
            creates.append(
                {
                    "milestone": parsed,
                    "milestone_id": str(item.get("id") or item.get("milestone_id") or "").strip() or None,
                    "pi_key": str(item.get("pi_key") or "").strip() or None,
                    "epic_id": str(item.get("epic_id") or "").strip() or None,
                    "feature_id": str(item.get("feature_id") or "").strip() or None,
                    "source_type": str(item.get("source_type") or "MANUAL").strip().upper() or "MANUAL",
                }
            )
    if isinstance(deletes_raw, list):
        for item in deletes_raw:
            mid = str(item or "").strip()
            if mid:
                deletes.append(mid)
    return {"creates": creates, "deletes": deletes}


def route_event(event: Any) -> Dict[str, Any]:
    et, payload = unpack_event(event)
    if not et:
        return {"event_type": "", "action": "noop", "payload": {}}

    if et == "view_change":
        state_patch = payload.get("state_patch") if isinstance(payload.get("state_patch"), dict) else {}
        return {
            "event_type": et,
            "action": "view_change",
            "payload": {
                "state_patch": state_patch,
                "selection_patch": payload.get("selection_patch") if isinstance(payload.get("selection_patch"), dict) else {},
                "group_by": group_by_from_payload(payload),
                "show_dependencies": payload.get("show_dependencies"),
                "view": payload.get("view"),
                "density": payload.get("density"),
                "timeframePreset": payload.get("timeframePreset"),
                "zoom": payload.get("zoom"),
                "showRoi": payload.get("showRoi"),
                "sortBy": payload.get("sortBy"),
                "sortDir": payload.get("sortDir"),
            },
        }

    if et == "drawer_open":
        return {"event_type": et, "action": "drawer_open", "payload": payload}

    if et == "window_change":
        return {
            "event_type": et,
            "action": "window_change",
            "payload": {
                "window_start": window_start_from_payload(payload),
                "startPiId": str(payload.get("startPiId") or "").strip(),
            },
        }

    if et == "selection_change":
        return {
            "event_type": et,
            "action": "selection_change",
            "payload": {
                "selection": selection_from_payload(payload),
            },
        }

    if et == "milestone_create":
        return {
            "event_type": et,
            "action": "milestone_create",
            "payload": {
                "milestone": milestone_create_from_payload(payload),
                "raw": payload,
            },
        }

    if et == "milestone_delete":
        return {
            "event_type": et,
            "action": "milestone_delete",
            "payload": {
                "milestone_id": milestone_delete_id_from_payload(payload),
            },
        }

    if et == "milestone_batch_apply":
        result_raw = payload.get("result")
        return {
            "event_type": et,
            "action": "milestone_batch_apply",
            "payload": {
                **milestone_batch_from_payload(payload),
                "result": result_raw if isinstance(result_raw, dict) else {},
            },
        }

    return {"event_type": et, "action": "noop", "payload": payload}
