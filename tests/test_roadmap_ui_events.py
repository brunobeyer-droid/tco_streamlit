import datetime as dt
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from core.roadmap_ui_events import (
    unpack_event,
    selection_from_payload,
    window_start_from_payload,
    group_by_from_payload,
    milestone_create_from_payload,
    milestone_delete_id_from_payload,
    route_event,
)


def test_unpack_event_supports_new_envelope():
    et, payload = unpack_event({"type": "selection_change", "requestId": "1", "payload": {"mode": "feature"}})
    assert et == "selection_change"
    assert payload == {"mode": "feature"}


def test_selection_from_payload_feature():
    sel = selection_from_payload(
        {
            "mode": "feature",
            "feature_id": "100",
            "feature_title": "F",
            "epic_id": "200",
            "epic_title": "E",
            "app_id": "App A",
            "pi_id": "2026 I1",
        }
    )
    assert sel is not None
    assert sel["mode"] == "feature"
    assert sel["feature_id"] == "100"
    assert sel["groupname"] == "App A"

    ms_sel = selection_from_payload({"mode": "milestone", "milestone_id": "ms-1"})
    assert ms_sel is not None
    assert ms_sel["mode"] == "milestone"
    assert ms_sel["milestone_id"] == "ms-1"

    dep_sel = selection_from_payload({"mode": "dependency", "source_id": "101", "target_id": "202"})
    assert dep_sel is not None
    assert dep_sel["mode"] == "dependency"
    assert dep_sel["source_id"] == "101"
    assert dep_sel["target_id"] == "202"


def test_window_start_and_group_by_parsing():
    assert window_start_from_payload({"window_start": 2}) == 2
    assert group_by_from_payload({"group_by": "epics"}) == "Epics"
    assert group_by_from_payload({"group_by": "applications"}) == "Applications"


def test_milestone_payload_parsing():
    parsed = milestone_create_from_payload({"title": "Release", "date": "2026-03-15", "tag": "Release"})
    assert parsed is not None
    assert parsed["title"] == "Release"
    assert parsed["target_date"] == dt.date(2026, 3, 15)
    assert milestone_delete_id_from_payload({"milestone_id": "abc"}) == "abc"


def test_route_event_view_change_and_unknown_noop():
    routed = route_event(
        {
            "type": "view_change",
            "payload": {
                "group_by": "epics",
                "view": "timeline",
                "zoom": 1.2,
                "state_patch": {"view": "timeline", "groupBy": "epics"},
                "selection_patch": {"kind": "feature", "featureId": "123"},
                "showRoi": True,
                "sortBy": "roi_bv",
                "sortDir": "desc",
                "future_field": "ignore_me",
            },
        }
    )
    assert routed["action"] == "view_change"
    assert routed["payload"]["group_by"] == "Epics"
    assert routed["payload"]["view"] == "timeline"
    assert routed["payload"]["zoom"] == 1.2
    assert routed["payload"]["state_patch"]["groupBy"] == "epics"
    assert routed["payload"]["selection_patch"]["kind"] == "feature"
    assert routed["payload"]["showRoi"] is True
    assert routed["payload"]["sortBy"] == "roi_bv"
    assert routed["payload"]["sortDir"] == "desc"

    unknown = route_event({"type": "something_new", "payload": {"x": 1}})
    assert unknown["action"] == "noop"


def test_route_event_milestone_batch_apply_supports_optional_result_envelope():
    routed = route_event(
        {
            "type": "milestone_batch_apply",
            "payload": {
                "creates": [{"title": "R1", "date": "2026-05-01", "tag": "Release"}],
                "deletes": ["123"],
                "result": {"created": 1, "deleted": 1, "failed": 0},
            },
        }
    )
    assert routed["action"] == "milestone_batch_apply"
    assert len(routed["payload"]["creates"]) == 1
    assert routed["payload"]["deletes"] == ["123"]
    assert routed["payload"]["result"]["created"] == 1
