"""Tests for SDK-backed sensor runtime compatibility helpers."""

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "sensors"))

import change_data_capture  # noqa: E402
from _sensor_runtime import emit_event  # noqa: E402


class _Resp:
    def raise_for_status(self):
        return None


class _HttpClient:
    def __init__(self):
        self.calls = []

    def post(self, path, *, json=None, timeout=None):
        self.calls.append({"path": path, "json": json, "timeout": timeout})
        return _Resp()


class _Sensor:
    def __init__(self):
        self.http_client = _HttpClient()


def test_emit_event_preserves_numeric_trigger_instance_id():
    sensor = _Sensor()

    assert emit_event(
        sensor,
        "salesforce.soql_record",
        {"record": {"Id": "001"}},
        trigger_instance_id="rule_123",
    )

    assert sensor.http_client.calls == [
        {
            "path": "/api/v1/events",
            "json": {
                "trigger_ref": "salesforce.soql_record",
                "payload": {"record": {"Id": "001"}},
                "trigger_instance_id": "rule_123",
            },
            "timeout": 30.0,
        }
    ]


def test_cdc_state_path_unchanged(monkeypatch, tmp_path):
    monkeypatch.setenv("ATTUNE_SENSOR_STATE_DIR", str(tmp_path))

    path = change_data_capture._state_path(7, "/data/AccountChangeEvent")

    assert path == str(tmp_path / "sf_cdc_rule_7_data_AccountChangeEvent.json")


def test_resolve_channels_from_channel():
    channels = change_data_capture._resolve_channels(
        {"channel": "/data/AccountChangeEvent"}
    )
    assert channels == ["/data/AccountChangeEvent"]


def test_resolve_channels_from_objects_standard_and_custom():
    channels = change_data_capture._resolve_channels(
        {"objects": ["Account", "Invoice__c"]}
    )
    assert channels == ["/data/AccountChangeEvent", "/data/Invoice__ChangeEvent"]


def test_resolve_channels_from_all_events():
    channels = change_data_capture._resolve_channels({"all_events": True})
    assert channels == ["/data/ChangeEvents"]


def test_resolve_channels_rejects_ambiguous_selector():
    try:
        change_data_capture._resolve_channels(
            {"channel": "/data/AccountChangeEvent", "all_events": True}
        )
    except ValueError as exc:
        assert "mutually exclusive" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError for ambiguous selectors")


def test_resolve_channels_rejects_missing_selector():
    try:
        change_data_capture._resolve_channels({})
    except ValueError as exc:
        assert "missing selector" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError when no selector is configured")


def test_resolve_channels_rejects_duplicates():
    try:
        change_data_capture._resolve_channels({"objects": ["Account", "Account"]})
    except ValueError as exc:
        assert "duplicate" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError for duplicate object/channel")


def test_resolve_channels_rejects_empty_objects():
    try:
        change_data_capture._resolve_channels({"objects": []})
    except ValueError as exc:
        assert "at least one object" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError for empty objects")


def test_replay_map_round_trip(tmp_path):
    path = str(tmp_path / "cdc_map.json")
    replay_map = {
        "/data/AccountChangeEvent": 101,
        "/data/ContactChangeEvent": 202,
    }
    change_data_capture._save_replay_map(path, replay_map)
    assert change_data_capture._load_replay_map(path) == replay_map


def test_cometd_version_label_prefers_api_version_label():
    client = type(
        "Client",
        (),
        {"api_version": type("ApiVersion", (), {"label": "67.0"})(), "data_url": None},
    )()
    assert change_data_capture._cometd_version_label(client, "v60.0") == "67.0"


def test_cometd_version_label_uses_data_url_when_api_version_missing():
    client = type("Client", (), {"api_version": None, "data_url": "/services/data/v61.0"})()
    assert change_data_capture._cometd_version_label(client, "v60.0") == "61.0"


def test_cometd_version_label_falls_back_to_config_version():
    client = type("Client", (), {"api_version": None, "data_url": None})()
    assert change_data_capture._cometd_version_label(client, "v60.0") == "60.0"
