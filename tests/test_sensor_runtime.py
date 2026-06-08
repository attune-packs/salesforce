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
