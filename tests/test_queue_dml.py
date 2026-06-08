"""Tests for queue-driven Salesforce DML actions."""

import json
import os
import sys
from typing import Any, Dict, List

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "actions"))

from lib import queue_dml  # noqa: E402
import process_queue_dml_batch  # noqa: E402
import process_queue_dml_item  # noqa: E402


class _DispatchRecorder:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, action_params, method, path, json_body=None, **kwargs):
        self.calls.append(
            {
                "method": method,
                "path": path,
                "body": json_body,
                "params": kwargs.get("params"),
            }
        )
        if method == "POST" and path.startswith("sobjects/"):
            return {"success": True, "id": "001NEW", "errors": []}
        if method in ("PATCH", "DELETE") and path.startswith("sobjects/"):
            return None
        if method == "DELETE":
            ids = (kwargs.get("params") or {}).get("ids", "").split(",")
            return [{"success": True, "id": item_id, "errors": []} for item_id in ids]
        records = (json_body or {}).get("records", [])
        return [
            {"success": True, "id": rec.get("Id") or rec.get("External_Id__c") or f"id-{i}", "errors": []}
            for i, rec in enumerate(records)
        ]


def _run_item(monkeypatch, capsys, params: Dict[str, Any], recorder: _DispatchRecorder):
    monkeypatch.setattr(process_queue_dml_item.sf_client, "read_params", lambda: params)
    monkeypatch.setattr(process_queue_dml_item.sf_client, "sf_request", recorder)
    process_queue_dml_item.main()
    return json.loads(capsys.readouterr().out.strip())


def _run_batch(monkeypatch, capsys, params: Dict[str, Any], recorder: _DispatchRecorder):
    monkeypatch.setattr(process_queue_dml_batch.sf_client, "read_params", lambda: params)
    monkeypatch.setattr(process_queue_dml_batch.sf_client, "sf_request", recorder)
    process_queue_dml_batch.main()
    return json.loads(capsys.readouterr().out.strip())


def test_normalize_item_accepts_payload_wrapper_and_record_type():
    item = queue_dml.normalize_item(
        {
            "operation": "insert",
            "record": {"attributes": {"type": "Account"}, "Name": "Acme"},
        }
    )
    assert item.operation == "create"
    assert item.sobject == "Account"
    assert item.fields == {"Name": "Acme"}


def test_normalize_upsert_uses_record_external_id_value():
    item = queue_dml.normalize_item(
        {
            "operation": "upsert",
            "sobject": "Account",
            "external_id_field": "External_Id__c",
            "record": {"External_Id__c": "acme-1", "Name": "Acme"},
        }
    )
    assert item.external_id_value == "acme-1"


def test_normalize_item_accepts_queue_envelope_payload():
    item = queue_dml.normalize_item(
        {
            "id": 99,
            "payload": {
                "operation": "create",
                "sobject": "Account",
                "record": {"Name": "Envelope"},
            },
        }
    )
    assert item.operation == "create"
    assert item.fields == {"Name": "Envelope"}


def test_unwrap_item_list_accepts_nested_payload_items():
    items = queue_dml.unwrap_item_list(
        {
            "payload": {
                "items": [
                    {
                        "payload": {
                            "operation": "delete",
                            "sobject": "Account",
                            "id": "001",
                        }
                    }
                ]
            }
        }
    )
    assert len(items) == 1
    assert items[0]["payload"]["operation"] == "delete"


def test_single_item_create_posts_to_sobject(monkeypatch, capsys):
    rec = _DispatchRecorder()
    result = _run_item(
        monkeypatch,
        capsys,
        {
            "credential_key": "sf",
            "payload": {
                "operation": "create",
                "sobject": "Account",
                "record": {"Name": "Acme"},
            },
        },
        rec,
    )
    assert rec.calls == [
        {
            "method": "POST",
            "path": "sobjects/Account",
            "body": {"Name": "Acme"},
            "params": None,
        }
    ]
    assert result["ok"] is True
    assert result["id"] == "001NEW"


def test_single_item_update_strips_id_from_body(monkeypatch, capsys):
    rec = _DispatchRecorder()
    result = _run_item(
        monkeypatch,
        capsys,
        {
            "credential_key": "sf",
            "queue_item": {
                "operation": "update",
                "sobject": "Account",
                "record": {"Id": "001OLD", "Name": "New"},
            },
        },
        rec,
    )
    assert rec.calls[0]["method"] == "PATCH"
    assert rec.calls[0]["path"] == "sobjects/Account/001OLD"
    assert rec.calls[0]["body"] == {"Name": "New"}
    assert result["id"] == "001OLD"


def test_batch_groups_operations_and_preserves_result_order(monkeypatch, capsys):
    rec = _DispatchRecorder()
    result = _run_batch(
        monkeypatch,
        capsys,
        {
            "credential_key": "sf",
            "queue_items": [
                {"operation": "update", "sobject": "Account", "id": "001U", "record": {"Name": "U"}},
                {"operation": "create", "sobject": "Contact", "record": {"LastName": "C"}},
                {"operation": "delete", "sobject": "Account", "id": "001D"},
                {
                    "operation": "upsert",
                    "sobject": "Account",
                    "external_id_field": "External_Id__c",
                    "record": {"External_Id__c": "EXT-1", "Name": "Up"},
                },
            ],
        },
        rec,
    )

    assert [call["method"] for call in rec.calls] == ["POST", "PATCH", "PATCH", "DELETE"]
    assert [call["path"] for call in rec.calls] == [
        "composite/sobjects",
        "composite/sobjects",
        "composite/sobjects/Account/External_Id__c",
        "composite/sobjects",
    ]
    assert rec.calls[0]["body"]["records"][0]["attributes"]["type"] == "Contact"
    assert rec.calls[1]["body"]["records"][0]["Id"] == "001U"
    assert rec.calls[2]["body"]["records"][0]["External_Id__c"] == "EXT-1"
    assert rec.calls[3]["params"]["ids"] == "001D"
    assert [r["id"] for r in result["results"]] == ["001U", "id-0", "001D", "EXT-1"]
    assert [r["index"] for r in result["results"]] == [0, 1, 2, 3]
    assert [r["operation"] for r in result["results"]] == ["update", "create", "delete", "upsert"]
    assert [r["sobject"] for r in result["results"]] == ["Account", "Contact", "Account", "Account"]
    assert result["ok"] is True


def test_batch_chunks_creates_at_200(monkeypatch, capsys):
    rec = _DispatchRecorder()
    items = [
        {"operation": "create", "sobject": "Account", "record": {"Name": f"A{i}"}}
        for i in range(201)
    ]
    _run_batch(monkeypatch, capsys, {"credential_key": "sf", "items": items}, rec)
    assert [len(call["body"]["records"]) for call in rec.calls] == [200, 1]


def test_batch_invalid_item_fails(monkeypatch, capsys):
    rec = _DispatchRecorder()
    with pytest.raises(SystemExit):
        _run_batch(
            monkeypatch,
            capsys,
            {"credential_key": "sf", "items": [{"operation": "update", "sobject": "Account"}]},
            rec,
        )
    assert "missing_id" in capsys.readouterr().out
