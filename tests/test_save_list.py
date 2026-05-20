"""Tests for the save_list action's pure helpers and dispatch logic."""

import os
import sys
from typing import Any, Dict, List

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "actions"))

import save_list  # noqa: E402


# ---------------------------------------------------------------------------
# _stamp_attributes
# ---------------------------------------------------------------------------


def test_stamp_attributes_uses_default_when_record_missing_type():
    out = save_list._stamp_attributes([{"Name": "x"}], "Account")
    assert out[0]["attributes"]["type"] == "Account"
    assert out[0]["Name"] == "x"


def test_stamp_attributes_keeps_existing_type():
    out = save_list._stamp_attributes(
        [{"attributes": {"type": "Contact"}, "Email": "a@b.c"}],
        "Account",
    )
    assert out[0]["attributes"]["type"] == "Contact"


def test_stamp_attributes_allows_heterogeneous_with_no_default():
    """Queue-dispatch path: no default sobject, every record carries its type."""
    out = save_list._stamp_attributes(
        [
            {"attributes": {"type": "User"}, "Username": "u@example.com"},
            {"attributes": {"type": "GroupMember"}, "GroupId": "g1"},
        ],
        None,
    )
    types = [r["attributes"]["type"] for r in out]
    assert types == ["User", "GroupMember"]


def test_stamp_attributes_fails_when_type_missing_and_no_default(capsys):
    with pytest.raises(SystemExit):
        save_list._stamp_attributes([{"Name": "x"}], None)
    err = capsys.readouterr().out
    assert "missing_record_type" in err


def test_stamp_attributes_fails_on_non_dict_record(capsys):
    with pytest.raises(SystemExit):
        save_list._stamp_attributes([42], "Account")  # type: ignore[list-item]
    err = capsys.readouterr().out
    assert "invalid_record" in err


# ---------------------------------------------------------------------------
# _split_insert_update — Id presence drives insert vs update
# ---------------------------------------------------------------------------


def test_split_insert_update_partitions_by_id():
    inserts, updates = save_list._split_insert_update(
        [
            {"attributes": {"type": "User"}, "Username": "new@x"},
            {"attributes": {"type": "User"}, "Id": "005xx", "FirstName": "U"},
            {"attributes": {"type": "GroupMember"}, "id": "03Gxx"},  # lowercase id
        ]
    )
    assert len(inserts) == 1
    assert inserts[0]["Username"] == "new@x"
    assert len(updates) == 2
    assert {u.get("Id") or u.get("id") for u in updates} == {"005xx", "03Gxx"}


# ---------------------------------------------------------------------------
# main() dispatch — multi-type batch (queue-style)
# ---------------------------------------------------------------------------


class _DispatchRecorder:
    """Captures sf_request calls so we can assert on dispatch shape."""

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, params, method, path, json_body=None, **_kwargs):
        self.calls.append({"method": method, "path": path, "body": json_body})
        # Pretend each record succeeded.
        recs = (json_body or {}).get("records", [])
        return [
            {"id": f"id-{i}", "success": True, "errors": []}
            for i, _ in enumerate(recs)
        ]


def _run_main(monkeypatch, capsys, params: Dict[str, Any], recorder: _DispatchRecorder):
    """Invoke save_list.main() with `params` and capture stdout."""
    import json

    monkeypatch.setattr(save_list.sf_client, "read_params", lambda: params)
    monkeypatch.setattr(save_list.sf_client, "sf_request", recorder)
    save_list.main()
    out = capsys.readouterr().out.strip()
    return json.loads(out)


def test_main_heterogeneous_queue_batch_routes_correctly(monkeypatch, capsys):
    """A queue could hand us User+GroupMember in one batch, mixed insert/update."""
    rec = _DispatchRecorder()
    params = {
        "credential_key": "sf",
        "records": [
            {"attributes": {"type": "User"}, "Username": "new@x"},                 # insert
            {"attributes": {"type": "User"}, "Id": "005AA", "IsActive": True},     # update
            {"attributes": {"type": "GroupMember"}, "GroupId": "g1", "UserOrGroupId": "u1"},  # insert
            {"attributes": {"type": "GroupMember"}, "Id": "03Gxx"},                # update
        ],
    }
    result = _run_main(monkeypatch, capsys, params, rec)

    # Two composite calls expected: one POST (inserts), one PATCH (updates).
    methods = sorted(c["method"] for c in rec.calls)
    assert methods == ["PATCH", "POST"]

    post_call = next(c for c in rec.calls if c["method"] == "POST")
    patch_call = next(c for c in rec.calls if c["method"] == "PATCH")
    assert post_call["path"] == "composite/sobjects"
    assert patch_call["path"] == "composite/sobjects"

    # Each call must contain heterogeneous types.
    post_types = sorted(r["attributes"]["type"] for r in post_call["body"]["records"])
    patch_types = sorted(r["attributes"]["type"] for r in patch_call["body"]["records"])
    assert post_types == ["GroupMember", "User"]
    assert patch_types == ["GroupMember", "User"]

    assert result["ok"] is True
    assert result["succeeded"] == 4
    assert result["failed"] == 0


def test_main_no_sobject_no_record_type_fails(monkeypatch, capsys):
    rec = _DispatchRecorder()
    params = {"credential_key": "sf", "records": [{"Name": "x"}]}
    with pytest.raises(SystemExit):
        _run_main(monkeypatch, capsys, params, rec)
    assert "missing_record_type" in capsys.readouterr().out


def test_main_default_sobject_stamps_unstamped_records(monkeypatch, capsys):
    rec = _DispatchRecorder()
    params = {
        "credential_key": "sf",
        "sobject": "Account",
        "records": [
            {"Name": "Acme"},
            {"attributes": {"type": "Contact"}, "LastName": "Doe"},
        ],
    }
    _run_main(monkeypatch, capsys, params, rec)
    body = rec.calls[0]["body"]
    types = sorted(r["attributes"]["type"] for r in body["records"])
    assert types == ["Account", "Contact"]


def test_main_upsert_heterogeneous_groups_per_type(monkeypatch, capsys):
    """external_id_field with mixed types groups records and issues per-type calls."""
    rec = _DispatchRecorder()
    params = {
        "credential_key": "sf",
        "external_id_field": "ExternalKey__c",
        "records": [
            {"attributes": {"type": "User"}, "ExternalKey__c": "u-1", "Username": "a@x"},
            {"attributes": {"type": "User"}, "ExternalKey__c": "u-2", "Username": "b@x"},
            {"attributes": {"type": "GroupMember"}, "ExternalKey__c": "gm-1"},
        ],
    }
    _run_main(monkeypatch, capsys, params, rec)
    paths = sorted(c["path"] for c in rec.calls)
    assert paths == [
        "composite/sobjects/GroupMember/ExternalKey__c",
        "composite/sobjects/User/ExternalKey__c",
    ]
    assert all(c["method"] == "PATCH" for c in rec.calls)


def test_main_upsert_requires_resolvable_sobject(monkeypatch, capsys):
    """Upsert with mixed types AND missing attributes.type must fail loudly."""
    rec = _DispatchRecorder()
    params = {
        "credential_key": "sf",
        "external_id_field": "ExternalKey__c",
        "records": [{"ExternalKey__c": "x"}, {"ExternalKey__c": "y"}],
    }
    with pytest.raises(SystemExit):
        _run_main(monkeypatch, capsys, params, rec)
    assert "upsert_requires_sobject" in capsys.readouterr().out


def test_main_empty_records_short_circuits(monkeypatch, capsys):
    rec = _DispatchRecorder()
    result = _run_main(monkeypatch, capsys, {"credential_key": "sf", "records": []}, rec)
    assert rec.calls == []
    assert result == {"ok": True, "results": [], "succeeded": 0, "failed": 0}


def test_main_chunks_at_200(monkeypatch, capsys):
    rec = _DispatchRecorder()
    records = [
        {"attributes": {"type": "User"}, "Username": f"u{i}@x"} for i in range(450)
    ]
    _run_main(monkeypatch, capsys, {"credential_key": "sf", "records": records}, rec)
    # All inserts → POSTs only, chunked into 200/200/50.
    assert [c["method"] for c in rec.calls] == ["POST", "POST", "POST"]
    sizes = [len(c["body"]["records"]) for c in rec.calls]
    assert sizes == [200, 200, 50]
