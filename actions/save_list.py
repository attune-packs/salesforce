#!/usr/bin/env python3
"""salesforce.save_list — Composite create/update/upsert of many records.

Behavior:

* If ``external_id_field`` is set: upsert semantics keyed on that field.
  Requires a single ``sobject`` (since the upsert endpoint is
  ``composite/sobjects/<SObject>/<Field>``); records can omit
  ``attributes.type`` (auto-stamped).

* Otherwise: insert (records WITHOUT ``Id``) vs update (records WITH ``Id``)
  is decided per-record. The Composite sObject Collections endpoint
  (``composite/sobjects``) accepts **heterogeneous** record types in a
  single call as long as each record has ``attributes.type``.

  - If ``sobject`` is provided, it is used as the default ``attributes.type``
    for any record that doesn't already have one.
  - If ``sobject`` is omitted, every record MUST carry ``attributes.type``.
    This is the queue-dispatch path: a queue groups inbound records by
    ``record.attributes.type`` (or doesn't, if it has multiple types per
    batch) and hands the batch to this action as-is.

Each call is chunked at 200 records (the Salesforce composite limit).
"""

from typing import Any, Dict, List, Optional

from _bootstrap import sf_client

_CHUNK = 200


def _stamp_attributes(
    records: List[Dict[str, Any]],
    default_sobject: Optional[str],
) -> List[Dict[str, Any]]:
    """Validate records and ensure each has a non-empty ``attributes.type``.

    If ``default_sobject`` is provided, it is used as a fallback for records
    that do not already declare a type. Records that end up without a type
    cause the action to fail (no silent guessing).
    """
    out: List[Dict[str, Any]] = []
    for idx, rec in enumerate(records):
        if not isinstance(rec, dict):
            sf_client.fail(
                f"invalid_record at index {idx}: each record must be an object"
            )
        attrs = rec.get("attributes")
        rec_type: Optional[str] = None
        if isinstance(attrs, dict):
            t = attrs.get("type")
            if isinstance(t, str) and t.strip():
                rec_type = t.strip()
        if not rec_type:
            rec_type = default_sobject
        if not rec_type:
            sf_client.fail(
                f"missing_record_type at index {idx}: each record must have "
                "attributes.type, or the action must be called with a default sobject"
            )
        new_attrs: Dict[str, Any] = dict(attrs) if isinstance(attrs, dict) else {}
        new_attrs["type"] = rec_type
        out.append({**rec, "attributes": new_attrs})
    return out


def _do_collection(
    params: Dict[str, Any],
    method: str,
    path: str,
    records: List[Dict[str, Any]],
    all_or_none: bool,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for batch in sf_client.chunked(records, _CHUNK):
        body = sf_client.sf_request(
            params,
            method,
            path,
            json_body={"allOrNone": all_or_none, "records": batch},
        )
        if isinstance(body, list):
            results.extend(body)
        else:
            results.append(body)
    return results


def _upsert(
    params: Dict[str, Any],
    sobject: Optional[str],
    ext_field: str,
    records: List[Dict[str, Any]],
    all_or_none: bool,
) -> List[Dict[str, Any]]:
    """Upsert path. The endpoint is per-sobject-type; if records have mixed
    ``attributes.type`` values they are grouped and dispatched per group."""
    by_type: Dict[str, List[Dict[str, Any]]] = {}
    for rec in records:
        t: str | None = (rec.get("attributes") or {}).get("type") or sobject
        if not t:
            sf_client.fail(
                "upsert_requires_sobject: cannot determine sObject type for a record"
            )
        by_type.setdefault(t, []).append(rec)
    results: List[Dict[str, Any]] = []
    for t, group in by_type.items():
        path = f"composite/sobjects/{t}/{ext_field}"
        results.extend(_do_collection(params, "PATCH", path, group, all_or_none))
    return results


def _split_insert_update(
    records: List[Dict[str, Any]],
):
    inserts = [r for r in records if not r.get("Id") and not r.get("id")]
    updates = [r for r in records if r.get("Id") or r.get("id")]
    return inserts, updates


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    sobject = params.get("sobject")
    records = params.get("records")
    all_or_none = bool(params.get("all_or_none", False))
    ext_field = params.get("external_id_field")

    if not isinstance(records, list):
        sf_client.fail("missing_param: records[] required")
    if not records:
        sf_client.emit({"ok": True, "results": [], "succeeded": 0, "failed": 0})
        return

    if ext_field and not sobject:
        # Salesforce's upsert endpoint is per-sObject; grouping handles mixed
        # types as long as every record carries attributes.type. Fail only
        # when we cannot determine a type for some record.
        missing = [
            i
            for i, r in enumerate(records)
            if not isinstance(r, dict)
            or not isinstance(r.get("attributes"), dict)
            or not (r["attributes"].get("type") or "").strip()
        ]
        if missing:
            sf_client.fail(
                "upsert_requires_sobject: external_id_field is set but no "
                "default sobject was provided and one or more records are "
                f"missing attributes.type (indices: {missing[:5]})"
            )

    stamped = _stamp_attributes(records, sobject)

    if ext_field:
        results = _upsert(params, sobject, ext_field, stamped, all_or_none)
    else:
        inserts, updates = _split_insert_update(stamped)
        results = []
        if inserts:
            results.extend(
                _do_collection(
                    params, "POST", "composite/sobjects", inserts, all_or_none
                )
            )
        if updates:
            results.extend(
                _do_collection(
                    params, "PATCH", "composite/sobjects", updates, all_or_none
                )
            )

    succeeded = sum(1 for r in results if r and r.get("success"))
    sf_client.emit(
        {
            "ok": True,
            "results": results,
            "succeeded": succeeded,
            "failed": len(results) - succeeded,
        }
    )


if __name__ == "__main__":
    main()
