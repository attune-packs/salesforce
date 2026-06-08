"""Helpers for Salesforce DML work items delivered from Attune queues."""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


VALID_OPERATIONS = {"create", "update", "upsert", "delete"}
_OP_ALIASES = {
    "insert": "create",
    "create": "create",
    "patch": "update",
    "update": "update",
    "upsert": "upsert",
    "delete": "delete",
    "remove": "delete",
}
_WRAPPER_KEYS = ("queue_item", "item", "payload")


class QueueDmlError(ValueError):
    """Raised when a queue DML item cannot be normalized."""


@dataclass(frozen=True)
class DmlItem:
    index: int
    operation: str
    sobject: str
    fields: Dict[str, Any]
    record_id: Optional[str] = None
    external_id_field: Optional[str] = None
    external_id_value: Optional[Any] = None


def unwrap_single_item(params: Dict[str, Any]) -> Dict[str, Any]:
    """Extract one queue item from common Attune action wrapper shapes."""
    for key in _WRAPPER_KEYS:
        value = params.get(key)
        if isinstance(value, dict):
            return value
    if isinstance(params.get("record"), dict) and params.get("operation"):
        return params
    raise QueueDmlError("missing_param: queue_item/item/payload object required")


def unwrap_item_list(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract queue items from common batch wrapper shapes."""
    for key in ("queue_items", "items", "payload"):
        value = params.get(key)
        if isinstance(value, list):
            if not all(isinstance(item, dict) for item in value):
                raise QueueDmlError(f"invalid_param: {key} must be an array of objects")
            return value
        if isinstance(value, dict):
            nested = value.get("queue_items") or value.get("items")
            if isinstance(nested, list):
                if not all(isinstance(item, dict) for item in nested):
                    raise QueueDmlError(f"invalid_param: {key}.items must be an array of objects")
                return nested
    single = params.get("queue_item") or params.get("item")
    if isinstance(single, dict):
        return [single]
    raise QueueDmlError("missing_param: queue_items/items/payload array required")


def normalize_item(
    item: Dict[str, Any],
    *,
    index: int = 0,
    default_sobject: Optional[str] = None,
    default_external_id_field: Optional[str] = None,
) -> DmlItem:
    if not (item.get("operation") or item.get("op") or item.get("action")):
        for key in _WRAPPER_KEYS:
            wrapped = item.get(key)
            if isinstance(wrapped, dict):
                item = wrapped
                break

    op_raw = item.get("operation") or item.get("op") or item.get("action")
    op = _OP_ALIASES.get(str(op_raw or "").strip().lower())
    if not op:
        raise QueueDmlError(
            f"invalid_operation at index {index}: expected one of {sorted(VALID_OPERATIONS)}"
        )

    record = item.get("record")
    fields = item.get("fields")
    if record is None:
        record = fields
    if record is None:
        record = {}
    if not isinstance(record, dict):
        raise QueueDmlError(f"invalid_record at index {index}: record/fields must be an object")

    attrs = record.get("attributes")
    attr_type = attrs.get("type") if isinstance(attrs, dict) else None
    sobject = item.get("sobject") or attr_type or default_sobject
    if not isinstance(sobject, str) or not sobject.strip():
        raise QueueDmlError(
            f"missing_sobject at index {index}: provide item.sobject, record.attributes.type, or default sobject"
        )
    sobject = sobject.strip()

    clean_fields = {
        key: value
        for key, value in record.items()
        if key not in ("attributes",)
    }

    record_id = item.get("id") or item.get("record_id") or clean_fields.get("Id") or clean_fields.get("id")
    if record_id is not None:
        record_id = str(record_id)

    ext_field = item.get("external_id_field") or default_external_id_field
    ext_value = item.get("external_id_value")
    if ext_value is None and ext_field:
        ext_value = clean_fields.get(str(ext_field))

    if op == "create":
        clean_fields.pop("Id", None)
        clean_fields.pop("id", None)
        if not clean_fields:
            raise QueueDmlError(f"missing_fields at index {index}: create requires record fields")
    elif op == "update":
        if not record_id:
            raise QueueDmlError(f"missing_id at index {index}: update requires id")
        clean_fields.pop("Id", None)
        clean_fields.pop("id", None)
        if not clean_fields:
            raise QueueDmlError(f"missing_fields at index {index}: update requires fields besides id")
    elif op == "upsert":
        if not ext_field:
            raise QueueDmlError(
                f"missing_external_id_field at index {index}: upsert requires external_id_field"
            )
        if ext_value in (None, ""):
            raise QueueDmlError(
                f"missing_external_id_value at index {index}: upsert requires external_id_value or record field"
            )
        clean_fields.pop("Id", None)
        clean_fields.pop("id", None)
    elif op == "delete" and not record_id:
        raise QueueDmlError(f"missing_id at index {index}: delete requires id")

    return DmlItem(
        index=index,
        operation=op,
        sobject=sobject,
        fields=clean_fields,
        record_id=record_id,
        external_id_field=str(ext_field) if ext_field else None,
        external_id_value=ext_value,
    )


def collection_record(item: DmlItem) -> Dict[str, Any]:
    """Return a Composite sObject Collections record for create/update/upsert."""
    rec: Dict[str, Any] = {"attributes": {"type": item.sobject}, **item.fields}
    if item.operation == "update":
        rec["Id"] = item.record_id
    if item.operation == "upsert" and item.external_id_field:
        rec[item.external_id_field] = item.external_id_value
    return rec


def quote_path(value: Any) -> str:
    return urllib.parse.quote(str(value), safe="")


def result_counts(results: Iterable[Optional[Dict[str, Any]]]) -> Tuple[int, int]:
    result_list = list(results)
    succeeded = sum(1 for result in result_list if result and result.get("success"))
    return succeeded, len(result_list) - succeeded
