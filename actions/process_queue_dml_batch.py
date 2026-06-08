#!/usr/bin/env python3
"""salesforce.process_queue_dml_batch — apply queued Salesforce DML items in batches."""

from collections import defaultdict
from typing import Any, Dict, List, Tuple

from _bootstrap import sf_client
from lib import queue_dml

_CHUNK = 200


def _empty_result(item: queue_dml.DmlItem) -> Dict[str, Any]:
    return {
        "index": item.index,
        "operation": item.operation,
        "sobject": item.sobject,
        "success": False,
        "id": item.record_id,
        "errors": [{"message": "not processed"}],
    }


def _annotate_result(item: queue_dml.DmlItem, result: Any) -> Dict[str, Any]:
    body = result or {"success": True, "id": item.record_id, "errors": []}
    if not isinstance(body, dict):
        body = {"success": False, "id": item.record_id, "errors": [{"message": str(body)}]}
    return {
        "index": item.index,
        "operation": item.operation,
        "sobject": item.sobject,
        **body,
    }


def _apply_results(
    results: List[Dict[str, Any]],
    items: List[queue_dml.DmlItem],
    body: Any,
) -> None:
    response_items = body if isinstance(body, list) else [body]
    for item, result in zip(items, response_items):
        results[item.index] = _annotate_result(item, result)


def _send_collection(
    params: Dict[str, Any],
    method: str,
    path: str,
    items: List[queue_dml.DmlItem],
    all_or_none: bool,
    results: List[Dict[str, Any]],
) -> None:
    for batch in sf_client.chunked(items, _CHUNK):
        body = sf_client.sf_request(
            params,
            method,
            path,
            json_body={
                "allOrNone": all_or_none,
                "records": [queue_dml.collection_record(item) for item in batch],
            },
        )
        _apply_results(results, batch, body)


def _send_deletes(
    params: Dict[str, Any],
    items: List[queue_dml.DmlItem],
    all_or_none: bool,
    results: List[Dict[str, Any]],
) -> None:
    for batch in sf_client.chunked(items, _CHUNK):
        body = sf_client.sf_request(
            params,
            "DELETE",
            "composite/sobjects",
            params={
                "ids": ",".join(str(item.record_id) for item in batch),
                "allOrNone": "true" if all_or_none else "false",
            },
        )
        _apply_results(results, batch, body)


def main() -> None:
    try:
        params = sf_client.read_params()
        raw_items = queue_dml.unwrap_item_list(params)
        items = [
            queue_dml.normalize_item(
                raw,
                index=index,
                default_sobject=params.get("sobject"),
                default_external_id_field=params.get("external_id_field"),
            )
            for index, raw in enumerate(raw_items)
        ]
    except (sf_client.ConfigError, queue_dml.QueueDmlError) as exc:
        sf_client.fail(str(exc))

    all_or_none = bool(params.get("all_or_none", False))
    results: List[Dict[str, Any]] = [_empty_result(item) for item in items]

    creates = [item for item in items if item.operation == "create"]
    updates = [item for item in items if item.operation == "update"]
    deletes = [item for item in items if item.operation == "delete"]
    upserts_by_endpoint: Dict[Tuple[str, str], List[queue_dml.DmlItem]] = defaultdict(list)
    for item in items:
        if item.operation == "upsert":
            upserts_by_endpoint[(item.sobject, item.external_id_field or "")].append(item)

    if creates:
        _send_collection(params, "POST", "composite/sobjects", creates, all_or_none, results)
    if updates:
        _send_collection(params, "PATCH", "composite/sobjects", updates, all_or_none, results)
    for (sobject, ext_field), group in upserts_by_endpoint.items():
        _send_collection(
            params,
            "PATCH",
            f"composite/sobjects/{sobject}/{queue_dml.quote_path(ext_field)}",
            group,
            all_or_none,
            results,
        )
    if deletes:
        _send_deletes(params, deletes, all_or_none, results)

    succeeded, failed = queue_dml.result_counts(results)
    sf_client.emit(
        {
            "ok": failed == 0,
            "results": results,
            "succeeded": succeeded,
            "failed": failed,
        }
    )


if __name__ == "__main__":
    main()
