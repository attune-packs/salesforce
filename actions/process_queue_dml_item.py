#!/usr/bin/env python3
"""salesforce.process_queue_dml_item — apply one queued Salesforce DML item."""

from _bootstrap import sf_client
from lib import queue_dml


def _emit_result(item: queue_dml.DmlItem, result: dict) -> None:
    errors = result.get("errors") or []
    sf_client.emit(
        {
            "ok": bool(result.get("success", not errors)),
            "operation": item.operation,
            "sobject": item.sobject,
            "id": result.get("id") or item.record_id,
            "created": result.get("created"),
            "errors": errors,
            "result": result,
        }
    )


def main() -> None:
    try:
        params = sf_client.read_params()
        raw_item = queue_dml.unwrap_single_item(params)
        item = queue_dml.normalize_item(
            raw_item,
            default_sobject=params.get("sobject"),
            default_external_id_field=params.get("external_id_field"),
        )
    except (sf_client.ConfigError, queue_dml.QueueDmlError) as exc:
        sf_client.fail(str(exc))

    if item.operation == "create":
        body = sf_client.sf_request(
            params,
            "POST",
            f"sobjects/{item.sobject}",
            json_body=item.fields,
        )
        _emit_result(item, body or {"success": True})
        return

    if item.operation == "update":
        sf_client.sf_request(
            params,
            "PATCH",
            f"sobjects/{item.sobject}/{queue_dml.quote_path(item.record_id)}",
            json_body=item.fields,
        )
        _emit_result(item, {"success": True, "id": item.record_id, "errors": []})
        return

    if item.operation == "upsert":
        body = sf_client.sf_request(
            params,
            "PATCH",
            (
                f"sobjects/{item.sobject}/{queue_dml.quote_path(item.external_id_field)}/"
                f"{queue_dml.quote_path(item.external_id_value)}"
            ),
            json_body=item.fields,
        )
        if body is None:
            _emit_result(item, {"success": True, "created": False, "errors": []})
        else:
            _emit_result(item, body)
        return

    if item.operation == "delete":
        sf_client.sf_request(
            params,
            "DELETE",
            f"sobjects/{item.sobject}/{queue_dml.quote_path(item.record_id)}",
        )
        _emit_result(item, {"success": True, "id": item.record_id, "errors": []})
        return

    sf_client.fail(f"unsupported_operation: {item.operation}")


if __name__ == "__main__":
    main()
