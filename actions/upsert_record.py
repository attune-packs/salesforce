#!/usr/bin/env python3
"""salesforce.upsert_record — upsert by external Id field."""

import urllib.parse

from _bootstrap import sf_client


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    sobject = params.get("sobject")
    field = params.get("external_id_field")
    value = params.get("external_id_value")
    fields = params.get("fields")
    if not all([sobject, field, value]) or not isinstance(fields, dict):
        sf_client.fail(
            "missing_param: sobject, external_id_field, external_id_value, fields required",
        )

    encoded = urllib.parse.quote(str(value), safe="")
    body = sf_client.sf_request(params,
        "PATCH",
        f"sobjects/{sobject}/{field}/{encoded}",
        json_body=fields,
    )
    # 201 returns body with id+success+created; 204 returns no body (update only).
    if body is None:
        sf_client.emit({"ok": True, "created": False, "id": None, "errors": []})
        return
    sf_client.emit({
        "ok": bool(body.get("success", True)),
        "id": body.get("id"),
        "created": bool(body.get("created", False)),
        "errors": body.get("errors") or [],
    })


if __name__ == "__main__":
    main()
