#!/usr/bin/env python3
"""salesforce.create_record — create a single record."""

from _bootstrap import sf_client


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    sobject = params.get("sobject")
    fields = params.get("fields")
    if not sobject or not isinstance(fields, dict):
        sf_client.fail("missing_param: sobject and fields (object) required")

    body = sf_client.sf_request(params, "POST", f"sobjects/{sobject}", json_body=fields)
    sf_client.emit({
        "ok": bool(body.get("success", True)),
        "id": body.get("id"),
        "errors": body.get("errors") or [],
    })


if __name__ == "__main__":
    main()
