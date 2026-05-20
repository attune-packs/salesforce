#!/usr/bin/env python3
"""salesforce.update_record — patch fields on a record."""

from _bootstrap import sf_client


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    sobject = params.get("sobject")
    record_id = params.get("id")
    fields = params.get("fields")
    if not sobject or not record_id or not isinstance(fields, dict):
        sf_client.fail("missing_param: sobject, id, fields (object) required")

    sf_client.sf_request(params, "PATCH", f"sobjects/{sobject}/{record_id}", json_body=fields,
    )
    sf_client.emit({"ok": True, "id": record_id})


if __name__ == "__main__":
    main()
