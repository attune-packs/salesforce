#!/usr/bin/env python3
"""salesforce.get_record — fetch a single record by Id."""

from _bootstrap import sf_client


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    sobject = params.get("sobject")
    record_id = params.get("id")
    if not sobject or not record_id:
        sf_client.fail("missing_param: sobject and id required")

    path = f"sobjects/{sobject}/{record_id}"
    fields = params.get("fields")
    qp = {"fields": ",".join(fields)} if fields else None

    record = sf_client.sf_request(params, "GET", path, params=qp)
    sf_client.emit({"ok": True, "record": record})


if __name__ == "__main__":
    main()
