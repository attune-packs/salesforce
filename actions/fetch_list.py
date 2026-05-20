#!/usr/bin/env python3
"""salesforce.fetch_list — Composite read of many records by Id."""

from _bootstrap import sf_client


_CHUNK = 2000  # Salesforce hard limit on collections retrieve


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    sobject = params.get("sobject")
    ids = params.get("ids") or []
    fields = params.get("fields") or []
    if not sobject or not isinstance(ids, list) or not isinstance(fields, list) or not fields:
        sf_client.fail("missing_param: sobject, ids[], fields[] required")

    records: list = []
    for batch in sf_client.chunked(ids, _CHUNK):
        body = sf_client.sf_request(params,
            "POST",
            f"composite/sobjects/{sobject}",
            json_body={"ids": batch, "fields": fields},
        )
        # Returns array aligned with input order; missing/inaccessible -> None
        if isinstance(body, list):
            records.extend(body)
        else:
            records.extend([None] * len(batch))

    found = sum(1 for r in records if r)
    sf_client.emit({
        "ok": True,
        "records": records,
        "found": found,
        "missing": len(records) - found,
    })


if __name__ == "__main__":
    main()
