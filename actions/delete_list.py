#!/usr/bin/env python3
"""salesforce.delete_list — Composite delete of many records by Id."""

from _bootstrap import sf_client


_CHUNK = 200


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    ids = params.get("ids") or []
    all_or_none = bool(params.get("all_or_none", False))
    if not isinstance(ids, list) or not ids:
        sf_client.fail("missing_param: ids[] required")

    results: list = []
    for batch in sf_client.chunked(ids, _CHUNK):
        body = sf_client.sf_request(params,
            "DELETE",
            "composite/sobjects",
            params={
                "ids": ",".join(batch),
                "allOrNone": "true" if all_or_none else "false",
            },
        )
        if isinstance(body, list):
            results.extend(body)
        else:
            results.append(body)

    succeeded = sum(1 for r in results if r and r.get("success"))
    sf_client.emit({
        "ok": True,
        "results": results,
        "succeeded": succeeded,
        "failed": len(results) - succeeded,
    })


if __name__ == "__main__":
    main()
