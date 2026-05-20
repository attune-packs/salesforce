#!/usr/bin/env python3
"""salesforce.query — execute SOQL and return records."""

from _bootstrap import sf_client


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    soql = params.get("soql")
    next_url = params.get("next_records_url")
    if not soql and not next_url:
        sf_client.fail("missing_param: soql or next_records_url required")

    fetch_all = bool(params.get("fetch_all_pages", False))
    query_all = bool(params.get("query_all", False))
    endpoint = "queryAll" if query_all else "query"

    records: list = []
    total_size = 0
    done = True
    next_records_url: str | None = None

    if next_url:
        body = sf_client.sf_request(params, "GET", next_url)
    else:
        body = sf_client.sf_request(params, "GET", endpoint, params={"q": soql})

    while True:
        records.extend(body.get("records", []))
        total_size = body.get("totalSize", total_size)
        done = body.get("done", True)
        next_records_url = body.get("nextRecordsUrl")
        if not fetch_all or done or not next_records_url:
            break
        body = sf_client.sf_request(params, "GET", next_records_url)

    sf_client.emit({
        "ok": True,
        "records": records,
        "total_size": total_size,
        "done": done if not fetch_all else True,
        "next_records_url": next_records_url if not fetch_all and not done else None,
    })


if __name__ == "__main__":
    main()
