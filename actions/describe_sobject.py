#!/usr/bin/env python3
"""salesforce.describe_sobject — return describe metadata for an sObject."""

from _bootstrap import sf_client


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    sobject = params.get("sobject")
    if not sobject:
        sf_client.fail("missing_param: sobject required")

    body = sf_client.sf_request(params, "GET", f"sobjects/{sobject}/describe")
    sf_client.emit({"ok": True, "describe": body})


if __name__ == "__main__":
    main()
