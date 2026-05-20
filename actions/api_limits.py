#!/usr/bin/env python3
"""salesforce.api_limits — return /limits org API usage."""

from _bootstrap import sf_client


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    body = sf_client.sf_request(params, "GET", "limits")
    sf_client.emit({"ok": True, "limits": body})


if __name__ == "__main__":
    main()
