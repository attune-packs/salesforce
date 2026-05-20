#!/usr/bin/env python3
"""salesforce.execute_apex — anonymous Apex via Tooling API.

Delegates to sf-toolkit's ``ToolingResource.execute_anonymous`` which
already wraps the ``/tooling/executeAnonymous`` endpoint and returns a
typed ``AnonExecResult`` NamedTuple. We just translate field names to
Attune's snake_case output convention.
"""

from _bootstrap import sf_client


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    apex = params.get("apex")
    if not apex:
        sf_client.fail("missing_param: apex required")

    client = sf_client.get_client(params)
    result = client.tooling.execute_anonymous(apex)

    sf_client.emit({
        "ok": bool(result.compiled and result.success),
        "compiled": bool(result.compiled),
        "success": bool(result.success),
        "compile_problem":   result.compileProblem,
        "exception_message": result.exceptionMessage,
        "exception_stack":   result.exceptionStackTrace,
        "line":   result.line,
        "column": result.column,
        "result": result._asdict(),
    })


if __name__ == "__main__":
    main()
