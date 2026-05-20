#!/usr/bin/env python3
"""salesforce.bulk_update — Bulk API 2.0 update.

Accepts records inline (``records`` param) for small batches OR an
artifact reference (``input_artifact_ref`` / ``input_artifact_id``) for
large datasets. Each record must include the ``Id`` field.
"""

from _bootstrap import sf_client
from lib import bulk_ingest


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    sobject = params.get("sobject")
    try:
        records = bulk_ingest.resolve_input_records(sf_client, params)
    except (ValueError, sf_client.ConfigError) as exc:
        sf_client.fail(str(exc))
        return

    if not sobject or not records:
        sf_client.fail(
            "missing_param: sobject and a non-empty records source "
            "(records[] or input_artifact_ref/input_artifact_id) required"
        )

    summary = bulk_ingest.run_ingest_job(
        sf_client,
        params,
        sobject=sobject,
        operation="update",
        records=records,
        poll_interval=float(params.get("poll_interval_seconds", 2)),
        timeout_seconds=int(params.get("timeout_seconds", 600)),
    )
    sf_client.emit({
        "ok": summary["state"] == "JobComplete",
        "records_input": len(records),
        **summary,
    })


if __name__ == "__main__":
    main()
