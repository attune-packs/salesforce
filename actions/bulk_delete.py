#!/usr/bin/env python3
"""salesforce.bulk_delete — Bulk API 2.0 delete (or hardDelete).

Accepts records inline OR via an input artifact. Each record only needs
the ``Id`` field; other fields are ignored by Salesforce. Optionally
supports ``hardDelete`` (skips Recycle Bin, requires the
``Bulk API Hard Delete`` user permission in the org).

You can also pass ``ids`` (an array of Salesforce 15/18-char Ids), or a
SOQL ``id_query`` whose results identify the records to delete. The
action issues the SOQL via the Bulk Query API, streams the resulting
``Id`` column, and feeds it into a Bulk Delete ingest job.
"""

import time
from typing import Any, Dict, List

from _bootstrap import sf_client
from lib import bulk_ingest


def _ids_from_soql(params: Dict[str, Any], soql: str) -> List[str]:
    """Run a Bulk Query for the Id column and stream Ids back."""
    client = sf_client.get_client(params)
    job = sf_client.sf_request(
        params,
        "POST",
        "jobs/query",
        json_body={"operation": "query", "query": soql, "contentType": "CSV"},
    )
    job_id = job["id"]

    deadline = time.time() + int(params.get("timeout_seconds", 600))
    poll = float(params.get("poll_interval_seconds", 2))
    state = "InProgress"
    info: Dict[str, Any] = {}
    while time.time() < deadline:
        info = sf_client.sf_request(params, "GET", f"jobs/query/{job_id}")
        state = info.get("state", state)
        if state in ("JobComplete", "Aborted", "Failed"):
            break
        time.sleep(poll)
    else:
        raise RuntimeError(f"id_query_timeout job_id={job_id} state={state}")
    if state != "JobComplete":
        raise RuntimeError(
            f"id_query_failed job_id={job_id} state={state} "
            f"err={info.get('errorMessage')}"
        )

    # Stream CSV pages and collect Id column.
    import csv
    import io as _io

    base = f"{client.data_url}/jobs/query/{job_id}/results"
    locator = None
    ids: List[str] = []
    while True:
        query_params = {"locator": locator} if locator else None
        resp = client.get(
            base,
            params=query_params,
            headers={"Accept": "text/csv"},
            timeout=120.0,
        )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"id_query_results_failed status={resp.status_code} body={resp.text[:300]}"
            )
        reader = csv.DictReader(_io.StringIO(resp.text))
        for row in reader:
            rid = row.get("Id") or row.get("ID") or row.get("id")
            if rid:
                ids.append(rid)
        locator = resp.headers.get("Sforce-Locator") or resp.headers.get("sforce-locator")
        if not locator or locator == "null":
            break
    return ids


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    sobject = params.get("sobject")
    if not sobject:
        sf_client.fail("missing_param: sobject required")

    operation = "hardDelete" if params.get("hard_delete") else "delete"

    # Source priority: id_query > ids > artifact/records
    id_query = params.get("id_query")
    ids = params.get("ids")
    records: List[Dict[str, Any]]
    if id_query:
        try:
            id_list = _ids_from_soql(params, str(id_query))
        except RuntimeError as exc:
            sf_client.fail(str(exc))
            return
        records = [{"Id": rid} for rid in id_list]
    elif isinstance(ids, list) and ids:
        records = [{"Id": str(rid)} for rid in ids if rid]
    else:
        try:
            records = bulk_ingest.resolve_input_records(sf_client, params)
        except (ValueError, sf_client.ConfigError) as exc:
            sf_client.fail(str(exc))
            return
        # Salesforce only needs the Id column for delete; trim everything else
        # so the CSV upload stays minimal and uniform.
        trimmed: List[Dict[str, Any]] = []
        for r in records:
            rid = r.get("Id") or r.get("id") or r.get("ID")
            if rid:
                trimmed.append({"Id": str(rid)})
        records = trimmed

    if not records:
        sf_client.fail(
            "missing_param: provide id_query, ids[], records[] (with Id), "
            "or input_artifact_ref/input_artifact_id with Id-bearing records"
        )

    summary = bulk_ingest.run_ingest_job(
        sf_client,
        params,
        sobject=sobject,
        operation=operation,
        records=records,
        poll_interval=float(params.get("poll_interval_seconds", 2)),
        timeout_seconds=int(params.get("timeout_seconds", 600)),
    )
    sf_client.emit({
        "ok": summary["state"] == "JobComplete",
        "operation": operation,
        "records_input": len(records),
        **summary,
    })


if __name__ == "__main__":
    main()
