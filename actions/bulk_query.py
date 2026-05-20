#!/usr/bin/env python3
"""salesforce.bulk_query — Bulk Query API 2.0 for large SOQL results.

Results are streamed page-by-page from the Bulk API (CSV) directly to a
file-backed Attune artifact on the shared artifact volume, rather than
being materialised into the execution row's ``result`` JSONB. The
emit payload references the artifact by id/ref/version so downstream
tasks can fetch or post-process it without bloating PostgreSQL.
"""

import os
import time
from typing import Any, Dict, Optional

from _bootstrap import sf_client


def _stream_results_to_file(
    params: Dict[str, Any],
    job_id: str,
    out_path: str,
    max_records: Optional[int],
) -> Dict[str, int]:
    """Stream Bulk-Query CSV pages straight into ``out_path``.

    The first page's header line is preserved; subsequent pages have their
    header stripped so the file is a single, well-formed CSV. Returns a
    summary with byte/row counts.
    """
    client = sf_client.get_client(params)
    base = f"{client.data_url}/jobs/query/{job_id}/results"
    locator: Optional[str] = None
    rows = 0
    bytes_written = 0
    truncated = False
    header_written = False

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as fh:
        while True:
            query_params = {"locator": locator} if locator else None
            with client.stream(
                "GET",
                base,
                params=query_params,
                headers={"Accept": "text/csv"},
                timeout=120.0,
            ) as resp:
                if resp.status_code >= 400:
                    body = resp.read().decode("utf-8", errors="replace")[:500]
                    raise RuntimeError(
                        f"bulk_query_results_failed status={resp.status_code} body={body}"
                    )
                # Salesforce returns the data row count for this page in this
                # header; we use it as the authoritative per-page row count
                # and avoid having to count newlines ourselves for limits.
                page_rows_hdr = resp.headers.get("Sforce-NumberOfRecords") or resp.headers.get(
                    "sforce-numberofrecords"
                )
                try:
                    page_rows = int(page_rows_hdr) if page_rows_hdr else None
                except ValueError:
                    page_rows = None

                first_chunk = True
                buffered_header_pending = not header_written
                for chunk in resp.iter_bytes():
                    if not chunk:
                        continue
                    if buffered_header_pending:
                        # Write everything including the header line for the
                        # very first page.
                        fh.write(chunk)
                        bytes_written += len(chunk)
                        buffered_header_pending = False
                        first_chunk = False
                        header_written = True
                        continue
                    if first_chunk and header_written:
                        # Strip the header line of subsequent pages.
                        nl = chunk.find(b"\n")
                        if nl == -1:
                            # Header spills into next chunk; rare but handle:
                            # keep accumulating until we see a newline.
                            # Simpler: skip this whole chunk; SF pages always
                            # have a header followed by row data so a chunk
                            # without any newline would be pathological.
                            first_chunk = False
                            continue
                        chunk = chunk[nl + 1:]
                        first_chunk = False
                        if not chunk:
                            continue
                    fh.write(chunk)
                    bytes_written += len(chunk)

                if page_rows is not None:
                    rows += page_rows

                if max_records and rows >= max_records:
                    truncated = rows > max_records
                    rows = min(rows, max_records)
                    return {
                        "rows": rows,
                        "bytes": bytes_written,
                        "truncated": int(truncated),
                    }

                locator = resp.headers.get("Sforce-Locator") or resp.headers.get(
                    "sforce-locator"
                )
            if not locator or locator == "null":
                return {"rows": rows, "bytes": bytes_written, "truncated": 0}


def main() -> None:
    try:
        params = sf_client.read_params()
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))

    soql = params.get("soql")
    if not soql:
        sf_client.fail("missing_param: soql required")

    operation = "queryAll" if params.get("query_all") else "query"
    job = sf_client.sf_request(
        params,
        "POST",
        "jobs/query",
        json_body={"operation": operation, "query": soql, "contentType": "CSV"},
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
        sf_client.fail(f"bulk_query_timeout job_id={job_id} state={state}")

    if state != "JobComplete":
        sf_client.emit({
            "ok": False,
            "job_id": job_id,
            "state": state,
            "records_processed": info.get("numberRecordsProcessed", 0),
            "error": info.get("errorMessage"),
        })
        return

    # ---- Allocate file artifact and stream CSV to disk -----------------
    exec_id = os.environ.get("ATTUNE_EXEC_ID", "noexec")
    default_ref = f"salesforce.bulk_query.{exec_id}"
    artifact_ref = params.get("artifact_ref") or default_ref

    version = sf_client.allocate_file_artifact_version(
        artifact_ref,
        owner="salesforce.bulk_query",
        scope="action",
        artifact_type="file_text",
        content_type="text/csv",
        name=params.get("artifact_name") or f"Bulk Query {job_id}",
        description=(
            params.get("artifact_description")
            or f"CSV results of Bulk API job {job_id}"
        ),
        visibility=params.get("artifact_visibility"),
        retention_policy=params.get("artifact_retention_policy", "versions"),
        retention_limit=int(params.get("artifact_retention_limit", 10)),
    )

    file_path = version["file_path"]
    full_path = os.path.join(sf_client.artifacts_dir(), file_path)

    max_records = params.get("max_records")
    summary = _stream_results_to_file(
        params,
        job_id,
        full_path,
        int(max_records) if max_records else None,
    )

    sf_client.emit({
        "ok": True,
        "job_id": job_id,
        "state": state,
        "records_processed": info.get("numberRecordsProcessed", summary["rows"]),
        "artifact": {
            "ref": artifact_ref,
            "id": version.get("artifact"),
            "version_id": version.get("id"),
            "version": version.get("version"),
            "file_path": file_path,
            "content_type": "text/csv",
            "rows": summary["rows"],
            "bytes": summary["bytes"],
            "truncated": bool(summary["truncated"]),
        },
    })


if __name__ == "__main__":
    main()
