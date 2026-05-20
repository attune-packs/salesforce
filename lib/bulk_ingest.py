"""
Helpers for Bulk API 2.0 ingest jobs (insert / update / upsert / delete).

This module is a thin Attune-side adapter around sf-toolkit's
``BulkApiIngestJob``: the toolkit owns job creation, batched CSV upload
(with the 100MB-per-batch chunking the Salesforce docs require), state
polling, and result retrieval. We just normalise the return shape for
Attune action emitters.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def resolve_input_records(sf_client, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pick the records source for a bulk write action.

    Priority:
      1. ``input_artifact_id`` (+ optional ``input_artifact_version``)
      2. ``input_artifact_ref`` (+ optional ``input_artifact_version``)
      3. Inline ``records`` array on the params payload.

    Returns a list of flat field-map dicts ready for
    ``BulkApiIngestJob.upload_batches``. Empty list is allowed — the
    caller decides whether that's an error.
    """
    artifact_id = params.get("input_artifact_id")
    artifact_ref = params.get("input_artifact_ref")
    version = params.get("input_artifact_version")
    if artifact_id is not None:
        return sf_client.read_artifact_records(
            artifact_id=int(artifact_id),
            version=int(version) if version is not None else None,
        )
    if artifact_ref:
        return sf_client.read_artifact_records(
            ref=str(artifact_ref),
            version=int(version) if version is not None else None,
        )
    inline = params.get("records") or []
    if not isinstance(inline, list):
        raise ValueError("records must be an array of records")
    return [r for r in inline if isinstance(r, dict)]


def run_ingest_job(
    sf_client,
    params: Dict[str, Any],
    *,
    sobject: str,
    operation: str,                       # insert | update | upsert | delete | hardDelete
    records: List[Dict[str, Any]],
    external_id_field: Optional[str] = None,
    poll_interval: float = 2.0,
    timeout_seconds: int = 600,
) -> Dict[str, Any]:
    """Run a Bulk API 2.0 ingest job synchronously and return summary info.

    Returns a dict shaped to match the prior custom implementation so
    the bulk_insert / bulk_update / bulk_upsert / delete_list actions
    don't change.
    """
    from sf_toolkit.data.bulk import BulkApiIngestJob

    client = sf_client.get_client(params)
    callout: Dict[str, Any] = {"timeout": float(timeout_seconds)}

    job = BulkApiIngestJob.init_job(
        sobject_type=sobject,
        operation=operation,  # type: ignore[arg-type]
        external_id_field=external_id_field,
        connection=client,
        **callout,
    )
    if records:
        job.upload_batches(records, **callout)
    job.monitor_until_complete(poll_interval=poll_interval, connection=client)

    return {
        "job_id": str(getattr(job, "id", "")),
        "state": str(getattr(job, "state", "")),
        "records_processed": getattr(job, "numberRecordsProcessed", None),
        "records_failed": getattr(job, "numberRecordsFailed", None),
        "info": {
            "object": getattr(job, "object", sobject),
            "operation": getattr(job, "operation", operation),
            "errorMessage": getattr(job, "errorMessage", None),
            "totalProcessingTime": getattr(job, "totalProcessingTime", None),
            "apiActiveProcessingTime": getattr(job, "apiActiveProcessingTime", None),
            "apexProcessingTime": getattr(job, "apexProcessingTime", None),
        },
    }
