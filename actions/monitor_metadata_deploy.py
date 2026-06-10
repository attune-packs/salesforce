#!/usr/bin/env python3
"""salesforce.monitor_metadata_deploy — poll Metadata API deploy status."""

import time
from typing import Any, Dict, Optional

from _bootstrap import sf_client
from lib import metadata_deploy


def _emit_status_event(
    params: Dict[str, Any],
    *,
    deploy_id: str,
    org: str,
    status_result: Dict[str, Any],
    signature: Dict[str, Any],
    event_type: str,
) -> bool:
    trigger_ref = params.get("event_trigger_ref") or "salesforce.metadata_deploy_status"
    payload = {
        "event_type": event_type,
        "deploy_id": deploy_id,
        "org": org,
        "status": signature.get("status"),
        "done": signature.get("done"),
        "success": signature.get("success"),
        "counts": signature.get("counts"),
        "component_error_count": signature.get("component_error_count"),
        "test_error_count": signature.get("test_error_count"),
        "check_only": bool(params.get("check_only", False)),
    }
    # Include compact error details as soon as Salesforce reports them; the
    # full result is written to an artifact at the end of monitoring.
    component_errors, test_errors = metadata_deploy.extract_errors(status_result)
    if component_errors:
        payload["component_errors"] = component_errors
    if test_errors:
        payload["test_errors"] = test_errors
    return sf_client.emit_attune_event(
        str(trigger_ref),
        payload,
        trigger_instance_id=params.get("event_trigger_instance_id"),
    )


def main() -> None:
    try:
        params = sf_client.read_params()
        deploy_id = params.get("deploy_id")
        if not deploy_id:
            sf_client.fail("missing_param: deploy_id required")
        deploy_id = str(deploy_id)

        poll_interval = float(params.get("poll_interval_seconds", 15))
        timeout_seconds = float(params.get("timeout_seconds", 3600))
        deadline = time.time() + timeout_seconds
        success_on_partial = bool(params.get("success_on_partial", False))

        latest: Dict[str, Any] = {}
        latest_signature: Optional[Dict[str, Any]] = None
        org = metadata_deploy.org_identifier(params)
        emitted_events = 0
        monitoring_timed_out = False

        while True:
            latest, client = metadata_deploy.check_deploy_status(params, deploy_id)
            org = metadata_deploy.org_identifier(params, client)
            signature = metadata_deploy.event_signature(latest)
            if signature != latest_signature:
                event_type = (
                    "deployment_terminal"
                    if str(signature.get("status")) in metadata_deploy.TERMINAL_STATUSES
                    else "deployment_status_changed"
                )
                if _emit_status_event(
                    params,
                    deploy_id=deploy_id,
                    org=org,
                    status_result=latest,
                    signature=signature,
                    event_type=event_type,
                ):
                    emitted_events += 1
                latest_signature = signature

            status = str(latest.get("status") or latest.get("state") or "")
            done_raw = latest.get("done")
            done = (
                str(done_raw).lower() == "true"
                if isinstance(done_raw, str)
                else bool(done_raw) or status in metadata_deploy.TERMINAL_STATUSES
            )
            if done or status in metadata_deploy.TERMINAL_STATUSES:
                break
            if time.time() >= deadline:
                monitoring_timed_out = True
                break
            metadata_deploy.sleep_until_next_poll(poll_interval, deadline)

        result = metadata_deploy.summarize_result(
            latest,
            deploy_id=deploy_id,
            monitoring_timed_out=monitoring_timed_out,
            success_on_partial=success_on_partial,
        )
        artifact = metadata_deploy.write_result_artifact(params, deploy_id, result)
        result["artifact"] = artifact
        result["org"] = org
        result["events_emitted"] = emitted_events
        sf_client.emit(result)
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))
    except Exception as exc:  # noqa: BLE001 - action boundary
        sf_client.fail(f"metadata_deploy_monitor_failed: {exc}")


if __name__ == "__main__":
    main()
