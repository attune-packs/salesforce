#!/usr/bin/env python3
"""salesforce.start_metadata_deploy — submit a Metadata API ZIP deployment."""

from _bootstrap import sf_client
from lib import metadata_deploy


def main() -> None:
    try:
        params = sf_client.read_params()
        artifact_ref = params.get("metadata_zip_artifact_ref")
        artifact_id = params.get("metadata_zip_artifact_id")
        if not artifact_ref and artifact_id in (None, ""):
            sf_client.fail(
                "missing_param: metadata_zip_artifact_ref or metadata_zip_artifact_id required"
            )

        zip_bytes, artifact_meta, artifact_version = sf_client.read_file_artifact_bytes(
            ref=str(artifact_ref) if artifact_ref else None,
            artifact_id=int(artifact_id) if artifact_id not in (None, "") else None,
            version=(
                int(params["metadata_zip_artifact_version"])
                if params.get("metadata_zip_artifact_version") not in (None, "")
                else None
            ),
        )
        if not zip_bytes:
            sf_client.fail("metadata_zip_empty: artifact content is empty")

        deploy_result, client = metadata_deploy.start_deploy(params, zip_bytes)
        deploy_id = deploy_result.get("id")
        if not deploy_id:
            sf_client.fail("metadata_deploy_missing_id", details=deploy_result)

        status = deploy_result.get("state") or deploy_result.get("status") or "Queued"
        org = metadata_deploy.org_identifier(params, client)
        event_payload = {
            "event_type": "deployment_requested",
            "deploy_id": deploy_id,
            "org": org,
            "status": status,
            "done": deploy_result.get("done"),
            "success": deploy_result.get("success"),
            "check_only": bool(params.get("check_only", False)),
            "metadata_zip_artifact": {
                "ref": artifact_ref or artifact_meta.get("ref"),
                "id": artifact_meta.get("id") or artifact_id,
                "version_id": artifact_version.get("id"),
                "version": artifact_version.get("version"),
                "file_path": artifact_version.get("file_path"),
            },
        }
        trigger_ref = params.get("event_trigger_ref") or "salesforce.metadata_deploy_status"
        event_emitted = sf_client.emit_attune_event(
            str(trigger_ref),
            event_payload,
            trigger_instance_id=params.get("event_trigger_instance_id"),
        )

        sf_client.emit(
            {
                "ok": True,
                "deploy_id": deploy_id,
                "org": org,
                "status": status,
                "done": deploy_result.get("done"),
                "success": deploy_result.get("success"),
                "event_emitted": event_emitted,
                "details": deploy_result,
            }
        )
    except sf_client.ConfigError as exc:
        sf_client.fail(str(exc))
    except Exception as exc:  # noqa: BLE001 - action boundary
        sf_client.fail(f"metadata_deploy_start_failed: {exc}")


if __name__ == "__main__":
    main()
