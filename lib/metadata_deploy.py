"""Helpers for Salesforce Metadata API deployments.

The pack's regular REST actions go through ``sf_client.sf_request``. Metadata
deployments use Salesforce's SOAP Metadata API, so this module contains the
small XML/normalisation layer shared by the start and monitor actions.
"""

from __future__ import annotations

import base64
import html
import json
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET

from . import sf_client


TERMINAL_STATUSES = {"Succeeded", "SucceededPartial", "Failed", "Canceled"}


def api_version_number(params: Dict[str, Any]) -> str:
    return sf_client.get_api_version(params).lstrip("vV")


def org_identifier(params: Dict[str, Any], client: Optional[Any] = None) -> str:
    for key in ("org", "org_alias", "instance_url", "connection_name", "credential_key"):
        value = params.get(key)
        if value not in (None, ""):
            return str(value)
    if client is not None:
        base_url = getattr(client, "base_url", None)
        if base_url:
            return str(base_url).rstrip("/")
    return "salesforce_org"


def _strip_namespace(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _xml_to_plain(elem: ET.Element) -> Any:
    children = list(elem)
    text = (elem.text or "").strip()
    if not children:
        return text

    out: Dict[str, Any] = {}
    for child in children:
        key = _strip_namespace(child.tag)
        value = _xml_to_plain(child)
        if key in out:
            if not isinstance(out[key], list):
                out[key] = [out[key]]
            out[key].append(value)
        else:
            out[key] = value
    return out


def _first_child_named(elem: ET.Element, name: str) -> Optional[ET.Element]:
    for child in elem.iter():
        if _strip_namespace(child.tag) == name:
            return child
    return None


def parse_soap_result(xml_text: str, result_name: str) -> Dict[str, Any]:
    root = ET.fromstring(xml_text)
    fault = _first_child_named(root, "Fault")
    if fault is not None:
        raise RuntimeError(f"metadata_soap_fault: {_xml_to_plain(fault)}")
    result = _first_child_named(root, result_name)
    if result is None:
        raise RuntimeError(f"metadata_soap_missing_result: {result_name}")
    parsed = _xml_to_plain(result)
    return parsed if isinstance(parsed, dict) else {"value": parsed}


def _session_token_value(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        return value
    for attr in ("token", "access_token", "session_id", "sessionId"):
        nested = getattr(value, attr, None)
        if nested not in (None, ""):
            return str(nested)
    return None


def _extract_session_id(client: Any) -> Optional[str]:
    candidates: List[Any] = [client]
    for attr in ("auth", "_auth"):
        nested = getattr(client, attr, None)
        if nested is not None:
            candidates.append(nested)
    for obj in candidates:
        for attr in ("token", "_token", "access_token", "session_id", "sessionId"):
            token = _session_token_value(getattr(obj, attr, None))
            if token:
                return token
    return None


def _force_login(client: Any) -> None:
    """Force sf-toolkit's lazy auth so the SOAP SessionHeader can be filled."""
    try:
        client.request(
            "GET",
            f"{client.data_url}/limits",
            headers={"Accept": "application/json"},
            timeout=30.0,
            response_status_raise=False,
        )
    except TypeError:
        client.request(
            "GET",
            f"{client.data_url}/limits",
            headers={"Accept": "application/json"},
            timeout=30.0,
        )


def _soap_envelope(inner_xml: str, session_id: Optional[str]) -> str:
    header = ""
    if session_id:
        header = (
            "<env:Header>"
            "<met:SessionHeader>"
            f"<met:sessionId>{html.escape(session_id)}</met:sessionId>"
            "</met:SessionHeader>"
            "</env:Header>"
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/" '
        'xmlns:met="http://soap.sforce.com/2006/04/metadata">'
        f"{header}<env:Body>{inner_xml}</env:Body></env:Envelope>"
    )


def metadata_soap_request(
    params: Dict[str, Any],
    operation: str,
    inner_xml: str,
    *,
    result_name: str,
    timeout: float = 120.0,
) -> Tuple[Dict[str, Any], Any]:
    client = sf_client.get_client(params)
    session_id = _extract_session_id(client)
    if not session_id:
        _force_login(client)
        session_id = _extract_session_id(client)

    url = f"/services/Soap/m/{api_version_number(params)}"
    request_kwargs = {
        "content": _soap_envelope(inner_xml, session_id).encode("utf-8"),
        "headers": {
            "Content-Type": "text/xml; charset=UTF-8",
            "SOAPAction": operation,
        },
        "timeout": timeout,
    }
    try:
        resp = client.request(
            "POST",
            url,
            **request_kwargs,
            response_status_raise=False,
        )
    except TypeError:
        resp = client.request("POST", url, **request_kwargs)
    if resp.status_code >= 400:
        raise RuntimeError(
            f"metadata_soap_error status={resp.status_code} operation={operation} "
            f"body={resp.text[:500]}"
        )
    return parse_soap_result(resp.text, result_name), client


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    return bool(value)


def _bool_xml(name: str, value: Any) -> str:
    return f"<met:{name}>{'true' if _coerce_bool(value) else 'false'}</met:{name}>"


def deploy_options_xml(params: Dict[str, Any]) -> str:
    parts = [
        _bool_xml("allowMissingFiles", params.get("allow_missing_files", False)),
        _bool_xml("autoUpdatePackage", params.get("auto_update_package", False)),
        _bool_xml("checkOnly", params.get("check_only", False)),
        _bool_xml("ignoreWarnings", params.get("ignore_warnings", False)),
        _bool_xml("performRetrieve", params.get("perform_retrieve", False)),
        _bool_xml("purgeOnDelete", params.get("purge_on_delete", False)),
        _bool_xml("rollbackOnError", params.get("rollback_on_error", True)),
        _bool_xml("singlePackage", params.get("single_package", True)),
    ]
    test_level = params.get("test_level")
    if test_level:
        parts.append(f"<met:testLevel>{html.escape(str(test_level))}</met:testLevel>")
    run_tests = params.get("run_tests") or []
    if isinstance(run_tests, str):
        run_tests = [v.strip() for v in run_tests.split(",") if v.strip()]
    for test in run_tests:
        parts.append(f"<met:runTests>{html.escape(str(test))}</met:runTests>")
    return "<met:DeployOptions>" + "".join(parts) + "</met:DeployOptions>"


def start_deploy(params: Dict[str, Any], zip_bytes: bytes) -> Tuple[Dict[str, Any], Any]:
    zip_b64 = base64.b64encode(zip_bytes).decode("ascii")
    inner = (
        "<met:deploy>"
        f"<met:ZipFile>{zip_b64}</met:ZipFile>"
        f"{deploy_options_xml(params)}"
        "</met:deploy>"
    )
    return metadata_soap_request(
        params,
        "deploy",
        inner,
        result_name="result",
        timeout=float(params.get("soap_timeout_seconds", 300)),
    )


def check_deploy_status(params: Dict[str, Any], deploy_id: str) -> Tuple[Dict[str, Any], Any]:
    inner = (
        "<met:checkDeployStatus>"
        f"<met:asyncProcessId>{html.escape(deploy_id)}</met:asyncProcessId>"
        "<met:includeDetails>true</met:includeDetails>"
        "</met:checkDeployStatus>"
    )
    return metadata_soap_request(
        params,
        "checkDeployStatus",
        inner,
        result_name="result",
        timeout=float(params.get("soap_timeout_seconds", 120)),
    )


def _as_list(value: Any) -> List[Any]:
    if value in (None, ""):
        return []
    return value if isinstance(value, list) else [value]


def deployment_counts(details: Dict[str, Any]) -> Dict[str, int]:
    keys = (
        "numberComponentsDeployed",
        "numberComponentsTotal",
        "numberComponentErrors",
        "numberTestsCompleted",
        "numberTestsTotal",
        "numberTestErrors",
    )
    out: Dict[str, int] = {}
    for key in keys:
        try:
            out[key] = int(details.get(key) or 0)
        except (TypeError, ValueError):
            out[key] = 0
    return out


def extract_errors(details: Dict[str, Any]) -> Tuple[List[Any], List[Any]]:
    detail_block = details.get("details") if isinstance(details.get("details"), dict) else {}
    component_errors = _as_list(detail_block.get("componentFailures"))
    run_test_result = detail_block.get("runTestResult")
    if not isinstance(run_test_result, dict):
        run_test_result = {}
    test_errors = _as_list(run_test_result.get("failures"))
    return component_errors, test_errors


def summarize_result(
    details: Dict[str, Any],
    *,
    deploy_id: str,
    monitoring_timed_out: bool = False,
    success_on_partial: bool = False,
) -> Dict[str, Any]:
    status = str(details.get("status") or details.get("state") or "Unknown")
    done_raw = details.get("done")
    done = (
        str(done_raw).lower() == "true"
        if isinstance(done_raw, str)
        else bool(done_raw) or status in TERMINAL_STATUSES
    )
    success_raw = details.get("success")
    success = str(success_raw).lower() == "true" if isinstance(success_raw, str) else bool(success_raw)
    if success_on_partial and status == "SucceededPartial":
        success = True
    component_errors, test_errors = extract_errors(details)
    counts = deployment_counts(details)
    ok = bool(done and success and not monitoring_timed_out)
    summary = {
        "deploy_id": deploy_id,
        "status": status,
        "done": done,
        "success": success,
        "ok": ok,
        "monitoring_timed_out": monitoring_timed_out,
        "counts": counts,
        "component_error_count": len(component_errors),
        "test_error_count": len(test_errors),
    }
    return {
        **summary,
        "component_errors": component_errors,
        "test_errors": test_errors,
        "details": details,
        "summary": summary,
    }


def event_signature(details: Dict[str, Any]) -> Dict[str, Any]:
    component_errors, test_errors = extract_errors(details)
    return {
        "status": details.get("status") or details.get("state"),
        "success": details.get("success"),
        "done": details.get("done"),
        "counts": deployment_counts(details),
        "component_error_count": len(component_errors),
        "test_error_count": len(test_errors),
    }


def write_result_artifact(
    params: Dict[str, Any],
    deploy_id: str,
    result: Dict[str, Any],
) -> Dict[str, Any]:
    exec_id = os.environ.get("ATTUNE_EXEC_ID", "noexec")
    artifact_ref = (
        params.get("result_artifact_ref")
        or params.get("artifact_ref")
        or f"salesforce.metadata_deploy.{deploy_id or exec_id}"
    )
    version = sf_client.allocate_file_artifact_version(
        str(artifact_ref),
        owner="salesforce.monitor_metadata_deploy",
        scope="action",
        artifact_type="file_json",
        content_type="application/json",
        name=params.get("result_artifact_name") or f"Salesforce Metadata Deploy {deploy_id}",
        description=(
            params.get("result_artifact_description")
            or f"Full Salesforce metadata deployment result for {deploy_id}"
        ),
        visibility=params.get("result_artifact_visibility"),
        retention_policy=params.get("result_artifact_retention_policy", "versions"),
        retention_limit=int(params.get("result_artifact_retention_limit", 10)),
    )
    file_path = version["file_path"]
    full_path = os.path.join(sf_client.artifacts_dir(), file_path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2, sort_keys=True, default=str)
        fh.write("\n")
    return {
        "ref": artifact_ref,
        "id": version.get("artifact"),
        "version_id": version.get("id"),
        "version": version.get("version"),
        "file_path": file_path,
        "content_type": "application/json",
    }


def meaningful_status_changes(
    statuses: Iterable[Dict[str, Any]],
) -> Iterable[Tuple[Dict[str, Any], Dict[str, Any]]]:
    previous: Optional[Dict[str, Any]] = None
    for status in statuses:
        signature = event_signature(status)
        if signature != previous:
            previous = signature
            yield status, signature


def sleep_until_next_poll(poll_interval_seconds: float, deadline: float) -> None:
    remaining = deadline - time.time()
    if remaining <= 0:
        return
    time.sleep(min(float(poll_interval_seconds), remaining))
