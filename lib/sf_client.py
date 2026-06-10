"""Shared Salesforce client helper for the salesforce pack.

This module is a thin Attune-side adapter on top of `sf-toolkit`. The
toolkit provides:

* Auth-flow auto-selection (``lazy_login(**kwargs)``).
* In-process connection caching via ``SalesforceClient``'s class-level
  registry (``connection_name=...`` + ``SalesforceClient.get_connection(name)``).
* Automatic token refresh on 401 responses, exposed via
  ``token_refresh_callback``.
* A pre-authenticated ``httpx.Client`` (``SalesforceClient`` subclasses
  ``httpx.Client``) — every method on httpx.Client is available, with
  the bearer-auth header injected automatically.

The Attune-specific contributions in this module are:

* Resolving an action/sensor's direct ``credentials`` input or
  ``credential_key`` parameter to a Salesforce credential blob.
* Persisting refreshed access tokens back to the keystore for
  ``credential_key``-backed connections (under a derived ref
  ``<credential_key>_session_token``) via the toolkit's
  ``token_refresh_callback`` so sibling worker processes can skip the
  initial login round-trip. Direct credentials use only in-process caching.
* A ``sf_request(...)`` convenience that targets the Salesforce REST
  API through the same ``SalesforceClient`` (so we never re-authenticate
  separately and never spin up a second HTTP client).

This module uses ``attune-sdk`` for action stdin/stdout helpers, execution
context, and generated keystore API calls. Raw artifact endpoints still go
through the SDK's authenticated ``httpx`` client until typed artifact helpers
are available in the SDK.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, Iterator, List, NoReturn, Optional, Tuple

import httpx

logger = logging.getLogger("attune.salesforce")

DEFAULT_API_VERSION = "v60.0"

# Discard cached session tokens older than this. Salesforce access-token
# TTL is org-configurable but defaults to ~2 hours; 90 minutes leaves
# headroom for in-flight requests near the boundary.
DEFAULT_TOKEN_MAX_AGE_SECONDS = 90 * 60

# Default httpx timeout for Attune API round-trips.
_KEYSTORE_TIMEOUT = httpx.Timeout(15.0)

# Fields we forward into sf_toolkit.lazy_login(). Any extra keys in the
# credential blob are dropped to avoid TypeError from unknown kwargs.
_LAZY_LOGIN_FIELDS = (
    "username",
    "password",
    "consumer_key",
    "consumer_secret",
    "private_key",
    "domain",
    "security_token",
    "sf_cli_alias",
    "sf_exec_path",
    "organizationId",
)


class ConfigError(Exception):
    """Raised when the action/sensor inputs do not yield a usable client."""


# ---------------------------------------------------------------------------
# I/O helpers used by every action
# ---------------------------------------------------------------------------


def read_params() -> Dict[str, Any]:
    try:
        from attune.action import read_params as _read_params  # type: ignore
    except ImportError:
        import sys

        raw = sys.stdin.read().strip()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"invalid_input_json: {exc}") from exc
    try:
        return _read_params()
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid_input_json: {exc}") from exc


def emit(payload: Dict[str, Any]) -> None:
    try:
        from attune.action import emit_result  # type: ignore
    except ImportError:
        import sys

        json.dump(payload, sys.stdout, default=str)
        sys.stdout.write("\n")
        sys.stdout.flush()
        return
    emit_result(payload)


def fail(msg: str, **extra: Any) -> NoReturn:
    import sys

    body: Dict[str, Any] = {"ok": False, "error": msg}
    body.update(extra)
    emit(body)
    sys.exit(1)


def chunked(items: Iterable[Any], size: int) -> Iterator[List[Any]]:
    buf: List[Any] = []
    for item in items:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def to_plain(record: Any) -> Any:
    """Best-effort conversion of sf-toolkit SObject instances to plain JSON."""
    if record is None or isinstance(record, (str, int, float, bool)):
        return record
    if isinstance(record, dict):
        return {k: to_plain(v) for k, v in record.items()}
    if isinstance(record, (list, tuple)):
        return [to_plain(v) for v in record]
    for attr in ("model_dump", "dict", "to_dict"):
        method = getattr(record, attr, None)
        if callable(method):
            try:
                value = method()
            except TypeError:
                continue
            return to_plain(value)
    if hasattr(record, "__dict__"):
        return {
            k: to_plain(v) for k, v in vars(record).items() if not k.startswith("_")
        }
    return str(record)


# ---------------------------------------------------------------------------
# Attune API helpers (SDK first, direct httpx fallback for local tests)
# ---------------------------------------------------------------------------


def _attune_env() -> Tuple[str, str]:
    try:
        import attune  # type: ignore
    except ImportError:
        api_url = os.environ.get("ATTUNE_API_URL")
        api_token = os.environ.get("ATTUNE_API_TOKEN")
    else:
        api_url = attune.context.api_url
        api_token = attune.context.api_token
    if not api_url or not api_token:
        raise ConfigError(
            "missing_attune_env: ATTUNE_API_URL and ATTUNE_API_TOKEN required to "
            "resolve credentials/session via the keystore"
        )
    return api_url.rstrip("/"), api_token


def _auth_header() -> Dict[str, str]:
    _, token = _attune_env()
    return {"Authorization": f"Bearer {token}"}


def _attune_sdk_client() -> Optional[Any]:
    try:
        import attune  # type: ignore
    except ImportError:
        return None
    try:
        return attune.context.client
    except RuntimeError as exc:
        raise ConfigError(
            "missing_attune_env: ATTUNE_API_URL and ATTUNE_API_TOKEN required to "
            "resolve credentials/session via the keystore"
        ) from exc


def _attune_http_request(
    method: str,
    path: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    json_body: Any = None,
    timeout: httpx.Timeout = _KEYSTORE_TIMEOUT,
) -> httpx.Response:
    client = _attune_sdk_client()
    if client is not None:
        return client.get_httpx_client().request(
            method,
            path,
            headers=headers,
            json=json_body,
            timeout=timeout,
        )

    api_url, _ = _attune_env()
    merged_headers = {**_auth_header(), **(headers or {})}
    url = f"{api_url}{path}"
    method_l = method.lower()
    if method_l == "get":
        return httpx.get(url, headers=merged_headers, timeout=timeout)
    if method_l == "put":
        return httpx.put(url, headers=merged_headers, json=json_body, timeout=timeout)
    if method_l == "post":
        return httpx.post(url, headers=merged_headers, json=json_body, timeout=timeout)
    return httpx.request(
        method,
        url,
        headers=merged_headers,
        json=json_body,
        timeout=timeout,
    )


def _fetch_keystore_value(ref: str) -> Optional[Any]:
    client = _attune_sdk_client()
    if client is not None:
        from attune.api_client.api.secrets import get_key  # type: ignore

        result = get_key.sync_detailed(ref, client=client)
        status = int(result.status_code)
        if status == 404:
            return None
        if status >= 400:
            raise ConfigError(
                f"keystore_lookup_failed ref={ref} status={status} body={result.content[:300]!r}"
            )
        return getattr(result.parsed.data, "value", None) if result.parsed else None

    resp = _attune_http_request(
        "GET",
        f"/api/v1/keys/{ref}",
        headers={"Accept": "application/json"},
        timeout=_KEYSTORE_TIMEOUT,
    )
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise ConfigError(
            f"keystore_lookup_failed ref={ref} status={resp.status_code} body={resp.text[:300]}"
        )
    body = resp.json() or {}
    data = body.get("data") or body
    return data.get("value")


def _fetch_credential_from_keystore(ref: str) -> Dict[str, Any]:
    value = _fetch_keystore_value(ref)
    if value is None:
        raise ConfigError(f"credential_key_not_found ref={ref}")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ConfigError(
                f"credential_key_not_object ref={ref} (expected JSON object, got string): {exc}"
            ) from exc
    if not isinstance(value, dict):
        raise ConfigError(
            f"credential_key_not_object ref={ref} (expected JSON object, got {type(value).__name__})"
        )
    return value


def _put_keystore_value(ref: str, value: Any, *, encrypted: bool = True) -> bool:
    """PUT /api/v1/keys/{ref}. Returns True if updated, False if not found."""
    client = _attune_sdk_client()
    if client is not None:
        from attune.api_client.api.secrets import update_key  # type: ignore
        from attune.api_client.models.update_key_request import UpdateKeyRequest  # type: ignore

        result = update_key.sync_detailed(
            ref,
            client=client,
            body=UpdateKeyRequest(value=value, encrypted=encrypted),
        )
        status = int(result.status_code)
        if status == 404:
            return False
        if status >= 400:
            raise ConfigError(
                f"keystore_update_failed ref={ref} status={status} body={result.content[:300]!r}"
            )
        return True

    resp = _attune_http_request(
        "PUT",
        f"/api/v1/keys/{ref}",
        headers={"Content-Type": "application/json"},
        json_body={"value": value, "encrypted": encrypted},
        timeout=_KEYSTORE_TIMEOUT,
    )
    if resp.status_code == 404:
        return False
    if resp.status_code >= 400:
        raise ConfigError(
            f"keystore_update_failed ref={ref} status={resp.status_code} body={resp.text[:300]}"
        )
    return True


def _post_keystore_key(
    ref: str,
    value: Any,
    *,
    name: str,
    pack_ref: str,
    encrypted: bool = True,
) -> None:
    client = _attune_sdk_client()
    if client is not None:
        from attune.api_client.api.secrets import create_key  # type: ignore
        from attune.api_client.models.create_key_request import CreateKeyRequest  # type: ignore
        from attune.api_client.models.owner_type import OwnerType  # type: ignore

        result = create_key.sync_detailed(
            client=client,
            body=CreateKeyRequest(
                ref=ref,
                name=name,
                owner_type=OwnerType.PACK,
                owner_pack_ref=pack_ref,
                value=value,
                encrypted=encrypted,
            ),
        )
        status = int(result.status_code)
        if status == 409:
            _put_keystore_value(ref, value, encrypted=encrypted)
            return
        if status >= 400:
            raise ConfigError(
                f"keystore_create_failed ref={ref} status={status} body={result.content[:300]!r}"
            )
        return

    resp = _attune_http_request(
        "POST",
        "/api/v1/keys",
        headers={"Content-Type": "application/json"},
        json_body={
            "ref": ref,
            "name": name,
            "owner_type": "pack",
            "owner_pack_ref": pack_ref,
            "value": value,
            "encrypted": encrypted,
        },
        timeout=_KEYSTORE_TIMEOUT,
    )
    if resp.status_code == 409:
        # Race: a sibling created it between our PUT and POST. Retry update.
        _put_keystore_value(ref, value, encrypted=encrypted)
        return
    if resp.status_code >= 400:
        raise ConfigError(
            f"keystore_create_failed ref={ref} status={resp.status_code} body={resp.text[:300]}"
        )


# ---------------------------------------------------------------------------
# Artifact helpers (for actions that produce large outputs and shouldn't
# overload execution.result with full payloads)
# ---------------------------------------------------------------------------


def allocate_file_artifact_version(
    artifact_ref: str,
    *,
    owner: str,
    scope: str = "action",
    artifact_type: str = "file_text",
    content_type: str = "text/plain",
    name: Optional[str] = None,
    description: Optional[str] = None,
    visibility: Optional[str] = None,
    retention_policy: str = "versions",
    retention_limit: int = 10,
    execution_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Upsert a file artifact by ref and allocate a new version.

    Single API call to ``POST /api/v1/artifacts/ref/{ref}/versions/file`` —
    creates the artifact if missing, allocates the next version, and
    returns the relative ``file_path`` the action should write to under
    ``$ATTUNE_ARTIFACTS_DIR``.

    Returns the version response dict (`id`, `artifact`, `version`,
    `file_path`, ...) verbatim so the caller can stash the IDs in their
    emit payload.
    """
    if execution_id is None:
        env_exec = os.environ.get("ATTUNE_EXEC_ID")
        if env_exec:
            try:
                execution_id = int(env_exec)
            except ValueError:
                execution_id = None

    payload: Dict[str, Any] = {
        "scope": scope,
        "owner": owner,
        "type": artifact_type,
        "retention_policy": retention_policy,
        "retention_limit": retention_limit,
        "content_type": content_type,
        "created_by": owner,
    }
    if execution_id is not None:
        payload["execution"] = execution_id
    if visibility is not None:
        payload["visibility"] = visibility
    if name is not None:
        payload["name"] = name
    if description is not None:
        payload["description"] = description

    resp = _attune_http_request(
        "POST",
        f"/api/v1/artifacts/ref/{artifact_ref}/versions/file",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json_body=payload,
        timeout=_KEYSTORE_TIMEOUT,
    )
    if resp.status_code >= 400:
        raise ConfigError(
            f"artifact_alloc_failed ref={artifact_ref} status={resp.status_code} "
            f"body={resp.text[:300]}"
        )
    return resp.json().get("data", {})


def artifacts_dir() -> str:
    """Absolute root of the shared artifact volume mounted into the worker."""
    val = os.environ.get("ATTUNE_ARTIFACTS_DIR")
    if not val:
        raise ConfigError(
            "missing_attune_env: ATTUNE_ARTIFACTS_DIR required to stream "
            "results to a file artifact"
        )
    return val


def emit_attune_event(
    trigger_ref: str,
    payload: Dict[str, Any],
    *,
    trigger_instance_id: Optional[str] = None,
    timeout: float = 30.0,
) -> bool:
    """Emit an Attune event from an action.

    Actions use this for deployment lifecycle events. Event emission is
    best-effort: failures are logged and reported to the caller as ``False``
    but do not make the Salesforce operation fail.
    """
    body: Dict[str, Any] = {"trigger_ref": trigger_ref, "payload": payload}
    if trigger_instance_id:
        body["trigger_instance_id"] = trigger_instance_id
    try:
        resp = _attune_http_request(
            "POST",
            "/api/v1/events",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json_body=body,
            timeout=httpx.Timeout(float(timeout)),
        )
        if resp.status_code >= 400:
            logger.warning(
                "emit_attune_event failed trigger_ref=%s status=%s body=%s",
                trigger_ref,
                resp.status_code,
                resp.text[:300],
            )
            return False
        return True
    except Exception as exc:  # noqa: BLE001 - event emission is non-critical
        logger.warning("emit_attune_event failed trigger_ref=%s: %s", trigger_ref, exc)
        return False


def _get_artifact_metadata(*, ref: Optional[str] = None, artifact_id: Optional[int] = None) -> Dict[str, Any]:
    """GET /api/v1/artifacts/ref/{ref} or /api/v1/artifacts/{id}."""
    if not ref and artifact_id is None:
        raise ConfigError("artifact_lookup_failed: ref or artifact_id required")
    path = (
        f"/api/v1/artifacts/ref/{ref}"
        if ref
        else f"/api/v1/artifacts/{int(artifact_id)}"
    )
    resp = _attune_http_request(
        "GET",
        path,
        headers={"Accept": "application/json"},
        timeout=_KEYSTORE_TIMEOUT,
    )
    if resp.status_code == 404:
        target = ref if ref else f"id={artifact_id}"
        raise ConfigError(f"artifact_not_found: {target}")
    if resp.status_code >= 400:
        raise ConfigError(
            f"artifact_lookup_failed status={resp.status_code} body={resp.text[:300]}"
        )
    return resp.json().get("data", {})


def _get_artifact_version(artifact_id: int, version: Optional[int] = None) -> Dict[str, Any]:
    """Fetch a specific version (or the latest) of an artifact."""
    sub = f"versions/{int(version)}" if version is not None else "versions/latest"
    resp = _attune_http_request(
        "GET",
        f"/api/v1/artifacts/{int(artifact_id)}/{sub}",
        headers={"Accept": "application/json"},
        timeout=_KEYSTORE_TIMEOUT,
    )
    if resp.status_code == 404:
        raise ConfigError(
            f"artifact_version_not_found artifact_id={artifact_id} version={version}"
        )
    if resp.status_code >= 400:
        raise ConfigError(
            f"artifact_version_failed status={resp.status_code} body={resp.text[:300]}"
        )
    return resp.json().get("data", {})


def read_file_artifact_bytes(
    *,
    ref: Optional[str] = None,
    artifact_id: Optional[int] = None,
    version: Optional[int] = None,
) -> Tuple[bytes, Dict[str, Any], Dict[str, Any]]:
    """Resolve a file-backed Attune artifact and return its bytes.

    Returns ``(content, artifact_metadata, version_metadata)``. Reads
    file-backed versions directly from ``ATTUNE_ARTIFACTS_DIR`` and falls
    back to the artifact download endpoint when no file path is present.
    """
    if ref is None and artifact_id is None:
        raise ConfigError("read_file_artifact_bytes: ref or artifact_id required")

    meta: Dict[str, Any]
    if artifact_id is None:
        meta = _get_artifact_metadata(ref=ref)
        artifact_id = int(meta["id"])
    else:
        meta = _get_artifact_metadata(artifact_id=int(artifact_id))

    ver = _get_artifact_version(int(artifact_id), version=version)
    file_path = ver.get("file_path")
    if file_path:
        full = os.path.join(artifacts_dir(), file_path)
        with open(full, "rb") as fh:
            return fh.read(), meta, ver

    sub = f"versions/{int(version)}/download" if version is not None else "download"
    resp = _attune_http_request(
        "GET",
        f"/api/v1/artifacts/{int(artifact_id)}/{sub}",
        timeout=_KEYSTORE_TIMEOUT,
    )
    if resp.status_code >= 400:
        raise ConfigError(
            f"artifact_download_failed status={resp.status_code} body={resp.text[:300]}"
        )
    return resp.content, meta, ver


def read_artifact_records(
    *,
    ref: Optional[str] = None,
    artifact_id: Optional[int] = None,
    version: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Resolve an Attune artifact and return its content as a list of records.

    Supports artifacts whose content is one of:
      * CSV (``content_type`` starts with ``text/csv`` or path ends in ``.csv``)
      * JSON array of records (``application/json`` / ``.json``)
      * JSON Lines (``application/x-ndjson`` or ``.jsonl`` / ``.ndjson``)
      * In-DB JSON content (``content_json`` field on the version)

    Reads file-backed versions directly off the shared artifact volume —
    no HTTP download. Falls back to ``GET /artifacts/{id}/download`` only
    if the version has no ``file_path`` and no ``content_json``.
    """
    if ref is None and artifact_id is None:
        raise ConfigError("read_artifact_records: ref or artifact_id required")

    if artifact_id is None:
        meta = _get_artifact_metadata(ref=ref)
        artifact_id = int(meta["id"])

    ver = _get_artifact_version(int(artifact_id), version=version)
    file_path = ver.get("file_path")
    content_json = ver.get("content_json")
    content_type = (ver.get("content_type") or "").lower()

    # In-DB JSON path
    if content_json is not None:
        return _records_from_json_value(content_json)

    if file_path:
        full = os.path.join(artifacts_dir(), file_path)
        ext = os.path.splitext(file_path)[1].lower()
        if content_type.startswith("text/csv") or ext == ".csv":
            return _records_from_csv_path(full)
        if content_type in ("application/x-ndjson", "application/jsonl") or ext in (".jsonl", ".ndjson"):
            return _records_from_jsonl_path(full)
        if content_type.startswith("application/json") or ext == ".json":
            with open(full, "r", encoding="utf-8") as fh:
                return _records_from_json_value(json.load(fh))
        # Heuristic fallback: try CSV first, then JSON.
        try:
            return _records_from_csv_path(full)
        except Exception:
            with open(full, "r", encoding="utf-8") as fh:
                return _records_from_json_value(json.load(fh))

    # Last resort: download via API
    sub = f"versions/{int(version)}/download" if version is not None else "download"
    resp = _attune_http_request(
        "GET",
        f"/api/v1/artifacts/{int(artifact_id)}/{sub}",
        timeout=_KEYSTORE_TIMEOUT,
    )
    if resp.status_code >= 400:
        raise ConfigError(
            f"artifact_download_failed status={resp.status_code} body={resp.text[:300]}"
        )
    body = resp.text
    ct = resp.headers.get("content-type", "").lower()
    if ct.startswith("text/csv"):
        import csv
        import io as _io
        return [dict(r) for r in csv.DictReader(_io.StringIO(body))]
    if ct in ("application/x-ndjson", "application/jsonl"):
        return [json.loads(line) for line in body.splitlines() if line.strip()]
    return _records_from_json_value(json.loads(body))


def _records_from_json_value(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [r for r in value if isinstance(r, dict)]
    if isinstance(value, dict) and isinstance(value.get("records"), list):
        return [r for r in value["records"] if isinstance(r, dict)]
    raise ConfigError(
        "artifact_content_invalid: expected JSON array of records or "
        "object with `records` array"
    )


def _records_from_csv_path(path: str) -> List[Dict[str, Any]]:
    import csv
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            out.append(dict(row))
    return out


def _records_from_jsonl_path(path: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                out.append(obj)
    return out


# ---------------------------------------------------------------------------
# Session token cache (sf-toolkit token_refresh_callback target)
# ---------------------------------------------------------------------------


def _session_token_ref(connection_name: str) -> str:
    return f"{connection_name}_session_token"


def _pack_ref() -> str:
    return os.environ.get("ATTUNE_PACK_REF") or "salesforce"


def _max_token_age_seconds(action_params: Optional[Dict[str, Any]] = None) -> int:
    candidates: List[Any] = []
    if action_params is not None:
        candidates.append(action_params.get("session_token_max_age_seconds"))
    candidates.append(os.environ.get("SF_SESSION_TOKEN_MAX_AGE_SECONDS"))
    for candidate in candidates:
        if candidate in (None, ""):
            continue
        try:
            value = int(candidate)
            if value > 0:
                return value
        except (TypeError, ValueError):
            continue
    return DEFAULT_TOKEN_MAX_AGE_SECONDS


def _load_cached_token(
    connection_name: str,
    action_params: Optional[Dict[str, Any]] = None,
) -> Any:
    """Return a sf_toolkit.SalesforceToken or None if no fresh cache exists."""
    try:
        value = _fetch_keystore_value(_session_token_ref(connection_name))
    except ConfigError as exc:
        logger.debug("cached token lookup failed: %s", exc)
        return None
    if not isinstance(value, dict):
        return None
    issued_at = value.get("issued_at")
    max_age = _max_token_age_seconds(action_params)
    if isinstance(issued_at, (int, float)) and (time.time() - issued_at) > max_age:
        logger.info(
            "Discarding cached Salesforce session token for %s (older than %ss)",
            connection_name,
            max_age,
        )
        return None
    instance = value.get("instance")
    token = value.get("token")
    if not instance or not token:
        return None
    try:
        from sf_toolkit.auth import SalesforceToken  # type: ignore

        return SalesforceToken(instance=httpx.URL(instance), token=token)
    except Exception as exc:  # pragma: no cover — defensive against API drift
        logger.warning("Could not rehydrate cached SalesforceToken: %s", exc)
        return None


def _save_cached_token(connection_name: str, token: Any) -> None:
    instance = getattr(token, "instance", None)
    raw_token = getattr(token, "token", None)
    if not instance or not raw_token:
        logger.debug("save_cached_token: token missing fields, skipping")
        return
    payload = {
        "instance": str(instance),
        "token": str(raw_token),
        "issued_at": int(time.time()),
    }
    ref = _session_token_ref(connection_name)
    try:
        if not _put_keystore_value(ref, payload, encrypted=True):
            _post_keystore_key(
                ref,
                payload,
                name=f"Salesforce session token ({connection_name})",
                pack_ref=_pack_ref(),
                encrypted=True,
            )
    except ConfigError as exc:
        # Caching is best-effort; never fail the action because we couldn't
        # write the token back.
        logger.warning("Failed to persist Salesforce session token: %s", exc)


# ---------------------------------------------------------------------------
# Connection resolution
# ---------------------------------------------------------------------------


def _credentials_from_input(action_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return Salesforce credentials supplied directly in action/sensor input."""
    value = action_params.get("credentials")
    if value in (None, ""):
        login_kwargs = _filter_login_kwargs(action_params)
        return login_kwargs or None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"credentials_not_object: expected JSON object string: {exc}") from exc
    if not isinstance(value, dict):
        raise ConfigError(
            f"credentials_not_object: expected object, got {type(value).__name__}"
        )
    return value


def _direct_connection_name(creds: Dict[str, Any], action_params: Dict[str, Any]) -> str:
    explicit = action_params.get("connection_name")
    if explicit is not None:
        explicit_name = str(explicit).strip()
        if explicit_name:
            return explicit_name
    login_kwargs = _filter_login_kwargs(creds)
    fingerprint = json.dumps(login_kwargs, sort_keys=True, default=str)
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:16]
    return f"direct:{digest}"


def _connection_name(action_params: Dict[str, Any]) -> str:
    direct_creds = _credentials_from_input(action_params)
    if direct_creds is not None:
        name = _direct_connection_name(direct_creds, action_params)
    else:
        name = (
            action_params.get("credential_key") or os.environ.get("SF_CREDENTIAL_KEY") or ""
        )
    name = str(name).strip()
    if not name:
        raise ConfigError(
            "missing_salesforce_credentials: pass `credentials` (a Salesforce "
            "credential object), top-level Salesforce login fields, `credential_key` "
            "(a pack-scoped Attune keystore ref pointing at a JSON credential "
            "object), or set SF_CREDENTIAL_KEY"
        )
    return name


def _resolve_credentials(action_params: Dict[str, Any]) -> Tuple[str, Dict[str, Any], bool]:
    direct_creds = _credentials_from_input(action_params)
    if direct_creds is not None:
        return _direct_connection_name(direct_creds, action_params), direct_creds, False

    name = (
        action_params.get("credential_key") or os.environ.get("SF_CREDENTIAL_KEY") or ""
    )
    name = str(name).strip()
    if not name:
        raise ConfigError(
            "missing_salesforce_credentials: pass `credentials` (a Salesforce "
            "credential object), top-level Salesforce login fields, `credential_key`, "
            "or set SF_CREDENTIAL_KEY"
        )
    return name, _fetch_credential_from_keystore(name), True


def _api_version(action_params: Dict[str, Any]) -> str:
    return (
        action_params.get("api_version")
        or os.environ.get("SF_API_VERSION")
        or DEFAULT_API_VERSION
    )


def get_api_version(action_params: Dict[str, Any]) -> str:
    return _api_version(action_params)


def _filter_login_kwargs(creds: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in _LAZY_LOGIN_FIELDS:
        if k in creds and creds[k] not in (None, ""):
            out[k] = creds[k]
    # Common alias used in some Salesforce credential exports.
    if "client_id" in creds and "consumer_key" not in out:
        out["consumer_key"] = creds["client_id"]
    if "client_secret" in creds and "consumer_secret" not in out:
        out["consumer_secret"] = creds["client_secret"]
    # Coerce PEM string private keys into bytes (sf-toolkit / cryptography
    # accepts both, but bytes is safer across versions).
    pk = out.get("private_key")
    if isinstance(pk, str):
        out["private_key"] = pk.encode("utf-8")
    return out


def get_client(action_params: Dict[str, Any]) -> Any:
    """Return a registered sf_toolkit.SalesforceClient for this credential.

    Connection name is the keystore ref (``credential_key``), an explicit
    ``connection_name`` for direct credentials, or a stable hash of the direct
    credential fields. Resolution order on a cold call:
      1. Return existing in-process registration if one exists.
      2. Use direct credentials from input, or load the credential blob from
         keystore via ``credential_key``.
      3. For keystore-backed credentials, try to load a still-fresh session
         token from the keystore so sf-toolkit can skip the initial login.
      4. Build & register ``SalesforceClient(connection_name=..., login=...,
         token=cached_or_None, token_refresh_callback=save_to_keystore)``.

    sf-toolkit handles in-process token refresh and triggers our callback
    on every refresh; the callback persists only when the connection came from
    ``credential_key``.

    Returns an ``httpx.Client`` subclass — callers can use ``.get(url)``,
    ``.post(url, json=...)``, ``.put(url, content=...)``, etc. directly,
    with bearer auth injected by the toolkit.
    """
    name, creds, use_keystore_cache = _resolve_credentials(action_params)

    try:
        from sf_toolkit import SalesforceClient  # type: ignore
        from sf_toolkit.auth import lazy_login  # type: ignore
    except ImportError as exc:
        raise ConfigError(
            f"sf_toolkit_not_installed: install with `pip install sf-toolkit`: {exc}"
        ) from exc

    # 1) In-process cache (sf-toolkit's class-level connection registry)
    try:
        existing = SalesforceClient.get_connection(name)
    except Exception:
        existing = None
    if existing is not None:
        return existing

    # 2) Credentials
    login_kwargs = _filter_login_kwargs(creds)
    if not login_kwargs:
        raise ConfigError(
            f"empty_credential_object ref={name}: no recognised lazy_login fields "
            f"(expected one of {sorted(_LAZY_LOGIN_FIELDS)})"
        )

    # 3) Cached session token (best-effort, optional)
    cached_token = _load_cached_token(name, action_params) if use_keystore_cache else None

    # 4) Build the toolkit client
    def _refresh_callback(token: Any) -> None:
        if not use_keystore_cache:
            return
        try:
            _save_cached_token(name, token)
        except Exception as exc:  # never let caching break the request
            logger.warning("token_refresh_callback failed: %s", exc)

    api_version = _api_version(action_params)
    login_fn = lazy_login(**login_kwargs)

    # Try the full constructor first; degrade gracefully if the installed
    # toolkit version is older and rejects one of these kwargs.
    construction_attempts: List[Dict[str, Any]] = [
        {
            "connection_name": name,
            "login": login_fn,
            "token": cached_token,
            "token_refresh_callback": _refresh_callback,
            "version": api_version,
        },
        {
            "connection_name": name,
            "login": login_fn,
            "token": cached_token,
            "token_refresh_callback": _refresh_callback,
        },
        {
            "connection_name": name,
            "login": login_fn,
            "token_refresh_callback": _refresh_callback,
        },
        {"connection_name": name, "login": login_fn},
        {"login": login_fn},
    ]
    last_exc: Optional[Exception] = None
    for kwargs in construction_attempts:
        clean = {k: v for k, v in kwargs.items() if v is not None}
        try:
            return SalesforceClient(**clean)
        except TypeError as exc:
            last_exc = exc
            continue
    raise ConfigError(f"sf_toolkit_client_construction_failed: {last_exc}")


# ---------------------------------------------------------------------------
# Salesforce HTTP helpers — go through the SalesforceClient (httpx.Client)
# ---------------------------------------------------------------------------
#
# sf-toolkit's ``SalesforceAuth`` (an ``httpx.Auth``) handles three things
# automatically:
#
# * Lazy login on the first call (login() is invoked, token is captured).
# * Refreshing the token on 401 / "Bad_OAuth_Token" responses.
# * Rewriting **relative URLs** to absolute by prepending the token's
#   instance host. This means callers can pass a path like
#   ``"/services/data/v60.0/limits/"`` (or ``"/cometd/60.0"`` for the
#   streaming sensor) and the auth_flow injects the right host without any
#   extra plumbing on our side.
#
# So this module no longer needs ``_force_login``, ``_build_data_url``, or
# an ``instance_url`` helper — callers just hand relative URLs to the
# client.


def sf_request(
    action_params: Dict[str, Any],
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
    timeout: float = 60.0,
) -> Any:
    """Call the Salesforce REST API and return parsed JSON.

    ``path`` is interpreted relative to the org's data URL
    (``/services/data/{api_version}``). Pass an absolute URL (``http(s)://``
    or starting with ``/services/...``) to bypass the data-url prefix.

    Returns parsed JSON, ``None`` for 204/empty bodies, or ``str`` for
    non-JSON success bodies. Raises ``RuntimeError`` on >=400 status.
    """
    client = get_client(action_params)
    if path.startswith(("http://", "https://", "/")):
        url = path
    else:
        url = f"{client.data_url}/{path.lstrip('/')}"

    resp = client.request(
        method.upper(),
        url,
        params=params,
        json=json_body,
        headers={"Accept": "application/json"},
        timeout=timeout,
        # Don't let the toolkit raise on 4xx — surface a uniform error message.
        response_status_raise=False,
    )
    if resp.status_code == 204 or not resp.content:
        return None
    if resp.status_code >= 400:
        raise RuntimeError(
            f"salesforce_api_error status={resp.status_code} method={method} "
            f"path={path} body={resp.text[:500]}"
        )
    try:
        return resp.json()
    except (json.JSONDecodeError, ValueError):
        return resp.text


# ---------------------------------------------------------------------------
# Async variants — for sensors / long-running consumers that must not let
# one slow Salesforce call block other concurrent work.
# ---------------------------------------------------------------------------


def get_async_client(action_params: Dict[str, Any]) -> Any:
    """Return a registered ``sf_toolkit.AsyncSalesforceClient``.

    Uses the resolved base connection name suffixed with ``:async`` so it is
    registered separately from the sync client (the toolkit's class-level
    connection registry is shared across both client types). Token caching,
    refresh callbacks, and lazy-login work identically to ``get_client``.

    Returns an ``httpx.AsyncClient`` subclass — callers should ``await``
    its ``.get / .post / .request`` methods.
    """
    base_name, creds, use_keystore_cache = _resolve_credentials(action_params)
    async_name = f"{base_name}:async"

    try:
        from sf_toolkit import AsyncSalesforceClient  # type: ignore
        from sf_toolkit.auth import lazy_login  # type: ignore
    except ImportError as exc:
        raise ConfigError(
            f"sf_toolkit_not_installed: install with `pip install sf-toolkit`: {exc}"
        ) from exc

    try:
        existing = AsyncSalesforceClient.get_connection(async_name)
    except Exception:
        existing = None
    if existing is not None:
        return existing

    login_kwargs = _filter_login_kwargs(creds)
    if not login_kwargs:
        raise ConfigError(
            f"empty_credential_object ref={base_name}: no recognised lazy_login fields "
            f"(expected one of {sorted(_LAZY_LOGIN_FIELDS)})"
        )

    cached_token = _load_cached_token(base_name, action_params) if use_keystore_cache else None

    def _refresh_callback(token: Any) -> None:
        if not use_keystore_cache:
            return
        try:
            _save_cached_token(base_name, token)
        except Exception as exc:
            logger.warning("token_refresh_callback (async) failed: %s", exc)

    api_version = _api_version(action_params)
    login_fn = lazy_login(**login_kwargs)

    construction_attempts: List[Dict[str, Any]] = [
        {
            "connection_name": async_name,
            "login": login_fn,
            "token": cached_token,
            "token_refresh_callback": _refresh_callback,
            "api_version": api_version,
        },
        {
            "connection_name": async_name,
            "login": login_fn,
            "token": cached_token,
            "token_refresh_callback": _refresh_callback,
        },
        {
            "connection_name": async_name,
            "login": login_fn,
            "token_refresh_callback": _refresh_callback,
        },
        {"connection_name": async_name, "login": login_fn},
        {"login": login_fn},
    ]
    last_exc: Optional[Exception] = None
    for kwargs in construction_attempts:
        clean = {k: v for k, v in kwargs.items() if v is not None}
        try:
            return AsyncSalesforceClient(**clean)
        except TypeError as exc:
            last_exc = exc
            continue
    raise ConfigError(f"sf_toolkit_async_client_construction_failed: {last_exc}")


async def sf_request_async(
    action_params: Dict[str, Any],
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
    timeout: float = 60.0,
) -> Any:
    """Async counterpart of :func:`sf_request`.

    Uses the shared ``AsyncSalesforceClient`` so concurrent callers (e.g.
    multiple SOQL polling rules dispatched via ``asyncio.gather``) don't
    block each other. The toolkit's async ``request()`` always
    ``raise_for_status`` — we catch its ``SalesforceError`` and surface a
    uniform ``RuntimeError`` matching the sync function.
    """
    from sf_toolkit.exceptions import SalesforceError  # type: ignore

    client = get_async_client(action_params)
    if path.startswith(("http://", "https://", "/")):
        url = path
    else:
        url = f"{client.data_url}/{path.lstrip('/')}"

    try:
        resp = await client.request(
            method.upper(),
            url,
            params=params,
            json=json_body,
            headers={"Accept": "application/json"},
            timeout=timeout,
        )
    except SalesforceError as exc:
        # Re-raise as RuntimeError with a uniform message (matches sync path).
        body = ""
        sf_resp = getattr(exc, "response", None)
        if sf_resp is not None:
            try:
                body = sf_resp.text[:500]
            except Exception:
                body = ""
        status = getattr(sf_resp, "status_code", "?")
        raise RuntimeError(
            f"salesforce_api_error status={status} method={method} "
            f"path={path} body={body}"
        ) from exc

    if resp.status_code == 204 or not resp.content:
        return None
    try:
        return resp.json()
    except (json.JSONDecodeError, ValueError):
        return resp.text


async def close_async_client(action_params: Dict[str, Any]) -> None:
    """Best-effort: close the registered async client for this credential."""
    base_name = _connection_name(action_params)
    async_name = f"{base_name}:async"
    try:
        from sf_toolkit import AsyncSalesforceClient  # type: ignore
        existing = AsyncSalesforceClient.get_connection(async_name)
    except Exception:
        existing = None
    if existing is not None:
        try:
            await existing.aclose()
        except Exception as exc:  # noqa: BLE001
            logger.warning("close_async_client(%s) failed: %s", async_name, exc)
