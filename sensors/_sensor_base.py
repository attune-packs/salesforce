"""
Shared helper for standalone Python sensors in this pack.

Sensors are launched by the Attune sensor service as long-running
processes with these env vars:

* ``ATTUNE_API_URL``        — base URL of the API service
* ``ATTUNE_API_TOKEN``      — sensor JWT token
* ``ATTUNE_SENSOR_ID``      — sensor row id
* ``ATTUNE_SENSOR_REF``     — sensor ref (e.g. salesforce.soql_poll)
* ``ATTUNE_SENSOR_TRIGGERS``— JSON array ``[{id, ref, config}, ...]`` of
                              enabled rules' trigger instances

Each rule's ``config`` is the per-rule trigger params (e.g. SOQL query for
this sensor) merged with the sensor's own parameter defaults. Sensors
emit events by POSTing to ``/api/v1/events``.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional

# Make `from lib import sf_client` resolvable from any sensor entrypoint.
_PACK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PACK_ROOT not in sys.path:
    sys.path.insert(0, _PACK_ROOT)


def configure_logging() -> logging.Logger:
    level_name = os.environ.get("ATTUNE_LOG_LEVEL", "info").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
        force=True,
    )
    return logging.getLogger(os.environ.get("ATTUNE_SENSOR_REF", "salesforce.sensor"))


def load_trigger_instances() -> List[Dict[str, Any]]:
    raw = os.environ.get("ATTUNE_SENSOR_TRIGGERS", "[]")
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass
    return []


def install_shutdown_handler() -> Dict[str, bool]:
    """Return a mutable dict whose ``stop`` key flips to True on SIGTERM/SIGINT."""
    state = {"stop": False}

    def _handler(signum, _frame):  # noqa: ARG001
        state["stop"] = True

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
    return state


def post_event(
    trigger_ref: str,
    payload: Dict[str, Any],
    *,
    trigger_instance_id: Optional[str] = None,
    timeout: int = 30,
) -> bool:
    """POST an event to the API. Returns True on success."""
    import httpx  # lazy import — sensor virtualenv installs it (declared in requirements.txt)

    api_url = os.environ.get("ATTUNE_API_URL", "").rstrip("/")
    token = os.environ.get("ATTUNE_API_TOKEN", "")
    if not api_url or not token:
        logging.error("post_event: missing ATTUNE_API_URL or ATTUNE_API_TOKEN")
        return False

    body: Dict[str, Any] = {"trigger_ref": trigger_ref, "payload": payload}
    if trigger_instance_id:
        body["trigger_instance_id"] = trigger_instance_id

    try:
        resp = httpx.post(
            f"{api_url}/api/v1/events",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=float(timeout),
        )
    except Exception as exc:  # noqa: BLE001
        logging.warning("post_event: request failed: %s", exc)
        return False

    if resp.status_code >= 400:
        logging.warning(
            "post_event: API returned %s — %s",
            resp.status_code,
            resp.text[:300],
        )
        return False
    return True


def sleep_responsive(seconds: float, stop_state: Dict[str, bool], step: float = 1.0) -> None:
    """Sleep in ``step`` second increments so SIGTERM is observed promptly."""
    end = time.time() + seconds
    while not stop_state["stop"] and time.time() < end:
        time.sleep(min(step, max(0.0, end - time.time())))
