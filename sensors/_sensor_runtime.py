"""Runtime glue for Salesforce sensors.

The pack requires attune-sdk in managed runtimes. The local test environment
may not be able to install it, so this module provides a tiny compatibility
surface for imports and pure-helper tests while keeping production behavior on
the SDK lifecycle classes.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Dict, Optional

_PACK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PACK_ROOT not in sys.path:
    sys.path.insert(0, _PACK_ROOT)

try:
    import attune  # type: ignore
except ImportError:  # pragma: no cover - exercised by local Python 3.8 tests
    attune = None  # type: ignore

SDK_AVAILABLE = attune is not None

if SDK_AVAILABLE:
    Sensor = attune.Sensor
    RuleState = attune.RuleState
    run_sensor = attune.run_sensor
else:

    @dataclass
    class RuleState:
        rule_id: int
        rule_ref: str
        trigger_ref: str = ""
        trigger_params: Dict[str, Any] = field(default_factory=dict)
        enabled: bool = True

    class Sensor:
        """Small local fallback matching the attune-sdk Sensor surface we use."""

        def __init__(self) -> None:
            self.context = SimpleNamespace(
                sensor_ref=os.environ.get("ATTUNE_SENSOR_REF", "salesforce.sensor"),
                api_url=os.environ.get("ATTUNE_API_URL", "http://localhost:8080"),
                api_token=os.environ.get("ATTUNE_API_TOKEN", ""),
                mq_url=os.environ.get("ATTUNE_MQ_URL", ""),
            )
            self._shutdown_event = threading.Event()
            self._rules: Dict[int, RuleState] = {}
            self._rules_lock = threading.Lock()
            self._http_client: Any = None
            logging.basicConfig(
                level=getattr(logging, os.environ.get("ATTUNE_LOG_LEVEL", "info").upper(), logging.INFO),
                format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                stream=sys.stdout,
                force=True,
            )
            self.logger = logging.getLogger(self.context.sensor_ref)

        @property
        def is_shutting_down(self) -> bool:
            return self._shutdown_event.is_set()

        @property
        def rules(self) -> Dict[int, RuleState]:
            with self._rules_lock:
                return dict(self._rules)

        @property
        def http_client(self) -> Any:
            if self._http_client is None:
                import httpx

                self._http_client = httpx.Client(
                    base_url=self.context.api_url,
                    headers={
                        "Authorization": f"Bearer {self.context.api_token}",
                        "Content-Type": "application/json",
                    },
                    timeout=10,
                )
            return self._http_client

        def _handle_signal(self, signum: int, _frame: Any) -> None:
            self.logger.info("Received signal %s, shutting down", signum)
            self._shutdown_event.set()

        def _bootstrap_rules(self) -> None:
            raw = os.environ.get("ATTUNE_SENSOR_TRIGGERS", "[]")
            try:
                triggers = json.loads(raw)
            except json.JSONDecodeError:
                triggers = []
            if not isinstance(triggers, list):
                triggers = []
            for item in triggers:
                if not isinstance(item, dict):
                    continue
                rule_id = item.get("id") or item.get("rule_id")
                if rule_id is None:
                    continue
                rule = RuleState(
                    rule_id=int(rule_id),
                    rule_ref=item.get("ref", item.get("rule_ref", f"rule_{rule_id}")),
                    trigger_ref=item.get("trigger_ref", ""),
                    trigger_params=item.get("config", item.get("trigger_params", {})) or {},
                    enabled=True,
                )
                with self._rules_lock:
                    self._rules[rule.rule_id] = rule
                self.on_rule_created(rule)

        def _start_mq_consumer(self) -> Optional[threading.Thread]:
            return None

        def on_rule_created(self, rule: RuleState) -> None:  # noqa: ARG002
            return None

        def on_rule_enabled(self, rule: RuleState) -> None:  # noqa: ARG002
            return None

        def on_rule_disabled(self, rule: RuleState) -> None:  # noqa: ARG002
            return None

        def on_rule_deleted(self, rule: RuleState) -> None:  # noqa: ARG002
            return None

        def run(self) -> None:
            while not self.is_shutting_down:
                self._shutdown_event.wait(timeout=10)

        def cleanup(self) -> None:
            return None

        def _run_lifecycle(self) -> int:
            signal.signal(signal.SIGTERM, self._handle_signal)
            signal.signal(signal.SIGINT, self._handle_signal)
            try:
                self._bootstrap_rules()
                self._start_mq_consumer()
                self.run()
                return 0
            finally:
                self._shutdown_event.set()
                self.cleanup()
                if self._http_client is not None:
                    self._http_client.close()
                    self._http_client = None

    def run_sensor(sensor_class: type[Sensor]) -> None:
        sensor = sensor_class()
        sys.exit(sensor._run_lifecycle())


def emit_event(
    sensor: Optional[Sensor],
    trigger_ref: str,
    payload: Dict[str, Any],
    *,
    trigger_instance_id: Optional[str] = None,
    timeout: float = 30.0,
) -> bool:
    """Emit an Attune event while preserving this pack's trigger_instance_id."""
    body: Dict[str, Any] = {"trigger_ref": trigger_ref, "payload": payload}
    if trigger_instance_id:
        body["trigger_instance_id"] = trigger_instance_id

    try:
        if sensor is not None:
            resp = sensor.http_client.post(
                "/api/v1/events",
                json=body,
                timeout=float(timeout),
            )
        else:
            import httpx

            api_url = os.environ.get("ATTUNE_API_URL", "").rstrip("/")
            token = os.environ.get("ATTUNE_API_TOKEN", "")
            if not api_url or not token:
                logging.error("emit_event: missing ATTUNE_API_URL or ATTUNE_API_TOKEN")
                return False
            resp = httpx.post(
                f"{api_url}/api/v1/events",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=float(timeout),
            )
        resp.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001
        logger = getattr(sensor, "logger", logging.getLogger("salesforce.sensor"))
        logger.warning("emit_event: request failed: %s", exc)
        return False
