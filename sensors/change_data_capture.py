#!/usr/bin/env python3
"""
salesforce.change_data_capture — standalone Salesforce CDC sensor.

Connects to the Salesforce CometD/Bayeux Streaming endpoint
``/cometd/<api_version>`` for each enabled rule on this sensor's trigger,
subscribes to the configured CDC channel with a replay extension, and
forwards every event as a ``salesforce.change_event`` Attune event.

CometD long-polling is implemented manually on top of the
``SalesforceClient`` (an ``httpx.Client``) so bearer auth, automatic
token refresh, and connection pooling come for free. Each rule is
handled in a dedicated background thread; the main thread waits for
SIGTERM/SIGINT.

Replay state is persisted under ``$ATTUNE_SENSOR_STATE_DIR`` so the sensor
can resume from the last successfully-processed event id across restarts
(within Salesforce's 72h retention window).
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import traceback
from typing import Any, Dict, List, Optional

from _sensor_runtime import RuleState, Sensor, emit_event, run_sensor
from lib import sf_client

logger = logging.getLogger("salesforce.change_data_capture")


def _state_dir() -> str:
    return os.environ.get("ATTUNE_SENSOR_STATE_DIR") or "/tmp"


def _state_path(rule_id: int, channel: str) -> str:
    safe = channel.replace("/", "_").strip("_")
    return os.path.join(_state_dir(), f"sf_cdc_rule_{rule_id}_{safe}.json")


def _state_map_path(rule_id: int) -> str:
    return os.path.join(_state_dir(), f"sf_cdc_rule_{rule_id}_replay_map.json")


def _load_replay(path: str) -> Optional[int]:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
            v = data.get("replay_id")
            return int(v) if v is not None else None
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError):
        return None


def _save_replay(path: str, replay_id: int) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"replay_id": replay_id}, fh)
    os.replace(tmp, path)


def _load_replay_map(path: str) -> Dict[str, int]:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    replay_ids = data.get("replay_ids")
    if not isinstance(replay_ids, dict):
        return {}
    result: Dict[str, int] = {}
    for channel, replay_id in replay_ids.items():
        if isinstance(channel, str):
            try:
                result[channel] = int(replay_id)
            except (TypeError, ValueError):
                continue
    return result


def _save_replay_map(path: str, replay_ids: Dict[str, int]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"replay_ids": replay_ids}, fh)
    os.replace(tmp, path)


def _resolve_object_channel(obj_name: str) -> str:
    obj = obj_name.strip()
    if not obj:
        raise ValueError("empty object name")
    if obj in ("ChangeEvents", "*", "/data/ChangeEvents"):
        raise ValueError("use all_events=true for catch-all CDC subscription")
    if obj.startswith("/data/"):
        return obj
    if obj.endswith("__c"):
        return f"/data/{obj[:-3]}__ChangeEvent"
    return f"/data/{obj}ChangeEvent"


def _resolve_channels(cfg_in: Dict[str, Any]) -> List[str]:
    raw_channel = cfg_in.get("channel")
    has_channel = isinstance(raw_channel, str) and raw_channel.strip() != ""
    objects = cfg_in.get("objects")
    if isinstance(objects, list) and not objects:
        raise ValueError("invalid objects: provide at least one object API name")
    has_objects = isinstance(objects, list) and len(objects) > 0
    all_events = bool(cfg_in.get("all_events"))

    selected_modes = int(has_channel) + int(has_objects) + int(all_events)
    if selected_modes == 0:
        raise ValueError("missing selector: set exactly one of channel, objects, or all_events=true")
    if selected_modes > 1:
        raise ValueError("ambiguous selector: channel, objects, and all_events are mutually exclusive")

    if has_channel:
        return [raw_channel.strip()]
    if all_events:
        return ["/data/ChangeEvents"]

    assert isinstance(objects, list)
    channels = []
    for value in objects:
        if not isinstance(value, str):
            raise ValueError("invalid objects: every entry must be a string object API name")
        channels.append(_resolve_object_channel(value))

    deduped: List[str] = []
    seen = set()
    for channel in channels:
        if channel in seen:
            raise ValueError(f"duplicate object/channel resolves to {channel}")
        seen.add(channel)
        deduped.append(channel)
    return deduped


# ---------------------------------------------------------------------------
# Minimal CometD client — handshake / subscribe / connect long-poll loop
# ---------------------------------------------------------------------------


class CometDSession:
    """Bayeux/CometD long-poll client built on top of an httpx.Client.

    The client is the same ``SalesforceClient`` that talks to the REST API,
    so bearer auth, refresh callbacks, and HTTP keep-alive are all shared.
    The CometD endpoint is a *relative* path; the toolkit's auth_flow
    rewrites it to ``<instance>/cometd/<version>`` on first request.
    """

    def __init__(self, http_client: Any, *, default_api_version: str):
        self.client = http_client
        # The CometD endpoint sits OUTSIDE /services/data/, so use the
        # ApiVersion's numeric label (e.g. "60.0") rather than data_url.
        version = _cometd_version_label(http_client, default_api_version)
        self.endpoint = f"/cometd/{version}"
        self.client_id: Optional[str] = None
        self._msg_id = 0

    def _next_id(self) -> str:
        self._msg_id += 1
        return str(self._msg_id)

    def _send(self, messages: List[Dict[str, Any]], timeout: float = 120.0) -> List[Dict[str, Any]]:
        resp = self.client.post(
            self.endpoint,
            json=messages,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=timeout,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"cometd_http_{resp.status_code}: {resp.text[:300]}")
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError(f"cometd_invalid_json: {exc}: {resp.text[:200]}") from exc
        return data if isinstance(data, list) else [data]

    def handshake(self, replay_map: Optional[Dict[str, int]] = None) -> None:
        payload: Dict[str, Any] = {
            "channel": "/meta/handshake",
            "version": "1.0",
            "supportedConnectionTypes": ["long-polling"],
            "minimumVersion": "1.0",
            "id": self._next_id(),
        }
        if replay_map:
            payload["ext"] = {"replay": replay_map}
        replies = self._send([payload])
        for r in replies:
            if r.get("channel") == "/meta/handshake":
                if not r.get("successful"):
                    raise RuntimeError(f"cometd_handshake_failed: {r.get('error')}")
                self.client_id = r["clientId"]
                return
        raise RuntimeError("cometd_handshake_no_reply")

    def subscribe(self, channel: str, replay_id: int) -> None:
        if not self.client_id:
            raise RuntimeError("not_handshaked")
        replies = self._send([{
            "channel": "/meta/subscribe",
            "clientId": self.client_id,
            "subscription": channel,
            "ext": {"replay": {channel: replay_id}},
            "id": self._next_id(),
        }])
        for r in replies:
            if r.get("channel") == "/meta/subscribe" and not r.get("successful"):
                raise RuntimeError(f"cometd_subscribe_failed: {r.get('error')}")

    def connect(self) -> List[Dict[str, Any]]:
        if not self.client_id:
            raise RuntimeError("not_handshaked")
        return self._send([{
            "channel": "/meta/connect",
            "clientId": self.client_id,
            "connectionType": "long-polling",
            "id": self._next_id(),
        }])

    def disconnect(self) -> None:
        if not self.client_id:
            return
        try:
            self._send([{
                "channel": "/meta/disconnect",
                "clientId": self.client_id,
                "id": self._next_id(),
            }], timeout=10)
        except Exception:  # noqa: BLE001
            pass
        self.client_id = None


# ---------------------------------------------------------------------------
# Per-rule subscription thread
# ---------------------------------------------------------------------------


def _normalise_event(channel: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a CometD ``data`` payload into the salesforce.change_event shape."""
    payload = event_data.get("payload") or {}
    header = payload.get("ChangeEventHeader") or {}
    event_meta = event_data.get("event") or {}
    return {
        "change_type":      header.get("changeType"),
        "entity_name":      header.get("entityName"),
        "record_ids":       header.get("recordIds") or [],
        "changed_fields":   header.get("changedFields") or [],
        "commit_timestamp": header.get("commitTimestamp"),
        "commit_user":      header.get("commitUser"),
        "transaction_key":  header.get("transactionKey"),
        "sequence_number":  header.get("sequenceNumber"),
        "replay_id":        event_meta.get("replayId"),
        "channel":          channel,
        "payload":          payload,
    }


def _sleep_responsive(seconds: float, sensor: Sensor, stop_event: threading.Event, step: float = 1.0) -> None:
    end = time.time() + seconds
    while not sensor.is_shutting_down and not stop_event.is_set() and time.time() < end:
        time.sleep(min(step, max(0.0, end - time.time())))


def _normalise_api_version(version: str) -> str:
    v = str(version).strip()
    if v.lower().startswith("v"):
        return v[1:]
    return v


def _cometd_version_label(http_client: Any, fallback_api_version: str) -> str:
    api_version = getattr(http_client, "api_version", None)
    if api_version is not None:
        label = getattr(api_version, "label", None)
        if isinstance(label, str) and label.strip():
            return label.strip()
        if isinstance(api_version, str) and api_version.strip():
            return _normalise_api_version(api_version)
    # Some sf-toolkit versions assert when data_url is accessed before
    # login metadata is populated; treat that as a normal fallback path.
    try:
        data_url = getattr(http_client, "data_url", None)
    except AssertionError:
        data_url = None
    if isinstance(data_url, str):
        m = re.search(r"/services/data/v?([0-9]+(?:\.[0-9]+)?)", data_url)
        if m:
            return m.group(1)
    return _normalise_api_version(fallback_api_version)


def _run_rule(rule: RuleState, sensor: Sensor, stop_event: threading.Event) -> None:
    rule_id = int(rule.rule_id)
    cfg_in = rule.trigger_params or {}
    try:
        channels = _resolve_channels(cfg_in)
    except ValueError as exc:
        sensor.logger.warning("rule %s: invalid CDC selector config: %s — thread exiting", rule_id, exc)
        return

    # The rule config IS the action_params surface for sf_client.
    params = cfg_in

    replay_default = int(cfg_in.get("replay_id", -1))
    replay_by_channel = {channel: replay_default for channel in channels}
    replay_map_path = _state_map_path(rule_id)

    # Preferred multi-channel storage.
    replay_by_channel.update(_load_replay_map(replay_map_path))

    # Backward compatibility for existing single-channel rules.
    if len(channels) == 1:
        legacy_path = _state_path(rule_id, channels[0])
        legacy_replay = _load_replay(legacy_path)
        if legacy_replay is not None:
            replay_by_channel[channels[0]] = legacy_replay

    reconnect = int(cfg_in.get("reconnect_interval_seconds", 5))
    instance_id = f"rule_{rule_id}"

    while not sensor.is_shutting_down and not stop_event.is_set():
        cometd: Optional[CometDSession] = None
        try:
            http_client = sf_client.get_client(params)
            cometd = CometDSession(
                http_client,
                default_api_version=sf_client.get_api_version(params),
            )
            cometd.handshake(replay_by_channel)

            subscribed_channels: List[str] = []
            for channel in channels:
                try:
                    cometd.subscribe(channel, replay_by_channel[channel])
                    subscribed_channels.append(channel)
                except Exception as exc:  # noqa: BLE001
                    sensor.logger.warning(
                        "rule %s subscribe failed for %s: %s", rule_id, channel, exc
                    )

            if not subscribed_channels:
                raise RuntimeError("cometd_subscribe_failed_all_channels")

            subscribed_set = set(subscribed_channels)
            sensor.logger.info(
                "rule %s subscribed to %s", rule_id, ", ".join(subscribed_channels),
            )

            while not sensor.is_shutting_down and not stop_event.is_set():
                replies = cometd.connect()
                for msg in replies:
                    ch = msg.get("channel")
                    if isinstance(ch, str) and ch in subscribed_set and msg.get("data"):
                        normalised = _normalise_event(ch, msg["data"])
                        if emit_event(
                            sensor,
                            "salesforce.change_event",
                            normalised,
                            trigger_instance_id=instance_id,
                        ):
                            rid = normalised.get("replay_id")
                            if isinstance(rid, int):
                                replay_by_channel[ch] = rid
                                _save_replay_map(replay_map_path, replay_by_channel)
                                if len(channels) == 1:
                                    _save_replay(_state_path(rule_id, ch), rid)
                    elif ch == "/meta/connect" and not msg.get("successful"):
                        # advice may instruct re-handshake
                        advice = msg.get("advice") or {}
                        if advice.get("reconnect") == "handshake":
                            raise RuntimeError(f"cometd_rehandshake_required: {msg.get('error')}")
                        sensor.logger.warning("rule %s connect unsuccessful: %s", rule_id, msg.get("error"))
                        time.sleep(1)

        except Exception as exc:  # noqa: BLE001
            sensor.logger.warning(
                "rule %s CDC loop error: %s: %r traceback=%s — reconnecting in %ss",
                rule_id,
                type(exc).__name__,
                exc,
                traceback.format_exc(limit=8).strip(),
                reconnect,
            )
        finally:
            if cometd:
                cometd.disconnect()

        if sensor.is_shutting_down or stop_event.is_set():
            break
        _sleep_responsive(reconnect, sensor, stop_event, step=1.0)

    sensor.logger.info("rule %s CDC thread stopped", rule_id)


class ChangeDataCaptureSensor(Sensor):
    """SDK-managed sensor that keeps one CometD worker thread per rule."""

    def __init__(self) -> None:
        super().__init__()
        self._rule_threads: Dict[int, threading.Thread] = {}
        self._rule_stops: Dict[int, threading.Event] = {}
        self._threads_lock = threading.Lock()

    def _start_rule_thread(self, rule: RuleState) -> None:
        self._stop_rule_thread(rule.rule_id)
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_run_rule,
            args=(rule, self, stop_event),
            name=f"cdc-rule-{rule.rule_id}",
            daemon=True,
        )
        with self._threads_lock:
            self._rule_stops[rule.rule_id] = stop_event
            self._rule_threads[rule.rule_id] = thread
        thread.start()

    def _stop_rule_thread(self, rule_id: int) -> None:
        with self._threads_lock:
            stop_event = self._rule_stops.pop(rule_id, None)
            thread = self._rule_threads.pop(rule_id, None)
        if stop_event is not None:
            stop_event.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)

    def on_rule_created(self, rule: RuleState) -> None:
        self._start_rule_thread(rule)

    def on_rule_enabled(self, rule: RuleState) -> None:
        self._start_rule_thread(rule)

    def on_rule_disabled(self, rule: RuleState) -> None:
        self._stop_rule_thread(rule.rule_id)

    def on_rule_deleted(self, rule: RuleState) -> None:
        self._stop_rule_thread(rule.rule_id)

    def on_rule_updated(self, rule: RuleState, old_params: Dict[str, Any]) -> None:  # noqa: ARG002
        self._start_rule_thread(rule)

    def run(self) -> None:
        self.logger.info("%s starting with %d rule(s)", self.context.sensor_ref, len(self.rules))

        if not self._rule_threads and not os.environ.get("ATTUNE_MQ_URL"):
            self.logger.info("%s: no enabled rules — exiting", self.context.sensor_ref)
            return

        while not self.is_shutting_down:
            with self._threads_lock:
                threads = list(self._rule_threads.values())
            if threads and not any(t.is_alive() for t in threads):
                self.logger.warning("all CDC worker threads have exited — sensor terminating")
                break
            time.sleep(1.0)

        self.logger.info("%s stopped cleanly", self.context.sensor_ref)

    def cleanup(self) -> None:
        with self._threads_lock:
            rule_ids = list(self._rule_threads)
        for rule_id in rule_ids:
            self._stop_rule_thread(rule_id)


def main() -> None:
    run_sensor(ChangeDataCaptureSensor)


if __name__ == "__main__":
    main()
