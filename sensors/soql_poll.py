#!/usr/bin/env python3
"""
salesforce.soql_poll — standalone sensor process.

For each enabled rule on this sensor's trigger, runs the rule's SOQL
periodically, advances a watermark (``cursor_field``), and POSTs new
records back to the Attune API as either ``salesforce.soql_record``
(per-row) or ``salesforce.soql_batch`` (per-tick) events.

The tick loop runs each rule concurrently via ``asyncio.gather`` so a
slow Salesforce query for one rule cannot block the others. Per-rule
queries can be capped with ``query_timeout_seconds`` (default 300s).
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import hashlib
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Tuple

import _sensor_base
from lib import sf_client

logger = _sensor_base.configure_logging()

DEFAULT_QUERY_TIMEOUT = 300.0
DEFAULT_POLL_INTERVAL = 60.0
DEFAULT_LOOKBACK_SECONDS = 0.0
# Backoff caps for transient failures on a long-lived sensor process.
MAX_BACKOFF_MULTIPLIER = 10.0
BACKOFF_BASE = 2.0
# Errors we treat as transient — they trigger backoff but never crash the loop.
TRANSIENT_ERROR_TYPES: Tuple[type, ...] = (
    ConnectionError,
    OSError,
    asyncio.TimeoutError,
    TimeoutError,
)
try:  # httpx is a hard dep, but be defensive in case import order shifts.
    import httpx as _httpx  # type: ignore

    TRANSIENT_ERROR_TYPES = TRANSIENT_ERROR_TYPES + (
        _httpx.RequestError,
        _httpx.HTTPError,
    )
except Exception:  # noqa: BLE001
    pass


def _state_dir() -> str:
    return os.environ.get("ATTUNE_SENSOR_STATE_DIR") or "/tmp"


def _state_path(rule_id: int, soql: str) -> str:
    h = hashlib.sha256(soql.encode("utf-8")).hexdigest()[:12]
    return os.path.join(_state_dir(), f"sf_soql_poll_rule_{rule_id}_{h}.json")


def _load_state(path: str) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh)
    os.replace(tmp, path)


def _format_cursor(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        if re.fullmatch(r"-?\d+(?:\.\d+)?", value):
            return value
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)?", value):
            return value
        return "'" + value.replace("'", "\\'") + "'"
    return str(value)


def _infer_sobject(soql: str) -> str | None:
    m = re.search(r"\bFROM\s+([A-Za-z0-9_]+)", soql, flags=re.IGNORECASE)
    return m.group(1) if m else None


def _max_cursor(records: List[Dict[str, Any]], field: str, current: Any) -> Any:
    best = current
    for r in records:
        v = r.get(field)
        if v is None:
            continue
        if best is None or str(v) > str(best):
            best = v
    return best


def _format_iso_z(dt: _dt.datetime) -> str:
    """Format a UTC datetime as Salesforce-friendly ``YYYY-MM-DDTHH:MM:SSZ``."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _compute_ceiling(lookback_seconds: float, now: _dt.datetime | None = None) -> str:
    """Return the ISO-Z upper bound for a lookback window.

    The window is ``cursor > old_cursor AND cursor <= now - lookback``. Set
    ``lookback_seconds = 0`` to disable the upper bound (legacy behavior).
    """
    base = now or _dt.datetime.utcnow()
    return _format_iso_z(base - _dt.timedelta(seconds=max(0.0, float(lookback_seconds))))


def _build_soql_template(cfg: Dict[str, Any]) -> str | None:
    """Compose a SOQL template from structured rule config.

    Accepted keys (any subset; ``sobject`` is required):
      - ``sobject`` (str, required): API name of the object to query.
      - ``fields`` (list[str] | str): SELECT list. Defaults to
        ``["Id", cursor_field]`` (deduped). May also be a comma-separated
        string. The cursor field is always included automatically.
      - ``where`` (str): SOQL fragment without the leading ``WHERE``.
        Combined with the auto-injected cursor predicates via ``AND``.
      - ``order_by`` (str): ORDER BY clause without the leading keyword.
        Defaults to ``{cursor_field} ASC``.
      - ``limit`` (int): optional LIMIT.
      - ``lookback_seconds`` (number, default 0): when > 0, an upper-bound
        predicate ``{cursor_field} <= :ceiling`` is added so rows committed
        within the lookback window aren't read until they've had a chance
        to settle (handles the gap between Salesforce commit and SOQL
        visibility / clock skew).

    Returns the SOQL string with ``:cursor`` and (when applicable)
    ``:ceiling`` placeholders intact — the caller substitutes them.
    Returns ``None`` if no ``sobject`` was supplied.
    """
    sobject = cfg.get("sobject")
    if not sobject:
        return None
    sobject = str(sobject).strip()
    if not sobject:
        return None

    cursor_field = cfg.get("cursor_field") or "SystemModstamp"
    id_field = cfg.get("id_field") or "Id"

    raw_fields = cfg.get("fields")
    if raw_fields is None:
        fields_list: List[str] = [id_field, cursor_field]
    elif isinstance(raw_fields, str):
        fields_list = [f.strip() for f in raw_fields.split(",") if f.strip()]
    else:
        fields_list = [str(f).strip() for f in raw_fields if str(f).strip()]

    lower = {f.lower() for f in fields_list}
    if cursor_field.lower() not in lower:
        fields_list.append(cursor_field)
    if id_field.lower() not in {f.lower() for f in fields_list}:
        fields_list.insert(0, id_field)

    select_clause = ", ".join(fields_list)

    where_extra = (cfg.get("where") or "").strip()
    cursor_predicate = f"{cursor_field} > :cursor"

    try:
        lookback = float(cfg.get("lookback_seconds") or 0)
    except (TypeError, ValueError):
        lookback = 0.0
    if lookback > 0:
        cursor_predicate = f"{cursor_predicate} AND {cursor_field} <= :ceiling"

    if where_extra:
        where_clause = f"({where_extra}) AND {cursor_predicate}"
    else:
        where_clause = cursor_predicate

    order_by = (cfg.get("order_by") or f"{cursor_field} ASC").strip()

    parts = [
        f"SELECT {select_clause}",
        f"FROM {sobject}",
        f"WHERE {where_clause}",
        f"ORDER BY {order_by}",
    ]

    limit = cfg.get("limit")
    if limit is not None:
        try:
            parts.append(f"LIMIT {int(limit)}")
        except (TypeError, ValueError):
            pass

    return " ".join(parts)


def _resolve_soql_template(cfg: Dict[str, Any]) -> str | None:
    """Pick an explicit ``soql`` template, else build one from structured cfg."""
    explicit = cfg.get("soql")
    if explicit:
        return str(explicit)
    return _build_soql_template(cfg)


async def _run_query_async(
    params: Dict[str, Any], soql: str, query_all: bool
) -> List[Dict[str, Any]]:
    endpoint = "queryAll" if query_all else "query"
    body = await sf_client.sf_request_async(params, "GET", endpoint, params={"q": soql})
    out: List[Dict[str, Any]] = list(body.get("records", []))
    next_url = body.get("nextRecordsUrl")
    while next_url:
        body = await sf_client.sf_request_async(params, "GET", next_url)
        out.extend(body.get("records", []))
        next_url = body.get("nextRecordsUrl")
    return out


async def _process_one_rule_async(rule: Dict[str, Any]) -> Tuple[int, int]:
    """Run one tick for a single rule. Returns ``(events_emitted, errors)``.

    All Salesforce I/O is awaited; ``post_event`` (sync ``httpx.post``) is
    dispatched via ``asyncio.to_thread`` so concurrent rules don't serialise
    on event delivery either.
    """
    rule_id = int(rule.get("id", 0))
    cfg_in = rule.get("config") or {}
    soql_template = _resolve_soql_template(cfg_in)
    if not soql_template:
        logger.warning(
            "rule %s: missing 'soql' (or 'sobject' for structured query) in trigger config — skipping",
            rule_id,
        )
        return (0, 1)

    params = cfg_in

    cursor_field = cfg_in.get("cursor_field") or "SystemModstamp"
    id_field = cfg_in.get("id_field") or "Id"
    mode = cfg_in.get("mode") or "per_record"
    query_all = bool(cfg_in.get("query_all", False))
    query_timeout = float(cfg_in.get("query_timeout_seconds") or DEFAULT_QUERY_TIMEOUT)

    state_path = _state_path(rule_id, soql_template)
    state = _load_state(state_path)
    cursor = state.get("cursor")
    if cursor is None:
        cursor = cfg_in.get("cursor_initial") or _format_iso_z(_dt.datetime.utcnow())
    seen_ids = set(state.get("last_tick_ids") or [])

    try:
        lookback = float(cfg_in.get("lookback_seconds") or 0)
    except (TypeError, ValueError):
        lookback = 0.0
    ceiling: str | None = _compute_ceiling(lookback) if lookback > 0 else None

    soql = soql_template.replace(":cursor", _format_cursor(cursor))
    if ceiling is not None:
        soql = soql.replace(":ceiling", _format_cursor(ceiling))

    try:
        records = await asyncio.wait_for(
            _run_query_async(params, soql, query_all),
            timeout=query_timeout,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "rule %s: query exceeded query_timeout_seconds=%.1f — skipping tick",
            rule_id,
            query_timeout,
        )
        raise
    except TRANSIENT_ERROR_TYPES as exc:
        logger.warning("rule %s: transient query error (will back off): %s", rule_id, exc)
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("rule %s: query failed: %s", rule_id, exc)
        return (0, 1)

    fresh = [r for r in records if r.get(id_field) not in seen_ids]
    seen_max = _max_cursor(fresh, cursor_field, cursor)
    # When a lookback ceiling was applied, advance the watermark to the
    # ceiling even if we saw no records — the window has been safely
    # scanned, so the next tick can start strictly after it.
    if ceiling is not None and (seen_max is None or str(seen_max) < str(ceiling)):
        new_cursor: Any = ceiling
        new_ids: List[Any] = []
    else:
        new_cursor = seen_max
        new_ids = [
            r.get(id_field)
            for r in fresh
            if str(r.get(cursor_field)) == str(new_cursor)
        ]
    _save_state(state_path, {"cursor": new_cursor, "last_tick_ids": new_ids})

    if not fresh:
        return (0, 0)

    sobject = _infer_sobject(soql_template)
    instance_id = f"rule_{rule_id}"

    if mode == "batch":
        ok = await asyncio.to_thread(
            _sensor_base.post_event,
            "salesforce.soql_batch",
            {
                "sobject": sobject,
                "records": fresh,
                "count": len(fresh),
                "query": soql,
                "cursor_value": str(new_cursor) if new_cursor is not None else None,
            },
            trigger_instance_id=instance_id,
        )
        return (int(bool(ok)), 0)

    # per_record mode — fan event POSTs out concurrently.
    coros = [
        asyncio.to_thread(
            _sensor_base.post_event,
            "salesforce.soql_record",
            {
                "sobject": sobject,
                "record": rec,
                "cursor_value": str(rec.get(cursor_field))
                if rec.get(cursor_field) is not None
                else None,
                "query": soql,
            },
            trigger_instance_id=instance_id,
        )
        for rec in fresh
    ]
    results = await asyncio.gather(*coros, return_exceptions=True)
    emitted = 0
    errors = 0
    for r in results:
        if isinstance(r, Exception):
            errors += 1
            logger.warning("rule %s: post_event failed: %s", rule_id, r)
        elif r:
            emitted += 1
    return (emitted, errors)


# Backwards-compatible sync wrapper kept for any external imports / tests.
def _process_one_rule(rule: Dict[str, Any]) -> Tuple[int, int]:
    return asyncio.run(_process_one_rule_async(rule))


async def _async_sleep_responsive(seconds: float, stop: Dict[str, Any], step: float = 1.0) -> None:
    """``asyncio``-friendly counterpart of ``_sensor_base.sleep_responsive``."""
    remaining = max(0.0, float(seconds))
    while remaining > 0 and not stop.get("stop"):
        chunk = min(step, remaining)
        await asyncio.sleep(chunk)
        remaining -= chunk


def _rule_interval(rule: Dict[str, Any]) -> float:
    cfg = rule.get("config") or {}
    raw = cfg.get("poll_interval_seconds")
    if raw is None:
        raw = cfg.get("poll_interval")
    if raw is None:
        raw = DEFAULT_POLL_INTERVAL
    try:
        val = float(raw)
    except (TypeError, ValueError):
        val = DEFAULT_POLL_INTERVAL
    return max(1.0, val)


async def _run_rule_with_backoff(
    rule: Dict[str, Any], rule_state: Dict[str, Any]
) -> None:
    """Run one tick for a rule and update its ``rule_state`` book-keeping.

    On a transient failure (network/timeout), the rule's ``next_due_at``
    is pushed out by an exponential-backoff multiplier of its configured
    poll interval (capped at ``MAX_BACKOFF_MULTIPLIER``). On a clean tick
    the failure counter resets.
    """
    rid = rule.get("id")
    interval = _rule_interval(rule)
    try:
        emitted, errors = await _process_one_rule_async(rule)
    except TRANSIENT_ERROR_TYPES as exc:
        rule_state["consecutive_failures"] = rule_state.get("consecutive_failures", 0) + 1
        n = rule_state["consecutive_failures"]
        multiplier = min(MAX_BACKOFF_MULTIPLIER, BACKOFF_BASE ** (n - 1))
        delay = interval * multiplier
        rule_state["next_due_at"] = time.monotonic() + delay
        logger.warning(
            "rule %s: transient failure #%d (%s) — next attempt in %.1fs",
            rid,
            n,
            type(exc).__name__,
            delay,
        )
        return
    except Exception as exc:  # noqa: BLE001
        # Programming / config errors — schedule normally, just log loudly.
        logger.exception("rule %s tick crashed: %s", rid, exc)
        rule_state["next_due_at"] = time.monotonic() + interval
        return

    rule_state["consecutive_failures"] = 0
    rule_state["next_due_at"] = time.monotonic() + interval
    if emitted:
        logger.info("rule %s emitted %d event(s)", rid, emitted)
    if errors:
        logger.warning("rule %s tick had %d error(s)", rid, errors)


async def _async_main() -> None:
    sensor_ref = os.environ.get("ATTUNE_SENSOR_REF", "salesforce.soql_poll")
    rules = _sensor_base.load_trigger_instances()
    logger.info("%s starting with %d rule(s)", sensor_ref, len(rules))

    if not rules:
        logger.info("%s: no enabled rules — exiting", sensor_ref)
        return

    stop = _sensor_base.install_shutdown_handler()

    # Per-rule scheduling: each rule has its own next-due time so a
    # 5-minute rule isn't dragged into a 60-second sibling's cadence.
    schedule: Dict[Any, Dict[str, Any]] = {
        rule.get("id"): {"next_due_at": time.monotonic(), "consecutive_failures": 0}
        for rule in rules
    }

    try:
        while not stop["stop"]:
            now = time.monotonic()
            due_rules = [
                rule for rule in rules
                if schedule[rule.get("id")]["next_due_at"] <= now
            ]

            if due_rules:
                try:
                    await asyncio.gather(
                        *[
                            _run_rule_with_backoff(rule, schedule[rule.get("id")])
                            for rule in due_rules
                        ],
                        return_exceptions=False,
                    )
                except Exception as exc:  # noqa: BLE001
                    # _run_rule_with_backoff swallows everything, but be
                    # paranoid: this loop is supposed to live forever.
                    logger.exception("scheduler tick crashed: %s", exc)

            # Sleep until the next rule is due (or 1s, whichever is sooner)
            # so shutdown signals are honored quickly.
            now = time.monotonic()
            next_due = min(s["next_due_at"] for s in schedule.values())
            sleep_for = max(0.0, min(next_due - now, 60.0))
            if sleep_for <= 0:
                # Yield to event loop to avoid a hot-spin if a rule's
                # interval is misconfigured to <= 0.
                await asyncio.sleep(0.05)
            else:
                await _async_sleep_responsive(sleep_for, stop, step=1.0)
    finally:
        # Best-effort: close any registered AsyncSalesforceClients so the
        # process exits cleanly without dangling httpx connections.
        seen: set[str] = set()
        for rule in rules:
            cfg = rule.get("config") or {}
            try:
                key = sf_client._connection_name(cfg)
            except Exception:
                continue
            if key in seen:
                continue
            seen.add(key)
            try:
                await sf_client.close_async_client(cfg)
            except Exception as exc:  # noqa: BLE001
                logger.debug("close_async_client(%s) failed: %s", key, exc)
        logger.info("%s stopped cleanly", sensor_ref)


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
