"""Tests for the soql_poll sensor's pure helpers."""

import asyncio
import os
import sys

# Add pack root + sensors dir so the sensor module is importable.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "sensors"))

import soql_poll  # noqa: E402
from _sensor_runtime import RuleState  # noqa: E402


def test_format_cursor_iso_passthrough():
    assert soql_poll._format_cursor("2024-01-01T00:00:00Z") == "2024-01-01T00:00:00Z"


def test_format_cursor_quotes_strings():
    assert soql_poll._format_cursor("hello") == "'hello'"
    assert soql_poll._format_cursor("o'brien") == "'o\\'brien'"


def test_format_cursor_numeric_passthrough():
    assert soql_poll._format_cursor("42") == "42"
    assert soql_poll._format_cursor(7) == "7"


def test_infer_sobject():
    assert soql_poll._infer_sobject("SELECT Id FROM Account WHERE x>1") == "Account"
    assert soql_poll._infer_sobject("select id from My_Custom__c") == "My_Custom__c"


def test_max_cursor_advances():
    records = [
        {"SystemModstamp": "2024-01-01T00:00:00Z"},
        {"SystemModstamp": "2024-02-01T00:00:00Z"},
        {"SystemModstamp": "2024-01-15T00:00:00Z"},
    ]
    assert soql_poll._max_cursor(records, "SystemModstamp", None) == "2024-02-01T00:00:00Z"
    assert (
        soql_poll._max_cursor(records, "SystemModstamp", "2024-03-01T00:00:00Z")
        == "2024-03-01T00:00:00Z"
    )


def test_build_soql_template_minimal():
    soql = soql_poll._build_soql_template({"sobject": "User"})
    assert soql == (
        "SELECT Id, SystemModstamp FROM User "
        "WHERE SystemModstamp > :cursor "
        "ORDER BY SystemModstamp ASC"
    )


def test_build_soql_template_with_fields_and_where():
    soql = soql_poll._build_soql_template(
        {
            "sobject": "GroupMember",
            "fields": ["Id", "GroupId", "UserOrGroupId"],
            "where": "Group.Type = 'Regular'",
            "limit": 200,
        }
    )
    # cursor_field auto-appended; user where is wrapped in parens and ANDed
    assert "FROM GroupMember" in soql
    assert "GroupId" in soql and "UserOrGroupId" in soql
    assert "(Group.Type = 'Regular') AND SystemModstamp > :cursor" in soql
    assert soql.endswith("LIMIT 200")
    assert "ORDER BY SystemModstamp ASC" in soql


def test_build_soql_template_custom_cursor_and_order():
    soql = soql_poll._build_soql_template(
        {
            "sobject": "Account",
            "cursor_field": "LastModifiedDate",
            "fields": "Id, Name",
            "order_by": "Name ASC",
        }
    )
    assert "SELECT Id, Name, LastModifiedDate FROM Account" in soql
    assert "WHERE LastModifiedDate > :cursor" in soql
    assert "ORDER BY Name ASC" in soql


def test_build_soql_template_no_sobject_returns_none():
    assert soql_poll._build_soql_template({}) is None
    assert soql_poll._build_soql_template({"sobject": "  "}) is None


def test_resolve_soql_template_prefers_explicit():
    explicit = "SELECT Id FROM Foo WHERE Id = '1'"
    assert soql_poll._resolve_soql_template({"soql": explicit, "sobject": "Bar"}) == explicit


def test_resolve_soql_template_falls_back_to_structured():
    soql = soql_poll._resolve_soql_template({"sobject": "Contact"})
    assert soql is not None and "FROM Contact" in soql


def test_build_soql_template_with_lookback_adds_ceiling():
    soql = soql_poll._build_soql_template(
        {"sobject": "Account", "lookback_seconds": 10}
    )
    assert "SystemModstamp > :cursor AND SystemModstamp <= :ceiling" in soql


def test_build_soql_template_lookback_zero_omits_ceiling():
    soql = soql_poll._build_soql_template(
        {"sobject": "Account", "lookback_seconds": 0}
    )
    assert ":ceiling" not in soql
    assert "SystemModstamp > :cursor" in soql


def test_build_soql_template_lookback_with_where():
    soql = soql_poll._build_soql_template(
        {
            "sobject": "Lead",
            "where": "Status != 'Closed'",
            "lookback_seconds": 30,
            "cursor_field": "LastModifiedDate",
        }
    )
    # User where wrapped in parens, ANDed with both cursor predicates
    assert (
        "(Status != 'Closed') AND LastModifiedDate > :cursor AND LastModifiedDate <= :ceiling"
        in soql
    )


def test_compute_ceiling_subtracts_lookback():
    import datetime as dt

    now = dt.datetime(2026, 4, 30, 12, 0, 30)
    assert soql_poll._compute_ceiling(10, now=now) == "2026-04-30T12:00:20Z"


def test_compute_ceiling_negative_clamped_to_zero():
    import datetime as dt

    now = dt.datetime(2026, 4, 30, 12, 0, 30)
    # Negative lookback shouldn't push the ceiling into the future
    assert soql_poll._compute_ceiling(-5, now=now) == "2026-04-30T12:00:30Z"


def test_rule_interval_clamps_minimum():
    assert soql_poll._rule_interval({"config": {"poll_interval_seconds": 0}}) == 1.0
    assert soql_poll._rule_interval({"config": {"poll_interval_seconds": -10}}) == 1.0
    assert soql_poll._rule_interval({"config": {}}) == soql_poll.DEFAULT_POLL_INTERVAL
    assert soql_poll._rule_interval({"config": {"poll_interval_seconds": 30}}) == 30.0
    # Legacy alias
    assert soql_poll._rule_interval({"config": {"poll_interval": 45}}) == 45.0


def test_rule_interval_invalid_falls_back_to_default():
    assert (
        soql_poll._rule_interval({"config": {"poll_interval_seconds": "nope"}})
        == soql_poll.DEFAULT_POLL_INTERVAL
    )


def test_rule_interval_accepts_sdk_rule_state():
    rule = RuleState(
        rule_id=123,
        rule_ref="rule.ref",
        trigger_ref="salesforce.soql_record",
        trigger_params={"poll_interval_seconds": 90},
    )
    assert soql_poll._rule_interval(rule) == 90.0


def test_rule_config_accepts_sdk_rule_state():
    rule = RuleState(
        rule_id=123,
        rule_ref="rule.ref",
        trigger_ref="salesforce.soql_record",
        trigger_params={"sobject": "Account"},
    )
    assert soql_poll._rule_id(rule) == 123
    assert soql_poll._rule_config(rule) == {"sobject": "Account"}


def test_transient_error_types_include_httpx_and_os():
    types = soql_poll.TRANSIENT_ERROR_TYPES
    assert ConnectionError in types
    assert OSError in types
    # asyncio.TimeoutError is aliased to TimeoutError in 3.11+; tolerate both
    import asyncio as _aio

    assert _aio.TimeoutError in types or TimeoutError in types


def test_run_rule_with_backoff_uses_exponential_delay(monkeypatch):
    async def fake_process(*_args, **_kwargs):
        raise TimeoutError("temporary")

    monkeypatch.setattr(soql_poll, "_process_one_rule_async", fake_process)
    monkeypatch.setattr(soql_poll.time, "monotonic", lambda: 1000.0)
    rule = RuleState(
        rule_id=123,
        rule_ref="rule.ref",
        trigger_ref="salesforce.soql_record",
        trigger_params={"poll_interval_seconds": 60, "sobject": "Account"},
    )
    state = {"next_due_at": 1000.0, "consecutive_failures": 0}

    asyncio.run(soql_poll._run_rule_with_backoff(rule, state))
    assert state == {"next_due_at": 1060.0, "consecutive_failures": 1}

    asyncio.run(soql_poll._run_rule_with_backoff(rule, state))
    assert state == {"next_due_at": 1120.0, "consecutive_failures": 2}
