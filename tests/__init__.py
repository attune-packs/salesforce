"""
Lightweight unit tests for the salesforce pack.

These tests exercise the pure helpers in ``lib/sf_client.py`` and the
sensor logic without making real HTTP calls. They are intended to run
with ``pytest`` from the pack root::

    pip install -r requirements.txt pytest responses
    PYTHONPATH=. pytest tests/

Tests that depend on a live Salesforce org are intentionally omitted —
this pack is meant to be exercised against a sandbox via the pack-test
framework once integration credentials are available.
"""
