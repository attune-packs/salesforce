"""Tests for sf_client.py — the thin Attune↔sf-toolkit adapter.

Most logic now lives in sf-toolkit itself (connection registry, token
refresh). These tests cover the Attune-specific glue: keystore lookup,
session-token caching, and the kwargs we forward to lazy_login.
"""

import os
import sys
import time
import types
from unittest.mock import MagicMock

import pytest
import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import sf_client  # noqa: E402


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def test_to_plain_handles_basic_types():
    assert sf_client.to_plain({"a": 1, "b": [1, 2, {"c": "x"}]}) == {
        "a": 1,
        "b": [1, 2, {"c": "x"}],
    }
    assert sf_client.to_plain(None) is None
    assert sf_client.to_plain("hello") == "hello"


def test_chunked_splits_evenly():
    assert list(sf_client.chunked(range(5), 2)) == [[0, 1], [2, 3], [4]]
    assert list(sf_client.chunked([], 3)) == []


def test_session_token_ref_format():
    assert sf_client._session_token_ref("acme") == "acme_session_token"
    assert sf_client._session_token_ref("foo_bar") == "foo_bar_session_token"


def test_filter_login_kwargs_strips_unknowns_and_aliases():
    out = sf_client._filter_login_kwargs({
        "username": "u",
        "client_id": "ck",          # alias → consumer_key
        "client_secret": "cs",      # alias → consumer_secret
        "private_key": "PEMSTRING",
        "domain": "test",
        "garbage": "ignored",
    })
    assert out["consumer_key"] == "ck"
    assert out["consumer_secret"] == "cs"
    assert out["domain"] == "test"
    assert isinstance(out["private_key"], bytes)  # str → bytes
    assert "garbage" not in out
    assert "client_id" not in out


def test_filter_login_kwargs_does_not_clobber_explicit_consumer_key():
    out = sf_client._filter_login_kwargs({
        "client_id": "alias-id",
        "consumer_key": "explicit-ck",
        "username": "u",
    })
    assert out["consumer_key"] == "explicit-ck"


# ---------------------------------------------------------------------------
# Connection name resolution
# ---------------------------------------------------------------------------

def test_connection_name_from_param(monkeypatch):
    monkeypatch.delenv("SF_CREDENTIAL_KEY", raising=False)
    assert sf_client._connection_name({"credential_key": "acme"}) == "acme"


def test_connection_name_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("SF_CREDENTIAL_KEY", "from-env")
    assert sf_client._connection_name({}) == "from-env"


def test_connection_name_missing_raises(monkeypatch):
    monkeypatch.delenv("SF_CREDENTIAL_KEY", raising=False)
    with pytest.raises(sf_client.ConfigError, match="missing_credential_key"):
        sf_client._connection_name({})


# ---------------------------------------------------------------------------
# Keystore credential lookup
# ---------------------------------------------------------------------------

def _set_attune_env(monkeypatch):
    monkeypatch.setenv("ATTUNE_API_URL", "https://attune.local")
    monkeypatch.setenv("ATTUNE_API_TOKEN", "exec-token-xyz")


def _install_fake_attune_sdk(monkeypatch, *, key_value):
    client = object()
    attune_mod = types.ModuleType("attune")
    attune_mod.context = types.SimpleNamespace(
        api_url="https://attune.local",
        api_token="exec-token-xyz",
        client=client,
    )

    api_client_mod = types.ModuleType("attune.api_client")
    api_mod = types.ModuleType("attune.api_client.api")
    secrets_mod = types.ModuleType("attune.api_client.api.secrets")
    get_key_mod = types.ModuleType("attune.api_client.api.secrets.get_key")

    calls = {}

    def fake_sync_detailed(ref, *, client):
        calls["ref"] = ref
        calls["client"] = client
        return types.SimpleNamespace(
            status_code=200,
            content=b"",
            parsed=types.SimpleNamespace(
                data=types.SimpleNamespace(value=key_value),
            ),
        )

    get_key_mod.sync_detailed = fake_sync_detailed
    secrets_mod.get_key = get_key_mod

    monkeypatch.setitem(sys.modules, "attune", attune_mod)
    monkeypatch.setitem(sys.modules, "attune.api_client", api_client_mod)
    monkeypatch.setitem(sys.modules, "attune.api_client.api", api_mod)
    monkeypatch.setitem(sys.modules, "attune.api_client.api.secrets", secrets_mod)
    monkeypatch.setitem(
        sys.modules,
        "attune.api_client.api.secrets.get_key",
        get_key_mod,
    )
    return client, calls


def test_fetch_credential_from_keystore_object(monkeypatch):
    _set_attune_env(monkeypatch)
    captured = {}

    class _Resp:
        status_code = 200
        text = ""

        def json(self):
            return {"data": {"id": 1, "ref": "sf_creds", "value": {
                "consumer_key": "ck-from-keystore",
                "username": "u@acme.com",
                "private_key": "PEMDATA",
                "domain": "test",
            }}}

    def fake_get(url, headers=None, timeout=None):
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        return _Resp()

    monkeypatch.setattr(httpx, "get", fake_get)
    creds = sf_client._fetch_credential_from_keystore("sf_creds")
    assert captured["url"] == "https://attune.local/api/v1/keys/sf_creds"
    assert captured["auth"] == "Bearer exec-token-xyz"
    assert creds["consumer_key"] == "ck-from-keystore"


def test_fetch_credential_uses_attune_sdk_client(monkeypatch):
    expected = {
        "consumer_key": "ck-from-sdk",
        "username": "u@acme.com",
        "private_key": "PEMDATA",
    }
    client, calls = _install_fake_attune_sdk(monkeypatch, key_value=expected)

    creds = sf_client._fetch_credential_from_keystore("sf_creds")

    assert creds == expected
    assert calls == {"ref": "sf_creds", "client": client}


def test_fetch_credential_from_keystore_404(monkeypatch):
    _set_attune_env(monkeypatch)

    class _NotFound:
        status_code = 404
        text = "not found"

        def json(self):
            return {}

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _NotFound())
    with pytest.raises(sf_client.ConfigError, match="credential_key_not_found"):
        sf_client._fetch_credential_from_keystore("missing")


def test_fetch_credential_missing_env(monkeypatch):
    monkeypatch.delenv("ATTUNE_API_URL", raising=False)
    monkeypatch.delenv("ATTUNE_API_TOKEN", raising=False)
    with pytest.raises(sf_client.ConfigError, match="missing_attune_env"):
        sf_client._fetch_credential_from_keystore("anything")


def test_fetch_credential_value_is_string_json(monkeypatch):
    _set_attune_env(monkeypatch)

    class _Resp:
        status_code = 200
        text = ""

        def json(self):
            return {"data": {"value": '{"consumer_key":"ck","username":"u","private_key":"k"}'}}

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _Resp())
    creds = sf_client._fetch_credential_from_keystore("sf_creds")
    assert creds["consumer_key"] == "ck"


def test_fetch_credential_invalid_value_raises(monkeypatch):
    _set_attune_env(monkeypatch)

    class _Resp:
        status_code = 200
        text = ""

        def json(self):
            return {"data": {"value": 12345}}

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _Resp())
    with pytest.raises(sf_client.ConfigError, match="credential_key_not_object"):
        sf_client._fetch_credential_from_keystore("sf_creds")


# ---------------------------------------------------------------------------
# Session-token cache (the new bit — sf-toolkit token_refresh_callback target)
# ---------------------------------------------------------------------------

def test_load_cached_token_returns_none_when_missing(monkeypatch):
    _set_attune_env(monkeypatch)

    class _NotFound:
        status_code = 404
        text = ""

        def json(self):
            return {}

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _NotFound())
    assert sf_client._load_cached_token("acme") is None


def test_load_cached_token_returns_none_when_expired(monkeypatch):
    _set_attune_env(monkeypatch)

    class _Resp:
        status_code = 200
        text = ""

        def json(self):
            # issued_at far in the past
            return {"data": {"value": {
                "instance": "https://x.my.salesforce.com",
                "token": "00D...",
                "issued_at": int(time.time()) - 24 * 3600,
            }}}

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _Resp())
    assert sf_client._load_cached_token("acme") is None


def test_load_cached_token_rehydrates_when_fresh(monkeypatch):
    """Verify a fresh cached token is converted back into a SalesforceToken."""
    pytest.importorskip("sf_toolkit", reason="sf-toolkit not installed in test env")
    _set_attune_env(monkeypatch)
    issued = int(time.time()) - 60  # one minute ago — well within 90min default

    class _Resp:
        status_code = 200
        text = ""

        def json(self):
            return {"data": {"value": {
                "instance": "https://x.my.salesforce.com",
                "token": "00D-cached-token",
                "issued_at": issued,
            }}}

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _Resp())
    tok = sf_client._load_cached_token("acme")
    assert tok is not None
    assert getattr(tok, "token", None) == "00D-cached-token"


def test_load_cached_token_respects_param_max_age(monkeypatch):
    _set_attune_env(monkeypatch)

    class _Resp:
        status_code = 200
        text = ""

        def json(self):
            return {"data": {"value": {
                "instance": "https://x.my.salesforce.com",
                "token": "00D...",
                "issued_at": int(time.time()) - 120,  # 2 minutes ago
            }}}

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _Resp())
    # max age 60s — token is older than that, should be discarded
    assert sf_client._load_cached_token(
        "acme", action_params={"session_token_max_age_seconds": 60}
    ) is None


def test_save_cached_token_updates_existing_key(monkeypatch):
    """When the session-token key already exists, _save_cached_token PUTs it."""
    _set_attune_env(monkeypatch)
    monkeypatch.setenv("ATTUNE_PACK_REF", "salesforce")

    calls = {"put": 0, "post": 0}

    class _Ok:
        status_code = 200
        text = ""

        def json(self):
            return {"data": {}}

    def fake_put(url, headers=None, json=None, timeout=None):
        calls["put"] += 1
        assert url.endswith("/api/v1/keys/acme_session_token")
        assert json["encrypted"] is True
        assert json["value"]["token"] == "tok"
        assert json["value"]["instance"] == "https://x.my.salesforce.com"
        return _Ok()

    def fake_post(*a, **k):
        calls["post"] += 1
        return _Ok()

    monkeypatch.setattr(httpx, "put", fake_put)
    monkeypatch.setattr(httpx, "post", fake_post)

    fake_token = MagicMock()
    fake_token.instance = "https://x.my.salesforce.com"
    fake_token.token = "tok"
    sf_client._save_cached_token("acme", fake_token)
    assert calls["put"] == 1
    assert calls["post"] == 0


def test_save_cached_token_creates_when_missing(monkeypatch):
    """On a 404 from PUT, _save_cached_token falls back to POST /keys."""
    _set_attune_env(monkeypatch)
    monkeypatch.setenv("ATTUNE_PACK_REF", "salesforce")

    calls = {"put": 0, "post": 0}

    class _NotFound:
        status_code = 404
        text = ""

        def json(self):
            return {}

    class _Created:
        status_code = 201
        text = ""

        def json(self):
            return {"data": {}}

    def fake_put(*a, **k):
        calls["put"] += 1
        return _NotFound()

    def fake_post(url, headers=None, json=None, timeout=None):
        calls["post"] += 1
        assert url.endswith("/api/v1/keys")
        assert json["ref"] == "acme_session_token"
        assert json["owner_type"] == "pack"
        assert json["owner_pack_ref"] == "salesforce"
        assert json["encrypted"] is True
        return _Created()

    monkeypatch.setattr(httpx, "put", fake_put)
    monkeypatch.setattr(httpx, "post", fake_post)

    fake_token = MagicMock()
    fake_token.instance = "https://x.my.salesforce.com"
    fake_token.token = "tok"
    sf_client._save_cached_token("acme", fake_token)
    assert calls["put"] == 1
    assert calls["post"] == 1


def test_save_cached_token_swallows_failures(monkeypatch):
    """A keystore write failure must NEVER break the action — log + carry on."""
    _set_attune_env(monkeypatch)

    class _Boom:
        status_code = 500
        text = "internal error"

        def json(self):
            return {}

    monkeypatch.setattr(httpx, "put", lambda *a, **k: _Boom())
    monkeypatch.setattr(httpx, "post", lambda *a, **k: _Boom())

    fake_token = MagicMock()
    fake_token.instance = "https://x.my.salesforce.com"
    fake_token.token = "tok"
    # No exception should escape
    sf_client._save_cached_token("acme", fake_token)


# ---------------------------------------------------------------------------
# get_api_version
# ---------------------------------------------------------------------------

def test_get_api_version_default():
    assert sf_client.get_api_version({}) == sf_client.DEFAULT_API_VERSION


def test_get_api_version_param_overrides_default():
    assert sf_client.get_api_version({"api_version": "v62.0"}) == "v62.0"


def test_get_api_version_env_fallback(monkeypatch):
    monkeypatch.setenv("SF_API_VERSION", "v55.0")
    assert sf_client.get_api_version({}) == "v55.0"
