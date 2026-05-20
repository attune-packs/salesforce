# Salesforce Integration Pack

A standard interface for the Salesforce REST and Streaming APIs, backed by
the [python-sf-toolkit](https://androxxtraxxon.github.io/python-sf-toolkit/)
library.

## Authentication

This pack is a thin adapter on top of
[python-sf-toolkit](https://androxxtraxxon.github.io/python-sf-toolkit/).
The toolkit owns auth-flow selection (via
[`lazy_login`](https://androxxtraxxon.github.io/python-sf-toolkit/auth.html#lazy-authentication-auto-selection)),
in-process connection caching (via
[named connections](https://androxxtraxxon.github.io/python-sf-toolkit/client.html)),
and automatic
[token refresh](https://androxxtraxxon.github.io/python-sf-toolkit/auth.html#token-refresh-and-callbacks).
The pack contributes the Attune-specific glue: looking up your credential
blob in the Attune keystore, and persisting refreshed access tokens back
to the keystore so multiple worker processes share a single live session.

### One-time setup (JWT Bearer — recommended)

1. Generate an RSA key pair:
   ```
   openssl req -x509 -nodes -newkey rsa:2048 -days 3650 \
     -keyout server.key -out server.crt
   ```
2. Create a Salesforce Connected App with **OAuth Settings → Use Digital
   Signatures** and upload `server.crt`.
3. Authorize the Connected App for an integration user (Salesforce setup
   → Manage Connected Apps → Permitted Users → Admin pre-approved).
4. Pre-authorize the user once via the standard OAuth flow.

### Configure credentials in the keystore

Store the credential object — its shape mirrors `lazy_login`'s kwargs —
as a pack-scoped encrypted key:

```bash
attune key create -e \
  --owner-type pack --owner-pack-ref salesforce \
  --ref salesforce_acme \
  --value '{
    "username":     "integration@acme.com",
    "consumer_key": "3MVG9...",
    "private_key":  "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n",
    "domain":       "login"
  }'
```

Then reference it from rules / workflows / action params via
`credential_key`:

```yaml
action_params:
  credential_key: "salesforce_acme"
  soql:           "SELECT Id, Name FROM Account LIMIT 10"
```

The same `credential_key` value is used for three things at once:

* **Credential lookup** — at runtime the action calls
  `GET /api/v1/keys/<credential_key>` using its execution-scoped
  `ATTUNE_API_TOKEN` to fetch the credential blob.
* **sf-toolkit `connection_name`** — sf-toolkit's class-level connection
  registry is keyed on this name, so within a worker process repeat
  invocations skip both the keystore lookup and the login round-trip.
* **Cached session-token ref prefix** — sf-toolkit's
  `token_refresh_callback` writes every (re)issued access token to
  `<credential_key>_session_token` (a separate, encrypted, pack-scoped
  keystore key). Cold-started processes load that cached token and skip
  straight to the first request, only falling back to a full
  `lazy_login` if the cached token is stale or rejected. By default
  cached tokens are discarded after 90 minutes — override with the
  `session_token_max_age_seconds` pack config or per-action parameter.

You never need to manage the `_session_token` key by hand: it's created
and updated automatically by the action.

### Auth flows supported

`lazy_login` supports several flows. Set the relevant fields on your
credential blob:

| Flow | Required fields |
|---|---|
| JWT Bearer | `username`, `consumer_key`, `private_key`, `domain` |
| Password | `username`, `password`, `consumer_key` (+ optional `consumer_secret`) |
| Client Credentials | `consumer_key`, `consumer_secret` |
| Salesforce CLI | `sf_cli_alias` |
| Security Token | `username`, `password`, `security_token` |

`client_id` is accepted as an alias for `consumer_key`, and
`client_secret` for `consumer_secret`.

## Actions

| Action | Purpose |
|---|---|
| `salesforce.query` | Run SOQL, paginate via `nextRecordsUrl` |
| `salesforce.get_record` | Read one record by Id |
| `salesforce.create_record` | Create a record |
| `salesforce.update_record` | Update a record by Id |
| `salesforce.upsert_record` | Upsert by external Id |
| `salesforce.delete_record` | Delete a record by Id |
| `salesforce.fetch_list` | **Composite**: read many records by Id |
| `salesforce.save_list` | **Composite**: create/update/upsert many in one call |
| `salesforce.delete_list` | **Composite**: delete many by Id |
| `salesforce.describe_sobject` | sObject metadata |
| `salesforce.api_limits` | Org API usage / limits |
| `salesforce.bulk_insert` | Bulk API 2.0 insert |
| `salesforce.bulk_update` | Bulk API 2.0 update |
| `salesforce.bulk_upsert` | Bulk API 2.0 upsert |
| `salesforce.bulk_query` | Bulk Query API for large SOQL |
| `salesforce.execute_apex` | Run anonymous Apex |

**Composite vs Bulk:** composite (`fetch_list` / `save_list` / `delete_list`)
is synchronous and limited to ~200 records per call — use it for medium
sized batches inside workflows. Bulk API 2.0 is asynchronous (job-based)
and best for large data loads.

## Sensors

| Sensor | Trigger(s) emitted | Use |
|---|---|---|
| `salesforce.soql_poll` | `salesforce.soql_record`, `salesforce.soql_batch` | Periodic SOQL polling with watermark cursor |
| `salesforce.change_data_capture` | `salesforce.change_event` | Subscribe to Change Data Capture events via CometD |

> **PushTopic:** PushTopic is the legacy Streaming API. Modern Salesforce
> orgs should use Change Data Capture instead, which this pack supports.
> The `change_data_capture` sensor connects to channels such as
> `/data/AccountChangeEvent` or `/data/ChangeEvents` (all CDC events).
