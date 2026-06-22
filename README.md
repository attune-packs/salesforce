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

### Configure org credentials in the keystore

Store org-level auth material (consumer key, private key, domain) as a
pack-scoped encrypted key:

```bash
attune key create -e \
  --owner-type pack --owner-pack-ref salesforce \
  --ref salesforce_acme \
  --value '{
    "consumer_key": "3MVG9...",
    "private_key":  "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n",
    "domain":       "login"
  }'
```

Set `default_org_credential_key` in pack config to this key for the
default org. At runtime, callers usually provide only `username`:

```yaml
action_params:
  username:       "integration@acme.com"
  soql:           "SELECT Id, Name FROM Account LIMIT 10"
```

To target a non-default org, pass `org_credential_key` per invocation:

```yaml
action_params:
  org_credential_key: "salesforce_other_org"
  username:           "integration@other-org.com"
  soql:               "SELECT Id FROM Account LIMIT 10"
```

`credential_key` is still accepted as a compatibility alias for
`org_credential_key`.

The selected org key (default or override) plus runtime `username` is used
for three things at once:

* **Credential lookup** — at runtime the action calls
  `GET /api/v1/keys/<org_key>` using its execution-scoped
  `ATTUNE_API_TOKEN` to fetch the credential blob.
* **sf-toolkit `connection_name`** — sf-toolkit's class-level connection
  registry is keyed on a stable hash of `(org_key, username)`, so within a
  worker process repeat
  invocations skip both the keystore lookup and the login round-trip.
* **Cached session-token ref** — sf-toolkit's
  `token_refresh_callback` writes every (re)issued access token to
  a separate, encrypted, pack-scoped
  keystore key). Cold-started processes load that cached token and skip
  straight to the first request, only falling back to a full
  `lazy_login` if the cached token is stale or rejected. By default
  cached tokens are discarded after 90 minutes — override with the
  `session_token_max_age_seconds` pack config or per-action parameter.

You never need to manage session-token keys by hand: they're created and
updated automatically.

### Auth flows supported

`lazy_login` supports several flows. Set the relevant fields on your
credential blob:

| Flow | Required fields |
|---|---|
| JWT Bearer | `consumer_key`, `private_key`, `domain` (+ runtime `username`) |
| Password | `username`, `password`, `consumer_key` (+ optional `consumer_secret`) |
| Client Credentials | `consumer_key`, `consumer_secret` |
| Salesforce CLI | `sf_cli_alias` |
| Security Token | `username`, `password`, `security_token` |

`client_id` is accepted as an alias for `consumer_key`, and
`client_secret` for `consumer_secret`.

### Quick setup: External Client App + cross-pack usage

1. In Salesforce, create an **External Client App** for JWT bearer auth:
   - Enable OAuth and digital signatures.
   - Upload the public cert (`server.crt`).
   - Capture the app's consumer key.
   - Pre-authorize the integration users (the usernames your workflows will pass).
2. In Attune, create one Salesforce pack key per org with app-level material:
   - `consumer_key`, `private_key`, and `domain` (`login` or `test`).
   - Keep username out of this key unless you want legacy fallback behavior.
3. Set the Salesforce pack default to that key via `default_org_credential_key`.
4. From another pack, call Salesforce actions with runtime `username`:

```yaml
tasks:
  - name: query_accounts
    action: salesforce.query
    input:
      username: "integration@acme.com"
      soql: "SELECT Id, Name FROM Account LIMIT 10"
```

5. To target a non-default org from another pack, pass `org_credential_key`:

```yaml
tasks:
  - name: query_other_org
    action: salesforce.query
    input:
      org_credential_key: "salesforce_other_org"
      username: "integration@other-org.com"
      soql: "SELECT Id, Name FROM Account LIMIT 10"
```

If your caller pack owns credentials separately, you can also pass a direct
`credentials` object to Salesforce actions instead of an org key.

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
> The `change_data_capture` sensor can target one or more objects via
> `objects: ["Account", "Contact"]`, or subscribe to all CDC traffic with
> explicit `all_events: true` (`/data/ChangeEvents`).

### `salesforce.change_event` payload fields

| Field | Meaning |
|---|---|
| `change_type` | CDC operation type (`CREATE`, `UPDATE`, `DELETE`, `UNDELETE`, or `GAP_*`). |
| `entity_name` | Salesforce object API name for the event (`Account`, `Contact`, `Custom__c`, etc.). |
| `record_ids` | One or more Salesforce record IDs affected by the change. |
| `changed_fields` | Field API names changed in this event (typically populated for updates). |
| `commit_timestamp` | Commit timestamp from Salesforce (milliseconds since epoch). |
| `commit_user` | Salesforce user ID that committed the change. |
| `transaction_key` | Transaction identifier shared by events from the same transaction. |
| `sequence_number` | Event order within the transaction. |
| `replay_id` | Replay cursor for resumable subscriptions. |
| `channel` | CDC channel that delivered the event (`/data/AccountChangeEvent`, etc.). |
| `payload` | Raw Salesforce CDC payload (`ChangeEventHeader` + object field data). |
