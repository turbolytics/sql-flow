# TurboStats v1 amendment: sections, signing, and the reporter

This spec amends `2026-09-10-turbostats-v1-design.md`. It reshapes the bundle
into sections, adds a `serve` section, replaces the bearer token with signed
requests, fixes the response envelope, and specifies the reporter for both
`sqlflow run` and `sqlflow serve`.

The control plane in `sql-flow-control` is built against this document. Its
own spec is `docs/superpowers/specs/2026-09-19-control-plane-v1-design.md` in
that repository.

Where this spec and the 2026-09-10 spec disagree, this spec wins. Everything
the earlier spec says that this one does not mention still holds.

## Why now

The control plane's first deliverable shows four instances: two pipelines and
two `serve` processes. Three facts about the shipped code block it:

- The reporter does not exist. `GET /turbostats/v1` shipped in #258, and the
  outbound POST was deferred until a receiver existed.
- `sqlflow serve` reports nothing. The bundle's `pipeline` section describes
  a consume loop, and `serve` has none.
- The earlier spec authenticates with a bearer token. The control plane is a
  public multi-tenant service that will later send commands. A secret that
  crosses the wire on every request is the wrong base for that.

## One exception to the versioning rule

The earlier spec says a moved or renamed field is v2. This amendment moves
one field and renames another, and stays v1:

- `last_message_at` moves from the top level into `pipeline`.
- `instance.pipeline` becomes `instance.name`.

The rule protects receivers, and no receiver exists. The 2026-09-10 spec
proposed a cron `curl` on the Bluesky host as a stopgap reader, and nobody
built it. The window closes when the control plane ships. After that, the rule
holds without exception.

## Decisions

| Decision | Choice | Rejected |
|---|---|---|
| Process kinds | A section's presence says what the process does. | A `kind` enum: a process that both consumes and serves would need a schema change. |
| Staleness across kinds | A top-level `last_activity_at`, the latest of the section timestamps. | The receiver inspecting each section: the fleet page would need to know every section that will ever exist. |
| Unknown content | Every reader ignores unknown sections and unknown fields. | Strict parsing: an old receiver would reject a new engine. |
| Authentication | Ed25519 signature over a canonical string. | A bearer token: the secret crosses the wire on every request. HMAC: the receiver must store every secret recoverable. RFC 9421: negotiation and canonicalization this design does not need. |
| Key id | The first 16 hex characters of the SHA-256 of the public key. | An id the control plane assigns: an operator could not bring a key the control plane has never seen. |
| Command channel | The heartbeat response. Its envelope is fixed now and its contents are reserved. | Defining verbs now: nobody has used a command, so any shape is a guess. |
| Scope ceiling | The instance config lists the scopes it allows. The reporter drops anything outside them. | Trusting the control plane's scopes alone: a compromised control plane could grant itself `execute`. |
| Shared code | A public Go package in this repository holds the bundle types and the signing functions. | Copies in both repositories: the two would drift. |
| The HTTP route | `GET /turbostats/v1` stays behind a CLI flag, for local inspection, tests, and the memory soak. The system is push-based, and no deployed instance turns it on. | A config key for it, in the `turbostats` block or anywhere else: a permanent surface for a route nothing in production calls. |

## The document

```jsonc
{
  "v": 1,
  "sent_at": "2026-09-19T20:00:00Z",
  "interval_seconds": 60,
  "last_activity_at": "2026-09-19T19:59:58Z",

  "instance": {
    "id": "bluesky-pipeline-01",
    "name": "bluesky-firehose",
    "version": "v2026.09.18.1",
    "commit": "c24e0ed",
    "arch": "linux/arm64",
    "config_hash": "sha256:9f3c…"
  },

  "process": {
    "started_at": "2026-09-10T08:12:44Z",
    "rss_bytes": 222298112,
    "goroutines": 19
  },

  "pipeline": {
    "message_count": 184203311,
    "handler_rows_read": 184203311,
    "error_count": 12,
    "sink_flush_count": 1842033,
    "sink_rows_accepted": 184203311,
    "sink_rows_written": 184203311,
    "state_commit_count": 1842033,
    "state_db_size_bytes": 4194304,
    "last_message_at": "2026-09-19T19:59:58Z"
  },

  "serve": {
    "request_count": 1204332,
    "request_error_count": 17,
    "sessions_in_use": 2,
    "sessions_total": 8,
    "last_request_at": "2026-09-19T19:59:57Z",
    "cache": {
      "hit_count": 1100210,
      "miss_count": 90122,
      "shared_count": 14000,
      "eviction_count": 5210,
      "bytes": 41943040,
      "entries": 3120
    }
  },

  "commands": [],

  "exit": { "reason": "SIGTERM", "code": 0 }
}
```

The example shows every section at once. A real bundle from `sqlflow run`
carries `pipeline` and no `serve`. A bundle from `sqlflow serve` carries
`serve` and no `pipeline`.

### Extensibility rules

1. **Sections are optional top-level objects.** `instance` and `process` are
   always present. Every other section is present only when the process does
   that work.
2. **Every reader ignores unknown sections and unknown fields.** A new section
   or a new field is additive and stays v1.
3. **A section owns its fields.** A number that describes the consume loop
   lives in `pipeline`. Nothing section-specific sits at the top level, with
   one denormalized exception: `last_activity_at`.
4. **Reserved names.** `commands` at the top level of the bundle and of the
   response. v1 never populates either.

### New and changed fields

`interval_seconds` is the reporter's configured interval. The receiver needs
it to tell a late heartbeat from a normal one. `GET /turbostats/v1` omits it
when no reporter is configured.

`last_activity_at` is the latest of the section activity timestamps the
bundle carries: `pipeline.last_message_at` and `serve.last_request_at`.
`Collect` computes it. The section fields are the source, and this field is a
denormalized copy. It is absent until any section sees activity. All three
timestamps come from the instance's clock, so `sent_at − last_activity_at`
carries no skew.

`instance.name` is `pipeline.name` under `run` and `serve.name` under `serve`.
It is required when reporting is on, as `instance.id` is.

### The `serve` section

Every number is read from a dimensionless series, the same rule the
`pipeline` section follows. `serve` records a flat twin beside each
attributed instrument, and the recording site decides what counts:

| Bundle field | Recorded |
|---|---|
| `request_count` | Every request answered, any dataset, any code |
| `request_error_count` | Responses with a 5xx code. A 4xx is the caller's error, not the server's. |
| `sessions_in_use`, `sessions_total` | The pool's `Stats()`, read at collect time |
| `last_request_at` | Once per request answered |
| `cache.hit_count`, `cache.miss_count`, `cache.shared_count` | One per cached-dataset request, by outcome |
| `cache.eviction_count` | Every eviction, any reason |
| `cache.bytes`, `cache.entries` | The cache's own stats, read at collect time |

`cache` is absent on a server where no dataset opted into the cache. An
absent cache and an empty cache are different facts.

The three latency histograms stay out, for the reason the earlier spec gives.

`serve` builds its meter provider only when the config asks for `/metrics`.
That repeats the defect the earlier spec fixed for `run`. The provider and a
manual reader must exist always, and the Prometheus exporter attaches as a
second reader when configured.

### Size

The bound stays 1 KiB per bundle. A test holds a full `run` bundle and a full
`serve` bundle under it, each separately.

## The response

Every 2xx response to a heartbeat carries this body:

```json
{ "v": 1, "commands": [] }
```

Three rules bind the v1 reporter, because instances deployed today must
survive the day the control plane starts sending commands:

- Parse the body as a JSON object and ignore unknown fields.
- Treat any 2xx as success, whatever the body holds. A body that does not
  parse is logged at debug level and is still a success.
- Ignore `commands` entirely. v1 defines no verb.

The contents of `commands`, in both directions, belong to a later spec. That
spec will also define how the control plane signs a command and how the
instance pins the control plane's public key.

## Signing

The instance signs every heartbeat with Ed25519. No secret crosses the wire.

The signature covers this canonical string, with `\n` as a literal newline:

```
v1\n{method}\n{path}\n{timestamp}\n{sha256_hex(body)}
```

`timestamp` is Unix seconds. `path` is the request path without the query.

The request carries three headers:

| Header | Value |
|---|---|
| `X-Turbostats-Key-Id` | 16 hex characters: the start of the SHA-256 of the 32-byte public key |
| `X-Turbostats-Timestamp` | The timestamp in the canonical string |
| `X-Turbostats-Signature` | The 64-byte signature, base64 |

The receiver rejects a request when:

- The key id is unknown, revoked, or expired.
- The signature does not verify.
- The timestamp is more than 5 minutes from the receiver's clock.
- The bundle's `sent_at` is not newer than the last bundle stored for that
  instance. This is the replay defense, and it needs no nonce table.

### The credential string

The operator holds one string: `sfc_` plus the base64url encoding of the
32-byte seed. `ed25519.NewKeyFromSeed` rebuilds the private key, the public
key follows from it, and the key id follows from the public key. The string
carries nothing else.

v1 of the control plane generates the keypair and shows this string once.
Because the key id is a fingerprint, an operator can later generate keys on
their own devices and register only the public keys. Instances signed with an
unregistered key receive `401` until registration, then start succeeding with
no restart. That flow needs no change to this contract.

## The public package

`internal/turbostats` stays the collector. The wire contract moves to a
public package so `sql-flow-control` can import it:

```
github.com/turbolytics/sql-flow/turbostats/wire
```

It exports the `Bundle` types, the `Response` type, `MediaType`, `Version`,
the header names, `CanonicalString`, `Sign`, `Verify`, `KeyID`, and
`ParseCredential`. It imports only the standard library.

`testdata/vectors.json` pins a seed, a request, and the expected key id and
signature. Both repositories test against it.

## The reporter

The config block is the same under `pipeline` and under `serve`:

```yaml
pipeline:
  name: bluesky-firehose
  turbostats:
    id: bluesky-pipeline-01
    report_to: https://control.turbolytics.io/v1/turbostats
    key: {{ SQLFLOW_TURBOSTATS_KEY }}
    interval_seconds: 60
    allow: [read]
```

`key` replaces the earlier spec's `token`. `allow` lists the scopes this
instance permits and defaults to `[read]`. v1 accepts only `read`, and any
other value fails validation. `validate` also fails on a `key` that does not
parse as a credential string.

Every reporter rule in the earlier spec still holds: one post at start, one
per interval with ten percent jitter, one on a clean drain with `exit`; a
10-second timeout; no retry and no queue; a failed post never touches the
pipeline; plaintext to a non-loopback address fails config load.

A `401` follows the same failure rules as any other failed post. An
unregistered fleet logs one warning when the run of failures begins and one
info line when it ends.

`sqlflow serve` sends its final bundle after the HTTP server drains.

## Testing

Unit, all under `go test -short`:

- `wire`: the vectors verify. A tampered body, a tampered path, and a wrong
  key each fail. `KeyID` and `ParseCredential` match the vectors.
- `Collect` emits `pipeline` and no `serve` under `run`, and the reverse
  under `serve`. `last_activity_at` equals the later section timestamp and is
  absent when neither exists.
- `serve` flat totals: a 5xx increments `request_error_count` and a 4xx does
  not. `cache` is absent without a cache.
- Both example bundles marshal under 1 KiB.
- The reporter signs every post. It treats a 2xx with an unknown field, with
  a non-empty `commands`, and with an unparseable body as success. A hung
  receiver does not delay the caller past the timeout.
- Config validation rejects a missing `id`, a malformed `key`, a scope other
  than `read`, and a plaintext non-loopback `report_to`.

Release, in `tests/release`: the shipped image under `serve` answers
`GET /turbostats/v1` with a `serve` section.

The feature registry gains `observability.turbostats.reporter` and
`observability.turbostats.serve`.

## Rollout

1. **PR 1: the contract.** The `wire` package, the field move and the rename,
   `interval_seconds`, `last_activity_at`, the `serve` flat totals and
   section, and the always-present meter provider in `serve`.
2. **PR 2: the reporter.** The config block, its validation, signing, and the
   final bundle, in both commands. It lands when the control plane's ingest
   endpoint is deployed.

Each PR carries a memory soak, because both touch the process that runs for
weeks.

## What breaks if this is wrong

- A reporter that fails on an unknown response field strands every deployed
  instance the day commands ship. The response rules above are the guard.
- A key id the two sides derive differently rejects every heartbeat. The
  shared vectors are the guard.
- A `Collect` that blocks stalls nothing but itself: it runs on the
  reporter's goroutine under the post's timeout.

## Out of scope

- Any command verb, command status, or command signing.
- Registering an operator-generated public key. The contract allows it, and
  the control plane does not build it yet.
- Histograms, per-dataset numbers, and per-table gauges.
- Buffering or replay across an outage.
