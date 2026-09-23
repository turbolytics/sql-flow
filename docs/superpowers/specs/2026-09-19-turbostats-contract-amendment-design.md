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
| Scope ceiling | **Withdrawn 2026-09-21.** Deferred to the change that defines commands. See below. | A config key with nothing to enforce: `allow` shipped in #351, was parsed and validated, and was read by nothing. |
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
cancels a constant skew. It does not cancel a clock step between the two
readings, which is what the durations below are for.

### Uptime and idle (added 2026-09-22)

Two fields carry durations from the process's monotonic clock. A receiver
judges start and work from these, and keeps `started_at` and
`last_activity_at` for display.

- `process.uptime_seconds`: seconds since the process started. Absent from
  older engines.
- `idle_seconds`: seconds since the process last did work. Absent before any
  work, and from older engines.

Both are time awake. Go's monotonic clock is `CLOCK_MONOTONIC` on Linux and
`mach_absolute_time` on macOS, and neither advances while the host is
suspended. A gateway that sleeps for an hour reports an uptime without that
hour, so `uptime_seconds` can be less than `sent_at − started_at` without
either being wrong. For work, time awake is the right measure: a suspended
host sends no reports, and the receiver's silence check sees the sleep.

A receiver that gets neither field is talking to an older engine. It falls
back to differences of the instance's own wall readings: `sent_at −
started_at` for uptime and `sent_at − last_activity_at` for idle. Those
cancel a constant clock offset and not a step between the two readings, so
a receiver should distrust a `started_at` it has another reason to doubt:
zero, later than `sent_at`, or earlier than the engine version's release.
During a rollout a fleet mixes both kinds, and the fallback is per bundle.

Every bundle holds three invariants:

- `last_activity_at` is at or before `sent_at`.
- `idle_seconds` is at most `process.uptime_seconds`.
- Activity is per process. It resets on restart. A field that survives a
  restart, such as a durable `sqlflow_progress` arrival time, must not feed
  `last_activity_at` or `idle_seconds`.

`instance.name` is `pipeline.name` under `run` and `serve.name` under `serve`.
It is required when reporting is on, as `instance.id` is.

### Metadata (added 2026-09-22)

`instance` carries what the process is made of, read from its config at
startup: `source_type`, `sink_type` and `handler_type`, lower case. A serve
process has none of the three and sends none of them. The handler's short
name is the registry's -- `structured`, `inferred_mem`, `inferred_disk` --
not the long form the config writes.

`instance.labels` is the operator's own map, declared in the `turbostats`
block and fixed for the life of the process. At most 10 entries; a key
matches `[a-z][a-z0-9_]*` and is at most 32 characters; a value is at most
64. These names are refused, because a label that shadows a field this
contract defines makes two different things share a name: `id`, `name`,
`version`, `commit`, `arch`, `config_hash`, `source_type`, `sink_type`,
`handler_type`.

The bounds are what keep the map inside the shape rule. Its width is set by
whoever wrote the config, never by a broker's partition count or a stream's
error codes, and it cannot change while the process runs.

`sqlflow validate` enforces every rule, and both commands refuse a config
that breaks one. All of it holds whether or not reporting is on: the labels
reach `GET /turbostats/v1` either way, and a rule that would refuse the
config the moment someone sets `report_to` is a defect in it now.

### Signals (added 2026-09-22)

`pipeline.duration` and `serve.duration` carry how long the work takes.
`pipeline` has `batch` and `sink_flush`; `serve` has `request`. Each phase
has `count`, `sum_seconds`, `min_seconds`, `max_seconds` and `buckets`.

Count, sum and the buckets are counters since the process started, so a
receiver subtracts two reports for the interval's distribution and sums
buckets across a fleet. Min and max are since the process started and never
reset, because the reporter and `GET /turbostats/v1` both read this bundle
and a value reset on read would hide events from whichever reader did not
see it. Per-interval movement comes from the buckets.

The boundaries are fixed in this contract, in seconds: 0.001, 0.01, 0.05,
0.25, 1, 5, 30, 60, with a ninth bucket above the last. They are not in the
bundle: nine counts carry a distribution, and buckets that differ per
instance cannot be summed across a fleet. Changing one is a contract change.
A phase with no samples is absent.

Min and max earn their place beside the buckets because nobody knows a
customer's workload. One whose flushes all take 90 seconds puts every sample
in the ninth bucket, and the distribution says nothing; count, sum, min and
max stay true whatever the workload.

`pipeline` carries errors by the phase they were attributed to:
`handler_error_count`, `sink_error_count` and `state_error_count`, with
`dlq_rows` for rows diverted rather than dropped, and `last_error_code` and
`last_error_at`. `source_error_count` exists in this contract and is absent
from an engine that attributes no error to a source phase, which every
engine does today: a zero would say the source has never failed, which is a
different claim. `error_count` remains the total and stays authoritative for
it.

**No error message, ever.** A message carries the row that failed, a
connection string, a customer's data. The code is the taxonomy's, bounded
and safe to store, and an operator with the code and the timestamp finds the
message in their own logs. There is also no count per code: that map grows
as new codes occur, which the shape rule refuses.

`pipeline.recv_wait_seconds` is how long the consume loop has spent waiting
for input, as a counter. It separates two states that look identical from
outside: a pipeline waiting on a quiet source, and one saturated by its own
work. Near the wall clock means the source is quiet; near zero means the
engine is the bottleneck.

Invariants:

- A phase's buckets sum to its `count`, and `min_seconds` is at most
  `max_seconds`.
- `error_count` is at least the sum of the per-phase counters.
- Every one of these fields is absent from an engine that predates it, and a
  receiver treats absent as absent rather than zero.

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

A realistic `serve` bundle stays under 1 KiB. A realistic `run` bundle stays
under 2 KiB: the three types and a label set put it at 1041 bytes and the
durations take it to 1121. Each has its own test.

The numbers exist to catch a field that scales with data, not to shave
bytes. The receiver's limit is 16 KiB and a reporter sends once a minute.

A third test prices the shape rather than the budget: a bundle with every
field at its widest, ten maximal labels and 2^62 in every counter and
bucket, stays under 8 KiB. Nobody sends that bundle; the point is that the
contract's widest legal shape still fits with room to spare.

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
```

`key` replaces the earlier spec's `token`. `validate` fails on a `key` that
does not parse as a credential string.

**The scope ceiling is withdrawn.** This spec proposed `allow`, a list of the
scopes an instance permits, so a compromised control plane could not grant
itself `execute`. It shipped in #351 and did nothing: v1 sends no commands, so
there was nothing for the reporter to drop, and no code read the field. An
operator who set `allow: [read]` got every sign of protection and none of it,
and `sqlflow config example` printed the key into every generated config.
It is removed before any release carried it. The loader decodes strictly, so a
leftover `allow` fails config load rather than reassuring anyone. The ceiling
is still the right defence against a compromised control plane, and it
returns in the change that defines commands, enforced by the code that
receives them.

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
