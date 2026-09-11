# TurboStats v1: design

A sqlflow process reports its own state as one small, versioned document:
what it is, when it started, how much memory it holds, and the totals every
instrument has counted since it started. The document is TurboStats. The
process serves it at `GET /turbostats/v1` for anything that can reach the
process, and posts it outbound to a control plane on an interval, because
most instances cannot be reached.

This is the contract the control plane, its first demo page, and every later
fleet feature depend on. It is written down before any of them exist so that
each is built against the document rather than against the others.

## Why now

The control plane's first deliverable is a read-only demo at
control-demo.turbolytics.io showing a live fleet: instance, uptime, restarts
with causes, and resident memory over time. Two facts about that fleet fix the
shape of this design:

- Instances cannot be dialed. The Raspberry Pi is behind home NAT with no
  resolvable address, and industrial deployments look the same. Status has to
  leave the instance as a push it initiates. Nothing can be pulled from it.
- The targets are constrained. A sidecar that scrapes the Prometheus text
  format and re-encodes it is two processes and a text parser on a device
  that may have 512 MiB. Prometheus stays as the exporter for cloud
  deployments. The instance itself reports, and it reports the instrument
  values directly, not a rendering of them.

Two facts about the code fix the rest:

- The Prometheus exporter builds a bare `prom.NewRegistry()`. `/metrics`
  carries the pipeline's instruments and nothing about the process: no start
  time, no resident memory, no goroutines. The demo needs exactly those.
- The meter provider is built only for `--metrics=prometheus`. With metrics
  off there is no provider and the instruments record into nothing, so there
  is nothing for a reporter to read. The provider has to exist always.

The Bluesky firehose instance has been running for days and nothing is
recording its uptime. History does not backfill, so the sequence below starts
with a cron line, not a package.

## Decisions

Made in the design conversation. Each one closes a fork.

| Decision | Choice | Rejected |
|---|---|---|
| Who builds the document | The sqlflow process, from its own meter. | A sidecar scraping `/metrics`: a second process and a text parser on constrained hardware, and no access to the process's own memory. |
| Direction | The instance initiates every connection. Status is pushed. | The control plane scraping instances: nothing can resolve or dial one. |
| One document, two transports | `Collect()` builds a `Bundle`; the HTTP handler serves it and the reporter posts it. Neither computes anything. | A different shape per transport. |
| Field names | The OTel instrument names: `message_count`, `error_count`. | The Prometheus series names, `message_count_messages_total`: those suffixes belong to one exporter's rendering. |
| Counters | Totals since `started_at`. The receiver derives rates. | Rates computed on the instance: they need the previous sample remembered, and remembered state is what goes wrong on a device that reboots. |
| Restarts | Derived by the receiver from `started_at` changing. | Reported by the instance: the process that died cannot say so, and its replacement should not guess. |
| Restart causes | A final bundle on a clean drain carries `exit`. A crash sends nothing. `config_hash` differing across a gap is a config change. | Supervisor integration. |
| Histograms | Excluded from v1. | Included: buckets are most of a scrape by bytes, and the demo reads none of them. |
| Per-table gauges | Excluded from v1. `state_db_size_bytes` alone. | `state_table_rows` and `reference_table_rows`: a table dimension makes the size unbounded. |
| Identity | `instance.id` is an explicit config field, required when reporting is on. | Derived from machine-id plus pipeline name: silently collides when a device is imaged from another. |
| Buffering while unreachable | None. A missed interval is a gap, and the gap is the signal. | A queue on the instance: state, memory, and replayed samples that hide the outage the page exists to show. |
| Versioning | `/turbostats/v1` in the path and `"v": 1` in the body. Additive changes stay v1. A removed or renamed field is v2 at a new path. | Content negotiation: more machinery than a path segment. |
| Media type | `application/vnd.turbolytics.turbostats.v1+json` | `application/json`: says nothing about which version this is. |
| Encoding | JSON. | CBOR: smaller, but the debug path on a device is `curl`, and 550 bytes is already small. |
| Where it lives | `internal/turbostats` in this repository. | The private control-plane repository: the builder ships in the binary the control plane observes. |

## The document

```jsonc
{
  "v": 1,
  "sent_at": "2026-09-10T21:14:03Z",

  "instance": {
    "id": "pi-basement-01",
    "pipeline": "bluesky-firehose",
    "version": "v1.1.0",
    "commit": "723f823",
    "arch": "linux/arm64",
    "config_hash": "sha256:9f3c…"
  },

  "process": {
    "started_at": "2026-09-01T08:12:44Z",
    "rss_bytes": 26214400,
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
    "consumer_lag": 0,
    "state_db_size_bytes": 4194304
  },

  "exit": {
    "reason": "SIGTERM",
    "code": 0
  }
}
```

### Fields

`v` is the document version. A receiver rejects a `v` it does not know.

`sent_at` is when the bundle was built, RFC 3339, UTC.

`instance`:

- `id`: the operator's name for this instance. From `pipeline.turbostats.id`
  in the config. Required when reporting is enabled; a config that enables
  reporting without it fails validation.
- `pipeline`: `pipeline.name` from the config. Required when reporting is
  enabled, for the same reason.
- `version`, `commit`: the values the Makefile stamps into `internal/cli`.
  The package cannot import `internal/cli` without a cycle, so the run command
  hands them to the package once at startup.
- `arch`: `runtime.GOOS + "/" + runtime.GOARCH`.
- `config_hash`: `sha256:` plus the hex digest of the rendered config text,
  after templating and before parsing. Two instances running the same file
  under different environments hash differently, which is correct: they are
  running different configs.

`process`:

- `started_at`: taken once, when the run command starts. RFC 3339, UTC.
- `rss_bytes`: anonymous resident memory. On Linux, `RssAnon` from
  `/proc/self/status`, which excludes file-backed pages and is exact. On other
  platforms, `getrusage` peak resident size, which only rises; it is a weaker
  signal there and the field is still populated. This is the reading
  `internal/handlers/leak_test.go` already makes, moved into a package the
  test then imports.
- `goroutines`: `runtime.NumGoroutine()`.

`pipeline` holds one field per instrument in `internal/core/metrics.go` and
`internal/core/counting.go` that is a counter or a dimensionless gauge, named
for the instrument. Counters are the total since `started_at`. Gauges are the
value now. An instrument that carries attributes is summed across them, so
`consumer_lag` is the sum over partitions, with one exception: a data point
whose `role` attribute is not `pipeline` is skipped. The DLQ sink records
`sink_rows_accepted` and `sink_rows_written` with `role=dlq` so its rows
never sum into the pipeline's delivered series, and the bundle keeps them
out the same way. `state_db_size_bytes` comes from
the same stats function `/stats` uses, and is omitted, not zero, when the
pipeline has no state path: absent state and empty state are different facts.

`exit` is present only in the last bundle a clean shutdown sends, after the
drain completes. `reason` is the signal name or `max-msgs` when the run ended
by count. `code` is the process exit code. A bundle without `exit` is an
instance that is still running, or one that died without saying so.

### Size

A bundle is under 1 KiB. A test holds the example above under that bound,
and the bound is the reason histograms and per-table gauges are out.

## The package

`internal/turbostats` exports:

```go
// Static is what the run command knows once and the package cannot learn.
type Static struct {
    ID, Pipeline, Version, Commit, ConfigHash string
    StartedAt                                 time.Time
}

// Collect builds one bundle. It is the only function that does.
func Collect(ctx context.Context, s Static, r *sdkmetric.ManualReader,
             stats func() (*core.StateStats, error)) (Bundle, error)
```

`Collect` calls `r.Collect` on the manual reader, walks the resource metrics,
and picks each instrument by name. It reads `/proc/self/status` and the
runtime. It calls `stats` when it is non-nil. It allocates one bundle and
touches nothing shared, so the handler and the reporter can call it at once.

The manual reader is attached to the meter provider at startup, always. The
Prometheus exporter is a second reader attached only for
`--metrics=prometheus`. Both read the same instruments, which is the property
the design exists for.

## The endpoint

`GET /turbostats/v1` on the existing mux at `:8000`, beside `/metrics` and
`/stats`. It responds `200` with the media type above and one bundle. It
serves whenever the HTTP server is up.

The server is up for `--metrics=prometheus`, as today, or for a new
`--turbostats` flag, which starts the server without the Prometheus exporter.
The default stays off. A device should not listen on a port unless told to.

`/stats` stays as it is. It is the detailed per-table view of durable state,
and renaming it breaks whoever reads it. The bundle's `state_db_size_bytes`
reads through the same function so the two cannot disagree.

## The reporter

Configured in the pipeline file:

```yaml
pipeline:
  name: bluesky-firehose
  turbostats:
    id: pi-basement-01
    report_to: https://control-demo.turbolytics.io/v1/turbostats
    token: {{ SQLFLOW_TURBOSTATS_TOKEN }}
    interval_seconds: 60
```

`report_to` enables it. `token` is a bearer token, supplied through the
template as every secret in a config is, so `sqlflow validate` reports it
missing before the pipeline starts. `interval_seconds` defaults to 60.

The reporter does not need the HTTP server. It reads the same manual reader
the handler reads and opens its own outbound connection, so a device can
report without listening on any port. `--turbostats` and `report_to` are
independent, and a fleet instance will usually set only the second.

The reporter posts one bundle at start, one every interval, and one on a
clean drain. Each post is `POST report_to` with `Authorization: Bearer`,
the media type above, and a 10-second timeout. The interval carries ten
percent jitter, so a fleet that loses power together does not report
together.

Three rules about failure:

- **A failed post never touches the pipeline.** The reporter runs on its own
  goroutine, every post is bounded by its timeout, and nothing it does can
  block the consume loop or delay a shutdown by more than that timeout.
- **A failed post is logged and forgotten.** Debug level per failure, one
  warning when a run of failures begins, one info when it ends. No retry
  within an interval, no queue.
- **Plaintext to a non-loopback address is refused** at config load. TLS is
  not optional on a public network, and a device is on one.

The final bundle is sent from the run command after `ConsumeLoop` returns and
the drain completes, before the process exits, on a context bounded by the
same timeout. A crash never reaches that line, which is the point.

## Testing

Unit, all in-process, all under `go test -short`:

- `Collect` reads a counter's total from a manual reader, sums it across
  attribute sets, reads a gauge's current value, and omits every histogram.
- `Collect` omits `state_db_size_bytes` when `stats` is nil and populates it
  when it is not.
- The example bundle marshals under 1 KiB.
- The handler responds with the media type and a bundle whose `v` is 1.
- The reporter posts at start, at each interval, and on drain with `exit`
  set; a receiver that hangs does not delay the caller past the timeout;
  a plaintext non-loopback `report_to` fails config validation.
- The feature registry gains `observability.turbostats`, and every test
  above carries its marker, so the coverage gate holds the package to the
  same standard as the rest of the engine.

Release, in `tests/release`: the shipped image with `--turbostats` answers
`GET /turbostats/v1` with a bundle whose `version` matches the image tag.

## Rollout

### First, today: start the record

On the Bluesky host, one cron line every minute appending the time, the
process start time, and RSS from `ps` to a file. No code. The demo's uptime
graph starts from today rather than from the day the endpoint ships, and the
file is backfilled into the control plane's store later.

### PR 1: the document and the endpoint

`internal/turbostats`, `Collect`, the always-present manual reader, the
`--turbostats` flag, `GET /turbostats/v1`, the feature entry, and the tests
above except the reporter's. Deploy to the Bluesky instance. Its first
recorded restart; record the cause. Switch the cron from `ps` to `curl`.

### PR 2: the reporter

The `turbostats` config block, its validation, the reporter, and the final
bundle on drain. Lands when the control plane has an endpoint to post to.

### After

The private control-plane repository: an ingest endpoint validating the
media type and the token, a store of bundles and the uptime segments derived
from them, a read-only query API, the three-panel page, and the deployment.
Each is its own design.

## Out of scope

- Any command path. v1 has no verb the control plane can send an instance.
- The three latency histograms, per-table gauges, and `duckdb_memory()`.
  Each is additive if a later panel needs it.
- Buffering or replay of bundles across an outage.
- CBOR or any second encoding.
- Issue #178's Kubernetes operator. It assumes provider-hosted pipelines in a
  cluster you control, where none of this design's constraints apply.
