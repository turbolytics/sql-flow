# Deploy to Render: a metrics pipeline

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/turbolytics/sql-flow)

One click creates a Postgres, a sqlflow pipeline that accepts metrics over a
signed webhook and aggregates them by minute, and an HTTP API that reads them
back. Everything it deploys is in this directory, and
[`render.yaml`](../render.yaml) at the repository root declares it.

It is a paid deploy: a `0.1c-256mb` Postgres and two `0.5c-512mb` web
services, in `virginia`. Render shows the price before you confirm.

## Deploy

Render asks for three values. Make the first two before you click, and keep
them. You choose both, so you have
them when the deploy finishes:

```sh
openssl rand -hex 32   # SQLFLOW_WEBHOOK_HMAC_SECRET
openssl rand -hex 16   # SQLFLOW_SERVE_CLIENT_ID
```

| Prompt | What it does |
|---|---|
| `SQLFLOW_WEBHOOK_HMAC_SECRET` | Signs what you send. Any long random string. |
| `SQLFLOW_SERVE_CLIENT_ID` | Reads what you sent, as `?client_id=`. Letters, digits, `.`, `_`, `~` and `-` only, so it can be pasted into a URL. |
| `SQLFLOW_TELEMETRY` | Leave it blank, or type `off`. See [What this sends](#what-this-sends). |

A blank value fails its service, on purpose. The pipeline writes to your
database and will not start unsigned unless you set
`SQLFLOW_WEBHOOK_AUTH=none` yourself. The client id is the only thing between
a reader and your metrics, so it has no default. It travels in the URL: treat
it as you would a link to a private document, and change it on the API
service if it leaks.

When the deploy finishes, copy the URLs of `sqlflow-metrics-ingest` and
`sqlflow-metrics-api` from the Render dashboard.

## What this sends

Unless you type `off` at the `SQLFLOW_TELEMETRY` prompt, this deploy sends the
sqlflow maintainers two events, once each, for as long as it exists. They
tell us that the template was deployed and that it worked. This is the whole
of both:

```json
{"name": "install.deployed", "type": "count", "value": 1,
 "dimensions": {"install_id": "3f0c1b7e-…", "source": "render",
                "template": "render-metrics", "sqlflow_version": "v2026.09.19.1"}}
```

```json
{"name": "install.first_request", "type": "count", "value": 1,
 "dimensions": {"install_id": "3f0c1b7e-…", "source": "render",
                "template": "render-metrics", "sqlflow_version": "v2026.09.19.1"}}
```

| Field | What it is |
|---|---|
| `install_id` | A random uuid your database made for itself in `migrations/0005_install.sql`. It is derived from nothing: not your account, a hostname, an address or a Render id. It lets us count a deploy once however often it restarts. |
| `source`, `template` | Constants: that this is the Render template. |
| `sqlflow_version` | The image tag. |

The first is sent when the pipeline first starts. The second is sent when your
database holds its first metric, from anyone. **Your metrics are never read
for it and never sent:** not a name, a value, a dimension or a count.
`bin/telemetry.sh` asks Postgres one question, whether `metrics_1m` has a row.

They go to `https://telemetry.turbolytics.io`, which is this same template
with signatures off. As with any HTTP request, the receiving end sees the
address it came from. The pipeline neither reads nor stores it. Render's
request log for that service holds it for a while, as any host's does.

The ingest service logs each event in full when it sends it. `bin/telemetry.sh`
is 91 lines and is the only code that sends anything. The sqlflow
binary itself never phones home.

**To turn it off**, type `off` at the prompt, or set `SQLFLOW_TELEMETRY=off` on
`sqlflow-metrics-ingest` later. `false`, `0` and `no` also work. Nothing is
sent, and the log says so.

A collector that is down, slow or gone never affects your deploy. A send is
bounded at five seconds, runs in the background, and is retried later.

## Send a metric

```sh
export URL=https://sqlflow-metrics-ingest-xxxx.onrender.com
export SECRET=<the secret you typed>
bin/send.sh '{"name":"hello","type":"count","dimensions":{"from":"readme"}}'
```

```
{"status":"received"}
```

`bin/send.sh` is short. The signature is an HMAC-SHA256 of the exact
bytes of the body, hex-encoded, in `X-Signature-256` with a `sha256=` prefix:

```sh
sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')
curl -X POST "$URL/events" -H "X-Signature-256: sha256=$sig" --data-binary "$body"
```

## Read it back

A metric is readable once its minute closes, one to two minutes after you
send it. Ask which series exist:

```sh
export API=https://sqlflow-metrics-api-xxxx.onrender.com
export CLIENT_ID=<the client id you typed>
curl -sG "$API/v1/datasets/series" --data-urlencode "client_id=$CLIENT_ID"
```

Then read one:

```sh
curl -sG "$API/v1/datasets/metric" --data-urlencode "client_id=$CLIENT_ID" \
  --data-urlencode 'name=hello'
```

**The pipeline answers 200 to anything signed.** It acknowledges a request
before it reads it, so a metric with no `name`, a body that is not JSON, and
a bare JSON array all answer `{"status":"received"}` and store nothing. If a
metric does not appear in `series` after two minutes, check it against the
rules below.

## The metric

One metric:

```json
{
  "name": "checkout.completed",
  "type": "count",
  "value": 1,
  "dimensions": {"region": "us-east", "plan": "pro"},
  "timestamp": "2026-09-18T14:03:22Z"
}
```

Several, in one signed request:

```json
{"metrics": [{"name": "a", "type": "count"}, {"name": "b", "type": "gauge", "value": 0.73}]}
```

| Field | Required | Default | Rule |
|---|---|---|---|
| `name` | yes | | 1 to 200 bytes. |
| `type` | yes | | `count` or `gauge`. |
| `value` | no | `1` | A finite number. |
| `dimensions` | no | `{}` | A JSON object of at most 16 keys. Every value is stored as a string: `1` and `"1"` are the same. |
| `timestamp` | no | arrival | RFC 3339. More than a minute in the future is dropped. Older than the open minutes, about two, is dropped: there is no backfill. |

A series is a name, a type and a set of dimensions. Key order does not
matter.

Send `timestamp` when order matters. Metrics without one that arrive within
the same five seconds share an arrival time, and which of them is a gauge's
`value_last` is not defined.

## What is stored

Every minute of every series stores five values: `value_sum`, `value_count`,
`value_min`, `value_max` and `value_last`. `type` does not change what is
stored. It says which value to read: `value_sum` for a count, `value_last`
for a gauge. An average is `value_sum / value_count`, at any grain.

### More than one pipeline instance

You can run several instances of `sqlflow-metrics-ingest`, and Render runs two
for a moment during every deploy, the old beside the new. Each instance holds
its own minute in memory, so two of them can each hold part of the same
minute of the same series.

The pipeline therefore writes to `metrics_1m_writers`, keyed on the series
and on a `writer` id that `bin/entrypoint.sh` makes new on every start. An
instance replaces only what it published itself. A trigger merges every
writer's row into `metrics_1m` inside the same transaction: sums add, min and
max nest, and `value_last` is the reading with the latest event time across
instances. Everything else reads `metrics_1m` and never sees a writer.

Keyed on the series alone, the second instance to publish replaced the first:
two instances sent 7 and 5 of one minute stored 7. `make test` runs two
instances and sends each a share of one minute, and
`internal/rendertemplate` runs four concurrent writers through forty rounds
and holds every grain to what they published.

Never set `SQLFLOW_WRITER_ID` yourself. Two instances that share one replace
each other's minutes, which is the defect it exists to prevent.

`metrics_1m_writers` holds one row per instance per minute per series, so with
one instance it is as large as `metrics_1m`. Nothing reads a row of it once
its minute is merged and no instance can publish that minute again, a few
minutes later. Deleting older rows is safe and changes no other table.

### Coarser grains

Postgres keeps the same five values at 5m, 15m, 1h, 6h and 1d, by triggers
that run inside the pipeline's own write. [`rollups.yml`](rollups.yml)
declares them, `migrations/0003_rollups.sql` is generated from it with
`make rollups`, and `make validate` fails when it has drifted.

## The API

Every request except `/healthz` and `/metrics` sends `?client_id=`. It names
the caller in the log, and a request without it answers 401.

An answer is held for 30 seconds. Ask twice within that and the second answer
is the first, with `"cache": "hit"` and its age in `age_ms`.

| Route | Returns |
|---|---|
| `GET /v1/datasets/series` | Every series: `name`, `type`, `dimensions`, `first_bucket`, `last_bucket`. |
| `GET /v1/datasets/metric?name=…` | Per bucket per series: the five values, `type`, and `dimensions`. |
| `GET /v1/datasets/metric_total?name=…` | Per bucket, across every series of the name: `value_sum`, `value_count`, `value_min`, `value_max`. |

`dimensions` in a response is a JSON object encoded as a string. Parse it.

`metric` and `metric_total` take:

| Param | Meaning |
|---|---|
| `name` | Required. Without it the answer is empty. |
| `since`, `until` | RFC 3339 with an offset. Encode `+` as `%2B`. Default: the last hour. |
| `grain` | Optional. Without it the API picks the finest grain that covers the range, which is what you want. Pin one only with a range at least as wide as its bucket. |
| `dimensions` | `metric` only. A JSON object. The answer holds the series whose dimensions contain every pair in it. |

`dimensions={"region":"us-east"}` returns every plan in us-east, each its own
series. `dimensions={"region":"us-east","plan":"pro"}` returns one series. A
`dimensions` that is not JSON matches nothing.

The API snaps a range to the grain's bucket boundaries, and a bucket is in
the range when its start is. The response's `range` says what was resolved.
So `grain=1d` with the default range, the last hour, resolves to an empty
range and answers no rows: no day starts inside the last hour. Send `since`
at or before the start of the first bucket you want.

Use `metric_total` for a name with many series, such as one per user. It
reads one row per bucket. `metric` reads a row per bucket per series, and a
response stops at 10,000 rows and says `"truncated": true`. There is no
filter-then-sum: sum the series of a `metric` response yourself. Every value
but `value_last` adds.

| Grain | Widest range |
|---|---|
| `1m` | 6 hours |
| `5m` | 1 day |
| `15m` | 3 days |
| `1h` | 14 days |
| `6h` | 90 days |
| `1d` | 365 days |

## Health

Both services answer `GET` and `HEAD /healthz` without a signature or a client
id, and Render checks both. Point an uptime monitor at either. The pipeline's
answers 200 while it admits metrics and 503 once it is shutting down. A
health check stores nothing and is not counted as a request.

## Settings

Set on the `sqlflow-metrics-ingest` service, in the dashboard. None of these
is in `render.yaml`, on purpose: Render keeps a variable the Blueprint does
not name and rewrites one it does, so a setting made here survives a sync.

| Variable | Default | Meaning |
|---|---|---|
| `SQLFLOW_WEBHOOK_AUTH` | `hmac` | `none` accepts unsigned requests from anyone who has the URL. Anything but exactly `none` is signed. |
| `SQLFLOW_WEBHOOK_HMAC_SECRET` | | Required under `hmac`. |
| `SQLFLOW_METRIC_NAME_PREFIX` | empty | When set, a metric whose name does not start with it is dropped. Letters, digits, `.`, `_` and `-`. |
| `SQLFLOW_WEBHOOK_MAX_BODY_BYTES` | 26214400 | A larger body is refused with 413. |
| `SQLFLOW_TELEMETRY` | on | `off` sends nothing. See [What this sends](#what-this-sends). |

## Run it locally

```sh
make up        # Postgres, two pipeline instances on :10000 and :10001, the API on :8080
make test      # two pipeline instances: posts metrics, reads them back at every grain
make validate  # the configs, the generated migration, and render.yaml
```

Locally the secret is `local-secret`, the client id is `local-dev`, and a
minute closes in seconds.
