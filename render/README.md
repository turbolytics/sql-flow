# Deploy to Render: a metrics pipeline

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/turbolytics/sql-flow)

One click creates a Postgres, a sqlflow pipeline that accepts metrics over a
signed webhook and aggregates them by minute, and an HTTP API that reads them
back. Everything it deploys is in this directory, and
[`render.yaml`](../render.yaml) at the repository root declares it.

It is a paid deploy: a `0.1c-256mb` Postgres and two `0.5c-512mb` web
services, in `virginia`. Render shows the price before you confirm.

## Deploy

Render asks for one value:

| Prompt | Type |
|---|---|
| `SQLFLOW_WEBHOOK_HMAC_SECRET` | Any long random string, such as the output of `openssl rand -hex 32`. Keep it: you sign requests with it. |

A blank secret fails the deploy, on purpose. The pipeline writes to your
database, and it will not start unsigned unless you set
`SQLFLOW_WEBHOOK_AUTH=none` yourself.

When the deploy finishes, copy two things from the Render dashboard: the URL
of `sqlflow-metrics-ingest` and of `sqlflow-metrics-api`, and the value of
`SQLFLOW_SERVE_CLIENT_ID` on the API service.

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
export CLIENT_ID=<SQLFLOW_SERVE_CLIENT_ID>
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

Postgres keeps the same five values at 5m, 15m, 1h, 6h and 1d, by triggers
that run inside the pipeline's own write. [`rollups.yml`](rollups.yml)
declares them, `migrations/0003_rollups.sql` is generated from it with
`make rollups`, and `make validate` fails when it has drifted.

## The API

Every request except `/healthz` and `/metrics` sends `?client_id=`. It names
the caller in the log. It is an identifier, not a secret.

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

## Settings

Set on the `sqlflow-metrics-ingest` service.

| Variable | Default | Meaning |
|---|---|---|
| `SQLFLOW_WEBHOOK_AUTH` | `hmac` | `none` accepts unsigned requests from anyone who has the URL. |
| `SQLFLOW_WEBHOOK_HMAC_SECRET` | | Required under `hmac`. |
| `SQLFLOW_METRIC_NAME_PREFIX` | empty | When set, a metric whose name does not start with it is dropped. Letters, digits, `.`, `_` and `-`. |
| `SQLFLOW_WEBHOOK_MAX_BODY_BYTES` | 26214400 | A larger body is refused with 413. |

## Run it locally

```sh
make up        # Postgres, the pipeline on :10000, the API on :8080
make test      # posts metrics and reads them back at every grain
make validate  # the configs, the generated migration, and render.yaml
```

Locally the secret is `local-secret`, the client id is `local-dev`, and a
minute closes in seconds.
