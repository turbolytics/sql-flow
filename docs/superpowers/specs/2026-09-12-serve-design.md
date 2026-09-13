# `sqlflow serve`: named SQL over HTTP, from the same config machinery as `run`

No issue yet. Verified against `main` at ae8fd28 on 2026-09-12.

## The problem

A pipeline ends at a sink. The rows it wrote sit in Postgres, and the next
thing a practitioner needs is programmatic access to them: a small HTTP API a
web page or another service can call. Today that means a second stack, a
Node service, a Hasura, a PostgREST, each with its own config, auth, and
deploy. sqlflow already has the config format, the template rendering, the
validation, and the DuckDB attachment that reaches the data. It has no way to
answer a request.

`sqlflow serve` is that way. A config declares datasets, each a name and a
fixed SQL statement with typed parameters. The process attaches the backend
through the same `commands:` block a pipeline uses, then answers HTTP
requests by binding parameters into that SQL and returning JSON. Nothing in a
request becomes SQL text.

The audience is the practitioner who writes SQL and YAML and runs one binary.
Not an analyst, not a semantic layer. The SQL in the config is the API.

## Scope

In:

- A new config shape, `serve:`, in its own file beside `commands:`.
- `sqlflow serve -c serve.yml`: a request/response process that exposes only
  the datasets the config declares.
- Bearer-token auth, where a token names a client identity.
- Typed parameters bound through prepared statements: `string`, `integer`,
  `timestamp`.
- Datasets with grains: one SQL statement per grain, selected by name.
- A row cap and a query timeout, enforced.
- A rate-limit policy in the config, global and per dataset, parsed and
  refused. The shape ships so a config written today keeps working when the
  policy is enforced.
- CORS for browser callers.
- `sqlflow validate` for serve configs.
- A README section, an example config, and a release test.

Out, each a follow-up:

- Rate limiting itself.
- Response caching. Every request runs the query.
- A DuckDB connection pool. v1 holds one connection behind a mutex.
- Per-token dataset allowlists. One identity per token is enough until a
  second identity exists.
- Pushing aggregation into the backend. DuckDB pushes filters and projections
  to an attached Postgres and runs `GROUP BY` itself. The example config shows
  the practitioner's answer: a view in the backend and a filtered select over
  it.
- `POST` with a JSON body, pagination, and cursors.
- Serving from a running pipeline's DuckDB. DuckDB holds an exclusive lock on
  a state file, and a pipeline's DuckDB holds open windows, not history.
- TLS. A reverse proxy or the hosting platform terminates it.

## Decisions

Made in the design conversation. Each one closes a fork.

| Decision | Choice | Rejected |
|---|---|---|
| What `serve` is | Named queries with typed parameters. The config holds the SQL; the server binds and runs it. | A semantic layer that composes SQL from dimensions and measures. That serves analysts, and sqlflow's user writes SQL. |
| Config file | A separate file with `commands:` and `serve:`. | A `serve:` block inside a pipeline file. Two processes with different lifecycles and secrets in one file, and every pipeline config carries a block most users lack. |
| Query engine | sqlflow's own in-memory DuckDB, attached to the backend by the `commands:` block. | A Postgres driver. New dependency, one backend, and the one command in the product that is not DuckDB. |
| Token semantics | An identifier. It names a client, appears in logs, and can be revoked by deleting it. A browser page ships it in plain sight. | A secret. Nothing stops a user treating it as one by not publishing it. |
| Concurrency | One connection, one mutex, in v1. | A pool. `SET` is per connection and `ATTACH` is per database, so a pool needs a rule for which commands run where. Not a v1 question. |
| Reserved config keys | Parsed, documented in the schema, refused with an error when set. | Accepted and ignored. A config author would believe the policy was enforced. |
| Response rows | JSON objects keyed by column name. | Arrays. Smaller, but every consumer rebuilds the objects. |
| Parameter defaults | In the SQL, with `coalesce`. An absent parameter binds `NULL`. | A `default:` field. A second language for something SQL already does. |

## Config

```yaml
# serve.yml
commands:
  - name: pin the session timezone
    sql: SET TimeZone='UTC';
  - name: load postgres extension
    sql: |
      INSTALL postgres;
      LOAD postgres;
  - name: bound the postgres connections one scan may open
    sql: SET pg_connection_limit = 4;
  - name: attach postgres
    sql: |
      ATTACH '{{ SQLFLOW_POSTGRES_URI }}' AS pg (TYPE POSTGRES, READ_ONLY);

serve:
  name: bluesky-demo-api

  http:
    addr: "0.0.0.0:{{ SQLFLOW_SERVE_PORT|default('8080') }}"
    cors:
      allowed_origins:
        - https://turbolytics.io

  auth:
    tokens:
      - name: bluesky-demo-page
        token: "{{ SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO }}"

  limits:
    max_rows: 10000
    timeout_seconds: 10
    rate_limit:
      requests_per_second: 0
      burst: 0

  datasets:
    - name: pipeline_status
      description: First and latest minute, last write, totals. One row.
      sql: |
        SELECT first_bucket, latest_bucket, last_write_at,
               minutes_observed, total_posts
        FROM pg.pipeline_status

    - name: posts_by_lang
      description: Posts per bucket per language.
      params:
        - {name: since, type: timestamp}
        - {name: until, type: timestamp}
        - {name: lang,  type: string}
      limits:
        rate_limit:
          requests_per_second: 0
      grains:
        5m:
          sql: |
            SELECT bucket, lang, posts
            FROM pg.posts_per_5m_by_lang
            WHERE bucket >= coalesce($since, now() - INTERVAL '12 hours')
              AND bucket <  coalesce($until, now())
              AND lang = coalesce($lang, lang)
            ORDER BY bucket, lang
        1h:
          sql: |
            SELECT bucket, lang, posts
            FROM pg.posts_per_hour_by_lang
            WHERE bucket >= coalesce($since, now() - INTERVAL '7 days')
              AND bucket <  coalesce($until, now())
              AND lang = coalesce($lang, lang)
            ORDER BY bucket, lang
```

### Structs

In `internal/config/serve.go`. Every field carries a doc comment, because the
JSON Schema and `sqlflow config example` read them.

```go
// ServeConf is a whole serve file.
type ServeConf struct {
    Commands []SQLCommand `yaml:"commands,omitempty"`
    Serve    Serve        `yaml:"serve"`
}

type Serve struct {
    Name     string    `yaml:"name,omitempty"`
    HTTP     HTTP      `yaml:"http"`
    Auth     Auth      `yaml:"auth"`
    Limits   *Limits   `yaml:"limits,omitempty"`
    Datasets []Dataset `yaml:"datasets"`
}

type HTTP struct {
    Addr string `yaml:"addr,omitempty"` // default 0.0.0.0:8080
    CORS *CORS  `yaml:"cors,omitempty"`
}

type CORS struct {
    AllowedOrigins []string `yaml:"allowed_origins"`
}

type Auth struct {
    Tokens []Token `yaml:"tokens"`
}

type Token struct {
    Name  string `yaml:"name"`
    Token string `yaml:"token"`
}

type Limits struct {
    MaxRows        int        `yaml:"max_rows,omitempty"`        // default 10000
    TimeoutSeconds int        `yaml:"timeout_seconds,omitempty"` // default 10
    RateLimit      *RateLimit `yaml:"rate_limit,omitempty"`
}

// RateLimit is reserved. Parsed and documented so a config written now keeps
// its shape; refused with user.config.invalid when any field is non-zero.
type RateLimit struct {
    RequestsPerSecond float64 `yaml:"requests_per_second,omitempty"`
    Burst             int     `yaml:"burst,omitempty"`
}

type Dataset struct {
    Name        string           `yaml:"name"`
    Description string           `yaml:"description,omitempty"`
    Params      []Param          `yaml:"params,omitempty"`
    Limits      *DatasetLimits   `yaml:"limits,omitempty"`
    SQL         string           `yaml:"sql,omitempty"`
    Grains      map[string]Grain `yaml:"grains,omitempty"`
}

type DatasetLimits struct {
    MaxRows        int        `yaml:"max_rows,omitempty"`
    TimeoutSeconds int        `yaml:"timeout_seconds,omitempty"`
    RateLimit      *RateLimit `yaml:"rate_limit,omitempty"`
}

type Param struct {
    Name string `yaml:"name"`
    Type string `yaml:"type" jsonschema:"enum=string,enum=integer,enum=timestamp"`
}

type Grain struct {
    SQL string `yaml:"sql"`
}
```

`Conf` stays as it is. A pipeline file and a serve file are two types with two
schemas, `config.json` and `serve.json`, both generated by `internal/schema`
from the structs and both checked by the golden test. `sqlflow validate` and
`sqlflow config validate` read the top-level keys before choosing a schema:
`pipeline:` selects the pipeline schema, `serve:` the serve schema, both or
neither is `user.config.invalid`.

### Rules the schema cannot express

Checked by `validate` and again by `serve` at start, each a diagnostic with a
YAML position:

- A dataset has `sql` or `grains`, not both and not neither.
- Dataset names are unique and match `^[a-z][a-z0-9_]*$`, because they are
  URL path segments.
- Grain names are unique within a dataset and match the same pattern.
- Param names are unique within a dataset, match the same pattern, and are
  not DuckDB reserved words. `$from` fails to parse; the check names the
  word and suggests renaming.
- Token names are unique. Token values are non-empty after rendering. Two
  tokens with the same value are one identity with two names, which is an
  error.
- Every `allowed_origins` entry is a scheme plus host plus optional port, no
  path, no wildcard.
- `rate_limit` is zero everywhere. A non-zero value reports
  `user.config.invalid`: "rate_limit is not enforced in this version".
- `max_rows` and `timeout_seconds` are positive when set. A dataset value
  overrides the global one.

A token value rendered from an unset template variable is empty, and
`validate` already demotes an unsupplied variable to a warning so CI without
secrets still passes. `serve` does not demote it: an empty token at start is
`user.config.invalid`.

## HTTP contract

All routes are `GET`. Any other method is `405`.

| Route | Auth | Returns |
|---|---|---|
| `/healthz` | none | `200` with `{"status":"ok"}` when `SELECT 1` answers on the connection, `503` otherwise. |
| `/v1/datasets` | bearer | Every dataset: name, description, params, grains with their SQL. |
| `/v1/datasets/{name}` | bearer | Rows. `grain` is required when the dataset has grains and rejected when it does not. Every other query parameter must be a declared param. |

### Auth

`Authorization: Bearer <token>`. The token is compared in constant time
against every configured token. A missing header, a malformed header, or an
unknown token is `401` with `unauthorized`. The matched token's `name` goes
in the request log; the token value never does. Tokens are not accepted in
the query string.

### Parameters

| Type | Accepts | Binds as |
|---|---|---|
| `string` | Any value. | `VARCHAR` |
| `integer` | Base-10, optional sign, fits in `int64`. | `BIGINT` |
| `timestamp` | RFC 3339. A value without an offset is `400`. | `TIMESTAMP WITH TIME ZONE` |

A declared param absent from the request binds `NULL`. A param present more
than once is `400`. A query parameter that is not `grain` and not a declared
param is `400` with `unknown_param`, so a misspelled name fails rather than
silently binding `NULL`.

### Response

`200`:

```json
{
  "dataset": "posts_by_lang",
  "grain": "1h",
  "columns": [
    {"name": "bucket", "type": "TIMESTAMP WITH TIME ZONE"},
    {"name": "lang",   "type": "VARCHAR"},
    {"name": "posts",  "type": "BIGINT"}
  ],
  "rows": [
    {"bucket": "2026-09-10T00:00:00Z", "lang": "en", "posts": 102340}
  ],
  "row_count": 1,
  "truncated": false,
  "elapsed_ms": 41
}
```

`grain` is omitted for a dataset without grains. `columns[].type` is the
DuckDB type name, mapped from the Arrow field type by a fixed table in the
encoder: `utf8` to `VARCHAR`, `int32` to `INTEGER`, `int64` to `BIGINT`,
`float64` to `DOUBLE`, `bool` to `BOOLEAN`, `date32` to `DATE`, `timestamp`
with a zone to `TIMESTAMP WITH TIME ZONE` and without to `TIMESTAMP`,
`decimal` to `DECIMAL(p,s)`, `list` to `LIST`, `struct` to `STRUCT`. An Arrow
type outside the table reports its Arrow name. Timestamps serialize as RFC 3339 in
UTC with microsecond precision when non-zero. Dates as `YYYY-MM-DD`. Integers
as JSON numbers. `HUGEINT` and `DECIMAL` as strings, because JSON numbers lose
them. Binary as base64. Lists and structs as JSON arrays and objects. `NULL`
as `null`.

`truncated: true` means `max_rows` cut the result. The status stays `200`:
the rows returned are correct, there are just more of them.

`/v1/datasets`:

```json
{
  "datasets": [
    {
      "name": "posts_by_lang",
      "description": "Posts per bucket per language.",
      "params": [{"name": "since", "type": "timestamp"}],
      "grains": {"5m": {"sql": "SELECT ..."}, "1h": {"sql": "SELECT ..."}}
    },
    {
      "name": "pipeline_status",
      "description": "...",
      "params": [],
      "sql": "SELECT ..."
    }
  ]
}
```

### Errors

One shape:

```json
{"error": {"code": "unknown_grain", "message": "dataset posts_by_lang has no grain 15m; grains: 1h, 5m"}}
```

| Status | Code | When |
|---|---|---|
| `400` | `unknown_param` | A query parameter no param declares. |
| `400` | `invalid_param` | A value that does not parse as its type. The message names the param and the type. |
| `400` | `missing_grain` | The dataset has grains and the request has no `grain`. |
| `400` | `unknown_grain` | The dataset has no grain by that name. The message lists the grains. |
| `401` | `unauthorized` | No token or an unknown one. |
| `404` | `unknown_dataset` | No dataset by that name. |
| `405` | `method_not_allowed` | Anything but `GET`, except `OPTIONS` for CORS. |
| `500` | `query_failed` | DuckDB returned an error. The message is DuckDB's. The SQL is the author's, so the error is the author's debugging aid. |
| `504` | `query_timeout` | The deadline passed before the query returned. |

### Headers

Every response carries `Content-Type: application/json`. Every data response
carries `Cache-Control: no-store`, so nothing between the caller and the
server holds an answer while caching is deferred.

CORS, when `cors` is configured: an `OPTIONS` request from an allowed origin
gets `204` with `Access-Control-Allow-Origin` echoing the origin,
`Access-Control-Allow-Methods: GET`, `Access-Control-Allow-Headers:
Authorization`, and `Access-Control-Max-Age: 600`. A `GET` from an allowed
origin gets `Access-Control-Allow-Origin` and `Vary: Origin`. An origin not in
the list gets no CORS headers. Credentials mode is never enabled. Without a
`cors` block, no CORS header is ever sent.

### Logging

One line per request at `INFO`: `token`, `dataset`, `grain`, `status`,
`code` when non-empty, `rows`, `elapsed_ms`, `remote`. A `500` also logs the
DuckDB error at `ERROR`.

## The server

### Packages

- `internal/config/serve.go`: the structs above and the cross-field rules.
- `internal/serve/`: the server. `New(conf, conn, lock, logger) (*Server,
  error)` prepares every statement and returns an `http.Handler`.
  `Serve(ctx, net.Listener) error` runs it with graceful shutdown. The
  webhook source's lifecycle is the model: a real `http.Server`, `Serve(ln)`,
  a shutdown timeout, an address option for port 0 in tests.
- `internal/cli/serve/`: the cobra command, registered in `root.go` beside
  `run`.

### Startup

1. Load and render the config through `config.LoadRendered` with the serve
   type. Run the schema check and the cross-field rules. Any fault is
   `user.config.invalid` and exit code 10.
2. Open an in-memory DuckDB with `duckdb.Open`. Run `commands:` through
   `core.InitCommands`. A failed `ATTACH` fails here with DuckDB's message.
3. Prepare every dataset's every grain. A syntax error, a missing table, or a
   reserved parameter name fails here, naming the dataset and grain.
4. Listen on `http.addr`. Log the address and each dataset's name and grains.
5. On SIGTERM or SIGINT: stop accepting, drain in-flight requests for up to
   5 s, close the connection, exit 0.

### Flags

| Flag | Default | Description |
|---|---|---|
| `-c`, `--config` | required | Path to the serve config, unless given positionally. |
| `--metrics` | off | Metrics exporter. `prometheus` serves `/metrics` on `:8000`, as `run` does. |
| `--pprof` | `false` | Serve pprof on `:6060`. |

No flag overrides `http.addr`. The address is config so it validates and
renders from the environment like everything else.

### One request

1. Method check, CORS, auth. Cheap rejections before the lock.
2. Resolve the dataset, then the grain. Parse every declared param. Reject
   unknown params.
3. Take the lock. Bind the params. Execute with a context deadline of the
   dataset's or the global `timeout_seconds`.
4. Stream Arrow records into the encoder row by row, stop at `max_rows`, set
   `truncated`. Encode while the record is alive: string and binary columns
   alias Arrow buffers that the reader frees on release, which is the rule the
   debug API already follows.
5. Release the reader, release the lock, write the body, log the line.

The lock is the same shape as `run`'s: one `sync.Mutex` around one
`adbc.Connection`. `/healthz` takes it too, so a slow query delays the health
check by at most the timeout.

### Binding

The config author writes `$since`. DuckDB's prepared statements accept named
parameters. ADBC's `Bind` takes an Arrow record. Whether DuckDB's ADBC layer
matches record fields to `$name` by name or only to `$1` by position is the
first thing the implementation plan verifies, in Go, before any other task.
If by name: the record's fields are the declared params. If by position: at
prepare time the server asks DuckDB for the statement's parameter order and
builds the record in that order. The config surface is the same either way.

A `NULL` bind for a param never sent in the request needs the record field to
be nullable and typed. The bind type comes from the declared param type, not
from the value, so `coalesce($since, now() - INTERVAL '6 hours')` types
correctly when `$since` is `NULL`.

### Timeout

The context deadline stops the handler waiting and returns `504`. Whether it
stops DuckDB depends on ADBC exposing statement cancellation through the
driver manager. The plan verifies this beside binding. If cancellation works,
the connection is free when the `504` is written. If it does not, the query
runs to completion, the lock stays held, and the README says so: a timeout
bounds the caller's wait, not the engine's work. Either way the `504` is
honest.

### Read-only

`serve` runs the author's SQL. It does not parse it to reject writes: the SQL
is the author's, and the author controls the backend's attach options. The
README says to attach with `READ_ONLY` and shows it. A write in a dataset's
SQL against a `READ_ONLY` attachment fails at prepare or at the first
request with DuckDB's error, which is the right place.

## Error codes

Two new codes in `internal/errs`, both `user.config` class, exit code 10:

- `user.config.serve_reserved`: a reserved policy is set. Message names the
  key.
- `user.config.serve_dataset`: a dataset rule failed. Message names the
  dataset, grain, and rule.

Runtime request errors are HTTP responses, not process errors. A failed
`ATTACH` or prepare at start uses the existing `user.config.invalid` or the
engine's error class, whichever the underlying call returns.

## Tests

Unit, in `internal/serve`, against an in-memory DuckDB with a local table
built by the test's own `commands:`. No Postgres. Each test attaches to the
coverage feature `cli.serve` or a named invariant:

- Every error code in the table above, one test each, asserting status, code,
  and that the message names the thing.
- Auth: missing header, wrong scheme, unknown token, known token. Two tokens,
  each resolves to its own name in the log.
- Params: each type parses its good values and rejects its bad ones. An
  absent param binds `NULL` and the `coalesce` default applies. A repeated
  param is `400`. `from` as a param name fails validation.
- Grains: required when present, rejected when absent, unknown name lists the
  grains.
- Truncation at `max_rows`, dataset override beats global.
- Timeout: a `SELECT` over a generated series large enough to run past a
  100 ms deadline returns `504`. A second test asserts the cancellation fact
  the plan's first task established: either the connection answers `SELECT 1`
  within one second of the `504`, or it does not until the query completes.
  The README states whichever the test asserts.
- Response shape: every field, `grain` omitted without grains, timestamp and
  date and null and list encoding.
- CORS: preflight from an allowed origin, preflight from a stranger, `GET`
  headers, no `cors` block means no headers.
- Graceful shutdown: an in-flight request completes, a new connection is
  refused.
- `/v1/datasets` returns the SQL as written.
- `/healthz` without a token.

Config, in `internal/config` and `internal/validate`:

- `serve.json` golden matches the generated schema.
- `validate` picks the schema by top-level key. A file with both keys and a
  file with neither each report `user.config.invalid` with a position.
- Every cross-field rule reports a position.
- `dev/config/examples/serve/bluesky.postgres.yml` renders and validates in
  the example sweep. The sweep's `enable_external_access=false` means the
  `INSTALL` fails fast rather than reaching the network, as it does for every
  other Postgres example.

Release, in `tests/release/test_image.py`: the image starts `serve` against a
config with a local table, `/healthz` answers, `/v1/datasets` lists the
dataset, a data request returns the row. This is the proof that the CLI
surface exists in the artifact users pull.

Coverage: `cli.serve` in `docs/coverage/features.yml`, `requires: [unit,
release]`. `make coverage-page` clean.

Soak: `serve` touches ADBC, Arrow, and DuckDB, so the memory soak runs once
against a `serve` process under a request loop. A leak in the row encoder is
the failure to look for, and it is the same shape as the handler-table leaks
fixed in #243 and #247.

## Docs

- README: a row in the CLI table and a `### sqlflow serve` section: the
  config, the routes, the response, the errors, the limits, the READ_ONLY
  attach, the pushdown paragraph with the view pattern and the
  `postgres_query()` escape hatch, the connection-limit setting, and the two
  cancellation and pool caveats.
- `sqlflow config example --serve` prints the serve skeleton.
- `dev/config/examples/serve/bluesky.postgres.yml`: the config above, the way
  `bluesky.postgres.windowed.yml` is the pipeline example.
- CHANGELOG entry under the next minor.

## What breaks if this is wrong

- If DuckDB's ADBC layer binds neither by name nor exposes parameter order,
  the config surface has to change to positional `$1` parameters. The plan's
  first task finds this before any handler is written.
- If the row encoder aliases an Arrow buffer past release, a response carries
  garbage from a freed buffer. The soak and the encoding tests cover it.
- If a token compares with `==`, timing leaks a token byte at a time. The
  auth test asserts `subtle.ConstantTimeCompare` is on the path.
- If `rate_limit` is accepted and ignored, a user ships a config believing
  they are protected. The reserved-key test asserts refusal.

## Build order

1. Verify binding and cancellation in Go, against the pinned DuckDB. Record
   the answer in the plan.
2. Config structs, schema generation, `validate` dispatch, cross-field rules.
3. `internal/serve`: prepare, bind, execute, encode, with unit tests.
4. Auth, CORS, limits, errors, logging.
5. The cobra command, graceful shutdown, `--metrics`, `--pprof`.
6. Example config, README, release test, coverage entry, CHANGELOG.
7. Soak.
8. Release as the next minor.
