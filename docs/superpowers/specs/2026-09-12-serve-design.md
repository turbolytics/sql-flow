# `sqlflow serve`: named SQL over HTTP, from the same config machinery as `run`

No issue yet. Verified against `main` at ae8fd28 on 2026-09-12. The ADBC
facts under "The server" were probed in Go against DuckDB 1.5.2 and
arrow-adbc 1.6.0 on 2026-09-13.

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
JSON Schema and `sqlflow config example` read them. Every type is prefixed
`Serve`, because the package already holds pipeline types with plain names.

```go
type ServeConf struct {
    Commands []SQLCommand `yaml:"commands,omitempty"`
    Serve    Serve        `yaml:"serve"`
}

type Serve struct {
    Name     string         `yaml:"name,omitempty"`
    HTTP     *ServeHTTP     `yaml:"http,omitempty"`
    Auth     ServeAuth      `yaml:"auth"`
    Limits   *ServeLimits   `yaml:"limits,omitempty"`
    Datasets []ServeDataset `yaml:"datasets"`
}

type ServeHTTP struct {
    Addr string     `yaml:"addr,omitempty"` // default 0.0.0.0:8080
    CORS *ServeCORS `yaml:"cors,omitempty"`
}

type ServeCORS struct {
    AllowedOrigins []string `yaml:"allowed_origins"`
}

type ServeAuth struct {
    Tokens []ServeToken `yaml:"tokens"`
}

type ServeToken struct {
    Name  string `yaml:"name"`
    Token string `yaml:"token"`
}

// The same block at the top level and on a dataset. A dataset's non-zero
// value overrides the top level's.
type ServeLimits struct {
    MaxRows        int             `yaml:"max_rows,omitempty"`        // default 10000
    TimeoutSeconds int             `yaml:"timeout_seconds,omitempty"` // default 10
    RateLimit      *ServeRateLimit `yaml:"rate_limit,omitempty"`
}

// Reserved. Parsed and documented so a config written now keeps its shape;
// refused with user.config.serve_reserved when any field is non-zero.
type ServeRateLimit struct {
    RequestsPerSecond float64 `yaml:"requests_per_second,omitempty"`
    Burst             int     `yaml:"burst,omitempty"`
}

type ServeDataset struct {
    Name        string                `yaml:"name"`
    Description string                `yaml:"description,omitempty"`
    Params      []ServeParam          `yaml:"params,omitempty"`
    Limits      *ServeLimits          `yaml:"limits,omitempty"`
    SQL         string                `yaml:"sql,omitempty"`
    Grains      map[string]ServeGrain `yaml:"grains,omitempty"`
}

type ServeParam struct {
    Name string `yaml:"name"`
    Type string `yaml:"type" jsonschema:"enum=string,enum=integer,enum=timestamp"`
}

type ServeGrain struct {
    SQL string `yaml:"sql"`
}
```

`Conf` stays as it is. A pipeline file and a serve file are two types with two
schemas, `config.json` and `serve.json`, both generated by `internal/schema`
from the structs and both checked by a golden test. `sqlflow validate` and
`sqlflow config validate` read the top-level keys before choosing a schema: a
file with `serve:` and no `pipeline:` is checked against the serve schema.
Every other file is checked as a pipeline, exactly as today, so a file with
both keys reports `serve` as an unknown property.

### Rules the schema cannot express

Checked by `validate` and again by `serve` at start, each a diagnostic with a
YAML position:

- At least one dataset and at least one token.
- A dataset has `sql` or `grains`, not both and not neither.
- Dataset names are unique and match `^[a-z][a-z0-9_]*$`, because they are
  URL path segments.
- Grain names match `^[a-z0-9][a-z0-9_]*$`. `5m` and `1h` start with a digit.
- Param names are unique within a dataset and match `^[a-z][a-z0-9_]*$`. No
  param is named `grain`, because `grain` selects the grain.
- Every statement uses named placeholders only. `$1` is an error, because
  DuckDB refuses to mix positional and named parameters and the server
  numbers them itself.
- Every `$name` a statement uses is a declared param, and every declared
  param is used by every statement in the dataset. A grain that ignores
  `lang` would answer a `lang` filter with unfiltered rows.
- Token names are unique. Token values are non-empty after rendering. Two
  tokens with the same value are one identity with two names, which is an
  error.
- Every `allowed_origins` entry is `http` or `https`, a host, and an optional
  port. No path, no query, no wildcard.
- `http.addr`, when set, is a `host:port`.
- `rate_limit` is zero everywhere. A non-zero value reports
  `user.config.serve_reserved`: "rate_limit is not enforced in this version".
- `max_rows` and `timeout_seconds` are not negative. Zero means the default.
  A dataset value overrides the global one.

DuckDB accepts SQL keywords as parameter names. `$from` prepares through
ADBC. The CLI's `EXECUTE q(from := …)` call syntax is what rejects it, so no
reserved-word rule exists.

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
encoder:

| Arrow | Reported |
|---|---|
| `utf8`, `large_utf8` | `VARCHAR` |
| `bool` | `BOOLEAN` |
| `int8`, `int16`, `int32`, `int64` | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` |
| `uint8`, `uint16`, `uint32`, `uint64` | `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT` |
| `float32`, `float64` | `FLOAT`, `DOUBLE` |
| `date32` | `DATE` |
| `time64` | `TIME` |
| `timestamp` with a zone, without | `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP` |
| `decimal(p, s)` | `DECIMAL(p,s)` |
| `binary`, `large_binary` | `BLOB` |
| `month_day_nano_interval` | `INTERVAL` |
| `list`, `struct`, `map` | `LIST`, `STRUCT`, `MAP` |
| `dictionary` | the value type's name; a DuckDB `ENUM` reports `VARCHAR` |

Any other Arrow type reports its Arrow name. DuckDB hands `HUGEINT` over as
`decimal(38, 0)`, so it reports `DECIMAL(38,0)`.

Values:

- A timestamp with a zone is RFC 3339 in UTC, whatever the session
  `TimeZone`: `2026-09-10T00:00:00Z`. A timestamp without a zone has no
  offset: `2026-09-10T01:02:03.456`.
- A decimal is a JSON string of its exact digits: `"12.34"`. Arrow's own JSON
  rendering goes through a float and prints `HUGEINT` as `1.7e+38`.
- A float that is `NaN` or infinite is the string `"NaN"`, `"Infinity"`, or
  `"-Infinity"`. JSON has no literal for them.
- Everything else uses Arrow's JSON rendering: dates as `YYYY-MM-DD`,
  integers as numbers, binary as base64, lists as arrays, structs as objects,
  maps as arrays of `{"key", "value"}` objects, intervals as
  `{"months", "days", "nanoseconds"}`.
- `NULL` is `null`.

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
| `404` | `not_found` | A path that is not a route. |
| `405` | `method_not_allowed` | Anything but `GET`, except `OPTIONS` for CORS. |
| `500` | `query_failed` | DuckDB returned an error. The message is DuckDB's. The SQL is the author's, so the error is the author's debugging aid. |
| `504` | `query_timeout` | The deadline passed before the query returned. |

### Headers

Every response carries `Content-Type: application/json`. Every data response
carries `Cache-Control: no-store`, so nothing between the caller and the
server holds an answer while caching is deferred.

CORS, when `cors` is configured: every response carries `Vary: Origin`. An
`OPTIONS` request gets `204`, and from an allowed origin it also carries
`Access-Control-Allow-Origin` echoing the origin,
`Access-Control-Allow-Methods: GET`, `Access-Control-Allow-Headers:
Authorization`, and `Access-Control-Max-Age: 600`. A `GET` from an allowed
origin carries `Access-Control-Allow-Origin`. An origin not in the list gets
no `Access-Control-*` header, so the browser blocks it. A preflight needs no
token, because browsers never send one. Credentials mode is never enabled.
Without a `cors` block, no CORS header is ever sent and `OPTIONS` is `405`.

### Logging

One line per request at `INFO`: `token`, `dataset`, `grain`, `status`,
`code` when non-empty, `rows`, `elapsed_ms`, `remote`. A `500` also logs the
DuckDB error at `ERROR`.

## The server

### Packages

- `internal/sqlparams/`: the placeholder scanner. It finds `$name` in SQL,
  skipping string literals, quoted identifiers, comments, and dollar-quoted
  strings, and rewrites each name to a `$N` position. Both the config rules
  and the server use it.
- `internal/config/serve.go`: the structs above, the strict loader, and the
  cross-field rules.
- `internal/serve/`: the server. `New(ctx, conf, conn, opts...) (*Server,
  error)` checks the rules and prepares every statement once.
  `Handler() http.Handler` serves the routes. `Serve(ctx, net.Listener)`
  runs it until the context ends, then shuts down gracefully. The webhook
  source's lifecycle is the model: a real `http.Server`, `Serve(ln)`, a
  shutdown timeout, port 0 in tests.
- `internal/cli/serve/`: the cobra command, registered in `root.go` beside
  `run`.

### Startup

1. Load and render the config through `config.LoadServeRendered`. Decoding is
   strict, as it is for a pipeline. Run the cross-field rules. Any violation
   exits 10 with every violation in the message.
2. Open an in-memory DuckDB. Run `commands:` through `core.InitCommands`. A
   failed `ATTACH` fails here with DuckDB's message.
3. Prepare every dataset's every statement once. DuckDB binds a statement
   when its SQL is set, so a syntax error, a missing table or view, or a
   missing column fails here as `user.sql.invalid`, naming the dataset and
   grain. The server also compares DuckDB's parameter count with the
   scanner's. A mismatch means the scanner misread the SQL, and the process
   refuses to start rather than bind values to the wrong placeholders.
4. Listen on `http.addr`. Log the address and each dataset's name and grains.
5. On SIGTERM or SIGINT: stop accepting, drain in-flight requests for up to
   5 s, wait for any query still running, close the connection, exit 0.

### Flags

| Flag | Default | Description |
|---|---|---|
| `-c`, `--config` | required | Path to the serve config, unless given positionally. |
| `--pprof` | `false` | Serve pprof on `:6060`. |

No flag overrides `http.addr`. The address is config so it validates and
renders from the environment like everything else. `--metrics` is a
follow-up: `serve` records no instruments yet, and an exporter with nothing
to export is a flag that does nothing.

### One request

1. CORS headers, then the method check, then auth. Cheap rejections before
   the lock.
2. Resolve the dataset, then the grain. Parse every query parameter against
   its declared type. Reject unknown and repeated ones.
3. Start a goroutine that takes the lock. If the request's deadline passed
   while it waited, it gives the lock back and runs nothing. Otherwise it
   builds a fresh statement from the rewritten SQL, binds the params, executes,
   reads at most `max_rows` rows into a JSON buffer, releases the reader, and
   releases the lock.
4. The handler waits for that goroutine or for the deadline, whichever comes
   first. The goroutine hands back finished bytes, so nothing writes to the
   response after the handler returns.
5. Write the body, log the line.

The lock is the same shape as `run`'s: one `sync.Mutex` around one
`adbc.Connection`. `/healthz` takes it too, under the same timeout, so a
query that holds the connection past the timeout turns the health check red.

A statement per request, not one held for the process. DuckDB's ADBC layer
plans a statement when its SQL is set and keeps that plan, and a plan can
fold table statistics into constants. `StructuredBatch` hit exactly that: a
plan built against an empty table returned nothing forever after. A served
table can change under the process, so every request plans against the data
as it is. The per-request cost is a parse and a plan. The Postgres
extension caches the attached catalog, so no round trip is added.

### Binding

DuckDB binds parameters by position. ADBC's parameter schema names them `0`,
`1`, with type `null`, and a bound Arrow record's field names are ignored.
So the server numbers the placeholders itself. At startup
`sqlparams.Rewrite` turns `$since`, `$until`, `$lang` into `$1`, `$2`, `$3`
in order of first appearance, and records the names in that order. A name
used twice gets one number. Each request builds a record with one field per
name, in that order.

The scanner skips `'…'` strings, `"…"` identifiers, `--` and `/* */`
comments, and `$$…$$` or `$tag$…$tag$` strings, so `$fake` inside a literal
is not a parameter. An `E'…'` string with a backslash-escaped quote is not
handled. The parameter-count check at startup catches a scanner that
miscounts.

A field's type comes from the declared param type, never from the value:
`string` binds `utf8`, `integer` binds `int64`, `timestamp` binds
`timestamp[us, tz=UTC]`. An absent param is a null in that typed field, so
`coalesce($since, now() - INTERVAL '6 hours')` types correctly. A held
statement re-evaluates `now()` on each execute, so a default range never
freezes at startup.

### Timeout

A timeout cannot stop DuckDB. The Go driver manager ignores the context on
`ExecuteQuery` and exposes no cancel call. So the timeout bounds the
caller's wait, not the engine's work. At the deadline the handler returns
`504`. The query keeps running, and keeps the lock, until it finishes. A
request that arrives meanwhile waits for the lock under its own deadline and
may `504` too. The README says this plainly. The row cap does bound the
work: DuckDB streams results, so releasing the reader at `max_rows` stops the
query.

### Read-only

`serve` runs the author's SQL. It does not parse it to reject writes: the SQL
is the author's, and the author controls the backend's attach options.
Through ADBC, DuckDB executes a `CREATE TABLE` or an `INSERT` in a dataset's
SQL, and it executes every statement of a multi-statement string. The README
says to attach with `READ_ONLY` and shows it. A write against a `READ_ONLY`
attachment fails at startup with DuckDB's error.

## Error codes

Two new codes in `internal/errs`, both `user.config` class, exit code 10:

- `user.config.serve_reserved`: a reserved policy is set. Message names the
  key.
- `user.config.serve_dataset`: a dataset rule failed. Message names the
  dataset, grain, and rule.

Every other rule reports `user.config.invalid`. A statement that fails to
prepare at startup reports the existing `user.sql.invalid`. A failed `ATTACH`
carries no code and exits 1, the same as `run`: a database that is not up yet
is worth a restart.

Runtime request errors are HTTP responses, not process errors.

## Tests

Unit, in `internal/serve`, against an in-memory DuckDB with a local table the
test creates. No Postgres. Every test attaches to the coverage feature
`cli.serve`:

- Every error code in the table above, asserting status, code, and that the
  message names the thing.
- Auth: missing header, wrong scheme, unknown token, known token. Two tokens,
  each resolves to its own name in the log.
- Params: each type parses its good values and rejects its bad ones. An
  absent param binds `NULL` and the `coalesce` default applies. A repeated
  param is `400`.
- Binding order: a statement that uses `$b` before `$a`, with `$a` twice,
  binds each value to its own placeholder.
- Grains: required when present, rejected when absent, unknown name lists the
  grains.
- Truncation at `max_rows`, dataset override beats global, and the next
  request on the same dataset still answers.
- Timeout: a query over `range(200000000)` runs past a 100 ms deadline and
  returns `504`. A request made right after also waits, and the lock frees
  once the query completes.
- Response shape: every field, `grain` omitted without grains, and each value
  rule above, including `HUGEINT`, a naive timestamp, a timestamp under a
  non-UTC session, and `NaN`.
- CORS: preflight from an allowed origin, preflight from a stranger, `GET`
  headers, no `cors` block means no headers and `OPTIONS` is `405`.
- Graceful shutdown: an in-flight request completes, and `Serve` returns once
  the context ends.
- `/v1/datasets` returns the SQL as written, not as rewritten.
- `/healthz` without a token.
- Memory: a loop of requests through the handler does not grow the process,
  in the style of `internal/handlers/leak_test.go`. The encoder copies out of
  Arrow buffers that DuckDB owns, which is the same boundary the handler-table
  leaks in #243 and #247 crossed.

In `internal/sqlparams`: strings, identifiers, both comment forms, both
dollar-quote forms, repeated names, a positional `$1`, and a lone `$`.

Config, in `internal/config`, `internal/schema` and `internal/validate`:

- `serve.json` golden matches the generated schema, and it accepts every
  shipped serve example.
- `validate` checks a `serve:` file against the serve schema and runs the
  rules. A pipeline file is checked exactly as before.
- Every rule reports a position. An empty token from an unset template
  variable is a warning in `validate` and an error in `serve`.

Examples: `dev/config/serve/local.table.yml` builds a full server in a sweep
test. `dev/config/serve/bluesky.postgres.yml` renders, validates, and skips
at its `ATTACH` under `enable_external_access=false`, as every Postgres
pipeline example does. Serve examples live outside `dev/config/examples/`,
because two existing sweeps decode every file there as a pipeline.

CLI, in `internal/cli/serve`: the command serves a config on port 0, answers
`/healthz`, and returns nil when its context ends. A config with a non-zero
`rate_limit` exits 10.

Release, in `tests/release/test_image.py`: the image starts `serve` against
`local.table.yml`, `/healthz` answers, `/v1/datasets` lists the dataset, and
a data request returns the rows. This is the proof that the CLI surface
exists in the artifact users pull.

Coverage: `cli.serve` in `docs/coverage/features.yml`, `requires: [unit,
release]`. `make coverage-page` clean.

Soak: the memory soak drives the pipeline, and `serve` adds nothing to that
path. The loader refactor does touch `run`'s startup, so the soak runs once
to show the pipeline unchanged. The serve path's memory gate is the leak test
above.

## Docs

- README: a row in the CLI table and a `### sqlflow serve` section: the
  config, the routes, the response, the errors, the limits, the READ_ONLY
  attach, the pushdown paragraph with the view pattern and the
  `postgres_query()` escape hatch, the connection-limit setting, the
  timeout caveat, and the one-connection caveat.
- `sqlflow config example --serve` prints the serve skeleton.
- `dev/config/serve/local.table.yml`, which runs with nothing installed, and
  `dev/config/serve/bluesky.postgres.yml`, the config above.
- CHANGELOG entry under v1.3.0.

## What breaks if this is wrong

- If the scanner misnumbers a placeholder, a request binds a value to the
  wrong column and returns wrong rows with `200`. The parameter-count check
  at startup and the binding-order test cover it.
- If the encoder reads an Arrow buffer after the reader is released, a
  response carries garbage from freed memory. Every value is encoded inside
  the read loop, and the leak test drives the path.
- If a token compares with `==`, timing leaks a token byte at a time. The
  comparison uses `subtle.ConstantTimeCompare` against every token, with no
  early return.
- If `rate_limit` is accepted and ignored, a user ships a config believing
  they are protected. The reserved-key test asserts refusal.
- If a handler writes the response from the query goroutine, a timed-out
  request races the handler's own `504`. The goroutine returns bytes, and
  the race detector runs over the timeout test.

## Build order

1. The coverage feature, the placeholder scanner, and the error codes.
2. Config structs, loader, and rules.
3. Schema generation, `validate` dispatch, `config example --serve`.
4. The encoder.
5. Query execution: prepare, bind, run under the lock and the deadline.
6. The HTTP layer: routes, auth, CORS, params, errors, logging.
7. Lifecycle and the cobra command.
8. Examples, release test, leak test, README, CHANGELOG, coverage status.
9. Release as v1.3.0.
