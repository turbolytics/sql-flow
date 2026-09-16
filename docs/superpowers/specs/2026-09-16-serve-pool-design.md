# `sqlflow serve`: a pool of sessions behind an executor interface

Serve answers one request at a time. Every request waits on one mutex around
one DuckDB connection, so a slow query makes every request behind it wait and
throughput is whatever one connection can do.

This replaces that mutex with a pool of sessions, behind an interface that
names what serve needs from a backend — prepare a statement, run it, return
Arrow — rather than handing the request path a DuckDB connection. It adds the
metrics that size the pool, because serve records none today.

The wire contract does not change, except that a response gains `queued_ms`
and `elapsed_ms` stops counting time spent waiting.

## The problem

Measured against the Bluesky demo on Render on 2026-09-16, after rollup tables
cut the query itself to 13 ms. `ab -n 200 -l`, one client machine, against
`posts_by_lang` over the last 24 hours:

| Clients | Throughput | p50 | p95 | Failures |
|---|---|---|---|---|
| 1 | 5.8/s | 164 ms | 251 ms | 0 |
| 4 | 12.2/s | 303 ms | 418 ms | 0 |
| 8 | 12.1/s | 613 ms | 855 ms | 0 |
| 16 | 13.4/s | 1,191 ms | 1,318 ms | 0 |
| 32 | 13.2/s | 2,328 ms | 2,682 ms | 0 |

Throughput stops climbing at four clients and latency grows in proportion
after that, which is what a serialized server looks like. The same request
reports `elapsed_ms` of 13 when idle and 945 to 1,219 under sixteen clients:
the query is unchanged and the rest is queueing, counted as if it were work.

Before the rollups the same test failed 66% of requests at sixteen clients.
Cheaper queries raised the ceiling from 1.1/s to 13/s. Only concurrency raises
it again.

## Scope

In:

- An `Executor` interface: prepare a statement, acquire a session, run,
  release. One implementation, DuckDB over ADBC.
- A pool of sessions with a bounded wait, and the stats it publishes.
- Session setup: which config commands run once, which run per session, and
  the timezone serve pins itself.
- `queued_ms` in the response, and `elapsed_ms` narrowed to the query.
- Prometheus metrics on the existing HTTP server, off unless configured.
- `/healthz` that does not queue behind queries.

Out:

- Caching. The next spec, and it belongs behind the same interface. See
  "Scaling out": it is also what makes more than one instance worth running.
- A second executor implementation. The interface exists so one can be added
  without touching the request path; writing it now would be guessing.
- Per-dataset pools, priorities, rate limiting.
- Making the JSON encoding pluggable. Arrow is sqlflow's in-process format
  and the encoder is shared. See "Arrow".

## Depends on

Nothing unreleased. It builds on `serve` as of v2026.09.16.

## Decisions

| Decision | Choice | Rejected |
|---|---|---|
| The seam | `Executor`, `Session`, `Statement` interfaces in serve, one DuckDB implementation. | Passing `adbc.Connection` through the request path, as today. It welds one driver into every layer, which is what made the Postgres sink's move off DuckDB's postgres extension (#290) expensive. |
| The interchange | `array.RecordReader`. Serve owns the encoder and the row cap. | A `Result` of already-encoded JSON per backend: the four documented rendering rules would be re-implemented per backend and could drift. A generic `Next()`/`Value() any` cursor: `any` discards the precision that the decimal and HUGEINT bug needed. |
| Default pool size | 4. | 1, which keeps every deployment serialized until its author finds the setting. Sizing from `NumCPU`, which makes memory vary by host and load tests hard to compare. |
| Session timezone | Serve runs `SET TimeZone='UTC'` on every session it opens. | Trusting the config's `commands` to reach each session. Measured: `SET TimeZone` is session-scoped, so a second connection inherits the host zone. |
| Config commands | Run once, on a setup session. | Running them per session: `ATTACH` is database-wide and re-attaching errors. |
| Waiting for a session | Counts toward the dataset's timeout; a request that never gets one is `504 query_timeout`, as today. | A new status for a busy pool. The caller's experience is a timeout either way, and `queued_ms` plus the metrics say which it was. |
| Stopping abandoned work | `readRows` takes the request's context and stops at a batch boundary, then releases the reader, which ADBC documents as equivalent to cancel. Postgres `statement_timeout` in the ATTACH string bounds the scan. | Leaving abandoned queries to run to completion, as today. With one connection that cost the next request its turn; with a pool it can hold every session at once. |
| Metrics | `GET /metrics` on the existing server, no token, off unless `serve.metrics.enabled`. | A second port, as `sqlflow run` uses: Render routes one port, so the demo could not scrape it. |

## Arrow

Arrow is sqlflow's in-process storage format. Sources, handlers and sinks
already move batches as Arrow, and ADBC returns it natively, so a DuckDB
executor converts nothing.

The interface returns `array.RecordReader` and serve keeps one encoder. That
is what holds the response contract together: zoned timestamps render as UTC,
naive ones carry no offset, decimals are exact digit strings, and NaN and
infinity are strings. Each of those exists because Arrow's own rendering was
wrong for JSON, and each is documented in the README. One encoder means one
place they are true.

It also keeps one subtle rule in one place. `readRows` encodes every value
while its record is current, because a string value aliases a buffer the
driver frees on the next `Next()`. A backend that returned rows some other way
would have to rediscover that.

What this costs, stated plainly:

- A backend that is not Arrow-native pays a conversion. A pgx implementation
  would go pgx to Arrow to JSON where it could go straight to JSON. ADBC ships
  a Postgres driver, so that is likely moot, and the Postgres sink already
  carries a pgx type table to borrow from if not.
- The next backend inherits Arrow's quirks whether its own types have them or
  not.
- `columns[].type` is a SQL type name reconstructed from an Arrow type, so a
  Postgres `text` would surface as `VARCHAR`. Cosmetic, and it is published
  contract.
- Buffer lifetimes are part of the interface: a session's reader must stay
  valid until the caller finishes reading it. The interface says so.

## The interfaces

In `internal/serve/executor.go`. Nothing here names DuckDB or ADBC.

```go
// Executor runs a serve config's datasets. Serve holds one; each request
// borrows a Session from it.
//
// This is an interface because the engine underneath may change. The Postgres
// sink had to leave DuckDB's postgres extension for pgx when that extension
// read the whole target table on every write (#290), and that move was
// expensive because the driver was welded into the write path.
type Executor interface {
	// Prepare checks one dataset statement and returns a handle for it.
	// Called at startup for every statement, so a dataset that cannot run
	// fails the start rather than the first request.
	Prepare(ctx context.Context, spec StatementSpec) (Statement, error)

	// Acquire borrows a session. It blocks until one is free, ctx ends, or
	// the executor closes, and returns ctx.Err() or ErrClosed in those cases.
	Acquire(ctx context.Context) (Session, error)

	// Stats reports what the pool gauges publish.
	Stats() Stats

	// Close waits for every borrowed session to be released, then refuses
	// later acquires and closes the sessions.
	Close()
}

// Session runs one statement at a time. A caller holds it for one request.
type Session interface {
	// Run executes st with one request's values. The returned reader stays
	// valid until it is released, and values read from it may alias buffers
	// the reader owns, so a caller encodes each value before advancing.
	//
	// values maps a declared param name to a string, int64 or time.Time. A
	// name the map does not carry binds NULL.
	Run(ctx context.Context, st Statement, values map[string]any) (array.RecordReader, error)

	// Release returns the session. It is safe to call twice; the second is a
	// no-op, so a deferred Release beside an early return cannot double-free.
	Release()
}

// Statement is one statement an Executor has checked. Its contents belong to
// the executor; serve only names it in errors.
type Statement interface {
	// Where names the statement: "dataset posts_by_lang grain 1h".
	Where() string
}

// StatementSpec is what an Executor needs to prepare a statement.
type StatementSpec struct {
	Dataset string
	// Grain is empty for a dataset without grains.
	Grain  string
	SQL    string
	Params []config.ServeParam
}

// Stats is the pool's state at one instant, for the gauges and for the
// health endpoint. How long callers wait is a histogram, not a gauge, so it
// is not here.
type Stats struct {
	// Size is every session the executor holds.
	Size int
	// InUse is the sessions currently borrowed.
	InUse int
}
```

`maxRows` is deliberately not in `Run`. The cap is serve's contract, not the
backend's, and serve applies it while encoding, where it already stops the
reader early.

`serve.New` takes an `Executor` in place of an `adbc.Connection`. The handler
acquires, runs, encodes, releases. `internal/serve/duckdb.go` is the only file
in the package that imports ADBC.

## The pool

`executor.go` also holds the pool mechanics, because queueing and its
measurement are the same whatever runs the SQL. A DuckDB executor embeds it.

Sessions live in a buffered channel of capacity `size`. `Acquire` selects on
the channel, `ctx.Done()` and a closed channel. `Release` returns the session
unless the pool is closing.

`Close` is the part worth stating. It stops accepting acquires, then waits for
every borrowed session to come back before closing any of them. A session
closed while a query runs on it takes the process down, and nothing outside
the reading goroutine can stop that query (see "Stopping the work"), so
waiting is the only option. The wait is bounded by the drain deadline the HTTP
server already applies.

### Config

```yaml
serve:
  pool:
    # Sessions the executor holds. Each is a DuckDB connection; a concurrent
    # query on one costs about 15 MiB. 0 means the default, 4.
    size: 4
  metrics:
    # Serve GET /metrics on the same listener, without a token. Off by
    # default: that port is public, and the metrics name every dataset.
    enabled: false
```

Rules, reported as `user.config.invalid` at the key's path:

- `pool.size` is not negative, and not greater than 64. A larger pool on a
  small box is a memory failure waiting to happen, and the ceiling is a
  number someone can raise with evidence.

## Sessions and the commands block

Measured on DuckDB 1.5.2 through ADBC on 2026-09-16, two connections to one
in-memory database with a Postgres attached:

| Behaviour | Result |
|---|---|
| `ATTACH` on connection 1 | Connection 2 queries `pg.*` without attaching. |
| `ATTACH` the same alias again | `Binder Error: database with name "pg" already exists`. |
| `SET TimeZone='UTC'` on connection 1 | Connection 2 reads `America/New_York`, the host zone. |
| `SET pg_connection_limit = 4` on connection 1 | Connection 2 reads 4. |
| A table created on connection 1 | Visible from connection 2. |
| Two slow queries, one per connection | 200 ms, against 377 ms run one after the other. |

So the database is shared and most setup is database-wide, but session
settings are not. Startup therefore:

1. Opens the database and one setup session.
2. Runs the config's `commands` once, on that session. `ATTACH`, `INSTALL`,
   `LOAD` and global `SET`s all take effect for every session.
3. Prepares every dataset statement once, to fail the start on a broken one.
4. Opens `size` sessions and runs `SET TimeZone='UTC'` on each.

Serve pins the timezone rather than trusting `commands` to reach each session.
Serve's contract is UTC — the encoder already renders zoned timestamps as UTC
whatever the session says — but the session zone still decides what
`date_trunc` and a naive cast mean inside a dataset's SQL. A pooled request
that evaluated those in the host zone would return wrong buckets from a
correct config.

A config that needs some other per-session setting has no way to ask for one.
That is deliberate for this version: the timezone is the case that breaks
correctness, and a `session_commands` block can follow when something needs
it.

## Timeouts, and what a response says

Waiting for a session counts toward the dataset's timeout. A request that
waits past its deadline gets `504 query_timeout` and never runs, which is the
honest answer: the caller waited as long as it agreed to.

A response gains one field:

```json
{"dataset": "posts_by_lang", "grain": "5m", "queued_ms": 0, "elapsed_ms": 13}
```

`elapsed_ms` becomes the query alone. Today it silently includes the wait,
which is why the Render measurement above reads 945 ms for a 13 ms query —
a number that sent the reader looking for a slow query that did not exist.

### Stopping the work, not just the waiting

A timeout bounds the response. It does not, today, bound the work, and with a
pool that gap costs more than it did with one connection: a session held by a
query nobody is waiting for is a session the next request cannot have. The
worst case for occupancy is `pool.size` multiplied by the longest query that
can actually run, not by `timeout_seconds`.

ADBC's Go API has no `Cancel`. Checked against arrow-adbc v1.6.0 on
2026-09-16: neither `adbc.Statement` nor the driver manager exposes one,
though the C API has had `AdbcStatementCancel` since ADBC 1.1.0. What the Go
interface does say is this, on `ExecuteQuery`:

> Since ADBC 1.1.0: releasing the returned RecordReader without consuming it
> fully is equivalent to calling AdbcStatementCancel.

So the reader is the cancel. There is no method another goroutine can call,
but the goroutine that holds the reader can stop.

Three bounds, in the order they bite:

1. **Between batches.** `readRows` takes the request's context and stops at
   the first batch boundary after it ends, then releases the reader, which
   cancels. That bounds abandoned work at one batch rather than one query.
   It does not help inside a single `Next` that never returns.
2. **In Postgres.** Most of a request is the scan of the attached database,
   and a libpq connection string carries
   `options='-c statement_timeout=30000'`, so the ATTACH can cap it
   server-side. This is the only one of the three that Postgres enforces
   rather than sqlflow hoping, and it is the one that covers the dominant
   cost. The README says so where it documents attaching.
3. **`max_rows`.** Already stops a reader early, and the rollup bounds mean
   a well-formed config never reaches it.

What none of them bound is a single DuckDB operator that runs long before
yielding a batch — a sort or an aggregation over more rows than a dataset
should be reading. The answer there is the row bounds from the rollup spec,
not a timeout.

## Metrics

`GET /metrics`, Prometheus text format, on the listener that already serves
the datasets, when `serve.metrics.enabled` is true. No token: it carries no
row data, and the port is already public. It is off by default because the
metric labels name every dataset and grain.

| Metric | Type | Labels | Why |
|---|---|---|---|
| `sqlflow_serve_requests_total` | counter | `dataset`, `grain`, `code` | Rate and error mix. `code` is the error code, or `ok`. |
| `sqlflow_serve_request_duration_seconds` | histogram | `dataset`, `grain` | What a caller experiences. |
| `sqlflow_serve_query_duration_seconds` | histogram | `dataset`, `grain` | The work alone, so a slow backend is distinguishable from a full pool. |
| `sqlflow_serve_session_wait_seconds` | histogram | — | The sizing signal. A p99 that climbs while query duration is flat says the pool is too small. |
| `sqlflow_serve_sessions_in_use` | gauge | — | Pinned at `size` means saturated. |
| `sqlflow_serve_sessions_total` | gauge | — | So a dashboard can draw in-use against it without hardcoding the config. |

Six instruments, no more. Every one answers a question the Render test raised
and could not: was that second spent working or waiting, and how many sessions
would have removed it.

Buckets for the two duration histograms and the wait: 1 ms doubling to about
16 s, so both a 13 ms query and a request that hit the 10 s timeout land in a
bucket rather than the overflow.

## Health

`/healthz` acquires a session, runs `SELECT 1`, and releases it. Today it
waits on the same mutex as a query, so under load it queues with everything
else and a supervisor can kill a server that is merely busy.

With a pool it takes the health timeout as its deadline and answers `503` with
`{"status":"busy"}` when no session comes free in time, distinct from `503`
`{"status":"down"}` when the query itself fails. Busy is not dead, and an
operator reading a restart loop needs to know which one it was.

## Memory, and why 4

DuckDB's `memory_limit` is one budget for the database, shared by every
session, so concurrency divides it rather than multiplying it. Resident memory
is not bounded by it: the limit covers DuckDB's buffer manager, not the Arrow
results, the Go heap, or what the allocator keeps.

Measured on macOS, DuckDB 1.5.2, `memory_limit='128MB'`, N sessions each
running the demo's fold at once:

| Sessions | Widest the grain ladder allows (365 buckets × 170 languages) | Unbounded (2,016 buckets × 170 languages) |
|---|---|---|
| 1 | 52 MiB | 100 MiB |
| 4 | 99 MiB | 279 MiB |
| 8 | 146 MiB | every query failed, out of memory |

Idle sessions cost nothing measurable: 27 MiB whether one or eight are open.
The cost is per concurrent query, roughly 15 MiB each.

Two things follow. Four sessions fit a 256 MB box with room, which is the
default. And the bounds from the rollup spec are what make that true: without
them the same four sessions needed 279 MiB and eight failed outright, every
concurrent request failing together as they fought over one budget. A pool
without a bound on what a request may read turns a slow server into a broken
one.

These numbers are macOS, and `Maxrss` is bytes there and KiB on Linux. The
first implementation task repeats the measurement in the release container and
fixes the default against that, changing 4 if the number says so.

## Postgres fan-out

One DuckDB scan of an attached Postgres opens up to `pg_connection_limit`
connections. With a pool, the worst case may be `size × pg_connection_limit`
against a database that also carries the pipeline's writer. `pg_connection_limit`
is global, so it does not scale with the pool by itself, but whether the
attachment's connections are shared across sessions or held per session is not
something this spec should assume.

`TestIntegrationServePool_PostgresBackendsStayBounded` settles it: run a pool
of 8 against a testcontainers Postgres, drive concurrent scans, and read
`pg_stat_activity`. The README then documents the real relationship, and the
demo sets `pg_connection_limit` from it.

## Scaling out

A pool scales one process. The question it raises is whether the next step is
more processes, so this section records what is true today, before a cache
exists to change it.

Serve is stateless: an in-memory DuckDB, no durable local state, no session
affinity, tokens from config, and every request self-contained. Instances need
no coordination, and startup is already safe for several at once, because each
runs the migration script and that script takes an advisory lock. So more
instances is a deployment change, not a code change.

Adding them buys real capacity, because real work happens in serve. A request
splits between an index range scan in Postgres, which ships the rows, and the
fold in DuckDB — `dense_rank` over a partition, then `GROUP BY` — plus JSON
encoding. If the fold ran in Postgres, another instance would add nothing.

Two things bound it, and both are shared:

- **Connections.** The worst case is `instances × pool.size × pg_connection_limit`
  against a database that also carries the pipeline's writer. At the defaults
  that is up to 16 per instance, which exhausts a small Postgres within
  single-digit instances. The exact number waits on the integration test
  above.
- **Repeated work.** Instances share nothing, so each pulls the same hot rows
  for the same popular ranges. Ten instances means ten times the scanning and
  transfer for identical queries: database capacity spent to buy serve
  capacity, which is backwards.

So scaling out today trades a bottleneck for a worse one. What changes that is
the cache, which is the argument for building it next: a settled bucket never
changes, so an instance holding settled buckets answers most requests without
touching Postgres, the database sees only the live tail, and instances become
nearly independent. The cache is not primarily a latency optimisation. It is
what makes horizontal scale pay.

Until then the order under load is: size the pool from
`sqlflow_serve_session_wait_seconds`, then give the database more capacity or
a replica, and only then add instances.

## Tests

Unit, `go test -short`:

- `TestCliServe_PoolRunsQueriesConcurrently`: N slow statements across a pool
  of N finish in about the time of one, not N. With `pool.size: 1` the same
  test takes N times as long, which is the control.
- `TestCliServe_EverySessionIsUTC`: a dataset whose SQL returns
  `current_setting('TimeZone')`, driven until every session has answered,
  reports UTC from all of them. Building sessions without the pin fails it
  wherever the host is not UTC, so the test sets a non-UTC `TZ`.
- `TestCliServe_AcquireReturnsWhenTheRequestGivesUp`: a cancelled request
  releases its place, and a waiter behind it proceeds.
- `TestCliServe_CloseWaitsForBorrowedSessions`: Close does not return while a
  query runs, and acquires after it are refused.
- `TestCliServe_ReleaseTwiceIsSafe`.
- `TestCliServe_AFullPoolTimesOutAsQueryTimeout`: the code is `query_timeout`
  and `queued_ms` carries the wait.
- `TestCliServe_QueuedMsSeparatesWaitFromWork`: under a full pool, a fast
  query reports a small `elapsed_ms` and a large `queued_ms`.
- `TestCliServe_AbandonedWorkStopsAtABatch`: a request that times out over a
  reader with many batches stops reading rather than draining it, and its
  session comes back before the query would have finished. Dropping the
  context check from `readRows` fails it.
- `TestCliServe_HealthzIsBusyNotDownWhenThePoolIsFull`.
- `TestCliServe_MetricsAreOffUnlessEnabled`, and with it enabled, `/metrics`
  carries all six instruments after one request.
- `TestCliServe_PoolSizeRules`: negative and over-64 sizes report at their
  path.
- The existing `TestCliServe_DoesNotLeakNativeMemory` runs against a pool.

Integration, against testcontainers Postgres:

- `TestIntegrationServePool_PostgresBackendsStayBounded`, above.

The feature `cli.serve` already requires unit and release coverage; the pool
adds `integration`.

## What breaks if this is wrong

| If | Then | Caught by |
|---|---|---|
| A session misses the UTC pin | A correct config returns buckets in the host zone, from some requests and not others. | `EverySessionIsUTC`, with a non-UTC `TZ`. |
| `commands` runs per session | Startup fails on the second `ATTACH`. | Any pool test against a config with an attachment. |
| Close closes a session mid-query | The process dies during a deploy, mid-request. | `CloseWaitsForBorrowedSessions`. |
| A cancelled request leaks its session | The pool shrinks under load until it deadlocks. | `AcquireReturnsWhenTheRequestGivesUp`. |
| Abandoned queries run to completion | A handful of slow requests hold every session while nobody waits for them, and the pool is a queue for work no one wants. | `AbandonedWorkStopsAtABatch`, and `statement_timeout` on the attachment. |
| The pool is sized past the box | Concurrent queries fail together, out of memory, rather than queueing. | The container measurement in task 1; `memory_limit` is the backstop. |
| `queued_ms` and `elapsed_ms` are swapped | An operator tunes the wrong thing, as this spec's own Render numbers nearly did. | `QueuedMsSeparatesWaitFromWork`. |

## Build order

1. Measure per-session resident memory in the release container, and fix the
   default. The rest of the spec assumes 4.
2. The interfaces and the pool, with the DuckDB implementation behind them.
   `serve.New` takes an `Executor`. No behaviour change at `size: 1`.
3. Session setup: `commands` once, the UTC pin per session.
4. `queued_ms`, `elapsed_ms` narrowed to the query, and `readRows` stopping
   at a batch boundary when the request is gone.
5. Metrics, and the config that enables them.
6. `/healthz` busy.
7. The Postgres fan-out measurement, and what the README says about it.
8. README and CHANGELOG.

The demo then sets `pool.size`, enables metrics, and the Render test above
runs again as the check: throughput should rise with the pool, and
`session_wait_seconds` should say whether it is sized right.
