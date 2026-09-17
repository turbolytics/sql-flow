# Serve: an opt-in response cache, keyed at the grain

Written against `main` at 5c12de1 on 2026-09-17. Follows the pool spec
(`2026-09-16-serve-pool-design.md`), whose Scaling out section names a cache
as the next step.

## The problem

Every request runs its query, even when the same answer was computed a second
ago for another caller. Measured against the demo's API on 2026-09-17, at 16
concurrent callers and a pool of four sessions:

| Query | Body | Requests/s | Median |
| --- | --- | --- | --- |
| `posts_by_lang`, 24 hours, grain 5m | 120 KB | 5.6 to 6.2 | 2.4 to 2.6 s |
| `posts_by_lang`, one hour, grain 1m | 36 KB | 20.8 | 687 ms |
| `pipeline_status` | 542 B | 50.8 | 300 ms |

The callers are tabs of one page, each polling once a minute for the same
chart. Their requests are the same question and no two are equal: the page
sends `since` and `until` from `Date.now()`, to the millisecond. A cache keyed
on the request as sent would never hit.

Two properties of serve make repeated work worse than it looks:

- An abandoned query is not cancelled. ADBC's Go API has no Cancel; serve
  stops reading at the next batch once a caller gives up, which frees a
  streaming scan early and does nothing for a query like the demo's, whose
  ranking and grouping finish before the first batch exists. A caller that
  gives up leaves that work running on one of four sessions, and whatever it
  computes is thrown away.
- A burst of identical requests arrives together, at a page load spike or
  when a cached answer expires. Without coordination each of them runs.

## Scope

In:

- A response cache inside one serve process, off unless a dataset opts in.
- A cache key that treats two ranges as equal when they select the same
  buckets.
- One query for concurrent identical requests, whose result outlives the
  callers that asked for it.
- A byte bound, a TTL per dataset, metrics, and a response field that says
  what happened.
- `sqlflow rollup serve` emitting the opt-in, so the demo can adopt it.

Out, each a follow-up if something asks for it:

- A cache of rollup rows that serves settled buckets without Postgres, which
  the pool spec sketched. `posts_by_lang` ranks languages over the whole
  range, so its response does not decompose by bucket; that cache would hold
  rows and re-run the fold, which is a local replica, not a cache. It waits
  for a second instance to exist.
- HTTP caching: `ETag`, `304`, `Cache-Control: max-age`. Responses stay
  `no-store`. A token is public and a shared proxy cache keyed on the URL
  would serve one token's response to another, which is harmless today and
  would not be once datasets are authorized per token.
- A longer TTL for a range that lies wholly in the past. A re-merge after a
  replay rewrites old buckets, so "settled" is not a fact serve has.
- A cache shared between instances.
- Letting a caller bypass the cache. A token is public; a bypass is a way to
  turn the cache off for everyone.
- Rate limiting. `limits.rate_limit` stays reserved.

## Design

### Config

```yaml
serve:
  cache:                      # optional; bounds the cache, does not enable it
    max_mb: 16                # default 16

  datasets:
    - name: pipeline_status
      cache:                  # the opt-in
        ttl_seconds: 15
      sql: ...

    - name: posts_by_lang
      cache:
        ttl_seconds: 30
      range: {since: since, until: until, default: 24h}
      grains:
        5m:
          bucket: 5m          # new: how wide one bucket of this grain is
          max_range: 1d
          sql: ...
```

A dataset without a `cache` block is served exactly as today: no key is
built, no request is coalesced, and its responses are byte for byte what they
were. There is no server-wide switch that turns caching on for every dataset.

Why opt-in per dataset: the key below is exact only if the SQL reads `since`
and `until` by comparing them with bucket-aligned values. Serve cannot prove
that of arbitrary SQL. The `cache` block is the author saying so.

Why `bucket` is a key of its own: a grain's name is a label. Serve never
parses `5m` out of it, and a dataset may name its grains `fine` and `coarse`.
`sqlflow rollup serve` knows every grain's width and writes it.

Rules, checked at start and by `validate`. A dataset's rules carry
`user.config.serve_dataset` and `max_mb`'s carries `user.config.invalid`, the
codes their neighbours in `config/serve.go` already use:

- `ttl_seconds` is an integer of at least 1.
- A dataset with `cache` and `range` declares `bucket` on every grain.
- `bucket` uses the units `max_range` does, is positive, and either divides
  one day or is exactly `1d`. Buckets are aligned to the Unix epoch in UTC,
  which is what `time_bucket` and `date_trunc` produce for those widths. A
  week is refused: `date_trunc('week')` starts on a Monday and the epoch was
  a Thursday, so epoch alignment would round to the wrong boundary.
- `bucket` without `range` is refused: there is nothing for it to align.
- `max_mb` is not negative. 0 means the default, as it does for `max_rows`,
  `timeout_seconds` and `pool.size`.

`bucket` is allowed on a ranged dataset without `cache`. It is a fact about
the grain, and the generator writes it whether or not the dataset is cached.

### The key

For a cached dataset the handler builds the key after the grain is chosen:

```
dataset \0 grain \0 value(param 1) \0 value(param 2) ...
```

Params are taken in declared order. A timestamp is its Unix microseconds, an
integer is decimal, a string is length-prefixed, and an absent param is a
marker no value can spell. Query-string order, repeated unknown keys and the
spelling of a timestamp's offset do not reach the key.

For a ranged dataset, `since` and `until` are first rounded **up** to the
grain's `bucket`. The rounding is exact, not approximate. Buckets are aligned,
so for any aligned `b`:

- `b >= since` holds exactly when `b >= ceil(since)`
- `b < until` holds exactly when `b < ceil(until)`

So every request in the same bucket-wide window selects the same buckets. At
grain 5m, every tab polling a 24-hour chart within the same five minutes
builds one key.

That is the half-open range, `bucket >= $since AND bucket < $until`, and it is
the shape the `cache` block asserts. It does not hold for `bucket <= $until`:
rounding `until` up would admit the bucket that starts at the rounded value.
A dataset written that way must not opt in, and the README says so beside the
key. The generator's SQL is half-open in every grain.

The grain is chosen from the width as sent, before rounding. Rounding can
widen a range by less than one bucket, and a request that fit `max_range`
must not be refused for it.

The rounded values are what the statement binds and what `range` echoes. The
serve spec's rule stands: the echoed range says exactly what the statement
saw. This is the one visible change for a dataset that opts in, and it holds
on a miss as well as a hit, so a response never depends on who filled the
cache.

### One unit: `cache.go`

```go
// cache holds encoded results by key, bounded by bytes, each for its TTL,
// and runs one fill for concurrent requests of one key.
type cache struct { ... }

func newCache(maxBytes int64, now func() time.Time) *cache

// do returns the result for key: a stored one that has not expired, the
// result of a fill already running, or the result of fill, which it starts.
// ctx bounds this caller's wait and nothing else. fill takes no context: the
// cache never cancels one, and whoever builds fill gives it its own deadline.
// age is how long ago the result's fill started.
func (c *cache) do(ctx context.Context, key string, ttl time.Duration,
	fill func() (result, error)) (res result, out cacheOutcome, age time.Duration, err error)

type cacheOutcome string // "miss", "hit", "shared"
```

`result` is the type `encode.go` already has: a query's rows, already encoded,
pointing into no Arrow buffer.

It knows nothing of HTTP, datasets or DuckDB, and is tested without them.

Inside: a map from key to entry, a doubly linked list in recency order, a
min-heap in expiry order, a byte total, and a map of fills in flight, all
under one mutex. The mutex is never held across a fill. An entry's size is the
length of its rows plus its columns. A result larger than a quarter of
`max_mb` is returned and not stored, so one response cannot empty the cache.

Storing makes room in two steps, in this order: pop every expired entry off
the heap, then evict from the cold end of the list until the total fits.
Recency order is not expiry order, and without the heap the bound would evict
an answer still good while an expired one kept its bytes. A lookup that finds
an expired entry removes it. There is no sweeper goroutine: nothing is
reclaimed while nothing is stored, and nothing needs to be, because the bound
already holds. The gauges count what is held, so after a quiet spell they
include expired entries until the next store; the README says so.

What is stored is the query's outcome, already encoded: columns, the rows as
`json.RawMessage`, the row count, `truncated`, and the time its fill
**started**. A hit builds the envelope around those bytes, which copies them
once, and borrows no session.

### Freshness, and why nothing is invalidated

Serve is never told that Postgres changed, so no entry is invalidated. Every
entry is bounded instead: it expires `ttl_seconds` after its fill started, and
no entry has a longer life than that for any reason. The age runs from the
start of the fill because that is when the query read its data. Counted from
the end, a 2.5 s query would serve answers 2.5 s staler than the TTL says.

What can make a stored answer wrong, and what bounds each:

| Change | Bound |
| --- | --- |
| The open bucket fills in | The TTL. |
| Time passes | `ceil(until)` crosses a boundary and requests build a new key. The old key is not asked for again and expires. |
| A replay or backfill re-merges old buckets | The TTL, which is why a range in the past gets no longer one. |
| An older fill finishing after a newer one | Cannot happen: one fill per key at a time. |
| A deploy, a changed config, changed SQL | The cache is in memory and serve has no reload, so a new process starts empty. |
| A failed query | Never stored. |

Expiry compares times that carry Go's monotonic reading, so a wall-clock step
does not lengthen an entry's life.

An operator who has backfilled and wants it seen now waits one TTL or restarts
serve. There is no purge endpoint: at the TTLs this is for, the wait is
shorter than finding the endpoint's token.

### Fills are detached

`fill` runs under a context of its own: the dataset's timeout, not the
caller's context. Each caller waits on the fill under its own deadline.

- The first caller hanging up does not fail the callers sharing its fill.
- A fill whose callers have all gone still finishes, because DuckDB would
  have finished it anyway, and its result is stored. Work that was thrown
  away is now the next caller's hit.
- A caller whose deadline passes gets `query_timeout` as today. The fill
  continues and the next caller finds its result.

A fill that fails stores nothing. Every caller waiting on it gets the error
the first would have got, and the next request starts a new fill. Errors are
never cached: a Postgres restart must not be served for thirty seconds.

A fill's own timeout is `query_timeout` for its waiters. The session wait
counts toward it, as it does today.

### The handler

`queryDataset` changes in one place. Where it calls `query(...)` today, a
cached dataset calls `s.cache.do(ctx, key, ds.cacheTTL, fill)` with a `fill`
that calls `query` as today. An uncached dataset takes the old path untouched.

The response gains two fields, present only for a dataset that opted in:

```json
"cache": "hit",        // miss | hit | shared
"age_ms": 12400        // how old the result is; 0 on a miss
```

On `hit` and `shared`, `queued_ms` and `elapsed_ms` report this request's own
wait and work: both 0 for a hit, and for `shared` the wait is `queued_ms` and
`elapsed_ms` is 0. They never repeat the filling request's numbers, for the
reason the pool spec gave: a reader sent looking for a slow query that this
request did not run.

The request log line gains `cache` with the same three values, empty for an
uncached dataset.

`/v1/datasets` lists `cache.ttl_seconds` and each grain's `bucket`, so a
caller can know how stale an answer may be and how ranges are rounded.

### Tokens

The cache is shared across tokens. A token names a caller; it does not scope
data, and every token reads every dataset. Authentication runs before the
lookup, so an unauthenticated request never learns that a key is warm. If
datasets are ever authorized per token, the check stays in front of the cache
and the key needs no token, because the data does not differ by caller.

### Memory

`max_mb` defaults to 16 against a 256 MB box whose four sessions peak at
72 MiB. The demo's largest response is 120 KB, so the default holds over a
hundred of them. The bound is on stored bytes. The copy a hit makes to build
its envelope is transient and is the same allocation a miss makes today.

A caller can vary `lang`, `top` and the range to fill the cache with keys
nobody else asks for. The bound makes that churn, not growth: the worst case
is today's behavior, every request a miss.

### Metrics

- `sqlflow_serve_cache_requests_total{dataset, outcome}` with `outcome` one
  of `hit`, `miss`, `shared`.
- `sqlflow_serve_cache_bytes` and `sqlflow_serve_cache_entries`, gauges.
- `sqlflow_serve_cache_evictions_total{reason}` with `reason` one of `size`,
  `expired`.

`sqlflow_serve_query_duration_seconds` is observed only when a query ran, so
a hit does not drag the query histogram toward zero.
`sqlflow_serve_request_duration_seconds` observes every request, which is
where a hit shows.

### The rollup generator

`rollups.yml` gains one optional key on a serve dataset:

```yaml
serve:
  datasets:
    - name: posts_by_lang
      cache_ttl_seconds: 30
```

`sqlflow rollup serve` writes `bucket` on every grain always, and the `cache`
block when the key is present. `sqlflow rollup check` compares both. The
generated SQL already compares `bucket >= $since AND bucket < $until` in every
grain, checked in the demo's `serve.yml` on 2026-09-17, which is the shape the
key needs.

## Invariants

The cache's promises go in `docs/coverage/invariants.yml`, so the matrix shows
them proven or not, and a later change that breaks one fails a test that names
it. Serve has no invariants there today, and the registry has no kind for it.
Three additions, in the PR that lands the code, so no row is ever declared
without its evidence:

- `serve` joins `KINDS` and `INTEGRATION_KINDS` in
  `scripts/coverage_matrix/registries.py`, the way `manager` did.
- `docs/coverage/integrations/serve.duckdb.yml`: `kind: serve`,
  `constructed: false`, `implements: [Executor]`, `feature: cli.serve`. The
  integration is the executor because that is the part that may be swapped,
  and every invariant below must hold whatever sits under the cache. A second
  executor proves them again or is exempt with a reason.
- `cache` joins `FAMILIES`, and all seven rows carry it. The matrix draws one
  table per family with a column per integration that family touches, so
  rows filed under `resilience` or `errors` would put an empty `serve.duckdb`
  column on the sinks' tables.

Each is `verified_by: harness`. The handler tests under Tests emit
`coverage.Invariant(t, id, "serve.duckdb")`, and each test runs against the
real handler with a counting executor, never against `cache` alone: the claim
is about what a caller is sent.

| id | class | claim |
| --- | --- | --- |
| `serve.cache.bounded_staleness` | safety | A response is never built from a result whose fill started more than the dataset's `ttl_seconds` before the request arrived. |
| `serve.cache.bucket_exact` | safety | For a ranged dataset, a response from the cache carries the rows a query bound to the request's own `since` and `until` would have returned against the same data. Rounding to the bucket changes the key and never the rows. |
| `serve.cache.opt_in` | safety | A dataset without a `cache` block runs one query per request, and its response carries no `cache` or `age_ms`. |
| `serve.cache.bounded_bytes` | safety | The bytes held never exceed `max_mb`, whatever is requested. |
| `serve.cache.errors_not_stored` | safety | A request that fails stores nothing: the next request for its key runs a query. |
| `serve.cache.one_fill` | safety | Concurrent requests for one key run one query, and a caller leaving fails no other caller. |
| `serve.cache.answers_from_memory` | liveness | A second request for a key inside its TTL is answered without a query and without a session. |

The liveness row is there for the reason `registries.py` gives: every safety
row above holds for a cache that never stores anything.

`bounded_staleness` and `bucket_exact` are each proven by a test built to
fail. The first advances an injected clock across the TTL with the data
changed underneath, and fails if the expiry check is removed. The second
drives requests at both edges of a bucket window and one either side of it,
compares each cached response with an uncached dataset running the same SQL on
the unrounded values, and fails if rounding goes down instead of up or if the
SQL is changed to `bucket <= $until`. That second failure is the point: it is
the case the `cache` block asks an author to rule out, shown to be real.

`requires` is empty on all seven, as it is on every row today.

## Several instances

Each instance holds its own cache and they share nothing. Two instances fill
the same key once each per TTL, not once per request, which is already most
of what the pool spec wanted from a cache: Postgres sees a rate set by the
number of instances and distinct keys, not by the number of callers. Two
instances may answer the same request with results up to one TTL apart. A
chart polled through a load balancer can therefore step back by one fill;
`age_ms` says so.

## Tests

Unit, `go test -short`, on `cache` alone with an injected clock:

- A second `do` within the TTL is a `hit` and does not call `fill`.
- After the TTL, `do` is a `miss` and calls `fill` again.
- N concurrent `do` calls for one key call `fill` once; one reports `miss`
  and the rest `shared`.
- The first caller cancelling does not fail the others, and the result is
  stored.
- Every caller cancelling still stores the result: the next `do` is a `hit`.
- A failed fill reaches every waiter, stores nothing, and the next `do`
  fills again.
- Storing past `max_bytes` evicts the least recently used; a hit refreshes
  recency.
- A result over a quarter of the bound is returned and not stored.
- An entry expires one TTL after its fill started, not after it finished: a
  fill that takes two seconds of the injected clock leaves TTL minus two.
- With the cache full of expired entries and one live one, a store removes
  the expired ones and keeps the live one, though the live one is coldest.
- Invariants, checked after each step of a seeded random run of stores,
  lookups, expiries and clock advances: the byte total equals the sum of the
  entries' sizes and never exceeds the bound, and the map, the list and the
  heap hold the same entries.
- Run under `-race`.

Unit, on the key:

- Two ranges in one bucket window build one key; ranges either side of a
  boundary build two.
- An aligned `since` or `until` is unchanged by rounding.
- A range that fits `max_range` before rounding is served, not refused.
- Param order in the query string, and `Z` against `+00:00`, build one key.
- An absent param and an empty string build different keys.

Through the handler, with the executor counting queries:

- An uncached dataset's response has no `cache` field and its query count
  matches its request count: the control.
- A cached dataset answers two requests with one query; the second says
  `hit`, a positive `age_ms`, and `elapsed_ms` 0.
- The echoed `range` is the rounded one, on the miss and on the hit.
- A `query_failed` is not cached.
- The staleness bound, end to end: the data changes after a fill, a request
  inside the TTL still sees the old rows, and the first request past it sees
  the new ones. Removing the expiry check fails this test, which is what makes
  it a test.
- A hit borrows no session: with every session held, a warm key still
  answers.
- An unauthenticated request for a warm key is a 401.

Config and `validate`: each rule above, at the line of its key.

Generator: `bucket` on every grain; the `cache` block only with
`cache_ttl_seconds`; `rollup check` fails when either is edited by hand.

Load, recorded in the PR and not run in CI: the three rows of the table at
the top, re-measured with the cache on, from a machine the demo's owner
picks, with their say-so.
