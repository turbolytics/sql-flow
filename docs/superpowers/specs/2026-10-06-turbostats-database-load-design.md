# TurboStats database section: load

**Date:** 2026-10-06
**Status:** Draft
**Amends:** `2026-10-05-turbostats-database-section-design.md`
**Producer:** `turbolytics/dbhealth`
**Consumer:** `turbolytics/sql-flow-control`

## The question

The section answers "is it serving, is it near its limits, are its tables
current". The next thing a person asks is **what are people doing to it**:
how busy is it right now, how much work is it doing per second, is that
work hitting memory or disk, and which queries are the work. This amendment
adds that, as facts, for every kind the section covers.

## Decisions

- **One `load` sub-section, not a Postgres one.** Every field is a thing
  Postgres, MySQL, MongoDB, Snowflake and Redshift can each say in their own
  terms, named by what it means, not where it came from. A kind that cannot
  say one omits it. The per-kind table below is the proof.
- **Two kinds of fact, said apart.** A *sample* is what was true at the
  instant of collection: sessions active now. A *rate* is work over the
  interval, computed by dbhealth from the system's own counters, two
  readings apart. Samples are named `..._now`; rates `..._per_second`. A
  reader judging "busy" uses the rates; "stuck right now" the samples.
- **Rates come from counters, not sampling.** Transactions per second is
  `(commits_now - commits_then) / seconds`, exact over the interval. The
  first interval after start has no rate, and sends none. A counter that
  went backwards (stats reset, restart) sends no rate that interval.
- **Per-query facts are opt-in and bounded.** Query text is the one thing
  here that can carry a schema's names. `queries` is sent only when the
  config asks, at most `top` entries, normalized text truncated to 200
  characters, and never literals.
- **Nothing is judged.** No "slow", "hot", "saturated". Control decides.

## The section

Added to `database`, beside `resources`:

```jsonc
"load": {
  // Samples: at the instant of collection.
  "sessions_active_now": 12,            // executing a statement
  "sessions_idle_in_transaction_now": 3,
  "sessions_waiting_now": 1,            // blocked: on a lock, a queue, a resource
  "queries_queued_now": 0,              // waiting to start; warehouses and WLM
  "longest_query_seconds": 41.2,        // the oldest statement still running

  // Rates: the interval's work, from counters two readings apart.
  "queries_per_second": 1240.5,
  "transactions_per_second": 182.4,
  "rollbacks_per_second": 0.3,
  "rows_read_per_second": 90210.0,      // returned to clients
  "rows_written_per_second": 1850.0,    // inserted + updated + deleted
  "bytes_scanned_per_second": 0,        // storage read to answer queries
  "cache_hit_ratio": 0.993,             // block or page reads served from memory, 0..1
  "deadlocks_per_second": 0,
  "temp_bytes_per_second": 0            // sorts and hashes spilling to disk
},

// Opt-in: the queries doing the work, by share of time, at most config.top.
"queries": [
  {
    "id": "a7f3…",                       // the system's query id or a hash of the text
    "text": "SELECT * FROM events WHERE customer = $1 AND ts > $2",   // normalized, ≤ 200 chars
    "calls_per_second": 310.2,
    "mean_ms": 2.4,
    "time_share": 0.31,                  // of total query time in the interval, 0..1
    "rows_per_call": 18.0
  }
]
```

And per table, in `tables[]`:

```jsonc
"dead_rows": 1203,                    // rows deleted or updated, not yet reclaimed
"seq_scans_per_second": 0.1,          // whole-table reads
"index_scans_per_second": 44.0
```

### Field rules

- Every `load` field is optional. The object is present when the kind
  provides at least one; a probe that failed sends no `load`, as it sends no
  `resources`.
- A rate is absent on the first interval and after a counter reset. It is
  never zero in those cases.
- `cache_hit_ratio` is `hits / (hits + misses)` for the interval's reads,
  not since the server started: a 99.9% lifetime ratio hides a disk-bound
  hour.
- `queries[].text` is the system's normalized form where it has one
  (Postgres `pg_stat_statements`, MySQL `DIGEST_TEXT`, MongoDB `queryShape`),
  else the text with literals replaced by `?`. Truncated to 200 characters.
  `id` is stable across intervals for the same shape.
- `queries` is ordered by `time_share` descending, at most `top` entries,
  `top` at most 20.

## What each kind can provide

| field | Postgres | MySQL | MongoDB | Snowflake | Redshift |
|---|---|---|---|---|---|
| `sessions_active_now` | `pg_stat_activity` state=active | `Threads_running` | `currentOp` active | `QUERY_HISTORY` running | `STV_INFLIGHT` |
| `sessions_idle_in_transaction_now` | state=idle in transaction | `processlist` Sleep with open trx (`innodb_trx`) | absent | absent | absent |
| `sessions_waiting_now` | `wait_event_type` = Lock | `innodb_lock_waits` | `globalLock.currentQueue` | absent | `STV_WLM_QUERY_STATE` Queued |
| `queries_queued_now` | absent | absent | `currentQueue.total` | `WAREHOUSE_LOAD_HISTORY` queued | WLM queued |
| `longest_query_seconds` | `now() - min(query_start)` active | `processlist` max `TIME` | `currentOp` max `secs_running` | running `QUERY_HISTORY` max elapsed | `STV_INFLIGHT` max |
| `queries_per_second` | `pg_stat_statements` calls Δ, else absent | `Questions` Δ | `opcounters` Δ | `QUERY_HISTORY` count/interval | `SYS_QUERY_HISTORY` count |
| `transactions_per_second` | `xact_commit` Δ | `Com_commit` Δ | absent | absent | `STL_COMMIT_STATS` Δ |
| `rollbacks_per_second` | `xact_rollback` Δ | `Com_rollback` Δ | absent | absent | absent |
| `rows_read_per_second` | `tup_returned + tup_fetched` Δ | `Innodb_rows_read` Δ | `opcounters.query` Δ | `ROWS_PRODUCED` sum | `SYS_QUERY_HISTORY` rows |
| `rows_written_per_second` | `tup_inserted+updated+deleted` Δ | `Innodb_rows_inserted+updated+deleted` Δ | `insert+update+delete` Δ | DML `ROWS_*` sum | rows inserted/deleted |
| `bytes_scanned_per_second` | absent | absent | absent | `BYTES_SCANNED` sum | `SVL_QUERY_METRICS` scan bytes |
| `cache_hit_ratio` | `blks_hit/(hit+read)` Δ | `Innodb_buffer_pool_read_requests` vs `reads` Δ | WiredTiger cache hit | `PERCENTAGE_SCANNED_FROM_CACHE` | absent |
| `deadlocks_per_second` | `deadlocks` Δ | `innodb_metrics` lock_deadlocks Δ | absent | absent | absent |
| `temp_bytes_per_second` | `temp_bytes` Δ | `Created_tmp_disk_tables` Δ (count) | absent | `BYTES_SPILLED_TO_LOCAL_STORAGE` | `SVL_QUERY_METRICS` spill |
| `queries[]` | `pg_stat_statements` | `events_statements_summary_by_digest` | `$queryStats` (7.1+) / profiler | `QUERY_HISTORY` grouped by hash | `SYS_QUERY_HISTORY` grouped |
| `tables[].dead_rows` | `n_dead_tup` | absent | absent | absent | `SVV_TABLE_INFO` unsorted/stats_off |
| `tables[].seq/index_scans_per_second` | `seq_scan`, `idx_scan` Δ | `table_io_waits_summary_by_table` | absent | absent | absent |

Two notes on the warehouses. Snowflake's `ACCOUNT_USAGE` views lag up to
45 minutes; the kind reads `INFORMATION_SCHEMA.QUERY_HISTORY()`, which is
current, and sends `queries` by shape from it. Redshift and Snowflake have
no transactions to count in the OLTP sense; they send queries and bytes
instead, which is what load means there.

## The config

```yaml
tables:
  # existing keys unchanged

load:
  enabled: true                       # the load sub-section; default true, one or two catalog reads
  queries:
    top: 10                           # per-query facts; default 0, off. Needs pg_stat_statements on Postgres
    interval_seconds: 300             # the per-query read; never every probe
```

- `load.enabled: false` for a role that may not read the stats views, or an
  operator who wants only the first section's cost.
- `queries.top` over 0 needs the kind's statement store: `pg_stat_statements`
  on Postgres, `performance_schema` on MySQL. `dbhealth validate --connect`
  says which is missing. Without it the interval sends no `queries` and one
  `collection.errors` entry naming the view.
- The per-query read is the one costly read here (`pg_stat_statements` is
  wide); it runs at `queries.interval_seconds`, and the bundle in between
  carries the last answer.

## Cost

`load` is two catalog reads on Postgres (`pg_stat_activity`, already read for
connections, and `pg_stat_database`), one on MySQL (`SHOW GLOBAL STATUS`),
one on MongoDB (`serverStatus`). The warehouses pay a query-history read
each. `queries` is one more read, at its own interval. Every read is counted
in `collection.queries` as before.

The bundle grows by about 400 bytes for `load` and up to 20 × ~350 bytes for
`queries`; the widest bundle stays under the 48 KiB ceiling with
`wire.MaxDatabaseQueries = 20`.

## Control

Nothing new is required of control beyond storing the fields, as every
section is stored. Rendering load — a rate line, the top queries — is
control's own design. `queries[].text` is shown only to the org that sent it.

## What breaks if this is wrong

- **A rate from two different processes.** dbhealth restarts between
  readings and the counters it saved are gone: no rate that interval, by the
  first-interval rule. A database restart resets the counters: the delta is
  negative, no rate. Both are the rules above; the test is a reset between
  two collections producing an absent rate, not a huge one.
- **Query text with a secret in it.** Normalized forms replace literals;
  where a kind has no normalized form, dbhealth replaces quoted strings and
  numbers with `?` before sending. A test feeds a query with a literal
  password and checks the bundle.
- **`pg_stat_statements` on a busy server** holds 5,000 rows; reading it
  every minute is the cost people fear. It runs on its own interval, five
  minutes by default, and `top` bounds what is kept.

## Testing

- `wire`: the widest bundle with `load` and 20 queries stays under 48 KiB.
- `postgres`: against testcontainers, `load` fields after two intervals with
  known work in between (N inserts → `rows_written_per_second`), the first
  interval sending no rates, a `pg_stat_reset()` between readings sending no
  rate, `queries` present only with the extension and `top` set.
- `collector`: rates from a fake's counters, including the reset case.
- e2e: against the usage-metering Postgres under the count workers' load,
  `transactions_per_second` > 0 and `cache_hit_ratio` in (0, 1].

## Out of scope

MySQL, MongoDB, Snowflake and Redshift collectors themselves: this names
what they will send so the fields are theirs from the start. Explain plans.
Per-user or per-application breakdowns. Alerts, which are control's.
