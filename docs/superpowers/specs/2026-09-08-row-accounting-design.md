# Row accounting: make enrichment drops visible

Issue: #240. Related: #221 (the same silent-loss signature, in the sink),
#169 (static validation, which shares the AST parse this uses).

## The problem

A pipeline that enriches a stream by joining a dimension table has one
dangerous failure: the dimension does not load, or loads empty, and every row
takes the unmatched path. An `INNER JOIN` that drops 3 rows and one that drops
400,000 produce the same thing — a smaller target table and a green pipeline.

The join policy needs no feature. `INNER JOIN` drops, `LEFT JOIN` keeps. The
operator writes that policy in the SQL they were already writing. What is
missing is the ability to see which one happened.

Rows in versus rows out is the ratio the failure shows up in, and it is not
computable from the metrics endpoint today.

## Evidence

Verified against the code on 2026-09-08, not assumed:

| Claim | Where | Result |
|---|---|---|
| Rows written is unsummable | `internal/core/metrics.go:74` | `Int64Gauge`, recorded once per flush at `turbine.go:844` |
| `message_count` counts messages, not rows | `turbine.go:467` | `Add(ctx, int64(len(msgBatch)))` |
| Rows the SQL ran over are not counted | — | no instrument exists |
| Three call sites write to a sink | `turbine.go:809`, `managers/tumbling.go:118`, `turbine.go:641` | a counter at the flush site misses window output entirely |
| No repo example materializes a dimension | `dev/config/examples/*.yml` | every join example uses `CREATE VIEW` or `ATTACH` |
| A materialized row count is free | `duckdb_tables().estimated_size` | `CREATE TABLE dim AS SELECT * FROM range(5)` reports `5`, no scan |
| The engine can derive its reference tables | `json_serialize_sql` | returns `BASE_TABLE` nodes with `table_name`, `schema_name`, `catalog_name`, and the `join_type` |
| README undercounts the instruments | `README.md:655` | says twelve; sixteen exist (`sink_buffered_rows`, `sink_retry_count` and both `internal/webhook` instruments are undocumented) |

## The three counts

The engine publishes numbers. The operator interprets the ratio. sqlflow runs
opaque DuckDB SQL, so it cannot tell an `INNER JOIN` that dropped 400,000 rows
from a `GROUP BY` that legitimately collapsed them. Only the operator knows the
shape of their pipeline.

Four cumulative counts make every stage of the path visible:

| Count | Instrument | Stage |
|---|---|---|
| Messages consumed | `message_count` (exists) | the source delivered them |
| Rows the SQL ran over | `handler_rows_read` | the handler accepted them into `batch` |
| Rows handed to the sink | `sink_rows_accepted` | `WriteTable` buffered them |
| Rows the destination acknowledged | `sink_rows_written` | `Flush` returned nil |

Each adjacent ratio isolates one kind of loss:

- `handler_rows_read` / `message_count` — parse and DLQ loss. A message the
  handler rejected as malformed is counted as consumed and never reaches
  `batch`.
- `sink_rows_accepted` / `handler_rows_read` — whatever the SQL does. Join
  drops, `WHERE`, `GROUP BY`, window emits. This is the enrichment ratio, and
  its meaning depends on the pipeline.
- `sink_rows_written` / `sink_rows_accepted` — delivery loss. Rows that entered
  the buffer and were never acknowledged. A ratio that stays below 1 is a sink
  that is not draining, which is the #221 class as a running number.

**Every ratio is a floor, not an equality.** sqlflow is at-least-once: a crash
between the flush and the offset commit replays the batch, and the sink writes
those rows again. After any normal recovery a ratio can exceed 1.

Alert on a ratio that is low. Never alert on one that is not exactly 1, or a
healthy restart pages somebody. The README says this beside the ratios, because
an operator meets them there rather than here.

## Design

### Where the counters attach

`sinks.New` (`internal/sinks/init.go`) builds every sink the pipeline uses: the
pipeline sink (`run/root.go:293`), the DLQ (`run/root.go:51`) and each window
manager's sink (`run/managers.go:37`). A counting decorator applied inside
`New` covers all three writers.

This is the point of the design rather than an implementation convenience. A
counter added at `turbine.go:844` would report **zero rows written** for a
windowed pipeline whose entire output comes from `managers/tumbling.go:118`.

Two attributes separate the series:

- `sink` — the sink type (`kafka`, `clickhouse`, `console`), matching the
  attribute `retryCounter` already uses in `internal/sinks/metrics.go`.
- `role` — `pipeline`, `dlq` or `manager`, supplied by a new
  `WithSinkRole(string)` option at the three call sites.

Without `role`, DLQ rows sum into the same series as delivered rows and the
end-to-end ratio silently overstates delivery.

### Count on flush success, not on write

`sink_rows_written` increments only when `Flush` returns nil, by the number of
rows that flush delivered.

The invariant `sink.write.buffers_only` states that `WriteTable` reaches
nothing and only `Flush` does. Counting at write time would report rows the
destination never received — the exact defect #221 fixed. The decorator holds a
pending count, adds it to the counter on a successful flush, and leaves it
pending otherwise, which matches `sink.flush.keeps_batch`: a failed flush keeps
its rows, so those rows are counted when the retry delivers them, once.

`sink_rows_accepted` increments on a successful `WriteTable`, which is what
makes the buffered-versus-delivered ratio meaningful.

### Removing sink_buffered_rows

The `sink_buffered_rows` gauge is deleted. The two counters derive it:

```promql
sink_rows_accepted_total - sink_rows_written_total
```

Rows accepted but not yet acknowledged are the rows the sink is holding. The
counters carry the same attribute set, so the subtraction is well defined at
any scrape, and it yields a rate the gauge could not.

The identity holds while every accepted row is eventually written or still
buffered — no silent discard. That is what `sink.flush.keeps_batch` and
`sink.flush.no_hollow_success` guarantee, and #221 was the bug where it did not
hold. The derived depth is correct *because* those invariants are enforced.

A discard path added later would make the difference drift up and never return
to zero. That is a louder failure than a self-reported gauge, which would go on
reporting zero — the exact behaviour `lyingSink` models in
`conformance_test.go:327`. A derived value cannot lie about a batch the
pipeline watched it accept.

**What stays.** The `BufferedRowReporter` interface, its five implementations
and the `sink.buffer.reports_depth` invariant are untouched. That invariant is
enforced through the conformance harness (`conformance.go:200-263`), not
through the metric, so deleting the instrument costs no coverage. Only the
gauge and `recordBufferedRows` (`turbine.go:778-785`) go.

The gauge was never documented in the README, so nothing published changes.

### Rows the SQL ran over

`handler_rows_read` reports the rows the handler put into the `batch` table.
The `Handler` interface gains a method for it:

```go
// RowsRead reports the rows the last Invoke ingested into the batch table.
RowsRead() int64
```

`turbine.go` records it after `Invoke` returns.

The cheaper version — counting successful `handler.Write` calls in the pipeline
— was rejected. It counts messages, and it is wrong on a path that exists
today: when `inferSchema` or `buildRecord` fails, `Invoke` produces no table at
all, so the SQL ran over nothing while the count already moved. A metric named
for rows that reports messages, and overstates them on a failure path, is the
defect this whole design exists to remove.

Reporting zero for a failed `Invoke` is the honest answer, and it is what the
interface gives. The cost is one method on four handlers.

### Reference table row counts at startup

After `InitCommands` and `InitTables` in `run/root.go`, the engine derives the
tables the handler SQL reads and counts them.

Deriving beats declaring. The operator adds no config, and the check tracks the
SQL automatically instead of drifting from it when a table is renamed.

The steps:

1. Serialize the handler SQL with `json_serialize_sql`.
2. Walk the AST for `BASE_TABLE` nodes, collecting
   `catalog_name`/`schema_name`/`table_name`.
3. Drop every name declared in the AST's `cte_map`. DuckDB emits a CTE
   reference as a `BASE_TABLE` node, so `WITH recent AS (...) SELECT ... FROM
   recent` yields `recent` alongside the real tables. Counting it fails,
   because the CTE does not exist outside its own query, and every pipeline
   using `WITH` would log a warning at every start. A check that cries wolf is
   worse than no check. The declared names are in the same AST, so the fix
   costs one more walk.
4. Drop `batch`, and drop every name the `tables:` block created. Managed
   aggregate tables legitimately start empty; a dimension table does not.
5. Count each remaining table, timed, choosing the query by catalog:
   - Local catalog: `SELECT COUNT(*)`. Measured at 0.37s against a 417MB,
     20M-row CSV view on 2026-09-08. Cheap enough to be unconditional.
   - Attached catalog, which the AST reports as a non-empty `catalog_name`:
     `SELECT EXISTS(SELECT 1 FROM t LIMIT 1)`. A full count there is a
     sequential scan on somebody else's server, and `kafka.postgres.join.yml`
     is that shape. The exact number is not what the operator needs; the
     difference between some rows and none is.

The query runs on the signal context, so a SIGTERM during startup stops it
rather than waiting it out. `sinks.New` bounds its dial the same way
(`run/root.go:293`), and startup work in this repo is interruptible rather
than time-capped.

`dev invoke` runs it too. `internal/cli/dev.go:86` executes the same
`InitCommands`, and the developer iterating on a join against a fixture is the
person most likely to have an empty dimension table. Wiring it only into `run`
would hide the warning from the one workflow built for finding this.

The output is a log line and a gauge:

```
INFO  reference table locations: 14203 rows in 412ms
WARN  reference table locations: 0 rows, joined by handler SQL
INFO  reference table pgusersdb.users: has rows, checked in 31ms
WARN  reference table pgusersdb.users: empty, joined by handler SQL
```

An attached table reports presence rather than a number, and the log says
which it is. `reference_table_rows` is recorded only where a real count ran;
an absent series and a zero are different facts.

A count that errors logs a warning and startup continues. A table that does not
exist fails later at prepare, which is static validation's job (#169), not
this one.

`reference_table_rows` is recorded once, at startup, with a `table` attribute.
A synchronous gauge retains its last value across scrapes, so the series
reports what the table held when the pipeline started. That is a startup fact
and the description says so. Re-counting on a schedule would rescan the CSV or
cross the wire to Postgres on every interval, which is a cost the operator
never asked for.

`state_table_rows` is the precedent: same unit, same `table` attribute shape.

### Instruments

Four new instruments. The Prometheus exporter appends the unit and then
`_total`, which is why `message_count` reads as `message_count_messages_total`
today. It skips the unit when the name already contains it, so
`handler_rows_read` exports as `handler_rows_read_total` rather than repeating
the word. Instrument names therefore carry no `_total` of their own, and a name
that ends in its unit is the way to avoid a doubled suffix.

Verified on 2026-09-09 by registering every name below against the real
exporter and gathering the registry, not read off the spec.

| Instrument | Type | Unit | Attributes | Exported as |
|---|---|---|---|---|
| `handler_rows_read` | counter | rows | — | `handler_rows_read_total` |
| `sink_rows_accepted` | counter | rows | `sink`, `role` | `sink_rows_accepted_total` |
| `sink_rows_written` | counter | rows | `sink`, `role` | `sink_rows_written_total` |
| `reference_table_rows` | gauge | rows | `table` | `reference_table_rows` |

One instrument is removed: the `sink_buffered_rows` gauge, which the two
counters derive. The engine exports sixteen today and nineteen after this.

`sink_flush_num_rows` is unchanged. Changing an instrument's type renames the
exported series regardless — the gauge exports as `sink_flush_num_rows` and a
counter would export as `sink_flush_num_rows_total` — so there is no in-place
upgrade to be had, and the gauge remains a legitimate signal for batch sizing
and flush shape.

`sink_buffered_rows` is removed, so `sink_rows_buffered` is a free name. The
counter is called `sink_rows_accepted` on tense: "buffered" describes what a
sink is holding now, and a monotonic count of rows that entered the buffer is
not that. The trio reads as read, accepted, written.

## Invariant

Add to `docs/coverage/invariants.yml`, in the `resilience` family:

```yaml
- id: sink.rows.counted_on_delivery
  family: resilience
  class: safety
  applies_to: sink
  claim: >
    sink_rows_written counts a row once, when a Flush acknowledged it. A
    failed flush counts nothing; the retry that delivers those rows counts
    them.
  verified_by: harness
  requires: []
```

The subject is a break, a failed flush, a heal, and an assertion that the
counter moved once. `Break` and `Heal` already exist.

**The harness has to apply the decorator, or this invariant proves nothing.**
Conformance subjects build their sinks directly —
`NewIcebergSink(ctx, catalogName, "default.rows")` in
`sinks/conformance_pipeline_test.go:91` — and the harness wraps that in its own
`recordingSink` before handing it to a real `core.NewTurbine`
(`pipeline.go:542`). Nothing in that path goes through `sinks.New`, so the
counters would be absent and the assertion would pass against an
uninstrumented sink.

So `recordingSink` wraps with the row counters. Every subject then proves the
invariant for free, which is the reason to fix it here rather than writing one
unit test against the decorator.

The decorator stays in `sinks.New` rather than moving into `core.Turbine`. The
DLQ and the window managers own sinks the pipeline never holds, and a
Turbine-level wrap would miss both — which is the blind spot this design
started from.

The decorator sits **outside** the retry ladder, so one logical flush is one
call. The totals come out the same either way, since `retrying.WriteTable`
delegates and a failed attempt adds nothing, but the invariant needs the
position pinned to mean anything.

## Not doing

**The drop-ratio threshold (issue item 3).** The engine cannot distinguish a
dropping join from an aggregating query without knowing pipeline shape. The
numbers are published; the alert belongs where the operator's knowledge is. The
issue should be updated to say so rather than leaving the item open.

**Failing startup on an empty reference table.** The right UX is not yet known.
A warning is reversible; a refusal to start is not.

**Any new config key.** Nothing here is declared. Every fact is derived from
SQL the operator already wrote.

## Repairing the existing surface

Enumerating every instrument for this change turned up four defects in the
surface that already ships. All four are fixed here. A design whose premise is
that an operator can trust the metrics endpoint cannot leave that endpoint
misdocumented.

**1. The README documents twelve of sixteen instruments.** It omits
`sink_retry_count`, `sink_buffered_rows` (removed here), and both
`internal/webhook` instruments, which record under the `sqlflow.sources.http`
meter rather than `sqlflow`. The metrics section is rewritten to list all
nineteen, grouped, each with its exported Prometheus name beside the instrument
name. The two differ, and only the exported name is queryable.

**2. `state_commit_latency` declares its unit as `s`.** Every other latency
declares `seconds`. It becomes `seconds`.

The change is name-neutral: the exporter normalizes UCUM `s` to `seconds`
before appending it, so the series stays `state_commit_latency_seconds`.
Verified 2026-09-09 against the real exporter, because a unit edit that renamed
a series would break dashboards silently.

**3. `error_count` and `sink_retry_count` declare unit `count`, which the
exporter drops as unitless.** They stay exactly as they are.

Giving either a descriptive unit renames it — `error_count_total` becomes
`error_count_errors_total`, measured, not assumed — which breaks every
dashboard built on the current name and buys nothing. `metrics.go` gets a
comment recording that, so the inconsistency is not tidied up later by someone
who has not run the exporter.

**4. `reference_table_rows` is not a state metric.** It carries a `table`
attribute like `state_table_rows`, but it appears whenever the handler SQL
joins a table, with or without a state path. The README gives it its own group
rather than folding it in with the four that require a state path.

### The names become machine-checked

A test registers the real `core.Metrics`, `sinks` and `webhook` instruments
against a Prometheus exporter, gathers the registry, and asserts the exact set
of exported series names.

This is what makes the README table trustworthy rather than aspirational. It
also catches an OTel upgrade that changes unit suffixing, which would otherwise
rename every series in this repo with no test failing. Three of my own claims
about these names during design were wrong until I ran exactly this.

## Files touched

| File | Change |
|---|---|
| `internal/core/metrics.go` | add `HandlerRowsRead`, `ReferenceTableRows`; drop `SinkBufferedRows`; `state_commit_latency` unit `s` to `seconds`; comment why `error_count` keeps unit `count` |
| `internal/sinks/metrics.go` | add the row counters the decorator records through |
| `internal/sinks/init.go` | counting decorator, `WithSinkRole` option |
| `internal/core/turbine.go` | record `handler_rows_read` after `Invoke`; add `RowsRead` to the `Handler` interface; drop `recordBufferedRows` and its two call sites |
| `internal/handlers/*.go` | `RowsRead` on the structured, inferred-mem, inferred-disk and noop handlers |
| `internal/core/reftables.go` | new: AST walk with CTE exclusion, count, log, record gauge |
| `internal/cli/run/root.go` | call it after `InitTables`; pass `WithSinkRole` |
| `internal/cli/run/managers.go` | pass `WithSinkRole("manager")` |
| `internal/cli/dev.go` | call the reference-table check after `InitCommands` |
| `internal/conformance/pipeline.go` | `recordingSink` wraps with the row counters, and `passthroughHandler` gains `RowsRead` |
| `internal/sinks/metrics.go` | comment why `sink_retry_count` keeps unit `count` |
| `internal/cli/run/metrics_test.go` | assert the exact set of exported series names |
| `docs/coverage/invariants.yml` | `sink.rows.counted_on_delivery` |
| `README.md` | rewrite the metrics section: all nineteen, grouped, with exported names; note the `sqlflow.sources.http` meter; "Twelve instruments" becomes nineteen |

## Testing

- A failed flush followed by a successful one counts the rows once. This is the
  invariant, and it runs in the conformance harness against a real sink.
- The harness's sink actually carries the counters. Without this the invariant
  above passes against an uninstrumented sink and proves nothing.
- Across that same break and heal, `accepted - written` rises to the buffered
  row count and returns to zero. This is the derived gauge, proven against the
  case that motivates it.
- A windowed pipeline reports non-zero `sink_rows_written`. This is the
  regression the `managers/tumbling.go` blind spot would otherwise reproduce.
- DLQ rows carry `role="dlq"` and do not sum into the pipeline series.
- The AST walk returns `locations` for the `csv.mem.join.yml` handler SQL, and
  excludes `batch`. That query nests its join inside a subquery, so this also
  covers the recursive descent.
- A handler SQL with a `WITH` clause excludes the CTE name and counts nothing
  for it. This is the false-positive case, and it is the one most likely to be
  reintroduced.
- `handler_rows_read` reports zero for a batch whose `Invoke` failed schema
  inference, not the message count.
- A table declared in `tables:` is excluded from the reference set.
- A reference table with zero rows logs at warn.
- A table in an attached catalog is probed with `EXISTS`, not counted, and
  records no `reference_table_rows` series.
- A count that errors logs at warn and startup proceeds.
- The exported series names match the README exactly, asserted as a set so a
  new instrument fails the test until it is documented.
- `state_commit_latency` still exports as `state_commit_latency_seconds` after
  the unit change. This is the regression the unit edit could cause.
