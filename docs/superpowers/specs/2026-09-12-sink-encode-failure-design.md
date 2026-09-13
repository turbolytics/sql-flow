# A value the sink cannot encode is permanent, not unreachable

Issue #233. Verified against `main` at a3da41e on 2026-09-12.

## The problem

The ClickHouse sink builds its batch client side. When the driver refuses a
value during `batch.Append`, the sink returns the error with no code. The retry
ladder retries anything uncoded, so the same value is re-encoded on every
attempt, and after the ladder is spent the failure is reported as
`system.sink.unreachable`. Nothing was unreachable, the class blames sqlflow
for a payload fault, and the exit code tells a supervisor to restart into the
same failure.

Two probes against `main` confirm the issue and find a second instance of it.

The issue's value still fails. `temporalFromString` accepts the driver's own
layouts plus a zone-less form read as UTC. `2026-09-01T12:00:00Z` matches
neither, so it goes to the driver unchanged and fails there:

```
temporalFromString(DateTime, "2026-09-01T12:00:00Z") ok=false
```

A coded user error is retried too. `retryable` lists three codes that are not
retried and retries everything else. `user.sink.type_unsupported`, which
`arrowValue` returns for a column type the sink cannot convert, is not on the
list:

```
inner error: [user.sink.type_unsupported] bad type
attempts=4 code=system.sink.unreachable
err=[system.sink.unreachable] sink still failing after 4 attempts: [user.sink.type_unsupported] bad type
```

So the fault is in two layers:

1. The sink hands an encode failure up without a code.
2. The ladder decides retryability by listing what not to retry, so any
   permanent failure it did not anticipate is retried.

Fixing only the first leaves the second to bite the next code someone adds.
Fixing only the second leaves the uncoded `append row` error retried, because
an uncoded error has to stay retryable: a driver's timeout or reset arrives
without a code, and those are what the ladder exists for.

## The change

### 1. `retryable` decides by class

A failure is retried unless another attempt cannot change the outcome. The
rule, in order:

| Error | Retried | Why |
| --- | --- | --- |
| Class `user` | No | The config, SQL or data is wrong. It fails identically every time. |
| `system.sink.write_failed` | No | The destination answered and refused. Same result next time. |
| `system.sink.unreachable` | Yes | The destination may come back. |
| Uncoded | Yes | A driver's timeout or reset arrives unclassified. The deadline bounds the cost of guessing wrong. |
| Any other `system` code | Yes | Unchanged from today. |

The implementation replaces the three-code switch with `errs.ClassOf(err) ==
errs.ClassUser` plus the `write_failed` case. `user.config.invalid` and
`user.sink.invalid`, the two user codes on today's list, are covered by the
class rule. The doc comment on `retryable` states the rule as the table
above.

This is the part that is not ClickHouse specific. Any sink that codes an
encode failure as a user error gets the right retry behavior from the ladder
with no further wiring.

### 2. A code for a value the sink cannot encode

New registry entry:

```
user.sink.encode_failed
Summary: The sink's client could not encode a result value for the destination
         column. It fails the same way on every attempt and is not retried.
Action:  Cast or format the column in the handler SQL to match the destination
         column's type. The message names the column and the value.
```

Class `user` exits 10. A supervisor reads that as terminal, which is correct:
the value is in the topic and a restart re-reads it.

The existing `user.sink.type_unsupported` stays for what it means today, a
column whose Arrow type the sink cannot convert. That is a property of the
schema and fails for every row. `encode_failed` is a property of one value in
a column whose type is supported. An operator who reads "type unsupported"
for a string that is merely formatted wrong would look at the wrong thing.

`codes.golden` gains the line. The registry is append-only and the test
enforces that.

### 3. The ClickHouse sink codes its encode failures

In `appendTables`, the `batch.Append` error is wrapped with
`user.sink.encode_failed`. The driver's message already names the column and
quotes the value, so the wrap adds only the sink name:

```
[user.sink.encode_failed] clickhouse sink: encode row: clickhouse [AppendRow]:
dt_plain parsing time "2026-09-01T12:00:00Z" as "2006-01-02 15:04:05":
cannot parse "T12:00:00Z" as " "
```

The `arrowValue` path keeps `user.sink.type_unsupported`. Both are class
`user`, so both stop the ladder after one attempt.

`PrepareBatch` and `batch.Send` keep `sinkError`, which separates unreachable
from write-failed by inspecting the error. Those two calls are where bytes
cross the network, and their classification is already right.

The sink still requeues the tables on failure. That is unchanged: whether the
batch can be dropped is the pipeline's error policy's decision, not the
sink's.

### The Iceberg sink

Iceberg is the other sink the ladder wraps. `table.AppendTable` converts and
writes in one call, and its error is returned uncoded, so an encode failure
there is retried today too. iceberg-go does not separate the two phases at
the call boundary, and the error types it returns for a conversion fault are
not distinguishable from its I/O faults without string matching. This spec
leaves Iceberg as it is and records the gap. A false "permanent" on a
transient Iceberg fault would drop a batch, which is worse than three wasted
attempts.

### Tests

Retry ladder, `internal/sinks/retry_test.go`:

- A sink failing with `user.sink.encode_failed` is attempted once. The
  returned error keeps that code.
- A sink failing with `user.sink.type_unsupported` is attempted once. This is
  the second probe above, inverted into an assertion.
- A sink failing with an uncoded error is attempted `MaxAttempts` times and
  the result is `system.sink.unreachable`. This pins the rule that uncoded
  stays retryable, so a future tidy-up cannot turn a driver timeout into a
  terminal failure.
- The existing `write_failed` and `sink.invalid` tests continue to pass
  unchanged.

ClickHouse sink, `internal/sinks/clickhouse_test.go`:

- `appendTables` against a fake `driver.Batch` whose `Append` returns the
  driver's parse error yields `user.sink.encode_failed`, and the driver's
  message survives in the chain. `driver.Batch` is an interface, so the fake
  needs no server.
- `temporalFromString` on `2026-09-01T12:00:00Z` returns `ok == false`. This
  documents that the value reaches the driver, which is the precondition for
  the encode path. It is not a claim that the value should fail; see
  follow-ups.

Registry, `internal/errs`:

- `codes.golden` regenerated. The append-only test passes.
- `ExitCode` of a `user.sink.encode_failed` error is 10.

End to end, if the ClickHouse conformance harness runs in this change: the
issue's config and message produce `user.sink.encode_failed` on the first
attempt, no retry counter increment, and exit 10. This is the acceptance the
issue describes, and it is the only test here that needs a ClickHouse.

### Acceptance

The issue's own repro, after the change:

```
Error: [user.sink.encode_failed] clickhouse sink: encode row: clickhouse
[AppendRow]: dt_plain parsing time "2026-09-01T12:00:00Z" as
"2006-01-02 15:04:05": cannot parse "T12:00:00Z" as " "
$ echo $?
10
```

One attempt. `sink_retry_count_total` does not move.

### Docs

The v1.2.1 changelog entry names the defect: an encode failure was retried
and then reported as unreachable, and any user-class sink error took the same
path. It names the new code and the retry rule.

The error code reference, wherever `errs.All()` is rendered, picks up the new
entry from the registry.

## What breaks if this is wrong

If the class rule is too broad, a transient fault that some sink coded as
`user` stops being retried. Today no sink codes a transient fault as `user`:
the user codes are config, SQL, type and, now, encode. The retry tests pin
each case, so a sink that starts doing this fails a test rather than a
pipeline.

If the ClickHouse wrap is wrong, `batch.Append` has a failure mode that a
retry could fix. It does not: `Append` validates and buffers in memory, and
the driver sends nothing until `Send`. Checked in clickhouse-go v2.48.0: both
the native `batch.Append` and `httpBatch.Append` call `block.Append` in memory
and return. Neither touches the connection.

## Follow-ups, not in this change

- Accept RFC 3339 in `temporalFromString`. The issue's value is the most
  common JSON timestamp form, and with this change it fails fast and clearly
  rather than succeeding. Whether the Python engine, which sends the string
  to the server, accepts it depends on the server's `date_time_input_format`,
  and that parity question is unverified. It is a type-matrix change with its
  own spec, not a classification fix.
- Code Iceberg's encode failures when iceberg-go exposes a way to tell them
  from I/O failures.
