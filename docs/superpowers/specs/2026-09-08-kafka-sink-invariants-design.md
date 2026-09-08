# Kafka sink invariants

Status: design, 2026-09-08.

## The contract

sqlflow delivers at-least-once, end to end. A row reaches the destination one or
more times and is never lost. Deduplication is the consumer's job.

`turbine.go:849` already says so for the crash window: "a crash between the
flush and the commit replays the batch, so an external sink may see a duplicate
-- recoverable -- while state and offsets stay consistent." The tumbling
manager says so by construction: a window stays in DuckDB until its flush
succeeds, so a failed poll republishes it.

This design writes that contract into the invariant matrix, and fixes the two
Kafka sink defects that are not about delivery semantics at all.

## The problem

The invariant matrix proves three of the ten resilience invariants for
`sink.kafka`. The harness that proves them asserts the wrong thing.

`sinkVerdicts` ends by requiring the destination to hold exactly one row:

```go
if keeps.failure == "" && (len(got) != 1 || got[0]["id"] != int64(1)) {
```

That is exactly-once. Against an at-least-once stack it fails sinks that are
behaving correctly, and it is why the Kafka cell needed investigating at all.

Measured on 2026-09-08 against `confluentinc/confluent-local:7.5.0` behind
toxiproxy, with a clean flush added before the fault so the topic already
exists, the sink read back `[1 2 2]`. `Client.Flush(ctx)` returns `ctx.Err()`
while franz-go keeps the record buffered and goes on retrying it, so both the
sink and franz-go deliver a copy. No row was lost. Under at-least-once that
read-back is correct, and the assertion above is what is wrong.

Checked against loss instead: `Flush` keeps every row whose promise did not fire
clean (`!acked[i] || didFail`), so a row the destination never took stays
buffered. **The Kafka sink has no loss defect.**

### Defect 1: no flush in production can time out

`Flush` honours its context, and no caller gives it one that expires.
`managerCtx` is `context.WithCancel(context.Background())`
(`internal/cli/run/root.go:350`). The consume loop's drain uses
`context.WithoutCancel(ctx)`, which strips the deadline along with the
cancellation. `kgo.RecordDeliveryTimeout` defaults to off. So `client.Flush`
against a hung broker blocks until something cancels the run, while franz-go
retries underneath it.

The sink's own comment overstates what honouring a context buys. A context with
no deadline stops nothing.

A windowed pipeline shows the cost. Kafka source, 60s tumbling window, Kafka
sink, 10s poll interval, one closed window holding `id=101`, and a broker that
hangs:

1. **t=0.** Tick. `collectClosed` returns `[101]`, `Flush` produces it, and
   `client.Flush(managerCtx)` blocks.
2. **t=10,20,30.** The ticker fires into a goroutine still inside `Poll`, so the
   ticks are dropped. No window publishes, and nothing is logged. The pipeline
   looks healthy.
3. **t=30.** SIGTERM. `stopManagers()` cancels `managerCtx`, the flush returns
   `context.Canceled`, and the manager runs its final poll on
   `context.Background()` -- which can neither be cancelled nor time out.
4. Broker still down: shutdown hangs in `managerWG.Wait()` until the supervisor
   sends SIGKILL.

Step 2 is the serious one. An outage silently stops every window from
publishing, and no metric moves.

The consume loop reaches the same hang by the shorter route: a failed flush is
fatal (`turbine.go:422`, `451`, `513`, `525`), but its drain still blocks
forever on `context.WithoutCancel`.

### Defect 2: the error carries no code

`Flush` returns a bare `fmt.Errorf`. `errs.CodeOf` reads
`system.internal.unexpected`, so the process exits 1 rather than 12
(`ExitSinkUnreachable`) and the error metric is labelled for a bug rather than
for a dead broker. `isUnreachable` already returns true for the underlying
error, so the classification is available and unused. ClickHouse is the only
sink that calls `sinkError`.

### Defect 3: no Prober

A pipeline whose broker list is wrong starts clean and fails at the first flush.
That is the failure ClickHouse's `Probe` exists to prevent, and the console and
sqlcommand exemption -- "reaches nothing that can be absent" -- does not fit a
sink that crosses a network.

### Defect 4: Batch() never clears

After a successful flush it still returns the last table written.

## Decisions

Made in the design conversation. Each one closes a fork.

| Decision | Choice | Rejected |
|---|---|---|
| Delivery semantics | At-least-once, written into the harness. Duplicates pass; loss and reordering fail. | Exactly-once assertions; per-sink special cases. |
| The duplicate | Not a defect. No sink change. | Produce-once ownership tracking; `AbortBufferedRecords`. |
| The blocked flush | `kgo.RecordDeliveryTimeout`, so franz-go releases a record it cannot deliver. | Leaving the bound to callers that have never supplied one. |
| `Batch()` | Delete it from `core.Sink` and every implementation. Retire `sink.batch.reports_buffer`. | Fix the implementations; restore the Python diagnostic that called it. |
| Classification | Wrap Kafka's error with `sinkError`, and stop there. No `Reject` seam. `sink.error.classifies` stays declared and unproven. | Build the seam and prove the invariant for all five subjects. |

### Why at-least-once belongs in the harness

An invariant that is stricter than the contract fails correct code, and the
first person to hit it will loosen the sink rather than the assertion. Writing
the contract down once, in the place that judges every sink, is what stops that.

Three claims change meaning, and none of them weakens:

- `flush.keeps_batch` asserts no row is lost across a failed flush and a retry.
  A repeat is allowed. Today's exact-equality check is replaced by a
  subsequence check.
- `flush.preserves_order` asserts rows appear in `WriteTable` order, repeats
  permitted. `[A, B, B, C]` passes. `[A, C, B]` fails.
- `flush.no_hollow_success` is untouched. It is a claim about loss -- `Flush`
  returns nil only when every row was acknowledged -- and duplication cannot
  satisfy it.

A sink that delivers nothing still fails all three, which is the property that
matters.

### Why classification stops at one line

The invariant's stated reason -- "so the retry ladder retries the right ones" --
does not apply to Kafka. `retriesHelp` wraps only ClickHouse and Iceberg.

Two things in the tree read a sink error code: the ladder at
`internal/sinks/retry.go:171`, which never sees a Kafka error, and
`internal/core/turbine.go:578`, which turns it into a metric label and two log
fields. `errs.ExitCode` adds a third reader, where uncoded gives 1 and coded
gives 12 -- and `exit.go` documents both as retryable, so a supervisor behaves
identically.

Classification therefore changes Kafka's observability and no control flow.
Wrapping the error is one line and worth doing. Building a `Reject` seam for
five subjects to prove the claim is not, and the invariant-matrix design already
deferred it for the same reason. `sink.error.classifies` stays missing rather
than exempt: the claim is true, unproven, and honest about it.

### Why `Batch()` goes

It has no caller. `internal/conformance/pipeline.go:781` is the only call site
outside tests, and it is a forwarder in the harness's own double. No non-test
caller has ever existed in the Go tree, checked at `7d27a2b`, `cd6303b` and
`d193f6a`. `internal/managers/tumbling.go` calls `WriteTable` and `Flush` only,
so the comment at `internal/sinks/kafka_test.go:150` naming the tumbling-window
manager as its reader is wrong.

The method came from the Python engine, where `sqlflow/pipeline.py:139` logged
the rows a sink still held when `flush()` raised. The Go rewrite carried the
method and dropped the caller.

## The harness sequence

`internal/conformance` grows a pre-warm step, an at-least-once comparison, and
one extra phase. Every judgment is another assertion on one sequence, which is
the pattern the invariant-matrix design set.

The pre-warm matters even though the duplicate it exposed turned out to be
legal. A destination in its steady state is the one production runs against,
and a sink that loses rows only after its first successful flush is invisible
to a sequence that never performs one.

### Phase 1: delivery

| Step | Action | Judges |
|---|---|---|
| 0 | `Flush` with nothing buffered. Returns nil, destination untouched. | `flush.empty_is_noop` |
| 1 | `WriteTable(A)`, `Flush`. Returns nil, destination holds `A`. | -- |
| 2 | `WriteTable(B)`. Destination has not gained `B`, depth 1. | `write.buffers_only`, `buffer.reports_depth` |
| 3 | `Break`. `Flush` under a bounded context fails with `ctx.Err()`. Destination unchanged, depth still 1. | `flush.honours_context` |
| 4 | `WriteTable(C)` while broken. Depth 2. | `buffer.reports_depth` |
| 5 | `Flush` again, still broken. Fails again rather than returning nil. | `flush.no_hollow_success` |
| 6 | `Heal`. `Flush` returns nil, depth 0. | -- |
| 7 | `A`, `B` and `C` each appear at least once, and `[A, B, C]` is a subsequence of the read-back. | `flush.keeps_batch`, `flush.preserves_order` |

Step 7 fails a sink that dropped a row and a sink that delivered out of order.
It passes one that delivered a row twice.

### Phase 2: start and stop

A fresh sink from `New`, with the destination broken. `Probe` must fail inside a
bound; a prober that retries blows the bound rather than passing quietly. Then
`Close` twice.

Both are found by type assertion, so `SinkSubject` gains no field in this
design. A sink that implements neither is skipped, and `integrations.yml`
carries the exemption -- the same two-statements-must-agree rule `Break`
already uses.

## Two subjects cannot witness order today

`SinkSubject.ReadBack` is documented to return rows "in delivery order". The
ClickHouse and sqlcommand subjects both read back with `ORDER BY id`
(`conformance_test.go:111`, `conformance_local_test.go:117`), which sorts rather
than observes. A sink that delivered `[C, B, A]` would read back `[A, B, C]` and
pass -- a false positive of exactly the kind this work removes.

PR B fixes both subjects before it judges `preserves_order`. Each conformance
table gains an arrival column the read-back orders by; the subject owns its DDL,
so this touches no sink. The Kafka and console subjects already observe order:
Kafka reads by offset, console reads the bytes it wrote.

## The Kafka sink

Three changes. None touches how `Flush` buffers, because that part is correct.

`kgo.RecordDeliveryTimeout` at construction. franz-go releases a record it has
not delivered inside the timeout, failing every record buffered for that
partition together, which keeps the retry gapless. The blocked flush becomes a
bounded one, so a poll fails in seconds instead of stalling, later ticks run,
and the outage becomes visible. This is the fix for defect 1, and it works
where honouring the context cannot: the drain's context has no deadline to
honour.

`sinkError` around the returned error, so the code, the metric label and the
exit status say "unreachable destination" rather than "unexpected internal".
That is defect 2.

`Probe(ctx)` wrapping `client.Ping`, for defect 3.

The conformance topic is created with one partition. franz-go's default
`UniformBytesPartitioner` switches partition every 64 KiB, and Kafka orders
within a partition only, so a multi-partition topic makes `preserves_order`
untestable rather than false.

The retry ladder is unaffected. `retriesHelp` excludes Kafka because franz-go
retries a produce internally, and that reasoning still holds.

## The column when this lands

Nine of the ten cells, with one honest gap:

| Invariant | Outcome |
|---|---|
| `write.buffers_only` | proven, re-proven against a warm destination |
| `flush.keeps_batch` | proven, against the contract rather than a stricter one |
| `buffer.reports_depth` | proven |
| `flush.empty_is_noop` | proven, step 0 |
| `flush.honours_context` | proven, step 3 |
| `flush.no_hollow_success` | proven, step 5 |
| `flush.preserves_order` | proven, step 7 |
| `probe.fails_start` | proven, phase 2 |
| `lifecycle.close.idempotent` | proven, phase 2 |
| `error.classifies` | **missing.** The wrap is fixed and unit tested; the claim needs a `Reject` seam this design does not build. |
| `batch.reports_buffer` | retired |

## Registries

`invariants.yml` loses `sink.batch.reports_buffer`. A retired invariant is not
an invariant, and the commit that removes it carries the reason.

The `keeps_batch` and `preserves_order` claims gain a sentence naming
at-least-once, so nobody re-tightens them later.

`integrations.yml` gains exemptions only where a subject genuinely cannot
exercise a claim: a sink with no `Probe` or no `Close`. Every exemption names
the test that proves the reason, as the existing entries do.

`scripts/coverage_matrix.py` needs no change.

## What this design does not know

The new assertions judge clickhouse, iceberg, sqlcommand and console, not only
kafka. This design does not predict what they do. The plan runs the extended
sequence against all five subjects, records what goes red, and decides
fix-or-exempt per cell against that output. Guessing now would produce
exemptions written before the evidence.

## Rollout

Three PRs. Each merges green.

**A. Delete `Batch()`.** Remove the method from `core.Sink` and every
implementation: six sinks, the retry decorator, the unwired `internal/local`
console, the harness doubles, and the test call sites. Retire the invariant.
Mechanical and independent of everything below.

**B. At-least-once, the pre-warm, and phase 1.** Replace the exact-equality
check with the subsequence check and record the contract in `invariants.yml`.
Fix the two subjects that sort their read-back. Extend the sequence and add a
harness double per new judgment, including one that loses a row and one that
reorders, so the loosened assertion is shown to still catch both. Then fix the
sink: `RecordDeliveryTimeout` and the `sinkError` wrap, each with a named test.

**C. Start and stop.** Add the Kafka `Probe`, then phase 2 for
`sink.probe.fails_start` and `lifecycle.close.idempotent`.

## Out of scope

- Exactly-once anywhere. The stack is at-least-once and this design commits to
  it rather than working around it.
- The `Reject` seam and `sink.error.classifies`. Its correctness value is
  concentrated on ClickHouse and Iceberg, the sinks the ladder wraps, so it
  belongs with the work that hardens those.
- The type axis. `type.*` is `verified_by: typetable`, a mechanism the
  ClickHouse type-table design builds separately.
- Source invariants. `source.*` needs a `Sources` harness that does not exist.
- Filling any `requires` field. Enforcement is legal only once every non-exempt
  integration has a subject, and this work does not finish that for every
  invariant it touches.
- Restoring the flush-failure diagnostic that `Batch()` once served.
- Making `RecordDeliveryTimeout` configurable. It ships with a default.
