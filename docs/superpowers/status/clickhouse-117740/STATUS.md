# ClickHouse docs PR #117740: re-verification status

Resumed and re-run on 2026-10-09 against `v2026.10.08`. Paused first on
2026-09-18 against `v2026.09.17.1`. Everything below was produced by running
it, on the versions named.

**The PR:** [ClickHouse/ClickHouse#117740](https://github.com/ClickHouse/ClickHouse/pull/117740),
the SQLFlow community integration page. Draft. Head `fe238499` on
`turbolytics/ClickHouse:docs-sqlflow-integration`. One file, 511 lines, plus a
navigation entry. Three review threads are open, at lines 372, 443 and 444.

**The goal:** pin the newest release, re-verify every claim on it, fix the
reviewer's three open majors, and take the page out of draft.

**The standard the user set, 2026-09-18:** every claim needs test evidence and
a configuration to reproduce it, with versions. The page does not meet that
bar today and neither does the repo. See Pending, item 3.

**The draft page** is [sqlflow.mdx](sqlflow.mdx) beside this file. It applies
every finding below. Nothing has been pushed to the ClickHouse fork.

## Versions

| Component | Measured on | Page currently says |
| --- | --- | --- |
| SQLFlow | `turbolytics/sql-flow:v2026.10.08` | v1.1.0 |
| ClickHouse self-hosted | 26.8.1.2041, timezone UTC | 26.8 |
| ClickHouse Cloud | 26.4.1.2359, us-east-2, on `v2026.09.17.1` only | 26.2 |
| Kafka | `confluentinc/cp-kafka:7.3.2` | not stated |
| Docker Desktop | 29.8.0 | not stated |

Cloud was not re-run on `v2026.10.08`. This session had no Cloud DSN. See
Pending, item 1.

## Findings

Eight wrong claims. Three are the reviewer's open majors. Five came out of
re-running. Every one reproduces on `v2026.10.08`; the output below is from
that release. `probes.sh` runs every single-message case in one go, and
[probes.out](probes.out) is its output.

### 1. The string row overstates what the matrix proves (reviewer, major)

Page line 372 presents `VARCHAR`, `UUID`, `JSON` against nine destinations as
a cross-product. The matrix groups them only because they share the `utf8`
Arrow carrier, and it overrides the payload per destination. The matrix is
unchanged at `v2026.10.08` apart from the product name.

From `docs/coverage/integrations/sink.clickhouse.yml`, the `utf8:` key:

| Destination | Value the test actually writes |
| --- | --- |
| `String`, `LowCardinality(String)` | `L'Œil 👁 "quoted" back\slash\ttab` |
| `DateTime64(3)` | `2026-09-01 12:00:00.123` |
| `DateTime` | `2026-09-01 12:00:00` |
| `Enum8('' = 0, 'a' = 1, 'b' = 2)` | `a` |
| `FixedString(36)`, `UUID` | `0b7e2c9a-4d31-4f6e-9a02-7c1de3f10b45` |
| `IPv4` | `192.168.1.1` |
| `Decimal(10, 2)` | `1234.56` |

So a UUID payload does not reparse as `DateTime` or `Decimal`.

**Fix, in the draft:** the row is `VARCHAR`, with `UUID` and `JSON` named as
types that reach the sink as text. A note under the table gives the text form
each destination needs.

### 2. The NULL rule breaks on Enum (reviewer, major)

A `NULL` into a non-`Nullable` `Enum8` with no zero member writes a row that
cannot be read back.

```
Enum8('a' = 1, 'b' = 2), non-Nullable, receives NULL
  sqlflow exit            0        (the write succeeds)
  SELECT count()          1        (the row is there)
  SELECT e                Code: 691. DB::Exception: Unexpected value 0 in enum.
                          (UNKNOWN_ELEMENT_OF_ENUM) (version 26.8.1.2041)
  SELECT CAST(e AS Int8)  0

Enum8('' = 0, 'a' = 1, 'b' = 2) receives NULL   -> empty string, reads fine
Nullable(Enum8('a' = 1, 'b' = 2)) receives NULL -> NULL
```

**Fix, in the draft:** the table row and the limits bullet carve out `Enum`.
An enum that may receive a `NULL` needs a `0` member or `Nullable`.

### 3. ReplacingMergeTree does not remove replay duplicates on read (reviewer, major)

Both halves are now measured. `dedup.sh` runs the self-hosted half, and
[dedup.out](dedup.out) is its output.

Self-hosted 26.8.1, non-replicated, one replay sent as the identical block
twice:

| Table | `count()` | `count() FINAL` | After |
| --- | --- | --- | --- |
| `ReplacingMergeTree`, a newer version of each key | 4 | 2 | `OPTIMIZE ... FINAL`: 2, the newer versions |
| `ReplacingMergeTree`, the identical block twice | 4 | 2 | |
| `MergeTree`, the identical block twice | 4 | | |
| `MergeTree SETTINGS non_replicated_deduplication_window = 100`, the identical block twice | 2 | | one part |

Cloud 26.4.1, measured 2026-09-18 on `v2026.09.17.1`: a `ReplacingMergeTree`
read 4 rows, and 2 with `FINAL`. A plain `MergeTree` read 2 for an identical
block sent twice, because every Cloud table is replicated and
`replicated_deduplication_window` drops the second block at insert.

So `ReplacingMergeTree` collapses duplicates on a background merge, and reads
see both until then. Insert-time deduplication drops only a byte-identical
block. A replay re-cuts its batches from the committed offset, and a batch the
flush interval cut short does not come back with the same boundaries, so a
replayed block is not guaranteed identical.

**Fix, in the draft:** the at-least-once bullet states eventual deduplication
on merge, and points at `FINAL` for a read that must be exact now. It names
insert-time deduplication as a partial measure.

### 4. The retry bullet is wrong

The page says a value the driver cannot encode "is retried the full ladder
before failing, and reports as `system.sink.unreachable`". That changed in
sql-flow [#269](https://github.com/turbolytics/sql-flow/pull/269).

ISO 8601 string into a `DateTime` column:

```
exit=10 wall=0s
Error: [user.sink.encode_failed] clickhouse sink: encode row: clickhouse [AppendRow]:
ts parsing time "2026-09-01T12:00:00Z" as "2006-01-02 15:04:05": cannot parse
"T12:00:00Z" as " "
```

`DOUBLE` into `Decimal(10, 2)`:

```
exit=10 wall=0s
Error: [user.sink.encode_failed] clickhouse sink: encode row: clickhouse [AppendRow]:
amount clickhouse [AppendRow]: converting float64 to Decimal(10, 2) is unsupported
```

The limits bullet and the timestamp troubleshooting entry both repeat the old
claim. The draft fixes both.

### 5. The flush_interval_seconds floor does not exist

The page says 30 "is also the smallest value the config schema accepts, so a
lower setting fails validation at startup", and steers readers to lower
`batch_size`. Both halves are false.

```
flush_interval_seconds: 5
  sqlflow validate   ->  /conf/p_flush5.yml: valid
  sqlflow run        ->  exit=0, the row landed
```

**Decided by the code since the pause.** The 2026-09-18 note asked whether
the schema should gain the floor that a comment on `flushIntervalFor`
claimed. That comment no longer claims one. It says absent, zero and
negative all mean the default of 30. The schema carries no `minimum`. So the
page is wrong and the engine is not. This is a doc fix only.

### 6. The tumbling-window bullet describes removed behavior

The page says a window is evaluated against wall clock, that a replay running
behind real time publishes a bucket in parts, "five rows of 100", and that
window output must never be deduplicated.

The window now closes on an event-time watermark that the engine asserts. The
config shape changed with it. A source names its `event_time`, the handler
cuts the bucket as `time_bucket(INTERVAL '<size>', event_time)`, and
`validate` refuses `late_rows` and `poll_interval_seconds`. The 2026-09-18
`win.yml` would no longer validate. It has been rewritten.

`win.sh` replays 1,000 events in one past one-minute bucket, read in ten
batches of 100, with `idle_close_seconds: 15`. [win.out](win.out):

```
   ┌──────────────bucket─┬────n─┐
1. │ 2026-09-01 12:00:00 │ 1000 │
   └─────────────────────┴──────┘
rows published: 1
```

One row, complete. A related check: `allowed_lateness_seconds` with a
ClickHouse window sink validates with a warning. A late row republishes its
bucket as a whole value, so the table must replace the row by key:

```
warning: [user.config.invalid] tables.sql[0] window: allowed_lateness_seconds is
60, so a late row republishes its bucket as a whole value. The clickhouse sink
must replace the row for (bucket, key) rather than add to it, which is its SQL's
or its table engine's to guarantee
```

### 7. The Docker Desktop throughput row no longer holds

The page's third throughput row says Docker Desktop's port forwarding caps
the Kafka fetch at 10 to 15 MB/s, about 24,000 rows a second. On Docker
Desktop 29.8.0 a host binary reaching Kafka and ClickHouse through port
forwarding ran 57,322 and 55,313 rows a second. That matches the in-network
figure. The cap belonged to Docker Desktop 20.10.

The host binary was built from `main` at `8ba6709`, not from the release tag.
The release ships as an image.

**Fix, in the draft:** the row and its paragraph are gone.

### 8. Version pins are stale

`v1.1.0` appears in the prerequisites and in both `docker run` lines. The
draft pins `v2026.10.08` and links the tag.

## What reproduces unchanged

All on `v2026.10.08`.

**The taxi example, extracted verbatim from the page and run end to end.**
The published configs under `dev/config/examples/published/` match the
harness copies byte for byte.

```
publish.py            1,000,660 rows published in 13s
sqlflow               1,000,000 rows, exit 0, 0 errors, wall 18s
```

The Verify query returns exactly the counts the page prints:

```
┌─payment_type─┬──trips─┬─avg_total─┐
│ CSH          │ 617237 │     17.82 │
│ CRE          │ 377897 │     13.91 │
│ NOC          │   3573 │     13.89 │
│ DIS          │   1293 │     12.09 │
└──────────────┴────────┴───────────┘
```

Zero U+FFFD in `pickup_ntaname` or `dropoff_ntaname`. `pickup_datetime` spans
2015-07-01 00:00:16 to 2015-09-30 23:59:58. The page says publishing "takes a
couple of minutes". It took 13 seconds, and 28 on 2026-09-18.

**The Bluesky WebSocket example**, self-hosted, 2,000 posts:

```
exit=0 wall=40s errors=0
count()   2000
┌─lang─┬─posts─┐
│ en   │  1070 │
│ ja   │   236 │
│ es   │    63 │
│ de   │    53 │
│ pt   │    50 │
└──────┴───────┘
```

**NULL handling, the four cases the page distinguishes,** plus an omitted
column:

```
DateTime DEFAULT now()   <- NULL  ->  1970-01-01 00:00:00
String DEFAULT 'unset'   <- NULL  ->  (empty string)
Int32 DEFAULT 7          <- NULL  ->  0
Nullable(String)         <- NULL  ->  NULL
String DEFAULT 'omitted-default', column omitted  ->  omitted-default
```

**Timestamp strings.** The sink parses them in UTC
(`temporalFromString`, `internal/sinks/clickhouse.go`).

```
'2026-09-01 12:00:00'        -> 2026-09-01 12:00:00  (DateTime('UTC') and plain DateTime alike)
'2026-09-01 12:00:00 +09:00' -> 2026-09-01 03:00:00
```

**Decimal via string, and the STRUCT workaround.**

```
CAST(12.34 AS VARCHAR) -> Decimal(10, 2)  ->  12.34
to_json({'k': 1})      -> String          ->  {"k":1}
STRUCT into a String column, exit=10:
  clickhouse sink: column "s": [user.sink.type_unsupported]
  unsupported arrow type struct<k: int32 nullable>
```

**A missing column.** The page quotes
`column "<name>" is not present in the table <table>`. The real message has no
quotes around the name, and carries a code and a prefix. The draft quotes it
as it reads:

```
exit=1
[system.sink.write_failed] clickhouse sink: prepare batch: failed to init block
for HTTP batch: failed to determine columns for HTTP insert: column extra is
not present in the table colmiss
```

## Throughput, re-measured

Self-hosted, SQLFlow and Kafka on the same Docker network, one million
17-column rows, a fresh consumer group per run, every run landing 1,000,000
rows with zero errors. [perf.out](perf.out):

| `batch_size` | `v2026.10.08` run A | run B | `v2026.09.17.1`, 2026-09-18 |
| --- | --- | --- | --- |
| 5,000 | 33,908 | 34,019 | 36,640 and 36,811 |
| 20,000 | 57,877 | 55,908 | 62,733 and 68,103 |
| 50,000 | 58,253 | 56,949 | 68,911 and 68,829 |
| 100,000 | 56,153 | 57,694 | 69,986 and 63,197 |

The shape still holds, so the page's `batch_size` guidance stands. The draft
carries the `v2026.10.08` figures.

**A possible engine regression.** The same day, same machine, same topic, the
old image ran faster:

```
v2026.09.17.1  bs=20000  65,188 and 66,473 rows/s
v2026.10.08    bs=20000  57,877, 55,908 and 58,423 rows/s
```

That is about 13 percent. Two runs a side is not a bisect. See Pending,
item 2.

## Pending

1. **Cloud on `v2026.10.08`.** Needs a Cloud DSN in `$CLICKHOUSE_DSN`. Run
   the taxi example, the Verify query and `perfcloud.sh` twice at `batch_size`
   20,000. The draft keeps the 2026-09-18 Cloud figures marked as such, and
   drops "the same counts against Cloud and self-hosted" until it is re-run.
2. **The throughput drop between `v2026.09.17.1` and `v2026.10.08`.** Confirm
   with more runs, then bisect. Commits between the tags that touch the
   per-record path: the event-time assignment on the Kafka source (#389,
   #383), the watermark (#393, #399), and the TurboStats counters
   (#354, #359, #367, #368). This is an engine question. Publish the page on
   the numbers measured, or fix first: the user's call.
3. **Design the evidence layer the user asked for.** Every claim needs a
   config and a test behind it. Two existing patterns to extend:
   - `dev/config/examples/published/` holds the source-of-truth config for
     every config in third-party docs.
   - `docs/coverage/integrations/sink.clickhouse.yml` declares each type cell
     and an integration test proves it.

   The page's behavioral claims, NULL, Enum, retry, window, flush interval
   and deduplication, have neither. `probes.sh`, `win.sh` and `dedup.sh` are
   the configurations. They are not yet tests.
4. **Push the draft** to `turbolytics/ClickHouse:docs-sqlflow-integration`,
   which updates the PR, and answer the three open threads. This is a write to
   a third-party repository under the user's name: show the diff and the
   replies, and wait for an explicit go.
5. **The user decides** whether to mark it ready for review.

## Notes for whoever resumes

**Benchmark with a fresh consumer group per run.** Not an offset reset. A
stuck container holds the group, and every later run consumes nothing and
reports zero. That poisoned two rounds of numbers on 2026-09-17. `perf.sh`
does it the right way.

**Check the Docker VM's free disk first.** A full VM killed Kafka mid-run on
2026-09-17. On 2026-10-09 it was at 94 percent before the run.
`docker builder prune -af` freed 22 GB.

```bash
docker run --rm alpine df -h /
```

**Kafka can fail to start after a VM restart** with
`KeeperErrorCode = NodeExists` on broker registration: the old session still
holds the broker's ZooKeeper node. Start `kafka1` again once ZooKeeper has
expired it.

**Environment as left.** The `nyc-taxi-trips` topic holds its 1,000,660
messages. `nyc_taxi.trips_small` holds the last run's million rows on
self-hosted. The `probe` database holds every probe table. The dev stack is up.
