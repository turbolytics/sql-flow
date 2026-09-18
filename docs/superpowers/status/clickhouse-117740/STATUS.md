# ClickHouse docs PR #117740: re-verification status

Paused on 2026-09-18. This file is the handoff. Everything below was produced
by running it, on the versions named.

**The PR:** [ClickHouse/ClickHouse#117740](https://github.com/ClickHouse/ClickHouse/pull/117740),
the sqlflow community integration page. Draft. Head `fe238499` on
`turbolytics/ClickHouse:docs-sqlflow-integration`. One file, 511 lines, plus a
navigation entry.

**The goal:** pin the newest release, re-verify every claim on it, fix the
reviewer's three open majors, and take the page out of draft.

**The standard the user set, 2026-09-18:** every claim needs test evidence and
a configuration to reproduce it, with versions. The page does not meet that
bar today and neither does the repo: the two worked examples have configs
under `dev/config/examples/published/`, but the page's behavioral claims have
no config and no test behind them. See Pending, item 5.

## Versions

| Component | Measured on | Page currently says |
| --- | --- | --- |
| sqlflow | `turbolytics/sql-flow:v2026.09.17.1` | v1.1.0 |
| ClickHouse self-hosted | 26.8.1.2041, timezone UTC | 26.8 |
| ClickHouse Cloud | 26.4.1.2359, us-east-2 | 26.2 |
| Kafka | `confluentinc/cp-kafka:7.3.2` | not stated |

## Findings

Seven wrong claims. Three are the reviewer's open majors; four I found while
re-running. Each is reproduced below.

### 1. The string row overstates what the matrix proves (reviewer, major)

Page line 372 presents `VARCHAR`, `UUID`, `JSON` against nine destinations as
a cross-product. The matrix groups them only because they share the `utf8`
Arrow carrier, and it overrides the payload per destination.

From `docs/coverage/integrations/sink.clickhouse.yml` at `v2026.09.17.1`, the
`utf8:` key:

| Destination | Value the test actually writes |
| --- | --- |
| `String`, `LowCardinality(String)` | `L'Œil 👁 "quoted" back\slash\ttab` |
| `DateTime64(3)` | `2026-09-01 12:00:00.123` |
| `DateTime` | `2026-09-01 12:00:00` |
| `Enum8('' = 0, 'a' = 1, 'b' = 2)` | `a` |
| `FixedString(36)`, `UUID` | `0b7e2c9a-4d31-4f6e-9a02-7c1de3f10b45` |
| `IPv4` | `192.168.1.1` |
| `Decimal(10, 2)` | `1234.56` |

So a UUID payload does not reparse as `DateTime` or `Decimal`. The page tells
readers it does, because it also says "a pairing in the table is proven".

**Fix:** say these are string-carrying columns and the payload must already be
in the target type's textual form. Give the form per destination.

### 2. The NULL rule breaks on Enum (reviewer, major)

Confirmed. A `NULL` into a non-`Nullable` `Enum8` with no zero member writes a
row that cannot be read back.

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

The zero member is the safety condition, and the matrix's proven cell carries
one deliberately. The page never says so.

**Fix:** carve `Enum` out of the blanket NULL rule, in the limits bullet and
in the table row. An enum that may receive a NULL needs a `0` member or
`Nullable`.

### 3. ReplacingMergeTree does not remove replay duplicates on read (reviewer, major)

Confirmed on Cloud 26.4.1. Distinct blocks, same keys:

```
count()           4     <- an ordinary read sees both versions
count() FINAL     2
active parts      2
rows              1,"first"  1,"second"  2,"first"  2,"second"
after OPTIMIZE TABLE ... FINAL:
count()           2
```

The page's at-least-once bullet offers `ReplacingMergeTree` as the mitigation
and says it "removes" duplicates. It collapses them on a background merge, and
reads see both until then.

**A separate finding, worth stating on the page.** On a replicated table,
which Cloud always is, an identical block inserted twice is dropped at insert
time by `replicated_deduplication_window`. A plain `MergeTree` on Cloud
returned `count() = 2`, not 4, for the same block sent twice. That is not
`ReplacingMergeTree` and it does not apply to a non-replicated self-hosted
table. Not yet measured on self-hosted; see Pending, item 2.

**Fix:** state the real contract, eventual deduplication on merge, and point
at `FINAL` or an idempotent insert for immediate correctness.

### 4. The retry bullet is wrong (mine)

The page says a value the driver cannot encode "is retried the full ladder
before failing, and reports as `system.sink.unreachable`". That changed in
sql-flow [#269](https://github.com/turbolytics/sql-flow/pull/269).

Measured, ISO 8601 string into a `DateTime` column:

```
wall  2s      (one attempt, no ladder)
exit  10
[user.sink.encode_failed] clickhouse sink: encode row: clickhouse [AppendRow]:
ts parsing time "2026-09-01T12:00:00Z" as "2006-01-02 15:04:05":
cannot parse "T12:00:00Z" as " "
```

Same shape for a `DOUBLE` into `Decimal(10, 2)`:

```
exit  10
[user.sink.encode_failed] clickhouse sink: encode row: clickhouse [AppendRow]:
amount clickhouse [AppendRow]: converting float64 to Decimal(10, 2) is unsupported
```

Both the limits bullet and the troubleshooting section repeat the old claim.

### 5. The flush_interval_seconds floor does not exist (mine)

The page says 30 "is also the smallest value the config schema accepts, so
a lower setting fails validation at startup", and steers readers to lower
`batch_size` instead. Both halves are false at `v2026.09.17.1`.

```
flush_interval_seconds: 5
  sqlflow validate   ->  /conf/flush5.yml: valid
  sqlflow run        ->  exit 0, 100 rows inserted
```

The generated schema carries no `minimum` on the key. `flushIntervalFor` in
`internal/cli/run/metrics.go` has a comment claiming a floor the schema does
not enforce, which is probably where the page's claim came from.

**Decide before fixing:** either the schema should carry the floor the comment
claims, or the page and the comment are both wrong. This one may be an engine
finding rather than a doc finding.

### 6. The tumbling-window bullet describes removed behavior (mine)

The page says, measured on v1.0.6, that a window is evaluated against wall
clock, that a replay running behind real time publishes a bucket in parts,
"five rows of 100", and that window output must therefore never be
deduplicated.

The engine-owned watermark shipped in `v2026.09.14`. The README now says
"Wall clock appears nowhere in the close."

Measured on `v2026.09.17.1`, 1,000 events in one past one-minute bucket, read
in ten batches of 100, `idle_close_seconds: 15`:

```
┌──────────────bucket─┬────n─┐
│ 2026-09-01 12:00:00 │ 1000 │
└─────────────────────┴──────┘
rows published: 1
```

One row, complete. The whole bullet and the advice built on it are obsolete.

### 7. Version pins are stale (mine)

`v1.1.0` appears in prerequisites and in both `docker run` lines. Cloud is
named as 26.2 and measured at 26.4.1. House convention also wants the version
in prose to link to its tag; the prerequisites line is bare text today.

## What reproduces unchanged

Worth keeping, and worth saying that it was re-run rather than assumed.

**The taxi example, extracted verbatim from the page and run end to end.**

```
publish.py            1,000,660 rows published in 28s
sqlflow               1,000,000 consumed, exit 0, 0 errors, wall 21s
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

Zero U+FFFD in `pickup_ntaname` or `dropoff_ntaname`, so the lossless
publisher rewrite still holds. `pickup_datetime` spans 2015-07-01 00:00:16 to
2015-09-30 23:59:58.

One wording fix: the page says publishing "takes a couple of minutes". It took
28 seconds.

**NULL handling, the four cases the page distinguishes.**

```
DateTime DEFAULT now()   <- NULL  ->  1970-01-01 00:00:00
String DEFAULT 'unset'   <- NULL  ->  (empty string)
Int32 DEFAULT 7          <- NULL  ->  0
Nullable(String)         <- NULL  ->  NULL
String DEFAULT 'omitted-default', column omitted  ->  omitted-default
```

**Timestamp strings.** The sink parses them client-side in UTC
(`temporalFromString`, `internal/sinks/clickhouse.go`, via
`time.ParseInLocation(..., time.UTC)`), so the server timezone does not move
the instant.

```
'2026-09-01 12:00:00'        -> 2026-09-01 12:00:00  (DateTime('UTC') and plain DateTime alike)
'2026-09-01 12:00:00 +09:00' -> 2026-09-01 03:00:00
```

**Decimal via string, and the STRUCT workaround.**

```
CAST(12.34 AS VARCHAR) -> Decimal(10, 2)  ->  12.34
to_json({'k': 1})      -> String          ->  {"k":1}
STRUCT into a String column:
  clickhouse sink: column "s": [user.sink.type_unsupported]
  unsupported arrow type struct<k: int32 nullable>
```

**A missing column.** The page quotes
`column "<name>" is not present in the table <table>`. The real message has no
quotes around the name and carries a code and a prefix:

```
exit 1
[system.sink.write_failed] clickhouse sink: prepare batch: failed to init block
for HTTP batch: failed to determine columns for HTTP insert: column extra is
not present in the table colmiss
```

**All seven links on the page resolve.**

## Throughput, re-measured

Self-hosted, sqlflow and Kafka on the same Docker network, one million
17-column rows, fresh consumer group per run, every run landing 1,000,000 rows
with zero errors.

| `batch_size` | Run A | Run B |
| --- | --- | --- |
| 5,000 | 36,640 | 36,811 |
| 20,000 | 62,733 | 68,103 |
| 50,000 | 68,911 | 68,829 |
| 100,000 | 69,986 | 63,197 |

ClickHouse Cloud, us-east-2, over the public internet, `batch_size` 20,000:
28,215 and 25,436 rows a second.

The shape matches what the page already says, so its `batch_size` guidance
stands. The numbers themselves need replacing.

## Pending

1. **Re-run the Bluesky WebSocket example**, the page's second example, 2,000
   posts, on `v2026.09.17.1`.
2. **ReplacingMergeTree on self-hosted**, non-replicated, to contrast with
   Cloud's insert-time deduplication. Finding 3 has the Cloud half only.
3. **The third throughput row**, Docker Desktop host-to-container port
   forwarding. Re-measure or drop it.
4. **Cloud Verify query counts**, to keep the page's claim that both targets
   return the same counts.
5. **Design the evidence layer the user asked for.** Every claim needs a
   config and a test behind it. Two existing patterns to extend rather than
   replace:
   - `dev/config/examples/published/` holds the source-of-truth config for
     every config that appears in third-party docs. Its README already argues
     the case: a config that lives only in a vendor's page is checked by
     nobody.
   - `docs/coverage/integrations/sink.clickhouse.yml` declares each type cell
     and an integration test proves it.

   The page's behavioral claims (NULL, Enum, retry, window, flush interval,
   deduplication) have neither. They are the gap.
6. **Rewrite the page** and push to
   `turbolytics/ClickHouse:docs-sqlflow-integration`, which updates the PR.
   The fork is `turbolytics/ClickHouse`; the docs tree has been edited through
   the GitHub API rather than by cloning the monorepo.
7. **The user decides** whether to mark it ready for review.

## Notes for whoever resumes

**Benchmark with a fresh consumer group per run.** Not an offset reset. Three
of my own sqlflow containers sat stuck on the `nyc-taxi` group for 17 hours
after Kafka died, holding the topic's single partition, so every later run
consumed nothing and reported zero. Two rounds of numbers were poisoned before
I noticed. `perf.sh` in this directory does it the right way.

**The Docker VM filled up**, which is what killed Kafka mid-run. 13 GB was
freed: 7.3 GB of buildx builder state, 11.3 GB of one-off Kafka test topics
(soak gates and benchmark runs), 2.7 GB of build cache, 1.0 GB of dangling
images. Kafka's data volume went from 12.2 GB to 846 MB. Check
`docker exec clickhouse df -h /var/lib/clickhouse` before a long run.

**Environment as left.** The `nyc-taxi-trips` topic holds its 1,000,660
messages. `nyc_taxi.trips_small` exists on both targets. The dev stack
(`kafka1`, `clickhouse` on `dev_default`) is up. The Cloud DSN is in
`$CLICKHOUSE_DSN`.

The harness scripts are beside this file. They were written against the dev
stack's container names and the `dev_default` network.
