# `sqlflow rollup`: rollup tables the database keeps current, and the serve grains that read them

A rollup declaration names a source table, its time column, the dimensions
and measures to keep, and a ladder of grains. `sqlflow rollup` generates two
things from it: a Postgres migration that creates one table per grain and the
triggers that keep each table current, and the `serve.yml` dataset that reads
those tables within proven bounds. `sqlflow rollup check` fails CI when either
generated file drifts from the declaration.

The pipeline does not change. It writes the finest grain, as it does today.
Serve does not change beyond the two dependencies below. The aggregation lives
in the database, as an analytics layer beside the pipeline.

## The problem

`turbolytics/sql-flow-bluesky-demo` serves per-language post counts from views
that re-aggregate minutes on every request. Load tested on Render on
2026-09-15, with 2.5 days of history:

| Request | Median from a client, 20 requests one at a time |
|---|---|
| `pipeline_status` | 214 ms |
| `5m`, default 12 h | 140 ms |
| `1h`, default 7 d | 1.5 s |
| `1d`, default 30 d | 1.5 s |

A view reads every minute in the range, whatever the grain, so the coarse
grains cost the most and get slower with every day of history. Serve answers
one request at a time, so a 1.5 s request makes every request behind it wait:
at 16 concurrent clients, 66% of requests hit the 10 s timeout, including
`5m` requests that take 140 ms alone.

The fold is not the cost. DuckDB v1.5.2, one thread, 128 MB `memory_limit`,
synthetic rollups with 170 languages in every bucket, folds the widest range
of every grain in 11 to 63 ms from memory and 18 to 80 ms from Parquet on
disk. The production `1h` request folded about 10,000 rows. The rest of its
1.5 s is Postgres re-aggregating minutes or the rows crossing to DuckDB; an
`EXPLAIN (ANALYZE, BUFFERS)` against the production database says which
before the demo adopts this.

## Scope

In:

- The rollup declaration, `rollups.yml`, its JSON Schema, and the rules
  `sqlflow validate` reports.
- A type system for measures: what each stores per bucket, how two buckets
  merge, and what a request reads.
- `sqlflow rollup ddl`: the Postgres generator.
- `sqlflow rollup serve`: the serve dataset generator.
- `sqlflow rollup check`: drift and bound checks for CI.

Out:

- ClickHouse and Iceberg generators. The declaration is backend-neutral so
  they can follow; each gets its own spec. Iceberg has no trigger primitive,
  so its generator will need a sql-flow refresh job.
- A connection pool, metrics and caching in serve. Separate specs, in that
  order.
- Chunked backfill, retention, and destructive declaration changes (see
  "Changing a declaration").
- Serving a dimension set with more than one dimension.

## Depends on

- turbolytics/sql-flow#282: a dataset `range`, each grain's `max_range`, and
  grain selection from the range. The generated dataset uses all three.
- Integer param bounds in serve: `{name: top, type: integer, min: 1, max: 20}`,
  answered with `400 invalid_param` outside the bounds. A separate, small serve
  PR. The generated dataset declares them, and `check` reads `max` to prove the
  row bound.

## Decisions

| Decision | Choice | Rejected |
|---|---|---|
| Where rollups are kept current | Database triggers the generator writes, fired by the pipeline's write of the finest grain. One firing per sink write, so the cost is per closed window, not per event. | A window per grain in the pipeline: the upsert replaces counts, so a restart overwrites the open hour and day with the partial count since the restart. A job that recomputes on a schedule: rollups lag, and a recompute horizon misses a minute republished after it; it stays the Iceberg path. A cache in serve's DuckDB: memory on a 256 MB box, and analytics logic in serve. |
| How a bucket updates | Re-merge: recompute the touched coarse buckets from their finer children. | Delta, adding `NEW - OLD`: only measures with an inverse, sum and count, can subtract. min, max and sketches cannot. |
| Two writers at once | A transaction advisory lock per touched bucket, taken in its own statement before the re-merge. | No lock: two overlapping writers lost an update, measured 7 where 12 was right. SERIALIZABLE: every writer would need to retry serialization failures. |
| Deletes on the source | Not propagated. | Propagated: a retention job deleting old minutes would erase the hourly and daily history, which is what a rollup is for. |
| Serve's grains | Generated into `serve.yml` and checked by `rollup check`. Serve reads SQL, as it does today. | Serve reading `rollups.yml` at startup: rollup logic inside serve, deferred until a scaling barrier or a customer needs it. |
| Out-of-bounds requests | Refused: `400 range_too_wide` (#282) and `400 invalid_param` for `top`. | Clamped: a `200` with less than was asked for, rejected in #284. |
| Backends | A backend-neutral declaration. Postgres is the only generator in this spec. | Designing the ClickHouse or Iceberg generator now. |

## The declaration

The demo's file, complete:

```yaml
# rollups.yml
rollups:
  - name: posts
    source:
      table: posts_per_minute_by_lang
      time_column: bucket
      grain: 1m
      # The source's key besides time. A dimension set equal to it can serve
      # the source grain directly.
      dimensions: [lang]

    # Each grain is built from a finer one. A width is a whole multiple of its
    # from grain's width.
    grains:
      5m:  {from: 1m}
      15m: {from: 5m}
      1h:  {from: 15m}
      6h:  {from: 1h}
      1d:  {from: 6h}

    dimension_sets:
      - name: posts_by_lang
        dimensions: [lang]
        measures:
          posts: {type: sum, column: posts}
      - name: posts_total
        dimensions: []
        measures:
          posts:   {type: sum, column: posts}
          minutes: {type: count_buckets}

    serve:
      # The catalog serve.yml's commands attach the database as.
      catalog: pg
      max_buckets: 365
      datasets:
        - name: posts_by_lang
          dimension_set: posts_by_lang
          description: >-
            Posts per bucket per language. The top languages by posts over the
            range keep their own series and the rest sum into lang "other".
          default_range: 24h
          max_range: {1m: 6h, 5m: 1d, 15m: 3d, 1h: 14d, 6h: 90d, 1d: 365d}
          fold: {dimension: lang, top: {default: 10, max: 20}}
          # A param that filters one dimension value, never folded.
          filters: {lang: lang}
```

Each dimension set gets one table per grain, named `<dimension set>_<grain>`:
`posts_by_lang_5m` through `posts_by_lang_1d` and `posts_total_5m` through
`posts_total_1d`. Durations take one unit, `s`, `m`, `h` or `d`, the same
parser as #282.

### Rules

Reported by `sqlflow validate rollups.yml` and by every `rollup` command, each
as `user.config.rollup` with a YAML path:

1. `source.table`, `source.time_column` and `source.grain` are set.
2. Every grain's `from` is `source.grain` or another declared grain, and the
   graph has no cycle.
3. Every grain's width is a whole multiple of its `from` grain's width.
4. Dimension set names and dataset names are unique, and no generated table
   name is the source table's.
5. A dimension set's dimensions are a subset of `source.dimensions`.
6. Every measure's type is one this version generates (see "Measures").
   `column` names a source column where the type needs one.
7. A served dataset names a declared dimension set with at most one dimension.
8. A served dataset with a dimension declares a `fold` on it.
9. A folded dataset serves no `count_buckets` measure.
10. `default_range` parses and fits the widest `max_range`.
11. Every `max_range` names a declared grain, or the source grain when the
    dimension set equals `source.dimensions`.
12. `max_range / width <= max_buckets` for every grain.
13. `fold.top.default` lies within 1 and `fold.top.max`.

## Measures

A measure has a stored form per bucket, an expression that builds it from the
source, an expression that merges finer buckets into a coarser one, and the
expression a request reads. A measure can roll up only if its merge gives the
same result whatever order the children arrive in.

| Type | Stored | From the source | Merge | Read |
|---|---|---|---|---|
| `sum` | `x bigint` | `sum(col)` | `sum(x)` | `x` |
| `min` | `x`, the column's type | `min(col)` | `min(x)` | `x` |
| `max` | `x`, the column's type | `max(col)` | `max(x)` | `x` |
| `count_buckets` | `x bigint` | `count(DISTINCT time_column)` | `sum(x)` | `x` |

`count_buckets` counts the source buckets present, such as minutes observed.
Its merge is a sum because source buckets never overlap in time. They do
overlap across dimension values, a minute holds many languages, so rule 9
refuses it in a folded dataset.

Defined here so the schema reserves their shape, and refused by rule 6 until a
follow-up generates them:

| Type | Stored | Merge | Read |
|---|---|---|---|
| `avg` | `sum`, `count` | sums | `sum / count` |
| `gauge` | `last`, `last_at`, `min`, `max`, `sum`, `count` | `last` of the row with the greatest `last_at`; min, max, sums | last, min, max, avg |
| `histogram` | `counts bigint[]`, fixed `bounds` | element-wise sum | counts, or quantiles estimated from them |
| sketch (t-digest, HLL) | the extension's state | the extension's merge | the extension's estimate |

An exact median and an exact distinct count have no merge, and are refused
for good. A histogram or a gauge at a coarse grain needs the source to carry
the stored form at the finest grain, so the pipeline's `emit_sql` writes it.
The declaration describes those columns. It does not create them.

## Commands

```
sqlflow rollup ddl   -c rollups.yml [--backend postgres]
sqlflow rollup serve -c rollups.yml [--dataset NAME]
sqlflow rollup check -c rollups.yml --migration FILE --serve FILE
```

`ddl` and `serve` print to stdout. Neither connects to anything. `check` exits
non-zero with every difference and every violated bound, each at a YAML path.

## Generated Postgres DDL

One script, with no `BEGIN` or `COMMIT`. A migration runner wraps it in a
transaction; the demo's `bin/migrate.sh` does.

### Tables

```sql
CREATE TABLE IF NOT EXISTS posts_by_lang_15m (
  bucket TIMESTAMPTZ NOT NULL,
  lang   TEXT        NOT NULL,
  posts  BIGINT      NOT NULL,
  PRIMARY KEY (bucket, lang)
);
```

The key leads with `bucket`, so a range read and a re-merge both use it. A
dimension column copies its source column's type.

### Buckets

Every grain bins with `date_bin(width, t, TIMESTAMPTZ '2000-01-01 00:00:00+00')`.
The origin is midnight UTC, so hours, 6-hour buckets and days fall on UTC
boundaries whatever the session's `TimeZone`. A `7d` grain starts on
Saturdays, because 2000-01-01 was one. `date_bin` takes no months, so no grain
is a month.

### Triggers

For every edge from a finer table to a coarser one, one function and two
triggers on the finer table:

```sql
CREATE OR REPLACE FUNCTION sqlflow_rollup_posts_by_lang_15m() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  -- The lock below serializes writers only because each statement in READ
  -- COMMITTED takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_by_lang_15m needs READ COMMITTED, not %',
      current_setting('transaction_isolation');
  END IF;

  -- One lock per touched bucket, in bucket order, before reading. Skipped
  -- during the migration's backfill, which holds the source table's lock.
  -- The key is the bucket's epoch: b::text renders in the session's
  -- TimeZone, and two writers in different zones would lock different keys.
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_by_lang_15m:' || extract(epoch FROM b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin('15 minutes', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b
          FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO posts_by_lang_15m (bucket, lang, posts)
  SELECT date_bin('15 minutes', f.bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00'), f.lang, sum(f.posts)
  FROM posts_by_lang_5m AS f
  JOIN (SELECT DISTINCT date_bin('15 minutes', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b
        FROM changed) AS touched
    ON f.bucket >= touched.b AND f.bucket < touched.b + INTERVAL '15 minutes'
  GROUP BY 1, 2
  ON CONFLICT (bucket, lang) DO UPDATE SET posts = excluded.posts;

  RETURN NULL;
END $$;

CREATE OR REPLACE TRIGGER sqlflow_rollup_posts_by_lang_15m_ins
  AFTER INSERT ON posts_by_lang_5m
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION sqlflow_rollup_posts_by_lang_15m();

CREATE OR REPLACE TRIGGER sqlflow_rollup_posts_by_lang_15m_upd
  AFTER UPDATE ON posts_by_lang_5m
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION sqlflow_rollup_posts_by_lang_15m();
```

What the shape rests on, each checked on Postgres 18.6 on 2026-09-15:

- Two triggers, not one: Postgres refuses a transition table on a trigger
  with more than one event, "transition tables cannot be specified for
  triggers with more than one event".
- `INSERT ... ON CONFLICT DO UPDATE` fires both statement triggers, once each.
  The INSERT trigger's transition table holds the inserted rows and the UPDATE
  trigger's the updated rows. When every row conflicts, the INSERT trigger
  still fires, with an empty table, and the function writes nothing.
- A trigger's write fires the next table's triggers, so one write cascades up
  the ladder in the writer's transaction. A republished minute replaced its
  count at every grain rather than adding to it.
- Two writers in the same 5m bucket, one holding its transaction open: without
  the lock the 5m row read 7 where the minutes summed to 12. With the lock it
  read 12.

An edge from the source applies each measure's source expression, such as
`count(DISTINCT bucket)`. An edge between rollup tables applies its merge, as
above.

The re-merge reads the finer table through its key by range, never through
an expression on `bucket`. Sorting the locks within a grain, and taking grains
in ladder order, keeps two writers from deadlocking.

A write to the demo's minute table re-merges one bucket per grain. Per
language it reads 5 minutes, 3 five-minute rows, 4 quarter-hours, 6 hours and
4 six-hour rows: 22 rows. At 170 languages, about 3,700 rows per sink write,
all through primary keys. A ladder with small steps keeps every re-merge
small.

The demo's sink writes in READ COMMITTED: `internal/sinks/postgres.go` opens
its transaction with `conn.Begin(ctx)`, which takes the server default.

### Backfill

The script's first statement locks the source table:

```sql
LOCK TABLE posts_per_minute_by_lang IN SHARE ROW EXCLUSIVE MODE;
```

The tables, functions and triggers follow, then the backfill:

```sql
SET LOCAL sqlflow.rollup_backfill = on;

INSERT INTO posts_by_lang_5m (bucket, lang, posts)
SELECT date_bin('5 minutes', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00'), lang, sum(posts)
FROM posts_per_minute_by_lang
GROUP BY 1, 2
ON CONFLICT (bucket, lang) DO UPDATE SET posts = excluded.posts;
-- …and the same for every grain built directly from the source.

SET LOCAL sqlflow.rollup_backfill = off;
```

Only the grains built from the source are backfilled. Their inserts fire the
triggers, which fill every coarser grain once. The table lock blocks the
pipeline's writes until the migration commits, so no write lands between the
triggers existing and the backfill reading. With it held, the per-bucket
locks are unnecessary, and a year of 5m buckets would exhaust the lock table,
so the backfill skips them.

The pipeline's next write waits for the backfill. At the demo's 3,500 minutes
that is under a second. A large history needs a chunked backfill, out of
scope.

### Changing a declaration

The script is additive and safe to run again: `IF NOT EXISTS`,
`CREATE OR REPLACE`, and a backfill that upserts. Adding a grain or a
dimension set is a new migration holding the regenerated script. Removing
one, renaming one, or changing a measure's type needs a hand-written
migration; `rollup check` compares only the migration named by `--migration`.

## Generated serve dataset

`sqlflow rollup serve` prints one dataset per `serve.datasets` entry. For the
demo, with each grain's SQL shown once:

```yaml
- name: posts_by_lang
  description: >-
    Posts per bucket per language. The top languages by posts over the
    range keep their own series and the rest sum into lang "other".
  params:
    - {name: since, type: timestamp}
    - {name: until, type: timestamp}
    - {name: lang,  type: string}
    - {name: top,   type: integer, min: 1, max: 20}
  range: {since: since, until: until, default: 24h}
  grains:
    1m:  {max_range: 6h,   sql: "… FROM pg.posts_per_minute_by_lang …"}
    5m:  {max_range: 1d,   sql: "… FROM pg.posts_by_lang_5m …"}
    15m: {max_range: 3d,   sql: "… FROM pg.posts_by_lang_15m …"}
    1h:  {max_range: 14d,  sql: "… FROM pg.posts_by_lang_1h …"}
    6h:  {max_range: 90d,  sql: "… FROM pg.posts_by_lang_6h …"}
    1d:  {max_range: 365d, sql: "… FROM pg.posts_by_lang_1d …"}
```

Each grain's SQL:

```sql
SELECT bucket,
       CASE WHEN lang_rank <= coalesce($top, 10) THEN lang ELSE 'other' END AS lang,
       sum(posts)::BIGINT AS posts
FROM (
  SELECT bucket, lang, posts,
         dense_rank() OVER (ORDER BY lang_total DESC, lang) AS lang_rank
  FROM (
    SELECT bucket, lang, posts, sum(posts) OVER (PARTITION BY lang) AS lang_total
    FROM pg.posts_by_lang_1h
    WHERE bucket >= $since AND bucket < $until
      AND lang = coalesce($lang, lang)
  )
)
GROUP BY ALL
ORDER BY bucket, lang
```

#282 resolves `since` and `until` before binding, so the SQL needs no default
range and no clamp. Serve's param bounds refuse a `top` outside 1 to 20, so
the SQL needs no clamp there either. The fold's outer aggregate is the
measure's merge: `sum` for `sum`, `min` for `min`, `max` for `max`.

The `1m` grain reads the source table's rows as they are, since they already
hold one row per minute per language. It exists only because `posts_by_lang`
has the source's dimensions (rule 11).

A dataset with no dimension has no fold: each grain selects `bucket` and the
measures from its table within the range, ordered by `bucket`.

## Bounds

Every request's cost is bounded before it runs. `rollup check` proves what the
config can prove, serve refuses what a request can break, and the rest is
measured.

| Bound | Guarantee | How |
|---|---|---|
| Buckets | No grain returns more than `max_buckets` buckets. | Proven: rule 12, checked by `validate` and `check`. |
| Rows out | No response reaches `max_rows`, so `truncated` never happens. | Proven by `check`: `max_buckets × series <= max_rows`, where series is `top.max + 1` for a folded dataset and 1 without a dimension, reading `max_rows` from `serve.yml`. The demo: 365 × 21 = 7,665 <= 10,000. |
| Range | Only a range some grain serves is answered. | Refused per request by #282: `400 range_too_wide`. |
| Series | `top` stays within 1 and `top.max`. | Refused per request by serve's param bounds: `400 invalid_param`. |
| Rows read | At most `max_buckets` × the dimension values present, from one table. | Measured, not proven: the number of distinct values is data, not config. The load test after the pool spec measures it. |

Serve's `max_rows` stays in place as a backstop for a `serve.yml` edited
without running `check`.

## `rollup check`

1. Runs every rule.
2. Generates the DDL and compares it byte for byte with `--migration`.
3. Loads `--serve` with serve's own config loader, finds each generated
   dataset by name, and compares params (name, type, min, max), range, grain
   names, each `max_range`, and each grain's SQL with whitespace collapsed. A
   hand-written dataset such as `pipeline_status` is ignored. Comments and
   key order in `serve.yml` do not matter.
4. Proves the row bound against the `max_rows` that applies to each dataset:
   its own `limits.max_rows`, else `serve.limits.max_rows`, else serve's
   default.

## Tests

Unit, `go test -short`:

- One case per rule, asserting the message and the YAML path.
- Golden files for the demo declaration's DDL and serve dataset.
- `check` fails on a hand edit to each compared field of the migration and
  the dataset, and passes on a comment or key-order change.
- `check` fails when `max_buckets × (top.max + 1)` exceeds `max_rows`, and
  names the dataset.

Integration, against a testcontainers Postgres 18, applying the generated
script for the demo declaration:

- `TestIntegrationRollup_EveryGrainEqualsItsSource`: random minute writes and
  republishes in random batches, then every table at every grain equals a
  `GROUP BY` of the source computed from scratch.
- `TestIntegrationRollup_ARepublishedMinuteReplaces`: a minute written twice
  counts once, at every grain.
- `TestIntegrationRollup_AnUpsertWhereEveryRowConflicts`.
- `TestIntegrationRollup_OverlappingWritersLoseNothing`: two transactions in
  one 5m bucket, the first held open, run once with both sessions in UTC and
  once with the second in `Asia/Kolkata`. Generating the function without the
  lock statement, or keying the lock on `b::text`, must fail this test.
- `TestIntegrationRollup_RefusesAnotherIsolationLevel`: a write under
  REPEATABLE READ raises the function's message.
- `TestIntegrationRollup_BackfillMissesNoConcurrentWrite`: a write started
  while the script runs lands in every grain after it commits.
- `TestIntegrationRollup_BackfillPastTheLockTable`: a backfill over more
  buckets than `max_locks_per_transaction × max_connections` succeeds.
- `TestIntegrationRollup_DeletesDoNotPropagate`.
- `TestIntegrationRollup_BucketsAreUTC`: 6h and 1d buckets read back from an
  `Asia/Kolkata` session sit on UTC boundaries.
- `TestIntegrationRollup_CountBuckets`: `posts_total_1d.minutes` equals the
  distinct minutes of the day.

The feature joins the coverage registry with unit and integration evidence,
the way `sink.postgres` did.

## What breaks if this is wrong

| If | Then | Caught by |
|---|---|---|
| The lock is missing or taken in the re-merge statement | Totals silently lower than the source under overlapping writers. Render runs the old and new worker side by side during a deploy. | `OverlappingWritersLoseNothing`, which must fail without the lock. |
| The writer is not READ COMMITTED | The lock waits, then reads a stale snapshot. | The function raises; `RefusesAnotherIsolationLevel`. |
| The backfill takes per-bucket locks | `out of shared memory` on a large history, and the migration fails. | `BackfillPastTheLockTable`. |
| A trigger errors | The pipeline's write fails and the worker exits; a write that can never succeed is a restart loop. | Every integration test runs the pipeline's own upsert shape. |
| Bucket origin or width drifts between grains | A coarse bucket's children straddle its boundary. | `EveryGrainEqualsItsSource`, `BucketsAreUTC`. |

## Build order

1. Serve's integer param bounds, its own PR.
2. The declaration types, schema and rules, with `validate`.
3. The Postgres DDL generator and the integration tests.
4. The serve dataset generator.
5. `rollup check`.
6. A release. The demo adopts it in
   `sql-flow-bluesky-demo/docs/superpowers/specs/2026-09-15-bluesky-rollup-tables-design.md`.
