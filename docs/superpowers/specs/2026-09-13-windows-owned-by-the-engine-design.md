# Windows owned by the engine: a watermark manager and a late-row policy

Issue #203. Written against `main` at 5e16dec on 2026-09-13.

## The problem

A tumbling window is the user's problem today. They declare the table, write
the aggregate, and then write the close twice: a `collect_closed_windows_sql`
and a `delete_closed_windows_sql` that must agree. The close has to combine
three clocks, and each has a rule the user has to know:

- Event time is the `bucket` column their handler computes.
- Wall time is DuckDB's `now()`, which is frozen at the start of the open
  transaction whenever the pipeline has a state path. Idle ticks exist so
  that clock moves.
- Engine time is `last_arrival` in `sqlflow_progress`, written on a throttle.

Every window bug so far was one of those interactions. #158 was the frozen
clock. #234 was a wall-clock close that fragmented a window under replay.
#267 was a refused publish retried forever. #280 is the progress write
racing the manager's collect on the connection they share. The in-memory and
stateful configurations behave differently for the same YAML, because only
one of them freezes `now()`.

What the config calls a manager is not one. It is a loop that runs the
user's two statements on the pipeline's connection, inside the pipeline's
transaction.
That placement has a consequence nobody asked for: the collect reads rows the
current batch has written and not yet committed. A batch that then fails
rolls those rows back, but the sink already has a window that counted them,
and the replay counts them again. That path is traced, not yet demonstrated,
and this design removes it either way.

Late rows are the last gap, and the one the Iceberg milestone leans on. A
row that arrives after its bucket was published recreates the bucket, and
the next poll publishes it again as a second, smaller row for the same key.
A sink that merges absorbs that. An append-only sink cannot.

## The change

The engine owns the window. The user declares it and writes the aggregate.
Nothing else is theirs. For each declared window the engine runs a
watermark manager: it owns the watermark, decides the close, publishes, and
deletes. The manager is an engine component with no user-facing surface.

```yaml
tables:
  sql:
    - name: posts_per_minute_by_lang
      sql: |
        CREATE TABLE IF NOT EXISTS posts_per_minute_by_lang (
          bucket TIMESTAMPTZ,
          lang TEXT,
          posts INTEGER
        );
      window:
        time_column: bucket
        size_seconds: 60
        grace_seconds: 60
        idle_close_seconds: 300
        late_rows: drop
        poll_interval_seconds: 10
        emit_sql: |
          SELECT bucket, lang, sum(posts) AS posts
          FROM closed
          GROUP BY ALL
        sink:
          type: sqlcommand
          sqlcommand:
            sql: |
              INSERT INTO pg.posts_per_minute_by_lang (bucket, lang, posts, updated_at)
              SELECT bucket, lang, posts, now() FROM sqlflow_sink_batch
              ON CONFLICT (bucket, lang) DO UPDATE
                SET posts = EXCLUDED.posts, updated_at = EXCLUDED.updated_at
```

`window` replaces `manager`. The two predicates are gone. `emit_sql` is the
only SQL, it is optional, and it reads one relation the engine supplies:
`closed`, the rows of every bucket that has just closed. Its default is
`SELECT * FROM closed`.

| Key | Meaning | Default |
| --- | --- | --- |
| `time_column` | The column holding the bucket start. `TIMESTAMPTZ`. | required |
| `size_seconds` | The bucket length. A bucket ends at `time_column + size`. | required |
| `grace_seconds` | How far past a bucket's end the stream must reach before it closes. | 0 |
| `idle_close_seconds` | How long the stream may be quiet before every open bucket closes. | 0, never |
| `late_rows` | What to do with a row for a bucket that already closed: `drop` or `reemit`. | required |
| `poll_interval_seconds` | How often the engine looks for closed buckets. | 10 |
| `emit_sql` | Shapes the closed rows before the sink. Reads `closed`. | `SELECT * FROM closed` |
| `sink` | Where closed windows go. Unchanged. | required |

Seconds, because every duration in the config is `_seconds`.

### The watermark

One value per window, in event time, persisted, and never moving backwards.
A bucket is closed when its end is at or before the watermark.

```
watermark = max(previous, newest - grace)                  while data arrives
watermark = max(previous, newest + size)                   after idle_close of silence
```

`newest` is `max(time_column)` over the table's committed rows. `previous` is
the persisted value. The first rule is #234's stream clock, stated once: a
bucket closes when the stream has moved past its end by the grace. The second
is the idleness bound every example carries today, restated in event time: a
stream that stops closes everything it has.

The idle rule is triggered by wall clock and produces event time. Nothing
else can trigger it: an idle stream delivers no event time. The engine
compares its own clock with `last_arrival` from `sqlflow_progress`, in Go,
and when the silence has lasted `idle_close_seconds` it moves the watermark
to the newest bucket's end. So the claim is narrower than "no wall clock":
wall clock decides when the idle rule fires, the watermark's value is always
event time, and `now()` appears in no SQL the manager runs. That last part
is what removes the frozen-transaction clock from the close.

`newest` is one value for the whole table, so it is the fastest partition's
clock. A topic whose partitions run at uneven rates has a slow partition
whose rows arrive late through no fault of their own, and the grace is the
only allowance for them. A per-partition watermark, the minimum across
partitions, needs event time per partition, and the handler aggregates that
away before the table sees it. It is a follow-up with its own issue, below,
and `window_late_rows_total` is how an operator sees whether the grace
covers the skew until then.

The watermark lives in a new engine table beside `sqlflow_offsets` and
`sqlflow_progress`:

```sql
CREATE TABLE IF NOT EXISTS sqlflow_windows (
    name       VARCHAR PRIMARY KEY,
    watermark  TIMESTAMPTZ,
    closed_at  TIMESTAMPTZ
)
```

It is state, so with a state path it survives a restart and with none it
does not, the same as the window table it describes. `/stats` reports it
per window, and `CollectStateStats` treats it as an engine table the way it
treats the other two.

### The close, per poll

The engine runs this on the window's own connection, in its own transaction.

1. Read `previous` from `sqlflow_windows`.
2. Late rows: every row with `time_column + size <= previous` belongs to a
   bucket that already closed. Under `drop`, delete them and count them.
   Under `reemit`, leave them for step 4, which publishes them again.
3. Compute the watermark from the rules above. If it did not move and there
   are no late rows to re-emit, commit nothing and return.
4. Collect: `closed` is every row with `time_column + size <= watermark`.
   Run `emit_sql` over it. Write the result to the sink and flush.
5. In one transaction: delete the collected rows, write the watermark,
   commit.

A failure at step 4 leaves everything as it was: the rows stay, the
watermark stays, and the poll returns the sink's error, which stops the
process the way #270 made it. A crash between steps 4 and 5 replays the
close on the next start, so delivery to the sink is at-least-once, the same
as the consume loop's. That is the one duplicate this design does not remove,
and it is the same one every sink already handles. For an append-only
Iceberg writer, #205 can make its append idempotent on the window name and
watermark it carries, which closes that gap without touching the window.

### Its own connections

The window runs on two connections of its own to the same DuckDB, the way
`/stats` already has one. The close runs on the first, with autocommit off:
it sees committed rows only, so a batch that rolls back was never counted,
and its delete and its watermark commit together. The sink runs on the
second, under autocommit, because DuckDB lets one transaction write to one
database only, and a sink stages a batch table or writes into an attached
Postgres. The idle tick has one job again: moving the pipeline's commit
clock. Nothing the window does runs under the pipeline's lock, and #280's
race has no connection to happen on.

`closed` is spliced into `emit_sql` as a common table expression rather than
created as a view, for the same one-database rule: a `CREATE`, even of a
temporary view, is a catalog write.

DuckDB resolves a write conflict between the two connections by failing the
later transaction. The window's transaction touches rows whose buckets have
closed, and the handler writes rows for buckets that have not, so they
conflict only when a late row arrives for a bucket the window is deleting.
That is a failed poll. It logs, and the next poll retries the same close,
which is what a poll loop is for. This is the one place the loop keeps a
retry.

### Late rows

`drop` is for sinks that cannot merge: append-only Iceberg, a Kafka topic
read by something that counts. `reemit` is today's behaviour, for sinks that
upsert. `late_rows` has no default: the two policies are different promises
to the sink, and a config that does not say which one it makes is refused.

`reemit` publishes a second row for a bucket the sink already holds. A sink
that upserts on the bucket's key, Postgres through `sqlcommand` with
`ON CONFLICT`, replaces the value. A sink that appends holds both rows, and
its reader cannot tell which is current. `sqlflow validate` warns when
`reemit` is paired with the Iceberg or Kafka sink, and the docs say plainly
that `reemit` produces corrections the downstream has to apply.

The metric says how often the choice matters: a `drop` count that keeps
rising means the grace is too short for the stream.

### The watermark manager

`internal/managers` keeps its name. `Tumbling` becomes `Watermark`, built
from the declaration, and it generates its own SQL. It keeps the shape #270
gave the manager: one poll per interval, a final poll on the drain budget
after a cancel, and a refused publish stops the process. The harness subject
and the `manager.*` invariants stay, and gain three:

| Invariant | Class | Claim |
| --- | --- | --- |
| `manager.watermark.never_regresses` | safety | Across polls and restarts, the persisted watermark never moves backwards. |
| `manager.close.committed_rows_only` | safety | A close never publishes a row the pipeline has not committed. |
| `manager.late.policy_holds` | safety | Under `drop`, a bucket below the watermark is never published again. Under `reemit`, a late row is published once. |

The sink role label stays `manager`, because the rows still leave through
one.

### Metrics

- `window_watermark_seconds{window}`, a gauge, the watermark as Unix time.
  Wall time minus this is how far the stream's clock trails, which is the
  number an operator sizes `grace_seconds` from.
- `window_closed_total{window}`, buckets closed.
- `window_late_rows_total{window,policy}`, as #203 asks.

### Validation

`sqlflow validate` checks the declaration without running anything:

- `time_column` is a column of the table's `CREATE`, and its type is
  `TIMESTAMPTZ`. A `TIMESTAMP` is the bug the progress table's comment
  describes, and it is caught here rather than in production.
- `emit_sql` parses, reads `closed`, and reads no other table.
- `manager:` is an error that names `window:` and the four keys that replace
  the two predicates. The engine does not accept both. The examples, the
  tutorials and the demo all move in the same release, so nothing published
  carries the old block.

## Compatibility

This is a breaking change to the pipeline file. A config with a `manager`
block stops validating and stops running, and there is no translation: the
declaration cannot be derived from two arbitrary predicates. The release
that ships it is a major version, with a migration note that maps the old
keys to the new ones:

| Was | Is |
| --- | --- |
| `manager.tumbling_window.collect_closed_windows_sql` | `window.emit_sql` over `closed`, with the predicate dropped |
| `manager.tumbling_window.delete_closed_windows_sql` | gone |
| `manager.tumbling_window.poll_interval_seconds` | `window.poll_interval_seconds` |
| `manager.sink` | `window.sink` |
| the predicate's grace | `window.grace_seconds` |
| the predicate's idleness clause | `window.idle_close_seconds` |
| the bucket length the predicate assumed | `window.size_seconds` |
| the bucket column the predicate named | `window.time_column` |
| nothing | `window.late_rows`, required |

Versioning the pipeline schema itself, so a file says which format it is,
is the pending item that this change makes due. It is its own issue, and
this design does not wait for it.

Durations stay `_seconds` integers because every other duration in the file
is one. A duration type that accepts `5m` as well as `300` belongs to all of
them at once, and is a separate change.

## What this removes

- Both predicates, from every config and every tutorial.
- `now()` from anything that decides a close, and with it the difference
  between the in-memory and stateful configurations.
- The idle tick's second job.
- The manager's reads of uncommitted rows.
- #280, by construction.

## What it does not do

- Hopping and session windows. The declaration extends to them with a `hop`
  key and a gap, and nothing here prevents that. Tumbling only.
- A per-partition watermark. The watermark is the fastest partition's clock,
  and the grace covers the skew. The follow-up needs event time per
  partition from the batch table, before the handler aggregates it.
- An exactly-once close. See the crash between steps 4 and 5.
- Bounding the idle rule without wall clock. An idle stream has no event
  time to offer, so the trigger is the engine's clock.
- Retention for `reemit`. A late row under `reemit` republishes its bucket
  forever, once per arrival, which is what today's configs do.

## Done when

- The Bluesky window with `late_rows: drop`, a 60 second size and a 60
  second grace produces exactly one row per bucket and language across a 30
  minute run, verified by a primary key insert into Postgres that never
  conflicts. This is #203's criterion, unchanged.
- The same pipeline restarted mid-run resumes with its watermark and does
  not publish a bucket it published before the restart.
- Every example and tutorial that had a `manager` block has a `window`
  block, and `sqlflow validate` rejects the old one with the message above.
- The watermark manager proves the three new invariants and the four it
  inherits, and the matrix enforces all seven.
