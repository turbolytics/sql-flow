# Windows owned by the engine: a watermark, a late-row policy, and no manager

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

The manager is not a manager. It is a loop that runs the user's two
statements on the pipeline's connection, inside the pipeline's transaction.
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
Nothing else is theirs.

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
| `late_rows` | What to do with a row for a bucket that already closed: `drop` or `reemit`. | `reemit` |
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
stream that stops closes everything it has. Both rules produce an event-time
value, so the watermark never mixes clocks, and `now()` appears nowhere in
anything that decides a close.

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

### Its own connection

The window runs on a second connection to the same DuckDB, the way `/stats`
already does. It sees committed rows only, so a batch that rolls back was
never counted. Its delete and its watermark commit together, on their own,
so the idle tick has one job again: moving the pipeline's commit clock.
Nothing it does runs under the pipeline's lock, and #280's race has no
connection to happen on.

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
upsert. Both are explicit, and the metric says how often the choice matters:
a `drop` count that keeps rising means the grace is too short for the
stream.

### The loop

`internal/managers` becomes `internal/windows`. The type is `Window`, it is
built from the declaration, and it generates its own SQL. It keeps the shape
#270 gave the manager: one poll per interval, a final poll on the drain
budget after a cancel, and a refused publish stops the process. The harness
subject stays, renamed. `manager.*` invariants become `window.*` and gain
three:

| Invariant | Class | Claim |
| --- | --- | --- |
| `window.watermark.never_regresses` | safety | Across polls and restarts, the persisted watermark never moves backwards. |
| `window.close.committed_rows_only` | safety | A close never publishes a row the pipeline has not committed. |
| `window.late.policy_holds` | safety | Under `drop`, a bucket below the watermark is never published again. Under `reemit`, a late row is published once. |

The sink role label becomes `window`. It was `manager`.

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
- An exactly-once close. See the crash between steps 4 and 5.
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
- `internal/windows` proves the three new invariants and the four it
  inherits, and the matrix enforces all seven.
