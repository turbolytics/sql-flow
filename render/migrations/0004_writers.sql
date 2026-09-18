-- What each pipeline process published, before the processes are merged.
--
-- A pipeline process holds its own window in memory and publishes a closed
-- minute by replacing the row for its key. Replacing is right for one
-- process: a minute written twice, by a retry or a republish, is the same
-- minute. It is wrong across processes. Two instances behind a load balancer
-- each hold part of the 14:03 checkouts, and keyed on the series alone the
-- second to publish replaces the first: 7 and 5 stored as 5. Two instances
-- also run during every deploy, the old beside the new.
--
-- So the key carries the writer. A process replaces only what it published
-- itself, and metrics_1m is the merge of every writer's row. The pipeline
-- writes here and never to metrics_1m.
--
-- writer names one process from start to exit. bin/entrypoint.sh makes a new
-- one on every start, never a stable one: a restarted process has lost its
-- window, and what it publishes next for a minute must add to what the dead
-- process published for it, not replace it.
CREATE TABLE metrics_1m_writers (
  bucket         TIMESTAMPTZ      NOT NULL,
  name           TEXT             NOT NULL,
  type           TEXT             NOT NULL CHECK (type IN ('count', 'gauge')),
  dimensions_key TEXT             NOT NULL,
  writer         TEXT             NOT NULL,
  value_sum      DOUBLE PRECISION NOT NULL,
  value_count    BIGINT           NOT NULL,
  value_min      DOUBLE PRECISION NOT NULL,
  value_max      DOUBLE PRECISION NOT NULL,
  value_last     DOUBLE PRECISION NOT NULL,
  last_at        TIMESTAMPTZ      NOT NULL,
  updated_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, name, type, dimensions_key, writer)
);

-- Re-merges every minute of every series a statement touched, from all of its
-- writers' rows, in the writer's own transaction. The write into metrics_1m
-- fires the series trigger and the rollup triggers, so every grain follows.
--
-- Sums add, min and max nest, and value_last is the value with the latest
-- last_at across writers. The writer breaks a tie so the answer is the same
-- on every re-merge.
CREATE FUNCTION metrics_1m_merge() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock below serializes writers only because each statement in READ
  -- COMMITTED takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'metrics_1m_merge needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;

  -- One lock, taken before this transaction touches anything another writer
  -- wants: metrics_1m, series, or a rollup table.
  --
  -- It does two jobs. Without it two writers publishing one minute at once
  -- each re-merge from a snapshot missing the other's uncommitted row, and the
  -- later commit overwrites the merge with its own half.
  --
  -- It is one lock and not a lock per minute because of what runs under it.
  -- The write below fires the series trigger, which locks a series row, and
  -- then the rollup triggers, which take a lock per bucket of each grain. An
  -- upsert fires this function twice, once for the rows it inserted and once
  -- for the rows it updated, so a transaction can hold a rollup lock from its
  -- first pass and want a series row in its second, while another holds that
  -- row and wants the rollup lock. Four concurrent writers deadlocked that
  -- way within one round under a lock per minute. Behind one lock, writers
  -- run the whole chain one at a time, and no cycle can form.
  --
  -- It serializes publishes across processes. A publish is a few milliseconds
  -- and a process publishes once every poll, ten seconds by default.
  PERFORM pg_advisory_xact_lock(hashtextextended('metrics_1m_merge', 0));

  INSERT INTO metrics_1m (bucket, name, type, dimensions_key,
                          value_sum, value_count, value_min, value_max, value_last, last_at)
  SELECT f.bucket, f.name, f.type, f.dimensions_key,
         sum(f.value_sum),
         sum(f.value_count)::bigint,
         min(f.value_min),
         max(f.value_max),
         (array_agg(f.value_last ORDER BY f.last_at DESC, f.writer))[1],
         max(f.last_at)
  FROM metrics_1m_writers AS f
  JOIN (SELECT DISTINCT bucket, name, type, dimensions_key FROM changed) AS touched
    USING (bucket, name, type, dimensions_key)
  GROUP BY f.bucket, f.name, f.type, f.dimensions_key
  ON CONFLICT (bucket, name, type, dimensions_key) DO UPDATE SET
    value_sum   = excluded.value_sum,
    value_count = excluded.value_count,
    value_min   = excluded.value_min,
    value_max   = excluded.value_max,
    value_last  = excluded.value_last,
    last_at     = excluded.last_at;
  RETURN NULL;
END $fn$;

-- Two triggers: Postgres refuses a transition table on a trigger with more
-- than one event. An upsert fires both, each with its own rows.
CREATE TRIGGER metrics_1m_merge_ins AFTER INSERT ON metrics_1m_writers
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION metrics_1m_merge();
CREATE TRIGGER metrics_1m_merge_upd AFTER UPDATE ON metrics_1m_writers
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION metrics_1m_merge();
